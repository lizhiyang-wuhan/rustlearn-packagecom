//! 消息总线核心：服务路由表 + 发布/订阅广播 + 请求/响应。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bus_api::blob::BlobClient;
use bus_api::error::{BusError, Result};
use bus_api::event::Event;
use bus_api::topic::Topic;
use bus_api::traits::{
    ErasedHandler, Publisher, Requester, ServiceRegistry, Subscriber, Subscription, SubscriptionStream,
};
use tokio::sync::{broadcast, oneshot};
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::blob::SharedMemoryBlobClient;
use crate::param::ParamStore;

/// 默认广播通道容量（每个 topic 的环形缓冲长度）。
pub const DEFAULT_BROADCAST_CAPACITY: usize = 256;
/// 默认请求响应超时。
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

struct Inner {
    /// 服务路由表：topic -> handler。查表即释放锁，故用 `std::sync::Mutex`。
    handlers: Mutex<HashMap<Topic, Arc<dyn ErasedHandler>>>,
    /// 发布/订阅广播表：每个 topic 一个 broadcast sender。
    /// publish/subscribe 均为同步 API，故用 `std::sync::RwLock`。
    topics: RwLock<HashMap<Topic, broadcast::Sender<Event>>>,
    broadcast_capacity: usize,
    request_timeout: Duration,
    params: ParamStore,
    blobs: Arc<dyn BlobClient>,
}

/// 消息总线。`Clone` 仅复制内部 `Arc`，共享同一路由/广播状态。
pub struct MessageBus {
    inner: Arc<Inner>,
}

impl Clone for MessageBus {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl MessageBus {
    /// 用默认参数创建总线。
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// 创建构造器。
    pub fn builder() -> MessageBusBuilder {
        MessageBusBuilder::default()
    }

    /// 参数存储（供装配层构造 ModuleContext）。
    pub fn params(&self) -> &ParamStore {
        &self.inner.params
    }

    /// 为某模块构造能力上下文：把总线自身作为各类能力对象注入。
    pub fn context_for(&self, module_name: &str) -> bus_api::traits::ModuleContext {
        let shared: Arc<MessageBus> = Arc::new(self.clone());
        bus_api::traits::ModuleContext {
            module_name: Arc::from(module_name),
            publisher: shared.clone(),
            subscriber: shared.clone(),
            requester: shared.clone(),
            registry: shared.clone(),
            params: self.inner.params.as_client(),
            param_registry: self.inner.params.as_provider(),
            blobs: Arc::clone(&self.inner.blobs),
        }
    }

    /// 取出（或惰性创建）某 topic 的 broadcast sender。
    fn topic_sender(&self, topic: &Topic) -> broadcast::Sender<Event> {
        // 先读锁快路径
        if let Some(tx) = self.inner.topics.read().unwrap().get(topic) {
            return tx.clone();
        }
        // 升级写锁创建
        let mut table = self.inner.topics.write().unwrap();
        table
            .entry(topic.clone())
            .or_insert_with(|| broadcast::channel(self.inner.broadcast_capacity).0)
            .clone()
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Publisher for MessageBus {
    fn publish(&self, event: Event) -> Result<()> {
        let tx = self.topic_sender(&event.topic);
        // broadcast::send 在无接收者时返回 SendError(原始消息)；映射为 NoSubscriber
        tx.send(event)
            .map(|_n| ())
            .map_err(|e| BusError::NoSubscriber(e.0.topic.to_string()))
    }
}

impl Subscriber for MessageBus {
    fn subscribe(&self, topic: Topic) -> Result<Arc<dyn Subscription>> {
        let tx = self.topic_sender(&topic);
        Ok(Arc::new(BroadcastSubscription { topic, tx }))
    }
}

#[async_trait]
impl Requester for MessageBus {
    async fn request(&self, event: Event) -> Result<Event> {
        let topic = event.topic.clone();
        // 查表取出 handler 的 Arc，随即释放锁（不在持锁期间 await）
        let handler = self.inner.handlers.lock().unwrap().get(&topic).cloned();
        let handler = handler.ok_or_else(|| BusError::NoHandler {
            topic: topic.to_string(),
        })?;

        let (reply_tx, reply_rx) = oneshot::channel();
        // handler 内部自行 tokio::spawn 后通过 oneshot 回话，此调用立即返回
        handler.handle(event, reply_tx);

        match timeout(self.inner.request_timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_recv_err)) => Err(BusError::Closed(format!(
                "handler for topic '{topic}' dropped the reply channel"
            ))),
            Err(_elapsed) => Err(BusError::Timeout {
                topic: topic.to_string(),
            }),
        }
    }
}

impl ServiceRegistry for MessageBus {
    fn register_handler(&self, topic: Topic, handler: Arc<dyn ErasedHandler>) -> Result<()> {
        let mut table = self.inner.handlers.lock().unwrap();
        if table.contains_key(&topic) {
            return Err(BusError::DuplicateHandler {
                topic: topic.to_string(),
            });
        }
        table.insert(topic, handler);
        Ok(())
    }
}

/// 基于 broadcast receiver 的订阅会话。持有 sender，`into_stream` 时再 `subscribe()`
/// 出独立 receiver，因此 `clone_box` 得到的每个会话各自独立、互不抢占消息。
struct BroadcastSubscription {
    topic: Topic,
    tx: broadcast::Sender<Event>,
}

impl Subscription for BroadcastSubscription {
    fn topic(&self) -> &Topic {
        &self.topic
    }

    fn clone_box(&self) -> Arc<dyn Subscription> {
        Arc::new(BroadcastSubscription {
            topic: self.topic.clone(),
            tx: self.tx.clone(),
        })
    }

    fn into_stream(self: Arc<Self>) -> SubscriptionStream {
        let rx = self.tx.subscribe();
        let topic = self.topic.clone();
        // 把 broadcast 的 Result<Event, Lagged> 适配为不中断的 Event 流：
        // Lagged（慢消费者丢帧）记录告警后返回 None 跳过，其余原样产出。
        // 注意：tokio_stream 的 filter_map 闭包同步返回 Option<T>（非 Future）。
        let stream = BroadcastStream::new(rx).filter_map(move |item| match item {
            Ok(event) => Some(event),
            Err(lagged) => {
                tracing::warn!(
                    topic = %topic,
                    error = %lagged,
                    "subscriber lagged behind broadcast buffer, dropping missed events"
                );
                None
            }
        });
        Box::pin(stream)
    }
}

/// [`MessageBus`] 构造器。
#[derive(Clone)]
pub struct MessageBusBuilder {
    broadcast_capacity: usize,
    request_timeout: Duration,
    blobs: Option<Arc<dyn BlobClient>>,
    params: Option<ParamStore>,
}

impl Default for MessageBusBuilder {
    fn default() -> Self {
        Self {
            broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            blobs: None,
            params: None,
        }
    }
}

impl MessageBusBuilder {
    /// 设置每个 topic 的广播环形缓冲容量。
    pub fn broadcast_capacity(mut self, cap: usize) -> Self {
        self.broadcast_capacity = cap.max(1);
        self
    }

    /// 设置请求响应超时。
    pub fn request_timeout(mut self, dur: Duration) -> Self {
        self.request_timeout = dur;
        self
    }

    /// 注入自定义 blob 客户端（默认进程内共享内存实现）。
    pub fn blob_client(mut self, client: Arc<dyn BlobClient>) -> Self {
        self.blobs = Some(client);
        self
    }

    /// 注入外部参数存储（默认新建一个）。
    pub fn param_store(mut self, store: ParamStore) -> Self {
        self.params = Some(store);
        self
    }

    /// 构建总线。
    pub fn build(self) -> MessageBus {
        MessageBus {
            inner: Arc::new(Inner {
                handlers: Mutex::new(HashMap::new()),
                topics: RwLock::new(HashMap::new()),
                broadcast_capacity: self.broadcast_capacity,
                request_timeout: self.request_timeout,
                params: self.params.unwrap_or_else(ParamStore::new),
                blobs: self.blobs.unwrap_or_else(SharedMemoryBlobClient::shared),
            }),
        }
    }
}
