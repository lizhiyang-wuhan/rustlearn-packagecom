//! 总线能力抽象：跨 crate 传递的是这些 trait 对象（"具备发布能力的对象"），
//! 而不是任何具体通道。模块依赖抽象，运行时实现可替换。

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch};
use tokio_stream::Stream;

use crate::blob::{BlobClient, BlobSink, BlobStream, BlobWriter};
use crate::error::Result;
use crate::event::Event;
use crate::param::{ParamClient, ParamProvider};
use crate::topic::Topic;

/// 发布能力：向某个 topic 广播事件（发布/订阅语义的发送端）。
///
/// 同步签名——底层实现（broadcast send）本身非阻塞；
/// 无人订阅时返回 [`crate::BusError::NoSubscriber`]，由调用方决定是否忽略。
pub trait Publisher: Send + Sync {
    fn publish(&self, event: Event) -> Result<()>;
}

/// 一次订阅会话。底层是 broadcast receiver 还是 mpsc，对模块不可见。
pub trait Subscription: Send + Sync {
    /// 订阅的 topic（用于日志与慢消费者告警）。
    fn topic(&self) -> &Topic;

    /// 复制订阅句柄（同一会话可开出多条独立的流）。
    fn clone_box(&self) -> Arc<dyn Subscription>;

    /// 转成事件流。实现方需自行消化底层错误（如 broadcast 的 Lagged
    /// 映射为日志告警后跳过），保证流不中断。
    fn into_stream(self: Arc<Self>) -> SubscriptionStream;
}

/// 事件流别名：模块只见 `Stream<Item = Event>` 抽象。
pub type SubscriptionStream = Pin<Box<dyn Stream<Item = Event> + Send>>;

/// 订阅能力：按 topic 建立订阅（发布/订阅、广播语义的接收端）。
pub trait Subscriber: Send + Sync {
    fn subscribe(&self, topic: Topic) -> Result<Arc<dyn Subscription>>;
}

/// 请求响应能力：事件驱动的请求/响应语义（调用方不感知 handler 是谁）。
#[async_trait]
pub trait Requester: Send + Sync {
    /// 向 topic 上注册的服务 handler 发起请求并等待响应。
    /// 失败路径：`NoHandler`（无人注册）、`Timeout`（超时）、
    /// `HandlerError`（对端业务错误）、`TypeMismatch`（载荷类型不符）。
    async fn request(&self, event: Event) -> Result<Event>;
}

/// 类型擦除的服务 handler。
///
/// tokio 惯用法：不用 async trait 方法，而是显式传入 `oneshot` 回话通道——
/// 实现方内部 `tokio::spawn` 异步逻辑后立即返回，天然对象安全且不阻塞分发。
pub trait ErasedHandler: Send + Sync {
    fn handle(&self, req: Event, reply: oneshot::Sender<Result<Event>>);
}

/// 模块自报能力的服务注册接口。装配层（app）收集各模块的注册。
pub trait ServiceRegistry: Send + Sync {
    fn register_handler(&self, topic: Topic, handler: Arc<dyn ErasedHandler>) -> Result<()>;
}

/// 强类型的业务事件处理函数：模块只需实现它，
/// 由 [`erase_handler`] 擦除后注册进 [`ServiceRegistry`]。
#[async_trait]
pub trait Handler<M, R>: Send + Sync
where
    M: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn handle(&self, msg: M) -> Result<R>;
}

/// 闭包式 handler：方便用一行闭包注册简单服务。
#[async_trait]
impl<M, R, F, Fut> Handler<M, R> for F
where
    M: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(M) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<R>> + Send + 'static,
{
    async fn handle(&self, msg: M) -> Result<R> {
        (self)(msg).await
    }
}

/// 把强类型 [`Handler`] 擦除为 [`ErasedHandler`]（请求/响应两端的 downcast 都在此完成）。
pub fn erase_handler<M, R, H>(handler: H) -> Arc<dyn ErasedHandler>
where
    M: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
    H: Handler<M, R> + 'static,
{
    struct Adapter<M, R, H> {
        inner: Arc<H>,
        _marker: std::marker::PhantomData<fn(M) -> R>,
    }

    impl<M, R, H> ErasedHandler for Adapter<M, R, H>
    where
        M: Send + Sync + Clone + 'static,
        R: Send + Sync + 'static,
        H: Handler<M, R> + 'static,
    {
        fn handle(&self, req: Event, reply: oneshot::Sender<Result<Event>>) {
            // downcast 在 spawn 之前完成：类型错误可同步反馈给请求方。
            // payload 是 Arc<dyn Any>，downcast 得到 Arc<M>，需 clone 出 M 交给 handler。
            let msg = match req.payload.downcast::<M>() {
                Ok(arc_msg) => (*arc_msg).clone(),
                Err(_) => {
                    let _ = reply.send(Err(crate::BusError::TypeMismatch {
                        expected: std::any::type_name::<M>(),
                        found: "<erased payload>".to_string(),
                    }));
                    return;
                }
            };
            let (topic, source) = (req.topic, req.source);
            let inner = Arc::clone(&self.inner);
            // handler 在独立任务中执行，不阻塞总线的分发循环
            tokio::spawn(async move {
                let outcome = match inner.handle(msg).await {
                    Ok(resp) => Ok(Event::new(topic, source, Event::payload_arc(resp))),
                    Err(e) => Err(e),
                };
                // 请求方可能已超时离开，send 失败可忽略
                let _ = reply.send(outcome);
            });
        }
    }

    Arc::new(Adapter {
        inner: Arc::new(handler),
        _marker: std::marker::PhantomData,
    })
}

/// 模块拿到的完整能力上下文——"具备发布能力的对象"的具体形态。
///
/// 全部字段都是 trait 对象句柄，模块通过它与外界通信，
/// 完全不知道（也不需要知道）对端是谁、底层用什么通道。
#[derive(Clone)]
pub struct ModuleContext {
    /// 本模块名（作为事件的 source 字段）。
    pub module_name: Arc<str>,
    /// 发布能力。
    pub publisher: Arc<dyn Publisher>,
    /// 订阅能力。
    pub subscriber: Arc<dyn Subscriber>,
    /// 请求响应能力。
    pub requester: Arc<dyn Requester>,
    /// 服务注册能力（模块自报事件处理函数）。
    pub registry: Arc<dyn ServiceRegistry>,
    /// 参数 get/set 客户端。
    pub params: Arc<dyn ParamClient>,
    /// 参数注册端（模块暴露自己的内部参数）。
    pub param_registry: Arc<dyn ParamProvider>,
    /// 大文件共享内存传输客户端。
    pub blobs: Arc<dyn BlobClient>,
}

impl ModuleContext {
    /// 以本模块名义构造一个事件信封。
    pub fn event(&self, topic: impl Into<Topic>, payload: crate::event::Payload) -> Event {
        Event::new(topic, Arc::clone(&self.module_name), payload)
    }

    /// 参数 topic 的 watch 接收端快捷方式。
    pub fn watch_param(&self, module: &str, key: &str) -> Result<watch::Receiver<crate::event::Value>> {
        // 具体 watch 通道由 ParamClient 实现方管理，这里做一层转发
        self.params.watch(module, key)
    }

    /// 创建共享内存 blob 写入端。
    pub fn blob_writer(&self, name: impl Into<String>, capacity_hint: usize) -> BlobWriter {
        self.blobs.create_writer(name.into(), capacity_hint)
    }

    /// 打开一条带背压的 blob 传输流。
    pub fn open_blob_stream(&self, name: impl Into<String>, chunk_size: usize, buf_chunks: usize) -> (BlobSink, BlobStream) {
        self.blobs.open_stream(name.into(), chunk_size, buf_chunks)
    }
}
