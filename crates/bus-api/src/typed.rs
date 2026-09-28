//! 强类型封装：消除 `Any` downcast 样板。
//!
//! - [`Typed<M>`]：pub/sub 用的类型化 topic（只关心一种消息类型 M）。
//! - [`TypedService<Req, Resp>`]：请求/响应用的类型化服务端点（请求 Req、响应 Resp）。
//!
//! 二者都在内部完成 payload 的装箱与 downcast，业务代码只见具体 Rust 类型，
//! 拿到编译期类型安全；主路径零序列化（进程内 `Arc<dyn Any>`）。

use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::Result;
use crate::event::Event;
use crate::topic::Topic;
use crate::traits::{ErasedHandler, Handler, Publisher, Requester, erase_handler};

/// 类型化 topic，用于发布/订阅一种消息类型 `M`。
pub struct Typed<M> {
    topic: Topic,
    _marker: PhantomData<fn() -> M>,
}

impl<M> Typed<M> {
    /// 用路由键构造类型化 topic。
    pub fn new(topic: impl Into<Topic>) -> Self {
        Self {
            topic: topic.into(),
            _marker: PhantomData,
        }
    }

    /// 底层路由键。
    pub fn topic(&self) -> &Topic {
        &self.topic
    }
}

impl<M> Typed<M>
where
    M: Send + Sync + 'static,
{
    /// 以 `source` 名义把消息装入事件信封。
    pub fn event(&self, source: impl Into<Arc<str>>, msg: M) -> Event {
        Event::new(self.topic.clone(), source, Event::payload_arc(msg))
    }

    /// 通过发布能力广播一条类型化消息（发布/订阅语义）。
    pub fn publish(&self, publisher: &dyn Publisher, source: impl Into<Arc<str>>, msg: M) -> Result<()> {
        publisher.publish(self.event(source, msg))
    }

    /// 从收到的事件中按类型取出载荷引用（downcast）。
    pub fn extract<'a>(&self, event: &'a Event) -> Result<&'a M> {
        event.payload_as::<M>()
    }

    /// 从收到的事件中克隆出载荷（要求 `M: Clone`）。
    pub fn take(&self, event: &Event) -> Result<M>
    where
        M: Clone,
    {
        event.payload_cloned::<M>()
    }
}

impl<M> Clone for Typed<M> {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            _marker: PhantomData,
        }
    }
}

/// 类型化请求/响应服务端点：请求类型 `Req`，响应类型 `Resp`。
pub struct TypedService<Req, Resp> {
    topic: Topic,
    _marker: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> TypedService<Req, Resp> {
    /// 用路由键构造类型化服务端点。
    pub fn new(topic: impl Into<Topic>) -> Self {
        Self {
            topic: topic.into(),
            _marker: PhantomData,
        }
    }

    /// 底层路由键（服务注册用）。
    pub fn topic(&self) -> &Topic {
        &self.topic
    }
}

impl<Req, Resp> TypedService<Req, Resp>
where
    Req: Send + Sync + Clone + 'static,
    Resp: Send + Sync + Clone + 'static,
{
    /// 发起类型化请求并等待类型化响应（请求/响应语义）。
    pub async fn request(
        &self,
        requester: &dyn Requester,
        source: impl Into<Arc<str>>,
        req: Req,
    ) -> Result<Resp> {
        let event = Event::new(self.topic.clone(), source, Event::payload_arc(req));
        let resp_event = requester.request(event).await?;
        resp_event.payload_cloned::<Resp>()
    }

    /// 把一个强类型 [`Handler<Req, Resp>`] 擦除为可注册的 [`ErasedHandler`]。
    pub fn erase<H>(&self, handler: H) -> Arc<dyn ErasedHandler>
    where
        H: Handler<Req, Resp> + 'static,
    {
        erase_handler::<Req, Resp, H>(handler)
    }
}

impl<Req, Resp> Clone for TypedService<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            _marker: PhantomData,
        }
    }
}
