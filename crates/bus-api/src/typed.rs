//! 强类型封装：消除 `Any` downcast 样板。
//!
//! - [`Typed<M>`]：pub/sub 用的类型化 topic（只关心一种消息类型 M）。
//! - [`TypedService<Req, Resp>`]：请求/响应用的类型化服务端点（请求 Req、响应 Resp）。
//!
//! 二者都在内部完成 payload 的装箱与 downcast，业务代码只见具体 Rust 类型，
//! 拿到编译期类型安全；主路径零序列化（进程内 `Arc<dyn Any>`）。
//!
//! # 本文件的核心手法：Phantom Type（幽灵类型）+ 分层 impl block
//!
//! 两个结构体都只用一个字段存 `Topic`（运行时就是个字符串），泛型参数 `M`/`Req`/`Resp`
//! 在运行时**不存在**——它们只活在编译期，用 `PhantomData` 占位。这样做的目的是：
//! 用类型系统把"这个 topic 只能发 M 类型"这条约束固化进签名，让编译器拦住用错类型的调用。
//!
//! 另一个反复出现的模式是**把一个类型的 impl 拆成两块**：一块无约束（放 `new`/`topic`
//! 这类不碰 `M` 的方法），一块带 `where M: ...`（放 `publish`/`take` 这类真正用到 `M`
//! 的方法）。这样即使 `M` 不满足 `Send + 'static`，也仍能构造和查询 topic，只有用到
//! 载荷时才要求约束——把约束推迟到真正需要的地方，是泛型 API 设计的常见讲究。

use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::Result;
use crate::event::Event;
use crate::topic::Topic;
use crate::traits::{ErasedHandler, Handler, Publisher, Requester, erase_handler};

/// 类型化 topic，用于发布/订阅一种消息类型 `M`。
///
/// # 语法解读：`PhantomData<fn() -> M>` 为何这样写
///
/// `M` 在结构体里没有任何字段真正用到，但 Rust 要求"声明的泛型参数必须被使用"，
/// 否则报 `unused type parameter`。`PhantomData<T>` 就是专门用来"假装用到了 T"的
/// 零大小标记类型，编译后不占任何空间。
///
/// 为什么是 `fn() -> M` 而不是 `PhantomData<M>`？两者都能消除未使用报错，区别在
/// **变型（variance）与自动 trait 推导**：
/// - `PhantomData<M>` 表示"我逻辑上拥有一个 M"，会把 `M` 的 drop 检查、`Send`/`Sync`
///   约束牵连到 `Typed<M>` 上。
/// - `PhantomData<fn() -> M>` 表示"我能产出一个 M"（协变位置），且因为函数指针
///   不拥有数据，不会给 `Typed<M>` 强加 `M: Send/Sync` 之类的所有权语义。
/// 这里 `Typed` 并不真的持有 `M` 的值，只是把 `M` 当作编译期标签，所以用 `fn() -> M`
/// 最贴合语义、约束最少。（`TypedService` 用 `fn(Req) -> Resp`，同理表达"消费 Req、产出 Resp"。）
pub struct Typed<M> {
    topic: Topic,
    _marker: PhantomData<fn() -> M>,
}

// 无约束的 impl block：new / topic 不触碰 M 的具体行为，因此不需要 M: Send + 'static。
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

// 带约束的 impl block：以下方法要把 M 装进/取出 Arc<dyn Any>，因此要求 M: Send + Sync + 'static。
// 把它单独拆出来，就能让上面的 new/topic 对任意 M 可用——约束只加在真正需要的方法上。
impl<M> Typed<M>
where
    M: Send + Sync + 'static,
{
    /// 以 `source` 名义把消息装入事件信封。
    pub fn event(&self, source: impl Into<Arc<str>>, msg: M) -> Event {
        Event::new(self.topic.clone(), source, Event::payload_arc(msg))
    }

    /// 通过发布能力广播一条类型化消息（发布/订阅语义）。
    ///
    /// 参数 `publisher: &dyn Publisher`：这里用 `&dyn` 而非泛型 `P: Publisher`，
    /// 因为调用方持有的本就是 `Arc<dyn Publisher>`，取引用传入即可，无需再单态化。
    pub fn publish(&self, publisher: &dyn Publisher, source: impl Into<Arc<str>>, msg: M) -> Result<()> {
        publisher.publish(self.event(source, msg))
    }

    /// 从收到的事件中按类型取出载荷引用（downcast）。
    ///
    /// # 语法解读：方法级生命周期 `'a`
    ///
    /// `extract<'a>(&self, event: &'a Event) -> Result<&'a M>` 把返回引用的生命周期
    /// 绑定到**入参 `event`**（而非 `&self`）。这告诉编译器：返回的 `&M` 活多久取决于
    /// `event` 活多久，与 `self` 无关。因为 downcast 出的引用实际借自 `event.payload`，
    /// 这样标注才不会过度约束调用方。
    pub fn extract<'a>(&self, event: &'a Event) -> Result<&'a M> {
        event.payload_as::<M>()
    }

    /// 从收到的事件中克隆出载荷（要求 `M: Clone`）。
    ///
    /// # 语法解读：方法级 where 约束
    ///
    /// 注意 `where M: Clone` 写在**方法**上而非 impl block 上。impl block 已经要求
    /// `M: Send + Sync + 'static`，但 `Clone` 只有 `take` 这一个方法需要（`extract`
    /// 返回引用不需要）。把 `Clone` 约束下放到方法级，就避免了对不用 `take` 的 `M`
    /// 强加 `Clone`——这是"最小约束原则"的体现：约束加在最小的作用域上。
    pub fn take(&self, event: &Event) -> Result<M>
    where
        M: Clone,
    {
        event.payload_cloned::<M>()
    }
}

// Clone 手写而非 #[derive(Clone)]：derive 会给泛型参数加上 `M: Clone` 约束，
// 但 Typed<M> 实际只 clone topic（M 只是 PhantomData，无需 M: Clone）。
// 手写 impl 可以只对真正需要 clone 的字段加约束，避免对 M 提出多余要求。
impl<M> Clone for Typed<M> {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            _marker: PhantomData,
        }
    }
}

/// 类型化请求/响应服务端点：请求类型 `Req`，响应类型 `Resp`。
///
/// 与 [`Typed<M>`] 同理，`Req`/`Resp` 只存在于编译期。`PhantomData<fn(Req) -> Resp>`
/// 精确表达"消费一个 Req、产出一个 Resp"的型变关系（Req 在逆变位置、Resp 在协变位置）。
pub struct TypedService<Req, Resp> {
    topic: Topic,
    _marker: PhantomData<fn(Req) -> Resp>,
}

// 无约束 impl：构造与查路由键不碰 Req/Resp 的具体行为。
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

// 带约束 impl：request 要把 Req 装箱、把 Resp 拆箱，所以要求两者都 Send + Sync + Clone + 'static。
// （Clone 是因为请求/响应两端都从 Arc<dyn Any> 里 clone 出具体值，见 erase_handler 与 payload_cloned。）
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
    ///
    /// # 巧妙点：帮业务封装 turbofish
    ///
    /// 直接调 [`erase_handler`] 需要写 `erase_handler::<Req, Resp, H>(...)`（因为 M/R
    /// 无法从参数反推）。这里把 `Req`/`Resp` 从 `TypedService` 自身的类型参数直接带入，
    /// 调用方只需 `svc.erase(my_handler)`，类型全部自动对齐——这就是类型化端点把
    /// "类型信息集中定义、两处复用"的价值：发布端与注册端共享同一份 `Req`/`Resp`，不会写错。
    pub fn erase<H>(&self, handler: H) -> Arc<dyn ErasedHandler>
    where
        H: Handler<Req, Resp> + 'static,
    {
        erase_handler::<Req, Resp, H>(handler)
    }
}

// 同 Typed：手写 Clone 避免 derive 对 Req/Resp 强加 Clone 约束（它们只是 PhantomData）。
impl<Req, Resp> Clone for TypedService<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            _marker: PhantomData,
        }
    }
}
