//! 总线能力抽象：跨 crate 传递的是这些 trait 对象（"具备发布能力的对象"），
//! 而不是任何具体通道。模块依赖抽象，运行时实现可替换。
//!
//! # 本文件的设计哲学（阅读前先看这里）
//!
//! 这里定义的每个 trait 都是"能力接口"（capability interface）：只描述"能做什么"，
//! 不描述"怎么做"。架构上的关键考量有三条：
//!
//! 1. **对象安全（object safety）优先**：因为最终都要以 `Arc<dyn Trait>` 形式跨 crate
//!    传递，所以每个 trait 都必须是对象安全的。这直接决定了下面很多签名的写法——
//!    例如为什么 `Handler` 用 `#[async_trait]`（把 `async fn` 擦成返回 `Pin<Box<dyn Future>>`），
//!    而 `ErasedHandler` 干脆不用 async（改用显式 `oneshot` 回话通道）。
//! 2. **`Send + Sync` 作为 supertrait**：几乎所有 trait 都写成 `trait Foo: Send + Sync`。
//!    因为实现最终要被 `tokio::spawn` 到多线程运行时里、被多个模块共享（`Arc`），
//!    编译器必须能在 `dyn Foo` 上推断出 `Send`/`Sync`。把它放进 supertrait，
//!    就等于在契约层面强制"任何实现都必须能跨线程共享"。
//! 3. **泛型只在"编译期已知类型"的地方出现**：`Handler<M, R>`、`Typed<M>` 这类
//!    面向业务、类型在编译期确定的接口才用泛型；而进入总线路由表后必须类型擦除，
//!    于是有了 `erase_handler` 这个"泛型世界 → dyn 世界"的桥梁。

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
///
/// # 语法解读：`trait Publisher: Send + Sync`
///
/// 冒号后的 `Send + Sync` 是 **supertrait（父 trait）约束**，读作"任何 `Publisher`
/// 实现者必须同时是 `Send + Sync`"。这样写的好处：持有 `Arc<dyn Publisher>` 的代码
/// 无需再写 `Arc<dyn Publisher + Send + Sync>`——约束已经内建进契约。架构考量是
/// "能力接口天生要能跨线程共享"，与其在每个使用点重复标注，不如在定义处一次说清。
///
/// # 为什么 `publish` 是同步 `fn` 而不是 `async fn`
///
/// 底层 `tokio::sync::broadcast::send` 是同步非阻塞的（放进环形缓冲即返回），
/// 没有 `.await` 点，因此没必要 async。保持同步签名让 trait 天然对象安全，
/// 也省去 `#[async_trait]` 的装箱开销——这是"按实际需要选择同步/异步"的典型取舍。
pub trait Publisher: Send + Sync {
    fn publish(&self, event: Event) -> Result<()>;
}

/// 一次订阅会话。底层是 broadcast receiver 还是 mpsc，对模块不可见。
///
/// # 巧妙点：`clone_box` 是"对象安全的 clone"手工实现
///
/// `Clone` trait 不是对象安全的（它的 `fn clone(&self) -> Self` 返回 `Self`，
/// 大小编译期未知），所以 `dyn Subscription` 无法直接 `.clone()`。惯用解法就是
/// 定义一个返回 `Arc<dyn Subscription>` 的 `clone_box`，由实现方在"还知道具体类型"
/// 时完成复制，再擦回 trait 对象。这是 Rust 里给 trait 对象补 clone 能力的标准手法。
///
/// # `into_stream(self: Arc<Self>)` 的接收者语法
///
/// 参数写成 `self: Arc<Self>` 而非 `&self`/`self`，表示"调用时消费一个 `Arc<Self>`"。
/// 之所以要拿到 `Arc` 所有权，是因为返回的流需要持有订阅句柄存活到迭代结束——
/// 用 `Arc<Self>` 既能让流拥有会话，又不阻止调用方在别处保留同一会话的其他 `Arc`。
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
///
/// # 语法解读：为什么是 `Pin<Box<dyn Stream<Item = Event> + Send>>`
///
/// 三层包装各有其必要性，缺一不可：
/// - `dyn Stream`：类型擦除。不同 topic 的流底层实现不同（BroadcastStream 等），
///   擦成 `dyn` 才能用同一个类型别名统一返回，且让 trait 保持对象安全。
/// - `Box<...>`：`dyn Stream` 是 unsized（大小未知），不能直接作为返回类型放栈上，
///   必须装箱到堆，`Box` 提供所有权与固定大小的指针。
/// - `Pin<...>`：`Stream` 的 `poll_next` 要求 `Pin<&mut Self>`——异步流可能是
///   自引用的（内部 Future 指向自己的字段），`Pin` 保证它不会被移动，是 async
///   生态里返回 `dyn Future`/`dyn Stream` 的固定搭配。
/// - `+ Send`：流会被 `tokio::spawn` 到别的线程上驱动，必须 `Send`。
pub type SubscriptionStream = Pin<Box<dyn Stream<Item = Event> + Send>>;

/// 订阅能力：按 topic 建立订阅（发布/订阅、广播语义的接收端）。
///
/// 返回 `Arc<dyn Subscription>` 而非 `impl Subscription`：因为本 trait 自身要作为
/// `Arc<dyn Subscriber>` 存进 [`ModuleContext`]，返回值也必须是可以类型擦除的
/// trait 对象（`impl Trait` 在 trait 方法返回位置会带来对象安全问题）。
pub trait Subscriber: Send + Sync {
    fn subscribe(&self, topic: Topic) -> Result<Arc<dyn Subscription>>;
}

/// 请求响应能力：事件驱动的请求/响应语义（调用方不感知 handler 是谁）。
///
/// # 语法解读：`#[async_trait]` 在这里做了什么
///
/// Rust 稳定版一度不允许 trait 里直接写 `async fn`（返回 `impl Future` 的方法
/// 不是对象安全的，无法 `dyn`）。`async_trait` 宏把
/// `async fn request(&self, e: Event) -> Result<Event>`
/// 改写成
/// `fn request<'a>(&'a self, e: Event) -> Pin<Box<dyn Future<Output=Result<Event>> + Send + 'a>>`，
/// 即"返回一个装箱的、可跨线程的动态 Future"，从而恢复对象安全。
/// 代价是每次调用一次堆分配（装箱）——对低频的请求/响应完全可接受，
/// 这也是为什么高频的 `publish` 反而坚持用同步签名、不加这个宏。
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
///
/// # 为什么这里故意不用 `async fn`（与 `Requester` 对比）
///
/// 这是本文件最值得体会的一处设计对比。`Requester::request` 用 `#[async_trait]` 是因为
/// 调用方需要 `.await` 等结果；而 `ErasedHandler::handle` 故意用**同步签名 + 回话通道**，
/// 原因是：
/// 1. **不阻塞分发循环**：总线从路由表取出 handler 后调 `handle`，handler 内部
///    `tokio::spawn` 异步逻辑后**立即返回**，总线可以马上处理下一个请求。如果
///    `handle` 是 async 且总线直接 `.await` 它，一个慢 handler 就会卡住整条分发链路。
/// 2. **天然对象安全**：同步 `fn` 不涉及 `impl Future` 返回类型，无需 `#[async_trait]`
///    的装箱，也避免了对 `dyn ErasedHandler` 做动态分发的额外麻烦。
/// 3. **超时/取消更好做**：请求方拿到的是 `oneshot::Receiver`，可以用
///    `tokio::time::timeout` 包住它；即使请求方超时离开，handler 侧 `reply.send`
///    失败也可忽略（见 `erase_handler` 里的 `let _ = reply.send(...)`）。
///
/// `oneshot::Sender<Result<Event>>` 就是"回话通道"：handler 算完后用它把结果发回，
/// 一发即弃，正好匹配请求/响应的一问一答语义。
pub trait ErasedHandler: Send + Sync {
    fn handle(&self, req: Event, reply: oneshot::Sender<Result<Event>>);
}

/// 模块自报能力的服务注册接口。装配层（app）收集各模块的注册。
///
/// 注意参数是 `Arc<dyn ErasedHandler>` 而非泛型 `H: ErasedHandler`：因为注册表要在
/// 运行时存下"任意类型"的 handler（`HashMap<Topic, Arc<dyn ErasedHandler>>`），
/// 必须在接口边界就完成类型擦除，所以这里收的是 trait 对象而不是泛型。
pub trait ServiceRegistry: Send + Sync {
    fn register_handler(&self, topic: Topic, handler: Arc<dyn ErasedHandler>) -> Result<()>;
}

/// 强类型的业务事件处理函数：模块只需实现它，
/// 由 [`erase_handler`] 擦除后注册进 [`ServiceRegistry`]。
///
/// # 语法解读：`Handler<M, R>` 的泛型参数与 where 约束
///
/// 这是本文件第一处"业务侧泛型"。逐个拆解声明：
///
/// ```text
/// pub trait Handler<M, R>: Send + Sync
/// where
///     M: Send + Sync + 'static,   // 请求消息类型
///     R: Send + Sync + 'static,   // 响应消息类型
/// ```
///
/// - **`M` / `R` 是类型参数**：分别代表 handler 的输入消息与输出响应类型。
///   业务写 `impl Handler<ReadNow, TemperatureEvent> for ...` 时把它们实例化。
/// - **`where` 子句 vs 尖括号内联约束**：这里用 `where` 把约束单独列出，而不是写成
///   `Handler<M: Send + Sync + 'static, R: ...>`。两种写法等价，但当约束较长时
///   `where` 更清晰——这是架构者可读性优先的选择。
/// - **为什么每个 bound 都必要**：
///   - `Send + Sync`：消息要经 `Arc<dyn Any>` 装进 `Event`，跨线程传给 handler 任务，
///     必须能安全跨线程共享。
///   - `'static`：消息要装进 `dyn Any`（`Any: 'static`），且会被 `tokio::spawn`
///     到独立任务（任务要求 `'static`，不能借用栈上数据）。所以消息必须是自拥有的、
///     不含非 `'static` 引用的类型。
///
/// # 为什么 `Handler` 用 `#[async_trait]` 而 `ErasedHandler` 不用
///
/// `Handler` 是**业务面向**的接口，业务逻辑天然异步（要查库、要 IO），写成 `async fn`
/// 最自然；而它是泛型的、编译期单态化的，`#[async_trait]` 的装箱开销在这里可接受。
/// `ErasedHandler` 是**总线面向**的接口，要进路由表被高频动态分发，所以用同步 + spawn。
/// 两者通过 `erase_handler` 衔接——这正是"业务用泛型求类型安全，总线用 dyn 求统一存储"的分层。
#[async_trait]
pub trait Handler<M, R>: Send + Sync
where
    M: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    async fn handle(&self, msg: M) -> Result<R>;
}

/// 闭包式 handler：方便用一行闭包注册简单服务。
///
/// # 语法解读：这是一个 blanket implementation（毯式实现）
///
/// 所谓 blanket impl，就是"为所有满足某组约束的类型"一次性实现某 trait，而不是
/// 为某个具体类型实现。它的威力在于：业务方**无需手写 `impl Handler<..> for MyStruct`**，
/// 只要传一个签名匹配的闭包，编译器就自动认定它是 `Handler`。
///
/// 逐个拆解这段（本项目泛型约束最密的声明之一）：
///
/// ```text
/// impl<M, R, F, Fut> Handler<M, R> for F
/// where
///     M: Send + Sync + 'static,                              // 输入消息
///     R: Send + Sync + 'static,                              // 输出响应
///     F: Fn(M) -> Fut + Send + Sync + 'static,               // 闭包本身
///     Fut: std::future::Future<Output = Result<R>> + Send + 'static, // 闭包返回的 Future
/// ```
///
/// - **`for F`**：被实现 `Handler` 的类型是 `F`——也就是"任何可调用对象"（闭包/函数指针）。
/// - **`F: Fn(M) -> Fut`**：约束 `F` 必须是"接收 `M`、返回 `Fut`"的可调用体。用 `Fn`
///   （而非 `FnOnce`/`FnMut`）是因为 handler 会被多次调用，且 `&self` 调用不消费闭包。
/// - **`Fut: Future<Output = Result<R>>`**：闭包返回的是一个 Future（因为业务是 async 的），
///   其产出必须是 `Result<R>`，正好对上 `Handler::handle` 的返回类型。
/// - **为什么 `Fut` 要单独作为一个类型参数**：`async` 闭包/块返回的 Future 是编译器
///   生成的匿名类型，无法手写名字，只能用泛型参数 `Fut` 把它"接住"，再用 where 约束它的行为。
/// - **`Send + 'static` 一路传染**：因为最终 `handle` 内会 `.await` 这个 Future，
///   而整个调用可能发生在 `tokio::spawn` 的多线程上下文，所以 `F` 和 `Fut` 都得 `Send + 'static`。
///
/// # 巧妙所在
///
/// 这段 blanket impl 让"注册一个简单服务"从"定义 struct + impl trait + 写 async 方法"
/// 三步压缩成一行闭包，同时**没有牺牲任何类型安全**——`M`/`R` 仍由闭包签名在编译期锁定。
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
///
/// # 这是"泛型世界 → dyn 世界"的桥梁，也是全项目最巧妙的一段
///
/// 业务侧是强类型的 `Handler<M, R>`（编译期单态化），但总线路由表需要统一的
/// `Arc<dyn ErasedHandler>`（运行时多态）。本函数就是两个世界的转换点：它吃掉
/// 一个具体类型的 handler，吐出一个类型擦除的 handler，并把两端的 downcast/upcast
/// 封装在生成的适配器里，让业务代码永远不碰 `Any`。
///
/// # 语法解读：函数级泛型参数与 turbofish
///
/// ```text
/// pub fn erase_handler<M, R, H>(handler: H) -> Arc<dyn ErasedHandler>
/// where
///     M: Send + Sync + Clone + 'static,   // 请求类型
///     R: Send + Sync + 'static,           // 响应类型
///     H: Handler<M, R> + 'static,         // 具体 handler 类型
/// ```
///
/// - **`<M, R, H>` 是函数的泛型参数**，与 trait 的泛型参数写法一致，只是作用域限在本函数。
/// - **`M: ... + Clone`**：注意 `M` 比 `Handler` 里多了个 `Clone`。因为下面要从
///   `Arc<M>` 里 `(*arc_msg).clone()` 拷一份 `M` 交给 handler（handler 要拥有所有权），
///   所以调用点需要 `M: Clone`。这是一个"因为实现细节而额外加的约束"的典型例子。
/// - **`H: Handler<M, R>`**：把 `H` 与 `M`/`R` 关联起来——编译器由此知道"这个 `H`
///   处理的是 `M` 进 `R` 出"，从而在适配器里正确 downcast。
/// - **调用时的 turbofish**：因为 `M`/`R` 无法从参数 `handler: H` 反推出来（类型信息
///   在 `H` 内部），调用方往往需要显式指定，如 `erase_handler::<ReadNow, TemperatureEvent, _>(h)`
///   （`_` 让编译器自己推 `H`）。`TypedService::erase` 就是帮业务把这个 turbofish 包好了。
///
/// # 内部 `Adapter` 的设计：为什么需要它
///
/// 返回类型是 `Arc<dyn ErasedHandler>`，但 `H` 只实现了 `Handler<M, R>`、没实现
/// `ErasedHandler`，两者签名不同（一个吃 `M` 一个吃 `Event`）。所以要在中间插一个
/// 适配器 `Adapter<M, R, H>`，由它实现 `ErasedHandler`，内部持有 `Arc<H>` 并负责
/// `Event <-> M/R` 的转换。这是标准的 Adapter 模式。
pub fn erase_handler<M, R, H>(handler: H) -> Arc<dyn ErasedHandler>
where
    M: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
    H: Handler<M, R> + 'static,
{
    // 适配器：持有强类型 handler，对外表现为类型擦除的 ErasedHandler。
    struct Adapter<M, R, H> {
        inner: Arc<H>,
        // PhantomData 告诉编译器"Adapter 逻辑上与 M、R 相关"（否则未使用的泛型
        // 参数 M/R 会报错）。用 `fn(M) -> R` 而非 `PhantomData<(M, R)>`，是为了精确
        // 表达"消费 M、产出 R"的变型关系，且不引入不必要的所有权语义。
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
                    // 类型不匹配：不 spawn，直接用回话通道把 TypeMismatch 发回。
                    // 放在 spawn 前是为了让错误能"同步"返回，而不是淹没在异步任务里。
                    let _ = reply.send(Err(crate::BusError::TypeMismatch {
                        expected: std::any::type_name::<M>(),
                        found: "<erased payload>".to_string(),
                    }));
                    return;
                }
            };
            // topic/source 先取出，因为 req.payload 已被上面 downcast 消费（部分移动）。
            let (topic, source) = (req.topic, req.source);
            // clone 一个 Arc<H> 移进任务：避免把 &self 借用到 'static 任务里。
            let inner = Arc::clone(&self.inner);
            // handler 在独立任务中执行，不阻塞总线的分发循环
            tokio::spawn(async move {
                let outcome = match inner.handle(msg).await {
                    // 出站 upcast：把强类型响应 R 装回 Arc<dyn Any>，重建 Event 信封。
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
///
/// # 设计意图：能力注入（dependency injection）的载体
///
/// 这个 struct 是整个"依赖倒置"的落地点：装配层（`main.rs`）把总线的各种能力
/// 以 `Arc<dyn Trait>` 的形式装进来，再注入给每个模块。模块拿到的是抽象句柄，
/// 而不是具体的 `MessageBus`——于是底层实现可替换（进程内 broadcast 换成跨进程 TCP），
/// 模块代码零修改。每个字段都是 `Arc`，所以 `ModuleContext` 可廉价 `clone`（只增引用计数），
/// 方便模块在 `init` 时 clone 一份存起来、`run` 时取用。
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
    ///
    /// # 语法解读：`impl Into<Topic>` 参数
    ///
    /// `topic: impl Into<Topic>` 是"接受任何能转成 `Topic` 的类型"的简写（等价于
    /// 一个匿名泛型 `<T: Into<Topic>>`）。因为 `Topic` 实现了 `From<&str>`/`From<String>`，
    /// 调用方可以直接传 `"sensor.temperature"` 字符串字面量，无需手动 `Topic::new(...)`。
    /// 这是 API 易用性的常见手法：用 `impl Into<T>` 让调用方少写转换样板。
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
