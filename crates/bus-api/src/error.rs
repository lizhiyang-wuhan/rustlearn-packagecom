//! 统一错误类型。

use thiserror::Error;

/// 总线统一错误类型，覆盖四种通信语义的全部失败路径。
///
/// # 设计意图：为什么用一个枚举统一所有错误
///
/// 把 pub/sub、请求/响应、参数、blob 的所有失败情形收拢到一个 `BusError` 枚举，
/// 而不是每个模块自定义错误。好处：调用方用 `Result<T>`（下方别名）就能处理所有总线错误，
/// 无需层层 `?` 转换错误类型；且每个 variant 携带足够的上下文（topic/expected/found 等），
/// 便于定位问题。
///
/// # 语法解读：`#[derive(Error)]` 与 `#[error("...")]`
///
/// - `#[derive(Error)]`（来自 `thiserror`）自动为本类型实现 `std::error::Error` trait，
///   免去手写 `Display`/`Error` 的样板。
/// - 每个 variant 上的 `#[error("...")]` 定义它的 `Display` 输出，字符串里的
///   `{topic}`/`{0}` 会取对应字段的值：具名 variant 用 `{字段名}`，元组 variant 用 `{0}`。
/// - variant 有两种形态：**具名字段**（如 `NoHandler { topic: String }`，字段多、语义清楚）
///   与**元组字段**（如 `Closed(String)`、`NoSubscriber(String)`，只带一个值时更简洁）。
///
/// # 一个细节：`TypeMismatch` 为何 `expected: &'static str` 而 `found: String`
///
/// `expected` 来自 `std::any::type_name::<T>()`，它返回 `&'static str`（编译期已知的类型名），
/// 直接存静态引用即可；`found` 往往是运行时拼出的描述，所以用拥有的 `String`。两者类型不同
/// 正好反映了"期望类型编译期已知、实际类型运行时才知"的语义差异。
#[derive(Debug, Error)]
pub enum BusError {
    /// 请求的 topic 上没有任何注册的服务 handler。
    #[error("no handler registered for topic '{topic}'")]
    NoHandler { topic: String },

    /// 同一条请求路由 topic 被注册了两次（模块能力冲突，装配期即暴露）。
    #[error("handler for topic '{topic}' is already registered")]
    DuplicateHandler { topic: String },

    /// handler 内部返回的业务错误。
    #[error("handler error on topic '{topic}': {message}")]
    HandlerError { topic: String, message: String },

    /// 消息载荷的实际类型与期望类型不符。
    #[error("payload type mismatch: expected `{expected}`, found `{found}`")]
    TypeMismatch { expected: &'static str, found: String },

    /// 请求响应超时。
    #[error("request to topic '{topic}' timed out")]
    Timeout { topic: String },

    /// 总线已关闭 / 对端已丢弃。
    #[error("bus closed: {0}")]
    Closed(String),

    /// 参数不存在（模块或 key 未注册）。
    #[error("param not found: {module}.{key}")]
    ParamNotFound { module: String, key: String },

    /// 发布/订阅通道上没有订阅者（broadcast send 无人接收）。
    #[error("no subscriber on topic '{0}'")]
    NoSubscriber(String),

    /// 订阅者消费过慢，广播环形缓冲被覆盖而丢帧（仅告警性质，流会继续）。
    #[error("slow consumer lagged on topic '{topic}', {skipped} events dropped")]
    SlowConsumer { topic: String, skipped: u64 },
}

/// 统一 Result 别名：把错误类型固定为 [`BusError`]，让全项目签名更简洁。
///
/// 这是库设计的常见惯例（标准库也有 `std::io::Result`）：定义 `type Result<T> = ...`
/// 后，各方法只需写 `Result<T>` 而非 `std::result::Result<T, BusError>`。
pub type Result<T> = std::result::Result<T, BusError>;
