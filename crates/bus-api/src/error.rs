//! 统一错误类型。

use thiserror::Error;

/// 总线统一错误类型，覆盖四种通信语义的全部失败路径。
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

pub type Result<T> = std::result::Result<T, BusError>;
