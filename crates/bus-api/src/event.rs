//! 事件信封与载荷类型。

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use crate::Topic;

/// 类型擦除后的消息载荷：`Any` 为主路径（零序列化、进程内零拷贝）。
///
/// 通过 [`crate::typed`] 的强类型封装使用，业务代码几乎不需要直接接触 downcast。
pub type Payload = Arc<dyn Any + Send + Sync>;

/// 事件信封。可廉价 clone（载荷是 `Arc`），因此适合 broadcast 多订阅者分发。
#[derive(Clone)]
pub struct Event {
    /// 路由键。
    pub topic: Topic,
    /// 事件来源模块名（用于日志追踪与调试）。
    pub source: Arc<str>,
    /// 类型擦除的载荷。
    pub payload: Payload,
}

impl Event {
    /// 构造事件信封。
    pub fn new(topic: impl Into<Topic>, source: impl Into<Arc<str>>, payload: Payload) -> Self {
        Self {
            topic: topic.into(),
            source: source.into(),
            payload,
        }
    }

    /// 把任意 `Send + Sync + 'static` 消息装入载荷（Arc 化）。
    pub fn payload_arc<T: Send + Sync + 'static>(msg: T) -> Payload {
        Arc::new(msg)
    }

    /// 按期望类型取出载荷的共享引用。
    pub fn payload_as<T: Send + Sync + 'static>(&self) -> crate::Result<&T> {
        self.payload.downcast_ref::<T>().ok_or(crate::BusError::TypeMismatch {
            expected: std::any::type_name::<T>(),
            found: PAYLOAD_TYPE_NAME.to_string(),
        })
    }

    /// 按期望类型克隆出载荷（要求消息类型 `Clone`）。
    pub fn payload_cloned<T: Send + Sync + Clone + 'static>(&self) -> crate::Result<T> {
        self.payload_as::<T>().cloned()
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("topic", &self.topic)
            .field("source", &self.source)
            .field("payload", &PAYLOAD_TYPE_NAME)
            .finish()
    }
}

/// `dyn Any` 无法在稳定 Rust 上反查类型名，类型不匹配时统一报告为擦除载荷。
const PAYLOAD_TYPE_NAME: &str = "<erased payload>";

/// 参数值：跨 crate 传递参数时使用的可序列化枚举（get/set 语义的载体）。
///
/// 与 `Payload`（Any 主路径）互补：参数是低频、小体积、需要跨模块约定格式的数据，
/// 用封闭枚举可以避免 downcast，也天然对齐未来 serde 跨进程路径。
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde-payload", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Bytes),
    /// JSON 文档（`serde_json::Value` 的字符串形态，避免强依赖 serde_json）。
    Json(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::U64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v:?}"),
            Value::Bytes(v) => write!(f, "<{} bytes>", v.len()),
            Value::Json(v) => write!(f, "{v}"),
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}
impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::U64(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::F64(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
impl From<Bytes> for Value {
    fn from(v: Bytes) -> Self {
        Value::Bytes(v)
    }
}
