//! 事件信封与载荷类型。
//!
//! # 本文件的设计核心：类型擦除的"信封 + 载荷"模型
//!
//! 总线要能传输**任意类型**的消息，但一个 `broadcast<Event>` 通道只能装同一种类型。
//! 解法是把具体消息装进 `Arc<dyn Any>`（载荷），外面包一层固定结构的 `Event`（信封）。
//! 信封的形状编译期已知（topic + source + payload），载荷的类型则被擦除到运行时。
//! 发送端装箱（upcast）、接收端拆箱（downcast），两端的具体类型检查由 [`crate::typed`] 封装。

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use crate::Topic;

/// 类型擦除后的消息载荷：`Any` 为主路径（零序列化、进程内零拷贝）。
///
/// 通过 [`crate::typed`] 的强类型封装使用，业务代码几乎不需要直接接触 downcast。
///
/// # 语法解读：`Arc<dyn Any + Send + Sync>` 每一部分的作用
///
/// - `dyn Any`：类型擦除的核心。`Any` 是标准库 trait，为所有 `'static` 类型自动实现，
///   提供 `downcast_ref::<T>()` 等"运行时类型识别 + 安全转回具体类型"的能力。
/// - `+ Send + Sync`：`dyn Any` 默认不保证跨线程安全，显式加上这两个 auto trait，
///   载荷才能随 `Event` 被 `tokio::spawn` 到别的线程、被多个订阅者共享。
/// - `Arc<...>`：`dyn Any` 是 unsized，必须放在指针后；用 `Arc`（而非 `Box`）是因为
///   `Event` 要能被 broadcast **廉价 clone 给多个订阅者**，`Arc` 的 clone 只增引用计数、
///   共享同一份载荷，零拷贝。这也是为什么 `Event` 能 `#[derive(Clone)]` 却不贵。
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
    ///
    /// 两个 `impl Into<...>` 参数让调用方可直接传字面量：`topic` 收 `&str`/`String`/`Topic`，
    /// `source` 收 `&str`/`String`/`Arc<str>`，无需手动转换。
    pub fn new(topic: impl Into<Topic>, source: impl Into<Arc<str>>, payload: Payload) -> Self {
        Self {
            topic: topic.into(),
            source: source.into(),
            payload,
        }
    }

    /// 把任意 `Send + Sync + 'static` 消息装入载荷（Arc 化）。
    ///
    /// # 语法解读：这就是"upcast"发生的地方
    ///
    /// `Arc::new(msg)` 本身只产生 `Arc<T>`，但因为返回类型是 `Payload`（即 `Arc<dyn Any + Send + Sync>`），
    /// 编译器在返回位置自动插入一次 **unsizing coercion**：把瘦指针 `Arc<T>` 胖化为
    /// `Arc<dyn Any>`，并附加一个记录 `TypeId`、drop 方式的 vtable。堆上的数据一个字节不变。
    ///
    /// 泛型约束 `<T: Send + Sync + 'static>` 的必要性：
    /// - `Send + Sync`：要匹配 `Payload` 别名里的 `dyn Any + Send + Sync`。
    /// - `'static`：`Any` trait 要求 `Self: 'static`，且 `Arc<T>` 要能转 `Arc<dyn Any>`，
    ///   `T` 必须不含非 `'static` 引用。
    pub fn payload_arc<T: Send + Sync + 'static>(msg: T) -> Payload {
        Arc::new(msg)
    }

    /// 按期望类型取出载荷的共享引用。
    ///
    /// # 语法解读：这就是"downcast"，与 `payload_arc` 相反
    ///
    /// `self.payload.downcast_ref::<T>()` 比较载荷 vtable 里的 `TypeId` 与 `T` 的 `TypeId`：
    /// - 相等 → `Some(&T)`，安全地借出具体类型引用（不拷贝、不转移所有权）。
    /// - 不等 → `None`，这里用 `ok_or` 转成 `TypeMismatch` 错误，把"运行时才能发现的
    ///   类型错误"包装成显式的 `Result`，而不是 panic。
    ///
    /// 返回 `&T`（而非 `T`）意味着不要求 `T: Clone`，适合只读场景；需要拿走所有权时用
    /// [`payload_cloned`]。
    pub fn payload_as<T: Send + Sync + 'static>(&self) -> crate::Result<&T> {
        self.payload.downcast_ref::<T>().ok_or(crate::BusError::TypeMismatch {
            expected: std::any::type_name::<T>(),
            found: PAYLOAD_TYPE_NAME.to_string(),
        })
    }

    /// 按期望类型克隆出载荷（要求消息类型 `Clone`）。
    ///
    /// # 语法解读：泛型约束为何比 `payload_as` 多一个 `Clone`
    ///
    /// `payload_as` 返回 `&T`，不需要 `Clone`；而本方法要返回**拥有的** `T`，只能从
    /// `&T` 克隆一份，所以额外要求 `T: Clone`。这是"约束随方法语义变化"的直观例子：
    /// 多要一个能力（所有权），就多一个 bound。内部就是 `payload_as::<T>().cloned()`。
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
///
/// # 设计权衡：为什么参数不用 `Arc<dyn Any>` 而用封闭枚举
///
/// 事件载荷用 `dyn Any` 是因为消息类型开放、且追求零拷贝；但参数不同：
/// - 参数种类**天然有限**（bool/整数/浮点/字符串/字节/JSON），封闭枚举完全够用；
/// - 参数需要**可打印、可展示**（日志/调试面板），枚举能直接实现 `Display`，而 `dyn Any` 不能；
/// - 参数未来可能要**跨进程序列化**，枚举能派生 `Serialize`，而 `dyn Any` 不能；
/// - 用枚举 `match` 能**穷尽所有情况**，编译期保证不漏处理，而 downcast 只能运行时才发现类型错。
///
/// # 语法解读：`#[cfg_attr(feature = "serde-payload", derive(...))]`
///
/// 这是**条件属性**：只有启用 `serde-payload` feature 时，才把 `derive(Serialize, Deserialize)`
/// 应用到 `Value` 上。为什么不直接 `derive`？因为 `serde` 是可选依赖（`optional = true`），
/// 默认不引入；若直接 derive，默认编译时 `serde` 不存在就会报错。`cfg_attr` 让序列化能力
/// 按需开启，不增加默认编译的依赖负担——这是库设计"可选特性"的标准手法。
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

// 一组 From 实现：让常见原生类型能 `into()` 成 Value，调用方可写 `Value::from(500u64)`
// 或 `500u64.into()`，而不必每次手写 `Value::U64(500)`。这是 newtype/枚举的人性化惯例：
// 为每种合法输入实现 From，把构造细节藏在转换里。注意没有 From<Value>（会与之冲突），
// 也没有为 i32/u32 等实现（避免歧义），只选了参数场景最常用的几种宽度。
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
