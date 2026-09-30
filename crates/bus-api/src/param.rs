//! 参数请求/响应（get/set 语义）的抽象接口。
//!
//! 参数 topic 约定使用保留前缀 `param://{module}/{key}`，
//! 由运行时的 ParamStore 统一路由，模块之间不直接通信。

use async_trait::async_trait;
use tokio::sync::watch;

use crate::error::Result;
use crate::event::Value;

/// 参数值类型别名。
///
/// 参数 get/set 与 serde 可选路径共用同一个封闭枚举 [`Value`]；
/// 在参数语境下以 `ParamValue` 之名暴露，语义更贴切（Bool/I64/U64/F64/String/Bytes/Json）。
///
/// # 语法解读：`type ParamValue = Value;` 是类型别名而非新类型
///
/// 这只是给 `Value` 起了个在参数语境下更达意的名字，二者是**同一个类型**（可互换）。
/// 与 newtype（`struct Topic(String)`，是不同类型）不同，别名不产生类型隔离，只提升可读性。
pub type ParamValue = Value;

/// 参数 topic 的保留前缀。
pub const PARAM_TOPIC_PREFIX: &str = "param://";

/// 构造规范的参数 topic 字符串：`param://{module}/{key}`。
pub fn param_topic(module: &str, key: &str) -> String {
    format!("{PARAM_TOPIC_PREFIX}{module}/{key}")
}

/// 参数客户端：任何模块都可以 get/set 其他模块暴露的参数。
///
/// # 设计意图：Client 与 Provider 为何拆成两个 trait
///
/// 这是**接口隔离原则（ISP）**的体现：把"读写别人参数"（`ParamClient`）与
/// "声明自己参数"（`ParamProvider`）分开。虽然运行时 `ParamStore` 同时实现两者，
/// 但模块拿到的 `ModuleContext` 里是两个独立字段（`params` 与 `param_registry`）：
/// 一个只消费的模块可以只拿 `ParamClient`，不需要看到 `declare` 能力。职责越窄，
/// 契约越清晰，也更容易在不同场景下分别替换或限制。
///
/// # 语法解读：为什么 get/set/list 是 async，而 watch 是同步 fn
///
/// 前三个是 `async fn`：它们可能需要等待（比如未来换成跨进程实现时，get 要等网络往返），
/// 预留 async 签名让实现可以是异步的（即使当前进程内实现是同步完成的）。
/// `watch` 是同步 `fn`：它只是从参数表里 `subscribe()` 出一个 `watch::Receiver`，
/// 这个动作本身不阻塞（真正的等待发生在后续 `receiver.changed().await`），所以无需 async。
/// 这是"按操作是否真正需要等待来选同步/异步"的一致取舍。
#[async_trait]
pub trait ParamClient: Send + Sync {
    /// 读取参数当前值。模块或 key 未注册时返回 `ParamNotFound`。
    async fn get(&self, module: &str, key: &str) -> Result<Value>;

    /// 写入参数并通知持有者（watch 热更新）。
    async fn set(&self, module: &str, key: &str, value: Value) -> Result<()>;

    /// 列出某模块已注册的全部参数 key。
    async fn list(&self, module: &str) -> Result<Vec<String>>;

    /// 订阅参数变更通知（tokio watch 语义：`changed().await` 即时感知 set）。
    ///
    /// 返回 `watch::Receiver<Value>`：watch 是"最新值通道"，只保留最新值、旧值被覆盖，
    /// 正适合参数这种"只关心当前值、不关心历史"的场景（对比：broadcast 会保留环形缓冲、
    /// mpsc 会堆积所有值）。持有者把它放进 `tokio::select!`，就能在参数被 set 时即时重建行为。
    fn watch(&self, module: &str, key: &str) -> Result<watch::Receiver<Value>>;
}

/// 参数注册端：模块在 init 阶段把自己的内部参数暴露到总线上。
///
/// 只有一个 `declare` 方法，职责单一：注册（或覆盖）一个参数并返回其 watch 接收端。
/// 返回 `Receiver` 是给"持有者自己"用的——模块声明参数后，自己也持有一个 receiver，
/// 以便在别的模块 `set` 了该参数时通过 `changed().await` 感知并热更新（见 SensorModule 的采样周期）。
pub trait ParamProvider: Send + Sync {
    /// 注册（或覆盖）一个参数，返回其 watch 接收端供持有者感知热更新。
    fn declare(&self, module: &str, key: &str, initial: Value) -> Result<watch::Receiver<Value>>;
}
