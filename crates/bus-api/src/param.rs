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
pub type ParamValue = Value;

/// 参数 topic 的保留前缀。
pub const PARAM_TOPIC_PREFIX: &str = "param://";

/// 构造规范的参数 topic 字符串：`param://{module}/{key}`。
pub fn param_topic(module: &str, key: &str) -> String {
    format!("{PARAM_TOPIC_PREFIX}{module}/{key}")
}

/// 参数客户端：任何模块都可以 get/set 其他模块暴露的参数。
#[async_trait]
pub trait ParamClient: Send + Sync {
    /// 读取参数当前值。模块或 key 未注册时返回 `ParamNotFound`。
    async fn get(&self, module: &str, key: &str) -> Result<Value>;

    /// 写入参数并通知持有者（watch 热更新）。
    async fn set(&self, module: &str, key: &str, value: Value) -> Result<()>;

    /// 列出某模块已注册的全部参数 key。
    async fn list(&self, module: &str) -> Result<Vec<String>>;

    /// 订阅参数变更通知（tokio watch 语义：`changed().await` 即时感知 set）。
    fn watch(&self, module: &str, key: &str) -> Result<watch::Receiver<Value>>;
}

/// 参数注册端：模块在 init 阶段把自己的内部参数暴露到总线上。
pub trait ParamProvider: Send + Sync {
    /// 注册（或覆盖）一个参数，返回其 watch 接收端供持有者感知热更新。
    fn declare(&self, module: &str, key: &str, initial: Value) -> Result<watch::Receiver<Value>>;
}
