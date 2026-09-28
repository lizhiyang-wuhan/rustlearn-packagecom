//! 模块生命周期抽象。

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::traits::ModuleContext;

/// 一个业务模块。装配层（app）为每个模块构造 [`ModuleContext`]（注入各种能力对象），
/// 依次调用 [`init`](Module::init) 收集其能力注册，再 [`run`](Module::run) 启动主循环。
///
/// 模块之间互不依赖：它们只通过 `ctx` 里的 trait 对象与外界通信。
#[async_trait]
pub trait Module: Send + Sync + 'static {
    /// 模块名（作为事件 source、参数命名空间）。
    fn name(&self) -> &str;

    /// 装配期回调：注册参数、注册服务 handler。默认空实现。
    ///
    /// 在 `run` 之前由装配层调用一次。此处应把需要长期持有的能力（如 publisher）
    /// 从 `ctx` 克隆进模块自身状态。
    async fn init(self: Arc<Self>, ctx: &ModuleContext) -> Result<()> {
        let _ = ctx;
        Ok(())
    }

    /// 模块主循环：应持续运行直到 `shutdown` 被取消，然后优雅退出。
    ///
    /// 约定用 `tokio::select!` 同时监听业务事件与 `shutdown.cancelled()`。
    async fn run(self: Arc<Self>, shutdown: CancellationToken) -> Result<()>;
}
