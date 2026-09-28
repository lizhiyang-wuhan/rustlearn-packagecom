//! 装配运行时：把总线与各模块组合起来（Composition Root 的辅助器）。
//!
//! 业务装配层（app）用它注册所有模块、统一初始化、并发运行并优雅停机。
//! 这里是"唯一知道所有模块"的地方；模块之间仍然互不依赖。

use std::sync::Arc;

use bus_api::error::Result;
use bus_api::module::Module;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::bus::MessageBus;

/// 运行时：持有总线与已注册模块列表。
pub struct Runtime {
    bus: MessageBus,
    modules: Vec<Arc<dyn Module>>,
}

impl Runtime {
    /// 用给定总线创建运行时。
    pub fn new(bus: MessageBus) -> Self {
        Self {
            bus,
            modules: Vec::new(),
        }
    }

    /// 借访问总线（如需在装配层直接注册全局 handler）。
    pub fn bus(&self) -> &MessageBus {
        &self.bus
    }

    /// 注册一个模块（拥有所有权版本）。
    pub fn register<M>(&mut self, module: M)
    where
        M: Module,
    {
        self.modules.push(Arc::new(module));
    }

    /// 注册一个已共享的模块。
    pub fn register_arc(&mut self, module: Arc<dyn Module>) {
        self.modules.push(module);
    }

    /// 依次为每个模块构造 [`bus_api::traits::ModuleContext`] 并调用其 `init`。
    ///
    /// init 阶段模块完成参数注册、服务 handler 注册（能力自报），
    /// 因此必须在 `run` 之前完成——这样任何模块 run 时对端能力都已就绪。
    pub async fn init(&self) -> Result<()> {
        for module in &self.modules {
            let ctx = self.bus.context_for(module.name());
            tracing::info!(module = module.name(), "initializing module");
            Arc::clone(module).init(&ctx).await?;
        }
        Ok(())
    }

    /// 并发运行所有模块，直到 `shutdown` 被取消，随后等待各模块优雅退出。
    pub async fn run(&self, shutdown: CancellationToken) -> Result<()> {
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(self.modules.len());
        for module in &self.modules {
            let module = Arc::clone(module);
            let token = shutdown.clone();
            handles.push(tokio::spawn(async move {
                let name = module.name().to_string();
                tracing::info!(module = %name, "module started");
                if let Err(e) = module.run(token).await {
                    tracing::error!(module = %name, error = %e, "module exited with error");
                } else {
                    tracing::info!(module = %name, "module stopped gracefully");
                }
            }));
        }

        // 等待停机信号
        shutdown.cancelled().await;
        tracing::info!("shutdown requested, waiting for modules to finish");

        // 等待所有模块任务结束（各自 select 到 cancelled 后返回）
        for handle in handles {
            let _ = handle.await;
        }
        Ok(())
    }
}
