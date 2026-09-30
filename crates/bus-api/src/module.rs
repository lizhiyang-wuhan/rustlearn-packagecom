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
///
/// # 语法解读：`trait Module: Send + Sync + 'static`
///
/// supertrait 里除了 `Send + Sync` 还多了 `'static`，这是因为模块实例会被
/// `Runtime` 装进 `Arc<dyn Module>` 并 `tokio::spawn` 到独立任务——任务要求 `'static`
/// （不能携带栈上借用）。`dyn Module` 本身就隐含 `Module: 'static`，这里显式写出更清楚。
#[async_trait]
pub trait Module: Send + Sync + 'static {
    /// 模块名（作为事件 source、参数命名空间）。
    fn name(&self) -> &str;

    /// 装配期回调：注册参数、注册服务 handler。默认空实现。
    ///
    /// 在 `run` 之前由装配层调用一次。此处应把需要长期持有的能力（如 publisher）
    /// 从 `ctx` 克隆进模块自身状态。
    ///
    /// # 语法解读：`self: Arc<Self>` 接收者（重点）
    ///
    /// 这是本文件最值得理解的语法。方法的第一个参数不只能是 `self`/`&self`/`&mut self`，
    /// 也可以是智能指针包装的 `self`，称为 **arbitrary self types（任意 self 类型）**。
    /// `self: Arc<Self>` 意思是：调用者必须用 `Arc` 持有这个对象，方法内部拿到的是
    /// `Arc<Self>` 而非 `Self`。
    ///
    /// 为什么必须用 `Arc<Self>` 而不能用 `&self` 或 `self`？因为 `init`/`run` 是 async 的：
    /// - 用 `&self`：`tokio::spawn` 要求 future 是 `'static`，不能持有对栈上对象的借用，编译不过。
    /// - 用 `self`（消费所有权）：`init` 把对象消耗掉后，`run` 就没对象可用了。
    /// - 用 `Arc<Self>`：共享所有权。`init` 里可以 clone 内部状态分发给 handler，自身仍保留；
    ///   `run` 里可以把 `Arc` move 进 spawn 任务，满足 `'static`，对象活到任务结束。
    ///
    /// # 默认方法实现（provided method）
    ///
    /// `init` 带默认实现（`{ let _ = ctx; Ok(()) }`），意味着实现 `Module` 的类型
    /// **可以不写 `init`**（像 `ProcessorModule` 只存 ctx），只有需要注册能力的模块
    /// （像 `SensorModule`）才重写它。`let _ = ctx;` 是为了显式"忽略这个参数"、
    /// 避免 `unused variable` 警告——这是给默认空实现占位的惯用写法。
    async fn init(self: Arc<Self>, ctx: &ModuleContext) -> Result<()> {
        let _ = ctx;
        Ok(())
    }

    /// 模块主循环：应持续运行直到 `shutdown` 被取消，然后优雅退出。
    ///
    /// 约定用 `tokio::select!` 同时监听业务事件与 `shutdown.cancelled()`。
    ///
    /// 注意本方法**没有默认实现**（只有签名、以 `;` 结束），所以每个模块都必须实现它——
    /// 这是模块的"必填项"：主循环逻辑因模块而异，无法给出通用默认。与 `init` 的
    /// "选填"形成对比，体现 trait 设计里"哪些方法必须有默认、哪些必须强制实现"的取舍。
    async fn run(self: Arc<Self>, shutdown: CancellationToken) -> Result<()>;
}
