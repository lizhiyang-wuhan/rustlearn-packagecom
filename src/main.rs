//! # app 装配层（Composition Root）
//!
//! 这是整个系统中**唯一认识所有模块**的地方：创建总线运行时，把各模块注册进去，
//! 统一初始化、并发运行、优雅停机。模块之间互不依赖，只通过总线抽象通信。
//!
//! 依赖方向：`module-sensor` / `module-processor` → `bus-api` ← `bus-runtime`；
//! 本 app → 全部。跨 crate 传递的是能力对象（`Arc<dyn Publisher>` 等），不是通道。

use std::time::Duration;

use bus_runtime::{MessageBus, Runtime};
use module_processor::ProcessorModule;
use module_sensor::SensorModule;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// demo 自动结束时长（保证示例能自动跑完退出，也可用 Ctrl-C 提前结束）。
const DEMO_DURATION: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 日志：默认 info 级，可用 RUST_LOG 覆盖（如 RUST_LOG=debug）
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    tracing::info!("=== 多 crate 模块间通信中间件 demo 启动 ===");

    // 1) 创建总线运行时（tokio 实现层）
    let bus = MessageBus::builder()
        .broadcast_capacity(256)
        .request_timeout(Duration::from_secs(5))
        .build();

    // 2) 装配层注册所有模块——唯一把它们"搜集到一块"的地方
    let mut runtime = Runtime::new(bus);
    runtime.register(SensorModule::new());
    runtime.register(ProcessorModule::new());

    // 3) 初始化：为每个模块构造能力上下文并收集其能力注册（参数 + 服务 handler）
    //    必须在 run 之前完成，确保任何模块运行时对端能力都已就绪
    runtime.init().await?;

    // 4) 停机控制：Ctrl-C 或 demo 定时器先到者触发
    let shutdown = CancellationToken::new();
    {
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("收到 Ctrl-C，准备停机");
                }
                _ = tokio::time::sleep(DEMO_DURATION) => {
                    tracing::info!("demo 定时器到期（{:?}），准备停机", DEMO_DURATION);
                }
            }
            token.cancel();
        });
    }

    // 5) 并发运行所有模块，直到停机信号，随后等待各模块优雅退出
    runtime.run(shutdown).await?;

    tracing::info!("=== demo 结束，所有模块已优雅退出 ===");
    Ok(())
}
