//! # module-sensor —— 示例模块 A（温度传感器）
//!
//! 演示三种能力，且**只依赖 bus-api**，完全不知道谁会消费它的输出：
//!
//! 1. 事件生产者：周期性 `publish` 温度事件到 `sensor.temperature`（pub/sub 广播）。
//! 2. 服务提供者：注册 `sensor.read_now` / `sensor.stats` 两个请求/响应 handler
//!    （模块自报能力，由装配层收集注册）。
//! 3. 参数持有者：声明 `sample_interval_ms` / `unit`，支持被 `set` 后经
//!    `tokio::sync::watch` 热更新采样周期。

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bus_api::event::Value;
use bus_api::messages::{
    self, ReadNow, SensorStats, StatsRequest, TemperatureEvent,
    topic::{SENSOR, STATS, READ_NOW},
};
use bus_api::traits::{Handler, ModuleContext};
use bus_api::module::Module;
use bus_api::{BusError, Result};
use tokio_util::sync::CancellationToken;

/// 传感器累计状态。所有访问都在 `std::sync::Mutex` 短临界区内完成，绝不跨 await 持锁。
struct SensorState {
    start: Instant,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    last: Option<TemperatureEvent>,
}

impl SensorState {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            last: None,
        }
    }

    /// 产生一次采样并更新累计量。用正弦波模拟温度波动，保证确定性可复现。
    fn sample(&mut self) -> TemperatureEvent {
        let ts_ms = self.start.elapsed().as_millis() as u64;
        let celsius = 20.0 + (self.count as f64 * 0.7).sin() * 5.0;
        self.count += 1;
        self.sum += celsius;
        self.min = self.min.min(celsius);
        self.max = self.max.max(celsius);
        let ev = TemperatureEvent { ts_ms, celsius };
        self.last = Some(ev.clone());
        ev
    }

    fn stats(&self) -> SensorStats {
        let avg = if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        };
        SensorStats {
            count: self.count,
            min: if self.count == 0 { 0.0 } else { self.min },
            max: if self.count == 0 { 0.0 } else { self.max },
            avg,
        }
    }
}

/// `sensor.read_now` 服务：立即产生并返回一次采样。
struct ReadNowHandler {
    state: Arc<Mutex<SensorState>>,
}

#[async_trait]
impl Handler<ReadNow, TemperatureEvent> for ReadNowHandler {
    async fn handle(&self, _req: ReadNow) -> Result<TemperatureEvent> {
        let ev = self.state.lock().unwrap().sample();
        tracing::debug!(celsius = ev.celsius, "read_now handled");
        Ok(ev)
    }
}

/// `sensor.stats` 服务：返回累计统计。
struct StatsHandler {
    state: Arc<Mutex<SensorState>>,
}

#[async_trait]
impl Handler<StatsRequest, SensorStats> for StatsHandler {
    async fn handle(&self, _req: StatsRequest) -> Result<SensorStats> {
        Ok(self.state.lock().unwrap().stats())
    }
}

/// 传感器模块。
pub struct SensorModule {
    state: Arc<Mutex<SensorState>>,
    /// init 阶段注入、run 阶段使用的能力上下文。
    ctx: OnceLock<ModuleContext>,
}

impl SensorModule {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SensorState::new())),
            ctx: OnceLock::new(),
        }
    }
}

impl Default for SensorModule {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Module for SensorModule {
    fn name(&self) -> &str {
        SENSOR
    }

    async fn init(self: Arc<Self>, ctx: &ModuleContext) -> Result<()> {
        // 保存上下文供 run 使用（ModuleContext 是若干 Arc 能力句柄，clone 廉价）
        let _ = self.ctx.set(ctx.clone());

        // 3) 声明参数（get/set + watch 热更新的持有方）
        ctx.param_registry.declare(SENSOR, "sample_interval_ms", Value::U64(500))?;
        ctx.param_registry.declare(SENSOR, "unit", Value::String("celsius".into()))?;

        // 2) 注册请求/响应服务 handler（模块自报能力）
        let read_now = messages::read_now_service();
        ctx.registry.register_handler(
            read_now.topic().clone(),
            read_now.erase(ReadNowHandler {
                state: Arc::clone(&self.state),
            }),
        )?;
        let stats = messages::stats_service();
        ctx.registry.register_handler(
            stats.topic().clone(),
            stats.erase(StatsHandler {
                state: Arc::clone(&self.state),
            }),
        )?;

        tracing::info!(
            module = SENSOR,
            services = ?[READ_NOW, STATS],
            params = ?["sample_interval_ms", "unit"],
            "sensor capabilities registered"
        );
        Ok(())
    }

    async fn run(self: Arc<Self>, shutdown: CancellationToken) -> Result<()> {
        let ctx = self
            .ctx
            .get()
            .cloned()
            .ok_or_else(|| BusError::Closed("sensor.run called before init".into()))?;

        let temperature = messages::temperature();

        // 读取初始采样周期，并订阅其 watch 以支持热更新
        let mut interval_ms = read_interval_ms(&ctx).await?;
        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(10)));
        let mut interval_rx = ctx.watch_param(SENSOR, "sample_interval_ms")?;
        // 标记当前值已读，避免首次 changed() 立即返回造成的伪触发
        let _ = interval_rx.borrow_and_update();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(module = SENSOR, "shutdown signal received");
                    break;
                }
                _ = ticker.tick() => {
                    // 1) 周期发布温度事件（pub/sub 广播）
                    let ev = self.state.lock().unwrap().sample();
                    match temperature.publish(ctx.publisher.as_ref(), ctx.module_name.clone(), ev.clone()) {
                        Ok(()) => tracing::info!(ts_ms = ev.ts_ms, celsius = format!("{:.2}", ev.celsius), "published temperature event"),
                        // 尚无订阅者属正常情况（消费者可能还没起来），降级为 debug
                        Err(BusError::NoSubscriber(_)) => tracing::debug!("temperature published but no subscriber yet"),
                        Err(e) => tracing::warn!(error = %e, "publish temperature failed"),
                    }
                }
                _ = interval_rx.changed() => {
                    // 3) 参数热更新：set 后立即感知并重建定时器
                    let new_ms = value_to_u64(&interval_rx.borrow(), interval_ms);
                    interval_ms = new_ms;
                    ticker = tokio::time::interval(Duration::from_millis(interval_ms.max(10)));
                    tracing::info!(interval_ms, "sample_interval_ms hot-reloaded via watch");
                }
            }
        }
        Ok(())
    }
}

/// 从参数存储读取采样周期，缺省 500ms。
async fn read_interval_ms(ctx: &ModuleContext) -> Result<u64> {
    let v = ctx.params.get(SENSOR, "sample_interval_ms").await?;
    Ok(value_to_u64(&v, 500))
}

fn value_to_u64(v: &Value, fallback: u64) -> u64 {
    match v {
        Value::U64(x) => *x,
        Value::I64(x) => (*x).max(0) as u64,
        _ => fallback,
    }
}
