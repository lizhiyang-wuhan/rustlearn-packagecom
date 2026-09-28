//! 示例消息契约（模拟真实项目中的"协议 crate"）。
//!
//! module-sensor 与 module-processor **互不依赖**，它们都只依赖 bus-api。
//! 双方要通信的消息类型与 topic 约定集中放在这里，充当共享协议：
//! 生产者按此发布/应答，消费者按此订阅/请求，字段即契约。
//!
//! 这也演示了 serde 混合方案：消息类型在 `serde-payload` feature 下自动获得
//! 序列化能力，为未来跨进程演进留好接口；主路径仍是进程内 `Arc<dyn Any>` 零拷贝。

use crate::topic::Topic;
use crate::typed::{Typed, TypedService};

/// topic 与模块名常量。
pub mod topic {
    /// 传感器模块名（也是参数命名空间）。
    pub const SENSOR: &str = "sensor";
    /// 处理器模块名。
    pub const PROCESSOR: &str = "processor";

    /// pub/sub：周期温度事件。
    pub const TEMPERATURE: &str = "sensor.temperature";
    /// 请求/响应：立即读取一次采样。
    pub const READ_NOW: &str = "sensor.read_now";
    /// 请求/响应：读取累计统计。
    pub const STATS: &str = "sensor.stats";
    /// pub/sub：处理器把大文件 blob 句柄发布到此 topic（演示句柄经事件通道传递）。
    pub const BLOB_READY: &str = "processor.blob_ready";
}

/// 温度采样事件（pub/sub 载荷）。
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde-payload", derive(serde::Serialize, serde::Deserialize))]
pub struct TemperatureEvent {
    /// 采样时间戳（毫秒，进程内单调）。
    pub ts_ms: u64,
    /// 摄氏温度值。
    pub celsius: f64,
}

/// 立即读取请求（无字段，占位）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-payload", derive(serde::Serialize, serde::Deserialize))]
pub struct ReadNow;

/// 统计请求（无字段，占位）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-payload", derive(serde::Serialize, serde::Deserialize))]
pub struct StatsRequest;

/// 累计统计响应。
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde-payload", derive(serde::Serialize, serde::Deserialize))]
pub struct SensorStats {
    /// 采样次数。
    pub count: u64,
    /// 最小值。
    pub min: f64,
    /// 最大值。
    pub max: f64,
    /// 平均值。
    pub avg: f64,
}

/// blob 就绪通知：载荷是 [`crate::blob::SharedBlob`] 句柄本身（零拷贝传递）。
///
/// 演示"大文件走共享内存、事件通道只传句柄"的模式。
pub use crate::blob::SharedBlob as BlobReady;

// ---- 类型化端点：集中定义，两个模块都从这里取，避免各自硬编码 topic 字符串 ----

/// 温度事件的类型化 topic（pub/sub）。
pub fn temperature() -> Typed<TemperatureEvent> {
    Typed::new(Topic::new(topic::TEMPERATURE))
}

/// 立即读取服务的类型化端点（请求/响应）。
pub fn read_now_service() -> TypedService<ReadNow, TemperatureEvent> {
    TypedService::new(Topic::new(topic::READ_NOW))
}

/// 统计服务的类型化端点（请求/响应）。
pub fn stats_service() -> TypedService<StatsRequest, SensorStats> {
    TypedService::new(Topic::new(topic::STATS))
}

/// blob 就绪的类型化 topic（pub/sub，载荷为共享内存句柄）。
pub fn blob_ready() -> Typed<crate::blob::SharedBlob> {
    Typed::new(Topic::new(topic::BLOB_READY))
}
