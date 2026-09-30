//! 示例消息契约（模拟真实项目中的"协议 crate"）。
//!
//! module-sensor 与 module-processor **互不依赖**，它们都只依赖 bus-api。
//! 双方要通信的消息类型与 topic 约定集中放在这里，充当共享协议：
//! 生产者按此发布/应答，消费者按此订阅/请求，字段即契约。
//!
//! 这也演示了 serde 混合方案：消息类型在 `serde-payload` feature 下自动获得
//! 序列化能力，为未来跨进程演进留好接口；主路径仍是进程内 `Arc<dyn Any>` 零拷贝。
//!
//! # 设计意图：为什么把消息契约放在抽象层
//!
//! 真实项目里常有一个独立的"协议 crate"（只有数据类型与 topic 常量，无逻辑），
//! 让互不依赖的生产者与消费者共享同一份契约。本示例把它合并进 bus-api 以简化 crate 数量，
//! 但职责边界是清楚的：这个文件里的东西两个模块都会用，而它们彼此不用。
//! 这正是"模块零依赖"能成立的前提——共享的是契约，不是彼此。

use crate::topic::Topic;
use crate::typed::{Typed, TypedService};

/// topic 与模块名常量。
///
/// # 设计意图：把字符串常量集中成一个子模块
///
/// 用一个 `pub mod topic` 把所有路由键字面量收拢，而不是让两个模块各自硬编码
/// `"sensor.temperature"`。这样：改一个 topic 名只需改一处；且两端引用同一个常量，
/// 从根上消除了"拼写不一致导致发/收对不上"的隐蔽 bug。常量用 `&str` 而非 `Topic`，
/// 是因为 `const` 不能调用非 const 构造函数（`Topic::new` 要分配 `String`），所以存原始字符串、用时再包。
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
///
/// # 语法解读：单元结构体 `struct ReadNow;`
///
/// 没有字段的结构体叫**单元结构体**，用作"不需要携带数据的请求"的类型标记。
/// 即使请求无参数，也定义一个专属类型（而非用 `()`），是为了让 `TypedService<ReadNow, _>`
/// 的泛型参数有明确语义、且与其他请求类型区分开。派生 `Copy + Default` 是因为它零大小，
/// 可随意复制/构造。
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
///
/// # 语法解读：`pub use ... as ...` 重导出
///
/// 这不是定义新类型，而是把 `SharedBlob` **以 `BlobReady` 之名重新导出**。目的：
/// 在消息契约里用业务语义的名字（`BlobReady`）暴露它，让读消息定义的人不必关心
/// 底层是 `blob::SharedBlob`。两者是同一类型，只是多了一个语境化的别名。
pub use crate::blob::SharedBlob as BlobReady;

// ---- 类型化端点：集中定义，两个模块都从这里取，避免各自硬编码 topic 字符串 ----

/// 温度事件的类型化 topic（pub/sub）。
///
/// # 设计意图：为什么是函数而不是常量
///
/// `Typed<M>` 内含 `PhantomData`，不是 `const` 可构造的，且 `Topic::new` 要分配 `String`，
/// 所以用函数每次构造一个轻量的类型化端点。关键价值在返回类型 `Typed<TemperatureEvent>`：
/// 它把"这个 topic 只发 TemperatureEvent"写进了类型签名。两个模块都调同一个函数，
/// 于是发布端与订阅端共享同一份类型契约，编译器保证两端类型一致。
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
