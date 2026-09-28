//! # bus-api —— 进程内消息总线中间件的纯抽象层
//!
//! 本 crate 是整个中间件的"契约层"：只包含 trait、消息类型与错误定义，
//! **不包含任何运行时实现**。业务模块 crate 只依赖本 crate，从而做到：
//!
//! - 模块之间零依赖、完全解耦（不知道对面提供什么功能）；
//! - 跨 crate 传递的不是 tokio 通道，而是"具备发布能力的对象"
//!   （[`Publisher`] / [`Requester`] / [`Subscriber`] 等 trait 对象），
//!   依赖抽象而不是实现——底层通道可随时替换（甚至换成跨进程实现）；
//! - 只有业务装配层（app）认识所有模块，把它们注册到同一个运行时总线上。
//!
//! ## 支持的四种通信语义
//!
//! | 语义 | 入口 | 底层原语（对模块不可见） |
//! |---|---|---|
//! | 发布/订阅、广播 | [`Publisher`] + [`Subscriber`] | `tokio::sync::broadcast` |
//! | 事件驱动请求/响应 | [`Requester`] + [`Handler`]/[`ErasedHandler`] | `tokio::spawn` + `tokio::sync::oneshot` |
//! | 参数请求/响应 get/set | [`ParamClient`] / [`ParamProvider`] | `tokio::sync::watch` + RwLock 参数表 |
//! | 大文件传输（共享内存） | [`BlobClient`] / [`blob::SharedBlob`] | `Arc<Bytes>` 共享缓冲 + `tokio::sync::mpsc` 背压流 |
//!
//! ## 大文件传输的演进路径
//!
//! 当前 [`blob::SharedBlob`] 是进程内 `Arc<Bytes>` 共享内存实现（零拷贝、写一次读多次）。
//! 未来若跨进程部署，可将其替换为 `memmap2` 匿名 mmap / 共享内存段实现，
//! API 保持不变——因为模块拿到的是 [`BlobClient`] 抽象，而非具体缓冲。

pub mod blob;
pub mod error;
pub mod event;
pub mod messages;
pub mod module;
pub mod param;
pub mod topic;
pub mod traits;
pub mod typed;

pub use error::{BusError, Result};
pub use event::{Event, Payload};
pub use topic::Topic;
