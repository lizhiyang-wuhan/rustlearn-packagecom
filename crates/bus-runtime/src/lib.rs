//! # bus-runtime —— 消息总线中间件的 tokio 实现层
//!
//! 实现 [`bus_api`] 定义的全部能力抽象，底层 100% 使用 tokio 生态：
//!
//! - 请求/响应：`tokio::spawn` + `tokio::sync::oneshot` + `tokio::time::timeout`
//! - 发布/订阅、广播：每 topic 一个 `tokio::sync::broadcast`
//! - 参数 get/set + 热更新：`std::sync::RwLock` 参数表 + `tokio::sync::watch`
//! - 大文件传输：`Arc<Bytes>` 共享内存 + `tokio::sync::mpsc` 背压流
//!
//! 对外主要暴露 [`MessageBus`]（总线本体）、[`Runtime`]（装配运行器）与
//! [`SharedMemoryBlobClient`]、[`ParamStore`]（可替换的组件实现）。

pub mod blob;
pub mod bus;
pub mod param;
pub mod runtime;

pub use blob::SharedMemoryBlobClient;
pub use bus::{DEFAULT_BROADCAST_CAPACITY, DEFAULT_REQUEST_TIMEOUT, MessageBus, MessageBusBuilder};
pub use param::ParamStore;
pub use runtime::Runtime;
