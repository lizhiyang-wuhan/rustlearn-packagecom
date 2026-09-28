# Tokio 学习实践 —— 进程内消息总线中间件

之前工作中使用的是内部的packagecom轮子，用于C++ Cmake项目中的动态库通信，曾研究过一段时间没太明白，正好在rust中重新实现一个demo，主要用来学习，基于qwen-3.8max实现

> **当前版本：v0.0.1**（初次迭代完成，核心功能可运行）
>
> 以 Tokio 异步运行时为基础，通过构建一个**多 crate 模块间通信中间件**来深入学习 Rust 异步编程、trait 抽象、依赖注入与优雅停机。

---

## 项目概述

本项目实现了一个轻量级的**进程内消息总线中间件**，支持四种通信语义：

| 语义 | 底层原语 | 说明 |
|---|---|---|
| 发布/订阅（广播） | `tokio::sync::broadcast` | 每 topic 一个环形缓冲，多订阅者独立消费 |
| 请求/响应 | `tokio::spawn` + `oneshot` | 事件驱动，handler 在独立任务中执行，不阻塞分发 |
| 参数 get/set + 热更新 | `RwLock` 参数表 + `tokio::sync::watch` | 低频配置数据，set 后所有 watch 订阅者即时感知 |
| 大文件传输 | `Arc<Bytes>` 共享内存 + `mpsc` 背压流 | 通道只传句柄/分块，数据零拷贝或受控内存 |

核心设计原则：
- **模块间零依赖** —— 业务模块只依赖抽象层 `bus-api`，互不知道对方的存在
- **依赖抽象而非实现** —— 跨 crate 传递的是 trait 对象（`Arc<dyn Publisher>` 等），不是具体通道
- **装配层统一注册** —— 只有 `main.rs`（Composition Root）认识所有模块

---


## 快速开始

### 运行 Demo

```bash
# 运行消息总线中间件 demo（自动 10s 后退出，或 Ctrl-C 提前结束）
cargo run

# 调整日志级别
RUST_LOG=debug cargo run
```

### 运行 Tokio 基础示例

> `examples/` 目录下是早期的 Tokio 基础练习，与消息总线中间件独立，用于熟悉 Tokio 的 TCP 网络编程。

```bash
# TCP Echo 服务端
cargo run --example echo_tcp

# TCP Hello World 客户端（需先启动 echo_tcp）
cargo run --example hello_world
```

### 示例输出

运行 `cargo run` 后的典型输出（节选）：

```
=== 多 crate 模块间通信中间件 demo 启动 ===
[sensor]  initializing module
[sensor]  sensor capabilities registered  services=["sensor.read_now", "sensor.stats"]  params=["sample_interval_ms", "unit"]
[processor] initializing module
[processor] processor initialized
[sensor]  module started
[processor] module started
[processor] 演示1(pub/sub): processor 收到温度事件  seq=1 celsius=20.00 from=sensor
[processor] 演示1(pub/sub): processor 收到温度事件  seq=2 celsius=23.38 from=sensor
...
[processor] 演示1(pub/sub): processor 收到温度事件  seq=5 celsius=24.70 from=sensor
[processor] 演示2(请求/响应): processor 请求 sensor.stats 成功  stats=SensorStats { count: 5, min: 15.85, max: 24.70, avg: 20.42 }
=== 演示 3: 参数 get/set + watch 热更新 ===
[processor] get sensor.sample_interval_ms  value=500
[processor] set sensor.sample_interval_ms = 200
[sensor]  sample_interval_ms hot-reloaded via watch  interval_ms=200
=== 演示 4a: 大文件背压流式传输（8MiB / 64KiB 分块 / 2 块缓冲）===
[processor] blob stream transfer complete  sent_bytes=8388608 checksum_ok=true
=== 演示 4b: SharedBlob 句柄经事件通道零拷贝传递 ===
[processor] SharedBlob handle received  len=1024 checksum_ok=true
demo 定时器到期（10s），准备停机
shutdown requested, waiting for modules to finish
[sensor]  shutdown signal received
[sensor]  module stopped gracefully
[processor] module stopped gracefully
=== demo 结束，所有模块已优雅退出 ===
```

---

## 技术栈

| 类别 | 依赖 |
|---|---|
| 异步运行时 | `tokio` (full) |
| 异步流 | `tokio-stream` + `tokio-util` |
| 序列化（可选） | `serde` + `serde_json`（`serde-payload` feature） |
| 日志追踪 | `tracing` + `tracing-subscriber` |
| 错误处理 | `thiserror` |
| 异步 trait | `async-trait` |
| 字节缓冲 | `bytes` |

---

## 项目结构

```
packagecom/
├── crates/
│   ├── bus-api/src/          # 纯抽象层：trait + 消息类型 + 错误定义
│   ├── bus-runtime/          # tokio 实现层：MessageBus + Runtime + ParamStore + BlobClient
│   │   ├── src/
│   │   └── tests/
│   ├── module-sensor/src/    # 示例模块 A：温度传感器
│   └── module-processor/src/ # 示例模块 B：数据处理器
├── examples/                 # 早期 Tokio 基础练习（TCP Echo / Hello World）
├── src/main.rs               # 装配层（Composition Root）
└── Cargo.toml                # Workspace 配置
```
