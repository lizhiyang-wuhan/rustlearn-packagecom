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

## 架构设计

> 详细的架构文档（UML 类图、调用链路、接口汇总）见 [docs/architecture.md](docs/architecture.md)。
>
> 代码学习问答手册（按主题归类的提问精讲：类型擦除、泛型约束、对象安全、异步背压等）见 [docs/learning-qa.md](docs/learning-qa.md)。

### 依赖方向

```
┌─────────────────┐     ┌──────────────────┐
│ module-sensor   │     │ module-processor │   ← 业务模块（互不依赖）
│ 只依赖 bus-api  │     │ 只依赖 bus-api   │
└────────┬────────┘     └────────┬─────────┘
         ▼                       ▼
     ┌───────────────────────────────┐
     │          bus-api              │   ← 纯抽象层（trait + 消息类型）
     └──────────────┬────────────────┘
                    │  implements
                    ▼
     ┌───────────────────────────────┐
     │        bus-runtime            │   ← tokio 实现层
     └──────────────┬────────────────┘
                    │  组装
                    ▼
     ┌───────────────────────────────┐
     │       src/main.rs             │   ← 装配层（Composition Root）
     └───────────────────────────────┘
```

### Crate 职责

| Crate | 类型 | 职责 |
|---|---|---|
| **bus-api** | 纯抽象层 | trait 定义、事件信封、Topic、消息契约、错误类型、类型化端点封装 |
| **bus-runtime** | 实现层 | `MessageBus` 核心总线、`Runtime` 装配运行器、`ParamStore`、`SharedMemoryBlobClient` |
| **module-sensor** | 业务模块 A | 温度传感器：周期发布 + 服务提供 + 参数持有 |
| **module-processor** | 业务模块 B | 数据处理器：订阅消费 + 服务调用 + 参数操作 + 大文件传输 |
| **tokioexample** (根) | 装配层 | `main.rs` 作为 Composition Root，创建总线、注册模块、统一启停 |

### 四种通信语义速览

| 语义 | 底层原语 | 调用链路 |
|---|---|---|
| 发布/订阅 | `broadcast` | publish → topic_sender → tx.send → stream.next → downcast |
| 请求/响应 | `oneshot` + `spawn` | request → 查路由表 → handler.handle → spawn → reply.send → timeout |
| 参数 get/set | `RwLock` + `watch` | set → send_modify → watch 通知 → changed() → 热更新 |
| 大文件传输 | `Arc<Bytes>` + `mpsc` | 背压流：send_all → mpsc.send（背压）→ collect_to_end；句柄：publish(SharedBlob) → downcast → read |

> 完整调用链路图（含每一步的函数调用和底层原语）见 [docs/architecture.md](docs/architecture.md#四种通信语义的调用链路)。

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

## 当前版本 v0.0.1 已知局限

- 仅支持**进程内**通信，所有模块必须在同一进程
- 每个 topic 只允许注册**一个** handler（不支持同 topic 多实例负载均衡）
- 广播通道容量固定，慢消费者会丢帧（仅 warn 日志，无重传）
- 缺少集成测试覆盖，边界场景（handler 超时、重复注册等）尚未测试
- 无中间件/拦截器机制，无法在消息路径上插入横切逻辑

---

## 下一步计划

> 以下计划围绕消息总线核心能力逐步完善，每一步都是在前一步基础上自然延伸。

### 阶段一：加固基础（v0.1.0）

> 目标：让现有功能更健壮，补上测试和错误处理的短板。

- [ ] **集成测试** —— 为 `bus-runtime` 补充测试用例：覆盖 pub/sub 多订阅者、请求/响应正常路径与超时、参数 get/set/watch、大文件背压流等
- [ ] **错误处理加固** —— 统一 handler panic 捕获（`tokio::spawn` 内 panic 不应让进程崩溃），补充 `BusError` 的更多恢复路径
- [ ] **广播通道容量可配置** —— 支持按 topic 设置不同的 broadcast capacity（当前全局统一 256）

### 阶段二：丰富通信能力（v0.2.0）

> 目标：在四种语义之上，增加实用的通信模式。

- [ ] **多 handler 注册** —— 支持同一 topic 注册多个 handler（如轮询分发或广播分发），扩展请求/响应的灵活性
- [ ] **通配符订阅** —— 支持 `sensor.*` 风格的 pattern 订阅，让一个消费者监听某类 topic 的全部事件
- [ ] **事件过滤** —— 在 `Subscriber` 层增加可选的消息过滤条件，减少不必要的 downcast 开销

### 阶段三：工程化完善（v0.3.0）

> 目标：让中间件从"能跑"走向"能用"，提升可维护性和可调试性。

- [ ] **结构化日志增强** —— 为每条事件/请求自动附加 `trace_id`，在 `Event` 信封中预留 metadata 字段
- [ ] **配置化装配** —— 通过 TOML 文件声明模块列表和 topic 路由规则，替代 `main.rs` 中的硬编码注册
- [ ] **优雅重启** —— 支持单个模块的热重启（停掉旧实例、启动新实例并重新注册 handler），不影响其他模块运行

### 阶段四：跨进程探索（v1.0.0）

> 目标：突破进程内限制，验证架构的可替换性。

- [ ] **TCP 传输层** —— 基于 `tokio::net::TcpStream` 实现跨进程的 `Publisher`/`Subscriber`，验证 trait 抽象的可替换性
- [ ] **序列化载荷** —— 启用 `serde-payload` feature，将 `Arc<dyn Any>` 零拷贝路径与 `serde_json` 序列化路径并存，按场景选择
- [ ] **共享内存 Blob** —— 将 `BlobClient` 的 `SharedMemoryBlobClient` 替换为 `memmap2` 匿名 mmap 实现，支持跨进程大文件传输

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
