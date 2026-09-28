## 架构设计

### 依赖方向

```
┌─────────────────┐     ┌──────────────────┐
│ module-sensor   │     │ module-processor │   ← 业务模块（互不依赖）
│                 │     │                  │
│ 只依赖 bus-api  │     │ 只依赖 bus-api   │
└────────┬────────┘     └────────┬─────────┘
         │                       │
         ▼                       ▼
     ┌───────────────────────────────┐
     │          bus-api              │   ← 纯抽象层（trait + 消息类型 + 错误定义）
     │  Publisher / Subscriber       │
     │  Requester / Handler          │
     │  ParamClient / ParamProvider  │
     │  BlobClient / Module          │
     └──────────────┬────────────────┘
                    │  implements
                    ▼
     ┌───────────────────────────────┐
     │        bus-runtime            │   ← tokio 实现层
     │  MessageBus (核心总线)         │
     │  Runtime (装配运行器)          │
     │  ParamStore / BlobClient      │
     └──────────────┬────────────────┘
                    │  组装
                    ▼
     ┌───────────────────────────────┐
     │       src/main.rs             │   ← 装配层（Composition Root）
     │  创建总线 → 注册模块 → 初始化  │
     │  → 并发运行 → 优雅停机         │
     └───────────────────────────────┘
```

### Crate 职责

| Crate | 类型 | 职责 |
|---|---|---|
| **bus-api** | 纯抽象层 | trait 定义、事件信封、Topic、消息契约、错误类型、类型化端点封装 |
| **bus-runtime** | 实现层 | `MessageBus` 核心总线、`Runtime` 装配运行器、`ParamStore` 参数存储、`SharedMemoryBlobClient` |
| **module-sensor** | 业务模块 A | 温度传感器：周期发布温度事件 + 提供 `read_now`/`stats` 服务 + 持有可热更新参数 |
| **module-processor** | 业务模块 B | 数据处理器：订阅温度 + 调用统计服务 + 参数 get/set + 大文件背压流/共享内存传输 |
| **tokioexample** (根) | 装配层 | `main.rs` 作为 Composition Root，创建总线、注册模块、统一启停 |

### 模块生命周期

```
Runtime::new(bus)
    │
    ├── register(SensorModule)
    ├── register(ProcessorModule)
    │
    ├── init()          ← 依次为每个模块构造 ModuleContext，
    │   │                  模块在此注册参数、服务 handler（能力自报）
    │   └── 必须在 run 之前完成，确保对端能力就绪
    │
    └── run(shutdown)   ← 并发 tokio::spawn 所有模块主循环
        │                  等待 CancellationToken 触发
        └── 各模块 select! 到 cancelled 后优雅退出
```

---

## 四种通信语义详解

### 1. 发布/订阅（Pub/Sub）

`sensor.temperature` topic 为例：

- **SensorModule** 每 500ms 周期 `publish` 一个 `TemperatureEvent`
- **ProcessorModule** `subscribe` 该 topic，通过 `Stream<Item = Event>` 持续消费
- 底层：每 topic 一个 `tokio::sync::broadcast`，慢消费者丢帧时记 warn 日志但不中断流

### 2. 请求/响应（Request/Response）

`sensor.stats` 服务为例：

- **SensorModule** 在 `init` 阶段注册 `StatsHandler`（模块自报能力）
- **ProcessorModule** 通过 `TypedService::request()` 发起类型化请求
- 底层：查路由表取 handler → `oneshot` 回话 → `tokio::time::timeout` 超时保护
- handler 在独立 `tokio::spawn` 中执行，不阻塞总线分发循环

### 3. 参数 get/set + 热更新

`sensor.sample_interval_ms` 参数为例：

- **SensorModule** 在 `init` 阶段 `declare` 参数（成为持有方）
- **ProcessorModule** 通过 `ParamClient::set()` 修改参数值
- **SensorModule** 内部通过 `watch::changed()` 即时感知并重建定时器
- 底层：`RwLock<HashMap>` 参数表 + `tokio::sync::watch` 通知

### 4. 大文件传输

两种模式：

| 模式 | 适用场景 | 机制 |
|---|---|---|
| `SharedBlob` 句柄传递 | 放得进内存的文件 | `Arc<Bytes>` 写一次读多次，经事件通道传句柄，零拷贝 |
| `BlobSink`/`BlobStream` 背压流 | 超大文件 | `mpsc` 有界通道逐块发送，缓冲满即 await；接收端 `collect_to_end` 聚合完整数据后才返回 |

---

## 类型安全设计

通过 `Typed<M>` 和 `TypedService<Req, Resp>` 封装，业务代码只见具体 Rust 类型：

```rust
// 定义（messages.rs）
pub fn temperature() -> Typed<TemperatureEvent> { ... }
pub fn stats_service() -> TypedService<StatsRequest, SensorStats> { ... }

// 发布端 —— 编译期类型安全
temperature.publish(publisher, source, TemperatureEvent { ts_ms, celsius })?;

// 消费端 —— 自动 downcast
let ev = temperature.take(&event)?;

// 请求端 —— 类型化请求/响应
let stats = stats_svc.request(requester, source, StatsRequest).await?;
```

主路径零序列化（进程内 `Arc<dyn Any>`），`serde-payload` feature 可选开启序列化能力，为跨进程演进预留接口。

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