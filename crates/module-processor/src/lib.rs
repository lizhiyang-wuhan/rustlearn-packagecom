//! # module-processor —— 示例模块 B（数据处理器）
//!
//! 演示消费侧的全部通信语义，且**只依赖 bus-api**（不依赖 module-sensor）：
//!
//! 1. 事件消费者：订阅 `sensor.temperature`（pub/sub 接收端）。
//! 2. 服务调用者：收到若干事件后请求 `sensor.stats`（请求/响应，调用方不知对端是谁）。
//! 3. 参数操作者：`get`/`set` 传感器参数，触发热更新。
//! 4. 大文件收发：
//!    - 背压流：8MiB 数据经 64KiB 分块、仅缓冲 2 块的 mpsc 流发送，接收端
//!      `collect_to_end` 聚合为**完整数据**后校验；
//!    - 共享内存句柄：把 `SharedBlob` 句柄经普通事件通道零拷贝传递。

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bus_api::blob::send_all;
use bus_api::event::Value;
use bus_api::messages::{
    self, StatsRequest,
    topic::{BLOB_READY, PROCESSOR, SENSOR, TEMPERATURE},
};
use bus_api::module::Module;
use bus_api::traits::ModuleContext;
use bus_api::{BusError, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

/// 处理器模块。
pub struct ProcessorModule {
    ctx: OnceLock<ModuleContext>,
}

impl ProcessorModule {
    pub fn new() -> Self {
        Self {
            ctx: OnceLock::new(),
        }
    }
}

impl Default for ProcessorModule {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Module for ProcessorModule {
    fn name(&self) -> &str {
        PROCESSOR
    }

    async fn init(self: Arc<Self>, ctx: &ModuleContext) -> Result<()> {
        let _ = self.ctx.set(ctx.clone());
        tracing::info!(module = PROCESSOR, "processor initialized");
        Ok(())
    }

    async fn run(self: Arc<Self>, shutdown: CancellationToken) -> Result<()> {
        let ctx = self
            .ctx
            .get()
            .cloned()
            .ok_or_else(|| BusError::Closed("processor.run called before init".into()))?;

        // 演示 1+2：并发启动"订阅温度事件 -> 请求统计"的消费任务
        let consumer = {
            let ctx = ctx.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move { consume_temperature(ctx, shutdown).await })
        };

        // 稍等消费者建立订阅后再开始其余演示，避免竞态
        tokio::time::sleep(Duration::from_millis(100)).await;

        tracing::info!("=== 演示 3: 参数 get/set + watch 热更新 ===");
        report(param_demo(&ctx).await, "param demo");

        tracing::info!("=== 演示 4a: 大文件背压流式传输（8MiB / 64KiB 分块 / 2 块缓冲）===");
        report(blob_stream_demo(&ctx).await, "blob stream demo");

        tracing::info!("=== 演示 4b: SharedBlob 句柄经事件通道零拷贝传递 ===");
        report(blob_handle_demo(&ctx).await, "blob handle demo");

        // 演示脚本执行完毕，等待停机信号，随后回收消费任务
        shutdown.cancelled().await;
        let _ = consumer.await;
        Ok(())
    }
}

fn report(r: Result<()>, what: &str) {
    if let Err(e) = r {
        tracing::error!(error = %e, "{what} failed");
    }
}

/// 演示 1+2：订阅温度事件（pub/sub 接收端），累计到 5 条后发起一次统计请求（请求/响应）。
async fn consume_temperature(ctx: ModuleContext, shutdown: CancellationToken) {
    let sub = match ctx.subscriber.subscribe(TEMPERATURE.into()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "subscribe temperature failed");
            return;
        }
    };
    let mut stream = Arc::clone(&sub).into_stream();
    let temperature = messages::temperature();
    let stats_svc = messages::stats_service();

    let mut count: u64 = 0;
    let mut stats_requested = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            maybe = stream.next() => {
                let Some(event) = maybe else { break };
                match temperature.take(&event) {
                    Ok(ev) => {
                        count += 1;
                        tracing::info!(
                            seq = count,
                            celsius = format!("{:.2}", ev.celsius),
                            from = %event.source,
                            "演示1(pub/sub): processor 收到温度事件"
                        );
                        if count == 5 && !stats_requested {
                            stats_requested = true;
                            match stats_svc
                                .request(ctx.requester.as_ref(), ctx.module_name.clone(), StatsRequest)
                                .await
                            {
                                Ok(stats) => tracing::info!(
                                    ?stats,
                                    "演示2(请求/响应): processor 请求 sensor.stats 成功"
                                ),
                                Err(e) => tracing::warn!(error = %e, "stats request failed"),
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "bad temperature payload"),
                }
            }
        }
    }
    tracing::info!(total = count, "temperature consumer stopped");
}

/// 演示 3：读取传感器参数 -> 修改（触发热更新）-> 读回验证 -> 读取不存在参数（预期报错）。
async fn param_demo(ctx: &ModuleContext) -> Result<()> {
    let before = ctx.params.get(SENSOR, "sample_interval_ms").await?;
    tracing::info!(value = %before, "get sensor.sample_interval_ms");

    ctx.params
        .set(SENSOR, "sample_interval_ms", Value::U64(200))
        .await?;
    tracing::info!("set sensor.sample_interval_ms = 200（sensor 侧将经 watch 热更新采样周期）");

    let after = ctx.params.get(SENSOR, "sample_interval_ms").await?;
    tracing::info!(value = %after, "get sensor.sample_interval_ms after set");

    let keys = ctx.params.list(SENSOR).await?;
    tracing::info!(?keys, "list sensor params");

    match ctx.params.get(SENSOR, "nonexistent").await {
        Err(BusError::ParamNotFound { .. }) => {
            tracing::info!("get sensor.nonexistent -> ParamNotFound（符合预期）");
        }
        other => tracing::warn!(?other, "unexpected result for nonexistent param"),
    }
    Ok(())
}

/// 演示 4a：大文件背压流。发送端逐块 send（缓冲满即 await 形成背压），
/// 接收端 collect_to_end 聚合完整数据后校验，体现"传完才能计算"。
async fn blob_stream_demo(ctx: &ModuleContext) -> Result<()> {
    const TOTAL: usize = 8 * 1024 * 1024; // 8 MiB
    const CHUNK: usize = 64 * 1024; // 64 KiB
    const BUF_CHUNKS: usize = 2; // 强背压：通道仅缓冲 2 块

    let data = pseudo_random_bytes(TOTAL);
    let expected = checksum(&data);

    let (sink, mut stream) = ctx.open_blob_stream("bigfile.bin", CHUNK, BUF_CHUNKS);
    tracing::info!(total = TOTAL, chunk = CHUNK, buf_chunks = BUF_CHUNKS, "open backpressured blob stream");

    // 发送任务：send_all 内部按 chunk_size 切分并逐块背压发送
    let sender = tokio::spawn(async move { send_all(sink, data).await });

    // 接收：直到发送端关闭，聚合成完整 Bytes 才返回
    let received = stream.collect_to_end().await?;
    let got = checksum(&received);
    let sent = sender
        .await
        .map_err(|e| BusError::Closed(format!("sender task panicked: {e}")))??;

    tracing::info!(
        sent_bytes = sent,
        received_bytes = received.len(),
        expected,
        got,
        checksum_ok = expected == got,
        "blob stream transfer complete, checksum verified"
    );
    Ok(())
}

/// 演示 4b：把数据写入 SharedBlob（共享内存），只把**句柄**经事件通道发布，
/// 接收端拿到句柄后 read() 得到完整数据（零拷贝），再校验。
async fn blob_handle_demo(ctx: &ModuleContext) -> Result<()> {
    // 先订阅自己即将发布的 topic，确保发布时有接收者
    let sub = ctx.subscriber.subscribe(BLOB_READY.into())?;
    let mut stream = Arc::clone(&sub).into_stream();

    // 写入共享内存 blob（写一次、读多次）
    let mut writer = ctx.blob_writer("shared.bin", 1024);
    writer.write_chunk(pseudo_random_bytes(512));
    writer.write_chunk(pseudo_random_bytes(512));
    let blob = writer.finish();
    let expected = checksum(&blob.read());
    let len = blob.len();

    // 经普通事件通道发布"句柄"而非数据本身
    let blob_ready = messages::blob_ready();
    blob_ready.publish(ctx.publisher.as_ref(), ctx.module_name.clone(), blob.clone())?;
    tracing::info!(len, "published SharedBlob handle via event channel");

    // 接收端拿到句柄后读取完整数据
    let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .map_err(|_| BusError::Timeout {
            topic: BLOB_READY.to_string(),
        })?
        .ok_or_else(|| BusError::Closed("blob_ready stream ended".into()))?;
    let received = blob_ready.take(&event)?;
    let got = checksum(&received.read());

    tracing::info!(
        len = received.len(),
        expected,
        got,
        checksum_ok = expected == got,
        "SharedBlob handle received, zero-copy read verified"
    );
    Ok(())
}

/// 确定性伪随机数据（xorshift），保证收发两端校验一致且可复现。
fn pseudo_random_bytes(len: usize) -> Bytes {
    let mut out = BytesMut::with_capacity(len);
    let mut state: u64 = 0x1234_5678_9abc_def0;
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out.freeze()
}

/// FNV-1a 风格滚动校验和。
fn checksum(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut cursor = data;
    while cursor.has_remaining() {
        let b = cursor.get_u8();
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
