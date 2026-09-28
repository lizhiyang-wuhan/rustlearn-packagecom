//! 大文件传输（共享内存语义）。
//!
//! 设计原则：通道只适合传简单消息与数据 payload；大文件走"共享内存 + 背压流"，
//! 且**接收方必须拿到完整数据后才能进行计算**。因此这里的数据载体都是
//! "写完整、再交出"的形态：
//!
//! - [`SharedBlob`]：写一次读多次的共享缓冲（`Arc<Bytes>`），通过普通事件通道
//!   传递**句柄**而非数据本身，零拷贝、零序列化。适合放得进内存的文件。
//! - [`BlobSink`] / [`BlobStream`]：带背压的分块流。发送方逐块 `send`（缓冲满即 await
//!   形成背压），接收方 `collect_to_end` 持续接收直到流结束，**聚合成完整 `Bytes`
//!   后才返回**，内存占用受 `chunk_size * buf_chunks` 上限约束。适合超大文件。
//!
//! 可替换的抽象是工厂 [`BlobClient`]：当前运行时给出进程内共享内存实现；
//! 未来跨进程可替换为 `memmap2` 匿名 mmap / 共享内存段实现，本模块 API 不变。

use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::error::{BusError, Result};

/// 写一次、读多次的共享 blob。内部 `Arc<Bytes>`，clone 仅增加引用计数。
#[derive(Clone)]
pub struct SharedBlob {
    name: Arc<str>,
    data: Bytes,
}

impl SharedBlob {
    /// 由已完整的字节构造。
    pub fn new(name: impl Into<Arc<str>>, data: Bytes) -> Self {
        Self {
            name: name.into(),
            data,
        }
    }

    /// blob 名称（用于日志/检索）。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 读取完整数据。`Bytes` clone 是引用计数操作，零拷贝。
    pub fn read(&self) -> Bytes {
        self.data.clone()
    }

    /// 数据总长度（字节）。
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl std::fmt::Debug for SharedBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBlob")
            .field("name", &self.name)
            .field("len", &self.data.len())
            .finish()
    }
}

/// [`SharedBlob`] 的写入端：累积 chunk，`finish` 后一次性交出完整数据。
pub struct BlobWriter {
    name: Arc<str>,
    buf: BytesMut,
}

impl BlobWriter {
    /// 由运行时实现方（[`BlobClient`]）调用创建；业务模块通常经
    /// `ModuleContext::blob_writer` 获取。
    pub fn new(name: impl Into<Arc<str>>, capacity_hint: usize) -> Self {
        Self {
            name: name.into(),
            buf: BytesMut::with_capacity(capacity_hint),
        }
    }

    /// 追加一块数据（拷贝进内部缓冲；发送方通常逐块读文件后调用）。
    pub fn write_chunk(&mut self, chunk: Bytes) {
        self.buf.extend_from_slice(&chunk);
    }

    /// 已写入的字节数。
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// 冻结为共享 blob（此后不可再写，可被多方零拷贝读取）。
    pub fn finish(self) -> SharedBlob {
        SharedBlob {
            name: self.name,
            data: self.buf.freeze(),
        }
    }
}

/// 背压流的发送端。底层是 `tokio::sync::mpsc` 有界通道，缓冲满时 `send` await。
pub struct BlobSink {
    name: Arc<str>,
    chunk_size: usize,
    tx: mpsc::Sender<Bytes>,
}

impl BlobSink {
    /// 由运行时实现方（[`BlobClient`]）调用创建；`tx` 为背压流的有界发送端。
    pub fn new(name: impl Into<Arc<str>>, chunk_size: usize, tx: mpsc::Sender<Bytes>) -> Self {
        Self { name: name.into(), chunk_size, tx }
    }

    /// 建议的分块大小（发送方可据此切分数据）。
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// blob 名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 发送一块数据。缓冲已满时在此 await（形成背压）。
    /// 接收端已关闭时返回 `Closed`。
    pub async fn send(&mut self, chunk: Bytes) -> Result<()> {
        self.tx.send(chunk).await.map_err(|e| BusError::Closed(e.to_string()))
    }

    /// 主动结束流：drop 发送端即关闭通道，接收端 `collect_to_end` 随之完成。
    pub fn finish(self) {
        drop(self.tx);
    }
}

/// 背压流的接收端。`collect_to_end` 聚合全部 chunk 成完整 `Bytes` 后才返回，
/// 保证"接收方拿到完整数据后才能计算"的语义。
pub struct BlobStream {
    name: Arc<str>,
    rx: mpsc::Receiver<Bytes>,
}

impl BlobStream {
    /// 由运行时实现方（[`BlobClient`]）调用创建；`rx` 为背压流的接收端。
    pub fn new(name: impl Into<Arc<str>>, rx: mpsc::Receiver<Bytes>) -> Self {
        Self { name: name.into(), rx }
    }

    /// blob 名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 持续接收直到发送端关闭，聚合为完整数据后返回。
    pub async fn collect_to_end(&mut self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = self.rx.recv().await {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }
}

/// 大文件传输客户端（可替换的抽象工厂）。
///
/// 模块通过它拿到写入端 / 背压流，而不接触任何具体通道或内存实现。
pub trait BlobClient: Send + Sync {
    /// 创建共享内存 blob 的写入端（`capacity_hint` 用于预分配，可为 0）。
    fn create_writer(&self, name: String, capacity_hint: usize) -> BlobWriter;

    /// 打开一条带背压的分块传输流。
    /// - `chunk_size`：建议分块大小；
    /// - `buf_chunks`：通道缓冲的块数（越小背压越强，内存占用上限约为 `chunk_size * buf_chunks`）。
    fn open_stream(&self, name: String, chunk_size: usize, buf_chunks: usize) -> (BlobSink, BlobStream);
}

/// 便捷函数：把一段完整数据按 `chunk_size` 切分，通过 `sink` 逐块背压发送。
///
/// 演示"大文件分块 + 背压"的常见发送模式；`sink` 在数据发完后自动结束。
pub async fn send_all(mut sink: BlobSink, data: Bytes) -> Result<usize> {
    let chunk_size = sink.chunk_size().max(1);
    let total = data.len();
    let mut cursor = data;
    while cursor.has_remaining() {
        let n = chunk_size.min(cursor.remaining());
        let chunk = cursor.copy_to_bytes(n);
        sink.send(chunk).await?;
    }
    sink.finish();
    Ok(total)
}
