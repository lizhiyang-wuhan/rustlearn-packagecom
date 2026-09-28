//! 大文件传输的运行时实现：进程内共享内存 + tokio mpsc 背压流。

use std::sync::Arc;

use bus_api::blob::{BlobClient, BlobSink, BlobStream, BlobWriter};
use tokio::sync::mpsc;

/// 进程内共享内存 blob 客户端。
///
/// - [`create_writer`](BlobClient::create_writer)：返回基于 `BytesMut` 的写入端，
///   `finish` 后成为 `Arc<Bytes>` 共享缓冲（写一次读多次，零拷贝）。
/// - [`open_stream`](BlobClient::open_stream)：返回基于 `tokio::sync::mpsc` 有界通道的
///   背压流，缓冲满即阻塞发送方。
///
/// 跨进程演进：把本类型替换为基于 `memmap2` 匿名 mmap 的实现即可，`BlobClient` API 不变。
#[derive(Debug, Clone, Copy, Default)]
pub struct SharedMemoryBlobClient;

impl SharedMemoryBlobClient {
    pub fn new() -> Self {
        Self
    }

    /// 作为能力对象暴露。
    pub fn shared() -> Arc<dyn BlobClient> {
        Arc::new(Self)
    }
}

impl BlobClient for SharedMemoryBlobClient {
    fn create_writer(&self, name: String, capacity_hint: usize) -> BlobWriter {
        let name: Arc<str> = Arc::from(name);
        BlobWriter::new(name, capacity_hint)
    }

    fn open_stream(&self, name: String, chunk_size: usize, buf_chunks: usize) -> (BlobSink, BlobStream) {
        // 有界通道：容量 = buf_chunks，缓冲满时 send().await 挂起 → 背压
        let (tx, rx) = mpsc::channel::<bytes::Bytes>(buf_chunks.max(1));
        let name: Arc<str> = Arc::from(name);
        let sink = BlobSink::new(name.clone(), chunk_size.max(1), tx);
        let stream = BlobStream::new(name, rx);
        (sink, stream)
    }
}
