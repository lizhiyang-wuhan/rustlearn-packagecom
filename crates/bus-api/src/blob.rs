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
///
/// # 设计意图：为什么用 `Bytes` 而不是 `Vec<u8>`
///
/// `bytes::Bytes` 是一个**引用计数的不可变字节缓冲**：`clone()` 不拷贝数据，只增加
/// 内部引用计数（O(1)）。这正是"句柄经事件通道传递"需要的：把 `SharedBlob` 装进
/// `Arc<dyn Any>` 广播给多个订阅者时，每个订阅者拿到的都是指向同一块内存的句柄，
/// 零拷贝。`name` 用 `Arc<str>` 同理（共享字符串，clone 仅增计数）。
///
/// `#[derive(Clone)]` 在这里是廉价的（两个字段都是引用计数），所以放心派生——
/// 这与 [`crate::typed::Typed`] 手写 Clone 的情况不同（那里是为了避开 derive 的多余约束）。
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
///
/// # 设计意图："写完整、再交出"的类型化保证
///
/// `BlobWriter` 与 `SharedBlob` 是两个独立类型，这不是冗余，而是用**类型系统编码状态**：
/// 处于 `BlobWriter` 阶段的数据是"可变的、未完成的"，不能拿去读；只有调 `finish(self)`
/// （消费自身）才能得到不可变的 `SharedBlob`。因为 `finish` 拿走 `self` 所有权，
/// 编译器保证"冻结之后不能再写"——把"写一次读多次"的约束从文档约定升级为编译期事实。
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
    ///
    /// # 语法解读：`self`（而非 `&self`）与 `BytesMut::freeze`
    ///
    /// 参数是 `self`（拿走所有权），所以 `finish` 之后 `BlobWriter` 就被消耗、不能再用，
    /// 从类型上保证"写完即冻结"。`BytesMut::freeze()` 把可变的 `BytesMut` 转成不可变的
    /// `Bytes`，这个转换是 O(1) 的（只改内部标志，不拷数据），且之后 `Bytes` 可廉价 clone 共享。
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
///
/// # 设计意图：为什么把工厂抽象成 trait
///
/// 这是整个 blob 模块"可替换性"的关键。模块拿到的是 `Arc<dyn BlobClient>`，而不是具体的
/// `SharedMemoryBlobClient`。于是运行时可以把实现从"进程内 `Arc<Bytes>` 共享内存"换成
/// "跨进程 `memmap2` 匿名 mmap"，而这个 trait 的两个方法签名保持不变——上面两个业务模块
/// 一行都不用改。这就是"依赖抽象而非实现"在大文件传输上的落地。
///
/// # 为什么方法返回具体类型（BlobWriter/BlobSink）而不是 trait 对象
///
/// 注意与 `Publisher` 等 trait 不同，这里返回的是**具体结构体**。因为这些结构体本身就是
/// "数据载体的句柄"，它们的字段（`BytesMut`、`mpsc::Sender`）已经是抽象过的、与实现无关的
/// 类型；再包一层 `dyn` 只会增加间接性而无收益。真正需要可替换的是"生产这些句柄的工厂"，
/// 所以抽象点放在 `BlobClient` 这一层。
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
///
/// # 语法解读：`mut sink: BlobSink` 与 `Buf` 游标式切分
///
/// 参数 `mut sink` 按值接收并取得可变所有权——因为 `send(&mut self)` 需要 `&mut`，
/// 且函数末尾要 `sink.finish()`（消费 sink）。数据切分用 `bytes::Buf` 的游标 API：
/// `cursor.copy_to_bytes(n)` 从零拷贝地取出前 n 字节并推进游标，比手动 `&data[i..i+n]`
/// 切片更安全（不会越界、不拷贝）。`chunk_size.max(1)` 防止 0 造成死循环。
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
