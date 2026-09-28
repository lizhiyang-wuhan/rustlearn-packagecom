//! bus-runtime 集成测试：覆盖请求/响应、发布订阅、参数、大文件四类语义的关键路径。

use std::sync::Arc;
use std::time::Duration;

use bus_api::blob::{BlobClient, send_all};
use bus_api::error::BusError;
use bus_api::event::{Event, Value};
use bus_api::param::{ParamClient, ParamProvider};
use bus_api::traits::{ErasedHandler, Requester, ServiceRegistry, Subscriber};
use bus_api::typed::{Typed, TypedService};
use bus_runtime::{MessageBus, ParamStore, SharedMemoryBlobClient};
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

#[derive(Clone, Debug, PartialEq)]
struct Ping(u32);
#[derive(Clone, Debug, PartialEq)]
struct Pong(u32);

/// 请求/响应：往返一次，handler 在独立任务中执行并回话。
#[tokio::test]
async fn request_response_roundtrip() {
    let bus = MessageBus::new();
    let svc: TypedService<Ping, Pong> = TypedService::new("test.echo");
    bus.register_handler(
        svc.topic().clone(),
        svc.erase(|p: Ping| async move { Ok(Pong(p.0 * 2)) }),
    )
    .unwrap();

    let resp = svc.request(&bus, "tester", Ping(21)).await.unwrap();
    assert_eq!(resp, Pong(42));
}

/// 无人注册的 topic -> NoHandler。
#[tokio::test]
async fn request_no_handler() {
    let bus = MessageBus::new();
    let svc: TypedService<Ping, Pong> = TypedService::new("test.missing");
    let err = svc.request(&bus, "tester", Ping(1)).await.unwrap_err();
    assert!(matches!(err, BusError::NoHandler { .. }), "got {err:?}");
}

/// 请求载荷类型与 handler 期望不符 -> TypeMismatch。
#[tokio::test]
async fn request_type_mismatch() {
    let bus = MessageBus::new();
    let svc: TypedService<Ping, Pong> = TypedService::new("test.mm");
    bus.register_handler(
        svc.topic().clone(),
        svc.erase(|p: Ping| async move { Ok(Pong(p.0)) }),
    )
    .unwrap();

    // 用 String 载荷请求一个期望 Ping 的服务
    let bad = Event::new("test.mm", "tester", Event::payload_arc("not a ping".to_string()));
    let err = bus.request(bad).await.unwrap_err();
    assert!(matches!(err, BusError::TypeMismatch { .. }), "got {err:?}");
}

/// handler 迟迟不回话 -> Timeout（受总线 request_timeout 约束）。
struct SlowHandler;
impl ErasedHandler for SlowHandler {
    fn handle(&self, _req: Event, reply: oneshot::Sender<bus_api::Result<Event>>) {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let _ = reply.send(Ok(Event::new("slow", "slow", Event::payload_arc(0u32))));
        });
    }
}

#[tokio::test]
async fn request_timeout() {
    let bus = MessageBus::builder()
        .request_timeout(Duration::from_millis(100))
        .build();
    bus.register_handler("slow".into(), Arc::new(SlowHandler)).unwrap();

    let req = Event::new("slow", "tester", Event::payload_arc(1u32));
    let err = bus.request(req).await.unwrap_err();
    assert!(matches!(err, BusError::Timeout { .. }), "got {err:?}");
}

/// 发布/订阅：多订阅者各自收到同一条广播（Event 载荷 Arc 廉价克隆）。
#[tokio::test]
async fn broadcast_multi_subscriber() {
    let bus = MessageBus::new();
    let t = Typed::<Ping>::new("test.pubsub");

    let s1 = bus.subscribe(t.topic().clone()).unwrap();
    let s2 = bus.subscribe(t.topic().clone()).unwrap();
    let mut st1 = Arc::clone(&s1).into_stream();
    let mut st2 = Arc::clone(&s2).into_stream();

    t.publish(&bus, "pub", Ping(1)).unwrap();
    t.publish(&bus, "pub", Ping(2)).unwrap();

    for expected in [1u32, 2] {
        let e1 = tokio::time::timeout(Duration::from_secs(1), st1.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t.take(&e1).unwrap(), Ping(expected));
        let e2 = tokio::time::timeout(Duration::from_secs(1), st2.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(t.take(&e2).unwrap(), Ping(expected));
    }
}

/// 慢消费者：灌爆小容量广播后，流不会卡死，仍能在跳过丢帧后收到新事件。
#[tokio::test]
async fn slow_consumer_recovers_after_lag() {
    let bus = MessageBus::builder().broadcast_capacity(2).build();
    let t = Typed::<Ping>::new("test.lag");

    let sub = bus.subscribe(t.topic().clone()).unwrap();
    let mut stream = Arc::clone(&sub).into_stream();

    // 远超容量的发布，制造 lag
    for i in 0..50u32 {
        let _ = t.publish(&bus, "pub", Ping(i));
    }
    // 标记消息：lag 之后应仍能收到
    let _ = t.publish(&bus, "pub", Ping(999));

    let mut got_marker = false;
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(ev)) if t.take(&ev).unwrap() == Ping(999) => {
                got_marker = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    assert!(got_marker, "stream should recover and deliver new events after lag");
}

/// 参数：declare/get/set + watch 热更新 + not found。
#[tokio::test]
async fn param_get_set_and_watch() {
    let store = ParamStore::new();
    let mut rx = store.declare("m", "k", Value::U64(1)).unwrap();
    rx.borrow_and_update(); // 标记初始值已读

    assert_eq!(store.get("m", "k").await.unwrap(), Value::U64(1));
    store.set("m", "k", Value::U64(42)).await.unwrap();
    assert_eq!(store.get("m", "k").await.unwrap(), Value::U64(42));

    // watch 感知到 set
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), Value::U64(42));

    // 列表
    assert_eq!(store.list("m").await.unwrap(), vec!["k".to_string()]);

    // 不存在
    assert!(matches!(
        store.get("m", "missing").await,
        Err(BusError::ParamNotFound { .. })
    ));
    assert!(matches!(
        store.get("nomod", "k").await,
        Err(BusError::ParamNotFound { .. })
    ));
}

/// 大文件：SharedBlob 写一次读多次，clone 共享同一份数据。
#[tokio::test]
async fn blob_writer_roundtrip() {
    let client = SharedMemoryBlobClient::new();
    let mut w = client.create_writer("f.bin".into(), 0);
    w.write_chunk(Bytes::from_static(b"hello "));
    w.write_chunk(Bytes::from_static(b"world"));
    let blob = w.finish();

    assert_eq!(blob.len(), 11);
    assert_eq!(&blob.read()[..], b"hello world");
    let cloned = blob.clone();
    assert_eq!(&cloned.read()[..], b"hello world");
}

/// 大文件：背压流分块发送，接收端 collect_to_end 聚合完整数据（顺序与内容一致）。
#[tokio::test]
async fn blob_stream_backpressure_and_complete() {
    let client = SharedMemoryBlobClient::new();
    // 仅 2 块缓冲 -> 强背压
    let (sink, mut stream) = client.open_stream("big".into(), 64 * 1024, 2);

    let total = 1024 * 1024; // 1 MiB
    let data = Bytes::from(vec![7u8; total]);
    let expected_len = data.len();

    let sender = tokio::spawn(async move { send_all(sink, data).await.unwrap() });
    let received = stream.collect_to_end().await.unwrap();
    let sent = sender.await.unwrap();

    assert_eq!(sent, expected_len);
    assert_eq!(received.len(), expected_len);
    assert!(received.iter().all(|&b| b == 7));
}

/// 大文件：发送端 finish（关闭）后，接收端能收到已发数据并正常结束。
#[tokio::test]
async fn blob_stream_finish_completes() {
    let client = SharedMemoryBlobClient::new();
    let (mut sink, mut stream) = client.open_stream("c".into(), 8, 2);
    sink.send(Bytes::from_static(b"abc")).await.unwrap();
    sink.finish(); // 关闭发送端 -> 流结束
    let got = stream.collect_to_end().await.unwrap();
    assert_eq!(&got[..], b"abc");
}

/// 大文件：传输中途取消。接收方被 abort（drop 掉 BlobStream）后，
/// 发送方后续 send 应感知通道关闭并返回 `Closed`，而非永久阻塞在背压上。
#[tokio::test]
async fn blob_stream_cancel_midway() {
    let client = SharedMemoryBlobClient::new();
    // 容量 2：先成功发送一块（部分进度），再取消接收方
    let (mut sink, stream) = client.open_stream("cancel".into(), 8, 2);

    // 接收方在后台聚合完整数据
    let receiver = tokio::spawn(async move {
        let mut s = stream;
        s.collect_to_end().await
    });

    // 中途已成功发送一块（进入"传输中"状态）
    sink.send(Bytes::from_static(b"partial1")).await.unwrap();

    // 接收方中途取消：abort 会 drop BlobStream，从而关闭 mpsc 通道
    receiver.abort();
    assert!(receiver.await.is_err(), "receiver task should be cancelled");

    // 取消后继续发送应观察到 Closed（对端已消失，背压不会永久挂起）
    let mut observed_closed = false;
    for _ in 0..8 {
        match sink.send(Bytes::from_static(b"partial2")).await {
            Err(BusError::Closed(_)) => {
                observed_closed = true;
                break;
            }
            Err(e) => panic!("unexpected error after cancel: {e:?}"),
            Ok(()) => continue,
        }
    }
    assert!(observed_closed, "sender should observe Closed after receiver cancelled");
}
