use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use pipeline::App;
use pipeline::AppConfig;
use pipeline::BackoffConfig;
use pipeline::broker::redis::RedisAckToken;
use pipeline::broker::redis::RedisBroker;
use pipeline::broker::redis::RedisMessage;
use pipeline::broker::redis::RedisSubscription;
use pipeline::core::Broker;
use pipeline::core::BrokerMessage;
use pipeline::core::Context;
use pipeline::core::Envelope;
use pipeline::core::Handler;
use pipeline::core::HandlerError;
use pipeline::core::Outcome;
use redis::ErrorKind;
use redis::RedisError;
use redis::Value;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const VISIBLE_STREAM: &str = "test_end_to_end_visible";

#[derive(Debug, Clone)]
struct RemapSubscription {
    inner: RedisSubscription,
    visible_stream: Arc<str>,
}

#[derive(Debug, Clone)]
struct RemapMessage {
    stream: Arc<str>,
    inner: RedisMessage,
}

impl BrokerMessage for RemapMessage {
    fn stream(&self) -> &str { self.stream.as_ref() }

    fn id(&self) -> &str { self.inner.id() }

    fn payload(&self) -> &[u8] { self.inner.payload() }

    fn headers(&self) -> &[(bytes::Bytes, bytes::Bytes)] { self.inner.headers() }

    fn ts_unix_ms(&self) -> u64 { self.inner.ts_unix_ms() }
}

#[derive(Debug, Clone)]
struct RemapBroker {
    inner: RedisBroker,
}

impl RemapBroker {
    fn new(inner: RedisBroker) -> Self { Self { inner } }
}

impl Broker for RemapBroker {
    type Message<'a>
        = RemapMessage
    where
        Self: 'a;
    type AckToken = RedisAckToken;
    type Subscription = RemapSubscription;
    type Error = RedisError;

    async fn poll<'a>(
        &'a self, sub: &'a Self::Subscription,
    ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
        let batch = self.inner.poll(&sub.inner).await?;
        let visible = sub.visible_stream.clone();
        let mapped = batch
            .into_iter()
            .map(|(message, token)| (RemapMessage { stream: visible.clone(), inner: message }, token))
            .collect::<Vec<_>>();
        Ok(mapped)
    }

    async fn ack(&self, token: Self::AckToken) -> Result<(), Self::Error> { self.inner.ack(token).await }

    async fn reclaim<'a>(
        &'a self, sub: &'a Self::Subscription,
    ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
        let batch = self.inner.reclaim(&sub.inner).await?;
        let visible = sub.visible_stream.clone();
        let mapped = batch
            .into_iter()
            .map(|(message, token)| (RemapMessage { stream: visible.clone(), inner: message }, token))
            .collect::<Vec<_>>();
        Ok(mapped)
    }
}

#[derive(Debug, Clone)]
struct RecordingHandler {
    processed_ids: Arc<Mutex<Vec<String>>>,
}

impl Handler<RemapBroker> for RecordingHandler {
    const STREAMS: &'static [&'static str] = &[VISIBLE_STREAM];

    async fn handle(&self, _ctx: &Context<RemapBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
        self.processed_ids.lock().await.push(msg.id.to_string());
        Ok(Outcome::Ack)
    }
}

fn response_error(message: &str) -> RedisError {
    RedisError::from((ErrorKind::Client, "test assertion failed", message.to_owned()))
}

fn value_to_u64(value: Value) -> redis::RedisResult<u64> {
    match value {
        Value::Int(value) => Ok(value as u64),
        Value::BulkString(bytes) => String::from_utf8(bytes)
            .map_err(|_| response_error("expected utf-8 bulk string"))?
            .parse::<u64>()
            .map_err(|_| response_error("expected u64 string value")),
        Value::SimpleString(text) => text.parse::<u64>().map_err(|_| response_error("expected u64 string value")),
        _ => Err(response_error("expected integer-like value")),
    }
}

async fn pending_count(
    conn: &mut redis::aio::MultiplexedConnection, stream: &str, group: &str,
) -> redis::RedisResult<u64> {
    let value: Value = redis::cmd("XPENDING").arg(stream).arg(group).query_async(conn).await?;
    match value {
        Value::Array(mut items) => {
            if items.is_empty() {
                return Err(response_error("expected pending summary array"));
            }
            value_to_u64(items.remove(0))
        }
        _ => Err(response_error("expected pending summary array")),
    }
}

#[tokio::test]
async fn redis_end_to_end() -> redis::RedisResult<()> {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set (example: redis://:password@host:6379)");

    let broker = RedisBroker::connect(redis_url).await?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).expect("time should be valid").as_nanos();

    let stream = format!("test_stream_end_to_end_{suffix}");
    let group = format!("test_group_end_to_end_{suffix}");
    let consumer = format!("test_consumer_end_to_end_{suffix}");

    let mut conn = broker.get_multiplexed_connection().await?;
    let mut cleanup_conn = broker.get_multiplexed_connection().await?;

    let cancellation = CancellationToken::new();
    let processed_ids = Arc::new(Mutex::new(Vec::new()));
    let app_completed = Arc::new(AtomicBool::new(false));

    let remap_broker = RemapBroker::new(broker.clone());
    let subscription = RemapSubscription {
        inner: RedisSubscription {
            streams: vec![Arc::from(stream.as_str())],
            group: Arc::from(group.as_str()),
            consumer: Arc::from(consumer.as_str()),
            block_ms: 200,
            count: 128,
            payload_key: Bytes::from_static(b"payload"),
            min_idle_ms: 60_000,
            reclaim_count: 128,
        },
        visible_stream: Arc::from(VISIBLE_STREAM),
    };

    let app = App::new(remap_broker, subscription)
        .with_config(AppConfig {
            dispatcher_capacity: 64,
            global_max_in_flight: 32,
            ack_retry_queue_capacity: 64,
            reclaim_interval_ms: 200,
            backoff: BackoffConfig { initial_ms: 5, max_ms: 50, factor: 2.0, jitter: 0.0 },
        })
        .register(RecordingHandler { processed_ids: processed_ids.clone() });

    let app_run = app.run(cancellation.clone());
    tokio::pin!(app_run);

    let app_completed_flag = app_completed.clone();
    let result = async {
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await?;

        let id_one: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg(b"{\"order_id\":1}")
            .query_async(&mut conn)
            .await?;

        let id_two: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg(b"{\"order_id\":2}")
            .query_async(&mut conn)
            .await?;

        let expected_ids = vec![id_one.clone(), id_two.clone()];

        timeout(Duration::from_secs(8), async {
            loop {
                let processed = processed_ids.lock().await.clone();
                let all_seen = expected_ids.iter().all(|id| processed.contains(id));
                if all_seen {
                    let mut pending_conn = broker.get_multiplexed_connection().await?;
                    let pending = pending_count(&mut pending_conn, &stream, &group).await?;
                    if pending == 0 {
                        return Ok::<(), RedisError>(());
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                    run_result = &mut app_run => {
                        app_completed_flag.store(true, Ordering::SeqCst);
                        return Err(response_error(&format!("app runtime exited unexpectedly: {run_result:?}")));
                    }
                }
            }
        })
        .await
        .map_err(|_| response_error("timeout waiting for app to process messages"))??;

        let processed = processed_ids.lock().await.clone();
        for id in &expected_ids {
            if !processed.contains(id) {
                return Err(response_error("processed ids missing expected message id"));
            }
        }

        cancellation.cancel();
        timeout(Duration::from_secs(2), &mut app_run)
            .await
            .map_err(|_| response_error("timeout waiting for app shutdown"))?
            .map_err(|error| response_error(&format!("app exited with error: {error}")))?;
        app_completed_flag.store(true, Ordering::SeqCst);

        Ok(())
    }
    .await;

    if !app_completed.load(Ordering::SeqCst) {
        cancellation.cancel();
        let _ = timeout(Duration::from_secs(1), &mut app_run).await;
        app_completed.store(true, Ordering::SeqCst);
    }

    let _cleanup_group: redis::RedisResult<()> =
        redis::cmd("XGROUP").arg("DESTROY").arg(&stream).arg(&group).query_async(&mut cleanup_conn).await;
    let _cleanup_stream: redis::RedisResult<()> = redis::cmd("DEL").arg(&stream).query_async(&mut cleanup_conn).await;

    result
}
