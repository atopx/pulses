use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use pipeline::broker::redis::RedisBroker;
use pipeline::broker::redis::RedisSubscription;
use pipeline::core::Broker;
use pipeline::core::BrokerMessage;
use redis::ErrorKind;

fn response_error(message: &str) -> redis::RedisError {
    redis::RedisError::from((ErrorKind::Client, "test assertion failed", message.to_owned()))
}

#[tokio::test]
async fn redis_poll_parses_envelope() -> redis::RedisResult<()> {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set (example: redis://:password@host:6379)");

    let broker = RedisBroker::connect(redis_url).await?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).expect("time should be valid").as_nanos();

    let stream = format!("test_stream_{suffix}");
    let group = format!("test_group_{suffix}");
    let consumer = format!("test_consumer_{suffix}");
    let payload_field = Bytes::from_static(b"body");
    let payload = Bytes::from_static(br#"{"order_id":42}"#);
    let header_key = Bytes::from_static(b"tenant");
    let header_value = Bytes::from_static(b"t1");

    let mut conn = broker.get_multiplexed_connection().await?;
    let mut cleanup_conn = broker.get_multiplexed_connection().await?;

    let result = async {
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await?;

        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg(payload_field.as_ref())
            .arg(payload.as_ref())
            .arg(header_key.as_ref())
            .arg(header_value.as_ref())
            .query_async(&mut conn)
            .await?;

        let subscription = RedisSubscription {
            streams: vec![Arc::from(stream.as_str())],
            group: Arc::from(group.as_str()),
            consumer: Arc::from(consumer.as_str()),
            payload_key: payload_field.clone(),
            ..RedisSubscription::default()
        };

        let batch = broker.poll(&subscription).await?;
        let mut iter = batch.into_iter();
        let (message, token) = iter.next().ok_or_else(|| response_error("expected one message"))?;
        if iter.next().is_some() {
            return Err(response_error("expected exactly one message"));
        }

        if message.stream() != stream {
            return Err(response_error("stream mismatch"));
        }
        if message.id().is_empty() {
            return Err(response_error("id should not be empty"));
        }
        if message.payload() != payload.as_ref() {
            return Err(response_error("payload mismatch"));
        }

        let headers = message.headers();
        if headers.len() != 1 {
            return Err(response_error("headers length mismatch"));
        }
        if headers[0].0.as_ref() != header_key.as_ref() {
            return Err(response_error("header key mismatch"));
        }
        if headers[0].1.as_ref() != header_value.as_ref() {
            return Err(response_error("header value mismatch"));
        }

        let expected_ts = message.id().split_once('-').and_then(|(prefix, _)| prefix.parse::<u64>().ok()).unwrap_or(0);
        if message.ts_unix_ms() != expected_ts {
            return Err(response_error("ts_unix_ms mismatch"));
        }

        if token.stream.as_ref() != stream {
            return Err(response_error("ack token stream mismatch"));
        }
        if token.group.as_ref() != group {
            return Err(response_error("ack token group mismatch"));
        }
        if token.id.as_ref() != message.id() {
            return Err(response_error("ack token id mismatch"));
        }

        Ok(())
    }
    .await;

    let _cleanup_group: redis::RedisResult<()> =
        redis::cmd("XGROUP").arg("DESTROY").arg(&stream).arg(&group).query_async(&mut cleanup_conn).await;
    let _cleanup_stream: redis::RedisResult<()> = redis::cmd("DEL").arg(&stream).query_async(&mut cleanup_conn).await;

    result
}
