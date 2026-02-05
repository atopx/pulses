use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use pipeline::broker::redis::RedisBroker;
use pipeline::broker::redis::RedisSubscription;
use pipeline::core::Broker;
use pipeline::core::BrokerMessage;
use redis::ErrorKind;
use redis::Value;

fn response_error(message: &str) -> redis::RedisError {
    redis::RedisError::from((ErrorKind::Client, "test assertion failed", message.to_owned()))
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
async fn redis_ack_and_reclaim() -> redis::RedisResult<()> {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set (example: redis://:password@host:6379)");

    let broker = RedisBroker::connect(redis_url).await?;
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).expect("time should be valid").as_nanos();

    let stream = format!("test_stream_{suffix}");
    let group = format!("test_group_{suffix}");
    let consumer_a = format!("test_consumer_a_{suffix}");
    let consumer_b = format!("test_consumer_b_{suffix}");

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
            .arg("payload")
            .arg(Bytes::from_static(br#"{"order_id":1}"#).as_ref())
            .query_async(&mut conn)
            .await?;

        let subscription_a = RedisSubscription {
            streams: vec![Arc::from(stream.as_str())],
            group: Arc::from(group.as_str()),
            consumer: Arc::from(consumer_a.as_str()),
            ..RedisSubscription::default()
        };

        let batch = broker.poll(&subscription_a).await?;
        let mut iter = batch.into_iter();
        let (_message, token) = iter.next().ok_or_else(|| response_error("expected one message"))?;
        if iter.next().is_some() {
            return Err(response_error("expected exactly one message"));
        }

        broker.ack(token).await?;

        let count_after_ack = pending_count(&mut conn, &stream, &group).await?;
        if count_after_ack != 0 {
            return Err(response_error("pending count should be zero after ack"));
        }

        let payload = Bytes::from_static(br#"{"order_id":2}"#);
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg(payload.as_ref())
            .query_async(&mut conn)
            .await?;

        let batch = broker.poll(&subscription_a).await?;
        let mut iter = batch.into_iter();
        let (message, _token) = iter.next().ok_or_else(|| response_error("expected second message"))?;
        if iter.next().is_some() {
            return Err(response_error("expected exactly one message for pending"));
        }

        let subscription_b = RedisSubscription {
            streams: vec![Arc::from(stream.as_str())],
            group: Arc::from(group.as_str()),
            consumer: Arc::from(consumer_b.as_str()),
            min_idle_ms: 0,
            ..RedisSubscription::default()
        };

        let reclaimed = broker.reclaim(&subscription_b).await?;
        let mut reclaimed_iter = reclaimed.into_iter();
        let (reclaimed_message, reclaimed_token) =
            reclaimed_iter.next().ok_or_else(|| response_error("expected reclaimed message"))?;
        if reclaimed_iter.next().is_some() {
            return Err(response_error("expected exactly one reclaimed message"));
        }

        if reclaimed_message.id() != message.id() {
            return Err(response_error("reclaimed id mismatch"));
        }
        if reclaimed_message.payload() != payload.as_ref() {
            return Err(response_error("reclaimed payload mismatch"));
        }

        broker.ack(reclaimed_token).await?;
        let count_after_reclaim_ack = pending_count(&mut conn, &stream, &group).await?;
        if count_after_reclaim_ack != 0 {
            return Err(response_error("pending count should be zero after reclaim ack"));
        }

        Ok(())
    }
    .await;

    let _cleanup_group: redis::RedisResult<()> =
        redis::cmd("XGROUP").arg("DESTROY").arg(&stream).arg(&group).query_async(&mut cleanup_conn).await;
    let _cleanup_stream: redis::RedisResult<()> = redis::cmd("DEL").arg(&stream).query_async(&mut cleanup_conn).await;

    result
}
