use std::sync::Arc;

use bytes::Bytes;
use redis::ErrorKind;
use redis::RedisError;
use redis::RedisResult;
use redis::Value;
use redis::aio::MultiplexedConnection;

use crate::core::Broker;
use crate::core::BrokerMessage;

#[derive(Debug, Clone)]
pub struct RedisSubscription {
    pub streams: Vec<Arc<str>>,
    pub group: Arc<str>,
    pub consumer: Arc<str>,
    pub block_ms: u64,
    pub count: usize,
    pub payload_key: Bytes,
    pub min_idle_ms: u64,
    pub reclaim_count: usize,
}

impl Default for RedisSubscription {
    fn default() -> Self {
        Self {
            streams: Vec::new(),
            group: Arc::<str>::from("default-group"),
            consumer: Arc::<str>::from("default-consumer"),
            block_ms: 0,
            count: 256,
            payload_key: Bytes::from_static(b"payload"),
            min_idle_ms: 60_000,
            reclaim_count: 256,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisAckToken {
    pub stream: Arc<str>,
    pub group: Arc<str>,
    pub id: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct RedisMessage {
    stream: Arc<str>,
    id: Arc<str>,
    payload: Bytes,
    headers: Vec<(Bytes, Bytes)>,
    ts_unix_ms: u64,
}

impl BrokerMessage for RedisMessage {
    fn stream(&self) -> &str { self.stream.as_ref() }

    fn id(&self) -> &str { self.id.as_ref() }

    fn payload(&self) -> &[u8] { self.payload.as_ref() }

    fn headers(&self) -> &[(Bytes, Bytes)] { &self.headers }

    fn ts_unix_ms(&self) -> u64 { self.ts_unix_ms }
}

#[derive(Clone)]
pub struct RedisBroker {
    redis_url: Arc<str>,
    client: redis::Client,
}

impl std::fmt::Debug for RedisBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisBroker").field("redis_url", &self.redis_url).finish()
    }
}

impl RedisBroker {
    pub fn new(redis_url: impl Into<Arc<str>>) -> RedisResult<Self> {
        let redis_url = redis_url.into();
        let client = redis::Client::open(redis_url.as_ref())?;
        Ok(Self { redis_url, client })
    }

    pub async fn connect(redis_url: impl Into<Arc<str>>) -> RedisResult<Self> {
        let broker = Self::new(redis_url)?;
        let _ = broker.ping().await?;
        Ok(broker)
    }

    pub fn redis_url(&self) -> &str { self.redis_url.as_ref() }

    pub async fn get_multiplexed_connection(&self) -> RedisResult<MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    pub async fn ping(&self) -> RedisResult<String> {
        let mut conn = self.get_multiplexed_connection().await?;
        redis::cmd("PING").query_async(&mut conn).await
    }

    async fn ensure_groups(&self, sub: &RedisSubscription, conn: &mut MultiplexedConnection) -> RedisResult<()> {
        for stream in &sub.streams {
            let _: RedisResult<()> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream.as_ref())
                .arg(sub.group.as_ref())
                .arg("0")
                .arg("MKSTREAM")
                .query_async(conn)
                .await;
        }
        Ok(())
    }
}

impl Broker for RedisBroker {
    type Message<'a>
        = RedisMessage
    where
        Self: 'a;
    type AckToken = RedisAckToken;
    type Subscription = RedisSubscription;
    type Error = RedisError;

    async fn poll<'a>(
        &'a self, sub: &'a Self::Subscription,
    ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
        if sub.streams.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.get_multiplexed_connection().await?;
        let stream_keys: Vec<&str> = sub.streams.iter().map(|stream| stream.as_ref()).collect();
        let stream_ids: Vec<&str> = vec![">"; stream_keys.len()];

        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP").arg(sub.group.as_ref()).arg(sub.consumer.as_ref());
        if sub.count > 0 {
            cmd.arg("COUNT").arg(sub.count);
        }
        if sub.block_ms > 0 {
            cmd.arg("BLOCK").arg(sub.block_ms);
        }
        cmd.arg("STREAMS").arg(&stream_keys).arg(&stream_ids);

        let reply: Value = match cmd.query_async(&mut conn).await {
            Ok(reply) => reply,
            Err(e) => {
                if e.to_string().contains("NOGROUP") {
                    self.ensure_groups(sub, &mut conn).await?;
                    cmd.query_async(&mut conn).await?
                } else {
                    return Err(e);
                }
            }
        };

        parse_read_reply(reply, &sub.group, &sub.payload_key)
    }

    async fn ack(&self, _token: Self::AckToken) -> Result<(), Self::Error> {
        let RedisAckToken { stream, group, id } = _token;
        let mut conn = self.get_multiplexed_connection().await?;
        let acked: i64 =
            redis::cmd("XACK").arg(stream.as_ref()).arg(group.as_ref()).arg(id.as_ref()).query_async(&mut conn).await?;
        if acked == 0 {
            eprintln!(
                "warn: redis xack returned 0 (stream={}, group={}, id={})",
                stream.as_ref(),
                group.as_ref(),
                id.as_ref()
            );
        }
        Ok(())
    }

    async fn reclaim<'a>(
        &'a self, sub: &'a Self::Subscription,
    ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
        if sub.streams.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.get_multiplexed_connection().await?;
        let mut reclaimed = Vec::new();

        for stream in &sub.streams {
            let mut cmd = redis::cmd("XAUTOCLAIM");
            cmd.arg(stream.as_ref()).arg(sub.group.as_ref()).arg(sub.consumer.as_ref()).arg(sub.min_idle_ms).arg("0-0");
            if sub.reclaim_count > 0 {
                cmd.arg("COUNT").arg(sub.reclaim_count);
            }

            let reply: Value = match cmd.query_async(&mut conn).await {
                Ok(reply) => reply,
                Err(e) => {
                    if e.to_string().contains("NOGROUP") {
                        self.ensure_groups(sub, &mut conn).await?;
                        // Re-create command because cmd was consumed or we need fresh one?
                        // Actually redis::Cmd is reusable if we didn't consume it?
                        // query_async takes &self.
                        // But wait, in the loop we create new cmd every time.
                        let mut retry_cmd = redis::cmd("XAUTOCLAIM");
                        retry_cmd
                            .arg(stream.as_ref())
                            .arg(sub.group.as_ref())
                            .arg(sub.consumer.as_ref())
                            .arg(sub.min_idle_ms)
                            .arg("0-0");
                        if sub.reclaim_count > 0 {
                            retry_cmd.arg("COUNT").arg(sub.reclaim_count);
                        }
                        retry_cmd.query_async(&mut conn).await?
                    } else {
                        return Err(e);
                    }
                }
            };
            let mut batch = parse_autoclaim_reply(reply, stream, &sub.group, &sub.payload_key)?;
            reclaimed.append(&mut batch);
        }

        Ok(reclaimed)
    }
}

fn parse_read_reply(value: Value, group: &Arc<str>, payload_key: &Bytes) -> RedisResult<Vec<(RedisMessage, RedisAckToken)>> {
    let mut messages = Vec::new();
    match value {
        Value::Nil => Ok(messages),
        Value::Array(streams) => {
            for stream in streams {
                let Value::Array(mut items) = stream else {
                    return Err(unexpected_return("expected stream array"));
                };

                if items.len() != 2 {
                    return Err(unexpected_return("expected stream array of length 2"));
                }

                let entries_value = items.pop().expect("stream entries");
                let stream_value = items.pop().expect("stream key");
                let stream_name = value_to_string(stream_value)?;
                let stream_arc: Arc<str> = Arc::from(stream_name);

                let Value::Array(entries) = entries_value else {
                    return Err(unexpected_return("expected entries array"));
                };

                for entry in entries {
                    let Value::Array(mut entry_items) = entry else {
                        return Err(unexpected_return("expected entry array"));
                    };

                    if entry_items.len() != 2 {
                        return Err(unexpected_return("expected entry array of length 2"));
                    }

                    let fields_value = entry_items.pop().expect("entry fields");
                    let id_value = entry_items.pop().expect("entry id");
                    let id_string = value_to_string(id_value)?;
                    let id_arc: Arc<str> = Arc::from(id_string);
                    let ts_unix_ms = parse_ts_unix_ms(id_arc.as_ref());
                    let (payload, headers) = parse_entry_fields(fields_value, payload_key)?;

                    let message =
                        RedisMessage { stream: stream_arc.clone(), id: id_arc.clone(), payload, headers, ts_unix_ms };
                    let token = RedisAckToken { stream: stream_arc.clone(), group: group.clone(), id: id_arc.clone() };
                    messages.push((message, token));
                }
            }
            Ok(messages)
        }
        _ => Err(unexpected_return("expected array or nil reply")),
    }
}

fn parse_entry_fields(value: Value, payload_key: &Bytes) -> RedisResult<(Bytes, Vec<(Bytes, Bytes)>)> {
    let Value::Array(items) = value else {
        return Err(unexpected_return("expected entry fields array"));
    };

    if items.len() % 2 != 0 {
        return Err(unexpected_return("expected even number of field/value items"));
    }

    let mut payload = Bytes::new();
    let mut payload_taken = false;
    let mut headers = Vec::with_capacity(items.len() / 2);
    let mut iter = items.into_iter();

    while let Some(field) = iter.next() {
        let value = iter.next().expect("field/value pairs");
        let key_bytes = value_to_bytes(field)?;
        let value_bytes = value_to_bytes(value)?;

        if !payload_taken && key_bytes.as_ref() == payload_key.as_ref() {
            payload = value_bytes;
            payload_taken = true;
        } else {
            headers.push((key_bytes, value_bytes));
        }
    }

    Ok((payload, headers))
}

fn parse_autoclaim_reply(
    value: Value, stream: &Arc<str>, group: &Arc<str>, payload_key: &Bytes,
) -> RedisResult<Vec<(RedisMessage, RedisAckToken)>> {
    let mut messages = Vec::new();
    match value {
        Value::Nil => Ok(messages),
        Value::Array(items) => {
            if items.len() < 2 {
                return Err(unexpected_return("expected autoclaim array length >= 2"));
            }
            let mut iter = items.into_iter();
            let _next_start = iter.next().expect("next start id");
            let entries_value = iter.next().expect("entries array");

            let Value::Array(entries) = entries_value else {
                return Err(unexpected_return("expected autoclaim entries array"));
            };

            for entry in entries {
                let Value::Array(mut entry_items) = entry else {
                    return Err(unexpected_return("expected autoclaim entry array"));
                };

                if entry_items.len() != 2 {
                    return Err(unexpected_return("expected autoclaim entry array of length 2"));
                }

                let fields_value = entry_items.pop().expect("entry fields");
                let id_value = entry_items.pop().expect("entry id");
                let id_string = value_to_string(id_value)?;
                let id_arc: Arc<str> = Arc::from(id_string);
                let ts_unix_ms = parse_ts_unix_ms(id_arc.as_ref());
                let (payload, headers) = parse_entry_fields(fields_value, payload_key)?;

                let message = RedisMessage { stream: stream.clone(), id: id_arc.clone(), payload, headers, ts_unix_ms };
                let token = RedisAckToken { stream: stream.clone(), group: group.clone(), id: id_arc.clone() };
                messages.push((message, token));
            }

            Ok(messages)
        }
        _ => Err(unexpected_return("expected autoclaim array or nil reply")),
    }
}

fn value_to_string(value: Value) -> RedisResult<String> {
    match value {
        Value::BulkString(bytes) => {
            String::from_utf8(bytes).map_err(|_| unexpected_return("expected utf-8 bulk string"))
        }
        Value::SimpleString(text) => Ok(text),
        Value::Int(value) => Ok(value.to_string()),
        Value::Nil => Err(unexpected_return("unexpected nil for string value")),
        _ => Err(unexpected_return("expected string-like value")),
    }
}

fn value_to_bytes(value: Value) -> RedisResult<Bytes> {
    match value {
        Value::BulkString(bytes) => Ok(Bytes::from(bytes)),
        Value::SimpleString(text) => Ok(Bytes::from(text.into_bytes())),
        Value::Int(value) => Ok(Bytes::from(value.to_string().into_bytes())),
        Value::Nil => Err(unexpected_return("unexpected nil for bytes value")),
        _ => Err(unexpected_return("expected bulk string value")),
    }
}

fn parse_ts_unix_ms(id: &str) -> u64 {
    let Some((prefix, _)) = id.split_once('-') else {
        eprintln!("warn: failed to parse redis entry id timestamp: {id}");
        return 0;
    };

    match prefix.parse::<u64>() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("warn: failed to parse redis entry id timestamp: {id}");
            0
        }
    }
}

fn unexpected_return(message: &str) -> RedisError {
    RedisError::from((ErrorKind::UnexpectedReturnType, "redis reply parse error", message.to_owned()))
}
