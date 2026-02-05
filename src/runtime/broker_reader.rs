use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::BackoffConfig;
use crate::core::Broker;
use crate::core::BrokerMessage;
use crate::core::Envelope;
use crate::dispatcher::DispatchItem;
use crate::handler_pool::HandlerTask;
use crate::handler_set::RoutingTable;

#[derive(Debug)]
pub struct BrokerReader<B>
where
    B: Broker,
{
    broker: B,
    subscription: B::Subscription,
    routing_table: RoutingTable,
    dispatch_tx: mpsc::Sender<DispatchItem<HandlerTask<B::AckToken>>>,
    backoff: BackoffConfig,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerReaderError {
    DispatcherChannelClosed,
}

impl std::fmt::Display for BrokerReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DispatcherChannelClosed => write!(f, "dispatcher input channel is closed"),
        }
    }
}

impl std::error::Error for BrokerReaderError {}

impl<B> BrokerReader<B>
where
    B: Broker,
{
    pub fn new(
        broker: B, subscription: B::Subscription, routing_table: RoutingTable,
        dispatch_tx: mpsc::Sender<DispatchItem<HandlerTask<B::AckToken>>>, backoff: BackoffConfig,
        cancellation: CancellationToken,
    ) -> Self {
        Self { broker, subscription, routing_table, dispatch_tx, backoff, cancellation }
    }

    pub async fn run(self) -> Result<(), BrokerReaderError> {
        let mut attempt = 1_u32;

        loop {
            let poll_result = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Ok(());
                }
                poll_result = self.broker.poll(&self.subscription) => poll_result,
            };

            match poll_result {
                Ok(batch) => {
                    attempt = 1;

                    for (message, ack_token) in batch.into_iter() {
                        let route_mask = match self.routing_table.get(message.stream()) {
                            Some(route_mask) => *route_mask,
                            None => continue,
                        };

                        let envelope = envelope_from_message(&message);
                        let dispatch_item = DispatchItem { route_mask, payload: HandlerTask { envelope, ack_token } };

                        tokio::select! {
                            _ = self.cancellation.cancelled() => {
                                return Ok(());
                            }
                            send_result = self.dispatch_tx.send(dispatch_item) => {
                                if send_result.is_err() {
                                    return Err(BrokerReaderError::DispatcherChannelClosed);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    eprintln!("broker poll failed: {error}");
                    let sleep_duration = backoff_with_full_jitter(&self.backoff, attempt);
                    attempt = attempt.saturating_add(1);

                    tokio::select! {
                        _ = self.cancellation.cancelled() => {
                            return Ok(());
                        }
                        _ = tokio::time::sleep(sleep_duration) => {}
                    }
                }
            }
        }
    }
}

fn envelope_from_message(message: &impl BrokerMessage) -> Envelope {
    let mut headers = SmallVec::<[(Bytes, Bytes); 8]>::new();
    headers.extend(message.headers().iter().cloned());

    Envelope {
        stream: Arc::<str>::from(message.stream()),
        id: Arc::<str>::from(message.id()),
        payload: Bytes::copy_from_slice(message.payload()),
        headers,
        ts_unix_ms: message.ts_unix_ms(),
    }
}

fn backoff_with_full_jitter(config: &BackoffConfig, attempt: u32) -> Duration {
    let initial_ms = config.initial_ms.max(1);
    let max_ms = config.max_ms.max(initial_ms);
    let factor = config.factor.max(1.0);

    let growth = factor.powi((attempt.saturating_sub(1)) as i32);
    let base_ms = (initial_ms as f64 * growth).min(max_ms as f64);

    let jitter = config.jitter.clamp(0.0, 1.0);
    let lower_ms = base_ms * (1.0 - jitter);
    let upper_ms = base_ms * (1.0 + jitter);

    let jittered_ms =
        if upper_ms <= lower_ms { lower_ms } else { pseudo_random_unit() * (upper_ms - lower_ms) + lower_ms };

    Duration::from_millis(jittered_ms.max(0.0).round() as u64)
}

fn pseudo_random_unit() -> f64 {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0_u128, |duration| duration.as_nanos());
    let mut state = nanos as u64;
    state ^= state.rotate_left(13);
    state ^= state.rotate_right(7);
    state ^= state.rotate_left(17);

    state as f64 / u64::MAX as f64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::config::BackoffConfig;
    use crate::core::Broker;
    use crate::core::BrokerMessage;
    use crate::handler_set::RoutingTable;
    use crate::runtime::broker_reader::BrokerReader;

    #[derive(Debug, Clone)]
    struct MockMessage {
        stream: &'static str,
        id: &'static str,
        payload: &'static [u8],
        headers: Vec<(Bytes, Bytes)>,
        ts_unix_ms: u64,
    }

    impl BrokerMessage for MockMessage {
        fn stream(&self) -> &str { self.stream }

        fn id(&self) -> &str { self.id }

        fn payload(&self) -> &[u8] { self.payload }

        fn headers(&self) -> &[(Bytes, Bytes)] { &self.headers }

        fn ts_unix_ms(&self) -> u64 { self.ts_unix_ms }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockAckToken {
        stream: String,
        group: String,
        id: String,
    }

    #[derive(Debug, Clone, Default)]
    struct MockSubscription;

    #[derive(Debug, Clone, Default)]
    struct MockBroker {
        polls: Arc<AtomicUsize>,
    }

    #[derive(Debug, Clone)]
    struct MockBrokerError;

    impl std::fmt::Display for MockBrokerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "mock broker poll error") }
    }

    impl std::error::Error for MockBrokerError {}

    impl Broker for MockBroker {
        type Message<'a>
            = MockMessage
        where
            Self: 'a;
        type AckToken = MockAckToken;
        type Subscription = MockSubscription;
        type Error = MockBrokerError;

        async fn poll<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            let poll_index = self.polls.fetch_add(1, Ordering::SeqCst);
            if poll_index == 0 {
                let message = MockMessage {
                    stream: "orders",
                    id: "1731016450000-1",
                    payload: br#"{"order_id":42}"#,
                    headers: vec![(Bytes::from_static(b"tenant"), Bytes::from_static(b"t1"))],
                    ts_unix_ms: 1_731_016_450_000,
                };
                let token = MockAckToken {
                    stream: "orders".to_owned(),
                    group: "g1".to_owned(),
                    id: "1731016450000-1".to_owned(),
                };

                return Ok(vec![(message, token)]);
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(Vec::<(MockMessage, MockAckToken)>::new())
        }

        async fn ack(&self, _token: Self::AckToken) -> Result<(), Self::Error> { Ok(()) }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(Vec::<(MockMessage, MockAckToken)>::new())
        }
    }

    #[tokio::test]
    async fn broker_reader_sends_items() {
        let broker = MockBroker::default();
        let subscription = MockSubscription;
        let mut routing_table = RoutingTable::new();
        routing_table.insert(Arc::<str>::from("orders"), 1_u128 << 0);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(4);
        let cancellation = CancellationToken::new();
        let reader = BrokerReader::new(
            broker,
            subscription,
            routing_table,
            dispatch_tx,
            BackoffConfig { initial_ms: 5, max_ms: 20, factor: 2.0, jitter: 0.0 },
            cancellation.clone(),
        );

        let reader_run = reader.run();
        tokio::pin!(reader_run);

        let dispatched = timeout(Duration::from_secs(1), async {
            let mut maybe_item = None;
            while maybe_item.is_none() {
                tokio::select! {
                    recv_item = dispatch_rx.recv() => {
                        maybe_item = recv_item;
                    }
                    run_result = &mut reader_run => {
                        panic!("broker reader exited unexpectedly: {run_result:?}");
                    }
                }
            }

            maybe_item.expect("dispatcher channel should receive one item")
        })
        .await
        .expect("broker reader should dispatch one item in time");

        assert_eq!(1_u128 << 0, dispatched.route_mask);
        assert_eq!("orders", dispatched.payload.envelope.stream.as_ref());
        assert_eq!("1731016450000-1", dispatched.payload.envelope.id.as_ref());
        assert_eq!(br#"{"order_id":42}"#, dispatched.payload.envelope.payload.as_ref());
        assert_eq!(1, dispatched.payload.envelope.headers.len());
        assert_eq!((Bytes::from_static(b"tenant"), Bytes::from_static(b"t1")), dispatched.payload.envelope.headers[0]);
        assert_eq!(1_731_016_450_000, dispatched.payload.envelope.ts_unix_ms);
        assert_eq!(
            MockAckToken { stream: "orders".to_owned(), group: "g1".to_owned(), id: "1731016450000-1".to_owned() },
            dispatched.payload.ack_token
        );

        cancellation.cancel();
        drop(dispatch_rx);

        timeout(Duration::from_secs(1), &mut reader_run)
            .await
            .expect("broker reader task should stop after cancellation")
            .expect("broker reader should exit cleanly");
    }
}
