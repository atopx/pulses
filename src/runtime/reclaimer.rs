use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::Broker;
use crate::core::BrokerMessage;
use crate::core::Envelope;
use crate::dispatcher::DispatchItem;
use crate::handler_pool::HandlerTask;
use crate::handler_set::RoutingTable;

#[derive(Debug)]
pub struct Reclaimer<B>
where
    B: Broker,
{
    broker: B,
    subscription: B::Subscription,
    routing_table: RoutingTable,
    dispatch_tx: mpsc::Sender<DispatchItem<HandlerTask<B::AckToken>>>,
    reclaim_interval: Duration,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimerError {
    DispatcherChannelClosed,
}

impl std::fmt::Display for ReclaimerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DispatcherChannelClosed => write!(f, "dispatcher input channel is closed"),
        }
    }
}

impl std::error::Error for ReclaimerError {}

impl<B> Reclaimer<B>
where
    B: Broker,
{
    pub fn new(
        broker: B, subscription: B::Subscription, routing_table: RoutingTable,
        dispatch_tx: mpsc::Sender<DispatchItem<HandlerTask<B::AckToken>>>, reclaim_interval: Duration,
        cancellation: CancellationToken,
    ) -> Self {
        Self { broker, subscription, routing_table, dispatch_tx, reclaim_interval, cancellation }
    }

    pub async fn run(self) -> Result<(), ReclaimerError> {
        let mut ticker = tokio::time::interval(self.reclaim_interval.max(Duration::from_millis(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Ok(());
                }
                _ = ticker.tick() => {}
            }

            let reclaim_result = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Ok(());
                }
                reclaim_result = self.broker.reclaim(&self.subscription) => reclaim_result,
            };

            match reclaim_result {
                Ok(batch) => {
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
                                    return Err(ReclaimerError::DispatcherChannelClosed);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    eprintln!("broker reclaim failed: {error}");
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

    use crate::core::Broker;
    use crate::core::BrokerMessage;
    use crate::handler_set::RoutingTable;
    use crate::runtime::reclaimer::Reclaimer;

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
        reclaims: Arc<AtomicUsize>,
    }

    #[derive(Debug, Clone)]
    struct MockBrokerError;

    impl std::fmt::Display for MockBrokerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "mock broker reclaim error") }
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
            Ok(Vec::<(MockMessage, MockAckToken)>::new())
        }

        async fn ack(&self, _token: Self::AckToken) -> Result<(), Self::Error> { Ok(()) }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            let reclaim_index = self.reclaims.fetch_add(1, Ordering::SeqCst);
            if reclaim_index == 0 {
                let message = MockMessage {
                    stream: "orders",
                    id: "1731016450000-3",
                    payload: br#"{"order_id":43}"#,
                    headers: vec![(Bytes::from_static(b"tenant"), Bytes::from_static(b"t1"))],
                    ts_unix_ms: 1_731_016_450_000,
                };
                let token = MockAckToken {
                    stream: "orders".to_owned(),
                    group: "g1".to_owned(),
                    id: "1731016450000-3".to_owned(),
                };
                return Ok(vec![(message, token)]);
            }

            Ok(Vec::<(MockMessage, MockAckToken)>::new())
        }
    }

    #[tokio::test]
    async fn reclaimer_smoke_with_mock_broker() {
        let broker = MockBroker::default();
        let subscription = MockSubscription;
        let mut routing_table = RoutingTable::new();
        routing_table.insert(Arc::<str>::from("orders"), 1_u128 << 0);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(4);
        let cancellation = CancellationToken::new();
        let reclaimer = Reclaimer::new(
            broker,
            subscription,
            routing_table,
            dispatch_tx,
            Duration::from_millis(5),
            cancellation.clone(),
        );

        let reclaimer_run = reclaimer.run();
        tokio::pin!(reclaimer_run);

        let dispatched = timeout(Duration::from_secs(1), async {
            let mut maybe_item = None;
            while maybe_item.is_none() {
                tokio::select! {
                    recv_item = dispatch_rx.recv() => {
                        maybe_item = recv_item;
                    }
                    run_result = &mut reclaimer_run => {
                        panic!("reclaimer exited unexpectedly: {run_result:?}");
                    }
                }
            }

            maybe_item.expect("dispatcher channel should receive one reclaimed item")
        })
        .await
        .expect("reclaimer should forward one reclaimed item in time");

        assert_eq!(1_u128 << 0, dispatched.route_mask);
        assert_eq!("orders", dispatched.payload.envelope.stream.as_ref());
        assert_eq!("1731016450000-3", dispatched.payload.envelope.id.as_ref());
        assert_eq!(br#"{"order_id":43}"#, dispatched.payload.envelope.payload.as_ref());
        assert_eq!(1, dispatched.payload.envelope.headers.len());
        assert_eq!((Bytes::from_static(b"tenant"), Bytes::from_static(b"t1")), dispatched.payload.envelope.headers[0]);
        assert_eq!(1_731_016_450_000, dispatched.payload.envelope.ts_unix_ms);
        assert_eq!(
            MockAckToken { stream: "orders".to_owned(), group: "g1".to_owned(), id: "1731016450000-3".to_owned() },
            dispatched.payload.ack_token
        );

        cancellation.cancel();
        drop(dispatch_rx);

        timeout(Duration::from_secs(1), &mut reclaimer_run)
            .await
            .expect("reclaimer task should stop after cancellation")
            .expect("reclaimer should exit cleanly");
    }
}
