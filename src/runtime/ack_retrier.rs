use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::BackoffConfig;
use crate::core::Broker;

#[derive(Debug)]
pub struct AckRetrier<B>
where
    B: Broker,
    B::AckToken: Clone,
{
    broker: B,
    input_rx: Arc<Mutex<mpsc::Receiver<B::AckToken>>>,
    backoff: BackoffConfig,
    cancellation: CancellationToken,
}

impl<B> Clone for AckRetrier<B>
where
    B: Broker,
    B::AckToken: Clone,
{
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            input_rx: self.input_rx.clone(),
            backoff: self.backoff.clone(),
            cancellation: self.cancellation.clone(),
        }
    }
}

impl<B> AckRetrier<B>
where
    B: Broker,
    B::AckToken: Clone,
{
    pub fn new(
        broker: B, input_rx: mpsc::Receiver<B::AckToken>, backoff: BackoffConfig, cancellation: CancellationToken,
    ) -> Self {
        Self { broker, input_rx: Arc::new(Mutex::new(input_rx)), backoff, cancellation }
    }

    pub async fn run(&self) {
        loop {
            let token = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return;
                }
                maybe_token = async {
                    let mut input_rx = self.input_rx.lock().await;
                    input_rx.recv().await
                } => {
                    match maybe_token {
                        Some(token) => token,
                        None => return,
                    }
                }
            };

            self.retry_one(token).await;
        }
    }

    async fn retry_one(&self, mut token: B::AckToken) {
        let mut attempt = 1_u32;
        loop {
            let retry_token = token.clone();
            let ack_result = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return;
                }
                ack_result = self.broker.ack(token) => ack_result,
            };

            if ack_result.is_ok() {
                return;
            }

            let sleep_duration = backoff_with_full_jitter(&self.backoff, attempt);
            token = retry_token;
            attempt = attempt.saturating_add(1);

            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return;
                }
                _ = tokio::time::sleep(sleep_duration) => {}
            }
        }
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
    use crate::core::traits::Broker;
    use crate::core::traits::BrokerMessage;
    use crate::runtime::ack_retrier::AckRetrier;

    #[derive(Debug, Clone, Default)]
    struct MockBroker {
        ack_attempts: Arc<AtomicUsize>,
    }

    impl MockBroker {
        fn ack_attempts(&self) -> usize { self.ack_attempts.load(Ordering::SeqCst) }
    }

    #[derive(Debug, Clone)]
    struct MockMessage;

    impl BrokerMessage for MockMessage {
        fn stream(&self) -> &str { "orders" }

        fn id(&self) -> &str { "0-0" }

        fn payload(&self) -> &[u8] { &[] }

        fn headers(&self) -> &[(Bytes, Bytes)] { &[] }

        fn ts_unix_ms(&self) -> u64 { 0 }
    }

    #[derive(Debug, Clone)]
    struct MockAckToken {
        stream: String,
        group: String,
        id: String,
    }

    #[derive(Debug, Clone)]
    struct MockSubscription;

    #[derive(Debug, Clone)]
    struct MockBrokerError;

    impl std::fmt::Display for MockBrokerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "mock ack error") }
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
            Ok(std::iter::empty::<(MockMessage, MockAckToken)>())
        }

        async fn ack(&self, token: Self::AckToken) -> Result<(), Self::Error> {
            let _token_identity = (token.stream, token.group, token.id);
            let attempt = self.ack_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(MockBrokerError);
            }

            Ok(())
        }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::empty::<(MockMessage, MockAckToken)>())
        }
    }

    #[tokio::test]
    async fn ack_retrier_retries_with_backoff() {
        let broker = MockBroker::default();
        let (input_tx, input_rx) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let backoff = BackoffConfig { initial_ms: 10, max_ms: 20, factor: 2.0, jitter: 0.0 };
        let retrier = AckRetrier::new(broker.clone(), input_rx, backoff, cancellation.clone());

        let retrier_task = tokio::spawn(async move {
            retrier.run().await;
        });

        input_tx
            .send(MockAckToken {
                stream: "orders".to_owned(),
                group: "g1".to_owned(),
                id: "1731016450000-1".to_owned(),
            })
            .await
            .expect("failed ack token send should succeed");

        timeout(Duration::from_secs(1), async {
            loop {
                if broker.ack_attempts() >= 2 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("ack retrier should retry and eventually succeed");

        cancellation.cancel();
        drop(input_tx);

        timeout(Duration::from_secs(1), retrier_task)
            .await
            .expect("ack retrier task should stop after cancellation")
            .expect("ack retrier task join should succeed");

        assert_eq!(2, broker.ack_attempts());
    }
}
