use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinSet;
use tokio::task::LocalSet;

use crate::core::Broker;
use crate::core::Context;
use crate::core::Envelope;
use crate::core::Handler;
use crate::core::Outcome;

#[derive(Debug, Clone)]
pub struct HandlerTask<AckToken> {
    pub envelope: Envelope,
    pub ack_token: AckToken,
}

#[derive(Debug)]
pub struct HandlerPool<B, H>
where
    B: Broker,
    H: Handler<B>,
    B::AckToken: Clone,
{
    context: Context<B>,
    handler: Arc<H>,
    worker_count: usize,
    in_flight_limit: usize,
    input_rx: Arc<Mutex<mpsc::Receiver<HandlerTask<B::AckToken>>>>,
    ack_retry_tx: Option<mpsc::Sender<B::AckToken>>,
}

impl<B, H> Clone for HandlerPool<B, H>
where
    B: Broker,
    H: Handler<B>,
    B::AckToken: Clone,
{
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            handler: self.handler.clone(),
            worker_count: self.worker_count,
            in_flight_limit: self.in_flight_limit,
            input_rx: self.input_rx.clone(),
            ack_retry_tx: self.ack_retry_tx.clone(),
        }
    }
}

#[derive(Debug)]
pub enum HandlerPoolError {
    WorkerJoin(tokio::task::JoinError),
}

impl std::fmt::Display for HandlerPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkerJoin(error) => write!(f, "handler worker task join failed: {error}"),
        }
    }
}

impl std::error::Error for HandlerPoolError {}

impl<B, H> HandlerPool<B, H>
where
    B: Broker,
    H: Handler<B>,
    B::AckToken: Clone,
{
    pub fn new(
        handler: H, context: Context<B>, global_max_in_flight: usize, queue_capacity: usize,
        ack_retry_tx: Option<mpsc::Sender<B::AckToken>>,
    ) -> (mpsc::Sender<HandlerTask<B::AckToken>>, Self) {
        let worker_count = H::WORKERS.max(1);
        let in_flight_limit = global_max_in_flight.min(H::MAX_IN_FLIGHT).max(1);
        let (input_tx, input_rx) = mpsc::channel(queue_capacity);
        let pool = Self {
            context,
            handler: Arc::new(handler),
            worker_count,
            in_flight_limit,
            input_rx: Arc::new(Mutex::new(input_rx)),
            ack_retry_tx,
        };
        (input_tx, pool)
    }

    pub async fn run(&self) -> Result<(), HandlerPoolError> {
        let shared_rx = self.input_rx.clone();
        let semaphore = Arc::new(Semaphore::new(self.in_flight_limit));
        let local_set = LocalSet::new();

        local_set
            .run_until(async move {
                let mut workers = JoinSet::new();

                for _ in 0..self.worker_count {
                    workers.spawn_local(worker_loop::<B, H>(
                        self.handler.clone(),
                        self.context.clone(),
                        shared_rx.clone(),
                        semaphore.clone(),
                        self.ack_retry_tx.clone(),
                    ));
                }

                while let Some(join_result) = workers.join_next().await {
                    join_result.map_err(HandlerPoolError::WorkerJoin)?;
                }

                Ok(())
            })
            .await
    }
}

async fn worker_loop<B, H>(
    handler: Arc<H>, context: Context<B>, shared_rx: Arc<Mutex<mpsc::Receiver<HandlerTask<B::AckToken>>>>,
    semaphore: Arc<Semaphore>, ack_retry_tx: Option<mpsc::Sender<B::AckToken>>,
) where
    B: Broker,
    H: Handler<B>,
    B::AckToken: Clone,
{
    loop {
        let next_item = {
            let mut input_rx = shared_rx.lock().await;
            input_rx.recv().await
        };

        let Some(item) = next_item else {
            return;
        };

        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            return;
        };

        process_item(handler.as_ref(), &context, item, ack_retry_tx.as_ref()).await;

        drop(permit);
    }
}

async fn process_item<B, H>(
    handler: &H, context: &Context<B>, item: HandlerTask<B::AckToken>, ack_retry_tx: Option<&mpsc::Sender<B::AckToken>>,
) where
    B: Broker,
    H: Handler<B>,
    B::AckToken: Clone,
{
    let outcome = match handler.handle(context, &item.envelope).await {
        Ok(outcome) => outcome,
        Err(error) => {
            eprintln!("handler execution failed for message {}: {error}", item.envelope.id);
            Outcome::Retry { after_ms: 0 }
        }
    };

    if let Outcome::Retry { after_ms } = outcome {
        eprintln!(
            "handler requested retry for message {} after {}ms; rely on reclaim in MVP",
            item.envelope.id, after_ms
        );
        return;
    }

    if let Outcome::DeadLetter { reason } = &outcome {
        eprintln!("dead-letter in MVP is ack+log for message {}: {reason}", item.envelope.id);
    }

    let retry_token = item.ack_token.clone();
    if let Err(error) = context.broker.ack(item.ack_token).await {
        eprintln!("ack failed for message {}: {error}", item.envelope.id);
        enqueue_failed_ack(ack_retry_tx, retry_token);
    }
}

fn enqueue_failed_ack<AckToken>(ack_retry_tx: Option<&mpsc::Sender<AckToken>>, token: AckToken) {
    let Some(ack_retry_tx) = ack_retry_tx else {
        eprintln!("ack retry queue is not configured; dropped failed ack token");
        return;
    };

    match ack_retry_tx.try_send(token) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            eprintln!("ack retry queue is full; dropped failed ack token");
        }
        Err(TrySendError::Closed(_)) => {
            eprintln!("ack retry queue is closed; dropped failed ack token");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use smallvec::SmallVec;
    use tokio::time::timeout;

    use crate::core::traits::Broker;
    use crate::core::traits::BrokerMessage;
    use crate::core::traits::Handler;
    use crate::core::types::Context;
    use crate::core::types::Envelope;
    use crate::core::types::HandlerError;
    use crate::core::types::Outcome;
    use crate::handler_pool::HandlerPool;
    use crate::handler_pool::HandlerTask;

    #[derive(Debug, Clone, Default)]
    struct MockBroker {
        ack_count: Arc<AtomicUsize>,
    }

    impl MockBroker {
        fn ack_count(&self) -> usize { self.ack_count.load(Ordering::SeqCst) }
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

    impl MockAckToken {
        fn new(stream: &str, group: &str, id: &str) -> Self {
            Self { stream: stream.to_owned(), group: group.to_owned(), id: id.to_owned() }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockSubscription;

    impl Broker for MockBroker {
        type Message<'a>
            = MockMessage
        where
            Self: 'a;
        type AckToken = MockAckToken;
        type Subscription = MockSubscription;
        type Error = Infallible;

        async fn poll<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::empty::<(MockMessage, MockAckToken)>())
        }

        async fn ack(&self, token: Self::AckToken) -> Result<(), Self::Error> {
            let _token_identity = (token.stream, token.group, token.id);
            self.ack_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::empty::<(MockMessage, MockAckToken)>())
        }
    }

    #[derive(Debug, Default)]
    struct SequencedOutcomeHandler {
        calls: AtomicUsize,
    }

    impl Handler<MockBroker> for SequencedOutcomeHandler {
        const STREAMS: &'static [&'static str] = &["orders"];

        async fn handle(&self, _ctx: &Context<MockBroker>, _msg: &Envelope) -> Result<Outcome, HandlerError> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                return Ok(Outcome::Ack);
            }

            Ok(Outcome::Retry { after_ms: 500 })
        }
    }

    #[tokio::test]
    async fn handler_pool_ack_and_retry() {
        let broker = MockBroker::default();
        let context = Context::new(broker.clone());
        let handler = SequencedOutcomeHandler::default();
        let (input_tx, pool) = HandlerPool::new(handler, context, 16, 8, None);

        input_tx
            .send(HandlerTask {
                envelope: make_envelope("1731016450000-1"),
                ack_token: MockAckToken::new("orders", "g1", "1731016450000-1"),
            })
            .await
            .expect("first handler task send should succeed");
        input_tx
            .send(HandlerTask {
                envelope: make_envelope("1731016450000-2"),
                ack_token: MockAckToken::new("orders", "g1", "1731016450000-2"),
            })
            .await
            .expect("second handler task send should succeed");

        drop(input_tx);

        timeout(Duration::from_secs(1), pool.run())
            .await
            .expect("handler pool should exit when input channel is closed")
            .expect("handler pool run should succeed");

        assert_eq!(1, broker.ack_count());
    }

    fn make_envelope(id: &'static str) -> Envelope {
        Envelope {
            stream: Arc::<str>::from("orders"),
            id: Arc::<str>::from(id),
            payload: Bytes::new(),
            headers: SmallVec::new(),
            ts_unix_ms: 1_731_016_450_000,
        }
    }
}
