use std::convert::Infallible;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::core::Broker;
use crate::core::Context;
use crate::core::Handler;
use crate::dispatcher::DispatchError;
use crate::dispatcher::Dispatcher;
use crate::handler_pool::HandlerPool;
use crate::handler_pool::HandlerTask;
use crate::handler_set::HandlerSet;
use crate::handler_set::build_routing_table;
use crate::runtime::ack_retrier::AckRetrier;
use crate::runtime::broker_reader::BrokerReader;
use crate::runtime::reclaimer::Reclaimer;
use crate::runtime::supervisor::Supervisor;

#[derive(Debug)]
pub struct App<B, HS>
where
    B: Broker,
    HS: HandlerSet<B>,
{
    broker: B,
    subscription: B::Subscription,
    handlers: HS,
    config: AppConfig,
}

#[derive(Debug)]
pub enum AppError {
    InvalidConfig(&'static str),
    Dispatcher(DispatchError),
    RuntimeTaskJoin(tokio::task::JoinError),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(error) => write!(f, "invalid app config: {error}"),
            Self::Dispatcher(error) => write!(f, "dispatcher setup failed: {error}"),
            Self::RuntimeTaskJoin(error) => write!(f, "runtime task join failed: {error}"),
        }
    }
}

impl std::error::Error for AppError {}

impl<B> App<B, ()>
where
    B: Broker,
{
    pub fn new(broker: B, subscription: B::Subscription) -> Self {
        Self { broker, subscription, handlers: (), config: AppConfig::default() }
    }
}

impl<B, HS> App<B, HS>
where
    B: Broker,
    HS: HandlerSet<B>,
{
    pub fn with_config(mut self, config: AppConfig) -> Self {
        self.config = config;
        self
    }

    pub fn register<H>(self, handler: H) -> App<B, (HS, H)>
    where
        H: Handler<B>,
    {
        App {
            broker: self.broker,
            subscription: self.subscription,
            handlers: (self.handlers, handler),
            config: self.config,
        }
    }

    pub fn config(&self) -> &AppConfig { &self.config }
}

impl<B, HS> App<B, HS>
where
    B: Broker,
    B::Subscription: Clone,
    B::AckToken: Clone,
    HS: HandlerSet<B> + SpawnHandlerPools<B>,
{
    pub async fn run(self, cancellation: CancellationToken) -> Result<(), AppError> {
        self.config.validate().map_err(AppError::InvalidConfig)?;

        let routing_table = build_routing_table::<B, HS>();
        let supervisor = Supervisor::new(self.config.backoff.clone());
        let local_set = LocalSet::new();

        local_set
            .run_until(async move {
                let App { broker, subscription, handlers, config } = self;
                let mut task_handles = Vec::<JoinHandle<()>>::new();

                let (ack_retry_tx, ack_retry_rx) = mpsc::channel(config.ack_retry_queue_capacity.max(1));

                let mut handler_txs = Vec::new();
                let handler_context = Context::new(broker.clone());
                handlers.spawn_handler_pools(
                    handler_context,
                    config.dispatcher_capacity.max(1),
                    config.global_max_in_flight.max(1),
                    Some(ack_retry_tx.clone()),
                    &supervisor,
                    &cancellation,
                    &mut task_handles,
                    &mut handler_txs,
                );

                let (dispatch_tx, dispatcher) =
                    Dispatcher::new(config.dispatcher_capacity.max(1), handler_txs).map_err(AppError::Dispatcher)?;
                let dispatcher_actor = dispatcher.clone();
                task_handles.push(supervisor.spawn_supervised_local("dispatcher", cancellation.clone(), move || {
                    let dispatcher_actor = dispatcher_actor.clone();
                    async move { dispatcher_actor.run().await }
                }));

                let ack_retrier =
                    AckRetrier::new(broker.clone(), ack_retry_rx, config.backoff.clone(), cancellation.clone());
                let ack_retrier_actor = ack_retrier.clone();
                task_handles.push(supervisor.spawn_supervised_local("ack_retrier", cancellation.clone(), move || {
                    let ack_retrier_actor = ack_retrier_actor.clone();
                    async move {
                        ack_retrier_actor.run().await;
                        Ok::<(), Infallible>(())
                    }
                }));

                let reader_broker = broker.clone();
                let reader_subscription = subscription.clone();
                let reader_routing_table = routing_table.clone();
                let reader_dispatch_tx = dispatch_tx.clone();
                let reader_backoff = config.backoff.clone();
                let reader_cancellation = cancellation.clone();
                task_handles.push(supervisor.spawn_supervised_local(
                    "broker_reader",
                    cancellation.clone(),
                    move || {
                        let reader = BrokerReader::new(
                            reader_broker.clone(),
                            reader_subscription.clone(),
                            reader_routing_table.clone(),
                            reader_dispatch_tx.clone(),
                            reader_backoff.clone(),
                            reader_cancellation.clone(),
                        );
                        async move { reader.run().await }
                    },
                ));

                let reclaimer_broker = broker.clone();
                let reclaimer_subscription = subscription.clone();
                let reclaimer_routing_table = routing_table.clone();
                let reclaimer_dispatch_tx = dispatch_tx.clone();
                let reclaimer_interval = Duration::from_millis(config.reclaim_interval_ms.max(1));
                let reclaimer_cancellation = cancellation.clone();
                task_handles.push(supervisor.spawn_supervised_local("reclaimer", cancellation.clone(), move || {
                    let reclaimer = Reclaimer::new(
                        reclaimer_broker.clone(),
                        reclaimer_subscription.clone(),
                        reclaimer_routing_table.clone(),
                        reclaimer_dispatch_tx.clone(),
                        reclaimer_interval,
                        reclaimer_cancellation.clone(),
                    );
                    async move { reclaimer.run().await }
                }));

                cancellation.cancelled().await;
                drop(dispatch_tx);
                drop(ack_retry_tx);

                for handle in task_handles {
                    handle.await.map_err(AppError::RuntimeTaskJoin)?;
                }

                Ok(())
            })
            .await
    }
}

pub trait SpawnHandlerPools<B>: HandlerSet<B>
where
    B: Broker,
    B::AckToken: Clone,
{
    #[allow(clippy::too_many_arguments)]
    fn spawn_handler_pools(
        self, context: Context<B>, queue_capacity: usize, global_max_in_flight: usize,
        ack_retry_tx: Option<mpsc::Sender<B::AckToken>>, supervisor: &Supervisor, cancellation: &CancellationToken,
        task_handles: &mut Vec<JoinHandle<()>>, handler_txs: &mut Vec<mpsc::Sender<HandlerTask<B::AckToken>>>,
    );
}

impl<B> SpawnHandlerPools<B> for ()
where
    B: Broker,
    B::AckToken: Clone,
{
    fn spawn_handler_pools(
        self, _context: Context<B>, _queue_capacity: usize, _global_max_in_flight: usize,
        _ack_retry_tx: Option<mpsc::Sender<B::AckToken>>, _supervisor: &Supervisor, _cancellation: &CancellationToken,
        _task_handles: &mut Vec<JoinHandle<()>>, _handler_txs: &mut Vec<mpsc::Sender<HandlerTask<B::AckToken>>>,
    ) {
    }
}

impl<B, Tail, H> SpawnHandlerPools<B> for (Tail, H)
where
    B: Broker,
    B::AckToken: Clone,
    Tail: SpawnHandlerPools<B>,
    H: Handler<B>,
{
    fn spawn_handler_pools(
        self, context: Context<B>, queue_capacity: usize, global_max_in_flight: usize,
        ack_retry_tx: Option<mpsc::Sender<B::AckToken>>, supervisor: &Supervisor, cancellation: &CancellationToken,
        task_handles: &mut Vec<JoinHandle<()>>, handler_txs: &mut Vec<mpsc::Sender<HandlerTask<B::AckToken>>>,
    ) {
        let (tail, handler) = self;
        tail.spawn_handler_pools(
            context.clone(),
            queue_capacity,
            global_max_in_flight,
            ack_retry_tx.clone(),
            supervisor,
            cancellation,
            task_handles,
            handler_txs,
        );

        let (handler_tx, handler_pool) =
            HandlerPool::new(handler, context, global_max_in_flight, queue_capacity, ack_retry_tx);
        let handler_pool_actor = handler_pool.clone();
        task_handles.push(supervisor.spawn_supervised_local("handler_pool", cancellation.clone(), move || {
            let handler_pool_actor = handler_pool_actor.clone();
            async move { handler_pool_actor.run().await }
        }));
        handler_txs.push(handler_tx);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::sync::Mutex;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::app::App;
    use crate::config::AppConfig;
    use crate::config::BackoffConfig;
    use crate::core::Broker;
    use crate::core::BrokerMessage;
    use crate::core::Context;
    use crate::core::Envelope;
    use crate::core::Handler;
    use crate::core::HandlerError;
    use crate::core::Outcome;

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
        ack_count: Arc<AtomicUsize>,
    }

    impl MockBroker {
        fn ack_count(&self) -> usize { self.ack_count.load(Ordering::SeqCst) }
    }

    #[derive(Debug, Clone)]
    struct MockBrokerError;

    impl std::fmt::Display for MockBrokerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "mock broker error") }
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

        async fn ack(&self, token: Self::AckToken) -> Result<(), Self::Error> {
            let _token_identity = (token.stream, token.group, token.id);
            self.ack_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(Vec::<(MockMessage, MockAckToken)>::new())
        }
    }

    #[derive(Debug, Clone)]
    struct RecordingHandler {
        processed_ids: Arc<Mutex<Vec<String>>>,
    }

    impl Handler<MockBroker> for RecordingHandler {
        const STREAMS: &'static [&'static str] = &["orders"];

        async fn handle(&self, _ctx: &Context<MockBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
            self.processed_ids.lock().await.push(msg.id.to_string());
            Ok(Outcome::Ack)
        }
    }

    #[tokio::test]
    async fn app_end_to_end_with_mock_broker() {
        let broker = MockBroker::default();
        let processed_ids = Arc::new(Mutex::new(Vec::new()));
        let app = App::new(broker.clone(), MockSubscription)
            .with_config(AppConfig {
                dispatcher_capacity: 16,
                global_max_in_flight: 16,
                ack_retry_queue_capacity: 16,
                reclaim_interval_ms: 20,
                backoff: BackoffConfig { initial_ms: 5, max_ms: 20, factor: 2.0, jitter: 0.0 },
            })
            .register(RecordingHandler { processed_ids: processed_ids.clone() });
        let cancellation = CancellationToken::new();

        let app_run = app.run(cancellation.clone());
        tokio::pin!(app_run);

        timeout(Duration::from_secs(2), async {
            loop {
                if broker.ack_count() >= 1 {
                    return;
                }

                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                    run_result = &mut app_run => {
                        panic!("app runtime exited unexpectedly: {run_result:?}");
                    }
                }
            }
        })
        .await
        .expect("app should process and ack at least one message");

        let processed = processed_ids.lock().await.clone();
        assert_eq!(vec!["1731016450000-1".to_string()], processed);
        assert_eq!(1, broker.ack_count());

        cancellation.cancel();
        timeout(Duration::from_secs(1), &mut app_run)
            .await
            .expect("app should stop after cancellation")
            .expect("app should exit cleanly");
    }
}
