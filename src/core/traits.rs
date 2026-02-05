#![allow(async_fn_in_trait)]

use bytes::Bytes;

use crate::core::types::Context;
use crate::core::types::Envelope;
use crate::core::types::HandlerError;
use crate::core::types::Outcome;

pub trait Broker: Clone + Send + Sync + 'static {
    type Message<'a>: BrokerMessage + Send + 'a
    where
        Self: 'a;
    type AckToken: Send + Sync + 'static;
    type Subscription: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn poll<'a>(
        &'a self, sub: &'a Self::Subscription,
    ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error>;

    async fn ack(&self, token: Self::AckToken) -> Result<(), Self::Error>;

    async fn reclaim<'a>(
        &'a self, sub: &'a Self::Subscription,
    ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error>;
}

pub trait BrokerMessage: Send {
    fn stream(&self) -> &str;
    fn id(&self) -> &str;
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &[(Bytes, Bytes)];
    fn ts_unix_ms(&self) -> u64;
}

pub trait Handler<B: Broker>: Send + Sync + 'static {
    const STREAMS: &'static [&'static str];
    const WORKERS: usize = 1;
    const MAX_IN_FLIGHT: usize = 1_024;

    async fn handle(&self, ctx: &Context<B>, msg: &Envelope) -> Result<Outcome, HandlerError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use smallvec::SmallVec;

    use crate::core::traits::Broker;
    use crate::core::traits::BrokerMessage;
    use crate::core::traits::Handler;
    use crate::core::types::Context;
    use crate::core::types::Envelope;
    use crate::core::types::HandlerError;
    use crate::core::types::Outcome;

    #[derive(Debug, Clone)]
    struct DummyMessage {
        stream: String,
        id: String,
        payload: Bytes,
        headers: Vec<(Bytes, Bytes)>,
        ts_unix_ms: u64,
    }

    impl BrokerMessage for DummyMessage {
        fn stream(&self) -> &str { &self.stream }

        fn id(&self) -> &str { &self.id }

        fn payload(&self) -> &[u8] { self.payload.as_ref() }

        fn headers(&self) -> &[(Bytes, Bytes)] { &self.headers }

        fn ts_unix_ms(&self) -> u64 { self.ts_unix_ms }
    }

    impl BrokerMessage for &DummyMessage {
        fn stream(&self) -> &str { (*self).stream() }

        fn id(&self) -> &str { (*self).id() }

        fn payload(&self) -> &[u8] { (*self).payload() }

        fn headers(&self) -> &[(Bytes, Bytes)] { (*self).headers() }

        fn ts_unix_ms(&self) -> u64 { (*self).ts_unix_ms() }
    }

    #[derive(Debug, Clone)]
    struct DummyAckToken {
        stream: String,
        group: String,
        id: String,
    }

    #[derive(Debug, Clone)]
    struct DummySubscription {
        message: DummyMessage,
        ack_token: DummyAckToken,
    }

    impl DummySubscription {
        fn sample() -> Self {
            Self {
                message: DummyMessage {
                    stream: "orders".to_owned(),
                    id: "1731016450000-1".to_owned(),
                    payload: Bytes::from_static(br#"{"order_id":42}"#),
                    headers: vec![(Bytes::from_static(b"tenant"), Bytes::from_static(b"t1"))],
                    ts_unix_ms: 1_731_016_450_000,
                },
                ack_token: DummyAckToken {
                    stream: "orders".to_owned(),
                    group: "g1".to_owned(),
                    id: "1731016450000-1".to_owned(),
                },
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct DummyBroker {
        ack_count: Arc<AtomicUsize>,
    }

    impl DummyBroker {
        fn ack_count(&self) -> usize { self.ack_count.load(Ordering::SeqCst) }
    }

    impl Broker for DummyBroker {
        type Message<'a>
            = &'a DummyMessage
        where
            Self: 'a;
        type AckToken = DummyAckToken;
        type Subscription = DummySubscription;
        type Error = std::io::Error;

        async fn poll<'a>(
            &'a self, sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::once((&sub.message, sub.ack_token.clone())))
        }

        async fn ack(&self, token: Self::AckToken) -> Result<(), Self::Error> {
            let _token_identity = (token.stream, token.group, token.id);
            self.ack_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::empty::<(&'a DummyMessage, DummyAckToken)>())
        }
    }

    struct DummyHandler;

    impl Handler<DummyBroker> for DummyHandler {
        const STREAMS: &'static [&'static str] = &["orders"];

        async fn handle(&self, _ctx: &Context<DummyBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
            if msg.stream.as_ref() == "orders" {
                return Ok(Outcome::Ack);
            }

            Ok(Outcome::Drop)
        }
    }

    #[tokio::test]
    async fn core_trait_smoke() {
        let broker = DummyBroker::default();
        let subscription = DummySubscription::sample();
        let mut batch = broker.poll(&subscription).await.expect("dummy poll should succeed").into_iter();

        let (message, token) = batch.next().expect("dummy poll should return one message");

        let mut headers = SmallVec::<[(Bytes, Bytes); 8]>::new();
        headers.extend(message.headers().iter().cloned());
        let envelope = Envelope {
            stream: Arc::<str>::from(message.stream()),
            id: Arc::<str>::from(message.id()),
            payload: Bytes::copy_from_slice(message.payload()),
            headers,
            ts_unix_ms: message.ts_unix_ms(),
        };

        let handler = DummyHandler;
        let context = Context::new(broker.clone());
        let outcome = handler.handle(&context, &envelope).await.expect("dummy handler should succeed");

        if outcome.should_ack() {
            broker.ack(token).await.expect("dummy ack should succeed");
        }

        assert_eq!(1, broker.ack_count());
    }
}
