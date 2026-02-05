use std::collections::HashMap;
use std::sync::Arc;

use crate::core::Broker;
use crate::core::Handler;

pub type RouteMask = u128;
pub const MAX_HANDLERS: usize = RouteMask::BITS as usize;
pub type RoutingTable = HashMap<Arc<str>, RouteMask>;

pub trait HandlerSet<B: Broker> {
    const LEN: usize;
    const ASSERT_WITHIN_LIMIT: ();

    fn build_routing_table() -> RoutingTable {
        let () = Self::ASSERT_WITHIN_LIMIT;
        let mut routing_table = RoutingTable::new();
        Self::append_routes(&mut routing_table);
        routing_table
    }

    fn append_routes(routes: &mut RoutingTable);
}

impl<B: Broker> HandlerSet<B> for () {
    const LEN: usize = 0;
    const ASSERT_WITHIN_LIMIT: () = ();

    fn append_routes(_routes: &mut RoutingTable) {}
}

impl<B, Tail, H> HandlerSet<B> for (Tail, H)
where
    B: Broker,
    Tail: HandlerSet<B>,
    H: Handler<B>,
{
    const LEN: usize = Tail::LEN + 1;
    const ASSERT_WITHIN_LIMIT: () = {
        let () = Tail::ASSERT_WITHIN_LIMIT;
        assert!(Self::LEN <= MAX_HANDLERS, "handler count exceeds RouteMask capacity");
    };

    fn append_routes(routes: &mut RoutingTable) {
        let () = Self::ASSERT_WITHIN_LIMIT;
        Tail::append_routes(routes);

        let handler_bit = 1_u128.checked_shl(Tail::LEN as u32).expect("handler bit index should fit RouteMask");

        for stream in H::STREAMS {
            routes.entry(Arc::<str>::from(*stream)).and_modify(|mask| *mask |= handler_bit).or_insert(handler_bit);
        }
    }
}

pub fn build_routing_table<B, HS>() -> RoutingTable
where
    B: Broker,
    HS: HandlerSet<B>,
{
    HS::build_routing_table()
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use bytes::Bytes;

    use crate::core::traits::Broker;
    use crate::core::traits::BrokerMessage;
    use crate::core::traits::Handler;
    use crate::core::types::Context;
    use crate::core::types::Envelope;
    use crate::core::types::HandlerError;
    use crate::core::types::Outcome;
    use crate::handler_set::build_routing_table;

    #[derive(Debug, Clone, Default)]
    struct NoopBroker;

    #[derive(Debug, Clone)]
    struct NoopMessage;

    impl BrokerMessage for NoopMessage {
        fn stream(&self) -> &str { "noop" }

        fn id(&self) -> &str { "0-0" }

        fn payload(&self) -> &[u8] { &[] }

        fn headers(&self) -> &[(Bytes, Bytes)] { &[] }

        fn ts_unix_ms(&self) -> u64 { 0 }
    }

    #[derive(Debug, Clone)]
    struct NoopAckToken;

    #[derive(Debug, Clone, Default)]
    struct NoopSubscription;

    impl Broker for NoopBroker {
        type Message<'a>
            = NoopMessage
        where
            Self: 'a;
        type AckToken = NoopAckToken;
        type Subscription = NoopSubscription;
        type Error = Infallible;

        async fn poll<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::empty::<(NoopMessage, NoopAckToken)>())
        }

        async fn ack(&self, _token: Self::AckToken) -> Result<(), Self::Error> { Ok(()) }

        async fn reclaim<'a>(
            &'a self, _sub: &'a Self::Subscription,
        ) -> Result<impl IntoIterator<Item = (Self::Message<'a>, Self::AckToken)> + 'a, Self::Error> {
            Ok(std::iter::empty::<(NoopMessage, NoopAckToken)>())
        }
    }

    struct OrdersHandler;
    struct SharedHandler;

    impl Handler<NoopBroker> for OrdersHandler {
        const STREAMS: &'static [&'static str] = &["orders", "shared"];

        async fn handle(&self, _ctx: &Context<NoopBroker>, _msg: &Envelope) -> Result<Outcome, HandlerError> {
            Ok(Outcome::Ack)
        }
    }

    impl Handler<NoopBroker> for SharedHandler {
        const STREAMS: &'static [&'static str] = &["shared", "payments"];

        async fn handle(&self, _ctx: &Context<NoopBroker>, _msg: &Envelope) -> Result<Outcome, HandlerError> {
            Ok(Outcome::Ack)
        }
    }

    #[test]
    fn routing_table_builds_expected_masks() {
        type Handlers = (((), OrdersHandler), SharedHandler);

        let routing = build_routing_table::<NoopBroker, Handlers>();

        assert_eq!(Some(&(1_u128 << 0)), routing.get("orders"));
        assert_eq!(Some(&((1_u128 << 0) | (1_u128 << 1))), routing.get("shared"));
        assert_eq!(Some(&(1_u128 << 1)), routing.get("payments"));
        assert_eq!(3, routing.len());
    }
}
