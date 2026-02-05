use std::sync::Arc;

use bytes::Bytes;
use smallvec::SmallVec;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Envelope {
    pub stream: Arc<str>,
    pub id: Arc<str>,
    pub payload: Bytes,
    pub headers: SmallVec<[(Bytes, Bytes); 8]>,
    pub ts_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ack,
    Retry { after_ms: u64 },
    Drop,
    DeadLetter { reason: &'static str },
}

impl Outcome {
    pub fn should_ack(&self) -> bool { matches!(self, Self::Ack | Self::Drop | Self::DeadLetter { .. }) }
}

#[derive(Clone, PartialEq, Eq)]
pub enum HandlerError {
    Message(String),
}

impl std::fmt::Debug for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) => f.debug_tuple("Message").field(message).finish(),
        }
    }
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) => write!(f, "handler failed: {message}"),
        }
    }
}

impl std::error::Error for HandlerError {}

#[derive(Debug, Clone)]
pub struct Context<B> {
    pub broker: B,
}

impl<B> Context<B> {
    pub fn new(broker: B) -> Self { Self { broker } }
}

#[cfg(test)]
mod tests {
    use super::Outcome;

    #[test]
    fn core_outcome_semantics() {
        assert!(Outcome::Ack.should_ack());
        assert!(Outcome::Drop.should_ack());
        assert!(Outcome::DeadLetter { reason: "invalid_payload" }.should_ack());
        assert!(!Outcome::Retry { after_ms: 250 }.should_ack());
    }
}
