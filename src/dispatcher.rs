use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::handler_set::MAX_HANDLERS;
use crate::handler_set::RouteMask;

#[derive(Debug, Clone)]
pub struct Dispatcher<T> {
    input_rx: Arc<Mutex<mpsc::Receiver<DispatchItem<T>>>>,
    handler_txs: Vec<mpsc::Sender<T>>,
}

#[derive(Debug, Clone)]
pub struct DispatchItem<T> {
    pub route_mask: RouteMask,
    pub payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchError {
    TooManyHandlers { count: usize },
    MissingHandlerChannel { index: usize },
    HandlerChannelClosed { index: usize },
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyHandlers { count } => {
                write!(f, "handler channel count {count} exceeds max {MAX_HANDLERS}")
            }
            Self::MissingHandlerChannel { index } => {
                write!(f, "route mask points to missing handler channel index {index}")
            }
            Self::HandlerChannelClosed { index } => {
                write!(f, "handler channel {index} is closed")
            }
        }
    }
}

impl std::error::Error for DispatchError {}

impl<T> Dispatcher<T>
where
    T: Clone + Send + 'static,
{
    pub fn new(
        capacity: usize, handler_txs: Vec<mpsc::Sender<T>>,
    ) -> Result<(mpsc::Sender<DispatchItem<T>>, Self), DispatchError> {
        if handler_txs.len() > MAX_HANDLERS {
            return Err(DispatchError::TooManyHandlers { count: handler_txs.len() });
        }

        let (input_tx, input_rx) = mpsc::channel(capacity);
        Ok((input_tx, Self { input_rx: Arc::new(Mutex::new(input_rx)), handler_txs }))
    }

    pub async fn run(&self) -> Result<(), DispatchError> {
        loop {
            let maybe_item = {
                let mut input_rx = self.input_rx.lock().await;
                input_rx.recv().await
            };

            let Some(item) = maybe_item else {
                return Ok(());
            };

            self.dispatch_one(item).await?;
        }
    }

    async fn dispatch_one(&self, item: DispatchItem<T>) -> Result<(), DispatchError> {
        let mut targets = route_indexes(item.route_mask);
        if targets.is_empty() {
            return Ok(());
        }

        let mut original_payload = Some(item.payload);
        let target_count = targets.len();
        for (position, handler_index) in targets.drain(..).enumerate() {
            let payload = if position + 1 == target_count {
                original_payload.take().expect("last dispatch should take original payload")
            } else {
                original_payload.as_ref().expect("original payload should exist").clone()
            };

            let handler_tx = self
                .handler_txs
                .get(handler_index)
                .ok_or(DispatchError::MissingHandlerChannel { index: handler_index })?;

            handler_tx.send(payload).await.map_err(|_| DispatchError::HandlerChannelClosed { index: handler_index })?;
        }

        Ok(())
    }
}

fn route_indexes(route_mask: RouteMask) -> Vec<usize> {
    let mut indexes = Vec::new();
    let mut remaining = route_mask;
    let mut index = 0usize;

    while remaining != 0 {
        if (remaining & 1) == 1 {
            indexes.push(index);
        }

        remaining >>= 1;
        index += 1;
    }

    indexes
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::dispatcher::DispatchItem;
    use crate::dispatcher::Dispatcher;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestPayload(&'static str);

    #[tokio::test]
    async fn dispatcher_routes_to_correct_pools() {
        let (handler0_tx, mut handler0_rx) = mpsc::channel::<TestPayload>(4);
        let (handler1_tx, mut handler1_rx) = mpsc::channel::<TestPayload>(4);
        let (input_tx, dispatcher) =
            Dispatcher::new(8, vec![handler0_tx, handler1_tx]).expect("dispatcher creation should succeed");

        let join_handle = tokio::spawn(async move { dispatcher.run().await });

        input_tx
            .send(DispatchItem { route_mask: 1 << 0, payload: TestPayload("orders-1") })
            .await
            .expect("dispatch send should succeed");
        input_tx
            .send(DispatchItem { route_mask: 1 << 1, payload: TestPayload("payments-1") })
            .await
            .expect("dispatch send should succeed");
        input_tx
            .send(DispatchItem { route_mask: (1 << 0) | (1 << 1), payload: TestPayload("shared-1") })
            .await
            .expect("dispatch send should succeed");

        drop(input_tx);

        let first_h0 = handler0_rx.recv().await.expect("handler-0 should receive first item");
        let second_h0 = handler0_rx.recv().await.expect("handler-0 should receive shared item");
        let first_h1 = handler1_rx.recv().await.expect("handler-1 should receive first item");
        let second_h1 = handler1_rx.recv().await.expect("handler-1 should receive shared item");

        assert_eq!(TestPayload("orders-1"), first_h0);
        assert_eq!(TestPayload("shared-1"), second_h0);
        assert_eq!(TestPayload("payments-1"), first_h1);
        assert_eq!(TestPayload("shared-1"), second_h1);

        tokio::time::timeout(Duration::from_secs(1), async {
            join_handle.await.expect("dispatcher task join should succeed")
        })
        .await
        .expect("dispatcher task should exit when input channel is closed")
        .expect("dispatcher run should succeed");
    }
}
