# Pulses

Pulses is a robust and flexible background job processing library for Rust. It is designed to be modular, allowing for pluggable brokers (currently supporting Redis Streams) and providing a type-safe way to define message handlers.

## Features

- **Pluggable Architecture**: Support for different message brokers. Currently ships with a Redis Streams implementation.
- **Type-Safe Handlers**: Define handlers using Rust traits with compile-time checks.
- **Concurrency Control**: Fine-grained control over worker counts and max-in-flight messages per handler.
- **Reliability**: Built-in mechanisms for message acknowledgement, automatic retries, and reclaiming pending messages from crashed consumers.
- **Graceful Shutdown**: Integrated with `tokio`'s cancellation tokens for clean shutdown sequences.

## Installation

Add `pulses` to your `Cargo.toml`. Since this is currently a library within your workspace or a local path, you might reference it like this:

```toml
[dependencies]
pulses = { version = "0.1.1" } # Or git repository
tokio = { version = "1", features = ["full"] }
```

## Usage

Here is a simple example of how to create a handler and run the application using the Redis broker.

### 1. Define a Handler

Implement the `Handler` trait for your logic. You can specify which streams to listen to and how many concurrent workers to run.

```rust
use std::sync::Arc;
use pulses::core::{Context, Envelope, Handler, Outcome, HandlerError};
use pulses::broker::redis::RedisBroker;

struct MyHandler;

impl Handler<RedisBroker> for MyHandler {
    // The Redis streams this handler will consume from
    const STREAMS: &'static [&'static str] = &["my-stream"];
    
    // Number of concurrent tasks processing messages for this handler
    const WORKERS: usize = 4;

    async fn handle(&self, _ctx: &Context<RedisBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
        println!("Received message ID: {}, payload: {:?}", msg.id, msg.payload);
        
        // Return Outcome::Ack to confirm successful processing
        // Other options: Outcome::Retry { after_ms }, Outcome::Drop, Outcome::DeadLetter
        Ok(Outcome::Ack)
    }
}
```

### 2. Run the Application

Initialize the broker, configure the subscription (consumer group settings), register your handlers, and run the app.

```rust
use pulses::App;
use pulses::broker::redis::{RedisBroker, RedisSubscription};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connect to Redis
    let broker = RedisBroker::connect("redis://127.0.0.1:6379").await?;

    // 2. Configure Subscription (Consumer Group)
    let subscription = RedisSubscription {
        group: "my-group".into(),
        consumer: "consumer-1".into(),
        ..Default::default()
    };

    // 3. Build and Run the App
    let app = App::new(broker, subscription)
        .register(MyHandler);

    println!("Starting Pulses worker...");
    
    // Use a CancellationToken for graceful shutdown
    let token = CancellationToken::new();
    
    // In a real application, you would trigger the token on SIGINT/SIGTERM
    // e.g., tokio::signal::ctrl_c().await.unwrap(); token.cancel();

    app.run(token).await?;

    Ok(())
}
```

## Architecture

- **App**: The main entry point that ties the broker, subscription, and handlers together.
- **Broker**: A trait abstracting the underlying message queue (e.g., Redis, Kafka).
- **Handler**: User-defined logic for processing messages.
- **Runtime**: Handles the loop of polling, dispatching to handlers, managing retries, and reclaiming stuck messages.

## License

[MIT](LICENSE)
