use std::sync::Arc;
use pulses::core::{Context, Envelope, Handler, Outcome, HandlerError};
use pulses::broker::redis::{RedisBroker, RedisSubscription};
use pulses::App;
use tokio_util::sync::CancellationToken;

struct MyHandler;

impl Handler<RedisBroker> for MyHandler {
    const STREAMS: &'static [&'static str] = &["my-stream"];
    const WORKERS: usize = 4;

    async fn handle(&self, _ctx: &Context<RedisBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
        println!("Received message ID: {}, payload: {:?}", msg.id, msg.payload);
        Ok(Outcome::Ack)
    }
}

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
    
    let token = CancellationToken::new();
    
    // Allow easy cancellation with Ctrl+C
    let cancel_token = token.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            println!("Shutting down...");
            cancel_token.cancel();
        }
    });

    app.run(token).await?;

    Ok(())
}
