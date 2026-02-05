use std::sync::Arc;
use std::time::Duration;

use pulses::App;
use pulses::broker::redis::RedisBroker;
use pulses::broker::redis::RedisSubscription;
use pulses::core::Context;
use pulses::core::Envelope;
use pulses::core::Handler;
use pulses::core::HandlerError;
use pulses::core::Outcome;
use tokio_util::sync::CancellationToken;

// Task 1: Email Handler
// Best Practice: Define separate structs for distinct tasks/queues
struct EmailHandler;

impl Handler<RedisBroker> for EmailHandler {
    // Define which stream this handler consumes from using the constant
    const STREAMS: &'static [&'static str] = &["email-tasks"];
    // Configure concurrency for this specific task
    const WORKERS: usize = 2;

    async fn handle(&self, _ctx: &Context<RedisBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
        // Parse payload (assuming simple string for demo)
        // In real apps, you might use serde_json to deserialize a struct
        let payload = String::from_utf8_lossy(&msg.payload);
        println!("[EmailHandler] Sending email: {}", payload);

        // Simulate work
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("[EmailHandler] Email sent: {}", payload);
        Ok(Outcome::Ack)
    }
}

// Task 2: Report Handler
struct ReportHandler;

impl Handler<RedisBroker> for ReportHandler {
    const STREAMS: &'static [&'static str] = &["report-tasks"];
    // Heavier tasks might need fewer workers
    const WORKERS: usize = 1;

    async fn handle(&self, _ctx: &Context<RedisBroker>, msg: &Envelope) -> Result<Outcome, HandlerError> {
        let payload = String::from_utf8_lossy(&msg.payload);
        println!("[ReportHandler] Generating report: {}", payload);

        // Simulate heavier work
        tokio::time::sleep(Duration::from_millis(500)).await;

        println!("[ReportHandler] Report generated: {}", payload);
        Ok(Outcome::Ack)
    }
}

// Helper: Produce messages to test the multi-task worker
async fn produce_test_messages() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut con = client.get_multiplexed_async_connection().await?;

    println!("-> Producing test messages...");

    // Produce Email tasks
    for i in 1..=5 {
        let payload = format!("User_{}_Welcome", i);
        // Best Practice: Use MAXLEN to cap the stream size and prevent infinite growth.
        // XADD stream MAXLEN ~ 1000 * payload "..."
        let _: String = redis::cmd("XADD")
            .arg(EmailHandler::STREAMS[0])
            .arg("MAXLEN")
            .arg("~")
            .arg("1000")
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async(&mut con)
            .await?;
    }

    // Produce Report tasks
    for i in 1..=3 {
        let payload = format!("Monthly_Report_{}", i);
        let _: String = redis::cmd("XADD")
            .arg(ReportHandler::STREAMS[0])
            .arg("MAXLEN")
            .arg("~")
            .arg("1000")
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async(&mut con)
            .await?;
    }

    println!("-> Test messages produced.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 0. Produce data for demonstration purposes
    if let Err(e) = produce_test_messages().await {
        eprintln!("Warning: Failed to produce test messages (is Redis running?): {}", e);
        eprintln!("Continuing anyway...");
    }

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    // 1. Connect to Broker
    let broker = RedisBroker::connect(redis_url).await?;

    // 2. Configure Subscription
    // Best Practice: Use a descriptive Consumer Group name
    let mut subscription =
        RedisSubscription { group: "multi-task-group".into(), consumer: "worker-node-1".into(), ..Default::default() };

    subscription.streams.extend(EmailHandler::STREAMS.iter().map(|&s| Arc::from(s)));
    subscription.streams.extend(ReportHandler::STREAMS.iter().map(|&s| Arc::from(s)));

    // 3. Register Multiple Handlers
    // Best Practice: Chain register calls to add multiple task handlers
    // The framework will automatically route messages from different streams
    // to the correct handler based on the `STREAMS` constant.
    let app = App::new(broker, subscription).register(EmailHandler).register(ReportHandler);

    println!("Starting Multi-Task Worker...");
    println!("EmailHandler listening on: {:?}", EmailHandler::STREAMS);
    println!("ReportHandler listening on: {:?}", ReportHandler::STREAMS);
    println!("Press Ctrl+C to stop.");

    // 4. Run with Graceful Shutdown
    let token = CancellationToken::new();
    let cancel_token = token.clone();

    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            println!("\nShutting down...");
            cancel_token.cancel();
        }
    });

    app.run(token).await?;

    Ok(())
}
