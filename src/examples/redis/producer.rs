use redis::AsyncCommands;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connect to Redis
    let client = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut con = client.get_multiplexed_async_connection().await?;

    let stream = "my-stream";
    
    // 2. Produce some messages
    // By default, Pulses expects a field named "payload" in the stream entry.
    // You can customize this in RedisSubscription::payload_key.
    
    for i in 0..10 {
        let payload = format!("Message #{}", i);
        
        // XADD my-stream * payload "Message #N"
        let id: String = con.xadd(
            stream,
            "*", // Auto-generate ID
            &[("payload", payload)]
        ).await?;
        
        println!("Produced message: {} to stream: {}", id, stream);
    }
    
    println!("Done producing messages.");
    Ok(())
}
