use pipeline::broker::redis::RedisBroker;

#[tokio::test]
async fn redis_connects_and_pings() {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set (example: redis://:password@host:6379)");

    let broker = RedisBroker::connect(redis_url).await.expect("connect should succeed");
    let pong = broker.ping().await.expect("ping should succeed");

    assert_eq!(pong.as_str(), "PONG");
}
