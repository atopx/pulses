pub const MAX_DISPATCHER_CAPACITY: usize = 65_536;

#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub factor: f64,
    pub jitter: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self { Self { initial_ms: 50, max_ms: 5_000, factor: 2.0, jitter: 0.2 } }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub dispatcher_capacity: usize,
    pub global_max_in_flight: usize,
    pub ack_retry_queue_capacity: usize,
    pub reclaim_interval_ms: u64,
    pub backoff: BackoffConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.dispatcher_capacity > MAX_DISPATCHER_CAPACITY {
            return Err("dispatcher capacity exceeds hard max");
        }

        Ok(())
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            dispatcher_capacity: 4_096,
            global_max_in_flight: 1_024,
            ack_retry_queue_capacity: 4_096,
            reclaim_interval_ms: 2_000,
            backoff: BackoffConfig::default(),
        }
    }
}
