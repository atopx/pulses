use std::future::Future;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::BackoffConfig;

#[derive(Debug, Clone)]
pub struct Supervisor {
    backoff: BackoffConfig,
}

impl Supervisor {
    pub fn new(backoff: BackoffConfig) -> Self { Self { backoff } }

    pub fn spawn_supervised_local<F, Fut, E>(
        &self, name: &'static str, cancellation: CancellationToken, make_future: F,
    ) -> JoinHandle<()>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let backoff = self.backoff.clone();
        tokio::task::spawn_local(async move {
            let mut attempt = 1_u32;

            loop {
                if cancellation.is_cancelled() {
                    return;
                }

                let mut child = tokio::task::spawn_local(make_future());
                let child_result = tokio::select! {
                    _ = cancellation.cancelled() => {
                        child.abort();
                        let _ = child.await;
                        return;
                    }
                    child_result = &mut child => child_result,
                };

                match child_result {
                    Ok(Ok(())) => {
                        return;
                    }
                    Ok(Err(error)) => {
                        eprintln!("{name} exited with error: {error}");
                    }
                    Err(join_error) if join_error.is_panic() => {
                        eprintln!("{name} panicked, restarting");
                    }
                    Err(join_error) => {
                        eprintln!("{name} task join error, restarting: {join_error}");
                    }
                }

                let sleep_duration = backoff_with_full_jitter(&backoff, attempt);
                attempt = attempt.saturating_add(1);
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return;
                    }
                    _ = tokio::time::sleep(sleep_duration) => {}
                }
            }
        })
    }
}

fn backoff_with_full_jitter(config: &BackoffConfig, attempt: u32) -> Duration {
    let initial_ms = config.initial_ms.max(1);
    let max_ms = config.max_ms.max(initial_ms);
    let factor = config.factor.max(1.0);

    let growth = factor.powi((attempt.saturating_sub(1)) as i32);
    let base_ms = (initial_ms as f64 * growth).min(max_ms as f64);

    let jitter = config.jitter.clamp(0.0, 1.0);
    let lower_ms = base_ms * (1.0 - jitter);
    let upper_ms = base_ms * (1.0 + jitter);

    let jittered_ms =
        if upper_ms <= lower_ms { lower_ms } else { pseudo_random_unit() * (upper_ms - lower_ms) + lower_ms };

    Duration::from_millis(jittered_ms.max(0.0).round() as u64)
}

fn pseudo_random_unit() -> f64 {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0_u128, |duration| duration.as_nanos());
    let mut state = nanos as u64;
    state ^= state.rotate_left(13);
    state ^= state.rotate_right(7);
    state ^= state.rotate_left(17);

    state as f64 / u64::MAX as f64
}
