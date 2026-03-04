use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

use crate::metrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBudgetLane {
    Bytes,
    Sqe,
}

#[derive(Debug, Clone)]
pub struct IoTokenBucket {
    inner: Arc<IoTokenBucketInner>,
}

#[derive(Debug)]
struct IoTokenBucketInner {
    lane: IoBudgetLane,
    enabled: bool,
    refill_tokens_per_sec: AtomicU64,
    burst_tokens: AtomicU64,
    state: AsyncMutex<IoTokenBucketState>,
}

#[derive(Debug)]
struct IoTokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl IoTokenBucket {
    pub fn new(enabled: bool, refill_tokens_per_sec: u64, burst_tokens: u64) -> Self {
        Self::new_for_lane(
            IoBudgetLane::Bytes,
            enabled,
            refill_tokens_per_sec,
            burst_tokens,
        )
    }

    pub fn new_for_lane(
        lane: IoBudgetLane,
        enabled: bool,
        refill_tokens_per_sec: u64,
        burst_tokens: u64,
    ) -> Self {
        let refill_tokens_per_sec = if enabled {
            refill_tokens_per_sec.max(1)
        } else {
            0
        };
        let burst_tokens = if enabled { burst_tokens.max(1) } else { 0 };
        let now = Instant::now();
        let state = IoTokenBucketState {
            tokens: burst_tokens as f64,
            last_refill: now,
        };
        Self {
            inner: Arc::new(IoTokenBucketInner {
                lane,
                enabled,
                refill_tokens_per_sec: AtomicU64::new(refill_tokens_per_sec),
                burst_tokens: AtomicU64::new(burst_tokens),
                state: AsyncMutex::new(state),
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    pub async fn configure(&self, refill_tokens_per_sec: u64, burst_tokens: u64) {
        if !self.enabled() {
            return;
        }

        let refill_tokens_per_sec = refill_tokens_per_sec.max(1);
        let burst_tokens = burst_tokens.max(1);
        self.inner
            .refill_tokens_per_sec
            .store(refill_tokens_per_sec, Ordering::Relaxed);
        self.inner
            .burst_tokens
            .store(burst_tokens, Ordering::Relaxed);

        let mut guard = self.inner.state.lock().await;
        let burst_f = burst_tokens as f64;
        guard.tokens = guard.tokens.min(burst_f).max(0.0);
        match self.inner.lane {
            IoBudgetLane::Bytes => metrics::set_bg_io_tokens_available(guard.tokens.floor() as u64),
            IoBudgetLane::Sqe => {
                metrics::set_bg_io_sqe_tokens_available(guard.tokens.floor() as u64)
            }
        }
    }

    pub async fn acquire(&self, tokens: u64) {
        if !self.enabled() {
            return;
        }

        let burst_tokens = self.inner.burst_tokens.load(Ordering::Relaxed).max(1) as f64;
        let mut needed = tokens.max(1) as f64;
        if needed > burst_tokens {
            needed = burst_tokens;
        }

        loop {
            let maybe_wait = {
                let mut guard = self.inner.state.lock().await;
                let refill_tokens_per_sec = self
                    .inner
                    .refill_tokens_per_sec
                    .load(Ordering::Relaxed)
                    .max(1) as f64;
                let burst_tokens = self.inner.burst_tokens.load(Ordering::Relaxed).max(1) as f64;
                let now = Instant::now();
                let elapsed = now
                    .saturating_duration_since(guard.last_refill)
                    .as_secs_f64();
                if elapsed > 0.0 {
                    let refill = elapsed * refill_tokens_per_sec;
                    guard.tokens = (guard.tokens + refill).min(burst_tokens);
                    guard.last_refill = now;
                }

                if guard.tokens >= needed {
                    guard.tokens -= needed;
                    match self.inner.lane {
                        IoBudgetLane::Bytes => {
                            metrics::set_bg_io_tokens_available(guard.tokens.floor() as u64);
                        }
                        IoBudgetLane::Sqe => {
                            metrics::set_bg_io_sqe_tokens_available(guard.tokens.floor() as u64);
                        }
                    }
                    None
                } else {
                    let deficit = needed - guard.tokens;
                    let secs = deficit / refill_tokens_per_sec;
                    let wait = Duration::from_secs_f64(secs.max(0.001));
                    match self.inner.lane {
                        IoBudgetLane::Bytes => {
                            metrics::set_bg_io_tokens_available(guard.tokens.floor() as u64);
                        }
                        IoBudgetLane::Sqe => {
                            metrics::set_bg_io_sqe_tokens_available(guard.tokens.floor() as u64);
                        }
                    }
                    Some(wait)
                }
            };

            match maybe_wait {
                Some(wait) => {
                    match self.inner.lane {
                        IoBudgetLane::Bytes => {
                            metrics::observe_bg_io_throttle_wait_ms(wait.as_millis() as u64);
                        }
                        IoBudgetLane::Sqe => {
                            metrics::observe_bg_io_sqe_wait_ms(wait.as_millis() as u64);
                        }
                    }
                    tokio::time::sleep(wait).await;
                }
                None => return,
            }
        }
    }
}
