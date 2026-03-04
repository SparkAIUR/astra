use std::sync::atomic::{AtomicUsize, Ordering};

use crate::errors::StoreError;

#[derive(Debug)]
pub struct MemoryTracker {
    max_bytes: usize,
    used_bytes: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    Normal,
    High,
    Critical,
}

impl MemoryTracker {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            used_bytes: AtomicUsize::new(0),
        }
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn pressure(&self) -> MemoryPressure {
        let used = self.used_bytes();
        let ratio = used as f64 / self.max_bytes.max(1) as f64;
        if ratio >= 0.95 {
            MemoryPressure::Critical
        } else if ratio >= 0.85 {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        }
    }

    pub fn try_increase(&self, amount: usize) -> Result<(), StoreError> {
        if amount == 0 {
            return Ok(());
        }

        let mut prev = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = prev.saturating_add(amount);
            if next > self.max_bytes {
                return Err(StoreError::ResourceExhausted(format!(
                    "memory cap exceeded: requested={} used={} max={}",
                    amount, prev, self.max_bytes
                )));
            }

            match self.used_bytes.compare_exchange_weak(
                prev,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
        Ok(())
    }

    pub fn decrease(&self, amount: usize) {
        if amount == 0 {
            return;
        }

        let mut prev = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = prev.saturating_sub(amount);
            match self.used_bytes.compare_exchange_weak(
                prev,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }
}
