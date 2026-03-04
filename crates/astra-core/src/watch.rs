use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::debug;

use crate::config::WatchBacklogMode;

#[derive(Debug, Clone)]
pub enum WatchEventKind {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub key: Vec<u8>,
    pub value: Arc<[u8]>,
    pub prev_value: Arc<[u8]>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

#[derive(Debug, Clone)]
pub struct WatchFilter {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub start_revision: i64,
}

impl WatchFilter {
    pub fn matches(&self, candidate: &[u8]) -> bool {
        if self.range_end.is_empty() {
            return candidate == self.key.as_slice();
        }

        if self.key.is_empty() && self.range_end == [0] {
            return true;
        }

        candidate >= self.key.as_slice() && candidate < self.range_end.as_slice()
    }
}

pub struct WatchSubscription {
    pub backlog: Vec<Arc<WatchEvent>>,
    pub receiver: broadcast::Receiver<Arc<WatchEvent>>,
}

#[derive(Debug)]
pub struct WatchRing {
    ring: RwLock<VecDeque<Arc<WatchEvent>>>,
    ring_bytes: AtomicUsize,
    capacity: usize,
    tx: broadcast::Sender<Arc<WatchEvent>>,
    backlog_mode: WatchBacklogMode,
    dropped_no_subscriber: AtomicUsize,
}

impl WatchRing {
    pub fn new(
        ring_capacity: usize,
        broadcast_capacity: usize,
        backlog_mode: WatchBacklogMode,
    ) -> Self {
        let (tx, _rx) = broadcast::channel(broadcast_capacity.max(32));
        Self {
            ring: RwLock::new(VecDeque::with_capacity(ring_capacity)),
            ring_bytes: AtomicUsize::new(0),
            capacity: ring_capacity,
            tx,
            backlog_mode,
            dropped_no_subscriber: AtomicUsize::new(0),
        }
    }

    pub fn publish(&self, event: WatchEvent) {
        let event = Arc::new(event);
        let subscribers = self.tx.receiver_count();
        let keep_backlog = match self.backlog_mode {
            WatchBacklogMode::Strict => true,
            WatchBacklogMode::Relaxed => subscribers > 0,
        };

        if keep_backlog {
            let mut ring = self.ring.write();
            let mut ring_bytes = self.ring_bytes.load(Ordering::Relaxed);
            while ring.len() >= self.capacity && !ring.is_empty() {
                if let Some(ev) = ring.pop_front() {
                    ring_bytes = ring_bytes.saturating_sub(event_bytes(&ev));
                }
            }

            ring.push_back(event.clone());
            ring_bytes = ring_bytes.saturating_add(event_bytes(&event));
            self.ring_bytes.store(ring_bytes, Ordering::Relaxed);
        } else if subscribers == 0 {
            let dropped = self.dropped_no_subscriber.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped % 4096 == 0 {
                debug!(
                    dropped,
                    ring_len = self.ring.read().len(),
                    ring_bytes = self.ring_bytes.load(Ordering::Relaxed),
                    "watch event dropped because no subscribers and backlog_mode=relaxed"
                );
            }
        }

        if subscribers > 0 {
            let _ = self.tx.send(event);
        }
    }

    pub fn subscribe(&self, filter: &WatchFilter) -> WatchSubscription {
        let backlog = {
            let ring = self.ring.read();
            ring.iter()
                .filter(|ev| {
                    ev.mod_revision >= filter.start_revision && filter.matches(ev.key.as_slice())
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        WatchSubscription {
            backlog,
            receiver: self.tx.subscribe(),
        }
    }
}

fn event_bytes(event: &WatchEvent) -> usize {
    event
        .key
        .len()
        .saturating_add(event.value.len())
        .saturating_add(event.prev_value.len())
        .saturating_add(64)
}
