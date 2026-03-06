use std::collections::{HashMap, VecDeque};
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
    pub commit_ts_micros: u64,
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
    pub receiver: WatchReceiver,
}

struct ActiveSubscriptionGuard {
    active_subscribers: Arc<AtomicUsize>,
}

impl Drop for ActiveSubscriptionGuard {
    fn drop(&mut self) {
        self.active_subscribers.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct WatchReceiver {
    inner: broadcast::Receiver<Arc<WatchEvent>>,
    _active_guard: ActiveSubscriptionGuard,
}

impl WatchReceiver {
    pub async fn recv(
        &mut self,
    ) -> Result<Arc<WatchEvent>, tokio::sync::broadcast::error::RecvError> {
        self.inner.recv().await
    }

    pub fn try_recv(
        &mut self,
    ) -> Result<Arc<WatchEvent>, tokio::sync::broadcast::error::TryRecvError> {
        self.inner.try_recv()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WatchRouteKey {
    key: Vec<u8>,
    range_end: Vec<u8>,
}

#[derive(Debug, Clone)]
struct WatchRoute {
    key: Vec<u8>,
    range_end: Vec<u8>,
    tx: broadcast::Sender<Arc<WatchEvent>>,
}

impl WatchRoute {
    fn matches(&self, candidate: &[u8]) -> bool {
        watch_range_matches(&self.key, &self.range_end, candidate)
    }
}

#[derive(Debug)]
pub struct WatchRing {
    ring: RwLock<VecDeque<Arc<WatchEvent>>>,
    ring_bytes: AtomicUsize,
    capacity: usize,
    broadcast_capacity: usize,
    routes: RwLock<HashMap<WatchRouteKey, WatchRoute>>,
    active_subscribers: Arc<AtomicUsize>,
    backlog_mode: WatchBacklogMode,
    dropped_no_subscriber: AtomicUsize,
}

impl WatchRing {
    pub fn new(
        ring_capacity: usize,
        broadcast_capacity: usize,
        backlog_mode: WatchBacklogMode,
    ) -> Self {
        Self {
            ring: RwLock::new(VecDeque::with_capacity(ring_capacity)),
            ring_bytes: AtomicUsize::new(0),
            capacity: ring_capacity,
            broadcast_capacity: broadcast_capacity.max(32),
            routes: RwLock::new(HashMap::new()),
            active_subscribers: Arc::new(AtomicUsize::new(0)),
            backlog_mode,
            dropped_no_subscriber: AtomicUsize::new(0),
        }
    }

    pub fn publish(&self, event: WatchEvent) {
        let event = Arc::new(event);
        let subscribers = self.active_subscribers.load(Ordering::Relaxed);
        let matching_routes = if subscribers > 0 {
            let routes = self.routes.read();
            routes
                .values()
                .filter(|route| route.matches(event.key.as_slice()))
                .map(|route| route.tx.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let keep_backlog = match self.backlog_mode {
            WatchBacklogMode::Strict => true,
            // In relaxed mode, retain backlog only for keys with active matching routes.
            // This avoids extra follower apply work for unrelated write storms.
            WatchBacklogMode::Relaxed => !matching_routes.is_empty(),
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

        if !matching_routes.is_empty() {
            for tx in matching_routes {
                let _ = tx.send(event.clone());
            }
        }
    }

    pub fn backlog(&self, filter: &WatchFilter) -> Vec<Arc<WatchEvent>> {
        let ring = self.ring.read();
        let newest_revision = ring.back().map(|ev| ev.mod_revision).unwrap_or(0);
        if newest_revision < filter.start_revision {
            Vec::new()
        } else {
            ring.iter()
                .filter(|ev| {
                    ev.mod_revision >= filter.start_revision && filter.matches(ev.key.as_slice())
                })
                .cloned()
                .collect::<Vec<_>>()
        }
    }

    pub fn subscribe_live(&self, filter: &WatchFilter) -> WatchReceiver {
        let route_key = WatchRouteKey {
            key: filter.key.clone(),
            range_end: filter.range_end.clone(),
        };
        let receiver = {
            let mut routes = self.routes.write();
            if routes.len() >= 4096 {
                routes.retain(|_, route| route.tx.receiver_count() > 0);
            }
            routes
                .entry(route_key.clone())
                .or_insert_with(|| {
                    let (tx, _rx) = broadcast::channel(self.broadcast_capacity);
                    WatchRoute {
                        key: route_key.key.clone(),
                        range_end: route_key.range_end.clone(),
                        tx,
                    }
                })
                .tx
                .subscribe()
        };
        self.active_subscribers.fetch_add(1, Ordering::Relaxed);

        WatchReceiver {
            inner: receiver,
            _active_guard: ActiveSubscriptionGuard {
                active_subscribers: self.active_subscribers.clone(),
            },
        }
    }

    pub fn subscribe(&self, filter: &WatchFilter) -> WatchSubscription {
        WatchSubscription {
            backlog: self.backlog(filter),
            receiver: self.subscribe_live(filter),
        }
    }
}

fn watch_range_matches(key: &[u8], range_end: &[u8], candidate: &[u8]) -> bool {
    if range_end.is_empty() {
        return candidate == key;
    }

    if key.is_empty() && range_end == [0] {
        return true;
    }

    candidate >= key && candidate < range_end
}

fn event_bytes(event: &WatchEvent) -> usize {
    event
        .key
        .len()
        .saturating_add(event.value.len())
        .saturating_add(event.prev_value.len())
        .saturating_add(64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(key: &[u8], revision: i64) -> WatchEvent {
        WatchEvent {
            kind: WatchEventKind::Put,
            key: key.to_vec(),
            value: Arc::<[u8]>::from(b"value".as_slice()),
            prev_value: Arc::<[u8]>::from(b"".as_slice()),
            commit_ts_micros: 0,
            create_revision: revision,
            mod_revision: revision,
            version: 1,
            lease: 0,
        }
    }

    fn prefix_filter(prefix: &[u8]) -> WatchFilter {
        let mut range_end = prefix.to_vec();
        if let Some(last) = range_end.last_mut() {
            *last = last.saturating_add(1);
        } else {
            range_end.push(0);
        }
        WatchFilter {
            key: prefix.to_vec(),
            range_end,
            start_revision: 1,
        }
    }

    #[test]
    fn subscribe_only_receives_matching_live_events() {
        let ring = WatchRing::new(8, 8, WatchBacklogMode::Relaxed);
        let mut sub = ring.subscribe(&prefix_filter(b"/watch/ns/"));

        ring.publish(event(b"/watch/write/a", 1));
        assert!(matches!(
            sub.receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        ring.publish(event(b"/watch/ns/a", 2));
        let recv = sub.receiver.try_recv().expect("matching event");
        assert_eq!(recv.key.as_slice(), b"/watch/ns/a");
    }

    #[tokio::test]
    async fn subscribe_receives_matching_live_events_via_recv() {
        let ring = WatchRing::new(8, 8, WatchBacklogMode::Relaxed);
        let mut sub = ring.subscribe(&WatchFilter {
            key: b"/phase12/watch/ns".to_vec(),
            range_end: {
                let mut end = b"/phase12/watch/ns".to_vec();
                let last = end.len() - 1;
                end[last] += 1;
                end
            },
            start_revision: 3,
        });

        ring.publish(event(b"/phase12/watch/write/a", 2));
        ring.publish(event(b"/phase12/watch/ns", 3));

        let recv = tokio::time::timeout(std::time::Duration::from_millis(50), sub.receiver.recv())
            .await
            .expect("recv timeout")
            .expect("recv ok");
        assert_eq!(recv.key.as_slice(), b"/phase12/watch/ns");
    }

    #[test]
    fn subscribe_skips_backlog_scan_when_start_revision_is_newer() {
        let ring = WatchRing::new(8, 8, WatchBacklogMode::Strict);
        ring.publish(event(b"/watch/ns/a", 5));
        ring.publish(event(b"/watch/ns/b", 6));

        let sub = ring.subscribe(&WatchFilter {
            key: b"/watch/ns/".to_vec(),
            range_end: b"/watch/ns0".to_vec(),
            start_revision: 7,
        });

        assert!(sub.backlog.is_empty());
    }

    #[test]
    fn relaxed_mode_skips_backlog_for_unmatched_keys() {
        let ring = WatchRing::new(8, 8, WatchBacklogMode::Relaxed);
        let mut sub = ring.subscribe(&prefix_filter(b"/watch/ns/"));

        ring.publish(event(b"/watch/other/a", 1));

        assert!(matches!(
            sub.receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let replay = ring.subscribe(&prefix_filter(b"/watch/ns/"));
        assert!(replay.backlog.is_empty());
    }
}
