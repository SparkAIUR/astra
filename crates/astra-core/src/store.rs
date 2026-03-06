use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::config::WatchBacklogMode;
use crate::errors::StoreError;
use crate::memory::{MemoryPressure, MemoryTracker};
use crate::metrics;
use crate::watch::{
    WatchEvent, WatchEventKind, WatchFilter, WatchReceiver, WatchRing, WatchSubscription,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueEntry {
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseEntry {
    pub id: i64,
    pub granted_ttl: i64,
    pub ttl: i64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SnapshotState {
    pub kv: Vec<(Vec<u8>, ValueEntry)>,
    pub leases: Vec<(i64, LeaseEntry)>,
    pub next_lease_id: i64,
    pub revision: i64,
    pub compact_revision: i64,
}

#[derive(Debug)]
struct StoreState {
    map: BTreeMap<Vec<u8>, ValueEntry>,
    leases: HashMap<i64, LeaseEntry>,
    prefix_max_mod_revision: HashMap<Vec<u8>, i64>,
    next_lease_id: i64,
    revision: i64,
    compact_revision: i64,
}

impl Default for StoreState {
    fn default() -> Self {
        Self {
            map: BTreeMap::new(),
            leases: HashMap::new(),
            prefix_max_mod_revision: HashMap::new(),
            next_lease_id: 1,
            revision: 0,
            compact_revision: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PutOutput {
    pub revision: i64,
    pub prev: Option<ValueEntry>,
    pub current: ValueEntry,
}

#[derive(Debug, Clone)]
pub struct RangeOutput {
    pub revision: i64,
    pub count: i64,
    pub more: bool,
    pub kvs: Vec<(Vec<u8>, ValueEntry)>,
}

#[derive(Debug, Clone)]
pub struct DeleteOutput {
    pub revision: i64,
    pub deleted: i64,
    pub prev_kvs: Vec<(Vec<u8>, ValueEntry)>,
}

#[derive(Debug, Clone)]
pub struct LeaseGrantOutput {
    pub revision: i64,
    pub id: i64,
    pub ttl: i64,
}

#[derive(Debug, Clone)]
pub struct LeaseRevokeOutput {
    pub revision: i64,
    pub deleted: i64,
}

#[derive(Debug, Clone)]
pub struct LeaseTtlOutput {
    pub revision: i64,
    pub id: i64,
    pub ttl: i64,
    pub granted_ttl: i64,
    pub keys: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub struct KvStore {
    state: RwLock<StoreState>,
    memory: MemoryTracker,
    watch_ring: WatchRing,
    hot_revision_window: i64,
    prefix_filter_enabled: bool,
    revision_filter_enabled: bool,
}

impl KvStore {
    pub fn open(
        data_dir: &Path,
        max_memory_bytes: usize,
        hot_revision_window: i64,
        prefix_filter_enabled: bool,
        revision_filter_enabled: bool,
        watch_ring_capacity: usize,
        watch_broadcast_capacity: usize,
        watch_backlog_mode: WatchBacklogMode,
    ) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir)?;

        Ok(Self {
            state: RwLock::new(StoreState::default()),
            memory: MemoryTracker::new(max_memory_bytes),
            watch_ring: WatchRing::new(
                watch_ring_capacity,
                watch_broadcast_capacity,
                watch_backlog_mode,
            ),
            hot_revision_window,
            prefix_filter_enabled,
            revision_filter_enabled,
        })
    }

    fn with_indexed_prefixes(key: &[u8], mut visit: impl FnMut(&[u8])) {
        if key.is_empty() {
            return;
        }

        let mut segment_count = 0usize;
        for (idx, byte) in key.iter().enumerate() {
            if *byte != b'/' || idx == 0 {
                continue;
            }
            segment_count = segment_count.saturating_add(1);
            if segment_count > 4 {
                break;
            }
            visit(&key[..=idx]);
        }
        visit(key);
    }

    fn update_prefix_revision_index_for_entry(state: &mut StoreState, key: &[u8], revision: i64) {
        Self::with_indexed_prefixes(key, |prefix| {
            let current = state
                .prefix_max_mod_revision
                .entry(prefix.to_vec())
                .or_insert(revision);
            if revision > *current {
                *current = revision;
            }
        });
    }

    fn rebuild_prefix_revision_index_locked(state: &mut StoreState) {
        state.prefix_max_mod_revision.clear();
        let rows = state
            .map
            .iter()
            .map(|(k, v)| (k.clone(), v.mod_revision))
            .collect::<Vec<_>>();
        for (key, revision) in rows {
            Self::update_prefix_revision_index_for_entry(state, &key, revision);
        }
    }

    fn refresh_compaction_locked(&self, state: &mut StoreState) {
        let watermark = state.revision.saturating_sub(self.hot_revision_window);
        if watermark > state.compact_revision {
            state.compact_revision = watermark;
        }
    }

    fn bump_revision(&self) -> i64 {
        let mut state = self.state.write();
        state.revision += 1;
        self.refresh_compaction_locked(&mut state);
        state.revision
    }

    pub fn reserve_revision(&self) -> i64 {
        self.bump_revision()
    }

    pub fn memory_pressure(&self) -> MemoryPressure {
        self.memory.pressure()
    }

    pub fn current_revision(&self) -> i64 {
        self.state.read().revision
    }

    pub fn compact_revision(&self) -> i64 {
        self.state.read().compact_revision
    }

    pub fn compact_to(&self, revision: i64) -> Result<i64, StoreError> {
        if revision <= 0 {
            return Err(StoreError::InvalidArgument(
                "compact revision must be > 0".to_string(),
            ));
        }

        let mut state = self.state.write();
        if revision > state.revision {
            return Err(StoreError::InvalidArgument(format!(
                "compact revision {} is ahead of current revision {}",
                revision, state.revision
            )));
        }
        if revision > state.compact_revision {
            state.compact_revision = revision;
        }
        Ok(state.compact_revision)
    }

    pub fn is_empty(&self) -> bool {
        self.state.read().map.is_empty()
    }

    pub fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        ignore_value: bool,
        ignore_lease: bool,
    ) -> Result<PutOutput, StoreError> {
        let revision = self.bump_revision();
        self.apply_put_at_revision(key, value, lease, ignore_value, ignore_lease, revision)
    }

    pub fn apply_put_at_revision(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        ignore_value: bool,
        ignore_lease: bool,
        revision: i64,
    ) -> Result<PutOutput, StoreError> {
        if key.is_empty() {
            return Err(StoreError::InvalidArgument("empty key".to_string()));
        }

        let (prev, current) = {
            let mut state = self.state.write();
            let prev = state.map.get(&key).cloned();

            if ignore_value && prev.is_none() {
                return Err(StoreError::InvalidArgument(
                    "ignore_value requires existing key".to_string(),
                ));
            }

            if ignore_lease && prev.is_none() {
                return Err(StoreError::InvalidArgument(
                    "ignore_lease requires existing key".to_string(),
                ));
            }

            let next_value = if ignore_value {
                prev.as_ref().map(|v| v.value.clone()).unwrap_or_default()
            } else {
                value
            };

            let next_lease = if ignore_lease {
                prev.as_ref().map(|v| v.lease).unwrap_or(0)
            } else {
                lease
            };

            if next_lease > 0 && !state.leases.contains_key(&next_lease) {
                return Err(StoreError::InvalidArgument(format!(
                    "lease {} not found",
                    next_lease
                )));
            }

            let prev_len = prev.as_ref().map(|v| v.value.len()).unwrap_or(0);
            if next_value.len() > prev_len {
                self.memory.try_increase(next_value.len() - prev_len)?;
            } else {
                self.memory.decrease(prev_len - next_value.len());
            }

            let current = ValueEntry {
                create_revision: prev.as_ref().map(|v| v.create_revision).unwrap_or(revision),
                mod_revision: revision,
                version: prev.as_ref().map(|v| v.version + 1).unwrap_or(1),
                value: next_value,
                lease: next_lease,
            };

            state.map.insert(key.clone(), current.clone());
            Self::update_prefix_revision_index_for_entry(&mut state, &key, revision);
            state.revision = state.revision.max(revision);
            self.refresh_compaction_locked(&mut state);

            (prev, current)
        };

        self.watch_ring.publish(WatchEvent {
            kind: WatchEventKind::Put,
            key,
            value: Arc::<[u8]>::from(current.value.clone()),
            prev_value: Arc::<[u8]>::from(
                prev.as_ref().map(|v| v.value.clone()).unwrap_or_default(),
            ),
            commit_ts_micros: now_micros(),
            create_revision: current.create_revision,
            mod_revision: current.mod_revision,
            version: current.version,
            lease: current.lease,
        });

        Ok(PutOutput {
            revision,
            prev,
            current,
        })
    }

    pub fn range(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
        keys_only: bool,
        count_only: bool,
    ) -> Result<RangeOutput, StoreError> {
        let state = self.state.read();

        if revision > 0 && revision <= state.compact_revision {
            return Err(StoreError::Compacted(state.compact_revision));
        }

        let is_prefix_query =
            !key.is_empty() && !range_end.is_empty() && range_end == prefix_end(key).as_slice();
        if self.prefix_filter_enabled && is_prefix_query {
            let has_prefix = state
                .map
                .range(key.to_vec()..)
                .next()
                .map(|(k, _)| k.starts_with(key))
                .unwrap_or(false);
            if !has_prefix {
                metrics::inc_list_prefix_filter_skips();
                return Ok(RangeOutput {
                    revision: state.revision,
                    count: 0,
                    more: false,
                    kvs: Vec::new(),
                });
            }
            metrics::inc_list_prefix_filter_hits();
        }
        if self.revision_filter_enabled && is_prefix_query && revision > 0 {
            let max_mod_revision = state
                .prefix_max_mod_revision
                .get(key)
                .copied()
                .unwrap_or_default();
            if max_mod_revision < revision {
                metrics::inc_list_revision_filter_skips();
                return Ok(RangeOutput {
                    revision: state.revision,
                    count: 0,
                    more: false,
                    kvs: Vec::new(),
                });
            }
            metrics::inc_list_revision_filter_hits();
        }

        let mut found = Vec::new();
        let mut total: i64 = 0;
        let mut more = false;
        let limit = if limit > 0 {
            Some(limit as usize)
        } else {
            None
        };

        let mut visit = |k: &Vec<u8>, v: &ValueEntry| {
            total = total.saturating_add(1);
            if count_only {
                return;
            }
            if let Some(lim) = limit {
                if found.len() >= lim {
                    more = true;
                    return;
                }
            }
            let mut vv = v.clone();
            if keys_only {
                vv.value.clear();
            }
            found.push((k.clone(), vv));
        };

        if range_end.is_empty() {
            if let Some(v) = state.map.get(key) {
                visit(&key.to_vec(), v);
            }
        } else if key.is_empty() && range_end == [0] {
            for (k, v) in state.map.iter() {
                visit(k, v);
            }
        } else {
            for (k, v) in state.map.range(key.to_vec()..range_end.to_vec()) {
                visit(k, v);
            }
        }

        Ok(RangeOutput {
            revision: state.revision,
            count: total,
            more,
            kvs: found,
        })
    }

    pub fn delete_range(
        &self,
        key: &[u8],
        range_end: &[u8],
        prev_kv: bool,
    ) -> Result<DeleteOutput, StoreError> {
        let revision = self.bump_revision();
        self.apply_delete_at_revision(key, range_end, prev_kv, revision)
    }

    pub fn apply_delete_at_revision(
        &self,
        key: &[u8],
        range_end: &[u8],
        prev_kv: bool,
        revision: i64,
    ) -> Result<DeleteOutput, StoreError> {
        let mut removed = Vec::new();
        {
            let mut state = self.state.write();
            let keys = state
                .map
                .keys()
                .filter(|k| key_in_range(k.as_slice(), key, range_end))
                .cloned()
                .collect::<Vec<_>>();

            for k in keys {
                if let Some(v) = state.map.remove(&k) {
                    self.memory.decrease(v.value.len());
                    removed.push((k, v));
                }
            }
            if !removed.is_empty() && self.revision_filter_enabled {
                Self::rebuild_prefix_revision_index_locked(&mut state);
            }

            state.revision = state.revision.max(revision);
            self.refresh_compaction_locked(&mut state);
        }

        for (k, v) in &removed {
            self.watch_ring.publish(WatchEvent {
                kind: WatchEventKind::Delete,
                key: k.clone(),
                value: Arc::<[u8]>::from(Vec::<u8>::new()),
                prev_value: Arc::<[u8]>::from(v.value.clone()),
                commit_ts_micros: now_micros(),
                create_revision: v.create_revision,
                mod_revision: revision,
                version: v.version,
                lease: v.lease,
            });
        }

        Ok(DeleteOutput {
            revision,
            deleted: removed.len() as i64,
            prev_kvs: if prev_kv { removed } else { Vec::new() },
        })
    }

    pub fn subscribe_watch(&self, filter: WatchFilter) -> WatchSubscription {
        self.watch_ring.subscribe(&filter)
    }

    pub fn watch_backlog(&self, filter: &WatchFilter) -> Vec<Arc<WatchEvent>> {
        self.watch_ring.backlog(filter)
    }

    pub fn subscribe_watch_live(&self, filter: &WatchFilter) -> WatchReceiver {
        self.watch_ring.subscribe_live(filter)
    }

    pub fn lease_grant(&self, id: i64, ttl: i64) -> Result<LeaseGrantOutput, StoreError> {
        if ttl <= 0 {
            return Err(StoreError::InvalidArgument("ttl must be > 0".to_string()));
        }

        let mut state = self.state.write();
        let lease_id = if id > 0 {
            id
        } else {
            let out = state.next_lease_id.max(1);
            state.next_lease_id = out.saturating_add(1);
            out
        };

        state.leases.insert(
            lease_id,
            LeaseEntry {
                id: lease_id,
                granted_ttl: ttl,
                ttl,
            },
        );
        state.next_lease_id = state.next_lease_id.max(lease_id.saturating_add(1));

        Ok(LeaseGrantOutput {
            revision: state.revision,
            id: lease_id,
            ttl,
        })
    }

    pub fn lease_keep_alive(&self, id: i64) -> Result<LeaseGrantOutput, StoreError> {
        if id <= 0 {
            return Err(StoreError::InvalidArgument(
                "lease id must be > 0".to_string(),
            ));
        }

        let mut state = self.state.write();
        let lease = state.leases.get_mut(&id).ok_or(StoreError::KeyNotFound)?;
        lease.ttl = lease.granted_ttl;
        let ttl = lease.ttl;
        let revision = state.revision;
        Ok(LeaseGrantOutput { revision, id, ttl })
    }

    pub fn lease_time_to_live(
        &self,
        id: i64,
        include_keys: bool,
    ) -> Result<LeaseTtlOutput, StoreError> {
        if id <= 0 {
            return Err(StoreError::InvalidArgument(
                "lease id must be > 0".to_string(),
            ));
        }
        let state = self.state.read();
        let lease = state.leases.get(&id).ok_or(StoreError::KeyNotFound)?;

        let mut keys = Vec::new();
        if include_keys {
            for (k, v) in state.map.iter() {
                if v.lease == id {
                    keys.push(k.clone());
                }
            }
        }

        Ok(LeaseTtlOutput {
            revision: state.revision,
            id,
            ttl: lease.ttl,
            granted_ttl: lease.granted_ttl,
            keys,
        })
    }

    pub fn lease_list(&self) -> Vec<i64> {
        let state = self.state.read();
        let mut leases = state.leases.keys().copied().collect::<Vec<_>>();
        leases.sort_unstable();
        leases
    }

    pub fn lease_revoke(&self, id: i64) -> Result<LeaseRevokeOutput, StoreError> {
        let revision = self.bump_revision();
        self.apply_lease_revoke_at_revision(id, revision)
    }

    pub fn apply_lease_revoke_at_revision(
        &self,
        id: i64,
        revision: i64,
    ) -> Result<LeaseRevokeOutput, StoreError> {
        if id <= 0 {
            return Err(StoreError::InvalidArgument(
                "lease id must be > 0".to_string(),
            ));
        }

        let mut removed = Vec::new();
        {
            let mut state = self.state.write();
            if state.leases.remove(&id).is_none() {
                return Err(StoreError::KeyNotFound);
            }

            let keys = state
                .map
                .iter()
                .filter(|(_, v)| v.lease == id)
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>();
            for k in keys {
                if let Some(v) = state.map.remove(&k) {
                    self.memory.decrease(v.value.len());
                    removed.push((k, v));
                }
            }
            if !removed.is_empty() && self.revision_filter_enabled {
                Self::rebuild_prefix_revision_index_locked(&mut state);
            }

            state.revision = state.revision.max(revision);
            self.refresh_compaction_locked(&mut state);
        }

        for (k, v) in &removed {
            self.watch_ring.publish(WatchEvent {
                kind: WatchEventKind::Delete,
                key: k.clone(),
                value: Arc::<[u8]>::from(Vec::<u8>::new()),
                prev_value: Arc::<[u8]>::from(v.value.clone()),
                commit_ts_micros: now_micros(),
                create_revision: v.create_revision,
                mod_revision: revision,
                version: v.version,
                lease: v.lease,
            });
        }

        Ok(LeaseRevokeOutput {
            revision,
            deleted: removed.len() as i64,
        })
    }

    pub fn snapshot_state(&self) -> SnapshotState {
        let state = self.state.read();
        SnapshotState {
            kv: state
                .map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>(),
            leases: state
                .leases
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect::<Vec<_>>(),
            next_lease_id: state.next_lease_id,
            revision: state.revision,
            compact_revision: state.compact_revision,
        }
    }

    pub fn load_snapshot_state(&self, snapshot: SnapshotState) -> Result<(), StoreError> {
        let mut state = self.state.write();

        state.map.clear();
        self.memory.decrease(self.memory.used_bytes());

        for (k, v) in snapshot.kv {
            self.memory.try_increase(v.value.len())?;
            state.map.insert(k, v);
        }
        if self.revision_filter_enabled {
            Self::rebuild_prefix_revision_index_locked(&mut state);
        } else {
            state.prefix_max_mod_revision.clear();
        }
        state.leases.clear();
        for (k, v) in snapshot.leases {
            state.leases.insert(k, v);
        }
        state.next_lease_id = snapshot.next_lease_id.max(1);

        state.revision = snapshot.revision;
        state.compact_revision = snapshot.compact_revision;
        Ok(())
    }
}

pub fn key_in_range(candidate: &[u8], key: &[u8], range_end: &[u8]) -> bool {
    if range_end.is_empty() {
        return candidate == key;
    }

    if key.is_empty() && range_end == [0] {
        return true;
    }

    candidate >= key && candidate < range_end
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    vec![0]
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::config::WatchBacklogMode;

    use super::KvStore;

    #[test]
    fn put_get_delete_roundtrip() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let store = KvStore::open(
            tmp.path(),
            4 * 1024 * 1024,
            100,
            true,
            true,
            1024,
            1024,
            WatchBacklogMode::Strict,
        )?;

        let out = store.put(b"a".to_vec(), b"1".to_vec(), 0, false, false)?;
        assert_eq!(out.current.mod_revision, 1);

        let range = store.range(b"a", b"", 0, 0, false, false)?;
        assert_eq!(range.count, 1);

        let del = store.delete_range(b"a", b"", true)?;
        assert_eq!(del.deleted, 1);

        Ok(())
    }

    #[test]
    fn compacted_error_for_old_revision() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let store = KvStore::open(
            tmp.path(),
            4 * 1024 * 1024,
            1,
            true,
            true,
            1024,
            1024,
            WatchBacklogMode::Strict,
        )?;

        store.put(b"a".to_vec(), b"1".to_vec(), 0, false, false)?;
        store.put(b"b".to_vec(), b"2".to_vec(), 0, false, false)?;

        let res = store.range(b"a", b"", 0, 1, false, false);
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn revision_filter_skips_prefix_when_window_is_stale() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let store = KvStore::open(
            tmp.path(),
            4 * 1024 * 1024,
            100,
            true,
            true,
            1024,
            1024,
            WatchBacklogMode::Strict,
        )?;

        let prefix = b"/registry/configmaps/default/";
        let prefix_end = super::prefix_end(prefix);

        store.put(
            b"/registry/configmaps/default/cm-a".to_vec(),
            b"v1".to_vec(),
            0,
            false,
            false,
        )?;
        let fresh = store.range(prefix, &prefix_end, 0, 1, false, false)?;
        assert_eq!(fresh.count, 1);

        let stale = store.range(prefix, &prefix_end, 0, 5, false, false)?;
        assert_eq!(stale.count, 0);

        Ok(())
    }
}
