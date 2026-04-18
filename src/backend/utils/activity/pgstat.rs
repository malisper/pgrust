use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use crate::include::nodes::datetime::TimestampTzADT;

use super::pgstat_function::{
    FunctionCallFrame, FunctionStatsDelta, FunctionStatsEntry, TrackFunctionsSetting,
};
use super::pgstat_io::{IoStatsDelta, IoStatsEntry, IoStatsKey, default_pg_stat_io_keys};
use super::pgstat_relation::{RelationStatsDelta, RelationStatsEntry, RelationTransactionState};
use super::pgstat_xact::StatsMutationEffect;

#[derive(Debug, Clone, Default)]
struct StatsReadCache {
    relations: BTreeMap<u32, Option<RelationStatsEntry>>,
    functions: BTreeMap<u32, Option<FunctionStatsEntry>>,
    io: BTreeMap<IoStatsKey, Option<IoStatsEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsFetchConsistency {
    None,
    Cache,
    Snapshot,
}

impl StatsFetchConsistency {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "cache" => Some(Self::Cache),
            "snapshot" => Some(Self::Snapshot),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Cache => "cache",
            Self::Snapshot => "snapshot",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StatsDelta {
    pub relations: BTreeMap<u32, RelationStatsDelta>,
    pub functions: BTreeMap<u32, FunctionStatsDelta>,
    pub io: BTreeMap<IoStatsKey, IoStatsDelta>,
}

impl StatsDelta {
    pub(crate) fn clear(&mut self) {
        self.relations.clear();
        self.functions.clear();
        self.io.clear();
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DatabaseStatsStore {
    pub relations: BTreeMap<u32, RelationStatsEntry>,
    pub functions: BTreeMap<u32, FunctionStatsEntry>,
    pub io: BTreeMap<IoStatsKey, IoStatsEntry>,
}

impl DatabaseStatsStore {
    pub(crate) fn with_default_io_rows() -> Self {
        let mut store = Self::default();
        for key in default_pg_stat_io_keys() {
            store.io.insert(
                key,
                IoStatsEntry {
                    stats_reset: Some(now_timestamptz()),
                    ..IoStatsEntry::default()
                },
            );
        }
        store
    }

    pub(crate) fn apply_pending_flush(&mut self, pending: &mut StatsDelta) {
        for (oid, delta) in std::mem::take(&mut pending.relations) {
            self.relations.entry(oid).or_default().apply_delta(&delta);
        }
        for (oid, delta) in std::mem::take(&mut pending.functions) {
            self.functions.entry(oid).or_default().apply_delta(&delta);
        }
        for (key, delta) in std::mem::take(&mut pending.io) {
            self.io.entry(key).or_default().apply_delta(&delta);
        }
    }

    pub(crate) fn snapshot_with_pending(&self, pending: &StatsDelta) -> Self {
        let mut snapshot = self.clone();
        for (oid, delta) in &pending.relations {
            snapshot.relations.entry(*oid).or_default().apply_delta(delta);
        }
        for (oid, delta) in &pending.functions {
            snapshot.functions.entry(*oid).or_default().apply_delta(delta);
        }
        for (key, delta) in &pending.io {
            snapshot.io.entry(key.clone()).or_default().apply_delta(delta);
        }
        snapshot
    }

    pub(crate) fn merged_relation_entry(
        &self,
        pending: &StatsDelta,
        oid: u32,
    ) -> Option<RelationStatsEntry> {
        let mut entry = self.relations.get(&oid).cloned();
        if let Some(delta) = pending.relations.get(&oid) {
            entry
                .get_or_insert_with(RelationStatsEntry::default)
                .apply_delta(delta);
        }
        entry
    }

    pub(crate) fn merged_function_entry(
        &self,
        pending: &StatsDelta,
        oid: u32,
    ) -> Option<FunctionStatsEntry> {
        let mut entry = self.functions.get(&oid).cloned();
        if let Some(delta) = pending.functions.get(&oid) {
            entry
                .get_or_insert_with(FunctionStatsEntry::default)
                .apply_delta(delta);
        }
        entry
    }

    pub(crate) fn merged_io_entry(
        &self,
        pending: &StatsDelta,
        key: &IoStatsKey,
    ) -> Option<IoStatsEntry> {
        let mut entry = self.io.get(key).cloned();
        if let Some(delta) = pending.io.get(key) {
            entry
                .get_or_insert_with(IoStatsEntry::default)
                .apply_delta(delta);
        }
        entry
    }

    pub(crate) fn remove_relation(&mut self, oid: u32) {
        self.relations.remove(&oid);
    }

    pub(crate) fn remove_function(&mut self, oid: u32) {
        self.functions.remove(&oid);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionStatsState {
    pub pending_flush: StatsDelta,
    pub fetch_consistency: StatsFetchConsistency,
    pub track_functions: TrackFunctionsSetting,
    cache_snapshot: StatsReadCache,
    snapshot_store: Option<DatabaseStatsStore>,
    pub snapshot_timestamp: Option<TimestampTzADT>,
    pub relation_xact: BTreeMap<u32, RelationTransactionState>,
    pub function_xact: BTreeMap<u32, FunctionStatsDelta>,
    pub stats_effects: Vec<StatsMutationEffect>,
    pub dropped_relations_in_xact: BTreeSet<u32>,
    pub dropped_functions_in_xact: BTreeSet<u32>,
    pub xact_active: bool,
    pub(super) call_stack: Vec<FunctionCallFrame>,
}

impl Default for SessionStatsState {
    fn default() -> Self {
        Self {
            pending_flush: StatsDelta::default(),
            fetch_consistency: StatsFetchConsistency::Cache,
            track_functions: TrackFunctionsSetting::None,
            cache_snapshot: StatsReadCache::default(),
            snapshot_store: None,
            snapshot_timestamp: None,
            relation_xact: BTreeMap::new(),
            function_xact: BTreeMap::new(),
            stats_effects: Vec::new(),
            dropped_relations_in_xact: BTreeSet::new(),
            dropped_functions_in_xact: BTreeSet::new(),
            xact_active: false,
            call_stack: Vec::new(),
        }
    }
}

impl SessionStatsState {
    pub(crate) fn clear_snapshot(&mut self) {
        self.cache_snapshot = StatsReadCache::default();
        self.snapshot_store = None;
        self.snapshot_timestamp = None;
    }

    pub(crate) fn set_fetch_consistency(&mut self, fetch_consistency: StatsFetchConsistency) {
        if self.fetch_consistency != fetch_consistency {
            self.fetch_consistency = fetch_consistency;
            self.clear_snapshot();
        }
    }

    pub(crate) fn set_track_functions(&mut self, track_functions: TrackFunctionsSetting) {
        self.track_functions = track_functions;
    }

    pub(crate) fn visible_relation_entry(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oid: u32,
    ) -> Option<RelationStatsEntry> {
        match self.fetch_consistency {
            StatsFetchConsistency::None => db_stats
                .read()
                .merged_relation_entry(&self.pending_flush, oid),
            StatsFetchConsistency::Cache => {
                if let Some(cached) = self.cache_snapshot.relations.get(&oid).cloned() {
                    return cached;
                }
                let entry = db_stats
                    .read()
                    .merged_relation_entry(&self.pending_flush, oid);
                self.cache_snapshot.relations.insert(oid, entry.clone());
                entry
            }
            StatsFetchConsistency::Snapshot => {
                self.ensure_snapshot_started(db_stats);
                self.snapshot_store
                    .as_ref()
                    .and_then(|snapshot| snapshot.relations.get(&oid).cloned())
            }
        }
    }

    pub(crate) fn visible_relation_entries(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oids: impl IntoIterator<Item = u32>,
    ) -> BTreeMap<u32, RelationStatsEntry> {
        let requested = oids.into_iter().collect::<BTreeSet<_>>();
        match self.fetch_consistency {
            StatsFetchConsistency::None => {
                let store = db_stats.read();
                requested
                    .into_iter()
                    .filter_map(|oid| {
                        store
                            .merged_relation_entry(&self.pending_flush, oid)
                            .map(|entry| (oid, entry))
                    })
                    .collect()
            }
            StatsFetchConsistency::Cache => {
                let missing = requested
                    .iter()
                    .copied()
                    .filter(|oid| !self.cache_snapshot.relations.contains_key(oid))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    let computed = {
                        let store = db_stats.read();
                        missing
                            .iter()
                            .map(|oid| {
                                (*oid, store.merged_relation_entry(&self.pending_flush, *oid))
                            })
                            .collect::<Vec<_>>()
                    };
                    for (oid, entry) in computed {
                        self.cache_snapshot.relations.insert(oid, entry);
                    }
                }
                requested
                    .into_iter()
                    .filter_map(|oid| {
                        self.cache_snapshot
                            .relations
                            .get(&oid)
                            .cloned()
                            .flatten()
                            .map(|entry| (oid, entry))
                    })
                    .collect()
            }
            StatsFetchConsistency::Snapshot => {
                self.ensure_snapshot_started(db_stats);
                let Some(snapshot) = self.snapshot_store.as_ref() else {
                    return BTreeMap::new();
                };
                requested
                    .into_iter()
                    .filter_map(|oid| {
                        snapshot
                            .relations
                            .get(&oid)
                            .cloned()
                            .map(|entry| (oid, entry))
                    })
                    .collect()
            }
        }
    }

    pub(crate) fn has_visible_relation_stats(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oid: u32,
    ) -> bool {
        self.visible_relation_entry(db_stats, oid).is_some()
    }

    pub(crate) fn visible_function_entry(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oid: u32,
    ) -> Option<FunctionStatsEntry> {
        match self.fetch_consistency {
            StatsFetchConsistency::None => db_stats
                .read()
                .merged_function_entry(&self.pending_flush, oid),
            StatsFetchConsistency::Cache => {
                if let Some(cached) = self.cache_snapshot.functions.get(&oid).cloned() {
                    return cached;
                }
                let entry = db_stats
                    .read()
                    .merged_function_entry(&self.pending_flush, oid);
                self.cache_snapshot.functions.insert(oid, entry.clone());
                entry
            }
            StatsFetchConsistency::Snapshot => {
                self.ensure_snapshot_started(db_stats);
                self.snapshot_store
                    .as_ref()
                    .and_then(|snapshot| snapshot.functions.get(&oid).cloned())
            }
        }
    }

    pub(crate) fn visible_function_entries(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oids: impl IntoIterator<Item = u32>,
    ) -> BTreeMap<u32, FunctionStatsEntry> {
        let requested = oids.into_iter().collect::<BTreeSet<_>>();
        match self.fetch_consistency {
            StatsFetchConsistency::None => {
                let store = db_stats.read();
                requested
                    .into_iter()
                    .filter_map(|oid| {
                        store
                            .merged_function_entry(&self.pending_flush, oid)
                            .map(|entry| (oid, entry))
                    })
                    .collect()
            }
            StatsFetchConsistency::Cache => {
                let missing = requested
                    .iter()
                    .copied()
                    .filter(|oid| !self.cache_snapshot.functions.contains_key(oid))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    let computed = {
                        let store = db_stats.read();
                        missing
                            .iter()
                            .map(|oid| {
                                (*oid, store.merged_function_entry(&self.pending_flush, *oid))
                            })
                            .collect::<Vec<_>>()
                    };
                    for (oid, entry) in computed {
                        self.cache_snapshot.functions.insert(oid, entry);
                    }
                }
                requested
                    .into_iter()
                    .filter_map(|oid| {
                        self.cache_snapshot
                            .functions
                            .get(&oid)
                            .cloned()
                            .flatten()
                            .map(|entry| (oid, entry))
                    })
                    .collect()
            }
            StatsFetchConsistency::Snapshot => {
                self.ensure_snapshot_started(db_stats);
                let Some(snapshot) = self.snapshot_store.as_ref() else {
                    return BTreeMap::new();
                };
                requested
                    .into_iter()
                    .filter_map(|oid| {
                        snapshot
                            .functions
                            .get(&oid)
                            .cloned()
                            .map(|entry| (oid, entry))
                    })
                    .collect()
            }
        }
    }

    pub(crate) fn has_visible_function_stats(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oid: u32,
    ) -> bool {
        self.visible_function_entry(db_stats, oid).is_some()
    }

    pub(crate) fn visible_io_entries(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        keys: impl IntoIterator<Item = IoStatsKey>,
    ) -> BTreeMap<IoStatsKey, IoStatsEntry> {
        let requested = keys.into_iter().collect::<BTreeSet<_>>();
        match self.fetch_consistency {
            StatsFetchConsistency::None => {
                let store = db_stats.read();
                requested
                    .into_iter()
                    .filter_map(|key| {
                        store
                            .merged_io_entry(&self.pending_flush, &key)
                            .map(|entry| (key, entry))
                    })
                    .collect()
            }
            StatsFetchConsistency::Cache => {
                let missing = requested
                    .iter()
                    .filter(|key| !self.cache_snapshot.io.contains_key(*key))
                    .cloned()
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    let computed = {
                        let store = db_stats.read();
                        missing
                            .iter()
                            .map(|key| {
                                (key.clone(), store.merged_io_entry(&self.pending_flush, key))
                            })
                            .collect::<Vec<_>>()
                    };
                    for (key, entry) in computed {
                        self.cache_snapshot.io.insert(key, entry);
                    }
                }
                requested
                    .into_iter()
                    .filter_map(|key| {
                        self.cache_snapshot
                            .io
                            .get(&key)
                            .cloned()
                            .flatten()
                            .map(|entry| (key, entry))
                    })
                    .collect()
            }
            StatsFetchConsistency::Snapshot => {
                self.ensure_snapshot_started(db_stats);
                let Some(snapshot) = self.snapshot_store.as_ref() else {
                    return BTreeMap::new();
                };
                requested
                    .into_iter()
                    .filter_map(|key| {
                        snapshot.io.get(&key).cloned().map(|entry| (key, entry))
                    })
                    .collect()
            }
        }
    }

    pub(crate) fn snapshot_timestamp(&self) -> Option<TimestampTzADT> {
        self.snapshot_timestamp
    }

    pub(crate) fn flush_pending(&mut self, db_stats: &Arc<RwLock<DatabaseStatsStore>>) {
        db_stats
            .write()
            .apply_pending_flush(&mut self.pending_flush);
        self.clear_snapshot();
    }

    fn ensure_snapshot_started(&mut self, db_stats: &Arc<RwLock<DatabaseStatsStore>>) {
        if self.snapshot_store.is_none() {
            self.snapshot_store = Some(db_stats.read().snapshot_with_pending(&self.pending_flush));
        }
        if self.snapshot_timestamp.is_none() {
            self.snapshot_timestamp = Some(now_timestamptz());
        }
    }
}

pub(crate) fn now_timestamptz() -> TimestampTzADT {
    TimestampTzADT(current_postgres_timestamp_usecs())
}
