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

    pub(crate) fn merged_with_pending(&self, pending: &StatsDelta) -> Self {
        let mut merged = self.clone();
        for (oid, delta) in &pending.relations {
            merged.relations.entry(*oid).or_default().apply_delta(delta);
        }
        for (oid, delta) in &pending.functions {
            merged.functions.entry(*oid).or_default().apply_delta(delta);
        }
        for (key, delta) in &pending.io {
            merged.io.entry(key.clone()).or_default().apply_delta(delta);
        }
        merged
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
    pub cache_snapshot: Option<DatabaseStatsStore>,
    pub full_snapshot: Option<DatabaseStatsStore>,
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
            cache_snapshot: None,
            full_snapshot: None,
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
        self.cache_snapshot = None;
        self.full_snapshot = None;
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

    pub(crate) fn visible_stats(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
    ) -> DatabaseStatsStore {
        match self.fetch_consistency {
            StatsFetchConsistency::None => db_stats.read().merged_with_pending(&self.pending_flush),
            StatsFetchConsistency::Cache => {
                if self.cache_snapshot.is_none() {
                    self.cache_snapshot =
                        Some(db_stats.read().merged_with_pending(&self.pending_flush));
                }
                self.cache_snapshot.clone().unwrap_or_default()
            }
            StatsFetchConsistency::Snapshot => {
                if self.full_snapshot.is_none() {
                    self.full_snapshot =
                        Some(db_stats.read().merged_with_pending(&self.pending_flush));
                    self.snapshot_timestamp = Some(now_timestamptz());
                }
                self.full_snapshot.clone().unwrap_or_default()
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
}

pub(crate) fn now_timestamptz() -> TimestampTzADT {
    TimestampTzADT(current_postgres_timestamp_usecs())
}
