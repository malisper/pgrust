use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use crate::include::nodes::datetime::TimestampTzADT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsFetchConsistency {
    None,
    Cache,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackFunctionsSetting {
    None,
    Pl,
    All,
}

impl TrackFunctionsSetting {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "pl" => Some(Self::Pl),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    pub(crate) const fn tracks_plpgsql(self) -> bool {
        !matches!(self, Self::None)
    }
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
pub(crate) struct RelationStatsEntry {
    pub numscans: i64,
    pub tuples_returned: i64,
    pub tuples_fetched: i64,
    pub tuples_inserted: i64,
    pub tuples_updated: i64,
    pub tuples_deleted: i64,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub blocks_fetched: i64,
    pub blocks_hit: i64,
    pub lastscan: Option<TimestampTzADT>,
}

impl RelationStatsEntry {
    pub(crate) fn apply_delta(&mut self, delta: &RelationStatsDelta) {
        self.numscans += delta.numscans;
        self.tuples_returned += delta.tuples_returned;
        self.tuples_fetched += delta.tuples_fetched;
        self.tuples_inserted += delta.tuples_inserted;
        self.tuples_updated += delta.tuples_updated;
        self.tuples_deleted += delta.tuples_deleted;
        self.live_tuples += delta.live_tuples;
        self.dead_tuples += delta.dead_tuples;
        self.blocks_fetched += delta.blocks_fetched;
        self.blocks_hit += delta.blocks_hit;
        if let Some(ts) = delta.lastscan {
            self.lastscan = Some(ts);
        }
        self.live_tuples = self.live_tuples.max(0);
        self.dead_tuples = self.dead_tuples.max(0);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RelationStatsDelta {
    pub numscans: i64,
    pub tuples_returned: i64,
    pub tuples_fetched: i64,
    pub tuples_inserted: i64,
    pub tuples_updated: i64,
    pub tuples_deleted: i64,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub blocks_fetched: i64,
    pub blocks_hit: i64,
    pub lastscan: Option<TimestampTzADT>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FunctionStatsEntry {
    pub calls: i64,
    pub total_time_micros: u64,
    pub self_time_micros: u64,
}

impl FunctionStatsEntry {
    pub(crate) fn apply_delta(&mut self, delta: &FunctionStatsDelta) {
        self.calls += delta.calls;
        self.total_time_micros += delta.total_time_micros;
        self.self_time_micros += delta.self_time_micros;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FunctionStatsDelta {
    pub calls: i64,
    pub total_time_micros: u64,
    pub self_time_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct IoStatsKey {
    pub backend_type: String,
    pub object: String,
    pub context: String,
}

impl IoStatsKey {
    pub(crate) fn new(
        backend_type: impl Into<String>,
        object: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self {
            backend_type: backend_type.into(),
            object: object.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IoStatsEntry {
    pub reads: i64,
    pub read_bytes: i64,
    pub read_time_micros: u64,
    pub writes: i64,
    pub write_bytes: i64,
    pub write_time_micros: u64,
    pub writebacks: i64,
    pub writeback_time_micros: u64,
    pub extends: i64,
    pub extend_bytes: i64,
    pub extend_time_micros: u64,
    pub hits: i64,
    pub evictions: i64,
    pub reuses: i64,
    pub fsyncs: i64,
    pub fsync_time_micros: u64,
    pub stats_reset: Option<TimestampTzADT>,
}

impl IoStatsEntry {
    pub(crate) fn apply_delta(&mut self, delta: &IoStatsDelta) {
        self.reads += delta.reads;
        self.read_bytes += delta.read_bytes;
        self.read_time_micros += delta.read_time_micros;
        self.writes += delta.writes;
        self.write_bytes += delta.write_bytes;
        self.write_time_micros += delta.write_time_micros;
        self.writebacks += delta.writebacks;
        self.writeback_time_micros += delta.writeback_time_micros;
        self.extends += delta.extends;
        self.extend_bytes += delta.extend_bytes;
        self.extend_time_micros += delta.extend_time_micros;
        self.hits += delta.hits;
        self.evictions += delta.evictions;
        self.reuses += delta.reuses;
        self.fsyncs += delta.fsyncs;
        self.fsync_time_micros += delta.fsync_time_micros;
        if delta.touched && self.stats_reset.is_none() {
            self.stats_reset = Some(now_timestamptz());
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IoStatsDelta {
    pub reads: i64,
    pub read_bytes: i64,
    pub read_time_micros: u64,
    pub writes: i64,
    pub write_bytes: i64,
    pub write_time_micros: u64,
    pub writebacks: i64,
    pub writeback_time_micros: u64,
    pub extends: i64,
    pub extend_bytes: i64,
    pub extend_time_micros: u64,
    pub hits: i64,
    pub evictions: i64,
    pub reuses: i64,
    pub fsyncs: i64,
    pub fsync_time_micros: u64,
    pub touched: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationTransactionState {
    pub current: RelationStatsDelta,
    pub before_truncate: Option<RelationStatsDelta>,
    pub truncated: bool,
}

impl Default for RelationTransactionState {
    fn default() -> Self {
        Self {
            current: RelationStatsDelta::default(),
            before_truncate: None,
            truncated: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatsMutationEffect {
    DropRelation {
        oid: u32,
        saved_xact: Option<RelationTransactionState>,
    },
    DropFunction {
        oid: u32,
        saved_xact: Option<FunctionStatsDelta>,
    },
}

#[derive(Debug, Clone)]
struct FunctionCallFrame {
    funcid: u32,
    started_at: Instant,
    child_micros: u64,
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
    call_stack: Vec<FunctionCallFrame>,
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

    pub(crate) fn begin_top_level_xact(&mut self) {
        self.xact_active = true;
        self.relation_xact.clear();
        self.function_xact.clear();
        self.stats_effects.clear();
        self.dropped_relations_in_xact.clear();
        self.dropped_functions_in_xact.clear();
        self.call_stack.clear();
    }

    pub(crate) fn commit_top_level_xact(&mut self, db_stats: &Arc<RwLock<DatabaseStatsStore>>) {
        let visible_before_commit = db_stats.read().merged_with_pending(&self.pending_flush);
        let effects = std::mem::take(&mut self.stats_effects);
        for (oid, state) in std::mem::take(&mut self.relation_xact) {
            if self.dropped_relations_in_xact.contains(&oid) {
                continue;
            }
            let relation_delta = if state.truncated {
                let current = state.current;
                let base = visible_before_commit
                    .relations
                    .get(&oid)
                    .cloned()
                    .unwrap_or_default();
                RelationStatsDelta {
                    numscans: current.numscans,
                    tuples_returned: current.tuples_returned,
                    tuples_fetched: current.tuples_fetched,
                    tuples_inserted: current.tuples_inserted,
                    tuples_updated: current.tuples_updated,
                    tuples_deleted: current.tuples_deleted,
                    live_tuples: current.live_tuples - base.live_tuples,
                    dead_tuples: current.dead_tuples - base.dead_tuples,
                    blocks_fetched: current.blocks_fetched,
                    blocks_hit: current.blocks_hit,
                    lastscan: current.lastscan,
                }
            } else {
                state.current
            };
            self.pending_flush
                .relations
                .entry(oid)
                .or_default()
                .apply_assign(&relation_delta);
        }
        for (oid, delta) in std::mem::take(&mut self.function_xact) {
            if self.dropped_functions_in_xact.contains(&oid) {
                continue;
            }
            let entry = self.pending_flush.functions.entry(oid).or_default();
            entry.calls += delta.calls;
            entry.total_time_micros += delta.total_time_micros;
            entry.self_time_micros += delta.self_time_micros;
        }
        self.dropped_relations_in_xact.clear();
        self.dropped_functions_in_xact.clear();
        self.xact_active = false;
        self.call_stack.clear();
        self.clear_snapshot();

        if !effects.is_empty() {
            let mut store = db_stats.write();
            for effect in effects {
                match effect {
                    StatsMutationEffect::DropRelation { oid, .. } => {
                        store.remove_relation(oid);
                        self.pending_flush.relations.remove(&oid);
                    }
                    StatsMutationEffect::DropFunction { oid, .. } => {
                        store.remove_function(oid);
                        self.pending_flush.functions.remove(&oid);
                    }
                }
            }
        }
    }

    pub(crate) fn rollback_top_level_xact(&mut self) {
        self.stats_effects.clear();
        self.relation_xact.clear();
        self.function_xact.clear();
        self.dropped_relations_in_xact.clear();
        self.dropped_functions_in_xact.clear();
        self.xact_active = false;
        self.call_stack.clear();
        self.clear_snapshot();
    }

    pub(crate) fn note_relation_scan(&mut self, oid: u32) {
        let timestamp = now_timestamptz();
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.numscans += 1;
            current.lastscan = Some(timestamp);
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.numscans += 1;
            pending.lastscan = Some(timestamp);
        }
        self.clear_snapshot();
    }

    pub(crate) fn note_relation_tuple_returned(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.tuples_returned += 1);
    }

    pub(crate) fn note_relation_tuple_fetched(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.tuples_fetched += 1);
    }

    pub(crate) fn note_relation_block_fetched(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.blocks_fetched += 1);
    }

    pub(crate) fn note_relation_block_hit(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.blocks_hit += 1);
    }

    pub(crate) fn note_relation_insert(&mut self, oid: u32) {
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.tuples_inserted += 1;
            current.live_tuples += 1;
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.tuples_inserted += 1;
            pending.live_tuples += 1;
        }
        self.clear_snapshot();
    }

    pub(crate) fn note_relation_update(&mut self, oid: u32) {
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.tuples_updated += 1;
            current.dead_tuples += 1;
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.tuples_updated += 1;
            pending.dead_tuples += 1;
        }
        self.clear_snapshot();
    }

    pub(crate) fn note_relation_delete(&mut self, oid: u32) {
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.tuples_deleted += 1;
            current.live_tuples -= 1;
            current.dead_tuples += 1;
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.tuples_deleted += 1;
            pending.live_tuples -= 1;
            pending.dead_tuples += 1;
        }
        self.clear_snapshot();
    }

    pub(crate) fn note_relation_truncate(&mut self, oid: u32) {
        if !self.xact_active {
            return;
        }
        let state = self.relation_xact.entry(oid).or_default();
        state.before_truncate = Some(state.current.clone());
        state.current = RelationStatsDelta::default();
        state.truncated = true;
        self.clear_snapshot();
    }

    pub(crate) fn note_relation_drop(&mut self, oid: u32, db_stats: &Arc<RwLock<DatabaseStatsStore>>) {
        if self.xact_active {
            let saved_xact = self.relation_xact.remove(&oid);
            self.stats_effects.push(StatsMutationEffect::DropRelation { oid, saved_xact });
            self.dropped_relations_in_xact.insert(oid);
        } else {
            self.pending_flush.relations.remove(&oid);
            db_stats.write().remove_relation(oid);
        }
        self.clear_snapshot();
    }

    pub(crate) fn note_function_drop(&mut self, oid: u32, db_stats: &Arc<RwLock<DatabaseStatsStore>>) {
        if self.xact_active {
            let saved_xact = self.function_xact.remove(&oid);
            self.stats_effects.push(StatsMutationEffect::DropFunction { oid, saved_xact });
            self.dropped_functions_in_xact.insert(oid);
        } else {
            self.pending_flush.functions.remove(&oid);
            db_stats.write().remove_function(oid);
        }
        self.clear_snapshot();
    }

    pub(crate) fn note_io_read(
        &mut self,
        backend_type: &str,
        object: &str,
        context: &str,
        bytes: i64,
    ) {
        let delta = self
            .pending_flush
            .io
            .entry(IoStatsKey::new(backend_type, object, context))
            .or_default();
        delta.reads += 1;
        delta.read_bytes += bytes;
        delta.touched = true;
        self.clear_snapshot();
    }

    pub(crate) fn note_io_hit(&mut self, backend_type: &str, object: &str, context: &str) {
        let delta = self
            .pending_flush
            .io
            .entry(IoStatsKey::new(backend_type, object, context))
            .or_default();
        delta.hits += 1;
        delta.touched = true;
        self.clear_snapshot();
    }

    pub(crate) fn note_io_write(
        &mut self,
        backend_type: &str,
        object: &str,
        context: &str,
        bytes: i64,
    ) {
        let delta = self
            .pending_flush
            .io
            .entry(IoStatsKey::new(backend_type, object, context))
            .or_default();
        delta.writes += 1;
        delta.write_bytes += bytes;
        delta.touched = true;
        self.clear_snapshot();
    }

    pub(crate) fn begin_function_call(&mut self, funcid: u32) {
        self.call_stack.push(FunctionCallFrame {
            funcid,
            started_at: Instant::now(),
            child_micros: 0,
        });
    }

    pub(crate) fn finish_function_call(&mut self, funcid: u32) {
        let Some(frame) = self.call_stack.pop() else {
            return;
        };
        if frame.funcid != funcid {
            self.call_stack.clear();
            return;
        }
        let elapsed_micros = frame.started_at.elapsed().as_micros() as u64;
        let self_micros = elapsed_micros.saturating_sub(frame.child_micros);
        if self.xact_active {
            let xact = self.function_xact.entry(funcid).or_default();
            xact.calls += 1;
            xact.total_time_micros += elapsed_micros;
            xact.self_time_micros += self_micros;
        } else {
            let pending = self.pending_flush.functions.entry(funcid).or_default();
            pending.calls += 1;
            pending.total_time_micros += elapsed_micros;
            pending.self_time_micros += self_micros;
        }
        if let Some(parent) = self.call_stack.last_mut() {
            parent.child_micros += elapsed_micros;
        }
        self.clear_snapshot();
    }

    pub(crate) fn visible_stats(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
    ) -> DatabaseStatsStore {
        match self.fetch_consistency {
            StatsFetchConsistency::None => db_stats.read().merged_with_pending(&self.pending_flush),
            StatsFetchConsistency::Cache => {
                if self.cache_snapshot.is_none() {
                    self.cache_snapshot = Some(
                        db_stats
                            .read()
                            .merged_with_pending(&self.pending_flush),
                    );
                }
                self.cache_snapshot.clone().unwrap_or_default()
            }
            StatsFetchConsistency::Snapshot => {
                if self.full_snapshot.is_none() {
                    self.full_snapshot = Some(
                        db_stats
                            .read()
                            .merged_with_pending(&self.pending_flush),
                    );
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
        db_stats.write().apply_pending_flush(&mut self.pending_flush);
        self.clear_snapshot();
    }

    fn note_relation_counter(
        &mut self,
        oid: u32,
        update: impl FnOnce(&mut RelationStatsDelta),
    ) {
        if self.xact_active {
            update(&mut self.relation_xact.entry(oid).or_default().current);
        } else {
            update(self.pending_flush.relations.entry(oid).or_default());
        }
        self.clear_snapshot();
    }
}

impl RelationStatsDelta {
    fn apply_assign(&mut self, other: &RelationStatsDelta) {
        self.numscans += other.numscans;
        self.tuples_returned += other.tuples_returned;
        self.tuples_fetched += other.tuples_fetched;
        self.tuples_inserted += other.tuples_inserted;
        self.tuples_updated += other.tuples_updated;
        self.tuples_deleted += other.tuples_deleted;
        self.live_tuples += other.live_tuples;
        self.dead_tuples += other.dead_tuples;
        self.blocks_fetched += other.blocks_fetched;
        self.blocks_hit += other.blocks_hit;
        if let Some(ts) = other.lastscan {
            self.lastscan = Some(ts);
        }
    }
}

pub(crate) fn default_pg_stat_io_keys() -> Vec<IoStatsKey> {
    let rows = [
        ("autovacuum launcher", "relation", "bulkread"),
        ("autovacuum launcher", "relation", "init"),
        ("autovacuum launcher", "relation", "normal"),
        ("autovacuum launcher", "wal", "init"),
        ("autovacuum launcher", "wal", "normal"),
        ("autovacuum worker", "relation", "bulkread"),
        ("autovacuum worker", "relation", "init"),
        ("autovacuum worker", "relation", "normal"),
        ("autovacuum worker", "relation", "vacuum"),
        ("autovacuum worker", "wal", "init"),
        ("autovacuum worker", "wal", "normal"),
        ("background worker", "relation", "bulkread"),
        ("background worker", "relation", "bulkwrite"),
        ("background worker", "relation", "init"),
        ("background worker", "relation", "normal"),
        ("background worker", "relation", "vacuum"),
        ("background worker", "temp relation", "normal"),
        ("background worker", "wal", "init"),
        ("background worker", "wal", "normal"),
        ("background writer", "relation", "init"),
        ("background writer", "relation", "normal"),
        ("background writer", "wal", "init"),
        ("background writer", "wal", "normal"),
        ("checkpointer", "relation", "init"),
        ("checkpointer", "relation", "normal"),
        ("checkpointer", "wal", "init"),
        ("checkpointer", "wal", "normal"),
        ("client backend", "relation", "bulkread"),
        ("client backend", "relation", "bulkwrite"),
        ("client backend", "relation", "init"),
        ("client backend", "relation", "normal"),
        ("client backend", "relation", "vacuum"),
        ("client backend", "temp relation", "normal"),
        ("client backend", "wal", "init"),
        ("client backend", "wal", "normal"),
        ("io worker", "relation", "bulkread"),
        ("io worker", "relation", "bulkwrite"),
        ("io worker", "relation", "init"),
        ("io worker", "relation", "normal"),
        ("io worker", "relation", "vacuum"),
        ("io worker", "temp relation", "normal"),
        ("io worker", "wal", "init"),
        ("io worker", "wal", "normal"),
        ("slotsync worker", "relation", "bulkread"),
        ("slotsync worker", "relation", "bulkwrite"),
        ("slotsync worker", "relation", "init"),
        ("slotsync worker", "relation", "normal"),
        ("slotsync worker", "relation", "vacuum"),
        ("slotsync worker", "temp relation", "normal"),
        ("slotsync worker", "wal", "init"),
        ("slotsync worker", "wal", "normal"),
        ("standalone backend", "relation", "bulkread"),
        ("standalone backend", "relation", "bulkwrite"),
        ("standalone backend", "relation", "init"),
        ("standalone backend", "relation", "normal"),
        ("standalone backend", "relation", "vacuum"),
        ("standalone backend", "wal", "init"),
        ("standalone backend", "wal", "normal"),
        ("startup", "relation", "bulkread"),
        ("startup", "relation", "bulkwrite"),
        ("startup", "relation", "init"),
        ("startup", "relation", "normal"),
        ("startup", "relation", "vacuum"),
        ("startup", "wal", "init"),
        ("startup", "wal", "normal"),
        ("walreceiver", "wal", "init"),
        ("walreceiver", "wal", "normal"),
        ("walsender", "relation", "bulkread"),
        ("walsender", "relation", "bulkwrite"),
        ("walsender", "relation", "init"),
        ("walsender", "relation", "normal"),
        ("walsender", "relation", "vacuum"),
        ("walsender", "temp relation", "normal"),
        ("walsender", "wal", "init"),
        ("walsender", "wal", "normal"),
        ("walsummarizer", "wal", "init"),
        ("walsummarizer", "wal", "normal"),
        ("walwriter", "wal", "init"),
        ("walwriter", "wal", "normal"),
    ];
    rows.into_iter()
        .map(|(backend_type, object, context)| IoStatsKey::new(backend_type, object, context))
        .collect()
}

pub(crate) fn now_timestamptz() -> TimestampTzADT {
    TimestampTzADT(current_postgres_timestamp_usecs())
}
