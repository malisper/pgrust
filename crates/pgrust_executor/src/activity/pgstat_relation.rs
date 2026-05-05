use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use pgrust_nodes::datetime::TimestampTzADT;

use super::pgstat::{DatabaseStatsStore, SessionStatsState, now_timestamptz};
use super::pgstat_xact::StatsMutationEffect;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationStatsEntry {
    pub numscans: i64,
    pub tuples_returned: i64,
    pub tuples_fetched: i64,
    pub tuples_inserted: i64,
    pub tuples_updated: i64,
    pub tuples_hot_updated: i64,
    pub tuples_deleted: i64,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub mod_since_analyze: i64,
    pub ins_since_vacuum: i64,
    pub blocks_fetched: i64,
    pub blocks_hit: i64,
    pub lastscan: Option<TimestampTzADT>,
    pub last_vacuum: Option<TimestampTzADT>,
    pub last_autovacuum: Option<TimestampTzADT>,
    pub last_analyze: Option<TimestampTzADT>,
    pub last_autoanalyze: Option<TimestampTzADT>,
    pub vacuum_count: i64,
    pub autovacuum_count: i64,
    pub analyze_count: i64,
    pub autoanalyze_count: i64,
    pub total_vacuum_time_micros: i64,
    pub total_autovacuum_time_micros: i64,
    pub total_analyze_time_micros: i64,
    pub total_autoanalyze_time_micros: i64,
}

impl RelationStatsEntry {
    pub fn apply_delta(&mut self, delta: &RelationStatsDelta) {
        self.numscans += delta.numscans;
        self.tuples_returned += delta.tuples_returned;
        self.tuples_fetched += delta.tuples_fetched;
        self.tuples_inserted += delta.tuples_inserted;
        self.tuples_updated += delta.tuples_updated;
        self.tuples_hot_updated += delta.tuples_hot_updated;
        self.tuples_deleted += delta.tuples_deleted;
        self.live_tuples += delta.live_tuples;
        self.dead_tuples += delta.dead_tuples;
        self.mod_since_analyze += delta.mod_since_analyze;
        self.ins_since_vacuum += delta.ins_since_vacuum;
        self.blocks_fetched += delta.blocks_fetched;
        self.blocks_hit += delta.blocks_hit;
        if let Some(ts) = delta.lastscan {
            self.lastscan = Some(ts);
        }
        if let Some(ts) = delta.last_vacuum {
            self.last_vacuum = Some(ts);
        }
        if let Some(ts) = delta.last_autovacuum {
            self.last_autovacuum = Some(ts);
        }
        if let Some(ts) = delta.last_analyze {
            self.last_analyze = Some(ts);
        }
        if let Some(ts) = delta.last_autoanalyze {
            self.last_autoanalyze = Some(ts);
        }
        self.vacuum_count += delta.vacuum_count;
        self.autovacuum_count += delta.autovacuum_count;
        self.analyze_count += delta.analyze_count;
        self.autoanalyze_count += delta.autoanalyze_count;
        self.total_vacuum_time_micros += delta.total_vacuum_time_micros;
        self.total_autovacuum_time_micros += delta.total_autovacuum_time_micros;
        self.total_analyze_time_micros += delta.total_analyze_time_micros;
        self.total_autoanalyze_time_micros += delta.total_autoanalyze_time_micros;
        self.live_tuples = self.live_tuples.max(0);
        self.dead_tuples = self.dead_tuples.max(0);
        self.mod_since_analyze = self.mod_since_analyze.max(0);
        self.ins_since_vacuum = self.ins_since_vacuum.max(0);
    }

    pub fn report_vacuum(
        &mut self,
        auto: bool,
        elapsed: Duration,
        removed_dead_tuples: i64,
        remaining_dead_tuples: i64,
    ) {
        let timestamp = now_timestamptz();
        self.dead_tuples = self
            .dead_tuples
            .saturating_sub(removed_dead_tuples.max(0))
            .max(remaining_dead_tuples.max(0));
        self.ins_since_vacuum = 0;
        let elapsed_micros = elapsed.as_micros().min(i64::MAX as u128) as i64;
        if auto {
            self.last_autovacuum = Some(timestamp);
            self.autovacuum_count += 1;
            self.total_autovacuum_time_micros += elapsed_micros;
        } else {
            self.last_vacuum = Some(timestamp);
            self.vacuum_count += 1;
            self.total_vacuum_time_micros += elapsed_micros;
        }
    }

    pub fn report_analyze(&mut self, auto: bool, elapsed: Duration, reltuples: f64) {
        let timestamp = now_timestamptz();
        self.live_tuples = (reltuples.max(0.0).round() as i64).max(0);
        self.mod_since_analyze = 0;
        let elapsed_micros = elapsed.as_micros().min(i64::MAX as u128) as i64;
        if auto {
            self.last_autoanalyze = Some(timestamp);
            self.autoanalyze_count += 1;
            self.total_autoanalyze_time_micros += elapsed_micros;
        } else {
            self.last_analyze = Some(timestamp);
            self.analyze_count += 1;
            self.total_analyze_time_micros += elapsed_micros;
        }
    }

    fn delta_from(before: &Self, after: &Self) -> RelationStatsDelta {
        RelationStatsDelta {
            numscans: after.numscans - before.numscans,
            tuples_returned: after.tuples_returned - before.tuples_returned,
            tuples_fetched: after.tuples_fetched - before.tuples_fetched,
            tuples_inserted: after.tuples_inserted - before.tuples_inserted,
            tuples_updated: after.tuples_updated - before.tuples_updated,
            tuples_hot_updated: after.tuples_hot_updated - before.tuples_hot_updated,
            tuples_deleted: after.tuples_deleted - before.tuples_deleted,
            live_tuples: after.live_tuples - before.live_tuples,
            dead_tuples: after.dead_tuples - before.dead_tuples,
            mod_since_analyze: after.mod_since_analyze - before.mod_since_analyze,
            ins_since_vacuum: after.ins_since_vacuum - before.ins_since_vacuum,
            blocks_fetched: after.blocks_fetched - before.blocks_fetched,
            blocks_hit: after.blocks_hit - before.blocks_hit,
            lastscan: (after.lastscan != before.lastscan)
                .then_some(after.lastscan)
                .flatten(),
            last_vacuum: (after.last_vacuum != before.last_vacuum)
                .then_some(after.last_vacuum)
                .flatten(),
            last_autovacuum: (after.last_autovacuum != before.last_autovacuum)
                .then_some(after.last_autovacuum)
                .flatten(),
            last_analyze: (after.last_analyze != before.last_analyze)
                .then_some(after.last_analyze)
                .flatten(),
            last_autoanalyze: (after.last_autoanalyze != before.last_autoanalyze)
                .then_some(after.last_autoanalyze)
                .flatten(),
            vacuum_count: after.vacuum_count - before.vacuum_count,
            autovacuum_count: after.autovacuum_count - before.autovacuum_count,
            analyze_count: after.analyze_count - before.analyze_count,
            autoanalyze_count: after.autoanalyze_count - before.autoanalyze_count,
            total_vacuum_time_micros: after.total_vacuum_time_micros
                - before.total_vacuum_time_micros,
            total_autovacuum_time_micros: after.total_autovacuum_time_micros
                - before.total_autovacuum_time_micros,
            total_analyze_time_micros: after.total_analyze_time_micros
                - before.total_analyze_time_micros,
            total_autoanalyze_time_micros: after.total_autoanalyze_time_micros
                - before.total_autoanalyze_time_micros,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationStatsDelta {
    pub numscans: i64,
    pub tuples_returned: i64,
    pub tuples_fetched: i64,
    pub tuples_inserted: i64,
    pub tuples_updated: i64,
    pub tuples_hot_updated: i64,
    pub tuples_deleted: i64,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub mod_since_analyze: i64,
    pub ins_since_vacuum: i64,
    pub blocks_fetched: i64,
    pub blocks_hit: i64,
    pub lastscan: Option<TimestampTzADT>,
    pub last_vacuum: Option<TimestampTzADT>,
    pub last_autovacuum: Option<TimestampTzADT>,
    pub last_analyze: Option<TimestampTzADT>,
    pub last_autoanalyze: Option<TimestampTzADT>,
    pub vacuum_count: i64,
    pub autovacuum_count: i64,
    pub analyze_count: i64,
    pub autoanalyze_count: i64,
    pub total_vacuum_time_micros: i64,
    pub total_autovacuum_time_micros: i64,
    pub total_analyze_time_micros: i64,
    pub total_autoanalyze_time_micros: i64,
}

impl RelationStatsDelta {
    pub fn apply_assign(&mut self, other: &RelationStatsDelta) {
        self.numscans += other.numscans;
        self.tuples_returned += other.tuples_returned;
        self.tuples_fetched += other.tuples_fetched;
        self.tuples_inserted += other.tuples_inserted;
        self.tuples_updated += other.tuples_updated;
        self.tuples_hot_updated += other.tuples_hot_updated;
        self.tuples_deleted += other.tuples_deleted;
        self.live_tuples += other.live_tuples;
        self.dead_tuples += other.dead_tuples;
        self.mod_since_analyze += other.mod_since_analyze;
        self.ins_since_vacuum += other.ins_since_vacuum;
        self.blocks_fetched += other.blocks_fetched;
        self.blocks_hit += other.blocks_hit;
        if let Some(ts) = other.lastscan {
            self.lastscan = Some(ts);
        }
        if let Some(ts) = other.last_vacuum {
            self.last_vacuum = Some(ts);
        }
        if let Some(ts) = other.last_autovacuum {
            self.last_autovacuum = Some(ts);
        }
        if let Some(ts) = other.last_analyze {
            self.last_analyze = Some(ts);
        }
        if let Some(ts) = other.last_autoanalyze {
            self.last_autoanalyze = Some(ts);
        }
        self.vacuum_count += other.vacuum_count;
        self.autovacuum_count += other.autovacuum_count;
        self.analyze_count += other.analyze_count;
        self.autoanalyze_count += other.autoanalyze_count;
        self.total_vacuum_time_micros += other.total_vacuum_time_micros;
        self.total_autovacuum_time_micros += other.total_autovacuum_time_micros;
        self.total_analyze_time_micros += other.total_analyze_time_micros;
        self.total_autoanalyze_time_micros += other.total_autoanalyze_time_micros;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationTransactionState {
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

impl SessionStatsState {
    pub fn note_relation_scan(&mut self, oid: u32) {
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

    pub fn note_relation_tuple_returned(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.tuples_returned += 1);
    }

    pub fn note_relation_tuple_fetched(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.tuples_fetched += 1);
    }

    pub fn note_relation_block_fetched(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.blocks_fetched += 1);
    }

    pub fn note_relation_block_hit(&mut self, oid: u32) {
        self.note_relation_counter(oid, |delta| delta.blocks_hit += 1);
    }

    pub fn note_relation_insert(&mut self, oid: u32) {
        self.note_relation_insert_with_persistence(oid, 'p');
    }

    pub fn note_relation_insert_with_persistence(&mut self, oid: u32, relpersistence: char) {
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.tuples_inserted += 1;
            current.live_tuples += 1;
            current.mod_since_analyze += 1;
            current.ins_since_vacuum += 1;
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.tuples_inserted += 1;
            pending.live_tuples += 1;
            pending.mod_since_analyze += 1;
            pending.ins_since_vacuum += 1;
        }
        if relpersistence == 't' {
            self.note_io_extend("client backend", "temp relation", "normal", 8192);
            self.note_io_write("client backend", "temp relation", "normal", 8192);
            self.note_io_eviction("client backend", "temp relation", "normal");
        } else {
            self.note_io_extend("client backend", "relation", "normal", 8192);
            self.note_io_write("client backend", "wal", "normal", 8192);
        }
        self.clear_snapshot();
    }

    pub fn note_relation_update(&mut self, oid: u32) {
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.tuples_updated += 1;
            current.tuples_hot_updated += 1;
            current.dead_tuples += 1;
            current.mod_since_analyze += 1;
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.tuples_updated += 1;
            pending.tuples_hot_updated += 1;
            pending.dead_tuples += 1;
            pending.mod_since_analyze += 1;
        }
        self.note_io_write("client backend", "relation", "normal", 8192);
        self.note_io_write("client backend", "wal", "normal", 8192);
        self.clear_snapshot();
    }

    pub fn note_relation_delete(&mut self, oid: u32) {
        if self.xact_active {
            let current = &mut self.relation_xact.entry(oid).or_default().current;
            current.tuples_deleted += 1;
            current.live_tuples -= 1;
            current.dead_tuples += 1;
            current.mod_since_analyze += 1;
        } else {
            let pending = self.pending_flush.relations.entry(oid).or_default();
            pending.tuples_deleted += 1;
            pending.live_tuples -= 1;
            pending.dead_tuples += 1;
            pending.mod_since_analyze += 1;
        }
        self.note_io_write("client backend", "relation", "normal", 8192);
        self.note_io_write("client backend", "wal", "normal", 8192);
        self.clear_snapshot();
    }

    pub fn note_relation_truncate(&mut self, oid: u32) {
        if !self.xact_active {
            return;
        }
        let state = self.relation_xact.entry(oid).or_default();
        state.before_truncate = Some(state.current.clone());
        state.current = RelationStatsDelta::default();
        state.truncated = true;
        self.clear_snapshot();
    }

    pub fn note_relation_drop(&mut self, oid: u32, db_stats: &Arc<RwLock<DatabaseStatsStore>>) {
        if self.xact_active {
            let saved_xact = self.relation_xact.remove(&oid);
            self.stats_effects
                .push(StatsMutationEffect::DropRelation { oid, saved_xact });
            self.dropped_relations_in_xact.insert(oid);
        } else {
            self.pending_flush.relations.remove(&oid);
            db_stats.write().remove_relation(oid);
        }
        self.clear_snapshot();
    }

    fn note_relation_counter(&mut self, oid: u32, update: impl FnOnce(&mut RelationStatsDelta)) {
        if self.xact_active {
            update(&mut self.relation_xact.entry(oid).or_default().current);
        } else {
            update(self.pending_flush.relations.entry(oid).or_default());
        }
        self.clear_snapshot();
    }

    pub fn report_relation_analyze(
        &mut self,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
        oid: u32,
        auto: bool,
        elapsed: Duration,
        reltuples: f64,
    ) {
        if self.xact_active {
            let mut before = db_stats
                .read()
                .merged_relation_entry(&self.pending_flush, oid)
                .unwrap_or_default();
            if let Some(state) = self.relation_xact.get(&oid) {
                before.apply_delta(&state.current);
            }
            let mut after = before.clone();
            after.report_analyze(auto, elapsed, reltuples);
            let delta = RelationStatsEntry::delta_from(&before, &after);
            self.relation_xact
                .entry(oid)
                .or_default()
                .current
                .apply_assign(&delta);
        } else {
            db_stats
                .write()
                .report_relation_analyze(oid, auto, elapsed, reltuples);
        }
        self.clear_snapshot();
    }
}
