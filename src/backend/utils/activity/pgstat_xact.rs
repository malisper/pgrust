use std::sync::Arc;

use parking_lot::RwLock;

use super::pgstat::{DatabaseStatsStore, SessionStatsState};
use super::pgstat_function::FunctionStatsDelta;
use super::pgstat_relation::{RelationStatsDelta, RelationTransactionState};

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

impl SessionStatsState {
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
        let effects = std::mem::take(&mut self.stats_effects);
        for (oid, state) in std::mem::take(&mut self.relation_xact) {
            if self.dropped_relations_in_xact.contains(&oid) {
                continue;
            }
            let relation_delta = if state.truncated {
                let current = state.current;
                let base = db_stats
                    .read()
                    .merged_relation_entry(&self.pending_flush, oid)
                    .unwrap_or_default();
                RelationStatsDelta {
                    numscans: current.numscans,
                    tuples_returned: current.tuples_returned,
                    tuples_fetched: current.tuples_fetched,
                    tuples_inserted: current.tuples_inserted,
                    tuples_updated: current.tuples_updated,
                    tuples_hot_updated: current.tuples_hot_updated,
                    tuples_deleted: current.tuples_deleted,
                    live_tuples: current.live_tuples - base.live_tuples,
                    dead_tuples: current.dead_tuples - base.dead_tuples,
                    mod_since_analyze: current.mod_since_analyze,
                    ins_since_vacuum: current.ins_since_vacuum,
                    blocks_fetched: current.blocks_fetched,
                    blocks_hit: current.blocks_hit,
                    lastscan: current.lastscan,
                    last_vacuum: current.last_vacuum,
                    last_autovacuum: current.last_autovacuum,
                    last_analyze: current.last_analyze,
                    last_autoanalyze: current.last_autoanalyze,
                    vacuum_count: current.vacuum_count,
                    autovacuum_count: current.autovacuum_count,
                    analyze_count: current.analyze_count,
                    autoanalyze_count: current.autoanalyze_count,
                    total_vacuum_time_micros: current.total_vacuum_time_micros,
                    total_autovacuum_time_micros: current.total_autovacuum_time_micros,
                    total_analyze_time_micros: current.total_analyze_time_micros,
                    total_autoanalyze_time_micros: current.total_autoanalyze_time_micros,
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
            self.force_clear_snapshot();
        }
    }

    pub(crate) fn rollback_top_level_xact(&mut self) {
        let effects = std::mem::take(&mut self.stats_effects);
        for (oid, state) in std::mem::take(&mut self.relation_xact) {
            let delta = aborted_relation_delta(&state, None);
            if relation_delta_has_values(&delta) {
                self.pending_flush
                    .relations
                    .entry(oid)
                    .or_default()
                    .apply_assign(&delta);
            }
        }
        for effect in effects {
            if let StatsMutationEffect::DropRelation {
                oid,
                saved_xact: Some(state),
            } = effect
            {
                let delta = aborted_relation_delta(&state, None);
                if relation_delta_has_values(&delta) {
                    self.pending_flush
                        .relations
                        .entry(oid)
                        .or_default()
                        .apply_assign(&delta);
                }
            }
        }
        for (oid, delta) in std::mem::take(&mut self.function_xact) {
            let entry = self.pending_flush.functions.entry(oid).or_default();
            entry.calls += delta.calls;
            entry.total_time_micros += delta.total_time_micros;
            entry.self_time_micros += delta.self_time_micros;
        }
        self.relation_xact.clear();
        self.dropped_relations_in_xact.clear();
        self.dropped_functions_in_xact.clear();
        self.xact_active = false;
        self.call_stack.clear();
        self.clear_snapshot();
    }

    pub(crate) fn restore_after_savepoint_rollback(&mut self, mut saved: SessionStatsState) {
        let current = self.clone();
        for (oid, state) in &current.relation_xact {
            let before = saved.relation_xact.get(oid);
            let delta = aborted_relation_delta(state, before);
            if relation_delta_has_values(&delta) {
                saved
                    .relation_xact
                    .entry(*oid)
                    .or_default()
                    .current
                    .apply_assign(&delta);
            }
        }
        for effect in &current.stats_effects {
            if let StatsMutationEffect::DropRelation {
                oid,
                saved_xact: Some(state),
            } = effect
            {
                let before = saved.relation_xact.get(oid);
                let delta = aborted_relation_delta(state, before);
                if relation_delta_has_values(&delta) {
                    saved
                        .relation_xact
                        .entry(*oid)
                        .or_default()
                        .current
                        .apply_assign(&delta);
                }
            }
        }

        // Function and IO stats are not transactional in PostgreSQL. Rolling
        // back a savepoint must undo stats drop effects, but calls and IO that
        // already happened remain visible to xact/backend counters.
        saved.function_xact = current.function_xact;
        saved.pending_flush.io = current.pending_flush.io;
        saved.backend_io = current.backend_io;
        saved.relation_have_stats_false_once = current.relation_have_stats_false_once;
        saved.clear_snapshot();
        *self = saved;
    }
}

fn relation_delta_has_values(delta: &RelationStatsDelta) -> bool {
    delta.numscans != 0
        || delta.tuples_returned != 0
        || delta.tuples_fetched != 0
        || delta.tuples_inserted != 0
        || delta.tuples_updated != 0
        || delta.tuples_hot_updated != 0
        || delta.tuples_deleted != 0
        || delta.live_tuples != 0
        || delta.dead_tuples != 0
        || delta.mod_since_analyze != 0
        || delta.ins_since_vacuum != 0
        || delta.blocks_fetched != 0
        || delta.blocks_hit != 0
        || delta.lastscan.is_some()
        || delta.last_vacuum.is_some()
        || delta.last_autovacuum.is_some()
        || delta.last_analyze.is_some()
        || delta.last_autoanalyze.is_some()
        || delta.vacuum_count != 0
        || delta.autovacuum_count != 0
        || delta.analyze_count != 0
        || delta.autoanalyze_count != 0
        || delta.total_vacuum_time_micros != 0
        || delta.total_autovacuum_time_micros != 0
        || delta.total_analyze_time_micros != 0
        || delta.total_autoanalyze_time_micros != 0
}

fn aborted_relation_delta(
    state: &RelationTransactionState,
    before: Option<&RelationTransactionState>,
) -> RelationStatsDelta {
    let base = before.map(|state| &state.current);
    let source = state.before_truncate.as_ref().unwrap_or(&state.current);
    let mut delta = RelationStatsDelta {
        numscans: source.numscans - base.map(|d| d.numscans).unwrap_or_default(),
        tuples_returned: source.tuples_returned
            - base.map(|d| d.tuples_returned).unwrap_or_default(),
        tuples_fetched: source.tuples_fetched - base.map(|d| d.tuples_fetched).unwrap_or_default(),
        tuples_inserted: source.tuples_inserted
            - base.map(|d| d.tuples_inserted).unwrap_or_default(),
        tuples_updated: source.tuples_updated - base.map(|d| d.tuples_updated).unwrap_or_default(),
        tuples_hot_updated: source.tuples_hot_updated
            - base.map(|d| d.tuples_hot_updated).unwrap_or_default(),
        tuples_deleted: source.tuples_deleted - base.map(|d| d.tuples_deleted).unwrap_or_default(),
        live_tuples: 0,
        dead_tuples: 0,
        mod_since_analyze: source.mod_since_analyze
            - base.map(|d| d.mod_since_analyze).unwrap_or_default(),
        ins_since_vacuum: source.ins_since_vacuum
            - base.map(|d| d.ins_since_vacuum).unwrap_or_default(),
        blocks_fetched: source.blocks_fetched - base.map(|d| d.blocks_fetched).unwrap_or_default(),
        blocks_hit: source.blocks_hit - base.map(|d| d.blocks_hit).unwrap_or_default(),
        lastscan: source.lastscan,
        last_vacuum: source.last_vacuum,
        last_autovacuum: source.last_autovacuum,
        last_analyze: source.last_analyze,
        last_autoanalyze: source.last_autoanalyze,
        vacuum_count: source.vacuum_count - base.map(|d| d.vacuum_count).unwrap_or_default(),
        autovacuum_count: source.autovacuum_count
            - base.map(|d| d.autovacuum_count).unwrap_or_default(),
        analyze_count: source.analyze_count - base.map(|d| d.analyze_count).unwrap_or_default(),
        autoanalyze_count: source.autoanalyze_count
            - base.map(|d| d.autoanalyze_count).unwrap_or_default(),
        total_vacuum_time_micros: source.total_vacuum_time_micros
            - base.map(|d| d.total_vacuum_time_micros).unwrap_or_default(),
        total_autovacuum_time_micros: source.total_autovacuum_time_micros
            - base
                .map(|d| d.total_autovacuum_time_micros)
                .unwrap_or_default(),
        total_analyze_time_micros: source.total_analyze_time_micros
            - base
                .map(|d| d.total_analyze_time_micros)
                .unwrap_or_default(),
        total_autoanalyze_time_micros: source.total_autoanalyze_time_micros
            - base
                .map(|d| d.total_autoanalyze_time_micros)
                .unwrap_or_default(),
    };
    let live_delta = source.live_tuples - base.map(|d| d.live_tuples).unwrap_or_default();
    delta.dead_tuples = live_delta.max(0);
    delta
}
