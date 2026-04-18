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
}
