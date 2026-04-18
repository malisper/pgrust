use std::sync::Arc;

use parking_lot::RwLock;

use crate::include::nodes::datetime::TimestampTzADT;

use super::pgstat::{DatabaseStatsStore, SessionStatsState, now_timestamptz};
use super::pgstat_xact::StatsMutationEffect;

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

impl RelationStatsDelta {
    pub(crate) fn apply_assign(&mut self, other: &RelationStatsDelta) {
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

impl SessionStatsState {
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

    pub(crate) fn note_relation_drop(
        &mut self,
        oid: u32,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
    ) {
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
}
