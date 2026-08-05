// pgstat_xact.c — pgStatXactStack (one PgStat_SubXactStatus per live nesting
// level, deepest last) and the commit/abort/2PC schedule of transactional
// stats drops. C allocates nodes in TopTransactionContext; the stack owns
// them here and the same teardown points free them. Shared-entry drop and
// entry-ref GC (pgstat_shmem.c) are phase 2: the local half removes the
// backend's pending entry.

use mcx::{Mcx, PgVec};
use types_core::xact::XlXactStatsItem;
use types_core::Oid;
use types_error::PgResult;

use crate::database;
use crate::pending::{self, PgStatState, PgStat_HashKey, PgStat_Kind};
use crate::relation;

pub struct PgStat_PendingDroppedStatsItem {
    pub item: XlXactStatsItem,
    pub is_create: bool,
}

pub struct PgStat_SubXactStatus {
    pub nest_level: i32,
    pub first: PgVec<'static, PgStat_HashKey>,
    pub pending_drops: PgVec<'static, PgStat_PendingDroppedStatsItem>,
}

pub(crate) fn pgstat_get_xact_stack_level_mut(
    st: &mut PgStatState,
    nest_level: i32,
) -> &mut PgStat_SubXactStatus {
    // C checks only the current top node.
    let need = st
        .xact_stack
        .last()
        .is_none_or(|top| top.nest_level != nest_level);
    if need {
        let mcx = st.ctx.mcx();
        st.xact_stack.push(PgStat_SubXactStatus {
            nest_level,
            first: PgVec::new_in(mcx),
            pending_drops: PgVec::new_in(mcx),
        });
    }
    st.xact_stack.last_mut().unwrap()
}

pub fn AtEOXact_PgStat(isCommit: bool, parallel: bool) {
    database::AtEOXact_PgStat_Database(isCommit, parallel);

    pending::with_state(|st| {
        if let Some(top) = st.xact_stack.pop() {
            debug_assert_eq!(top.nest_level, 1);
            debug_assert!(st.xact_stack.is_empty());
            relation::AtEOXact_PgStat_Relations(st, &top, isCommit);
            AtEOXact_PgStat_DroppedStats(st, &top, isCommit);
        }
        st.xact_stack.clear();
    });

    pending::pgstat_clear_snapshot();
}

fn AtEOXact_PgStat_DroppedStats(
    st: &mut PgStatState,
    xact_state: &PgStat_SubXactStatus,
    isCommit: bool,
) {
    for pending_drop in &xact_state.pending_drops {
        if isCommit != pending_drop.is_create {
            // commit drops stats of dropped objects; abort drops stats of
            // created objects
            drop_entry_local(st, &pending_drop.item);
        }
    }
}

pub fn AtEOSubXact_PgStat(isCommit: bool, nestDepth: i32) {
    pending::with_state(|st| {
        let pop = st
            .xact_stack
            .last()
            .is_some_and(|top| top.nest_level >= nestDepth);
        if pop {
            let mut xact_state = st.xact_stack.pop().unwrap();
            relation::AtEOSubXact_PgStat_Relations(st, &xact_state, isCommit, nestDepth);
            AtEOSubXact_PgStat_DroppedStats(st, &mut xact_state, isCommit, nestDepth);
        }
    });
}

fn AtEOSubXact_PgStat_DroppedStats(
    st: &mut PgStatState,
    xact_state: &mut PgStat_SubXactStatus,
    isCommit: bool,
    nestDepth: i32,
) {
    if xact_state.pending_drops.is_empty() {
        return;
    }
    pgstat_get_xact_stack_level_mut(st, nestDepth - 1);
    while let Some(pending_drop) = xact_state.pending_drops.pop() {
        if !isCommit && pending_drop.is_create {
            drop_entry_local(st, &pending_drop.item);
        } else if isCommit {
            // a committed subxact drop must survive to the top-level outcome
            pgstat_get_xact_stack_level_mut(st, nestDepth - 1)
                .pending_drops
                .push(pending_drop);
        }
    }
}

pub fn AtPrepare_PgStat() -> PgResult<()> {
    pending::with_state(|st| {
        let Some(top) = st.xact_stack.pop() else {
            return Ok(());
        };
        debug_assert_eq!(top.nest_level, 1);
        let r = relation::AtPrepare_PgStat_Relations(st, &top);
        st.xact_stack.push(top);
        r
    })
}

pub fn PostPrepare_PgStat() {
    pending::with_state(|st| {
        if let Some(top) = st.xact_stack.pop() {
            debug_assert_eq!(top.nest_level, 1);
            debug_assert!(st.xact_stack.is_empty());
            relation::PostPrepare_PgStat_Relations(st, &top);
        }
        st.xact_stack.clear();
    });
    pending::pgstat_clear_snapshot();
}

pub fn pgstat_get_transactional_drops<'mcx>(
    mcx: Mcx<'mcx>,
    isCommit: bool,
) -> PgResult<PgVec<'mcx, XlXactStatsItem>> {
    pending::with_state(|st| {
        let Some(xact_state) = st.xact_stack.last() else {
            return Ok(PgVec::new_in(mcx));
        };
        // called for subxact abort (which logs WAL), never subxact commit
        debug_assert!(!isCommit || xact_state.nest_level == 1);

        let mut items = PgVec::with_capacity_in(xact_state.pending_drops.len(), mcx);
        for pending_drop in &xact_state.pending_drops {
            if isCommit == pending_drop.is_create {
                continue;
            }
            items.push(pending_drop.item);
        }
        Ok(items)
    })
}

pub fn pgstat_execute_transactional_drops(
    items: &[XlXactStatsItem],
    _is_redo: bool,
) -> PgResult<()> {
    pending::with_state(|st| {
        for it in items {
            drop_entry_local(st, it);
        }
    });
    Ok(())
}

fn drop_entry_local(st: &mut PgStatState, item: &XlXactStatsItem) {
    let key = PgStat_HashKey {
        kind: PgStat_Kind(item.kind as u32),
        dboid: item.dboid,
        objid: item.objid,
    };
    st.delete_pending_entry(key);
    crate::shmem::drop_entry(key);
}

fn create_drop_transactional_internal(kind: PgStat_Kind, dboid: Oid, objid: u64, is_create: bool) {
    let nest_level = xact_seams::get_current_transaction_nest_level::call();
    pending::with_state(|st| {
        pgstat_get_xact_stack_level_mut(st, nest_level)
            .pending_drops
            .push(PgStat_PendingDroppedStatsItem {
                is_create,
                item: XlXactStatsItem {
                    kind: kind.0 as i32,
                    dboid,
                    objid,
                },
            });
    });
}

pub fn pgstat_create_transactional(kind: PgStat_Kind, dboid: Oid, objid: u64) {
    // C warns + resets when a shared entry already exists: phase 2 (needs the
    // shared hash).
    create_drop_transactional_internal(kind, dboid, objid, true);
}

pub fn pgstat_drop_transactional(kind: PgStat_Kind, dboid: Oid, objid: u64) {
    create_drop_transactional_internal(kind, dboid, objid, false);
}
