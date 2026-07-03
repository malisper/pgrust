// The immediate after-trigger queue: AfterTriggerSaveEvent /
// AfterTriggerBeginQuery / AfterTriggerEndQuery / afterTriggerInvokeEvents /
// AfterTriggerExecute (trigger.c), RI lane. Deferrable triggers and
// transition tables are loud; events re-fetch tuples by ctid under
// SnapshotAny as C's table_tuple_fetch_row_version does (slot machinery
// replaced by direct pinned-page reads).
use std::cell::RefCell;

use mcx::Mcx;
use ri_triggers_seams::RiTriggerData;
use types_core::{CommandId, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{NoLock, Relation};
use types_snapshot::{SnapshotData, SNAPSHOT_ANY};
use types_trigger::{
    Trigger, TriggerDesc, RI_TRIGGER_FK, RI_TRIGGER_NONE, RI_TRIGGER_PK, TRIGGER_DISABLED,
    TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSERT, TRIGGER_EVENT_OPMASK, TRIGGER_EVENT_ROW,
    TRIGGER_EVENT_UPDATE, TRIGGER_FIRES_ON_REPLICA, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE,
    TRIGGER_TYPE_INSERT, TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_ROW, TRIGGER_TYPE_TIMING_MASK,
    TRIGGER_TYPE_UPDATE,
};
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::{FIRING_COUNTER, QUERY_DEPTH};

const F_RI_FKEY_CHECK_INS: Oid = 1644;
const F_RI_FKEY_CHECK_UPD: Oid = 1645;
const F_RI_FKEY_CASCADE_DEL: Oid = 1646;
const F_RI_FKEY_SETDEFAULT_UPD: Oid = 1653;
const F_RI_FKEY_NOACTION_DEL: Oid = 1654;
const F_RI_FKEY_NOACTION_UPD: Oid = 1655;

const AFTER_TRIGGER_DONE: u32 = 0x1000_0000;
const AFTER_TRIGGER_IN_PROGRESS: u32 = 0x2000_0000;

struct AfterTriggerEvent {
    flags: u32,
    ctid1: ItemPointerData,
    ctid2: ItemPointerData,
    event: u32,
    tgoid: Oid,
    relid: Oid,
    firing_id: CommandId,
}

thread_local! {
    static QUERY_STACK: RefCell<Vec<Vec<AfterTriggerEvent>>> =
        const { RefCell::new(Vec::new()) };
}

fn ri_trigger_kind(tgfoid: Oid) -> i32 {
    match tgfoid {
        F_RI_FKEY_CASCADE_DEL..=F_RI_FKEY_SETDEFAULT_UPD
        | F_RI_FKEY_NOACTION_DEL
        | F_RI_FKEY_NOACTION_UPD => RI_TRIGGER_PK,
        F_RI_FKEY_CHECK_INS | F_RI_FKEY_CHECK_UPD => RI_TRIGGER_FK,
        _ => RI_TRIGGER_NONE,
    }
}

pub fn AfterTriggerBeginQuery() {
    QUERY_DEPTH.with(|c| c.set(c.get() + 1));
}

pub(crate) fn query_stack_nonempty() -> bool {
    QUERY_STACK.with(|s| s.borrow().iter().any(|q| !q.is_empty()))
}

pub(crate) fn query_stack_clear() {
    QUERY_STACK.with(|s| s.borrow_mut().clear());
}

// Owns its scratch context (C's AfterTriggerTupleContext): the caller must
// not hold executor registry borrows across the firing loop (RI checks
// re-enter the executor through SPI).
pub fn AfterTriggerEndQuery() -> PgResult<()> {
    let depth = QUERY_DEPTH.with(|c| c.get());
    debug_assert!(depth >= 0, "AfterTriggerEndQuery outside a query");
    let d = depth as usize;
    if QUERY_STACK.with(|s| s.borrow().len()) <= d {
        QUERY_DEPTH.with(|c| c.set(depth - 1));
        return Ok(());
    }
    let scratch = ::mcx::MemoryContext::new("AfterTriggerTupleContext");
    let mcx = scratch.mcx();
    loop {
        // afterTriggerMarkEvents: stamp unscheduled events for this cycle.
        let firing_id = FIRING_COUNTER.with(|c| c.get());
        let found = QUERY_STACK.with(|s| {
            let mut st = s.borrow_mut();
            let mut found = false;
            for ev in &mut st[d] {
                if ev.flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS) == 0 {
                    ev.firing_id = firing_id;
                    ev.flags |= AFTER_TRIGGER_IN_PROGRESS;
                    found = true;
                }
            }
            found
        });
        if !found {
            break;
        }
        FIRING_COUNTER.with(|c| c.set(firing_id + 1));
        afterTriggerInvokeEvents(mcx, d, firing_id)?;
    }
    QUERY_STACK.with(|s| {
        let mut st = s.borrow_mut();
        st[d].clear();
        st.truncate(d);
    });
    QUERY_DEPTH.with(|c| c.set(depth - 1));
    Ok(())
}

fn afterTriggerInvokeEvents<'mcx>(mcx: Mcx<'mcx>, d: usize, firing_id: CommandId) -> PgResult<()> {
    let mut i = 0;
    loop {
        // Borrow per event: firing re-enters the queue (RI SPI queries).
        let next = QUERY_STACK.with(|s| {
            let st = s.borrow();
            let evs = &st[d];
            while i < evs.len() {
                let ev = &evs[i];
                if ev.flags & AFTER_TRIGGER_IN_PROGRESS != 0 && ev.firing_id == firing_id {
                    return Some((
                        ev.ctid1,
                        ev.ctid2,
                        ev.event,
                        ev.tgoid,
                        ev.relid,
                    ));
                }
                i += 1;
            }
            None
        });
        let Some((ctid1, ctid2, event, tgoid, relid)) = next else {
            return Ok(());
        };
        AfterTriggerExecute(mcx, ctid1, ctid2, event, tgoid, relid)?;
        QUERY_STACK.with(|s| {
            let mut st = s.borrow_mut();
            let ev = &mut st[d][i];
            ev.flags &= !AFTER_TRIGGER_IN_PROGRESS;
            ev.flags |= AFTER_TRIGGER_DONE;
        });
        i += 1;
    }
}

fn AfterTriggerExecute<'mcx>(
    mcx: Mcx<'mcx>,
    ctid1: ItemPointerData,
    ctid2: ItemPointerData,
    event: u32,
    tgoid: Oid,
    relid: Oid,
) -> PgResult<()> {
    let Some(trigdesc) = relcache::RelationGetTriggerDesc(relid)? else {
        return Ok(());
    };
    let Some(trigger) = trigdesc.triggers.iter().find(|t| t.tgoid == tgoid) else {
        return Ok(());
    };
    let rel = table::table_open(mcx, relid, NoLock)?;

    let snap = SnapshotData::sentinel(mcx, SNAPSHOT_ANY);
    let r1 = heapam::heap_fetch(&rel, &snap, ctid1, false)?;
    if !r1.found {
        return Err(fetch_failed(1));
    }
    let t1 = r1.tuple().expect("found fetch has a tuple");
    let is_update = event & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_UPDATE;
    let r2;
    let t2 = if is_update {
        r2 = heapam::heap_fetch(&rel, &snap, ctid2, false)?;
        if !r2.found {
            return Err(fetch_failed(2));
        }
        Some(r2.tuple().expect("found fetch has a tuple"))
    } else {
        None
    };

    if ri_trigger_kind(trigger.tgfoid) == RI_TRIGGER_NONE {
        panic!(
            "AfterTriggerExecute (trigger.c): non-RI trigger function {} unported \
             (fmgr trigger-call lane)",
            trigger.tgfoid
        );
    }
    let data = RiTriggerData {
        tg_event: event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW),
        tg_relation: &rel,
        tg_trigtuple: &t1,
        tg_newtuple: t2.as_ref(),
        tg_trigger: trigger,
    };
    let result = ri_triggers_seams::ri_fkey_trigger::call(mcx, trigger.tgfoid, &data);
    drop(data);
    rel.close(NoLock)?;
    result
}

fn trigger_enabled(t: &Trigger<'_>) -> bool {
    // SESSION_REPLICATION_ROLE_ORIGIN (the only ported role).
    if t.tgenabled == TRIGGER_DISABLED || t.tgenabled == TRIGGER_FIRES_ON_REPLICA {
        return false;
    }
    if t.tgqual.is_some() || t.tgnattr > 0 {
        panic!("TriggerEnabled (trigger.c): WHEN clause / UPDATE OF columns unported");
    }
    true
}

fn trigger_type_matches(tgtype: i16, event: i16) -> bool {
    tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | event)
        == TRIGGER_TYPE_ROW | TRIGGER_TYPE_AFTER | event
}

#[allow(clippy::too_many_arguments)]
fn after_trigger_save_event<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    event: u32,
    tgtype_event: i16,
    ctid1: ItemPointerData,
    ctid2: ItemPointerData,
    old_tup: Option<&HeapTupleData<'_>>,
    new_tup: Option<&HeapTupleData<'_>>,
) -> PgResult<()> {
    let depth = QUERY_DEPTH.with(|c| c.get());
    if depth < 0 {
        return Err(outside_query());
    }
    let d = depth as usize;
    for trigger in trigdesc.triggers.iter() {
        if !trigger_type_matches(trigger.tgtype, tgtype_event) {
            continue;
        }
        if !trigger_enabled(trigger) {
            continue;
        }
        if trigger.tgdeferrable || trigger.tginitdeferred {
            panic!(
                "AfterTriggerSaveEvent (trigger.c): deferrable constraint \
                 trigger {} unported",
                trigger.tgname.as_str()
            );
        }
        let is_update = event == TRIGGER_EVENT_UPDATE;
        let is_delete = event == TRIGGER_EVENT_DELETE;
        if is_update || is_delete {
            match ri_trigger_kind(trigger.tgfoid) {
                RI_TRIGGER_PK => {
                    // C also skips DELETEs whose old key contains a NULL
                    // (RI_FKey_pk_upd_check_required with newslot NULL);
                    // divergence: those queue and no-op inside ri_restrict.
                    if is_update
                        && !ri_triggers_seams::ri_fkey_pk_upd_check_required::call(
                            mcx,
                            trigger,
                            rel,
                            old_tup.expect("UPDATE old tuple"),
                            new_tup.expect("UPDATE new tuple"),
                        )?
                    {
                        continue;
                    }
                }
                RI_TRIGGER_FK => {
                    if is_update
                        && !ri_triggers_seams::ri_fkey_fk_upd_check_required::call(
                            mcx,
                            trigger,
                            rel,
                            old_tup.expect("UPDATE old tuple"),
                            new_tup.expect("UPDATE new tuple"),
                        )?
                    {
                        continue;
                    }
                }
                _ => {}
            }
        }
        QUERY_STACK.with(|s| {
            let mut st = s.borrow_mut();
            while st.len() <= d {
                st.push(Vec::new());
            }
            st[d].push(AfterTriggerEvent {
                flags: 0,
                ctid1,
                ctid2,
                event: event | TRIGGER_EVENT_ROW,
                tgoid: trigger.tgoid,
                relid: rel.rd_id,
                firing_id: 0,
            });
        });
    }
    Ok(())
}

pub fn ExecARInsertTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    new_tid: ItemPointerData,
) -> PgResult<()> {
    if !trigdesc.trig_insert_after_row {
        return Ok(());
    }
    after_trigger_save_event(
        mcx,
        rel,
        trigdesc,
        TRIGGER_EVENT_INSERT,
        TRIGGER_TYPE_INSERT,
        new_tid,
        ItemPointerData::default(),
        None,
        None,
    )
}

pub fn ExecARDeleteTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    old_tid: ItemPointerData,
) -> PgResult<()> {
    if !trigdesc.trig_delete_after_row {
        return Ok(());
    }
    after_trigger_save_event(
        mcx,
        rel,
        trigdesc,
        TRIGGER_EVENT_DELETE,
        TRIGGER_TYPE_DELETE,
        old_tid,
        ItemPointerData::default(),
        None,
        None,
    )
}

pub fn ExecARUpdateTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    old_tid: ItemPointerData,
    new_tid: ItemPointerData,
) -> PgResult<()> {
    if !trigdesc.trig_update_after_row {
        return Ok(());
    }
    // GetTupleForTrigger's fetch, for the RI-skip inspections.
    let snap = SnapshotData::sentinel(mcx, SNAPSHOT_ANY);
    let r_old = heapam::heap_fetch(rel, &snap, old_tid, false)?;
    if !r_old.found {
        return Err(fetch_failed(1));
    }
    let r_new = heapam::heap_fetch(rel, &snap, new_tid, false)?;
    if !r_new.found {
        return Err(fetch_failed(2));
    }
    let old_t = r_old.tuple().expect("found fetch has a tuple");
    let new_t = r_new.tuple().expect("found fetch has a tuple");
    after_trigger_save_event(
        mcx,
        rel,
        trigdesc,
        TRIGGER_EVENT_UPDATE,
        TRIGGER_TYPE_UPDATE,
        old_tid,
        new_tid,
        Some(&old_t),
        Some(&new_t),
    )
}

#[cold]
#[inline(never)]
fn outside_query() -> Box<PgError> {
    Box::new(
        PgError::error("AfterTriggerSaveEvent() called outside of query".to_string())
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cold]
#[inline(never)]
fn fetch_failed(which: u32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("failed to fetch tuple{which} for AFTER trigger"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}
