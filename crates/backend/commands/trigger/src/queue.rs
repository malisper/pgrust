// The afterTriggers machinery (trigger.c): per-query event lists, the
// transaction-level deferred list, SET CONSTRAINTS state, and subxact
// save/restore. Events re-fetch tuples by ctid under SnapshotAny as C's
// table_tuple_fetch_row_version does. LOUD: transition tables, statement
// triggers, WHEN/UPDATE-OF on the AFTER save path.
use std::cell::{Cell, RefCell};

use mcx::Mcx;
use ri_triggers_seams::RiTriggerData;
use types_core::{CommandId, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{NoLock, Relation};
use types_snapshot::{SnapshotData, SNAPSHOT_ANY};
use types_trigger::{
    Trigger, TriggerDesc, AFTER_TRIGGER_DEFERRABLE, AFTER_TRIGGER_INITDEFERRED, RI_TRIGGER_FK,
    RI_TRIGGER_NONE, RI_TRIGGER_PK, TRIGGER_DISABLED, TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSERT,
    TRIGGER_EVENT_OPMASK, TRIGGER_EVENT_ROW, TRIGGER_EVENT_UPDATE, TRIGGER_FIRES_ON_REPLICA,
    TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_LEVEL_MASK,
    TRIGGER_TYPE_ROW, TRIGGER_TYPE_TIMING_MASK, TRIGGER_TYPE_UPDATE,
};
use types_tuple::{HeapTupleData, ItemPointerData};

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
    // ats_event: op | ROW | DEFERRABLE | INITDEFERRED.
    event: u32,
    ctid1: ItemPointerData,
    ctid2: ItemPointerData,
    tgoid: Oid,
    relid: Oid,
    firing_id: CommandId,
}

pub(crate) struct SetConstraintState {
    pub all_isset: bool,
    pub all_isdeferred: bool,
    pub trigstates: Vec<(Oid, bool)>,
}

impl SetConstraintState {
    pub(crate) fn create() -> Self {
        SetConstraintState { all_isset: false, all_isdeferred: false, trigstates: Vec::new() }
    }
    fn copy(&self) -> Self {
        SetConstraintState {
            all_isset: self.all_isset,
            all_isdeferred: self.all_isdeferred,
            trigstates: self.trigstates.clone(),
        }
    }
}

#[derive(Default)]
struct SavedTrans {
    state: Option<SetConstraintState>,
    state_saved: bool,
    events_len: usize,
    query_depth: i32,
    firing_counter: CommandId,
}

thread_local! {
    static FIRING_COUNTER: Cell<CommandId> = const { Cell::new(0) };
    static QUERY_DEPTH: Cell<i32> = const { Cell::new(-1) };
    static QUERY_STACK: RefCell<Vec<Vec<AfterTriggerEvent>>> =
        const { RefCell::new(Vec::new()) };
    static XACT_EVENTS: RefCell<Vec<AfterTriggerEvent>> = const { RefCell::new(Vec::new()) };
    static CON_STATE: RefCell<Option<SetConstraintState>> = const { RefCell::new(None) };
    static TRANS_STACK: RefCell<Vec<SavedTrans>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn query_depth() -> i32 {
    QUERY_DEPTH.with(|c| c.get())
}

pub(crate) fn firing_counter() -> CommandId {
    FIRING_COUNTER.with(|c| c.get())
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

#[derive(Clone, Copy, PartialEq)]
enum EvList {
    Query(usize),
    Xact,
}

fn with_list<R>(sel: EvList, f: impl FnOnce(&mut Vec<AfterTriggerEvent>) -> R) -> R {
    match sel {
        EvList::Query(d) => QUERY_STACK.with(|s| {
            let mut st = s.borrow_mut();
            while st.len() <= d {
                st.push(Vec::new());
            }
            f(&mut st[d])
        }),
        EvList::Xact => XACT_EVENTS.with(|s| f(&mut s.borrow_mut())),
    }
}

// afterTriggerCheckState (trigger.c).
fn check_state(event: u32, tgoid: Oid) -> bool {
    if event & AFTER_TRIGGER_DEFERRABLE == 0 {
        return false;
    }
    CON_STATE.with(|c| {
        if let Some(state) = c.borrow().as_ref() {
            for &(oid, deferred) in &state.trigstates {
                if oid == tgoid {
                    return deferred;
                }
            }
            if state.all_isset {
                return state.all_isdeferred;
            }
        }
        event & AFTER_TRIGGER_INITDEFERRED != 0
    })
}

// afterTriggerMarkEvents (trigger.c); move_deferred = (move_list != NULL).
fn mark_events(sel: EvList, immediate_only: bool, move_deferred: bool) -> bool {
    debug_assert!(move_deferred == matches!(sel, EvList::Query(_)));
    let firing_id = FIRING_COUNTER.with(|c| c.get());
    let mut moved: Vec<AfterTriggerEvent> = Vec::new();
    let found = with_list(sel, |evs| {
        let mut found = false;
        for ev in evs.iter_mut() {
            if ev.flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS) != 0 {
                continue;
            }
            if immediate_only && check_state(ev.event, ev.tgoid) {
                if move_deferred {
                    moved.push(AfterTriggerEvent {
                        flags: 0,
                        event: ev.event,
                        ctid1: ev.ctid1,
                        ctid2: ev.ctid2,
                        tgoid: ev.tgoid,
                        relid: ev.relid,
                        firing_id: 0,
                    });
                    ev.flags |= AFTER_TRIGGER_DONE;
                }
            } else {
                ev.firing_id = firing_id;
                ev.flags |= AFTER_TRIGGER_IN_PROGRESS;
                found = true;
            }
        }
        found
    });
    if !moved.is_empty() {
        XACT_EVENTS.with(|s| s.borrow_mut().append(&mut moved));
    }
    found
}

// afterTriggerInvokeEvents (trigger.c); returns all_fired.
fn invoke_events<'mcx>(
    mcx: Mcx<'mcx>,
    sel: EvList,
    firing_id: CommandId,
    delete_ok: bool,
) -> PgResult<bool> {
    let mut i = 0;
    loop {
        // Borrow per event: firing re-enters the queue (RI SPI queries,
        // cascade DML).
        let next = with_list(sel, |evs| {
            while i < evs.len() {
                let ev = &evs[i];
                if ev.flags & AFTER_TRIGGER_IN_PROGRESS != 0 && ev.firing_id == firing_id {
                    return Some((ev.ctid1, ev.ctid2, ev.event, ev.tgoid, ev.relid));
                }
                i += 1;
            }
            None
        });
        let Some((ctid1, ctid2, event, tgoid, relid)) = next else {
            break;
        };
        AfterTriggerExecute(mcx, ctid1, ctid2, event, tgoid, relid)?;
        with_list(sel, |evs| {
            let ev = &mut evs[i];
            ev.flags &= !AFTER_TRIGGER_IN_PROGRESS;
            ev.flags |= AFTER_TRIGGER_DONE;
        });
        i += 1;
    }
    let all_fired =
        with_list(sel, |evs| evs.iter().all(|ev| ev.flags & AFTER_TRIGGER_DONE != 0));
    if delete_ok && all_fired {
        with_list(sel, |evs| evs.clear());
    }
    Ok(all_fired)
}

pub fn AfterTriggerBeginXact() -> PgResult<()> {
    FIRING_COUNTER.with(|c| c.set(1));
    QUERY_DEPTH.with(|c| c.set(-1));
    debug_assert!(XACT_EVENTS.with(|s| s.borrow().is_empty()));
    debug_assert!(CON_STATE.with(|c| c.borrow().is_none()));
    debug_assert!(!QUERY_STACK.with(|s| s.borrow().iter().any(|q| !q.is_empty())));
    Ok(())
}

pub fn AfterTriggerBeginQuery() {
    QUERY_DEPTH.with(|c| c.set(c.get() + 1));
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
        if !mark_events(EvList::Query(d), true, true) {
            break;
        }
        let firing_id = FIRING_COUNTER.with(|c| {
            let id = c.get();
            c.set(id + 1);
            id
        });
        if invoke_events(mcx, EvList::Query(d), firing_id, false)? {
            break;
        }
    }
    QUERY_STACK.with(|s| {
        let mut st = s.borrow_mut();
        st[d].clear();
        st.truncate(d);
    });
    QUERY_DEPTH.with(|c| c.set(depth - 1));
    Ok(())
}

pub fn AfterTriggerFireDeferred() -> PgResult<()> {
    debug_assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
    let snap_pushed = XACT_EVENTS.with(|s| !s.borrow().is_empty());
    if snap_pushed {
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
    }
    let scratch = ::mcx::MemoryContext::new("AfterTriggerTupleContext");
    let mcx = scratch.mcx();
    loop {
        if !mark_events(EvList::Xact, false, false) {
            break;
        }
        let firing_id = FIRING_COUNTER.with(|c| {
            let id = c.get();
            c.set(id + 1);
            id
        });
        if invoke_events(mcx, EvList::Xact, firing_id, true)? {
            break;
        }
    }
    if snap_pushed {
        snapmgr::PopActiveSnapshot()?;
    }
    Ok(())
}

pub fn AfterTriggerEndXact(_is_commit: bool) -> PgResult<()> {
    XACT_EVENTS.with(|s| s.borrow_mut().clear());
    QUERY_STACK.with(|s| s.borrow_mut().clear());
    CON_STATE.with(|c| *c.borrow_mut() = None);
    TRANS_STACK.with(|s| s.borrow_mut().clear());
    QUERY_DEPTH.with(|c| c.set(-1));
    Ok(())
}

pub fn AfterTriggerBeginSubXact() -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
    TRANS_STACK.with(|s| {
        let mut st = s.borrow_mut();
        while st.len() <= my_level {
            st.push(SavedTrans::default());
        }
        st[my_level] = SavedTrans {
            state: None,
            state_saved: false,
            events_len: XACT_EVENTS.with(|e| e.borrow().len()),
            query_depth: QUERY_DEPTH.with(|c| c.get()),
            firing_counter: FIRING_COUNTER.with(|c| c.get()),
        };
    });
    Ok(())
}

pub fn AfterTriggerEndSubXact(is_commit: bool) -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
    if is_commit {
        TRANS_STACK.with(|s| {
            let mut st = s.borrow_mut();
            assert!(my_level < st.len());
            st[my_level].state = None;
            st[my_level].state_saved = false;
            debug_assert_eq!(QUERY_DEPTH.with(|c| c.get()), st[my_level].query_depth);
        });
        return Ok(());
    }
    let saved = TRANS_STACK.with(|s| {
        let mut st = s.borrow_mut();
        if my_level >= st.len() {
            return None;
        }
        Some(std::mem::take(&mut st[my_level]))
    });
    let Some(saved) = saved else {
        return Ok(());
    };
    // Free query levels the aborted subxact opened; restore query_depth.
    QUERY_STACK.with(|s| {
        let mut st = s.borrow_mut();
        let keep = (saved.query_depth + 1).max(0) as usize;
        for q in st.iter_mut().skip(keep) {
            q.clear();
        }
        st.truncate(keep);
    });
    QUERY_DEPTH.with(|c| c.set(saved.query_depth));
    XACT_EVENTS.with(|s| s.borrow_mut().truncate(saved.events_len));
    if saved.state_saved {
        CON_STATE.with(|c| *c.borrow_mut() = saved.state);
    }
    // Un-mark deferred events scheduled by this subxact or a child.
    XACT_EVENTS.with(|s| {
        for ev in s.borrow_mut().iter_mut() {
            if ev.flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS) != 0
                && ev.firing_id >= saved.firing_counter
            {
                ev.flags &= !(AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS);
            }
        }
    });
    Ok(())
}

// AfterTriggerSetState's write access to the shared state (state.rs).
pub(crate) fn with_con_state<R>(f: impl FnOnce(&mut SetConstraintState) -> R) -> R {
    let my_level = xact::GetCurrentTransactionNestLevel() as usize;
    CON_STATE.with(|c| {
        let mut b = c.borrow_mut();
        let state = b.get_or_insert_with(SetConstraintState::create);
        if my_level > 1 {
            TRANS_STACK.with(|s| {
                let mut st = s.borrow_mut();
                if my_level < st.len() && !st[my_level].state_saved {
                    st[my_level].state = Some(state.copy());
                    st[my_level].state_saved = true;
                }
            });
        }
        f(state)
    })
}

// The SET CONSTRAINTS ... IMMEDIATE retroactive firing loop (state.rs).
pub(crate) fn fire_now_immediate() -> PgResult<()> {
    let scratch = ::mcx::MemoryContext::new("AfterTriggerTupleContext");
    let mcx = scratch.mcx();
    let mut snapshot_set = false;
    loop {
        if !mark_events(EvList::Xact, true, false) {
            break;
        }
        if !snapshot_set {
            let snap = snapmgr::GetTransactionSnapshot()?;
            snapmgr::PushActiveSnapshot(&snap)?;
            snapshot_set = true;
        }
        let firing_id = FIRING_COUNTER.with(|c| {
            let id = c.get();
            c.set(id + 1);
            id
        });
        if invoke_events(mcx, EvList::Xact, firing_id, !xact::IsSubTransaction())? {
            break;
        }
    }
    if snapshot_set {
        snapmgr::PopActiveSnapshot()?;
    }
    Ok(())
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
    let mut t1 = r1.tuple().expect("found fetch has a tuple");
    let is_update = event & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_UPDATE;
    let r2;
    let mut t2 = if is_update {
        r2 = heapam::heap_fetch(&rel, &snap, ctid2, false)?;
        if !r2.found {
            return Err(fetch_failed(2));
        }
        Some(r2.tuple().expect("found fetch has a tuple"))
    } else {
        None
    };

    let tg_event = event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW);
    let result = if ri_trigger_kind(trigger.tgfoid) == RI_TRIGGER_NONE {
        let mut finfo = fmgr_seams::fmgr_info::call(trigger.tgfoid)?;
        let mut tdata = types_trigger_call::TriggerData::new(
            tg_event,
            &rel,
            Some(&mut t1),
            t2.as_mut(),
            trigger,
        );
        // AFTER ROW triggers: the returned tuple is ignored (C frees it).
        crate::exec::ExecCallTriggerFunc(mcx, &mut tdata, &mut finfo).map(|_| ())
    } else {
        let data = RiTriggerData {
            tg_event,
            tg_relation: &rel,
            tg_trigtuple: &t1,
            tg_newtuple: t2.as_ref(),
            tg_trigger: trigger,
        };
        ri_triggers_seams::ri_fkey_trigger::call(mcx, trigger.tgfoid, &data)
    };
    rel.close(NoLock)?;
    result
}

fn trigger_enabled(t: &Trigger<'_>) -> bool {
    // SESSION_REPLICATION_ROLE_ORIGIN (the only ported role).
    if t.tgenabled == TRIGGER_DISABLED || t.tgenabled == TRIGGER_FIRES_ON_REPLICA {
        return false;
    }
    if t.tgqual.is_some() || t.tgnattr > 0 {
        panic!(
            "TriggerEnabled (trigger.c): WHEN clause / UPDATE OF columns \
             unported on the AFTER ROW save path"
        );
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
        if trigger.tgoldtable.is_some() || trigger.tgnewtable.is_some() {
            panic!(
                "AfterTriggerSaveEvent (trigger.c): transition tables unported \
                 (trigger {})",
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
        const F_UNIQUE_KEY_RECHECK: Oid = 1250;
        if trigger.tgfoid == F_UNIQUE_KEY_RECHECK {
            panic!(
                "AfterTriggerSaveEvent (trigger.c): unique_key_recheck \
                 (recheckIndexes) unported"
            );
        }
        let ats_event = (event & TRIGGER_EVENT_OPMASK)
            | TRIGGER_EVENT_ROW
            | if trigger.tgdeferrable { AFTER_TRIGGER_DEFERRABLE } else { 0 }
            | if trigger.tginitdeferred { AFTER_TRIGGER_INITDEFERRED } else { 0 };
        with_list(EvList::Query(d), |evs| {
            evs.push(AfterTriggerEvent {
                flags: 0,
                ctid1,
                ctid2,
                event: ats_event,
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
