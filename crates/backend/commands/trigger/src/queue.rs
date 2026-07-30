// The afterTriggers machinery (trigger.c): per-query event lists, the
// transaction-level deferred list, SET CONSTRAINTS state, and subxact
// save/restore. Events re-fetch tuples by ctid under SnapshotAny as C's
// table_tuple_fetch_row_version does (statement events carry no tuples).
// Transition tables: per-depth AfterTriggersTableData with tuplestores in the
// hold registry; events reference them by index (C ats_table).
use std::cell::{Cell, RefCell};

use mcx::Mcx;
use ri_triggers_seams::RiTriggerData;
use types_core::{CommandId, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodes_enums::CmdType;
use types_portal::TuplestoreHandle;
use types_rel::{NoLock, Relation, RELKIND_PARTITIONED_TABLE};
use types_snapshot::{SnapshotData, SNAPSHOT_ANY};
use types_trigger::{
    Trigger, TriggerDesc, AFTER_TRIGGER_DEFERRABLE, AFTER_TRIGGER_INITDEFERRED, RI_TRIGGER_FK,
    RI_TRIGGER_NONE, RI_TRIGGER_PK, TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSERT,
    TRIGGER_EVENT_OPMASK, TRIGGER_EVENT_ROW, TRIGGER_EVENT_TRUNCATE, TRIGGER_EVENT_UPDATE,
    TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT,
    TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_ROW, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_TIMING_MASK,
    TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_UPDATE,
};
use heaptuple::HeapTuple;
use types_tuple::{HeapTupleData, ItemPointerData, TupleDescData};

use crate::exec::TriggerWhenEval;

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
    // C ats_table: index into this depth's TRANS_TABLES entry; MAX = none.
    table_idx: u32,
    // C AFTER_TRIGGER_CP_UPDATE ate_src_part/ate_dst_part: the leaf partitions
    // ctid1/ctid2 point into for a cross-partition update event on the root.
    src_part: Oid,
    dst_part: Oid,
    // C ats_rolid: role to execute the trigger (trigger.c:3699, 6535).
    rolid: Oid,
    // C ats_modifiedcols: the UPDATE's changed columns (offset-encoded, as
    // ExecGetAllUpdatedCols), owned so it survives to deferred firing time.
    // None for INSERT/DELETE (tg_updatedcols is NULL there).
    modifiedcols: Option<Box<[i32]>>,
}

pub(crate) struct TransTable {
    relid: Oid,
    cmd: CmdType,
    closed: bool,
    old_ts: TuplestoreHandle,
    new_ts: TuplestoreHandle,
    // C AfterTriggersTableData.before_trig_done / after_trig_done +
    // after_trig_events (trigger.c:3919-3921): statement-trigger firing
    // state rides the open table so a closed table lets a fresh set queue.
    before_trig_done: bool,
    after_trig_done: bool,
    after_trig_pos: usize,
}

// C TransitionCaptureState: need flags + AfterTriggersTableData references
// (depth-local indexes); handed to nodemodifytable per statement.
pub struct TransitionCaptureState {
    pub tcs_delete_old_table: bool,
    pub tcs_update_old_table: bool,
    pub tcs_update_new_table: bool,
    pub tcs_insert_new_table: bool,
    ins_idx: u32,
    upd_idx: u32,
    del_idx: u32,
}

impl TransitionCaptureState {
    fn table_for(&self, event_op: u32) -> u32 {
        match event_op {
            TRIGGER_EVENT_INSERT => self.ins_idx,
            TRIGGER_EVENT_UPDATE => self.upd_idx,
            TRIGGER_EVENT_DELETE => self.del_idx,
            _ => u32::MAX,
        }
    }
}

fn get_transition_table(depth: usize, relid: Oid, cmd: CmdType) -> u32 {
    TRANS_TABLES.with(|t| {
        let mut tt = t.borrow_mut();
        while tt.len() <= depth {
            tt.push(Vec::new());
        }
        let tables = &mut tt[depth];
        if let Some(i) = tables
            .iter()
            .position(|tb| tb.relid == relid && tb.cmd == cmd && !tb.closed)
        {
            return i as u32;
        }
        tables.push(TransTable {
            relid,
            cmd,
            closed: false,
            old_ts: TuplestoreHandle::NULL,
            new_ts: TuplestoreHandle::NULL,
            before_trig_done: false,
            after_trig_done: false,
            after_trig_pos: 0,
        });
        (tables.len() - 1) as u32
    })
}

fn ensure_store(depth: usize, idx: u32, old: bool) -> TuplestoreHandle {
    TRANS_TABLES.with(|t| {
        let mut tt = t.borrow_mut();
        let tb = &mut tt[depth][idx as usize];
        let slot = if old { &mut tb.old_ts } else { &mut tb.new_ts };
        if slot.is_null() {
            *slot = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(
                false,
                false,
                init_small::globals::work_mem(),
            ));
        }
        *slot
    })
}

fn free_tables_at_depth(d: usize) {
    TRANS_TABLES.with(|t| {
        let mut tt = t.borrow_mut();
        if let Some(tables) = tt.get_mut(d) {
            for tb in tables.drain(..) {
                tuplestore::hold::end(tb.old_ts);
                tuplestore::hold::end(tb.new_ts);
            }
        }
    });
}

// MakeTransitionCaptureState (trigger.c): None when no trigger wants a
// transition table for this operation.
pub fn MakeTransitionCaptureState(
    trigdesc: &TriggerDesc<'_>,
    relid: Oid,
    cmd_type: CmdType,
) -> PgResult<Option<TransitionCaptureState>> {
    let (need_old_upd, need_new_upd, need_old_del, need_new_ins) = match cmd_type {
        CmdType::CMD_INSERT => (false, false, false, trigdesc.trig_insert_new_table),
        CmdType::CMD_UPDATE => (
            trigdesc.trig_update_old_table,
            trigdesc.trig_update_new_table,
            false,
            false,
        ),
        CmdType::CMD_DELETE => (false, false, trigdesc.trig_delete_old_table, false),
        CmdType::CMD_MERGE => (
            trigdesc.trig_update_old_table,
            trigdesc.trig_update_new_table,
            trigdesc.trig_delete_old_table,
            trigdesc.trig_insert_new_table,
        ),
        other => panic!("unexpected CmdType: {other:?}"),
    };
    if !need_old_upd && !need_new_upd && !need_new_ins && !need_old_del {
        return Ok(None);
    }
    let depth = QUERY_DEPTH.with(|c| c.get());
    if depth < 0 {
        return Err(outside_query());
    }
    let d = depth as usize;
    let ins_idx = if need_new_ins {
        let i = get_transition_table(d, relid, CmdType::CMD_INSERT);
        ensure_store(d, i, false);
        i
    } else {
        u32::MAX
    };
    let upd_idx = if need_old_upd || need_new_upd {
        let i = get_transition_table(d, relid, CmdType::CMD_UPDATE);
        if need_old_upd {
            ensure_store(d, i, true);
        }
        if need_new_upd {
            ensure_store(d, i, false);
        }
        i
    } else {
        u32::MAX
    };
    let del_idx = if need_old_del {
        let i = get_transition_table(d, relid, CmdType::CMD_DELETE);
        ensure_store(d, i, true);
        i
    } else {
        u32::MAX
    };
    Ok(Some(TransitionCaptureState {
        tcs_delete_old_table: need_old_del,
        tcs_update_old_table: need_old_upd,
        tcs_update_new_table: need_new_upd,
        tcs_insert_new_table: need_new_ins,
        ins_idx,
        upd_idx,
        del_idx,
    }))
}

// C ri_ChildToRootMap (ExecGetChildToRootMap, execUtils.c:1300): the attmap
// the executor resolves per child result relation for converting its tuples
// to the root target relation's rowtype. Consumed by transition capture
// (TransitionTableAddTuple, trigger.c:5587) and the cross-partition UPDATE
// event legs (AfterTriggerSaveEvent trigger.c:6384-6410).
pub struct ChildToRoot<'a, 'mcx> {
    pub map: &'a [i16],
    pub child_desc: &'a TupleDescData<'mcx>,
    pub root_desc: &'a TupleDescData<'mcx>,
}

impl ChildToRoot<'_, '_> {
    fn convert<'m>(&self, mcx: Mcx<'m>, tup: &HeapTupleData<'_>) -> PgResult<HeapTuple<'m>> {
        heaptuple::execute_attr_map_tuple(mcx, tup, self.child_desc, self.root_desc, self.map)
    }
}

// AfterTriggerSaveEvent's transition-capture head, tuple-based. Tuples from
// an attno-remapped child are converted to the root's rowtype before storing
// (C TransitionTableAddTuple via GetAfterTriggersStoreSlot; the tuplestore
// copies on put, so the converted image is transient query-mcx storage).
fn capture_transition_tuples(
    mcx: Mcx<'_>,
    tc: &TransitionCaptureState,
    event: u32,
    old_tup: Option<&HeapTupleData<'_>>,
    new_tup: Option<&HeapTupleData<'_>>,
    old_conv: Option<&ChildToRoot<'_, '_>>,
    new_conv: Option<&ChildToRoot<'_, '_>>,
) -> PgResult<()> {
    let depth = QUERY_DEPTH.with(|c| c.get());
    debug_assert!(depth >= 0);
    let d = depth as usize;
    if let Some(old) = old_tup {
        let ts = match event {
            TRIGGER_EVENT_DELETE if tc.tcs_delete_old_table => {
                TRANS_TABLES.with(|t| t.borrow()[d][tc.del_idx as usize].old_ts)
            }
            TRIGGER_EVENT_UPDATE if tc.tcs_update_old_table => {
                TRANS_TABLES.with(|t| t.borrow()[d][tc.upd_idx as usize].old_ts)
            }
            _ => TuplestoreHandle::NULL,
        };
        if !ts.is_null() {
            match old_conv {
                Some(c) => {
                    let t = c.convert(mcx, old)?;
                    tuplestore::hold::put_heap_tuple(ts, t.as_tuple())?
                }
                None => tuplestore::hold::put_heap_tuple(ts, old)?,
            }
        }
    }
    if let Some(new) = new_tup {
        let ts = match event {
            TRIGGER_EVENT_INSERT if tc.tcs_insert_new_table => {
                TRANS_TABLES.with(|t| t.borrow()[d][tc.ins_idx as usize].new_ts)
            }
            TRIGGER_EVENT_UPDATE if tc.tcs_update_new_table => {
                TRANS_TABLES.with(|t| t.borrow()[d][tc.upd_idx as usize].new_ts)
            }
            _ => TuplestoreHandle::NULL,
        };
        if !ts.is_null() {
            match new_conv {
                Some(c) => {
                    let t = c.convert(mcx, new)?;
                    tuplestore::hold::put_heap_tuple(ts, t.as_tuple())?
                }
                None => tuplestore::hold::put_heap_tuple(ts, new)?,
            }
        }
    }
    Ok(())
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
    // AfterTriggersQueryData.tables, per query depth.
    static TRANS_TABLES: RefCell<Vec<Vec<TransTable>>> = const { RefCell::new(Vec::new()) };
}

fn stmt_cmd_type(op: u32) -> CmdType {
    match op {
        TRIGGER_EVENT_INSERT => CmdType::CMD_INSERT,
        TRIGGER_EVENT_UPDATE => CmdType::CMD_UPDATE,
        TRIGGER_EVENT_DELETE => CmdType::CMD_DELETE,
        other => panic!("stmt_cmd_type: unexpected trigger event {other:#x}"),
    }
}

// before_stmt_triggers_fired (trigger.c:2896): check-and-mark on the open
// AfterTriggersTableData, so a closed table lets BS triggers fire again.
pub fn before_stmt_triggers_fired(relid: Oid, cmd_event: u32) -> bool {
    let depth = QUERY_DEPTH.with(|c| c.get());
    if depth < 0 {
        return false;
    }
    let idx = get_transition_table(depth as usize, relid, stmt_cmd_type(cmd_event));
    TRANS_TABLES.with(|t| {
        let mut tt = t.borrow_mut();
        let tb = &mut tt[depth as usize][idx as usize];
        let done = tb.before_trig_done;
        tb.before_trig_done = true;
        done
    })
}

pub(crate) fn query_depth() -> i32 {
    QUERY_DEPTH.with(|c| c.get())
}

pub(crate) fn firing_counter() -> CommandId {
    FIRING_COUNTER.with(|c| c.get())
}

// AfterTriggerPendingOnRel (trigger.c): DONE events ignored — a DONE flag
// rolled back by subxact abort rolls the TRUNCATE/etc back too.
pub fn AfterTriggerPendingOnRel(relid: Oid) -> bool {
    let hit = XACT_EVENTS.with(|s| {
        s.borrow().iter().any(|ev| ev.flags & AFTER_TRIGGER_DONE == 0 && ev.relid == relid)
    });
    if hit {
        return true;
    }
    let depth = query_depth();
    if depth < 0 {
        return false;
    }
    QUERY_STACK.with(|s| {
        s.borrow()
            .iter()
            .take(depth as usize + 1)
            .flatten()
            .any(|ev| ev.flags & AFTER_TRIGGER_DONE == 0 && ev.relid == relid)
    })
}

// RI_FKey_trigger_type (ri_triggers.c).
pub fn ri_trigger_kind(tgfoid: Oid) -> i32 {
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
fn mark_events(sel: EvList, immediate_only: bool, move_deferred: bool) -> PgResult<bool> {
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
                        table_idx: ev.table_idx,
                        src_part: ev.src_part,
                        dst_part: ev.dst_part,
                        rolid: ev.rolid,
                        // Source is marked DONE; the deferred copy owns the set.
                        modifiedcols: ev.modifiedcols.take(),
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
        // trigger.c:4663-4671: a deferred trigger may not be queued past the
        // end of a security-restricted operation.
        if miscinit::InSecurityRestrictedOperation() {
            return Err(PgError::error(
                "cannot fire deferred trigger within security-restricted operation".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .into());
        }
        XACT_EVENTS.with(|s| s.borrow_mut().append(&mut moved));
    }
    Ok(found)
}

// afterTriggerInvokeEvents (trigger.c); returns all_fired. Owns the
// per-tuple scratch (C's AfterTriggerTupleContext) so the empty-queue
// mark_events loop in every caller never pays a context create/destroy.
fn invoke_events(sel: EvList, firing_id: CommandId, delete_ok: bool) -> PgResult<bool> {
    let scratch = ::mcx::MemoryContext::new("AfterTriggerTupleContext");
    let mcx = scratch.mcx();
    let mut i = 0;
    loop {
        // Borrow per event: firing re-enters the queue (RI SPI queries,
        // cascade DML).
        let next = with_list(sel, |evs| {
            while i < evs.len() {
                let ev = &evs[i];
                if ev.flags & AFTER_TRIGGER_IN_PROGRESS != 0 && ev.firing_id == firing_id {
                    return Some((
                        ev.ctid1, ev.ctid2, ev.event, ev.tgoid, ev.relid, ev.table_idx,
                        ev.src_part, ev.dst_part, ev.rolid,
                        ev.modifiedcols.clone(),
                    ));
                }
                i += 1;
            }
            None
        });
        let Some((
            ctid1, ctid2, event, tgoid, relid, table_idx, src_part, dst_part, rolid, modifiedcols,
        )) = next
        else {
            break;
        };
        AfterTriggerExecute(
            mcx, ctid1, ctid2, event, tgoid, relid, table_idx, src_part, dst_part, rolid,
            modifiedcols.as_deref(),
        )?;
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
    loop {
        if !mark_events(EvList::Query(d), true, true)? {
            break;
        }
        let firing_id = FIRING_COUNTER.with(|c| {
            let id = c.get();
            c.set(id + 1);
            id
        });
        if invoke_events(EvList::Query(d), firing_id, false)? {
            break;
        }
    }
    QUERY_STACK.with(|s| {
        let mut st = s.borrow_mut();
        st[d].clear();
        st.truncate(d);
    });
    free_tables_at_depth(d);
    QUERY_DEPTH.with(|c| c.set(depth - 1));
    Ok(())
}

pub fn AfterTriggerFireDeferred() -> PgResult<()> {
    debug_assert_eq!(QUERY_DEPTH.with(|c| c.get()), -1);
    // Empty queue: mark_events cannot find work; C's loop body never runs.
    if XACT_EVENTS.with(|s| s.borrow().is_empty()) {
        return Ok(());
    }
    {
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
    }
    loop {
        if !mark_events(EvList::Xact, false, false)? {
            break;
        }
        let firing_id = FIRING_COUNTER.with(|c| {
            let id = c.get();
            c.set(id + 1);
            id
        });
        if invoke_events(EvList::Xact, firing_id, true)? {
            break;
        }
    }
    snapmgr::PopActiveSnapshot()?;
    Ok(())
}

pub fn AfterTriggerEndXact(_is_commit: bool) -> PgResult<()> {
    let ndepths = TRANS_TABLES.with(|t| t.borrow().len());
    for d in 0..ndepths {
        free_tables_at_depth(d);
    }
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
    let ndepths = TRANS_TABLES.with(|t| t.borrow().len());
    let keep = (saved.query_depth + 1).max(0) as usize;
    for d in keep..ndepths {
        free_tables_at_depth(d);
    }
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
    let mut snapshot_set = false;
    loop {
        if !mark_events(EvList::Xact, true, false)? {
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
        if invoke_events(EvList::Xact, firing_id, !xact::IsSubTransaction())? {
            break;
        }
    }
    if snapshot_set {
        snapmgr::PopActiveSnapshot()?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn AfterTriggerExecute<'mcx>(
    mcx: Mcx<'mcx>,
    ctid1: ItemPointerData,
    ctid2: ItemPointerData,
    event: u32,
    tgoid: Oid,
    relid: Oid,
    table_idx: u32,
    src_part: Oid,
    dst_part: Oid,
    rolid: Oid,
    modifiedcols: Option<&[i32]>,
) -> PgResult<()> {
    let Some(trigdesc) = relcache::RelationGetTriggerDesc(relid)? else {
        return Ok(());
    };
    // Rebuild the UPDATE's ats_modifiedcols in the firing mcx; the pointer
    // rides tg_updatedcols for the trigger's bms_is_member tests. Empty for
    // INSERT/DELETE (tg_updatedcols = NULL).
    let mut updatedcols = types_nodes::Bitmapset::empty();
    let updatedcols_ptr = match modifiedcols {
        Some(members) => {
            for &m in members {
                updatedcols.add_member(mcx, m)?;
            }
            &updatedcols as *const _ as usize
        }
        None => 0,
    };
    let Some(trigger) = trigdesc.triggers.iter().find(|t| t.tgoid == tgoid) else {
        return Ok(());
    };
    let rel = table::table_open(mcx, relid, NoLock)?;

    // C L4516-4529: hand transition tuplestores to the function and mark the
    // table closed so later statements get fresh stores.
    let mut tg_oldtable = TuplestoreHandle::NULL;
    let mut tg_newtable = TuplestoreHandle::NULL;
    if table_idx != u32::MAX {
        let depth = QUERY_DEPTH.with(|c| c.get());
        debug_assert!(depth >= 0);
        TRANS_TABLES.with(|t| {
            let mut tt = t.borrow_mut();
            let tb = &mut tt[depth as usize][table_idx as usize];
            if trigger.tgoldtable.is_some() {
                tg_oldtable = tb.old_ts;
                tb.closed = true;
            }
            if trigger.tgnewtable.is_some() {
                tg_newtable = tb.new_ts;
                tb.closed = true;
            }
        });
    }

    if event & TRIGGER_EVENT_ROW == 0 {
        let tg_event = event & TRIGGER_EVENT_OPMASK;
        let mut finfo = fmgr_seams::fmgr_info::call(trigger.tgfoid)?;
        let mut tdata =
            types_trigger_call::TriggerData::new(tg_event, &rel, None, None, trigger);
        tdata.tg_oldtable = tg_oldtable.0;
        tdata.tg_newtable = tg_newtable.0;
        // AFTER triggers: any returned tuple is discarded (C L4559-4567).
        let restore = become_queuing_role(rolid);
        let result =
            crate::exec::ExecCallTriggerFunc(mcx, &mut tdata, &mut finfo).map(|_| ());
        restore_role(restore);
        rel.close(NoLock)?;
        return result;
    }

    // C AFTER_TRIGGER_CP_UPDATE: the two ctids live in the source/destination
    // leaf partitions, not in rel (the root); fetch from those. The caller
    // vetted the leaves as attno-identical to the root, so the tuples are
    // already in the root's format (C converts via ExecGetChildToRootMap).
    let cp_rels = if src_part != Oid::default() {
        Some((table::table_open(mcx, src_part, NoLock)?, table::table_open(mcx, dst_part, NoLock)?))
    } else {
        None
    };
    let (fetch1_rel, fetch2_rel) = match &cp_rels {
        Some((s, d)) => (s, d),
        None => (&rel, &rel),
    };
    let snap = SnapshotData::sentinel(mcx, SNAPSHOT_ANY);
    let r1 = heapam::heap_fetch(fetch1_rel, &snap, ctid1, false)?;
    if !r1.found {
        return Err(fetch_failed(1));
    }
    let mut t1 = r1.tuple().expect("found fetch has a tuple");
    let is_update = event & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_UPDATE;
    let r2;
    let mut t2 = if is_update {
        r2 = heapam::heap_fetch(fetch2_rel, &snap, ctid2, false)?;
        if !r2.found {
            return Err(fetch_failed(2));
        }
        Some(r2.tuple().expect("found fetch has a tuple"))
    } else {
        None
    };

    // AfterTriggerExecute (trigger.c:4438-4500): a cross-partition event's
    // tuples fetched from the leaf partitions are converted to the root
    // partitioned table's format — tg_relation is the root. C caches the
    // maps on the executor's ResultRelInfos (ExecGetChildToRootMap); the
    // queue rebuilds them per event (cold path: FK enforcement on moved
    // rows only).
    let (mut t1_conv, mut t2_conv): (Option<HeapTuple<'_>>, Option<HeapTuple<'_>>) = (None, None);
    if let Some((s, d)) = &cp_rels {
        if let Some(map) =
            tupdesc::build_attrmap_by_name_if_req(mcx, &s.rd_att, &rel.rd_att, false)?
        {
            t1_conv =
                Some(heaptuple::execute_attr_map_tuple(mcx, &t1, &s.rd_att, &rel.rd_att, &map)?);
        }
        if let Some(t2v) = &t2 {
            if let Some(map) =
                tupdesc::build_attrmap_by_name_if_req(mcx, &d.rd_att, &rel.rd_att, false)?
            {
                t2_conv = Some(heaptuple::execute_attr_map_tuple(
                    mcx, t2v, &d.rd_att, &rel.rd_att, &map,
                )?);
            }
        }
    }
    let t1_ref: &mut HeapTupleData<'_> = match t1_conv.as_mut() {
        Some(c) => c.as_tuple_mut(),
        None => &mut t1,
    };
    let mut t2_ref: Option<&mut HeapTupleData<'_>> = match t2_conv.as_mut() {
        Some(c) => Some(c.as_tuple_mut()),
        None => t2.as_mut(),
    };

    let tg_event = event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW);
    let restore = become_queuing_role(rolid);
    let result = if ri_trigger_kind(trigger.tgfoid) == RI_TRIGGER_NONE {
        let mut finfo = fmgr_seams::fmgr_info::call(trigger.tgfoid)?;
        let mut tdata = types_trigger_call::TriggerData::new(
            tg_event,
            &rel,
            Some(t1_ref),
            t2_ref.as_deref_mut(),
            trigger,
        );
        tdata.tg_oldtable = tg_oldtable.0;
        tdata.tg_newtable = tg_newtable.0;
        tdata.tg_updatedcols = updatedcols_ptr;
        // AFTER ROW triggers: the returned tuple is ignored (C frees it).
        crate::exec::ExecCallTriggerFunc(mcx, &mut tdata, &mut finfo).map(|_| ())
    } else {
        let data = RiTriggerData {
            tg_event,
            tg_relation: &rel,
            tg_trigtuple: t1_ref,
            tg_newtuple: t2_ref.as_deref(),
            tg_trigger: trigger,
        };
        ri_triggers_seams::ri_fkey_trigger::call(mcx, trigger.tgfoid, &data)
    };
    restore_role(restore);
    if let Some((s, d)) = cp_rels {
        s.close(NoLock)?;
        d.close(NoLock)?;
    }
    rel.close(NoLock)?;
    result
}

fn trigger_enabled(t: &Trigger<'_>) -> bool {
    crate::exec::TriggerEnabled(t)
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
    recheck_indexes: &[Oid],
    transition_capture: Option<&TransitionCaptureState>,
    mut when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
    // C is_crosspart_update: a DELETE on the source partition of a row
    // movement, or the UPDATE queued on the root by
    // ExecCrossPartitionUpdateForeignKey (cp_parts = source/dest leaf oids).
    is_crosspart_update: bool,
    cp_parts: Option<(Oid, Oid)>,
    // C ats_modifiedcols: the row's changed columns for an UPDATE event, saved
    // onto each queued event so tg_updatedcols is exact at (deferred) firing.
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<()> {
    let depth = QUERY_DEPTH.with(|c| c.get());
    if depth < 0 {
        return Err(outside_query());
    }
    let d = depth as usize;
    let partitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    for (tgindx, trigger) in trigdesc.triggers.iter().enumerate() {
        if !trigger_type_matches(trigger.tgtype, tgtype_event) {
            continue;
        }
        if !trigger_enabled(trigger) {
            continue;
        }
        if trigger.tgqual.is_some() || trigger.tgnattr > 0 {
            let Some(w) = when.as_deref_mut() else {
                panic!(
                    "TriggerEnabled (trigger.c): WHEN/UPDATE-OF trigger fired \
                     through a path without an evaluator (trigger {})",
                    trigger.tgname.as_str()
                );
            };
            if !w.check_tuples(tgindx, trigger, rel, event, old_tup, new_tup)? {
                continue;
            }
        }
        let is_update = event == TRIGGER_EVENT_UPDATE;
        let is_delete = event == TRIGGER_EVENT_DELETE;
        if is_update || is_delete {
            match ri_trigger_kind(trigger.tgfoid) {
                RI_TRIGGER_PK => {
                    // Cross-partition update: the component DELETE's cloned PK
                    // triggers are skipped — the root's CP UPDATE event
                    // enforces the FK (C's tgisclone skip).
                    if is_delete && is_crosspart_update && trigger.tgisclone {
                        continue;
                    }
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
                    // C: skip the UPDATE event on a partitioned FK table
                    // during its own cross-partition update — the destination
                    // leaf's INSERT event performs the check.
                    if is_update && partitioned {
                        continue;
                    }
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
                // RI_TRIGGER_NONE: ordinary row triggers on a partitioned
                // rel are not queued for the root CP UPDATE event — the same
                // trigger exists (cloned) on the affected leaves (C's arm).
                _ => {
                    if partitioned {
                        continue;
                    }
                }
            }
        }
        const F_UNIQUE_KEY_RECHECK: Oid = 1250;
        if trigger.tgfoid == F_UNIQUE_KEY_RECHECK
            && !recheck_indexes.contains(&trigger.tgconstrindid)
        {
            continue;
        }
        let ats_event = (event & TRIGGER_EVENT_OPMASK)
            | TRIGGER_EVENT_ROW
            | if trigger.tgdeferrable { AFTER_TRIGGER_DEFERRABLE } else { 0 }
            | if trigger.tginitdeferred { AFTER_TRIGGER_INITDEFERRED } else { 0 };
        let table_idx = if trigger.tgoldtable.is_some() || trigger.tgnewtable.is_some() {
            transition_capture
                .map(|tc| tc.table_for(event & TRIGGER_EVENT_OPMASK))
                .unwrap_or(u32::MAX)
        } else {
            u32::MAX
        };
        let (src_part, dst_part) = cp_parts.unwrap_or_default();
        with_list(EvList::Query(d), |evs| {
            evs.push(AfterTriggerEvent {
                flags: 0,
                ctid1,
                ctid2,
                event: ats_event,
                tgoid: trigger.tgoid,
                relid: rel.rd_id,
                firing_id: 0,
                table_idx,
                src_part,
                dst_part,
                rolid: miscinit::GetUserId(),
                modifiedcols: modified_cols
                    .map(|b| b.iter().collect::<Vec<i32>>().into_boxed_slice()),
            });
        });
    }
    Ok(())
}

// cancel_prior_stmt_triggers (trigger.c:2929-3010): cancel the AS set this
// table's last save queued (even mid-firing — C clears IN_PROGRESS), then
// record the insertion point for the set about to queue. The state rides the
// open AfterTriggersTableData: once an AS trigger fires and closes the table,
// later sub-statements queue a fresh set against fresh transition tables.
fn cancel_prior_stmt_triggers(relid: Oid, op: u32) {
    let depth = QUERY_DEPTH.with(|c| c.get());
    if depth < 0 {
        return;
    }
    let d = depth as usize;
    let idx = get_transition_table(d, relid, stmt_cmd_type(op)) as usize;
    let after_done = TRANS_TABLES.with(|t| {
        let tt = t.borrow();
        let tb = &tt[d][idx];
        tb.after_trig_done.then_some(tb.after_trig_pos)
    });
    let new_pos = with_list(EvList::Query(d), |evs| {
        if let Some(pos) = after_done {
            let start = pos.min(evs.len());
            for ev in evs[start..].iter_mut() {
                if ev.relid != relid
                    || ev.event & TRIGGER_EVENT_OPMASK != op
                    || ev.event & TRIGGER_EVENT_ROW != 0
                {
                    break;
                }
                ev.flags &= !AFTER_TRIGGER_IN_PROGRESS;
                ev.flags |= AFTER_TRIGGER_DONE;
            }
        }
        evs.len()
    });
    TRANS_TABLES.with(|t| {
        let mut tt = t.borrow_mut();
        let tb = &mut tt[d][idx];
        tb.after_trig_done = true;
        tb.after_trig_pos = new_pos;
    });
}

// AfterTriggerSaveEvent, statement-level arm (row_trigger=false): no tuples,
// both ctids invalid. TRUNCATE never cancels a prior set (C's switch).
fn save_stmt_event<'mcx>(
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    event: u32,
    tgtype_event: i16,
    transition_capture: Option<&TransitionCaptureState>,
    mut when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
) -> PgResult<()> {
    let depth = QUERY_DEPTH.with(|c| c.get());
    if depth < 0 {
        return Err(outside_query());
    }
    let d = depth as usize;
    if event != TRIGGER_EVENT_TRUNCATE {
        cancel_prior_stmt_triggers(rel.rd_id, event & TRIGGER_EVENT_OPMASK);
    }
    for (tgindx, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype
            & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | tgtype_event)
            != TRIGGER_TYPE_STATEMENT | TRIGGER_TYPE_AFTER | tgtype_event
        {
            continue;
        }
        if !trigger_enabled(trigger) {
            continue;
        }
        if trigger.tgqual.is_some() || trigger.tgnattr > 0 {
            let Some(w) = when.as_deref_mut() else {
                panic!(
                    "TriggerEnabled (trigger.c): WHEN/UPDATE-OF trigger fired \
                     through a path without an evaluator (trigger {})",
                    trigger.tgname.as_str()
                );
            };
            if !w.check_tuples(tgindx, trigger, rel, event, None, None)? {
                continue;
            }
        }
        let ats_event = (event & TRIGGER_EVENT_OPMASK)
            | if trigger.tgdeferrable { AFTER_TRIGGER_DEFERRABLE } else { 0 }
            | if trigger.tginitdeferred { AFTER_TRIGGER_INITDEFERRED } else { 0 };
        let table_idx = if trigger.tgoldtable.is_some() || trigger.tgnewtable.is_some() {
            transition_capture
                .map(|tc| tc.table_for(event & TRIGGER_EVENT_OPMASK))
                .unwrap_or(u32::MAX)
        } else {
            u32::MAX
        };
        with_list(EvList::Query(d), |evs| {
            evs.push(AfterTriggerEvent {
                flags: 0,
                ctid1: ItemPointerData::default(),
                ctid2: ItemPointerData::default(),
                event: ats_event,
                tgoid: trigger.tgoid,
                relid: rel.rd_id,
                firing_id: 0,
                table_idx,
                src_part: Oid::default(),
                dst_part: Oid::default(),
                rolid: miscinit::GetUserId(),
                // Statement-level arm keeps tg_updatedcols unset (see fn doc).
                modifiedcols: None,
            });
        });
    }
    Ok(())
}

pub fn ExecASInsertTriggers<'mcx>(
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    transition_capture: Option<&TransitionCaptureState>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
) -> PgResult<()> {
    if !trigdesc.trig_insert_after_statement {
        return Ok(());
    }
    save_stmt_event(rel, trigdesc, TRIGGER_EVENT_INSERT, TRIGGER_TYPE_INSERT, transition_capture, when)
}

pub fn ExecASDeleteTriggers<'mcx>(
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    transition_capture: Option<&TransitionCaptureState>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
) -> PgResult<()> {
    if !trigdesc.trig_delete_after_statement {
        return Ok(());
    }
    save_stmt_event(rel, trigdesc, TRIGGER_EVENT_DELETE, TRIGGER_TYPE_DELETE, transition_capture, when)
}

pub fn ExecASUpdateTriggers<'mcx>(
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    transition_capture: Option<&TransitionCaptureState>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
) -> PgResult<()> {
    if !trigdesc.trig_update_after_statement {
        return Ok(());
    }
    save_stmt_event(rel, trigdesc, TRIGGER_EVENT_UPDATE, TRIGGER_TYPE_UPDATE, transition_capture, when)
}

// ExecASTruncateTriggers (trigger.c).
pub fn ExecASTruncateTriggers<'mcx>(
    rel: &Relation<'mcx>,
    trigdesc: &TriggerDesc<'static>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
) -> PgResult<()> {
    if !trigdesc.trig_truncate_after_statement {
        return Ok(());
    }
    save_stmt_event(rel, trigdesc, TRIGGER_EVENT_TRUNCATE, TRIGGER_TYPE_TRUNCATE, None, when)
}

#[allow(clippy::too_many_arguments)]
pub fn ExecARInsertTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: Option<&TriggerDesc<'static>>,
    new_tid: ItemPointerData,
    recheck_indexes: &[Oid],
    transition_capture: Option<&TransitionCaptureState>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
    // rel's child->root map when rel is an attno-remapped child of the
    // capture target (C TransitionTableAddTuple's ExecGetChildToRootMap).
    child_to_root: Option<&ChildToRoot<'_, 'mcx>>,
) -> PgResult<()> {
    let after_row = trigdesc.is_some_and(|td| td.trig_insert_after_row);
    let capture = transition_capture.filter(|tc| tc.tcs_insert_new_table);
    if !after_row && capture.is_none() {
        return Ok(());
    }
    // C evaluates WHEN quals against the executor slots at queue time; the
    // ctid re-fetch stands in for them (capture needs the tuple anyway).
    let need_tuple = capture.is_some()
        || trigdesc.is_some_and(|td| td.triggers.iter().any(|t| t.tgqual.is_some()));
    let snap = SnapshotData::sentinel(mcx, SNAPSHOT_ANY);
    let mut r_new = None;
    if need_tuple {
        let r = heapam::heap_fetch(rel, &snap, new_tid, false)?;
        if !r.found {
            return Err(fetch_failed(1));
        }
        r_new = Some(r);
    }
    let new_t = r_new.as_ref().map(|r| r.tuple().expect("found fetch has a tuple"));
    if let Some(tc) = capture {
        capture_transition_tuples(
            mcx,
            tc,
            TRIGGER_EVENT_INSERT,
            None,
            Some(new_t.as_ref().expect("fetched above")),
            None,
            child_to_root,
        )?;
        if !after_row {
            return Ok(());
        }
    }
    after_trigger_save_event(
        mcx,
        rel,
        trigdesc.expect("after_row implies a trigdesc"),
        TRIGGER_EVENT_INSERT,
        TRIGGER_TYPE_INSERT,
        new_tid,
        ItemPointerData::default(),
        None,
        new_t.as_ref(),
        recheck_indexes,
        transition_capture,
        when,
        false,
        None,
        None, // INSERT: tg_updatedcols is NULL
    )
}

pub fn ExecARDeleteTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: Option<&TriggerDesc<'static>>,
    old_tid: ItemPointerData,
    transition_capture: Option<&TransitionCaptureState>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
    // C is_crosspart_update: this DELETE is the source half of a row
    // movement; cloned PK RI triggers are skipped (the root CP event runs).
    is_crosspart_update: bool,
    child_to_root: Option<&ChildToRoot<'_, 'mcx>>,
) -> PgResult<()> {
    let after_row = trigdesc.is_some_and(|td| td.trig_delete_after_row);
    let capture = transition_capture.filter(|tc| tc.tcs_delete_old_table);
    if !after_row && capture.is_none() {
        return Ok(());
    }
    let need_tuple = capture.is_some()
        || trigdesc.is_some_and(|td| td.triggers.iter().any(|t| t.tgqual.is_some()));
    let snap = SnapshotData::sentinel(mcx, SNAPSHOT_ANY);
    let mut r_old = None;
    if need_tuple {
        let r = heapam::heap_fetch(rel, &snap, old_tid, false)?;
        if !r.found {
            return Err(fetch_failed(1));
        }
        r_old = Some(r);
    }
    let old_t = r_old.as_ref().map(|r| r.tuple().expect("found fetch has a tuple"));
    if let Some(tc) = capture {
        capture_transition_tuples(
            mcx,
            tc,
            TRIGGER_EVENT_DELETE,
            Some(old_t.as_ref().expect("fetched above")),
            None,
            child_to_root,
            None,
        )?;
        if !after_row {
            return Ok(());
        }
    }
    after_trigger_save_event(
        mcx,
        rel,
        trigdesc.expect("after_row implies a trigdesc"),
        TRIGGER_EVENT_DELETE,
        TRIGGER_TYPE_DELETE,
        old_tid,
        ItemPointerData::default(),
        old_t.as_ref(),
        None,
        &[],
        transition_capture,
        when,
        is_crosspart_update,
        None,
        None, // DELETE: tg_updatedcols is NULL
    )
}

// C ExecARUpdateTriggers: src_rel/dst_rel are the source/destination leaf
// partitions of a cross-partition update — old_tid points into src_rel (or
// rel) and new_tid into dst_rel (or rel). One-sided calls (old-only on the
// source's DELETE half, new-only on the destination's INSERT half) capture
// into the UPDATE transition tables and queue no row events (C's
// TupIsNull-XOR early return).
#[allow(clippy::too_many_arguments)]
pub fn ExecARUpdateTriggers<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    trigdesc: Option<&TriggerDesc<'static>>,
    src_rel: Option<&Relation<'mcx>>,
    dst_rel: Option<&Relation<'mcx>>,
    old_tid: Option<ItemPointerData>,
    new_tid: Option<ItemPointerData>,
    recheck_indexes: &[Oid],
    transition_capture: Option<&TransitionCaptureState>,
    when: Option<&mut TriggerWhenEval<'_, 'mcx>>,
    is_crosspart_update: bool,
    // Child->root maps for the old (source) and new (destination) tuples;
    // one-sided capture calls pass the leaf's own map on the live side, the
    // root CP event passes the source/destination leaf maps.
    src_conv: Option<&ChildToRoot<'_, 'mcx>>,
    dst_conv: Option<&ChildToRoot<'_, 'mcx>>,
    // C ExecGetAllUpdatedCols → each event's ats_modifiedcols → tg_updatedcols.
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<()> {
    let after_row = trigdesc.is_some_and(|td| td.trig_update_after_row);
    let capture = transition_capture
        .filter(|tc| tc.tcs_update_old_table || tc.tcs_update_new_table);
    if !after_row && capture.is_none() {
        return Ok(());
    }
    // GetTupleForTrigger's fetch, for the RI-skip inspections and capture.
    let snap = SnapshotData::sentinel(mcx, SNAPSHOT_ANY);
    let r_old = match old_tid {
        Some(tid) => {
            let r = heapam::heap_fetch(src_rel.unwrap_or(rel), &snap, tid, false)?;
            if !r.found {
                return Err(fetch_failed(1));
            }
            Some(r)
        }
        None => None,
    };
    let r_new = match new_tid {
        Some(tid) => {
            let r = heapam::heap_fetch(dst_rel.unwrap_or(rel), &snap, tid, false)?;
            if !r.found {
                return Err(fetch_failed(2));
            }
            Some(r)
        }
        None => None,
    };
    let old_t = r_old.as_ref().map(|r| r.tuple().expect("found fetch has a tuple"));
    let new_t = r_new.as_ref().map(|r| r.tuple().expect("found fetch has a tuple"));
    if let Some(tc) = capture {
        capture_transition_tuples(
            mcx, tc, TRIGGER_EVENT_UPDATE, old_t.as_ref(), new_t.as_ref(), src_conv, dst_conv,
        )?;
    }
    if !after_row || old_t.is_some() != new_t.is_some() {
        return Ok(());
    }
    // C AfterTriggerSaveEvent (trigger.c:6384-6410): the root CP UPDATE
    // event's leaf tuples are converted to the partitioned root's format
    // before the trigger loop — the WHEN and RI key checks run in root
    // coordinates, and the queued ctids still point into the leaves.
    let (old_root, new_root);
    let (mut old_ref, mut new_ref) = (old_t.as_ref(), new_t.as_ref());
    if src_rel.is_some() && dst_rel.is_some() {
        if let Some(c) = src_conv {
            old_root = c.convert(mcx, old_ref.expect("checked above"))?;
            old_ref = Some(old_root.as_tuple());
        }
        if let Some(c) = dst_conv {
            new_root = c.convert(mcx, new_ref.expect("checked above"))?;
            new_ref = Some(new_root.as_tuple());
        }
    }
    after_trigger_save_event(
        mcx,
        rel,
        trigdesc.expect("after_row implies a trigdesc"),
        TRIGGER_EVENT_UPDATE,
        TRIGGER_TYPE_UPDATE,
        old_tid.expect("checked above"),
        new_tid.expect("checked above"),
        old_ref,
        new_ref,
        recheck_indexes,
        transition_capture,
        when,
        is_crosspart_update,
        src_rel.zip(dst_rel).map(|(s, d)| (s.rd_id, d.rd_id)),
        modified_cols,
    )
}

// AfterTriggerExecute's queued-role swap (trigger.c:4550-4571): become the
// role active when the trigger was queued; a mismatch restores after the
// call. Error paths abort the (sub)transaction, which restores userid state.
fn become_queuing_role(rolid: Oid) -> Option<(Oid, i32)> {
    let (save_rolid, save_sec) = miscinit::GetUserIdAndSecContext();
    if save_rolid != rolid {
        miscinit::SetUserIdAndSecContext(
            rolid,
            save_sec | types_core::SECURITY_LOCAL_USERID_CHANGE,
        );
        Some((save_rolid, save_sec))
    } else {
        None
    }
}

fn restore_role(restore: Option<(Oid, i32)>) {
    if let Some((rolid, sec)) = restore {
        miscinit::SetUserIdAndSecContext(rolid, sec);
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn outside_query() -> Box<PgError> {
    Box::new(
        PgError::error("AfterTriggerSaveEvent() called outside of query".to_string())
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn fetch_failed(which: u32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("failed to fetch tuple{which} for AFTER trigger"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ats_modifiedcols pin: a deferred UPDATE event moved to the xact list
    // at query end keeps its modified-column set (red before the
    // tg_updatedcols fix — the queue dropped the set and deferred triggers
    // saw tg_updatedcols = NULL; contrib/lo's deferred legs are the e2e).
    #[test]
    fn deferred_move_carries_modifiedcols() {
        const TGOID: Oid = 424_242;
        AfterTriggerBeginXact().unwrap();
        AfterTriggerBeginQuery();
        with_list(EvList::Query(0), |evs| {
            evs.push(AfterTriggerEvent {
                flags: 0,
                ctid1: ItemPointerData::default(),
                ctid2: ItemPointerData::default(),
                event: TRIGGER_EVENT_UPDATE
                    | TRIGGER_EVENT_ROW
                    | AFTER_TRIGGER_DEFERRABLE
                    | AFTER_TRIGGER_INITDEFERRED,
                tgoid: TGOID,
                relid: 424_243,
                firing_id: 0,
                table_idx: u32::MAX,
                src_part: Oid::default(),
                dst_part: Oid::default(),
                rolid: 10,
                modifiedcols: Some(vec![9, 11].into_boxed_slice()),
            });
        });
        AfterTriggerEndQuery().unwrap();
        let carried = XACT_EVENTS.with(|s| {
            s.borrow().iter().find(|e| e.tgoid == TGOID).map(|e| e.modifiedcols.clone())
        });
        assert_eq!(carried, Some(Some(vec![9, 11].into_boxed_slice())));
        XACT_EVENTS.with(|s| s.borrow_mut().retain(|e| e.tgoid != TGOID));
    }
}
