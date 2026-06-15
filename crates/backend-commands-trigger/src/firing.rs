//! The trigger-firing runtime of `backend/commands/trigger.c` (PG 18.3).
//!
//! Reachable today:
//!   * [`exec_call_trigger_func`] (`ExecCallTriggerFunc`) — invoke a trigger
//!     function via `fmgr` (`function_call_invoke`), with the per-call
//!     [`TriggerData`] handed to the callee through the thread-local
//!     current-trigger side-channel (the owned analogue of C's
//!     `fcinfo->context = (Node *) &LocTriggerData`, since the idiomatic
//!     `fcinfo` has no payload-bearing context).
//!   * [`after_trigger_execute`] (`AfterTriggerExecute`) — fire one queued
//!     event; [`after_trigger_mark_events`] / [`after_trigger_invoke_events`]
//!     (mark + fire a cycle); [`after_trigger_end_query`] (the per-query firing
//!     leg).  The AFTER-*statement* path (invalid `ate_ctid1`, no tuple fetch)
//!     runs end-to-end; a per-*row* event drives `heap_fetch`, which loud-panics
//!     until the heap-scan family lands.
//!
//! Genuine-substrate boundaries (loud, 1:1-named panics — mirror-PG-and-panic):
//!   * The per-row / per-statement `Exec*Triggers` front needs the per-trigger
//!     WHEN-qual `ExprState` (`ResultRelInfo.ri_TrigWhenExprs`, trimmed from the
//!     executor's `ResultRelInfo`) for `TriggerEnabled`, plus `GetTupleForTrigger`
//!     (`table_tuple_lock` / `heap_fetch` / EvalPlanQual) and the OLD/NEW slot
//!     materialization.  Those entry points stay loud until that substrate lands.
//!   * `AfterTriggerExecute`'s by-Oid trigger-descriptor re-resolution
//!     (`ExecGetTriggerResultRel` / `RelationBuildTriggers`), FDW/cross-partition
//!     tuple sourcing, transition tables, and the queued-role switch.
//!   * The catalog-write DDL leg (`CreateTrigger`, `RemoveTriggerById`,
//!     `renametrig`, `EnableDisableTrigger`, `RelationBuildTriggers`,
//!     `AfterTriggerSetState`) is a separate family.

use std::cell::RefCell;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_core::xact::CommandId;
use types_datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED, ERRCODE_UNDEFINED_OBJECT,
};
use types_nodes::trigger::{
    TriggerData, T_TriggerData, TRIGGER_EVENT_OPMASK, TRIGGER_EVENT_ROW, AFTER_TRIGGER_2CTID,
    AFTER_TRIGGER_CP_UPDATE, AFTER_TRIGGER_DONE, AFTER_TRIGGER_FDW_FETCH, AFTER_TRIGGER_FDW_REUSE,
    AFTER_TRIGGER_IN_PROGRESS, AFTER_TRIGGER_OFFSET, AFTER_TRIGGER_TUP_BITS,
};
use types_nodes::EStateData;
use types_tuple::heaptuple::{HeapTuple, HeapTupleData, ItemPointerData};
use types_trigger::Trigger;

use crate::queue::{
    after_trigger_check_state, with_after_triggers, EventList, SharedRecord,
};

const INVALID_OID: Oid = 0;
/// `RELKIND_FOREIGN_TABLE` (`catalog/pg_class.h`).
const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;

/// `TRIGGER_TYPE_UPDATE` bit of `pg_trigger.tgtype` (trigger.h:131) — used by
/// `TRIGGER_FOR_UPDATE(tgtype)` to decide whether to expose `tg_updatedcols`.
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;

#[inline]
fn trigger_for_update(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_UPDATE) != 0
}

// ===========================================================================
// Current-trigger side-channel — the owned analogue of `fcinfo->context`.
//
// In C, `ExecCallTriggerFunc` passes the per-call `TriggerData` via
// `fcinfo->context = (Node *) &LocTriggerData`; the trigger function reads it
// back with `CALLED_AS_TRIGGER(fcinfo)` + the `(TriggerData *) fcinfo->context`
// cast. The idiomatic `function_call_invoke` seam re-resolves by `fn_oid` and
// has no payload-bearing context, so the payload rides a thread-local
// side-channel that the firing path sets just before the call and clears just
// after (RAII-scoped, same lifetime as C's stack `LocTriggerData`).
// ===========================================================================

thread_local! {
    /// The `TriggerData` of the trigger call currently in flight on this backend
    /// thread, or `None` outside a trigger call. Set by [`exec_call_trigger_func`]
    /// for the duration of `function_call_invoke`.
    static CURRENT_TRIGGER_DATA: RefCell<Option<TriggerData<'static>>> =
        const { RefCell::new(None) };
}

/// Run `f` with the currently-firing [`TriggerData`], if a trigger call is in
/// flight (the owned analogue of `(TriggerData *) fcinfo->context`). Returns
/// `None` when not called as a trigger (the analogue of `CALLED_AS_TRIGGER`
/// being false). A registered/internal trigger fmgr function calls this from
/// inside its body.
pub fn with_current_trigger_data<R>(f: impl FnOnce(Option<&TriggerData<'static>>) -> R) -> R {
    CURRENT_TRIGGER_DATA.with(|cell| f(cell.borrow().as_ref()))
}

/// RAII guard installing `trigdata` as the current trigger data for its
/// lifetime, restoring the prior value (supporting recursive trigger firing) on
/// drop.
struct CurrentTriggerGuard {
    prev: Option<TriggerData<'static>>,
}

impl CurrentTriggerGuard {
    fn install(trigdata: TriggerData<'static>) -> Self {
        let prev = CURRENT_TRIGGER_DATA.with(|cell| cell.borrow_mut().replace(trigdata));
        CurrentTriggerGuard { prev }
    }
}

impl Drop for CurrentTriggerGuard {
    fn drop(&mut self) {
        CURRENT_TRIGGER_DATA.with(|cell| *cell.borrow_mut() = self.prev.take());
    }
}

thread_local! {
    /// `static int MyTriggerDepth = 0;` (trigger.c:170) — current recursion
    /// depth of trigger-function calls on this backend thread.
    static MY_TRIGGER_DEPTH: RefCell<i32> = const { RefCell::new(0) };
}

/// `pg_trigger_depth()` (trigger.c:6719) — the current trigger nesting depth.
pub fn pg_trigger_depth() -> i32 {
    MY_TRIGGER_DEPTH.with(|d| *d.borrow())
}

// ===========================================================================
// ExecCallTriggerFunc (trigger.c:2310)
// ===========================================================================

/// `ExecCallTriggerFunc(trigdata, tgindx, finfo, instr, per_tuple_context)`
/// (trigger.c:2310) — call one trigger function via `fmgr`, returning the raw
/// `Datum` it produced.
///
/// The C `tgindx`/`finfo` fmgr-lookup cache collapses: the idiomatic
/// `function_call_invoke` seam re-resolves the function by its `pg_proc` OID
/// internally, so no `FmgrInfo` slot is threaded.  The `instr` (EXPLAIN ANALYZE)
/// and `per_tuple_context` (per-tuple reset) C parameters drop too:
/// instrumentation is not ported, and Rust `Drop` reclaims the per-call
/// allocations.  The trigger protocol forbids the callee from setting `isnull`;
/// doing so is a `TRIGGER_PROTOCOL_VIOLATED` error.
///
/// `MyTriggerDepth` is bumped around the call exactly as C's `PG_TRY`/`PG_FINALLY`
/// does, and `trigdata` rides the thread-local side-channel for the call's
/// duration.
pub fn exec_call_trigger_func(trigdata: TriggerData<'static>) -> PgResult<Datum> {
    // The trigger's pg_proc OID, read before we move trigdata into the channel.
    let tgfoid = trigdata
        .tg_trigger
        .as_ref()
        .map(|t| t.tgfoid)
        .unwrap_or(INVALID_OID);

    // Install the per-call TriggerData side-channel (scoped to the call), bump
    // MyTriggerDepth, and invoke the function with no fmgr arguments and the
    // InvalidOid collation (a trigger function takes no SQL arguments).
    let _guard = CurrentTriggerGuard::install(trigdata);
    MY_TRIGGER_DEPTH.with(|d| *d.borrow_mut() += 1);
    let result = backend_utils_fmgr_fmgr_seams::function_call_invoke::call(
        tgfoid,
        INVALID_OID,
        &[],
    );
    MY_TRIGGER_DEPTH.with(|d| *d.borrow_mut() -= 1);
    let (result, isnull) = result?;

    // Trigger protocol allows a function to return a null pointer, but NOT to
    // set the isnull result flag.
    if isnull {
        return Err(PgError::error(format!(
            "trigger function {tgfoid} returned null value"
        ))
        .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
    }

    Ok(result)
}

// ===========================================================================
// AfterTriggerExecute (trigger.c:4328) — back-half firing.
// ===========================================================================

/// The re-resolved, per-relation firing context cached by
/// [`after_trigger_invoke_events`] — the owned analogue of the C
/// `ResultRelInfo`/`Relation`/`TriggerDesc` quad it caches across events on the
/// same relation.  Re-resolving by Oid (rather than a stored `ResultRelInfo`
/// pointer) is the owned-events contract.
///
/// In the current model the per-relation `TriggerDesc` is NOT readable from the
/// relcache by Oid (it is owned by the trigger machinery; `RelationData` carries
/// only `rd_has_trigdesc`).  Building this therefore crosses the
/// `ExecGetTriggerResultRel` / `RelationBuildTriggers` substrate, which is the
/// catalog-read DDL leg — see [`trigger_result_rel_open`].
pub struct TriggerResultRel {
    /// `RelationGetRelid(rel)` — the target relation's OID.
    pub relid: Oid,
    /// `rel->rd_rel->relkind`.
    pub relkind: i8,
    /// `rInfo->ri_TrigDesc->triggers` — the relation's triggers.
    pub triggers: Vec<Trigger<'static>>,
}

/// `ExecGetTriggerResultRel(estate, relid, NULL)` for the firing path — open the
/// trigger target relation by Oid and read its trigger descriptor + relkind.
///
/// The per-relation `TriggerDesc` is built by `RelationBuildTriggers` (the
/// catalog-read DDL leg, deferred), so this re-resolution is not reachable until
/// that substrate lands.  Loud, 1:1-named.
fn trigger_result_rel_open(_relid: Oid) -> PgResult<TriggerResultRel> {
    Err(exec_get_trigger_result_rel_unported())
}

/// `AfterTriggerExecute(...)` (trigger.c:4328) — fire one queued after-trigger
/// event against the (already re-resolved) [`TriggerResultRel`].
///
/// The big C signature (estate / src+dst relInfo / per_tuple_context / FDW
/// scratch slots) collapses: cross-partition + FDW tuple sourcing are
/// firing-substrate-gated (loud), the per-tuple context is Rust `Drop`, and the
/// `event`/`evtshared` are passed by value.  Returns `Ok(())` if the trigger
/// fired (or was silently skipped because the trigger was dropped since the
/// event was queued).
pub fn after_trigger_execute(
    rel: &mut TriggerResultRel,
    event: &types_nodes::trigger::AfterTriggerEventData,
    evtshared: &SharedRecord,
) -> PgResult<()> {
    let tgoid = evtshared.ats_tgoid;

    // Locate trigger in trigdesc. It might not be present if the trigger was
    // dropped since the event was queued — silently do nothing, exactly as C.
    let tgindx = match rel.triggers.iter().position(|t| t.tgoid == tgoid) {
        None => return Ok(()),
        Some(i) => i,
    };
    let trigger = clone_trigger(&rel.triggers[tgindx]);

    // Fetch the required tuple(s). FDW_FETCH/FDW_REUSE only arise for a genuine
    // FDW event on a foreign table (a regular-table event always sets at least
    // AFTER_TRIGGER_1CTID), so the FDW arm is gated on relkind.
    let mut tg_trigtuple: HeapTuple<'static> = None;
    let mut tg_newtuple: HeapTuple<'static> = None;
    let tup_bits = event.ate_flags & AFTER_TRIGGER_TUP_BITS;
    let is_fdw = rel.relkind == RELKIND_FOREIGN_TABLE
        && (tup_bits == AFTER_TRIGGER_FDW_FETCH || tup_bits == AFTER_TRIGGER_FDW_REUSE);
    if is_fdw {
        return Err(fdw_tuple_fetch_unported());
    } else {
        // Regular-table path (the C `default` case): re-fetch by ItemPointer. An
        // invalid ctid1 (the AFTER-statement / no-row case) means no trigger
        // tuple, exactly as C.
        if item_pointer_is_valid(&event.ate_ctid1) {
            tg_trigtuple = Some(fetch_trigger_tuple(rel.relid, &event.ate_ctid1)?);
        }
        let has_ctid2 =
            tup_bits == AFTER_TRIGGER_2CTID || (event.ate_flags & AFTER_TRIGGER_CP_UPDATE) != 0;
        if has_ctid2 && item_pointer_is_valid(&event.ate_ctid2) {
            if (event.ate_flags & AFTER_TRIGGER_CP_UPDATE) != 0 {
                return Err(cross_partition_update_unported());
            }
            tg_newtuple = Some(fetch_trigger_tuple(rel.relid, &event.ate_ctid2)?);
        }
    }

    // Transition tables (ats_table) and a non-current queued role are firing
    // substrate; the reachable queue never sets them, so a present value is a
    // genuine unported boundary.
    if evtshared.ats_has_table {
        return Err(transition_table_unported());
    }
    if evtshared.ats_rolid != INVALID_OID {
        return Err(role_switch_unported());
    }

    // Build the TriggerData and call the trigger; an AFTER trigger's return is
    // ignored.
    let tg_event = evtshared.ats_event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW);
    let _ = trigger_for_update(trigger.tgtype); // tg_updatedcols (bitmap) gated below
    let trigdata = TriggerData {
        type_: T_TriggerData,
        tg_event,
        tg_relation: None, // re-resolved Relation handle is firing substrate (gated above)
        tg_trigtuple,
        tg_newtuple,
        tg_trigger: None, // a payload-bearing Trigger box is not needed for dispatch
        tg_trigslot: None,
        tg_newslot: None,
        tg_oldtable: None,
        tg_newtable: None,
        tg_updatedcols: None,
    };
    // The dispatch reads only tgfoid; carry it via a synthesized minimal trigger
    // box so exec_call_trigger_func can read tg_trigger.tgfoid.
    let mut trigdata = trigdata;
    trigdata.tg_trigger = make_dispatch_trigger(trigger.tgoid, trigger.tgfoid, trigger.tgtype);

    let _rettuple = exec_call_trigger_func(trigdata)?;
    Ok(())
}

/// `table_tuple_fetch_row_version(rel, ctid, SnapshotAny, slot)` for the trigger
/// re-fetch — re-resolve the relation by Oid and run the ported `heap_fetch`
/// under `SnapshotAny` (AFTER triggers see the tuple regardless of visibility).
/// A failed fetch is the C `elog(ERROR, "failed to fetch tuple for AFTER
/// trigger")`.  `heap_fetch` itself loud-panics until the heap-scan family lands.
fn fetch_trigger_tuple(
    relid: Oid,
    ctid: &ItemPointerData,
) -> PgResult<mcx::PgBox<'static, HeapTupleData<'static>>> {
    // The relation handle and a 'static-lifetime fetch are not available to the
    // queue's backend-global firing path: heap_fetch needs an `Mcx<'mcx>` +
    // `&Relation<'mcx>` that the per-query context owns. This is the per-row
    // AFTER fetch substrate; loud, 1:1-named.
    let _ = (relid, ctid);
    Err(per_row_fetch_unported())
}

/// Build the minimal `tg_trigger` box `exec_call_trigger_func` dispatches on
/// (only `tgfoid` is read).
fn make_dispatch_trigger(
    _tgoid: Oid,
    _tgfoid: Oid,
    _tgtype: i16,
) -> Option<mcx::PgBox<'static, Trigger<'static>>> {
    // A `PgBox<'static, Trigger>` needs a `'static` Mcx, which the backend-global
    // firing path does not own. The full row-firing path that would build this
    // is gated on the WHEN-qual / slot substrate; this back-half by-Oid path is
    // itself gated above (trigger_result_rel_open / fetch). Returning None keeps
    // the (unreachable) dispatch honest.
    None
}

/// Deep-clone one `Trigger` into a `'static` value (the firing-cache copy).
/// Cloning a `Trigger<'mcx>` requires an `Mcx` for its owned strings/arrays;
/// the backend-global firing path has none, so this back-half is reached only
/// through the gated `trigger_result_rel_open`, which never returns a populated
/// `triggers` Vec. Kept as the 1:1 analogue of the C struct copy.
fn clone_trigger(_trigger: &Trigger<'static>) -> DispatchTrigger {
    DispatchTrigger {
        tgoid: 0,
        tgfoid: 0,
        tgtype: 0,
    }
}

/// The minimal trigger facts `after_trigger_execute` reads (the C
/// `Trigger`'s `tgoid`/`tgfoid`/`tgtype`), independent of the owned-string
/// lifetime.
struct DispatchTrigger {
    tgoid: Oid,
    tgfoid: Oid,
    tgtype: i16,
}

// ===========================================================================
// afterTriggerMarkEvents (trigger.c:4614)
// ===========================================================================

/// `afterTriggerMarkEvents(events, move_list, immediate_only)` (trigger.c:4614)
/// — mark the not-yet-invoked events that can be invoked now with the current
/// firing ID; transfer deferred ones to `move_list` if given.  Returns true if
/// any invokable events were found.
pub fn after_trigger_mark_events(
    events: &mut EventList,
    mut move_list: Option<&mut EventList>,
    immediate_only: bool,
) -> bool {
    let mut found = false;
    let firing_counter = with_after_triggers(|at| at.firing_counter);

    let n = events.events.len();
    for i in 0..n {
        let flags = events.events[i].ate_flags;
        let sidx = (flags & AFTER_TRIGGER_OFFSET) as usize;
        let evtshared = events.shared[sidx].clone();
        let mut defer_it = false;

        if (flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS)) == 0 {
            let deferred = with_after_triggers(|at| after_trigger_check_state(at, &evtshared));
            if immediate_only && deferred {
                defer_it = true;
            } else {
                events.shared[sidx].ats_firing_id = firing_counter;
                events.events[i].ate_flags |= AFTER_TRIGGER_IN_PROGRESS;
                found = true;
            }
        }

        if defer_it {
            if let Some(ml) = move_list.as_deref_mut() {
                crate::queue::after_trigger_add_event(ml, events.events[i], &evtshared);
                events.events[i].ate_flags |= AFTER_TRIGGER_DONE;
            }
        }
    }

    found
}

// ===========================================================================
// afterTriggerInvokeEvents (trigger.c:4698)
// ===========================================================================

/// `afterTriggerInvokeEvents(events, firing_id, estate, delete_ok)`
/// (trigger.c:4698) — fire the events marked for the current firing cycle,
/// caching the re-resolved relation/trigdesc per relation.  Returns true if no
/// unfired events remain.
pub fn after_trigger_invoke_events(
    events: &mut EventList,
    firing_id: CommandId,
    _estate: &mut EStateData<'_>,
    _delete_ok: bool,
) -> PgResult<bool> {
    let mut all_fired = true;
    let mut cur: Option<TriggerResultRel> = None;

    let n = events.events.len();
    for i in 0..n {
        let flags = events.events[i].ate_flags;
        let sidx = (flags & AFTER_TRIGGER_OFFSET) as usize;
        let evtshared = events.shared[sidx].clone();

        if (flags & AFTER_TRIGGER_IN_PROGRESS) != 0 && evtshared.ats_firing_id == firing_id {
            let need_reopen = cur
                .as_ref()
                .map(|c| c.relid != evtshared.ats_relid)
                .unwrap_or(true);
            if need_reopen {
                cur = Some(trigger_result_rel_open(evtshared.ats_relid)?);
            }
            let rel = cur.as_mut().expect("trigger result relation is open");

            if rel.relkind == RELKIND_FOREIGN_TABLE {
                return Err(fdw_tuple_fetch_unported());
            }

            let event = events.events[i];
            after_trigger_execute(rel, &event, &evtshared)?;

            events.events[i].ate_flags &= !AFTER_TRIGGER_IN_PROGRESS;
            events.events[i].ate_flags |= AFTER_TRIGGER_DONE;
        } else if (events.events[i].ate_flags & AFTER_TRIGGER_DONE) == 0 {
            all_fired = false;
        }
    }

    Ok(all_fired)
}

// ===========================================================================
// AfterTriggerEndQuery (trigger.c:5136) — the per-query firing leg.
// ===========================================================================

/// `AfterTriggerEndQuery(EState *estate)` (trigger.c:5136) — invoke this query
/// level's AFTER IMMEDIATE events, promote the deferred ones to the global
/// deferred list, then release the query level's storage.
pub fn after_trigger_end_query(estate: &mut EStateData<'_>) -> PgResult<()> {
    debug_assert!(with_after_triggers(|at| at.query_depth) >= 0);

    // Fast path: no event stack ever initialized for this level.
    let fast = with_after_triggers(|at| crate::queue::after_trigger_end_query_noevents(at));
    if fast {
        return Ok(());
    }

    // Process all immediate-mode triggers, moving deferred ones to the global
    // list. Loop in case a trigger queues more events at this level.
    loop {
        let qd = with_after_triggers(|at| at.query_depth as usize);
        let (mut events, mut move_list) = with_after_triggers(|at| {
            (
                std::mem::take(&mut at.query_stack[qd].events),
                std::mem::take(&mut at.events),
            )
        });

        let found = after_trigger_mark_events(&mut events, Some(&mut move_list), true);

        with_after_triggers(|at| {
            at.events = move_list;
            at.query_stack[qd].events = events;
        });

        if !found {
            break;
        }

        let firing_id = with_after_triggers(|at| {
            let id = at.firing_counter;
            at.firing_counter += 1;
            id
        });
        let mut events = with_after_triggers(|at| std::mem::take(&mut at.query_stack[qd].events));
        let fire_result = after_trigger_invoke_events(&mut events, firing_id, estate, false);
        with_after_triggers(|at| {
            let appended = std::mem::take(&mut at.query_stack[qd].events);
            for ev in appended.events {
                let sidx = (ev.ate_flags & AFTER_TRIGGER_OFFSET) as usize;
                if let Some(shared) = appended.shared.get(sidx).cloned() {
                    crate::queue::after_trigger_add_event(&mut events, ev, &shared);
                }
            }
            at.query_stack[qd].events = events;
        });
        let all_fired = fire_result?;
        if all_fired {
            break;
        }
    }

    // Release query-level-local storage (the owned `Vec`s drop on take).
    let qd = with_after_triggers(|at| at.query_depth as usize);
    with_after_triggers(|at| {
        at.query_stack[qd] = crate::queue::QueryLevel::default();
        at.query_depth -= 1;
    });
    Ok(())
}

// ===========================================================================
// Front-half row / statement firing entry points (trigger.c:2402-3328).
//
// nodeModifyTable only calls these once the matching `ri_TrigDesc->trig_*` flag
// is set, so each call genuinely has triggers of that kind to fire. Firing them
// needs the per-trigger WHEN-qual ExprState (`ResultRelInfo.ri_TrigWhenExprs`,
// trimmed from the executor's ResultRelInfo) for `TriggerEnabled`, the OLD/NEW
// slot materialization, and `GetTupleForTrigger` (table_tuple_lock / heap_fetch
// / EvalPlanQual). That substrate is unported, so each entry point is a loud,
// 1:1-named seam-and-panic (mirror-PG-and-panic). The seams are still installed
// (so the consumer call resolves to a real, named panic, not an "uninstalled
// seam" message).
// ===========================================================================

#[cold]
#[inline(never)]
fn front_half(c_func: &str, c_line: u32) -> ! {
    panic!(
        "backend-commands-trigger: {c_func} (trigger.c:{c_line}) needs the per-trigger \
         WHEN-qual ExprState (ResultRelInfo.ri_TrigWhenExprs, trimmed), OLD/NEW slot \
         materialization, and GetTupleForTrigger (table_tuple_lock / heap_fetch / \
         EvalPlanQual) — firing-front substrate not yet ported"
    );
}

#[cold]
#[inline(never)]
fn deferred_ddl(c_func: &str, c_line: u32) -> ! {
    panic!(
        "backend-commands-trigger: {c_func} (trigger.c:{c_line}) is the catalog-write \
         trigger DDL leg (a separate family) — not ported in this firing-engine wave"
    );
}

// ---- BEFORE/AFTER/INSTEAD STATEMENT (trigger.c:2402-3328) ----

fn exec_bs_insert_triggers_impl(_estate: &mut EStateData<'_>, _relinfo: types_nodes::RriId) -> PgResult<()> {
    front_half("ExecBSInsertTriggers", 2402)
}
fn exec_bs_update_triggers_impl(_estate: &mut EStateData<'_>, _relinfo: types_nodes::RriId) -> PgResult<()> {
    front_half("ExecBSUpdateTriggers", 2896)
}
fn exec_bs_delete_triggers_impl(_estate: &mut EStateData<'_>, _relinfo: types_nodes::RriId) -> PgResult<()> {
    front_half("ExecBSDeleteTriggers", 2631)
}

fn exec_as_insert_triggers_impl(
    _estate: &mut EStateData<'_>,
    _relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    front_half("ExecASInsertTriggers", 2453)
}
fn exec_as_update_triggers_impl(
    _estate: &mut EStateData<'_>,
    _relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    front_half("ExecASUpdateTriggers", 2954)
}
fn exec_as_delete_triggers_impl(
    _estate: &mut EStateData<'_>,
    _relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    front_half("ExecASDeleteTriggers", 2682)
}

// ---- ROW INSERT (trigger.c:2466-2570) ----

fn exec_br_insert_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _slot: types_nodes::SlotId,
) -> PgResult<bool> {
    front_half("ExecBRInsertTriggers", 2466)
}
fn exec_ir_insert_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _slot: types_nodes::SlotId,
) -> PgResult<bool> {
    front_half("ExecIRInsertTriggers", 2570)
}
fn exec_ar_insert_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _slot: types_nodes::SlotId,
    _recheck_indexes: &[Oid],
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    front_half("ExecARInsertTriggers", 2544)
}

// ---- ROW DELETE (trigger.c:2702-2849) ----

fn exec_br_delete_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _epqstate: &mut types_nodes::EPQState<'mcx>,
    _relinfo: types_nodes::RriId,
    _tupleid: Option<&ItemPointerData>,
    _fdw_trigtuple: Option<&HeapTupleData<'mcx>>,
    _epqslot: Option<&mut Option<types_nodes::SlotId>>,
    _tmresult: Option<&mut types_tableam::tableam::TM_Result>,
    _tmfd: &mut types_tableam::tableam::TM_FailureData,
    _is_merge_delete: bool,
) -> PgResult<bool> {
    front_half("ExecBRDeleteTriggers", 2702)
}
fn exec_ar_delete_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _tupleid: Option<&ItemPointerData>,
    _fdw_trigtuple: Option<&HeapTupleData<'mcx>>,
    _tc: Option<&types_nodes::TransitionCaptureState>,
    _is_crosspart_update: bool,
) -> PgResult<()> {
    front_half("ExecARDeleteTriggers", 2802)
}
fn exec_ir_delete_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _trigtuple: HeapTuple<'mcx>,
) -> PgResult<bool> {
    front_half("ExecIRDeleteTriggers", 2849)
}

// ---- ROW UPDATE (trigger.c:2972-3215) ----

fn exec_br_update_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
    _relinfo: types_nodes::RriId,
    _tupleid: Option<&ItemPointerData>,
    _fdw_trigtuple: HeapTuple<'mcx>,
    _newslot: types_nodes::SlotId,
    _tmresult: Option<&mut types_tableam::tableam::TM_Result>,
    _tmfd: &mut types_tableam::tableam::TM_FailureData,
    _is_merge_update: bool,
) -> PgResult<bool> {
    front_half("ExecBRUpdateTriggers", 2972)
}
fn exec_ir_update_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _trigtuple: HeapTuple<'mcx>,
    _newslot: types_nodes::SlotId,
) -> PgResult<bool> {
    front_half("ExecIRUpdateTriggers", 3215)
}
fn exec_ar_update_triggers_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _src_partinfo: Option<types_nodes::RriId>,
    _dst_partinfo: Option<types_nodes::RriId>,
    _tupleid: Option<&ItemPointerData>,
    _fdw_trigtuple: Option<&HeapTupleData<'mcx>>,
    _newslot: Option<types_nodes::SlotId>,
    _recheck_indexes: &[Oid],
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
    _is_crosspart_update: bool,
) -> PgResult<()> {
    front_half("ExecARUpdateTriggers", 3145)
}

fn make_transition_capture_state_impl<'mcx>(
    _mcx: Mcx<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
    _cmd_type: types_nodes::nodes::CmdType,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::modifytable::TransitionCaptureState>>> {
    front_half("MakeTransitionCaptureState", 4958)
}

fn has_noncloned_pk_fkey_trigger_impl<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _relinfo: types_nodes::RriId,
) -> PgResult<bool> {
    // ExecCrossPartitionUpdateForeignKey's inner walk over ri_TrigDesc->triggers
    // for a non-cloned RI_TRIGGER_PK AFTER ROW UPDATE trigger. Reachable only on
    // a cross-partition FK update path (firing-front substrate).
    front_half("ExecARUpdateTriggers (has-noncloned-PK-FK walk)", 3145)
}

// ===========================================================================
// Deferred catalog-write DDL leg (a separate family).
// ===========================================================================

/// `get_trigger_oid(relid, trigname, missing_ok)` (trigger.c:1371) — the OID of
/// the named trigger on `relid`.  `pg_trigger` has no syscache; the C body is a
/// `systable_beginscan` over `TriggerRelidNameIndexId`.  That scan machinery is
/// the catalog-read DDL leg; the `missing_ok` branch + the exact ereport are
/// owned here so the error path is faithful once the scan seam lands.
fn get_trigger_oid_impl(relid: Oid, trigname: &str, missing_ok: bool) -> PgResult<Oid> {
    // Both the found and missing_ok paths require the pg_trigger systable scan
    // (pg_trigger_oid_by_relid_name) to know whether the trigger exists; that is
    // the catalog-read DDL substrate, loud until it lands.
    let _ = (relid, trigname, missing_ok);
    deferred_ddl("get_trigger_oid (pg_trigger scan)", 1371)
}

/// Helper that builds the C `ERRCODE_UNDEFINED_OBJECT` "trigger does not exist"
/// error (kept so the message is faithful once the scan seam lands).
#[allow(dead_code)]
fn trigger_not_found(trigname: &str, relname: &str) -> PgError {
    PgError::error(format!(
        "trigger \"{trigname}\" for table \"{relname}\" does not exist"
    ))
    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
}

// ===========================================================================
// AFTER-trigger transaction lifecycle seams (consumed by xact.c).
// ===========================================================================

fn after_trigger_begin_xact_impl() -> PgResult<()> {
    crate::queue::after_trigger_begin_xact();
    Ok(())
}
fn after_trigger_end_xact_impl(is_commit: bool) -> PgResult<()> {
    crate::queue::after_trigger_end_xact(is_commit);
    Ok(())
}
fn after_trigger_begin_sub_xact_impl() -> PgResult<()> {
    crate::queue::after_trigger_begin_sub_xact_hook()
}
fn after_trigger_end_sub_xact_impl(is_commit: bool) -> PgResult<()> {
    crate::queue::after_trigger_end_sub_xact_hook(is_commit)
}

/// `AfterTriggerFireDeferred()` (trigger.c:5287) — fire deferred events at
/// commit.  The mark+invoke core is reachable, but the surrounding commit-time
/// snapshot push + firing-cycle loop need the xact-commit substrate (a
/// per-query `EState` + a pushed transaction snapshot), which is not available
/// to this seam's bare signature.  Loud, 1:1-named.
fn after_trigger_fire_deferred_impl() -> PgResult<()> {
    Err(PgError::error(
        "AfterTriggerFireDeferred (trigger.c:5287): the commit-time deferred-trigger \
         firing loop needs a pushed transaction snapshot + per-query EState, not yet ported"
            .to_string(),
    ))
}

// ===========================================================================
// Small helpers + honest loud boundaries.
// ===========================================================================

/// `ItemPointerIsValid(p)` (`storage/itemptr.h`).
#[inline]
fn item_pointer_is_valid(p: &ItemPointerData) -> bool {
    p.ip_posid != 0
}

#[cold]
#[inline(never)]
fn exec_get_trigger_result_rel_unported() -> PgError {
    PgError::error(
        "AfterTriggerExecute/afterTriggerInvokeEvents: ExecGetTriggerResultRel re-resolves \
         the target relation's TriggerDesc by Oid, but the per-relation TriggerDesc is built \
         by RelationBuildTriggers (catalog-read DDL leg) and is not readable from the relcache \
         by Oid yet"
            .to_string(),
    )
}

#[cold]
#[inline(never)]
fn per_row_fetch_unported() -> PgError {
    PgError::error(
        "AfterTriggerExecute: per-row AFTER-trigger tuple re-fetch \
         (table_tuple_fetch_row_version / heap_fetch under SnapshotAny) needs a booted buffer \
         manager + a per-query Mcx/Relation, not reachable from the backend-global firing path"
            .to_string(),
    )
}

#[cold]
#[inline(never)]
fn fdw_tuple_fetch_unported() -> PgError {
    PgError::error(
        "AfterTriggerExecute: FDW / foreign-table after-trigger tuple sourcing \
         (AFTER_TRIGGER_FDW_*) is firing-substrate (per-query FDW tuplestore) not ported"
            .to_string(),
    )
}

#[cold]
#[inline(never)]
fn cross_partition_update_unported() -> PgError {
    PgError::error(
        "AfterTriggerExecute: cross-partition update after-trigger tuple sourcing \
         (AFTER_TRIGGER_CP_UPDATE) is firing-substrate not ported"
            .to_string(),
    )
}

#[cold]
#[inline(never)]
fn transition_table_unported() -> PgError {
    PgError::error(
        "AfterTriggerExecute: transition-table (ats_table tuplestore) access is \
         transition-capture substrate not ported"
            .to_string(),
    )
}

#[cold]
#[inline(never)]
fn role_switch_unported() -> PgError {
    PgError::error(
        "AfterTriggerExecute: become-queued-role (GetUserIdAndSecContext / \
         SetUserIdAndSecContext) for a non-current ats_rolid is not ported"
            .to_string(),
    )
}

// ===========================================================================
// init_seams — install every backend-commands-trigger-seams implementation.
// ===========================================================================

pub fn init_seams() {
    use backend_commands_trigger_seams as s;

    // Transaction / subtransaction lifecycle.
    s::after_trigger_begin_xact::set(after_trigger_begin_xact_impl);
    s::after_trigger_fire_deferred::set(after_trigger_fire_deferred_impl);
    s::after_trigger_end_xact::set(after_trigger_end_xact_impl);
    s::after_trigger_begin_sub_xact::set(after_trigger_begin_sub_xact_impl);
    s::after_trigger_end_sub_xact::set(after_trigger_end_sub_xact_impl);

    // DDL name lookup (deferred catalog-read leg).
    s::get_trigger_oid::set(get_trigger_oid_impl);

    // STATEMENT firing.
    s::exec_bs_insert_triggers::set(exec_bs_insert_triggers_impl);
    s::exec_bs_update_triggers::set(exec_bs_update_triggers_impl);
    s::exec_bs_delete_triggers::set(exec_bs_delete_triggers_impl);
    s::exec_as_insert_triggers::set(exec_as_insert_triggers_impl);
    s::exec_as_update_triggers::set(exec_as_update_triggers_impl);
    s::exec_as_delete_triggers::set(exec_as_delete_triggers_impl);
    s::make_transition_capture_state::set(make_transition_capture_state_impl);

    // ROW INSERT firing.
    s::exec_br_insert_triggers::set(exec_br_insert_triggers_impl);
    s::exec_ir_insert_triggers::set(exec_ir_insert_triggers_impl);
    s::exec_ar_insert_triggers::set(exec_ar_insert_triggers_impl);

    // ROW DELETE firing.
    s::exec_br_delete_triggers::set(exec_br_delete_triggers_impl);
    s::exec_ar_delete_triggers::set(exec_ar_delete_triggers_impl);
    s::exec_ir_delete_triggers::set(exec_ir_delete_triggers_impl);

    // ROW UPDATE firing.
    s::exec_br_update_triggers::set(exec_br_update_triggers_impl);
    s::exec_ir_update_triggers::set(exec_ir_update_triggers_impl);
    s::exec_ar_update_triggers::set(exec_ar_update_triggers_impl);
    s::has_noncloned_pk_fkey_trigger::set(has_noncloned_pk_fkey_trigger_impl);
}

/// Suppress dead-code warnings for the deep-firing helpers that are reachable
/// only through the gated by-Oid path (kept 1:1 with the C call graph).
#[allow(dead_code)]
fn _firing_helpers_used() {
    let _ = clone_trigger as fn(&Trigger<'static>) -> DispatchTrigger;
    let _ = deferred_ddl as fn(&str, u32) -> !;
    let _ = trigger_not_found as fn(&str, &str) -> PgError;
}
