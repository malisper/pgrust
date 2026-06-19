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
use types_storage::lock::{AccessExclusiveLock, NoLock};
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
    TRIGGER_EVENT_INSERT,
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

    /// The owner-side slot payloads (`tg_trigslot`/`tg_newslot`) of the
    /// currently-firing AFTER trigger.  In C these are real `TupleTableSlot`s
    /// owned by the firing `EState`; the idiomatic `TriggerData` carries only the
    /// opaque `TupleTableSlotRef` markers (`SLOT_TRIG`/`SLOT_NEW`), so the slot's
    /// re-fetched on-page tuple + the relation's tuple descriptor ride this
    /// side-channel, set/cleared together with [`CURRENT_TRIGGER_DATA`].  The
    /// `slot_*` accessors (`slot_getattr` / `slot_attisnull` / `slot_tid` /
    /// `slot_is_current_xact_tuple` / `pk_datum_image_eq`) resolve their marker
    /// to the matching tuple here and deform it against the descriptor — the
    /// owned analogue of `slot_getattr(slot, attnum, &isnull)`.
    static CURRENT_TRIGGER_SLOTS: RefCell<Option<CurrentTriggerSlots>> =
        const { RefCell::new(None) };
}

/// The re-fetched OLD/NEW slot tuples + the trigger relation, for the currently
/// firing AFTER trigger.  All values are allocated in the per-query context that
/// outlives the install/drop of this side-channel (see [`fetch_trigger_tuple`]),
/// so the `'static` markers are sound for the firing call's duration.
struct CurrentTriggerSlots {
    /// `trigdata->tg_relation` — the heap relation the trigger fired on.
    relation: types_rel::Relation<'static>,
    /// `trigdata->tg_trigslot` payload — the OLD tuple (DELETE/UPDATE) or, for
    /// INSERT, the inserted tuple.
    trigtuple: Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'static>>,
    /// `trigdata->tg_newslot` payload — the NEW tuple (UPDATE), or NULL.
    newtuple: Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'static>>,
}

/// RAII guard installing the per-call slot side-channel (paired with
/// [`CurrentTriggerGuard`]); restores the prior value on drop so recursive
/// trigger firing nests correctly.
struct CurrentSlotsGuard {
    prev: Option<CurrentTriggerSlots>,
}

impl CurrentSlotsGuard {
    fn install(slots: CurrentTriggerSlots) -> Self {
        let prev = CURRENT_TRIGGER_SLOTS.with(|cell| cell.borrow_mut().replace(slots));
        CurrentSlotsGuard { prev }
    }
}

impl Drop for CurrentSlotsGuard {
    fn drop(&mut self) {
        CURRENT_TRIGGER_SLOTS.with(|cell| *cell.borrow_mut() = self.prev.take());
    }
}

/// Run `f` with the slot payload (`FormedTuple`) the marker addresses, plus the
/// relation's tuple descriptor.  `None` when no slot side-channel is installed or
/// the addressed tuple is NULL (the C `TupIsNull(slot)` case).
fn with_slot_tuple<R>(
    marker: u64,
    f: impl FnOnce(
        &types_tuple::backend_access_common_heaptuple::FormedTuple<'static>,
        &types_rel::Relation<'static>,
    ) -> R,
) -> Option<R> {
    CURRENT_TRIGGER_SLOTS.with(|cell| {
        let b = cell.borrow();
        let s = b.as_ref()?;
        let tup = match marker {
            x if x == crate::ri_accessors::SLOT_TRIG => s.trigtuple.as_ref()?,
            x if x == crate::ri_accessors::SLOT_NEW => s.newtuple.as_ref()?,
            _ => return None,
        };
        Some(f(tup, &s.relation))
    })
}

// ---- slot value accessors (the owner side of the `slot_*` seams) -----------
//
// The RI procs read the OLD/NEW tuple values through `slot_getattr` /
// `slot_attisnull` / `slot_tid` / `slot_is_current_xact_tuple` /
// `pk_datum_image_eq` and the liveness test `tg_relation_tuple_satisfies_-
// snapshot_self`.  In C these dispatch through the slot's `tts_ops` vtable; here
// the slot payload is the re-fetched on-page tuple on [`CURRENT_TRIGGER_SLOTS`],
// deformed against the trigger relation's descriptor — the owned realization of
// the heap-slot vtable for the AFTER-trigger firing path.

/// `slot_attisnull(slot, attnum)` — null test of one attribute (no value copy;
/// consults the tuple's null bitmap against the relation descriptor).
pub fn slot_attisnull_impl(slot: types_ri_triggers::TupleTableSlotRef, attnum: i16) -> PgResult<bool> {
    let r = with_slot_tuple(slot.0, |tup, rel| {
        backend_access_common_heaptuple::heap_attisnull(&tup.tuple, attnum as i32, Some(&rel.rd_att))
    });
    r.ok_or_else(|| slot_no_payload("slot_attisnull"))
}

/// `slot_getattr(slot, attnum, &isnull)` — fetch one attribute's value + null
/// flag, deforming the slot's tuple against the relation descriptor.
pub fn slot_getattr_impl<'mcx>(
    mcx: Mcx<'mcx>,
    slot: types_ri_triggers::TupleTableSlotRef,
    attnum: i16,
) -> PgResult<(types_tuple::backend_access_common_heaptuple::Datum<'mcx>, bool)> {
    type SlotDatum<'a> = types_tuple::backend_access_common_heaptuple::Datum<'a>;
    let r = CURRENT_TRIGGER_SLOTS.with(|cell| -> PgResult<Option<(SlotDatum<'mcx>, bool)>> {
        let b = cell.borrow();
        let s = match b.as_ref() {
            Some(s) => s,
            None => return Ok(None),
        };
        let tup = match resolve_slot(s, slot.0) {
            Some(t) => t,
            None => return Ok(None),
        };
        let col =
            backend_access_common_heaptuple::heap_getattr(mcx, tup, attnum as i32, &s.relation.rd_att)?;
        Ok(Some((col.0, col.1)))
    })?;
    r.ok_or_else(|| slot_no_payload("slot_getattr"))
}

/// `slot->tts_tid` — the TID of the slot's tuple.
pub fn slot_tid_impl(slot: types_ri_triggers::TupleTableSlotRef) -> ItemPointerData {
    with_slot_tuple(slot.0, |tup, _rel| tup.tuple.t_self)
        .unwrap_or_else(|| panic!("slot_tid: no active slot payload"))
}

/// `slot_is_current_xact_tuple(slot)` — was the slot's tuple created/modified by
/// the current transaction? (`TransactionIdIsCurrentTransactionId(xmin)`.)
pub fn slot_is_current_xact_tuple_impl(slot: types_ri_triggers::TupleTableSlotRef) -> PgResult<bool> {
    let r = with_slot_tuple(slot.0, |tup, _rel| {
        let hdr = tup
            .tuple
            .t_data
            .as_ref()
            .expect("slot_is_current_xact_tuple: tuple has no t_data");
        let xmin = match &hdr.t_choice {
            types_tuple::heaptuple::HeapTupleHeaderChoice::THeap(f) => f.t_xmin,
            _ => 0,
        };
        backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xmin)
    });
    r.ok_or_else(|| slot_no_payload("slot_is_current_xact_tuple"))
}

/// `datum_image_eq(oldvalue, newvalue, attbyval, attlen)` for the PK side, with
/// `attbyval`/`attlen` read from the slot relation's compact descriptor.
pub fn pk_datum_image_eq_impl<'mcx>(
    slot: types_ri_triggers::TupleTableSlotRef,
    attnum: i16,
    oldvalue: &types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    newvalue: &types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> bool {
    let r = CURRENT_TRIGGER_SLOTS.with(|cell| -> Option<bool> {
        let b = cell.borrow();
        let s = b.as_ref()?;
        let _ = resolve_slot(s, slot.0)?;
        let ca = s.relation.rd_att.compact_attr((attnum as usize) - 1);
        backend_utils_adt_datum_seams::datum_image_eq_v::call(
            oldvalue,
            newvalue,
            ca.attbyval,
            ca.attlen,
        )
        .ok()
    });
    r.unwrap_or_else(|| panic!("pk_datum_image_eq: no active slot payload"))
}

/// `table_tuple_satisfies_snapshot(trigdata->tg_relation, slot, SnapshotSelf)` —
/// is the slot's re-fetched tuple live per `SnapshotSelf`?  Runs the ported
/// `HeapTupleSatisfiesVisibility(SNAPSHOT_SELF)` against the materialized tuple
/// (no buffer pin — the tuple is a query-context copy, so hint-bit writes are
/// skipped with `InvalidBuffer`).
pub fn tg_relation_tuple_satisfies_snapshot_self_impl(
    _trigdata: types_ri_triggers::TriggerDataRef,
    slot: types_ri_triggers::TupleTableSlotRef,
) -> PgResult<bool> {
    let r = CURRENT_TRIGGER_SLOTS.with(|cell| -> PgResult<Option<bool>> {
        let b = cell.borrow();
        let s = match b.as_ref() {
            Some(s) => s,
            None => return Ok(None),
        };
        let tup = match resolve_slot(s, slot.0) {
            Some(t) => t,
            None => return Ok(None),
        };
        let mut htup = tup.tuple.clone();
        let mut snap = types_snapshot::SnapshotData::sentinel(types_snapshot::SnapshotType::SNAPSHOT_SELF);
        let live = backend_access_heap_heapam_visibility::HeapTupleSatisfiesVisibility(
            &mut htup,
            &mut snap,
            types_storage::buf::InvalidBuffer,
        )?;
        Ok(Some(live))
    })?;
    r.ok_or_else(|| slot_no_payload("tg_relation_tuple_satisfies_snapshot_self"))
}

/// `trigdata->tg_relation` — the live relation, aliased into the caller's `mcx`
/// (the `unique_key_recheck` driver in `constraint.c` reads it to drive the
/// table-AM / index-AM; the RI procs use the OID-only accessors above).
pub fn tg_relation_impl<'mcx>(
    mcx: Mcx<'mcx>,
    _trigdata: types_ri_triggers::TriggerDataRef,
) -> PgResult<types_rel::Relation<'mcx>> {
    let r = CURRENT_TRIGGER_SLOTS.with(|cell| {
        cell.borrow().as_ref().map(|s| {
            // The side-channel relation is a query-context value; re-alias for the
            // caller's `mcx` (same query context).
            let aliased: types_rel::Relation<'static> = s.relation.alias();
            // SAFETY: `mcx` is the same per-query context the side-channel relation
            // was aliased from; narrowing the 'static marker back to 'mcx is sound.
            unsafe {
                core::mem::transmute::<types_rel::Relation<'static>, types_rel::Relation<'mcx>>(
                    aliased,
                )
            }
        })
    });
    let _ = mcx;
    r.ok_or_else(|| slot_no_payload("tg_relation"))
}

/// Resolve a slot marker to its [`FormedTuple`] payload within the installed
/// slot side-channel (`None` for an empty / unrecognized slot).
fn resolve_slot<'a>(
    s: &'a CurrentTriggerSlots,
    marker: u64,
) -> Option<&'a types_tuple::backend_access_common_heaptuple::FormedTuple<'static>> {
    match marker {
        x if x == crate::ri_accessors::SLOT_TRIG => s.trigtuple.as_ref(),
        x if x == crate::ri_accessors::SLOT_NEW => s.newtuple.as_ref(),
        _ => None,
    }
}

/// The protocol error when a slot accessor is called without a live slot payload
/// (the C `TupIsNull` / empty-slot violation).
fn slot_no_payload(what: &str) -> PgError {
    PgError::error(format!(
        "trigger manager slot accessor {what} called without an active slot payload"
    ))
    .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED)
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
pub struct TriggerResultRel<'mcx> {
    /// `RelationGetRelid(rel)` — the target relation's OID.
    pub relid: Oid,
    /// `rel->rd_rel->relkind`.
    pub relkind: i8,
    /// The opened trigger target relation (held `NoLock`; the row-event lock is
    /// retained from queue time).  Carried so `after_trigger_execute` can set
    /// `trigdata->tg_relation` and deform the re-fetched tuple against its
    /// descriptor — what the RI accessors read off `tg_relation`.
    pub relation: types_rel::Relation<'mcx>,
    /// `rInfo->ri_TrigDesc->triggers` — the relation's triggers, cloned into the
    /// per-query context so the live `Trigger` (carrying
    /// `tgconstraint`/`tgconstrrelid`/`tgconstrindid`, which the RI procs read)
    /// is available when building the firing `TriggerData`.
    pub triggers: Vec<mcx::PgBox<'mcx, Trigger<'mcx>>>,
}

/// `ExecGetTriggerResultRel(estate, relid, NULL)` for the firing path — open the
/// trigger target relation by Oid and read its trigger descriptor + relkind.
///
/// `ExecGetTriggerResultRel` opens the target relation (no new lock — the lock
/// is held from queue time) and reads its relcache `rd_trigdesc`, built by
/// `RelationBuildTriggers`. We `table_open(relid, NoLock)`, project the dispatch
/// facts off `rd_trigdesc->triggers`, and close (NoLock) keeping the lock,
/// mirroring the C cache-miss path.
fn trigger_result_rel_open(mcx: Mcx<'_>, relid: Oid) -> PgResult<TriggerResultRel<'_>> {
    let rel = backend_access_table_table_seams::table_open::call(mcx, relid, NoLock)?;
    let relkind = rel.rd_rel.relkind as i8;
    let mut triggers: Vec<mcx::PgBox<'_, Trigger<'_>>> = Vec::new();
    if let Some(td) = rel.rd_trigdesc.as_ref() {
        for t in td.triggers.iter() {
            let cloned = t.clone_in(mcx)?;
            triggers.push(mcx::PgBox::try_new_in(cloned, mcx).map_err(|_| mcx.oom(0))?);
        }
    }
    Ok(TriggerResultRel {
        relid,
        relkind,
        relation: rel,
        triggers,
    })
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
pub fn after_trigger_execute<'mcx>(
    mcx: Mcx<'mcx>,
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
    let tgfoid = rel.triggers[tgindx].tgfoid;
    let tgtype = rel.triggers[tgindx].tgtype;
    // `LocTriggerData.tg_trigger = &(trigdesc->triggers[tgindx])` — the live
    // trigger (carries tgconstraint/tgconstrrelid/tgconstrindid the RI procs
    // read), cloned into the per-query context.
    let trigger_box: mcx::PgBox<'static, Trigger<'static>> = {
        let cloned = rel.triggers[tgindx].clone_in(mcx)?;
        let boxed: mcx::PgBox<'mcx, Trigger<'mcx>> =
            mcx::PgBox::try_new_in(cloned, mcx).map_err(|_| mcx.oom(0))?;
        // SAFETY: allocated in `mcx` (= es_query_cxt); the side-channel that
        // borrows it is installed/dropped within this call.
        unsafe { core::mem::transmute(boxed) }
    };

    // Fetch the required tuple(s). FDW_FETCH/FDW_REUSE only arise for a genuine
    // FDW event on a foreign table (a regular-table event always sets at least
    // AFTER_TRIGGER_1CTID), so the FDW arm is gated on relkind.
    let mut trig_formed: Option<
        types_tuple::backend_access_common_heaptuple::FormedTuple<'static>,
    > = None;
    let mut new_formed: Option<
        types_tuple::backend_access_common_heaptuple::FormedTuple<'static>,
    > = None;
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
            trig_formed = Some(fetch_trigger_tuple(mcx, rel.relid, &event.ate_ctid1)?);
        }
        let has_ctid2 =
            tup_bits == AFTER_TRIGGER_2CTID || (event.ate_flags & AFTER_TRIGGER_CP_UPDATE) != 0;
        if has_ctid2 && item_pointer_is_valid(&event.ate_ctid2) {
            if (event.ate_flags & AFTER_TRIGGER_CP_UPDATE) != 0 {
                return Err(cross_partition_update_unported());
            }
            new_formed = Some(fetch_trigger_tuple(mcx, rel.relid, &event.ate_ctid2)?);
        }
    }

    // Transition tables (ats_table) and a non-current queued role are firing
    // substrate; the reachable queue never sets them, so a present value is a
    // genuine unported boundary.
    if evtshared.ats_has_table {
        return Err(transition_table_unported());
    }
    // GetUserIdAndSecContext(&save_rolid, ...); if (save_rolid != ats_rolid)
    // SetUserIdAndSecContext(...). The event was queued with ats_rolid =
    // GetUserId(); firing in the same session it equals the current user, so no
    // role switch is needed. A genuine mismatch (e.g. a deferred event fired
    // under a different role) needs SetUserIdAndSecContext, still unported.
    if evtshared.ats_rolid != backend_utils_init_miscinit::GetUserId() {
        return Err(role_switch_unported());
    }

    // `trigdata->tg_relation` — the live relation, aliased (refcount bump) for
    // the duration of the call (the RI accessors read relname/relnamespace/attrs
    // + the tuple descriptor off it).
    let tg_relation: types_rel::Relation<'static> = {
        let aliased = rel.relation.alias();
        // SAFETY: same query-context lifetime extension as the slot payloads.
        unsafe { core::mem::transmute(aliased) }
    };

    // The HeapTuple views (`tg_trigtuple`/`tg_newtuple`) for the TriggerData; the
    // matching `FormedTuple`s (header + data) ride the slot side-channel so the
    // `slot_*` accessors can deform them.
    let tg_trigtuple: HeapTuple<'static> = match &trig_formed {
        Some(f) => {
            let copied: HeapTupleData<'mcx> = f.tuple.clone_in(mcx)?;
            let boxed: mcx::PgBox<'mcx, HeapTupleData<'mcx>> =
                mcx::PgBox::try_new_in(copied, mcx).map_err(|_| mcx.oom(0))?;
            // SAFETY: query-context lifetime extension, as above.
            Some(unsafe { core::mem::transmute(boxed) })
        }
        None => None,
    };
    let tg_newtuple: HeapTuple<'static> = match &new_formed {
        Some(f) => {
            let copied: HeapTupleData<'mcx> = f.tuple.clone_in(mcx)?;
            let boxed: mcx::PgBox<'mcx, HeapTupleData<'mcx>> =
                mcx::PgBox::try_new_in(copied, mcx).map_err(|_| mcx.oom(0))?;
            Some(unsafe { core::mem::transmute(boxed) })
        }
        None => None,
    };
    let tg_trigslot = trig_formed
        .as_ref()
        .map(|_| types_nodes::SlotId(crate::ri_accessors::SLOT_TRIG as u32));
    let tg_newslot = new_formed
        .as_ref()
        .map(|_| types_nodes::SlotId(crate::ri_accessors::SLOT_NEW as u32));

    // Build the TriggerData and call the trigger; an AFTER trigger's return is
    // ignored.
    let tg_event = evtshared.ats_event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW);
    let _ = trigger_for_update(tgtype); // tg_updatedcols (bitmap) gated below
    let _ = (tgfoid, tgtype);
    let trigdata = TriggerData {
        type_: T_TriggerData,
        tg_event,
        tg_relation: Some(tg_relation),
        tg_trigtuple,
        tg_newtuple,
        tg_trigger: Some(trigger_box),
        tg_trigslot,
        tg_newslot,
        tg_oldtable: None,
        tg_newtable: None,
        tg_updatedcols: None,
    };

    // Install the slot side-channel (the FormedTuples + the relation for the
    // descriptor) for the call's duration, paired with the TriggerData channel.
    let slots_relation: types_rel::Relation<'static> = {
        let aliased = rel.relation.alias();
        // SAFETY: query-context lifetime extension, as above.
        unsafe { core::mem::transmute(aliased) }
    };
    let _slots_guard = CurrentSlotsGuard::install(CurrentTriggerSlots {
        relation: slots_relation,
        trigtuple: trig_formed,
        newtuple: new_formed,
    });

    let _rettuple = exec_call_trigger_func(trigdata)?;
    Ok(())
}

/// `table_tuple_fetch_row_version(rel, ctid, SnapshotAny, slot)` +
/// `ExecFetchSlotHeapTuple` for the trigger re-fetch — re-resolve the relation
/// by Oid and run the ported `heap_fetch` under `SnapshotAny` (AFTER triggers
/// see the tuple regardless of visibility), then materialize the on-page tuple
/// into the query context. A failed fetch is the C `elog(ERROR, "failed to fetch
/// tuple1 for AFTER trigger")`.
///
/// The returned `HeapTupleData` is deep-copied into `mcx` (the per-query
/// `es_query_cxt`) and the pinned buffer is released, so no buffer pin escapes.
/// The `'static` lifetime on the result is an extension of that `'mcx`
/// allocation: the side-channel `TriggerData` is installed and dropped strictly
/// within `after_trigger_execute`, which runs inside the same query context, so
/// the tuple outlives every read of it. (The owned-events model stores the queue
/// as a `'static` backend-global; this is the documented boundary where a
/// per-query tuple re-enters that path.)
fn fetch_trigger_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    ctid: &ItemPointerData,
) -> PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'static>> {
    let rel = backend_access_table_table_seams::table_open::call(mcx, relid, NoLock)?;
    let snapshot_any =
        types_snapshot::SnapshotData::sentinel(types_snapshot::SnapshotType::SNAPSHOT_ANY);

    let fetched =
        backend_access_heap_heapam_seams::heap_fetch::call(mcx, &rel, &snapshot_any, *ctid, false)?;

    let result = if fetched.found {
        // ExecFetchSlotHeapTuple: the on-page tuple (header + user data area),
        // deep-copied into mcx so it survives the buffer release below.
        let formed = fetched
            .tuple
            .expect("heap_fetch found==true must carry the tuple");
        let copied = formed.clone_in(mcx)?;
        // SAFETY: `copied` is allocated in `mcx` (= estate.es_query_cxt). The
        // side-channel slot payload that borrows this tuple is installed and
        // dropped within the enclosing `after_trigger_execute` call, which runs
        // inside the same query context, so the data outlives all reads.
        let extended: types_tuple::backend_access_common_heaptuple::FormedTuple<'static> =
            unsafe { core::mem::transmute(copied) };
        Ok(extended)
    } else {
        Err(PgError::error(
            "failed to fetch tuple1 for AFTER trigger".to_string(),
        ))
    };

    // ReleaseBuffer(userbuf) — drop the pin heap_fetch left on success.
    if types_storage::buf::BufferIsValid(fetched.userbuf) {
        backend_storage_buffer_bufmgr_seams::release_buffer::call(fetched.userbuf);
    }
    rel.close(NoLock)?;
    result
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
    estate: &mut EStateData<'_>,
    _delete_ok: bool,
) -> PgResult<bool> {
    let mut all_fired = true;
    let mut cur: Option<TriggerResultRel> = None;
    let mcx = estate.es_query_cxt;

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
                cur = Some(trigger_result_rel_open(mcx, evtshared.ats_relid)?);
            }
            let rel = cur.as_mut().expect("trigger result relation is open");

            if rel.relkind == RELKIND_FOREIGN_TABLE {
                return Err(fdw_tuple_fetch_unported());
            }

            let event = events.events[i];
            after_trigger_execute(mcx, rel, &event, &evtshared)?;

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

/// `trigdesc == NULL || !trigdesc->trig_<which>_before_statement` — whether a
/// BEFORE STATEMENT entry point has no work (the C `if (trigdesc == NULL)
/// return; if (!trig_..._before_statement) return;`). The trimmed
/// `ResultRelInfo` carries the per-statement trigger flags.
fn bs_trigger_flag(
    estate: &EStateData<'_>,
    relinfo: types_nodes::RriId,
    pick: fn(&types_trigger::TriggerDesc<'_>) -> bool,
) -> bool {
    estate
        .result_rel(relinfo)
        .ri_TrigDesc
        .as_ref()
        .is_some_and(|td| pick(td))
}

fn exec_bs_insert_triggers_impl(estate: &mut EStateData<'_>, relinfo: types_nodes::RriId) -> PgResult<()> {
    // if (trigdesc == NULL) return; if (!trig_insert_before_statement) return;
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_insert_before_statement) {
        return Ok(());
    }
    front_half("ExecBSInsertTriggers", 2402)
}
fn exec_bs_update_triggers_impl(estate: &mut EStateData<'_>, relinfo: types_nodes::RriId) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_update_before_statement) {
        return Ok(());
    }
    front_half("ExecBSUpdateTriggers", 2896)
}
fn exec_bs_delete_triggers_impl(estate: &mut EStateData<'_>, relinfo: types_nodes::RriId) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_delete_before_statement) {
        return Ok(());
    }
    front_half("ExecBSDeleteTriggers", 2631)
}

fn exec_as_insert_triggers_impl(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    // if (trigdesc && trig_insert_after_statement) AfterTriggerSaveEvent(...);
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_insert_after_statement) {
        return Ok(());
    }
    front_half("ExecASInsertTriggers", 2453)
}
fn exec_as_update_triggers_impl(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_update_after_statement) {
        return Ok(());
    }
    front_half("ExecASUpdateTriggers", 2954)
}
fn exec_as_delete_triggers_impl(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_delete_after_statement) {
        return Ok(());
    }
    front_half("ExecASDeleteTriggers", 2682)
}

// ---- TRUNCATE STATEMENT (tablecmds.c ExecuteTruncateGuts + trigger.c) ----

/// `AfterTriggerBeginQuery() + CreateExecutorState() + InitResultRelInfo() per
/// rel + ExecBSTruncateTriggers()` (tablecmds.c:2090-2136 / trigger.c:3281).
///
/// Coarse seam: the EState / ResultRelInfo machinery lives in the C caller, so
/// the whole BEFORE-STATEMENT-TRUNCATE block crosses by relids. We open each
/// relation (already locked AccessExclusiveLock by the caller; the relcache
/// entry is hot), and consult its `rd_trigdesc`. `ExecBSTruncateTriggers`
/// early-returns when `trigdesc == NULL || !trig_truncate_before_statement`
/// (trigger.c:3289-3292) — the no-trigger common case is a faithful no-op. A
/// rel that actually carries a BEFORE STATEMENT TRUNCATE trigger needs the
/// per-trigger firing substrate (`front_half`), still unported.
fn exec_truncate_fire_before_triggers_impl(
    mcx: Mcx<'_>,
    relids: &[Oid],
    _run_as_table_owner: bool,
) -> PgResult<()> {
    for &relid in relids {
        // C holds AccessExclusiveLock from the caller's table_open; re-open to
        // read the relcache TriggerDesc, then release our extra refcount but
        // keep the lock (NoLock close, as the caller does).
        let rel =
            backend_access_table_table_seams::table_open::call(mcx, relid, AccessExclusiveLock)?;
        let fires = rel
            .rd_trigdesc
            .as_ref()
            .is_some_and(|td| td.trig_truncate_before_statement);
        rel.close(NoLock)?;
        if fires {
            front_half("ExecBSTruncateTriggers", 3281);
        }
    }
    Ok(())
}

/// `ExecASTruncateTriggers() per rel + AfterTriggerEndQuery() +
/// FreeExecutorState()` (tablecmds.c:2334-2352 / trigger.c:3327).
///
/// `ExecASTruncateTriggers` only queues an AFTER event when
/// `trigdesc && trig_truncate_after_statement` (trigger.c:3332); otherwise it
/// is a no-op. With no truncate triggers there is nothing to queue, so the
/// `AfterTriggerBeginQuery`/`AfterTriggerEndQuery` bracket and the EState are
/// pure overhead — a faithful no-op. A rel carrying an AFTER STATEMENT
/// TRUNCATE trigger needs `AfterTriggerSaveEvent`, still unported.
fn exec_truncate_fire_after_triggers_impl(
    mcx: Mcx<'_>,
    relids: &[Oid],
    _run_as_table_owner: bool,
) -> PgResult<()> {
    for &relid in relids {
        let rel =
            backend_access_table_table_seams::table_open::call(mcx, relid, AccessExclusiveLock)?;
        let fires = rel
            .rd_trigdesc
            .as_ref()
            .is_some_and(|td| td.trig_truncate_after_statement);
        rel.close(NoLock)?;
        if fires {
            front_half("ExecASTruncateTriggers", 3327);
        }
    }
    Ok(())
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
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    slot: types_nodes::SlotId,
    recheck_indexes: &[Oid],
    tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    // The FDW + transition-capture guard reads ri_FdwRoutine (not carried on the
    // trimmed ResultRelInfo; ri_has_fdw_routine == false here) — skip.

    // if ((trigdesc && trig_insert_after_row) ||
    //     (transition_capture && tcs_insert_new_table)) AfterTriggerSaveEvent(...);
    let has_ar_row = estate
        .result_rel(relinfo)
        .ri_TrigDesc
        .as_ref()
        .is_some_and(|td| td.trig_insert_after_row);
    let tc_new_table = tc.as_ref().is_some_and(|t| t.tcs_insert_new_table);
    if !has_ar_row && !tc_new_table {
        return Ok(());
    }
    // Transition tables are firing substrate (GetAfterTriggersTransitionTable /
    // TransitionTableAddTuple); a present transition_capture means the
    // transition-table leg is needed, which is not ported.
    if tc.is_some() {
        return Err(transition_table_unported());
    }
    // AfterTriggerSaveEvent(estate, relinfo, NULL, NULL, TRIGGER_EVENT_INSERT,
    //                       true, NULL, slot, recheckIndexes, NULL, NULL, false);
    after_trigger_save_event(
        estate,
        relinfo,
        TRIGGER_EVENT_INSERT,
        /* row_trigger */ true,
        /* old_ctid */ None,
        /* newslot */ Some(slot),
        recheck_indexes,
    )
}

// ===========================================================================
// AfterTriggerSaveEvent (trigger.c:4925) — queue the after-trigger events.
//
// Ported for the reachable INSERT/DELETE/UPDATE *row* path on a regular
// (non-FDW, non-partitioned) table with no transition tables. The
// transition-capture, FDW-tuplestore, cross-partition root-conversion, and
// statement-level (cancel_prior_stmt_triggers) legs are firing substrate and
// loud-guarded by the callers / below.
// ===========================================================================

/// `AfterTriggerSaveEvent(...)` (trigger.c:4925) for the regular-table row path.
///
/// The big C signature collapses: `src_partinfo`/`dst_partinfo` (cross-partition
/// update), `oldslot`/`modifiedCols` (UPDATE/DELETE column set), and
/// `transition_capture` are not threaded on this reachable INSERT-row leg (the
/// caller guards transition capture). `recheck_indexes` is needed for the
/// deferred-unique-constraint skip (`F_UNIQUE_KEY_RECHECK`).
fn after_trigger_save_event<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    event: u32,
    row_trigger: bool,
    old_ctid: Option<ItemPointerData>,
    newslot: Option<types_nodes::SlotId>,
    recheck_indexes: &[Oid],
) -> PgResult<()> {
    use types_catalog::pg_trigger::{
        TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_MATCHES,
        TRIGGER_TYPE_ROW, TRIGGER_TYPE_UPDATE,
    };
    use types_nodes::trigger::{
        AFTER_TRIGGER_1CTID, AFTER_TRIGGER_2CTID, AFTER_TRIGGER_DEFERRABLE,
        AFTER_TRIGGER_INITDEFERRED,
    };
    const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;
    const TRIGGER_EVENT_DELETE: u32 = 1;
    const TRIGGER_EVENT_UPDATE: u32 = 2;

    // if (afterTriggers.query_depth < 0) elog(ERROR, "... outside of query");
    let query_depth = with_after_triggers(|at| at.query_depth);
    if query_depth < 0 {
        return Err(PgError::error(
            "AfterTriggerSaveEvent() called outside of query".to_string(),
        ));
    }
    // Be sure we have enough space to record events at this query depth.
    with_after_triggers(|at| crate::queue::after_trigger_enlarge_query_state(at));

    // relkind / relid of the target relation.
    let rel_oid = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_id)
        .expect("AfterTriggerSaveEvent: ResultRelInfo has no relation");
    let relkind = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_rel.relkind)
        .expect("AfterTriggerSaveEvent: ResultRelInfo has no relation");

    // The statement-level (no-tuple) and cross-partition save legs are not
    // reached here; the row-level INSERT/DELETE/UPDATE legs are ported.
    if !row_trigger {
        return Err(PgError::error(
            "AfterTriggerSaveEvent: statement-level event save not yet ported".to_string(),
        ));
    }
    if relkind as i8 == RELKIND_FOREIGN_TABLE {
        return Err(fdw_tuple_fetch_unported());
    }
    // The partitioned-table row event only arises on a cross-partition update
    // (loud-guarded at the front-half); a plain row event on a non-partitioned
    // table is the path here.
    if relkind as i8 == RELKIND_PARTITIONED_TABLE {
        return Err(cross_partition_update_unported());
    }

    // Validate the event code, collect the tuple CTID(s), and pick the event
    // bitmask + the AFTER_TRIGGER_{1,2}CTID flag (trigger.c:6280-6360).
    let (tgtype_event, ctid1, ctid2, tup_flag) = match event {
        TRIGGER_EVENT_INSERT => {
            // ItemPointerCopy(&newslot->tts_tid, &new_event.ate_ctid1);
            let ns = newslot.expect("AfterTriggerSaveEvent: INSERT row event needs a newslot");
            (
                TRIGGER_TYPE_INSERT,
                estate.slot(ns).tts_tid,
                ItemPointerData::default(),
                AFTER_TRIGGER_1CTID,
            )
        }
        TRIGGER_EVENT_DELETE => {
            // ItemPointerCopy(&oldslot->tts_tid, &new_event.ate_ctid1).  In the
            // owned model the OLD slot's tid is the delete's `tupleid` directly
            // (GetTupleForTrigger fetches that ctid into oldslot).
            let oc =
                old_ctid.expect("AfterTriggerSaveEvent: DELETE row event needs the old ctid");
            (
                TRIGGER_TYPE_DELETE,
                oc,
                ItemPointerData::default(),
                AFTER_TRIGGER_1CTID,
            )
        }
        TRIGGER_EVENT_UPDATE => {
            // ctid1 = oldslot->tts_tid (= the update's `tupleid`);
            // ctid2 = newslot->tts_tid; 2CTID.
            let oc =
                old_ctid.expect("AfterTriggerSaveEvent: UPDATE row event needs the old ctid");
            let ns = newslot.expect("AfterTriggerSaveEvent: UPDATE row event needs a newslot");
            (
                TRIGGER_TYPE_UPDATE,
                oc,
                estate.slot(ns).tts_tid,
                AFTER_TRIGGER_2CTID,
            )
        }
        other => {
            return Err(PgError::error(format!(
                "invalid after-trigger event code: {other}"
            )));
        }
    };
    let mut new_event = types_nodes::trigger::AfterTriggerEventData {
        ate_flags: tup_flag,
        ate_ctid1: ctid1,
        ate_ctid2: ctid2,
        ate_src_part: INVALID_OID,
        ate_dst_part: INVALID_OID,
    };

    let tgtype_level = TRIGGER_TYPE_ROW;
    let user_id = backend_utils_init_miscinit::GetUserId();
    let fired_by_upd_or_del =
        event == TRIGGER_EVENT_UPDATE || event == TRIGGER_EVENT_DELETE;

    // for (i = 0; i < trigdesc->numtriggers; i++) { ... afterTriggerAddEvent }
    // Collect the matching triggers first (immutable borrow of estate), then add
    // events into the query-level list.
    let trigs: Vec<(Oid, bool, bool, Oid, Oid)> = {
        let rri = estate.result_rel(relinfo);
        let trigdesc = rri
            .ri_TrigDesc
            .as_ref()
            .expect("AfterTriggerSaveEvent: ri_TrigDesc is NULL but event reached save");
        let mut out = Vec::new();
        for trig in trigdesc.triggers.iter() {
            if !TRIGGER_TYPE_MATCHES(trig.tgtype, tgtype_level, TRIGGER_TYPE_AFTER, tgtype_event) {
                continue;
            }
            // TriggerEnabled(estate, relinfo, trigger, event, ...). The WHEN-clause
            // (tgqual) leg compiles an ExprState (ExecPrepareQual / stringToNode),
            // which is the #159 plan-layer gate — keep it loud. The no-WHEN common
            // case is the replication-role / tgenabled check below.
            if trig.tgqual.is_some() {
                return Err(when_qual_unported());
            }
            if !trigger_enabled_no_qual(trig.tgenabled) {
                continue;
            }
            // FK-enforcement-trigger skip (trigger.c:6442). On UPDATE/DELETE,
            // RI_FKey_trigger_type classifies the trigger function; the PK/FK
            // `*_check_required` skips are *optimizations* that require the
            // old/new value slots (RI re-derives "no action" if we don't skip),
            // so we conservatively queue them here. The RI_TRIGGER_NONE arm only
            // skips on a partitioned table (excluded above), so it never skips.
            // The unique-key-recheck skip below is value-independent.
            let _ = fired_by_upd_or_del;
            // F_UNIQUE_KEY_RECHECK skip: queue only if the constraint's index is
            // in recheckIndexes (otherwise uniqueness was definitely not
            // violated). F_UNIQUE_KEY_RECHECK == 1250 (pg_proc.dat).
            const F_UNIQUE_KEY_RECHECK: Oid = 1250;
            if trig.tgfoid == F_UNIQUE_KEY_RECHECK
                && !recheck_indexes.contains(&trig.tgconstrindid)
            {
                continue;
            }
            out.push((
                trig.tgoid,
                trig.tgdeferrable,
                trig.tginitdeferred,
                trig.tgconstrindid,
                trig.tgfoid,
            ));
        }
        out
    };

    let qd = query_depth as usize;
    for (tgoid, tgdeferrable, tginitdeferred, _tgconstrindid, _tgfoid) in trigs {
        let new_shared = SharedRecord {
            ats_event: (event & TRIGGER_EVENT_OPMASK)
                | (if row_trigger { TRIGGER_EVENT_ROW } else { 0 })
                | (if tgdeferrable { AFTER_TRIGGER_DEFERRABLE } else { 0 })
                | (if tginitdeferred { AFTER_TRIGGER_INITDEFERRED } else { 0 }),
            ats_tgoid: tgoid,
            ats_relid: rel_oid,
            ats_rolid: user_id,
            ats_firing_id: 0,
            ats_modifiedcols: None,
            ats_has_table: false,
        };
        with_after_triggers(|at| {
            crate::queue::after_trigger_add_event(
                &mut at.query_stack[qd].events,
                new_event,
                &new_shared,
            );
        });
    }
    let _ = &mut new_event;
    Ok(())
}

/// The no-WHEN portion of `TriggerEnabled` (trigger.c): the replication-role /
/// `tgenabled` firing-control check. (The column-specific `tgattr` check only
/// fires for UPDATE; the WHEN `tgqual` leg is handled by the caller as the
/// #159-gated path.)
fn trigger_enabled_no_qual(tgenabled: i8) -> bool {
    use types_catalog::pg_trigger::{
        TRIGGER_DISABLED, TRIGGER_FIRES_ON_ORIGIN, TRIGGER_FIRES_ON_REPLICA,
    };
    // SessionReplicationRole: this port runs in the ORIGIN/LOCAL role (replica
    // apply is a separate path), so a TRIGGER_FIRES_ON_REPLICA / TRIGGER_DISABLED
    // trigger is skipped; ORIGIN/ALWAYS fires.
    if tgenabled == TRIGGER_FIRES_ON_REPLICA || tgenabled == TRIGGER_DISABLED {
        return false;
    }
    let _ = TRIGGER_FIRES_ON_ORIGIN;
    true
}

#[cold]
#[inline(never)]
fn when_qual_unported() -> PgError {
    PgError::error(
        "TriggerEnabled: a WHEN-clause (tgqual) trigger needs ExecPrepareQual / \
         stringToNode to compile the predicate into ri_TrigWhenExprs (the #159 \
         plan-layer expression gate) — not ported"
            .to_string(),
    )
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
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    tupleid: Option<&ItemPointerData>,
    fdw_trigtuple: Option<&HeapTupleData<'mcx>>,
    tc: Option<&types_nodes::TransitionCaptureState>,
    _is_crosspart_update: bool,
) -> PgResult<()> {
    // TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    let rri = estate.result_rel(relinfo);

    // if (relinfo->ri_FdwRoutine && transition_capture &&
    //     transition_capture->tcs_delete_old_table)
    //     ereport(ERROR, "cannot collect transition tuples from child foreign tables");
    if rri.ri_has_fdw_routine && tc.map(|t| t.tcs_delete_old_table).unwrap_or(false) {
        return Err(PgError::error(
            "cannot collect transition tuples from child foreign tables",
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // if ((trigdesc && trigdesc->trig_delete_after_row) ||
    //     (transition_capture && transition_capture->tcs_delete_old_table)) { ... fire ... }
    let after_row = rri
        .ri_TrigDesc
        .as_ref()
        .map(|td| td.trig_delete_after_row)
        .unwrap_or(false);
    let cap_old = tc.map(|t| t.tcs_delete_old_table).unwrap_or(false);
    if !(after_row || cap_old) {
        // No AFTER ROW DELETE trigger and no transition capture: nothing to do.
        return Ok(());
    }

    // Transition tables need the tuplestore firing substrate (not ported).
    if cap_old || tc.is_some() {
        return Err(transition_table_unported());
    }
    // The FDW-supplied tuple leg (ExecForceStoreHeapTuple) is only reached for a
    // foreign table; the on-disk delete passes the `tupleid` (the OLD slot's tid
    // after GetTupleForTrigger).  AFTER firing re-fetches that ctid by SnapshotAny.
    if fdw_trigtuple.is_some() {
        return Err(fdw_tuple_fetch_unported());
    }
    // Assert(HeapTupleIsValid(fdw_trigtuple) ^ ItemPointerIsValid(tupleid));
    const TRIGGER_EVENT_DELETE: u32 = 1;
    let old_ctid =
        tupleid.copied().expect("ExecARDeleteTriggers: a non-FDW delete needs a tupleid");
    // AfterTriggerSaveEvent(estate, relinfo, NULL, NULL, TRIGGER_EVENT_DELETE,
    //                       true, slot, NULL, NIL, NULL, transition_capture, false);
    after_trigger_save_event(
        estate,
        relinfo,
        TRIGGER_EVENT_DELETE,
        /* row_trigger */ true,
        /* old_ctid */ Some(old_ctid),
        /* newslot */ None,
        &[],
    )
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
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    src_partinfo: Option<types_nodes::RriId>,
    _dst_partinfo: Option<types_nodes::RriId>,
    tupleid: Option<&ItemPointerData>,
    fdw_trigtuple: Option<&HeapTupleData<'mcx>>,
    newslot: Option<types_nodes::SlotId>,
    recheck_indexes: &[Oid],
    tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
    is_crosspart_update: bool,
) -> PgResult<()> {
    let rri = estate.result_rel(relinfo);

    // if (relinfo->ri_FdwRoutine && transition_capture &&
    //     (tcs_update_old_table || tcs_update_new_table))
    //     ereport(ERROR, "cannot collect transition tuples from child foreign tables");
    let cap_old = tc.as_ref().map(|t| t.tcs_update_old_table).unwrap_or(false);
    let cap_new = tc.as_ref().map(|t| t.tcs_update_new_table).unwrap_or(false);
    if rri.ri_has_fdw_routine && (cap_old || cap_new) {
        return Err(PgError::error(
            "cannot collect transition tuples from child foreign tables",
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // if ((trigdesc && trigdesc->trig_update_after_row) ||
    //     (transition_capture && (tcs_update_old_table || tcs_update_new_table))) { ... fire ... }
    let after_row = rri
        .ri_TrigDesc
        .as_ref()
        .map(|td| td.trig_update_after_row)
        .unwrap_or(false);
    if !(after_row || cap_old || cap_new) {
        // No AFTER ROW UPDATE trigger and no transition capture: nothing to do.
        return Ok(());
    }

    // Transition tables / cross-partition routing need the firing substrate
    // (tuplestore + root-format conversion) — not ported.
    if cap_old || cap_new || tc.is_some() {
        return Err(transition_table_unported());
    }
    if is_crosspart_update || src_partinfo.is_some() {
        return Err(cross_partition_update_unported());
    }
    if fdw_trigtuple.is_some() {
        return Err(fdw_tuple_fetch_unported());
    }
    // tupsrc = src_partinfo ? src_partinfo : relinfo (= relinfo here). The OLD
    // slot's tid is the update's `tupleid`; the NEW slot is `newslot`.
    const TRIGGER_EVENT_UPDATE: u32 = 2;
    let old_ctid = match tupleid {
        Some(t) => Some(*t),
        // C `ExecClearTuple(oldslot)` (oldslot empty) only on the transition-only
        // routing leg, excluded above; a real UPDATE always has a tupleid.
        None => return Err(cross_partition_update_unported()),
    };
    let ns = newslot.expect("ExecARUpdateTriggers: a non-FDW update needs a newslot");
    // AfterTriggerSaveEvent(estate, relinfo, src_partinfo, dst_partinfo,
    //                       TRIGGER_EVENT_UPDATE, true, oldslot, newslot,
    //                       recheckIndexes, ExecGetAllUpdatedCols(...),
    //                       transition_capture, is_crosspart_update);
    after_trigger_save_event(
        estate,
        relinfo,
        TRIGGER_EVENT_UPDATE,
        /* row_trigger */ true,
        /* old_ctid */ old_ctid,
        /* newslot */ Some(ns),
        recheck_indexes,
    )
}

fn make_transition_capture_state_impl<'mcx>(
    _mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    cmd_type: types_nodes::nodes::CmdType,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::modifytable::TransitionCaptureState>>> {
    use types_nodes::nodes::CmdType;

    // if (trigdesc == NULL) return NULL;
    let trigdesc = match estate.result_rel(relinfo).ri_TrigDesc.as_ref() {
        Some(td) => td,
        None => return Ok(None),
    };

    // Detect which table(s) we need.
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
        _ => {
            return Err(PgError::error(format!(
                "unexpected CmdType: {}",
                cmd_type as i32
            )));
        }
    };

    // if (!need_old_upd && !need_new_upd && !need_new_ins && !need_old_del) return NULL;
    if !need_old_upd && !need_new_upd && !need_new_ins && !need_old_del {
        return Ok(None);
    }

    // A relation with transition-table triggers needs the after-trigger
    // query-state / tuplestore substrate (afterTriggers.query_depth,
    // AfterTriggersTableData, the (sub)transaction resource owner). That
    // firing-front substrate is not yet ported.
    front_half("MakeTransitionCaptureState (transition-table allocation)", 4958)
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

/// `AfterTriggerFireDeferred()` (trigger.c:5287) — fire all pending DEFERRED
/// triggers, called just before the current transaction commits.
///
/// ```c
/// Assert(afterTriggers.query_depth == -1);
/// events = &afterTriggers.events;
/// if (events->head != NULL) {
///     PushActiveSnapshot(GetTransactionSnapshot());
///     snap_pushed = true;
/// }
/// while (afterTriggerMarkEvents(events, NULL, false)) {
///     CommandId firing_id = afterTriggers.firing_counter++;
///     if (afterTriggerInvokeEvents(events, firing_id, NULL, true))
///         break;
/// }
/// if (snap_pushed) PopActiveSnapshot();
/// ```
///
/// The empty-queue fast path (`events->head == NULL`) is the common case — at
/// transaction commit with no deferred events queued this is a no-op, exactly
/// as in C. That path is ported faithfully. When the queue is *non-empty* the C
/// loop pushes the transaction snapshot and runs the firing cycle against a
/// `NULL` `EState`; in this tree `afterTriggerInvokeEvents` requires an
/// `&mut EStateData` and there is no `PushActiveSnapshot` seam reachable from
/// this crate, so the deferred-firing leg stays a loud, 1:1-named boundary until
/// the commit-time per-query `EState` + active-snapshot substrate lands. (A
/// fresh boot never queues deferred events, so it never reaches that leg.)
fn after_trigger_fire_deferred_impl() -> PgResult<()> {
    // Assert(afterTriggers.query_depth == -1) — must not be inside a query.
    debug_assert_eq!(with_after_triggers(|at| at.query_depth), -1);

    // events = &afterTriggers.events; if (events->head != NULL) ...
    let has_events = with_after_triggers(|at| !at.events.events.is_empty());
    if !has_events {
        // No deferred triggers to fire: the C function pushes no snapshot, the
        // mark loop finds nothing, and it returns. No-op.
        return Ok(());
    }

    // events->head != NULL: PushActiveSnapshot(GetTransactionSnapshot()) and run
    // the firing cycle. This commit-time leg needs the active-snapshot push and a
    // per-query EState that are not reachable from this crate's bare seam; keep
    // it loud rather than silently dropping queued deferred triggers.
    Err(PgError::error(
        "AfterTriggerFireDeferred (trigger.c:5287): firing queued DEFERRED triggers at \
         commit needs PushActiveSnapshot(GetTransactionSnapshot()) + a per-query EState for \
         the afterTriggerInvokeEvents cycle, not yet ported"
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

    // Per-query firing bracket (ExecutorStart / ExecutorFinish).
    s::after_trigger_begin_query::set(|| {
        crate::queue::after_trigger_begin_query();
        Ok(())
    });
    s::after_trigger_end_query::set(after_trigger_end_query);
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

    // STATEMENT TRUNCATE firing (coarse seams on tablecmds-seams; the EState /
    // ResultRelInfo machinery lives in the tablecmds caller).
    backend_commands_tablecmds_seams::exec_truncate_fire_before_triggers::set(
        exec_truncate_fire_before_triggers_impl,
    );
    backend_commands_tablecmds_seams::exec_truncate_fire_after_triggers::set(
        exec_truncate_fire_after_triggers_impl,
    );
}

/// Suppress dead-code warnings for the deep-firing helpers that are reachable
/// only through the gated by-Oid path (kept 1:1 with the C call graph).
#[allow(dead_code)]
fn _firing_helpers_used() {
    let _ = deferred_ddl as fn(&str, u32) -> !;
    let _ = trigger_not_found as fn(&str, &str) -> PgError;
}
