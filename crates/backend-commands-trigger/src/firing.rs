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
//! Reachable firing front (trigger.c:2466-2570):
//!   * [`exec_br_insert_triggers_impl`] (`ExecBRInsertTriggers`) /
//!     [`exec_ir_insert_triggers_impl`] (`ExecIRInsertTriggers`) — fire the
//!     BEFORE / INSTEAD-OF FOR EACH ROW INSERT triggers: dispatch via
//!     `TRIGGER_TYPE_MATCHES`, evaluate the WHEN qual via [`trigger_enabled`]
//!     (lazily compiling `ResultRelInfo.ri_TrigWhenExprs[i]` from `pg_trigger.tgqual`
//!     — `stringToNode` → `expand_generated_columns_in_expr` → OLD/NEW→INNER/OUTER
//!     `ChangeVarNodes` → `ExecPrepareQual`, then `ExecQual` against the NEW slot),
//!     materialize the NEW slot (`ExecFetchSlotHeapTuple`), build the
//!     [`TriggerData`], call the trigger via `fmgr`, and apply the returned tuple
//!     (`ExecForceStoreHeapTuple`) or signal "do nothing".  The returned row
//!     crosses back over the [BEFORE-trigger return-tuple channel](set_before_trigger_result_tuple_impl).
//!
//! Genuine-substrate boundaries (loud, 1:1-named panics — mirror-PG-and-panic):
//!   * The BEFORE/INSTEAD-OF row trigger's RETURN value: the trigger-language
//!     executor's return-tuple convention (`plpgsql_exec_trigger` depositing the
//!     row via `set_before_trigger_result_tuple`) + the fmgr trigger-context bridge
//!     (`fcinfo->context` carrying the rich `TriggerData` so `CALLED_AS_TRIGGER`
//!     fires and `take_trigger_data` resolves the NEW/OLD row) are not yet ported.
//!   * The ROW UPDATE/DELETE front needs `GetTupleForTrigger`
//!     (`table_tuple_lock` / `heap_fetch` / EvalPlanQual) to fetch the OLD row;
//!     the BEFORE/AFTER STATEMENT front needs the statement-event save leg.
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
    TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSERT, TRIGGER_EVENT_INSTEAD,
    TRIGGER_EVENT_UPDATE,
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

/// `tg_slot_formed_tuple(slot)` — the fully-formed OLD/NEW tuple the marker
/// addresses on the per-call slot side-channel, copied into `mcx`. `Ok(None)`
/// when no slot side-channel is installed or the addressed slot is empty (the C
/// `TupIsNull(slot)`).
pub fn tg_slot_formed_tuple_impl<'mcx>(
    mcx: Mcx<'mcx>,
    slot: types_ri_triggers::TupleTableSlotRef,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>> {
    CURRENT_TRIGGER_SLOTS.with(|cell| {
        let b = cell.borrow();
        let Some(s) = b.as_ref() else {
            return Ok(None);
        };
        let tup = match resolve_slot(s, slot.0) {
            Some(t) => t,
            None => return Ok(None),
        };
        Ok(Some(tup.clone_in(mcx)?))
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

/// `render_slot_columns(slot, attnums)` — `ri_ReportViolation`'s slot-rendering
/// leg (ri_triggers.c:2723-2758): for each 1-based `attnum`, read the attribute
/// name and `atttypid` from the slot relation's descriptor, fetch the value via
/// `slot_getattr`, and (when non-NULL) render it with
/// `getTypeOutputInfo(atttypid)` + `OidOutputFunctionCall`. NULL renders as
/// `None` (the caller prints the literal `"null"`).
pub fn render_slot_columns_impl<'mcx>(
    mcx: Mcx<'mcx>,
    slot: types_ri_triggers::TupleTableSlotRef,
    attnums: &[i16],
) -> PgResult<mcx::PgVec<'mcx, types_ri_triggers::ResultColumn<'mcx>>> {
    let mut out: mcx::PgVec<'mcx, types_ri_triggers::ResultColumn<'mcx>> = mcx::PgVec::new_in(mcx);
    for &attnum in attnums {
        // Attribute name + type OID from the slot relation's descriptor.
        let (name_bytes, atttypid) = CURRENT_TRIGGER_SLOTS.with(
            |cell| -> PgResult<Option<(Vec<u8>, Oid)>> {
                let b = cell.borrow();
                let s = match b.as_ref() {
                    Some(s) => s,
                    None => return Ok(None),
                };
                if resolve_slot(s, slot.0).is_none() {
                    return Ok(None);
                }
                let att = s.relation.rd_att.attr((attnum as usize) - 1);
                Ok(Some((att.attname.name_str().to_vec(), att.atttypid)))
            },
        )?
        .ok_or_else(|| slot_no_payload("render_slot_columns"))?;

        let (datum, isnull) = slot_getattr_impl(mcx, slot, attnum)?;

        let value = if isnull {
            None
        } else {
            let (foutoid, _typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(atttypid)?;
            Some(
                backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::call(
                    mcx, foutoid, datum,
                )?,
            )
        };

        let mut namebuf: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
        namebuf.extend_from_slice(&name_bytes);
        out.push(types_ri_triggers::ResultColumn {
            name: namebuf,
            value,
        });
    }
    Ok(out)
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
    // C: `fcinfo->context = (Node *) &LocTriggerData;` before FunctionCallInvoke
    // — stamp the T_TriggerData node-tag on the call frame fmgr-core builds, so
    // the trigger-language handler's CALLED_AS_TRIGGER(fcinfo) demux fires. The
    // rich payload is the CURRENT_TRIGGER_DATA side-channel installed just above
    // (read by the tg_* accessors); only the demux tag crosses through fmgr.
    let _ctx_guard =
        types_fmgr::fmgr::CallContextTagGuard::install(types_nodes::trigger::T_TriggerData.0 as u32);
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
// validateForeignKeyConstraint (tablecmds.c:13694)
// ===========================================================================

/// `TRIGGER_FIRES_ON_ORIGIN` (`commands/trigger.h`) — the default `tgenabled`.
const TRIGGER_FIRES_ON_ORIGIN: i8 = b'O' as i8;

/// `validateForeignKeyConstraint(conname, rel, pkrel, pkindOid, constraintOid,
/// hasperiod)` (tablecmds.c:13694) — validate that every existing row of the
/// referencing relation `rel` satisfies the FK constraint.
///
/// It lives with the trigger manager (rather than tablecmds) because both legs
/// read the RI procs' `Trigger`/`TriggerData` off the current-trigger
/// side-channel: a synthetic `Trigger` carrying the constraint identity
/// (`tgname`/`tgconstrrelid`/`tgconstrindid`/`tgconstraint`) is installed for the
/// set-based `RI_Initial_Check`, and a per-row `TriggerData` (with the scanned
/// row as `tg_trigslot`) for the fire-the-trigger fallback. The owned analogue of
/// C's stack `Trigger trig = {0}`.
pub fn validate_foreign_key_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    conname: &str,
    rel: &types_rel::Relation<'mcx>,
    pkrel: &types_rel::Relation<'mcx>,
    pkind_oid: Oid,
    constraint_oid: Oid,
    hasperiod: bool,
) -> PgResult<()> {
    // ereport(DEBUG1, "validating foreign key constraint \"%s\"") — no-op.

    // Build a trigger call structure; we'll need it either way.
    //   trig.tgoid = InvalidOid;            trig.tgname = conname;
    //   trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;  trig.tgisinternal = true;
    //   trig.tgconstrrelid = RelationGetRelid(pkrel);
    //   trig.tgconstrindid = pkindOid;      trig.tgconstraint = constraintOid;
    //   trig.tgdeferrable = false;          trig.tginitdeferred = false;
    // The remaining fields are the C `{0}` zero-fill. Box the synthetic Trigger
    // into `mcx` and extend its lifetime to 'static — the side-channel that
    // borrows it is installed and dropped within this call (same query-context
    // discipline as `after_trigger_execute`).
    let trigger_box: mcx::PgBox<'static, Trigger<'static>> = {
        let boxed = trigger_box_clone(mcx, conname, pkrel.rd_id, pkind_oid, constraint_oid)?;
        // SAFETY: allocated in `mcx`; borrowed only for this call's duration.
        unsafe { core::mem::transmute(boxed) }
    };

    // `tg_relation` for the RI procs (relname/attrs/descriptor reads): the
    // referencing relation, aliased (refcount bump) for the call's duration.
    let tg_relation: types_rel::Relation<'static> = {
        let aliased = rel.alias();
        // SAFETY: query-context lifetime extension, as in `after_trigger_execute`.
        unsafe { core::mem::transmute(aliased) }
    };

    // See if we can do it with a single LEFT JOIN query (RI_Initial_Check). We
    // can't do a LEFT JOIN for temporal FKs yet (hasperiod), and a false result
    // means we must proceed with the fire-the-trigger method.
    //
    // Install the side-channel TriggerData (no slot — the initial check reads
    // only the trigger's tgconstraint) for the duration of the call.
    {
        let trigdata = TriggerData {
            type_: T_TriggerData,
            tg_event: 0,
            tg_relation: Some(tg_relation),
            tg_trigtuple: None,
            tg_newtuple: None,
            tg_trigger: Some(trigger_box),
            tg_trigslot: None,
            tg_newslot: None,
            tg_oldtable: None,
            tg_newtable: None,
            tg_updatedcols: None,
        };
        let _td_guard = CurrentTriggerGuard::install(trigdata);
        if !hasperiod
            && backend_utils_adt_ri_triggers_seams::ri_initial_check::call(
                mcx,
                types_ri_triggers::TriggerRef(crate::ri_accessors::CURRENT),
                rel,
                pkrel,
            )?
        {
            return Ok(());
        }
    }

    // Scan through each tuple, calling RI_FKey_check_ins (insert trigger) as if
    // that tuple had just been inserted. If any fail, RI_FKey_check_ins
    // ereport(ERROR)s and that's that.
    //
    //   snapshot = RegisterSnapshot(GetLatestSnapshot());
    let snapshot = backend_utils_time_snapmgr_seams::register_snapshot::call(
        backend_utils_time_snapmgr_seams::get_latest_snapshot::call()?,
    )?;

    //   scan = table_beginscan(rel, snapshot, 0, NULL);
    // C's table_beginscan flags: SO_TYPE_SEQSCAN | SO_ALLOW_STRAT |
    // SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE.
    use types_tableam::relscan::{
        SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TYPE_SEQSCAN,
    };
    let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
    let mut scan = backend_access_heap_heapam_seams::heap_beginscan::call(
        mcx,
        rel.alias(),
        snapshot.clone(),
        flags,
    )?;

    // The synthetic trigger / tg_relation must be rebuilt for the per-row
    // TriggerData (the initial-check TriggerData consumed the originals above).
    let scan_result: PgResult<()> = (|| {
        while let Some(formed) =
            backend_access_heap_heapam_seams::heap_getnext::call(mcx, &mut scan)?
        {
            // CHECK_FOR_INTERRUPTS(): no signal machinery reachable here (no-op).

            // The scanned row, deep-copied into `mcx`, rides the slot
            // side-channel as `tg_trigslot` so RI_FKey_check_ins' slot_getattr
            // reads its FK columns; its HeapTuple view is `tg_trigtuple`.
            let formed_static: types_tuple::backend_access_common_heaptuple::FormedTuple<'static> = {
                // SAFETY: `formed` is allocated in `mcx`; the side-channel that
                // borrows it is installed and dropped within this loop iteration.
                unsafe { core::mem::transmute(formed) }
            };

            // Rebuild the synthetic Trigger + tg_relation for this row's
            // TriggerData (each is moved into the per-call channel).
            let row_trigger: mcx::PgBox<'static, Trigger<'static>> = {
                let cloned = trigger_box_clone(mcx, conname, pkrel.rd_id, pkind_oid, constraint_oid)?;
                unsafe { core::mem::transmute(cloned) }
            };
            let row_relation: types_rel::Relation<'static> = {
                let aliased = rel.alias();
                unsafe { core::mem::transmute(aliased) }
            };
            let tg_trigtuple: HeapTuple<'static> = {
                let copied: HeapTupleData<'mcx> = formed_static.tuple.clone_in(mcx)?;
                let boxed: mcx::PgBox<'mcx, HeapTupleData<'mcx>> =
                    mcx::PgBox::try_new_in(copied, mcx).map_err(|_| mcx.oom(0))?;
                Some(unsafe { core::mem::transmute(boxed) })
            };

            let trigdata = TriggerData {
                type_: T_TriggerData,
                tg_event: TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW,
                tg_relation: Some(row_relation.alias()),
                tg_trigtuple,
                tg_newtuple: None,
                tg_trigger: Some(row_trigger),
                tg_trigslot: Some(types_nodes::SlotId(crate::ri_accessors::SLOT_TRIG as u32)),
                tg_newslot: None,
                tg_oldtable: None,
                tg_newtable: None,
                tg_updatedcols: None,
            };

            // Install the per-row slot side-channel (the scanned tuple + the
            // relation descriptor) and the TriggerData channel, then fire the
            // INSERT check trigger.
            let _slots_guard = CurrentSlotsGuard::install(CurrentTriggerSlots {
                relation: row_relation,
                trigtuple: Some(formed_static),
                newtuple: None,
            });
            let _td_guard = CurrentTriggerGuard::install(trigdata);

            backend_utils_adt_ri_triggers_seams::ri_fkey_check_ins::call(
                mcx,
                types_ri_triggers::TriggerDataRef(crate::ri_accessors::CURRENT),
            )?;
        }
        Ok(())
    })();

    // table_endscan(scan); UnregisterSnapshot(snapshot);
    // (Always run, even on a violation Err, so the scan/snapshot don't leak.)
    let end = backend_access_heap_heapam_seams::heap_endscan::call(scan);
    backend_utils_time_snapmgr_seams::unregister_snapshot::call(snapshot);
    scan_result?;
    end?;
    Ok(())
}

/// Build the synthetic FK-validation `Trigger` (the C `Trigger trig = {0}` with
/// the constraint-identity fields filled in), used per row in the
/// fire-the-trigger fallback of [`validate_foreign_key_constraint`].
fn trigger_box_clone<'mcx>(
    mcx: Mcx<'mcx>,
    conname: &str,
    pkrelid: Oid,
    pkind_oid: Oid,
    constraint_oid: Oid,
) -> PgResult<mcx::PgBox<'mcx, Trigger<'mcx>>> {
    let trig = Trigger {
        tgoid: INVALID_OID,
        tgname: mcx::PgString::from_str_in(conname, mcx)?,
        tgfoid: INVALID_OID,
        tgtype: 0,
        tgenabled: TRIGGER_FIRES_ON_ORIGIN,
        tgisinternal: true,
        tgisclone: false,
        tgconstrrelid: pkrelid,
        tgconstrindid: pkind_oid,
        tgconstraint: constraint_oid,
        tgdeferrable: false,
        tginitdeferred: false,
        tgnargs: 0,
        tgnattr: 0,
        tgattr: mcx::PgVec::new_in(mcx),
        tgargs: mcx::PgVec::new_in(mcx),
        tgqual: None,
        tgoldtable: None,
        tgnewtable: None,
    };
    mcx::PgBox::try_new_in(trig, mcx).map_err(|_| mcx.oom(0))
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
    exec_before_statement_triggers(
        estate,
        relinfo,
        crate::queue::CmdType::Insert,
        types_catalog::pg_trigger::TRIGGER_TYPE_INSERT,
        TRIGGER_EVENT_INSERT,
    )
}
fn exec_bs_update_triggers_impl(estate: &mut EStateData<'_>, relinfo: types_nodes::RriId) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_update_before_statement) {
        return Ok(());
    }
    exec_before_statement_triggers(
        estate,
        relinfo,
        crate::queue::CmdType::Update,
        types_catalog::pg_trigger::TRIGGER_TYPE_UPDATE,
        TRIGGER_EVENT_UPDATE,
    )
}
fn exec_bs_delete_triggers_impl(estate: &mut EStateData<'_>, relinfo: types_nodes::RriId) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_delete_before_statement) {
        return Ok(());
    }
    exec_before_statement_triggers(
        estate,
        relinfo,
        crate::queue::CmdType::Delete,
        types_catalog::pg_trigger::TRIGGER_TYPE_DELETE,
        TRIGGER_EVENT_DELETE,
    )
}

/// Shared body of `ExecBSInsertTriggers` / `ExecBSUpdateTriggers` /
/// `ExecBSDeleteTriggers` (trigger.c:2402/2896/2631) — fire the BEFORE STATEMENT
/// FOR EACH STATEMENT triggers.  These take no tuple (`tg_trigtuple`/`tg_trigslot`
/// are NULL); a statement trigger cannot have a WHEN clause and must not return a
/// value.
///
/// `tg_event_op` is the per-command `TRIGGER_TYPE_*` match bit; `tg_event_bit` is
/// the `TRIGGER_EVENT_*` opcode placed (with `TRIGGER_EVENT_BEFORE`, no
/// `TRIGGER_EVENT_ROW`) into `LocTriggerData.tg_event`.
fn exec_before_statement_triggers(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    cmd_type: crate::queue::CmdType,
    tg_event_op: i16,
    tg_event_bit: u32,
) -> PgResult<()> {
    use types_catalog::pg_trigger::{
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_MATCHES, TRIGGER_TYPE_STATEMENT,
    };

    // no-op if we already fired BS triggers in this context.
    let rel_oid = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_id)
        .expect("ExecBS*Triggers: ResultRelInfo has no relation");
    let already = crate::queue::before_stmt_triggers_fired(rel_oid, cmd_type).map_err(|()| {
        PgError::error("before_stmt_triggers_fired() called outside of query".to_string())
    })?;
    if already {
        return Ok(());
    }

    // LocTriggerData.tg_event = TRIGGER_EVENT_<OP> | TRIGGER_EVENT_BEFORE;
    let tg_event = tg_event_bit | TRIGGER_EVENT_BEFORE;

    let numtriggers = match estate.result_rel(relinfo).ri_TrigDesc.as_ref() {
        Some(td) => td.triggers.len(),
        None => return Ok(()),
    };

    for i in 0..numtriggers {
        let (tgtype, tgenabled, tgnattr, has_qual) = {
            let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[i];
            (trig.tgtype, trig.tgenabled, trig.tgnattr, trig.tgqual.is_some())
        };
        // if (!TRIGGER_TYPE_MATCHES(tgtype, STATEMENT, BEFORE, <op>)) continue;
        if !TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, tg_event_op) {
            continue;
        }
        // if (!TriggerEnabled(estate, relinfo, trigger, tg_event, NULL, NULL, NULL))
        //   continue;   (no slots for a statement trigger.)
        if !trigger_enabled(
            estate, relinfo, i, tgenabled, tgnattr, has_qual, tg_event,
            /* oldslot */ None, /* newslot */ None,
        )? {
            continue;
        }

        // newtuple = ExecCallTriggerFunc(&LocTriggerData, ...);
        let returned = fire_statement_trigger(estate, relinfo, i, tg_event)?;

        // if (newtuple) ereport(ERROR, "BEFORE STATEMENT trigger cannot return a value");
        if returned {
            return Err(PgError::error(
                "BEFORE STATEMENT trigger cannot return a value".to_string(),
            )
            .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
        }
    }
    Ok(())
}

/// Fire one BEFORE STATEMENT trigger: build a tuple-less `TriggerData`, call the
/// function, and report whether it returned a non-null value (which is a protocol
/// violation for a statement trigger).
fn fire_statement_trigger(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    tgindx: usize,
    tg_event: u32,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;

    // tg_trigger = &(trigdesc->triggers[tgindx]) — cloned into the query context.
    let trigger_box: mcx::PgBox<'static, Trigger<'static>> = {
        let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[tgindx];
        let cloned = trig.clone_in(mcx)?;
        let boxed: mcx::PgBox<'_, Trigger<'_>> =
            mcx::PgBox::try_new_in(cloned, mcx).map_err(|_| mcx.oom(0))?;
        // SAFETY: allocated in mcx (= es_query_cxt); the side-channel that borrows
        // it is installed/dropped within this call.
        unsafe { core::mem::transmute(boxed) }
    };

    // tg_relation = relinfo->ri_RelationDesc — aliased for the call's duration.
    let tg_relation: types_rel::Relation<'static> = {
        let rel = estate
            .result_rel(relinfo)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecBS*Triggers: ResultRelInfo has no relation");
        let aliased = rel.alias();
        // SAFETY: query-context lifetime extension; released at the call's end.
        unsafe {
            core::mem::transmute::<types_rel::Relation<'_>, types_rel::Relation<'static>>(aliased)
        }
    };
    let slots_relation: types_rel::Relation<'static> = {
        let rel = estate.result_rel(relinfo).ri_RelationDesc.as_ref().unwrap();
        let aliased = rel.alias();
        unsafe {
            core::mem::transmute::<types_rel::Relation<'_>, types_rel::Relation<'static>>(aliased)
        }
    };

    let trigdata = TriggerData {
        type_: T_TriggerData,
        tg_event,
        tg_relation: Some(tg_relation),
        tg_trigtuple: None,
        tg_newtuple: None,
        tg_trigger: Some(trigger_box),
        tg_trigslot: None,
        tg_newslot: None,
        tg_oldtable: None,
        tg_newtable: None,
        tg_updatedcols: None,
    };

    // Install the slots side-channel with the relation only (no NEW/OLD tuple):
    // the trigger-language handler reads `tg_relation` (for the descriptor) off
    // this channel even for a statement trigger; the per-row tuple accessors are
    // never reached because a statement trigger has no NEW/OLD row.
    let _slots_guard = CurrentSlotsGuard::install(CurrentTriggerSlots {
        relation: slots_relation,
        trigtuple: None,
        newtuple: None,
    });

    let result = exec_call_trigger_func(trigdata)?;
    // result is a HeapTuple-pointer Datum in C; here the PL handler deposits any
    // returned row on the BEFORE_TRIGGER_RESULT channel.  A statement trigger
    // returning a value is a protocol error, reported by the caller.
    let returned = decode_before_trigger_result(mcx, result)?;
    Ok(returned.is_some())
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
    if _tc.is_some() {
        return Err(transition_table_unported());
    }
    // AfterTriggerSaveEvent(estate, relinfo, NULL, NULL, TRIGGER_EVENT_INSERT,
    //                       false, NULL, NULL, NIL, NULL, transition_capture, false);
    after_trigger_save_event_stmt(estate, relinfo, TRIGGER_EVENT_INSERT, crate::queue::CmdType::Insert)
}
fn exec_as_update_triggers_impl(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_update_after_statement) {
        return Ok(());
    }
    if _tc.is_some() {
        return Err(transition_table_unported());
    }
    after_trigger_save_event_stmt(estate, relinfo, TRIGGER_EVENT_UPDATE, crate::queue::CmdType::Update)
}
fn exec_as_delete_triggers_impl(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    _tc: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
) -> PgResult<()> {
    if !bs_trigger_flag(estate, relinfo, |td| td.trig_delete_after_statement) {
        return Ok(());
    }
    if _tc.is_some() {
        return Err(transition_table_unported());
    }
    after_trigger_save_event_stmt(estate, relinfo, TRIGGER_EVENT_DELETE, crate::queue::CmdType::Delete)
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

/// `ExecBRInsertTriggers(estate, relinfo, slot)` (trigger.c:2466) — fire the
/// BEFORE INSERT FOR EACH ROW triggers against `slot`.
///
/// Returns `false` ("do nothing" — skip the insert) when a trigger returned a
/// NULL tuple; otherwise `true`, with `slot` holding the (possibly trigger-
/// modified) NEW tuple.
fn exec_br_insert_triggers_impl<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<bool> {
    exec_br_ir_insert_triggers(estate, relinfo, slot, /* instead */ false)
}
/// `ExecIRInsertTriggers(estate, relinfo, slot)` (trigger.c:2570) — fire the
/// INSTEAD OF INSERT FOR EACH ROW triggers (on a view).
fn exec_ir_insert_triggers_impl<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<bool> {
    exec_br_ir_insert_triggers(estate, relinfo, slot, /* instead */ true)
}

/// Shared body of `ExecBRInsertTriggers` (BEFORE) and `ExecIRInsertTriggers`
/// (INSTEAD OF) — the two differ only in the `TRIGGER_TYPE_BEFORE` vs
/// `TRIGGER_TYPE_INSTEAD` timing match and the `TRIGGER_EVENT_BEFORE` vs
/// `TRIGGER_EVENT_INSTEAD` event bit.  Both fire FOR EACH ROW on INSERT, both
/// thread the NEW tuple through each trigger and apply a returned tuple.
fn exec_br_ir_insert_triggers<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    slot: types_nodes::SlotId,
    instead: bool,
) -> PgResult<bool> {
    use types_catalog::pg_trigger::{
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_MATCHES,
        TRIGGER_TYPE_ROW,
    };

    let mcx = estate.es_query_cxt;
    // LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW |
    //   (TRIGGER_EVENT_BEFORE | TRIGGER_EVENT_INSTEAD);
    let tg_event = TRIGGER_EVENT_INSERT
        | TRIGGER_EVENT_ROW
        | if instead { TRIGGER_EVENT_INSTEAD } else { TRIGGER_EVENT_BEFORE };
    let timing = if instead { TRIGGER_TYPE_INSTEAD } else { TRIGGER_TYPE_BEFORE };

    // The number of triggers (trigdesc->numtriggers).
    let numtriggers = {
        let rri = estate.result_rel(relinfo);
        match rri.ri_TrigDesc.as_ref() {
            Some(td) => td.triggers.len(),
            None => return Ok(true),
        }
    };

    // newtuple == NULL until first materialized (ExecFetchSlotHeapTuple(slot)).
    let mut newtuple: Option<
        types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    > = None;

    for i in 0..numtriggers {
        // trigger = &trigdesc->triggers[i]; read the dispatch facts under an
        // immutable borrow (the firing call below needs &mut estate).
        let (tgtype, tgenabled, tgoid, has_qual, tgnattr) = {
            let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[i];
            (
                trig.tgtype,
                trig.tgenabled,
                trig.tgoid,
                trig.tgqual.is_some(),
                trig.tgnattr,
            )
        };

        // if (!TRIGGER_TYPE_MATCHES(tgtype, ROW, BEFORE|INSTEAD, INSERT)) continue;
        if !TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, timing, TRIGGER_TYPE_INSERT) {
            continue;
        }
        // if (!TriggerEnabled(estate, relinfo, trigger, tg_event, NULL, NULL, slot))
        //   continue;   (oldslot == NULL on INSERT; newslot == slot.)
        if !trigger_enabled(
            estate,
            relinfo,
            i,
            tgenabled,
            tgnattr,
            has_qual,
            tg_event,
            /* oldslot */ None,
            /* newslot */ Some(slot),
        )? {
            continue;
        }

        // if (!newtuple) newtuple = ExecFetchSlotHeapTuple(slot, true, &should_free);
        if newtuple.is_none() {
            let (formed, _should_free) = {
                let sd = estate.slot_data_mut(slot);
                backend_executor_execTuples_seams::exec_fetch_slot_heap_tuple::call(mcx, sd, true)?
            };
            newtuple = Some(formed);
        }
        // oldtuple = newtuple — the tuple handed to this trigger as tg_trigtuple.
        let oldtuple = newtuple.clone().unwrap();

        // newtuple = ExecCallTriggerFunc(&LocTriggerData, ...);
        let returned = fire_row_insert_trigger(estate, relinfo, i, tgoid, tg_event, &oldtuple)?;

        match returned {
            // newtuple == NULL  ->  "do nothing".
            None => return Ok(false),
            Some(rt) => {
                if formed_tuple_same(&rt, &oldtuple) {
                    // newtuple == oldtuple — row unchanged; keep it cached.
                    newtuple = Some(rt);
                } else {
                    // newtuple != oldtuple — the trigger modified the row.
                    // ExecForceStoreHeapTuple(newtuple, slot, false);
                    backend_executor_execTuples_seams::exec_force_store_formed_heap_tuple::call(
                        estate,
                        slot,
                        rt.clone_in(mcx)?,
                        false,
                    )?;
                    // signal tuple should be re-fetched if used.
                    newtuple = None;
                }
            }
        }
    }

    Ok(true)
}

/// The C `newtuple != oldtuple` pointer-identity test, realized in the owned
/// model as a data-bytes comparison: a trigger that returns its NEW row
/// unchanged yields an identical user-data area; a modified row differs.
fn formed_tuple_same(
    a: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
    b: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
) -> bool {
    a.data == b.data
}

/// Fire one BEFORE/INSTEAD-OF row INSERT trigger: build the `TriggerData`,
/// install the per-call side-channels (the trigger func reads NEW via the slot
/// accessors), call the function, and decode the returned tuple.
///
/// Returns `None` for the C `NULL` ("do nothing") result, else the returned NEW
/// `FormedTuple`.
fn fire_row_insert_trigger<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    tgindx: usize,
    _tgoid: Oid,
    tg_event: u32,
    trigtuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'static>>> {
    let mcx = estate.es_query_cxt;

    // tg_trigger = &(trigdesc->triggers[tgindx]) — cloned into the query context.
    let trigger_box: mcx::PgBox<'static, Trigger<'static>> = {
        let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[tgindx];
        let cloned = trig.clone_in(mcx)?;
        let boxed: mcx::PgBox<'mcx, Trigger<'mcx>> =
            mcx::PgBox::try_new_in(cloned, mcx).map_err(|_| mcx.oom(0))?;
        // SAFETY: allocated in mcx (= es_query_cxt); the side-channel that borrows
        // it is installed/dropped within this call.
        unsafe { core::mem::transmute(boxed) }
    };

    // tg_relation = relinfo->ri_RelationDesc — aliased for the call's duration.
    let tg_relation: types_rel::Relation<'static> = {
        let rel = estate
            .result_rel(relinfo)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecBRInsertTriggers: ResultRelInfo has no relation");
        let aliased = rel.alias();
        // SAFETY: query-context lifetime extension; released when the side-channel
        // guard drops at the end of this call.
        unsafe {
            core::mem::transmute::<types_rel::Relation<'_>, types_rel::Relation<'static>>(aliased)
        }
    };
    let slots_relation: types_rel::Relation<'static> = {
        let rel = estate.result_rel(relinfo).ri_RelationDesc.as_ref().unwrap();
        let aliased = rel.alias();
        unsafe {
            core::mem::transmute::<types_rel::Relation<'_>, types_rel::Relation<'static>>(aliased)
        }
    };

    // tg_trigtuple = newtuple (the NEW row): the FormedTuple rides the slot
    // side-channel (so slot_getattr deforms it) and a HeapTuple view goes on
    // TriggerData.
    let formed_static: types_tuple::backend_access_common_heaptuple::FormedTuple<'static> = {
        let copied = trigtuple.clone_in(mcx)?;
        // SAFETY: allocated in mcx; installed/dropped within this call.
        unsafe { core::mem::transmute(copied) }
    };
    let tg_trigtuple: HeapTuple<'static> = {
        let copied: HeapTupleData<'mcx> = formed_static.tuple.clone_in(mcx)?;
        let boxed: mcx::PgBox<'mcx, HeapTupleData<'mcx>> =
            mcx::PgBox::try_new_in(copied, mcx).map_err(|_| mcx.oom(0))?;
        Some(unsafe { core::mem::transmute(boxed) })
    };

    let trigdata = TriggerData {
        type_: T_TriggerData,
        tg_event,
        tg_relation: Some(tg_relation),
        tg_trigtuple,
        tg_newtuple: None,
        tg_trigger: Some(trigger_box),
        // tg_trigslot = the NEW slot; resolves to the SLOT_TRIG payload below.
        tg_trigslot: Some(types_nodes::SlotId(crate::ri_accessors::SLOT_TRIG as u32)),
        tg_newslot: None,
        tg_oldtable: None,
        tg_newtable: None,
        tg_updatedcols: None,
    };

    let _slots_guard = CurrentSlotsGuard::install(CurrentTriggerSlots {
        relation: slots_relation,
        trigtuple: Some(formed_static),
        newtuple: None,
    });

    // result = ExecCallTriggerFunc(&LocTriggerData, ...);
    // newtuple = (HeapTuple) DatumGetPointer(result);
    let result = exec_call_trigger_func(trigdata)?;
    decode_before_trigger_result(mcx, result)
}

/// Fire one BEFORE row UPDATE or DELETE trigger (the shared
/// `ExecBRUpdateTriggers`/`ExecBRDeleteTriggers` inner-loop body): build the
/// `TriggerData` with the OLD tuple as `tg_trigtuple`/`tg_trigslot` and — for an
/// UPDATE — the NEW tuple as `tg_newtuple`/`tg_newslot`, install the per-call
/// side-channels, call the function, and decode the returned tuple.
///
/// `new` is `Some((newslot, newtuple))` for an UPDATE (the NEW row), `None` for
/// a DELETE.  Returns `None` for the C `NULL` ("do nothing" / suppress) result.
#[allow(clippy::too_many_arguments)]
fn fire_row_modify_trigger<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    tgindx: usize,
    _tgoid: Oid,
    tg_event: u32,
    _oldslot: types_nodes::SlotId,
    trigtuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    new: Option<(
        types_nodes::SlotId,
        &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    )>,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'static>>> {
    let mcx = estate.es_query_cxt;

    // tg_trigger = &(trigdesc->triggers[tgindx]) — cloned into the query context.
    let trigger_box: mcx::PgBox<'static, Trigger<'static>> = {
        let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[tgindx];
        let cloned = trig.clone_in(mcx)?;
        let boxed: mcx::PgBox<'mcx, Trigger<'mcx>> =
            mcx::PgBox::try_new_in(cloned, mcx).map_err(|_| mcx.oom(0))?;
        // SAFETY: allocated in mcx (= es_query_cxt); the side-channel that borrows
        // it is installed/dropped within this call.
        unsafe { core::mem::transmute(boxed) }
    };

    // tg_relation = relinfo->ri_RelationDesc — aliased for the call's duration.
    let tg_relation: types_rel::Relation<'static> = {
        let rel = estate
            .result_rel(relinfo)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecBR{Update,Delete}Triggers: ResultRelInfo has no relation");
        let aliased = rel.alias();
        // SAFETY: query-context lifetime extension; released when the side-channel
        // guard drops at the end of this call.
        unsafe {
            core::mem::transmute::<types_rel::Relation<'_>, types_rel::Relation<'static>>(aliased)
        }
    };
    let slots_relation: types_rel::Relation<'static> = {
        let rel = estate.result_rel(relinfo).ri_RelationDesc.as_ref().unwrap();
        let aliased = rel.alias();
        unsafe {
            core::mem::transmute::<types_rel::Relation<'_>, types_rel::Relation<'static>>(aliased)
        }
    };

    // tg_trigtuple = the OLD row (the slot side-channel carries the FormedTuple so
    // slot_getattr deforms it; a HeapTuple view goes on TriggerData).
    let old_formed_static: types_tuple::backend_access_common_heaptuple::FormedTuple<'static> = {
        let copied = trigtuple.clone_in(mcx)?;
        unsafe { core::mem::transmute(copied) }
    };
    let tg_trigtuple: HeapTuple<'static> = {
        let copied: HeapTupleData<'mcx> = old_formed_static.tuple.clone_in(mcx)?;
        let boxed: mcx::PgBox<'mcx, HeapTupleData<'mcx>> =
            mcx::PgBox::try_new_in(copied, mcx).map_err(|_| mcx.oom(0))?;
        Some(unsafe { core::mem::transmute(boxed) })
    };

    // tg_newtuple / tg_newslot — only for UPDATE.
    let (new_formed_static, tg_newtuple, tg_newslot) = match new {
        Some((newslot, newtuple)) => {
            let nf: types_tuple::backend_access_common_heaptuple::FormedTuple<'static> = {
                let copied = newtuple.clone_in(mcx)?;
                unsafe { core::mem::transmute(copied) }
            };
            let ntv: HeapTuple<'static> = {
                let copied: HeapTupleData<'mcx> = nf.tuple.clone_in(mcx)?;
                let boxed: mcx::PgBox<'mcx, HeapTupleData<'mcx>> =
                    mcx::PgBox::try_new_in(copied, mcx).map_err(|_| mcx.oom(0))?;
                Some(unsafe { core::mem::transmute(boxed) })
            };
            let _ = newslot;
            (
                Some(nf),
                ntv,
                Some(types_nodes::SlotId(crate::ri_accessors::SLOT_NEW as u32)),
            )
        }
        None => (None, None, None),
    };

    let trigdata = TriggerData {
        type_: T_TriggerData,
        tg_event,
        tg_relation: Some(tg_relation),
        tg_trigtuple,
        tg_newtuple,
        tg_trigger: Some(trigger_box),
        // tg_trigslot = the OLD slot; resolves to the SLOT_TRIG payload below.
        tg_trigslot: Some(types_nodes::SlotId(crate::ri_accessors::SLOT_TRIG as u32)),
        tg_newslot,
        tg_oldtable: None,
        tg_newtable: None,
        tg_updatedcols: None,
    };

    let _slots_guard = CurrentSlotsGuard::install(CurrentTriggerSlots {
        relation: slots_relation,
        trigtuple: Some(old_formed_static),
        newtuple: new_formed_static,
    });

    // result = ExecCallTriggerFunc(&LocTriggerData, ...);
    let result = exec_call_trigger_func(trigdata)?;
    decode_before_trigger_result(mcx, result)
}

// ---------------------------------------------------------------------------
// BEFORE-trigger return-tuple channel — the owned analogue of C's
// `(HeapTuple) DatumGetPointer(result)`.
//
// A BEFORE/INSTEAD-OF row trigger function returns the HeapTuple it wants
// applied (or NULL for "do nothing").  In C this rides the fmgr `Datum` result
// as a bare HeapTuple pointer.  The idiomatic `Datum` is an opaque `usize` that
// cannot safely carry an arena pointer across the fmgr boundary, so a PL trigger
// executor instead deposits the returned row on this per-call thread-local and
// returns a sentinel `Datum`; the firing path takes it back here.  Set/taken
// strictly within a single `ExecCallTriggerFunc` invocation, so the `'static`
// marker is sound (the payload is allocated in the firing query context).
// ---------------------------------------------------------------------------

thread_local! {
    /// The HeapTuple a BEFORE/INSTEAD-OF row trigger function returned, deposited
    /// by the PL trigger executor (via [`set_before_trigger_result_tuple`]) and
    /// taken by [`decode_before_trigger_result`].
    static BEFORE_TRIGGER_RESULT: RefCell<Option<BeforeTriggerResult>> =
        const { RefCell::new(None) };
}

/// The two cases of a BEFORE/INSTEAD-OF row trigger return: a row to apply, or
/// the C `NULL` "do nothing".
enum BeforeTriggerResult {
    /// `return NEW`/`return OLD`/`return <row>` — the row to apply.
    Tuple(types_tuple::backend_access_common_heaptuple::FormedTuple<'static>),
    /// `return NULL` — skip the operation ("do nothing").
    DoNothing,
}

/// `plpgsql_exec_trigger` (and the SQL/C trigger handlers) deposit the row a
/// BEFORE/INSTEAD-OF trigger returned here, just before returning the sentinel
/// `Datum` from the fmgr call.  `None` is the C `return NULL` ("do nothing").
///
/// # Safety
/// The deposited `FormedTuple` must outlive the enclosing `ExecCallTriggerFunc`
/// (the firing path takes it back within the same call); the PL executor
/// allocates it in the firing query context, satisfying this.
pub fn set_before_trigger_result_tuple_impl<'mcx>(
    tuple: Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>,
) {
    let v = match tuple {
        // SAFETY: the firing path takes this back within the same
        // ExecCallTriggerFunc call; the depositor allocates in the firing query
        // context, which outlives that call.
        Some(t) => BeforeTriggerResult::Tuple(unsafe {
            core::mem::transmute::<
                types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
                types_tuple::backend_access_common_heaptuple::FormedTuple<'static>,
            >(t)
        }),
        None => BeforeTriggerResult::DoNothing,
    };
    BEFORE_TRIGGER_RESULT.with(|c| *c.borrow_mut() = Some(v));
}

/// `return PointerGetDatum(trigdata->tg_trigtuple)` — deposit the trigger's OLD
/// (`tg_trigtuple`) row, fully formed off the per-call slot side-channel, as the
/// BEFORE-trigger result.  This is the kernel of `src/test/regress/regress.c`'s
/// `trigger_return_old`: a C trigger function that returns the unmodified OLD
/// tuple.  Because a registry-loaded C trigger function returns through the same
/// [`BEFORE_TRIGGER_RESULT`] channel the PL executor uses (the result `Datum` the
/// fmgr call returns is the ignored sentinel), it deposits here rather than
/// lowering a tuple onto the fmgr return lane.
///
/// Returns `false` when no trigger slot side-channel is installed or the OLD slot
/// is empty (the analogue of a NULL `tg_trigtuple`); the caller then mirrors C's
/// `PointerGetDatum(NULL)` "do nothing".
pub fn set_before_trigger_result_to_trigtuple() -> bool {
    CURRENT_TRIGGER_SLOTS.with(|cell| {
        let b = cell.borrow();
        let Some(s) = b.as_ref() else {
            return false;
        };
        let Some(trigtuple) = s.trigtuple.as_ref() else {
            return false;
        };
        // The slot tuple already lives 'static in the firing query context (it is
        // taken back within the same ExecCallTriggerFunc call), so depositing a
        // clone-free copy is sound.
        let v = BeforeTriggerResult::Tuple(trigtuple.clone());
        BEFORE_TRIGGER_RESULT.with(|c| *c.borrow_mut() = Some(v));
        true
    })
}

/// `(HeapTuple) DatumGetPointer(result)` for a BEFORE/INSTEAD-OF row trigger —
/// take back the row the trigger function deposited on the per-call channel.
///
/// `Ok(None)` is the C `NULL` ("do nothing").  An empty channel means the trigger
/// function did not deposit a result: the trigger-language handler that runs it
/// (`plpgsql_exec_trigger` for PL/pgSQL) is not yet ported to the return-tuple
/// convention — a loud, named boundary rather than a fake pointer dereference.
fn decode_before_trigger_result<'mcx>(
    mcx: Mcx<'mcx>,
    _result: Datum,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'static>>> {
    let taken = BEFORE_TRIGGER_RESULT.with(|c| c.borrow_mut().take());
    match taken {
        Some(BeforeTriggerResult::DoNothing) => Ok(None),
        Some(BeforeTriggerResult::Tuple(t)) => {
            // Re-anchor into the firing query context (the deposit is already in
            // es_query_cxt; clone keeps the lifetime story explicit).
            let copied = t.clone_in(mcx)?;
            // SAFETY: copied is in mcx (= es_query_cxt), which outlives this call.
            Ok(Some(unsafe { core::mem::transmute(copied) }))
        }
        None => Err(before_trigger_return_unported()),
    }
}

#[cold]
#[inline(never)]
fn before_trigger_return_unported() -> PgError {
    PgError::error(
        "ExecBRInsertTriggers: the trigger function returned without depositing a \
         result row — the trigger-language executor's return-tuple convention \
         (plpgsql_exec_trigger -> set_before_trigger_result_tuple) is not yet \
         ported; the BEFORE/INSTEAD-OF row firing front, WHEN-qual gating, and \
         NEW-slot materialization are in place up to the fmgr call"
            .to_string(),
    )
}

// ---------------------------------------------------------------------------
// TriggerEnabled (trigger.c:3483) — the replication-role / tgenabled /
// column-specific / WHEN-qual firing-control test.
// ---------------------------------------------------------------------------

/// `TriggerEnabled(estate, relinfo, trigger, event, modifiedCols, oldslot,
/// newslot)` (trigger.c:3483).
///
/// `trigger` is `relinfo->ri_TrigDesc->triggers[tgindx]`; the matching
/// `ri_TrigWhenExprs[tgindx]` slot caches the compiled WHEN predicate (lazily
/// built on first use, surviving in `es_query_cxt`).  `oldslot`/`newslot` carry
/// the OLD/NEW rows the WHEN clause references as `OLD`/`NEW` (mapped to
/// `INNER_VAR`/`OUTER_VAR`).  The column-specific (`tgnattr`) check only applies
/// to UPDATE; on INSERT/DELETE `modified_cols` is `None` and the arm is skipped.
#[allow(clippy::too_many_arguments)]
fn trigger_enabled<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    tgindx: usize,
    tgenabled: i8,
    tgnattr: i16,
    has_qual: bool,
    event: u32,
    oldslot: Option<types_nodes::SlotId>,
    newslot: Option<types_nodes::SlotId>,
) -> PgResult<bool> {
    // Replication-role-dependent enable state.
    if !trigger_enabled_no_qual(tgenabled) {
        return Ok(false);
    }

    // Column-specific trigger (only possible for UPDATE; tgattr ignored
    // otherwise).  modifiedCols is not threaded on the INSERT/DELETE paths
    // (None), so the column check is a no-op there.  When an UPDATE path threads
    // a modified-columns set, the per-column tgattr check belongs here; on
    // INSERT (this caller) tgnattr-gated columns never restrict firing.
    let _ = tgnattr;

    // WHEN clause (tgqual).
    if has_qual {
        // predicate = &relinfo->ri_TrigWhenExprs[tgindx]; build it on first use.
        let needs_build = estate
            .result_rel(relinfo)
            .ri_TrigWhenExprs
            .as_ref()
            .and_then(|v| v.get(tgindx))
            .map(|p| p.is_none())
            .unwrap_or(true);
        if needs_build {
            let predicate = build_trigger_when_predicate(estate, relinfo, tgindx)?;
            if let Some(slot_vec) = estate.result_rel_mut(relinfo).ri_TrigWhenExprs.as_mut() {
                if let Some(cell) = slot_vec.get_mut(tgindx) {
                    *cell = predicate;
                }
            }
        }

        // econtext = GetPerTupleExprContext(estate);
        // econtext->ecxt_innertuple = oldslot; ecxt_outertuple = newslot;
        let econtext = backend_executor_execUtils_seams::get_per_tuple_expr_context::call(estate)?;
        {
            let ecxt = estate.ecxt_mut(econtext);
            ecxt.ecxt_innertuple = oldslot;
            ecxt.ecxt_outertuple = newslot;
        }

        // if (!ExecQual(*predicate, econtext)) return false;
        // The compiled predicate lives in ri_TrigWhenExprs[tgindx]; take it out to
        // satisfy exec_qual's &mut ExprState + &mut estate, then put it back.
        let mut predicate = match estate
            .result_rel_mut(relinfo)
            .ri_TrigWhenExprs
            .as_mut()
            .and_then(|v| v.get_mut(tgindx))
            .and_then(|c| c.take())
        {
            Some(p) => p,
            // A NULL predicate after build means the clause folded to constant
            // TRUE (ExecPrepareQual returned NULL) — ExecQual(NULL) is TRUE.
            None => return Ok(true),
        };
        let pass = backend_executor_execExpr_seams::exec_qual::call(&mut predicate, econtext, estate)?;
        // Put the compiled predicate back for the next row.
        if let Some(cell) = estate
            .result_rel_mut(relinfo)
            .ri_TrigWhenExprs
            .as_mut()
            .and_then(|v| v.get_mut(tgindx))
        {
            *cell = Some(predicate);
        }
        if !pass {
            return Ok(false);
        }
    }
    let _ = event;

    Ok(true)
}

/// Build the compiled WHEN-clause `ExprState` for `triggers[tgindx]`
/// (trigger.c:3524-3548): `stringToNode(tgqual)` →
/// `expand_generated_columns_in_expr` (OLD varno 1, NEW varno 2) →
/// `ChangeVarNodes(OLD→INNER_VAR, NEW→OUTER_VAR)` → `make_ands_implicit` →
/// `ExecPrepareQual`.  Returns `None` when the clause const-folds to TRUE
/// (`ExecPrepareQual` returned NULL).
fn build_trigger_when_predicate<'mcx>(
    estate: &mut EStateData<'mcx>,
    relinfo: types_nodes::RriId,
    tgindx: usize,
) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    const PRS2_OLD_VARNO: i32 = 1;
    const PRS2_NEW_VARNO: i32 = 2;
    const INNER_VAR: i32 = -1;
    const OUTER_VAR: i32 = -2;

    let mcx = estate.es_query_cxt;

    // tgqual text + the target relation OID (for expand_generated_columns).
    let (tgqual, rel_oid) = {
        let rri = estate.result_rel(relinfo);
        let trig = &rri.ri_TrigDesc.as_ref().unwrap().triggers[tgindx];
        let q = trig
            .tgqual
            .as_ref()
            .expect("build_trigger_when_predicate: trigger has no tgqual")
            .as_str()
            .to_string();
        let oid = rri
            .ri_RelationDesc
            .as_ref()
            .expect("build_trigger_when_predicate: ResultRelInfo has no relation")
            .rd_id;
        (q, oid)
    };

    // tgqual = stringToNode(trigger->tgqual);
    let node = backend_nodes_read_seams::string_to_node::call(mcx, &tgqual)?;
    let mut expr: types_nodes::primnodes::Expr = node
        .as_expr()
        .ok_or_else(|| {
            PgError::error("trigger WHEN clause tgqual did not parse to an expression node".to_string())
        })?
        .clone();

    // tgqual = expand_generated_columns_in_expr(tgqual, rel, PRS2_OLD_VARNO);
    // tgqual = expand_generated_columns_in_expr(tgqual, rel, PRS2_NEW_VARNO);
    expr = backend_rewrite_rewritehandler_seams::expand_generated_columns_in_expr::call(
        mcx,
        Some(expr),
        rel_oid,
        PRS2_OLD_VARNO,
    )?
    .expect("expand_generated_columns_in_expr dropped the WHEN expression");
    expr = backend_rewrite_rewritehandler_seams::expand_generated_columns_in_expr::call(
        mcx,
        Some(expr),
        rel_oid,
        PRS2_NEW_VARNO,
    )?
    .expect("expand_generated_columns_in_expr dropped the WHEN expression");

    // ChangeVarNodes(tgqual, PRS2_OLD_VARNO, INNER_VAR, 0);
    // ChangeVarNodes(tgqual, PRS2_NEW_VARNO, OUTER_VAR, 0);
    expr = change_var_nodes_expr(expr, PRS2_OLD_VARNO, INNER_VAR);
    expr = change_var_nodes_expr(expr, PRS2_NEW_VARNO, OUTER_VAR);

    // tgqual = (Node *) make_ands_implicit((Expr *) tgqual);
    let quals: Vec<types_nodes::primnodes::Expr> =
        backend_nodes_core::makefuncs::make_ands_implicit(Some(expr));

    // *predicate = ExecPrepareQual((List *) tgqual, estate);
    backend_executor_execExpr_seams::exec_prepare_qual::call(
        if quals.is_empty() { None } else { Some(&quals) },
        estate,
    )
}

/// `ChangeVarNodes(node, rt_index, new_index, 0)` (rewriteManip.c) restricted to
/// a `sublevels_up == 0` re-stamp of an owned `Expr` tree (the trigger WHEN
/// clause has no sub-selects with deeper level refs).  Walks the tree with
/// `expression_tree_mutator`, re-stamping every top-level `Var` whose `varno`
/// equals `rt_index` (and `varlevelsup == 0`) to `new_index`.
fn change_var_nodes_expr(
    expr: types_nodes::primnodes::Expr,
    rt_index: i32,
    new_index: i32,
) -> types_nodes::primnodes::Expr {
    use types_nodes::primnodes::Expr;
    fn walk(node: Expr, rt_index: i32, new_index: i32) -> Expr {
        match node {
            Expr::Var(mut v) => {
                if v.varlevelsup == 0 && v.varno == rt_index {
                    v.varno = new_index;
                    if v.varnosyn as i32 == rt_index {
                        v.varnosyn = new_index as types_core::primitive::Index;
                    }
                }
                Expr::Var(v)
            }
            other => backend_nodes_core::nodefuncs::expression_tree_mutator(other, &mut |child| {
                walk(child, rt_index, new_index)
            }),
        }
    }
    walk(expr, rt_index, new_index)
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

/// `AfterTriggerSaveEvent(...)` (trigger.c:6169) for the STATEMENT-level
/// (`row_trigger == false`) path — the AFTER STATEMENT trigger queue leg.
///
/// A statement-level event carries no tuple: both ctids are invalid, the flag is
/// `AFTER_TRIGGER_1CTID`, the level is `TRIGGER_TYPE_STATEMENT`, and
/// `cancel_prior_stmt_triggers` is run to retire any prior batch of AS events for
/// this relation+command at this query level.  A statement trigger has no WHEN
/// clause, so the WHEN-qual leg is unreachable here.
fn after_trigger_save_event_stmt(
    estate: &mut EStateData<'_>,
    relinfo: types_nodes::RriId,
    event: u32,
    cmd_type: crate::queue::CmdType,
) -> PgResult<()> {
    use types_catalog::pg_trigger::{
        TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_MATCHES,
        TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_UPDATE,
    };
    use types_nodes::trigger::{
        AFTER_TRIGGER_1CTID, AFTER_TRIGGER_DEFERRABLE, AFTER_TRIGGER_INITDEFERRED,
    };
    const TRIGGER_EVENT_DELETE: u32 = 1;
    const TRIGGER_EVENT_UPDATE: u32 = 2;

    // if (afterTriggers.query_depth < 0) elog(ERROR, "... outside of query");
    let query_depth = with_after_triggers(|at| at.query_depth);
    if query_depth < 0 {
        return Err(PgError::error(
            "AfterTriggerSaveEvent() called outside of query".to_string(),
        ));
    }
    with_after_triggers(|at| crate::queue::after_trigger_enlarge_query_state(at));

    let rel_oid = estate
        .result_rel(relinfo)
        .ri_RelationDesc
        .as_ref()
        .map(|r| r.rd_id)
        .expect("AfterTriggerSaveEvent: ResultRelInfo has no relation");

    // tgtype_event + cancel_prior_stmt_triggers per command.
    let tgtype_event = match event {
        TRIGGER_EVENT_INSERT => TRIGGER_TYPE_INSERT,
        TRIGGER_EVENT_DELETE => TRIGGER_TYPE_DELETE,
        TRIGGER_EVENT_UPDATE => TRIGGER_TYPE_UPDATE,
        other => {
            return Err(PgError::error(format!(
                "invalid after-trigger event code: {other}"
            )));
        }
    };
    // ItemPointerSetInvalid on both ctids; cancel any prior AS batch.
    crate::queue::cancel_prior_stmt_triggers(rel_oid, cmd_type, event & TRIGGER_EVENT_OPMASK);

    let new_event = types_nodes::trigger::AfterTriggerEventData {
        ate_flags: AFTER_TRIGGER_1CTID,
        ate_ctid1: ItemPointerData::default(),
        ate_ctid2: ItemPointerData::default(),
        ate_src_part: INVALID_OID,
        ate_dst_part: INVALID_OID,
    };

    let user_id = backend_utils_init_miscinit::GetUserId();

    // for (i = 0; i < trigdesc->numtriggers; i++) — match STATEMENT/AFTER/<op>.
    let trigs: Vec<(Oid, bool, bool)> = {
        let rri = estate.result_rel(relinfo);
        let trigdesc = rri
            .ri_TrigDesc
            .as_ref()
            .expect("AfterTriggerSaveEvent: ri_TrigDesc is NULL but stmt event reached save");
        let mut out = Vec::new();
        for trig in trigdesc.triggers.iter() {
            if !TRIGGER_TYPE_MATCHES(trig.tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, tgtype_event)
            {
                continue;
            }
            // A statement trigger has no WHEN clause; TriggerEnabled reduces to the
            // replication-role / tgenabled check.
            if !trigger_enabled_no_qual(trig.tgenabled) {
                continue;
            }
            out.push((trig.tgoid, trig.tgdeferrable, trig.tginitdeferred));
        }
        out
    };

    let qd = query_depth as usize;
    for (tgoid, tgdeferrable, tginitdeferred) in trigs {
        let new_shared = SharedRecord {
            // row_trigger == false: no TRIGGER_EVENT_ROW bit.
            ats_event: (event & TRIGGER_EVENT_OPMASK)
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
    estate: &mut EStateData<'mcx>,
    epqstate: &mut types_nodes::EPQState<'mcx>,
    relinfo: types_nodes::RriId,
    tupleid: Option<&ItemPointerData>,
    fdw_trigtuple: Option<&HeapTupleData<'mcx>>,
    epqslot: Option<&mut Option<types_nodes::SlotId>>,
    tmresult: Option<&mut types_tableam::tableam::TM_Result>,
    tmfd: &mut types_tableam::tableam::TM_FailureData,
    is_merge_delete: bool,
) -> PgResult<bool> {
    use types_catalog::pg_trigger::{
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_MATCHES, TRIGGER_TYPE_ROW,
    };

    let mcx = estate.es_query_cxt;

    // TupleTableSlot *slot = ExecGetTriggerOldSlot(estate, relinfo);
    let slot = backend_commands_trigger_seams::exec_get_trigger_old_slot::call(estate, relinfo)?;

    // Assert(HeapTupleIsValid(fdw_trigtuple) ^ ItemPointerIsValid(tupleid));
    if fdw_trigtuple.is_some() {
        // The FDW-supplied tuple leg (foreign table) goes through
        // ExecForceStoreHeapTuple into the OLD slot — not the common heap path.
        let _ = is_merge_delete;
        return Err(fdw_tuple_fetch_unported());
    }

    // Get + lock a copy of the on-disk tuple we are planning to delete, into the
    // OLD slot (GetTupleForTrigger).  `do_epq_recheck = !is_merge_delete`.
    let mut epqslot_candidate: Option<types_nodes::SlotId> = None;
    let tid = tupleid.expect("ExecBRDeleteTriggers: a non-FDW delete needs a tupleid");
    if !get_tuple_for_trigger(
        estate,
        epqstate,
        relinfo,
        tid,
        types_tableam::tableam::LockTupleMode::LockTupleExclusive,
        slot,
        /* do_epq_recheck */ !is_merge_delete,
        Some(&mut epqslot_candidate),
        tmresult,
        tmfd,
    )? {
        return Ok(false);
    }

    // If the tuple was concurrently updated and the caller wanted the updated
    // tuple, skip the trigger execution.
    if let Some(cand) = epqslot_candidate {
        if let Some(out) = epqslot {
            *out = Some(cand);
            return Ok(false);
        }
    }

    // trigtuple = ExecFetchSlotHeapTuple(slot, true, &should_free);
    let trigtuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx> = {
        let sd = estate.slot_data_mut(slot);
        let (formed, _should_free) =
            backend_executor_execTuples_seams::exec_fetch_slot_heap_tuple::call(mcx, sd, true)?;
        formed
    };

    // LocTriggerData.tg_event = TRIGGER_EVENT_DELETE | ROW | BEFORE;
    let tg_event = TRIGGER_EVENT_DELETE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;

    let numtriggers = {
        let rri = estate.result_rel(relinfo);
        rri.ri_TrigDesc.as_ref().map(|td| td.triggers.len()).unwrap_or(0)
    };

    let mut result = true;
    for i in 0..numtriggers {
        let (tgtype, tgenabled, tgoid, has_qual, tgnattr) = {
            let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[i];
            (trig.tgtype, trig.tgenabled, trig.tgoid, trig.tgqual.is_some(), trig.tgnattr)
        };

        // if (!TRIGGER_TYPE_MATCHES(tgtype, ROW, BEFORE, DELETE)) continue;
        if !TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE)
        {
            continue;
        }
        // if (!TriggerEnabled(estate, relinfo, trigger, tg_event, NULL, slot, NULL))
        //   continue;   (oldslot == slot; newslot == NULL on DELETE.)
        if !trigger_enabled(
            estate,
            relinfo,
            i,
            tgenabled,
            tgnattr,
            has_qual,
            tg_event,
            /* oldslot */ Some(slot),
            /* newslot */ None,
        )? {
            continue;
        }

        // newtuple = ExecCallTriggerFunc(&LocTriggerData, ...);
        let returned =
            fire_row_modify_trigger(estate, relinfo, i, tgoid, tg_event, slot, &trigtuple, None)?;
        match returned {
            // newtuple == NULL  ->  suppress the delete.
            None => {
                result = false;
                break;
            }
            // newtuple != NULL: a DELETE trigger's return is otherwise ignored
            // (heap_freetuple if != trigtuple — a no-op in the owned model).
            Some(_) => {}
        }
    }

    Ok(result)
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
    estate: &mut EStateData<'mcx>,
    epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
    relinfo: types_nodes::RriId,
    tupleid: Option<&ItemPointerData>,
    fdw_trigtuple: HeapTuple<'mcx>,
    newslot: types_nodes::SlotId,
    tmresult: Option<&mut types_tableam::tableam::TM_Result>,
    tmfd: &mut types_tableam::tableam::TM_FailureData,
    is_merge_update: bool,
) -> PgResult<bool> {
    use types_catalog::pg_trigger::{
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_MATCHES, TRIGGER_TYPE_ROW, TRIGGER_TYPE_UPDATE,
    };

    let mcx = estate.es_query_cxt;

    // TupleTableSlot *oldslot = ExecGetTriggerOldSlot(estate, relinfo);
    let oldslot = backend_commands_trigger_seams::exec_get_trigger_old_slot::call(estate, relinfo)?;

    // lockmode = ExecUpdateLockMode(estate, relinfo);
    let lockmode = backend_commands_trigger_seams::exec_update_lock_mode::call(estate, relinfo)?;

    // Assert(HeapTupleIsValid(fdw_trigtuple) ^ ItemPointerIsValid(tupleid));
    if fdw_trigtuple.is_some() {
        // The FDW-supplied tuple leg (foreign table) — ExecForceStoreHeapTuple
        // into the OLD slot; not the common heap path.
        return Err(fdw_tuple_fetch_unported());
    }

    // Get + lock a copy of the on-disk tuple we are planning to update, into the
    // OLD slot (GetTupleForTrigger).  `do_epq_recheck = !is_merge_update`.
    let mut epqslot_candidate: Option<types_nodes::SlotId> = None;
    let tid = tupleid.expect("ExecBRUpdateTriggers: a non-FDW update needs a tupleid");
    if !get_tuple_for_trigger(
        estate,
        epqstate,
        relinfo,
        tid,
        lockmode,
        oldslot,
        /* do_epq_recheck */ !is_merge_update,
        Some(&mut epqslot_candidate),
        tmresult,
        tmfd,
    )? {
        return Ok(false); // cancel the update action
    }

    // A concurrent READ-COMMITTED update would hand back a raw subplan tuple in
    // epqslot_candidate that must be re-formed via ExecGetUpdateNewTuple to
    // replace `newslot`.  GetTupleForTrigger only sets epqslot_candidate on the
    // `traversed` EPQ leg (a genuine concurrent update); the common clean-lock
    // path leaves it None, so a present value is the deferred EPQ-recheck leg.
    if epqslot_candidate.is_some() {
        return Err(epq_recheck_unported());
    }

    // trigtuple = ExecFetchSlotHeapTuple(oldslot, true, &should_free_trig);
    let trigtuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx> = {
        let sd = estate.slot_data_mut(oldslot);
        let (formed, _should_free) =
            backend_executor_execTuples_seams::exec_fetch_slot_heap_tuple::call(mcx, sd, true)?;
        formed
    };

    // LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | ROW | BEFORE;
    let tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    // updatedCols = ExecGetAllUpdatedCols(relinfo, estate); LocTriggerData.tg_updatedcols
    // — only consulted by a column-specific (tgattr) trigger and the WHEN qual's
    // tg_updatedcols accessor, which the common (no-tgattr) path does not exercise.

    let numtriggers = {
        let rri = estate.result_rel(relinfo);
        rri.ri_TrigDesc.as_ref().map(|td| td.triggers.len()).unwrap_or(0)
    };

    // newtuple == NULL until first materialized (ExecFetchSlotHeapTuple(newslot)).
    let mut newtuple: Option<
        types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    > = None;

    for i in 0..numtriggers {
        let (tgtype, tgenabled, tgoid, has_qual, tgnattr) = {
            let trig = &estate.result_rel(relinfo).ri_TrigDesc.as_ref().unwrap().triggers[i];
            (trig.tgtype, trig.tgenabled, trig.tgoid, trig.tgqual.is_some(), trig.tgnattr)
        };

        // if (!TRIGGER_TYPE_MATCHES(tgtype, ROW, BEFORE, UPDATE)) continue;
        if !TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE)
        {
            continue;
        }
        // if (!TriggerEnabled(estate, relinfo, trigger, tg_event, updatedCols,
        //                     oldslot, newslot)) continue;
        if !trigger_enabled(
            estate,
            relinfo,
            i,
            tgenabled,
            tgnattr,
            has_qual,
            tg_event,
            /* oldslot */ Some(oldslot),
            /* newslot */ Some(newslot),
        )? {
            continue;
        }

        // if (!newtuple) newtuple = ExecFetchSlotHeapTuple(newslot, true, &should_free_new);
        if newtuple.is_none() {
            let sd = estate.slot_data_mut(newslot);
            let (formed, _should_free) =
                backend_executor_execTuples_seams::exec_fetch_slot_heap_tuple::call(mcx, sd, true)?;
            newtuple = Some(formed);
        }
        // oldtuple = newtuple — the NEW row handed to this trigger as tg_newtuple.
        let oldtuple = newtuple.clone().unwrap();

        // newtuple = ExecCallTriggerFunc(&LocTriggerData, ...);  (tg_trigtuple =
        // trigtuple [OLD], tg_newtuple = newtuple [NEW], tg_trigslot = oldslot,
        // tg_newslot = newslot.)
        let returned = fire_row_modify_trigger(
            estate,
            relinfo,
            i,
            tgoid,
            tg_event,
            oldslot,
            &trigtuple,
            Some((newslot, &oldtuple)),
        )?;

        match returned {
            // newtuple == NULL  ->  "do nothing".
            None => return Ok(false),
            Some(rt) => {
                if formed_tuple_same(&rt, &oldtuple) {
                    // newtuple == oldtuple — row unchanged; keep it cached.
                    newtuple = Some(rt);
                } else {
                    // newtuple != oldtuple — the trigger modified the NEW row.
                    // (check_modified_virtual_generated is a no-op without virtual
                    // generated columns.)  ExecForceStoreHeapTuple(newtuple, newslot, false);
                    backend_executor_execTuples_seams::exec_force_store_formed_heap_tuple::call(
                        estate,
                        newslot,
                        rt.clone_in(mcx)?,
                        false,
                    )?;
                    // signal tuple should be re-fetched if used.
                    newtuple = None;
                }
            }
        }
    }

    Ok(true)
}

/// `GetTupleForTrigger(estate, epqstate, relinfo, tid, lockmode, oldslot,
/// do_epq_recheck, epqslot, tmresultp, tmfdp)` (trigger.c:3345) — fetch + lock
/// the OLD on-disk tuple identified by `tid` into `oldslot`.
///
/// The `epqslot != NULL` branch (the firing path always passes one): lock the
/// tuple with `table_tuple_lock` and dispatch on the `TM_Result`.  The common
/// clean-lock outcome (`TM_Ok`, not `traversed`) returns `true` with the OLD
/// row in `oldslot`.  `TM_SelfModified` with `cmax != es_output_cid` is the
/// faithful "tuple already modified by an operation triggered by the current
/// command" error; the deleted/self-deleted cases return `false` (skip).
///
/// The concurrent-update EPQ re-execution (`TM_Ok && traversed && do_epq_recheck`
/// → `EvalPlanQual`) needs the EPQ sub-plan re-run, which is the deferred leg —
/// a precise loud error.  When `do_epq_recheck` is false (a MERGE action), the
/// `traversed` case returns `false` with `TM_Updated`, exactly as C.
#[allow(clippy::too_many_arguments)]
fn get_tuple_for_trigger<'mcx>(
    estate: &mut EStateData<'mcx>,
    _epqstate: &mut types_nodes::EPQState<'mcx>,
    relinfo: types_nodes::RriId,
    tid: &ItemPointerData,
    lockmode: types_tableam::tableam::LockTupleMode,
    oldslot: types_nodes::SlotId,
    do_epq_recheck: bool,
    epqslot: Option<&mut Option<types_nodes::SlotId>>,
    mut tmresultp: Option<&mut types_tableam::tableam::TM_Result>,
    tmfdp: &mut types_tableam::tableam::TM_FailureData,
) -> PgResult<bool> {
    use types_tableam::tableam::TM_Result;

    // The firing path always passes epqslot != NULL; the no-epqslot branch
    // (table_tuple_fetch_row_version under SnapshotAny) is the AFTER-fetch leg,
    // handled by `fetch_trigger_tuple` elsewhere.
    let epqslot = match epqslot {
        Some(e) => e,
        None => {
            // *oldslot = table_tuple_fetch_row_version(rel, tid, SnapshotAny);
            if !backend_commands_trigger_seams::get_tuple_for_trigger_fetch::call(
                estate, relinfo, tid, oldslot,
            )? {
                return Err(PgError::error("failed to fetch tuple for trigger"));
            }
            return Ok(true);
        }
    };
    *epqslot = None;

    // if (!IsolationUsesXactSnapshot()) lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
    let find_last_version =
        !backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call();

    // test = table_tuple_lock(relation, tid, es_snapshot, oldslot, es_output_cid,
    //                         lockmode, LockWaitBlock, lockflags, &tmfd);
    let test = backend_commands_trigger_seams::get_tuple_for_trigger_lock::call(
        estate,
        relinfo,
        tid,
        oldslot,
        lockmode,
        find_last_version,
        tmfdp,
    )?;

    // Let the caller know about the status of this operation.
    if let Some(r) = tmresultp.as_deref_mut() {
        *r = test;
    }

    match test {
        TM_Result::TM_SelfModified => {
            // The target tuple was already updated/deleted by the current command,
            // or by a later command in the current transaction.  Ignore the former,
            // throw in the latter.
            if tmfdp.cmax != estate.es_output_cid {
                return Err(PgError::error(
                    "tuple to be updated was already modified by an operation \
                     triggered by the current command",
                )
                .with_sqlstate(types_error::ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
                .with_hint(
                    "Consider using an AFTER trigger instead of a BEFORE trigger \
                     to propagate changes to other rows.",
                ));
            }
            // treat it as deleted; do not process
            Ok(false)
        }
        TM_Result::TM_Ok => {
            if tmfdp.traversed {
                // Recheck the tuple using EPQ, if requested.  This is the
                // concurrent-update re-execution sub-tree — deferred (the
                // LockRows lane left epq_needed false on TM_Ok).
                if do_epq_recheck {
                    Err(epq_recheck_unported())
                } else {
                    // Just return that it was concurrently updated.
                    if let Some(r) = tmresultp.as_deref_mut() {
                        *r = TM_Result::TM_Updated;
                    }
                    Ok(false)
                }
            } else {
                Ok(true)
            }
        }
        TM_Result::TM_Updated => {
            if backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call() {
                return Err(PgError::error(
                    "could not serialize access due to concurrent update",
                )
                .with_sqlstate(types_error::ERRCODE_T_R_SERIALIZATION_FAILURE));
            }
            Err(PgError::error(format!(
                "unexpected table_tuple_lock status: {:?}",
                test
            )))
        }
        TM_Result::TM_Deleted => {
            if backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call() {
                return Err(PgError::error(
                    "could not serialize access due to concurrent delete",
                )
                .with_sqlstate(types_error::ERRCODE_T_R_SERIALIZATION_FAILURE));
            }
            // tuple was deleted
            Ok(false)
        }
        TM_Result::TM_Invisible => {
            Err(PgError::error("attempted to lock invisible tuple"))
        }
        other => Err(PgError::error(format!(
            "unrecognized table_tuple_lock status: {:?}",
            other
        ))),
    }
}

#[cold]
#[inline(never)]
fn epq_recheck_unported() -> PgError {
    PgError::error(
        "GetTupleForTrigger: the target tuple was concurrently updated \
         (TM_Ok && traversed) — the EvalPlanQual re-execution sub-tree \
         (EvalPlanQual / ExecGetUpdateNewTuple) is not yet ported; the common \
         clean-lock (TM_Ok) BEFORE-ROW UPDATE/DELETE path runs end-to-end",
    )
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
    // The C `get_trigger_oid` allocates in CurrentMemoryContext; the inward seam
    // carries no `mcx`, so wrap the scan in a scratch context (cf.
    // RemoveRewriteRuleById's install wrapper).
    let ctx = mcx::MemoryContext::new("get_trigger_oid");
    get_trigger_oid_scan(ctx.mcx(), relid, trigname, missing_ok)
}

/// `get_trigger_oid(relid, trigname, missing_ok)` (trigger.c:1371-1415) — open
/// `pg_trigger`, `systable_beginscan` over `TriggerRelidNameIndexId` keyed on
/// `(tgrelid = relid, tgname = trigname)`, return the first matching row's oid.
fn get_trigger_oid_scan<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    trigname: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    use backend_access_common_scankey::ScanKeyInit;
    use backend_access_index_genam_seams as genam_seams;
    use types_catalog::pg_trigger as pt;
    use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_storage::lock::AccessShareLock;
    use types_tuple::backend_access_common_heaptuple::Datum as ScanDatum;

    // tgrel = table_open(TriggerRelationId, AccessShareLock);
    let tgrel =
        backend_access_table_table_seams::table_open::call(mcx, pt::TriggerRelationId, AccessShareLock)?;

    // ScanKeyInit(&skey[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, relid)
    let mut k0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k0,
        pt::Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ScanDatum::from_oid(relid),
    )?;
    // ScanKeyInit(&skey[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, trigname)
    let mut k1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k1,
        pt::Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        ScanDatum::ByRef(mcx::slice_in(mcx, trigname.as_bytes())?),
    )?;
    let keys = [k0, k1];

    // tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 2, skey);
    let mut scan = genam_seams::systable_beginscan::call(
        &tgrel,
        pt::TriggerRelidNameIndexId,
        true,
        None,
        &keys,
    )?;

    // tup = systable_getnext(tgscan);
    let oid = if let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? {
        // oid = ((Form_pg_trigger) GETSTRUCT(tup))->oid;
        let cols = backend_access_common_heaptuple::heap_deform_tuple(
            mcx,
            &tup.tuple,
            &tgrel.rd_att,
            &tup.data,
        )?;
        cols[pt::Anum_pg_trigger_oid as usize - 1].0.as_oid()
    } else if !missing_ok {
        // ereport(ERROR, ERRCODE_UNDEFINED_OBJECT,
        //   "trigger \"%s\" for table \"%s\" does not exist", trigname, get_rel_name(relid));
        let relname = backend_utils_cache_lsyscache::relation::get_rel_name(mcx, relid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let _ = scan;
        tgrel.close(AccessShareLock)?;
        return Err(trigger_not_found(trigname, &relname));
    } else {
        INVALID_OID
    };

    // systable_endscan(tgscan); table_close(tgrel, AccessShareLock);
    let _ = scan;
    tgrel.close(AccessShareLock)?;
    Ok(oid)
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
// AfterTriggerSetState (trigger.c:5767) — SET CONSTRAINTS.
// ===========================================================================

/// Inward seam for `AfterTriggerSetState(ConstraintsSetStmt *stmt)`
/// (trigger.c:5767) — `SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED |
/// IMMEDIATE }`. The dispatcher hands the rich `ConstraintsSetStmt` node; this
/// allocates the catalog-scan scratch in a private context (the bare seam
/// carries no `mcx`, cf. `get_trigger_oid_impl`).
fn after_trigger_set_state_seam<'mcx>(stmt: &types_nodes::nodes::Node<'mcx>) -> PgResult<()> {
    let css = match stmt.as_constraintssetstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "after_trigger_set_state_seam: statement is not a ConstraintsSetStmt",
            ))
        }
    };
    let ctx = mcx::MemoryContext::new("AfterTriggerSetState");
    after_trigger_set_state(ctx.mcx(), css)
}

/// `AfterTriggerSetState(ConstraintsSetStmt *stmt)` (trigger.c:5767).
fn after_trigger_set_state<'mcx, 'n>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::ddlnodes::ConstraintsSetStmt<'n>,
) -> PgResult<()> {
    use crate::queue::{
        set_constraint_state_add_item, set_constraint_state_copy, set_constraint_state_create,
    };

    // int my_level = GetCurrentTransactionNestLevel();
    let my_level = backend_access_transam_xact_seams::get_current_transaction_nest_level::call();

    // If we haven't already done so, initialize our state; and, if in a
    // subtransaction and we didn't save the current state already, save it so it
    // can be restored on subtransaction abort.
    with_after_triggers(|at| {
        if at.state.is_none() {
            at.state = Some(set_constraint_state_create(8));
        }
        if my_level > 1 {
            let lvl = my_level as usize;
            if lvl < at.trans_stack.len() && at.trans_stack[lvl].state.is_none() {
                let copy = set_constraint_state_copy(at.state.as_ref().unwrap());
                at.trans_stack[lvl].state = Some(copy);
            }
        }
    });

    if stmt.constraints.is_empty() {
        // SET CONSTRAINTS ALL ...
        with_after_triggers(|at| {
            let state = at.state.as_mut().unwrap();
            state.numstates = 0;
            state.trigstates.clear();
            state.all_isset = true;
            state.all_isdeferred = stmt.deferred;
        });
    } else {
        // SET CONSTRAINTS constraint-name [, ...]
        let tgoidlist = resolve_constraint_trigger_oids(mcx, stmt)?;

        // Set the trigger states of individual triggers for this xact.
        with_after_triggers(|at| {
            for &tgoid in &tgoidlist {
                let state = at.state.as_mut().unwrap();
                let mut found = false;
                for i in 0..(state.numstates as usize) {
                    if state.trigstates[i].sct_tgoid == tgoid {
                        state.trigstates[i].sct_tgisdeferred = stmt.deferred;
                        found = true;
                        break;
                    }
                }
                if !found {
                    set_constraint_state_add_item(
                        at.state.as_mut().unwrap(),
                        tgoid,
                        stmt.deferred,
                    );
                }
            }
        });
    }

    // SQL99: setting a constraint to IMMEDIATE fires any of its now-immediate
    // deferred events retroactively. A SET ... DEFERRED can't convert any unfired
    // event to immediate, so nothing to do in that case.
    if !stmt.deferred {
        let has_events = with_after_triggers(|at| !at.events.events.is_empty());
        if has_events {
            // The retroactive firing cycle pushes the transaction snapshot and
            // runs afterTriggerMarkEvents/afterTriggerInvokeEvents against a NULL
            // EState. In this tree afterTriggerInvokeEvents requires an
            // &mut EStateData and there is no reachable PushActiveSnapshot seam
            // (identical boundary to AfterTriggerFireDeferred); keep it loud
            // rather than silently dropping the now-immediate events. A SET
            // CONSTRAINTS issued before any deferred event is queued (the common
            // case) takes the empty-queue path above and returns cleanly.
            return Err(PgError::error(
                "AfterTriggerSetState (trigger.c:6027): retroactively firing now-immediate \
                 deferred triggers needs PushActiveSnapshot(GetTransactionSnapshot()) + a \
                 per-query EState for the afterTriggerInvokeEvents cycle, not yet ported"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

/// trigger.c:5802-6013 — resolve `stmt->constraints` (named constraints) to the
/// list of deferrable trigger OIDs implementing them, including descendant
/// constraints in partitions. Returns the `tgoidlist`.
fn resolve_constraint_trigger_oids<'mcx, 'n>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::ddlnodes::ConstraintsSetStmt<'n>,
) -> PgResult<Vec<Oid>> {
    use backend_access_common_scankey::ScanKeyInit;
    use backend_access_index_genam_seams as genam_seams;
    use types_catalog::pg_constraint as pc;
    use types_catalog::pg_trigger as pt;
    use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_storage::lock::AccessShareLock;
    use types_tuple::backend_access_common_heaptuple::Datum as ScanDatum;

    let mut conoidlist: Vec<Oid> = Vec::new();

    // conrel = table_open(ConstraintRelationId, AccessShareLock);
    let conrel = backend_access_table_table_seams::table_open::call(
        mcx,
        types_catalog::catalog::CONSTRAINT_RELATION_ID,
        AccessShareLock,
    )?;

    for cnode in stmt.constraints.iter() {
        let constraint = match cnode.as_rangevar() {
            Some(rv) => rv,
            None => {
                conrel.close(AccessShareLock)?;
                return Err(PgError::error(
                    "SET CONSTRAINTS: constraint name element is not a RangeVar",
                ));
            }
        };
        let relname = constraint
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();

        // catalogname: only our own database is referenceable.
        if let Some(cat) = constraint.catalogname.as_ref() {
            let dbname = backend_commands_dbcommands_seams::get_database_name::call(
                mcx,
                backend_utils_init_small_seams::my_database_id::call(),
            )?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
            if cat.as_str() != dbname {
                let schemaname = constraint
                    .schemaname
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                conrel.close(AccessShareLock)?;
                return Err(PgError::error(format!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    cat.as_str(),
                    schemaname,
                    relname
                ))
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
            }
        }

        // namespacelist: explicit schema, else the active search path.
        let namespacelist: Vec<Oid> = if let Some(sch) = constraint.schemaname.as_ref() {
            let nsoid = backend_catalog_namespace_seams::lookup_explicit_namespace::call(
                sch.as_str(),
                false,
            )?;
            vec![nsoid]
        } else {
            backend_catalog_namespace_seams::fetch_search_path::call(mcx, true)?
                .iter()
                .copied()
                .collect()
        };

        let mut found = false;
        for &namespace_id in &namespacelist {
            // ScanKeyInit conname = relname, connamespace = namespaceId.
            let mut k0 = ScanKeyData::empty();
            ScanKeyInit(
                &mut k0,
                pc::Anum_pg_constraint_conname,
                BTEqualStrategyNumber,
                F_NAMEEQ,
                ScanDatum::ByRef(mcx::slice_in(mcx, relname.as_bytes())?),
            )?;
            let mut k1 = ScanKeyData::empty();
            ScanKeyInit(
                &mut k1,
                pc::Anum_pg_constraint_connamespace,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ScanDatum::from_oid(namespace_id),
            )?;
            let keys = [k0, k1];

            let mut conscan = genam_seams::systable_beginscan::call(
                &conrel,
                pc::ConstraintNameNspIndexId,
                true,
                None,
                &keys,
            )?;

            while let Some(tup) =
                genam_seams::systable_getnext::call(mcx, conscan.desc_mut())?
            {
                let cols = backend_access_common_heaptuple::heap_deform_tuple(
                    mcx,
                    &tup.tuple,
                    &conrel.rd_att,
                    &tup.data,
                )?;
                let condeferrable =
                    cols[pc::Anum_pg_constraint_condeferrable as usize - 1].0.as_bool();
                if condeferrable {
                    let conoid = cols[pc::Anum_pg_constraint_oid as usize - 1].0.as_oid();
                    conoidlist.push(conoid);
                } else if stmt.deferred {
                    let _ = conscan;
                    conrel.close(AccessShareLock)?;
                    return Err(PgError::error(format!(
                        "constraint \"{relname}\" is not deferrable"
                    ))
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE));
                }
                found = true;
            }
            let _ = conscan;

            // Once a matching constraint is found, do not search later path parts.
            if found {
                break;
            }
        }

        if !found {
            conrel.close(AccessShareLock)?;
            return Err(PgError::error(format!(
                "constraint \"{relname}\" does not exist"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    }

    // Scan for descendants of the constraints, appending to the same list we are
    // scanning so further descendants are caught too.
    let mut idx = 0;
    while idx < conoidlist.len() {
        let parent = conoidlist[idx];
        idx += 1;

        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            pc::Anum_pg_constraint_conparentid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ScanDatum::from_oid(parent),
        )?;
        let keys = [key];

        let mut scan = genam_seams::systable_beginscan::call(
            &conrel,
            pc::ConstraintParentIndexId,
            true,
            None,
            &keys,
        )?;
        while let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? {
            let cols = backend_access_common_heaptuple::heap_deform_tuple(
                mcx,
                &tup.tuple,
                &conrel.rd_att,
                &tup.data,
            )?;
            let conoid = cols[pc::Anum_pg_constraint_oid as usize - 1].0.as_oid();
            conoidlist.push(conoid);
        }
        let _ = scan;
    }

    conrel.close(AccessShareLock)?;

    // Locate the deferrable trigger(s) implementing each constraint.
    let mut tgoidlist: Vec<Oid> = Vec::new();
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        AccessShareLock,
    )?;

    for &conoid in &conoidlist {
        let mut skey = ScanKeyData::empty();
        ScanKeyInit(
            &mut skey,
            pt::Anum_pg_trigger_tgconstraint,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ScanDatum::from_oid(conoid),
        )?;
        let keys = [skey];

        let mut tgscan = genam_seams::systable_beginscan::call(
            &tgrel,
            pt::TriggerConstraintIndexId,
            true,
            None,
            &keys,
        )?;
        while let Some(htup) = genam_seams::systable_getnext::call(mcx, tgscan.desc_mut())? {
            let cols = backend_access_common_heaptuple::heap_deform_tuple(
                mcx,
                &htup.tuple,
                &tgrel.rd_att,
                &htup.data,
            )?;
            // Silently skip triggers marked non-deferrable in pg_trigger: a
            // deferrable RI constraint may have some non-deferrable actions.
            let tgdeferrable =
                cols[pt::Anum_pg_trigger_tgdeferrable as usize - 1].0.as_bool();
            if tgdeferrable {
                let tgoid = cols[pt::Anum_pg_trigger_oid as usize - 1].0.as_oid();
                tgoidlist.push(tgoid);
            }
        }
        let _ = tgscan;
    }

    tgrel.close(AccessShareLock)?;

    Ok(tgoidlist)
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

    // SET CONSTRAINTS (utility.c `T_ConstraintsSetStmt`).
    backend_tcop_utility_out_seams::after_trigger_set_state::set(after_trigger_set_state_seam);

    // FK phase-3 validation scan (validateForeignKeyConstraint), called from
    // ALTER TABLE ADD/ALTER CONSTRAINT through tablecmds.
    s::validate_foreign_key_constraint::set(validate_foreign_key_constraint);

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
    // BEFORE/INSTEAD-OF row trigger return-tuple channel (the owned analogue of
    // C's `(HeapTuple) DatumGetPointer(result)`); the PL/C trigger handlers
    // deposit the returned row here.
    s::set_before_trigger_result_tuple::set(set_before_trigger_result_tuple_impl);

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
