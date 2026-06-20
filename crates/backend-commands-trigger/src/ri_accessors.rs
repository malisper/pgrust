//! The `TriggerData *` / `Trigger *` field accessors `ri_triggers.c` reads off
//! the trigger manager's per-call context (the analogue of C's
//! `(TriggerData *) fcinfo->context` casts and the `trigdata->tg_*` /
//! `trigger->tg*` field reads, plus the `RIAtt*` tuple-descriptor lookups on
//! `trigdata->tg_relation`).
//!
//! In C these are plain pointer-field reads on the `TriggerData` the trigger
//! manager passed in `fcinfo->context`.  The idiomatic `function_call_invoke`
//! seam carries no payload-bearing context, so the live `TriggerData` rides the
//! `CURRENT_TRIGGER_DATA` thread-local side-channel (set by
//! [`crate::firing::exec_call_trigger_func`] for the duration of the call) — the
//! owned analogue of the stack `LocTriggerData`.  These accessors resolve their
//! opaque [`TriggerDataRef`]/[`TriggerRef`] handle to that current `TriggerData`
//! and read the requested field, exactly as the C macro dereferences
//! `fcinfo->context`.
//!
//! Because there is at most one `TriggerData` in flight per backend at a time
//! (the recursion-scoped side-channel), the handles are stable markers for "the
//! current trigger data" (`TriggerDataRef`) and "its `tg_trigger`"
//! (`TriggerRef`); the firing path mints them only while a `TriggerData` is
//! installed.  A handle read outside a trigger call is the C `CALLED_AS_TRIGGER`
//! being false — a trigger-protocol violation the RI procs reject up front.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED};
use types_ri_triggers::{TriggerDataRef, TriggerRef, TupleTableSlotRef};
use types_tuple::access::RELKIND_PARTITIONED_TABLE;

use crate::firing::with_current_trigger_data;

/// The marker handle value the firing path mints for "the current
/// `TriggerData`" and "its `tg_trigger`".  The handle carries no payload (the
/// payload is the thread-local); a non-zero discriminant distinguishes it from
/// the C `NULL` pointer.
pub(crate) const CURRENT: u64 = 1;
/// `TupleTableSlotRef` discriminant for `trigdata->tg_trigslot`.
pub(crate) const SLOT_TRIG: u64 = 1;
/// `TupleTableSlotRef` discriminant for `trigdata->tg_newslot`.
pub(crate) const SLOT_NEW: u64 = 2;

/// `(TriggerData *) fcinfo->context` is `NULL` (not called as a trigger), or the
/// dereferenced field was the C `NULL` the trigger protocol does not permit
/// here.  Mirrors the `elog`/`ereport` the RI procs raise on a protocol
/// violation.
fn not_a_trigger_context(what: &str) -> PgError {
    PgError::error(format!(
        "trigger manager accessor {what} called without an active TriggerData context"
    ))
    .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED)
}

/// `CALLED_AS_TRIGGER(fcinfo)` — is a `TriggerData` in flight on this backend?
/// (The handle is a marker for the current context; the real test is whether
/// the side-channel holds a `TriggerData`.)
pub fn called_as_trigger_impl(_trigdata: TriggerDataRef) -> bool {
    with_current_trigger_data(|td| td.is_some())
}

/// `trigdata->tg_event`.
pub fn tg_event_impl(_trigdata: TriggerDataRef) -> u32 {
    with_current_trigger_data(|td| td.map(|t| t.tg_event))
        .unwrap_or_else(|| panic!("tg_event: no active TriggerData context"))
}

/// `trigdata->tg_trigger` — hand back the marker for the current trigger.  (The
/// `Trigger` itself lives in the thread-local `TriggerData`; the marker resolves
/// to it in the `trigger_*` accessors below.)
pub fn tg_trigger_impl(_trigdata: TriggerDataRef) -> TriggerRef {
    with_current_trigger_data(|td| {
        if td.and_then(|t| t.tg_trigger.as_ref()).is_none() {
            panic!("tg_trigger: TriggerData has no tg_trigger (NULL)");
        }
    });
    TriggerRef(CURRENT)
}

/// `trigdata->tg_trigslot`.
pub fn tg_trigslot_impl(_trigdata: TriggerDataRef) -> TupleTableSlotRef {
    TupleTableSlotRef(SLOT_TRIG)
}

/// `trigdata->tg_newslot`.
pub fn tg_newslot_impl(_trigdata: TriggerDataRef) -> TupleTableSlotRef {
    TupleTableSlotRef(SLOT_NEW)
}

// ---- tg_relation field reads (RelationGetRelid / Namespace / Owner / relkind,
//      RIAttName / RIAttType / RIAttCollation) -------------------------------

/// Run `f` with the current `TriggerData`'s `tg_relation`, or raise the protocol
/// violation when there is no context / a NULL `tg_relation` (the C macros all
/// dereference `trigdata->tg_relation`, which is never NULL in a real RI call).
fn with_tg_relation<R>(
    what: &str,
    f: impl FnOnce(&types_rel::Relation<'static>) -> R,
) -> PgResult<R> {
    with_current_trigger_data(|td| match td.and_then(|t| t.tg_relation.as_ref()) {
        Some(rel) => Ok(f(rel)),
        None => Err(not_a_trigger_context(what)),
    })
}

/// `RelationGetRelid(trigdata->tg_relation)`.
pub fn tg_relation_oid_impl(_trigdata: TriggerDataRef) -> Oid {
    with_tg_relation("tg_relation_oid", |rel| rel.rd_id)
        .unwrap_or_else(|_| panic!("tg_relation_oid: no active tg_relation"))
}

/// `RelationGetRelationName(trigdata->tg_relation)` — the `relname` bytes,
/// copied into `mcx`.
pub fn tg_relation_name_impl<'mcx>(
    mcx: Mcx<'mcx>,
    _trigdata: TriggerDataRef,
) -> PgResult<PgVec<'mcx, u8>> {
    with_tg_relation("tg_relation_name", |rel| {
        mcx::slice_in(mcx, rel.rd_rel.relname.as_bytes())
    })?
}

/// `RelationGetNamespace(trigdata->tg_relation)`.
pub fn tg_relation_namespace_impl(_trigdata: TriggerDataRef) -> Oid {
    with_tg_relation("tg_relation_namespace", |rel| rel.rd_rel.relnamespace)
        .unwrap_or_else(|_| panic!("tg_relation_namespace: no active tg_relation"))
}

/// `trigdata->tg_relation->rd_rel->relowner`.
pub fn tg_relation_owner_impl(_trigdata: TriggerDataRef) -> Oid {
    with_tg_relation("tg_relation_owner", |rel| rel.rd_rel.relowner)
        .unwrap_or_else(|_| panic!("tg_relation_owner: no active tg_relation"))
}

/// `trigdata->tg_relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE`.
pub fn tg_relation_is_partitioned_impl(_trigdata: TriggerDataRef) -> bool {
    with_tg_relation("tg_relation_is_partitioned", |rel| {
        rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
    })
    .unwrap_or_else(|_| panic!("tg_relation_is_partitioned: no active tg_relation"))
}

/// Resolve a 1-based `attnum` to its 0-based `rd_att` index, panicking on a
/// system / out-of-range column exactly as `TupleDescAttr` would on a bad index
/// (RI only ever passes user-column attnums it read from the constraint).
#[inline]
fn att_index(rel: &types_rel::Relation<'static>, attnum: i16) -> usize {
    let idx = (attnum as isize) - 1;
    if idx < 0 || idx as usize >= rel.rd_att.attrs.len() {
        panic!("RIAtt*: attnum {attnum} out of range for relation {}", rel.rd_id);
    }
    idx as usize
}

/// `RIAttName(trigdata->tg_relation, attnum)` — the attribute `attname` bytes,
/// copied into `mcx`.
pub fn tg_relation_att_name_impl<'mcx>(
    mcx: Mcx<'mcx>,
    _trigdata: TriggerDataRef,
    attnum: i16,
) -> PgResult<PgVec<'mcx, u8>> {
    with_tg_relation("tg_relation_att_name", |rel| {
        mcx::slice_in(mcx, rel.rd_att.attr(att_index(rel, attnum)).attname.name_str())
    })?
}

/// `RIAttType(trigdata->tg_relation, attnum)` (`attnumTypeId`).
pub fn tg_relation_att_type_impl(_trigdata: TriggerDataRef, attnum: i16) -> Oid {
    with_tg_relation("tg_relation_att_type", |rel| {
        rel.rd_att.attr(att_index(rel, attnum)).atttypid
    })
    .unwrap_or_else(|_| panic!("tg_relation_att_type: no active tg_relation"))
}

/// `RIAttCollation(trigdata->tg_relation, attnum)` (`attnumCollationId`).
pub fn tg_relation_att_collation_impl(_trigdata: TriggerDataRef, attnum: i16) -> Oid {
    with_tg_relation("tg_relation_att_collation", |rel| {
        rel.rd_att.attr(att_index(rel, attnum)).attcollation
    })
    .unwrap_or_else(|_| panic!("tg_relation_att_collation: no active tg_relation"))
}

/// `trigdata->tg_trigtuple` — the OLD/row-being-modified `HeapTuple` the trigger
/// manager handed the trigger function, copied into `mcx`. `Ok(None)` mirrors a
/// NULL `tg_trigtuple`.
pub fn tg_trigtuple_impl<'mcx>(
    mcx: Mcx<'mcx>,
    _trigdata: TriggerDataRef,
) -> PgResult<Option<types_tuple::heaptuple::HeapTupleData<'mcx>>> {
    with_current_trigger_data(|td| match td.and_then(|t| t.tg_trigtuple.as_ref()) {
        Some(tup) => Ok(Some(tup.clone_in(mcx)?)),
        None => Ok(None),
    })
}

/// `trigdata->tg_newtuple` — the NEW `HeapTuple` (for an UPDATE) the trigger
/// manager handed the trigger function, copied into `mcx`. `Ok(None)` mirrors a
/// NULL `tg_newtuple`.
pub fn tg_newtuple_impl<'mcx>(
    mcx: Mcx<'mcx>,
    _trigdata: TriggerDataRef,
) -> PgResult<Option<types_tuple::heaptuple::HeapTupleData<'mcx>>> {
    with_current_trigger_data(|td| match td.and_then(|t| t.tg_newtuple.as_ref()) {
        Some(tup) => Ok(Some(tup.clone_in(mcx)?)),
        None => Ok(None),
    })
}

// ---- trigger field reads (trigger->tg*) -----------------------------------

/// Run `f` with the current `TriggerData`'s `tg_trigger`, or raise the protocol
/// violation when there is no context / a NULL `tg_trigger`.
fn with_tg_trigger<R>(
    what: &str,
    f: impl FnOnce(&types_trigger::Trigger<'static>) -> R,
) -> PgResult<R> {
    with_current_trigger_data(|td| match td.and_then(|t| t.tg_trigger.as_ref()) {
        Some(trig) => Ok(f(trig)),
        None => Err(not_a_trigger_context(what)),
    })
}

/// `trigger->tgconstraint`.
pub fn trigger_constraint_impl(trigger: TriggerRef) -> Oid {
    debug_assert_eq!(trigger.0, CURRENT);
    with_tg_trigger("trigger_constraint", |t| t.tgconstraint)
        .unwrap_or_else(|_| panic!("trigger_constraint: no active tg_trigger"))
}

/// `trigger->tgconstrrelid`.
pub fn trigger_constrrelid_impl(trigger: TriggerRef) -> Oid {
    debug_assert_eq!(trigger.0, CURRENT);
    with_tg_trigger("trigger_constrrelid", |t| t.tgconstrrelid)
        .unwrap_or_else(|_| panic!("trigger_constrrelid: no active tg_trigger"))
}

/// `trigger->tgconstrindid`.
pub fn trigger_constrindid_impl(trigger: TriggerRef) -> Oid {
    debug_assert_eq!(trigger.0, CURRENT);
    with_tg_trigger("trigger_constrindid", |t| t.tgconstrindid)
        .unwrap_or_else(|_| panic!("trigger_constrindid: no active tg_trigger"))
}

/// `trigger->tgname` — the trigger-name bytes, copied into `mcx`.
pub fn trigger_name_impl<'mcx>(
    mcx: Mcx<'mcx>,
    trigger: TriggerRef,
) -> PgResult<PgVec<'mcx, u8>> {
    debug_assert_eq!(trigger.0, CURRENT);
    with_tg_trigger("trigger_name", |t| mcx::slice_in(mcx, t.tgname.as_bytes()))?
}

/// `trigger->tgnargs` — number of textual trigger arguments (`TG_NARGS`).
pub fn tg_nargs_impl(_trigdata: TriggerDataRef) -> i32 {
    with_tg_trigger("tg_nargs", |t| t.tgnargs as i32)
        .unwrap_or_else(|_| panic!("tg_nargs: no active tg_trigger"))
}

/// `trigger->tgargs[i]` — the i-th textual trigger argument (`TG_ARGV[i]`),
/// copied into `mcx`. `Ok(None)` for an out-of-range index.
pub fn tg_argv_impl<'mcx>(
    mcx: Mcx<'mcx>,
    _trigdata: TriggerDataRef,
    i: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    with_tg_trigger("tg_argv", |t| {
        if i < 0 || i as usize >= t.tgargs.len() {
            Ok(None)
        } else {
            Ok(Some(mcx::slice_in(mcx, t.tgargs[i as usize].as_bytes())?))
        }
    })?
}

/// Install the `TriggerData`/`Trigger` field accessors that resolve off the
/// current-trigger side-channel.  The slot-value accessors (`slot_getattr`,
/// `slot_attisnull`, `slot_tid`, `slot_is_current_xact_tuple`,
/// `pk_datum_image_eq`, `tg_relation_tuple_satisfies_snapshot_self`), the live
/// `tg_relation` carrier, and the DDL catalog-write leg (`RemoveTriggerById`,
/// `renametrig`, `create_unique_key_recheck_trigger`) are NOT installed here:
/// they require the per-row AFTER-trigger firing substrate (EState-owned slot
/// materialization + heap-scan family) / the trigger-DDL family, which is a
/// separate campaign — they stay loud, 1:1-named seam-and-panic until it lands.
pub fn init_seams() {
    use backend_commands_trigger_seams as s;

    s::called_as_trigger::set(called_as_trigger_impl);
    s::tg_event::set(tg_event_impl);
    s::tg_trigger::set(tg_trigger_impl);
    s::tg_trigslot::set(tg_trigslot_impl);
    s::tg_newslot::set(tg_newslot_impl);

    s::tg_relation_oid::set(tg_relation_oid_impl);
    s::tg_relation_name::set(tg_relation_name_impl);
    s::tg_relation_namespace::set(tg_relation_namespace_impl);
    s::tg_relation_owner::set(tg_relation_owner_impl);
    s::tg_relation_is_partitioned::set(tg_relation_is_partitioned_impl);
    s::tg_relation_att_name::set(tg_relation_att_name_impl);
    s::tg_relation_att_type::set(tg_relation_att_type_impl);
    s::tg_relation_att_collation::set(tg_relation_att_collation_impl);

    s::tg_trigtuple::set(tg_trigtuple_impl);
    s::tg_newtuple::set(tg_newtuple_impl);
    s::tg_slot_formed_tuple::set(crate::firing::tg_slot_formed_tuple_impl);
    s::tg_nargs::set(tg_nargs_impl);
    s::tg_argv::set(tg_argv_impl);

    s::trigger_constraint::set(trigger_constraint_impl);
    s::trigger_constrrelid::set(trigger_constrrelid_impl);
    s::trigger_constrindid::set(trigger_constrindid_impl);
    s::trigger_name::set(trigger_name_impl);

    // The slot-value + live-relation accessors are now satisfied by the
    // AFTER-trigger firing path (`after_trigger_execute` materializes the OLD/NEW
    // slot payloads onto the per-call side-channel), so they resolve to the
    // owner-side deform of the re-fetched tuple rather than loud-panic.
    s::slot_getattr::set(crate::firing::slot_getattr_impl);
    s::slot_attisnull::set(crate::firing::slot_attisnull_impl);
    s::slot_tid::set(crate::firing::slot_tid_impl);
    s::slot_is_current_xact_tuple::set(crate::firing::slot_is_current_xact_tuple_impl);
    s::pk_datum_image_eq::set(crate::firing::pk_datum_image_eq_impl);
    s::tg_relation::set(crate::firing::tg_relation_impl);
    s::tg_relation_tuple_satisfies_snapshot_self::set(
        crate::firing::tg_relation_tuple_satisfies_snapshot_self_impl,
    );
}
