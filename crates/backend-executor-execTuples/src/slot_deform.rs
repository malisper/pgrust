//! Family: slot deform — `slot_deform_heap_tuple` and the
//! `slot_getsomeattrs[_int]` / `slot_getmissingattrs` / `slot_getattr` /
//! `slot_getallattrs` deconstruction entry points (execTuples.c).
//!
//! Deforming detoasts and fills the slot's `tts_values`/`tts_isnull` arrays up
//! to a watermark, so these are fallible (`elog(ERROR)` on a too-short tuple or
//! detoast failure).

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::tuptable::{HeapTupleTableSlot, SlotData};

/// `slot_deform_heap_tuple(slot, tuple, &offp, natts)` (execTuples.c): the
/// incremental byte-deform engine — fill `tts_values`/`tts_isnull` for the
/// first `natts` attributes from the slot's physical heap tuple, resuming from
/// the saved `off`/`TTS_SLOW` state.
pub fn slot_deform_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut HeapTupleTableSlot<'mcx>,
    _natts: i32,
) -> PgResult<()> {
    todo!("execTuples.c slot_deform_heap_tuple")
}

/// `slot_getsomeattrs_int(slot, attnum)` (execTuples.c): the slow path of
/// `slot_getsomeattrs` — call the slot-ops `getsomeattrs` and pad any
/// remaining requested attributes with `slot_getmissingattrs`.
pub fn slot_getsomeattrs_int<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _attnum: i32,
) -> PgResult<()> {
    todo!("execTuples.c slot_getsomeattrs_int")
}

/// `slot_getsomeattrs(slot, attnum)` (tuptable.h inline): ensure the first
/// `attnum` attributes of the slot are deconstructed into
/// `tts_values`/`tts_isnull` (fast path checks `tts_nvalid`).
pub fn slot_getsomeattrs<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _attnum: i32,
) -> PgResult<()> {
    todo!("execTuples.c slot_getsomeattrs")
}

/// `slot_getallattrs(slot)` (tuptable.h inline): deconstruct all attributes of
/// the slot's descriptor.
pub fn slot_getallattrs<'mcx>(_mcx: Mcx<'mcx>, _slot: &mut SlotData<'mcx>) -> PgResult<()> {
    todo!("execTuples.c slot_getallattrs")
}

/// `slot_getmissingattrs(slot, startAttNum, lastAttNum)` (execTuples.c): fill
/// the `[startAttNum, lastAttNum)` range of `tts_values`/`tts_isnull` from the
/// descriptor's attribute missing-value defaults (or NULL).
pub fn slot_getmissingattrs<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _start_att_num: i32,
    _last_att_num: i32,
) -> PgResult<()> {
    todo!("execTuples.c slot_getmissingattrs")
}

/// `slot_getattr(slot, attnum, &isnull)` (tuptable.h inline): fetch a single
/// attribute as `(datum, isnull)`, deforming as needed; negative `attnum` is a
/// system attribute (dispatched to `getsysattr`).
pub fn slot_getattr<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut SlotData<'mcx>,
    _attnum: AttrNumber,
) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c slot_getattr")
}
