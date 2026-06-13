//! Family: tuple-descriptor constructors + tuple output — `ExecTypeFromTL` /
//! `ExecCleanTypeFromTL` / `ExecTypeFromExprList` / `ExecTypeSetColNames` /
//! `BlessTupleDesc` / `TupleDescGetAttInMetadata` / `BuildTupleFromCStrings` /
//! `HeapTupleHeaderGetDatum` and the `begin/do/end_tup_output` convenience
//! routines (execTuples.c).
//!
//! `CreateTemplateTupleDesc`/`TupleDescInitEntry` are a cycle-free direct dep
//! on `backend-access-common-tupdesc`; the per-expression `exprType` /
//! `exprTypmod` / `exprCollation` metadata (nodeFuncs.c) and the type input
//! function lookup (fmgr / typcache) are reached through their owners' seams in
//! follow-up passes.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::primnodes::TargetEntry;
use types_nodes::tuptable::{AttInMetadata, SlotData, TupOutputState};
use types_datum::Datum;
use types_tuple::heaptuple::{HeapTuple, TupleDesc, TupleDescData};

/// `ExecTypeFromTL(targetList)` (execTuples.c): build a tuple descriptor from a
/// target list (including resjunk entries).
pub fn ExecTypeFromTL<'mcx>(
    _mcx: Mcx<'mcx>,
    _target_list: &[TargetEntry<'mcx>],
) -> PgResult<TupleDesc<'mcx>> {
    todo!("execTuples.c ExecTypeFromTL")
}

/// `ExecCleanTypeFromTL(targetList)` (execTuples.c): like `ExecTypeFromTL` but
/// omitting resjunk columns.
pub fn ExecCleanTypeFromTL<'mcx>(
    _mcx: Mcx<'mcx>,
    _target_list: &[TargetEntry<'mcx>],
) -> PgResult<TupleDesc<'mcx>> {
    todo!("execTuples.c ExecCleanTypeFromTL")
}

/// `ExecTypeFromExprList(exprList)` (execTuples.c): build a tuple descriptor
/// from a bare list of expressions (no names).
pub fn ExecTypeFromExprList<'mcx>(
    _mcx: Mcx<'mcx>,
    _expr_list: &[Node<'mcx>],
) -> PgResult<TupleDesc<'mcx>> {
    todo!("execTuples.c ExecTypeFromExprList")
}

/// `ExecTypeSetColNames(typeInfo, namesList)` (execTuples.c): apply column
/// names to an already-built descriptor.
pub fn ExecTypeSetColNames<'mcx>(
    _type_info: &mut TupleDescData<'mcx>,
    _names_list: &[&str],
) -> PgResult<()> {
    todo!("execTuples.c ExecTypeSetColNames")
}

/// `BlessTupleDesc(tupdesc)` (execTuples.c): register a transient record type
/// for an anonymous descriptor and return it.
pub fn BlessTupleDesc<'mcx>(_mcx: Mcx<'mcx>, _tupdesc: TupleDesc<'mcx>) -> PgResult<TupleDesc<'mcx>> {
    todo!("execTuples.c BlessTupleDesc")
}

/// `TupleDescGetAttInMetadata(tupdesc)` (execTuples.c): build the per-attribute
/// input-function metadata for `BuildTupleFromCStrings`.
pub fn TupleDescGetAttInMetadata<'mcx>(
    _mcx: Mcx<'mcx>,
    _tupdesc: TupleDesc<'mcx>,
) -> PgResult<AttInMetadata<'mcx>> {
    todo!("execTuples.c TupleDescGetAttInMetadata")
}

/// `BuildTupleFromCStrings(attinmeta, values)` (execTuples.c): build a heap
/// tuple from an array of C-string column values via the type input functions.
pub fn BuildTupleFromCStrings<'mcx>(
    _mcx: Mcx<'mcx>,
    _attinmeta: &AttInMetadata<'mcx>,
    _values: &[Option<&str>],
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c BuildTupleFromCStrings")
}

/// `HeapTupleHeaderGetDatum(tuple)` (execTuples.c): wrap a heap tuple's header
/// as a composite `Datum` (blessing the record type if needed).
pub fn HeapTupleHeaderGetDatum<'mcx>(
    _mcx: Mcx<'mcx>,
    _tuple: HeapTuple<'mcx>,
) -> PgResult<(HeapTuple<'mcx>, Datum)> {
    todo!("execTuples.c HeapTupleHeaderGetDatum")
}

/// `begin_tup_output_tupdesc(dest, tupdesc, tts_ops)` (execTuples.c): set up a
/// `TupOutputState` for sending rows of `tupdesc` to `dest`.
pub fn begin_tup_output_tupdesc<'mcx>(
    _mcx: Mcx<'mcx>,
    _dest: types_nodes::parsestmt::DestReceiverHandle,
    _tupdesc: TupleDesc<'mcx>,
    _tts_ops: types_nodes::TupleSlotKind,
) -> PgResult<TupOutputState<'mcx>> {
    todo!("execTuples.c begin_tup_output_tupdesc")
}

/// `do_tup_output(tstate, values, isnull)` (execTuples.c): store one row into
/// the output slot and send it to the receiver.
pub fn do_tup_output<'mcx>(
    _mcx: Mcx<'mcx>,
    _tstate: &mut TupOutputState<'mcx>,
    _values: &[Datum],
    _isnull: &[bool],
) -> PgResult<()> {
    todo!("execTuples.c do_tup_output")
}

/// `do_text_output_multiline(tstate, txt)` (execTuples.c): emit `txt` as one
/// single-text-column row per line.
pub fn do_text_output_multiline<'mcx>(
    _mcx: Mcx<'mcx>,
    _tstate: &mut TupOutputState<'mcx>,
    _txt: &str,
) -> PgResult<()> {
    todo!("execTuples.c do_text_output_multiline")
}

/// `end_tup_output(tstate)` (execTuples.c): shut down the receiver and drop the
/// output slot.
pub fn end_tup_output<'mcx>(_tstate: TupOutputState<'mcx>) -> PgResult<()> {
    todo!("execTuples.c end_tup_output")
}

/// `&Slot` use marker — keeps the live-slot type referenced from the output
/// family's documented surface (the output slot is a [`SlotData`]).
#[allow(dead_code)]
fn _output_slot_marker(_slot: &SlotData) {}
