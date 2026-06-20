//! The `ExecJust*` specialized evalfuncs (`execExprInterp.c`): hand-tuned fast
//! paths `ExecReadyInterpretedExpr` installs for the common single-Var /
//! single-Const / hash-key expression shapes, plus their shared `*Impl`
//! helpers.
//!
//! Each `ExecJust*` matches the C `ExprStateEvalFunc` shape
//! (`Datum f(ExprState*, ExprContext*, bool *isnull)`); the owned model returns
//! `(Datum, bool)` and threads the econtext id + EState explicitly.
//!
//! Slot-value access. C reads `slot->tts_values[attnum]` /
//! `slot->tts_isnull[attnum]` and calls `slot_getattr` / `slot_getsomeattrs`,
//! all of which deform the slot's tuple. The slot payload (the `tts_values` /
//! `tts_isnull` arrays, the descriptor, `tts_nvalid`) is owned by the
//! `execTuples` unit, which is not ported yet; its `TupleTableSlot` in the
//! shared vocabulary is trimmed to the header bits. The faithful path is the
//! owner's `slot_getallattrs` seam, which fully deforms the slot and returns
//! its per-attribute `(value, isnull)` pairs — exactly the array C indexes —
//! and panics loudly until `execTuples` lands. `slot_getattr(slot, attnum)`
//! therefore maps to: deform via the seam, then take attribute `attnum - 1`.

use backend_executor_execTuples_seams::slot_getallattrs_by_id;
use backend_utils_fmgr_fmgr_seams::function_call_invoke_datum;
// The canonical unified value type (Datum-unification keystone) — what the
// interpreter eval entry points return, and what the keystone-owned
// const/init step-payload values carry.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_core::primitive::Oid;
use types_error::PgResult;

/// `DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo))` for a single hash-key
/// argument — the by-reference-capable hash call the `ExecJust*Hash*Var` fast
/// paths drive. The canonical `value` crosses the fmgr boundary via the
/// `function_call_invoke_datum` lane (by-value word OR by-reference referent
/// bytes), so a by-ref `text`/`name`/`varchar` hash key survives the gather
/// (the former `function_call1_coll` bare-word seam panicked on such a value).
/// The owned `FmgrInfo` carries only `fn_oid`; the seam re-resolves and
/// dispatches under `collation` (`fcinfo->fncollation`). Returns the hash word.
#[inline]
fn hash_one_datum<'mcx>(
    fn_oid: Oid,
    collation: Oid,
    value: &Datum<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<u32> {
    let mcx = estate.es_query_cxt;
    let args = [value.clone()];
    let (result, _isnull) = function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], None)?;
    Ok(result.as_u32())
}
use types_nodes::execexpr::{ExprEvalStepData, ExprState};
use types_nodes::execnodes::EcxtId;
use types_nodes::{EStateData, SlotId};
use types_tuple::backend_access_common_heaptuple::DeformedColumn;

use crate::dispatch::CheckOpSlotCompatibility;

/// Read the `(fn_oid, fncollation)` an `EEOP_HASHDATUM_*` step caches in its
/// `op->d.hashdatum` payload (`finfo->fn_oid` + `fcinfo_data->fncollation`),
/// for the `ExecJust*Hash*Var` fast paths. The collation is `fcinfo->fncollation`
/// (the compiler's `init_fcinfo` sets it from the hash expression's
/// `inputcollid`); a collation-aware hash function (e.g. `hashtext` under a
/// nondeterministic collation) reads it back.
#[inline]
fn hashdatum_fn_and_coll(step: &ExprEvalStepData, who: &str) -> (Oid, Oid) {
    match step {
        ExprEvalStepData::HashDatum { finfo, fcinfo_data, .. } => {
            let fn_oid = finfo
                .as_ref()
                .unwrap_or_else(|| panic!("{who}: hashop finfo not resolved"))
                .fn_oid;
            let collation = fcinfo_data
                .as_ref()
                .map(|f| f.fncollation)
                .unwrap_or(types_core::primitive::InvalidOid);
            (fn_oid, collation)
        }
        _ => panic!("{who}: step is not an EEOP_HASHDATUM_*"),
    }
}

/// `pg_rotate_left32(word, n)` (pg_bitutils.h) — rotate a 32-bit word left by
/// `n` bits. The hash fast paths seed/combine the running hash key with this.
#[inline]
fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    word.rotate_left(n)
}

/// Read the `(Datum, bool)` of the single hash-key Var at 0-based `attnum`
/// (C: `slot->tts_values[attnum]` / `slot->tts_isnull[attnum]`), then dispatch
/// the hash function. The owned `FmgrInfo` carries only `fn_oid` (the fmgr seam
/// returns no typed `PGFunction`), so the call re-resolves by OID through the
/// fmgr owner's `FunctionCall1Coll` leaf — the contract every hashing caller in
/// the tree uses (cf. nodeMemoize `MemoizeHash_hash`). C's hash fcinfo carries
/// `fncollation`; the trimmed `FunctionCallInfoBaseData` (and the `HashDatum`
/// payload) drop the collation (the compiler's `init_fcinfo` discards
/// `inputcollid`), so the inherited trim passes `InvalidOid` here. Returns the
/// `(value, isnull)` of the hash-key attribute (the caller decides the
/// strict/non-strict NULL behaviour) plus a closure-free split so each fast
/// path can shape its own NULL handling.
#[inline]
fn read_hash_var<'mcx>(
    slot: SlotId,
    attnum: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // fcinfo->args[0].value  = slot->tts_values[attnum];
    // fcinfo->args[0].isnull = slot->tts_isnull[attnum];
    //
    // attnum is 0-based here (var->d.var.attnum); slot_getattr is 1-based.
    slot_getattr(slot, attnum + 1, estate)
}

/// Convert one deformed column into the canonical `(Datum, bool)` the
/// interpreter returns. For a by-value attribute the column is the scalar word
/// itself (C: `tts_values[attnum]`); for a by-reference attribute it is the
/// column's on-disk bytes (C: a pointer `Datum` into the tuple). The canonical
/// value type carries both arms directly, so this is now a faithful clone of
/// the deformed column — no by-reference round-trip is lost.
#[inline]
fn deformed_column_to_datum<'mcx>(col: &DeformedColumn<'mcx>) -> (Datum<'mcx>, bool) {
    let (value, isnull) = col;
    (value.clone(), *isnull)
}

/// Read attribute `attnum` (1-based, as C `slot_getattr`) out of the pool slot
/// `slot` by fully deforming it through the `execTuples` owner's seam.
fn slot_getattr<'mcx>(
    slot: SlotId,
    attnum: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let cols = slot_getallattrs_by_id::call(estate, slot)?;
    let col = &cols[(attnum - 1) as usize];
    Ok(deformed_column_to_datum(col))
}

/// `ExecJustVarImpl(ExprState *state, TupleTableSlot *slot, bool *isnull)` —
/// shared body for the plain non-virtual single-Var fast paths.
pub fn ExecJustVarImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *op = &state->steps[1];
    // int attnum = op->d.var.attnum + 1;
    let steps = state.steps.as_ref().expect("ExecJustVarImpl: steps not ready");
    let attnum = match &steps[1].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum + 1,
        _ => unreachable!("ExecJustVarImpl: step[1] is not an EEOP_*_VAR"),
    };

    // CheckOpSlotCompatibility(&state->steps[0], slot);
    CheckOpSlotCompatibility(&steps[0], estate.slot(slot))?;

    // Since we use slot_getattr(), we don't need to implement the FETCHSOME
    // step explicitly, and we also needn't Assert that the attnum is in range
    // --- slot_getattr() will take care of any problems.
    // return slot_getattr(slot, attnum, isnull);
    slot_getattr(slot, attnum, estate)
}

/// `ExecJustAssignVarImpl(ExprState *state, TupleTableSlot *inslot, bool *isnull)`
/// — shared body for the single-Var-assigned-to-resultslot fast paths.
pub fn ExecJustAssignVarImpl<'mcx>(
    state: &mut ExprState<'mcx>,
    inslot_id: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *op = &state->steps[1];
    // int attnum = op->d.assign_var.attnum + 1;
    // int resultnum = op->d.assign_var.resultnum;
    // TupleTableSlot *outslot = state->resultslot;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustAssignVarImpl: steps not ready");
    let (attnum, resultnum) = match &steps[1].d {
        ExprEvalStepData::AssignVar { attnum, resultnum } => (*attnum + 1, *resultnum),
        _ => unreachable!("ExecJustAssignVarImpl: step[1] is not an EEOP_ASSIGN_*_VAR"),
    };

    // CheckOpSlotCompatibility(&state->steps[0], inslot);
    let inslot = estate.slot(inslot_id);
    CheckOpSlotCompatibility(&steps[0], inslot)?;

    // We do not need CheckVarSlotCompatibility here; that was taken care of at
    // compilation time.
    //
    // Assert(resultnum >= 0 && resultnum < outslot->tts_tupleDescriptor->natts);
    // outslot->tts_values[resultnum] =
    //     slot_getattr(inslot, attnum, &outslot->tts_isnull[resultnum]);
    // return 0;
    //
    // The source attribute is read through the execTuples deform seam
    // (slot_getattr, 1-based) and written into state->resultslot's per-attribute
    // value/null arrays, which the canonical TupleTableSlot now carries.
    let (value, isnull) = slot_getattr(inslot_id, attnum, estate)?;
    write_resultslot(state, estate, resultnum, value, isnull);
    Ok((Datum::null(), false))
}

/// Write `(value, isnull)` into `state->resultslot->tts_values[resultnum]` /
/// `tts_isnull[resultnum]` (the C `outslot->tts_values/tts_isnull[resultnum]`
/// assignment shared by the `ExecJustAssignVar*` paths).
fn write_resultslot<'mcx>(
    state: &ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    resultnum: i32,
    value: Datum<'mcx>,
    isnull: bool,
) {
    // C: outslot = state->resultslot (a `TupleTableSlot *`); in the owned model
    // a pool SlotId resolved against the EState tuple table.
    let slot_id = state
        .resultslot
        .expect("ExecJustAssignVar*: ExprState has no resultslot");
    let outslot = estate.slot_mut(slot_id);
    let idx = resultnum as usize;
    debug_assert!(
        idx < outslot.tts_values.len(),
        "ExecJustAssignVar*: resultnum {resultnum} out of range"
    );
    outslot.tts_values[idx] = value;
    outslot.tts_isnull[idx] = isnull;
}

/// `ExecJustVarVirtImpl` — shared body for the virtual-slot single-Var paths.
pub fn ExecJustVarVirtImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *op = &state->steps[0];
    // int attnum = op->d.var.attnum;
    let steps = state.steps.as_ref().expect("ExecJustVarVirtImpl: steps not ready");
    let attnum = match &steps[0].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("ExecJustVarVirtImpl: step[0] is not an EEOP_*_VAR"),
    };

    // As it is guaranteed that a virtual slot is used, there never is a need to
    // perform tuple deforming. Verify, as much as possible, that the
    // determination was accurate.
    // Assert(TTS_IS_VIRTUAL(slot));
    // Assert(TTS_FIXED(slot));
    // Assert(attnum >= 0 && attnum < slot->tts_nvalid);
    debug_assert!(estate.slot(slot).is_fixed());

    // *isnull = slot->tts_isnull[attnum];
    // return slot->tts_values[attnum];
    //
    // A virtual slot's values are still owned by the execTuples slot model; the
    // trimmed slot exposes no value arrays, so the read goes through the same
    // deform seam (which a virtual slot satisfies directly). attnum here is
    // 0-based, so request attnum + 1.
    slot_getattr(slot, attnum + 1, estate)
}

/// `ExecJustAssignVarVirtImpl` — shared body for the virtual-slot assign paths.
pub fn ExecJustAssignVarVirtImpl<'mcx>(
    state: &mut ExprState<'mcx>,
    inslot_id: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *op = &state->steps[0];
    // int attnum = op->d.assign_var.attnum;
    // int resultnum = op->d.assign_var.resultnum;
    // TupleTableSlot *outslot = state->resultslot;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustAssignVarVirtImpl: steps not ready");
    let (attnum, resultnum) = match &steps[0].d {
        ExprEvalStepData::AssignVar { attnum, resultnum } => (*attnum, *resultnum),
        _ => unreachable!("ExecJustAssignVarVirtImpl: step[0] is not an EEOP_ASSIGN_*_VAR"),
    };

    // outslot->tts_values[resultnum] = inslot->tts_values[attnum];
    // outslot->tts_isnull[resultnum] = inslot->tts_isnull[attnum];
    // return 0;
    //
    // The source column is read off the virtual input slot through the deform
    // seam (attnum is 0-based here, slot_getattr is 1-based) and written into
    // state->resultslot's per-attribute value/null arrays.
    let (value, isnull) = slot_getattr(inslot_id, attnum + 1, estate)?;
    write_resultslot(state, estate, resultnum, value, isnull);
    Ok((Datum::null(), false))
}

/// `ExecJustHashVarImpl` — shared body for the single-Var hash-key paths.
pub fn ExecJustHashVarImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *fetchop = &state->steps[0];
    // ExprEvalStep *var = &state->steps[1];
    // ExprEvalStep *hashop = &state->steps[2];
    // FunctionCallInfo fcinfo = hashop->d.hashdatum.fcinfo_data;
    // int attnum = var->d.var.attnum;
    let steps = state.steps.as_ref().expect("ExecJustHashVarImpl: steps not ready");
    let attnum = match &steps[1].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("ExecJustHashVarImpl: step[1] is not an EEOP_*_VAR"),
    };
    let (fn_oid, collation) = hashdatum_fn_and_coll(&steps[2].d, "ExecJustHashVarImpl");

    // CheckOpSlotCompatibility(fetchop, slot);
    CheckOpSlotCompatibility(&steps[0], estate.slot(slot))?;

    // slot_getsomeattrs(slot, fetchop->d.fetch.last_var);
    // fcinfo->args[0].value  = slot->tts_values[attnum];
    // fcinfo->args[0].isnull = slot->tts_isnull[attnum];
    //
    // The FETCHSOME deform is subsumed by the slot-deform seam (read_hash_var ->
    // slot_getattr -> slot_getallattrs), exactly as the *Var fast paths fold
    // FETCHSOME into slot_getattr.
    let (value, isnull) = read_hash_var(slot, attnum, estate)?;

    // *isnull = false;
    // if (!fcinfo->args[0].isnull)
    //     return DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo));
    // else
    //     return (Datum) 0;
    //
    // The owned FmgrInfo carries only fn_oid; the fmgr seam re-resolves and
    // dispatches (the typed PGFunction is never produced — see the F0 contract).
    if !isnull {
        let hashvalue = hash_one_datum(fn_oid, collation, &value, estate)?;
        // DatumGetUInt32 then UInt32GetDatum is identity on the word.
        Ok((Datum::from_u32(hashvalue), false))
    } else {
        Ok((Datum::null(), false))
    }
}

/// `ExecJustHashVarVirtImpl` — shared body for the virtual-slot hash-key paths.
pub fn ExecJustHashVarVirtImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *var = &state->steps[0];
    // ExprEvalStep *hashop = &state->steps[1];
    // FunctionCallInfo fcinfo = hashop->d.hashdatum.fcinfo_data;
    // int attnum = var->d.var.attnum;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustHashVarVirtImpl: steps not ready");
    let attnum = match &steps[0].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("ExecJustHashVarVirtImpl: step[0] is not an EEOP_*_VAR"),
    };
    let (fn_oid, collation) = hashdatum_fn_and_coll(&steps[1].d, "ExecJustHashVarVirtImpl");

    // fcinfo->args[0].value  = slot->tts_values[attnum];
    // fcinfo->args[0].isnull = slot->tts_isnull[attnum];
    //
    // Virtual slot: no FETCHSOME, the deform seam satisfies a virtual slot
    // directly (see ExecJustVarVirtImpl).
    let (value, isnull) = read_hash_var(slot, attnum, estate)?;

    // *isnull = false;
    // if (!fcinfo->args[0].isnull)
    //     return DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo));
    // else return (Datum) 0;
    if !isnull {
        let hashvalue = hash_one_datum(fn_oid, collation, &value, estate)?;
        Ok((Datum::from_u32(hashvalue), false))
    } else {
        Ok((Datum::null(), false))
    }
}

/// Resolve the inner/outer/scan slot id linked from the econtext, then the live
/// slot. `None` (C `NULL`) is unreachable on these fast paths (the compiler
/// only installs them when the corresponding slot is present), so a missing
/// link is a caller bug.
macro_rules! resolve_slot {
    ($estate:ident, $econtext:ident, $field:ident) => {{
        let id = $estate.es_exprcontexts[$econtext.0 as usize]
            .as_ref()
            .expect("ExecJust*: econtext freed")
            .$field
            .expect(concat!("ExecJust*: econtext->", stringify!($field), " is NULL"));
        id
    }};
}

/// `ExecJustInnerVar` — read one Var from the inner slot.
pub fn ExecJustInnerVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustVarImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    ExecJustVarImpl(state, slot_id, estate)
}

/// `ExecJustOuterVar` — read one Var from the outer slot.
pub fn ExecJustOuterVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustVarImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    ExecJustVarImpl(state, slot_id, estate)
}

/// `ExecJustScanVar` — read one Var from the scan slot.
pub fn ExecJustScanVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustVarImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    ExecJustVarImpl(state, slot_id, estate)
}

/// `ExecJustAssignInnerVar` — assign one inner Var to the result slot.
pub fn ExecJustAssignInnerVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustAssignVarImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    ExecJustAssignVarImpl(state, slot_id, estate)
}

/// `ExecJustAssignOuterVar` — assign one outer Var to the result slot.
pub fn ExecJustAssignOuterVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustAssignVarImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    ExecJustAssignVarImpl(state, slot_id, estate)
}

/// `ExecJustAssignScanVar` — assign one scan Var to the result slot.
pub fn ExecJustAssignScanVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustAssignVarImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    ExecJustAssignVarImpl(state, slot_id, estate)
}

/// `ExecJustApplyFuncToCase` — single strict function over a CaseTest input.
pub fn ExecJustApplyFuncToCase<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let _ = econtext;
    // ExprEvalStep *op = &state->steps[0];
    //
    // The CASE_TESTVAL step copies the innermost CASE test cell into its own
    // result cell, which the compiler made identical to the strict function's
    // first argument cell (arg_cells[0]). In the owned model the cells live in
    // state.result_cells; read the casetest source, write the CASE_TESTVAL
    // result cell.
    //
    // *op->resvalue = *op->d.casetest.value;
    // *op->resnull  = *op->d.casetest.isnull;
    let (casetest_src, casetest_dst) = {
        let steps = state
            .steps
            .as_ref()
            .expect("ExecJustApplyFuncToCase: steps not ready");
        let src = match &steps[0].d {
            ExprEvalStepData::CaseTest { value } => *value,
            _ => unreachable!("ExecJustApplyFuncToCase: step[0] is not an EEOP_CASE_TESTVAL"),
        };
        (src, steps[0].resvalue)
    };
    let src_cell = state.result_cells.get(casetest_src);
    state.result_cells.set(casetest_dst, src_cell);

    // op++;
    // nargs = op->d.func.nargs;
    // fcinfo = op->d.func.fcinfo_data;
    // args = fcinfo->args;
    //
    // The function's argument cells (the C `&fcinfo->args[i]`) are the Func
    // step's `arg_cells` in the owned model.
    // nargs = op->d.func.nargs;
    // fcinfo = op->d.func.fcinfo_data;
    // args = fcinfo->args;
    //
    // Gather the function's `(fn_oid, fncollation)`, its per-argument result
    // cells (the C `&fcinfo->args[i]`), and the call-expression node stamped on
    // the resolved `FmgrInfo` (for polymorphic callees), exactly as
    // `eval_scalar::func_step_inputs` does for the general FUNCEXPR opcodes. The
    // per-arg value carries the CANONICAL `Datum<'mcx>` straight from the cell
    // (a by-reference text/name argument survives the gather — WALL 1aq).
    let (nargs, arg_cells, fn_oid, collation, fn_expr) = {
        let steps = state
            .steps
            .as_ref()
            .expect("ExecJustApplyFuncToCase: steps not ready");
        match &steps[1].d {
            ExprEvalStepData::Func {
                nargs,
                arg_cells,
                finfo,
                fcinfo_data,
                ..
            } => {
                let cells: Vec<_> = arg_cells
                    .as_ref()
                    .map(|c| c.iter().copied().collect())
                    .unwrap_or_default();
                let finfo = finfo
                    .as_ref()
                    .expect("ExecJustApplyFuncToCase: func finfo not resolved");
                let oid = finfo.fn_oid;
                let collation = fcinfo_data
                    .as_ref()
                    .expect("ExecJustApplyFuncToCase: func fcinfo_data missing")
                    .fncollation;
                // Pass the call node as the cheap `Rc`-backed erased handle (a
                // `clone()` of the step's stamped `fn_expr`), NOT a borrow the
                // dispatch would deep-clone per call.
                let fn_expr = finfo.fn_expr.clone();
                (*nargs, cells, oid, collation, fn_expr)
            }
            _ => unreachable!("ExecJustApplyFuncToCase: step[1] is not an EEOP_FUNCEXPR_STRICT*"),
        }
    };

    // strict function, so check for NULL args
    // for (int argno = 0; argno < nargs; argno++)
    //     if (args[argno].isnull) { *isnull = true; return (Datum) 0; }
    //
    // and build the canonical arg frame for the run case.
    let mut args: Vec<Datum<'mcx>> = Vec::with_capacity(nargs as usize);
    for argno in 0..(nargs as usize) {
        let cell = state.result_cells.get(arg_cells[argno]);
        if cell.isnull {
            return Ok((Datum::null(), true));
        }
        args.push(cell.value.clone());
    }

    // fcinfo->isnull = false;
    // d = op->d.func.fn_addr(fcinfo);
    // *isnull = fcinfo->isnull;
    // return d;
    //
    // The general arbitrary-`nargs` fmgr dispatch goes through the
    // `function_call_invoke_datum` seam (the by-reference-capable canonical lane):
    // the owned `FmgrInfo` carries only `fn_oid`, so the owner re-resolves by OID
    // and runs the function under `fncollation` on the built `args` frame,
    // returning the result `Datum` and the callee's `fcinfo->isnull`.
    let mcx = estate.es_query_cxt;
    let (value, isnull) =
        function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], fn_expr)?;
    Ok((value, isnull))
}

/// `ExecJustConst` — return a single Const value.
pub fn ExecJustConst<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let _ = (econtext, estate);
    // ExprEvalStep *op = &state->steps[0];
    // *isnull = op->d.constval.isnull;
    // return op->d.constval.value;
    let steps = state.steps.as_ref().expect("ExecJustConst: steps not ready");
    match &steps[0].d {
        // The canonical const value is returned directly (by-value scalar word
        // or by-reference image) through the now-canonical eval entry point.
        ExprEvalStepData::ConstVal { value, isnull } => Ok((value.clone(), *isnull)),
        _ => unreachable!("ExecJustConst: step[0] is not an EEOP_CONST"),
    }
}

/// `ExecJustInnerVarVirt` — read one Var from a virtual inner slot.
pub fn ExecJustInnerVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustVarVirtImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    ExecJustVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustOuterVarVirt` — read one Var from a virtual outer slot.
pub fn ExecJustOuterVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustVarVirtImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    ExecJustVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustScanVarVirt` — read one Var from a virtual scan slot.
pub fn ExecJustScanVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustVarVirtImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    ExecJustVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustAssignInnerVarVirt` — assign one virtual inner Var.
pub fn ExecJustAssignInnerVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustAssignVarVirtImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    ExecJustAssignVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustAssignOuterVarVirt` — assign one virtual outer Var.
pub fn ExecJustAssignOuterVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustAssignVarVirtImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    ExecJustAssignVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustAssignScanVarVirt` — assign one virtual scan Var.
pub fn ExecJustAssignScanVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustAssignVarVirtImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    ExecJustAssignVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustHashInnerVarWithIV` — hash one inner Var, seeded with an init value.
pub fn ExecJustHashInnerVarWithIV<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *fetchop = &state->steps[0];
    // ExprEvalStep *setivop = &state->steps[1];
    // ExprEvalStep *innervar = &state->steps[2];
    // ExprEvalStep *hashop  = &state->steps[3];
    // FunctionCallInfo fcinfo = hashop->d.hashdatum.fcinfo_data;
    // int attnum = innervar->d.var.attnum;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustHashInnerVarWithIV: steps not ready");
    // hashkey = DatumGetUInt32(setivop->d.hashdatum_initvalue.init_value);
    // The init value is a by-value uint32 seed read directly off the canonical
    // const payload via the `DatumGetUInt32` accessor — no shim word needed.
    let init_hashkey = match &steps[1].d {
        ExprEvalStepData::HashDatumInitValue { init_value } => init_value.as_u32(),
        _ => unreachable!("ExecJustHashInnerVarWithIV: step[1] is not EEOP_HASHDATUM_SET_INITVAL"),
    };
    let attnum = match &steps[2].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("ExecJustHashInnerVarWithIV: step[2] is not an EEOP_*_VAR"),
    };
    let (fn_oid, collation) = hashdatum_fn_and_coll(&steps[3].d, "ExecJustHashInnerVarWithIV");

    // CheckOpSlotCompatibility(fetchop, econtext->ecxt_innertuple);
    // slot_getsomeattrs(econtext->ecxt_innertuple, fetchop->d.fetch.last_var);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    CheckOpSlotCompatibility(&steps[0], estate.slot(slot_id))?;

    // fcinfo->args[0].value  = econtext->ecxt_innertuple->tts_values[attnum];
    // fcinfo->args[0].isnull = econtext->ecxt_innertuple->tts_isnull[attnum];
    let (value, isnull) = read_hash_var(slot_id, attnum, estate)?;

    // hashkey = pg_rotate_left32(hashkey, 1);
    let mut hashkey = init_hashkey;
    hashkey = pg_rotate_left32(hashkey, 1);

    // if (!fcinfo->args[0].isnull) {
    //     hashvalue = DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo));
    //     hashkey = hashkey ^ hashvalue;
    // }
    if !isnull {
        let hashvalue = hash_one_datum(fn_oid, collation, &value, estate)?;
        hashkey ^= hashvalue;
    }

    // *isnull = false;
    // return UInt32GetDatum(hashkey);
    Ok((Datum::from_u32(hashkey), false))
}

/// `ExecJustHashOuterVar` — hash one outer Var.
pub fn ExecJustHashOuterVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustHashVarImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    ExecJustHashVarImpl(state, slot_id, estate)
}

/// `ExecJustHashInnerVar` — hash one inner Var.
pub fn ExecJustHashInnerVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustHashVarImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    ExecJustHashVarImpl(state, slot_id, estate)
}

/// `ExecJustHashOuterVarVirt` — hash one virtual outer Var.
pub fn ExecJustHashOuterVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustHashVarVirtImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    ExecJustHashVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustHashInnerVarVirt` — hash one virtual inner Var.
pub fn ExecJustHashInnerVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // return ExecJustHashVarVirtImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    ExecJustHashVarVirtImpl(state, slot_id, estate)
}

/// `ExecJustHashOuterVarStrict` — hash one outer Var, strict (null short-circuit).
pub fn ExecJustHashOuterVarStrict<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // ExprEvalStep *fetchop = &state->steps[0];
    // ExprEvalStep *var = &state->steps[1];
    // ExprEvalStep *hashop = &state->steps[2];
    // FunctionCallInfo fcinfo = hashop->d.hashdatum.fcinfo_data;
    // int attnum = var->d.var.attnum;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustHashOuterVarStrict: steps not ready");
    let attnum = match &steps[1].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("ExecJustHashOuterVarStrict: step[1] is not an EEOP_*_VAR"),
    };
    let (fn_oid, collation) = hashdatum_fn_and_coll(&steps[2].d, "ExecJustHashOuterVarStrict");

    // CheckOpSlotCompatibility(fetchop, econtext->ecxt_outertuple);
    // slot_getsomeattrs(econtext->ecxt_outertuple, fetchop->d.fetch.last_var);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    CheckOpSlotCompatibility(&steps[0], estate.slot(slot_id))?;

    // fcinfo->args[0].value  = econtext->ecxt_outertuple->tts_values[attnum];
    // fcinfo->args[0].isnull = econtext->ecxt_outertuple->tts_isnull[attnum];
    let (value, isnull) = read_hash_var(slot_id, attnum, estate)?;

    // if (!fcinfo->args[0].isnull) {
    //     *isnull = false;
    //     return DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo));
    // } else {
    //     *isnull = true;            /* return NULL on NULL input */
    //     return (Datum) 0;
    // }
    if !isnull {
        let hashvalue = hash_one_datum(fn_oid, collation, &value, estate)?;
        Ok((Datum::from_u32(hashvalue), false))
    } else {
        Ok((Datum::null(), true))
    }
}

