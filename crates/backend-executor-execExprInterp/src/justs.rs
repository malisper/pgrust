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

use backend_executor_execTuples_seams::slot_getallattrs;
use backend_utils_fmgr_fmgr_seams::function_call1_coll;
use types_core::primitive::InvalidOid;
// The bare-word newtype: the eval entry-point return word.
use types_datum::Datum;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned const/init step-payload values carry.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::PgResult;

/// Recover the bare scalar word from a stored canonical by-value datum.
#[inline]
fn word_of(v: &DatumV<'_>) -> Datum {
    Datum::from_usize(v.as_usize())
}
use types_nodes::execexpr::{ExprEvalStepData, ExprState};
use types_nodes::execnodes::EcxtId;
use types_nodes::executor::TupleTableSlot;
use types_nodes::EStateData;
use types_tuple::backend_access_common_heaptuple::{DeformedColumn, TupleValue};

use crate::dispatch::CheckOpSlotCompatibility;

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
    slot: &TupleTableSlot,
    attnum: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // fcinfo->args[0].value  = slot->tts_values[attnum];
    // fcinfo->args[0].isnull = slot->tts_isnull[attnum];
    //
    // attnum is 0-based here (var->d.var.attnum); slot_getattr is 1-based.
    slot_getattr(slot, attnum + 1, estate)
}

/// Convert one deformed column into the `(Datum, bool)` the interpreter
/// returns. For a by-value attribute the column is the scalar word itself
/// (C: `tts_values[attnum]`). A by-reference attribute is a pointer `Datum`
/// in C; the owned model carries the bytes instead and the trimmed
/// `TupleTableSlot` cannot yet round-trip that to a bare `Datum`. The
/// `slot_getallattrs` seam panics before any column is produced (its owner is
/// unported), so this conversion is never reached at runtime today; it is kept
/// faithful for when the slot-payload model lands.
#[inline]
fn deformed_column_to_datum(col: &DeformedColumn<'_>) -> (Datum, bool) {
    let (value, isnull) = col;
    let datum = match value {
        TupleValue::ByVal(d) => *d,
        TupleValue::ByRef(_) => {
            // A by-reference Var fast path needs the slot's pointer Datum,
            // which the trimmed slot model (execTuples-owned) does not yet
            // expose. Unreachable until the slot payload model lands, because
            // slot_getallattrs panics first.
            panic!(
                "ExecJust* by-reference slot value not representable until the \
                 execTuples slot payload model lands"
            )
        }
    };
    (datum, *isnull)
}

/// Read attribute `attnum` (1-based, as C `slot_getattr`) out of `slot` by
/// fully deforming it through the `execTuples` owner's seam.
fn slot_getattr<'mcx>(
    slot: &TupleTableSlot,
    attnum: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    let cols = slot_getallattrs::call(estate.es_query_cxt, slot)?;
    let col = &cols[(attnum - 1) as usize];
    Ok(deformed_column_to_datum(col))
}

/// `ExecJustVarImpl(ExprState *state, TupleTableSlot *slot, bool *isnull)` —
/// shared body for the plain non-virtual single-Var fast paths.
pub fn ExecJustVarImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: &TupleTableSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // ExprEvalStep *op = &state->steps[1];
    // int attnum = op->d.var.attnum + 1;
    let steps = state.steps.as_ref().expect("ExecJustVarImpl: steps not ready");
    let attnum = match &steps[1].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum + 1,
        _ => unreachable!("ExecJustVarImpl: step[1] is not an EEOP_*_VAR"),
    };

    // CheckOpSlotCompatibility(&state->steps[0], slot);
    CheckOpSlotCompatibility(&steps[0], slot)?;

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
    inslot: &TupleTableSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // ExprEvalStep *op = &state->steps[1];
    // int attnum = op->d.assign_var.attnum + 1;
    // int resultnum = op->d.assign_var.resultnum;
    // TupleTableSlot *outslot = state->resultslot;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustAssignVarImpl: steps not ready");
    let (_attnum, _resultnum) = match &steps[1].d {
        ExprEvalStepData::AssignVar { attnum, resultnum } => (*attnum + 1, *resultnum),
        _ => unreachable!("ExecJustAssignVarImpl: step[1] is not an EEOP_ASSIGN_*_VAR"),
    };

    // CheckOpSlotCompatibility(&state->steps[0], inslot);
    CheckOpSlotCompatibility(&steps[0], inslot)?;

    // We do not need CheckVarSlotCompatibility here; that was taken care of at
    // compilation time.
    //
    // Assert(resultnum >= 0 && resultnum < outslot->tts_tupleDescriptor->natts);
    // outslot->tts_values[resultnum] =
    //     slot_getattr(inslot, attnum, &outslot->tts_isnull[resultnum]);
    // return 0;
    //
    // The step payload (d.assign_var.attnum/resultnum) is now modeled (read
    // above). What is still missing is the destination: the write into
    // state->resultslot->tts_values[resultnum] / tts_isnull[resultnum]. The
    // shared TupleTableSlot is trimmed to its header bits (no tts_values /
    // tts_isnull arrays); those are owned by the execTuples unit, which is not
    // ported yet. slot_getattr() on the source likewise routes through the
    // execTuples slot_getallattrs seam, which panics first. Faithful as soon as
    // execTuples lands the slot value-array model.
    let _ = (inslot, estate);
    panic!(
        "ExecJustAssignVarImpl: writing into state->resultslot->tts_values/\
         tts_isnull[resultnum] needs the execTuples slot payload model (the \
         trimmed TupleTableSlot has no value arrays); blocked until execTuples \
         lands"
    )
}

/// `ExecJustVarVirtImpl` — shared body for the virtual-slot single-Var paths.
pub fn ExecJustVarVirtImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: &TupleTableSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
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
    debug_assert!(slot.is_fixed());

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
    inslot: &TupleTableSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // ExprEvalStep *op = &state->steps[0];
    // int attnum = op->d.assign_var.attnum;
    // int resultnum = op->d.assign_var.resultnum;
    // TupleTableSlot *outslot = state->resultslot;
    let steps = state
        .steps
        .as_ref()
        .expect("ExecJustAssignVarVirtImpl: steps not ready");
    let (_attnum, _resultnum) = match &steps[0].d {
        ExprEvalStepData::AssignVar { attnum, resultnum } => (*attnum, *resultnum),
        _ => unreachable!("ExecJustAssignVarVirtImpl: step[0] is not an EEOP_ASSIGN_*_VAR"),
    };

    // outslot->tts_values[resultnum] = inslot->tts_values[attnum];
    // outslot->tts_isnull[resultnum] = inslot->tts_isnull[attnum];
    // return 0;
    //
    // The step payload (d.assign_var.attnum/resultnum) is now modeled (read
    // above). The copy itself reads inslot->tts_values/tts_isnull[attnum] and
    // writes state->resultslot->tts_values/tts_isnull[resultnum]; both arrays
    // are execTuples-owned and absent from the trimmed shared TupleTableSlot.
    // Faithful as soon as execTuples lands the slot value-array model.
    let _ = (inslot, estate);
    panic!(
        "ExecJustAssignVarVirtImpl: the inslot->outslot value-array copy needs \
         the execTuples slot payload model (the trimmed TupleTableSlot has no \
         value arrays); blocked until execTuples lands"
    )
}

/// `ExecJustHashVarImpl` — shared body for the single-Var hash-key paths.
pub fn ExecJustHashVarImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: &TupleTableSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
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
    let fn_oid = match &steps[2].d {
        ExprEvalStepData::HashDatum { finfo, .. } => finfo
            .as_ref()
            .expect("ExecJustHashVarImpl: hashop finfo not resolved")
            .fn_oid,
        _ => unreachable!("ExecJustHashVarImpl: step[2] is not an EEOP_HASHDATUM_*"),
    };

    // CheckOpSlotCompatibility(fetchop, slot);
    CheckOpSlotCompatibility(&steps[0], slot)?;

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
        let result = function_call1_coll::call(fn_oid, InvalidOid, value)?;
        // DatumGetUInt32 then UInt32GetDatum is identity on the word.
        Ok((Datum::from_u32(result.as_u32()), false))
    } else {
        Ok((Datum::null(), false))
    }
}

/// `ExecJustHashVarVirtImpl` — shared body for the virtual-slot hash-key paths.
pub fn ExecJustHashVarVirtImpl<'mcx>(
    state: &ExprState<'mcx>,
    slot: &TupleTableSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
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
    let fn_oid = match &steps[1].d {
        ExprEvalStepData::HashDatum { finfo, .. } => finfo
            .as_ref()
            .expect("ExecJustHashVarVirtImpl: hashop finfo not resolved")
            .fn_oid,
        _ => unreachable!("ExecJustHashVarVirtImpl: step[1] is not an EEOP_HASHDATUM_*"),
    };

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
        let result = function_call1_coll::call(fn_oid, InvalidOid, value)?;
        Ok((Datum::from_u32(result.as_u32()), false))
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
) -> PgResult<(Datum, bool)> {
    // return ExecJustVarImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustVarImpl(state, &slot, estate)
}

/// `ExecJustOuterVar` — read one Var from the outer slot.
pub fn ExecJustOuterVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustVarImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustVarImpl(state, &slot, estate)
}

/// `ExecJustScanVar` — read one Var from the scan slot.
pub fn ExecJustScanVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustVarImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustVarImpl(state, &slot, estate)
}

/// `ExecJustAssignInnerVar` — assign one inner Var to the result slot.
pub fn ExecJustAssignInnerVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustAssignVarImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustAssignVarImpl(state, &slot, estate)
}

/// `ExecJustAssignOuterVar` — assign one outer Var to the result slot.
pub fn ExecJustAssignOuterVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustAssignVarImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustAssignVarImpl(state, &slot, estate)
}

/// `ExecJustAssignScanVar` — assign one scan Var to the result slot.
pub fn ExecJustAssignScanVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustAssignVarImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustAssignVarImpl(state, &slot, estate)
}

/// `ExecJustApplyFuncToCase` — single strict function over a CaseTest input.
pub fn ExecJustApplyFuncToCase<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
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
    let (nargs, arg_cells, fn_oid) = {
        let steps = state
            .steps
            .as_ref()
            .expect("ExecJustApplyFuncToCase: steps not ready");
        match &steps[1].d {
            ExprEvalStepData::Func {
                nargs,
                arg_cells,
                finfo,
                ..
            } => {
                let cells: Vec<_> = arg_cells
                    .as_ref()
                    .map(|c| c.iter().copied().collect())
                    .unwrap_or_default();
                let oid = finfo
                    .as_ref()
                    .expect("ExecJustApplyFuncToCase: func finfo not resolved")
                    .fn_oid;
                (*nargs, cells, oid)
            }
            _ => unreachable!("ExecJustApplyFuncToCase: step[1] is not an EEOP_FUNCEXPR_STRICT*"),
        }
    };

    // strict function, so check for NULL args
    // for (int argno = 0; argno < nargs; argno++)
    //     if (args[argno].isnull) { *isnull = true; return (Datum) 0; }
    for argno in 0..(nargs as usize) {
        if state.result_cells.get(arg_cells[argno]).isnull {
            return Ok((Datum::null(), true));
        }
    }

    // fcinfo->isnull = false;
    // d = op->d.func.fn_addr(fcinfo);
    // *isnull = fcinfo->isnull;
    // return d;
    //
    // The strict-NULL arg scan and the casetest data shuffle above are done in
    // the owned ResultCellArena. The function dispatch itself is a general
    // N-argument fmgr call (`fn_addr(fcinfo)` over `fcinfo->args[0..nargs]`).
    // The fmgr owner (backend-utils-fmgr-fmgr, #52) is merged, but its seam
    // surface only exposes arity-specific leaves (`FunctionCall1/2/3Coll`) — it
    // has no general "invoke this resolved frame" entry, and the trimmed
    // `FunctionCallInfoBaseData` carries no `args[]` to pass one. A faithful
    // dispatch for the arbitrary-nargs FUNCEXPR_STRICT shape therefore needs a
    // general FunctionCallInvoke seam from the fmgr owner; gathering the
    // arg-cells here and invoking it is the remaining step.
    let _ = (fn_oid, estate);
    panic!(
        "ExecJustApplyFuncToCase: the strict-NULL arg scan and casetest shuffle \
         are modeled; the arbitrary-nargs fn_addr(fcinfo) dispatch still needs a \
         general FunctionCallInvoke seam from the fmgr owner (its seam surface \
         exposes only FunctionCall1/2/3Coll, and the trimmed \
         FunctionCallInfoBaseData has no args[] to pass a built frame)"
    )
}

/// `ExecJustConst` — return a single Const value.
pub fn ExecJustConst<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    let _ = (econtext, estate);
    // ExprEvalStep *op = &state->steps[0];
    // *isnull = op->d.constval.isnull;
    // return op->d.constval.value;
    let steps = state.steps.as_ref().expect("ExecJustConst: steps not ready");
    match &steps[0].d {
        // The canonical const value crosses back to the bare word the eval
        // entry-point contract returns.
        ExprEvalStepData::ConstVal { value, isnull } => Ok((word_of(value), *isnull)),
        _ => unreachable!("ExecJustConst: step[0] is not an EEOP_CONST"),
    }
}

/// `ExecJustInnerVarVirt` — read one Var from a virtual inner slot.
pub fn ExecJustInnerVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustVarVirtImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustVarVirtImpl(state, &slot, estate)
}

/// `ExecJustOuterVarVirt` — read one Var from a virtual outer slot.
pub fn ExecJustOuterVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustVarVirtImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustVarVirtImpl(state, &slot, estate)
}

/// `ExecJustScanVarVirt` — read one Var from a virtual scan slot.
pub fn ExecJustScanVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustVarVirtImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustVarVirtImpl(state, &slot, estate)
}

/// `ExecJustAssignInnerVarVirt` — assign one virtual inner Var.
pub fn ExecJustAssignInnerVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustAssignVarVirtImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustAssignVarVirtImpl(state, &slot, estate)
}

/// `ExecJustAssignOuterVarVirt` — assign one virtual outer Var.
pub fn ExecJustAssignOuterVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustAssignVarVirtImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustAssignVarVirtImpl(state, &slot, estate)
}

/// `ExecJustAssignScanVarVirt` — assign one virtual scan Var.
pub fn ExecJustAssignScanVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustAssignVarVirtImpl(state, econtext->ecxt_scantuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_scantuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustAssignVarVirtImpl(state, &slot, estate)
}

/// `ExecJustHashInnerVarWithIV` — hash one inner Var, seeded with an init value.
pub fn ExecJustHashInnerVarWithIV<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
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
    let init_value = match &steps[1].d {
        // The canonical init value crosses back to the bare word.
        ExprEvalStepData::HashDatumInitValue { init_value } => word_of(init_value),
        _ => unreachable!("ExecJustHashInnerVarWithIV: step[1] is not EEOP_HASHDATUM_SET_INITVAL"),
    };
    let attnum = match &steps[2].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("ExecJustHashInnerVarWithIV: step[2] is not an EEOP_*_VAR"),
    };
    let fn_oid = match &steps[3].d {
        ExprEvalStepData::HashDatum { finfo, .. } => finfo
            .as_ref()
            .expect("ExecJustHashInnerVarWithIV: hashop finfo not resolved")
            .fn_oid,
        _ => unreachable!("ExecJustHashInnerVarWithIV: step[3] is not an EEOP_HASHDATUM_*"),
    };

    // CheckOpSlotCompatibility(fetchop, econtext->ecxt_innertuple);
    // slot_getsomeattrs(econtext->ecxt_innertuple, fetchop->d.fetch.last_var);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    CheckOpSlotCompatibility(&steps[0], &slot)?;

    // fcinfo->args[0].value  = econtext->ecxt_innertuple->tts_values[attnum];
    // fcinfo->args[0].isnull = econtext->ecxt_innertuple->tts_isnull[attnum];
    let (value, isnull) = read_hash_var(&slot, attnum, estate)?;

    // hashkey = DatumGetUInt32(setivop->d.hashdatum_initvalue.init_value);
    // hashkey = pg_rotate_left32(hashkey, 1);
    let mut hashkey = init_value.as_u32();
    hashkey = pg_rotate_left32(hashkey, 1);

    // if (!fcinfo->args[0].isnull) {
    //     hashvalue = DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo));
    //     hashkey = hashkey ^ hashvalue;
    // }
    if !isnull {
        let result = function_call1_coll::call(fn_oid, InvalidOid, value)?;
        let hashvalue = result.as_u32();
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
) -> PgResult<(Datum, bool)> {
    // return ExecJustHashVarImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustHashVarImpl(state, &slot, estate)
}

/// `ExecJustHashInnerVar` — hash one inner Var.
pub fn ExecJustHashInnerVar<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustHashVarImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustHashVarImpl(state, &slot, estate)
}

/// `ExecJustHashOuterVarVirt` — hash one virtual outer Var.
pub fn ExecJustHashOuterVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustHashVarVirtImpl(state, econtext->ecxt_outertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustHashVarVirtImpl(state, &slot, estate)
}

/// `ExecJustHashInnerVarVirt` — hash one virtual inner Var.
pub fn ExecJustHashInnerVarVirt<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    // return ExecJustHashVarVirtImpl(state, econtext->ecxt_innertuple, isnull);
    let slot_id = resolve_slot!(estate, econtext, ecxt_innertuple);
    let slot = estate.slot(slot_id).clone();
    ExecJustHashVarVirtImpl(state, &slot, estate)
}

/// `ExecJustHashOuterVarStrict` — hash one outer Var, strict (null short-circuit).
pub fn ExecJustHashOuterVarStrict<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
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
    let fn_oid = match &steps[2].d {
        ExprEvalStepData::HashDatum { finfo, .. } => finfo
            .as_ref()
            .expect("ExecJustHashOuterVarStrict: hashop finfo not resolved")
            .fn_oid,
        _ => unreachable!("ExecJustHashOuterVarStrict: step[2] is not an EEOP_HASHDATUM_*"),
    };

    // CheckOpSlotCompatibility(fetchop, econtext->ecxt_outertuple);
    // slot_getsomeattrs(econtext->ecxt_outertuple, fetchop->d.fetch.last_var);
    let slot_id = resolve_slot!(estate, econtext, ecxt_outertuple);
    let slot = estate.slot(slot_id).clone();
    CheckOpSlotCompatibility(&steps[0], &slot)?;

    // fcinfo->args[0].value  = econtext->ecxt_outertuple->tts_values[attnum];
    // fcinfo->args[0].isnull = econtext->ecxt_outertuple->tts_isnull[attnum];
    let (value, isnull) = read_hash_var(&slot, attnum, estate)?;

    // if (!fcinfo->args[0].isnull) {
    //     *isnull = false;
    //     return DatumGetUInt32(hashop->d.hashdatum.fn_addr(fcinfo));
    // } else {
    //     *isnull = true;            /* return NULL on NULL input */
    //     return (Datum) 0;
    // }
    if !isnull {
        let result = function_call1_coll::call(fn_oid, InvalidOid, value)?;
        Ok((Datum::from_u32(result.as_u32()), false))
    } else {
        Ok((Datum::null(), true))
    }
}
