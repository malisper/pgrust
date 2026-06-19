//! The main interpreter loop (`ExecInterpExpr`, `execExprInterp.c` lines
//! 460–2279). Split out of [`crate::dispatch`] because the single C function is
//! ~1800 lines — far past the per-function size that keeps a family module
//! reviewable. The surrounding interpreter core (ready/still-valid/slot checks)
//! stays in `dispatch`; this module holds only the step-program walk and writes
//! its result through the same owned-step conventions documented there.
//!
//! Owned-model translation of the C dispatch machinery. C uses a computed-goto
//! / switch over `op->opcode`, with `EEO_NEXT()` advancing `op` to the next
//! step and `EEO_JUMP(stepno)` jumping to an absolute step index. Here the
//! program counter is the index `op` into `state.steps`; the body matches on
//! `state.steps[op].opcode` and ends each arm by setting `op` to the next
//! (`op + 1`, i.e. `EEO_NEXT`) or to a jump target (`EEO_JUMP`), then looping.
//!
//! Result cells. C threads `Datum *resvalue` / `bool *resnull` pointers per
//! step, several steps aliasing one cell. The F0 model replaces the pointers
//! with [`ResultCellId`] indices into the [`ExprState`]'s
//! [`ResultCellArena`](types_nodes::execexpr::ResultCellArena): `op->resvalue`
//! and `op->resnull` carry the same id, and `*op->resvalue` / `*op->resnull`
//! map to reading/writing that cell's `value` / `isnull`. The well-known
//! [`STATE_RESULT_CELL`] aliases the `ExprState`'s own `resvalue`/`resnull`
//! (the C `&state->resvalue` / `&state->resnull` default target); the
//! `EEOP_DONE_RETURN` / `EEOP_ASSIGN_TMP` arms read/write that cell, which is
//! kept in sync with the `ExprState` scalar fields by [`read_cell`] /
//! [`write_cell`].
//!
//! Slot value access. The slot per-attribute value/null arrays
//! (`slot->tts_values[]` / `tts_isnull[]`) now live on the canonical
//! [`TupleTableSlot`](types_slot::TupleTableSlot) (slot-payload keystone, #199);
//! the `EEOP_*_FETCHSOME` / `EEOP_*_VAR` arms read input columns through the
//! `execTuples` owner's `slot_getsomeattr` seam, and the `EEOP_ASSIGN_*_VAR` /
//! `EEOP_ASSIGN_TMP[_MAKE_RO]` arms write the projected columns directly into
//! [`ExprState::resultslot`]'s value/null arrays.
//!
//! Blocked surfaces (genuinely-unported owners, seam-and-panic'd inline):
//! - The fmgr call frame (`fcinfo->args[]` / `fcinfo->isnull`): the shared
//!   [`FunctionCallInfoBaseData`](types_nodes::fmgr::FunctionCallInfoBaseData)
//!   is trimmed to `resultinfo` only (the fmgr port widens it). Every arm that
//!   reads `fcinfo->args[i].isnull` for a strict null-check or dispatches
//!   `op->d.func.fn_addr(fcinfo)` is blocked on that widening — the same
//!   blocker the keystone compiler (`execExpr.c`) records at its call-frame
//!   build sites. `FmgrInfo.fn_strict` / `fn_addr` (fmgr #52, merged) are
//!   available, but with no `args[]` to gather into and no `isnull` to read
//!   back there is nothing to call.
//! - nodeAgg per-group/per-trans state (`op->d.agg_trans.pertrans` /
//!   `.aggcontext`, `aggstate->all_pergroups`): parked as opaque `usize`
//!   addresses in the F0 step model (nodeAgg has not yet threaded the real
//!   `AggStatePerTrans` / aggcontext `EcxtId` / `all_pergroups` indexing into
//!   the step payloads), so the `EEOP_AGG_PLAIN_TRANS_*` /
//!   `EEOP_AGG_PLAIN_PERGROUP_NULLCHECK` / `EEOP_AGG_PRESORTED_DISTINCT_*` /
//!   `EEOP_AGG_*DESERIALIZE` arms have no real state to drive.

// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ExprState.resvalue` / `ResultCell.value` and the canonical
// step-payload values (`ConstVal`, `HashDatumInitValue`) carry, and what the
// interpreter loop's cell helpers now operate on directly.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;
use types_nodes::execexpr::ExprEvalOp::*;

use types_nodes::execexpr::{ExprEvalOp, ExprEvalStepData, ExprState, ResultCell, ResultCellId};
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

use crate::dispatch::CheckOpSlotCompatibility;
use crate::eval_agg;
use crate::eval_array;
use crate::eval_composite;
use crate::eval_json_xml;
use crate::eval_misc;
use crate::eval_scalar;
use crate::eval_subscript;

/// Read the `(value, isnull)` of the cell named by [`ResultCellId`] `id`.
///
/// C dereferences `*resvalue` / `*resnull`. For the well-known
/// [`STATE_RESULT_CELL`] the C target is `&state->resvalue` / `&state->resnull`
/// (kept on the `ExprState` itself), so reads of that id come from the
/// `ExprState` scalar fields; all other ids index the arena.
#[inline]
pub(crate) fn read_cell<'mcx>(state: &ExprState<'mcx>, id: ResultCellId) -> (Datum<'mcx>, bool) {
    if id == types_nodes::execexpr::STATE_RESULT_CELL {
        (state.resvalue.clone(), state.resnull)
    } else {
        let c = state.result_cells.get(id);
        (c.value, c.isnull)
    }
}

/// Write the `(value, isnull)` of the cell named by `id` (see [`read_cell`]).
#[inline]
pub(crate) fn write_cell<'mcx>(
    state: &mut ExprState<'mcx>,
    id: ResultCellId,
    value: Datum<'mcx>,
    isnull: bool,
) {
    if id == types_nodes::execexpr::STATE_RESULT_CELL {
        state.resvalue = value;
        state.resnull = isnull;
    } else {
        state
            .result_cells
            .set(id, ResultCell { value, isnull });
    }
}

/// `castNode(AggState, state->parent)` — re-derive the live, owned
/// `AggStateData` the aggregate's compiled `evaltrans` `ExprState` belongs to,
/// for exclusive mutation by the `EEOP_AGG_PLAIN_TRANS_*` /
/// `EEOP_AGG_PLAIN_PERGROUP_NULLCHECK` steps.
///
/// The `PlanStateLink` is `Copy`, so it is copied out of `state.parent` first;
/// `get_mut` then re-derives a fresh `&mut PlanStateNode` from the raw address
/// without borrowing `state`, and the tag-checked downcast recovers the concrete
/// `AggStateData`. This is the owned-model rendering of C's `castNode(AggState,
/// state->parent)` + the subsequent `aggstate->all_pergroups[...]` mutation: in
/// C the same node is reached both through the `ExprState` being walked and
/// through `state->parent`; the trans steps touch only per-group/per-trans
/// state, disjoint from the `ExprState`'s step program, so the aliasing is sound
/// (the same discipline `ExecEvalGroupingFunc` uses, here for `&mut`).
#[inline]
fn agg_parent_mut<'a, 'mcx>(
    state: &ExprState<'mcx>,
) -> &'a mut backend_executor_nodeAgg::AggStateData<'mcx> {
    let mut link = state
        .parent
        .expect("EEOP_AGG_*: aggregate-owned ExprState has no parent AggState back-link");
    link.get_mut()
        .as_agg_state_mut_typed::<backend_executor_nodeAgg::AggStateData<'mcx>>()
        .expect("EEOP_AGG_*: castNode(AggState, state->parent) — parent is not an AggStateData")
}

/// `ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull)` —
/// the main interpreter: walk the step program and return the result datum and
/// its null flag.
///
/// The five input slots C caches up front (`innerslot`/`outerslot`/`scanslot`/
/// `oldslot`/`newslot`) are resolved on demand from the econtext when a VAR /
/// SYSVAR arm needs them, so the hot loop need not borrow them across the whole
/// walk; the resolution mirrors the C `econtext->ecxt_*tuple` reads.
pub fn ExecInterpExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    // C: op = state->steps; (program counter). The slots are resolved per-arm
    // from `econtext` below (see doc comment).
    let mut op: usize = 0;

    loop {
        // The opcode + result-cell ids are Copy; read them out so arm bodies can
        // borrow `state` mutably for the result cells without aliasing the step.
        let opcode = {
            let steps = state
                .steps
                .as_ref()
                .expect("ExecInterpExpr: steps not ready");
            steps[op].opcode
        };
        let resv = {
            let steps = state.steps.as_ref().unwrap();
            steps[op].resvalue
        };

        match opcode {
            EEOP_DONE_RETURN => {
                // *isnull = state->resnull; return state->resvalue;
                return Ok((state.resvalue.clone(), state.resnull));
            }

            EEOP_DONE_NO_RETURN => {
                // Assert(isnull == NULL); return (Datum) 0;
                return Ok((Datum::null(), false));
            }

            EEOP_INNER_FETCHSOME
            | EEOP_OUTER_FETCHSOME
            | EEOP_SCAN_FETCHSOME
            | EEOP_OLD_FETCHSOME
            | EEOP_NEW_FETCHSOME => {
                // C: CheckOpSlotCompatibility(op, <slot>);
                //    slot_getsomeattrs(<slot>, op->d.fetch.last_var);
                //
                // Deform the first `last_var` columns of the input slot. The slot
                // payload (tts_values/tts_isnull) now lives on the canonical
                // TupleTableSlot, addressed through the EState tuple-table pool;
                // the execTuples owner's `slot_getsomeattr` seam runs the real
                // `slot_getsomeattrs` deform.
                let slot_id = input_slot(opcode, econtext, estate);
                let last_var = fetch_last_var(state, op);
                {
                    let steps = state.steps.as_ref().unwrap();
                    CheckOpSlotCompatibility(&steps[op], estate.slot(slot_id))?;
                }
                // slot_getsomeattrs(slot, last_var) deforms columns 1..=last_var;
                // the seam fetches up to a 1-based attnum and returns that
                // attribute (discarded here — the subsequent VAR arms re-read the
                // deformed columns). last_var == 0 means "nothing to deform".
                if last_var > 0 {
                    let _ =
                        backend_executor_execTuples_seams::slot_getsomeattr::call(
                            estate, slot_id, last_var,
                        )?;
                }
                op += 1;
            }

            EEOP_INNER_VAR | EEOP_OUTER_VAR | EEOP_SCAN_VAR | EEOP_OLD_VAR | EEOP_NEW_VAR => {
                // C: int attnum = op->d.var.attnum;
                //    *op->resvalue = <slot>->tts_values[attnum];
                //    *op->resnull  = <slot>->tts_isnull[attnum];
                //
                // The decomposed-data arrays (tts_values/tts_isnull, filled by
                // the preceding FETCHSOME) now live on the canonical
                // TupleTableSlot. attnum is 0-based here (op->d.var.attnum); the
                // seam is 1-based, so read attnum + 1.
                let slot_id = input_slot(opcode, econtext, estate);
                let attnum = var_attnum(state, op);
                let (value, isnull) =
                    backend_executor_execTuples_seams::slot_getsomeattr::call(
                        estate,
                        slot_id,
                        attnum + 1,
                    )?;
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_INNER_SYSVAR
            | EEOP_OUTER_SYSVAR
            | EEOP_SCAN_SYSVAR
            | EEOP_OLD_SYSVAR
            | EEOP_NEW_SYSVAR => {
                // C: ExecEvalSysVar(state, op, econtext, <slot>);
                let slot = sysvar_slot(opcode, econtext, estate);
                eval_scalar::ExecEvalSysVar(state, op, econtext, slot, estate)?;
                op += 1;
            }

            EEOP_WHOLEROW => {
                // C: ExecEvalWholeRowVar(state, op, econtext);
                eval_composite::ExecEvalWholeRowVar(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_ASSIGN_INNER_VAR
            | EEOP_ASSIGN_OUTER_VAR
            | EEOP_ASSIGN_SCAN_VAR
            | EEOP_ASSIGN_OLD_VAR
            | EEOP_ASSIGN_NEW_VAR => {
                // C: int resultnum = op->d.assign_var.resultnum;
                //    int attnum    = op->d.assign_var.attnum;
                //    resultslot->tts_values[resultnum] = <slot>->tts_values[attnum];
                //    resultslot->tts_isnull[resultnum] = <slot>->tts_isnull[attnum];
                //
                // The source <slot> column is read through the execTuples deform
                // seam (attnum is 0-based, the seam is 1-based) and written into
                // state->resultslot's per-attribute value/null arrays, which the
                // canonical TupleTableSlot now carries directly. The preceding
                // FETCHSOME has already deformed the source slot; CheckVarSlot-
                // Compatibility was handled at compile time, so no check here.
                let slot_id = input_slot(opcode, econtext, estate);
                let (resultnum, attnum) = assign_var_fields(state, op);
                let (value, isnull) =
                    backend_executor_execTuples_seams::slot_getsomeattr::call(
                        estate,
                        slot_id,
                        attnum + 1,
                    )?;
                write_resultslot(state, estate, resultnum, value, isnull);
                op += 1;
            }

            EEOP_ASSIGN_TMP => {
                // C: int resultnum = op->d.assign_tmp.resultnum;
                //    resultslot->tts_values[resultnum] = state->resvalue;
                //    resultslot->tts_isnull[resultnum] = state->resnull;
                let resultnum = assign_tmp_resultnum(state, op);
                let value = state.resvalue.clone();
                let isnull = state.resnull;
                write_resultslot(state, estate, resultnum, value, isnull);
                op += 1;
            }

            EEOP_ASSIGN_TMP_MAKE_RO => {
                // C: resultnum = op->d.assign_tmp.resultnum;
                //    resultslot->tts_isnull[resultnum] = state->resnull;
                //    if (!resultslot->tts_isnull[resultnum])
                //        resultslot->tts_values[resultnum] =
                //            MakeExpandedObjectReadOnlyInternal(state->resvalue);
                //    else resultslot->tts_values[resultnum] = state->resvalue;
                let resultnum = assign_tmp_resultnum(state, op);
                let isnull = state.resnull;
                let value = if !isnull {
                    // MakeExpandedObjectReadOnlyInternal: a read-write expanded
                    // object becomes its built-in read-only pointer; any other
                    // datum (by-value or non-expanded by-reference) passes
                    // through unchanged. The pointer deref lives in the
                    // expandeddatum owner, so the transform crosses the seam.
                    let mcx = estate.es_query_cxt;
                    backend_utils_adt_misc2_seams::make_expanded_object_read_only_internal_v::call(
                        mcx,
                        &state.resvalue,
                    )?
                } else {
                    state.resvalue.clone()
                };
                write_resultslot(state, estate, resultnum, value, isnull);
                op += 1;
            }

            EEOP_CONST => {
                // C: *op->resnull = op->d.constval.isnull;
                //    *op->resvalue = op->d.constval.value;
                let (value, isnull) = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::ConstVal { value, isnull } => (value.clone(), *isnull),
                    _ => unreachable!("EEOP_CONST: payload is not ConstVal"),
                };
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            // Function-call implementations. Arguments have previously been
            // evaluated directly into fcinfo->args (in C); the owned model
            // gathers the per-argument result cells into fcinfo->args just before
            // dispatch (exec_func_step). #296: the fmgr-widened call frame carries
            // fncollation/args/isnull, and function_call_invoke re-dispatches by
            // fn_oid under the recorded collation. EEOP_FUNCEXPR is non-strict;
            // the _STRICT[_1|_2] variants short-circuit to NULL on any NULL arg
            // (the nargs-specialized forms are identical here — the args vector is
            // already sized by the step).
            EEOP_FUNCEXPR => {
                // C: ExecInterpExecuteFuncStep (non-strict path).
                eval_scalar::exec_func_step(state, op, false, estate)?;
                op += 1;
            }
            EEOP_FUNCEXPR_STRICT
            | EEOP_FUNCEXPR_STRICT_1
            | EEOP_FUNCEXPR_STRICT_2 => {
                // C: strict function — short-circuit to NULL on any NULL arg,
                //    else dispatch.
                eval_scalar::exec_func_step(state, op, true, estate)?;
                op += 1;
            }

            EEOP_FUNCEXPR_FUSAGE => {
                // C: ExecEvalFuncExprFusage(state, op, econtext);
                eval_scalar::ExecEvalFuncExprFusage(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_FUNCEXPR_STRICT_FUSAGE => {
                // C: ExecEvalFuncExprStrictFusage(state, op, econtext);
                eval_scalar::ExecEvalFuncExprStrictFusage(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_BOOL_AND_STEP_FIRST => {
                // C: *op->d.boolexpr.anynull = false;  /* then fall through to
                //    EEOP_BOOL_AND_STEP */
                let anynull = boolexpr_anynull(state, op);
                write_cell(state, anynull, Datum::from_bool(false), false);
                op = bool_and_step(state, op, resv);
            }

            EEOP_BOOL_AND_STEP => {
                op = bool_and_step(state, op, resv);
            }

            EEOP_BOOL_AND_STEP_LAST => {
                // C: if (*op->resnull) { /* keep NULL */ }
                //    else if (!DatumGetBool(*op->resvalue)) { /* keep FALSE */ }
                //    else if (*op->d.boolexpr.anynull) { *resvalue = 0; *resnull = true; }
                //    else { /* keep TRUE */ }
                let (value, isnull) = read_cell(state, resv);
                if isnull {
                    // keep NULL
                } else if !value.as_bool() {
                    // keep FALSE
                } else {
                    let anynull = boolexpr_anynull(state, op);
                    if read_cell(state, anynull).0.as_bool() {
                        write_cell(state, resv, Datum::null(), true);
                    }
                    // else keep TRUE
                }
                op += 1;
            }

            EEOP_BOOL_OR_STEP_FIRST => {
                // C: *op->d.boolexpr.anynull = false;  /* fall through to OR_STEP */
                let anynull = boolexpr_anynull(state, op);
                write_cell(state, anynull, Datum::from_bool(false), false);
                op = bool_or_step(state, op, resv);
            }

            EEOP_BOOL_OR_STEP => {
                op = bool_or_step(state, op, resv);
            }

            EEOP_BOOL_OR_STEP_LAST => {
                // C: if (*op->resnull) { /* keep NULL */ }
                //    else if (DatumGetBool(*op->resvalue)) { /* keep TRUE */ }
                //    else if (*op->d.boolexpr.anynull) { *resvalue = 0; *resnull = true; }
                //    else { /* keep FALSE */ }
                let (value, isnull) = read_cell(state, resv);
                if isnull {
                    // keep NULL
                } else if value.as_bool() {
                    // keep TRUE
                } else {
                    let anynull = boolexpr_anynull(state, op);
                    if read_cell(state, anynull).0.as_bool() {
                        write_cell(state, resv, Datum::null(), true);
                    }
                    // else keep FALSE
                }
                op += 1;
            }

            EEOP_BOOL_NOT_STEP => {
                // C: *op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
                // (resnull deliberately ignored: NULL in => NULL out)
                let (value, isnull) = read_cell(state, resv);
                write_cell(state, resv, Datum::from_bool(!value.as_bool()), isnull);
                op += 1;
            }

            EEOP_QUAL => {
                // C: if (*op->resnull || !DatumGetBool(*op->resvalue)) {
                //        *op->resnull = false;
                //        *op->resvalue = BoolGetDatum(false);
                //        EEO_JUMP(op->d.qualexpr.jumpdone);
                //    }
                let (value, isnull) = read_cell(state, resv);
                if isnull || !value.as_bool() {
                    write_cell(state, resv, Datum::from_bool(false), false);
                    op = qual_jumpdone(state, op);
                } else {
                    op += 1;
                }
            }

            EEOP_JUMP => {
                // C: EEO_JUMP(op->d.jump.jumpdone);
                op = jump_target(state, op);
            }

            EEOP_JUMP_IF_NULL => {
                // C: if (*op->resnull) EEO_JUMP(op->d.jump.jumpdone);
                if read_cell(state, resv).1 {
                    op = jump_target(state, op);
                } else {
                    op += 1;
                }
            }

            EEOP_JUMP_IF_NOT_NULL => {
                // C: if (!*op->resnull) EEO_JUMP(op->d.jump.jumpdone);
                if !read_cell(state, resv).1 {
                    op = jump_target(state, op);
                } else {
                    op += 1;
                }
            }

            EEOP_JUMP_IF_NOT_TRUE => {
                // C: if (*op->resnull || !DatumGetBool(*op->resvalue))
                //        EEO_JUMP(op->d.jump.jumpdone);
                let (value, isnull) = read_cell(state, resv);
                if isnull || !value.as_bool() {
                    op = jump_target(state, op);
                } else {
                    op += 1;
                }
            }

            EEOP_NULLTEST_ISNULL => {
                // C: *op->resvalue = BoolGetDatum(*op->resnull);
                //    *op->resnull = false;
                let isnull = read_cell(state, resv).1;
                write_cell(state, resv, Datum::from_bool(isnull), false);
                op += 1;
            }

            EEOP_NULLTEST_ISNOTNULL => {
                // C: *op->resvalue = BoolGetDatum(!*op->resnull);
                //    *op->resnull = false;
                let isnull = read_cell(state, resv).1;
                write_cell(state, resv, Datum::from_bool(!isnull), false);
                op += 1;
            }

            EEOP_NULLTEST_ROWISNULL => {
                // C: ExecEvalRowNull(state, op, econtext);
                eval_composite::ExecEvalRowNull(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_NULLTEST_ROWISNOTNULL => {
                // C: ExecEvalRowNotNull(state, op, econtext);
                eval_composite::ExecEvalRowNotNull(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_BOOLTEST_IS_TRUE => {
                // C: if (*op->resnull) { *resvalue = false; *resnull = false; }
                //    /* else input value is the correct output */
                let (_value, isnull) = read_cell(state, resv);
                if isnull {
                    write_cell(state, resv, Datum::from_bool(false), false);
                }
                op += 1;
            }

            EEOP_BOOLTEST_IS_NOT_TRUE => {
                // C: if (*op->resnull) { *resvalue = true; *resnull = false; }
                //    else *resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
                let (value, isnull) = read_cell(state, resv);
                if isnull {
                    write_cell(state, resv, Datum::from_bool(true), false);
                } else {
                    write_cell(state, resv, Datum::from_bool(!value.as_bool()), isnull);
                }
                op += 1;
            }

            EEOP_BOOLTEST_IS_FALSE => {
                // C: if (*op->resnull) { *resvalue = false; *resnull = false; }
                //    else *resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
                let (value, isnull) = read_cell(state, resv);
                if isnull {
                    write_cell(state, resv, Datum::from_bool(false), false);
                } else {
                    write_cell(state, resv, Datum::from_bool(!value.as_bool()), isnull);
                }
                op += 1;
            }

            EEOP_BOOLTEST_IS_NOT_FALSE => {
                // C: if (*op->resnull) { *resvalue = true; *resnull = false; }
                //    /* else input value is the correct output */
                let (_value, isnull) = read_cell(state, resv);
                if isnull {
                    write_cell(state, resv, Datum::from_bool(true), false);
                }
                op += 1;
            }

            EEOP_PARAM_EXEC => {
                // C: ExecEvalParamExec(state, op, econtext);
                eval_scalar::ExecEvalParamExec(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_PARAM_EXTERN => {
                // C: ExecEvalParamExtern(state, op, econtext);
                eval_scalar::ExecEvalParamExtern(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_PARAM_CALLBACK => {
                // C: op->d.cparam.paramfunc(state, op, econtext);
                //
                // paramfunc is an ExecEvalSubroutine an extension module installs
                // to supply a PARAM_EXTERN value (the "allow an extension module
                // to supply a PARAM_EXTERN value" path). No extension param
                // provider is ported, and the F0 ExecEvalSubroutine fn-pointer
                // shape (void (*)(ExprState*, ExprEvalStep*, ExprContext*)) does
                // not yet thread the EState the body needs to read/write cells
                // and the param list — so there is no faithful body to dispatch.
                let _ = (op, econtext, estate);
                panic!(
                    "EEOP_PARAM_CALLBACK: dispatches op->d.cparam.paramfunc, an \
                     extension-supplied ExecEvalSubroutine; no param-callback \
                     provider is ported and the F0 ExecEvalSubroutine shape does not \
                     thread the EState the callback needs; blocked until a param \
                     provider lands"
                );
            }

            EEOP_PARAM_SET => {
                // C: ExecEvalParamSet(state, op, econtext);
                eval_scalar::ExecEvalParamSet(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_CASE_TESTVAL => {
                // C: *op->resvalue = *op->d.casetest.value;
                //    *op->resnull  = *op->d.casetest.isnull;
                let cell = casetest_cell(state, op);
                let (value, isnull) = read_cell(state, cell);
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_CASE_TESTVAL_EXT => {
                // C: *op->resvalue = econtext->caseValue_datum;
                //    *op->resnull  = econtext->caseValue_isNull;
                let ecxt = estate.ecxt(econtext);
                let (value, isnull) = (ecxt.caseValue_datum.clone(), ecxt.caseValue_isNull);
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_MAKE_READONLY => {
                // C: if (!*op->d.make_readonly.isnull)
                //        *op->resvalue =
                //            MakeExpandedObjectReadOnlyInternal(*op->d.make_readonly.value);
                //    *op->resnull = *op->d.make_readonly.isnull;
                //
                // The source cell read (op->d.make_readonly.value/isnull) and the
                // resnull copy ARE expressible, but the non-null branch applies
                // MakeExpandedObjectReadOnlyInternal (expandeddatum.c, unported).
                // Faithfully blocked: the arm's whole job on a non-null value is
                // that R/O transform.
                let _ = (op, estate);
                panic!(
                    "EEOP_MAKE_READONLY: on a non-null value forces \
                     MakeExpandedObjectReadOnlyInternal(*op->d.make_readonly.value) \
                     (expandeddatum.c, unported); blocked until expandeddatum lands"
                );
            }

            EEOP_IOCOERCE => {
                // C: output function then input function over fcinfo->args[0],
                //    dispatched via FunctionCallInvoke(fcinfo_out/in). #296: the
                //    call frame carries args/collation/isnull, so each I/O
                //    function dispatches by OID through function_call_invoke.
                eval_scalar::ExecEvalCoerceViaIO(state, op, estate)?;
                op += 1;
            }

            EEOP_IOCOERCE_SAFE => {
                // C: ExecEvalCoerceViaIOSafe(state, op);
                eval_scalar::ExecEvalCoerceViaIOSafe(state, op, estate)?;
                op += 1;
            }

            EEOP_DISTINCT => {
                // C: IS DISTINCT FROM — both NULL -> false, one NULL -> true,
                //    else invert the equality function's result.
                eval_scalar::exec_distinct_step(state, op, false, estate)?;
                op += 1;
            }

            EEOP_NOT_DISTINCT => {
                // C: IS NOT DISTINCT FROM — both NULL -> true, one NULL -> false,
                //    else the raw equality function result.
                eval_scalar::exec_distinct_step(state, op, true, estate)?;
                op += 1;
            }

            EEOP_NULLIF => {
                // C: compare the two (already-gathered) args via the equality fn;
                //    return NULL if equal, else the first argument.
                eval_scalar::exec_nullif_step(state, op, estate)?;
                op += 1;
            }

            EEOP_SQLVALUEFUNCTION => {
                // C: ExecEvalSQLValueFunction(state, op);
                eval_scalar::ExecEvalSQLValueFunction(state, op, estate)?;
                op += 1;
            }

            EEOP_CURRENTOFEXPR => {
                // C: ExecEvalCurrentOfExpr(state, op);
                eval_scalar::ExecEvalCurrentOfExpr(state, op, estate)?;
                op += 1;
            }

            EEOP_NEXTVALUEEXPR => {
                // C: ExecEvalNextValueExpr(state, op);
                eval_scalar::ExecEvalNextValueExpr(state, op, estate)?;
                op += 1;
            }

            EEOP_RETURNINGEXPR => {
                // C: if (state->flags & op->d.returningexpr.nullflag) {
                //        *op->resvalue = 0; *op->resnull = true;
                //        EEO_JUMP(op->d.returningexpr.jumpdone);
                //    }
                let (nullflag, jumpdone) = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::ReturningExpr { nullflag, jumpdone } => {
                        (*nullflag, *jumpdone)
                    }
                    _ => unreachable!("EEOP_RETURNINGEXPR: payload is not ReturningExpr"),
                };
                if state.flags & nullflag != 0 {
                    write_cell(state, resv, Datum::null(), true);
                    op = jumpdone as usize;
                } else {
                    op += 1;
                }
            }

            EEOP_ARRAYEXPR => {
                // C: ExecEvalArrayExpr(state, op);
                eval_array::ExecEvalArrayExpr(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_ARRAYCOERCE => {
                // C: ExecEvalArrayCoerce(state, op, econtext);
                eval_array::ExecEvalArrayCoerce(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_ROW => {
                // C: ExecEvalRow(state, op);
                eval_composite::ExecEvalRow(state, op, estate)?;
                op += 1;
            }

            EEOP_ROWCOMPARE_STEP => {
                // C: force NULL if (finfo->fn_strict && (args[0]|args[1] null)),
                //    else dispatch fn_addr(fcinfo) and branch on the int result.
                let (jumpnull, jumpdone) = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::RowCompareStep { jumpnull, jumpdone, .. } => {
                        (*jumpnull, *jumpdone)
                    }
                    _ => unreachable!("EEOP_ROWCOMPARE_STEP: payload is not RowCompareStep"),
                };
                op = eval_scalar::exec_rowcompare_step(state, op, jumpnull, jumpdone, estate)?;
            }

            EEOP_ROWCOMPARE_FINAL => {
                // C: int32 cmpresult = DatumGetInt32(*op->resvalue);
                //    CompareType cmptype = op->d.rowcompare_final.cmptype;
                //    *op->resnull = false;
                //    switch (cmptype) { LT: <0; LE: <=0; GE: >=0; GT: >0 }
                use types_nodes::execexpr::CompareType;
                let cmptype = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::RowCompareFinal { cmptype } => *cmptype,
                    _ => unreachable!("EEOP_ROWCOMPARE_FINAL: payload is not RowCompareFinal"),
                };
                let cmpresult = read_cell(state, resv).0.as_i32();
                let result = match cmptype {
                    CompareType::COMPARE_LT => cmpresult < 0,
                    CompareType::COMPARE_LE => cmpresult <= 0,
                    CompareType::COMPARE_GE => cmpresult >= 0,
                    CompareType::COMPARE_GT => cmpresult > 0,
                    // EQ and NE cases aren't allowed here (C: Assert(false)).
                    _ => unreachable!("EEOP_ROWCOMPARE_FINAL: invalid cmptype {cmptype:?}"),
                };
                write_cell(state, resv, Datum::from_bool(result), false);
                op += 1;
            }

            EEOP_MINMAX => {
                // C: ExecEvalMinMax(state, op);
                eval_composite::ExecEvalMinMax(state, op, estate)?;
                op += 1;
            }

            EEOP_FIELDSELECT => {
                // C: ExecEvalFieldSelect(state, op, econtext);
                eval_composite::ExecEvalFieldSelect(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_FIELDSTORE_DEFORM => {
                // C: ExecEvalFieldStoreDeForm(state, op, econtext);
                eval_composite::ExecEvalFieldStoreDeForm(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_FIELDSTORE_FORM => {
                // C: ExecEvalFieldStoreForm(state, op, econtext);
                eval_composite::ExecEvalFieldStoreForm(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_SBSREF_SUBSCRIPTS => {
                // C: if (op->d.sbsref_subscript.subscriptfunc(state, op, econtext))
                //        EEO_NEXT();
                //    else EEO_JUMP(op->d.sbsref_subscript.jumpdone);
                //
                // subscriptfunc is a SubscriptMethod discriminant; the EState is
                // threaded in (see eval_subscript module note).
                let (method, jumpdone) = {
                    let steps = state.steps.as_ref().unwrap();
                    match &steps[op].d {
                        ExprEvalStepData::SbsRefSubscript {
                            subscriptfunc: Some(m),
                            jumpdone,
                            ..
                        } => (*m, *jumpdone),
                        other => unreachable!(
                            "EEOP_SBSREF_SUBSCRIPTS: step.d is not SbsRefSubscript: {other:?}"
                        ),
                    }
                };
                if eval_subscript::exec_sbsref_subscripts(
                    state, op, econtext, estate, method, resv,
                )? {
                    op += 1;
                } else {
                    op = jumpdone as usize;
                }
            }

            EEOP_SBSREF_OLD | EEOP_SBSREF_ASSIGN | EEOP_SBSREF_FETCH => {
                // C: op->d.sbsref.subscriptfunc(state, op, econtext);
                let method = {
                    let steps = state.steps.as_ref().unwrap();
                    match &steps[op].d {
                        ExprEvalStepData::SbsRef {
                            subscriptfunc: Some(m),
                            ..
                        } => *m,
                        other => unreachable!(
                            "EEOP_SBSREF_OLD/ASSIGN/FETCH: step.d is not SbsRef: {other:?}"
                        ),
                    }
                };
                eval_subscript::exec_sbsref(state, op, econtext, estate, method, resv)?;
                op += 1;
            }

            EEOP_CONVERT_ROWTYPE => {
                // C: ExecEvalConvertRowtype(state, op, econtext);
                eval_composite::ExecEvalConvertRowtype(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_SCALARARRAYOP => {
                // C: ExecEvalScalarArrayOp(state, op);
                eval_scalar::ExecEvalScalarArrayOp(state, op, estate)?;
                op += 1;
            }

            EEOP_HASHED_SCALARARRAYOP => {
                // C: ExecEvalHashedScalarArrayOp(state, op, econtext);
                eval_scalar::ExecEvalHashedScalarArrayOp(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_DOMAIN_TESTVAL => {
                // C: *op->resvalue = *op->d.casetest.value;
                //    *op->resnull  = *op->d.casetest.isnull;
                let cell = casetest_cell(state, op);
                let (value, isnull) = read_cell(state, cell);
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_DOMAIN_TESTVAL_EXT => {
                // C: *op->resvalue = econtext->domainValue_datum;
                //    *op->resnull  = econtext->domainValue_isNull;
                let ecxt = estate.ecxt(econtext);
                let (value, isnull) = (ecxt.domainValue_datum.clone(), ecxt.domainValue_isNull);
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_DOMAIN_NOTNULL => {
                // C: ExecEvalConstraintNotNull(state, op);
                eval_scalar::ExecEvalConstraintNotNull(state, op, estate)?;
                op += 1;
            }

            EEOP_DOMAIN_CHECK => {
                // C: ExecEvalConstraintCheck(state, op);
                eval_scalar::ExecEvalConstraintCheck(state, op, estate)?;
                op += 1;
            }

            EEOP_HASHDATUM_SET_INITVAL => {
                // C: *op->resvalue = op->d.hashdatum_initvalue.init_value;
                //    *op->resnull = false;
                let init_value = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::HashDatumInitValue { init_value } => init_value.clone(),
                    _ => unreachable!(
                        "EEOP_HASHDATUM_SET_INITVAL: payload is not HashDatumInitValue"
                    ),
                };
                write_cell(state, resv, init_value, false);
                op += 1;
            }

            EEOP_HASHDATUM_FIRST
            | EEOP_HASHDATUM_FIRST_STRICT
            | EEOP_HASHDATUM_NEXT32
            | EEOP_HASHDATUM_NEXT32_STRICT => {
                // C: read fcinfo->args[0].isnull, dispatch op->d.hashdatum.fn_addr(fcinfo),
                //    combine with iresult (NEXT32 rotates+XORs). The FIRST variants
                //    seed the hash; the _STRICT variants return NULL (and jump) on
                //    a NULL input.
                let first = matches!(opcode, EEOP_HASHDATUM_FIRST | EEOP_HASHDATUM_FIRST_STRICT);
                let strict = matches!(
                    opcode,
                    EEOP_HASHDATUM_FIRST_STRICT | EEOP_HASHDATUM_NEXT32_STRICT
                );
                let jumpdone = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::HashDatum { jumpdone, .. } => *jumpdone,
                    _ => unreachable!("EEOP_HASHDATUM_*: payload is not HashDatum"),
                };
                op = eval_scalar::exec_hashdatum_step(state, op, first, strict, jumpdone, estate)?;
            }

            EEOP_XMLEXPR => {
                // C: ExecEvalXmlExpr(state, op);
                eval_json_xml::ExecEvalXmlExpr(state, op, estate)?;
                op += 1;
            }

            EEOP_JSON_CONSTRUCTOR => {
                // C: ExecEvalJsonConstructor(state, op, econtext);
                eval_json_xml::ExecEvalJsonConstructor(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_IS_JSON => {
                // C: ExecEvalJsonIsPredicate(state, op);
                eval_json_xml::ExecEvalJsonIsPredicate(state, op, estate)?;
                op += 1;
            }

            EEOP_JSONEXPR_PATH => {
                // C: EEO_JUMP(ExecEvalJsonExprPath(state, op, econtext));
                // The path evaluator returns the next step index to jump to.
                op = eval_json_xml::ExecEvalJsonExprPath(state, op, econtext, estate)? as usize;
            }

            EEOP_JSONEXPR_COERCION => {
                // C: ExecEvalJsonCoercion(state, op, econtext);
                eval_json_xml::ExecEvalJsonCoercion(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_JSONEXPR_COERCION_FINISH => {
                // C: ExecEvalJsonCoercionFinish(state, op);
                eval_json_xml::ExecEvalJsonCoercionFinish(state, op, estate)?;
                op += 1;
            }

            EEOP_AGGREF => {
                // C: int aggno = op->d.aggref.aggno;
                //    *op->resvalue = econtext->ecxt_aggvalues[aggno];
                //    *op->resnull  = econtext->ecxt_aggnulls[aggno];
                let aggno = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::Aggref { aggno } => *aggno,
                    _ => unreachable!("EEOP_AGGREF: payload is not Aggref"),
                };
                let ecxt = estate.ecxt(econtext);
                let value = ecxt.ecxt_aggvalues[aggno as usize].clone();
                let isnull = ecxt.ecxt_aggnulls[aggno as usize];
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_GROUPING_FUNC => {
                // C: ExecEvalGroupingFunc(state, op);
                eval_misc::ExecEvalGroupingFunc(state, op, estate)?;
                op += 1;
            }

            EEOP_WINDOW_FUNC => {
                // C: WindowFuncExprState *wfunc = op->d.window_func.wfstate;
                //    *op->resvalue = econtext->ecxt_aggvalues[wfunc->wfuncno];
                //    *op->resnull  = econtext->ecxt_aggnulls[wfunc->wfuncno];
                //
                // The owned model parks the WindowFuncExprState list on the parent
                // WindowAggState.funcs (drained there from the ExprState's
                // found_window_funcs channel by ExecInitWindowAgg); the step
                // carries `funcidx`, the position of this window function's state
                // in that list. The `wfuncno` (the index into
                // ecxt_aggvalues/ecxt_aggnulls, assigned by ExecInitWindowAgg's
                // dedup loop) is read through the ExprState.parent back-link — the
                // same parent-reach the EEOP_AGG_* steps use — exactly C's
                // `wfunc->wfuncno`.
                let funcidx = match &state.steps.as_ref().unwrap()[op].d {
                    ExprEvalStepData::WindowFunc { funcidx } => *funcidx as usize,
                    _ => unreachable!("EEOP_WINDOW_FUNC: payload is not WindowFunc"),
                };
                let link = state
                    .parent
                    .expect("EEOP_WINDOW_FUNC: window-owned ExprState has no parent WindowAggState back-link");
                let winstate = link
                    .get()
                    .as_window_agg_state()
                    .expect("EEOP_WINDOW_FUNC: castNode(WindowAggState, state->parent) — parent is not a WindowAggState");
                let wfuncno = winstate
                    .funcs
                    .as_ref()
                    .expect("EEOP_WINDOW_FUNC: winstate->funcs not populated")
                    [funcidx]
                    .wfuncno as usize;
                let ecxt = estate.ecxt(econtext);
                let value = ecxt.ecxt_aggvalues[wfuncno].clone();
                let isnull = ecxt.ecxt_aggnulls[wfuncno];
                write_cell(state, resv, value, isnull);
                op += 1;
            }

            EEOP_MERGE_SUPPORT_FUNC => {
                // C: ExecEvalMergeSupportFunc(state, op, econtext);
                eval_misc::ExecEvalMergeSupportFunc(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_SUBPLAN => {
                // C: ExecEvalSubPlan(state, op, econtext);
                eval_misc::ExecEvalSubPlan(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_AGG_STRICT_DESERIALIZE | EEOP_AGG_DESERIALIZE => {
                // C (STRICT): if (op->d.agg_deserialize.fcinfo_data->args[0].isnull)
                //                 EEO_JUMP(op->d.agg_deserialize.jumpnull);  /* else fall through */
                // C (DESERIALIZE): switch to aggstate->tmpcontext memory, dispatch
                //                  FunctionCallInvoke(fcinfo), store result.
                //
                // The args[0].isnull read and the FunctionCallInvoke dispatch are
                // now expressible (fmgr #296 widened the call frame +
                // function_call_invoke). The REMAINING blocker is the non-strict
                // arm (which the STRICT arm falls through into): it runs the
                // deserialfn inside aggstate->tmpcontext->ecxt_per_tuple_memory,
                // and aggstate = castNode(AggState, state->parent) — the
                // nodeAgg-owned AggState reached through state->parent, which the
                // F0 model does not thread here (AggState-as-Node, #200). Running
                // the deserialfn in the wrong memory context would leak/misplace
                // its allocations, so this stays mirror-PG-and-panic. The pair
                // can't be split: the STRICT arm is a bare null-check that falls
                // through to the non-strict body.
                let _ = (op, estate);
                panic!(
                    "EEOP_AGG_*DESERIALIZE: the args[0].isnull check + deserialfn \
                     FunctionCallInvoke are modeled (fmgr #296), but the non-strict \
                     arm must run the call inside aggstate->tmpcontext \
                     (aggstate = castNode(AggState, state->parent)), the \
                     nodeAgg-owned AggState the F0 model does not thread here \
                     (AggState-as-Node, #200); blocked until nodeAgg threads \
                     state->parent's AggState + its tmpcontext"
                );
            }

            EEOP_AGG_STRICT_INPUT_CHECK_ARGS
            | EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1
            | EEOP_AGG_STRICT_INPUT_CHECK_NULLS => {
                // C (ARGS/ARGS_1): NullableDatum *args = op->d.agg_strict_input_check.args;
                //   for argno in 0..nargs { if (args[argno].isnull) EEO_JUMP(jumpnull); }
                // C (NULLS): bool *nulls = op->d.agg_strict_input_check.nulls;
                //   for argno in 0..nargs { if (nulls[argno]) EEO_JUMP(jumpnull); }
                //
                // The ARGS variants scan the per-arg cells the transfn-argument
                // sub-expressions evaluated into (`arg_cells`, the owned-model
                // `trans_fcinfo->args + 1`). The single-column-sort NULLS variant
                // reads `&state->resnull` (STATE_RESULT_CELL), into which that
                // path evaluated its single input.
                let (arg_cells, uses_nulls, jumpnull) = {
                    let steps = state.steps.as_ref().unwrap();
                    match &steps[op].d {
                        ExprEvalStepData::AggStrictInputCheck {
                            arg_cells,
                            jumpnull,
                            ..
                        } => {
                            let uses_nulls = opcode == EEOP_AGG_STRICT_INPUT_CHECK_NULLS;
                            let cells: Vec<ResultCellId> = arg_cells
                                .as_ref()
                                .map(|v| v.iter().copied().collect())
                                .unwrap_or_default();
                            (cells, uses_nulls, *jumpnull)
                        }
                        other => unreachable!(
                            "EEOP_AGG_STRICT_INPUT_CHECK_*: payload is not AggStrictInputCheck: {other:?}"
                        ),
                    }
                };
                let any_null = if uses_nulls {
                    // strictnulls = &state->resnull (the single-column sort path).
                    read_cell(state, types_nodes::execexpr::STATE_RESULT_CELL).1
                } else {
                    arg_cells.iter().any(|&c| read_cell(state, c).1)
                };
                if any_null {
                    op = jumpnull as usize; // EEO_JUMP(jumpnull)
                } else {
                    op += 1; // EEO_NEXT()
                }
            }

            EEOP_AGG_PLAIN_PERGROUP_NULLCHECK => {
                // C: AggState *aggstate = castNode(AggState, state->parent);
                //    AggStatePerGroup pergroup_allaggs =
                //        aggstate->all_pergroups[op->d.agg_plain_pergroup_nullcheck.setoff];
                //    if (pergroup_allaggs == NULL) EEO_JUMP(jumpnull);
                let (setoff, jumpnull) = {
                    let steps = state.steps.as_ref().unwrap();
                    match &steps[op].d {
                        ExprEvalStepData::AggPlainPergroupNullcheck { setoff, jumpnull } => {
                            (*setoff as usize, *jumpnull)
                        }
                        other => unreachable!(
                            "EEOP_AGG_PLAIN_PERGROUP_NULLCHECK: payload mismatch: {other:?}"
                        ),
                    }
                };
                let aggstate = agg_parent_mut(state);
                let is_null =
                    backend_executor_nodeAgg::transition::agg_pergroup_allaggs_is_null(
                        aggstate, setoff,
                    );
                if is_null {
                    op = jumpnull as usize; // EEO_JUMP(jumpnull)
                } else {
                    op += 1; // EEO_NEXT()
                }
            }

            EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL
            | EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL
            | EEOP_AGG_PLAIN_TRANS_BYVAL
            | EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
            | EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
            | EEOP_AGG_PLAIN_TRANS_BYREF => {
                // C: aggstate = castNode(AggState, state->parent);
                //    pertrans = op->d.agg_trans.pertrans;
                //    pergroup = &aggstate->all_pergroups[setoff][transno];
                //    INIT_STRICT: if (noTransValue) ExecAggInitGroup(...)
                //                 else if (!transValueIsNull) ExecAggPlainTransBy*(...)
                //    STRICT:      if (!transValueIsNull) ExecAggPlainTransBy*(...)
                //    plain:       ExecAggPlainTransBy*(...)
                let (transno, setoff, setno, aggcontext, arg_cell_ids) = {
                    let steps = state.steps.as_ref().unwrap();
                    match &steps[op].d {
                        ExprEvalStepData::AggTrans {
                            pertrans,
                            aggcontext,
                            setno,
                            transno,
                            setoff,
                            arg_cells,
                        } => {
                            let _ = transno;
                            let cells: Vec<ResultCellId> = arg_cells.iter().copied().collect();
                            (*pertrans, *setoff as usize, *setno, *aggcontext, cells)
                        }
                        other => unreachable!(
                            "EEOP_AGG_PLAIN_TRANS_*: payload is not AggTrans: {other:?}"
                        ),
                    }
                };
                // `pertrans` (carried as the AggTrans `pertrans` field) is the
                // index into `aggstate->pertrans`.
                let transno_idx = transno;

                // Gather the transfn's per-row input args (fcinfo->args[1..]) from
                // the cells the input sub-expressions evaluated into. Read these
                // BEFORE re-deriving the &mut AggState (the cells live on `state`,
                // the trans state on the parent AggState — disjoint, as in C).
                let mut input_args: Vec<Datum<'mcx>> = Vec::with_capacity(arg_cell_ids.len());
                let mut input_args_null: Vec<bool> = Vec::with_capacity(arg_cell_ids.len());
                for &c in &arg_cell_ids {
                    let (v, isnull) = read_cell(state, c);
                    input_args.push(v);
                    input_args_null.push(isnull);
                }

                let byref = matches!(
                    opcode,
                    EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
                        | EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
                        | EEOP_AGG_PLAIN_TRANS_BYREF
                );
                let is_init = matches!(
                    opcode,
                    EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL
                        | EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
                );
                let is_strict = matches!(
                    opcode,
                    EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL | EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
                );

                let aggstate = agg_parent_mut(state);
                let (no_trans_value, trans_value_is_null) =
                    backend_executor_nodeAgg::transition::agg_pergroup_flags(
                        aggstate, setoff, transno_idx,
                    );

                if is_init && no_trans_value {
                    backend_executor_nodeAgg::transition::ExecAggInitGroup(
                        aggstate,
                        transno_idx,
                        setoff,
                        aggcontext,
                        &input_args,
                        estate,
                    )?;
                } else if (is_init || is_strict) && trans_value_is_null {
                    // strict: skip the transfn while the running value is NULL.
                } else if byref {
                    backend_executor_nodeAgg::transition::ExecAggPlainTransByRef(
                        aggstate,
                        transno_idx,
                        setoff,
                        setno,
                        aggcontext,
                        &input_args,
                        &input_args_null,
                        estate,
                    )?;
                } else {
                    backend_executor_nodeAgg::transition::ExecAggPlainTransByVal(
                        aggstate,
                        transno_idx,
                        setoff,
                        setno,
                        aggcontext,
                        &input_args,
                        &input_args_null,
                        estate,
                    )?;
                }

                op += 1; // EEO_NEXT()
            }

            EEOP_AGG_PRESORTED_DISTINCT_SINGLE | EEOP_AGG_PRESORTED_DISTINCT_MULTI => {
                // C: pertrans = op->d.agg_presorted_distinctcheck.pertrans;
                //    aggstate = castNode(AggState, state->parent);
                //    if (ExecEvalPreOrderedDistinct{Single,Multi}(aggstate, pertrans))
                //        EEO_NEXT();
                //    else EEO_JUMP(op->d.agg_presorted_distinctcheck.jumpdistinct);
                let (pertrans, input_cell, jumpdistinct) = {
                    let steps = state.steps.as_ref().unwrap();
                    match &steps[op].d {
                        ExprEvalStepData::AggPresortedDistinctCheck {
                            pertrans,
                            input_cell,
                            jumpdistinct,
                            ..
                        } => (*pertrans, *input_cell, *jumpdistinct),
                        other => unreachable!(
                            "EEOP_AGG_PRESORTED_DISTINCT_*: payload mismatch: {other:?}"
                        ),
                    }
                };
                let is_single = opcode == EEOP_AGG_PRESORTED_DISTINCT_SINGLE;
                let distinct = if is_single {
                    // The SINGLE comparator reads pertrans->transfn_fcinfo->args[1];
                    // the owned model evaluated the input into `input_cell`, so copy
                    // it into the per-trans frame before the comparison (C recurses
                    // the input straight into that fcinfo arg).
                    let (value, isnull) = read_cell(state, input_cell);
                    let aggstate = agg_parent_mut(state);
                    backend_executor_nodeAgg::transition::set_transfn_arg(
                        aggstate, pertrans, 1, value, isnull,
                    );
                    eval_agg::ExecEvalPreOrderedDistinctSingle(aggstate, pertrans, estate)?
                } else {
                    let aggstate = agg_parent_mut(state);
                    eval_agg::ExecEvalPreOrderedDistinctMulti(aggstate, pertrans, estate)?
                };
                if distinct {
                    op += 1; // EEO_NEXT()
                } else {
                    op = jumpdistinct as usize; // EEO_JUMP(jumpdistinct)
                }
            }

            EEOP_AGG_ORDERED_TRANS_DATUM => {
                // C: ExecEvalAggOrderedTransDatum(state, op, econtext);
                eval_agg::ExecEvalAggOrderedTransDatum(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_AGG_ORDERED_TRANS_TUPLE => {
                // C: ExecEvalAggOrderedTransTuple(state, op, econtext);
                eval_agg::ExecEvalAggOrderedTransTuple(state, op, econtext, estate)?;
                op += 1;
            }

            EEOP_LAST => {
                // C: /* unreachable */ Assert(false); goto out_error;
                unreachable!("EEOP_LAST is not a real opcode (used only for array sizing)");
            }
        }
    }
}

/// Resolve the input slot a `*_SYSVAR` opcode reads, mirroring the C cached
/// `innerslot`/`outerslot`/`scanslot`/`oldslot`/`newslot` setup. The compiler
/// only emits a SYSVAR opcode where the corresponding econtext slot is present,
/// so a missing link is a caller bug (C dereferences the NULL slot).
fn sysvar_slot(
    opcode: ExprEvalOp,
    econtext: EcxtId,
    estate: &EStateData<'_>,
) -> types_nodes::SlotId {
    let ecxt = estate.ecxt(econtext);
    let (slot, name) = match opcode {
        ExprEvalOp::EEOP_INNER_SYSVAR => (ecxt.ecxt_innertuple, "ecxt_innertuple"),
        ExprEvalOp::EEOP_OUTER_SYSVAR => (ecxt.ecxt_outertuple, "ecxt_outertuple"),
        ExprEvalOp::EEOP_SCAN_SYSVAR => (ecxt.ecxt_scantuple, "ecxt_scantuple"),
        ExprEvalOp::EEOP_OLD_SYSVAR => (ecxt.ecxt_oldtuple, "ecxt_oldtuple"),
        ExprEvalOp::EEOP_NEW_SYSVAR => (ecxt.ecxt_newtuple, "ecxt_newtuple"),
        _ => unreachable!("sysvar_slot: not a SYSVAR opcode"),
    };
    slot.unwrap_or_else(|| panic!("EEOP_*_SYSVAR: econtext->{name} is NULL"))
}

/// Resolve the input slot a `*_FETCHSOME` / `*_VAR` / `ASSIGN_*_VAR` opcode
/// reads, mirroring the C cached `innerslot`/`outerslot`/`scanslot`/`oldslot`/
/// `newslot` setup. The compiler only emits the opcode where the matching
/// econtext slot is present, so a missing link is a caller bug (C dereferences
/// the NULL slot).
fn input_slot(
    opcode: ExprEvalOp,
    econtext: EcxtId,
    estate: &EStateData<'_>,
) -> types_nodes::SlotId {
    use ExprEvalOp::*;
    let ecxt = estate.ecxt(econtext);
    let (slot, name) = match opcode {
        EEOP_INNER_FETCHSOME | EEOP_INNER_VAR | EEOP_ASSIGN_INNER_VAR => {
            (ecxt.ecxt_innertuple, "ecxt_innertuple")
        }
        EEOP_OUTER_FETCHSOME | EEOP_OUTER_VAR | EEOP_ASSIGN_OUTER_VAR => {
            (ecxt.ecxt_outertuple, "ecxt_outertuple")
        }
        EEOP_SCAN_FETCHSOME | EEOP_SCAN_VAR | EEOP_ASSIGN_SCAN_VAR => {
            (ecxt.ecxt_scantuple, "ecxt_scantuple")
        }
        EEOP_OLD_FETCHSOME | EEOP_OLD_VAR | EEOP_ASSIGN_OLD_VAR => {
            (ecxt.ecxt_oldtuple, "ecxt_oldtuple")
        }
        EEOP_NEW_FETCHSOME | EEOP_NEW_VAR | EEOP_ASSIGN_NEW_VAR => {
            (ecxt.ecxt_newtuple, "ecxt_newtuple")
        }
        _ => unreachable!("input_slot: not a FETCHSOME/VAR/ASSIGN_VAR opcode"),
    };
    slot.unwrap_or_else(|| panic!("EEOP_*: econtext->{name} is NULL"))
}

/// `op->d.fetch.last_var` — the highest 1-based attribute a FETCHSOME must
/// deform.
fn fetch_last_var(state: &ExprState<'_>, op: usize) -> i32 {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::Fetch { last_var, .. } => *last_var,
        _ => unreachable!("EEOP_*_FETCHSOME: payload is not Fetch"),
    }
}

/// `op->d.var.attnum` — the 0-based attribute a VAR reads.
fn var_attnum(state: &ExprState<'_>, op: usize) -> i32 {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::Var { attnum, .. } => *attnum,
        _ => unreachable!("EEOP_*_VAR: payload is not Var"),
    }
}

/// `(op->d.assign_var.resultnum, op->d.assign_var.attnum)`.
fn assign_var_fields(state: &ExprState<'_>, op: usize) -> (i32, i32) {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::AssignVar { resultnum, attnum } => (*resultnum, *attnum),
        _ => unreachable!("EEOP_ASSIGN_*_VAR: payload is not AssignVar"),
    }
}

/// `op->d.assign_tmp.resultnum`.
fn assign_tmp_resultnum(state: &ExprState<'_>, op: usize) -> i32 {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::AssignTmp { resultnum } => *resultnum,
        _ => unreachable!("EEOP_ASSIGN_TMP[_MAKE_RO]: payload is not AssignTmp"),
    }
}

/// Write `(value, isnull)` into `state->resultslot->tts_values[resultnum]` /
/// `tts_isnull[resultnum]`. C asserts `resultnum < resultslot->...->natts`; the
/// canonical slot's `tts_values`/`tts_isnull` carry the projected columns.
fn write_resultslot<'mcx>(
    state: &ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    resultnum: i32,
    value: Datum<'mcx>,
    isnull: bool,
) {
    // C: resultslot = state->resultslot (a `TupleTableSlot *`). In the owned
    // model that pointer is a pool SlotId into the EState's tuple table; resolve
    // it to the canonical slot whose tts_values/tts_isnull arrays carry the
    // projected columns.
    let slot_id = state
        .resultslot
        .expect("EEOP_ASSIGN_*: ExprState has no resultslot");
    let resultslot = estate.slot_mut(slot_id);
    let idx = resultnum as usize;
    debug_assert!(
        idx < resultslot.tts_values.len(),
        "EEOP_ASSIGN_*: resultnum {resultnum} out of range"
    );
    resultslot.tts_values[idx] = value;
    resultslot.tts_isnull[idx] = isnull;
}

/// `op->d.boolexpr.anynull` — the shared is-null tracking cell for one
/// AND/OR expression's steps.
fn boolexpr_anynull(state: &ExprState<'_>, op: usize) -> ResultCellId {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::BoolExpr { anynull, .. } => *anynull,
        _ => unreachable!("EEOP_BOOL_*_STEP: payload is not BoolExpr"),
    }
}

/// `op->d.boolexpr.jumpdone` — the early-out target for an AND/OR step.
fn boolexpr_jumpdone(state: &ExprState<'_>, op: usize) -> usize {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::BoolExpr { jumpdone, .. } => *jumpdone as usize,
        _ => unreachable!("EEOP_BOOL_*_STEP: payload is not BoolExpr"),
    }
}

/// `op->d.qualexpr.jumpdone`.
fn qual_jumpdone(state: &ExprState<'_>, op: usize) -> usize {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::QualExpr { jumpdone } => *jumpdone as usize,
        _ => unreachable!("EEOP_QUAL: payload is not QualExpr"),
    }
}

/// `op->d.jump.jumpdone`.
fn jump_target(state: &ExprState<'_>, op: usize) -> usize {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::Jump { jumpdone } => *jumpdone as usize,
        _ => unreachable!("EEOP_JUMP*: payload is not Jump"),
    }
}

/// `op->d.casetest.value` — the test-value cell for CASE/domain TESTVAL.
fn casetest_cell(state: &ExprState<'_>, op: usize) -> ResultCellId {
    match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::CaseTest { value } => *value,
        _ => unreachable!("EEOP_CASE_TESTVAL/DOMAIN_TESTVAL: payload is not CaseTest"),
    }
}

/// `EEOP_BOOL_AND_STEP` body, shared with `EEOP_BOOL_AND_STEP_FIRST` (which
/// falls through after resetting `anynull`). Returns the next program counter
/// (`EEO_NEXT` => `op + 1`, or `EEO_JUMP(jumpdone)` on an early FALSE).
///
/// C: if (*op->resnull) *anynull = true;
///    else if (!DatumGetBool(*op->resvalue)) EEO_JUMP(jumpdone);  /* keep FALSE */
fn bool_and_step(state: &mut ExprState<'_>, op: usize, resv: ResultCellId) -> usize {
    let (value, isnull) = read_cell(state, resv);
    if isnull {
        let anynull = boolexpr_anynull(state, op);
        write_cell(state, anynull, Datum::from_bool(true), false);
        op + 1
    } else if !value.as_bool() {
        // result is already FALSE in place; bail out early
        boolexpr_jumpdone(state, op)
    } else {
        op + 1
    }
}

/// `EEOP_BOOL_OR_STEP` body, shared with `EEOP_BOOL_OR_STEP_FIRST`.
///
/// C: if (*op->resnull) *anynull = true;
///    else if (DatumGetBool(*op->resvalue)) EEO_JUMP(jumpdone);  /* keep TRUE */
fn bool_or_step(state: &mut ExprState<'_>, op: usize, resv: ResultCellId) -> usize {
    let (value, isnull) = read_cell(state, resv);
    if isnull {
        let anynull = boolexpr_anynull(state, op);
        write_cell(state, anynull, Datum::from_bool(true), false);
        op + 1
    } else if value.as_bool() {
        // result is already TRUE in place; bail out early
        boolexpr_jumpdone(state, op)
    } else {
        op + 1
    }
}
