//! Array-opcode evaluators (`execExprInterp.c`): ArrayExpr construction and
//! ArrayCoerce element-wise coercion.
//!
//! Owned-model conventions (see [`crate::dispatch`]): a step is addressed by
//! its index `op` into `state.steps`; the handler reads the F0
//! [`ExprEvalStepData`] payload the compiler wrote at `state.steps[op].d` and
//! reads/writes its result through the step's `resvalue`/`resnull`
//! [`ResultCellId`]s into `state.result_cells`. The C `*op->resvalue` /
//! `*op->resnull` writes therefore become a `result_cells.set(id, ..)`, and
//! reads of the same cell become `result_cells.get(id)`.
//!
//! Owner boundary (mirror-PG-and-panic). The actual array *fabrication* —
//! `construct_md_array` / `construct_empty_array`, the nested-subarray byte
//! surgery over the `array.h` `ARR_*` macros (`DatumGetArrayTypeP`,
//! `ARR_ELEMTYPE`, `ARR_NDIM`, `ARR_DIMS`, `ARR_LBOUND`, `ARR_DATA_PTR`,
//! `ARR_NULLBITMAP`, `ArrayGetNItems`, `array_bitmap_copy`, `SET_VARSIZE`,
//! …), `DatumGetArrayTypePCopy`, and `array_map` — is owned by
//! `backend-utils-adt-arrayfuncs` (still being ported; `array_map` itself is
//! not yet complete) and is not reachable as a seam from this crate. A
//! constructed array is a palloc'd varlena whose pointer becomes the result
//! `Datum`; the owned model has no global address space, so the varlena owner
//! must mint that `Datum`. Until the arrayfuncs boundary lands, the fabrication
//! steps panic loudly naming the owner, exactly as C would have called into it.
//! The interpreter's own control flow (payload read, the multidims decision,
//! the resnull/resvalue default, and the NULL-array short-circuit) is rendered
//! faithfully here.

use crate::dispatch;
use arrayfuncs_seams as arrayfuncs_seam;
use lsyscache_seams as lsyscache_seam;
use types_error::PgResult;
use nodes::execexpr::{
    ExprEvalStepData, ExprState, ResultCell, ResultCellId, STATE_RESULT_CELL,
};
use nodes::execnodes::EcxtId;
use nodes::EStateData;
use types_tuple::heaptuple::Datum;

/// `ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op)` — build an array
/// Datum from the per-element results of an ArrayExpr.
///
/// The individual array elements (or subarrays) have already been evaluated
/// into `op->d.arrayexpr.elemvalues[]`/`elemnulls[]`.
pub fn ExecEvalArrayExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Oid element_type = op->d.arrayexpr.elemtype;
    // int nelems = op->d.arrayexpr.nelems;
    let step = &state.steps.as_ref().expect("ExecEvalArrayExpr: steps not ready")[op];
    let resnull_id = step.resnull;
    let resvalue_id = step.resvalue;

    let (element_type, nelems, elemlength, elembyval, elemalign, multidims) = match &step.d {
        ExprEvalStepData::ArrayExpr {
            elemtype,
            nelems,
            elemlength,
            elembyval,
            elemalign,
            multidims,
            ..
        } => (
            *elemtype,
            *nelems,
            *elemlength,
            *elembyval,
            *elemalign,
            *multidims,
        ),
        _ => unreachable!("ExecEvalArrayExpr: step is not EEOP_ARRAYEXPR"),
    };

    // The element sub-expressions evaluated their results into the step's
    // per-element result cells (op->d.arrayexpr.elem_cells, the owned-model
    // stand-in for C's &op->d.arrayexpr.elemvalues[i]/elemnulls[i] aliasing).
    // Collect those cell ids, then gather them into elemvalues/elemnulls below
    // (the C code reads them straight from the scratch arrays the recursion
    // wrote). Clone the ids out so the immutable borrow of `state.steps` ends
    // before we read/write the result cells.
    let elem_cells: Vec<ResultCellId> = match &step.d {
        ExprEvalStepData::ArrayExpr { elem_cells, .. } => elem_cells
            .as_ref()
            .map(|v| v.as_slice().to_vec())
            .unwrap_or_default(),
        _ => unreachable!(),
    };

    let mut elemvalues: Vec<Datum<'mcx>> = Vec::with_capacity(nelems as usize);
    let mut elemnulls: Vec<bool> = Vec::with_capacity(nelems as usize);
    for &cell in &elem_cells {
        let c = state.result_cells.get(cell);
        elemvalues.push(c.value.clone());
        elemnulls.push(c.isnull);
    }

    // Set non-null as default.
    // *op->resnull = false;
    // STATE_RESULT_CELL aliases state.resnull (the C &state->resnull default
    // target); any other id is a per-step arena cell.
    if resnull_id == STATE_RESULT_CELL {
        state.resnull = false;
    } else {
        let mut cell = state.result_cells.get(resnull_id);
        cell.isnull = false;
        state.result_cells.set(resnull_id, cell);
    }

    // C allocates the result varlena in CurrentMemoryContext (the per-tuple
    // expression context during ExecInterpExpr); the owned model resolves that
    // off the EState's per-query arena, which lives for 'mcx (behavior-preserving
    // — a longer-lived allocation, exactly as eval_subscript does).
    let mcx = estate.ecxt(econtext).ecxt_per_query_memory;

    // result = construct_md_array(...) for the scalar 1-D case, or the
    // nested-subarray fabrication for the multi-D case. Owned by arrayfuncs;
    // reached through its value-lane seam. The returned varlena image becomes
    // *op->resvalue as a by-reference Datum (PointerGetDatum(result)).
    let image = arrayfuncs_seam::construct_array_expr::call(
        mcx,
        &elemvalues,
        &elemnulls,
        element_type,
        elemlength,
        elembyval,
        elemalign,
        multidims,
    )?;

    // *op->resvalue = PointerGetDatum(result): the 6-arm result cell carries the
    // array varlena image inline as a ByRef value. STATE_RESULT_CELL writes
    // through state.resvalue/resnull (the C &state->resvalue default target),
    // which EEOP_DONE_RETURN reads back; any other id is a per-step arena cell.
    let value = Datum::ByRef(image);
    if resvalue_id == STATE_RESULT_CELL {
        state.resvalue = value;
        state.resnull = false;
    } else {
        state.result_cells.set(
            resvalue_id,
            ResultCell {
                value,
                isnull: false,
            },
        );
    }

    Ok(())
}

/// `ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce each element of an array to a new type.
///
/// Source array is in the step's result variable.
pub fn ExecEvalArrayCoerce<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let step = &state.steps.as_ref().expect("ExecEvalArrayCoerce: steps not ready")[op];
    let resnull_id = step.resnull;
    let resvalue_id = step.resvalue;

    let (has_elemexpr, resultelemtype) = match &step.d {
        ExprEvalStepData::ArrayCoerce {
            elemexprstate,
            resultelemtype,
            ..
        } => (elemexprstate.is_some(), *resultelemtype),
        _ => unreachable!("ExecEvalArrayCoerce: step is not EEOP_ARRAYCOERCE"),
    };

    // NULL array -> NULL result.
    // if (*op->resnull) return;
    //
    // `op->resvalue`/`op->resnull` are pointers that, in C, alias the step's
    // result variable — which is `&state->resvalue`/`&state->resnull` (the
    // STATE_RESULT_CELL sentinel) when the array sub-expression wrote into the
    // ExprState's own result slot. That is exactly what happens for an
    // ArrayCoerce arg compiled with target `resv` (e.g. the array operand of a
    // ScalarArrayOpExpr): the preceding EEOP_CONST/EEOP_ARRAYEXPR step writes
    // the source array into `state.resvalue`. Reading the raw `result_cells`
    // slot would miss that aliasing (slot 0 is the sentinel, never a populated
    // arena cell — it would read a zero `ByVal` default), so resolve both the
    // null flag and the source array through the sentinel-aware accessor.
    let (arraydatum, resnull) = crate::interp_loop::read_cell(state, resvalue_id);
    if resnull {
        return Ok(());
    }

    // The per-query memory context the coerced result varlena is allocated in.
    let mcx = estate.ecxt(econtext).ecxt_per_query_memory;

    if !has_elemexpr {
        // If it's binary-compatible, modify the element type in the array
        // header, but otherwise leave the array as we received it.
        //   ArrayType *array = DatumGetArrayTypePCopy(arraydatum);
        //   ARR_ELEMTYPE(array) = op->d.arraycoerce.resultelemtype;
        //   *op->resvalue = PointerGetDatum(array);
        let image =
            arrayfuncs_seam::array_coerce_relabel::call(mcx, arraydatum, resultelemtype)?;
        write_result(state, resvalue_id, resnull_id, Datum::ByRef(image));
        return Ok(());
    }

    // Use array_map to apply the sub-expression to each array element.
    //   *op->resvalue = array_map(arraydatum, op->d.arraycoerce.elemexprstate,
    //                             econtext, op->d.arraycoerce.resultelemtype,
    //                             op->d.arraycoerce.amstate);
    //
    // array_map's body is split at the per-element ExecEvalExpr boundary: the
    // arrayfuncs owner deconstructs the source (front half) and assembles the
    // result reusing the source dims (back half); the loop in between writes
    // each source element into the element sub-expression's innermost_caseval /
    // innermost_casenull and runs ExecEvalExpr, exactly as array_map does.

    // retType: the result element type's storage attrs (array_map's ret_extra).
    let ret = lsyscache_seam::get_typlenbyvalalign::call(resultelemtype)?;

    // Front half: detoast + deconstruct the whole source array (the input
    // element type's storage attrs are looked up inside the owner).
    let src = arrayfuncs_seam::array_map_deconstruct::call(mcx, arraydatum)?;
    let nitems = src.elems.len();

    // Take the element sub-expression state out of the step so it can be run
    // mutably while `state` is otherwise borrowed; restore it afterwards. The
    // C `op->d.arraycoerce.elemexprstate` is a persistent ExprState* reused
    // across rows — moving it out and back preserves that identity.
    let mut elemstate = match &mut state.steps.as_mut().unwrap()[op].d {
        ExprEvalStepData::ArrayCoerce { elemexprstate, .. } => elemexprstate
            .take()
            .expect("ExecEvalArrayCoerce: elemexprstate present"),
        _ => unreachable!(),
    };

    // The cell the sub-expression's CaseTestExpr reads the source element from
    // (exprstate->innermost_caseval / innermost_casenull in C).
    let source_cell = elemstate
        .innermost_caseval
        .expect("ExecEvalArrayCoerce: elemstate.innermost_caseval set");

    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(nitems);
    let mut nulls: Vec<bool> = Vec::with_capacity(nitems);
    let mut run_err: PgResult<()> = Ok(());
    for (elem, elemnull) in src.elems.iter() {
        // *transform_source = element; *transform_source_isnull = isnull;
        elemstate.result_cells.set(
            source_cell,
            ResultCell {
                value: elem.clone(),
                isnull: *elemnull,
            },
        );
        // values[i] = ExecEvalExpr(exprstate, econtext, &nulls[i]);
        match dispatch::ExecInterpExprStillValid(&mut elemstate, econtext, estate) {
            Ok((v, n)) => {
                values.push(v);
                nulls.push(n);
            }
            Err(e) => {
                run_err = Err(e);
                break;
            }
        }
    }

    // Restore the persistent element sub-expression state into the step.
    match &mut state.steps.as_mut().unwrap()[op].d {
        ExprEvalStepData::ArrayCoerce { elemexprstate, .. } => {
            *elemexprstate = Some(elemstate);
        }
        _ => unreachable!(),
    }
    run_err?;

    // Back half: assemble the coerced result array, reusing the source dims.
    let image = arrayfuncs_seam::array_map_build::call(
        mcx,
        src.ndim,
        &src.dims,
        &src.lbs,
        &values,
        &nulls,
        resultelemtype,
        ret.typlen,
        ret.typbyval,
        ret.typalign as core::ffi::c_char,
    )?;
    write_result(state, resvalue_id, resnull_id, Datum::ByRef(image));
    Ok(())
}

/// Write a non-null by-reference result into the step's resvalue/resnull cells
/// (`*op->resvalue = value; *op->resnull = false;`). [`STATE_RESULT_CELL`]
/// aliases `state.resvalue`/`state.resnull` (the C `&state->resvalue` default
/// target); any other id is a per-step arena cell.
fn write_result<'mcx>(
    state: &mut ExprState<'mcx>,
    resvalue_id: ResultCellId,
    resnull_id: ResultCellId,
    value: Datum<'mcx>,
) {
    if resnull_id == STATE_RESULT_CELL {
        state.resnull = false;
    } else {
        let mut cell = state.result_cells.get(resnull_id);
        cell.isnull = false;
        state.result_cells.set(resnull_id, cell);
    }
    if resvalue_id == STATE_RESULT_CELL {
        state.resvalue = value;
        state.resnull = false;
    } else {
        state.result_cells.set(
            resvalue_id,
            ResultCell {
                value,
                isnull: false,
            },
        );
    }
}
