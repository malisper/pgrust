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

use types_error::PgResult;
use types_nodes::execexpr::{ExprEvalStepData, ExprState};
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op)` — build an array
/// Datum from the per-element results of an ArrayExpr.
///
/// The individual array elements (or subarrays) have already been evaluated
/// into `op->d.arrayexpr.elemvalues[]`/`elemnulls[]`.
pub fn ExecEvalArrayExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Oid element_type = op->d.arrayexpr.elemtype;
    // int nelems = op->d.arrayexpr.nelems;
    let step = &state.steps.as_ref().expect("ExecEvalArrayExpr: steps not ready")[op];
    let resnull_id = step.resnull;
    let resvalue_id = step.resvalue;

    let (element_type, nelems, multidims) = match &step.d {
        ExprEvalStepData::ArrayExpr {
            elemtype,
            nelems,
            multidims,
            ..
        } => (*elemtype, *nelems, *multidims),
        _ => unreachable!("ExecEvalArrayExpr: step is not EEOP_ARRAYEXPR"),
    };

    // Set non-null as default.
    // *op->resnull = false;
    let mut cell = state.result_cells.get(resnull_id);
    cell.isnull = false;
    state.result_cells.set(resnull_id, cell);
    let _ = resvalue_id;

    if !multidims {
        // Elements are presumably of scalar type.
        //
        // Datum *dvalues = op->d.arrayexpr.elemvalues;
        // bool  *dnulls  = op->d.arrayexpr.elemnulls;
        //
        // setup for 1-D array of the given length:
        //   ndims = 1; dims[0] = nelems; lbs[0] = 1;
        //
        // result = construct_md_array(dvalues, dnulls, ndims, dims, lbs,
        //                             element_type,
        //                             op->d.arrayexpr.elemlength,
        //                             op->d.arrayexpr.elembyval,
        //                             op->d.arrayexpr.elemalign);
        //
        // construct_md_array fabricates the varlena and is owned by the
        // (unported) backend-utils-adt-arrayfuncs unit; the constructed array's
        // pointer would become *op->resvalue. Panic at the owner boundary.
        let _ = (element_type, nelems);
        panic!(
            "ExecEvalArrayExpr (1-D): construct_md_array is owned by the \
             unported backend-utils-adt-arrayfuncs unit (it palloc's the \
             ArrayType varlena whose pointer becomes the result Datum); not \
             reachable as a seam from this crate yet (mirror-PG-and-panic)"
        );
    } else {
        // Must be nested array expressions.
        //
        // The C loops over op->d.arrayexpr.elemvalues[]/elemnulls[], detoasts
        // each subarray (DatumGetArrayTypeP), checks ARR_ELEMTYPE against
        // element_type (error "cannot merge incompatible arrays", with
        // format_type_be names), gathers ARR_DATA_PTR / ARR_NULLBITMAP /
        // ARR_SIZE / ARR_DIMS / ARR_LBOUND of each, validates matching
        // dimensions, then palloc0's the result, SET_VARSIZE's it, copies the
        // sub-data with array_bitmap_copy, and stores PointerGetDatum(result).
        // The empty/zero-D special case returns
        // PointerGetDatum(construct_empty_array(element_type)).
        //
        // Every operation in that body reads/writes the ArrayType varlena
        // through the array.h ARR_* macros / construct_empty_array, all owned
        // by the (unported) backend-utils-adt-arrayfuncs unit, and mints the
        // result Datum from a freshly palloc'd varlena. Panic at the owner
        // boundary.
        panic!(
            "ExecEvalArrayExpr (multi-D): the nested-subarray byte surgery over \
             the array.h ARR_* macros (DatumGetArrayTypeP, ARR_ELEMTYPE, \
             ARR_NDIM/ARR_DIMS/ARR_LBOUND, ARR_DATA_PTR, ARR_NULLBITMAP, \
             ArrayGetNItems, array_bitmap_copy) and construct_empty_array are \
             owned by the unported backend-utils-adt-arrayfuncs unit and mint \
             the result varlena's pointer Datum; not reachable as a seam from \
             this crate yet (mirror-PG-and-panic)"
        );
    }
}

/// `ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce each element of an array to a new type.
///
/// Source array is in the step's result variable.
pub fn ExecEvalArrayCoerce<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    _estate: &mut EStateData<'mcx>,
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
    let resnull = state.result_cells.get(resnull_id).isnull;
    if resnull {
        return Ok(());
    }

    // arraydatum = *op->resvalue;
    let arraydatum = state.result_cells.get(resvalue_id).value;

    if !has_elemexpr {
        // If it's binary-compatible, modify the element type in the array
        // header, but otherwise leave the array as we received it.
        //
        //   ArrayType *array = DatumGetArrayTypePCopy(arraydatum);
        //   ARR_ELEMTYPE(array) = op->d.arraycoerce.resultelemtype;
        //   *op->resvalue = PointerGetDatum(array);
        //
        // DatumGetArrayTypePCopy (detoast + copy) and the ARR_ELEMTYPE header
        // write over the varlena are owned by the unported
        // backend-utils-adt-arrayfuncs unit, and the copied varlena's pointer
        // becomes the new result Datum. Panic at the owner boundary.
        let _ = (arraydatum, resultelemtype, resvalue_id);
        panic!(
            "ExecEvalArrayCoerce (binary-compatible): DatumGetArrayTypePCopy + \
             the ARR_ELEMTYPE header rewrite are owned by the unported \
             backend-utils-adt-arrayfuncs unit (copy the varlena, then mint its \
             pointer as the result Datum); not reachable as a seam from this \
             crate yet (mirror-PG-and-panic)"
        );
    } else {
        // Use array_map to apply the sub-expression to each array element.
        //
        //   *op->resvalue = array_map(arraydatum,
        //                             op->d.arraycoerce.elemexprstate,
        //                             econtext,
        //                             op->d.arraycoerce.resultelemtype,
        //                             op->d.arraycoerce.amstate);
        //
        // array_map is owned by the unported backend-utils-adt-arrayfuncs unit
        // (its own port is not yet complete — it needs exactly this
        // ExprState/ExprContext element-transform boundary) and mints a fresh
        // result varlena Datum. Panic at the owner boundary.
        let _ = (arraydatum, resultelemtype, econtext, resvalue_id);
        panic!(
            "ExecEvalArrayCoerce (array_map): array_map is owned by the \
             unported backend-utils-adt-arrayfuncs unit (it applies the \
             per-element ExprState transform via this ExprContext and mints the \
             coerced result varlena Datum); not reachable as a seam from this \
             crate yet (mirror-PG-and-panic)"
        );
    }
}
