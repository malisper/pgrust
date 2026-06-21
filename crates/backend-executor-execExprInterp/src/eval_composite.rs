//! Row / composite-value opcode evaluators (`execExprInterp.c`): RowExpr,
//! NullTest on rows, FieldSelect, FieldStore deform/form, ConvertRowtype,
//! WholeRowVar, and GREATEST/LEAST (MinMax).
//!
//! Owned-model conventions (shared with the other family modules): a step is
//! addressed by its index `op` into `state.steps`; the body reads the
//! instruction via `state.steps[op]` and writes its `(value, isnull)` result
//! through that step's [`ResultCellId`] cell (the C `op->resvalue`/`op->resnull`
//! aliasing pointers; see [`ResultCellArena`]). `econtext` is an [`EcxtId`] into
//! the EState expr-context pool. Evaluation can `ereport(ERROR)`, so the bodies
//! return [`PgResult`].
//!
//! These opcodes read the real F0 inline payloads the `execExpr.c` compiler
//! writes (`nulltest_row`, `row`, `minmax`, `fieldselect`, `fieldstore`,
//! `convert_rowtype`, `wholerow`), modeled field-for-field in
//! [`ExprEvalStepData`]. The faithful control flow that is expressible against
//! the modeled vocabulary — the result load/store, the NULL/zero-field
//! shortcuts, the slot selection and `wholerow.first`/`slow` flag handling,
//! reading every payload field — is ported here.
//!
//! The *composite-`Datum` bridge* — a composite value is a pass-by-reference
//! (varlena) `Datum` carried by [`Datum::Composite`] (a `FormedTuple` image) or
//! as a flat `Datum::ByRef` HeapTupleHeader image — is now ported. The heap-tuple
//! form/deform/getattr (`heap_form_tuple`, `heap_deform_tuple`, `heap_getattr`),
//! the rowtype cache (`lookup_rowtype_tupdesc` / `lookup_rowtype_tupdesc_domain`),
//! the tuple-conversion map (`convert_tuples_by_name` / `execute_attr_map_tuple`
//! / `heap_copy_tuple_as_datum`), the TOAST flattener
//! (`toast_build_flattened_tuple`) and `BlessTupleDesc` are all wired here, so
//! `ExecEvalRow`, `ExecEvalFieldSelect`, `ExecEvalWholeRowVar`,
//! `ExecEvalConvertRowtype` and `ExecEvalFieldStoreDeForm`/`Form` run
//! end-to-end. The FieldStore pair deforms the input composite into the step's
//! per-column `ResultCellId` cells (`heap_deform_tuple`), the per-field newval
//! sub-expressions overwrite their target cell, then FORM re-forms the tuple
//! (`heap_form_tuple` + `HeapTupleGetDatum`) into a new composite `Datum`.

// The bare-word newtype: the scalar form the composite eval helpers operate on.
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ExprState.resvalue` / `ResultCell.value` carry, and what the
// composite helpers operate on directly.
use backend_utils_fmgr_fmgr_seams::function_call_invoke_datum;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;
use types_nodes::execexpr::{
    ExprEvalStepData, ExprState, MinMaxOp, ResultCell, ResultCellId, EEO_FLAG_NEW_IS_NULL,
    EEO_FLAG_OLD_IS_NULL, STATE_RESULT_CELL,
};
use types_nodes::execnodes::EcxtId;
use types_nodes::primnodes::VarReturningType;
use types_nodes::{EStateData, SlotId};

/// `INNER_VAR` / `OUTER_VAR` (primnodes.h) — the special `varno` sentinels for a
/// Var referencing the inner/outer side of a join. Any other `varno` selects the
/// scan slot (INDEX_VAR is handled by this default case too).
const INNER_VAR: i32 = -1;
const OUTER_VAR: i32 = -2;

/// Read a step's current `(value, isnull)` result (the C `*op->resvalue` /
/// `*op->resnull`). [`STATE_RESULT_CELL`] aliases the owning `ExprState`'s own
/// `resvalue`/`resnull` (the C `&state->resvalue`/`&state->resnull` default
/// target); any other id reads the per-step cell from the arena.
fn load_result<'mcx>(state: &ExprState<'mcx>, op: usize) -> (Datum<'mcx>, bool) {
    let cell = state.steps.as_ref().expect("eval_composite: steps not ready")[op].resvalue;
    if cell == STATE_RESULT_CELL {
        (state.resvalue.clone(), state.resnull)
    } else {
        let c = state.result_cells.get(cell);
        (c.value, c.isnull)
    }
}

/// Write a step's `(value, isnull)` result (the C `*op->resvalue = value;
/// *op->resnull = isnull;`). [`STATE_RESULT_CELL`] writes through the owning
/// `ExprState`'s own `resvalue`/`resnull`; any other id writes the arena cell.
fn store_result<'mcx>(state: &mut ExprState<'mcx>, op: usize, value: Datum<'mcx>, isnull: bool) {
    let cell: ResultCellId = state.steps.as_ref().expect("eval_composite: steps not ready")[op].resvalue;
    if cell == STATE_RESULT_CELL {
        state.resvalue = value;
        state.resnull = isnull;
    } else {
        state
            .result_cells
            .set(cell, ResultCell { value, isnull });
    }
}

/// `DatumGetHeapTupleHeader(value)` (htup_details.h):
/// `((HeapTupleHeader) PG_DETOAST_DATUM(value))` — decode a composite/record
/// `Datum` into a [`FormedTuple`]. A composite value stored into a table column
/// can itself be TOASTed (stored out-of-line / compressed) when the whole row
/// image exceeds the toast threshold; like every by-reference `Datum` consumer,
/// the composite-Datum bridge must `PG_DETOAST_DATUM` the value before
/// reinterpreting its bytes as a `HeapTupleHeader`. A `Datum::Composite` is
/// already a live, never-toasted tuple, so it re-homes directly. A
/// `Datum::ByRef` flat image is detoasted iff `VARATT_IS_EXTENDED` (a 1-byte /
/// external / compressed header) before decoding the flat header image;
/// a plain 4-byte uncompressed image (what `to_datum_image` mints) is decoded
/// verbatim, matching C's `PG_DETOAST_DATUM` fall-through.
fn datum_get_heap_tuple_header<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    value: &Datum<'_>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>> {
    match value {
        Datum::Composite(t) => t.clone_in(mcx),
        Datum::ByRef(image) => {
            let bytes = image.as_slice();
            // VARATT_IS_EXTENDED(PTR): NOT a plain 4-byte uncompressed datum
            // (low two bits of the leading header byte != 0b00). Such a value is
            // an external TOAST pointer, a compressed, or a short-header varlena
            // and must be fetched/decompressed before the flat header decode.
            let extended = bytes.first().is_some_and(|b| (b & 0x03) != 0x00);
            if extended {
                let flat = backend_access_common_detoast_seams::detoast_attr::call(mcx, bytes)?;
                types_tuple::backend_access_common_heaptuple::FormedTuple::from_datum_image(
                    mcx,
                    flat.as_slice(),
                )
            } else {
                types_tuple::backend_access_common_heaptuple::FormedTuple::from_datum_image(
                    mcx, bytes,
                )
            }
        }
        other => unreachable!(
            "DatumGetHeapTupleHeader: composite input Datum is neither Composite nor ByRef: {other:?}"
        ),
    }
}

/// `ExecEvalRowNull(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — `IS NULL` test on a row value.
pub fn ExecEvalRowNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecEvalRowNullInt(state, op, econtext, true);
    ExecEvalRowNullInt(state, op, econtext, true, estate)
}

/// `ExecEvalRowNotNull(...)` — `IS NOT NULL` test on a row value.
pub fn ExecEvalRowNotNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecEvalRowNullInt(state, op, econtext, false);
    ExecEvalRowNullInt(state, op, econtext, false, estate)
}

/// `ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext, bool checkisnull)` — shared body for the row
/// null/not-null tests.
///
/// SQL-standard semantics: "R IS NULL" is true iff every (non-dropped) field is
/// the null value; "R IS NOT NULL" is true iff no field is null; a NULL row
/// variable is treated like a NULL scalar (result is `checkisnull`); zero-field
/// rows vacuously satisfy both predicates.
pub fn ExecEvalRowNullInt<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _econtext: EcxtId,
    checkisnull: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // Datum value = *op->resvalue;
    // bool  isnull = *op->resnull;
    let (value, isnull) = load_result(state, op);

    // *op->resnull = false;
    // /* NULL row variables are treated just as NULL scalar columns */
    // if (isnull) { *op->resvalue = BoolGetDatum(checkisnull); return; }
    if isnull {
        store_result(state, op, Datum::from_bool(checkisnull), false);
        return Ok(());
    }

    // Confirm this is the modeled NULLTEST_ROW payload (the rowcache the
    // get_cached_rowtype lookup below would consult).
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    match &steps[op].d {
        ExprEvalStepData::NullTestRow { .. } => {}
        other => unreachable!("ExecEvalRowNullInt: step.d is not NullTestRow: {other:?}"),
    }

    // tuple = DatumGetHeapTupleHeader(value);
    // tupType = HeapTupleHeaderGetTypeId(tuple); tupTypmod = HeapTupleHeaderGetTypMod(tuple);
    // tupDesc = get_cached_rowtype(tupType, tupTypmod, &op->d.nulltest_row.rowcache, NULL);
    //
    // Decode the composite Datum (Composite or a flat by-ref HeapTupleHeader
    // image) into a FormedTuple, then look up its rowtype descriptor (the
    // internally-cached typcache lookup stands in for the C void* rowcache).
    let tuple = datum_get_heap_tuple_header(mcx, &value)?;
    let header = tuple
        .tuple
        .t_data
        .as_ref()
        .expect("ExecEvalRowNullInt: composite Datum has no header");
    let tup_type = types_tuple::heaptuple::HeapTupleHeaderGetTypeId(header);
    let tup_typmod = types_tuple::heaptuple::HeapTupleHeaderGetTypMod(header);
    let tup_desc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
        mcx, tup_type, tup_typmod,
    )?;

    // for (att = 1; att <= tupDesc->natts; att++) {
    //     if (TupleDescCompactAttr(tupDesc, att - 1)->attisdropped) continue;
    //     if (heap_attisnull(&tmptup, att, tupDesc)) {
    //         if (!checkisnull) { *op->resvalue = BoolGetDatum(false); return; }
    //     } else {
    //         if (checkisnull)  { *op->resvalue = BoolGetDatum(false); return; }
    //     }
    // }
    // *op->resvalue = BoolGetDatum(true);
    for att in 1..=tup_desc.natts {
        if tup_desc.compact_attrs[(att - 1) as usize].attisdropped {
            continue;
        }
        let attisnull = backend_access_common_heaptuple::heap_attisnull(
            &tuple.tuple,
            att,
            Some(&tup_desc),
        );
        if attisnull {
            if !checkisnull {
                store_result(state, op, Datum::from_bool(false), false);
                return Ok(());
            }
        } else if checkisnull {
            store_result(state, op, Datum::from_bool(false), false);
            return Ok(());
        }
    }

    store_result(state, op, Datum::from_bool(true), false);
    Ok(())
}

/// `ExecEvalRow(ExprState *state, ExprEvalStep *op)` — build a composite Datum
/// from the per-column results of a RowExpr.
///
/// The individual columns have already been evaluated into
/// `op->d.row.elemvalues[]`/`elemnulls[]`; this step forms the tuple and stores
/// its composite datum.
pub fn ExecEvalRow<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tuple = heap_form_tuple(op->d.row.tupdesc,
    //                         op->d.row.elemvalues, op->d.row.elemnulls);
    // *op->resvalue = HeapTupleGetDatum(tuple);
    // *op->resnull = false;
    //
    // The individual columns were evaluated into per-field result cells
    // (op->d.row.elem_cells, the owned stand-in for C's
    // op->d.row.elemvalues[]/elemnulls[] workspace arrays); gather them into the
    // values/nulls the forming function expects. A field whose cell is the
    // STATE_RESULT_CELL sentinel (a dropped or extra named-type column) reads as
    // NULL (C memset elemnulls = true for those slots).
    let mcx = estate.es_query_cxt;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (tupdesc, elem_cells) = match &steps[op].d {
        ExprEvalStepData::Row {
            tupdesc,
            elem_cells,
            ..
        } => {
            let tupdesc = tupdesc
                .as_ref()
                .expect("ExecEvalRow: op->d.row.tupdesc not built");
            let elem_cells = elem_cells
                .as_ref()
                .expect("ExecEvalRow: op->d.row.elem_cells missing");
            (tupdesc.clone_in(mcx)?, elem_cells.clone())
        }
        other => unreachable!("ExecEvalRow: step.d is not Row: {other:?}"),
    };

    let natts = tupdesc.natts as usize;
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(natts);
    for i in 0..natts {
        let cell = elem_cells[i];
        if cell == STATE_RESULT_CELL {
            // Dropped / extra column: always NULL.
            values.push(Datum::null());
            nulls.push(true);
        } else {
            let c = state.result_cells.get(cell);
            values.push(c.value.clone());
            nulls.push(c.isnull);
        }
    }

    // tuple = heap_form_tuple(tupdesc, values, nulls);
    let tuple = backend_access_common_heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| types_error::PgError::error(format!("ExecEvalRow: {e:?}")))?;

    // *op->resvalue = HeapTupleGetDatum(tuple);  *op->resnull = false;
    //
    // HeapTupleGetDatum -> HeapTupleHeaderGetDatum (execTuples.c:2413): if the
    // formed tuple carries any external TOAST pointers, inline them now so the
    // composite Datum does not reference foreign toast (which would dangle once
    // that relation is deleted/dropped). The HASEXTERNAL tuple takes the flatten
    // path; the common case keeps the formed tuple by value.
    let has_external = tuple
        .tuple
        .t_data
        .as_ref()
        .map(|h| types_tuple::heaptuple::HeapTupleHeaderHasExternal(h))
        .unwrap_or(false);
    let datum = if has_external {
        let (flattened, _datum) =
            backend_access_heap_heaptoast::heap_tuple_header_get_datum(mcx, tuple)?;
        Datum::Composite(flattened)
    } else {
        Datum::Composite(tuple)
    };
    store_result(state, op, datum, false);
    Ok(())
}

/// `ExecEvalMinMax(ExprState *state, ExprEvalStep *op)` — GREATEST / LEAST (note
/// this is *not* MIN()/MAX()).
///
/// All operands have already been evaluated into `op->d.minmax.values[]` /
/// `nulls[]`. Default result is null; the first non-null operand is adopted,
/// then each subsequent non-null operand replaces the running result when the
/// comparison function says it is more extreme in the requested direction
/// (`IS_LEAST` keeps the smaller, `IS_GREATEST` the larger). A null comparison
/// result is ignored.
pub fn ExecEvalMinMax<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Datum *values = op->d.minmax.values;
    // bool  *nulls  = op->d.minmax.nulls;
    // FunctionCallInfo fcinfo = op->d.minmax.fcinfo_data;
    // MinMaxOp operator = op->d.minmax.op;
    // Assert(fcinfo->args[0].isnull == false);
    // Assert(fcinfo->args[1].isnull == false);
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (arg_cells, nelems, operator, fn_oid, collation) = match &steps[op].d {
        ExprEvalStepData::MinMax {
            arg_cells,
            nelems,
            op: minmax_op,
            finfo,
            fcinfo_data,
            ..
        } => {
            let arg_cells = arg_cells
                .as_ref()
                .expect("ExecEvalMinMax: op->d.minmax.arg_cells missing")
                .clone();
            let finfo = finfo
                .as_ref()
                .expect("ExecEvalMinMax: op->d.minmax.finfo not resolved");
            let fcinfo = fcinfo_data
                .as_ref()
                .expect("ExecEvalMinMax: op->d.minmax.fcinfo_data missing");
            (
                arg_cells,
                *nelems,
                *minmax_op,
                finfo.fn_oid,
                fcinfo.fncollation,
            )
        }
        other => unreachable!("ExecEvalMinMax: step.d is not MinMax: {other:?}"),
    };

    // Gather the per-argument result cells into the `values`/`nulls` workspace
    // (the C `ExecInitExprRec` wrote `values[off]` directly; the owned model
    // recorded each arg's result cell in `arg_cells` and gathers them here,
    // immediately before the comparison loop).
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(arg_cells.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(arg_cells.len());
    for &cell in arg_cells.iter() {
        let c = state.result_cells.get(cell);
        values.push(c.value.clone());
        nulls.push(c.isnull);
    }

    // /* default to null result */
    // *op->resnull = true;
    store_result(state, op, Datum::null(), true);

    // for (int off = 0; off < op->d.minmax.nelems; off++)
    for off in 0..nelems as usize {
        // /* ignore NULL inputs */
        // if (nulls[off]) continue;
        if nulls[off] {
            continue;
        }

        let (cur_value, cur_isnull) = load_result(state, op);
        if cur_isnull {
            // /* first nonnull input, adopt value */
            // *op->resvalue = values[off]; *op->resnull = false;
            store_result(state, op, values[off].clone(), false);
        } else {
            // /* apply comparison function */
            // fcinfo->args[0].value = *op->resvalue;
            // fcinfo->args[1].value = values[off];
            // fcinfo->isnull = false;
            // cmpresult = DatumGetInt32(FunctionCallInvoke(fcinfo));
            // if (fcinfo->isnull) continue;   /* probably should not happen */
            // if (cmpresult > 0 && operator == IS_LEAST)    *op->resvalue = values[off];
            // else if (cmpresult < 0 && operator == IS_GREATEST) *op->resvalue = values[off];
            //
            // The resolved FmgrInfo cannot cross the seam, so dispatch by
            // fn_oid through function_call_invoke_datum (#296), the canonical
            // (by-reference-capable) call-frame lane: the BTORDER_PROC compare
            // function operands may be by-reference values (text/numeric/etc.),
            // so they must cross the boundary as `Datum` (by-ref bytes preserved)
            // rather than being forced through `as_usize()`, which panics on a
            // by-reference arm. The two compared values are gathered into the
            // args[0]/args[1] frame; both are non-null here.
            let mcx = estate.es_query_cxt;
            let args = [cur_value.clone(), values[off].clone()];
            let (result, isnull) =
                function_call_invoke_datum::call(mcx, fn_oid, collation, &args, &[], None)?;
            if isnull {
                // probably should not happen
                continue;
            }
            // The comparison yields a by-value int32 cmpresult.
            let cmpresult = types_datum::Datum::from_usize(result.as_usize()).as_i32();
            if (cmpresult > 0 && operator == MinMaxOp::IS_LEAST)
                || (cmpresult < 0 && operator == MinMaxOp::IS_GREATEST)
            {
                store_result(state, op, values[off].clone(), false);
            }
        }
    }

    Ok(())
}

/// `ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — extract one field from a composite value.
///
/// A NULL record yields a NULL result. Expanded records take a fast path
/// (`expanded_record_get_field`); otherwise the composite datum is decoded, its
/// rowtype `TupleDesc` cached, the field's `pg_attribute` located (dropped
/// columns force NULL, a type mismatch ereports), and `heap_getattr` extracts
/// the field.
pub fn ExecEvalFieldSelect<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // AttrNumber fieldnum    = op->d.fieldselect.fieldnum;
    // Oid        resulttype  = op->d.fieldselect.resulttype;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (fieldnum, resulttype) = match &steps[op].d {
        ExprEvalStepData::FieldSelect {
            fieldnum,
            resulttype,
            ..
        } => (*fieldnum, *resulttype),
        other => unreachable!("ExecEvalFieldSelect: step.d is not FieldSelect: {other:?}"),
    };

    // /* NULL record -> NULL result */
    // if (*op->resnull) return;
    let (tup_datum, isnull) = load_result(state, op);
    if isnull {
        return Ok(());
    }

    // tupDatum = *op->resvalue;
    //
    // The expanded-record fast path (VARATT_IS_EXTERNAL_EXPANDED) is not produced
    // yet (the Expanded Datum arm is wave 2); the flat path decodes the composite
    // Datum to its HeapTupleHeader:
    //
    //   tuple = DatumGetHeapTupleHeader(tupDatum);
    //   tupType   = HeapTupleHeaderGetTypeId(tuple);
    //   tupTypmod = HeapTupleHeaderGetTypMod(tuple);
    //   tupDesc   = get_cached_rowtype(tupType, tupTypmod, &op->d.fieldselect.rowcache, NULL);
    //
    // A composite value reaches this step either as `Datum::Composite` (minted by
    // ExecEvalRow / record_in) or — for a composite column deformed out of a heap
    // tuple — as a flat `Datum::ByRef` HeapTupleHeader image; decode either into a
    // FormedTuple.
    let mcx = estate.es_query_cxt;
    let tuple = datum_get_heap_tuple_header(mcx, &tup_datum)?;

    let header = tuple
        .tuple
        .t_data
        .as_ref()
        .expect("ExecEvalFieldSelect: composite Datum has no header");
    let tup_type = types_tuple::heaptuple::HeapTupleHeaderGetTypeId(header);
    let tup_typmod = types_tuple::heaptuple::HeapTupleHeaderGetTypMod(header);

    // tupDesc = get_cached_rowtype(tupType, tupTypmod, &op->d.fieldselect.rowcache, NULL);
    //
    // The owned rowcache caching contract (ExprEvalRowtypeCache::cacheptr) cannot
    // round-trip the C void* TypeCacheEntry*/TupleDesc* pointer; the faithful
    // substitute is the typcache lookup itself, which is internally cached
    // (lookup_rowtype_tupdesc → lookup_type_cache(TYPECACHE_TUPDESC)).
    let tup_desc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
        mcx, tup_type, tup_typmod,
    )?;

    // /*
    //  * Find field's attr record.  Note we don't support system columns here: a
    //  * datum tuple doesn't have valid values for most of the interesting
    //  * system columns anyway.
    //  */
    // if (fieldnum <= 0)  elog(ERROR, "unsupported reference to system column %d ...");
    // if (fieldnum > tupDesc->natts)  elog(ERROR, "attribute number %d exceeds number of columns %d");
    if fieldnum <= 0 {
        return Err(types_error::PgError::error(format!(
            "unsupported reference to system column {fieldnum} in FieldSelect"
        )));
    }
    if fieldnum as i32 > tup_desc.natts {
        return Err(types_error::PgError::error(format!(
            "attribute number {fieldnum} exceeds number of columns {}",
            tup_desc.natts
        )));
    }
    let attr = tup_desc.attr((fieldnum - 1) as usize);

    // /* Check for dropped column, and force a NULL result if so */
    // if (attr->attisdropped) { *op->resnull = true; return; }
    if attr.attisdropped {
        store_result(state, op, Datum::null(), true);
        return Ok(());
    }

    // /* Check for type mismatch --- possible after ALTER COLUMN TYPE? */
    // if (op->d.fieldselect.resulttype != attr->atttypid)
    //     ereport(ERROR, "attribute %d has wrong type", ...);
    if resulttype != attr.atttypid {
        return Err(types_error::PgError::error(format!(
            "attribute {fieldnum} has wrong type"
        ))
        .with_detail(format!(
            "Table has type {}, but query expects type {}.",
            attr.atttypid, resulttype
        )));
    }

    // /* heap_getattr needs a HeapTuple not a bare HeapTupleHeader */
    // tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
    // tmptup.t_data = tuple;
    // *op->resvalue = heap_getattr(&tmptup, fieldnum, tupDesc, op->resnull);
    let (value, fieldisnull) =
        backend_access_common_heaptuple::heap_getattr(mcx, &tuple, fieldnum as i32, &tup_desc)?;
    store_result(state, op, value, fieldisnull);
    Ok(())
}

/// `ExecEvalFieldStoreDeForm(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — deform the composite value a FieldStore updates.
///
/// A NULL input becomes an all-nulls row; otherwise the composite datum is
/// decoded, its rowtype `TupleDesc` cached (after the detoast that
/// `DatumGetHeapTupleHeader` may do), a too-many-columns mismatch is an
/// `elog(ERROR)`, and `heap_deform_tuple` fills the step's values/nulls arrays
/// for the subsequent field-update steps.
pub fn ExecEvalFieldStoreDeForm<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let (tup_datum, isnull) = load_result(state, op);

    // Read the step's resulttype / per-column cells / ncolumns up front.
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (resulttype, col_cells, ncolumns) = match &steps[op].d {
        ExprEvalStepData::FieldStore {
            resulttype,
            col_cells,
            ncolumns,
            ..
        } => {
            let col_cells = col_cells
                .as_ref()
                .expect("ExecEvalFieldStoreDeForm: op->d.fieldstore.col_cells not allocated")
                .clone();
            (*resulttype, col_cells, *ncolumns as usize)
        }
        other => unreachable!("ExecEvalFieldStoreDeForm: step.d is not FieldStore: {other:?}"),
    };

    // if (*op->resnull) {
    //     /* Convert null input tuple into an all-nulls row */
    //     memset(op->d.fieldstore.nulls, true, op->d.fieldstore.ncolumns * sizeof(bool));
    // }
    if isnull {
        for i in 0..ncolumns {
            state.result_cells.set(
                col_cells[i],
                ResultCell {
                    value: Datum::null(),
                    isnull: true,
                },
            );
        }
        return Ok(());
    }

    // else {
    //     Datum tupDatum = *op->resvalue;
    //     tuphdr = DatumGetHeapTupleHeader(tupDatum);
    //     tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
    //     ItemPointerSetInvalid(&(tmptup.t_self));
    //     tmptup.t_tableOid = InvalidOid;
    //     tmptup.t_data = tuphdr;
    //     tupDesc = get_cached_rowtype(op->d.fieldstore.fstore->resulttype, -1,
    //                                  op->d.fieldstore.rowcache, NULL);
    //     if (unlikely(tupDesc->natts > op->d.fieldstore.ncolumns))
    //         elog(ERROR, "too many columns in composite type %u", ...);
    //     heap_deform_tuple(&tmptup, tupDesc, op->d.fieldstore.values, op->d.fieldstore.nulls);
    // }
    //
    // Decode the input composite Datum (Composite or a flat by-ref
    // HeapTupleHeader image) into a FormedTuple — the owned stand-in for
    // DatumGetHeapTupleHeader / building the bare `tmptup`.
    let tuple = datum_get_heap_tuple_header(mcx, &tup_datum)?;

    // tupDesc = get_cached_rowtype(op->d.fieldstore.fstore->resulttype, -1, rowcache, NULL);
    //
    // The owned rowcache cannot round-trip the C void* cache pointer; the
    // faithful substitute is the internally-cached typcache lookup.
    let tup_desc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
        mcx, resulttype, -1,
    )?;

    // if (unlikely(tupDesc->natts > op->d.fieldstore.ncolumns))
    //     elog(ERROR, "too many columns in composite type %u", fstore->resulttype);
    if tup_desc.natts as usize > ncolumns {
        return Err(types_error::PgError::error(format!(
            "too many columns in composite type {resulttype}"
        )));
    }

    // heap_deform_tuple(&tmptup, tupDesc, op->d.fieldstore.values, op->d.fieldstore.nulls);
    let cols = backend_access_common_heaptuple::heap_deform_tuple(
        mcx,
        &tuple.tuple,
        &tup_desc,
        tuple.data.as_slice(),
    )?;

    // Scatter the deformed (value, isnull) pairs into the per-column cells.
    for (i, (value, colisnull)) in cols.iter().enumerate().take(ncolumns) {
        state.result_cells.set(
            col_cells[i],
            ResultCell {
                value: value.clone(),
                isnull: *colisnull,
            },
        );
    }
    Ok(())
}

/// `ExecEvalFieldStoreForm(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — re-form the composite value after field updates.
///
/// Looks up the (already-valid) rowtype `TupleDesc` and `heap_form_tuple`s the
/// step's values/nulls into the new composite datum.
pub fn ExecEvalFieldStoreForm<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // tupDesc = get_cached_rowtype(op->d.fieldstore.fstore->resulttype, -1,
    //                              op->d.fieldstore.rowcache, NULL);
    // tuple = heap_form_tuple(tupDesc, op->d.fieldstore.values, op->d.fieldstore.nulls);
    // *op->resvalue = HeapTupleGetDatum(tuple);
    // *op->resnull = false;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (resulttype, col_cells, ncolumns) = match &steps[op].d {
        ExprEvalStepData::FieldStore {
            resulttype,
            col_cells,
            ncolumns,
            ..
        } => {
            let col_cells = col_cells
                .as_ref()
                .expect("ExecEvalFieldStoreForm: op->d.fieldstore.col_cells not allocated")
                .clone();
            (*resulttype, col_cells, *ncolumns as usize)
        }
        other => unreachable!("ExecEvalFieldStoreForm: step.d is not FieldStore: {other:?}"),
    };

    // tupDesc = get_cached_rowtype(fstore->resulttype, -1, rowcache, NULL);
    let tup_desc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
        mcx, resulttype, -1,
    )?;

    // Gather the per-column cells (DEFORM-populated, then overwritten by each
    // newval sub-expression) into the values/nulls heap_form_tuple expects.
    let natts = tup_desc.natts as usize;
    debug_assert!(natts <= ncolumns);
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(natts);
    for i in 0..natts {
        let c = state.result_cells.get(col_cells[i]);
        values.push(c.value);
        nulls.push(c.isnull);
    }

    // tuple = heap_form_tuple(tupDesc, values, nulls);
    let tuple = backend_access_common_heaptuple::heap_form_tuple(mcx, &tup_desc, &values, &nulls)
        .map_err(|e| types_error::PgError::error(format!("ExecEvalFieldStoreForm: {e:?}")))?;

    // *op->resvalue = HeapTupleGetDatum(tuple); *op->resnull = false;
    store_result(state, op, Datum::Composite(tuple), false);
    Ok(())
}

/// `ExecEvalConvertRowtype(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce a composite value to another rowtype.
///
/// A NULL input yields a NULL output. The in/out rowtype `TupleDesc`s are cached
/// and pinned; on first use (or after a cache change) the attribute map is
/// (re)built in the per-query context. If a map is needed the tuple is
/// rearranged via `execute_attr_map_tuple`; otherwise it is physically
/// compatible and only relabeled with the destination rowtype via
/// `heap_copy_tuple_as_datum`. Both descriptors are unpinned before return.
pub fn ExecEvalConvertRowtype<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    _econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (inputtype, outputtype) = match &steps[op].d {
        ExprEvalStepData::ConvertRowtype {
            inputtype,
            outputtype,
            ..
        } => (*inputtype, *outputtype),
        other => unreachable!("ExecEvalConvertRowtype: step.d is not ConvertRowtype: {other:?}"),
    };

    // /* NULL in -> NULL out */
    // if (*op->resnull) return;
    let (tup_datum, isnull) = load_result(state, op);
    if isnull {
        return Ok(());
    }

    // tupDatum = *op->resvalue; tuple = DatumGetHeapTupleHeader(tupDatum);
    //
    // A composite value reaches this step as `Datum::Composite` (minted by
    // ExecEvalRow / ExecEvalWholeRowVar / record_in) or as a flat by-ref
    // HeapTupleHeader image (a composite column deformed out of a heap tuple);
    // decode either into a FormedTuple.
    let in_tuple = datum_get_heap_tuple_header(mcx, &tup_datum)?;

    // indesc  = get_cached_rowtype(op->d.convert_rowtype.inputtype,  -1, incache,  &changed);
    // outdesc = get_cached_rowtype(op->d.convert_rowtype.outputtype, -1, outcache, &changed);
    //
    // The owned rowcache (ExprEvalRowtypeCache) cannot round-trip the C void*
    // cache pointer; the faithful substitute is the typcache lookup itself, which
    // is internally cached (lookup_rowtype_tupdesc → lookup_type_cache). Since the
    // descriptors are recomputed each call we always (re)build the conversion map,
    // matching C's `changed` path.
    let indesc =
        backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(mcx, inputtype, -1)?;
    let outdesc =
        backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(mcx, outputtype, -1)?;

    // map = convert_tuples_by_name(indesc, outdesc);
    let map = backend_access_common_next_seams::convert_tuples_by_name::call(mcx, &indesc, &outdesc)?;

    // tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple); tmptup.t_data = tuple;
    let result = if let Some(map) = map.as_ref() {
        // result = execute_attr_map_tuple(&tmptup, map);
        backend_access_common_next_seams::execute_attr_map_tuple::call(
            mcx,
            &in_tuple.tuple,
            in_tuple.data.as_slice(),
            map,
        )?
    } else {
        // physically compatible: just relabel with the destination rowtype.
        // *op->resvalue = heap_copy_tuple_as_datum(&tmptup, outdesc);
        backend_access_common_heaptuple::heap_copy_tuple_as_datum(mcx, &in_tuple, &outdesc)?
    };

    // *op->resvalue = HeapTupleGetDatum(result); *op->resnull stays false.
    store_result(state, op, Datum::Composite(result), false);
    Ok(())
}

/// `ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — materialize a whole-row Var as a composite Datum.
///
/// Selects the source slot for the Var's varno (INNER/OUTER/SCAN, or OLD/NEW
/// with a NULL shortcut), applies any junk filter, on first use builds and
/// blesses the output `TupleDesc` (verifying type compatibility for a named
/// composite, or absorbing the slot's rowtype/aliases for RECORD), then builds a
/// flattened composite datum labelled with the identified rowtype.
pub fn ExecEvalWholeRowVar<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // Var *variable = op->d.wholerow.var;
    // Assert(variable->varattno == InvalidAttrNumber);
    //
    // Read the (immutable) bits of the Var + the step's `first`/`slow`/junk
    // fields up front; the step is re-borrowed mutably below to update `first` /
    // `slow` / `tupdesc`.
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (varno, varreturningtype, vartype, var_typmod, first, slow, junk_filter) =
        match &steps[op].d {
            ExprEvalStepData::WholeRow {
                var,
                first,
                slow,
                junk_filter,
                ..
            } => {
                let var = var
                    .as_ref()
                    .expect("ExecEvalWholeRowVar: op->d.wholerow.var not threaded");
                debug_assert_eq!(
                    var.varattno, 0,
                    "ExecEvalWholeRowVar: whole-row Var must have varattno == InvalidAttrNumber"
                );
                (
                    var.varno,
                    var.varreturningtype,
                    var.vartype,
                    var.vartypmod,
                    *first,
                    *slow,
                    *junk_filter,
                )
            }
            other => unreachable!("ExecEvalWholeRowVar: step.d is not WholeRow: {other:?}"),
        };
    let _ = var_typmod;

    // Get the input slot we want (switch on variable->varno).
    let ecxt = &estate.es_exprcontexts[econtext.0 as usize]
        .as_ref()
        .expect("ExecEvalWholeRowVar: econtext freed");
    let slot_id: SlotId = match varno {
        // case INNER_VAR: slot = econtext->ecxt_innertuple;
        INNER_VAR => ecxt
            .ecxt_innertuple
            .expect("ExecEvalWholeRowVar: ecxt_innertuple is NULL"),
        // case OUTER_VAR: slot = econtext->ecxt_outertuple;
        OUTER_VAR => ecxt
            .ecxt_outertuple
            .expect("ExecEvalWholeRowVar: ecxt_outertuple is NULL"),
        // default: get the tuple from the relation being scanned. By default the
        // "scan" tuple slot, but a wholerow Var in RETURNING may refer to
        // OLD/NEW; if that row doesn't exist, return NULL.
        _ => match varreturningtype {
            VarReturningType::VAR_RETURNING_DEFAULT => ecxt
                .ecxt_scantuple
                .expect("ExecEvalWholeRowVar: ecxt_scantuple is NULL"),
            VarReturningType::VAR_RETURNING_OLD => {
                if state.flags & EEO_FLAG_OLD_IS_NULL != 0 {
                    store_result(state, op, Datum::null(), true);
                    return Ok(());
                }
                ecxt.ecxt_oldtuple
                    .expect("ExecEvalWholeRowVar: ecxt_oldtuple is NULL")
            }
            VarReturningType::VAR_RETURNING_NEW => {
                if state.flags & EEO_FLAG_NEW_IS_NULL != 0 {
                    store_result(state, op, Datum::null(), true);
                    return Ok(());
                }
                ecxt.ecxt_newtuple
                    .expect("ExecEvalWholeRowVar: ecxt_newtuple is NULL")
            }
        },
    };

    // Apply the junkfilter if any.
    //
    // The junk filter is parked as an opaque address (the execJunk owner is not
    // wired here); a non-zero value means a junk filter is present and the slot
    // selection above would need rerouting through ExecFilterJunk. None of the
    // wholerow paths the executor reaches today install one.
    if junk_filter != 0 {
        return Err(types_error::PgError::error(
            "ExecEvalWholeRowVar: a junk filter is attached to this whole-row Var, \
             but execJunk's ExecFilterJunk is not wired into this crate yet",
        ));
    }

    // If first time through, obtain the output tuple descriptor and check
    // compatibility, then bless it and cache it on the step.
    if first {
        // optimistically assume we don't need the slow path
        let mut new_slow = false;

        let output_tupdesc;
        if vartype != types_tuple::heaptuple::RECORDOID {
            // Named composite: check the slot's actual rowtype is compatible.
            //
            // var_tupdesc = lookup_rowtype_tupdesc_domain(variable->vartype, -1, false);
            let var_tupdesc =
                backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc_domain::call(
                    mcx, vartype, -1, false,
                )?
                .expect("ExecEvalWholeRowVar: named composite vartype is not composite");

            // slot_tupdesc = slot->tts_tupleDescriptor;
            let slot_tupdesc =
                backend_executor_execTuples_seams::exec_slot_descriptor::call(mcx, estate, slot_id)?
                    .expect("ExecEvalWholeRowVar: slot has no tuple descriptor");

            // if (var_tupdesc->natts != slot_tupdesc->natts) ereport(...);
            if var_tupdesc.natts != slot_tupdesc.natts {
                return Err(types_error::PgError::error(
                    "table row type and query-specified row type do not match",
                )
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                .with_detail(format!(
                    "Table row contains {} attributes, but query expects {}.",
                    slot_tupdesc.natts, var_tupdesc.natts
                )));
            }

            // for each attribute, check datatypes; tolerate dropped columns.
            for i in 0..var_tupdesc.natts as usize {
                let vattr = var_tupdesc.attr(i);
                let sattr = slot_tupdesc.attr(i);

                if vattr.atttypid == sattr.atttypid {
                    continue; // no worries
                }
                if !vattr.attisdropped {
                    let sname =
                        backend_utils_adt_format_type_seams::format_type_be_owned::call(
                            sattr.atttypid,
                        )?;
                    let vname =
                        backend_utils_adt_format_type_seams::format_type_be_owned::call(
                            vattr.atttypid,
                        )?;
                    return Err(types_error::PgError::error(
                        "table row type and query-specified row type do not match",
                    )
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                    .with_detail(format!(
                        "Table has type {} at ordinal position {}, but query expects {}.",
                        sname,
                        i + 1,
                        vname
                    )));
                }

                if vattr.attlen != sattr.attlen || vattr.attalign != sattr.attalign {
                    new_slow = true; // need to check for nulls
                }
            }

            // Use the variable's declared rowtype as the output descriptor (it
            // carries the attisdropped markings). C copies into the per-query
            // context; clone_in(mcx) here.
            output_tupdesc = var_tupdesc.clone_in(mcx)?;
        } else {
            // RECORD case: use the input slot's rowtype as the output descriptor,
            // reset to RECORD, and adopt the source RTE's column aliases.
            let slot_tupdesc =
                backend_executor_execTuples_seams::exec_slot_descriptor::call(mcx, estate, slot_id)?
                    .expect("ExecEvalWholeRowVar: slot has no tuple descriptor");
            let mut out = slot_tupdesc.clone_in(mcx)?;

            // We're supposed to return RECORD, so reset to that.
            out.tdtypeid = types_tuple::heaptuple::RECORDOID;
            out.tdtypmod = -1;

            // Try to find the source RTE and adopt its column aliases (a String
            // node list, C `rte->eref->colnames`). If we can't locate the RTE or
            // its eref, the slot's existing names are kept.
            if varno >= 1 && (varno as usize) <= estate.es_range_table_size {
                let colnames: Option<Vec<String>> = estate
                    .es_range_table
                    .get((varno - 1) as usize)
                    .and_then(|rte| rte.eref.as_ref())
                    .map(|eref| {
                        eref.colnames
                            .iter()
                            .map(|n| match n.as_string() {
                                Some(s) => s.sval.as_str().to_string(),
                                // Dropped columns are represented by an empty
                                // String node; any non-String is unexpected but
                                // treated as an empty (skipped) name.
                                None => String::new(),
                            })
                            .collect::<Vec<_>>()
                    });
                if let Some(names) = colnames {
                    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                    backend_executor_execTuples_seams::exec_type_set_col_names::call(
                        &mut out, &refs,
                    )?;
                }
            }
            output_tupdesc = out;
        }

        // Bless the tupdesc if needed, and save it on the step.
        let output_tupdesc = mcx::alloc_in(mcx, output_tupdesc)?;
        let blessed =
            backend_executor_execTuples_seams::bless_tuple_desc::call(mcx, Some(output_tupdesc))?;

        let steps = state
            .steps
            .as_mut()
            .expect("eval_composite: steps not ready");
        match &mut steps[op].d {
            ExprEvalStepData::WholeRow {
                first: first_mut,
                slow: slow_mut,
                tupdesc,
                ..
            } => {
                *tupdesc = blessed;
                *slow_mut = new_slow;
                *first_mut = false;
            }
            _ => unreachable!("ExecEvalWholeRowVar: step.d is not WholeRow on writeback"),
        }
    }

    // Make sure all columns of the slot are accessible in the slot's
    // Datum/isnull arrays (and read its descriptor for the slow check / build).
    let cols = backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, slot_id)?;
    let slot_tupdesc =
        backend_executor_execTuples_seams::exec_slot_descriptor::call(mcx, estate, slot_id)?
            .expect("ExecEvalWholeRowVar: slot has no tuple descriptor");

    // Re-read the (now-populated) blessed output tupdesc.
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let out_tupdesc = match &steps[op].d {
        ExprEvalStepData::WholeRow { tupdesc, slow, .. } => {
            let td = tupdesc
                .as_ref()
                .expect("ExecEvalWholeRowVar: blessed tupdesc not built")
                .clone_in(mcx)?;
            (td, *slow)
        }
        _ => unreachable!("ExecEvalWholeRowVar: step.d is not WholeRow"),
    };
    let (var_tupdesc, slow) = out_tupdesc;

    if slow {
        // Check that any dropped attributes are non-null.
        debug_assert_eq!(var_tupdesc.natts, slot_tupdesc.natts);
        for i in 0..var_tupdesc.natts as usize {
            let vattr = &var_tupdesc.compact_attrs[i];
            let sattr = &slot_tupdesc.compact_attrs[i];
            if !vattr.attisdropped {
                continue; // already checked non-dropped cols
            }
            if cols[i].1 {
                continue; // null is always okay
            }
            if vattr.attlen != sattr.attlen || vattr.attalignby != sattr.attalignby {
                return Err(types_error::PgError::error(
                    "table row type and query-specified row type do not match",
                )
                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
                .with_detail(format!(
                    "Physical storage mismatch on dropped attribute at ordinal position {}.",
                    i + 1
                )));
            }
        }
    }

    // Build a composite datum, making sure any toasted fields get detoasted.
    // (Critical: we must not change the slot's state here, which is why we built
    // the values/nulls workspace from the deformed copy above.)
    //
    // tuple = toast_build_flattened_tuple(slot->tts_tupleDescriptor,
    //                                     slot->tts_values, slot->tts_isnull);
    let values: Vec<Datum<'mcx>> = cols.iter().map(|c| c.0.clone()).collect();
    let nulls: Vec<bool> = cols.iter().map(|c| c.1).collect();
    let mut tuple =
        backend_access_heap_heaptoast::toast_build_flattened_tuple(mcx, &slot_tupdesc, &values, &nulls)?;

    // Label the datum with the composite type info identified before.
    // HeapTupleHeaderSetTypeId(dtuple, op->d.wholerow.tupdesc->tdtypeid);
    // HeapTupleHeaderSetTypMod(dtuple, op->d.wholerow.tupdesc->tdtypmod);
    {
        let header = tuple
            .tuple
            .t_data
            .as_mut()
            .expect("ExecEvalWholeRowVar: built tuple has no header");
        types_tuple::heaptuple::HeapTupleHeaderSetTypeId(header, var_tupdesc.tdtypeid);
        types_tuple::heaptuple::HeapTupleHeaderSetTypMod(header, var_tupdesc.tdtypmod);
    }

    // *op->resvalue = PointerGetDatum(dtuple); *op->resnull = false;
    store_result(state, op, Datum::Composite(tuple), false);
    Ok(())
}
