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
//! The cross-crate owners these steps then call are NOT wired as dependencies
//! of this crate (the only seam deps are execExpr / execTuples / nodeSubplan /
//! fmgr), and the boundary they sit behind is the *composite-`Datum` bridge*:
//! a composite value is a pass-by-reference (varlena) `Datum`, and the trimmed
//! [`Datum`] model (`SIZEOF_DATUM` scalar word only) cannot round-trip a
//! pointer payload to a `HeapTupleHeader` (`DatumGetHeapTupleHeader`) nor mint
//! one back (`HeapTupleGetDatum` / `PointerGetDatum`). That payload model is
//! owned by `execTuples` / `backend-access-common-heaptuple` (the same blocker
//! the `ExecJust*` by-reference fast paths document). The heap-tuple
//! form/deform/getattr (`heap_form_tuple`, `heap_deform_tuple`, `heap_getattr`,
//! `heap_attisnull`), the rowtype cache (`get_cached_rowtype`, sibling dispatch
//! family), the tuple-conversion map (`convert_tuples_by_name` /
//! `execute_attr_map_tuple` / `heap_copy_tuple_as_datum`), the TOAST flattener
//! (`toast_build_flattened_tuple`), the expanded-record helpers, the junk
//! filter (`ExecFilterJunk`) and the fmgr comparison call frame
//! (`FunctionCallInvoke` over `fcinfo->args[]`) all live behind that boundary.
//! Per mirror-PG-and-panic, once the modeled control flow reaches that boundary
//! it aborts with a loud panic naming the unported owner rather than silently
//! stubbing a result.

// The bare-word newtype: the scalar form the composite eval helpers operate on.
use types_datum::Datum;
// The canonical unified value type (Datum-unification keystone) — what the
// keystone-owned `ExprState.resvalue` / `ResultCell.value` carry.
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;
use types_error::PgResult;
use types_nodes::execexpr::{
    ExprEvalStepData, ExprState, MinMaxOp, ResultCell, ResultCellId, STATE_RESULT_CELL,
};

/// Recover the bare scalar word from a stored canonical by-value datum (the
/// transitional bridge: the composite helpers operate on a word).
#[inline]
fn word_of(v: &DatumV<'_>) -> Datum {
    Datum::from_usize(v.as_usize())
}
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// The composite-`Datum` bridge (`DatumGetHeapTupleHeader` / `HeapTupleGetDatum`
/// / `PointerGetDatum`) and the heap-tuple / tupconvert / TOAST / expanded-record
/// owners these composite opcodes call sit behind a not-yet-ported boundary:
/// a composite value is a pass-by-reference (varlena) `Datum`, which the trimmed
/// `Datum` scalar-word model cannot round-trip, and the heap-tuple payload model
/// is owned by `execTuples` / `backend-access-common-heaptuple` (not wired as a
/// dependency of this crate). Mirror-PG-and-panic: abort loudly naming the owner
/// rather than no-op a fabricated result.
#[cold]
#[inline(never)]
fn composite_datum_owner_unported(what: &str) -> ! {
    panic!(
        "backend-access-common-heaptuple / backend-executor-execTuples: {what} \
         needs the composite-Datum bridge (DatumGetHeapTupleHeader / \
         HeapTupleGetDatum / PointerGetDatum) and the heap-tuple payload model; \
         a composite value is a pass-by-reference varlena Datum the trimmed \
         Datum scalar-word model cannot round-trip, and those owners are not \
         ported / not wired as a dependency of this crate yet"
    )
}

/// Read a step's current `(value, isnull)` result (the C `*op->resvalue` /
/// `*op->resnull`). [`STATE_RESULT_CELL`] aliases the owning `ExprState`'s own
/// `resvalue`/`resnull` (the C `&state->resvalue`/`&state->resnull` default
/// target); any other id reads the per-step cell from the arena.
fn load_result(state: &ExprState<'_>, op: usize) -> (Datum, bool) {
    let cell = state.steps.as_ref().expect("eval_composite: steps not ready")[op].resvalue;
    if cell == STATE_RESULT_CELL {
        // The canonical result value crosses back to the bare scalar word the
        // composite helpers operate on.
        (word_of(&state.resvalue), state.resnull)
    } else {
        let c = state.result_cells.get(cell);
        (word_of(&c.value), c.isnull)
    }
}

/// Write a step's `(value, isnull)` result (the C `*op->resvalue = value;
/// *op->resnull = isnull;`). [`STATE_RESULT_CELL`] writes through the owning
/// `ExprState`'s own `resvalue`/`resnull`; any other id writes the arena cell.
fn store_result(state: &mut ExprState<'_>, op: usize, value: Datum, isnull: bool) {
    let cell: ResultCellId = state.steps.as_ref().expect("eval_composite: steps not ready")[op].resvalue;
    if cell == STATE_RESULT_CELL {
        // The bare scalar word crosses into the canonical by-value arm.
        state.resvalue = DatumV::ByVal(value);
        state.resnull = isnull;
    } else {
        state
            .result_cells
            .set(cell, ResultCell { value: DatumV::ByVal(value), isnull });
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Datum value = *op->resvalue;
    // bool  isnull = *op->resnull;
    let (_value, isnull) = load_result(state, op);

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
    // tupType = HeapTupleHeaderGetTypeId(tuple);
    // tupTypmod = HeapTupleHeaderGetTypMod(tuple);
    // tupDesc = get_cached_rowtype(tupType, tupTypmod,
    //                              &op->d.nulltest_row.rowcache, NULL);
    // tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
    // tmptup.t_data = tuple;
    // for (att = 1; att <= tupDesc->natts; att++) {
    //     if (TupleDescCompactAttr(tupDesc, att - 1)->attisdropped) continue;
    //     if (heap_attisnull(&tmptup, att, tupDesc)) {
    //         if (!checkisnull) { *op->resvalue = BoolGetDatum(false); return; }
    //     } else {
    //         if (checkisnull) { *op->resvalue = BoolGetDatum(false); return; }
    //     }
    // }
    // *op->resvalue = BoolGetDatum(true);
    //
    // DatumGetHeapTupleHeader(value) + heap_attisnull cross the composite-Datum
    // bridge / heap-tuple payload boundary.
    composite_datum_owner_unported(
        "ExecEvalRowNullInt: decoding the row Datum (DatumGetHeapTupleHeader) and \
         per-field heap_attisnull tests",
    )
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tuple = heap_form_tuple(op->d.row.tupdesc,
    //                         op->d.row.elemvalues, op->d.row.elemnulls);
    // *op->resvalue = HeapTupleGetDatum(tuple);
    // *op->resnull = false;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    match &steps[op].d {
        ExprEvalStepData::Row { .. } => {}
        other => unreachable!("ExecEvalRow: step.d is not Row: {other:?}"),
    }
    // heap_form_tuple + HeapTupleGetDatum cross the heap-tuple / composite-Datum
    // boundary.
    composite_datum_owner_unported(
        "ExecEvalRow: heap_form_tuple(op->d.row.tupdesc, elemvalues, elemnulls) \
         then HeapTupleGetDatum",
    )
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Datum *values = op->d.minmax.values;
    // bool  *nulls  = op->d.minmax.nulls;
    // FunctionCallInfo fcinfo = op->d.minmax.fcinfo_data;
    // MinMaxOp operator = op->d.minmax.op;
    // Assert(fcinfo->args[0].isnull == false);
    // Assert(fcinfo->args[1].isnull == false);
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let (values, nulls, nelems, operator) = match &steps[op].d {
        ExprEvalStepData::MinMax {
            values,
            nulls,
            nelems,
            op: minmax_op,
            ..
        } => {
            let values = values
                .as_ref()
                .expect("ExecEvalMinMax: op->d.minmax.values not allocated");
            let nulls = nulls
                .as_ref()
                .expect("ExecEvalMinMax: op->d.minmax.nulls not allocated");
            (values.clone(), nulls.clone(), *nelems, *minmax_op)
        }
        other => unreachable!("ExecEvalMinMax: step.d is not MinMax: {other:?}"),
    };

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
            store_result(state, op, word_of(&values[off]), false);
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
            // The comparison `FunctionCallInvoke(fcinfo)` over `fcinfo->args[0/1]`
            // needs the fmgr-widened FunctionCallInfoBaseData (trimmed model has no
            // args[]/isnull) — same blocker the ExecJust* func paths document.
            let _ = (cur_value, operator, MinMaxOp::IS_GREATEST, MinMaxOp::IS_LEAST);
            panic!(
                "backend-utils-fmgr-fmgr: ExecEvalMinMax comparison \
                 FunctionCallInvoke(fcinfo) over fcinfo->args[0]/args[1] needs the \
                 fmgr-widened FunctionCallInfoBaseData (the trimmed model carries \
                 only resultinfo, no args[]/isnull); blocked until fmgr widens the \
                 call frame in this lineage"
            )
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // AttrNumber fieldnum = op->d.fieldselect.fieldnum;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    let _fieldnum = match &steps[op].d {
        ExprEvalStepData::FieldSelect { fieldnum, .. } => *fieldnum,
        other => unreachable!("ExecEvalFieldSelect: step.d is not FieldSelect: {other:?}"),
    };

    // /* NULL record -> NULL result */
    // if (*op->resnull) return;
    let (_tup_datum, isnull) = load_result(state, op);
    if isnull {
        return Ok(());
    }

    // tupDatum = *op->resvalue;
    // if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(tupDatum))) {
    //     ... expanded_record_get_tupdesc / expanded_record_get_field ...
    // } else {
    //     tuple = DatumGetHeapTupleHeader(tupDatum);
    //     tupType/tupTypmod = HeapTupleHeaderGetTypeId/TypMod(tuple);
    //     tupDesc = get_cached_rowtype(tupType, tupTypmod, &op->d.fieldselect.rowcache, NULL);
    //     /* fieldnum bounds + attisdropped + resulttype mismatch checks */
    //     *op->resvalue = heap_getattr(&tmptup, fieldnum, tupDesc, op->resnull);
    // }
    //
    // Both the expanded-record fast path and DatumGetHeapTupleHeader + heap_getattr
    // cross the composite-Datum / heap-tuple boundary.
    composite_datum_owner_unported(
        "ExecEvalFieldSelect: decoding the record Datum (expanded-record fast path \
         or DatumGetHeapTupleHeader) and heap_getattr extraction",
    )
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let (_tup_datum, isnull) = load_result(state, op);

    // if (*op->resnull) {
    //     /* Convert null input tuple into an all-nulls row */
    //     memset(op->d.fieldstore.nulls, true, op->d.fieldstore.ncolumns * sizeof(bool));
    // }
    let steps = state.steps.as_mut().expect("eval_composite: steps not ready");
    if isnull {
        match &mut steps[op].d {
            ExprEvalStepData::FieldStore { nulls, ncolumns, .. } => {
                let ncolumns = *ncolumns as usize;
                let nulls = nulls
                    .as_mut()
                    .expect("ExecEvalFieldStoreDeForm: op->d.fieldstore.nulls not allocated");
                for n in nulls.iter_mut().take(ncolumns) {
                    *n = true;
                }
            }
            other => {
                unreachable!("ExecEvalFieldStoreDeForm: step.d is not FieldStore: {other:?}")
            }
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
    // DatumGetHeapTupleHeader + heap_deform_tuple cross the composite-Datum /
    // heap-tuple boundary; note the C `fstore->resulttype` lookup is itself parked
    // on the unported FieldStore node (op->d.fieldstore.fstore is an opaque address).
    composite_datum_owner_unported(
        "ExecEvalFieldStoreDeForm: decoding the input record Datum \
         (DatumGetHeapTupleHeader) and heap_deform_tuple into the step's \
         values/nulls arrays",
    )
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tupDesc = get_cached_rowtype(op->d.fieldstore.fstore->resulttype, -1,
    //                              op->d.fieldstore.rowcache, NULL);
    // tuple = heap_form_tuple(tupDesc, op->d.fieldstore.values, op->d.fieldstore.nulls);
    // *op->resvalue = HeapTupleGetDatum(tuple);
    // *op->resnull = false;
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    match &steps[op].d {
        ExprEvalStepData::FieldStore { .. } => {}
        other => unreachable!("ExecEvalFieldStoreForm: step.d is not FieldStore: {other:?}"),
    }
    // heap_form_tuple + HeapTupleGetDatum cross the heap-tuple / composite-Datum
    // boundary (and the C resulttype lookup is parked on the unported FieldStore node).
    composite_datum_owner_unported(
        "ExecEvalFieldStoreForm: heap_form_tuple(tupDesc, values, nulls) then \
         HeapTupleGetDatum",
    )
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
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    match &steps[op].d {
        ExprEvalStepData::ConvertRowtype { .. } => {}
        other => unreachable!("ExecEvalConvertRowtype: step.d is not ConvertRowtype: {other:?}"),
    }

    // /* NULL in -> NULL out */
    // if (*op->resnull) return;
    let (_tup_datum, isnull) = load_result(state, op);
    if isnull {
        return Ok(());
    }

    // tupDatum = *op->resvalue;
    // tuple = DatumGetHeapTupleHeader(tupDatum);
    // indesc  = get_cached_rowtype(op->d.convert_rowtype.inputtype,  -1, incache,  &changed);
    // IncrTupleDescRefCount(indesc);
    // outdesc = get_cached_rowtype(op->d.convert_rowtype.outputtype, -1, outcache, &changed);
    // IncrTupleDescRefCount(outdesc);
    // Assert(... typeid matches indesc or RECORDOID);
    // if (changed) { old = MemoryContextSwitchTo(per_query); map = convert_tuples_by_name(indesc, outdesc); MemoryContextSwitchTo(old); }
    // tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple); tmptup.t_data = tuple;
    // if (map != NULL) { result = execute_attr_map_tuple(&tmptup, map); *op->resvalue = HeapTupleGetDatum(result); }
    // else *op->resvalue = heap_copy_tuple_as_datum(&tmptup, outdesc);
    // DecrTupleDescRefCount(indesc); DecrTupleDescRefCount(outdesc);
    //
    // DatumGetHeapTupleHeader + execute_attr_map_tuple / heap_copy_tuple_as_datum
    // cross the composite-Datum / heap-tuple / tupconvert boundary.
    composite_datum_owner_unported(
        "ExecEvalConvertRowtype: decoding the input record Datum \
         (DatumGetHeapTupleHeader), building the conversion map \
         (convert_tuples_by_name) and rearranging/relabeling the tuple \
         (execute_attr_map_tuple / heap_copy_tuple_as_datum)",
    )
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
    _econtext: EcxtId,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Var *variable = op->d.wholerow.var;
    // Assert(variable->varattno == InvalidAttrNumber);
    let steps = state.steps.as_ref().expect("eval_composite: steps not ready");
    match &steps[op].d {
        ExprEvalStepData::WholeRow { .. } => {}
        other => unreachable!("ExecEvalWholeRowVar: step.d is not WholeRow: {other:?}"),
    }

    // The Var-driven slot selection (switch on variable->varno /
    // variable->varreturningtype, with the OLD/NEW NULL shortcuts), the junk
    // filter (ExecFilterJunk), the wholerow.first TupleDesc build/bless
    // (lookup_rowtype_tupdesc_domain, CreateTupleDescCopy, BlessTupleDesc,
    // ExecTypeSetColNames), slot_getallattrs, the wholerow.slow dropped-attr
    // check, and the final toast_build_flattened_tuple + HeapTupleHeaderSetTypeId/
    // TypMod + PointerGetDatum all read the slot's value arrays
    // (execTuples-owned) and the heap-tuple / composite-Datum bridge. The
    // wholerow.var node itself is parked as an opaque address until primnodes
    // threads the real Var through this step. None of those owners are wired as
    // dependencies of this crate.
    composite_datum_owner_unported(
        "ExecEvalWholeRowVar: slot selection / junk filter / first-time \
         BlessTupleDesc build / slot_getallattrs / toast_build_flattened_tuple + \
         PointerGetDatum — needs the execTuples slot payload model, the heap-tuple \
         / composite-Datum bridge, and the parked wholerow.var node",
    )
}
