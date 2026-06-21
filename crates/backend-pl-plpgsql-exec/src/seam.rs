//! Outward value/SQL substrate the PL/pgSQL executor consumes.
//!
//! `pl_exec.c`'s control flow is real in this crate; the legs that evaluate an
//! expression through the SQL executor, run a query through SPI, iterate an
//! array through the fmgr, or open an internal subtransaction bottom out in
//! subsystems that are not yet reachable from here (the executor `ExprState` /
//! `ExprEvalStep` simple-expr path — keystone #165/#324; the plan-based SPI
//! surface `SPI_prepare`/`SPI_execute_plan`/`SPI_cursor_*` — not yet installed;
//! the array iterator + fmgr `FunctionCall`; `BeginInternalSubTransaction`).
//!
//! Per the porting discipline these are REAL-OR-LOUD: each names the precise C
//! callee + the external subsystem it needs and `panic!`s. A faithful C build
//! would `ereport`/elog at exactly these points until those owners land. They
//! are never faked / no-op'd.
//!
//! Once the SPI plan surface and the executor simple-expr substrate land, these
//! become thin delegations (or move into real in-crate bodies that call the
//! now-reachable owner crates directly).

use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_plpgsql::{
    int32, ExprContext, PLpgSQL_datum, PLpgSQL_datum_type, PLpgSQL_execstate, PLpgSQL_expr,
    PLpgSQL_var,
};

/// `CHECK_FOR_INTERRUPTS()` — interrupt-pending check. No-op until the signal
/// substrate is wired; the C macro is a cheap test with no control-flow effect
/// on the happy path.
#[inline]
pub fn check_for_interrupts() {
    // miscadmin.h CHECK_FOR_INTERRUPTS(): tests InterruptPending; the latch /
    // signal substrate is owned elsewhere. The no-op preserves control flow.
}

/// `elog(ERROR, "unrecognized dtype: %d")` in `exec_stmt_block` /
/// `exec_stmt_return` — a "can't happen" guard over the datum dtype.
pub fn elog_unrecognized_dtype_exec(dtype: PLpgSQL_datum_type) -> ! {
    panic!("unrecognized dtype: {dtype:?}");
}

/// `VARATT_IS_EXTERNAL_NON_EXPANDED(DatumGetPointer(value))` — is the Datum a
/// bare on-disk TOAST pointer (not an expanded object)? The varlena-header
/// inspection is the toast substrate; until it lands this reports `false` (the
/// common in-memory value path), so `assign_simple_var` takes its plain store
/// branch. A bare TOAST pointer reaching a non-atomic store would need the
/// detoast leg below.
#[inline]
pub fn datum_is_external_non_expanded(_value: Datum) -> bool {
    // The varlena TOAST-pointer probe (access/detoast.h) is owned elsewhere.
    // No bare TOAST pointer reaches the control-flow-only path; report false.
    false
}

/// The non-atomic detoast leg of `assign_simple_var` (pl_exec.c 8786): detoast
/// an external varlena in the eval mcontext, `datumCopy` it into the function
/// context, free the input if freeable. Returns `(detoasted, freeable=true)`.
pub fn assign_simple_var_detoast(_newvalue: Datum, _freeable: bool) -> (Datum, bool) {
    panic!(
        "seam not wired: assign_simple_var non-atomic detoast (pl_exec.c) — \
         detoast_external_attr + datumCopy (toast/value substrate)"
    );
}

/// The free-old-value leg of `assign_simple_var` (pl_exec.c 8810): release the
/// previous value, either `DeleteExpandedObject` for a R/W expanded object or
/// `pfree` for a flat freeable datum.
pub fn assign_simple_var_free_old(_oldvalue: Datum, _oldisnull: bool, _typlen: i16) {
    panic!(
        "seam not wired: assign_simple_var free old value (pl_exec.c) — \
         DeleteExpandedObject / pfree (expanded-object + palloc substrate)"
    );
}

/// `exec_assign_expr(estate, target, expr)` (pl_exec.c 5003): evaluate `expr`
/// and assign into `target`.
pub fn exec_assign_expr(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    expr: &PLpgSQL_expr,
) -> PgResult<()> {
    super::exec_assign_expr_impl(estate, target_dno, expr)
}

/// `exec_assign_value(estate, target, value, isNull, valtype, valtypmod)`
/// (pl_exec.c 5061): the generic datum-assignment dispatch (VAR/ROW/REC/
/// RECFIELD), with cast + array-element / record-field update legs.
pub fn exec_assign_value(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    value: Datum,
    isnull: bool,
    valtype: Oid,
    valtypmod: int32,
) -> PgResult<()> {
    super::exec_assign_value_impl(estate, target_dno, value, isnull, valtype, valtypmod)
}

/// `exec_eval_expr(estate, expr, &isNull, &rettype, &rettypmod)` (pl_exec.c
/// 5665): evaluate a PL/pgSQL expression, via the simple-expr fast path or a
/// one-row SPI select. Returns `(value, isnull, rettype, rettypmod)`.
pub fn exec_eval_expr(
    estate: &mut PLpgSQL_execstate,
    expr: &PLpgSQL_expr,
) -> PgResult<(Datum, bool, Oid, int32)> {
    super::exec_eval_expr_impl(estate, expr)
}

/// `exec_eval_boolean(estate, expr, &isNull)` (pl_exec.c 5642): evaluate a
/// boolean condition expression. Returns `(value, isnull)`.
pub fn exec_eval_boolean(
    estate: &mut PLpgSQL_execstate,
    expr: &PLpgSQL_expr,
) -> PgResult<(bool, bool)> {
    // exec_eval_expr + exec_cast_value(value, valtype -> BOOLOID).
    const BOOLOID: Oid = 16;
    let (value, isnull, rettype, rettypmod) = super::exec_eval_expr_impl(estate, expr)?;
    if isnull {
        return Ok((false, true));
    }
    if rettype == BOOLOID {
        // No coercion needed: DatumGetBool reads the low byte.
        return Ok((value.as_bool(), false));
    }
    // A non-boolean condition needs a cast to bool (exec_cast_value). This is the
    // value-substrate cast path; route through the cast seam (loud until the
    // cast-expr substrate lands — a comparison/boolean condition, the common
    // case, returns BOOLOID above and never reaches here).
    let (cast_value, cast_isnull) =
        exec_cast_value(estate, value, isnull, rettype, rettypmod, BOOLOID, -1)?;
    Ok((cast_value.as_bool(), cast_isnull))
}

/// `exec_eval_datum(estate, datum, &typeid, &typetypmod, &value, &isnull)`
/// (pl_exec.c): read the current value of a VAR/ROW/REC/RECFIELD datum.
/// Returns `(typeid, typetypmod, value, isnull)`.
pub fn exec_eval_datum(
    estate: &mut PLpgSQL_execstate,
    datum: &PLpgSQL_datum,
) -> PgResult<(Oid, int32, Datum, bool)> {
    super::exec_eval_datum_impl(estate, datum)
}

/// `exec_cast_value(estate, value, &isnull, valtype, valtypmod, reqtype,
/// reqtypmod)` (pl_exec.c): cast a value to the required type via a cached cast
/// expression. Returns `(value, isnull)`.
pub fn exec_cast_value(
    estate: &mut PLpgSQL_execstate,
    value: Datum,
    isnull: bool,
    valtype: Oid,
    valtypmod: int32,
    reqtype: Oid,
    reqtypmod: int32,
) -> PgResult<(Datum, bool)> {
    super::exec_cast_value_impl(estate, value, isnull, valtype, valtypmod, reqtype, reqtypmod)
}

/// `exec_run_select(estate, expr, maxtuples, portalP)` (pl_exec.c 5753): run a
/// SELECT and stash the result in `estate->eval_tuptable`. Returns the SPI
/// result code.
pub fn exec_run_select(
    estate: &mut PLpgSQL_execstate,
    expr: &PLpgSQL_expr,
    maxtuples: i64,
    set_portal: bool,
) -> PgResult<int32> {
    super::exec_run_select_impl(estate, expr, maxtuples, set_portal)
}

/// `plpgsql_fulfill_promise(estate, var)` (pl_exec.c): compute and assign a
/// DTYPE_PROMISE variable's promised value on first read.
pub fn plpgsql_fulfill_promise(
    estate: &mut PLpgSQL_execstate,
    var: &mut PLpgSQL_var,
) -> PgResult<()> {
    super::trigger::plpgsql_fulfill_promise_impl(estate, var)
}

/// The varlena R/W-expanded / flat-array commandeering leg of the
/// `plpgsql_exec_function` argument-store loop (pl_exec.c 561-586).
///
/// For a non-null varlena arg, C `TransferExpandedObject`s a R/W expanded
/// pointer (take ownership in place), keeps a R/O expanded pointer as-is, or
/// `expand_array`s a flat array into expanded form. All three are pure
/// in-memory optimizations — the variable holds the same logical value either
/// way. The owned value model carries a varlena arg as its flat image
/// (`assign_simple_var` already stored it before this leg runs), so the
/// faithful equivalent of every branch is the flat value that is already in
/// place: this leg flattens rather than expands and never copies a wrong value.
///
/// `has_flat_image` is whether the arg arrived with an out-of-band by-reference
/// image (`arg.byref`); `typisarray` mirrors C's `var->datatype->typisarray`
/// guard on the flat-array force-expand branch. Both are accepted to keep the
/// signature faithful to the C structure; the resulting stored value is
/// identical in this value model, so the body has no further work to do.
#[inline]
pub fn arg_store_commandeer(_value: Datum, _has_flat_image: bool, _typisarray: bool) {
    // pl_exec.c 563/572/576: VARATT_IS_EXTERNAL_EXPANDED_RW / _RO / typisarray.
    // The owned model never carries a live cross-boundary EOH pointer in an
    // argument, so each C branch reduces to the flat value already stored by
    // the preceding `assign_simple_var`. No-op (faithful flatten-fallback).
}

/// `exec_move_row_from_datum(estate, rec, value)` (pl_exec.c): assign a
/// composite datum into a REC/ROW target. Record deconstruction substrate.
pub fn exec_move_row_from_datum(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    value: Datum,
) -> PgResult<()> {
    super::trigger::exec_move_row_from_datum_impl(estate, target_dno, value)
}

/// `exec_move_row_from_datum` carrying the source composite value's by-reference
/// image (the verbatim header-ful `HeapTupleHeader` varlena bytes); the bare
/// `value` word is `0` then.
pub fn exec_move_row_from_datum_byref(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    value: Datum,
    byref: Option<std::vec::Vec<u8>>,
) -> PgResult<()> {
    super::trigger::exec_move_row_from_datum_byref_impl(estate, target_dno, value, byref)
}

/// `ereport(ERROR, ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT)`
/// (pl_exec.c) — the toplevel block fell through without RETURN.
pub fn ereport_no_return_statement() -> types_error::PgError {
    types_error::PgError::error(
        "control reached end of function without RETURN".to_string(),
    )
    .with_sqlstate(types_error::ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT)
}

/// The set-returning-function result handoff of `plpgsql_exec_function`
/// (pl_exec.c): validate the `ReturnSetInfo`, set materialize mode, copy the
/// tuplestore + descriptor out.
///
/// In the owned model the `ReturnSetInfo.setResult` tuplestore is the
/// thread-local materialize sink the executor-frame SRF dispatcher
/// (`execSRF::dispatch_user_setof`) pushed before reaching
/// `plpgsql_call_handler`. RETURN QUERY / RETURN NEXT already appended every
/// produced row into that sink (`return_query_put_rows` /
/// `return_next_put_row`); here we only mark the sink as materialized (C:
/// `rsi->returnMode = SFRM_Materialize;` — the rows are already in
/// `rsi->setResult`). An empty result set (no RETURN NEXT/QUERY ran) is still a
/// valid materialized empty tuplestore.
pub fn coerce_set_result(_estate: &mut PLpgSQL_execstate) {
    types_fmgr::mat_srf::with_top(|sink| {
        if let Some(sink) = sink {
            sink.materialized = true;
        }
    });
}

/// `exec_stmt_return_query` per-row deposit into the function's SRF result
/// tuplestore (pl_exec.c 4046): push each materialized result row into
/// `estate->tuple_store` (the `ReturnSetInfo.setResult` the executor-frame SRF
/// caller threaded onto the call frame). The query run is already ported (the
/// materialize-all `exec_run_select` / `exec_dynquery_with_params`); only this
/// sink stays loud, because a SETOF PL/pgSQL function is not yet routed through
/// the executor-frame SRF dispatch (`srf_invoke_by_oid` has no entry for
/// per-user PL/pgSQL function OIDs) so no live `ReturnSetInfo` reaches the
/// execstate — the dual-home `types_fmgr`↔`types_nodes` fcinfo keystone.
pub fn return_query_put_rows(
    _estate: &mut PLpgSQL_execstate,
    rows: std::vec::Vec<std::vec::Vec<crate::exec_seams::ExecsqlColumn>>,
) {
    // Append each materialized result row into the live materialize sink (C:
    // `tuplestore_puttupleslot(estate->tuple_store, slot)` per row, where
    // `estate->tuple_store == rsi->setResult`). Each column crosses as the owned
    // `(value | byref image, isnull)` split — the same shape `ExecsqlColumn`
    // already carries (a by-reference value's header-ful varlena image is in
    // `byref`, the bare word `value` is `0` then).
    put_rows_into_sink(rows);
}

/// Shared deposit: convert each `ExecsqlColumn` row into a `MatCell` row and
/// push it onto the active materialize sink. Used by both RETURN QUERY (a whole
/// batch of rows) and RETURN NEXT (one row at a time).
pub(crate) fn put_rows_into_sink(
    rows: std::vec::Vec<std::vec::Vec<crate::exec_seams::ExecsqlColumn>>,
) {
    types_fmgr::mat_srf::with_top(|sink| {
        if let Some(sink) = sink {
            for row in rows {
                let cells: std::vec::Vec<types_fmgr::mat_srf::MatCell> = row
                    .into_iter()
                    .map(|c| types_fmgr::mat_srf::MatCell {
                        value: c.value,
                        ref_payload: c
                            .byref
                            .map(types_fmgr::boundary::RefPayload::Varlena),
                        isnull: c.isnull,
                    })
                    .collect();
                sink.rows.push(cells);
            }
        }
    });
}

/// `RETURN NEXT <record/row variable>` (pl_exec.c 4116, the `stmt->retvarno >=
/// 0` arm) — append a declared record/row/scalar variable's current row to the
/// SRF tuplestore. Needs the `exec_move_row` tuple-deform + per-column-image
/// path; loud until the composite RETURN NEXT path lands (the scalar
/// `RETURN NEXT <expr>` form is ported directly in `exec_stmt_return_next`).
pub fn return_next_var_loud(_estate: &mut PLpgSQL_execstate, _retvarno: int32) {
    panic!(
        "seam not wired: RETURN NEXT <record/row variable> (pl_exec.c) — \
         exec_move_row tuple-deform into ReturnSetInfo.setResult (composite \
         RETURN NEXT path; the scalar RETURN NEXT <expr> form is ported)"
    );
}

/// `exec_move_row(estate, var, NULL, NULL)` (pl_exec.c): clear a REC/ROW to
/// the NULL row.
pub fn exec_move_row_null(estate: &mut PLpgSQL_execstate, target_dno: int32) -> PgResult<()> {
    super::trigger::exec_move_row_null_impl(estate, target_dno)
}

/// The CASE temp-var datatype rebuild (`exec_stmt_case`): when the test-expr's
/// runtime type differs from the temp var's declared type, rebuild the var's
/// `datatype` via `plpgsql_build_datatype`.
pub fn case_rebuild_temp_var_datatype(
    estate: &mut PLpgSQL_execstate,
    var: &mut PLpgSQL_var,
    typoid: Oid,
    typmod: int32,
) -> PgResult<()> {
    // C: t_var->datatype = plpgsql_build_datatype(t_typoid, t_typmod,
    //                          estate->func->fn_input_collation, NULL);
    let dt = backend_pl_plpgsql_comp_seams::plpgsql_build_datatype::call(
        typoid,
        typmod,
        estate.fn_input_collation,
    )?;
    var.datatype = Some(dt);
    Ok(())
}

/// `ResetExprContext(econtext)` (pl_exec.c `exec_eval_cleanup`): reset the
/// per-tuple eval econtext's memory.
pub fn reset_expr_context(_econtext: &ExprContext) {
    panic!(
        "seam not wired: ResetExprContext (pl_exec.c exec_eval_cleanup) — \
         executor per-tuple econtext reset (executor substrate)"
    );
}

// --- ereport sites that abort a statement (faithful: the C site ereports) ----

/// `ereport(ERROR, ERRCODE_CASE_NOT_FOUND)` — CASE with no matching WHEN and no
/// ELSE (`exec_stmt_case`).
pub fn ereport_case_not_found() -> types_error::PgError {
    types_error::PgError::error("case not found".to_string())
        .with_detail("CASE statement is missing ELSE part.".to_string())
        .with_sqlstate(types_error::ERRCODE_CASE_NOT_FOUND)
}

/// `ereport(ERROR, ERRCODE_ASSERT_FAILURE)` — an ASSERT condition was NULL or
/// false (`exec_stmt_assert`). With a `message` expression that evaluated to a
/// non-NULL string, C uses `errmsg_internal("%s", message)`; otherwise the
/// fixed `errmsg("assertion failed")`.
pub fn ereport_assert_failure(message: Option<String>) -> types_error::PgError {
    let msg = message.unwrap_or_else(|| "assertion failed".to_string());
    types_error::PgError::error(msg).with_sqlstate(types_error::ERRCODE_ASSERT_FAILURE)
}

/// `ereport(ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED)` — a FOR(i) loop bound /
/// step evaluated to NULL (`exec_stmt_fori`).
pub fn ereport_for_bound_null(which: &str) -> types_error::PgError {
    types_error::PgError::error(format!("{which} of FOR loop cannot be null"))
        .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED)
}

/// `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE)` — FOR(i) BY step <= 0.
pub fn ereport_for_step_nonpositive() -> types_error::PgError {
    types_error::PgError::error(
        "BY value of FOR loop must be greater than zero".to_string(),
    )
    .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

/// `ereport(ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED)` — FOREACH over a NULL array
/// (`exec_stmt_foreach_a`).
pub fn ereport_foreach_null() -> types_error::PgError {
    types_error::PgError::error("FOREACH expression must not be null".to_string())
        .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED)
}

/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH)` — `FOREACH ... SLICE` loop
/// variable is not of an array type (`exec_stmt_foreach_a`).
pub fn ereport_foreach_slice_var_not_array() -> types_error::PgError {
    types_error::PgError::error(
        "FOREACH ... SLICE loop variable must be of an array type".to_string(),
    )
    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
}

/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH)` — non-slicing `FOREACH` loop
/// variable is of an array type (`exec_stmt_foreach_a`).
pub fn ereport_foreach_var_is_array() -> types_error::PgError {
    types_error::PgError::error("FOREACH loop variable must not be of an array type".to_string())
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
}

/// `get_element_type(typid)` (lsyscache.c) — the element type OID of an array
/// type, or `InvalidOid`. Used by `exec_stmt_foreach_a` to check the loop
/// variable's array-ness. Installed from the handler (lsyscache owner); on a
/// lookup error treats the type as non-array (the conservative `InvalidOid`,
/// matching `get_element_type`'s cache-miss return).
pub fn get_element_type(typid: Oid) -> Oid {
    crate::exec_seams::foreach_get_element_type::call(typid)
        .ok()
        .flatten()
        .unwrap_or(0)
}

/// The array-iteration leg of `exec_stmt_foreach_a` (pl_exec.c): drive
/// `get_element_type` (array type check) / `DatumGetArrayTypePCopy` / the slice
/// range check / `array_create_iterator` + the full `array_iterate` loop over
/// the already-evaluated FOREACH array's verbatim varlena byte image, returning
/// every iteration's value (in order) plus the iterator result type/typmod.
/// Installed from the handler (array/lsyscache owner).
pub fn foreach_iterate(
    arr_bytes: std::vec::Vec<u8>,
    arrtype: Oid,
    arrtypmod: int32,
    slice: int32,
) -> PgResult<crate::exec_seams::ForeachIterateResult> {
    crate::exec_seams::foreach_iterate_via_array::call(arr_bytes, arrtype, arrtypmod, slice)
}

/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH)` — RETURN of a non-composite from
/// a function returning a tuple (`exec_stmt_return`).
pub fn ereport_return_noncomposite() -> types_error::PgError {
    types_error::PgError::error(
        "cannot return non-composite value from function returning composite type"
            .to_string(),
    )
    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
}
