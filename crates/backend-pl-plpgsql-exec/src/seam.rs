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
/// and assign into `target`. Needs the executor simple-expr / SPI eval path.
pub fn exec_assign_expr(_estate: &mut PLpgSQL_execstate, _target_dno: int32, _expr: &PLpgSQL_expr) {
    panic!(
        "seam not wired: exec_assign_expr (pl_exec.c) — exec_eval_expr + \
         exec_assign_value (executor ExprState #165/#324 + value substrate)"
    );
}

/// `exec_assign_value(estate, target, value, isNull, valtype, valtypmod)`
/// (pl_exec.c 5061): the generic datum-assignment dispatch (VAR/ROW/REC/
/// RECFIELD), with cast + array-element / record-field update legs.
pub fn exec_assign_value(
    _estate: &mut PLpgSQL_execstate,
    _target_dno: int32,
    _value: Datum,
    _isnull: bool,
    _valtype: Oid,
    _valtypmod: int32,
) {
    panic!(
        "seam not wired: exec_assign_value (pl_exec.c) — exec_cast_value + \
         array_set_element / expanded-record field set (fmgr + value substrate)"
    );
}

/// `exec_eval_expr(estate, expr, &isNull, &rettype, &rettypmod)` (pl_exec.c
/// 5665): evaluate a PL/pgSQL expression, via the simple-expr fast path or a
/// one-row SPI select. Returns `(value, isnull, rettype, rettypmod)`.
pub fn exec_eval_expr(
    estate: &mut PLpgSQL_execstate,
    expr: &PLpgSQL_expr,
) -> (Datum, bool, Oid, int32) {
    super::exec_eval_expr_impl(estate, expr)
}

/// `exec_eval_boolean(estate, expr, &isNull)` (pl_exec.c 5642): evaluate a
/// boolean condition expression. Returns `(value, isnull)`.
pub fn exec_eval_boolean(estate: &mut PLpgSQL_execstate, expr: &PLpgSQL_expr) -> (bool, bool) {
    // exec_eval_expr + exec_cast_value(value, valtype -> BOOLOID).
    const BOOLOID: Oid = 16;
    let (value, isnull, rettype, rettypmod) = super::exec_eval_expr_impl(estate, expr);
    if isnull {
        return (false, true);
    }
    if rettype == BOOLOID {
        // No coercion needed: DatumGetBool reads the low byte.
        return (value.as_bool(), false);
    }
    // A non-boolean condition needs a cast to bool (exec_cast_value). This is the
    // value-substrate cast path; route through the cast seam (loud until the
    // cast-expr substrate lands — a comparison/boolean condition, the common
    // case, returns BOOLOID above and never reaches here).
    let (cast_value, cast_isnull) =
        exec_cast_value(estate, value, isnull, rettype, rettypmod, BOOLOID, -1);
    (cast_value.as_bool(), cast_isnull)
}

/// `exec_eval_datum(estate, datum, &typeid, &typetypmod, &value, &isnull)`
/// (pl_exec.c): read the current value of a VAR/ROW/REC/RECFIELD datum.
/// Returns `(typeid, typetypmod, value, isnull)`.
pub fn exec_eval_datum(
    _estate: &mut PLpgSQL_execstate,
    _datum: &PLpgSQL_datum,
) -> (Oid, int32, Datum, bool) {
    panic!(
        "seam not wired: exec_eval_datum (pl_exec.c) — composite/expanded-record \
         deconstruction + heap_getattr (value substrate)"
    );
}

/// `exec_cast_value(estate, value, &isnull, valtype, valtypmod, reqtype,
/// reqtypmod)` (pl_exec.c): cast a value to the required type via a cached cast
/// expression. Returns `(value, isnull)`.
pub fn exec_cast_value(
    _estate: &mut PLpgSQL_execstate,
    _value: Datum,
    _isnull: bool,
    _valtype: Oid,
    _valtypmod: int32,
    _reqtype: Oid,
    _reqtypmod: int32,
) -> (Datum, bool) {
    panic!(
        "seam not wired: exec_cast_value (pl_exec.c) — get_cast_hashentry + \
         ExecEvalExpr over the cast expression (executor + fmgr substrate)"
    );
}

/// `exec_run_select(estate, expr, maxtuples, portalP)` (pl_exec.c 5753): run a
/// SELECT and stash the result in `estate->eval_tuptable`. Returns the SPI
/// result code.
pub fn exec_run_select(
    _estate: &mut PLpgSQL_execstate,
    _expr: &PLpgSQL_expr,
    _maxtuples: i64,
    _set_portal: bool,
) -> int32 {
    panic!(
        "seam not wired: exec_run_select (pl_exec.c) — exec_prepare_plan + \
         SPI_execute_plan_extended (SPI plan surface not installed)"
    );
}

/// `plpgsql_fulfill_promise(estate, var)` (pl_exec.c): compute and assign a
/// DTYPE_PROMISE variable's promised value on first read.
pub fn plpgsql_fulfill_promise(_estate: &mut PLpgSQL_execstate, _var: &mut PLpgSQL_var) {
    panic!(
        "seam not wired: plpgsql_fulfill_promise (pl_exec.c) — promise value \
         computation (trigger/SRF context substrate) + assign_simple_var"
    );
}

/// The varlena R/W-expanded / flat-array commandeering leg of the
/// `plpgsql_exec_function` argument-store loop (pl_exec.c): for a non-null
/// varlena arg, take ownership of a R/W expanded object or force a flat array
/// into expanded form. `TransferExpandedObject` / `expand_array` are the
/// expanded-object value substrate.
pub fn arg_store_expanded_object(_value: Datum) {
    panic!(
        "seam not wired: plpgsql_exec_function varlena arg commandeer (pl_exec.c) — \
         TransferExpandedObject / expand_array (expanded-object value substrate)"
    );
}

/// `exec_move_row_from_datum(estate, rec, value)` (pl_exec.c): assign a
/// composite datum into a REC/ROW target. Record deconstruction substrate.
pub fn exec_move_row_from_datum(
    _estate: &mut PLpgSQL_execstate,
    _target_dno: int32,
    _value: Datum,
) {
    panic!(
        "seam not wired: exec_move_row_from_datum (pl_exec.c) — composite/expanded-record \
         deconstruction + assign (value substrate)"
    );
}

/// `ereport(ERROR, ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT)`
/// (pl_exec.c) — the toplevel block fell through without RETURN.
pub fn ereport_no_return_statement() -> ! {
    std::panic::panic_any(
        types_error::PgError::error(
            "control reached end of function without RETURN".to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
    );
}

/// The set-returning-function result handoff of `plpgsql_exec_function`
/// (pl_exec.c): validate the `ReturnSetInfo`, set materialize mode, copy the
/// tuplestore + descriptor out. SRF / tuplestore / executor substrate.
pub fn coerce_set_result(_estate: &mut PLpgSQL_execstate) {
    panic!(
        "seam not wired: plpgsql_exec_function SETOF result (pl_exec.c) — \
         ReturnSetInfo materialize-mode + tuplestore handoff (SRF/executor substrate)"
    );
}

/// The composite-result coercion leg of `plpgsql_exec_function` (pl_exec.c):
/// coerce a tuple result to the declared rowtype, handling dropped columns, and
/// copy it out to the upper executor context. Tupdesc/heaptuple value substrate.
pub fn coerce_function_result_tuple(_estate: &mut PLpgSQL_execstate) {
    panic!(
        "seam not wired: plpgsql_exec_function tuple result coercion (pl_exec.c) — \
         coerce_function_result_tuple / CreateTupleDescCopy + datumCopy (tupdesc/value substrate)"
    );
}

/// `exec_move_row(estate, var, NULL, NULL)` (pl_exec.c): clear a REC/ROW to
/// the NULL row.
pub fn exec_move_row_null(_estate: &mut PLpgSQL_execstate, _target_dno: int32) {
    panic!(
        "seam not wired: exec_move_row (pl_exec.c, NULL row) — record/row \
         deconstruction + assign (value substrate)"
    );
}

/// The CASE temp-var datatype rebuild (`exec_stmt_case`): when the test-expr's
/// runtime type differs from the temp var's declared type, rebuild the var's
/// `datatype` via `plpgsql_build_datatype`.
pub fn case_rebuild_temp_var_datatype(
    _estate: &mut PLpgSQL_execstate,
    _var: &mut PLpgSQL_var,
    _typoid: Oid,
    _typmod: int32,
) {
    panic!(
        "seam not wired: exec_stmt_case temp-var datatype rebuild (pl_exec.c) — \
         plpgsql_build_datatype (compiler datatype builder over typcache)"
    );
}

/// `ResetExprContext(econtext)` (pl_exec.c `exec_eval_cleanup`): reset the
/// per-tuple eval econtext's memory.
pub fn reset_expr_context(_econtext: &ExprContext) {
    panic!(
        "seam not wired: ResetExprContext (pl_exec.c exec_eval_cleanup) — \
         executor per-tuple econtext reset (executor substrate)"
    );
}

/// `type_is_rowtype(rettype)` (lsyscache.c, via `exec_stmt_return`): is the
/// type a composite/row type?
pub fn type_is_rowtype(_typeid: Oid) -> bool {
    panic!(
        "seam not wired: type_is_rowtype (lsyscache.c) — composite-type test \
         for RETURN of a non-composite from a SETOF/composite function"
    );
}

// --- ereport sites that abort a statement (faithful: the C site ereports) ----

/// `ereport(ERROR, ERRCODE_CASE_NOT_FOUND)` — CASE with no matching WHEN and no
/// ELSE (`exec_stmt_case`).
pub fn ereport_case_not_found() -> ! {
    std::panic::panic_any(
        types_error::PgError::error("case not found".to_string())
            .with_detail("CASE statement is missing ELSE part.".to_string())
            .with_sqlstate(types_error::ERRCODE_CASE_NOT_FOUND),
    );
}

/// `ereport(ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED)` — a FOR(i) loop bound /
/// step evaluated to NULL (`exec_stmt_fori`).
pub fn ereport_for_bound_null(which: &str) -> ! {
    std::panic::panic_any(
        types_error::PgError::error(format!("{which} of FOR loop cannot be null"))
            .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
    );
}

/// `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE)` — FOR(i) BY step <= 0.
pub fn ereport_for_step_nonpositive() -> ! {
    std::panic::panic_any(
        types_error::PgError::error(
            "BY value of FOR loop must be greater than zero".to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    );
}

/// `ereport(ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED)` — FOREACH over a NULL array
/// (`exec_stmt_foreach_a`).
pub fn ereport_foreach_null() -> ! {
    std::panic::panic_any(
        types_error::PgError::error("FOREACH expression must not be null".to_string())
            .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
    );
}

/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH)` — RETURN of a non-composite from
/// a function returning a tuple (`exec_stmt_return`).
pub fn ereport_return_noncomposite() -> ! {
    std::panic::panic_any(
        types_error::PgError::error(
            "cannot return non-composite value from function returning composite type"
                .to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
    );
}
