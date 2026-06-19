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

/// `assign_simple_var(estate, var, newvalue, isnull, freeable)` (pl_exec.c
/// 8770): assign to a "simple" variable's value/isnull, freeing the old value
/// and updating any promise / shared-datum bookkeeping. Touches the runtime
/// Datum store + `exec_eval_using_params`/expanded-datum transfer substrate.
pub fn assign_simple_var(
    _estate: &mut PLpgSQL_execstate,
    _var: &mut PLpgSQL_var,
    _newvalue: Datum,
    _isnull: bool,
    _freeable: bool,
) {
    panic!(
        "seam not wired: assign_simple_var (pl_exec.c) — runtime Datum store + \
         datumTransfer / expanded-datum bookkeeping (value substrate)"
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
    _estate: &mut PLpgSQL_execstate,
    _expr: &PLpgSQL_expr,
) -> (Datum, bool, Oid, int32) {
    panic!(
        "seam not wired: exec_eval_expr (pl_exec.c) — exec_eval_simple_expr \
         (executor ExprState/ExprEvalStep #165/#324) / exec_run_select (SPI plan surface)"
    );
}

/// `exec_eval_boolean(estate, expr, &isNull)` (pl_exec.c 5642): evaluate a
/// boolean condition expression. Returns `(value, isnull)`.
pub fn exec_eval_boolean(_estate: &mut PLpgSQL_execstate, _expr: &PLpgSQL_expr) -> (bool, bool) {
    panic!(
        "seam not wired: exec_eval_boolean (pl_exec.c) — exec_eval_expr + \
         exec_cast_value to BOOLOID (executor ExprState #165/#324)"
    );
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
    panic!("case not found (ERRCODE_CASE_NOT_FOUND): CASE statement is missing ELSE part");
}

/// `ereport(ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED)` — a FOR(i) loop bound /
/// step evaluated to NULL (`exec_stmt_fori`).
pub fn ereport_for_bound_null(which: &str) -> ! {
    panic!("{which} of FOR loop cannot be null (ERRCODE_NULL_VALUE_NOT_ALLOWED)");
}

/// `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE)` — FOR(i) BY step <= 0.
pub fn ereport_for_step_nonpositive() -> ! {
    panic!("BY value of FOR loop must be greater than zero (ERRCODE_INVALID_PARAMETER_VALUE)");
}

/// `ereport(ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED)` — FOREACH over a NULL array
/// (`exec_stmt_foreach_a`).
pub fn ereport_foreach_null() -> ! {
    panic!("FOREACH expression must not be null (ERRCODE_NULL_VALUE_NOT_ALLOWED)");
}

/// `ereport(ERROR, ERRCODE_DATATYPE_MISMATCH)` — RETURN of a non-composite from
/// a function returning a tuple (`exec_stmt_return`).
pub fn ereport_return_noncomposite() -> ! {
    panic!(
        "cannot return non-composite value from function returning composite type \
         (ERRCODE_DATATYPE_MISMATCH)"
    );
}
