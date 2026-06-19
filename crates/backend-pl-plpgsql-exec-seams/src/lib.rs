//! Seam declarations for the PL/pgSQL executor unit (`pl_exec.c`).
//!
//! The compiler (`pl_comp.c`, `backend-pl-plpgsql-comp`) calls back into the
//! executor at compile time from `make_datum_param`
//! (`plpgsql_exec_get_datum_type_info`) to learn the type/typmod/collation of a
//! `PLpgSQL_datum` so it can stamp a `Param` node. That callee lives in
//! `pl_exec.c` (this unit, `backend-pl-plpgsql-exec`), which depends on the
//! compiler — a cycle. The compiler therefore reaches it through this seam; the
//! executor unit installs it from its `init_seams()` when it lands. Until then
//! a call panics loudly (mirror-PG-and-panic).
//!
//! ## Modeling the C out-parameter contract
//!
//! ```c
//! void plpgsql_exec_get_datum_type_info(PLpgSQL_execstate *estate,
//!                                       PLpgSQL_datum *datum,
//!                                       Oid *typeId, int32 *typMod, Oid *collation);
//! ```
//!
//! The three out-parameters are returned as a [`DatumTypeInfo`] value. The
//! `estate`/`datum` pair is identified by the datum's `dno` against the live
//! execstate the compiler is currently building an expression for
//! (`expr->func->cur_estate`), which the executor owns; the seam carries the
//! `dno` plus the executor's opaque execstate handle.

use types_core::Oid;
use types_error::PgResult;
use types_plpgsql::int32;

/// The `(typeId, typMod, collation)` triple filled by
/// `plpgsql_exec_get_datum_type_info`.
#[derive(Clone, Copy, Debug)]
pub struct DatumTypeInfo {
    pub type_id: Oid,
    pub typmod: int32,
    pub collation: Oid,
}

seam_core::seam!(
    /// `plpgsql_exec_get_datum_type_info(estate, datum, &typeId, &typMod, &collation)`
    /// (`pl_exec.c`): report the type/typmod/collation of the datum identified
    /// by `dno` in the execstate identified by `estate_handle`
    /// (`expr->func->cur_estate`). The compiler calls this from
    /// `make_datum_param` while building a `Param` node.
    pub fn plpgsql_exec_get_datum_type_info(
        estate_handle: u64,
        dno: int32,
    ) -> PgResult<DatumTypeInfo>
);

/// One PL/pgSQL scalar datum value bound into a `Param` for expression
/// evaluation: the bare-word value, its is-null flag, and its type OID
/// (`estate->datums[dno]` projected to what `setup_param_list` binds).
#[derive(Clone, Copy, Debug)]
pub struct EvalParamValue {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
}

/// The raw result of evaluating a PL/pgSQL expression to a single value (the
/// first row's first column, `SPI_getbinval(tuptab->vals[0], tupdesc, 1)`).
#[derive(Clone, Copy, Debug)]
pub struct EvalExprResult {
    /// The bare-word result datum (`0` when null).
    pub value: usize,
    pub isnull: bool,
    /// The result column's type OID (`SPI_gettypeid(tupdesc, 1)`).
    pub typeid: Oid,
    /// `SPI_processed` — the number of rows the expression produced.
    pub processed: u64,
}

// SPI result codes (`spi.h`) the execsql bridge returns, mirrored here so the
// executor unit can classify the result without depending on the SPI crate.
pub const SPI_ERROR_COPY: int32 = -2;
pub const SPI_OK_UTILITY: int32 = 4;
pub const SPI_OK_SELECT: int32 = 5;
pub const SPI_OK_SELINTO: int32 = 6;
pub const SPI_OK_INSERT: int32 = 7;
pub const SPI_OK_DELETE: int32 = 8;
pub const SPI_OK_UPDATE: int32 = 9;
pub const SPI_OK_INSERT_RETURNING: int32 = 11;
pub const SPI_OK_DELETE_RETURNING: int32 = 12;
pub const SPI_OK_UPDATE_RETURNING: int32 = 13;
pub const SPI_OK_REWRITTEN: int32 = 14;

/// The assembled fields of the `ereport` `exec_stmt_raise` throws (`pl_exec.c`):
/// the elog level, the (possibly-zero) packed SQLSTATE, the primary message, and
/// the optional DETAIL / HINT / COLUMN / CONSTRAINT / DATATYPE / TABLE / SCHEMA
/// strings. Lowered to a single `ereport` call by the owner (`elog.c`), which
/// emits a `NOTICE`/`WARNING`/`INFO`/`LOG` to the client or throws for `ERROR`.
#[derive(Clone, Debug, Default)]
pub struct RaiseEreport {
    pub elog_level: int32,
    pub err_code: int32,
    pub message: std::string::String,
    pub detail: Option<std::string::String>,
    pub hint: Option<std::string::String>,
    pub column: Option<std::string::String>,
    pub constraint: Option<std::string::String>,
    pub datatype: Option<std::string::String>,
    pub table: Option<std::string::String>,
    pub schema: Option<std::string::String>,
}

seam_core::seam!(
    /// The final `ereport(stmt->elog_level, ...)` of `exec_stmt_raise`
    /// (`pl_exec.c`): emit the assembled report. For a non-`ERROR` level this
    /// reports a message to the client and returns `Ok(())`; for `ERROR` it
    /// raises (returns `Err`). The executor (`pl_exec.c`, this unit) reaches the
    /// elog report cycle (`ThrowErrorData`) through the owner, which the handler
    /// installs.
    pub fn raise_ereport(report: RaiseEreport) -> PgResult<()>
);

seam_core::seam!(
    /// `convert_value_to_string(estate, value, valtype)` (`pl_exec.c`):
    /// `getTypeOutputInfo(valtype)` + `OidOutputFunctionCall(typoutput, value)` —
    /// render a datum to its external text representation (the `%` substitution
    /// in `RAISE`, the `USING` option text). The executor (`pl_exec.c`, this
    /// unit) is layered below the fmgr/lsyscache output-function path, so the
    /// handler installs this from its `init_seams()`. `value` is the bare-word
    /// datum (`0` when the caller already screened NULL); the result is the
    /// NUL-excluded output bytes as an owned `String`.
    pub fn convert_value_to_string(value: usize, valtype: Oid) -> PgResult<std::string::String>
);

seam_core::seam!(
    /// `plpgsql_recognize_err_condition(condname, allow_sqlstate)` (`pl_comp.c`):
    /// translate a condition name (or, when `allow_sqlstate`, a 5-char SQLSTATE)
    /// to its packed `int32` SQLSTATE. The executor needs this from
    /// `exec_stmt_raise` (the `condname` leg and the `ERRCODE` USING option); the
    /// real body lives in the compiler unit (`backend-pl-plpgsql-comp`), so the
    /// handler bridges to it. `Err` carries the `unrecognized exception
    /// condition` ereport.
    pub fn recognize_err_condition(
        condname: std::string::String,
        allow_sqlstate: bool,
    ) -> PgResult<int32>
);

/// One INTO-target field the execsql / SELECT-INTO store leg fills (`exec_move_row`
/// into a scalar `PLpgSQL_row` field): the target datum number plus the variable's
/// required `(type, typmod)` for the per-column cast.
#[derive(Clone, Copy, Debug)]
pub struct ExecsqlIntoField {
    pub dno: int32,
    pub reqtype: Oid,
    pub reqtypmod: int32,
}

/// One column value the execsql SELECT-INTO leg returns for a result row: the
/// bare-word datum, its is-null flag, and the source column type/typmod (so the
/// caller can cast it into the target variable).
#[derive(Clone, Copy, Debug)]
pub struct ExecsqlColumn {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
    pub typmod: int32,
}

/// The raw result of running an embedded SQL statement (`exec_stmt_execsql` via
/// `SPI_execute_plan_with_paramlist`): the SPI return code (`SPI_OK_*`), the
/// `SPI_processed` row count, whether a tuple table was produced, and — when an
/// INTO was requested — the first result row's columns (already capped to the
/// requested tuple count, so the caller can detect the no-rows / too-many-rows
/// cases from `processed`).
#[derive(Clone, Debug)]
pub struct ExecsqlResult {
    pub code: int32,
    pub processed: u64,
    pub returned_tuptable: bool,
    /// The first result row's column values (empty when no row, or when the
    /// statement produced no tuple table). Only read when `into` was set.
    pub first_row: std::vec::Vec<ExecsqlColumn>,
}

seam_core::seam!(
    /// `exec_stmt_execsql(estate, stmt)` core (`pl_exec.c`'s
    /// `exec_prepare_plan` + `setup_param_list` +
    /// `SPI_execute_plan_with_paramlist`): prepare the embedded SQL `query` (in
    /// its `parse_mode`, with the PL/pgSQL parser hooks installed so variable
    /// barewords resolve to `$dno+1` `Param`s), bind the referenced scalar datums
    /// from `datum_snapshot`, run the statement under the transaction snapshot,
    /// and return the SPI result code + row count (+ the first row's columns when
    /// `into` is set). DML (INSERT/UPDATE/DELETE) and plain SELECT are handled;
    /// the `read_only` flag mirrors `estate->readonly_func`. `tcount` caps the
    /// row count (1 / 2 for INTO, 0 = run to completion).
    ///
    /// The executor (`pl_exec.c`, this unit) cannot reach the SPI plan surface
    /// directly (it is layered below SPI), so the SPI owner installs this from the
    /// handler's `init_seams()`.
    pub fn exec_execsql_via_spi(
        query: std::string::String,
        parse_mode: types_parsenodes::RawParseMode,
        parse_state: types_nodes::parsestmt::PlpgsqlExprParseState,
        datum_snapshot: std::vec::Vec<Option<EvalParamValue>>,
        read_only: bool,
        into: bool,
        tcount: i64,
    ) -> PgResult<ExecsqlResult>
);

seam_core::seam!(
    /// `exec_cast_value(estate, value, isnull, valtype, valtypmod, reqtype,
    /// reqtypmod)` slow path (`pl_exec.c`'s `do_cast_value` /
    /// `get_cast_hashentry` + `ExecEvalExpr` over the cached cast expression):
    /// coerce `value` from `(valtype, valtypmod)` to `(reqtype, reqtypmod)`. The
    /// executor reaches the coercion/executor substrate through the SPI owner;
    /// the handler installs it. `value` is the bare-word datum; the result is
    /// `(value, isnull)`. The no-op relabel case (`valtype == reqtype` and the
    /// typmod is unconstrained) is handled in-crate and never reaches here.
    pub fn exec_cast_value_via_spi(
        value: usize,
        isnull: bool,
        valtype: Oid,
        valtypmod: int32,
        reqtype: Oid,
        reqtypmod: int32,
    ) -> PgResult<(usize, bool)>
);

seam_core::seam!(
    /// `exec_eval_expr(estate, expr)` slow path (`pl_exec.c`'s `exec_run_select`):
    /// prepare the PL/pgSQL expression `query` (in its `parse_mode`) with the
    /// PL/pgSQL parser hooks installed (so variable barewords resolve to
    /// `$dno+1` `Param`s), bind the referenced scalar datums from
    /// `datum_snapshot` (indexed by `dno`), run the one-row SELECT, and return
    /// the first row's first-column raw datum.
    ///
    /// The executor (`pl_exec.c`, this unit) cannot reach the SPI plan surface
    /// directly (it is layered below SPI), so the SPI owner installs this from
    /// the handler's `init_seams()`. `datum_snapshot[dno]` carries the current
    /// `(value, isnull, typeid)` of each scalar datum (the
    /// `setup_param_list`/`plpgsql_param_fetch` value); a `None` entry is a
    /// non-scalar datum that cannot be a simple-expr Param.
    pub fn exec_eval_expr_via_spi(
        query: std::string::String,
        parse_mode: types_parsenodes::RawParseMode,
        parse_state: types_nodes::parsestmt::PlpgsqlExprParseState,
        datum_snapshot: std::vec::Vec<Option<EvalParamValue>>,
        maxtuples: i64,
    ) -> PgResult<EvalExprResult>
);

// ---------------------------------------------------------------------------
// `exec_stmt_block` EXCEPTION-leg substrate (keystone #215).
//
// The catchable-error channel of a `BEGIN ... EXCEPTION ... END` block runs the
// body inside an internal subtransaction. The xact entry points
// (`BeginInternalSubTransaction` / `ReleaseCurrentSubTransaction` /
// `RollbackAndReleaseCurrentSubTransaction`) live in
// `backend-access-transam-xact`; the executor (`pl_exec.c`, this unit) is
// layered below it, so the handler installs these from its `init_seams()`. They
// are thin delegations to the now-ported owners — no behavior is added here.
// (Modern PG dropped the explicit `SPI_restore_connection` after the abort:
// xact's `AbortSubTransaction` drives `AtEOSubXact_SPI` through its own seam.)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `BeginInternalSubTransaction(NULL)` (`xact.c`): start the internal
    /// subtransaction the EXCEPTION block body runs inside.
    pub fn begin_internal_subtransaction() -> PgResult<()>
);

seam_core::seam!(
    /// `ReleaseCurrentSubTransaction()` (`xact.c`): commit (RELEASE) the
    /// EXCEPTION block's internal subtransaction on the no-error path.
    pub fn release_current_subtransaction() -> PgResult<()>
);

seam_core::seam!(
    /// `RollbackAndReleaseCurrentSubTransaction()` (`xact.c`): roll back the
    /// EXCEPTION block's internal subtransaction when the body raised an error,
    /// popping back to the parent transaction state.
    pub fn rollback_and_release_current_subtransaction() -> PgResult<()>
);

seam_core::seam!(
    /// `CStringGetTextDatum(s)` (`builtins.h` / `varlena.c cstring_to_text`):
    /// build a `text` Datum from an owned Rust `String`, returned as the
    /// bare-word datum (`DatumGetPointer` view). Used by the EXCEPTION handler to
    /// bind the SQLSTATE and SQLERRM special variables (`assign_error_vars`) and
    /// by `exec_stmt_getdiag`. The result word points at a header-ful `text`
    /// varlena allocated in a backend-lifetime context (mirroring how
    /// `CStringGetTextDatum` palloc's in `CurrentMemoryContext`), so the bytes
    /// outlive the call and the caller stores the word straight into the target
    /// `text` variable. The executor is layered below the varlena substrate, so
    /// the handler installs it.
    pub fn cstring_to_text_datum(s: std::string::String) -> PgResult<usize>
);
