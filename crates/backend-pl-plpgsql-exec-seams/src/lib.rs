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
use types_resowner::ResourceOwner;

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
///
/// A pass-by-reference scalar datum (a `text`/`varchar`/`numeric` argument or
/// variable) carries its verbatim header-ful varlena / cstring byte image in
/// `byref` (the out-of-band companion to the bare `value` word, which is `0` in
/// that case); the SPI param-bind reconstructs a `Datum::ByRef` from it so the
/// image survives into the executed plan. `None` for a by-value datum.
#[derive(Clone, Debug)]
pub struct EvalParamValue {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
    pub byref: Option<std::vec::Vec<u8>>,
}

/// The raw result of evaluating a PL/pgSQL expression to a single value (the
/// first row's first column, `SPI_getbinval(tuptab->vals[0], tupdesc, 1)`).
#[derive(Clone, Debug)]
pub struct EvalExprResult {
    /// The bare-word result datum (`0` when null, or when the result is a
    /// pass-by-reference value carried in `byref`).
    pub value: usize,
    pub isnull: bool,
    /// `Some(image)` for a non-null pass-by-reference result: the verbatim
    /// header-ful varlena / cstring byte image (`datumCopy`'d out of the SPI
    /// arena), so a by-ref result (text/varchar/numeric/…) survives to the
    /// caller's result context. `None` for a by-value or NULL result.
    pub byref: Option<std::vec::Vec<u8>>,
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
pub const SPI_OK_MERGE: int32 = 18;
pub const SPI_OK_MERGE_RETURNING: int32 = 19;

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
    /// The PL/pgSQL error-context line(s) for the live exec stack, supplied for
    /// a non-`ERROR` level RAISE. C's `error_context_stack` callbacks fire at
    /// report time for every elevel; the owned model attaches context lazily on
    /// *propagation*, which covers `ERROR` but not a `NOTICE`/`WARNING` reported
    /// straight to the client. This carries the same lines so they appear in the
    /// non-`ERROR` message too (psql `SHOW_CONTEXT always`). Empty for `ERROR`
    /// (that path attaches on propagation, avoiding a duplicate).
    pub context: Option<std::string::String>,
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
    /// datum (`0` when the caller already screened NULL); for a pass-by-reference
    /// type (`text`/`varchar`/`numeric`/…) the bare word is `0` and the real
    /// referent is its varlena/cstring image, carried out-of-band in `byref`
    /// (C: the `Datum` *is* the pointer; the owned model materializes the image).
    /// The result is the NUL-excluded output bytes as an owned `String`.
    pub fn convert_value_to_string(
        value: usize,
        byref: std::option::Option<std::vec::Vec<u8>>,
        valtype: Oid,
    ) -> PgResult<std::string::String>
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
///
/// A pass-by-reference column (a `text`/`varchar`/`numeric` value fetched into a
/// scalar INTO target) carries its verbatim header-ful varlena / cstring byte
/// image in `byref` (the out-of-band companion to the bare `value` word, which
/// is `0` in that case); the INTO store reconstructs a `Datum::ByRef` from it so
/// the fetched image survives into the target variable. `None` for a by-value
/// column.
#[derive(Clone, Debug)]
pub struct ExecsqlColumn {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
    pub typmod: int32,
    /// `SPI_fname(tupdesc, i+1)` — the result column name (the field name a
    /// `SELECT ... INTO <record>` target resolves `r.<name>` against).
    pub name: std::string::String,
    pub byref: Option<std::vec::Vec<u8>>,
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

/// The raw result of running a query for the FOR-loop / RETURN QUERY iteration
/// path (`pl_exec.c`'s `exec_run_select` + `exec_for_query`): the SPI return
/// code, the row count, and **every** result row's columns (the materialize-all
/// analogue of C's portal-fetch loop — each row is iterated, in order).
#[derive(Clone, Debug)]
pub struct RunSelectResult {
    pub code: int32,
    pub processed: u64,
    pub returned_tuptable: bool,
    /// All result rows, each a vector of its columns.
    pub all_rows: std::vec::Vec<std::vec::Vec<ExecsqlColumn>>,
}

seam_core::seam!(
    /// `exec_run_select(estate, query, 0, NULL)` materialize-all path
    /// (`pl_exec.c`): prepare the embedded `query` (in its `parse_mode`, with the
    /// PL/pgSQL parser hooks installed), bind the referenced scalar datums from
    /// `datum_snapshot`, run it, and return **every** result row's columns for
    /// the FOR-loop / RETURN QUERY iteration (`exec_for_query`). Unlike the
    /// portal/cursor leg (`SPI_cursor_open` keystone), this materializes all rows
    /// up front; the observable iteration (every row, in order) is identical to
    /// C's batched portal fetch.
    ///
    /// The executor unit is layered below SPI and reaches the SPI plan surface
    /// through this seam; the handler (top layer, with SPI access) installs it.
    pub fn exec_run_select_via_spi(
        query: std::string::String,
        parse_mode: types_parsenodes::RawParseMode,
        parse_state: types_nodes::parsestmt::PlpgsqlExprParseState,
        datum_snapshot: std::vec::Vec<Option<EvalParamValue>>,
        read_only: bool,
        must_return_tuples: bool,
    ) -> PgResult<RunSelectResult>
);

/// One already-evaluated `USING` parameter of a dynamic `EXECUTE` (`pl_exec.c`'s
/// `exec_eval_using_params`): the bare-word value, its is-null flag, and its
/// resolved type OID. A pass-by-reference value carries its verbatim header-ful
/// varlena / cstring byte image in `byref` (the bare `value` word is `0` then),
/// reconstructed into a `Datum::ByRef` by the SPI param-bind; `None` for a
/// by-value param. The `$i+1` `Param` placeholder in the dynamic query binds to
/// `params[i]`.
#[derive(Clone, Debug)]
pub struct DynUsingParam {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
    pub byref: Option<std::vec::Vec<u8>>,
}

/// The raw result of running a dynamic `EXECUTE` query string (`pl_exec.c`'s
/// `exec_stmt_dynexecute` / `exec_dynquery_with_params` via
/// `SPI_execute_extended` / `SPI_cursor_parse_open`): the SPI result code, the
/// row count, whether a tuple table was produced, the first result row's columns
/// (for `EXECUTE ... INTO`), and **every** result row's columns (for
/// `FOR ... IN EXECUTE`). `first_row` is populated when `into` was requested;
/// `all_rows` when `collect_all` was requested.
#[derive(Clone, Debug)]
pub struct DynExecResult {
    pub code: int32,
    pub processed: u64,
    pub returned_tuptable: bool,
    pub first_row: std::vec::Vec<ExecsqlColumn>,
    pub all_rows: std::vec::Vec<std::vec::Vec<ExecsqlColumn>>,
}

seam_core::seam!(
    /// `exec_stmt_dynexecute` / `exec_dynquery_with_params` core (`pl_exec.c`):
    /// run the **dynamic** query string `query` (the runtime text after the
    /// `EXECUTE` keyword) as a one-shot, with the already-evaluated `USING`
    /// `params` (param id `$i+1`) bound as external params. The query is analyzed
    /// with NO PL/pgSQL parser hooks — a bareword does not resolve to a variable;
    /// only `$n` placeholders substitute (their types come from the `params`).
    /// `into` collects the first row (`EXECUTE ... INTO`); `collect_all` collects
    /// every row (`FOR ... IN EXECUTE`); `tcount` caps the row count (0 = run to
    /// completion). All command types — `SELECT`, DML, utility (DDL) — run.
    ///
    /// The executor (`pl_exec.c`, this unit) is layered below SPI and reaches the
    /// SPI one-shot plan surface through this seam; the handler installs it.
    pub fn exec_dynexecute_via_spi(
        query: std::string::String,
        params: std::vec::Vec<DynUsingParam>,
        read_only: bool,
        into: bool,
        collect_all: bool,
        tcount: i64,
        must_return_tuples: bool,
    ) -> PgResult<DynExecResult>
);

/// The coerced result of [`exec_cast_value_via_spi`]: the bare-word coerced
/// datum + its is-null flag, plus — when the target type is pass-by-reference
/// (`text`/`varchar`/`numeric`/…) — the coerced value's verbatim header-ful
/// varlena / cstring byte image (`datumCopy`'d out of the cast working context),
/// in which case the bare `value` word is `0`. `byref == None` for a by-value
/// result, where `value` is the scalar word.
#[derive(Clone, Debug)]
pub struct CastValueResult {
    pub value: usize,
    pub isnull: bool,
    pub byref: Option<std::vec::Vec<u8>>,
}

seam_core::seam!(
    /// `exec_cast_value(estate, value, isnull, valtype, valtypmod, reqtype,
    /// reqtypmod)` slow path (`pl_exec.c`'s `do_cast_value` /
    /// `get_cast_hashentry` + `ExecEvalExpr` over the cached cast expression):
    /// coerce `value` from `(valtype, valtypmod)` to `(reqtype, reqtypmod)`. The
    /// executor reaches the coercion/executor substrate through the SPI owner;
    /// the handler installs it. `value` is the bare-word datum and `value_byref`
    /// its verbatim by-reference image when the source is a pass-by-reference
    /// type (`None` for by-value); the result is the coerced
    /// [`CastValueResult`], whose own `byref` carries the coerced value's image
    /// when the target is pass-by-reference (the bare `value` is `0` then). The
    /// no-op relabel case (`valtype == reqtype` and the typmod is unconstrained)
    /// is handled in-crate and never reaches here.
    pub fn exec_cast_value_via_spi(
        value: usize,
        value_byref: Option<std::vec::Vec<u8>>,
        isnull: bool,
        valtype: Oid,
        valtypmod: int32,
        reqtype: Oid,
        reqtypmod: int32,
    ) -> PgResult<CastValueResult>
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
        read_only: bool,
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
    /// Read `CurrentResourceOwner` (resowner.c global). `exec_stmt_block`'s
    /// EXCEPTION leg snapshots this (`oldowner = CurrentResourceOwner`) before
    /// `BeginInternalSubTransaction` so it can restore it after the subxact
    /// release/rollback — the subxact engine leaves `CurrentResourceOwner` set
    /// to the parent (CurTransaction) owner, not the portal owner that was
    /// current when the block ran.
    pub fn current_resource_owner() -> ResourceOwner
);

seam_core::seam!(
    /// `CurrentResourceOwner = owner` (resowner.c global). `exec_stmt_block`'s
    /// EXCEPTION leg restores the snapshot (`CurrentResourceOwner = oldowner`)
    /// after the internal subtransaction is released or rolled back, exactly as
    /// pl_exec.c does.
    pub fn set_current_resource_owner(owner: ResourceOwner)
);

seam_core::seam!(
    /// `CStringGetTextDatum(s)` (`builtins.h` / `varlena.c cstring_to_text`):
    /// build a `text` Datum from an owned Rust `String`. Used by the EXCEPTION
    /// handler to bind the SQLSTATE and SQLERRM special variables
    /// (`assign_error_vars`) and by `exec_stmt_getdiag`. Returns
    /// `(word, image)`: the `DatumGetPointer` bare-word view AND the verbatim
    /// header-ful `text` varlena byte image. The word points at the same
    /// backend-lifetime varlena the image holds (mirroring how
    /// `CStringGetTextDatum` palloc's in `CurrentMemoryContext`), so the bytes
    /// outlive the call. The caller stores the word into the target `text`
    /// variable AND threads the image into the variable's `value_byref`
    /// out-of-band companion, so a later expression evaluation (e.g.
    /// `RETURN SQLERRM`, a text comparison over the special var) binds the rich
    /// `Datum::ByRef` instead of a bare word that the varlena fmgr cores reject
    /// ("by-ref arg missing from by-ref lane"). The executor is layered below
    /// the varlena substrate, so the handler installs it.
    pub fn cstring_to_text_datum(s: std::string::String) -> PgResult<(usize, std::vec::Vec<u8>)>
);

seam_core::seam!(
    /// `DirectFunctionCall1(namein, CStringGetDatum(s))` — build a `name` value
    /// (the fixed 64-byte NUL-padded `NameData` buffer) from `s`. Used by
    /// `plpgsql_fulfill_promise` for the `name`-typed trigger promises
    /// (`TG_NAME` / `TG_TABLE_NAME` / `TG_TABLE_SCHEMA`). Returns the verbatim
    /// header-less `NameData` byte image (the by-ref lane carries it).
    pub fn cstring_to_name_datum(s: std::string::String) -> PgResult<std::vec::Vec<u8>>
);

seam_core::seam!(
    /// `get_namespace_name(nspoid)` (`lsyscache.c`) — the schema name for a
    /// namespace OID (`TG_TABLE_SCHEMA`). Returns the server-encoded bytes.
    pub fn get_namespace_name(nspoid: Oid) -> PgResult<std::string::String>
);

seam_core::seam!(
    /// `construct_md_array(elems, NULL, 1, {nelems}, {0}, TEXTOID, -1, false,
    /// TYPALIGN_INT)` over the trigger's textual arguments — build the `TG_ARGV`
    /// `text[]` value. The lower bound is 0 (not 1): for historical reasons
    /// TG_ARGV[] subscripts start at zero, which is why C uses construct_md_array
    /// here rather than construct_array. Each element is `Some(bytes)` for a
    /// present argument (server-encoded text) or `None` for a NULL element.
    /// Returns the verbatim header-ful array varlena byte image (the by-ref lane
    /// carries it).
    pub fn construct_text_array_datum(
        elems: std::vec::Vec<Option<std::vec::Vec<u8>>>,
    ) -> PgResult<std::vec::Vec<u8>>
);

/// One materialized FOREACH-over-array iteration item (`exec_stmt_foreach_a`'s
/// `array_iterate` loop body input): the value to assign to the loop variable
/// for this iteration, plus its is-null flag. A pass-by-reference value (a
/// `text`/`numeric`/… array element, or — in the SLICE case — the freshly built
/// sub-array) carries its verbatim header-ful varlena byte image in `byref` (the
/// bare `value` word is `0` then); a by-value element (`int4`/`bool`/…) carries
/// the scalar word in `value` with `byref == None`.
#[derive(Clone, Debug)]
pub struct ForeachItem {
    pub value: usize,
    pub isnull: bool,
    pub byref: Option<std::vec::Vec<u8>>,
}

/// The materialized result of `exec_stmt_foreach_a`'s array-iteration setup +
/// `array_iterate` loop (`pl_exec.c`): every iteration's `ForeachItem` (in
/// order), plus the iterator result type/typmod that
/// `exec_assign_value(loop_var, value, …, iterator_result_type,
/// iterator_result_typmod)` uses. Without slicing the result type is the array's
/// element type; when slicing (`slice > 0`) it is the array type itself.
#[derive(Clone, Debug)]
pub struct ForeachIterateResult {
    pub items: std::vec::Vec<ForeachItem>,
    pub result_type: Oid,
    pub result_typmod: int32,
}

seam_core::seam!(
    /// `get_element_type(typid)` (`lsyscache.c`) — the element type OID of an
    /// array type, or `None` (`InvalidOid`). `exec_stmt_foreach_a` uses it to
    /// check the loop variable's array-ness (`FOREACH ... SLICE loop variable
    /// must be of an array type` / `FOREACH loop variable must not be of an array
    /// type`). The executor unit is layered below lsyscache; the handler installs
    /// it.
    pub fn foreach_get_element_type(typid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// The array-iteration leg of `exec_stmt_foreach_a` (`pl_exec.c`): given the
    /// already-evaluated FOREACH array's verbatim varlena byte image (`arr_bytes`,
    /// from `exec_eval_expr`'s by-ref result), its runtime array type `arrtype` /
    /// typmod `arrtypmod`, and the `slice` dimension, perform the C steps that
    /// reach into the array + fmgr substrate:
    ///
    /// * `get_element_type(arrtype)` — error "FOREACH expression must yield an
    ///   array, not type %s" (`ERRCODE_DATATYPE_MISMATCH`) when invalid;
    /// * `DatumGetArrayTypePCopy(value)` (detoast);
    /// * the slice range check ("slice dimension (%d) is out of the valid range
    ///   0..%d", `ERRCODE_ARRAY_SUBSCRIPT_ERROR`);
    /// * `array_create_iterator(arr, slice, NULL)` + the full `array_iterate`
    ///   loop, materializing every element (`slice == 0`) or sub-array
    ///   (`slice > 0`) as a [`ForeachItem`], in iteration order.
    ///
    /// The loop-variable array-ness sanity checks (`FOREACH ... SLICE loop
    /// variable must be of an array type` / `FOREACH loop variable must not be of
    /// an array type`) are done by the caller (they read the loop variable's
    /// declared type, which the executor owns). This seam returns the iterator
    /// result type/typmod so the caller's per-iteration `exec_assign_value` casts
    /// the element/slice to the loop variable's type exactly as C does.
    ///
    /// The executor unit is layered below the array/lsyscache owners; the handler
    /// (which depends on `backend-utils-adt-arrayfuncs` + lsyscache) installs it.
    pub fn foreach_iterate_via_array(
        arr_bytes: std::vec::Vec<u8>,
        arrtype: Oid,
        arrtypmod: int32,
        slice: int32,
    ) -> PgResult<ForeachIterateResult>
);

seam_core::seam!(
    /// `plpgsql_check_asserts` (pl_handler.c) — the `plpgsql.check_asserts` GUC
    /// (default `true`). `exec_stmt_assert` returns early when it is off, so the
    /// executor must read it; the GUC variable lives in the handler unit
    /// (`pl_handler.c`), which is layered above the executor. The handler installs
    /// it. Infallible bool read.
    pub fn plpgsql_check_asserts() -> bool
);

seam_core::seam!(
    /// `plpgsql_extra_warnings` (pl_handler.c) — the live `plpgsql.extra_warnings`
    /// GUC bitmask. The runtime too-many-rows / strict-multi-assignment checks
    /// (`exec_stmt_execsql` / `exec_move_row`) read the *current* session value
    /// (not the function's compile-time value), so the executor must reach the
    /// handler-owned GUC. Infallible int read.
    pub fn plpgsql_extra_warnings() -> int32
);

seam_core::seam!(
    /// `plpgsql_extra_errors` (pl_handler.c) — the live `plpgsql.extra_errors`
    /// GUC bitmask (companion to [`plpgsql_extra_warnings`]).
    pub fn plpgsql_extra_errors() -> int32
);

seam_core::seam!(
    /// `type_is_rowtype(typid)` (`lsyscache.c`) — is the type a composite/row
    /// type (or a domain over one)? `exec_stmt_return` uses it to decide whether
    /// a RETURN value of a SETOF/composite function is a row. The executor unit
    /// is layered below lsyscache; the handler installs it.
    pub fn type_is_rowtype(typid: Oid) -> PgResult<bool>
);

/// The composite Datum a whole-row ROW datum (`exec_eval_datum` DTYPE_ROW)
/// flattens to: the verbatim `HeapTupleHeader` varlena image, plus the row's
/// registered rowtype id/typmod (set by `BlessTupleDesc`).
#[derive(Clone, Debug)]
pub struct RowCompositeDatum {
    /// The flat composite-Datum varlena image (`HeapTupleGetDatum`).
    pub image: std::vec::Vec<u8>,
    /// `row->rowtupdesc->tdtypeid` after `BlessTupleDesc` (RECORDOID for an
    /// anonymous OUT-parameter row).
    pub typeid: Oid,
    /// `row->rowtupdesc->tdtypmod` after `BlessTupleDesc` (the registered
    /// anonymous-record typmod).
    pub typmod: int32,
}

seam_core::seam!(
    /// `exec_eval_datum` DTYPE_ROW (pl_exec.c 5316) — `BlessTupleDesc` the row's
    /// `rowtupdesc`, then `make_tuple_from_row` (`heap_form_tuple` over the
    /// already-evaluated scalar field values) and `HeapTupleGetDatum`. The
    /// executor (this unit) is layered below execTuples/heaptuple and the
    /// compiler's `rowtupdesc_table`; the handler — which sits above all three —
    /// installs it.
    ///
    /// `fields` carries each field's current value (already read by the executor
    /// via `exec_eval_datum`), in row-field order; `rowtupdesc_handle` is the
    /// compiled row's `rowtupdesc` handle (1-based; `0` is the C NULL, which is
    /// "row variable has no tupdesc"). A field's `typeid` not matching the
    /// descriptor's column type is C's `make_tuple_from_row` NULL return →
    /// "row not compatible with its own tupdesc".
    pub fn form_row_composite_datum(
        fields: std::vec::Vec<ExecsqlColumn>,
        rowtupdesc_handle: u64,
    ) -> PgResult<RowCompositeDatum>
);

// ===========================================================================
// Cursor surface (pl_exec.c's exec_stmt_open / exec_stmt_fetch /
// exec_stmt_close / exec_stmt_forc) over the SPI cursor functions
// (SPI_cursor_open_with_paramlist / SPI_cursor_parse_open /
// SPI_scroll_cursor_fetch / SPI_scroll_cursor_move / SPI_cursor_find /
// SPI_cursor_close). The executor unit (pl_exec.c) is layered below the SPI
// cursor/portal surface, so it reaches it through these seams; the handler
// (top layer with SPI access) installs them.
// ===========================================================================

/// `FetchDirection` (`nodes/parsenodes.h`) as it crosses the cursor seam — the
/// fetch/move direction of a `FETCH`/`MOVE`. Mirrors `types_plpgsql::FetchDirection`
/// (same `repr(i32)` values) so the executor passes it without depending on the
/// portal crate; the SPI installer maps it onto `types_portal::FetchDirection`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum CursorFetchDirection {
    Forward = 0,
    Backward = 1,
    Absolute = 2,
    Relative = 3,
}

/// The result of a cursor `FETCH`/`MOVE` (`SPI_scroll_cursor_fetch`/`_move` then
/// `SPI_processed` / `SPI_tuptable`): the number of rows fetched/skipped and —
/// for a `FETCH` (not a `MOVE`) — every fetched row's columns. A `MOVE` returns
/// no rows (C's `None_Receiver`).
#[derive(Clone, Debug)]
pub struct CursorFetchResult {
    pub processed: u64,
    /// The fetched rows (empty for a `MOVE`). Each row is its columns in the
    /// cursor result-descriptor order.
    pub rows: std::vec::Vec<std::vec::Vec<ExecsqlColumn>>,
}

seam_core::seam!(
    /// `SPI_cursor_open_with_paramlist(name, plan, paramLI, read_only)` →
    /// `SPI_cursor_open_internal` (`spi.c`), specialized to the PL/pgSQL `OPEN`
    /// path over a static query: prepare the embedded `query` (in `parse_mode`,
    /// with the PL/pgSQL bareword parser hooks from `parse_state`), bind the
    /// referenced scalar datums from `datum_snapshot`, open a real portal with the
    /// given `cursor_options` (`CURSOR_OPT_*`), and return the open portal's name
    /// (`portal->name`). `curname` is the explicit cursor name (`None`/empty → a
    /// generated nonconflicting name, C's `CreateNewPortal`).
    pub fn spi_cursor_open(
        curname: std::option::Option<std::string::String>,
        query: std::string::String,
        parse_mode: types_parsenodes::RawParseMode,
        parse_state: types_nodes::parsestmt::PlpgsqlExprParseState,
        cursor_options: int32,
        read_only: bool,
        datum_snapshot: std::vec::Vec<Option<EvalParamValue>>,
    ) -> PgResult<std::string::String>
);

seam_core::seam!(
    /// `exec_dynquery_with_params(estate, dynquery, params, curname, cursorOptions)`
    /// (`pl_exec.c`) → `SPI_cursor_parse_open(name, querystr, options)` (`spi.c`):
    /// open an implicit cursor over the already-rendered dynamic query string
    /// `query` (parsed `RAW_PARSE_DEFAULT`) with the already-evaluated `USING`
    /// params, and the given `cursor_options`. Returns the open portal's name.
    pub fn spi_cursor_open_execute(
        curname: std::option::Option<std::string::String>,
        query: std::string::String,
        params: std::vec::Vec<DynUsingParam>,
        cursor_options: int32,
        read_only: bool,
    ) -> PgResult<std::string::String>
);

seam_core::seam!(
    /// `SPI_cursor_find(name)` (`spi.c`) — does a cursor (portal) of this name
    /// currently exist? (`GetPortalByName(name) != NULL`.)
    pub fn spi_cursor_find(name: std::string::String) -> PgResult<bool>
);

seam_core::seam!(
    /// `SPI_scroll_cursor_fetch` / `SPI_scroll_cursor_move` →
    /// `_SPI_cursor_operation` (`spi.c`): find the cursor by name, run
    /// `PortalRunFetch` in `direction` for `count` rows, and (for a fetch) return
    /// every fetched row's columns. A move (`is_move`) uses the `None` receiver and
    /// returns no rows.
    pub fn spi_cursor_fetch_move(
        name: std::string::String,
        direction: CursorFetchDirection,
        count: i64,
        is_move: bool,
    ) -> PgResult<CursorFetchResult>
);

seam_core::seam!(
    /// `SPI_cursor_close(portal)` (`spi.c`) — close (drop) the named cursor.
    /// `Err` for an invalid portal name (C: `elog(ERROR, "invalid portal …")`).
    pub fn spi_cursor_close(name: std::string::String) -> PgResult<()>
);
