//! Outward catalog / syscache / SQL-engine surface the compiler consumes.
//!
//! `pl_comp.c` reaches into syscache (`SearchSysCache1(TYPEOID,…)`),
//! lsyscache (`get_array_type`/`get_rel_type_id`/`get_base_element_type`/…),
//! `format_type_be`, the typcache, the raw type-name parser, `RangeVarGetRelid`,
//! and the fmgr `FunctionCallInfo`. Where the owner crate is present and its
//! seam signature maps cleanly, these delegate to it; where the callee subsystem
//! is not yet reachable (the fmgr call path, polymorphic-argtype resolution, the
//! composite-type tupdesc typcache, `RangeVarGetRelid`, the plpgsql-mode raw
//! parser, and the exec callback `plpgsql_exec_get_datum_type_info`), they
//! mirror-PG-and-panic — faithful: the C site would `ereport`/elog there too
//! until those owners land.

use core::cell::Cell;

use types_core::Oid;
use types_plpgsql::PLpgSQL_resolve_option;
use types_tuple::pg_type::FormData_pg_type;

/// `InvalidOid`.
pub const INVALID_OID: Oid = 0;

// ---------------------------------------------------------------------------
// PL/pgSQL custom-GUC globals (defined in pl_handler.c, the next layer).
//
// `plpgsql_compile_callback` reads these per-backend `int`/`bool` globals when
// assembling the function.  pl_handler.c owns the GUC variables + their
// `DefineCustomEnumVariable`/`DefineCustomBoolVariable` registration; until that
// crate lands these hold the C-source defaults (`PLPGSQL_RESOLVE_ERROR`,
// `false`, `0`, `0`).  The handler installs the real GUC values through the
// `set_*` mutators when it lands.
// ---------------------------------------------------------------------------

thread_local! {
    static VARIABLE_CONFLICT: Cell<PLpgSQL_resolve_option> =
        const { Cell::new(PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR) };
    static PRINT_STRICT_PARAMS: Cell<bool> = const { Cell::new(false) };
    static EXTRA_WARNINGS: Cell<i32> = const { Cell::new(0) };
    static EXTRA_ERRORS: Cell<i32> = const { Cell::new(0) };
}

/// `plpgsql_variable_conflict`.
pub fn plpgsql_variable_conflict() -> PLpgSQL_resolve_option {
    VARIABLE_CONFLICT.with(Cell::get)
}
/// `plpgsql_print_strict_params`.
pub fn plpgsql_print_strict_params() -> bool {
    PRINT_STRICT_PARAMS.with(Cell::get)
}
/// `plpgsql_extra_warnings`.
pub fn plpgsql_extra_warnings() -> i32 {
    EXTRA_WARNINGS.with(Cell::get)
}
/// `plpgsql_extra_errors`.
pub fn plpgsql_extra_errors() -> i32 {
    EXTRA_ERRORS.with(Cell::get)
}

/// `plpgsql_variable_conflict = value` (the handler's GUC assign hook).
pub fn set_plpgsql_variable_conflict(value: PLpgSQL_resolve_option) {
    VARIABLE_CONFLICT.with(|c| c.set(value));
}
/// `plpgsql_print_strict_params = value`.
pub fn set_plpgsql_print_strict_params(value: bool) {
    PRINT_STRICT_PARAMS.with(|c| c.set(value));
}
/// `plpgsql_extra_warnings = value`.
pub fn set_plpgsql_extra_warnings(value: i32) {
    EXTRA_WARNINGS.with(|c| c.set(value));
}
/// `plpgsql_extra_errors = value`.
pub fn set_plpgsql_extra_errors(value: i32) {
    EXTRA_ERRORS.with(|c| c.set(value));
}

/// `check_function_bodies` (guc.c) — read the core GUC directly (low crate, no
/// cycle); the inline (`DO`) compile gates extra syntax checking on it.
pub fn check_function_bodies() -> bool {
    (backend_utils_misc_guc_tables::vars::check_function_bodies.get().get)()
}

/// `OidIsValid(oid)`.
pub fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

// ---------------------------------------------------------------------------
// pg_type syscache row (`SearchSysCache1(TYPEOID, …)` + GETSTRUCT).
// ---------------------------------------------------------------------------

/// `SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid))` projected to the
/// fixed `Form_pg_type` columns. Panics with the genuine
/// `cache lookup failed for type %u` message on a miss (`elog(ERROR)`), exactly
/// as `plpgsql_build_datatype` does after `!HeapTupleIsValid`.
pub fn pg_type_form(type_oid: Oid) -> FormData_pg_type {
    match backend_utils_cache_syscache_seams::pg_type_form::call(type_oid) {
        Ok(Some(form)) => form,
        Ok(None) => panic!("cache lookup failed for type {}", type_oid),
        Err(_) => panic!("cache lookup failed for type {}", type_oid),
    }
}

/// `NameStr(type_struct->typname)`.
pub fn typname_string(form: &FormData_pg_type) -> String {
    String::from_utf8_lossy(form.typname.name_str()).into_owned()
}

// ---------------------------------------------------------------------------
// lsyscache.c lookups.
// ---------------------------------------------------------------------------

/// `get_array_type(typid)` — the array type over `typid`, or `InvalidOid`.
pub fn get_array_type(typid: Oid) -> Oid {
    match backend_utils_cache_lsyscache_seams::get_array_type::call(typid) {
        Ok(Some(oid)) => oid,
        Ok(None) => INVALID_OID,
        Err(_) => INVALID_OID,
    }
}

/// `get_base_element_type(typid)` — the element type of `typid` (peeling one
/// domain level), or `InvalidOid`.
pub fn get_base_element_type(typid: Oid) -> Oid {
    backend_utils_cache_lsyscache_seams::get_base_element_type::call(typid).unwrap_or(INVALID_OID)
}

/// `get_rel_type_id(relid)` — the composite type OID of a relation, or
/// `InvalidOid`.
pub fn get_rel_type_id(relid: Oid) -> Oid {
    backend_utils_cache_lsyscache_seams::get_rel_type_id::call(relid).unwrap_or(INVALID_OID)
}

/// `type_is_rowtype(typid)`.
pub fn type_is_rowtype(typid: Oid) -> bool {
    backend_utils_cache_lsyscache_seams::type_is_rowtype::call(typid).unwrap_or(false)
}

/// `F_ARRAY_SUBSCRIPT_HANDLER` (catalog/pg_proc_d.h) — OID of the standard
/// `array_subscript_handler` function (pg_proc.dat oid 6179).
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

/// `IsTrueArrayType(typeForm)` (catalog/pg_type.h):
/// `OidIsValid(typelem) && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER`.
pub fn is_true_array_type(form: &FormData_pg_type) -> bool {
    oid_is_valid(form.typelem) && form.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

// ---------------------------------------------------------------------------
// format_type_be (diagnostics).
// ---------------------------------------------------------------------------

/// `format_type_be(typeOid)` — printable type name for error messages.
pub fn format_type_be(type_oid: Oid) -> String {
    backend_utils_adt_format_type_seams::format_type_be_owned::call(type_oid)
        .unwrap_or_else(|_| format!("type {}", type_oid))
}

// ---------------------------------------------------------------------------
// Not-yet-reachable owners — mirror-PG-and-panic (the callee crate's WAL of
// work is the gate, not this crate's logic).
// ---------------------------------------------------------------------------

/// `lookup_type_cache(typoid, flags)` for a composite type's tupdesc identity —
/// the typcache owner's composite-tupdesc projection is not yet reachable from
/// the compile path.
pub fn composite_tupdesc_id(_typoid: Oid) -> ! {
    panic!(
        "plpgsql compile: lookup_type_cache(TYPECACHE_TUPDESC) for a named \
         composite type is not reachable (typcache composite-tupdesc owner unwired)"
    )
}

/// `RangeVarGetRelid` / `RelnameGetRelid` for `%TYPE` / `%ROWTYPE` over a table.
/// Returns the relation OID in C; the namespace owner is not reachable from the
/// compile path yet, so this mirror-PG-and-panics (the `%ROWTYPE`/`%TYPE`-over-
/// a-table feature gates on it).
pub fn relname_get_relid(_relname: &str) -> Oid {
    panic!("plpgsql compile: RangeVarGetRelid()/RelnameGetRelid() not reachable (namespace owner unwired for compile)")
}

/// `parse_datatype(string, location)` — the plpgsql-mode raw type-name parser.
pub fn parse_datatype(_string: &str, _location: i32) -> ! {
    panic!("plpgsql compile: parse_datatype() not reachable (parser typename owner unwired for compile)")
}

/// `get_collation_oid(names, missing_ok)`.
pub fn get_collation_oid(_names: &[String], _missing_ok: bool) -> ! {
    panic!("plpgsql compile: get_collation_oid() not reachable (namespace collation owner unwired for compile)")
}

/// `check_sql_expr(stmt, parseMode, location, yyscanner)` (pl_gram.y) — when
/// `plpgsql_check_syntax` is set, raw-parse the SQL text for syntax only
/// (`(void) raw_parser(stmt, parseMode)`), discarding the parse tree; a syntax
/// error is raised inside the parser at the error position. The caller (the
/// grammar, via the comp-seam) already short-circuits when `!plpgsql_check_syntax`
/// — this is reached only in the validating (`forValidator`) compile.
pub fn check_sql_expr(
    stmt: &str,
    mode: types_plpgsql::RawParseMode,
    _location: i32,
) -> types_error::PgResult<()> {
    backend_parser_driver_seams::raw_parse_syntax_check::call(stmt.to_owned(), mode)
}

/// `CreateTemplateTupleDesc(numvars)` + per-member `TupleDescInitEntry` /
/// `TupleDescInitEntryCollation` (`build_row_from_vars`).  In `types-plpgsql`
/// the row's `rowtupdesc` is an opaque handle with no in-repo constructor, so
/// the genuine tupdesc build is the tupdesc owner's; until the handle model
/// unifies this mirror-PG-and-panics for a non-empty row.  An empty member list
/// (numvars == 0) yields no descriptor.
pub fn build_row_tupledesc(members: &[crate::RowMember]) -> Option<types_plpgsql::TupleDesc> {
    if members.is_empty() {
        return None;
    }
    panic!(
        "plpgsql compile: build_row_from_vars rowtupdesc ({} members) not reachable \
         (TupleDesc opaque-handle model has no in-repo constructor)",
        members.len()
    )
}

/// `cached_function_compile(fcinfo, fn_extra, plpgsql_compile_callback, …)` +
/// the `fn_extra` save (`plpgsql_compile`).  The funccache driver
/// (`backend-utils-cache-funccache::cached_function_compile`) exists, but it
/// operates on `CachedFunctionRef` and `PLpgSQL_function` does not yet implement
/// the `CachedFunctionPayload` bridge (the opaque-`cfunc` header-unification
/// keystone), and the owned `FunctionCallInfo` model carries no `fn_extra`
/// channel — so this mirror-PG-and-panics until those land.
pub fn compile_cached(
    _fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    _for_validator: bool,
) -> ! {
    panic!(
        "plpgsql compile: cached_function_compile()/fn_extra dispatch not reachable \
         (PLpgSQL_function CachedFunctionPayload bridge + fcinfo fn_extra channel unmodeled)"
    )
}

/// `quote_identifier(ident)` (ruleutils.c) — delegate to the ruleutils owner
/// (the reserved-keyword test + double-quoting live there; pl_gram.y's
/// positional cursor-arg list text calls it).
pub fn quote_identifier(ident: &str) -> String {
    let ctx = mcx::MemoryContext::new("plpgsql quote_identifier");
    let out = backend_utils_adt_ruleutils_seams::quote_identifier::call(ctx.mcx(), ident);
    match out {
        Ok(s) => s.as_str().to_owned(),
        Err(e) => panic!("quote_identifier failed: {e:?}"),
    }
}

