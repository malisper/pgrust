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

use ::types_core::Oid;
use ::plpgsql::PLpgSQL_resolve_option;
use ::types_tuple::pg_type::FormData_pg_type;

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
    (guc_tables::vars::check_function_bodies.get().get)()
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
    match syscache_seams::pg_type_form::call(type_oid) {
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
    match lsyscache_seams::get_array_type::call(typid) {
        Ok(Some(oid)) => oid,
        Ok(None) => INVALID_OID,
        Err(_) => INVALID_OID,
    }
}

/// `get_base_element_type(typid)` — the element type of `typid` (peeling one
/// domain level), or `InvalidOid`.
pub fn get_base_element_type(typid: Oid) -> Oid {
    lsyscache_seams::get_base_element_type::call(typid).unwrap_or(INVALID_OID)
}

/// `get_rel_type_id(relid)` — the composite type OID of a relation, or
/// `InvalidOid`.
pub fn get_rel_type_id(relid: Oid) -> Oid {
    lsyscache_seams::get_rel_type_id::call(relid).unwrap_or(INVALID_OID)
}

/// `type_is_rowtype(typid)`.
pub fn type_is_rowtype(typid: Oid) -> bool {
    lsyscache_seams::type_is_rowtype::call(typid).unwrap_or(false)
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
    format_type_seams::format_type_be_owned::call(type_oid)
        .unwrap_or_else(|_| format!("type {}", type_oid))
}

// ---------------------------------------------------------------------------
// Not-yet-reachable owners — mirror-PG-and-panic (the callee crate's WAL of
// work is the gate, not this crate's logic).
// ---------------------------------------------------------------------------

/// `lookup_type_cache(typoid, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO)`
/// (chaining to the domain base for a domain-over-composite) for a named
/// composite type's tupdesc identity — the typcache entry handle and the current
/// `tupDesc_identifier` recorded in `PLpgSQL_type` so the exec layer can detect
/// tupdesc changes (including drops). When the type is not composite the
/// resolved `tupDesc` is `None`, which C turns into
/// `ERRCODE_WRONG_OBJECT_TYPE` ("type %s is not composite").
pub fn composite_tupdesc_id(typoid: Oid) -> (Option<::plpgsql::TypeCacheEntry>, u64) {
    // The seam clones the composite tupdesc into the supplied context; build a
    // throwaway one (we only read the scalar identifier + the not-composite flag).
    let ctx = mcx::MemoryContext::new("plpgsql composite_tupdesc_id");
    let mcx = ctx.mcx();
    let view = typcache_seams::lookup_type_cache_expanded_record::call(
        mcx, typoid,
    )
    .expect("lookup_type_cache(TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO) for a named composite type");
    if view.tup_desc.is_none() {
        panic!(
            "type {} is not composite (SQLSTATE 42809)",
            format_type_be(typoid)
        );
    }
    // typ->tcache is a TypeCacheEntry* handle in C; the exec layer keys it by
    // type OID for revalidation. typ->tupdesc_id mirrors tupDesc_identifier.
    (
        Some(::plpgsql::TypeCacheEntry(typoid as u64)),
        view.tup_desc_identifier,
    )
}

/// `RelnameGetRelid(ident)` — resolve an unqualified table name to its relation
/// OID for `%ROWTYPE` over a table. `missing_ok = true` returns `InvalidOid` (no
/// raise) when the relation isn't found, matching C's `plpgsql_parse_wordrowtype`
/// (which then throws its own traditional "relation does not exist" message).
pub fn relname_get_relid(relname: &str) -> types_error::PgResult<Oid> {
    qualified_relname_get_relid(&[relname], true)
}

/// `RangeVarGetRelid(makeRangeVarFromNameList(idents), NoLock, missing_ok)` — the
/// (possibly-qualified) name resolution used by the `%TYPE` / `%ROWTYPE` paths.
/// `%TYPE` over a column uses `missing_ok = false` (the relation must exist; C
/// `plpgsql_parse_cwordtype` calls `RangeVarGetRelid(.., false)`); the
/// `%ROWTYPE` paths pass `missing_ok = true` and check the OID themselves.
pub fn qualified_relname_get_relid(
    idents: &[&str],
    missing_ok: bool,
) -> types_error::PgResult<Oid> {
    // Avoid memory leaks in the long-term function context — C does the lookup
    // in plpgsql_compile_tmp_cxt. The RangeVar and any transient catalog copies
    // live in this throwaway context.
    let ctx = mcx::MemoryContext::new("plpgsql relname_get_relid");
    let mcx = ctx.mcx();
    let relvar = namespace_seams::make_range_var_from_name_list::call(idents)?;
    // NoLock (== 0): can't lock — we might not have privileges. (LOCKMODE = i32.)
    const NO_LOCK: i32 = 0;
    namespace_seams::range_var_get_relid::call(mcx, &relvar, NO_LOCK, missing_ok)
}

/// `(SearchSysCacheAttName + build_datatype)` for a `tablename.colname%TYPE`
/// reference: resolve the column's `(atttypid, atttypmod, attcollation)` and
/// build the compiler type from it. A missing column raises
/// `ERRCODE_UNDEFINED_COLUMN`, matching C's `plpgsql_parse_cwordtype`.
pub fn column_atttype(
    class_oid: Oid,
    relname: &str,
    fldname: &str,
) -> types_error::PgResult<Box<::plpgsql::PLpgSQL_type>> {
    use lsyscache_seams as lsyscache;
    let attnum = lsyscache::get_attnum::call(class_oid, fldname)?;
    if attnum == 0 {
        // InvalidAttrNumber — no such column.
        return Err(types_error::PgError::error(format!(
            "column \"{fldname}\" of relation \"{relname}\" does not exist"
        ))
        .with_sqlstate(crate::ERRCODE_UNDEFINED_COLUMN));
    }
    let (typid, typmod, collid) = lsyscache::get_atttypetypmodcoll::call(class_oid, attnum)?;
    // build_datatype(typetup, atttypmod, attcollation, NULL) — found-by-OID.
    Ok(crate::plpgsql_build_datatype_internal(typid, typmod, collid, None))
}

/// `parse_datatype(string, location)` (pl_gram.y 3844) — the plpgsql-mode raw
/// type-name parser. Let the main parser parse the type string under standard
/// SQL rules (`typeStringToTypeName` + `typenameTypeIdAndMod`), then build the
/// `PLpgSQL_type` for it (`plpgsql_build_datatype`) at the current compile's
/// `fn_input_collation`. The `sql_error_callback` errcontext is the diagnostic
/// position decoration (owned by the error stack; a parse failure still raises
/// faithfully via `?`).
pub fn parse_datatype(
    string: &str,
    _location: i32,
) -> types_error::PgResult<Box<::plpgsql::PLpgSQL_type>> {
    // typeStringToTypeName(string, NULL) + typenameTypeIdAndMod(NULL, typeName,
    // &type_id, &typmod). parseTypeString folds both (no soft-error context, so
    // a bad type name raises).
    let ctx = mcx::MemoryContext::new("plpgsql parse_datatype");
    let mcx = ctx.mcx();
    let typeName = parse_type::typeStringToTypeName(string, None)?
        .expect("typeStringToTypeName with no soft-error context yields a TypeName or raises");
    let (type_id, typmod) =
        parse_type::typenameTypeIdAndMod(mcx, None, &typeName)?;

    // plpgsql_build_datatype(type_id, typmod, plpgsql_curr_compile->
    // fn_input_collation, typeName). The C passes the parsed `typeName` as
    // `origtypname` (kept for re-resolving the type on a cached recompile). The
    // owned `PLpgSQL_type.origtypname` is `::plpgsql::TypeName`, a distinct
    // model from the parser's `parsenodes::TypeName`; a directly-named type
    // re-resolves to the same OID by name, so `None` is faithful here (origtypname
    // matters for %TYPE/%ROWTYPE-derived types, which take the wordtype path).
    let _ = typeName;
    let collation = super::curr_compile_field(|f| f.fn_input_collation);
    Ok(super::plpgsql_build_datatype_internal(
        type_id, typmod, collation, None,
    ))
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
    mode: ::plpgsql::RawParseMode,
    _location: i32,
) -> types_error::PgResult<()> {
    driver_seams::raw_parse_syntax_check::call(stmt.to_owned(), mode)
}

/// `CreateTemplateTupleDesc(numvars)` + per-member `TupleDescInitEntry` /
/// `TupleDescInitEntryCollation` (`build_row_from_vars`, pl_comp.c:1838).  Builds
/// the genuine composite `TupleDesc` for a compiled row variable: a fresh
/// template descriptor with one entry per member, each carrying the member's
/// name/typoid/typmod and collation.
///
/// In `types-plpgsql` the row's `rowtupdesc` field is a lifetime-free `u64`
/// handle (the compiled structs are `Clone + Debug`), so the real
/// `TupleDescData` is built in a private backend-lifetime `MemoryContext` —
/// the analogue of the C compile context that owns `rowtupdesc` for the
/// cached function's life — and registered in [`crate::rowtupdesc_table`],
/// which returns the 1-based handle.  An empty member list (numvars == 0)
/// yields no descriptor (the C NULL).
pub fn build_row_tupledesc(
    members: &[crate::RowMember],
) -> types_error::PgResult<Option<::plpgsql::TupleDesc>> {
    if members.is_empty() {
        return Ok(None);
    }

    // The C compile context analogue: a private context whose arena backs the
    // descriptor (and the column names it owns) for the backend's lifetime.
    let ctx = Box::new(mcx::MemoryContext::new("PL/pgSQL row tupdesc"));
    let mcx: mcx::Mcx<'static> =
        unsafe { core::mem::transmute::<mcx::Mcx<'_>, mcx::Mcx<'static>>(ctx.mcx()) };

    // CreateTemplateTupleDesc(numvars).
    let mut td = tupdesc::CreateTemplateTupleDesc(mcx, members.len() as i32)?;

    // Per member: TupleDescInitEntry(rowtupdesc, i+1, refname, typoid, typmod, 0)
    // then TupleDescInitEntryCollation(rowtupdesc, i+1, typcoll).
    for (i, m) in members.iter().enumerate() {
        let attno = (i + 1) as i16;
        tupdesc::TupleDescInitEntry(
            &mut td,
            attno,
            Some(&m.attname),
            m.typoid,
            m.typmod,
            0,
        )?;
        tupdesc::TupleDescInitEntryCollation(&mut td, attno, m.typcoll)?;
    }

    let handle = crate::rowtupdesc_table::register(ctx, td);
    Ok(Some(::plpgsql::TupleDesc(handle)))
}

/// `cached_function_compile(fcinfo, fn_extra, plpgsql_compile_callback, …)` +
/// the `fn_extra` save (`plpgsql_compile`).  The funccache driver
/// (`backend-utils-cache-funccache::cached_function_compile`) exists, but it
/// operates on `CachedFunctionRef` and `PLpgSQL_function` does not yet implement
/// the `CachedFunctionPayload` bridge (the opaque-`cfunc` header-unification
/// keystone), and the owned `FunctionCallInfo` model carries no `fn_extra`
/// channel — so this mirror-PG-and-panics until those land.
pub fn compile_cached(
    _fcinfo: &nodes::fmgr::FunctionCallInfoBaseData<'_>,
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
    let out = ruleutils_seams::quote_identifier::call(ctx.mcx(), ident);
    match out {
        Ok(s) => s.as_str().to_owned(),
        Err(e) => panic!("quote_identifier failed: {e:?}"),
    }
}

