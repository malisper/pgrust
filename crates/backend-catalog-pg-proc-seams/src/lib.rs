//! Seam declarations for `backend-catalog-pg-proc` (`catalog/pg_proc.c`).
//!
//! `ProcedureCreate`'s decision logic, field formation, dependency-recording
//! order, and the validators' control flow all live in the owner crate. The
//! seams below are the genuinely-unported external legs that owner reaches:
//!
//!   * the replace-path syscache probe `SearchSysCache3(PROCNAMEARGSNSP, ...)`
//!     plus the three sub-checks that read the *held old tuple*
//!     (`build_function_result_tupdesc_t`/`_d` + `equalRowTypes`,
//!     `get_func_input_arg_names`, `stringToNode` + `exprType` over
//!     `proargdefaults`) — these read the on-disk old `pg_proc` row, which is
//!     the syscache owner's responsibility;
//!   * the three language validators' bodies
//!     (`fmgr_internal_function` / `load_external_function` + `fetch_finfo_record`
//!     / the SQL-body parse → `pg_analyze_and_rewrite_withcb` → `check_sql_fn_*`
//!     re-parse) — reaching fmgr/dfmgr/parser/executor-functions owners;
//!   * the validator run wrapper (`CommandCounterIncrement` is direct; the GUC
//!     nest-level + `OidFunctionCall1(languageValidator)` cross here because the
//!     nest-level lifetime must wrap the fmgr dispatch);
//!   * `pgstat_create_function` (pgstat owner);
//!   * `function_parse_error_transpose`'s error-position plumbing
//!     (`geterrposition` / `getinternalerrposition` / `errposition` /
//!     `internalerrposition` / `internalerrquery` / the `ActivePortal`
//!     source-text read / `errcontext`).
//!
//! Each seam defaults to a loud panic until its real owner lands —
//! `mirror-pg-and-panic`.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;

/// The fixed columns + the named-parameter / defaults columns of an existing
/// `pg_proc` row that the replace path reads (`Form_pg_proc` GETSTRUCT +
/// `SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, ...)`). Returned by
/// [`search_proc_name_args_nsp`] so the owner makes every replace-path decision
/// against owned values.
#[derive(Clone, Debug)]
pub struct OldProcFacts {
    /// `oldproc->oid`.
    pub oid: Oid,
    /// `oldproc->prokind`.
    pub prokind: i8,
    /// `oldproc->prorettype`.
    pub prorettype: Oid,
    /// `oldproc->proretset`.
    pub proretset: bool,
    /// `oldproc->pronargdefaults`.
    pub pronargdefaults: i16,
    /// `SysCacheGetAttr(Anum_pg_proc_proargnames)` decoded to per-arg names
    /// (`None` ≡ the column is SQL NULL; an inner `None` ≡ an unnamed slot).
    pub proargnames: Option<Vec<Option<String>>>,
    /// `SysCacheGetAttr(Anum_pg_proc_proargmodes)` decoded to the mode chars
    /// (`None` ≡ the column is SQL NULL).
    pub proargmodes: Option<Vec<i8>>,
    /// `SysCacheGetAttr(Anum_pg_proc_proargdefaults)` text
    /// (`TextDatumGetCString`), `None` ≡ SQL NULL.
    pub proargdefaults: Option<String>,
}

seam_core::seam!(
    /// `oldtup = SearchSysCache3(PROCNAMEARGSNSP, name, parameterTypes, nsp)`
    /// (pg_proc.c:386-389): returns the held old tuple (copied into `mcx`) plus
    /// its projected [`OldProcFacts`]. `parameter_types` is the input-argument
    /// `oidvector` (the syscache key frames it). `Ok(None)` is
    /// `!HeapTupleIsValid(oldtup)` (no pre-existing definition). The owner needs
    /// the [`FormedTuple`] to drive `heap_modify_tuple`'s not-replaced columns
    /// (oid/proowner/proacl) on the update path. `Err` carries the lookup error
    /// surface.
    pub fn search_proc_name_args_nsp<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        procedure_name: &str,
        parameter_types: &[Oid],
        proc_namespace: Oid,
    ) -> PgResult<
        Option<(
            OldProcFacts,
            types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        )>,
    >
);

/// Outcome of the `returnType == RECORDOID` OUT-parameter row-type comparison
/// (pg_proc.c:455-477).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordTypeChange {
    /// `olddesc == NULL && newdesc == NULL` — both runtime-defined RECORDs (ok).
    BothRuntime,
    /// `equalRowTypes(olddesc, newdesc)` — equal (ok).
    Equal,
    /// One side NULL, or unequal — the cannot-change-return-type error.
    Different,
}

seam_core::seam!(
    /// pg_proc.c:455-477: `olddesc = build_function_result_tupdesc_t(oldtup);
    /// newdesc = build_function_result_tupdesc_d(prokind, allParameterTypes,
    /// parameterModes, parameterNames);` then classify by `equalRowTypes`. The
    /// old row is identified by `old_funcoid` (the held tuple's OID); the new
    /// descriptor is built from the OUT-parameter inputs. `Err` carries the
    /// tupdesc-build error surface.
    pub fn record_type_change(
        old_funcoid: Oid,
        prokind: i8,
        all_parameter_types: Option<Vec<Oid>>,
        parameter_modes: Option<Vec<i8>>,
        parameter_names: Option<Vec<Option<String>>>,
    ) -> PgResult<RecordTypeChange>
);

seam_core::seam!(
    /// pg_proc.c:484-523: `get_func_input_arg_names(old proargnames,
    /// old proargmodes, &old_arg_names)` vs `get_func_input_arg_names(new
    /// parameterNames, new parameterModes, &new_arg_names)`, returning the first
    /// old input-parameter name that was renamed (the
    /// `cannot change name of input parameter` trigger), or `None` when every
    /// name is unchanged. The old row's `proargnames`/`proargmodes` cross via
    /// the [`OldProcFacts`] the caller holds. `Err` carries the decode error
    /// surface.
    pub fn check_input_param_names_unchanged(
        old_proargnames: Option<Vec<Option<String>>>,
        old_proargmodes: Option<Vec<i8>>,
        new_parameter_names: Option<Vec<Option<String>>>,
        new_parameter_modes: Option<Vec<i8>>,
    ) -> PgResult<Option<String>>
);

/// Outcome of the existing-defaults compatibility check (pg_proc.c:533-573).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultCompat {
    /// Every retained default keeps its `exprType` — ok.
    Ok,
    /// `exprType(oldDef) != exprType(newDef)` — the cannot-change-data-type
    /// error.
    TypeChanged,
}

seam_core::seam!(
    /// pg_proc.c:549-573: `oldDefaults = stringToNode(old proargdefaults)`,
    /// then for each retained old default compare
    /// `exprType(oldDef) != exprType(newDef)` against the tail of the new
    /// `parameterDefaults`. `old_proargdefaults` is the held old row's
    /// `proargdefaults` `nodeToString` text; `new_parameter_defaults` is the
    /// new defaults' `nodeToString` text (the owner already has the new nodes,
    /// serialized for the cross). `Err` carries the parse / type error surface.
    pub fn check_defaults_compatible(
        old_proargdefaults: String,
        old_nargdefaults: i16,
        new_parameter_defaults: Vec<types_parsenodes::Node>,
    ) -> PgResult<DefaultCompat>
);

seam_core::seam!(
    /// `CheckFunctionValidatorAccess(validatorOid, functionOid)` (fmgr.c): may
    /// the current user run `functionOid` through the validator `validatorOid`?
    /// Returns `false` when the validator should silently skip (the C
    /// `PG_RETURN_VOID()` early-out). Reaches the fmgr/syscache validator-access
    /// owner. `Err` carries the lookup error surface.
    pub fn check_function_validator_access(
        validator_fn_oid: Oid,
        func_oid: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `check_function_bodies` GUC read (guc.c): whether function-body checks
    /// run (gates the SQL validator's body parse and the per-function GUC
    /// nest-level). Reaches the GUC owner.
    pub fn check_function_bodies() -> PgResult<bool>
);

/// The fixed facts `fmgr_sql_validator` reads off the `pg_proc` row before the
/// body check (pg_proc.c:854-882): the result type and the declared input arg
/// types (for the pseudotype gates).
#[derive(Clone, Debug)]
pub struct SqlValidatorProcFacts {
    /// `proc->prorettype`.
    pub prorettype: Oid,
    /// `proc->pronargs`.
    pub pronargs: i16,
    /// `proc->proargtypes.values[..pronargs]` — declared input arg types.
    pub proargtypes: Vec<Oid>,
}

seam_core::seam!(
    /// `tuple = SearchSysCache1(PROCOID, funcoid)` + `GETSTRUCT` of the fields
    /// `fmgr_sql_validator` inspects (pg_proc.c:851-854). `Ok(None)` on a cache
    /// miss — the caller raises `cache lookup failed for function %u`. `Err`
    /// carries the lookup error surface.
    pub fn search_proc_oid_sql(funcoid: Oid) -> PgResult<Option<SqlValidatorProcFacts>>
);

seam_core::seam!(
    /// pg_proc.c:761-772: `SearchSysCache1(PROCOID, funcoid)` + `prosrc` read,
    /// then `if (fmgr_internal_function(prosrc) == InvalidOid)` raise the
    /// `there is no built-in function named "%s"` `ERRCODE_UNDEFINED_FUNCTION`
    /// error. The fmgr-builtin lookup is the fmgr owner's. `Err` carries that
    /// error (and the cache-lookup-failed `elog`).
    pub fn validate_internal_function(funcoid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// pg_proc.c:807-818: `SearchSysCache1(PROCOID, funcoid)` + `prosrc`/`probin`
    /// read, then `load_external_function(probin, prosrc, true, &libraryhandle)`
    /// + `fetch_finfo_record(libraryhandle, prosrc)`. The dynamic-loader legs
    /// are the dfmgr owner's. `Err` carries the load / finfo error surface.
    pub fn validate_c_function(funcoid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// pg_proc.c:884-988, the `check_function_bodies` body of `fmgr_sql_validator`:
    /// read `prosrc`/`prosqlbody`, set up the `sql_function_parse_error_callback`
    /// error context, `pg_parse_query` / `AcquireRewriteLocks` /
    /// `pg_rewrite_query` / `pg_analyze_and_rewrite_withcb` re-parse, then
    /// `check_sql_fn_statements` + `get_func_result_type` +
    /// `check_sql_fn_retval`. This reaches the parser/rewriter/executor-functions
    /// owners; the whole body crosses because the `error_context_stack`
    /// push/pop must wrap the cross-crate parse. `Err` carries any parse / type
    /// error.
    pub fn run_sql_function_body_check(funcoid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// pg_proc.c:711-727: the `OidFunctionCall1(languageValidator,
    /// ObjectIdGetDatum(retval))` dispatch wrapped in the per-function GUC
    /// nest-level (`check_function_bodies` gate; `proconfig` SET items applied
    /// via `NewGUCNestLevel` + `ProcessGUCArray`, undone by `AtEOXact_GUC`). The
    /// fmgr dispatch + GUC nest lifetime cross together. `Err` carries the
    /// validator's error.
    pub fn run_language_validator(
        language_validator: Oid,
        retval: Oid,
        proconfig: Option<Vec<String>>,
    ) -> PgResult<()>
);

/* ---- prosqlbody / parameterDefaults cooked-tree operations ---------------- *
 *
 * `commands/functioncmds.c` builds `prosqlbody` (a parsed SQL-body `Node *`) and
 * `parameterDefaults` (a `List *` of cooked default-expr `Node *`) in its own
 * node vocabulary (`types_parsenodes::Node`). pg_proc.c's `nodeToString` of
 * those columns and `recordDependencyOnExpr` over them operate on the cooked
 * `Node` tree, whose serializer (outfuncs.c) and reference walker
 * (dependency.c's `find_expression_references`) need the full cooked-node model.
 * These four seams carry the `types_parsenodes::Node` the consumer produces; the
 * cooked-tree owner (the parser/analyze model that yields real `Query`/expr
 * trees) installs them when it lands. `mirror-pg-and-panic`. */

/* The SQL-function body (`prosqlbody`) is no longer carried as a cooked
 * `types_parsenodes::Node` through pg-proc: `interpret_sql_body` (in the
 * parser-owning crate) serializes the cooked body to its `pg_node_tree` text and
 * extracts its object references up front, so `ProcedureCreate` stores the text
 * directly and records the references without a seam (the
 * `node_to_string_sqlbody` / `record_dependency_on_sqlbody` seams are gone). */

seam_core::seam!(
    /// `nodeToString((Node *) parameterDefaults)` (pg_proc.c:360): serialize the
    /// `List *` of cooked default-expr nodes to its `pg_node_tree` text. `Err`
    /// carries OOM / serializer error.
    pub fn node_to_string_defaults(
        parameter_defaults: Vec<types_parsenodes::Node>,
    ) -> PgResult<String>
);

seam_core::seam!(
    /// `recordDependencyOnExpr(&myself, (Node *) parameterDefaults, NIL,
    /// DEPENDENCY_NORMAL)` (pg_proc.c:670): record the default exprs' object
    /// references against the new function. `Err` carries the heap/index-mutation
    /// `ereport(ERROR)`s.
    pub fn record_dependency_on_defaults(
        func_oid: Oid,
        parameter_defaults: Vec<types_parsenodes::Node>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `pgstat_create_function(retval)` (pgstat): ensure the new function's
    /// stats are dropped if the transaction aborts. Owned by the pgstat unit.
    pub fn pgstat_create_function(func_oid: Oid) -> PgResult<()>
);

/* ---- function_parse_error_transpose error-position plumbing (errcodes.c /
 * elog.c / pquery.c) ------------------------------------------------------- */

seam_core::seam!(
    /// `geterrposition()` (elog.c): the current error's cursor position
    /// (`>0` when set). Reads backend error state. `Err` is unused (infallible
    /// in C) but kept on the seam for the loud-panic default.
    pub fn geterrposition() -> PgResult<i32>
);

seam_core::seam!(
    /// `getinternalerrposition()` (elog.c): the current error's internal cursor
    /// position.
    pub fn getinternalerrposition() -> PgResult<i32>
);

seam_core::seam!(
    /// `errposition(cursorpos)` (elog.c): set the current error's cursor
    /// position.
    pub fn errposition(cursorpos: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `internalerrposition(cursorpos)` (elog.c): set the internal cursor
    /// position.
    pub fn internalerrposition(cursorpos: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `internalerrquery(query)` (elog.c): set/clear the internal query text
    /// (`None` ≡ the C `NULL`).
    pub fn internalerrquery(query: Option<String>) -> PgResult<()>
);

seam_core::seam!(
    /// `(ActivePortal && ActivePortal->status == PORTAL_ACTIVE) ?
    /// ActivePortal->sourceText : NULL` (pquery.c): the active portal's original
    /// command text, or `None` when there is no active portal.
    pub fn active_portal_source_text() -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `errcontext("SQL function \"%s\"", proname)` (elog.c): push the SQL
    /// function context line onto the error stack.
    pub fn errcontext_sql_function(proname: String) -> PgResult<()>
);
