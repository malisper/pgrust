//! Seam declarations for `backend-commands-functioncmds` (functioncmds.c).
//!
//! Every external the function/cast/transform/DO/CALL command drivers call
//! that would otherwise cycle back into this crate's owners — the ACL
//! machinery, type/function/language lookups, the `defGet*` accessors, the GUC
//! proconfig array op, the parser transform boundary (parameter defaults /
//! inline SQL bodies), the catalog-munging helpers
//! (`ProcedureCreate`/`CastCreate`/transform insert/`RemoveFunctionById`), and
//! the executor/fmgr DO/CALL invocation — is declared here. Each panics until
//! its owner lands.
//!
//! Signature carriers (the form/arg bundles the owners trade in) live here too:
//! they belong to the not-yet-ported owners and reference the parse-tree
//! vocabulary in `types-parsenodes`.

use types_acl::{AclMode, AclResult};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use parsenodes::{DefElem, InlineCodeBlock, Node, TypeName};

// ---------------------------------------------------------------------------
// Signature carriers
// ---------------------------------------------------------------------------

/// Result of `LookupTypeName(...)` + `typisdefined` + `typeTypeId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LookupTypeResult {
    /// `typeTypeId(typtup)`.
    pub type_oid: Oid,
    /// `((Form_pg_type) GETSTRUCT(typtup))->typisdefined`.
    pub typisdefined: bool,
}

/// The pg_language form fields `CreateFunction` / `ExecuteDoStmt` read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LanguageForm {
    pub oid: Oid,
    pub lanpltrusted: bool,
    pub lanvalidator: Oid,
    pub laninline: Oid,
    pub lanname: String,
}

/// The pg_proc form fields `CreateCast` reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CastFuncForm {
    pub pronargs: i16,
    pub prorettype: Oid,
    pub proargtypes: [Oid; 3],
    pub prokind: i8,
    pub proretset: bool,
}

/// The pg_proc form fields `check_transform_function` reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransformFuncForm {
    pub provolatile: i8,
    pub prokind: i8,
    pub proretset: bool,
    pub pronargs: i16,
    pub proargtype0: Oid,
    pub prorettype: Oid,
}

/// Lookup + permission preamble result of `AlterFunction`'s opening.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterFunctionTarget {
    pub func_oid: Oid,
    pub prokind: i8,
    pub proretset: bool,
    pub prosupport: Oid,
    /// The held row's existing `proconfig` (`SysCacheGetAttr(PROCOID,
    /// Anum_pg_proc_proconfig)` decoded to `name=value` strings), `None` ≡ SQL
    /// NULL. `AlterFunction` feeds it to `update_proconfig_value` as the base
    /// array when a SET/RESET action is present.
    pub proconfig: Option<Vec<String>>,
}

/// The set of pg_proc fields `AlterFunction` decided to overwrite.
#[derive(Clone, Debug, Default)]
pub struct AlterFunctionChanges {
    pub func_oid: Oid,
    pub provolatile: Option<i8>,
    pub proisstrict: Option<bool>,
    pub prosecdef: Option<bool>,
    pub proleakproof: Option<bool>,
    pub procost: Option<f64>,
    pub prorows: Option<f64>,
    pub prosupport: Option<Oid>,
    pub proparallel: Option<i8>,
    /// The new `proconfig` array, when one or more SET/RESET actions were given
    /// (`AlterFunction` already merged them onto the existing array with
    /// `update_proconfig_value`). The outer `Some` is the C `repl_repl[
    /// Anum_pg_proc_proconfig - 1] = true` gate; the inner `None` is the
    /// `repl_null[..] = true` (RESET ALL emptied the array) arm.
    pub proconfig: Option<Option<Vec<String>>>,
}

/// The bundle of arguments `CreateFunction` passes to `ProcedureCreate`.
#[derive(Clone, Debug)]
pub struct ProcedureCreateArgs {
    pub procedure_name: String,
    pub namespace_id: Oid,
    pub replace: bool,
    pub returns_set: bool,
    pub prorettype: Oid,
    pub proowner: Oid,
    pub language_oid: Oid,
    pub language_validator: Oid,
    pub prosrc: String,
    pub probin: Option<String>,
    /// The cooked SQL-function body, already serialized to its `pg_node_tree`
    /// text by `interpret_sql_body` (`None` for the classic `AS '...'` form).
    pub prosqlbody: Option<String>,
    /// The body's referenced objects (extracted from the in-memory cooked node),
    /// recorded against the new function as `DEPENDENCY_NORMAL` dependencies.
    pub prosqlbody_refs: Vec<ObjectAddress>,
    pub prokind: i8,
    pub security: bool,
    pub is_leak_proof: bool,
    pub is_strict: bool,
    pub volatility: i8,
    pub parallel: i8,
    pub parameter_types: Vec<Oid>,
    pub all_parameter_types: Option<Vec<Oid>>,
    pub parameter_modes: Option<Vec<i8>>,
    pub parameter_names: Option<Vec<Option<String>>>,
    /// The cooked `parameterDefaults` `List`, already serialized to its
    /// `pg_node_tree` text (`proargdefaults`) plus the object references it
    /// depends on — produced up front by `cook_parameter_defaults` (mirrors the
    /// prosqlbody path). `text: None` for a function with no defaults.
    pub parameter_defaults: CookedParameterDefaults,
    pub trftypes: Option<Vec<Oid>>,
    pub trfoids: Vec<Oid>,
    pub proconfig: Option<Vec<String>>,
    pub prosupport: Oid,
    pub procost: f32,
    pub prorows: f32,
}

// ---------------------------------------------------------------------------
// Seam declarations
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `GetUserId()` — the current effective user.
    pub fn get_user_id() -> PgResult<Oid>
);

seam_core::seam!(
    /// `superuser()` — whether the current user is a superuser.
    pub fn superuser() -> PgResult<bool>
);

seam_core::seam!(
    /// `object_aclcheck(NamespaceRelationId, namespaceId, roleid, mode)`.
    pub fn namespace_aclcheck(namespace_id: Oid, role_id: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `object_aclcheck(TypeRelationId, typeId, roleid, mode)`.
    pub fn type_aclcheck(type_id: Oid, role_id: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `object_aclcheck(LanguageRelationId, langoid, roleid, mode)`.
    pub fn language_aclcheck(lang_oid: Oid, role_id: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `object_aclcheck(ProcedureRelationId, funcoid, roleid, mode)`.
    pub fn proc_aclcheck(func_oid: Oid, role_id: Oid, mode: AclMode) -> PgResult<AclResult>
);

seam_core::seam!(
    /// `object_ownercheck(TypeRelationId, typeId, roleid)`.
    pub fn type_ownercheck(type_id: Oid, role_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `object_ownercheck(ProcedureRelationId, funcoid, roleid)`.
    pub fn proc_ownercheck(func_oid: Oid, role_id: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(nsp))` — always raises.
    pub fn aclcheck_error_schema(aclresult: AclResult, objname: Option<String>) -> PgResult<()>
);

seam_core::seam!(
    /// `aclcheck_error(aclresult, OBJECT_LANGUAGE, lanname)` — always raises.
    pub fn aclcheck_error_language(aclresult: AclResult, objname: String) -> PgResult<()>
);

seam_core::seam!(
    /// `aclcheck_error(aclresult, OBJECT_FUNCTION, name)` — always raises.
    pub fn aclcheck_error_function(aclresult: AclResult, objname: String) -> PgResult<()>
);

seam_core::seam!(
    /// `aclcheck_error(aclresult, OBJECT_PROCEDURE, name)` — always raises.
    /// The CALL (`ExecuteCallStmt`) EXECUTE-privilege error: a procedure, not a
    /// function, so it reports "permission denied for procedure %s".
    pub fn aclcheck_error_procedure(aclresult: AclResult, objname: String) -> PgResult<()>
);

// NOTE: `aclcheck_error_type` (aclchk.c) was RE-HOMED to
// `backend-catalog-aclchk-seams` (its real owner is aclchk.c, which now has a
// ported owner crate; functioncmds was merely its first consumer). Consumers
// (functioncmds, objectaddress) call `aclchk_seams::aclcheck_error_type`.

seam_core::seam!(
    /// `typenameTypeId(NULL, typeName)`.
    pub fn typename_type_id(type_name: TypeName) -> PgResult<Oid>
);

seam_core::seam!(
    /// `TypeNameToString(typeName)`.
    pub fn type_name_to_string(type_name: TypeName) -> PgResult<String>
);

seam_core::seam!(
    /// `get_namespace_name(nspid)` — `None` when it no longer exists.
    pub fn get_namespace_name(nspid: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `get_func_name(funcid)` — `None` when the function no longer exists.
    pub fn get_func_name(func_oid: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `get_language_name(langid, false)`.
    pub fn get_language_name(lang_oid: Oid) -> PgResult<String>
);

// `get_element_type`, `get_base_element_type`, `get_typtype`,
// `get_typlenbyvalalign` are NOT declared here: they are lsyscache.c functions
// whose canonical (installed) contract lives in
// `backend-utils-cache-lsyscache-seams`. functioncmds calls that channel
// directly (contract unified 2026-06-17).

seam_core::seam!(
    /// `TypeShellMake(typename, namespaceId, ownerId)` → the shell type's address.
    pub fn type_shell_make(typname: String, nsp: Oid, owner: Oid) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `LookupFuncWithArgs(objtype, func, missing_ok)`.
    pub fn lookup_func_with_args(objtype: i32, func: Node, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `funcname_signature_string(proname, pronargs, NIL, argtypes)`.
    pub fn funcname_signature_string(
        proname: String,
        pronargs: i32,
        arg_types: Vec<Oid>,
    ) -> PgResult<String>
);

seam_core::seam!(
    /// `defGetNumeric(def)` → its `float8` value.
    pub fn def_get_numeric(defel: DefElem) -> PgResult<f64>
);

seam_core::seam!(
    /// The AS clause `def->arg` — returned as the owned `String` `Node`s.
    pub fn def_get_as_clause(defel: DefElem) -> PgResult<Vec<Node>>
);

seam_core::seam!(
    /// The TRANSFORM clause `def->arg` — returned as the owned `TypeName` `Node`s.
    pub fn def_get_transform_type_names(defel: DefElem) -> PgResult<Vec<Node>>
);

seam_core::seam!(
    /// `get_transform_oid(type_id, lang_id, missing_ok)` (functioncmds.c): the
    /// OID of the transform for (`type_id`, `lang_id`), or `InvalidOid` with
    /// `missing_ok = true`. With `missing_ok = false` a miss raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`).
    pub fn get_transform_oid(
        mcx: mcx::Mcx<'_>,
        type_id: Oid,
        lang_id: Oid,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `extension_file_exists(language)`.
    pub fn extension_file_exists(ext_name: String) -> PgResult<bool>
);

// NOTE: `ExtractSetVariableArgs` (guc_funcs.c) was mis-homed here because
// functioncmds was its first consumer; its decl now lives on guc_funcs.c's own
// `-seams` crate (`backend-utils-misc-guc-funcs-seams`), where its real owner
// installs it (CONTRACT_RECONCILE_PENDING retired). Consumers call it there.

// (GUCArrayAdd/Delete/Reset re-homed to backend-utils-misc-guc-seams — guc.c's
// real owner — and installed by backend-utils-misc-guc. Consumers call them
// there.)

seam_core::seam!(
    /// `defGetQualifiedName(def)` (commands/define.c) — the qualified name list.
    pub fn def_get_qualified_name(defel: DefElem) -> PgResult<Vec<String>>
);

seam_core::seam!(
    /// `LookupFuncName(funcname, nargs, argtypes, missing_ok)`
    /// (parser/parse_func.c).
    pub fn lookup_func_name(
        funcname: Vec<String>,
        nargs: i32,
        argtypes: Vec<Oid>,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

// `get_func_rettype` is NOT declared here: its canonical (installed) contract
// lives in `backend-utils-cache-lsyscache-seams`; functioncmds calls that
// channel directly (contract unified 2026-06-17).

seam_core::seam!(
    /// `func_signature_string(funcname, nargs, NIL, argtypes)`
    /// (parser/parse_func.c) — render `name(argtype, ...)`.
    pub fn func_signature_string(
        funcname: Vec<String>,
        nargs: i32,
        argtypes: Vec<Oid>,
    ) -> PgResult<String>
);

seam_core::seam!(
    /// `NameListToString(names)` (catalog/namespace.c) — render a possibly
    /// qualified name list as a dotted string.
    pub fn name_list_to_string(names: Vec<String>) -> PgResult<String>
);

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c): returns the
    /// 1-based character cursor position
    /// (`pg_mbstrlen_with_len(p_sourcetext, location) + 1`) for a `>= 0`
    /// `location`, or `0` when `location < 0` or the source text is `None`
    /// (the C `pstate == NULL || pstate->p_sourcetext == NULL` no-op). The
    /// installer carries the active query string (`pstate->p_sourcetext`).
    pub fn parser_errposition(source: Option<String>, location: i32) -> i32
);

seam_core::seam!(
    /// The DEFAULT-expression pipeline for one input parameter
    /// (functioncmds.c:419-447). Transforms the raw rich `defexpr`
    /// (`transformExpr` with `EXPR_KIND_FUNCTION_DEFAULT`), coerces it to the
    /// parameter type (`coerce_to_specific_type(..., "DEFAULT")`), assigns
    /// collations (`assign_expr_collations`), and enforces the no-table-references
    /// rule (`pstate->p_rtable != NIL || contain_var_clause(def)` —
    /// `p_rtable` is always NIL for the fresh DEFAULT pstate, so the check reduces
    /// to `contain_var_clause`). Returns the cooked default as a rich node
    /// allocated in `mcx` (later serialized as the `proargdefaults` `List`).
    /// The owner (`backend-parser-analyze`) installs it: it owns `transformExpr`,
    /// `coerce_to_specific_type`, `assign_expr_collations`, and
    /// `contain_var_clause`. `query_string` carries `pstate->p_sourcetext` for
    /// error positions.
    pub fn transform_parameter_default<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        defexpr: &nodes::nodes::Node<'mcx>,
        toid: Oid,
        location: i32,
        query_string: Option<String>,
    ) -> PgResult<nodes::nodes::Node<'mcx>>
);

/// The cooked `parameterDefaults` `List`, ready for storage: its serialized
/// `pg_node_tree` text (`nodeToString` of the whole `List`) plus the object
/// references the defaults depend on (`recordDependencyOnExpr`'s reference half),
/// to be recorded against the new function in `ProcedureCreate`. Mirrors
/// [`InterpretedSqlBody`]; lets `ProcedureCreate` store the text without owning
/// the cooked-node serializer.
#[derive(Clone, Debug, Default)]
pub struct CookedParameterDefaults {
    /// `nodeToString((Node *) parameterDefaults)` — the stored `pg_node_tree`,
    /// `None` when there are no defaults.
    pub text: Option<String>,
    /// `pronargdefaults` — the number of defaulted parameters (list_length of the
    /// cooked `List`), carried so `ProcedureCreate` need not re-parse the text.
    pub nargdefaults: i32,
    /// The defaults' referenced objects (`find_expr_references` over the cooked
    /// `List`), recorded against the new function in `ProcedureCreate`.
    pub refs: Vec<ObjectAddress>,
}

seam_core::seam!(
    /// Serialize the cooked `parameterDefaults` `List` to its `pg_node_tree` text
    /// (`nodeToString`, pg_proc.c:360) and collect its object references
    /// (`recordDependencyOnExpr` reference half, pg_proc.c:670). The owner
    /// (`backend-parser-analyze`) installs it: it owns the rich `nodeToString`
    /// serializer and the dependency-reference walker. An empty `defaults`
    /// yields `text: None`, `refs: []`.
    pub fn cook_parameter_defaults<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        defaults: Vec<&nodes::nodes::Node<'mcx>>,
    ) -> PgResult<CookedParameterDefaults>
);

/// The product of `interpret_AS_clause`'s inline-SQL-body branch: the serialized
/// `pg_node_tree` text (stored in `pg_proc.prosqlbody`) plus the object
/// references the cooked body depends on, extracted from the *in-memory* cooked
/// node (so dependency recording never has to round-trip the text back through
/// `stringToNode`, matching C's in-memory `recordDependencyOnExpr`).
#[derive(Clone, Debug, Default)]
pub struct InterpretedSqlBody {
    /// `nodeToString(*sql_body_out)` — the stored `pg_node_tree`.
    pub text: String,
    /// The body's referenced objects (`find_expr_references` over the cooked
    /// node), to be recorded against the new function in `ProcedureCreate`.
    pub body_refs: Vec<ObjectAddress>,
}

seam_core::seam!(
    /// The whole inline-SQL-body branch of `interpret_AS_clause`
    /// (functioncmds.c:910). Transforms the raw `sql_body_in` (a `ReturnStmt`
    /// for `RETURN expr`, or the `BEGIN ATOMIC ... END` statement list) into the
    /// cooked SQL-body node-tree and returns its serialized `pg_node_tree` text
    /// (`nodeToString`) plus the body's object references. The owner
    /// (`backend-parser-analyze`) installs it: it owns `transformStmt`, rich
    /// `nodeToString`, and the dependency-reference walker.
    pub fn interpret_sql_body<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        funcname: String,
        sql_body_in: &nodes::nodes::Node<'mcx>,
        parameter_types: Vec<Oid>,
        in_parameter_names: Vec<String>,
        query_string: Option<String>,
    ) -> PgResult<InterpretedSqlBody>
);

seam_core::seam!(
    /// Guts of `RemoveFunctionById`.
    pub fn remove_function_tuple(func_oid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `CastCreate(...)` (catalog/pg_cast.c).
    pub fn cast_create(
        source_type: Oid,
        target_type: Oid,
        func_id: Oid,
        in_cast_id: Oid,
        out_cast_id: Oid,
        cast_context: i8,
        cast_method: i8,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `IsBinaryCoercibleWithCast(srctype, targettype, &castoid)` → `(coercible, castoid)`.
    pub fn is_binary_coercible_with_cast(
        source_type: Oid,
        target_type: Oid,
    ) -> PgResult<(bool, Oid)>
);

seam_core::seam!(
    /// The full pg_transform catalog insert/update of `CreateTransform`.
    pub fn create_transform_tuple(
        type_id: Oid,
        lang_id: Oid,
        fromsql_func: Oid,
        tosql_func: Oid,
        replace: bool,
        lang_name: String,
    ) -> PgResult<ObjectAddress>
);

seam_core::seam!(
    /// `SearchSysCacheExists3(PROCNAMEARGSNSP, proname, proargtypes, nspOid)`.
    pub fn function_exists_in_namespace(
        proname: String,
        proargtypes: Vec<Oid>,
        nsp: Oid,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `changeDependencyFor(ProcedureRelationId, funcOid, ...)` → count changed.
    pub fn change_support_dependency(
        func_oid: Oid,
        old_support: Oid,
        new_support: Oid,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `recordDependencyOn(&address, &referenced, DEPENDENCY_NORMAL)`.
    pub fn record_support_dependency(func_oid: Oid, new_support: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `OidFunctionCall1(laninline, PointerGetDatum(codeblock))` — DO inline handler.
    pub fn execute_inline_handler(laninline: Oid, codeblock: InlineCodeBlock) -> PgResult<()>
);

// NOTE: `execute_call_stmt` and `call_stmt_result_desc` were re-homed out of
// this crate by the call_stmt decomp: their bodies are genuine unported-owner
// work (the planner `FuncExpr` expression tree + execExpr eval + runtime
// params/dest for the former; funcapi `build_function_result_tupdesc_t` +
// nodeFuncs `exprType` for the latter), not functioncmds' own logic.
// `execute_call_stmt` now lives in `backend-executor-execMain-seams`;
// `call_stmt_result_desc` in `backend-utils-fmgr-funcapi-seams`.

seam_core::seam!(
    /// `LookupTypeName(...)` + `typisdefined` + `typeTypeId` + `ReleaseSysCache`.
    pub fn lookup_type_name(type_name: TypeName) -> PgResult<Option<LookupTypeResult>>
);

seam_core::seam!(
    /// `SearchSysCache1(LANGNAME, language)` + `GETSTRUCT` + `ReleaseSysCache`.
    pub fn lookup_language_by_name(langname: String) -> PgResult<Option<LanguageForm>>
);

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, funcid)` + `GETSTRUCT` cast-function form read.
    pub fn fetch_cast_func_form(func_id: Oid) -> PgResult<Option<CastFuncForm>>
);

seam_core::seam!(
    /// `SearchSysCache1(PROCOID, funcid)` + `GETSTRUCT` transform-function form.
    pub fn fetch_transform_func_form(func_id: Oid) -> PgResult<Option<TransformFuncForm>>
);

seam_core::seam!(
    /// `AlterFunction`'s lookup + permission preamble + form-field read.
    pub fn alter_function_begin(objtype: i32, func: Node) -> PgResult<AlterFunctionTarget>
);

seam_core::seam!(
    /// Apply `AlterFunction`'s collected changes to the pg_proc tuple.
    pub fn alter_function_apply(changes: AlterFunctionChanges) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcedureCreate(...)` (catalog/pg_proc.c) — the actual catalog insert.
    pub fn procedure_create(args: ProcedureCreateArgs) -> PgResult<ObjectAddress>
);
