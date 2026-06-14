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
use types_parsenodes::{DefElem, InlineCodeBlock, Node, TypeName, VariableSetStmt};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlterFunctionTarget {
    pub func_oid: Oid,
    pub prokind: i8,
    pub proretset: bool,
    pub prosupport: Oid,
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
    /// The SET/RESET items (`VariableSetStmt` nodes), or `None`.
    pub set_items: Option<Vec<Node>>,
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
    pub prosqlbody: Option<Box<Node>>,
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
    pub parameter_defaults: Vec<Node>,
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
    /// `aclcheck_error_type(aclresult, typeId)` — always raises.
    pub fn aclcheck_error_type(aclresult: AclResult, type_id: Oid) -> PgResult<()>
);

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

seam_core::seam!(
    /// `get_element_type(typid)` — element type OID, or `InvalidOid`.
    pub fn get_element_type(type_oid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_base_element_type(typid)`.
    pub fn get_base_element_type(type_oid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_typtype(typid)` — the `pg_type.typtype` char.
    pub fn get_typtype(type_oid: Oid) -> PgResult<i8>
);

seam_core::seam!(
    /// `get_typlenbyvalalign(typid, ...)` → `(typlen, typbyval, typalign)`.
    pub fn get_typlenbyvalalign(type_oid: Oid) -> PgResult<(i16, bool, i8)>
);

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
    /// `get_language_oid(langname, missing_ok)`.
    pub fn get_language_oid(langname: String, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `extension_file_exists(language)`.
    pub fn extension_file_exists(ext_name: String) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExtractSetVariableArgs(sstmt)` (utils/misc/guc.c) — the SET arg string,
    /// or `None` for a RESET. Owns the `A_Const` arg-list flattening.
    pub fn extract_set_variable_args(sstmt: VariableSetStmt) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `GUCArrayAdd(array, name, value)` (utils/misc/guc.c) — append/replace the
    /// `name=value` entry in the proconfig `text[]`.
    pub fn guc_array_add(
        a: Option<Vec<String>>,
        name: String,
        value: String,
    ) -> PgResult<Vec<String>>
);

seam_core::seam!(
    /// `GUCArrayDelete(array, name)` (utils/misc/guc.c) — drop the `name=...`
    /// entry from the proconfig `text[]` (`None` if the array becomes empty).
    pub fn guc_array_delete(
        a: Option<Vec<String>>,
        name: String,
    ) -> PgResult<Option<Vec<String>>>
);

seam_core::seam!(
    /// `GUCArrayReset(array)` (utils/misc/guc.c) — for a superuser, reset
    /// (drop) all GUC entries, returning `None`; for a non-superuser, drop only
    /// the entries that user may reset, returning the surviving `text[]`
    /// (`None` if it becomes empty). `Err` carries the value-parse error
    /// surface.
    pub fn guc_array_reset(a: Vec<String>) -> PgResult<Option<Vec<String>>>
);

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

seam_core::seam!(
    /// `get_func_rettype(funcid)` (utils/cache/lsyscache.c).
    pub fn get_func_rettype(func_oid: Oid) -> PgResult<Oid>
);

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
    /// `parser_errposition(pstate, location)`.
    pub fn parser_errposition(location: i32) -> i32
);

seam_core::seam!(
    /// The DEFAULT-expression pipeline for one input parameter.
    pub fn transform_parameter_default(defexpr: Node, toid: Oid) -> PgResult<Node>
);

seam_core::seam!(
    /// `pstate->p_rtable != NIL || contain_var_clause(def)`.
    pub fn default_has_table_refs(def: Node) -> PgResult<bool>
);

seam_core::seam!(
    /// The whole inline-SQL-body branch of `interpret_AS_clause`.
    pub fn interpret_sql_body(
        funcname: String,
        sql_body_in: Node,
        parameter_types: Vec<Oid>,
        in_parameter_names: Vec<String>,
        query_string: Option<String>,
    ) -> PgResult<Node>
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
