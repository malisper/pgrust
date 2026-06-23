//! KEYSTONE family for `backend/commands/functioncmds.c`.
//!
//! The shared types / ABI / lifetime foundation every other family compiles
//! against, ported in full so the crate compiles:
//!
//!   * carrier structs surfaced from the per-owner seams crate,
//!   * the `Mcx` + `PgResult` failure-surface conventions,
//!   * the `Node`/`DefElem`/`TypeName` extraction helpers,
//!   * the shared error helpers,
//!   * the language-permission check shared by `CreateFunction`/`ExecuteDoStmt`,
//!   * the `ObjectType`/language-OID constants.
//!
//! Carrier types live in the LAYERED `types-*` stack and the per-owner
//! `backend-commands-functioncmds-seams` crate (NOT the monolithic
//! src-idiomatic `seams`/`types` crates, which do not exist in this repo).

use ::functioncmds_seams::{self as seam, LanguageForm};
use ::utils_error::ereport;
use ::types_acl::{ACLCHECK_NO_PRIV, ACLCHECK_OK, ACL_USAGE};
use ::types_core::Oid;
use ::types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_SYNTAX_ERROR, ERROR,
};
use ::parsenodes::{Boolean, DefElem, Node, StringNode, TypeName};

// `ObjectType` discriminants (nodes/parsenodes.h) — passed to the
// `lookup_func_with_args` seam as an `int`, matching the C `objtype` argument.
pub(crate) const OBJECT_FUNCTION: i32 = 19;
pub(crate) const OBJECT_AGGREGATE: i32 = 1;
pub(crate) const OBJECT_PROCEDURE: i32 = 29;

// Language OIDs (`pg_language.dat`) — compile-time constants in PG.
/// `INTERNALlanguageId`.
pub(crate) const INTERNAL_LANGUAGE_ID: Oid = 12;
/// `ClanguageId`.
pub(crate) const C_LANGUAGE_ID: Oid = 13;
/// `SQLlanguageId`.
pub(crate) const SQL_LANGUAGE_ID: Oid = 14;

/// `errstart`/`errfinish` source location helper.
pub(crate) fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/functioncmds.c", lineno, funcname)
}

/// `defel->defname` — the attribute name of a `DefElem`, or `""` when absent.
pub(crate) fn def_name(defel: &DefElem) -> &str {
    defel.defname.as_deref().unwrap_or("")
}

/// `strVal(node)` — read a `String` node's value.
pub(crate) fn str_val(node: &Node) -> PgResult<String> {
    match node.as_string() {
        Some(StringNode { sval, .. }) => Ok(sval.clone().unwrap_or_default()),
        _ => Err(ereport(ERROR)
            .errmsg_internal("strVal: node is not a String")
            .into_error()),
    }
}

/// `boolVal(node)` — read a `Boolean` node's value.
pub(crate) fn bool_val(node: &Node) -> PgResult<bool> {
    match node.as_boolean() {
        Some(Boolean { boolval, .. }) => Ok(*boolval),
        _ => Err(ereport(ERROR)
            .errmsg_internal("boolVal: node is not a Boolean")
            .into_error()),
    }
}

/// Read a `TypeName` out of a node-link (the seam takes a `TypeName` by value).
pub(crate) fn as_type_name(node: &Node) -> PgResult<TypeName> {
    match node.as_typename() {
        Some(tn) => Ok(tn.clone()),
        _ => Err(ereport(ERROR)
            .errmsg_internal("expected a TypeName node")
            .into_error()),
    }
}

/// Convert a `List *` of `String` (the qualified name) into the
/// `Vec<Option<String>>` shape `QualifiedNameGetCreationNamespace` consumes.
pub(crate) fn string_nodes_to_namelist(names: &[StringNode]) -> Vec<Option<String>> {
    names.iter().map(|s| s.sval.clone()).collect()
}

/// `strVal(defel->arg)`.
pub(crate) fn def_arg_str_val(defel: &DefElem) -> PgResult<String> {
    match &defel.arg {
        Some(node) => str_val(node),
        None => Err(ereport(ERROR)
            .errmsg_internal("DefElem has no arg")
            .into_error()),
    }
}

/// `boolVal(defel->arg)`.
pub(crate) fn def_arg_bool_val(defel: &DefElem) -> PgResult<bool> {
    match &defel.arg {
        Some(node) => bool_val(node),
        None => Err(ereport(ERROR)
            .errmsg_internal("DefElem has no arg")
            .into_error()),
    }
}

/// `NameListToString(names)` — render a qualified name list for messages.
pub(crate) fn name_list_to_string(names: &[String]) -> String {
    names.join(".")
}

/// `goto procedure_error;` target in compute_common_attribute. `query_string`
/// is the active query source (`pstate->p_sourcetext`), forwarded to
/// `parser_errposition` so the error carries a cursor position (`LINE 1: ...^`).
pub(crate) fn procedure_error(defel: &DefElem, query_string: Option<&str>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
        .errmsg("invalid attribute in procedure definition")
        .errposition(seam::parser_errposition::call(
            query_string.map(|s| s.to_string()),
            defel.location,
        ))
        .into_error()
}

/// `errorConflictingDefElem(defel, pstate)` (commands/define.c).
pub(crate) fn error_conflicting_def_elem(defel: &DefElem) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(seam::parser_errposition::call(None, defel.location))
        .into_error()
}

/// `errmsg("cache lookup failed for function %u", funcid)`.
pub(crate) fn cache_lookup_failed_function(funcid: Oid) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(format!("cache lookup failed for function {funcid}"))
        .into_error()
}

/// The trusted/untrusted language permission check shared by `CreateFunction`
/// and `ExecuteDoStmt`.
pub(crate) fn check_language_permissions(language_struct: &LanguageForm) -> PgResult<()> {
    if language_struct.lanpltrusted {
        /* if trusted language, need USAGE privilege */
        let aclresult = seam::language_aclcheck::call(
            language_struct.oid,
            seam::get_user_id::call()?,
            ACL_USAGE,
        )?;
        if aclresult != ACLCHECK_OK {
            seam::aclcheck_error_language::call(aclresult, language_struct.lanname.clone())?;
        }
    } else {
        /* if untrusted language, must be superuser */
        if !seam::superuser::call()? {
            seam::aclcheck_error_language::call(ACLCHECK_NO_PRIV, language_struct.lanname.clone())?;
        }
    }
    Ok(())
}
