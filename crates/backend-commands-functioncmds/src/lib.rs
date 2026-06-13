#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `backend/commands/functioncmds.c` — CREATE/ALTER/DROP FUNCTION & PROCEDURE,
//! CREATE CAST, CREATE TRANSFORM, DO, CALL support (PostgreSQL 18.3).
//!
//! Every C function — public (`CreateFunction`, `RemoveFunctionById`,
//! `AlterFunction`, `CreateCast`, `CreateTransform`, `get_transform_oid`,
//! `IsThereFunctionInNamespace`, `ExecuteDoStmt`, `ExecuteCallStmt`,
//! `CallStmtResultDesc`, `interpret_function_parameter_list`) and static
//! (`compute_return_type`, `compute_common_attribute`,
//! `interpret_func_volatility`, `interpret_func_parallel`,
//! `compute_function_attributes`, `interpret_AS_clause`,
//! `check_transform_function`) — is implemented in-crate with identical branch
//! order, permission-check ordering, constants, error messages, SQLSTATEs, and
//! the same argument bundle handed to `ProcedureCreate`/`CastCreate`.
//!
//! `QualifiedNameGetCreationNamespace` (foundation crate) and the
//! `GetSysCacheOid2(TRFTYPELANG, …)` core of `get_transform_oid` are called
//! directly; every other genuine external crosses
//! [`backend_commands_functioncmds_seams`], panicking until its owner lands.

use backend_catalog_namespace::QualifiedNameGetCreationNamespace;
use backend_commands_functioncmds_seams::{
    self as seam, AlterFunctionChanges, CastFuncForm, LanguageForm, ProcedureCreateArgs,
    TransformFuncForm,
};
use backend_utils_error::ereport;
use mcx::Mcx;
use types_acl::{
    ACLCHECK_NOT_OWNER, ACLCHECK_NO_PRIV, ACLCHECK_OK, ACL_CREATE, ACL_EXECUTE, ACL_USAGE,
};
use types_catalog::catalog_dependency::ObjectAddress;
use types_cache::SysCacheKey;
use types_core::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_datum::Datum;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE, WARNING,
};
use types_parsenodes::{
    AlterFunctionStmt, Boolean, CallStmt, CoercionContext, CreateCastStmt, CreateFunctionStmt,
    CreateTransformStmt, DefElem, DoStmt, FunctionParameter, InlineCodeBlock, Node, StringNode,
    TypeName, COERCION_CODE_ASSIGNMENT, COERCION_CODE_EXPLICIT, COERCION_CODE_IMPLICIT,
    COERCION_METHOD_BINARY, COERCION_METHOD_FUNCTION, COERCION_METHOD_INOUT, FUNC_PARAM_DEFAULT,
    FUNC_PARAM_IN, FUNC_PARAM_OUT, FUNC_PARAM_TABLE, FUNC_PARAM_VARIADIC, PROKIND_FUNCTION,
    PROKIND_PROCEDURE, PROKIND_WINDOW, PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE, PROPARALLEL_UNSAFE,
    PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE, PROVOLATILE_VOLATILE, ProcedureRelationId,
    TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE, TYPTYPE_PSEUDO,
    TYPTYPE_RANGE,
};
use types_tuple::{
    TupleDesc, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYOID, BOOLOID, INT4OID, INTERNALOID, RECORDOID,
    VOIDOID,
};

// `ObjectType` discriminants (nodes/parsenodes.h) — passed to the
// `lookup_func_with_args` seam as an `int`, matching the C `objtype` argument.
const OBJECT_FUNCTION: i32 = 19;
const OBJECT_AGGREGATE: i32 = 1;
const OBJECT_PROCEDURE: i32 = 29;

// Language OIDs (`pg_language.dat`) — compile-time constants in PG
// (`pg_language_d.h`), not catalog lookups.
/// `INTERNALlanguageId`.
const INTERNAL_LANGUAGE_ID: Oid = 12;
/// `ClanguageId`.
const C_LANGUAGE_ID: Oid = 13;
/// `SQLlanguageId`.
const SQL_LANGUAGE_ID: Oid = 14;

/// `errstart`/`errfinish` source location helper.
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/functioncmds.c", lineno, funcname)
}

/// `defel->defname` — the attribute name of a `DefElem`, or `""` when absent.
fn def_name(defel: &DefElem) -> &str {
    defel.defname.as_deref().unwrap_or("")
}

/// `strVal(node)` — read a `String` node's value.
fn str_val(node: &Node) -> PgResult<String> {
    match node.as_string() {
        Some(StringNode { sval, .. }) => Ok(sval.clone().unwrap_or_default()),
        _ => Err(ereport(ERROR)
            .errmsg_internal("strVal: node is not a String")
            .into_error()),
    }
}

/// `boolVal(node)` — read a `Boolean` node's value.
fn bool_val(node: &Node) -> PgResult<bool> {
    match node.as_boolean() {
        Some(Boolean { boolval, .. }) => Ok(*boolval),
        _ => Err(ereport(ERROR)
            .errmsg_internal("boolVal: node is not a Boolean")
            .into_error()),
    }
}

/// Read a `TypeName` out of a node-link (the seam takes a `TypeName` by value).
fn as_type_name(node: &Node) -> PgResult<TypeName> {
    match node.as_typename() {
        Some(tn) => Ok(tn.clone()),
        _ => Err(ereport(ERROR)
            .errmsg_internal("expected a TypeName node")
            .into_error()),
    }
}

/// Convert a `List *` of `String` (the qualified name) into the
/// `Vec<Option<String>>` shape `QualifiedNameGetCreationNamespace` consumes.
fn string_nodes_to_namelist(names: &[StringNode]) -> Vec<Option<String>> {
    names.iter().map(|s| s.sval.clone()).collect()
}

// ===========================================================================
// compute_return_type (functioncmds.c:87)
// ===========================================================================

fn compute_return_type(
    mcx: Mcx<'_>,
    return_type: &TypeName,
    language_oid: Oid,
) -> PgResult<(Oid, bool)> {
    let rettype: Oid;

    let typtup = seam::lookup_type_name::call(return_type.clone())?;

    if let Some(typtup) = typtup {
        if !typtup.typisdefined {
            if language_oid == SQL_LANGUAGE_ID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(format!(
                        "SQL function cannot return shell type {}",
                        seam::type_name_to_string::call(return_type.clone())?
                    ))
                    .into_error());
            } else {
                ereport(NOTICE)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!(
                        "return type {} is only a shell",
                        seam::type_name_to_string::call(return_type.clone())?
                    ))
                    .finish(errloc(107, "compute_return_type"))?;
            }
        }
        rettype = typtup.type_oid;
    } else {
        let typnam = seam::type_name_to_string::call(return_type.clone())?;

        /*
         * Only C-coded functions can be I/O functions.  We enforce this
         * restriction here mainly to prevent littering the catalogs with shell
         * types due to simple typos in user-defined function definitions.
         */
        if language_oid != INTERNAL_LANGUAGE_ID && language_oid != C_LANGUAGE_ID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("type \"{typnam}\" does not exist"))
                .into_error());
        }

        /* Reject if there's typmod decoration, too */
        if !return_type.typmods.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "type modifier cannot be specified for shell type \"{typnam}\""
                ))
                .into_error());
        }

        /* Otherwise, go ahead and make a shell type */
        ereport(NOTICE)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("type \"{typnam}\" is not yet defined"))
            .errdetail("Creating a shell type definition.")
            .finish(errloc(142, "compute_return_type"))?;

        let name_cells: Vec<Option<String>> = return_type
            .names
            .iter()
            .map(|n| match n.as_string() {
                Some(StringNode { sval, .. }) => sval.clone(),
                _ => None,
            })
            .collect();
        let (namespace_id, typname) = QualifiedNameGetCreationNamespace(mcx, &name_cells)?;
        let typname = typname.to_string();
        let aclresult =
            seam::namespace_aclcheck::call(namespace_id, seam::get_user_id::call()?, ACL_CREATE)?;
        if aclresult != ACLCHECK_OK {
            seam::aclcheck_error_schema::call(
                aclresult,
                seam::get_namespace_name::call(namespace_id)?,
            )?;
        }
        let address =
            seam::type_shell_make::call(typname, namespace_id, seam::get_user_id::call()?)?;
        rettype = address.objectId;
        debug_assert!(OidIsValid(rettype));
    }

    let aclresult = seam::type_aclcheck::call(rettype, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, rettype)?;
    }

    Ok((rettype, return_type.setof))
}

// ===========================================================================
// interpret_function_parameter_list (functioncmds.c:182)
// ===========================================================================

/// The out-parameter bundle filled by [`interpret_function_parameter_list`].
#[derive(Clone, Debug, Default)]
pub struct InterpretedParameters {
    pub parameter_types: Vec<Oid>,
    pub parameter_types_list: Vec<Oid>,
    pub all_parameter_types: Option<Vec<Oid>>,
    pub parameter_modes: Option<Vec<i8>>,
    pub parameter_names: Option<Vec<Option<String>>>,
    pub in_parameter_names_list: Vec<String>,
    pub parameter_defaults: Vec<Node>,
    pub variadic_arg_type: Oid,
    pub required_result_type: Oid,
}

/// `interpret_function_parameter_list(pstate, parameters, languageOid, objtype,
/// ...)` (functioncmds.c:182). `objtype` is the `ObjectType` int.
pub fn interpret_function_parameter_list(
    parameters: &[Node],
    language_oid: Oid,
    objtype: i32,
    want_parameter_types_list: bool,
    want_in_parameter_names_list: bool,
) -> PgResult<InterpretedParameters> {
    let parameter_count = parameters.len();
    let mut in_types: Vec<Oid> = Vec::with_capacity(parameter_count);
    let mut all_types: Vec<Oid> = vec![InvalidOid; parameter_count];
    let mut param_modes: Vec<i8> = vec![0; parameter_count];
    let mut param_names: Vec<Option<String>> = vec![None; parameter_count];
    let mut out_count = 0i32;
    let mut var_count = 0i32;
    let mut have_names = false;
    let mut have_defaults = false;

    let mut out = InterpretedParameters::default();

    /* Scan the list and extract data into work arrays */
    let cells: Vec<&FunctionParameter> = parameters
        .iter()
        .map(|node| match node.as_functionparameter() {
            Some(fp) => Ok(fp),
            _ => Err(ereport(ERROR)
                .errmsg_internal("interpret_function_parameter_list: not a FunctionParameter")
                .into_error()),
        })
        .collect::<PgResult<Vec<_>>>()?;

    for (i, &fp) in cells.iter().enumerate() {
        let t = match &fp.argType {
            Some(b) => as_type_name(b)?,
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal("FunctionParameter without an argType")
                    .into_error());
            }
        };
        let mut fpmode = fp.mode;
        let mut isinput = false;
        let toid: Oid;

        /* For our purposes here, a defaulted mode spec is identical to IN */
        if fpmode == FUNC_PARAM_DEFAULT {
            fpmode = FUNC_PARAM_IN;
        }

        let typtup = seam::lookup_type_name::call(t.clone())?;
        if let Some(typtup) = typtup {
            if !typtup.typisdefined {
                /* As above, hard error if language is SQL */
                if language_oid == SQL_LANGUAGE_ID {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg(format!(
                            "SQL function cannot accept shell type {}",
                            seam::type_name_to_string::call(t.clone())?
                        ))
                        .errposition(seam::parser_errposition::call(t.location))
                        .into_error());
                } else if objtype == OBJECT_AGGREGATE {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg(format!(
                            "aggregate cannot accept shell type {}",
                            seam::type_name_to_string::call(t.clone())?
                        ))
                        .errposition(seam::parser_errposition::call(t.location))
                        .into_error());
                } else {
                    ereport(NOTICE)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!(
                            "argument type {} is only a shell",
                            seam::type_name_to_string::call(t.clone())?
                        ))
                        .errposition(seam::parser_errposition::call(t.location))
                        .finish(errloc(255, "interpret_function_parameter_list"))?;
                }
            }
            toid = typtup.type_oid;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "type {} does not exist",
                    seam::type_name_to_string::call(t.clone())?
                ))
                .errposition(seam::parser_errposition::call(t.location))
                .into_error());
        }

        let aclresult = seam::type_aclcheck::call(toid, seam::get_user_id::call()?, ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            seam::aclcheck_error_type::call(aclresult, toid)?;
        }

        if t.setof {
            if objtype == OBJECT_AGGREGATE {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("aggregates cannot accept set arguments")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            } else if objtype == OBJECT_PROCEDURE {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("procedures cannot accept set arguments")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("functions cannot accept set arguments")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            }
        }

        /* handle input parameters */
        if fpmode != FUNC_PARAM_OUT && fpmode != FUNC_PARAM_TABLE {
            /* other input parameters can't follow a VARIADIC parameter */
            if var_count > 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("VARIADIC parameter must be the last input parameter")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            }
            in_types.push(toid);
            isinput = true;
            if want_parameter_types_list {
                out.parameter_types_list.push(toid);
            }
        }

        /* handle output parameters */
        if fpmode != FUNC_PARAM_IN && fpmode != FUNC_PARAM_VARIADIC {
            if objtype == OBJECT_PROCEDURE {
                /*
                 * We disallow OUT-after-VARIADIC only for procedures.  While
                 * such a case causes no confusion in ordinary function calls,
                 * it would cause confusion in a CALL statement.
                 */
                if var_count > 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg("VARIADIC parameter must be the last parameter")
                        .errposition(seam::parser_errposition::call(fp.location))
                        .into_error());
                }
                /* Procedures with output parameters always return RECORD */
                out.required_result_type = RECORDOID;
            } else if out_count == 0 {
                /* save first output param's type */
                out.required_result_type = toid;
            }
            out_count += 1;
        }

        if fpmode == FUNC_PARAM_VARIADIC {
            out.variadic_arg_type = toid;
            var_count += 1;
            /* validate variadic parameter type */
            match toid {
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID | ANYOID => { /* okay */ }
                _ => {
                    if !OidIsValid(seam::get_element_type::call(toid)?) {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                            .errmsg("VARIADIC parameter must be an array")
                            .errposition(seam::parser_errposition::call(fp.location))
                            .into_error());
                    }
                }
            }
        }

        all_types[i] = toid;
        param_modes[i] = fpmode;

        let fp_name_str = match &fp.name {
            Some(s) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };

        if let Some(ref name) = fp_name_str {
            /*
             * As of Postgres 9.0 we disallow using the same name for two input
             * or two output function parameters.
             */
            for &prevfp in &cells {
                if core::ptr::eq(prevfp, fp) {
                    break;
                }
                /* as above, default mode is IN */
                let mut prevfpmode = prevfp.mode;
                if prevfpmode == FUNC_PARAM_DEFAULT {
                    prevfpmode = FUNC_PARAM_IN;
                }
                /* pure in doesn't conflict with pure out */
                if (fpmode == FUNC_PARAM_IN || fpmode == FUNC_PARAM_VARIADIC)
                    && (prevfpmode == FUNC_PARAM_OUT || prevfpmode == FUNC_PARAM_TABLE)
                {
                    continue;
                }
                if (prevfpmode == FUNC_PARAM_IN || prevfpmode == FUNC_PARAM_VARIADIC)
                    && (fpmode == FUNC_PARAM_OUT || fpmode == FUNC_PARAM_TABLE)
                {
                    continue;
                }
                if let Some(ref prev_name) = prevfp.name {
                    if !prev_name.is_empty() && prev_name == name {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                            .errmsg(format!("parameter name \"{name}\" used more than once"))
                            .errposition(seam::parser_errposition::call(fp.location))
                            .into_error());
                    }
                }
            }

            param_names[i] = Some(name.clone());
            have_names = true;
        }

        if want_in_parameter_names_list {
            let s = fp_name_str.clone().unwrap_or_default();
            out.in_parameter_names_list.push(s);
        }

        if let Some(defexpr) = &fp.defexpr {
            if !isinput {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("only input parameters can have default values")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            }

            let def = seam::transform_parameter_default::call((**defexpr).clone(), toid)?;

            /*
             * Make sure no variables are referred to (this is probably dead
             * code now that add_missing_from is history).
             */
            if seam::default_has_table_refs::call(def.clone())? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                    .errmsg("cannot use table references in parameter default value")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            }

            out.parameter_defaults.push(def);
            have_defaults = true;
        } else {
            if isinput && have_defaults {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(
                        "input parameters after one with a default value must also have defaults",
                    )
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            }

            /*
             * For procedures, we also can't allow OUT parameters after one with
             * a default, because the same sort of confusion arises in a CALL
             * statement.
             */
            if objtype == OBJECT_PROCEDURE && have_defaults {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("procedure OUT parameters cannot appear after one with a default value")
                    .errposition(seam::parser_errposition::call(fp.location))
                    .into_error());
            }
        }
    }

    /* Now construct the proper outputs as needed */
    out.parameter_types = in_types;

    if out_count > 0 || var_count > 0 {
        out.all_parameter_types = Some(all_types);
        out.parameter_modes = Some(param_modes);
        if out_count > 1 {
            out.required_result_type = RECORDOID;
        }
        /* otherwise we set requiredResultType correctly above */
    } else {
        out.all_parameter_types = None;
        out.parameter_modes = None;
    }

    if have_names {
        out.parameter_names = Some(param_names);
    } else {
        out.parameter_names = None;
    }

    Ok(out)
}

// ===========================================================================
// compute_common_attribute (functioncmds.c:514)
// ===========================================================================

#[derive(Clone, Debug, Default)]
struct CommonAttributes {
    volatility_item: Option<DefElem>,
    strict_item: Option<DefElem>,
    security_item: Option<DefElem>,
    leakproof_item: Option<DefElem>,
    set_items: Vec<Node>,
    cost_item: Option<DefElem>,
    rows_item: Option<DefElem>,
    support_item: Option<DefElem>,
    parallel_item: Option<DefElem>,
}

fn compute_common_attribute(
    is_procedure: bool,
    defel: &DefElem,
    attrs: &mut CommonAttributes,
) -> PgResult<bool> {
    let defname = def_name(defel);

    if defname == "volatility" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.volatility_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.volatility_item = Some(defel.clone());
    } else if defname == "strict" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.strict_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.strict_item = Some(defel.clone());
    } else if defname == "security" {
        if attrs.security_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.security_item = Some(defel.clone());
    } else if defname == "leakproof" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.leakproof_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.leakproof_item = Some(defel.clone());
    } else if defname == "set" {
        if let Some(arg) = &defel.arg {
            attrs.set_items.push((**arg).clone());
        }
    } else if defname == "cost" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.cost_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.cost_item = Some(defel.clone());
    } else if defname == "rows" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.rows_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.rows_item = Some(defel.clone());
    } else if defname == "support" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.support_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.support_item = Some(defel.clone());
    } else if defname == "parallel" {
        if is_procedure {
            return Err(procedure_error(defel));
        }
        if attrs.parallel_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.parallel_item = Some(defel.clone());
    } else {
        return Ok(false);
    }

    /* Recognized an option */
    Ok(true)
}

/// `goto procedure_error;` target in compute_common_attribute.
fn procedure_error(defel: &DefElem) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
        .errmsg("invalid attribute in procedure definition")
        .errposition(seam::parser_errposition::call(defel.location))
        .into_error()
}

/// `errorConflictingDefElem(defel, pstate)` (commands/define.c).
fn error_conflicting_def_elem(defel: &DefElem) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(seam::parser_errposition::call(defel.location))
        .into_error()
}

// ===========================================================================
// interpret_func_volatility / interpret_func_parallel (functioncmds.c:616/634)
// ===========================================================================

fn interpret_func_volatility(defel: &DefElem) -> PgResult<i8> {
    let str = def_arg_str_val(defel)?;
    if str == "immutable" {
        Ok(PROVOLATILE_IMMUTABLE)
    } else if str == "stable" {
        Ok(PROVOLATILE_STABLE)
    } else if str == "volatile" {
        Ok(PROVOLATILE_VOLATILE)
    } else {
        Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal(format!("invalid volatility \"{str}\""))
            .into_error())
    }
}

fn interpret_func_parallel(defel: &DefElem) -> PgResult<i8> {
    let str = def_arg_str_val(defel)?;
    if str == "safe" {
        Ok(PROPARALLEL_SAFE)
    } else if str == "unsafe" {
        Ok(PROPARALLEL_UNSAFE)
    } else if str == "restricted" {
        Ok(PROPARALLEL_RESTRICTED)
    } else {
        Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE")
            .into_error())
    }
}

/// `strVal(defel->arg)`.
fn def_arg_str_val(defel: &DefElem) -> PgResult<String> {
    match &defel.arg {
        Some(node) => str_val(node),
        None => Err(ereport(ERROR)
            .errmsg_internal("DefElem has no arg")
            .into_error()),
    }
}

/// `boolVal(defel->arg)`.
fn def_arg_bool_val(defel: &DefElem) -> PgResult<bool> {
    match &defel.arg {
        Some(node) => bool_val(node),
        None => Err(ereport(ERROR)
            .errmsg_internal("DefElem has no arg")
            .into_error()),
    }
}

// ===========================================================================
// update_proconfig_value (functioncmds.c:659)
// ===========================================================================

fn update_proconfig_value(
    a: Option<Vec<String>>,
    set_items: Vec<Node>,
) -> PgResult<Option<Vec<String>>> {
    seam::update_proconfig_value::call(a, set_items)
}

// ===========================================================================
// compute_function_attributes (functioncmds.c:728)
// ===========================================================================

#[derive(Clone, Debug)]
struct FunctionAttributes {
    as_clause: Vec<Node>,
    as_clause_set: bool,
    language: Option<String>,
    transform: Vec<Node>,
    window_func: bool,
    volatility: i8,
    is_strict: bool,
    security_definer: bool,
    is_leakproof: bool,
    proconfig: Option<Vec<String>>,
    procost: f32,
    prorows: f32,
    prosupport: Oid,
    parallel: i8,
}

fn compute_function_attributes(
    is_procedure: bool,
    options: &[Node],
    attrs: &mut FunctionAttributes,
) -> PgResult<()> {
    let mut as_item: Option<DefElem> = None;
    let mut language_item: Option<DefElem> = None;
    let mut transform_item: Option<DefElem> = None;
    let mut windowfunc_item: Option<DefElem> = None;
    let mut common = CommonAttributes::default();

    for option in options {
        let defel = match option.as_defelem() {
            Some(d) => d,
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("compute_function_attributes: option is not a DefElem")
                    .into_error());
            }
        };
        let defname = def_name(defel);

        if defname == "as" {
            if as_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            as_item = Some(defel.clone());
        } else if defname == "language" {
            if language_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            language_item = Some(defel.clone());
        } else if defname == "transform" {
            if transform_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            transform_item = Some(defel.clone());
        } else if defname == "window" {
            if windowfunc_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            if is_procedure {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("invalid attribute in procedure definition")
                    .errposition(seam::parser_errposition::call(defel.location))
                    .into_error());
            }
            windowfunc_item = Some(defel.clone());
        } else if compute_common_attribute(is_procedure, defel, &mut common)? {
            /* recognized common option */
            continue;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .into_error());
        }
    }

    if let Some(as_item) = &as_item {
        attrs.as_clause = seam::def_get_as_clause::call(as_item.clone())?;
        attrs.as_clause_set = true;
    }
    if let Some(language_item) = &language_item {
        attrs.language = Some(def_arg_str_val(language_item)?);
    }
    if let Some(transform_item) = &transform_item {
        attrs.transform = seam::def_get_transform_type_names::call(transform_item.clone())?;
    }
    if let Some(windowfunc_item) = &windowfunc_item {
        attrs.window_func = def_arg_bool_val(windowfunc_item)?;
    }
    if let Some(item) = &common.volatility_item {
        attrs.volatility = interpret_func_volatility(item)?;
    }
    if let Some(item) = &common.strict_item {
        attrs.is_strict = def_arg_bool_val(item)?;
    }
    if let Some(item) = &common.security_item {
        attrs.security_definer = def_arg_bool_val(item)?;
    }
    if let Some(item) = &common.leakproof_item {
        attrs.is_leakproof = def_arg_bool_val(item)?;
    }
    if !common.set_items.is_empty() {
        attrs.proconfig = update_proconfig_value(None, common.set_items.clone())?;
    }
    if let Some(item) = &common.cost_item {
        attrs.procost = seam::def_get_numeric::call(item.clone())? as f32;
        if attrs.procost <= 0.0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("COST must be positive")
                .into_error());
        }
    }
    if let Some(item) = &common.rows_item {
        attrs.prorows = seam::def_get_numeric::call(item.clone())? as f32;
        if attrs.prorows <= 0.0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("ROWS must be positive")
                .into_error());
        }
    }
    if let Some(item) = &common.support_item {
        attrs.prosupport = seam::interpret_func_support::call(item.clone())?;
    }
    if let Some(item) = &common.parallel_item {
        attrs.parallel = interpret_func_parallel(item)?;
    }

    Ok(())
}

// ===========================================================================
// interpret_AS_clause (functioncmds.c:865)
// ===========================================================================

fn interpret_AS_clause(
    language_oid: Oid,
    language_name: &str,
    funcname: &str,
    as_clause: &[Node],
    as_clause_set: bool,
    sql_body_in: Option<&Node>,
    parameter_types: Vec<Oid>,
    in_parameter_names: Vec<String>,
    query_string: Option<String>,
) -> PgResult<(String, Option<String>, Option<Box<Node>>)> {
    if sql_body_in.is_none() && !as_clause_set {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("no function body specified")
            .into_error());
    }

    if sql_body_in.is_some() && as_clause_set {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("duplicate function body specified")
            .into_error());
    }

    if sql_body_in.is_some() && language_oid != SQL_LANGUAGE_ID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("inline SQL function body only valid for language SQL")
            .into_error());
    }

    let mut sql_body_out: Option<Box<Node>> = None;
    let prosrc_str: String;
    let probin_str: Option<String>;

    if language_oid == C_LANGUAGE_ID {
        /*
         * For "C" language, store the file name in probin and, when given, the
         * link symbol name in prosrc.  If link symbol is omitted, substitute
         * procedure name.
         */
        probin_str = Some(str_val(&as_clause[0])?);
        if as_clause.len() == 1 {
            prosrc_str = funcname.to_string();
        } else {
            let mut s = str_val(&as_clause[1])?;
            if s == "-" {
                s = funcname.to_string();
            }
            prosrc_str = s;
        }
    } else if let Some(sql_body_in) = sql_body_in {
        sql_body_out = Some(Box::new(seam::interpret_sql_body::call(
            funcname.to_string(),
            sql_body_in.clone(),
            parameter_types,
            in_parameter_names,
            query_string,
        )?));
        /*
         * We must put something in prosrc.  For the moment, just record an
         * empty string.
         */
        prosrc_str = String::new();
        /* But we definitely don't need probin. */
        probin_str = None;
    } else {
        /* Everything else wants the given string in prosrc. */
        let mut src = str_val(&as_clause[0])?;
        probin_str = None;

        if as_clause.len() != 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!(
                    "only one AS item needed for language \"{language_name}\""
                ))
                .into_error());
        }

        if language_oid == INTERNAL_LANGUAGE_ID {
            /*
             * accept an empty "prosrc" value as meaning the supplied SQL
             * function name.
             */
            if src.is_empty() {
                src = funcname.to_string();
            }
        }
        prosrc_str = src;
    }

    Ok((prosrc_str, probin_str, sql_body_out))
}

// ===========================================================================
// CreateFunction (functioncmds.c:1025)
// ===========================================================================

/// `CreateFunction(pstate, stmt)` (functioncmds.c:1025).
///
/// `query_string` is `pstate->p_sourcetext`, for the inline SQL-body interpreter.
pub fn CreateFunction(
    mcx: Mcx<'_>,
    stmt: &CreateFunctionStmt,
    query_string: Option<String>,
) -> PgResult<ObjectAddress> {
    /* Convert list of names to a name and namespace */
    let name_cells = string_nodes_to_namelist(&stmt.funcname);
    let (namespace_id, funcname) = QualifiedNameGetCreationNamespace(mcx, &name_cells)?;
    let funcname = funcname.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult =
        seam::namespace_aclcheck::call(namespace_id, seam::get_user_id::call()?, ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_schema::call(aclresult, seam::get_namespace_name::call(namespace_id)?)?;
    }

    /* Set default attributes */
    let mut attrs = FunctionAttributes {
        as_clause: Vec::new(),
        as_clause_set: false,
        language: None,
        transform: Vec::new(),
        window_func: false,
        is_strict: false,
        security_definer: false,
        is_leakproof: false,
        volatility: PROVOLATILE_VOLATILE,
        proconfig: None,
        procost: -1.0, /* indicates not set */
        prorows: -1.0, /* indicates not set */
        prosupport: InvalidOid,
        parallel: PROPARALLEL_UNSAFE,
    };

    let is_procedure = stmt.is_procedure;

    /* Extract non-default attributes from stmt->options list */
    compute_function_attributes(is_procedure, &stmt.options, &mut attrs)?;

    let language: String = match &attrs.language {
        Some(l) => l.clone(),
        None => {
            if stmt.sql_body.is_some() {
                "sql".to_string()
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("no language specified")
                    .into_error());
            }
        }
    };

    /* Look up the language and validate permissions */
    let language_struct = match seam::lookup_language_by_name::call(language.clone())? {
        Some(ls) => ls,
        None => {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("language \"{language}\" does not exist"));
            if seam::extension_file_exists::call(language.clone())? {
                b = b.errhint("Use CREATE EXTENSION to load the language into the database.");
            }
            return Err(b.into_error());
        }
    };

    let language_oid = language_struct.oid;

    check_language_permissions(&language_struct)?;

    let language_validator = language_struct.lanvalidator;

    /* Only superuser is allowed to create leakproof functions. */
    if attrs.is_leakproof && !seam::superuser::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("only superuser can define a leakproof function")
            .into_error());
    }

    let mut trftypes_list: Vec<Oid> = Vec::new();
    let mut trfoids_list: Vec<Oid> = Vec::new();
    if !attrs.transform.is_empty() {
        for lc in &attrs.transform {
            let tn = as_type_name(lc)?;
            let mut typeid = seam::typename_type_id::call(tn)?;
            let elt = seam::get_base_element_type::call(typeid)?;
            typeid = if OidIsValid(elt) { elt } else { typeid };
            let transformid = get_transform_oid(mcx, typeid, language_oid, false)?;
            trftypes_list.push(typeid);
            trfoids_list.push(transformid);
        }
    }

    /* Convert remaining parameters of CREATE to form wanted by ProcedureCreate. */
    let params = interpret_function_parameter_list(
        &stmt.parameters,
        language_oid,
        if is_procedure {
            OBJECT_PROCEDURE
        } else {
            OBJECT_FUNCTION
        },
        true,
        true,
    )?;

    let prorettype: Oid;
    let returns_set: bool;

    if is_procedure {
        debug_assert!(stmt.returnType.is_none());
        prorettype = if OidIsValid(params.required_result_type) {
            params.required_result_type
        } else {
            VOIDOID
        };
        returns_set = false;
    } else if let Some(return_type) = &stmt.returnType {
        /* explicit RETURNS clause */
        let rt = as_type_name(return_type)?;
        let (rettype, rset) = compute_return_type(mcx, &rt, language_oid)?;
        prorettype = rettype;
        returns_set = rset;
        if OidIsValid(params.required_result_type) && prorettype != params.required_result_type {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!(
                    "function result type must be {} because of OUT parameters",
                    seam::format_type_be::call(params.required_result_type)?
                ))
                .into_error());
        }
    } else if OidIsValid(params.required_result_type) {
        /* default RETURNS clause from OUT parameters */
        prorettype = params.required_result_type;
        returns_set = false;
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("function result type must be specified")
            .into_error());
    }

    let trftypes: Option<Vec<Oid>> = if !trftypes_list.is_empty() {
        Some(trftypes_list)
    } else {
        /* store SQL NULL instead of empty array */
        None
    };

    let (prosrc_str, probin_str, prosqlbody) = interpret_AS_clause(
        language_oid,
        &language,
        &funcname,
        &attrs.as_clause,
        attrs.as_clause_set,
        stmt.sql_body.as_deref(),
        params.parameter_types_list.clone(),
        params.in_parameter_names_list.clone(),
        query_string,
    )?;

    /*
     * Set default values for COST and ROWS depending on other parameters;
     * reject ROWS if it's not returnsSet.
     */
    let mut procost = attrs.procost;
    if procost < 0.0 {
        /* SQL and PL-language functions are assumed more expensive */
        if language_oid == INTERNAL_LANGUAGE_ID || language_oid == C_LANGUAGE_ID {
            procost = 1.0;
        } else {
            procost = 100.0;
        }
    }
    let mut prorows = attrs.prorows;
    if prorows < 0.0 {
        if returns_set {
            prorows = 1000.0;
        } else {
            prorows = 0.0; /* dummy value if not returnsSet */
        }
    } else if !returns_set {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("ROWS is not applicable when function does not return a set")
            .into_error());
    }

    let prokind = if is_procedure {
        PROKIND_PROCEDURE
    } else if attrs.window_func {
        PROKIND_WINDOW
    } else {
        PROKIND_FUNCTION
    };

    /* And now create the function. */
    seam::procedure_create::call(ProcedureCreateArgs {
        procedure_name: funcname,
        namespace_id,
        replace: stmt.replace,
        returns_set,
        prorettype,
        proowner: seam::get_user_id::call()?,
        language_oid,
        language_validator,
        prosrc: prosrc_str,
        probin: probin_str,
        prosqlbody,
        prokind,
        security: attrs.security_definer,
        is_leak_proof: attrs.is_leakproof,
        is_strict: attrs.is_strict,
        volatility: attrs.volatility,
        parallel: attrs.parallel,
        parameter_types: params.parameter_types,
        all_parameter_types: params.all_parameter_types,
        parameter_modes: params.parameter_modes,
        parameter_names: params.parameter_names,
        parameter_defaults: params.parameter_defaults,
        trftypes,
        trfoids: trfoids_list,
        proconfig: attrs.proconfig,
        prosupport: attrs.prosupport,
        procost,
        prorows,
    })
}

/// The trusted/untrusted language permission check shared by CreateFunction and
/// ExecuteDoStmt.
fn check_language_permissions(language_struct: &LanguageForm) -> PgResult<()> {
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

// ===========================================================================
// RemoveFunctionById (functioncmds.c:1310)
// ===========================================================================

pub fn RemoveFunctionById(func_oid: Oid) -> PgResult<()> {
    seam::remove_function_tuple::call(func_oid)
}

// ===========================================================================
// AlterFunction (functioncmds.c:1360)
// ===========================================================================

pub fn AlterFunction(stmt: &AlterFunctionStmt) -> PgResult<ObjectAddress> {
    let func = match &stmt.func {
        Some(f) => (**f).clone(),
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("AlterFunction: stmt->func is NULL")
                .into_error());
        }
    };

    /*
     * table_open(pg_proc), LookupFuncWithArgs, fetch tuple copy, owner check,
     * reject aggregate.  This preamble is raw catalog tuple I/O; the
     * decisions below are in-crate.
     */
    let target = seam::alter_function_begin::call(stmt.objtype, func)?;
    let func_oid = target.func_oid;

    let address = ObjectAddress {
        classId: ProcedureRelationId,
        objectId: func_oid,
        objectSubId: 0,
    };

    let is_procedure = target.prokind == PROKIND_PROCEDURE;

    /* Examine requested actions. */
    let mut common = CommonAttributes::default();
    for l in &stmt.actions {
        let defel = match l.as_defelem() {
            Some(d) => d,
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("AlterFunction: action is not a DefElem")
                    .into_error());
            }
        };
        if !compute_common_attribute(is_procedure, defel, &mut common)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("option \"{}\" not recognized", def_name(defel)))
                .into_error());
        }
    }

    let mut changes = AlterFunctionChanges {
        func_oid,
        ..Default::default()
    };

    if let Some(item) = &common.volatility_item {
        changes.provolatile = Some(interpret_func_volatility(item)?);
    }
    if let Some(item) = &common.strict_item {
        changes.proisstrict = Some(def_arg_bool_val(item)?);
    }
    if let Some(item) = &common.security_item {
        changes.prosecdef = Some(def_arg_bool_val(item)?);
    }
    if let Some(item) = &common.leakproof_item {
        let leakproof = def_arg_bool_val(item)?;
        changes.proleakproof = Some(leakproof);
        if leakproof && !seam::superuser::call()? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("only superuser can define a leakproof function")
                .into_error());
        }
    }
    if let Some(item) = &common.cost_item {
        let cost = seam::def_get_numeric::call(item.clone())? as f32;
        changes.procost = Some(cost as f64);
        if cost <= 0.0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("COST must be positive")
                .into_error());
        }
    }
    if let Some(item) = &common.rows_item {
        let rows = seam::def_get_numeric::call(item.clone())? as f32;
        changes.prorows = Some(rows as f64);
        if rows <= 0.0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("ROWS must be positive")
                .into_error());
        }
        if !target.proretset {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("ROWS is not applicable when function does not return a set")
                .into_error());
        }
    }
    if let Some(item) = &common.support_item {
        /* interpret_func_support handles the privilege check */
        let newsupport = seam::interpret_func_support::call(item.clone())?;

        /* Add or replace dependency on support function */
        if OidIsValid(target.prosupport) {
            if seam::change_support_dependency::call(func_oid, target.prosupport, newsupport)? != 1 {
                let name = seam::get_func_name::call(func_oid)?.unwrap_or_default();
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg_internal(format!(
                        "could not change support dependency for function {name}"
                    ))
                    .into_error());
            }
        } else {
            seam::record_support_dependency::call(func_oid, newsupport)?;
        }

        changes.prosupport = Some(newsupport);
    }
    if let Some(item) = &common.parallel_item {
        changes.proparallel = Some(interpret_func_parallel(item)?);
    }
    if !common.set_items.is_empty() {
        changes.set_items = Some(common.set_items);
    }

    /* Do the update (heap_modify_tuple + CatalogTupleUpdate + post-alter hook) */
    seam::alter_function_apply::call(changes)?;

    Ok(address)
}

// ===========================================================================
// CreateCast (functioncmds.c:1538)
// ===========================================================================

pub fn CreateCast(stmt: &CreateCastStmt) -> PgResult<ObjectAddress> {
    let sourcetype = match &stmt.sourcetype {
        Some(b) => as_type_name(b)?,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateCast: stmt->sourcetype is NULL")
                .into_error());
        }
    };
    let targettype = match &stmt.targettype {
        Some(b) => as_type_name(b)?,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateCast: stmt->targettype is NULL")
                .into_error());
        }
    };

    let sourcetypeid = seam::typename_type_id::call(sourcetype.clone())?;
    let targettypeid = seam::typename_type_id::call(targettype.clone())?;
    let sourcetyptype = seam::get_typtype::call(sourcetypeid)?;
    let targettyptype = seam::get_typtype::call(targettypeid)?;

    /* No pseudo-types allowed */
    if sourcetyptype == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "source data type {} is a pseudo-type",
                seam::type_name_to_string::call(sourcetype.clone())?
            ))
            .into_error());
    }

    if targettyptype == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "target data type {} is a pseudo-type",
                seam::type_name_to_string::call(targettype.clone())?
            ))
            .into_error());
    }

    /* Permission check */
    if !seam::type_ownercheck::call(sourcetypeid, seam::get_user_id::call()?)?
        && !seam::type_ownercheck::call(targettypeid, seam::get_user_id::call()?)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "must be owner of type {} or type {}",
                seam::format_type_be::call(sourcetypeid)?,
                seam::format_type_be::call(targettypeid)?
            ))
            .into_error());
    }

    let aclresult = seam::type_aclcheck::call(sourcetypeid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, sourcetypeid)?;
    }

    let aclresult = seam::type_aclcheck::call(targettypeid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, targettypeid)?;
    }

    /* Domains are allowed for historical reasons, but we warn */
    if sourcetyptype == TYPTYPE_DOMAIN {
        ereport(WARNING)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cast will be ignored because the source data type is a domain")
            .finish(errloc(1592, "CreateCast"))?;
    } else if targettyptype == TYPTYPE_DOMAIN {
        ereport(WARNING)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cast will be ignored because the target data type is a domain")
            .finish(errloc(1597, "CreateCast"))?;
    }

    /* Determine the cast method */
    let castmethod: i8 = if stmt.func.is_some() {
        COERCION_METHOD_FUNCTION
    } else if stmt.inout {
        COERCION_METHOD_INOUT
    } else {
        COERCION_METHOD_BINARY
    };

    let funcid: Oid;
    let nargs: i32;
    let mut incastid = InvalidOid;
    let mut outcastid = InvalidOid;

    if castmethod == COERCION_METHOD_FUNCTION {
        let func = match &stmt.func {
            Some(f) => (**f).clone(),
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal("CreateCast: COERCION_METHOD_FUNCTION but func is NULL")
                    .into_error());
            }
        };
        funcid = seam::lookup_func_with_args::call(OBJECT_FUNCTION, func, false)?;

        let procstruct: CastFuncForm = match seam::fetch_cast_func_form::call(funcid)? {
            Some(p) => p,
            None => {
                return Err(cache_lookup_failed_function(funcid));
            }
        };

        nargs = procstruct.pronargs as i32;
        if nargs < 1 || nargs > 3 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cast function must take one to three arguments")
                .into_error());
        }
        let (coercible, castid) =
            seam::is_binary_coercible_with_cast::call(sourcetypeid, procstruct.proargtypes[0])?;
        incastid = castid;
        if !coercible {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("argument of cast function must match or be binary-coercible from source data type")
                .into_error());
        }
        if nargs > 1 && procstruct.proargtypes[1] != INT4OID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "second argument of cast function must be type {}",
                    "integer"
                ))
                .into_error());
        }
        if nargs > 2 && procstruct.proargtypes[2] != BOOLOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "third argument of cast function must be type {}",
                    "boolean"
                ))
                .into_error());
        }
        let (coercible, castid) =
            seam::is_binary_coercible_with_cast::call(procstruct.prorettype, targettypeid)?;
        outcastid = castid;
        if !coercible {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("return data type of cast function must match or be binary-coercible to target data type")
                .into_error());
        }

        /* NOT_USED volatility check intentionally omitted, as in C */

        if procstruct.prokind != PROKIND_FUNCTION {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cast function must be a normal function")
                .into_error());
        }
        if procstruct.proretset {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cast function must not return a set")
                .into_error());
        }
    } else {
        funcid = InvalidOid;
        nargs = 0;
    }

    if castmethod == COERCION_METHOD_BINARY {
        /* Must be superuser to create binary-compatible casts. */
        if !seam::superuser::call()? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("must be superuser to create a cast WITHOUT FUNCTION")
                .into_error());
        }

        /*
         * Insist that the types match as to size, alignment, and pass-by-value
         * attributes.
         */
        let (typ1len, typ1byval, typ1align) = seam::get_typlenbyvalalign::call(sourcetypeid)?;
        let (typ2len, typ2byval, typ2align) = seam::get_typlenbyvalalign::call(targettypeid)?;
        if typ1len != typ2len || typ1byval != typ2byval || typ1align != typ2align {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("source and target data types are not physically compatible")
                .into_error());
        }

        /* Composite, array, range and enum types are never binary-compatible. */
        if sourcetyptype == TYPTYPE_COMPOSITE || targettyptype == TYPTYPE_COMPOSITE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("composite data types are not binary-compatible")
                .into_error());
        }

        if OidIsValid(seam::get_element_type::call(sourcetypeid)?)
            || OidIsValid(seam::get_element_type::call(targettypeid)?)
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("array data types are not binary-compatible")
                .into_error());
        }

        if sourcetyptype == TYPTYPE_RANGE
            || targettyptype == TYPTYPE_RANGE
            || sourcetyptype == TYPTYPE_MULTIRANGE
            || targettyptype == TYPTYPE_MULTIRANGE
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("range data types are not binary-compatible")
                .into_error());
        }

        if sourcetyptype == TYPTYPE_ENUM || targettyptype == TYPTYPE_ENUM {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("enum data types are not binary-compatible")
                .into_error());
        }

        /* Disallow binary-compatibility casts involving domains. */
        if sourcetyptype == TYPTYPE_DOMAIN || targettyptype == TYPTYPE_DOMAIN {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("domain data types must not be marked binary-compatible")
                .into_error());
        }
    }

    /*
     * Allow source and target types to be same only for length coercion
     * functions.  We assume a multi-arg function does length coercion.
     */
    if sourcetypeid == targettypeid && nargs < 2 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("source data type and target data type are the same")
            .into_error());
    }

    /* convert CoercionContext enum to char value for castcontext */
    let castcontext: i8 = match stmt.context {
        CoercionContext::COERCION_IMPLICIT => COERCION_CODE_IMPLICIT,
        CoercionContext::COERCION_ASSIGNMENT => COERCION_CODE_ASSIGNMENT,
        /* COERCION_PLPGSQL is intentionally not covered here */
        CoercionContext::COERCION_EXPLICIT => COERCION_CODE_EXPLICIT,
        other => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("unrecognized CoercionContext: {}", other as i32))
                .into_error());
        }
    };

    seam::cast_create::call(
        sourcetypeid,
        targettypeid,
        funcid,
        incastid,
        outcastid,
        castcontext,
        castmethod,
    )
}

// ===========================================================================
// check_transform_function (functioncmds.c:1801)
// ===========================================================================

fn check_transform_function(procstruct: &TransformFuncForm) -> PgResult<()> {
    if procstruct.provolatile == PROVOLATILE_VOLATILE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must not be volatile")
            .into_error());
    }
    if procstruct.prokind != PROKIND_FUNCTION {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must be a normal function")
            .into_error());
    }
    if procstruct.proretset {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must not return a set")
            .into_error());
    }
    if procstruct.pronargs != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must take one argument")
            .into_error());
    }
    if procstruct.proargtype0 != INTERNALOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "first argument of transform function must be type {}",
                "internal"
            ))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// CreateTransform (functioncmds.c:1831)
// ===========================================================================

pub fn CreateTransform(stmt: &CreateTransformStmt) -> PgResult<ObjectAddress> {
    /* Get the type */
    let type_name = match &stmt.type_name {
        Some(b) => as_type_name(b)?,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateTransform: stmt->type_name is NULL")
                .into_error());
        }
    };
    let typeid = seam::typename_type_id::call(type_name.clone())?;
    let typtype = seam::get_typtype::call(typeid)?;

    if typtype == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "data type {} is a pseudo-type",
                seam::type_name_to_string::call(type_name.clone())?
            ))
            .into_error());
    }

    if typtype == TYPTYPE_DOMAIN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "data type {} is a domain",
                seam::type_name_to_string::call(type_name.clone())?
            ))
            .into_error());
    }

    if !seam::type_ownercheck::call(typeid, seam::get_user_id::call()?)? {
        seam::aclcheck_error_type::call(ACLCHECK_NOT_OWNER, typeid)?;
    }

    let aclresult = seam::type_aclcheck::call(typeid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, typeid)?;
    }

    /* Get the language */
    let lang = stmt.lang.clone().unwrap_or_default();
    let langid = seam::get_language_oid::call(lang.clone(), false)?;

    let aclresult = seam::language_aclcheck::call(langid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_language::call(aclresult, lang.clone())?;
    }

    /* Get the functions */
    let fromsqlfuncid = check_transform_func(&stmt.fromsql, typeid, true)?;
    let tosqlfuncid = check_transform_func(&stmt.tosql, typeid, false)?;

    /*
     * Ready to go — the pg_transform insert/update, dependency rebuild,
     * extension dependency, and post-create hook are raw catalog tuple I/O.
     */
    seam::create_transform_tuple::call(
        typeid,
        langid,
        fromsqlfuncid,
        tosqlfuncid,
        stmt.replace,
        lang,
    )
}

/// The per-direction function lookup + permission + signature check of
/// `CreateTransform` (functioncmds.c:1880-1953). `is_from` selects the FROM-SQL
/// return-type rule (`internal`) vs the TO-SQL rule (the transform data type).
fn check_transform_func(func: &Option<Box<Node>>, typeid: Oid, is_from: bool) -> PgResult<Oid> {
    let func = match func {
        Some(f) => (**f).clone(),
        None => return Ok(InvalidOid),
    };

    /* `func` is an `ObjectWithArgs`; objname is its qualified name list. */
    let objname: Vec<String> = match func.as_objectwithargs() {
        Some(owa) => owa.objname.clone(),
        _ => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateTransform: fromsql/tosql is not an ObjectWithArgs")
                .into_error());
        }
    };

    let funcid = seam::lookup_func_with_args::call(OBJECT_FUNCTION, func, false)?;

    if !seam::proc_ownercheck::call(funcid, seam::get_user_id::call()?)? {
        seam::aclcheck_error_function::call(ACLCHECK_NOT_OWNER, name_list_to_string(&objname))?;
    }

    let aclresult = seam::proc_aclcheck::call(funcid, seam::get_user_id::call()?, ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_function::call(aclresult, name_list_to_string(&objname))?;
    }

    let procstruct = match seam::fetch_transform_func_form::call(funcid)? {
        Some(p) => p,
        None => return Err(cache_lookup_failed_function(funcid)),
    };

    if is_from {
        if procstruct.prorettype != INTERNALOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "return data type of FROM SQL function must be {}",
                    "internal"
                ))
                .into_error());
        }
    } else if procstruct.prorettype != typeid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("return data type of TO SQL function must be the transform data type")
            .into_error());
    }
    check_transform_function(&procstruct)?;

    Ok(funcid)
}

/// `NameListToString(names)` — render a qualified name list for messages.
fn name_list_to_string(names: &[String]) -> String {
    names.join(".")
}

// ===========================================================================
// get_transform_oid (functioncmds.c:2036)
// ===========================================================================

/// `Anum_pg_transform_oid` = 1 (`catalog/pg_transform_d.h`).
const Anum_pg_transform_oid: AttrNumber = 1;

/// `get_transform_oid(type_id, lang_id, missing_ok)` (functioncmds.c:2036).
///
/// The lookup core is `GetSysCacheOid2(TRFTYPELANG, Anum_pg_transform_oid,
/// ObjectIdGetDatum(type_id), ObjectIdGetDatum(lang_id))` over the live
/// syscache; only the error-message helpers cross seams.
pub fn get_transform_oid(
    mcx: Mcx<'_>,
    type_id: Oid,
    lang_id: Oid,
    missing_ok: bool,
) -> PgResult<Oid> {
    let oid = backend_utils_cache_syscache::GetSysCacheOid(
        mcx,
        backend_utils_cache_syscache::TRFTYPELANG,
        Anum_pg_transform_oid,
        SysCacheKey::Value(Datum::from_oid(type_id)),
        SysCacheKey::Value(Datum::from_oid(lang_id)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "transform for type {} language \"{}\" does not exist",
                seam::format_type_be::call(type_id)?,
                seam::get_language_name::call(lang_id)?
            ))
            .into_error());
    }
    Ok(oid)
}

// ===========================================================================
// IsThereFunctionInNamespace (functioncmds.c:2060)
// ===========================================================================

pub fn IsThereFunctionInNamespace(
    proname: &str,
    pronargs: i32,
    proargtypes: &[Oid],
    nsp_oid: Oid,
) -> PgResult<()> {
    /* check for duplicate name (more friendly than unique-index failure) */
    if seam::function_exists_in_namespace::call(proname.to_string(), proargtypes.to_vec(), nsp_oid)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_FUNCTION)
            .errmsg(format!(
                "function {} already exists in schema \"{}\"",
                seam::funcname_signature_string::call(
                    proname.to_string(),
                    pronargs,
                    proargtypes.to_vec()
                )?,
                seam::get_namespace_name::call(nsp_oid)?.unwrap_or_default()
            ))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// ExecuteDoStmt (functioncmds.c:2083)
// ===========================================================================

pub fn ExecuteDoStmt(stmt: &DoStmt, atomic: bool) -> PgResult<()> {
    let mut codeblock = InlineCodeBlock {
        source_text: None,
        langOid: InvalidOid,
        langIsTrusted: false,
        atomic: false,
    };
    let mut as_item: Option<DefElem> = None;
    let mut language_item: Option<DefElem> = None;

    /* Process options we got from gram.y */
    for arg in &stmt.args {
        let defel = match arg.as_defelem() {
            Some(d) => d,
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("ExecuteDoStmt: arg is not a DefElem")
                    .into_error());
            }
        };
        let defname = def_name(defel);

        if defname == "as" {
            if as_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            as_item = Some(defel.clone());
        } else if defname == "language" {
            if language_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            language_item = Some(defel.clone());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .into_error());
        }
    }

    if let Some(as_item) = &as_item {
        codeblock.source_text = Some(def_arg_str_val(as_item)?);
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("no inline code specified")
            .into_error());
    }

    /* if LANGUAGE option wasn't specified, use the default */
    let language = if let Some(language_item) = &language_item {
        def_arg_str_val(language_item)?
    } else {
        "plpgsql".to_string()
    };

    /* Look up the language and validate permissions */
    let language_struct = match seam::lookup_language_by_name::call(language.clone())? {
        Some(ls) => ls,
        None => {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("language \"{language}\" does not exist"));
            if seam::extension_file_exists::call(language.clone())? {
                b = b.errhint("Use CREATE EXTENSION to load the language into the database.");
            }
            return Err(b.into_error());
        }
    };

    codeblock.langOid = language_struct.oid;
    codeblock.langIsTrusted = language_struct.lanpltrusted;
    codeblock.atomic = atomic;

    check_language_permissions(&language_struct)?;

    /* get the handler function's OID */
    let laninline = language_struct.laninline;
    if !OidIsValid(laninline) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "language \"{}\" does not support inline code execution",
                language_struct.lanname
            ))
            .into_error());
    }

    /* execute the inline handler */
    seam::execute_inline_handler::call(laninline, codeblock)
}

// ===========================================================================
// ExecuteCallStmt (functioncmds.c:2205)
// ===========================================================================

pub fn ExecuteCallStmt(stmt: &CallStmt, atomic: bool) -> PgResult<()> {
    seam::execute_call_stmt::call(stmt.clone(), atomic)
}

// ===========================================================================
// CallStmtResultDesc (functioncmds.c:2382)
// ===========================================================================

pub fn CallStmtResultDesc<'mcx>(mcx: Mcx<'mcx>, stmt: &CallStmt) -> PgResult<TupleDesc<'mcx>> {
    seam::call_stmt_result_desc::call(mcx, stmt.clone())
}

// ===========================================================================
// shared error helpers
// ===========================================================================

/// `errmsg("cache lookup failed for function %u", funcid)`.
fn cache_lookup_failed_function(funcid: Oid) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(format!("cache lookup failed for function {funcid}"))
        .into_error()
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every seam this crate owns. functioncmds owns no inward seam (no
/// other crate calls back into it across a cycle yet), so this installs nothing.
pub fn init_seams() {}

#[allow(unused_imports)]
use backend_commands_functioncmds_seams as _functioncmds_seams_dep;
#[allow(unused_imports)]
use types_nodes as _types_nodes_dep;
#[allow(unused_imports)]
use seam_core as _seam_core_dep;
