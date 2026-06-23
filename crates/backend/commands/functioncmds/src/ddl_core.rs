//! DDL-core family of `backend/commands/functioncmds.c`.
//!
//! `CreateFunction` / `AlterFunction` / `RemoveFunctionById` and their static
//! helpers (`compute_return_type`, `interpret_function_parameter_list`,
//! `compute_common_attribute`, `interpret_func_volatility`,
//! `interpret_func_parallel`, `update_proconfig_value`,
//! `compute_function_attributes`, `interpret_AS_clause`).

use crate::cast_transform_do::get_transform_oid;
use crate::keystone::{
    as_type_name, check_language_permissions, def_arg_bool_val, def_arg_str_val, def_name,
    error_conflicting_def_elem, errloc, procedure_error, str_val, string_nodes_to_namelist,
    C_LANGUAGE_ID, INTERNAL_LANGUAGE_ID, OBJECT_AGGREGATE, OBJECT_FUNCTION, OBJECT_PROCEDURE,
    SQL_LANGUAGE_ID,
};
use catalog_namespace::QualifiedNameGetCreationNamespace;
use functioncmds_seams::{
    self as seam, AlterFunctionChanges, ProcedureCreateArgs,
};
use lsyscache_seams as lsc;
use guc_seams as guc_seam;
use utils_error::ereport;
use mcx::Mcx;
use types_acl::{ACLCHECK_OK, ACL_CREATE, ACL_USAGE};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use parsenodes::{
    AlterFunctionStmt, CreateFunctionStmt, DefElem, FunctionParameter, Node, ProcedureRelationId,
    StringNode, TypeName, FUNC_PARAM_DEFAULT, FUNC_PARAM_IN, FUNC_PARAM_OUT, FUNC_PARAM_TABLE,
    FUNC_PARAM_VARIADIC, PROKIND_FUNCTION, PROKIND_PROCEDURE, PROKIND_WINDOW, PROPARALLEL_RESTRICTED,
    PROPARALLEL_SAFE, PROPARALLEL_UNSAFE, PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE,
    PROVOLATILE_VOLATILE, VariableSetKind,
};
use types_tuple::{ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYOID, INTERNALOID, RECORDOID, VOIDOID};

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
        aclchk_seams::aclcheck_error_type::call(aclresult, rettype)?;
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
    /// The cooked `parameterDefaults` `List`, already serialized to its
    /// `pg_node_tree` text plus its object references — mirrors the prosqlbody
    /// path (`interpret_sql_body` serializes up front, `ProcedureCreate` just
    /// stores the text). `text: None` when there are no defaults.
    pub parameter_defaults: functioncmds_seams::CookedParameterDefaults,
    pub variadic_arg_type: Oid,
    pub required_result_type: Oid,
}

/// `interpret_function_parameter_list(pstate, parameters, languageOid, objtype,
/// ...)` (functioncmds.c:182). `objtype` is the `ObjectType` int.
///
/// `rich_parameters` is the rich (raw-parse) `FunctionParameter` list parallel to
/// the flat `parameters`; its `defexpr` carries the raw DEFAULT expression (an
/// arbitrary `A_Const`/`TypeCast`/`FuncCall`/... node the flat
/// `parsenodes` vocabulary cannot hold), which is transformed and cooked
/// here. `mcx` owns the cooked default nodes until they are serialized.
pub fn interpret_function_parameter_list<'mcx>(
    mcx: Mcx<'mcx>,
    parameters: &[Node],
    rich_parameters: &[&nodes::nodes::Node<'mcx>],
    language_oid: Oid,
    objtype: i32,
    want_parameter_types_list: bool,
    want_in_parameter_names_list: bool,
    source_text: Option<&str>,
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

    /* Cooked DEFAULT-expression nodes accumulate here (one per defaulted input
     * parameter), in mcx, until they are serialized into the `proargdefaults`
     * `List` after the scan. */
    let mut parameter_defaults: mcx::PgVec<'mcx, nodes::nodes::NodePtr<'mcx>> =
        mcx::PgVec::new_in(mcx);

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
                        .errposition(seam::parser_errposition::call(source_text.map(str::to_string), t.location))
                        .into_error());
                } else if objtype == OBJECT_AGGREGATE {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg(format!(
                            "aggregate cannot accept shell type {}",
                            seam::type_name_to_string::call(t.clone())?
                        ))
                        .errposition(seam::parser_errposition::call(source_text.map(str::to_string), t.location))
                        .into_error());
                } else {
                    ereport(NOTICE)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!(
                            "argument type {} is only a shell",
                            seam::type_name_to_string::call(t.clone())?
                        ))
                        .errposition(seam::parser_errposition::call(source_text.map(str::to_string), t.location))
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
                .errposition(seam::parser_errposition::call(source_text.map(str::to_string), t.location))
                .into_error());
        }

        let aclresult = seam::type_aclcheck::call(toid, seam::get_user_id::call()?, ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            aclchk_seams::aclcheck_error_type::call(aclresult, toid)?;
        }

        if t.setof {
            if objtype == OBJECT_AGGREGATE {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("aggregates cannot accept set arguments")
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
                    .into_error());
            } else if objtype == OBJECT_PROCEDURE {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("procedures cannot accept set arguments")
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("functions cannot accept set arguments")
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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
                        .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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
                    if lsc::get_element_type::call(toid)?.is_none() {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                            .errmsg("VARIADIC parameter must be an array")
                            .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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
                            .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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

        /* The raw DEFAULT expression lives on the *rich* parameter node — the flat
         * `fp.defexpr` only signals presence (the flat vocabulary can't hold an
         * arbitrary expression). */
        let rich_defexpr: Option<&nodes::nodes::Node<'mcx>> = rich_parameters
            .get(i)
            .and_then(|rp| rp.as_functionparameter())
            .and_then(|rfp| rfp.defexpr.as_deref());

        if let Some(defexpr) = rich_defexpr {
            if !isinput {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("only input parameters can have default values")
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
                    .into_error());
            }

            /*
             * transformExpr + coerce_to_specific_type(..., "DEFAULT") +
             * assign_expr_collations, then the `pstate->p_rtable != NIL ||
             * contain_var_clause(def)` no-table-references check — all inside the
             * parser-owned seam (functioncmds.c:419-433). The cooked node is
             * allocated in `mcx`.
             */
            let def = seam::transform_parameter_default::call(
                mcx,
                defexpr,
                toid,
                fp.location,
                source_text.map(str::to_string),
            )?;

            parameter_defaults
                .try_reserve(1)
                .map_err(|_| mcx.oom(1))?;
            parameter_defaults.push(mcx::alloc_in(mcx, def)?);
            have_defaults = true;
        } else {
            if isinput && have_defaults {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(
                        "input parameters after one with a default value must also have defaults",
                    )
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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
                    .errposition(seam::parser_errposition::call(source_text.map(str::to_string), fp.location))
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

    /* Serialize the cooked `parameterDefaults` `List` to its `pg_node_tree` text
     * (`nodeToString`) and collect its object references, up front in the
     * parser-owned seam (mirrors the prosqlbody path). `ProcedureCreate` then
     * stores the text and records the references without owning the cooked-node
     * serializer. */
    let default_refs: Vec<&nodes::nodes::Node<'mcx>> =
        parameter_defaults.iter().map(|b| &**b).collect();
    out.parameter_defaults = seam::cook_parameter_defaults::call(mcx, default_refs)?;

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
    query_string: Option<&str>,
) -> PgResult<bool> {
    let defname = def_name(defel);

    if defname == "volatility" {
        if is_procedure {
            return Err(procedure_error(defel, query_string));
        }
        if attrs.volatility_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.volatility_item = Some(defel.clone());
    } else if defname == "strict" {
        if is_procedure {
            return Err(procedure_error(defel, query_string));
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
            return Err(procedure_error(defel, query_string));
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
            return Err(procedure_error(defel, query_string));
        }
        if attrs.cost_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.cost_item = Some(defel.clone());
    } else if defname == "rows" {
        if is_procedure {
            return Err(procedure_error(defel, query_string));
        }
        if attrs.rows_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.rows_item = Some(defel.clone());
    } else if defname == "support" {
        if is_procedure {
            return Err(procedure_error(defel, query_string));
        }
        if attrs.support_item.is_some() {
            return Err(error_conflicting_def_elem(defel));
        }
        attrs.support_item = Some(defel.clone());
    } else if defname == "parallel" {
        if is_procedure {
            return Err(procedure_error(defel, query_string));
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

// ===========================================================================
// update_proconfig_value (functioncmds.c:659)
// ===========================================================================

fn update_proconfig_value(
    mut a: Option<Vec<String>>,
    set_items: Vec<Node>,
) -> PgResult<Option<Vec<String>>> {
    for l in set_items {
        let Node::VariableSetStmt(sstmt) = l else {
            // lfirst_node(VariableSetStmt, l) — the parser only ever puts a
            // VariableSetStmt in the proconfig "set" item list.
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg("set_items element is not a VariableSetStmt")
                .into_error());
        };

        if sstmt.kind == VariableSetKind::ResetAll {
            a = None;
        } else {
            let name = sstmt.name.clone().unwrap_or_default();
            let valuestr =
                guc_funcs_seams::extract_set_variable_args::call(sstmt)?;

            if let Some(valuestr) = valuestr {
                a = Some(guc_seam::guc_array_add::call(a, name, valuestr)?);
            } else {
                // RESET
                a = guc_seam::guc_array_delete::call(a, name)?;
            }
        }
    }

    Ok(a)
}

// ===========================================================================
// interpret_func_support (functioncmds.c:684)
// ===========================================================================

fn interpret_func_support(defel: DefElem) -> PgResult<Oid> {
    let proc_name = seam::def_get_qualified_name::call(defel)?;

    // Support functions always take one INTERNAL argument and return INTERNAL.
    let arg_list = vec![INTERNALOID];

    let proc_oid = seam::lookup_func_name::call(proc_name.clone(), 1, arg_list.clone(), true)?;
    if !OidIsValid(proc_oid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                seam::func_signature_string::call(proc_name, 1, arg_list)?
            ))
            .into_error());
    }

    if lsc::get_func_rettype::call(proc_oid)? != INTERNALOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "support function {} must return type {}",
                seam::name_list_to_string::call(proc_name)?,
                "internal"
            ))
            .into_error());
    }

    // Someday we might want an ACL check here; but for now, we insist that you
    // be superuser to specify a support function, so privilege on the support
    // function is moot.
    if !seam::superuser::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to specify a support function")
            .into_error());
    }

    Ok(proc_oid)
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
    query_string: Option<&str>,
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
                    .errposition(seam::parser_errposition::call(
                        query_string.map(|s| s.to_string()),
                        defel.location,
                    ))
                    .into_error());
            }
            windowfunc_item = Some(defel.clone());
        } else if compute_common_attribute(is_procedure, defel, &mut common, query_string)? {
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
        attrs.prosupport = interpret_func_support(item.clone())?;
    }
    if let Some(item) = &common.parallel_item {
        attrs.parallel = interpret_func_parallel(item)?;
    }

    Ok(())
}
// ===========================================================================
// interpret_AS_clause (functioncmds.c:865)
// ===========================================================================

fn interpret_AS_clause<'a>(
    mcx: Mcx<'a>,
    language_oid: Oid,
    language_name: &str,
    funcname: &str,
    as_clause: &[Node],
    as_clause_set: bool,
    sql_body_in: Option<&nodes::nodes::Node<'a>>,
    parameter_types: Vec<Oid>,
    in_parameter_names: Vec<String>,
    query_string: Option<String>,
) -> PgResult<(String, Option<String>, Option<String>, Vec<ObjectAddress>)> {
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

    let mut sql_body_out: Option<String> = None;
    let mut sql_body_refs: Vec<ObjectAddress> = Vec::new();
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
        let interpreted = seam::interpret_sql_body::call(
            mcx,
            funcname.to_string(),
            sql_body_in,
            parameter_types,
            in_parameter_names,
            query_string,
        )?;
        sql_body_out = Some(interpreted.text);
        sql_body_refs = interpreted.body_refs;
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

    Ok((prosrc_str, probin_str, sql_body_out, sql_body_refs))
}
// ===========================================================================
// CreateFunction (functioncmds.c:1025)
// ===========================================================================

/// `CreateFunction(pstate, stmt)` (functioncmds.c:1025).
///
/// `query_string` is `pstate->p_sourcetext`, for the inline SQL-body interpreter.
pub fn CreateFunction<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateFunctionStmt,
    rich_parameters: &[&nodes::nodes::Node<'mcx>],
    sql_body_rich: Option<&nodes::nodes::Node<'mcx>>,
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
    compute_function_attributes(is_procedure, &stmt.options, &mut attrs, query_string.as_deref())?;

    let language: String = match &attrs.language {
        Some(l) => l.clone(),
        None => {
            if sql_body_rich.is_some() {
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
            let elt = lsc::get_base_element_type::call(typeid)?;
            typeid = if OidIsValid(elt) { elt } else { typeid };
            let transformid = get_transform_oid(mcx, typeid, language_oid, false)?;
            trftypes_list.push(typeid);
            trfoids_list.push(transformid);
        }
    }

    /* Convert remaining parameters of CREATE to form wanted by ProcedureCreate. */
    let params = interpret_function_parameter_list(
        mcx,
        &stmt.parameters,
        rich_parameters,
        language_oid,
        if is_procedure {
            OBJECT_PROCEDURE
        } else {
            OBJECT_FUNCTION
        },
        true,
        true,
        query_string.as_deref(),
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
                    format_type_seams::format_type_be_str::call(
                        params.required_result_type,
                    )?
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

    let (prosrc_str, probin_str, prosqlbody, prosqlbody_refs) = interpret_AS_clause(
        mcx,
        language_oid,
        &language,
        &funcname,
        &attrs.as_clause,
        attrs.as_clause_set,
        sql_body_rich,
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
        prosqlbody_refs,
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
// ===========================================================================
// RemoveFunctionById (functioncmds.c:1310)
// ===========================================================================

pub fn RemoveFunctionById(func_oid: Oid) -> PgResult<()> {
    seam::remove_function_tuple::call(func_oid)
}

// ===========================================================================
// AlterFunction (functioncmds.c:1360)
// ===========================================================================

pub fn AlterFunction(stmt: &AlterFunctionStmt, query_string: Option<&str>) -> PgResult<ObjectAddress> {
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
        if !compute_common_attribute(is_procedure, defel, &mut common, query_string)? {
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
        let newsupport = interpret_func_support(item.clone())?;

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
        /* extract existing proconfig setting, then update according to each SET
         * or RESET item, left to right (functioncmds.c:1495-1501). */
        let a = update_proconfig_value(target.proconfig.clone(), common.set_items)?;
        changes.proconfig = Some(a);
    }

    /* Do the update (heap_modify_tuple + CatalogTupleUpdate + post-alter hook) */
    seam::alter_function_apply::call(changes)?;

    Ok(address)
}
