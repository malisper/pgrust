#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use catalog_namespace::{FuncCandidate, OperCandidate};
use coerce::{COERCION_EXPLICIT, COERCION_IMPLICIT, COERCION_PATH_COERCEVIAIO,
    COERCION_PATH_RELABELTYPE, TYPCATEGORY_INVALID, TYPCATEGORY_STRING};
use elog::ereport;
use mcx::{Mcx, PgVec};
use parser_small1::{parser_errposition, ParseState};
use types_core::catalog::{RECORDOID, UNKNOWNOID, VOIDOID};
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_FUNCTION,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::primnodes::{Aggref, WindowFunc, AGGKIND_HYPOTHETICAL, AGGKIND_ORDERED_SET};
use types_nodes::parsenodes::ObjectType;
use types_nodes::rawnodes::FuncCall;
use types_nodes::{CoercionForm, FuncExpr, Node, NodeList, NodeTag};

const FUNC_MAX_ARGS: usize = 100;

pub fn init_seams() {
    parse_func_seams::LookupFuncWithArgs::set(lookup_func_with_args_seam);
    parse_func_seams::LookupFuncName::set(lookup_func_name_seam);
    clauses_seams::recheck_cast_function_args::set(recheck_cast_function_args);
    clauses_seams::make_fn_arguments_nullstate::set(|mcx, args, actual, declared| {
        let pstate = parser_small1::make_parsestate(mcx, None);
        let args = args.clone_in(mcx)?;
        make_fn_arguments(mcx, &pstate, args, actual, declared)
    });
}

const PROKIND_AGGREGATE: i8 = b'a' as i8;
const PROKIND_FUNCTION: i8 = b'f' as i8;
const PROKIND_PROCEDURE: i8 = b'p' as i8;
const PROKIND_WINDOW: i8 = b'w' as i8;

enum FuncDetail<'mcx> {
    Normal {
        funcid: Oid,
        rettype: Oid,
        retset: bool,
        declared_arg_types: PgVec<'mcx, Oid>,
        vatype: Oid,
        nvargs: i16,
        argdefaults: PgVec<'mcx, Node<'mcx>>,
    },
    Aggregate {
        funcid: Oid,
        rettype: Oid,
        retset: bool,
        declared_arg_types: PgVec<'mcx, Oid>,
        vatype: Oid,
        nvargs: i16,
    },
    Coercion { rettype: Oid },
    Multiple,
    Procedure {
        funcid: Oid,
        rettype: Oid,
        retset: bool,
        declared_arg_types: PgVec<'mcx, Oid>,
        vatype: Oid,
        nvargs: i16,
        argdefaults: PgVec<'mcx, Node<'mcx>>,
    },
    WindowFunc {
        funcid: Oid,
        rettype: Oid,
        retset: bool,
        declared_arg_types: PgVec<'mcx, Oid>,
        argdefaults: PgVec<'mcx, Node<'mcx>>,
    },
    NotFound,
}

pub trait CandidateArgs {
    fn cand_args(&self) -> &[Oid];
}

impl CandidateArgs for FuncCandidate<'_> {
    fn cand_args(&self) -> &[Oid] {
        self.args.as_slice()
    }
}

impl CandidateArgs for OperCandidate {
    fn cand_args(&self) -> &[Oid] {
        &self.args
    }
}

/// C `ParseFuncOrColumn`; is_column mirrors C's fn == NULL attribute-notation
/// leg (complex-projection column syntax itself lands with
/// ParseComplexProjection). Divergence: actual arg types precomputed by the
/// caller (parse_oper::make_op precedent).
#[allow(clippy::too_many_arguments)]
pub fn ParseFuncOrColumn<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    funcname: &NodeList<'mcx>,
    fargs: NodeList<'mcx>,
    actual_arg_types: &[Oid],
    fn_call: &FuncCall<'mcx>,
    // fn->agg_filter, already through transformWhereClause(EXPR_KIND_FILTER)
    // by the caller (dependency direction; C transforms it here first).
    agg_filter: Option<Node<'mcx>>,
    _last_srf: Option<Node<'mcx>>,
    proc_call: bool,
    is_column: bool,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    debug_assert_eq!(fargs.len(), actual_arg_types.len());
    let mut buf = [""; 4];
    let parts = name_parts(funcname, &mut buf);

    let over = fn_call.over;

    if fargs.len() > FUNC_MAX_ARGS {
        return Err(too_many_arguments(pstate, location));
    }

    // VOID-typed Param markers are discarded before lookup (the JDBC-driver
    // accommodation for {call ...} return placeholders) — but never for
    // column syntax or WITHIN GROUP, where the argument count is load-bearing.
    let mut kept_types: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let (fargs, actual_arg_types) = if !is_column
        && !fn_call.agg_within_group
        && fargs
            .iter()
            .zip(actual_arg_types)
            .any(|(a, &t)| t == VOIDOID && a.node_tag() == NodeTag::T_Param)
    {
        let mut kept = NodeList::nil();
        for (arg, &t) in fargs.iter().zip(actual_arg_types) {
            if t == VOIDOID && arg.node_tag() == NodeTag::T_Param {
                continue;
            }
            kept.lappend(mcx, arg)?;
            kept_types.push(t);
        }
        (kept, kept_types.as_slice())
    } else {
        (fargs, actual_arg_types)
    };

    let mut argnames: PgVec<'mcx, &'mcx str> = PgVec::new_in(mcx);
    for arg in &fargs {
        match arg.as_named_arg_expr() {
            Some(na) => {
                let name = na.name.expect("NamedArgExpr has a name");
                if argnames.iter().any(|&n| n == name) {
                    return Err(named_arg_error(
                        pstate,
                        format!("argument name \"{name}\" used more than once"),
                        na.location,
                    ));
                }
                argnames.push(name);
            }
            None => {
                if !argnames.is_empty() {
                    return Err(named_arg_error(
                        pstate,
                        "positional argument cannot follow named argument".to_string(),
                        expr_location(arg),
                    ));
                }
            }
        }
    }

    let fdresult = func_get_detail(
        mcx,
        parts,
        &fargs,
        argnames.as_slice(),
        fargs.len() as i16,
        actual_arg_types,
        !fn_call.func_variadic,
        proc_call,
    )?;

    if proc_call
        && matches!(
            fdresult,
            FuncDetail::Normal { .. }
                | FuncDetail::Aggregate { .. }
                | FuncDetail::WindowFunc { .. }
                | FuncDetail::Coercion { .. }
        )
    {
        return Err(wrong_object_type_hint(
            pstate,
            format!(
                "{} is not a procedure",
                func_signature_string(parts, argnames.as_slice(), actual_arg_types)?
            ),
            "To call a function, use SELECT.",
            location,
        ));
    }

    match fdresult {
        FuncDetail::Coercion { rettype } => coerce::coerce_type(
            mcx,
            pstate,
            fargs.nth(0),
            actual_arg_types[0],
            rettype,
            -1,
            COERCION_EXPLICIT,
            CoercionForm::COERCE_EXPLICIT_CALL,
            location,
        ),
        FuncDetail::Multiple => {
            Err(ambiguous_function(pstate, parts, argnames.as_slice(), actual_arg_types, proc_call, location))
        }
        FuncDetail::Procedure { .. } if !proc_call => Err(wrong_object_type_hint(
            pstate,
            format!(
                "{} is a procedure",
                func_signature_string(parts, argnames.as_slice(), actual_arg_types)?
            ),
            "To call a procedure, use CALL.",
            location,
        )),
        FuncDetail::Normal {
            funcid,
            rettype,
            retset,
            declared_arg_types,
            vatype,
            nvargs,
            argdefaults,
        }
        | FuncDetail::Procedure {
            funcid,
            rettype,
            retset,
            declared_arg_types,
            vatype,
            nvargs,
            argdefaults,
        } => {
            if fn_call.agg_star {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "{}(*) specified, but {} is not an aggregate function",
                        name_list_to_string(parts),
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            if fn_call.agg_distinct {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "DISTINCT specified, but {} is not an aggregate function",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            if fn_call.agg_within_group {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "WITHIN GROUP specified, but {} is not an aggregate function",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            if !fn_call.agg_order.is_nil() {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "ORDER BY specified, but {} is not an aggregate function",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            if agg_filter.is_some() {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "FILTER specified, but {} is not an aggregate function",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            if over.is_some() {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "OVER specified, but {} is not a window function nor an aggregate \
                         function",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }

            let mut declared_arg_types = declared_arg_types;
            // C: default types join the generic-consistency check, but the
            // defaults are NOT put into the parse node — their values may
            // change before execution; the planner inserts them at plan time.
            let mut all_arg_types: PgVec<'mcx, Oid> =
                mcx::vec_with_capacity_in(mcx, actual_arg_types.len() + argdefaults.len())?;
            for &t in actual_arg_types {
                all_arg_types.push(t);
            }
            for d in argdefaults.iter() {
                if all_arg_types.len() >= FUNC_MAX_ARGS {
                    return Err(too_many_arguments(pstate, location));
                }
                all_arg_types.push(nodes_core::expr_type(*d));
            }
            let actual_arg_types = all_arg_types.as_slice();
            let rettype = coerce::enforce_generic_type_consistency(
                actual_arg_types,
                declared_arg_types.as_mut_slice(),
                rettype,
                false,
            )?;
            let fargs =
                make_fn_arguments(mcx, pstate, fargs, actual_arg_types, &declared_arg_types)?;

            // C: forget VARIADIC decoration on a non-variadic function.
            let mut func_variadic = fn_call.func_variadic && OidIsValid(vatype);
            let fargs = if let Some((packed, _)) = pack_variadic_args(
                mcx,
                pstate,
                &fargs,
                actual_arg_types,
                &declared_arg_types,
                nvargs,
                vatype,
            )? {
                debug_assert!(!func_variadic);
                func_variadic = true;
                packed
            } else {
                fargs
            };
            if !fargs.is_nil() && vatype == types_core::catalog::ANYOID && func_variadic {
                let va_arr_typid = actual_arg_types[actual_arg_types.len() - 1];
                if !OidIsValid(lsyscache::get_base_element_type(va_arr_typid)?) {
                    return Err(variadic_not_array(pstate, &fargs));
                }
            }
            if retset {
                check_srf_call_placement(pstate, _last_srf, location)?;
            }

            let retval = Node::mk(
                mcx,
                FuncExpr {
                    funcid,
                    funcresulttype: rettype,
                    funcretset: retset,
                    funcvariadic: func_variadic,
                    funcformat: fn_call.funcformat,
                    funccollid: InvalidOid,
                    inputcollid: InvalidOid,
                    args: fargs,
                    location,
                },
            )?;
            if retset {
                pstate.p_last_srf = Some(retval);
            }
            Ok(retval)
        }
        FuncDetail::Aggregate { funcid, rettype, retset, declared_arg_types, vatype, nvargs } => {
            let aggshape = syscache_seams::lookup_pg_aggregate_shape::call(funcid)?
                .unwrap_or_else(|| {
                    panic!("cache lookup failed for aggregate {funcid} (parse_func.c)")
                });
            let aggkind = aggshape.aggkind;
            let cat_direct_args = aggshape.aggnumdirectargs as i64;
            let mut fargs = fargs;
            let mut actual_types_buf: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
            let mut actual_arg_types = actual_arg_types;
            if aggkind == AGGKIND_ORDERED_SET || aggkind == AGGKIND_HYPOTHETICAL {
                if !fn_call.agg_within_group {
                    return Err(wrong_object_type(
                        pstate,
                        format!(
                            "WITHIN GROUP is required for ordered-set aggregate {}",
                            name_list_to_string(parts)
                        ),
                        location,
                    ));
                }
                if over.is_some() {
                    return Err(feature_not_supported(
                        pstate,
                        format!(
                            "OVER is not supported for ordered-set aggregate {}",
                            name_list_to_string(parts)
                        ),
                        None,
                        location,
                    ));
                }
                // gram.y rejects DISTINCT/VARIADIC + WITHIN GROUP.
                debug_assert!(!fn_call.agg_distinct);
                debug_assert!(!fn_call.func_variadic);

                let nargs = fargs.len() as i64;
                let num_aggregated_args = fn_call.agg_order.len() as i64;
                let num_direct_args = nargs - num_aggregated_args;
                debug_assert!(num_direct_args >= 0);

                if !OidIsValid(vatype) {
                    if num_direct_args != cat_direct_args {
                        return Err(ordered_set_direct_args_error(
                            pstate,
                            parts,
                            argnames.as_slice(),
                            actual_arg_types,
                            cat_direct_args,
                            num_direct_args,
                            location,
                        ));
                    }
                } else {
                    let mut pronargs = nargs;
                    if nvargs > 1 {
                        pronargs -= nvargs as i64 - 1;
                    }
                    if cat_direct_args < pronargs {
                        if num_direct_args != cat_direct_args {
                            return Err(ordered_set_direct_args_error(
                                pstate,
                                parts,
                                argnames.as_slice(),
                                actual_arg_types,
                                cat_direct_args,
                                num_direct_args,
                                location,
                            ));
                        }
                    } else if aggkind == AGGKIND_HYPOTHETICAL {
                        if nvargs as i64 != 2 * num_aggregated_args {
                            return Err(hypothetical_args_mismatch_error(
                                pstate,
                                parts,
                                argnames.as_slice(),
                                actual_arg_types,
                                nvargs as i64 - num_aggregated_args,
                                num_aggregated_args,
                                location,
                            ));
                        }
                    } else if nvargs as i64 <= num_aggregated_args {
                        return Err(ordered_set_min_direct_args_error(
                            pstate,
                            parts,
                            argnames.as_slice(),
                            actual_arg_types,
                            cat_direct_args,
                            location,
                        ));
                    }
                }

                if aggkind == AGGKIND_HYPOTHETICAL {
                    for &t in actual_arg_types {
                        actual_types_buf.push(t);
                    }
                    fargs = unify_hypothetical_args(
                        mcx,
                        pstate,
                        fargs,
                        num_aggregated_args as usize,
                        actual_types_buf.as_mut_slice(),
                        declared_arg_types.as_slice(),
                    )?;
                    actual_arg_types = actual_types_buf.as_slice();
                }
            } else if fn_call.agg_within_group {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "{} is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }

            let mut declared_arg_types = declared_arg_types;
            let rettype = coerce::enforce_generic_type_consistency(
                actual_arg_types,
                declared_arg_types.as_mut_slice(),
                rettype,
                false,
            )?;
            let fargs =
                make_fn_arguments(mcx, pstate, fargs, actual_arg_types, &declared_arg_types)?;
            // C: the variadic ArrayExpr packing is shared across function
            // kinds — a variadic aggregate call (vatype != "any") packs too.
            let mut func_variadic = fn_call.func_variadic && OidIsValid(vatype);
            let mut packed_array_type = InvalidOid;
            let fargs = if let Some((packed, array_typeid)) = pack_variadic_args(
                mcx,
                pstate,
                &fargs,
                actual_arg_types,
                &declared_arg_types,
                nvargs,
                vatype,
            )? {
                debug_assert!(!func_variadic);
                func_variadic = true;
                packed_array_type = array_typeid;
                packed
            } else {
                fargs
            };
            if let Some(over_node) = over {
                return build_window_func(
                    mcx, pstate, funcid, rettype, retset, fargs, true, fn_call, agg_filter,
                    parts, over_node, _last_srf, location,
                );
            }
            // C's aggargtypes = exprType per coerced arg (parse_agg.c);
            // parse_expr::expr_type is dependency-forbidden here, so replay
            // coerce_type's head-arm result-type contract. A packed variadic
            // call has one trailing ArrayExpr arg in place of the nvargs
            // actuals.
            let n_plain = if OidIsValid(packed_array_type) {
                actual_arg_types.len() - nvargs as usize
            } else {
                actual_arg_types.len()
            };
            let mut agg_arg_types = mcx::vec_with_capacity_in(mcx, n_plain + 1)?;
            for (&a, &d) in
                actual_arg_types.iter().zip(declared_arg_types.iter()).take(n_plain)
            {
                agg_arg_types.push(post_coercion_type(a, d)?);
            }
            if OidIsValid(packed_array_type) {
                agg_arg_types.push(packed_array_type);
            }

            if fargs.is_nil() && !fn_call.agg_star && !fn_call.agg_within_group {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "{}(*) must be used to call a parameterless aggregate function",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            if retset {
                let encoding = mbutils::GetDatabaseEncoding();
                return Err(Box::new(
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg("aggregates cannot return sets")
                        .errposition(parser_errposition(pstate, location, encoding))
                        .into_error()
                        .with_error_location(ErrorLocation::new(
                            "parse_func.c",
                            0,
                            "ParseFuncOrColumn",
                        )),
                ));
            }

            let mut aggref = Node::build::<Aggref>(mcx)?;
            aggref.aggfnoid = funcid;
            aggref.aggtype = rettype;
            aggref.aggkind = aggkind;
            aggref.aggfilter = agg_filter;
            aggref.aggstar = fn_call.agg_star;
            aggref.aggvariadic = func_variadic;
            aggref.location = location;

            parse_agg::transformAggregateCall(
                mcx,
                pstate,
                &mut aggref,
                &fargs,
                &agg_arg_types,
                &fn_call.agg_order,
                fn_call.agg_distinct,
            )?;

            Ok(aggref.seal())
        }
        FuncDetail::WindowFunc { funcid, rettype, retset, declared_arg_types, argdefaults } => {
            let Some(over_node) = over else {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "window function {} requires an OVER clause",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            };
            if fn_call.agg_within_group {
                return Err(wrong_object_type(
                    pstate,
                    format!(
                        "window function {} cannot have WITHIN GROUP",
                        name_list_to_string(parts)
                    ),
                    location,
                ));
            }
            let mut declared_arg_types = declared_arg_types;
            // C: default types join the generic-consistency check; the
            // defaults themselves stay out of the parse node (the planner
            // inserts them at plan time).
            let mut all_arg_types: PgVec<'mcx, Oid> =
                mcx::vec_with_capacity_in(mcx, actual_arg_types.len() + argdefaults.len())?;
            for &t in actual_arg_types {
                all_arg_types.push(t);
            }
            for d in argdefaults.iter() {
                if all_arg_types.len() >= FUNC_MAX_ARGS {
                    return Err(too_many_arguments(pstate, location));
                }
                all_arg_types.push(nodes_core::expr_type(*d));
            }
            let actual_arg_types = all_arg_types.as_slice();
            let rettype = coerce::enforce_generic_type_consistency(
                actual_arg_types,
                declared_arg_types.as_mut_slice(),
                rettype,
                false,
            )?;
            let fargs =
                make_fn_arguments(mcx, pstate, fargs, actual_arg_types, &declared_arg_types)?;
            build_window_func(
                mcx, pstate, funcid, rettype, retset, fargs, false, fn_call, agg_filter, parts,
                over_node, _last_srf, location,
            )
        }
        FuncDetail::NotFound => {
            // C parse_func.c:592-599 — could_be_projection (line 216-229):
            // a single-arg, undecorated, unqualified call on a composite
            // value is retried as `(arg).funcname` before erroring.
            if fargs.len() == 1
                && !proc_call
                && fn_call.agg_order.is_nil()
                && agg_filter.is_none()
                && !fn_call.agg_star
                && !fn_call.agg_distinct
                && over.is_none()
                && !fn_call.func_variadic
                && argnames.is_empty()
                && parts.len() == 1
                && (actual_arg_types[0] == RECORDOID || is_complex(actual_arg_types[0])?)
            {
                if let Some(node) =
                    ParseComplexProjection(mcx, pstate, parts[0], fargs.nth(0), location)?
                {
                    return Ok(node);
                }
            }
            Err(undefined_function(
                pstate,
                parts,
                argnames.as_slice(),
                actual_arg_types,
                fn_call.agg_order.len() > 1 && !fn_call.agg_within_group,
                proc_call,
                location,
            ))
        }
    }
}

// C ISCOMPLEX (parse_type.h): typeOrDomainTypeRelid(typeid) != InvalidOid,
// i.e. the type (or its base, for a domain) has a backing pg_class row.
fn is_complex(typid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(lsyscache::get_typ_typrelid(lsyscache::getBaseType(typid)?)?))
}

// C: ParseFuncOrColumn's shared variadic packing — the trailing nvargs
// coerced arguments become one ArrayExpr and the call turns VARIADIC; a
// VARIADIC "any" call stays unpacked. Applies to normal functions,
// aggregates, and window functions alike. Returns None when no packing
// applies; otherwise the new fargs plus the packed array type (the packed
// call's trailing argument type).
fn pack_variadic_args<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    fargs: &NodeList<'mcx>,
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
    nvargs: i16,
    vatype: Oid,
) -> PgResult<Option<(NodeList<'mcx>, Oid)>> {
    if nvargs == 0 || vatype == types_core::catalog::ANYOID {
        return Ok(None);
    }
    let non_var_args = fargs.len() - nvargs as usize;
    let mut plain: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(mcx, non_var_args + 1)?;
    let mut vargs: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(mcx, nvargs as usize)?;
    for (i, n) in fargs.iter().enumerate() {
        if i < non_var_args {
            plain.push(n);
        } else {
            vargs.push(n);
        }
    }
    let element_typeid =
        post_coercion_type(actual_arg_types[non_var_args], declared_arg_types[non_var_args])?;
    let array_typeid = lsyscache::get_array_type(element_typeid)?;
    let vargs = NodeList::from_slice(mcx, vargs.as_slice())?;
    if !OidIsValid(array_typeid) {
        let encoding = mbutils::GetDatabaseEncoding();
        let loc = vargs.first().map_or(-1, expr_location);
        return Err(Box::new(
            ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find array type for data type {}",
                    format_type::format_type_be(element_typeid)?
                ))
                .errposition(parser_errposition(pstate, loc, encoding))
                .into_error(),
        ));
    }
    let list_loc = {
        let mut loc = -1;
        for n in vargs.iter() {
            loc = if loc < 0 {
                expr_location(n)
            } else {
                let l = expr_location(n);
                if l < 0 { loc } else { loc.min(l) }
            };
        }
        loc
    };
    let newa = Node::mk(
        mcx,
        types_nodes::primnodes::ArrayExpr {
            elements: vargs,
            element_typeid,
            array_typeid,
            multidims: false,
            location: list_loc,
            ..Default::default()
        },
    )?;
    plain.push(newa);
    Ok(Some((NodeList::from_slice(mcx, plain.as_slice())?, array_typeid)))
}

// unify_hypothetical_args (parse_func.c): coerce each hypothetical direct
// argument to the type of its corresponding aggregated argument when the
// agg declared them ANY (make_fn_arguments will not touch ANY args).
fn unify_hypothetical_args<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    fargs: NodeList<'mcx>,
    num_aggregated_args: usize,
    actual_arg_types: &mut [Oid],
    declared_arg_types: &[Oid],
) -> PgResult<NodeList<'mcx>> {
    let num_args = fargs.len();
    let num_direct_args = num_args - num_aggregated_args;
    let Some(num_non_hypothetical_args) = num_direct_args.checked_sub(num_aggregated_args)
    else {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errmsg("incorrect number of arguments to hypothetical-set aggregate")
                .into_error()
                .with_error_location(ErrorLocation::new(
                    "parse_func.c",
                    0,
                    "unify_hypothetical_args",
                )),
        ));
    };
    let mut nodes: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(mcx, num_args)?;
    for n in fargs.iter() {
        nodes.push(n);
    }
    let mut changed = false;
    for hargpos in num_non_hypothetical_args..num_direct_args {
        let aargpos = num_direct_args + (hargpos - num_non_hypothetical_args);
        if declared_arg_types[hargpos] != declared_arg_types[aargpos] {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errmsg(
                        "hypothetical-set aggregate has inconsistent declared argument types",
                    )
                    .into_error()
                    .with_error_location(ErrorLocation::new(
                        "parse_func.c",
                        0,
                        "unify_hypothetical_args",
                    )),
            ));
        }
        if declared_arg_types[hargpos] != types_core::catalog::ANYOID {
            continue;
        }
        // C prefers the aggregated argument's type (coerce the one direct
        // value instead of every aggregated value).
        let (harg, aarg) = (nodes[hargpos], nodes[aargpos]);
        let commontype = coerce::select_common_type(
            pstate,
            &[
                (nodes_core::expr_type(aarg), nodes_core::expr_location(aarg)),
                (nodes_core::expr_type(harg), nodes_core::expr_location(harg)),
            ],
            Some("WITHIN GROUP"),
        )?;
        let commontypmod = coerce::select_common_typmod(
            &[
                (nodes_core::expr_type(aarg), nodes_core::expr_typmod(aarg)),
                (nodes_core::expr_type(harg), nodes_core::expr_typmod(harg)),
            ],
            commontype,
        );
        nodes[hargpos] = coerce::coerce_type(
            mcx,
            pstate,
            harg,
            actual_arg_types[hargpos],
            commontype,
            commontypmod,
            COERCION_IMPLICIT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        actual_arg_types[hargpos] = commontype;
        nodes[aargpos] = coerce::coerce_type(
            mcx,
            pstate,
            aarg,
            actual_arg_types[aargpos],
            commontype,
            commontypmod,
            COERCION_IMPLICIT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        actual_arg_types[aargpos] = commontype;
        changed = true;
    }
    if changed {
        NodeList::from_slice(mcx, nodes.as_slice())
    } else {
        Ok(fargs)
    }
}

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn ordered_set_direct_args_error(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    cat_direct_args: i64,
    num_direct_args: i64,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let hint = if cat_direct_args == 1 {
        format!(
            "There is an ordered-set aggregate {}, but it requires {} direct argument, not {}.",
            name_list_to_string(parts),
            cat_direct_args,
            num_direct_args
        )
    } else {
        format!(
            "There is an ordered-set aggregate {}, but it requires {} direct arguments, not {}.",
            name_list_to_string(parts),
            cat_direct_args,
            num_direct_args
        )
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function {sig} does not exist"))
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn hypothetical_args_mismatch_error(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    num_hypothetical: i64,
    num_ordering: i64,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function {sig} does not exist"))
            .errhint(format!(
                "To use the hypothetical-set aggregate {}, the number of hypothetical direct \
                 arguments (here {}) must match the number of ordering columns (here {}).",
                name_list_to_string(parts),
                num_hypothetical,
                num_ordering
            ))
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn ordered_set_min_direct_args_error(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    cat_direct_args: i64,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let hint = if cat_direct_args == 1 {
        format!(
            "There is an ordered-set aggregate {}, but it requires at least {} direct argument.",
            name_list_to_string(parts),
            cat_direct_args
        )
    } else {
        format!(
            "There is an ordered-set aggregate {}, but it requires at least {} direct arguments.",
            name_list_to_string(parts),
            cat_direct_args
        )
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function {sig} does not exist"))
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn named_arg_error(pstate: &ParseState<'_, '_>, msg: String, location: ParseLoc) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(types_error::ERRCODE_SYNTAX_ERROR)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

// C's window-function retval branch of ParseFuncOrColumn. DIVERGENCE: the
// nested-SRF errposition points at the window function call, not the inner
// SRF (exprLocation lives in parse_expr; dependency cycle — same divergence
// as check_srf_call_placement).
#[allow(clippy::too_many_arguments)]
fn build_window_func<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    funcid: Oid,
    rettype: Oid,
    retset: bool,
    fargs: NodeList<'mcx>,
    winagg: bool,
    fn_call: &FuncCall<'mcx>,
    agg_filter: Option<Node<'mcx>>,
    parts: &[&str],
    over_node: Node<'mcx>,
    last_srf: Option<Node<'mcx>>,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    if fn_call.agg_distinct {
        return Err(feature_not_supported(
            pstate,
            "DISTINCT is not implemented for window functions".into(),
            None,
            location,
        ));
    }
    if winagg && fargs.is_nil() && !fn_call.agg_star {
        return Err(wrong_object_type(
            pstate,
            format!(
                "{}(*) must be used to call a parameterless aggregate function",
                name_list_to_string(parts)
            ),
            location,
        ));
    }
    if !fn_call.agg_order.is_nil() {
        return Err(feature_not_supported(
            pstate,
            "aggregate ORDER BY is not implemented for window functions".into(),
            None,
            location,
        ));
    }
    if !winagg && agg_filter.is_some() {
        return Err(feature_not_supported(
            pstate,
            "FILTER is not implemented for non-aggregate window functions".into(),
            None,
            location,
        ));
    }
    let srf_added = match (pstate.p_last_srf, last_srf) {
        (None, None) => false,
        (Some(a), Some(b)) => !a.ptr_eq(b),
        _ => true,
    };
    if srf_added {
        // C errpositions at exprLocation(pstate->p_last_srf), the inner SRF.
        let srf_location = pstate.p_last_srf.map(nodes_core::expr_location).unwrap_or(-1);
        return Err(feature_not_supported(
            pstate,
            "window function calls cannot contain set-returning function calls".into(),
            Some(
                "You might be able to move the set-returning function into a LATERAL FROM item.",
            ),
            srf_location,
        ));
    }
    if retset {
        let encoding = mbutils::GetDatabaseEncoding();
        return Err(Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("window functions cannot return sets")
                .errposition(parser_errposition(pstate, location, encoding))
                .into_error()
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
        ));
    }

    let mut wfunc = Node::build::<WindowFunc>(mcx)?;
    wfunc.winfnoid = funcid;
    wfunc.wintype = rettype;
    wfunc.args = fargs;
    wfunc.winstar = fn_call.agg_star;
    wfunc.winagg = winagg;
    wfunc.aggfilter = agg_filter;
    wfunc.location = location;

    parse_agg::transformWindowFuncCall(mcx, pstate, &mut wfunc, over_node)?;

    Ok(wfunc.seal())
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(
    pstate: &ParseState<'_, '_>,
    msg: String,
    hint: Option<&'static str>,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    let mut b = ereport(ERROR)
        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(msg)
        .errposition(parser_errposition(pstate, location, encoding));
    if let Some(h) = hint {
        b = b.errhint(h);
    }
    Box::new(
        b.into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

/// C `func_match_argtypes`. C prepends survivors (reversing order); order is
/// outcome-neutral downstream, so this keeps input order.
pub fn func_match_argtypes<'c, 'mcx, C: CandidateArgs>(
    mcx: Mcx<'mcx>,
    input_typeids: &[Oid],
    raw_candidates: &'c [C],
) -> PgResult<PgVec<'mcx, &'c C>> {
    let mut out: PgVec<'mcx, &'c C> = mcx::vec_with_capacity_in(mcx, raw_candidates.len())?;
    for c in raw_candidates {
        if coerce::can_coerce_type(input_typeids, c.cand_args(), COERCION_IMPLICIT)? {
            out.push(c);
        }
    }
    Ok(out)
}

fn keep_best<'c, C: CandidateArgs>(
    candidates: &mut PgVec<'_, &'c C>,
    nmatch: impl Fn(&C) -> PgResult<usize>,
) -> PgResult<()> {
    let mut best = 0usize;
    for c in candidates.iter() {
        best = best.max(nmatch(c)?);
    }
    let mut i = 0;
    while i < candidates.len() {
        if nmatch(candidates[i])? == best {
            i += 1;
        } else {
            candidates.remove(i);
        }
    }
    Ok(())
}

/// C `func_select_candidate`.
pub fn func_select_candidate<'c, C: CandidateArgs>(
    input_typeids: &[Oid],
    mut candidates: PgVec<'_, &'c C>,
) -> PgResult<Option<&'c C>> {
    let nargs = input_typeids.len();
    if nargs > FUNC_MAX_ARGS {
        return Err(Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
                .errmsg(format!("cannot pass more than {FUNC_MAX_ARGS} arguments to a function"))
                .into_error()
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "func_select_candidate")),
        ));
    }

    let mut input_base_typeids = [InvalidOid; FUNC_MAX_ARGS];
    let mut nunknowns = 0usize;
    for i in 0..nargs {
        if input_typeids[i] != UNKNOWNOID {
            input_base_typeids[i] = lsyscache::getBaseType(input_typeids[i])?;
        } else {
            input_base_typeids[i] = UNKNOWNOID;
            nunknowns += 1;
        }
    }
    let base = &input_base_typeids[..nargs];

    keep_best(&mut candidates, |c| {
        let args = c.cand_args();
        Ok((0..nargs).filter(|&i| base[i] != UNKNOWNOID && args[i] == base[i]).count())
    })?;
    if candidates.len() == 1 {
        return Ok(Some(candidates[0]));
    }

    let mut slot_category = [TYPCATEGORY_INVALID; FUNC_MAX_ARGS];
    for i in 0..nargs {
        slot_category[i] = coerce::TypeCategory(base[i])?;
    }
    {
        let slot_category = &slot_category[..nargs];
        keep_best(&mut candidates, |c| {
            let args = c.cand_args();
            let mut nmatch = 0;
            for i in 0..nargs {
                if base[i] != UNKNOWNOID
                    && (args[i] == base[i] || coerce::IsPreferredType(slot_category[i], args[i])?)
                {
                    nmatch += 1;
                }
            }
            Ok(nmatch)
        })?;
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates[0]));
    }

    if nunknowns == 0 {
        return Ok(None);
    }

    let mut slot_has_preferred_type = [false; FUNC_MAX_ARGS];
    let mut resolved_unknowns = false;
    for i in 0..nargs {
        if base[i] != UNKNOWNOID {
            continue;
        }
        resolved_unknowns = true;
        slot_category[i] = TYPCATEGORY_INVALID;
        slot_has_preferred_type[i] = false;
        let mut have_conflict = false;
        for c in candidates.iter() {
            let (current_category, current_is_preferred) =
                lsyscache::get_type_category_preferred(c.cand_args()[i])?;
            if slot_category[i] == TYPCATEGORY_INVALID {
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = current_is_preferred;
            } else if current_category == slot_category[i] {
                slot_has_preferred_type[i] |= current_is_preferred;
            } else if current_category == TYPCATEGORY_STRING {
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = current_is_preferred;
            } else {
                have_conflict = true;
            }
        }
        if have_conflict && slot_category[i] != TYPCATEGORY_STRING {
            resolved_unknowns = false;
            break;
        }
    }

    if resolved_unknowns {
        let keepit = |c: &C| -> PgResult<bool> {
            for i in 0..nargs {
                if base[i] != UNKNOWNOID {
                    continue;
                }
                let (current_category, current_is_preferred) =
                    lsyscache::get_type_category_preferred(c.cand_args()[i])?;
                if current_category != slot_category[i]
                    || (slot_has_preferred_type[i] && !current_is_preferred)
                {
                    return Ok(false);
                }
            }
            Ok(true)
        };
        let mut any_kept = false;
        for c in candidates.iter() {
            if keepit(c)? {
                any_kept = true;
                break;
            }
        }
        // C keeps the whole list when the category rule would reject all.
        if any_kept {
            let mut i = 0;
            while i < candidates.len() {
                if keepit(candidates[i])? {
                    i += 1;
                } else {
                    candidates.remove(i);
                }
            }
            if candidates.len() == 1 {
                return Ok(Some(candidates[0]));
            }
        }
    }

    if nunknowns < nargs {
        let mut known_type = UNKNOWNOID;
        for i in 0..nargs {
            if base[i] == UNKNOWNOID {
                continue;
            }
            if known_type == UNKNOWNOID {
                known_type = base[i];
            } else if known_type != base[i] {
                known_type = UNKNOWNOID;
                break;
            }
        }
        if known_type != UNKNOWNOID {
            let all_known = [known_type; FUNC_MAX_ARGS];
            let mut winner = None;
            let mut ncandidates = 0;
            for c in candidates.iter() {
                if coerce::can_coerce_type(&all_known[..nargs], c.cand_args(), COERCION_IMPLICIT)? {
                    ncandidates += 1;
                    if ncandidates > 1 {
                        break;
                    }
                    winner = Some(*c);
                }
            }
            if ncandidates == 1 {
                return Ok(winner);
            }
        }
    }

    Ok(None)
}

// FuncNameAsType (parse_func.c); LookupTypeNameExtended reduced to the plain
// possibly-qualified name (no typmod, temp_ok=false).
fn FuncNameAsType(parts: &[&str]) -> PgResult<Oid> {
    let (schemaname, typname) = catalog_namespace::DeconstructQualifiedName(parts)?;
    let typoid = match schemaname {
        Some(s) => {
            let ns = catalog_namespace::LookupExplicitNamespace(s, false)?;
            syscache_seams::lookup_pg_type_oid_by_name::call(typname, ns)?
        }
        None => catalog_namespace::TypenameGetTypidExtended(typname, false)?,
    };
    if !OidIsValid(typoid) {
        return Ok(InvalidOid);
    }
    if lsyscache::get_typisdefined(typoid)?
        && !OidIsValid(lsyscache::get_typ_typrelid(typoid)?)
    {
        Ok(typoid)
    } else {
        Ok(InvalidOid)
    }
}

// exprType over the node families proargdefaults carries (system_functions
// defaults are Consts, occasionally coerced).
fn default_expr_type(node: Node<'_>) -> Oid {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resulttype,
        NodeTag::T_ConvertRowtypeExpr => node.as_convert_rowtype_expr().unwrap().resulttype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        tag => panic!("default_expr_type: node family {tag:?} not ported"),
    }
}

#[allow(clippy::too_many_arguments)]
fn func_get_detail<'mcx>(
    mcx: Mcx<'mcx>,
    parts: &[&str],
    fargs: &NodeList<'mcx>,
    fargnames: &[&str],
    nargs: i16,
    argtypes: &[Oid],
    expand_variadic: bool,
    include_out_arguments: bool,
) -> PgResult<FuncDetail<'mcx>> {
    let candidates = catalog_namespace::FuncnameGetCandidatesExtended(
        mcx,
        parts,
        nargs,
        fargnames,
        expand_variadic,
        true,
        include_out_arguments,
        false,
    )?;

    let mut best: Option<&FuncCandidate<'_>> = None;
    for cand in candidates.iter() {
        // C: memcmp over nargs entries (variadic/default tails excluded).
        if cand.args.as_slice()[..argtypes.len().min(cand.args.len())] == *argtypes {
            best = Some(cand);
            break;
        }
    }

    if best.is_none() {
        if nargs == 1 && !fargs.is_nil() && fargnames.is_empty() {
            let target_type = FuncNameAsType(parts)?;
            if OidIsValid(target_type) {
                let source_type = argtypes[0];
                let iscoercion = if source_type == UNKNOWNOID
                    && fargs.nth(0).node_tag() == NodeTag::T_Const
                {
                    true
                } else {
                    match coerce::find_coercion_pathway(target_type, source_type, COERCION_EXPLICIT)?.0
                    {
                        COERCION_PATH_RELABELTYPE => true,
                        COERCION_PATH_COERCEVIAIO => {
                            !((source_type == RECORDOID
                                || OidIsValid(lsyscache::get_typ_typrelid(source_type)?))
                                && coerce::TypeCategory(target_type)? == TYPCATEGORY_STRING)
                        }
                        _ => false,
                    }
                };
                if iscoercion {
                    return Ok(FuncDetail::Coercion { rettype: target_type });
                }
            }
        }

        if !candidates.is_empty() {
            let matched = func_match_argtypes(mcx, argtypes, candidates.as_slice())?;
            if matched.len() == 1 {
                best = Some(matched[0]);
            } else if matched.len() > 1 {
                match func_select_candidate(argtypes, matched)? {
                    Some(c) => best = Some(c),
                    None => return Ok(FuncDetail::Multiple),
                }
            }
        }
    }

    let Some(best) = best else {
        return Ok(FuncDetail::NotFound);
    };
    // C: an InvalidOid "best candidate" is the ambiguous-set placeholder.
    if !OidIsValid(best.oid) {
        return Ok(FuncDetail::Multiple);
    }
    // C: VARIADIC-with-named pedantry — the decorated last arg must have
    // matched the variadic parameter.
    if let Some(argnumbers) = best.argnumbers.as_ref() {
        if !expand_variadic && nargs > 0 && argnumbers[nargs as usize - 1] != nargs as i32 - 1 {
            return Ok(FuncDetail::NotFound);
        }
        for (i, arg) in fargs.iter().enumerate() {
            if arg.node_tag() == NodeTag::T_NamedArgExpr {
                let n = argnumbers[i];
                // SAFETY: parse analysis exclusively owns the just-built tree.
                unsafe {
                    arg.with_mut::<types_nodes::primnodes::NamedArgExpr, _>(|na| {
                        na.argnumber = n
                    })
                }
                .expect("node checked as NamedArgExpr");
            }
        }
    }
    let funcid = best.oid;
    let declared_arg_types = mcx::slice_in(mcx, best.args.as_slice())?;

    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid} (parse_func.c)"));
    if OidIsValid(shape.provariadic)
        && shape.prokind != PROKIND_FUNCTION
        && shape.prokind != PROKIND_PROCEDURE
        && shape.prokind != PROKIND_AGGREGATE
    {
        panic!(
            "func_get_detail (parse_func.c): variadic {} {funcid} unported",
            shape.prokind
        );
    }
    // C: fetch and parse the argument defaults the call omitted.
    let mut argdefaults: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    if best.ndargs > 0 {
        if shape.prokind != PROKIND_FUNCTION
            && shape.prokind != PROKIND_PROCEDURE
            && shape.prokind != PROKIND_WINDOW
        {
            panic!(
                "func_get_detail (parse_func.c): defaulted {} {funcid} unported",
                shape.prokind
            );
        }
        let src = syscache_seams::pg_proc_proargdefaults::call(mcx, funcid)?
            .unwrap_or_else(|| panic!("cache lookup failed for function {funcid} (parse_func.c)"))
            .unwrap_or_else(|| {
                panic!("not enough default arguments (proargdefaults null for {funcid})")
            });
        let defaults = readfuncs::stringToNode(mcx, src.as_str())?;
        let Some(list) = defaults.as_list() else {
            panic!("proargdefaults of {funcid} is not a List");
        };
        match best.argnumbers.as_ref() {
            Some(argnumbers) => {
                // C: named notation can leave any subset of the defaults
                // unused; select by the argnumbers of the defaulted tail.
                let first = (best.nargs - best.ndargs) as usize;
                let defargnumbers: &[i32] = &argnumbers.as_slice()[first..best.nargs as usize];
                let mut i = best.nominal_nargs as i64 - list.len() as i64;
                for cell in list.iter() {
                    if defargnumbers.contains(&(i as i32)) {
                        argdefaults.push(cell);
                    }
                    i += 1;
                }
                debug_assert_eq!(argdefaults.len(), best.ndargs as usize);
            }
            None => {
                // Positional defaults attach to the trailing parameters.
                let skip = list.len() - best.ndargs as usize;
                for (i, cell) in list.iter().enumerate() {
                    if i >= skip {
                        argdefaults.push(cell);
                    }
                }
            }
        }
    }
    Ok(match shape.prokind {
        PROKIND_AGGREGATE => FuncDetail::Aggregate {
            funcid,
            rettype: shape.prorettype,
            retset: shape.proretset,
            declared_arg_types,
            vatype: shape.provariadic,
            nvargs: best.nvargs,
        },
        PROKIND_FUNCTION => FuncDetail::Normal {
            funcid,
            rettype: shape.prorettype,
            retset: shape.proretset,
            declared_arg_types,
            vatype: shape.provariadic,
            nvargs: best.nvargs,
            argdefaults,
        },
        PROKIND_PROCEDURE => FuncDetail::Procedure {
            funcid,
            rettype: shape.prorettype,
            retset: shape.proretset,
            declared_arg_types,
            vatype: shape.provariadic,
            nvargs: best.nvargs,
            argdefaults,
        },
        PROKIND_WINDOW => FuncDetail::WindowFunc {
            funcid,
            rettype: shape.prorettype,
            retset: shape.proretset,
            declared_arg_types,
            argdefaults,
        },
        other => panic!("unrecognized prokind: {other} (parse_func.c func_get_detail)"),
    })
}

// check_srf_call_placement.
pub fn check_srf_call_placement(
    pstate: &mut ParseState<'_, '_>,
    last_srf: Option<Node<'_>>,
    location: ParseLoc,
) -> PgResult<()> {
    use parser_small1::ParseExprKind::*;
    let mut err: Option<&'static str> = None;
    let mut errkind = false;
    match pstate.p_expr_kind {
        EXPR_KIND_NONE => debug_assert!(false),
        EXPR_KIND_OTHER => {}
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            err = Some("set-returning functions are not allowed in JOIN conditions");
        }
        EXPR_KIND_FROM_SUBSELECT => errkind = true,
        EXPR_KIND_FROM_FUNCTION => {
            let same = match (pstate.p_last_srf, last_srf) {
                (None, None) => true,
                (Some(a), Some(b)) => a.ptr_eq(b),
                _ => false,
            };
            if !same {
                let encoding = mbutils::GetDatabaseEncoding();
                // C errpositions at exprLocation(pstate->p_last_srf), the
                // inner SRF.
                let srf_location = pstate.p_last_srf.map(nodes_core::expr_location).unwrap_or(-1);
                return Err(Box::new(
                    ereport(ERROR)
                        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("set-returning functions must appear at top level of FROM")
                        .errposition(parser_errposition(pstate, srf_location, encoding))
                        .into_error()
                        .with_error_location(ErrorLocation::new(
                            "parse_func.c",
                            0,
                            "check_srf_call_placement",
                        )),
                ));
            }
        }
        EXPR_KIND_WHERE => errkind = true,
        EXPR_KIND_POLICY => {
            err = Some("set-returning functions are not allowed in policy expressions");
        }
        EXPR_KIND_HAVING => errkind = true,
        EXPR_KIND_FILTER => errkind = true,
        EXPR_KIND_WINDOW_PARTITION | EXPR_KIND_WINDOW_ORDER => {
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS => {
            err = Some("set-returning functions are not allowed in window definitions");
        }
        EXPR_KIND_SELECT_TARGET | EXPR_KIND_INSERT_TARGET => {
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => errkind = true,
        EXPR_KIND_GROUP_BY | EXPR_KIND_ORDER_BY | EXPR_KIND_DISTINCT_ON => {
            pstate.p_hasTargetSRFs = true;
        }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => errkind = true,
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => errkind = true,
        EXPR_KIND_VALUES => errkind = true,
        EXPR_KIND_VALUES_SINGLE => pstate.p_hasTargetSRFs = true,
        EXPR_KIND_MERGE_WHEN => {
            err = Some("set-returning functions are not allowed in MERGE WHEN conditions");
        }
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = Some("set-returning functions are not allowed in check constraints");
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = Some("set-returning functions are not allowed in DEFAULT expressions");
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = Some("set-returning functions are not allowed in index expressions");
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = Some("set-returning functions are not allowed in index predicates");
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = Some("set-returning functions are not allowed in statistics expressions");
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = Some("set-returning functions are not allowed in transform expressions");
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = Some("set-returning functions are not allowed in EXECUTE parameters");
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = Some("set-returning functions are not allowed in trigger WHEN conditions");
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = Some("set-returning functions are not allowed in partition bound");
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = Some("set-returning functions are not allowed in partition key expressions");
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = Some("set-returning functions are not allowed in CALL arguments");
        }
        EXPR_KIND_COPY_WHERE => {
            err = Some("set-returning functions are not allowed in COPY FROM WHERE conditions");
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = Some("set-returning functions are not allowed in column generation expressions");
        }
        EXPR_KIND_CYCLE_MARK => errkind = true,
    }
    if err.is_some() || errkind {
        let msg = match err {
            Some(err) => String::from(err),
            None => format!(
                "set-returning functions are not allowed in {}",
                srf_expr_kind_name(pstate.p_expr_kind)
            ),
        };
        let encoding = mbutils::GetDatabaseEncoding();
        return Err(Box::new(
            ereport(ERROR)
                .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(msg)
                .errposition(parser_errposition(pstate, location, encoding))
                .into_error()
                .with_error_location(ErrorLocation::new(
                    "parse_func.c",
                    0,
                    "check_srf_call_placement",
                )),
        ));
    }
    Ok(())
}

// ParseExprKindName (parse_expr.c), errkind arms only — parse_expr depends on
// this crate, so the full mapping cannot be imported (parse_agg precedent).
fn srf_expr_kind_name(kind: parser_small1::ParseExprKind) -> &'static str {
    use parser_small1::ParseExprKind::*;
    match kind {
        EXPR_KIND_FROM_SUBSELECT => "sub-SELECT in FROM",
        EXPR_KIND_WHERE => "WHERE",
        EXPR_KIND_HAVING => "HAVING",
        EXPR_KIND_FILTER => "FILTER",
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => "UPDATE",
        EXPR_KIND_LIMIT => "LIMIT",
        EXPR_KIND_OFFSET => "OFFSET",
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => "RETURNING",
        EXPR_KIND_VALUES => "VALUES",
        EXPR_KIND_CYCLE_MARK => "CYCLE",
        other => panic!("srf_expr_kind_name: non-errkind kind {other:?}"),
    }
}

// coerce_type's result type per its head arms: ANY-family targets pass the
// node through (type stays actual), domain inputs under poly-array targets
// relabel to their base, everything else lands on the declared type.
fn post_coercion_type(actual: Oid, declared: Oid) -> PgResult<Oid> {
    use types_core::catalog::{
        ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
        ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID,
        ANYENUMOID, ANYMULTIRANGEOID, ANYNONARRAYOID, ANYOID, ANYRANGEOID,
    };
    if actual == declared {
        return Ok(actual);
    }
    Ok(match declared {
        ANYOID | ANYELEMENTOID | ANYNONARRAYOID | ANYCOMPATIBLEOID
        | ANYCOMPATIBLENONARRAYOID => actual,
        ANYARRAYOID | ANYENUMOID | ANYRANGEOID | ANYMULTIRANGEOID | ANYCOMPATIBLEARRAYOID
        | ANYCOMPATIBLERANGEOID | ANYCOMPATIBLEMULTIRANGEOID
            if actual != UNKNOWNOID =>
        {
            lsyscache::getBaseType(actual)?
        }
        _ => declared,
    })
}

// make_fn_arguments (parse_func.c). Divergence: rebuilds the list instead of
// C's in-place lfirst replacement; NamedArgExpr is unrepresented in
// types_nodes, so named-argument wrapping cannot arise.
pub fn make_fn_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    fargs: NodeList<'mcx>,
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
) -> PgResult<NodeList<'mcx>> {
    if actual_arg_types == declared_arg_types {
        return Ok(fargs);
    }
    let mut out = NodeList::nil();
    for (i, node) in fargs.iter().enumerate() {
        let node = if actual_arg_types[i] != declared_arg_types[i] {
            // C coerces through a NamedArgExpr wrapper in place.
            if node.node_tag() == NodeTag::T_NamedArgExpr {
                let inner = node
                    .as_named_arg_expr()
                    .unwrap()
                    .arg
                    .expect("NamedArgExpr has an arg");
                let coerced = coerce::coerce_type(
                    mcx,
                    pstate,
                    inner,
                    actual_arg_types[i],
                    declared_arg_types[i],
                    -1,
                    COERCION_IMPLICIT,
                    CoercionForm::COERCE_IMPLICIT_CAST,
                    -1,
                )?;
                // SAFETY: parse analysis exclusively owns the just-built tree.
                unsafe {
                    node.with_mut::<types_nodes::primnodes::NamedArgExpr, _>(|na| {
                        na.arg = Some(coerced)
                    })
                }
                .expect("node checked as NamedArgExpr");
                node
            } else {
                coerce::coerce_type(
                    mcx,
                    pstate,
                    node,
                    actual_arg_types[i],
                    declared_arg_types[i],
                    -1,
                    COERCION_IMPLICIT,
                    CoercionForm::COERCE_IMPLICIT_CAST,
                    -1,
                )?
            }
        } else {
            node
        };
        out.lappend(mcx, node)?;
    }
    Ok(out)
}

fn name_parts<'a, 'mcx>(name: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = name.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = name.nth(i).as_string().expect("function name list holds String nodes").sval;
    }
    &buf[..n]
}

fn name_list_to_string(parts: &[&str]) -> String {
    parts.join(".")
}

fn func_signature_string(parts: &[&str], argnames: &[&str], argtypes: &[Oid]) -> PgResult<String> {
    let mut sig = name_list_to_string(parts);
    sig.push('(');
    let numposargs = argtypes.len() - argnames.len();
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push_str(", ");
        }
        if i >= numposargs {
            sig.push_str(argnames[i - numposargs]);
            sig.push_str(" => ");
        }
        sig.push_str(&format_type::format_type_be(t)?);
    }
    sig.push(')');
    Ok(sig)
}

#[track_caller]
#[cold]
#[inline(never)]
fn variadic_not_array(pstate: &ParseState<'_, '_>, fargs: &NodeList<'_>) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    let loc = fargs.last().map_or(-1, expr_location);
    Box::new(
        ereport(ERROR)
            .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
            .errmsg("VARIADIC argument must be an array")
            .errposition(parser_errposition(pstate, loc, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

// C exprLocation (nodeFuncs.c) over transformed call arguments; closed-set
// copy of parse_expr::node_funcs (dependency cycle forbids sharing).
fn expr_location(node: Node<'_>) -> ParseLoc {
    fn leftmost(a: ParseLoc, b: ParseLoc) -> ParseLoc {
        if a < 0 { b } else if b < 0 { a } else { a.min(b) }
    }
    fn list_loc(l: &NodeList<'_>) -> ParseLoc {
        let mut loc = -1;
        for n in l.iter() {
            loc = leftmost(loc, expr_location(n));
            if loc == 0 {
                break;
            }
        }
        loc
    }
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Param => node.as_param().unwrap().location,
        NodeTag::T_Aggref => node.as_aggref().unwrap().location,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().location,
        NodeTag::T_OpExpr => {
            let op = node.as_op_expr().unwrap();
            leftmost(op.location, list_loc(&op.args))
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            leftmost(f.location, list_loc(&f.args))
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            leftmost(r.location, expr_location(r.arg))
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            leftmost(c.location, expr_location(c.arg))
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            leftmost(a.location, expr_location(a.arg))
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            leftmost(c.location, expr_location(c.arg))
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().location,
        NodeTag::T_CaseTestExpr => -1,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().location,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().location,
        NodeTag::T_SQLValueFunction => node.as_sql_value_function().unwrap().location,
        NodeTag::T_SubLink => node.as_sub_link().unwrap().location,
        NodeTag::T_SetToDefault => node.as_set_to_default().unwrap().location,
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            leftmost(b.location, list_loc(&b.args))
        }
        NodeTag::T_NullTest => {
            let n = node.as_null_test().unwrap();
            leftmost(n.location, n.arg.map_or(-1, expr_location))
        }
        other => panic!("exprLocation (nodeFuncs.c): arm for {other:?} unported"),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_arguments(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg(format!("cannot pass more than {FUNC_MAX_ARGS} arguments to a function"))
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn wrong_object_type(
    pstate: &ParseState<'_, '_>,
    msg: String,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn wrong_object_type_hint(
    pstate: &ParseState<'_, '_>,
    msg: String,
    hint: &'static str,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(msg)
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn ambiguous_function(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    proc_call: bool,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let (msg, hint) = if proc_call {
        (
            format!("procedure {sig} is not unique"),
            "Could not choose a best candidate procedure. You might need to add explicit \
             type casts.",
        )
    } else {
        (
            format!("function {sig} is not unique"),
            "Could not choose a best candidate function. You might need to add explicit \
             type casts.",
        )
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
            .errmsg(msg)
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_function(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    misplaced_order_by: bool,
    proc_call: bool,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let (msg, hint) = if misplaced_order_by {
        (
            format!("function {sig} does not exist"),
            "No aggregate function matches the given name and argument types. Perhaps you \
             misplaced ORDER BY; ORDER BY must appear after all regular arguments of the \
             aggregate.",
        )
    } else if proc_call {
        (
            format!("procedure {sig} does not exist"),
            "No procedure matches the given name and argument types. You might need to add \
             explicit type casts.",
        )
    } else {
        (
            format!("function {sig} does not exist"),
            "No function matches the given name and argument types. You might need to add \
             explicit type casts.",
        )
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(msg)
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ParseFuncOrColumn")),
    )
}

// LookupFuncNameInternal (parse_func.c), OBJECT_FUNCTION/OBJECT_ROUTINE
// slice.
fn lookup_func_name_internal(
    objtype: ObjectType,
    parts: &[&str],
    nargs: i16,
    argtypes: &[Oid],
    include_out_arguments: bool,
    missing_ok: bool,
) -> PgResult<Result<Oid, bool>> {
    // Err(true) = ambiguous, Err(false) = no such function.
    let scratch = mcx::MemoryContext::new("LookupFuncNameInternal");
    let clist = catalog_namespace::FuncnameGetCandidatesExtended(
        scratch.mcx(),
        parts,
        nargs,
        &[],
        false,
        false,
        include_out_arguments,
        missing_ok,
    )?;
    let mut result = InvalidOid;
    for cand in clist.iter() {
        if nargs > 0 && cand.args.as_slice()[..nargs as usize] != argtypes[..nargs as usize] {
            continue;
        }
        // FuncnameGetCandidates duplicate marker (oid cleared).
        if !OidIsValid(cand.oid) {
            return Ok(Err(true));
        }
        let prokind = lsyscache::get_func_prokind(cand.oid)?;
        match objtype {
            ObjectType::OBJECT_FUNCTION | ObjectType::OBJECT_AGGREGATE
                if prokind == PROKIND_PROCEDURE =>
            {
                continue
            }
            ObjectType::OBJECT_PROCEDURE if prokind != PROKIND_PROCEDURE => continue,
            _ => {}
        }
        if OidIsValid(result) {
            return Ok(Err(true));
        }
        result = cand.oid;
    }
    if OidIsValid(result) {
        Ok(Ok(result))
    } else {
        Ok(Err(false))
    }
}

#[track_caller]
#[cold]
fn func_name_not_unique(parts: &[&str]) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
            .errmsg(format!("function name \"{}\" is not unique", name_list_to_string(parts)))
            .errhint("Specify the argument list to select the function unambiguously.".to_string())
            .into_error(),
    )
}

#[cold]
fn function_does_not_exist(parts: &[&str], argtypes: &[Oid]) -> PgResult<Box<PgError>> {
    Ok(Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string(parts, &[], argtypes)?
            ))
            .into_error(),
    ))
}

// LookupFuncName (parse_func.c) with explicit arg types.
pub fn LookupFuncName(
    funcname: &NodeList<'_>,
    nargs: i16,
    argtypes: &[Oid],
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut buf = [""; 4];
    let parts = name_parts(funcname, &mut buf);
    match lookup_func_name_internal(ObjectType::OBJECT_FUNCTION, parts, nargs, argtypes, false, missing_ok)? {
        Ok(oid) => Ok(oid),
        Err(true) => Err(func_name_not_unique(parts)),
        Err(false) => {
            if missing_ok {
                return Ok(InvalidOid);
            }
            Err(function_does_not_exist(parts, &argtypes[..nargs.max(0) as usize])?)
        }
    }
}

// LookupFuncWithArgs (parse_func.c) over a grammar ObjectWithArgs (objargs
// TypeNames; args_unspecified => any-arity lookup). Divergence: the
// PROCEDURE/ROUTINE include_out_arguments second pass is not ported
// (OUT-parameter procedures).
pub fn LookupFuncWithArgs(
    objtype: ObjectType,
    func: &types_nodes::parsenodes::ObjectWithArgs<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    use types_nodes::parsenodes::ObjectType::*;
    debug_assert!(matches!(objtype, OBJECT_AGGREGATE | OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE));
    let objname = &func.objname;
    let objargs = &func.objargs;
    let args_unspecified = func.args_unspecified;
    let argcount = objargs.len();
    if argcount > FUNC_MAX_ARGS {
        let noun = if objtype == OBJECT_PROCEDURE { "procedures" } else { "functions" };
        return Err(Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
                .errmsg(format!("{noun} cannot have more than {FUNC_MAX_ARGS} arguments"))
                .into_error(),
        ));
    }
    let scratch = mcx::MemoryContext::new("LookupFuncWithArgs");
    let mut argoids = [InvalidOid; FUNC_MAX_ARGS];
    for (i, n) in objargs.iter().enumerate() {
        // None cells arise only from oper_argtypes NONE; function paths never
        // carry them (C extractArgTypes emits no NULLs).
        let t = n
            .expect("function objargs cell is non-NULL")
            .as_variant::<types_nodes::rawnodes::TypeName>()
            .expect("objargs holds TypeName nodes");
        argoids[i] = parse_utilcmd::LookupTypeNameOidExtended(scratch.mcx(), t, missing_ok)?;
        if !OidIsValid(argoids[i]) {
            debug_assert!(missing_ok);
            return Ok(InvalidOid);
        }
    }
    let nargs: i16 = if args_unspecified { -1 } else { argcount as i16 };

    let mut buf = [""; 4];
    let parts = name_parts(objname, &mut buf);
    // With an argument list the objtype filter is disabled (OBJECT_ROUTINE):
    // "object is of wrong type" beats "object doesn't exist".
    let lookup_objtype = if args_unspecified { objtype } else { OBJECT_ROUTINE };
    let mut lookup =
        lookup_func_name_internal(lookup_objtype, parts, nargs, &argoids, false, missing_ok)?;

    // Second lookup considering all arguments (proallargtypes), for
    // PROCEDURE/ROUTINE with a mode-markerless argument list.
    if matches!(objtype, OBJECT_PROCEDURE | OBJECT_ROUTINE)
        && !func.objfuncargs.is_nil()
        && lookup != Err(true)
    {
        let have_param_mode = func.objfuncargs.iter().any(|n| {
            n.as_variant::<types_nodes::parsenodes::FunctionParameter>()
                .expect("objfuncargs holds FunctionParameter nodes")
                .mode
                != types_nodes::parsenodes::FunctionParameterMode::FUNC_PARAM_DEFAULT
        });
        if !have_param_mode {
            debug_assert_eq!(func.objfuncargs.len(), argcount);
            match lookup_func_name_internal(
                objtype,
                parts,
                argcount as i16,
                &argoids,
                true,
                missing_ok,
            )? {
                Ok(poid) => {
                    lookup = match lookup {
                        Ok(oid) if oid != poid => Err(true),
                        _ => Ok(poid),
                    };
                }
                Err(true) => lookup = Err(true),
                Err(false) => {}
            }
        }
    }

    match lookup {
        Ok(oid) => {
            let prokind = lsyscache::get_func_prokind(oid)?;
            match objtype {
                OBJECT_FUNCTION if prokind == PROKIND_PROCEDURE => {
                    Err(wrong_prokind("%s is not a function", parts, &argoids[..argcount])?)
                }
                OBJECT_PROCEDURE if prokind != PROKIND_PROCEDURE => {
                    Err(wrong_prokind("%s is not a procedure", parts, &argoids[..argcount])?)
                }
                OBJECT_AGGREGATE if prokind != PROKIND_AGGREGATE => Err(wrong_prokind(
                    "function %s is not an aggregate",
                    parts,
                    &argoids[..argcount],
                )?),
                _ => Ok(oid),
            }
        }
        Err(true) => {
            let noun = match objtype {
                OBJECT_PROCEDURE => "procedure",
                OBJECT_AGGREGATE => "aggregate",
                OBJECT_ROUTINE => "routine",
                _ => "function",
            };
            let mut rpt = ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                .errmsg(format!("{noun} name \"{}\" is not unique", name_list_to_string(parts)));
            if args_unspecified {
                rpt = rpt.errhint(format!(
                    "Specify the argument list to select the {noun} unambiguously."
                ));
            }
            Err(Box::new(rpt.into_error()))
        }
        Err(false) => {
            if missing_ok {
                return Ok(InvalidOid);
            }
            let (noun, named) = match objtype {
                OBJECT_PROCEDURE => ("procedure", "a procedure"),
                OBJECT_AGGREGATE => ("aggregate", "an aggregate"),
                _ => ("function", "a function"),
            };
            if args_unspecified {
                Err(Box::new(
                    ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_FUNCTION)
                        .errmsg(format!(
                            "could not find {named} named \"{}\"",
                            name_list_to_string(parts)
                        ))
                        .into_error(),
                ))
            } else if objtype == OBJECT_AGGREGATE && argcount == 0 {
                Err(Box::new(
                    ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_FUNCTION)
                        .errmsg(format!(
                            "aggregate {}(*) does not exist",
                            name_list_to_string(parts)
                        ))
                        .into_error(),
                ))
            } else {
                Err(Box::new(
                    ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_FUNCTION)
                        .errmsg(format!(
                            "{noun} {} does not exist",
                            func_signature_string(parts, &[], &argoids[..argcount])?
                        ))
                        .into_error(),
                ))
            }
        }
    }
}

#[cold]
fn wrong_prokind(template: &str, parts: &[&str], argtypes: &[Oid]) -> PgResult<Box<PgError>> {
    Ok(Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(template.replacen("%s", &func_signature_string(parts, &[], argtypes)?, 1))
            .into_error(),
    ))
}

// recheck_cast_function_args' parser tail (clauses.c): re-resolve polymorphics
// and re-coerce exactly as the parser did; installed behind clauses_seams (a
// clauses -> parser dependency cycles).
fn recheck_cast_function_args<'mcx>(
    mcx: Mcx<'mcx>,
    args: NodeList<'mcx>,
    actual_arg_types: &[Oid],
    declared_arg_types: &[Oid],
    result_type: Oid,
    prorettype: Oid,
) -> PgResult<NodeList<'mcx>> {
    if args.len() > FUNC_MAX_ARGS {
        return Err(Box::new(PgError::error("too many function arguments")));
    }
    debug_assert_eq!(args.len(), actual_arg_types.len());
    let mut declared = mcx::slice_in(mcx, declared_arg_types)?;
    let rettype = coerce::enforce_generic_type_consistency(
        actual_arg_types,
        declared.as_mut_slice(),
        prorettype,
        false,
    )?;
    // C: just check we got the same answer as the parser did.
    if rettype != result_type {
        return Err(Box::new(PgError::error(
            "function's resolved result type changed during planning",
        )));
    }
    let pstate = parser_small1::make_parsestate(mcx, None);
    make_fn_arguments(mcx, &pstate, args, actual_arg_types, declared.as_slice())
}

fn lookup_func_with_args_seam(
    objtype: i32,
    func: &types_nodes::parsenodes::ObjectWithArgs<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    LookupFuncWithArgs(objtype_from_i32(objtype), func, missing_ok)
}

fn lookup_func_name_seam(
    parts: &[&str],
    nargs: i16,
    argtypes: &[Oid],
    missing_ok: bool,
) -> PgResult<Oid> {
    match lookup_func_name_internal(ObjectType::OBJECT_FUNCTION, parts, nargs, argtypes, false, missing_ok)? {
        Ok(oid) => Ok(oid),
        Err(true) => Err(func_name_not_unique(parts)),
        Err(false) => {
            if missing_ok {
                return Ok(InvalidOid);
            }
            Err(function_does_not_exist(parts, &argtypes[..nargs.max(0) as usize])?)
        }
    }
}

fn objtype_from_i32(objtype: i32) -> types_nodes::parsenodes::ObjectType {
    assert!(
        (0..=types_nodes::parsenodes::ObjectType::OBJECT_VIEW as i32).contains(&objtype),
        "LookupFuncWithArgs seam: bad ObjectType {objtype}"
    );
    // SAFETY: ObjectType is repr(u32) and contiguous over the asserted range.
    unsafe { core::mem::transmute::<u32, types_nodes::parsenodes::ObjectType>(objtype as u32) }
}

/// C `ParseComplexProjection` (parse_func.c): Ok(None) = not a column of the
/// composite; caller decides between function notation and unknown_attribute.
pub fn ParseComplexProjection<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    funcname: &str,
    first_arg: Node<'mcx>,
    location: ParseLoc,
) -> PgResult<Option<Node<'mcx>>> {
    if let Some(v) = first_arg.as_var() {
        if v.varattno == types_core::InvalidAttrNumber {
            let nsitem =
                parse_relation::GetNSItemByRangeTablePosn(pstate, v.varno, v.varlevelsup as i32);
            return parse_relation::scanNSItemForColumn(
                mcx,
                pstate,
                nsitem,
                v.varlevelsup as i32,
                funcname,
                location,
            );
        }
    }
    let tupdesc = if first_arg.as_var().is_some_and(|v| v.vartype == RECORDOID) {
        Some(parse_func_seams::expandRecordVariable::call(mcx, pstate, first_arg, 0)?)
    } else {
        funcapi::get_expr_result_tupdesc(mcx, Some(first_arg), true)?
    };
    let Some(tupdesc) = tupdesc else {
        return Ok(None);
    };
    for i in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(i);
        if !att.attisdropped && att.attname.name_str() == funcname.as_bytes() {
            return Ok(Some(Node::mk(
                mcx,
                types_nodes::FieldSelect {
                    arg: first_arg,
                    fieldnum: (i + 1) as types_core::AttrNumber,
                    resulttype: att.atttypid,
                    resulttypmod: att.atttypmod,
                    resultcollid: att.attcollation,
                },
            )?));
        }
    }
    Ok(None)
}
