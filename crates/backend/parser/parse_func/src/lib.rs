#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use catalog_namespace::{FuncCandidate, FuncnameGetCandidates, OperCandidate};
use coerce::{COERCION_EXPLICIT, COERCION_IMPLICIT, COERCION_PATH_COERCEVIAIO,
    COERCION_PATH_RELABELTYPE, TYPCATEGORY_INVALID, TYPCATEGORY_STRING};
use elog::ereport;
use mcx::{Mcx, PgVec};
use parser_small1::{parser_errposition, ParseState};
use types_core::catalog::{RECORDOID, UNKNOWNOID};
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_AMBIGUOUS_FUNCTION,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_ARGUMENTS,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::primnodes::{Aggref, WindowFunc, AGGKIND_HYPOTHETICAL, AGGKIND_ORDERED_SET};
use types_nodes::parsenodes::ObjectType;
use types_nodes::rawnodes::FuncCall;
use types_nodes::{CoercionForm, FuncExpr, NamedArgExpr, Node, NodeList, NodeTag};



const FUNC_MAX_ARGS: usize = 100;

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
    Aggregate { funcid: Oid, rettype: Oid, retset: bool, declared_arg_types: PgVec<'mcx, Oid> },
    Coercion { rettype: Oid },
    Multiple,
    Procedure { funcid: Oid },
    WindowFunc { funcid: Oid, rettype: Oid, retset: bool, declared_arg_types: PgVec<'mcx, Oid> },
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

/// C `ParseFuncOrColumn` function-syntax path (fn always present; column
/// syntax lands with ParseComplexProjection). Divergence: actual arg types
/// precomputed by the caller (parse_oper::make_op precedent).
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
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    debug_assert_eq!(fargs.len(), actual_arg_types.len());
    let mut buf = [""; 4];
    let parts = name_parts(funcname, &mut buf);

    let over = fn_call.over;

    if fargs.len() > FUNC_MAX_ARGS {
        return Err(too_many_arguments(pstate, location));
    }

    // C: mixed notation is allowed, but only with all the named parameters
    // after all the unnamed ones, so argnames maps onto the last N actuals.
    let mut argnames: PgVec<'mcx, &'mcx str> = PgVec::new_in(mcx);
    for arg in &fargs {
        if let Some(na) = arg.as_named_arg_expr() {
            let name = na.name.expect("NamedArgExpr in fargs carries a name");
            if argnames.iter().any(|prev| *prev == name) {
                return Err(duplicate_argument_name(pstate, name, na.location));
            }
            argnames.push(name);
        } else if !argnames.is_empty() {
            return Err(positional_after_named(pstate, expr_location(arg)));
        }
    }

    let nargs = fargs.len() as i16;
    let (fdresult, fargs) = func_get_detail(
        mcx,
        parts,
        fargs,
        nargs,
        actual_arg_types,
        argnames.as_slice(),
        !fn_call.func_variadic,
    )?;

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
            Err(ambiguous_function(pstate, parts, argnames.as_slice(), actual_arg_types, location))
        }
        FuncDetail::Normal {
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
            // C: append omitted-argument defaults to the arg list.
            let mut all_arg_types: PgVec<'mcx, Oid> =
                mcx::vec_with_capacity_in(mcx, actual_arg_types.len() + argdefaults.len())?;
            for &t in actual_arg_types {
                all_arg_types.push(t);
            }
            let fargs = if argdefaults.is_empty() {
                fargs
            } else {
                let mut cells: PgVec<'mcx, Node<'mcx>> =
                    mcx::vec_with_capacity_in(mcx, fargs.len() + argdefaults.len())?;
                for c in fargs.iter() {
                    cells.push(c);
                }
                for d in argdefaults.iter() {
                    cells.push(*d);
                    all_arg_types.push(default_expr_type(*d));
                }
                NodeList::from_slice(mcx, cells.as_slice())?
            };
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
            let fargs = if nvargs > 0 && vatype != types_core::catalog::ANYOID {
                // C: pack the trailing nvargs coerced arguments into an
                // ArrayExpr — the call becomes VARIADIC.
                let non_var_args = fargs.len() - nvargs as usize;
                let mut plain: PgVec<'mcx, Node<'mcx>> =
                    mcx::vec_with_capacity_in(mcx, non_var_args + 1)?;
                let mut vargs: PgVec<'mcx, Node<'mcx>> =
                    mcx::vec_with_capacity_in(mcx, nvargs as usize)?;
                for (i, n) in fargs.iter().enumerate() {
                    if i < non_var_args {
                        plain.push(n);
                    } else {
                        vargs.push(n);
                    }
                }
                let element_typeid = post_coercion_type(
                    actual_arg_types[non_var_args],
                    declared_arg_types[non_var_args],
                )?;
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
                debug_assert!(!func_variadic);
                func_variadic = true;
                NodeList::from_slice(mcx, plain.as_slice())?
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
        FuncDetail::Aggregate { funcid, rettype, retset, declared_arg_types } => {
            let aggshape = syscache_seams::lookup_pg_aggregate_shape::call(funcid)?
                .unwrap_or_else(|| {
                    panic!("cache lookup failed for aggregate {funcid} (parse_func.c)")
                });
            let aggkind = aggshape.aggkind;
            if aggkind == AGGKIND_ORDERED_SET || aggkind == AGGKIND_HYPOTHETICAL {
                panic!(
                    "ParseFuncOrColumn (parse_func.c): ordered-set aggregate arm \
                     (WITHIN GROUP direct-args split) unported — unit backend-parser-func"
                );
            }
            if fn_call.agg_within_group {
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
            if let Some(over_node) = over {
                return build_window_func(
                    mcx, pstate, funcid, rettype, retset, fargs, true, fn_call, agg_filter,
                    parts, over_node, _last_srf, location,
                );
            }
            // C's aggargtypes = exprType per coerced arg (parse_agg.c);
            // parse_expr::expr_type is dependency-forbidden here, so replay
            // coerce_type's head-arm result-type contract.
            let mut agg_arg_types = mcx::vec_with_capacity_in(mcx, actual_arg_types.len())?;
            for (&a, &d) in actual_arg_types.iter().zip(declared_arg_types.iter()) {
                agg_arg_types.push(post_coercion_type(a, d)?);
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

            // C: named notation would need NamedArgExpr-vs-TargetEntry layering
            // decisions plus planner reordering; disallowed for aggregates.
            if !argnames.is_empty() {
                return Err(feature_not_supported(
                    pstate,
                    "aggregates cannot use named arguments".into(),
                    None,
                    location,
                ));
            }

            let mut aggref = Node::build::<Aggref>(mcx)?;
            aggref.aggfnoid = funcid;
            aggref.aggtype = rettype;
            aggref.aggkind = aggkind;
            aggref.aggfilter = agg_filter;
            aggref.aggstar = fn_call.agg_star;
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
        FuncDetail::Procedure { .. } => Err(wrong_object_type_hint(
            pstate,
            format!(
                "{} is a procedure",
                func_signature_string(parts, argnames.as_slice(), actual_arg_types)?
            ),
            "To call a procedure, use CALL.",
            location,
        )),
        FuncDetail::WindowFunc { funcid, rettype, retset, declared_arg_types } => {
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
        FuncDetail::NotFound => Err(undefined_function(
            pstate,
            parts,
            argnames.as_slice(),
            actual_arg_types,
            fn_call.agg_order.len() > 1 && !fn_call.agg_within_group,
            location,
        )),
    }
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
        return Err(feature_not_supported(
            pstate,
            "window function calls cannot contain set-returning function calls".into(),
            Some(
                "You might be able to move the set-returning function into a LATERAL FROM item.",
            ),
            location,
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
                .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
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
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
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
                .with_error_location(ErrorLocation::new("parse_func.c", 0, "func_select_candidate")),
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
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        tag => panic!("default_expr_type: node family {tag:?} not ported"),
    }
}

// Named/mixed notation returns a rebuilt fargs whose NamedArgExpr wrappers
// carry the winning candidate's argnumbers (C writes na->argnumber in place).
fn func_get_detail<'mcx>(
    mcx: Mcx<'mcx>,
    parts: &[&str],
    fargs: NodeList<'mcx>,
    nargs: i16,
    argtypes: &[Oid],
    argnames: &[&str],
    expand_variadic: bool,
) -> PgResult<(FuncDetail<'mcx>, NodeList<'mcx>)> {
    let candidates = FuncnameGetCandidates(mcx, parts, nargs, argnames, expand_variadic, true)?;

    let mut best: Option<&FuncCandidate<'_>> = None;
    for cand in candidates.iter() {
        // C: memcmp over nargs entries (variadic/default tails excluded).
        if cand.args.as_slice()[..argtypes.len().min(cand.args.len())] == *argtypes {
            best = Some(cand);
            break;
        }
    }

    if best.is_none() {
        if nargs == 1 && !fargs.is_nil() && argnames.is_empty() {
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
                    return Ok((FuncDetail::Coercion { rettype: target_type }, fargs));
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
                    None => return Ok((FuncDetail::Multiple, fargs)),
                }
            }
        }
    }

    let Some(best) = best else {
        return Ok((FuncDetail::NotFound, fargs));
    };
    // C: an InvalidOid "best candidate" is the ambiguous-set placeholder.
    if !OidIsValid(best.oid) {
        return Ok((FuncDetail::Multiple, fargs));
    }
    // C: VARIADIC with named arguments is only allowed when the decorated
    // last argument actually matched the variadic parameter.
    if !argnames.is_empty() && !expand_variadic && nargs > 0 {
        let an = best.argnumbers.as_ref().expect("named-notation candidate carries argnumbers");
        if an[nargs as usize - 1] != nargs as i32 - 1 {
            return Ok((FuncDetail::NotFound, fargs));
        }
    }
    let funcid = best.oid;
    let declared_arg_types = mcx::slice_in(mcx, best.args.as_slice())?;

    // C: return actual argument positions into the NamedArgExpr nodes.
    let fargs = match best.argnumbers.as_ref() {
        None => fargs,
        Some(an) => {
            let mut out = NodeList::nil();
            for (i, cell) in fargs.iter().enumerate() {
                let node = match cell.as_named_arg_expr() {
                    None => cell,
                    Some(na) => Node::mk(
                        mcx,
                        NamedArgExpr {
                            arg: na.arg,
                            name: na.name,
                            argnumber: an[i],
                            location: na.location,
                        },
                    )?,
                };
                out.lappend(mcx, node)?;
            }
            out
        }
    };

    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid} (parse_func.c)"));
    if OidIsValid(shape.provariadic) && shape.prokind != PROKIND_FUNCTION {
        panic!(
            "func_get_detail (parse_func.c): variadic {} {funcid} unported",
            shape.prokind
        );
    }
    // C: fetch and parse the trailing argument defaults the call omitted.
    let mut argdefaults: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    if best.ndargs > 0 {
        if shape.prokind != PROKIND_FUNCTION {
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
            // C: in named notation the supplied args can replace any subset of
            // the defaults; keep the defaults whose argnumber the candidate's
            // defaulted tail names (positional order preserved).
            Some(an) => {
                let firstdefarg = &an[(best.nargs - best.ndargs) as usize..best.nargs as usize];
                let start = best.nominal_nargs as i32 - list.len() as i32;
                for (i, cell) in list.iter().enumerate() {
                    if firstdefarg.contains(&(start + i as i32)) {
                        argdefaults.push(cell);
                    }
                }
                debug_assert_eq!(argdefaults.len(), best.ndargs as usize);
            }
            None => {
                // Defaults attach to the trailing parameters: take the tail.
                let skip = list.len() - best.ndargs as usize;
                for (i, cell) in list.iter().enumerate() {
                    if i >= skip {
                        argdefaults.push(cell);
                    }
                }
            }
        }
    }
    let detail = match shape.prokind {
        PROKIND_AGGREGATE => FuncDetail::Aggregate {
            funcid,
            rettype: shape.prorettype,
            retset: shape.proretset,
            declared_arg_types,
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
        PROKIND_PROCEDURE => FuncDetail::Procedure { funcid },
        PROKIND_WINDOW => FuncDetail::WindowFunc {
            funcid,
            rettype: shape.prorettype,
            retset: shape.proretset,
            declared_arg_types,
        },
        other => panic!("unrecognized prokind: {other} (parse_func.c func_get_detail)"),
    };
    Ok((detail, fargs))
}

// check_srf_call_placement. DIVERGENCE: the nested-SRF errposition points at
// this call, not the inner SRF (exprLocation walker unported).
fn check_srf_call_placement(
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
                return Err(Box::new(
                    ereport(ERROR)
                        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("set-returning functions must appear at top level of FROM")
                        .errposition(parser_errposition(pstate, location, encoding))
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
// C's in-place lfirst/na->arg replacement.
fn make_fn_arguments<'mcx>(
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
            let coerce_one = |arg| {
                coerce::coerce_type(
                    mcx,
                    pstate,
                    arg,
                    actual_arg_types[i],
                    declared_arg_types[i],
                    -1,
                    COERCION_IMPLICIT,
                    CoercionForm::COERCE_IMPLICIT_CAST,
                    -1,
                )
            };
            match node.as_named_arg_expr() {
                // C: coerce the input expr; the NamedArgExpr stays on top.
                Some(na) => Node::mk(
                    mcx,
                    NamedArgExpr {
                        arg: coerce_one(na.arg)?,
                        name: na.name,
                        argnumber: na.argnumber,
                        location: na.location,
                    },
                )?,
                None => coerce_one(node)?,
            }
        } else {
            node
        };
        out.lappend(mcx, node)?;
    }
    Ok(out)
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

#[cold]
#[inline(never)]
fn duplicate_argument_name(
    pstate: &ParseState<'_, '_>,
    name: &str,
    location: ParseLoc,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("argument name \"{name}\" used more than once"))
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

#[cold]
#[inline(never)]
fn positional_after_named(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("positional argument cannot follow named argument")
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

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
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
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
        NodeTag::T_NamedArgExpr => {
            let na = node.as_named_arg_expr().unwrap();
            leftmost(na.location, expr_location(na.arg))
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            leftmost(r.location, expr_location(r.arg))
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
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
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

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
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

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
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

#[cold]
#[inline(never)]
fn ambiguous_function(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
            .errmsg(format!("function {sig} is not unique"))
            .errhint(
                "Could not choose a best candidate function. You might need to add explicit \
                 type casts.",
            )
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

#[cold]
#[inline(never)]
fn undefined_function(
    pstate: &ParseState<'_, '_>,
    parts: &[&str],
    argnames: &[&str],
    argtypes: &[Oid],
    misplaced_order_by: bool,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argnames, argtypes) {
        Ok(sig) => sig,
        Err(e) => return e,
    };
    let hint = if misplaced_order_by {
        "No aggregate function matches the given name and argument types. Perhaps you \
         misplaced ORDER BY; ORDER BY must appear after all regular arguments of the aggregate."
    } else {
        "No function matches the given name and argument types. You might need to add \
         explicit type casts."
    };
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function {sig} does not exist"))
            .errhint(hint)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_func.c", 0, "ParseFuncOrColumn")),
    )
}

// LookupFuncNameInternal (parse_func.c), OBJECT_FUNCTION/OBJECT_ROUTINE
// slice; include_out_arguments lanes (procedures) are unreachable here.
fn lookup_func_name_internal(
    objtype: ObjectType,
    parts: &[&str],
    nargs: i16,
    argtypes: &[Oid],
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
        missing_ok,
    )?;
    let mut result = InvalidOid;
    for cand in clist.iter() {
        if nargs > 0 && cand.args.as_slice()[..nargs as usize] != argtypes[..nargs as usize] {
            continue;
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
    match lookup_func_name_internal(ObjectType::OBJECT_FUNCTION, parts, nargs, argtypes, missing_ok)? {
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
// (OUT-parameter spellings resolve as missing — notes/grant-objpriv-lane.md).
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
        let t = n
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
    match lookup_func_name_internal(lookup_objtype, parts, nargs, &argoids, missing_ok)? {
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

pub fn init_seams() {
    parse_func_seams::LookupFuncWithArgs::set(lookup_func_with_args_seam);
    parse_func_seams::LookupFuncName::set(lookup_func_name_seam);
    clauses_seams::recheck_cast_function_args::set(recheck_cast_function_args);
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
    match lookup_func_name_internal(ObjectType::OBJECT_FUNCTION, parts, nargs, argtypes, missing_ok)? {
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
