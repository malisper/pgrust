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
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::primnodes::{Aggref, WindowFunc, AGGKIND_HYPOTHETICAL, AGGKIND_ORDERED_SET};
use types_nodes::rawnodes::FuncCall;
use types_nodes::{CoercionForm, FuncExpr, Node, NodeList, NodeTag};

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
    _last_srf: Option<Node<'mcx>>,
    location: ParseLoc,
) -> PgResult<Node<'mcx>> {
    debug_assert_eq!(fargs.len(), actual_arg_types.len());
    let mut buf = [""; 4];
    let parts = name_parts(funcname, &mut buf);

    if fn_call.agg_filter.is_some() {
        panic!(
            "ParseFuncOrColumn (parse_func.c): FILTER needs transformWhereClause \
             EXPR_KIND_FILTER lane — unit backend-parser-func"
        );
    }
    let over = fn_call.over;

    if fargs.len() > FUNC_MAX_ARGS {
        return Err(too_many_arguments(pstate, location));
    }

    let fdresult = func_get_detail(
        mcx,
        parts,
        &fargs,
        fargs.len() as i16,
        actual_arg_types,
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
        FuncDetail::Multiple => Err(ambiguous_function(pstate, parts, actual_arg_types, location)),
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
            if nvargs > 0 && vatype != types_core::catalog::ANYOID {
                panic!(
                    "ParseFuncOrColumn (parse_func.c): implicit variadic array \
                     (ArrayExpr) unported — function {funcid}"
                );
            }
            if !fargs.is_nil() && vatype == types_core::catalog::ANYOID && func_variadic {
                let va_arr_typid = actual_arg_types[actual_arg_types.len() - 1];
                if !OidIsValid(lsyscache::get_base_element_type(va_arr_typid)?) {
                    let encoding = mbutils::GetDatabaseEncoding();
                    return Err(Box::new(
                        ereport(ERROR)
                            .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                            .errmsg("VARIADIC argument must be an array".to_string())
                            .errposition(parser_errposition(pstate, location, encoding))
                            .into_error()
                            .with_error_location(ErrorLocation::new(
                                "parse_func.c",
                                0,
                                "ParseFuncOrColumn",
                            )),
                    ));
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
                    mcx, pstate, funcid, rettype, retset, fargs, true, fn_call, parts,
                    over_node, _last_srf, location,
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

            let mut aggref = Node::build::<Aggref>(mcx)?;
            aggref.aggfnoid = funcid;
            aggref.aggtype = rettype;
            aggref.aggkind = aggkind;
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
            format!("{} is a procedure", func_signature_string(parts, actual_arg_types)?),
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
                mcx, pstate, funcid, rettype, retset, fargs, false, fn_call, parts, over_node,
                _last_srf, location,
            )
        }
        FuncDetail::NotFound => Err(undefined_function(
            pstate,
            parts,
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
    debug_assert!(fn_call.agg_filter.is_none(), "FILTER is a loud lane upstream");
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
    wfunc.aggfilter = None;
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

fn func_get_detail<'mcx>(
    mcx: Mcx<'mcx>,
    parts: &[&str],
    fargs: &NodeList<'mcx>,
    nargs: i16,
    argtypes: &[Oid],
    expand_variadic: bool,
) -> PgResult<FuncDetail<'mcx>> {
    let candidates = FuncnameGetCandidates(mcx, parts, nargs, expand_variadic, true)?;

    let mut best: Option<&FuncCandidate<'_>> = None;
    for cand in candidates.iter() {
        // C: memcmp over nargs entries (variadic/default tails excluded).
        if cand.args.as_slice()[..argtypes.len().min(cand.args.len())] == *argtypes {
            best = Some(cand);
            break;
        }
    }

    if best.is_none() {
        if nargs == 1 && !fargs.is_nil() {
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
    let funcid = best.oid;
    let declared_arg_types = mcx::slice_in(mcx, best.args.as_slice())?;

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
        // Defaults attach to the trailing parameters: take the list tail.
        let skip = list.len() - best.ndargs as usize;
        for (i, cell) in list.iter().enumerate() {
            if i >= skip {
                argdefaults.push(cell);
            }
        }
    }
    Ok(match shape.prokind {
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
    })
}

// check_srf_call_placement, FROM_FUNCTION arm only: every other expr kind's
// SRF handling belongs to the ProjectSet/targetlist-SRF lane and stays loud.
// DIVERGENCE: the nested-SRF errposition points at this call, not the inner
// SRF (exprLocation walker unported).
fn check_srf_call_placement(
    pstate: &ParseState<'_, '_>,
    last_srf: Option<Node<'_>>,
    location: ParseLoc,
) -> PgResult<()> {
    use parser_small1::ParseExprKind;
    match pstate.p_expr_kind {
        ParseExprKind::EXPR_KIND_FROM_FUNCTION => {
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
            Ok(())
        }
        other => panic!(
            "check_srf_call_placement (parse_func.c): SRF in {other:?} — only the \
             FROM_FUNCTION arm is ported (targetlist SRFs are the ProjectSet lane)"
        ),
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

fn func_signature_string(parts: &[&str], argtypes: &[Oid]) -> PgResult<String> {
    let mut sig = name_list_to_string(parts);
    sig.push('(');
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push_str(", ");
        }
        sig.push_str(&format_type::format_type_be(t)?);
    }
    sig.push(')');
    Ok(sig)
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
    argtypes: &[Oid],
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argtypes) {
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
    argtypes: &[Oid],
    misplaced_order_by: bool,
    location: ParseLoc,
) -> Box<PgError> {
    let sig = match func_signature_string(parts, argtypes) {
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
