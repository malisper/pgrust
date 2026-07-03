#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use catalog_namespace::FuncnameGetCandidates;
use elog::ereport;
use mcx::Mcx;
use parser_small1::{parser_errposition, ParseState};
use types_core::{InvalidOid, Oid, OidIsValid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::primnodes::{Aggref, AGGKIND_HYPOTHETICAL, AGGKIND_ORDERED_SET};
use types_nodes::rawnodes::FuncCall;
use types_nodes::{FuncExpr, Node, NodeList};

const FUNC_MAX_ARGS: usize = 100;

const PROKIND_AGGREGATE: i8 = b'a' as i8;
const PROKIND_FUNCTION: i8 = b'f' as i8;
const PROKIND_PROCEDURE: i8 = b'p' as i8;
const PROKIND_WINDOW: i8 = b'w' as i8;

enum FuncDetail {
    Normal { funcid: Oid, rettype: Oid, retset: bool },
    Aggregate { funcid: Oid, rettype: Oid, retset: bool },
    Procedure { funcid: Oid },
    WindowFunc { funcid: Oid },
    NotFound,
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
    if fn_call.over.is_some() {
        panic!(
            "ParseFuncOrColumn (parse_func.c): OVER needs transformWindowFuncCall \
             (parse_agg.c) — backend-parser window lane"
        );
    }

    if fargs.len() > FUNC_MAX_ARGS {
        return Err(too_many_arguments(pstate, location));
    }

    let fdresult = func_get_detail(mcx, parts, fargs.len() as i16, actual_arg_types)?;

    match fdresult {
        FuncDetail::Normal { funcid, rettype, retset } => {
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

            check_exact_arg_types(funcid, actual_arg_types, rettype);
            if retset {
                panic!(
                    "ParseFuncOrColumn (parse_func.c): set-returning function needs \
                     check_srf_call_placement — unit backend-parser-func"
                );
            }

            Node::mk(
                mcx,
                FuncExpr {
                    funcid,
                    funcresulttype: rettype,
                    funcretset: retset,
                    funcvariadic: false,
                    funcformat: fn_call.funcformat,
                    funccollid: InvalidOid,
                    inputcollid: InvalidOid,
                    args: fargs,
                    location,
                },
            )
        }
        FuncDetail::Aggregate { funcid, rettype, retset } => {
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

            check_exact_arg_types(funcid, actual_arg_types, rettype);

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
                actual_arg_types,
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
        FuncDetail::WindowFunc { .. } => panic!(
            "ParseFuncOrColumn (parse_func.c): FUNCDETAIL_WINDOWFUNC arm needs \
             transformWindowFuncCall (parse_agg.c) — backend-parser window lane"
        ),
        FuncDetail::NotFound => Err(undefined_function(
            pstate,
            parts,
            actual_arg_types,
            fn_call.agg_order.len() > 1 && !fn_call.agg_within_group,
            location,
        )),
    }
}

// C `func_get_detail` exact-signature slice: candidates exist but no exact
// binary match => the inexact resolution machinery would run; loud instead.
fn func_get_detail(
    mcx: Mcx<'_>,
    parts: &[&str],
    nargs: i16,
    argtypes: &[Oid],
) -> PgResult<FuncDetail> {
    let candidates = FuncnameGetCandidates(mcx, parts, nargs, true, true)?;

    let mut exact = None;
    for cand in candidates.iter() {
        if cand.args.as_slice() == argtypes {
            exact = Some(cand.oid);
            break;
        }
    }

    let Some(funcid) = exact else {
        if !candidates.is_empty() || nargs == 1 {
            panic!(
                "func_get_detail (parse_func.c): inexact function resolution unported \
                 (FuncNameAsType coercion probe / func_match_argtypes / \
                 func_select_candidate) — unit backend-parser-func; function \
                 \"{}\" nargs={nargs} argtypes={argtypes:?}",
                name_list_to_string(parts)
            );
        }
        return Ok(FuncDetail::NotFound);
    };

    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid} (parse_func.c)"));
    if OidIsValid(shape.provariadic) {
        panic!(
            "func_get_detail (parse_func.c): variadic function {funcid} escaped \
             FuncnameGetCandidates' variadic panic"
        );
    }
    Ok(match shape.prokind {
        PROKIND_AGGREGATE => {
            FuncDetail::Aggregate { funcid, rettype: shape.prorettype, retset: shape.proretset }
        }
        PROKIND_FUNCTION => {
            FuncDetail::Normal { funcid, rettype: shape.prorettype, retset: shape.proretset }
        }
        PROKIND_PROCEDURE => FuncDetail::Procedure { funcid },
        PROKIND_WINDOW => FuncDetail::WindowFunc { funcid },
        other => panic!("unrecognized prokind: {other} (parse_func.c func_get_detail)"),
    })
}

// enforce_generic_type_consistency + make_fn_arguments on the exact-match
// path: declared == actual by construction, so both reduce to the
// no-polymorphic assertion (parse_oper::make_op precedent).
fn check_exact_arg_types(funcid: Oid, actual_arg_types: &[Oid], rettype: Oid) {
    let mut declared: [Oid; FUNC_MAX_ARGS] = [InvalidOid; FUNC_MAX_ARGS];
    let n = actual_arg_types.len();
    declared[..n].copy_from_slice(actual_arg_types);
    let out = coerce::enforce_generic_type_consistency(
        actual_arg_types,
        &mut declared[..n],
        rettype,
        false,
    );
    if out != rettype || declared[..n] != *actual_arg_types {
        panic!(
            "make_fn_arguments (parse_func.c): coercion for exact-match function {funcid} \
             should be a no-op"
        );
    }
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
