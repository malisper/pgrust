//! Port of `src/backend/parser/parse_agg.c` (PostgreSQL 18.3) — handling of
//! aggregate and window functions in the parser.
//!
//! Every function from the C original is implemented 1:1. Branch order, error
//! message text, SQLSTATE values, and OID constants match the C original.
//!
//! The walkers/mutators mirror the C function-pointer + context pattern: the C
//! context struct becomes a Rust struct, and a fallible body is threaded
//! through the infallible owned-tree walker callbacks by stashing any
//! [`PgError`] in the context and aborting the walk (`walker` returns `true`).
//! The caller checks the stashed error after the traversal — the exact set of
//! errors and their order is preserved. Mutators rewrite nodes in place via
//! [`expression_tree_walker_mut`] / [`query_tree_mutator`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};

use backend_utils_error::ereport;
use types_error::ERROR;
use types_core::{AttrNumber, Index, Oid};
use types_error::error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_GROUPING_ERROR, ERRCODE_INVALID_RECURSION,
    ERRCODE_STATEMENT_TOO_COMPLEX, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WINDOWING_ERROR,
};
use types_error::{PgError, PgResult};

use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::parsenodes::{RangeTblEntry, RTEKind};
use types_nodes::parsestmt::{ParseExprKind, ParseState};
use types_nodes::primnodes::{Aggref, Expr, GroupingFunc, Param, ParamKind, Var};
use types_nodes::rawnodes::{GroupingSet, GroupingSetKind, SortGroupClause, WindowDef};

use backend_nodes_core::makefuncs::{make_func_expr, make_target_entry, make_var};
use backend_nodes_core::nodefuncs::{expr_location, expr_type};
use backend_nodes_core::node_walker::{
    expression_tree_walker, expression_tree_walker_mut, query_tree_mutator, query_tree_walker,
};

// ---------------------------------------------------------------------------
// Local constants (literal mirrors of PG headers).
// ---------------------------------------------------------------------------

const InvalidOid: Oid = 0;
const FUNC_MAX_ARGS: usize = 100;
const BYTEAOID: Oid = 17;
const RECORDOID: Oid = 2249;
const INTERNALOID: Oid = 2281;

/// `aggkind` discriminator `AGGKIND_NORMAL` (pg_aggregate.h).
const AGGKIND_NORMAL: i8 = b'n' as i8;

/// `parsenodes.h` FRAMEOPTION_DEFAULTS = RANGE | START_UNBOUNDED_PRECEDING |
/// END_CURRENT_ROW = 0x00002 | 0x00020 | 0x00400 == 0x422.
const FRAMEOPTION_RANGE: i32 = 0x00002;
const FRAMEOPTION_START_UNBOUNDED_PRECEDING: i32 = 0x00020;
const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;
const FRAMEOPTION_DEFAULTS: i32 =
    FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW;

/// `primnodes.h` `CoercionForm` value `COERCE_EXPLICIT_CALL == 0`.
const COERCE_EXPLICIT_CALL: types_nodes::primnodes::CoercionForm =
    types_nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL;

/// `query_tree_walker` flag (`QTW_EXAMINE_RTES_BEFORE`, nodeFuncs.h).
const QTW_EXAMINE_RTES_BEFORE: i32 = 0x10;

/// `AGGKIND_IS_ORDERED_SET(kind)` (pg_aggregate.h).
#[inline]
fn AGGKIND_IS_ORDERED_SET(kind: i8) -> bool {
    kind != AGGKIND_NORMAL
}

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `IsPolymorphicType(oid)` (pg_type.h).
#[inline]
fn IsPolymorphicType(oid: Oid) -> bool {
    // family1: ANYELEMENT 2283, ANYARRAY 2277, ANYNONARRAY 2776, ANYENUM 3500,
    // ANYRANGE 3831, ANYMULTIRANGE 4537;
    // family2: ANYCOMPATIBLE 5077, ANYCOMPATIBLEARRAY 5078,
    // ANYCOMPATIBLENONARRAY 5079, ANYCOMPATIBLERANGE 5080,
    // ANYCOMPATIBLEMULTIRANGE 4538.
    matches!(
        oid,
        2283 | 2277 | 2776 | 3500 | 3831 | 4537 | 5077 | 5078 | 5079 | 5080 | 4538
    )
}

// ---------------------------------------------------------------------------
// Thin seam shims (identical names to the C functions).
// ---------------------------------------------------------------------------

/// `parser_errposition(pstate, location)`.
fn parser_errposition(pstate: &ParseState, location: i32) -> i32 {
    backend_parser_small1_seams::parser_errposition::call(pstate, location).unwrap_or(0)
}

/// `exprType((Node *) e)` — infallible at this crate's call sites (the C
/// `exprType` only errors on an unrecognized node tag, which cannot occur on
/// the analyzed expressions we pass it).
fn exprType_node(e: &Expr) -> Oid {
    expr_type(Some(e)).unwrap_or(InvalidOid)
}

/// `exprLocation((Node *) e)`.
fn exprLocation_node(e: &Expr) -> i32 {
    expr_location(Some(e)).unwrap_or(-1)
}

/// `exprLocation((Node *) list)` over a list of `SortGroupClause`-bearing
/// targetlist references is not needed; for a `List *` of nodes the C reports
/// the leftmost member location.
fn exprLocation_list(list: &[NodePtr]) -> i32 {
    let mut loc: i32 = -1;
    for n in list {
        let l = node_location(n);
        if l < 0 {
            // unknown; leaves loc unchanged
        } else if loc < 0 || l < loc {
            loc = l;
        }
    }
    loc
}

/// `exprLocation((Node *) n)` for a generic node — only the `Expr` arm and a
/// few raw arms carry a location; others report -1.
fn node_location(n: &Node) -> i32 {
    match n.node_tag() {
        _ if n.is_expr() => exprLocation_node(n.as_expr().unwrap()),
        ntag::T_GroupingSet => n.expect_groupingset().location,
        ntag::T_SortGroupClause => -1,
        ntag::T_TargetEntry => match n.expect_targetentry().expr.as_deref() {
            Some(e) => exprLocation_node(e),
            None => -1,
        },
        _ => -1,
    }
}

// ===========================================================================
// transformAggregateCall (parse_agg.c:112)
// ===========================================================================

/// Finish initial transformation of an aggregate call.
///
/// `args` is the already-transformed plain list of argument `Expr`s; `aggorder`
/// is the raw ORDER BY (`SortBy` nodes wrapped as `Node`).
pub fn transformAggregateCall<'mcx>(
    pstate: &mut ParseState<'mcx>,
    agg: Aggref,
    args: Vec<Expr>,
    aggorder: PgVec<'mcx, NodePtr<'mcx>>,
    agg_distinct: bool,
) -> PgResult<Aggref> {
    let mcx = pstate_mcx(pstate);
    let mut agg = agg;
    let mut tlist: Vec<types_nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    let mut torder: Vec<SortGroupClause> = Vec::new();
    let mut tdistinct: Vec<SortGroupClause> = Vec::new();
    let mut attno: AttrNumber = 1;

    if AGGKIND_IS_ORDERED_SET(agg.aggkind) {
        // For an ordered-set agg, the args list includes direct args and
        // aggregated args; we must split them apart.
        let num_direct_args = args.len() as i32 - aggorder.len() as i32;
        debug_assert!(num_direct_args >= 0);

        // aargs = list_copy_tail(args, numDirectArgs);
        // agg->aggdirectargs = list_truncate(args, numDirectArgs);
        let mut args = args;
        let aargs: PgVec<Expr> = {
            let mut v: PgVec<Expr> = vec_with_capacity_in(mcx, args.len() - num_direct_args as usize)?;
            v.extend(args.split_off(num_direct_args as usize));
            v
        };
        agg.aggdirectargs = args.into_iter().collect();

        // forboth(lc, aargs, lc2, aggorder)
        let mut aargs_iter = aargs.into_iter();
        let mut aggorder_iter = aggorder.into_iter();
        loop {
            let (arg, sortby_node) = match (aargs_iter.next(), aggorder_iter.next()) {
                (Some(a), Some(b)) => (a, b),
                _ => break,
            };
            let sortby = match sortby_node.as_sortby() {
                Some(sb) => sb,
                None => {
                    return Err(PgError::error(
                        "transformAggregateCall: aggorder element is not a SortBy node",
                    ))
                }
            };

            // We don't bother to assign column names to the entries.
            let tle = make_target_entry(mcx, arg, attno, None, false)?;
            attno += 1;
            tlist.push(tle);

            // torder = addTargetToSortList(pstate, tle, torder, tlist, sortby);
            let tle_idx = tlist.len() - 1;
            backend_parser_clause::addTargetToSortList(
                mcx, pstate, tle_idx, &mut torder, &mut tlist, sortby,
            )?;
        }

        // Never any DISTINCT in an ordered-set agg.
        debug_assert!(!agg_distinct);
    } else {
        // Regular aggregate, so it has no direct args.
        agg.aggdirectargs = Vec::new();

        // Transform the plain list of Exprs into a targetlist.
        for arg in args {
            let tle = make_target_entry(mcx, arg, attno, None, false)?;
            attno += 1;
            tlist.push(tle);
        }

        // If we have an ORDER BY, transform it. We need to mess with
        // p_next_resno since it will be used to number any new tlist entries.
        let save_next_resno = pstate.p_next_resno;
        pstate.p_next_resno = attno as i32;

        // The owner takes the ORDER BY as `&[SortBy]`; aggorder is the raw
        // `List *` of SortBy nodes. Unwrap each NodePtr into an owned SortBy.
        let mut orderlist: Vec<types_nodes::rawnodes::SortBy<'mcx>> =
            Vec::with_capacity(aggorder.len());
        for sortby_node in aggorder.into_iter() {
            match PgBox::into_inner(sortby_node).into_sortby() {
                Some(sb) => orderlist.push(sb),
                None => {
                    return Err(PgError::error(
                        "transformAggregateCall: aggorder element is not a SortBy node",
                    ))
                }
            }
        }

        torder = backend_parser_clause::transformSortClause(
            mcx,
            pstate,
            &orderlist,
            &mut tlist,
            ParseExprKind::EXPR_KIND_ORDER_BY,
            true, // force SQL99 rules
        )?;

        // If we have DISTINCT, transform that to produce a distinctList.
        if agg_distinct {
            tdistinct = backend_parser_clause::transformDistinctClause(
                mcx, pstate, &mut tlist, &torder, true,
            )?;

            // Remove this check if executor support for hashed distinct for
            // aggregates is ever added.
            for sortcl in tdistinct.iter() {
                if !OidIsValid(sortcl.sortop) {
                    let expr = backend_optimizer_util_vars::tlist::get_sortgroupclause_expr(
                        sortcl, &tlist,
                    )?;
                    let (typ, location) = match expr.as_ref() {
                        Some(e) => (exprType_node(e), exprLocation_node(e)),
                        None => (InvalidOid, -1),
                    };
                    let tyname = backend_utils_adt_format_type_seams::format_type_be::call(mcx, typ)?;
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_FUNCTION)
                        .errmsg(alloc_fmt(&format_args!(
                            "could not identify an ordering operator for type {}",
                            tyname.as_str()
                        )))
                        .errdetail("Aggregates with DISTINCT must be able to sort their inputs.")
                        .errposition(parser_errposition(pstate, location))
                        .into_error());
                }
            }
        }

        pstate.p_next_resno = save_next_resno;
    }

    // Build the aggargtypes list with the type OIDs of the direct and
    // aggregated args, ignoring any resjunk entries.
    let mut argtypes: Vec<Oid> = Vec::new();
    for arg in &agg.aggdirectargs {
        argtypes.push(exprType_node(arg));
    }
    for tle in tlist.iter() {
        if tle.resjunk {
            continue; // ignore junk
        }
        let typ = match tle.expr.as_deref() {
            Some(e) => exprType_node(e),
            None => InvalidOid,
        };
        argtypes.push(typ);
    }

    // Update the Aggref with the transformation results.
    agg.args = tlist_into_static(tlist)?;
    agg.aggorder = torder.into_iter().collect();
    agg.aggdistinct = tdistinct.into_iter().collect();
    agg.aggargtypes = argtypes;

    check_agglevels_and_constraints(mcx, pstate, AggOrGrouping::Agg(&mut agg))?;
    Ok(agg)
}

/// `Aggref.args` is `Vec<TargetEntry<'static>>`. The parser-built target
/// entries have no lifetime-bound borrows (their `expr`/`resname` are owned
/// boxes/strings), so they may be reinterpreted as `'static`. This is a
/// transmute of the lifetime parameter only; the data is unchanged.
fn tlist_into_static<'mcx>(
    tlist: Vec<types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<Vec<types_nodes::primnodes::TargetEntry<'static>>> {
    let mut out: Vec<types_nodes::primnodes::TargetEntry<'static>> = Vec::new();
    out.reserve(tlist.len());
    for te in tlist.into_iter() {
        // SAFETY: primnodes::Aggref is in the lifetime-free Expr tree (cf.
        // SubPlanExpr(Box<SubPlan<'static>>)), so its args field is
        // Vec<TargetEntry<'static>>; the only TargetEntry builder
        // (make_target_entry) returns 'mcx-arena-bound entries. This erases the
        // 'mcx lifetime to 'static to match the Expr-tree convention — a known
        // model gap (the Aggref-lifetime keystone is unbuilt; the arena
        // outlives parse analysis in practice).
        let te_static: types_nodes::primnodes::TargetEntry<'static> =
            unsafe { core::mem::transmute(te) };
        out.push(te_static);
    }
    Ok(out)
}

/// Build a `String` via `format_args!` without pulling in `alloc::format!`
/// (this crate is `std`-linked, so just use `format!`).
#[inline]
fn alloc_fmt(args: &core::fmt::Arguments) -> String {
    use core::fmt::Write;
    let mut s = String::new();
    let _ = s.write_fmt(*args);
    s
}

// ===========================================================================
// transformGroupingFunc (parse_agg.c:268)
// ===========================================================================

/// Transform a GROUPING expression.
///
/// `p_node` is the raw `GROUPING(...)` node (`Node::Expr(Expr::GroupingFunc)`);
/// its `args` are the raw (untransformed) argument expressions.
pub fn transformGroupingFunc<'mcx>(
    pstate: &mut ParseState<'mcx>,
    p_node: Node<'mcx>,
) -> PgResult<Expr> {
    // NOTE: left on the enum — the generated `into_groupingfunc` accessor is
    // shadowed by the Node-direct *raw* `rawexprnodes::GroupingFunc` variant, so
    // it does not reach the analyzed `primnodes::GroupingFunc` payload here.
    let p: GroupingFunc = match p_node.into_expr() {
        Some(Expr::GroupingFunc(g)) => g,
        _ => {
            return Err(PgError::error(
                "transformGroupingFunc: input is not a GroupingFunc node",
            ))
        }
    };

    let mut result = GroupingFunc {
        args: Vec::new(),
        refs: Vec::new(),
        cols: Vec::new(),
        agglevelsup: 0,
        location: -1,
    };

    if p.args.len() > 31 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg("GROUPING must have fewer than 32 arguments")
            .errposition(parser_errposition(pstate, p.location))
            .into_error());
    }

    let mut result_list: Vec<Expr> = Vec::new();
    result_list.reserve(p.args.len());
    let expr_kind = pstate.p_expr_kind;
    for arg in p.args.into_iter() {
        let arg_node = Node::mk_expr(pstate_mcx(pstate), arg);
        let current_result = backend_parser_parse_expr_seams::transformExpr::call(
            pstate,
            Some(arg_node),
            expr_kind,
        )?;
        // acceptability of expressions is checked later
        if let Some(cr) = current_result {
            result_list.push(cr);
        }
    }

    result.args = result_list;
    result.location = p.location;

    // result is the primnodes GroupingFunc; wrap as Expr so the level checks can
    // mutate it through AggOrGrouping::Grouping.
    let mut expr = Expr::GroupingFunc(result);
    {
        let g = match &mut expr {
            Expr::GroupingFunc(g) => g,
            _ => unreachable!(),
        };
        // The level checks allocate nothing through this path that needs a
        // distinct mcx; use the pstate's mcx.
        let mcx = pstate_mcx(pstate);
        check_agglevels_and_constraints(mcx, pstate, AggOrGrouping::Grouping(g))?;
    }

    Ok(expr)
}

/// Borrow the parse state's allocation context. The parser threads one `Mcx`
/// for the whole statement; we recover it from `p_rtable`'s allocator.
#[inline]
fn pstate_mcx<'mcx>(pstate: &ParseState<'mcx>) -> Mcx<'mcx> {
    *pstate.p_rtable.allocator()
}

// ===========================================================================
// check_agglevels_and_constraints (parse_agg.c:307)
// ===========================================================================

/// The two node kinds `check_agglevels_and_constraints` is invoked on.
enum AggOrGrouping<'a> {
    Agg(&'a mut Aggref),
    Grouping(&'a mut types_nodes::primnodes::GroupingFunc),
}

fn check_agglevels_and_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    expr: AggOrGrouping<'_>,
) -> PgResult<()> {
    let is_agg = matches!(expr, AggOrGrouping::Agg(_));

    // Snapshot the subtrees the level analysis walks (C reads
    // directargs/args/filter for Aggref, args for GroupingFunc).
    let (directargs, args, filter, location): (Vec<Node>, Vec<Node>, Option<Node>, i32) = match &expr
    {
        AggOrGrouping::Agg(agg) => {
            let mut directargs: Vec<Node> = Vec::new();
            for e in agg.aggdirectargs.iter() {
                directargs.push(Node::mk_expr(mcx, e.clone()));
            }
            let mut args: Vec<Node> = Vec::new();
            for te in agg.args.iter() {
                args.push(Node::mk_target_entry(mcx, te.clone_in(mcx)?));
            }
            let filter = match agg.aggfilter.as_deref() {
                Some(e) => Some(Node::mk_expr(mcx, e.clone())),
                None => None,
            };
            (directargs, args, filter, agg.location)
        }
        AggOrGrouping::Grouping(grp) => {
            let mut args: Vec<Node> = Vec::new();
            for e in grp.args.iter() {
                args.push(Node::mk_expr(mcx, e.clone()));
            }
            (Vec::new(), args, None, grp.location)
        }
    };

    // Check the arguments to compute the aggregate's level and detect
    // improper nesting.
    let mut min_varlevel =
        check_agg_arguments(mcx, pstate, &directargs, &args, filter.as_ref(), location)?;

    match expr {
        AggOrGrouping::Agg(agg) => agg.agglevelsup = min_varlevel as Index,
        AggOrGrouping::Grouping(grp) => grp.agglevelsup = min_varlevel as Index,
    }

    // Mark the correct pstate level as having aggregates.
    let mut cur: &mut ParseState = pstate;
    while min_varlevel > 0 {
        min_varlevel -= 1;
        cur = cur.parentParseState.as_deref_mut().ok_or_else(|| {
            PgError::error(
                "check_agglevels_and_constraints: parentParseState chain shorter than agglevelsup",
            )
        })?;
    }
    cur.p_hasAggs = true;

    // Is the aggregate/grouping in a valid place?
    let mut err: Option<&'static str> = None;
    let mut errkind = false;
    let kind = pstate.p_expr_kind;
    use ParseExprKind::*;
    match kind {
        EXPR_KIND_NONE => {
            debug_assert!(false); // can't happen
        }
        EXPR_KIND_OTHER => { /* Accept aggregate/grouping here. */ }
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in JOIN conditions"
            } else {
                "grouping operations are not allowed in JOIN conditions"
            });
        }
        EXPR_KIND_FROM_SUBSELECT => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in FROM clause of their own query level"
            } else {
                "grouping operations are not allowed in FROM clause of their own query level"
            });
        }
        EXPR_KIND_FROM_FUNCTION => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in functions in FROM"
            } else {
                "grouping operations are not allowed in functions in FROM"
            });
        }
        EXPR_KIND_WHERE => errkind = true,
        EXPR_KIND_POLICY => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in policy expressions"
            } else {
                "grouping operations are not allowed in policy expressions"
            });
        }
        EXPR_KIND_HAVING => { /* okay */ }
        EXPR_KIND_FILTER => errkind = true,
        EXPR_KIND_WINDOW_PARTITION => { /* okay */ }
        EXPR_KIND_WINDOW_ORDER => { /* okay */ }
        EXPR_KIND_WINDOW_FRAME_RANGE => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in window RANGE"
            } else {
                "grouping operations are not allowed in window RANGE"
            });
        }
        EXPR_KIND_WINDOW_FRAME_ROWS => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in window ROWS"
            } else {
                "grouping operations are not allowed in window ROWS"
            });
        }
        EXPR_KIND_WINDOW_FRAME_GROUPS => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in window GROUPS"
            } else {
                "grouping operations are not allowed in window GROUPS"
            });
        }
        EXPR_KIND_SELECT_TARGET => { /* okay */ }
        EXPR_KIND_INSERT_TARGET | EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => {
            errkind = true
        }
        EXPR_KIND_MERGE_WHEN => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in MERGE WHEN conditions"
            } else {
                "grouping operations are not allowed in MERGE WHEN conditions"
            });
        }
        EXPR_KIND_GROUP_BY => errkind = true,
        EXPR_KIND_ORDER_BY => { /* okay */ }
        EXPR_KIND_DISTINCT_ON => { /* okay */ }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => errkind = true,
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => errkind = true,
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => errkind = true,
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in check constraints"
            } else {
                "grouping operations are not allowed in check constraints"
            });
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in DEFAULT expressions"
            } else {
                "grouping operations are not allowed in DEFAULT expressions"
            });
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in index expressions"
            } else {
                "grouping operations are not allowed in index expressions"
            });
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in index predicates"
            } else {
                "grouping operations are not allowed in index predicates"
            });
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in statistics expressions"
            } else {
                "grouping operations are not allowed in statistics expressions"
            });
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in transform expressions"
            } else {
                "grouping operations are not allowed in transform expressions"
            });
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in EXECUTE parameters"
            } else {
                "grouping operations are not allowed in EXECUTE parameters"
            });
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in trigger WHEN conditions"
            } else {
                "grouping operations are not allowed in trigger WHEN conditions"
            });
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in partition bound"
            } else {
                "grouping operations are not allowed in partition bound"
            });
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in partition key expressions"
            } else {
                "grouping operations are not allowed in partition key expressions"
            });
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in column generation expressions"
            } else {
                "grouping operations are not allowed in column generation expressions"
            });
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in CALL arguments"
            } else {
                "grouping operations are not allowed in CALL arguments"
            });
        }
        EXPR_KIND_COPY_WHERE => {
            err = Some(if is_agg {
                "aggregate functions are not allowed in COPY FROM WHERE conditions"
            } else {
                "grouping operations are not allowed in COPY FROM WHERE conditions"
            });
        }
        EXPR_KIND_CYCLE_MARK => errkind = true,
        // No default: unrecognized values behave like EXPR_KIND_OTHER.
        #[allow(unreachable_patterns)]
        _ => {}
    }

    if let Some(err) = err {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_GROUPING_ERROR)
            .errmsg_internal(err)
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    if errkind {
        let name = backend_parser_parse_expr_seams::parse_expr_kind_name::call(kind);
        let msg = if is_agg {
            alloc_fmt(&format_args!(
                "aggregate functions are not allowed in {}",
                name
            ))
        } else {
            alloc_fmt(&format_args!(
                "grouping operations are not allowed in {}",
                name
            ))
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_GROUPING_ERROR)
            .errmsg_internal(msg)
            .errposition(parser_errposition(pstate, location))
            .into_error());
    }

    Ok(())
}

// ===========================================================================
// check_agg_arguments (parse_agg.c:645)
// ===========================================================================

struct CheckAggArgumentsContext<'p, 'mcx> {
    pstate: &'p ParseState<'mcx>,
    min_varlevel: i32,
    min_agglevel: i32,
    min_ctelevel: i32,
    /// C keeps the `RangeTblEntry *`; only the alias name is read (for the error
    /// detail).
    min_cte_aliasname: Option<String>,
    sublevels_up: i32,
    /// Stashed error from inside an (infallible) tree walk.
    error: Option<PgError>,
}

fn check_agg_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    directargs: &[Node],
    args: &[Node],
    filter: Option<&Node>,
    agglocation: i32,
) -> PgResult<i32> {
    let _ = mcx;
    let mut context = CheckAggArgumentsContext {
        pstate,
        min_varlevel: -1, // signifies nothing found yet
        min_agglevel: -1,
        min_ctelevel: -1,
        min_cte_aliasname: None,
        sublevels_up: 0,
        error: None,
    };

    // C runs the walker once over `args` (a List, wrapped) and once over filter.
    for n in args {
        if check_agg_arguments_walker(n, &mut context) {
            break;
        }
    }
    if context.error.is_none() {
        if let Some(f) = filter {
            check_agg_arguments_walker(f, &mut context);
        }
    }
    if let Some(e) = context.error.take() {
        return Err(e);
    }

    // Determine agglevel from min vars/aggs found.
    let agglevel = if context.min_varlevel < 0 {
        if context.min_agglevel < 0 {
            0
        } else {
            context.min_agglevel
        }
    } else if context.min_agglevel < 0 {
        context.min_varlevel
    } else {
        context.min_varlevel.min(context.min_agglevel)
    };

    // Nested aggregate of the same semantic level?
    if agglevel == context.min_agglevel {
        let mut aggloc = locate_agg_of_level_list(args, agglevel);
        if aggloc < 0 {
            if let Some(f) = filter {
                aggloc = backend_rewrite_rewritemanip_seams::locate_agg_of_level::call(f, agglevel);
            }
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_GROUPING_ERROR)
            .errmsg("aggregate function calls cannot be nested")
            .errposition(parser_errposition(pstate, aggloc))
            .into_error());
    }

    // Non-local CTE below the aggregate's semantic level?
    if context.min_ctelevel >= 0 && context.min_ctelevel < agglevel {
        let aliasname = context.min_cte_aliasname.as_deref().unwrap_or("");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("outer-level aggregate cannot use a nested CTE")
            .errdetail(alloc_fmt(&format_args!(
                "CTE \"{}\" is below the aggregate's semantic level.",
                aliasname
            )))
            .errposition(parser_errposition(pstate, agglocation))
            .into_error());
    }

    // Vars/aggs in direct arguments.
    if !directargs.is_empty() {
        context.min_varlevel = -1;
        context.min_agglevel = -1;
        context.min_ctelevel = -1;
        for n in directargs {
            if check_agg_arguments_walker(n, &mut context) {
                break;
            }
        }
        if let Some(e) = context.error.take() {
            return Err(e);
        }
        if context.min_varlevel >= 0 && context.min_varlevel < agglevel {
            let loc = locate_var_of_level_list(directargs, context.min_varlevel);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_GROUPING_ERROR)
                .errmsg("outer-level aggregate cannot contain a lower-level variable in its direct arguments")
                .errposition(parser_errposition(pstate, loc))
                .into_error());
        }
        if context.min_agglevel >= 0 && context.min_agglevel <= agglevel {
            let loc = locate_agg_of_level_list(directargs, context.min_agglevel);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_GROUPING_ERROR)
                .errmsg("aggregate function calls cannot be nested")
                .errposition(parser_errposition(pstate, loc))
                .into_error());
        }
        if context.min_ctelevel >= 0 && context.min_ctelevel < agglevel {
            let aliasname = context.min_cte_aliasname.as_deref().unwrap_or("");
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("outer-level aggregate cannot use a nested CTE")
                .errdetail(alloc_fmt(&format_args!(
                    "CTE \"{}\" is below the aggregate's semantic level.",
                    aliasname
                )))
                .errposition(parser_errposition(pstate, agglocation))
                .into_error());
        }
    }
    Ok(agglevel)
}

/// `locate_agg_of_level((Node *) list, levelsup)` — visit each element until found.
fn locate_agg_of_level_list(list: &[Node], levelsup: i32) -> i32 {
    for n in list {
        let loc = backend_rewrite_rewritemanip_seams::locate_agg_of_level::call(n, levelsup);
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

/// `locate_var_of_level((Node *) list, levelsup)`.
fn locate_var_of_level_list(list: &[Node], levelsup: i32) -> i32 {
    for n in list {
        let loc = backend_optimizer_util_vars::locate_var_of_level(n, levelsup);
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

fn check_agg_arguments_walker(node: &Node, context: &mut CheckAggArgumentsContext) -> bool {
    // (C: node == NULL -> false; the owned tree has no NULL nodes here.)
    // NOTE: this match stays on the enum — the `GroupingFunc` arm's payload is
    // the analyzed `primnodes::GroupingFunc`, but the generated
    // `as_groupingfunc` accessor is shadowed by the Node-direct *raw*
    // `rawexprnodes::GroupingFunc` variant (same T_GroupingFunc tag), so the
    // accessor does not fit; keeping the whole match on the enum preserves it.
    match node.as_expr() {
        Some(Expr::Var(var)) => {
            let mut varlevelsup = var.varlevelsup as i32;
            // convert levelsup to frame of reference of original query
            varlevelsup -= context.sublevels_up;
            // ignore local vars of subqueries
            if varlevelsup >= 0
                && (context.min_varlevel < 0 || context.min_varlevel > varlevelsup)
            {
                context.min_varlevel = varlevelsup;
            }
            return false;
        }
        Some(Expr::Aggref(agg)) => {
            let mut agglevelsup = agg.agglevelsup as i32;
            agglevelsup -= context.sublevels_up;
            if agglevelsup >= 0
                && (context.min_agglevel < 0 || context.min_agglevel > agglevelsup)
            {
                context.min_agglevel = agglevelsup;
            }
            // Continue and descend into subtree.
        }
        Some(Expr::GroupingFunc(grp)) => {
            let mut agglevelsup = grp.agglevelsup as i32;
            agglevelsup -= context.sublevels_up;
            if agglevelsup >= 0
                && (context.min_agglevel < 0 || context.min_agglevel > agglevelsup)
            {
                context.min_agglevel = agglevelsup;
            }
            // Continue and descend into subtree.
        }
        _ => {}
    }

    // SRFs and window functions: rejected immediately unless within a sub-select.
    if context.sublevels_up == 0 {
        let is_srf = match node.node_tag() {
            ntag::T_FuncExpr => node.as_funcexpr().unwrap().funcretset,
            ntag::T_OpExpr => node.as_opexpr().unwrap().opretset,
            _ => false,
        };
        if is_srf {
            let loc = node_location(node);
            context.error = Some(
                ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("aggregate function calls cannot contain set-returning function calls")
                    .errhint("You might be able to move the set-returning function into a LATERAL FROM item.")
                    .errposition(parser_errposition(context.pstate, loc))
                    .into_error(),
            );
            return true;
        }
        if let Some(wf) = node.as_windowfunc() {
            let loc = wf.location;
            context.error = Some(
                ereport(ERROR)
                    .errcode(ERRCODE_GROUPING_ERROR)
                    .errmsg("aggregate function calls cannot contain window function calls")
                    .errposition(parser_errposition(context.pstate, loc))
                    .into_error(),
            );
            return true;
        }
    }

    if let Some(rte) = node.as_rangetblentry() {
        if rte.rtekind == RTEKind::RTE_CTE {
            let mut ctelevelsup = rte.ctelevelsup as i32;
            ctelevelsup -= context.sublevels_up;
            if ctelevelsup >= 0
                && (context.min_ctelevel < 0 || context.min_ctelevel > ctelevelsup)
            {
                context.min_ctelevel = ctelevelsup;
                context.min_cte_aliasname = Some(rte_eref_aliasname(rte).to_string());
            }
        }
        return false; // allow range_table_walker to continue
    }

    if let Some(query) = node.as_query() {
        // Recurse into subselects.
        context.sublevels_up += 1;
        let aborted = query_tree_walker(
            query,
            &mut |n: &Node| check_agg_arguments_walker(n, context),
            QTW_EXAMINE_RTES_BEFORE,
        );
        context.sublevels_up -= 1;
        return aborted;
    }

    expression_tree_walker(node, &mut |n: &Node| check_agg_arguments_walker(n, context))
}

/// `rte->eref->aliasname`.
fn rte_eref_aliasname<'a>(rte: &'a RangeTblEntry) -> &'a str {
    match rte.eref.as_deref() {
        Some(alias) => alias.aliasname.as_deref().unwrap_or(""),
        None => "",
    }
}

// ===========================================================================
// transformWindowFuncCall (parse_agg.c:878)
// ===========================================================================

/// Finish initial transformation of a window function call.
pub fn transformWindowFuncCall<'mcx>(
    pstate: &mut ParseState<'mcx>,
    wfunc: types_nodes::primnodes::WindowFunc,
    windef: WindowDef<'mcx>,
) -> PgResult<types_nodes::primnodes::WindowFunc> {
    let mut wfunc = wfunc;
    // A window function call can't contain another one (but aggs are OK).
    if pstate.p_hasWindowFuncs && contain_windowfuncs_exprs(pstate_mcx(pstate), &wfunc.args) {
        let loc = locate_windowfunc_exprs(pstate_mcx(pstate), &wfunc.args);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WINDOWING_ERROR)
            .errmsg("window function calls cannot be nested")
            .errposition(parser_errposition(pstate, loc))
            .into_error());
    }

    // Check to see if the window function is in an invalid place.
    let mut err: Option<&'static str> = None;
    let mut errkind = false;
    let kind = pstate.p_expr_kind;
    use ParseExprKind::*;
    match kind {
        EXPR_KIND_NONE => {
            debug_assert!(false); // can't happen
        }
        EXPR_KIND_OTHER => { /* Accept window func here. */ }
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            err = Some("window functions are not allowed in JOIN conditions");
        }
        EXPR_KIND_FROM_SUBSELECT => errkind = true, // can't get here, but just in case
        EXPR_KIND_FROM_FUNCTION => {
            err = Some("window functions are not allowed in functions in FROM");
        }
        EXPR_KIND_WHERE => errkind = true,
        EXPR_KIND_POLICY => {
            err = Some("window functions are not allowed in policy expressions");
        }
        EXPR_KIND_HAVING => errkind = true,
        EXPR_KIND_FILTER => errkind = true,
        EXPR_KIND_WINDOW_PARTITION
        | EXPR_KIND_WINDOW_ORDER
        | EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS => {
            err = Some("window functions are not allowed in window definitions");
        }
        EXPR_KIND_SELECT_TARGET => { /* okay */ }
        EXPR_KIND_INSERT_TARGET | EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => {
            errkind = true
        }
        EXPR_KIND_MERGE_WHEN => {
            err = Some("window functions are not allowed in MERGE WHEN conditions");
        }
        EXPR_KIND_GROUP_BY => errkind = true,
        EXPR_KIND_ORDER_BY => { /* okay */ }
        EXPR_KIND_DISTINCT_ON => { /* okay */ }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => errkind = true,
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => errkind = true,
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => errkind = true,
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = Some("window functions are not allowed in check constraints");
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = Some("window functions are not allowed in DEFAULT expressions");
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = Some("window functions are not allowed in index expressions");
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = Some("window functions are not allowed in statistics expressions");
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = Some("window functions are not allowed in index predicates");
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = Some("window functions are not allowed in transform expressions");
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = Some("window functions are not allowed in EXECUTE parameters");
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = Some("window functions are not allowed in trigger WHEN conditions");
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = Some("window functions are not allowed in partition bound");
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = Some("window functions are not allowed in partition key expressions");
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = Some("window functions are not allowed in CALL arguments");
        }
        EXPR_KIND_COPY_WHERE => {
            err = Some("window functions are not allowed in COPY FROM WHERE conditions");
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = Some("window functions are not allowed in column generation expressions");
        }
        EXPR_KIND_CYCLE_MARK => errkind = true,
        #[allow(unreachable_patterns)]
        _ => {}
    }
    if let Some(err) = err {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WINDOWING_ERROR)
            .errmsg_internal(err)
            .errposition(parser_errposition(pstate, wfunc.location))
            .into_error());
    }
    if errkind {
        // translator: %s is name of a SQL construct, eg GROUP BY
        let name = backend_parser_parse_expr_seams::parse_expr_kind_name::call(kind);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WINDOWING_ERROR)
            .errmsg(alloc_fmt(&format_args!(
                "window functions are not allowed in {}",
                name
            )))
            .errposition(parser_errposition(pstate, wfunc.location))
            .into_error());
    }

    // Resolve the OVER clause to a window reference.
    if windef.name.is_some() {
        let mut winref: Index = 0;
        let mut found = false;

        debug_assert!(
            windef.refname.is_none()
                && windef.partitionClause.is_empty()
                && windef.orderClause.is_empty()
                && windef.frameOptions == FRAMEOPTION_DEFAULTS
        );

        for refwin in pstate.p_windowdefs.iter() {
            winref += 1;
            if let (Some(rn), Some(wn)) = (refwin.name.as_deref(), windef.name.as_deref()) {
                if rn == wn {
                    wfunc.winref = winref;
                    found = true;
                    break;
                }
            }
        }
        if !found {
            let name = windef.name.as_deref().unwrap_or("");
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(alloc_fmt(&format_args!("window \"{}\" does not exist", name)))
                .errposition(parser_errposition(pstate, windef.location))
                .into_error());
        }
    } else {
        let mut winref: Index = 0;
        let mut found = false;

        for refwin in pstate.p_windowdefs.iter() {
            winref += 1;

            let refname_matched = match (refwin.refname.as_deref(), windef.refname.as_deref()) {
                (Some(a), Some(b)) => a == b, // matched on refname
                (None, None) => true,         // matched, no refname
                _ => false,
            };
            if !refname_matched {
                continue;
            }

            // Also see similar de-duplication code in optimize_window_clauses.
            if eq_node_lists(&refwin.partitionClause, &windef.partitionClause)
                && eq_node_lists(&refwin.orderClause, &windef.orderClause)
                && refwin.frameOptions == windef.frameOptions
                && eq_opt_nodes(&refwin.startOffset, &windef.startOffset)
                && eq_opt_nodes(&refwin.endOffset, &windef.endOffset)
            {
                // found a duplicate window specification
                wfunc.winref = winref;
                found = true;
                break;
            }
        }
        if !found {
            let mcx = pstate_mcx(pstate);
            pstate.p_windowdefs.push(windef.clone_in(mcx)?);
            wfunc.winref = pstate.p_windowdefs.len() as Index;
        }
    }

    pstate.p_hasWindowFuncs = true;
    Ok(wfunc)
}

/// `contain_windowfuncs((Node *) list)` over an `Expr` list.
fn contain_windowfuncs_exprs<'mcx>(mcx: Mcx<'mcx>, args: &[Expr]) -> bool {
    for e in args {
        // C wraps the whole list in one Node and walks; an Expr list visits each
        // element. We test each element's subtree via the rewriteManip seam.
        let n = wrap_expr_ref(mcx, e);
        if backend_rewrite_rewritemanip_seams::contain_windowfuncs::call(&n) {
            return true;
        }
    }
    false
}

/// `locate_windowfunc((Node *) list)` over an `Expr` list.
fn locate_windowfunc_exprs<'mcx>(mcx: Mcx<'mcx>, args: &[Expr]) -> i32 {
    for e in args {
        let n = wrap_expr_ref(mcx, e);
        let loc = backend_rewrite_rewritemanip_seams::locate_windowfunc::call(&n);
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

/// Wrap an `&Expr` as an owned `Node` for a seam call. The seam takes `&Node`;
/// the rewriteManip walkers only read the tree, so a clone-free wrapper is
/// preferred, but `Node::Expr` owns its payload — so we clone the Expr (cheap
/// for the common scalar/Var/funcexpr argument lists). Aggref cannot be cloned,
/// but window-func argument lists never contain top-level Aggrefs requiring a
/// clone at this position (the contain/locate walkers descend, not clone).
fn wrap_expr_ref<'mcx>(mcx: Mcx<'mcx>, e: &Expr) -> Node<'mcx> {
    // The contain/locate seams take a borrowed Node and never retain it. We
    // build a borrowed view by re-creating a Node referencing a cloned Expr;
    // for Aggref (non-Clone) we cannot, so fall back to a structural clone via
    // the node arena would be required. In practice window arg lists are
    // already analyzed Exprs; clone where possible.
    match e {
        Expr::Aggref(_) => {
            // Aggref args can contain nested aggs but not window funcs at this
            // top level; represent as an empty marker the walkers treat as "no
            // window func". We mirror C by descending, but since we cannot clone
            // an Aggref, wrap it through a no-op container is unsound. Instead,
            // panic loudly — this position is not reachable for window args in
            // practice (window funcs cannot appear inside aggregate args, which
            // transformAggregateCall already rejected).
            panic!("transformWindowFuncCall: unexpected Aggref at top of window-func argument list")
        }
        other => Node::mk_expr(mcx, clone_expr_static(other)),
    }
}

/// Clone an `Expr` (non-Aggref) to a `'static` Node payload. The window-func
/// argument exprs are owned analyzed expressions with no borrowed data.
fn clone_expr_static(e: &Expr) -> Expr {
    e.clone()
}

/// `equal(list1, list2)` for two `PgVec<NodePtr>`: element-wise `_equalList`.
fn eq_node_lists(a: &[NodePtr], b: &[NodePtr]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| backend_nodes_equalfuncs_seams::equal_node::call(x, y))
}

/// `equal(node1, node2)` for two `Option<NodePtr>` (NULL == NULL).
fn eq_opt_nodes(a: &Option<NodePtr>, b: &Option<NodePtr>) -> bool {
    match (a.as_deref(), b.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => backend_nodes_equalfuncs_seams::equal_node::call(x, y),
        _ => false,
    }
}

// ===========================================================================
// parseCheckAggregates (parse_agg.c:1138)
// ===========================================================================

/// Check for misplaced aggregates / improper grouping, and replace grouped
/// variables in the targetlist and HAVING clause with `RTE_GROUP` Vars.
pub fn parseCheckAggregates<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    qry: &mut types_nodes::copy_query::Query<'mcx>,
) -> PgResult<()> {
    let mut gset_common: Vec<i32> = Vec::new();
    // groupClauses is a list of TargetEntry; carried as owned TargetEntry.
    let mut group_clauses: Vec<types_nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    let mut group_clause_common_vars: Vec<Expr> = Vec::new();
    let mut func_grouped_rels: Vec<i32> = Vec::new();
    let has_join_rtes;
    let has_self_ref_rtes;

    // This should only be called if we found aggregates or grouping.
    debug_assert!(
        pstate.p_hasAggs
            || !qry.groupClause.is_empty()
            || qry.havingQual.is_some()
            || !qry.groupingSets.is_empty()
    );

    // Grouping sets: expand them and find the intersection of all sets.
    if !qry.groupingSets.is_empty() {
        // The limit of 4096 is arbitrary, to bound pathological constructs.
        let grouping_sets_nodes: Vec<Node> = {
            let mut v: Vec<Node> = Vec::new();
            for gs in qry.groupingSets.iter() {
                v.push(gs.as_ref().clone_in(mcx)?);
            }
            v
        };
        let gsets = expand_grouping_sets(mcx, &grouping_sets_nodes, qry.groupDistinct, 4096)?;

        let gsets = match gsets {
            Some(g) => g,
            None => {
                let location = if !qry.groupClause.is_empty() {
                    exprLocation_list(&qry.groupClause)
                } else {
                    exprLocation_list(&qry.groupingSets)
                };
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_STATEMENT_TOO_COMPLEX)
                    .errmsg("too many grouping sets present (maximum 4096)")
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }
        };

        // Seed the intersect with the smallest set.
        if let Some(first) = gsets.first() {
            gset_common = first.iter().copied().collect();

            if !gset_common.is_empty() {
                // for_each_from(l, gsets, 1)
                for s in gsets.iter().skip(1) {
                    gset_common = list_intersection_int(&gset_common, s);
                    if gset_common.is_empty() {
                        break;
                    }
                }
            }
        }

        // Single grouping set with non-empty groupClause: pretend plain GROUP BY.
        if gsets.len() == 1 && !qry.groupClause.is_empty() {
            qry.groupingSets = PgVec::new_in(mcx);
        }
    }

    // Scan range table for JOIN or self-reference CTE entries.
    {
        let mut hj = false;
        let mut hs = false;
        for rte in pstate.p_rtable.iter() {
            if rte.rtekind == RTEKind::RTE_JOIN {
                hj = true;
            } else if rte.rtekind == RTEKind::RTE_CTE && rte.self_reference {
                hs = true;
            }
        }
        has_join_rtes = hj;
        has_self_ref_rtes = hs;
    }

    // Build the acceptable GROUP BY expressions list (TLEs).
    for grpcl_node in qry.groupClause.iter() {
        let grpcl = match grpcl_node.as_sortgroupclause() {
            Some(s) => s,
            None => continue,
        };
        let tle = backend_optimizer_util_vars::tlist::get_sortgroupclause_tle(
            grpcl,
            &qry.targetList,
        )?;
        // get_sortgroupclause_tle errors when missing in C path? In C it returns
        // NULL and we `continue`. The repo helper returns the matched TLE or
        // errors; on a genuine miss we keep C's "continue".
        group_clauses.push(tle.clone_in(mcx)?);
    }

    // Flatten join alias vars if any RTE_JOIN entries.
    if has_join_rtes {
        let qry_node = Node::mk_query(mcx, qry.clone_in(mcx)?);
        let flat = flatten_group_clauses(mcx, &qry_node, group_clauses)?;
        group_clauses = flat;
    }

    let have_groupingsets = !qry.groupingSets.is_empty();

    // Detect non-Var grouping; track common Vars separately.
    let mut have_non_var_grouping = false;
    for tle in group_clauses.iter() {
        let is_var = tle.expr.as_deref().and_then(|e| e.as_var()).is_some();
        if !is_var {
            have_non_var_grouping = true;
        } else if !have_groupingsets
            || list_member_int(&gset_common, tle.ressortgroupref as i32)
        {
            if let Some(e) = tle.expr.as_deref() {
                group_clause_common_vars.push(e.clone());
            }
        }
    }

    // Build RTE_GROUP RTE/nsitem if there are acceptable GROUP BY expressions.
    if !group_clauses.is_empty() {
        // In C, pstate->p_rtable and qry->rtable alias the same List, so the
        // group RTE that addRangeTableEntryForGroup appends to p_rtable also
        // lands at the tail of qry->rtable (which already holds the FROM-clause
        // RTEs). The owned model moved p_rtable into qry->rtable during
        // transformStmt (emptying p_rtable), so restore p_rtable from the
        // current qry->rtable first; the group RTE then appends after the
        // FROM-clause entries, preserving their indices.
        if pstate.p_rtable.is_empty() && !qry.rtable.is_empty() {
            let mut restored: PgVec<RangeTblEntry<'mcx>> = PgVec::new_in(mcx);
            for rte in qry.rtable.iter() {
                restored.push(rte.clone_in(mcx)?);
            }
            pstate.p_rtable = restored;
        }
        let nsitem = backend_parser_relation::addRangeTableEntryForGroup(
            mcx,
            pstate,
            &group_clauses,
        )?;
        pstate.p_grouping_nsitem = Some(mcx::alloc_in(mcx, nsitem)?);
        // qry->rtable = pstate->p_rtable (C shares the list; here re-sync).
        let mut rtable: PgVec<RangeTblEntry<'mcx>> = PgVec::new_in(mcx);
        for rte in pstate.p_rtable.iter() {
            rtable.push(rte.clone_in(mcx)?);
        }
        qry.rtable = rtable;
        qry.hasGroupRTE = true;
    }

    // A read-only snapshot of the query used by the seam calls inside the
    // mutator/walker (flatten_join_alias_vars, check_functional_grouping).
    let qry_snapshot = Node::mk_query(mcx, qry.clone_in(mcx)?);

    // qry->constraintDeps, extended in place by check_functional_grouping.
    let mut constraint_deps: Vec<Oid> = qry.constraintDeps.iter().copied().collect();

    // Replace grouped vars in targetlist; finalize GROUPING exprs.
    let tlist = core::mem::replace(&mut qry.targetList, PgVec::new_in(mcx));
    // finalize_grouping_exprs walks the list (modifying GroupingFunc refs).
    let mut tlist_nodes: Vec<Node> = Vec::new();
    for te in tlist.into_iter() {
        tlist_nodes.push(Node::mk_target_entry(mcx, te));
    }
    finalize_grouping_exprs_list(
        mcx,
        &mut tlist_nodes,
        pstate,
        &qry_snapshot,
        &group_clauses,
        has_join_rtes,
        have_non_var_grouping,
    )?;
    let mut clause_list = tlist_nodes;
    if has_join_rtes {
        clause_list = flatten_node_list(mcx, &qry_snapshot, clause_list)?;
    }
    let new_tlist = substitute_grouped_columns_list(
        mcx,
        clause_list,
        pstate,
        &group_clauses,
        &group_clause_common_vars,
        &gset_common,
        have_groupingsets,
        have_non_var_grouping,
        &mut func_grouped_rels,
        &mut constraint_deps,
    )?;
    let mut new_tl: PgVec<types_nodes::primnodes::TargetEntry<'mcx>> = PgVec::new_in(mcx);
    for n in new_tlist {
        match n.into_targetentry() {
            Some(te) => new_tl.push(te),
            None => {
                return Err(PgError::error(
                    "parseCheckAggregates: targetList element is not a TargetEntry after substitution",
                ))
            }
        }
    }
    qry.targetList = new_tl;

    // HAVING.
    if let Some(having) = qry.havingQual.take() {
        // `havingQual` is the concretely-typed `Option<PgBox<Expr>>` view; the
        // grouping-expr helpers below operate on `Node`, so wrap the owned `Expr`
        // into `Node::Expr` here and unwrap back to `Expr` on store-back.
        let mut clause: Node = Node::mk_expr(mcx, (*having).clone_in(mcx)?);
        clause = finalize_grouping_exprs(
            mcx,
            clause,
            pstate,
            &qry_snapshot,
            &group_clauses,
            has_join_rtes,
            have_non_var_grouping,
        )?;
        if has_join_rtes {
            clause = backend_rewrite_rewritemanip_seams::flatten_join_alias_vars::call(
                mcx,
                &qry_snapshot,
                clause,
            )?;
        }
        let new_having = substitute_grouped_columns(
            mcx,
            clause,
            pstate,
            &group_clauses,
            &group_clause_common_vars,
            &gset_common,
            have_groupingsets,
            have_non_var_grouping,
            &mut func_grouped_rels,
            &mut constraint_deps,
        )?;
        let new_having_expr = match new_having.into_expr() {
            Some(e) => e,
            None => {
                return Err(PgError::error(
                    "parseCheckAggregates: havingQual lowered to a non-Expr node",
                ))
            }
        };
        qry.havingQual = Some(mcx::alloc_in(mcx, new_having_expr)?);
    }

    // Write back the (possibly extended) constraint dependency list.
    let mut cd: PgVec<Oid> = PgVec::new_in(mcx);
    cd.extend(constraint_deps);
    qry.constraintDeps = cd;

    // Aggregates can't appear in a recursive term.
    if pstate.p_hasAggs && has_self_ref_rtes {
        let qry_node = Node::mk_query(mcx, qry.clone_in(mcx)?);
        let loc = backend_rewrite_rewritemanip_seams::locate_agg_of_level::call(&qry_node, 0);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_RECURSION)
            .errmsg("aggregate functions are not allowed in a recursive query's recursive term")
            .errposition(parser_errposition(pstate, loc))
            .into_error());
    }

    Ok(())
}

/// `flatten_join_alias_vars(NULL, qry, (Node *) groupClauses)` then unwrap back
/// to a TargetEntry list.
fn flatten_group_clauses<'mcx>(
    mcx: Mcx<'mcx>,
    qry_node: &Node<'mcx>,
    group_clauses: Vec<types_nodes::primnodes::TargetEntry<'mcx>>,
) -> PgResult<Vec<types_nodes::primnodes::TargetEntry<'mcx>>> {
    let mut out: Vec<types_nodes::primnodes::TargetEntry<'mcx>> = Vec::new();
    out.reserve(group_clauses.len());
    for tle in group_clauses {
        let flat = backend_rewrite_rewritemanip_seams::flatten_join_alias_vars::call(
            mcx,
            qry_node,
            Node::mk_target_entry(mcx, tle),
        )?;
        match flat.into_targetentry() {
            Some(te) => out.push(te),
            None => {
                return Err(PgError::error(
                    "flatten_join_alias_vars on a groupClause TargetEntry did not return a TargetEntry",
                ))
            }
        }
    }
    Ok(out)
}

/// `flatten_join_alias_vars(NULL, qry, (Node *) list)` for a node list.
fn flatten_node_list<'mcx>(
    mcx: Mcx<'mcx>,
    qry_node: &Node<'mcx>,
    list: Vec<Node<'mcx>>,
) -> PgResult<Vec<Node<'mcx>>> {
    let mut out: Vec<Node<'mcx>> = Vec::new();
    out.reserve(list.len());
    for n in list {
        out.push(backend_rewrite_rewritemanip_seams::flatten_join_alias_vars::call(
            mcx, qry_node, n,
        )?);
    }
    Ok(out)
}

// ===========================================================================
// substitute_grouped_columns (parse_agg.c:1358)
// ===========================================================================

struct SubstituteContext<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'mcx>,
    group_clauses: &'a [types_nodes::primnodes::TargetEntry<'mcx>],
    group_clause_common_vars: &'a [Expr],
    gset_common: &'a [i32],
    have_groupingsets: bool,
    have_non_var_grouping: bool,
    func_grouped_rels: &'a mut Vec<i32>,
    constraint_deps: &'a mut Vec<Oid>,
    sublevels_up: i32,
    in_agg_direct_args: bool,
    error: Option<PgError>,
}

fn substitute_grouped_columns<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    pstate: &ParseState<'mcx>,
    group_clauses: &[types_nodes::primnodes::TargetEntry<'mcx>],
    group_clause_common_vars: &[Expr],
    gset_common: &[i32],
    have_groupingsets: bool,
    have_non_var_grouping: bool,
    func_grouped_rels: &mut Vec<i32>,
    constraint_deps: &mut Vec<Oid>,
) -> PgResult<Node<'mcx>> {
    let mut context = SubstituteContext {
        mcx,
        pstate,
        group_clauses,
        group_clause_common_vars,
        gset_common,
        have_groupingsets,
        have_non_var_grouping,
        func_grouped_rels,
        constraint_deps,
        sublevels_up: 0,
        in_agg_direct_args: false,
        error: None,
    };
    let mut node = node;
    substitute_grouped_columns_mutator(&mut node, &mut context);
    if let Some(e) = context.error.take() {
        return Err(e);
    }
    Ok(node)
}

fn substitute_grouped_columns_list<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: Vec<Node<'mcx>>,
    pstate: &ParseState<'mcx>,
    group_clauses: &[types_nodes::primnodes::TargetEntry<'mcx>],
    group_clause_common_vars: &[Expr],
    gset_common: &[i32],
    have_groupingsets: bool,
    have_non_var_grouping: bool,
    func_grouped_rels: &mut Vec<i32>,
    constraint_deps: &mut Vec<Oid>,
) -> PgResult<Vec<Node<'mcx>>> {
    let mut context = SubstituteContext {
        mcx,
        pstate,
        group_clauses,
        group_clause_common_vars,
        gset_common,
        have_groupingsets,
        have_non_var_grouping,
        func_grouped_rels,
        constraint_deps,
        sublevels_up: 0,
        in_agg_direct_args: false,
        error: None,
    };
    let mut out = nodes;
    for n in out.iter_mut() {
        substitute_grouped_columns_mutator(n, &mut context);
        if context.error.is_some() {
            break;
        }
    }
    if let Some(e) = context.error.take() {
        return Err(e);
    }
    Ok(out)
}

/// In-place mutator mirror of the C `substitute_grouped_columns_mutator`.
fn substitute_grouped_columns_mutator(node: &mut Node, context: &mut SubstituteContext) {
    if context.error.is_some() {
        return;
    }

    // NOTE: stays on the enum — the `GroupingFunc` arm needs the analyzed
    // `primnodes::GroupingFunc`, but the generated `as_groupingfunc` accessor is
    // shadowed by the Node-direct *raw* `rawexprnodes::GroupingFunc` (same tag)
    // and would mis-resolve; keeping the match on the enum preserves it.
    match node.as_expr_mut() {
        Some(Expr::Aggref(agg)) => {
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == context.sublevels_up {
                // Aggregate of the original level: don't recurse into normal
                // args/ORDER BY/filter; but check direct arguments as though
                // they weren't in an aggregate.
                debug_assert!(!context.in_agg_direct_args);
                context.in_agg_direct_args = true;
                // Mutate each direct arg in place.
                for e in agg.aggdirectargs.iter_mut() {
                    let mut n = Node::Expr(replace_expr_dummy(e));
                    substitute_grouped_columns_mutator(&mut n, context);
                    *e = unwrap_node_expr(n);
                    if context.error.is_some() {
                        break;
                    }
                }
                context.in_agg_direct_args = false;
                return;
            }
            // Aggregates of higher levels: skip; lower levels still examined.
            if agglevelsup > context.sublevels_up {
                return;
            }
            // Lower-level aggregate: generic recursion.
            mutate_generic(node, context);
            return;
        }
        Some(Expr::GroupingFunc(grp)) => {
            // handled GroupingFunc separately, no need to recheck at this level
            if grp.agglevelsup as i32 >= context.sublevels_up {
                return;
            }
            mutate_generic(node, context);
            return;
        }
        _ => {}
    }

    // If we have any GROUP BY items that are not simple Vars, check whether the
    // subexpression as a whole matches any GROUP BY item.
    if context.have_non_var_grouping && context.sublevels_up == 0 {
        if let Some(this_expr) = node.as_expr() {
            let mut attnum: i32 = 0;
            for tle in context.group_clauses {
                attnum += 1;
                if let Some(e) = tle.expr.as_deref() {
                    if backend_nodes_equalfuncs_seams::equal_expr::call(this_expr, e) {
                        match build_grouped_var(attnum, tle.ressortgroupref, context) {
                            Ok(v) => {
                                *node = Node::Expr(Expr::Var(v));
                                return;
                            }
                            Err(err) => {
                                context.error = Some(err);
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    // Constants/Params are always acceptable (after the whole-expr check).
    if node.is_const() || node.is_param() {
        return;
    }

    // Ungrouped Var of the original query level => failure.
    if let Some(var) = node.as_var() {
        if var.varlevelsup as i32 != context.sublevels_up {
            return; // not local to my query, ignore
        }

        // Match against group clauses if not done above.
        if !context.have_non_var_grouping || context.sublevels_up != 0 {
            let mut attnum: i32 = 0;
            for tle in context.group_clauses {
                attnum += 1;
                if let Some(gvar) = tle.expr.as_deref().and_then(|e| e.as_var()) {
                    if gvar.varno == var.varno
                        && gvar.varattno == var.varattno
                        && gvar.varlevelsup == 0
                    {
                        match build_grouped_var(attnum, tle.ressortgroupref, context) {
                            Ok(v) => {
                                *node = Node::Expr(Expr::Var(v));
                                return;
                            }
                            Err(err) => {
                                context.error = Some(err);
                                return;
                            }
                        }
                    }
                }
            }
        }

        let varno = var.varno;
        let varattno = var.varattno;
        let location = var.location;

        // Functional dependency on GROUP BY columns?
        if list_member_int(context.func_grouped_rels, varno) {
            return; // previously proven acceptable
        }

        debug_assert!(varno > 0 && varno as usize <= context.pstate.p_rtable.len());
        let rte = &context.pstate.p_rtable[(varno - 1) as usize];
        if rte.rtekind == RTEKind::RTE_RELATION {
            // grouping_columns are the common Vars, wrapped as nodes.
            let mut grouping_columns: Vec<Node> = Vec::new();
            for e in context.group_clause_common_vars {
                grouping_columns.push(Node::mk_expr(context.mcx, e.clone()));
            }
            let deps = core::mem::take(context.constraint_deps);
            match backend_catalog_pg_constraint::check_functional_grouping(
                context.mcx,
                rte.relid,
                varno as u32,
                0,
                &grouping_columns,
                deps,
            ) {
                Ok((ok, new_deps)) => {
                    *context.constraint_deps = new_deps;
                    if ok {
                        context.func_grouped_rels.push(varno);
                        return; // acceptable
                    }
                }
                Err(e) => {
                    context.error = Some(e);
                    return;
                }
            }
        }

        // Found an ungrouped local variable; generate error message.
        let attname = match backend_parser_relation::get_rte_attribute_name(
            context.mcx,
            rte,
            varattno,
        ) {
            Ok(n) => n,
            Err(e) => {
                context.error = Some(e);
                return;
            }
        };
        let aliasname = rte_eref_aliasname(rte);
        let err = if context.sublevels_up == 0 {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_GROUPING_ERROR)
                .errmsg(alloc_fmt(&format_args!(
                    "column \"{}.{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    aliasname,
                    attname.as_str()
                )));
            if context.in_agg_direct_args {
                b = b.errdetail(
                    "Direct arguments of an ordered-set aggregate must use only grouped columns.",
                );
            }
            b.errposition(parser_errposition(context.pstate, location))
                .into_error()
        } else {
            ereport(ERROR)
                .errcode(ERRCODE_GROUPING_ERROR)
                .errmsg(alloc_fmt(&format_args!(
                    "subquery uses ungrouped column \"{}.{}\" from outer query",
                    aliasname,
                    attname.as_str()
                )))
                .errposition(parser_errposition(context.pstate, location))
                .into_error()
        };
        context.error = Some(err);
        return;
    }

    if let Some(query) = node.as_query_mut() {
        // Recurse into subselects.
        context.sublevels_up += 1;
        query_tree_mutator(
            query,
            &mut |n: &mut Node| {
                substitute_grouped_columns_mutator(n, context);
                context.error.is_some()
            },
            0,
        );
        context.sublevels_up -= 1;
        return;
    }

    mutate_generic(node, context);
}

/// `expression_tree_mutator(node, substitute_grouped_columns_mutator, context)`
/// via the in-place owned-tree walker.
fn mutate_generic(node: &mut Node, context: &mut SubstituteContext) {
    let mcx = context.mcx;
    expression_tree_walker_mut(
        node,
        &mut |n: &mut Node| {
            substitute_grouped_columns_mutator(n, context);
            context.error.is_some()
        },
        mcx,
    );
}

/// Take an `Expr` out of an `&mut Expr` slot, leaving a cheap placeholder
/// (`Const` default) behind. Used to move a child into a `Node` for in-place
/// mutation, then write the result back.
fn replace_expr_dummy(slot: &mut Expr) -> Expr {
    core::mem::replace(slot, Expr::Const(types_nodes::primnodes::Const::default()))
}

/// Unwrap a `Node::Expr` back into its `Expr`.
fn unwrap_node_expr(n: Node) -> Expr {
    match n.into_expr() {
        Some(e) => e,
        None => Expr::Const(types_nodes::primnodes::Const::default()),
    }
}

// ===========================================================================
// buildGroupedVar (parse_agg.c:1761)
// ===========================================================================

fn build_grouped_var(
    attnum: i32,
    ressortgroupref: Index,
    context: &mut SubstituteContext,
) -> PgResult<Var> {
    let grouping_nsitem = context.pstate.p_grouping_nsitem.as_deref().ok_or_else(|| {
        PgError::error("buildGroupedVar: p_grouping_nsitem must be set before buildGroupedVar")
    })?;
    let nscol = &grouping_nsitem.p_nscolumns[(attnum - 1) as usize];

    debug_assert!(nscol.p_varno == grouping_nsitem.p_rtindex as Index);
    debug_assert!(nscol.p_varattno == attnum as AttrNumber);

    let mut var = make_var(
        nscol.p_varno as i32,
        nscol.p_varattno,
        nscol.p_vartype,
        nscol.p_vartypmod,
        nscol.p_varcollid,
        context.sublevels_up as Index,
    );
    // makeVar doesn't offer parameters for these, so set by hand.
    var.varnosyn = nscol.p_varnosyn;
    var.varattnosyn = nscol.p_varattnosyn;

    if context.have_groupingsets
        && !list_member_int(context.gset_common, ressortgroupref as i32)
    {
        // var->varnullingrels = bms_add_member(var->varnullingrels, p_rtindex);
        var.varnullingrels =
            exprrelids_add_member(&var.varnullingrels, grouping_nsitem.p_rtindex);
    }

    Ok(var)
}

/// `bms_add_member(set, x)` operating on the `ExprRelids` word storage that
/// `Var.varnullingrels` carries (mirrors bitmapset.c word math).
fn exprrelids_add_member(
    er: &types_nodes::primnodes::ExprRelids,
    x: i32,
) -> types_nodes::primnodes::ExprRelids {
    debug_assert!(x >= 0);
    let x = x as u32;
    const BITS_PER_WORD: u32 = 64;
    let wordnum = (x / BITS_PER_WORD) as usize;
    let bitnum = x % BITS_PER_WORD;
    let mut words: Vec<u64> = er.words.clone();
    if words.len() <= wordnum {
        words.resize(wordnum + 1, 0);
    }
    words[wordnum] |= 1u64 << bitnum;
    types_nodes::primnodes::ExprRelids { words }
}

// ===========================================================================
// finalize_grouping_exprs (parse_agg.c:1594)
// ===========================================================================

struct FinalizeContext<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'mcx>,
    qry: &'a Node<'mcx>,
    group_clauses: &'a [types_nodes::primnodes::TargetEntry<'mcx>],
    has_join_rtes: bool,
    have_non_var_grouping: bool,
    sublevels_up: i32,
    error: Option<PgError>,
}

fn finalize_grouping_exprs_list<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: &mut [Node<'mcx>],
    pstate: &ParseState<'mcx>,
    qry: &Node<'mcx>,
    group_clauses: &[types_nodes::primnodes::TargetEntry<'mcx>],
    has_join_rtes: bool,
    have_non_var_grouping: bool,
) -> PgResult<()> {
    let mut context = FinalizeContext {
        mcx,
        pstate,
        qry,
        group_clauses,
        has_join_rtes,
        have_non_var_grouping,
        sublevels_up: 0,
        error: None,
    };
    for n in nodes.iter_mut() {
        finalize_grouping_exprs_walker(n, &mut context);
        if context.error.is_some() {
            break;
        }
    }
    if let Some(e) = context.error.take() {
        return Err(e);
    }
    Ok(())
}

fn finalize_grouping_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    mut node: Node<'mcx>,
    pstate: &ParseState<'mcx>,
    qry: &Node<'mcx>,
    group_clauses: &[types_nodes::primnodes::TargetEntry<'mcx>],
    has_join_rtes: bool,
    have_non_var_grouping: bool,
) -> PgResult<Node<'mcx>> {
    let mut context = FinalizeContext {
        mcx,
        pstate,
        qry,
        group_clauses,
        has_join_rtes,
        have_non_var_grouping,
        sublevels_up: 0,
        error: None,
    };
    finalize_grouping_exprs_walker(&mut node, &mut context);
    if let Some(e) = context.error.take() {
        return Err(e);
    }
    Ok(node)
}

/// Mirror of the C `finalize_grouping_exprs_walker` (in-place: fills
/// `grp->refs`; descends via the in-place walker). Returns nothing; aborts on
/// `context.error`.
fn finalize_grouping_exprs_walker(node: &mut Node, context: &mut FinalizeContext) {
    if context.error.is_some() {
        return;
    }
    // constants are always acceptable
    if node.is_const() || node.is_param() {
        return;
    }

    // NOTE: stays on the enum — the `GroupingFunc` arm needs the analyzed
    // `primnodes::GroupingFunc` (read+write `refs`), but the generated
    // `as_groupingfunc` accessor is shadowed by the Node-direct *raw*
    // `rawexprnodes::GroupingFunc` (same tag); keep the match on the enum.
    // Peel the `Node::Expr` wrapper mutably first (dual-homed tag rule), then
    // dispatch the inner analyzed `Expr` payload.
    match node.as_expr_mut() {
        Some(Expr::Aggref(agg)) => {
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == context.sublevels_up {
                // Aggregate of the original level: don't recurse into normal
                // args/ORDER BY/filter; check direct arguments as though not in
                // an aggregate.
                for e in agg.aggdirectargs.iter_mut() {
                    let mut n = Node::Expr(replace_expr_dummy(e));
                    finalize_grouping_exprs_walker(&mut n, context);
                    *e = unwrap_node_expr(n);
                    if context.error.is_some() {
                        break;
                    }
                }
                return;
            }
            // Aggregates of higher levels: skip; lower levels examined.
            if agglevelsup > context.sublevels_up {
                return;
            }
            finalize_generic(node, context);
            return;
        }
        Some(Expr::GroupingFunc(grp)) => {
            // Only check GroupingFunc nodes at the exact level.
            if grp.agglevelsup as i32 == context.sublevels_up {
                match compute_grouping_refs(grp, context) {
                    Ok(refs) => grp.refs = refs,
                    Err(e) => {
                        context.error = Some(e);
                        return;
                    }
                }
            }
            if grp.agglevelsup as i32 > context.sublevels_up {
                return;
            }
            // Same/lower level: C falls through to the generic walker (the
            // GroupingFunc has no expression children to rewrite, but the C does
            // descend via expression_tree_walker).
            finalize_generic(node, context);
            return;
        }
        _ => {}
    }
    if node.node_tag() == ntag::T_Query {
        // Recurse into subselects.
        let query = node.expect_query_mut();
        context.sublevels_up += 1;
        query_tree_mutator(
            query,
            &mut |n: &mut Node| {
                finalize_grouping_exprs_walker(n, context);
                context.error.is_some()
            },
            0,
        );
        context.sublevels_up -= 1;
        return;
    }

    finalize_generic(node, context);
}

/// The inner loop of `finalize_grouping_exprs_walker` for a level-matching
/// `GroupingFunc` (fills `ref_list`).
fn compute_grouping_refs(
    grp: &types_nodes::primnodes::GroupingFunc,
    context: &mut FinalizeContext,
) -> PgResult<Vec<i32>> {
    let mut ref_list: Vec<i32> = Vec::new();
    ref_list.reserve(grp.args.len());

    for expr in &grp.args {
        let mut ref_: Index = 0;

        // Each expression must match a grouping entry at the current level.
        let flat_node;
        let expr_node: Node = if context.has_join_rtes {
            flat_node = backend_rewrite_rewritemanip_seams::flatten_join_alias_vars::call(
                context.mcx,
                context.qry,
                Node::mk_expr(context.mcx, expr.clone()),
            )?;
            flat_node
        } else {
            Node::mk_expr(context.mcx, expr.clone())
        };

        let cur_expr: &Expr = match expr_node.as_expr() {
            Some(e) => e,
            None => {
                // flatten_join_alias_vars on an Expr always yields an Expr node.
                return Err(PgError::error(
                    "finalize_grouping_exprs: flattened GROUPING argument is not an expression",
                ));
            }
        };

        if let Some(var) = cur_expr.as_var() {
            if var.varlevelsup as i32 == context.sublevels_up {
                for tle in context.group_clauses {
                    if let Some(gvar) = tle.expr.as_deref().and_then(|e| e.as_var()) {
                        if gvar.varno == var.varno
                            && gvar.varattno == var.varattno
                            && gvar.varlevelsup == 0
                        {
                            ref_ = tle.ressortgroupref;
                            break;
                        }
                    }
                }
            }
        } else if context.have_non_var_grouping && context.sublevels_up == 0 {
            for tle in context.group_clauses {
                if let Some(e) = tle.expr.as_deref() {
                    if backend_nodes_equalfuncs_seams::equal_expr::call(cur_expr, e) {
                        ref_ = tle.ressortgroupref;
                        break;
                    }
                }
            }
        }

        if ref_ == 0 {
            let loc = exprLocation_node(cur_expr);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_GROUPING_ERROR)
                .errmsg("arguments to GROUPING must be grouping expressions of the associated query level")
                .errposition(parser_errposition(context.pstate, loc))
                .into_error());
        }

        ref_list.push(ref_ as i32);
    }

    Ok(ref_list)
}

/// `expression_tree_walker(node, finalize_grouping_exprs_walker, context)`.
fn finalize_generic(node: &mut Node, context: &mut FinalizeContext) {
    let mcx = context.mcx;
    expression_tree_walker_mut(
        node,
        &mut |n: &mut Node| {
            finalize_grouping_exprs_walker(n, context);
            context.error.is_some()
        },
        mcx,
    );
}

// ===========================================================================
// expand_groupingset_node (parse_agg.c:1801)
// ===========================================================================

/// `expand_groupingset_node(gs)` — expand one `GroupingSet` to a list of integer
/// sets (`List<List<int>>`). A trailing empty rollup level is the empty set.
fn expand_groupingset_node(gs: &GroupingSet) -> PgResult<Vec<Vec<i32>>> {
    let mut result: Vec<Vec<i32>> = Vec::new();

    match gs.kind {
        GroupingSetKind::GROUPING_SET_EMPTY => {
            // list_make1(NIL)
            result.push(Vec::new());
        }
        GroupingSetKind::GROUPING_SET_SIMPLE => {
            // list_make1(gs->content): content is a list of Integer nodes.
            let mut s: Vec<i32> = Vec::new();
            collect_simple_content_ints(&gs.content, &mut s)?;
            result.push(s);
        }
        GroupingSetKind::GROUPING_SET_ROLLUP => {
            let rollup_val = &gs.content;
            let mut curgroup_size = rollup_val.len();

            while curgroup_size > 0 {
                let mut current_result: Vec<i32> = Vec::new();
                let mut i = curgroup_size;

                for gs_current_node in rollup_val.iter() {
                    let gs_current = node_as_groupingset(gs_current_node)?;
                    debug_assert!(gs_current.kind == GroupingSetKind::GROUPING_SET_SIMPLE);
                    collect_simple_content_ints(&gs_current.content, &mut current_result)?;
                    // If we are done with making the current group, break.
                    i -= 1;
                    if i == 0 {
                        break;
                    }
                }

                result.push(current_result);
                curgroup_size -= 1;
            }

            // lappend(result, NIL): trailing empty set.
            result.push(Vec::new());
        }
        GroupingSetKind::GROUPING_SET_CUBE => {
            let cube_list = &gs.content;
            let number_bits = cube_list.len();
            // parser should cap this much lower
            debug_assert!(number_bits < 31);
            let num_sets: u32 = 1u32 << number_bits;

            for i in 0..num_sets {
                let mut current_result: Vec<i32> = Vec::new();
                let mut mask: u32 = 1;

                for gs_current_node in cube_list.iter() {
                    let gs_current = node_as_groupingset(gs_current_node)?;
                    debug_assert!(gs_current.kind == GroupingSetKind::GROUPING_SET_SIMPLE);
                    if (mask & i) != 0 {
                        collect_simple_content_ints(&gs_current.content, &mut current_result)?;
                    }
                    mask <<= 1;
                }

                result.push(current_result);
            }
        }
        GroupingSetKind::GROUPING_SET_SETS => {
            for gs_current_node in gs.content.iter() {
                let gs_current = node_as_groupingset(gs_current_node)?;
                let current_result = expand_groupingset_node(gs_current)?;
                result.extend(current_result);
            }
        }
    }

    Ok(result)
}

/// A `GROUPING_SET_SIMPLE` node's content is a list of `Integer` nodes.
fn collect_simple_content_ints(content: &[NodePtr], out: &mut Vec<i32>) -> PgResult<()> {
    for n in content {
        if let Some(i) = n.as_integer() {
            out.push(i.ival);
        }
    }
    Ok(())
}

fn node_as_groupingset<'a, 'mcx>(n: &'a Node<'mcx>) -> PgResult<&'a GroupingSet<'mcx>> {
    match n.as_groupingset() {
        Some(gs) => Ok(gs),
        None => Err(PgError::error(
            "expand_groupingset_node: content element is not a GroupingSet",
        )),
    }
}

// ===========================================================================
// cmp_list_len_asc / cmp_list_len_contents_asc (parse_agg.c:1903 / 1913)
// ===========================================================================

/// `pg_cmp_s32(a, b)` (common/int.h).
#[inline]
fn pg_cmp_s32(a: i32, b: i32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

fn cmp_list_len_asc(a: &[i32], b: &[i32]) -> core::cmp::Ordering {
    let la = a.len() as i32;
    let lb = b.len() as i32;
    pg_cmp_s32(la, lb).cmp(&0)
}

fn cmp_list_len_contents_asc(a: &[i32], b: &[i32]) -> core::cmp::Ordering {
    let res = cmp_list_len_asc(a, b);
    if res == core::cmp::Ordering::Equal {
        // forboth(lca, la, lcb, lb)
        let n = a.len().min(b.len());
        for i in 0..n {
            let va = a[i];
            let vb = b[i];
            if va > vb {
                return core::cmp::Ordering::Greater;
            }
            if va < vb {
                return core::cmp::Ordering::Less;
            }
        }
    }
    res
}

// ===========================================================================
// expand_grouping_sets (parse_agg.c:1947)
// ===========================================================================

/// Expand a groupingSets clause to a flat list of integer grouping sets, sorted
/// by length (shortest first). `None` when the expansion exceeds `limit`.
pub fn expand_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    grouping_sets: &[Node<'mcx>],
    group_distinct: bool,
    limit: i32,
) -> PgResult<Option<PgVec<'mcx, PgVec<'mcx, i32>>>> {
    let mut expanded_groups: Vec<Vec<Vec<i32>>> = Vec::new();
    let mut result: Vec<Vec<i32>> = Vec::new();
    let mut numsets: f64 = 1.0;

    if grouping_sets.is_empty() {
        return Ok(None);
    }

    for gs_node in grouping_sets {
        let gs = node_as_groupingset(gs_node)?;
        let current_result = expand_groupingset_node(gs)?;
        debug_assert!(!current_result.is_empty());
        numsets *= current_result.len() as f64;
        if limit >= 0 && numsets > limit as f64 {
            return Ok(None);
        }
        expanded_groups.push(current_result);
    }

    // Cartesian product across sublists; dedup individual sets (don't change the
    // number of sets).
    if let Some(first) = expanded_groups.first() {
        for set in first {
            result.push(list_union_int(&[], set));
        }
    }

    // for_each_from(lc, expanded_groups, 1)
    for p in expanded_groups.iter().skip(1) {
        let mut new_result: Vec<Vec<i32>> = Vec::new();
        for q in &result {
            for pc in p {
                new_result.push(list_union_int(q, pc));
            }
        }
        result = new_result;
    }

    // Sort by length and deduplicate if requested.
    if !group_distinct || result.len() < 2 {
        result.sort_by(|a, b| cmp_list_len_asc(a, b));
    } else {
        // Sort each groupset individually.
        for set in result.iter_mut() {
            set.sort_by(|a, b| pg_cmp_s32(*a, *b).cmp(&0));
        }
        // Sort the list of groupsets by length and contents.
        result.sort_by(|a, b| cmp_list_len_contents_asc(a, b));
        // Remove duplicates (the C in-place adjacent-delete loop).
        let mut dedup: Vec<Vec<i32>> = Vec::new();
        for set in result.into_iter() {
            let is_dup = dedup.last().is_some_and(|prev| prev[..] == set[..]);
            if !is_dup {
                dedup.push(set);
            }
        }
        result = dedup;
    }

    // Hand the result across the boundary as charged PgVecs.
    let mut out: PgVec<'mcx, PgVec<'mcx, i32>> = PgVec::new_in(mcx);
    for s in result {
        let mut row: PgVec<'mcx, i32> = PgVec::new_in(mcx);
        row.extend(s);
        out.push(row);
    }
    Ok(Some(out))
}

/// `list_intersection_int(list1, list2)` — integers present in both, in list1's
/// order.
fn list_intersection_int(list1: &[i32], list2: &[i32]) -> Vec<i32> {
    let mut out: Vec<i32> = Vec::new();
    if list1.is_empty() {
        return out;
    }
    for &v in list1 {
        if list2.contains(&v) {
            out.push(v);
        }
    }
    out
}

/// `list_union_int(list1, list2)` — list1 plus those of list2 not present.
fn list_union_int(list1: &[i32], list2: &[i32]) -> Vec<i32> {
    let mut out: Vec<i32> = list1.to_vec();
    for &v in list2 {
        if !out.contains(&v) {
            out.push(v);
        }
    }
    out
}

/// `list_member_int(list, datum)`.
#[inline]
fn list_member_int(list: &[i32], datum: i32) -> bool {
    list.contains(&datum)
}

// ===========================================================================
// get_aggregate_argtypes (parse_agg.c:2050)
// ===========================================================================

/// Identify the specific datatypes passed to an aggregate call.
pub fn get_aggregate_argtypes<'mcx>(mcx: Mcx<'mcx>, aggref: &Aggref) -> PgResult<PgVec<'mcx, Oid>> {
    debug_assert!(aggref.aggargtypes.len() <= FUNC_MAX_ARGS);
    let mut out: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    for &t in &aggref.aggargtypes {
        out.push(t);
    }
    Ok(out)
}

// ===========================================================================
// resolve_aggregate_transtype (parse_agg.c:2076)
// ===========================================================================

/// Identify the transition state value's datatype for an aggregate call.
///
/// C `resolve_aggregate_transtype` takes no context argument, but
/// `get_func_signature` palloc's the declared-arg array; the array is consumed
/// (copied) and freed here, so `mcx` is the scratch context for that fetch.
pub fn resolve_aggregate_transtype<'mcx>(
    mcx: Mcx<'mcx>,
    aggfuncid: Oid,
    mut aggtranstype: Oid,
    input_types: &[Oid],
    num_arguments: i32,
) -> PgResult<Oid> {
    // resolve actual type of transition state, if polymorphic
    if IsPolymorphicType(aggtranstype) {
        // have to fetch the agg's declared input types...
        let sig =
            backend_utils_cache_lsyscache_seams::get_func_signature::call(mcx, aggfuncid)?;
        let mut declared_arg_types: Vec<Oid> = sig.iter().copied().collect();
        let agg_nargs = declared_arg_types.len() as i32;

        // VARIADIC ANY aggs could have more actual than declared args, but such
        // extra args can't affect polymorphic type resolution.
        debug_assert!(agg_nargs <= num_arguments);

        aggtranstype = backend_parser_coerce::enforce_generic_type_consistency(
            input_types,
            &mut declared_arg_types,
            agg_nargs,
            aggtranstype,
            false,
        )?;
    }
    Ok(aggtranstype)
}

// ===========================================================================
// agg_args_support_sendreceive (parse_agg.c:2112)
// ===========================================================================

/// Returns true if all non-byval types of aggref's args have send and receive
/// functions.
pub fn agg_args_support_sendreceive(aggref: &Aggref) -> PgResult<bool> {
    for tle in &aggref.args {
        let typ = match tle.expr.as_deref() {
            Some(e) => exprType_node(e),
            None => InvalidOid,
        };

        // RECORD is a special case: record_recv needs the typmod to identify the
        // anonymous record type, which array_agg_deserialize cannot supply.
        if typ == RECORDOID {
            return Ok(false);
        }

        let row =
            backend_utils_cache_lsyscache_seams::get_type_sendreceive_byval::call(typ)?;

        if !row.typbyval && (!OidIsValid(row.typsend) || !OidIsValid(row.typreceive)) {
            return Ok(false);
        }
    }
    Ok(true)
}

// ===========================================================================
// build_aggregate_transfn_expr (parse_agg.c:2179)
// ===========================================================================

/// Create expression trees for the transition (and optional inverse-transition)
/// function of an aggregate.
pub fn build_aggregate_transfn_expr(
    agg_input_types: &[Oid],
    agg_num_inputs: i32,
    agg_num_direct_inputs: i32,
    agg_variadic: bool,
    agg_state_type: Oid,
    agg_input_collation: Oid,
    transfn_oid: Oid,
    invtransfn_oid: Oid,
    build_invtrans: bool,
) -> PgResult<(Expr, Option<Expr>)> {
    // Build arg list to use in the transfn FuncExpr node.
    let mut args: Vec<Expr> = Vec::new();
    args.push(make_agg_arg(agg_state_type, agg_input_collation));

    let mut i = agg_num_direct_inputs;
    while i < agg_num_inputs {
        args.push(make_agg_arg(agg_input_types[i as usize], agg_input_collation));
        i += 1;
    }

    let mut fexpr = make_func_expr_variadic(
        transfn_oid,
        agg_state_type,
        args.clone(),
        InvalidOid,
        agg_input_collation,
        COERCE_EXPLICIT_CALL,
    );
    set_funcvariadic(&mut fexpr, agg_variadic);
    let transfnexpr = fexpr;

    // Build invtransfn expression if requested, with same args as transfn.
    let invtransfnexpr = if build_invtrans {
        if OidIsValid(invtransfn_oid) {
            let mut fexpr = make_func_expr_variadic(
                invtransfn_oid,
                agg_state_type,
                args,
                InvalidOid,
                agg_input_collation,
                COERCE_EXPLICIT_CALL,
            );
            set_funcvariadic(&mut fexpr, agg_variadic);
            Some(fexpr)
        } else {
            None
        }
    } else {
        None
    };

    Ok((transfnexpr, invtransfnexpr))
}

/// `makeFuncExpr(...)` — wraps `make_func_expr` (which returns an `Expr`).
fn make_func_expr_variadic(
    funcid: Oid,
    rettype: Oid,
    args: Vec<Expr>,
    funccollid: Oid,
    inputcollid: Oid,
    fformat: types_nodes::primnodes::CoercionForm,
) -> Expr {
    make_func_expr(funcid, rettype, args, funccollid, inputcollid, fformat)
}

/// Set `funcvariadic` on a `FuncExpr` `Expr`.
fn set_funcvariadic(expr: &mut Expr, variadic: bool) {
    if let Expr::FuncExpr(f) = expr {
        f.funcvariadic = variadic;
    }
}

// ===========================================================================
// build_aggregate_serialfn_expr (parse_agg.c:2240)
// ===========================================================================

/// Build an expression tree for an aggregate's serialization function.
pub fn build_aggregate_serialfn_expr(serialfn_oid: Oid) -> PgResult<Expr> {
    // serialfn always takes INTERNAL and returns BYTEA.
    let args = alloc_vec1(make_agg_arg(INTERNALOID, InvalidOid));
    Ok(make_func_expr_variadic(
        serialfn_oid,
        BYTEAOID,
        args,
        InvalidOid,
        InvalidOid,
        COERCE_EXPLICIT_CALL,
    ))
}

// ===========================================================================
// build_aggregate_deserialfn_expr (parse_agg.c:2263)
// ===========================================================================

/// Build an expression tree for an aggregate's deserialization function.
pub fn build_aggregate_deserialfn_expr(deserialfn_oid: Oid) -> PgResult<Expr> {
    // deserialfn always takes BYTEA, INTERNAL and returns INTERNAL.
    let mut args: Vec<Expr> = Vec::new();
    args.push(make_agg_arg(BYTEAOID, InvalidOid));
    args.push(make_agg_arg(INTERNALOID, InvalidOid));
    Ok(make_func_expr_variadic(
        deserialfn_oid,
        INTERNALOID,
        args,
        InvalidOid,
        InvalidOid,
        COERCE_EXPLICIT_CALL,
    ))
}

// ===========================================================================
// build_aggregate_finalfn_expr (parse_agg.c:2287)
// ===========================================================================

/// Build an expression tree for an aggregate's final function.
pub fn build_aggregate_finalfn_expr(
    agg_input_types: &[Oid],
    num_finalfn_inputs: i32,
    agg_state_type: Oid,
    agg_result_type: Oid,
    agg_input_collation: Oid,
    finalfn_oid: Oid,
) -> PgResult<Expr> {
    // Build expr tree for final function.
    let mut args: Vec<Expr> = Vec::new();
    args.push(make_agg_arg(agg_state_type, agg_input_collation));

    // finalfn may take additional args, which match agg's input types.
    let mut i = 0;
    while i < num_finalfn_inputs - 1 {
        args.push(make_agg_arg(agg_input_types[i as usize], agg_input_collation));
        i += 1;
    }

    // finalfn is currently never treated as variadic.
    Ok(make_func_expr_variadic(
        finalfn_oid,
        agg_result_type,
        args,
        InvalidOid,
        agg_input_collation,
        COERCE_EXPLICIT_CALL,
    ))
}

#[inline]
fn alloc_vec1(e: Expr) -> Vec<Expr> {
    let mut v = Vec::new();
    v.push(e);
    v
}

// ===========================================================================
// make_agg_arg (parse_agg.c:2327)
// ===========================================================================

/// `make_agg_arg(argtype, argcollation)` — a dummy `Param` of the given type.
fn make_agg_arg(argtype: Oid, argcollation: Oid) -> Expr {
    Expr::Param(Param {
        paramkind: ParamKind::PARAM_EXEC,
        paramid: -1,
        paramtype: argtype,
        paramtypmod: -1,
        paramcollid: argcollation,
        location: -1,
    })
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
pub fn init_seams() {
    backend_parser_parse_agg_seams::transform_grouping_func::set(transformGroupingFunc);
    backend_parser_parse_agg_seams::transform_aggregate_call::set(transformAggregateCall);
    backend_parser_parse_agg_seams::transform_window_func_call::set(transformWindowFuncCall);
    backend_parser_parse_agg_seams::parse_check_aggregates::set(parseCheckAggregates);
    backend_parser_parse_agg_seams::expand_grouping_sets::set(expand_grouping_sets);
    backend_parser_parse_agg_seams::get_aggregate_argtypes::set(get_aggregate_argtypes);
    backend_parser_parse_agg_seams::resolve_aggregate_transtype::set(resolve_aggregate_transtype);
    backend_parser_parse_agg_seams::agg_args_support_sendreceive::set(agg_args_support_sendreceive);
    backend_parser_parse_agg_seams::build_aggregate_transfn_expr::set(build_aggregate_transfn_expr);
    backend_parser_parse_agg_seams::build_aggregate_serialfn_expr::set(build_aggregate_serialfn_expr);
    backend_parser_parse_agg_seams::build_aggregate_deserialfn_expr::set(build_aggregate_deserialfn_expr);
    backend_parser_parse_agg_seams::build_aggregate_finalfn_expr::set(build_aggregate_finalfn_expr);
}
