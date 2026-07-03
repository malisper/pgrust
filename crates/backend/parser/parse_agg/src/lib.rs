#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use elog::ereport;
use mcx::Mcx;
use parser_small1::{parser_errposition, ParseExprKind, ParseState};
use types_core::{Oid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_GROUPING_ERROR,
    ERROR,
};
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::Aggref;
use types_nodes::{Node, NodeList, NodeTag};

pub fn transformAggregateCall<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    agg: &mut Aggref<'mcx>,
    args: &NodeList<'mcx>,
    arg_types: &[Oid],
    agg_order: &NodeList<'mcx>,
    agg_distinct: bool,
) -> PgResult<()> {
    if agg.aggkind != types_nodes::primnodes::AGGKIND_NORMAL {
        panic!(
            "transformAggregateCall (parse_agg.c): ordered-set direct/aggregated arg split \
             unported — backend-parser-agg ordered-set lane"
        );
    }
    if !agg_order.is_nil() {
        panic!(
            "transformAggregateCall (parse_agg.c): agg ORDER BY needs transformSortClause \
             (parse_clause.c) resjunk tlist handling — backend-parser-agg ordered lane"
        );
    }
    if agg_distinct {
        panic!(
            "transformAggregateCall (parse_agg.c): DISTINCT needs transformDistinctClause \
             (parse_clause.c) — backend-parser-agg distinct lane"
        );
    }

    let mut tlist = NodeList::nil();
    let mut attno: i16 = 1;
    for arg in args {
        let tle = Node::mk_target_entry(mcx, arg, attno, None, false)?;
        tlist.lappend(mcx, tle)?;
        attno += 1;
    }
    agg.aggdirectargs = NodeList::nil();
    agg.args = tlist;
    agg.aggorder = NodeList::nil();
    agg.aggdistinct = NodeList::nil();

    // Divergence: aggargtypes from caller-computed exprType values (nodeFuncs
    // slice lives in parse_expr; parse_oper::make_op precedent).
    let mut argtypes = types_nodes::list::OidList::nil();
    for &t in arg_types {
        argtypes.lappend(mcx, t)?;
    }
    agg.aggargtypes = argtypes;

    agg.agglevelsup =
        check_agglevels_and_constraints(pstate, &agg.args, agg.aggfilter, agg.location)?;
    Ok(())
}

fn check_agglevels_and_constraints<'mcx>(
    pstate: &mut ParseState<'_, 'mcx>,
    args: &NodeList<'mcx>,
    filter: Option<Node<'mcx>>,
    location: ParseLoc,
) -> PgResult<u32> {
    let min_varlevel = check_agg_arguments(pstate, args, filter, location)?;
    if min_varlevel > 0 {
        panic!(
            "check_agglevels_and_constraints (parse_agg.c): outer-level aggregate \
             (agglevelsup > 0) needs parentParseState hops — backend-parser-agg"
        );
    }
    pstate.p_hasAggs = true;

    let err: Option<&'static str> = match pstate.p_expr_kind {
        ParseExprKind::EXPR_KIND_NONE => {
            panic!("check_agglevels_and_constraints (parse_agg.c): EXPR_KIND_NONE cannot happen")
        }
        ParseExprKind::EXPR_KIND_OTHER
        | ParseExprKind::EXPR_KIND_HAVING
        | ParseExprKind::EXPR_KIND_WINDOW_PARTITION
        | ParseExprKind::EXPR_KIND_WINDOW_ORDER
        | ParseExprKind::EXPR_KIND_SELECT_TARGET
        | ParseExprKind::EXPR_KIND_ORDER_BY
        | ParseExprKind::EXPR_KIND_DISTINCT_ON => None,
        ParseExprKind::EXPR_KIND_JOIN_ON | ParseExprKind::EXPR_KIND_JOIN_USING => {
            Some("aggregate functions are not allowed in JOIN conditions")
        }
        ParseExprKind::EXPR_KIND_FROM_SUBSELECT => {
            Some("aggregate functions are not allowed in FROM clause of their own query level")
        }
        ParseExprKind::EXPR_KIND_FROM_FUNCTION => {
            Some("aggregate functions are not allowed in functions in FROM")
        }
        ParseExprKind::EXPR_KIND_POLICY => {
            Some("aggregate functions are not allowed in policy expressions")
        }
        ParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE => {
            Some("aggregate functions are not allowed in window RANGE")
        }
        ParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS => {
            Some("aggregate functions are not allowed in window ROWS")
        }
        ParseExprKind::EXPR_KIND_WINDOW_FRAME_GROUPS => {
            Some("aggregate functions are not allowed in window GROUPS")
        }
        ParseExprKind::EXPR_KIND_MERGE_WHEN => {
            Some("aggregate functions are not allowed in MERGE WHEN conditions")
        }
        ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT | ParseExprKind::EXPR_KIND_DOMAIN_CHECK => {
            Some("aggregate functions are not allowed in check constraints")
        }
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT | ParseExprKind::EXPR_KIND_FUNCTION_DEFAULT => {
            Some("aggregate functions are not allowed in DEFAULT expressions")
        }
        ParseExprKind::EXPR_KIND_INDEX_EXPRESSION => {
            Some("aggregate functions are not allowed in index expressions")
        }
        ParseExprKind::EXPR_KIND_INDEX_PREDICATE => {
            Some("aggregate functions are not allowed in index predicates")
        }
        ParseExprKind::EXPR_KIND_STATS_EXPRESSION => {
            Some("aggregate functions are not allowed in statistics expressions")
        }
        ParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM => {
            Some("aggregate functions are not allowed in transform expressions")
        }
        ParseExprKind::EXPR_KIND_EXECUTE_PARAMETER => {
            Some("aggregate functions are not allowed in EXECUTE parameters")
        }
        ParseExprKind::EXPR_KIND_TRIGGER_WHEN => {
            Some("aggregate functions are not allowed in trigger WHEN conditions")
        }
        ParseExprKind::EXPR_KIND_PARTITION_BOUND => {
            Some("aggregate functions are not allowed in partition bound")
        }
        ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION => {
            Some("aggregate functions are not allowed in partition key expressions")
        }
        ParseExprKind::EXPR_KIND_GENERATED_COLUMN => {
            Some("aggregate functions are not allowed in column generation expressions")
        }
        ParseExprKind::EXPR_KIND_CALL_ARGUMENT => {
            Some("aggregate functions are not allowed in CALL arguments")
        }
        ParseExprKind::EXPR_KIND_COPY_WHERE => {
            Some("aggregate functions are not allowed in COPY FROM WHERE conditions")
        }
        ParseExprKind::EXPR_KIND_WHERE
        | ParseExprKind::EXPR_KIND_FILTER
        | ParseExprKind::EXPR_KIND_INSERT_TARGET
        | ParseExprKind::EXPR_KIND_UPDATE_SOURCE
        | ParseExprKind::EXPR_KIND_UPDATE_TARGET
        | ParseExprKind::EXPR_KIND_GROUP_BY
        | ParseExprKind::EXPR_KIND_LIMIT
        | ParseExprKind::EXPR_KIND_OFFSET
        | ParseExprKind::EXPR_KIND_RETURNING
        | ParseExprKind::EXPR_KIND_MERGE_RETURNING
        | ParseExprKind::EXPR_KIND_VALUES
        | ParseExprKind::EXPR_KIND_VALUES_SINGLE
        | ParseExprKind::EXPR_KIND_CYCLE_MARK => {
            return Err(grouping_error(
                pstate,
                format!(
                    "aggregate functions are not allowed in {}",
                    parse_expr_kind_name(pstate.p_expr_kind)
                ),
                location,
                "check_agglevels_and_constraints",
            ));
        }
    };
    if let Some(msg) = err {
        return Err(grouping_error(pstate, msg.into(), location, "check_agglevels_and_constraints"));
    }
    Ok(min_varlevel as u32)
}

struct AggArgContext {
    min_varlevel: i32,
    min_agglevel: i32,
    agg_loc: ParseLoc,
}

fn check_agg_arguments<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    args: &NodeList<'mcx>,
    filter: Option<Node<'mcx>>,
    _agglocation: ParseLoc,
) -> PgResult<i32> {
    debug_assert!(filter.is_none());
    let mut ctx = AggArgContext { min_varlevel: -1, min_agglevel: -1, agg_loc: -1 };
    for node in args {
        check_agg_arguments_walker(pstate, node, &mut ctx)?;
    }

    let agglevel = match (ctx.min_varlevel, ctx.min_agglevel) {
        (-1, -1) => 0,
        (-1, a) => a,
        (v, -1) => v,
        (v, a) => v.min(a),
    };
    if agglevel == ctx.min_agglevel {
        return Err(grouping_error(
            pstate,
            "aggregate function calls cannot be nested".into(),
            ctx.agg_loc,
            "check_agg_arguments",
        ));
    }
    Ok(agglevel)
}

fn check_agg_arguments_walker<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    ctx: &mut AggArgContext,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let varlevelsup = node.as_var().unwrap().varlevelsup as i32;
            if ctx.min_varlevel < 0 || ctx.min_varlevel > varlevelsup {
                ctx.min_varlevel = varlevelsup;
            }
            Ok(())
        }
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32;
            if ctx.min_agglevel < 0 || ctx.min_agglevel > agglevelsup {
                ctx.min_agglevel = agglevelsup;
                ctx.agg_loc = agg.location;
            }
            for tle in &agg.args {
                check_agg_arguments_walker(pstate, tle, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_WindowFunc => Err(grouping_error(
            pstate,
            "aggregate function calls cannot contain window function calls".into(),
            -1,
            "check_agg_arguments_walker",
        )),
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if f.funcretset {
                return Err(srf_in_agg_error(pstate, f.location));
            }
            for arg in &f.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            if o.opretset {
                return Err(srf_in_agg_error(pstate, o.location));
            }
            for arg in &o.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_TargetEntry => {
            check_agg_arguments_walker(pstate, node.as_target_entry().unwrap().expr, ctx)
        }
        NodeTag::T_RelabelType => {
            check_agg_arguments_walker(pstate, node.as_relabel_type().unwrap().arg, ctx)
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => check_agg_arguments_walker(pstate, arg, ctx),
            None => Ok(()),
        },
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                check_agg_arguments_walker(pstate, elem, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_CaseTestExpr => Ok(()),
        other => panic!(
            "check_agg_arguments_walker (parse_agg.c): arm for {other:?} unported — \
             backend-parser-agg (Query recursion needs query_tree_walker)"
        ),
    }
}

pub fn parseCheckAggregates<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
) -> PgResult<()> {
    debug_assert!(
        pstate.p_hasAggs
            || !qry.groupClause.is_nil()
            || qry.havingQual.is_some()
            || !qry.groupingSets.is_nil()
    );
    if !qry.groupingSets.is_nil() {
        panic!(
            "parseCheckAggregates (parse_agg.c): expand_grouping_sets unported — \
             backend-parser-agg grouping-sets lane"
        );
    }
    if !qry.groupClause.is_nil() {
        panic!(
            "parseCheckAggregates (parse_agg.c): GROUP BY needs substitute_grouped_columns \
             + RTE_GROUP (addRangeTableEntryForGroup) — backend-parser-agg grouping lane"
        );
    }
    if qry.havingQual.is_some() {
        panic!(
            "parseCheckAggregates (parse_agg.c): HAVING walk unported — \
             backend-parser-agg having lane"
        );
    }

    for tle in &qry.targetList {
        check_ungrouped_columns(pstate, qry, tle)?;
    }
    Ok(())
}

// substitute_grouped_columns_mutator's no-GROUP-BY leg: any level-zero Var
// outside an aggregate is ungrouped.
fn check_ungrouped_columns<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup == 0 {
                return Err(ungrouped_var_error(pstate, qry, var));
            }
            Ok(())
        }
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            if agg.agglevelsup == 0 {
                return Ok(());
            }
            panic!(
                "check_ungrouped_columns (parse_agg.c): outer-level Aggref recursion \
                 unported — backend-parser-agg"
            );
        }
        NodeTag::T_TargetEntry => {
            check_ungrouped_columns(pstate, qry, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            for arg in &node.as_op_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            for arg in &node.as_func_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => {
            check_ungrouped_columns(pstate, qry, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => check_ungrouped_columns(pstate, qry, arg),
            None => Ok(()),
        },
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_CaseTestExpr => Ok(()),
        other => panic!(
            "check_ungrouped_columns (parse_agg.c): arm for {other:?} unported — \
             backend-parser-agg"
        ),
    }
}

// C parse_expr.c ParseExprKindName; only the kinds the generic 42803 message
// renders are reachable through check_agglevels_and_constraints.
fn parse_expr_kind_name(kind: ParseExprKind) -> &'static str {
    match kind {
        ParseExprKind::EXPR_KIND_WHERE | ParseExprKind::EXPR_KIND_COPY_WHERE => "WHERE",
        ParseExprKind::EXPR_KIND_FILTER => "FILTER",
        ParseExprKind::EXPR_KIND_INSERT_TARGET => "INSERT",
        ParseExprKind::EXPR_KIND_UPDATE_SOURCE | ParseExprKind::EXPR_KIND_UPDATE_TARGET => {
            "UPDATE"
        }
        ParseExprKind::EXPR_KIND_GROUP_BY => "GROUP BY",
        ParseExprKind::EXPR_KIND_LIMIT => "LIMIT",
        ParseExprKind::EXPR_KIND_OFFSET => "OFFSET",
        ParseExprKind::EXPR_KIND_RETURNING | ParseExprKind::EXPR_KIND_MERGE_RETURNING => {
            "RETURNING"
        }
        ParseExprKind::EXPR_KIND_VALUES | ParseExprKind::EXPR_KIND_VALUES_SINGLE => "VALUES",
        ParseExprKind::EXPR_KIND_CYCLE_MARK => "CYCLE",
        other => panic!(
            "ParseExprKindName (parse_expr.c): arm for {other:?} unreachable from \
             check_agglevels_and_constraints"
        ),
    }
}

#[cold]
#[inline(never)]
fn grouping_error(
    pstate: &ParseState<'_, '_>,
    msg: String,
    location: ParseLoc,
    funcname: &'static str,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_GROUPING_ERROR)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, funcname)),
    )
}

#[cold]
#[inline(never)]
fn srf_in_agg_error(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("aggregate function calls cannot contain set-returning function calls".into())
            .errhint(
                "You might be able to move the set-returning function into a LATERAL FROM item."
                    .into(),
            )
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(
                "parse_agg.c",
                0,
                "check_agg_arguments_walker",
            )),
    )
}

#[cold]
#[inline(never)]
fn ungrouped_var_error(
    pstate: &ParseState<'_, '_>,
    qry: &Query<'_>,
    var: &types_nodes::primnodes::Var<'_>,
) -> Box<PgError> {
    let rte = qry.rtable.nth(var.varno as usize - 1).as_range_tbl_entry().unwrap_or_else(|| {
        panic!("check_ungrouped_columns (parse_agg.c): varno {} has no RTE", var.varno)
    });
    let eref = rte.eref.unwrap_or_else(|| {
        panic!("check_ungrouped_columns (parse_agg.c): RTE without eref for varno {}", var.varno)
    });
    let relname = eref
        .aliasname
        .unwrap_or_else(|| panic!("check_ungrouped_columns (parse_agg.c): eref without aliasname"));
    let attname = eref
        .colnames
        .nth(var.varattno as usize - 1)
        .as_string()
        .unwrap_or_else(|| {
            panic!(
                "check_ungrouped_columns (parse_agg.c): no eref colname for attno {}",
                var.varattno
            )
        })
        .sval;
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_GROUPING_ERROR)
            .errmsg(format!(
                "column \"{relname}.{attname}\" must appear in the GROUP BY clause or be used \
                 in an aggregate function"
            ))
            .errposition(parser_errposition(pstate, var.location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, "check_ungrouped_columns")),
    )
}
