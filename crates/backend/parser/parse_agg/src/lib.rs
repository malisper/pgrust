#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use elog::ereport;
use mcx::{Mcx, PgVec};
use parser_small1::{parser_errposition, ParseExprKind, ParseState};
use types_core::{Index, Oid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_GROUPING_ERROR,
    ERRCODE_STATEMENT_TOO_COMPLEX, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WINDOWING_ERROR, ERROR,
};
use types_nodes::parsenodes::{GroupingSet, GroupingSetKind, Query};
use types_nodes::primnodes::{Aggref, GroupingFunc, WindowFunc};
use types_nodes::rawnodes::FRAMEOPTION_DEFAULTS;
use types_nodes::{equal_opt, Node, NodeEqual, NodeList, NodeTag};

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
    let mut tlist = NodeList::nil();
    let mut attno: i16 = 1;
    for arg in args {
        let tle = Node::mk_target_entry(mcx, arg, attno, None, false)?;
        tlist.lappend(mcx, tle)?;
        attno += 1;
    }
    agg.aggdirectargs = NodeList::nil();

    let mut argtypes = types_nodes::list::OidList::nil();
    if !agg_order.is_nil() || agg_distinct {
        // ORDER BY exprs not in the arg list join tlist as resjunk entries,
        // numbered from attno via p_next_resno.
        let save_next_resno = pstate.p_next_resno;
        pstate.p_next_resno = attno as i32;
        let (torder, tdistinct, tlist_argtypes) =
            parse_clause_seams::transform_agg_order_distinct::call(
                mcx,
                pstate,
                &mut tlist,
                agg_order,
                agg_distinct,
            )?;
        pstate.p_next_resno = save_next_resno;
        agg.aggorder = torder;
        agg.aggdistinct = tdistinct;
        for &t in tlist_argtypes.iter() {
            argtypes.lappend(mcx, t)?;
        }
    } else {
        agg.aggorder = NodeList::nil();
        agg.aggdistinct = NodeList::nil();
        // Divergence: aggargtypes from caller-computed exprType values
        // (nodeFuncs slice lives in parse_expr; parse_oper::make_op
        // precedent).
        for &t in arg_types {
            argtypes.lappend(mcx, t)?;
        }
    }
    agg.args = tlist;
    agg.aggargtypes = argtypes;

    agg.agglevelsup =
        check_agglevels_and_constraints(pstate, &agg.args, agg.aggfilter, agg.location, true)?;
    Ok(())
}

/// C `transformGroupingFunc`: GROUPING() behaves very like an aggregate
/// (levels, nesting, p_hasAggs).  `transform_expr` is the caller-supplied
/// transformExpr (parse_expr sits above this crate).
pub fn transformGroupingFunc<'p, 'mcx, F>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'p, 'mcx>,
    p: &GroupingFunc<'mcx>,
    mut transform_expr: F,
) -> PgResult<Node<'mcx>>
where
    F: FnMut(Mcx<'mcx>, &mut ParseState<'p, 'mcx>, Node<'mcx>) -> PgResult<Node<'mcx>>,
{
    if p.args.len() > 31 {
        return Err(Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
                .errmsg("GROUPING must have fewer than 32 arguments")
                .errposition(parser_errposition(pstate, p.location, mbutils::GetDatabaseEncoding()))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_agg.c", 0, "transformGroupingFunc")),
        ));
    }

    let mut result_list = NodeList::nil();
    for arg in &p.args {
        // Acceptability of the expressions is checked later
        // (finalize_grouping_exprs).
        result_list.lappend(mcx, transform_expr(mcx, pstate, arg)?)?;
    }

    let agglevelsup =
        check_agglevels_and_constraints(pstate, &result_list, None, p.location, false)?;
    Node::mk(
        mcx,
        GroupingFunc {
            args: result_list,
            refs: types_nodes::IntList::nil(),
            cols: types_nodes::IntList::nil(),
            agglevelsup,
            location: p.location,
        },
    )
}

fn check_agglevels_and_constraints<'mcx>(
    pstate: &mut ParseState<'_, 'mcx>,
    args: &NodeList<'mcx>,
    filter: Option<Node<'mcx>>,
    location: ParseLoc,
    is_agg: bool,
) -> PgResult<u32> {
    let min_varlevel = check_agg_arguments(pstate, args, filter, location)?;
    if min_varlevel > 0 {
        panic!(
            "check_agglevels_and_constraints (parse_agg.c): outer-level aggregate \
             (agglevelsup > 0) needs parentParseState hops — backend-parser-agg"
        );
    }
    pstate.p_hasAggs = true;

    // C keeps two full string tables ("aggregate functions ..." vs "grouping
    // operations ...") for translation; the rendered text is identical to
    // composing noun + context here.
    let noun = if is_agg { "aggregate functions" } else { "grouping operations" };
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
            Some("JOIN conditions")
        }
        ParseExprKind::EXPR_KIND_FROM_SUBSELECT => {
            Some("FROM clause of their own query level")
        }
        ParseExprKind::EXPR_KIND_FROM_FUNCTION => Some("functions in FROM"),
        ParseExprKind::EXPR_KIND_POLICY => Some("policy expressions"),
        ParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE => Some("window RANGE"),
        ParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS => Some("window ROWS"),
        ParseExprKind::EXPR_KIND_WINDOW_FRAME_GROUPS => Some("window GROUPS"),
        ParseExprKind::EXPR_KIND_MERGE_WHEN => Some("MERGE WHEN conditions"),
        ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT | ParseExprKind::EXPR_KIND_DOMAIN_CHECK => {
            Some("check constraints")
        }
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT | ParseExprKind::EXPR_KIND_FUNCTION_DEFAULT => {
            Some("DEFAULT expressions")
        }
        ParseExprKind::EXPR_KIND_INDEX_EXPRESSION => Some("index expressions"),
        ParseExprKind::EXPR_KIND_INDEX_PREDICATE => Some("index predicates"),
        ParseExprKind::EXPR_KIND_STATS_EXPRESSION => Some("statistics expressions"),
        ParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM => Some("transform expressions"),
        ParseExprKind::EXPR_KIND_EXECUTE_PARAMETER => Some("EXECUTE parameters"),
        ParseExprKind::EXPR_KIND_TRIGGER_WHEN => Some("trigger WHEN conditions"),
        ParseExprKind::EXPR_KIND_PARTITION_BOUND => Some("partition bound"),
        ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION => Some("partition key expressions"),
        ParseExprKind::EXPR_KIND_GENERATED_COLUMN => Some("column generation expressions"),
        ParseExprKind::EXPR_KIND_CALL_ARGUMENT => Some("CALL arguments"),
        ParseExprKind::EXPR_KIND_COPY_WHERE => Some("COPY FROM WHERE conditions"),
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
                    "{noun} are not allowed in {}",
                    parse_expr_kind_name(pstate.p_expr_kind)
                ),
                location,
                "check_agglevels_and_constraints",
            ));
        }
    };
    if let Some(what) = err {
        return Err(grouping_error(
            pstate,
            format!("{noun} are not allowed in {what}"),
            location,
            "check_agglevels_and_constraints",
        ));
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
    let mut ctx = AggArgContext { min_varlevel: -1, min_agglevel: -1, agg_loc: -1 };
    for node in args {
        check_agg_arguments_walker(pstate, node, &mut ctx)?;
    }
    if let Some(f) = filter {
        check_agg_arguments_walker(pstate, f, &mut ctx)?;
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
        // C treats GroupingFunc agglevelsup exactly like an Aggref's, then
        // descends into the subtree.
        NodeTag::T_GroupingFunc => {
            let grp = node.as_grouping_func().unwrap();
            let agglevelsup = grp.agglevelsup as i32;
            if ctx.min_agglevel < 0 || ctx.min_agglevel > agglevelsup {
                ctx.min_agglevel = agglevelsup;
                ctx.agg_loc = grp.location;
            }
            for arg in &grp.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
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
        NodeTag::T_CollateExpr => {
            check_agg_arguments_walker(pstate, node.as_collate_expr().unwrap().arg, ctx)
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
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => check_agg_arguments_walker(pstate, arg, ctx),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            for arg in &node.as_distinct_expr().unwrap().args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                check_agg_arguments_walker(pstate, elem, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            for w in &c.args {
                check_agg_arguments_walker(pstate, w, ctx)?;
            }
            match c.defresult {
                Some(d) => check_agg_arguments_walker(pstate, d, ctx),
                None => Ok(()),
            }
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            check_agg_arguments_walker(pstate, cw.expr.expect("CaseWhen.expr"), ctx)?;
            check_agg_arguments_walker(pstate, cw.result.expect("CaseWhen.result"), ctx)
        }
        NodeTag::T_CoalesceExpr => {
            for arg in &node.as_coalesce_expr().unwrap().args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for arg in &node.as_min_max_expr().unwrap().args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for arg in &node.as_scalar_array_op_expr().unwrap().args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for elem in &node.as_array_expr().unwrap().elements {
                check_agg_arguments_walker(pstate, elem, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceViaIO => {
            check_agg_arguments_walker(pstate, node.as_coerce_via_io().unwrap().arg, ctx)
        }
        NodeTag::T_CoerceToDomain => {
            check_agg_arguments_walker(pstate, node.as_coerce_to_domain().unwrap().arg, ctx)
        }
        other => panic!(
            "check_agg_arguments_walker (parse_agg.c): arm for {other:?} unported — \
             backend-parser-agg (Query recursion needs query_tree_walker)"
        ),
    }
}

pub fn transformWindowFuncCall<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    wfunc: &mut WindowFunc<'mcx>,
    windef_node: Node<'mcx>,
) -> PgResult<()> {
    let windef = windef_node.as_window_def().expect("OVER clause holds a WindowDef");

    if pstate.p_hasWindowFuncs {
        if let Some(loc) = locate_windowfunc_in_list(&wfunc.args) {
            return Err(windowing_error(
                pstate,
                "window function calls cannot be nested".into(),
                loc,
                "transformWindowFuncCall",
            ));
        }
    }

    let err: Option<&'static str> = match pstate.p_expr_kind {
        ParseExprKind::EXPR_KIND_NONE => {
            panic!("transformWindowFuncCall (parse_agg.c): EXPR_KIND_NONE cannot happen")
        }
        ParseExprKind::EXPR_KIND_OTHER
        | ParseExprKind::EXPR_KIND_SELECT_TARGET
        | ParseExprKind::EXPR_KIND_ORDER_BY
        | ParseExprKind::EXPR_KIND_DISTINCT_ON => None,
        ParseExprKind::EXPR_KIND_JOIN_ON | ParseExprKind::EXPR_KIND_JOIN_USING => {
            Some("window functions are not allowed in JOIN conditions")
        }
        ParseExprKind::EXPR_KIND_FROM_FUNCTION => {
            Some("window functions are not allowed in functions in FROM")
        }
        ParseExprKind::EXPR_KIND_POLICY => {
            Some("window functions are not allowed in policy expressions")
        }
        ParseExprKind::EXPR_KIND_WINDOW_PARTITION
        | ParseExprKind::EXPR_KIND_WINDOW_ORDER
        | ParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE
        | ParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS
        | ParseExprKind::EXPR_KIND_WINDOW_FRAME_GROUPS => {
            Some("window functions are not allowed in window definitions")
        }
        ParseExprKind::EXPR_KIND_MERGE_WHEN => {
            Some("window functions are not allowed in MERGE WHEN conditions")
        }
        ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT | ParseExprKind::EXPR_KIND_DOMAIN_CHECK => {
            Some("window functions are not allowed in check constraints")
        }
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT | ParseExprKind::EXPR_KIND_FUNCTION_DEFAULT => {
            Some("window functions are not allowed in DEFAULT expressions")
        }
        ParseExprKind::EXPR_KIND_INDEX_EXPRESSION => {
            Some("window functions are not allowed in index expressions")
        }
        ParseExprKind::EXPR_KIND_STATS_EXPRESSION => {
            Some("window functions are not allowed in statistics expressions")
        }
        ParseExprKind::EXPR_KIND_INDEX_PREDICATE => {
            Some("window functions are not allowed in index predicates")
        }
        ParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM => {
            Some("window functions are not allowed in transform expressions")
        }
        ParseExprKind::EXPR_KIND_EXECUTE_PARAMETER => {
            Some("window functions are not allowed in EXECUTE parameters")
        }
        ParseExprKind::EXPR_KIND_TRIGGER_WHEN => {
            Some("window functions are not allowed in trigger WHEN conditions")
        }
        ParseExprKind::EXPR_KIND_PARTITION_BOUND => {
            Some("window functions are not allowed in partition bound")
        }
        ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION => {
            Some("window functions are not allowed in partition key expressions")
        }
        ParseExprKind::EXPR_KIND_CALL_ARGUMENT => {
            Some("window functions are not allowed in CALL arguments")
        }
        ParseExprKind::EXPR_KIND_COPY_WHERE => {
            Some("window functions are not allowed in COPY FROM WHERE conditions")
        }
        ParseExprKind::EXPR_KIND_GENERATED_COLUMN => {
            Some("window functions are not allowed in column generation expressions")
        }
        ParseExprKind::EXPR_KIND_FROM_SUBSELECT
        | ParseExprKind::EXPR_KIND_WHERE
        | ParseExprKind::EXPR_KIND_HAVING
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
            return Err(windowing_error(
                pstate,
                format!(
                    "window functions are not allowed in {}",
                    parse_expr_kind_name(pstate.p_expr_kind)
                ),
                wfunc.location,
                "transformWindowFuncCall",
            ));
        }
    };
    if let Some(msg) = err {
        return Err(windowing_error(pstate, msg.into(), wfunc.location, "transformWindowFuncCall"));
    }

    if let Some(name) = windef.name {
        debug_assert!(
            windef.refname.is_none()
                && windef.partitionClause.is_nil()
                && windef.orderClause.is_nil()
                && windef.frameOptions == FRAMEOPTION_DEFAULTS
        );
        let mut winref = 0u32;
        let mut found = false;
        for refwin_node in &pstate.p_windowdefs {
            let refwin = refwin_node.as_window_def().expect("p_windowdefs cell");
            winref += 1;
            if refwin.name == Some(name) {
                wfunc.winref = winref;
                found = true;
                break;
            }
        }
        if !found {
            return Err(Box::new(
                ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("window \"{name}\" does not exist"))
                    .errposition(parser_errposition(
                        pstate,
                        windef.location,
                        mbutils::GetDatabaseEncoding(),
                    ))
                    .into_error()
                    .with_error_location(ErrorLocation::new(
                        "parse_agg.c",
                        0,
                        "transformWindowFuncCall",
                    )),
            ));
        }
    } else {
        let mut winref = 0u32;
        let mut found = false;
        for refwin_node in &pstate.p_windowdefs {
            let refwin = refwin_node.as_window_def().expect("p_windowdefs cell");
            winref += 1;
            let refname_match = match (refwin.refname, windef.refname) {
                (Some(a), Some(b)) => a == b,
                (None, None) => true,
                _ => false,
            };
            if !refname_match {
                continue;
            }
            if refwin.partitionClause.node_equal(&windef.partitionClause)
                && refwin.orderClause.node_equal(&windef.orderClause)
                && refwin.frameOptions == windef.frameOptions
                && equal_opt(refwin.startOffset, windef.startOffset)
                && equal_opt(refwin.endOffset, windef.endOffset)
            {
                wfunc.winref = winref;
                found = true;
                break;
            }
        }
        if !found {
            pstate.p_windowdefs.lappend(mcx, windef_node)?;
            wfunc.winref = pstate.p_windowdefs.len() as u32;
        }
    }

    pstate.p_hasWindowFuncs = true;
    Ok(())
}

// contain_windowfuncs/locate_windowfunc (rewriteManip.c) fused: first
// current-level WindowFunc's location, None if none.
pub fn locate_windowfunc_in_list(nodes: &NodeList<'_>) -> Option<ParseLoc> {
    struct W {
        loc: ParseLoc,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(wf) = node.as_window_func() {
                self.loc = wf.location;
                return Ok(true);
            }
            // C's expression_tree_walker GroupingFunc arm (unported in
            // nodes_core): walk the args.
            if let Some(g) = node.as_grouping_func() {
                return nodes_core::walk_list(&g.args, self);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { loc: -1 };
    match nodes_core::walk_list(nodes, &mut w) {
        Ok(true) => Some(w.loc),
        _ => None,
    }
}

pub fn locate_windowfunc(node: Node<'_>) -> ParseLoc {
    struct W {
        loc: ParseLoc,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(wf) = node.as_window_func() {
                self.loc = wf.location;
                return Ok(true);
            }
            // C's expression_tree_walker GroupingFunc arm (unported in
            // nodes_core): walk the args.
            if let Some(g) = node.as_grouping_func() {
                return nodes_core::walk_list(&g.args, self);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { loc: -1 };
    match nodes_core::NodeWalker::visit(&mut w, node) {
        Ok(true) => w.loc,
        _ => -1,
    }
}

pub fn contain_windowfuncs(node: Node<'_>) -> bool {
    struct W;
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == NodeTag::T_WindowFunc {
                return Ok(true);
            }
            // C's expression_tree_walker GroupingFunc arm (unported in
            // nodes_core): walk the args.
            if let Some(g) = node.as_grouping_func() {
                return nodes_core::walk_list(&g.args, self);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    matches!(nodes_core::NodeWalker::visit(&mut W, node), Ok(true))
}

/// DIVERGENCE from C 18.3: substitute_grouped_columns' RTE_GROUP rewrite
/// (grouped Vars retargeted at an RTE_GROUP entry, qry.hasGroupRTE) is not
/// performed — the Query keeps the pre-18 direct-Var shape and the planner's
/// grouping arm consumes it directly; the 42803 checks are C-equivalent.
pub fn parseCheckAggregates<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &mut Query<'mcx>,
) -> PgResult<()> {
    debug_assert!(
        pstate.p_hasAggs
            || !qry.groupClause.is_nil()
            || qry.havingQual.is_some()
            || !qry.groupingSets.is_nil()
    );

    if !qry.groupingSets.is_nil() {
        // The 4096 limit is arbitrary, bounding pathological constructs.
        let Some(gsets) = expand_grouping_sets(mcx, &qry.groupingSets, qry.groupDistinct, 4096)?
        else {
            let location = if !qry.groupClause.is_nil() {
                // C exprLocation over the SortGroupClause list is always -1.
                -1
            } else {
                grouping_sets_location(&qry.groupingSets)
            };
            return Err(grouping_sets_limit_error(pstate, location));
        };
        // gset_common (intersection seeded with the smallest set) feeds
        // functional-dependency checks and varnullingrels, both on unported
        // lanes here.
        let mut gset_common: PgVec<'_, i32> = PgVec::new_in(mcx);
        if let Some(first) = gsets.first() {
            gset_common.extend_from_slice(first);
            if !gset_common.is_empty() {
                for s in gsets.iter().skip(1) {
                    gset_common = list_intersection_int(mcx, &gset_common, s);
                    if gset_common.is_empty() {
                        break;
                    }
                }
            }
        }
        // One expanded set plus a non-empty groupClause: ditch the grouping
        // sets and pretend plain GROUP BY.
        if gsets.len() == 1 && !qry.groupClause.is_nil() {
            qry.groupingSets = NodeList::nil();
        }
    }

    let has_join_rtes = qry.rtable.iter().any(|rte| {
        rte.as_range_tbl_entry().expect("rtable cell").rtekind
            == types_nodes::parsenodes::RTEKind::RTE_JOIN
    });

    let mut hnvg = false;
    for gc_node in &qry.groupClause {
        let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
        let tle = qry
            .targetList
            .iter()
            .find(|n| {
                n.as_target_entry().expect("tlist cell").ressortgroupref == gc.tleSortGroupRef
            })
            .expect("groupClause sortgroupref has a tlist entry");
        let expr = tle.as_target_entry().unwrap().expr;
        if has_join_rtes {
            vars::flatten_join_alias_vars(qry, expr)?;
        }
        if expr.as_var().is_none() {
            hnvg = true;
        }
    }

    let hnvg = hnvg;
    for tle in &qry.targetList {
        finalize_grouping_exprs(mcx, pstate, qry, hnvg, tle)?;
    }
    for tle in &qry.targetList {
        if has_join_rtes {
            vars::flatten_join_alias_vars(qry, tle)?;
        }
        check_ungrouped_columns(pstate, qry, hnvg, tle)?;
    }
    if let Some(having) = qry.havingQual {
        finalize_grouping_exprs(mcx, pstate, qry, hnvg, having)?;
        if has_join_rtes {
            vars::flatten_join_alias_vars(qry, having)?;
        }
        check_ungrouped_columns(pstate, qry, hnvg, having)?;
    }
    Ok(())
}

// C exprLocation over the groupingSets list: first member with a location.
fn grouping_sets_location(grouping_sets: &NodeList<'_>) -> ParseLoc {
    for gs in grouping_sets {
        let loc = gs.as_grouping_set().expect("groupingSets cell").location;
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

#[cold]
#[inline(never)]
fn grouping_sets_limit_error(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_STATEMENT_TOO_COMPLEX)
            .errmsg("too many grouping sets present (maximum 4096)")
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, "parseCheckAggregates")),
    )
}

/// C `finalize_grouping_exprs_walker`, direct-Var Query shape (no RTE_GROUP,
/// no join-alias flattening, sublevels_up fixed at 0 — subqueries are loud):
/// resolve each GROUPING() argument to a group-clause ressortgroupref and
/// store the list into `grp.refs` in place.
fn finalize_grouping_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    hnvg: bool,
    node: Node<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_CaseTestExpr | NodeTag::T_Var => Ok(()),
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            if agg.agglevelsup == 0 {
                // Do not recurse into a same-level aggregate's normal
                // arguments, ORDER BY, or filter; only direct arguments are
                // checked as though outside the aggregate.
                for arg in &agg.aggdirectargs {
                    finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
                }
                return Ok(());
            }
            panic!(
                "finalize_grouping_exprs (parse_agg.c): outer-level Aggref recursion \
                 unported — backend-parser-agg"
            );
        }
        NodeTag::T_GroupingFunc => {
            let grp = node.as_grouping_func().unwrap();
            debug_assert!(grp.agglevelsup == 0);
            let mut ref_list = types_nodes::IntList::nil();
            for expr in &grp.args {
                // Each argument must match a grouping entry at the current
                // query level; no functional dependencies or outer
                // references.
                let r#ref = if hnvg {
                    grouping_expr_ref(qry, expr)
                } else {
                    expr.as_var()
                        .filter(|v| v.varlevelsup == 0)
                        .and_then(|var| grouping_var_ref(qry, var))
                };
                let Some(r#ref) = r#ref else {
                    return Err(grouping_error(
                        pstate,
                        "arguments to GROUPING must be grouping expressions of the \
                         associated query level"
                            .into(),
                        grouping_arg_location(expr),
                        "finalize_grouping_exprs",
                    ));
                };
                ref_list.lappend(mcx, r#ref as i32)?;
            }
            // SAFETY: parse analysis holds exclusive access to the tree it is
            // finalizing; the `grp` borrow above is dead before this write.
            unsafe {
                node.with_mut::<GroupingFunc, _>(|g| g.refs = ref_list).unwrap();
            }
            let grp = node.as_grouping_func().unwrap();
            for arg in &grp.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            match wf.aggfilter {
                Some(f) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, f),
                None => Ok(()),
            }
        }
        NodeTag::T_TargetEntry => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            for arg in &node.as_op_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            for arg in &node.as_func_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_CollateExpr => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, node.as_collate_expr().unwrap().arg)
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg),
            None => Ok(()),
        },
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            for arg in &node.as_distinct_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_SQLValueFunction | NodeTag::T_CoerceToDomainValue => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            for w in &c.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, w)?;
            }
            match c.defresult {
                Some(d) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, cw.expr.expect("CaseWhen.expr"))?;
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, cw.result.expect("CaseWhen.result"))
        }
        NodeTag::T_CoalesceExpr => {
            for arg in &node.as_coalesce_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for arg in &node.as_min_max_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for arg in &node.as_scalar_array_op_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for elem in &node.as_array_expr().unwrap().elements {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, elem)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceViaIO => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, node.as_coerce_via_io().unwrap().arg)
        }
        NodeTag::T_CoerceToDomain => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, elem)?;
            }
            Ok(())
        }
        other => panic!(
            "finalize_grouping_exprs (parse_agg.c): arm for {other:?} unported — \
             backend-parser-agg"
        ),
    }
}

// The equal() leg of the GROUPING()-argument match (have_non_var_grouping).
fn grouping_expr_ref(qry: &Query<'_>, expr: Node<'_>) -> Option<Index> {
    for gc_node in &qry.groupClause {
        let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
        let tle = qry
            .targetList
            .iter()
            .find(|n| {
                n.as_target_entry().expect("tlist cell").ressortgroupref == gc.tleSortGroupRef
            })
            .expect("groupClause sortgroupref has a tlist entry")
            .as_target_entry()
            .unwrap();
        if types_nodes::equal(tle.expr, expr) {
            return Some(tle.ressortgroupref);
        }
    }
    None
}

// The Var leg of the GROUPING()-argument match against group-clause TLEs.
fn grouping_var_ref(qry: &Query<'_>, var: &types_nodes::primnodes::Var<'_>) -> Option<Index> {
    for gc_node in &qry.groupClause {
        let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
        let tle = qry
            .targetList
            .iter()
            .find(|n| {
                n.as_target_entry().expect("tlist cell").ressortgroupref == gc.tleSortGroupRef
            })
            .expect("groupClause sortgroupref has a tlist entry")
            .as_target_entry()
            .unwrap();
        if let Some(gvar) = tle.expr.as_var() {
            if gvar.varno == var.varno
                && gvar.varattno == var.varattno
                && gvar.varlevelsup == 0
            {
                return Some(tle.ressortgroupref);
            }
        }
    }
    None
}

// Local slice of C exprLocation over transformed GROUPING() arguments (the
// full accessor lives in parse_expr, above this crate); -1 is C's default arm.
fn grouping_arg_location(node: Node<'_>) -> ParseLoc {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_Param => node.as_param().unwrap().location,
        NodeTag::T_Aggref => node.as_aggref().unwrap().location,
        NodeTag::T_GroupingFunc => node.as_grouping_func().unwrap().location,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().location,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().location,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().location,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().location,
        _ => -1,
    }
}

// is_var_grouped: the substitute_grouped_columns Var match, direct-Var shape.
fn is_var_grouped(qry: &Query<'_>, var: &types_nodes::primnodes::Var<'_>) -> bool {
    for gc_node in &qry.groupClause {
        let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
        let tle = qry
            .targetList
            .iter()
            .find(|n| {
                n.as_target_entry().expect("tlist cell").ressortgroupref == gc.tleSortGroupRef
            })
            .expect("groupClause sortgroupref has a tlist entry");
        if let Some(gvar) = tle.as_target_entry().unwrap().expr.as_var() {
            if gvar.varno == var.varno
                && gvar.varattno == var.varattno
                && gvar.varlevelsup == var.varlevelsup
            {
                return true;
            }
        }
    }
    false
}

// substitute_grouped_columns_mutator's 42803 check (all grouping exprs are
// Vars on this lane): a level-zero Var outside an aggregate must be grouped.
fn check_ungrouped_columns<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    hnvg: bool,
    node: Node<'mcx>,
) -> PgResult<()> {
    // With non-Var grouping exprs, any subtree equal() to one is grouped —
    // checked before the Var leg, as C ("if we didn't do it above").
    if hnvg {
        for gc_node in &qry.groupClause {
            let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
            let tle = qry
                .targetList
                .iter()
                .find(|n| {
                    n.as_target_entry().expect("tlist cell").ressortgroupref
                        == gc.tleSortGroupRef
                })
                .expect("groupClause sortgroupref has a tlist entry");
            if types_nodes::equal(tle.as_target_entry().unwrap().expr, node) {
                return Ok(());
            }
        }
    }
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup == 0 && (hnvg || !is_var_grouped(qry, var)) {
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
        // C's mutator skips a current-level GroupingFunc entirely: its
        // arguments are not evaluated, so they are not checked here.
        NodeTag::T_GroupingFunc => Ok(()),
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            match wf.aggfilter {
                Some(f) => check_ungrouped_columns(pstate, qry, hnvg, f),
                None => Ok(()),
            }
        }
        NodeTag::T_TargetEntry => {
            check_ungrouped_columns(pstate, qry, hnvg, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            for arg in &node.as_op_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            for arg in &node.as_func_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => {
            check_ungrouped_columns(pstate, qry, hnvg, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_CollateExpr => {
            check_ungrouped_columns(pstate, qry, hnvg, node.as_collate_expr().unwrap().arg)
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => check_ungrouped_columns(pstate, qry, hnvg, arg),
            None => Ok(()),
        },
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => check_ungrouped_columns(pstate, qry, hnvg, arg),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            for arg in &node.as_distinct_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            for w in &c.args {
                check_ungrouped_columns(pstate, qry, hnvg, w)?;
            }
            match c.defresult {
                Some(d) => check_ungrouped_columns(pstate, qry, hnvg, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            check_ungrouped_columns(pstate, qry, hnvg, cw.expr.expect("CaseWhen.expr"))?;
            check_ungrouped_columns(pstate, qry, hnvg, cw.result.expect("CaseWhen.result"))
        }
        NodeTag::T_CoalesceExpr => {
            for arg in &node.as_coalesce_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for arg in &node.as_min_max_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for arg in &node.as_scalar_array_op_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for elem in &node.as_array_expr().unwrap().elements {
                check_ungrouped_columns(pstate, qry, hnvg, elem)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceViaIO => {
            check_ungrouped_columns(pstate, qry, hnvg, node.as_coerce_via_io().unwrap().arg)
        }
        NodeTag::T_CoerceToDomain => {
            check_ungrouped_columns(pstate, qry, hnvg, node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                check_ungrouped_columns(pstate, qry, hnvg, elem)?;
            }
            Ok(())
        }
        other => panic!(
            "check_ungrouped_columns (parse_agg.c): arm for {other:?} unported — \
             backend-parser-agg"
        ),
    }
}

/// C `expand_groupingset_node`: one GroupingSet into its list of integer
/// grouping sets (EMPTY -> [()], SIMPLE -> [content], ROLLUP/CUBE -> the
/// expansions, SETS -> concatenated recursion).
fn expand_groupingset_node<'mcx>(
    mcx: Mcx<'mcx>,
    gs: &GroupingSet<'mcx>,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, i32>>> {
    let mut result: PgVec<'_, PgVec<'_, i32>> = PgVec::new_in(mcx);

    match gs.kind {
        GroupingSetKind::GROUPING_SET_EMPTY => result.push(PgVec::new_in(mcx)),
        GroupingSetKind::GROUPING_SET_SIMPLE => {
            let mut s = PgVec::new_in(mcx);
            collect_simple_content(&gs.content, &mut s);
            result.push(s);
        }
        GroupingSetKind::GROUPING_SET_ROLLUP => {
            let mut curgroup_size = gs.content.len();
            while curgroup_size > 0 {
                let mut current = PgVec::new_in(mcx);
                let mut i = curgroup_size;
                for n in &gs.content {
                    let gs_current = n.as_grouping_set().expect("ROLLUP content cell");
                    debug_assert!(gs_current.kind == GroupingSetKind::GROUPING_SET_SIMPLE);
                    collect_simple_content(&gs_current.content, &mut current);
                    i -= 1;
                    if i == 0 {
                        break;
                    }
                }
                result.push(current);
                curgroup_size -= 1;
            }
            result.push(PgVec::new_in(mcx));
        }
        GroupingSetKind::GROUPING_SET_CUBE => {
            let number_bits = gs.content.len();
            // The parser caps CUBE at 12 elements.
            debug_assert!(number_bits < 31);
            let num_sets = 1u32 << number_bits;
            for i in 0..num_sets {
                let mut current = PgVec::new_in(mcx);
                let mut mask = 1u32;
                for n in &gs.content {
                    let gs_current = n.as_grouping_set().expect("CUBE content cell");
                    debug_assert!(gs_current.kind == GroupingSetKind::GROUPING_SET_SIMPLE);
                    if mask & i != 0 {
                        collect_simple_content(&gs_current.content, &mut current);
                    }
                    mask <<= 1;
                }
                result.push(current);
            }
        }
        GroupingSetKind::GROUPING_SET_SETS => {
            for n in &gs.content {
                let sub =
                    expand_groupingset_node(mcx, n.as_grouping_set().expect("SETS content cell"))?;
                result.extend(sub);
            }
        }
    }

    Ok(result)
}

// SIMPLE content is a list of Integer ressortgroupref cells (transformGroupingSet).
fn collect_simple_content<'mcx>(content: &NodeList<'_>, out: &mut PgVec<'mcx, i32>) {
    for n in content {
        out.push(n.as_integer().expect("SIMPLE grouping-set content cell").ival);
    }
}

fn cmp_list_len_asc(a: &[i32], b: &[i32]) -> core::cmp::Ordering {
    a.len().cmp(&b.len())
}

fn cmp_list_len_contents_asc(a: &[i32], b: &[i32]) -> core::cmp::Ordering {
    cmp_list_len_asc(a, b).then_with(|| a.cmp(b))
}

// C list_union_int: list1's cells plus each list2 cell not already present.
fn list_union_int<'mcx>(mcx: Mcx<'mcx>, list1: &[i32], list2: &[i32]) -> PgVec<'mcx, i32> {
    let mut result: PgVec<'_, i32> = PgVec::new_in(mcx);
    result.extend_from_slice(list1);
    for &v in list2 {
        if !result.contains(&v) {
            result.push(v);
        }
    }
    result
}

// C list_intersection_int: list1 cells also present in list2, in list1 order.
fn list_intersection_int<'mcx>(mcx: Mcx<'mcx>, list1: &[i32], list2: &[i32]) -> PgVec<'mcx, i32> {
    let mut result: PgVec<'_, i32> = PgVec::new_in(mcx);
    for &v in list1 {
        if list2.contains(&v) {
            result.push(v);
        }
    }
    result
}

/// C `expand_grouping_sets`: flat list of integer grouping sets sorted
/// shortest-first (groupDistinct also dedups); `None` past `limit`.
pub fn expand_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    grouping_sets: &NodeList<'mcx>,
    group_distinct: bool,
    limit: i32,
) -> PgResult<Option<PgVec<'mcx, PgVec<'mcx, i32>>>> {
    if grouping_sets.is_nil() {
        return Ok(None);
    }

    let mut expanded_groups: PgVec<'_, PgVec<'_, PgVec<'_, i32>>> = PgVec::new_in(mcx);
    let mut numsets = 1f64;
    for gs_node in grouping_sets {
        let current =
            expand_groupingset_node(mcx, gs_node.as_grouping_set().expect("groupingSets cell"))?;
        debug_assert!(!current.is_empty());
        numsets *= current.len() as f64;
        if limit >= 0 && numsets > limit as f64 {
            return Ok(None);
        }
        expanded_groups.push(current);
    }

    // Cartesian product across the sublists, dropping duplicate members from
    // individual sets (without changing the number of sets).
    let mut result: PgVec<'_, PgVec<'_, i32>> = PgVec::new_in(mcx);
    for set in expanded_groups.first().expect("groupingSets is non-nil").iter() {
        result.push(list_union_int(mcx, &[], set));
    }
    for p in expanded_groups.iter().skip(1) {
        let mut new_result = PgVec::new_in(mcx);
        for q in result.iter() {
            for set in p.iter() {
                new_result.push(list_union_int(mcx, q, set));
            }
        }
        result = new_result;
    }

    if !group_distinct || result.len() < 2 {
        result.sort_by(|a, b| cmp_list_len_asc(a, b));
    } else {
        for set in result.iter_mut() {
            set.sort_unstable();
        }
        result.sort_by(|a, b| cmp_list_len_contents_asc(a, b));
        let mut dedup: PgVec<'_, PgVec<'_, i32>> = PgVec::new_in(mcx);
        for set in result {
            if dedup.last().is_none_or(|prev| prev.as_slice() != set.as_slice()) {
                dedup.push(set);
            }
        }
        result = dedup;
    }

    Ok(Some(result))
}

// C parse_expr.c ParseExprKindName; only the kinds the generic 42803 message
// renders are reachable through check_agglevels_and_constraints.
fn parse_expr_kind_name(kind: ParseExprKind) -> &'static str {
    match kind {
        ParseExprKind::EXPR_KIND_FROM_SUBSELECT => "FROM",
        ParseExprKind::EXPR_KIND_HAVING => "HAVING",
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
fn windowing_error(
    pstate: &ParseState<'_, '_>,
    msg: String,
    location: ParseLoc,
    funcname: &'static str,
) -> Box<PgError> {
    let encoding = mbutils::GetDatabaseEncoding();
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_WINDOWING_ERROR)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, funcname)),
    )
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
            .errmsg("aggregate function calls cannot contain set-returning function calls")
            .errhint(
                "You might be able to move the set-returning function into a LATERAL FROM item.",
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
