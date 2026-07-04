#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use elog::ereport;
use mcx::{Mcx, PgVec};
use parser_small1::{parser_errposition, ParseExprKind, ParseState};
use types_core::{Index, Oid, ParseLoc};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_GROUPING_ERROR,
    ERRCODE_INVALID_RECURSION, ERRCODE_STATEMENT_TOO_COMPLEX, ERRCODE_TOO_MANY_ARGUMENTS,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WINDOWING_ERROR, ERROR,
};
use types_nodes::parsenodes::{GroupingSet, GroupingSetKind, Query, RTEKind};
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

struct AggArgContext<'mcx> {
    min_varlevel: i32,
    min_agglevel: i32,
    agg_loc: ParseLoc,
    min_ctelevel: i32,
    min_cte_name: Option<&'mcx str>,
    sublevels_up: i32,
}

fn check_agg_arguments<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    args: &NodeList<'mcx>,
    filter: Option<Node<'mcx>>,
    agglocation: ParseLoc,
) -> PgResult<i32> {
    let mut ctx = AggArgContext {
        min_varlevel: -1,
        min_agglevel: -1,
        agg_loc: -1,
        min_ctelevel: -1,
        min_cte_name: None,
        sublevels_up: 0,
    };
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
    if ctx.min_ctelevel >= 0 && ctx.min_ctelevel < agglevel {
        let name = ctx.min_cte_name.unwrap_or("");
        return Err(Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("outer-level aggregate cannot use a nested CTE")
                .errdetail(format!("CTE \"{name}\" is below the aggregate's semantic level."))
                .errposition(parser_errposition(
                    pstate,
                    agglocation,
                    mbutils::GetDatabaseEncoding(),
                ))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_agg.c", 0, "check_agg_arguments")),
        ));
    }
    Ok(agglevel)
}

fn caa_query<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
    ctx: &mut AggArgContext<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'b, 'p, 'q, 'mcx> {
        pstate: &'a ParseState<'p, 'mcx>,
        ctx: &'b mut AggArgContext<'q>,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, '_, '_, 'mcx, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            check_agg_arguments_walker(self.pstate, node, self.ctx)?;
            Ok(false)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            caa_query(self.pstate, q, self.ctx)?;
            Ok(false)
        }
    }
    ctx.sublevels_up += 1;
    let mut w = W { pstate, ctx };
    nodes_core::query_tree_walker(q, &mut w, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
    ctx.sublevels_up -= 1;
    Ok(())
}

fn check_agg_arguments_walker<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    ctx: &mut AggArgContext<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let varlevelsup = node.as_var().unwrap().varlevelsup as i32 - ctx.sublevels_up;
            if varlevelsup >= 0 && (ctx.min_varlevel < 0 || ctx.min_varlevel > varlevelsup) {
                ctx.min_varlevel = varlevelsup;
            }
            Ok(())
        }
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32 - ctx.sublevels_up;
            if agglevelsup >= 0 && (ctx.min_agglevel < 0 || ctx.min_agglevel > agglevelsup) {
                ctx.min_agglevel = agglevelsup;
                ctx.agg_loc = agg.location;
            }
            for e in &agg.aggdirectargs {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            for tle in &agg.args {
                check_agg_arguments_walker(pstate, tle, ctx)?;
            }
            for e in &agg.aggorder {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            for e in &agg.aggdistinct {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            match agg.aggfilter {
                Some(f) => check_agg_arguments_walker(pstate, f, ctx),
                None => Ok(()),
            }
        }
        // C treats GroupingFunc agglevelsup exactly like an Aggref's, then
        // descends into the subtree.
        NodeTag::T_GroupingFunc => {
            let grp = node.as_grouping_func().unwrap();
            let agglevelsup = grp.agglevelsup as i32 - ctx.sublevels_up;
            if agglevelsup >= 0 && (ctx.min_agglevel < 0 || ctx.min_agglevel > agglevelsup) {
                ctx.min_agglevel = agglevelsup;
                ctx.agg_loc = grp.location;
            }
            for arg in &grp.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_WindowFunc if ctx.sublevels_up == 0 => Err(grouping_error(
            pstate,
            "aggregate function calls cannot contain window function calls".into(),
            node.as_window_func().unwrap().location,
            "check_agg_arguments_walker",
        )),
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            match wf.aggfilter {
                Some(f) => check_agg_arguments_walker(pstate, f, ctx),
                None => Ok(()),
            }
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if f.funcretset && ctx.sublevels_up == 0 {
                return Err(srf_in_agg_error(pstate, f.location));
            }
            for arg in &f.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            if o.opretset && ctx.sublevels_up == 0 {
                return Err(srf_in_agg_error(pstate, o.location));
            }
            for arg in &o.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            if let Some(t) = sl.testexpr {
                check_agg_arguments_walker(pstate, t, ctx)?;
            }
            let q = sl
                .subselect
                .as_query()
                .expect("SubLink.subselect is a Query after parse analysis");
            caa_query(pstate, q, ctx)
        }
        NodeTag::T_CommonTableExpr => {
            match node.as_common_table_expr().unwrap().ctequery {
                Some(cq) => {
                    let q = cq.as_query().expect("CommonTableExpr.ctequery is a Query");
                    caa_query(pstate, q, ctx)
                }
                None => Ok(()),
            }
        }
        NodeTag::T_RangeTblEntry => {
            let rte = node.as_range_tbl_entry().unwrap();
            if rte.rtekind == RTEKind::RTE_CTE {
                let ctelevelsup = rte.ctelevelsup as i32 - ctx.sublevels_up;
                if ctelevelsup >= 0
                    && (ctx.min_ctelevel < 0 || ctx.min_ctelevel > ctelevelsup)
                {
                    ctx.min_ctelevel = ctelevelsup;
                    ctx.min_cte_name = rte.eref.and_then(|e| e.aliasname);
                }
            }
            Ok(())
        }
        NodeTag::T_SortGroupClause => Ok(()),
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
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for e in sr.refupperindexpr.iter().flatten() {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            for e in sr.reflowerindexpr.iter().flatten() {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            if let Some(e) = sr.refexpr {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            if let Some(e) = sr.refassgnexpr {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceViaIO => {
            check_agg_arguments_walker(pstate, node.as_coerce_via_io().unwrap().arg, ctx)
        }
        NodeTag::T_CoerceToDomain => {
            check_agg_arguments_walker(pstate, node.as_coerce_to_domain().unwrap().arg, ctx)
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for arg in &c.args {
                check_agg_arguments_walker(pstate, arg, ctx)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => match node.as_json_is_predicate().unwrap().expr {
            Some(e) => check_agg_arguments_walker(pstate, e, ctx),
            None => Ok(()),
        },
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => check_agg_arguments_walker(pstate, e, ctx),
            None => Ok(()),
        },
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                check_agg_arguments_walker(pstate, e, ctx)?;
            }
            for v in &j.passing_values {
                check_agg_arguments_walker(pstate, v, ctx)?;
            }
            Ok(())
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
        finalize_grouping_exprs(mcx, pstate, qry, hnvg, 0, tle)?;
    }
    for tle in &qry.targetList {
        if has_join_rtes {
            vars::flatten_join_alias_vars(qry, tle)?;
        }
        check_ungrouped_columns(pstate, qry, hnvg, 0, false, tle)?;
    }
    if let Some(having) = qry.havingQual {
        finalize_grouping_exprs(mcx, pstate, qry, hnvg, 0, having)?;
        if has_join_rtes {
            vars::flatten_join_alias_vars(qry, having)?;
        }
        check_ungrouped_columns(pstate, qry, hnvg, 0, false, having)?;
    }

    // C: per spec, aggregates can't appear in a recursive term.
    let has_self_ref_rtes = qry.rtable.iter().any(|rte_node| {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_CTE && rte.self_reference
    });
    if pstate.p_hasAggs && has_self_ref_rtes {
        let location = locate_agg_of_level(qry, 0)?;
        return Err(agg_in_recursive_term(pstate, location));
    }
    Ok(())
}

// locate_agg_of_level (rewriteManip.c): parse location of the first agg of
// the given query level, or -1. The entry Query arrives as a plain reborrow,
// so its fields are walked directly (query_tree_walker wants an arena &'mcx).
fn locate_agg_of_level<'mcx>(qry: &Query<'mcx>, levelsup: Index) -> PgResult<ParseLoc> {
    let mut w = LocateAggOfLevel { agg_location: -1, sublevels_up: levelsup };
    locate_agg_walk_query_fields(qry, &mut w)?;
    Ok(w.agg_location)
}

struct LocateAggOfLevel {
    agg_location: ParseLoc,
    sublevels_up: Index,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for LocateAggOfLevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Aggref => {
                let a = node.as_aggref().expect("tag checked");
                if a.agglevelsup == self.sublevels_up && a.location >= 0 {
                    self.agg_location = a.location;
                    return Ok(true);
                }
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_GroupingFunc => {
                let g = node.as_grouping_func().expect("tag checked");
                if g.agglevelsup == self.sublevels_up && g.location >= 0 {
                    self.agg_location = g.location;
                    return Ok(true);
                }
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_Query => self.visit_query_ref(node.as_query().expect("tag checked")),
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let result = nodes_core::query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        result
    }
}

// query_tree_walker's field walk at flags == 0, over a non-arena borrow.
fn locate_agg_walk_query_fields<'mcx>(
    q: &Query<'mcx>,
    w: &mut LocateAggOfLevel,
) -> PgResult<bool> {
    if nodes_core::walk_list(&q.targetList, w)?
        || nodes_core::walk_list(&q.withCheckOptions, w)?
        || nodes_core::walk_opt(q.onConflict, w)?
        || nodes_core::walk_list(&q.mergeActionList, w)?
        || nodes_core::walk_opt(q.mergeJoinCondition, w)?
        || nodes_core::walk_list(&q.returningList, w)?
    {
        return Ok(true);
    }
    if let Some(jt) = q.jointree {
        if nodes_core::walk_list(&jt.fromlist, w)? || nodes_core::walk_opt(jt.quals, w)? {
            return Ok(true);
        }
    }
    if nodes_core::walk_opt(q.setOperations, w)?
        || nodes_core::walk_opt(q.havingQual, w)?
        || nodes_core::walk_opt(q.limitOffset, w)?
        || nodes_core::walk_opt(q.limitCount, w)?
    {
        return Ok(true);
    }
    for wc_node in &q.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause element");
        if nodes_core::walk_opt(wc.startOffset, w)? || nodes_core::walk_opt(wc.endOffset, w)? {
            return Ok(true);
        }
    }
    if nodes_core::walk_list(&q.cteList, w)? {
        return Ok(true);
    }
    nodes_core::range_table_walker(&q.rtable, w, 0)
}

#[cold]
#[inline(never)]
fn agg_in_recursive_term(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_RECURSION)
            .errmsg("aggregate functions are not allowed in a recursive query's recursive term")
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, "parseCheckAggregates")),
    )
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
fn fge_query<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    hnvg: bool,
    sublevels_up: i32,
    q: &'mcx Query<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'b, 'p, 'mcx> {
        mcx: Mcx<'mcx>,
        pstate: &'a ParseState<'p, 'mcx>,
        qry: &'b Query<'mcx>,
        hnvg: bool,
        sublevels_up: i32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, '_, '_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            finalize_grouping_exprs(
                self.mcx,
                self.pstate,
                self.qry,
                self.hnvg,
                self.sublevels_up,
                node,
            )?;
            Ok(false)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            fge_query(self.mcx, self.pstate, self.qry, self.hnvg, self.sublevels_up, q)?;
            Ok(false)
        }
    }
    let mut w = W { mcx, pstate, qry, hnvg, sublevels_up: sublevels_up + 1 };
    nodes_core::query_tree_walker(q, &mut w, 0)?;
    Ok(())
}

fn finalize_grouping_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    hnvg: bool,
    sublevels_up: i32,
    node: Node<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_CaseTestExpr | NodeTag::T_Var => Ok(()),
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == sublevels_up {
                // Do not recurse into a same-level aggregate's normal
                // arguments, ORDER BY, or filter; only direct arguments are
                // checked as though outside the aggregate.
                for arg in &agg.aggdirectargs {
                    finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
                }
                return Ok(());
            }
            if agglevelsup > sublevels_up {
                return Ok(());
            }
            for e in &agg.aggdirectargs {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e)?;
            }
            for tle in &agg.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, tle)?;
            }
            for e in &agg.aggorder {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e)?;
            }
            for e in &agg.aggdistinct {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e)?;
            }
            match agg.aggfilter {
                Some(f) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, f),
                None => Ok(()),
            }
        }
        NodeTag::T_GroupingFunc => {
            let grp = node.as_grouping_func().unwrap();
            let agglevelsup = grp.agglevelsup as i32;
            if agglevelsup == sublevels_up {
                let mut ref_list = types_nodes::IntList::nil();
                for expr in &grp.args {
                    // Each argument must match a grouping entry at the current
                    // query level; no functional dependencies or outer
                    // references.
                    let r#ref = if let Some(var) = expr.as_var() {
                        if var.varlevelsup as i32 == sublevels_up {
                            grouping_var_ref(qry, var)
                        } else {
                            None
                        }
                    } else if hnvg && sublevels_up == 0 {
                        grouping_expr_ref(qry, expr)
                    } else {
                        None
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
                // SAFETY: parse analysis holds exclusive access to the tree it
                // is finalizing; the `grp` borrow above is dead before this
                // write.
                unsafe {
                    node.with_mut::<GroupingFunc, _>(|g| g.refs = ref_list).unwrap();
                }
            }
            if agglevelsup > sublevels_up {
                return Ok(());
            }
            let grp = node.as_grouping_func().unwrap();
            for arg in &grp.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            if let Some(t) = sl.testexpr {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, t)?;
            }
            let q = sl
                .subselect
                .as_query()
                .expect("SubLink.subselect is a Query after parse analysis");
            fge_query(mcx, pstate, qry, hnvg, sublevels_up, q)
        }
        NodeTag::T_CommonTableExpr => match node.as_common_table_expr().unwrap().ctequery {
            Some(cq) => {
                let q = cq.as_query().expect("CommonTableExpr.ctequery is a Query");
                fge_query(mcx, pstate, qry, hnvg, sublevels_up, q)
            }
            None => Ok(()),
        },
        NodeTag::T_SortGroupClause => Ok(()),
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            match wf.aggfilter {
                Some(f) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, f),
                None => Ok(()),
            }
        }
        NodeTag::T_TargetEntry => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            for arg in &node.as_op_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            for arg in &node.as_func_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_CollateExpr => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, node.as_collate_expr().unwrap().arg)
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg),
            None => Ok(()),
        },
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            for arg in &node.as_distinct_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_SQLValueFunction | NodeTag::T_CoerceToDomainValue => Ok(()),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(arg) = c.arg {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            for w in &c.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, w)?;
            }
            match c.defresult {
                Some(d) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, cw.expr.expect("CaseWhen.expr"))?;
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, cw.result.expect("CaseWhen.result"))
        }
        NodeTag::T_CoalesceExpr => {
            for arg in &node.as_coalesce_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for arg in &node.as_min_max_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for arg in &node.as_scalar_array_op_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for elem in &node.as_array_expr().unwrap().elements {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, elem)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceViaIO => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, node.as_coerce_via_io().unwrap().arg)
        }
        NodeTag::T_CoerceToDomain => {
            finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, elem)?;
            }
            Ok(())
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for arg in &c.args {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, arg)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => match node.as_json_is_predicate().unwrap().expr {
            Some(e) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e),
            None => Ok(()),
        },
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e),
            None => Ok(()),
        },
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, e)?;
            }
            for v in &j.passing_values {
                finalize_grouping_exprs(mcx, pstate, qry, hnvg, sublevels_up, v)?;
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
                && gvar.varlevelsup == 0
            {
                return true;
            }
        }
    }
    false
}

fn cuc_query<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    hnvg: bool,
    sublevels_up: i32,
    in_agg_direct_args: bool,
    q: &'mcx Query<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'b, 'p, 'mcx> {
        pstate: &'a ParseState<'p, 'mcx>,
        qry: &'b Query<'mcx>,
        hnvg: bool,
        sublevels_up: i32,
        in_agg_direct_args: bool,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_, '_, '_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            check_ungrouped_columns(
                self.pstate,
                self.qry,
                self.hnvg,
                self.sublevels_up,
                self.in_agg_direct_args,
                node,
            )?;
            Ok(false)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            cuc_query(
                self.pstate,
                self.qry,
                self.hnvg,
                self.sublevels_up,
                self.in_agg_direct_args,
                q,
            )?;
            Ok(false)
        }
    }
    let mut w = W {
        pstate,
        qry,
        hnvg,
        sublevels_up: sublevels_up + 1,
        in_agg_direct_args,
    };
    nodes_core::query_tree_walker(q, &mut w, 0)?;
    Ok(())
}

// substitute_grouped_columns_mutator's 42803 check (all grouping exprs are
// Vars on this lane): an original-level Var outside an aggregate must be
// grouped.
fn check_ungrouped_columns<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    hnvg: bool,
    sublevels_up: i32,
    in_agg_direct_args: bool,
    node: Node<'mcx>,
) -> PgResult<()> {
    // With non-Var grouping exprs, any subtree equal() to one is grouped —
    // checked before the Var leg, as C ("if we didn't do it above"); outer
    // query level only.
    if hnvg && sublevels_up == 0 {
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
            if var.varlevelsup as i32 != sublevels_up {
                return Ok(());
            }
            if (!hnvg || sublevels_up != 0) && is_var_grouped(qry, var) {
                return Ok(());
            }
            Err(ungrouped_var_error(pstate, qry, var, in_agg_direct_args))
        }
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == sublevels_up {
                debug_assert!(!in_agg_direct_args);
                for arg in &agg.aggdirectargs {
                    check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, true, arg)?;
                }
                return Ok(());
            }
            if agglevelsup > sublevels_up {
                return Ok(());
            }
            for e in &agg.aggdirectargs {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e)?;
            }
            for tle in &agg.args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, tle)?;
            }
            for e in &agg.aggorder {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e)?;
            }
            for e in &agg.aggdistinct {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e)?;
            }
            match agg.aggfilter {
                Some(f) => {
                    check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, f)
                }
                None => Ok(()),
            }
        }
        // C's mutator skips a current-or-higher-level GroupingFunc entirely:
        // its arguments are not evaluated, so they are not checked here.
        NodeTag::T_GroupingFunc => {
            let grp = node.as_grouping_func().unwrap();
            if grp.agglevelsup as i32 >= sublevels_up {
                return Ok(());
            }
            for arg in &grp.args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            if let Some(t) = sl.testexpr {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, t)?;
            }
            let q = sl
                .subselect
                .as_query()
                .expect("SubLink.subselect is a Query after parse analysis");
            cuc_query(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, q)
        }
        NodeTag::T_CommonTableExpr => match node.as_common_table_expr().unwrap().ctequery {
            Some(cq) => {
                let q = cq.as_query().expect("CommonTableExpr.ctequery is a Query");
                cuc_query(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, q)
            }
            None => Ok(()),
        },
        NodeTag::T_SortGroupClause => Ok(()),
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for arg in &wf.args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            match wf.aggfilter {
                Some(f) => check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, f),
                None => Ok(()),
            }
        }
        NodeTag::T_TargetEntry => {
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            for arg in &node.as_op_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            for arg in &node.as_func_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RelabelType => {
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_CollateExpr => {
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, node.as_collate_expr().unwrap().arg)
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg),
            None => Ok(()),
        },
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            for arg in &node.as_distinct_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for arg in &node.as_row_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
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
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            for w in &c.args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, w)?;
            }
            match c.defresult {
                Some(d) => check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CaseWhen => {
            let cw = node.as_case_when().unwrap();
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, cw.expr.expect("CaseWhen.expr"))?;
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, cw.result.expect("CaseWhen.result"))
        }
        NodeTag::T_CoalesceExpr => {
            for arg in &node.as_coalesce_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for arg in &node.as_min_max_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for arg in &node.as_scalar_array_op_expr().unwrap().args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for elem in &node.as_array_expr().unwrap().elements {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, elem)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceViaIO => {
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, node.as_coerce_via_io().unwrap().arg)
        }
        NodeTag::T_CoerceToDomain => {
            check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_List => {
            for elem in node.as_list().unwrap() {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, elem)?;
            }
            Ok(())
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for arg in &c.args {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, arg)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => match node.as_json_is_predicate().unwrap().expr {
            Some(e) => check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e),
            None => Ok(()),
        },
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e),
            None => Ok(()),
        },
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, e)?;
            }
            for v in &j.passing_values {
                check_ungrouped_columns(pstate, qry, hnvg, sublevels_up, in_agg_direct_args, v)?;
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
    in_agg_direct_args: bool,
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
    let mut b = ereport(ERROR)
        .errcode(ERRCODE_GROUPING_ERROR)
        .errmsg(format!(
            "column \"{relname}.{attname}\" must appear in the GROUP BY clause or be used \
             in an aggregate function"
        ));
    if in_agg_direct_args {
        b = b.errdetail(
            "Direct arguments of an ordered-set aggregate must use only grouped columns.",
        );
    }
    Box::new(
        b.errposition(parser_errposition(pstate, var.location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_agg.c", 0, "check_ungrouped_columns")),
    )
}
