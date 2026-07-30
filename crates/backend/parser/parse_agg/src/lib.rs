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
        // Ordered-set agg: args = direct args ++ aggregated args; split them,
        // and make one sortlist entry per aggregated arg (never resjunk).
        let num_direct_args = args.len() - agg_order.len();
        let mut direct = NodeList::nil();
        let mut tlist = NodeList::nil();
        let mut attno: i16 = 1;
        for (i, arg) in args.iter().enumerate() {
            if i < num_direct_args {
                direct.lappend(mcx, arg)?;
            } else {
                let tle = Node::mk_target_entry(mcx, arg, attno, None, false)?;
                tlist.lappend(mcx, tle)?;
                attno += 1;
            }
        }
        agg.aggdirectargs = direct;
        let torder =
            parse_clause_seams::transform_agg_within_group::call(mcx, pstate, &tlist, agg_order)?;
        debug_assert!(!agg_distinct);
        agg.aggorder = torder;
        agg.aggdistinct = NodeList::nil();

        // aggargtypes from the post-processing exprs (addTargetToSortList can
        // resolve unknown literals); caller-computed arg_types unused here.
        let _ = arg_types;
        let mut argtypes = types_nodes::list::OidList::nil();
        for d in &agg.aggdirectargs {
            argtypes.lappend(mcx, nodes_core::expr_type(d))?;
        }
        for tle_node in &tlist {
            let tle = tle_node.as_target_entry().expect("tlist cell");
            if !tle.resjunk {
                argtypes.lappend(mcx, nodes_core::expr_type(tle.expr))?;
            }
        }
        agg.args = tlist;
        agg.aggargtypes = argtypes;

        agg.agglevelsup = check_agglevels_and_constraints(
            pstate,
            &agg.aggdirectargs,
            &agg.args,
            agg.aggfilter,
            agg.location,
            true,
        )?;
        return Ok(());
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

    agg.agglevelsup = check_agglevels_and_constraints(
        pstate,
        &NodeList::nil(),
        &agg.args,
        agg.aggfilter,
        agg.location,
        true,
    )?;
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
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformGroupingFunc")),
        ));
    }

    let mut result_list = NodeList::nil();
    for arg in &p.args {
        // Acceptability of the expressions is checked later
        // (finalize_grouping_exprs).
        result_list.lappend(mcx, transform_expr(mcx, pstate, arg)?)?;
    }

    let agglevelsup = check_agglevels_and_constraints(
        pstate,
        &NodeList::nil(),
        &result_list,
        None,
        p.location,
        false,
    )?;
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
    pstate: &ParseState<'_, 'mcx>,
    directargs: &NodeList<'mcx>,
    args: &NodeList<'mcx>,
    filter: Option<Node<'mcx>>,
    location: ParseLoc,
    is_agg: bool,
) -> PgResult<u32> {
    let min_varlevel = check_agg_arguments(pstate, directargs, args, filter, location)?;
    // C reassigns pstate up parentParseState min_varlevel times: p_hasAggs
    // and the clause-context checks below apply to the level the aggregate
    // semantically belongs to, not where it syntactically appears.
    let mut pstate = pstate;
    for _ in 0..min_varlevel {
        pstate = pstate
            .parentParseState
            .expect("check_agg_arguments bounds the level by the parse chain");
    }
    pstate.p_hasAggs.set(true);

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
    var_loc: ParseLoc,
    min_ctelevel: i32,
    min_cte_name: Option<&'mcx str>,
    sublevels_up: i32,
}

fn check_agg_arguments<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    directargs: &NodeList<'mcx>,
    args: &NodeList<'mcx>,
    filter: Option<Node<'mcx>>,
    agglocation: ParseLoc,
) -> PgResult<i32> {
    let mut ctx = AggArgContext {
        min_varlevel: -1,
        min_agglevel: -1,
        agg_loc: -1,
        var_loc: -1,
        min_ctelevel: -1,
        min_cte_name: None,
        sublevels_up: 0,
    };
    // C walks args + filter only here; directargs get a separate pass below
    // and do not participate in the agg's semantic-level computation.
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
    let nested_cte_error = |ctx: &AggArgContext<'mcx>| -> Box<PgError> {
        let name = ctx.min_cte_name.unwrap_or("");
        Box::new(
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
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "check_agg_arguments")),
        )
    };
    if ctx.min_ctelevel >= 0 && ctx.min_ctelevel < agglevel {
        return Err(nested_cte_error(&ctx));
    }

    // C's separate direct-arguments pass: a Var of the agg's semantic level
    // is allowed there, a lower-level Var or a same-level agg is not.
    if !directargs.is_nil() {
        ctx.min_varlevel = -1;
        ctx.min_agglevel = -1;
        ctx.min_ctelevel = -1;
        for node in directargs {
            check_agg_arguments_walker(pstate, node, &mut ctx)?;
        }
        if ctx.min_varlevel >= 0 && ctx.min_varlevel < agglevel {
            return Err(grouping_error(
                pstate,
                "outer-level aggregate cannot contain a lower-level variable in its direct \
                 arguments"
                    .into(),
                ctx.var_loc,
                "check_agg_arguments",
            ));
        }
        if ctx.min_agglevel >= 0 && ctx.min_agglevel <= agglevel {
            return Err(grouping_error(
                pstate,
                "aggregate function calls cannot be nested".into(),
                ctx.agg_loc,
                "check_agg_arguments",
            ));
        }
        if ctx.min_ctelevel >= 0 && ctx.min_ctelevel < agglevel {
            return Err(nested_cte_error(&ctx));
        }
    }
    Ok(agglevel)
}

struct CaaWalker<'a, 'b, 'p, 'mcx> {
    pstate: &'a ParseState<'p, 'mcx>,
    ctx: &'b mut AggArgContext<'mcx>,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for CaaWalker<'_, '_, '_, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        check_agg_arguments_walker(self.pstate, node, self.ctx)?;
        Ok(false)
    }
    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        caa_query(self.pstate, q, self.ctx)?;
        Ok(false)
    }
}

fn caa_query<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
    ctx: &mut AggArgContext<'mcx>,
) -> PgResult<()> {
    ctx.sublevels_up += 1;
    let r = nodes_core::query_tree_walker(
        q,
        &mut CaaWalker { pstate, ctx: &mut *ctx },
        nodes_core::QTW_EXAMINE_RTES_BEFORE,
    );
    ctx.sublevels_up -= 1;
    r.map(|_| ())
}

fn check_agg_arguments_walker<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    ctx: &mut AggArgContext<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            let varlevelsup = var.varlevelsup as i32 - ctx.sublevels_up;
            if varlevelsup >= 0 && (ctx.min_varlevel < 0 || ctx.min_varlevel > varlevelsup) {
                ctx.min_varlevel = varlevelsup;
                // locate_var_of_level(min_varlevel): first Var of the winning
                // level in traversal order (mins only tighten).
                ctx.var_loc = var.location;
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
            caa_descend(pstate, node, ctx)
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
            caa_descend(pstate, node, ctx)
        }
        NodeTag::T_WindowFunc if ctx.sublevels_up == 0 => Err(grouping_error(
            pstate,
            "aggregate function calls cannot contain window function calls".into(),
            node.as_window_func().unwrap().location,
            "check_agg_arguments_walker",
        )),
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            if f.funcretset && ctx.sublevels_up == 0 {
                return Err(srf_in_agg_error(pstate, f.location));
            }
            caa_descend(pstate, node, ctx)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            if o.opretset && ctx.sublevels_up == 0 {
                return Err(srf_in_agg_error(pstate, o.location));
            }
            caa_descend(pstate, node, ctx)
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
        NodeTag::T_Query => {
            caa_query(pstate, node.as_query().expect("tag checked"), ctx)
        }
        _ => caa_descend(pstate, node, ctx),
    }
}

fn caa_descend<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    node: Node<'mcx>,
    ctx: &mut AggArgContext<'mcx>,
) -> PgResult<()> {
    nodes_core::expression_tree_walker(node, &mut CaaWalker { pstate, ctx })?;
    Ok(())
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

pub fn parseCheckAggregates<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    qry: &mut Query<'mcx>,
) -> PgResult<()> {
    debug_assert!(
        pstate.p_hasAggs.get()
            || !qry.groupClause.is_nil()
            || qry.havingQual.is_some()
            || !qry.groupingSets.is_nil()
    );

    // gset_common: intersection of all expanded sets, seeded with the
    // smallest; restricts which grouping Vars can prove functional
    // dependencies.
    let mut gset_common: PgVec<'_, i32> = PgVec::new_in(mcx);
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

    // Group-clause exprs, join-alias-flattened as C's groupClauses list; hnvg
    // is decided on the flattened form (a merged FULL USING column is a
    // COALESCE, not a Var). common_vars is C's groupClauseCommonVars: grouping
    // Vars present in every grouping set, the only ones usable for
    // functional-dependency proofs.
    let mut grp: PgVec<'_, (Node<'mcx>, Index)> = PgVec::new_in(mcx);
    let mut common_vars: PgVec<'_, Node<'mcx>> = PgVec::new_in(mcx);
    let mut group_tles = NodeList::nil();
    let mut hnvg = false;
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
        let mut expr = tle.expr;
        if has_join_rtes {
            expr = vars::flatten_join_alias_vars(mcx, &qry.rtable, qry.jointree, None, expr)?;
        }
        if expr.as_var().is_none() {
            hnvg = true;
        } else if qry.groupingSets.is_nil()
            || gset_common.contains(&(tle.ressortgroupref as i32))
        {
            common_vars.push(expr);
        }
        group_tles.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr,
                    resno: (grp.len() + 1) as i16,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: false,
                },
            )?,
        )?;
        grp.push((expr, tle.ressortgroupref));
    }

    let hnvg = hnvg;
    let grp = grp.as_slice();

    // The RTE_GROUP entry + nsitem for the grouping step's output; the rtable
    // has already moved onto qry, so it is loaned back around the call.
    if !grp.is_empty() {
        pstate.p_rtable = core::mem::take(&mut qry.rtable);
        let nsitem = parse_relation::addRangeTableEntryForGroup(mcx, pstate, &group_tles)?;
        pstate.p_grouping_nsitem = Some(nsitem);
        qry.rtable = core::mem::take(&mut pstate.p_rtable);
        qry.hasGroupRTE = true;
    }

    for tle in &qry.targetList {
        finalize_grouping_exprs(mcx, pstate, qry, grp, has_join_rtes, hnvg, 0, tle)?;
    }
    if let Some(having) = qry.havingQual {
        finalize_grouping_exprs(mcx, pstate, qry, grp, has_join_rtes, hnvg, 0, having)?;
    }
    let mut constraint_deps: PgVec<'_, Oid> = PgVec::new_in(mcx);
    let (new_tlist, new_having) = {
        let mut ctx = SgcCtx {
            mcx,
            pstate: &*pstate,
            qry: &*qry,
            grp,
            gset_common: gset_common.as_slice(),
            has_grouping_sets: !qry.groupingSets.is_nil(),
            hnvg,
            nsitem: pstate.p_grouping_nsitem,
            common_vars: common_vars.as_slice(),
            func_grouped_rels: PgVec::new_in(mcx),
            constraint_deps: PgVec::new_in(mcx),
            sublevels_up: 0,
            in_agg_direct_args: false,
        };
        let mut new_tlist = NodeList::nil();
        for tle in &ctx.qry.targetList {
            let clause = if has_join_rtes {
                vars::flatten_join_alias_vars(mcx, &ctx.qry.rtable, ctx.qry.jointree, None, tle)?
            } else {
                tle
            };
            let substituted = sgc_mutate(&mut ctx, clause)?.unwrap_or(clause);
            new_tlist.lappend(mcx, substituted)?;
        }
        let new_having = match ctx.qry.havingQual {
            None => None,
            Some(having) => {
                let clause = if has_join_rtes {
                    vars::flatten_join_alias_vars(
                        mcx,
                        &ctx.qry.rtable,
                        ctx.qry.jointree,
                        None,
                        having,
                    )?
                } else {
                    having
                };
                Some(sgc_mutate(&mut ctx, clause)?.unwrap_or(clause))
            }
        };
        constraint_deps.extend_from_slice(&ctx.constraint_deps);
        (new_tlist, new_having)
    };
    qry.targetList = new_tlist;
    qry.havingQual = new_having;
    for &dep in constraint_deps.iter() {
        qry.constraintDeps.lappend(mcx, dep)?;
    }

    // C: per spec, aggregates can't appear in a recursive term.
    let has_self_ref_rtes = qry.rtable.iter().any(|rte_node| {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_CTE && rte.self_reference
    });
    if pstate.p_hasAggs.get() && has_self_ref_rtes {
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

#[track_caller]
#[cold]
#[inline(never)]
fn agg_in_recursive_term(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_RECURSION)
            .errmsg("aggregate functions are not allowed in a recursive query's recursive term")
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "parseCheckAggregates")),
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

#[track_caller]
#[cold]
#[inline(never)]
fn grouping_sets_limit_error(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_STATEMENT_TOO_COMPLEX)
            .errmsg("too many grouping sets present (maximum 4096)")
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "parseCheckAggregates")),
    )
}

struct FgeWalker<'a, 'b, 'g, 'p, 'mcx> {
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'p, 'mcx>,
    qry: &'b Query<'mcx>,
    grp: &'g [(Node<'mcx>, Index)],
    has_join_rtes: bool,
    hnvg: bool,
    sublevels_up: i32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for FgeWalker<'_, '_, '_, '_, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        finalize_grouping_exprs(
            self.mcx,
            self.pstate,
            self.qry,
            self.grp,
            self.has_join_rtes,
            self.hnvg,
            self.sublevels_up,
            node,
        )?;
        Ok(false)
    }
    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        fge_query(self.mcx, self.pstate, self.qry, self.grp, self.has_join_rtes, self.hnvg, self.sublevels_up, q)?;
        Ok(false)
    }
}

/// C `finalize_grouping_exprs_walker` (direct-Var Query shape, no RTE_GROUP):
/// resolve each GROUPING() argument to a group-clause ressortgroupref and
/// store the list into `grp.refs` in place.
#[allow(clippy::too_many_arguments)]
fn fge_query<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    grp: &[(Node<'mcx>, Index)],
    has_join_rtes: bool,
    hnvg: bool,
    sublevels_up: i32,
    q: &'mcx Query<'mcx>,
) -> PgResult<()> {
    let mut w =
        FgeWalker { mcx, pstate, qry, grp, has_join_rtes, hnvg, sublevels_up: sublevels_up + 1 };
    nodes_core::query_tree_walker(q, &mut w, 0)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finalize_grouping_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    grp: &[(Node<'mcx>, Index)],
    has_join_rtes: bool,
    hnvg: bool,
    sublevels_up: i32,
    node: Node<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Const | NodeTag::T_Param => Ok(()),
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == sublevels_up {
                // Do not recurse into a same-level aggregate's normal
                // arguments, ORDER BY, or filter; only direct arguments are
                // checked as though outside the aggregate.
                for arg in &agg.aggdirectargs {
                    finalize_grouping_exprs(mcx, pstate, qry, grp, has_join_rtes, hnvg, sublevels_up, arg)?;
                }
                return Ok(());
            }
            if agglevelsup > sublevels_up {
                return Ok(());
            }
            fge_descend(mcx, pstate, qry, grp, has_join_rtes, hnvg, sublevels_up, node)
        }
        NodeTag::T_GroupingFunc => {
            let gfn = node.as_grouping_func().unwrap();
            let agglevelsup = gfn.agglevelsup as i32;
            if agglevelsup == sublevels_up {
                let mut ref_list = types_nodes::IntList::nil();
                for expr in &gfn.args {
                    // Each argument must match a grouping entry at the current
                    // query level; no functional dependencies or outer
                    // references.
                    let expr = if has_join_rtes {
                        vars::flatten_join_alias_vars(mcx, &qry.rtable, qry.jointree, None, expr)?
                    } else {
                        expr
                    };
                    let r#ref = if let Some(var) = expr.as_var() {
                        if var.varlevelsup as i32 == sublevels_up {
                            grouping_var_ref(grp, var)
                        } else {
                            None
                        }
                    } else if hnvg && sublevels_up == 0 {
                        grouping_expr_ref(grp, expr)
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
                // is finalizing; the `gfn` borrow above is dead before this
                // write.
                unsafe {
                    node.with_mut::<GroupingFunc, _>(|g| g.refs = ref_list).unwrap();
                }
            }
            if agglevelsup > sublevels_up {
                return Ok(());
            }
            fge_descend(mcx, pstate, qry, grp, has_join_rtes, hnvg, sublevels_up, node)
        }
        NodeTag::T_Query => fge_query(
            mcx,
            pstate,
            qry,
            grp,
            has_join_rtes,
            hnvg,
            sublevels_up,
            node.as_query().expect("tag checked"),
        ),
        _ => fge_descend(mcx, pstate, qry, grp, has_join_rtes, hnvg, sublevels_up, node),
    }
}

#[allow(clippy::too_many_arguments)]
fn fge_descend<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    qry: &Query<'mcx>,
    grp: &[(Node<'mcx>, Index)],
    has_join_rtes: bool,
    hnvg: bool,
    sublevels_up: i32,
    node: Node<'mcx>,
) -> PgResult<()> {
    let mut w = FgeWalker { mcx, pstate, qry, grp, has_join_rtes, hnvg, sublevels_up };
    nodes_core::expression_tree_walker(node, &mut w)?;
    Ok(())
}

// The equal() leg of the GROUPING()-argument match (have_non_var_grouping).
fn grouping_expr_ref(grp: &[(Node<'_>, Index)], expr: Node<'_>) -> Option<Index> {
    for (gexpr, sortgroupref) in grp {
        if types_nodes::equal(*gexpr, expr) {
            return Some(*sortgroupref);
        }
    }
    None
}

// The Var leg of the GROUPING()-argument match against group-clause TLEs.
fn grouping_var_ref(grp: &[(Node<'_>, Index)], var: &types_nodes::primnodes::Var<'_>) -> Option<Index> {
    for (gexpr, sortgroupref) in grp {
        if let Some(gvar) = gexpr.as_var() {
            if gvar.varno == var.varno
                && gvar.varattno == var.varattno
                && gvar.varlevelsup == 0
            {
                return Some(*sortgroupref);
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

// substitute_grouped_columns (parse_agg.c:1335-1590): grouped expressions in
// the targetlist and HAVING become Vars referencing the RTE_GROUP entry
// (varnullingrels marked for columns absent from some grouping set);
// ungrouped local Vars are an error unless functionally dependent on the
// common grouping Vars.
struct SgcCtx<'a, 'g, 'p, 'mcx> {
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'p, 'mcx>,
    qry: &'a Query<'mcx>,
    grp: &'g [(Node<'mcx>, Index)],
    gset_common: &'g [i32],
    has_grouping_sets: bool,
    hnvg: bool,
    nsitem: Option<&'mcx parser_small1::ParseNamespaceItem<'mcx>>,
    common_vars: &'g [Node<'mcx>],
    func_grouped_rels: PgVec<'mcx, i32>,
    constraint_deps: PgVec<'mcx, Oid>,
    sublevels_up: i32,
    in_agg_direct_args: bool,
}

// buildGroupedVar (parse_agg.c:1595-1621).
fn build_grouped_var<'mcx>(
    ctx: &SgcCtx<'_, '_, '_, 'mcx>,
    attnum: usize,
    sortgroupref: Index,
) -> PgResult<Node<'mcx>> {
    let nsitem = ctx.nsitem.expect("grouping nsitem built");
    let nscol = &nsitem.p_nscolumns[attnum - 1];
    debug_assert!(nscol.p_varno == nsitem.p_rtindex as Index);
    debug_assert!(nscol.p_varattno as usize == attnum);
    let varnullingrels = if ctx.has_grouping_sets
        && !ctx.gset_common.contains(&(sortgroupref as i32))
    {
        types_nodes::Bitmapset::make_singleton(ctx.mcx, nsitem.p_rtindex)?
    } else {
        types_nodes::Bitmapset::empty()
    };
    Node::mk(
        ctx.mcx,
        types_nodes::primnodes::Var {
            varno: nscol.p_varno as i32,
            varattno: nscol.p_varattno,
            vartype: nscol.p_vartype,
            vartypmod: nscol.p_vartypmod,
            varcollid: nscol.p_varcollid,
            varnullingrels,
            varlevelsup: ctx.sublevels_up as Index,
            varreturningtype: nscol.p_varreturningtype,
            varnosyn: nscol.p_varnosyn,
            varattnosyn: nscol.p_varattnosyn,
            location: -1,
        },
    )
}

/// None = unchanged (caller keeps the original list).
fn sgc_list<'mcx>(
    ctx: &mut SgcCtx<'_, '_, '_, 'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out: Vec<Node<'mcx>> = Vec::with_capacity(list.len());
    for item in list.iter() {
        match sgc_mutate(ctx, item)? {
            Some(new) => {
                changed = true;
                out.push(new);
            }
            None => out.push(item),
        }
    }
    if !changed {
        return Ok(None);
    }
    let mut l = NodeList::nil();
    for n in out {
        l.lappend(ctx.mcx, n)?;
    }
    Ok(Some(l))
}

fn sgc_mutate_opt<'mcx>(
    ctx: &mut SgcCtx<'_, '_, '_, 'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => sgc_mutate(ctx, n),
    }
}

// substitute_grouped_columns_mutator (parse_agg.c:1380-1590); None =
// unchanged. Sub-Queries are rewritten in place (exclusive parse tree), so
// their handles never change.
fn sgc_mutate<'mcx>(
    ctx: &mut SgcCtx<'_, '_, '_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    // C order: Aggref/GroupingFunc level gates first, then the hnvg equal()
    // match (outer query level only), then Const/Param, then the Var check.
    match node.node_tag() {
        NodeTag::T_Aggref => {
            let agg = node.as_aggref().unwrap();
            let agglevelsup = agg.agglevelsup as i32;
            if agglevelsup == ctx.sublevels_up {
                // Same-level agg: only direct arguments carry grouped Vars.
                debug_assert!(!ctx.in_agg_direct_args);
                ctx.in_agg_direct_args = true;
                let new_direct = sgc_list(ctx, &agg.aggdirectargs)?;
                ctx.in_agg_direct_args = false;
                if let Some(l) = new_direct {
                    // SAFETY: exclusive parse tree; no derived refs live.
                    unsafe { node.with_mut::<Aggref, _>(|a| a.aggdirectargs = l) }
                        .expect("Aggref");
                }
                return Ok(None);
            }
            if agglevelsup > ctx.sublevels_up {
                return Ok(None);
            }
        }
        // A current-or-higher-level GroupingFunc's arguments are not
        // evaluated; not recursed into.
        NodeTag::T_GroupingFunc => {
            if node.as_grouping_func().unwrap().agglevelsup as i32 >= ctx.sublevels_up {
                return Ok(None);
            }
        }
        _ => {}
    }
    if ctx.hnvg && ctx.sublevels_up == 0 {
        for (attnum0, (gexpr, sortgroupref)) in ctx.grp.iter().enumerate() {
            if types_nodes::equal(*gexpr, node) {
                return Ok(Some(build_grouped_var(ctx, attnum0 + 1, *sortgroupref)?));
            }
        }
    }
    match node.node_tag() {
        NodeTag::T_Const | NodeTag::T_Param => Ok(None),
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup as i32 != ctx.sublevels_up {
                return Ok(None);
            }
            if !ctx.hnvg || ctx.sublevels_up != 0 {
                for (attnum0, (gexpr, sortgroupref)) in ctx.grp.iter().enumerate() {
                    if let Some(gvar) = gexpr.as_var() {
                        if gvar.varno == var.varno
                            && gvar.varattno == var.varattno
                            && gvar.varlevelsup == 0
                        {
                            return Ok(Some(build_grouped_var(
                                ctx,
                                attnum0 + 1,
                                *sortgroupref,
                            )?));
                        }
                    }
                }
            }
            // Functional-dependency escape (last-ditch before erroring): a
            // rel whose PK is a subset of the common grouping Vars may expose
            // any column; the proof's PK constraint joins constraintDeps.
            if ctx.func_grouped_rels.contains(&var.varno) {
                return Ok(None);
            }
            let rte = ctx
                .qry
                .rtable
                .nth(var.varno as usize - 1)
                .as_range_tbl_entry()
                .expect("varno has an RTE");
            // The relid guard keeps synthetic catalog-less RTEs (unit-test
            // fixtures) away from the pg_constraint scan.
            if rte.rtekind == RTEKind::RTE_RELATION
                && rte.relid != types_core::InvalidOid
                && pg_constraint::check_functional_grouping(
                    ctx.mcx,
                    rte.relid,
                    var.varno,
                    0,
                    ctx.common_vars,
                    &mut ctx.constraint_deps,
                )?
            {
                ctx.func_grouped_rels.push(var.varno);
                return Ok(None);
            }
            Err(ungrouped_var_error(
                ctx.pstate,
                ctx.qry,
                var,
                ctx.in_agg_direct_args,
                ctx.sublevels_up,
            ))
        }
        NodeTag::T_Query => {
            ctx.sublevels_up += 1;
            sgc_query_inplace(ctx, node)?;
            ctx.sublevels_up -= 1;
            Ok(None)
        }
        // The generic engine keeps SubLink.subselect shared; grouped outer
        // Vars inside are rewritten in place before testexpr goes through it.
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            debug_assert!(sl.subselect.node_tag() == NodeTag::T_Query);
            ctx.sublevels_up += 1;
            sgc_query_inplace(ctx, sl.subselect)?;
            ctx.sublevels_up -= 1;
            nodes_core::expression_tree_mutator(ctx.mcx, node, &mut |n| sgc_mutate(ctx, n))
        }
        _ => nodes_core::expression_tree_mutator(ctx.mcx, node, &mut |n| sgc_mutate(ctx, n)),
    }
}

struct QueryNews<'mcx> {
    target: Option<NodeList<'mcx>>,
    returning: Option<NodeList<'mcx>>,
    having: Option<Node<'mcx>>,
    limit_off: Option<Node<'mcx>>,
    limit_cnt: Option<Node<'mcx>>,
    setops: Option<Node<'mcx>>,
    merge_join_cond: Option<Node<'mcx>>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
}

impl<'mcx> QueryNews<'mcx> {
    fn any(&self) -> bool {
        self.target.is_some()
            || self.returning.is_some()
            || self.having.is_some()
            || self.limit_off.is_some()
            || self.limit_cnt.is_some()
            || self.setops.is_some()
            || self.merge_join_cond.is_some()
            || self.jointree.is_some()
    }
    fn apply(self, qm: &mut Query<'mcx>) {
        if let Some(t) = self.target {
            qm.targetList = t;
        }
        if let Some(r) = self.returning {
            qm.returningList = r;
        }
        if self.having.is_some() {
            qm.havingQual = self.having;
        }
        if self.limit_off.is_some() {
            qm.limitOffset = self.limit_off;
        }
        if self.limit_cnt.is_some() {
            qm.limitCount = self.limit_cnt;
        }
        if self.setops.is_some() {
            qm.setOperations = self.setops;
        }
        if self.merge_join_cond.is_some() {
            qm.mergeJoinCondition = self.merge_join_cond;
        }
        if let Some(jt) = self.jointree {
            qm.jointree = Some(jt);
        }
    }
}

// The sub-Query arm of substitute_grouped_columns_mutator: C's
// query_tree_mutator (nodeFuncs.c, flags 0) applied through the Query node
// handle; nested structures rewrite in place through their own nodes.
fn sgc_query_inplace<'mcx>(
    ctx: &mut SgcCtx<'_, '_, '_, 'mcx>,
    qnode: Node<'mcx>,
) -> PgResult<()> {
    let q = qnode.as_query().expect("Query");
    let news = sgc_query_news(ctx, q)?;
    if news.any() {
        // SAFETY: exclusive parse tree; no derived refs live.
        unsafe { qnode.with_mut::<Query, _>(|qm| news.apply(qm)) }.expect("Query");
    }
    Ok(())
}

fn sgc_query_news<'mcx>(
    ctx: &mut SgcCtx<'_, '_, '_, 'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<QueryNews<'mcx>> {
    let mcx = ctx.mcx;
    let new_target = sgc_list(ctx, &q.targetList)?;
    let new_returning = sgc_list(ctx, &q.returningList)?;
    let new_having = sgc_mutate_opt(ctx, q.havingQual)?;
    let new_limit_off = sgc_mutate_opt(ctx, q.limitOffset)?;
    let new_limit_cnt = sgc_mutate_opt(ctx, q.limitCount)?;
    let new_setops = sgc_mutate_opt(ctx, q.setOperations)?;
    let new_merge_join_cond = sgc_mutate_opt(ctx, q.mergeJoinCondition)?;
    for wco_node in &q.withCheckOptions {
        let wco = wco_node.as_with_check_option().expect("withCheckOptions cell");
        if let Some(new_qual) = sgc_mutate_opt(ctx, wco.qual)? {
            // SAFETY: exclusive parse tree; no derived refs live.
            unsafe {
                wco_node.with_mut::<types_nodes::parsenodes::WithCheckOption, _>(|w| {
                    w.qual = Some(new_qual)
                })
            }
            .expect("WithCheckOption");
        }
    }
    for action_node in &q.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        let new_qual = sgc_mutate_opt(ctx, action.qual)?;
        let new_tlist = sgc_list(ctx, &action.targetList)?;
        if new_qual.is_some() || new_tlist.is_some() {
            // SAFETY: as above.
            unsafe {
                action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                    if new_qual.is_some() {
                        a.qual = new_qual;
                    }
                    if let Some(t) = new_tlist {
                        a.targetList = t;
                    }
                })
            }
            .expect("MergeAction");
        }
    }
    if let Some(oc_node) = q.onConflict {
        let oc = oc_node.as_on_conflict_expr().expect("onConflict");
        let new_arbiter = sgc_list(ctx, &oc.arbiterElems)?;
        let new_arb_where = sgc_mutate_opt(ctx, oc.arbiterWhere)?;
        let new_set = sgc_list(ctx, &oc.onConflictSet)?;
        let new_where = sgc_mutate_opt(ctx, oc.onConflictWhere)?;
        let new_excl = sgc_list(ctx, &oc.exclRelTlist)?;
        if new_arbiter.is_some()
            || new_arb_where.is_some()
            || new_set.is_some()
            || new_where.is_some()
            || new_excl.is_some()
        {
            // SAFETY: as above.
            unsafe {
                oc_node.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                    if let Some(l) = new_arbiter {
                        o.arbiterElems = l;
                    }
                    if new_arb_where.is_some() {
                        o.arbiterWhere = new_arb_where;
                    }
                    if let Some(l) = new_set {
                        o.onConflictSet = l;
                    }
                    if new_where.is_some() {
                        o.onConflictWhere = new_where;
                    }
                    if let Some(l) = new_excl {
                        o.exclRelTlist = l;
                    }
                })
            }
            .expect("OnConflictExpr");
        }
    }
    for wc_node in &q.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        if sgc_mutate_opt(ctx, wc.startOffset)?.is_some()
            || sgc_mutate_opt(ctx, wc.endOffset)?.is_some()
        {
            // unported: C rebuilds the WindowClause when a grouped outer Var
            // sits inside a window frame offset (obscure but reachable from
            // SQL); raise a clean 0A000 until that rebuild is ported.
            return Err(Box::new(
                ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(
                        "grouped outer column references inside a window frame offset \
                         are not supported yet",
                    )
                    .into_error()
                    .with_error_location(ErrorLocation::new(
                        "parse_agg.c",
                        0,
                        "substitute_grouped_columns",
                    )),
            ));
        }
    }
    let new_jointree = match q.jointree {
        None => None,
        Some(jt) => {
            let fl = sgc_list(ctx, &jt.fromlist)?;
            let quals = sgc_mutate_opt(ctx, jt.quals)?;
            if fl.is_some() || quals.is_some() {
                Some(mcx::alloc_leak_in(
                    mcx,
                    types_nodes::primnodes::FromExpr {
                        fromlist: match fl {
                            Some(l) => l,
                            None => jt.fromlist.clone_in(mcx)?,
                        },
                        quals: match quals {
                            Some(qu) => Some(qu),
                            None => jt.quals,
                        },
                    },
                )?)
            } else {
                None
            }
        }
    };
    for cte_node in &q.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        if let Some(cq) = cte.ctequery {
            debug_assert!(cq.node_tag() == NodeTag::T_Query);
            // Route through the mutator: the Query arm rewrites in place.
            let unchanged = sgc_mutate(ctx, cq)?.is_none();
            debug_assert!(unchanged);
        }
    }
    for rte_node in &q.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_SUBQUERY => {
                if let Some(subq) = rte.subquery {
                    ctx.sublevels_up += 1;
                    let news = sgc_query_news(ctx, subq)?;
                    ctx.sublevels_up -= 1;
                    if news.any() {
                        // The RTE holds a bare &Query (no node handle), so a
                        // changed sub-Query is re-allocated, not written over.
                        let mut nq = rewrite_manip::flat_copy_query(mcx, subq)?;
                        news.apply(&mut nq);
                        let nref: &'mcx Query<'mcx> = mcx::leak_in(mcx::alloc_in(mcx, nq)?);
                        // SAFETY: exclusive parse tree; no derived refs live.
                        unsafe {
                            rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(
                                |r| r.subquery = Some(nref),
                            )
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            RTEKind::RTE_JOIN => {
                if let Some(l) = sgc_list(ctx, &rte.joinaliasvars)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.joinaliasvars = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_FUNCTION => {
                if let Some(l) = sgc_list(ctx, &rte.functions)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.functions = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_TABLEFUNC => {
                if let Some(tf) = sgc_mutate_opt(ctx, rte.tablefunc)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.tablefunc = Some(tf)
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_VALUES => {
                if let Some(l) = sgc_list(ctx, &rte.values_lists)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.values_lists = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_GROUP => {
                if let Some(l) = sgc_list(ctx, &rte.groupexprs)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.groupexprs = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            _ => {}
        }
        if let Some(l) = sgc_list(ctx, &rte.securityQuals)? {
            // SAFETY: as above.
            unsafe {
                rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.securityQuals = l
                })
            }
            .expect("RangeTblEntry");
        }
    }
    Ok(QueryNews {
        target: new_target,
        returning: new_returning,
        having: new_having,
        limit_off: new_limit_off,
        limit_cnt: new_limit_cnt,
        setops: new_setops,
        merge_join_cond: new_merge_join_cond,
        jointree: new_jointree,
    })
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

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, funcname)),
    )
}

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, funcname)),
    )
}

#[track_caller]
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

#[track_caller]
#[cold]
#[inline(never)]
fn ungrouped_var_error(
    pstate: &ParseState<'_, '_>,
    qry: &Query<'_>,
    var: &types_nodes::primnodes::Var<'_>,
    in_agg_direct_args: bool,
    sublevels_up: i32,
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
    let mut b = ereport(ERROR).errcode(ERRCODE_GROUPING_ERROR);
    if sublevels_up == 0 {
        b = b.errmsg(format!(
            "column \"{relname}.{attname}\" must appear in the GROUP BY clause or be used \
             in an aggregate function"
        ));
        if in_agg_direct_args {
            b = b.errdetail(
                "Direct arguments of an ordered-set aggregate must use only grouped columns.",
            );
        }
    } else {
        b = b.errmsg(format!(
            "subquery uses ungrouped column \"{relname}.{attname}\" from outer query"
        ));
    }
    Box::new(
        b.errposition(parser_errposition(pstate, var.location, encoding))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "check_ungrouped_columns")),
    )
}
