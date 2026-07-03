use mcx::alloc_leak_in;
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::{Node, NodeTag};
use types_pathnodes::JoinDomain;

use crate::grouping::grouping_planner;
use crate::pathnode::set_cheapest;
use crate::planmain::fetch_final_rel;
use crate::prep::{preprocess_rowmarks, remove_useless_result_rtes, replace_empty_jointree};
use crate::relnode::relids_singleton;
use crate::run::PlannerRun;

pub const EXPRKIND_QUAL: i32 = 0;
pub const EXPRKIND_TARGET: i32 = 1;
pub const EXPRKIND_RTFUNC: i32 = 2;
pub const EXPRKIND_VALUES: i32 = 4;
pub const EXPRKIND_LIMIT: i32 = 6;

// Top-level arm plus the make_subplan recursion (run.push_root pre-sets the
// child root's query_level); hasRecursion/setops stay behind the panics below.
pub fn subquery_planner<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut parse: Query<'mcx>,
    tuple_fraction: f64,
    setops: Option<&'mcx types_nodes::parsenodes::SetOperationStmt<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    if run.suspended_roots.is_empty() {
        run.root.query_level = 1;
    }
    debug_assert!(run.root.query_level >= 1);
    if parse.resultRelation != 0 {
        run.root.all_result_relids = relids_singleton(mcx, parse.resultRelation as u32);
    }
    run.root.wt_param_id = -1;
    run.root.join_domains.push(JoinDomain::default());

    if !parse.cteList.is_nil() {
        crate::cte::ss_process_ctes(run, &parse)?;
    }
    if parse.commandType == CmdType::CMD_MERGE {
        panic!("transform_MERGE_to_join (prepjointree.c): M2 MERGE lane");
    }
    replace_empty_jointree(mcx, &mut parse)?;
    if parse.hasSubLinks {
        crate::subselect::pull_up_sublinks(run, &mut parse)?;
    }
    if parse
        .rtable
        .iter()
        .any(|n| n.as_range_tbl_entry().expect("rtable cell").rtekind == RTEKind::RTE_SUBQUERY)
    {
        crate::prepjointree::pull_up_subqueries(mcx, &mut parse)?;
    }

    let mut has_outer_joins = false;
    let mut has_result_rtes = false;
    let mut join_rtes: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
    for (rti0, rte_node) in parse.rtable.iter().enumerate() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                if rte.inh {
                    // has_subclass (pg_inherits.c) reads pg_class.relhassubclass
                    // via syscache; the relcache entry carries the same field.
                    let rel = table::table_open(mcx, rte.relid, types_rel::NoLock)?;
                    let sub = rel.rd_rel.relhassubclass;
                    table::table_close(rel, types_rel::NoLock)?;
                    if sub {
                        panic!("expand_inherited_rtes (inherit.c): M2 scan lane");
                    }
                    // SAFETY: pre-seal Query owned by this invocation; the
                    // shared `rte` borrow is not read past this write.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.inh = false
                        })
                    };
                }
                if rte.tablesample.is_some() {
                    panic!("preprocess_expression (planner.c): TABLESAMPLE; M2 lane");
                }
            }
            RTEKind::RTE_RESULT => has_result_rtes = true,
            RTEKind::RTE_JOIN => {
                run.root.hasJoinRTEs = true;
                join_rtes.push(rti0 as i32 + 1);
                if rte.jointype != types_nodes::jointype::JoinType::JOIN_INNER {
                    has_outer_joins = true;
                }
            }
            RTEKind::RTE_SUBQUERY => {
                // Simple subqueries were pulled up above (non-pullable ones
                // panicked there); only dangling flattened RTEs remain --
                // except set-operation leaves, which live outside the jointree.
                debug_assert!(rte.subquery.is_none() || parse.setOperations.is_some());
            }
            RTEKind::RTE_FUNCTION => {
                // preprocess_function_rtes: inline_set_returning_function is a
                // no-op for non-SQL-language functions and non-builtins cannot
                // resolve on this lane; EXPRKIND_RTFUNC preprocess_expression
                // skipped (grammar-Const args).
            }
            RTEKind::RTE_VALUES => {
                assert!(!rte.lateral, "preprocess_expression (planner.c): EXPRKIND_VALUES_LATERAL; M2 lateral lane");
                let lists = preprocess_expression_list(
                    run,
                    rte.values_lists.clone_in(mcx)?,
                    EXPRKIND_VALUES,
                    parse.hasSubLinks,
                )?;
                // SAFETY: as the RTE_RELATION arm above.
                unsafe {
                    rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                        r.values_lists = lists
                    })
                };
            }
            RTEKind::RTE_TABLEFUNC => {
                panic!("preprocess_function_rtes (prepjointree.c): {:?}; M2 lane", rte.rtekind)
            }
            RTEKind::RTE_CTE => {
                assert!(
                    !rte.self_reference,
                    "subquery_planner (planner.c): recursive self-reference; M2 recursive-CTE lane"
                );
            }
            RTEKind::RTE_NAMEDTUPLESTORE => {
                panic!("subquery_planner (planner.c): {:?} RTE; M2 lane", rte.rtekind)
            }
            RTEKind::RTE_GROUP => {
                panic!("subquery_planner (planner.c): RTE_GROUP survey; M2 grouping lane")
            }
        }
        if rte.lateral {
            run.root.hasLateralRTEs = true;
        }
        if !rte.securityQuals.is_nil() {
            panic!("subquery_planner (planner.c): securityQuals; M2 RLS lane");
        }
        // View perminfos flow through unchanged: ExecCheckOneRelPerms'
        // relation-level object_aclcheck arm covers relkind 'v'.
    }

    preprocess_rowmarks(&parse);
    run.root.hasHavingQual = parse.havingQual.is_some();

    let has_sublinks = parse.hasSubLinks;
    parse.targetList =
        preprocess_expression_list(run, parse.targetList, EXPRKIND_TARGET, has_sublinks)?;
    debug_assert!(parse.withCheckOptions.is_nil());
    parse.returningList =
        preprocess_expression_list(run, parse.returningList, EXPRKIND_TARGET, has_sublinks)?;
    preprocess_qual_conditions(run, &mut parse, has_sublinks)?;
    parse.havingQual =
        preprocess_expression(run, parse.havingQual, EXPRKIND_QUAL, has_sublinks)?;
    for wc_node in &parse.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        let start = preprocess_expression(run, wc.startOffset, EXPRKIND_LIMIT, has_sublinks)?;
        let end = preprocess_expression(run, wc.endOffset, EXPRKIND_LIMIT, has_sublinks)?;
        // SAFETY: parse tree is planner-owned; no derived refs live.
        unsafe {
            wc_node
                .with_mut::<types_nodes::parsenodes::WindowClause, _>(|w| {
                    w.startOffset = start;
                    w.endOffset = end;
                })
                .expect("WindowClause");
        }
    }
    parse.limitOffset =
        preprocess_expression(run, parse.limitOffset, EXPRKIND_LIMIT, has_sublinks)?;
    parse.limitCount =
        preprocess_expression(run, parse.limitCount, EXPRKIND_LIMIT, has_sublinks)?;
    if parse.onConflict.is_some() || !parse.mergeActionList.is_nil() {
        panic!("preprocess_expression (planner.c): ON CONFLICT/MERGE; M2 DML lane");
    }
    debug_assert!(run.root.append_rel_list.is_empty());
    // Per-RTE expression preprocessing: expression-bearing RTEs panicked above.

    if parse.hasGroupRTE {
        panic!("flatten_group_exprs (var.c): M2 grouping lane");
    }
    if parse.hasTargetSRFs {
        panic!("expression_returns_set (nodeFuncs.c): M2 SRF lane");
    }
    if !parse.groupingSets.is_nil() {
        panic!("expand_grouping_sets (parse_agg.c): M2 grouping lane");
    }
    if let Some(hq) = parse.havingQual {
        debug_assert!(parse.groupingSets.is_nil());
        let havinglist = hq.as_list().expect("preprocessed havingQual is a list");
        let mut new_having = NodeList::nil();
        for hc in havinglist {
            if clauses::contain_agg_clause(hc)?
                || clauses::contain_volatile_functions(hc)?
                || clauses::contain_subplans(hc)?
            {
                new_having.lappend(mcx, hc)?;
            } else if !parse.groupClause.is_nil() {
                move_qual_to_where(run, &mut parse, hc)?;
            } else {
                // Degenerate grouping: a copy goes to WHERE, the clause stays
                // in HAVING (C copyObject; the arena share is our copy model).
                move_qual_to_where(run, &mut parse, hc)?;
                new_having.lappend(mcx, hc)?;
            }
        }
        parse.havingQual = if new_having.is_nil() {
            None
        } else {
            Some(Node::mk_list(mcx, new_having)?)
        };
    }
    if has_outer_joins {
        crate::prepjointree::reduce_outer_joins(mcx, &mut parse)?;
    }

    // Mutation done; seal the Query (C shares root->parse by pointer).
    let sealed: &'mcx Query<'mcx> = alloc_leak_in(mcx, parse)?;
    run.root.parse = run.intern_query(sealed);

    if run.root.hasJoinRTEs {
        assert_no_join_alias_vars(sealed, &join_rtes)?;
    }

    // Deferred half of standard_planner's parallel-mode assessment (lib.rs).
    // Guarded to the top level: C scans only the top query (recursing itself);
    // a sub-level scan would clobber the verdict (Gather consumers are loud).
    if run.assess_parallel && run.root.query_level == 1 {
        run.glob.max_parallel_hazard = clauses::max_parallel_hazard(sealed)?;
        run.glob.parallel_mode_ok = run.glob.max_parallel_hazard != crate::PROPARALLEL_UNSAFE;
    }
    run.glob.parallel_mode_needed = run.glob.parallel_mode_ok
        && crate::gucs::debug_parallel_query() != guc_tables::consts::DEBUG_PARALLEL_OFF;

    if has_result_rtes {
        remove_useless_result_rtes(run, sealed);
    }

    grouping_planner(run, tuple_fraction, setops)?;

    // SS_identify_outer_params ran at run.push_root (see run.rs); correlated
    // plan_params cannot exist on this lane.
    debug_assert!(run.root.plan_params.is_empty());

    let final_rel = fetch_final_rel(run);
    crate::subselect::ss_charge_for_initplans(run, final_rel)?;
    set_cheapest(run, final_rel)?;
    Ok(())
}

pub fn preprocess_expression<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Option<Node<'mcx>>,
    kind: i32,
    has_sublinks: bool,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(mut expr) = expr else { return Ok(None) };

    // flatten_join_alias_vars: INNER JOIN ... ON produces no join-alias Vars
    // (join nscolumns reference the base rels), so C's rewrite is the identity
    // here; the post-seal assert_no_join_alias_vars sweep keeps the merged
    // USING/NATURAL and whole-row shapes loud.
    if kind != EXPRKIND_RTFUNC {
        expr = clauses::eval_const_expressions_with_params(
            run.mcx,
            expr,
            run.glob.bound_params,
        )?;
    }
    if kind == EXPRKIND_QUAL {
        expr = crate::prepqual::canonicalize_qual(run.mcx, expr, false)?;
    }
    if kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET {
        clauses::convert_saop_to_hashed_saop(expr)?;
    }
    if has_sublinks {
        expr = crate::subselect::ss_process_sublinks(run, expr, kind == EXPRKIND_QUAL)?;
    }
    if run.root.query_level > 1 {
        expr = crate::subselect::ss_replace_correlation_vars(expr)?;
    }
    // make_ands_implicit runs last in C; constant TRUE reduces to None.
    if kind == EXPRKIND_QUAL {
        let list = clauses::make_ands_implicit(run.mcx, Some(expr))?;
        if list.is_nil() {
            return Ok(None);
        }
        expr = Node::mk_list(run.mcx, list)?;
    }
    Ok(Some(expr))
}

fn preprocess_expression_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: NodeList<'mcx>,
    kind: i32,
    has_sublinks: bool,
) -> PgResult<NodeList<'mcx>> {
    if list.is_nil() {
        return Ok(list);
    }
    let node = Node::mk_list(run.mcx, list)?;
    let folded =
        preprocess_expression(run, Some(node), kind, has_sublinks)?.expect("list in, list out");
    match folded.node_tag() {
        // clone_in copies the 8-byte cells, mirroring C's mutator list_copy.
        NodeTag::T_List => Ok(folded.as_list().unwrap().clone_in(run.mcx)?),
        other => panic!("preprocess_expression: list folded to {other:?}"),
    }
}

// The shared FromExpr is rebuilt to carry the lappended implicit-AND list.
fn move_qual_to_where<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    havingclause: Node<'mcx>,
) -> PgResult<()> {
    let f = parse.jointree.expect("jointree is a FromExpr");
    let mut quals = match f.quals {
        Some(q) => q.as_list().expect("preprocessed quals are a list").clone_in(run.mcx)?,
        None => NodeList::nil(),
    };
    quals.lappend(run.mcx, havingclause)?;
    parse.jointree = Some(alloc_leak_in(
        run.mcx,
        types_nodes::primnodes::FromExpr {
            fromlist: f.fromlist.clone_in(run.mcx)?,
            quals: Some(Node::mk_list(run.mcx, quals)?),
        },
    )?);
    Ok(())
}

// C mutates jointree quals in place; the FromExpr/JoinExpr nodes are shared
// here, so rebuilt equivalents carry the preprocessed quals.
fn preprocess_qual_conditions<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    has_sublinks: bool,
) -> PgResult<()> {
    let f = parse.jointree.expect("jointree is a FromExpr");
    let mut fromlist = types_nodes::list::NodeList::nil();
    for child in &f.fromlist {
        fromlist.lappend(run.mcx, preprocess_jointree_quals(run, child, has_sublinks)?)?;
    }
    let quals = preprocess_expression(run, f.quals, EXPRKIND_QUAL, has_sublinks)?;
    parse.jointree = Some(alloc_leak_in(
        run.mcx,
        types_nodes::primnodes::FromExpr { fromlist, quals },
    )?);
    Ok(())
}

fn preprocess_jointree_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    has_sublinks: bool,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(node),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().expect("FromExpr");
            let mut fromlist = types_nodes::list::NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(run.mcx, preprocess_jointree_quals(run, child, has_sublinks)?)?;
            }
            let quals = preprocess_expression(run, f.quals, EXPRKIND_QUAL, has_sublinks)?;
            Node::mk(
                run.mcx,
                types_nodes::primnodes::FromExpr { fromlist, quals },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().expect("JoinExpr");
            let larg = preprocess_jointree_quals(run, j.larg, has_sublinks)?;
            let rarg = preprocess_jointree_quals(run, j.rarg, has_sublinks)?;
            let quals = preprocess_expression(run, j.quals, EXPRKIND_QUAL, has_sublinks)?;
            Node::mk(
                run.mcx,
                types_nodes::JoinExpr {
                    jointype: j.jointype,
                    isNatural: j.isNatural,
                    larg,
                    rarg,
                    usingClause: j.usingClause.clone_in(run.mcx)?,
                    join_using_alias: j.join_using_alias,
                    quals,
                    alias: j.alias,
                    rtindex: j.rtindex,
                },
            )
        }
        other => panic!("preprocess_qual_conditions (planner.c): {other:?}; M2 join lane"),
    }
}

// flatten_join_alias_vars (var.c), detection form: a Var whose varno names an
// RTE_JOIN entry only arises from merged USING/NATURAL columns or a join
// whole-row reference — both unported. INNER ... ON join columns carry base
// relids, so C's rewrite is the identity on everything that parses today.
fn assert_no_join_alias_vars<'mcx>(
    sealed: &'mcx Query<'mcx>,
    join_rtes: &[i32],
) -> PgResult<()> {
    struct W<'a> {
        join_rtes: &'a [i32],
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W<'_> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                if v.varlevelsup == 0 && self.join_rtes.contains(&v.varno) {
                    panic!(
                        "flatten_join_alias_vars (var.c): join alias Var (varno {}); \
                         join-using lane",
                        v.varno
                    );
                }
                return Ok(false);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { join_rtes };
    nodes_core::query_tree_walker(sealed, &mut w, 0)?;
    Ok(())
}
