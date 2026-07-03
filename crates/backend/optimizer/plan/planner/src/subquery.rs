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
pub const EXPRKIND_LIMIT: i32 = 6;

// Top-level arm; the recursive sub-Query entry (parent_root, hasRecursion,
// setops) stays behind the SubLink/subquery panics below.
pub fn subquery_planner<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut parse: Query<'mcx>,
    tuple_fraction: f64,
) -> PgResult<()> {
    let mcx = run.mcx;
    run.root.query_level = 1;
    if parse.resultRelation != 0 {
        run.root.all_result_relids = relids_singleton(mcx, parse.resultRelation as u32);
    }
    run.root.wt_param_id = -1;
    run.root.join_domains.push(JoinDomain::default());

    if !parse.cteList.is_nil() {
        panic!("SS_process_ctes (subselect.c): M2 CTE lane");
    }
    if parse.commandType == CmdType::CMD_MERGE {
        panic!("transform_MERGE_to_join (prepjointree.c): M2 MERGE lane");
    }
    replace_empty_jointree(mcx, &mut parse)?;
    if parse.hasSubLinks {
        panic!("pull_up_sublinks (prepjointree.c): M2 sublink lane");
    }

    let mut has_outer_joins = false;
    let mut has_result_rtes = false;
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                if rte.inh {
                    panic!("has_subclass (pg_inherits.c): inh survey; M2 scan lane");
                }
                if rte.tablesample.is_some() {
                    panic!("preprocess_expression (planner.c): TABLESAMPLE; M2 lane");
                }
            }
            RTEKind::RTE_RESULT => has_result_rtes = true,
            RTEKind::RTE_JOIN => {
                run.root.hasJoinRTEs = true;
                if rte.jointype != types_nodes::jointype::JoinType::JOIN_INNER {
                    has_outer_joins = true;
                }
            }
            RTEKind::RTE_SUBQUERY => {
                panic!("pull_up_subqueries (prepjointree.c): M2 subquery lane")
            }
            RTEKind::RTE_FUNCTION | RTEKind::RTE_TABLEFUNC | RTEKind::RTE_VALUES => {
                panic!("preprocess_function_rtes (prepjointree.c): {:?}; M2 lane", rte.rtekind)
            }
            RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE => {
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
        // View-RTE ExecCheckOneRelPerms guard: needs relkind, executor lane.
        if rte.perminfoindex != 0 && rte.relkind == b'v' {
            panic!("ExecCheckOneRelPerms (execMain.c): view perms; M2 lane");
        }
    }

    preprocess_rowmarks(&parse);
    run.root.hasHavingQual = parse.havingQual.is_some();

    parse.targetList =
        preprocess_expression_list(run, parse.targetList, EXPRKIND_TARGET)?;
    debug_assert!(parse.withCheckOptions.is_nil());
    parse.returningList =
        preprocess_expression_list(run, parse.returningList, EXPRKIND_TARGET)?;
    preprocess_qual_conditions(run, &mut parse)?;
    parse.havingQual = preprocess_expression(run, parse.havingQual, EXPRKIND_QUAL)?;
    if !parse.windowClause.is_nil() {
        panic!("preprocess_expression (planner.c): window frame offsets; M2 window lane");
    }
    parse.limitOffset = preprocess_expression(run, parse.limitOffset, EXPRKIND_LIMIT)?;
    parse.limitCount = preprocess_expression(run, parse.limitCount, EXPRKIND_LIMIT)?;
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
    if parse.havingQual.is_some() {
        panic!("subquery_planner (planner.c): HAVING-to-WHERE move; M2 lane");
    }
    if has_outer_joins {
        panic!("reduce_outer_joins (prepjointree.c): M2 join lane");
    }

    // Mutation done; seal the Query (C shares root->parse by pointer).
    let sealed: &'mcx Query<'mcx> = alloc_leak_in(mcx, parse)?;
    run.root.parse = run.intern_query(sealed);

    // Deferred half of standard_planner's parallel-mode assessment (lib.rs).
    if run.assess_parallel {
        run.glob.max_parallel_hazard = clauses::max_parallel_hazard(sealed)?;
        run.glob.parallel_mode_ok = run.glob.max_parallel_hazard != crate::PROPARALLEL_UNSAFE;
    }
    run.glob.parallel_mode_needed = run.glob.parallel_mode_ok
        && crate::gucs::debug_parallel_query() != guc_tables::consts::DEBUG_PARALLEL_OFF;

    if has_result_rtes {
        remove_useless_result_rtes(run, sealed);
    }

    grouping_planner(run, tuple_fraction)?;

    // SS_identify_outer_params/SS_charge_for_initplans: no params, no initplans.
    debug_assert!(run.root.plan_params.is_empty() && run.root.init_plans.is_empty());

    let final_rel = fetch_final_rel(run);
    set_cheapest(run, final_rel)?;
    Ok(())
}

pub fn preprocess_expression<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Option<Node<'mcx>>,
    kind: i32,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(mut expr) = expr else { return Ok(None) };

    if run.root.hasJoinRTEs {
        // vars::flatten_join_alias_vars is itself a loud panic today.
        panic!("flatten_join_alias_vars (var.c): M2 join lane");
    }
    if kind != EXPRKIND_RTFUNC {
        expr = clauses::eval_const_expressions(run.mcx, expr)?;
    }
    if kind == EXPRKIND_QUAL {
        expr = canonicalize_qual(expr);
    }
    if kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET {
        clauses::convert_saop_to_hashed_saop(expr)?;
    }
    // SS_process_sublinks unreachable: hasSubLinks panicked in the survey.
    if run.root.query_level > 1 {
        panic!("SS_replace_correlation_vars (subselect.c): M2 subquery lane");
    }
    Ok(Some(expr))
}

fn preprocess_expression_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: NodeList<'mcx>,
    kind: i32,
) -> PgResult<NodeList<'mcx>> {
    if list.is_nil() {
        return Ok(list);
    }
    let node = Node::mk_list(run.mcx, list)?;
    let folded = preprocess_expression(run, Some(node), kind)?.expect("list in, list out");
    match folded.node_tag() {
        // clone_in copies the 8-byte cells, mirroring C's mutator list_copy.
        NodeTag::T_List => Ok(folded.as_list().unwrap().clone_in(run.mcx)?),
        other => panic!("preprocess_expression: list folded to {other:?}"),
    }
}

// canonicalize_qual (prepqual.c): find_duplicate_ors leaves a non-AND/OR
// clause untouched; the boolean-connective rewrites are the M2 qual lane.
fn canonicalize_qual(qual: Node<'_>) -> Node<'_> {
    match qual.node_tag() {
        NodeTag::T_BoolExpr | NodeTag::T_List => {
            panic!("find_duplicate_ors (prepqual.c): AND/OR tree; M2 qual lane")
        }
        _ => qual,
    }
}

// C mutates jointree->quals in place; the FromExpr is shared here, so an
// equivalent one carries the preprocessed quals.
fn preprocess_qual_conditions<'mcx>(run: &mut PlannerRun<'mcx>, parse: &mut Query<'mcx>) -> PgResult<()> {
    let f = parse.jointree.expect("jointree is a FromExpr");
    for child in &f.fromlist {
        match child.node_tag() {
            NodeTag::T_RangeTblRef => {}
            other => panic!("preprocess_qual_conditions (planner.c): {other:?}; M2 join lane"),
        }
    }
    let quals = preprocess_expression(run, f.quals, EXPRKIND_QUAL)?;
    parse.jointree = Some(alloc_leak_in(
        run.mcx,
        types_nodes::primnodes::FromExpr { fromlist: f.fromlist.clone_in(run.mcx)?, quals },
    )?);
    Ok(())
}
