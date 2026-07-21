use mcx::alloc_leak_in;
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, WithCheckOption};
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
pub const EXPRKIND_RTFUNC_LATERAL: i32 = 3;
pub const EXPRKIND_VALUES: i32 = 4;
pub const EXPRKIND_VALUES_LATERAL: i32 = 5;
pub const EXPRKIND_LIMIT: i32 = 6;
pub const EXPRKIND_APPINFO: i32 = 7;
pub const EXPRKIND_TABLEFUNC: i32 = 8;
pub const EXPRKIND_TABLEFUNC_LATERAL: i32 = 9;
pub const EXPRKIND_ARBITER_ELEM: i32 = 10;
pub const EXPRKIND_PHV: i32 = 11;
pub const EXPRKIND_TABLESAMPLE: i32 = 12;
pub const EXPRKIND_GROUPEXPR: i32 = 13;

// Top-level arm plus the make_subplan recursion (run.push_root pre-sets the
// child root's query_level).
pub fn subquery_planner<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &'mcx mut Query<'mcx>,
    has_recursion: bool,
    tuple_fraction: f64,
    setops: Option<&'mcx types_nodes::parsenodes::SetOperationStmt<'mcx>>,
) -> PgResult<()> {
    let mcx = run.mcx;
    if run.suspended_roots.is_empty() {
        run.root.query_level = 1;
    }
    debug_assert!(run.root.query_level >= 1);
    // standard_planner's raw-tree parallel-mode assessment (planner.c:349-353):
    // must precede any preprocess_expression — SS_process_sublinks plans the
    // sublink subqueries there, and their rels read glob.parallel_mode_ok.
    if run.assess_parallel && run.root.query_level == 1 {
        run.glob.max_parallel_hazard = clauses::max_parallel_hazard(&*parse)?;
        run.glob.parallel_mode_ok = run.glob.max_parallel_hazard != crate::PROPARALLEL_UNSAFE;
        // C initializes parallelModeNeeded ONCE, in standard_planner (planner.c
        // ~430, right after parallelModeOK): false unless debug_parallel_query,
        // then flipped true by create_gather_plan/create_gather_merge_plan.
        // It must run HERE (once, before any subplanning) and not per
        // subquery_planner call: the old placement just before grouping_planner
        // re-ran on the outer query AFTER SS_process_ctes had already planned
        // CTE subplans, clobbering the flag their Gathers set — a Gather that
        // lives only inside a CTE/sublink subplan then shipped with
        // parallelModeNeeded=false and silently launched 0 workers
        // (q15probe lane: a CTE-of-grouped-agg reused twice, Workers Planned 7 / Launched 0,
        // 3.68x-of-C; notes/q15probe-lane.md).
        run.glob.parallel_mode_needed = run.glob.parallel_mode_ok
            && crate::gucs::debug_parallel_query() != guc_tables::consts::DEBUG_PARALLEL_OFF;
    }
    run.root.command_type = parse.commandType;
    if parse.resultRelation != 0 {
        run.root.all_result_relids = relids_singleton(mcx, parse.resultRelation as u32);
    }
    run.root.hasRecursion = has_recursion;
    run.root.wt_param_id = if has_recursion {
        crate::cte::assign_special_exec_param(run)?
    } else {
        -1
    };
    run.root.non_recursive_path = None;
    run.root.join_domains.push(JoinDomain::default());

    if !parse.cteList.is_nil() {
        crate::cte::ss_process_ctes(run, &*parse)?;
    }
    crate::prepjointree::transform_MERGE_to_join(mcx, &mut *parse)?;
    replace_empty_jointree(mcx, &mut *parse)?;
    if parse.hasSubLinks {
        crate::subselect::pull_up_sublinks(run, &mut *parse)?;
    }
    // One scan feeds both triggers: preprocess_function_rtes (prepjointree.c)
    // loops the rtable and no-ops without an RTE_FUNCTION entry, so gating the
    // call is semantics-preserving; SRF inlining only turns FUNCTION RTEs into
    // SUBQUERY ones, and a FUNCTION RTE already sets the pull-up trigger, so
    // the trigger computed pre-call matches main's post-call scan.
    let mut has_function_rte = false;
    let mut has_pullup_rte = false;
    for n in &parse.rtable {
        match n.as_range_tbl_entry().expect("rtable cell").rtekind {
            RTEKind::RTE_FUNCTION => {
                has_function_rte = true;
                has_pullup_rte = true;
            }
            RTEKind::RTE_SUBQUERY | RTEKind::RTE_VALUES => has_pullup_rte = true,
            _ => {}
        }
    }
    if has_function_rte {
        crate::prepjointree::preprocess_function_rtes(run, &mut *parse)?;
    }
    if has_pullup_rte {
        crate::prepjointree::pull_up_subqueries(run, &mut *parse)?;
    }
    if parse.setOperations.is_some() {
        crate::prepjointree::flatten_simple_union_all(run, &mut *parse)?;
    }
    if parse.rtable.iter().any(|n| {
        let r = n.as_range_tbl_entry().expect("rtable cell");
        r.rtekind == RTEKind::RTE_RELATION && matches!(r.relkind, b'r' | b'p')
    }) {
        crate::prepjointree::expand_virtual_generated_columns(run, &mut *parse)?;
    }

    let mut has_outer_joins = false;
    let mut has_result_rtes = false;
    // Gates for the two outlined passes below: `!rtable.is_nil()` is never
    // false here (replace_empty_jointree gives FROM-less queries an
    // RTE_RESULT), so gate on RTE content instead — SELECT 1 pays neither
    // call (select1 instruction bracket).
    let mut saw_view_rte = false;
    let mut needs_rte_expr_pre = false;
    let mut join_rtes: mcx::PgVec<'mcx, i32> = mcx::PgVec::new_in(mcx);
    for (rti0, rte_node) in parse.rtable.iter().enumerate() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                if rte.tablesample.is_some() {
                    needs_rte_expr_pre = true;
                }
                if rte.inh {
                    // has_subclass (pg_inherits.c) reads pg_class.relhassubclass
                    // via syscache; the relcache entry carries the same field.
                    let rel = table::table_open(mcx, rte.relid, types_rel::NoLock)?;
                    let sub = rel.rd_rel.relhassubclass;
                    table::table_close(rel, types_rel::NoLock)?;
                    // SAFETY: pre-seal Query owned by this invocation; the
                    // shared `rte` borrow is not read past this write.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.inh = sub
                        })
                    };
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
                // Simple subqueries were pulled up above; retained ones plan
                // recursively in set_subquery_pathlist (their expressions are
                // preprocessed inside that subroot, never here — C matches).
            }
            RTEKind::RTE_FUNCTION | RTEKind::RTE_VALUES | RTEKind::RTE_TABLEFUNC => {
                // Expression preprocessing happens in the post-tlist rtable
                // pass below (planner.c:1007) so SubPlan numbering matches C.
                needs_rte_expr_pre = true;
            }
            RTEKind::RTE_CTE => {
                // A self-reference is only legal under a recursive-union level
                // somewhere up the chain.
                debug_assert!(
                    !rte.self_reference
                        || run.root.hasRecursion
                        || run.suspended_roots.iter().any(|s| s.root.hasRecursion)
                );
            }
            // C's default arm: an ENR RTE carries no preprocessable exprs.
            RTEKind::RTE_NAMEDTUPLESTORE => {}
            RTEKind::RTE_GROUP => {
                debug_assert!(parse.hasGroupRTE);
                run.root.group_rtindex = rti0 as i32 + 1;
                needs_rte_expr_pre = true;
            }
        }
        if rte.lateral {
            run.root.hasLateralRTEs = true;
        }
        if !rte.securityQuals.is_nil() {
            run.root.qual_security_level =
                run.root.qual_security_level.max(rte.securityQuals.len() as u32);
            needs_rte_expr_pre = true;
        }
        if rte.perminfoindex != 0 && rte.relkind == b'v' {
            saw_view_rte = true;
        }
        // View perminfos flow through unchanged: ExecCheckOneRelPerms'
        // relation-level object_aclcheck arm covers relkind 'v'.
    }

    if parse.resultRelation != 0 {
        let rte = parse
            .rtable
            .nth(parse.resultRelation as usize - 1)
            .as_range_tbl_entry()
            .expect("rtable cell");
        if !rte.inh {
            run.root.leaf_result_relids = relids_singleton(mcx, parse.resultRelation as u32);
        }
    }

    if saw_view_rte {
        check_view_perms_at_planner_startup(parse)?;
    }

    preprocess_rowmarks(run, &*parse)?;
    run.root.hasHavingQual = parse.havingQual.is_some();

    let has_sublinks = parse.hasSubLinks;
    let tlist = core::mem::replace(&mut parse.targetList, NodeList::nil());
    parse.targetList =
        preprocess_expression_list(run, &parse.rtable, parse.jointree, tlist, EXPRKIND_TARGET, has_sublinks)?;
    if !parse.withCheckOptions.is_nil() {
        let mut new_wcos = NodeList::nil();
        for wco_node in &parse.withCheckOptions {
            let wco_qual =
                wco_node.as_with_check_option().expect("withCheckOptions cell").qual;
            let qual = preprocess_expression(run, &parse.rtable, parse.jointree, wco_qual, EXPRKIND_QUAL, has_sublinks)?;
            // SAFETY: parse tree is planner-owned; no derived refs live.
            unsafe {
                wco_node.with_mut::<WithCheckOption, _>(|w| w.qual = qual)
            }
            .expect("WithCheckOption node");
            if qual.is_some() {
                new_wcos.lappend(run.mcx, wco_node)?;
            }
        }
        parse.withCheckOptions = new_wcos;
    }
    let rlist = core::mem::replace(&mut parse.returningList, NodeList::nil());
    parse.returningList =
        preprocess_expression_list(run, &parse.rtable, parse.jointree, rlist, EXPRKIND_TARGET, has_sublinks)?;
    preprocess_qual_conditions(run, &mut *parse, has_sublinks)?;
    parse.havingQual =
        preprocess_expression(run, &parse.rtable, parse.jointree, parse.havingQual, EXPRKIND_QUAL, has_sublinks)?;
    for wc_node in &parse.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        let start = preprocess_expression(run, &parse.rtable, parse.jointree, wc.startOffset, EXPRKIND_LIMIT, has_sublinks)?;
        let end = preprocess_expression(run, &parse.rtable, parse.jointree, wc.endOffset, EXPRKIND_LIMIT, has_sublinks)?;
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
        preprocess_expression(run, &parse.rtable, parse.jointree, parse.limitOffset, EXPRKIND_LIMIT, has_sublinks)?;
    parse.limitCount =
        preprocess_expression(run, &parse.rtable, parse.jointree, parse.limitCount, EXPRKIND_LIMIT, has_sublinks)?;
    for action_node in &parse.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        let new_tlist = preprocess_expression_list(
            run,
            &parse.rtable,
            parse.jointree,
            action.targetList.clone_in(mcx)?,
            EXPRKIND_TARGET,
            has_sublinks,
        )?;
        let new_qual = preprocess_expression(run, &parse.rtable, parse.jointree, action.qual, EXPRKIND_QUAL, has_sublinks)?;
        // SAFETY: parse tree is planner-owned; no derived refs live.
        unsafe {
            action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(|a| {
                a.targetList = new_tlist;
                a.qual = new_qual;
            })
        }
        .expect("MergeAction");
    }
    parse.mergeJoinCondition =
        preprocess_expression(run, &parse.rtable, parse.jointree, parse.mergeJoinCondition, EXPRKIND_QUAL, has_sublinks)?;
    if let Some(oc_node) = parse.onConflict {
        let oc = oc_node.as_on_conflict_expr().expect("onConflict is OnConflictExpr");
        for elem_node in &oc.arbiterElems {
            let elem = elem_node.as_inference_elem().expect("arbiterElems cell");
            let new_expr =
                preprocess_expression(run, &parse.rtable, parse.jointree, elem.expr, EXPRKIND_ARBITER_ELEM, has_sublinks)?;
            // SAFETY: parse tree is planner-owned; no derived refs live.
            unsafe {
                elem_node
                    .with_mut::<types_nodes::primnodes::InferenceElem, _>(|e| e.expr = new_expr)
            }
            .expect("InferenceElem");
        }
        let arbiter_where =
            preprocess_expression(run, &parse.rtable, parse.jointree, oc.arbiterWhere, EXPRKIND_QUAL, has_sublinks)?;
        let conflict_set = oc.onConflictSet.clone_in(run.mcx)?;
        let conflict_set =
            preprocess_expression_list(run, &parse.rtable, parse.jointree, conflict_set, EXPRKIND_TARGET, has_sublinks)?;
        let conflict_where =
            preprocess_expression(run, &parse.rtable, parse.jointree, oc.onConflictWhere, EXPRKIND_QUAL, has_sublinks)?;
        // exclRelTlist contains only Vars, so no preprocessing needed.
        // SAFETY: same exclusive parse-tree ownership as above.
        unsafe {
            oc_node.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                o.arbiterWhere = arbiter_where;
                o.onConflictSet = conflict_set;
                o.onConflictWhere = conflict_where;
            })
        }
        .expect("OnConflictExpr");
    }
    // EXPRKIND_APPINFO: UNION ALL pull-up leaves arbitrary expressions in
    // AppendRelInfo translated_vars.
    for ai in 0..run.root.append_rel_list.len() {
        let n = run.root.append_rel_list[ai].translated_vars.len();
        for j in 0..n {
            let tid = run.root.append_rel_list[ai].translated_vars[j];
            if tid == types_pathnodes::NodeId::default() {
                continue;
            }
            let node = *run.root.expr_node(tid);
            let new = preprocess_expression(
                run,
                &parse.rtable,
                parse.jointree,
                Some(node),
                EXPRKIND_APPINFO,
                parse.hasSubLinks,
            )?
            .expect("translated var never folds to nothing");
            let nid = run.intern_expr(new);
            run.root.append_rel_list[ai].translated_vars[j] = nid;
        }
    }
    // Per-RTE expression preprocessing runs after the top-level lists
    // (planner.c:1007) so SubPlan numbering matches C. Outlined off the
    // subquery_planner hot body (select1 instruction bracket).
    if needs_rte_expr_pre {
        preprocess_rte_expressions(run, parse, has_sublinks)?;
    }

    // planner.c:1069: joinaliasvars no longer match the preprocessed
    // expressions; get rid of them so later tree scans (including the
    // post-seal sweep) don't walk stale alias lists.
    if run.root.hasJoinRTEs {
        for rte_node in &parse.rtable {
            let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
            if !rte.joinaliasvars.is_nil() {
                // SAFETY: parse tree is planner-owned; no derived refs live.
                unsafe {
                    rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                        r.joinaliasvars = NodeList::nil()
                    })
                }
                .expect("RangeTblEntry");
            }
        }
    }

    // Equality-semantics conflicts must be detected before
    // flatten_group_exprs, while HAVING still contains GROUP Vars carrying
    // the GROUP BY collation as varcollid (planner.c find_having_conflicts).
    // Indexes stay valid across flattening: it preserves havingQual list
    // length and order.
    let having_pushdown_conflicts = if parse.hasGroupRTE {
        find_having_conflicts(&parse, run.root.group_rtindex)?
    } else {
        Vec::new()
    };
    // GROUP Vars in the targetlist and HAVING give way to the preprocessed
    // grouping expressions (planner.c:1096-1103), varnullingrels preserved.
    if parse.hasGroupRTE {
        parse.targetList =
            crate::flatten_group::flatten_group_exprs_list(run, &parse, &parse.targetList)?;
        if let Some(hq) = parse.havingQual {
            parse.havingQual =
                Some(crate::flatten_group::flatten_group_exprs_node(run, &parse, hq)?);
        }
    }
    if parse.hasTargetSRFs {
        let mut still_has = false;
        for tle_node in &parse.targetList {
            let tle = tle_node.as_target_entry().expect("targetList cell");
            if coerce::expression_returns_set(tle.expr) {
                still_has = true;
                break;
            }
        }
        parse.hasTargetSRFs = still_has;
    }
    if !parse.groupingSets.is_nil() {
        // Expand before optimizing HAVING (empty-set detection needs it);
        // C stores the int-lists back into parse->groupingSets.
        let expanded =
            parse_agg::expand_grouping_sets(mcx, &parse.groupingSets, parse.groupDistinct, -1)?
                .expect("limit -1 never trips");
        let mut list = NodeList::nil();
        for set in expanded.iter() {
            let mut il = types_nodes::list::IntList::nil();
            for &x in set.iter() {
                il.lappend(mcx, x)?;
            }
            list.lappend(mcx, Node::mk_int_list(mcx, il)?)?;
        }
        parse.groupingSets = list;
    }
    if let Some(hq) = parse.havingQual {
        let first_gset_nonempty = parse.groupingSets.is_nil()
            || crate::groupingsets::grouping_set_nonempty(parse.groupingSets.nth(0));
        let havinglist = hq.as_list().expect("preprocessed havingQual is a list");
        let mut new_having = NodeList::nil();
        for (having_idx, hc) in havinglist.iter().enumerate() {
            // A clause referencing columns nullable by grouping sets stays in
            // HAVING: their nulled values do not exist before grouping
            // (planner.c:1160-1168, the group_rtindex pull_varnos member
            // test).
            if clauses::contain_agg_clause(hc)?
                || clauses::contain_volatile_functions(hc)?
                || clauses::contain_subplans(hc)?
                || having_pushdown_conflicts.contains(&having_idx)
                || (!parse.groupClause.is_nil()
                    && !parse.groupingSets.is_nil()
                    && vars::pull_varnos(mcx, hc)?.is_member(run.root.group_rtindex))
            {
                new_having.lappend(mcx, hc)?;
            } else if !parse.groupClause.is_nil() && first_gset_nonempty {
                move_qual_to_where(run, &mut *parse, hc)?;
            } else {
                // Degenerate grouping: a copy goes to WHERE, the clause stays
                // in HAVING (C copyObject; the arena share is our copy model).
                move_qual_to_where(run, &mut *parse, hc)?;
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
        crate::prepjointree::reduce_outer_joins(run, &mut *parse)?;
    }
    // C gates this on hasResultRTEs || hasOuterJoins (planner.c:1215): the
    // pass also flattens single-child FromExprs under outer joins, folding
    // their quals into the upper join's ON list so make_outerjoininfo sees
    // identity-3 ordering constraints from intermediate degenerate quals.
    if has_result_rtes || has_outer_joins {
        remove_useless_result_rtes(run, &mut *parse)?;
    }

    // Mutation done; seal the Query by reference (C shares root->parse by
    // pointer; parse is already arena-resident).
    let sealed: &'mcx Query<'mcx> = parse;
    run.root.parse = run.intern_query(sealed);

    if run.root.hasJoinRTEs {
        assert_no_join_alias_vars(sealed, &join_rtes)?;
    }

    grouping_planner(run, tuple_fraction, setops)?;

    // Correlation params requested of this level were consumed by each
    // make_subplan (outer_params recomputes at pop; see run.rs).
    debug_assert!(run.root.plan_params.is_empty());

    let final_rel = fetch_final_rel(run);
    crate::subselect::ss_charge_for_initplans(run, final_rel)?;
    set_cheapest(run, final_rel)?;
    Ok(())
}

// find_having_conflicts (planner.c): zero-based havingQual indexes of
// clauses that apply a different equivalence relation than GROUP BY and so
// must not be moved to WHERE. Must run before flatten_group_exprs, while
// GROUP Vars still carry the GROUP BY collation as varcollid and let us
// recover the grouping eqop via varattno.
fn find_having_conflicts(parse: &Query<'_>, group_rtindex: i32) -> PgResult<Vec<usize>> {
    let mut result = Vec::new();
    let Some(hq) = parse.havingQual else {
        return Ok(result);
    };
    let havinglist = hq.as_list().expect("preprocessed havingQual is a list");
    for (idx, clause) in havinglist.iter().enumerate() {
        let conflict = clauses::expression_has_grouping_conflict(clause, &mut |var| {
            if var.varno != group_rtindex || var.varlevelsup != 0 {
                return Ok(types_core::InvalidOid);
            }
            group_var_eqop(parse, var)
        })?;
        if conflict {
            result.push(idx);
        }
    }
    Ok(result)
}

// group_var_eqop (planner.c): a GROUP Var's varattno is its 1-based position
// in the RTE_GROUP groupexprs list, built by iterating parse->groupClause;
// replay that traversal to recover the SortGroupClause's eqop.
fn group_var_eqop(
    parse: &Query<'_>,
    var: &types_nodes::primnodes::Var<'_>,
) -> PgResult<types_core::Oid> {
    debug_assert_eq!(var.varlevelsup, 0);
    let mut counter = 0;
    for n in &parse.groupClause {
        let sgc = n.as_sort_group_clause().expect("groupClause cell");
        counter += 1;
        if counter == var.varattno as i32 {
            return Ok(sgc.eqop);
        }
    }
    panic!("could not find GROUP clause for GROUP Var attno {}", var.varattno);
}


pub fn preprocess_expression<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rtable: &NodeList<'mcx>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
    expr: Option<Node<'mcx>>,
    kind: i32,
    has_sublinks: bool,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(mut expr) = expr else { return Ok(None) };

    // C skips flattening only for RTFUNC/VALUES/TABLESAMPLE/TABLEFUNC kinds
    // (the last two have no EXPRKIND here yet).
    if run.root.hasJoinRTEs && kind != EXPRKIND_RTFUNC && kind != EXPRKIND_VALUES {
        // root != NULL in C: pulled-up joinaliasvars entries may need a
        // PlaceHolderVar wrapper, whose phid comes from glob.last_ph_id.
        let last_ph_id = core::cell::Cell::new(run.glob.last_ph_id);
        expr = vars::flatten_join_alias_vars(
            run.mcx,
            rtable,
            jointree,
            Some(&vars::FjavRoot { last_ph_id: &last_ph_id }),
            expr,
        )?;
        run.glob.last_ph_id = last_ph_id.get();
    }
    if kind != EXPRKIND_RTFUNC {
        // root != NULL in C: folded constraint-less domains are recorded as
        // plan type dependencies (clauses.c:3630).
        let mut type_deps: Vec<types_core::Oid> = Vec::new();
        expr = clauses::fold::eval_const_expressions_planner(
            run.mcx,
            expr,
            run.glob.bound_params,
            &mut type_deps,
        )?;
        for typid in type_deps {
            crate::setrefs::record_plan_type_dependency(run, typid)?;
        }
    }
    if kind == EXPRKIND_QUAL {
        expr = crate::prepqual::canonicalize_qual(run.mcx, expr, false)?;
    }
    if kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET {
        clauses::convert_saop_to_hashed_saop(expr)?;
    }
    // C order: replace correlation Vars first, then expand SubLinks — each
    // sub-level repeats the pair, so uplevel Vars inside sublink bodies are
    // parked exactly once (subselect.c header comment).
    if run.root.query_level > 1 {
        expr = crate::subselect::ss_replace_correlation_vars(run, expr)?;
    }
    if has_sublinks {
        expr = crate::subselect::ss_process_sublinks(run, expr, kind == EXPRKIND_QUAL)?;
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

// preprocess_phv_expression (planner.c).
pub fn preprocess_phv_expression<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let parse = run.parse();
    Ok(preprocess_expression(
        run,
        &parse.rtable,
        parse.jointree,
        Some(expr),
        EXPRKIND_PHV,
        parse.hasSubLinks,
    )?
    .expect("EXPRKIND_PHV never reduces to nothing"))
}

fn preprocess_expression_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rtable: &NodeList<'mcx>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
    list: NodeList<'mcx>,
    kind: i32,
    has_sublinks: bool,
) -> PgResult<NodeList<'mcx>> {
    if list.is_nil() {
        return Ok(list);
    }
    let node = Node::mk_list(run.mcx, list)?;
    let folded =
        preprocess_expression(run, rtable, jointree, Some(node), kind, has_sublinks)?.expect("list in, list out");
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
    let rtable = &parse.rtable;
    let jointree = parse.jointree;
    let mut fromlist = types_nodes::list::NodeList::nil();
    for child in &f.fromlist {
        fromlist.lappend(run.mcx, preprocess_jointree_quals(run, rtable, jointree, child, has_sublinks)?)?;
    }
    let quals = preprocess_expression(run, rtable, jointree, f.quals, EXPRKIND_QUAL, has_sublinks)?;
    parse.jointree = Some(alloc_leak_in(
        run.mcx,
        types_nodes::primnodes::FromExpr { fromlist, quals },
    )?);
    Ok(())
}

fn preprocess_jointree_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rtable: &NodeList<'mcx>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
    node: Node<'mcx>,
    has_sublinks: bool,
) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => Ok(node),
        NodeTag::T_FromExpr => {
            let f = node.as_from_expr().expect("FromExpr");
            let mut fromlist = types_nodes::list::NodeList::nil();
            for child in &f.fromlist {
                fromlist.lappend(run.mcx, preprocess_jointree_quals(run, rtable, jointree, child, has_sublinks)?)?;
            }
            let quals = preprocess_expression(run, rtable, jointree, f.quals, EXPRKIND_QUAL, has_sublinks)?;
            Node::mk(
                run.mcx,
                types_nodes::primnodes::FromExpr { fromlist, quals },
            )
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().expect("JoinExpr");
            let larg = preprocess_jointree_quals(run, rtable, jointree, j.larg, has_sublinks)?;
            let rarg = preprocess_jointree_quals(run, rtable, jointree, j.rarg, has_sublinks)?;
            let quals = preprocess_expression(run, rtable, jointree, j.quals, EXPRKIND_QUAL, has_sublinks)?;
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

// C subquery_planner: view permissions are checked at planner startup
// (other relations wait for executor startup) so selectivity estimation
// can't leak statistics that only the view owner may read —
// all_rows_selectable checks view-owner ACLs, not the current user's.
// Outlined off subquery_planner's hot body (select1 instruction bracket).
#[cold]
#[inline(never)]
fn check_view_perms_at_planner_startup(parse: &types_nodes::parsenodes::Query<'_>) -> PgResult<()> {
    const RELKIND_VIEW: u8 = b'v';
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if rte.perminfoindex != 0 && rte.relkind == RELKIND_VIEW {
            let perminfo = parse
                .rteperminfos
                .nth(rte.perminfoindex as usize - 1)
                .as_rte_permission_info()
                .expect("rteperminfos cell");
            if !execmain::exec_check_one_rel_perms(perminfo)? {
                const ACLCHECK_NO_PRIV: i32 = 1;
                let name = syscache_seams::pg_class_relname::call(perminfo.relid)?;
                let name = name
                    .as_ref()
                    .map(|n| core::str::from_utf8(n.name_str()).unwrap_or(""))
                    .unwrap_or("");
                aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NO_PRIV,
                    types_nodes::parsenodes::ObjectType::OBJECT_VIEW as i32,
                    name,
                )?;
            }
        }
    }
    Ok(())
}

// Per-RTE expression preprocessing (planner.c:1007). #[cold]/#[inline(never)]:
// runs only for rtable-bearing queries; keeps subquery_planner's hot body lean.
#[cold]
#[inline(never)]
fn preprocess_rte_expressions<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parse: &mut Query<'mcx>,
    has_sublinks: bool,
) -> PgResult<()> {
    let mcx = run.mcx;
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                if rte.tablesample.is_some() {
                    let ts = preprocess_expression(
                        run,
                        &parse.rtable,
                        parse.jointree,
                        rte.tablesample,
                        EXPRKIND_TABLESAMPLE,
                        parse.hasSubLinks,
                    )?;
                    // SAFETY: pre-seal Query owned by this invocation; the
                    // shared `rte` borrow is not read past this write.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.tablesample = ts
                        })
                    };
                }
            }
            RTEKind::RTE_FUNCTION => {
                // FUNCTION RTEs that preprocess_function_rtes inlined are
                // RTE_SUBQUERY by now. C preprocesses non-lateral functions
                // too — uplevel correlation Vars appear without LATERAL and
                // must become Params here — and its eval_const_expressions
                // pass is mandatory: it inserts default arguments and converts
                // named notation to positional. EXPRKIND_RTFUNC skips the
                // second eval inside preprocess_expression, as in C.
                let kind = if rte.lateral {
                    EXPRKIND_RTFUNC_LATERAL
                } else {
                    EXPRKIND_RTFUNC
                };
                let mut new_functions = NodeList::nil();
                for f_node in &rte.functions {
                    let f = f_node.as_range_tbl_function().expect("functions cell");
                    let funcexpr = match f.funcexpr {
                        Some(e) => Some(clauses::eval_const_expressions_with_params(
                            mcx,
                            e,
                            run.glob.bound_params,
                        )?),
                        None => None,
                    };
                    let funcexpr = preprocess_expression(
                        run,
                        &parse.rtable,
                        parse.jointree,
                        funcexpr,
                        kind,
                        parse.hasSubLinks,
                    )?;
                    new_functions.lappend(
                        mcx,
                        Node::mk(
                            mcx,
                            types_nodes::parsenodes::RangeTblFunction {
                                funcexpr,
                                funccolcount: f.funccolcount,
                                funccolnames: f.funccolnames.clone_in(mcx)?,
                                funccoltypes: f.funccoltypes.clone_in(mcx)?,
                                funccoltypmods: f.funccoltypmods.clone_in(mcx)?,
                                funccolcollations: f.funccolcollations.clone_in(mcx)?,
                                funcparams: f.funcparams.clone_in(mcx)?,
                            },
                        )?,
                    )?;
                }
                // SAFETY: as the RTE_RELATION arm above.
                unsafe {
                    rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                        r.functions = new_functions
                    })
                };
            }
            RTEKind::RTE_VALUES => {
                let kind =
                    if rte.lateral { EXPRKIND_VALUES_LATERAL } else { EXPRKIND_VALUES };
                let lists = preprocess_expression_list(
                    run,
                    &parse.rtable,
                    parse.jointree,
                    rte.values_lists.clone_in(mcx)?,
                    kind,
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
                let kind =
                    if rte.lateral { EXPRKIND_TABLEFUNC_LATERAL } else { EXPRKIND_TABLEFUNC };
                let tf =
                    preprocess_expression(run, &parse.rtable, parse.jointree, rte.tablefunc, kind, parse.hasSubLinks)?;
                // SAFETY: as the RTE_RELATION arm above.
                unsafe {
                    rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                        r.tablefunc = tf
                    })
                };
            }
            RTEKind::RTE_GROUP => {
                let exprs = preprocess_expression_list(
                    run,
                    &parse.rtable,
                    parse.jointree,
                    rte.groupexprs.clone_in(mcx)?,
                    EXPRKIND_GROUPEXPR,
                    parse.hasSubLinks,
                )?;
                // SAFETY: as the RTE_RELATION arm above.
                unsafe {
                    rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                        r.groupexprs = exprs
                    })
                };
            }
            _ => {}
        }
        // Re-derive the shared ref: the with_mut arms above invalidated the
        // pre-match `rte` (with_mut contract, node_tree.rs — no reference
        // derived before the mutation may be used during or after it).
        // (Miri F5, notes/miri-pilot-lane.md.)
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if !rte.securityQuals.is_nil() {
            let mut quals = types_nodes::list::NodeList::nil();
            for sq in &rte.securityQuals {
                // A constant-true element preprocesses to None; keep an empty
                // sublist so per-element security levels stay aligned.
                let one = match preprocess_expression(run, &parse.rtable, parse.jointree, Some(sq), EXPRKIND_QUAL, has_sublinks)? {
                    Some(n) => n,
                    None => Node::mk_list(mcx, types_nodes::list::NodeList::nil())?,
                };
                quals.lappend(mcx, one)?;
            }
            // SAFETY: as the RTE_RELATION arm above.
            unsafe {
                rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.securityQuals = quals
                })
            };
        }
    }
    Ok(())
}
