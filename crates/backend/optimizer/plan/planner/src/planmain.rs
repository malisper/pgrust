//! query_planner (planmain.c): the trivial RTE_RESULT shortcut plus the
//! single-baserel spine; everything multi-rel is a named panic.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::parsenodes::RTEKind;
use types_nodes::NodeTag;
use types_pathnodes::{RelId, UPPERREL_FINAL};

use crate::pathnode::{add_path, create_group_result_path, set_cheapest};
use crate::relnode::{build_simple_rel, setup_simple_rel_arrays};
use crate::run::PlannerRun;
use crate::{gucs, is_parallel_safe_opt};

pub fn query_planner<'mcx>(
    run: &mut PlannerRun<'mcx>,
    qp_callback: fn(&mut PlannerRun<'mcx>) -> PgResult<()>,
) -> PgResult<RelId> {
    let parse = run.parse();
    setup_simple_rel_arrays(&mut run.root, parse.rtable.len());

    let jointree = parse.jointree.expect("jointree is a FromExpr");
    assert!(!jointree.fromlist.is_nil());
    if jointree.fromlist.len() == 1 {
        let jtnode = jointree.fromlist.nth(0);
        if jtnode.node_tag() == NodeTag::T_RangeTblRef {
            let varno = jtnode.as_range_tbl_ref().unwrap().rtindex as u32;
            let rte = run.rte(varno as usize);
            if rte.rtekind == RTEKind::RTE_RESULT {
                let final_rel = build_simple_rel(run, varno, rte.rtekind)?;

                if run.glob.parallel_mode_ok
                    && (run.root.query_level > 1
                        || gucs::debug_parallel_query() != guc_tables::consts::DEBUG_PARALLEL_OFF)
                {
                    let safe = is_parallel_safe_opt(run, jointree.quals)?;
                    run.root.rel_mut(final_rel).consider_parallel = safe;
                }

                let target_id = run.rel_reltarget_id(final_rel);
                let mut quals: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(run.mcx);
                if let Some(q) = jointree.quals {
                    let list = q.as_list().expect("preprocessed quals are an implicit-AND list");
                    for clause in list {
                        quals.push(run.intern_expr(clause));
                    }
                }
                let path = create_group_result_path(run, final_rel, target_id, quals)?;
                let pid = run.root.alloc_path(path);
                add_path(run, final_rel, pid);

                set_cheapest(run, final_rel)?;
                run.root.ec_merging_done = true;
                qp_callback(run)?;
                return Ok(final_rel);
            }
        }
    }

    // General spine, single-baserel arm.
    for item in &jointree.fromlist {
        add_base_rels_to_query(run, item)?;
    }

    // remove_useless_groupby_columns / find_placeholders_in_jointree /
    // find_lateral_references: no GROUP BY, PHVs or lateral refs exist.
    crate::initsplan::build_base_rel_tlists(run)?;
    debug_assert!(!run.root.hasLateralRTEs);

    let joinlist = crate::initsplan::deconstruct_jointree(run)?;

    crate::initsplan::reconsider_outer_join_clauses(run)?;

    // No ECs exist (initsplan.rs documents the EC-const divergence).
    debug_assert!(run.root.eq_classes.is_empty());
    run.root.ec_merging_done = true;

    qp_callback(run)?;

    // remove_useless_joins (analyzejoins.c): decision slice only — an
    // actually-removable LEFT join is loud (remove_rel_from_query unported).
    if !run.root.join_info_list.is_empty() {
        check_useless_joins(run)?;
    }

    // reduce_unique_semijoins (analyzejoins.c): decision slice only — a
    // provably-reducible semijoin is loud (the inner_unique costing lane it
    // reduces into is also loud).
    if !run.root.join_info_list.is_empty() {
        check_unique_semijoins(run)?;
    }

    // fix_placeholder_input_needed_levels / self-join removal / lateral join
    // info / match_foreign_keys_to_quals / extract_restriction_or_clauses /
    // add_other_rels_to_query / row identity vars: all no-ops with no
    // placeholders, no lateral refs, no fkeys and no OR clauses.
    debug_assert!(run.root.placeholder_list.is_empty() && run.root.fkey_list.is_empty());

    let final_rel = crate::allpaths::make_one_rel(run, &joinlist)?;
    if run.root.rel(final_rel).cheapest_total_path.is_none()
        || run.root.rel(final_rel).pathlist.is_empty()
    {
        panic!("failed to construct the join relation");
    }
    Ok(final_rel)
}

// join_is_removable (analyzejoins.c) early exits: keep the join when the
// inner rel can't be proven distinct or its outputs are needed above the
// join; anything that survives to the unique-index proof is loud.
fn check_useless_joins(run: &mut PlannerRun<'_>) -> PgResult<()> {
    for i in 0..run.root.join_info_list.len() {
        let sjinfo = run.root.join_info_list[i].clone();
        if sjinfo.jointype != types_pathnodes::JOIN_LEFT {
            continue;
        }
        let Some(innerrelid) =
            crate::relnode::relids_singleton_member(&sjinfo.min_righthand)
        else {
            continue;
        };
        let innerrel = crate::relnode::find_base_rel(&run.root, innerrelid);
        // rel_supports_distinctness, baserel arm.
        {
            let rel = run.root.rel(innerrel);
            if rel.rtekind != types_pathnodes::RTE_RELATION
                || !rel
                    .indexlist
                    .iter()
                    .any(|ind| ind.unique && ind.immediate && ind.indpred.is_empty())
            {
                continue;
            }
        }
        let mcx = run.mcx;
        let mut inputrelids = crate::relnode::relids_union(
            mcx,
            &sjinfo.min_lefthand,
            &sjinfo.min_righthand,
        );
        if sjinfo.ojrelid != 0 {
            inputrelids =
                crate::relnode::relids_add_member(mcx, &inputrelids, sjinfo.ojrelid);
        }
        let needed_above = {
            let rel = run.root.rel(innerrel);
            rel.attr_needed
                .iter()
                .any(|a| !crate::relnode::relids_is_subset(a, &inputrelids))
        };
        if needed_above {
            continue;
        }
        panic!(
            "join_is_removable (analyzejoins.c): LEFT join over provably-unique unused \
             inner rel; join-removal lane unported"
        );
    }
    Ok(())
}

// reduce_unique_semijoins -> rel_is_distinct_for (analyzejoins.c) as a
// detection pass: proving the RHS distinct for the semijoin clauses would
// reduce it to a plain inner join in C.
fn check_unique_semijoins(run: &mut PlannerRun<'_>) -> PgResult<()> {
    for i in 0..run.root.join_info_list.len() {
        let sjinfo = run.root.join_info_list[i].clone();
        if sjinfo.jointype != types_pathnodes::JOIN_SEMI {
            continue;
        }
        let Some(innerrelid) =
            crate::relnode::relids_singleton_member(&sjinfo.min_righthand)
        else {
            continue;
        };
        let innerrel = crate::relnode::find_base_rel(&run.root, innerrelid);
        {
            let rel = run.root.rel(innerrel);
            if rel.reloptkind != types_pathnodes::RELOPT_BASEREL
                || rel.rtekind != types_pathnodes::RTE_RELATION
                || !rel
                    .indexlist
                    .iter()
                    .any(|ind| ind.unique && ind.immediate && ind.indpred.is_empty())
            {
                continue;
            }
        }
        let mcx = run.mcx;
        let joinrelids = crate::relnode::relids_union(
            mcx,
            &sjinfo.min_lefthand,
            &sjinfo.min_righthand,
        );
        // Eclass-lite keeps join equalities in joininfo, so
        // generate_join_implied_equalities contributes nothing extra.
        let mut clause_rids: mcx::PgVec<'_, types_pathnodes::RinfoId> = mcx::PgVec::new_in(mcx);
        for j in 0..run.root.rel(innerrel).joininfo.len() {
            let rid = run.root.rel(innerrel).joininfo[j];
            if crate::relnode::relids_is_subset(
                &run.root.rinfo(rid).required_relids,
                &joinrelids,
            ) {
                clause_rids.push(rid);
            }
        }
        // rel_is_distinct_for: keep clauses of the form outer op inner with a
        // mergejoinable operator, then look for a matching unique index.
        let inner_relids = crate::relnode::relids_copy(mcx, &run.root.rel(innerrel).relids);
        let inner_relid_u32 = run.root.rel(innerrel).relid;
        let mut matched_cols: mcx::PgVec<'_, (types_pathnodes::RinfoId, bool)> =
            mcx::PgVec::new_in(mcx);
        for &rid in clause_rids.iter() {
            let ri = run.root.rinfo(rid);
            if !ri.can_join || ri.mergeopfamilies.is_empty() {
                continue;
            }
            let inner_is_right =
                crate::relnode::relids_is_subset(&ri.right_relids, &inner_relids);
            matched_cols.push((rid, inner_is_right));
        }
        let n_indexes = run.root.rel(innerrel).indexlist.len();
        for k in 0..n_indexes {
            let ind = run.root.rel(innerrel).indexlist[k];
            if !ind.unique || !ind.immediate || !ind.indpred.is_empty() {
                continue;
            }
            let mut all = true;
            for c in 0..ind.nkeycolumns as usize {
                let mut m = false;
                for &(rid, inner_is_right) in matched_cols.iter() {
                    let ri = run.root.rinfo(rid);
                    if !ri.mergeopfamilies.iter().any(|&f| f == ind.opfamily[c]) {
                        continue;
                    }
                    let clause = *run.root.expr_node(ri.clause);
                    let o = clause.as_op_expr().expect("mergeclause is an OpExpr");
                    let mut iexpr = if inner_is_right { o.args.nth(1) } else { o.args.nth(0) };
                    while let Some(r) = iexpr.as_relabel_type() {
                        iexpr = r.arg;
                    }
                    if let Some(var) = iexpr.as_var() {
                        if var.varno as u32 == inner_relid_u32
                            && var.varattno != 0
                            && ind.indexkeys[c] == var.varattno as i32
                        {
                            m = true;
                            break;
                        }
                    }
                }
                if !m {
                    all = false;
                    break;
                }
            }
            if all {
                panic!(
                    "reduce_unique_semijoins (analyzejoins.c): semijoin RHS provably \
                     unique; unique-semijoin reduction lane unported"
                );
            }
        }
    }
    Ok(())
}

// add_base_rels_to_query (initsplan.c): FromExpr items handled by the caller.
fn add_base_rels_to_query<'mcx>(
    run: &mut PlannerRun<'mcx>,
    item: types_nodes::Node<'mcx>,
) -> PgResult<()> {
    match item.node_tag() {
        NodeTag::T_RangeTblRef => {
            let varno = item.as_range_tbl_ref().unwrap().rtindex as u32;
            let rte = run.rte(varno as usize);
            build_simple_rel(run, varno, rte.rtekind)?;
        }
        NodeTag::T_JoinExpr => {
            let j = item.as_join_expr().unwrap();
            add_base_rels_to_query(run, j.larg)?;
            add_base_rels_to_query(run, j.rarg)?;
        }
        NodeTag::T_FromExpr => {
            let f = item.as_from_expr().unwrap();
            for child in &f.fromlist {
                add_base_rels_to_query(run, child)?;
            }
        }
        other => panic!("add_base_rels_to_query (initsplan.c): {other:?}; M2 join lane"),
    }
    Ok(())
}

// The UPPERREL_FINAL rel accessor both planner.c call sites use.
pub fn fetch_final_rel(run: &mut PlannerRun<'_>) -> RelId {
    crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_FINAL)
}
