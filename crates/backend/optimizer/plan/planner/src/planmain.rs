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
                let quals: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(run.mcx);
                debug_assert!(jointree.quals.is_none());
                let path = create_group_result_path(run, final_rel, target_id, quals);
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

    // fix_placeholder_input_needed_levels / reduce_unique_semijoins /
    // self-join removal / lateral join info / match_foreign_keys_to_quals /
    // extract_restriction_or_clauses / add_other_rels_to_query / row
    // identity vars: all no-ops with no placeholders, no semijoins, no
    // lateral refs, no fkeys and no OR clauses.
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
        other => panic!("add_base_rels_to_query (initsplan.c): {other:?}; M2 join lane"),
    }
    Ok(())
}

// The UPPERREL_FINAL rel accessor both planner.c call sites use.
pub fn fetch_final_rel(run: &mut PlannerRun<'_>) -> RelId {
    crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_FINAL)
}
