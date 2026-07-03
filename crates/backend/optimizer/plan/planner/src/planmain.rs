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

    let joinlist = crate::analyzejoins::remove_useless_joins(run, joinlist)?;

    crate::analyzejoins::reduce_unique_semijoins(run)?;

    // fix_placeholder_input_needed_levels / self-join removal / lateral join
    // info / match_foreign_keys_to_quals / extract_restriction_or_clauses /
    // row identity vars: all no-ops with no placeholders, no lateral refs,
    // no fkeys and no OR clauses.
    debug_assert!(run.root.placeholder_list.is_empty() && run.root.fkey_list.is_empty());

    crate::inherit::add_other_rels_to_query(run)?;

    let final_rel = crate::allpaths::make_one_rel(run, &joinlist)?;
    if run.root.rel(final_rel).cheapest_total_path.is_none()
        || run.root.rel(final_rel).pathlist.is_empty()
    {
        panic!("failed to construct the join relation");
    }
    Ok(final_rel)
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
