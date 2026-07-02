use mcx::PgVec;
use types_error::PgResult;
use types_nodes::parsenodes::RTEKind;
use types_nodes::NodeTag;
use types_pathnodes::{RelId, UPPERREL_FINAL};

use crate::pathnode::{add_path, create_group_result_path, set_cheapest};
use crate::relnode::{build_simple_rel, setup_simple_rel_arrays};
use crate::run::PlannerRun;
use crate::{gucs, is_parallel_safe_opt};

// query_planner (planmain.c): only the single-RTE_RESULT shortcut is live;
// the deconstruct_jointree/make_one_rel spine is the M2 join lane.
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
                let final_rel = build_simple_rel(&mut run.root, varno, rte.rtekind);

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
                add_path(run, final_rel, path);

                set_cheapest(run, final_rel)?;
                run.root.ec_merging_done = true;
                qp_callback(run)?;
                return Ok(final_rel);
            }
        }
    }

    panic!(
        "query_planner (planmain.c): non-trivial jointree \
         (deconstruct_jointree/make_one_rel); M2 scan/join lane"
    );
}

// The UPPERREL_FINAL rel accessor both planner.c call sites use.
pub fn fetch_final_rel(run: &mut PlannerRun<'_>) -> RelId {
    crate::relnode::fetch_upper_rel(&mut run.root, UPPERREL_FINAL)
}
