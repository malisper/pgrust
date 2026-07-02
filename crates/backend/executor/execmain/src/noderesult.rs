use ::execexpr::{exec_build_projection_info, exec_init_qual, exec_project, exec_qual, ExprState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{alloc_in, PgBox};
use ::types_error::PgResult;
use ::types_nodes::plannodes::Result as ResultPlan;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

use crate::procnode::{
    exec_end_node, exec_init_node, exec_proc_node, with_eval_slots, PlanStateBase, PlanStateNode,
};
use crate::typefromtl::exec_type_from_tl;

pub struct ResultState<'mcx> {
    pub ps: PlanStateBase<'mcx>,
    pub outer: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    pub resconstantqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    pub rs_done: bool,
    pub rs_checkqual: bool,
}

/// `ExecInitResult` (nodeResult.c).
pub fn exec_init_result<'mcx>(
    node: &'mcx ResultPlan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<ResultState<'mcx>> {
    debug_assert!(
        eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD) == 0 || node.plan.lefttree.is_some()
    );
    let mcx = estate.es_query_cxt;
    let ecxt = estate.exec_assign_expr_context();
    let outer = exec_init_node(node.plan.lefttree, estate, eflags)?;
    debug_assert!(node.plan.righttree.is_none());

    let desc = exec_type_from_tl(&node.plan.targetlist)?;
    let slot = estate.exec_init_extra_tuple_slot(Some(desc.clone()), TupleSlotKind::Virtual);
    let proj = exec_build_projection_info(mcx, &node.plan.targetlist, None)?;

    let qual = exec_init_qual(mcx, &node.plan.qual)?;
    let resconstantqual = match node.resconstantqual {
        None => None,
        Some(n) => {
            let list = n
                .as_list()
                .unwrap_or_else(|| panic!("Result.resconstantqual: expected List, got {:?}", n.node_tag()));
            exec_init_qual(mcx, list)?
        }
    };

    let outer = match outer {
        Some(o) => Some(alloc_in(mcx, o)?),
        None => None,
    };
    let rs_checkqual = resconstantqual.is_some();
    Ok(ResultState {
        ps: PlanStateBase {
            plan: &node.plan,
            ps_ExprContext: Some(ecxt),
            ps_ResultTupleDesc: Some(desc),
            ps_ResultTupleSlot: Some(slot),
            ps_ProjInfo: Some(proj),
            qual,
        },
        outer,
        resconstantqual,
        rs_done: false,
        rs_checkqual,
    })
}

/// `ExecResult` (nodeResult.c).
pub fn exec_result<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    crate::cfi()?;
    let ecxt = node.ps.ps_ExprContext.expect("ResultState without ExprContext");

    if node.rs_checkqual {
        let resconstantqual = node.resconstantqual.as_deref_mut();
        let qual_result =
            with_eval_slots(estate, ecxt, None, |slots, _, _| exec_qual(resconstantqual, slots))?;
        node.rs_checkqual = false;
        if !qual_result {
            node.rs_done = true;
            return Ok(None);
        }
    }

    estate.reset_expr_context(ecxt);

    if node.rs_done {
        return Ok(None);
    }

    if let Some(outer) = node.outer.as_deref_mut() {
        let Some(outer_slot) = exec_proc_node(outer, estate)? else {
            return Ok(None);
        };
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(outer_slot);
    } else {
        node.rs_done = true;
    }

    let result_slot = node.ps.ps_ResultTupleSlot.expect("ResultState without result slot");
    let proj = node.ps.ps_ProjInfo.as_deref_mut().expect("ResultState without projection");
    with_eval_slots(estate, ecxt, Some(result_slot), |slots, result, mcx| {
        exec_project(proj, slots, result.unwrap(), mcx)
    })?;
    Ok(Some(result_slot))
}

/// `ExecEndResult` (nodeResult.c).
pub fn exec_end_result<'mcx>(
    node: &mut ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node.outer.as_deref_mut() {
        Some(outer) => exec_end_node(outer, estate),
        None => Ok(()),
    }
}
