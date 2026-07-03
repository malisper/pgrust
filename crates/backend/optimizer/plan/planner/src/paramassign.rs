//! paramassign.c slice: replace_outer_var/assign_param_for_var. PHV/Aggref/
//! GroupingFunc/MergeSupport/Returning replacement legs are loud upstream.

use types_error::PgResult;
use types_nodes::primnodes::{Param, ParamKind, Var};
use types_nodes::Node;
use types_pathnodes::PlannerParamItem;

use crate::run::PlannerRun;

/// replace_outer_var (paramassign.c): PARAM_EXEC Param for an uplevel Var,
/// parking the need on the owning ancestor's plan_params.
pub(crate) fn replace_outer_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &Var<'mcx>,
) -> PgResult<Node<'mcx>> {
    debug_assert!(var.varlevelsup > 0);
    let paramid = assign_param_for_var(run, var)?;
    Node::mk(
        run.mcx,
        Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid,
            paramtype: var.vartype,
            paramtypmod: var.vartypmod,
            paramcollid: var.varcollid,
            location: var.location,
        },
    )
}

fn assign_param_for_var<'mcx>(run: &mut PlannerRun<'mcx>, var: &Var<'mcx>) -> PgResult<i32> {
    let idx = run
        .suspended_roots
        .len()
        .checked_sub(var.varlevelsup as usize)
        .unwrap_or_else(|| {
            panic!(
                "assign_param_for_var (paramassign.c): varlevelsup {} exceeds the \
                 ancestor chain",
                var.varlevelsup
            )
        });
    {
        // Comparison mirrors _equalVar minus varlevelsup (and the fields
        // _equalVar ignores: varnosyn/varattnosyn/location).
        let target = &run.suspended_roots[idx].root;
        for &pid in target.plan_params.iter() {
            let pitem = target.planner_param_item(pid);
            if let Some(pvar) = target.expr_node(pitem.item).as_var() {
                if pvar.varno == var.varno
                    && pvar.varattno == var.varattno
                    && pvar.vartype == var.vartype
                    && pvar.vartypmod == var.vartypmod
                    && pvar.varcollid == var.varcollid
                    && pvar.varreturningtype == var.varreturningtype
                    && pvar.varnullingrels.equal(&var.varnullingrels)
                {
                    return Ok(pitem.paramId);
                }
            }
        }
    }
    let mcx = run.mcx;
    let item = Node::mk(
        mcx,
        Var {
            varlevelsup: 0,
            varnullingrels: var.varnullingrels.clone_in(mcx)?,
            ..*var
        },
    )?;
    let param_id = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(mcx, var.vartype)?;
    let target = &mut run.suspended_roots[idx].root;
    let item_id = target.alloc_expr_node(item);
    let pp = target.alloc_planner_param_item(PlannerParamItem { item: item_id, paramId: param_id });
    target.plan_params.push(pp);
    Ok(param_id)
}
