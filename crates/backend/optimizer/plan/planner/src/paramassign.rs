//! paramassign.c slice. MergeSupport/Returning replacement legs are
//! structurally absent (no MergeSupportFunc/ReturningExpr node types).

use types_error::PgResult;
use types_nodes::primnodes::{Aggref, GroupingFunc, Param, ParamKind, PlaceHolderVar, Var};
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

fn assign_param_for_placeholdervar<'mcx>(
    run: &mut PlannerRun<'mcx>,
    phv: &PlaceHolderVar<'mcx>,
    phv_node: Node<'mcx>,
) -> PgResult<i32> {
    let idx = run
        .suspended_roots
        .len()
        .checked_sub(phv.phlevelsup as usize)
        .unwrap_or_else(|| {
            panic!(
                "assign_param_for_placeholdervar (paramassign.c): phlevelsup {} exceeds \
                 the ancestor chain",
                phv.phlevelsup
            )
        });
    {
        let target = &run.suspended_roots[idx].root;
        for &pid in target.plan_params.iter() {
            let pitem = target.planner_param_item(pid);
            if let Some(pphv) = target.expr_node(pitem.item).as_place_holder_var() {
                if pphv.phid == phv.phid {
                    return Ok(pitem.paramId);
                }
            }
        }
    }
    let mcx = run.mcx;
    let copy = rewrite_manip::copy_node(mcx, phv_node)?;
    rewrite_manip::IncrementVarSublevelsUp(copy, -(phv.phlevelsup as i32), 0)?;
    let param_id = run.glob.param_exec_types.len() as i32;
    let ptype = nodes_core::expr_type(phv.phexpr);
    run.glob.param_exec_types.lappend(mcx, ptype)?;
    let target = &mut run.suspended_roots[idx].root;
    let item_id = target.alloc_expr_node(copy);
    let pp = target.alloc_planner_param_item(PlannerParamItem { item: item_id, paramId: param_id });
    target.plan_params.push(pp);
    Ok(param_id)
}

/// replace_outer_placeholdervar (paramassign.c).
pub(crate) fn replace_outer_placeholdervar<'mcx>(
    run: &mut PlannerRun<'mcx>,
    phv: &PlaceHolderVar<'mcx>,
    phv_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    debug_assert!(phv.phlevelsup > 0);
    let paramid = assign_param_for_placeholdervar(run, phv, phv_node)?;
    Node::mk(
        run.mcx,
        Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid,
            paramtype: nodes_core::expr_type(phv.phexpr),
            paramtypmod: nodes_core::expr_typmod(phv.phexpr),
            paramcollid: nodes_core::expr_collation(phv.phexpr),
            location: -1,
        },
    )
}

/// replace_outer_agg (paramassign.c): no dedupe — a new slot per reference.
pub(crate) fn replace_outer_agg<'mcx>(
    run: &mut PlannerRun<'mcx>,
    agg: &Aggref<'mcx>,
    agg_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    debug_assert!(agg.agglevelsup > 0);
    let idx = run
        .suspended_roots
        .len()
        .checked_sub(agg.agglevelsup as usize)
        .unwrap_or_else(|| {
            panic!(
                "replace_outer_agg (paramassign.c): agglevelsup {} exceeds the \
                 ancestor chain",
                agg.agglevelsup
            )
        });
    let mcx = run.mcx;
    let copy = rewrite_manip::copy_node(mcx, agg_node)?;
    rewrite_manip::IncrementVarSublevelsUp(copy, -(agg.agglevelsup as i32), 0)?;
    let param_id = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(mcx, agg.aggtype)?;
    let target = &mut run.suspended_roots[idx].root;
    let item_id = target.alloc_expr_node(copy);
    let pp = target.alloc_planner_param_item(PlannerParamItem { item: item_id, paramId: param_id });
    target.plan_params.push(pp);
    Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: param_id,
            paramtype: agg.aggtype,
            paramtypmod: -1,
            paramcollid: agg.aggcollid,
            location: agg.location,
        },
    )
}

/// replace_outer_grouping (paramassign.c): no dedupe, as replace_outer_agg.
pub(crate) fn replace_outer_grouping<'mcx>(
    run: &mut PlannerRun<'mcx>,
    grp: &GroupingFunc<'mcx>,
    grp_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    debug_assert!(grp.agglevelsup > 0);
    let idx = run
        .suspended_roots
        .len()
        .checked_sub(grp.agglevelsup as usize)
        .unwrap_or_else(|| {
            panic!(
                "replace_outer_grouping (paramassign.c): agglevelsup {} exceeds the \
                 ancestor chain",
                grp.agglevelsup
            )
        });
    let mcx = run.mcx;
    let ptype = nodes_core::expr_type(grp_node);
    let copy = rewrite_manip::copy_node(mcx, grp_node)?;
    rewrite_manip::IncrementVarSublevelsUp(copy, -(grp.agglevelsup as i32), 0)?;
    let param_id = run.glob.param_exec_types.len() as i32;
    run.glob.param_exec_types.lappend(mcx, ptype)?;
    let target = &mut run.suspended_roots[idx].root;
    let item_id = target.alloc_expr_node(copy);
    let pp = target.alloc_planner_param_item(PlannerParamItem { item: item_id, paramId: param_id });
    target.plan_params.push(pp);
    Node::mk(
        mcx,
        Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: param_id,
            paramtype: ptype,
            paramtypmod: -1,
            paramcollid: 0,
            location: grp.location,
        },
    )
}

/// replace_nestloop_param_var (paramassign.c): PARAM_EXEC Param for a Var
/// supplied by a nestloop outer rel, parked on root->curOuterParams.
pub(crate) fn replace_nestloop_param_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &Var<'mcx>,
    var_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    for i in 0..run.root.curOuterParams.len() {
        let id = run.root.curOuterParams[i];
        let nlp = run
            .root
            .expr_node(id)
            .as_nest_loop_param()
            .expect("curOuterParams holds NestLoopParam nodes");
        if types_nodes::equal(var_node, nlp.paramval) {
            return Node::mk(
                mcx,
                Param {
                    paramkind: ParamKind::PARAM_EXEC,
                    paramid: nlp.paramno,
                    paramtype: var.vartype,
                    paramtypmod: var.vartypmod,
                    paramcollid: var.varcollid,
                    location: var.location,
                },
            );
        }
    }
    let (mut prm, _) = crate::subselect::generate_new_exec_param(
        run,
        var.vartype,
        var.vartypmod,
        var.varcollid,
    )?;
    prm.location = var.location;
    let paramval = Node::mk(
        mcx,
        Var { varnullingrels: var.varnullingrels.clone_in(mcx)?, ..*var },
    )?;
    let nlp = Node::mk(
        mcx,
        types_nodes::plannodes::NestLoopParam { paramno: prm.paramid, paramval },
    )?;
    let id = run.intern_expr(nlp);
    run.root.curOuterParams.push(id);
    Node::mk(mcx, prm)
}

/// replace_nestloop_param_placeholdervar (paramassign.c).
#[allow(dead_code)]
pub(crate) fn replace_nestloop_param_placeholdervar<'mcx>(
    run: &mut PlannerRun<'mcx>,
    phv: &PlaceHolderVar<'mcx>,
    phv_node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let (ptype, ptypmod, pcollid) = (
        nodes_core::expr_type(phv.phexpr),
        nodes_core::expr_typmod(phv.phexpr),
        nodes_core::expr_collation(phv.phexpr),
    );
    for i in 0..run.root.curOuterParams.len() {
        let id = run.root.curOuterParams[i];
        let nlp = run
            .root
            .expr_node(id)
            .as_nest_loop_param()
            .expect("curOuterParams holds NestLoopParam nodes");
        if types_nodes::equal(phv_node, nlp.paramval) {
            return Node::mk(
                mcx,
                Param {
                    paramkind: ParamKind::PARAM_EXEC,
                    paramid: nlp.paramno,
                    paramtype: ptype,
                    paramtypmod: ptypmod,
                    paramcollid: pcollid,
                    location: -1,
                },
            );
        }
    }
    let (prm, _) = crate::subselect::generate_new_exec_param(run, ptype, ptypmod, pcollid)?;
    let paramval = rewrite_manip::copy_node(mcx, phv_node)?;
    let nlp = Node::mk(
        mcx,
        types_nodes::plannodes::NestLoopParam { paramno: prm.paramid, paramval },
    )?;
    let id = run.intern_expr(nlp);
    run.root.curOuterParams.push(id);
    Node::mk(mcx, prm)
}

/// process_subquery_nestloop_params (paramassign.c), Var arm (PHVs are loud
/// upstream).
pub(crate) fn process_subquery_nestloop_params<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subplan_params: &[types_pathnodes::NodeId],
) -> PgResult<()> {
    let mcx = run.mcx;
    for &pid in subplan_params {
        let (param_id, item_id) = {
            let pitem = run.root.planner_param_item(pid);
            (pitem.paramId, pitem.item)
        };
        let item = *run.root.expr_node(item_id);
        let var = item.as_var().unwrap_or_else(|| {
            panic!("process_subquery_nestloop_params (paramassign.c): non-Var subquery parameter")
        });
        if !crate::relnode::relids_is_member(var.varno, &run.root.curOuterRels) {
            panic!("non-LATERAL parameter required by subquery");
        }
        let mut present = false;
        for i in 0..run.root.curOuterParams.len() {
            let id = run.root.curOuterParams[i];
            let nlp = run
                .root
                .expr_node(id)
                .as_nest_loop_param()
                .expect("curOuterParams holds NestLoopParam nodes");
            if nlp.paramno == param_id {
                debug_assert!(types_nodes::equal(item, nlp.paramval));
                present = true;
                break;
            }
        }
        if !present {
            let paramval = Node::mk(
                mcx,
                Var { varnullingrels: var.varnullingrels.clone_in(mcx)?, ..*var },
            )?;
            let nlp = Node::mk(
                mcx,
                types_nodes::plannodes::NestLoopParam { paramno: param_id, paramval },
            )?;
            let id = run.intern_expr(nlp);
            run.root.curOuterParams.push(id);
        }
    }
    Ok(())
}

/// identify_current_nestloop_params (paramassign.c), Var arm; returns the
/// nestParams this join must supply and removes them from curOuterParams.
pub(crate) fn identify_current_nestloop_params<'mcx>(
    run: &mut PlannerRun<'mcx>,
    leftrelids: &types_pathnodes::Relids<'mcx>,
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut result = types_nodes::NodeList::nil();
    let mut i = 0;
    while i < run.root.curOuterParams.len() {
        let id = run.root.curOuterParams[i];
        let (paramno, paramval) = {
            let nlp = run
                .root
                .expr_node(id)
                .as_nest_loop_param()
                .expect("curOuterParams holds NestLoopParam nodes");
            (nlp.paramno, nlp.paramval)
        };
        let var = paramval
            .as_var()
            .expect("NestLoopParam values are Vars (PHVs are loud upstream)");
        if crate::relnode::relids_is_member(var.varno, leftrelids) {
            run.root.curOuterParams.remove(i);
            let rel = crate::relnode::find_base_rel(&run.root, var.varno);
            let nulling = {
                let nr = &run.root.rel(rel).nulling_relids;
                crate::relnode::relids_intersect(mcx, nr, leftrelids)
            };
            let mut nullingrels = types_nodes::Bitmapset::empty();
            for x in crate::relnode::relids_members(&nulling) {
                nullingrels.add_member(mcx, x)?;
            }
            let newvar = Node::mk(mcx, Var { varnullingrels: nullingrels, ..*var })?;
            let nlp_node = Node::mk(
                mcx,
                types_nodes::plannodes::NestLoopParam { paramno, paramval: newvar },
            )?;
            result.lappend(mcx, nlp_node)?;
        } else {
            i += 1;
        }
    }
    Ok(result)
}
