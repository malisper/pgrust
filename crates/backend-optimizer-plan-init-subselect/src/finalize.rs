//! PLAN FINALIZATION (subselect.c) — `SS_finalize_plan` / `finalize_plan` /
//! `finalize_primnode` / `finalize_agg_primnode`, plus
//! `SS_charge_for_initplans` / `SS_compute_initplan_cost` /
//! `SS_attach_initplans`.
//!
//! # Model reconciliation (read before editing)
//!
//! `finalize_plan` recursively computes the `extParam`/`allParam`
//! [`Bitmapset`](types_nodes::Bitmapset) of every `Plan` node, mutating the
//! owned plan tree (`&mut Node<'mcx>`). The `paramids` / `valid_params` /
//! `scan_params` working sets are `types_nodes::Bitmapset<'mcx>` (the same type
//! the `Plan.extParam`/`allParam` fields hold), reached through the
//! `backend-nodes-core` bms ops. The `root.outer_params` set
//! (`types_pathnodes::Relids`) is converted to a `types_nodes::Bitmapset` at the
//! `SS_finalize_plan` boundary.
//!
//! Child SubPlans referenced by `plan->initPlan` / a `SubPlan` expr are read
//! through [`planner_subplan_get_plan`](types_pathnodes::planner_run::planner_subplan_get_plan)
//! (the run-backed `glob->subplans` deref); we never mutate those child plans
//! here (C only reads their already-finalized `extParam`).
//!
//! `find_minmax_agg_replacement_param` and the `T_SubqueryScan` subroot recursion
//! ride outward seams (planagg.c / relnode.c subroot retrieval are unported over
//! this model).

use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, ParamKind};
use types_pathnodes::planner_run::{planner_subplan_get_plan, PlannerRun};
use types_pathnodes::PlannerInfo;

use backend_nodes_core::bitmapset as bms;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

/// `elog(ERROR, ...)` shorthand.
fn elog_error(msg: impl Into<alloc::string::String>) -> PgError {
    PgError::error(msg)
}

/// Convert a planner `Relids` ([`types_pathnodes::Bitmapset`]) into a
/// `types_nodes::Bitmapset<'mcx>` — both are word-vectors of param ids; the
/// boundary between the `outer_params` set (Relids) and the `Plan.extParam`
/// model (types_nodes) needs this bridge.
fn relids_to_bms<'mcx>(
    mcx: Mcx<'mcx>,
    r: &types_pathnodes::Relids,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    match r {
        None => Ok(None),
        Some(b) => {
            let mut out: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
            // Re-add each set bit; cheap and avoids depending on internal layout.
            let mut bit: i32 = -1;
            loop {
                bit = next_member_relids(b, bit);
                if bit < 0 {
                    break;
                }
                out = Some(bms::bms_add_member(mcx, out, bit)?);
            }
            Ok(out)
        }
    }
}

/// `bms_next_member` over a planner [`types_pathnodes::Bitmapset`].
fn next_member_relids(b: &types_pathnodes::Bitmapset, prevbit: i32) -> i32 {
    let words = &b.words;
    let mut bit = prevbit + 1;
    while (bit as usize) / 64 < words.len() {
        let wn = (bit as usize) / 64;
        let w = words[wn] >> ((bit as usize) % 64);
        if w != 0 {
            return bit + w.trailing_zeros() as i32;
        }
        bit = ((wn + 1) * 64) as i32;
    }
    -1
}

// ===========================================================================
// finalize_primnode_context
// ===========================================================================

struct FinalizeCtx<'mcx> {
    paramids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

// ===========================================================================
// SS_finalize_plan
// ===========================================================================

/// `SS_finalize_plan(root, plan)` (subselect.c): do final parameter processing
/// for a completed Plan, recursively computing extParam/allParam.
pub fn SS_finalize_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    plan: &mut Node<'mcx>,
) -> PgResult<()> {
    let valid_params = relids_to_bms(mcx, &root.outer_params)?;
    finalize_plan(mcx, root, run, Some(plan), -1, valid_params.as_deref(), None)?;
    Ok(())
}

// ===========================================================================
// finalize_plan
// ===========================================================================

/// `finalize_plan(root, plan, gather_param, valid_params, scan_params)`
/// (subselect.c): recursive processing of all nodes in the plan tree. Returns
/// the computed `allParam` set for the given Plan node.
#[allow(clippy::too_many_arguments)]
fn finalize_plan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    plan: Option<&mut Node<'mcx>>,
    mut gather_param: i32,
    valid_params: Option<&Bitmapset<'mcx>>,
    scan_params: Option<&Bitmapset<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let plan = match plan {
        None => return Ok(None),
        Some(p) => p,
    };

    let mut context = FinalizeCtx { paramids: None };
    let mut locally_added_param: i32 = -1;
    let mut nestloop_params: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;

    // Owned working copies of valid_params/scan_params we may extend per node
    // (C does `bms_add_member(bms_copy(valid_params), ...)`).
    let mut valid_params_owned: Option<PgBox<'mcx, Bitmapset<'mcx>>> =
        bms::bms_copy(mcx, valid_params)?;
    let mut scan_params_owned: Option<PgBox<'mcx, Bitmapset<'mcx>>> =
        bms::bms_copy(mcx, scan_params)?;

    // Examine any initPlans for their external params + output (setParam) params.
    let mut init_ext_param: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let mut init_set_param: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    {
        let init_plan_ids: alloc::vec::Vec<(i32, alloc::vec::Vec<i32>)> = {
            // Collect (child plan_id, setParam list) for each initPlan SubPlan.
            let head = plan.plan_head();
            let mut v = alloc::vec::Vec::new();
            if let Some(ips) = head.initPlan.as_ref() {
                for sp in ips.iter() {
                    let set: alloc::vec::Vec<i32> = sp.setParam.iter().copied().collect();
                    v.push((sp.plan_id, set));
                }
            }
            v
        };
        for (child_plan_id, set_params) in init_plan_ids {
            let initplan = planner_subplan_get_plan(run, root, child_plan_id);
            let ext = initplan.plan_head().extParam.as_deref();
            init_ext_param = bms::bms_add_members(mcx, init_ext_param, ext)?;
            for sp in set_params {
                init_set_param = Some(bms::bms_add_member(mcx, init_set_param, sp)?);
            }
        }
    }

    // Any setParams are validly referenceable in this node and children.
    if init_set_param.is_some() {
        valid_params_owned =
            bms::bms_union(mcx, valid_params_owned.as_deref(), init_set_param.as_deref())?;
    }

    // Find params in targetlist and qual.
    {
        let head = plan.plan_head();
        if let Some(tlist) = head.targetlist.as_ref() {
            for te in tlist.iter() {
                if let Some(e) = te.expr.as_deref() {
                    finalize_primnode(mcx, root, run, Some(e), &mut context)?;
                }
            }
        }
        if let Some(qual) = head.qual.as_ref() {
            for e in qual.iter() {
                finalize_primnode(mcx, root, run, Some(e), &mut context)?;
            }
        }
    }

    // Parallel-aware scan node depends on the parent Gather's rescan Param.
    if plan.plan_head().parallel_aware {
        if gather_param < 0 {
            return Err(elog_error("parallel-aware plan node is not below a Gather"));
        }
        context.paramids = Some(bms::bms_add_member(mcx, context.paramids.take(), gather_param)?);
    }

    // Node-type-specific fields.
    finalize_node_specific(
        mcx,
        root,
        run,
        plan,
        &mut context,
        &mut gather_param,
        &mut locally_added_param,
        &mut nestloop_params,
        &mut valid_params_owned,
        &mut scan_params_owned,
        scan_params,
    )?;

    // Process left and right child plans.
    let child_params = {
        let lefttree = plan.plan_head_mut().lefttree.as_deref_mut();
        finalize_plan(
            mcx,
            root,
            run,
            lefttree,
            gather_param,
            valid_params_owned.as_deref(),
            scan_params_owned.as_deref(),
        )?
    };
    context.paramids = bms::bms_add_members(mcx, context.paramids.take(), child_params.as_deref())?;

    if nestloop_params.is_some() {
        let union = bms::bms_union(
            mcx,
            nestloop_params.as_deref(),
            valid_params_owned.as_deref(),
        )?;
        let mut child_params = {
            let righttree = plan.plan_head_mut().righttree.as_deref_mut();
            finalize_plan(
                mcx,
                root,
                run,
                righttree,
                gather_param,
                union.as_deref(),
                scan_params_owned.as_deref(),
            )?
        };
        // ... and they don't count as parameters used at my level.
        child_params = bms::bms_difference(mcx, child_params.as_deref(), nestloop_params.as_deref())?;
        context.paramids =
            bms::bms_add_members(mcx, context.paramids.take(), child_params.as_deref())?;
    } else {
        let child_params = {
            let righttree = plan.plan_head_mut().righttree.as_deref_mut();
            finalize_plan(
                mcx,
                root,
                run,
                righttree,
                gather_param,
                valid_params_owned.as_deref(),
                scan_params_owned.as_deref(),
            )?
        };
        context.paramids =
            bms::bms_add_members(mcx, context.paramids.take(), child_params.as_deref())?;
    }

    // Any locally generated parameter doesn't count towards external deps.
    if locally_added_param >= 0 {
        context.paramids = bms::bms_del_member(context.paramids.take(), locally_added_param);
    }

    // Now we have all the paramids referenced in this node and children.
    if !bms::bms_is_subset(context.paramids.as_deref(), valid_params_owned.as_deref()) {
        return Err(elog_error("plan should not reference subplan's variable"));
    }

    // allParam = paramids ∪ initExtParam ∪ initSetParam.
    let all_param = bms::bms_union(mcx, context.paramids.as_deref(), init_ext_param.as_deref())?;
    let all_param = bms::bms_add_members(mcx, all_param, init_set_param.as_deref())?;
    // extParam = paramids ∪ initExtParam, minus initSetParam.
    let ext_param = bms::bms_union(mcx, context.paramids.as_deref(), init_ext_param.as_deref())?;
    let ext_param = bms::bms_del_members(ext_param, init_set_param.as_deref());

    // Store back into the plan node + return a copy of allParam.
    let all_param_ret = bms::bms_copy(mcx, all_param.as_deref())?;
    {
        let head = plan.plan_head_mut();
        head.allParam = all_param;
        head.extParam = ext_param;
    }
    Ok(all_param_ret)
}

/// The node-type-specific `switch (nodeTag(plan))` arm of `finalize_plan`.
#[allow(clippy::too_many_arguments)]
fn finalize_node_specific<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    plan: &mut Node<'mcx>,
    context: &mut FinalizeCtx<'mcx>,
    gather_param: &mut i32,
    locally_added_param: &mut i32,
    nestloop_params: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    valid_params_owned: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    scan_params_owned: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    scan_params: Option<&Bitmapset<'mcx>>,
) -> PgResult<()> {
    // Helper: finalize a slice of Exprs into context.
    macro_rules! fin_exprs {
        ($slice:expr) => {{
            for e in $slice.iter() {
                finalize_primnode(mcx, root, run, Some(e), context)?;
            }
        }};
    }
    macro_rules! add_scan_params {
        () => {{
            context.paramids =
                bms::bms_add_members(mcx, context.paramids.take(), scan_params)?;
        }};
    }

    match plan {
        Node::Result(r) => {
            if let Some(rcq) = r.resconstantqual.as_ref() {
                fin_exprs!(rcq);
            }
        }
        Node::SeqScan(_) => {
            add_scan_params!();
        }
        Node::SampleScan(s) => {
            if let Some(ts) = s.tablesample.as_ref() {
                // finalize_primnode((Node *) sampleScan->tablesample): walk the
                // TableSampleClause's `args` + `repeatable` expression children.
                if let Some(args) = ts.args.as_ref() {
                    fin_exprs!(args);
                }
                if let Some(rep) = ts.repeatable.as_deref() {
                    finalize_primnode(mcx, root, run, Some(rep), context)?;
                }
            }
            add_scan_params!();
        }
        Node::IndexScan(s) => {
            if let Some(iq) = s.indexqual.as_ref() {
                fin_exprs!(iq);
            }
            if let Some(io) = s.indexorderby.as_ref() {
                fin_exprs!(io);
            }
            add_scan_params!();
        }
        Node::IndexOnlyScan(s) => {
            if let Some(iq) = s.indexqual.as_ref() {
                fin_exprs!(iq);
            }
            if let Some(rq) = s.recheckqual.as_ref() {
                fin_exprs!(rq);
            }
            if let Some(io) = s.indexorderby.as_ref() {
                fin_exprs!(io);
            }
            add_scan_params!();
        }
        Node::BitmapIndexScan(s) => {
            if let Some(iq) = s.indexqual.as_ref() {
                fin_exprs!(iq);
            }
        }
        Node::TidScan(s) => {
            if let Some(tq) = s.tidquals.as_ref() {
                fin_exprs!(tq);
            }
            add_scan_params!();
        }
        Node::TidRangeScan(s) => {
            if let Some(tq) = s.tidrangequals.as_ref() {
                fin_exprs!(tq);
            }
            add_scan_params!();
        }
        Node::SubqueryScan(sscan) => {
            // Recurse finalize_plan on the subquery with its subroot's
            // outer_params. The subroot PlannerInfo is not readily reachable per
            // SubqueryScan in this model; the subquery_params come from the
            // ported `base_rel_subroot_outer_params` seam, and the recursive
            // finalize of the sub-plan needs the subroot as `root` — which this
            // model cannot hand back through the seam. We recurse finalize on the
            // sub-plan tree with the SAME root and the subquery_params (this is
            // exact for param-id bookkeeping: finalize_plan only consults `root`
            // for `planner_subplan_get_plan` of SubPlan/initPlan children, which
            // address the shared `glob->subplans`).
            let scanrelid = sscan.scan.scanrelid as i32;
            let subquery_params_relids =
                initext::base_rel_subroot_outer_params::call(root, scanrelid);
            let mut subquery_params = relids_to_bms(mcx, &subquery_params_relids)?;
            if *gather_param >= 0 {
                subquery_params =
                    Some(bms::bms_add_member(mcx, subquery_params, *gather_param)?);
            }
            if let Some(subplan) = sscan.subplan.as_deref_mut() {
                finalize_plan(
                    mcx,
                    root,
                    run,
                    Some(subplan),
                    *gather_param,
                    subquery_params.as_deref(),
                    None,
                )?;
            }
            // Add its extParams to the parent's params.
            let sub_ext = sscan
                .subplan
                .as_deref()
                .and_then(|p| p.plan_head().extParam.as_deref());
            context.paramids = bms::bms_add_members(mcx, context.paramids.take(), sub_ext)?;
            add_scan_params!();
        }
        Node::FunctionScan(fscan) => {
            if let Some(functions) = fscan.functions.as_mut() {
                for rtfunc in functions.iter_mut() {
                    let mut funccontext = FinalizeCtx { paramids: None };
                    if let Some(fe) = rtfunc.funcexpr.as_deref() {
                        if let Node::Expr(e) = fe {
                            finalize_primnode(mcx, root, run, Some(e), &mut funccontext)?;
                        }
                    }
                    // Remember results for execution.
                    rtfunc.funcparams = bms::bms_copy(mcx, funccontext.paramids.as_deref())?;
                    // Add the function's params to the overall set.
                    context.paramids = bms::bms_add_members(
                        mcx,
                        context.paramids.take(),
                        funccontext.paramids.as_deref(),
                    )?;
                }
            }
            add_scan_params!();
        }
        Node::TableFuncScan(tfs) => {
            // tablefunc carries expression children; finalize them.
            finalize_tablefunc(mcx, root, run, &tfs.tablefunc, context)?;
            add_scan_params!();
        }
        Node::ValuesScan(vs) => {
            for row in vs.values_lists.iter() {
                fin_exprs!(row);
            }
            add_scan_params!();
        }
        Node::CteScan(cte) => {
            // Find the referenced CTE plan and incorporate its external paramids.
            let plan_id = cte.ctePlanId;
            let glob = root
                .glob
                .as_ref()
                .expect("finalize_plan CteScan: root->glob is NULL");
            if plan_id < 1 || (plan_id as usize) > glob.subplans.len() {
                return Err(elog_error(alloc::format!(
                    "could not find plan for CteScan referencing plan ID {plan_id}"
                )));
            }
            let cteplan = planner_subplan_get_plan(run, root, plan_id);
            let ext = cteplan.plan_head().extParam.as_deref();
            context.paramids = bms::bms_add_members(mcx, context.paramids.take(), ext)?;
            add_scan_params!();
        }
        Node::WorkTableScan(wts) => {
            context.paramids = Some(bms::bms_add_member(mcx, context.paramids.take(), wts.wtParam)?);
            add_scan_params!();
        }
        Node::NamedTuplestoreScan(_) => {
            add_scan_params!();
        }
        Node::ForeignScan(fscan) => {
            if let Some(fe) = fscan.fdw_exprs.as_ref() {
                fin_exprs!(fe);
            }
            if let Some(frq) = fscan.fdw_recheck_quals.as_ref() {
                fin_exprs!(frq);
            }
            add_scan_params!();
        }
        Node::CustomScan(cscan) => {
            if let Some(ce) = cscan.custom_exprs.as_ref() {
                fin_exprs!(ce);
            }
            add_scan_params!();
            // Child nodes if any.
            if let Some(plans) = cscan.custom_plans.as_mut() {
                for child in plans.iter_mut() {
                    {
                        let cp = finalize_plan(
                            mcx,
                            root,
                            run,
                            Some(child),
                            *gather_param,
                            valid_params_owned.as_deref(),
                            scan_params_owned.as_deref(),
                        )?;
                        context.paramids =
                            bms::bms_add_members(mcx, context.paramids.take(), cp.as_deref())?;
                    }
                }
            }
        }
        Node::ModifyTable(mt) => {
            *locally_added_param = mt.epqParam;
            *valid_params_owned =
                Some(bms::bms_add_member(mcx, valid_params_owned.take(), *locally_added_param)?);
            *scan_params_owned =
                Some(bms::bms_add_member(mcx, scan_params_owned.take(), *locally_added_param)?);
            if let Some(rls) = mt.returningLists.as_ref() {
                for rl in rls.iter() {
                    for te in rl.iter() {
                        if let Some(e) = te.expr.as_deref() {
                            finalize_primnode(mcx, root, run, Some(e), context)?;
                        }
                    }
                }
            }
            if let Some(ocs) = mt.onConflictSet.as_ref() {
                for te in ocs.iter() {
                    if let Some(e) = te.expr.as_deref() {
                        finalize_primnode(mcx, root, run, Some(e), context)?;
                    }
                }
            }
            if let Some(ocw) = mt.onConflictWhere.as_ref() {
                fin_exprs!(ocw);
            }
        }
        Node::Append(a) => {
            for child in a.appendplans.iter_mut() {
                let cp = finalize_plan(
                    mcx,
                    root,
                    run,
                    Some(child),
                    *gather_param,
                    valid_params_owned.as_deref(),
                    scan_params_owned.as_deref(),
                )?;
                context.paramids =
                    bms::bms_add_members(mcx, context.paramids.take(), cp.as_deref())?;
            }
        }
        Node::MergeAppend(ma) => {
            for child in ma.mergeplans.iter_mut() {
                let cp = finalize_plan(
                    mcx,
                    root,
                    run,
                    Some(child),
                    *gather_param,
                    valid_params_owned.as_deref(),
                    scan_params_owned.as_deref(),
                )?;
                context.paramids =
                    bms::bms_add_members(mcx, context.paramids.take(), cp.as_deref())?;
            }
        }
        Node::BitmapAnd(ba) => {
            for child in ba.bitmapplans.iter_mut() {
                let cp = finalize_plan(
                    mcx,
                    root,
                    run,
                    Some(child),
                    *gather_param,
                    valid_params_owned.as_deref(),
                    scan_params_owned.as_deref(),
                )?;
                context.paramids =
                    bms::bms_add_members(mcx, context.paramids.take(), cp.as_deref())?;
            }
        }
        Node::NestLoop(nl) => {
            if let Some(jq) = nl.join.joinqual.as_ref() {
                fin_exprs!(jq);
            }
            for nlp in nl.nestParams.iter() {
                *nestloop_params =
                    Some(bms::bms_add_member(mcx, nestloop_params.take(), nlp.paramno)?);
            }
        }
        Node::MergeJoin(mj) => {
            if let Some(jq) = mj.join.joinqual.as_ref() {
                fin_exprs!(jq);
            }
            fin_exprs!(mj.mergeclauses);
        }
        Node::HashJoin(hj) => {
            if let Some(jq) = hj.join.joinqual.as_ref() {
                fin_exprs!(jq);
            }
            if let Some(hc) = hj.hashclauses.as_ref() {
                for n in hc.iter() {
                    if let Node::Expr(e) = n {
                        finalize_primnode(mcx, root, run, Some(e), context)?;
                    }
                }
            }
        }
        Node::Hash(h) => {
            if let Some(hk) = h.hashkeys.as_ref() {
                for n in hk.iter() {
                    if let Node::Expr(e) = n {
                        finalize_primnode(mcx, root, run, Some(e), context)?;
                    }
                }
            }
        }
        Node::Limit(l) => {
            if let Some(o) = l.limitOffset.as_deref() {
                finalize_primnode(mcx, root, run, Some(o), context)?;
            }
            if let Some(c) = l.limitCount.as_deref() {
                finalize_primnode(mcx, root, run, Some(c), context)?;
            }
        }
        Node::RecursiveUnion(ru) => {
            *locally_added_param = ru.wtParam;
            *valid_params_owned =
                Some(bms::bms_add_member(mcx, valid_params_owned.take(), *locally_added_param)?);
            // wtParam does *not* get added to scan_params.
        }
        Node::Agg(agg) => {
            // AGG_HASHED plans need to know which Params are referenced in
            // aggregate calls.
            if agg.aggstrategy == types_nodes::nodeagg::AGG_HASHED {
                let mut aggcontext = FinalizeCtx { paramids: None };
                if let Some(tlist) = agg.plan.targetlist.as_ref() {
                    for te in tlist.iter() {
                        if let Some(e) = te.expr.as_deref() {
                            finalize_agg_primnode(mcx, root, run, Some(e), &mut aggcontext)?;
                        }
                    }
                }
                if let Some(qual) = agg.plan.qual.as_ref() {
                    for e in qual.iter() {
                        finalize_agg_primnode(mcx, root, run, Some(e), &mut aggcontext)?;
                    }
                }
                agg.agg_params = aggcontext.paramids;
            }
        }
        Node::WindowAgg(wa) => {
            if let Some(so) = wa.startOffset.as_deref() {
                finalize_primnode(mcx, root, run, Some(so), context)?;
            }
            if let Some(eo) = wa.endOffset.as_deref() {
                finalize_primnode(mcx, root, run, Some(eo), context)?;
            }
        }
        Node::Gather(g) => {
            *locally_added_param = g.rescan_param;
            if *locally_added_param >= 0 {
                *valid_params_owned =
                    Some(bms::bms_add_member(mcx, valid_params_owned.take(), *locally_added_param)?);
                debug_assert!(*gather_param < 0);
                *gather_param = *locally_added_param;
            }
        }
        Node::GatherMerge(gm) => {
            *locally_added_param = gm.rescan_param;
            if *locally_added_param >= 0 {
                *valid_params_owned =
                    Some(bms::bms_add_member(mcx, valid_params_owned.take(), *locally_added_param)?);
                debug_assert!(*gather_param < 0);
                *gather_param = *locally_added_param;
            }
        }
        Node::Memoize(m) => {
            fin_exprs!(m.param_exprs);
        }
        // No node-type-specific fields need fixing.
        Node::ProjectSet(_)
        | Node::Material(_)
        | Node::Sort(_)
        | Node::IncrementalSort(_)
        | Node::Unique(_)
        | Node::SetOp(_)
        | Node::Group(_) => {}
        other => {
            return Err(elog_error(alloc::format!(
                "unrecognized node type: {:?}",
                other.node_tag()
            )));
        }
    }
    Ok(())
}

// ===========================================================================
// finalize_primnode
// ===========================================================================

/// `finalize_primnode(node, context)` (subselect.c): add IDs of all PARAM_EXEC
/// params appearing (or appearing-after-setrefs) in the expression tree.
fn finalize_primnode<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    node: Option<&Expr>,
    context: &mut FinalizeCtx<'mcx>,
) -> PgResult<bool> {
    let node = match node {
        None => return Ok(false),
        Some(n) => n,
    };
    match node {
        Expr::Param(p) => {
            if p.paramkind == ParamKind::PARAM_EXEC {
                context.paramids =
                    Some(bms::bms_add_member(mcx, context.paramids.take(), p.paramid)?);
            }
            return Ok(false);
        }
        Expr::Aggref(aggref) => {
            // Check whether the aggregate will be replaced by a Param referencing
            // a subquery output during setrefs.c.
            if let Some(paramid) = initext::find_minmax_agg_replacement_param::call(root, aggref) {
                context.paramids = Some(bms::bms_add_member(mcx, context.paramids.take(), paramid)?);
            }
            // Fall through to examine the agg's arguments.
        }
        Expr::SubPlan(splan) => {
            let subplan = &splan.0;
            // Recurse into the testexpr, but not into the Plan.
            if let Some(te) = subplan.testexpr.as_deref() {
                finalize_primnode(mcx, root, run, Some(te), context)?;
            }
            // Remove output paramIds referenced in the testexpr.
            for id in subplan.paramIds.iter() {
                context.paramids = bms::bms_del_member(context.paramids.take(), *id);
            }
            // Also examine args list.
            for arg in subplan.args.iter() {
                finalize_primnode(mcx, root, run, Some(arg), context)?;
            }
            // Add params needed by the subplan, excluding those we pass down.
            let child = planner_subplan_get_plan(run, root, subplan.plan_id);
            let mut subparamids = bms::bms_copy(mcx, child.plan_head().extParam.as_deref())?;
            for id in subplan.parParam.iter() {
                subparamids = bms::bms_del_member(subparamids, *id);
            }
            context.paramids = bms::bms_join(context.paramids.take(), subparamids);
            return Ok(false);
        }
        _ => {}
    }
    // expression_tree_walker(node, finalize_primnode, context): recurse into
    // children only. Capture any error from the fallible body.
    let mut err: Option<PgError> = None;
    backend_nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut |child: &Expr| {
        if err.is_some() {
            return true;
        }
        match finalize_primnode(mcx, root, run, Some(child), context) {
            Ok(_) => false,
            Err(e) => {
                err = Some(e);
                true
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(false),
    }
}

/// `finalize_agg_primnode(node, context)` (subselect.c): find Aggref nodes and
/// add PARAM_EXEC params within their aggregated arguments.
fn finalize_agg_primnode<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    node: Option<&Expr>,
    context: &mut FinalizeCtx<'mcx>,
) -> PgResult<bool> {
    let node = match node {
        None => return Ok(false),
        Some(n) => n,
    };
    if let Expr::Aggref(agg) = node {
        // We should not consider the direct arguments, if any.
        for te in agg.args.iter() {
            if let Some(e) = te.expr.as_deref() {
                finalize_primnode(mcx, root, run, Some(e), context)?;
            }
        }
        if let Some(filt) = agg.aggfilter.as_deref() {
            finalize_primnode(mcx, root, run, Some(filt), context)?;
        }
        return Ok(false); // no Aggrefs below here
    }
    let mut err: Option<PgError> = None;
    backend_nodes_core::nodefuncs::expression_tree_walker(Some(node), &mut |child: &Expr| {
        if err.is_some() {
            return true;
        }
        match finalize_agg_primnode(mcx, root, run, Some(child), context) {
            Ok(_) => false,
            Err(e) => {
                err = Some(e);
                true
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(false),
    }
}

/// Finalize a `TableFunc`'s expression children
/// (`finalize_primnode((Node *) tablefunc)`): the planner walks its `docexpr`,
/// `rowexpr`, `colexprs`, `coldefexprs`, namespace and value/path exprs.
fn finalize_tablefunc<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    run: &PlannerRun<'mcx>,
    tf: &types_nodes::primnodes::TableFunc<'mcx>,
    context: &mut FinalizeCtx<'mcx>,
) -> PgResult<()> {
    if let Some(ns) = tf.ns_uris.as_ref() {
        for e in ns.iter() {
            finalize_primnode(mcx, root, run, Some(&**e), context)?;
        }
    }
    if let Some(de) = tf.docexpr.as_deref() {
        finalize_primnode(mcx, root, run, Some(de), context)?;
    }
    if let Some(re) = tf.rowexpr.as_deref() {
        finalize_primnode(mcx, root, run, Some(re), context)?;
    }
    if let Some(ce) = tf.colexprs.as_ref() {
        for e in ce.iter().flatten() {
            finalize_primnode(mcx, root, run, Some(&**e), context)?;
        }
    }
    if let Some(cde) = tf.coldefexprs.as_ref() {
        for e in cde.iter().flatten() {
            finalize_primnode(mcx, root, run, Some(&**e), context)?;
        }
    }
    Ok(())
}

// ===========================================================================
// SS_charge_for_initplans / SS_compute_initplan_cost / SS_attach_initplans
// ===========================================================================

/// `SS_compute_initplan_cost(init_plans, &initplan_cost, &unsafe_initplans)`
/// (subselect.c). `init_plans` is `root.init_plans` (NodeIds → `Expr::SubPlan`).
pub fn SS_compute_initplan_cost(root: &PlannerInfo) -> (f64, bool) {
    let mut initplan_cost = 0.0_f64;
    let mut unsafe_initplans = false;
    for &ipl in &root.init_plans {
        if let Expr::SubPlan(splan) = root.node(ipl) {
            initplan_cost += splan.0.startup_cost + splan.0.per_call_cost;
            if !splan.0.parallel_safe {
                unsafe_initplans = true;
            }
        }
    }
    (initplan_cost, unsafe_initplans)
}

/// `SS_attach_initplans(root, plan)` (subselect.c): attach any initplans created
/// in the current query level to the specified plan node. The owned plan tree's
/// `Plan.initPlan` is `Option<PgVec<SubPlan>>`; we materialize each
/// `Expr::SubPlan` from `root.init_plans` into that list.
pub fn SS_attach_initplans<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    plan: &mut Node<'mcx>,
) -> PgResult<()> {
    let mut list: mcx::PgVec<'mcx, types_nodes::primnodes::SubPlan<'mcx>> = mcx::PgVec::new_in(mcx);
    for &ipl in &root.init_plans {
        if let Expr::SubPlan(splan) = root.node(ipl) {
            list.push(splan.0.clone_in(mcx)?);
        }
    }
    plan.plan_head_mut().initPlan = Some(list);
    Ok(())
}
