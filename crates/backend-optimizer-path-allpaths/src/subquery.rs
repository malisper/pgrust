//! Subquery / CTE pathlist machinery.
//!
//! `set_subquery_pathlist` (allpaths.c:2528) plans an `RTE_SUBQUERY` by running
//! `subquery_planner` over the `rte->subquery` and building SubqueryScan paths;
//! its pushdown-safety cluster (`subquery_is_pushdown_safe` /
//! `qual_is_pushdown_safe` / `remove_unused_subquery_outputs` /
//! `check_and_push_window_quals` / …) reads the `Query` subtrees (`targetList`,
//! `setOperations`, `windowClause`, `distinctClause`, …). It is owned by the
//! (unported) planner-entry crate that runs `subquery_planner`, so it routes
//! through a planner-entry-owned seam until that keystone lands.
//!
//! `set_cte_pathlist` (2906) and `set_worktable_pathlist` (3039) resolve a CTE
//! by name out of `cteroot->parse->cteList`. Those subtrees are now carried as
//! owned `Query` values in the [`PlannerRun`] store (interned by
//! `SS_process_ctes`), and `glob->subplans`/`subpaths`/`cte_plan_ids` are all
//! populated by the time `set_rel_size` runs, so `set_cte_pathlist` is ported
//! here in full. `set_worktable_pathlist` (the self-reference / recursive leg)
//! still routes to its owner.

extern crate alloc;
use alloc::format;

use types_core::primitive::Index;
use types_error::{PgError, PgResult};
use types_pathnodes::planner_run::{planner_subplan_get_plan, PlannerRun};
use types_pathnodes::{PlannerInfo, RelId, TargetEntryNode};

use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;

/// `set_subquery_pathlist` (allpaths.c:2528) — SubqueryScan access paths for a
/// subquery RTE. Routes to the planner-entry owner (it runs `subquery_planner`
/// over the owned `Query` subtree and applies the pushdown-safety cluster).
pub fn set_subquery_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::set_subquery_pathlist::call(root, rel, rti)
}

/// `set_cte_pathlist` (allpaths.c:2906) — the single access path for a
/// non-self-reference CTE RTE. Walks `cteroot->parse->cteList` (resolving
/// `levelsup` parent roots) to find the CTE by name, reads its `plan_id` from
/// `cteroot->cte_plan_ids`, fetches the previously-built plan/path from
/// `glob->subplans`/`subpaths`, sizes the rel from the plan's `plan_rows`,
/// converts the source path's pathkeys to the outer query's representation, and
/// adds a CteScan path.
pub fn set_cte_pathlist<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    // RangeTblEntry for this CTE scan.
    let rte = types_pathnodes::planner_run::planner_rt_fetch(run, root, rti);
    let ctename = rte
        .ctename
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let mut levelsup = rte.ctelevelsup;

    // Find the referenced CTE root by walking up `parent_root` `levelsup` times.
    let mut cteroot: &PlannerInfo = root;
    while levelsup > 0 {
        levelsup -= 1;
        cteroot = cteroot
            .parent_root
            .as_deref()
            .ok_or_else(|| PgError::error(format!("bad levelsup for CTE \"{ctename}\"")))?;
    }

    // ndx = index of the matching CTE in cteroot->parse->cteList. (cte_plan_ids
    // can be shorter than cteList when this is a side-reference from another CTE
    // still being planned, so we must not zip the two lists.)
    let mut ndx: usize = 0;
    let mut found = false;
    {
        let parse = run.resolve(cteroot.parse);
        for cte_node in parse.cteList.iter() {
            let this_name = match cte_node.as_commontableexpr() {
                Some(c) => c.ctename.as_ref().map(|s| s.as_str()).unwrap_or(""),
                None => return Err(PgError::error("cteList element is not a CommonTableExpr")),
            };
            if this_name == ctename {
                found = true;
                break;
            }
            ndx += 1;
        }
    }
    if !found {
        return Err(PgError::error(format!("could not find CTE \"{ctename}\"")));
    }
    if ndx >= cteroot.cte_plan_ids.len() {
        return Err(PgError::error(format!("could not find plan for CTE \"{ctename}\"")));
    }
    let plan_id = cteroot.cte_plan_ids[ndx];
    if plan_id <= 0 {
        return Err(PgError::error(format!("no plan was made for CTE \"{ctename}\"")));
    }

    // ctepath = list_nth(glob->subpaths, plan_id - 1); cteplan = list_nth(
    // glob->subplans, plan_id - 1). The subpath PathId resolves in the subplan's
    // own subroot path arena; the subplan Plan resolves through the run store.
    let sub_plan_id = {
        let glob = root
            .glob
            .as_ref()
            .ok_or_else(|| PgError::error("set_cte_pathlist: root->glob is NULL"))?;
        debug_assert_eq!(glob.subpaths.len(), glob.subplans.len());
        glob.subplans[(plan_id as usize) - 1]
    };

    // cteplan->plan_rows, plus a NodeId rendering of cteplan->targetlist in the
    // outer root's arena (convert_subquery_pathkeys matches TLEs by
    // resno/ressortgroupref/resjunk; the expr is interned for completeness).
    // Collect first (borrowing the run), then intern (borrowing root mutably).
    struct PendingTle {
        expr: types_nodes::primnodes::Expr,
        resno: types_core::primitive::AttrNumber,
        ressortgroupref: Index,
        resorigtbl: types_core::primitive::Oid,
        resorigcol: types_core::primitive::AttrNumber,
        resjunk: bool,
    }
    let (cte_plan_rows, pending) = {
        let cteplan = planner_subplan_get_plan(run, root, plan_id);
        let head = cteplan.plan_head();
        let rows = head.plan_rows;
        let mut pend = alloc::vec::Vec::new();
        if let Some(tl) = head.targetlist.as_ref() {
            pend.reserve(tl.len());
            for tle in tl.iter() {
                pend.push(PendingTle {
                    expr: tle
                        .expr
                        .as_deref()
                        .cloned()
                        .unwrap_or(types_nodes::primnodes::Expr::Const(Default::default())),
                    resno: tle.resno,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                });
            }
        }
        (rows, pend)
    };
    let mut cte_tlist_ids = alloc::vec::Vec::with_capacity(pending.len());
    for p in pending {
        let expr_id = root.alloc_node(p.expr);
        cte_tlist_ids.push(root.alloc_targetentry(TargetEntryNode {
            expr: expr_id,
            resno: p.resno,
            resname: None,
            ressortgroupref: p.ressortgroupref,
            resorigtbl: p.resorigtbl,
            resorigcol: p.resorigcol,
            resjunk: p.resjunk,
        }));
    }

    // ctepath->pathkeys live in the subplan's subroot path arena. `sub_plan_id`
    // is the PlanId handle stored at glob->subplans[plan_id-1]; it keys the
    // parallel subroots/subpaths stores.
    let ctepath_pathkeys = {
        let subroot = run.resolve_subroot(sub_plan_id);
        let ctepath = run.resolve_subpath(sub_plan_id);
        subroot.path(ctepath).base().pathkeys.clone()
    };

    // Mark rel with estimated output rows, width, etc.
    backend_optimizer_path_costsize::sizeest::set_cte_size_estimates(run, root, rel, cte_plan_rows);

    // Convert the ctepath's pathkeys to the outer query's representation.
    let pathkeys = backend_optimizer_path_pathkeys::convert_subquery_pathkeys(
        root,
        rel,
        &ctepath_pathkeys,
        &cte_tlist_ids,
    );

    // We don't support pushing join clauses into a CTE scan's quals, but it may
    // still be parameterized by LATERAL refs in its tlist.
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);

    let path = pathnode::create_ctescan_path::call(root, run, rel, pathkeys, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

/// `set_worktable_pathlist` (allpaths.c:3039) — the access path for a
/// self-reference (recursive) CTE RTE. Reads `cteroot->non_recursive_path` after
/// resolving the CTE by name; routed to the planner-entry owner.
pub fn set_worktable_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::set_worktable_pathlist::call(root, rel, rti)
}

/// Planner-entry-owned seams for the subquery/CTE vertical (Query-value
/// keystone). Installed by the planner-entry crate (`subquery_planner` /
/// `planner.c`) once it lands; registered in `CONTRACT_RECONCILE_PENDING`
/// meanwhile.
pub mod seams {
    use types_core::primitive::Index;
    use types_error::PgResult;
    use types_pathnodes::{PlannerInfo, RelId};

    seam_core::seam!(
        /// `set_subquery_pathlist(root, rel, rti, rte)` (allpaths.c) — runs
        /// `subquery_planner` over `rte->subquery` and builds SubqueryScan paths.
        pub fn set_subquery_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()>
    );
    seam_core::seam!(
        /// `set_worktable_pathlist(root, rel, rti, rte)` (allpaths.c).
        pub fn set_worktable_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()>
    );
}
