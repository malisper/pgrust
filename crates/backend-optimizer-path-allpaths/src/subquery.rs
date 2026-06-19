//! Subquery / CTE pathlist machinery.
//!
//! `set_subquery_pathlist` (allpaths.c:2528) plans an `RTE_SUBQUERY` by running
//! `subquery_planner` over the `rte->subquery` (via the planner-owned
//! `subquery_planner_for_fromsubquery` seam — the planner unit owns
//! `subquery_planner`) and building SubqueryScan paths. Its pushdown-safety
//! cluster (`subquery_is_pushdown_safe` / `qual_is_pushdown_safe` /
//! `remove_unused_subquery_outputs` / `check_and_push_window_quals` / …, in
//! [`crate::pushdown`]) reads/mutates the owned subquery `Query` subtrees
//! (`targetList`, `setOperations`, `windowClause`, `distinctClause`, …). Each
//! subroot path is imported into the outer root's arena via the cross-root
//! `import_path_from_subroot` primitive before `create_subqueryscan_path` wraps
//! it (the subroot/outer arenas are separate; a raw subroot `PathId` is
//! meaningless in the outer arena).
//!
//! `set_cte_pathlist` (2906) and `set_worktable_pathlist` (3039) resolve a CTE
//! by name out of `cteroot->parse->cteList`. Those subtrees are now carried as
//! owned `Query` values in the [`PlannerRun`] store (interned by
//! `SS_process_ctes`), and `glob->subplans`/`subpaths`/`cte_plan_ids` are all
//! populated by the time `set_rel_size` runs, so `set_cte_pathlist` is ported
//! here in full. `set_worktable_pathlist` (the self-reference / recursive leg)
//! reads `cteroot->non_recursive_path` (set by `generate_recursion_path`) and is
//! likewise ported here in full.

extern crate alloc;
use alloc::format;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::Index;
use types_error::{PgError, PgResult};
use types_pathnodes::planner_run::{planner_subplan_get_plan, PlannerRun};
use types_pathnodes::{PathId, PlannerInfo, RelId, TargetEntryNode, UPPERREL_FINAL};

use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;

/// `set_subquery_pathlist` (allpaths.c:2528) — build SubqueryScan access paths
/// for a plain `RTE_SUBQUERY` in the FROM clause. Copies the subquery `Query`,
/// considers pushing the rel's baserestrictinfo down into it (the pushdown-
/// safety cluster), plans the (possibly modified) subquery into its own subroot
/// via the planner-owned `subquery_planner_for_fromsubquery` seam, then for each
/// path the subroot produced imports it into the outer arena
/// (`import_path_from_subroot`) and wraps it in a SubqueryScanPath.
pub fn set_subquery_pathlist<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    // parse = root->parse; subquery = rte->subquery (C:2531-2532).
    //
    // Must copyObject the subquery so planning doesn't scribble on the RTE
    // contents (C:2546). The owned model already hands us a fresh value via
    // `clone_in`, satisfying the same invariant.
    let rte_id = root.simple_rte_array[rti as usize];
    let mut subquery = run
        .resolve_rte(rte_id)
        .subquery
        .as_deref()
        .ok_or_else(|| PgError::error("set_subquery_pathlist: RTE_SUBQUERY has no subquery"))?
        .clone_in(mcx)?;

    // If it's a LATERAL subquery it may reference Vars of the current query
    // level, requiring parameterization (C:2553).
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);

    // Consider pushing the rel's restriction clauses down into the subquery
    // (C:2564-2622). `subquery_is_pushdown_safe` / `qual_is_pushdown_safe` /
    // `subquery_push_qual` / `check_and_push_window_quals` form the pushdown-
    // safety cluster ported in [`crate::pushdown`]; they operate on the owned
    // (mutable) subquery Query. `run_cond_attrs` collects the attnos pushed into
    // WindowAgg run conditions so `remove_unused_subquery_outputs` does not treat
    // them as unused.
    let security_barrier = run.resolve_rte(rte_id).security_barrier;
    let mut safety_info = crate::pushdown::PushdownSafetyInfo::new(subquery.targetList.len());
    safety_info.unsafe_leaky = security_barrier;

    let mut run_cond_attrs: types_pathnodes::Relids = None;

    // The baserestrictinfo clauses are arena RestrictInfos; materialize the
    // (rinfo_id, clause NodeId, pseudoconstant) triples up front so we can read
    // them while mutating the subquery.
    let baserestrict: Vec<(types_pathnodes::RinfoId, bool)> = root
        .rel(rel)
        .baserestrictinfo
        .iter()
        .map(|&rid| (rid, root.rinfo(rid).pseudoconstant))
        .collect();

    if !baserestrict.is_empty()
        && crate::pushdown::subquery_is_pushdown_safe(mcx, &subquery, &subquery, &mut safety_info)?
    {
        // OK to consider pushing down individual quals (C:2585).
        let mut upperrestrictlist: Vec<types_pathnodes::RinfoId> = Vec::new();
        for (rinfo_id, pseudoconstant) in baserestrict.iter().copied() {
            if pseudoconstant {
                // Don't push pseudoconstants; keep a gating qual above (C:2595).
                upperrestrictlist.push(rinfo_id);
                continue;
            }
            // clause = (Node *) rinfo->clause — an arena expr; materialize an
            // owned copy to push into / test against the owned subquery.
            let clause_id = root.rinfo(rinfo_id).clause;
            let clause = root.node(clause_id).clone();

            match crate::pushdown::qual_is_pushdown_safe(&subquery, rti, &clause, &safety_info)? {
                crate::pushdown::PushdownSafe::Safe => {
                    // subquery_push_qual reads the RTE; clone it out first so the
                    // immutable `run` borrow does not overlap the `&mut subquery`.
                    let rte = run.resolve_rte(rte_id).clone_in(mcx)?;
                    crate::pushdown::subquery_push_qual(mcx, &mut subquery, &rte, rti, &clause)?;
                }
                crate::pushdown::PushdownSafe::WindowclauseRuncond => {
                    if !subquery.hasWindowFuncs
                        || crate::pushdown::check_and_push_window_quals(
                            mcx,
                            &mut subquery,
                            rti,
                            &clause,
                            &mut run_cond_attrs,
                        )?
                    {
                        upperrestrictlist.push(rinfo_id);
                    }
                }
                crate::pushdown::PushdownSafe::Unsafe => {
                    upperrestrictlist.push(rinfo_id);
                }
            }
        }
        root.rel_mut(rel).baserestrictinfo = upperrestrictlist;
    }

    // The upper query might not use all subquery output columns; trim if so
    // (C:2632), preserving the WindowAgg run-condition attrs.
    crate::pushdown::remove_unused_subquery_outputs(mcx, root, rel, &mut subquery, run_cond_attrs)?;

    // Pass the outer tuple_fraction down only if the outer level has no joining,
    // aggregation, or sorting to do (C:2641-2651).
    let tuple_fraction = {
        let parse = run.resolve(root.parse);
        // bms_membership(root->all_baserels) == BMS_MULTIPLE (C:2649).
        const BMS_MULTIPLE: i32 = 2;
        let multiple = bms::relids_membership::call(&root.all_baserels) == BMS_MULTIPLE;
        if parse.hasAggs
            || !parse.groupClause.is_empty()
            || !parse.groupingSets.is_empty()
            || root.hasHavingQual
            || !parse.distinctClause.is_empty()
            || !parse.sortClause.is_empty()
            || multiple
        {
            0.0
        } else {
            root.tuple_fraction
        }
    };

    // plan_params should not be in use in the current query level (C:2654).
    debug_assert!(root.plan_params.is_empty());

    // Generate a subroot and Paths for the subquery (C:2657). Intern the
    // (modified) subquery, move the shared glob down into the planner, run
    // subquery_planner, and move the (mutated) glob back onto the outer root.
    //
    // Capture the subquery output-column count NOW (= list_length(
    // subquery->targetList), the C trivial_pathtarget comparand) before the
    // subquery is moved into the run.
    let subquery_tlist_len = subquery.targetList.len();
    let subquery_id = run.intern(subquery);
    let glob = *root
        .glob
        .take()
        .ok_or_else(|| PgError::error("set_subquery_pathlist: outer root has no glob"))?;
    // C passes the outer `root` as the subroot's parent_root so the subquery's
    // CTE / upper-Var references can walk up to this level. The owned model
    // moves the outer root in by value and recovers it from `subroot.parent_root`
    // afterwards (any upper-Var plan_params land on it, exactly as in C).
    let parent_root = core::mem::take(root);
    let mut subroot = backend_optimizer_plan_planner_seams::subquery_planner_for_fromsubquery::call(
        mcx,
        run,
        glob,
        subquery_id,
        parent_root,
        tuple_fraction,
    )?;
    *root = *subroot
        .parent_root
        .take()
        .expect("set_subquery_pathlist: subroot lost its parent_root");
    root.glob = subroot.glob.take();

    // Isolate the params needed by this specific subplan (C:2660-2661).
    root.rel_mut(rel).subplan_params = core::mem::take(&mut root.plan_params);

    // Stash the subroot inside the rel (C: rel->subroot = subroot). The size-est
    // and import steps below read it back out (mirrors prepunion / costsize).
    root.rel_mut(rel).subroot = types_pathnodes::Subroot(Some(alloc::boxed::Box::new(subroot)));

    // sub_final_rel = fetch_upper_rel(rel->subroot, UPPERREL_FINAL, NULL); if it
    // is dummy (constraint exclusion proved the subquery empty), produce an
    // unadorned dummy path (C:2669-2677).
    let sub_final_dummy = {
        let subroot = root.rel(rel).subroot.0.as_deref().expect("subroot vanished");
        let final_rel = find_existing_upper_final(subroot);
        backend_optimizer_path_joinrels::is_dummy_rel(subroot, final_rel)
    };
    if sub_final_dummy {
        crate::dummy::set_dummy_rel_pathlist(root, run, rel)?;
        return Ok(());
    }

    // Mark rel with estimated output rows, width, etc. — must precede outer-path
    // generation so cost_subqueryscan is happy (C:2684).
    backend_optimizer_path_costsize::sizeest::set_subquery_size_estimates(run, root, rel);

    // Detect whether the reltarget is trivial (fetches all subplan output
    // columns in order), to pass to cost_subqueryscan (C:2691-2715).
    let trivial_pathtarget = compute_trivial_pathtarget(root, rel, rti, subquery_tlist_len);

    // For each Path subquery_planner produced, make a SubqueryScanPath in the
    // outer query (C:2721-2735). Collect the subroot final-rel pathlist, then
    // import + wrap each.
    let (final_pathlist, partial_pathlist) = {
        let subroot = root.rel(rel).subroot.0.as_deref().expect("subroot vanished");
        let final_rel = find_existing_upper_final(subroot);
        (
            subroot.rel(final_rel).pathlist.clone(),
            subroot.rel(final_rel).partial_pathlist.clone(),
        )
    };

    for sub_id in final_pathlist {
        let sqs = wrap_subquery_subpath(
            mcx,
            root,
            run,
            rel,
            sub_id,
            trivial_pathtarget,
            &required_outer,
        )?;
        pathnode::add_path::call(root, rel, sqs)?;
    }

    // If the outer rel allows parallelism, do the same for partial paths
    // (C:2738-2762).
    if root.rel(rel).consider_parallel && required_outer.is_none() {
        for sub_id in partial_pathlist {
            let sqs = wrap_subquery_subpath(
                mcx,
                root,
                run,
                rel,
                sub_id,
                trivial_pathtarget,
                &required_outer,
            )?;
            pathnode::add_partial_path::call(root, rel, sqs)?;
        }
    }

    Ok(())
}

/// Find an existing `UPPERREL_FINAL` rel in a subroot (created while the
/// subquery was planned).
fn find_existing_upper_final(subroot: &PlannerInfo) -> RelId {
    for &id in subroot.upper_rels[UPPERREL_FINAL as usize].iter() {
        if subroot.rel(id).relids.is_none() {
            return id;
        }
    }
    panic!("set_subquery_pathlist: subroot has no UPPERREL_FINAL rel");
}

/// `trivial_pathtarget` detection (C:2691-2715): true iff `rel->reltarget->exprs`
/// fetches exactly the subplan output columns 1..n in order (each is `Var` with
/// `varno == rti` and `varattno == position + 1`).
fn compute_trivial_pathtarget(
    root: &PlannerInfo,
    rel: RelId,
    rti: Index,
    subquery_tlist_len: usize,
) -> bool {
    let exprs: Vec<types_pathnodes::NodeId> = match root.rel(rel).reltarget.as_deref() {
        Some(t) => t.exprs.clone(),
        None => Vec::new(),
    };
    if exprs.len() != subquery_tlist_len {
        return false;
    }
    for (i, expr_id) in exprs.iter().enumerate() {
        match root.node(*expr_id).as_var() {
            Some(var) => {
                if var.varno != rti as i32 || var.varattno as usize != i + 1 {
                    return false;
                }
            }
            None => return false,
        }
    }
    true
}

/// Import one subroot final-rel path into the outer arena, convert its pathkeys
/// to the outer representation, and wrap it in a SubqueryScanPath. `sub_id` is
/// the original subroot-arena path (passed as `subroot_subpath` so
/// `create_subqueryscan_plan` rebuilds the leaf scans against the subroot range
/// table); `imported_id` is the in-root cost copy.
fn wrap_subquery_subpath<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    rel: RelId,
    sub_id: PathId,
    trivial_pathtarget: bool,
    required_outer: &types_pathnodes::Relids,
) -> PgResult<PathId> {
    let (imported_id, sub_pathkeys) = {
        let subroot = root.rel_mut(rel).subroot.0.take().expect("subroot vanished");
        let sub_pathkeys = subroot.path(sub_id).base().pathkeys.clone();
        let id = pathnode::import_path_from_subroot::call(mcx, root, &subroot, sub_id);
        root.rel_mut(rel).subroot.0 = Some(subroot);
        (id, sub_pathkeys)
    };

    // make_tlist_from_pathtarget(subpath->pathtarget) over the imported path's
    // (in-root) reltarget, then convert_subquery_pathkeys to outer repr.
    let imported_tlist = make_tlist_from_pathtarget_ids(root, imported_id);
    let pathkeys = backend_optimizer_path_pathkeys::convert_subquery_pathkeys(
        root,
        rel,
        &sub_pathkeys,
        &imported_tlist,
    );

    pathnode::create_subqueryscan_path::call(
        root,
        run,
        rel,
        imported_id,
        Some(sub_id),
        trivial_pathtarget,
        pathkeys,
        required_outer,
    )
}

/// `make_tlist_from_pathtarget` over an in-root path's reltarget exprs (each
/// already a `NodeId` in `root`'s arena), wrapping each in a `TargetEntry`
/// (resno 1..n).
fn make_tlist_from_pathtarget_ids(
    root: &mut PlannerInfo,
    path: PathId,
) -> Vec<types_pathnodes::NodeId> {
    let exprs: Vec<types_pathnodes::NodeId> = match root.path(path).base().pathtarget.as_deref() {
        Some(t) => t.exprs.clone(),
        None => Vec::new(),
    };
    let mut out: Vec<types_pathnodes::NodeId> = Vec::with_capacity(exprs.len());
    for (i, expr_id) in exprs.into_iter().enumerate() {
        out.push(root.alloc_targetentry(TargetEntryNode {
            expr: expr_id,
            resno: (i + 1) as types_core::primitive::AttrNumber,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        }));
    }
    out
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
/// self-reference (recursive) CTE RTE. The non-recursive term's path is in the
/// plan level processing the recursive UNION, which is one level *below* where
/// the CTE comes from; walk `parent_root` `ctelevelsup - 1` times and read
/// `cteroot->non_recursive_path`.
pub fn set_worktable_pathlist<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<()> {
    let rte = types_pathnodes::planner_run::planner_rt_fetch(run, root, rti);
    let ctename = rte
        .ctename
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    let levelsup = rte.ctelevelsup;
    if levelsup == 0 {
        // shouldn't happen
        return Err(PgError::error(format!("bad levelsup for CTE \"{ctename}\"")));
    }

    // C reads `cteroot->non_recursive_path->rows` by walking `parent_root`
    // `ctelevelsup - 1` times to the recursion-planning root. PlannerInfo is not
    // `Clone` in this model, so `parent_root` is not populated; the recursion
    // planner instead stamps the non-recursive term's row estimate (and the
    // work-table param id) onto this leaf subroot. Prefer the parent-root walk
    // when present; otherwise use the stamped carrier.
    let cte_rows = {
        let mut up = levelsup - 1;
        let mut cteroot: &PlannerInfo = root;
        let mut walked = true;
        while up > 0 {
            match cteroot.parent_root.as_deref() {
                Some(p) => {
                    cteroot = p;
                    up -= 1;
                }
                None => {
                    walked = false;
                    break;
                }
            }
        }
        if walked {
            if let Some(ctepath) = cteroot.non_recursive_path {
                cteroot.path(ctepath).base().rows
            } else {
                root.non_recursive_rows.ok_or_else(|| {
                    PgError::error(format!("could not find path for CTE \"{ctename}\""))
                })?
            }
        } else {
            root.non_recursive_rows.ok_or_else(|| {
                PgError::error(format!("could not find path for CTE \"{ctename}\""))
            })?
        }
    };

    // Mark rel with estimated output rows, width, etc.
    backend_optimizer_path_costsize::sizeest::set_cte_size_estimates(run, root, rel, cte_rows);

    // We don't support pushing join clauses into a worktable scan's quals, but
    // it could still have required parameterization due to LATERAL refs.
    let required_outer = bms::relids_copy::call(&root.rel(rel).lateral_relids);

    let path = pathnode::create_worktablescan_path::call(root, run, rel, &required_outer)?;
    pathnode::add_path::call(root, rel, path)?;
    Ok(())
}

