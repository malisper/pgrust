//! Seam declarations for the `backend-optimizer-plan-createplan` unit — the
//! `createplan.c` converter arms whose owner is NOT this crate.
//!
//! `create_plan_recurse` (in the owning `backend-optimizer-plan-createplan`
//! crate) is the `best_path->pathtype` dispatch over the 36-variant
//! [`PathNode`](pathnodes::PathNode) enum. createplan.c is a single
//! translation unit, so the converters that ARE ported here
//! (`create_seqscan_plan`, `create_nestloop_plan`, `create_mergejoin_plan`,
//! `create_append_plan`, the sort/group/setop/gather upper converters, …) are
//! reached by `create_plan_recurse` / `create_scan_plan` as **direct in-crate
//! calls** — they are no longer indirection seams.
//!
//! The seams that REMAIN here are the genuine cross-/forward-boundary arms:
//!
//! * scan converters whose owner is an unported / separate unit
//!   (`create_indexscan_plan`, `create_bitmap_scan_plan`,
//!   `create_tablefuncscan_plan`, `create_foreignscan_plan` (FDW floor),
//!   `create_customscan_plan` (custom-scan floor));
//! * upper converters not yet ported (`create_minmaxagg_plan`,
//!   `create_memoize_plan`, `create_windowagg_plan`, `create_lockrows_plan`
//!   (PlanRowMark carrier gap));
//! * the `create_plan` tail steps owned by the tlist / subselect / planner
//!   keystones (`apply_tlist_labeling`, `ss_attach_initplans`) — installed by
//!   those crates; and
//! * the SubPlan-init / subroot-recursion legs of the cte / worktable /
//!   subquery scan converters (`resolve_cte_subplan`,
//!   `resolve_worktable_param`, `create_subqueryscan_subplan`).
//!
//! Each remaining converter mirrors the C `create_*_plan(root, (XxxPath *)
//! best_path, flags)`: a `Path *` is a [`PathId`](pathnodes::PathId) into
//! `PlannerInfo::path_arena` (the owner recovers the subtype from the
//! [`PathNode`] variant), plus the `mcx` the produced
//! [`Node`](nodes::nodes::Node) plan tree is allocated in and `run:
//! &PlannerRun<'mcx>` (the `'mcx`-scoped planner-run store the lifetime-free
//! [`PlannerInfo`] cannot hold). The scan converters also receive the
//! already-decided `tlist` and resolved `scan_clauses` that `create_scan_plan`
//! computed. `Err` carries each converter's `ereport(ERROR)` surface.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use nodes::nodes::Node;
use nodes::primnodes::TargetEntry;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{PathId, PlannerInfo, RinfoId};

// ---------------------------------------------------------------------------
// Per-scan-type converters reached from `create_scan_plan`'s second switch
// (createplan.c:677) whose owner is unported or a separate unit. The ported
// scan converters (seqscan/samplescan/tid/tidrange/subquery/function/values/
// cte/namedtuplestore/result/worktable) are direct in-crate calls and no longer
// declared here. `IndexScan` / `IndexOnlyScan` share `create_indexscan_plan`
// (the C `indexonly` bool).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `create_indexscan_plan(root, (IndexPath *) best_path, tlist,
    /// scan_clauses, indexonly)` — covers both `T_IndexScan` (`indexonly =
    /// false`) and `T_IndexOnlyScan` (`indexonly = true`).
    pub fn create_indexscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
        indexonly: bool,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_bitmap_scan_plan(root, (BitmapHeapPath *) best_path, tlist,
    /// scan_clauses)`.
    pub fn create_bitmap_scan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_tablefuncscan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_tablefuncscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_foreignscan_plan(root, (ForeignPath *) best_path, tlist,
    /// scan_clauses)` — FDW floor (GetForeignPlan vtable).
    pub fn create_foreignscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_customscan_plan(root, (CustomPath *) best_path, tlist,
    /// scan_clauses)` — custom-scan provider floor.
    pub fn create_customscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

// ---------------------------------------------------------------------------
// Non-scan / non-join create_plan_recurse arms not yet ported in the owning
// crate. (createplan.c create_plan_recurse switch.)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `create_minmaxagg_plan(root, (MinMaxAggPath *) best_path)`
    /// (the `T_Result` / `IsA(best_path, MinMaxAggPath)` arm).
    pub fn create_minmaxagg_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_memoize_plan(root, (MemoizePath *) best_path, flags)`.
    pub fn create_memoize_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_windowagg_plan(root, (WindowAggPath *) best_path)`.
    pub fn create_windowagg_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_lockrows_plan(root, (LockRowsPath *) best_path, flags)` —
    /// blocked on the `PlanRowMark` carrier (LockRowsPath.rowMarks is bare
    /// `Vec<NodeId>` with no arena->PlanRowMark resolver).
    pub fn create_lockrows_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

// ---------------------------------------------------------------------------
// Final-finish steps owned by the planner / tlist / subselect keystones.
// (createplan.c create_plan tail.) These mutate / attach to the produced
// topmost plan and are installed by their owners' crates.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `apply_tlist_labeling(plan->targetlist, root->processed_tlist)`
    /// (tlist.c, called at the top of `create_plan` on the topmost
    /// non-`ModifyTable` plan). Owned by the tlist unit; mutates the produced
    /// plan's top targetlist labels in place.
    pub fn apply_tlist_labeling<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        plan: &mut Node<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SS_attach_initplans(root, plan)` (subselect.c, called at the top of
    /// `create_plan`). Attaches this query level's initplans to the topmost
    /// plan; owned by the subselect SubPlan-building unit.
    pub fn ss_attach_initplans<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        plan: &mut Node<'mcx>,
    ) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// SubPlan-init-plan / subroot-recursion resolution legs of the cte /
// worktable / subquery scan converters (createplan.c create_ctescan_plan /
// create_worktablescan_plan / create_subqueryscan_plan).
//
// These three legs dereference state built by subselect.c
// (`SS_process_ctes` init SubPlans, recursive-CTE `wt_param_id`) or recurse
// into a different planner context (`create_plan(rel->subroot, ...)`), all of
// which the unported subselect / planner driver owns. The rest of each
// converter (clause ordering / nestloop params / `make_*scan`) is ported in the
// owning crate; these seams carry the genuinely-unported legs 1:1 and
// loud-panic until subselect lands.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// The CTE-`SubPlan` resolution leg of `create_ctescan_plan`
    /// (createplan.c:3884): walk `cteroot->parse->cteList` to find the
    /// referenced CTE's index, read its `plan_id` from `cteroot->cte_plan_ids`,
    /// locate the matching init `SubPlan` in `cteroot->init_plans`, and return
    /// `(plan_id, cte_param_id)` where `cte_param_id = linitial_int(ctesplan->
    /// setParam)`. Dereferences subselect.c's built init SubPlans; owned by the
    /// subselect SubPlan-building unit. `scanrelid` is the CTE base rel's RT
    /// index.
    pub fn resolve_cte_subplan<'mcx>(
        root: &PlannerInfo,
        run: &PlannerRun<'mcx>,
        scanrelid: u32,
    ) -> types_error::PgResult<(i32, i32)>
);

seam_core::seam!(
    /// The work-table-`Param` resolution leg of `create_worktablescan_plan`
    /// (createplan.c:4055): walk `parent_root` to the plan level processing the
    /// recursive UNION (one below the CTE's level) and return its
    /// `cteroot->wt_param_id`. The `wt_param_id` is assigned during subselect.c
    /// recursive-CTE planning; owned by the subselect unit. `scanrelid` is the
    /// work-table (self-reference CTE) base rel's RT index.
    pub fn resolve_worktable_param<'mcx>(
        root: &PlannerInfo,
        run: &PlannerRun<'mcx>,
        scanrelid: u32,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// The subroot-recursion leg of `create_subqueryscan_plan`
    /// (createplan.c:3695): `create_plan(rel->subroot, best_path->subpath)` —
    /// build the subquery's child plan by recursing into `create_plan` with the
    /// subquery's *own* `PlannerInfo` (`rel->subroot`) and its `PlannerRun`.
    /// Entering a different planner context requires the subroot's range table,
    /// owned by the planner driver; routed here 1:1. `best_path` is the
    /// `SubqueryScanPath`'s [`PathId`].
    pub fn create_subqueryscan_subplan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);
