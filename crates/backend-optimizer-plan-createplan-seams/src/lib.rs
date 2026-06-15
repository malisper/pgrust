//! Seam declarations for the `backend-optimizer-plan-createplan` unit — the
//! per-`Path`-subtype `create_*_plan` converter family of `createplan.c`.
//!
//! `create_plan_recurse` (in the owning `backend-optimizer-plan-createplan`
//! crate) is the `best_path->pathtype` dispatch over the 36-variant
//! [`PathNode`](types_pathnodes::PathNode) enum. The dispatch itself is the F1
//! deliverable and ships whole; every leaf converter arm
//! (`create_seqscan_plan`, `create_indexscan_plan`, `create_nestloop_plan`,
//! `create_agg_plan`, …) is one of the seams declared here, so the dispatch
//! compiles before any family is ported.
//!
//! Until a family lands, its seam has no installer and loud-panics on the first
//! call (the seam-and-panic contract). The later F-families
//! (scan / join / append / upper) each install the converters they own from
//! their `init_seams()`. None is reachable yet: `create_plan` itself is only
//! invoked from `standard_planner` / `subquery_planner` (unported), so these
//! panics are latent.
//!
//! ## Model (arena handles, lifetime-free `PlannerInfo`)
//!
//! Each converter mirrors the C `create_*_plan(root, (XxxPath *) best_path,
//! flags)`. In this repo a `Path *` is a [`PathId`](types_pathnodes::PathId)
//! into `PlannerInfo::path_arena`; the concrete subtype is recovered by the
//! owner from the [`PathNode`] variant (the analogue of the C up-cast). So every
//! converter takes `best_path: PathId` rather than a typed pointer, plus the
//! `mcx` the produced [`Node`](types_nodes::nodes::Node) plan tree is allocated
//! in. Every converter also receives `run: &PlannerRun<'mcx>` — the
//! `'mcx`-scoped planner-run store (queries + range-table entries) that the
//! lifetime-free [`PlannerInfo`] cannot hold (see
//! [`types_pathnodes::planner_run::PlannerRun`]). It is the safe-Rust rendering
//! of `root` reaching `simple_rte_array`: the scan converters call
//! [`planner_rt_fetch`](types_pathnodes::planner_run::planner_rt_fetch)`(run,
//! root, scanrelid)` to read their `RangeTblEntry`
//! (`rtekind`/`functions`/`values_lists`/`ctename`/…), exactly as C dereferences
//! `planner_rt_fetch(scanrelid, root)`. The scan-family converters also receive
//! the already-decided `tlist` and
//! resolved `scan_clauses` that `create_scan_plan` computed (the C
//! `create_scan_plan` passes `tlist` + `scan_clauses` into each
//! `create_*scan_plan`). `Err` carries each converter's `ereport(ERROR)`
//! surface.

#![allow(non_snake_case)]

extern crate alloc;

use types_nodes::nodes::Node;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PathId, PlannerInfo};

// ---------------------------------------------------------------------------
// Secondary dispatchers (createplan.c create_scan_plan / create_join_plan).
//
// These two are themselves dispatchers over a path-subtype family; they are
// ported in F2 (scan) / a later join family. The top dispatch
// `create_plan_recurse` routes the scan and join `pathtype`s here.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `create_scan_plan(root, best_path, flags)` (createplan.c:558): the scan
    /// dispatch — extract restriction clauses, pick the tlist (physical vs path),
    /// route to the per-scan-type `create_*scan_plan`, and add a gating `Result`
    /// for pseudoconstant quals. Filled by the F2 scan family.
    pub fn create_scan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_join_plan(root, (JoinPath *) best_path)` (createplan.c:1080): the
    /// join dispatch (`MergeJoin` / `HashJoin` / `NestLoop`) + gating `Result`.
    /// Filled by the join family.
    pub fn create_join_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

// ---------------------------------------------------------------------------
// Result / non-scan-non-join arms reached directly from create_plan_recurse.
// (createplan.c create_plan_recurse switch.)
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `create_append_plan(root, (AppendPath *) best_path, flags)`
    /// (createplan.c). Append family.
    pub fn create_append_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_merge_append_plan(root, (MergeAppendPath *) best_path, flags)`.
    pub fn create_merge_append_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_projection_plan(root, (ProjectionPath *) best_path, flags)`
    /// (the `T_Result` / `IsA(best_path, ProjectionPath)` arm).
    pub fn create_projection_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

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
    /// `create_group_result_plan(root, (GroupResultPath *) best_path)`
    /// (the `T_Result` / `IsA(best_path, GroupResultPath)` arm).
    pub fn create_group_result_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_project_set_plan(root, (ProjectSetPath *) best_path)`.
    pub fn create_project_set_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_material_plan(root, (MaterialPath *) best_path, flags)`.
    pub fn create_material_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
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
    /// The `T_Unique` arm: `create_upper_unique_plan` /
    /// `create_unique_plan` (chosen by `IsA(best_path, UpperUniquePath)`).
    /// The sub-discrimination is internal to the owning family; the dispatch
    /// routes the whole `T_Unique` `pathtype` here.
    pub fn create_unique_dispatch_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_gather_plan(root, (GatherPath *) best_path)`.
    pub fn create_gather_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_sort_plan(root, (SortPath *) best_path, flags)`.
    pub fn create_sort_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_incrementalsort_plan(root, (IncrementalSortPath *) best_path, flags)`.
    pub fn create_incrementalsort_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_group_plan(root, (GroupPath *) best_path)`.
    pub fn create_group_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// The `T_Agg` arm: `create_groupingsets_plan` / `create_agg_plan`
    /// (chosen by `IsA(best_path, GroupingSetsPath)`). The sub-discrimination is
    /// internal to the owning family; the dispatch routes the whole `T_Agg`
    /// `pathtype` here.
    pub fn create_agg_dispatch_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
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
    /// `create_setop_plan(root, (SetOpPath *) best_path, flags)`.
    pub fn create_setop_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_recursiveunion_plan(root, (RecursiveUnionPath *) best_path)`.
    pub fn create_recursiveunion_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_lockrows_plan(root, (LockRowsPath *) best_path, flags)`.
    pub fn create_lockrows_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_modifytable_plan(root, (ModifyTablePath *) best_path)`.
    pub fn create_modifytable_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_limit_plan(root, (LimitPath *) best_path, flags)`.
    pub fn create_limit_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        flags: i32,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_gather_merge_plan(root, (GatherMergePath *) best_path)`.
    pub fn create_gather_merge_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
    ) -> types_error::PgResult<Node<'mcx>>
);

// ---------------------------------------------------------------------------
// Final-finish steps owned by the planner / tlist / subselect keystones.
// (createplan.c create_plan tail.) These mutate / attach to the produced
// topmost plan and are routed through their owners' seams; the F2 scan family
// or the planner unit installs them. Declared here so create_plan compiles
// whole.
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
