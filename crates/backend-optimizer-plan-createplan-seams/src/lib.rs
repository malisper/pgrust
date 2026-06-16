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

use alloc::vec::Vec;

use types_nodes::nodes::Node;
use types_nodes::primnodes::TargetEntry;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PathId, PlannerInfo, RinfoId};

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
// Per-scan-type converters reached from `create_scan_plan`'s second switch
// (createplan.c:677). `create_scan_plan` (now installed by F2b) computes the
// `tlist` (physical-vs-path) and resolved `scan_clauses` and routes to one of
// these `create_*scan_plan` converters; each is filled by the F2c scan-converter
// family and loud-panics until then (the seam-and-panic contract).
//
// In the C the converter takes the typed up-cast `(XxxPath *) best_path`; here
// it takes `best_path: PathId` (the owner recovers the [`PathNode`] subtype),
// plus the already-decided `tlist` (`Vec<TargetEntry<'mcx>>`, empty = the C
// `NIL`) and the relation's `scan_clauses` (`Vec<RinfoId>` — the RestrictInfo
// list, exactly as C passes `scan_clauses`; each converter does its own
// `order_qual_clauses` / `extract_actual_clauses` / `replace_nestloop_params`).
// `IndexScan` / `IndexOnlyScan` share `create_indexscan_plan`
// (the C `indexonly` bool); the dispatch routes both `pathtype`s here with the
// flag. `T_Result` (a `RTE_RESULT` base relation Path) routes to
// `create_resultscan_plan`.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `create_seqscan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_seqscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_samplescan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_samplescan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

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
    /// `create_tidscan_plan(root, (TidPath *) best_path, tlist, scan_clauses)`.
    pub fn create_tidscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_tidrangescan_plan(root, (TidRangePath *) best_path, tlist,
    /// scan_clauses)`.
    pub fn create_tidrangescan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_subqueryscan_plan(root, (SubqueryScanPath *) best_path, tlist,
    /// scan_clauses)`.
    pub fn create_subqueryscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_functionscan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_functionscan_plan<'mcx>(
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
    /// `create_valuesscan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_valuesscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_ctescan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_ctescan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_namedtuplestorescan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_namedtuplestorescan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_resultscan_plan(root, best_path, tlist, scan_clauses)` — the
    /// `RTE_RESULT` base-relation `T_Result` arm of `create_scan_plan`.
    pub fn create_resultscan_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        run: &PlannerRun<'mcx>,
        best_path: PathId,
        tlist: Vec<TargetEntry<'mcx>>,
        scan_clauses: Vec<RinfoId>,
    ) -> types_error::PgResult<Node<'mcx>>
);

seam_core::seam!(
    /// `create_worktablescan_plan(root, best_path, tlist, scan_clauses)`.
    pub fn create_worktablescan_plan<'mcx>(
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
    /// scan_clauses)`.
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
    /// scan_clauses)`.
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

seam_core::seam!(
    /// `create_gating_plan(root, path, plan, gating_quals)` (createplan.c:1022) —
    /// stack a gating `Result` node carrying the pseudoconstant
    /// `gating_quals` atop the already-built `plan`. `create_scan_plan` (and the
    /// join family) routes here; the body builds the `Result` via `make_result`
    /// over `build_path_tlist(root, path)`, which lives in the F2c
    /// scan-converter / `make_*` plan-node family. Declared here so
    /// `create_scan_plan` compiles before that family lands. `path` is the
    /// [`PathId`] (the C `Path *path`); `plan` is the built subplan node;
    /// `gating_quals` is the resolved pseudoconstant clause list (arena
    /// [`NodeId`]s).
    pub fn create_gating_plan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        path: PathId,
        plan: Node<'mcx>,
        gating_quals: Vec<NodeId>,
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
    pub fn resolve_cte_subplan(
        root: &PlannerInfo,
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
    pub fn resolve_worktable_param(
        root: &PlannerInfo,
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
