//! Seam declarations for the `backend-optimizer-prep-preptlist` unit
//! (`optimizer/prep/preptlist.c`).
//!
//! preptlist.c preprocesses the parse-tree targetlist; its driver
//! `preprocess_targetlist` is called from `grouping_planner` (planner.c, still
//! unported), so this crate declares it as an inward seam the planner driver
//! can be wired to. The unit's two other public entry points
//! (`extract_update_targetlist_colnos`, `get_plan_rowmark`) are exposed as
//! plain `pub fn`s on the owner crate (no cross-unit seam needed — their callers
//! are in the same prep/plan layer and depend on the owner directly).
//!
//! ## Model
//!
//! The C `preprocess_targetlist(PlannerInfo *root)` reads/mutates `root->parse`
//! (the top `Query`) and writes `root->processed_tlist` / `root->update_colnos`.
//! Here `PlannerInfo` is lifetime-free; the top `Query` lives in the
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) store behind the
//! `PlannerInfo.parse` [`QueryId`](types_pathnodes::QueryId) handle. The caller
//! resolves it (`run.resolve_mut(root.parse)`) and threads the `&mut Query`
//! alongside the `&mut PlannerInfo` (whose `node_arena` / `processed_tlist` the
//! pass also mutates) — the two are distinct objects, so no aliasing conflict.
//! `mcx` is the planner-run context new arena nodes allocate in.
//!
//! `processed_tlist` is carried as a `Vec<NodeId>` of arena handles into
//! `PlannerInfo.node_arena` (the [`ArenaNode::TargetEntry`] id-space). The owner
//! deep-clones each resolved `TargetEntry<'mcx>` (via `Expr::clone_in` /
//! `TargetEntry::clone_in`, keystone #280) into the arena and stores the
//! handle, exactly as the C list of `TargetEntry *` aliases nodes built earlier.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `preprocess_targetlist(root)` (preptlist.c): driver for preprocessing the
    /// parse-tree targetlist. Writes the fully-processed targetlist into
    /// `root.processed_tlist` (as `NodeId` handles into `root.node_arena`) and,
    /// for an UPDATE, the target column numbers into `root.update_colnos`.
    /// Mutates `parse` (the INSERT targetlist expansion / UPDATE renumbering) and
    /// `root` (`processed_tlist`, `update_colnos`, `node_arena`). `Err` carries
    /// the `eval_const_expressions` / clone `ereport(ERROR)` surface.
    ///
    /// **SELECT core ported now** (the only currently-reachable path: the
    /// SELECT-analyze milestone produces no result relation / no rowMarks / no
    /// RETURNING). The INSERT/UPDATE/DELETE/MERGE legs (`expand_insert_targetlist`,
    /// `extract_update_targetlist_colnos`, `add_row_identity_columns`, the MERGE
    /// action handling) and the FOR-UPDATE/SHARE rowMarks junk-column stanza are
    /// deferred to the DML-analyze family + the PlanRowMark-carrier keystone;
    /// they seam-and-panic until then.
    pub fn preprocess_targetlist<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut types_pathnodes::PlannerInfo,
        parse: &mut types_nodes::copy_query::Query<'mcx>,
    ) -> types_error::PgResult<()>
);
