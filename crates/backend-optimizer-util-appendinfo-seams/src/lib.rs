#![forbid(unsafe_code)]

//! Seam declarations for `optimizer/util/appendinfo.c` — the append-relation
//! attribute-translation routines the partitionwise-join machinery
//! (`joinrels.c:try_partitionwise_join` / `build_child_join_sjinfo`) drives.
//!
//! `find_appinfos_by_relids` collects the `AppendRelInfo`s for a child relid
//! set (the C `AppendRelInfo **` + `nappinfos`, here a freshly-owned `Vec`).
//! `adjust_child_relids` translates a parent `Relids` into the corresponding
//! child `Relids`. `adjust_appendrel_attrs_restrictlist` is the
//! `(List *) adjust_appendrel_attrs(root, (Node *) restrictlist, ...)`
//! specialization joinrels.c uses on a `RestrictInfo` list — carried as a
//! `RinfoId` handle list. The owning crate
//! (`backend-optimizer-util-appendinfo`) installs these from its `init_seams()`
//! once it lands; until then a call panics loudly.
//!
//! **Failure surface.** The collectors / translators `palloc` (and so can
//! `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`), so they return [`PgResult`].

extern crate alloc;

use alloc::vec::Vec;

use types_error::PgResult;
use types_pathnodes::{
    AppendRelInfo, IndexClause, NodeId, PlannerInfo, RelId, Relids, RinfoId,
};

seam_core::seam!(
    /// `find_appinfos_by_relids(root, relids, &nappinfos)` (appendinfo.c) — the
    /// `AppendRelInfo`s for the child relations named in `relids` (a freshly
    /// owned vector; the C `AppendRelInfo **` array + its length).
    pub fn find_appinfos_by_relids(root: &PlannerInfo, relids: &Relids) -> PgResult<Vec<AppendRelInfo>>
);
seam_core::seam!(
    /// `adjust_child_relids(relids, nappinfos, appinfos)` (appendinfo.c) —
    /// translate a parent `Relids` to the corresponding child `Relids`.
    pub fn adjust_child_relids(relids: &Relids, appinfos: &[AppendRelInfo]) -> Relids
);
seam_core::seam!(
    /// `(List *) adjust_appendrel_attrs(root, (Node *) restrictlist, nappinfos,
    /// appinfos)` (appendinfo.c) — translate a parent-join `RestrictInfo` list
    /// (carried as `RinfoId` handles) into the child join's restrictlist.
    pub fn adjust_appendrel_attrs_restrictlist<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        restrictlist: &[RinfoId],
        appinfos: &[AppendRelInfo],
    ) -> PgResult<Vec<RinfoId>>
);
seam_core::seam!(
    /// `adjust_child_relids_multilevel(root, relids, childrel, parentrel)`
    /// (appendinfo.c) — translate a parent `Relids` to the child's, across
    /// possibly multiple inheritance levels. Used by
    /// `reparameterize_path_by_child` to re-key the `ppi_req_outer`.
    pub fn adjust_child_relids_multilevel(
        root: &mut PlannerInfo,
        relids: &Relids,
        childrel: RelId,
        parentrel: RelId,
    ) -> PgResult<Relids>
);
seam_core::seam!(
    /// `(List *) adjust_appendrel_attrs_multilevel(root, (Node *) restrictlist,
    /// child_rel, top_parent)` — the multilevel `RestrictInfo`-list translation
    /// (`ADJUST_CHILD_ATTRS` of a restrictinfo-list field in
    /// `reparameterize_path_by_child`).
    pub fn adjust_restrictlist_multilevel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        restrictlist: &[RinfoId],
        childrel: RelId,
        parentrel: RelId,
    ) -> PgResult<Vec<RinfoId>>
);
seam_core::seam!(
    /// `(List *) adjust_appendrel_attrs_multilevel(root, (Node *) nodelist,
    /// child_rel, top_parent)` — the multilevel translation of a bare
    /// expression-node-handle list (`param_exprs` / `pathtarget->exprs`).
    pub fn adjust_nodelist_multilevel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        handles: &[NodeId],
        childrel: RelId,
        parentrel: RelId,
    ) -> PgResult<Vec<NodeId>>
);
seam_core::seam!(
    /// `(List *) adjust_appendrel_attrs_multilevel(root, (Node *) indexclauses,
    /// child_rel, top_parent)` — the multilevel translation of an `IndexClause`
    /// list (`ADJUST_CHILD_ATTRS(ipath->indexclauses)`).
    pub fn adjust_indexclauses_multilevel<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        root: &mut PlannerInfo,
        indexclauses: &[IndexClause],
        childrel: RelId,
        parentrel: RelId,
    ) -> PgResult<Vec<IndexClause>>
);
seam_core::seam!(
    /// `distribute_row_identity_vars(root)` (appendinfo.c) — distribute any
    /// UPDATE/DELETE/MERGE row-identity variables to the target relations once
    /// appendrel expansion is finished. `query_planner` (planmain.c) calls this
    /// on the general join path after `add_other_rels_to_query`. Void in C; can
    /// `palloc`, so it returns [`PgResult`].
    ///
    /// The C body reads `root->parse` (`commandType`/`resultRelation`/`rtable`),
    /// which is the opaque [`types_pathnodes::QueryId`] here; the planner-run
    /// resolver (`run`) resolves it to the owned `Query<'mcx>`, and `mcx` is the
    /// planner arena the rare constraint-exclusion edge case allocates in.
    pub fn distribute_row_identity_vars<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `add_row_identity_columns(root, rtindex, target_rte, target_relation)`
    /// (appendinfo.c) — add the core row-identity junk columns (CTID for a
    /// regular table; the FDW whole-row Var for a foreign table) to
    /// `root->processed_tlist` for an UPDATE/DELETE/MERGE target relation.
    /// `preprocess_targetlist` (preptlist.c) drives it. The caller passes the
    /// resolved relation fields (`relkind`/`relid`) plus the resolved
    /// `command_type`/`result_relation` (the opaque [`types_pathnodes::QueryId`]
    /// resolves only through the caller's run). `has_delete_row_trigger` is the
    /// foreign-table delete-trigger predicate.
    pub fn add_row_identity_columns(
        root: &mut PlannerInfo,
        rtindex: types_core::primitive::Index,
        command_type: types_nodes::nodes::CmdType,
        relid: types_core::Oid,
        relkind: u8,
        has_delete_row_trigger: bool,
        result_relation: types_core::primitive::Index,
    ) -> PgResult<()>
);
