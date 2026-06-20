//! Seam declarations for the `backend-optimizer-plan-setrefs` unit
//! (`optimizer/plan/setrefs.c`).
//!
//! These are the genuine cross-subsystem externals reached from setrefs.c that
//! would otherwise cycle (or whose owners are unported). Each is an OUTWARD seam
//! that loud-panics until its owner installs it from `init_seams()`. None of the
//! expression/plan-walking logic is seamed — that is real in-crate code over the
//! `Node`/`Expr` model.

#![allow(non_snake_case)]

extern crate alloc;

seam_core::seam!(
    /// Append a `PlanInvalItem` to `glob->invalItems`
    /// (`record_plan_function_dependency` / `record_plan_type_dependency`).
    ///
    /// C builds `makeNode(PlanInvalItem)`, sets `cacheId` and
    /// `hashValue = GetSysCacheHashValue1(cacheId, ObjectIdGetDatum(oid))`, and
    /// `lappend`s it onto `glob->invalItems`. The syscache hash lives with the
    /// syscache subsystem, so the hash computation + the concrete-pair append are
    /// performed by the owner; the list itself is now a concrete
    /// `Vec<PlanInvalItem>` (`(cacheId, hashValue)` pairs) so `standard_planner`
    /// can read them straight into `PlannedStmt.invalItems`. `cache_id` is the
    /// syscache identifier (PROCOID / TYPEOID); `oid` the function/type OID.
    pub fn record_inval_item(
        inval_items: &mut alloc::vec::Vec<types_nodes::nodeindexscan::PlanInvalItem>,
        cache_id: i32,
        oid: types_core::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `IS_DUMMY_REL(fetch_upper_rel(subroot, UPPERREL_FINAL, NULL))`
    /// (`add_rtes_to_flat_rtable`) — is the subquery's final upper rel dummy?
    /// `subroot_index` is the outer RT index; the owner navigates
    /// `root.simple_rel_array[subroot_index].subroot` to the subquery's
    /// `PlannerInfo`, fetches its `UPPERREL_FINAL` rel and runs `is_dummy_rel`.
    /// The upper-rel fetch + dummy test live with the path/relnode owner.
    pub fn subroot_final_rel_is_dummy(
        root: &mut types_pathnodes::PlannerInfo,
        subroot_index: usize,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `copyObject(list_nth(root->multiexpr_params[subqueryid-1], colno-1))`
    /// (`fix_param_node`) — resolve a `PARAM_MULTIEXPR` Param to its replacement
    /// expression. `multiexpr_params` carries opaque `NodeId` handles owned by
    /// subselect; the owner resolves+copies. `subqueryid`/`colno` are 1-based.
    pub fn multiexpr_param_lookup(
        root: &types_pathnodes::PlannerInfo,
        subqueryid: usize,
        colno: usize,
    ) -> types_error::PgResult<types_nodes::primnodes::Expr>
);

seam_core::seam!(
    /// `find_minmax_agg_replacement_param`'s per-`minmax_aggs` test+pick: for the
    /// `MinMaxAggInfo` at `idx`, if `mminfo->aggfnoid == aggref->aggfnoid` (caller
    /// pre-checks the aggfnoid) and `equal(mminfo->target, cur_target_expr)`,
    /// return `mminfo->param`. `minmax_aggs` carries opaque `NodeId` handles owned
    /// by planagg; the owner resolves the `MinMaxAggInfo`.
    pub fn minmax_replacement_param(
        root: &types_pathnodes::PlannerInfo,
        idx: usize,
        aggfnoid: types_core::Oid,
        cur_target_expr: &types_nodes::primnodes::Expr,
    ) -> types_error::PgResult<Option<types_nodes::primnodes::Param>>
);

seam_core::seam!(
    /// `mark_partial_aggref(agg, aggsplit)` (planner.c) — adjust an `Aggref` to
    /// represent a partial-aggregation phase. Lives in planner.c (which this unit
    /// must not touch); routed through this seam for `convert_combining_aggrefs`.
    pub fn mark_partial_aggref(
        agg: &mut types_nodes::primnodes::Aggref,
        aggsplit: types_nodes::AggSplit,
    ) -> types_error::PgResult<()>
);

/// The three dependency out-params the VALUE `extract_query_dependencies` writes
/// (mirrors `backend_nodes_copyfuncs_pc_seams::QueryDependencies`, but the
/// inval-item keys are the bare `(cacheId, hashValue)` pair so this seam crate
/// need not depend on `types-plancache`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryDependenciesValue {
    /// `relationOids`.
    pub relation_oids: alloc::vec::Vec<types_core::Oid>,
    /// `invalItems` — `(PlanInvalItem.cacheId, PlanInvalItem.hashValue)` pairs.
    pub inval_items: alloc::vec::Vec<(i32, u32)>,
    /// `*hasRowSecurity` (the `glob.dependsOnRole` hack).
    pub depends_on_rls: bool,
}

seam_core::seam!(
    /// `extract_query_dependencies((Node *) query_list, &relationOids,
    /// &invalItems, &hasRowSecurity)` (setrefs.c:3635) over the OWNED value
    /// `Query` tree. Given a rewritten-but-not-yet-planned querytree list, extract
    /// the relation-OID and function-inval-item dependencies and detect whether
    /// any rewrite step was affected by RLS — exactly as `set_plan_references`
    /// would. Installed by backend-optimizer-plan-setrefs (the owner); this is the
    /// VALUE counterpart of the handle-based
    /// `backend_nodes_copyfuncs_pc_seams::extract_query_dependencies` that
    /// plancache's F0 de-handle will switch to.
    pub fn extract_query_dependencies_value<'mcx, 'q>(
        mcx: mcx::Mcx<'mcx>,
        query_list: &[types_nodes::copy_query::Query<'q>],
    ) -> types_error::PgResult<QueryDependenciesValue>
);
