//! `execExpr-modify` family — the nodeModifyTable / execPartition leaf
//! projection/qual builders that live behind execExpr seams.
//!
//! These are thin `ExecInitQual` / `ExecBuildProjectionInfo` /
//! `ExecBuildUpdateProjection` wrappers, but the parse-node inputs (MERGE
//! actions, WITH CHECK OPTION quals, RETURNING/ON CONFLICT lists) and the
//! attribute-map rewrite (`build_attrmap_by_name` / `map_variable_attnos`) sit
//! at the boundary between nodeModifyTable / execPartition / rewrite and the
//! expression compiler, so the whole map-and-build is owned by execExpr and
//! reached through these seams.
//!
//! Every one of these wrappers is a leaf that drives, in C:
//!   * the in-unit expression *compiler* (`ExecInitQual`,
//!     `ExecBuildProjectionInfo`, `ExecBuildUpdateProjection`) and the
//!     projection *evaluator* (`ExecProject`), all owned by the sibling
//!     `execExpr_core` family of this same unit; and
//!   * a set of cross-unit callees — `build_attrmap_by_name` (attmap.c),
//!     `map_variable_attnos` (rewriteManip.c), `adjust_partition_colnos` /
//!     `adjust_partition_colnos_using_map` / `ExecGetRootToChildMap`
//!     (execPartition.c), and `table_slot_create` (tableam.c).
//!
//! Only `exec_project_returning` is reachable today: it reads the already-built
//! `ResultRelInfo.ri_projectReturning` (modeled in `types-nodes`) and evaluates
//! it through the in-unit `execExpr_core::exec_project_info`
//! (`ExecProject(projInfo)`), with the caller having pre-wired the econtext
//! slots and the OLD/NEW flags per the C contract.
//!
//! The remaining wrappers cannot yet emit their full C logic on this scaffold:
//! the `execExpr_core` compiler primitives they call are still `todo!()` with
//! node-driven (not explicit-target-list) signatures; the `ResultRelInfo`
//! `ri_projectNew` field and the `Node`→`Expr`-qual-list view the C casts
//! through (`(List *) action->qual`) are not yet modeled in `types-nodes`; and
//! the attmap/rewrite/partition/tableam callees have no reachable owner seam in
//! this crate's dependency set. Each therefore loud-panics, naming the owner it
//! routes to — the sanctioned "owner not yet landed" marker — until those
//! families/units land (mirror-PG-and-panic).

use mcx::{Mcx, PgBox};
use types_core::primitive::Index;
use types_error::PgResult;
use types_nodes::execexpr::{ExprState, ProjectionInfo};
use types_nodes::modifytable::MergeAction;
use types_nodes::nodes::Node;
use types_nodes::primnodes::TargetEntry;
use types_nodes::{EStateData, EcxtId, ModifyTableState, RriId, SlotId};

use crate::execExpr_core;

/// `mas_whenqual = ExecInitQual((List *) action->qual, &mtstate->ps)`.
///
/// Compile one MERGE action's WHEN [NOT MATCHED] AND conditions into an
/// `ExprState` via the in-unit `execExpr_core::exec_init_qual`
/// (`ExecInitQual`). The C casts the `Node *` qual to a `List *` of `Expr`s; the
/// trimmed `types-nodes` `Node` enum carries plan nodes only and cannot yet
/// surface that `Expr` qual list, so the cast cannot be expressed. Routes to the
/// execExpr_core compiler (and the missing `Node`→`Expr`-list modeling).
pub fn exec_init_merge_when_qual<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _qual: Option<&Node<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    panic!(
        "execExpr-modify::exec_init_merge_when_qual: ExecInitQual((List *) action->qual) \
         routes to execExpr_core (ExecInitQual, still todo!()) and the not-yet-modeled \
         Node->Expr qual-list view in types-nodes"
    )
}

/// `ExecBuildProjectionInfo(action->targetList, ..., tgtdesc)` for a MERGE
/// INSERT action.
///
/// Build the explicit-target-list projection via the in-unit
/// `execExpr_core::exec_build_projection_info` (`ExecBuildProjectionInfo`). The
/// scaffold core variant is node-driven (reads the target list off a
/// `PlanStateData`) and does not yet accept the explicit `target_list` / `slot`
/// / `inputDesc` the C passes here, so the call cannot be expressed. Routes to
/// the execExpr_core compiler.
pub fn exec_build_merge_insert_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _target_list: &[TargetEntry<'mcx>],
    _econtext: EcxtId,
    _tgt_slot: SlotId,
    _tgt_desc_rel: RriId,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    panic!(
        "execExpr-modify::exec_build_merge_insert_projection: \
         ExecBuildProjectionInfo(targetList, econtext, slot, parent, tgtdesc) routes to \
         execExpr_core (explicit-target-list ExecBuildProjectionInfo, still todo!())"
    )
}

/// `ExecBuildUpdateProjection(...)` for a MERGE UPDATE action.
///
/// Build the UPDATE "new tuple" projection over the explicit `target_list` /
/// `update_colnos` via the in-unit `execExpr_core::exec_build_update_projection`
/// (`ExecBuildUpdateProjection`). The scaffold core variant stores into a
/// `ResultRelInfo.ri_projectNew` and does not return a standalone
/// `ProjectionInfo` for the per-action `mas_proj`, and the explicit-target-list
/// build it needs is still `todo!()`. Routes to the execExpr_core compiler.
pub fn exec_build_merge_update_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _target_list: &[TargetEntry<'mcx>],
    _update_colnos: &[i32],
    _econtext: EcxtId,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    panic!(
        "execExpr-modify::exec_build_merge_update_projection: \
         ExecBuildUpdateProjection(targetList, true, updateColnos, relDesc, econtext, slot, \
         parent) routes to execExpr_core (explicit-target-list ExecBuildUpdateProjection, \
         still todo!())"
    )
}

/// `ri_MergeJoinCondition = ExecInitQual((List *) joinCondition, &mtstate->ps)`.
///
/// Compile the MERGE join condition for `result_rel_info` and store the
/// compiled `ExprState` on the pooled `ResultRelInfo.ri_MergeJoinCondition`. The
/// `Node`→`Expr`-qual-list cast feeding `ExecInitQual` is not yet modeled and
/// the in-unit `ExecInitQual` is still `todo!()`. Routes to execExpr_core.
pub fn exec_init_merge_join_condition<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _join_condition: Option<&Node<'mcx>>,
) -> PgResult<()> {
    panic!(
        "execExpr-modify::exec_init_merge_join_condition: \
         ri_MergeJoinCondition = ExecInitQual((List *) joinCondition) routes to execExpr_core \
         (ExecInitQual, still todo!()) and the not-yet-modeled Node->Expr qual-list view"
    )
}

/// The inherited-root WCO / RETURNING setup of `ExecInitMerge`
/// (nodeModifyTable.c L3856-3950).
///
/// When the MERGE targets an inherited (non-partitioned) table with INSERT
/// actions, the root `ResultRelInfo` is not in the `resultRelInfo[]` array, so
/// its WCO constraints and RETURNING projection must be initialized here:
/// `build_attrmap_by_name` + `map_variable_attnos` the first plan WCO/RETURNING
/// list to the root's attnos (when root and first result rel differ),
/// `ExecInitQual` each WCO qual, and `ExecBuildProjectionInfo` the RETURNING
/// list. The `build_attrmap_by_name` (attmap.c) and `map_variable_attnos`
/// (rewriteManip.c) callees have no reachable owner seam, and the execExpr_core
/// compiler primitives are still `todo!()`.
pub fn exec_init_merge_inherited_root<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _root_result_rel_info: RriId,
    _first_result_rel: RriId,
    _econtext: EcxtId,
) -> PgResult<()> {
    panic!(
        "execExpr-modify::exec_init_merge_inherited_root: inherited-root WCO/RETURNING setup \
         routes to attmap.c (build_attrmap_by_name), rewriteManip.c (map_variable_attnos) — no \
         reachable owner seam — and execExpr_core (ExecInitQual/ExecBuildProjectionInfo, still \
         todo!())"
    )
}

/// The WITH CHECK OPTION map-and-build of `ExecInitPartitionInfo`
/// (execPartition.c L556-614).
///
/// `build_attrmap_by_name(partrel, firstResultRel)` + `map_variable_attnos(..,
/// firstVarno, 0, attmap, partrel reltype)` the first plan's WCO list into the
/// leaf partition's attnos, `ExecInitQual` each `WithCheckOption.qual`, and
/// store `ri_WithCheckOptions` / `ri_WithCheckOptionExprs` on the leaf
/// `ResultRelInfo`. Routes to attmap.c / rewriteManip.c (no reachable owner
/// seam) and execExpr_core (ExecInitQual, still `todo!()`).
pub fn partition_init_with_check_options<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _leaf_part_rri: RriId,
    _first_result_rel: RriId,
    _first_varno: Index,
    _ref_wco_list: &[Node<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_with_check_options: WCO map-and-build routes to \
         attmap.c (build_attrmap_by_name), rewriteManip.c (map_variable_attnos) — no reachable \
         owner seam — and execExpr_core (ExecInitQual, still todo!())"
    )
}

/// The RETURNING map-and-build of `ExecInitPartitionInfo` (execPartition.c
/// L623-679).
///
/// `build_attrmap_by_name` + `map_variable_attnos` the first plan's RETURNING
/// list into the leaf partition's attnos, store `ri_returningList`, and build
/// `ri_projectReturning` via `ExecBuildProjectionInfo` using
/// `mtstate->ps.ps_ResultTupleSlot` / `ps_ExprContext` and the partition's
/// tupdesc. Routes to attmap.c / rewriteManip.c (no reachable owner seam) and
/// execExpr_core (ExecBuildProjectionInfo, still `todo!()`).
pub fn partition_init_returning<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _leaf_part_rri: RriId,
    _first_result_rel: RriId,
    _first_varno: Index,
    _ref_returning_list: &[TargetEntry<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_returning: RETURNING map-and-build routes to attmap.c \
         (build_attrmap_by_name), rewriteManip.c (map_variable_attnos) — no reachable owner \
         seam — and execExpr_core (explicit-target-list ExecBuildProjectionInfo, still todo!())"
    )
}

/// The ON CONFLICT DO UPDATE `OnConflictSetState` build of
/// `ExecInitPartitionInfo` (execPartition.c L736-861).
///
/// `ExecGetRootToChildMap(leaf_part_rri)` — when `NULL` reuse the root
/// `ri_onConflict`'s slot/proj/where; otherwise `map_variable_attnos`
/// (INNER_VAR then firstVarno) over `on_conflict_set`,
/// `adjust_partition_colnos(on_conflict_cols)`, `table_slot_create(partrel)`,
/// `ExecBuildUpdateProjection`, and (when present) map + `ExecInitQual` the
/// WHERE clause. Routes to execPartition.c (ExecGetRootToChildMap /
/// adjust_partition_colnos), rewriteManip.c (map_variable_attnos), tableam.c
/// (table_slot_create) — no reachable owner seams — and execExpr_core
/// (ExecBuildUpdateProjection / ExecInitQual, still `todo!()`).
#[allow(clippy::too_many_arguments)]
pub fn partition_init_on_conflict_update<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _leaf_part_rri: RriId,
    _root_result_rel_info: RriId,
    _first_result_rel: RriId,
    _first_varno: Index,
    _on_conflict_set: &[TargetEntry<'mcx>],
    _on_conflict_cols: &[i32],
    _on_conflict_where: Option<&Node<'mcx>>,
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_on_conflict_update: OnConflictSetState build routes to \
         execPartition.c (ExecGetRootToChildMap, adjust_partition_colnos), rewriteManip.c \
         (map_variable_attnos), tableam.c (table_slot_create) — no reachable owner seam — and \
         execExpr_core (ExecBuildUpdateProjection/ExecInitQual, still todo!())"
    )
}

/// The per-partition MERGE action map-and-build of `ExecInitPartitionInfo`
/// (execPartition.c L886-981).
///
/// `build_attrmap_by_name`, map the join condition via `map_variable_attnos` +
/// `ExecInitQual` into `ri_MergeJoinCondition`, then for each `MergeAction`:
/// build the `MergeActionState` (CMD_INSERT `ExecBuildProjectionInfo` over the
/// partition's `ri_newTupleSlot`; CMD_UPDATE
/// `adjust_partition_colnos_using_map` + `ExecBuildUpdateProjection`),
/// map + `ExecInitQual` the action's `qual` into `mas_whenqual`, and append into
/// `ri_MergeActions[matchKind]`. Routes to attmap.c / rewriteManip.c /
/// execPartition.c (no reachable owner seam) and execExpr_core
/// (ExecBuildProjectionInfo/ExecBuildUpdateProjection/ExecInitQual, still
/// `todo!()`).
#[allow(clippy::too_many_arguments)]
pub fn partition_init_merge_actions<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _leaf_part_rri: RriId,
    _first_result_rel: RriId,
    _first_varno: Index,
    _econtext: EcxtId,
    _ref_join_condition: Option<&Node<'mcx>>,
    _ref_merge_action_list: &[MergeAction<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_merge_actions: per-partition MERGE action build routes \
         to attmap.c (build_attrmap_by_name), rewriteManip.c (map_variable_attnos), \
         execPartition.c (adjust_partition_colnos_using_map) — no reachable owner seam — and \
         execExpr_core (ExecBuildProjectionInfo/ExecBuildUpdateProjection/ExecInitQual, still \
         todo!())"
    )
}

/// `ExecProject(resultRelInfo->ri_projectNew)` from a result relation
/// (nodeModifyTable.c, the UPDATE/INSERT "new tuple" build).
///
/// Wire the projection's econtext (`ecxt_outertuple = plan_slot`, and for
/// UPDATE `ecxt_scantuple = old_slot`), then `ExecProject(ri_projectNew)`. The
/// `ResultRelInfo.ri_projectNew` field is not yet modeled in `types-nodes`
/// (only its `ri_projectNewInfoValid` flag is), so the projection cannot be read
/// to evaluate. Routes to the not-yet-modeled `ri_projectNew` field and the
/// in-unit `execExpr_core::exec_project_info` (ExecProject, still `todo!()`).
pub fn exec_project_new_tuple<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _plan_slot: SlotId,
    _old_slot: Option<SlotId>,
) -> PgResult<SlotId> {
    panic!(
        "execExpr-modify::exec_project_new_tuple: ExecProject(ri_projectNew) routes to the \
         not-yet-modeled ResultRelInfo.ri_projectNew field in types-nodes and execExpr_core \
         (ExecProject, still todo!())"
    )
}

/// Compile every WITH CHECK OPTION qual for a result relation
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// `foreach(ll, wcoList) { wcoExpr = ExecInitQual(wco->qual, &mtstate->ps);
/// wcoExprs = lappend(wcoExprs, wcoExpr); }` then store `ri_WithCheckOptions` /
/// `ri_WithCheckOptionExprs` on the pooled `ResultRelInfo`. The
/// `WithCheckOption.qual` (a `Node`→`Expr` list) is not yet modeled in
/// `types-nodes` and the in-unit `ExecInitQual` is still `todo!()`. Routes to
/// execExpr_core and the not-yet-modeled WCO qual view.
pub fn exec_init_with_check_options<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _wco_list: &[Node<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::exec_init_with_check_options: per-WCO ExecInitQual(wco->qual) routes \
         to execExpr_core (ExecInitQual, still todo!()) and the not-yet-modeled \
         WithCheckOption.qual Expr-list view in types-nodes"
    )
}

/// Build the RETURNING projection for one result relation
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// `ri_projectReturning = ExecBuildProjectionInfo(rlist, econtext,
/// ps_ResultTupleSlot, &mtstate->ps, rd_att)`, also storing `ri_returningList`.
/// The scaffold core variant is node-driven and does not accept the explicit
/// `rlist` the C passes here, and is still `todo!()`. Routes to execExpr_core.
pub fn exec_build_returning_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _rlist: &[TargetEntry<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::exec_build_returning_projection: \
         ExecBuildProjectionInfo(rlist, econtext, ps_ResultTupleSlot, parent, rd_att) routes to \
         execExpr_core (explicit-target-list ExecBuildProjectionInfo, still todo!())"
    )
}

/// Build the ON CONFLICT DO UPDATE SET projection for the result relation
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// `ExecBuildUpdateProjection(node->onConflictSet, true, node->onConflictCols,
/// relationDesc, econtext, onconfl->oc_ProjSlot, &mtstate->ps)`. The scaffold
/// core variant stores into `ri_projectNew` rather than returning a standalone
/// `ProjectionInfo`, and the explicit-target-list build it needs is still
/// `todo!()`. Routes to execExpr_core.
pub fn exec_build_on_conflict_set_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _on_conflict_set: &[TargetEntry<'mcx>],
    _on_conflict_cols: &[i32],
    _econtext: EcxtId,
    _proj_slot: SlotId,
) -> PgResult<ProjectionInfo<'mcx>> {
    panic!(
        "execExpr-modify::exec_build_on_conflict_set_projection: \
         ExecBuildUpdateProjection(onConflictSet, true, onConflictCols, relDesc, econtext, \
         oc_ProjSlot, parent) routes to execExpr_core (explicit-target-list \
         ExecBuildUpdateProjection, still todo!())"
    )
}

/// `ExecInitQual((List *) node->onConflictWhere, &mtstate->ps)`
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// Compile the ON CONFLICT DO UPDATE WHERE clause into an `ExprState`; a `None`
/// clause yields `None`. The `Node`→`Expr`-qual-list cast is not yet modeled in
/// `types-nodes` and the in-unit `ExecInitQual` is still `todo!()`. Routes to
/// execExpr_core.
pub fn exec_init_on_conflict_where<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _on_conflict_where: Option<&Node<'mcx>>,
) -> PgResult<Option<ExprState<'mcx>>> {
    panic!(
        "execExpr-modify::exec_init_on_conflict_where: \
         ExecInitQual((List *) node->onConflictWhere) routes to execExpr_core (ExecInitQual, \
         still todo!()) and the not-yet-modeled Node->Expr qual-list view in types-nodes"
    )
}

/// `ExecProject(resultRelInfo->ri_projectReturning)`
/// (nodeModifyTable.c `ExecProcessReturning`, the trailing `return
/// ExecProject(projectReturning)`).
///
/// The caller (`ExecProcessReturning`) has already wired the projection's
/// `pi_exprContext` slots (`ecxt_scantuple` / `ecxt_outertuple` /
/// `ecxt_oldtuple` / `ecxt_newtuple`) and its `pi_state.flags` (the OLD/NEW
/// `EEO_FLAG_*` bits); this leaf only evaluates the compiled projection. The
/// projection lives on the pooled `ResultRelInfo`, read by id, and is evaluated
/// through the in-unit `execExpr_core::exec_project_info` (`ExecProject`).
pub fn exec_project_returning<'mcx>(
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
) -> PgResult<SlotId> {
    // ProjectionInfo *projectReturning = resultRelInfo->ri_projectReturning;
    //
    // The pooled ResultRelInfo and the EState are aliased by the &mut estate, so
    // detach the projection out of the pool to satisfy the borrow checker, run
    // ExecProject, then restore it (the projection's identity / contents are
    // unchanged by evaluation — ExecProject only fills its result slot).
    let project_returning = estate
        .result_rel_mut(result_rel_info)
        .ri_projectReturning
        .take()
        .expect("ExecProject: resultRelInfo->ri_projectReturning is NULL");

    // return ExecProject(projectReturning);
    let result = execExpr_core::exec_project_info(&project_returning, estate);

    estate.result_rel_mut(result_rel_info).ri_projectReturning = Some(project_returning);

    result
}
