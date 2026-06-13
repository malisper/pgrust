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
//! The `execExpr_core` compiler primitives these wrappers drive are now landed
//! (the explicit-target-list `exec_build_projection_info_impl` /
//! `exec_build_update_projection_impl`, `exec_project_info`, and `exec_init_qual`),
//! so every wrapper whose inputs are already in `Expr` / `TargetEntry` form is
//! fully wired to the core builder here: the four RETURNING / SET /
//! MERGE-INSERT/UPDATE *projection* builders, `exec_project_returning`, and the
//! standalone *qual* wrappers — MERGE WHEN qual (`exec_init_merge_when_qual`),
//! MERGE join condition (`exec_init_merge_join_condition`), and ON CONFLICT
//! WHERE (`exec_init_on_conflict_where`). Their qual is now modeled as the
//! implicit-AND `List` of `Expr` the C cast `(List *) node->qual` produces, so
//! it feeds `execExpr_core::exec_init_qual` directly.
//!
//! The remaining wrappers still loud-panic, but on a *genuine unported owner*,
//! not on the core compiler (which is ready):
//!   * `exec_project_new_tuple` needs the `ResultRelInfo.ri_projectNew` field,
//!     not yet modeled in `types-nodes` (only its `ri_projectNewInfoValid` flag
//!     is);
//!   * the `execPartition` / inherited-root map-and-build wrappers must first
//!     `build_attrmap_by_name` (attmap.c) + `map_variable_attnos`
//!     (rewriteManip.c) / `adjust_partition_colnos*` / `ExecGetRootToChildMap`
//!     (execPartition.c) / `table_slot_create` (tableam.c) — none of which has a
//!     reachable owner seam in this crate's dependency set — before they could
//!     reach the (ready) core builder.
//! Each such wrapper names the genuine owner it is blocked on
//! (mirror-PG-and-panic).

use mcx::{Mcx, PgBox};
use types_core::primitive::Index;
use types_error::PgResult;
use types_nodes::execexpr::{ExprState, ProjectionInfo};
use types_nodes::modifytable::MergeAction;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, TargetEntry};
use types_nodes::{EStateData, EcxtId, ModifyTableState, RriId, SlotId};

use crate::execExpr_core;

/// `mas_whenqual = ExecInitQual((List *) action->qual, &mtstate->ps)`.
///
/// Compile one MERGE action's WHEN [NOT MATCHED] AND conditions into an
/// `ExprState` via the in-unit `execExpr_core::exec_init_qual` (`ExecInitQual`).
/// The `MergeAction.qual` is now modeled as the implicit-AND `List` of `Expr`
/// the C cast `(List *) action->qual` produces, so it feeds `exec_init_qual`
/// directly. A `None` qual (the C `NIL`) yields `None`.
pub fn exec_init_merge_when_qual<'mcx>(
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    qual: Option<&[Expr]>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    execExpr_core::exec_init_qual(qual, &mut mtstate.ps, estate)
}

/// `ExecBuildProjectionInfo(action->targetList, ..., tgtdesc)` for a MERGE
/// INSERT action.
///
/// Build the explicit-target-list projection via the in-unit
/// `execExpr_core::exec_build_projection_info_impl` (`ExecBuildProjectionInfo`).
/// The C is
/// `ExecBuildProjectionInfo(action->targetList, mtstate->ps.ps_ExprContext,
/// tgtslot, &mtstate->ps, tgtdesc)`: the result slot (`tgt_slot`) only matters
/// at `ExecProject` time and is not consulted during compilation, and the
/// parent `PlanState` is ignored by the owned spine (mirrors `ExecInitExpr`), so
/// the build needs only the target list, the econtext id, and the target tuple
/// descriptor (`tgt_desc_rel`'s relation `rd_att`).
pub fn exec_build_merge_insert_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    target_list: &[TargetEntry<'mcx>],
    econtext: EcxtId,
    _tgt_slot: SlotId,
    tgt_desc_rel: RriId,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    let mcx = estate.es_query_cxt;
    // tgtdesc = resultRelInfo->ri_RelationDesc->rd_att (the MERGE INSERT
    // action's target relation descriptor). Clone it out of the pooled
    // ResultRelInfo so it can be borrowed alongside the &mut estate.
    let tgt_desc = estate
        .result_rel(tgt_desc_rel)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecBuildMergeInsertProjection: ri_RelationDesc is NULL")
        .rd_att_clone_in(mcx)?;
    execExpr_core::exec_build_projection_info_impl(
        estate,
        target_list,
        econtext,
        Some(&tgt_desc),
    )
}

/// `ExecBuildUpdateProjection(...)` for a MERGE UPDATE action.
///
/// Build the UPDATE "new tuple" projection over the explicit `target_list` /
/// `update_colnos` via the in-unit
/// `execExpr_core::exec_build_update_projection_impl`
/// (`ExecBuildUpdateProjection`), returning the standalone per-action
/// `ProjectionInfo` for `mas_proj`. The C is
/// `ExecBuildUpdateProjection(action->targetList, true, action->updateColnos,
/// resultRelInfo->ri_RelationDesc->rd_att, mtstate->ps.ps_ExprContext, tgtslot,
/// &mtstate->ps)`: `evalTargetList = true`, the relation descriptor comes from
/// `result_rel_info`'s `rd_att`, and the result slot only matters at
/// `ExecProject` time (not during compilation).
pub fn exec_build_merge_update_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    target_list: &[TargetEntry<'mcx>],
    update_colnos: &[i32],
    econtext: EcxtId,
) -> PgResult<PgBox<'mcx, ProjectionInfo<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let rel_desc = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecBuildMergeUpdateProjection: ri_RelationDesc is NULL")
        .rd_att_clone_in(mcx)?;
    execExpr_core::exec_build_update_projection_impl(
        estate,
        target_list,
        true,
        update_colnos,
        &rel_desc,
        econtext,
    )
}

/// `ri_MergeJoinCondition = ExecInitQual((List *) joinCondition, &mtstate->ps)`.
///
/// Compile the MERGE join condition for `result_rel_info` and store the
/// compiled `ExprState` on the pooled `ResultRelInfo.ri_MergeJoinCondition`. The
/// `joinCondition` is now modeled as the implicit-AND `List` of `Expr` the C
/// cast `(List *) joinCondition` produces, so it feeds the in-unit
/// `execExpr_core::exec_init_qual` (`ExecInitQual`) directly.
pub fn exec_init_merge_join_condition<'mcx>(
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    join_condition: Option<&[Expr]>,
) -> PgResult<()> {
    // resultRelInfo->ri_MergeJoinCondition =
    //     ExecInitQual((List *) joinCondition, &mtstate->ps);
    let compiled = execExpr_core::exec_init_qual(join_condition, &mut mtstate.ps, estate)?;
    estate.result_rel_mut(result_rel_info).ri_MergeJoinCondition = compiled;
    Ok(())
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
/// list. The execExpr_core compiler primitives are landed, but the
/// `build_attrmap_by_name` (attmap.c) and `map_variable_attnos` (rewriteManip.c)
/// remap that must run first has no reachable owner seam, so this is blocked on
/// those unported owners.
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
         is blocked on attmap.c (build_attrmap_by_name) + rewriteManip.c \
         (map_variable_attnos) — no reachable owner seam — which must remap the WCO/RETURNING \
         lists before the (landed) execExpr_core ExecInitQual/ExecBuildProjectionInfo can run"
    )
}

/// The WITH CHECK OPTION map-and-build of `ExecInitPartitionInfo`
/// (execPartition.c L556-614).
///
/// `build_attrmap_by_name(partrel, firstResultRel)` + `map_variable_attnos(..,
/// firstVarno, 0, attmap, partrel reltype)` the first plan's WCO list into the
/// leaf partition's attnos, `ExecInitQual` each `WithCheckOption.qual`, and
/// store `ri_WithCheckOptions` / `ri_WithCheckOptionExprs` on the leaf
/// `ResultRelInfo`. The execExpr_core `ExecInitQual` is landed; the
/// attmap.c / rewriteManip.c remap that must run first has no reachable owner
/// seam, so this is blocked on those unported owners.
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
         attmap.c (build_attrmap_by_name) + rewriteManip.c (map_variable_attnos) — no reachable \
         owner seam — which must remap the WCO list before the (landed) execExpr_core \
         ExecInitQual can run"
    )
}

/// The RETURNING map-and-build of `ExecInitPartitionInfo` (execPartition.c
/// L623-679).
///
/// `build_attrmap_by_name` + `map_variable_attnos` the first plan's RETURNING
/// list into the leaf partition's attnos, store `ri_returningList`, and build
/// `ri_projectReturning` via `ExecBuildProjectionInfo` using
/// `mtstate->ps.ps_ResultTupleSlot` / `ps_ExprContext` and the partition's
/// tupdesc. The execExpr_core `ExecBuildProjectionInfo` is landed; the
/// attmap.c / rewriteManip.c remap that must run first has no reachable owner
/// seam, so this is blocked on those unported owners.
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
         (build_attrmap_by_name) + rewriteManip.c (map_variable_attnos) — no reachable owner \
         seam — which must remap the RETURNING list before the (landed) execExpr_core \
         explicit-target-list ExecBuildProjectionInfo can run"
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
/// (table_slot_create) — none with a reachable owner seam. The execExpr_core
/// ExecBuildUpdateProjection / ExecInitQual it would then call are landed, so
/// the block is purely those unported owners.
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
    _on_conflict_where: Option<&[Expr]>,
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_on_conflict_update: OnConflictSetState build routes to \
         execPartition.c (ExecGetRootToChildMap, adjust_partition_colnos), rewriteManip.c \
         (map_variable_attnos), tableam.c (table_slot_create) — no reachable owner seam — which \
         must remap/create the projection inputs before the (landed) execExpr_core \
         ExecBuildUpdateProjection/ExecInitQual can run"
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
/// `ri_MergeActions[matchKind]`. The execExpr_core
/// ExecBuildProjectionInfo/ExecBuildUpdateProjection/ExecInitQual it would call
/// are landed; the attmap.c / rewriteManip.c / execPartition.c remap that must
/// run first has no reachable owner seam, so this is blocked on those owners.
#[allow(clippy::too_many_arguments)]
pub fn partition_init_merge_actions<'mcx>(
    _mcx: Mcx<'mcx>,
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _leaf_part_rri: RriId,
    _first_result_rel: RriId,
    _first_varno: Index,
    _econtext: EcxtId,
    _ref_join_condition: Option<&[Expr]>,
    _ref_merge_action_list: &[MergeAction<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_merge_actions: per-partition MERGE action build routes \
         to attmap.c (build_attrmap_by_name), rewriteManip.c (map_variable_attnos), \
         execPartition.c (adjust_partition_colnos_using_map) — no reachable owner seam — which \
         must remap the per-action inputs before the (landed) execExpr_core \
         ExecBuildProjectionInfo/ExecBuildUpdateProjection/ExecInitQual can run"
    )
}

/// `ExecProject(resultRelInfo->ri_projectNew)` from a result relation
/// (nodeModifyTable.c, the UPDATE/INSERT "new tuple" build).
///
/// Wire the projection's econtext (`ecxt_outertuple = plan_slot`, and for
/// UPDATE `ecxt_scantuple = old_slot`), then `ExecProject(ri_projectNew)`. The
/// `ResultRelInfo.ri_projectNew` field is not yet modeled in `types-nodes`
/// (only its `ri_projectNewInfoValid` flag is), so the projection cannot be read
/// to evaluate. The in-unit `execExpr_core::exec_project_info` (`ExecProject`)
/// is landed; this is blocked on that not-yet-modeled `ri_projectNew` field.
pub fn exec_project_new_tuple<'mcx>(
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _plan_slot: SlotId,
    _old_slot: Option<SlotId>,
) -> PgResult<SlotId> {
    panic!(
        "execExpr-modify::exec_project_new_tuple: ExecProject(ri_projectNew) routes to the \
         not-yet-modeled ResultRelInfo.ri_projectNew field in types-nodes; the execExpr_core \
         ExecProject it would evaluate is landed"
    )
}

/// Compile every WITH CHECK OPTION qual for a result relation
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// `foreach(ll, wcoList) { wcoExpr = ExecInitQual(wco->qual, &mtstate->ps);
/// wcoExprs = lappend(wcoExprs, wcoExpr); }` then store `ri_WithCheckOptions` /
/// `ri_WithCheckOptionExprs` on the pooled `ResultRelInfo`. The
/// in-unit `ExecInitQual` is landed; what is missing is the
/// `WithCheckOption.qual` (a `Node`→`Expr` list) modeling in `types-nodes`
/// (`ri_WithCheckOptions` is stored as plain `Node`s with no Expr-list path), so
/// this is blocked on that not-yet-modeled WCO qual view.
pub fn exec_init_with_check_options<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _result_rel_info: RriId,
    _wco_list: &[Node<'mcx>],
) -> PgResult<()> {
    panic!(
        "execExpr-modify::exec_init_with_check_options: per-WCO ExecInitQual(wco->qual) is \
         blocked on the not-yet-modeled WithCheckOption node in types-nodes (the per-rel WCO \
         list is a List of WithCheckOption nodes; without that node the individual wco->qual \
         Expr-lists cannot be extracted to feed execExpr_core::exec_init_qual)"
    )
}

/// Build the RETURNING projection for one result relation
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// `ri_returningList = rlist; ri_projectReturning =
/// ExecBuildProjectionInfo(rlist, econtext, ps_ResultTupleSlot, &mtstate->ps,
/// rd_att)`. `econtext` is `mtstate->ps.ps_ExprContext`; the result slot
/// (`ps_ResultTupleSlot`) only matters at `ExecProject` time, not during
/// compilation, so it is not threaded into the build. Stores the compiled
/// projection and the source list on the pooled `ResultRelInfo` and sets the
/// `ri_has_project_returning` flag.
pub fn exec_build_returning_projection<'mcx>(
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    rlist: &[TargetEntry<'mcx>],
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // econtext = mtstate->ps.ps_ExprContext
    let econtext = mtstate
        .ps
        .ps_ExprContext
        .expect("ExecBuildReturningProjection: mtstate->ps.ps_ExprContext is NULL");
    // rd_att = resultRelInfo->ri_RelationDesc->rd_att
    let rd_att = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecBuildReturningProjection: ri_RelationDesc is NULL")
        .rd_att_clone_in(mcx)?;

    let proj =
        execExpr_core::exec_build_projection_info_impl(estate, rlist, econtext, Some(&rd_att))?;

    // resultRelInfo->ri_returningList = rlist;
    let mut stored_list: mcx::PgVec<'mcx, TargetEntry<'mcx>> =
        mcx::vec_with_capacity_in(mcx, rlist.len())?;
    for tle in rlist {
        stored_list.push(tle.clone_in(mcx)?);
    }
    let rri = estate.result_rel_mut(result_rel_info);
    rri.ri_returningList = Some(stored_list);
    // resultRelInfo->ri_projectReturning = ...
    rri.ri_projectReturning = Some(proj);
    rri.ri_has_project_returning = true;
    Ok(())
}

/// Build the ON CONFLICT DO UPDATE SET projection for the result relation
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// `ExecBuildUpdateProjection(node->onConflictSet, true, node->onConflictCols,
/// relationDesc, econtext, onconfl->oc_ProjSlot, &mtstate->ps)`, returning the
/// compiled `OnConflictSetState.oc_ProjInfo`. `relationDesc` is
/// `result_rel_info`'s `rd_att`; `evalTargetList = true`; the projection slot
/// (`proj_slot` / `oc_ProjSlot`) only matters at `ExecProject` time, not during
/// compilation.
pub fn exec_build_on_conflict_set_projection<'mcx>(
    _mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    on_conflict_set: &[TargetEntry<'mcx>],
    on_conflict_cols: &[i32],
    econtext: EcxtId,
    _proj_slot: SlotId,
) -> PgResult<ProjectionInfo<'mcx>> {
    let mcx = estate.es_query_cxt;
    let rel_desc = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecBuildOnConflictSetProjection: ri_RelationDesc is NULL")
        .rd_att_clone_in(mcx)?;
    let proj = execExpr_core::exec_build_update_projection_impl(
        estate,
        on_conflict_set,
        true,
        on_conflict_cols,
        &rel_desc,
        econtext,
    )?;
    Ok(PgBox::into_inner(proj))
}

/// `ExecInitQual((List *) node->onConflictWhere, &mtstate->ps)`
/// (nodeModifyTable.c `ExecInitModifyTable`).
///
/// Compile the ON CONFLICT DO UPDATE WHERE clause into an `ExprState`; a `None`
/// clause yields `None`. The `onConflictWhere` is now modeled as the
/// implicit-AND `List` of `Expr` the C cast `(List *) node->onConflictWhere`
/// produces, so it feeds the in-unit `execExpr_core::exec_init_qual`
/// (`ExecInitQual`) directly.
pub fn exec_init_on_conflict_where<'mcx>(
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    on_conflict_where: Option<&[Expr]>,
) -> PgResult<Option<ExprState<'mcx>>> {
    let compiled = execExpr_core::exec_init_qual(on_conflict_where, &mut mtstate.ps, estate)?;
    Ok(compiled.map(PgBox::into_inner))
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
