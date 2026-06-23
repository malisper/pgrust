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
use ::types_core::primitive::Index;
use ::types_error::PgResult;
use ::nodes::execexpr::{ExprState, ProjectionInfo};
use ::nodes::nodes::Node;
use ::nodes::primnodes::{Expr, TargetEntry};
use nodes::{EStateData, EcxtId, ModifyTableState, RriId, SlotId};

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
    qual: Option<&[Expr<'mcx>]>,
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
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    target_list: &[TargetEntry<'mcx>],
    econtext: EcxtId,
    tgt_slot: SlotId,
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
    let mut proj = execExpr_core::exec_build_projection_info_impl(
        estate,
        target_list,
        econtext,
        Some(tgt_slot),
        Some(&tgt_desc),
    )?;
    // C `ExecBuildProjectionInfo(action->targetList, ..., &mtstate->ps, ...)`
    // passes the ModifyTable PlanState as parent, so a SubPlan compiled in this
    // INSERT action's target list (e.g. `INSERT VALUES ((SELECT ...)))`) is
    // appended to `mtstate->ps.subPlan` via `state->parent->subPlan = lappend(...)`.
    // The owned spine defers this to a drain here (read by EXPLAIN to print the
    // SubPlan under the Merge node).
    execExpr_core::drain_found_subplan_ids(mcx, &mut mtstate.ps, &mut proj.pi_state)?;
    Ok(proj)
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
    mtstate: &mut ModifyTableState<'mcx>,
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
    // result slot = resultRelInfo->ri_newTupleSlot (the MERGE UPDATE "new tuple"
    // slot).
    let slot = estate.result_rel(result_rel_info).ri_newTupleSlot;
    let mut proj = execExpr_core::exec_build_update_projection_impl(
        estate,
        target_list,
        true,
        update_colnos,
        &rel_desc,
        econtext,
        slot,
    )?;
    // C `ExecBuildUpdateProjection(action->targetList, true, ..., &mtstate->ps)`
    // passes the ModifyTable PlanState as parent. With `evalTargetList = true`
    // the action target list is evaluated here, so a SubPlan in it (e.g.
    // `UPDATE SET (b,c) = (SELECT ...)`) is appended to `mtstate->ps.subPlan`
    // via `state->parent->subPlan = lappend(...)`. The owned spine defers this
    // to a drain here (read by EXPLAIN to print the SubPlan under the Merge node).
    execExpr_core::drain_found_subplan_ids(mcx, &mut mtstate.ps, &mut proj.pi_state)?;
    Ok(proj)
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
    join_condition: Option<&[Expr<'mcx>]>,
) -> PgResult<()> {
    // resultRelInfo->ri_MergeJoinCondition =
    //     ExecInitQual((List *) joinCondition, &mtstate->ps);
    let compiled = execExpr_core::exec_init_qual(join_condition, &mut mtstate.ps, estate)?;
    estate.result_rel_mut(result_rel_info).ri_MergeJoinCondition = compiled;
    Ok(())
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
         ExecInitQual can run; additionally blocked on the not-yet-modeled WithCheckOption node \
         in types-nodes (the ref_wco_list is a plain Node list, so wco->qual cannot be extracted \
         as an Expr-list) — the still-unmodeled WCO node, NOT the resolved gap-3 Expr-list retype"
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
    _on_conflict_where: Option<&[Expr<'mcx>]>,
) -> PgResult<()> {
    panic!(
        "execExpr-modify::partition_init_on_conflict_update: OnConflictSetState build routes to \
         execPartition.c (ExecGetRootToChildMap, adjust_partition_colnos), rewriteManip.c \
         (map_variable_attnos), tableam.c (table_slot_create) — no reachable owner seam — which \
         must remap/create the projection inputs before the (landed) execExpr_core \
         ExecBuildUpdateProjection/ExecInitQual can run"
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
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    plan_slot: SlotId,
    old_slot: Option<SlotId>,
) -> PgResult<SlotId> {
    // ProjectionInfo *newProj = relinfo->ri_projectNew; (take it out to satisfy
    // the &mut estate borrow during evaluation; restore it after).
    let mut new_proj = estate
        .result_rel_mut(result_rel_info)
        .ri_projectNew
        .take()
        .expect("exec_project_new_tuple: ri_projectNew is NULL");

    // econtext = newProj->pi_exprContext;
    // econtext->ecxt_outertuple = planSlot;  (the source for ASSIGN_OUTER_VAR)
    // econtext->ecxt_scantuple  = oldSlot;   (the source for ASSIGN_SCAN_VAR)
    let econtext = new_proj
        .pi_exprContext
        .expect("exec_project_new_tuple: ri_projectNew has no pi_exprContext");
    {
        let ecxt = estate.ecxt_mut(econtext);
        ecxt.ecxt_outertuple = Some(plan_slot);
        ecxt.ecxt_scantuple = old_slot;
    }

    // return ExecProject(newProj);
    let slot = crate::execExpr_core::exec_project_info(&mut new_proj, estate);

    // Restore the projection on the ResultRelInfo.
    estate.result_rel_mut(result_rel_info).ri_projectNew = Some(new_proj);
    slot
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
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    wco_list: &[Node<'mcx>],
) -> PgResult<()> {
    // nodeModifyTable.c ExecInitModifyTable, per-rel WCO loop:
    //   foreach(ll, wcoList) {
    //       WithCheckOption *wco = lfirst(ll);
    //       ExprState *wcoExpr = ExecInitQual((List *) wco->qual, &mtstate->ps);
    //       wcoExprs = lappend(wcoExprs, wcoExpr);
    //   }
    //   resultRelInfo->ri_WithCheckOptions = wcoList;
    //   resultRelInfo->ri_WithCheckOptionExprs = wcoExprs;
    //
    // `wco->qual` is the canonicalized barrier/with-check qual Node; C casts it
    // to `(List *)` and feeds ExecInitQual, which reads it as an implicit-AND
    // list. Mirror that by splitting the qual Expr with make_ands_implicit. A
    // real RLS / view WCO always carries a non-trivial qual, so the compiled
    // ExprState is always present (a NULL qual would compile to the C NULL
    // ExprState == always-true; we never store that for an aligned WCO because
    // ri_WithCheckOptionExprs holds non-optional ExprStates — matching the fact
    // that every WCO the rewriter emits has a qual).
    let mcx = estate.es_query_cxt;
    let mut wco_exprs: ::mcx::PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> =
        ::mcx::PgVec::new_in(mcx);
    let mut stored_wcos: ::mcx::PgVec<'mcx, Node<'mcx>> = ::mcx::PgVec::new_in(mcx);

    for wco_node in wco_list.iter() {
        let wco = wco_node.as_withcheckoption().ok_or_else(|| {
            ::types_error::PgError::error(
                "ExecInitModifyTable: WITH CHECK OPTION list element is not a \
                 WithCheckOption node",
            )
        })?;

        // (List *) wco->qual: split the qual Expr into its implicit-AND conjuncts.
        let qual_expr: Option<Expr> = match wco.qual.as_deref() {
            None => None,
            Some(q) => q.as_expr().map(|e| e.clone_in(mcx)).transpose()?,
        };
        let conjuncts = nodes_core::makefuncs::make_ands_implicit(qual_expr);

        let wco_expr = execExpr_core::exec_init_qual(
            if conjuncts.is_empty() {
                None
            } else {
                Some(conjuncts.as_slice())
            },
            &mut mtstate.ps,
            estate,
        )?;

        match wco_expr {
            Some(es) => wco_exprs.push(es),
            None => {
                // The qual const-folded to TRUE (make_ands_implicit drops a
                // constant-true input → empty conjunct list → C's
                // ExecInitQual(NIL) == NULL ExprState == always-true). C stores
                // that NULL and ExecQual(NULL) returns true. `ri_WithCheckOptionExprs`
                // holds non-optional ExprStates here, so compile a trivially-true
                // ExprState from a single TRUE Const to keep the slot aligned and
                // semantically always-passing.
                let true_const = nodes_core::makefuncs::make_bool_const(true, false);
                let true_clause = [Expr::Const(true_const)];
                let es = execExpr_core::exec_init_qual(
                    Some(&true_clause),
                    &mut mtstate.ps,
                    estate,
                )?
                .ok_or_else(|| {
                    ::types_error::PgError::error(
                        "ExecInitModifyTable: failed to compile always-true WCO ExprState",
                    )
                })?;
                wco_exprs.push(es);
            }
        }

        stored_wcos.push(wco_node.clone_in(mcx)?);
    }

    let rri = estate.result_rel_mut(result_rel_info);
    rri.ri_has_with_check_options = !stored_wcos.is_empty();
    rri.ri_WithCheckOptions = if stored_wcos.is_empty() {
        None
    } else {
        Some(stored_wcos)
    };
    rri.ri_WithCheckOptionExprs = if wco_exprs.is_empty() {
        None
    } else {
        Some(wco_exprs)
    };
    Ok(())
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

    // result slot = mtstate->ps.ps_ResultTupleSlot
    let slot = mtstate.ps.ps_ResultTupleSlot;
    let mut proj = execExpr_core::exec_build_projection_info_impl(
        estate,
        rlist,
        econtext,
        slot,
        Some(&rd_att),
    )?;

    // C `ExecBuildProjectionInfo(..., &mtstate->ps, ...)` passes the ModifyTable
    // PlanState as `parent`, so any expression SubPlan compiled in the RETURNING
    // target list is appended to `mtstate->ps.subPlan` (read by EXPLAIN to print
    // the `SubPlan N` subtrees). The owned-model `exec_build_projection_info_impl`
    // takes no parent, so drain the discovered SubPlan ids into the ModifyTable
    // head here, mirroring the parent-bearing projection wrapper.
    execExpr_core::drain_found_subplan_ids(mcx, &mut mtstate.ps, &mut proj.pi_state)?;

    // resultRelInfo->ri_returningList = rlist;
    let mut stored_list: ::mcx::PgVec<'mcx, TargetEntry<'mcx>> =
        ::mcx::vec_with_capacity_in(mcx, rlist.len())?;
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
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_rel_info: RriId,
    on_conflict_set: &[TargetEntry<'mcx>],
    on_conflict_cols: &[i32],
    econtext: EcxtId,
    proj_slot: SlotId,
) -> PgResult<ProjectionInfo<'mcx>> {
    let mcx = estate.es_query_cxt;
    let rel_desc = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecBuildOnConflictSetProjection: ri_RelationDesc is NULL")
        .rd_att_clone_in(mcx)?;
    let mut proj = execExpr_core::exec_build_update_projection_impl(
        estate,
        on_conflict_set,
        true,
        on_conflict_cols,
        &rel_desc,
        econtext,
        Some(proj_slot),
    )?;
    // C `ExecBuildUpdateProjection(node->onConflictSet, true, ..., &mtstate->ps)`
    // passes the ModifyTable PlanState as parent. With `evalTargetList = true` the
    // ON CONFLICT SET target list is evaluated here, so a SubPlan in it (e.g.
    // `ON CONFLICT DO UPDATE SET c = (SELECT ...)`) is appended to
    // `mtstate->ps.subPlan` via `state->parent->subPlan = lappend(...)`. The owned
    // spine defers this to a drain here (read by EXPLAIN to print the SubPlan under
    // the ModifyTable/Insert node and by ExecReScan chgParam propagation).
    execExpr_core::drain_found_subplan_ids(mcx, &mut mtstate.ps, &mut proj.pi_state)?;
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
    on_conflict_where: Option<&[Expr<'mcx>]>,
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
    let mut project_returning = estate
        .result_rel_mut(result_rel_info)
        .ri_projectReturning
        .take()
        .expect("ExecProject: resultRelInfo->ri_projectReturning is NULL");

    // return ExecProject(projectReturning);
    let result = execExpr_core::exec_project_info(&mut project_returning, estate);

    estate.result_rel_mut(result_rel_info).ri_projectReturning = Some(project_returning);

    result
}
