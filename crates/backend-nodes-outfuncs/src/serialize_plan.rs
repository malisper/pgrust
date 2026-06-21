//! `ExecSerializePlan` (execParallel.c:145-221) — the worker plan-shipping
//! serializer. This is the executor's `ExecSerializePlan` body, homed in
//! `outfuncs` because the whole pipeline is plan-shipping: `copyObject(plan)`
//! (deep-copy via the owned `Node::copy_node_in`) → clear the top target list's
//! `resjunk` → build the dummy `PlannedStmt` (field-fill + parallel-safe-subplan
//! filtering) → serialize it with the `_outPlannedStmt` writer.
//!
//! The serialized text is consumed only by parallel *workers* (which reconstruct
//! it with `stringToNode` in `ParallelQueryMain`). When no workers attach (the
//! leader-only Gather path — `nworkers_launched == 0`), `ExecGather` runs the
//! child plan in the leader and the text is never read back; producing it is
//! still required because `ExecInitParallelPlan` sizes the DSM from its length.
//!
//! The worker round-trip leg (`_readPlannedStmt` / the bgworker
//! `ParallelQueryMain`) is the separate parallel-worker keystone; this module
//! supplies the leader-side serialization so the Gather node stops truncating.

use alloc::string::String;

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::EStateData;

use crate::{
    framed, write_bitmapset_opt_field, write_bool_field, write_enum_field, write_int64_field,
    write_int_field, write_location_field, write_node_field, write_node_list_field,
};

/// `ExecSerializePlan(plan, estate)` — create a serialized representation of the
/// plan to be sent to each worker. Returns the textual `nodeToString(pstmt)` of a
/// dummy `PlannedStmt` wrapping a `resjunk`-cleared copy of `plan`.
///
/// `plan` is the leader plan node (C `planstate->plan`); the copy is allocated
/// against `mcx` (the executor's query context).
pub fn serialize_plan_for_workers(
    mcx: Mcx<'_>,
    plan_src: &types_nodes::nodes::Node<'_>,
    estate: &mut EStateData<'_>,
) -> PgResult<String> {
    // We can't scribble on the original plan, so make a copy.
    //   plan = copyObject(plan);
    let mut plan = plan_src.copy_node_in(mcx)?;

    // The worker will start its own copy of the executor, and that copy will
    // insert a junk filter if the toplevel node has any resjunk entries. We
    // don't want that to happen, because while resjunk columns shouldn't be
    // sent back to the user, here the tuples are coming back to another backend
    // which may very well need them. So mutate the target list accordingly.
    //   foreach(lc, plan->targetlist) tle->resjunk = false;
    if let Some(tlist) = plan.plan_head_mut().targetlist.as_mut() {
        for tle in tlist.iter_mut() {
            tle.resjunk = false;
        }
    }

    // Build the dummy PlannedStmt and serialize it. Most fields don't need to
    // be valid for our purposes, but the worker needs at least a minimal
    // PlannedStmt to start the executor. Because the repo's `PlannedStmt`
    // carrier is trimmed (no `planId`/`appendRelations`/`rewindPlanIDs`/...),
    // we emit `_outPlannedStmt`'s text directly from the live `EState`,
    // mirroring the C dummy field-fill field-for-field; the omitted carrier
    // fields render as their C empty form (`<>` / 0 / false), matching what a
    // `makeNode(PlannedStmt)`-zeroed dummy would emit.
    let mut buf = String::new();
    framed(&mut buf, |b| {
        out_dummy_plannedstmt(b, estate, &plan);
    });
    Ok(buf)
}

/// `_outPlannedStmt(str, pstmt)` (outfuncs.funcs.c:4713) over the dummy
/// `PlannedStmt` the C `ExecSerializePlan` builds. Field order mirrors the
/// generated writer exactly; `planTree` is the already-copied, `resjunk`-cleared
/// plan node; the list/bitmapset fields are read live off the `EState` (the C
/// dummy copies `estate->es_range_table`, `es_rteperminfos`, `es_unpruned_relids`,
/// and the parallel-safe-filtered `es_plannedstmt->subplans`).
fn out_dummy_plannedstmt(
    buf: &mut String,
    estate: &EStateData<'_>,
    plan: &types_nodes::nodes::Node<'_>,
) {
    // WRITE_NODE_TYPE("PLANNEDSTMT")
    buf.push_str("PLANNEDSTMT");

    // pstmt->commandType = CMD_SELECT;
    write_enum_field(buf, "commandType", CmdType::CMD_SELECT as i32);
    // pstmt->queryId = pgstat_get_my_query_id();   (dummy: 0)
    write_int64_field(buf, "queryId", 0);
    // pstmt->planId = pgstat_get_my_plan_id();      (carrier omits planId; 0)
    write_int64_field(buf, "planId", 0);
    // pstmt->hasReturning = false;
    write_bool_field(buf, "hasReturning", false);
    // pstmt->hasModifyingCTE = false;
    write_bool_field(buf, "hasModifyingCTE", false);
    // pstmt->canSetTag = true;
    write_bool_field(buf, "canSetTag", true);
    // pstmt->transientPlan = false;
    write_bool_field(buf, "transientPlan", false);
    // pstmt->dependsOnRole = false;
    write_bool_field(buf, "dependsOnRole", false);
    // pstmt->parallelModeNeeded = false;
    write_bool_field(buf, "parallelModeNeeded", false);
    // pstmt->jitFlags = 0;   (dummy leaves the makeNode-zeroed value)
    write_int_field(buf, "jitFlags", 0);

    // pstmt->planTree = plan;
    write_node_field(buf, "planTree", Some(plan), false);

    // pstmt->partPruneInfos = estate->es_part_prune_infos;
    // The trimmed carrier types these as opaque, with no `_outPartitionPruneInfo`
    // writer; the C dummy copies the list verbatim. Non-partitioned plans (the
    // worker-shipping cases reaching here) have an empty list — emit C `NIL`.
    let _ = write!(buf, " :partPruneInfos <>");

    // pstmt->rtable = estate->es_range_table;
    write_node_list_field(
        buf,
        "rtable",
        if estate.es_range_table.is_empty() {
            None
        } else {
            Some(estate.es_range_table.as_slice())
        },
        false,
        |b, rte, wl| framed(b, |bb| crate::out_parse_family::out_range_tbl_entry(bb, rte, wl)),
    );

    // pstmt->unprunableRelids = estate->es_unpruned_relids;
    write_bitmapset_opt_field(buf, "unprunableRelids", estate.es_unpruned_relids.as_deref());

    // pstmt->permInfos = estate->es_rteperminfos;
    write_node_list_field(
        buf,
        "permInfos",
        if estate.es_rteperminfos.is_empty() {
            None
        } else {
            Some(estate.es_rteperminfos.as_slice())
        },
        false,
        |b, pi, wl| framed(b, |bb| crate::out_parse_family::out_rte_perm_info(bb, pi, wl)),
    );

    // pstmt->resultRelations = NIL;
    let _ = write!(buf, " :resultRelations <>");
    // pstmt->appendRelations = NIL;   (carrier omits; C dummy sets NIL)
    let _ = write!(buf, " :appendRelations <>");

    // Transfer only parallel-safe subplans, leaving a NULL "hole" in the list
    // for unsafe ones (so the list indexes of the safe ones are preserved).
    //   foreach(lc, estate->es_plannedstmt->subplans)
    //       if (subplan && !subplan->parallel_safe) subplan = NULL;
    write_subplans(buf, estate);

    // pstmt->rewindPlanIDs = NULL;   (carrier omits; C dummy NULL bitmapset)
    let _ = write!(buf, " :rewindPlanIDs (b)");
    // pstmt->rowMarks = NIL;
    let _ = write!(buf, " :rowMarks <>");
    // pstmt->relationOids = NIL;
    let _ = write!(buf, " :relationOids <>");
    // pstmt->invalItems = NIL;
    let _ = write!(buf, " :invalItems <>");

    // pstmt->paramExecTypes = estate->es_plannedstmt->paramExecTypes;
    // `Oid` is `u32`; render the OID list directly (NIL -> `<>`).
    let param_exec_types = estate
        .es_plannedstmt
        .as_ref()
        .and_then(|p| p.paramExecTypes.as_ref());
    match param_exec_types {
        None => {
            let _ = write!(buf, " :paramExecTypes <>");
        }
        Some(v) => {
            let _ = write!(buf, " :paramExecTypes (o");
            for oid in v.iter() {
                let _ = write!(buf, " {}", *oid);
            }
            buf.push(')');
        }
    }

    // pstmt->utilityStmt = NULL;
    let _ = write!(buf, " :utilityStmt <>");
    // pstmt->stmt_location = -1; pstmt->stmt_len = -1;
    write_location_field(buf, "stmt_location", -1, false);
    write_location_field(buf, "stmt_len", -1, false);
}

/// `WRITE_NODE_FIELD(subplans)` over the parallel-safe-filtered subplan list.
/// Each element is the deep copy of the source subplan, or `<>` (NULL hole) for
/// a non-parallel-safe subplan, preserving list indexes.
fn write_subplans(buf: &mut String, estate: &EStateData<'_>) {
    let subplans = estate
        .es_plannedstmt
        .as_ref()
        .and_then(|p| p.subplans.as_ref());
    match subplans {
        None => {
            let _ = write!(buf, " :subplans <>");
        }
        Some(list) => {
            // C builds the dummy `subplans` list pointing at the originals
            // (`subplan` or a NULL hole for non-parallel-safe ones); the
            // serializer reads those nodes without mutating, so write them
            // directly from `es_plannedstmt->subplans`.
            let _ = write!(buf, " :subplans (");
            let mut first = true;
            for sp in list.iter() {
                if !first {
                    buf.push(' ');
                }
                first = false;
                match sp.as_deref() {
                    None => buf.push_str("<>"),
                    Some(node) => {
                        // if (subplan && !subplan->parallel_safe) subplan = NULL;
                        // A subplan is always a plan node, so `plan_head` is safe.
                        if node.plan_head().parallel_safe {
                            crate::out_node_inner(buf, node, false);
                        } else {
                            buf.push_str("<>");
                        }
                    }
                }
            }
            buf.push(')');
        }
    }
}

use core::fmt::Write as _;
