//! Tuple-routing structure build/teardown family:
//! `ExecSetupPartitionTupleRouting`, `ExecInitPartitionDispatchInfo`,
//! `ExecInitRoutingInfo`, `ExecCleanupTupleRouting`.
//! `ExecInitPartitionInfo` (the largest C function here, ~479 lines) lives in
//! its own `routing_init_info` sub-module.

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::executor::TupleSlotKind;
use types_nodes::partition::PartitionDescData;
use types_nodes::{EStateData, ModifyTableState, Opaque, ResultRelInfo, RriId};
use types_rel::Relation;
use types_storage::lock::{NoLock, RowExclusiveLock};

use crate::{PartitionDispatchData, PartitionDispatchId, PartitionTupleRouting};

/// `ExecSetupPartitionTupleRouting(estate, rel)` — set up the information
/// needed during tuple routing for a partitioned table and return it.
///
/// Allocates the routing struct and all subsidiary structs in `mcx` (C: the
/// current context, typically `estate->es_query_cxt`); fallible on OOM and on
/// the relcache reads `ExecInitPartitionDispatchInfo` performs.
pub fn ExecSetupPartitionTupleRouting<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
) -> PgResult<PartitionTupleRouting<'mcx>> {
    // proute = (PartitionTupleRouting *) palloc0(sizeof(PartitionTupleRouting));
    // proute->partition_root = rel;
    // proute->memcxt = CurrentMemoryContext;
    // Rest of members initialized by zeroing.
    let partoid = rel.rd_id;
    let mut proute = PartitionTupleRouting {
        partition_root: Some(rel),
        partition_dispatch_info: PgVec::new_in(mcx),
        nonleaf_partitions: PgVec::new_in(mcx),
        num_dispatch: 0,
        max_dispatch: 0,
        partitions: PgVec::new_in(mcx),
        is_borrowed_rel: PgVec::new_in(mcx),
        num_partitions: 0,
        max_partitions: 0,
        // proute->memcxt = CurrentMemoryContext; the owned model allocates from
        // the `mcx` threaded into each call, so this is the C null handle.
        memcxt: Opaque::default(),
    };

    // Initialize this table's PartitionDispatch object.  Here we pass in the
    // parent as NULL as we don't need to care about any parent of the target
    // partitioned table.
    //
    // ExecInitPartitionDispatchInfo(estate, proute, RelationGetRelid(rel),
    //                               NULL, 0, NULL);
    ExecInitPartitionDispatchInfo(mcx, estate, &mut proute, partoid, None, 0, None)?;

    Ok(proute)
}

/// `ExecInitRoutingInfo(mtstate, estate, proute, dispatch, partRelInfo,
/// partidx, is_borrowed_rel)` — set up tuple-conversion info for a partition
/// and track it in `proute`. Fallible (slot/array allocation, FDW init).
pub(crate) fn ExecInitRoutingInfo<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    dispatch: PartitionDispatchId,
    part_rel_info: RriId,
    partidx: i32,
    is_borrowed_rel: bool,
) -> PgResult<()> {
    // C: oldcxt = MemoryContextSwitchTo(proute->memcxt);
    // The owned model allocates from `mcx` (threaded explicitly), so the
    // context switch is a no-op here.
    let _ = mtstate;

    // Set up tuple conversion between root parent and the partition if the two
    // have different rowtypes.  If conversion is indeed required, also
    // initialize a slot dedicated to storing this partition's converted tuples.
    //
    // if (ExecGetRootToChildMap(partRelInfo, estate) != NULL) {
    //     Relation partrel = partRelInfo->ri_RelationDesc;
    //     partRelInfo->ri_PartitionTupleSlot =
    //         table_slot_create(partrel, &estate->es_tupleTable);
    // } else
    //     partRelInfo->ri_PartitionTupleSlot = NULL;
    let root_to_child =
        backend_executor_execUtils_seams::exec_get_root_to_child_map::call(mcx, estate, part_rel_info)?;
    let partition_tuple_slot = if root_to_child.is_some() {
        // table_slot_create pins the partition's TupleDesc and builds a slot of
        // the partition's AM-appropriate class; ExecAllocTableSlot then registers
        // it in es_tupleTable (C: the `&estate->es_tupleTable` reglist) so it is
        // dropped at end of command.
        let partrel = estate
            .result_rel(part_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("partition ResultRelInfo.ri_RelationDesc set")
            .alias();
        let slot = backend_access_table_tableam::table_slot_create(mcx, &partrel)?;
        Some(estate.push_slot_data(slot)?)
    } else {
        None
    };

    // If the partition is a foreign table, let the FDW init itself for routing
    // tuples to the partition (BeginForeignInsert), then determine its batch
    // size and reset the multi-insert buffer.  The trimmed ResultRelInfo carries
    // none of ri_FdwRoutine / ri_BatchSize / ri_CopyMultiInsertBuffer; those land
    // with the full nodeModifyTable ResultRelInfo.  For every relation the
    // trimmed type can represent (ri_FdwRoutine == NULL), the C path is: skip the
    // FDW branches, set ri_BatchSize = 1, ri_CopyMultiInsertBuffer = NULL — all
    // no-ops against the fields this type omits.
    let part_rel_info_data = estate.result_rel_mut(part_rel_info);
    part_rel_info_data.ri_PartitionTupleSlot = partition_tuple_slot;

    // Assert(dispatch->indexes[partidx] == -1);
    debug_assert_eq!(
        proute.partition_dispatch_info[dispatch].indexes[partidx as usize],
        -1
    );

    // rri_index = proute->num_partitions++;
    let rri_index = proute.num_partitions;
    proute.num_partitions += 1;

    // Allocate or enlarge the array, as needed.
    if proute.num_partitions >= proute.max_partitions {
        if proute.max_partitions == 0 {
            proute.max_partitions = 8;
        } else {
            proute.max_partitions *= 2;
        }
        let cap = proute.max_partitions as usize;
        let extra_p = cap.saturating_sub(proute.partitions.len());
        proute
            .partitions
            .try_reserve(extra_p)
            .map_err(|_| mcx.oom(extra_p * core::mem::size_of::<RriId>()))?;
        let extra_b = cap.saturating_sub(proute.is_borrowed_rel.len());
        proute
            .is_borrowed_rel
            .try_reserve(extra_b)
            .map_err(|_| mcx.oom(extra_b * core::mem::size_of::<bool>()))?;
    }

    // proute->partitions[rri_index] = partRelInfo;
    // proute->is_borrowed_rel[rri_index] = is_borrowed_rel;
    // (The owned vectors grow by push; rri_index is always the next slot.)
    debug_assert_eq!(rri_index as usize, proute.partitions.len());
    proute.partitions.push(part_rel_info);
    proute.is_borrowed_rel.push(is_borrowed_rel);

    // dispatch->indexes[partidx] = rri_index;
    proute.partition_dispatch_info[dispatch].indexes[partidx as usize] = rri_index;

    Ok(())
}

/// `ExecInitPartitionDispatchInfo(estate, proute, partoid, parent_pd, partidx,
/// rootResultRelInfo)` — lock the partitioned table (if not already), build its
/// `PartitionDispatch`, store it in `proute->partition_dispatch_info`, and
/// record the parent downlink. Returns the new dispatch's id. Fallible.
pub(crate) fn ExecInitPartitionDispatchInfo<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    partoid: Oid,
    parent_pd: Option<PartitionDispatchId>,
    partidx: i32,
    root_result_rel_info: Option<RriId>,
) -> PgResult<PartitionDispatchId> {
    // For data modification, it is better that executor does not include
    // partitions being detached, except when running in snapshot-isolation mode.
    //
    // if (estate->es_partition_directory == NULL)
    //     estate->es_partition_directory =
    //         CreatePartitionDirectory(estate->es_query_cxt,
    //                                  !IsolationUsesXactSnapshot());
    if estate.es_partition_directory.0.is_none() {
        let omit_detached =
            !backend_access_transam_xact_seams::isolation_uses_xact_snapshot::call();
        let query_cxt = estate.es_query_cxt;
        estate.es_partition_directory =
            backend_partitioning_core_seams::create_partition_directory::call(
                query_cxt,
                omit_detached,
            )?;
    }

    // Only sub-partitioned tables need to be locked here.  The root partitioned
    // table will already have been locked as it's referenced in the query's
    // rtable.
    //
    // if (partoid != RelationGetRelid(proute->partition_root))
    //     rel = table_open(partoid, RowExclusiveLock);
    // else
    //     rel = proute->partition_root;
    let root_oid = proute
        .partition_root
        .as_ref()
        .expect("PartitionTupleRouting.partition_root set")
        .rd_id;
    // `rel` here is the alias used to read the relation (key, descr, partdesc);
    // when we open a sub-partition we keep the owning handle to install as the
    // dispatch's reldesc, otherwise we alias the routing root.
    let (rel_owned, rel): (Option<Relation<'mcx>>, Relation<'mcx>) = if partoid != root_oid {
        let opened = backend_access_common_relation_seams::relation_open::call(
            mcx,
            partoid,
            RowExclusiveLock,
        )?;
        let alias = opened.alias();
        (Some(opened), alias)
    } else {
        let root = proute.partition_root.as_ref().unwrap();
        (None, root.alias())
    };

    // partdesc = PartitionDirectoryLookup(estate->es_partition_directory, rel);
    let partdesc: mcx::PgBox<'mcx, PartitionDescData<'mcx>> =
        backend_partitioning_core_seams::partition_directory_lookup::call(
            mcx,
            &mut estate.es_partition_directory,
            rel.alias(),
        )?;

    // pd = (PartitionDispatch) palloc(offsetof(PartitionDispatchData, indexes) +
    //                                 partdesc->nparts * sizeof(int));
    // pd->reldesc = rel;
    // pd->key = RelationGetPartitionKey(rel);
    // pd->keystate = NIL;
    // pd->partdesc = partdesc;
    let nparts = partdesc.nparts;
    let key = backend_utils_cache_partcache_seams::relation_get_partition_key::call(mcx, rel.alias())?;

    // For sub-partitioned tables, set up the tuple conversion map/slot from the
    // direct parent's rowtype; not required for the root partitioned table.
    let (tupmap, tupslot) = if let Some(parent) = parent_pd {
        // TupleDesc tupdesc = RelationGetDescr(rel);
        // pd->tupmap = build_attrmap_by_name_if_req(
        //     RelationGetDescr(parent_pd->reldesc), tupdesc, false);
        let parent_desc_owner = {
            let parent_pd_data = &proute.partition_dispatch_info[parent];
            parent_pd_data
                .reldesc
                .as_ref()
                .expect("parent PartitionDispatch.reldesc set")
                .alias()
        };
        let tupmap = backend_access_common_next_seams::build_attrmap_by_name_if_req::call(
            mcx,
            &parent_desc_owner.rd_att,
            &rel.rd_att,
            false,
        )?;
        // pd->tupslot = pd->tupmap ?
        //     MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual) : NULL;
        let tupslot = if tupmap.is_some() {
            let tupdesc_copy = rel.rd_att.clone_in(mcx)?;
            let tupdesc: types_tuple::heaptuple::TupleDesc<'mcx> =
                Some(mcx::alloc_in(mcx, tupdesc_copy)?);
            Some(backend_executor_execTuples_seams::make_single_tuple_table_slot::call(
                mcx,
                tupdesc,
                TupleSlotKind::Virtual,
            )?)
        } else {
            None
        };
        (tupmap, tupslot)
    } else {
        // Not required for the root partitioned table.
        (None, None)
    };

    // memset(pd->indexes, -1, sizeof(int) * partdesc->nparts);
    let mut indexes = mcx::vec_with_capacity_in(mcx, nparts as usize)?;
    for _ in 0..nparts {
        indexes.push(-1);
    }

    // pd->reldesc owns the open for a sub-partition (closed by
    // ExecCleanupTupleRouting); for the routing root it is the caller-owned
    // alias (the root is closed by ExecEndPlan/DoCopy, not here).
    let pd_reldesc = match rel_owned {
        Some(owned) => owned,
        None => rel.alias(),
    };
    let pd = PartitionDispatchData {
        reldesc: Some(pd_reldesc),
        key,
        keystate: PgVec::new_in(mcx),
        partdesc: Some(partdesc),
        tupslot,
        tupmap,
        indexes,
    };

    // Track in PartitionTupleRouting for later use.
    // dispatchidx = proute->num_dispatch++;
    let dispatchidx = proute.num_dispatch;
    proute.num_dispatch += 1;

    // Allocate or enlarge the array, as needed.
    if proute.num_dispatch >= proute.max_dispatch {
        if proute.max_dispatch == 0 {
            proute.max_dispatch = 4;
        } else {
            proute.max_dispatch *= 2;
        }
        let cap = proute.max_dispatch as usize;
        let extra_d = cap.saturating_sub(proute.partition_dispatch_info.len());
        proute
            .partition_dispatch_info
            .try_reserve(extra_d)
            .map_err(|_| {
                mcx.oom(extra_d * core::mem::size_of::<mcx::PgBox<'_, PartitionDispatchData<'_>>>())
            })?;
        let extra_n = cap.saturating_sub(proute.nonleaf_partitions.len());
        proute
            .nonleaf_partitions
            .try_reserve(extra_n)
            .map_err(|_| mcx.oom(extra_n * core::mem::size_of::<Option<RriId>>()))?;
    }

    // proute->partition_dispatch_info[dispatchidx] = pd;
    debug_assert_eq!(dispatchidx as usize, proute.partition_dispatch_info.len());
    proute
        .partition_dispatch_info
        .push(mcx::alloc_in(mcx, pd)?);

    // If setting up a PartitionDispatch for a sub-partitioned table, we may also
    // need a minimally valid ResultRelInfo for checking the partition constraint
    // later; set that up now.
    //
    // if (parent_pd) {
    //     ResultRelInfo *rri = makeNode(ResultRelInfo);
    //     InitResultRelInfo(rri, rel, 0, rootResultRelInfo, 0);
    //     proute->nonleaf_partitions[dispatchidx] = rri;
    // } else
    //     proute->nonleaf_partitions[dispatchidx] = NULL;
    if parent_pd.is_some() {
        let mut rri = ResultRelInfo::default();
        backend_executor_execMain_seams::init_result_rel_info::call(
            mcx,
            &mut rri,
            rel.alias(),
            0,
            root_result_rel_info,
            0,
        )?;
        let rri_id = estate.add_result_rel(rri)?;
        proute.nonleaf_partitions.push(Some(rri_id));
    } else {
        proute.nonleaf_partitions.push(None);
    }

    // Finally, if setting up a PartitionDispatch for a sub-partitioned table,
    // install a downlink in the parent to allow quick descent.
    //
    // if (parent_pd) {
    //     Assert(parent_pd->indexes[partidx] == -1);
    //     parent_pd->indexes[partidx] = dispatchidx;
    // }
    if let Some(parent) = parent_pd {
        let parent_pd_data = &mut proute.partition_dispatch_info[parent];
        debug_assert_eq!(parent_pd_data.indexes[partidx as usize], -1);
        parent_pd_data.indexes[partidx as usize] = dispatchidx;
    }

    Ok(dispatchidx as PartitionDispatchId)
}

/// `ExecCleanupTupleRouting(mtstate, proute)` — close all partitioned tables,
/// leaf partitions, and their indices set up for routing. Fallible (table/index
/// close, FDW shutdown can `elog(ERROR)`).
pub fn ExecCleanupTupleRouting<'mcx>(
    mtstate: &mut ModifyTableState<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
) -> PgResult<()> {
    let _ = mtstate;

    // Remember, proute->partition_dispatch_info[0] corresponds to the root
    // partitioned table, which we must not try to close (closed by the caller).
    // Also, tupslot is NULL for the root partitioned table.
    //
    // for (i = 1; i < proute->num_dispatch; i++) {
    //     PartitionDispatch pd = proute->partition_dispatch_info[i];
    //     table_close(pd->reldesc, NoLock);
    //     if (pd->tupslot)
    //         ExecDropSingleTupleTableSlot(pd->tupslot);
    // }
    for i in 1..proute.num_dispatch as usize {
        let pd = &mut proute.partition_dispatch_info[i];
        if let Some(reldesc) = pd.reldesc.take() {
            reldesc.close(NoLock)?;
        }
        if let Some(tupslot) = pd.tupslot.take() {
            backend_executor_execTuples_seams::exec_drop_single_tuple_table_slot::call(tupslot)?;
        }
    }

    // for (i = 0; i < proute->num_partitions; i++) {
    //     ResultRelInfo *resultRelInfo = proute->partitions[i];
    //     /* Allow any FDWs to shut down */
    //     if (resultRelInfo->ri_FdwRoutine != NULL &&
    //         resultRelInfo->ri_FdwRoutine->EndForeignInsert != NULL)
    //         resultRelInfo->ri_FdwRoutine->EndForeignInsert(mtstate->ps.state,
    //                                                        resultRelInfo);
    //     /* skip result relations borrowed from the owning ModifyTableState */
    //     if (proute->is_borrowed_rel[i]) continue;
    //     ExecCloseIndices(resultRelInfo);
    //     table_close(resultRelInfo->ri_RelationDesc, NoLock);
    // }
    //
    // FDW EndForeignInsert reads ri_FdwRoutine, absent from the trimmed
    // ResultRelInfo; it lands with the full nodeModifyTable ResultRelInfo, and is
    // a no-op for every relation the trimmed type can represent
    // (ri_FdwRoutine == NULL).  The non-borrowed leaf partitions, however, must
    // have their indices closed (ExecCloseIndices) before the relation is closed
    // (table_close).  ExecCloseIndices is owned by execIndexing.c, which has no
    // seam crate authored yet — consistent with this crate's other blocked-owner
    // sites (ExecInitPartitionInfo's ExecOpenIndices), the close of a routed leaf
    // partition's indices cannot run until that owner lands; panic loudly rather
    // than silently leak the index opens.
    for i in 0..proute.num_partitions as usize {
        if proute.is_borrowed_rel[i] {
            continue;
        }
        // The relation is closed in the same step as ExecCloseIndices in C;
        // splitting them (close the rel now, leak the indices) would be a worse
        // half-port than refusing the whole non-borrowed close until the
        // ExecCloseIndices owner exists.
        panic!(
            "ExecCleanupTupleRouting: closing a routed (non-borrowed) leaf \
             partition needs ExecCloseIndices (execIndexing.c), whose seam owner \
             has not landed"
        );
    }

    Ok(())
}
