//! Tuple-routing structure build/teardown family:
//! `ExecSetupPartitionTupleRouting`, `ExecInitPartitionDispatchInfo`,
//! `ExecInitPartitionInfo`, `ExecInitRoutingInfo`, `ExecCleanupTupleRouting`.

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

/// `ExecInitPartitionInfo(mtstate, estate, proute, dispatch, rootResultRelInfo,
/// partidx)` — lock the partition, build its `ResultRelInfo`, and store it in
/// the next free slot of `proute->partitions`. Returns the new `ResultRelInfo`
/// id. Fallible (table open, index open, expression compilation, OOM).
pub(crate) fn ExecInitPartitionInfo<'mcx>(
    mcx: Mcx<'mcx>,
    mtstate: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    proute: &mut PartitionTupleRouting<'mcx>,
    dispatch: PartitionDispatchId,
    root_result_rel_info: RriId,
    partidx: i32,
) -> PgResult<RriId> {
    // Oid partOid = dispatch->partdesc->oids[partidx];
    let part_oid = {
        let pd = &proute.partition_dispatch_info[dispatch];
        let partdesc = pd
            .partdesc
            .as_ref()
            .expect("PartitionDispatch.partdesc set");
        partdesc.oids[partidx as usize]
    };

    // partrel = table_open(partOid, RowExclusiveLock);
    let partrel =
        backend_access_common_relation_seams::relation_open::call(mcx, part_oid, RowExclusiveLock)?;

    // leaf_part_rri = makeNode(ResultRelInfo);
    let mut leaf_part_rri = ResultRelInfo::default();

    // InitResultRelInfo(leaf_part_rri, partrel, 0, rootResultRelInfo,
    //                   estate->es_instrument);
    let instrument = estate.es_instrument;
    backend_executor_execMain_seams::init_result_rel_info::call(
        mcx,
        &mut leaf_part_rri,
        partrel.alias(),
        0,
        Some(root_result_rel_info),
        instrument,
    )?;

    // The following per-partition setup steps — verifying the result rel is a
    // valid INSERT target (CheckValidResultRel), opening partition indices
    // (ExecOpenIndices), building the WITH CHECK OPTION / RETURNING / ON
    // CONFLICT / MERGE state, and storing the per-partition FdwRoutine/batch
    // bookkeeping — all read or write `ResultRelInfo` fields that the trimmed
    // executor type (and their owning -seams crates) do not yet carry.  Until
    // those land, the C body past InitResultRelInfo cannot be expressed; a loud
    // panic here beats silently dropping that logic.
    let _ = (mtstate, partidx, &partrel, &mut leaf_part_rri);
    panic!(
        "ExecInitPartitionInfo: CheckValidResultRel / ExecOpenIndices / WCO / \
         RETURNING / ON CONFLICT / MERGE setup not yet portable — trimmed \
         ResultRelInfo and its owners (execMain index/WCO, execExpr update \
         projection, nodeModifyTable merge) have not landed"
    );
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
    // The tuple-conversion-slot setup (ExecGetRootToChildMap + table_slot_create
    // into ri_PartitionTupleSlot) and the FDW init / batch-size / multi-insert
    // bookkeeping all touch `ResultRelInfo` fields the trimmed executor type does
    // not carry yet, and seams for ExecGetRootToChildMap / table_slot_create /
    // the FdwRoutine vtable have not been authored.  The array-tracking tail,
    // however, is fully expressible and is the part `proute` depends on.
    let _ = (mtstate, estate);

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
    //     ... FDW EndForeignInsert ...
    //     if (proute->is_borrowed_rel[i]) continue;
    //     ExecCloseIndices(resultRelInfo);
    //     table_close(resultRelInfo->ri_RelationDesc, NoLock);
    // }
    //
    // Closing a routed leaf partition runs FDW shutdown (EndForeignInsert, which
    // reads ri_FdwRoutine — absent from the trimmed ResultRelInfo), closes its
    // indices (ExecCloseIndices — no seam authored), and closes the relation.
    // Until those owners land this per-partition close loop cannot run; a loud
    // panic beats silently leaking the opens.  (No partitions are ever stored in
    // `proute->partitions` yet because ExecInitPartitionInfo/ExecInitRoutingInfo
    // are likewise blocked, so this is unreachable in practice.)
    if proute.num_partitions > 0 {
        panic!(
            "ExecCleanupTupleRouting: per-partition close (FDW EndForeignInsert, \
             ExecCloseIndices) not yet portable — ri_FdwRoutine and the \
             ExecCloseIndices owner have not landed"
        );
    }

    Ok(())
}
