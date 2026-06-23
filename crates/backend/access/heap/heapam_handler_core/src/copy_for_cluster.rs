//! `heapam_relation_copy_for_cluster` (heapam_handler.c) — the heap AM's
//! `relation_copy_for_cluster` callback that rewrites a table's tuples into a
//! new heap in CLUSTER / VACUUM-FULL order.
//!
//! Two scan paths:
//!  * **index-scan** (`OldIndex != NULL && !use_sort`): scan the old heap in the
//!    old index's order via an `index_beginscan`/`index_getnext_slot` loop;
//!  * **seqscan + optional sort** (VACUUM FULL, or CLUSTER when seqscan+sort wins):
//!    a `table_beginscan`/`table_scan_getnextslot` loop. When `use_sort` is true
//!    (`plan_cluster_use_sort` picked seqscan+sort) the visible tuples are routed
//!    through the cluster tuplesort (`tuplesort_begin_cluster` /
//!    `tuplesort_putheaptuple`) and, after `tuplesort_performsort`, read back in
//!    index order (`tuplesort_getheaptuple`) and rewritten. When `use_sort` is
//!    false (VACUUM FULL, no index) the tuples are rewritten directly in scan
//!    order.
//!
//! In all paths the old heap is scanned with `SnapshotAny` and each tuple's fate
//! is decided by `HeapTupleSatisfiesVacuum`: live/recently-dead tuples are
//! copied (`rewrite_heap_tuple`, preserving update chains and frozen xids), dead
//! tuples are dropped (but still fed to `rewrite_heap_dead_tuple` so the rewrite
//! module can resolve update chains).

use ::mcx::Mcx;
use types_core::{MultiXactId, TransactionId};
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_slot::SlotData;
use ::types_cluster::CopyForClusterResult;

use heapam as heapam;
use heapam_visibility as visibility;
use rewriteheap as rewriteheap;
use indexam as indexam;
use bufmgr_seams as bufmgr_seam;
// CLUSTER's seqscan+sort path drives the tuplesort through its seam crate (the
// concrete tuplesort owner depends on this crate's executor seams indirectly;
// routing the calls through the seams avoids a dependency cycle).
use tuplesort_seams as tuplesort_seam;
use ::execTuples::exec_init_slots::ExecDropSingleTupleTableSlot;
use ::execTuples::slot_store_fetch::ExecFetchSlotHeapTuple;

use ::types_scan::sdir::ForwardScanDirection;
use ::snapshot::snapshot::{SnapshotData, SnapshotType};
use ::types_storage::buf::{BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use ::types_storage::Buffer;
use ::snapshot::snapshot::HTSV_Result;

use ::utils_error::ereport;
use types_error::{ErrorLocation, ERROR, WARNING};

/// `heapam_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
/// OldestXmin, *xid_cutoff, *multi_cutoff, *num_tuples, *tups_vacuumed,
/// *tups_recently_dead)` (heapam_handler.c).
///
/// `xid_cutoff`/`multi_cutoff` are in/out in C; the freeze xid / cutoff multi
/// are not modified by the heap AM (the rewrite state keeps them), so they are
/// echoed back unchanged on `CopyForClusterResult`. The three counters are the
/// out-params accumulated over the scan.
#[allow(clippy::too_many_arguments)]
pub fn heapam_relation_copy_for_cluster<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap: &Relation<'mcx>,
    new_heap: &Relation<'mcx>,
    old_index: Option<&Relation<'mcx>>,
    use_sort: bool,
    oldest_xmin: TransactionId,
    xid_cutoff: TransactionId,
    multi_cutoff: MultiXactId,
) -> PgResult<CopyForClusterResult> {
    // Remember if it's a system catalog.
    let is_system_catalog = catalog_catalog::IsSystemRelation(old_heap);

    // Valid smgr_targblock implies something already wrote to the relation.
    // Assert(RelationGetTargetBlock(NewHeap) == InvalidBlockNumber); — debug
    // only; the value model has no such persistent target block here.

    // Out-param accumulators (C's *num_tuples / *tups_vacuumed /
    // *tups_recently_dead, initialized to 0 by the caller `copy_table_data`).
    let mut num_tuples: f64 = 0.0;
    let mut tups_vacuumed: f64 = 0.0;
    let mut tups_recently_dead: f64 = 0.0;

    // Initialize the rewrite operation. (The values/isnull scratch arrays C
    // pallocs are local to `reform_and_rewrite_tuple` in the owned model.)
    let mut rwstate =
        rewriteheap::begin_heap_rewrite(mcx, old_heap, new_heap, oldest_xmin, xid_cutoff, multi_cutoff)?;

    // Set up sorting if wanted: tuplesort = tuplesort_begin_cluster(oldTupDesc,
    // OldIndex, maintenance_work_mem, NULL, TUPLESORT_NONE).
    let mut tuplesort: Option<nodes::Tuplesortstate<'mcx>> = if use_sort {
        let old_index = old_index
            .expect("copy_for_cluster: use_sort implies a btree OldIndex (cluster.c)");
        let maintenance_work_mem =
            vacuumlazy_seams::maintenance_work_mem::call()?;
        Some(tuplesort_seam::tuplesort_begin_cluster::call(
            mcx,
            &old_heap.rd_att,
            old_index,
            maintenance_work_mem,
            nodes::TUPLESORT_NONE,
        )?)
    } else {
        None
    };

    // Prepare to scan the OldHeap. To ensure we see recently-dead tuples that
    // still need to be copied, we scan with SnapshotAny and use
    // HeapTupleSatisfiesVacuum for the visibility test.
    let use_index_scan = old_index.is_some() && !use_sort;

    // table_slot_create(OldHeap, NULL): a BufferHeapTupleTableSlot.
    let mut slot = table_tableam::table_slot_create(mcx, old_heap)?;

    // Scan descriptors — exactly one of these is live.
    let mut index_scan: Option<::types_tableam::relscan::IndexScanDesc<'mcx>> = None;
    let mut table_scan: Option<std::boxed::Box<::types_tableam::relscan::TableScanDescData<'mcx>>> =
        None;

    if use_index_scan {
        let old_index = old_index.unwrap();
        // indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, NULL, 0, 0);
        let snapshot_any = SnapshotData::sentinel(SnapshotType::SNAPSHOT_ANY);
        let mut iscan =
            indexam::index_beginscan(mcx, old_heap, old_index, snapshot_any, None, 0, 0)?;
        // index_rescan(indexScan, NULL, 0, NULL, 0);
        indexam::index_rescan(mcx, &mut iscan, &[], 0, &[], 0)?;
        index_scan = Some(iscan);
    } else {
        // tableScan = table_beginscan(OldHeap, SnapshotAny, 0, NULL);
        // SnapshotAny == None in the value model; the C `table_beginscan` flags
        // are SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE.
        use ::types_tableam::relscan::{
            SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TYPE_SEQSCAN,
        };
        let flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
        let tscan = heapam::scan::heap_beginscan(
            mcx,
            old_heap.alias(),
            None,
            0,
            ::mcx::vec_with_capacity_in(mcx, 0)?,
            None,
            flags,
        )?;
        table_scan = Some(tscan);
    }

    // Scan through the OldHeap, either in OldIndex order or sequentially; copy
    // each tuple into the NewHeap. Dead tuples are not bothered with.
    loop {
        // CHECK_FOR_INTERRUPTS(): cooperative-cancellation point; the owned model
        // has no signal machinery reachable here, so it is a no-op.

        // Fetch the next tuple into the slot.
        let got = if let Some(iscan) = index_scan.as_mut() {
            let more = indexam::index_getnext_slot(mcx, iscan, ForwardScanDirection, &mut slot)?;
            if more && iscan.xs_recheck {
                // Since we used no scan keys, should never need to recheck.
                return Err(ereport(ERROR)
                    .errmsg_internal("CLUSTER does not support lossy index conditions")
                    .into_error());
            }
            more
        } else {
            let tscan = table_scan.as_mut().expect("seqscan path has a table scan");
            table_tableam::table_scan_getnextslot(
                mcx,
                tscan,
                ForwardScanDirection,
                &mut slot,
            )?
        };
        if !got {
            break;
        }

        // tuple = ExecFetchSlotHeapTuple(slot, false, NULL); buf = hslot->buffer;
        let (tuple, _should_free) = ExecFetchSlotHeapTuple(mcx, &mut slot, false)?;
        let buf: Buffer = match &slot {
            SlotData::BufferHeap(bslot) => bslot.buffer,
            _ => panic!("copy_for_cluster: table_slot_create did not yield a BufferHeap slot"),
        };

        // LockBuffer(buf, BUFFER_LOCK_SHARE);
        bufmgr_seam::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;

        // Run the visibility test, then decide live/dead. The header reads for
        // the in-progress warnings need a fresh mutable view (HeapTupleSatisfies
        // Vacuum may resolve a frozen xmax), so do them while we still hold the
        // share lock, mirroring C (which keeps the lock across the switch).
        let mut tuple_for_vac = tuple.tuple.clone_in(mcx)?;
        let htsv = visibility::HeapTupleSatisfiesVacuum(&mut tuple_for_vac, oldest_xmin, buf)?;

        let isdead: bool = match htsv {
            HTSV_Result::HEAPTUPLE_DEAD => true,
            HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
                tups_recently_dead += 1.0;
                /* fall through — live or recently dead, must copy */
                false
            }
            HTSV_Result::HEAPTUPLE_LIVE => false,
            HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {
                // Normally only visible if inserted earlier in our own xact;
                // can happen in system catalogs. Warn if neither case applies,
                // but copy it regardless.
                let xmin = {
                    let hdr = header_ref(&tuple_for_vac);
                    visibility::htup::HeapTupleHeaderGetXmin(hdr)
                };
                if !is_system_catalog
                    && !transam_xact::TransactionIdIsCurrentTransactionId(xmin)
                {
                    ereport(WARNING)
                        .errmsg_internal(format!(
                            "concurrent insert in progress within table \"{}\"",
                            old_heap.name()
                        ))
                        .finish(ErrorLocation::new(
                            "heapam_handler.c",
                            872,
                            "heapam_relation_copy_for_cluster",
                        ))?;
                }
                /* treat as live */
                false
            }
            HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
                let update_xid = {
                    let hdr = header_ref(&tuple_for_vac);
                    visibility::HeapTupleHeaderGetUpdateXid(hdr)?
                };
                if !is_system_catalog
                    && !transam_xact::TransactionIdIsCurrentTransactionId(update_xid)
                {
                    ereport(WARNING)
                        .errmsg_internal(format!(
                            "concurrent delete in progress within table \"{}\"",
                            old_heap.name()
                        ))
                        .finish(ErrorLocation::new(
                            "heapam_handler.c",
                            884,
                            "heapam_relation_copy_for_cluster",
                        ))?;
                }
                /* treat as recently dead */
                tups_recently_dead += 1.0;
                false
            }
        };

        // LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        bufmgr_seam::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;

        if isdead {
            tups_vacuumed += 1.0;
            // heap rewrite module still needs to see it...
            if rewriteheap::rewrite_heap_dead_tuple(&mut rwstate, &tuple)? {
                // A previous recently-dead tuple is now known dead.
                tups_vacuumed += 1.0;
                tups_recently_dead -= 1.0;
            }
            continue;
        }

        num_tuples += 1.0;
        if let Some(ts) = tuplesort.as_mut() {
            // In scan-and-sort mode, stash the tuple in the tuplesort module; it
            // is written to the new heap (in index order) after the sort.
            tuplesort_seam::tuplesort_putheaptuple::call(ts, &tuple)?;
        } else {
            // reform_and_rewrite_tuple(tuple, OldHeap, NewHeap, values, isnull, rwstate).
            reform_and_rewrite_tuple(mcx, &tuple, old_heap, new_heap, &mut rwstate)?;
        }
    }

    // index_endscan(indexScan); / table_endscan(tableScan);
    if let Some(iscan) = index_scan.take() {
        indexam::index_endscan(mcx, iscan)?;
    }
    if let Some(tscan) = table_scan.take() {
        heapam::scan::heap_endscan(tscan)?;
    }
    // ExecDropSingleTupleTableSlot(slot): release the final page's buffer pin and
    // free the slot.
    ExecDropSingleTupleTableSlot(slot)?;

    // In scan-and-sort mode, complete the sort, then read out all live tuples
    // from the tuplestore and write them to the new relation.
    if let Some(mut ts) = tuplesort.take() {
        tuplesort_seam::tuplesort_performsort::call(&mut ts)?;

        loop {
            // CHECK_FOR_INTERRUPTS(): cooperative-cancellation point (no-op here).
            match tuplesort_seam::tuplesort_getheaptuple::call(&mut ts, true)? {
                None => break,
                Some(tuple) => {
                    reform_and_rewrite_tuple(mcx, &tuple, old_heap, new_heap, &mut rwstate)?;
                }
            }
        }

        // tuplesort_end(tuplesort).
        let boxed: ::mcx::PgBox<'mcx, nodes::Tuplesortstate<'mcx>> = ::mcx::alloc_in(mcx, ts)?;
        tuplesort_seam::tuplesort_end::call(boxed)?;
    }

    // Write out any remaining tuples, and fsync if needed.
    rewriteheap::end_heap_rewrite(rwstate)?;

    Ok(CopyForClusterResult {
        // The heap AM does not advance the freeze xid / cutoff multi here; echo
        // the caller's values back.
        new_frozen_xid: xid_cutoff,
        new_cutoff_multi: multi_cutoff,
        num_tuples,
        tups_vacuumed,
        tups_recently_dead,
    })
}

/// `reform_and_rewrite_tuple(tuple, OldHeap, NewHeap, values, isnull, rwstate)`
/// (heapam_handler.c) — deform the old tuple by the old descriptor, null out any
/// dropped columns per the new descriptor, re-form by the new descriptor, and
/// hand it to the rewrite module.
fn reform_and_rewrite_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &heaptuple::FormedTuple<'mcx>,
    old_heap: &Relation<'mcx>,
    new_heap: &Relation<'mcx>,
    rwstate: &mut rewriteheap::seams::RewriteState<'mcx>,
) -> PgResult<()> {
    let old_tup_desc = &old_heap.rd_att;
    let new_tup_desc = &new_heap.rd_att;

    // heap_deform_tuple(tuple, oldTupDesc, values, isnull);
    let cols =
        heaptuple::heap_deform_tuple(mcx, &tuple.tuple, old_tup_desc, &tuple.data)?;

    let new_natts = new_tup_desc.natts as usize;
    let mut values: Vec<heaptuple::Datum<'mcx>> = Vec::with_capacity(new_natts);
    let mut isnull: Vec<bool> = Vec::with_capacity(new_natts);
    for i in 0..new_natts {
        if i < cols.len() {
            values.push(cols[i].0.clone());
            isnull.push(cols[i].1);
        } else {
            // Defensive: new descriptor has more columns than the deform yielded
            // (shouldn't happen — descriptors have the same column count).
            values.push(heaptuple::Datum::null());
            isnull.push(true);
        }
        // Be sure to null out any dropped columns.
        if new_tup_desc.attr(i).attisdropped {
            isnull[i] = true;
        }
    }

    // copiedTuple = heap_form_tuple(newTupDesc, values, isnull);
    let copied_tuple = heaptuple::heap_form_tuple(mcx, new_tup_desc, &values, &isnull)
        .map_err(|e| {
            ereport(ERROR)
                .errmsg_internal(format!("heap_form_tuple failed in CLUSTER rewrite: {e:?}"))
                .into_error()
        })?;

    // The heap rewrite module does the rest. (heap_freetuple(copiedTuple) in C —
    // the owned model drops `copiedTuple` when `rewrite_heap_tuple` consumes it.)
    rewriteheap::rewrite_heap_tuple(rwstate, tuple, copied_tuple)?;

    Ok(())
}

/// Borrow the tuple header for the in-progress-warning xmin/update-xid reads.
fn header_ref<'a, 'mcx>(
    tuple: &'a types_tuple::heaptuple::HeapTupleData<'mcx>,
) -> &'a types_tuple::heaptuple::HeapTupleHeaderData<'mcx> {
    tuple
        .t_data
        .as_ref()
        .expect("copy_for_cluster: live tuple has no t_data")
}
