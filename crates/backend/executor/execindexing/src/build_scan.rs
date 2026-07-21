// heapam_index_build_range_scan (heapam_handler.c), serial lane; boundary
// hoisted above heapam_handler (execindexing already sits above the AM stack
// — a heapam_handler home would cycle).
// Loud: concurrent builds, parallel scans, foreign in-progress xacts
// (XactLockTableWait lane; "anyvisible" mode indexes those without waiting).
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{BlockNumber, INDEX_MAX_KEYS, InvalidBlockNumber};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_slot::SlotData;
use ::types_snapshot::HTSV_Result::*;
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::itemptr::{InvalidOffsetNumber, ItemPointerData};
use ::types_tuple::HeapTupleData;
use tableam_vocab::{SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TYPE_SEQSCAN};

use crate::{index_predicate_passes, unported, FormIndexDatum, IndexInfo};

pub fn table_index_build_scan<'mcx, F>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    allow_sync: bool,
    callback: F,
) -> PgResult<f64>
where
    F: FnMut(&Relation<'mcx>, &ItemPointerData, &[Datum], &[bool], bool) -> PgResult<()>,
{
    table_index_build_range_scan(
        mcx,
        heap_relation,
        index_relation,
        index_info,
        allow_sync,
        false,
        0,
        InvalidBlockNumber,
        callback,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn table_index_build_range_scan<'mcx, F>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    allow_sync: bool,
    anyvisible: bool,
    start_blockno: BlockNumber,
    numblocks: BlockNumber,
    callback: F,
) -> PgResult<f64>
where
    F: FnMut(&Relation<'mcx>, &ItemPointerData, &[Datum], &[bool], bool) -> PgResult<()>,
{
    table_index_build_range_scan_with_xmin(
        mcx,
        heap_relation,
        index_relation,
        index_info,
        allow_sync,
        anyvisible,
        start_blockno,
        numblocks,
        None,
        callback,
    )
}

/// [`table_index_build_range_scan`] with a HOISTED `OldestXmin` (M4.2
/// parallel build): the SnapshotAny lane's horizon is per-tuple
/// conservative, so any single value valid at scan start works for every
/// range — the pool driver computes it ONCE on the leader and shares it, so
/// (i) per-claim `GetOldestNonRemovableTransactionId` procarray scans are
/// not paid at morsel cadence and (ii) every worker routes DEAD vs
/// RECENTLY_DEAD identically to a serial scan started at the same instant
/// (the parity leg's determinism). `None` keeps today's per-call compute.
#[allow(clippy::too_many_arguments)]
pub fn table_index_build_range_scan_with_xmin<'mcx, F>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    index_info: &mut IndexInfo<'mcx>,
    allow_sync: bool,
    anyvisible: bool,
    start_blockno: BlockNumber,
    numblocks: BlockNumber,
    hoisted_oldest_xmin: Option<types_core::TransactionId>,
    mut callback: F,
) -> PgResult<f64>
where
    F: FnMut(&Relation<'mcx>, &ItemPointerData, &[Datum], &[bool], bool) -> PgResult<()>,
{
    let concurrent = index_info.ii_Concurrent;
    let is_system_catalog = catalog::IsSystemRelation(heap_relation);
    let checking_uniqueness = index_info.ii_Unique || index_info.ii_HasExclusion;
    // C: "any visible" is incompatible with uniqueness checks and only valid
    // under SnapshotAny (non-concurrent).
    debug_assert!(!(anyvisible && checking_uniqueness));
    debug_assert!(!(anyvisible && concurrent));

    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        types_slot::TupleSlotKind::BufferHeapTuple,
        Some(heap_relation.rd_att.clone()),
    );

    // Concurrent builds scan under a registered MVCC snapshot; OldestXmin is
    // only for the SnapshotAny lane's HTSV routing.
    let registered = if concurrent {
        let snap = snapmgr::GetTransactionSnapshot()?;
        Some(snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered snapshot"))
    } else {
        None
    };
    let oldest_xmin = if concurrent {
        types_core::InvalidTransactionId
    } else if let Some(x) = hoisted_oldest_xmin {
        debug_assert!(x != 0);
        x
    } else {
        let x = procarray::GetOldestNonRemovableTransactionId(heap_relation)?;
        debug_assert!(x != 0);
        x
    };

    let mut flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE;
    if allow_sync {
        flags |= SO_ALLOW_SYNC;
    }
    let mut scan = heapam::heap_beginscan(
        mcx,
        heap_relation,
        registered.clone(), // None is SnapshotAny
        0,
        PgVec::new_in(mcx),
        None,
        flags,
    )?;

    if !allow_sync {
        heapam::heap_setscanlimits(&mut scan, start_blockno, numblocks);
    } else {
        // C: syncscan can only be requested on the whole relation.
        debug_assert!(start_blockno == 0 && numblocks == InvalidBlockNumber);
    }

    // C heapam_handler.c ExecPrepareQual runs before the scan: predicate fold
    // errors surface even when no tuple is ever tested.
    crate::prepare_index_predicate(mcx, index_info)?;

    let mut reltuples = 0.0f64;
    let mut root_blkno = InvalidBlockNumber;
    let mut root_offsets = [InvalidOffsetNumber; MaxHeapTuplesPerPage];
    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];
    // C's per-tuple econtext reset (heapam_handler.c:1611): expression and
    // predicate results land here, consumed by `callback` within the
    // iteration, freed before the next tuple.
    let mut per_tuple = mcx::MemoryContext::new_bump("IndexBuildPerTuple");

    loop {
        per_tuple.reset();
        let Some((mut tuple, buffer)) = next_tuple(&mut scan)? else {
            break;
        };

        if scan.rs_cblock != root_blkno {
            let pin = scan.rs_cbuf.as_ref().expect("pinned page for returned tuple");
            let guard = pin.lock_share()?;
            pruneheap::heap_get_root_tuples(pin.page(), &mut root_offsets)?;
            drop(guard);
            root_blkno = scan.rs_cblock;
        }

        let tuple_is_alive;
        let index_it;
        if concurrent {
            // MVCC-snapshot lane: every returned tuple is visible and indexed.
            index_it = true;
            tuple_is_alive = true;
            reltuples += 1.0;
        } else {
            let pin = scan.rs_cbuf.as_ref().expect("pinned page for returned tuple");
            let guard = pin.lock_share()?;
            let htsv = heapam_visibility::HeapTupleSatisfiesVacuum(&mut tuple, oldest_xmin, buffer)?;
            drop(guard);
            match htsv {
                HEAPTUPLE_DEAD => {
                    index_it = false;
                    tuple_is_alive = false;
                }
                HEAPTUPLE_LIVE => {
                    index_it = true;
                    tuple_is_alive = true;
                    reltuples += 1.0;
                }
                HEAPTUPLE_RECENTLY_DEAD => {
                    if tuple.t_data().is_hot_updated() {
                        index_it = false;
                        index_info.ii_BrokenHotChain = true;
                    } else {
                        index_it = true;
                    }
                    tuple_is_alive = false;
                }
                HEAPTUPLE_INSERT_IN_PROGRESS if anyvisible => {
                    index_it = true;
                    tuple_is_alive = true;
                    reltuples += 1.0;
                }
                HEAPTUPLE_INSERT_IN_PROGRESS => {
                    let xwait = tuple.t_data().xmin();
                    if !xact::TransactionIdIsCurrentTransactionId(xwait) {
                        if !is_system_catalog {
                            // unported: XactLockTableWait lane
                            // (heapam_index_build_range_scan).
                            return Err(unported(
                                "index build over a concurrent in-progress insert",
                            ));
                        }
                        if checking_uniqueness {
                            // unported: XactLockTableWait recheck lane.
                            return Err(unported(
                                "unique-index build over a concurrent in-progress insert",
                            ));
                        }
                    } else {
                        reltuples += 1.0;
                    }
                    index_it = true;
                    tuple_is_alive = true;
                }
                HEAPTUPLE_DELETE_IN_PROGRESS if anyvisible => {
                    index_it = true;
                    tuple_is_alive = false;
                    reltuples += 1.0;
                }
                HEAPTUPLE_DELETE_IN_PROGRESS => {
                    let xwait = heapam::HeapTupleHeaderGetUpdateXid(tuple.t_data())?;
                    if !xact::TransactionIdIsCurrentTransactionId(xwait) {
                        if !is_system_catalog || checking_uniqueness || tuple.t_data().is_hot_updated()
                        {
                            // unported: XactLockTableWait lane
                            // (heapam_index_build_range_scan).
                            return Err(unported(
                                "index build over a concurrent in-progress delete",
                            ));
                        }
                        index_it = true;
                        reltuples += 1.0;
                    } else if tuple.t_data().is_hot_updated() {
                        index_it = false;
                        index_info.ii_BrokenHotChain = true;
                    } else {
                        index_it = true;
                    }
                    tuple_is_alive = false;
                }
            }
        }

        if !index_it {
            continue;
        }

        exectuples::exec_store_buffer_heap_tuple(&mut slot, mcx, tuple, buffer);

        if !index_info.ii_Predicate.is_nil()
            && !index_predicate_passes(mcx, per_tuple.mcx(), index_info, &mut slot)?
        {
            continue;
        }

        FormIndexDatum(
            mcx,
            per_tuple.mcx(),
            index_info,
            &mut slot,
            &mut values[..],
            &mut isnull[..],
        )?;

        let self_tid = slot_tid(&slot);
        if slot_is_heap_only(&slot) {
            let offnum = self_tid.ip_posid;
            let mut root = root_offsets[offnum as usize - 1];
            if root == InvalidOffsetNumber {
                let pin = scan.rs_cbuf.as_ref().expect("pinned page for returned tuple");
                let guard = pin.lock_share()?;
                pruneheap::heap_get_root_tuples(pin.page(), &mut root_offsets)?;
                drop(guard);
                root = root_offsets[offnum as usize - 1];
            }
            let blkno = ::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(&self_tid);
            if root == InvalidOffsetNumber {
                return Err(Box::new(types_error::PgError::error(format!(
                    "failed to find parent tuple for heap-only tuple at ({blkno},{offnum}) in table \"{}\"",
                    heap_relation.name()
                ))
                .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED)));
            }
            let tid = ItemPointerData::new(blkno, root);
            callback(index_relation, &tid, &values[..], &isnull[..], tuple_is_alive)?;
        } else {
            callback(index_relation, &self_tid, &values[..], &isnull[..], tuple_is_alive)?;
        }
    }

    exectuples::exec_clear_tuple(&mut slot, mcx);
    heapam::heap_endscan(scan)?;
    if let Some(snap) = registered {
        snapmgr::UnregisterSnapshot(Some(&snap));
    }
    Ok(reltuples)
}

fn next_tuple<'mcx>(
    scan: &mut heapam::HeapScanDescData<'mcx>,
) -> PgResult<Option<(HeapTupleData<'mcx>, types_core::Buffer)>> {
    use types_scan::ScanDirection;
    if heapam::heap_getnext(scan, ScanDirection::ForwardScanDirection)?.is_none() {
        return Ok(None);
    }
    let t = scan.rs_ctup().expect("just returned Some");
    let buffer = scan.rs_cbuf.as_ref().expect("pinned").buffer();
    // SAFETY: same live pinned image rs_ctup views; hint-bit writes through
    // the &mut view are the C SatisfiesVacuum contract (buffer share-locked).
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid)
    };
    Ok(Some((tuple, buffer)))
}

fn slot_tid(slot: &SlotData<'_>) -> ItemPointerData {
    slot.base().tts_tid
}

fn slot_is_heap_only(slot: &SlotData<'_>) -> bool {
    match slot {
        SlotData::BufferHeap(b) => b
            .base
            .tuple
            .as_ref()
            .is_some_and(|t| t.t_data().is_heap_only()),
        _ => unreachable!("build scan slot is BufferHeap"),
    }
}
