//! heapam_handler.c sample-scan lane (heapam_scan_sample_next_block /
//! heapam_scan_sample_next_tuple + SampleHeapTupleVisible). The TSM rides in
//! as the SampleScanDriver trait object (tsmapi.h's open extension point).

use crate::{
    check_for_interrupts, heap_prepare_pagescan, pgstat_count_heap_getnext, store_ctup_into_slot,
    HeapCheckForSerializableConflictOut, HeapScanDescData,
};
use ::bufmgr_seams::BufferPin;
use ::mcx::Mcx;
use ::tableam_vocab::{SampleScanDriver, SO_ALLOW_PAGEMODE, SO_ALLOW_SYNC};
use ::types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use ::types_error::PgResult;
use ::types_slot::SlotData;
use ::types_tuple::{HeapTupleData, ItemPointerData};

pub fn heap_scan_sample_next_block<'mcx>(
    scan: &mut HeapScanDescData<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    donetuples: i64,
) -> PgResult<bool> {
    if scan.rs_nblocks == 0 {
        return Ok(false);
    }

    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }

    let blockno = if scanstate.has_next_sample_block() {
        scanstate.next_sample_block(scan.rs_nblocks, donetuples)
    } else if scan.rs_cblock == InvalidBlockNumber {
        debug_assert!(!scan.rs_inited);
        scan.rs_startblock
    } else {
        debug_assert!(scan.rs_inited);
        let mut block = scan.rs_cblock + 1;
        if block >= scan.rs_nblocks {
            block = 0;
        }
        // Report before the end-of-scan check: the hint parks at the scan's
        // start (as C).
        if (scan.rs_base.rs_flags & SO_ALLOW_SYNC) != 0 {
            syncscan_seams::ss_report_location::call(&scan.rs_base.rs_rd, block)?;
        }
        if block == scan.rs_startblock {
            InvalidBlockNumber
        } else {
            block
        }
    };

    scan.rs_cblock = blockno;
    if blockno == InvalidBlockNumber {
        scan.rs_inited = false;
        return Ok(false);
    }
    debug_assert!(blockno < scan.rs_nblocks);

    // At least one interrupt check per page: long runs of dead tuples must
    // stay cancellable (as C).
    check_for_interrupts()?;

    let buf = bufmgr_seams::read_buffer_strategy::call(
        &scan.rs_base.rs_rd,
        blockno,
        scan.rs_strategy.clone(),
    )?;
    scan.rs_cbuf = BufferPin::adopt(buf);

    if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heap_prepare_pagescan(scan)?;
    }

    scan.rs_inited = true;
    Ok(true)
}

pub fn heap_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    scanstate: &mut dyn SampleScanDriver,
    donetuples: i64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let blockno = scan.rs_cblock;
    let pagemode = (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0;

    let found = {
        let relation = &scan.rs_base.rs_rd;
        let snapshot = scan
            .rs_base
            .rs_snapshot
            .as_deref()
            .expect("sample scan requires an MVCC snapshot");
        let pin = scan
            .rs_cbuf
            .as_ref()
            .expect("sample scan positioned without a buffer");
        let buffer = pin.buffer();
        // Non-pagemode visibility checks run under the content lock; found
        // tuples stay good under the pin alone after unlock (as C).
        let lock = if pagemode {
            None
        } else {
            Some(pin.lock_share()?)
        };
        let page = pin.page();
        let all_visible = page.is_all_visible() && !snapshot.takenDuringRecovery;
        let maxoffset = page.max_offset_number();

        let mut found = None;
        loop {
            check_for_interrupts()?;
            let tupoffset = scanstate.next_sample_tuple(blockno, maxoffset, donetuples);
            if tupoffset == types_tuple::itemptr::InvalidOffsetNumber {
                break;
            }
            let lp = page.item_id(tupoffset);
            if !lp.is_normal() {
                continue;
            }
            let (ptr, len) = page.item_raw(lp);
            // SAFETY: normal line pointer on the page pinned by rs_cbuf (and
            // share-locked when not in pagemode).
            let mut loctup = unsafe {
                HeapTupleData::from_raw_parts(
                    ptr,
                    len,
                    ItemPointerData::new(blockno, tupoffset),
                    relation.rd_id,
                )
            };
            let visible = if all_visible {
                true
            } else {
                sample_heap_tuple_visible(scan, tupoffset, &mut loctup)?
            };
            // In pagemode heap_prepare_pagescan already did this per tuple.
            if !pagemode {
                HeapCheckForSerializableConflictOut(
                    visible,
                    relation,
                    &mut loctup,
                    buffer,
                    snapshot,
                )?;
            }
            if visible {
                found = Some((ptr, len, tupoffset));
                break;
            }
        }
        drop(lock);
        found
    };

    match found {
        Some((ptr, len, tupoffset)) => {
            // SAFETY: normal line pointer on the page pinned by rs_cbuf; the
            // struct invariant ties rs_ctup's image to that pin.
            scan.rs_ctup = Some(unsafe {
                HeapTupleData::from_raw_parts(
                    ptr,
                    len,
                    ItemPointerData::new(blockno, tupoffset),
                    scan.rs_base.rs_rd.rd_id,
                )
            });
            pgstat_count_heap_getnext(scan);
            store_ctup_into_slot(mcx, scan, slot);
            Ok(true)
        }
        None => {
            exectuples::exec_clear_tuple(slot, mcx);
            Ok(false)
        }
    }
}

// SampleHeapTupleVisible (heapam_handler.c): pagemode consults the
// rs_vistuples[] populated by heap_prepare_pagescan (binary search over the
// sorted array); otherwise a per-tuple snapshot test.
fn sample_heap_tuple_visible(
    scan: &HeapScanDescData<'_>,
    tupoffset: OffsetNumber,
    loctup: &mut HeapTupleData<'_>,
) -> PgResult<bool> {
    if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        let vist = &scan.rs_vistuples[..scan.rs_ntuples as usize];
        Ok(vist.binary_search(&tupoffset).is_ok())
    } else {
        let snapshot = scan
            .rs_base
            .rs_snapshot
            .as_deref()
            .expect("sample scan requires an MVCC snapshot");
        let buffer = scan
            .rs_cbuf
            .as_ref()
            .expect("visibility check without a buffer")
            .buffer();
        crate::hv_seam::heap_tuple_satisfies_visibility::call(loctup, snapshot, buffer)
    }
}
