//! heapam_handler.c bitmap-scan lane (heapam_scan_bitmap_next_tuple +
//! BitmapHeapScanNextBlock). C divergence: the read stream is collapsed to a
//! synchronous per-page ReadBuffer (prefetch/aio is the aio lane), and the
//! TBM iterator + bitmap ride in as parameters instead of
//! rs_base.st.rs_tbmiterator (the iterator carries no bitmap back-pointer).

use tidbitmap::{TIDBitmap, TbmIterator, TBM_MAX_TUPLES_PER_PAGE};

use crate::{
    heap_hot_search_buffer, store_ctup_into_slot, HeapCheckForSerializableConflictOut,
    HeapScanDescData,
};
use ::bufmgr_seams::BufferPin;
use ::mcx::Mcx;
use ::tableam_vocab::SO_TYPE_BITMAPSCAN;
use ::types_error::PgResult;
use ::types_slot::SlotData;
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::{FirstOffsetNumber, HeapTupleData, ItemPointerData};

pub fn heap_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    tbm: Option<&TIDBitmap<'_>>,
    iterator: &mut TbmIterator,
    slot: &mut SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    while scan.rs_cindex >= scan.rs_ntuples {
        if !bitmap_next_block(scan, tbm, iterator, recheck, lossy_pages, exact_pages)? {
            return Ok(false);
        }
    }

    let i = scan.rs_cindex;
    heap_scan_bitmap_batch_store(mcx, scan, i, slot);
    Ok(true)
}

/// Fused-drive support: advance to the next page with visible tuples and
/// return the staged count (rs_vistuples[0..n], visibility resolved at
/// staging); 0 = bitmap exhausted.
pub fn heap_scan_bitmap_next_pagebatch<'mcx>(
    scan: &mut HeapScanDescData<'mcx>,
    tbm: Option<&TIDBitmap<'_>>,
    iterator: &mut TbmIterator,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<u32> {
    loop {
        if !bitmap_next_block(scan, tbm, iterator, recheck, lossy_pages, exact_pages)? {
            return Ok(0);
        }
        if scan.rs_ntuples > 0 {
            return Ok(scan.rs_ntuples);
        }
    }
}

/// Store staged tuple `i` of the current bitmap page into `slot`.
#[inline(always)]
pub fn heap_scan_bitmap_batch_store<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    i: u32,
    slot: &mut SlotData<'mcx>,
) {
    debug_assert!(i < scan.rs_ntuples);
    let targoffset = scan.rs_vistuples[i as usize];
    let block = scan.rs_cblock;
    let rd_id = scan.rs_base.rs_rd.rd_id;
    let pin = scan
        .rs_cbuf
        .as_ref()
        .expect("bitmap scan positioned without a buffer");
    let page = pin.page();
    let lp = page.item_id(targoffset);
    debug_assert!(lp.is_normal());
    let (ptr, len) = page.item_raw(lp);
    // SAFETY: normal line pointer on the page pinned by rs_cbuf; the struct
    // invariant ties rs_ctup's image to that pin.
    scan.rs_ctup = Some(unsafe {
        HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(block, targoffset), rd_id)
    });

    store_ctup_into_slot(mcx, scan, slot);

    scan.rs_cindex = i + 1;
}

fn bitmap_next_block(
    scan: &mut HeapScanDescData<'_>,
    tbm: Option<&TIDBitmap<'_>>,
    iterator: &mut TbmIterator,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    debug_assert!((scan.rs_base.rs_flags & SO_TYPE_BITMAPSCAN) != 0);

    scan.rs_cindex = 0;
    scan.rs_ntuples = 0;

    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }

    let serializable = xact_seams::isolation_is_serializable::call();
    let (blockno, lossy, res_recheck, noffsets, offsets) = loop {
        crate::check_for_interrupts()?;
        let Some(tbmres) = iterator.next(tbm) else {
            return Ok(false);
        };
        // Entries past our snapshot of the relation end cannot be visible;
        // serializable isolation must still examine them.
        if !serializable && tbmres.blockno >= scan.rs_nblocks {
            continue;
        }
        let mut offsets = [0 as ::types_core::OffsetNumber; TBM_MAX_TUPLES_PER_PAGE];
        let noffsets = if !tbmres.lossy {
            tbmres.extract_page_tuples(&mut offsets)
        } else {
            0
        };
        break (
            tbmres.blockno,
            tbmres.lossy,
            tbmres.recheck,
            noffsets,
            offsets,
        );
    };

    *recheck = res_recheck;
    scan.rs_cblock = blockno;

    let buf = bufmgr_seams::read_buffer_strategy::call(
        &scan.rs_base.rs_rd,
        blockno,
        scan.rs_strategy.clone(),
    )?;
    scan.rs_cbuf = BufferPin::adopt(buf);
    let pin = scan
        .rs_cbuf
        .as_ref()
        .expect("read_buffer returned an invalid buffer");

    pruneheap_seams::heap_page_prune_opt::call(&scan.rs_base.rs_rd, pin.buffer())?;

    let relation = &scan.rs_base.rs_rd;
    let snapshot = scan
        .rs_base
        .rs_snapshot
        .as_deref()
        .expect("bitmap heap scan requires an MVCC snapshot");
    let buffer = pin.buffer();

    // Found-visible tuples stay good under the pin alone after unlock.
    let lock = pin.lock_share()?;
    let mut ntup: u32 = 0;
    if !lossy {
        for &offnum in &offsets[..noffsets] {
            let tid = ItemPointerData::new(blockno, offnum);
            let res = heap_hot_search_buffer(tid, relation, pin, snapshot, false, true)?;
            if res.found {
                scan.rs_vistuples[ntup as usize] =
                    ::types_tuple::itemptr::ItemPointerGetOffsetNumber(&res.tid);
                ntup += 1;
            }
        }
    } else {
        let page = lock.page();
        let maxoff = page.max_offset_number();
        assert!(
            maxoff as usize <= MaxHeapTuplesPerPage,
            "corrupt heap page: pd_lower implies {maxoff} line pointers"
        );
        let mut offnum = FirstOffsetNumber;
        while offnum <= maxoff {
            let lp = page.item_id(offnum);
            if !lp.is_normal() {
                offnum += 1;
                continue;
            }
            let (ptr, len) = page.item_raw(lp);
            // SAFETY: normal line pointer on a pinned + share-locked heap page.
            let mut loctup = unsafe {
                HeapTupleData::from_raw_parts(
                    ptr,
                    len,
                    ItemPointerData::new(blockno, offnum),
                    relation.rd_id,
                )
            };
            let valid = crate::hv_seam::heap_tuple_satisfies_visibility::call(
                &mut loctup,
                snapshot,
                buffer,
            )?;
            if valid {
                scan.rs_vistuples[ntup as usize] = offnum;
                ntup += 1;
                // PredicateLockTID (predicate.c): per-tuple SIREAD lock under
                // lossy pages (C heapam_scan_bitmap_next_block); the exact-TID
                // branch takes it inside heap_hot_search_buffer.
                if serializable {
                    predicate_seams::predicate_lock_tid::call(
                        relation,
                        ItemPointerData::new(blockno, offnum),
                        snapshot,
                        loctup.t_data().xmin(),
                    )?;
                }
            }
            HeapCheckForSerializableConflictOut(valid, relation, &mut loctup, buffer, snapshot)?;
            offnum += 1;
        }
    }
    drop(lock);

    debug_assert!(ntup as usize <= MaxHeapTuplesPerPage);
    scan.rs_ntuples = ntup;

    if lossy {
        *lossy_pages += 1;
    } else {
        *exact_pages += 1;
    }

    Ok(true)
}
