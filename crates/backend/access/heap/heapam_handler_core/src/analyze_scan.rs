//! `access/heap/heapam_handler.c::heapam_scan_analyze_next_block` /
//! `heapam_scan_analyze_next_tuple` — the heap AM's ANALYZE sampling scan
//! primitives, driven by `commands/analyze.c::acquire_sample_rows`.
//!
//! The outer block callback pins + share-locks the next block to sample (handed
//! to it already pinned by the read stream); the inner tuple callback walks that
//! block's line pointers, runs each tuple through `HeapTupleSatisfiesVacuum` to
//! maintain the live/dead row counters, and stores the next sampleable tuple
//! into the slot (leaving the buffer locked between tuples and unlocking +
//! clearing the slot at end of block).
//!
//! Ported faithfully branch-for-branch from C. The read stream lives in
//! `analyze.c` (far above this crate); the block callback receives the
//! "next pinned buffer" as a closure the owner builds over its stream (the
//! same closure-across-layers technique the index-build callback uses), so the
//! `read_stream_next_buffer(stream, NULL)` of C is the closure call here.

use mcx::Mcx;

use bufmgr_seams as bufmgr_seam;
use execTuples_seams as slot_seam;
use types_core::primitive::OffsetNumber;
use types_core::TransactionId;
use types_error::PgResult;
use types_slot::SlotData;
use types_storage::buf::{Buffer, BufferIsValid, InvalidBuffer};
use types_tableam::relscan::TableScanDescData;
use types_tuple::heaptuple::FormedTuple;

use heapam as heapam;
use heapam_visibility as visibility;
use page::{
    ItemIdGetLength, ItemIdIsDead, ItemIdIsNormal, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageRef,
};

/// `BUFFER_LOCK_SHARE` (bufmgr.h).
const BUFFER_LOCK_SHARE: i32 = 1;

/// `FirstOffsetNumber` (off.h).
const FIRST_OFFSET_NUMBER: OffsetNumber = types_tuple::heaptuple::FIRST_OFFSET_NUMBER;

/// `heapam_scan_analyze_next_block(scan, stream)` (heapam_handler.c).
///
/// We must maintain a pin on the target page's buffer to ensure that concurrent
/// activity — e.g. HOT pruning — doesn't delete tuples out from under us. It
/// comes from the stream already pinned. We also choose to hold sharelock on
/// the buffer throughout — we could release and re-acquire sharelock for each
/// tuple, but since we aren't doing much work per tuple, the extra lock traffic
/// is probably better avoided.
pub fn heapam_scan_analyze_next_block<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    next_buffer: &mut dyn FnMut() -> PgResult<Buffer>,
) -> PgResult<bool> {
    // hscan->rs_cbuf = read_stream_next_buffer(stream, NULL);
    let cbuf = next_buffer()?;
    heapam::scan::heap_scan_state(scan).rs_cbuf = cbuf;
    if !BufferIsValid(cbuf) {
        return Ok(false);
    }

    bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;

    let cblock = bufmgr_seam::buffer_get_block_number::call(cbuf);
    let hscan = heapam::scan::heap_scan_state(scan);
    hscan.rs_cblock = cblock;
    hscan.rs_cindex = FIRST_OFFSET_NUMBER as u32;
    Ok(true)
}

/// Per-tuple sampling decision computed from `HeapTupleSatisfiesVacuum`:
/// whether to sample this tuple, and the increments to the live/dead counters.
struct AnalyzeDecision {
    sample_it: bool,
    live: f64,
    dead: f64,
}

/// `heapam_scan_analyze_next_tuple(scan, OldestXmin, liverows, deadrows, slot)`
/// (heapam_handler.c). Inner loop over all tuples on the selected page.
pub fn heapam_scan_analyze_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut TableScanDescData<'mcx>,
    oldest_xmin: TransactionId,
    liverows: &mut f64,
    deadrows: &mut f64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Assert(TTS_IS_BUFFERTUPLE(slot)).
    debug_assert!(matches!(slot, SlotData::BufferHeap(_)));

    let relid = scan.rs_rd.rd_id;
    let cbuf = heapam::scan::heap_scan_state(scan).rs_cbuf;
    let cblock = heapam::scan::heap_scan_state(scan).rs_cblock;

    // targpage = BufferGetPage(hscan->rs_cbuf); maxoffset =
    // PageGetMaxOffsetNumber(targpage). The buffer is share-locked (held since
    // scan_analyze_next_block) so reading the page is safe.
    let mut maxoffset: OffsetNumber = 0;
    bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        maxoffset = PageGetMaxOffsetNumber(&page);
        Ok(())
    })?;

    // Inner loop over all tuples on the selected page.
    loop {
        let cindex = heapam::scan::heap_scan_state(scan).rs_cindex;
        if cindex as OffsetNumber > maxoffset {
            break;
        }
        let off = cindex as OffsetNumber;

        // Read the line pointer; on a non-normal pointer count DEAD ones as
        // dead (we need vacuum to run to get rid of them) and skip. This rule
        // agrees with the way heap_page_prune_and_freeze() counts things.
        let mut produced: Option<FormedTuple<'mcx>> = None;
        let mut count_dead_lp = false;
        let mut is_normal = false;
        bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
            let page = PageRef::new(page_bytes)?;
            let itemid = PageGetItemId(&page, off)?;
            if !ItemIdIsNormal(&itemid) {
                if ItemIdIsDead(&itemid) {
                    count_dead_lp = true;
                }
                return Ok(());
            }
            is_normal = true;
            // ItemPointerSet(&targtuple->t_self, rs_cblock, rs_cindex);
            // targtuple->t_tableOid = RelationGetRelid(scan->rs_rd);
            // targtuple->t_data = PageGetItem(targpage, itemid);
            // targtuple->t_len = ItemIdGetLength(itemid).
            let item = PageGetItem(&page, &itemid)?;
            produced = Some(FormedTuple::read_on_page_full(
                mcx,
                &item[..ItemIdGetLength(&itemid) as usize],
                cblock,
                off,
                relid,
            )?);
            Ok(())
        })?;

        if !is_normal {
            if count_dead_lp {
                *deadrows += 1.0;
            }
            heapam::scan::heap_scan_state(scan).rs_cindex += 1;
            continue;
        }

        let mut targtuple = produced.expect("analyze: normal item produced no tuple");

        let htsv = visibility::HeapTupleSatisfiesVacuum(&mut targtuple.tuple, oldest_xmin, cbuf)?;
        let decision = classify_analyze_result(htsv, &targtuple)?;

        *liverows += decision.live;
        *deadrows += decision.dead;

        if decision.sample_it {
            // ExecStoreBufferHeapTuple(targtuple, slot, hscan->rs_cbuf);
            slot_seam::exec_store_buffer_heap_tuple::call(targtuple, slot, cbuf)?;
            heapam::scan::heap_scan_state(scan).rs_cindex += 1;
            // Note that we leave the buffer locked here!
            return Ok(true);
        }

        heapam::scan::heap_scan_state(scan).rs_cindex += 1;
    }

    // Now release the lock and pin on the page.
    bufmgr_seam::unlock_release_buffer::call(cbuf);
    heapam::scan::heap_scan_state(scan).rs_cbuf = InvalidBuffer;

    // Also prevent old slot contents from having pin on page.
    slot_seam::exec_clear_tuple_payload::call(slot)?;

    Ok(false)
}

/// Classify a tuple's `HeapTupleSatisfiesVacuum` result into the sampling
/// decision + live/dead counter increments, faithfully matching the C `switch`
/// in `heapam_scan_analyze_next_tuple`.
fn classify_analyze_result(
    htsv: snapshot::snapshot::HTSV_Result,
    targtuple: &FormedTuple<'_>,
) -> PgResult<AnalyzeDecision> {
    use snapshot::snapshot::HTSV_Result::*;

    let hdr = targtuple
        .tuple
        .t_data
        .as_ref()
        .expect("analyze: tuple has no header");

    Ok(match htsv {
        HEAPTUPLE_LIVE => AnalyzeDecision {
            sample_it: true,
            live: 1.0,
            dead: 0.0,
        },
        // Count dead and recently-dead rows.
        HEAPTUPLE_DEAD | HEAPTUPLE_RECENTLY_DEAD => AnalyzeDecision {
            sample_it: false,
            live: 0.0,
            dead: 1.0,
        },
        HEAPTUPLE_INSERT_IN_PROGRESS => {
            // Insert-in-progress rows are not counted. We assume that when the
            // inserting transaction commits or aborts, it will send a stats
            // message to increment the proper count. A special case is that the
            // inserting transaction might be our own: count and sample the row,
            // to accommodate loading a table and analyzing it in one
            // transaction.
            if transam_xact_seams::transaction_id_is_current_transaction_id::call(
                visibility::htup::HeapTupleHeaderGetXmin(hdr),
            ) {
                AnalyzeDecision {
                    sample_it: true,
                    live: 1.0,
                    dead: 0.0,
                }
            } else {
                AnalyzeDecision {
                    sample_it: false,
                    live: 0.0,
                    dead: 0.0,
                }
            }
        }
        HEAPTUPLE_DELETE_IN_PROGRESS => {
            // We count and sample delete-in-progress rows the same as live ones,
            // so the stats counters come out right if the deleting transaction
            // commits after us. If the delete was done by our own transaction,
            // however, we must count the row as dead to make
            // pgstat_report_analyze's stats adjustments come out right.
            if transam_xact_seams::transaction_id_is_current_transaction_id::call(
                visibility::HeapTupleHeaderGetUpdateXid(hdr)?,
            ) {
                AnalyzeDecision {
                    sample_it: false,
                    live: 0.0,
                    dead: 1.0,
                }
            } else {
                AnalyzeDecision {
                    sample_it: true,
                    live: 1.0,
                    dead: 0.0,
                }
            }
        }
    })
}
