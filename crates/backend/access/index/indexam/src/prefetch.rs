//! Heap readahead for plain btree index scans: the distance-controlled half of
//! upstream's index-prefetching series (CF 4351) over the landed heap-batch
//! machinery. Advisory fadvise only — never changes what the scan reads.

use types_core::{BlockNumber, ForkNumber, InvalidBlockNumber};
use types_error::PgResult;
use types_nbtree::{BTScanOpaqueData, BTScanPosIsValid};
use types_rel::RelationData;
use types_relscan::IndexPrefetchState;
use types_scan::sdir::ScanDirectionIsForward;
use types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck;

use crate::{IndexScanDescData, IndexScanOpaque};

// Upstream v28's start rule: engage on the 4th distinct-heap-block switch.
const ACTIVATE_SWITCHES: u16 = 4;
const INIT_DISTANCE: u16 = 2;
// Long all-cached probe streak: go dormant, re-arm only after fresh real reads.
const DEACTIVATE_STREAK: u16 = 128;

fn eff_io_concurrency() -> i32 {
    let v = &guc_tables::vars::effective_io_concurrency;
    if v.installed() {
        v.read()
    } else {
        16
    }
}

#[inline]
pub(crate) fn on_heap_fetch(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let blk = ItemPointerGetBlockNumberNoCheck(&scan.xs_heaptid);
    if blk == scan.xs_prefetch.last_block {
        return Ok(());
    }
    block_switch(scan, blk)
}

#[inline(never)]
fn block_switch(scan: &mut IndexScanDescData<'_>, blk: BlockNumber) -> PgResult<()> {
    let IndexScanDescData {
        xs_prefetch: pf,
        heapRelation,
        opaque,
        xs_want_itup,
        ..
    } = scan;
    pf.last_block = blk;
    if !pf.active {
        if pf.switches == 0 {
            pf.read_base = bufmgr::counters::shared_blks_read();
        }
        pf.switches = pf.switches.saturating_add(1);
        // Arm only once the scan has done a real read (warm scans stay here)
        // and the block-switch count clears the start rule.
        if pf.switches < ACTIVATE_SWITCHES
            || bufmgr::counters::shared_blks_read() == pf.read_base
        {
            return Ok(());
        }
        if eff_io_concurrency() <= 0 {
            pf.switches = 0;
            return Ok(());
        }
        pf.active = true;
        pf.distance = INIT_DISTANCE;
        pf.inflight = 0;
        pf.cached_streak = 0;
        pf.leaf_page = InvalidBlockNumber;
    } else {
        pf.inflight = pf.inflight.saturating_sub(1);
    }
    if *xs_want_itup {
        // Index-only scans: most TIDs never reach a heap fetch — advising
        // them is upstream's v27 adversarial regression. Stay per-tuple.
        pf.active = false;
        return Ok(());
    }
    let Some(rel) = heapRelation.as_ref() else {
        pf.active = false;
        return Ok(());
    };
    let IndexScanOpaque::Btree(so) = &*opaque else {
        pf.active = false;
        return Ok(());
    };
    issue(pf, rel, so, blk)
}

/// Walk currPos beyond the issuance cursor, keeping `distance` distinct heap
/// blocks of lookahead advised. Ascending-sequential runs are counted but not
/// advised (kernel readahead owns them — upstream's 500%-regression fix);
/// adjacent duplicates collapse (dedup is efficiency, not correctness).
fn issue(
    pf: &mut IndexPrefetchState,
    rel: &RelationData<'_>,
    so: &BTScanOpaqueData<'_>,
    cur_blk: BlockNumber,
) -> PgResult<()> {
    let pos = &so.currPos;
    if !BTScanPosIsValid(pos) || pos.itemIndex < pos.firstItem || pos.itemIndex > pos.lastItem {
        return Ok(());
    }
    let forward = ScanDirectionIsForward(pos.dir);
    let step: i32 = if forward { 1 } else { -1 };
    if pf.leaf_page != pos.currPage {
        pf.leaf_page = pos.currPage;
        pf.issued_idx = pos.itemIndex + step;
    } else if forward {
        pf.issued_idx = pf.issued_idx.max(pos.itemIndex + 1);
    } else {
        pf.issued_idx = pf.issued_idx.min(pos.itemIndex - 1);
    }
    let cap = (eff_io_concurrency().clamp(1, 64) as u16) * 4;
    let mut j = pf.issued_idx;
    let mut prev_seen = cur_blk;
    while pf.inflight < pf.distance && j >= pos.firstItem && j <= pos.lastItem {
        // SAFETY: j within [firstItem, lastItem] (loop bound): written slot.
        let b = ItemPointerGetBlockNumberNoCheck(&unsafe { pos.item(j as usize) }.heapTid);
        j += step;
        if b == prev_seen {
            continue;
        }
        prev_seen = b;
        if b == pf.last_issued {
            continue;
        }
        // Near-sequential window, not stride-1: a pool-DIO prefetch inside a
        // sequential run punches a hole in the buffered-fd read stream and
        // collapses kernel readahead for the whole run (m2cold correlated
        // 245ms -> 1368ms with stride-1 only). 16 blocks = io_combine_limit
        // (128kB), inside one kernel readahead window.
        if (1..=16).contains(&b.wrapping_sub(pf.last_issued)) {
            pf.last_issued = b;
            pf.inflight += 1;
            continue;
        }
        match bufmgr::PrefetchBuffer(rel, ForkNumber::MAIN_FORKNUM, b)? {
            bufmgr::PrefetchOutcome::Cached => {
                pf.cached_streak = pf.cached_streak.saturating_add(1);
                if pf.distance > 1 {
                    pf.distance -= 1;
                }
                if pf.cached_streak >= DEACTIVATE_STREAK {
                    pf.active = false;
                    pf.switches = 0;
                    pf.cached_streak = 0;
                    pf.issued_idx = j;
                    return Ok(());
                }
            }
            bufmgr::PrefetchOutcome::Issued => {
                pf.cached_streak = 0;
                pf.distance = (pf.distance * 2).min(cap);
            }
            bufmgr::PrefetchOutcome::Skipped => {
                pf.active = false;
                pf.issued_idx = j;
                return Ok(());
            }
        }
        pf.last_issued = b;
        pf.inflight += 1;
    }
    pf.issued_idx = j;
    Ok(())
}
