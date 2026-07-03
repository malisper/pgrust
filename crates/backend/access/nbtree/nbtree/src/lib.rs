//! B-tree access method (nbtree.c/nbtsearch.c/nbtinsert.c/nbtpage.c/
//! nbtutils.c/nbtpreprocesskeys.c): read path, insert/split, and the VACUUM
//! lane (bulkdelete/cleanup + page deletion). Phase 2, loud panics, never
//! silent: dedup, parallel scans, SAOP arrays, skip scan, row comparisons,
//! mark/restore across primitive scans.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(irrefutable_let_patterns)]

mod dedup;
mod delete;
mod fcframe;
mod insert;
pub mod itup;
mod page;
mod pagedel;
mod preprocess;
mod search;
mod splitloc;
mod utils;
pub mod vacuum;
mod wal;

#[cfg(test)]
mod tests;

pub use insert::btinsert;
pub use page::{bt_getrootheight, bt_initmetapage, bt_metaversion, bt_pageinit};
pub use vacuum::{btbulkdelete, btvacuumcleanup, IndexVacuumInfo};

use ::mcx::Mcx;
use ::types_core::{BLCKSZ, InvalidSubTransactionId};
use ::types_error::PgResult;
use ::types_nbtree::{BTScanOpaqueData, BTScanPosInvalidate, BTScanPosIsPinned, BTScanPosIsValid};
use ::types_rel::Relation;
use ::types_relscan::{relation_get_index_scan, IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::ScanDirection;
use ::types_snapshot::IsMVCCSnapshot;

use search::{bt_first, bt_gettuple_continue, pos_unpin_if_pinned, restore_scanpos, ScanCtx};
use utils::bt_killitems;

pub use search::BtScanInsert;
pub use utils::{bt_check_third_page, bt_keep_natts_fast, bt_mkscankey, bt_truncate};
pub use fcframe::OrderProcFrame;

#[cold]
#[inline(never)]
pub(crate) fn unported_phase2(what: &str) -> ! {
    panic!("unported: nbtree {what} is phase 2")
}

#[cold]
#[inline(never)]
fn non_btree_opaque() -> ! {
    panic!("nbtree entry point reached with a non-btree scan opaque")
}

pub(crate) fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        unported_phase2("ProcessInterrupts (tcop/postgres.c)");
    }
}

macro_rules! split_scan {
    ($scan:expr) => {{
        let IndexScanDescData {
            indexRelation,
            xs_snapshot,
            keyData,
            ignore_killed_tuples,
            xs_heaptid,
            xs_itup,
            xs_pgstat_index_scans,
            xs_nsearches,
            opaque,
            ..
        } = $scan;
        let IndexScanOpaque::Btree(so) = opaque else {
            non_btree_opaque()
        };
        ScanCtx {
            rel: indexRelation,
            so: &mut **so,
            snapshot: xs_snapshot.as_deref(),
            ignore_killed_tuples: *ignore_killed_tuples,
            input_keys: keyData.as_mut_slice(),
            xs_heaptid,
            xs_itup,
            xs_pgstat_index_scans,
            xs_nsearches,
            frame: crate::fcframe::OrderProcFrame::new(),
        }
    }};
}

/// btbeginscan.
pub fn btbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    debug_assert!(norderbys == 0);
    let so = BTScanOpaqueData::alloc_in(mcx)?;
    let mut scan = relation_get_index_scan(
        mcx,
        rel,
        nkeys,
        norderbys,
        IndexScanOpaque::Btree(so),
        xact::TransactionStartedDuringRecovery(),
    )?;
    scan.xs_itupdesc = Some(rel.rd_att.clone());
    Ok(scan)
}

// RelationNeedsWAL (rel.h); XLogIsNeeded ≡ the xlog_standby_info_active seam.
pub(crate) fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == InvalidSubTransactionId))
}

/// btrescan. `scankey: None` restarts with the keys already in scan.keyData.
pub fn btrescan(
    scan: &mut IndexScanDescData<'_>,
    scankey: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    {
        let IndexScanOpaque::Btree(so) = &mut scan.opaque else {
            non_btree_opaque()
        };
        if BTScanPosIsValid(&so.currPos) {
            if so.numKilled > 0 {
                bt_killitems(&scan.indexRelation, so)?;
            }
            pos_unpin_if_pinned(&mut so.currPos)?;
            BTScanPosInvalidate(&mut so.currPos);
        }

        // off for index-only scans, non-MVCC snapshots, unlogged, bitmap.
        so.dropPin = !scan.xs_want_itup
            && scan.xs_snapshot.as_deref().is_some_and(IsMVCCSnapshot)
            && relation_needs_wal(&scan.indexRelation)
            && scan.heapRelation.is_some();

        so.markItemIndex = -1;
        so.needPrimScan = false;
        so.scanBehind = false;
        so.oppositeDirCheck = false;
        pos_unpin_if_pinned(&mut so.markPos)?;
        BTScanPosInvalidate(&mut so.markPos);

        if scan.xs_want_itup && so.currTuples.is_none() {
            let mcx = *so.keyData.allocator();
            so.currTuples = Some(::mcx::vec_with_capacity_in(mcx, BLCKSZ)?);
            so.markTuples = Some(::mcx::vec_with_capacity_in(mcx, BLCKSZ)?);
        }

        so.numberOfKeys = 0; // until _bt_preprocess_keys sets it
        so.numArrayKeys = 0; // ditto
    }

    if let Some(keys) = scankey {
        if scan.numberOfKeys > 0 {
            debug_assert!(keys.len() == scan.numberOfKeys as usize);
            scan.keyData.clear();
            scan.keyData.extend(keys.iter().cloned());
        }
    }
    Ok(())
}

/// btgettuple.
pub fn btgettuple(scan: &mut IndexScanDescData<'_>, dir: ScanDirection) -> PgResult<bool> {
    debug_assert!(scan.heapRelation.is_some());

    scan.xs_recheck = false;

    let kill_prior_tuple = scan.kill_prior_tuple;
    let mut ctx = split_scan!(&mut *scan);

    let res = if !BTScanPosIsValid(&ctx.so.currPos) {
        bt_first(&mut ctx, dir)?
    } else {
        bt_gettuple_continue(&mut ctx, dir, kill_prior_tuple)?
    };
    if !res && ctx.so.numArrayKeys != 0 {
        unported_phase2("_bt_start_prim_scan (SAOP/skip-scan lane)");
    }
    Ok(res)
}

/// btgetbitmap: drain all matching heap TIDs into `tbm`, forward only.
pub fn btgetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    debug_assert!(scan.heapRelation.is_none());
    let mut ntids: i64 = 0;

    let mut ctx = split_scan!(&mut *scan);
    if bt_first(&mut ctx, ::types_scan::sdir::ForwardScanDirection)? {
        tbm.add_tuples(core::slice::from_ref(ctx.xs_heaptid), false)?;
        ntids += 1;

        loop {
            ctx.so.currPos.itemIndex += 1;
            if ctx.so.currPos.itemIndex > ctx.so.currPos.lastItem {
                if !search::bt_next(&mut ctx, ::types_scan::sdir::ForwardScanDirection)? {
                    break;
                }
            }
            // SAFETY: itemIndex in [firstItem, lastItem], written by bt_readpage.
            let item = unsafe { ctx.so.currPos.item(ctx.so.currPos.itemIndex as usize) };
            tbm.add_tuples(core::slice::from_ref(&item.heapTid), false)?;
            ntids += 1;
        }
    }
    if ctx.so.numArrayKeys != 0 {
        unported_phase2("_bt_start_prim_scan (SAOP/skip-scan lane)");
    }
    Ok(ntids)
}

/// btendscan. Storage is freed with the scan value (mcx lifetime).
pub fn btendscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Btree(so) = &mut scan.opaque else {
        non_btree_opaque()
    };

    if BTScanPosIsValid(&so.currPos) {
        if so.numKilled > 0 {
            bt_killitems(&scan.indexRelation, so)?;
        }
        pos_unpin_if_pinned(&mut so.currPos)?;
    }

    so.markItemIndex = -1;
    pos_unpin_if_pinned(&mut so.markPos)?;
    Ok(())
}

/// btmarkpos.
pub fn btmarkpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Btree(so) = &mut scan.opaque else {
        non_btree_opaque()
    };

    pos_unpin_if_pinned(&mut so.markPos)?;

    // the scan leaves the page before the mark is moved.
    if BTScanPosIsValid(&so.currPos) {
        so.markItemIndex = so.currPos.itemIndex;
    } else {
        BTScanPosInvalidate(&mut so.markPos);
        so.markItemIndex = -1;
    }
    Ok(())
}

/// btrestrpos.
pub fn btrestrpos(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let IndexScanOpaque::Btree(so) = &mut scan.opaque else {
        non_btree_opaque()
    };

    if so.markItemIndex >= 0 {
        so.currPos.itemIndex = so.markItemIndex;
        return Ok(());
    }

    if BTScanPosIsValid(&so.currPos) {
        if so.numKilled > 0 {
            bt_killitems(&scan.indexRelation, so)?;
        }
        pos_unpin_if_pinned(&mut so.currPos)?;
    }

    if BTScanPosIsValid(&so.markPos) {
        if BTScanPosIsPinned(&so.markPos) {
            bufmgr_seams::incr_buffer_ref_count::call(so.markPos.buf);
        }
        restore_scanpos(so);
        if so.numArrayKeys != 0 {
            unported_phase2("mark/restore with array keys (_bt_start_array_keys)");
        }
    } else {
        BTScanPosInvalidate(&mut so.currPos);
    }
    Ok(())
}

#[cfg(feature = "bench-internals")]
pub mod bench_internals {
    pub use crate::fcframe::OrderProcFrame;
    pub use crate::page::{bt_relbuf, page_item, page_opaque};
    pub use crate::search::{bt_binsrch, bt_compare, bt_search, order_procinfo};
}
