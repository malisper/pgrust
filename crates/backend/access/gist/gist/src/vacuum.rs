//! gistvacuum.c, collect arm: gistbulkdelete/gistvacuumscan driven by
//! validate_index's never-delete callback. Tuple deletion, empty-leaf-page
//! deletion, and deleted-page recycling stay loud (gistvacuum lane).
use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::types_core::{BlockNumber, ForkNumber, InvalidBlockNumber};
use ::types_error::PgResult;
use ::types_gist::{
    GistFollowRight, GistPageGetNSN, GistPageIsDeleted, GistPageIsLeaf, GIST_ROOT_BLKNO,
};
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::ItemPointerData;

use crate::util::{gistGetFakeLSN, gist_tuple_is_invalid, itup_get_tid, page_item, FirstOffsetNumber};

pub use ::nbtree::IndexVacuumInfo;

/// gistbulkdelete with C's collect-only callback shape (validate_index).
pub fn gistbulkdelete_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    let rel = info.index;
    let mut stats = IndexBulkDeleteResult::default();

    let start_nsn = if crate::relation_needs_wal(rel) {
        transam_xlog::GetInsertRecPtr()
    } else {
        gistGetFakeLSN(rel)?
    };

    // gistvacuumscan's physical-order outer loop; the relation length is
    // rechecked so leaf pages added by concurrent splits are visited.
    let mut current: BlockNumber = GIST_ROOT_BLKNO;
    let mut num_pages;
    loop {
        num_pages =
            bufmgr::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
        if current >= num_pages {
            break;
        }
        while current < num_pages {
            crate::check_for_interrupts();
            gistvacuumpage_collect(info, &mut stats, callback, current, start_nsn)?;
            current += 1;
        }
    }

    stats.num_pages = num_pages;
    Ok(stats)
}

// gistvacuumpage, collect arm. The rightlink recursion (page split after the
// scan started moved tuples to a lower-numbered page) is C's `goto restart`.
fn gistvacuumpage_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: &mut IndexBulkDeleteResult,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
    orig_blkno: BlockNumber,
    start_nsn: ::types_core::XLogRecPtr,
) -> PgResult<()> {
    let rel = info.index;
    let mut blkno = orig_blkno;
    loop {
        let mut recurse_to = InvalidBlockNumber;
        let pin = BufferPin::adopt(bufmgr::read_buffer_extended::call(
            rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            info.strategy.clone(),
        )?)
        .expect("ReadBufferExtended returned InvalidBuffer");
        bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_EXCLUSIVE)?;
        {
            let page = pin.page();
            if page.is_new() || GistPageIsDeleted(&page) {
                // gistPageRecyclable/GistPageIsDeleted arms: such pages only
                // exist once the loud vacuum lane has deleted pages.
                panic!("unported: gist deleted/new-page accounting (gistvacuum lane)");
            } else if GistPageIsLeaf(&page) {
                let opaque = ::types_gist::page_opaque(&page);
                if (GistFollowRight(&page) || start_nsn < GistPageGetNSN(&page))
                    && opaque.rightlink != InvalidBlockNumber
                    && opaque.rightlink < orig_blkno
                {
                    recurse_to = opaque.rightlink;
                }
                let maxoff = page.max_offset_number();
                for off in FirstOffsetNumber..=maxoff {
                    let itup = page_item(&page, off);
                    // SAFETY: page item under our exclusive content lock.
                    let tid = unsafe { itup_get_tid(itup) };
                    callback(&tid)?;
                }
                let nremain = maxoff as i64 - FirstOffsetNumber as i64 + 1;
                if nremain == 0 {
                    // Only the empty-leaf deletion stage (loud lane) consumes
                    // this; C records it and deletes after the scan.
                    panic!("unported: gist empty-leaf-page deletion (gistvacuum lane)");
                }
                stats.num_index_tuples += nremain as f64;
            } else {
                for off in FirstOffsetNumber..=page.max_offset_number() {
                    let itup = page_item(&page, off);
                    // SAFETY: page item under our exclusive content lock.
                    if unsafe { gist_tuple_is_invalid(itup) } {
                        panic!(
                            "index \"{}\" contains an inner tuple marked as invalid",
                            rel.name()
                        );
                    }
                }
            }
        }
        bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
        drop(pin);
        if recurse_to == InvalidBlockNumber {
            return Ok(());
        }
        blkno = recurse_to;
    }
}
