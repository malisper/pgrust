//! Hash index AM (hash.c / hashinsert.c / hashpage.c / hashovfl.c /
//! hashsearch.c / hashutil.c). Loud, never silent: reloptions, parallel
//! anything.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub(crate) mod insert;
pub mod ovfl;
pub(crate) mod page;
pub(crate) mod search;
pub mod util;
pub(crate) mod wal;

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, OffsetNumber};
use ::types_error::PgResult;
use ::types_hash::*;
use ::types_rel::Relation;
use ::types_relscan::{relation_get_index_scan, IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::ScanDirection;
use ::types_storage::buf::BufferAccessStrategy;
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_tuple::itemptr::ItemPointerData;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_NO_CHANGE, REGBUF_NO_IMAGE, REGBUF_STANDARD};

use bufmgr_seams as bm;
use page::{page_mut, page_opaque, page_ref, write_opaque};
use search::HashScanCtx;

pub use ::nbtree::IndexVacuumInfo;

pub use insert::_hash_doinsert;
pub use page::_hash_init;
pub use util::{_hash_convert_tuple, hash_procinfo};

pub(crate) const RM_HASH: u8 = ::types_core::primitive::RmgrIds::RM_HASH_ID as u8;

#[cold]
#[inline(never)]
fn non_hash_opaque() -> ! {
    panic!("hash entry point reached with a non-hash scan opaque")
}

pub(crate) fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// RelationNeedsWAL (rel.h), as nbtree renders it.
pub(crate) fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == ::types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == ::types_core::InvalidSubTransactionId))
}

macro_rules! split_scan {
    ($scan:expr) => {{
        let IndexScanDescData {
            indexRelation,
            xs_snapshot,
            keyData,
            ignore_killed_tuples,
            xs_heaptid,
            xs_pgstat_index_scans,
            xs_nsearches,
            opaque,
            ..
        } = $scan;
        let IndexScanOpaque::Hash(so) = opaque else {
            non_hash_opaque()
        };
        HashScanCtx {
            rel: indexRelation.as_ref().expect("index scan parked (skeleton)"),
            so: &mut **so,
            snapshot: xs_snapshot.as_deref(),
            ignore_killed_tuples: *ignore_killed_tuples,
            keys: keyData.as_slice(),
            xs_heaptid,
            xs_pgstat_index_scans,
            xs_nsearches,
        }
    }};
}

/// hashinsert.
pub fn hashinsert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    heapRel: &Relation<'mcx>,
) -> PgResult<bool> {
    let Some(hash_datum) = _hash_convert_tuple(rel, values, isnull)? else {
        return Ok(false);
    };

    let mut itup =
        nbtree::itup::index_form_tuple(mcx, &rel.rd_att, &[hash_datum], &[false])?;
    // SAFETY: t_tid = first 6 bytes of the owned image (itup.h).
    unsafe {
        itup.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(*ht_ctid);
    }
    // SAFETY: itup.size() bytes of the live owned image.
    let image = unsafe { core::slice::from_raw_parts(itup.as_ptr(), itup.size()) };
    _hash_doinsert(rel, image, heapRel, false)?;

    Ok(false)
}

/// hashgettuple.
pub fn hashgettuple(scan: &mut IndexScanDescData<'_>, dir: ScanDirection) -> PgResult<bool> {
    // Hash indexes are always lossy (only the hash code is stored).
    scan.xs_recheck = true;

    let kill_prior_tuple = scan.kill_prior_tuple;
    let mut ctx = split_scan!(&mut *scan);

    if !HashScanPosIsValid(&ctx.so.currPos) {
        search::_hash_first(&mut ctx, dir)
    } else {
        if kill_prior_tuple {
            if ctx.so.killedItems.capacity() == 0 {
                ctx.so.killedItems.try_reserve_exact(MaxIndexTuplesPerPage).map_err(|_| {
                    Box::new(::types_error::PgError::error("out of memory"))
                })?;
            }
            if (ctx.so.numKilled as usize) < MaxIndexTuplesPerPage {
                // C's killedItems[numKilled++] overwrite (see nbtree twin):
                // _hash_kill_items resets numKilled without truncating.
                let n = ctx.so.numKilled as usize;
                if n < ctx.so.killedItems.len() {
                    ctx.so.killedItems[n] = ctx.so.currPos.itemIndex;
                } else {
                    ctx.so.killedItems.push(ctx.so.currPos.itemIndex);
                }
                ctx.so.numKilled += 1;
            }
        }
        search::_hash_next(&mut ctx, dir)
    }
}

/// hashgetbitmap.
pub fn hashgetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    let mut ntids: i64 = 0;
    let mut ctx = split_scan!(&mut *scan);

    let mut res = search::_hash_first(&mut ctx, ::types_scan::sdir::ForwardScanDirection)?;
    while res {
        // SAFETY: itemIndex within the range readpage loaded.
        let curr_item = unsafe { ctx.so.currPos.item(ctx.so.currPos.itemIndex as usize) };
        // _hash_first/_hash_next eliminate killed tuples when
        // ignore_killed_tuples; everything here is (lossily) recheck-true.
        tbm.add_tuples(core::slice::from_ref(&curr_item.heapTid), true)?;
        ntids += 1;
        res = search::_hash_next(&mut ctx, ::types_scan::sdir::ForwardScanDirection)?;
    }

    Ok(ntids)
}

/// hashbeginscan.
pub fn hashbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDescData<'mcx>> {
    debug_assert!(norderbys == 0);
    let so = HashScanOpaqueData::alloc_in(mcx)?;
    relation_get_index_scan(
        mcx,
        rel,
        nkeys,
        norderbys,
        IndexScanOpaque::Hash(so),
        xact::TransactionStartedDuringRecovery(),
    )
}

/// hashrescan. `scankey: None` restarts with the keys already in keyData.
pub fn hashrescan(
    scan: &mut IndexScanDescData<'_>,
    scankey: Option<&[ScanKeyData]>,
) -> PgResult<()> {
    {
        let mut ctx = split_scan!(&mut *scan);
        if HashScanPosIsValid(&ctx.so.currPos) {
            if ctx.so.numKilled > 0 {
                search::_hash_kill_items(&mut ctx)?;
            }
        }
        page::_hash_dropscanbuf(ctx.so)?;
        HashScanPosInvalidate(&mut ctx.so.currPos);
        ctx.so.hashso_buc_populated = false;
        ctx.so.hashso_buc_split = false;
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

/// hashendscan. Storage is freed with the scan value (mcx lifetime).
pub fn hashendscan(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let mut ctx = split_scan!(&mut *scan);
    if HashScanPosIsValid(&ctx.so.currPos) {
        if ctx.so.numKilled > 0 {
            search::_hash_kill_items(&mut ctx)?;
        }
    }
    page::_hash_dropscanbuf(ctx.so)?;
    Ok(())
}

// The bulkdelete callback, monomorphized to its two producers: vac_tid_reaped
// (sorted dead-TID slice) and validate_index's never-delete collector.
pub enum HashVacDelete<'a> {
    DeadItems(&'a [ItemPointerData]),
    Collect(&'a mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + 'a)),
}

// vac_tid_reaped over the sorted dead-TID image.
fn tid_is_dead(dead_items: &[ItemPointerData], tid: &ItemPointerData) -> bool {
    dead_items
        .binary_search_by(|probe| ::types_tuple::itemptr::ItemPointerCompare(probe, tid).cmp(&0))
        .is_ok()
}

fn vacuum_delay_point() -> PgResult<()> {
    check_for_interrupts()?;
    if init_small::globals::VacuumCostActive() {
        vacuum_seams::vacuum_delay_point::call(false)?;
    }
    Ok(())
}

/// hashbulkdelete.
pub fn hashbulkdelete<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    hashbulkdelete_guts(info, stats, &mut HashVacDelete::DeadItems(dead_items))
}

/// hashbulkdelete with C's collect-only callback shape (validate_index).
pub fn hashbulkdelete_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    hashbulkdelete_guts(info, None, &mut HashVacDelete::Collect(callback))
}

fn hashbulkdelete_guts<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    delete: &mut HashVacDelete<'_>,
) -> PgResult<IndexBulkDeleteResult> {
    let rel = info.index;
    let mut tuples_removed = 0.0f64;
    let mut num_index_tuples = 0.0f64;

    // A cached copy of the metapage is good enough for bucket addressing;
    // staleness is detected and refreshed below.
    let mut metabuf = InvalidBuffer;
    page::_hash_getcachedmetap(rel, &mut metabuf, false)?;
    let (orig_maxbucket, orig_ntuples) =
        page::with_cached_metap(rel, |m| (m.hashm_maxbucket, m.hashm_ntuples));

    let mut cur_bucket: Bucket = 0;
    let mut cur_maxbucket = orig_maxbucket;

    'loop_top: loop {
        while cur_bucket <= cur_maxbucket {
            let bucket_blkno = page::with_cached_metap(rel, |m| m.bucket_to_blkno(cur_bucket));
            let blkno = bucket_blkno;

            // Cleanup lock on the primary bucket page to out-wait concurrent
            // scans before deleting the dead tuples.
            let buf = bm::read_buffer_extended::call(
                rel,
                ForkNumber::MAIN_FORKNUM,
                blkno,
                ::types_storage::ReadBufferMode::Normal,
                info.strategy.clone(),
            )?;
            bm::lock_buffer_for_cleanup::call(buf)?;
            util::_hash_checkpage(rel, buf, LH_BUCKET_PAGE)?;

            // Tuples moved by a split can only be deleted once the split has
            // finished; scans need them until then.
            let mut split_cleanup = false;
            {
                // SAFETY: cleanup lock held.
                let opaque = page_opaque(&unsafe { page_ref(buf) });
                if !H_BUCKET_BEING_SPLIT(opaque.hasho_flag)
                    && H_NEEDS_SPLIT_CLEANUP(opaque.hasho_flag)
                {
                    split_cleanup = true;

                    // The bucket may have been split since the cached
                    // metapage was read; with the primary page locked (no
                    // further splits possible), refresh if stale.
                    debug_assert!(opaque.hasho_prevblkno != InvalidBlockNumber);
                    if opaque.hasho_prevblkno
                        > page::with_cached_metap(rel, |m| m.hashm_maxbucket)
                    {
                        page::_hash_getcachedmetap(rel, &mut metabuf, true)?;
                    }
                }
            }

            let bucket_buf = buf;
            let (maxbucket, highmask, lowmask) = page::with_cached_metap(rel, |m| {
                (m.hashm_maxbucket, m.hashm_highmask, m.hashm_lowmask)
            });

            hashbucketcleanup(
                rel,
                cur_bucket,
                bucket_buf,
                blkno,
                info.strategy.clone(),
                maxbucket,
                highmask,
                lowmask,
                Some(&mut tuples_removed),
                Some(&mut num_index_tuples),
                split_cleanup,
                Some(delete),
            )?;

            page::_hash_dropbuf(bucket_buf)?;

            cur_bucket += 1;
        }

        if metabuf == InvalidBuffer {
            metabuf = page::_hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, LH_META_PAGE)?;
        }

        // Write-lock metapage and check for a split since we started.
        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;
        if cur_maxbucket != page::with_meta(metabuf, |m| m.hashm_maxbucket) {
            bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;
            page::_hash_getcachedmetap(rel, &mut metabuf, true)?;
            cur_maxbucket = page::with_cached_metap(rel, |m| m.hashm_maxbucket);
            continue 'loop_top;
        }
        break;
    }

    // Update phase (C's critical section).
    let meta_ntuples = page::with_meta_mut(metabuf, |metap| {
        if orig_maxbucket == metap.hashm_maxbucket && orig_ntuples == metap.hashm_ntuples {
            // No split or insert since the scan started: our count is gospel.
            metap.hashm_ntuples = num_index_tuples;
        } else {
            // Split buckets may have been double-scanned; dead-reckon (still
            // estimated_count = false, better than not updating reltuples).
            if metap.hashm_ntuples > tuples_removed {
                metap.hashm_ntuples -= tuples_removed;
            } else {
                metap.hashm_ntuples = 0.0;
            }
            num_index_tuples = metap.hashm_ntuples;
        }
        metap.hashm_ntuples
    });

    bm::mark_buffer_dirty::call(metabuf)?;

    if relation_needs_wal(rel) {
        let xlrec = wal::xl_hash_update_meta_page(meta_ntuples);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_UPDATE_META_PAGE,
            0,
            &[&xlrec],
            &[XLogRegBuf {
                block_id: 0,
                buffer: metabuf,
                flags: REGBUF_STANDARD,
                bufdata: &[],
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(metabuf) }.set_lsn(recptr);
    }

    page::_hash_relbuf(metabuf)?;

    let mut stats = stats.unwrap_or_default();
    stats.estimated_count = false;
    stats.num_index_tuples = num_index_tuples;
    stats.tuples_removed += tuples_removed;
    // hashvacuumcleanup fills in num_pages.
    Ok(stats)
}

/// hashvacuumcleanup. None stats (the ANALYZE-only call, or bulkdelete never
/// ran) returns None signifying no change.
pub fn hashvacuumcleanup<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let Some(mut stats) = stats else {
        return Ok(None);
    };
    stats.num_pages =
        bm::relation_get_number_of_blocks_in_fork::call(info.index, ForkNumber::MAIN_FORKNUM)?;
    Ok(Some(stats))
}

/// hashbucketcleanup.
pub(crate) fn hashbucketcleanup(
    rel: &Relation<'_>,
    cur_bucket: Bucket,
    bucket_buf: Buffer,
    bucket_blkno: BlockNumber,
    bstrategy: BufferAccessStrategy,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
    mut tuples_removed: Option<&mut f64>,
    mut num_index_tuples: Option<&mut f64>,
    split_cleanup: bool,
    mut callback: Option<&mut HashVacDelete<'_>>,
) -> PgResult<()> {
    let mut blkno = bucket_blkno;
    let mut buf = bucket_buf;
    let mut bucket_dirty = false;

    let new_bucket = if split_cleanup {
        util::_hash_get_newbucket_from_oldbucket(cur_bucket, lowmask, maxbucket)
    } else {
        InvalidBucket
    };

    loop {
        let mut deletable = [0 as OffsetNumber; MaxIndexTuplesPerPage];
        let mut ndeletable = 0usize;
        let mut retain_pin = false;
        let mut clear_dead_marking = false;

        vacuum_delay_point()?;

        // SAFETY: lock held on buf (cleanup on entry, write along the chain).
        let (maxoffno, mut opaque) = {
            let page = unsafe { page_ref(buf) };
            (page.max_offset_number(), page_opaque(&page))
        };

        for offno in 1..=maxoffno {
            // SAFETY: bounded offset under the buf lock.
            let (hashkey, htup) = unsafe {
                let page = page_ref(buf);
                let id = page.item_id(offno);
                let itup = page.item_raw(id).0;
                (
                    util::_hash_get_indextuple_hashkey(itup),
                    nbtree::itup::t_tid(itup),
                )
            };

            // Dead-tuple removal relies strictly on the callback's verdict
            // (see btvacuumpage).
            let mut kill_tuple = false;
            let callback_dead = match callback.as_deref_mut() {
                Some(HashVacDelete::DeadItems(dead_items)) => tid_is_dead(dead_items, &htup),
                Some(HashVacDelete::Collect(collect)) => {
                    collect(&htup)?;
                    false
                }
                None => false,
            };
            if callback_dead {
                kill_tuple = true;
                if let Some(t) = tuples_removed.as_deref_mut() {
                    *t += 1.0;
                }
            } else if split_cleanup {
                // Delete the tuples that were moved by split; they belong to
                // cur_bucket or new_bucket only (no further splits from a
                // bucket containing garbage, per _hash_expandtable).
                let bucket = util::_hash_hashkey2bucket(hashkey, maxbucket, highmask, lowmask);
                if bucket != cur_bucket {
                    debug_assert!(bucket == new_bucket);
                    kill_tuple = true;
                }
            }

            if kill_tuple {
                deletable[ndeletable] = offno;
                ndeletable += 1;
            } else if let Some(n) = num_index_tuples.as_deref_mut() {
                *n += 1.0;
            }
        }

        if blkno == bucket_blkno {
            retain_pin = true;
        }

        blkno = opaque.hasho_nextblkno;

        if ndeletable > 0 {
            {
                // SAFETY: lock held on buf.
                let mut page = unsafe { page_mut(buf) };
                page.index_multi_delete(&deletable[..ndeletable]);
            }
            bucket_dirty = true;

            let removed_any = tuples_removed.as_deref().map_or(false, |t| *t > 0.0);
            if removed_any && H_HAS_DEAD_TUPLES(opaque.hasho_flag) {
                // SAFETY: lock held on buf.
                let mut page = unsafe { page_mut(buf) };
                opaque.hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
                write_opaque(&mut page, &opaque);
                clear_dead_marking = true;
            }

            bm::mark_buffer_dirty::call(buf)?;

            if relation_needs_wal(rel) {
                let is_primary = buf == bucket_buf;
                let xlrec = wal::xl_hash_delete(clear_dead_marking, is_primary);
                let mut delbytes: Vec<u8> = Vec::with_capacity(ndeletable * 2);
                for &off in &deletable[..ndeletable] {
                    delbytes.extend_from_slice(&off.to_ne_bytes());
                }

                let del_data: [&[u8]; 1] = [&delbytes];
                let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(2);
                if !is_primary {
                    bufs.push(XLogRegBuf {
                        block_id: 0,
                        buffer: bucket_buf,
                        flags: REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE,
                        bufdata: &[],
                    });
                }
                bufs.push(XLogRegBuf {
                    block_id: 1,
                    buffer: buf,
                    flags: REGBUF_STANDARD,
                    bufdata: &del_data,
                });

                let recptr = ::xloginsert_seams::xlog_insert_record::call(
                    RM_HASH,
                    XLOG_HASH_DELETE,
                    0,
                    &[&xlrec],
                    &bufs,
                )?;
                // SAFETY: pin + lock held.
                unsafe { page_mut(buf) }.set_lsn(recptr);
            }
        }

        if blkno == InvalidBlockNumber {
            break;
        }

        let next_buf = page::_hash_getbuf_with_strategy(
            rel,
            blkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE,
            bstrategy.clone(),
        )?;

        if retain_pin {
            bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
        } else {
            page::_hash_relbuf(buf)?;
        }

        buf = next_buf;
    }

    if buf != bucket_buf {
        page::_hash_relbuf(buf)?;
        bm::lock_buffer::call(bucket_buf, bm::BUFFER_LOCK_EXCLUSIVE)?;
    }

    if split_cleanup {
        {
            // SAFETY: exclusive lock held on the primary bucket page.
            let mut page = unsafe { page_mut(bucket_buf) };
            let mut bucket_opaque = page_opaque(&page.as_ref());
            bucket_opaque.hasho_flag &= !LH_BUCKET_NEEDS_SPLIT_CLEANUP;
            write_opaque(&mut page, &bucket_opaque);
        }
        bm::mark_buffer_dirty::call(bucket_buf)?;

        if relation_needs_wal(rel) {
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RM_HASH,
                XLOG_HASH_SPLIT_CLEANUP,
                0,
                &[],
                &[XLogRegBuf {
                    block_id: 0,
                    buffer: bucket_buf,
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                }],
            )?;
            // SAFETY: pin + exclusive lock held.
            unsafe { page_mut(bucket_buf) }.set_lsn(recptr);
        }
    }

    if bucket_dirty && bufmgr::IsBufferCleanupOK(bucket_buf) {
        ovfl::_hash_squeezebucket(rel, cur_bucket, bucket_blkno, bucket_buf, bstrategy)?;
    } else {
        bm::lock_buffer::call(bucket_buf, bm::BUFFER_LOCK_UNLOCK)?;
    }
    Ok(())
}
