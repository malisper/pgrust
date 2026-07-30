// nbtsort.c, serial build: spool via tuplesort + _bt_load page accumulation,
// duplicate keys merged into posting lists. Loud: launched-gang parallel
// builds (never ported — see pool.rs census); the M4.2 pool arm (pool.rs,
// kill switch PGRUST_RUNTIME_INDEXBUILD_POOL default OFF) parallelizes the
// SCAN phase onto morsel-pool workers and feeds the same leader-owned
// spools, so the sort + load tail below is one code path for both arms.
#![allow(non_snake_case)]

mod pool;

use ::mcx::Mcx;
use ::types_core::{BlockNumber, ForkNumber, InvalidOid, OffsetNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_nbtree::{
    BTMaxItemSize, BTPageOpaqueData, BTP_LEAF, BTP_ROOT, BTREE_DEFAULT_FILLFACTOR,
    BTREE_NONLEAF_FILLFACTOR, P_FIRSTKEY, P_HIKEY,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{ItemIdData, PageMut, SizeOfPageHeaderData};
use ::types_nbtree::dedup::BTDedupState;
use ::types_tuple::itemptr::{ItemPointerData, InvalidOffsetNumber};
use bulkwrite::{BulkWriteBuffer, BulkWriteState};
use execindexing::IndexInfo;
use nbtree::itup::{
    self, index_tuple_size, maxalign, set_t_info, set_t_tid, ITup, ItupBuf,
    INDEX_SIZE_MASK, INDEX_TUPLE_HEADER_SIZE,
};
use nbtree::{BtScanInsert, OrderProcFrame};

const P_NONE: BlockNumber = 0;
const BTREE_METAPAGE: BlockNumber = 0;
const BTEQUALIMAGE_PROC: i16 = 4;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: nbtsort {what}")
}

struct BTPageState<'mcx> {
    buf: BulkWriteBuffer,
    blkno: BlockNumber,
    lowkey: Option<ItupBuf<'mcx>>,
    lastoff: OffsetNumber,
    lastextra: usize,
    level: u32,
    full: usize,
}

struct BTWriteState<'a, 'mcx> {
    index: &'a Relation<'mcx>,
    bulkstate: BulkWriteState,
    inskey: BtScanInsert,
    frame: OrderProcFrame,
    pages_alloced: BlockNumber,
}

pub fn btbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    let mut sortstate = spool_begin(heap, index, indexInfo)?;
    // Unique builds route dead tuples into a second spool, kept out of the
    // uniqueness check and merged back in _bt_load (nbtsort.c:436); it gets
    // only work_mem.
    let mut spool2 = if indexInfo.ii_Unique {
        Some(tuplesort::Tuplesort::begin_index_btree(
            heap,
            index,
            false,
            false,
            init_small::globals::work_mem(),
            tuplesort::TUPLESORT_NONE,
        )?)
    } else {
        None
    };
    let mut havedead = false;
    let mut indtuples = 0.0f64;

    // M4.2 pool arm (default OFF): pool workers scan block-range morsels
    // and stream FORMED tuple images; the routing below is the serial
    // callback's, image form. Fallback (refusal-class only, nothing
    // consumed) keeps the serial scan byte-identical.
    let mut pool_fed: Option<f64> = None;
    {
        let mut put = |image: &[u8], alive: bool| -> PgResult<()> {
            if alive || spool2.is_none() {
                sortstate.put_index_tuple_image(image)?;
            } else {
                havedead = true;
                spool2.as_mut().expect("spool2").put_index_tuple_image(image)?;
            }
            indtuples += 1.0;
            Ok(())
        };
        if let pool::PoolFeed::Fed { reltuples } =
            pool::pool_feed_spools(heap, index, indexInfo, &mut put)?
        {
            pool_fed = Some(reltuples);
        }
    }

    let reltuples = match pool_fed {
        Some(r) => r,
        None => execindexing::table_index_build_scan(
            mcx,
            heap,
            index,
            indexInfo,
            true,
            |_index_rel, tid, values, isnull, tuple_is_alive| {
                if tuple_is_alive || spool2.is_none() {
                    sortstate.putindextuplevalues(*tid, values, isnull)?;
                } else {
                    havedead = true;
                    spool2.as_mut().expect("spool2").putindextuplevalues(*tid, values, isnull)?;
                }
                indtuples += 1.0;
                Ok(())
            },
        )?,
    };

    if !havedead {
        spool2 = None;
    }

    leafbuild(mcx, index, sortstate, spool2, indexInfo.ii_Unique)?;

    Ok(IndexBuildResult { heap_tuples: reltuples, index_tuples: indtuples })
}

fn spool_begin<'mcx>(
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &IndexInfo<'_>,
) -> PgResult<tuplesort::Tuplesort> {
    tuplesort::Tuplesort::begin_index_btree(
        heap,
        index,
        indexInfo.ii_Unique,
        indexInfo.ii_NullsNotDistinct,
        init_small::globals::maintenance_work_mem(),
        tuplesort::TUPLESORT_NONE,
    )
}

/// btbuildempty (nbtree.c): unlogged indexes' INIT_FORKNUM metapage.
pub fn btbuildempty(index: &Relation<'_>) -> PgResult<()> {
    let allequalimage = bt_allequalimage(index, false)?;
    let mut bulkstate = bulkwrite::smgr_bulk_start_rel(index, ForkNumber::INIT_FORKNUM)?;
    let mut metabuf = bulkwrite::smgr_bulk_get_buf(&bulkstate);
    {
        let mut page = page_mut_of(&mut metabuf);
        nbtree::bt_initmetapage(&mut page, P_NONE, 0, allequalimage);
    }
    bulkwrite::smgr_bulk_write(&mut bulkstate, BTREE_METAPAGE, metabuf, true)?;
    bulkwrite::smgr_bulk_finish(bulkstate)
}

fn leafbuild<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    mut sortstate: tuplesort::Tuplesort,
    mut spool2: Option<tuplesort::Tuplesort>,
    is_unique: bool,
) -> PgResult<()> {
    sortstate.performsort()?;
    if let Some(s2) = spool2.as_mut() {
        s2.performsort()?;
    }

    let mut inskey = nbtree::bt_mkscankey(index, None)?;
    inskey.allequalimage = bt_allequalimage(index, true)?;

    let mut wstate = BTWriteState {
        index,
        bulkstate: bulkwrite::smgr_bulk_start_rel(index, ForkNumber::MAIN_FORKNUM)?,
        inskey,
        frame: OrderProcFrame::new(),
        pages_alloced: BTREE_METAPAGE + 1,
    };

    let deduplicate =
        wstate.inskey.allequalimage && !is_unique && bt_get_deduplicate_items(index);

    let mut levels: Vec<BTPageState<'mcx>> = Vec::new();
    if let Some(mut sort2) = spool2 {
        // _bt_load merge arm (nbtsort.c:1156): interleave the live and dead
        // spools in key-then-TID order; dedup never applies (unique build).
        let mut itup = sortstate.getindextuple(true)?;
        let mut itup2 = sort2.getindextuple(true)?;
        loop {
            let load1 = match (itup, itup2) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                // SAFETY: each image stays live until its own spool's next
                // getindextuple; both fetches below happen after buildadd
                // copies what it retains.
                (Some(t1), Some(t2)) => {
                    // SAFETY: see contract above.
                    let cmp = unsafe { sortstate.compare_index_tuples(t1, t2) };
                    cmp <= 0
                }
            };
            if levels.is_empty() {
                let st = pagestate(&mut wstate, 0);
                levels.push(st);
            }
            if load1 {
                // SAFETY: live image (contract above).
                unsafe { buildadd(mcx, &mut wstate, &mut levels, 0, itup.expect("itup"), 0)? };
                itup = sortstate.getindextuple(true)?;
            } else {
                // SAFETY: live image (contract above).
                unsafe { buildadd(mcx, &mut wstate, &mut levels, 0, itup2.expect("itup2"), 0)? };
                itup2 = sort2.getindextuple(true)?;
            }
        }
    } else if deduplicate {
        let keysz = index.indnkeyatts();
        // C: 1/10 of the page, MAXALIGN_DOWN, minus one line pointer.
        let maxpostingsize = ((BLCKSZ * 10 / 100) & !7) - core::mem::size_of::<ItemIdData>();
        let mut dstate = BTDedupState::new(maxpostingsize);
        let mut basebuf = ItupBuf::with_size(mcx, INDEX_SIZE_MASK as usize + 1)?;
        while let Some(itup) = sortstate.getindextuple(true)? {
            // SAFETY: sorted-run image stays live until the next
            // getindextuple call; base is copied into basebuf, htids into
            // dstate, before that.
            unsafe {
                if levels.is_empty() {
                    let st = pagestate(&mut wstate, 0);
                    levels.push(st);
                    start_pending_copy(&mut dstate, &mut basebuf, itup);
                } else if nbtree::bt_keep_natts_fast(index, dstate.base, itup) > keysz
                    && dstate.save_htid(itup)
                {
                } else {
                    sort_dedup_finish_pending(mcx, &mut wstate, &mut levels, &mut dstate)?;
                    start_pending_copy(&mut dstate, &mut basebuf, itup);
                }
            }
        }
        if !levels.is_empty() {
            // SAFETY: base lives in basebuf.
            unsafe { sort_dedup_finish_pending(mcx, &mut wstate, &mut levels, &mut dstate)? };
        }
    } else {
        while let Some(itup) = sortstate.getindextuple(true)? {
            if levels.is_empty() {
                let st = pagestate(&mut wstate, 0);
                levels.push(st);
            }
            // SAFETY: sorted-run image stays live until the next
            // getindextuple call; buildadd copies what it retains.
            unsafe { buildadd(mcx, &mut wstate, &mut levels, 0, itup, 0)? };
        }
    }

    uppershutdown(mcx, &mut wstate, levels)?;
    bulkwrite::smgr_bulk_finish(wstate.bulkstate)?;
    drop(sortstate);
    Ok(())
}

fn blnewpage(wstate: &mut BTWriteState<'_, '_>, level: u32) -> BulkWriteBuffer {
    let mut buf = bulkwrite::smgr_bulk_get_buf(&wstate.bulkstate);
    let mut page = page_mut_of(&mut buf);
    nbtree::bt_pageinit(&mut page);
    write_opaque(
        &mut page,
        &BTPageOpaqueData {
            btpo_prev: P_NONE,
            btpo_next: P_NONE,
            btpo_level: level,
            btpo_flags: if level > 0 { 0 } else { BTP_LEAF },
            btpo_cycleid: 0,
        },
    );
    let lower = page.as_ref().pd_lower();
    page.set_pd_lower(lower + core::mem::size_of::<ItemIdData>() as u16);
    buf
}

fn pagestate<'mcx>(wstate: &mut BTWriteState<'_, '_>, level: u32) -> BTPageState<'mcx> {
    let buf = blnewpage(wstate, level);
    let blkno = wstate.pages_alloced;
    wstate.pages_alloced += 1;
    let full = if level > 0 {
        BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR as usize) / 100
    } else {
        bt_get_target_page_free_space(wstate.index)
    };
    BTPageState { buf, blkno, lowkey: None, lastoff: P_HIKEY, lastextra: 0, level, full }
}

// BTGetDeduplicateItems
fn bt_get_deduplicate_items(index: &Relation<'_>) -> bool {
    index
        .rd_options
        .as_ref()
        .and_then(|o| o.btree())
        .map(|o| o.deduplicate_items)
        .unwrap_or(true)
}

/// _bt_dedup_start_pending over an owned base copy (C's CopyIndexTuple).
/// # Safety
/// `itup` is a live index-tuple image.
unsafe fn start_pending_copy(dstate: &mut BTDedupState, basebuf: &mut ItupBuf<'_>, itup: ITup) {
    core::ptr::copy_nonoverlapping(itup, basebuf.as_mut_ptr(), index_tuple_size(itup));
    dstate.start_pending(basebuf.as_ptr(), InvalidOffsetNumber);
}

/// _bt_sort_dedup_finish_pending.
/// # Safety
/// `dstate.base` lives in the caller's basebuf.
unsafe fn sort_dedup_finish_pending<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'_, 'mcx>,
    levels: &mut Vec<BTPageState<'mcx>>,
    dstate: &mut BTDedupState,
) -> PgResult<()> {
    let base = dstate.base;
    match dstate.sort_finish_pending() {
        None => buildadd(mcx, wstate, levels, 0, base, 0),
        Some((posting, _, truncextra)) => buildadd(mcx, wstate, levels, 0, posting, truncextra),
    }
}

// BTGetTargetPageFreeSpace
fn bt_get_target_page_free_space(index: &Relation<'_>) -> usize {
    BLCKSZ * (100 - index.get_fillfactor(BTREE_DEFAULT_FILLFACTOR as i32) as usize) / 100
}

fn slideleft(page: &mut PageMut<'_>) {
    let maxoff = page.as_ref().max_offset_number();
    debug_assert!(maxoff >= P_FIRSTKEY);
    for off in P_FIRSTKEY..=maxoff {
        let id = page.as_ref().item_id(off);
        page.set_item_id(off - 1, id);
    }
    let lower = page.as_ref().pd_lower();
    page.set_pd_lower(lower - core::mem::size_of::<ItemIdData>() as u16);
}

/// # Safety
/// `itup` is a live index-tuple image of `itemsize` bytes.
unsafe fn sortaddtup(
    page: &mut PageMut<'_>,
    itemsize: usize,
    itup: ITup,
    itup_off: OffsetNumber,
    newfirstdataitem: bool,
) -> PgResult<()> {
    let mut trunc = [0u8; INDEX_TUPLE_HEADER_SIZE];
    let (bytes, len): (*const u8, usize) = if newfirstdataitem {
        core::ptr::copy_nonoverlapping(itup, trunc.as_mut_ptr(), INDEX_TUPLE_HEADER_SIZE);
        set_t_info(trunc.as_mut_ptr(), INDEX_TUPLE_HEADER_SIZE as u16);
        itup::bt_tuple_set_natts(trunc.as_mut_ptr(), 0, false);
        (trunc.as_ptr(), INDEX_TUPLE_HEADER_SIZE)
    } else {
        (itup, itemsize)
    };
    let item = core::slice::from_raw_parts(bytes, len);
    if page.add_item(item, itup_off, 0).is_none() {
        return Err(Box::new(types_error::PgError::error(
            "failed to add item to the index page",
        )));
    }
    Ok(())
}

/// _bt_buildadd.
/// # Safety
/// `itup` is a live index-tuple image.
unsafe fn buildadd<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'_, 'mcx>,
    levels: &mut Vec<BTPageState<'mcx>>,
    level_idx: usize,
    itup: ITup,
    truncextra: usize,
) -> PgResult<()> {
    let itupsz = maxalign(index_tuple_size(itup));
    let isleaf = levels[level_idx].level == 0;
    let last_truncextra = levels[level_idx].lastextra;
    levels[level_idx].lastextra = truncextra;
    debug_assert!(last_truncextra == 0 || isleaf);

    if itupsz > BTMaxItemSize {
        let state = &mut levels[level_idx];
        let page = page_mut_of(&mut state.buf);
        nbtree::bt_check_third_page(wstate.index, wstate.index, isleaf, &page.as_ref(), itup)?;
    }

    let pgspc = {
        let state = &mut levels[level_idx];
        page_mut_of(&mut state.buf).as_ref().free_space()
    };
    let last_off = levels[level_idx].lastoff;

    let tid_space = if isleaf {
        maxalign(core::mem::size_of::<ItemPointerData>())
    } else {
        0
    };
    if pgspc < itupsz + tid_space
        || (pgspc + last_truncextra < levels[level_idx].full && last_off > P_FIRSTKEY)
    {
        let obuf_blkno = levels[level_idx].blkno;
        let level = levels[level_idx].level;
        let nblkno = wstate.pages_alloced;
        wstate.pages_alloced += 1;
        let mut nbuf = blnewpage(wstate, level);

        {
            let state = &mut levels[level_idx];
            let mut opage = page_mut_of(&mut state.buf);

            debug_assert!(state.lastoff > P_FIRSTKEY);
            let ii = opage.as_ref().item_id(state.lastoff);
            let (optr, olen) = opage.as_ref().item_raw(ii);
            let mut npage = page_mut_of(&mut nbuf);
            sortaddtup(&mut npage, olen as usize, optr, P_FIRSTKEY, level > 0)?;

            opage.set_item_id(P_HIKEY, ii);
            let mut unused = ii;
            unused.set_unused();
            opage.set_item_id(state.lastoff, unused);
            let lower = opage.as_ref().pd_lower();
            opage.set_pd_lower(lower - core::mem::size_of::<ItemIdData>() as u16);

            if level == 0 {
                let hi = opage.as_ref().item_id(P_HIKEY);
                let (oitup_ptr, _) = opage.as_ref().item_raw(hi);
                let lastleft_id = opage.as_ref().item_id(state.lastoff - 1);
                let (lastleft_ptr, _) = opage.as_ref().item_raw(lastleft_id);
                let truncated = nbtree::bt_truncate(
                    mcx,
                    wstate.index,
                    lastleft_ptr,
                    oitup_ptr,
                    &mut wstate.inskey,
                    &mut wstate.frame,
                )?;
                let tsz = index_tuple_size(truncated.as_ptr());
                let titem = core::slice::from_raw_parts(truncated.as_ptr(), tsz);
                if !opage.index_tuple_overwrite(P_HIKEY, titem) {
                    return Err(Box::new(types_error::PgError::error(
                        "failed to add high key to the index page",
                    )));
                }
            }
        }

        if levels.len() == level_idx + 1 {
            let parent = pagestate(wstate, level + 1);
            levels.push(parent);
        }

        // Link the old page into its parent via its low key.
        let mut lowkey = levels[level_idx].lowkey.take().expect("low key for finished page");
        itup::bt_tuple_set_downlink(lowkey.as_mut_ptr(), obuf_blkno);
        buildadd(mcx, wstate, levels, level_idx + 1, lowkey.as_ptr(), 0)?;
        drop(lowkey);

        // New page's low key = old page's high key.
        {
            let state = &mut levels[level_idx];
            let opage = page_mut_of(&mut state.buf);
            let hi = opage.as_ref().item_id(P_HIKEY);
            let (hiptr, _) = opage.as_ref().item_raw(hi);
            let copied = itup::copy_index_tuple(mcx, hiptr)?;
            state.lowkey = Some(copied);
        }

        {
            let state = &mut levels[level_idx];
            let mut opage = page_mut_of(&mut state.buf);
            let mut oop = read_opaque(&opage);
            oop.btpo_next = nblkno;
            write_opaque(&mut opage, &oop);
            let mut npage = page_mut_of(&mut nbuf);
            let mut nop = read_opaque(&npage);
            nop.btpo_prev = obuf_blkno;
            nop.btpo_next = P_NONE;
            write_opaque(&mut npage, &nop);
        }
        let obuf = core::mem::replace(&mut levels[level_idx].buf, nbuf);
        bulkwrite::smgr_bulk_write(&mut wstate.bulkstate, obuf_blkno, obuf, true)?;
        levels[level_idx].blkno = nblkno;
        // The moved last item sits at P_FIRSTKEY on the new page.
        levels[level_idx].lastoff = P_FIRSTKEY;
    }

    // First item on its entire level gets a minus-infinity low key.
    if levels[level_idx].lastoff == P_HIKEY {
        debug_assert!(levels[level_idx].lowkey.is_none());
        let mut minus_inf = ItupBuf::with_size(mcx, INDEX_TUPLE_HEADER_SIZE)?;
        set_t_tid(minus_inf.as_mut_ptr(), ItemPointerData::new(0, 0));
        set_t_info(minus_inf.as_mut_ptr(), INDEX_TUPLE_HEADER_SIZE as u16);
        itup::bt_tuple_set_natts(minus_inf.as_mut_ptr(), 0, false);
        levels[level_idx].lowkey = Some(minus_inf);
    }

    let state = &mut levels[level_idx];
    let new_off = state.lastoff + 1;
    let level = state.level;
    let mut page = page_mut_of(&mut state.buf);
    sortaddtup(&mut page, itupsz, itup, new_off, level > 0 && new_off == P_FIRSTKEY)?;
    state.lastoff = new_off;
    Ok(())
}

fn uppershutdown<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'_, 'mcx>,
    mut levels: Vec<BTPageState<'mcx>>,
) -> PgResult<()> {
    let mut rootblkno = P_NONE;
    let mut rootlevel = 0u32;

    let nlevels = levels.len();
    for i in 0..nlevels {
        let blkno = levels[i].blkno;
        if i + 1 == nlevels {
            let state = &mut levels[i];
            let mut page = page_mut_of(&mut state.buf);
            let mut op = read_opaque(&page);
            op.btpo_flags |= BTP_ROOT;
            write_opaque(&mut page, &op);
            rootblkno = blkno;
            rootlevel = state.level;
        } else {
            let mut lowkey = levels[i].lowkey.take().expect("low key for last page on level");
            // SAFETY: lowkey is a live owned tuple image.
            unsafe {
                itup::bt_tuple_set_downlink(lowkey.as_mut_ptr(), blkno);
                buildadd(mcx, wstate, &mut levels, i + 1, lowkey.as_ptr(), 0)?;
            }
        }
        let state = &mut levels[i];
        let mut page = page_mut_of(&mut state.buf);
        slideleft(&mut page);
        let empty = bulkwrite::smgr_bulk_get_buf(&wstate.bulkstate);
        let buf = core::mem::replace(&mut state.buf, empty);
        let blkno = state.blkno;
        bulkwrite::smgr_bulk_write(&mut wstate.bulkstate, blkno, buf, true)?;
    }

    let mut metabuf = bulkwrite::smgr_bulk_get_buf(&wstate.bulkstate);
    {
        let mut page = page_mut_of(&mut metabuf);
        nbtree::bt_initmetapage(&mut page, rootblkno, rootlevel, wstate.inskey.allequalimage);
    }
    bulkwrite::smgr_bulk_write(&mut wstate.bulkstate, BTREE_METAPAGE, metabuf, true)
}

fn page_mut_of(buf: &mut BulkWriteBuffer) -> PageMut<'_> {
    // SAFETY: exclusively owned, aligned build page.
    unsafe { PageMut::from_raw(core::ptr::NonNull::new_unchecked(buf.page_mut().as_mut_ptr())) }
}

fn read_opaque(page: &PageMut<'_>) -> BTPageOpaqueData {
    let special = page.as_ref().pd_special() as usize;
    // SAFETY: special area sized for BTPageOpaqueData by bt_pageinit.
    unsafe {
        page.as_ref()
            .as_ptr()
            .add(special)
            .cast::<BTPageOpaqueData>()
            .read()
    }
}

fn write_opaque(page: &mut PageMut<'_>, opaque: &BTPageOpaqueData) {
    let special = page.as_ref().pd_special() as usize;
    // SAFETY: special area sized for BTPageOpaqueData by bt_pageinit.
    unsafe {
        page.as_ref()
            .as_ptr()
            .cast_mut()
            .add(special)
            .cast::<BTPageOpaqueData>()
            .write(*opaque)
    }
}

// _bt_allequalimage (nbtutils.c). `debugmessage` as in C: only the CREATE
// INDEX build path (leafbuild, C _bt_leafbuild) reports the verdict.
fn bt_allequalimage(rel: &Relation<'_>, debugmessage: bool) -> PgResult<bool> {
    let allequalimage = bt_allequalimage_check(rel)?;
    if debugmessage {
        // C nbtutils.c:4291-4299 (elog(DEBUG1, ...): not translated).
        let (msg, lineno) = if allequalimage {
            (
                format!("index \"{}\" can safely use deduplication", rel.name()),
                4294,
            )
        } else {
            (
                format!("index \"{}\" cannot use deduplication", rel.name()),
                4297,
            )
        };
        elog_seams::ereport::call(
            types_error::PgError::new(types_error::DEBUG1, msg).with_location(
                "nbtutils.c",
                lineno,
                "_bt_allequalimage",
            ),
        )?;
    }
    Ok(allequalimage)
}

fn bt_allequalimage_check(rel: &Relation<'_>) -> PgResult<bool> {
    // INCLUDE indexes can never support deduplication (nbtutils.c:4264).
    if rel.indnatts() != rel.indnkeyatts() {
        return Ok(false);
    }
    for i in 0..rel.indnkeyatts() as usize {
        let opfamily = rel.rd_opfamily[i];
        let opcintype = rel.rd_opcintype[i];
        let collation = rel.rd_indcollation[i];
        let equalimageproc =
            lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, BTEQUALIMAGE_PROC)?;
        if equalimageproc == InvalidOid {
            return Ok(false);
        }
        let mut finfo = fmgr_seams::fmgr_info::call(equalimageproc)?;
        let mut fcinfo = types_fmgr::LocalFcinfo::<1>::fresh(collation);
        fcinfo.set_arg(0, datum::Datum::from_oid(opcintype));
        if !finfo.invoke(&mut fcinfo)?.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

const _: () = assert!(BLCKSZ == 8192);
const _: () = assert!(SizeOfPageHeaderData == 24);
