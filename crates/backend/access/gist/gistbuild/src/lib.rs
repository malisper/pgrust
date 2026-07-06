//! gistbuild.c: insert-based (plain), buffered, and sorted build. LOUD lane:
//! non-range sortsupport opclasses (gist_point_sortsupport z-order).
#![allow(non_snake_case)]

pub mod buffered;
pub mod buffers;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext};
use ::types_core::{BlockNumber, ForkNumber, BLCKSZ, RELPERSISTENCE_UNLOGGED};
use ::types_error::PgResult;
use ::types_gist::{GistBuildLSN, F_LEAF, GIST_DEFAULT_FILLFACTOR, GIST_ROOT_BLKNO, GIST_SORTSUPPORT_PROC};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef};
use ::types_tuple::itemptr::ItemPointerData;
use execindexing::IndexInfo;

use gist::state::{index_getprocid, initGISTstate, GistState};
use gist::util::{
    gist_init_buffer, gistCompressValues, gistFormTuple, gistNewBuffer, gistextractpage,
    gistfillbuffer, gistfillitupvec, gistinitpage, gistunion,
};

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

const BUFFERING_MODE_SWITCH_CHECK_STEP: u64 = 256;
const BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET: u64 = 4096;

/// GistBuildMode, minus GIST_SORTED_BUILD (dispatched up front).
#[derive(Clone, Copy, PartialEq, Eq)]
enum GistBuildMode {
    Disabled,
    Auto,
    Stats,
    Active,
}

/// gistbuild.
pub fn gistbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    let mut build_mode = match index.rd_options.as_ref().and_then(|o| o.gist()) {
        Some(o) => match o.buffering_mode {
            types_rel::GistOptBufferingMode::GIST_OPTION_BUFFERING_ON => GistBuildMode::Stats,
            types_rel::GistOptBufferingMode::GIST_OPTION_BUFFERING_OFF => GistBuildMode::Disabled,
            types_rel::GistOptBufferingMode::GIST_OPTION_BUFFERING_AUTO => GistBuildMode::Auto,
        },
        None => GistBuildMode::Auto,
    };

    let fillfactor = index.get_fillfactor(GIST_DEFAULT_FILLFACTOR);
    let freespace = BLCKSZ * (100 - fillfactor as usize) / 100;

    let mut giststate = initGISTstate(mcx, index)?;
    let mut temp = MemoryContext::new_bump("GiST temporary context");

    // Unless buffering mode was forced, use sorting when possible.
    if build_mode != GistBuildMode::Stats {
        let nkeys = index.indnkeyatts() as usize;
        let hasallsortsupports =
            (0..nkeys).all(|i| index_getprocid(index, i, GIST_SORTSUPPORT_PROC) != 0);
        if hasallsortsupports {
            return gist_sorted_build(mcx, heap, index, indexInfo, &mut giststate, &mut temp);
        }
    }

    {
        let buffer = gistNewBuffer(index, heap)?;
        debug_assert!(buffer.block_number() == GIST_ROOT_BLKNO);
        gist_init_buffer(&buffer, F_LEAF);
        bufmgr_seams::mark_buffer_dirty::call(buffer.buffer())?;
        gist::buf_page_mut_pub(buffer.buffer()).set_lsn(GistBuildLSN);
        bufmgr_seams::lock_buffer::call(buffer.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
        drop(buffer);
    }

    let mut indtuples: u64 = 0;
    let mut indtuples_size: u64 = 0;
    let mut bufstate: Option<buffered::BufBuild<'mcx>> = None;

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        true,
        // gistBuildCallback
        |index_rel, tid, values, isnull, _tuple_is_alive| {
            {
                let tmcx = temp.mcx();
                let mut itup =
                    gistFormTuple(tmcx, &mut giststate, index_rel, values, isnull, true)?;
                unsafe {
                    itup.as_mut_ptr()
                        .cast::<ItemPointerData>()
                        .write_unaligned(*tid);
                }

                indtuples += 1;
                // SAFETY: owned live image.
                indtuples_size += unsafe { gist::util::index_tuple_size(itup.as_ptr()) } as u64;

                if build_mode != GistBuildMode::Active {
                    gist::insert::gistdoinsert(
                        tmcx,
                        index_rel,
                        itup.as_ptr(),
                        freespace,
                        &mut giststate,
                        heap,
                        true,
                    )?;
                } else {
                    let bb = bufstate.as_mut().expect("buffering active");
                    let rootlevel = bb.gfbb.rootlevel;
                    // SAFETY: owned live image.
                    let itup_slice = unsafe { gist::util::itup_slice(itup.as_ptr()) };
                    buffered::gist_process_itup(
                        tmcx,
                        index_rel,
                        heap,
                        freespace,
                        &mut giststate,
                        bb,
                        itup_slice,
                        0,
                        rootlevel,
                    )?;
                }
            }
            if build_mode == GistBuildMode::Active {
                buffered::gist_process_emptying_queue(
                    &mut temp,
                    index_rel,
                    heap,
                    freespace,
                    &mut giststate,
                    bufstate.as_mut().expect("buffering active"),
                )?;
            }
            temp.reset();

            if build_mode == GistBuildMode::Active
                && indtuples % BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0
            {
                let bb = bufstate.as_mut().expect("buffering active");
                bb.gfbb.pages_per_buffer = buffered::calculate_pages_per_buffer(
                    index_rel,
                    freespace,
                    indtuples,
                    indtuples_size,
                    bb.gfbb.level_step,
                );
            }

            // Switch to buffering once the index outgrows the cache (auto) or
            // once tuple-size stats are settled (buffering=on).
            let switch = match build_mode {
                GistBuildMode::Auto => {
                    indtuples % BUFFERING_MODE_SWITCH_CHECK_STEP == 0 && {
                        let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(
                            index_rel,
                            ForkNumber::MAIN_FORKNUM,
                        )?;
                        let effective_cache_size =
                            (guc_tables::vars::effective_cache_size.get().get)() as u64;
                        effective_cache_size < nblocks as u64
                    }
                }
                GistBuildMode::Stats => indtuples >= BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET,
                _ => false,
            };
            if switch {
                match buffered::gist_init_buffering(
                    mcx,
                    index_rel,
                    freespace,
                    indtuples,
                    indtuples_size,
                )? {
                    Some(bb) => {
                        bufstate = Some(bb);
                        build_mode = GistBuildMode::Active;
                    }
                    None => build_mode = GistBuildMode::Disabled,
                }
            }
            Ok(())
        },
    )?;

    if build_mode == GistBuildMode::Active {
        let bb = bufstate.as_mut().expect("buffering active");
        buffered::gist_empty_all_buffers(&mut temp, index, heap, freespace, &mut giststate, bb)?;
        bufstate.take().expect("buffering active").gfbb.free()?;
    }

    if gist::relation_needs_wal_pub(index) {
        let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)?;
        xloginsert::log_newpage_range(index, ForkNumber::MAIN_FORKNUM, 0, nblocks, true)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: indtuples as f64,
    })
}

const GIST_SORTED_BUILD_PAGE_NUM: usize = 4;

// GistSortedBuildLevelState: an in-memory buffer of the last pages at one
// tree level; flushing applies picksplit across the buffered pages so the
// split can be multidimension-aware.
struct GistLevelState {
    current_page: usize,
    last_blkno: BlockNumber,
    parent: Option<Box<GistLevelState>>,
    pages: [Option<Box<bulkwrite::AlignedPage>>; GIST_SORTED_BUILD_PAGE_NUM],
}

impl GistLevelState {
    fn new(flags: u16) -> GistLevelState {
        let mut st = GistLevelState {
            current_page: 0,
            last_blkno: 0,
            parent: None,
            pages: [None, None, None, None],
        };
        st.pages[0] = Some(new_build_page());
        gistinitpage(&mut page_mut_of(st.pages[0].as_mut().expect("just set")), flags);
        st
    }
}

fn new_build_page() -> Box<bulkwrite::AlignedPage> {
    Box::new(bulkwrite::AlignedPage([0u8; BLCKSZ]))
}

fn page_mut_of(p: &mut bulkwrite::AlignedPage) -> PageMut<'_> {
    // SAFETY: exclusively owned, aligned build page.
    unsafe { PageMut::from_raw(core::ptr::NonNull::new_unchecked(p.0.as_mut_ptr())) }
}

fn page_ref_of(p: &bulkwrite::AlignedPage) -> PageRef<'_> {
    // SAFETY: owned, aligned build page (shared read view).
    unsafe {
        PageRef::from_raw(core::ptr::NonNull::new_unchecked(p.0.as_ptr().cast_mut()))
    }
}

struct SortedWriteState<'a, 'mcx> {
    indexrel: &'a Relation<'mcx>,
    giststate: &'a mut GistState<'mcx>,
    pages_allocated: BlockNumber,
    bulkstate: bulkwrite::BulkWriteState,
}

/// gistbuild, GIST_SORTED_BUILD arm + gist_indexsortbuild.
fn gist_sorted_build<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
    giststate: &mut GistState<'mcx>,
    temp: &mut MemoryContext,
) -> PgResult<IndexBuildResult> {
    let mut sortstate = tuplesort::Tuplesort::begin_index_gist(
        heap,
        index,
        init_small::globals::maintenance_work_mem(),
        tuplesort::TUPLESORT_NONE,
    )?;

    let natts = index.rd_att.natts as usize;
    let mut indtuples: u64 = 0;
    let reltuples = {
        let sortstate = &mut sortstate;
        let giststate = &mut *giststate;
        execindexing::table_index_build_scan(
            mcx,
            heap,
            index,
            indexInfo,
            true,
            // gistSortedBuildCallback
            |index_rel, tid, values, isnull, _tuple_is_alive| {
                let tmcx = temp.mcx();
                let mut compressed =
                    [Datum::null(); ::types_core::fmgr::INDEX_MAX_KEYS as usize];
                gistCompressValues(
                    tmcx,
                    giststate,
                    index_rel,
                    values,
                    isnull,
                    true,
                    &mut compressed[..natts],
                )?;
                sortstate.putindextuplevalues(*tid, &compressed[..natts], isnull)?;
                temp.reset();
                indtuples += 1;
                Ok(())
            },
        )?
    };

    sortstate.performsort()?;

    // gist_indexsortbuild: block 0 reserved for the root, written last.
    let mut wstate = SortedWriteState {
        indexrel: index,
        giststate,
        pages_allocated: 1,
        bulkstate: bulkwrite::smgr_bulk_start_rel(index, ForkNumber::MAIN_FORKNUM)?,
    };
    let mut levelstate = Box::new(GistLevelState::new(F_LEAF));

    while let Some(itup) = sortstate.getindextuple(true)? {
        // SAFETY: sort-owned image, live until the next tuplesort call; it
        // is copied into the level page before then.
        let slice = unsafe { gist::util::itup_slice(itup) };
        levelstate_add(&mut wstate, &mut levelstate, slice)?;
    }

    // Flush partially full non-root pages; flush may stack a new root level.
    while levelstate.parent.is_some() || levelstate.current_page != 0 {
        levelstate_flush(&mut wstate, &mut levelstate)?;
        levelstate = levelstate.parent.take().expect("flush created the parent level");
    }

    {
        let root = levelstate.pages[0].as_mut().expect("root page");
        page_mut_of(root).set_lsn(GistBuildLSN);
        let mut buf = bulkwrite::smgr_bulk_get_buf(&wstate.bulkstate);
        buf.page_mut().copy_from_slice(&root.0);
        bulkwrite::smgr_bulk_write(&mut wstate.bulkstate, GIST_ROOT_BLKNO, buf, true)?;
    }
    bulkwrite::smgr_bulk_finish(wstate.bulkstate)?;
    drop(sortstate);

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: indtuples as f64,
    })
}

const SIZEOF_ITEM_ID_DATA: usize = 4;

/// gist_indexsortbuild_levelstate_add.
fn levelstate_add(
    wstate: &mut SortedWriteState<'_, '_>,
    levelstate: &mut GistLevelState,
    itup: &[u8],
) -> PgResult<()> {
    // C: fillfactor deliberately ignored here.
    let size_needed = itup.len() + SIZEOF_ITEM_ID_DATA;
    let cur = levelstate.current_page;
    let (cur_free, old_flags) = {
        let p = levelstate.pages[cur].as_ref().expect("current level page");
        let r = page_ref_of(p);
        (r.free_space(), ::types_gist::page_opaque(&r).flags)
    };
    if cur_free < size_needed {
        if cur + 1 == GIST_SORTED_BUILD_PAGE_NUM {
            levelstate_flush(wstate, levelstate)?;
        } else {
            levelstate.current_page += 1;
        }
        let slot = &mut levelstate.pages[levelstate.current_page];
        if slot.is_none() {
            *slot = Some(new_build_page());
        }
        gistinitpage(&mut page_mut_of(slot.as_mut().expect("just set")), old_flags);
    }
    let page = levelstate.pages[levelstate.current_page]
        .as_mut()
        .expect("current level page");
    gistfillbuffer(
        wstate.indexrel.name(),
        &mut page_mut_of(page),
        &[itup],
        ::types_tuple::itemptr::InvalidOffsetNumber,
    )
}

/// gist_indexsortbuild_levelstate_flush.
fn levelstate_flush(
    wstate: &mut SortedWriteState<'_, '_>,
    levelstate: &mut GistLevelState,
) -> PgResult<()> {
    let scratch = MemoryContext::new("gist sorted build flush");
    let smcx = scratch.mcx();

    let isleaf = {
        let p0 = levelstate.pages[0].as_ref().expect("level page 0");
        ::types_gist::GistPageIsLeaf(&page_ref_of(p0))
    };

    let mut itvec = {
        let p0 = levelstate.pages[0].as_ref().expect("level page 0");
        gistextractpage(smcx, &page_ref_of(p0))?
    };
    for i in 1..=levelstate.current_page {
        // gistjoinvector
        let pi = levelstate.pages[i].as_ref().expect("filled level page");
        itvec.append(&mut gistextractpage(smcx, &page_ref_of(pi))?);
    }
    let itptrs: Vec<gist::util::ITup> = itvec.iter().map(|b| b.as_ptr()).collect();

    let mut dist = if levelstate.current_page > 0 {
        gist::insert::gistSplit(smcx, wstate.indexrel, &itptrs, wstate.giststate)?
    } else {
        let union_tuple = gistunion(smcx, wstate.indexrel, &itptrs, wstate.giststate)?;
        let slices: Vec<&[u8]> = itvec
            .iter()
            // SAFETY: locally owned live images.
            .map(|b| unsafe { gist::util::itup_slice(b.as_ptr()) })
            .collect();
        vec![gist::insert::SplitPageLayout {
            blkno: ::types_core::InvalidBlockNumber,
            block_num: itptrs.len() as i32,
            list: gistfillitupvec(smcx, &slices)?,
            itup: Some(union_tuple),
            buf: 0,
            pin: None,
            temp_page: None,
        }]
    };

    levelstate.current_page = 0;

    for d in dist.iter_mut() {
        let mut buf = bulkwrite::smgr_bulk_get_buf(&wstate.bulkstate);
        {
            let mut target = page_mut_of_bulk(&mut buf);
            gistinitpage(&mut target, if isleaf { F_LEAF } else { 0 });
            let tuples: Vec<&[u8]> = {
                let mut v = Vec::with_capacity(d.block_num as usize);
                let mut off = 0usize;
                for _ in 0..d.block_num {
                    let p = d.list[off..].as_ptr();
                    // SAFETY: list is the concatenated tuple images.
                    let sz = unsafe { gist::util::index_tuple_size(p) };
                    v.push(&d.list[off..off + sz]);
                    off += sz;
                }
                v
            };
            gistfillbuffer(
                wstate.indexrel.name(),
                &mut target,
                &tuples,
                ::types_tuple::itemptr::FirstOffsetNumber,
            )?;
            // Right link to the previous page: right-links only need to form
            // a chain per level (GiST pages are unordered).
            if levelstate.last_blkno != 0 {
                ::types_gist::GistPageSetRightLink(&mut target, levelstate.last_blkno);
            }
            target.set_lsn(GistBuildLSN);
        }
        let blkno = wstate.pages_allocated;
        wstate.pages_allocated += 1;
        bulkwrite::smgr_bulk_write(&mut wstate.bulkstate, blkno, buf, true)?;

        let union_tuple = d.itup.as_mut().expect("split emits a downlink");
        // SAFETY: owned image; block-number write inside its extent.
        let uslice = unsafe {
            let sz = gist::util::index_tuple_size(union_tuple.as_ptr());
            core::slice::from_raw_parts_mut(union_tuple.as_mut_ptr(), sz)
        };
        gist::util::itup_set_block_number(uslice, blkno);
        levelstate.last_blkno = blkno;

        let parent = levelstate
            .parent
            .get_or_insert_with(|| Box::new(GistLevelState::new(0)));
        levelstate_add(wstate, parent, uslice)?;
    }
    Ok(())
}

fn page_mut_of_bulk(buf: &mut bulkwrite::BulkWriteBuffer) -> PageMut<'_> {
    // SAFETY: exclusively owned, aligned build page.
    unsafe { PageMut::from_raw(core::ptr::NonNull::new_unchecked(buf.page_mut().as_mut_ptr())) }
}

/// gistbuildempty (gist.c): unlogged indexes' INIT_FORKNUM root page.
pub fn gistbuildempty(index: &Relation<'_>) -> PgResult<()> {
    debug_assert!(index.rd_rel.relpersistence == RELPERSISTENCE_UNLOGGED);
    let (buf, _) = bufmgr_seams::extend_buffered_rel_by::call(
        index,
        ForkNumber::INIT_FORKNUM,
        None,
        bufmgr_seams::EB_SKIP_EXTENSION_LOCK | bufmgr_seams::EB_LOCK_FIRST,
        1,
    )?;
    let pin = bufmgr_seams::BufferPin::adopt(buf).expect("extend returns a valid buffer");
    init_small::globals::StartCriticalSection();
    gist_init_buffer(&pin, F_LEAF);
    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;
    xloginsert::log_newpage_buffer(pin.buffer(), true)?;
    init_small::globals::EndCriticalSection();
    bufmgr_seams::lock_buffer::call(pin.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    pin.release();
    Ok(())
}
