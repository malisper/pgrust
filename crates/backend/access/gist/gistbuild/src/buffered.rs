//! gistbuild.c buffering-mode machinery: gistInitBuffering, gistProcessItup,
//! gistbufferinginserttuples, the emptying queue, and the parent map.

use std::collections::HashMap;

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_gist::{GISTPageOpaqueData, GistPageIsLeaf, GIST_ROOT_BLKNO};
use ::types_rel::Relation;
use ::types_storage::bufpage::SizeOfPageHeaderData;
use ::types_tuple::itemptr::{FirstOffsetNumber, InvalidOffsetNumber};

use gist::insert::gistplacetopage;
use gist::state::GistState;
use gist::util::{gistcheckpage, gistchoose, gistgetadjusted, itup_block_number, itup_slice, page_item};

use crate::buffers::{gistRelocateBuildBuffersOnSplit, GistBuildBuffers};

const GIST_SHARE: i32 = bufmgr::BUFFER_LOCK_SHARE;
const GIST_EXCLUSIVE: i32 = bufmgr::BUFFER_LOCK_EXCLUSIVE;

const SIZEOF_ITEM_ID_DATA: usize = 4;
const SIZEOF_INDEX_TUPLE_DATA_MAXALIGNED: usize = 8;
const VARHDRSZ: usize = 4;

fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        panic!("unported: ProcessInterrupts (tcop/postgres.c) reached from gist build");
    }
}

fn lock(pin: &BufferPin, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(pin.buffer(), mode)
}

fn unlock_release(pin: BufferPin) -> PgResult<()> {
    bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
    drop(pin);
    Ok(())
}

fn read_buffer<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<BufferPin> {
    Ok(BufferPin::adopt(bufmgr::read_buffer::call(rel, blkno)?).expect("ReadBuffer"))
}

/// Buffering-build state: GISTBuildState's gfbb + parentMap.
pub struct BufBuild<'mcx> {
    pub gfbb: GistBuildBuffers<'mcx>,
    pub parent_map: HashMap<BlockNumber, BlockNumber>,
}

fn page_free_space_for_tuples(freespace: usize) -> usize {
    BLCKSZ
        - SizeOfPageHeaderData
        - core::mem::size_of::<GISTPageOpaqueData>()
        - SIZEOF_ITEM_ID_DATA
        - freespace
}

fn effective_cache_size() -> i32 {
    (guc_tables::vars::effective_cache_size.get().get)()
}

/// gistInitBuffering. Returns None when there's not enough cache or
/// maintenance_work_mem (C's fall back to GIST_BUFFERING_DISABLED).
pub fn gist_init_buffering<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'_>,
    freespace: usize,
    indtuples: u64,
    indtuples_size: u64,
) -> PgResult<Option<BufBuild<'mcx>>> {
    let page_free_space = page_free_space_for_tuples(freespace);
    let itup_avg_size = indtuples_size as f64 / indtuples as f64;

    // C's caveat applies: short varlenas and padding are not accounted for.
    let mut itup_min_size = SIZEOF_INDEX_TUPLE_DATA_MAXALIGNED;
    for i in 0..index.rd_att.natts as usize {
        let attlen = index.rd_att.compact_attr(i).attlen;
        itup_min_size += if attlen < 0 { VARHDRSZ } else { attlen as usize };
    }

    let avg_index_tuples_per_page = page_free_space as f64 / itup_avg_size;
    let max_index_tuples_per_page = page_free_space as f64 / itup_min_size as f64;

    // levelStep: the highest subtree depth that still fits in a quarter of
    // effective_cache_size, bounded by one in-memory page per lowest-level
    // buffer under maintenance_work_mem.
    let mut level_step: i32 = 1;
    loop {
        let subtreesize = (1.0 - avg_index_tuples_per_page.powi(level_step + 1))
            / (1.0 - avg_index_tuples_per_page);
        let maxlowestlevelpages = max_index_tuples_per_page.powi(level_step);

        if subtreesize > (effective_cache_size() / 4) as f64 {
            break;
        }
        if maxlowestlevelpages
            > (init_small::globals::maintenance_work_mem() as f64 * 1024.0) / BLCKSZ as f64
        {
            break;
        }
        level_step += 1;
    }
    level_step -= 1;

    if level_step <= 0 {
        return Ok(None);
    }

    let pages_per_buffer =
        calculate_pages_per_buffer(index, freespace, indtuples, indtuples_size, level_step);
    let gfbb = GistBuildBuffers::new(mcx, pages_per_buffer, level_step, gist_get_max_level(index)?)?;

    Ok(Some(BufBuild {
        gfbb,
        parent_map: HashMap::new(),
    }))
}

/// calculatePagesPerBuffer: emptying half a buffer fills on average one page
/// in every buffer at the next lower level.
pub fn calculate_pages_per_buffer(
    index: &Relation<'_>,
    freespace: usize,
    indtuples: u64,
    indtuples_size: u64,
    level_step: i32,
) -> i32 {
    let _ = index;
    let page_free_space = page_free_space_for_tuples(freespace);
    let itup_avg_size = indtuples_size as f64 / indtuples as f64;
    let avg_index_tuples_per_page = page_free_space as f64 / itup_avg_size;
    let pages_per_buffer = 2.0 * avg_index_tuples_per_page.powi(level_step);
    pages_per_buffer.round_ties_even() as i32
}

/// gistGetMaxLevel.
pub fn gist_get_max_level(index: &Relation<'_>) -> PgResult<i32> {
    let mut max_level = 0;
    let mut blkno = GIST_ROOT_BLKNO;
    loop {
        let pin = read_buffer(index, blkno)?;
        // No concurrent access during build; locking is pro forma.
        lock(&pin, GIST_SHARE)?;
        let next = {
            let page = pin.page();
            if GistPageIsLeaf(&page) {
                None
            } else {
                // The tree has the same depth everywhere: follow the first
                // downlink.
                let itup = page_item(&page, FirstOffsetNumber);
                // SAFETY: page item under our content lock.
                Some(unsafe { itup_block_number(itup) })
            }
        };
        unlock_release(pin)?;
        match next {
            None => break,
            Some(next_blkno) => {
                blkno = next_blkno;
                max_level += 1;
            }
        }
    }
    Ok(max_level)
}

/// gistProcessItup: run one tuple down the tree to a leaf page or a buffered
/// level (gistBufferingBuildInsert's first half; the caller runs the emptying
/// queue after). Returns true if a lower-level buffer overflowed (stop
/// emptying).
#[allow(clippy::too_many_arguments)]
pub fn gist_process_itup(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    freespace: usize,
    giststate: &mut GistState<'_>,
    bb: &mut BufBuild<'_>,
    itup: &[u8],
    startblkno: BlockNumber,
    startlevel: i32,
) -> PgResult<bool> {
    check_for_interrupts();

    let mut result = false;
    let mut blkno = startblkno;
    let mut level = startlevel;
    let mut downlinkoffnum: OffsetNumber = InvalidOffsetNumber;
    let mut parentblkno: BlockNumber = InvalidBlockNumber;

    loop {
        if bb.gfbb.level_has_buffers(level) && level != startlevel {
            break;
        }
        if level == 0 {
            break;
        }

        let pin = read_buffer(index, blkno)?;
        lock(&pin, GIST_EXCLUSIVE)?;

        let (childoffnum, childblkno) = {
            let page = pin.page();
            let childoffnum = gistchoose(mcx, index, &page, itup.as_ptr(), giststate)?;
            let idxtuple = page_item(&page, childoffnum);
            // SAFETY: page item under our content lock.
            (childoffnum, unsafe { itup_block_number(idxtuple) })
        };

        if level > 1 {
            bb.parent_map.insert(childblkno, blkno);
        }

        let newtup = {
            let page = pin.page();
            let idxtuple = page_item(&page, childoffnum);
            gistgetadjusted(mcx, index, idxtuple, itup.as_ptr(), giststate)?
        };
        if let Some(newtup) = newtup {
            // SAFETY: owned image, live for the call.
            let newtup_slice = unsafe { itup_slice(newtup.as_ptr()) };
            blkno = gist_buffering_insert_tuples(
                mcx,
                index,
                heap,
                freespace,
                giststate,
                bb,
                pin,
                level,
                &[newtup_slice],
                childoffnum,
                InvalidBlockNumber,
                InvalidOffsetNumber,
            )?;
        } else {
            unlock_release(pin)?;
        }

        parentblkno = blkno;
        blkno = childblkno;
        downlinkoffnum = childoffnum;
        debug_assert!(level > 0);
        level -= 1;
    }

    if bb.gfbb.level_has_buffers(level) {
        bb.gfbb.get_node_buffer(blkno, level);
        bb.gfbb.push_itup(blkno, itup)?;
        if bb.gfbb.node_buffers[&blkno].blocks_count > bb.gfbb.pages_per_buffer {
            result = true;
        }
    } else {
        debug_assert!(level == 0);
        let pin = read_buffer(index, blkno)?;
        lock(&pin, GIST_EXCLUSIVE)?;
        gist_buffering_insert_tuples(
            mcx,
            index,
            heap,
            freespace,
            giststate,
            bb,
            pin,
            level,
            &[itup],
            InvalidOffsetNumber,
            parentblkno,
            downlinkoffnum,
        )?;
    }

    Ok(result)
}

/// gistbufferinginserttuples: gistinserttuples analogue for the buffered
/// build; consumes (unlocks + releases) `buffer`. Returns the block the
/// (first) new or updated tuple landed on.
#[allow(clippy::too_many_arguments)]
fn gist_buffering_insert_tuples(
    mcx: Mcx<'_>,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    freespace: usize,
    giststate: &mut GistState<'_>,
    bb: &mut BufBuild<'_>,
    buffer: BufferPin,
    level: i32,
    itup: &[&[u8]],
    oldoffnum: OffsetNumber,
    parentblk: BlockNumber,
    downlinkoffnum: OffsetNumber,
) -> PgResult<BlockNumber> {
    let mut placed_to_blk = InvalidBlockNumber;
    let (is_split, mut splitinfo) = gistplacetopage(
        mcx,
        index,
        freespace,
        giststate,
        &buffer,
        itup,
        oldoffnum,
        None,
        false,
        heap,
        true,
        Some(&mut placed_to_blk),
    )?;

    // Root split: keep the in-memory root path complete so parent re-finding
    // always terminates.
    if is_split && buffer.block_number() == GIST_ROOT_BLKNO {
        debug_assert!(level == bb.gfbb.rootlevel);
        bb.gfbb.rootlevel += 1;

        // The old root's downlinks all moved to the new children: memorize
        // the grandchildren's parents.
        if bb.gfbb.rootlevel > 1 {
            let children: Vec<BlockNumber> = {
                let page = buffer.page();
                let maxoff = page.max_offset_number();
                (FirstOffsetNumber..=maxoff)
                    // SAFETY: page items under our content lock.
                    .map(|off| unsafe { itup_block_number(page_item(&page, off)) })
                    .collect()
            };
            for childblkno in children {
                let childpin = read_buffer(index, childblkno)?;
                lock(&childpin, GIST_SHARE)?;
                memorize_all_downlinks(&mut bb.parent_map, &childpin);
                unlock_release(childpin)?;
                bb.parent_map.insert(childblkno, GIST_ROOT_BLKNO);
            }
        }
    }

    if !splitinfo.is_empty() {
        // The parent may have changed since this path was memorized.
        let (parent_pin, next_downlinkoffnum) = gist_buffering_find_correct_parent(
            index,
            bb,
            buffer.block_number(),
            level,
            parentblk,
            downlinkoffnum,
        )?;

        // Split the page's buffer too, folding its tuples into the downlinks.
        gistRelocateBuildBuffersOnSplit(
            mcx,
            &mut bb.gfbb,
            giststate,
            index,
            level,
            &buffer,
            &mut splitinfo,
        )?;

        let parent_blkno = parent_pin.block_number();
        let mut downlinks = Vec::with_capacity(splitinfo.len());
        let mut original_pin = Some(buffer);
        for si in splitinfo {
            let si_blkno = bufmgr::buffer_get_block_number::call(si.buf);

            // Downlinks must fit on the parent page for this to be enough; a
            // parent split re-updates the map in the recursive call.
            if level > 0 {
                bb.parent_map.insert(si_blkno, parent_blkno);
            }
            if level > 1 {
                let pin_ref = si
                    .pin
                    .as_ref()
                    .unwrap_or_else(|| original_pin.as_ref().expect("original split page"));
                memorize_all_downlinks(&mut bb.parent_map, pin_ref);
            }

            // No concurrent access: release the lower-level pages now.
            bufmgr::lock_buffer::call(si.buf, bufmgr::BUFFER_LOCK_UNLOCK)?;
            match si.pin {
                Some(p) => drop(p),
                None => drop(original_pin.take().expect("original split page")),
            }
            downlinks.push(si.downlink);
        }

        // SAFETY: owned downlink images, live for the call.
        let dls: Vec<&[u8]> = downlinks
            .iter()
            .map(|d| unsafe { itup_slice(d.as_ptr()) })
            .collect();
        gist_buffering_insert_tuples(
            mcx,
            index,
            heap,
            freespace,
            giststate,
            bb,
            parent_pin,
            level + 1,
            &dls,
            next_downlinkoffnum,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        )?;
    } else {
        unlock_release(buffer)?;
    }

    Ok(placed_to_blk)
}

/// gistBufferingFindCorrectParent: returns the parent page exclusively
/// locked, plus the downlink's offset on it.
fn gist_buffering_find_correct_parent(
    index: &Relation<'_>,
    bb: &BufBuild<'_>,
    childblkno: BlockNumber,
    level: i32,
    parentblkno: BlockNumber,
    downlinkoffnum: OffsetNumber,
) -> PgResult<(BufferPin, OffsetNumber)> {
    let parent = if level > 0 {
        *bb.parent_map.get(&childblkno).unwrap_or_else(|| {
            panic!("could not find parent of block {childblkno} in lookup table")
        })
    } else {
        // A leaf's parent must be supplied by the caller.
        if parentblkno == InvalidBlockNumber {
            panic!("no parent buffer provided of child {childblkno}");
        }
        parentblkno
    };

    let pin = read_buffer(index, parent)?;
    lock(&pin, GIST_EXCLUSIVE)?;
    gistcheckpage(index, &pin)?;

    let found = {
        let page = pin.page();
        let maxoff = page.max_offset_number();

        let unmoved = parent == parentblkno
            && parentblkno != InvalidBlockNumber
            && downlinkoffnum != InvalidOffsetNumber
            && downlinkoffnum <= maxoff
            // SAFETY: page item under our content lock.
            && unsafe { itup_block_number(page_item(&page, downlinkoffnum)) } == childblkno;
        if unmoved {
            Some(downlinkoffnum)
        } else {
            // The downlink moved within this page (the parent map always
            // knows which page it is on).
            (FirstOffsetNumber..=maxoff)
                // SAFETY: page items under our content lock.
                .find(|&off| unsafe { itup_block_number(page_item(&page, off)) } == childblkno)
        }
    };
    match found {
        Some(off) => Ok((pin, off)),
        None => panic!("failed to re-find parent for block {childblkno}"),
    }
}

/// gistProcessEmptyingQueue: drain the emptying stack, cascading tuples to
/// lower-level buffers or leaf pages.
pub fn gist_process_emptying_queue(
    temp: &mut MemoryContext,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    freespace: usize,
    giststate: &mut GistState<'_>,
    bb: &mut BufBuild<'_>,
) -> PgResult<()> {
    while let Some(blkno) = bb.gfbb.buffer_emptying_queue.pop_front() {
        let nb = bb
            .gfbb
            .node_buffers
            .get_mut(&blkno)
            .expect("queued node buffer exists");
        nb.queued_for_emptying = false;
        let level = nb.level;

        // The emptying targets' last pages get loaded; drop earlier loads.
        bb.gfbb.unload_node_buffers()?;

        // Keep popping until a lower-level buffer fills up or this buffer
        // runs empty. If this buffer's page splits meanwhile, the map entry
        // (same block number) carries on as the left half.
        loop {
            let Some(itup) = bb.gfbb.pop_itup(blkno)? else {
                break;
            };
            let stop = {
                let tmcx = temp.mcx();
                gist_process_itup(
                    tmcx, index, heap, freespace, giststate, bb, &itup, blkno, level,
                )?
            };
            if stop {
                break;
            }
            temp.reset();
        }
    }
    Ok(())
}

/// gistEmptyAllBuffers: top-to-bottom final flush; consumes the per-level
/// buffer lists.
pub fn gist_empty_all_buffers(
    temp: &mut MemoryContext,
    index: &Relation<'_>,
    heap: &Relation<'_>,
    freespace: usize,
    giststate: &mut GistState<'_>,
    bb: &mut BufBuild<'_>,
) -> PgResult<()> {
    // Split-created buffers can appear on a level's list mid-flush; a buffer
    // can't refill once fully emptied, so remove entries as they empty.
    for i in (0..bb.gfbb.buffers_on_levels.len()).rev() {
        loop {
            let Some(&blkno) = bb.gfbb.buffers_on_levels[i].front() else {
                break;
            };
            let nb = bb
                .gfbb
                .node_buffers
                .get_mut(&blkno)
                .expect("listed node buffer exists");
            if nb.blocks_count != 0 {
                if !nb.queued_for_emptying {
                    nb.queued_for_emptying = true;
                    bb.gfbb.buffer_emptying_queue.push_front(blkno);
                }
                gist_process_emptying_queue(temp, index, heap, freespace, giststate, bb)?;
            } else {
                bb.gfbb.buffers_on_levels[i].pop_front();
            }
        }
    }
    Ok(())
}

/// gistMemorizeAllDownlinks.
fn memorize_all_downlinks(parent_map: &mut HashMap<BlockNumber, BlockNumber>, parent: &BufferPin) {
    let parentblkno = parent.block_number();
    let page = parent.page();
    debug_assert!(!GistPageIsLeaf(&page));
    let maxoff = page.max_offset_number();
    for off in FirstOffsetNumber..=maxoff {
        // SAFETY: page item under our content lock.
        let childblkno = unsafe { itup_block_number(page_item(&page, off)) };
        parent_map.insert(childblkno, parentblkno);
    }
}
