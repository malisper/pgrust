#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// Every fallible function here returns the project-wide `::types_error::PgResult`
// (== `Result<_, PgError>`). `PgError` is a large owned struct, so the un-boxed
// `Err` variant is large; the un-boxed `PgResult` return type is the
// project-wide error contract these ports must match (matching the sibling
// storage crates), so we accept the large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]
//! Port of the PostgreSQL Free Space Map
//! (`src/backend/storage/freespace/`): `freespace.c`, `fsmpage.c`, and
//! `indexfsm.c`.
//!
//! The FSM tracks the amount of free space on the pages of a relation and lets
//! callers quickly find a page with enough space. It is stored in a dedicated
//! relation fork (`FSM_FORKNUM`) organized as a three- (or four-) level tree of
//! FSM pages; within each page the slots form a binary max-tree of one-byte
//! free-space categories. `indexfsm.c` reuses the same machinery to track
//! whole-page free/used state for index access methods.
//!
//! Every function across the three files is implemented here. A code path may
//! panic because a *callee's crate* (the buffer manager, smgr, xlogutils, ...)
//! isn't installed yet — those calls go through the owner's seam crate; the
//! FSM logic itself (tree addressing, the in-page binary-max-tree search, the
//! category math, the `fsm_set_avail`/`fsm_search`/`fsm_vacuum_page` clock) is
//! all in-crate.
//!
//! # Page model
//!
//! The buffer-resident FSM page body — `(FSMPage) PageGetContents(page)` in C —
//! is round-tripped as the owned [`FSMPageData`]: the buffer-manager seam reads
//! it out (`fsm_buffer_get_page`), the tree algorithm mutates it in-crate, and
//! the seam stores it back (`fsm_buffer_set_page`), all bracketed by the
//! buffer-lock seams exactly where C holds the content lock. No raw `Page`
//! pointer crosses any boundary.

extern crate alloc;

use alloc::format;
use alloc::string::String;

use ::utils_error::elog;

use ::types_error::{PgResult, DEBUG1, ERROR};
use ::fsm::{
    FSMPageData, LeafNodesPerPage, NodesPerPage, NonLeafNodesPerPage, SlotsPerFSMPage,
};
use ::types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, BLCKSZ,
};
use ::rel::Relation;
use ::types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use ::types_storage::{Buffer, BufferIsValid, InvalidBuffer, RelFileLocator};

use bufmgr_seams as bufmgr;
use smgr_seams as smgr;
use xloginsert_seams as xloginsert;
use xlogutils_seams as xlogutils;
use transam_xlog_seams as xlog;
use relcache_seams as relcache;
use miscinit_seams as miscadmin;

// ---------------------------------------------------------------------------
// Module-local constants, mirroring the `#define`s at the top of freespace.c.
// ---------------------------------------------------------------------------

/// `#define FSM_CATEGORIES 256` — we use one byte per page, so free space is
/// bucketed into 256 categories.
pub const FSM_CATEGORIES: usize = 256;

/// `#define FSM_CAT_STEP (BLCKSZ / FSM_CATEGORIES)` — bytes per category step
/// (`32` on an 8 KiB page).
const FSM_CAT_STEP: usize = BLCKSZ / FSM_CATEGORIES;

/// `MaxHeapTupleSize` (`access/htup_details.h`) —
/// `BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData))` = 8160.
const fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}
const SizeOfPageHeaderData: usize = 24;
const SizeofItemIdData: usize = 4;
const MaxHeapTupleSize: usize = BLCKSZ - maxalign(SizeOfPageHeaderData + SizeofItemIdData);

/// `#define MaxFSMRequestSize MaxHeapTupleSize` — the largest request the FSM
/// can represent (category 255).
const MaxFSMRequestSize: usize = MaxHeapTupleSize;

/// `#define FSM_TREE_DEPTH ((SlotsPerFSMPage >= 1626) ? 3 : 4)` — depth of the
/// on-disk tree (3 with the default 8 KiB page).
const FSM_TREE_DEPTH: i32 = if SlotsPerFSMPage >= 1626 { 3 } else { 4 };

/// `#define FSM_ROOT_LEVEL (FSM_TREE_DEPTH - 1)`.
const FSM_ROOT_LEVEL: i32 = FSM_TREE_DEPTH - 1;

/// `#define FSM_BOTTOM_LEVEL 0`.
const FSM_BOTTOM_LEVEL: i32 = 0;

/// Compile-time guards locking these constants to the values the in-crate slot
/// arithmetic depends on (a wrong slot count corrupts free-space tracking).
const _: () = assert!(FSM_CATEGORIES == 256);
const _: () = assert!(SlotsPerFSMPage == LeafNodesPerPage);
const _: () = assert!(NodesPerPage == NonLeafNodesPerPage + LeafNodesPerPage);

/// `typedef struct { int level; int logpageno; } FSMAddress;`
///
/// The internal FSM routines work on a logical addressing scheme. Each level of
/// the tree can be thought of as a separately addressable file. Purely
/// in-crate: this type never crosses a seam boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FSMAddress {
    /// level
    level: i32,
    /// page number within the level
    logpageno: i32,
}

/// `static const FSMAddress FSM_ROOT_ADDRESS = {FSM_ROOT_LEVEL, 0};` — address
/// of the root page.
const FSM_ROOT_ADDRESS: FSMAddress = FSMAddress {
    level: FSM_ROOT_LEVEL,
    logpageno: 0,
};

// ---------------------------------------------------------------------------
// Error helpers.
// ---------------------------------------------------------------------------

/// `elog(ERROR, "invalid FSM request size %zu", needed)`.
fn elog_error<T>(message: String) -> PgResult<T> {
    match elog(ERROR, message) {
        Err(error) => Err(error),
        Ok(()) => unreachable!("ERROR level elog must return an error"),
    }
}

// ---------------------------------------------------------------------------
// `fsmpage.c` — the in-page binary-tree slot store.
//
// In C, fsmpage.c casts a page body to `(FSMPage) PageGetContents(page)` and
// pokes one-byte tree nodes. Here the page body *is* the owned `FSMPageData`
// (`fp_next_slot: i32`, `fp_nodes: Vec<u8>`); these functions index `fp_nodes`
// with the same signed `int` node indices C uses (the navigation arithmetic can
// compute transient negatives), so the slot math matches bit-for-bit.
// ---------------------------------------------------------------------------

/// `#define leftchild(x) (2 * (x) + 1)`.
#[inline]
fn leftchild(x: i32) -> i32 {
    2 * x + 1
}

/// `#define rightchild(x) (2 * (x) + 2)`.
#[inline]
#[allow(dead_code)] // present for parity with the C macro set; C uses `lchild + 1`.
fn rightchild(x: i32) -> i32 {
    2 * x + 2
}

/// `#define parentof(x) (((x) - 1) / 2)`.
#[inline]
fn parentof(x: i32) -> i32 {
    (x - 1) / 2
}

/// `rightneighbor(x)` — find the right neighbor of `x`, wrapping around within
/// the level.
fn rightneighbor(mut x: i32) -> i32 {
    // Move right. This might wrap around, stepping to the leftmost node at the
    // next level.
    x += 1;

    // Check if we stepped to the leftmost node at next level, and correct if so.
    // The leftmost nodes at each level are numbered x = 2^level - 1, so check if
    // (x + 1) is a power of two, using a standard twos-complement-arithmetic
    // trick.
    if ((x + 1) & x) == 0 {
        x = parentof(x);
    }

    x
}

/// Read node `nodeno` of the page's `fp_nodes` array.
#[inline]
fn node_get(fsmpage: &FSMPageData, nodeno: i32) -> u8 {
    fsmpage.fp_nodes[nodeno as usize]
}

/// Write `value` into node `nodeno` of the page's `fp_nodes` array.
#[inline]
fn node_set(fsmpage: &mut FSMPageData, nodeno: i32, value: u8) {
    fsmpage.fp_nodes[nodeno as usize] = value;
}

/// `fsm_set_avail` — set the value of a slot on a page. Returns true if the page
/// was modified. The caller must hold an exclusive lock on the page.
pub fn fsm_set_avail(fsmpage: &mut FSMPageData, slot: i32, value: u8) -> bool {
    let mut nodeno = NonLeafNodesPerPage as i32 + slot;

    debug_assert!(slot < LeafNodesPerPage as i32);

    let mut oldvalue = node_get(fsmpage, nodeno);

    // If the value hasn't changed, we don't need to do anything.
    if oldvalue == value && value <= node_get(fsmpage, 0) {
        return false;
    }

    node_set(fsmpage, nodeno, value);

    // Propagate up, until we hit the root or a node that doesn't need to be
    // updated.
    loop {
        nodeno = parentof(nodeno);
        let lchild = leftchild(nodeno);
        let rchild = lchild + 1;

        let mut newvalue = node_get(fsmpage, lchild);
        if rchild < NodesPerPage as i32 {
            newvalue = newvalue.max(node_get(fsmpage, rchild));
        }

        oldvalue = node_get(fsmpage, nodeno);
        if oldvalue == newvalue {
            break;
        }

        node_set(fsmpage, nodeno, newvalue);

        if nodeno <= 0 {
            break;
        }
    }

    // sanity check: if the new value is (still) higher than the value at the top,
    // the tree is corrupt. If so, rebuild.
    if value > node_get(fsmpage, 0) {
        fsm_rebuild_page(fsmpage);
    }

    true
}

/// `fsm_get_avail` — return the value of a given slot on a page. Since this is a
/// read-only access of a single byte, the page doesn't need to be locked.
pub fn fsm_get_avail(fsmpage: &FSMPageData, slot: i32) -> u8 {
    debug_assert!(slot < LeafNodesPerPage as i32);
    node_get(fsmpage, NonLeafNodesPerPage as i32 + slot)
}

/// `fsm_get_max_avail` — return the value at the root of a page.
pub fn fsm_get_max_avail(fsmpage: &FSMPageData) -> u8 {
    node_get(fsmpage, 0)
}

/// `fsm_search_avail` — search for a slot with category at least `minvalue`.
/// Returns the slot number, or `-1` if none found.
///
/// The caller must hold at least a shared lock on the page; this function can
/// unlock and re-lock the page in exclusive mode if it needs to repair a torn
/// page. `exclusive_lock_held` should be true if the caller already holds an
/// exclusive lock, to avoid extra work. If `advancenext` is false, `fp_next_slot`
/// is set to point to the returned slot; if true, to the slot after the returned
/// slot.
///
/// The page is read out of the buffer at entry, mutated in-crate, and stored
/// back: the round-robin `fp_next_slot` update (and any torn-page rebuild) are
/// written back via `fsm_buffer_set_page`, matching C's in-place page mutation.
pub fn fsm_search_avail(
    buf: Buffer,
    minvalue: u8,
    advancenext: bool,
    mut exclusive_lock_held: bool,
) -> PgResult<i32> {
    let mut fsmpage = bufmgr::fsm_buffer_get_page::call(buf)?;

    'restart: loop {
        // Check the root first, and exit quickly if there's no leaf with enough
        // free space.
        if node_get(&fsmpage, 0) < minvalue {
            return Ok(-1);
        }

        // Start search using fp_next_slot. It's just a hint, so check that it's
        // sane. (This also handles wrapping around when the prior call returned
        // the last slot on the page.)
        let mut target = fsmpage.fp_next_slot;
        if target < 0 || target >= LeafNodesPerPage as i32 {
            target = 0;
        }
        target += NonLeafNodesPerPage as i32;

        // Start the search from the target slot. At every step, move one node to
        // the right, then climb up to the parent. Stop when we reach a node with
        // enough free space (as we must, since the root has enough space).
        let mut nodeno = target;
        while nodeno > 0 {
            if node_get(&fsmpage, nodeno) >= minvalue {
                break;
            }

            // Move to the right, wrapping around on same level if necessary, then
            // climb up.
            nodeno = parentof(rightneighbor(nodeno));
        }

        // We're now at a node with enough free space, somewhere in the middle of
        // the tree. Descend to the bottom, following a path with enough free
        // space, preferring to move left if there's a choice.
        while nodeno < NonLeafNodesPerPage as i32 {
            let mut childnodeno = leftchild(nodeno);

            if childnodeno < NodesPerPage as i32 && node_get(&fsmpage, childnodeno) >= minvalue {
                nodeno = childnodeno;
                continue;
            }
            childnodeno += 1; // point to right child
            if childnodeno < NodesPerPage as i32 && node_get(&fsmpage, childnodeno) >= minvalue {
                nodeno = childnodeno;
            } else {
                // Oops. The parent node promised that either left or right child
                // has enough space, but neither actually did. This can happen in
                // case of a "torn page", IOW if we crashed earlier while writing
                // the page to disk, and only part of the page made it to disk.
                //
                // Fix the corruption and restart.
                let (rlocator, forknum, blknum) = bufmgr::buffer_get_tag::call(buf)?;
                elog(
                    DEBUG1,
                    format!(
                        "fixing corrupt FSM block {}, relation {}/{}/{}",
                        blknum, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber
                    ),
                )?;
                let _ = forknum;

                // make sure we hold an exclusive lock
                if !exclusive_lock_held {
                    bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                    bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
                    exclusive_lock_held = true;
                    // Re-read the page now that we hold the exclusive lock, in case
                    // another backend repaired it while we were unlocked.
                    fsmpage = bufmgr::fsm_buffer_get_page::call(buf)?;
                }
                fsm_rebuild_page(&mut fsmpage);
                bufmgr::fsm_buffer_set_page::call(buf, fsmpage)?;
                bufmgr::mark_buffer_dirty_hint::call(buf, false);
                fsmpage = bufmgr::fsm_buffer_get_page::call(buf)?;
                continue 'restart;
            }
        }

        // We're now at the bottom level, at a node with enough space.
        let slot = nodeno - NonLeafNodesPerPage as i32;

        // Update the next-target pointer. Note that we do this even if we're only
        // holding a shared lock, on the grounds that it's better to use a shared
        // lock and get a garbled next pointer every now and then, than take the
        // concurrency hit of an exclusive lock.
        //
        // Wrap-around is handled at the beginning of this function.
        fsmpage.fp_next_slot = slot + if advancenext { 1 } else { 0 };
        bufmgr::fsm_buffer_set_page::call(buf, fsmpage)?;

        return Ok(slot);
    }
}

/// `fsm_truncate_avail` — set the available space to zero for all slots numbered
/// `>= nslots`. Returns true if the page was modified.
pub fn fsm_truncate_avail(fsmpage: &mut FSMPageData, nslots: i32) -> bool {
    let mut changed = false;

    debug_assert!(nslots >= 0 && nslots < LeafNodesPerPage as i32);

    // Clear all truncated leaf nodes.
    let mut nodeno = NonLeafNodesPerPage as i32 + nslots;
    while nodeno < NodesPerPage as i32 {
        if node_get(fsmpage, nodeno) != 0 {
            changed = true;
        }
        node_set(fsmpage, nodeno, 0);
        nodeno += 1;
    }

    // Fix upper nodes.
    if changed {
        fsm_rebuild_page(fsmpage);
    }

    changed
}

/// `fsm_rebuild_page` — reconstruct the upper levels of a page. Returns true if
/// the page was modified.
pub fn fsm_rebuild_page(fsmpage: &mut FSMPageData) -> bool {
    let mut changed = false;

    // Start from the lowest non-leaf level, at the last node, working our way
    // backwards, through all non-leaf nodes at all levels, up to the root.
    let mut nodeno = NonLeafNodesPerPage as i32 - 1;
    while nodeno >= 0 {
        let lchild = leftchild(nodeno);
        let rchild = lchild + 1;
        let mut newvalue: u8 = 0;

        // The first few nodes we examine might have zero or one child.
        if lchild < NodesPerPage as i32 {
            newvalue = node_get(fsmpage, lchild);
        }

        if rchild < NodesPerPage as i32 {
            newvalue = newvalue.max(node_get(fsmpage, rchild));
        }

        if node_get(fsmpage, nodeno) != newvalue {
            node_set(fsmpage, nodeno, newvalue);
            changed = true;
        }

        nodeno -= 1;
    }

    changed
}

// ---------------------------------------------------------------------------
// `freespace.c` — category conversions.
// ---------------------------------------------------------------------------

/// `fsm_space_avail_to_cat` — return the category corresponding to `avail` bytes
/// of free space (rounding down).
fn fsm_space_avail_to_cat(avail: usize) -> u8 {
    debug_assert!(avail < BLCKSZ);

    if avail >= MaxFSMRequestSize {
        return 255;
    }

    let mut cat = avail / FSM_CAT_STEP;

    // The highest category, 255, is reserved for MaxFSMRequestSize bytes or more.
    if cat > 254 {
        cat = 254;
    }

    cat as u8
}

/// `fsm_space_cat_to_avail` — return the lower bound of the range of free space
/// represented by a given category.
fn fsm_space_cat_to_avail(cat: u8) -> usize {
    // The highest category represents exactly MaxFSMRequestSize bytes.
    if cat == 255 {
        MaxFSMRequestSize
    } else {
        cat as usize * FSM_CAT_STEP
    }
}

/// `fsm_space_needed_to_cat` — which category does a page need to have to
/// accommodate `needed` bytes of data? Rounds up (unlike
/// `fsm_space_avail_to_cat`).
fn fsm_space_needed_to_cat(needed: usize) -> PgResult<u8> {
    // Can't ask for more space than the highest category represents.
    if needed > MaxFSMRequestSize {
        return elog_error(format!("invalid FSM request size {needed}"));
    }

    if needed == 0 {
        return Ok(1);
    }

    // Faithful transcription of the C round-up expression
    // `(needed + FSM_CAT_STEP - 1) / FSM_CAT_STEP`.
    #[allow(clippy::manual_div_ceil)]
    let mut cat = (needed + FSM_CAT_STEP - 1) / FSM_CAT_STEP;

    if cat > 255 {
        cat = 255;
    }

    Ok(cat as u8)
}

// ---------------------------------------------------------------------------
// `freespace.c` — FSM-tree addressing.
// ---------------------------------------------------------------------------

/// `fsm_logical_to_physical` — return the physical block number of an FSM page.
fn fsm_logical_to_physical(addr: FSMAddress) -> BlockNumber {
    // Calculate the logical page number of the first leaf page below the given
    // page. (C uses `int leafno`; the multiply can overflow `int` for very large
    // relations exactly as in C, so use a wrapping i32 to match.)
    let mut leafno: i32 = addr.logpageno;
    for _l in 0..addr.level {
        leafno = leafno.wrapping_mul(SlotsPerFSMPage as i32);
    }

    // Count upper level nodes required to address the leaf page.
    let mut pages: BlockNumber = 0;
    for _l in 0..FSM_TREE_DEPTH {
        pages = pages.wrapping_add((leafno as BlockNumber).wrapping_add(1));
        leafno /= SlotsPerFSMPage as i32;
    }

    // If the page we were asked for wasn't at the bottom level, subtract the
    // additional lower level pages we counted above.
    pages = pages.wrapping_sub(addr.level as BlockNumber);

    // Turn the page count into 0-based block number.
    pages.wrapping_sub(1)
}

/// `fsm_get_location` — return the FSM location corresponding to a given heap
/// block, and the slot within that page.
fn fsm_get_location(heapblk: BlockNumber, slot: &mut u16) -> FSMAddress {
    let addr = FSMAddress {
        level: FSM_BOTTOM_LEVEL,
        logpageno: (heapblk / SlotsPerFSMPage as BlockNumber) as i32,
    };
    *slot = (heapblk % SlotsPerFSMPage as BlockNumber) as u16;

    addr
}

/// `fsm_get_heap_blk` — return the heap block number corresponding to a given
/// location in the FSM.
fn fsm_get_heap_blk(addr: FSMAddress, slot: u16) -> BlockNumber {
    debug_assert!(addr.level == FSM_BOTTOM_LEVEL);
    (addr.logpageno as u32).wrapping_mul(SlotsPerFSMPage as u32) + slot as u32
}

/// `fsm_get_parent` — given a logical address of a child page, get the logical
/// page number of the parent, and the slot within the parent corresponding to the
/// child.
fn fsm_get_parent(child: FSMAddress, slot: &mut u16) -> FSMAddress {
    debug_assert!(child.level < FSM_ROOT_LEVEL);

    let parent = FSMAddress {
        level: child.level + 1,
        logpageno: child.logpageno / SlotsPerFSMPage as i32,
    };
    *slot = (child.logpageno % SlotsPerFSMPage as i32) as u16;

    parent
}

/// `fsm_get_child` — given a logical address of a parent page and a slot number,
/// get the logical address of the corresponding child page.
fn fsm_get_child(parent: FSMAddress, slot: u16) -> FSMAddress {
    debug_assert!(parent.level > FSM_BOTTOM_LEVEL);

    FSMAddress {
        level: parent.level - 1,
        logpageno: parent.logpageno * SlotsPerFSMPage as i32 + slot as i32,
    }
}

// ---------------------------------------------------------------------------
// `freespace.c` — buffer-access workhorses.
// ---------------------------------------------------------------------------

/// `fsm_readbuf` — read an FSM page.
///
/// If the page doesn't exist, `InvalidBuffer` is returned, or if `extend` is
/// true, the FSM file is extended.
fn fsm_readbuf(rel: &Relation<'_>, addr: FSMAddress, extend: bool) -> PgResult<Buffer> {
    let blkno = fsm_logical_to_physical(addr);
    let buf: Buffer;

    // If we haven't cached the size of the FSM yet, check it first. Also recheck
    // if the requested block seems to be past end, since our cached value might be
    // stale. (We send smgr inval messages on truncation, but not on extension.)
    //
    // In C this inspects/populates reln->smgr_cached_nblocks[FSM_FORKNUM]: when
    // the cache is missing or the block looks past-end, it invalidates the cache
    // and re-derives the count from `smgrexists ? smgrnblocks : 0`. The smgr cache
    // lives in the SMgrRelation, so this re-derivation runs over the smgr seams.
    let nblocks = fsm_cached_nblocks(rel, blkno)?;

    // For reading we use ZERO_ON_ERROR mode, and initialize the page if necessary.
    // The FSM information is not accurate anyway, so it's better to clear corrupt
    // pages than error out.
    //
    // We use the same path below to initialize pages when extending the relation,
    // as a concurrent extension can end up with vm_extend() returning an
    // already-initialized page.
    if blkno >= nblocks {
        if extend {
            buf = fsm_extend(rel, blkno + 1)?;
        } else {
            return Ok(InvalidBuffer);
        }
    } else {
        buf = bufmgr::read_buffer_extended_fsm::call(rel, blkno)?;
    }

    // Initializing the page when needed is trickier than it looks, because of the
    // possibility of multiple backends doing this concurrently, and our desire to
    // not uselessly take the buffer lock in the normal path where the page is OK.
    // We must take the lock to initialize the page, so recheck page newness after
    // we have the lock, in case someone else already did it.
    if bufmgr::page_is_new::call(buf)? {
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        if bufmgr::page_is_new::call(buf)? {
            bufmgr::page_init::call(buf)?;
        }
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    }
    Ok(buf)
}

/// `fsm_extend` — ensure that the FSM fork is at least `fsm_nblocks` long,
/// extending it if necessary with empty (all-zero, i.e. no-free-space) pages.
fn fsm_extend(rel: &Relation<'_>, fsm_nblocks: BlockNumber) -> PgResult<Buffer> {
    bufmgr::extend_buffered_rel_to_fsm::call(rel, fsm_nblocks)
}

/// `RelationGetSmgr(rel)->smgr_cached_nblocks[FSM_FORKNUM]` re-derivation from
/// `fsm_readbuf`: when the cache is missing or the requested block looks past
/// end, invalidate and re-read from `smgrexists ? smgrnblocks : 0`. Returns the
/// effective FSM-fork block count to compare `blkno` against.
fn fsm_cached_nblocks(rel: &Relation<'_>, blkno: BlockNumber) -> PgResult<BlockNumber> {
    let rlocator = rel.rd_locator;
    let backend = rel.rd_backend;

    let cached = smgr::smgr_cached_nblocks::call(rlocator, backend, ForkNumber::FSM_FORKNUM);
    if cached == InvalidBlockNumber || blkno >= cached {
        // Invalidate the cache so smgrnblocks asks the kernel.
        if smgr::smgrexists::call(rlocator, backend, ForkNumber::FSM_FORKNUM)? {
            return smgr::smgrnblocks::call(rlocator, backend, ForkNumber::FSM_FORKNUM);
        }
        return Ok(0);
    }
    Ok(cached)
}

/// `fsm_set_and_search` — set the value in a given FSM page and slot.
///
/// If `minValue > 0`, the updated page is also searched for a page with at least
/// `minValue` of free space; if one is found its slot number is returned, `-1`
/// otherwise.
fn fsm_set_and_search(
    rel: &Relation<'_>,
    addr: FSMAddress,
    slot: u16,
    newValue: u8,
    minValue: u8,
) -> PgResult<i32> {
    let mut newslot = -1;

    let buf = fsm_readbuf(rel, addr, true)?;
    bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

    let mut page = bufmgr::fsm_buffer_get_page::call(buf)?;

    let modified = fsm_set_avail(&mut page, slot as i32, newValue);
    // Store the (possibly mutated) page back; the C code mutates the page in place
    // inside the buffer, so the write-back is the idiomatic equivalent regardless
    // of whether fsm_set_avail reported a change (and the search below re-reads it
    // from the buffer).
    bufmgr::fsm_buffer_set_page::call(buf, page)?;
    if modified {
        bufmgr::mark_buffer_dirty_hint::call(buf, false);
    }

    if minValue != 0 {
        // Search while we still hold the lock.
        newslot = fsm_search_avail(buf, minValue, addr.level == FSM_BOTTOM_LEVEL, true)?;
    }

    bufmgr::unlock_release_buffer::call(buf);

    Ok(newslot)
}

/// `fsm_search` — search the tree for a heap page with at least `min_cat` of free
/// space.
fn fsm_search(rel: &Relation<'_>, min_cat: u8) -> PgResult<BlockNumber> {
    let mut restarts = 0;
    let mut addr = FSM_ROOT_ADDRESS;

    loop {
        let slot: i32;
        let mut max_avail: u8 = 0;

        // Read the FSM page.
        let buf = fsm_readbuf(rel, addr, false)?;

        // Search within the page.
        if BufferIsValid(buf) {
            bufmgr::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
            slot = fsm_search_avail(buf, min_cat, addr.level == FSM_BOTTOM_LEVEL, false)?;
            if slot == -1 {
                let page = bufmgr::fsm_buffer_get_page::call(buf)?;
                max_avail = fsm_get_max_avail(&page);
                bufmgr::unlock_release_buffer::call(buf);
            } else {
                // Keep the pin for possible update below.
                bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            }
        } else {
            slot = -1;
        }

        if slot != -1 {
            // Descend the tree, or return the found block if we're at the bottom.
            if addr.level == FSM_BOTTOM_LEVEL {
                let blkno = fsm_get_heap_blk(addr, slot as u16);

                if fsm_does_block_exist(rel, blkno)? {
                    bufmgr::release_buffer::call(buf);
                    return Ok(blkno);
                }

                // Block is past the end of the relation. Update FSM, and restart
                // from root. The usual "advancenext" behavior is pessimal for this
                // rare scenario, since every later slot is unusable in the same
                // way. We could zero all affected slots on the same FSM page, but
                // don't bet on the benefits of that optimization justifying its
                // compiled code bulk.
                bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
                let mut page = bufmgr::fsm_buffer_get_page::call(buf)?;
                fsm_set_avail(&mut page, slot, 0);
                bufmgr::fsm_buffer_set_page::call(buf, page)?;
                bufmgr::mark_buffer_dirty_hint::call(buf, false);
                bufmgr::unlock_release_buffer::call(buf);
                // C uses post-increment `if (restarts++ > 10000)` — compare before
                // bump.
                if restarts > 10000 {
                    // same rationale as below
                    return Ok(InvalidBlockNumber);
                }
                restarts += 1;
                addr = FSM_ROOT_ADDRESS;
            } else {
                bufmgr::release_buffer::call(buf);
            }
            // C runs `addr = fsm_get_child(addr, slot)` unconditionally after the
            // bottom-level past-EOF branch (which reset addr to the root) as well
            // as after the non-bottom descend branch.
            addr = fsm_get_child(addr, slot as u16);
        } else if addr.level == FSM_ROOT_LEVEL {
            // At the root, failure means there's no page with enough free space in
            // the FSM. Give up.
            return Ok(InvalidBlockNumber);
        } else {
            // At lower level, failure can happen if the value in the upper-level
            // node didn't reflect the value on the lower page. Update the upper
            // node, to avoid falling into the same trap again, and start over.
            //
            // There's a race condition here, if another backend updates this page
            // right after we release it, and gets the lock on the parent page
            // before us. We'll then update the parent page with the now stale
            // information we had. It's OK, because it should happen rarely, and
            // will be fixed by the next vacuum.
            let mut parentslot: u16 = 0;
            let parent = fsm_get_parent(addr, &mut parentslot);
            fsm_set_and_search(rel, parent, parentslot, max_avail, 0)?;

            // If the upper pages are badly out of date, we might need to loop quite
            // a few times, updating them as we go. Any inconsistencies should
            // eventually be corrected and the loop should end. Looping indefinitely
            // is nevertheless scary, so provide an emergency valve. C uses
            // post-increment `if (restarts++ > 10000)` — compare before bump.
            if restarts > 10000 {
                return Ok(InvalidBlockNumber);
            }
            restarts += 1;

            // Start search all over from the root.
            addr = FSM_ROOT_ADDRESS;
        }
    }
}

/// `fsm_vacuum_page` — recursive guts of `FreeSpaceMapVacuum`.
///
/// Examine the FSM page indicated by `addr`, as well as its children, updating
/// upper-level nodes that cover the heap block range from `start` to `end - 1`.
/// (It's okay if `end` is beyond the actual end of the map.) Returns the maximum
/// freespace value on this page. If `addr` is past the end of the FSM, set
/// `*eof_p` to true and return 0.
fn fsm_vacuum_page(
    rel: &Relation<'_>,
    addr: FSMAddress,
    start: BlockNumber,
    end: BlockNumber,
    eof_p: &mut bool,
) -> PgResult<u8> {
    // Read the page if it exists, or return EOF.
    let buf = fsm_readbuf(rel, addr, false)?;
    if !BufferIsValid(buf) {
        *eof_p = true;
        return Ok(0);
    } else {
        *eof_p = false;
    }

    let mut page = bufmgr::fsm_buffer_get_page::call(buf)?;

    // If we're above the bottom level, recurse into children, and fix the
    // information stored about them at this level.
    if addr.level > FSM_BOTTOM_LEVEL {
        let mut fsm_start_slot: u16 = 0;
        let mut fsm_end_slot: u16 = 0;
        let mut eof = false;

        // Compute the range of slots we need to update on this page, given the
        // requested range of heap blocks to consider. The first slot to update is
        // the one covering the "start" block, and the last slot is the one
        // covering "end - 1".
        let mut fsm_start = fsm_get_location(start, &mut fsm_start_slot);
        let mut fsm_end = fsm_get_location(end.wrapping_sub(1), &mut fsm_end_slot);

        while fsm_start.level < addr.level {
            fsm_start = fsm_get_parent(fsm_start, &mut fsm_start_slot);
            fsm_end = fsm_get_parent(fsm_end, &mut fsm_end_slot);
        }
        debug_assert!(fsm_start.level == addr.level);

        let start_slot: i32 = match fsm_start.logpageno.cmp(&addr.logpageno) {
            core::cmp::Ordering::Equal => fsm_start_slot as i32,
            core::cmp::Ordering::Greater => SlotsPerFSMPage as i32, // shouldn't get here...
            core::cmp::Ordering::Less => 0,
        };

        let end_slot: i32 = match fsm_end.logpageno.cmp(&addr.logpageno) {
            core::cmp::Ordering::Equal => fsm_end_slot as i32,
            core::cmp::Ordering::Greater => SlotsPerFSMPage as i32 - 1,
            core::cmp::Ordering::Less => -1, // shouldn't get here...
        };

        let mut slot = start_slot;
        while slot <= end_slot {
            miscadmin::check_for_interrupts::call()?;

            // After we hit end-of-file, just clear the rest of the slots.
            let child_avail: u8 = if !eof {
                fsm_vacuum_page(rel, fsm_get_child(addr, slot as u16), start, end, &mut eof)?
            } else {
                0
            };

            // Update information about the child.
            if fsm_get_avail(&page, slot) != child_avail {
                bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
                fsm_set_avail(&mut page, slot, child_avail);
                bufmgr::fsm_buffer_set_page::call(buf, page.clone())?;
                bufmgr::mark_buffer_dirty_hint::call(buf, false);
                bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            }

            slot += 1;
        }
    }

    // Now get the maximum value on the page, to return to caller.
    let max_avail = fsm_get_max_avail(&page);

    // Reset the next slot pointer. This encourages the use of low-numbered pages,
    // increasing the chances that a later vacuum can truncate the relation. We
    // don't bother with a lock here, nor with marking the page dirty if it wasn't
    // already, since this is just a hint.
    //   ((FSMPage) PageGetContents(page))->fp_next_slot = 0;
    page.fp_next_slot = 0;
    bufmgr::fsm_buffer_set_page::call(buf, page)?;

    bufmgr::release_buffer::call(buf);

    Ok(max_avail)
}

/// `fsm_does_block_exist` — check whether a block number is past the end of the
/// relation. This can happen after WAL replay, if the FSM reached disk but
/// newly-extended pages it refers to did not.
fn fsm_does_block_exist(rel: &Relation<'_>, blknumber: BlockNumber) -> PgResult<bool> {
    // If below the cached nblocks, the block surely exists. Otherwise, we face a
    // trade-off. We opt to compare to a fresh nblocks, incurring lseek() overhead.
    // The alternative would be to assume the block does not exist, but that would
    // cause FSM to set zero space available for blocks that main fork extension
    // just recorded.
    let cached_main =
        smgr::smgr_cached_nblocks::call(rel.rd_locator, rel.rd_backend, ForkNumber::MAIN_FORKNUM);
    if BlockNumberIsValid(cached_main) && blknumber < cached_main {
        return Ok(true);
    }
    Ok(blknumber < relcache::relation_get_number_of_blocks::call(rel)?)
}

// ---------------------------------------------------------------------------
// `freespace.c` — public API.
// ---------------------------------------------------------------------------

/// `GetPageWithFreeSpace` — try to find a page in the given relation with at
/// least the specified amount of free space.
///
/// If successful, returns the block number; otherwise `InvalidBlockNumber`.
///
/// The caller must be prepared for the possibility that the returned page will
/// turn out to have too little space available by the time the caller gets a lock
/// on it. In that case, the caller should report the actual amount of free space
/// available and try again (see [`RecordAndGetPageWithFreeSpace`]). If
/// `InvalidBlockNumber` is returned, extend the relation.
pub fn GetPageWithFreeSpace(rel: &Relation<'_>, spaceNeeded: usize) -> PgResult<BlockNumber> {
    let min_cat = fsm_space_needed_to_cat(spaceNeeded)?;

    fsm_search(rel, min_cat)
}

/// `RecordAndGetPageWithFreeSpace` — update info about a page and try again.
///
/// Combines [`RecordPageWithFreeSpace`] + [`GetPageWithFreeSpace`] to save some
/// locking overhead. There's also some effort to return a page close to the old
/// page; if there's a page with enough free space on the same FSM page where the
/// old one is located, it is preferred.
pub fn RecordAndGetPageWithFreeSpace(
    rel: &Relation<'_>,
    oldPage: BlockNumber,
    oldSpaceAvail: usize,
    spaceNeeded: usize,
) -> PgResult<BlockNumber> {
    let old_cat = fsm_space_avail_to_cat(oldSpaceAvail);
    let search_cat = fsm_space_needed_to_cat(spaceNeeded)?;
    let mut slot: u16 = 0;

    // Get the location of the FSM byte representing the heap block.
    let addr = fsm_get_location(oldPage, &mut slot);

    let search_slot = fsm_set_and_search(rel, addr, slot, old_cat, search_cat)?;

    // If fsm_set_and_search found a suitable new block, return that. Otherwise,
    // search as usual.
    if search_slot != -1 {
        let blknum = fsm_get_heap_blk(addr, search_slot as u16);

        // Check that the blknum is actually in the relation. Don't try to update
        // the FSM in that case, just fall back to the other case.
        if fsm_does_block_exist(rel, blknum)? {
            return Ok(blknum);
        }
    }
    fsm_search(rel, search_cat)
}

/// `RecordPageWithFreeSpace` — update info about a page.
///
/// Note that if the new `spaceAvail` value is higher than the old value stored in
/// the FSM, the space might not become visible to searchers until the next
/// [`FreeSpaceMapVacuum`] call, which updates the upper-level pages.
pub fn RecordPageWithFreeSpace(
    rel: &Relation<'_>,
    heapBlk: BlockNumber,
    spaceAvail: usize,
) -> PgResult<()> {
    let new_cat = fsm_space_avail_to_cat(spaceAvail);
    let mut slot: u16 = 0;

    // Get the location of the FSM byte representing the heap block.
    let addr = fsm_get_location(heapBlk, &mut slot);

    fsm_set_and_search(rel, addr, slot, new_cat, 0)?;
    Ok(())
}

/// `XLogRecordPageWithFreeSpace` — like [`RecordPageWithFreeSpace`], for use in
/// WAL replay.
pub fn XLogRecordPageWithFreeSpace(
    rlocator: RelFileLocator,
    heapBlk: BlockNumber,
    spaceAvail: usize,
) -> PgResult<()> {
    let new_cat = fsm_space_avail_to_cat(spaceAvail);
    let mut slot: u16 = 0;

    // Get the location of the FSM byte representing the heap block.
    let addr = fsm_get_location(heapBlk, &mut slot);
    let blkno = fsm_logical_to_physical(addr);

    // If the page doesn't exist already, extend.
    let buf = xlogutils::xlog_read_buffer_extended_fsm::call(rlocator, blkno)?;
    bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

    if bufmgr::page_is_new::call(buf)? {
        bufmgr::page_init::call(buf)?;
    }

    let mut page = bufmgr::fsm_buffer_get_page::call(buf)?;
    let modified = fsm_set_avail(&mut page, slot as i32, new_cat);
    bufmgr::fsm_buffer_set_page::call(buf, page)?;
    if modified {
        bufmgr::mark_buffer_dirty_hint::call(buf, false);
    }
    bufmgr::unlock_release_buffer::call(buf);
    Ok(())
}

/// `GetRecordedFreeSpace` — return the amount of free space on a particular page,
/// according to the FSM.
pub fn GetRecordedFreeSpace(rel: &Relation<'_>, heapBlk: BlockNumber) -> PgResult<usize> {
    let mut slot: u16 = 0;

    // Get the location of the FSM byte representing the heap block.
    let addr = fsm_get_location(heapBlk, &mut slot);

    let buf = fsm_readbuf(rel, addr, false)?;
    if !BufferIsValid(buf) {
        return Ok(0);
    }
    let page = bufmgr::fsm_buffer_get_page::call(buf)?;
    let cat = fsm_get_avail(&page, slot as i32);
    bufmgr::release_buffer::call(buf);

    Ok(fsm_space_cat_to_avail(cat))
}

/// `FreeSpaceMapPrepareTruncateRel` — prepare for truncation of a relation.
///
/// `nblocks` is the new size of the heap. Returns the number of blocks of the new
/// FSM. If it's `InvalidBlockNumber`, there is nothing to truncate; otherwise the
/// caller is responsible for calling `smgrtruncate()` to truncate the FSM pages,
/// and [`FreeSpaceMapVacuumRange`] to update upper-level pages.
pub fn FreeSpaceMapPrepareTruncateRel(
    rel: &Relation<'_>,
    nblocks: BlockNumber,
) -> PgResult<BlockNumber> {
    let new_nfsmblocks: BlockNumber;
    let mut first_removed_slot: u16 = 0;

    // If no FSM has been created yet for this relation, there's nothing to
    // truncate.
    if !smgr::smgrexists::call(rel.rd_locator, rel.rd_backend, ForkNumber::FSM_FORKNUM)? {
        return Ok(InvalidBlockNumber);
    }

    // Get the location in the FSM of the first removed heap block.
    let first_removed_address = fsm_get_location(nblocks, &mut first_removed_slot);

    // Zero out the tail of the last remaining FSM page. If the slot representing
    // the first removed heap block is at a page boundary, as the first slot on the
    // FSM page that first_removed_address points to, we can just truncate that page
    // altogether.
    if first_removed_slot > 0 {
        let buf = fsm_readbuf(rel, first_removed_address, false)?;
        if !BufferIsValid(buf) {
            // nothing to do; the FSM was already smaller
            return Ok(InvalidBlockNumber);
        }
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

        // NO EREPORT(ERROR) from here till changes are logged.
        miscadmin::start_crit_section::call();

        let mut page = bufmgr::fsm_buffer_get_page::call(buf)?;
        fsm_truncate_avail(&mut page, first_removed_slot as i32);
        bufmgr::fsm_buffer_set_page::call(buf, page)?;

        // This change is non-critical, because fsm_does_block_exist() would stop us
        // from returning a truncated-away block. However, since this may remove up
        // to SlotsPerFSMPage slots, it's nice to avoid the cost of that many
        // fsm_does_block_exist() rejections. Use a full MarkBufferDirty(), not
        // MarkBufferDirtyHint().
        bufmgr::mark_buffer_dirty::call(buf);

        // WAL-log like MarkBufferDirtyHint() might have done, just to avoid
        // differing from the rest of the file in this respect. This is optional;
        // see README mention of full page images.
        //
        // A higher-level operation calls us at WAL replay. If we crash before the
        // XLOG_SMGR_TRUNCATE flushes to disk, main fork length has not changed, and
        // our fork remains valid. If we crash after that flush, redo will return
        // here.
        if !xlog::in_recovery::call()
            && relcache::relation_needs_wal::call(rel)
            && xlog::xlog_hint_bit_is_needed::call()
        {
            xloginsert::log_newpage_buffer::call(buf, false)?;
        }

        // END_CRIT_SECTION();
        miscadmin::end_crit_section::call();

        bufmgr::unlock_release_buffer::call(buf);

        new_nfsmblocks = fsm_logical_to_physical(first_removed_address) + 1;
    } else {
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
        if smgr::smgrnblocks::call(rel.rd_locator, rel.rd_backend, ForkNumber::FSM_FORKNUM)?
            <= new_nfsmblocks
        {
            // nothing to do; the FSM was already smaller
            return Ok(InvalidBlockNumber);
        }
    }

    Ok(new_nfsmblocks)
}

/// `FreeSpaceMapVacuum` — update upper-level pages in the rel's FSM.
///
/// We assume that the bottom-level pages have already been updated with new
/// free-space information.
pub fn FreeSpaceMapVacuum(rel: &Relation<'_>) -> PgResult<()> {
    let mut dummy = false;

    // Recursively scan the tree, starting at the root.
    fsm_vacuum_page(rel, FSM_ROOT_ADDRESS, 0, InvalidBlockNumber, &mut dummy)?;
    Ok(())
}

/// `FreeSpaceMapVacuumRange` — update upper-level pages in the rel's FSM.
///
/// As above, but assume that only heap pages between `start` and `end - 1`
/// inclusive have new free-space information, so update only the upper-level slots
/// covering that block range. `end == InvalidBlockNumber` is equivalent to "all
/// the rest of the relation".
pub fn FreeSpaceMapVacuumRange(
    rel: &Relation<'_>,
    start: BlockNumber,
    end: BlockNumber,
) -> PgResult<()> {
    let mut dummy = false;

    // Recursively scan the tree, starting at the root.
    if end > start {
        fsm_vacuum_page(rel, FSM_ROOT_ADDRESS, start, end, &mut dummy)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `indexfsm.c` — whole-page free/used FSM for index access methods.
//
// Same FSM machinery as for heaps, but instead of tracking the amount of free
// space we only track whether pages are completely free or in use, using 0 for
// used pages and (BLCKSZ - 1) for unused.
// ---------------------------------------------------------------------------

/// `GetFreeIndexPage` — return a free page from the FSM.
///
/// As a side effect, the page is marked as used in the FSM.
pub fn GetFreeIndexPage(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    let blkno = GetPageWithFreeSpace(rel, BLCKSZ / 2)?;

    if blkno != InvalidBlockNumber {
        RecordUsedIndexPage(rel, blkno)?;
    }

    Ok(blkno)
}

/// `RecordFreeIndexPage` — mark a page as free in the FSM.
pub fn RecordFreeIndexPage(rel: &Relation<'_>, freeBlock: BlockNumber) -> PgResult<()> {
    RecordPageWithFreeSpace(rel, freeBlock, BLCKSZ - 1)
}

/// `RecordUsedIndexPage` — mark a page as used in the FSM.
pub fn RecordUsedIndexPage(rel: &Relation<'_>, usedBlock: BlockNumber) -> PgResult<()> {
    RecordPageWithFreeSpace(rel, usedBlock, 0)
}

/// `IndexFreeSpaceMapVacuum` — scan and fix any inconsistencies in the FSM.
pub fn IndexFreeSpaceMapVacuum(rel: &Relation<'_>) -> PgResult<()> {
    FreeSpaceMapVacuum(rel)
}

// ---------------------------------------------------------------------------
// Small inline predicates from the storage headers.
// ---------------------------------------------------------------------------

/// `BlockNumberIsValid(blockNumber)` — true unless `InvalidBlockNumber`
/// (block.h).
#[inline]
fn BlockNumberIsValid(blockNumber: BlockNumber) -> bool {
    blockNumber != InvalidBlockNumber
}

// ---------------------------------------------------------------------------
// Seam installation — the inward FSM-for-index seams this crate owns.
// ---------------------------------------------------------------------------

/// Install every seam declared in `backend-storage-freespace-seams` to the real
/// implementations in this crate.
pub fn init_seams() {
    freespace_seams::record_free_index_page::set(|rel, blkno| {
        RecordFreeIndexPage(rel, blkno)
    });
    freespace_seams::index_free_space_map_vacuum::set(|rel| {
        IndexFreeSpaceMapVacuum(rel)
    });
    freespace_seams::get_free_index_page::set(|rel| GetFreeIndexPage(rel));
    freespace_seams::get_page_with_free_space::set(|rel, space_needed| {
        GetPageWithFreeSpace(rel, space_needed)
    });
    freespace_seams::record_page_with_free_space::set(
        |rel, heap_blk, space_avail| RecordPageWithFreeSpace(rel, heap_blk, space_avail),
    );
    freespace_seams::record_and_get_page_with_free_space::set(
        |rel, old_page, old_space_avail, space_needed| {
            RecordAndGetPageWithFreeSpace(rel, old_page, old_space_avail, space_needed)
        },
    );
    freespace_seams::free_space_map_vacuum_range::set(|rel, start, end| {
        FreeSpaceMapVacuumRange(rel, start, end)
    });
    freespace_seams::free_space_map_vacuum::set(|rel| FreeSpaceMapVacuum(rel));

    // --- lazy-vacuum driver FSM calls (vacuumlazy.c; home in vacuumlazy-seams,
    //     freespace.c is their real owner) ---
    use vacuumlazy_seams as vx;
    vx::record_page_with_free_space::set(|rel, heap_blk, space_avail| {
        RecordPageWithFreeSpace(rel, heap_blk, space_avail)
    });
    vx::get_recorded_free_space::set(|rel, heap_blk| GetRecordedFreeSpace(rel, heap_blk));
    vx::free_space_map_vacuum_range::set(|rel, start, end| {
        FreeSpaceMapVacuumRange(rel, start, end)
    });
}
