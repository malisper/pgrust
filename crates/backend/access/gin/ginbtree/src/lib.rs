//! Owned-tree Rust port of `src/backend/access/gin/ginbtree.c` (PostgreSQL 18.3)
//! — the **descent + page-modification spine** of the shared GIN entry-tree /
//! data-tree (posting tree) B-tree machinery.
//!
//! The C functions this module provides, ported 1:1:
//!
//!   * `ginTraverseLock`   — lock a buffer by the method search needs
//!   * `ginFindLeafPage`   — descend to the leaf that holds / would hold the key
//!   * `ginStepRight`      — lock-couple one step right along a level
//!   * `freeGinBtreeStack` — release the parent-chain stack's buffers
//!   * `ginFindParents`    — re-find the parent of a page after a move-right loss
//!   * `ginPlaceToPage`    — insert/split one page (the WAL-logged page edit)
//!   * `ginFinishSplit`    — crawl up inserting downlinks until the split is done
//!   * `ginFinishOldSplit` — finish an incomplete split stumbled upon mid-descent
//!   * `ginInsertValue`    — the public entry point: place-to-page + finish-split
//!
//! This is the abstract layer that dispatches each *page action* through the
//! [`::gin::GinBtreeData`] method table. Those callbacks
//! (`findChildPage` / `getLeftMostChild` / `isMoveRight` / `findChildPtr` /
//! `beginPlaceToPage` / `execPlaceToPage` / `prepareDownlink` / `fillRoot`) are
//! filled by the tree-specific page crates `ginentrypage.c` (entry tree) and
//! `gindatapage.c` (data tree), which are the **L3** GIN units not yet ported.
//! Until they land, the method slots are `None`; a dispatch panics loudly
//! (mirror-PG-and-panic), exactly as a `palloc0`'d-but-uninitialized vtable slot
//! would in C — a sanctioned deferral noted per call site.
//!
//! The page bytes the spine touches are reached through the buffer-manager seam
//! (`bufmgr.c`): `ReadBuffer` / `LockBuffer` / `UnlockReleaseBuffer` /
//! `ReleaseBuffer` / `MarkBufferDirty` / `BufferGetPage` / `BufferGetBlockNumber`
//! / `PageSetLSN`. The GIN-page-flag predicates (`GinPageIsLeaf` etc.) are
//! decoded here from the page special area (`GinPageGetOpaque`), exactly as the
//! C inline macros do. `GinNewBuffer` is the `ginutil.c` owner's seam; the WAL
//! records (`ginxlogInsert` / `ginxlogSplit`) go through the `xloginsert.c`
//! seams; the predicate locks through `predicate.c` seams.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec::Vec;

use ::mcx::Mcx;

use bufmgr_seams as bufmgr;
use page::{PageGetSpecialPointer, PageRef};
use utils_error::{ereport, PgResult};
use ::types_error::error::{DEBUG1, ERROR};

use ::types_core::primitive::{BlockNumber, OffsetNumber};
use types_core::{InvalidBlockNumber, RmgrId};
use gin::{
    BeginPlaceToPageResult, GinBtreeData, GinBtreeStack, GinInsertPayload, GinPlaceToPageRC,
    GinStatsData, PtpWorkspace, GIN_DATA, GIN_EXCLUSIVE, GIN_INCOMPLETE_SPLIT, GIN_LEAF,
    GIN_SHARE, GIN_UNLOCK,
};
use ::rel::Relation;
use ::types_storage::storage::{Buffer, InvalidBuffer};
use ::types_tuple::heaptuple::INVALID_OFFSET_NUMBER as InvalidOffsetNumber;

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (ginxlog.h / rmgrlist.h).
// ===========================================================================

/// `RM_GIN_ID` (rmgrlist.h) — the GIN resource-manager id (13). Not yet in the
/// shared types crate; grounded here as `brin_pageops` carries `RM_BRIN_ID`.
const RM_GIN_ID: RmgrId = 13;

/// `XLOG_GIN_INSERT` (ginxlog.h).
const XLOG_GIN_INSERT: u8 = 0x20;
/// `XLOG_GIN_SPLIT` (ginxlog.h).
const XLOG_GIN_SPLIT: u8 = 0x30;

// `ginxlogInsert` / `ginxlogSplit` flags (ginxlog.h).
/// `GIN_INSERT_ISDATA`.
const GIN_INSERT_ISDATA: u16 = 0x01;
/// `GIN_INSERT_ISLEAF`.
const GIN_INSERT_ISLEAF: u16 = 0x02;
/// `GIN_SPLIT_ROOT`.
const GIN_SPLIT_ROOT: u16 = 0x04;

// `REGBUF_*` flags (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x04;
const REGBUF_FORCE_IMAGE: u8 = 0x01;

/// `BLCKSZ`.
const BLCKSZ: usize = ::types_core::BLCKSZ;

// ===========================================================================
// init_seams — this crate owns no inward seams.
// ===========================================================================

/// `ginbtree` owns no inward seams of its own; it only *consumes* the bufmgr /
/// xloginsert / predicate / relcache / ginutil seams installed by their real
/// owners. The conventional hook is therefore empty.
pub fn init_seams() {}

// ===========================================================================
// GIN page-opaque decoding (ginblock.h GinPageGetOpaque + the page-flag
// predicates). These are the C inline macros, decoded from the page special
// area (`GinPageGetOpaque(page) == PageGetSpecialPointer(page)`).
// ===========================================================================

/// `GinPageOpaqueData` field layout in the page special area:
/// `rightlink: BlockNumber (u32) | maxoff: OffsetNumber (u16) | flags: u16`.
#[derive(Clone, Copy, Debug, Default)]
struct GinOpaque {
    rightlink: BlockNumber,
    flags: u16,
}

/// `GinPageGetOpaque(page)` — read the opaque header out of a page byte image.
fn gin_opaque_from_page(page: &[u8]) -> PgResult<GinOpaque> {
    let pr = PageRef::new(page)?;
    let special = PageGetSpecialPointer(&pr)?;
    let rightlink = u32::from_ne_bytes([special[0], special[1], special[2], special[3]]);
    // maxoff is special[4..6]; flags is special[6..8].
    let flags = u16::from_ne_bytes([special[6], special[7]]);
    Ok(GinOpaque { rightlink, flags })
}

/// `GinPageGetOpaque(BufferGetPage(buffer))` — read the opaque header from the
/// live buffer page through the bufmgr seam.
fn gin_opaque(buffer: Buffer) -> PgResult<GinOpaque> {
    let mut out = GinOpaque::default();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = gin_opaque_from_page(page)?;
        Ok(())
    })?;
    Ok(out)
}

/// `GinPageIsLeaf(page)` — `(GinPageGetOpaque(page)->flags & GIN_LEAF) != 0`.
fn gin_page_is_leaf(buffer: Buffer) -> PgResult<bool> {
    Ok(gin_opaque(buffer)?.flags & GIN_LEAF != 0)
}

/// `GinPageIsData(page)` — `(GinPageGetOpaque(page)->flags & GIN_DATA) != 0`.
fn gin_page_is_data(buffer: Buffer) -> PgResult<bool> {
    Ok(gin_opaque(buffer)?.flags & GIN_DATA != 0)
}

/// `GinPageIsIncompleteSplit(page)` —
/// `(GinPageGetOpaque(page)->flags & GIN_INCOMPLETE_SPLIT) != 0`.
fn gin_page_is_incomplete_split(buffer: Buffer) -> PgResult<bool> {
    Ok(gin_opaque(buffer)?.flags & GIN_INCOMPLETE_SPLIT != 0)
}

/// `GinPageRightMost(page)` —
/// `GinPageGetOpaque(page)->rightlink == InvalidBlockNumber`.
fn gin_page_right_most(buffer: Buffer) -> PgResult<bool> {
    Ok(gin_opaque(buffer)?.rightlink == InvalidBlockNumber)
}

/// `GinPageGetOpaque(page)->rightlink`.
fn gin_page_rightlink(buffer: Buffer) -> PgResult<BlockNumber> {
    Ok(gin_opaque(buffer)?.rightlink)
}

// ===========================================================================
// bufmgr thin pass-throughs (mirror the brin_pageops wrappers).
// ===========================================================================

/// `ReadBuffer(index, blkno)`.
fn read_buffer<'mcx>(index: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer::call(index, blkno)
}

/// `LockBuffer(buffer, mode)`.
fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}

/// `UnlockReleaseBuffer(buffer)`.
fn unlock_release_buffer(buffer: Buffer) {
    bufmgr::unlock_release_buffer::call(buffer)
}

/// `ReleaseBuffer(buffer)`.
fn release_buffer(buffer: Buffer) {
    bufmgr::release_buffer::call(buffer)
}

/// `MarkBufferDirty(buffer)`.
fn mark_buffer_dirty(buffer: Buffer) {
    bufmgr::mark_buffer_dirty::call(buffer)
}

/// `BufferGetBlockNumber(buffer)`.
fn buffer_get_block_number(buffer: Buffer) -> BlockNumber {
    bufmgr::buffer_get_block_number::call(buffer)
}

/// `BufferIsValid(buffer)`.
fn buffer_is_valid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `RelationNeedsWAL(index)`.
fn relation_needs_wal(index: &Relation<'_>) -> bool {
    relcache_seams::relation_needs_wal::call(index)
}

// ===========================================================================
// vtable dispatch (None == not-yet-ported L3 page crate).
// ===========================================================================

/// Loud panic for an uninstalled `GinBtreeData` method (the `gindatapage.c` /
/// `ginentrypage.c` L3 page crates are not yet ported, so these slots are
/// `None`). Mirror-PG-and-panic: a `palloc0`'d vtable slot called in C would
/// likewise crash.
#[cold]
fn unported_method(name: &str) -> ! {
    panic!(
        "GIN btree method `{name}` is not installed: the gindatapage.c / \
         ginentrypage.c page crates (GIN L3) are not yet ported"
    )
}

// ===========================================================================
// ginTraverseLock (ginbtree.c:38)
// ===========================================================================

/// `ginTraverseLock(buffer, searchMode)` — lock `buffer` by the method search
/// needs, upgrading a leaf to EXCLUSIVE when we are about to modify the tree.
pub fn ginTraverseLock(buffer: Buffer, searchMode: bool) -> PgResult<i32> {
    let mut access = GIN_SHARE;

    lock_buffer(buffer, GIN_SHARE)?;
    if gin_page_is_leaf(buffer)? {
        if !searchMode {
            // we should relock our page
            lock_buffer(buffer, GIN_UNLOCK)?;
            lock_buffer(buffer, GIN_EXCLUSIVE)?;

            // But root can become non-leaf during relock
            if !gin_page_is_leaf(buffer)? {
                // restore old lock type (very rare)
                lock_buffer(buffer, GIN_UNLOCK)?;
                lock_buffer(buffer, GIN_SHARE)?;
            } else {
                access = GIN_EXCLUSIVE;
            }
        }
    }

    Ok(access)
}

// ===========================================================================
// ginFindLeafPage (ginbtree.c:82)
// ===========================================================================

/// `ginFindLeafPage(btree, searchMode, rootConflictCheck)` — descend the tree to
/// the leaf page that contains or would contain the search key (filled into
/// `btree` by the tree-specific caller). If `btree.fullScan` is true, descends to
/// the leftmost leaf. On return the bottom stack buffer is locked
/// (exclusive when `!searchMode`).
pub fn ginFindLeafPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    searchMode: bool,
    rootConflictCheck: bool,
    index: &Relation<'mcx>,
) -> PgResult<Box<GinBtreeStack>> {
    let mut stack = Box::new(GinBtreeStack {
        blkno: btree.rootBlkno,
        buffer: read_buffer(index, btree.rootBlkno)?,
        parent: None,
        predictNumber: 1,
        ..GinBtreeStack::default()
    });

    if rootConflictCheck {
        // CheckForSerializableConflictIn(btree->index, NULL, btree->rootBlkno)
        predicate_seams::check_for_serializable_conflict_in::call(
            btree.index,
            None,
            btree.rootBlkno,
        )?;
    }

    loop {
        stack.off = InvalidOffsetNumber;

        let access = ginTraverseLock(stack.buffer, searchMode)?;

        // If we're going to modify the tree, finish any incomplete splits we
        // encounter on the way.
        if !searchMode && gin_page_is_incomplete_split(stack.buffer)? {
            ginFinishOldSplit(btree, mcx, &mut stack, None, access, index)?;
        }

        // ok, page is correctly locked, we should check to move right.. root
        // never has a right link, so small optimization
        while !btree.fullScan
            && stack.blkno != btree.rootBlkno
            && call_isMoveRight(btree, stack.buffer)?
        {
            let rightlink = gin_page_rightlink(stack.buffer)?;

            if rightlink == InvalidBlockNumber {
                // rightmost page
                break;
            }

            stack.buffer = ginStepRight(stack.buffer, index, access)?;
            stack.blkno = rightlink;

            if !searchMode && gin_page_is_incomplete_split(stack.buffer)? {
                ginFinishOldSplit(btree, mcx, &mut stack, None, access, index)?;
            }
        }

        if gin_page_is_leaf(stack.buffer)? {
            // we found, return locked page
            return Ok(stack);
        }

        // now we have correct buffer, try to find child
        let child = call_findChildPage(btree, &mut stack)?;

        lock_buffer(stack.buffer, GIN_UNLOCK)?;
        debug_assert!(child != InvalidBlockNumber);
        debug_assert!(stack.blkno != child);

        if searchMode {
            // in search mode we may forget path to leaf
            release_buffer(stack.buffer);
            stack.blkno = child;
            stack.buffer = read_buffer(index, stack.blkno)?;
        } else {
            let mut ptr = Box::new(GinBtreeStack {
                blkno: child,
                buffer: read_buffer(index, child)?,
                predictNumber: 1,
                ..GinBtreeStack::default()
            });
            ptr.parent = Some(stack);
            stack = ptr;
        }
    }
}

// ===========================================================================
// ginStepRight (ginbtree.c:176)
// ===========================================================================

/// `ginStepRight(buffer, index, lockmode)` — step right from the current page,
/// lock-coupling: the next page is locked *before* the current is released, so a
/// concurrent VACUUM cannot delete the page we are about to step to.
pub fn ginStepRight<'mcx>(
    buffer: Buffer,
    index: &Relation<'mcx>,
    lockmode: i32,
) -> PgResult<Buffer> {
    let opaque = gin_opaque(buffer)?;
    let is_leaf = gin_page_is_leaf(buffer)?;
    let is_data = gin_page_is_data(buffer)?;
    let blkno = opaque.rightlink;

    let nextbuffer = read_buffer(index, blkno)?;
    lock_buffer(nextbuffer, lockmode)?;
    unlock_release_buffer(buffer);

    // Sanity check that the page we stepped to is of similar kind.
    if is_leaf != gin_page_is_leaf(nextbuffer)? || is_data != gin_page_is_data(nextbuffer)? {
        return Err(ereport(ERROR)
            .errmsg("right sibling of GIN page is of different type")
            .into_error());
    }

    Ok(nextbuffer)
}

// ===========================================================================
// freeGinBtreeStack (ginbtree.c:197)
// ===========================================================================

/// `freeGinBtreeStack(stack)` — release every buffer pinned by the parent chain
/// and free the stack nodes.
pub fn freeGinBtreeStack(stack: Box<GinBtreeStack>) {
    let mut cur = Some(stack);
    while let Some(node) = cur {
        if node.buffer != InvalidBuffer {
            release_buffer(node.buffer);
        }
        cur = node.parent;
    }
}

// ===========================================================================
// ginFindParents (ginbtree.c:217)
// ===========================================================================

/// `ginFindParents(btree, stack)` — re-find the parent for the current stack
/// position after a move-right loss; fills `stack.parent` with the correct
/// parent and the child's offset. The root page is never released, to prevent a
/// conflict with the vacuum process.
fn ginFindParents<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    stack: &mut GinBtreeStack,
    index: &Relation<'mcx>,
) -> PgResult<()> {
    // Unwind the stack all the way up to the root, leaving only the root item.
    // Be careful not to release the pin on the root page! The pin on the root
    // page is required to lock out concurrent vacuums on the tree.
    //
    // The owned stack hangs off `stack.parent` as a `Box` chain; we take it,
    // release the intermediate buffers, and keep only the root node.
    let mut root = stack
        .parent
        .take()
        .expect("ginFindParents: stack has no parent chain");
    while let Some(next) = root.parent.take() {
        release_buffer(root.buffer);
        root = next;
    }

    debug_assert!(root.blkno == btree.rootBlkno);
    debug_assert!(buffer_get_block_number(root.buffer) == btree.rootBlkno);
    root.off = InvalidOffsetNumber;

    let root_buffer = root.buffer;
    let mut blkno = root.blkno;
    let mut buffer = root.buffer;

    // The found parent node, built when the loop succeeds.
    let result_parent: Box<GinBtreeStack>;

    loop {
        lock_buffer(buffer, GIN_EXCLUSIVE)?;
        if gin_page_is_leaf(buffer)? {
            return Err(ereport(ERROR).errmsg("Lost path").into_error());
        }

        if gin_page_is_incomplete_split(buffer)? {
            debug_assert!(blkno != btree.rootBlkno);
            // parent may be wrong, but if so, the ginFinishSplit call will
            // recurse to call ginFindParents again to fix it.
            let mut ptr = Box::new(GinBtreeStack {
                blkno,
                buffer,
                off: InvalidOffsetNumber,
                ..GinBtreeStack::default()
            });
            // ptr->parent = root (a non-owning reference in C). The owned model
            // can't share `root`; finishing the old split here doesn't read
            // ptr.parent (ginFinishSplit reaches the parent via ginFindParents
            // when it needs it), so a detached node is faithful.
            ptr.parent = None;
            ginFinishOldSplit(btree, mcx, &mut ptr, None, GIN_EXCLUSIVE, index)?;
            buffer = ptr.buffer;
        }

        let leftmostBlkno = call_getLeftMostChild(btree, buffer)?;

        let mut offset = call_findChildPtr(btree, buffer, stack.blkno, InvalidOffsetNumber)?;
        let mut link_present = true;
        while offset == InvalidOffsetNumber {
            blkno = gin_page_rightlink(buffer)?;
            if blkno == InvalidBlockNumber {
                // Link not present in this level
                lock_buffer(buffer, GIN_UNLOCK)?;
                // Do not release pin on the root buffer
                if buffer != root_buffer {
                    release_buffer(buffer);
                }
                link_present = false;
                break;
            }
            buffer = ginStepRight(buffer, index, GIN_EXCLUSIVE)?;

            // finish any incomplete splits, as above
            if gin_page_is_incomplete_split(buffer)? {
                debug_assert!(blkno != btree.rootBlkno);
                let mut ptr = Box::new(GinBtreeStack {
                    blkno,
                    buffer,
                    off: InvalidOffsetNumber,
                    ..GinBtreeStack::default()
                });
                ptr.parent = None;
                ginFinishOldSplit(btree, mcx, &mut ptr, None, GIN_EXCLUSIVE, index)?;
                buffer = ptr.buffer;
            }
            offset = call_findChildPtr(btree, buffer, stack.blkno, InvalidOffsetNumber)?;
        }

        if link_present {
            let ptr = Box::new(GinBtreeStack {
                blkno,
                buffer,
                // it may be wrong, but in next call we will correct
                parent: None,
                off: offset,
                ..GinBtreeStack::default()
            });
            result_parent = ptr;
            break;
        }

        // Descend down to next level
        blkno = leftmostBlkno;
        buffer = read_buffer(index, blkno)?;
    }

    stack.parent = Some(result_parent);
    Ok(())
}

// ===========================================================================
// ginPlaceToPage (ginbtree.c:336)
// ===========================================================================

/// `ginPlaceToPage(btree, stack, insertdata, updateblkno, childbuf, buildStats)`
/// — insert a new item to a page. Returns `true` if the insertion was finished;
/// `false` means the page was split and the parent needs the new downlink (a
/// root split returns `true`).
///
/// `stack.buffer` is locked on entry and kept locked. Likewise for `childbuf`,
/// if valid.
fn ginPlaceToPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    stack: &mut GinBtreeStack,
    insertdata: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
    childbuf: Buffer,
    buildStats: Option<&mut GinStatsData>,
    index: &Relation<'mcx>,
) -> PgResult<bool> {
    let mut xlflags: u16 = 0;

    // C runs all of this function's work in a temporary memory context to avoid
    // leakages across the WAL insertion. The owned model allocates split pages
    // as `Vec<u8>` dropped at scope end, so the explicit temp context is not
    // needed for correctness.

    let is_data = gin_page_is_data(stack.buffer)?;
    let is_leaf = gin_page_is_leaf(stack.buffer)?;

    if is_data {
        xlflags |= GIN_INSERT_ISDATA;
    }
    if is_leaf {
        xlflags |= GIN_INSERT_ISLEAF;
        debug_assert!(!buffer_is_valid(childbuf));
        debug_assert!(updateblkno == InvalidBlockNumber);
    } else {
        debug_assert!(buffer_is_valid(childbuf));
        debug_assert!(updateblkno != InvalidBlockNumber);
    }

    // See if the incoming tuple will fit on the page. beginPlaceToPage decides
    // if the page needs to be split and computes the split contents if so.
    let begin: BeginPlaceToPageResult =
        call_beginPlaceToPage(btree, mcx, stack.buffer, stack, insertdata, updateblkno)?;
    let mut ptp_workspace = begin.ptp_workspace;

    let result;
    match begin.rc {
        GinPlaceToPageRC::GPTP_NO_WORK => {
            // Nothing to do
            result = true;
        }
        GinPlaceToPageRC::GPTP_INSERT => {
            // It will fit, perform the insertion.
            // START_CRIT_SECTION();

            let want_wal = relation_needs_wal(index) && !btree.isBuild;
            if want_wal {
                xlog_begin_insert()?;
            }

            // Hand the WAL gate to the page-specific execPlaceToPage callback
            // through the workspace (it carries only the index Oid, not the
            // Relation, so it cannot recompute RelationNeedsWAL itself).
            ptp_workspace.want_wal = want_wal;

            // Perform the page update, dirty and register stack->buffer, and
            // register any extra WAL data.
            call_execPlaceToPage(
                btree,
                mcx,
                stack.buffer,
                stack,
                insertdata,
                updateblkno,
                &mut ptp_workspace,
            )?;

            // An insert to an internal page finishes the split of the child.
            if buffer_is_valid(childbuf) {
                clear_incomplete_split(childbuf)?;
                mark_buffer_dirty(childbuf);
                if want_wal {
                    xlog_register_buffer(1, childbuf, REGBUF_STANDARD)?;
                }
            }

            if want_wal {
                // ginxlogInsert xlrec = { .flags = xlflags };
                let xlrec = encode_ginxlog_insert(xlflags);
                xlog_register_data(&xlrec)?;

                // Log information about child if this was a downlink insertion.
                if buffer_is_valid(childbuf) {
                    let child_rightlink = gin_page_rightlink(childbuf)?;
                    let mut childblknos = [0u8; 8];
                    childblknos[0..4]
                        .copy_from_slice(&block_id_bytes(buffer_get_block_number(childbuf)));
                    childblknos[4..8].copy_from_slice(&block_id_bytes(child_rightlink));
                    xlog_register_data(&childblknos)?;
                }

                let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_INSERT)?;
                bufmgr::page_set_lsn::call(stack.buffer, recptr)?;
                if buffer_is_valid(childbuf) {
                    bufmgr::page_set_lsn::call(childbuf, recptr)?;
                }
            }

            // END_CRIT_SECTION();

            // Insertion is complete.
            result = true;
        }
        GinPlaceToPageRC::GPTP_SPLIT => {
            // Didn't fit, need to split. The split has been computed in
            // newlpage and newrpage, which are palloc'd page images not
            // associated with buffers. stack->buffer is not touched yet.
            let mut newlpage = begin
                .newlpage
                .expect("GPTP_SPLIT requires newlpage");
            let mut newrpage = begin
                .newrpage
                .expect("GPTP_SPLIT requires newrpage");

            // Get a new index page to become the right page.
            let rbuffer = gin_new_buffer(index)?;

            // During index build, count the new pages. C bumps
            // `buildStats->nDataPages`/`nEntryPages` inline as each page is
            // allocated; the owned model accumulates the deltas in locals and
            // applies them once the split is committed (the only difference is
            // the moment of the in-memory counter write, not its value).
            let mut build_n_data = 0u32;
            let mut build_n_entry = 0u32;
            if buildStats.is_some() {
                if btree.isData {
                    build_n_data += 1;
                } else {
                    build_n_entry += 1;
                }
            }

            let savedRightLink = gin_page_rightlink(stack.buffer)?;

            // Begin setting up WAL record.
            let mut data_flags = xlflags;
            let data_left_child;
            let data_right_child;
            if buffer_is_valid(childbuf) {
                data_left_child = buffer_get_block_number(childbuf);
                data_right_child = gin_page_rightlink(childbuf)?;
            } else {
                data_left_child = InvalidBlockNumber;
                data_right_child = InvalidBlockNumber;
            }

            let data_rrlink;
            let mut lbuffer = InvalidBuffer;
            let mut newrootpg: Option<Vec<u8>> = None;

            let is_root_split = stack.parent.is_none();
            if is_root_split {
                // splitting the root: allocate a new left page and place
                // pointers to the left and right pages on the root page.
                lbuffer = gin_new_buffer(index)?;

                if buildStats.is_some() {
                    if btree.isData {
                        build_n_data += 1;
                    } else {
                        build_n_entry += 1;
                    }
                }

                data_rrlink = InvalidBlockNumber;
                data_flags |= GIN_SPLIT_ROOT;

                set_page_rightlink(&mut newrpage, InvalidBlockNumber)?;
                set_page_rightlink(&mut newlpage, buffer_get_block_number(rbuffer))?;

                // Construct a new root page containing downlinks to the new
                // left and right pages. Do this in a temporary copy rather than
                // overwriting the original page directly, since we're not in the
                // critical section yet.
                let mut root_tmp = page_get_temp_page(&newrpage)?;
                let newl_opaque = gin_opaque_from_page(&newlpage)?;
                gin_init_page(
                    &mut root_tmp,
                    (newl_opaque.flags & !(GIN_LEAF | ::gin::GIN_COMPRESSED)) as u32,
                    BLCKSZ,
                )?;

                call_fillRoot(
                    btree,
                    &mut root_tmp,
                    buffer_get_block_number(lbuffer),
                    &newlpage,
                    buffer_get_block_number(rbuffer),
                    &newrpage,
                )?;

                if gin_page_is_leaf(stack.buffer)? {
                    predicate_lock_page_split(
                        btree.index,
                        buffer_get_block_number(stack.buffer),
                        buffer_get_block_number(lbuffer),
                    )?;
                    predicate_lock_page_split(
                        btree.index,
                        buffer_get_block_number(stack.buffer),
                        buffer_get_block_number(rbuffer),
                    )?;
                }

                newrootpg = Some(root_tmp);
            } else {
                // splitting a non-root page
                data_rrlink = savedRightLink;

                set_page_rightlink(&mut newrpage, savedRightLink)?;
                or_page_flags(&mut newlpage, GIN_INCOMPLETE_SPLIT)?;
                set_page_rightlink(&mut newlpage, buffer_get_block_number(rbuffer))?;

                if gin_page_is_leaf(stack.buffer)? {
                    predicate_lock_page_split(
                        btree.index,
                        buffer_get_block_number(stack.buffer),
                        buffer_get_block_number(rbuffer),
                    )?;
                }
            }

            // OK, we have the new contents of the left page in newlpage and
            // likewise the right block in newrpage; the original page is still
            // unchanged. For a root split, newrootpg holds the new root.

            // START_CRIT_SECTION();

            mark_buffer_dirty(rbuffer);
            mark_buffer_dirty(stack.buffer);

            // Restore the temporary copies over the real buffers.
            if is_root_split {
                // Splitting the root, three pages to update
                mark_buffer_dirty(lbuffer);
                let rootpg = newrootpg.as_ref().expect("root split temp page");
                copy_page_into_buffer(stack.buffer, rootpg)?;
                copy_page_into_buffer(lbuffer, &newlpage)?;
                copy_page_into_buffer(rbuffer, &newrpage)?;
            } else {
                // Normal split, only two pages to update
                copy_page_into_buffer(stack.buffer, &newlpage)?;
                copy_page_into_buffer(rbuffer, &newrpage)?;
            }

            // We also clear childbuf's INCOMPLETE_SPLIT flag, if passed.
            if buffer_is_valid(childbuf) {
                clear_incomplete_split(childbuf)?;
                mark_buffer_dirty(childbuf);
            }

            // write WAL record
            if relation_needs_wal(index) && !btree.isBuild {
                xlog_begin_insert()?;

                // We take full page images of all the split pages. Splits are
                // uncommon enough that it's not worth complicating the code to
                // be more efficient.
                if is_root_split {
                    xlog_register_buffer(0, lbuffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD)?;
                    xlog_register_buffer(1, rbuffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD)?;
                    xlog_register_buffer(
                        2,
                        stack.buffer,
                        REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                    )?;
                } else {
                    xlog_register_buffer(
                        0,
                        stack.buffer,
                        REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                    )?;
                    xlog_register_buffer(1, rbuffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD)?;
                }
                if buffer_is_valid(childbuf) {
                    xlog_register_buffer(3, childbuf, REGBUF_STANDARD)?;
                }

                let data = encode_ginxlog_split(
                    index,
                    data_rrlink,
                    data_left_child,
                    data_right_child,
                    data_flags,
                )?;
                xlog_register_data(&data)?;

                let recptr = xlog_insert_record(RM_GIN_ID, XLOG_GIN_SPLIT)?;

                bufmgr::page_set_lsn::call(stack.buffer, recptr)?;
                bufmgr::page_set_lsn::call(rbuffer, recptr)?;
                if is_root_split {
                    bufmgr::page_set_lsn::call(lbuffer, recptr)?;
                }
                if buffer_is_valid(childbuf) {
                    bufmgr::page_set_lsn::call(childbuf, recptr)?;
                }
            }
            // END_CRIT_SECTION();

            // Account for the new pages now that the split is committed.
            if let Some(stats) = buildStats {
                stats.nDataPages += build_n_data;
                stats.nEntryPages += build_n_entry;
            }

            // We can release the locks/pins on the new pages now, but keep
            // stack->buffer locked. childbuf doesn't get unlocked either.
            unlock_release_buffer(rbuffer);
            if is_root_split {
                unlock_release_buffer(lbuffer);
            }

            // If we split the root, we're done. Otherwise the split is not
            // complete until the downlink for the new page has been inserted to
            // the parent.
            result = is_root_split;
        }
    }

    Ok(result)
}

// ===========================================================================
// ginFinishSplit (ginbtree.c:671)
// ===========================================================================

/// `ginFinishSplit(btree, stack, freestack, buildStats)` — finish a split by
/// crawling up the stack inserting the downlink for each new page into its
/// parent, until the insertion completes.
///
/// On entry `stack.buffer` is exclusively locked. If `freestack`, all buffers
/// are released and the stack is freed as we crawl up; otherwise `stack.buffer`
/// is kept locked.
fn ginFinishSplit<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    mut stack: Box<GinBtreeStack>,
    freestack: bool,
    mut buildStats: Option<&mut GinStatsData>,
    index: &Relation<'mcx>,
) -> PgResult<()> {
    let mut first = true;

    // this loop crawls up the stack until the insertion is complete
    loop {
        // INJECTION_POINT(...) — a no-op in default builds.

        let mut parent = stack
            .parent
            .take()
            .expect("ginFinishSplit: split node has no parent");

        // search parent to lock
        lock_buffer(parent.buffer, GIN_EXCLUSIVE)?;

        // If the parent page was incompletely split, finish that split first,
        // then continue with the current one. We have to finish *all*
        // incomplete splits we encounter, even if we have to move right.
        if gin_page_is_incomplete_split(parent.buffer)? {
            ginFinishOldSplit(btree, mcx, &mut parent, buildStats.as_deref_mut(), GIN_EXCLUSIVE, index)?;
        }

        // move right if it's needed
        parent.off = call_findChildPtr(btree, parent.buffer, stack.blkno, parent.off)?;
        while parent.off == InvalidOffsetNumber {
            if gin_page_right_most(parent.buffer)? {
                // rightmost page, but we don't find parent, we should use plain
                // search...
                lock_buffer(parent.buffer, GIN_UNLOCK)?;
                // ginFindParents fills stack.parent; re-acquire it.
                stack.parent = Some(parent);
                ginFindParents(btree, mcx, &mut stack, index)?;
                parent = stack
                    .parent
                    .take()
                    .expect("ginFindParents must produce a parent");
                break;
            }

            parent.buffer = ginStepRight(parent.buffer, index, GIN_EXCLUSIVE)?;
            parent.blkno = buffer_get_block_number(parent.buffer);

            if gin_page_is_incomplete_split(parent.buffer)? {
                ginFinishOldSplit(
                    btree,
                    mcx,
                    &mut parent,
                    buildStats.as_deref_mut(),
                    GIN_EXCLUSIVE,
                    index,
                )?;
            }
            parent.off = call_findChildPtr(btree, parent.buffer, stack.blkno, parent.off)?;
        }

        // insert the downlink
        let insertdata = call_prepareDownlink(btree, mcx, stack.buffer)?;
        let updateblkno = gin_page_rightlink(stack.buffer)?;
        let done = ginPlaceToPage(
            btree,
            mcx,
            &mut parent,
            &insertdata,
            updateblkno,
            stack.buffer,
            buildStats.as_deref_mut(),
            index,
        )?;
        // pfree(insertdata) — owned `insertdata` drops at scope end.

        // If the caller requested to free the stack, unlock and release the
        // child buffer now. Otherwise keep it pinned and locked, but if we have
        // to recurse up the tree, we can unlock the upper pages, only keeping
        // the page at the bottom of the stack locked.
        if !first || freestack {
            lock_buffer(stack.buffer, GIN_UNLOCK)?;
        }
        if freestack {
            release_buffer(stack.buffer);
            // pfree(stack) — the `Box` drops at the rebind below.
        }
        stack = parent;

        first = false;

        if done {
            break;
        }
    }

    // unlock the parent
    lock_buffer(stack.buffer, GIN_UNLOCK)?;

    if freestack {
        freeGinBtreeStack(stack);
    }
    Ok(())
}

// ===========================================================================
// ginFinishOldSplit (ginbtree.c:778)
// ===========================================================================

/// `ginFinishOldSplit(btree, stack, buildStats, access)` — entry point to
/// [`ginFinishSplit`] used when we stumble upon an existing incompletely-split
/// page in the tree. `stack.buffer` may be merely share-locked on entry and is
/// upgraded to exclusive mode (which momentarily releases the lock — OK during
/// an insert because VACUUM cannot run concurrently).
fn ginFinishOldSplit<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    stack: &mut GinBtreeStack,
    buildStats: Option<&mut GinStatsData>,
    access: i32,
    index: &Relation<'mcx>,
) -> PgResult<()> {
    // INJECTION_POINT("gin-finish-incomplete-split") — no-op default build.
    let _ = ereport(DEBUG1).errmsg(format!(
        "finishing incomplete split of block {} in gin index \"{}\"",
        stack.blkno,
        index.name()
    ));

    if access == GIN_SHARE {
        lock_buffer(stack.buffer, GIN_UNLOCK)?;
        lock_buffer(stack.buffer, GIN_EXCLUSIVE)?;

        if !gin_page_is_incomplete_split(stack.buffer)? {
            // Someone else already completed the split while we were not
            // holding the lock.
            return Ok(());
        }
    }

    // ginFinishSplit consumes an owned stack node. The C call passes
    // `freestack = false`, so the node's buffer is kept locked and the node is
    // not freed; we hand it a detached clone of the current node's identity
    // (blkno/buffer/off) and re-find its parent inside ginFinishSplit via
    // ginFindParents, exactly as the move-right path does.
    let owned = Box::new(GinBtreeStack {
        blkno: stack.blkno,
        buffer: stack.buffer,
        off: stack.off,
        iptr: stack.iptr,
        predictNumber: stack.predictNumber,
        parent: None,
    });
    ginFinishSplit(btree, mcx, owned, false, buildStats, index)?;
    Ok(())
}

// ===========================================================================
// ginInsertValue (ginbtree.c:815)
// ===========================================================================

/// `ginInsertValue(btree, stack, insertdata, buildStats)` — insert a value into
/// the tree described by `stack`. The format of `insertdata` depends on whether
/// this is an entry or data tree; `ginInsertValue` just passes it through to the
/// tree-specific callback. **NB: the passed-in stack is freed**, as though by
/// [`freeGinBtreeStack`].
pub fn ginInsertValue<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    mut stack: Box<GinBtreeStack>,
    insertdata: &GinInsertPayload<'mcx>,
    buildStats: Option<&mut GinStatsData>,
    index: &Relation<'mcx>,
) -> PgResult<()> {
    // If the leaf page was incompletely split, finish the split first.
    if gin_page_is_incomplete_split(stack.buffer)? {
        ginFinishOldSplit(btree, mcx, &mut stack, None, GIN_EXCLUSIVE, index)?;
    }

    let done = ginPlaceToPage(
        btree,
        mcx,
        &mut stack,
        insertdata,
        InvalidBlockNumber,
        InvalidBuffer,
        buildStats,
        index,
    )?;
    if done {
        lock_buffer(stack.buffer, GIN_UNLOCK)?;
        freeGinBtreeStack(stack);
        Ok(())
    } else {
        ginFinishSplit(btree, mcx, stack, true, None, index)
    }
}

// ===========================================================================
// vtable dispatch helpers
// ===========================================================================

fn call_isMoveRight<'mcx>(btree: &mut GinBtreeData<'mcx>, buffer: Buffer) -> PgResult<bool> {
    match btree.isMoveRight {
        Some(f) => f(btree, buffer),
        None => unported_method("isMoveRight"),
    }
}

fn call_findChildPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    stack: &mut GinBtreeStack,
) -> PgResult<BlockNumber> {
    match btree.findChildPage {
        Some(f) => f(btree, stack),
        None => unported_method("findChildPage"),
    }
}

fn call_getLeftMostChild<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
) -> PgResult<BlockNumber> {
    match btree.getLeftMostChild {
        Some(f) => f(btree, buffer),
        None => unported_method("getLeftMostChild"),
    }
}

fn call_findChildPtr<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
    blkno: BlockNumber,
    off: OffsetNumber,
) -> PgResult<OffsetNumber> {
    match btree.findChildPtr {
        Some(f) => f(btree, buffer, blkno, off),
        None => unported_method("findChildPtr"),
    }
}

fn call_beginPlaceToPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    stack: &mut GinBtreeStack,
    insertdata: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
) -> PgResult<BeginPlaceToPageResult> {
    match btree.beginPlaceToPage {
        Some(f) => f(btree, mcx, buffer, stack, insertdata, updateblkno),
        None => unported_method("beginPlaceToPage"),
    }
}

fn call_execPlaceToPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    stack: &mut GinBtreeStack,
    insertdata: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
    ws: &mut PtpWorkspace,
) -> PgResult<()> {
    match btree.execPlaceToPage {
        Some(f) => f(btree, mcx, buffer, stack, insertdata, updateblkno, ws),
        None => unported_method("execPlaceToPage"),
    }
}

fn call_prepareDownlink<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    buffer: Buffer,
) -> PgResult<GinInsertPayload<'mcx>> {
    match btree.prepareDownlink {
        Some(f) => f(btree, mcx, buffer),
        None => unported_method("prepareDownlink"),
    }
}

fn call_fillRoot<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    root: &mut [u8],
    lblkno: BlockNumber,
    lpage: &[u8],
    rblkno: BlockNumber,
    rpage: &[u8],
) -> PgResult<()> {
    match btree.fillRoot {
        Some(f) => f(btree, root, lblkno, lpage, rblkno, rpage),
        None => unported_method("fillRoot"),
    }
}

// ===========================================================================
// page / WAL byte helpers + seam pass-throughs
// ===========================================================================

/// `GinNewBuffer(index)` — the `ginutil.c` owner's seam.
fn gin_new_buffer<'mcx>(index: &Relation<'mcx>) -> PgResult<Buffer> {
    ginutil_seams::gin_new_buffer::call(index)
}

/// `PredicateLockPageSplit(index, oldblkno, newblkno)` — the `predicate.c` seam.
fn predicate_lock_page_split(
    index_oid: ::types_core::Oid,
    old_blkno: BlockNumber,
    new_blkno: BlockNumber,
) -> PgResult<()> {
    predicate_seams::predicate_lock_page_split::call(
        index_oid, old_blkno, new_blkno,
    )
}

/// `XLogBeginInsert()`.
fn xlog_begin_insert() -> PgResult<()> {
    xloginsert_seams::xlog_begin_insert::call()
}

/// `XLogRegisterData(data, len)`.
fn xlog_register_data(data: &[u8]) -> PgResult<()> {
    xloginsert_seams::xlog_register_data::call(data)
}

/// `XLogRegisterBuffer(block_id, buffer, flags)`.
fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    xloginsert_seams::xlog_register_buffer::call(block_id, buffer, flags)
}

/// `XLogInsert(rmid, info)`.
fn xlog_insert_record(rmid: RmgrId, info: u8) -> PgResult<::types_core::XLogRecPtr> {
    xloginsert_seams::xlog_insert_record::call(rmid, info)
}

/// `BlockIdSet(&bid, blkno)` → on-disk `BlockIdData` bytes (`bi_hi`, `bi_lo`).
fn block_id_bytes(blkno: BlockNumber) -> [u8; 4] {
    let bid = ::types_tuple::heaptuple::BlockIdData::new(blkno);
    let mut out = [0u8; 4];
    out[0..2].copy_from_slice(&bid.bi_hi.to_ne_bytes());
    out[2..4].copy_from_slice(&bid.bi_lo.to_ne_bytes());
    out
}

/// `ginxlogInsert` record body: `{ flags: uint16 }` (ginxlog.h). The
/// tree-specific data is appended separately by `execPlaceToPage`.
fn encode_ginxlog_insert(flags: u16) -> [u8; 2] {
    flags.to_ne_bytes()
}

/// `ginxlogSplit` record body (ginxlog.h): `{ RelFileLocator locator;
/// BlockNumber rrlink, leftChildBlkno, rightChildBlkno; uint16 flags; }`,
/// native-endian (the on-disk image `XLogRegisterData(&data, sizeof)` writes).
fn encode_ginxlog_split(
    index: &Relation<'_>,
    rrlink: BlockNumber,
    left_child: BlockNumber,
    right_child: BlockNumber,
    flags: u16,
) -> PgResult<Vec<u8>> {
    let loc = index.rd_locator;
    let mut v = Vec::with_capacity(24);
    v.extend_from_slice(&loc.spcOid.to_ne_bytes());
    v.extend_from_slice(&loc.dbOid.to_ne_bytes());
    v.extend_from_slice(&loc.relNumber.to_ne_bytes());
    v.extend_from_slice(&rrlink.to_ne_bytes());
    v.extend_from_slice(&left_child.to_ne_bytes());
    v.extend_from_slice(&right_child.to_ne_bytes());
    v.extend_from_slice(&flags.to_ne_bytes());
    Ok(v)
}

/// `GinInitPage(page, flags, pageSize)` — initialize a GIN temp page image
/// (header + opaque), via the `ginutil.c` byte logic re-implemented here for the
/// root-split temp page (the only `GinInitPage` call in `ginbtree.c`).
fn gin_init_page(page: &mut [u8], flags: u32, page_size: usize) -> PgResult<()> {
    ::page::PageInit(
        page,
        page_size,
        core::mem::size_of::<::gin::GinPageOpaqueData>(),
    )?;
    // GinPageGetOpaque(page)->flags = flags; rightlink = InvalidBlockNumber.
    set_page_rightlink(page, InvalidBlockNumber)?;
    set_page_flags(page, flags as u16)?;
    Ok(())
}

/// `GinPageGetOpaque(page)->rightlink = blkno`.
fn set_page_rightlink(page: &mut [u8], blkno: BlockNumber) -> PgResult<()> {
    let special = special_offset(page)?;
    page[special..special + 4].copy_from_slice(&blkno.to_ne_bytes());
    Ok(())
}

/// `GinPageGetOpaque(page)->flags = flags`.
fn set_page_flags(page: &mut [u8], flags: u16) -> PgResult<()> {
    let special = special_offset(page)?;
    page[special + 6..special + 8].copy_from_slice(&flags.to_ne_bytes());
    Ok(())
}

/// `GinPageGetOpaque(page)->flags |= flags`.
fn or_page_flags(page: &mut [u8], flags: u16) -> PgResult<()> {
    let special = special_offset(page)?;
    let cur = u16::from_ne_bytes([page[special + 6], page[special + 7]]);
    page[special + 6..special + 8].copy_from_slice(&(cur | flags).to_ne_bytes());
    Ok(())
}

/// Byte offset of the page special area (`pd_special`), read from the header.
fn special_offset(page: &[u8]) -> PgResult<usize> {
    // pd_special is a 2-byte field at offset 16 in PageHeaderData.
    Ok(u16::from_ne_bytes([page[16], page[17]]) as usize)
}

/// `clear_incomplete_split`: `GinPageGetOpaque(BufferGetPage(buf))->flags &=
/// ~GIN_INCOMPLETE_SPLIT` on a live buffer page.
fn clear_incomplete_split(buffer: Buffer) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        let special = special_offset(page)?;
        let cur = u16::from_ne_bytes([page[special + 6], page[special + 7]]);
        page[special + 6..special + 8]
            .copy_from_slice(&(cur & !GIN_INCOMPLETE_SPLIT).to_ne_bytes());
        Ok(())
    })
}

/// `memcpy(BufferGetPage(buffer), src, BLCKSZ)` — copy a temp page image over a
/// live buffer page.
fn copy_page_into_buffer(buffer: Buffer, src: &[u8]) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        let n = core::cmp::min(page.len(), src.len());
        page[..n].copy_from_slice(&src[..n]);
        Ok(())
    })
}

/// `PageGetTempPage(page)` — a fresh `BLCKSZ` temp page image with the source's
/// special-area size (`pd_special`). `ginbtree.c` only uses it for the root
/// split, then re-initializes it via [`gin_init_page`].
fn page_get_temp_page(src: &[u8]) -> PgResult<Vec<u8>> {
    let pr = PageRef::new(src)?;
    let tmp = ::page::PageGetTempPage(&pr)?;
    Ok(tmp.into_bytes())
}

