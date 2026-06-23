//! Port of `backend/utils/mmgr/freepage.c` — management of free memory pages.
//!
//! A `FreePageManager` tracks which `FPM_PAGE_SIZE` pages of a larger memory
//! extent are unused from the point of view of a higher-level allocator (most
//! importantly the dynamic-shared-memory allocator, which cannot use
//! `malloc`/`palloc` because those do not deal in relative pointers). It can
//! only allocate and free whole runs of pages, and freeing requires knowing
//! the run length.
//!
//! Because the manager lives in shared memory and has no underlying
//! allocator, all bookkeeping (a page-number-ordered btree of free ranges,
//! per-size-class freelists, and a recycle list of spare btree pages) is
//! stored *inside the pages it manages*, addressed by self-relative pointers
//! from a caller-supplied `base`. The struct layout is
//! [`::types_freepage::FreePageManager`]; the page-resident structures are
//! private to this crate, exactly as they are private to `freepage.c`.
//!
//! Public functions take `*mut FreePageManager`, matching the established
//! seam shape: the pointer designates caller-provided (usually shared)
//! memory. The caller must pass a pointer to a manager previously set up by
//! [`free_page_manager_initialize`], whose managed pages are not concurrently
//! mutated (in Postgres, callers hold the segment's lock).

use ::mcx::{Mcx, PgString};
use ::types_core::Size;
use ::types_error::{PgError, PgResult, FATAL};
use ::types_freepage::{FreePageManager, RelPtr, FPM_NUM_FREELISTS, FPM_PAGE_SIZE};

/// Magic numbers to identify various page types.
const FREE_PAGE_SPAN_LEADER_MAGIC: u32 = 0xea40_20f0;
const FREE_PAGE_LEAF_MAGIC: u32 = 0x98ea_e728;
const FREE_PAGE_INTERNAL_MAGIC: u32 = 0x19aa_32c9;

/// `struct FreePageSpanLeader` — doubly linked list of spans of free pages;
/// stored in the first page of the span.
#[repr(C)]
struct FreePageSpanLeader {
    magic: u32,
    npages: Size,
    prev: RelPtr,
    next: RelPtr,
}

/// `FreePageBtreeHeader` — common header for btree leaf and internal pages.
#[repr(C)]
struct FreePageBtreeHeader {
    /// `FREE_PAGE_LEAF_MAGIC` or `FREE_PAGE_INTERNAL_MAGIC`.
    magic: u32,
    /// Number of items used.
    nused: Size,
    /// Uplink.
    parent: RelPtr,
}

/// `FreePageBtreeInternalKey` — points to the next level of the btree.
#[derive(Clone, Copy)]
#[repr(C)]
struct FreePageBtreeInternalKey {
    /// Low bound for keys on the child page.
    first_page: Size,
    /// Downlink.
    child: RelPtr,
}

/// `FreePageBtreeLeafKey` — no payload data.
#[derive(Clone, Copy)]
#[repr(C)]
struct FreePageBtreeLeafKey {
    first_page: Size,
    npages: Size,
}

/// `FPM_ITEMS_PER_INTERNAL_PAGE` / `FPM_ITEMS_PER_LEAF_PAGE`.
const FPM_ITEMS_PER_INTERNAL_PAGE: Size = (FPM_PAGE_SIZE
    - core::mem::size_of::<FreePageBtreeHeader>())
    / core::mem::size_of::<FreePageBtreeInternalKey>();
const FPM_ITEMS_PER_LEAF_PAGE: Size = (FPM_PAGE_SIZE
    - core::mem::size_of::<FreePageBtreeHeader>())
    / core::mem::size_of::<FreePageBtreeLeafKey>();

/// A btree page of either sort.
#[repr(C)]
struct FreePageBtree {
    hdr: FreePageBtreeHeader,
    u: FreePageBtreeKeys,
}

#[repr(C)]
union FreePageBtreeKeys {
    internal_key: [FreePageBtreeInternalKey; FPM_ITEMS_PER_INTERNAL_PAGE],
    leaf_key: [FreePageBtreeLeafKey; FPM_ITEMS_PER_LEAF_PAGE],
}

const _: () = assert!(core::mem::size_of::<FreePageBtree>() <= FPM_PAGE_SIZE);
const _: () = assert!(core::mem::size_of::<FreePageSpanLeader>() <= FPM_PAGE_SIZE);

/// Results of a btree search (`FreePageBtreeSearchResult`).
struct FreePageBtreeSearchResult {
    page: *mut FreePageBtree,
    index: Size,
    found: bool,
    /// Number of additional btree pages needed to split for an insert.
    split_pages: u32,
}

// ---------------------------------------------------------------------------
// relptr.h and freepage.h macros.
// ---------------------------------------------------------------------------

/// `relptr_is_null`.
fn relptr_is_null(rp: RelPtr) -> bool {
    rp.relptr_off == 0
}

/// `relptr_offset` — only meaningful for a non-null relptr.
fn relptr_offset(rp: RelPtr) -> Size {
    rp.relptr_off.wrapping_sub(1)
}

/// `relptr_access`.
unsafe fn relptr_access<T>(base: *mut u8, rp: RelPtr) -> *mut T {
    if rp.relptr_off == 0 {
        core::ptr::null_mut()
    } else {
        base.add(rp.relptr_off - 1) as *mut T
    }
}

/// `relptr_store` (NULL stores 0; otherwise `val - base + 1`).
fn relptr_store<T>(base: *mut u8, rp: &mut RelPtr, val: *mut T) {
    rp.relptr_off = if val.is_null() {
        0
    } else {
        debug_assert!(val as usize >= base as usize);
        (val as usize).wrapping_sub(base as usize) + 1
    };
}

/// `fpm_page_to_pointer`.
unsafe fn fpm_page_to_pointer(base: *mut u8, page: Size) -> *mut u8 {
    base.add(FPM_PAGE_SIZE * page)
}

/// `fpm_pointer_to_page`.
fn fpm_pointer_to_page<T>(base: *mut u8, ptr: *const T) -> Size {
    (ptr as usize).wrapping_sub(base as usize) / FPM_PAGE_SIZE
}

/// `fpm_pointer_is_page_aligned`.
#[allow(dead_code)]
fn fpm_pointer_is_page_aligned<T>(base: *mut u8, ptr: *const T) -> bool {
    (ptr as usize).wrapping_sub(base as usize) % FPM_PAGE_SIZE == 0
}

/// `fpm_segment_base`.
unsafe fn fpm_segment_base(fpm: *mut FreePageManager) -> *mut u8 {
    (fpm as *mut u8).sub(relptr_offset((*fpm).self_))
}

/// The `fpm_size_to_pages` macro: convert an allocation size to a number of
/// pages.
pub fn fpm_size_to_pages(sz: Size) -> Size {
    (sz + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE
}

/// The `fpm_largest` macro: the manager's largest consecutive run of pages.
pub fn fpm_largest(fpm: *mut FreePageManager) -> Size {
    unsafe { (*fpm).contiguous_pages }
}

/// `elog(FATAL, "free page manager btree is corrupt")` as an error value.
fn btree_corrupt() -> PgError {
    PgError::new(FATAL, "free page manager btree is corrupt")
}

// ---------------------------------------------------------------------------
// Public API.
// ---------------------------------------------------------------------------

/// `FreePageManagerInitialize` — initialize a new, empty free page manager.
///
/// `fpm` references caller-provided memory large enough to contain a
/// `FreePageManager`. `base` is the address to which all relative pointers
/// are relative: the segment base for dynamic shared memory, or NULL/extent
/// start for backend-private memory.
pub fn free_page_manager_initialize(fpm: *mut FreePageManager, base: *mut u8) {
    unsafe {
        let f = &mut *fpm;
        relptr_store(base, &mut f.self_, fpm);
        relptr_store(base, &mut f.btree_root, core::ptr::null_mut::<FreePageBtree>());
        relptr_store(
            base,
            &mut f.btree_recycle,
            core::ptr::null_mut::<FreePageSpanLeader>(),
        );
        f.btree_depth = 0;
        f.btree_recycle_count = 0;
        f.singleton_first_page = 0;
        f.singleton_npages = 0;
        f.contiguous_pages = 0;
        f.contiguous_pages_dirty = true;
        for fl in f.freelist.iter_mut() {
            relptr_store(base, fl, core::ptr::null_mut::<FreePageSpanLeader>());
        }
    }
}

/// `FreePageManagerGet` — allocate a run of pages of the given length.
/// Returns the first page of the allocation, or `None` if the request cannot
/// be satisfied (the C `bool` + out-parameter pair).
pub fn free_page_manager_get(fpm: *mut FreePageManager, npages: Size) -> Option<Size> {
    unsafe {
        let result = free_page_manager_get_internal(fpm, npages);

        // It's a bit counterintuitive, but allocating pages can actually
        // create opportunities for cleanup that create larger ranges: pulling
        // a key out of the btree may let recycled pages be reinserted and
        // merge currently-separated ranges.
        let contiguous_pages = free_page_btree_cleanup(fpm);
        if (*fpm).contiguous_pages < contiguous_pages {
            (*fpm).contiguous_pages = contiguous_pages;
        }

        free_page_manager_update_largest(fpm);

        result
    }
}

/// `FreePageManagerPut` — transfer a run of pages to the free page manager.
///
/// The `Err` is the C `elog(FATAL, "free page manager btree is corrupt")`
/// reached when the manager cannot carve a bookkeeping page out of the pages
/// it supposedly has free.
pub fn free_page_manager_put(
    fpm: *mut FreePageManager,
    first_page: Size,
    npages: Size,
) -> PgResult<()> {
    debug_assert!(npages > 0);

    unsafe {
        // Record the new pages.
        let mut contiguous_pages =
            free_page_manager_put_internal(fpm, first_page, npages, false)?;

        // If the new range was contiguous with an existing range, it may have
        // opened up cleanup opportunities.
        if contiguous_pages > npages {
            let cleanup_contiguous_pages = free_page_btree_cleanup(fpm);
            if cleanup_contiguous_pages > contiguous_pages {
                contiguous_pages = cleanup_contiguous_pages;
            }
        }

        // See if we now have a new largest chunk.
        if (*fpm).contiguous_pages < contiguous_pages {
            (*fpm).contiguous_pages = contiguous_pages;
        }

        // PutInternal may have set contiguous_pages_dirty if it allocated
        // internal pages.
        free_page_manager_update_largest(fpm);
    }
    Ok(())
}

/// `FreePageManagerDump` — produce a debugging dump of the state of a free
/// page manager. The C version builds a `StringInfo` in
/// `CurrentMemoryContext`; here the target context is explicit.
pub fn free_page_manager_dump<'mcx>(
    fpm: *mut FreePageManager,
    mcx: Mcx<'mcx>,
) -> PgResult<PgString<'mcx>> {
    unsafe {
        let base = fpm_segment_base(fpm);
        let mut buf = PgString::new_in(mcx);

        // Dump general stuff.
        buf.try_push_str("metadata: self ")?;
        push_size(&mut buf, relptr_offset((*fpm).self_))?;
        buf.try_push_str(" max contiguous pages = ")?;
        push_size(&mut buf, (*fpm).contiguous_pages)?;
        buf.try_push_str("\n")?;

        // Dump btree.
        if (*fpm).btree_depth > 0 {
            buf.try_push_str("btree depth ")?;
            push_size(&mut buf, (*fpm).btree_depth as Size)?;
            buf.try_push_str(":\n")?;
            let root: *mut FreePageBtree = relptr_access(base, (*fpm).btree_root);
            free_page_manager_dump_btree(fpm, root, core::ptr::null_mut(), 0, &mut buf)?;
        } else if (*fpm).singleton_npages > 0 {
            buf.try_push_str("singleton: ")?;
            push_size(&mut buf, (*fpm).singleton_first_page)?;
            buf.try_push_str("(")?;
            push_size(&mut buf, (*fpm).singleton_npages)?;
            buf.try_push_str(")\n")?;
        }

        // Dump btree recycle list.
        let recycle: *mut FreePageSpanLeader = relptr_access(base, (*fpm).btree_recycle);
        if !recycle.is_null() {
            buf.try_push_str("btree recycle:")?;
            free_page_manager_dump_spans(fpm, recycle, 1, &mut buf)?;
        }

        // Dump free lists.
        let mut dumped_any_freelist = false;
        for f in 0..FPM_NUM_FREELISTS {
            if relptr_is_null((*fpm).freelist[f]) {
                continue;
            }
            if !dumped_any_freelist {
                buf.try_push_str("freelists:\n")?;
                dumped_any_freelist = true;
            }
            buf.try_push_str("  ")?;
            push_size(&mut buf, f + 1)?;
            buf.try_push_str(":")?;
            let span: *mut FreePageSpanLeader = relptr_access(base, (*fpm).freelist[f]);
            free_page_manager_dump_spans(fpm, span, f + 1, &mut buf)?;
        }

        Ok(buf)
    }
}

/// Install this crate's implementations of its seam declarations.
pub fn init_seams() {
    freepage_seams::free_page_manager_initialize::set(
        free_page_manager_initialize,
    );
    freepage_seams::free_page_manager_get::set(free_page_manager_get);
    freepage_seams::free_page_manager_put::set(free_page_manager_put);
}

// ---------------------------------------------------------------------------
// Btree helpers.
// ---------------------------------------------------------------------------

/// `FreePageManagerLargestContiguous` — compute the size of the largest run
/// of pages the user could successfully get.
unsafe fn free_page_manager_largest_contiguous(fpm: *mut FreePageManager) -> Size {
    let base = fpm_segment_base(fpm);
    let mut largest: Size = 0;

    if !relptr_is_null((*fpm).freelist[FPM_NUM_FREELISTS - 1]) {
        let mut candidate: *mut FreePageSpanLeader =
            relptr_access(base, (*fpm).freelist[FPM_NUM_FREELISTS - 1]);
        loop {
            if (*candidate).npages > largest {
                largest = (*candidate).npages;
            }
            candidate = relptr_access(base, (*candidate).next);
            if candidate.is_null() {
                break;
            }
        }
    } else {
        let mut f = FPM_NUM_FREELISTS - 1;
        loop {
            f -= 1;
            if !relptr_is_null((*fpm).freelist[f]) {
                largest = f + 1;
                break;
            }
            if f == 0 {
                break;
            }
        }
    }

    largest
}

/// `FreePageManagerUpdateLargest` — recompute the largest run if dirty.
unsafe fn free_page_manager_update_largest(fpm: *mut FreePageManager) {
    if !(*fpm).contiguous_pages_dirty {
        return;
    }
    (*fpm).contiguous_pages = free_page_manager_largest_contiguous(fpm);
    (*fpm).contiguous_pages_dirty = false;
}

/// `FreePageBtreeAdjustAncestorKeys` — propagate a change to the first key on
/// a page up through its ancestors as far as needed.
unsafe fn free_page_btree_adjust_ancestor_keys(
    fpm: *mut FreePageManager,
    btp: *mut FreePageBtree,
) {
    let base = fpm_segment_base(fpm);
    let first_page;

    // This might be either a leaf or an internal page.
    debug_assert!((*btp).hdr.nused > 0);
    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        debug_assert!((*btp).hdr.nused <= FPM_ITEMS_PER_LEAF_PAGE);
        first_page = (*btp).u.leaf_key[0].first_page;
    } else {
        debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
        debug_assert!((*btp).hdr.nused <= FPM_ITEMS_PER_INTERNAL_PAGE);
        first_page = (*btp).u.internal_key[0].first_page;
    }
    let mut child = btp;

    // Loop until we find an ancestor that does not require adjustment.
    loop {
        let parent: *mut FreePageBtree = relptr_access(base, (*child).hdr.parent);
        if parent.is_null() {
            break;
        }
        let mut s = free_page_btree_search_internal(parent, first_page);

        // Key is either at index s or index s-1; figure out which.
        if s >= (*parent).hdr.nused {
            debug_assert_eq!(s, (*parent).hdr.nused);
            s -= 1;
        } else {
            let check: *mut FreePageBtree =
                relptr_access(base, (*parent).u.internal_key[s].child);
            if check != child {
                debug_assert!(s > 0);
                s -= 1;
            }
        }

        debug_assert!(s < (*parent).hdr.nused);
        debug_assert!(
            relptr_access::<FreePageBtree>(base, (*parent).u.internal_key[s].child) == child
        );

        // Update the parent key.
        (*parent).u.internal_key[s].first_page = first_page;

        // If this is the first key in the parent, go up another level; else
        // done.
        if s > 0 {
            break;
        }
        child = parent;
    }
}

/// `FreePageBtreeCleanup` — attempt to reclaim space from the free-page
/// btree. Returns the largest range of contiguous pages created by the
/// cleanup operation.
unsafe fn free_page_btree_cleanup(fpm: *mut FreePageManager) -> Size {
    let base = fpm_segment_base(fpm);
    let mut max_contiguous_pages: Size = 0;

    // Attempt to shrink the depth of the btree.
    while !relptr_is_null((*fpm).btree_root) {
        let root: *mut FreePageBtree = relptr_access(base, (*fpm).btree_root);

        if (*root).hdr.nused == 1 {
            // Root contains only one key: shrink depth of tree by one.
            debug_assert!((*fpm).btree_depth > 0);
            (*fpm).btree_depth -= 1;
            if (*root).hdr.magic == FREE_PAGE_LEAF_MAGIC {
                // If root is a leaf, convert only entry to singleton range.
                relptr_store(
                    base,
                    &mut (*fpm).btree_root,
                    core::ptr::null_mut::<FreePageBtree>(),
                );
                (*fpm).singleton_first_page = (*root).u.leaf_key[0].first_page;
                (*fpm).singleton_npages = (*root).u.leaf_key[0].npages;
            } else {
                // If root is an internal page, make only child the root.
                debug_assert_eq!((*root).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
                (*fpm).btree_root = (*root).u.internal_key[0].child;
                let newroot: *mut FreePageBtree = relptr_access(base, (*fpm).btree_root);
                relptr_store(
                    base,
                    &mut (*newroot).hdr.parent,
                    core::ptr::null_mut::<FreePageBtree>(),
                );
            }
            free_page_btree_recycle(fpm, fpm_pointer_to_page(base, root));
        } else if (*root).hdr.nused == 2 && (*root).hdr.magic == FREE_PAGE_LEAF_MAGIC {
            let end_of_first =
                (*root).u.leaf_key[0].first_page + (*root).u.leaf_key[0].npages;
            let start_of_second = (*root).u.leaf_key[1].first_page;

            if end_of_first + 1 == start_of_second {
                let root_page = fpm_pointer_to_page(base, root);

                if end_of_first == root_page {
                    free_page_pop_span_leader(fpm, (*root).u.leaf_key[0].first_page);
                    free_page_pop_span_leader(fpm, (*root).u.leaf_key[1].first_page);
                    (*fpm).singleton_first_page = (*root).u.leaf_key[0].first_page;
                    (*fpm).singleton_npages = (*root).u.leaf_key[0].npages
                        + (*root).u.leaf_key[1].npages
                        + 1;
                    (*fpm).btree_depth = 0;
                    relptr_store(
                        base,
                        &mut (*fpm).btree_root,
                        core::ptr::null_mut::<FreePageBtree>(),
                    );
                    free_page_push_span_leader(
                        fpm,
                        (*fpm).singleton_first_page,
                        (*fpm).singleton_npages,
                    );
                    debug_assert_eq!(max_contiguous_pages, 0);
                    max_contiguous_pages = (*fpm).singleton_npages;
                }
            }

            // Whether it worked or not, it's time to stop.
            break;
        } else {
            // Nothing more to do. Stop.
            break;
        }
    }

    // Attempt to free recycled btree pages. We skip this if releasing the
    // recycled page would require a btree page split (soft insert), and only
    // ever attempt to recycle the first page on the list.
    while (*fpm).btree_recycle_count > 0 {
        let btp = free_page_btree_get_recycled(fpm);
        let first_page = fpm_pointer_to_page(base, btp);
        let contiguous_pages =
            match free_page_manager_put_internal(fpm, first_page, 1, true) {
                Ok(n) => n,
                // With soft == true, both elog(FATAL) sites are preceded by
                // `return 0` (freepage.c:1527-1528, 1665-1667), so a soft
                // insertion cannot fail.
                Err(_) => unreachable!("soft FreePageManagerPutInternal cannot fail"),
            };
        if contiguous_pages == 0 {
            free_page_btree_recycle(fpm, first_page);
            break;
        } else if contiguous_pages > max_contiguous_pages {
            max_contiguous_pages = contiguous_pages;
        }
    }

    max_contiguous_pages
}

/// `FreePageBtreeConsolidate` — consider consolidating the given page with
/// its left or right sibling, if it's fairly empty.
unsafe fn free_page_btree_consolidate(fpm: *mut FreePageManager, btp: *mut FreePageBtree) {
    let base = fpm_segment_base(fpm);

    // We only try to consolidate pages that are less than a third full;
    // the goal is to reclaim pages before things get egregiously out of
    // hand, not to keep the btree minimal.
    let max = if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        FPM_ITEMS_PER_LEAF_PAGE
    } else {
        debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
        FPM_ITEMS_PER_INTERNAL_PAGE
    };
    if (*btp).hdr.nused >= max / 3 {
        return;
    }

    // If we can fit our right sibling's keys onto this page, consolidate.
    let np = free_page_btree_find_right_sibling(base, btp);
    if !np.is_null() && (*btp).hdr.nused + (*np).hdr.nused <= max {
        if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
            core::ptr::copy_nonoverlapping(
                (*np).u.leaf_key.as_ptr(),
                (*btp).u.leaf_key.as_mut_ptr().add((*btp).hdr.nused),
                (*np).hdr.nused,
            );
            (*btp).hdr.nused += (*np).hdr.nused;
        } else {
            core::ptr::copy_nonoverlapping(
                (*np).u.internal_key.as_ptr(),
                (*btp).u.internal_key.as_mut_ptr().add((*btp).hdr.nused),
                (*np).hdr.nused,
            );
            (*btp).hdr.nused += (*np).hdr.nused;
            free_page_btree_update_parent_pointers(base, btp);
        }
        free_page_btree_remove_page(fpm, np);
        return;
    }

    // If we can fit our keys onto our left sibling's page, consolidate. We
    // move our keys onto the other page rather than vice versa, to avoid
    // having to adjust ancestor keys.
    let np = free_page_btree_find_left_sibling(base, btp);
    if !np.is_null() && (*btp).hdr.nused + (*np).hdr.nused <= max {
        if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
            core::ptr::copy_nonoverlapping(
                (*btp).u.leaf_key.as_ptr(),
                (*np).u.leaf_key.as_mut_ptr().add((*np).hdr.nused),
                (*btp).hdr.nused,
            );
            (*np).hdr.nused += (*btp).hdr.nused;
        } else {
            core::ptr::copy_nonoverlapping(
                (*btp).u.internal_key.as_ptr(),
                (*np).u.internal_key.as_mut_ptr().add((*np).hdr.nused),
                (*btp).hdr.nused,
            );
            (*np).hdr.nused += (*btp).hdr.nused;
            free_page_btree_update_parent_pointers(base, np);
        }
        free_page_btree_remove_page(fpm, btp);
    }
}

/// `FreePageBtreeFindLeftSibling` — the page at the same level of the tree
/// whose keyspace immediately precedes ours.
unsafe fn free_page_btree_find_left_sibling(
    base: *mut u8,
    btp: *mut FreePageBtree,
) -> *mut FreePageBtree {
    let mut p = btp;
    let mut levels = 0;

    // Move up until we can move left.
    loop {
        let first_page = free_page_btree_first_key(p);
        p = relptr_access(base, (*p).hdr.parent);

        if p.is_null() {
            return core::ptr::null_mut(); // we were passed the leftmost page
        }

        let index = free_page_btree_search_internal(p, first_page);
        if index > 0 {
            debug_assert_eq!((*p).u.internal_key[index].first_page, first_page);
            p = relptr_access(base, (*p).u.internal_key[index - 1].child);
            break;
        }
        debug_assert_eq!(index, 0);
        levels += 1;
    }

    // Descend right.
    while levels > 0 {
        debug_assert_eq!((*p).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
        p = relptr_access(base, (*p).u.internal_key[(*p).hdr.nused - 1].child);
        levels -= 1;
    }
    debug_assert_eq!((*p).hdr.magic, (*btp).hdr.magic);

    p
}

/// `FreePageBtreeFindRightSibling` — the page at the same level of the tree
/// whose keyspace immediately follows ours.
unsafe fn free_page_btree_find_right_sibling(
    base: *mut u8,
    btp: *mut FreePageBtree,
) -> *mut FreePageBtree {
    let mut p = btp;
    let mut levels = 0;

    // Move up until we can move right.
    loop {
        let first_page = free_page_btree_first_key(p);
        p = relptr_access(base, (*p).hdr.parent);

        if p.is_null() {
            return core::ptr::null_mut(); // we were passed the rightmost page
        }

        let index = free_page_btree_search_internal(p, first_page);
        if index < (*p).hdr.nused - 1 {
            debug_assert_eq!((*p).u.internal_key[index].first_page, first_page);
            p = relptr_access(base, (*p).u.internal_key[index + 1].child);
            break;
        }
        debug_assert_eq!(index, (*p).hdr.nused - 1);
        levels += 1;
    }

    // Descend left.
    while levels > 0 {
        debug_assert_eq!((*p).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
        p = relptr_access(base, (*p).u.internal_key[0].child);
        levels -= 1;
    }
    debug_assert_eq!((*p).hdr.magic, (*btp).hdr.magic);

    p
}

/// `FreePageBtreeFirstKey` — get the first key on a btree page.
unsafe fn free_page_btree_first_key(btp: *mut FreePageBtree) -> Size {
    debug_assert!((*btp).hdr.nused > 0);

    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        (*btp).u.leaf_key[0].first_page
    } else {
        debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
        (*btp).u.internal_key[0].first_page
    }
}

/// `FreePageBtreeGetRecycled` — get a page from the btree recycle list for
/// use as a btree page.
unsafe fn free_page_btree_get_recycled(fpm: *mut FreePageManager) -> *mut FreePageBtree {
    let base = fpm_segment_base(fpm);
    let victim: *mut FreePageSpanLeader = relptr_access(base, (*fpm).btree_recycle);

    debug_assert!(!victim.is_null());
    let newhead: *mut FreePageSpanLeader = relptr_access(base, (*victim).next);
    if !newhead.is_null() {
        (*newhead).prev = (*victim).prev;
    }
    relptr_store(base, &mut (*fpm).btree_recycle, newhead);
    debug_assert!(fpm_pointer_is_page_aligned(base, victim));
    (*fpm).btree_recycle_count -= 1;
    victim as *mut FreePageBtree
}

/// `FreePageBtreeInsertInternal` — insert an item into an internal page
/// (there must be room).
unsafe fn free_page_btree_insert_internal(
    base: *mut u8,
    btp: *mut FreePageBtree,
    index: Size,
    first_page: Size,
    child: *mut FreePageBtree,
) {
    debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
    debug_assert!((*btp).hdr.nused < FPM_ITEMS_PER_INTERNAL_PAGE);
    debug_assert!(index <= (*btp).hdr.nused);
    let keys = (*btp).u.internal_key.as_mut_ptr();
    core::ptr::copy(keys.add(index), keys.add(index + 1), (*btp).hdr.nused - index);
    (*keys.add(index)).first_page = first_page;
    relptr_store(base, &mut (*keys.add(index)).child, child);
    (*btp).hdr.nused += 1;
}

/// `FreePageBtreeInsertLeaf` — insert an item into a leaf page (there must be
/// room).
unsafe fn free_page_btree_insert_leaf(
    btp: *mut FreePageBtree,
    index: Size,
    first_page: Size,
    npages: Size,
) {
    debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_LEAF_MAGIC);
    debug_assert!((*btp).hdr.nused < FPM_ITEMS_PER_LEAF_PAGE);
    debug_assert!(index <= (*btp).hdr.nused);
    let keys = (*btp).u.leaf_key.as_mut_ptr();
    core::ptr::copy(keys.add(index), keys.add(index + 1), (*btp).hdr.nused - index);
    (*keys.add(index)).first_page = first_page;
    (*keys.add(index)).npages = npages;
    (*btp).hdr.nused += 1;
}

/// `FreePageBtreeRecycle` — put a page on the btree recycle list.
unsafe fn free_page_btree_recycle(fpm: *mut FreePageManager, pageno: Size) {
    let base = fpm_segment_base(fpm);
    let head: *mut FreePageSpanLeader = relptr_access(base, (*fpm).btree_recycle);

    let span = fpm_page_to_pointer(base, pageno) as *mut FreePageSpanLeader;
    (*span).magic = FREE_PAGE_SPAN_LEADER_MAGIC;
    (*span).npages = 1;
    relptr_store(base, &mut (*span).next, head);
    relptr_store(
        base,
        &mut (*span).prev,
        core::ptr::null_mut::<FreePageSpanLeader>(),
    );
    if !head.is_null() {
        relptr_store(base, &mut (*head).prev, span);
    }
    relptr_store(base, &mut (*fpm).btree_recycle, span);
    (*fpm).btree_recycle_count += 1;
}

/// `FreePageBtreeRemove` — remove an item from the btree at the given
/// position on the given (leaf) page.
unsafe fn free_page_btree_remove(
    fpm: *mut FreePageManager,
    btp: *mut FreePageBtree,
    index: Size,
) {
    debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_LEAF_MAGIC);
    debug_assert!(index < (*btp).hdr.nused);

    // When last item is removed, extirpate entire page from btree.
    if (*btp).hdr.nused == 1 {
        free_page_btree_remove_page(fpm, btp);
        return;
    }

    // Physically remove the key from the page.
    (*btp).hdr.nused -= 1;
    if index < (*btp).hdr.nused {
        let keys = (*btp).u.leaf_key.as_mut_ptr();
        core::ptr::copy(keys.add(index + 1), keys.add(index), (*btp).hdr.nused - index);
    }

    // If we just removed the first key, adjust ancestor keys.
    if index == 0 {
        free_page_btree_adjust_ancestor_keys(fpm, btp);
    }

    // Consider whether to consolidate this page with a sibling.
    free_page_btree_consolidate(fpm, btp);
}

/// `FreePageBtreeRemovePage` — remove a page from the btree; the caller has
/// relocated any keys still wanted. The page goes on the recycle list.
unsafe fn free_page_btree_remove_page(fpm: *mut FreePageManager, btp: *mut FreePageBtree) {
    let base = fpm_segment_base(fpm);
    let mut btp = btp;

    let parent = loop {
        // Find parent page.
        let parent: *mut FreePageBtree = relptr_access(base, (*btp).hdr.parent);
        if parent.is_null() {
            // We are removing the root page.
            relptr_store(
                base,
                &mut (*fpm).btree_root,
                core::ptr::null_mut::<FreePageBtree>(),
            );
            (*fpm).btree_depth = 0;
            debug_assert_eq!((*fpm).singleton_first_page, 0);
            debug_assert_eq!((*fpm).singleton_npages, 0);
            return;
        }

        // If the parent contains only one item, we need to remove it as well.
        if (*parent).hdr.nused > 1 {
            break parent;
        }
        free_page_btree_recycle(fpm, fpm_pointer_to_page(base, btp));
        btp = parent;
    };

    // Find and remove the downlink.
    let first_page = free_page_btree_first_key(btp);
    let index;
    if (*parent).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        index = free_page_btree_search_leaf(parent, first_page);
        debug_assert!(index < (*parent).hdr.nused);
        if index < (*parent).hdr.nused - 1 {
            let keys = (*parent).u.leaf_key.as_mut_ptr();
            core::ptr::copy(
                keys.add(index + 1),
                keys.add(index),
                (*parent).hdr.nused - index - 1,
            );
        }
    } else {
        index = free_page_btree_search_internal(parent, first_page);
        debug_assert!(index < (*parent).hdr.nused);
        if index < (*parent).hdr.nused - 1 {
            let keys = (*parent).u.internal_key.as_mut_ptr();
            core::ptr::copy(
                keys.add(index + 1),
                keys.add(index),
                (*parent).hdr.nused - index - 1,
            );
        }
    }
    (*parent).hdr.nused -= 1;
    debug_assert!((*parent).hdr.nused > 0);

    // Recycle the page.
    free_page_btree_recycle(fpm, fpm_pointer_to_page(base, btp));

    // Adjust ancestor keys if needed.
    if index == 0 {
        free_page_btree_adjust_ancestor_keys(fpm, parent);
    }

    // Consider whether to consolidate the parent with a sibling.
    free_page_btree_consolidate(fpm, parent);
}

/// `FreePageBtreeSearch` — search the btree for an entry for the given first
/// page. `page`/`index` are the position of an exact match, or where the new
/// key should be inserted; `split_pages` is the number of additional btree
/// pages needed to split for an insert.
unsafe fn free_page_btree_search(
    fpm: *mut FreePageManager,
    first_page: Size,
) -> FreePageBtreeSearchResult {
    let base = fpm_segment_base(fpm);
    let mut btp: *mut FreePageBtree = relptr_access(base, (*fpm).btree_root);
    let mut result = FreePageBtreeSearchResult {
        page: core::ptr::null_mut(),
        index: 0,
        found: false,
        split_pages: 1,
    };

    // If the btree is empty, there's nothing to find.
    if btp.is_null() {
        return result;
    }

    // Descend until we hit a leaf.
    while (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
        let mut index = free_page_btree_search_internal(btp, first_page);
        let found_exact = index < (*btp).hdr.nused
            && (*btp).u.internal_key[index].first_page == first_page;

        // If we found an exact match we descend directly. Otherwise, descend
        // into the child to the left if possible so that we can find the
        // insertion point at that child's high end.
        if !found_exact && index > 0 {
            index -= 1;
        }

        // Track required split depth for leaf insert.
        if (*btp).hdr.nused >= FPM_ITEMS_PER_INTERNAL_PAGE {
            debug_assert_eq!((*btp).hdr.nused, FPM_ITEMS_PER_INTERNAL_PAGE);
            result.split_pages += 1;
        } else {
            result.split_pages = 0;
        }

        // Descend to appropriate child page.
        debug_assert!(index < (*btp).hdr.nused);
        let child: *mut FreePageBtree = relptr_access(base, (*btp).u.internal_key[index].child);
        debug_assert!(relptr_access::<FreePageBtree>(base, (*child).hdr.parent) == btp);
        btp = child;
    }

    // Track required split depth for leaf insert.
    if (*btp).hdr.nused >= FPM_ITEMS_PER_LEAF_PAGE {
        // sic: the C asserts against FPM_ITEMS_PER_INTERNAL_PAGE here
        // (freepage.c:1118); the two constants are equal.
        debug_assert_eq!((*btp).hdr.nused, FPM_ITEMS_PER_INTERNAL_PAGE);
        result.split_pages += 1;
    } else {
        result.split_pages = 0;
    }

    // Search leaf page.
    let index = free_page_btree_search_leaf(btp, first_page);

    // Assemble results.
    result.page = btp;
    result.index = index;
    result.found =
        index < (*btp).hdr.nused && first_page == (*btp).u.leaf_key[index].first_page;
    result
}

/// `FreePageBtreeSearchInternal` — binary search an internal page for the
/// first key greater than or equal to `first_page`.
unsafe fn free_page_btree_search_internal(btp: *mut FreePageBtree, first_page: Size) -> Size {
    let mut low: Size = 0;
    let mut high: Size = (*btp).hdr.nused;

    debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
    debug_assert!(high > 0 && high <= FPM_ITEMS_PER_INTERNAL_PAGE);

    while low < high {
        let mid = (low + high) / 2;
        let val = (*btp).u.internal_key[mid].first_page;

        if first_page == val {
            return mid;
        } else if first_page < val {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    low
}

/// `FreePageBtreeSearchLeaf` — binary search a leaf page for the first key
/// greater than or equal to `first_page`.
unsafe fn free_page_btree_search_leaf(btp: *mut FreePageBtree, first_page: Size) -> Size {
    let mut low: Size = 0;
    let mut high: Size = (*btp).hdr.nused;

    debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_LEAF_MAGIC);
    debug_assert!(high > 0 && high <= FPM_ITEMS_PER_LEAF_PAGE);

    while low < high {
        let mid = (low + high) / 2;
        let val = (*btp).u.leaf_key[mid].first_page;

        if first_page == val {
            return mid;
        } else if first_page < val {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    low
}

/// `FreePageBtreeSplitPage` — allocate a new btree page (from the recycle
/// list, which the caller has stocked) and move half the keys from `btp` to
/// it. The caller must add a downlink to the returned page.
unsafe fn free_page_btree_split_page(
    fpm: *mut FreePageManager,
    btp: *mut FreePageBtree,
) -> *mut FreePageBtree {
    let newsibling = free_page_btree_get_recycled(fpm);

    (*newsibling).hdr.magic = (*btp).hdr.magic;
    (*newsibling).hdr.nused = (*btp).hdr.nused / 2;
    (*newsibling).hdr.parent = (*btp).hdr.parent;
    (*btp).hdr.nused -= (*newsibling).hdr.nused;

    if (*btp).hdr.magic == FREE_PAGE_LEAF_MAGIC {
        core::ptr::copy_nonoverlapping(
            (*btp).u.leaf_key.as_ptr().add((*btp).hdr.nused),
            (*newsibling).u.leaf_key.as_mut_ptr(),
            (*newsibling).hdr.nused,
        );
    } else {
        debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
        core::ptr::copy_nonoverlapping(
            (*btp).u.internal_key.as_ptr().add((*btp).hdr.nused),
            (*newsibling).u.internal_key.as_mut_ptr(),
            (*newsibling).hdr.nused,
        );
        free_page_btree_update_parent_pointers(fpm_segment_base(fpm), newsibling);
    }

    newsibling
}

/// `FreePageBtreeUpdateParentPointers` — after an internal page is split or
/// merged, repoint its children's parent uplinks at it.
unsafe fn free_page_btree_update_parent_pointers(base: *mut u8, btp: *mut FreePageBtree) {
    debug_assert_eq!((*btp).hdr.magic, FREE_PAGE_INTERNAL_MAGIC);
    for i in 0..(*btp).hdr.nused {
        let child: *mut FreePageBtree = relptr_access(base, (*btp).u.internal_key[i].child);
        relptr_store(base, &mut (*child).hdr.parent, btp);
    }
}

/// `FreePageManagerDumpBtree` — debugging dump of btree data.
unsafe fn free_page_manager_dump_btree(
    fpm: *mut FreePageManager,
    btp: *mut FreePageBtree,
    parent: *mut FreePageBtree,
    level: i32,
    buf: &mut PgString<'_>,
) -> PgResult<()> {
    let base = fpm_segment_base(fpm);
    let pageno = fpm_pointer_to_page(base, btp);

    stack_depth_seams::check_stack_depth::call()?;
    let check_parent: *mut FreePageBtree = relptr_access(base, (*btp).hdr.parent);
    buf.try_push_str("  ")?;
    push_size(buf, pageno)?;
    buf.try_push_str("@")?;
    push_size(buf, level as Size)?;
    buf.try_push_str(if (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
        " i"
    } else {
        " l"
    })?;
    if parent != check_parent {
        buf.try_push_str(" [actual parent ")?;
        push_size(buf, fpm_pointer_to_page(base, check_parent))?;
        buf.try_push_str(", expected ")?;
        push_size(buf, fpm_pointer_to_page(base, parent))?;
        buf.try_push_str("]")?;
    }
    buf.try_push_str(":")?;
    for index in 0..(*btp).hdr.nused {
        if (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
            buf.try_push_str(" ")?;
            push_size(buf, (*btp).u.internal_key[index].first_page)?;
            buf.try_push_str("->")?;
            push_size(
                buf,
                relptr_offset((*btp).u.internal_key[index].child) / FPM_PAGE_SIZE,
            )?;
        } else {
            buf.try_push_str(" ")?;
            push_size(buf, (*btp).u.leaf_key[index].first_page)?;
            buf.try_push_str("(")?;
            push_size(buf, (*btp).u.leaf_key[index].npages)?;
            buf.try_push_str(")")?;
        }
    }
    buf.try_push_str("\n")?;

    if (*btp).hdr.magic == FREE_PAGE_INTERNAL_MAGIC {
        for index in 0..(*btp).hdr.nused {
            let child: *mut FreePageBtree =
                relptr_access(base, (*btp).u.internal_key[index].child);
            free_page_manager_dump_btree(fpm, child, btp, level + 1, buf)?;
        }
    }
    Ok(())
}

/// `FreePageManagerDumpSpans` — debugging dump of free-span data.
unsafe fn free_page_manager_dump_spans(
    fpm: *mut FreePageManager,
    span: *mut FreePageSpanLeader,
    expected_pages: Size,
    buf: &mut PgString<'_>,
) -> PgResult<()> {
    let base = fpm_segment_base(fpm);
    let mut span = span;

    while !span.is_null() {
        buf.try_push_str(" ")?;
        push_size(buf, fpm_pointer_to_page(base, span))?;
        if (*span).npages != expected_pages {
            buf.try_push_str("(")?;
            push_size(buf, (*span).npages)?;
            buf.try_push_str(")")?;
        }
        span = relptr_access(base, (*span).next);
    }

    buf.try_push_str("\n")
}

/// `FreePageManagerGetInternal` — allocate a run of pages of the given length
/// from the free page manager.
unsafe fn free_page_manager_get_internal(
    fpm: *mut FreePageManager,
    npages: Size,
) -> Option<Size> {
    let base = fpm_segment_base(fpm);
    let mut victim: *mut FreePageSpanLeader = core::ptr::null_mut();
    let mut chosen_f: Size = 0;

    // Search for a free span: best fit, starting from the freelist for runs
    // of exactly `npages` (the last freelist holds everything too big for the
    // fixed-size lists and must be scanned).
    let mut f = npages.min(FPM_NUM_FREELISTS).wrapping_sub(1);
    while f < FPM_NUM_FREELISTS {
        // Skip empty freelists.
        if relptr_is_null((*fpm).freelist[f]) {
            f += 1;
            continue;
        }

        if f < FPM_NUM_FREELISTS - 1 {
            victim = relptr_access(base, (*fpm).freelist[f]);
        } else {
            let mut candidate: *mut FreePageSpanLeader =
                relptr_access(base, (*fpm).freelist[f]);
            loop {
                if (*candidate).npages >= npages
                    && (victim.is_null() || (*victim).npages > (*candidate).npages)
                {
                    victim = candidate;
                    if (*victim).npages == npages {
                        break;
                    }
                }
                candidate = relptr_access(base, (*candidate).next);
                if candidate.is_null() {
                    break;
                }
            }
        }
        chosen_f = f;
        break;
    }

    // If we didn't find an allocatable span, return failure.
    if victim.is_null() {
        return None;
    }
    let f = chosen_f;

    // Remove span from free list.
    debug_assert_eq!((*victim).magic, FREE_PAGE_SPAN_LEADER_MAGIC);
    let prev: *mut FreePageSpanLeader = relptr_access(base, (*victim).prev);
    let next: *mut FreePageSpanLeader = relptr_access(base, (*victim).next);
    if !prev.is_null() {
        (*prev).next = (*victim).next;
    } else {
        (*fpm).freelist[f] = (*victim).next;
    }
    if !next.is_null() {
        (*next).prev = (*victim).prev;
    }
    let victim_page = fpm_pointer_to_page(base, victim);

    // Decide whether we might be invalidating contiguous_pages.
    if f == FPM_NUM_FREELISTS - 1 && (*victim).npages == (*fpm).contiguous_pages {
        // The victim span came from the oversized freelist and had the same
        // size as the longest span; there may or may not be another of the
        // same size, so recompute to be safe.
        (*fpm).contiguous_pages_dirty = true;
    } else if f + 1 == (*fpm).contiguous_pages && relptr_is_null((*fpm).freelist[f]) {
        // The victim came from the fixed-size freelist for spans of the
        // current longest size, and that list is now empty.
        (*fpm).contiguous_pages_dirty = true;
    }

    // If we haven't initialized the btree yet, the victim must be the single
    // span stored within the FreePageManager itself.
    if relptr_is_null((*fpm).btree_root) {
        debug_assert_eq!(victim_page, (*fpm).singleton_first_page);
        debug_assert_eq!((*victim).npages, (*fpm).singleton_npages);
        debug_assert!((*victim).npages >= npages);
        (*fpm).singleton_first_page += npages;
        (*fpm).singleton_npages -= npages;
        if (*fpm).singleton_npages > 0 {
            free_page_push_span_leader(
                fpm,
                (*fpm).singleton_first_page,
                (*fpm).singleton_npages,
            );
        }
    } else {
        // If the span we found is exactly the right size, remove it from the
        // btree completely; otherwise adjust the btree entry to the
        // still-unallocated portion and put that portion on the appropriate
        // free list.
        let result = free_page_btree_search(fpm, victim_page);
        debug_assert!(result.found);
        if (*victim).npages == npages {
            free_page_btree_remove(fpm, result.page, result.index);
        } else {
            debug_assert!((*victim).npages > npages);
            let key = (*result.page).u.leaf_key.as_mut_ptr().add(result.index);
            debug_assert_eq!((*key).npages, (*victim).npages);
            (*key).first_page += npages;
            (*key).npages -= npages;
            if result.index == 0 {
                free_page_btree_adjust_ancestor_keys(fpm, result.page);
            }

            free_page_push_span_leader(fpm, victim_page + npages, (*victim).npages - npages);
        }
    }

    Some(fpm_pointer_to_page(base, victim))
}

/// `FreePageManagerPutInternal` — put a range of pages into the btree and
/// freelists, consolidating with adjacent free spans. If `soft`, only insert
/// if no new btree pages must be allocated, returning `Ok(0)` if the
/// insertion was skipped; otherwise return the size of the contiguous span
/// created by the insertion. The `Err` is the C
/// `elog(FATAL, "free page manager btree is corrupt")`, reachable only when
/// `soft` is false.
unsafe fn free_page_manager_put_internal(
    fpm: *mut FreePageManager,
    first_page: Size,
    npages: Size,
    soft: bool,
) -> PgResult<Size> {
    let base = fpm_segment_base(fpm);

    debug_assert!(npages > 0);

    // We can store a single free span without initializing the btree.
    if (*fpm).btree_depth == 0 {
        if (*fpm).singleton_npages == 0 {
            // Don't have a span yet; store this one.
            (*fpm).singleton_first_page = first_page;
            (*fpm).singleton_npages = npages;
            free_page_push_span_leader(fpm, first_page, npages);
            return Ok((*fpm).singleton_npages);
        } else if (*fpm).singleton_first_page + (*fpm).singleton_npages == first_page {
            // New span immediately follows sole existing span.
            (*fpm).singleton_npages += npages;
            free_page_pop_span_leader(fpm, (*fpm).singleton_first_page);
            free_page_push_span_leader(
                fpm,
                (*fpm).singleton_first_page,
                (*fpm).singleton_npages,
            );
            return Ok((*fpm).singleton_npages);
        } else if first_page + npages == (*fpm).singleton_first_page {
            // New span immediately precedes sole existing span.
            free_page_pop_span_leader(fpm, (*fpm).singleton_first_page);
            (*fpm).singleton_first_page = first_page;
            (*fpm).singleton_npages += npages;
            free_page_push_span_leader(
                fpm,
                (*fpm).singleton_first_page,
                (*fpm).singleton_npages,
            );
            return Ok((*fpm).singleton_npages);
        } else {
            // Not contiguous; we need to initialize the btree.
            let root: *mut FreePageBtree;
            if !relptr_is_null((*fpm).btree_recycle) {
                root = free_page_btree_get_recycled(fpm);
            } else if soft {
                return Ok(0); // Should not allocate if soft.
            } else if let Some(root_page) = free_page_manager_get_internal(fpm, 1) {
                root = fpm_page_to_pointer(base, root_page) as *mut FreePageBtree;
            } else {
                // We'd better be able to get a page from the existing range.
                return Err(btree_corrupt());
            }

            // Create the btree and move the preexisting range into it.
            (*root).hdr.magic = FREE_PAGE_LEAF_MAGIC;
            (*root).hdr.nused = 1;
            relptr_store(
                base,
                &mut (*root).hdr.parent,
                core::ptr::null_mut::<FreePageBtree>(),
            );
            (*root).u.leaf_key[0].first_page = (*fpm).singleton_first_page;
            (*root).u.leaf_key[0].npages = (*fpm).singleton_npages;
            relptr_store(base, &mut (*fpm).btree_root, root);
            (*fpm).singleton_first_page = 0;
            (*fpm).singleton_npages = 0;
            (*fpm).btree_depth = 1;

            // Corner case: the btree root may have taken the very last free
            // page, leaving the sole btree entry covering a zero-page run,
            // which is invalid. Overwrite it with the entry we're inserting.
            if (*root).u.leaf_key[0].npages == 0 {
                (*root).u.leaf_key[0].first_page = first_page;
                (*root).u.leaf_key[0].npages = npages;
                free_page_push_span_leader(fpm, first_page, npages);
                return Ok(npages);
            }

            // Fall through to insert the new key.
        }
    }

    // Search the btree.
    let mut result = free_page_btree_search(fpm, first_page);
    debug_assert!(!result.found);
    let mut prevkey: *mut FreePageBtreeLeafKey = core::ptr::null_mut();
    let mut nextkey: *mut FreePageBtreeLeafKey = core::ptr::null_mut();
    let np: *mut FreePageBtree;
    let nindex: Size;
    if result.index > 0 {
        prevkey = (*result.page).u.leaf_key.as_mut_ptr().add(result.index - 1);
    }
    if result.index < (*result.page).hdr.nused {
        np = result.page;
        nindex = result.index;
        nextkey = (*result.page).u.leaf_key.as_mut_ptr().add(result.index);
    } else {
        np = free_page_btree_find_right_sibling(base, result.page);
        nindex = 0;
        if !np.is_null() {
            nextkey = (*np).u.leaf_key.as_mut_ptr();
        }
    }

    // Consolidate with the previous entry if possible.
    if !prevkey.is_null() && (*prevkey).first_page + (*prevkey).npages >= first_page {
        let mut remove_next = false;

        debug_assert_eq!((*prevkey).first_page + (*prevkey).npages, first_page);
        (*prevkey).npages = (first_page - (*prevkey).first_page) + npages;

        // Check whether we can *also* consolidate with the following entry.
        if !nextkey.is_null()
            && (*prevkey).first_page + (*prevkey).npages >= (*nextkey).first_page
        {
            debug_assert_eq!(
                (*prevkey).first_page + (*prevkey).npages,
                (*nextkey).first_page
            );
            (*prevkey).npages =
                ((*nextkey).first_page - (*prevkey).first_page) + (*nextkey).npages;
            free_page_pop_span_leader(fpm, (*nextkey).first_page);
            remove_next = true;
        }

        // Put the span on the correct freelist and save size.
        free_page_pop_span_leader(fpm, (*prevkey).first_page);
        free_page_push_span_leader(fpm, (*prevkey).first_page, (*prevkey).npages);
        let result_npages = (*prevkey).npages;

        // If we consolidated with both neighbors, remove the following entry
        // last, because removing an element from the btree may invalidate
        // pointers we hold into the current data structure.
        if remove_next {
            free_page_btree_remove(fpm, np, nindex);
        }

        return Ok(result_npages);
    }

    // Consolidate with the next entry if possible.
    if !nextkey.is_null() && first_page + npages >= (*nextkey).first_page {
        // Compute new size for span.
        debug_assert_eq!(first_page + npages, (*nextkey).first_page);
        let newpages = ((*nextkey).first_page - first_page) + (*nextkey).npages;

        // Put span on correct free list.
        free_page_pop_span_leader(fpm, (*nextkey).first_page);
        free_page_push_span_leader(fpm, first_page, newpages);

        // Update key in place.
        (*nextkey).first_page = first_page;
        (*nextkey).npages = newpages;

        // If reducing first key on page, ancestors might need adjustment.
        if nindex == 0 {
            free_page_btree_adjust_ancestor_keys(fpm, np);
        }

        return Ok((*nextkey).npages);
    }

    // Split leaf page and as many of its ancestors as necessary.
    if result.split_pages > 0 {
        // If this is a soft insert, it's time to give up.
        if soft {
            return Ok(0);
        }

        // Check whether we need to allocate more btree pages to split.
        if result.split_pages > (*fpm).btree_recycle_count {
            // This should never fail: if there are enough free spans kicking
            // around that we need extra storage just to remember them all,
            // we certainly have enough to expand the btree.
            let pages_needed = (result.split_pages - (*fpm).btree_recycle_count) as Size;
            for _ in 0..pages_needed {
                let Some(recycle_page) = free_page_manager_get_internal(fpm, 1) else {
                    return Err(btree_corrupt());
                };
                free_page_btree_recycle(fpm, recycle_page);
            }

            // The act of allocating pages to recycle may have invalidated the
            // results of our previous btree search, so repeat it.
            result = free_page_btree_search(fpm, first_page);

            // Allocating pages for the btree should never make any page more
            // full, so the new split depth should be no greater than before.
            debug_assert!(result.split_pages <= (*fpm).btree_recycle_count);
        }

        // If we still need to perform a split, do it.
        if result.split_pages > 0 {
            let mut split_target = result.page;
            let mut child: *mut FreePageBtree = core::ptr::null_mut();
            let mut key = first_page;

            loop {
                // Identify parent page, which must receive a downlink.
                let parent: *mut FreePageBtree =
                    relptr_access(base, (*split_target).hdr.parent);

                // Split the page - downlink not added yet.
                let newsibling = free_page_btree_split_page(fpm, split_target);

                // We're always carrying a pending insertion: on the first
                // pass the actual key, on later passes the downlink produced
                // by the previous split. Since we just split the page,
                // there's room on one of the two resulting pages.
                if child.is_null() {
                    let insert_into = if key < (*newsibling).u.leaf_key[0].first_page {
                        split_target
                    } else {
                        newsibling
                    };
                    let index = free_page_btree_search_leaf(insert_into, key);
                    free_page_btree_insert_leaf(insert_into, index, key, npages);
                    if index == 0 && insert_into == split_target {
                        free_page_btree_adjust_ancestor_keys(fpm, split_target);
                    }
                } else {
                    let insert_into = if key < (*newsibling).u.internal_key[0].first_page {
                        split_target
                    } else {
                        newsibling
                    };
                    let index = free_page_btree_search_internal(insert_into, key);
                    free_page_btree_insert_internal(base, insert_into, index, key, child);
                    relptr_store(base, &mut (*child).hdr.parent, insert_into);
                    if index == 0 && insert_into == split_target {
                        free_page_btree_adjust_ancestor_keys(fpm, split_target);
                    }
                }

                // If the page we just split has no parent, split the root.
                if parent.is_null() {
                    let newroot = free_page_btree_get_recycled(fpm);
                    (*newroot).hdr.magic = FREE_PAGE_INTERNAL_MAGIC;
                    (*newroot).hdr.nused = 2;
                    relptr_store(
                        base,
                        &mut (*newroot).hdr.parent,
                        core::ptr::null_mut::<FreePageBtree>(),
                    );
                    (*newroot).u.internal_key[0].first_page =
                        free_page_btree_first_key(split_target);
                    relptr_store(base, &mut (*newroot).u.internal_key[0].child, split_target);
                    relptr_store(base, &mut (*split_target).hdr.parent, newroot);
                    (*newroot).u.internal_key[1].first_page =
                        free_page_btree_first_key(newsibling);
                    relptr_store(base, &mut (*newroot).u.internal_key[1].child, newsibling);
                    relptr_store(base, &mut (*newsibling).hdr.parent, newroot);
                    relptr_store(base, &mut (*fpm).btree_root, newroot);
                    (*fpm).btree_depth += 1;

                    break;
                }

                // If the parent page isn't full, insert the downlink.
                //
                // The C reads u.internal_key[0].first_page even when the new
                // sibling is a leaf; both union arms place first_page at
                // offset 0, so this is the sibling's first key either way.
                key = (*newsibling).u.internal_key[0].first_page;
                if (*parent).hdr.nused < FPM_ITEMS_PER_INTERNAL_PAGE {
                    let index = free_page_btree_search_internal(parent, key);
                    free_page_btree_insert_internal(base, parent, index, key, newsibling);
                    relptr_store(base, &mut (*newsibling).hdr.parent, parent);
                    if index == 0 {
                        free_page_btree_adjust_ancestor_keys(fpm, parent);
                    }
                    break;
                }

                // The parent also needs to be split, so loop around.
                child = newsibling;
                split_target = parent;
            }

            // The loop above did the insert, so just need to update the free
            // list, and we're done.
            free_page_push_span_leader(fpm, first_page, npages);

            return Ok(npages);
        }
    }

    // Physically add the key to the page.
    debug_assert!((*result.page).hdr.nused < FPM_ITEMS_PER_LEAF_PAGE);
    free_page_btree_insert_leaf(result.page, result.index, first_page, npages);

    // If new first key on page, ancestors might need adjustment.
    if result.index == 0 {
        free_page_btree_adjust_ancestor_keys(fpm, result.page);
    }

    // Put it on the free list.
    free_page_push_span_leader(fpm, first_page, npages);

    Ok(npages)
}

/// `FreePagePopSpanLeader` — remove a `FreePageSpanLeader` from the linked
/// list that contains it, either because we're changing the size of the span
/// or because we're allocating it.
unsafe fn free_page_pop_span_leader(fpm: *mut FreePageManager, pageno: Size) {
    let base = fpm_segment_base(fpm);
    let span = fpm_page_to_pointer(base, pageno) as *mut FreePageSpanLeader;

    let next: *mut FreePageSpanLeader = relptr_access(base, (*span).next);
    let prev: *mut FreePageSpanLeader = relptr_access(base, (*span).prev);
    if !next.is_null() {
        (*next).prev = (*span).prev;
    }
    if !prev.is_null() {
        (*prev).next = (*span).next;
    } else {
        let f = (*span).npages.min(FPM_NUM_FREELISTS) - 1;

        debug_assert_eq!(relptr_offset((*fpm).freelist[f]), pageno * FPM_PAGE_SIZE);
        (*fpm).freelist[f] = (*span).next;
    }
}

/// `FreePagePushSpanLeader` — initialize a new `FreePageSpanLeader` and put
/// it on the appropriate free list.
unsafe fn free_page_push_span_leader(fpm: *mut FreePageManager, first_page: Size, npages: Size) {
    let base = fpm_segment_base(fpm);
    let f = npages.min(FPM_NUM_FREELISTS) - 1;
    let head: *mut FreePageSpanLeader = relptr_access(base, (*fpm).freelist[f]);

    let span = fpm_page_to_pointer(base, first_page) as *mut FreePageSpanLeader;
    (*span).magic = FREE_PAGE_SPAN_LEADER_MAGIC;
    (*span).npages = npages;
    relptr_store(base, &mut (*span).next, head);
    relptr_store(
        base,
        &mut (*span).prev,
        core::ptr::null_mut::<FreePageSpanLeader>(),
    );
    if !head.is_null() {
        relptr_store(base, &mut (*head).prev, span);
    }
    relptr_store(base, &mut (*fpm).freelist[f], span);
}

/// Append a `Size` in decimal to the dump buffer (the `%zu` conversions),
/// charging the buffer's context fallibly.
fn push_size(buf: &mut PgString<'_>, value: Size) -> PgResult<()> {
    // A usize prints in at most 20 decimal digits.
    let mut tmp = [0u8; 20];
    let mut pos = tmp.len();
    let mut v = value;
    loop {
        pos -= 1;
        tmp[pos] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let s = core::str::from_utf8(&tmp[pos..]).expect("ascii digits are valid utf-8");
    buf.try_push_str(s)
}

// C function map (every function in backend/utils/mmgr/freepage.c):
//   FreePageManagerInitialize        -> free_page_manager_initialize
//   FreePageManagerGet               -> free_page_manager_get
//   FreePageManagerPut               -> free_page_manager_put
//   FreePageManagerDump              -> free_page_manager_dump
//   FreePageManagerLargestContiguous -> free_page_manager_largest_contiguous
//   FreePageManagerUpdateLargest     -> free_page_manager_update_largest
//   FreePageManagerGetInternal       -> free_page_manager_get_internal
//   FreePageManagerPutInternal       -> free_page_manager_put_internal
//   FreePageManagerDumpBtree         -> free_page_manager_dump_btree
//   FreePageManagerDumpSpans         -> free_page_manager_dump_spans
//   FreePageBtreeAdjustAncestorKeys  -> free_page_btree_adjust_ancestor_keys
//   FreePageBtreeCleanup             -> free_page_btree_cleanup
//   FreePageBtreeConsolidate         -> free_page_btree_consolidate
//   FreePageBtreeFindLeftSibling     -> free_page_btree_find_left_sibling
//   FreePageBtreeFindRightSibling    -> free_page_btree_find_right_sibling
//   FreePageBtreeFirstKey            -> free_page_btree_first_key
//   FreePageBtreeGetRecycled         -> free_page_btree_get_recycled
//   FreePageBtreeInsertInternal      -> free_page_btree_insert_internal
//   FreePageBtreeInsertLeaf          -> free_page_btree_insert_leaf
//   FreePageBtreeRecycle             -> free_page_btree_recycle
//   FreePageBtreeRemove              -> free_page_btree_remove
//   FreePageBtreeRemovePage          -> free_page_btree_remove_page
//   FreePageBtreeSearch              -> free_page_btree_search
//   FreePageBtreeSearchInternal      -> free_page_btree_search_internal
//   FreePageBtreeSearchLeaf          -> free_page_btree_search_leaf
//   FreePageBtreeSplitPage           -> free_page_btree_split_page
//   FreePageBtreeUpdateParentPointers-> free_page_btree_update_parent_pointers
//   FreePagePopSpanLeader            -> free_page_pop_span_leader
//   FreePagePushSpanLeader           -> free_page_push_span_leader
//   fpm_size_to_pages (macro)        -> fpm_size_to_pages
//   fpm_largest (macro)              -> fpm_largest
//   sum_free_pages, sum_free_pages_recurse -> not ported (FPM_EXTRA_ASSERTS
//     only; inactive in a default build).

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    /// One page-aligned page of a fake segment, like a DSM segment.
    #[repr(C, align(4096))]
    struct Page([u8; 4096]);

    struct TestFpm<const PAGES: usize> {
        seg: Box<[Page]>,
        first_usable: Size,
    }

    impl<const PAGES: usize> TestFpm<PAGES> {
        /// Mirror dsm_shmem_init: the manager sits at the segment base and
        /// the pages covering it are never handed out.
        fn new() -> Self {
            let seg = (0..PAGES)
                .map(|_| Page([0u8; 4096]))
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let mut t = TestFpm {
                seg,
                first_usable: 0,
            };
            let base = t.base();
            let fpm = base as *mut FreePageManager;
            let mut first_page: Size = 0;
            while first_page * FPM_PAGE_SIZE < core::mem::size_of::<FreePageManager>() {
                first_page += 1;
            }
            t.first_usable = first_page;
            free_page_manager_initialize(fpm, base);
            free_page_manager_put(fpm, first_page, PAGES - first_page).unwrap();
            t
        }

        fn base(&mut self) -> *mut u8 {
            self.seg.as_mut_ptr() as *mut u8
        }

        fn fpm(&mut self) -> *mut FreePageManager {
            self.base() as *mut FreePageManager
        }

        fn usable(&self) -> Size {
            PAGES - self.first_usable
        }
    }

    fn install_test_seams() {
        if !stack_depth_seams::check_stack_depth::is_installed() {
            stack_depth_seams::check_stack_depth::set(|| Ok(()));
        }
    }

    #[test]
    fn size_to_pages_rounds_up() {
        assert_eq!(fpm_size_to_pages(0), 0);
        assert_eq!(fpm_size_to_pages(1), 1);
        assert_eq!(fpm_size_to_pages(FPM_PAGE_SIZE), 1);
        assert_eq!(fpm_size_to_pages(FPM_PAGE_SIZE + 1), 2);
    }

    #[test]
    fn items_per_page_match_c() {
        // (4096 - 24) / 16 on LP64.
        assert_eq!(FPM_ITEMS_PER_INTERNAL_PAGE, 254);
        assert_eq!(FPM_ITEMS_PER_LEAF_PAGE, 254);
    }

    #[test]
    fn init_get_put_round_trip() {
        let mut t = TestFpm::<16>::new();
        let fpm = t.fpm();
        let usable = t.usable();

        assert_eq!(fpm_largest(fpm), usable);
        let p = free_page_manager_get(fpm, 4).unwrap();
        assert_eq!(p, t.first_usable);
        assert_eq!(fpm_largest(fpm), usable - 4);

        // No run of `usable` pages remains.
        assert_eq!(free_page_manager_get(fpm, usable), None);

        // Put it back; consolidation restores the single big run.
        free_page_manager_put(fpm, p, 4).unwrap();
        assert_eq!(fpm_largest(fpm), usable);
        assert_eq!(free_page_manager_get(fpm, usable), Some(t.first_usable));
        assert_eq!(free_page_manager_get(fpm, 1), None);
    }

    #[test]
    fn singleton_prepend_consolidates() {
        let mut t = TestFpm::<16>::new();
        let fpm = t.fpm();
        let usable = t.usable();

        let a = free_page_manager_get(fpm, 2).unwrap();
        let b = free_page_manager_get(fpm, 3).unwrap();
        assert_eq!(b, a + 2);

        // Free in reverse order: b first (follows the remaining singleton's
        // predecessor gap), then a (immediately precedes b's run).
        free_page_manager_put(fpm, b, 3).unwrap();
        free_page_manager_put(fpm, a, 2).unwrap();
        assert_eq!(fpm_largest(fpm), usable);
    }

    #[test]
    fn best_fit_prefers_smallest_sufficient_span() {
        let mut t = TestFpm::<64>::new();
        let fpm = t.fpm();

        // Carve the space into separated free runs of size 5 and 10 by
        // allocating everything and freeing selected ranges.
        let total = t.usable();
        let start = free_page_manager_get(fpm, total).unwrap();
        free_page_manager_put(fpm, start, 10).unwrap();
        free_page_manager_put(fpm, start + 12, 5).unwrap();

        // Initializing the btree for the second put consumed page `start`
        // (the btree root is carved from the 10-run), leaving a 9-run at
        // start+1 and the 5-run at start+12.
        // Best fit for 4 is the 5-run, not the 9-run.
        assert_eq!(free_page_manager_get(fpm, 4), Some(start + 12));
        // Remaining 1-page tail at start+16; next 4 comes from the 9-run.
        assert_eq!(free_page_manager_get(fpm, 4), Some(start + 1));
        // A single page: smallest sufficient run is the 1-page tail.
        assert_eq!(free_page_manager_get(fpm, 1), Some(start + 16));
    }

    #[test]
    fn scattered_free_builds_btree_and_remerges() {
        const PAGES: usize = 2048;
        let mut t = TestFpm::<PAGES>::new();
        let fpm = t.fpm();
        let first = t.first_usable;
        let total = t.usable();

        // Drain every page one at a time.
        let mut got = 0;
        while free_page_manager_get(fpm, 1).is_some() {
            got += 1;
        }
        assert_eq!(got, total);
        assert_eq!(fpm_largest(fpm), 0);

        // Free all odd pages: > FPM_ITEMS_PER_LEAF_PAGE scattered spans, so
        // the btree must initialize, split leaves, and grow internal levels.
        for p in (first..first + total).filter(|p| p % 2 == 1) {
            free_page_manager_put(fpm, p, 1).unwrap();
        }
        assert_eq!(fpm_largest(fpm), 1);

        // Free the even pages, bridging everything back together (btree
        // pages themselves get consumed/recycled along the way).
        for p in (first..first + total).filter(|p| p % 2 == 0) {
            free_page_manager_put(fpm, p, 1).unwrap();
        }

        // Everything must consolidate back into one run.
        assert_eq!(free_page_manager_get(fpm, total), Some(first));
        assert_eq!(free_page_manager_get(fpm, 1), None);
        free_page_manager_put(fpm, first, total).unwrap();
        assert_eq!(fpm_largest(fpm), total);
    }

    #[test]
    fn dump_reports_state() {
        install_test_seams();
        let mut t = TestFpm::<64>::new();
        let fpm = t.fpm();
        let ctx = MemoryContext::new("freepage-dump-test");

        let dump = free_page_manager_dump(fpm, ctx.mcx()).unwrap();
        let s = dump.as_str();
        assert!(s.contains("metadata: self "), "{s}");
        assert!(s.contains("singleton: "), "{s}");
        assert!(s.contains("freelists:"), "{s}");
        drop(dump);

        // Force a btree, then dump again.
        let total = t.usable();
        let start = free_page_manager_get(fpm, total).unwrap();
        free_page_manager_put(fpm, start, 2).unwrap();
        free_page_manager_put(fpm, start + 4, 2).unwrap();
        let dump = free_page_manager_dump(fpm, ctx.mcx()).unwrap();
        let s = dump.as_str();
        assert!(s.contains("btree depth 1:"), "{s}");
    }

    #[test]
    fn get_of_zero_or_oversized_fails() {
        let mut t = TestFpm::<16>::new();
        let fpm = t.fpm();
        assert_eq!(free_page_manager_get(fpm, 0), None);
        assert_eq!(free_page_manager_get(fpm, t.usable() + 1), None);
    }
}
