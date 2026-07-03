//! GIN index AM (ginutil.c / ginpostinglist.c / ginentrypage.c /
//! gindatapage.c / ginbtree.c / ginbulk.c / gininsert.c / ginfast.c /
//! ginscan.c / ginget.c / ginlogic.c). Loud, never silent: VACUUM entry
//! points, partial match, parallel build, array_ops, multicolumn indexes,
//! non-default reloptions.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub(crate) mod btree;
pub(crate) mod bulk;
pub(crate) mod datapage;
pub(crate) mod entrypage;
pub(crate) mod fast;
pub(crate) mod get;
pub(crate) mod insert;
pub(crate) mod logic;
pub(crate) mod opclass;
pub(crate) mod postinglist;
pub(crate) mod scan;
pub(crate) mod util;
pub(crate) mod wal;

use ::bufmgr_seams as bm;
use ::gin_vocab::*;
use ::types_core::{Buffer, BLCKSZ};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef};

pub use get::gingetbitmap;
pub use insert::{ginbuildempty, ginEntryInsert, gininsert};
pub use scan::{ginbeginscan, ginendscan, ginrescan};
pub use opclass::gincost_extract_query;
pub use util::{ginGetStats, ginUpdateStats};

pub(crate) const RM_GIN: u8 = ::types_core::primitive::RmgrIds::RM_GIN_ID as u8;

pub(crate) const GIN_UNLOCK: i32 = bm::BUFFER_LOCK_UNLOCK;
pub(crate) const GIN_SHARE: i32 = bm::BUFFER_LOCK_SHARE;
pub(crate) const GIN_EXCLUSIVE: i32 = bm::BUFFER_LOCK_EXCLUSIVE;

/// Build-path surface for the `ginbuild` crate (execindexing cycle split).
pub mod build {
    pub use crate::bulk::BuildAccumulator;
    pub use crate::util::{
        ginExtractEntries, ginUpdateStats, initGinState, GinInitBuffer, GinInitMetabuffer,
        GinNewBuffer,
    };
    pub use crate::{check_for_interrupts, relation_needs_wal};
}

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported: gin {what}")
}

#[cold]
#[inline(never)]
pub(crate) fn oom(n: usize) -> Box<::types_error::PgError> {
    Box::new(::types_error::PgError::error(format!(
        "out of memory ({n} bytes in gin)"
    )))
}

pub fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        unported("ProcessInterrupts (tcop/postgres.c)");
    }
}

// RelationNeedsWAL (rel.h), as nbtree/hash render it.
pub fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == ::types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get()
                    == ::types_core::InvalidSubTransactionId))
}

/// # Safety
/// `buf` is pinned; readers of the returned view hold at least a share lock.
pub(crate) unsafe fn page_ref<'p>(buf: Buffer) -> PageRef<'p> {
    unsafe { PageRef::from_raw(bm::buffer_get_page::call(buf)) }
}

/// # Safety
/// `buf` is pinned and exclusively locked for the borrow.
pub(crate) unsafe fn page_mut<'p>(buf: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bm::buffer_get_page::call(buf)) }
}

const SIZEOF_OPAQUE: usize = core::mem::size_of::<GinPageOpaqueData>();
const OPAQUE_OFF: usize = BLCKSZ - SIZEOF_OPAQUE;

// GIN page opaque accessors work over raw images too (temp pages, redo).

#[inline]
pub(crate) fn opaque_of(bytes: &[u8]) -> GinPageOpaqueData {
    debug_assert!(bytes.len() == BLCKSZ);
    // SAFETY: in-bounds 4-aligned special area of a BLCKSZ page image.
    unsafe { bytes.as_ptr().add(OPAQUE_OFF).cast::<GinPageOpaqueData>().read() }
}

#[inline]
pub(crate) fn write_opaque_to(bytes: &mut [u8], opaque: &GinPageOpaqueData) {
    debug_assert!(bytes.len() == BLCKSZ);
    // SAFETY: as opaque_of; exclusive access to the image.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(OPAQUE_OFF)
            .cast::<GinPageOpaqueData>()
            .write(*opaque)
    }
}

#[inline]
pub(crate) fn page_opaque(page: &PageRef<'_>) -> GinPageOpaqueData {
    debug_assert!(page.pd_special() as usize == OPAQUE_OFF);
    // SAFETY: in-bounds 4-aligned special area of a gin page.
    unsafe { page.as_ptr().add(OPAQUE_OFF).cast::<GinPageOpaqueData>().read() }
}

#[inline]
pub(crate) fn write_opaque(page: &mut PageMut<'_>, opaque: &GinPageOpaqueData) {
    // SAFETY: as page_opaque; exclusive page access.
    unsafe {
        page.as_ref()
            .as_ptr()
            .cast_mut()
            .add(OPAQUE_OFF)
            .cast::<GinPageOpaqueData>()
            .write(*opaque)
    }
}

#[inline]
pub(crate) fn page_bytes<'p>(page: &PageRef<'p>) -> &'p [u8] {
    // SAFETY: a page is a BLCKSZ image; lifetime rides the PageRef contract.
    unsafe { core::slice::from_raw_parts(page.as_ptr(), BLCKSZ) }
}

/// # Safety
/// Caller holds the exclusive lock; the borrow must not outlive it.
#[inline]
pub(crate) unsafe fn page_bytes_mut<'p>(page: &mut PageMut<'p>) -> &'p mut [u8] {
    unsafe { core::slice::from_raw_parts_mut(page.as_ref().as_ptr().cast_mut(), BLCKSZ) }
}

#[inline]
pub(crate) fn GinPageIsLeaf(o: &GinPageOpaqueData) -> bool {
    o.flags & GIN_LEAF != 0
}

#[inline]
pub(crate) fn GinPageIsData(o: &GinPageOpaqueData) -> bool {
    o.flags & GIN_DATA != 0
}

#[inline]
pub(crate) fn GinPageIsList(o: &GinPageOpaqueData) -> bool {
    o.flags & GIN_LIST != 0
}

#[inline]
pub(crate) fn GinPageIsDeleted(o: &GinPageOpaqueData) -> bool {
    o.flags & GIN_DELETED != 0
}

#[inline]
pub(crate) fn GinPageIsCompressed(o: &GinPageOpaqueData) -> bool {
    o.flags & GIN_COMPRESSED != 0
}

#[inline]
pub(crate) fn GinPageIsIncompleteSplit(o: &GinPageOpaqueData) -> bool {
    o.flags & GIN_INCOMPLETE_SPLIT != 0
}

#[inline]
pub(crate) fn GinPageRightMost(o: &GinPageOpaqueData) -> bool {
    o.rightlink == ::types_core::InvalidBlockNumber
}

pub(crate) const META_OFF: usize = SizeOfPageHeaderData;

/// GinPageGetMeta over a raw page image.
#[inline]
pub(crate) fn meta_of(bytes: &[u8]) -> GinMetaPageData {
    // SAFETY: metapage contents at MAXALIGN(SizeOfPageHeaderData) == 24,
    // 8-aligned within the BLCKSZ image.
    unsafe { bytes.as_ptr().add(META_OFF).cast::<GinMetaPageData>().read_unaligned() }
}

#[inline]
pub(crate) fn write_meta_to(bytes: &mut [u8], meta: &GinMetaPageData) {
    // SAFETY: as meta_of; exclusive access.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(META_OFF)
            .cast::<GinMetaPageData>()
            .write_unaligned(*meta)
    }
}

/// Single-reserve slice append (C memcpy shape; PgVec extend_from_slice is the
/// per-byte trap for u8 and unproven for PODs).
pub(crate) fn vec_append<'mcx, T: Copy>(
    v: &mut ::mcx::PgVec<'mcx, T>,
    src: &[T],
) -> ::types_error::PgResult<()> {
    let n = src.len();
    if n == 0 {
        return Ok(());
    }
    v.try_reserve(n).map_err(|_| oom(n * core::mem::size_of::<T>()))?;
    let old = v.len();
    // SAFETY: capacity reserved; src/dst disjoint; set_len covers n items.
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), v.as_mut_ptr().add(old), n);
        v.set_len(old + n);
    }
    Ok(())
}

/// ginbulkdelete/ginvacuumcleanup: the VACUUM lane, loud per the port scope.
pub fn ginbulkdelete() -> ! {
    unported("ginbulkdelete (VACUUM lane; land backend-access-gin-ginvacuum)")
}

pub fn ginvacuumcleanup() -> ! {
    unported("ginvacuumcleanup (VACUUM lane; land backend-access-gin-ginvacuum)")
}

/// GUC storage owned by this unit (C: ginfast.c gin_pending_list_limit,
/// ginget.c GinFuzzySearchLimit).
pub fn init_seams() {
    use std::cell::Cell;
    thread_local! {
        static GIN_FUZZY_SEARCH_LIMIT: Cell<i32> = const { Cell::new(0) };
        static GIN_PENDING_LIST_LIMIT: Cell<i32> = const { Cell::new(4096) };
    }
    guc_tables::vars::GinFuzzySearchLimit.install(guc_tables::GucVarAccessors {
        get: || GIN_FUZZY_SEARCH_LIMIT.with(Cell::get),
        set: |v| GIN_FUZZY_SEARCH_LIMIT.with(|c| c.set(v)),
    });
    guc_tables::vars::gin_pending_list_limit.install(guc_tables::GucVarAccessors {
        get: || GIN_PENDING_LIST_LIMIT.with(Cell::get),
        set: |v| GIN_PENDING_LIST_LIMIT.with(|c| c.set(v)),
    });
}

#[cfg(test)]
mod tests;
