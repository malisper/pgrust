//! On-disk / ABI structures and constants for the nbtree access method.
//!
//! These mirror `src/include/access/nbtree.h` from PostgreSQL 18.3.  Only the
//! types that genuinely cross the on-disk / page-layout boundary live here as
//! `#[repr(C)]` with compile-time layout assertions; the purely internal
//! runtime structures of the access method live inside
//! `backend-access-nbtree` itself as idiomatic Rust.

use crate::{
    uint16, uint32, BlockNumber, FullTransactionId, ItemIdData, ItemPointerData, PageHeaderData,
    BLCKSZ,
};

/// `float8` from c.h.
pub type float8 = f64;

/// There's room for a 16-bit vacuum cycle ID in `BTPageOpaqueData`.
pub type BTCycleId = uint16;

const MAXIMUM_ALIGNOF: usize = 8;

const fn maxalign(size: usize) -> usize {
    (size + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

const fn maxalign_down(size: usize) -> usize {
    size & !(MAXIMUM_ALIGNOF - 1)
}

/// `SizeOfPageHeaderData` == `offsetof(PageHeaderData, pd_linp)`.
const SIZE_OF_PAGE_HEADER_DATA: usize = core::mem::offset_of!(PageHeaderData, pd_linp);

/// `BTPageOpaqueData` -- stored in the special area at the end of every page.
///
/// We store a pointer to both siblings in the tree, the page's btree level,
/// flag bits indicating page type/status, and a vacuum cycle ID.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTPageOpaqueData {
    /// left sibling, or `P_NONE` if leftmost
    pub btpo_prev: BlockNumber,
    /// right sibling, or `P_NONE` if rightmost
    pub btpo_next: BlockNumber,
    /// tree level --- zero for leaf pages
    pub btpo_level: uint32,
    /// flag bits, see below
    pub btpo_flags: uint16,
    /// vacuum cycle ID of latest split
    pub btpo_cycleid: BTCycleId,
}

/// Bits defined in `btpo_flags`.
pub const BTP_LEAF: uint16 = 1 << 0; // leaf page, i.e. not internal page
pub const BTP_ROOT: uint16 = 1 << 1; // root page (has no parent)
pub const BTP_DELETED: uint16 = 1 << 2; // page has been deleted from tree
pub const BTP_META: uint16 = 1 << 3; // meta-page
pub const BTP_HALF_DEAD: uint16 = 1 << 4; // empty, but still in tree
pub const BTP_SPLIT_END: uint16 = 1 << 5; // rightmost page of split group
pub const BTP_HAS_GARBAGE: uint16 = 1 << 6; // page has LP_DEAD tuples (deprecated)
pub const BTP_INCOMPLETE_SPLIT: uint16 = 1 << 7; // right sibling's downlink is missing
pub const BTP_HAS_FULLXID: uint16 = 1 << 8; // contains BTDeletedPageData

/// The max allowed value of a cycle ID is a bit less than 64K.
pub const MAX_BT_CYCLE_ID: BTCycleId = 0xFF7F;

/// `BTMetaPageData` -- the contents of the metapage (always the first page).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTMetaPageData {
    /// should contain `BTREE_MAGIC`
    pub btm_magic: uint32,
    /// nbtree version (always <= `BTREE_VERSION`)
    pub btm_version: uint32,
    /// current root location
    pub btm_root: BlockNumber,
    /// tree level of the root page
    pub btm_level: uint32,
    /// current "fast" root location
    pub btm_fastroot: BlockNumber,
    /// tree level of the "fast" root page
    pub btm_fastlevel: uint32,
    /// number of deleted, non-recyclable pages during last cleanup
    pub btm_last_cleanup_num_delpages: uint32,
    /// number of heap tuples during last cleanup (deprecated)
    pub btm_last_cleanup_num_heap_tuples: float8,
    /// are all columns "equalimage"?
    pub btm_allequalimage: bool,
}

pub const BTREE_METAPAGE: BlockNumber = 0; // first page is meta
pub const BTREE_MAGIC: uint32 = 0x0005_3162; // magic number in metapage
pub const BTREE_VERSION: uint32 = 4; // current version number
pub const BTREE_MIN_VERSION: uint32 = 2; // minimum supported version
pub const BTREE_NOVAC_VERSION: uint32 = 3; // version with all meta fields set

/// `BTMaxItemSize`: maximum size of a btree index entry, including its tuple
/// header.  We need to be able to fit three items on every page, so restrict
/// any one item to 1/3 the per-page available space, accounting for the
/// tiebreaker heap-TID attribute `_bt_truncate()` may add.
pub const BTMaxItemSize: usize = maxalign_down(
    (BLCKSZ
        - maxalign(SIZE_OF_PAGE_HEADER_DATA + 3 * core::mem::size_of::<ItemIdData>())
        - maxalign(core::mem::size_of::<BTPageOpaqueData>()))
        / 3,
) - maxalign(core::mem::size_of::<ItemPointerData>());

pub const BTMaxItemSizeNoHeapTid: usize = maxalign_down(
    (BLCKSZ
        - maxalign(SIZE_OF_PAGE_HEADER_DATA + 3 * core::mem::size_of::<ItemIdData>())
        - maxalign(core::mem::size_of::<BTPageOpaqueData>()))
        / 3,
);

/// `MaxTIDsPerBTreePage`: upper bound on the number of heap TIDs that may be
/// stored on a btree leaf page; used to size per-page temporary buffers.
pub const MaxTIDsPerBTreePage: usize =
    (BLCKSZ - SIZE_OF_PAGE_HEADER_DATA - core::mem::size_of::<BTPageOpaqueData>())
        / core::mem::size_of::<ItemPointerData>();

// BTREE_MIN_FILLFACTOR / BTREE_DEFAULT_FILLFACTOR are defined in `reloptions`
// (drawn from access/nbtree.h) and re-exported from the crate root.
pub const BTREE_NONLEAF_FILLFACTOR: i32 = 70;
pub const BTREE_SINGLEVAL_FILLFACTOR: i32 = 96;

/// A special value to indicate "no page number".
pub const P_NONE: BlockNumber = 0;

/// `BTDeletedPageData` is the page contents of a deleted page.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTDeletedPageData {
    /// see `BTPageIsRecyclable()`
    pub safexid: FullTransactionId,
}

/// `INDEX_ALT_TID_MASK` -- set in an alternative-TID (pivot or posting) tuple.
pub const INDEX_ALT_TID_MASK: uint16 = crate::INDEX_AM_RESERVED_BIT;

/// Item pointer offset bit masks.
pub const BT_OFFSET_MASK: uint16 = 0x0FFF;
pub const BT_STATUS_OFFSET_MASK: uint16 = 0xF000;
/// `BT_STATUS_OFFSET_MASK` status bits.
pub const BT_PIVOT_HEAP_TID_ATTR: uint16 = 0x1000;
pub const BT_IS_POSTING: uint16 = 0x2000;

/// `BTreeTupleData` -- the on-disk index-tuple header used by nbtree (this is
/// physically an `IndexTupleData`; nbtree overloads `t_tid` and `t_info`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTreeTupleData {
    pub t_tid: ItemPointerData,
    pub t_info: uint16,
}

/// `BTOptions` -- the parsed contents of an nbtree relation's reloptions.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTOptions {
    /// varlena header (do not touch directly!)
    pub varlena_header_: i32,
    /// page fill factor in percent (0..100)
    pub fillfactor: i32,
    /// deprecated
    pub vacuum_cleanup_index_scale_factor: float8,
    /// Try to deduplicate items?
    pub deduplicate_items: bool,
}

/// `SK_BT_*` scan-key private flag bits (high byte of `sk_flags`).
pub const SK_BT_REQFWD: i32 = 0x0001_0000; // required to continue forward scan
pub const SK_BT_REQBKWD: i32 = 0x0002_0000; // required to continue backward scan
pub const SK_BT_SKIP: i32 = 0x0004_0000; // skip array on column without input = key
pub const SK_BT_MINVAL: i32 = 0x0008_0000; // invalid sk_argument, use low_compare
pub const SK_BT_MAXVAL: i32 = 0x0010_0000; // invalid sk_argument, use high_compare
pub const SK_BT_NEXT: i32 = 0x0020_0000; // positions the scan > sk_argument
pub const SK_BT_PRIOR: i32 = 0x0040_0000; // positions the scan < sk_argument

/// Remaps pg_index flag bits to uppermost `SK_BT_*` byte.
pub const SK_BT_INDOPTION_SHIFT: i32 = 24; // must clear the above bits
pub const SK_BT_DESC: i32 = (INDOPTION_DESC as i32) << SK_BT_INDOPTION_SHIFT;
pub const SK_BT_NULLS_FIRST: i32 = (INDOPTION_NULLS_FIRST as i32) << SK_BT_INDOPTION_SHIFT;

/// pg_index index-column option bits (`catalog/pg_index.h`).
pub const INDOPTION_DESC: uint16 = 0x0001; // values are in reverse order
pub const INDOPTION_NULLS_FIRST: uint16 = 0x0002; // NULLs are first instead of last

// `BTMaxStrategyNumber` is defined in `access` and re-exported from the crate
// root.

/// Progress-reporting phase numbers (must match `btbuildphasename`).
/// `PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE` (`commands/progress.h`).
pub const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: i32 = 1;
pub const PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN: i32 = 2;
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_1: i32 = 3;
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_2: i32 = 4;
pub const PROGRESS_BTREE_PHASE_LEAF_LOAD: i32 = 5;

// ---------------------------------------------------------------------------
// WAL (xlog) record definitions -- mirrors `src/include/access/nbtxlog.h`.
//
// These are the on-disk WAL formats for nbtree.  The `info` op-codes are the
// rmgr-specific portion of the WAL record's `xl_info` byte (after masking off
// `XLR_INFO_MASK`).  The `xl_btree_*` structs are the fixed-prefix headers of
// each record (variable-length payload follows in the record's main data or
// per-block data).
// ---------------------------------------------------------------------------

pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00; // add index tuple without split
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x10; // same, on a non-leaf page
pub const XLOG_BTREE_INSERT_META: u8 = 0x20; // same, plus update metapage
pub const XLOG_BTREE_SPLIT_L: u8 = 0x30; // add index tuple with split
pub const XLOG_BTREE_SPLIT_R: u8 = 0x40; // as above, new item on right
pub const XLOG_BTREE_INSERT_POST: u8 = 0x50; // add index tuple with posting split
pub const XLOG_BTREE_DEDUP: u8 = 0x60; // deduplicate tuples for a page
pub const XLOG_BTREE_DELETE: u8 = 0x70; // delete leaf index tuples for a page
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x80; // delete a half-dead page
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x90; // same, and update metapage
pub const XLOG_BTREE_NEWROOT: u8 = 0xA0; // new root page
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0xB0; // mark a leaf as half-dead
pub const XLOG_BTREE_VACUUM: u8 = 0xC0; // delete entries on a page during vacuum
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0; // old page is about to be reused from FSM
pub const XLOG_BTREE_META_CLEANUP: u8 = 0xE0; // update cleanup-related data in metapage

/// `xl_btree_metadata` -- all that we need to regenerate the meta-data page.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_metadata {
    pub version: uint32,
    pub root: BlockNumber,
    pub level: uint32,
    pub fastroot: BlockNumber,
    pub fastlevel: uint32,
    pub last_cleanup_num_delpages: uint32,
    pub allequalimage: bool,
}

/// `xl_btree_insert` -- simple (non-split) insert; used for INSERT_LEAF,
/// INSERT_UPPER, INSERT_META and INSERT_POST.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_insert {
    pub offnum: crate::OffsetNumber,
    // POSTING SPLIT OFFSET FOLLOWS (INSERT_POST case)
    // NEW TUPLE ALWAYS FOLLOWS AT THE END
}
pub const SizeOfBtreeInsert: usize =
    core::mem::offset_of!(xl_btree_insert, offnum) + core::mem::size_of::<crate::OffsetNumber>();

/// `xl_btree_split` -- shared by SPLIT_L and SPLIT_R.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_split {
    /// tree level of page being split
    pub level: uint32,
    /// first origpage item on rightpage
    pub firstrightoff: crate::OffsetNumber,
    /// new item's offset
    pub newitemoff: crate::OffsetNumber,
    /// offset inside orig posting tuple
    pub postingoff: uint16,
}
pub const SizeOfBtreeSplit: usize =
    core::mem::offset_of!(xl_btree_split, postingoff) + core::mem::size_of::<uint16>();

/// `xl_btree_dedup` -- a deduplication pass for a leaf page (an array of
/// `BTDedupInterval` follows).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_dedup {
    pub nintervals: uint16,
    // DEDUPLICATION INTERVALS FOLLOW
}
pub const SizeOfBtreeDedup: usize =
    core::mem::offset_of!(xl_btree_dedup, nintervals) + core::mem::size_of::<uint16>();

/// `xl_btree_reuse_page` -- only exists to generate a conflict point for Hot
/// Standby.  Includes a `RelFileLocator` because the buffer isn't registered.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_reuse_page {
    pub locator: crate::RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    /// to handle recovery conflict during logical decoding on standby
    pub isCatalogRel: bool,
}

/// `xl_btree_vacuum` -- deletion of index tuples on a leaf page during VACUUM.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_vacuum {
    pub ndeleted: uint16,
    pub nupdated: uint16,
}
pub const SizeOfBtreeVacuum: usize =
    core::mem::offset_of!(xl_btree_vacuum, nupdated) + core::mem::size_of::<uint16>();

/// `xl_btree_delete` -- ad-hoc deletion of leaf index tuples (during insert).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_delete {
    pub snapshotConflictHorizon: crate::TransactionId,
    pub ndeleted: uint16,
    pub nupdated: uint16,
    /// to handle recovery conflict during logical decoding on standby
    pub isCatalogRel: bool,
}
pub const SizeOfBtreeDelete: usize =
    core::mem::offset_of!(xl_btree_delete, isCatalogRel) + core::mem::size_of::<bool>();

/// `xl_btree_update` -- metadata for an updated (partial-TID-deletion) posting
/// list tuple; a uint16 array of 0-based posting offsets follows.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_update {
    pub ndeletedtids: uint16,
    // POSTING LIST uint16 OFFSETS TO A DELETED TID FOLLOW
}
pub const SizeOfBtreeUpdate: usize =
    core::mem::offset_of!(xl_btree_update, ndeletedtids) + core::mem::size_of::<uint16>();

/// `xl_btree_mark_page_halfdead` -- marking an empty subtree for deletion.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_mark_page_halfdead {
    /// deleted tuple id in parent page
    pub poffset: crate::OffsetNumber,
    /// leaf block ultimately being deleted
    pub leafblk: BlockNumber,
    /// leaf block's left sibling, if any
    pub leftblk: BlockNumber,
    /// leaf block's right sibling
    pub rightblk: BlockNumber,
    /// topmost internal page in the subtree
    pub topparent: BlockNumber,
}
pub const SizeOfBtreeMarkPageHalfDead: usize =
    core::mem::offset_of!(xl_btree_mark_page_halfdead, topparent)
        + core::mem::size_of::<BlockNumber>();

/// `xl_btree_unlink_page` -- deletion (unlink) of a btree page.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_unlink_page {
    /// target block's left sibling, if any
    pub leftsib: BlockNumber,
    /// target block's right sibling
    pub rightsib: BlockNumber,
    /// target block's level
    pub level: uint32,
    /// target block's BTPageSetDeleted() XID
    pub safexid: FullTransactionId,
    pub leafleftsib: BlockNumber,
    pub leafrightsib: BlockNumber,
    /// next child down in the subtree
    pub leaftopparent: BlockNumber,
    // xl_btree_metadata FOLLOWS IF XLOG_BTREE_UNLINK_PAGE_META
}
pub const SizeOfBtreeUnlinkPage: usize = core::mem::offset_of!(xl_btree_unlink_page, leaftopparent)
    + core::mem::size_of::<BlockNumber>();

/// `xl_btree_newroot` -- new root log record (zero tuples for an empty root, or
/// two if the result of splitting an old root).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_newroot {
    /// location of new root (redundant with blk 0)
    pub rootblk: BlockNumber,
    /// its tree level
    pub level: uint32,
}
pub const SizeOfBtreeNewroot: usize =
    core::mem::offset_of!(xl_btree_newroot, level) + core::mem::size_of::<uint32>();

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn btpageopaquedata_layout() {
        // BlockNumber(4) + BlockNumber(4) + uint32(4) + uint16(2) + uint16(2)
        assert_eq!(size_of::<BTPageOpaqueData>(), 16);
        assert_eq!(align_of::<BTPageOpaqueData>(), 4);
        assert_eq!(offset_of!(BTPageOpaqueData, btpo_prev), 0);
        assert_eq!(offset_of!(BTPageOpaqueData, btpo_next), 4);
        assert_eq!(offset_of!(BTPageOpaqueData, btpo_level), 8);
        assert_eq!(offset_of!(BTPageOpaqueData, btpo_flags), 12);
        assert_eq!(offset_of!(BTPageOpaqueData, btpo_cycleid), 14);
    }

    #[test]
    fn btmetapagedata_layout() {
        assert_eq!(offset_of!(BTMetaPageData, btm_magic), 0);
        assert_eq!(offset_of!(BTMetaPageData, btm_version), 4);
        assert_eq!(offset_of!(BTMetaPageData, btm_root), 8);
        assert_eq!(offset_of!(BTMetaPageData, btm_level), 12);
        assert_eq!(offset_of!(BTMetaPageData, btm_fastroot), 16);
        assert_eq!(offset_of!(BTMetaPageData, btm_fastlevel), 20);
        assert_eq!(
            offset_of!(BTMetaPageData, btm_last_cleanup_num_delpages),
            24
        );
        // float8 forces 8-byte alignment, so padding precedes this field.
        assert_eq!(
            offset_of!(BTMetaPageData, btm_last_cleanup_num_heap_tuples),
            32
        );
        assert_eq!(offset_of!(BTMetaPageData, btm_allequalimage), 40);
        assert_eq!(align_of::<BTMetaPageData>(), 8);
    }

    #[test]
    fn btdeletedpagedata_layout() {
        assert_eq!(size_of::<BTDeletedPageData>(), 8);
        assert_eq!(align_of::<BTDeletedPageData>(), 8);
        assert_eq!(offset_of!(BTDeletedPageData, safexid), 0);
    }

    #[test]
    fn btreetupledata_layout() {
        // Physically identical to IndexTupleData.
        assert_eq!(
            size_of::<BTreeTupleData>(),
            size_of::<crate::IndexTupleData>()
        );
        assert_eq!(offset_of!(BTreeTupleData, t_tid), 0);
    }

    #[test]
    fn btmaxitemsize_matches_c() {
        // BLCKSZ=8192, SizeOfPageHeaderData=24, ItemIdData=4, opaque=16.
        // (8192 - MAXALIGN(24+12) - MAXALIGN(16)) / 3 = (8192-40-16)/3 = 2712,
        // MAXALIGN_DOWN(2712)=2712, minus MAXALIGN(sizeof ItemPointerData=6)=8.
        assert_eq!(BTMaxItemSizeNoHeapTid, 2712);
        assert_eq!(BTMaxItemSize, 2704);
    }

    #[test]
    fn constants_match_c() {
        assert_eq!(BTREE_MAGIC, 0x053162);
        assert_eq!(BTREE_VERSION, 4);
        assert_eq!(BT_OFFSET_MASK, 0x0FFF);
        assert!(BT_OFFSET_MASK >= crate::INDEX_MAX_KEYS as uint16);
    }

    #[test]
    fn xlog_record_sizeofs_match_c() {
        // offsetof(xl_btree_insert, offnum) + sizeof(OffsetNumber)
        assert_eq!(SizeOfBtreeInsert, 2);
        // offsetof(xl_btree_split, postingoff) + sizeof(uint16)
        assert_eq!(SizeOfBtreeSplit, 10);
        // offsetof(xl_btree_dedup, nintervals) + sizeof(uint16)
        assert_eq!(SizeOfBtreeDedup, 2);
        // offsetof(xl_btree_vacuum, nupdated) + sizeof(uint16)
        assert_eq!(SizeOfBtreeVacuum, 4);
        // TransactionId(4) ndeleted(2) nupdated(2) isCatalogRel(1) -> offset 8 + 1
        assert_eq!(SizeOfBtreeDelete, 9);
        assert_eq!(offset_of!(xl_btree_delete, snapshotConflictHorizon), 0);
        assert_eq!(offset_of!(xl_btree_delete, ndeleted), 4);
        assert_eq!(offset_of!(xl_btree_delete, nupdated), 6);
        assert_eq!(offset_of!(xl_btree_delete, isCatalogRel), 8);
        // offsetof(xl_btree_update, ndeletedtids) + sizeof(uint16)
        assert_eq!(SizeOfBtreeUpdate, 2);
        // poffset(2 -> pad to 4) leafblk(4) leftblk(4) rightblk(4) topparent(4)
        assert_eq!(offset_of!(xl_btree_mark_page_halfdead, poffset), 0);
        assert_eq!(offset_of!(xl_btree_mark_page_halfdead, leafblk), 4);
        assert_eq!(offset_of!(xl_btree_mark_page_halfdead, topparent), 16);
        assert_eq!(SizeOfBtreeMarkPageHalfDead, 20);
        // leftsib(4) rightsib(4) level(4) [pad 4] safexid(8) leafleftsib(4)
        // leafrightsib(4) leaftopparent(4)
        assert_eq!(offset_of!(xl_btree_unlink_page, leftsib), 0);
        assert_eq!(offset_of!(xl_btree_unlink_page, safexid), 16);
        assert_eq!(offset_of!(xl_btree_unlink_page, leaftopparent), 32);
        assert_eq!(SizeOfBtreeUnlinkPage, 36);
        // rootblk(4) level(4)
        assert_eq!(SizeOfBtreeNewroot, 8);
        // xl_btree_reuse_page: locator(12) block(4) snapshotConflictHorizon(8)
        // isCatalogRel(1)
        assert_eq!(offset_of!(xl_btree_reuse_page, block), 12);
        assert_eq!(offset_of!(xl_btree_reuse_page, snapshotConflictHorizon), 16);
    }
}
