//! Runtime (not on-disk) B-tree access-method vocabulary (`access/nbtree.h`),
//! trimmed to what the `nbtree.c` AM entry points consume: the scan-position
//! state ([`BTScanPosData`]/[`BTScanPosItem`]), the per-scan private workspace
//! ([`BTScanOpaqueData`]), the VACUUM page-walk state ([`BTVacState`] /
//! [`BTVacuumPosting`] / [`BTPendingFSM`]), the page-flag constants, and the
//! `IndexUniqueCheck` insert mode (`access/genam.h`).
//!
//! The scan workspaces are `palloc`'d in C; here they are [`mcx::PgVec`] over
//! the scan's memory context, so the structs carry the context lifetime
//! `'mcx`.

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

extern crate alloc;

use mcx::PgVec;
use types_core::primitive::{
    uint16, AttrNumber, BlockNumber, InvalidBlockNumber, OffsetNumber, Size, XLogRecPtr, BLCKSZ,
};
use types_core::xact::FullTransactionId;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_scan::scankey::ScanKeyData;
use types_scan::sdir::ScanDirection;
use types_storage::storage::{Buffer, InvalidBuffer, LocationIndex};
use types_tuple::heaptuple::{IndexTuple, ItemPointerData};

/// There's room for a 16-bit vacuum cycle ID in `BTPageOpaqueData`.
pub type BTCycleId = uint16;

// Bits defined in `btpo_flags` (access/nbtree.h).
/// leaf page, i.e. not internal page
pub const BTP_LEAF: uint16 = 1 << 0;
/// root page (has no parent)
pub const BTP_ROOT: uint16 = 1 << 1;
/// page has been deleted from tree
pub const BTP_DELETED: uint16 = 1 << 2;
/// meta-page
pub const BTP_META: uint16 = 1 << 3;
/// empty, but still in tree
pub const BTP_HALF_DEAD: uint16 = 1 << 4;
/// rightmost page of split group
pub const BTP_SPLIT_END: uint16 = 1 << 5;
/// page has LP_DEAD tuples (deprecated)
pub const BTP_HAS_GARBAGE: uint16 = 1 << 6;
/// right sibling's downlink is missing
pub const BTP_INCOMPLETE_SPLIT: uint16 = 1 << 7;
/// contains BTDeletedPageData
pub const BTP_HAS_FULLXID: uint16 = 1 << 8;

/// The max allowed value of a cycle ID is a bit less than 64K.
pub const MAX_BT_CYCLE_ID: BTCycleId = 0xFF7F;

/// first page is meta
pub const BTREE_METAPAGE: BlockNumber = 0;

/// A special value to indicate "no page number".
pub const P_NONE: BlockNumber = 0;

/// `P_HIKEY` (access/nbtree.h) — the high key line-pointer offset.
pub const P_HIKEY: OffsetNumber = 1;
/// `P_FIRSTKEY` (access/nbtree.h) — the first data key offset on a page that
/// carries a high key.
pub const P_FIRSTKEY: OffsetNumber = 2;

/// `MaxTIDsPerBTreePage`: upper bound on the number of heap TIDs that may be
/// stored on a btree leaf page; used to size per-page killed-item buffers.
///
/// `(BLCKSZ - SizeOfPageHeaderData - sizeof(BTPageOpaqueData)) / sizeof(ItemPointerData)`
///   = `(8192 - 24 - 16) / 6` = 1358.
pub const MaxTIDsPerBTreePage: usize = 1358;

/// `MaxIndexTuplesPerPage` (`access/itup.h`):
/// `(BLCKSZ - SizeOfPageHeaderData) / (MAXALIGN(sizeof(IndexTupleData) + 1) + sizeof(ItemIdData))`
///   = `(8192 - 24) / (MAXALIGN(8 + 1) + 4)` = `8168 / (16 + 4)` = 408.
pub const MaxIndexTuplesPerPage: usize = (BLCKSZ - 24) / (16 + 4);

/// `BTORDER_PROC` (`access/nbtree.h`) — support function number 1, the 3-way
/// ORDER comparison proc.
pub const BTORDER_PROC: i16 = 1;

/// `BTMaxStrategyNumber` (`access/stratnum.h`).
pub const BTMaxStrategyNumber: u16 = 5;
/// `BTNProcs` (`access/nbtree.h`).
pub const BTNProcs: u16 = 6;
/// `BTOPTIONS_PROC` (`access/nbtree.h`).
pub const BTOPTIONS_PROC: u16 = 5;

/// `BTMaxItemSize` (`access/nbtree.h`): the largest tuple `_bt_dedup_pass` may
/// produce.
///
/// `MAXALIGN_DOWN((BLCKSZ - MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData))`
/// ` - MAXALIGN(sizeof(BTPageOpaqueData))) / 3) - MAXALIGN(sizeof(ItemPointerData))`
///   = `MAXALIGN_DOWN((8192 - MAXALIGN(24+12) - MAXALIGN(16)) / 3) - MAXALIGN(6)`
///   = `MAXALIGN_DOWN((8192 - 40 - 16) / 3) - 8` = `2712 - 8` = 2704.
pub const BTMaxItemSize: types_core::Size = 2704;

/// `BTREE_SINGLEVAL_FILLFACTOR` (`access/nbtree.h`) — effective leaf-page
/// fillfactor when a page is full of duplicates of a single value.
pub const BTREE_SINGLEVAL_FILLFACTOR: i32 = 96;

/// `BTREE_DEFAULT_FILLFACTOR` (`access/nbtree.h`) — default leaf-page fillfactor.
pub const BTREE_DEFAULT_FILLFACTOR: i32 = 90;

/// `BTREE_NONLEAF_FILLFACTOR` (`access/nbtree.h`) — fixed fillfactor used when
/// packing internal (non-leaf) pages during an index build.
pub const BTREE_NONLEAF_FILLFACTOR: i32 = 70;

/// `INDEX_ALT_TID_MASK` (`access/itup.h`, `= INDEX_AM_RESERVED_BIT`) — set in a
/// `t_info` to indicate an alternative (overloaded) `t_tid` interpretation.
pub const INDEX_ALT_TID_MASK: uint16 = 0x2000;
/// `BT_OFFSET_MASK` (`access/nbtree.h`) — mask for the number of posting-list
/// items / pivot-tuple attributes stored in the overloaded `t_tid` offset.
pub const BT_OFFSET_MASK: uint16 = 0x0FFF;
/// `BT_PIVOT_HEAP_TID_ATTR` (`access/nbtree.h`) — set when a pivot tuple stores
/// a tiebreaker heap TID attribute.
pub const BT_PIVOT_HEAP_TID_ATTR: uint16 = 0x1000;
/// `BT_IS_POSTING` (`access/nbtree.h`) — set in the overloaded `t_tid` offset
/// to indicate a posting-list tuple.
pub const BT_IS_POSTING: uint16 = 0x2000;

// ===========================================================================
// WAL record info-bit op codes (`access/nbtxlog.h`).
// ===========================================================================

/// add index tuple without split
pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00;
/// same, on a non-leaf page
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x10;
/// same, plus update metapage
pub const XLOG_BTREE_INSERT_META: u8 = 0x20;
/// add index tuple with split
pub const XLOG_BTREE_SPLIT_L: u8 = 0x30;
/// as above, new item on right
pub const XLOG_BTREE_SPLIT_R: u8 = 0x40;
/// add index tuple with posting split
pub const XLOG_BTREE_INSERT_POST: u8 = 0x50;
/// `XLOG_BTREE_DEDUP` (`access/nbtxlog.h`) — deduplicate tuples for a page.
pub const XLOG_BTREE_DEDUP: u8 = 0x60;
/// delete leaf index tuples for a page
pub const XLOG_BTREE_DELETE: u8 = 0x70;
/// delete a half-dead page
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x80;
/// same, and update metapage
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x90;
/// new root page
pub const XLOG_BTREE_NEWROOT: u8 = 0xA0;
/// mark a leaf as half-dead
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0xB0;
/// delete entries on a page during vacuum
pub const XLOG_BTREE_VACUUM: u8 = 0xC0;
/// old page is about to be reused from FSM
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0;
/// update cleanup-related data in the metapage
pub const XLOG_BTREE_META_CLEANUP: u8 = 0xE0;

/// `SizeOfBtreeDedup` (`access/nbtxlog.h`) — `offsetof(xl_btree_dedup,`
/// `nintervals) + sizeof(uint16)` = 2 (the record is a single `uint16`).
pub const SizeOfBtreeDedup: usize = 2;

/// `SizeOfBtreeUpdate` (`access/nbtxlog.h`) — `offsetof(xl_btree_update,`
/// `ndeletedtids) + sizeof(uint16)` = 2 (just the `ndeletedtids` count; the
/// posting-list deleted-TID offsets follow in the block data).
pub const SizeOfBtreeUpdate: usize = 2;

// ===========================================================================
// Metapage layout (`access/nbtree.h`).
// ===========================================================================

/// `BTREE_METADATA_MAGIC` (`access/nbtree.h`) — magic number identifying a
/// btree metapage.
pub const BTREE_MAGIC: u32 = 0x053162;
/// `BTREE_VERSION` (`access/nbtree.h`) — current version number.
pub const BTREE_VERSION: u32 = 4;
/// `BTREE_MIN_VERSION` (`access/nbtree.h`) — minimal supported version number.
pub const BTREE_MIN_VERSION: u32 = 2;
/// `BTREE_NOVAC_VERSION` (`access/nbtree.h`) — minimal version with support for
/// `btpo_level` in `last_cleanup_num_delpages`.
pub const BTREE_NOVAC_VERSION: u32 = 3;

/// `BTMetaPageData` (`access/nbtree.h`) — the contents of a btree metapage,
/// stored at `PageGetContents` of block [`BTREE_METAPAGE`].
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTMetaPageData {
    /// should contain `BTREE_MAGIC`
    pub btm_magic: u32,
    /// should contain `BTREE_VERSION`
    pub btm_version: u32,
    /// current root location
    pub btm_root: BlockNumber,
    /// tree level of the root page
    pub btm_level: u32,
    /// current "fast" root location
    pub btm_fastroot: BlockNumber,
    /// tree level of the "fast" root page
    pub btm_fastlevel: u32,
    /// number of deleted, non-recyclable pages during last cleanup
    pub btm_last_cleanup_num_delpages: u32,
    /// number of heap tuples during last cleanup (deprecated)
    pub btm_last_cleanup_num_heap_tuples: f64,
    /// are all columns "equalimage"?
    pub btm_allequalimage: bool,
}

// ===========================================================================
// WAL record on-disk structs (`access/nbtxlog.h`). The redo path decodes
// these field-by-field out of the (possibly unaligned) WAL byte buffers, so
// they carry no `#[repr(C)]` ABI contract here.
// ===========================================================================

/// `xl_btree_metadata` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_metadata {
    pub version: u32,
    pub root: BlockNumber,
    pub level: u32,
    pub fastroot: BlockNumber,
    pub fastlevel: u32,
    pub last_cleanup_num_delpages: u32,
    pub allequalimage: bool,
}

/// `xl_btree_insert` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_insert {
    pub offnum: OffsetNumber,
}

/// `xl_btree_split` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_split {
    /// tree level of page being split
    pub level: u32,
    /// first origpage item on rightpage
    pub firstrightoff: OffsetNumber,
    /// new item's offset
    pub newitemoff: OffsetNumber,
    /// offset inside orig posting tuple
    pub postingoff: uint16,
}

/// `xl_btree_dedup` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_dedup {
    pub nintervals: uint16,
}

/// `xl_btree_reuse_page` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_reuse_page {
    pub locator: types_storage::RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool,
}

/// `xl_btree_vacuum` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_vacuum {
    pub ndeleted: uint16,
    pub nupdated: uint16,
}

/// `xl_btree_delete` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_delete {
    pub snapshotConflictHorizon: types_core::primitive::TransactionId,
    pub ndeleted: uint16,
    pub nupdated: uint16,
    pub isCatalogRel: bool,
}

/// `xl_btree_mark_page_halfdead` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_mark_page_halfdead {
    /// deleted tuple id in parent page
    pub poffset: OffsetNumber,
    /// leaf block ultimately being deleted
    pub leafblk: BlockNumber,
    /// leaf block's left sibling, if any
    pub leftblk: BlockNumber,
    /// leaf block's right sibling
    pub rightblk: BlockNumber,
    /// topmost internal page in the subtree
    pub topparent: BlockNumber,
}

/// `xl_btree_unlink_page` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_unlink_page {
    /// target block's left sibling, if any
    pub leftsib: BlockNumber,
    /// target block's right sibling
    pub rightsib: BlockNumber,
    /// target block's level
    pub level: u32,
    /// target block's `BTPageSetDeleted()` XID
    pub safexid: FullTransactionId,
    /// last child of the to-be-deleted subtree's leftmost leaf-level sibling
    pub leafleftsib: BlockNumber,
    /// next child of the to-be-deleted subtree
    pub leafrightsib: BlockNumber,
    /// next remaining child in to-be-deleted subtree
    pub leaftopparent: BlockNumber,
}

/// `xl_btree_newroot` (`access/nbtxlog.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_newroot {
    pub rootblk: BlockNumber,
    pub level: u32,
}

/// `BTPageOpaqueData` (`access/nbtree.h`) — the btree-specific special-area
/// header at the end of every btree page (16 bytes, `#[repr(C)]`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTPageOpaqueData {
    /// left sibling, or `P_NONE` if leftmost
    pub btpo_prev: BlockNumber,
    /// right sibling, or `P_NONE` if rightmost
    pub btpo_next: BlockNumber,
    /// tree level --- zero for leaf pages
    pub btpo_level: u32,
    /// flag bits, see `BTP_*`
    pub btpo_flags: uint16,
    /// vacuum cycle ID of latest split
    pub btpo_cycleid: BTCycleId,
}

/// `BTDedupInterval` (`access/nbtree.h`) — describes a deduplicated interval
/// (a posting list that replaced one or more plain tuples) for WAL.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTDedupInterval {
    pub baseoff: OffsetNumber,
    pub nitems: uint16,
}

// `TM_IndexDelete` / `TM_IndexStatus` / `TM_IndexDeleteOp` (`access/tableam.h`)
// live in `types-tableam` (their C home); re-exported here for the index-AM
// callers that historically reached them through `types_nbtree`.
pub use types_tableam::{TmIndexDelete, TmIndexDeleteOp, TmIndexStatus};

/// `IndexUniqueCheck` (`access/genam.h`) — the uniqueness-check mode requested
/// by the executor for an index insert.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexUniqueCheck {
    /// `UNIQUE_CHECK_NO` — no uniqueness checking.
    No,
    /// `UNIQUE_CHECK_YES` — deferrable unique constraint, check now.
    Yes,
    /// `UNIQUE_CHECK_PARTIAL` — check partially (for deferred constraints).
    Partial,
    /// `UNIQUE_CHECK_EXISTING` — re-check existing tuple (no actual insert).
    Existing,
}

/// `BTPS_State` (`nbtree.c`) — parallel-scan page status. Drives the
/// `_bt_parallel_seize` state machine over the shared [`BTParallelScanDescData`].
///
/// - `BTPARALLEL_NOT_INITIALIZED`: the scan has not started.
/// - `BTPARALLEL_NEED_PRIMSCAN`: some process must seize the scan to advance it
///   via another call to `_bt_first`.
/// - `BTPARALLEL_ADVANCING`: some process is advancing the scan to a new page;
///   others must wait.
/// - `BTPARALLEL_IDLE`: no backend is advancing the scan; some process can start.
/// - `BTPARALLEL_DONE`: the scan is complete (including error exit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
#[allow(non_camel_case_types)]
pub enum BTPS_State {
    BTPARALLEL_NOT_INITIALIZED = 0,
    BTPARALLEL_NEED_PRIMSCAN = 1,
    BTPARALLEL_ADVANCING = 2,
    BTPARALLEL_IDLE = 3,
    BTPARALLEL_DONE = 4,
}

/// `BTParallelScanDescData` (`nbtree.c`) — btree-specific shared information
/// required for a parallel scan. Lives IN the DSM chunk the parallel
/// index-scan infrastructure (`indexam.c` `ParallelIndexScanDesc` +
/// `OffsetToPointer(parallel_scan, ps_offset_am)`) sets up; the nbtree
/// `_bt_parallel_*` state machine in `backend-access-nbtree-nbtree` operates on
/// it directly through a `*mut` resolved by `bt_resolve_parallel_scan`. This is
/// the C design verbatim: the descriptor is `#[repr(C)]` and placed in-chunk,
/// and the flexible-array tail (`btps_arrElems[FLEXIBLE_ARRAY_MEMBER]` plus the
/// trailing flattened skip-array datum region) lives immediately after the
/// header in the SAME chunk — modelled here by the inline zero-length FAM
/// marker `btps_arrElems`, whose address (`&self.btps_arrElems`) IS the C
/// `&btps_arrElems[0]`. The serialize/restore code indexes it exactly as C
/// indexes the FAM.
///
/// Every field C mutates concurrently across leader and workers is mutated
/// while holding `btps_lock` exclusively (the `LWLock`), so the shared
/// `&self`-via-`*mut` access the resolver hands out is sound for exactly the
/// reason C's plain-field-under-lock access is — the lock supplies the
/// exclusion and the ordering. This is why it is a sound [`SharedDsmObject`].
#[repr(C)]
#[derive(Debug)]
pub struct BTParallelScanDescData {
    /// next page to be scanned
    pub btps_nextScanPage: BlockNumber,
    /// page whose sibling link was copied into `btps_nextScanPage`
    pub btps_lastCurrPage: BlockNumber,
    /// indicates whether next page is available for scan
    pub btps_pageStatus: BTPS_State,
    /// protects shared parallel state
    pub btps_lock: types_storage::storage::LWLock,
    /// used to synchronize parallel scan
    pub btps_cv: types_condvar::ConditionVariable,
    /// `int btps_arrElems[FLEXIBLE_ARRAY_MEMBER]` — the inline zero-length FAM
    /// marker. Its address is the base of the in-chunk `btps_arrElems[]` `int`
    /// cur_elem array followed by the flattened skip-array datum region; the
    /// chunk is sized for the tail by `btestimateparallelscan`. Accessed via
    /// [`btps_arr_elem`]/[`set_btps_arr_elem`]/[`btps_datumshared`].
    pub btps_arrElems: [i32; 0],
}

// SAFETY: `#[repr(C)]` matching the C `BTParallelScanDescData` field-for-field
// with an inline `btps_arrElems[FLEXIBLE_ARRAY_MEMBER]`. Every field C mutates
// concurrently after the launch barrier is mutated under `btps_lock` held
// exclusively — the in-segment `LWLock` supplies the cross-process exclusion and
// ordering (exactly as C's plain `btps_pageStatus`/`btps_nextScanPage` fields
// are written under the lock); `btps_cv` is the in-segment CV. The leader's
// `btinitparallelscan` placement initializer writes every field before any
// worker attaches. A `*mut Self` used only under the lock is therefore sound to
// alias across processes.
unsafe impl types_parallel::SharedDsmObject for BTParallelScanDescData {}

/// `offsetof(BTParallelScanDescData, btps_arrElems)` — the size of the fixed
/// header preceding the flexible-array tail in the C DSM struct, used by
/// `btestimateparallelscan` as the base shared-state size and by the in-place
/// accessors to locate the FAM. With `#[repr(C)]` and the inline FAM marker this
/// is exactly `offset_of!`, mirroring the C `offsetof`.
pub const BTPARALLEL_HEADER_SIZE: usize =
    core::mem::offset_of!(BTParallelScanDescData, btps_arrElems);

impl BTParallelScanDescData {
    /// `&btps_arrElems[0]` — the in-chunk base of the flexible-array tail
    /// (`(char *) self + offsetof(.., btps_arrElems)`).
    ///
    /// # Safety
    /// `self` must be the in-chunk descriptor whose tail was sized by
    /// `btestimateparallelscan`.
    #[inline]
    unsafe fn btps_arrtail(&self) -> *mut u8 {
        // The address of the inline zero-length FAM marker IS the C
        // `&btps_arrElems[0]`. Cast through the struct base for a stable
        // provenance over the whole chunk (the marker is zero-sized).
        (self as *const Self as *mut u8).add(BTPARALLEL_HEADER_SIZE)
    }

    /// Read `btps_arrElems[i]` (the i-th `int` cur_elem slot at the head of the
    /// flexible-array tail).
    ///
    /// # Safety
    /// `i` must be within the `btps_arrElems[]` region sized by
    /// `btestimateparallelscan`, and `self` must be the in-chunk descriptor.
    pub unsafe fn btps_arr_elem(&self, i: usize) -> i32 {
        let p = self.btps_arrtail() as *const i32;
        core::ptr::read_unaligned(p.add(i))
    }

    /// Write `btps_arrElems[i] = v`.
    ///
    /// # Safety
    /// See [`btps_arr_elem`].
    pub unsafe fn set_btps_arr_elem(&mut self, i: usize, v: i32) {
        let p = self.btps_arrtail() as *mut i32;
        core::ptr::write_unaligned(p.add(i), v);
    }

    /// Pointer to the serialized-datum region: `(char *) &btps_arrElems[n]`,
    /// where `n == so->numArrayKeys`.
    ///
    /// # Safety
    /// `n` must equal the scan's `numArrayKeys`; the returned pointer aliases
    /// the in-chunk tail.
    pub unsafe fn btps_datumshared(&self, n: usize) -> *mut u8 {
        (self.btps_arrtail() as *mut i32).add(n) as *mut u8
    }
}

/// `BTSkipSupport` opclass sentinels for a skip array (`access/nbtree.h`).
///
/// Mirrors C's `SkipSupportData`: the `low_elem` / `high_elem` boundary values
/// plus the `increment` / `decrement` callbacks (held as
/// [`SkipSupportIncDecId`] tokens the skip-support substrate interprets, the
/// owned-model stand-in for the C `SkipSupportIncDec` function pointers).
#[derive(Clone, Debug)]
pub struct BTSkipSupport<'mcx> {
    /// lowest sorting non-NULL value
    pub low_elem: Datum<'mcx>,
    /// highest sorting non-NULL value
    pub high_elem: Datum<'mcx>,
    /// `SkipSupportIncDec increment` — increment-to-next-distinct callback token.
    pub increment: Option<types_sortsupport::SkipSupportIncDecId>,
    /// `SkipSupportIncDec decrement` — decrement-to-prev-distinct callback token.
    pub decrement: Option<types_sortsupport::SkipSupportIncDecId>,
    /// 1-based attribute number the skip support is for
    pub attno: AttrNumber,
}

/// `BTArrayKeyInfo` — one per equality-type `SK_SEARCHARRAY` scan key
/// (`access/nbtree.h`). Covers both SAOP arrays and skip arrays.
#[derive(Clone, Debug)]
pub struct BTArrayKeyInfo<'mcx> {
    /// index of associated key in keyData
    pub scan_key: i32,
    /// number of elems (-1 means skip array)
    pub num_elems: i32,
    /// array of num_elems Datums (skip arrays leave this empty)
    pub elem_values: PgVec<'mcx, Datum<'mcx>>,
    /// index of current element in elem_values
    pub cur_elem: i32,
    /// attr's length, in bytes
    pub attlen: i16,
    /// attr's `FormData_pg_attribute.attbyval`
    pub attbyval: bool,
    /// NULL is lowest/highest element?
    pub null_elem: bool,
    /// skip support fmgr handle (`None` if opclass lacks it)
    pub sksup: Option<u64>,
    /// skip-support sentinels (only meaningful when `sksup.is_some()`)
    pub sksup_data: Option<BTSkipSupport<'mcx>>,
    /// `ScanKey low_compare` — `> or >=` key (skip arrays only), or `None`.
    /// In C this is a pointer into the preprocessed input scan keys; the owned
    /// model carries an owned copy of the boxed scan key.
    pub low_compare: Option<alloc::boxed::Box<ScanKeyData<'mcx>>>,
    /// `ScanKey high_compare` — `< or <=` key (skip arrays only), or `None`.
    pub high_compare: Option<alloc::boxed::Box<ScanKeyData<'mcx>>>,
}

impl<'mcx> BTArrayKeyInfo<'mcx> {
    /// A fresh, empty array-key info over `mcx` (all fields zeroed as in a
    /// `palloc0`'d `BTArrayKeyInfo`, `cur_elem == -1` for "invalid").
    pub fn new_in(mcx: mcx::Mcx<'mcx>) -> Self {
        BTArrayKeyInfo {
            scan_key: 0,
            num_elems: 0,
            elem_values: PgVec::new_in(mcx),
            cur_elem: -1,
            attlen: 0,
            attbyval: false,
            null_elem: false,
            sksup: None,
            sksup_data: None,
            low_compare: None,
            high_compare: None,
        }
    }
}

/// `BTScanPosItem` — what we remember about each match (`access/nbtree.h`).
#[derive(Clone, Copy, Debug, Default)]
pub struct BTScanPosItem {
    /// TID of referenced heap item
    pub heapTid: ItemPointerData,
    /// index item's location within page
    pub indexOffset: OffsetNumber,
    /// IndexTuple's offset in workspace, if any
    pub tupleOffset: LocationIndex,
}

/// `BTScanPosData` — per-position scan state (`access/nbtree.h`).
///
/// The fixed `items[MaxTIDsPerBTreePage]` flexible array is modelled as a
/// `PgVec`; the scan code only indexes `items[itemIndex]` within
/// `[firstItem, lastItem]`.
#[derive(Clone, Debug)]
pub struct BTScanPosData<'mcx> {
    /// currPage buf (`InvalidBuffer == 0` means unpinned)
    pub buf: Buffer,
    /// page referenced by items array
    pub currPage: BlockNumber,
    /// currPage's left link
    pub prevPage: BlockNumber,
    /// currPage's right link
    pub nextPage: BlockNumber,
    /// currPage's LSN (when so->dropPin)
    pub lsn: XLogRecPtr,
    /// scan direction for the saved position's call to `_bt_readpage`
    pub dir: ScanDirection,
    /// first free location in the tuple storage workspace (index-only scans)
    pub nextTupleOffset: i32,
    /// may be matching entries to the left of the current page?
    pub moreLeft: bool,
    /// may be matching entries to the right of the current page?
    pub moreRight: bool,
    /// first valid index in items[]
    pub firstItem: i32,
    /// last valid index in items[]
    pub lastItem: i32,
    /// current index in items[]
    pub itemIndex: i32,
    /// the matches (ordered in index order)
    pub items: PgVec<'mcx, BTScanPosItem>,
}

impl<'mcx> BTScanPosData<'mcx> {
    /// A fresh, invalidated position over `mcx`.
    pub fn new(mcx: mcx::Mcx<'mcx>) -> Self {
        BTScanPosData {
            buf: InvalidBuffer,
            currPage: InvalidBlockNumber,
            prevPage: 0,
            nextPage: 0,
            lsn: 0,
            dir: ScanDirection::ForwardScanDirection,
            nextTupleOffset: 0,
            moreLeft: false,
            moreRight: false,
            firstItem: 0,
            lastItem: 0,
            itemIndex: 0,
            items: PgVec::new_in(mcx),
        }
    }
}

/// `BTScanPosIsPinned(scanpos)` (`access/nbtree.h`).
#[inline]
pub fn BTScanPosIsPinned(scanpos: &BTScanPosData) -> bool {
    scanpos.buf != InvalidBuffer
}

/// `BTScanPosIsValid(scanpos)` (`access/nbtree.h`).
#[inline]
pub fn BTScanPosIsValid(scanpos: &BTScanPosData) -> bool {
    scanpos.currPage != InvalidBlockNumber
}

/// `BTScanPosInvalidate(scanpos)` (`access/nbtree.h`).
#[inline]
pub fn BTScanPosInvalidate(scanpos: &mut BTScanPosData) {
    scanpos.buf = InvalidBuffer;
    scanpos.currPage = InvalidBlockNumber;
    scanpos.prevPage = InvalidBlockNumber;
    scanpos.nextPage = InvalidBlockNumber;
}

/// `BTScanOpaqueData` — the btree-private state for an index scan
/// (`access/nbtree.h`). Trimmed to the fields the `nbtree.c` AM entry points
/// touch.
#[derive(Clone, Debug)]
pub struct BTScanOpaqueData<'mcx> {
    /// false if qual can never be satisfied
    pub qual_ok: bool,
    /// number of preprocessed scan keys
    pub numberOfKeys: i32,
    /// array of preprocessed scan keys
    pub keyData: PgVec<'mcx, ScanKeyData<'mcx>>,

    /// number of equality-type array keys
    pub numArrayKeys: i32,
    /// at least one skip array in arrayKeys[]?
    pub skipScan: bool,
    /// new prim scan to continue in current dir?
    pub needPrimScan: bool,
    /// check scan not still behind on next page?
    pub scanBehind: bool,
    /// scanBehind opposite-scan-dir check?
    pub oppositeDirCheck: bool,
    /// info about each equality-type array key
    pub arrayKeys: PgVec<'mcx, BTArrayKeyInfo<'mcx>>,
    /// ORDER proc fmgr handles for required equality keys (parallel to keyData)
    pub orderProcs: PgVec<'mcx, u64>,

    /// currPos.items indexes of killed items
    pub killedItems: PgVec<'mcx, i32>,
    /// number of currently stored items
    pub numKilled: i32,
    /// drop leaf pin before btgettuple returns?
    pub dropPin: bool,
    /// `scan->ignore_killed_tuples` — skip LP_DEAD-marked items during the scan.
    /// Mirrored onto the opaque state because the `bt_first`/`bt_next` seams
    /// carry only `(rel, &mut so, dir)`; the AM driver sets this from the scan
    /// descriptor before each call (it never changes mid-scan).
    pub ignore_killed_tuples: bool,

    /// `scan->parallel_scan` — the in-DSM `ParallelIndexScanDesc` base address
    /// (C's bare pointer) when this is a parallel scan, else `None`. Mirrored
    /// onto the opaque state because the `bt_first`/`bt_next`/`_bt_steppage`/
    /// `_bt_readnextpage` search functions carry only `(rel, &mut so, dir)`, not
    /// the `IndexScanDesc`; the AM driver sets it from the scan descriptor (via
    /// `sync_in`) before each call. The nbtree-core search loop drives the
    /// `_bt_parallel_seize`/`release`/`done`/`primscan_schedule` seams with it
    /// (mirroring nbtsearch.c's `scan->parallel_scan != NULL` branches).
    pub parallel_scan: Option<u64>,

    /// `scan->xs_snapshot` — the scan's MVCC snapshot, carried for SSI
    /// predicate locking (`PredicateLockRelation`/`PredicateLockPage`). C's
    /// `_bt_first`/`_bt_readnextpage`/`_bt_endpoint` reach `scan->xs_snapshot`
    /// directly; the `bt_first`/`bt_next` seams here carry only
    /// `(rel, &mut so, dir)`, so the AM driver mirrors the snapshot onto the
    /// opaque (via `sync_in`) before each call, the same pattern as
    /// `ignore_killed_tuples`/`parallel_scan`. `None` outside a snapshot-bearing
    /// scan; predicate locking is a no-op then.
    pub predicate_snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData>>,

    /// itemIndex, or -1 if not valid
    pub markItemIndex: i32,

    /// Accumulates `_bt_first` primitive-scan starts for EXPLAIN ANALYZE's
    /// `Index Searches` counter. C increments `scan->instrument->nsearches`
    /// inside `_bt_first` (nbtsearch.c:959), but the `bt_first` seam carries
    /// only `(rel, &mut so, dir)` — no `IndexScanDesc` — so the count is parked
    /// here and flushed into `scan->instrument.nsearches` by the AM adapter
    /// (`btgettuple_am`/`btgetbitmap_am`), the same mirroring pattern as
    /// `ignore_killed_tuples`.
    pub nsearches: u64,

    /// tuple storage for currPos (index-only scans), or `None`
    pub currTuples: Option<PgVec<'mcx, u8>>,
    /// tuple storage for markPos (index-only scans), or `None`
    pub markTuples: Option<PgVec<'mcx, u8>>,

    /// current position data
    pub currPos: BTScanPosData<'mcx>,
    /// marked position, if any
    pub markPos: BTScanPosData<'mcx>,
}

impl<'mcx> BTScanOpaqueData<'mcx> {
    /// A fresh scan workspace over `mcx`.
    pub fn new(mcx: mcx::Mcx<'mcx>) -> Self {
        BTScanOpaqueData {
            qual_ok: false,
            numberOfKeys: 0,
            keyData: PgVec::new_in(mcx),
            numArrayKeys: 0,
            skipScan: false,
            needPrimScan: false,
            scanBehind: false,
            oppositeDirCheck: false,
            arrayKeys: PgVec::new_in(mcx),
            orderProcs: PgVec::new_in(mcx),
            killedItems: PgVec::new_in(mcx),
            numKilled: 0,
            dropPin: false,
            ignore_killed_tuples: false,
            parallel_scan: None,
            predicate_snapshot: None,
            markItemIndex: -1,
            nsearches: 0,
            currTuples: None,
            markTuples: None,
            currPos: BTScanPosData::new(mcx),
            markPos: BTScanPosData::new(mcx),
        }
    }
}

/// `BTReadPageState` (`access/nbtree.h`) — `_bt_readpage` state used across
/// `_bt_checkkeys` calls for a single leaf page. `page` is the page being read,
/// modelled as an owned byte buffer over `'mcx` (C carries a raw `Page`);
/// `finaltup` is the page's high key (forward) or first non-pivot tuple
/// (backward), needed by scans with array keys (`None` for the rightmost /
/// leftmost page). The per-tuple `offnum` and the output `skip`/`continuescan`
/// plus the private look-ahead/primscan-scheduling counters all mirror the C
/// struct field-for-field.
#[derive(Clone, Debug)]
pub struct BTReadPageState<'mcx> {
    /// Lowest non-pivot tuple's offset.
    pub minoff: OffsetNumber,
    /// Highest non-pivot tuple's offset.
    pub maxoff: OffsetNumber,
    /// Needed by scans with array keys (page high key / first non-pivot tuple),
    /// or `None` on the rightmost/leftmost page. Owned page-item bytes.
    pub finaltup: Option<PgVec<'mcx, u8>>,
    /// Page being read (owned bytes over the scan context).
    pub page: PgVec<'mcx, u8>,
    /// page is first for primitive scan?
    pub firstpage: bool,
    /// treat all keys as nonrequired?
    pub forcenonrequired: bool,
    /// start comparisons from this scan key.
    pub startikey: i32,

    /// current tuple's page offset number.
    pub offnum: OffsetNumber,

    /// Array keys "look ahead" skip offnum.
    pub skip: OffsetNumber,
    /// Terminate ongoing (primitive) index scan?
    pub continuescan: bool,

    /// Private `_bt_checkkeys` "look ahead" / primscan-scheduling state.
    pub rechecks: i16,
    pub targetdistance: i16,
    pub nskipadvances: i16,
}

impl<'mcx> BTReadPageState<'mcx> {
    /// A fresh read-page state over `mcx` (with an empty owned page buffer).
    pub fn new(mcx: mcx::Mcx<'mcx>) -> Self {
        BTReadPageState {
            minoff: 0,
            maxoff: 0,
            finaltup: None,
            page: PgVec::new_in(mcx),
            firstpage: false,
            forcenonrequired: false,
            startikey: 0,
            offnum: 0,
            skip: 0,
            continuescan: false,
            rechecks: 0,
            targetdistance: 0,
            nskipadvances: 0,
        }
    }
}

/// `BTPendingFSM` (`access/nbtree.h`) — one entry per page deleted by the
/// current VACUUM, remembered until `_bt_pendingfsm_finalize()` can hand it to
/// the FSM.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTPendingFSM {
    /// Page deleted by current VACUUM.
    pub target: BlockNumber,
    /// Page's `BTDeletedPageData.safexid`.
    pub safexid: FullTransactionId,
}

/// `BTVacuumPostingData` (`access/nbtree.h`) — describes how to VACUUM a
/// posting list tuple when some (not all) of its TIDs are to be deleted.
#[derive(Clone, Debug)]
pub struct BTVacuumPosting<'mcx> {
    /// Tuple that will be/was updated (owned bytes)
    pub itup: PgVec<'mcx, u8>,
    /// Page offset for tuple
    pub updatedoffset: OffsetNumber,
    /// indices into the original posting list that are to be deleted
    pub deletetids: PgVec<'mcx, u16>,
}

/// `BTVacState` (`access/nbtree.h`) — per-`btvacuumscan()` working state.
/// Trimmed to the bulk-delete stats and `_bt_pendingfsm_finalize()` state the
/// `nbtree.c` page-walk touches; `info`/`callback`/`pagedelcontext` are owned
/// by the vacuum driver.
#[derive(Clone, Debug)]
pub struct BTVacState<'mcx> {
    /// the current vacuum cycle ID
    pub cycleid: BTCycleId,
    /// the bulk-delete statistics this scan is accumulating
    pub stats: IndexBulkDeleteResult,

    /// pendingpages space (in # elements)
    pub bufsize: i32,
    /// max bufsize that respects work_mem
    pub maxbufsize: i32,
    /// one entry per newly deleted page
    pub pendingpages: PgVec<'mcx, BTPendingFSM>,
    /// current # valid pendingpages
    pub npendingpages: i32,
}

impl<'mcx> BTVacState<'mcx> {
    pub fn new(mcx: mcx::Mcx<'mcx>, cycleid: BTCycleId) -> Self {
        BTVacState {
            cycleid,
            stats: IndexBulkDeleteResult::default(),
            bufsize: 0,
            maxbufsize: 0,
            pendingpages: PgVec::new_in(mcx),
            npendingpages: 0,
        }
    }
}

/// `IndexBulkDeleteResult` (`access/genam.h`) — VACUUM statistics for an
/// index, accumulated across `btbulkdelete`/`btvacuumcleanup`. Canonically
/// defined in `types_tableam::genam` (the `access/genam.h` home); re-exported
/// here so existing `types_nbtree::IndexBulkDeleteResult` paths keep working.
pub use types_tableam::genam::IndexBulkDeleteResult;

// ===========================================================================
// Insertion / search scankey descriptor model (access/nbtree.h)
// ---------------------------------------------------------------------------
// These are the btree-private "insertion scankey" and tree-descent stack
// structures used by `_bt_search` / `_bt_compare` / `_bt_binsrch_insert`.
// They are runtime (not on-disk) state, `palloc`'d in C. amcheck
// (verify_nbtree.c) is a heavy consumer: every cross-page invariant check
// builds a `BTScanInsert` via `_bt_mkscankey` and descends with `_bt_search`.
// ===========================================================================

/// `BTStackData` (`access/nbtree.h`) — as we descend a tree, we push the
/// location of pivot tuples whose downlink we are about to follow onto a
/// private stack. Used to walk back up after a leaf split, and by
/// `_bt_rootdescend` in amcheck. The C `bts_parent` is a `BTStackData *`
/// chain; here it is an owned boxed link.
#[derive(Clone, Debug)]
pub struct BTStackData {
    /// `bts_blkno` — block number of the page the downlink was found on.
    pub bts_blkno: BlockNumber,
    /// `bts_offset` — offset of the pivot tuple whose downlink we followed.
    pub bts_offset: OffsetNumber,
    /// `bts_parent` — the next stack frame up the tree (the page one level
    /// above), or `None` at the root.
    pub bts_parent: Option<Box<BTStackData>>,
}

/// `BTStackData *` (`access/nbtree.h`).
pub type BTStack = Option<Box<BTStackData>>;

/// `BTScanInsertData` (`access/nbtree.h`) — the btree-private state needed to
/// find an initial position for an indexscan, or to insert new tuples: an
/// "insertion scankey" (not to be confused with a search scankey). Used to
/// descend a B-tree using `_bt_search`.
///
/// The C struct sizes `scankeys[INDEX_MAX_KEYS]` as a flexible array member
/// (sized to `keysz` at alloc time); here it is a heap [`Vec`], so `keysz`
/// equals `scankeys.len()`.
#[derive(Clone, Debug)]
pub struct BTScanInsertData<'mcx> {
    /// `heapkeyspace` — do we expect all keys in the index to be physically
    /// unique because heap TID is used as a tiebreaker (index version >= 4)?
    pub heapkeyspace: bool,
    /// `allequalimage` — is deduplication safe for the index?
    pub allequalimage: bool,
    /// `anynullkeys` — did any key have a NULL value when the scankey was
    /// built from an index tuple?
    pub anynullkeys: bool,
    /// `nextkey` — see comments in `_bt_first` for nextkey/backward.
    pub nextkey: bool,
    /// `backward` — backward index scan?
    pub backward: bool,
    /// `scantid` — the heap TID used as a final tiebreaker attribute, or
    /// `None` when the scan doesn't need to find a position for a specific
    /// physical tuple.
    pub scantid: Option<ItemPointerData>,
    /// `keysz` — size of the `scankeys` array (== `scankeys.len()`).
    pub keysz: i32,
    /// `scankeys[]` — scan key entries for attributes compared before
    /// `scantid` (user-visible attributes). Flexible array member in C.
    pub scankeys: Vec<ScanKeyData<'mcx>>,
}

/// `BTScanInsertData *` (`access/nbtree.h`).
pub type BTScanInsert<'mcx> = Option<Box<BTScanInsertData<'mcx>>>;

/// `BTInsertStateData` (`access/nbtree.h`) — a working area used during
/// insertion, filled in after descending the tree to the first leaf page the
/// new tuple might belong on. Tracks the current position while performing the
/// uniqueness check. Also used by `_bt_binsrch_insert`.
#[derive(Clone, Debug)]
pub struct BTInsertStateData<'mcx> {
    /// `itup` — the item we're inserting.
    pub itup: IndexTuple<'mcx>,
    /// `itemsz` — size of `itup`, should be `MAXALIGN()`'d.
    pub itemsz: Size,
    /// `itup_key` — insertion scankey.
    pub itup_key: BTScanInsert<'mcx>,
    /// `buf` — buffer containing the leaf page we're likely to insert on.
    pub buf: Buffer,
    /// `bounds_valid` — is the cached `low`/`stricthigh` bound within `buf`
    /// still valid?
    pub bounds_valid: bool,
    /// `low` — cached lower bound offset within `buf`.
    pub low: OffsetNumber,
    /// `stricthigh` — cached strict upper bound offset within `buf`.
    pub stricthigh: OffsetNumber,
    /// `postingoff` — if `_bt_binsrch_insert` found the location inside an
    /// existing posting list, the position inside that list. `-1` indicates
    /// overlap with an existing LP_DEAD posting-list tuple.
    pub postingoff: i32,
}

/// `BTInsertStateData *` (`access/nbtree.h`).
pub type BTInsertState<'mcx> = Option<Box<BTInsertStateData<'mcx>>>;
