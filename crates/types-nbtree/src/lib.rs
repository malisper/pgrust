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

use mcx::PgVec;
use types_core::primitive::{
    uint16, AttrNumber, BlockNumber, InvalidBlockNumber, OffsetNumber, XLogRecPtr, BLCKSZ,
};
use types_core::xact::FullTransactionId;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_scan::scankey::ScanKeyData;
use types_scan::sdir::ScanDirection;
use types_storage::storage::{Buffer, InvalidBuffer, LocationIndex};
use types_tuple::heaptuple::ItemPointerData;

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

/// `XLOG_BTREE_DEDUP` (`access/nbtxlog.h`) — WAL info bits for a dedup record.
pub const XLOG_BTREE_DEDUP: u8 = 0x60;

/// `SizeOfBtreeDedup` (`access/nbtxlog.h`) — `offsetof(xl_btree_dedup,`
/// `nintervals) + sizeof(uint16)` = 2 (the record is a single `uint16`).
pub const SizeOfBtreeDedup: usize = 2;

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

/// `TM_IndexDelete` (`access/tableam.h`) — one TID handed to the tableam by an
/// index AM during (bottom-up) index deletion.
#[derive(Clone, Copy, Debug)]
pub struct TmIndexDelete {
    /// table TID from index tuple
    pub tid: ItemPointerData,
    /// offset into the `TM_IndexStatus` array
    pub id: i16,
}

/// `TM_IndexStatus` (`access/tableam.h`) — mutable per-TID status that the
/// index AM initializes and the tableam updates.
#[derive(Clone, Copy, Debug)]
pub struct TmIndexStatus {
    /// index AM page offset number
    pub idxoffnum: OffsetNumber,
    /// currently known to be deletable?
    pub knowndeletable: bool,
    /// promising (duplicate) index tuple? (bottom-up only)
    pub promising: bool,
    /// space freed in index if deleted (bottom-up only)
    pub freespace: i16,
}

/// `TM_IndexDeleteOp` (`access/tableam.h`) — describes a (bottom-up) index
/// deletion operation. `irel` is carried by the caller's `Relation` argument
/// across the `_bt_delitems_delete_check` seam, so it is not duplicated here.
#[derive(Clone, Debug)]
pub struct TmIndexDeleteOp<'mcx> {
    /// index block number (for error reports)
    pub iblknum: BlockNumber,
    /// bottom-up (not simple) deletion?
    pub bottomup: bool,
    /// bottom-up space target
    pub bottomupfreespace: i32,
    /// the `deltids` array (its length is C's `ndeltids`)
    pub deltids: PgVec<'mcx, TmIndexDelete>,
    /// the per-TID `status` array (parallel to `deltids`)
    pub status: PgVec<'mcx, TmIndexStatus>,
}

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
/// required for a parallel scan. Lives in the DSM region the parallel
/// index-scan infrastructure (`indexam.c` `ParallelIndexScanDesc` +
/// `OffsetToPointer(parallel_scan, ps_offset_am)`) sets up; the nbtree
/// `_bt_parallel_*` state machine in `backend-access-nbtree-nbtree` operates on
/// it. The flexible-array tail (`btps_arrElems[]` plus a flattened skip-array
/// datum region) is modelled as a separate byte buffer `btps_arrtail` so the
/// fixed header fields stay addressable; the serialize/restore code indexes it
/// exactly as C indexes the FAM.
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
    /// `btps_arrElems[FLEXIBLE_ARRAY_MEMBER]` plus the trailing flattened
    /// skip-array datum region, as a raw byte buffer in the DSM area. Indexed
    /// by [`btps_arr_elem`]/[`set_btps_arr_elem`] for the `int` cur_elem slots
    /// and by raw offset for the serialized datums.
    pub btps_arrtail: *mut u8,
}

/// `offsetof(BTParallelScanDescData, btps_arrElems)` — the size of the fixed
/// header preceding the flexible-array tail in the C DSM struct, used by
/// `btestimateparallelscan` as the base shared-state size. Two `BlockNumber`
/// (4+4), the `BTPS_State` enum (4), then the `LWLock` and `ConditionVariable`
/// (both `MAXALIGN`-padded), with the FAM `int[]` 4-byte aligned. Verified
/// against the C struct layout: `4+4+4 + sizeof(LWLock) + sizeof(ConditionVariable)`
/// rounded to the FAM alignment. (Consumed only by the DSM allocator, which is
/// indexam-owned; the value mirrors the C `offsetof`.)
pub const BTPARALLEL_HEADER_SIZE: usize = {
    // BlockNumber x2 + BTPS_State(i32) = 12, then LWLock + ConditionVariable.
    let base = 4 + 4 + 4;
    let sync = core::mem::size_of::<types_storage::storage::LWLock>()
        + core::mem::size_of::<types_condvar::ConditionVariable>();
    base + sync
};

impl BTParallelScanDescData {
    /// Read `btps_arrElems[i]` (the i-th `int` cur_elem slot at the head of the
    /// flexible-array tail).
    ///
    /// # Safety
    /// `i` must be within the `btps_arrElems[]` region sized by
    /// `btestimateparallelscan`, and `btps_arrtail` must point at the DSM tail.
    pub unsafe fn btps_arr_elem(&self, i: usize) -> i32 {
        let p = self.btps_arrtail as *const i32;
        core::ptr::read_unaligned(p.add(i))
    }

    /// Write `btps_arrElems[i] = v`.
    ///
    /// # Safety
    /// See [`btps_arr_elem`].
    pub unsafe fn set_btps_arr_elem(&mut self, i: usize, v: i32) {
        let p = self.btps_arrtail as *mut i32;
        core::ptr::write_unaligned(p.add(i), v);
    }

    /// Pointer to the serialized-datum region: `(char *) &btps_arrElems[n]`,
    /// where `n == so->numArrayKeys`.
    ///
    /// # Safety
    /// `n` must equal the scan's `numArrayKeys`; the returned pointer aliases
    /// the DSM tail.
    pub unsafe fn btps_datumshared(&self, n: usize) -> *mut u8 {
        (self.btps_arrtail as *mut i32).add(n) as *mut u8
    }
}

/// `BTSkipSupport` opclass sentinels for a skip array (`access/nbtree.h`).
#[derive(Clone, Debug)]
pub struct BTSkipSupport<'mcx> {
    /// lowest sorting non-NULL value
    pub low_elem: Datum<'mcx>,
    /// highest sorting non-NULL value
    pub high_elem: Datum<'mcx>,
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

    /// itemIndex, or -1 if not valid
    pub markItemIndex: i32,

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
            markItemIndex: -1,
            currTuples: None,
            markTuples: None,
            currPos: BTScanPosData::new(mcx),
            markPos: BTScanPosData::new(mcx),
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
/// index, accumulated across `btbulkdelete`/`btvacuumcleanup`.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexBulkDeleteResult {
    /// `BlockNumber num_pages` — pages remaining in index.
    pub num_pages: BlockNumber,
    /// `bool estimated_count` — `num_index_tuples` is an estimate.
    pub estimated_count: bool,
    /// `double num_index_tuples` — tuples remaining.
    pub num_index_tuples: f64,
    /// `double tuples_removed` — number removed during vacuum operation.
    pub tuples_removed: f64,
    /// `BlockNumber pages_newly_deleted` — pages marked deleted by us.
    pub pages_newly_deleted: BlockNumber,
    /// `BlockNumber pages_deleted` — pages marked deleted (could be by us).
    pub pages_deleted: BlockNumber,
    /// `BlockNumber pages_free` — pages available for reuse.
    pub pages_free: BlockNumber,
}
