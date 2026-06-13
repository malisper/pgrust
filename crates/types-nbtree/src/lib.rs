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
use types_datum::Datum;
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

/// `BTSkipSupport` opclass sentinels for a skip array (`access/nbtree.h`).
#[derive(Clone, Copy, Debug)]
pub struct BTSkipSupport {
    /// lowest sorting non-NULL value
    pub low_elem: Datum,
    /// highest sorting non-NULL value
    pub high_elem: Datum,
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
    pub elem_values: PgVec<'mcx, Datum>,
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
    pub sksup_data: Option<BTSkipSupport>,
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
    pub keyData: PgVec<'mcx, ScanKeyData>,

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
