//! Runtime GiST access-method vocabulary (`access/gist.h`,
//! `access/gist_private.h`, `access/gistxlog.h`) on the real value model.
//!
//! The C structs are translated field-for-field, re-homed onto the owned
//! memory model: `MemoryContext` becomes [`mcx::Mcx`], `TupleDesc` becomes the
//! owned [`types_tuple::heaptuple::TupleDesc`], `FmgrInfo[INDEX_MAX_KEYS]`
//! becomes a [`Vec`] (one entry per index attribute), `Page` bytes are reached
//! through the bufmgr seam rather than carried as raw pointers, and
//! `palloc`'d working arrays become [`Vec`].

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{
    uint16, BlockNumber, OffsetNumber, Oid, Size, XLogRecPtr,
};
use types_core::xact::FullTransactionId;
use types_nodes::nodehash::BufFile;
use types_storage::storage::Buffer;
use types_tableam::genam::IndexOrderByDistance;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{HeapTuple, ItemPointerData, TupleDesc};

// ---------------------------------------------------------------------------
// gist.h — amproc indexes
// ---------------------------------------------------------------------------

pub const GIST_CONSISTENT_PROC: i32 = 1;
pub const GIST_UNION_PROC: i32 = 2;
pub const GIST_COMPRESS_PROC: i32 = 3;
pub const GIST_DECOMPRESS_PROC: i32 = 4;
pub const GIST_PENALTY_PROC: i32 = 5;
pub const GIST_PICKSPLIT_PROC: i32 = 6;
pub const GIST_EQUAL_PROC: i32 = 7;
pub const GIST_DISTANCE_PROC: i32 = 8;
pub const GIST_FETCH_PROC: i32 = 9;
pub const GIST_OPTIONS_PROC: i32 = 10;
pub const GIST_SORTSUPPORT_PROC: i32 = 11;
pub const GIST_TRANSLATE_CMPTYPE_PROC: i32 = 12;
pub const GISTNProcs: i32 = 12;

// gist.h — page opaque flags
/// leaf page
pub const F_LEAF: uint16 = 1 << 0;
/// the page has been deleted
pub const F_DELETED: uint16 = 1 << 1;
/// some tuples on the page were deleted
pub const F_TUPLES_DELETED: uint16 = 1 << 2;
/// page to the right has no downlink
pub const F_FOLLOW_RIGHT: uint16 = 1 << 3;
/// some tuples on the page are dead, but not deleted yet
pub const F_HAS_GARBAGE: uint16 = 1 << 4;

/// `GistNSN` (gist.h) — node sequence number, a special-purpose `XLogRecPtr`.
pub type GistNSN = XLogRecPtr;

/// `GistBuildLSN` (gist.h) — fake LSN/NSN used during index builds.
pub const GistBuildLSN: XLogRecPtr = 1;

/// `GIST_PAGE_ID` (gist.h) — page identifier stored as last 2 bytes on a page.
pub const GIST_PAGE_ID: uint16 = 0xFF81;

/// `GISTPageOpaqueData` (gist.h) — the special area of every GiST page.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GISTPageOpaqueData {
    /// `PageGistNSN nsn` — must change on page split.
    pub nsn: GistNSN,
    /// next page if any.
    pub rightlink: BlockNumber,
    /// see `F_*` bit definitions.
    pub flags: uint16,
    /// for identification of GiST indexes.
    pub gist_page_id: uint16,
}

/// `GIST_ROOT_BLKNO` (gist_private.h) — root page of a gist index.
pub const GIST_ROOT_BLKNO: BlockNumber = 0;

// gist_private.h — invalid-tuple sentinels
pub const TUPLE_IS_VALID: u16 = 0xffff;
pub const TUPLE_IS_INVALID: u16 = 0xfffe;

// gist_private.h — buffering mode
pub const GIST_MAX_SPLIT_PAGES: i32 = 75;
pub const GIST_MIN_FILLFACTOR: i32 = 10;
pub const GIST_DEFAULT_FILLFACTOR: i32 = 90;

/// `GISTDeletedPageContents` (gist.h) — stored after the page header on a
/// deleted page.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GISTDeletedPageContents {
    /// last xid which could see the page in a scan.
    pub deleteXid: FullTransactionId,
}

/// `GISTENTRY` (gist.h) — an entry on a GiST node: the key plus its own
/// location.
///
/// C carries `Relation rel` / `Page page` raw pointers so the support
/// functions can reach the page; in the owned model the page bytes are reached
/// through the bufmgr seam, so this carries the index relation Oid and the
/// block number identifying the page instead of raw pointers.
#[derive(Clone, Debug)]
pub struct GISTENTRY<'mcx> {
    /// `Datum key`.
    pub key: Datum<'mcx>,
    /// `Relation rel` — the index relation Oid (`InvalidOid` when absent).
    pub rel: Oid,
    /// `Page page` — block number of the page this entry lives on
    /// (`InvalidBlockNumber` when absent).
    pub page: BlockNumber,
    /// `OffsetNumber offset`.
    pub offset: OffsetNumber,
    /// `bool leafkey`.
    pub leafkey: bool,
}

/// `GIST_SPLITVEC` (gist.h) — the split vector returned by the PickSplit method.
#[derive(Clone, Debug, Default)]
pub struct GIST_SPLITVEC<'mcx> {
    /// `OffsetNumber *spl_left` — entries that go left (length = `spl_nleft`).
    pub spl_left: Vec<OffsetNumber>,
    /// `Datum spl_ldatum` — union of keys in `spl_left`.
    pub spl_ldatum: Option<Datum<'mcx>>,
    /// `bool spl_ldatum_exists`.
    pub spl_ldatum_exists: bool,
    /// `OffsetNumber *spl_right` — entries that go right (length = `spl_nright`).
    pub spl_right: Vec<OffsetNumber>,
    /// `Datum spl_rdatum` — union of keys in `spl_right`.
    pub spl_rdatum: Option<Datum<'mcx>>,
    /// `bool spl_rdatum_exists`.
    pub spl_rdatum_exists: bool,
}

/// `GistEntryVector` (gist.h) — vector of `GISTENTRY` structs passed to the
/// user-defined union and picksplit methods.
#[derive(Clone, Debug)]
pub struct GistEntryVector<'mcx> {
    /// `int32 n` — number of elements.
    pub n: i32,
    /// `GISTENTRY vector[FLEXIBLE_ARRAY_MEMBER]`.
    pub vector: Vec<GISTENTRY<'mcx>>,
}

// ---------------------------------------------------------------------------
// gist_private.h — GISTSTATE
// ---------------------------------------------------------------------------

/// `GISTSTATE` (gist_private.h) — information needed for any GiST index
/// operation: opclass support-function call info per index column, plus the
/// index's tuple descriptors.
pub struct GISTSTATE<'mcx> {
    /// `MemoryContext scanCxt` — context for scan-lifespan data.
    pub scanCxt: Mcx<'mcx>,
    /// `MemoryContext tempCxt` — short-term context for calling functions.
    pub tempCxt: Mcx<'mcx>,

    /// `TupleDesc leafTupdesc` — index's tuple descriptor.
    pub leafTupdesc: TupleDesc<'mcx>,
    /// `TupleDesc nonLeafTupdesc` — truncated descriptor for non-leaf pages.
    pub nonLeafTupdesc: TupleDesc<'mcx>,
    /// `TupleDesc fetchTupdesc` — descriptor for index-only-scan tuples.
    pub fetchTupdesc: TupleDesc<'mcx>,

    /// `FmgrInfo consistentFn[INDEX_MAX_KEYS]` (one per index attribute).
    pub consistentFn: Vec<types_core::fmgr::FmgrInfo>,
    pub unionFn: Vec<types_core::fmgr::FmgrInfo>,
    pub compressFn: Vec<types_core::fmgr::FmgrInfo>,
    pub decompressFn: Vec<types_core::fmgr::FmgrInfo>,
    pub penaltyFn: Vec<types_core::fmgr::FmgrInfo>,
    pub picksplitFn: Vec<types_core::fmgr::FmgrInfo>,
    pub equalFn: Vec<types_core::fmgr::FmgrInfo>,
    pub distanceFn: Vec<types_core::fmgr::FmgrInfo>,
    pub fetchFn: Vec<types_core::fmgr::FmgrInfo>,

    /// `Oid supportCollation[INDEX_MAX_KEYS]` — collations passed to support fns.
    pub supportCollation: Vec<Oid>,
}

// ---------------------------------------------------------------------------
// gist_private.h — scan queue items
// ---------------------------------------------------------------------------

/// `GISTSearchHeapItem` (gist_private.h) — individual heap tuple to be visited.
#[derive(Clone, Debug)]
pub struct GISTSearchHeapItem<'mcx> {
    /// `ItemPointerData heapPtr`.
    pub heapPtr: ItemPointerData,
    /// `bool recheck` — quals must be rechecked.
    pub recheck: bool,
    /// `bool recheckDistances` — distances must be rechecked.
    pub recheckDistances: bool,
    /// `HeapTuple recontup` — data reconstructed from the index (IOS).
    pub recontup: HeapTuple<'mcx>,
    /// `OffsetNumber offnum` — page offset, to mark tuple LP_DEAD.
    pub offnum: OffsetNumber,
}

/// The body of a `GISTSearchItem`: either an index page (carrying the parent's
/// LSN) or a heap tuple. Replaces the C `union data`.
#[derive(Clone, Debug)]
pub enum GISTSearchItemData<'mcx> {
    /// index page — parent page's LSN, to detect a concurrent split.
    Parentlsn(GistNSN),
    /// heap tuple — heap info.
    Heap(GISTSearchHeapItem<'mcx>),
}

/// `GISTSearchItem` (gist_private.h) — an unvisited item, index page or heap
/// tuple, queued during a search.
#[derive(Clone, Debug)]
pub struct GISTSearchItem<'mcx> {
    /// `BlockNumber blkno` — index page number, or `InvalidBlockNumber` for a
    /// heap item.
    pub blkno: BlockNumber,
    /// `union data` — parent LSN (page) or heap info (heap tuple).
    pub data: GISTSearchItemData<'mcx>,
    /// `IndexOrderByDistance distances[FLEXIBLE_ARRAY_MEMBER]` —
    /// `numberOfOrderBys` entries.
    pub distances: Vec<IndexOrderByDistance>,
}

/// `GISTSearchItemIsHeap(item)` (gist_private.h) — true if the item is a heap
/// tuple rather than an index page.
#[inline]
pub fn GISTSearchItemIsHeap(item: &GISTSearchItem<'_>) -> bool {
    item.blkno == types_core::InvalidBlockNumber
}

/// `GISTScanOpaqueData` is the concrete type stored in `IndexScanDescData.opaque`
/// (C's `void *opaque`); the A0 [`types_tableam::amopaque::AmOpaque`] carrier
/// downcasts to it in the GiST scan callbacks. The tag is defined centrally in
/// `types_tableam::amopaque::tags::GIST_SCAN`.
impl<'mcx> types_tableam::amopaque::AmOpaqueType<'mcx> for GISTScanOpaqueData<'mcx> {
    const TAG: types_tableam::amopaque::AmOpaqueTag = types_tableam::amopaque::tags::GIST_SCAN;
}

/// The pairing-heap comparator function-pointer type for the
/// [`GISTSearchItem`] search queue. A plain `fn` (not a closure) because the
/// only state the comparator needs — `scan->numberOfOrderBys` — is carried by
/// each item's `distances.len()`.
pub type GISTSearchItemCmp =
    fn(&GISTSearchItem<'_>, &GISTSearchItem<'_>) -> core::cmp::Ordering;

/// `GISTScanOpaqueData` (gist_private.h) — the GiST scan's `void *opaque`
/// payload (stored in `IndexScanDescData.opaque` via the A0 carrier).
pub struct GISTScanOpaqueData<'mcx> {
    /// `GISTSTATE *giststate` — index information, see above.
    pub giststate: GISTSTATE<'mcx>,
    /// `pairingheap *queue` — queue of unvisited items.
    pub queue: backend_lib_pairingheap::PairingHeap<GISTSearchItem<'mcx>, GISTSearchItemCmp>,
    /// `MemoryContext queueCxt` — context holding the queue.
    pub queueCxt: Mcx<'mcx>,
    /// `MemoryContext pageDataCxt` — context holding `pageData` tuples; `None`
    /// until an index-only scan needs it.
    pub pageDataCxt: Option<Mcx<'mcx>>,

    /// `bool qual_ok` — false if qual can never be satisfied.
    pub qual_ok: bool,
    /// `bool firstCall` — true until first gistgettuple call.
    pub firstCall: bool,

    /// `IndexOrderByDistance *distances` — output area for gistindex_keytest;
    /// `numberOfOrderBys` entries.
    pub distances: Vec<IndexOrderByDistance>,
    /// `Oid *orderByTypes` — datatypes of ordering operators; `numberOfOrderBys`
    /// entries.
    pub orderByTypes: Vec<Oid>,

    /// `OffsetNumber *killedItems` — offsets of killed items in the current
    /// page; allocated lazily.
    pub killedItems: Option<Vec<OffsetNumber>>,
    /// `int numKilled` — number of currently stored items.
    pub numKilled: i32,
    /// `BlockNumber curBlkno` — current number of block.
    pub curBlkno: BlockNumber,
    /// `GistNSN curPageLSN` — pos in the WAL stream when page was read.
    pub curPageLSN: XLogRecPtr,

    /// `GISTSearchHeapItem pageData[BLCKSZ / sizeof(IndexTupleData)]` — tuples
    /// found on the current leaf page (plain/non-ordered scan).
    pub pageData: Vec<GISTSearchHeapItem<'mcx>>,
    /// `OffsetNumber nPageData` — number of valid entries in `pageData`.
    pub nPageData: OffsetNumber,
    /// `OffsetNumber curPageData` — next entry in `pageData` to return.
    pub curPageData: OffsetNumber,
}

// ---------------------------------------------------------------------------
// gist_private.h — split / insert working structures
// ---------------------------------------------------------------------------

/// `gistxlogPage` (gist_private.h) — page header in a split layout (despite the
/// name, not part of any xlog record).
#[derive(Clone, Copy, Debug, Default)]
pub struct gistxlogPage {
    pub blkno: BlockNumber,
    /// number of index tuples following.
    pub num: i32,
}

/// `SplitPageLayout` (gist_private.h) — `gistSplit` function result, one per
/// produced page half (the C linked list becomes a `Vec` of these).
pub struct SplitPageLayout<'mcx> {
    pub block: gistxlogPage,
    /// `IndexTupleData *list` — the page's tuples (concatenated on-disk form).
    pub list: PgVec<'mcx, u8>,
    pub lenlist: i32,
    /// `IndexTuple itup` — union key (downlink) for the page; the on-disk index
    /// tuple byte image (`None` for the synthetic root page). In the owned model
    /// an index tuple is its contiguous byte image, not a header-only carrier.
    pub itup: Option<PgVec<'mcx, u8>>,
    /// `Page page` — block number of the page being written, once assigned.
    pub page: BlockNumber,
    /// `Buffer buffer` — to write after all proceed.
    pub buffer: Buffer,
}

/// `GISTInsertStack` (gist_private.h) — locking buffers and transferring
/// arguments during insertion. The C `parent` pointer becomes a `Vec`-indexed
/// stack owned by the descent; here each frame carries its own fields and the
/// descent threads parent links by index.
#[derive(Clone, Debug)]
pub struct GISTInsertStack {
    /// current page block number.
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    /// `Page page` — modelled by re-reading from `buffer`; kept here as the
    /// block number for identity (the bytes are fetched via the bufmgr seam).
    pub page: BlockNumber,
    /// `GistNSN lsn` — page LSN to recognize an update/split.
    pub lsn: GistNSN,
    /// `bool retry_from_parent`.
    pub retry_from_parent: bool,
    /// `OffsetNumber downlinkoffnum` — offset of the downlink in the parent.
    pub downlinkoffnum: OffsetNumber,
    /// index of the parent frame in the descent's stack, or `None` at the root.
    pub parent: Option<usize>,
}

/// `GistSplitVector` (gist_private.h) — working state and results for the
/// multi-column split logic in gistsplit.c.
#[derive(Clone, Debug, Default)]
pub struct GistSplitVector<'mcx> {
    /// `GIST_SPLITVEC splitVector` — passed to/from the user PickSplit method.
    pub splitVector: GIST_SPLITVEC<'mcx>,
    /// `Datum spl_lattr[INDEX_MAX_KEYS]` — union of subkeys in `spl_left`.
    pub spl_lattr: Vec<Option<Datum<'mcx>>>,
    pub spl_lisnull: Vec<bool>,
    /// `Datum spl_rattr[INDEX_MAX_KEYS]` — union of subkeys in `spl_right`.
    pub spl_rattr: Vec<Option<Datum<'mcx>>>,
    pub spl_risnull: Vec<bool>,
    /// `bool *spl_dontcare` — tuples that could go either side for zero penalty.
    pub spl_dontcare: Vec<bool>,
}

/// `GISTInsertState` (gist_private.h) — top-level insert state.
pub struct GISTInsertState {
    /// `Relation r` — the index relation Oid.
    pub r: Oid,
    /// `Relation heapRel` — the heap relation Oid.
    pub heapRel: Oid,
    /// `Size freespace` — free space to be left.
    pub freespace: Size,
    pub is_build: bool,
    /// `GISTInsertStack *stack` — the descent stack (`Vec`, root at index 0).
    pub stack: Vec<GISTInsertStack>,
}

/// `GISTPageSplitInfo` (gist_private.h) — a List of these is returned from
/// `gistplacetopage` in `*splitinfo`.
pub struct GISTPageSplitInfo<'mcx> {
    /// `Buffer buf` — the split page "half".
    pub buf: Buffer,
    /// `IndexTuple downlink` — downlink for this half; the on-disk index tuple
    /// byte image (an index tuple is its contiguous byte image here).
    pub downlink: PgVec<'mcx, u8>,
}

// ---------------------------------------------------------------------------
// gist_private.h — buffering-build node buffers
// ---------------------------------------------------------------------------

/// `GISTNodeBufferPage` (gist_private.h) — on-temp-file page format used by the
/// buffering build to spill node buffers to disk.
///
/// `BUFFER_PAGE_DATA_OFFSET = MAXALIGN(offsetof(GISTNodeBufferPage, tupledata))`
/// is the on-disk header size before the flexible `tupledata`; in the owned
/// model the header is the two scalar fields and `tupledata` is an owned `Vec`.
#[derive(Clone, Debug)]
pub struct GISTNodeBufferPage {
    /// `BlockNumber prev` — previous block of this node buffer.
    pub prev: BlockNumber,
    /// `uint32 freespace` — free space remaining on this page
    /// (`PAGE_FREE_SPACE`).
    pub freespace: u32,
    /// `char tupledata[FLEXIBLE_ARRAY_MEMBER]` — the tuples start here.
    pub tupledata: Vec<u8>,
}

/// `GISTNodeBuffer` (gist_private.h) — a buffer attached to an internal node,
/// used when building an index in buffering mode.
///
/// In C the same `GISTNodeBuffer *` is aliased from several places at once:
/// the `nodeBuffersTab` hash (keyed by block number), the
/// `bufferEmptyingQueue`, one of the `buffersOnLevels[level]` lists, and the
/// `loadedBuffers` array. On a page split the buffer is copied into a
/// temporary [`GISTNodeBuffer`] (`isTemp == true`) and the original is reused.
/// The owned-value model cannot express this 1-to-many aliasing, so the buffer
/// is carried behind [`SharedNodeBuffer`] = `Rc<RefCell<GISTNodeBuffer>>`
/// (the sanctioned shared-aliasing carrier, like the reorderbuffer / snapmgr
/// `Rc<RefCell>` models — not a registry) and every collection holds a clone
/// of that handle.
pub struct GISTNodeBuffer {
    /// `BlockNumber nodeBlocknum` — index block # this buffer is for.
    pub nodeBlocknum: BlockNumber,
    /// `int32 blocksCount` — current # of blocks occupied by buffer.
    pub blocksCount: i32,
    /// `BlockNumber pageBlocknum` — temporary file block #.
    pub pageBlocknum: BlockNumber,
    /// `GISTNodeBufferPage *pageBuffer` — in-memory buffer page (`NULL` when no
    /// page is currently loaded).
    pub pageBuffer: Option<GISTNodeBufferPage>,
    /// `bool queuedForEmptying` — is this buffer queued for emptying?
    pub queuedForEmptying: bool,
    /// `bool isTemp` — is this a temporary copy, not in the hash table?
    pub isTemp: bool,
    /// `int level` — 0 == leaf.
    pub level: i32,
}

/// A `GISTNodeBuffer *` as aliased by the buffering build: shared across
/// `nodeBuffersTab`, `bufferEmptyingQueue`, `buffersOnLevels`, and
/// `loadedBuffers`, with copy-and-reuse on split. See [`GISTNodeBuffer`].
pub type SharedNodeBuffer = Rc<RefCell<GISTNodeBuffer>>;

/// `GISTBuildBuffers` (gist_private.h) — general information about the build
/// buffers used by a buffering GiST index build (`gistbuildbuffers.c`).
///
/// The C `HTAB *nodeBuffersTab` keyed by block number becomes a
/// `HashMap<BlockNumber, SharedNodeBuffer>`; the `List *` queue and per-level
/// lists become `Vec` of the shared handles; the `GISTNodeBuffer **` loaded
/// array becomes a `Vec` of shared handles. The same buffer object appears in
/// several of these at once via cloned [`Rc`] handles, mirroring C's pointer
/// aliasing.
pub struct GISTBuildBuffers<'mcx> {
    /// `MemoryContext context` — persistent context for the buffers and
    /// metadata.
    pub context: Mcx<'mcx>,
    /// `BufFile *pfile` — temporary file to store buffers in (`None` before the
    /// first spill).
    pub pfile: Option<PgBox<'mcx, BufFile>>,
    /// `long nFileBlocks` — current size of the temporary file, in blocks.
    pub nFileBlocks: i64,

    /// `long *freeBlocks` — resizable array of free temp-file blocks. The C
    /// `nFreeBlocks` / `freeBlocksLen` length+capacity bookkeeping is the
    /// `Vec`'s own `len()` / `capacity()`.
    pub freeBlocks: Vec<i64>,

    /// `HTAB *nodeBuffersTab` — hash of buffers by block number.
    pub nodeBuffersTab: HashMap<BlockNumber, SharedNodeBuffer>,

    /// `List *bufferEmptyingQueue` — list of buffers scheduled for emptying.
    pub bufferEmptyingQueue: Vec<SharedNodeBuffer>,

    /// `int levelStep` — which levels in the tree have buffers
    /// (`LEVEL_HAS_BUFFERS`).
    pub levelStep: i32,
    /// `int pagesPerBuffer` — nominal size of each buffer, in pages
    /// (`BUFFER_HALF_FILLED` / `BUFFER_OVERFLOWED`).
    pub pagesPerBuffer: i32,

    /// `List **buffersOnLevels` — array of lists of buffers on each level, for
    /// final emptying. The C `buffersOnLevelsLen` is `buffersOnLevels.len()`.
    pub buffersOnLevels: Vec<Vec<SharedNodeBuffer>>,

    /// `GISTNodeBuffer **loadedBuffers` — buffers that currently have their last
    /// page loaded in main memory. The C `loadedBuffersCount` / `loadedBuffersLen`
    /// are the `Vec`'s `len()` / `capacity()`.
    pub loadedBuffers: Vec<SharedNodeBuffer>,

    /// `int rootlevel` — level of the current root node (= height of the index
    /// tree - 1).
    pub rootlevel: i32,
}

/// `GistSortedBuildLevelState` (gistbuild.c) — the sorted-build per-level
/// stack frame, one for each level, holding an in-memory buffer of the last
/// pages at that level.
///
/// (The GiST build campaign tracks this carrier under the working name
/// `GISTLoadedPartItem`: it is the "loaded last pages" item the sorted build
/// keeps in memory per level.)
///
/// `GIST_SORTED_BUILD_PAGE_NUM == 4` pages are buffered so a
/// multidimension-aware picksplit can be applied. The C `parent` pointer to
/// the upper level becomes an owned `Box` of the parent frame (the stack is
/// built bottom-up and threaded by ownership). Each `Page` is the owned page
/// bytes (`None` for a not-yet-allocated slot).
pub struct GistSortedBuildLevelState {
    /// `int current_page` — index of the page currently being filled in `pages`.
    pub current_page: i32,
    /// `BlockNumber last_blkno` — block # of the last written page at this level.
    pub last_blkno: BlockNumber,
    /// `struct GistSortedBuildLevelState *parent` — upper level, if any.
    pub parent: Option<Box<GistSortedBuildLevelState>>,
    /// `Page pages[GIST_SORTED_BUILD_PAGE_NUM]` — the buffered last pages.
    pub pages: [Option<Vec<u8>>; GIST_SORTED_BUILD_PAGE_NUM as usize],
}

/// `GIST_SORTED_BUILD_PAGE_NUM` (gistbuild.c) — number of last pages buffered
/// per level during a sorted GiST build.
pub const GIST_SORTED_BUILD_PAGE_NUM: i32 = 4;

/// `GistOptBufferingMode` (gist_private.h) — `GiSTOptions->buffering_mode`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GistOptBufferingMode {
    Auto = 0,
    On = 1,
    Off = 2,
}

impl Default for GistOptBufferingMode {
    fn default() -> Self {
        GistOptBufferingMode::Auto
    }
}

/// `GiSTOptions` (gist_private.h) — storage type for GiST's reloptions.
#[derive(Clone, Copy, Debug)]
pub struct GiSTOptions {
    /// varlena header (do not touch directly).
    pub vl_len_: i32,
    /// page fill factor in percent (0..100).
    pub fillfactor: i32,
    /// buffering build mode.
    pub buffering_mode: GistOptBufferingMode,
}

#[cfg(test)]
mod buildbuffers_tests {
    use super::*;
    use mcx::MemoryContext;

    fn new_node_buffer(blkno: BlockNumber, level: i32) -> SharedNodeBuffer {
        Rc::new(RefCell::new(GISTNodeBuffer {
            nodeBlocknum: blkno,
            blocksCount: 0,
            pageBlocknum: u32::MAX, // C InvalidBlockNumber
            pageBuffer: None,
            queuedForEmptying: false,
            isTemp: false,
            level,
        }))
    }

    #[test]
    fn node_buffer_aliases_across_collections() {
        let ctx = MemoryContext::new("gist-build");
        let mut gfbb = GISTBuildBuffers {
            context: ctx.mcx(),
            pfile: None,
            nFileBlocks: 0,
            freeBlocks: Vec::new(),
            nodeBuffersTab: HashMap::new(),
            bufferEmptyingQueue: Vec::new(),
            levelStep: 1,
            pagesPerBuffer: 4,
            buffersOnLevels: Vec::new(),
            loadedBuffers: Vec::new(),
            rootlevel: 0,
        };

        // One buffer, aliased into the hash table, the emptying queue, its
        // level list, and the loaded array — exactly as gistGetNodeBuffer wires
        // a fresh buffer in C.
        let buf = new_node_buffer(42, 0);
        gfbb.nodeBuffersTab.insert(42, Rc::clone(&buf));
        gfbb.bufferEmptyingQueue.push(Rc::clone(&buf));
        gfbb.buffersOnLevels.push(vec![Rc::clone(&buf)]);
        gfbb.loadedBuffers.push(Rc::clone(&buf));
        assert_eq!(Rc::strong_count(&buf), 5);

        // Mutate through the hash-table alias; every other alias sees it.
        gfbb.nodeBuffersTab.get(&42).unwrap().borrow_mut().blocksCount = 7;
        gfbb.nodeBuffersTab.get(&42).unwrap().borrow_mut().queuedForEmptying = true;
        assert_eq!(gfbb.bufferEmptyingQueue[0].borrow().blocksCount, 7);
        assert!(gfbb.buffersOnLevels[0][0].borrow().queuedForEmptying);
        assert_eq!(gfbb.loadedBuffers[0].borrow().blocksCount, 7);
    }

    #[test]
    fn temp_copy_on_split_reuses_original() {
        // gistRelocateBuildBuffersOnSplit copies a buffer into a temporary
        // (isTemp == true) and keeps reusing the original. The shared carrier
        // lets a deep clone of the inner value coexist with the live handle.
        let buf = new_node_buffer(10, 1);
        buf.borrow_mut().blocksCount = 3;

        let temp = {
            let orig = buf.borrow();
            Rc::new(RefCell::new(GISTNodeBuffer {
                nodeBlocknum: orig.nodeBlocknum,
                blocksCount: orig.blocksCount,
                pageBlocknum: orig.pageBlocknum,
                pageBuffer: orig.pageBuffer.clone(),
                queuedForEmptying: orig.queuedForEmptying,
                isTemp: true,
                level: orig.level,
            }))
        };

        // The original keeps being mutated independently of the temp copy.
        buf.borrow_mut().blocksCount = 9;
        assert!(temp.borrow().isTemp);
        assert_eq!(temp.borrow().blocksCount, 3);
        assert!(!buf.borrow().isTemp);
        assert_eq!(buf.borrow().blocksCount, 9);
    }

    #[test]
    fn node_buffer_page_layout() {
        let mut page = GISTNodeBufferPage {
            prev: u32::MAX,
            freespace: 100,
            tupledata: Vec::new(),
        };
        page.tupledata.extend_from_slice(&[1, 2, 3, 4]);
        page.freespace -= 4;
        assert_eq!(page.freespace, 96);
        assert_eq!(page.tupledata.len(), 4);
    }

    #[test]
    fn sorted_build_level_state_stack() {
        // A two-level stack: leaf level points at its parent.
        let parent = GistSortedBuildLevelState {
            current_page: 0,
            last_blkno: 1,
            parent: None,
            pages: [None, None, None, None],
        };
        let mut leaf = GistSortedBuildLevelState {
            current_page: 0,
            last_blkno: 2,
            parent: Some(Box::new(parent)),
            pages: [None, None, None, None],
        };
        assert_eq!(GIST_SORTED_BUILD_PAGE_NUM, 4);
        assert_eq!(leaf.pages.len(), GIST_SORTED_BUILD_PAGE_NUM as usize);

        leaf.pages[0] = Some(vec![0u8; 8192]);
        assert_eq!(leaf.pages[0].as_ref().unwrap().len(), 8192);
        assert!(leaf.pages[1].is_none());
        assert_eq!(leaf.parent.as_ref().unwrap().last_blkno, 1);
        assert!(leaf.parent.as_ref().unwrap().parent.is_none());
    }
}
