//! Runtime GIN access-method vocabulary (`access/gin.h`, `access/ginblock.h`,
//! `access/gin_private.h`) on the real owned value model.
//!
//! The C structs are translated field-for-field, re-homed onto the owned memory
//! model: `MemoryContext` becomes [`mcx::Mcx`], `TupleDesc` becomes the owned
//! [`types_tuple::heaptuple::TupleDesc`], `FmgrInfo[INDEX_MAX_KEYS]` becomes a
//! [`Vec`] (one entry per index attribute), `Relation` becomes the index
//! relation `Oid` (the page bytes are reached through the bufmgr seam, as in
//! `types-gist`), `Datum` becomes the owned [`types_datum`] value, `palloc`'d
//! working arrays become [`Vec`], and the C "array of `GinScanEntry` pointers"
//! is reified as a [`Vec`] of indices into the owning `GinScanOpaqueData.entries`
//! pool (`entries` is owned by the not-yet-ported `ginscan`).
//!
//! This crate is the single owner of the whole GIN vocabulary. `types-tsearch`
//! (the `tsvector_ops` GIN support functions) re-exports the ternary / search
//! vocabulary and the consistent-fn dispatch model from here, so the audited
//! `gin-core-probe` consistent-fn lane and the two fmgr consistent-call seams
//! reference the *same* full [`GinScanKey`] / [`GinState`] the eventual
//! `ginscan.c`/`ginget.c` machinery builds — exactly as in C, where the trimmed
//! and full views are one struct (`GinScanKeyData`).
//!
//! `GinBtreeData` carries C function pointers (the entry/data-page btree
//! vtable, filled by `ginentrypage.c`/`gindatapage.c`); that opacity is
//! inherited from C and kept as `Option<fn ...>` slots.

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::Mcx;
use types_error::PgResult;
use types_xlog_records::ginxlog::PostingItem;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{BlockNumber, OffsetNumber, Oid};
use types_core::{InvalidOid, INDEX_MAX_KEYS};
use types_scan::scankey::StrategyNumber;
use types_storage::storage::Buffer;
use types_tidbitmap::{TBMPrivateIterator, TIDBitmap};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, TupleDesc};

// ===========================================================================
// access/gin.h — ternary / search-mode vocabulary and amproc indexes.
// ===========================================================================

/// `GinTernaryValue` — a `char`-sized tri-state (access/gin.h).
pub type GinTernaryValue = i8;

/// `GIN_FALSE`: item is not present / does not match.
pub const GIN_FALSE: GinTernaryValue = 0;
/// `GIN_TRUE`: item is present / matches.
pub const GIN_TRUE: GinTernaryValue = 1;
/// `GIN_MAYBE`: don't know if item is present / matches.
pub const GIN_MAYBE: GinTernaryValue = 2;

/// `GIN_SEARCH_MODE_DEFAULT` (access/gin.h).
pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
/// `GIN_SEARCH_MODE_INCLUDE_EMPTY` (access/gin.h).
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
/// `GIN_SEARCH_MODE_ALL` (access/gin.h).
pub const GIN_SEARCH_MODE_ALL: i32 = 2;
/// `GIN_SEARCH_MODE_EVERYTHING` (access/gin.h) — for internal use only.
pub const GIN_SEARCH_MODE_EVERYTHING: i32 = 3;

// access/gin.h — opclass support-function numbers.
pub const GIN_COMPARE_PROC: i32 = 1;
pub const GIN_EXTRACTVALUE_PROC: i32 = 2;
pub const GIN_EXTRACTQUERY_PROC: i32 = 3;
pub const GIN_CONSISTENT_PROC: i32 = 4;
pub const GIN_COMPARE_PARTIAL_PROC: i32 = 5;
pub const GIN_TRICONSISTENT_PROC: i32 = 6;
pub const GIN_OPTIONS_PROC: i32 = 7;
/// `GINNProcs` (access/gin.h) — number of support functions.
pub const GINNProcs: i32 = 7;

/// `GIN_DEFAULT_USE_FASTUPDATE` (gin_private.h).
pub const GIN_DEFAULT_USE_FASTUPDATE: bool = true;

// access/gin_private.h — buffer lock modes (aliases of BUFFER_LOCK_*).
pub const GIN_UNLOCK: i32 = 0;
pub const GIN_SHARE: i32 = 1;
pub const GIN_EXCLUSIVE: i32 = 2;

// ===========================================================================
// access/ginblock.h — GinNullCategory, page opaque, metapage, flags.
// ===========================================================================

/// `GinNullCategory` (ginblock.h) — a `signed char` category accompanying every
/// stored key.
pub type GinNullCategory = i8;

/// `GIN_CAT_NORM_KEY` — normal, non-null key value.
pub const GIN_CAT_NORM_KEY: GinNullCategory = 0;
/// `GIN_CAT_NULL_KEY` — null key value.
pub const GIN_CAT_NULL_KEY: GinNullCategory = 1;
/// `GIN_CAT_EMPTY_ITEM` — placeholder for zero-key item.
pub const GIN_CAT_EMPTY_ITEM: GinNullCategory = 2;
/// `GIN_CAT_NULL_ITEM` — placeholder for null item.
pub const GIN_CAT_NULL_ITEM: GinNullCategory = 3;
/// `GIN_CAT_EMPTY_QUERY` — placeholder for full-scan query.
pub const GIN_CAT_EMPTY_QUERY: GinNullCategory = -1;

/// `GinPageOpaqueData` (ginblock.h) — the special area of every GIN page.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GinPageOpaqueData {
    /// `BlockNumber rightlink` — next page if any.
    pub rightlink: BlockNumber,
    /// `OffsetNumber maxoff` — number of `PostingItem`s on a `GIN_DATA &
    /// ~GIN_LEAF` page; on a `GIN_LIST` page, number of heap tuples.
    pub maxoff: OffsetNumber,
    /// `uint16 flags` — see the `GIN_*` page flag bit definitions.
    pub flags: u16,
}

// ginblock.h — page flag bits.
pub const GIN_DATA: u16 = 1 << 0;
pub const GIN_LEAF: u16 = 1 << 1;
pub const GIN_DELETED: u16 = 1 << 2;
pub const GIN_META: u16 = 1 << 3;
pub const GIN_LIST: u16 = 1 << 4;
pub const GIN_LIST_FULLROW: u16 = 1 << 5;
/// page was split, but parent not updated.
pub const GIN_INCOMPLETE_SPLIT: u16 = 1 << 6;
pub const GIN_COMPRESSED: u16 = 1 << 7;

/// `GIN_METAPAGE_BLKNO` (ginblock.h) — fixed location of the meta page.
pub const GIN_METAPAGE_BLKNO: BlockNumber = 0;
/// `GIN_ROOT_BLKNO` (ginblock.h) — fixed location of the entry-tree root.
pub const GIN_ROOT_BLKNO: BlockNumber = 1;

/// `GinMetaPageData` (ginblock.h) — contents of the GIN meta page.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GinMetaPageData {
    /// `BlockNumber head` — head of the pending list (GIN_LIST pages).
    pub head: BlockNumber,
    /// `BlockNumber tail` — tail of the pending list.
    pub tail: BlockNumber,
    /// `uint32 tailFreeSize` — free space in bytes in the pending list's tail.
    pub tailFreeSize: u32,
    /// `BlockNumber nPendingPages` — number of pages in the pending list.
    pub nPendingPages: BlockNumber,
    /// `int64 nPendingHeapTuples` — heap tuples in the pending list.
    pub nPendingHeapTuples: i64,
    /// `BlockNumber nTotalPages` — planner stats (as of last VACUUM).
    pub nTotalPages: BlockNumber,
    /// `BlockNumber nEntryPages`.
    pub nEntryPages: BlockNumber,
    /// `BlockNumber nDataPages`.
    pub nDataPages: BlockNumber,
    /// `int64 nEntries`.
    pub nEntries: i64,
    /// `int32 ginVersion` — GIN on-disk version (currently 2).
    pub ginVersion: i32,
}

/// `GIN_CURRENT_VERSION` (ginblock.h).
pub const GIN_CURRENT_VERSION: i32 = 2;

// ===========================================================================
// access/gin.h — GinStatsData (planner stats).
// ===========================================================================

/// `GinStatsData` (gin.h) — stats data for planner use.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GinStatsData {
    /// `BlockNumber nPendingPages`.
    pub nPendingPages: BlockNumber,
    /// `BlockNumber nTotalPages`.
    pub nTotalPages: BlockNumber,
    /// `BlockNumber nEntryPages`.
    pub nEntryPages: BlockNumber,
    /// `BlockNumber nDataPages`.
    pub nDataPages: BlockNumber,
    /// `int64 nEntries`.
    pub nEntries: i64,
    /// `int32 ginVersion`.
    pub ginVersion: i32,
}

// ===========================================================================
// gin_private.h — GinOptions (reloptions storage).
// ===========================================================================

/// `GinOptions` (gin_private.h) — storage type for GIN's reloptions, stored as a
/// `bytea` in `rd_options`. `#[repr(C)]` with the `vl_len_` varlena header first,
/// so `core::mem::offset_of!` of the option fields matches the C struct layout
/// `build_reloptions` writes into.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GinOptions {
    /// `int32 vl_len_` — varlena header (do not touch directly).
    pub vl_len_: i32,
    /// `bool useFastUpdate` — use fast updates?
    pub useFastUpdate: bool,
    /// `int pendingListCleanupSize` — maximum size of the pending list.
    pub pendingListCleanupSize: i32,
}

// ===========================================================================
// gin_private.h — GinState (per-index working data).
// ===========================================================================

/// `GinState` (gin_private.h) — working data structure describing the index
/// being worked on: the index's tuple descriptors plus the opclass support
/// functions and collations per index column.
///
/// In C the `[INDEX_MAX_KEYS]` arrays are fixed-size; here each becomes a
/// [`Vec`] (one slot per index attribute, populated by `initGinState`). The
/// audited `gin-core-probe` consistent-fn selection reads `consistentFn[i]`,
/// `triConsistentFn[i]` (by `fn_oid`) and `supportCollation[i]`.
pub struct GinState<'mcx> {
    /// `Relation index` — the index relation Oid (`InvalidOid` when absent).
    pub index: Oid,
    /// `bool oneCol` — true if single-column index.
    pub oneCol: bool,

    /// `TupleDesc origTupdesc` — the nominal tuple descriptor of the index.
    pub origTupdesc: TupleDesc<'mcx>,
    /// `TupleDesc tupdesc[INDEX_MAX_KEYS]` — actual leaf rowtype per column.
    pub tupdesc: Vec<TupleDesc<'mcx>>,

    /// `FmgrInfo compareFn[INDEX_MAX_KEYS]`.
    pub compareFn: Vec<FmgrInfo>,
    /// `FmgrInfo extractValueFn[INDEX_MAX_KEYS]`.
    pub extractValueFn: Vec<FmgrInfo>,
    /// `FmgrInfo extractQueryFn[INDEX_MAX_KEYS]`.
    pub extractQueryFn: Vec<FmgrInfo>,
    /// `FmgrInfo consistentFn[INDEX_MAX_KEYS]`.
    pub consistentFn: Vec<FmgrInfo>,
    /// `FmgrInfo triConsistentFn[INDEX_MAX_KEYS]`.
    pub triConsistentFn: Vec<FmgrInfo>,
    /// `FmgrInfo comparePartialFn[INDEX_MAX_KEYS]` — optional method.
    pub comparePartialFn: Vec<FmgrInfo>,
    /// `bool canPartialMatch[INDEX_MAX_KEYS]` — true if `comparePartialFn[i]` is
    /// valid.
    pub canPartialMatch: Vec<bool>,
    /// `Oid supportCollation[INDEX_MAX_KEYS]` — collations passed to support fns.
    pub supportCollation: Vec<Oid>,
}

impl Default for GinState<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'mcx> GinState<'mcx> {
    /// A `GinState` with `INDEX_MAX_KEYS` empty per-attribute slots and no
    /// descriptors, mirroring a freshly `palloc0`'d `GinState` before
    /// `initGinState` fills it.
    pub fn new() -> Self {
        let n = INDEX_MAX_KEYS as usize;
        GinState {
            index: InvalidOid,
            oneCol: false,
            origTupdesc: None,
            tupdesc: (0..n).map(|_| None).collect(),
            compareFn: alloc::vec![FmgrInfo::empty(); n],
            extractValueFn: alloc::vec![FmgrInfo::empty(); n],
            extractQueryFn: alloc::vec![FmgrInfo::empty(); n],
            consistentFn: alloc::vec![FmgrInfo::empty(); n],
            triConsistentFn: alloc::vec![FmgrInfo::empty(); n],
            comparePartialFn: alloc::vec![FmgrInfo::empty(); n],
            canPartialMatch: alloc::vec![false; n],
            supportCollation: alloc::vec![InvalidOid; n],
        }
    }
}

// ===========================================================================
// gin_private.h — consistent-fn dispatch model (ginlogic.c).
// ===========================================================================

/// Which boolean-consistent implementation `ginInitConsistentFunction` selected
/// for a scan key — the C function pointer `key->boolConsistentFn`, reproduced
/// as an explicit dispatch tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinBoolConsistentKind {
    /// `trueConsistentFn` — the EVERYTHING-key dummy.
    True,
    /// `directBoolConsistentFn` — the opclass provides a boolean consistent fn.
    Direct,
    /// `shimBoolConsistentFn` — emulate boolean via the ternary fn.
    Shim,
}

/// Which ternary-consistent implementation `ginInitConsistentFunction` selected
/// for a scan key — the C function pointer `key->triConsistentFn`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinTriConsistentKind {
    /// `trueTriConsistentFn` — the EVERYTHING-key dummy.
    True,
    /// `directTriConsistentFn` — the opclass provides a ternary consistent fn.
    Direct,
    /// `shimTriConsistentFn` — emulate ternary via the boolean fn.
    Shim,
}

// ===========================================================================
// gin_private.h — GinScanKeyData / GinScanEntryData.
// ===========================================================================

/// `GinScanKeyData` (gin_private.h) — a single GIN index qualifier expression.
///
/// This is the full faithful struct (`access/gin/ginscan.c` builds it via
/// `ginFillScanKey`/`ginNewScanKey`; `ginlogic.c`'s
/// `ginInitConsistentFunction` then selects the consistent-fn dispatch tags on
/// the *same* struct, and `ginget.c` reads `entryRes`/`scanEntry`/`curItem`).
///
/// Pointer-typed C fields are re-homed onto the owned model:
/// - `boolConsistentFn`/`triConsistentFn` (C fn pointers) become the
///   [`GinBoolConsistentKind`]/[`GinTriConsistentKind`] dispatch tags assigned
///   by `ginInitConsistentFunction`.
/// - `consistentFmgrInfo`/`triConsistentFmgrInfo` (the `FmgrInfo *` the key
///   points at in the owning `GinState`) are modeled as the support-function
///   OIDs the fmgr consistent-call seam needs.
/// - `scanEntry`/`requiredEntries`/`additionalEntries` (arrays of
///   `GinScanEntry` pointers into the `GinScanOpaqueData.entries` pool) are
///   reified as [`Vec`]s of indices into that pool.
/// - `query`/`queryValues`/`extra_data` become owned [`Datum`]/byte buffers.
#[derive(Clone, Debug)]
pub struct GinScanKey<'mcx> {
    /// `uint32 nentries` — real number of entries in `scanEntry[]` (always > 0).
    pub nentries: u32,
    /// `uint32 nuserentries` — entries `extractQueryFn`/`consistentFn` know of.
    pub nuserentries: u32,

    /// `GinScanEntry *scanEntry` — one index per extracted search condition,
    /// pointing into `GinScanOpaqueData.entries`.
    pub scanEntry: Vec<u32>,

    /// `GinScanEntry *requiredEntries` — entries at least one of which must be
    /// present for a tuple to match (indices into the entries pool).
    pub requiredEntries: Vec<u32>,
    /// `int nrequired`.
    pub nrequired: i32,
    /// `GinScanEntry *additionalEntries` — entries needed by the consistent fn
    /// but not sufficient on their own (indices into the entries pool).
    pub additionalEntries: Vec<u32>,
    /// `int nadditional`.
    pub nadditional: i32,

    /// `GinTernaryValue *entryRes` — array of check flags reported to the
    /// consistent fn (one per scan entry).
    pub entryRes: Vec<GinTernaryValue>,
    /// `bool (*boolConsistentFn)(GinScanKey)` — selected boolean implementation.
    pub boolConsistentFn: GinBoolConsistentKind,
    /// `GinTernaryValue (*triConsistentFn)(GinScanKey)` — selected ternary impl.
    pub triConsistentFn: GinTriConsistentKind,
    /// `consistentFmgrInfo->fn_oid` — opclass boolean consistent fn OID.
    pub consistent_fmgr_oid: Oid,
    /// `triConsistentFmgrInfo->fn_oid` — opclass ternary consistent fn OID.
    pub tri_consistent_fmgr_oid: Oid,
    /// `Oid collation` — the collation to pass when calling the support fn.
    pub collation: Oid,

    /// `Datum query` — the original query datum.
    pub query: Datum<'mcx>,
    /// `Datum *queryValues` — extracted query keys (only `nuserentries` long).
    pub queryValues: Vec<Datum<'mcx>>,
    /// `GinNullCategory *queryCategories` (only `nuserentries` long).
    pub queryCategories: Vec<GinNullCategory>,
    /// `Pointer *extra_data` — per-entry opclass-private data (only
    /// `nuserentries` long; `None` where the opclass returned NULL).
    pub extra_data: Vec<Option<Vec<u8>>>,
    /// `StrategyNumber strategy`.
    pub strategy: StrategyNumber,
    /// `int32 searchMode`.
    pub searchMode: i32,
    /// `OffsetNumber attnum` — the index attribute number (1-based).
    pub attnum: OffsetNumber,

    /// `bool excludeOnly` — a key that can filter but not enumerate matches.
    pub excludeOnly: bool,

    /// `ItemPointerData curItem` — the TID most recently tested.
    pub curItem: ItemPointerData,
    /// `bool curItemMatches` — `curItem` passes the consistent test.
    pub curItemMatches: bool,
    /// `bool recheckCurItem` — the recheck flag the consistent fn sets.
    pub recheckCurItem: bool,
    /// `bool isFinished` — all input entry streams are finished.
    pub isFinished: bool,
}

impl<'mcx> GinScanKey<'mcx> {
    /// Construct a scan key carrying the given `entryRes` array, with everything
    /// else at the defaults `ginFillScanKey`/`ginInitConsistentFunction` later
    /// overwrite. `nentries`/`nuserentries` are `entryRes.len()`, matching how
    /// the C code allocates `entryRes` with one slot per scan entry. Provided
    /// for the consistent-fn logic tests, which exercise the key independently
    /// of the not-yet-ported `ginscan` allocation path.
    pub fn from_entry_res(entry_res: Vec<GinTernaryValue>) -> Self {
        let nentries = entry_res.len() as u32;
        GinScanKey {
            nentries,
            nuserentries: nentries,
            scanEntry: Vec::new(),
            requiredEntries: Vec::new(),
            nrequired: 0,
            additionalEntries: Vec::new(),
            nadditional: 0,
            entryRes: entry_res,
            boolConsistentFn: GinBoolConsistentKind::Shim,
            triConsistentFn: GinTriConsistentKind::Shim,
            consistent_fmgr_oid: InvalidOid,
            tri_consistent_fmgr_oid: InvalidOid,
            collation: InvalidOid,
            query: Datum::default(),
            queryValues: Vec::new(),
            queryCategories: Vec::new(),
            extra_data: Vec::new(),
            strategy: 0,
            searchMode: 0,
            attnum: 1,
            excludeOnly: false,
            curItem: ItemPointerData::default(),
            curItemMatches: false,
            recheckCurItem: false,
            isFinished: false,
        }
    }
}

/// `TBMIterateResult` (`nodes/tidbitmap.h`) — the public result of one bitmap
/// iteration step, embedded by value in [`GinScanEntryData::matchResult`]. The
/// `void *internal_page` (a private `tidbitmap.c` `PagetableEntry`) is kept as
/// an opaque slot — its layout belongs to `tidbitmap.c`, and a GIN scan only
/// passes it back to `tbm_extract_page_tuple`.
#[derive(Default)]
pub struct TBMIterateResult {
    /// `BlockNumber blockno` — block number containing tuples from the bitmap.
    pub blockno: BlockNumber,
    /// `bool lossy`.
    pub lossy: bool,
    /// `bool recheck` — whether the tuples should be rechecked.
    pub recheck: bool,
    /// `void *internal_page` — opaque per-page bitmap handed back to
    /// `tbm_extract_page_tuple`; `None` for a lossy page (the C NULL).
    pub internal_page: Option<alloc::boxed::Box<dyn core::any::Any>>,
}

impl core::fmt::Debug for TBMIterateResult {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TBMIterateResult")
            .field("blockno", &self.blockno)
            .field("lossy", &self.lossy)
            .field("recheck", &self.recheck)
            .field("internal_page", &self.internal_page.is_some())
            .finish()
    }
}

/// `TBM_MAX_TUPLES_PER_PAGE` (`nodes/tidbitmap.h`): `MaxHeapTuplesPerPage`.
pub const TBM_MAX_TUPLES_PER_PAGE: usize = types_storage::bufpage::MaxHeapTuplesPerPage;

/// `GinScanEntryData` (gin_private.h) — one specific index search condition
/// extracted from a qual. Multiple `GinScanKey.scanEntry` indices may refer to
/// the same entry (deduplicated by `ginscan.c`).
#[derive(Debug)]
pub struct GinScanEntryData<'mcx> {
    /// `Datum queryKey` — query key from `extractQueryFn`.
    pub queryKey: Datum<'mcx>,
    /// `GinNullCategory queryCategory`.
    pub queryCategory: GinNullCategory,
    /// `bool isPartialMatch`.
    pub isPartialMatch: bool,
    /// `Pointer extra_data` — opclass-private data (`None` = C NULL).
    pub extra_data: Option<Vec<u8>>,
    /// `StrategyNumber strategy`.
    pub strategy: StrategyNumber,
    /// `int32 searchMode`.
    pub searchMode: i32,
    /// `OffsetNumber attnum`.
    pub attnum: OffsetNumber,

    /// `Buffer buffer` — current page in the posting tree.
    pub buffer: Buffer,

    /// `ItemPointerData curItem` — current ItemPointer to heap.
    pub curItem: ItemPointerData,

    /// `TIDBitmap *matchBitmap` — accumulates all TIDs for a partial-match or
    /// full-scan query.
    pub matchBitmap: Option<TIDBitmap>,
    /// `TBMPrivateIterator *matchIterator`.
    pub matchIterator: Option<TBMPrivateIterator>,

    /// `TBMIterateResult matchResult` — meaningless when its `blockno` is
    /// `InvalidBlockNumber`.
    pub matchResult: TBMIterateResult,
    /// `OffsetNumber matchOffsets[TBM_MAX_TUPLES_PER_PAGE]`.
    pub matchOffsets: Vec<OffsetNumber>,
    /// `int matchNtuples`.
    pub matchNtuples: i32,

    /// `ItemPointerData *list` — posting list / one posting-tree page.
    pub list: Vec<ItemPointerData>,
    /// `int nlist`.
    pub nlist: i32,
    /// `OffsetNumber offset`.
    pub offset: OffsetNumber,

    /// `bool isFinished`.
    pub isFinished: bool,
    /// `bool reduceResult`.
    pub reduceResult: bool,
    /// `uint32 predictNumberResult`.
    pub predictNumberResult: u32,
    /// `GinBtreeData btree`.
    pub btree: GinBtreeData<'mcx>,
}

// ===========================================================================
// gin_private.h — ginbtree.c abstract btree.
// ===========================================================================

/// `GinBtreeStack` (gin_private.h) — a stack of pages visited while descending
/// the entry/data btree.
#[derive(Clone, Debug, Default)]
pub struct GinBtreeStack {
    /// `BlockNumber blkno`.
    pub blkno: BlockNumber,
    /// `Buffer buffer`.
    pub buffer: Buffer,
    /// `OffsetNumber off`.
    pub off: OffsetNumber,
    /// `ItemPointerData iptr`.
    pub iptr: ItemPointerData,
    /// `uint32 predictNumber` — predicted number of pages on the current level.
    pub predictNumber: u32,
    /// `struct GinBtreeStack *parent`.
    pub parent: Option<alloc::boxed::Box<GinBtreeStack>>,
}

/// `GinPlaceToPageRC` (gin_private.h) — return codes for
/// `GinBtreeData.beginPlaceToPage`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinPlaceToPageRC {
    GPTP_NO_WORK,
    GPTP_INSERT,
    GPTP_SPLIT,
}

/// The `void *insertdata` argument the GIN btree descent threads through
/// `ginPlaceToPage` / `beginPlaceToPage` / `execPlaceToPage`, interpreted per
/// tree-kind exactly as in C, where the same `void *` is cast to one of:
/// - a `GinBtreeEntryInsertData *` for the entry tree (`ginentrypage.c`),
/// - a `GinBtreeDataLeafInsertData *` for a data (posting tree) leaf page, or
/// - a `PostingItem *` for an internal data page (`gindatapage.c`).
///
/// `ginbtree.c`'s spine never inspects the payload — it passes it straight
/// through to the (L3) vtable callbacks; this enum is the owned stand-in for the
/// untyped `void *` so the spine stays type-safe.
#[derive(Clone, Debug)]
pub enum GinInsertPayload<'mcx> {
    /// `GinBtreeEntryInsertData *` — entry-tree insert.
    Entry(GinBtreeEntryInsertData<'mcx>),
    /// `GinBtreeDataLeafInsertData *` — data-leaf insert.
    DataLeaf(GinBtreeDataLeafInsertData),
    /// `PostingItem *` — internal data-page downlink.
    DataInternal(PostingItem),
}

/// `*ptp_workspace` — the opaque `void *` workspace `beginPlaceToPage` produces
/// for `execPlaceToPage` (the computed page-edit plan). The concrete payload is
/// `gindatapage.c` / `ginentrypage.c` private (`disassembledLeaf`, the leaf
/// segment list, etc.); the spine only carries it between the two callbacks, so
/// it is modelled as an opaque erased box exactly like C's `void *`.
#[derive(Default)]
pub struct PtpWorkspace {
    /// `*ptp_workspace` (`None` == the C `NULL`, i.e. `GPTP_NO_WORK`).
    pub inner: Option<alloc::boxed::Box<dyn core::any::Any>>,
}

impl core::fmt::Debug for PtpWorkspace {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PtpWorkspace")
            .field("inner", &self.inner.is_some())
            .finish()
    }
}

/// The out-parameters `beginPlaceToPage` fills (the C `void **ptp_workspace`,
/// `Page *newlpage`, `Page *newrpage`). On `GPTP_SPLIT`, `newlpage`/`newrpage`
/// hold palloc'd page images (modelled as `BLCKSZ`-sized byte buffers) not
/// associated with buffers; otherwise they are `None`.
#[derive(Default)]
pub struct BeginPlaceToPageResult {
    /// The `GinPlaceToPageRC` return code.
    pub rc: GinPlaceToPageRC,
    /// `*ptp_workspace`.
    pub ptp_workspace: PtpWorkspace,
    /// `*newlpage` — palloc'd left split page (`GPTP_SPLIT` only).
    pub newlpage: Option<Vec<u8>>,
    /// `*newrpage` — palloc'd right split page (`GPTP_SPLIT` only).
    pub newrpage: Option<Vec<u8>>,
}

impl Default for GinPlaceToPageRC {
    fn default() -> Self {
        GinPlaceToPageRC::GPTP_NO_WORK
    }
}

impl core::fmt::Debug for BeginPlaceToPageResult {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BeginPlaceToPageResult")
            .field("rc", &self.rc)
            .field("newlpage", &self.newlpage.is_some())
            .field("newrpage", &self.newrpage.is_some())
            .finish_non_exhaustive()
    }
}

/// `GinBtreeData` (gin_private.h) — the abstract entry-tree / data-tree (posting
/// tree) btree. The method-table function pointers are filled by
/// `ginentrypage.c` (entry tree) and `gindatapage.c` (data tree); that opacity
/// is inherited from C and kept as `Option<fn ...>` slots (the eventual L3 port
/// installs them). The page bytes the callbacks operate on are reached through
/// the bufmgr seam, so the callbacks are keyed on the live `Buffer` rather than a
/// raw `Page` pointer (the C `Page` argument is `BufferGetPage(buffer)`), and the
/// C out-parameters become return values.
pub struct GinBtreeData<'mcx> {
    // search methods
    /// `BlockNumber (*findChildPage)(GinBtree, GinBtreeStack *)`.
    pub findChildPage:
        Option<fn(&mut GinBtreeData<'mcx>, &mut GinBtreeStack) -> PgResult<BlockNumber>>,
    /// `BlockNumber (*getLeftMostChild)(GinBtree, Page)` — the `Page` is
    /// `BufferGetPage(buffer)`.
    pub getLeftMostChild: Option<fn(&mut GinBtreeData<'mcx>, Buffer) -> PgResult<BlockNumber>>,
    /// `bool (*isMoveRight)(GinBtree, Page)`.
    pub isMoveRight: Option<fn(&mut GinBtreeData<'mcx>, Buffer) -> PgResult<bool>>,
    /// `bool (*findItem)(GinBtree, GinBtreeStack *)`.
    pub findItem: Option<fn(&mut GinBtreeData<'mcx>, &mut GinBtreeStack) -> PgResult<bool>>,

    // insert methods
    /// `OffsetNumber (*findChildPtr)(GinBtree, Page, BlockNumber, OffsetNumber)`.
    pub findChildPtr: Option<
        fn(&mut GinBtreeData<'mcx>, Buffer, BlockNumber, OffsetNumber) -> PgResult<OffsetNumber>,
    >,
    /// `GinPlaceToPageRC (*beginPlaceToPage)(GinBtree, Buffer, GinBtreeStack *,
    /// void *insertdata, BlockNumber updateblkno, void **ptp_workspace, Page
    /// *newlpage, Page *newrpage)`. The three out-params become the
    /// [`BeginPlaceToPageResult`] return value.
    pub beginPlaceToPage: Option<
        fn(
            &mut GinBtreeData<'mcx>,
            Mcx<'mcx>,
            Buffer,
            &mut GinBtreeStack,
            &GinInsertPayload<'mcx>,
            BlockNumber,
        ) -> PgResult<BeginPlaceToPageResult>,
    >,
    /// `void (*execPlaceToPage)(GinBtree, Buffer, GinBtreeStack *, void
    /// *insertdata, BlockNumber updateblkno, void *ptp_workspace)`.
    pub execPlaceToPage: Option<
        fn(
            &mut GinBtreeData<'mcx>,
            Mcx<'mcx>,
            Buffer,
            &mut GinBtreeStack,
            &GinInsertPayload<'mcx>,
            BlockNumber,
            &mut PtpWorkspace,
        ) -> PgResult<()>,
    >,
    /// `void *(*prepareDownlink)(GinBtree, Buffer)` — returns the freshly
    /// palloc'd `insertdata` for the parent's downlink.
    pub prepareDownlink:
        Option<fn(&mut GinBtreeData<'mcx>, Mcx<'mcx>, Buffer) -> PgResult<GinInsertPayload<'mcx>>>,
    /// `void (*fillRoot)(GinBtree, Page root, BlockNumber lblkno, Page lpage,
    /// BlockNumber rblkno, Page rpage)`. The pages are palloc'd temp images
    /// (byte buffers); the root is written in place.
    pub fillRoot: Option<
        fn(
            &mut GinBtreeData<'mcx>,
            &mut [u8],
            BlockNumber,
            &[u8],
            BlockNumber,
            &[u8],
        ) -> PgResult<()>,
    >,

    /// `bool isData`.
    pub isData: bool,

    /// `Relation index` — the index relation Oid.
    pub index: Oid,
    /// `BlockNumber rootBlkno`.
    pub rootBlkno: BlockNumber,
    /// `GinState *ginstate` — not valid in a data scan; the index relation Oid
    /// the (owned) `GinState` describes.
    pub ginstate: Oid,
    /// `bool fullScan`.
    pub fullScan: bool,
    /// `bool isBuild`.
    pub isBuild: bool,

    // Search key for the entry tree.
    /// `OffsetNumber entryAttnum`.
    pub entryAttnum: OffsetNumber,
    /// `Datum entryKey`.
    pub entryKey: Datum<'mcx>,
    /// `GinNullCategory entryCategory`.
    pub entryCategory: GinNullCategory,

    // Search key for the data tree (posting tree).
    /// `ItemPointerData itemptr`.
    pub itemptr: ItemPointerData,
}

impl core::fmt::Debug for GinBtreeData<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GinBtreeData")
            .field("isData", &self.isData)
            .field("index", &self.index)
            .field("rootBlkno", &self.rootBlkno)
            .field("fullScan", &self.fullScan)
            .field("isBuild", &self.isBuild)
            .field("entryAttnum", &self.entryAttnum)
            .field("entryCategory", &self.entryCategory)
            .field("itemptr", &self.itemptr)
            .finish_non_exhaustive()
    }
}

impl Default for GinBtreeData<'_> {
    /// A zeroed `GinBtreeData`, mirroring a `palloc0`'d btree before
    /// `ginPrepareEntryScan`/`ginPrepareDataScan` install the method table.
    fn default() -> Self {
        GinBtreeData {
            findChildPage: None,
            getLeftMostChild: None,
            isMoveRight: None,
            findItem: None,
            findChildPtr: None,
            beginPlaceToPage: None,
            execPlaceToPage: None,
            prepareDownlink: None,
            fillRoot: None,
            isData: false,
            index: InvalidOid,
            rootBlkno: 0,
            ginstate: InvalidOid,
            fullScan: false,
            isBuild: false,
            entryAttnum: 0,
            entryKey: Datum::default(),
            entryCategory: GIN_CAT_NORM_KEY,
            itemptr: ItemPointerData::default(),
        }
    }
}

/// `GinBtreeEntryInsertData` (gin_private.h) — a tuple to be inserted into the
/// entry tree.
#[derive(Clone, Debug)]
pub struct GinBtreeEntryInsertData<'mcx> {
    /// `IndexTuple entry` — tuple to insert.
    pub entry: types_tuple::heaptuple::IndexTuple<'mcx>,
    /// `bool isDelete` — delete the old tuple at the same offset?
    pub isDelete: bool,
}

/// `GinBtreeDataLeafInsertData` (gin_private.h) — itempointer(s) to be inserted
/// into a data (posting tree) leaf page.
#[derive(Clone, Debug, Default)]
pub struct GinBtreeDataLeafInsertData {
    /// `ItemPointerData *items`.
    pub items: Vec<ItemPointerData>,
    /// `uint32 nitem`.
    pub nitem: u32,
    /// `uint32 curitem`.
    pub curitem: u32,
}

// ===========================================================================
// gin_private.h — GinScanOpaqueData.
// ===========================================================================

/// `GinScanOpaqueData` (gin_private.h) — the GIN scan's private state, hung off
/// `IndexScanDescData.opaque`. Built by `ginbeginscan` and filled by
/// `ginNewScanKey` (both in the not-yet-ported `ginscan.c`). The pointer arrays
/// in `GinScanKey` index into this struct's `entries` pool.
pub struct GinScanOpaqueData<'mcx> {
    /// `MemoryContext tempCtx` — short-term per-tuple context.
    pub tempCtx: Mcx<'mcx>,
    /// `GinState ginstate`.
    pub ginstate: GinState<'mcx>,

    /// `GinScanKey keys` — one per scan qualifier expression.
    pub keys: Vec<GinScanKey<'mcx>>,
    /// `uint32 nkeys`.
    pub nkeys: u32,

    /// `GinScanEntry *entries` — one per index search condition (the pool the
    /// `GinScanKey` pointer arrays index into).
    pub entries: Vec<GinScanEntryData<'mcx>>,
    /// `uint32 totalentries`.
    pub totalentries: u32,
    /// `uint32 allocentries` — allocated length of `entries[]`.
    pub allocentries: u32,

    /// `MemoryContext keyCtx` — holds key and entry data.
    pub keyCtx: Mcx<'mcx>,

    /// `bool isVoidRes` — true if the query is unsatisfiable.
    pub isVoidRes: bool,
}
