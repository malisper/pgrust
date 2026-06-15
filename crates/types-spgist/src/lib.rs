//! Runtime SP-GiST access-method vocabulary (`access/spgist.h`,
//! `access/spgist_private.h`) on the real owned value model.
//!
//! This crate is the single owner of the SP-GiST type vocabulary, mirroring the
//! role `types-gin` plays for GIN and `types-brin` plays for BRIN. The C structs
//! are translated field-for-field and re-homed onto the owned memory model:
//!
//! * `Datum` becomes the owned [`types_tuple::Datum`] value;
//! * `MemoryContext` becomes [`mcx::Mcx`];
//! * `Relation` becomes the index relation `Oid` (the page bytes are reached
//!   through the bufmgr seam, exactly as `types-gin`/`types-gist` do);
//! * `TupleDesc` becomes the owned [`types_tuple::heaptuple::TupleDesc`];
//! * `ScanKey` (a `ScanKeyData *` array) becomes a [`Vec`] of the owned
//!   [`types_scan::scankey::ScanKeyData`];
//! * `Datum *nodeLabels` / `Datum *datums` and the various `int *` / `double **`
//!   working arrays become [`Vec`]s;
//! * `void *traversalValue` (opclass-specific opaque traverse state) becomes an
//!   owned byte buffer `Option<Vec<u8>>` — the honest owned analog of the C
//!   `void *`, which the opclass packs/unpacks at the typed dispatch seam.
//!
//! The on-disk page structures (`SpGistPageOpaqueData`, `SpGistMetaPageData`,
//! `SpGistInnerTupleData`, `SpGistLeafTupleData`, `SpGistDeadTupleData`) are kept
//! as `#[repr(C)]` with compile-time layout assertions so the byte layout the
//! bufmgr lane reads/writes is exact.
//!
//! No SP-GiST *logic* lives here — only the vocabulary. The SP-GiST AM crates
//! (`spgutils`/`spgdoinsert`/`spgscan`/the opclass support crates) build and
//! consume these values, and install their typed support-proc dispatch bodies
//! (config/choose/picksplit/inner_consistent/leaf_consistent) into the SP-GiST
//! core's per-AM typed seams, keyed on their support-proc OIDs — exactly the
//! BRIN opclass-dispatch idiom, *not* a generic fmgr-by-pointer path.

#![no_std]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{BlockNumber, Oid, TransactionId};
use types_scan::scankey::ScanKeyData;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, TupleDesc};

// ===========================================================================
// access/spgist.h — opclass support function numbers.
// ===========================================================================

/// `SPGIST_CONFIG_PROC` — opclass `config` support function (spgist.h).
pub const SPGIST_CONFIG_PROC: i32 = 1;
/// `SPGIST_CHOOSE_PROC` — opclass `choose` support function (spgist.h).
pub const SPGIST_CHOOSE_PROC: i32 = 2;
/// `SPGIST_PICKSPLIT_PROC` — opclass `picksplit` support function (spgist.h).
pub const SPGIST_PICKSPLIT_PROC: i32 = 3;
/// `SPGIST_INNER_CONSISTENT_PROC` — opclass `inner_consistent` (spgist.h).
pub const SPGIST_INNER_CONSISTENT_PROC: i32 = 4;
/// `SPGIST_LEAF_CONSISTENT_PROC` — opclass `leaf_consistent` (spgist.h).
pub const SPGIST_LEAF_CONSISTENT_PROC: i32 = 5;
/// `SPGIST_COMPRESS_PROC` — opclass `compress` support function (spgist.h).
pub const SPGIST_COMPRESS_PROC: i32 = 6;
/// `SPGIST_OPTIONS_PROC` — opclass `options` support function (spgist.h).
pub const SPGIST_OPTIONS_PROC: i32 = 7;
/// `SPGISTNRequiredProc` — number of always-required support functions.
pub const SPGISTNRequiredProc: i32 = 5;
/// `SPGISTNProc` — total number of support functions.
pub const SPGISTNProc: i32 = 7;

// ===========================================================================
// access/spgist.h — argument structs for the opclass support methods.
//
// These cross the typed support-proc dispatch seam by value; pointer/array
// members become owned `Vec`s, `Datum` becomes the owned value, and the opclass
// `void *traversalValue` becomes an owned byte buffer.
// ===========================================================================

/// `spgConfigIn` — input to the `spg_config` method (spgist.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct spgConfigIn {
    /// Data type to be indexed.
    pub attType: Oid,
}

/// `spgConfigOut` — output of the `spg_config` method (spgist.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct spgConfigOut {
    /// Data type of inner-tuple prefixes.
    pub prefixType: Oid,
    /// Data type of inner-tuple node labels.
    pub labelType: Oid,
    /// Data type of leaf-tuple values.
    pub leafType: Oid,
    /// Opclass can reconstruct original data.
    pub canReturnData: bool,
    /// Opclass can cope with values > 1 page.
    pub longValuesOK: bool,
}

/// `spgChooseIn` — input to the `spg_choose` method (spgist.h).
#[derive(Clone, Debug)]
pub struct spgChooseIn<'mcx> {
    /// Original datum to be indexed.
    pub datum: Datum<'mcx>,
    /// Current datum to be stored at leaf.
    pub leafDatum: Datum<'mcx>,
    /// Current level (counting from zero).
    pub level: i32,

    /// Tuple is marked all-the-same?
    pub allTheSame: bool,
    /// Tuple has a prefix?
    pub hasPrefix: bool,
    /// If so, the prefix value.
    pub prefixDatum: Datum<'mcx>,
    /// Number of nodes in the inner tuple.
    pub nNodes: i32,
    /// Node label values (`None` if none).
    pub nodeLabels: Option<Vec<Datum<'mcx>>>,
}

/// `spgChooseResultType` — action code for `spgChooseOut` (spgist.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum spgChooseResultType {
    /// Descend into existing node.
    spgMatchNode = 1,
    /// Add a node to the inner tuple.
    spgAddNode = 2,
    /// Split inner tuple (change its prefix).
    spgSplitTuple = 3,
}

/// Results for `spgMatchNode` (the `matchNode` arm of `spgChooseOut.result`).
#[derive(Clone, Debug)]
pub struct spgChooseOutMatchNode<'mcx> {
    /// Descend to this node (index from 0).
    pub nodeN: i32,
    /// Increment level by this much.
    pub levelAdd: i32,
    /// New leaf datum.
    pub restDatum: Datum<'mcx>,
}

/// Results for `spgAddNode` (the `addNode` arm of `spgChooseOut.result`).
#[derive(Clone, Debug)]
pub struct spgChooseOutAddNode<'mcx> {
    /// New node's label.
    pub nodeLabel: Datum<'mcx>,
    /// Where to insert it (index from 0).
    pub nodeN: i32,
}

/// Results for `spgSplitTuple` (the `splitTuple` arm of `spgChooseOut.result`).
#[derive(Clone, Debug)]
pub struct spgChooseOutSplitTuple<'mcx> {
    /// New upper-level inner tuple should have a prefix?
    pub prefixHasPrefix: bool,
    /// If so, its value.
    pub prefixPrefixDatum: Datum<'mcx>,
    /// Number of nodes.
    pub prefixNNodes: i32,
    /// Their labels (or `None` for no labels).
    pub prefixNodeLabels: Option<Vec<Datum<'mcx>>>,
    /// Which node gets child tuple.
    pub childNodeN: i32,

    /// New lower-level inner tuple should have a prefix?
    pub postfixHasPrefix: bool,
    /// If so, its value.
    pub postfixPrefixDatum: Datum<'mcx>,
}

/// `spgChooseOut.result` — the tagged union of per-action results (spgist.h).
#[derive(Clone, Debug)]
pub enum spgChooseOutResult<'mcx> {
    /// `out->result.matchNode`.
    MatchNode(spgChooseOutMatchNode<'mcx>),
    /// `out->result.addNode`.
    AddNode(spgChooseOutAddNode<'mcx>),
    /// `out->result.splitTuple`.
    SplitTuple(spgChooseOutSplitTuple<'mcx>),
}

/// `spgChooseOut` — output of the `spg_choose` method (spgist.h).
///
/// The C struct stores `resultType` plus a union; modelled here as a single
/// tagged enum whose discriminant *is* the action code (the `resultType` field
/// is recovered via [`spgChooseOut::resultType`]).
#[derive(Clone, Debug)]
pub struct spgChooseOut<'mcx> {
    /// Action code plus its associated result payload.
    pub result: spgChooseOutResult<'mcx>,
}

impl<'mcx> spgChooseOut<'mcx> {
    /// `out->resultType` — the action code implied by the result payload.
    #[inline]
    pub fn resultType(&self) -> spgChooseResultType {
        match self.result {
            spgChooseOutResult::MatchNode(_) => spgChooseResultType::spgMatchNode,
            spgChooseOutResult::AddNode(_) => spgChooseResultType::spgAddNode,
            spgChooseOutResult::SplitTuple(_) => spgChooseResultType::spgSplitTuple,
        }
    }
}

/// `spgPickSplitIn` — input to the `spg_picksplit` method (spgist.h).
#[derive(Clone, Debug)]
pub struct spgPickSplitIn<'mcx> {
    /// Their datums (array of length `nTuples`).
    pub datums: Vec<Datum<'mcx>>,
    /// Current level (counting from zero).
    pub level: i32,
}

impl<'mcx> spgPickSplitIn<'mcx> {
    /// `in->nTuples` — number of leaf tuples.
    #[inline]
    pub fn nTuples(&self) -> i32 {
        self.datums.len() as i32
    }
}

/// `spgPickSplitOut` — output of the `spg_picksplit` method (spgist.h).
#[derive(Clone, Debug, Default)]
pub struct spgPickSplitOut<'mcx> {
    /// New inner tuple should have a prefix?
    pub hasPrefix: bool,
    /// If so, its value.
    pub prefixDatum: Option<Datum<'mcx>>,

    /// Number of nodes for new inner tuple.
    pub nNodes: i32,
    /// Their labels (or `None` for no labels).
    pub nodeLabels: Option<Vec<Datum<'mcx>>>,

    /// Node index for each leaf tuple.
    pub mapTuplesToNodes: Vec<i32>,
    /// Datum to store in each new leaf tuple.
    pub leafTupleDatums: Vec<Datum<'mcx>>,
}

/// `spgInnerConsistentIn` — input to `spg_inner_consistent` (spgist.h).
#[derive(Clone, Debug)]
pub struct spgInnerConsistentIn<'mcx> {
    /// Array of operators and comparison values.
    pub scankeys: Vec<ScanKeyData<'mcx>>,
    /// Array of ordering operators and comparison values.
    pub orderbys: Vec<ScanKeyData<'mcx>>,

    /// Value reconstructed at parent.
    pub reconstructedValue: Datum<'mcx>,
    /// Opclass-specific traverse value (`void *`), owned byte buffer.
    pub traversalValue: Option<Vec<u8>>,
    /// Current level (counting from zero).
    pub level: i32,
    /// Original data must be returned?
    pub returnData: bool,

    /// Tuple is marked all-the-same?
    pub allTheSame: bool,
    /// Tuple has a prefix?
    pub hasPrefix: bool,
    /// If so, the prefix value.
    pub prefixDatum: Datum<'mcx>,
    /// Number of nodes in the inner tuple.
    pub nNodes: i32,
    /// Node label values (`None` if none).
    pub nodeLabels: Option<Vec<Datum<'mcx>>>,
}

impl<'mcx> spgInnerConsistentIn<'mcx> {
    /// `in->nkeys` — length of `scankeys`.
    #[inline]
    pub fn nkeys(&self) -> i32 {
        self.scankeys.len() as i32
    }
    /// `in->norderbys` — length of `orderbys`.
    #[inline]
    pub fn norderbys(&self) -> i32 {
        self.orderbys.len() as i32
    }
}

/// `spgInnerConsistentOut` — output of `spg_inner_consistent` (spgist.h).
#[derive(Clone, Debug, Default)]
pub struct spgInnerConsistentOut<'mcx> {
    /// Number of child nodes to be visited.
    pub nNodes: i32,
    /// Their indexes in the node array.
    pub nodeNumbers: Vec<i32>,
    /// Increment level by this much for each.
    pub levelAdds: Vec<i32>,
    /// Associated reconstructed values.
    pub reconstructedValues: Vec<Datum<'mcx>>,
    /// Opclass-specific traverse values (one owned byte buffer per node).
    pub traversalValues: Vec<Option<Vec<u8>>>,
    /// Associated distances (one distance array per node).
    pub distances: Vec<Vec<f64>>,
}

/// `spgLeafConsistentIn` — input to `spg_leaf_consistent` (spgist.h).
#[derive(Clone, Debug)]
pub struct spgLeafConsistentIn<'mcx> {
    /// Array of operators and comparison values.
    pub scankeys: Vec<ScanKeyData<'mcx>>,
    /// Array of ordering operators and comparison values.
    pub orderbys: Vec<ScanKeyData<'mcx>>,

    /// Value reconstructed at parent.
    pub reconstructedValue: Datum<'mcx>,
    /// Opclass-specific traverse value (`void *`), owned byte buffer.
    pub traversalValue: Option<Vec<u8>>,
    /// Current level (counting from zero).
    pub level: i32,
    /// Original data must be returned?
    pub returnData: bool,

    /// Datum in leaf tuple.
    pub leafDatum: Datum<'mcx>,
}

impl<'mcx> spgLeafConsistentIn<'mcx> {
    /// `in->nkeys` — length of `scankeys`.
    #[inline]
    pub fn nkeys(&self) -> i32 {
        self.scankeys.len() as i32
    }
    /// `in->norderbys` — length of `orderbys`.
    #[inline]
    pub fn norderbys(&self) -> i32 {
        self.orderbys.len() as i32
    }
}

/// `spgLeafConsistentOut` — output of `spg_leaf_consistent` (spgist.h).
#[derive(Clone, Debug, Default)]
pub struct spgLeafConsistentOut<'mcx> {
    /// Reconstructed original data, if any.
    pub leafValue: Option<Datum<'mcx>>,
    /// Set true if operator must be rechecked.
    pub recheck: bool,
    /// Set true if distances must be rechecked.
    pub recheckDistances: bool,
    /// Associated distances.
    pub distances: Option<Vec<f64>>,
}

// ===========================================================================
// access/spgist_private.h — reloptions.
// ===========================================================================

/// `SPGIST_MIN_FILLFACTOR` (spgist_private.h).
pub const SPGIST_MIN_FILLFACTOR: i32 = 10;
/// `SPGIST_DEFAULT_FILLFACTOR` (spgist_private.h).
pub const SPGIST_DEFAULT_FILLFACTOR: i32 = 80;

/// `SpGistOptions` — reloptions parsed from `rd_options` (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistOptions {
    /// varlena header (do not touch directly!).
    pub varlena_header_: i32,
    /// page fill factor in percent (0..100).
    pub fillfactor: i32,
}

// ===========================================================================
// access/spgist_private.h — leaf-tuple key/include column indexes.
// ===========================================================================

/// `spgKeyColumn` — index of the key column in an SP-GiST leaf tuple.
pub const spgKeyColumn: i32 = 0;
/// `spgFirstIncludeColumn` — index of the first included column.
pub const spgFirstIncludeColumn: i32 = 1;

// ===========================================================================
// access/spgist_private.h — fixed-location page numbers.
// ===========================================================================

/// `SPGIST_METAPAGE_BLKNO` — the metapage (spgist_private.h).
pub const SPGIST_METAPAGE_BLKNO: BlockNumber = 0;
/// `SPGIST_ROOT_BLKNO` — root for normal entries (spgist_private.h).
pub const SPGIST_ROOT_BLKNO: BlockNumber = 1;
/// `SPGIST_NULL_BLKNO` — root for null-value entries (spgist_private.h).
pub const SPGIST_NULL_BLKNO: BlockNumber = 2;
/// `SPGIST_LAST_FIXED_BLKNO` — last fixed-location page (spgist_private.h).
pub const SPGIST_LAST_FIXED_BLKNO: BlockNumber = SPGIST_NULL_BLKNO;

/// `SpGistBlockIsRoot(blkno)` — is `blkno` one of the root pages?
#[inline]
pub const fn SpGistBlockIsRoot(blkno: BlockNumber) -> bool {
    blkno == SPGIST_ROOT_BLKNO || blkno == SPGIST_NULL_BLKNO
}

/// `SpGistBlockIsFixed(blkno)` — is `blkno` a fixed-location page?
#[inline]
pub const fn SpGistBlockIsFixed(blkno: BlockNumber) -> bool {
    blkno <= SPGIST_LAST_FIXED_BLKNO
}

// ===========================================================================
// access/spgist_private.h — page special space.
// ===========================================================================

/// `SpGistPageOpaqueData` — the special area of every SP-GiST page.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistPageOpaqueData {
    /// see flag bit definitions below.
    pub flags: u16,
    /// number of redirection tuples on page.
    pub nRedirection: u16,
    /// number of placeholder tuples on page.
    pub nPlaceholder: u16,
    /// for identification of SP-GiST indexes.
    pub spgist_page_id: u16,
}

/// Flag bit: `SPGIST_META` — page is the metapage.
pub const SPGIST_META: u16 = 1 << 0;
/// Flag bit: `SPGIST_DELETED` — never set; kept for backwards compatibility.
pub const SPGIST_DELETED: u16 = 1 << 1;
/// Flag bit: `SPGIST_LEAF` — page is a leaf page.
pub const SPGIST_LEAF: u16 = 1 << 2;
/// Flag bit: `SPGIST_NULLS` — page stores null-valued tuples.
pub const SPGIST_NULLS: u16 = 1 << 3;

/// `SPGIST_PAGE_ID` — last 2 bytes of every SP-GiST page (spgist_private.h).
pub const SPGIST_PAGE_ID: u16 = 0xFF82;

// ===========================================================================
// access/spgist_private.h — last-used-page cache.
// ===========================================================================

/// `SpGistLastUsedPage` — one last-used-page cache slot (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistLastUsedPage {
    /// block number, or `InvalidBlockNumber`.
    pub blkno: BlockNumber,
    /// page's free space (could be obsolete!).
    pub freeSpace: i32,
}

/// `SPGIST_CACHED_PAGES` — number of last-used-page cache slots.
pub const SPGIST_CACHED_PAGES: usize = 8;

/// `SpGistLUPCache` — shared storage of last-used-page info (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SpGistLUPCache {
    /// per-page-class cache slots; indexes match `SpGistGetBuffer` flags.
    pub cachedPage: [SpGistLastUsedPage; SPGIST_CACHED_PAGES],
}

impl Default for SpGistLUPCache {
    #[inline]
    fn default() -> Self {
        SpGistLUPCache {
            cachedPage: [SpGistLastUsedPage::default(); SPGIST_CACHED_PAGES],
        }
    }
}

// ===========================================================================
// access/spgist_private.h — metapage.
// ===========================================================================

/// `SpGistMetaPageData` — contents of the SP-GiST metapage (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SpGistMetaPageData {
    /// for identity cross-check.
    pub magicNumber: u32,
    /// shared storage of last-used info.
    pub lastUsedPages: SpGistLUPCache,
}

impl Default for SpGistMetaPageData {
    #[inline]
    fn default() -> Self {
        SpGistMetaPageData {
            magicNumber: 0,
            lastUsedPages: SpGistLUPCache::default(),
        }
    }
}

/// `SPGIST_MAGIC_NUMBER` — metapage identity cross-check (spgist_private.h).
pub const SPGIST_MAGIC_NUMBER: u32 = 0xBA0B_ABEE;

// ===========================================================================
// access/spgist_private.h — in-memory working state.
// ===========================================================================

/// `SpGistTypeDesc` — per-datatype info needed in `SpGistState`
/// (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistTypeDesc {
    /// `type` (the catalog type OID; `type` is a Rust keyword, so `type_`).
    pub type_: Oid,
    /// `attlen`.
    pub attlen: i16,
    /// `attbyval`.
    pub attbyval: bool,
    /// `attalign`.
    pub attalign: i8,
    /// `attstorage`.
    pub attstorage: i8,
}

/// `SpGistState` — the per-operation working state shared by insert and search
/// code (spgist_private.h).
///
/// `index` (a `Relation`) is the index relation `Oid`; `deadTupleStorage`
/// (a `char *` workspace for `spgFormDeadTuple`) is an owned byte buffer.
///
/// Not `Clone`: `leafTupDesc` is an owned [`TupleDesc`] handle (`Box`), matching
/// the no-copy ownership of the C `SpGistState`'s `leafTupDesc` pointer.
#[derive(Debug)]
pub struct SpGistState<'mcx> {
    /// Index we're working with (index relation `Oid`).
    pub index: Oid,

    /// Filled in by opclass config method.
    pub config: spgConfigOut,

    /// Type of values to be indexed/restored.
    pub attType: SpGistTypeDesc,
    /// Type of leaf-tuple values.
    pub attLeafType: SpGistTypeDesc,
    /// Type of inner-tuple prefix values.
    pub attPrefixType: SpGistTypeDesc,
    /// Type of node label values.
    pub attLabelType: SpGistTypeDesc,

    /// Descriptor for leaf-level tuples (often the index's tupdesc, not always).
    pub leafTupDesc: TupleDesc<'mcx>,

    /// Workspace for `spgFormDeadTuple` (`char *`).
    pub deadTupleStorage: Option<Vec<u8>>,

    /// XID to use when creating a redirect tuple.
    pub redirectXid: TransactionId,
    /// True if doing index build.
    pub isBuild: bool,
}

// ===========================================================================
// access/spgist_private.h — rd_amcache contents.
// ===========================================================================

/// `SpGistCache` — what we keep in `index->rd_amcache` (spgist_private.h).
///
/// Static configuration plus the local last-used-pages cache.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SpGistCache {
    /// Filled in by opclass config method.
    pub config: spgConfigOut,

    /// Type of values to be indexed/restored.
    pub attType: SpGistTypeDesc,
    /// Type of leaf-tuple values.
    pub attLeafType: SpGistTypeDesc,
    /// Type of inner-tuple prefix values.
    pub attPrefixType: SpGistTypeDesc,
    /// Type of node label values.
    pub attLabelType: SpGistTypeDesc,

    /// Local storage of last-used info.
    pub lastUsedPages: SpGistLUPCache,
}

// ===========================================================================
// access/spgist_private.h — tuple state values.
// ===========================================================================

/// `SPGIST_LIVE` — normal live tuple (either inner or leaf).
pub const SPGIST_LIVE: u32 = 0;
/// `SPGIST_REDIRECT` — temporary redirection placeholder.
pub const SPGIST_REDIRECT: u32 = 1;
/// `SPGIST_DEAD` — dead, cannot be removed because of links.
pub const SPGIST_DEAD: u32 = 2;
/// `SPGIST_PLACEHOLDER` — placeholder, used to preserve offsets.
pub const SPGIST_PLACEHOLDER: u32 = 3;

// ===========================================================================
// access/spgist_private.h — inner tuple header.
// ===========================================================================

/// `SGITMAXNNODES` — largest value that fits the `nNodes:13` bit field.
pub const SGITMAXNNODES: u32 = 0x1FFF;
/// `SGITMAXPREFIXSIZE` — largest value that fits the `prefixSize:16` field.
pub const SGITMAXPREFIXSIZE: u32 = 0xFFFF;
/// `SGITMAXSIZE` — largest value that fits the inner-tuple `size` field.
pub const SGITMAXSIZE: u32 = 0xFFFF;

/// `SpGistInnerTupleData` — on-disk inner tuple header (spgist_private.h).
///
/// The C struct packs four bit fields into one `unsigned int` word:
/// `tupstate:2, allTheSame:1, nNodes:13, prefixSize:16`. We model that word as a
/// single `u32` (`bits`) with accessors/setters so the on-disk layout is exact.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistInnerTupleData {
    /// packed bit fields: tupstate:2, allTheSame:1, nNodes:13, prefixSize:16.
    pub bits: u32,
    /// total size of inner tuple.
    pub size: u16,
    /* On most machines there will be a couple of wasted bytes here.
     * prefix datum follows, then nodes. */
}

impl SpGistInnerTupleData {
    /// `tupstate:2`.
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.bits & 0x3
    }
    /// `allTheSame:1`.
    #[inline]
    pub const fn allTheSame(&self) -> bool {
        (self.bits >> 2) & 0x1 != 0
    }
    /// `nNodes:13`.
    #[inline]
    pub const fn nNodes(&self) -> u32 {
        (self.bits >> 3) & 0x1FFF
    }
    /// `prefixSize:16`.
    #[inline]
    pub const fn prefixSize(&self) -> u32 {
        (self.bits >> 16) & 0xFFFF
    }
    /// Set `tupstate:2`.
    #[inline]
    pub fn set_tupstate(&mut self, v: u32) {
        self.bits = (self.bits & !0x3) | (v & 0x3);
    }
    /// Set `allTheSame:1`.
    #[inline]
    pub fn set_allTheSame(&mut self, v: bool) {
        self.bits = (self.bits & !(0x1 << 2)) | ((v as u32) << 2);
    }
    /// Set `nNodes:13`.
    #[inline]
    pub fn set_nNodes(&mut self, v: u32) {
        self.bits = (self.bits & !(0x1FFF << 3)) | ((v & 0x1FFF) << 3);
    }
    /// Set `prefixSize:16`.
    #[inline]
    pub fn set_prefixSize(&mut self, v: u32) {
        self.bits = (self.bits & !(0xFFFF << 16)) | ((v & 0xFFFF) << 16);
    }
}

// ===========================================================================
// access/spgist_private.h — node tuple (an IndexTupleData).
// ===========================================================================

/// `SpGistNodeTupleData` — one node within an inner tuple (spgist_private.h).
///
/// Node tuples reuse the ordinary `IndexTupleData` header. We mirror its 8-byte
/// on-disk layout (`t_tid` ItemPointerData + `t_info` uint16) directly so the
/// node-tuple header is exact without depending on the index-tuple crate.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistNodeTupleData {
    /// reference TID to heap tuple (an `IndexTupleData.t_tid`).
    pub t_tid: ItemPointerData,
    /// number of index attributes plus various flags (`IndexTupleData.t_info`).
    pub t_info: u16,
}

// ===========================================================================
// access/spgist_private.h — leaf tuple header.
// ===========================================================================

/// `SpGistLeafTupleData` — on-disk leaf tuple header (spgist_private.h).
///
/// The C struct packs `tupstate:2, size:30` into one `unsigned int` word
/// (`bits`); `t_info` holds the 14-bit nextOffset plus two flag bits.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistLeafTupleData {
    /// packed bit fields: tupstate:2, size:30.
    pub bits: u32,
    /// nextOffset (14 bits) plus two flag bits.
    pub t_info: u16,
    /// TID of represented heap tuple.
    pub heapPtr: ItemPointerData,
    /* nulls bitmap follows if the flag bit for it is set;
     * leaf datum, then any included datums, follow on a MAXALIGN boundary. */
}

impl SpGistLeafTupleData {
    /// `tupstate:2`.
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.bits & 0x3
    }
    /// `size:30`.
    #[inline]
    pub const fn size(&self) -> u32 {
        self.bits >> 2
    }
    /// Set `tupstate:2`.
    #[inline]
    pub fn set_tupstate(&mut self, v: u32) {
        self.bits = (self.bits & !0x3) | (v & 0x3);
    }
    /// Set `size:30`.
    #[inline]
    pub fn set_size(&mut self, v: u32) {
        self.bits = (self.bits & 0x3) | (v << 2);
    }
    /// `SGLT_GET_NEXTOFFSET` — nextOffset (low 14 bits of `t_info`).
    #[inline]
    pub const fn get_nextOffset(&self) -> u16 {
        self.t_info & 0x3FFF
    }
    /// `SGLT_GET_HASNULLMASK` — has-nulls-bitmap flag (bit 0x8000 of `t_info`).
    #[inline]
    pub const fn get_hasNullMask(&self) -> bool {
        self.t_info & 0x8000 != 0
    }
    /// `SGLT_SET_NEXTOFFSET`.
    #[inline]
    pub fn set_nextOffset(&mut self, offset: u16) {
        self.t_info = (self.t_info & 0xC000) | (offset & 0x3FFF);
    }
    /// `SGLT_SET_HASNULLMASK`.
    #[inline]
    pub fn set_hasNullMask(&mut self, hasnulls: bool) {
        self.t_info = (self.t_info & 0x7FFF) | (if hasnulls { 0x8000 } else { 0 });
    }
}

// ===========================================================================
// access/spgist_private.h — dead tuple header.
// ===========================================================================

/// `SpGistDeadTupleData` — declaration for examining non-live tuples
/// (spgist_private.h).
///
/// Its `tupstate`/`size` bit fields match a leaf tuple's, its `t_info` is unused
/// (present only for alignment), and `pointer` sits where a leaf tuple's
/// `heapPtr` is so a leaf can be replaced in place by a dead tuple.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistDeadTupleData {
    /// packed bit fields: tupstate:2, size:30.
    pub bits: u32,
    /// not used in dead tuples (present for alignment).
    pub t_info: u16,
    /// redirection inside index (valid only when tupstate = REDIRECT).
    pub pointer: ItemPointerData,
    /// ID of xact that inserted this tuple (valid only when REDIRECT).
    pub xid: TransactionId,
}

impl SpGistDeadTupleData {
    /// `tupstate:2`.
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.bits & 0x3
    }
    /// `size:30`.
    #[inline]
    pub const fn size(&self) -> u32 {
        self.bits >> 2
    }
    /// Set `tupstate:2`.
    #[inline]
    pub fn set_tupstate(&mut self, v: u32) {
        self.bits = (self.bits & !0x3) | (v & 0x3);
    }
    /// Set `size:30`.
    #[inline]
    pub fn set_size(&mut self, v: u32) {
        self.bits = (self.bits & 0x3) | (v << 2);
    }
}

// ===========================================================================
// access/spgist_private.h — SpGistGetBuffer flag bits.
// ===========================================================================

/// `GBUF_LEAF` — request a leaf page (spgist_private.h).
pub const GBUF_LEAF: i32 = 0x03;
/// `GBUF_NULLS` — OR in to request a page for null-valued tuples.
pub const GBUF_NULLS: i32 = 0x04;
/// `GBUF_PARITY_MASK` — mask isolating the triple-parity group bits.
pub const GBUF_PARITY_MASK: i32 = 0x03;

/// `GBUF_INNER_PARITY(x)` — inner-page parity group for block number `x`.
#[inline]
pub const fn GBUF_INNER_PARITY(x: BlockNumber) -> i32 {
    (x % 3) as i32
}

/// `GBUF_REQ_LEAF(flags)` — does `flags` request a leaf page?
#[inline]
pub const fn GBUF_REQ_LEAF(flags: i32) -> bool {
    (flags & GBUF_PARITY_MASK) == GBUF_LEAF
}

/// `GBUF_REQ_NULLS(flags)` — does `flags` request a nulls page?
#[inline]
pub const fn GBUF_REQ_NULLS(flags: i32) -> bool {
    (flags & GBUF_NULLS) != 0
}

// ===========================================================================
// Compile-time layout assertions for the on-disk structs.
// ===========================================================================

const _: () = {
    // SpGistPageOpaqueData: 4 * uint16 = 8.
    assert!(core::mem::size_of::<SpGistPageOpaqueData>() == 8);
    assert!(core::mem::align_of::<SpGistPageOpaqueData>() == 2);

    // SpGistLastUsedPage: BlockNumber (4) + int (4) = 8.
    assert!(core::mem::size_of::<SpGistLastUsedPage>() == 8);

    // SpGistMetaPageData: magic (4) + 8 cache slots * 8 = 4 + 64 = 68.
    assert!(core::mem::size_of::<SpGistMetaPageData>() == 4 + SPGIST_CACHED_PAGES * 8);

    // SpGistInnerTupleData: uint32 (4) + uint16 (2), padded to align 4 = 8.
    assert!(core::mem::offset_of!(SpGistInnerTupleData, size) == 4);
    assert!(core::mem::align_of::<SpGistInnerTupleData>() == 4);

    // SpGistNodeTupleData == IndexTupleData: ItemPointerData (6) + uint16 (2) = 8.
    assert!(core::mem::size_of::<SpGistNodeTupleData>() == 8);
    assert!(core::mem::offset_of!(SpGistNodeTupleData, t_info) == 6);

    // SpGistLeafTupleData: uint32 (4) + uint16 (2) + ItemPointerData (6) = 12.
    assert!(core::mem::offset_of!(SpGistLeafTupleData, t_info) == 4);
    assert!(core::mem::offset_of!(SpGistLeafTupleData, heapPtr) == 6);
    assert!(core::mem::size_of::<SpGistLeafTupleData>() == 12);

    // SpGistDeadTupleData: uint32 (4) + uint16 (2) + ItemPointerData (6) +
    // TransactionId (4) = 16.
    assert!(core::mem::offset_of!(SpGistDeadTupleData, t_info) == 4);
    assert!(core::mem::offset_of!(SpGistDeadTupleData, pointer) == 6);
    assert!(core::mem::offset_of!(SpGistDeadTupleData, xid) == 12);
    assert!(core::mem::size_of::<SpGistDeadTupleData>() == 16);
};

#[cfg(test)]
mod tests;
