//! GIN vocabulary: on-disk block layouts (ginblock.h), GinState carrier,
//! scan opaque (gin_private.h), and WAL constants (ginxlog.h). Plain data
//! only; behavior lives in the `gin` crate.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use ::datum::Datum;
use ::mcx::{Mcx, PgBox, PgVec};
use ::tidbitmap::{TbmPrivateIterator, TIDBitmap, TBM_MAX_TUPLES_PER_PAGE};
use ::types_core::{
    uint16, uint32, BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber, Oid,
    BLCKSZ,
};
use ::types_error::PgResult;
use ::types_scan::scankey::StrategyNumber;
use ::types_tuple::itemptr::{
    BlockIdData, BlockIdGetBlockNumber, BlockIdSet, ItemPointerData,
    ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumberNoCheck,
};

pub const GIN_DATA: uint16 = 1 << 0;
pub const GIN_LEAF: uint16 = 1 << 1;
pub const GIN_DELETED: uint16 = 1 << 2;
pub const GIN_META: uint16 = 1 << 3;
pub const GIN_LIST: uint16 = 1 << 4;
pub const GIN_LIST_FULLROW: uint16 = 1 << 5;
pub const GIN_INCOMPLETE_SPLIT: uint16 = 1 << 6;
pub const GIN_COMPRESSED: uint16 = 1 << 7;

pub const GIN_METAPAGE_BLKNO: BlockNumber = 0;
pub const GIN_ROOT_BLKNO: BlockNumber = 1;

pub const GIN_CURRENT_VERSION: i32 = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GinPageOpaqueData {
    pub rightlink: BlockNumber,
    pub maxoff: OffsetNumber,
    pub flags: uint16,
}

const _: () = assert!(core::mem::size_of::<GinPageOpaqueData>() == 8);

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GinMetaPageData {
    pub head: BlockNumber,
    pub tail: BlockNumber,
    pub tailFreeSize: uint32,
    pub nPendingPages: BlockNumber,
    pub nPendingHeapTuples: i64,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: i64,
    pub ginVersion: i32,
}

const _: () = assert!(core::mem::size_of::<GinMetaPageData>() == 56);

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GinStatsData {
    pub nPendingPages: BlockNumber,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: i64,
    pub ginVersion: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PostingItem {
    pub child_blkno: BlockIdData,
    pub key: ItemPointerData,
}

const _: () = assert!(core::mem::size_of::<PostingItem>() == 10);
const _: () = assert!(core::mem::align_of::<PostingItem>() == 2);

#[inline]
pub fn PostingItemGetBlockNumber(p: &PostingItem) -> BlockNumber {
    BlockIdGetBlockNumber(&p.child_blkno)
}

#[inline]
pub fn PostingItemSetBlockNumber(p: &mut PostingItem, blkno: BlockNumber) {
    BlockIdSet(&mut p.child_blkno, blkno);
}

pub type GinNullCategory = i8;

pub const GIN_CAT_NORM_KEY: GinNullCategory = 0;
pub const GIN_CAT_NULL_KEY: GinNullCategory = 1;
pub const GIN_CAT_EMPTY_ITEM: GinNullCategory = 2;
pub const GIN_CAT_NULL_ITEM: GinNullCategory = 3;
pub const GIN_CAT_EMPTY_QUERY: GinNullCategory = -1;

pub type GinTernaryValue = i8;
pub const GIN_FALSE: GinTernaryValue = 0;
pub const GIN_TRUE: GinTernaryValue = 1;
pub const GIN_MAYBE: GinTernaryValue = 2;

pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
pub const GIN_SEARCH_MODE_ALL: i32 = 2;
pub const GIN_SEARCH_MODE_EVERYTHING: i32 = 3;

pub const GIN_COMPARE_PROC: uint16 = 1;
pub const GIN_EXTRACTVALUE_PROC: uint16 = 2;
pub const GIN_EXTRACTQUERY_PROC: uint16 = 3;
pub const GIN_CONSISTENT_PROC: uint16 = 4;
pub const GIN_COMPARE_PARTIAL_PROC: uint16 = 5;
pub const GIN_TRICONSISTENT_PROC: uint16 = 6;
pub const GIN_OPTIONS_PROC: uint16 = 7;
pub const GINNProcs: usize = 7;

pub const GIN_DEFAULT_USE_FASTUPDATE: bool = true;

#[inline]
pub fn gin_item_pointer_block(p: &ItemPointerData) -> BlockNumber {
    ItemPointerGetBlockNumberNoCheck(p)
}

#[inline]
pub fn gin_item_pointer_offset(p: &ItemPointerData) -> OffsetNumber {
    ItemPointerGetOffsetNumberNoCheck(p)
}

#[inline]
pub fn item_pointer_set_min(p: &mut ItemPointerData) {
    *p = ItemPointerData::new(0, 0);
}

#[inline]
pub fn item_pointer_is_min(p: &ItemPointerData) -> bool {
    gin_item_pointer_offset(p) == 0 && gin_item_pointer_block(p) == 0
}

#[inline]
pub fn item_pointer_set_max(p: &mut ItemPointerData) {
    *p = ItemPointerData::new(InvalidBlockNumber, 0xffff);
}

#[inline]
pub fn item_pointer_set_lossy_page(p: &mut ItemPointerData, b: BlockNumber) {
    *p = ItemPointerData::new(b, 0xffff);
}

#[inline]
pub fn item_pointer_is_lossy_page(p: &ItemPointerData) -> bool {
    gin_item_pointer_offset(p) == 0xffff && gin_item_pointer_block(p) != InvalidBlockNumber
}

#[inline]
pub fn ginCompareItemPointers(a: &ItemPointerData, b: &ItemPointerData) -> i32 {
    let ia = ((gin_item_pointer_block(a) as u64) << 32) | gin_item_pointer_offset(a) as u64;
    let ib = ((gin_item_pointer_block(b) as u64) << 32) | gin_item_pointer_offset(b) as u64;
    if ia < ib {
        -1
    } else {
        (ia > ib) as i32
    }
}

pub const fn MAXALIGN(x: usize) -> usize {
    (x + 7) & !7
}

pub const fn SHORTALIGN(x: usize) -> usize {
    (x + 1) & !1
}

pub const SizeOfPageHeaderData: usize = 24;
const SizeOfItemIdData: usize = 4;
pub const INDEX_SIZE_MASK: usize = 0x1FFF;

pub const GinMaxItemSize: usize = {
    let v = (BLCKSZ
        - MAXALIGN(SizeOfPageHeaderData + 3 * SizeOfItemIdData)
        - MAXALIGN(core::mem::size_of::<GinPageOpaqueData>()))
        / 3;
    let v = v & !7;
    if v < INDEX_SIZE_MASK {
        v
    } else {
        INDEX_SIZE_MASK
    }
};

pub const GinDataPageMaxDataSize: usize = BLCKSZ
    - MAXALIGN(SizeOfPageHeaderData)
    - MAXALIGN(core::mem::size_of::<ItemPointerData>())
    - MAXALIGN(core::mem::size_of::<GinPageOpaqueData>());

pub const GinListPageSize: usize =
    BLCKSZ - SizeOfPageHeaderData - MAXALIGN(core::mem::size_of::<GinPageOpaqueData>());

pub const GinDataPageDataOffset: usize =
    MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(core::mem::size_of::<ItemPointerData>());

pub const SizeOfGinPostingListHeader: usize = 8;

#[inline]
pub const fn size_of_gin_posting_list(nbytes: usize) -> usize {
    SizeOfGinPostingListHeader + SHORTALIGN(nbytes)
}

/// Closed opclass set (rule 4): support-proc OIDs map to a tag at
/// initGinState; unknown opclasses panic loudly there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinOpclass {
    JsonbOps,
    JsonbPathOps,
    TsvectorOps,
    ArrayOps,
    // contrib gin_trgm_ops: extension-oid procs matched by proname at
    // initGinState; bodies reached through gin_trgm_seams.
    TrgmOps,
    // contrib gin_hstore_ops: same proname scheme; bodies through
    // gin_hstore_seams (text keys, shimmed triconsistent).
    HstoreOps,
    // contrib gin__int_ops (intarray): shares ginarrayextract as proc 2, so
    // it is disambiguated from ArrayOps by the extractQuery proc; bodies
    // through gin_int4_seams (int4 keys, shimmed triconsistent).
    IntArrayOps,
    // contrib btree_gin scalar opclasses: same proname scheme; bodies
    // through gin_btree_seams.
    BtreeOps(GinBtreeType),
}

/// contrib/btree_gin per-type tag, one per C GIN_SUPPORT expansion.
/// timestamptz/cidr/varbit collapse onto Timestamp/Inet/Bit — identical
/// leftmost values and comparators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinBtreeType {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Money,
    Oid,
    Timestamp,
    Time,
    Timetz,
    Date,
    Interval,
    Macaddr,
    Macaddr8,
    Inet,
    Text,
    Bpchar,
    Char,
    Bytea,
    Bit,
    Numeric,
    Enum,
    Uuid,
    Name,
    Bool,
}

impl GinBtreeType {
    /// Stable tag order for the rd_amcache round-trip.
    pub const ALL: [GinBtreeType; 25] = [
        GinBtreeType::Int2,
        GinBtreeType::Int4,
        GinBtreeType::Int8,
        GinBtreeType::Float4,
        GinBtreeType::Float8,
        GinBtreeType::Money,
        GinBtreeType::Oid,
        GinBtreeType::Timestamp,
        GinBtreeType::Time,
        GinBtreeType::Timetz,
        GinBtreeType::Date,
        GinBtreeType::Interval,
        GinBtreeType::Macaddr,
        GinBtreeType::Macaddr8,
        GinBtreeType::Inet,
        GinBtreeType::Text,
        GinBtreeType::Bpchar,
        GinBtreeType::Char,
        GinBtreeType::Bytea,
        GinBtreeType::Bit,
        GinBtreeType::Numeric,
        GinBtreeType::Enum,
        GinBtreeType::Uuid,
        GinBtreeType::Name,
        GinBtreeType::Bool,
    ];

    pub fn tag(self) -> u8 {
        GinBtreeType::ALL.iter().position(|&t| t == self).unwrap() as u8
    }

    pub fn from_tag(tag: u8) -> Option<GinBtreeType> {
        GinBtreeType::ALL.get(tag as usize).copied()
    }

    /// C type-name suffix of the module's gin_extract_{value,query}_<name>
    /// support procs (extension oids are dynamic; initGinState resolves by
    /// proname).
    pub fn from_type_name(name: &str) -> Option<GinBtreeType> {
        Some(match name {
            "int2" => GinBtreeType::Int2,
            "int4" => GinBtreeType::Int4,
            "int8" => GinBtreeType::Int8,
            "float4" => GinBtreeType::Float4,
            "float8" => GinBtreeType::Float8,
            "money" => GinBtreeType::Money,
            "oid" => GinBtreeType::Oid,
            "timestamp" | "timestamptz" => GinBtreeType::Timestamp,
            "time" => GinBtreeType::Time,
            "timetz" => GinBtreeType::Timetz,
            "date" => GinBtreeType::Date,
            "interval" => GinBtreeType::Interval,
            "macaddr" => GinBtreeType::Macaddr,
            "macaddr8" => GinBtreeType::Macaddr8,
            "inet" | "cidr" => GinBtreeType::Inet,
            "text" => GinBtreeType::Text,
            "bpchar" => GinBtreeType::Bpchar,
            "char" => GinBtreeType::Char,
            "bytea" => GinBtreeType::Bytea,
            "bit" | "varbit" => GinBtreeType::Bit,
            "numeric" => GinBtreeType::Numeric,
            "anyenum" => GinBtreeType::Enum,
            "uuid" => GinBtreeType::Uuid,
            "name" => GinBtreeType::Name,
            "bool" => GinBtreeType::Bool,
            _ => return None,
        })
    }

    /// C's per-type is_varlena flag (detoast on extract).
    pub const fn is_varlena(self) -> bool {
        matches!(
            self,
            GinBtreeType::Inet
                | GinBtreeType::Text
                | GinBtreeType::Bpchar
                | GinBtreeType::Bytea
                | GinBtreeType::Bit
                | GinBtreeType::Numeric
        )
    }
}

/// array_ops has no GIN_COMPARE_PROC; C falls back to the element type's
/// default btree comparator via typcache (initGinState). Closed element set,
/// resolved once at initGinState; None only on states that never compare
/// (the gincost extractQuery probe).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinElemCmp {
    None,
    Int2,
    Int4,
    Int8,
    Oid,
    Text,
}

pub const JSP_GIN_OR: u8 = 0;
pub const JSP_GIN_AND: u8 = 1;
pub const JSP_GIN_ENTRY: u8 = 2;

/// Preorder-flattened jsonpath GIN expression tree (jsonb_gin.c
/// JsonPathGinNode, C's extra_data[0]); val = nargs (OR/AND) or check index.
#[derive(Clone, Copy, Debug)]
pub struct JspGinOp {
    pub kind: u8,
    pub val: u32,
}

/// One arc of a packed trigram regex graph (trgm_regexp.c TrgmPackedArc).
#[derive(Clone, Copy, Debug)]
pub struct TrgmPackedArc {
    pub target_state: i32,
    pub color_trgm: i32,
}

/// contrib/pg_trgm trgm_regexp.c TrgmPackedGraph: the compact NFA-derived
/// graph a ~ / ~* scan key evaluates per index entry (C's extra_data[0]).
/// Built once per scan key by pg_trgm's createTrgmNFA port; state 0 is
/// initial, state 1 final. Std Vec justified: per-scan-key value crossing
/// the gin_trgm seam by ownership (C pallocs it in the query context).
pub struct TrgmPackedGraph {
    /// Simple-trigram count per color trigram; the check[] array laid out
    /// group-by-group in color-trigram order.
    pub color_trigram_groups: Vec<i32>,
    /// Per-state (offset, count) into `arcs`.
    pub states: Vec<(u32, u32)>,
    pub arcs: Vec<TrgmPackedArc>,
    // trigramsMatchGraph workspace (C preallocates in the graph struct).
    color_trigrams_active: Vec<bool>,
    states_active: Vec<bool>,
    states_queue: Vec<i32>,
}

impl TrgmPackedGraph {
    pub fn new(
        color_trigram_groups: Vec<i32>,
        states: Vec<(u32, u32)>,
        arcs: Vec<TrgmPackedArc>,
    ) -> Self {
        let ncolors = color_trigram_groups.len();
        let nstates = states.len();
        TrgmPackedGraph {
            color_trigram_groups,
            states,
            arcs,
            color_trigrams_active: vec![false; ncolors],
            states_active: vec![false; nstates],
            states_queue: vec![0; nstates],
        }
    }

    /// trigramsMatchGraph: `check` is indexed by simple-trigram number in
    /// the array createTrgmNFA returned.
    pub fn matches(&mut self, check: &[bool]) -> bool {
        self.color_trigrams_active.fill(false);
        self.states_active.fill(false);

        let mut j = 0usize;
        for (i, &cnt) in self.color_trigram_groups.iter().enumerate() {
            self.color_trigrams_active[i] = check[j..j + cnt as usize].iter().any(|&c| c);
            j += cnt as usize;
        }

        self.states_active[0] = true;
        self.states_queue[0] = 0;
        let mut queue_in = 0usize;
        let mut queue_out = 1usize;

        while queue_in < queue_out {
            let stateno = self.states_queue[queue_in] as usize;
            queue_in += 1;
            let (off, cnt) = self.states[stateno];
            for arc in &self.arcs[off as usize..(off + cnt) as usize] {
                if self.color_trigrams_active[arc.color_trgm as usize] {
                    let next = arc.target_state;
                    if next == 1 {
                        return true;
                    }
                    if !self.states_active[next as usize] {
                        self.states_active[next as usize] = true;
                        self.states_queue[queue_out] = next;
                        queue_out += 1;
                    }
                }
            }
        }
        false
    }
}

/// Per-key-column resolved opclass state (C GinState's per-attnum arrays).
#[derive(Clone, Copy, Debug)]
pub struct GinColState {
    pub opclass: GinOpclass,
    pub elem_cmp: GinElemCmp,
    pub support_collation: Oid,
    pub can_partial_match: bool,
    pub key_byval: bool,
    pub key_len: i16,
}

/// INDEX_MAX_KEYS.
pub const GIN_MAX_KEY_COLS: usize = 32;

#[derive(Clone, Copy, Debug)]
pub struct GinState {
    pub natts: u16,
    pub one_col: bool,
    pub cols: [GinColState; GIN_MAX_KEY_COLS],
}

impl GinState {
    #[inline]
    pub fn col(&self, attnum: OffsetNumber) -> &GinColState {
        debug_assert!(attnum >= 1 && (attnum as u16) <= self.natts);
        &self.cols[attnum as usize - 1]
    }
}

// Scan opaque: entry sharing is u32 handles into GinScanWork.entries (C
// shares pointers); the 'static lifetimes are an erasure over key_ctx.

pub struct GinScanKeyData {
    pub nentries: u32,
    pub nuserentries: u32,
    pub scanEntry: PgVec<'static, u32>,
    pub requiredEntries: PgVec<'static, u32>,
    pub additionalEntries: PgVec<'static, u32>,
    pub entryRes: PgVec<'static, GinTernaryValue>,
    pub query: Datum,
    pub queryValues: PgVec<'static, Datum>,
    pub queryCategories: PgVec<'static, GinNullCategory>,
    pub jspOps: PgVec<'static, JspGinOp>,
    // tsvector_ops extra_data[0]: QueryItem index -> operand (entry) number.
    pub mapItemOperand: PgVec<'static, i32>,
    // gin_trgm_ops regexp extra_data[0]: the packed trigram graph.
    pub trgmGraph: Option<TrgmPackedGraph>,
    pub strategy: StrategyNumber,
    pub searchMode: i32,
    pub attnum: OffsetNumber,
    pub excludeOnly: bool,
    pub curItem: ItemPointerData,
    pub curItemMatches: bool,
    pub recheckCurItem: bool,
    pub isFinished: bool,
}

pub struct GinScanEntryData {
    pub queryKey: Datum,
    // btree_gin's original query datum (C QueryInfo.datum in extra_data):
    // for the < / <= strategies queryKey is the type's leftmost value and
    // comparePartial compares against this instead. Null elsewhere.
    pub queryOrig: Datum,
    pub queryCategory: GinNullCategory,
    pub isPartialMatch: bool,
    pub strategy: StrategyNumber,
    pub searchMode: i32,
    pub attnum: OffsetNumber,

    pub buffer: Buffer,
    pub curItem: ItemPointerData,

    pub matchBitmap: Option<TIDBitmap<'static>>,
    pub matchIterator: Option<TbmPrivateIterator>,
    // Extracted TBMIterateResult snapshot (C keeps a borrow into the bitmap).
    pub matchBlockno: BlockNumber,
    pub matchLossy: bool,
    pub matchNtuples: i32,
    pub matchOffsets: PgVec<'static, OffsetNumber>,

    pub list: PgVec<'static, ItemPointerData>,
    pub offset: usize,

    pub isFinished: bool,
    pub reduceResult: bool,
    pub predictNumberResult: u32,
    // Posting-tree root of the entry stream (only root block is state).
    pub postingRoot: BlockNumber,
}

impl GinScanEntryData {
    /// # Safety
    /// `mcx` must be the scan's key_ctx; the entry must not outlive it.
    pub unsafe fn new(mcx: Mcx<'_>) -> PgResult<Self> {
        let offsets: PgVec<'_, OffsetNumber> =
            mcx::vec_from_elem_in(mcx, 0 as OffsetNumber, TBM_MAX_TUPLES_PER_PAGE);
        Ok(GinScanEntryData {
            queryKey: Datum::null(),
            queryOrig: Datum::null(),
            queryCategory: GIN_CAT_NORM_KEY,
            isPartialMatch: false,
            strategy: 0,
            searchMode: GIN_SEARCH_MODE_DEFAULT,
            attnum: 0,
            buffer: InvalidBuffer,
            curItem: ItemPointerData::invalid(),
            matchBitmap: None,
            matchIterator: None,
            matchBlockno: InvalidBlockNumber,
            matchLossy: false,
            matchNtuples: -1,
            matchOffsets: unsafe { core::mem::transmute(offsets) },
            list: unsafe { core::mem::transmute(mcx::vec_new_in::<ItemPointerData>(mcx)) },
            offset: 0,
            isFinished: false,
            reduceResult: false,
            predictNumberResult: 0,
            postingRoot: InvalidBlockNumber,
        })
    }
}

/// Per-(re)scan key state: C's keyCtx plus everything allocated in it.
/// Dropped as a unit on rescan/endscan (vectors first, then the context).
pub struct GinScanWork {
    pub keys: PgVec<'static, GinScanKeyData>,
    pub entries: PgVec<'static, PgBox<'static, GinScanEntryData>>,
    pub key_ctx: Box<mcx::MemoryContext>,
    // C so->tempCtx: consistent-fn scratch, reset after each call.
    pub temp_ctx: Box<mcx::MemoryContext>,
}

impl GinScanWork {
    pub fn new() -> Self {
        let key_ctx = Box::new(mcx::MemoryContext::new_bump("Gin scan key context"));
        // SAFETY: erasure over key_ctx; both vecs die (in declaration order)
        // before key_ctx in Drop.
        let keys = unsafe {
            core::mem::transmute::<PgVec<'_, GinScanKeyData>, PgVec<'static, GinScanKeyData>>(
                PgVec::new_in(key_ctx.mcx()),
            )
        };
        let entries = unsafe {
            core::mem::transmute::<
                PgVec<'_, PgBox<'_, GinScanEntryData>>,
                PgVec<'static, PgBox<'static, GinScanEntryData>>,
            >(PgVec::new_in(key_ctx.mcx()))
        };
        GinScanWork {
            keys,
            entries,
            key_ctx,
            temp_ctx: Box::new(mcx::MemoryContext::new_bump("Gin scan temporary context")),
        }
    }

    /// # Safety
    /// Anything allocated from it must be stored in this GinScanWork.
    pub unsafe fn kcx(&self) -> Mcx<'static> {
        unsafe { core::mem::transmute(self.key_ctx.mcx()) }
    }
}

impl Default for GinScanWork {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GinScanOpaqueData {
    pub ginstate: Option<GinState>,
    pub work: Option<GinScanWork>,
    pub isVoidRes: bool,
}

pub const XLOG_GIN_CREATE_PTREE: u8 = 0x10;
pub const XLOG_GIN_INSERT: u8 = 0x20;
pub const XLOG_GIN_SPLIT: u8 = 0x30;
pub const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;
pub const XLOG_GIN_VACUUM_DATA_LEAF_PAGE: u8 = 0x90;
pub const XLOG_GIN_DELETE_PAGE: u8 = 0x50;
pub const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;
pub const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;
pub const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;

pub const GIN_INSERT_ISDATA: uint16 = 0x01;
pub const GIN_INSERT_ISLEAF: uint16 = 0x02;
pub const GIN_SPLIT_ROOT: uint16 = 0x04;

pub const GIN_SEGMENT_UNMODIFIED: u8 = 0;
pub const GIN_SEGMENT_DELETE: u8 = 1;
pub const GIN_SEGMENT_INSERT: u8 = 2;
pub const GIN_SEGMENT_REPLACE: u8 = 3;
pub const GIN_SEGMENT_ADDITEMS: u8 = 4;

pub const GIN_NDELETE_AT_ONCE: usize = 16;
