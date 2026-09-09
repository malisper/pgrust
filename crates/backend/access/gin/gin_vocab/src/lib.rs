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

// C's GinState keeps one FmgrInfo per support-proc slot and per column
// (ginutil.c initGinState: compareFn, extractValueFn, extractQueryFn,
// consistentFn, triConsistentFn, comparePartialFn), each resolved on its own
// through index_getprocinfo, so any pairing of support procs an opclass
// registers is honoured. pgrust resolves each slot the same way, to a
// known-set tag (rule 4: enum dispatch, no fmgr frame) keyed on the proc
// fmgr would dispatch to: the canonical builtin (a core proc or a LANGUAGE
// internal alias of one) or the C-language link symbol of an in-tree
// extension proc. Only the compare slot, whose procs take no `internal`
// argument, can name a proc outside the known set (a SQL / PL function);
// it carries the oid and is called through fmgr, as C does.

/// compareFn[i] (GIN_COMPARE_PROC, or the storage type's default btree
/// comparator from the typcache when the opclass omits proc 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinCompareFn {
    /// btint2cmp
    Int2,
    /// btint4cmp (jsonb_path_ops / gin_trgm_ops / gin__int_ops key compare)
    Int4,
    /// btint8cmp
    Int8,
    /// btoidcmp
    Oid,
    /// bttextcmp under the support collation (hstore, text-keyed array_ops)
    Text,
    /// gin_compare_jsonb
    Jsonb,
    /// gin_cmp_tslexeme
    TsLexeme,
    /// contrib/btree_gin FUNCTION 1 (the type's btree comparator, or
    /// gin_numeric_cmp / gin_enum_cmp); bodies through gin_btree_seams.
    Btree(GinBtreeType),
    /// Any other comparator (a SQL / PL function, a core comparator without
    /// a specialized arm): resolved at initGinState, called through fmgr per
    /// compare with the support collation (C's FunctionCall2Coll).
    Fmgr(::types_core::Oid),
}

/// extractValueFn[i] (GIN_EXTRACTVALUE_PROC).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinExtractValueFn {
    /// gin_extract_jsonb
    Jsonb,
    /// gin_extract_jsonb_path
    JsonbPath,
    /// gin_extract_tsvector
    Tsvector,
    /// ginarrayextract (array_ops; also intarray's gin__int_ops proc 2)
    Array,
    /// pg_trgm gin_extract_value_trgm
    Trgm,
    /// hstore gin_extract_hstore
    Hstore,
    /// btree_gin gin_extract_value_<type>
    Btree(GinBtreeType),
}

/// extractQueryFn[i] (GIN_EXTRACTQUERY_PROC).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinExtractQueryFn {
    /// gin_extract_jsonb_query
    Jsonb,
    /// gin_extract_jsonb_query_path
    JsonbPath,
    /// gin_extract_tsquery
    Tsquery,
    /// ginqueryarrayextract
    Array,
    /// pg_trgm gin_extract_query_trgm
    Trgm,
    /// hstore gin_extract_hstore_query
    Hstore,
    /// intarray ginint4_queryextract
    IntArray,
    /// btree_gin gin_extract_query_<type>
    Btree(GinBtreeType),
}

/// consistentFn[i] (GIN_CONSISTENT_PROC); None when the opclass registers
/// only the tri-state proc (ginlogic.c shimBoolConsistentFn).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinConsistentFn {
    /// gin_consistent_jsonb
    Jsonb,
    /// gin_consistent_jsonb_path
    JsonbPath,
    /// gin_tsquery_consistent
    Tsquery,
    /// ginarrayconsistent
    Array,
    /// pg_trgm gin_trgm_consistent
    Trgm,
    /// hstore gin_consistent_hstore
    Hstore,
    /// intarray ginint4_consistent
    IntArray,
    /// btree_gin gin_btree_consistent
    Btree,
}

/// triConsistentFn[i] (GIN_TRICONSISTENT_PROC); None when the opclass
/// registers only the binary proc (ginlogic.c shimTriConsistentFn).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinTriConsistentFn {
    /// gin_triconsistent_jsonb
    Jsonb,
    /// gin_triconsistent_jsonb_path
    JsonbPath,
    /// gin_tsquery_triconsistent
    Tsquery,
    /// ginarraytriconsistent
    Array,
    /// pg_trgm gin_trgm_triconsistent
    Trgm,
}

/// comparePartialFn[i] (GIN_COMPARE_PARTIAL_PROC); None means
/// canPartialMatch[i] = false.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinComparePartialFn {
    /// gin_cmp_prefix
    TsPrefix,
    /// btree_gin gin_compare_prefix_<type>
    Btree(GinBtreeType),
}

// rd_amcache tag codecs (process-local round trip; insert.rs). Tag 0 of an
// optional slot is None; the btree_gin arms carry the type tag above a base.
const GIN_TAG_BTREE_BASE: u8 = 32;

impl GinCompareFn {
    pub fn tag(self) -> u8 {
        match self {
            GinCompareFn::Int2 => 1,
            GinCompareFn::Int4 => 2,
            GinCompareFn::Int8 => 3,
            GinCompareFn::Oid => 4,
            GinCompareFn::Text => 5,
            GinCompareFn::Jsonb => 6,
            GinCompareFn::TsLexeme => 7,
            GinCompareFn::Fmgr(_) => 8,
            GinCompareFn::Btree(ty) => GIN_TAG_BTREE_BASE + ty.tag(),
        }
    }

    pub fn from_tag(tag: u8, proc_oid: ::types_core::Oid) -> Option<GinCompareFn> {
        Some(match tag {
            1 => GinCompareFn::Int2,
            2 => GinCompareFn::Int4,
            3 => GinCompareFn::Int8,
            4 => GinCompareFn::Oid,
            5 => GinCompareFn::Text,
            6 => GinCompareFn::Jsonb,
            7 => GinCompareFn::TsLexeme,
            8 => GinCompareFn::Fmgr(proc_oid),
            t if t >= GIN_TAG_BTREE_BASE => {
                GinCompareFn::Btree(GinBtreeType::from_tag(t - GIN_TAG_BTREE_BASE)?)
            }
            _ => return None,
        })
    }
}

impl GinExtractValueFn {
    pub fn tag(self) -> u8 {
        match self {
            GinExtractValueFn::Jsonb => 1,
            GinExtractValueFn::JsonbPath => 2,
            GinExtractValueFn::Tsvector => 3,
            GinExtractValueFn::Array => 4,
            GinExtractValueFn::Trgm => 5,
            GinExtractValueFn::Hstore => 6,
            GinExtractValueFn::Btree(ty) => GIN_TAG_BTREE_BASE + ty.tag(),
        }
    }

    pub fn from_tag(tag: u8) -> Option<GinExtractValueFn> {
        Some(match tag {
            1 => GinExtractValueFn::Jsonb,
            2 => GinExtractValueFn::JsonbPath,
            3 => GinExtractValueFn::Tsvector,
            4 => GinExtractValueFn::Array,
            5 => GinExtractValueFn::Trgm,
            6 => GinExtractValueFn::Hstore,
            t if t >= GIN_TAG_BTREE_BASE => {
                GinExtractValueFn::Btree(GinBtreeType::from_tag(t - GIN_TAG_BTREE_BASE)?)
            }
            _ => return None,
        })
    }
}

impl GinExtractQueryFn {
    pub fn tag(self) -> u8 {
        match self {
            GinExtractQueryFn::Jsonb => 1,
            GinExtractQueryFn::JsonbPath => 2,
            GinExtractQueryFn::Tsquery => 3,
            GinExtractQueryFn::Array => 4,
            GinExtractQueryFn::Trgm => 5,
            GinExtractQueryFn::Hstore => 6,
            GinExtractQueryFn::IntArray => 7,
            GinExtractQueryFn::Btree(ty) => GIN_TAG_BTREE_BASE + ty.tag(),
        }
    }

    pub fn from_tag(tag: u8) -> Option<GinExtractQueryFn> {
        Some(match tag {
            1 => GinExtractQueryFn::Jsonb,
            2 => GinExtractQueryFn::JsonbPath,
            3 => GinExtractQueryFn::Tsquery,
            4 => GinExtractQueryFn::Array,
            5 => GinExtractQueryFn::Trgm,
            6 => GinExtractQueryFn::Hstore,
            7 => GinExtractQueryFn::IntArray,
            t if t >= GIN_TAG_BTREE_BASE => {
                GinExtractQueryFn::Btree(GinBtreeType::from_tag(t - GIN_TAG_BTREE_BASE)?)
            }
            _ => return None,
        })
    }
}

impl GinConsistentFn {
    pub fn tag(self) -> u8 {
        match self {
            GinConsistentFn::Jsonb => 1,
            GinConsistentFn::JsonbPath => 2,
            GinConsistentFn::Tsquery => 3,
            GinConsistentFn::Array => 4,
            GinConsistentFn::Trgm => 5,
            GinConsistentFn::Hstore => 6,
            GinConsistentFn::IntArray => 7,
            GinConsistentFn::Btree => 8,
        }
    }

    pub fn from_tag(tag: u8) -> Option<GinConsistentFn> {
        Some(match tag {
            1 => GinConsistentFn::Jsonb,
            2 => GinConsistentFn::JsonbPath,
            3 => GinConsistentFn::Tsquery,
            4 => GinConsistentFn::Array,
            5 => GinConsistentFn::Trgm,
            6 => GinConsistentFn::Hstore,
            7 => GinConsistentFn::IntArray,
            8 => GinConsistentFn::Btree,
            _ => return None,
        })
    }
}

impl GinTriConsistentFn {
    pub fn tag(self) -> u8 {
        match self {
            GinTriConsistentFn::Jsonb => 1,
            GinTriConsistentFn::JsonbPath => 2,
            GinTriConsistentFn::Tsquery => 3,
            GinTriConsistentFn::Array => 4,
            GinTriConsistentFn::Trgm => 5,
        }
    }

    pub fn from_tag(tag: u8) -> Option<GinTriConsistentFn> {
        Some(match tag {
            1 => GinTriConsistentFn::Jsonb,
            2 => GinTriConsistentFn::JsonbPath,
            3 => GinTriConsistentFn::Tsquery,
            4 => GinTriConsistentFn::Array,
            5 => GinTriConsistentFn::Trgm,
            _ => return None,
        })
    }
}

impl GinComparePartialFn {
    pub fn tag(self) -> u8 {
        match self {
            GinComparePartialFn::TsPrefix => 1,
            GinComparePartialFn::Btree(ty) => GIN_TAG_BTREE_BASE + ty.tag(),
        }
    }

    pub fn from_tag(tag: u8) -> Option<GinComparePartialFn> {
        Some(match tag {
            1 => GinComparePartialFn::TsPrefix,
            t if t >= GIN_TAG_BTREE_BASE => {
                GinComparePartialFn::Btree(GinBtreeType::from_tag(t - GIN_TAG_BTREE_BASE)?)
            }
            _ => return None,
        })
    }
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

/// Per-key-column resolved opclass state (C GinState's per-attnum arrays:
/// one resolved support proc per slot, plus supportCollation /
/// canPartialMatch and the storage attribute's byval/len).
#[derive(Clone, Copy, Debug)]
pub struct GinColState {
    pub compare: GinCompareFn,
    pub extract_value: GinExtractValueFn,
    pub extract_query: GinExtractQueryFn,
    pub consistent: Option<GinConsistentFn>,
    pub tri_consistent: Option<GinTriConsistentFn>,
    pub compare_partial: Option<GinComparePartialFn>,
    pub support_collation: Oid,
    pub can_partial_match: bool,
    pub key_byval: bool,
    pub key_len: i16,
}

impl GinColState {
    /// The core array_ops shape (ginarrayproc.c: procs 2/3/4/6, no proc 1 —
    /// `compare` is the element type's default btree comparator) over a
    /// storage attribute of the given byval/len.
    pub const fn array_ops(compare: GinCompareFn, key_byval: bool, key_len: i16) -> GinColState {
        GinColState {
            compare,
            extract_value: GinExtractValueFn::Array,
            extract_query: GinExtractQueryFn::Array,
            consistent: Some(GinConsistentFn::Array),
            tri_consistent: Some(GinTriConsistentFn::Array),
            compare_partial: None,
            support_collation: ::types_core::catalog::DEFAULT_COLLATION_OID,
            can_partial_match: false,
            key_byval,
            key_len,
        }
    }

    /// The core jsonb_ops shape (text keys, gin_compare_jsonb).
    pub const fn jsonb_ops(support_collation: Oid) -> GinColState {
        GinColState {
            compare: GinCompareFn::Jsonb,
            extract_value: GinExtractValueFn::Jsonb,
            extract_query: GinExtractQueryFn::Jsonb,
            consistent: Some(GinConsistentFn::Jsonb),
            tri_consistent: Some(GinTriConsistentFn::Jsonb),
            compare_partial: None,
            support_collation,
            can_partial_match: false,
            key_byval: false,
            key_len: -1,
        }
    }

    /// The core tsvector_ops shape (text keys, gin_cmp_tslexeme, prefix
    /// partial match through gin_cmp_prefix).
    pub const fn tsvector_ops(support_collation: Oid) -> GinColState {
        GinColState {
            compare: GinCompareFn::TsLexeme,
            extract_value: GinExtractValueFn::Tsvector,
            extract_query: GinExtractQueryFn::Tsquery,
            consistent: Some(GinConsistentFn::Tsquery),
            tri_consistent: Some(GinTriConsistentFn::Tsquery),
            compare_partial: Some(GinComparePartialFn::TsPrefix),
            support_collation,
            can_partial_match: true,
            key_byval: false,
            key_len: -1,
        }
    }

    /// contrib/hstore gin_hstore_ops (text keys under bttextcmp, binary
    /// consistent only).
    pub const fn hstore_ops(support_collation: Oid) -> GinColState {
        GinColState {
            compare: GinCompareFn::Text,
            extract_value: GinExtractValueFn::Hstore,
            extract_query: GinExtractQueryFn::Hstore,
            consistent: Some(GinConsistentFn::Hstore),
            tri_consistent: None,
            compare_partial: None,
            support_collation,
            can_partial_match: false,
            key_byval: false,
            key_len: -1,
        }
    }

    /// contrib/btree_gin per-type opclass shape (binary consistent only,
    /// prefix compare through gin_compare_prefix_<type>).
    pub const fn btree_ops(
        ty: GinBtreeType,
        support_collation: Oid,
        key_byval: bool,
        key_len: i16,
    ) -> GinColState {
        GinColState {
            compare: GinCompareFn::Btree(ty),
            extract_value: GinExtractValueFn::Btree(ty),
            extract_query: GinExtractQueryFn::Btree(ty),
            consistent: Some(GinConsistentFn::Btree),
            tri_consistent: None,
            compare_partial: Some(GinComparePartialFn::Btree(ty)),
            support_collation,
            can_partial_match: true,
            key_byval,
            key_len,
        }
    }
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

#[repr(transparent)]
pub struct GinScanVec<T: 'static>(core::mem::MaybeUninit<PgVec<'static, T>>);

impl<T> GinScanVec<T> {
    fn new(value: PgVec<'static, T>) -> Self {
        Self(core::mem::MaybeUninit::new(value))
    }
}

impl<T> core::ops::Deref for GinScanVec<T> {
    type Target = PgVec<'static, T>;

    fn deref(&self) -> &Self::Target {
        // SAFETY: initialized once by new, destroyed only by Drop.
        unsafe { self.0.assume_init_ref() }
    }
}

impl<T> core::ops::DerefMut for GinScanVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: initialized and exclusively borrowed.
        unsafe { self.0.assume_init_mut() }
    }
}

impl<T> Drop for GinScanVec<T> {
    fn drop(&mut self) {
        // SAFETY: initialized once; union storage avoids move-time reference
        // protectors surviving vector destruction until the context is freed.
        unsafe { self.0.assume_init_drop() }
    }
}

/// Per-(re)scan key state: C's keyCtx plus everything allocated in it.
/// Dropped as a unit on rescan/endscan (vectors first, then the context).
pub struct GinScanWork {
    pub keys: GinScanVec<GinScanKeyData>,
    pub entries: GinScanVec<PgBox<'static, GinScanEntryData>>,
    key_ctx: mcx::PinnedContext,
    // C so->tempCtx: consistent-fn scratch, reset after each call.
    pub temp_ctx: Box<mcx::MemoryContext>,
}

impl GinScanWork {
    /// # Safety
    /// Keep every key-context borrower within this work; none may escape its
    /// destruction, including vectors moved out of the public fields.
    pub unsafe fn new() -> Self {
        let key_ctx = mcx::PinnedContext::new(mcx::MemoryContext::new_bump("Gin scan key context"));
        // SAFETY: both vectors precede their owner in field destruction order;
        // the caller preserves that order for borrowers moved out of fields.
        let kcx = unsafe { key_ctx.handle() };
        GinScanWork {
            keys: GinScanVec::new(PgVec::new_in(kcx)),
            entries: GinScanVec::new(PgVec::new_in(kcx)),
            key_ctx,
            temp_ctx: Box::new(mcx::MemoryContext::new_bump("Gin scan temporary context")),
        }
    }

    /// # Safety
    /// Anything allocated from it must be stored in this GinScanWork.
    pub unsafe fn kcx(&self) -> Mcx<'static> {
        // SAFETY: caller keeps allocations within this work's lifetime.
        unsafe { self.key_ctx.handle() }
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

#[cfg(test)]
mod owner_tests {
    use super::*;

    #[test]
    fn scan_work_owner_survives_moves_and_live_entry_drop() {
        // SAFETY: all borrowed vectors stay in work until its drop.
        let mut work = unsafe { GinScanWork::new() };
        // SAFETY: the entry and its vectors are stored in work.
        let kcx = unsafe { work.kcx() };
        let entry = unsafe { GinScanEntryData::new(kcx) }.unwrap();
        work.entries.push(mcx::alloc_in(kcx, entry).unwrap());
        let mut parked = Some(work);
        let mut work = parked.take().unwrap();
        work.entries[0].matchOffsets[0] = 17;
        assert_eq!(work.entries[0].matchOffsets[0], 17);
        drop(work);
    }
}
