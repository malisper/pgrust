use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use crate::{
    uint16, uint32, uint8, AttrNumber, BlockNumber, CommandId, Datum, OffsetNumber, Oid, Size,
    TransactionId,
};

pub type bits8 = uint8;
pub type Name = Option<Box<NameData>>;
pub type Form_pg_attribute = Option<Box<FormData_pg_attribute>>;
pub type HeapTupleHeader = Option<Box<HeapTupleHeaderData>>;
pub type HeapTuple = Option<Box<HeapTupleData>>;
pub type MinimalTuple = Option<Box<MinimalTupleData>>;
pub type IndexTuple = Option<Box<IndexTupleData>>;
pub type IndexAttributeBitMap = Option<Box<IndexAttributeBitMapData>>;
pub type TupleDesc = Option<Box<TupleDescData>>;

pub const RECORDOID: Oid = 2249;
/// `VOIDOID` — `void` pseudo-type (`pg_type_d.h:228`).
pub const VOIDOID: Oid = 2278;
pub const BOOLOID: Oid = 16;
pub const NAMEOID: Oid = 19;
pub const INT8OID: Oid = 20;
pub const INT2OID: Oid = 21;
pub const INT4OID: Oid = 23;
pub const TEXTOID: Oid = 25;
pub const OIDOID: Oid = 26;
pub const JSONOID: Oid = 114;
pub const JSONBOID: Oid = 3802;
pub const XMLOID: Oid = 142;
pub const FLOAT4OID: Oid = 700;
pub const FLOAT8OID: Oid = 701;
pub const UNKNOWNOID: Oid = 705;
pub const INT2VECTOROID: Oid = 22;
pub const OIDVECTOROID: Oid = 30;
pub const INT2ARRAYOID: Oid = 1005;
pub const TEXTARRAYOID: Oid = 1009;
pub const OIDARRAYOID: Oid = 1028;
pub const BITOID: Oid = 1560;
pub const NUMERICOID: Oid = 1700;
pub const CSTRINGOID: Oid = 2275;
pub const VARCHAROID: Oid = 1043;
pub const DATEOID: Oid = 1082;
pub const TIDOID: Oid = 27;
pub const XIDOID: Oid = 28;
pub const CIDOID: Oid = 29;
/// `refcursor` — reference to a cursor (portal name); uses `text`'s I/O routines
/// (`catalog/pg_type.dat`, oid 1790).
pub const REFCURSOROID: Oid = 1790;

/// Default array element delimiter (`','`, `catalog/pg_type.h`).
pub const DEFAULT_TYPDELIM: i8 = b',' as i8;

// pg_attribute.attgenerated values are defined canonically in `access.rs`
// (ATTRIBUTE_GENERATED_STORED / _VIRTUAL); not redefined here (merge-time
// ambiguous-glob collision when catalog-core + commands-ddl landed).

// CheckAttributeType() flags (`catalog/heap.h`).
pub const CHKATYPE_ANYARRAY: i32 = 1 << 0;
pub const CHKATYPE_ANYRECORD: i32 = 1 << 1;
pub const CHKATYPE_IS_PARTKEY: i32 = 1 << 2;
pub const CHKATYPE_IS_VIRTUAL: i32 = 1 << 3;

/// `pg_type.typtype` value for a domain type (`'d'`, pg_type.h).
pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;
pub const TIMEOID: Oid = 1083;
pub const TIMESTAMPOID: Oid = 1114;
pub const TIMESTAMPTZOID: Oid = 1184;
pub const TIMETZOID: Oid = 1266;
pub const DEFAULT_COLLATION_OID: Oid = 100;

// Additional built-in type OIDs used by the SQL/XML type-name mapping in
// `backend-utils-adt-xml` (see `catalog/pg_type.dat`).  These are fixed,
// catalog-stable OIDs.
pub const BYTEAOID: Oid = 17;
pub const BPCHAROID: Oid = 1042;
// DATEOID/TIMEOID/TIMESTAMPOID/TIMESTAMPTZOID/TIMETZOID/NUMERICOID already
// defined above (the merge brought a second copy from the xml branch).

// Pseudo-type + handler OIDs used by the backend-commands ports
// (conversioncmds/aggregatecmds/operatorcmds/proclang/amcmds).  Values from
// `catalog/pg_type.dat`.  (The polymorphic-type OIDs ANYELEMENTOID/ANYARRAYOID/
// … are already defined in `funccache.rs`.)
pub const INTERNALOID: Oid = 2281;
/// `TSQUERYOID` — `tsquery` (`catalog/pg_type.dat`), used by tsearchcmds.c's
/// `prsheadline` parser-support-function signature check.
pub const TSQUERYOID: Oid = 3615;
// Pseudo-type OIDs used by functioncmds.c's VARIADIC / shell-type checks
// (`catalog/pg_type.dat`).
pub const ANYOID: Oid = 2276;
// VOIDOID is already defined earlier in this module (= 2278); not redefined here.
pub const CHAROID: Oid = 18;
// Built-in procedural-language OIDs (`catalog/pg_language.dat`, verified vs
// build-rust/.../pg_language_d.h: INTERNAL=12, C=13, SQL=14).
pub const INTERNALlanguageId: Oid = 12;
pub const ClanguageId: Oid = 13;
pub const SQLlanguageId: Oid = 14;
pub const LANGUAGE_HANDLEROID: Oid = 2280;
pub const FDW_HANDLEROID: Oid = 3115;
pub const INDEX_AM_HANDLEROID: Oid = 325;
pub const TABLE_AM_HANDLEROID: Oid = 269;
pub const TSM_HANDLEROID: Oid = 3310;

// pg_type.typtype values (`catalog/pg_type.h`) used by aggregatecmds.
pub const TYPTYPE_BASE: i8 = b'b' as i8;
pub const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
pub const TYPTYPE_ENUM: i8 = b'e' as i8;
pub const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
pub const TYPTYPE_PSEUDO: i8 = b'p' as i8;
pub const TYPTYPE_RANGE: i8 = b'r' as i8;

// pg_aggregate.aggkind values (`catalog/pg_aggregate.h`).
pub const AGGKIND_NORMAL: i8 = b'n' as i8;
pub const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
pub const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

// pg_aggregate aggfinalmodify/aggmfinalmodify values
// (`catalog/pg_aggregate.h`).
pub const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;
pub const AGGMODIFY_SHAREABLE: i8 = b's' as i8;
pub const AGGMODIFY_READ_WRITE: i8 = b'w' as i8;

// pg_proc.proparallel values (`catalog/pg_proc.h`).
pub const PROPARALLEL_SAFE: i8 = b's' as i8;
pub const PROPARALLEL_RESTRICTED: i8 = b'r' as i8;
pub const PROPARALLEL_UNSAFE: i8 = b'u' as i8;

// pg_proc.provolatile values (`catalog/pg_proc.h`).
pub const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
pub const PROVOLATILE_STABLE: i8 = b's' as i8;
pub const PROVOLATILE_VOLATILE: i8 = b'v' as i8;

// pg_proc.prokind values (`catalog/pg_proc.h`).
pub const PROKIND_FUNCTION: i8 = b'f' as i8;
pub const PROKIND_AGGREGATE: i8 = b'a' as i8;
pub const PROKIND_WINDOW: i8 = b'w' as i8;
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

pub const TYPALIGN_CHAR: i8 = b'c' as i8;
pub const TYPALIGN_SHORT: i8 = b's' as i8;
pub const TYPALIGN_INT: i8 = b'i' as i8;
pub const TYPALIGN_DOUBLE: i8 = b'd' as i8;
pub const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
pub const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
pub const TYPSTORAGE_MAIN: i8 = b'm' as i8;
pub const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;
pub const InvalidCompressionMethod: i8 = 0;

pub const ATTNULLABLE_UNRESTRICTED: i8 = b'f' as i8;
pub const ATTNULLABLE_UNKNOWN: i8 = b'u' as i8;
pub const ATTNULLABLE_VALID: i8 = b'v' as i8;

pub const MaxTupleAttributeNumber: i32 = 1664;
pub const MaxHeapAttributeNumber: i32 = 1600;

pub const HEAP_HASNULL: uint16 = 0x0001;
pub const HEAP_HASVARWIDTH: uint16 = 0x0002;
pub const HEAP_HASEXTERNAL: uint16 = 0x0004;
pub const HEAP_HASOID_OLD: uint16 = 0x0008;
pub const HEAP_XMAX_KEYSHR_LOCK: uint16 = 0x0010;
pub const HEAP_COMBOCID: uint16 = 0x0020;
pub const HEAP_XMAX_EXCL_LOCK: uint16 = 0x0040;
pub const HEAP_XMAX_LOCK_ONLY: uint16 = 0x0080;
pub const HEAP_XMIN_COMMITTED: uint16 = 0x0100;
pub const HEAP_XMIN_INVALID: uint16 = 0x0200;
pub const HEAP_XMIN_FROZEN: uint16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
pub const HEAP_XMAX_COMMITTED: uint16 = 0x0400;
pub const HEAP_XMAX_INVALID: uint16 = 0x0800;
pub const HEAP_XMAX_IS_MULTI: uint16 = 0x1000;
pub const HEAP_UPDATED: uint16 = 0x2000;
pub const HEAP_MOVED_OFF: uint16 = 0x4000;
pub const HEAP_MOVED_IN: uint16 = 0x8000;
pub const HEAP_MOVED: uint16 = HEAP_MOVED_OFF | HEAP_MOVED_IN;
pub const HEAP_XACT_MASK: uint16 = 0xFFF0;

pub const HEAP_NATTS_MASK: uint16 = 0x07FF;
pub const HEAP_KEYS_UPDATED: uint16 = 0x2000;
pub const HEAP_HOT_UPDATED: uint16 = 0x4000;
pub const HEAP_ONLY_TUPLE: uint16 = 0x8000;
pub const HEAP2_XACT_MASK: uint16 = 0xE000;
pub const HEAP_TUPLE_HAS_MATCH: uint16 = HEAP_ONLY_TUPLE;

pub const SelfItemPointerAttributeNumber: AttrNumber = -1;
pub const MinTransactionIdAttributeNumber: AttrNumber = -2;
pub const MinCommandIdAttributeNumber: AttrNumber = -3;
pub const MaxTransactionIdAttributeNumber: AttrNumber = -4;
pub const MaxCommandIdAttributeNumber: AttrNumber = -5;
pub const TableOidAttributeNumber: AttrNumber = -6;
/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h:27): the most-negative
/// system attribute number, one below `TableOidAttributeNumber`.  Used to map an
/// `attnum` into a 0-based `Bitmapset` index (see `pg_column_is_updatable`,
/// `rewriteHandler`, `tupconvert`).
pub const FirstLowInvalidHeapAttributeNumber: AttrNumber = TableOidAttributeNumber - 1;
// access/sysattr.h:27 fixes this at (-7); lock it so the system-attr ladder
// cannot drift.
const _: () = assert!(FirstLowInvalidHeapAttributeNumber == -7);

pub const VARHDRSZ: usize = core::mem::size_of::<i32>();
pub const HIGHBIT: i32 = 0x80;
pub const MINIMAL_TUPLE_OFFSET: usize = 8;
/// `ATTRIBUTE_FIXED_PART_SIZE` (access/tupdesc.h): the on-disk size of the
/// fixed-width part of `FormData_pg_attribute`, i.e. through `attcollation`.
/// The C macro computes `offsetof(FormData_pg_attribute, attcollation) +
/// sizeof(Oid)`; on the catalog ABI this is a fixed 100 bytes.
pub const ATTRIBUTE_FIXED_PART_SIZE: usize = 100;
pub const INDEX_SIZE_MASK: uint16 = 0x1FFF;
pub const INDEX_AM_RESERVED_BIT: uint16 = 0x2000;
pub const INDEX_VAR_MASK: uint16 = 0x4000;
pub const INDEX_NULL_MASK: uint16 = 0x8000;
pub const INDEX_ATTRIBUTE_BITMAP_BYTES: usize = (crate::INDEX_MAX_KEYS as usize + 8 - 1) / 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameData {
    pub data: [u8; crate::NAMEDATALEN as usize],
}

impl NameData {
    /// `NameStr(name)` — the name's bytes up to (but not including) the first
    /// NUL terminator. A `NameData` is a fixed-size, NUL-padded C string.
    pub fn name_str(&self) -> &[u8] {
        let len = self
            .data
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.data.len());
        &self.data[..len]
    }
}

impl Default for NameData {
    fn default() -> Self {
        Self {
            data: [0; crate::NAMEDATALEN as usize],
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FormData_pg_attribute {
    pub attrelid: Oid,
    pub attname: NameData,
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnum: i16,
    pub atttypmod: i32,
    pub attndims: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcompression: i8,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: i8,
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i16,
    pub attcollation: Oid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockIdData {
    pub bi_hi: uint16,
    pub bi_lo: uint16,
}

impl BlockIdData {
    pub const fn new(block_number: BlockNumber) -> Self {
        Self {
            bi_hi: (block_number >> 16) as uint16,
            bi_lo: (block_number & 0xffff) as uint16,
        }
    }

    pub const fn block_number(&self) -> BlockNumber {
        ((self.bi_hi as BlockNumber) << 16) | self.bi_lo as BlockNumber
    }

    pub fn set_block_number(&mut self, block_number: BlockNumber) {
        *self = Self::new(block_number);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ItemPointerData {
    pub ip_blkid: BlockIdData,
    pub ip_posid: OffsetNumber,
}

impl ItemPointerData {
    pub const fn new(block_number: BlockNumber, offset_number: OffsetNumber) -> Self {
        Self {
            ip_blkid: BlockIdData::new(block_number),
            ip_posid: offset_number,
        }
    }
}

/// Was a C `union` of `t_heap` / `t_datum`; rewritten as a Rust enum.
#[derive(Clone, Debug)]
pub enum HeapTupleHeaderChoice {
    THeap(HeapTupleFields),
    TDatum(DatumTupleFields),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeapTupleFields {
    pub t_xmin: TransactionId,
    pub t_xmax: TransactionId,
    pub t_field3: HeapTupleField3,
}

/// Was a C `union` of `t_cid` / `t_xvac`; rewritten as a Rust enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeapTupleField3 {
    TCid(CommandId),
    TXvac(TransactionId),
}

impl Default for HeapTupleField3 {
    fn default() -> Self {
        Self::TCid(0)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatumTupleFields {
    pub datum_len_: i32,
    pub datum_typmod: i32,
    pub datum_typeid: Oid,
}

#[derive(Clone, Debug)]
pub struct HeapTupleHeaderData {
    pub t_choice: HeapTupleHeaderChoice,
    pub t_ctid: ItemPointerData,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    pub t_bits: Vec<bits8>,
}

#[derive(Clone, Debug, Default)]
pub struct MinimalTupleData {
    pub t_len: uint32,
    pub mt_padding: [i8; 6],
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    pub t_bits: Vec<bits8>,
}

#[derive(Clone, Debug)]
pub struct HeapTupleData {
    pub t_len: uint32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: HeapTupleHeader,
    /// The post-`t_hoff` user-data (column) bytes of the tuple, when this
    /// `HeapTupleData` is one freshly formed by `heap_form_tuple` and intended
    /// to be serialized back onto a page.
    ///
    /// In C, the header (`t_data`, including `t_bits`), optional alignment pad,
    /// and the column bytes are one contiguous `palloc` chunk reached through
    /// the `t_data` pointer.  The idiomatic owned model splits the *header*
    /// (here `t_data`: the fixed `HeapTupleHeaderData` + its `t_bits` null
    /// bitmap) from the *column bytes*; this side-channel carries the latter so
    /// the whole tuple (header + columns) can cross the page-write seam
    /// (`page_add_item` / `relation_put_heap_tuple`) and be laid down on disk by
    /// [`crate::backend_access_common_heaptuple`]-side
    /// `heap_tuple_to_disk_image`.
    ///
    /// `None` for a *page-resident* / decoded-header tuple (the scan, fetch,
    /// freeze, prune, and visibility paths): those tuples are header-only by
    /// design and are never re-serialized through the page-write seam.  Adding
    /// the field is purely additive — every existing construction site keeps
    /// `None`.
    pub t_user_data: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
    pub t_info: uint16,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexAttributeBitMapData {
    pub bits: [bits8; INDEX_ATTRIBUTE_BITMAP_BYTES],
}

#[derive(Clone, Debug)]
pub struct AttrDefault {
    pub adnum: AttrNumber,
    pub adbin: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ConstrCheck {
    pub ccname: Option<String>,
    pub ccbin: Option<String>,
    pub ccenforced: bool,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
}

#[derive(Clone, Debug)]
pub struct TupleConstr {
    pub defval: Vec<AttrDefault>,
    pub check: Vec<ConstrCheck>,
    pub missing: Vec<AttrMissing>,
    pub num_defval: uint16,
    pub num_check: uint16,
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttrMissing {
    pub am_present: bool,
    pub am_value: Datum,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactAttribute {
    pub attcacheoff: i32,
    pub attlen: i16,
    pub attbyval: bool,
    pub attispackable: bool,
    pub atthasmissing: bool,
    pub attisdropped: bool,
    pub attgenerated: bool,
    pub attnullability: i8,
    pub attalignby: uint8,
}

#[derive(Clone, Debug)]
pub struct TupleDescData {
    pub natts: i32,
    pub tdtypeid: Oid,
    pub tdtypmod: i32,
    pub tdrefcount: i32,
    pub constr: Option<Box<TupleConstr>>,
    pub compact_attrs: Vec<CompactAttribute>,
    /// The full `FormData_pg_attribute[]` flexible array that PG18's
    /// `TupleDescData` carries alongside `compact_attrs`
    /// (`access/tupdesc.h`).  In C this is the trailing flexible array reached
    /// by `TupleDescAttr(td, i)`; here it is an owned parallel `Vec` kept in
    /// lock-step with `compact_attrs` (one entry per attribute).  Consumers
    /// that only need `attlen/attbyval/attalign` can still use
    /// `compact_attrs`; consumers needing full attribute fields
    /// (`atttypid`, `attstorage`, `attcollation`, `attname`, ...) read this
    /// via [`TupleDescAttr`].  `populate_compact_attribute` derives the
    /// matching `compact_attrs[i]` from `attrs[i]`.
    pub attrs: Vec<FormData_pg_attribute>,
}

impl TupleDescData {
    /// `TupleDescAttr(tupdesc, i)` (`access/tupdesc.h`) — the `i`-th full
    /// `Form_pg_attribute` (0-based).
    pub fn attr(&self, i: usize) -> &FormData_pg_attribute {
        &self.attrs[i]
    }

    /// Mutable `TupleDescAttr(tupdesc, i)`.
    pub fn attr_mut(&mut self, i: usize) -> &mut FormData_pg_attribute {
        &mut self.attrs[i]
    }

    /// `TupleDescCompactAttr(tupdesc, i)` (`access/tupdesc.h`) — the `i`-th
    /// `CompactAttribute` (0-based).
    pub fn compact_attr(&self, i: usize) -> &CompactAttribute {
        &self.compact_attrs[i]
    }
}

pub const fn BITMAPLEN(natts: i32) -> i32 {
    (natts + 7) / 8
}

pub const fn HeapTupleHeaderGetNatts(tup: &HeapTupleHeaderData) -> uint16 {
    tup.t_infomask2 & HEAP_NATTS_MASK
}

pub fn HeapTupleHeaderSetNatts(tup: &mut HeapTupleHeaderData, natts: uint16) {
    tup.t_infomask2 = (tup.t_infomask2 & !HEAP_NATTS_MASK) | (natts & HEAP_NATTS_MASK);
}

pub const fn HeapTupleHeaderHasExternal(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask & HEAP_HASEXTERNAL) != 0
}

pub const fn HeapTupleHeaderXminCommitted(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask & HEAP_XMIN_COMMITTED) != 0
}

pub fn HeapTupleHeaderGetRawXmin(tup: &HeapTupleHeaderData) -> TransactionId {
    match &tup.t_choice {
        HeapTupleHeaderChoice::THeap(t_heap) => t_heap.t_xmin,
        HeapTupleHeaderChoice::TDatum(_) => 0,
    }
}

pub fn HeapTupleHeaderGetRawCommandId(tup: &HeapTupleHeaderData) -> CommandId {
    match &tup.t_choice {
        HeapTupleHeaderChoice::THeap(t_heap) => match t_heap.t_field3 {
            HeapTupleField3::TCid(t_cid) => t_cid,
            HeapTupleField3::TXvac(_) => 0,
        },
        HeapTupleHeaderChoice::TDatum(_) => 0,
    }
}

pub const fn IndexTupleSize(itup: &IndexTupleData) -> Size {
    (itup.t_info & INDEX_SIZE_MASK) as Size
}

pub const fn IndexTupleHasNulls(itup: &IndexTupleData) -> bool {
    (itup.t_info & INDEX_NULL_MASK) != 0
}

pub const fn IndexTupleHasVarwidths(itup: &IndexTupleData) -> bool {
    (itup.t_info & INDEX_VAR_MASK) != 0
}

pub fn HeapTupleHasNulls(tuple: &HeapTupleData) -> bool {
    match &tuple.t_data {
        Some(t_data) => (t_data.t_infomask & HEAP_HASNULL) != 0,
        None => false,
    }
}

pub fn HeapTupleNoNulls(tuple: &HeapTupleData) -> bool {
    !HeapTupleHasNulls(tuple)
}
