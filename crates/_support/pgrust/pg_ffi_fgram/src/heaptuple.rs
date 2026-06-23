use core::ffi::{c_char, c_int};

use crate::{
    uint16, uint32, uint8, AttrNumber, BlockNumber, CommandId, Datum, OffsetNumber, Oid, Size,
    TransactionId,
};

pub type bits8 = uint8;
pub type Name = *mut NameData;
pub type Form_pg_attribute = *mut FormData_pg_attribute;
pub type HeapTupleHeader = *mut HeapTupleHeaderData;
pub type HeapTuple = *mut HeapTupleData;
pub type MinimalTuple = *mut MinimalTupleData;
pub type IndexTuple = *mut IndexTupleData;
pub type IndexAttributeBitMap = *mut IndexAttributeBitMapData;
pub type TupleDesc = *mut TupleDescData;

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
pub const DEFAULT_TYPDELIM: c_char = b',' as c_char;

// pg_attribute.attgenerated values are defined canonically in `access.rs`
// (ATTRIBUTE_GENERATED_STORED / _VIRTUAL); not redefined here (merge-time
// ambiguous-glob collision when catalog-core + commands-ddl landed).

// CheckAttributeType() flags (`catalog/heap.h`).
pub const CHKATYPE_ANYARRAY: c_int = 1 << 0;
pub const CHKATYPE_ANYRECORD: c_int = 1 << 1;
pub const CHKATYPE_IS_PARTKEY: c_int = 1 << 2;
pub const CHKATYPE_IS_VIRTUAL: c_int = 1 << 3;

/// `pg_type.typtype` value for a domain type (`'d'`, pg_type.h).
pub const TYPTYPE_DOMAIN: core::ffi::c_char = b'd' as core::ffi::c_char;
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
pub const TYPTYPE_BASE: c_char = b'b' as c_char;
pub const TYPTYPE_COMPOSITE: c_char = b'c' as c_char;
pub const TYPTYPE_ENUM: c_char = b'e' as c_char;
pub const TYPTYPE_MULTIRANGE: c_char = b'm' as c_char;
pub const TYPTYPE_PSEUDO: c_char = b'p' as c_char;
pub const TYPTYPE_RANGE: c_char = b'r' as c_char;

// pg_aggregate.aggkind values (`catalog/pg_aggregate.h`).
pub const AGGKIND_NORMAL: c_char = b'n' as c_char;
pub const AGGKIND_ORDERED_SET: c_char = b'o' as c_char;
pub const AGGKIND_HYPOTHETICAL: c_char = b'h' as c_char;

// pg_aggregate aggfinalmodify/aggmfinalmodify values
// (`catalog/pg_aggregate.h`).
pub const AGGMODIFY_READ_ONLY: c_char = b'r' as c_char;
pub const AGGMODIFY_SHAREABLE: c_char = b's' as c_char;
pub const AGGMODIFY_READ_WRITE: c_char = b'w' as c_char;

// pg_proc.proparallel values (`catalog/pg_proc.h`).
pub const PROPARALLEL_SAFE: c_char = b's' as c_char;
pub const PROPARALLEL_RESTRICTED: c_char = b'r' as c_char;
pub const PROPARALLEL_UNSAFE: c_char = b'u' as c_char;

// pg_proc.provolatile values (`catalog/pg_proc.h`).
pub const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;
pub const PROVOLATILE_STABLE: c_char = b's' as c_char;
pub const PROVOLATILE_VOLATILE: c_char = b'v' as c_char;

// pg_proc.prokind values (`catalog/pg_proc.h`).
pub const PROKIND_FUNCTION: c_char = b'f' as c_char;
pub const PROKIND_AGGREGATE: c_char = b'a' as c_char;
pub const PROKIND_WINDOW: c_char = b'w' as c_char;
pub const PROKIND_PROCEDURE: c_char = b'p' as c_char;

pub const TYPALIGN_CHAR: c_char = b'c' as c_char;
pub const TYPALIGN_SHORT: c_char = b's' as c_char;
pub const TYPALIGN_INT: c_char = b'i' as c_char;
pub const TYPALIGN_DOUBLE: c_char = b'd' as c_char;
pub const TYPSTORAGE_PLAIN: c_char = b'p' as c_char;
pub const TYPSTORAGE_EXTERNAL: c_char = b'e' as c_char;
pub const TYPSTORAGE_MAIN: c_char = b'm' as c_char;
pub const TYPSTORAGE_EXTENDED: c_char = b'x' as c_char;
pub const InvalidCompressionMethod: c_char = 0;

pub const ATTNULLABLE_UNRESTRICTED: c_char = b'f' as c_char;
pub const ATTNULLABLE_UNKNOWN: c_char = b'u' as c_char;
pub const ATTNULLABLE_VALID: c_char = b'v' as c_char;

pub const MaxTupleAttributeNumber: c_int = 1664;
pub const MaxHeapAttributeNumber: c_int = 1600;

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
pub const HIGHBIT: c_int = 0x80;
pub const MINIMAL_TUPLE_OFFSET: usize = 8;
pub const ATTRIBUTE_FIXED_PART_SIZE: usize =
    core::mem::offset_of!(FormData_pg_attribute, attcollation) + core::mem::size_of::<Oid>();
pub const INDEX_SIZE_MASK: uint16 = 0x1FFF;
pub const INDEX_AM_RESERVED_BIT: uint16 = 0x2000;
pub const INDEX_VAR_MASK: uint16 = 0x4000;
pub const INDEX_NULL_MASK: uint16 = 0x8000;
pub const INDEX_ATTRIBUTE_BITMAP_BYTES: usize = (crate::INDEX_MAX_KEYS as usize + 8 - 1) / 8;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameData {
    pub data: [c_char; crate::NAMEDATALEN as usize],
}

impl NameData {
    /// `NameStr(name)` — the name's bytes up to (but not including) the first
    /// NUL terminator. A `NameData` is a fixed-size, NUL-padded C string; this
    /// mirrors treating `&name->data[0]` as a `char *`.
    pub fn name_str(&self) -> &[u8] {
        let bytes: &[u8] = unsafe {
            core::slice::from_raw_parts(self.data.as_ptr().cast::<u8>(), self.data.len())
        };
        let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        &bytes[..len]
    }
}

impl Default for NameData {
    fn default() -> Self {
        Self {
            data: [0; crate::NAMEDATALEN as usize],
        }
    }
}

#[repr(C)]
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
    pub attalign: c_char,
    pub attstorage: c_char,
    pub attcompression: c_char,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: c_char,
    pub attgenerated: c_char,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i16,
    pub attcollation: Oid,
}

#[repr(C)]
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

#[repr(C)]
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

#[repr(C)]
#[derive(Clone, Copy)]
pub union HeapTupleHeaderChoice {
    pub t_heap: HeapTupleFields,
    pub t_datum: DatumTupleFields,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeapTupleFields {
    pub t_xmin: TransactionId,
    pub t_xmax: TransactionId,
    pub t_field3: HeapTupleField3,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union HeapTupleField3 {
    pub t_cid: CommandId,
    pub t_xvac: TransactionId,
}

impl Default for HeapTupleField3 {
    fn default() -> Self {
        Self { t_cid: 0 }
    }
}

impl core::fmt::Debug for HeapTupleField3 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HeapTupleField3").finish_non_exhaustive()
    }
}

impl PartialEq for HeapTupleField3 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for HeapTupleField3 {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatumTupleFields {
    pub datum_len_: i32,
    pub datum_typmod: i32,
    pub datum_typeid: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapTupleHeaderData {
    pub t_choice: HeapTupleHeaderChoice,
    pub t_ctid: ItemPointerData,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    pub t_bits: [bits8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MinimalTupleData {
    pub t_len: uint32,
    pub mt_padding: [c_char; 6],
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    pub t_bits: [bits8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HeapTupleData {
    pub t_len: uint32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: HeapTupleHeader,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
    pub t_info: uint16,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexAttributeBitMapData {
    pub bits: [bits8; INDEX_ATTRIBUTE_BITMAP_BYTES],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AttrDefault {
    pub adnum: AttrNumber,
    pub adbin: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ConstrCheck {
    pub ccname: *mut c_char,
    pub ccbin: *mut c_char,
    pub ccenforced: bool,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupleConstr {
    pub defval: *mut AttrDefault,
    pub check: *mut ConstrCheck,
    pub missing: *mut AttrMissing,
    pub num_defval: uint16,
    pub num_check: uint16,
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttrMissing {
    pub am_present: bool,
    pub am_value: Datum,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactAttribute {
    pub attcacheoff: i32,
    pub attlen: i16,
    pub attbyval: bool,
    pub attispackable: bool,
    pub atthasmissing: bool,
    pub attisdropped: bool,
    pub attgenerated: bool,
    pub attnullability: c_char,
    pub attalignby: uint8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupleDescData {
    pub natts: c_int,
    pub tdtypeid: Oid,
    pub tdtypmod: i32,
    pub tdrefcount: c_int,
    pub constr: *mut TupleConstr,
    pub compact_attrs: [CompactAttribute; 0],
}

impl TupleDescData {
    /// Return a compact attribute pointer from a valid PostgreSQL `TupleDesc`.
    ///
    /// # Safety
    ///
    /// The descriptor must contain at least `index + 1` compact attributes.
    pub unsafe fn compact_attr_ptr(&self, index: usize) -> *const CompactAttribute {
        unsafe { self.compact_attrs.as_ptr().add(index) }
    }
}

pub const fn BITMAPLEN(natts: c_int) -> c_int {
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
    unsafe { tup.t_choice.t_heap.t_xmin }
}

pub fn HeapTupleHeaderGetRawCommandId(tup: &HeapTupleHeaderData) -> CommandId {
    unsafe { tup.t_choice.t_heap.t_field3.t_cid }
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

pub const fn IndexInfoFindDataOffset(t_info: uint16) -> Size {
    if (t_info & INDEX_NULL_MASK) == 0 {
        max_align(core::mem::size_of::<IndexTupleData>())
    } else {
        max_align(
            core::mem::size_of::<IndexTupleData>()
                + core::mem::size_of::<IndexAttributeBitMapData>(),
        )
    }
}

const fn max_align(value: usize) -> usize {
    (value + 8 - 1) & !(8 - 1)
}

pub const fn HeapTupleHasNulls(tuple: &HeapTupleData) -> bool {
    unsafe { !tuple.t_data.is_null() && ((*tuple.t_data).t_infomask & HEAP_HASNULL) != 0 }
}

pub const fn HeapTupleNoNulls(tuple: &HeapTupleData) -> bool {
    !HeapTupleHasNulls(tuple)
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn heaptuple_layout_matches_pg_abi() {
        assert_eq!(size_of::<BlockIdData>(), 4);
        assert_eq!(size_of::<ItemPointerData>(), 6);
        assert_eq!(align_of::<ItemPointerData>(), 2);
        assert_eq!(size_of::<HeapTupleFields>(), 12);
        assert_eq!(size_of::<DatumTupleFields>(), 12);
        assert_eq!(size_of::<HeapTupleHeaderData>(), 24);
        assert_eq!(offset_of!(HeapTupleHeaderData, t_ctid), 12);
        assert_eq!(offset_of!(HeapTupleHeaderData, t_infomask2), 18);
        assert_eq!(offset_of!(HeapTupleHeaderData, t_infomask), 20);
        assert_eq!(offset_of!(HeapTupleHeaderData, t_hoff), 22);
        assert_eq!(size_of::<MinimalTupleData>(), 16);
        assert_eq!(offset_of!(MinimalTupleData, t_infomask2), 10);
        assert_eq!(offset_of!(MinimalTupleData, t_infomask), 12);
        assert_eq!(offset_of!(MinimalTupleData, t_hoff), 14);
        assert_eq!(size_of::<HeapTupleData>(), 24);
        assert_eq!(align_of::<HeapTupleData>(), 8);
        assert_eq!(offset_of!(HeapTupleData, t_len), 0);
        assert_eq!(offset_of!(HeapTupleData, t_self), 4);
        assert_eq!(offset_of!(HeapTupleData, t_tableOid), 12);
        assert_eq!(offset_of!(HeapTupleData, t_data), 16);
        assert_eq!(size_of::<IndexTupleData>(), 8);
        assert_eq!(align_of::<IndexTupleData>(), 2);
        assert_eq!(offset_of!(IndexTupleData, t_tid), 0);
        assert_eq!(offset_of!(IndexTupleData, t_info), 6);
        assert_eq!(size_of::<IndexAttributeBitMapData>(), 4);
        assert_eq!(IndexInfoFindDataOffset(0), 8);
        assert_eq!(IndexInfoFindDataOffset(INDEX_NULL_MASK), 16);
        assert_eq!(size_of::<NameData>(), 64);
        assert_eq!(size_of::<FormData_pg_attribute>(), 100);
        assert_eq!(align_of::<FormData_pg_attribute>(), 4);
        assert_eq!(offset_of!(FormData_pg_attribute, attrelid), 0);
        assert_eq!(offset_of!(FormData_pg_attribute, attname), 4);
        assert_eq!(offset_of!(FormData_pg_attribute, atttypid), 68);
        assert_eq!(offset_of!(FormData_pg_attribute, attcollation), 96);
        assert_eq!(ATTRIBUTE_FIXED_PART_SIZE, 100);
        assert_eq!(size_of::<CompactAttribute>(), 16);
        assert_eq!(offset_of!(TupleDescData, compact_attrs), 24);
    }
}
