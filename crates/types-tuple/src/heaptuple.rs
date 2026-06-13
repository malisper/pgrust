use mcx::{alloc_in, slice_in, Mcx, PgBox, PgString, PgVec};
use types_core::{
    uint16, uint32, uint8, AttrNumber, BlockNumber, CommandId, OffsetNumber, Oid, Size,
    TransactionId,
};
use types_error::PgResult;

use crate::backend_access_common_heaptuple::TupleValue;

pub type bits8 = uint8;
// In C these are bare pointers to palloc'd structs; here the box allocates in
// (and cannot outlive) the memory context whose `'mcx` it carries.
pub type Name<'mcx> = Option<PgBox<'mcx, NameData>>;
pub type Form_pg_attribute<'mcx> = Option<PgBox<'mcx, FormData_pg_attribute>>;
pub type HeapTupleHeader<'mcx> = Option<PgBox<'mcx, HeapTupleHeaderData<'mcx>>>;
pub type HeapTuple<'mcx> = Option<PgBox<'mcx, HeapTupleData<'mcx>>>;
pub type MinimalTuple<'mcx> = Option<PgBox<'mcx, MinimalTupleData<'mcx>>>;
pub type IndexTuple<'mcx> = Option<PgBox<'mcx, IndexTupleData>>;
pub type IndexAttributeBitMap<'mcx> = Option<PgBox<'mcx, IndexAttributeBitMapData>>;
pub type TupleDesc<'mcx> = Option<PgBox<'mcx, TupleDescData<'mcx>>>;

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
/// `TSQUERYOID` — `tsquery` type OID (`pg_type_d.h`).
pub const TSQUERYOID: Oid = 3615;
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

pub const TYPALIGN_CHAR: i8 = b'c' as i8;
pub const TYPALIGN_SHORT: i8 = b's' as i8;
pub const TYPALIGN_INT: i8 = b'i' as i8;
pub const TYPALIGN_DOUBLE: i8 = b'd' as i8;
pub const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
pub const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
pub const TYPSTORAGE_MAIN: i8 = b'm' as i8;
pub const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;
pub const InvalidCompressionMethod: i8 = 0;

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
pub const INDEX_SIZE_MASK: uint16 = 0x1FFF;
pub const INDEX_AM_RESERVED_BIT: uint16 = 0x2000;
pub const INDEX_VAR_MASK: uint16 = 0x4000;
pub const INDEX_NULL_MASK: uint16 = 0x8000;
pub const INDEX_ATTRIBUTE_BITMAP_BYTES: usize = (types_core::INDEX_MAX_KEYS as usize + 8 - 1) / 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameData {
    pub data: [u8; types_core::NAMEDATALEN as usize],
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
            data: [0; types_core::NAMEDATALEN as usize],
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
pub struct HeapTupleHeaderData<'mcx> {
    pub t_choice: HeapTupleHeaderChoice,
    pub t_ctid: ItemPointerData,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    pub t_bits: PgVec<'mcx, bits8>,
}

impl HeapTupleHeaderData<'_> {
    /// Deep copy into `mcx` (C: part of the tuple-`memcpy` into the caller's
    /// current context). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<HeapTupleHeaderData<'b>> {
        Ok(HeapTupleHeaderData {
            t_choice: self.t_choice.clone(),
            t_ctid: self.t_ctid,
            t_infomask2: self.t_infomask2,
            t_infomask: self.t_infomask,
            t_hoff: self.t_hoff,
            t_bits: slice_in(mcx, &self.t_bits)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct MinimalTupleData<'mcx> {
    pub t_len: uint32,
    pub mt_padding: [i8; 6],
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
    pub t_bits: PgVec<'mcx, bits8>,
}

impl MinimalTupleData<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MinimalTupleData<'b>> {
        Ok(MinimalTupleData {
            t_len: self.t_len,
            mt_padding: self.mt_padding,
            t_infomask2: self.t_infomask2,
            t_infomask: self.t_infomask,
            t_hoff: self.t_hoff,
            t_bits: slice_in(mcx, &self.t_bits)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct HeapTupleData<'mcx> {
    pub t_len: uint32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: HeapTupleHeader<'mcx>,
}

impl HeapTupleData<'_> {
    /// Deep copy into `mcx` (C: `heap_copytuple`'s `memcpy` of the whole
    /// contiguous block into the caller's current context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<HeapTupleData<'b>> {
        Ok(HeapTupleData {
            t_len: self.t_len,
            t_self: self.t_self,
            t_tableOid: self.t_tableOid,
            t_data: match &self.t_data {
                Some(hdr) => Some(alloc_in(mcx, hdr.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
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

#[derive(Debug)]
pub struct AttrDefault<'mcx> {
    pub adnum: AttrNumber,
    pub adbin: Option<PgString<'mcx>>,
}

impl AttrDefault<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AttrDefault<'b>> {
        Ok(AttrDefault {
            adnum: self.adnum,
            adbin: clone_opt_string_in(&self.adbin, mcx)?,
        })
    }
}

#[derive(Debug)]
pub struct ConstrCheck<'mcx> {
    pub ccname: Option<PgString<'mcx>>,
    pub ccbin: Option<PgString<'mcx>>,
    pub ccenforced: bool,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
}

impl ConstrCheck<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ConstrCheck<'b>> {
        Ok(ConstrCheck {
            ccname: clone_opt_string_in(&self.ccname, mcx)?,
            ccbin: clone_opt_string_in(&self.ccbin, mcx)?,
            ccenforced: self.ccenforced,
            ccvalid: self.ccvalid,
            ccnoinherit: self.ccnoinherit,
        })
    }
}

fn clone_opt_string_in<'b>(s: &Option<PgString<'_>>, mcx: Mcx<'b>) -> PgResult<Option<PgString<'b>>> {
    match s {
        Some(s) => Ok(Some(s.clone_in(mcx)?)),
        None => Ok(None),
    }
}

#[derive(Debug)]
pub struct TupleConstr<'mcx> {
    pub defval: PgVec<'mcx, AttrDefault<'mcx>>,
    pub check: PgVec<'mcx, ConstrCheck<'mcx>>,
    pub missing: PgVec<'mcx, AttrMissing<'mcx>>,
    pub num_defval: uint16,
    pub num_check: uint16,
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

impl TupleConstr<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TupleConstr<'b>> {
        let mut defval = mcx::vec_with_capacity_in(mcx, self.defval.len())?;
        for d in &self.defval {
            defval.push(d.clone_in(mcx)?);
        }
        let mut check = mcx::vec_with_capacity_in(mcx, self.check.len())?;
        for c in &self.check {
            check.push(c.clone_in(mcx)?);
        }
        let mut missing = mcx::vec_with_capacity_in(mcx, self.missing.len())?;
        for m in &self.missing {
            missing.push(m.clone_in(mcx)?);
        }
        Ok(TupleConstr {
            defval,
            check,
            missing,
            num_defval: self.num_defval,
            num_check: self.num_check,
            has_not_null: self.has_not_null,
            has_generated_stored: self.has_generated_stored,
            has_generated_virtual: self.has_generated_virtual,
        })
    }
}

/// `AttrMissing` (`access/tupdesc.h`): one attribute's missing-value default.
///
/// C stores `am_value` as a bare `Datum`; for a pass-by-reference attribute
/// that Datum is a pointer whose pointee heaptuple.c keeps alive via its
/// file-static missing-values cache (`missing_hash`/`missing_match`/
/// `init_missing_cache` + `datumCopy` into `TopMemoryContext`). In the owned
/// model the value *is* its payload ([`TupleValue`]: a `ByVal` word or the
/// `ByRef` bytes), so the lifetime-extension cache dissolves
/// (`docs/mctx-design.md`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttrMissing<'mcx> {
    pub am_present: bool,
    pub am_value: TupleValue<'mcx>,
}

impl AttrMissing<'_> {
    /// Deep copy into `mcx`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AttrMissing<'b>> {
        Ok(AttrMissing {
            am_present: self.am_present,
            am_value: self.am_value.clone_in(mcx)?,
        })
    }
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

#[derive(Debug)]
pub struct TupleDescData<'mcx> {
    pub natts: i32,
    pub tdtypeid: Oid,
    pub tdtypmod: i32,
    pub tdrefcount: i32,
    pub constr: Option<PgBox<'mcx, TupleConstr<'mcx>>>,
    pub compact_attrs: PgVec<'mcx, CompactAttribute>,
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
    pub attrs: PgVec<'mcx, FormData_pg_attribute>,
}

impl TupleDescData<'_> {
    /// Deep copy into `mcx` (C: `CreateTupleDescCopyConstr` semantics — the
    /// descriptor plus its constraint payload, allocated in the caller's
    /// context).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TupleDescData<'b>> {
        Ok(TupleDescData {
            natts: self.natts,
            tdtypeid: self.tdtypeid,
            tdtypmod: self.tdtypmod,
            tdrefcount: self.tdrefcount,
            constr: match &self.constr {
                Some(c) => Some(alloc_in(mcx, c.clone_in(mcx)?)?),
                None => None,
            },
            compact_attrs: slice_in(mcx, &self.compact_attrs)?,
            attrs: slice_in(mcx, &self.attrs)?,
        })
    }
}

impl TupleDescData<'_> {
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

/// `HeapTupleHeaderGetTypeId(tup)` (`htup_details.h`) —
/// `tup->t_choice.t_datum.datum_typeid`. Only meaningful for a composite
/// Datum's header; panics if the header carries heap (xmin/xmax) fields
/// instead (C would read the other union arm's bytes).
pub fn HeapTupleHeaderGetTypeId(tup: &HeapTupleHeaderData) -> Oid {
    match &tup.t_choice {
        HeapTupleHeaderChoice::TDatum(d) => d.datum_typeid,
        HeapTupleHeaderChoice::THeap(_) => {
            panic!("HeapTupleHeaderGetTypeId: header is not a composite Datum")
        }
    }
}

/// `HeapTupleHeaderGetTypMod(tup)` (`htup_details.h`) —
/// `tup->t_choice.t_datum.datum_typmod`.
pub fn HeapTupleHeaderGetTypMod(tup: &HeapTupleHeaderData) -> i32 {
    match &tup.t_choice {
        HeapTupleHeaderChoice::TDatum(d) => d.datum_typmod,
        HeapTupleHeaderChoice::THeap(_) => {
            panic!("HeapTupleHeaderGetTypMod: header is not a composite Datum")
        }
    }
}

/// `HeapTupleHeaderGetDatumLength(tup)` (`htup_details.h`) — `VARSIZE(tup)`,
/// i.e. the composite Datum's `datum_len_` varlena length word.
pub fn HeapTupleHeaderGetDatumLength(tup: &HeapTupleHeaderData) -> i32 {
    match &tup.t_choice {
        HeapTupleHeaderChoice::TDatum(d) => d.datum_len_,
        HeapTupleHeaderChoice::THeap(_) => {
            panic!("HeapTupleHeaderGetDatumLength: header is not a composite Datum")
        }
    }
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

/// `HeapTupleHeaderGetRawXmin(tup)` (`htup_details.h`) —
/// `tup->t_choice.t_heap.t_xmin`. Only meaningful for a heap tuple's header;
/// panics if the header carries the composite-Datum union arm (C would read
/// the other arm's bytes — a caller bug there too).
pub fn HeapTupleHeaderGetRawXmin(tup: &HeapTupleHeaderData) -> TransactionId {
    match &tup.t_choice {
        HeapTupleHeaderChoice::THeap(t_heap) => t_heap.t_xmin,
        HeapTupleHeaderChoice::TDatum(_) => {
            panic!("HeapTupleHeaderGetRawXmin: header is a composite Datum")
        }
    }
}

/// `HeapTupleHeaderGetRawCommandId(tup)` (`htup_details.h`) —
/// `tup->t_choice.t_heap.t_field3.t_cid`. Panics on the TXvac / composite-Datum
/// arms (C would reinterpret the union bytes — a caller bug there too).
pub fn HeapTupleHeaderGetRawCommandId(tup: &HeapTupleHeaderData) -> CommandId {
    match &tup.t_choice {
        HeapTupleHeaderChoice::THeap(t_heap) => match t_heap.t_field3 {
            HeapTupleField3::TCid(t_cid) => t_cid,
            HeapTupleField3::TXvac(_) => {
                panic!("HeapTupleHeaderGetRawCommandId: t_field3 holds t_xvac")
            }
        },
        HeapTupleHeaderChoice::TDatum(_) => {
            panic!("HeapTupleHeaderGetRawCommandId: header is a composite Datum")
        }
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
