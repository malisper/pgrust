use ::mcx::{alloc_in, slice_in, Mcx, PgBox, PgString, PgVec};
use ::types_core::{
    uint16, uint32, uint8, AttrNumber, BlockNumber, CommandId, OffsetNumber, Oid, Size,
    TransactionId,
};
use ::types_error::PgResult;

pub use crate::common_heaptuple::*;

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
/// `RECORDARRAYOID` — `record[]` array type OID (`pg_type_d.h`).
pub const RECORDARRAYOID: Oid = 2287;
/// `VOIDOID` — `void` pseudo-type (`pg_type_d.h:228`).
pub const VOIDOID: Oid = 2278;
pub const BOOLOID: Oid = 16;
pub const BYTEAOID: Oid = 17;
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
/// `VARBITOID` — `varbit` (bit varying) type OID (`pg_type.dat`).
pub const VARBITOID: Oid = 1562;
pub const NUMERICOID: Oid = 1700;
pub const CSTRINGOID: Oid = 2275;
pub const VARCHAROID: Oid = 1043;
/// `BPCHAROID` — `bpchar` (blank-padded char) type OID (`pg_type.dat`).
pub const BPCHAROID: Oid = 1042;
pub const DATEOID: Oid = 1082;
/// `TIMEOID` — `time without time zone` type OID (`pg_type.dat`).
pub const TIMEOID: Oid = 1083;
/// `TIMETZOID` — `time with time zone` type OID (`pg_type.dat`).
pub const TIMETZOID: Oid = 1266;
/// `TIMESTAMPOID` — `timestamp without time zone` type OID (`pg_type.dat`).
pub const TIMESTAMPOID: Oid = 1114;
/// `TIMESTAMPTZOID` — `timestamp with time zone` type OID (`pg_type.dat`).
pub const TIMESTAMPTZOID: Oid = 1184;
/// `INTERVALOID` — `interval` type OID (`pg_type.dat`).
pub const INTERVALOID: Oid = 1186;
pub const TIDOID: Oid = 27;
pub const XIDOID: Oid = 28;
pub const CIDOID: Oid = 29;
/// `refcursor` — reference to a cursor (portal name); uses `text`'s I/O routines
/// (`catalog/pg_type.dat`, oid 1790).
pub const REFCURSOROID: Oid = 1790;
/// `internal` pseudo-type (`pg_type_d.h`, oid 2281).
pub const INTERNALOID: Oid = 2281;
/// `anyarray` pseudo-type (`pg_type_d.h`, oid 2277).
pub const ANYARRAYOID: Oid = 2277;
/// `any` pseudo-type (`pg_type_d.h`, oid 2276).
pub const ANYOID: Oid = 2276;
/// `anycompatiblearray` pseudo-type (`pg_type_d.h`, oid 5078).
pub const ANYCOMPATIBLEARRAYOID: Oid = 5078;
/// `anyelement` pseudo-type (`pg_type_d.h`, oid 2283).
pub const ANYELEMENTOID: Oid = 2283;
/// `anynonarray` pseudo-type (`pg_type_d.h`, oid 2776).
pub const ANYNONARRAYOID: Oid = 2776;
/// `anyenum` pseudo-type (`pg_type_d.h`, oid 3500).
pub const ANYENUMOID: Oid = 3500;
/// `anyrange` pseudo-type (`pg_type_d.h`, oid 3831).
pub const ANYRANGEOID: Oid = 3831;
/// `anymultirange` pseudo-type (`pg_type_d.h`, oid 4537).
pub const ANYMULTIRANGEOID: Oid = 4537;
/// `anycompatible` pseudo-type (`pg_type_d.h`, oid 5077).
pub const ANYCOMPATIBLEOID: Oid = 5077;
/// `anycompatiblenonarray` pseudo-type (`pg_type_d.h`, oid 5079).
pub const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
/// `anycompatiblerange` pseudo-type (`pg_type_d.h`, oid 5080).
pub const ANYCOMPATIBLERANGEOID: Oid = 5080;
/// `anycompatiblemultirange` pseudo-type (`pg_type_d.h`, oid 4538).
pub const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

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

/// `DEFAULT_COLLATION_OID` (`pg_collation.dat` oid 100) — the database default
/// collation.
pub const DEFAULT_COLLATION_OID: Oid = 100;

// `CompactAttribute.attnullability` values (`access/tupdesc.h`).
/// `ATTNULLABLE_UNRESTRICTED` — no not-null constraint exists (`'f'`).
pub const ATTNULLABLE_UNRESTRICTED: i8 = b'f' as i8;
/// `ATTNULLABLE_UNKNOWN` — a not-null constraint exists but its validity is
/// unknown (`'u'`).
pub const ATTNULLABLE_UNKNOWN: i8 = b'u' as i8;
/// `ATTNULLABLE_VALID` — a valid not-null constraint exists (`'v'`).
pub const ATTNULLABLE_VALID: i8 = b'v' as i8;
/// `ATTNULLABLE_INVALID` — a not-null constraint exists but is marked invalid
/// (`'i'`).
pub const ATTNULLABLE_INVALID: i8 = b'i' as i8;

/// `PG_INT16_MAX` (`c.h`).
pub const PG_INT16_MAX: i32 = i16::MAX as i32;

// `pg_config.h` alignment macros for the standard 64-bit build target;
// `populate_compact_attribute_internal` maps `pg_type.typalign` chars to these.
/// `pg_config.h`: `ALIGNOF_SHORT`.
pub const ALIGNOF_SHORT: u8 = 2;
/// `pg_config.h`: `ALIGNOF_INT`.
pub const ALIGNOF_INT: u8 = 4;
/// `pg_config.h`: `ALIGNOF_DOUBLE`.
pub const ALIGNOF_DOUBLE: u8 = 8;

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
pub const INDEX_ATTRIBUTE_BITMAP_BYTES: usize = (::types_core::INDEX_MAX_KEYS as usize + 8 - 1) / 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameData {
    pub data: [u8; ::types_core::NAMEDATALEN as usize],
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

    /// `namestrcpy(name, str)` (`backend/utils/adt/name.c`) — copy a C string
    /// into a fixed-size `Name`, NUL-terminating and zero-padding. The source
    /// is truncated to `NAMEDATALEN - 1` bytes (C copies up to the limit and
    /// always leaves a trailing NUL within the fixed buffer).
    pub fn namestrcpy(&mut self, src: &str) {
        self.data.fill(0);
        let bytes = src.as_bytes();
        let limit = ::types_core::NAMEDATALEN as usize - 1;
        let len = bytes.len().min(limit);
        self.data[..len].copy_from_slice(&bytes[..len]);
    }
}

impl Default for NameData {
    fn default() -> Self {
        Self {
            data: [0; ::types_core::NAMEDATALEN as usize],
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
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

    /// `ItemPointerSetInvalid(pointer)` (`storage/itemptr.h`): blkid =
    /// `InvalidBlockNumber`, posid = `InvalidOffsetNumber`.
    pub const fn invalid() -> Self {
        Self::new(::types_core::primitive::InvalidBlockNumber, INVALID_OFFSET_NUMBER)
    }
}

/// `InvalidOffsetNumber` (`storage/off.h`): `((OffsetNumber) 0)`.
pub const INVALID_OFFSET_NUMBER: OffsetNumber = 0;
/// `FirstOffsetNumber` (`storage/off.h`): `((OffsetNumber) 1)`.
pub const FIRST_OFFSET_NUMBER: OffsetNumber = 1;

/// `ItemPointerIsValid(pointer)` (`storage/itemptr.h`): a TID is valid iff its
/// offset number is not the invalid sentinel. (The C macro also null-checks the
/// pointer; the owned `&` makes that unnecessary.)
#[inline]
pub fn item_pointer_is_valid(pointer: &ItemPointerData) -> bool {
    pointer.ip_posid != INVALID_OFFSET_NUMBER
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

/// `offsetof(HeapTupleHeaderData, t_bits)` — the fixed on-disk header prefix
/// (`SizeofHeapTupleHeader`): `t_xmin(4) t_xmax(4) t_field3(4) t_ctid(6)
/// t_infomask2(2) t_infomask(2) t_hoff(1)`.
pub const ON_PAGE_HEADER_SIZE: usize = 23;

impl<'mcx> HeapTupleHeaderData<'mcx> {
    /// Read a `HeapTupleHeader`'s fixed header fields directly from an item's
    /// on-page bytes (the C pointer-cast `(HeapTupleHeader) PageGetItem(...)`).
    ///
    /// Only the fixed 23-byte prefix is decoded; `t_bits` is left empty because
    /// the page-bound heap-AM routines that use this (freeze / visibility) only
    /// consult the header words and the null bitmap stays on the page. The
    /// `t_choice` union is always the `THeap` arm for an on-page heap tuple;
    /// `t_field3` is decoded as `TXvac` when `HEAP_MOVED` is set (the only case
    /// where the `t_xvac` interpretation is meaningful) and `TCid` otherwise,
    /// matching the C union accessors.
    pub fn read_on_page(mcx: Mcx<'mcx>, item: &[u8]) -> PgResult<HeapTupleHeaderData<'mcx>> {
        if item.len() < ON_PAGE_HEADER_SIZE {
            return Err(::types_error::PgError::error(
                "heap tuple item shorter than header",
            ));
        }
        let u32_at = |o: usize| uint32::from_ne_bytes([item[o], item[o + 1], item[o + 2], item[o + 3]]);
        let u16_at = |o: usize| uint16::from_ne_bytes([item[o], item[o + 1]]);

        let t_xmin = u32_at(0);
        let t_xmax = u32_at(4);
        let field3_raw = u32_at(8);
        let bi_hi = u16_at(12);
        let bi_lo = u16_at(14);
        let ip_posid = u16_at(16);
        let t_infomask2 = u16_at(18);
        let t_infomask = u16_at(20);
        let t_hoff = item[22];

        let t_field3 = if (t_infomask & HEAP_MOVED) != 0 {
            HeapTupleField3::TXvac(field3_raw)
        } else {
            HeapTupleField3::TCid(field3_raw)
        };

        // Capture the on-page null bitmap into the owned header. C leaves it on
        // the page (the `t_data` pointer aliases it); the owned model copies the
        // `BITMAPLEN(natts)` bytes between the fixed header and `t_hoff` so that
        // `heap_attisnull` / `att_isnull` can read it. A tuple with no nulls
        // (`HEAP_HASNULL` clear) has no bitmap, leaving `t_bits` empty.
        let mut t_bits = PgVec::new_in(mcx);
        if (t_infomask & HEAP_HASNULL) != 0 {
            let end = core::cmp::min(t_hoff as usize, item.len());
            if end > ON_PAGE_HEADER_SIZE {
                for &b in &item[ON_PAGE_HEADER_SIZE..end] {
                    t_bits.push(b);
                }
            }
        }

        Ok(HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin,
                t_xmax,
                t_field3,
            }),
            t_ctid: ItemPointerData {
                ip_blkid: BlockIdData { bi_hi, bi_lo },
                ip_posid,
            },
            t_infomask2,
            t_infomask,
            t_hoff,
            t_bits,
        })
    }

    /// Ensure `t_choice` is the `THeap` union arm and return `&mut` to its
    /// fields, converting from `TDatum` if necessary.
    ///
    /// In C `t_choice` is a `union` of `HeapTupleFields` (`t_heap`) and
    /// `DatumTupleFields` (`t_datum`) over the *same* 12 bytes, so the
    /// `HeapTupleHeaderSet{Xmin,Xmax,Cmin,...}` macros write `t_heap` fields
    /// unconditionally — regardless of how the bytes were last interpreted.
    /// `heap_form_tuple` builds an in-memory tuple with the `t_datum` arm
    /// (`datum_len_`/`datum_typmod`/`datum_typeid`); when such a tuple is later
    /// stamped for on-page storage (`heap_prepare_insert`, `heap_update`'s
    /// new-tuple path), C's union write reinterprets those bytes as `t_heap`.
    /// The Rust enum models the union as a tagged variant, so a stamp on a
    /// `TDatum` header would otherwise be a silent no-op (the matching bug:
    /// xmin/xmax left as stale datum words, breaking visibility and HOT chains).
    /// This converts the arm — the datum words are about to be overwritten by
    /// the stamping caller, so they are dropped (the C reinterpretation likewise
    /// discards them).
    pub fn ensure_heap_arm(&mut self) -> &mut HeapTupleFields {
        if !matches!(self.t_choice, HeapTupleHeaderChoice::THeap(_)) {
            self.t_choice = HeapTupleHeaderChoice::THeap(HeapTupleFields::default());
        }
        match &mut self.t_choice {
            HeapTupleHeaderChoice::THeap(f) => f,
            // Just set above.
            HeapTupleHeaderChoice::TDatum(_) => unreachable!(),
        }
    }

    /// Write this header's fixed fields back over an item's on-page bytes (the C
    /// in-place stores through the `HeapTupleHeader` pointer). `t_bits` and the
    /// tuple's user-data area past byte 23 are left untouched.
    pub fn write_on_page(&self, item: &mut [u8]) -> PgResult<()> {
        if item.len() < ON_PAGE_HEADER_SIZE {
            return Err(::types_error::PgError::error(
                "heap tuple item shorter than header",
            ));
        }
        let (t_xmin, t_xmax, field3_raw) = match &self.t_choice {
            HeapTupleHeaderChoice::THeap(f) => {
                let raw = match f.t_field3 {
                    HeapTupleField3::TCid(c) => c,
                    HeapTupleField3::TXvac(x) => x,
                };
                (f.t_xmin, f.t_xmax, raw)
            }
            HeapTupleHeaderChoice::TDatum(d) => {
                // An on-page heap tuple is never a TDatum; serialize its words
                // 1:1 just in case (datum_len_, datum_typmod, datum_typeid).
                (d.datum_len_ as uint32, d.datum_typmod as uint32, d.datum_typeid)
            }
        };
        item[0..4].copy_from_slice(&t_xmin.to_ne_bytes());
        item[4..8].copy_from_slice(&t_xmax.to_ne_bytes());
        item[8..12].copy_from_slice(&field3_raw.to_ne_bytes());
        item[12..14].copy_from_slice(&self.t_ctid.ip_blkid.bi_hi.to_ne_bytes());
        item[14..16].copy_from_slice(&self.t_ctid.ip_blkid.bi_lo.to_ne_bytes());
        item[16..18].copy_from_slice(&self.t_ctid.ip_posid.to_ne_bytes());
        item[18..20].copy_from_slice(&self.t_infomask2.to_ne_bytes());
        item[20..22].copy_from_slice(&self.t_infomask.to_ne_bytes());
        item[22] = self.t_hoff;
        Ok(())
    }

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
        let mut defval = ::mcx::vec_with_capacity_in(mcx, self.defval.len())?;
        for d in &self.defval {
            defval.push(d.clone_in(mcx)?);
        }
        let mut check = ::mcx::vec_with_capacity_in(mcx, self.check.len())?;
        for c in &self.check {
            check.push(c.clone_in(mcx)?);
        }
        let mut missing = ::mcx::vec_with_capacity_in(mcx, self.missing.len())?;
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
/// model the value *is* its payload ([`Datum`]: a `ByVal` word or the
/// `ByRef` bytes), so the lifetime-extension cache dissolves
/// (`docs/mctx-design.md`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttrMissing<'mcx> {
    pub am_present: bool,
    pub am_value: Datum<'mcx>,
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

/// A lifetime-free image of an attribute missing value's `Datum`, for carriers
/// that outlive any single `'mcx` arena (the relcache entry's owned
/// `TupleConstr`, and the genam `scan_pg_attribute` decode DTO).
///
/// C's `AttrMissing.am_value` is a bare `Datum` whose pointee (for a
/// pass-by-reference value) is kept alive by heaptuple.c's file-static
/// missing-values cache. The owned model captures the value's payload by value
/// here — a `ByVal` machine word for a pass-by-value attribute, or the verbatim
/// detoasted `ByRef` bytes — so it can be re-materialized into a fresh
/// [`Datum`]`<'mcx>` whenever the entry's tuple descriptor is projected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MissingValueImage {
    /// Pass-by-value scalar (`att->attbyval`): the machine word
    /// ([`Datum::ByVal`]).
    ByVal(usize),
    /// Pass-by-reference value (`attlen == -1` varlena or `attlen > 0`
    /// fixed-length by-ref): the verbatim detoasted bytes ([`Datum::ByRef`]).
    ByRef(alloc::vec::Vec<u8>),
}

impl MissingValueImage {
    /// Capture a [`Datum`]'s payload as a lifetime-free image. Mirrors the C
    /// `array_get_element` result the relcache stores in `attrmiss[].am_value`:
    /// either the by-value word or a copy of the by-reference bytes. Panics on
    /// the non-flat arms (Composite/Expanded/Internal), which a stored
    /// attribute missing value never takes (C: a missing value is a flat
    /// `array_get_element` result).
    pub fn from_datum(d: &Datum<'_>) -> Self {
        match d {
            Datum::ByVal(w) => MissingValueImage::ByVal(*w),
            Datum::ByRef(b) => MissingValueImage::ByRef(b.as_slice().to_vec()),
            Datum::Cstring(s) => MissingValueImage::ByRef(s.as_bytes().to_vec()),
            Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
                panic!("MissingValueImage::from_datum on a non-flat Datum (Composite/Expanded/Internal)")
            }
        }
    }

    /// Re-materialize the image as a [`Datum`]`<'mcx>` in `mcx` (the by-value
    /// word verbatim, or the by-reference bytes copied into the arena).
    pub fn to_datum<'mcx>(&self, mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
        Ok(match self {
            MissingValueImage::ByVal(w) => Datum::ByVal(*w),
            MissingValueImage::ByRef(b) => Datum::ByRef(slice_in(mcx, b)?),
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

/// `HeapTupleHeaderSetTypeId(tup, typeid)` (`htup_details.h`) —
/// `tup->t_choice.t_datum.datum_typeid = typeid`. Relabels a composite Datum's
/// header with a new rowtype OID (e.g. `ExecEvalWholeRowVar` stamps the blessed
/// output tupdesc's type). Panics if the header carries heap fields instead (C
/// would clobber the other union arm).
pub fn HeapTupleHeaderSetTypeId(tup: &mut HeapTupleHeaderData, typeid: Oid) {
    match &mut tup.t_choice {
        HeapTupleHeaderChoice::TDatum(d) => d.datum_typeid = typeid,
        HeapTupleHeaderChoice::THeap(_) => {
            panic!("HeapTupleHeaderSetTypeId: header is not a composite Datum")
        }
    }
}

/// `HeapTupleHeaderSetTypMod(tup, typmod)` (`htup_details.h`) —
/// `tup->t_choice.t_datum.datum_typmod = typmod`.
pub fn HeapTupleHeaderSetTypMod(tup: &mut HeapTupleHeaderData, typmod: i32) {
    match &mut tup.t_choice {
        HeapTupleHeaderChoice::TDatum(d) => d.datum_typmod = typmod,
        HeapTupleHeaderChoice::THeap(_) => {
            panic!("HeapTupleHeaderSetTypMod: header is not a composite Datum")
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

/// `HeapTupleHeaderXminFrozen(tup)` (`htup_details.h`) — is the tuple's xmin
/// frozen (`(t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN`).
pub const fn HeapTupleHeaderXminFrozen(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
}

/// `HeapTupleHeaderGetXmin(tup)` (`htup_details.h`) — the effective xmin:
/// `FrozenTransactionId` if the tuple is frozen, else the raw xmin.
pub fn HeapTupleHeaderGetXmin(tup: &HeapTupleHeaderData) -> TransactionId {
    if HeapTupleHeaderXminFrozen(tup) {
        ::types_core::xact::FrozenTransactionId
    } else {
        HeapTupleHeaderGetRawXmin(tup)
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
