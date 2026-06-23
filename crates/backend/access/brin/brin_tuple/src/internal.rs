//! In-crate byte helpers for the BRIN tuple codec: the on-disk
//! [`BrinTupleImage`] header accessors (`brin_tuple.h`), the `tupmacs.h`
//! null-bit / alignment helpers, the `varatt.h` varlena macros, and the
//! on-disk descriptor builder (`brtuple_disk_tupdesc`).

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use brin::{
    BrinDesc, BRIN_EMPTY_RANGE_MASK, BRIN_NULLS_MASK, BRIN_OFFSET_MASK, BRIN_PLACEHOLDER_MASK,
    BT_INFO_OFFSET,
};
use types_core::{BlockNumber, Size};
use types_error::PgResult;
use types_tuple::heaptuple::{
    CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, TYPALIGN_CHAR,
    TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT,
};
use types_typcache::{TypeCacheEntry, TYPSTORAGE_PLAIN};

/// `MAXIMUM_ALIGNOF` on a standard 64-bit build (`c.h`).
pub const MAXIMUM_ALIGNOF: usize = 8;

/// `ALIGNOF_SHORT` (`pg_config.h`).
const ALIGNOF_SHORT: u8 = 2;
/// `ALIGNOF_INT` (`pg_config.h`).
const ALIGNOF_INT: u8 = 4;
/// `ALIGNOF_DOUBLE` (`pg_config.h`).
const ALIGNOF_DOUBLE: u8 = 8;

/// `HIGHBIT` (`c.h`): the high bit of a byte.
pub const HIGHBIT: i32 = 1 << 7;

/// `MAXALIGN(LEN)` (`c.h`).
#[inline]
pub const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `BITMAPLEN(NATTS)` (`htup_details.h`): bytes needed for `NATTS` bits.
#[inline]
pub const fn bitmaplen(natts: usize) -> usize {
    (natts + 7) / 8
}

/// `att_isnull(ATT, BITS)` (`tupmacs.h`): is bit `att` clear? BRIN reverses the
/// *sense* of these bits (1 == null), so callers invert the result.
#[inline]
pub fn att_isnull(att: usize, bits: &[u8]) -> bool {
    (bits[att >> 3] & (1 << (att & 0x07))) == 0
}

// ---------------------------------------------------------------------------
// varatt.h varlena macros (little-endian), over verbatim datum bytes.
// ---------------------------------------------------------------------------

/// `VARATT_IS_1B_E(PTR)`: a 1-byte TOAST pointer (`va_header == 0x01`).
#[inline]
pub fn varatt_is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_EXTENDED(PTR)`: header low two bits are not `00` (i.e. short,
/// compressed, or external — anything but a plain uncompressed 4-byte header).
#[inline]
pub fn varatt_is_extended(b: &[u8]) -> bool {
    (b[0] & 0x03) != 0x00
}

/// `VARSIZE(PTR)` == `VARSIZE_4B(PTR)`: `(va_header >> 2) & 0x3FFFFFFF`.
#[inline]
pub fn varsize(b: &[u8]) -> usize {
    let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    ((hdr >> 2) & 0x3FFF_FFFF) as usize
}

/// `VARSIZE_ANY(PTR)` (`varatt.h`): the total bytes a varlena occupies on disk,
/// dispatching on the header form (external TOAST pointer, short 1-byte, or
/// plain 4-byte). Mirrors the `att_addlength_pointer` size computation.
#[inline]
pub fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_external(b) {
        // VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR))
        VARHDRSZ_EXTERNAL + vartag_size(b[1])
    } else if (b[0] & 0x01) == 0x01 {
        // short 1-byte header: VARSIZE_1B = (va_header >> 1) & 0x7F
        ((b[0] >> 1) & 0x7F) as usize
    } else {
        varsize(b)
    }
}

/// `VARHDRSZ_EXTERNAL` (`varatt.h`): `offsetof(varattrib_1b_e, va_data)` == 2.
const VARHDRSZ_EXTERNAL: usize = 2;

/// `VARTAG_SIZE(tag)` (`varatt.h`): payload size of a TOAST pointer for the
/// given `va_tag`.
#[inline]
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_EXPANDED_RW: u8 = 3;
    const VARTAG_ONDISK: u8 = 18;
    match tag {
        VARTAG_INDIRECT => 8,                       // sizeof(varatt_indirect)
        VARTAG_EXPANDED_RO | VARTAG_EXPANDED_RW => 8, // sizeof(varatt_expanded)
        VARTAG_ONDISK => 16,                        // sizeof(varatt_external)
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// On-disk BrinTuple header accessors (brin_tuple.h), over an owned byte image.
// ---------------------------------------------------------------------------

/// An owned byte image of an on-disk `BrinTuple` (header + null bitmap + data).
/// C represents this as a `palloc`'d `BrinTuple *`; here it is a `PgVec<u8>` of
/// exactly the tuple's length, with header access through the helpers below.
#[derive(Debug)]
pub struct BrinTupleImage<'mcx> {
    /// The raw on-disk bytes; `bytes.len()` is the tuple's `Size`.
    pub bytes: PgVec<'mcx, u8>,
}

impl<'mcx> BrinTupleImage<'mcx> {
    /// `palloc0(len)`: a zeroed image of `len` bytes in `mcx`.
    pub fn zeroed(mcx: Mcx<'mcx>, len: usize) -> PgResult<Self> {
        let mut bytes = vec_with_capacity_in(mcx, len)?;
        bytes.resize(len, 0);
        Ok(Self { bytes })
    }

    /// The tuple length (`Size`).
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the image is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// `tuple->bt_blkno`.
    pub fn bt_blkno(&self) -> BlockNumber {
        brin_tuple_get_blkno(&self.bytes)
    }

    /// `tuple->bt_blkno = blkno`.
    pub fn set_bt_blkno(&mut self, blkno: BlockNumber) {
        self.bytes[..4].copy_from_slice(&blkno.to_ne_bytes());
    }

    /// `tuple->bt_info`.
    pub fn bt_info(&self) -> u8 {
        self.bytes[BT_INFO_OFFSET]
    }

    /// `tuple->bt_info = info`.
    pub fn set_bt_info(&mut self, info: u8) {
        self.bytes[BT_INFO_OFFSET] = info;
    }

    /// `tuple->bt_info |= mask`.
    pub fn or_bt_info(&mut self, mask: u8) {
        self.bytes[BT_INFO_OFFSET] |= mask;
    }
}

/// `offsetof(BrinTuple, bt_blkno)` is 0; read the heap block number.
#[inline]
pub fn brin_tuple_get_blkno(bytes: &[u8]) -> BlockNumber {
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[..4]);
    BlockNumber::from_ne_bytes(b)
}

/// `tuple->bt_info`.
#[inline]
pub fn brin_tuple_get_info(bytes: &[u8]) -> u8 {
    bytes[BT_INFO_OFFSET]
}

/// `BrinTupleDataOffset(tup)` (`brin_tuple.h`): the data-area offset.
#[inline]
pub fn brin_tuple_data_offset(bytes: &[u8]) -> Size {
    (brin_tuple_get_info(bytes) & BRIN_OFFSET_MASK) as Size
}

/// `BrinTupleHasNulls(tup)`.
#[inline]
pub fn brin_tuple_has_nulls(bytes: &[u8]) -> bool {
    (brin_tuple_get_info(bytes) & BRIN_NULLS_MASK) != 0
}

/// `BrinTupleIsPlaceholder(tup)`.
#[inline]
pub fn brin_tuple_is_placeholder(bytes: &[u8]) -> bool {
    (brin_tuple_get_info(bytes) & BRIN_PLACEHOLDER_MASK) != 0
}

/// `BrinTupleIsEmptyRange(tup)`.
#[inline]
pub fn brin_tuple_is_empty_range(bytes: &[u8]) -> bool {
    (brin_tuple_get_info(bytes) & BRIN_EMPTY_RANGE_MASK) != 0
}

// ---------------------------------------------------------------------------
// brtuple_disk_tupdesc (brin_tuple.c:60).
// ---------------------------------------------------------------------------

/// `attalignby` for a `typalign` char, mirroring `populate_compact_attribute`
/// (`access/common/tupdesc.c`).
#[inline]
fn alignby_for(typalign: i8) -> u8 {
    match typalign {
        x if x == TYPALIGN_INT => ALIGNOF_INT,
        x if x == TYPALIGN_CHAR => 1,
        x if x == TYPALIGN_DOUBLE => ALIGNOF_DOUBLE,
        x if x == TYPALIGN_SHORT => ALIGNOF_SHORT,
        // C: elog(ERROR, "invalid attalign value"). A typcache entry always
        // carries a valid pg_type.typalign, so this is unreachable in practice.
        _ => panic!("invalid attalign value: {typalign}"),
    }
}

/// Build the `CompactAttribute` for a stored column whose pg_type parameters are
/// `tce` — the part of `TupleDescInitEntry` + `populate_compact_attribute` the
/// on-disk descriptor depends on (`access/common/tupdesc.c`). `TupleDescInitEntry`
/// is called with `typmod = -1`, so the storage form is exactly the type's own.
fn compact_attr_from_typcache(tce: &TypeCacheEntry) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen: tce.typlen,
        attbyval: tce.typbyval,
        attispackable: tce.typstorage != TYPSTORAGE_PLAIN,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: alignby_for(tce.typalign),
    }
}

/// `brtuple_disk_tupdesc(brdesc)` (brin_tuple.c:60): the tuple descriptor used
/// for on-disk storage of BRIN tuples.
///
/// In C this is cached lazily in `brdesc->bd_disktdesc`, built from the
/// per-column `oi_typcache[j]->type_id` entries via `TupleDescInitEntry`. Here
/// it is recomputed from the stored columns' [`TypeCacheEntry`] storage
/// parameters (the cache is purely an optimization; recomputation is
/// behaviorally identical). The descriptor has `bd_totalstored` attributes.
pub fn brtuple_disk_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    brdesc: &BrinDesc<'_>,
) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>> {
    let total = brdesc.bd_totalstored as usize;
    let mut compact_attrs: PgVec<'mcx, CompactAttribute> = vec_with_capacity_in(mcx, total)?;
    let mut attrs: PgVec<'mcx, FormData_pg_attribute> = vec_with_capacity_in(mcx, total)?;

    // attno = 1; for i in natts { for j in oi_nstored { TupleDescInitEntry(..,
    //   oi_typcache[j]->type_id, -1, 0) } }
    for i in 0..brdesc.natts() {
        let info = &brdesc.bd_info[i];
        for j in 0..info.nstored() {
            let tce = &info.oi_typcache[j];
            compact_attrs.push(compact_attr_from_typcache(tce));
            attrs.push(disk_form_attr(tce, compact_attrs.len() as i16));
        }
    }

    mcx::alloc_in(
        mcx,
        TupleDescData {
            natts: total as i32,
            tdtypeid: 0, // RECORDOID is set by CreateTemplateTupleDesc; not read here
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs,
            attrs,
        },
    )
}

/// The full `FormData_pg_attribute` for a stored disk column, carrying the
/// `pg_type` fields `TupleDescInitEntry` copies (the consumers of the disk
/// descriptor only read `compact_attrs`, but the parallel `attrs` Vec is kept
/// in lock-step as `TupleDescData` requires).
fn disk_form_attr(tce: &TypeCacheEntry, attnum: i16) -> FormData_pg_attribute {
    FormData_pg_attribute {
        attrelid: 0,
        attname: NameData::default(),
        atttypid: tce.type_id,
        attlen: tce.typlen,
        attnum,
        atttypmod: -1,
        attndims: 0,
        attbyval: tce.typbyval,
        attalign: tce.typalign,
        attstorage: tce.typstorage,
        attcompression: 0,
        attnotnull: false,
        atthasdef: false,
        atthasmissing: false,
        attidentity: 0,
        attgenerated: 0,
        attisdropped: false,
        attislocal: true,
        attinhcount: 0,
        attcollation: 0,
    }
}
