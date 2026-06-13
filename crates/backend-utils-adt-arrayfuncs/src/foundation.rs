//! Foundation family: pure byte-buffer math over the standard varlena
//! `ArrayType` (and the storage-compatible `int2vector` / `oidvector`).
//!
//! These are the byte-offset equivalents of the C `array.h` access macros
//! (`ARR_*`), the `c.h` / `tupmacs.h` attribute helpers (`att_align_*`,
//! `att_addlength_*`, `fetch_att`, `store_att_byval`), and the
//! `arrayfuncs.c` byte-walk helpers (`array_seek`, `array_bitmap_copy`,
//! `array_copy`). Zero seams: this is pure arithmetic over `&[u8]`.

use types_array::{ArrayType, ARRAYTYPE_HDRSZ, MAXDIM};
use types_core::Oid;
use types_datum::datum::Datum;

// ---------------------------------------------------------------------------
// Build-time alignment constants (pg_config.h) for the LP64 / x86-64 / aarch64
// target this port runs on, exactly as the C build's configure step computes
// them.
// ---------------------------------------------------------------------------

/// `ALIGNOF_SHORT` — `sizeof(short)`.
const ALIGNOF_SHORT: usize = 2;
/// `ALIGNOF_INT` — `sizeof(int)`.
const ALIGNOF_INT: usize = 4;
/// `ALIGNOF_DOUBLE`.
const ALIGNOF_DOUBLE: usize = 8;
/// `MAXIMUM_ALIGNOF`.
const MAXIMUM_ALIGNOF: usize = 8;

/// `sizeof(int)` — the width of each `ARR_DIMS` / `ARR_LBOUND` entry.
const SIZEOF_INT: usize = 4;

// ---------------------------------------------------------------------------
// `attalign` codes (pg_type.h): TYPALIGN_*.
// ---------------------------------------------------------------------------

/// `TYPALIGN_CHAR` (`'c'`).
const TYPALIGN_CHAR: u8 = b'c';
/// `TYPALIGN_SHORT` (`'s'`).
const TYPALIGN_SHORT: u8 = b's';
/// `TYPALIGN_INT` (`'i'`).
const TYPALIGN_INT: u8 = b'i';
/// `TYPALIGN_DOUBLE` (`'d'`).
const TYPALIGN_DOUBLE: u8 = b'd';

// ---------------------------------------------------------------------------
// Limits (array.h / memutils.h).
// ---------------------------------------------------------------------------

/// `MaxAllocSize` (memutils.h): `0x3fffffff` — the palloc single-chunk cap.
pub const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// `MaxArraySize` (arrayfuncs.c): `MaxAllocSize / sizeof(Datum)`.
pub const MAX_ARRAY_SIZE: usize = MAX_ALLOC_SIZE / core::mem::size_of::<usize>();

/// `MAXDIM` (array.h) re-exported for convenience.
pub const MAX_DIM: i32 = MAXDIM;

// ---------------------------------------------------------------------------
// Element-type OID constants the `*_builtin` helpers switch on
// (`catalog/pg_type_d.h`). Re-exported from `types-tuple` where present.
// ---------------------------------------------------------------------------

pub use types_tuple::heaptuple::{
    ANYARRAYOID, BOOLOID, INT2VECTOROID, INT4OID, INT8OID, OIDOID, OIDVECTOROID, RECORDOID,
    TEXTOID, TIDOID,
};

/// `CHAROID` (`pg_type_d.h`).
pub const CHAROID: Oid = 18;
/// `NAMEOID` (`pg_type_d.h`).
pub const NAMEOID: Oid = 19;
/// `INT2OID` (`pg_type_d.h`).
pub const INT2OID: Oid = 21;
/// `XIDOID` (`pg_type_d.h`).
pub const XIDOID: Oid = 28;
/// `CIDOID` (`pg_type_d.h`).
pub const CIDOID: Oid = 29;
/// `FLOAT4OID` (`pg_type_d.h`).
pub const FLOAT4OID: Oid = 700;
/// `FLOAT8OID` (`pg_type_d.h`).
pub const FLOAT8OID: Oid = 701;
/// `CSTRINGOID` (`pg_type_d.h`).
pub const CSTRINGOID: Oid = 2275;
/// `REGTYPEOID` (`pg_type_d.h`).
pub const REGTYPEOID: Oid = 2206;

/// `NAMEDATALEN` (`pg_config_manual.h`).
pub const NAMEDATALEN: i32 = 64;
/// `sizeof(ItemPointerData)` — `tid` element width (block id + offset).
pub const SIZEOF_ITEM_POINTER_DATA: i32 = 6;
/// `FLOAT8PASSBYVAL` on a 64-bit build (`pg_config.h`).
pub const FLOAT8PASSBYVAL: bool = true;

// ---------------------------------------------------------------------------
// Alignment helpers (c.h: TYPEALIGN family).
// ---------------------------------------------------------------------------

/// `TYPEALIGN(alignval, len)` — round `len` up to a multiple of `alignval`.
///
/// C: `(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))`.
/// NOTE (c.h): does not work unless `ALIGNVAL` is a power of two.
pub fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `SHORTALIGN(len)` — `TYPEALIGN(ALIGNOF_SHORT, len)`.
pub fn short_align(len: usize) -> usize {
    type_align(ALIGNOF_SHORT, len)
}

/// `INTALIGN(len)` — `TYPEALIGN(ALIGNOF_INT, len)`.
pub fn int_align(len: usize) -> usize {
    type_align(ALIGNOF_INT, len)
}

/// `DOUBLEALIGN(len)` — `TYPEALIGN(ALIGNOF_DOUBLE, len)`.
pub fn double_align(len: usize) -> usize {
    type_align(ALIGNOF_DOUBLE, len)
}

/// `MAXALIGN(len)` — `TYPEALIGN(MAXIMUM_ALIGNOF, len)`.
pub fn maxalign(len: usize) -> usize {
    type_align(MAXIMUM_ALIGNOF, len)
}

/// `att_align_nominal(cur_offset, attalign)` (tupmacs.h): align a cursor for
/// the next attribute of the given alignment code (`'c'`/`'s'`/`'i'`/`'d'`).
///
/// The `attalign` cases are tested in the same order as the C macro (hopefully
/// their frequency of occurrence).
pub fn att_align_nominal(cur_offset: usize, attalign: u8) -> usize {
    if attalign == TYPALIGN_INT {
        int_align(cur_offset)
    } else if attalign == TYPALIGN_CHAR {
        cur_offset
    } else if attalign == TYPALIGN_DOUBLE {
        double_align(cur_offset)
    } else {
        // AssertMacro(attalign == TYPALIGN_SHORT)
        debug_assert_eq!(attalign, TYPALIGN_SHORT);
        short_align(cur_offset)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr)` (tupmacs.h): advance a
/// cursor past one attribute given its length code and the bytes at `off`.
///
/// `attptr` in C is the pointer to the field within the buffer; here it is
/// `buf[off..]`.
pub fn att_addlength_pointer(cur_offset: usize, attlen: i32, buf: &[u8], off: usize) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(buf, off)
    } else {
        // AssertMacro(attlen == -2): cstring, strlen + 1.
        debug_assert_eq!(attlen, -2);
        let mut len = 0usize;
        while buf[off + len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

// ---------------------------------------------------------------------------
// varlena header probes (varatt.h).
// ---------------------------------------------------------------------------

/// `VARATT_IS_1B(PTR)` (little-endian): `(va_header & 0x01) == 0x01`.
pub fn varatt_is_1b(buf: &[u8], off: usize) -> bool {
    (buf[off] & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)` (little-endian): `va_header == 0x01`.
pub fn varatt_is_1b_e(buf: &[u8], off: usize) -> bool {
    buf[off] == 0x01
}

/// `VARSIZE_1B(PTR)` (little-endian): `(va_header >> 1) & 0x7F`.
pub fn varsize_1b(buf: &[u8], off: usize) -> usize {
    ((buf[off] >> 1) & 0x7F) as usize
}

/// `VARSIZE_4B(PTR)` (little-endian): `(va_header >> 2) & 0x3FFFFFFF`.
///
/// `va_header` is the first 4-byte word of the varlena (`varattrib_4b`).
pub fn varsize_4b(buf: &[u8], off: usize) -> usize {
    let hdr = u32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
    ((hdr >> 2) & 0x3FFF_FFFF) as usize
}

/// `VARTAG_1B_E(PTR)` — the `va_tag` field of a `varattrib_1b_e` (byte at
/// offset 1).
fn vartag_1b_e(buf: &[u8], off: usize) -> u8 {
    buf[off + 1]
}

/// `VARTAG_SIZE(tag)` (varatt.h): the size of the type-specific TOAST-pointer
/// payload.
fn vartag_size(tag: u8) -> usize {
    // VARTAG_INDIRECT == 1 -> sizeof(varatt_indirect)  (one pointer)
    // VARTAG_IS_EXPANDED (2|3) -> sizeof(varatt_expanded) (one pointer)
    // VARTAG_ONDISK == 18 -> sizeof(varatt_external)
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_ONDISK: u8 = 18;
    if tag == VARTAG_INDIRECT {
        core::mem::size_of::<usize>()
    } else if (tag & !1) == 2 {
        // VARTAG_IS_EXPANDED: (tag & ~1) == VARTAG_EXPANDED_RO
        core::mem::size_of::<usize>()
    } else if tag == VARTAG_ONDISK {
        // varatt_external: int32 + uint32 + Oid + Oid = 16 bytes.
        16
    } else {
        // AssertMacro(false), 0
        debug_assert!(false, "invalid vartag {tag}");
        0
    }
}

/// `VARSIZE_EXTERNAL(PTR)` — `VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR))`.
///
/// `VARHDRSZ_EXTERNAL` is `offsetof(varattrib_1b_e, va_data)` = 2 (the 1-byte
/// `va_header` plus the 1-byte `va_tag`).
fn varsize_external(buf: &[u8], off: usize) -> usize {
    const VARHDRSZ_EXTERNAL: usize = 2;
    VARHDRSZ_EXTERNAL + vartag_size(vartag_1b_e(buf, off))
}

/// `VARSIZE_ANY(PTR)`.
pub fn varsize_any(buf: &[u8], off: usize) -> usize {
    if varatt_is_1b_e(buf, off) {
        varsize_external(buf, off)
    } else if varatt_is_1b(buf, off) {
        varsize_1b(buf, off)
    } else {
        varsize_4b(buf, off)
    }
}

// ---------------------------------------------------------------------------
// ARR_* accessors (array.h) over the on-disk `ArrayType` byte buffer.
// ---------------------------------------------------------------------------

/// Read the `int32` header field at byte offset `field_off` (native endian,
/// header words are aligned).
fn read_i32_field(a: &[u8], field_off: usize) -> i32 {
    i32::from_ne_bytes([
        a[field_off],
        a[field_off + 1],
        a[field_off + 2],
        a[field_off + 3],
    ])
}

/// `ARR_SIZE(a)` — total varlena size (`VARSIZE`).
pub fn arr_size(a: &[u8]) -> usize {
    // VARSIZE == VARSIZE_4B over the leading 4-byte varlena header word.
    varsize_4b(a, 0)
}

/// `ARR_NDIM(a)` — the `ndim` field (offset 4).
pub fn arr_ndim(a: &[u8]) -> i32 {
    read_i32_field(a, 4)
}

/// The raw `dataoffset` header field (offset 8).
pub fn arr_dataoffset_field(a: &[u8]) -> i32 {
    read_i32_field(a, 8)
}

/// `ARR_HASNULL(a)` — `(a)->dataoffset != 0`.
pub fn arr_hasnull(a: &[u8]) -> bool {
    arr_dataoffset_field(a) != 0
}

/// `ARR_ELEMTYPE(a)` — the `elemtype` field (offset 12).
pub fn arr_elemtype(a: &[u8]) -> Oid {
    u32::from_ne_bytes([a[12], a[13], a[14], a[15]])
}

/// Byte offset of the `ARR_DIMS` array (`sizeof(ArrayType)`).
pub fn arr_dims_off(_a: &[u8]) -> usize {
    ARRAYTYPE_HDRSZ
}

/// `ARR_DIMS(a)[i]`.
pub fn arr_dim(a: &[u8], i: usize) -> i32 {
    read_i32_field(a, arr_dims_off(a) + i * SIZEOF_INT)
}

/// `ARR_DIMS(a)` — all `ndim` dimension lengths.
pub fn arr_dims<'mcx>(mcx: mcx::Mcx<'mcx>, a: &[u8]) -> types_error::PgResult<mcx::PgVec<'mcx, i32>> {
    let ndim = arr_ndim(a) as usize;
    let mut v = mcx::vec_with_capacity_in(mcx, ndim)?;
    for i in 0..ndim {
        v.push(arr_dim(a, i));
    }
    Ok(v)
}

/// Byte offset of the `ARR_LBOUND` array
/// (`sizeof(ArrayType) + sizeof(int) * ARR_NDIM(a)`).
pub fn arr_lbound_off(a: &[u8]) -> usize {
    ARRAYTYPE_HDRSZ + SIZEOF_INT * arr_ndim(a) as usize
}

/// `ARR_LBOUND(a)[i]`.
pub fn arr_lbound(a: &[u8], i: usize) -> i32 {
    read_i32_field(a, arr_lbound_off(a) + i * SIZEOF_INT)
}

/// `ARR_LBOUND(a)` — all `ndim` lower bounds.
pub fn arr_lbounds<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &[u8],
) -> types_error::PgResult<mcx::PgVec<'mcx, i32>> {
    let ndim = arr_ndim(a) as usize;
    let mut v = mcx::vec_with_capacity_in(mcx, ndim)?;
    for i in 0..ndim {
        v.push(arr_lbound(a, i));
    }
    Ok(v)
}

/// Byte offset of the null bitmap, or `None` when the array has no bitmap.
///
/// C: `ARR_NULLBITMAP` is `sizeof(ArrayType) + 2 * sizeof(int) * ARR_NDIM(a)`
/// when `ARR_HASNULL`, else NULL.
pub fn arr_nullbitmap_off(a: &[u8]) -> Option<usize> {
    if arr_hasnull(a) {
        Some(ARRAYTYPE_HDRSZ + 2 * SIZEOF_INT * arr_ndim(a) as usize)
    } else {
        None
    }
}

/// `ARR_OVERHEAD_NONULLS(ndims)` —
/// `MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * ndims)`.
pub fn arr_overhead_nonulls(ndims: i32) -> usize {
    maxalign(ARRAYTYPE_HDRSZ + 2 * SIZEOF_INT * ndims as usize)
}

/// `ARR_OVERHEAD_WITHNULLS(ndims, nitems)` —
/// `MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * ndims + (nitems + 7) / 8)`.
pub fn arr_overhead_withnulls(ndims: i32, nitems: i32) -> usize {
    maxalign(ARRAYTYPE_HDRSZ + 2 * SIZEOF_INT * ndims as usize + (nitems as usize + 7) / 8)
}

/// `ARR_DATA_OFFSET(a)` — `ARR_HASNULL ? dataoffset : ARR_OVERHEAD_NONULLS(ndim)`.
pub fn arr_data_offset(a: &[u8]) -> usize {
    if arr_hasnull(a) {
        arr_dataoffset_field(a) as usize
    } else {
        arr_overhead_nonulls(arr_ndim(a))
    }
}

/// Byte offset of `ARR_DATA_PTR(a)` — `((char *) a) + ARR_DATA_OFFSET(a)`.
pub fn arr_data_ptr_off(a: &[u8]) -> usize {
    arr_data_offset(a)
}

// ---------------------------------------------------------------------------
// Header writers.
// ---------------------------------------------------------------------------

/// `SET_VARSIZE(a, len)` (little-endian): `va_header = (uint32) len << 2`.
pub fn set_varsize(a: &mut [u8], len: usize) {
    let hdr: u32 = (len as u32) << 2;
    a[0..4].copy_from_slice(&hdr.to_ne_bytes());
}

/// Write the four-field `ArrayType` header (`vl_len_` via `SET_VARSIZE`,
/// `ndim`, `dataoffset`, `elemtype`).
pub fn set_header(a: &mut [u8], total_size: usize, ndim: i32, dataoffset: i32, elemtype: Oid) {
    set_varsize(a, total_size);
    a[4..8].copy_from_slice(&ndim.to_ne_bytes());
    a[8..12].copy_from_slice(&dataoffset.to_ne_bytes());
    a[12..16].copy_from_slice(&elemtype.to_ne_bytes());
}

/// Write the `ARR_DIMS` array.
pub fn write_dims(a: &mut [u8], dims: &[i32]) {
    let base = ARRAYTYPE_HDRSZ;
    for (i, &d) in dims.iter().enumerate() {
        let off = base + i * SIZEOF_INT;
        a[off..off + 4].copy_from_slice(&d.to_ne_bytes());
    }
}

/// Write the `ARR_LBOUND` array (located after the `ndim`-long `ARR_DIMS`).
pub fn write_lbounds(a: &mut [u8], ndim: i32, lbs: &[i32]) {
    let base = ARRAYTYPE_HDRSZ + SIZEOF_INT * ndim as usize;
    for (i, &lb) in lbs.iter().enumerate() {
        let off = base + i * SIZEOF_INT;
        a[off..off + 4].copy_from_slice(&lb.to_ne_bytes());
    }
}

// ---------------------------------------------------------------------------
// Element fetch / store (tupmacs.h: fetch_att / store_att_byval).
// ---------------------------------------------------------------------------

/// `fetch_att(T, attbyval, attlen)` (tupmacs.h): read one by-value element of
/// the given length out of the buffer at `off`, returning its `Datum`.
///
/// The by-reference case in C returns `PointerGetDatum(T)` — i.e. the address
/// of the element. In the byte model the caller works with the element bytes
/// at `buf[off..]` directly; this function returns the byte offset re-wrapped
/// as a `Datum` word so the by-ref path stays expressible.
pub fn fetch_att(buf: &[u8], off: usize, attbyval: bool, attlen: i32) -> Datum {
    if attbyval {
        match attlen {
            // sizeof(char): CharGetDatum (signed 1-byte char, sign-extended).
            1 => Datum::from_char(buf[off] as i8),
            // sizeof(int16): Int16GetDatum.
            2 => Datum::from_i16(i16::from_ne_bytes([buf[off], buf[off + 1]])),
            // sizeof(int32): Int32GetDatum.
            4 => Datum::from_i32(i32::from_ne_bytes([
                buf[off],
                buf[off + 1],
                buf[off + 2],
                buf[off + 3],
            ])),
            // sizeof(Datum) (SIZEOF_DATUM == 8): the raw word.
            8 => Datum::from_usize(usize::from_ne_bytes([
                buf[off],
                buf[off + 1],
                buf[off + 2],
                buf[off + 3],
                buf[off + 4],
                buf[off + 5],
                buf[off + 6],
                buf[off + 7],
            ])),
            // C: elog(ERROR, "unsupported byval length: %d", attlen)
            _ => panic!("unsupported byval length: {attlen}"),
        }
    } else {
        // C: PointerGetDatum(T) — the element's address. The byte-model
        // equivalent is the in-buffer offset.
        Datum::from_usize(off)
    }
}

/// `store_att_byval(T, newdatum, attlen)` (tupmacs.h): write a by-value
/// element `Datum` of the given length into the buffer at `off`.
pub fn store_att_byval(dest: &mut [u8], off: usize, newdatum: Datum, attlen: i32) {
    let word = newdatum.as_usize() as u64;
    match attlen {
        // sizeof(char): DatumGetChar.
        1 => dest[off] = word as u8,
        // sizeof(int16): DatumGetInt16.
        2 => dest[off..off + 2].copy_from_slice(&(word as u16).to_ne_bytes()),
        // sizeof(int32): DatumGetInt32.
        4 => dest[off..off + 4].copy_from_slice(&(word as u32).to_ne_bytes()),
        // sizeof(Datum): the raw word.
        8 => dest[off..off + 8].copy_from_slice(&word.to_ne_bytes()),
        // C: elog(ERROR, "unsupported byval length: %d", attlen)
        _ => panic!("unsupported byval length: {attlen}"),
    }
}

// ---------------------------------------------------------------------------
// Byte-walk helpers (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_seek(ptr, offset, typlen, typbyval, typalign, nitems, &isNull,
/// &bitmap, &bitmask)` (arrayfuncs.c): advance a data pointer past `nitems`
/// elements, accounting for nulls via the bitmap. Returns the new byte offset.
pub fn array_seek(
    buf: &[u8],
    data_off: usize,
    nullbitmap: Option<usize>,
    bitmask_in: i32,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
    nitems: i32,
) -> (usize, i32) {
    let _ = typbyval;

    // easy if fixed-size elements and no NULLs.
    if typlen > 0 && nullbitmap.is_none() {
        // ptr + nitems * att_align_nominal(typlen, typalign).
        let step = att_align_nominal(typlen as usize, typalign);
        return (data_off + nitems as usize * step, bitmask_in);
    }

    let mut ptr = data_off;

    // seems worth having separate loops for NULL and no-NULLs cases.
    if let Some(bitmap_base) = nullbitmap {
        // The caller positions `nullbitmap` at its base; the per-byte cursor
        // and `bitmask` are threaded here (mirrors C advancing the local
        // `nullbitmap` pointer + `bitmask`).
        let mut bm = bitmap_base;
        let mut bitmask = bitmask_in;
        for _ in 0..nitems {
            if (buf[bm] as i32 & bitmask) != 0 {
                ptr = att_addlength_pointer(ptr, typlen, buf, ptr);
                ptr = att_align_nominal(ptr, typalign);
            }
            bitmask <<= 1;
            if bitmask == 0x100 {
                bm += 1;
                bitmask = 1;
            }
        }
        (ptr, bitmask)
    } else {
        for _ in 0..nitems {
            ptr = att_addlength_pointer(ptr, typlen, buf, ptr);
            ptr = att_align_nominal(ptr, typalign);
        }
        (ptr, bitmask_in)
    }
}

/// `array_nelems_size(ptr, offset, nullbitmap, nitems, typlen, typbyval,
/// typalign)` (arrayfuncs.c): byte size of `nitems` elements starting at
/// `offset`.
pub fn array_nelems_size(
    buf: &[u8],
    data_off: usize,
    nullbitmap: Option<usize>,
    nitems: i32,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> usize {
    // C: array_seek(ptr, offset, nullbitmap, nitems, ...) - ptr.
    // The refactored interface threads a pre-positioned bitmap base with the
    // first element's bitmask = 1 (offset-relative to the supplied base).
    let (end, _bitmask) = array_seek(
        buf, data_off, nullbitmap, 1, typlen, typbyval, typalign, nitems,
    );
    end - data_off
}

/// `array_copy(destptr, nitems, srcptr, offset, typlen, typbyval, typalign,
/// nullbitmap)` (arrayfuncs.c): copy `nitems` elements of data bytes.
pub fn array_copy(
    dest: &mut [u8],
    dest_off: usize,
    nitems: i32,
    src: &[u8],
    src_off: usize,
    nullbitmap: Option<usize>,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
) -> usize {
    // numbytes = array_nelems_size(srcptr, offset, nullbitmap, nitems, ...).
    let numbytes = array_nelems_size(
        src, src_off, nullbitmap, nitems, typlen, typbyval, typalign,
    );
    // memcpy(destptr, srcptr, numbytes).
    dest[dest_off..dest_off + numbytes].copy_from_slice(&src[src_off..src_off + numbytes]);
    numbytes
}

/// `array_bitmap_copy(destbitmap, destoffset, srcbitmap, srcoffset, nitems)`
/// (arrayfuncs.c): copy `nitems` null-bitmap bits between buffers.
pub fn array_bitmap_copy(
    dest: &mut [u8],
    dest_bitmap_off: usize,
    dest_offset: i32,
    src: &[u8],
    src_bitmap_off: Option<usize>,
    src_offset: i32,
    nitems: i32,
) {
    // Assert(destbitmap); — the caller passes a non-optional dest base.
    if nitems <= 0 {
        // don't risk fetch off end of memory.
        return;
    }

    // destbitmap += destoffset / 8; destbitmask = 1 << (destoffset % 8);
    let mut destbm = dest_bitmap_off + (dest_offset as usize) / 8;
    let mut destbitmask: i32 = 1 << (dest_offset % 8);
    let mut destbitval: i32 = dest[destbm] as i32;

    let mut remaining = nitems;

    if let Some(src_base) = src_bitmap_off {
        // srcbitmap += srcoffset / 8; srcbitmask = 1 << (srcoffset % 8);
        let mut srcbm = src_base + (src_offset as usize) / 8;
        let mut srcbitmask: i32 = 1 << (src_offset % 8);
        let mut srcbitval: i32 = src[srcbm] as i32;
        while remaining > 0 {
            remaining -= 1;
            if (srcbitval & srcbitmask) != 0 {
                destbitval |= destbitmask;
            } else {
                destbitval &= !destbitmask;
            }
            destbitmask <<= 1;
            if destbitmask == 0x100 {
                dest[destbm] = destbitval as u8;
                destbm += 1;
                destbitmask = 1;
                if remaining > 0 {
                    destbitval = dest[destbm] as i32;
                }
            }
            srcbitmask <<= 1;
            if srcbitmask == 0x100 {
                srcbm += 1;
                srcbitmask = 1;
                if remaining > 0 {
                    srcbitval = src[srcbm] as i32;
                }
            }
        }
        if destbitmask != 1 {
            dest[destbm] = destbitval as u8;
        }
    } else {
        // srcbitmap == NULL: source is all-non-NULL, fill 1's.
        while remaining > 0 {
            remaining -= 1;
            destbitval |= destbitmask;
            destbitmask <<= 1;
            if destbitmask == 0x100 {
                dest[destbm] = destbitval as u8;
                destbm += 1;
                destbitmask = 1;
                if remaining > 0 {
                    destbitval = dest[destbm] as i32;
                }
            }
        }
        if destbitmask != 1 {
            dest[destbm] = destbitval as u8;
        }
    }
}

/// Read one element's null bit from the bitmap at `nullbitmap` / `offset`
/// (`array_get_isnull`, arrayfuncs.c).
pub fn array_get_isnull(buf: &[u8], nullbitmap: Option<usize>, offset: i32) -> bool {
    match nullbitmap {
        // assume not null.
        None => false,
        Some(base) => {
            let byte = buf[base + (offset as usize) / 8] as i32;
            if (byte & (1 << (offset % 8))) != 0 {
                false // not null
            } else {
                true
            }
        }
    }
}

/// Write one element's null bit into the bitmap at `bitmap_off` / `offset`
/// (`array_set_isnull`, arrayfuncs.c).
pub fn array_set_isnull(buf: &mut [u8], bitmap_off: usize, offset: i32, is_null: bool) {
    let idx = bitmap_off + (offset as usize) / 8;
    let bitmask: i32 = 1 << (offset % 8);
    if is_null {
        buf[idx] = (buf[idx] as i32 & !bitmask) as u8;
    } else {
        buf[idx] = (buf[idx] as i32 | bitmask) as u8;
    }
}

/// `DatumGetArrayTypeP`-style sanity: confirm a buffer is at least a full
/// `ArrayType` header. (Helper used pervasively before accessor calls.)
pub fn has_header(a: &[u8]) -> bool {
    a.len() >= ARRAYTYPE_HDRSZ
}

/// Re-export of the on-disk header type for callers that need the field
/// layout directly.
pub type Header = ArrayType;
