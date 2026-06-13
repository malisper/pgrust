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
pub fn type_align(alignval: usize, len: usize) -> usize {
    todo!("foundation: TYPEALIGN")
}

/// `SHORTALIGN(len)`.
pub fn short_align(len: usize) -> usize {
    todo!("foundation: SHORTALIGN")
}

/// `INTALIGN(len)`.
pub fn int_align(len: usize) -> usize {
    todo!("foundation: INTALIGN")
}

/// `DOUBLEALIGN(len)`.
pub fn double_align(len: usize) -> usize {
    todo!("foundation: DOUBLEALIGN")
}

/// `MAXALIGN(len)`.
pub fn maxalign(len: usize) -> usize {
    todo!("foundation: MAXALIGN")
}

/// `att_align_nominal(cur_offset, attalign)` (tupmacs.h): align a cursor for
/// the next attribute of the given alignment code (`'c'`/`'s'`/`'i'`/`'d'`).
pub fn att_align_nominal(cur_offset: usize, attalign: u8) -> usize {
    todo!("foundation: att_align_nominal")
}

/// `att_addlength_pointer(cur_offset, attlen, attptr)` (tupmacs.h): advance a
/// cursor past one attribute given its length code and the bytes at `off`.
pub fn att_addlength_pointer(cur_offset: usize, attlen: i32, buf: &[u8], off: usize) -> usize {
    todo!("foundation: att_addlength_pointer")
}

// ---------------------------------------------------------------------------
// varlena header probes (varatt.h).
// ---------------------------------------------------------------------------

/// `VARATT_IS_1B(PTR)`.
pub fn varatt_is_1b(buf: &[u8], off: usize) -> bool {
    todo!("foundation: VARATT_IS_1B")
}

/// `VARATT_IS_1B_E(PTR)`.
pub fn varatt_is_1b_e(buf: &[u8], off: usize) -> bool {
    todo!("foundation: VARATT_IS_1B_E")
}

/// `VARSIZE_1B(PTR)`.
pub fn varsize_1b(buf: &[u8], off: usize) -> usize {
    todo!("foundation: VARSIZE_1B")
}

/// `VARSIZE_4B(PTR)`.
pub fn varsize_4b(buf: &[u8], off: usize) -> usize {
    todo!("foundation: VARSIZE_4B")
}

/// `VARSIZE_ANY(PTR)`.
pub fn varsize_any(buf: &[u8], off: usize) -> usize {
    todo!("foundation: VARSIZE_ANY")
}

// ---------------------------------------------------------------------------
// ARR_* accessors (array.h) over the on-disk `ArrayType` byte buffer.
// ---------------------------------------------------------------------------

/// `ARR_SIZE(a)` — total varlena size (`VARSIZE`).
pub fn arr_size(a: &[u8]) -> usize {
    todo!("foundation: ARR_SIZE")
}

/// `ARR_NDIM(a)`.
pub fn arr_ndim(a: &[u8]) -> i32 {
    todo!("foundation: ARR_NDIM")
}

/// The raw `dataoffset` header field.
pub fn arr_dataoffset_field(a: &[u8]) -> i32 {
    todo!("foundation: ARR_DATAOFFSET field")
}

/// `ARR_HASNULL(a)` — `ARR_DATAOFFSET(a) != 0`.
pub fn arr_hasnull(a: &[u8]) -> bool {
    todo!("foundation: ARR_HASNULL")
}

/// `ARR_ELEMTYPE(a)`.
pub fn arr_elemtype(a: &[u8]) -> Oid {
    todo!("foundation: ARR_ELEMTYPE")
}

/// Byte offset of the `ARR_DIMS` array (`sizeof(ArrayType)`).
pub fn arr_dims_off(a: &[u8]) -> usize {
    todo!("foundation: ARR_DIMS offset")
}

/// `ARR_DIMS(a)[i]`.
pub fn arr_dim(a: &[u8], i: usize) -> i32 {
    todo!("foundation: ARR_DIMS[i]")
}

/// `ARR_DIMS(a)` — all `ndim` dimension lengths.
pub fn arr_dims<'mcx>(mcx: mcx::Mcx<'mcx>, a: &[u8]) -> types_error::PgResult<mcx::PgVec<'mcx, i32>> {
    todo!("foundation: ARR_DIMS")
}

/// Byte offset of the `ARR_LBOUND` array.
pub fn arr_lbound_off(a: &[u8]) -> usize {
    todo!("foundation: ARR_LBOUND offset")
}

/// `ARR_LBOUND(a)[i]`.
pub fn arr_lbound(a: &[u8], i: usize) -> i32 {
    todo!("foundation: ARR_LBOUND[i]")
}

/// `ARR_LBOUND(a)` — all `ndim` lower bounds.
pub fn arr_lbounds<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    a: &[u8],
) -> types_error::PgResult<mcx::PgVec<'mcx, i32>> {
    todo!("foundation: ARR_LBOUND")
}

/// Byte offset of the null bitmap, or `None` when the array has no bitmap.
pub fn arr_nullbitmap_off(a: &[u8]) -> Option<usize> {
    todo!("foundation: ARR_NULLBITMAP offset")
}

/// `ARR_OVERHEAD_NONULLS(ndims)`.
pub fn arr_overhead_nonulls(ndims: i32) -> usize {
    todo!("foundation: ARR_OVERHEAD_NONULLS")
}

/// `ARR_OVERHEAD_WITHNULLS(ndims, nitems)`.
pub fn arr_overhead_withnulls(ndims: i32, nitems: i32) -> usize {
    todo!("foundation: ARR_OVERHEAD_WITHNULLS")
}

/// `ARR_DATA_OFFSET(a)`.
pub fn arr_data_offset(a: &[u8]) -> usize {
    todo!("foundation: ARR_DATA_OFFSET")
}

/// Byte offset of `ARR_DATA_PTR(a)`.
pub fn arr_data_ptr_off(a: &[u8]) -> usize {
    todo!("foundation: ARR_DATA_PTR offset")
}

// ---------------------------------------------------------------------------
// Header writers.
// ---------------------------------------------------------------------------

/// `SET_VARSIZE(a, len)`.
pub fn set_varsize(a: &mut [u8], len: usize) {
    todo!("foundation: SET_VARSIZE")
}

/// Write the four-field `ArrayType` header (`vl_len_` via `SET_VARSIZE`,
/// `ndim`, `dataoffset`, `elemtype`).
pub fn set_header(a: &mut [u8], total_size: usize, ndim: i32, dataoffset: i32, elemtype: Oid) {
    todo!("foundation: write ArrayType header")
}

/// Write the `ARR_DIMS` array.
pub fn write_dims(a: &mut [u8], dims: &[i32]) {
    todo!("foundation: write ARR_DIMS")
}

/// Write the `ARR_LBOUND` array.
pub fn write_lbounds(a: &mut [u8], ndim: i32, lbs: &[i32]) {
    todo!("foundation: write ARR_LBOUND")
}

// ---------------------------------------------------------------------------
// Element fetch / store (tupmacs.h: fetch_att / store_att_byval).
// ---------------------------------------------------------------------------

/// `fetch_att(T, attbyval, attlen)` (tupmacs.h): read one by-value element of
/// the given length out of the buffer at `off`, returning its `Datum`.
pub fn fetch_att(buf: &[u8], off: usize, attbyval: bool, attlen: i32) -> Datum {
    todo!("foundation: fetch_att")
}

/// `store_att_byval(T, newdatum, attlen)` (tupmacs.h): write a by-value
/// element `Datum` of the given length into the buffer at `off`.
pub fn store_att_byval(dest: &mut [u8], off: usize, newdatum: Datum, attlen: i32) {
    todo!("foundation: store_att_byval")
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
    todo!("foundation: array_seek")
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
    todo!("foundation: array_nelems_size")
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
    todo!("foundation: array_copy")
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
    todo!("foundation: array_bitmap_copy")
}

/// Read one element's null bit from the bitmap at `nullbitmap` / `offset`.
pub fn array_get_isnull(buf: &[u8], nullbitmap: Option<usize>, offset: i32) -> bool {
    todo!("foundation: array_get_isnull")
}

/// Write one element's null bit into the bitmap at `bitmap_off` / `offset`.
pub fn array_set_isnull(buf: &mut [u8], bitmap_off: usize, offset: i32, is_null: bool) {
    todo!("foundation: array_set_isnull")
}

/// `DatumGetArrayTypeP`-style sanity: confirm a buffer is at least a full
/// `ArrayType` header. (Helper used pervasively before accessor calls.)
pub fn has_header(a: &[u8]) -> bool {
    a.len() >= ARRAYTYPE_HDRSZ
}

/// Re-export of the on-disk header type for callers that need the field
/// layout directly.
pub type Header = ArrayType;
