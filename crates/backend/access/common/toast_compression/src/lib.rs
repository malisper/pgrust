//! Functions for TOAST compression (`access/common/toast_compression.c`).
//!
//! This is the wrapper layer that selects between the PGLZ and LZ4 compression
//! methods for compressible varlena values, on top of the `common-pglz`
//! primitives (`pglz_compress`/`pglz_decompress`). PGLZ is always available;
//! LZ4 is an optional build dependency (`#ifdef USE_LZ4`). PostgreSQL builds
//! here are *non*-LZ4 (`USE_LZ4` unset), so the four `lz4_*` routines raise the
//! `NO_LZ4_SUPPORT()` error exactly as the C `#ifndef USE_LZ4` branch does.
//!
//! The decompression-side LZ4 routines are also published as seams
//! (`backend-access-common-toast-compression-seams`) because the `detoast.c`
//! unit dispatches into them by compression-method id; [`init_seams`] installs
//! those, so a non-LZ4 detoast attempt produces the same `NO_LZ4_SUPPORT()`
//! error rather than a panic.
//!
//! Datums are represented as their verbatim on-the-wire varlena bytes; the
//! header bit-twiddling helpers mirror `varatt.h` directly (see the local
//! helper module), matching the convention used by `backend-access-common-detoast`.

use pglz::{pglz_compress, pglz_decompress_to_slice, PGLZ_MAX_OUTPUT};
use mcx::{Mcx, PgVec};
use ::datum::VARHDRSZ;
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED};

use toast_compression_seams as seams;

// ---------------------------------------------------------------------------
// Constants (access/toast_compression.h)
// ---------------------------------------------------------------------------

/// `ToastCompressionId` (`access/toast_compression.h`): the value stored in the
/// top two bits of a compressed varlena's `va_tcinfo` / an external pointer's
/// `va_extinfo`.
pub type ToastCompressionId = i32;

/// `TOAST_PGLZ_COMPRESSION_ID = 0`.
pub const TOAST_PGLZ_COMPRESSION_ID: ToastCompressionId = 0;
/// `TOAST_LZ4_COMPRESSION_ID = 1`.
pub const TOAST_LZ4_COMPRESSION_ID: ToastCompressionId = 1;
/// `TOAST_INVALID_COMPRESSION_ID = 2`.
pub const TOAST_INVALID_COMPRESSION_ID: ToastCompressionId = 2;

/// `TOAST_PGLZ_COMPRESSION` (`'p'`): the `pg_attribute.attcompression` value
/// selecting PGLZ.
pub const TOAST_PGLZ_COMPRESSION: u8 = b'p';
/// `TOAST_LZ4_COMPRESSION` (`'l'`).
pub const TOAST_LZ4_COMPRESSION: u8 = b'l';
/// `InvalidCompressionMethod` (`'\0'`).
pub const INVALID_COMPRESSION_METHOD: u8 = b'\0';

/// `VARHDRSZ_COMPRESSED` (`varatt.h`): `offsetof(varattrib_4b,
/// va_compressed.va_data)` — the 4-byte length word plus the 4-byte
/// `va_tcinfo` field that precede the compressed payload.
const VARHDRSZ_COMPRESSED: usize = VARHDRSZ + 4;

/// `VARLENA_EXTSIZE_BITS` (`varatt.h`): the external/raw size occupies the low
/// 30 bits of the `va_extinfo` / `va_tcinfo` word; the top two bits hold the
/// compression method id.
const VARLENA_EXTSIZE_BITS: u32 = 30;
/// `VARLENA_EXTSIZE_MASK` (`varatt.h`): `(1U << VARLENA_EXTSIZE_BITS) - 1`.
const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/// `VARTAG_ONDISK` (`varatt.h`).
const VARTAG_ONDISK: u8 = 18;

// ---------------------------------------------------------------------------
// NO_LZ4_SUPPORT()
// ---------------------------------------------------------------------------

/// `NO_LZ4_SUPPORT()` (toast_compression.c): the error raised on a non-LZ4
/// build when an LZ4 path is reached.
fn no_lz4_support() -> PgError {
    PgError::error("compression method lz4 not supported")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail("This functionality requires the server to be built with lz4 support.")
}

// ---------------------------------------------------------------------------
// Local varlena/TOAST-pointer header helpers (pure `varatt.h` bit-twiddling).
// They operate on the verbatim encoded varlena bytes.
// ---------------------------------------------------------------------------

/// `VARATT_IS_4B_C(PTR)` == `VARATT_IS_COMPRESSED(PTR)`: a 4-byte-header datum
/// with the low two bits `0b10`.
#[inline]
fn varatt_is_compressed(b: &[u8]) -> bool {
    !b.is_empty() && (b[0] & 0x03) == 0x02
}

/// `VARATT_IS_1B(PTR)`: a short (1-byte header) datum (`va_header` bit 0 set).
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    !b.is_empty() && (b[0] & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)` == `VARATT_IS_EXTERNAL(PTR)`: external (TOAST-pointer)
/// form — `va_header == 0x01`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    !b.is_empty() && b[0] == 0x01
}

/// `VARATT_IS_EXTERNAL_ONDISK(PTR)`: external form with `va_tag == VARTAG_ONDISK`.
#[inline]
fn varatt_is_external_ondisk(b: &[u8]) -> bool {
    varatt_is_external(b) && b.len() >= 2 && b[1] == VARTAG_ONDISK
}

/// `VARSIZE_4B(PTR)`: the 4-byte length word `>> 2`, masked to 30 bits.
#[inline]
fn varsize_4b(b: &[u8]) -> u32 {
    let header = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    (header >> 2) & VARLENA_EXTSIZE_MASK
}

/// `VARSIZE_1B(PTR)`: `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> u32 {
    ((b[0] >> 1) & 0x7f) as u32
}

/// `VARSIZE_ANY_EXHDR(PTR)`: payload size (header excluded) of any
/// non-external varlena form.
fn varsize_any_exhdr(b: &[u8]) -> i32 {
    if varatt_is_1b(b) {
        // VARSIZE_1B - VARHDRSZ_SHORT, where VARHDRSZ_SHORT == 1.
        varsize_1b(b) as i32 - 1
    } else {
        // VARSIZE_4B - VARHDRSZ.
        varsize_4b(b) as i32 - VARHDRSZ as i32
    }
}

/// `VARDATA_ANY(PTR)`: payload bytes of any non-external varlena form.
fn vardata_any(b: &[u8]) -> &[u8] {
    if varatt_is_1b(b) {
        &b[1..]
    } else {
        &b[VARHDRSZ..]
    }
}

/// `VARSIZE(PTR)` for a 4-byte-header datum.
fn varsize(b: &[u8]) -> u32 {
    varsize_4b(b)
}

/// `VARDATA_COMPRESSED_GET_EXTSIZE(PTR)`: the raw (uncompressed) payload size,
/// `va_tcinfo & VARLENA_EXTSIZE_MASK`.
fn vardata_compressed_get_extsize(b: &[u8]) -> u32 {
    let tcinfo = u32::from_ne_bytes([
        b[VARHDRSZ],
        b[VARHDRSZ + 1],
        b[VARHDRSZ + 2],
        b[VARHDRSZ + 3],
    ]);
    tcinfo & VARLENA_EXTSIZE_MASK
}

/// `VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR)`: `va_tcinfo >>
/// VARLENA_EXTSIZE_BITS`.
fn vardata_compressed_get_compress_method(b: &[u8]) -> i32 {
    let tcinfo = u32::from_ne_bytes([
        b[VARHDRSZ],
        b[VARHDRSZ + 1],
        b[VARHDRSZ + 2],
        b[VARHDRSZ + 3],
    ]);
    (tcinfo >> VARLENA_EXTSIZE_BITS) as i32
}

/// `SET_VARSIZE_4B(PTR, len)`: stamp the uncompressed 4-byte length word
/// (`len << 2`).
fn set_varsize(b: &mut [u8], size: i32) {
    let header = (size as u32) << 2;
    b[..VARHDRSZ].copy_from_slice(&header.to_ne_bytes());
}

/// `SET_VARSIZE_COMPRESSED(PTR, len)`: stamp the compressed 4-byte length word
/// (`(len << 2) | 0x02`).
fn set_varsize_compressed(b: &mut [u8], size: i32) {
    let header = ((size as u32) << 2) | 0x02;
    b[..VARHDRSZ].copy_from_slice(&header.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// struct varatt_external (the on-disk TOAST pointer)
// ---------------------------------------------------------------------------

/// `va_extinfo` (low 30 bits = external saved size; top 2 bits = compress
/// method) extracted from an on-disk TOAST pointer datum.
struct VarattExternal {
    va_rawsize: i32,
    va_extinfo: u32,
}

/// `VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)`: decode the on-disk TOAST
/// pointer that begins just after the 2-byte external header (`va_header`,
/// `va_tag`).
fn varatt_external_get_pointer(b: &[u8]) -> VarattExternal {
    // struct varatt_external { int32 va_rawsize; uint32 va_extinfo; ... }
    // begins at offset VARHDRSZ_EXTERNAL (2).
    let p = &b[2..];
    let va_rawsize = i32::from_ne_bytes([p[0], p[1], p[2], p[3]]);
    let va_extinfo = u32::from_ne_bytes([p[4], p[5], p[6], p[7]]);
    VarattExternal {
        va_rawsize,
        va_extinfo,
    }
}

/// `VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)`: the external saved size is
/// strictly smaller than the raw size (header excluded).
fn varatt_external_is_compressed(p: &VarattExternal) -> bool {
    (p.va_extinfo & VARLENA_EXTSIZE_MASK) < (p.va_rawsize as u32).wrapping_sub(VARHDRSZ as u32)
}

/// `VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer)`: `va_extinfo >>
/// VARLENA_EXTSIZE_BITS`.
fn varatt_external_get_compress_method(p: &VarattExternal) -> i32 {
    (p.va_extinfo >> VARLENA_EXTSIZE_BITS) as i32
}

// ---------------------------------------------------------------------------
// PGLZ wrappers
// ---------------------------------------------------------------------------

/// `pglz_compress_datum(value)` (toast_compression.c): compress a varlena using
/// PGLZ.
///
/// Returns the compressed varlena bytes, or `None` if compression fails (input
/// size out of the strategy's allowed range, or the data is incompressible).
pub fn pglz_compress_datum<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let valsize = varsize_any_exhdr(value);

    // No point in wasting a palloc cycle if value size is outside the allowed
    // range for compression. (PGLZ_strategy_default's window.)
    let strategy = ::pglz::PGLZ_strategy_default();
    if valsize < strategy.min_input_size || valsize > strategy.max_input_size {
        return Ok(None);
    }

    // Figure out the maximum possible size of the pglz output, add the bytes
    // that will be needed for varlena overhead, and allocate that amount.
    let mut tmp: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, PGLZ_MAX_OUTPUT(valsize as usize) + VARHDRSZ_COMPRESSED)?;
    tmp.resize(VARHDRSZ_COMPRESSED, 0);

    // pglz_compress(VARDATA_ANY(value), valsize, (char *) tmp + VARHDRSZ_COMPRESSED, NULL).
    let compressed = match pglz_compress(mcx, vardata_any(value), None)? {
        Ok(out) => out,
        // len < 0: compression failed. pfree(tmp); return NULL.
        Err(_) => return Ok(None),
    };
    let len = compressed.len() as i32;
    tmp.extend_from_slice(&compressed);

    // SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED).
    set_varsize_compressed(&mut tmp, len + VARHDRSZ_COMPRESSED as i32);

    Ok(Some(tmp))
}

/// `pglz_decompress_datum(value)` (toast_compression.c): decompress a varlena
/// that was compressed using PGLZ.
pub fn pglz_decompress_datum<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let extsize = vardata_compressed_get_extsize(value) as i32;

    // allocate memory for the uncompressed data:
    //   palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ).
    let mut result: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, extsize as usize + VARHDRSZ)?;
    result.resize(extsize as usize + VARHDRSZ, 0);

    // pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
    //                 VARSIZE(value) - VARHDRSZ_COMPRESSED,
    //                 VARDATA(result), extsize, true).
    let source = &value[VARHDRSZ_COMPRESSED..varsize(value) as usize];
    let rawsize = match pglz_decompress_to_slice(source, &mut result[VARHDRSZ..VARHDRSZ + extsize as usize], true) {
        Ok(n) => n as i32,
        // rawsize < 0: corrupt data.
        Err(_) => return Err(corrupt_pglz()),
    };

    // SET_VARSIZE(result, rawsize + VARHDRSZ).
    set_varsize(&mut result, rawsize + VARHDRSZ as i32);
    result.truncate(rawsize as usize + VARHDRSZ);

    Ok(result)
}

/// `pglz_decompress_datum_slice(value, slicelength)` (toast_compression.c):
/// decompress the front `slicelength` bytes of a PGLZ-compressed varlena.
pub fn pglz_decompress_datum_slice<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
    slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // allocate memory for the uncompressed data: palloc(slicelength + VARHDRSZ).
    let mut result: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, slicelength as usize + VARHDRSZ)?;
    result.resize(slicelength as usize + VARHDRSZ, 0);

    // pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
    //                 VARSIZE(value) - VARHDRSZ_COMPRESSED,
    //                 VARDATA(result), slicelength, false).
    let source = &value[VARHDRSZ_COMPRESSED..varsize(value) as usize];
    let rawsize = match pglz_decompress_to_slice(source, &mut result[VARHDRSZ..VARHDRSZ + slicelength as usize], false) {
        Ok(n) => n as i32,
        Err(_) => return Err(corrupt_pglz()),
    };

    // SET_VARSIZE(result, rawsize + VARHDRSZ).
    set_varsize(&mut result, rawsize + VARHDRSZ as i32);
    result.truncate(rawsize as usize + VARHDRSZ);

    Ok(result)
}

/// `ereport(ERROR, ERRCODE_DATA_CORRUPTED, "compressed pglz data is corrupt")`.
fn corrupt_pglz() -> PgError {
    PgError::error("compressed pglz data is corrupt").with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

// ---------------------------------------------------------------------------
// LZ4 wrappers (non-LZ4 build: every path is NO_LZ4_SUPPORT())
// ---------------------------------------------------------------------------

/// `lz4_compress_datum(value)` (toast_compression.c): on a non-LZ4 build, the
/// `#ifndef USE_LZ4` branch raises `NO_LZ4_SUPPORT()`.
pub fn lz4_compress_datum<'mcx>(
    _mcx: Mcx<'mcx>,
    _value: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    Err(no_lz4_support())
}

/// `lz4_decompress_datum(value)` (toast_compression.c): non-LZ4 build →
/// `NO_LZ4_SUPPORT()`.
pub fn lz4_decompress_datum<'mcx>(
    _mcx: Mcx<'mcx>,
    _value: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    Err(no_lz4_support())
}

/// `lz4_decompress_datum_slice(value, slicelength)` (toast_compression.c):
/// non-LZ4 build → `NO_LZ4_SUPPORT()`.
pub fn lz4_decompress_datum_slice<'mcx>(
    _mcx: Mcx<'mcx>,
    _value: &[u8],
    _slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    Err(no_lz4_support())
}

// ---------------------------------------------------------------------------
// toast_get_compression_id / name<->method mapping
// ---------------------------------------------------------------------------

/// `toast_get_compression_id(attr)` (toast_compression.c): extract the
/// compression-method id from a varlena.
///
/// Returns [`TOAST_INVALID_COMPRESSION_ID`] if the varlena is not compressed.
pub fn toast_get_compression_id(attr: &[u8]) -> ToastCompressionId {
    let mut cmid = TOAST_INVALID_COMPRESSION_ID;

    // If it is stored externally then fetch the compression method id from the
    // external toast pointer. If compressed inline, fetch it from the toast
    // compression header.
    if varatt_is_external_ondisk(attr) {
        let toast_pointer = varatt_external_get_pointer(attr);
        if varatt_external_is_compressed(&toast_pointer) {
            cmid = varatt_external_get_compress_method(&toast_pointer);
        }
    } else if varatt_is_compressed(attr) {
        cmid = vardata_compressed_get_compress_method(attr);
    }

    cmid
}

/// `CompressionNameToMethod(compression)` (toast_compression.c): map a
/// compression name to its `pg_attribute.attcompression` method char.
///
/// Returns `Ok(InvalidCompressionMethod)` when the name is not a built-in
/// method; an `Err(NO_LZ4_SUPPORT())` on a non-LZ4 build asked for "lz4".
pub fn compression_name_to_method(compression: &[u8]) -> PgResult<u8> {
    if compression == b"pglz" {
        Ok(TOAST_PGLZ_COMPRESSION)
    } else if compression == b"lz4" {
        // #ifndef USE_LZ4: NO_LZ4_SUPPORT();
        Err(no_lz4_support())
    } else {
        Ok(INVALID_COMPRESSION_METHOD)
    }
}

/// `GetCompressionMethodName(method)` (toast_compression.c): the textual name
/// for a compression-method char.
///
/// `Err` carries `elog(ERROR, "invalid compression method %c", method)`.
pub fn get_compression_method_name(method: u8) -> PgResult<&'static str> {
    match method {
        TOAST_PGLZ_COMPRESSION => Ok("pglz"),
        TOAST_LZ4_COMPRESSION => Ok("lz4"),
        _ => Err(PgError::error(format!(
            "invalid compression method {}",
            method as char
        ))),
    }
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's seams. Contains only `set()` calls; `init_all()` in
/// `seams-init` invokes it at startup.
std::thread_local! {
    /// `int default_toast_compression = TOAST_PGLZ_COMPRESSION` ('p' = 112)
    /// (toast_compression.c:26) — backing store for the guc-table enum slot;
    /// PGC_USERSET, boot value 112.
    static DEFAULT_TOAST_COMPRESSION: core::cell::Cell<i32> = const { core::cell::Cell::new(112) };
}

pub fn init_seams() {
    seams::lz4_decompress_datum::set(lz4_decompress_datum);
    seams::lz4_decompress_datum_slice::set(lz4_decompress_datum_slice);

    // toast_compression.c owns the `default_toast_compression` GUC global
    // (read by toast_internals.c). Install the guc-table slot accessors over
    // our backing cell so the GUC engine can read/write it.
    guc_tables::vars::default_toast_compression.install(
        guc_tables::GucVarAccessors {
            get: || DEFAULT_TOAST_COMPRESSION.with(core::cell::Cell::get),
            set: |v| DEFAULT_TOAST_COMPRESSION.with(|c| c.set(v)),
        },
    );
}
