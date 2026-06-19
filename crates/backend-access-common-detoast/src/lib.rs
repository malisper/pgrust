//! Port of PostgreSQL 18.3 `src/backend/access/common/detoast.c`.
//!
//! Retrieve compressed or external variable-size attributes. Public entry
//! points `detoast_external_attr`, `detoast_attr`, `detoast_attr_slice`,
//! `toast_raw_datum_size`, `toast_datum_size`; the static decompression
//! dispatch `toast_decompress_datum` / `toast_decompress_datum_slice` (the
//! `switch (cmid)` from `toast_compression.c`) and the PGLZ decompression path
//! (`pglz_decompress_datum` / `pglz_decompress_datum_slice`); and the
//! `pg_detoast_datum*` family that `fmgr.c` inlines.
//!
//! ## The owned model vs. C's pointer / Datum model
//!
//! `detoast.c` traffics in `struct varlena *` (a `char *` into a varlena
//! buffer) and returns freshly `palloc`'d results. Here a varlena `Datum`
//! argument is its raw *encoded bytes* (`&[u8]`, header included — exactly what
//! `DatumGetPointer` dereferences), and a produced varlena is an
//! [`mcx::PgVec<u8>`] allocated in the caller's memory context. The varlena
//! header bit-twiddling (`VARATT_IS_*`, `VARSIZE*`, `VARDATA*`, `SET_VARSIZE*`)
//! is pure `varatt.h` logic with no external dependency, ported in-crate.
//!
//! **Deliberate deviation from C (always-owned results).** Several C branches
//! return the *input pointer* unchanged with no allocation (the plain-value
//! `else` of `detoast_external_attr`/`detoast_attr`, and the
//! `pg_detoast_datum*` family's `else return datum;`). This always-owned port
//! returns a verbatim `VARSIZE_ANY` copy of the input instead, so every result
//! is a distinct, freeable buffer; callers must not rely on `result == input`
//! identity. The copy is byte-for-byte (it preserves a short or compressed
//! header exactly), matching C's contract that such a result "can still be
//! compressed or have a short header".
//!
//! ## What is seamed
//!
//! Genuinely cross-subsystem operations cross owner seams: fetching TOAST
//! chunks from the heap (`toast_fetch_datum` / `toast_fetch_datum_slice`) and
//! dereferencing indirect TOAST pointers (`indirect_pointer`) via the
//! cycle-partner `backend-access-common-toast-internals` seam crate; LZ4
//! decompression (`lz4_decompress_datum` / `lz4_decompress_datum_slice`, an
//! optional `#ifdef USE_LZ4` build dependency) via the `toast-compression`
//! seam; flattening expanded objects (`EOH_get_flat_size` / `EOH_flatten_into`)
//! via the expanded-datum (`misc2`) seam; and the not-yet-ported PGLZ
//! decompressor via the `common-pglz` seam. Each panics until its owner lands.

use mcx::{Mcx, PgVec};
use types_datum::expandeddatum::{VARTAG_EXPANDED_RO, VARTAG_EXPANDED_RW};
use types_datum::{ExpandedObjectRef, VARHDRSZ};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};

use backend_access_common_toast_compression_seams as lz4_seam;
use backend_access_common_toast_internals_seams as toast_seam;
use backend_utils_adt_misc2_seams as eoh_seam;
use common_pglz_seams as pglz_seam;

/// `Size` (`c.h`).
type Size = usize;

/// `ToastCompressionId` (`access/toast_compression.h`): the 2-bit method id.
type ToastCompressionId = u32;
/// `TOAST_PGLZ_COMPRESSION_ID` (`access/toast_compression.h`).
const TOAST_PGLZ_COMPRESSION_ID: ToastCompressionId = 0;
/// `TOAST_LZ4_COMPRESSION_ID` (`access/toast_compression.h`).
const TOAST_LZ4_COMPRESSION_ID: ToastCompressionId = 1;

/// `VARTAG_INDIRECT` (`varatt.h`).
const VARTAG_INDIRECT: u8 = 1;
/// `VARTAG_ONDISK` (`varatt.h`).
const VARTAG_ONDISK: u8 = 18;

/// `VARLENA_EXTSIZE_BITS` (`varatt.h`): the external/raw size occupies the low
/// 30 bits of the `va_extinfo` / `va_tcinfo` word.
const VARLENA_EXTSIZE_BITS: u32 = 30;
/// `VARLENA_EXTSIZE_MASK` (`varatt.h`): `(1U << VARLENA_EXTSIZE_BITS) - 1`.
const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/// `VARHDRSZ_EXTERNAL` (`varatt.h`): `offsetof(varattrib_1b_e, va_data)` —
/// the 1-byte `va_header` plus the 1-byte `va_tag`.
const VARHDRSZ_EXTERNAL: usize = 2;
/// `VARHDRSZ_SHORT` (`varatt.h`): a short (1-byte header) varlena's header.
const VARHDRSZ_SHORT: usize = 1;
/// `VARHDRSZ_COMPRESSED` (`varatt.h`): `offsetof(varattrib_4b,
/// va_compressed.va_data)` — the 4-byte length word plus the 4-byte
/// `va_tcinfo` field that precede the compressed payload.
const VARHDRSZ_COMPRESSED: usize = VARHDRSZ + 4;

/// `struct varatt_external` (`varatt.h`): the on-disk TOAST-pointer payload.
#[derive(Clone, Copy, Debug)]
struct VarattExternal {
    /// `va_rawsize`: original datum size, header included.
    va_rawsize: i32,
    /// `va_extinfo`: external saved size (low 30 bits) + compression method
    /// (top 2 bits).
    va_extinfo: u32,
    /// `va_valueid`: unique ID of value within the toast table.
    va_valueid: u32,
    /// `va_toastrelid`: RelID of the TOAST table containing it.
    #[allow(dead_code)]
    va_toastrelid: u32,
}

// ---------------------------------------------------------------------------
// Local varlena/TOAST-pointer header helpers (pure `varatt.h` bit-twiddling,
// no external dependency). They operate on the raw encoded varlena bytes.
// ---------------------------------------------------------------------------

/// `VARATT_IS_EXTERNAL(PTR)` for a 1-byte-header datum: `va_header == 0x01`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_EXTERNAL_ONDISK(PTR)`: external form with `va_tag == VARTAG_ONDISK`.
#[inline]
fn varatt_is_external_ondisk(b: &[u8]) -> bool {
    varatt_is_external(b) && b.len() >= 2 && b[1] == VARTAG_ONDISK
}

/// `VARATT_IS_EXTERNAL_INDIRECT(PTR)`: external form with `va_tag == VARTAG_INDIRECT`.
#[inline]
fn varatt_is_external_indirect(b: &[u8]) -> bool {
    varatt_is_external(b) && b.len() >= 2 && b[1] == VARTAG_INDIRECT
}

/// `VARATT_IS_EXTERNAL_EXPANDED(PTR)`: external form whose `va_tag` is
/// `VARTAG_EXPANDED_RO` or `VARTAG_EXPANDED_RW`.
#[inline]
fn varatt_is_external_expanded(b: &[u8]) -> bool {
    varatt_is_external(b)
        && b.len() >= 2
        && (b[1] == VARTAG_EXPANDED_RO || b[1] == VARTAG_EXPANDED_RW)
}

/// `VARATT_IS_COMPRESSED(PTR)` == `VARATT_IS_4B_C(PTR)`: 4-byte header, low two
/// bits `0b10`.
#[inline]
fn varatt_is_compressed(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

/// `VARATT_IS_4B(PTR)`: 4-byte-header form (low two bits `0b00`).
#[inline]
fn varatt_is_4b(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x00
}

/// `VARATT_IS_1B(PTR)`: any 1-byte-header form (low bit set).
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)`: the external (TOAST-pointer) 1-byte form
/// (`va_header == 0x01`).
#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_SHORT(PTR)` == `VARATT_IS_1B(PTR)`.
#[inline]
fn varatt_is_short(b: &[u8]) -> bool {
    varatt_is_1b(b)
}

/// `VARATT_IS_EXTENDED(PTR)`: NOT a plain 4-byte uncompressed datum.
#[inline]
fn varatt_is_extended(b: &[u8]) -> bool {
    !varatt_is_4b(b)
}

/// `VARSIZE(PTR)` == `VARSIZE_4B(PTR)`: the 4-byte length word `>> 2`, masked to
/// 30 bits (little-endian build).
#[inline]
fn varsize_4b(b: &[u8]) -> u32 {
    let word = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    #[cfg(target_endian = "big")]
    let len = word & VARLENA_EXTSIZE_MASK;
    #[cfg(target_endian = "little")]
    let len = (word >> 2) & VARLENA_EXTSIZE_MASK;
    len
}

/// `VARSIZE_SHORT(PTR)` == `VARSIZE_1B(PTR)`: `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> u32 {
    ((b[0] >> 1) & 0x7f) as u32
}

/// `VARSIZE_ANY(PTR)`: total on-the-wire size of any varlena form.
fn varsize_any(b: &[u8]) -> PgResult<usize> {
    if varatt_is_1b_e(b) {
        // VARSIZE_EXTERNAL = VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL).
        let tag = *b
            .get(1)
            .ok_or_else(|| PgError::error("truncated external datum"))?;
        Ok(VARHDRSZ_EXTERNAL + vartag_size(tag)?)
    } else if varatt_is_1b(b) {
        Ok(varsize_1b(b) as usize)
    } else {
        Ok(varsize_4b(b) as usize)
    }
}

/// `VARTAG_SIZE(tag)` (`varatt.h`): the type-specific payload size of an
/// external (1B_E) TOAST pointer.
fn vartag_size(tag: u8) -> PgResult<usize> {
    if tag == VARTAG_INDIRECT {
        // sizeof(varatt_indirect) — a single in-memory pointer.
        Ok(core::mem::size_of::<usize>())
    } else if tag == VARTAG_EXPANDED_RO || tag == VARTAG_EXPANDED_RW {
        // sizeof(varatt_expanded) — a single in-memory pointer.
        Ok(core::mem::size_of::<usize>())
    } else if tag == VARTAG_ONDISK {
        // sizeof(varatt_external) — four packed 4-byte fields.
        Ok(16)
    } else {
        // TrapMacro: VARTAG_SIZE rejects any other tag.
        Err(PgError::error(format!("unrecognized TOAST vartag {tag}")))
    }
}

/// `va_tcinfo` of a compressed datum: the 4-byte word after the length word.
fn tcinfo(b: &[u8]) -> PgResult<u32> {
    let word = b
        .get(VARHDRSZ..VARHDRSZ + 4)
        .ok_or_else(|| PgError::error("truncated compressed datum header"))?;
    Ok(u32::from_ne_bytes([word[0], word[1], word[2], word[3]]))
}

/// `TOAST_COMPRESS_METHOD(attr)` — compression method id in a compressed
/// datum's `va_tcinfo` (top two bits).
fn toast_compress_method(b: &[u8]) -> PgResult<ToastCompressionId> {
    Ok(tcinfo(b)? >> VARLENA_EXTSIZE_BITS)
}

/// `TOAST_COMPRESS_EXTSIZE(attr)` — external (raw) size in `va_tcinfo`.
fn toast_compress_extsize(b: &[u8]) -> PgResult<u32> {
    Ok(tcinfo(b)? & VARLENA_EXTSIZE_MASK)
}

/// `VARDATA_COMPRESSED_GET_EXTSIZE(attr)` — raw payload size embedded in a
/// compressed datum's `va_tcinfo` (low 30 bits).
fn vardata_compressed_get_extsize(b: &[u8]) -> PgResult<i32> {
    Ok((tcinfo(b)? & VARLENA_EXTSIZE_MASK) as i32)
}

/// `VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer)` — external saved size.
#[inline]
fn external_get_extsize(p: &VarattExternal) -> u32 {
    p.va_extinfo & VARLENA_EXTSIZE_MASK
}

/// `VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer)` — compression method.
#[inline]
fn external_get_compress_method(p: &VarattExternal) -> ToastCompressionId {
    p.va_extinfo >> VARLENA_EXTSIZE_BITS
}

/// `VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)`: the external saved size is
/// smaller than the raw size (minus header overhead).
#[inline]
fn external_is_compressed(p: &VarattExternal) -> bool {
    external_get_extsize(p) < (p.va_rawsize as u32).wrapping_sub(VARHDRSZ as u32)
}

/// `VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)`: copy the unaligned
/// `varatt_external` payload out of an on-disk TOAST-pointer datum.
fn external_pointer(attr: &[u8]) -> PgResult<VarattExternal> {
    let payload = attr
        .get(VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + 16)
        .ok_or_else(|| PgError::error("truncated external TOAST pointer"))?;
    Ok(VarattExternal {
        va_rawsize: i32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]),
        va_extinfo: u32::from_ne_bytes([payload[4], payload[5], payload[6], payload[7]]),
        va_valueid: u32::from_ne_bytes([payload[8], payload[9], payload[10], payload[11]]),
        va_toastrelid: u32::from_ne_bytes([payload[12], payload[13], payload[14], payload[15]]),
    })
}

/// `SET_VARSIZE(ptr, size)` == `SET_VARSIZE_4B(ptr, size)` (`varatt.h`): stamp
/// the 4-byte length word with the total length, low two bits `0b00`.
fn set_varsize(bytes: &mut [u8], size: usize) {
    #[cfg(target_endian = "big")]
    let header = (size as u32) & VARLENA_EXTSIZE_MASK;
    #[cfg(target_endian = "little")]
    let header = (size as u32) << 2;
    bytes[..VARHDRSZ].copy_from_slice(&header.to_ne_bytes());
}

/// `VARDATA(PTR)` of a 4-byte-header varlena.
fn vardata_4b(b: &[u8]) -> &[u8] {
    &b[VARHDRSZ..]
}

/// `VARDATA_SHORT(PTR)` of a short-header varlena.
fn vardata_short(b: &[u8]) -> &[u8] {
    &b[VARHDRSZ_SHORT..]
}

/// `(char *) value + VARHDRSZ_COMPRESSED`, length `VARSIZE(value) -
/// VARHDRSZ_COMPRESSED`: the raw compressed payload. Compressed datums use the
/// 4-byte (`SET_VARSIZE_COMPRESSED`) header form, so `VARSIZE` == `VARSIZE_4B`.
fn compressed_payload(value: &[u8]) -> PgResult<&[u8]> {
    let varsize = varsize_4b(value) as usize;
    value
        .get(VARHDRSZ_COMPRESSED..varsize)
        .ok_or_else(|| PgError::error("truncated compressed datum"))
}

/// A verbatim `palloc(VARSIZE_ANY(attr)); memcpy(...)`: a distinct `mcx` copy
/// preserving the input's header form exactly.
fn copy_verbatim<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = varsize_any(attr)?;
    let src = attr
        .get(..len)
        .ok_or_else(|| PgError::error("truncated varlena"))?;
    mcx::slice_in(mcx, src)
}

/// `mcx`-allocated zeroed buffer of `len` bytes (C: `palloc(len)` then writes).
fn palloc_zeroed<'mcx>(mcx: Mcx<'mcx>, len: usize) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = mcx::vec_with_capacity_in(mcx, len)?;
    v.resize(len, 0);
    Ok(v)
}

// ---------------------------------------------------------------------------
// detoast_external_attr
// ---------------------------------------------------------------------------

/// `detoast_external_attr` — public entry point to get back a toasted value from
/// external source (possibly still in compressed format).
///
/// Returns a datum that contains all the data internally (not relying on
/// external storage or memory), but it can still be compressed or have a short
/// header.
pub fn detoast_external_attr<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if varatt_is_external_ondisk(attr) {
        // This is an external stored plain value.
        toast_seam::toast_fetch_datum::call(mcx, attr)
    } else if varatt_is_external_indirect(attr) {
        // This is an indirect pointer --- dereference it.
        let inner = toast_seam::indirect_pointer::call(mcx, attr)?;

        // nested indirect Datums aren't allowed (Assert in C).
        debug_assert!(!varatt_is_external_indirect(&inner));

        // recurse if value is still external in some other way.
        if varatt_is_external(&inner) {
            detoast_external_attr(mcx, &inner)
        } else {
            // Copy into the caller's memory context, in case caller tries to
            // pfree the result. C does palloc(VARSIZE_ANY(attr)); memcpy(...).
            copy_verbatim(mcx, &inner)
        }
    } else if varatt_is_external_expanded(attr) {
        // This is an expanded-object pointer --- get flat format.
        let eoh = ExpandedObjectRef::from_expanded_datum_bytes(attr);
        let resultsize = eoh_seam::eoh_get_flat_size::call(eoh)?;
        let mut result = palloc_zeroed(mcx, resultsize)?;
        eoh_seam::eoh_flatten_into::call(eoh, &mut result)?;
        Ok(result)
    } else {
        // This is a plain value inside of the main tuple - why am I called?
        // C returns `attr` unchanged; this always-owned port returns a verbatim
        // copy (see the module-level deviation note).
        copy_verbatim(mcx, attr)
    }
}

// ---------------------------------------------------------------------------
// detoast_attr
// ---------------------------------------------------------------------------

/// `detoast_attr` — public entry point to get back a toasted value from
/// compression or external storage. The result is always non-extended varlena
/// form.
pub fn detoast_attr<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if varatt_is_external_ondisk(attr) {
        // This is an externally stored datum --- fetch it back from there.
        let fetched = toast_seam::toast_fetch_datum::call(mcx, attr)?;
        // If it's compressed, decompress it.
        if varatt_is_compressed(&fetched) {
            let result = toast_decompress_datum(mcx, &fetched)?;
            // pfree(tmp): drop the owned fetched buffer.
            drop(fetched);
            Ok(result)
        } else {
            Ok(fetched)
        }
    } else if varatt_is_external_indirect(attr) {
        // This is an indirect pointer --- dereference it.
        let inner = toast_seam::indirect_pointer::call(mcx, attr)?;

        // nested indirect Datums aren't allowed (Assert in C).
        debug_assert!(!varatt_is_external_indirect(&inner));

        // recurse in case value is still extended in some other way.
        detoast_attr(mcx, &inner)
    } else if varatt_is_external_expanded(attr) {
        // This is an expanded-object pointer --- get flat format.
        // flatteners are not allowed to produce compressed/short output.
        let result = detoast_external_attr(mcx, attr)?;
        debug_assert!(!varatt_is_extended(&result));
        Ok(result)
    } else if varatt_is_compressed(attr) {
        // This is a compressed value inside of the main tuple.
        toast_decompress_datum(mcx, attr)
    } else if varatt_is_short(attr) {
        // This is a short-header varlena --- convert to 4-byte header format.
        let data_size = varsize_1b(attr) as usize - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let mut new_attr = palloc_zeroed(mcx, new_size)?;
        set_varsize(&mut new_attr, new_size);
        new_attr[VARHDRSZ..].copy_from_slice(&vardata_short(attr)[..data_size]);
        Ok(new_attr)
    } else {
        // Plain 4-byte uncompressed value: C falls through and returns `attr`
        // unchanged. This always-owned port returns a verbatim copy (see the
        // module-level deviation note).
        copy_verbatim(mcx, attr)
    }
}

// ---------------------------------------------------------------------------
// detoast_attr_slice
// ---------------------------------------------------------------------------

/// `detoast_attr_slice` — public entry point to get back part of a toasted value
/// from compression or external storage.
///
/// `sliceoffset` is where to start (zero or more); if `slicelength < 0`, return
/// everything beyond `sliceoffset`.
pub fn detoast_attr_slice<'mcx>(
    mcx: Mcx<'mcx>,
    attr: &[u8],
    sliceoffset: i32,
    mut slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    if sliceoffset < 0 {
        // elog(ERROR, "invalid sliceoffset: %d", sliceoffset);
        return Err(PgError::error(format!("invalid sliceoffset: {sliceoffset}")));
    }

    // Compute slicelimit = offset + length, or -1 if we must fetch all of the
    // value. In case of integer overflow, we must fetch all.
    let slicelimit: i32 = if slicelength < 0 {
        -1
    } else {
        match sliceoffset.checked_add(slicelength) {
            // pg_add_s32_overflow returned false: slicelimit = sum.
            Some(limit) => limit,
            // pg_add_s32_overflow returned true: slicelength = slicelimit = -1.
            None => {
                slicelength = -1;
                -1
            }
        }
    };

    // Fetch / flatten `preslice`. For the inline case it stays borrowed from
    // `attr` (`preslice == attr` in C); for external/expanded cases it is freshly
    // owned. Keep the owning allocation alive so the borrowed view stays valid
    // through the final memcpy.
    let preslice_owned: Option<PgVec<'mcx, u8>> = if varatt_is_external_ondisk(attr) {
        let toast_pointer = external_pointer(attr)?;

        // fast path for non-compressed external datums.
        if !external_is_compressed(&toast_pointer) {
            return toast_seam::toast_fetch_datum_slice::call(mcx, attr, sliceoffset, slicelength);
        }

        // For compressed values, we need to fetch enough slices to decompress at
        // least the requested part (when a prefix is requested). Otherwise, just
        // fetch all slices.
        let fetched = if slicelimit >= 0 {
            let mut max_size = external_get_extsize(&toast_pointer) as i32;

            // Determine maximum amount of compressed data needed for a prefix of
            // a given length (after decompression). LZ4 has no such API, so we
            // fetch the whole thing for LZ4.
            if external_get_compress_method(&toast_pointer) == TOAST_PGLZ_COMPRESSION_ID {
                max_size = pglz_seam::pglz_maximum_compressed_size::call(slicelimit, max_size);
            }

            // Fetch enough compressed slices (compressed marker will get set
            // automatically).
            toast_seam::toast_fetch_datum_slice::call(mcx, attr, 0, max_size)?
        } else {
            toast_seam::toast_fetch_datum::call(mcx, attr)?
        };
        Some(fetched)
    } else if varatt_is_external_indirect(attr) {
        let inner = toast_seam::indirect_pointer::call(mcx, attr)?;
        // nested indirect Datums aren't allowed (Assert in C).
        debug_assert!(!varatt_is_external_indirect(&inner));
        return detoast_attr_slice(mcx, &inner, sliceoffset, slicelength);
    } else if varatt_is_external_expanded(attr) {
        // pass it off to detoast_external_attr to flatten.
        Some(detoast_external_attr(mcx, attr)?)
    } else {
        None
    };

    // Borrow the fetched/flattened buffer (`preslice`), or `attr` directly for
    // the inline case.
    let preslice: &[u8] = match preslice_owned.as_ref() {
        Some(owned) => owned,
        None => attr,
    };

    // Assert(!VARATT_IS_EXTERNAL(preslice));
    debug_assert!(!varatt_is_external(preslice));

    // Decompress enough to encompass the slice and the offset, if compressed.
    let decompressed: Option<PgVec<'mcx, u8>> = if varatt_is_compressed(preslice) {
        let tmp = if slicelimit >= 0 {
            toast_decompress_datum_slice(mcx, preslice, slicelimit)?
        } else {
            toast_decompress_datum(mcx, preslice)?
        };
        Some(tmp)
    } else {
        None
    };
    let view: &[u8] = match decompressed.as_ref() {
        Some(owned) => owned,
        None => preslice,
    };

    // attrdata / attrsize: VARDATA_SHORT/VARDATA depending on the header form.
    let (attrdata, attrsize): (&[u8], i32) = if varatt_is_short(view) {
        let data = vardata_short(view);
        let size = varsize_1b(view) as i32 - VARHDRSZ_SHORT as i32;
        (data, size)
    } else {
        let data = vardata_4b(view);
        let size = varsize_4b(view) as i32 - VARHDRSZ as i32;
        (data, size)
    };

    // slicing of datum for compressed cases and plain value.
    let mut sliceoffset = sliceoffset;
    if sliceoffset >= attrsize {
        sliceoffset = 0;
        slicelength = 0;
    } else if slicelength < 0 || slicelimit > attrsize {
        slicelength = attrsize - sliceoffset;
    }

    let offset = sliceoffset as usize;
    let length = slicelength as usize;
    let total_len = length + VARHDRSZ;
    let mut result = palloc_zeroed(mcx, total_len)?;
    set_varsize(&mut result, total_len);
    let src = attrdata
        .get(offset..offset + length)
        .ok_or_else(|| PgError::error("truncated varlena slice"))?;
    result[VARHDRSZ..].copy_from_slice(src);

    // The result has been copied out: free the decompressed buffer and the
    // fetched/flattened preslice (C's `if (tmp != attr) pfree(tmp)` and `if
    // (preslice != attr) pfree(preslice)`). Both are always distinct from `attr`
    // in this port (the inline path uses None).
    drop(decompressed);
    drop(preslice_owned);

    Ok(result)
}

// ---------------------------------------------------------------------------
// toast_decompress_datum / toast_decompress_datum_slice (toast_compression.c)
// ---------------------------------------------------------------------------

/// `toast_decompress_datum` — decompress a compressed version of a varlena datum
/// (the `toast_compression.c` `switch (cmid)` dispatch + the PGLZ decompressor).
pub fn toast_decompress_datum<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // Assert(VARATT_IS_COMPRESSED(attr));
    debug_assert!(varatt_is_compressed(attr));

    // Fetch the compression method id stored in the compression header and
    // decompress the data using the appropriate decompression routine.
    let cmid = toast_compress_method(attr)?;
    if cmid == TOAST_PGLZ_COMPRESSION_ID {
        pglz_decompress_datum(mcx, attr)
    } else if cmid == TOAST_LZ4_COMPRESSION_ID {
        lz4_seam::lz4_decompress_datum::call(mcx, attr)
    } else {
        // elog(ERROR, "invalid compression method id %d", cmid);
        Err(PgError::error(format!("invalid compression method id {cmid}")))
    }
}

/// `toast_decompress_datum_slice` — decompress the front of a compressed version
/// of a varlena datum. (Offset handling happens in `detoast_attr_slice`; here we
/// just decompress a slice from the front.)
pub fn toast_decompress_datum_slice<'mcx>(
    mcx: Mcx<'mcx>,
    attr: &[u8],
    slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // Assert(VARATT_IS_COMPRESSED(attr));
    debug_assert!(varatt_is_compressed(attr));

    // Some callers may pass a slicelength that's more than the actual
    // decompressed size. If so, just decompress normally. This avoids possibly
    // allocating a larger-than-necessary result object, and may be faster and/or
    // more robust as well.
    if (slicelength as u32) >= toast_compress_extsize(attr)? {
        return toast_decompress_datum(mcx, attr);
    }

    // Fetch the compression method id stored in the compression header and
    // decompress the data slice using the appropriate decompression routine.
    let cmid = toast_compress_method(attr)?;
    if cmid == TOAST_PGLZ_COMPRESSION_ID {
        pglz_decompress_datum_slice(mcx, attr, slicelength)
    } else if cmid == TOAST_LZ4_COMPRESSION_ID {
        lz4_seam::lz4_decompress_datum_slice::call(mcx, attr, slicelength)
    } else {
        // elog(ERROR, "invalid compression method id %d", cmid);
        Err(PgError::error(format!("invalid compression method id {cmid}")))
    }
}

/// `pglz_decompress_datum` (toast_compression.c) — decompress a PGLZ-compressed
/// varlena over the `common-pglz` decompressor.
fn pglz_decompress_datum<'mcx>(mcx: Mcx<'mcx>, value: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let rawmax = vardata_compressed_get_extsize(value)? as usize;

    // allocate memory for the uncompressed data.
    let mut result = palloc_zeroed(mcx, rawmax + VARHDRSZ)?;

    // decompress the data: source is the bytes past the compressed header
    // ((char *) value + VARHDRSZ_COMPRESSED), length VARSIZE - VARHDRSZ_COMPRESSED.
    let source = compressed_payload(value)?;
    let rawsize = match pglz_seam::pglz_decompress_to_slice::call(
        source,
        &mut result[VARHDRSZ..VARHDRSZ + rawmax],
        true,
    )? {
        Some(rawsize) => rawsize,
        // rawsize < 0
        None => return Err(corrupt_pglz()),
    };

    set_varsize(&mut result, rawsize + VARHDRSZ);
    Ok(result)
}

/// `pglz_decompress_datum_slice` (toast_compression.c) — decompress the front
/// `slicelength` bytes of a PGLZ-compressed varlena.
fn pglz_decompress_datum_slice<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
    slicelength: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let slicelength = slicelength as usize;

    // allocate memory for the uncompressed data.
    let mut result = palloc_zeroed(mcx, slicelength + VARHDRSZ)?;

    // decompress the data (check_complete = false).
    let source = compressed_payload(value)?;
    let rawsize = match pglz_seam::pglz_decompress_to_slice::call(
        source,
        &mut result[VARHDRSZ..VARHDRSZ + slicelength],
        false,
    )? {
        Some(rawsize) => rawsize,
        // rawsize < 0
        None => return Err(corrupt_pglz()),
    };

    set_varsize(&mut result, rawsize + VARHDRSZ);
    Ok(result)
}

// ---------------------------------------------------------------------------
// toast_raw_datum_size / toast_datum_size
// ---------------------------------------------------------------------------

/// `toast_raw_datum_size` — raw (detoasted) size of a varlena datum, including
/// the `VARHDRSZ` header.
pub fn toast_raw_datum_size(mcx: Mcx<'_>, value: &[u8]) -> PgResult<Size> {
    if varatt_is_external_ondisk(value) {
        // va_rawsize is the size of the original datum -- including header.
        let toast_pointer = external_pointer(value)?;
        Ok(toast_pointer.va_rawsize as Size)
    } else if varatt_is_external_indirect(value) {
        let inner = toast_seam::indirect_pointer::call(mcx, value)?;
        // nested indirect Datums aren't allowed (Assert in C).
        debug_assert!(!varatt_is_external_indirect(&inner));
        toast_raw_datum_size(mcx, &inner)
    } else if varatt_is_external_expanded(value) {
        let eoh = ExpandedObjectRef::from_expanded_datum_bytes(value);
        eoh_seam::eoh_get_flat_size::call(eoh)
    } else if varatt_is_compressed(value) {
        // here, va_rawsize is just the payload size.
        Ok(vardata_compressed_get_extsize(value)? as Size + VARHDRSZ)
    } else if varatt_is_short(value) {
        // we have to normalize the header length to VARHDRSZ or else the callers
        // of this function will be confused.
        Ok(varsize_1b(value) as Size - VARHDRSZ_SHORT + VARHDRSZ)
    } else {
        // plain untoasted datum.
        Ok(varsize_4b(value) as Size)
    }
}

/// `toast_datum_size` — physical storage size (possibly compressed) of a varlena
/// datum.
pub fn toast_datum_size(mcx: Mcx<'_>, value: &[u8]) -> PgResult<Size> {
    if varatt_is_external_ondisk(value) {
        // Attribute is stored externally - return the extsize whether compressed
        // or not. We do not count the size of the toast pointer.
        let toast_pointer = external_pointer(value)?;
        Ok(external_get_extsize(&toast_pointer) as Size)
    } else if varatt_is_external_indirect(value) {
        let inner = toast_seam::indirect_pointer::call(mcx, value)?;
        // nested indirect Datums aren't allowed (Assert in C).
        debug_assert!(!varatt_is_external_indirect(&inner));
        toast_datum_size(mcx, &inner)
    } else if varatt_is_external_expanded(value) {
        let eoh = ExpandedObjectRef::from_expanded_datum_bytes(value);
        eoh_seam::eoh_get_flat_size::call(eoh)
    } else if varatt_is_short(value) {
        Ok(varsize_1b(value) as Size)
    } else {
        // Attribute is stored inline either compressed or not, just calculate the
        // size of the datum in either case.
        Ok(varsize_4b(value) as Size)
    }
}

/// Seam `toast_datum_size` — `toast_datum_size(value)` returning the size as a
/// plain `usize` (the seam carrier). Thin wrapper over [`toast_datum_size`].
fn toast_datum_size_seam(mcx: Mcx<'_>, attr: &[u8]) -> PgResult<usize> {
    Ok(toast_datum_size(mcx, attr)? as usize)
}

/// Seam `toast_raw_datum_size` — `toast_raw_datum_size(value)` over the canonical
/// 6-arm value lane. The seam carries the value as `types_tuple::Datum<'mcx>`
/// (the executor/ADT value form); a varlena element keeps its bytes inline, so
/// we forward the stored image to [`toast_raw_datum_size`].
fn toast_raw_datum_size_seam<'mcx>(
    mcx: Mcx<'mcx>,
    value: types_tuple::Datum<'mcx>,
) -> PgResult<i64> {
    Ok(toast_raw_datum_size(mcx, value.as_ref_bytes())? as i64)
}

/// Seam `toast_chunk_id` — `pg_column_toast_chunk_id`'s
/// `VARATT_IS_EXTERNAL_ONDISK(attr)` test + `VARATT_EXTERNAL_GET_POINTER`'s
/// `va_valueid` extraction (varlena.c:5403-5408): the TOAST value OID of an
/// on-disk external varlena, or `None` when not stored on-disk-external.
fn toast_chunk_id(attr: &[u8]) -> PgResult<Option<types_core::Oid>> {
    if !varatt_is_external_ondisk(attr) {
        return Ok(None);
    }
    let toast_pointer = external_pointer(attr)?;
    Ok(Some(toast_pointer.va_valueid as types_core::Oid))
}

// ---------------------------------------------------------------------------
// pg_detoast_datum family (fmgr.c) — the in-crate detoast helpers fmgr.c
// inlines. Ported here against the owned model.
// ---------------------------------------------------------------------------

/// `pg_detoast_datum` (fmgr.c) — detoast a possibly-extended datum.
///
/// C returns a non-extended datum unchanged (`else return datum;`); this
/// always-owned port returns a verbatim `VARSIZE_ANY` copy instead (see the
/// module-level deviation note).
pub fn pg_detoast_datum<'mcx>(mcx: Mcx<'mcx>, datum: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if varatt_is_extended(datum) {
        detoast_attr(mcx, datum)
    } else {
        copy_verbatim(mcx, datum)
    }
}

/// `pg_detoast_datum_copy` (fmgr.c) — like `pg_detoast_datum`, but always returns
/// a fresh modifiable copy of a non-extended datum.
pub fn pg_detoast_datum_copy<'mcx>(mcx: Mcx<'mcx>, datum: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if varatt_is_extended(datum) {
        detoast_attr(mcx, datum)
    } else {
        // Make a modifiable copy of the varlena object (len = VARSIZE(datum)).
        copy_verbatim(mcx, datum)
    }
}

/// `pg_detoast_datum_slice` (fmgr.c) — get the specified portion from the toast
/// relation.
pub fn pg_detoast_datum_slice<'mcx>(
    mcx: Mcx<'mcx>,
    datum: &[u8],
    first: i32,
    count: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    detoast_attr_slice(mcx, datum, first, count)
}

/// `pg_detoast_datum_packed` (fmgr.c) — detoast only compressed/external datums.
/// C leaves a short-header or 4-byte uncompressed value *packed*; this
/// always-owned port returns a verbatim `VARSIZE_ANY` copy instead, which keeps
/// a short header short (see the module-level deviation note).
pub fn pg_detoast_datum_packed<'mcx>(mcx: Mcx<'mcx>, datum: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if varatt_is_compressed(datum) || varatt_is_external(datum) {
        detoast_attr(mcx, datum)
    } else {
        copy_verbatim(mcx, datum)
    }
}

// ---------------------------------------------------------------------------
// error helpers
// ---------------------------------------------------------------------------

/// `ereport(ERROR, errcode(ERRCODE_DATA_CORRUPTED),
/// errmsg_internal("compressed pglz data is corrupt"))` — raised when a PGLZ
/// payload cannot be decompressed to its claimed raw size (`rawsize < 0`).
fn corrupt_pglz() -> PgError {
    PgError::error("compressed pglz data is corrupt").with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

// ---------------------------------------------------------------------------
// seam installation
// ---------------------------------------------------------------------------

/// Install this crate's inbound seams (`backend-access-common-detoast-seams`).
pub fn init_seams() {
    backend_access_common_detoast_seams::detoast_external_attr::set(detoast_external_attr);
    backend_access_common_detoast_seams::detoast_attr::set(detoast_attr);
    backend_access_common_detoast_seams::pg_detoast_datum_packed::set(pg_detoast_datum_packed);
    backend_access_common_detoast_seams::toast_datum_size::set(toast_datum_size_seam);
    backend_access_common_detoast_seams::toast_raw_datum_size::set(toast_raw_datum_size_seam);
    backend_access_common_detoast_seams::toast_chunk_id::set(toast_chunk_id);
}

#[cfg(test)]
mod tests;
