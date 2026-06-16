//! The on-disk byte codec for the minmax-multi summary.
//!
//! `brin_range_serialize`/`brin_range_deserialize` (brin_minmax_multi.c:575/720)
//! pack the boundary values into a varlena `SerializedRanges` blob (header +
//! per-value bytes) using `store_att_byval`/`fetch_att` for by-value types and a
//! `memcpy` of the on-disk image for by-reference / varlena / cstring types.
//! Here the blob rides the canonical `Datum::ByRef` byte lane in
//! `column.bv_values[0]`; the [`SerializedRanges`] is the parsed-out
//! intermediate, and [`serialize_summary`] / [`deserialize_summary`] do the
//! byte (un)packing, recovering `typbyval`/`typlen` from the lsyscache seam.
//!
//! Header layout (matches C `offsetof(SerializedRanges, data) == 20`):
//!   vl_len_(4) typid(4) nranges(4) nvalues(4) maxvalues(4) | data...

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_cache_lsyscache_seams as lsyscache;

/// `SerializedRanges` (brin_minmax_multi.c:205): the parsed-out on-disk summary
/// (header fields + the `2*nranges + nvalues` boundary values, already in
/// [`Datum`] form).
pub struct SerializedRanges<'mcx> {
    pub typid: Oid,
    pub nranges: i32,
    pub nvalues: i32,
    pub maxvalues: i32,
    pub values: PgVec<'mcx, Datum<'mcx>>,
}

/// `offsetof(SerializedRanges, data)`: 4+4+4+4+4.
const DATA_OFFSET: usize = 20;

/// `MAXALIGN(len)` over a `usize` length (MAXIMUM_ALIGNOF == 8).
#[inline]
fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// `VARSIZE_ANY(ptr)`: the total varlena size from a 4-byte (uncompressed,
/// non-toasted) header. The minmax-multi summary is always built with a 4-byte
/// header (`SET_VARSIZE`), and only detoasted images reach this codec.
#[inline]
fn varsize_4b(bytes: &[u8]) -> usize {
    let word = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    #[cfg(target_endian = "little")]
    {
        ((word >> 2) & 0x3FFF_FFFF) as usize
    }
    #[cfg(target_endian = "big")]
    {
        (word & 0x3FFF_FFFF) as usize
    }
}

/// `SET_VARSIZE(ptr, len)` into the first 4 bytes (4-byte header, low 2 bits 00).
#[inline]
fn set_varsize(buf: &mut [u8], len: usize) {
    let word: u32;
    #[cfg(target_endian = "little")]
    {
        word = (len as u32) << 2;
    }
    #[cfg(target_endian = "big")]
    {
        word = len as u32;
    }
    buf[0..4].copy_from_slice(&word.to_ne_bytes());
}

/// `strlen(cstring) + 1`: the C-string length including the NUL terminator. The
/// cstring image in a `Datum::ByRef` does not store a NUL (per the repo's
/// `RefPayload::Cstring` convention), so we add 1 for the terminator C writes.
#[inline]
fn cstring_len_with_nul(bytes: &[u8]) -> usize {
    // The stored image is the raw text bytes; the on-disk form adds a NUL.
    bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len()) + 1
}

/// `brin_range_serialize`'s byte-packing leg (brin_minmax_multi.c:646-708):
/// produce the on-disk varlena image of a [`SerializedRanges`] as a
/// `Datum::ByRef`. `typbyval`/`typlen` of the boundary-value type drive the
/// per-value encoding.
pub fn serialize_summary<'mcx>(
    mcx: Mcx<'mcx>,
    s: &SerializedRanges<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let typbyval = lsyscache::get_typbyval::call(s.typid)?;
    let typlen = lsyscache::get_typlen::call(s.typid)?;

    let nvalues = (2 * s.nranges + s.nvalues) as usize;
    debug_assert_eq!(nvalues, s.values.len());

    // compute the data length (header + per-value bytes)
    let mut data_len: usize = 0;
    if typlen == -1 {
        // varlena
        for v in &s.values {
            data_len += varsize_4b(v.as_ref_bytes());
        }
    } else if typlen == -2 {
        // cstring (+ NUL)
        for v in &s.values {
            data_len += cstring_len_with_nul(v.as_ref_bytes());
        }
    } else {
        // fixed-length types (by-value or by-reference)
        debug_assert!(typlen > 0);
        data_len += nvalues * typlen as usize;
    }

    let total = DATA_OFFSET + data_len;
    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, total)?;
    for _ in 0..total {
        buf.push(0u8);
    }

    set_varsize(&mut buf, total);
    buf[4..8].copy_from_slice(&s.typid.to_ne_bytes());
    buf[8..12].copy_from_slice(&s.nranges.to_ne_bytes());
    buf[12..16].copy_from_slice(&s.nvalues.to_ne_bytes());
    buf[16..20].copy_from_slice(&s.maxvalues.to_ne_bytes());

    // copy the boundary values
    let mut ptr = DATA_OFFSET;
    for v in &s.values {
        if typbyval {
            // store_att_byval: the low `typlen` significant bytes of the word.
            let word = (v.as_usize() as u64).to_ne_bytes();
            let n = typlen as usize;
            #[cfg(target_endian = "little")]
            buf[ptr..ptr + n].copy_from_slice(&word[..n]);
            #[cfg(target_endian = "big")]
            buf[ptr..ptr + n].copy_from_slice(&word[8 - n..]);
            ptr += n;
        } else if typlen > 0 {
            // fixed-length by-ref types: copy typlen bytes of the image.
            let bytes = v.as_ref_bytes();
            buf[ptr..ptr + typlen as usize].copy_from_slice(&bytes[..typlen as usize]);
            ptr += typlen as usize;
        } else if typlen == -1 {
            // varlena: copy VARSIZE_ANY bytes of the image.
            let bytes = v.as_ref_bytes();
            let sz = varsize_4b(bytes);
            buf[ptr..ptr + sz].copy_from_slice(&bytes[..sz]);
            ptr += sz;
        } else {
            // cstring: copy strlen+1 bytes (the trailing slot is already 0).
            let bytes = v.as_ref_bytes();
            let sz = cstring_len_with_nul(bytes);
            buf[ptr..ptr + sz - 1].copy_from_slice(&bytes[..sz - 1]);
            ptr += sz;
        }
        debug_assert!(ptr <= total);
    }
    debug_assert_eq!(ptr, total);

    Ok(Datum::ByRef(buf))
}

/// `brin_range_deserialize`'s byte-unpacking leg + header parse: parse a
/// detoasted on-disk summary image (`column.bv_values[0]`, a `Datum::ByRef`)
/// into a [`SerializedRanges`] with the boundary values recovered into
/// [`Datum`]s (`fetch_att` for by-value, image copies for by-ref).
pub fn deserialize_summary<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
) -> PgResult<SerializedRanges<'mcx>> {
    let bytes = value.as_ref_bytes();
    debug_assert!(bytes.len() >= DATA_OFFSET);

    let typid = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let nranges = i32::from_ne_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let nvalues = i32::from_ne_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let maxvalues = i32::from_ne_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

    let typbyval = lsyscache::get_typbyval::call(typid)?;
    let typlen = lsyscache::get_typlen::call(typid)?;

    let total = (2 * nranges + nvalues) as usize;
    let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, total)?;

    let mut ptr = DATA_OFFSET;
    for _ in 0..total {
        if typbyval {
            // fetch_att: read typlen bytes into a zeroed word.
            let n = typlen as usize;
            let mut w = [0u8; 8];
            #[cfg(target_endian = "little")]
            w[..n].copy_from_slice(&bytes[ptr..ptr + n]);
            #[cfg(target_endian = "big")]
            w[8 - n..].copy_from_slice(&bytes[ptr..ptr + n]);
            values.push(Datum::from_usize(u64::from_ne_bytes(w) as usize));
            ptr += n;
        } else if typlen > 0 {
            // fixed-length by-ref: copy typlen bytes into an owned image.
            let n = typlen as usize;
            let img = slice_to_byref(mcx, &bytes[ptr..ptr + n])?;
            values.push(img);
            ptr += n;
        } else if typlen == -1 {
            // varlena: copy VARSIZE_ANY bytes into an owned image.
            let sz = varsize_4b(&bytes[ptr..]);
            let img = slice_to_byref(mcx, &bytes[ptr..ptr + sz])?;
            values.push(img);
            ptr += sz;
        } else {
            // cstring: copy strlen+1 bytes (image without the NUL terminator).
            let sz = cstring_len_with_nul(&bytes[ptr..]);
            let img = slice_to_byref(mcx, &bytes[ptr..ptr + sz - 1])?;
            values.push(img);
            ptr += sz;
        }
        debug_assert!(ptr <= bytes.len());
    }

    let _ = maxalign; // C MAXALIGNs the per-value scratch allocation only.

    Ok(SerializedRanges {
        typid,
        nranges,
        nvalues,
        maxvalues,
        values,
    })
}

/// Copy a byte slice into an owned `Datum::ByRef` in `mcx`.
pub(crate) fn slice_to_byref<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<Datum<'mcx>> {
    let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, src.len())?;
    for &b in src {
        v.push(b);
    }
    Ok(Datum::ByRef(v))
}
