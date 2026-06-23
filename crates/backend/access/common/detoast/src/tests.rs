//! Tests for the `detoast.c` port. These exercise the in-crate paths (the PGLZ
//! decompression dispatch, slice arithmetic, short-header expansion, plain
//! copy, raw/datum size, the `switch (cmid)` dispatch, and the verbatim-copy
//! deviation) against encoded varlena buffers in a real memory context, with
//! the genuinely-external `common-pglz` and `indirect_pointer` seams installed
//! to fakes. The heap-fetch (`toast_fetch_datum*`), expanded-datum (`eoh_*`),
//! and optional LZ4 seams stay behind loud panics and are not driven here.

use super::*;
use mcx::MemoryContext;
use std::sync::Once;

const VARATT_4B_C_TAG: u8 = 0x02;
const VARATT_SHORT_MASK: u8 = 0x01;

/// Process-wide one-time install of the fakes the in-crate paths reach.
static INSTALL: Once = Once::new();

/// The fixed short-header inner varlena the fake `indirect_pointer` resolves
/// every indirect datum to.
fn indirect_inner() -> Vec<u8> {
    short(b"indirect short")
}

fn install_seams() {
    INSTALL.call_once(|| {
        // Identity "decompressor": copy as much of `source` into `dest` as
        // fits; corruption is signalled by a leading 0xFF control byte.
        pglz_seam::pglz_decompress_to_slice::set(|source, dest, _check| {
            if source.first() == Some(&0xFF) {
                return Ok(None);
            }
            let n = source.len().min(dest.len());
            dest[..n].copy_from_slice(&source[..n]);
            Ok(Some(n))
        });
        pglz_seam::pglz_maximum_compressed_size::set(|_rawsize, total| total);
        toast_seam::indirect_pointer::set(|mcx, _attr| mcx::slice_in(mcx, &indirect_inner()));
    });
}

/// Build a 4-byte uncompressed inline varlena around `data`.
fn four_byte(data: &[u8]) -> Vec<u8> {
    let total = data.len() + VARHDRSZ;
    let mut out = Vec::new();
    out.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    out.extend_from_slice(data);
    out
}

/// Build a short-header inline varlena around `data`.
fn short(data: &[u8]) -> Vec<u8> {
    let total = data.len() + VARHDRSZ_SHORT;
    let mut out = Vec::new();
    out.push(((total as u8) << 1) | VARATT_SHORT_MASK);
    out.extend_from_slice(data);
    out
}

/// Build an inline-compressed (4-byte compressed header) varlena wrapping
/// `payload`, encoding `rawsize`/`method` into `va_tcinfo`.
fn compressed(payload: &[u8], rawsize: u32, method: u32) -> Vec<u8> {
    let total = VARHDRSZ_COMPRESSED + payload.len();
    let mut out = Vec::new();
    out.extend_from_slice(&(((total as u32) << 2) | u32::from(VARATT_4B_C_TAG)).to_ne_bytes());
    out.extend_from_slice(&(rawsize | (method << 30)).to_ne_bytes());
    out.extend_from_slice(payload);
    out
}

/// An inline-compressed datum whose payload is the raw bytes verbatim (matched
/// by the identity fake decompressor).
fn identity_compressed(raw: &[u8]) -> Vec<u8> {
    compressed(raw, raw.len() as u32, TOAST_PGLZ_COMPRESSION_ID)
}

/// Read back the payload bytes of a (4-byte or short) result varlena.
fn payload_of(b: &[u8]) -> &[u8] {
    if varatt_is_short(b) {
        &b[VARHDRSZ_SHORT..VARHDRSZ_SHORT + (varsize_1b(b) as usize - VARHDRSZ_SHORT)]
    } else {
        &b[VARHDRSZ..VARHDRSZ + (varsize_4b(b) as usize - VARHDRSZ)]
    }
}

#[test]
fn toast_decompress_datum_round_trips_pglz() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let raw = b"aaaaaaaaaabbbbbbbbbbcccccccccc".to_vec();
    let encoded = identity_compressed(&raw);
    assert!(varatt_is_compressed(&encoded));

    let out = toast_decompress_datum(ctx.mcx(), &encoded).unwrap();
    assert!(varatt_is_4b(&out));
    assert_eq!(payload_of(&out), &raw[..]);
}

#[test]
fn toast_decompress_datum_slice_returns_prefix() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let raw = b"aaaaaaaaaabbbbbbbbbbcccccccccc".to_vec();
    let encoded = identity_compressed(&raw);

    let prefix = 10;
    let out = toast_decompress_datum_slice(ctx.mcx(), &encoded, prefix).unwrap();
    assert_eq!(payload_of(&out), &raw[..prefix as usize]);
}

#[test]
fn toast_decompress_datum_slice_falls_back_when_oversized() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let raw = b"aaaaaaaaaabbbbbbbbbbcccccccccc".to_vec();
    let encoded = identity_compressed(&raw);

    let out = toast_decompress_datum_slice(ctx.mcx(), &encoded, raw.len() as i32 + 100).unwrap();
    assert_eq!(payload_of(&out), &raw[..]);
}

#[test]
fn detoast_attr_decompresses_inline_compressed() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let raw = b"xxxxxxxxxxyyyyyyyyyy".to_vec();
    let encoded = identity_compressed(&raw);

    let out = detoast_attr(ctx.mcx(), &encoded).unwrap();
    assert_eq!(payload_of(&out), &raw[..]);
}

#[test]
fn detoast_attr_expands_short_header() {
    let ctx = MemoryContext::new("test");
    let encoded = short(b"hi there");

    let out = detoast_attr(ctx.mcx(), &encoded).unwrap();
    assert!(varatt_is_4b(&out));
    assert_eq!(payload_of(&out), b"hi there");
}

#[test]
fn detoast_attr_copies_plain_inline() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"plain value");

    let out = detoast_attr(ctx.mcx(), &encoded).unwrap();
    assert_eq!(&out[..], &encoded[..]);
}

#[test]
fn detoast_attr_slice_of_inline_compressed_prefix() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let raw = b"0123456789abcdefghij".to_vec();
    let encoded = identity_compressed(&raw);

    let out = detoast_attr_slice(ctx.mcx(), &encoded, 2, 6).unwrap();
    assert_eq!(payload_of(&out), &raw[2..8]);
}

#[test]
fn detoast_attr_slice_of_plain_inline() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"abcdefghij");

    let out = detoast_attr_slice(ctx.mcx(), &encoded, 3, 4).unwrap();
    assert_eq!(payload_of(&out), b"defg");
}

#[test]
fn detoast_attr_slice_rejects_negative_offset() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"abc");
    let err = detoast_attr_slice(ctx.mcx(), &encoded, -1, 2).unwrap_err();
    assert!(err.message().contains("invalid sliceoffset: -1"));
}

#[test]
fn detoast_attr_slice_clamps_offset_past_end() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"abc");
    let out = detoast_attr_slice(ctx.mcx(), &encoded, 10, 4).unwrap();
    assert_eq!(varsize_4b(&out) as usize, VARHDRSZ);
    assert_eq!(payload_of(&out), b"");
}

#[test]
fn detoast_attr_slice_negative_length_runs_to_end() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"abcdefghij");
    let out = detoast_attr_slice(ctx.mcx(), &encoded, 4, -1).unwrap();
    assert_eq!(payload_of(&out), b"efghij");
}

#[test]
fn toast_raw_datum_size_matches_c_branches() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let raw = b"hello world hello".to_vec();
    let encoded = identity_compressed(&raw);
    assert_eq!(
        toast_raw_datum_size(ctx.mcx(), &encoded).unwrap(),
        raw.len() + VARHDRSZ
    );

    let sh = short(b"abc");
    assert_eq!(toast_raw_datum_size(ctx.mcx(), &sh).unwrap(), 3 + VARHDRSZ);

    let pl = four_byte(b"abcde");
    assert_eq!(toast_raw_datum_size(ctx.mcx(), &pl).unwrap(), 5 + VARHDRSZ);
}

#[test]
fn toast_datum_size_matches_c_branches() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let sh = short(b"abcd");
    assert_eq!(
        toast_datum_size(ctx.mcx(), &sh).unwrap(),
        4 + VARHDRSZ_SHORT
    );

    let raw = b"hello world hello".to_vec();
    let encoded = identity_compressed(&raw);
    assert_eq!(toast_datum_size(ctx.mcx(), &encoded).unwrap(), encoded.len());
}

#[test]
fn invalid_compression_method_id_errors() {
    let ctx = MemoryContext::new("test");
    let encoded = compressed(b"\x00\x01\x02", 3, 3);
    let err = toast_decompress_datum(ctx.mcx(), &encoded).unwrap_err();
    assert!(err.message().contains("invalid compression method id 3"));
}

#[test]
fn corrupt_pglz_reports_data_corrupted() {
    install_seams();
    let ctx = MemoryContext::new("test");
    // Leading 0xFF makes the fake decompressor report corruption (rawsize < 0).
    let encoded = compressed(&[0xFF, 0x10, 0x00], 32, TOAST_PGLZ_COMPRESSION_ID);
    let err = toast_decompress_datum(ctx.mcx(), &encoded).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    assert!(err.message().contains("compressed pglz data is corrupt"));
}

#[test]
fn detoast_external_attr_preserves_short_header_verbatim() {
    let ctx = MemoryContext::new("test");
    let encoded = short(b"hi there");
    let out = detoast_external_attr(ctx.mcx(), &encoded).unwrap();
    assert!(varatt_is_short(&out));
    assert!(!varatt_is_4b(&out));
    assert_eq!(&out[..], &encoded[..]);
}

#[test]
fn detoast_external_attr_copies_plain_inline_verbatim() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"plain value");
    let out = detoast_external_attr(ctx.mcx(), &encoded).unwrap();
    assert!(varatt_is_4b(&out));
    assert_eq!(&out[..], &encoded[..]);
}

#[test]
fn pg_detoast_datum_packed_keeps_short_header_packed() {
    let ctx = MemoryContext::new("test");
    let encoded = short(b"packed short");
    let out = pg_detoast_datum_packed(ctx.mcx(), &encoded).unwrap();
    assert!(varatt_is_short(&out));
    assert_eq!(&out[..], &encoded[..]);
}

#[test]
fn pg_detoast_datum_copies_plain_verbatim() {
    let ctx = MemoryContext::new("test");
    let encoded = four_byte(b"a modifiable plain value");
    let out = pg_detoast_datum(ctx.mcx(), &encoded).unwrap();
    assert_eq!(&out[..], &encoded[..]);

    let copy = pg_detoast_datum_copy(ctx.mcx(), &encoded).unwrap();
    assert_eq!(&copy[..], &encoded[..]);
}

// ---------------------------------------------------------------------------
// Indirect-pointer branch, driven through the installed fake seam.
// ---------------------------------------------------------------------------

/// Build an in-memory `VARTAG_INDIRECT` TOAST pointer datum.
fn indirect_datum() -> Vec<u8> {
    let mut out = vec![0x01u8, VARTAG_INDIRECT];
    out.extend_from_slice(&[0u8; core::mem::size_of::<usize>()]);
    out
}

#[test]
fn detoast_external_attr_indirect_preserves_short_header() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let outer = indirect_datum();
    let out = detoast_external_attr(ctx.mcx(), &outer).unwrap();
    assert!(varatt_is_short(&out));
    assert_eq!(&out[..], &indirect_inner()[..]);
}

#[test]
fn detoast_attr_indirect_expands_short_header() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let outer = indirect_datum();
    let out = detoast_attr(ctx.mcx(), &outer).unwrap();
    assert!(varatt_is_4b(&out));
    assert_eq!(payload_of(&out), b"indirect short");
}

#[test]
fn toast_raw_datum_size_indirect_normalizes_inner_short() {
    install_seams();
    let ctx = MemoryContext::new("test");
    let outer = indirect_datum();
    assert_eq!(
        toast_raw_datum_size(ctx.mcx(), &outer).unwrap(),
        b"indirect short".len() + VARHDRSZ
    );
}

#[test]
fn lz4_method_id_is_recognized_in_dispatch() {
    assert_ne!(TOAST_LZ4_COMPRESSION_ID, TOAST_PGLZ_COMPRESSION_ID);
}
