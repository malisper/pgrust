#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `backend-utils-adt-encode` — port of PostgreSQL 18.3
//! `src/backend/utils/adt/encode.c`: the `encode(bytea, text)` /
//! `decode(text, text)` SQL functions and the `hex` / `base64` / `escape`
//! binary-text codecs they dispatch to.
//!
//! Every function from the C file is implemented. The codec algorithms
//! (`hex_encode`/`hex_decode`, `pg_base64_encode`/`pg_base64_decode`,
//! `esc_encode`/`esc_decode`, the `*_len` estimators, `get_hex`,
//! `pg_find_encoding`) are the C control flow, operating on byte slices. The
//! `struct pg_encoding` dispatch table (four function pointers selected by
//! [`pg_find_encoding`]) is the C `enclist[]` static array, modelled with
//! [`PgEncoding`]; `binary_encode`/`binary_decode` look up the codec by name,
//! size the output with the codec's `_len` estimator, allocate a result buffer,
//! run the conversion, and return the true-length result — including the
//! `MaxAllocSize` overflow check and the FATAL estimate-too-small guard.
//!
//! ## Seam contract (allocate-and-return)
//!
//! In C the codec cores write into a caller-provided `char *dst` that the caller
//! has already sized via `hex_enc_len`/`hex_dec_len`, returning the byte count.
//! The `bytea` I/O consumer (`byteain`/`byteaout`) crosses the
//! `backend-utils-adt-encode-seams` boundary, where a borrowed destination
//! buffer cannot be passed; the two installed seams `hex_encode`/
//! `hex_decode_safe` therefore allocate the result in the caller's `Mcx<'mcx>`
//! (the `palloc`'d destination analog) and return the exact written payload as a
//! [`PgVec<u8>`].
//!
//! The single genuine external is `pg_mblen_range` (`src/backend/utils/mb`),
//! used to size the `%.*s` snippet in the hex/base64 error messages; it crosses
//! the `backend-utils-mb-mbutils-seams` boundary.

use mcx::{Mcx, PgVec, MAX_ALLOC_SIZE};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED, FATAL,
};

use backend_utils_mb_mbutils_seams::pg_mblen_range;

/// C: `VARHDRSZ` — `sizeof(int32)`. Used only to size the `MaxAllocSize`
/// overflow guard exactly as the C does (`resultlen > MaxAllocSize - VARHDRSZ`).
const VARHDRSZ: u64 = 4;

/// C: `IS_HIGHBIT_SET(c)` — `((c) & 0x80) != 0`.
#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & 0x80 != 0
}

// ---------------------------------------------------------------------------
// Encoding conversion API.
// ---------------------------------------------------------------------------

/// C: `struct pg_encoding` — the encoding conversion API. `encode_len`/
/// `decode_len` compute an upper bound on the output size; `encode`/`decode`
/// perform the conversion into the caller's buffer and return the true length.
///
/// The `encode`/`decode` members are fallible here because the Rust codecs
/// return their hard errors through `PgResult` (vs C's `ereport` longjmp).
///
/// `decode_len` is *also* fallible: in C the `decode_len` slot has an infallible
/// signature, but `esc_dec_len` `ereport(ERROR)`s on a lone backslash *before*
/// `binary_decode` reaches `palloc`. To preserve that contract — the failure
/// surfacing from the length call, before any allocation — the Rust slot returns
/// `PgResult<u64>`: `escape` plugs in the fallible [`esc_dec_len`] directly,
/// while `hex`/`base64` (whose C estimators never error) wrap their length in
/// `Ok`. `encode_len` stays infallible, matching C.
#[derive(Clone, Copy)]
pub struct PgEncoding {
    pub encode_len: fn(&[u8]) -> u64,
    pub decode_len: fn(&[u8]) -> PgResult<u64>,
    pub encode: fn(&[u8], &mut [u8]) -> PgResult<u64>,
    pub decode: fn(&[u8], &mut [u8]) -> PgResult<u64>,
}

// ---------------------------------------------------------------------------
// SQL functions
//
// In C these are `Datum binary_encode(PG_FUNCTION_ARGS)` /
// `Datum binary_decode(...)`: they take the already-detoasted varlena plus an
// encoding-name text, palloc a result varlena, and SET_VARSIZE it. The SQL
// fmgr (bare-word PGFunction) registry is deferred project-wide, so the entry
// points are exposed here as `*_bytes` cores on plain bytes, ready for the fmgr
// layer to drive once it lands.
// ---------------------------------------------------------------------------

/// The `binary_encode` core on plain bytes (C `binary_encode` after the
/// `PG_GETARG_*` unwrapping): detoasted `bytea` content `dataptr` plus encoding
/// `name`, producing the owned `text` content in `mcx`.
pub fn binary_encode_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    dataptr: &[u8],
    name: &str,
) -> PgResult<PgVec<'mcx, u8>> {
    let enc = match pg_find_encoding(name) {
        Some(enc) => enc,
        None => return Err(unrecognized_encoding(name)),
    };

    let resultlen = (enc.encode_len)(dataptr);

    run_conversion(mcx, dataptr, resultlen, enc.encode, true)
}

/// The `binary_decode` core on plain bytes (C `binary_decode`): detoasted `text`
/// content `dataptr` plus encoding `name`, producing the owned `bytea` content
/// in `mcx`.
pub fn binary_decode_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    dataptr: &[u8],
    name: &str,
) -> PgResult<PgVec<'mcx, u8>> {
    let enc = match pg_find_encoding(name) {
        Some(enc) => enc,
        None => return Err(unrecognized_encoding(name)),
    };

    // C: enc->decode_len(...). For "escape" this is `esc_dec_len`, which
    // `ereport(ERROR)`s on a lone backslash *before* `palloc` — so the failure
    // must surface here, before allocation, propagated with `?`.
    let resultlen = (enc.decode_len)(dataptr)?;

    run_conversion(mcx, dataptr, resultlen, enc.decode, false)
}

/// Shared body of `binary_encode`/`binary_decode` after encoding lookup and the
/// `resultlen` estimate: validate the estimate against `MaxAllocSize`, allocate
/// the result buffer in `mcx` (the `palloc`'d result analog), run `conv` into
/// it, FATAL-check the true length against the estimate, and truncate to the
/// true length. `encoding` selects the "encoding"/"decoding" and
/// "encode"/"decode" wording in the error variants, matching C.
fn run_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    dataptr: &[u8],
    resultlen: u64,
    conv: fn(&[u8], &mut [u8]) -> PgResult<u64>,
    encoding: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    // C: resultlen possibly overflows uint32, therefore on 32-bit machines it's
    // unsafe to rely on palloc's internal check.
    if resultlen > MAX_ALLOC_SIZE as u64 - VARHDRSZ {
        return Err(conversion_too_large(encoding));
    }

    // C: result = (text *) palloc(VARHDRSZ + resultlen). The result buffer is a
    // context-charged `PgVec<u8>` zero-filled to `resultlen`.
    let mut result = mcx::vec_with_capacity_in(mcx, resultlen as usize)?;
    result.resize(resultlen as usize, 0);

    // C: res = enc->encode/decode(VARDATA_ANY(data), datalen, VARDATA(result)).
    let res = conv(dataptr, result.as_mut_slice())?;

    // C: if (res > resultlen) elog(FATAL, ...). Make this FATAL 'cause we've
    // trodden on memory ...
    if res > resultlen {
        return Err(estimate_too_small(encoding));
    }

    // C: SET_VARSIZE(result, VARHDRSZ + res). Truncate to the true length.
    result.truncate(res as usize);
    Ok(result)
}

// ---------------------------------------------------------------------------
// Installed seams (allocate-and-return wrappers for the bytea I/O path).
// ---------------------------------------------------------------------------

/// Seam body for `hex_encode(mcx, src)`: allocate the `src.len() * 2`-byte hex
/// result in `mcx` and write the hex expansion into it. C: `byteaout` palloc's
/// `len*2 + 2 + 1` and calls `hex_encode(VARDATA_ANY(vlena), len, rp)`; this
/// returns just the hex bytes (the caller adds the `\x` prefix and NUL).
fn seam_hex_encode<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut dst = mcx::vec_with_capacity_in(mcx, src.len() * 2)?;
    dst.resize(src.len() * 2, 0);
    let n = hex_encode(src, dst.as_mut_slice());
    dst.truncate(n as usize);
    Ok(dst)
}

/// Seam body for `hex_decode_safe(mcx, src)`: allocate the `hex_dec_len`-sized
/// result in `mcx` and decode the hex text into it. C: `byteain` palloc's
/// `(len - 2) / 2 + VARHDRSZ` and calls
/// `hex_decode_safe(inputText + 2, len - 2, VARDATA(result), escontext)`.
///
/// The `&mut SoftErrorContext` sink cannot cross the seam boundary, so `soft`
/// (C's `escontext != NULL`) is carried as a bool: the owner builds a local
/// `SoftErrorContext`, lets [`hex_decode_safe`] `ereturn` into it, and on a
/// captured soft error returns the complete [`PgError`] as the inner `Err` of
/// the `Ok` arm so the consumer (`byteain`) can re-route it through its own
/// frame escontext. With `soft = false` the decode raises hard (the outer
/// `Err`), exactly as C's NULL-escontext path does.
fn seam_hex_decode_safe<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    soft: bool,
) -> PgResult<Result<PgVec<'mcx, u8>, PgError>> {
    let mut dst = mcx::vec_with_capacity_in(mcx, hex_dec_len(src) as usize)?;
    dst.resize(hex_dec_len(src) as usize, 0);

    if soft {
        // C: escontext != NULL — `hex_decode_safe` ereturns into it. Mirror with
        // a local sink that wants full details so the captured error carries the
        // message/detail/hint/sqlstate `pg_input_error_info` reports.
        let mut escontext = SoftErrorContext::new(true);
        let n = hex_decode_safe(src, dst.as_mut_slice(), Some(&mut escontext))?;
        if escontext.error_occurred() {
            // The soft error was captured; surface it as the inner `Err` for the
            // consumer to route through its own escontext.
            return Ok(Err(escontext
                .take_error()
                .expect("hex_decode_safe set error_occurred with details_wanted")));
        }
        dst.truncate(n as usize);
        Ok(Ok(dst))
    } else {
        // C: escontext == NULL — decode hard (a bad digit propagates as `Err`).
        let n = hex_decode_safe(src, dst.as_mut_slice(), None)?;
        dst.truncate(n as usize);
        Ok(Ok(dst))
    }
}

/// Install every seam this unit owns (`backend-utils-adt-encode-seams`):
/// `hex_encode` and `hex_decode_safe`, both consumed by the `bytea` I/O path
/// (`byteain`/`byteaout` in `backend-utils-adt-varlena`).
pub fn init_seams() {
    backend_utils_adt_encode_seams::hex_encode::set(seam_hex_encode);
    backend_utils_adt_encode_seams::hex_decode_safe::set(seam_hex_decode_safe);
    register_encode_builtins();
}

// ---------------------------------------------------------------------------
// fmgr builtins: `binary_encode` / `binary_decode` (C: their `fmgr_builtins[]`
// rows, pg_proc OIDs 1946 / 1947). These are the `encode(bytea, text)` /
// `decode(text, text)` SQL functions; they cross the by-ref varlena lane.
// ---------------------------------------------------------------------------

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// `VARDATA_ANY` payload bytes of a by-ref varlena arg: skip the 1-byte (short)
/// or 4-byte (long, uncompressed) header. Mirrors `vardata_any_slice` in
/// `backend-utils-adt-varlena`; inline literals (the only inputs these reach)
/// are never compressed/external.
fn vardata_any_slice(image: &[u8]) -> &[u8] {
    if image.is_empty() {
        return &[];
    }
    let header = image[0];
    if header != 0x01 && header & 0x01 == 0x01 {
        let total = (((header >> 1) & 0x7F) as usize).min(image.len());
        &image[1..total.max(1)]
    } else if image.len() >= VARHDRSZ as usize {
        &image[VARHDRSZ as usize..]
    } else {
        &[]
    }
}

/// `PG_GETARG_BYTEA_PP(i)` / `PG_GETARG_TEXT_PP(i)`: the detoasted payload bytes
/// of a by-ref varlena arg.
fn arg_varlena_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("encode fn: by-ref varlena arg missing from by-ref lane");
    vardata_any_slice(image)
}

/// Stamp a 4-byte uncompressed varlena header in front of a header-less payload
/// and set it as the by-ref `text`/`bytea` result (mirrors `ret_varlena` in
/// `backend-utils-adt-varlena`); the wire layer strips the header downstream.
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: &[u8]) -> Datum {
    let mut image = Vec::with_capacity(bytes.len() + VARHDRSZ as usize);
    image.extend_from_slice(&types_datum::varlena::set_varsize_4b(
        bytes.len() + VARHDRSZ as usize,
    ));
    image.extend_from_slice(bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Raise a builtin's `ereport(ERROR)` through the dispatch point every builtin
/// crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err)
}

/// `binary_encode(bytea, text) -> text` (C `binary_encode`).
fn fc_binary_encode(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let scratch = mcx::MemoryContext::new("binary_encode scratch");
    let result: Vec<u8> = {
        let data = arg_varlena_bytes(fcinfo, 0);
        let name = arg_varlena_bytes(fcinfo, 1);
        let name = std::str::from_utf8(name)
            .expect("encode: encoding name is database-encoding text");
        match binary_encode_bytes(scratch.mcx(), data, name) {
            Ok(v) => v.as_slice().to_vec(),
            Err(e) => raise(e),
        }
    };
    ret_varlena(fcinfo, &result)
}

/// `binary_decode(text, text) -> bytea` (C `binary_decode`).
fn fc_binary_decode(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let scratch = mcx::MemoryContext::new("binary_decode scratch");
    let result: Vec<u8> = {
        let data = arg_varlena_bytes(fcinfo, 0);
        let name = arg_varlena_bytes(fcinfo, 1);
        let name = std::str::from_utf8(name)
            .expect("decode: encoding name is database-encoding text");
        match binary_decode_bytes(scratch.mcx(), data, name) {
            Ok(v) => v.as_slice().to_vec(),
            Err(e) => raise(e),
        }
    };
    ret_varlena(fcinfo, &result)
}

/// Register the `encode.c` SQL builtins (C: their `fmgr_builtins[]` rows).
fn register_encode_builtins() {
    backend_utils_fmgr_core::register_builtins([
        BuiltinFunction {
            foid: 1946,
            name: "binary_encode".to_string(),
            nargs: 2,
            strict: true,
            retset: false,
            func: Some(fc_binary_encode),
        },
        BuiltinFunction {
            foid: 1947,
            name: "binary_decode".to_string(),
            nargs: 2,
            strict: true,
            retset: false,
            func: Some(fc_binary_decode),
        },
    ]);
}

// ---------------------------------------------------------------------------
// HEX
// ---------------------------------------------------------------------------

/// C: `hextbl[512]` — the hex expansion of each possible byte value (two
/// lowercase chars per value).
static HEXTBL: &[u8; 512] = b"\
000102030405060708090a0b0c0d0e0f\
101112131415161718191a1b1c1d1e1f\
202122232425262728292a2b2c2d2e2f\
303132333435363738393a3b3c3d3e3f\
404142434445464748494a4b4c4d4e4f\
505152535455565758595a5b5c5d5e5f\
606162636465666768696a6b6c6d6e6f\
707172737475767778797a7b7c7d7e7f\
808182838485868788898a8b8c8d8e8f\
909192939495969798999a9b9c9d9e9f\
a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
b0b1b2b3b4b5b6b7b8b9babbbcbdbebf\
c0c1c2c3c4c5c6c7c8c9cacbcccdcecf\
d0d1d2d3d4d5d6d7d8d9dadbdcdddedf\
e0e1e2e3e4e5e6e7e8e9eaebecedeeef\
f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

/// C: `hexlookup[128]` — maps an ASCII hex digit to its value, `-1` otherwise.
static HEXLOOKUP: [i8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1, //
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
];

/// C: `hex_encode(src, len, dst)` — write the hex expansion of `src` to `dst`;
/// returns `len * 2`.
pub fn hex_encode(src: &[u8], dst: &mut [u8]) -> u64 {
    let mut di = 0usize;
    for &usrc in src {
        let off = 2 * usrc as usize;
        dst[di..di + 2].copy_from_slice(&HEXTBL[off..off + 2]);
        di += 2;
    }
    src.len() as u64 * 2
}

/// C: `get_hex(cp, out)` (static inline) — decode one hex digit. Returns
/// `Some(value)` for a valid hex character (C's `*out`/`true`), `None`
/// otherwise (C's `false`).
#[inline]
pub fn get_hex(cp: u8) -> Option<u8> {
    let c = cp;
    let mut res: i32 = -1;

    if c < 127 {
        res = HEXLOOKUP[c as usize] as i32;
    }

    if res >= 0 {
        Some(res as u8)
    } else {
        None
    }
}

/// C: `hex_decode(src, len, dst)` — decode hex `src` into `dst`; errors hard on
/// invalid input (wraps [`hex_decode_safe`] with no soft-error context).
pub fn hex_decode(src: &[u8], dst: &mut [u8]) -> PgResult<u64> {
    hex_decode_safe(src, dst, None)
}

/// C: `hex_decode_safe(src, len, dst, escontext)` — as [`hex_decode`] but routes
/// `ERRCODE_INVALID_PARAMETER_VALUE` `invalid hexadecimal digit: "..."` /
/// `invalid hexadecimal data: odd number of digits` through the soft-error
/// context when one is supplied (C: `ereturn(escontext, 0, ...)`).
pub fn hex_decode_safe(
    src: &[u8],
    dst: &mut [u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<u64> {
    let srclen = src.len();
    let mut s = 0usize;
    let mut p = 0usize;

    while s < srclen {
        if src[s] == b' ' || src[s] == b'\n' || src[s] == b'\t' || src[s] == b'\r' {
            s += 1;
            continue;
        }
        let v1 = match get_hex(src[s]) {
            Some(v) => v,
            None => {
                return ereturn(escontext.as_deref_mut(), 0, invalid_hex_digit(&src[s..]));
            }
        };
        s += 1;
        if s >= srclen {
            return ereturn(escontext.as_deref_mut(), 0, invalid_hex_odd_digits());
        }
        let v2 = match get_hex(src[s]) {
            Some(v) => v,
            None => {
                return ereturn(escontext.as_deref_mut(), 0, invalid_hex_digit(&src[s..]));
            }
        };
        s += 1;
        dst[p] = (v1 << 4) | v2;
        p += 1;
    }

    Ok(p as u64)
}

/// C: `hex_enc_len(src, srclen)` (static) — `srclen << 1`.
pub fn hex_enc_len(src: &[u8]) -> u64 {
    (src.len() as u64) << 1
}

/// C: `hex_dec_len(src, srclen)` (static) — `srclen >> 1`.
pub fn hex_dec_len(src: &[u8]) -> u64 {
    (src.len() as u64) >> 1
}

// ---------------------------------------------------------------------------
// BASE64
// ---------------------------------------------------------------------------

/// C: `_base64[]` — the base64 alphabet.
static BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// C: `b64lookup[128]` — maps an ASCII base64 character to its 6-bit value,
/// `-1` otherwise.
static B64LOOKUP: [i8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, //
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, //
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, //
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, //
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, //
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, //
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, //
];

/// C: `pg_base64_encode(src, len, dst)` (static).
pub fn pg_base64_encode(src: &[u8], dst: &mut [u8]) -> PgResult<u64> {
    // C: char *lend = dst + 76; tracked as an absolute index into dst.
    let mut lend: usize = 76;
    let mut p: usize = 0; // C: p = dst
    let mut pos: i32 = 2;
    let mut buf: u32 = 0;

    for &byte in src {
        buf |= (byte as u32) << (pos << 3);
        pos -= 1;

        // write it out
        if pos < 0 {
            dst[p] = BASE64[((buf >> 18) & 0x3f) as usize];
            dst[p + 1] = BASE64[((buf >> 12) & 0x3f) as usize];
            dst[p + 2] = BASE64[((buf >> 6) & 0x3f) as usize];
            dst[p + 3] = BASE64[(buf & 0x3f) as usize];
            p += 4;

            pos = 2;
            buf = 0;
        }
        if p >= lend {
            dst[p] = b'\n';
            p += 1;
            lend = p + 76;
        }
    }
    if pos != 2 {
        dst[p] = BASE64[((buf >> 18) & 0x3f) as usize];
        dst[p + 1] = BASE64[((buf >> 12) & 0x3f) as usize];
        dst[p + 2] = if pos == 0 {
            BASE64[((buf >> 6) & 0x3f) as usize]
        } else {
            b'='
        };
        dst[p + 3] = b'=';
        p += 4;
    }

    Ok(p as u64)
}

/// C: `pg_base64_decode(src, len, dst)` (static).
///
/// Errors hard (`ereport(ERROR)`): `ERRCODE_INVALID_PARAMETER_VALUE`
/// `unexpected "=" while decoding base64 sequence` /
/// `invalid symbol "..." found while decoding base64 sequence` /
/// `invalid base64 end sequence`.
pub fn pg_base64_decode(src: &[u8], dst: &mut [u8]) -> PgResult<u64> {
    let srclen = src.len();
    let mut s = 0usize; // index into src
    let mut p = 0usize; // index into dst
    let mut b: i32;
    let mut buf: u32 = 0;
    let mut pos: i32 = 0;
    let mut end: i32 = 0;

    while s < srclen {
        let c = src[s];
        s += 1;

        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            continue;
        }

        if c == b'=' {
            // end sequence
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    return Err(unexpected_equals());
                }
            }
            b = 0;
        } else {
            b = -1;
            // C: if (c > 0 && c < 127) — `c` is a signed char in C, so the
            // high-bit-set range is excluded by `c > 0`.
            if c > 0 && c < 127 {
                b = B64LOOKUP[c as usize] as i32;
            }
            if b < 0 {
                return Err(invalid_base64_symbol(&src[s - 1..]));
            }
        }
        // add it to buffer
        buf = (buf << 6).wrapping_add(b as u32);
        pos += 1;
        if pos == 4 {
            dst[p] = ((buf >> 16) & 255) as u8;
            p += 1;
            if end == 0 || end > 1 {
                dst[p] = ((buf >> 8) & 255) as u8;
                p += 1;
            }
            if end == 0 || end > 2 {
                dst[p] = (buf & 255) as u8;
                p += 1;
            }
            buf = 0;
            pos = 0;
        }
    }

    if pos != 0 {
        return Err(invalid_base64_end());
    }

    Ok(p as u64)
}

/// C: `pg_base64_enc_len(src, srclen)` (static).
pub fn pg_base64_enc_len(src: &[u8]) -> u64 {
    // 3 bytes will be converted to 4, linefeed after 76 chars. Kept byte-for-byte
    // identical to the C expression `(srclen + 2) / 3 * 4 + srclen / (76 * 3 / 4)`
    // to preserve exact integer-division semantics.
    #[allow(clippy::manual_div_ceil)]
    {
        let srclen = src.len() as u64;
        (srclen + 2) / 3 * 4 + srclen / (76 * 3 / 4)
    }
}

/// C: `pg_base64_dec_len(src, srclen)` (static).
pub fn pg_base64_dec_len(src: &[u8]) -> u64 {
    (src.len() as u64 * 3) >> 2
}

// ---------------------------------------------------------------------------
// ESCAPE
//
// Minimally escape bytea to text; de-escape text to bytea. We must escape zero
// bytes and high-bit-set bytes, plus backslash itself. De-escaping processes
// `\\` and any `\###` octal.
// ---------------------------------------------------------------------------

/// C: `VAL(CH)` — `(CH) - '0'`.
#[inline]
fn val(ch: u8) -> i32 {
    (ch - b'0') as i32
}

/// C: `DIG(VAL)` — `(VAL) + '0'`.
#[inline]
fn dig(v: u8) -> u8 {
    v + b'0'
}

/// C: `esc_encode(src, srclen, dst)` (static).
pub fn esc_encode(src: &[u8], dst: &mut [u8]) -> PgResult<u64> {
    let mut rp = 0usize;
    let mut len: u64 = 0;

    for &c in src {
        if c == b'\0' || is_highbit_set(c) {
            dst[rp] = b'\\';
            dst[rp + 1] = dig(c >> 6);
            dst[rp + 2] = dig((c >> 3) & 7);
            dst[rp + 3] = dig(c & 7);
            rp += 4;
            len += 4;
        } else if c == b'\\' {
            dst[rp] = b'\\';
            dst[rp + 1] = b'\\';
            rp += 2;
            len += 2;
        } else {
            dst[rp] = c;
            rp += 1;
            len += 1;
        }
    }

    Ok(len)
}

/// C: `esc_decode(src, srclen, dst)` (static).
///
/// Errors hard: `ERRCODE_INVALID_TEXT_REPRESENTATION` `invalid input syntax for
/// type bytea`.
pub fn esc_decode(src: &[u8], dst: &mut [u8]) -> PgResult<u64> {
    let srclen = src.len();
    let mut si = 0usize; // index into src
    let mut rp = 0usize; // index into dst
    let mut len: u64 = 0;

    while si < srclen {
        if src[si] != b'\\' {
            dst[rp] = src[si];
            rp += 1;
            si += 1;
        } else if si + 3 < srclen
            && (src[si + 1] >= b'0' && src[si + 1] <= b'3')
            && (src[si + 2] >= b'0' && src[si + 2] <= b'7')
            && (src[si + 3] >= b'0' && src[si + 3] <= b'7')
        {
            let mut value = val(src[si + 1]);
            value <<= 3;
            value += val(src[si + 2]);
            value <<= 3;
            dst[rp] = (value + val(src[si + 3])) as u8;
            rp += 1;
            si += 4;
        } else if si + 1 < srclen && src[si + 1] == b'\\' {
            dst[rp] = b'\\';
            rp += 1;
            si += 2;
        } else {
            // One backslash, not followed by ### valid octal. Should never get
            // here, since esc_dec_len does same check.
            return Err(invalid_bytea_syntax());
        }

        len += 1;
    }

    Ok(len)
}

/// C: `esc_enc_len(src, srclen)` (static).
pub fn esc_enc_len(src: &[u8]) -> u64 {
    let mut len: u64 = 0;

    for &c in src {
        if c == b'\0' || is_highbit_set(c) {
            len += 4;
        } else if c == b'\\' {
            len += 2;
        } else {
            len += 1;
        }
    }

    len
}

/// C: `esc_dec_len(src, srclen)` (static).
///
/// This also performs the same hard-error check as [`esc_decode`]; the C
/// `decode_len` estimator `ereport`s before any allocation when given a lone
/// backslash, so the failure surfaces here too.
pub fn esc_dec_len(src: &[u8]) -> PgResult<u64> {
    let srclen = src.len();
    let mut si = 0usize;
    let mut len: u64 = 0;

    while si < srclen {
        if src[si] != b'\\' {
            si += 1;
        } else if si + 3 < srclen
            && (src[si + 1] >= b'0' && src[si + 1] <= b'3')
            && (src[si + 2] >= b'0' && src[si + 2] <= b'7')
            && (src[si + 3] >= b'0' && src[si + 3] <= b'7')
        {
            // backslash + valid octal
            si += 4;
        } else if si + 1 < srclen && src[si + 1] == b'\\' {
            // two backslashes = backslash
            si += 2;
        } else {
            // one backslash, not followed by ### valid octal
            return Err(invalid_bytea_syntax());
        }

        len += 1;
    }

    Ok(len)
}

// ---------------------------------------------------------------------------
// Common / dispatch
// ---------------------------------------------------------------------------

/// C: `enclist[]` entry for `"hex"`.
const HEX_ENCODING: PgEncoding = PgEncoding {
    encode_len: hex_enc_len,
    decode_len: |src| Ok(hex_dec_len(src)),
    encode: |src, dst| Ok(hex_encode(src, dst)),
    decode: hex_decode,
};

/// C: `enclist[]` entry for `"base64"`.
const BASE64_ENCODING: PgEncoding = PgEncoding {
    encode_len: pg_base64_enc_len,
    decode_len: |src| Ok(pg_base64_dec_len(src)),
    encode: pg_base64_encode,
    decode: pg_base64_decode,
};

/// C: `enclist[]` entry for `"escape"`.
///
/// The C `esc_dec_len` `ereport(ERROR)`s on a lone backslash, longjmping out of
/// `binary_decode` *before* `palloc`. [`esc_dec_len`] returns `PgResult` and is
/// plugged straight into the fallible `decode_len` slot, so `binary_decode`
/// propagates that error with `?` before allocating.
const ESCAPE_ENCODING: PgEncoding = PgEncoding {
    encode_len: esc_enc_len,
    decode_len: esc_dec_len,
    encode: esc_encode,
    decode: esc_decode,
};

/// C: `enclist[]` — the codec dispatch table (a static array of named
/// `pg_encoding` entries), scanned by [`pg_find_encoding`].
const ENCLIST: &[(&str, PgEncoding)] = &[
    ("hex", HEX_ENCODING),
    ("base64", BASE64_ENCODING),
    ("escape", ESCAPE_ENCODING),
];

/// C: `pg_find_encoding(name)` (static) — look up the [`PgEncoding`] table for a
/// codec name (`hex`, `base64`, `escape`), case-insensitively (C uses
/// `pg_strcasecmp`); `None` if unknown.
pub fn pg_find_encoding(name: &str) -> Option<PgEncoding> {
    for (encname, enc) in ENCLIST {
        if pg_strcasecmp_ascii(encname.as_bytes(), name.as_bytes()) {
            return Some(*enc);
        }
    }

    None
}

/// C: `pg_strcasecmp(enclist[i].name, name) == 0` — the dispatch names are fixed
/// ASCII (`hex`/`base64`/`escape`), so an ASCII case-insensitive equality is
/// identical to PostgreSQL's locale-aware `pg_strcasecmp` for these inputs. Stops
/// at NUL like C strings (so embedded NULs do not match through them).
#[inline]
#[allow(clippy::manual_ignore_case_cmp)]
fn pg_strcasecmp_ascii(a: &[u8], b: &[u8]) -> bool {
    let mut i = 0usize;
    loop {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        if ca.to_ascii_lowercase() != cb.to_ascii_lowercase() {
            return false;
        }
        if ca == 0 {
            return true;
        }
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// Error constructors
// ---------------------------------------------------------------------------

/// C: `errmsg("unrecognized encoding: \"%s\"", namebuf)` in `binary_encode` /
/// `binary_decode`.
fn unrecognized_encoding(namebuf: &str) -> PgError {
    PgError::error(format!("unrecognized encoding: \"{namebuf}\""))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C: `errmsg("result of encoding/decoding conversion is too large")`.
fn conversion_too_large(encoding: bool) -> PgError {
    let verb = if encoding { "encoding" } else { "decoding" };
    PgError::error(format!("result of {verb} conversion is too large"))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

/// C: `elog(FATAL, "overflow - encode/decode estimate too small")`.
fn estimate_too_small(encoding: bool) -> PgError {
    let verb = if encoding { "encode" } else { "decode" };
    PgError::new(FATAL, format!("overflow - {verb} estimate too small"))
}

/// C: `errmsg("invalid hexadecimal digit: \"%.*s\"", pg_mblen_range(s, srcend),
/// s)` — `rest` begins at the offending byte `s` and runs to the source end.
fn invalid_hex_digit(rest: &[u8]) -> PgError {
    PgError::error(format!(
        "invalid hexadecimal digit: \"{}\"",
        mb_snippet(rest)
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C: `errmsg("invalid hexadecimal data: odd number of digits")`.
fn invalid_hex_odd_digits() -> PgError {
    PgError::error("invalid hexadecimal data: odd number of digits")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C: `errmsg("unexpected \"=\" while decoding base64 sequence")`.
fn unexpected_equals() -> PgError {
    PgError::error("unexpected \"=\" while decoding base64 sequence")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C: `errmsg("invalid symbol \"%.*s\" found while decoding base64 sequence",
/// pg_mblen_range(s - 1, srcend), s - 1)`.
fn invalid_base64_symbol(rest: &[u8]) -> PgError {
    PgError::error(format!(
        "invalid symbol \"{}\" found while decoding base64 sequence",
        mb_snippet(rest)
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C: `errmsg("invalid base64 end sequence")` plus the hint.
fn invalid_base64_end() -> PgError {
    PgError::error("invalid base64 end sequence")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .with_hint("Input data is missing padding, is truncated, or is otherwise corrupted.")
}

/// C: `errmsg("invalid input syntax for type %s", "bytea")` in `esc_decode` /
/// `esc_dec_len`.
fn invalid_bytea_syntax() -> PgError {
    PgError::error("invalid input syntax for type bytea")
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// Render the `%.*s` snippet used in the hex/base64 error messages: the first
/// `pg_mblen_range(rest, srcend)` bytes of `rest`, lossily as UTF-8. C prints the
/// raw bytes; lossy UTF-8 is the faithful `String` representation. The
/// `.min(rest.len())` clamp is defensive only — `pg_mblen_range` already clamps
/// the length to the slice end. `pg_mblen_range` crosses the mbutils seam.
fn mb_snippet(rest: &[u8]) -> String {
    let n = (pg_mblen_range::call(rest) as usize).min(rest.len());
    String::from_utf8_lossy(&rest[..n]).into_owned()
}

#[cfg(test)]
mod tests;
