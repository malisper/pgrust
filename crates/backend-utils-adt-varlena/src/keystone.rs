//! KEYSTONE family — the shared carrier model, ABI/lifetime conventions, and
//! the foundational pure free functions that every other `varlena.c` family
//! compiles against.
//!
//! ## Carrier conventions (the project-wide layered shape)
//!
//! A `text`/`bytea`/`unknown` value crosses this crate's surface as its
//! **payload bytes** — the bytes *after* the 4-byte varlena header
//! (`VARHDRSZ`). On the way in that is a `&[u8]` (already detoasted by the
//! caller via the `backend-access-common-detoast-seams` owner; the carrier
//! IS the detoasted payload). On the way out a freshly built value is a
//! `PgVec<'mcx, u8>` charged to the caller's [`Mcx`] (C: `palloc` in the
//! current memory context, `SET_VARSIZE` + `memcpy` of the payload). The
//! 4-byte header is never materialized here — the layered `types-datum`
//! [`Varlena`]/[`Bytea`] types own header framing at the fmgr/Datum boundary.
//!
//! A `name` is the fixed 64-byte zero-padded buffer `[u8; NAMEDATALEN]`; its
//! logical value is the bytes up to the first NUL.
//!
//! `Oid` is `types_core::Oid`. Errors that the C `ereport(ERROR)`s carry are
//! returned on `PgResult::Err`; OOM from the charged buffers is likewise an
//! `Err` (never a panic) per the seam-signatures-mirror-c-failure-surface and
//! allocation-safety rules.
//!
//! ## What lives here (REAL, ported in the scaffold phase)
//!
//! - `cstring_to_text` / `cstring_to_text_with_len` / `text_to_cstring` /
//!   `text_to_cstring_buffer` — the payload<->cstring carrier converters
//!   (varlena.c "CONVERSION ROUTINES EXPORTED FOR USE BY C CODE").
//! - `text_length` / `text_catenate` / `charlen_to_bytelen` — the broken-out
//!   guts other families call directly.
//! - The shared state structs [`TextPositionState`], [`VarStringSortSupport`],
//!   [`SplitTextOutputData`] as type definitions other families name.
//! - [`NAMEDATALEN`] / [`VARHDRSZ`] / [`TEXTBUFLEN`] constants.

use mcx::{Mcx, PgVec};
use types_error::PgResult;

use backend_utils_mb_mbutils_seams as mb;

/// C: `c.h` `NAMEDATALEN` — the fixed width of a `name`.
pub const NAMEDATALEN: usize = types_core::fmgr::NAMEDATALEN as usize;

/// C: `postgres.h` `VARHDRSZ` — the 4-byte varlena length header.
pub const VARHDRSZ: usize = 4;

/// C: `varlena.c` `TEXTBUFLEN` — stack scratch sized so most strings fit.
pub const TEXTBUFLEN: usize = 1024;

// ===========================================================================
// Shared state structs (varlena.c top-of-file typedefs).
//
// These are the type-level foundation the comparison / sortsupport /
// position families compile against. Field-for-field mirrors of the C
// structs; the raw-pointer scratch fields of the C originals become
// owned/borrowed byte slices charged to the working Mcx in the owning
// family's port (see DESIGN HINT in each struct).
// ===========================================================================

/// C: `TextPositionState` (varlena.c) — state for the `text_position_*`
/// Boyer-Moore-Horspool / character-aware substring searcher.
///
/// In C `str1`/`str2` are `char *` into the (already-detoasted) haystack and
/// needle; here the position family holds them as borrowed payload slices for
/// the lifetime of the search. `locale` is the resolved collation
/// ([`types_locale::PgLocale`]); the skip table is the 256-entry BMH table.
#[derive(Debug)]
pub struct TextPositionState<'a, 'mcx> {
    /// `locale` — the resolved collation (`pg_locale_t`); collation used for
    /// substring matching. C stores a pointer into pg_locale.c's permanent
    /// cache; the layered carrier is the flag core
    /// ([`types_locale::PgLocale`]) copied into the working `Mcx`. C field
    /// order places this first.
    pub locale: types_locale::PgLocale<'mcx>,
    /// `is_multibyte_char_in_char` — need to check char boundaries?
    pub is_multibyte_char_in_char: bool,
    /// `greedy` — find the longest possible (nondeterministic) match?
    pub greedy: bool,
    /// `str1` — haystack payload bytes.
    pub str1: &'a [u8],
    /// `str2` — needle payload bytes.
    pub str2: &'a [u8],
    /// `len1` — haystack length in bytes.
    pub len1: i32,
    /// `len2` — needle length in bytes.
    pub len2: i32,
    /// `skiptablemask` — mask ANDed with skip-table subscripts.
    pub skiptablemask: i32,
    /// `skiptable[256]` — BMH skip distance for a mismatched char.
    pub skiptable: [i32; 256],
    /// `last_match` — byte offset of the last match within `str1` (C stores a
    /// pointer; the offset is the lifetime-safe equivalent), or `None`.
    pub last_match: Option<usize>,
    /// `last_match_len` — length of the last match.
    pub last_match_len: i32,
    /// `last_match_len_tmp` — same, internal scratch.
    pub last_match_len_tmp: i32,
    /// `refpoint` — byte offset within `str1` last converted to a char pos.
    pub refpoint: usize,
    /// `refpos` — 0-based char offset of `refpoint`.
    pub refpos: i32,
    /// Layering carrier (no C field): the collation OID the state was set up
    /// with. C dereferences `state->locale` (the `pg_locale_t` pointer)
    /// directly inside `text_position_next`; the layered locale seams re-key by
    /// collation OID, so the state carries the OID for those re-resolutions.
    pub collid: types_core::Oid,
}

/// C: `VarStringSortSupport` (varlena.c) — abbreviated-key sort state for
/// `text`/`bpchar`/`bytea`/`name`. The buffers, HyperLogLog cardinality
/// states, and resolved locale of the C original become the owning
/// sortsupport family's scratch (the abbreviated-key substrate); kept here as
/// the named carrier so other families can reference the type.
#[derive(Debug)]
pub struct VarStringSortSupport<'mcx> {
    /// `buf1` — 1st string / abbreviation original buf.
    pub buf1: PgVec<'mcx, u8>,
    /// `buf2` — 2nd string / strxfrm() buf.
    pub buf2: PgVec<'mcx, u8>,
    /// `last_len1` — length of last `buf1` strxfrm() input.
    pub last_len1: i32,
    /// `last_len2` — length of last `buf2` strxfrm() blob.
    pub last_len2: i32,
    /// `last_returned` — cached last comparison result.
    pub last_returned: i32,
    /// `cache_blob` — does `buf2` hold a strxfrm() blob?
    pub cache_blob: bool,
    /// `collate_c` — is the collation the C collation?
    pub collate_c: bool,
    /// `typid` — actual datatype OID (text/bpchar/bytea/name).
    pub typid: types_core::Oid,
    /// `prop_card` — required cardinality proportion.
    pub prop_card: f64,
    /// `locale` — the collation OID this support state resolved (the C
    /// `pg_locale_t` is reached by OID through the `pg_locale` seams; `None`
    /// when `collate_c`, mirroring the C `sss->locale = NULL`).
    pub locale: Option<types_core::Oid>,
    /// `locale->deterministic` — cached deterministic flag of the resolved
    /// `pg_locale_t` (the C comparator reads `sss->locale->deterministic` to
    /// decide whether to apply the `strcmp` tiebreak). `false` when
    /// `collate_c` / no locale.
    pub locale_deterministic: bool,
    /// `abbr_card` (`hyperLogLogState`) — abbreviated-key cardinality counter,
    /// held by value (C `hyperLogLogState abbr_card`); `None` until abbreviation
    /// is planned. The ops live in `backend-lib-hyperloglog`.
    pub abbr_card: Option<types_nodes::nodeagg::HyperLogLog<'mcx>>,
    /// `full_card` (`hyperLogLogState`) — authoritative-key cardinality counter,
    /// held by value; `None` until abbreviation is planned.
    pub full_card: Option<types_nodes::nodeagg::HyperLogLog<'mcx>>,
}

/// C: `SplitTextOutputData` (varlena.c) — `split_text()` output sink, either
/// an array build state or a tuplestore+tupdesc. Modeled by the split/format
/// family against the array-build / tuplestore owners; named here so its
/// signatures can reference it.
#[derive(Debug, Default)]
pub struct SplitTextOutputData {
    // Populated by the split/format family: an ArrayBuildState handle (array
    // output) XOR a Tuplestorestate + TupleDesc (table output). The owners
    // (arrayfuncs / tuplestore) are reached by seam; this is the carrier.
    _private: (),
}

// ===========================================================================
// CONVERSION ROUTINES EXPORTED FOR USE BY C CODE (varlena.c lines ~181-276).
// REAL — the carrier converters every family builds on.
// ===========================================================================

/// C: `cstring_to_text(const char *s)` — build a `text` payload from a
/// NUL-terminated C string. Here the input is already the bytes (no embedded
/// NUL handling: a Rust `&str`/`&[u8]` is the logical string). Returns the
/// payload charged to `mcx` (C: a fresh full-header palloc; the header is the
/// layered Datum boundary's job).
pub fn cstring_to_text<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    cstring_to_text_with_len(mcx, s, s.len() as i32)
}

/// C: `cstring_to_text_with_len(const char *s, int len)` — same as
/// [`cstring_to_text`] but with an explicit (possibly non-NUL-terminated)
/// length. The payload is the first `len` bytes of `s`, charged to `mcx`.
pub fn cstring_to_text_with_len<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    len: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let n = len.max(0) as usize;
    // C: palloc(len + VARHDRSZ); SET_VARSIZE; memcpy(VARDATA, s, len).
    // The carrier is the header-less payload, so we copy exactly `len` bytes.
    mcx::slice_in(mcx, &s[..n])
}

/// C: `text_to_cstring(const text *t)` — a NUL-terminated copy of a `text`
/// payload. The carrier is already the detoasted payload (C: detoasts here),
/// so this is a `palloc(len + 1)` + `memcpy` + trailing NUL, charged to `mcx`.
/// The returned vector includes the trailing NUL byte (C's cstring contract).
pub fn text_to_cstring<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = mcx::vec_with_capacity_in(mcx, t.len() + 1)?;
    out.extend_from_slice(t);
    out.push(0);
    Ok(out)
}

/// C: `text_to_cstring_buffer(const text *src, char *dst, size_t dst_len)` —
/// copy a `text` payload into a caller buffer of size `dst_len`, truncating
/// encoding-safely and always NUL-terminating (unless `dst_len == 0`). Returns
/// the number of payload bytes written (excluding the NUL); the caller owns
/// the buffer.
///
/// Mirrors the C `pg_mbcliplen` encoding-safe truncation via the mbutils seam.
pub fn text_to_cstring_buffer(src: &[u8], dst: &mut [u8]) -> usize {
    let dst_len = dst.len();
    if dst_len == 0 {
        return 0;
    }
    // C: dst_len--; (reserve room for the NUL).
    let avail = dst_len - 1;
    let src_len = src.len();
    let copy_len = if avail >= src_len {
        src_len
    } else {
        // C: encoding-safe truncation.
        mb::pg_mbcliplen::call(src, src_len as i32, avail as i32).max(0) as usize
    };
    dst[..copy_len].copy_from_slice(&src[..copy_len]);
    dst[copy_len] = 0;
    copy_len
}

// ===========================================================================
// Broken-out guts other families call directly (REAL).
// ===========================================================================

/// C: `text_length(Datum str)` — character length of a `text` payload (the
/// guts of `textlen`). Fast path: single-byte encodings return the byte
/// length; otherwise count characters via the mbutils seam.
///
/// In C the fast path uses `toast_raw_datum_size(str) - VARHDRSZ` to avoid
/// detoasting; the carrier here is already the detoasted payload, so the byte
/// length is exactly the payload length.
pub fn text_length(payload: &[u8]) -> i32 {
    if mb::pg_database_encoding_max_length::call() == 1 {
        payload.len() as i32
    } else {
        mb::pg_mbstrlen_with_len::call(payload, payload.len() as i32)
    }
}

/// C: `text_catenate(text *t1, text *t2)` — concatenate two `text` payloads.
/// The result payload is charged to `mcx` (C: a single `palloc` of
/// `len1 + len2 + VARHDRSZ`).
pub fn text_catenate<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // C clamps negative VARSIZE_ANY_EXHDR to 0; a Rust slice length is never
    // negative, so the clamp is a no-op here.
    let mut out = mcx::vec_with_capacity_in(mcx, t1.len() + t2.len())?;
    out.extend_from_slice(t1);
    out.extend_from_slice(t2);
    Ok(out)
}

/// C: `charlen_to_bytelen(const char *p, int n)` — bytes occupied by the first
/// `n` characters of `p`. Single-byte encodings short-circuit to `n`;
/// otherwise walk `n` characters via the unbounded `pg_mblen` (mapped to the
/// range-clamped mbutils seam, which never reads past the slice end).
///
/// The caller guarantees `p` holds at least `n` complete characters.
pub fn charlen_to_bytelen(p: &[u8], n: i32) -> i32 {
    if mb::pg_database_encoding_max_length::call() == 1 {
        n
    } else {
        let mut off = 0usize;
        let mut remaining = n;
        while remaining > 0 && off < p.len() {
            off += mb::pg_mblen_range::call(&p[off..]).max(1) as usize;
            remaining -= 1;
        }
        off as i32
    }
}

/// C: `check_collation_set(Oid collid)` — raise `ERRCODE_INDETERMINATE_COLLATION`
/// if `collid` is `InvalidOid`. Shared by the comparison/position/pattern
/// families. (REAL: pure guard, no external owner.)
pub fn check_collation_set(collid: types_core::Oid) -> PgResult<()> {
    if !types_core::OidIsValid(collid) {
        return Err(types_error::PgError::error(
            "could not determine which collation to use for string comparison",
        )
        .with_sqlstate(types_error::ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly."));
    }
    Ok(())
}
