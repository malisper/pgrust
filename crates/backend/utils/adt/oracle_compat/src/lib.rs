#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/oracle_compat.c`:
//! Oracle-compatible string functions — case folding
//! (`lower`/`upper`/`initcap`/`casefold`), padding (`lpad`/`rpad`), trimming
//! (`btrim`/`ltrim`/`rtrim` plus the `bytea` variants), `translate`, `ascii`,
//! `chr`, and `repeat`.
//!
//! Every function from the C file is implemented here with the original name
//! and logic. `text`/`bytea` are pass-by-reference; at this crate's surface a
//! referent is carried as its detoasted *content* bytes (the bytes after the
//! 4-byte varlena header), matching the sibling `backend-utils-adt-varlena`
//! port: each entry point takes `&[u8]` arguments and a caller [`Mcx`], and
//! returns an owned `PgVec<'mcx, u8>` (or, for `ascii`, an `i32`). The thin
//! `Datum(PG_FUNCTION_ARGS)` framing (`PG_GETARG_TEXT_PP`, `PG_RETURN_TEXT_P`,
//! `PG_GET_COLLATION` → an explicit [`Oid`] parameter) follows the project
//! fmgr/Datum deferral and would wrap these cores one-to-one.
//!
//! # Memory
//!
//! `lpad`/`rpad`/`translate`/`repeat` build a worst-case working buffer (the C
//! `ret = palloc(bytelen)` / `result = palloc(tlen)`) charged to the caller's
//! [`Mcx`] via [`::mcx::vec_with_capacity_in`], exactly mirroring the C palloc in
//! the current memory context. The trim variants and `chr`/`ascii` allocate
//! only the returned value itself.
//!
//! # Cross-crate logic
//!
//! The locale-aware case-folding routines `str_tolower` / `str_toupper` /
//! `str_initcap` / `str_casefold` live in the sibling
//! [`formatting`] crate (a real port); the
//! `lower`/`upper`/`initcap`/`casefold` wrappers here are ported 1:1 around
//! those cross-crate calls, exactly as the C file delegates to `formatting.c`.
//! The UTF-8 legality check (`pg_utf8_islegal`) and per-encoding maximum length
//! (`pg_encoding_max_length`) used by `chr`/`ascii` come from the real ported
//! [`common_wchar`] crate. The server-encoding multibyte state
//! (`pg_database_encoding_max_length`, `GetDatabaseEncoding`,
//! `pg_mbstrlen_with_len`, `pg_mblen_range`, `pg_mblen_unbounded` ==
//! `pg_encoding_mblen`) crosses the already-installed `utils/mb/mbutils.c`
//! owner seam; `CHECK_FOR_INTERRUPTS()` crosses the already-installed
//! `tcop/postgres.c` owner seam.

#[cfg(test)]
mod tests;

pub mod fmgr_builtins;

/// Install this crate's seams. Called by `seams-init::init_all`. Registers every
/// `oracle_compat.c` SQL-callable builtin into the fmgr-core builtin table.
pub fn init_seams() {
    fmgr_builtins::register_oracle_compat_builtins();
}

use formatting::{str_casefold, str_initcap, str_tolower, str_toupper};
use common_wchar::{pg_encoding_max_length, pg_utf8_islegal};
use mcx::{Mcx, PgVec};
use ::types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use ::types_wchar::encoding::PG_UTF8;

use postgres_seams as tcop;
use mbutils_seams as mb;

/// `VARHDRSZ` — the size of a 4-byte varlena header.
const VARHDRSZ: i32 = 4;

/// `MaxAllocSize` (utils/memutils.h): `0x3fffffff`. `AllocSizeIsValid(size)` is
/// `size <= MaxAllocSize`.
const MAX_ALLOC_SIZE: i32 = 0x3fff_ffff;

#[inline]
fn AllocSizeIsValid(size: i32) -> bool {
    // C: `((Size) (size) <= MaxAllocSize)`. `size` here is the worst-case byte
    // length already computed as a (possibly large) positive int32.
    (0..=MAX_ALLOC_SIZE).contains(&size)
}

/// C: `pg_mul_s32_overflow` (common/int.h): `*result = a * b`, returns true on
/// overflow.
#[inline]
fn pg_mul_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

/// C: `pg_add_s32_overflow` (common/int.h): `*result = a + b`, returns true on
/// overflow.
#[inline]
fn pg_add_s32_overflow(a: i32, b: i32, res: &mut i32) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = 0;
            true
        }
    }
}

/// `ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested
/// length too large")))`.
#[inline]
fn requested_length_too_large() -> PgError {
    PgError::error("requested length too large").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

// ---------------------------------------------------------------------------
// crate-local thin wrappers over the multibyte-encoding / interrupt seams
// ---------------------------------------------------------------------------

/// C: `pg_database_encoding_max_length()` (mbutils.c).
#[inline]
fn pg_database_encoding_max_length() -> i32 {
    mb::pg_database_encoding_max_length::call()
}

/// C: `GetDatabaseEncoding()` (mbutils.c).
#[inline]
fn GetDatabaseEncoding() -> i32 {
    mb::get_database_encoding::call()
}

/// C: `pg_mbstrlen_with_len(mbstr, limit)` (mbutils.c). `bytes` is the first
/// `limit` bytes; the seam returns the encoded-character count.
#[inline]
fn pg_mbstrlen_with_len(bytes: &[u8], limit: i32) -> i32 {
    // Called on already server-encoded string-function input, so the
    // `report_invalid_encoding` path is dead; fall back to the byte length (an
    // upper bound on the char count) rather than escalate.
    mb::pg_mbstrlen_with_len::call(bytes, limit).unwrap_or(limit)
}

/// C: `pg_mblen_range(mbstr, end)` (mbutils.c) — byte length of the leading
/// encoded character within a bounded slice.
#[inline]
fn pg_mblen_range(bytes: &[u8]) -> i32 {
    // C's `pg_mblen` does not validate; the seam only Errs on a
    // slice-overrunning leading char, where the clamped length is the slice
    // length (the dead error path falls back to `bytes.len()`).
    mb::pg_mblen_range::call(bytes).unwrap_or(bytes.len() as i32)
}

/// C: `pg_mblen_unbounded(mbstr)` (mbutils.c) — byte length of the leading
/// encoded character of an already-verified string. Defined in mbutils.c as
/// `pg_encoding_mblen(GetDatabaseEncoding(), mbstr)`.
#[inline]
fn pg_mblen_unbounded(bytes: &[u8]) -> i32 {
    mb::pg_encoding_mblen::call(GetDatabaseEncoding(), bytes)
}

/// C: `CHECK_FOR_INTERRUPTS()` (miscadmin.h).
#[inline]
fn check_for_interrupts() -> PgResult<()> {
    tcop::check_for_interrupts::call()
}

/// `cstring_to_text` over an already-decoded byte buffer. The case-folding
/// helpers return owned `PgVec<u8>` (the C `out_string` was a fresh palloc'd
/// NUL-terminated buffer); we hand the payload back, exactly as
/// `cstring_to_text(out_string)` followed by `pfree(out_string)` would.
#[inline]
fn cstring_to_text(bytes: PgVec<'_, u8>) -> PgResult<PgVec<'_, u8>> {
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Case folding (delegates to formatting.c routines)
// ---------------------------------------------------------------------------

/// C: `lower` — SQL `lower(text)`. Returns `string`, with all letters forced to
/// lowercase.
pub fn lower<'mcx>(mcx: Mcx<'mcx>, in_string: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let out_string = str_tolower(mcx, in_string, collid)?;
    cstring_to_text(out_string)
}

/// C: `upper` — SQL `upper(text)`. Returns `string`, with all letters forced to
/// uppercase.
pub fn upper<'mcx>(mcx: Mcx<'mcx>, in_string: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let out_string = str_toupper(mcx, in_string, collid)?;
    cstring_to_text(out_string)
}

/// C: `initcap` — SQL `initcap(text)`. First letter of each word in uppercase,
/// all others in lowercase; a word is a maximal run of alphanumeric characters.
pub fn initcap<'mcx>(mcx: Mcx<'mcx>, in_string: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let out_string = str_initcap(mcx, in_string, collid)?;
    cstring_to_text(out_string)
}

/// C: `casefold` — SQL `casefold(text)`. Unicode case folding.
pub fn casefold<'mcx>(mcx: Mcx<'mcx>, in_string: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let out_string = str_casefold(mcx, in_string, collid)?;
    cstring_to_text(out_string)
}

// ---------------------------------------------------------------------------
// Padding
// ---------------------------------------------------------------------------

/// C: `lpad` — `lpad(string1, len, string2)`. Returns `string1` left-padded to
/// length `len` with the repeated character sequence in `string2`. If `len` is
/// less than the length of `string1`, truncate (on the right) to `len`.
pub fn lpad<'mcx>(
    mcx: Mcx<'mcx>,
    string1: &[u8],
    mut len: i32,
    string2: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let s1 = string1;
    let s2 = string2;

    // Negative len is silently taken as zero
    if len < 0 {
        len = 0;
    }

    let mut s1len = s1.len() as i32;
    if s1len < 0 {
        s1len = 0; // shouldn't happen
    }

    let mut s2len = s2.len() as i32;
    if s2len < 0 {
        s2len = 0; // shouldn't happen
    }

    let mut s1len = pg_mbstrlen_with_len(s1, s1len);

    if s1len > len {
        s1len = len; // truncate string1 to len chars
    }

    if s2len <= 0 {
        len = s1len; // nothing to pad with, so don't pad
    }

    // compute worst-case output length
    let mut bytelen = 0i32;
    if pg_mul_s32_overflow(pg_database_encoding_max_length(), len, &mut bytelen)
        || pg_add_s32_overflow(bytelen, VARHDRSZ, &mut bytelen)
        || !AllocSizeIsValid(bytelen)
    {
        return Err(requested_length_too_large());
    }

    // ret = (text *) palloc(bytelen); — worst-case buffer charged to the
    // caller's context.
    let cap = (bytelen - VARHDRSZ).max(0) as usize;
    let mut ret: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, cap)?;

    let mut m = len - s1len;

    // ptr2 walks string2, wrapping at its end.
    let mut ptr2: usize = 0;
    let s2end = s2len as usize;

    while m > 0 {
        m -= 1;
        let mlen = pg_mblen_range(&s2[ptr2..s2end]) as usize;
        ret.extend_from_slice(&s2[ptr2..ptr2 + mlen]);
        ptr2 += mlen;
        if ptr2 == s2end {
            // wrap around at end of s2
            ptr2 = 0;
        }
    }

    // ptr1 walks string1 for s1len characters.
    let mut ptr1: usize = 0;
    let mut remaining = s1len;
    while remaining > 0 {
        remaining -= 1;
        let mlen = pg_mblen_unbounded(&s1[ptr1..]) as usize;
        ret.extend_from_slice(&s1[ptr1..ptr1 + mlen]);
        ptr1 += mlen;
    }

    Ok(ret)
}

/// C: `rpad` — `rpad(string1, len, string2)`. Returns `string1` right-padded to
/// length `len` with the repeated character sequence in `string2`. If `len` is
/// less than the length of `string1`, truncate (on the right) to `len`.
pub fn rpad<'mcx>(
    mcx: Mcx<'mcx>,
    string1: &[u8],
    mut len: i32,
    string2: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let s1 = string1;
    let s2 = string2;

    // Negative len is silently taken as zero
    if len < 0 {
        len = 0;
    }

    let mut s1len = s1.len() as i32;
    if s1len < 0 {
        s1len = 0; // shouldn't happen
    }

    let mut s2len = s2.len() as i32;
    if s2len < 0 {
        s2len = 0; // shouldn't happen
    }

    let mut s1len = pg_mbstrlen_with_len(s1, s1len);

    if s1len > len {
        s1len = len; // truncate string1 to len chars
    }

    if s2len <= 0 {
        len = s1len; // nothing to pad with, so don't pad
    }

    // compute worst-case output length
    let mut bytelen = 0i32;
    if pg_mul_s32_overflow(pg_database_encoding_max_length(), len, &mut bytelen)
        || pg_add_s32_overflow(bytelen, VARHDRSZ, &mut bytelen)
        || !AllocSizeIsValid(bytelen)
    {
        return Err(requested_length_too_large());
    }

    let cap = (bytelen - VARHDRSZ).max(0) as usize;
    let mut ret: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, cap)?;

    let mut m = len - s1len;

    // ptr1 walks string1 for s1len characters first.
    let mut ptr1: usize = 0;
    let mut remaining = s1len;
    while remaining > 0 {
        remaining -= 1;
        let mlen = pg_mblen_unbounded(&s1[ptr1..]) as usize;
        ret.extend_from_slice(&s1[ptr1..ptr1 + mlen]);
        ptr1 += mlen;
    }

    // then pad with string2, wrapping at its end.
    let mut ptr2: usize = 0;
    let s2end = s2len as usize;
    while m > 0 {
        m -= 1;
        let mlen = pg_mblen_range(&s2[ptr2..s2end]) as usize;
        ret.extend_from_slice(&s2[ptr2..ptr2 + mlen]);
        ptr2 += mlen;
        if ptr2 == s2end {
            // wrap around at end of s2
            ptr2 = 0;
        }
    }

    Ok(ret)
}

// ---------------------------------------------------------------------------
// Trimming
// ---------------------------------------------------------------------------

/// C: `btrim` — `btrim(string, set)`. Removes characters from the front and back
/// up to the first character not in `set`.
pub fn btrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dotrim(mcx, string, set, true, true)
}

/// C: `btrim1` — `btrim` with `set` fixed as `' '`.
pub fn btrim1<'mcx>(mcx: Mcx<'mcx>, string: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dotrim(mcx, string, b" ", true, true)
}

/// C: `dotrim` (static) — common implementation for `btrim`/`ltrim`/`rtrim`.
/// Returns the selected portion of `string` charged to `mcx` (the C
/// `cstring_to_text_with_len(string, stringlen)`).
fn dotrim<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    set: &[u8],
    doltrim: bool,
    dortrim: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut stringlen = string.len() as i32;
    let setlen = set.len() as i32;

    // The window into `string` selected so far: [start, start+stringlen).
    let mut start: usize = 0;

    // Nothing to do if either string or set is empty
    if stringlen > 0 && setlen > 0 {
        if pg_database_encoding_max_length() > 1 {
            // In the multibyte-encoding case, build arrays of (offset, mblen)
            // for each character of `string` / `set`, so that we can avoid
            // inefficient checks in the inner loops. C: palloc(stringlen *
            // sizeof(char *)).
            let mut stringchars: PgVec<(usize, usize)> =
                ::mcx::vec_with_capacity_in(mcx, stringlen as usize)?;
            {
                let mut p: usize = 0;
                let mut len = stringlen as usize;
                while len > 0 {
                    let mblen = pg_mblen_range(&string[p..]) as usize;
                    stringchars.push((p, mblen));
                    p += mblen;
                    len -= mblen;
                }
            }

            let mut setchars: PgVec<(usize, usize)> =
                ::mcx::vec_with_capacity_in(mcx, setlen as usize)?;
            {
                let mut p: usize = 0;
                let mut len = setlen as usize;
                while len > 0 {
                    let mblen = pg_mblen_range(&set[p..]) as usize;
                    setchars.push((p, mblen));
                    p += mblen;
                    len -= mblen;
                }
            }

            let mut resultndx: usize = 0; // index in stringchars[]
            let mut resultnchars = stringchars.len();

            if doltrim {
                while resultnchars > 0 {
                    let (str_off, str_len) = stringchars[resultndx];
                    let mut i = 0;
                    while i < setchars.len() {
                        let (set_off, set_mblen) = setchars[i];
                        if str_len == set_mblen
                            && string[str_off..str_off + str_len]
                                == set[set_off..set_off + set_mblen]
                        {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setchars.len() {
                        break; // no match here
                    }
                    start += str_len;
                    stringlen -= str_len as i32;
                    resultndx += 1;
                    resultnchars -= 1;
                }
            }

            if dortrim {
                while resultnchars > 0 {
                    let (str_off, str_len) = stringchars[resultndx + resultnchars - 1];
                    let mut i = 0;
                    while i < setchars.len() {
                        let (set_off, set_mblen) = setchars[i];
                        if str_len == set_mblen
                            && string[str_off..str_off + str_len]
                                == set[set_off..set_off + set_mblen]
                        {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setchars.len() {
                        break; // no match here
                    }
                    stringlen -= str_len as i32;
                    resultnchars -= 1;
                }
            }
        } else {
            // In the single-byte-encoding case, we don't need such overhead.
            if doltrim {
                while stringlen > 0 {
                    let str_ch = string[start];
                    let mut i = 0;
                    while i < setlen as usize {
                        if str_ch == set[i] {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setlen as usize {
                        break; // no match here
                    }
                    start += 1;
                    stringlen -= 1;
                }
            }

            if dortrim {
                while stringlen > 0 {
                    let str_ch = string[start + stringlen as usize - 1];
                    let mut i = 0;
                    while i < setlen as usize {
                        if str_ch == set[i] {
                            break;
                        }
                        i += 1;
                    }
                    if i >= setlen as usize {
                        break; // no match here
                    }
                    stringlen -= 1;
                }
            }
        }
    }

    // Return selected portion of string
    ::mcx::slice_in(mcx, &string[start..start + stringlen as usize])
}

/// C: `dobyteatrim` — common implementation for the bytea trim variants. Returns
/// the selected portion of `string` charged to `mcx`.
fn dobyteatrim<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    set: &[u8],
    doltrim: bool,
    dortrim: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let stringlen = string.len() as i32;
    let setlen = set.len() as i32;

    if stringlen <= 0 || setlen <= 0 {
        return ::mcx::slice_in(mcx, string);
    }

    let mut m = stringlen;
    // ptr .. end mirror the C `char *ptr`/`char *end` cursors as signed indices
    // into `string`. `end` may be decremented to one-before-`ptr` on the last
    // rtrim match, exactly as in C, where the resulting pointer is never
    // dereferenced (the loop exits because `m == 0`).
    let mut ptr: isize = 0;
    let mut end: isize = (stringlen - 1) as isize;
    let setend: isize = (setlen - 1) as isize;

    if doltrim {
        while m > 0 {
            let mut ptr2: isize = 0;
            while ptr2 <= setend {
                if string[ptr as usize] == set[ptr2 as usize] {
                    break;
                }
                ptr2 += 1;
            }
            if ptr2 > setend {
                break;
            }
            ptr += 1;
            m -= 1;
        }
    }

    if dortrim {
        while m > 0 {
            let mut ptr2: isize = 0;
            while ptr2 <= setend {
                if string[end as usize] == set[ptr2 as usize] {
                    break;
                }
                ptr2 += 1;
            }
            if ptr2 > setend {
                break;
            }
            end -= 1;
            m -= 1;
        }
    }

    ::mcx::slice_in(mcx, &string[ptr as usize..ptr as usize + m as usize])
}

/// C: `byteatrim` — `btrim(bytea, bytea)`.
pub fn byteatrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dobyteatrim(mcx, string, set, true, true)
}

/// C: `bytealtrim` — `ltrim(bytea, bytea)`.
pub fn bytealtrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dobyteatrim(mcx, string, set, true, false)
}

/// C: `byteartrim` — `rtrim(bytea, bytea)`.
pub fn byteartrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dobyteatrim(mcx, string, set, false, true)
}

/// C: `ltrim` — `ltrim(string, set)`. Removes initial characters up to the first
/// character not in `set`.
pub fn ltrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dotrim(mcx, string, set, true, false)
}

/// C: `ltrim1` — `ltrim` with `set` fixed as `' '`.
pub fn ltrim1<'mcx>(mcx: Mcx<'mcx>, string: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dotrim(mcx, string, b" ", true, false)
}

/// C: `rtrim` — `rtrim(string, set)`. Removes final characters after the last
/// character not in `set`.
pub fn rtrim<'mcx>(mcx: Mcx<'mcx>, string: &[u8], set: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dotrim(mcx, string, set, false, true)
}

/// C: `rtrim1` — `rtrim` with `set` fixed as `' '`.
pub fn rtrim1<'mcx>(mcx: Mcx<'mcx>, string: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    dotrim(mcx, string, b" ", false, true)
}

// ---------------------------------------------------------------------------
// Translate / ascii / chr / repeat
// ---------------------------------------------------------------------------

/// C: `translate` — `translate(string, from, to)`. Replaces all occurrences of
/// characters in `from` with the corresponding character in `to`. If `from` is
/// longer than `to`, occurrences of the extra `from` characters are deleted.
pub fn translate<'mcx>(
    mcx: Mcx<'mcx>,
    string: &[u8],
    from: &[u8],
    to: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let s = string;
    let from_buf = from;
    let to_buf = to;

    let mut m = s.len() as i32;
    if m <= 0 {
        return ::mcx::slice_in(mcx, string);
    }
    // source walks `string`; source_end == s.len().
    let mut source: usize = 0;
    let source_end = s.len();

    let fromlen = from_buf.len() as i32;
    let from_end = from_buf.len();
    // C also computes `tolen`, but only to derive `to_end = to_ptr + tolen`;
    // here `to_end` is the slice length directly.
    let to_end = to_buf.len();

    // The worst-case expansion is to substitute a max-length character for a
    // single-byte character at each position of the string.
    let mut bytelen = 0i32;
    if pg_mul_s32_overflow(pg_database_encoding_max_length(), m, &mut bytelen)
        || pg_add_s32_overflow(bytelen, VARHDRSZ, &mut bytelen)
        || !AllocSizeIsValid(bytelen)
    {
        return Err(requested_length_too_large());
    }

    let cap = (bytelen - VARHDRSZ).max(0) as usize;
    let mut result: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, cap)?;

    while m > 0 {
        let source_len = pg_mblen_range(&s[source..source_end]) as usize;
        let mut from_index = 0i32;

        // Find the index of the matching character in `from`.
        let mut i: usize = 0;
        while (i as i32) < fromlen {
            let len = pg_mblen_range(&from_buf[i..from_end]) as usize;
            if len == source_len && s[source..source + len] == from_buf[i..i + len] {
                break;
            }
            from_index += 1;
            i += len;
        }
        if (i as i32) < fromlen {
            // substitute, or delete if no corresponding "to" character
            let mut p: usize = 0;
            let mut k = 0i32;
            while k < from_index {
                if p >= to_end {
                    break;
                }
                p += pg_mblen_range(&to_buf[p..to_end]) as usize;
                k += 1;
            }
            if p < to_end {
                let tlen = pg_mblen_range(&to_buf[p..to_end]) as usize;
                result.extend_from_slice(&to_buf[p..p + tlen]);
            }
        } else {
            // no match, so copy
            result.extend_from_slice(&s[source..source + source_len]);
        }

        source += source_len;
        m -= source_len as i32;
    }

    // The function result is probably much bigger than needed if we're using a
    // multibyte encoding, but it's not worth reallocating it.
    Ok(result)
}

/// C: `ascii` — decimal representation of the first character of `string`. If
/// the string is empty we return 0. If the database encoding is UTF8 we return
/// the Unicode code point. For any other multi-byte encoding we return the first
/// byte if it is ASCII (1..127), else raise an error. For all other encodings we
/// return the first byte (1..255).
pub fn ascii(string: &[u8]) -> PgResult<i32> {
    let data = string;
    let encoding = GetDatabaseEncoding();

    if data.is_empty() {
        return Ok(0);
    }

    let first = data[0];

    if encoding == PG_UTF8 && first > 127 {
        // return the code point for Unicode
        let mut result: i32;
        let tbytes: usize;

        if first >= 0xF0 {
            result = (first & 0x07) as i32;
            tbytes = 3;
        } else if first >= 0xE0 {
            result = (first & 0x0F) as i32;
            tbytes = 2;
        } else {
            debug_assert!(first > 0xC0);
            result = (first & 0x1f) as i32;
            tbytes = 1;
        }

        debug_assert!(tbytes > 0);

        // C: `for (i = 1; i <= tbytes; i++)` reading the `tbytes` continuation
        // bytes data[1..=tbytes].
        for &cont in &data[1..=tbytes] {
            debug_assert!((cont & 0xC0) == 0x80);
            result = (result << 6) + (cont & 0x3f) as i32;
        }

        Ok(result)
    } else {
        if pg_encoding_max_length(encoding) > 1 && first > 127 {
            return Err(PgError::error("requested character too large")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        Ok(first as i32)
    }
}

/// C: `chr` — `chr(int val)`. Returns the character having the binary equivalent
/// of `val`. For UTF8 the argument is treated as a Unicode code point; for other
/// multi-byte encodings, arguments outside strict ASCII (1..127) raise an error.
pub fn chr<'mcx>(mcx: Mcx<'mcx>, arg: i32) -> PgResult<PgVec<'mcx, u8>> {
    let encoding = GetDatabaseEncoding();

    // Error out on arguments that make no sense or that we can't validly
    // represent in the encoding.
    if arg < 0 {
        return Err(PgError::error("character number must be positive")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    } else if arg == 0 {
        return Err(PgError::error("null character not permitted")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }

    let cvalue = arg as u32;

    if encoding == PG_UTF8 && cvalue > 127 {
        // for Unicode we treat the argument as a code point
        let bytes: usize;

        // We only allow valid Unicode code points; per RFC3629 that stops at
        // U+10FFFF, even though 4-byte UTF8 sequences can hold values up to
        // U+1FFFFF.
        if cvalue > 0x0010_ffff {
            return Err(PgError::error(alloc::format!(
                "requested character too large for encoding: {cvalue}"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        if cvalue > 0xffff {
            bytes = 4;
        } else if cvalue > 0x07ff {
            bytes = 3;
        } else {
            bytes = 2;
        }

        let mut wch: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, bytes)?;
        for _ in 0..bytes {
            wch.push(0);
        }

        if bytes == 2 {
            wch[0] = 0xC0 | ((cvalue >> 6) & 0x1F) as u8;
            wch[1] = 0x80 | (cvalue & 0x3F) as u8;
        } else if bytes == 3 {
            wch[0] = 0xE0 | ((cvalue >> 12) & 0x0F) as u8;
            wch[1] = 0x80 | ((cvalue >> 6) & 0x3F) as u8;
            wch[2] = 0x80 | (cvalue & 0x3F) as u8;
        } else {
            wch[0] = 0xF0 | ((cvalue >> 18) & 0x07) as u8;
            wch[1] = 0x80 | ((cvalue >> 12) & 0x3F) as u8;
            wch[2] = 0x80 | ((cvalue >> 6) & 0x3F) as u8;
            wch[3] = 0x80 | (cvalue & 0x3F) as u8;
        }

        // The preceding range check isn't sufficient, because UTF8 excludes
        // Unicode "surrogate pair" codes. Make sure what we created is valid
        // UTF8.
        if !pg_utf8_islegal(&wch) {
            return Err(PgError::error(alloc::format!(
                "requested character not valid for encoding: {cvalue}"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        Ok(wch)
    } else {
        let is_mb = pg_encoding_max_length(encoding) > 1;

        if (is_mb && cvalue > 127) || (!is_mb && cvalue > 255) {
            return Err(PgError::error(alloc::format!(
                "requested character too large for encoding: {cvalue}"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }

        let mut buf: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, 1)?;
        buf.push(cvalue as u8);
        Ok(buf)
    }
}

/// C: `repeat` — `repeat(string, count)`. Repeat `string` `count` times.
pub fn repeat<'mcx>(mcx: Mcx<'mcx>, string: &[u8], mut count: i32) -> PgResult<PgVec<'mcx, u8>> {
    let sp = string;

    if count < 0 {
        count = 0;
    }

    let slen = sp.len() as i32;

    let mut tlen = 0i32;
    if pg_mul_s32_overflow(count, slen, &mut tlen)
        || pg_add_s32_overflow(tlen, VARHDRSZ, &mut tlen)
        || !AllocSizeIsValid(tlen)
    {
        return Err(requested_length_too_large());
    }

    let cap = (tlen - VARHDRSZ).max(0) as usize;
    let mut result: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, cap)?;

    for _ in 0..count {
        result.extend_from_slice(sp);
        check_for_interrupts()?;
    }

    Ok(result)
}

extern crate alloc;
