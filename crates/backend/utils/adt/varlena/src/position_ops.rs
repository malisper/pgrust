//! FAMILY: substring / position / overlay / left / right / reverse, and the
//! literal `replace_text`.
//!
//! `text_substring`, `bytea_substring`, the `text_position_*`
//! Boyer-Moore-Horspool / char-aware searcher + `textpos`, `text_overlay`,
//! `text_left`/`text_right`/`text_reverse`, `pg_mbcharcliplen_chars`, and the
//! literal `replace_text`.
//!
//! Depends on the keystone for [`TextPositionState`](crate::keystone),
//! `charlen_to_bytelen`, `check_collation_set`, `cstring_to_text_with_len`,
//! and `text_catenate`. Reaches the mbutils seams for char/byte clipping and
//! the locale providers (`pg_newlocale_from_collation`, `pg_strncoll`) for
//! nondeterministic-collation matching (genuinely external: pg_locale.c).

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_SUBSTRING_ERROR};

use pg_locale_seams as locale;
use mbutils_seams as mb;

use crate::keystone::{
    check_collation_set, cstring_to_text_with_len, text_catenate, TextPositionState,
};

/// `PG_UTF8` (`mb/pg_wchar.h` `pg_enc`): UTF-8 is the one multibyte encoding in
/// which one character's byte sequence can never appear inside a longer
/// character, so its match-boundary verification can be skipped.
const PG_UTF8: i32 = 6;

/// C: `pg_mblen_unbounded(p)` — bytes in the character at `p`. The C original
/// reads without a bound (the caller has verified the encoding is well-formed);
/// the carrier here is a bounded slice, so we use the range-clamped mbutils
/// seam, which never reads past the slice end and returns at least 1.
#[inline]
fn pg_mblen_unbounded(p: &[u8]) -> PgResult<i32> {
    // C: pg_mblen_unbounded reads without a bound on an already-verified
    // string. The range-clamped seam never reads past the slice end and
    // report_invalid_encoding's (carried on Err) only on a byte sequence
    // invalid in the database encoding.
    Ok(mb::pg_mblen_range::call(p)?.max(1))
}

// ===========================================================================
// text_substring / bytea_substring / pg_mbcharcliplen_chars
// ===========================================================================

/// C: `text_substring(Datum str, int32 start, int32 length, bool
/// length_not_specified)` — the SQL `substring` worker on character positions.
/// The owner seam `text_substr` routes here.
///
/// The C original avoids detoasting via `DatumGetTextPSlice`; the carrier here
/// is the already-detoasted payload, so slicing is a plain subslice. The
/// "grab a conservatively large slice" optimization collapses to using the
/// whole payload.
pub fn text_substring<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    start: i32,
    length: i32,
    length_not_specified: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let eml = mb::pg_database_encoding_max_length::call();
    let s = start; // start position
    #[allow(non_snake_case)]
    let S1 = s.max(1); // adjusted start position
    #[allow(non_snake_case)]
    let L1: i32; // adjusted substring length
    #[allow(non_snake_case)]
    let E: i32; // end position, exclusive

    // life is easy if the encoding max length is 1
    if eml == 1 {
        if length_not_specified {
            // special case - get length to end of string
            L1 = -1;
        } else if length < 0 {
            // SQL99 says to throw an error for E < S, i.e., negative length
            return Err(PgError::error("negative substring length not allowed")
                .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
        } else {
            match s.checked_add(length) {
                None => {
                    // S + L overflowed: the substring must run to end of string.
                    L1 = -1;
                }
                Some(e) => {
                    E = e;
                    // A zero or negative end position can happen if the start
                    // was negative or one. SQL99: return a zero-length string.
                    if E < 1 {
                        return cstring_to_text_with_len(mcx, b"", 0);
                    }
                    L1 = E - S1;
                }
            }
        }

        // DatumGetTextPSlice(str, S1 - 1, L1): a subslice of the payload,
        // zero-based start S1-1, length L1 (-1 == to end), past-the-end start
        // yields an empty string.
        return text_p_slice(mcx, str, S1 - 1, L1);
    } else if eml > 1 {
        // When eml > 1 we can't get the char length without scanning. C grabs
        // a conservatively large byte slice; the carrier is the whole payload.
        let slice_size: i32;

        if length_not_specified {
            // special case - get length to end of string
            E = -1;
            slice_size = -1;
            L1 = -1;
        } else if length < 0 {
            return Err(PgError::error("negative substring length not allowed")
                .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
        } else {
            match s.checked_add(length) {
                None => {
                    // S + L overflowed: run to end of string.
                    E = i32::MAX; // unused beyond the slice_size branch below
                    slice_size = -1;
                    L1 = -1;
                }
                Some(e) => {
                    E = e;
                    // Ending at position 1, exclusive, yields an empty string;
                    // a zero or negative value can happen if the start was
                    // negative or one. SQL99: return a zero-length string.
                    if E <= 1 {
                        return cstring_to_text_with_len(mcx, b"", 0);
                    }
                    L1 = E - S1;
                    // Total slice size in bytes can't exceed (E-1)*eml; on
                    // overflow use -1 (== whole string).
                    slice_size = match (E - 1).checked_mul(eml) {
                        Some(v) => v,
                        None => -1,
                    };
                }
            }
        }

        // The carrier is the untoasted payload: slice_start is always 0 and
        // the conservative slice is the whole payload (a tighter byte slice
        // would only be an optimization). slice == the payload.
        let _ = slice_size;
        let slice: &[u8] = str;

        // see if we got back an empty string
        let slice_len = slice.len() as i32;
        if slice_len == 0 {
            return cstring_to_text_with_len(mcx, b"", 0);
        }

        // Actual length of the slice in MB characters, stopping at the end of
        // the substring (when the end is known).
        let slice_strlen = if slice_size == -1 {
            mb::pg_mbstrlen_with_len::call(slice, slice_len)?
        } else {
            pg_mbcharcliplen_chars(slice, slice_len, E - 1)?
        };

        // If the start position is past the slice's char length, return empty.
        if S1 > slice_strlen {
            return cstring_to_text_with_len(mcx, b"", 0);
        }

        // Adjust L1 and E1 now that we know the slice string length. S1 is
        // one-based, slice_start is zero-based (== 0 here).
        let slice_start: i32 = 0;
        #[allow(non_snake_case)]
        let E1: i32 = if L1 > -1 {
            (S1 + L1).min(slice_start + 1 + slice_strlen)
        } else {
            slice_start + 1 + slice_strlen
        };

        // Find the start byte offset in the slice (S1 is not zero based).
        let mut p: usize = 0;
        let mut i = 0;
        while i < S1 - 1 {
            p += pg_mblen_unbounded(&slice[p..])? as usize;
            i += 1;
        }

        // hang onto our start position
        let start_off = p;

        // Count the actual bytes used by the substring of the requested length.
        let mut i = S1;
        while i < E1 {
            p += pg_mblen_unbounded(&slice[p..])? as usize;
            i += 1;
        }

        // ret = palloc(VARHDRSZ + (p - s)); memcpy(VARDATA, s, p - s)
        return cstring_to_text_with_len(
            mcx,
            &slice[start_off..p],
            (p - start_off) as i32,
        );
    }

    // eml < 1: invalid backend encoding.
    Err(PgError::error("invalid backend encoding: encoding max length < 1"))
}

/// `DatumGetTextPSlice(str, off, len)` over an already-detoasted payload: the
/// substring of `payload` starting at byte `off` (zero-based, clamped at the
/// payload end, so a past-the-end start yields empty) of byte length `len`
/// (`-1` == to the end), charged to `mcx`.
fn text_p_slice<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    off: i32,
    len: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let total = payload.len();
    let start = (off.max(0) as usize).min(total);
    let end = if len < 0 {
        total
    } else {
        start.saturating_add(len as usize).min(total)
    };
    cstring_to_text_with_len(mcx, &payload[start..end], (end - start) as i32)
}

/// C: `pg_mbcharcliplen_chars(const char *mbstr, int len, int limit)` — mirror
/// `pg_mbcharcliplen()`, except the return value unit is chars, not bytes.
/// Counts up to `limit` whole characters within the first `len` bytes, stopping
/// at the first NUL or the byte limit. Mirrors the historical dubious behavior
/// (it counts the limit'th char before consuming it).
///
/// C asserts `len > 0`, `limit > 0`, `pg_database_encoding_max_length() > 1`.
pub fn pg_mbcharcliplen_chars(mbstr: &[u8], len: i32, limit: i32) -> PgResult<i32> {
    debug_assert!(len > 0);
    debug_assert!(limit > 0);
    debug_assert!(mb::pg_database_encoding_max_length::call() > 1);

    let mut nch: i32 = 0;
    let mut off: usize = 0;
    let mut remaining = len;

    while remaining > 0 && off < mbstr.len() && mbstr[off] != 0 {
        // C: pg_mblen_with_len(mbstr, len) — bytes of the current char,
        // bounded by the remaining slice.
        let l = pg_mblen_unbounded(&mbstr[off..])?;
        nch += 1;
        if nch == limit {
            break;
        }
        remaining -= l;
        off += l as usize;
    }
    Ok(nch)
}

/// C: `bytea_substring(Datum str, int S, int L, bool length_not_specified)`.
/// The logic matches [`text_substring`]'s single-byte path (bytea has no
/// encoding).
pub fn bytea_substring<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    s: i32,
    l: i32,
    length_not_specified: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    #[allow(non_snake_case)]
    let S1 = s.max(1); // adjusted start position
    #[allow(non_snake_case)]
    let L1: i32; // adjusted substring length

    if length_not_specified {
        // DatumGetByteaPSlice grabs everything to the end on a negative length.
        L1 = -1;
    } else if l < 0 {
        // SQL99 says to throw an error for E < S, i.e., negative length.
        return Err(PgError::error("negative substring length not allowed")
            .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
    } else {
        match s.checked_add(l) {
            None => {
                // S + L overflowed: run to end of string.
                L1 = -1;
            }
            #[allow(non_snake_case)]
            Some(E) => {
                // Zero/negative end => zero-length string.
                if E < 1 {
                    return cstring_to_text_with_len(mcx, b"", 0);
                }
                L1 = E - S1;
            }
        }
    }

    // DatumGetByteaPSlice(str, S1 - 1, L1).
    text_p_slice(mcx, str, S1 - 1, L1)
}

// ===========================================================================
// text_overlay
// ===========================================================================

/// C: `text_overlay(text *t1, text *t2, int sp, int sl)` — replace the `sl`
/// characters of `t1` starting at 1-based position `sp` with `t2`. Direct
/// implementation of the SQL standard's substring+concatenation definition.
pub fn text_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // Check for possible integer-overflow cases. For negative sp, throw a
    // "substring length" error per the spec's OVERLAY() definition.
    if sp <= 0 {
        return Err(PgError::error("negative substring length not allowed")
            .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
    }
    let sp_pl_sl = match sp.checked_add(sl) {
        Some(v) => v,
        None => {
            return Err(PgError::error("integer out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
    };

    // s1 = text_substring(t1, 1, sp - 1, false);
    let s1 = text_substring(mcx, t1, 1, sp - 1, false)?;
    // s2 = text_substring(t1, sp_pl_sl, -1, true);
    let s2 = text_substring(mcx, t1, sp_pl_sl, -1, true)?;
    // result = text_catenate(text_catenate(s1, t2), s2);
    let result = text_catenate(mcx, &s1, t2)?;
    text_catenate(mcx, &result, &s2)
}

// ===========================================================================
// text_position state machine + textpos
// ===========================================================================

/// C: `textpos(PG_FUNCTION_ARGS)` — the SQL `POSITION()` function.
pub fn textpos<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    text_position(mcx, t1, t2, collid)
}

/// C: `text_position(text *t1, text *t2, Oid collid)` — 1-based char position
/// of needle `t2` in haystack `t1`, 0 if absent. Wraps the
/// `text_position_setup`/`_next`/`_get_match_pos` state machine.
///
/// The carriers `t1`/`t2` are the detoasted payloads (`VARSIZE_ANY_EXHDR`
/// == slice length). Needs an `Mcx` to resolve the collation via the locale
/// owner seam (the scaffold's no-Mcx signature is widened here, the only
/// caller-visible change; no other family references it yet).
pub fn text_position<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    check_collation_set(collid)?;

    // Empty needle always matches at position 1.
    if t2.is_empty() {
        return Ok(1);
    }

    // Otherwise, can't match if haystack is shorter than needle (deterministic
    // collations only).
    let locale = locale::pg_newlocale_from_collation::call(mcx, collid)?;
    if t1.len() < t2.len() && locale.deterministic {
        return Ok(0);
    }

    let mut state = text_position_setup(mcx, t1, t2, collid)?;
    // don't need greedy mode here
    state.greedy = false;

    let result = if !text_position_next(&mut state)? {
        0
    } else {
        text_position_get_match_pos(&mut state)?
    };
    text_position_cleanup(&mut state);
    Ok(result)
}

/// C: `text_position_setup(text *t1, text *t2, Oid collid, TextPositionState
/// *state)` — initialize the search state and the Boyer-Moore-Horspool skip
/// table.
pub(crate) fn text_position_setup<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    t1: &'a [u8],
    t2: &'a [u8],
    collid: Oid,
) -> PgResult<TextPositionState<'a, 'mcx>> {
    let len1 = t1.len() as i32;
    let len2 = t2.len() as i32;

    check_collation_set(collid)?;

    let locale = locale::pg_newlocale_from_collation::call(mcx, collid)?;

    debug_assert!(len2 > 0);

    // Even with a multibyte encoding the search runs over the raw byte
    // sequence. For UTF-8 (and single-byte encodings) one character's bytes
    // can't appear inside a longer character, so no boundary re-check is
    // needed; for other multibyte encodings we verify afterwards.
    let is_multibyte_char_in_char = if mb::pg_database_encoding_max_length::call() == 1 {
        false
    } else if mb::get_database_encoding::call() == PG_UTF8 {
        false
    } else {
        true
    };

    let mut state = TextPositionState {
        is_multibyte_char_in_char,
        // Most callers need greedy mode; some unset it to optimize.
        greedy: true,
        str1: t1,
        str2: t2,
        len1,
        len2,
        skiptablemask: 0,
        skiptable: [0i32; 256],
        last_match: None,
        last_match_len: 0,
        last_match_len_tmp: 0,
        refpoint: 0,
        refpos: 0,
        locale,
        collid,
    };

    // Prepare the BMH skip table. Skip it for empty/oversized needles, for
    // one-character needles (no possible saving), and for nondeterministic
    // collations (the search is already multibyte-aware).
    if len1 >= len2 && len2 > 1 && state.locale.deterministic {
        let searchlength = len1 - len2;
        let skiptablemask: i32 = if searchlength < 16 {
            3
        } else if searchlength < 64 {
            7
        } else if searchlength < 128 {
            15
        } else if searchlength < 512 {
            31
        } else if searchlength < 2048 {
            63
        } else if searchlength < 4096 {
            127
        } else {
            255
        };
        state.skiptablemask = skiptablemask;

        // All elements default to the needle length (skip distance for any
        // char not in the needle).
        let mut i = 0;
        while i <= skiptablemask {
            state.skiptable[i as usize] = len2;
            i += 1;
        }

        // For each needle char except the last, set the skip distance. When
        // two chars share a table entry, the one later in the needle wins.
        let last = len2 - 1;
        let mut i = 0;
        while i < last {
            let idx = (t2[i as usize] as i32) & skiptablemask;
            state.skiptable[idx as usize] = last - i;
            i += 1;
        }
    }

    Ok(state)
}

/// C: `text_position_next(TextPositionState *state)` — advance to the next
/// match, starting from the end of the previous match (or the start on first
/// call). Returns true if a match is found. Refuses an empty needle.
pub(crate) fn text_position_next(state: &mut TextPositionState) -> PgResult<bool> {
    let needle_len = state.len2;

    if needle_len <= 0 {
        return Ok(false); // result for empty pattern
    }

    // Start from right after the previous match (byte offset into str1).
    let mut start_off: usize = match state.last_match {
        Some(m) => m + state.last_match_len as usize,
        None => 0,
    };

    loop {
        let matchptr = text_position_next_internal(start_off, state)?;

        let matchptr = match matchptr {
            None => return Ok(false),
            Some(m) => m,
        };

        // For a multibyte encoding where a char's bytes can appear inside a
        // longer char, verify the match is at a char boundary.
        if state.is_multibyte_char_in_char && state.locale.deterministic {
            // the search should never move backwards.
            debug_assert!(state.refpoint <= matchptr);

            let mut false_positive = false;
            while state.refpoint < matchptr {
                // step to next character
                state.refpoint += pg_mblen_unbounded(&state.str1[state.refpoint..])? as usize;
                state.refpos += 1;

                // If we stepped over the match start, it was a false positive
                // (byte sequence in the middle of a multibyte char). Skip it
                // and continue the search at the next char boundary.
                if state.refpoint > matchptr {
                    start_off = state.refpoint;
                    false_positive = true;
                    break;
                }
            }
            if false_positive {
                continue; // C: goto retry
            }
        }

        state.last_match = Some(matchptr);
        state.last_match_len = state.last_match_len_tmp;
        return Ok(true);
    }
}

/// C: `text_position_next_internal(char *start_ptr, TextPositionState *state)`
/// — search for the raw byte sequence starting at byte offset `start_off`,
/// ignoring multibyte issues. Returns the byte offset of the first match, or
/// `None`. Sets `last_match_len_tmp`.
fn text_position_next_internal(
    start_off: usize,
    state: &mut TextPositionState,
) -> PgResult<Option<usize>> {
    // C dereferences `state->locale` directly; the layered locale seam re-keys
    // by collation OID, which the state carries.
    let collid = state.collid;
    let haystack_len = state.len1 as usize;
    let needle_len = state.len2 as usize;
    let skiptablemask = state.skiptablemask;
    let haystack = state.str1;
    let needle = state.str2;

    debug_assert!(start_off <= haystack_len);
    debug_assert!(needle_len > 0);

    state.last_match_len_tmp = state.len2;

    if !state.locale.deterministic {
        // Nondeterministic collation: walk the haystack and test substrings of
        // the remaining string for collation-equality with the needle. The
        // matched substring can differ in length from the needle, so callers
        // read last_match_len. Greedy callers want the longest match.
        let mut result_hptr: Option<usize> = None;

        let mut hptr = start_off;
        while hptr < haystack_len {
            // Common case: a match of exactly the needle length (non-greedy).
            if !state.greedy
                && (haystack_len - hptr) >= needle_len
                && locale::pg_strncoll::call(
                    collid,
                    &haystack[hptr..hptr + needle_len],
                    &needle[..needle_len],
                )? == 0
            {
                return Ok(Some(hptr));
            }

            // Else check every nonempty substring starting at hptr.
            let mut test_end = hptr;
            loop {
                test_end += pg_mblen_unbounded(&haystack[test_end..])? as usize;
                if locale::pg_strncoll::call(
                    collid,
                    &haystack[hptr..test_end],
                    &needle[..needle_len],
                )? == 0
                {
                    state.last_match_len_tmp = (test_end - hptr) as i32;
                    result_hptr = Some(hptr);
                    if !state.greedy {
                        break;
                    }
                }
                if test_end >= haystack_len {
                    break;
                }
            }

            if result_hptr.is_some() {
                break;
            }

            hptr += pg_mblen_unbounded(&haystack[hptr..])? as usize;
        }

        return Ok(result_hptr);
    } else if needle_len == 1 {
        // No point in B-M-H for a one-character needle.
        let nchar = needle[0];
        let mut hptr = start_off;
        while hptr < haystack_len {
            if haystack[hptr] == nchar {
                return Ok(Some(hptr));
            }
            hptr += 1;
        }
    } else {
        let needle_last = needle_len - 1;

        // Start at startpos plus the length of the needle (minus 1).
        let mut hptr = start_off + needle_len - 1;
        while hptr < haystack_len {
            // Match the needle scanning *backward*.
            let mut nptr = needle_last;
            let mut p = hptr;
            while needle[nptr] == haystack[p] {
                // Matched it all? Return the 0-based byte offset.
                if nptr == 0 {
                    return Ok(Some(p));
                }
                nptr -= 1;
                p -= 1;
            }

            // No match: use the haystack char at hptr to decide the advance.
            let idx = (haystack[hptr] as i32) & skiptablemask;
            hptr += state.skiptable[idx as usize] as usize;
        }
    }

    Ok(None) // not found
}

/// C: `text_position_get_match_pos(TextPositionState *state)` — the 1-based
/// character offset of the current match. Converts the cached byte position to
/// a char position, advancing `refpoint`/`refpos` so successive calls are O(1)
/// amortized.
fn text_position_get_match_pos(state: &mut TextPositionState) -> PgResult<i32> {
    let last_match = state.last_match.expect("match must be set");
    // Convert the byte position to char position. C: pg_mbstrlen_with_len,
    // which report_invalid_encoding's (ereport ERROR) on a byte sequence
    // invalid in the database encoding; carried on Err.
    state.refpos += mb::pg_mbstrlen_with_len::call(
        &state.str1[state.refpoint..last_match],
        (last_match - state.refpoint) as i32,
    )?;
    state.refpoint = last_match;
    Ok(state.refpos + 1)
}

/// C: `text_position_get_match_ptr(TextPositionState *state)` — the byte offset
/// within the haystack `str1` of the current match (C returns `state->last_match`,
/// a `char *`; the lifetime-safe equivalent is the byte offset). Panics if no
/// match has been recorded, mirroring the C contract that callers only call this
/// after a successful `text_position_next`.
pub(crate) fn text_position_get_match_ptr(state: &TextPositionState) -> usize {
    state.last_match.expect("match must be set")
}

/// C: `text_position_reset(TextPositionState *state)` — reset to the initial
/// state installed by `text_position_setup`; the next `text_position_next`
/// searches from the start of the string.
pub(crate) fn text_position_reset(state: &mut TextPositionState) {
    state.last_match = None;
    state.refpoint = 0;
    state.refpos = 0;
}

/// C: `text_position_cleanup(TextPositionState *state)` — no cleanup needed.
pub(crate) fn text_position_cleanup(_state: &mut TextPositionState) {
    // no cleanup needed
}

// ===========================================================================
// text_left / text_right / text_reverse
// ===========================================================================

/// C: `text_left(PG_FUNCTION_ARGS)` — first `n` characters. For negative `n`,
/// all but the last `|n|` characters.
pub fn text_left<'mcx>(mcx: Mcx<'mcx>, t: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    if n < 0 {
        let p = t;
        let len = t.len() as i32;
        let nn = mb::pg_mbstrlen_with_len::call(p, len)? + n;
        let rlen = mb::pg_mbcliplen::call(p, len, nn);
        cstring_to_text_with_len(mcx, &p[..rlen.max(0) as usize], rlen)
    } else {
        // text_substring(str, 1, n, false)
        text_substring(mcx, t, 1, n, false)
    }
}

/// C: `text_right(PG_FUNCTION_ARGS)` — last `n` characters. For negative `n`,
/// all but the first `|n|` characters.
pub fn text_right<'mcx>(mcx: Mcx<'mcx>, t: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    let p = t;
    let len = t.len() as i32;
    let nn = if n < 0 {
        -n
    } else {
        mb::pg_mbstrlen_with_len::call(p, len)? - n
    };
    let off = mb::pg_mbcliplen::call(p, len, nn);
    let off = off.max(0);
    cstring_to_text_with_len(mcx, &p[off as usize..], len - off)
}

/// C: `text_reverse(PG_FUNCTION_ARGS)` — the reversed string. Multibyte-aware:
/// whole characters are reversed, not bytes.
pub fn text_reverse<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let p = t;
    let len = t.len();

    // result = palloc(len + VARHDRSZ); dst = VARDATA(result) + len (fill back).
    let mut out = mcx::vec_with_capacity_in(mcx, len)?;
    // Build into a scratch buffer filled from the back, then commit forward.
    let mut buf = vec![0u8; len];
    let mut dst = len; // write position, fills downward

    if mb::pg_database_encoding_max_length::call() > 1 {
        // multibyte version
        let mut off = 0usize;
        while off < len {
            let sz = pg_mblen_unbounded(&p[off..])? as usize;
            dst -= sz;
            buf[dst..dst + sz].copy_from_slice(&p[off..off + sz]);
            off += sz;
        }
    } else {
        // single byte version
        let mut off = 0usize;
        while off < len {
            dst -= 1;
            buf[dst] = p[off];
            off += 1;
        }
    }

    out.extend_from_slice(&buf);
    Ok(out)
}

// ===========================================================================
// replace_text (literal)
// ===========================================================================

/// C: `replace_text(PG_FUNCTION_ARGS)` — replace all occurrences of `from` in
/// `src` with `to`. Returns `src` unchanged if `from` or `src` is empty, or if
/// `from` is not found.
pub fn replace_text<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    from: &[u8],
    to: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let src_text_len = src.len();
    let from_sub_text_len = from.len();

    // Return unmodified source if empty source or pattern.
    if src_text_len < 1 || from_sub_text_len < 1 {
        return cstring_to_text_with_len(mcx, src, src_text_len as i32);
    }

    let mut state = text_position_setup(mcx, src, from, collid)?;

    let mut found = text_position_next(&mut state)?;

    // When from is not found, there is nothing to do.
    if !found {
        text_position_cleanup(&mut state);
        return cstring_to_text_with_len(mcx, src, src_text_len as i32);
    }

    // curr_ptr / start_ptr as byte offsets into src.
    let mut curr_ptr = state.last_match.expect("match set");
    let mut start_ptr: usize = 0;

    // initStringInfo: build the result charged to mcx.
    let mut str = mcx::vec_with_capacity_in(mcx, src_text_len)?;

    loop {
        // CHECK_FOR_INTERRUPTS(): not modeled (cooperative cancellation owner).

        // copy the data skipped over by the last text_position_next()
        str.extend_from_slice(&src[start_ptr..curr_ptr]);

        // appendStringInfoText(&str, to)
        str.extend_from_slice(to);

        start_ptr = curr_ptr + state.last_match_len as usize;

        found = text_position_next(&mut state)?;
        if found {
            curr_ptr = state.last_match.expect("match set");
        }

        if !found {
            break;
        }
    }

    // copy trailing data: from start_ptr to the end of src (VARSIZE_ANY EXHDR).
    str.extend_from_slice(&src[start_ptr..]);

    text_position_cleanup(&mut state);

    Ok(str)
}
