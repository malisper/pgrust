//! FAMILY: regexp-driven replacement (the `replace_text_regexp` owner seam
//! body) plus its `\N`/`\&` substitution helpers.
//!
//! `replace_text_regexp`, `check_replace_text_has_escape`,
//! `appendStringInfoRegexpSubstr`.
//!
//! The regex engine itself (compile/execute) is the genuinely-external
//! `backend-regex-core` owner, reached by `backend-utils-adt-regexp-seams`
//! (`regexp.c` is the immediate caller; this family is the body of the
//! `replace_text_regexp` owner seam declared in
//! `backend-utils-adt-varlena-seams`).

use crate::keystone::{charlen_to_bytelen, cstring_to_text_with_len};
use regex_core_seams as regex_core;
use regexp_seams as regexp;
use mbutils_seams as mb;
use mcx::{Mcx, PgVec};
use ::types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};
use regex::{RegMatch, RegexecResult, REG_NOSUB};

/// C: `replace_text_regexp(src_text, pattern_text, replace_text, cflags,
/// collation, search_start, n)` — owner seam body. `n = 0` replaces all,
/// `n > 0` only the n'th; `search_start` is a char offset.
///
/// `src_text`/`pattern_text`/`replace_text` are the already-detoasted `text`
/// payloads. The result payload is charged to `mcx`.
#[allow(clippy::too_many_arguments)]
pub fn replace_text_regexp<'mcx>(
    mcx: Mcx<'mcx>,
    src_text: &[u8],
    pattern_text: &[u8],
    replace_text: &[u8],
    mut cflags: i32,
    collation: Oid,
    mut search_start: i32,
    n: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let src_text_len = src_text.len() as i32;
    let mut nmatches: i32 = 0;

    // C: StringInfoData buf; initStringInfo(&buf). The carrier is the
    // header-less payload, so the buffer is a plain byte vector charged to mcx.
    let mut buf: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, 0)?;

    // C: regmatch_t pmatch[10]; main match plus \1 .. \9.
    let mut pmatch = [RegMatch::UNSET; 10];
    let mut nmatch: usize = pmatch.len();

    // C: convert data string to wide characters (palloc'd by the seam).
    let data = mb::pg_mb2wchar_with_len::call(mcx, src_text)?;
    let data_len = data.len() as i64;

    // C: check whether replace_text has escapes, especially regexp submatches.
    let escape_status = check_replace_text_has_escape(replace_text);

    // C: if no regexp submatches, we can use REG_NOSUB and only ask for the
    // whole-match location.
    if escape_status < 2 {
        cflags |= REG_NOSUB;
        nmatch = 1;
    }

    // C: prepare the regexp (compile, with the backend's RE cache).
    let re = regexp::RE_compile_and_cache::call(mcx, pattern_text, cflags, collation)?;

    // C: start_ptr points to the data_pos'th character of src_text. We track
    // it as a byte offset into src_text instead of a raw pointer.
    let mut start_off: usize = 0;
    let mut data_pos: i32 = 0;

    while (search_start as i64) <= data_len {
        // C: CHECK_FOR_INTERRUPTS().
        postgres_seams::check_for_interrupts::call()?;

        // C: pg_regexec(re, data, data_len, search_start, NULL, nmatch,
        //               pmatch, 0).
        let regexec_result = regex_core::pg_regexec::call(
            &re,
            &data,
            search_start,
            &mut pmatch[..nmatch],
        )?;

        match regexec_result {
            RegexecResult::NoMatch => break,
            RegexecResult::Matched => {}
            RegexecResult::Failed(failure) => {
                // C: pg_regerror(...) then ereport(ERROR, ...).
                return Err(PgError::error(format!(
                    "regular expression failed: {}",
                    failure.message
                ))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION));
            }
        }

        // C: count matches, and decide whether to replace this match.
        nmatches += 1;
        if n > 0 && nmatches != n {
            // C: advance search_start, but not start_ptr/data_pos.
            search_start = pmatch[0].rm_eo as i32;
            if pmatch[0].rm_so == pmatch[0].rm_eo {
                search_start += 1;
            }
            continue;
        }

        // C: copy the text to the left of the match position (char indexes).
        if pmatch[0].rm_so - data_pos as i64 > 0 {
            let chunk_len = charlen_to_bytelen(
                &src_text[start_off..],
                (pmatch[0].rm_so - data_pos as i64) as i32,
            )?;
            buf.extend_from_slice(&src_text[start_off..start_off + chunk_len as usize]);
            // C: advance start_ptr over that text to avoid rescans.
            start_off += chunk_len as usize;
            data_pos = pmatch[0].rm_so as i32;
        }

        // C: copy the replace_text, processing escapes if any are present.
        if escape_status > 0 {
            append_stringinfo_regexp_substr(
                &mut buf,
                replace_text,
                &pmatch,
                src_text,
                start_off,
                data_pos,
            )?;
        } else {
            // C: appendStringInfoText(&buf, replace_text).
            buf.extend_from_slice(replace_text);
        }

        // C: advance start_ptr and data_pos over the matched text.
        start_off += charlen_to_bytelen(
            &src_text[start_off..],
            pmatch[0].rm_eo as i32 - data_pos,
        )? as usize;
        data_pos = pmatch[0].rm_eo as i32;

        // C: if we only want to replace one occurrence, we're done.
        if n > 0 {
            break;
        }

        // C: advance search position. Normally start the next search at the
        // end of the previous match; but a zero-length match must advance by
        // one character to avoid finding the same match again.
        search_start = data_pos;
        if pmatch[0].rm_so == pmatch[0].rm_eo {
            search_start += 1;
        }
    }

    // C: copy the text to the right of the last match. The C computes the
    // remaining byte length as ((char *) src_text + VARSIZE_ANY(src_text)) -
    // start_ptr, i.e. everything from start_off to the payload end.
    if (data_pos as i64) < data_len {
        buf.extend_from_slice(&src_text[start_off..]);
    }
    let _ = src_text_len;

    // C: ret_text = cstring_to_text_with_len(buf.data, buf.len). The carrier is
    // the payload, so this copies the buffer into the result payload.
    cstring_to_text_with_len(mcx, &buf, buf.len() as i32)
}

/// C: `check_replace_text_has_escape(const text *replace_text)`.
///
/// Returns 0 if `replace_text` contains no backslashes that need processing,
/// 1 if it contains backslashes but no regexp submatch specifiers, and 2 if it
/// contains a regexp submatch specifier (`\1` .. `\9`).
pub fn check_replace_text_has_escape(replace_text: &[u8]) -> i32 {
    let mut result = 0;
    let mut i = 0usize;
    let len = replace_text.len();

    while i < len {
        // C: find next escape char, if any.
        match replace_text[i..].iter().position(|&b| b == b'\\') {
            None => break,
            Some(off) => i += off,
        }
        i += 1;
        // C: a backslash at the end doesn't require extra processing.
        if i < len {
            let c = replace_text[i];
            if (b'1'..=b'9').contains(&c) {
                return 2; // found a submatch specifier, so done
            }
            result = 1; // found some other sequence, keep looking
            i += 1;
        }
    }
    result
}

/// C: `appendStringInfoRegexpSubstr(StringInfo str, text *replace_text,
/// regmatch_t *pmatch, char *start_ptr, int data_pos)`.
///
/// Append `replace_text` to `str`, substituting regexp back references for
/// `\n` escapes. `start_off` is the byte offset, within `src_text`, of the
/// start of the match (logical character position `data_pos`).
fn append_stringinfo_regexp_substr(
    str: &mut PgVec<'_, u8>,
    replace_text: &[u8],
    pmatch: &[RegMatch],
    src_text: &[u8],
    start_off: usize,
    data_pos: i32,
) -> PgResult<()> {
    let p_end = replace_text.len();
    let mut p = 0usize;

    while p < p_end {
        let chunk_start = p;

        // C: find next escape char, if any.
        match replace_text[p..].iter().position(|&b| b == b'\\') {
            Some(off) => p += off,
            None => p = p_end,
        }

        // C: copy the text we just scanned over, if any.
        if p > chunk_start {
            str.extend_from_slice(&replace_text[chunk_start..p]);
        }

        // C: done if at end of string, else advance over escape char.
        if p >= p_end {
            break;
        }
        p += 1;

        if p >= p_end {
            // C: escape at very end of input. Treat same as unexpected char.
            str.push(b'\\');
            break;
        }

        let so;
        let eo;
        let c = replace_text[p];
        if (b'1'..=b'9').contains(&c) {
            // C: use the back reference of regexp.
            let idx = (c - b'0') as usize;
            so = pmatch[idx].rm_so;
            eo = pmatch[idx].rm_eo;
            p += 1;
        } else if c == b'&' {
            // C: use the entire matched string.
            so = pmatch[0].rm_so;
            eo = pmatch[0].rm_eo;
            p += 1;
        } else if c == b'\\' {
            // C: \\ means transfer one \ to output.
            str.push(b'\\');
            p += 1;
            continue;
        } else {
            // C: escape not followed by an expected char — treat the
            // backslash as ordinary data to copy.
            str.push(b'\\');
            continue;
        }

        if so >= 0 && eo >= 0 {
            // C: copy the back-referenced text. so and eo are counted in
            // characters, not bytes (Assert: so >= data_pos).
            let mut chunk_start_off = start_off;
            chunk_start_off += charlen_to_bytelen(
                &src_text[chunk_start_off..],
                (so - data_pos as i64) as i32,
            )? as usize;
            let chunk_len =
                charlen_to_bytelen(&src_text[chunk_start_off..], (eo - so) as i32)? as usize;
            str.extend_from_slice(&src_text[chunk_start_off..chunk_start_off + chunk_len]);
        }
    }
    Ok(())
}
