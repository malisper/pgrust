use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use ::regex::{RegMatch, RegexecResult, REG_NOSUB};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};

// C: charlen_to_bytelen; caller guarantees p holds at least n complete chars.
pub fn charlen_to_bytelen(p: &[u8], n: i32) -> PgResult<i32> {
    if mbutils::pg_database_encoding_max_length() == 1 {
        Ok(n)
    } else {
        let mut off = 0usize;
        let mut remaining = n;
        while remaining > 0 && off < p.len() {
            off += mbutils::pg_mblen_range(&p[off..])?.max(1) as usize;
            remaining -= 1;
        }
        Ok(off as i32)
    }
}

// 0: no backslash escapes; 1: escapes but no \1..\9 submatch; 2: submatch.
fn check_replace_text_has_escape(replace_text: &[u8]) -> i32 {
    let mut result = 0;
    let mut i = 0usize;
    let len = replace_text.len();
    while i < len {
        match replace_text[i..].iter().position(|&b| b == b'\\') {
            None => break,
            Some(off) => i += off,
        }
        i += 1;
        if i < len {
            let c = replace_text[i];
            if (b'1'..=b'9').contains(&c) {
                return 2;
            }
            result = 1;
            i += 1;
        }
    }
    result
}

fn append_regexp_substr(
    buf: &mut PgVec<'_, u8>,
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
        match replace_text[p..].iter().position(|&b| b == b'\\') {
            Some(off) => p += off,
            None => p = p_end,
        }
        if p > chunk_start {
            vec_append_bytes(buf, &replace_text[chunk_start..p])?;
        }
        if p >= p_end {
            break;
        }
        p += 1;
        if p >= p_end {
            buf.push(b'\\');
            break;
        }

        let so;
        let eo;
        let c = replace_text[p];
        if (b'1'..=b'9').contains(&c) {
            let idx = (c - b'0') as usize;
            so = pmatch[idx].rm_so;
            eo = pmatch[idx].rm_eo;
            p += 1;
        } else if c == b'&' {
            so = pmatch[0].rm_so;
            eo = pmatch[0].rm_eo;
            p += 1;
        } else if c == b'\\' {
            buf.push(b'\\');
            p += 1;
            continue;
        } else {
            buf.push(b'\\');
            continue;
        }

        if so >= 0 && eo >= 0 {
            debug_assert!(so >= data_pos as i64);
            let mut chunk_off = start_off;
            chunk_off +=
                charlen_to_bytelen(&src_text[chunk_off..], (so - data_pos as i64) as i32)? as usize;
            let chunk_len = charlen_to_bytelen(&src_text[chunk_off..], (eo - so) as i32)? as usize;
            vec_append_bytes(buf, &src_text[chunk_off..chunk_off + chunk_len])?;
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn regexp_failed(message: &str) -> PgError {
    PgError::error(format!("regular expression failed: {message}"))
        .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
}

// C: replace_text_regexp — n == 0 replaces all matches, n > 0 only the N'th.
// search_start is a character offset. Payload in, payload out.
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
    // regex_engine dispatch: RE2-compatible patterns take the byte-offset
    // RE2 path; everything else (and regex_engine=spencer) falls through to
    // the untouched C-parity Spencer path below. A whole-match-only pattern
    // may still ride RE2 when the replacement never consumes \1..\9.
    if let Some(re) = regexp_alt::dispatch(pattern_text, cflags, src_text)? {
        if re.capture_safe() || regexp_alt::check_replace_text_has_escape(replace_text) < 2 {
            return regexp_alt::replace_text_regexp_re2(
                mcx,
                &re,
                src_text,
                replace_text,
                search_start,
                n,
            );
        }
    }

    let mut nmatches: i32 = 0;
    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, src_text.len())?;

    let mut pmatch = [RegMatch::UNSET; 10];
    let mut nmatch: usize = pmatch.len();

    let data = mbutils::pg_mb2wchar_with_len(mcx, src_text)?;
    let data_len = data.len() as i64;

    let escape_status = check_replace_text_has_escape(replace_text);
    if escape_status < 2 {
        cflags |= REG_NOSUB;
        nmatch = 1;
    }

    let re = regexp_seams::RE_compile_and_cache::call(mcx, pattern_text, cflags, collation)?;

    let mut start_off: usize = 0;
    let mut data_pos: i32 = 0;

    while (search_start as i64) <= data_len {
        postgres_seams::check_for_interrupts::call()?;

        match regex_core_seams::pg_regexec::call(&re, &data, search_start, &mut pmatch[..nmatch])? {
            RegexecResult::NoMatch => break,
            RegexecResult::Matched => {}
            RegexecResult::Failed(f) => return Err(regexp_failed(&f.message).into()),
        }

        nmatches += 1;
        if n > 0 && nmatches != n {
            search_start = pmatch[0].rm_eo as i32;
            if pmatch[0].rm_so == pmatch[0].rm_eo {
                search_start += 1;
            }
            continue;
        }

        if pmatch[0].rm_so - data_pos as i64 > 0 {
            let chunk_len =
                charlen_to_bytelen(&src_text[start_off..], (pmatch[0].rm_so - data_pos as i64) as i32)?;
            vec_append_bytes(&mut buf, &src_text[start_off..start_off + chunk_len as usize])?;
            start_off += chunk_len as usize;
            data_pos = pmatch[0].rm_so as i32;
        }

        if escape_status > 0 {
            append_regexp_substr(&mut buf, replace_text, &pmatch, src_text, start_off, data_pos)?;
        } else {
            vec_append_bytes(&mut buf, replace_text)?;
        }

        start_off +=
            charlen_to_bytelen(&src_text[start_off..], pmatch[0].rm_eo as i32 - data_pos)? as usize;
        data_pos = pmatch[0].rm_eo as i32;

        if n > 0 {
            break;
        }

        search_start = data_pos;
        if pmatch[0].rm_so == pmatch[0].rm_eo {
            search_start += 1;
        }
    }

    if (data_pos as i64) < data_len {
        vec_append_bytes(&mut buf, &src_text[start_off..])?;
    }

    Ok(buf)
}
