#![allow(non_snake_case)]

use core::cell::RefCell;
use core::mem::ManuallyDrop;

use ::mcx::{slice_in, vec_append_bytes, vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexecResult, RegprefixResult, REG_ADVANCED,
    REG_EXPANDED, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NLANCH, REG_NLSTOP, REG_NOSUB,
    REG_QUOTE,
};
use ::types_core::{Oid, PgWChar};
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_ESCAPE_SEQUENCE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_REGULAR_EXPRESSION, ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER,
};
use regex_core::regex_export_free_error as engine;

pub mod builtins;
pub mod matches;

pub const MAX_CACHED_RES: usize = 32;

struct CachedRe {
    cre_pat: PgVec<'static, u8>,
    cre_flags: i32,
    cre_collation: Oid,
    re: RegexCompiled,
}

struct ReCache {
    mcx: Mcx<'static>,
    entries: PgVec<'static, CachedRe>,
}

thread_local! {
    static RE_CACHE: RefCell<Option<ManuallyDrop<ReCache>>> = const { RefCell::new(None) };
}

// INVARIANT: `f` must not re-enter the cache; the borrow spans its extent
// (loud RefCell panic otherwise).
fn with_cache<R>(f: impl FnOnce(&mut ReCache) -> R) -> R {
    RE_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let cache = slot.get_or_insert_with(|| {
            let mcx =
                ::mcx::session_root("RegexpCacheMemoryContext").mcx();
            ManuallyDrop::new(ReCache { mcx, entries: PgVec::new_in(mcx) })
        });
        f(cache)
    })
}

#[cold]
#[inline(never)]
fn invalid_regexp(message: &str) -> PgError {
    PgError::error(format!("invalid regular expression: {message}"))
        .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
}

#[cold]
#[inline(never)]
fn regexp_failed(message: &str) -> PgError {
    PgError::error(format!("regular expression failed: {message}"))
        .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
}

pub fn RE_compile_and_cache(
    mcx: Mcx<'_>,
    pattern: &[u8],
    cflags: i32,
    collation: Oid,
) -> PgResult<RegexCompiled> {
    let hit = with_cache(|cache| {
        let i = cache.entries.iter().position(|e| {
            e.cre_pat.len() == pattern.len()
                && e.cre_flags == cflags
                && e.cre_collation == collation
                && e.cre_pat.as_slice() == pattern
        })?;
        if i > 0 {
            let entry = cache.entries.remove(i);
            cache.entries.insert(0, entry);
        }
        Some(cache.entries[0].re.clone())
    });
    if let Some(re) = hit {
        return Ok(re);
    }

    let wide_pattern = mbutils::pg_mb2wchar_with_len(mcx, pattern)?;
    let compiled = match engine::seam_pg_regcomp(&wide_pattern, cflags, collation)? {
        RegcompResult::Compiled(c) => c,
        RegcompResult::Failed(f) => return Err(invalid_regexp(&f.message).into()),
    };
    drop(wide_pattern);

    let inserted: PgResult<()> = with_cache(|cache| {
        let pat_copy = slice_in(cache.mcx, pattern)?;
        cache
            .entries
            .try_reserve(1)
            .map_err(|_| cache.mcx.oom(core::mem::size_of::<CachedRe>()))?;
        if cache.entries.len() >= MAX_CACHED_RES {
            // C: MemoryContextDelete(re_array[num_res].cre_context); here the
            // engine state frees when the last RegexCompiled clone drops.
            cache.entries.pop();
        }
        cache.entries.insert(
            0,
            CachedRe { cre_pat: pat_copy, cre_flags: cflags, cre_collation: collation, re: compiled.clone() },
        );
        Ok(())
    });
    if let Err(e) = inserted {
        engine::pg_regfree(compiled);
        return Err(e);
    }

    Ok(compiled)
}

pub(crate) fn RE_wchar_execute(
    re: &RegexCompiled,
    data: &[PgWChar],
    start_search: i32,
    pmatch: &mut [RegMatch],
) -> PgResult<bool> {
    match engine::seam_pg_regexec(re, data, start_search, pmatch)? {
        RegexecResult::Matched => Ok(true),
        RegexecResult::NoMatch => Ok(false),
        RegexecResult::Failed(f) => Err(regexp_failed(&f.message).into()),
    }
}

fn RE_execute(
    mcx: Mcx<'_>,
    re: &RegexCompiled,
    dat: &[u8],
    pmatch: &mut [RegMatch],
) -> PgResult<bool> {
    let data = mbutils::pg_mb2wchar_with_len(mcx, dat)?;
    RE_wchar_execute(re, &data, 0, pmatch)
}

pub fn RE_compile_and_execute(
    mcx: Mcx<'_>,
    pattern: &[u8],
    dat: &[u8],
    mut cflags: i32,
    collation: Oid,
    pmatch: &mut [RegMatch],
) -> PgResult<bool> {
    if pmatch.len() < 2 {
        cflags |= REG_NOSUB;
        // regex_engine dispatch: boolean matches (~, ~*, regexp_like) on
        // RE2-compatible patterns skip the wchar conversion entirely.
        if let Some(re) = regexp_alt::dispatch(pattern, cflags, dat)? {
            return Ok(re.is_match(dat, 0));
        }
    }
    let re = RE_compile_and_cache(mcx, pattern, cflags, collation)?;
    RE_execute(mcx, &re, dat, pmatch)
}

pub fn nameregexeq(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, n, REG_ADVANCED, collation, &mut [])
}

pub fn nameregexne(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, n, REG_ADVANCED, collation, &mut [])?)
}

pub fn textregexeq(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, s, REG_ADVANCED, collation, &mut [])
}

pub fn textregexne(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, s, REG_ADVANCED, collation, &mut [])?)
}

pub fn nameicregexeq(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, n, REG_ADVANCED | REG_ICASE, collation, &mut [])
}

pub fn nameicregexne(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, n, REG_ADVANCED | REG_ICASE, collation, &mut [])?)
}

pub fn texticregexeq(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, s, REG_ADVANCED | REG_ICASE, collation, &mut [])
}

pub fn texticregexne(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, s, REG_ADVANCED | REG_ICASE, collation, &mut [])?)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PgReFlags {
    pub cflags: i32,
    pub glob: bool,
}

pub fn parse_re_flags(opts: Option<&[u8]>) -> PgResult<PgReFlags> {
    let mut flags = PgReFlags { cflags: REG_ADVANCED, glob: false };

    if let Some(opt_p) = opts {
        for i in 0..opt_p.len() {
            match opt_p[i] {
                b'g' => flags.glob = true,
                b'b' => flags.cflags &= !(REG_ADVANCED | REG_EXTENDED | REG_QUOTE),
                b'c' => flags.cflags &= !REG_ICASE,
                b'e' => {
                    flags.cflags |= REG_EXTENDED;
                    flags.cflags &= !(REG_ADVANCED | REG_QUOTE);
                }
                b'i' => flags.cflags |= REG_ICASE,
                b'm' | b'n' => flags.cflags |= REG_NEWLINE,
                b'p' => {
                    flags.cflags |= REG_NLSTOP;
                    flags.cflags &= !REG_NLANCH;
                }
                b'q' => {
                    flags.cflags |= REG_QUOTE;
                    flags.cflags &= !(REG_ADVANCED | REG_EXTENDED);
                }
                b's' => flags.cflags &= !REG_NEWLINE,
                b't' => flags.cflags &= !REG_EXPANDED,
                b'w' => {
                    flags.cflags &= !REG_NLSTOP;
                    flags.cflags |= REG_NLANCH;
                }
                b'x' => flags.cflags |= REG_EXPANDED,
                _ => return Err(invalid_re_option(&opt_p[i..]).into()),
            }
        }
    }

    Ok(flags)
}

#[cold]
#[inline(never)]
fn invalid_re_option(opt: &[u8]) -> PgError {
    let mblen =
        (mbutils::pg_mblen_range(opt).unwrap_or(opt.len() as i32) as usize).min(opt.len());
    PgError::error(format!(
        "invalid regular expression option: \"{}\"",
        String::from_utf8_lossy(&opt[..mblen])
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cold]
#[inline(never)]
pub(crate) fn invalid_param(name: &str, value: i32) -> PgError {
    PgError::error(format!("invalid value for parameter \"{name}\": {value}"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cold]
#[inline(never)]
pub(crate) fn global_unsupported(fnname: &str) -> PgError {
    PgError::error(format!("{fnname} does not support the \"global\" option"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

pub(crate) fn check_start(start: Option<i32>) -> PgResult<i32> {
    match start {
        Some(start) if start <= 0 => Err(invalid_param("start", start).into()),
        Some(start) => Ok(start),
        None => Ok(1),
    }
}

pub fn textregexsubstr<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if let Some(re) = regexp_alt::dispatch(p, REG_ADVANCED, s)?
        .filter(|re| re.capture_safe() || re.ngroups() == 0)
    {
        let mut groups = [(-1i64, -1i64); 2];
        let want = if re.ngroups() > 0 { 2 } else { 1 };
        if !re.exec(s, 0, &mut groups[..want]) {
            return Ok(None);
        }
        let (so, eo) = groups[want - 1];
        // 'foo(bar)?' matches 'foo' with no subexpression match.
        if so < 0 || eo < 0 {
            return Ok(None);
        }
        return Ok(Some(slice_in(mcx, &s[so as usize..eo as usize])?));
    }

    let re = RE_compile_and_cache(mcx, p, REG_ADVANCED, collation)?;

    let mut pmatch = [RegMatch::UNSET; 2];
    let wide = mbutils::pg_mb2wchar_with_len(mcx, s)?;
    if !RE_wchar_execute(&re, &wide, 0, &mut pmatch)? {
        return Ok(None);
    }

    let (so, eo) = if re.re_nsub > 0 {
        (pmatch[1].rm_so, pmatch[1].rm_eo)
    } else {
        (pmatch[0].rm_so, pmatch[0].rm_eo)
    };

    // 'foo(bar)?' matches 'foo' with no subexpression match, so this test is
    // not redundant with the whole-match test above.
    if so < 0 || eo < 0 {
        return Ok(None);
    }

    // C: text_substr(s, so+1, eo-so); the wchar slice round-trips to the same
    // bytes without a second character walk.
    if mbutils::pg_database_encoding_max_length() == 1 {
        Ok(Some(slice_in(mcx, &s[so as usize..eo as usize])?))
    } else {
        Ok(Some(mbutils::pg_wchar2mb_with_len(mcx, &wide[so as usize..eo as usize])?))
    }
}

pub fn textregexreplace_noopt<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    varlena::replace_regexp::replace_text_regexp(mcx, s, p, r, REG_ADVANCED, collation, 0, 1)
}

pub fn textregexreplace<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    opt: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    // A numeric-looking 4th arg of type UNKNOWN was probably meant for the
    // start-parameter form; same error parse_re_flags gives, plus a HINT.
    if !opt.is_empty() && opt[0].is_ascii_digit() {
        return Err(invalid_re_option(opt).with_hint(
            "If you meant to use regexp_replace() with a start parameter, cast the fourth argument to integer explicitly.",
        ).into());
    }

    let flags = parse_re_flags(Some(opt))?;

    varlena::replace_regexp::replace_text_regexp(
        mcx,
        s,
        p,
        r,
        flags.cflags,
        collation,
        0,
        if flags.glob { 0 } else { 1 },
    )
}

#[allow(clippy::too_many_arguments)]
pub fn textregexreplace_extended<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    start: Option<i32>,
    n: Option<i32>,
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let start = match start {
        Some(start) if start <= 0 => return Err(invalid_param("start", start).into()),
        Some(start) => start,
        None => 1,
    };
    let n_specified = n.is_some();
    let mut n = match n {
        Some(n) if n < 0 => return Err(invalid_param("n", n).into()),
        Some(n) => n,
        None => 1,
    };

    let re_flags = parse_re_flags(flags)?;

    if !n_specified {
        n = if re_flags.glob { 0 } else { 1 };
    }

    varlena::replace_regexp::replace_text_regexp(
        mcx,
        s,
        p,
        r,
        re_flags.cflags,
        collation,
        start - 1,
        n,
    )
}

pub fn similar_escape_internal<'mcx>(
    mcx: Mcx<'mcx>,
    pat_text: &[u8],
    esc_text: Option<&[u8]>,
) -> PgResult<PgVec<'mcx, u8>> {
    let p_bytes = pat_text;
    let plen = p_bytes.len();
    let mut p = 0usize;

    let e: Option<&[u8]>;
    let elen: usize;
    match esc_text {
        None => {
            e = Some(b"\\");
            elen = 1;
        }
        Some(esc) => {
            elen = esc.len();
            if elen == 0 {
                e = None;
            } else {
                if elen > 1 {
                    let escape_mblen = mbutils::pg_mbstrlen_with_len(esc)?;
                    if escape_mblen > 1 {
                        return Err(PgError::error("invalid escape string")
                            .with_sqlstate(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                            .with_hint("Escape string must be empty or one character.")
                            .into());
                    }
                }
                e = Some(esc);
            }
        }
    }

    // Emits ^(?:pat)$, or ^(?:part1){1,1}?(part2){1,1}(?:part3)$ when the
    // pattern is split by escape-double-quotes. charclass_pos: 1 right after
    // '[', 2 right after '[^', >=3 further inside (']' then ends the class);
    // meaningless while bracket_depth == 0.
    let mut afterescape = false;
    let mut nquotes = 0;
    let mut bracket_depth = 0;
    let mut charclass_pos = 0;

    // C: palloc(VARHDRSZ + 23 + 3 * plen); every write stays within this
    // reservation.
    let mut r: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 23 + 3 * plen)?;

    vec_append_bytes(&mut r, b"^(?:")?;

    let mut plen_left = plen;
    while plen_left > 0 {
        let pchar = p_bytes[p];

        // Only when the escape is multi-byte must we parse the pattern by
        // characters; a single-byte escape can never be a prefix of a valid
        // multi-byte character in any server encoding.
        if elen > 1 {
            let mblen = (mbutils::pg_mblen_range(&p_bytes[p..])? as usize).min(plen_left);
            if mblen > 1 {
                if afterescape {
                    r.push(b'\\');
                    vec_append_bytes(&mut r, &p_bytes[p..p + mblen])?;
                    afterescape = false;
                } else if e.is_some_and(|e| elen == mblen && e == &p_bytes[p..p + mblen]) {
                    afterescape = true;
                } else {
                    vec_append_bytes(&mut r, &p_bytes[p..p + mblen])?;
                }
                p += mblen;
                plen_left -= mblen;
                continue;
            }
        }

        if afterescape {
            if pchar == b'"' && bracket_depth < 1 {
                if nquotes == 0 {
                    vec_append_bytes(&mut r, b"){1,1}?(")?;
                } else if nquotes == 1 {
                    vec_append_bytes(&mut r, b"){1,1}(?:")?;
                } else {
                    return Err(PgError::error(
                        "SQL regular expression may not contain more than two escape-double-quote separators",
                    )
                    .with_sqlstate(ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER)
                    .into());
                }
                nquotes += 1;
            } else {
                // Any character may be escaped (POSIX class escapes like \d
                // stay reachable); the SQL spec is more restrictive.
                r.push(b'\\');
                r.push(pchar);
                charclass_pos = 3;
            }
            afterescape = false;
        } else if e.is_some_and(|e| pchar == e[0]) {
            afterescape = true;
        } else if bracket_depth > 0 {
            if pchar == b'\\' {
                r.push(b'\\');
            }
            r.push(pchar);

            if pchar == b']' && charclass_pos > 2 {
                bracket_depth -= 1;
            } else if pchar == b'[' {
                bracket_depth += 1;
                charclass_pos = 3;
            } else if pchar == b'^' {
                charclass_pos += 1;
            } else {
                charclass_pos = 3;
            }
        } else if pchar == b'[' {
            r.push(pchar);
            bracket_depth = 1;
            charclass_pos = 1;
        } else if pchar == b'%' {
            vec_append_bytes(&mut r, b".*")?;
        } else if pchar == b'_' {
            r.push(b'.');
        } else if pchar == b'(' {
            vec_append_bytes(&mut r, b"(?:")?;
        } else if pchar == b'\\' || pchar == b'.' || pchar == b'^' || pchar == b'$' {
            r.push(b'\\');
            r.push(pchar);
        } else {
            r.push(pchar);
        }
        p += 1;
        plen_left -= 1;
    }

    vec_append_bytes(&mut r, b")$")?;

    Ok(r)
}

pub fn similar_to_escape_2<'mcx>(
    mcx: Mcx<'mcx>,
    pat_text: &[u8],
    esc_text: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    similar_escape_internal(mcx, pat_text, Some(esc_text))
}

pub fn similar_to_escape_1<'mcx>(mcx: Mcx<'mcx>, pat_text: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    similar_escape_internal(mcx, pat_text, None)
}

// Legacy pre-v13 SIMILAR TO expansion; non-strict: NULL pattern returns NULL,
// NULL escape selects the default escape character.
pub fn similar_escape<'mcx>(
    mcx: Mcx<'mcx>,
    pat_text: Option<&[u8]>,
    esc_text: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let Some(pat_text) = pat_text else { return Ok(None) };
    Ok(Some(similar_escape_internal(mcx, pat_text, esc_text)?))
}

pub fn regexp_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    text_re: &[u8],
    case_insensitive: bool,
    collation: Oid,
) -> PgResult<Option<(PgVec<'mcx, u8>, bool)>> {
    let mut cflags = REG_ADVANCED;
    if case_insensitive {
        cflags |= REG_ICASE;
    }

    let re = RE_compile_and_cache(mcx, text_re, cflags | REG_NOSUB, collation)?;

    let (str, exact) = match engine::seam_pg_regprefix(mcx, &re)? {
        RegprefixResult::NoMatch => return Ok(None),
        RegprefixResult::Prefix(str) => (str, false),
        RegprefixResult::Exact(str) => (str, true),
        RegprefixResult::Failed(f) => return Err(regexp_failed(&f.message).into()),
    };

    let result = mbutils::pg_wchar2mb_with_len(mcx, &str)?;
    Ok(Some((result, exact)))
}

pub fn init_seams() {
    regexp_seams::RE_compile_and_cache::set(RE_compile_and_cache);
    regexp_seams::RE_compile_and_execute::set(RE_compile_and_execute);
    regexp_seams::regexp_fixed_prefix::set(regexp_fixed_prefix);
}

#[cfg(test)]
fn cache_keys() -> Vec<(Vec<u8>, i32, Oid)> {
    with_cache(|cache| {
        cache
            .entries
            .iter()
            .map(|e| (e.cre_pat.as_slice().to_vec(), e.cre_flags, e.cre_collation))
            .collect()
    })
}

#[cfg(test)]
mod tests;
