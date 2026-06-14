//! `utils/adt/regexp.c` — Postgres' interface to the regular expression
//! package: the `~` operator family, `substring(string from pattern)`,
//! the `regexp_*` functions, `SIMILAR TO` escaping, and the self-organizing
//! precompiled-RE cache.
//!
//! Functions are exposed on `text` payload bytes (database encoding) and
//! typed scalar arguments; the `Datum`/`PG_FUNCTION_ARGS` marshaling (and
//! the `SRF_*` protocol for the two set-returning functions, for which
//! [`regexp_matches`] / [`regexp_split_to_table`] are the materialize-mode
//! equivalents — [`setup_regexp_matches`] / [`build_regexp_match_result`] /
//! [`build_regexp_split_result`] are public for a future value-per-call
//! driver) belongs to the fmgr layer.
//!
//! The regex engine itself (`backend/regex/*`) is reached through
//! `backend-regex-core-seams`; `text_substr` and `replace_text_regexp`
//! (varlena.c) through `backend-utils-adt-varlena-seams`; the multibyte
//! helpers (mbutils.c) through `backend-utils-mb-mbutils-seams`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;

use backend_regex_core_seams as engine;
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_utils_mb_mbutils_seams as mb;
use mcx::{slice_in, vec_with_capacity_in, Mcx, McxOwned, MemoryContext, PgVec, MAX_ALLOC_SIZE};
use types_core::{Oid, PgWChar};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_ESCAPE_SEQUENCE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_REGULAR_EXPRESSION, ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexecResult, RegprefixResult, REG_ADVANCED,
    REG_EXPANDED, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NLANCH, REG_NLSTOP, REG_NOSUB,
    REG_QUOTE,
};

/// C: `MAX_CACHED_RES` — maximum number of cached compiled regular
/// expressions.
pub const MAX_CACHED_RES: usize = 32;

/// C: `pg_re_flags` — all the options of interest for regex functions.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgReFlags {
    /// `cflags` — compile flags for Spencer's regex code.
    pub cflags: i32,
    /// `glob` — do it globally (for each occurrence).
    pub glob: bool,
}

// ===========================================================================
// The precompiled-RE cache (RegexpCacheMemoryContext / re_array / num_res):
// a self-organizing list with most-recently-used entries at the front.
//
// In C each entry carries a dedicated per-regexp memory context (child of
// RegexpCacheMemoryContext, identifier = the pattern) holding the compiled
// regex_t and the pattern copy; eviction is MemoryContextDelete. Here the
// compiled state is the engine `regex_t` value carried in RegexCompiled (see
// types-regex), so the pattern copy lives directly in the cache context and
// eviction frees the engine state through the pg_regfree seam.
// ===========================================================================

/// C: `cached_re_str` — one cached compiled regular expression.
struct CachedRe<'mcx> {
    /// `cre_pat` / `cre_pat_len` — original RE bytes (not NUL-terminated).
    cre_pat: PgVec<'mcx, u8>,
    /// `cre_flags` — compile flags: extended, icase, etc.
    cre_flags: i32,
    /// `cre_collation` — collation to use.
    cre_collation: Oid,
    /// `cre_re` — the compiled regular expression (engine `regex_t` value +
    /// re_nsub).
    re: RegexCompiled,
}

/// C: `static MemoryContext RegexpCacheMemoryContext` + `static cached_re_str
/// re_array[MAX_CACHED_RES]` + `static int num_res`.
struct ReCache<'mcx> {
    mcx: Mcx<'mcx>,
    entries: PgVec<'mcx, CachedRe<'mcx>>,
}

mcx::bind!(ReCacheTy => ReCache<'mcx>);

thread_local! {
    static RE_CACHE: RefCell<Option<McxOwned<ReCacheTy>>> = const { RefCell::new(None) };
}

/// C: `RE_compile_and_cache` — compile a RE, caching if possible.
///
/// `pattern` is the RE `text` payload in the database encoding; `mcx` is for
/// the wide-character conversion scratch (C: palloc + pfree in the current
/// context). C returns a `regex_t *` into the cache front entry; here the
/// compiled RE crosses as [`RegexCompiled`].
pub fn RE_compile_and_cache(
    mcx: Mcx<'_>,
    pattern: &[u8],
    cflags: i32,
    collation: Oid,
) -> PgResult<RegexCompiled> {
    // Look for a match among previously compiled REs. Since the data
    // structure is self-organizing with most-used entries at the front, our
    // search strategy can just be to scan from the front.
    let hit = RE_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let Some(owned) = slot.as_mut() else { return None };
        owned.with_mut(|cache| {
            let i = cache.entries.iter().position(|e| {
                e.cre_pat.len() == pattern.len()
                    && e.cre_flags == cflags
                    && e.cre_collation == collation
                    && e.cre_pat.as_slice() == pattern
            })?;
            // Found a match; move it to front if not there already.
            if i > 0 {
                let entry = cache.entries.remove(i);
                cache.entries.insert(0, entry);
            }
            Some(cache.entries[0].re.clone())
        })
    });
    if let Some(re) = hit {
        return Ok(re);
    }

    // Set up the cache memory on first go through.
    let initialized = RE_CACHE.with(|cell| cell.borrow().is_some());
    if !initialized {
        let owned = McxOwned::<ReCacheTy>::try_new(
            MemoryContext::new("RegexpCacheMemoryContext"),
            |mcx| Ok(ReCache { mcx, entries: PgVec::new_in(mcx) }),
        )?;
        RE_CACHE.with(|cell| *cell.borrow_mut() = Some(owned));
    }

    // Couldn't find it, so try to compile the new RE.
    //
    // Convert pattern string to wide characters.
    let wide_pattern = mb::pg_mb2wchar_with_len::call(mcx, pattern)?;

    let compiled = match engine::pg_regcomp::call(&wide_pattern, cflags, collation)? {
        RegcompResult::Compiled(c) => c,
        RegcompResult::Failed(f) => {
            // re didn't compile (no need for pg_regfree, if so)
            return Err(PgError::error(format!("invalid regular expression: {}", f.message))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION));
        }
    };
    drop(wide_pattern); // C: pfree(pattern)

    // Okay, we have a valid new item; insert it into the storage array.
    // Discard the last entry if needed. On any insertion failure, free the
    // engine state (C gets this for free from the per-regexp context being
    // a child of the current context until re-parented).
    let inserted: PgResult<()> = RE_CACHE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let owned = slot.as_mut().expect("regexp cache initialized above");
        owned.with_mut(|cache| {
            // Copy the pattern into the cache memory (C: the per-regexp
            // context, identifier for pg_backend_memory_contexts).
            let pat_copy = slice_in(cache.mcx, pattern)?;
            cache
                .entries
                .try_reserve(1)
                .map_err(|_| cache.mcx.oom(core::mem::size_of::<CachedRe<'_>>()))?;
            if cache.entries.len() >= MAX_CACHED_RES {
                // C: --num_res; MemoryContextDelete(re_array[num_res].cre_context);
                if let Some(evicted) = cache.entries.pop() {
                    engine::pg_regfree::call(evicted.re);
                }
            }
            cache.entries.insert(
                0,
                CachedRe {
                    cre_pat: pat_copy,
                    cre_flags: cflags,
                    cre_collation: collation,
                    re: compiled.clone(),
                },
            );
            Ok(())
        })
    });
    if let Err(e) = inserted {
        engine::pg_regfree::call(compiled);
        return Err(e);
    }

    Ok(compiled)
}

/// C: `RE_wchar_execute` — execute a RE on `pg_wchar` data.
///
/// Returns true on match, false on no match; `pmatch` (the optional return
/// area for match details, `nmatch == pmatch.len()`) is filled on a match.
fn RE_wchar_execute(
    re: &RegexCompiled,
    data: &[PgWChar],
    start_search: i32,
    pmatch: &mut [RegMatch],
) -> PgResult<bool> {
    match engine::pg_regexec::call(re, data, start_search, pmatch)? {
        RegexecResult::Matched => Ok(true),
        RegexecResult::NoMatch => Ok(false),
        RegexecResult::Failed(f) => {
            // re failed???
            Err(PgError::error(format!("regular expression failed: {}", f.message))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION))
        }
    }
}

/// C: `RE_execute` — execute a RE on data in the database encoding.
///
/// The data is converted to `pg_wchar` (scratch in `mcx`) and matched from
/// offset 0.
fn RE_execute(mcx: Mcx<'_>, re: &RegexCompiled, dat: &[u8], pmatch: &mut [RegMatch]) -> PgResult<bool> {
    // Convert data string to wide characters.
    let data = mb::pg_mb2wchar_with_len::call(mcx, dat)?;
    RE_wchar_execute(re, &data, 0, pmatch)
}

/// C: `RE_compile_and_execute` — compile and execute a RE.
///
/// Returns true on match, false on no match. Both pattern and data are given
/// in the database encoding.
pub fn RE_compile_and_execute(
    mcx: Mcx<'_>,
    pattern: &[u8],
    dat: &[u8],
    mut cflags: i32,
    collation: Oid,
    pmatch: &mut [RegMatch],
) -> PgResult<bool> {
    // Use REG_NOSUB if caller does not want sub-match details.
    if pmatch.len() < 2 {
        cflags |= REG_NOSUB;
    }

    // Compile RE.
    let re = RE_compile_and_cache(mcx, pattern, cflags, collation)?;

    RE_execute(mcx, &re, dat, pmatch)
}

/// C: `parse_re_flags` — parse the options argument of `regexp_match` and
/// friends. `opts` is the options `text` payload, or `None` for defaults.
///
/// This accepts all the options allowed by any of the callers; callers that
/// don't want some have to reject them after the fact.
pub fn parse_re_flags(opts: Option<&[u8]>) -> PgResult<PgReFlags> {
    // regex flavor is always folded into the compile flags
    let mut flags = PgReFlags { cflags: REG_ADVANCED, glob: false };

    if let Some(opt_p) = opts {
        let mut i = 0;
        while i < opt_p.len() {
            match opt_p[i] {
                b'g' => flags.glob = true,
                b'b' => {
                    // BREs (but why???)
                    flags.cflags &= !(REG_ADVANCED | REG_EXTENDED | REG_QUOTE);
                }
                b'c' => {
                    // case sensitive
                    flags.cflags &= !REG_ICASE;
                }
                b'e' => {
                    // plain EREs
                    flags.cflags |= REG_EXTENDED;
                    flags.cflags &= !(REG_ADVANCED | REG_QUOTE);
                }
                b'i' => {
                    // case insensitive
                    flags.cflags |= REG_ICASE;
                }
                b'm' | b'n' => {
                    // 'm' is a Perloid synonym for 'n'; \n affects ^ $ . [^
                    flags.cflags |= REG_NEWLINE;
                }
                b'p' => {
                    // ~Perl, \n affects . [^
                    flags.cflags |= REG_NLSTOP;
                    flags.cflags &= !REG_NLANCH;
                }
                b'q' => {
                    // literal string
                    flags.cflags |= REG_QUOTE;
                    flags.cflags &= !(REG_ADVANCED | REG_EXTENDED);
                }
                b's' => {
                    // single line, \n ordinary
                    flags.cflags &= !REG_NEWLINE;
                }
                b't' => {
                    // tight syntax
                    flags.cflags &= !REG_EXPANDED;
                }
                b'w' => {
                    // weird, \n affects ^ $ only
                    flags.cflags &= !REG_NLSTOP;
                    flags.cflags |= REG_NLANCH;
                }
                b'x' => {
                    // expanded syntax
                    flags.cflags |= REG_EXPANDED;
                }
                _ => {
                    return Err(invalid_re_option(&opt_p[i..]));
                }
            }
            i += 1;
        }
    }

    Ok(flags)
}

/// The `errmsg("invalid regular expression option: \"%.*s\"",
/// pg_mblen_range(opt_p + i, opt_p + opt_len), opt_p + i)` ereport shared by
/// `parse_re_flags` and `textregexreplace`.
fn invalid_re_option(opt: &[u8]) -> PgError {
    let mblen = (mb::pg_mblen_range::call(opt) as usize).min(opt.len());
    PgError::error(format!(
        "invalid regular expression option: \"{}\"",
        String::from_utf8_lossy(&opt[..mblen])
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// ===========================================================================
// interface routines called by the function manager
// ===========================================================================

/// C: `nameregexeq` — the `~` operator over name.
pub fn nameregexeq(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, n, REG_ADVANCED, collation, &mut [])
}

/// C: `nameregexne` — the `!~` operator over name.
pub fn nameregexne(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, n, REG_ADVANCED, collation, &mut [])?)
}

/// C: `textregexeq` — the `~` operator.
pub fn textregexeq(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, s, REG_ADVANCED, collation, &mut [])
}

/// C: `textregexne` — the `!~` operator.
pub fn textregexne(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, s, REG_ADVANCED, collation, &mut [])?)
}

/// C: `nameicregexeq` — the case-insensitive `~*` operator over name.
pub fn nameicregexeq(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, n, REG_ADVANCED | REG_ICASE, collation, &mut [])
}

/// C: `nameicregexne` — the case-insensitive `!~*` operator over name.
pub fn nameicregexne(mcx: Mcx<'_>, n: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, n, REG_ADVANCED | REG_ICASE, collation, &mut [])?)
}

/// C: `texticregexeq` — the case-insensitive `~*` operator.
pub fn texticregexeq(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    RE_compile_and_execute(mcx, p, s, REG_ADVANCED | REG_ICASE, collation, &mut [])
}

/// C: `texticregexne` — the case-insensitive `!~*` operator.
pub fn texticregexne(mcx: Mcx<'_>, s: &[u8], p: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(!RE_compile_and_execute(mcx, p, s, REG_ADVANCED | REG_ICASE, collation, &mut [])?)
}

/// C: `textregexsubstr` — `substring(string from pattern)`: return a
/// substring matched by a regular expression. `None` is `PG_RETURN_NULL()`.
pub fn textregexsubstr<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Compile RE
    let re = RE_compile_and_cache(mcx, p, REG_ADVANCED, collation)?;

    // We pass two regmatch_t structs to get info about the overall match and
    // the match for the first parenthesized subexpression (if any). If there
    // is a parenthesized subexpression, we return what it matched; else
    // return what the whole regexp matched.
    let mut pmatch = [RegMatch::UNSET; 2];
    if !RE_execute(mcx, &re, s, &mut pmatch)? {
        return Ok(None); // definitely no match
    }

    let (so, eo) = if re.re_nsub > 0 {
        // has parenthesized subexpressions, use the first one
        (pmatch[1].rm_so, pmatch[1].rm_eo)
    } else {
        // no parenthesized subexpression, use whole match
        (pmatch[0].rm_so, pmatch[0].rm_eo)
    };

    // It is possible to have a match to the whole pattern but no match for a
    // subexpression; for example 'foo(bar)?' is considered to match 'foo' but
    // there is no subexpression match. So this extra test for match failure
    // is not redundant.
    if so < 0 || eo < 0 {
        return Ok(None);
    }

    Ok(Some(varlena_seams::text_substr::call(mcx, s, so as i32 + 1, (eo - so) as i32)?))
}

// ===========================================================================
// regexp_replace family (the replace_text_regexp worker is varlena.c's).
// ===========================================================================

/// C: `textregexreplace_noopt` — `regexp_replace(s, p, r)`: default to case
/// sensitive match, replace the first instance only.
pub fn textregexreplace_noopt<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    varlena_seams::replace_text_regexp::call(mcx, s, p, r, REG_ADVANCED, collation, 0, 1)
}

/// C: `textregexreplace` — `regexp_replace(s, p, r, opt)`.
pub fn textregexreplace<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    opt: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    // regexp_replace() with four arguments will be preferentially resolved
    // as this form when the fourth argument is of type UNKNOWN. However, the
    // user might have intended to call textregexreplace_extended_no_n. If we
    // see flags that look like an integer, emit the same error that
    // parse_re_flags would, but add a HINT about how to fix it.
    if !opt.is_empty() && opt[0].is_ascii_digit() {
        return Err(invalid_re_option(opt).with_hint(
            "If you meant to use regexp_replace() with a start parameter, cast the fourth argument to integer explicitly.",
        ));
    }

    let flags = parse_re_flags(Some(opt))?;

    varlena_seams::replace_text_regexp::call(
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

/// C: `textregexreplace_extended` — `regexp_replace` with a start position
/// and the choice of the occurrence to replace (0 means all occurrences).
///
/// `start`/`n`/`flags` are `None` when the SQL argument was absent
/// (C: `PG_NARGS()` checks); `start` is 1-based.
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
    // Collect optional parameters
    let start = match start {
        Some(start) => {
            if start <= 0 {
                return Err(invalid_param("start", start));
            }
            start
        }
        None => 1,
    };
    let n_specified = n.is_some();
    let mut n = match n {
        Some(n) => {
            if n < 0 {
                return Err(invalid_param("n", n));
            }
            n
        }
        None => 1,
    };

    // Determine options
    let re_flags = parse_re_flags(flags)?;

    // If N was not specified, deduce it from the 'g' flag
    if !n_specified {
        n = if re_flags.glob { 0 } else { 1 };
    }

    // Do the replacement(s)
    varlena_seams::replace_text_regexp::call(
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

/// C: `textregexreplace_extended_no_n` — separate to keep the opr_sanity
/// regression test from complaining.
pub fn textregexreplace_extended_no_n<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    start: i32,
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    textregexreplace_extended(mcx, s, p, r, Some(start), None, flags, collation)
}

/// C: `textregexreplace_extended_no_flags` — separate to keep the opr_sanity
/// regression test from complaining.
pub fn textregexreplace_extended_no_flags<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    p: &[u8],
    r: &[u8],
    start: i32,
    n: i32,
    collation: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    textregexreplace_extended(mcx, s, p, r, Some(start), Some(n), None, collation)
}

// ===========================================================================
// similar_to_escape() / similar_escape(): convert a SQL "SIMILAR TO" regexp
// pattern to POSIX style, so it can be used by our regexp engine.
// ===========================================================================

/// C: `similar_escape_internal` — the common workhorse for three SQL-exposed
/// functions. `esc_text` can be passed as `None` to select the default
/// escape (which is `\`), or as an empty string to select no escape
/// character.
pub fn similar_escape_internal<'mcx>(
    mcx: Mcx<'mcx>,
    pat_text: &[u8],
    esc_text: Option<&[u8]>,
) -> PgResult<PgVec<'mcx, u8>> {
    let p_bytes = pat_text;
    let plen = p_bytes.len();
    let mut p = 0usize;

    let e: Option<&[u8]>; // None == no escape character
    let elen: usize;
    match esc_text {
        None => {
            // No ESCAPE clause provided; default to backslash as escape.
            e = Some(b"\\");
            elen = 1;
        }
        Some(esc) => {
            elen = esc.len();
            if elen == 0 {
                e = None; // no escape character
            } else {
                if elen > 1 {
                    let escape_mblen = mb::pg_mbstrlen_with_len::call(esc, elen as i32);
                    if escape_mblen > 1 {
                        return Err(PgError::error("invalid escape string")
                            .with_sqlstate(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                            .with_hint("Escape string must be empty or one character."));
                    }
                }
                e = Some(esc);
            }
        }
    }

    // We surround the transformed input string with ^(?: ... )$; when the
    // pattern is divided into three parts by escape-double-quotes, what we
    // emit is ^(?:part1){1,1}?(part2){1,1}(?:part3)$. See regexp.c for the
    // full explanation of the greediness markers and the SUBSTRING capture.
    //
    // While we don't fully validate character classes (bracket expressions),
    // we parse them well enough to know where they end. charclass_pos tracks
    // where we are in a character class; its value is uninteresting when
    // bracket_depth is 0, but when bracket_depth > 0 it is 1 right after the
    // opening '[' (a following '^' will negate the class, while ']' is a
    // literal character), 2 right after a '^' after the opening '[' (']' is
    // still a literal character), and 3 or more further inside the character
    // class (']' ends the class).
    let mut afterescape = false;
    let mut nquotes = 0;
    let mut bracket_depth = 0; // square bracket nesting level
    let mut charclass_pos = 0; // position inside a character class

    // We need room for the prefix/postfix and part separators, plus as many
    // as 3 output bytes per input byte; since the input is at most 1GB this
    // can't overflow. C: palloc(VARHDRSZ + 23 + 3 * plen); every write below
    // stays within this reservation, so no later (re)allocation happens.
    let mut r: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, 23 + 3 * plen)?;

    r.extend_from_slice(b"^(?:");

    let mut plen_left = plen;
    while plen_left > 0 {
        let pchar = p_bytes[p];

        // If both the escape character and the current character from the
        // pattern are multi-byte, we need to take the slow path. But if one
        // of them is single-byte, we can process the pattern one byte at a
        // time, ignoring multi-byte characters. (This works because all
        // server-encodings have the property that a valid multi-byte
        // character representation cannot contain the representation of a
        // valid single-byte character.)
        if elen > 1 {
            let mblen = (mb::pg_mblen_range::call(&p_bytes[p..]) as usize).min(plen_left);
            if mblen > 1 {
                // slow, multi-byte path
                if afterescape {
                    r.push(b'\\');
                    r.extend_from_slice(&p_bytes[p..p + mblen]);
                    afterescape = false;
                } else if e.is_some_and(|e| elen == mblen && e == &p_bytes[p..p + mblen]) {
                    // SQL escape character; do not send to output
                    afterescape = true;
                } else {
                    // We know it's a multi-byte character, so we don't need
                    // to do all the comparisons to single-byte characters
                    // that we do below.
                    r.extend_from_slice(&p_bytes[p..p + mblen]);
                }
                p += mblen;
                plen_left -= mblen;
                continue;
            }
        }

        // fast path
        if afterescape {
            if pchar == b'"' && bracket_depth < 1 {
                // escape-double-quote? emit appropriate part separator
                if nquotes == 0 {
                    r.extend_from_slice(b"){1,1}?(");
                } else if nquotes == 1 {
                    r.extend_from_slice(b"){1,1}(?:");
                } else {
                    return Err(PgError::error(
                        "SQL regular expression may not contain more than two escape-double-quote separators",
                    )
                    .with_sqlstate(ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER));
                }
                nquotes += 1;
            } else {
                // We allow any character at all to be escaped; notably, this
                // allows access to POSIX character-class escapes such as
                // "\d". The SQL spec is considerably more restrictive.
                r.push(b'\\');
                r.push(pchar);
                // If we encounter an escaped character in a character class,
                // we are no longer at the beginning.
                charclass_pos = 3;
            }
            afterescape = false;
        } else if e.is_some_and(|e| pchar == e[0]) {
            // SQL escape character; do not send to output
            afterescape = true;
        } else if bracket_depth > 0 {
            // inside a character class
            if pchar == b'\\' {
                // Backslash is not the SQL escape character here, so treat
                // it as a literal class element, which requires doubling it.
                r.push(b'\\');
            }
            r.push(pchar);

            // parse the character class well enough to identify ending ']'
            if pchar == b']' && charclass_pos > 2 {
                // found the real end of a bracket pair
                bracket_depth -= 1;
                // don't reset charclass_pos, this may be an inner bracket
            } else if pchar == b'[' {
                // start of a nested bracket pair (a collating element, not a
                // character class in its own right)
                bracket_depth += 1;
                charclass_pos = 3;
            } else if pchar == b'^' {
                // A caret right after the opening bracket negates the
                // character class; incrementing keeps a following ']'
                // literal. Further inside the class it may pass 3 — fine.
                charclass_pos += 1;
            } else {
                // Anything else (including a leading ']') is an element of
                // the character class.
                charclass_pos = 3;
            }
        } else if pchar == b'[' {
            // start of a character class
            r.push(pchar);
            bracket_depth = 1;
            charclass_pos = 1;
        } else if pchar == b'%' {
            r.extend_from_slice(b".*");
        } else if pchar == b'_' {
            r.push(b'.');
        } else if pchar == b'(' {
            // convert to non-capturing parenthesis
            r.extend_from_slice(b"(?:");
        } else if pchar == b'\\' || pchar == b'.' || pchar == b'^' || pchar == b'$' {
            r.push(b'\\');
            r.push(pchar);
        } else {
            r.push(pchar);
        }
        p += 1;
        plen_left -= 1;
    }

    r.extend_from_slice(b")$");

    Ok(r)
}

/// C: `similar_to_escape_2` — `similar_to_escape(pattern, escape)`.
pub fn similar_to_escape_2<'mcx>(
    mcx: Mcx<'mcx>,
    pat_text: &[u8],
    esc_text: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    similar_escape_internal(mcx, pat_text, Some(esc_text))
}

/// C: `similar_to_escape_1` — `similar_to_escape(pattern)`, inserting the
/// default escape character.
pub fn similar_to_escape_1<'mcx>(mcx: Mcx<'mcx>, pat_text: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    similar_escape_internal(mcx, pat_text, None)
}

/// C: `similar_escape` — legacy function for compatibility with views stored
/// using the pre-v13 expansion of SIMILAR TO. Unlike the above functions,
/// this is non-strict, which leads to not-per-spec handling of "ESCAPE
/// NULL": a NULL pattern returns NULL, a NULL escape selects the default
/// escape character.
pub fn similar_escape<'mcx>(
    mcx: Mcx<'mcx>,
    pat_text: Option<&[u8]>,
    esc_text: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // This function is not strict, so must test explicitly
    let Some(pat_text) = pat_text else { return Ok(None) };
    Ok(Some(similar_escape_internal(mcx, pat_text, esc_text)?))
}

// ===========================================================================
// regexp_count / regexp_instr / regexp_like.
// ===========================================================================

/// C: `regexp_count` — the number of matches of a pattern within a string.
/// `start` is 1-based; `None` when the SQL argument was absent.
pub fn regexp_count(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: Option<i32>,
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<i32> {
    // Collect optional parameters
    let start = check_start(start)?;

    // Determine options
    let mut re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_count()"));
    }
    // But we find all the matches anyway
    re_flags.glob = true;

    // Do the matching
    let matchctx = setup_regexp_matches(
        mcx, str, pattern, &re_flags, start - 1, collation, false, false, false,
    )?;

    Ok(matchctx.nmatches)
}

/// C: `regexp_count_no_start` — separate for opr_sanity.
pub fn regexp_count_no_start(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<i32> {
    regexp_count(mcx, str, pattern, None, flags, collation)
}

/// C: `regexp_count_no_flags` — separate for opr_sanity.
pub fn regexp_count_no_flags(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    collation: Oid,
) -> PgResult<i32> {
    regexp_count(mcx, str, pattern, Some(start), None, collation)
}

/// C: `regexp_instr` — the match's position within the string.
pub fn regexp_instr(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: Option<i32>,
    n: Option<i32>,
    endoption: Option<i32>,
    flags: Option<&[u8]>,
    subexpr: Option<i32>,
    collation: Oid,
) -> PgResult<i32> {
    // Collect optional parameters
    let start = check_start(start)?;
    let n = match n {
        Some(n) => {
            if n <= 0 {
                return Err(invalid_param("n", n));
            }
            n
        }
        None => 1,
    };
    let endoption = match endoption {
        Some(endoption) => {
            if endoption != 0 && endoption != 1 {
                return Err(invalid_param("endoption", endoption));
            }
            endoption
        }
        None => 0,
    };
    let subexpr = match subexpr {
        Some(subexpr) => {
            if subexpr < 0 {
                return Err(invalid_param("subexpr", subexpr));
            }
            subexpr
        }
        None => 0,
    };

    // Determine options
    let mut re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_instr()"));
    }
    // But we find all the matches anyway
    re_flags.glob = true;

    // Do the matching
    let matchctx = setup_regexp_matches(
        mcx,
        str,
        pattern,
        &re_flags,
        start - 1,
        collation,
        subexpr > 0, // need submatches?
        false,
        false,
    )?;

    // When n exceeds matches return 0 (includes case of no matches)
    if n > matchctx.nmatches {
        return Ok(0);
    }

    // When subexpr exceeds number of subexpressions return 0
    if subexpr > matchctx.npatterns {
        return Ok(0);
    }

    // Select the appropriate match position to return
    let mut pos = (n - 1) * matchctx.npatterns;
    if subexpr > 0 {
        pos += subexpr - 1;
    }
    pos *= 2;
    if endoption == 1 {
        pos += 1;
    }

    if matchctx.match_locs[pos as usize] >= 0 {
        Ok(matchctx.match_locs[pos as usize] + 1)
    } else {
        Ok(0) // position not identifiable
    }
}

/// C: `regexp_instr_no_start` — separate for opr_sanity.
pub fn regexp_instr_no_start(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<i32> {
    regexp_instr(mcx, str, pattern, None, None, None, None, None, collation)
}

/// C: `regexp_instr_no_n` — separate for opr_sanity.
pub fn regexp_instr_no_n(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    collation: Oid,
) -> PgResult<i32> {
    regexp_instr(mcx, str, pattern, Some(start), None, None, None, None, collation)
}

/// C: `regexp_instr_no_endoption` — separate for opr_sanity.
pub fn regexp_instr_no_endoption(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    n: i32,
    collation: Oid,
) -> PgResult<i32> {
    regexp_instr(mcx, str, pattern, Some(start), Some(n), None, None, None, collation)
}

/// C: `regexp_instr_no_flags` — separate for opr_sanity.
pub fn regexp_instr_no_flags(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    n: i32,
    endoption: i32,
    collation: Oid,
) -> PgResult<i32> {
    regexp_instr(
        mcx,
        str,
        pattern,
        Some(start),
        Some(n),
        Some(endoption),
        None,
        None,
        collation,
    )
}

/// C: `regexp_instr_no_subexpr` — separate for opr_sanity.
pub fn regexp_instr_no_subexpr(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    n: i32,
    endoption: i32,
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<i32> {
    regexp_instr(
        mcx,
        str,
        pattern,
        Some(start),
        Some(n),
        Some(endoption),
        flags,
        None,
        collation,
    )
}

/// C: `regexp_like` — test for a pattern match within a string.
pub fn regexp_like(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<bool> {
    // Determine options
    let re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_like()"));
    }

    // Otherwise it's like textregexeq/texticregexeq
    RE_compile_and_execute(mcx, pattern, str, re_flags.cflags, collation, &mut [])
}

/// C: `regexp_like_no_flags` — separate for opr_sanity.
pub fn regexp_like_no_flags(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<bool> {
    regexp_like(mcx, str, pattern, None, collation)
}

// ===========================================================================
// regexp_match / regexp_matches.
// ===========================================================================

/// C: `regexp_match` — the first substring(s) matching a pattern within a
/// string. `None` is SQL NULL; an element is `None` for an unmatched
/// subexpression.
pub fn regexp_match<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, Option<PgVec<'mcx, u8>>>>> {
    // Determine options
    let re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_match()")
            .with_hint("Use the regexp_matches function instead."));
    }

    let mut matchctx =
        setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, true, false, false)?;

    if matchctx.nmatches == 0 {
        return Ok(None);
    }

    debug_assert_eq!(matchctx.nmatches, 1);

    Ok(Some(build_regexp_match_result(&mut matchctx)?))
}

/// C: `regexp_match_no_flags` — separate for opr_sanity.
pub fn regexp_match_no_flags<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, Option<PgVec<'mcx, u8>>>>> {
    regexp_match(mcx, orig_str, pattern, None, collation)
}

/// C: `regexp_matches` — a table of all matches of a pattern within a
/// string.
///
/// The C set-returning function streams one array per call through the
/// `SRF_*` protocol; this materializes the whole result set (one row per
/// match), the materialize-mode equivalent. The per-call pieces
/// ([`setup_regexp_matches`] + [`build_regexp_match_result`]) are public for
/// a future value-per-call fmgr driver.
pub fn regexp_matches<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, Option<PgVec<'mcx, u8>>>>> {
    // Determine options
    let re_flags = parse_re_flags(flags)?;

    let mut matchctx =
        setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, true, false, false)?;

    let mut rows = vec_with_capacity_in(mcx, matchctx.nmatches.max(0) as usize)?;
    while matchctx.next_match < matchctx.nmatches {
        rows.push(build_regexp_match_result(&mut matchctx)?);
        matchctx.next_match += 1;
    }
    Ok(rows)
}

/// C: `regexp_matches_no_flags` — separate for opr_sanity.
pub fn regexp_matches_no_flags<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, Option<PgVec<'mcx, u8>>>>> {
    regexp_matches(mcx, orig_str, pattern, None, collation)
}

// ===========================================================================
// setup_regexp_matches + the regexp_matches_ctx cross-call state.
// ===========================================================================

/// C: `regexp_matches_ctx` — cross-call state for `regexp_match` and
/// `regexp_split` functions.
///
/// The C struct's `elems`/`nulls` Datum workspace belongs to the fmgr array
/// constructor; the `conv_buf`/`conv_bufsiz` conversion buffer is subsumed
/// by the `pg_wchar2mb_with_len` seam allocating its own output.
pub struct RegexpMatchesCtx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    /// `orig_str` — data string in original TEXT (payload) form.
    orig_str: &'a [u8],
    /// `nmatches` — number of places where pattern matched.
    pub nmatches: i32,
    /// `npatterns` — number of capturing subpatterns.
    pub npatterns: i32,
    /// `match_locs` — 0-based character indexes: start char index and end+1
    /// char index for each match, `nmatches * npatterns * 2` entries, plus a
    /// trailing end-of-string position for the splitting code.
    match_locs: PgVec<'mcx, i32>,
    /// `next_match` — 0-based index of next match to process.
    pub next_match: i32,
    /// `wide_str` — wide-char version of original string (kept only for
    /// multibyte encodings, as in C).
    wide_str: Option<PgVec<'mcx, PgWChar>>,
}

/// C: `setup_regexp_matches` — do the initial matching for `regexp_match`,
/// `regexp_split`, and related functions.
///
/// To avoid having to re-find the compiled pattern on each call, we do all
/// the matching in one swoop. The returned [`RegexpMatchesCtx`] contains the
/// locations of all the substrings matching the pattern.
///
/// `start_search` is the character (not byte) offset in `orig_str` at which
/// to begin the search; returned positions are relative to `orig_str`
/// anyway. `use_subpatterns`: collect data about matches to parenthesized
/// subexpressions. `ignore_degenerate`: ignore zero-length matches.
/// `fetching_unmatched`: caller wants to fetch unmatched substrings (in C
/// this sizes the conversion buffer; the computation is kept, the buffer is
/// subsumed by the conversion seam).
pub fn setup_regexp_matches<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    pattern: &[u8],
    re_flags: &PgReFlags,
    mut start_search: i32,
    collation: Oid,
    mut use_subpatterns: bool,
    ignore_degenerate: bool,
    fetching_unmatched: bool,
) -> PgResult<RegexpMatchesCtx<'a, 'mcx>> {
    let eml = mb::pg_database_encoding_max_length::call();
    let mut maxlen: i64 = 0; // largest fetch length in characters

    // convert string to pg_wchar form for matching
    let wide_str = mb::pg_mb2wchar_with_len::call(mcx, orig_str)?;
    let wide_len = wide_str.len() as i32;

    // set up the compiled pattern
    let mut cflags = re_flags.cflags;
    if !use_subpatterns {
        cflags |= REG_NOSUB;
    }
    let cpattern = RE_compile_and_cache(mcx, pattern, cflags, collation)?;

    // do we want to remember subpatterns?
    let npatterns: i32;
    let pmatch_len: usize;
    if use_subpatterns && cpattern.re_nsub > 0 {
        npatterns = cpattern.re_nsub as i32;
        pmatch_len = cpattern.re_nsub + 1;
    } else {
        use_subpatterns = false;
        npatterns = 1;
        pmatch_len = 1;
    }

    // temporary output space for RE package
    let mut pmatch: PgVec<'_, RegMatch> = vec_with_capacity_in(mcx, pmatch_len)?;
    pmatch.resize(pmatch_len, RegMatch::UNSET);

    // the real output space (grown dynamically if needed)
    //
    // use values 2^n-1, not 2^n, so that we hit the limit at 2^28-1 rather
    // than at 2^27
    let mut array_len: i32 = if re_flags.glob { 255 } else { 31 };
    let mut match_locs: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, array_len as usize)?;
    match_locs.resize(array_len as usize, 0);
    let mut array_idx: usize = 0;
    let mut nmatches: i32 = 0;

    // search for the pattern, perhaps repeatedly
    let mut prev_match_end: i64 = 0;
    let mut prev_valid_match_end: i64 = 0;
    while RE_wchar_execute(&cpattern, &wide_str, start_search, &mut pmatch)? {
        // If requested, ignore degenerate matches, which are zero-length
        // matches occurring at the start or end of a string or just after a
        // previous match.
        if !ignore_degenerate
            || (pmatch[0].rm_so < wide_len as i64 && pmatch[0].rm_eo > prev_match_end)
        {
            // enlarge output space if needed
            while array_idx + (npatterns as usize) * 2 + 1 > array_len as usize {
                array_len += array_len + 1; // 2^n-1 => 2^(n+1)-1
                if array_len as usize > MAX_ALLOC_SIZE / core::mem::size_of::<i32>() {
                    return Err(PgError::error("too many regular expression matches")
                        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
                }
                let extra = array_len as usize - match_locs.len();
                match_locs
                    .try_reserve(extra)
                    .map_err(|_| mcx.oom(array_len as usize * core::mem::size_of::<i32>()))?;
                match_locs.resize(array_len as usize, 0);
            }

            // save this match's locations
            if use_subpatterns {
                for i in 1..=npatterns as usize {
                    let so = pmatch[i].rm_so;
                    let eo = pmatch[i].rm_eo;
                    match_locs[array_idx] = so as i32;
                    array_idx += 1;
                    match_locs[array_idx] = eo as i32;
                    array_idx += 1;
                    if so >= 0 && eo >= 0 && (eo - so) > maxlen {
                        maxlen = eo - so;
                    }
                }
            } else {
                let so = pmatch[0].rm_so;
                let eo = pmatch[0].rm_eo;
                match_locs[array_idx] = so as i32;
                array_idx += 1;
                match_locs[array_idx] = eo as i32;
                array_idx += 1;
                if so >= 0 && eo >= 0 && (eo - so) > maxlen {
                    maxlen = eo - so;
                }
            }
            nmatches += 1;

            // check length of unmatched portion between end of previous
            // valid (nondegenerate, or degenerate but not ignored) match and
            // start of current one
            if fetching_unmatched
                && pmatch[0].rm_so >= 0
                && (pmatch[0].rm_so - prev_valid_match_end) > maxlen
            {
                maxlen = pmatch[0].rm_so - prev_valid_match_end;
            }
            prev_valid_match_end = pmatch[0].rm_eo;
        }
        prev_match_end = pmatch[0].rm_eo;

        // if not glob, stop after one match
        if !re_flags.glob {
            break;
        }

        // Advance search position. Normally we start the next search at the
        // end of the previous match; but if the match was of zero length, we
        // have to advance by one character, or we'd just find the same match
        // again.
        start_search = prev_match_end as i32;
        if pmatch[0].rm_so == pmatch[0].rm_eo {
            start_search += 1;
        }
        if start_search > wide_len {
            break;
        }
    }

    // check length of unmatched portion between end of last match and end of
    // input string
    if fetching_unmatched && (wide_len as i64 - prev_valid_match_end) > maxlen {
        maxlen = wide_len as i64 - prev_valid_match_end;
    }
    // C sizes conv_buf from maxlen here; the conversion seam allocates its
    // own output, so maxlen has no further consumer.
    let _ = maxlen;

    // Keep a note of the end position of the string for the benefit of
    // splitting code.
    match_locs[array_idx] = wide_len;

    // No need to keep the wide string if we're in a single-byte charset.
    let wide_str = if eml > 1 { Some(wide_str) } else { None };

    Ok(RegexpMatchesCtx {
        mcx,
        orig_str,
        nmatches,
        npatterns,
        match_locs,
        next_match: 0,
        wide_str,
    })
}

/// C: `build_regexp_match_result` — build the output array for the current
/// match. Each element is `None` for an unmatched subexpression (the array
/// NULL).
pub fn build_regexp_match_result<'mcx>(
    matchctx: &mut RegexpMatchesCtx<'_, 'mcx>,
) -> PgResult<PgVec<'mcx, Option<PgVec<'mcx, u8>>>> {
    let mcx = matchctx.mcx;
    let mut elems = vec_with_capacity_in(mcx, matchctx.npatterns as usize)?;

    // Extract matching substrings from the original string
    let mut loc = (matchctx.next_match * matchctx.npatterns * 2) as usize;
    for _ in 0..matchctx.npatterns {
        let so = matchctx.match_locs[loc];
        loc += 1;
        let eo = matchctx.match_locs[loc];
        loc += 1;

        if so < 0 || eo < 0 {
            elems.push(None);
        } else if let Some(wide) = &matchctx.wide_str {
            // multibyte: pg_wchar2mb_with_len(wide_str + so, buf, eo - so)
            let converted = mb::pg_wchar2mb_with_len::call(mcx, &wide[so as usize..eo as usize])?;
            elems.push(Some(converted));
        } else {
            // single-byte: DirectFunctionCall3(text_substr, orig_str, so+1, eo-so)
            elems.push(Some(varlena_seams::text_substr::call(
                mcx,
                matchctx.orig_str,
                so + 1,
                eo - so,
            )?));
        }
    }

    Ok(elems)
}

// ===========================================================================
// regexp_split_to_table / regexp_split_to_array.
// ===========================================================================

/// C: `regexp_split_to_table` — split the string at matches of the pattern,
/// returning the split-out substrings as a table.
///
/// Materialize-mode equivalent of the C SRF (see [`regexp_matches`]).
pub fn regexp_split_to_table<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    // Determine options
    let mut re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_split_to_table()"));
    }
    // But we find all the matches anyway
    re_flags.glob = true;

    let mut splitctx =
        setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, false, true, true)?;

    let mut rows = vec_with_capacity_in(mcx, splitctx.nmatches.max(0) as usize + 1)?;
    while splitctx.next_match <= splitctx.nmatches {
        rows.push(build_regexp_split_result(&mut splitctx)?);
        splitctx.next_match += 1;
    }
    Ok(rows)
}

/// C: `regexp_split_to_table_no_flags` — separate for opr_sanity.
pub fn regexp_split_to_table_no_flags<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    regexp_split_to_table(mcx, orig_str, pattern, None, collation)
}

/// C: `regexp_split_to_array` — split the string at matches of the pattern,
/// returning the split-out substrings as an array (here: the element list;
/// the `accumArrayResult`/`makeArrayResult` array construction is the fmgr
/// layer's).
pub fn regexp_split_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    // Determine options
    let mut re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_split_to_array()"));
    }
    // But we find all the matches anyway
    re_flags.glob = true;

    let mut splitctx =
        setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, false, true, true)?;

    let mut elems = vec_with_capacity_in(mcx, splitctx.nmatches.max(0) as usize + 1)?;
    while splitctx.next_match <= splitctx.nmatches {
        elems.push(build_regexp_split_result(&mut splitctx)?);
        splitctx.next_match += 1;
    }
    Ok(elems)
}

/// C: `regexp_split_to_array_no_flags` — separate for opr_sanity.
pub fn regexp_split_to_array_no_flags<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    regexp_split_to_array(mcx, orig_str, pattern, None, collation)
}

/// C: `build_regexp_split_result` — build the output string for the current
/// match: the string between the current match and the previous one, or the
/// string after the last match when `next_match == nmatches`.
pub fn build_regexp_split_result<'mcx>(
    splitctx: &mut RegexpMatchesCtx<'_, 'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let startpos = if splitctx.next_match > 0 {
        splitctx.match_locs[(splitctx.next_match * 2 - 1) as usize]
    } else {
        0
    };
    if startpos < 0 {
        return Err(PgError::error("invalid match ending position"));
    }

    let endpos = splitctx.match_locs[(splitctx.next_match * 2) as usize];
    if endpos < startpos {
        return Err(PgError::error("invalid match starting position"));
    }

    if let Some(wide) = &splitctx.wide_str {
        mb::pg_wchar2mb_with_len::call(
            splitctx.mcx,
            &wide[startpos as usize..endpos as usize],
        )
    } else {
        varlena_seams::text_substr::call(
            splitctx.mcx,
            splitctx.orig_str,
            startpos + 1,
            endpos - startpos,
        )
    }
}

// ===========================================================================
// regexp_substr.
// ===========================================================================

/// C: `regexp_substr` — the substring that matches a regular expression
/// pattern. `None` is SQL NULL.
pub fn regexp_substr<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    pattern: &[u8],
    start: Option<i32>,
    n: Option<i32>,
    flags: Option<&[u8]>,
    subexpr: Option<i32>,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Collect optional parameters
    let start = check_start(start)?;
    let n = match n {
        Some(n) => {
            if n <= 0 {
                return Err(invalid_param("n", n));
            }
            n
        }
        None => 1,
    };
    let subexpr = match subexpr {
        Some(subexpr) => {
            if subexpr < 0 {
                return Err(invalid_param("subexpr", subexpr));
            }
            subexpr
        }
        None => 0,
    };

    // Determine options
    let mut re_flags = parse_re_flags(flags)?;
    // User mustn't specify 'g'
    if re_flags.glob {
        return Err(global_unsupported("regexp_substr()"));
    }
    // But we find all the matches anyway
    re_flags.glob = true;

    // Do the matching
    let matchctx = setup_regexp_matches(
        mcx,
        str,
        pattern,
        &re_flags,
        start - 1,
        collation,
        subexpr > 0, // need submatches?
        false,
        false,
    )?;

    // When n exceeds matches return NULL (includes case of no matches)
    if n > matchctx.nmatches {
        return Ok(None);
    }

    // When subexpr exceeds number of subexpressions return NULL
    if subexpr > matchctx.npatterns {
        return Ok(None);
    }

    // Select the appropriate match position to return
    let mut pos = (n - 1) * matchctx.npatterns;
    if subexpr > 0 {
        pos += subexpr - 1;
    }
    pos *= 2;
    let so = matchctx.match_locs[pos as usize];
    let eo = matchctx.match_locs[(pos + 1) as usize];

    if so < 0 || eo < 0 {
        return Ok(None); // unidentifiable location
    }

    Ok(Some(varlena_seams::text_substr::call(mcx, str, so + 1, eo - so)?))
}

/// C: `regexp_substr_no_start` — separate for opr_sanity.
pub fn regexp_substr_no_start<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    pattern: &[u8],
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    regexp_substr(mcx, str, pattern, None, None, None, None, collation)
}

/// C: `regexp_substr_no_n` — separate for opr_sanity.
pub fn regexp_substr_no_n<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    regexp_substr(mcx, str, pattern, Some(start), None, None, None, collation)
}

/// C: `regexp_substr_no_flags` — separate for opr_sanity.
pub fn regexp_substr_no_flags<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    n: i32,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    regexp_substr(mcx, str, pattern, Some(start), Some(n), None, None, collation)
}

/// C: `regexp_substr_no_subexpr` — separate for opr_sanity.
pub fn regexp_substr_no_subexpr<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    pattern: &[u8],
    start: i32,
    n: i32,
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    regexp_substr(mcx, str, pattern, Some(start), Some(n), flags, None, collation)
}

// ===========================================================================
// regexp_fixed_prefix.
// ===========================================================================

/// C: `regexp_fixed_prefix` — extract the fixed prefix, if any, for a
/// regexp. The result is `None` if there is no fixed prefix, else the prefix
/// (database encoding, allocated in `mcx`) plus the `*exact` flag: true for
/// an exact match, not just a prefix.
pub fn regexp_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    text_re: &[u8],
    case_insensitive: bool,
    collation: Oid,
) -> PgResult<Option<(PgVec<'mcx, u8>, bool)>> {
    // Compile RE
    let mut cflags = REG_ADVANCED;
    if case_insensitive {
        cflags |= REG_ICASE;
    }

    let re = RE_compile_and_cache(mcx, text_re, cflags | REG_NOSUB, collation)?;

    // Examine it to see if there's a fixed prefix
    let (str, exact) = match engine::pg_regprefix::call(mcx, &re)? {
        RegprefixResult::NoMatch => return Ok(None),
        RegprefixResult::Prefix(str) => (str, false),
        RegprefixResult::Exact(str) => (str, true),
        RegprefixResult::Failed(f) => {
            // re failed???
            return Err(PgError::error(format!("regular expression failed: {}", f.message))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION));
        }
    };

    // Convert pg_wchar result back to database encoding
    let result = mb::pg_wchar2mb_with_len::call(mcx, &str)?;
    drop(str); // C: pfree(str)

    Ok(Some((result, exact)))
}

// ===========================================================================
// small shared helpers
// ===========================================================================

/// The shared `start <= 0` validation of `regexp_count` / `regexp_instr` /
/// `regexp_substr` (C: inline at each `PG_NARGS() > 2` block).
fn check_start(start: Option<i32>) -> PgResult<i32> {
    match start {
        Some(start) => {
            if start <= 0 {
                Err(invalid_param("start", start))
            } else {
                Ok(start)
            }
        }
        None => Ok(1),
    }
}

/// C: `ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
/// errmsg("invalid value for parameter \"%s\": %d", name, value)))`.
fn invalid_param(name: &str, value: i32) -> PgError {
    PgError::error(format!("invalid value for parameter \"{name}\": {value}"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// C: `ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
/// errmsg("%s does not support the \"global\" option", fnname)))`.
fn global_unsupported(fnname: &str) -> PgError {
    PgError::error(format!("{fnname} does not support the \"global\" option"))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// ===========================================================================
// seam installation
// ===========================================================================

/// Install this crate's implementations into `backend-utils-adt-regexp-seams`.
pub fn init_seams() {
    backend_utils_adt_regexp_seams::RE_compile_and_cache::set(RE_compile_and_cache);
    backend_utils_adt_regexp_seams::RE_compile_and_execute::set(RE_compile_and_execute);
    backend_utils_adt_regexp_seams::regexp_fixed_prefix::set(regexp_fixed_prefix);
}

#[cfg(test)]
mod tests;
