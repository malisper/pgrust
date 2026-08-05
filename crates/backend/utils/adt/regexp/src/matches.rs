use ::mcx::{slice_in, vec_with_capacity_in, Mcx, PgVec, MAX_ALLOC_SIZE};
use ::regex::{RegMatch, REG_NOSUB};
use ::types_core::{Oid, PgWChar};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

use crate::{
    check_start, global_unsupported, invalid_param, parse_re_flags, PgReFlags,
    RE_compile_and_cache, RE_wchar_execute,
};

pub struct RegexpMatchesCtx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    pub nmatches: i32,
    pub npatterns: i32,
    match_locs: PgVec<'mcx, i32>,
    pub next_match: i32,
    wide_str: Option<PgVec<'mcx, PgWChar>>,
    // RE2 path: match_locs holds byte offsets (wide_str stays None; byte
    // slicing is then exactly right for fetch_chars, but 1-based character
    // positions need conversion — see one_based_position).
    byte_offsets: bool,
}

impl RegexpMatchesCtx<'_, '_> {
    // Converts a match_locs offset to the 1-based character position the
    // regexp_instr contract requires.
    fn one_based_position(&self, loc: i32) -> PgResult<i32> {
        if !self.byte_offsets || mbutils::pg_database_encoding_max_length() == 1 {
            return Ok(loc + 1);
        }
        Ok(mbutils::pg_mbstrlen_with_len(&self.orig_str[..loc as usize])? + 1)
    }
}

// C: setup_regexp_matches — all the matching in one swoop; match_locs holds
// nmatches*npatterns*2 char indexes plus a trailing end-of-string position.
#[allow(clippy::too_many_arguments)]
pub fn setup_regexp_matches<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    pattern: &[u8],
    re_flags: &PgReFlags,
    mut start_search: i32,
    collation: Oid,
    mut use_subpatterns: bool,
    ignore_degenerate: bool,
) -> PgResult<RegexpMatchesCtx<'a, 'mcx>> {
    // regex_engine dispatch: RE2-compatible patterns take the byte-offset
    // path; everything else runs the untouched Spencer path below. Callers
    // consuming submatches need the capture-safe tier; whole-match callers
    // (count, split, instr/substr subexpr 0) ride RE2 either way.
    if let Some(re) = regexp_alt::dispatch(pattern, re_flags.cflags, orig_str)?
        .filter(|re| re.capture_safe() || !use_subpatterns || re.ngroups() == 0)
    {
        return setup_regexp_matches_re2(
            mcx,
            orig_str,
            &re,
            re_flags,
            start_search,
            use_subpatterns,
            ignore_degenerate,
        );
    }

    let eml = mbutils::pg_database_encoding_max_length();

    let wide_str = mbutils::pg_mb2wchar_with_len(mcx, orig_str)?;
    let wide_len = wide_str.len() as i32;

    let mut cflags = re_flags.cflags;
    if !use_subpatterns {
        cflags |= REG_NOSUB;
    }
    let cpattern = RE_compile_and_cache(mcx, pattern, cflags, collation)?;

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

    let mut pmatch: PgVec<'_, RegMatch> = vec_with_capacity_in(mcx, pmatch_len)?;
    pmatch.resize(pmatch_len, RegMatch::UNSET);

    // C: 2^n-1 sizes so the limit trips at 2^28-1, not 2^27.
    let mut array_len: i32 = if re_flags.glob { 255 } else { 31 };
    let mut match_locs: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, array_len as usize)?;
    match_locs.resize(array_len as usize, 0);
    let mut array_idx: usize = 0;
    let mut nmatches: i32 = 0;

    let mut prev_match_end: i64 = 0;
    while RE_wchar_execute(&cpattern, &wide_str, start_search, &mut pmatch)? {
        if !ignore_degenerate
            || (pmatch[0].rm_so < wide_len as i64 && pmatch[0].rm_eo > prev_match_end)
        {
            while array_idx + (npatterns as usize) * 2 + 1 > array_len as usize {
                array_len += array_len + 1;
                if array_len as usize > MAX_ALLOC_SIZE / core::mem::size_of::<i32>() {
                    return Err(too_many_matches().into());
                }
                let extra = array_len as usize - match_locs.len();
                match_locs
                    .try_reserve(extra)
                    .map_err(|_| mcx.oom(array_len as usize * core::mem::size_of::<i32>()))?;
                match_locs.resize(array_len as usize, 0);
            }

            if use_subpatterns {
                for i in 1..=npatterns as usize {
                    match_locs[array_idx] = pmatch[i].rm_so as i32;
                    match_locs[array_idx + 1] = pmatch[i].rm_eo as i32;
                    array_idx += 2;
                }
            } else {
                match_locs[array_idx] = pmatch[0].rm_so as i32;
                match_locs[array_idx + 1] = pmatch[0].rm_eo as i32;
                array_idx += 2;
            }
            nmatches += 1;
        }
        prev_match_end = pmatch[0].rm_eo;

        if !re_flags.glob {
            break;
        }

        start_search = prev_match_end as i32;
        if pmatch[0].rm_so == pmatch[0].rm_eo {
            start_search += 1;
        }
        if start_search > wide_len {
            break;
        }
    }

    match_locs[array_idx] = wide_len;

    let wide_str = if eml > 1 { Some(wide_str) } else { None };

    Ok(RegexpMatchesCtx {
        mcx,
        orig_str,
        nmatches,
        npatterns,
        match_locs,
        next_match: 0,
        wide_str,
        byte_offsets: false,
    })
}

// The RE2 arm of setup_regexp_matches: identical control flow driven by byte
// offsets; match_locs holds byte offsets and the trailing sentinel is the
// byte length.
fn setup_regexp_matches_re2<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    re: &regexp_alt::Re2Pattern,
    re_flags: &PgReFlags,
    start_search: i32,
    mut use_subpatterns: bool,
    ignore_degenerate: bool,
) -> PgResult<RegexpMatchesCtx<'a, 'mcx>> {
    let len = orig_str.len();

    let npatterns: i32;
    let ngroups: usize;
    if use_subpatterns && re.ngroups() > 0 {
        npatterns = re.ngroups() as i32;
        ngroups = re.ngroups() + 1;
    } else {
        use_subpatterns = false;
        npatterns = 1;
        ngroups = 1;
    }

    let mut groups: PgVec<'_, (i64, i64)> = vec_with_capacity_in(mcx, ngroups)?;
    groups.resize(ngroups, (-1, -1));

    let mut array_len: i32 = if re_flags.glob { 255 } else { 31 };
    let mut match_locs: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, array_len as usize)?;
    match_locs.resize(array_len as usize, 0);
    let mut array_idx: usize = 0;
    let mut nmatches: i32 = 0;

    let mut search_pos = regexp_alt::char_off_to_byte(orig_str, start_search);
    let mut prev_match_end: i64 = 0;
    while search_pos <= len {
        postgres_seams::check_for_interrupts::call()?;
        if !re.exec(orig_str, search_pos, &mut groups) {
            break;
        }
        if !ignore_degenerate || (groups[0].0 < len as i64 && groups[0].1 > prev_match_end) {
            while array_idx + (npatterns as usize) * 2 + 1 > array_len as usize {
                array_len += array_len + 1;
                if array_len as usize > MAX_ALLOC_SIZE / core::mem::size_of::<i32>() {
                    return Err(too_many_matches().into());
                }
                let extra = array_len as usize - match_locs.len();
                match_locs
                    .try_reserve(extra)
                    .map_err(|_| mcx.oom(array_len as usize * core::mem::size_of::<i32>()))?;
                match_locs.resize(array_len as usize, 0);
            }

            if use_subpatterns {
                for g in &groups[1..] {
                    match_locs[array_idx] = g.0 as i32;
                    match_locs[array_idx + 1] = g.1 as i32;
                    array_idx += 2;
                }
            } else {
                match_locs[array_idx] = groups[0].0 as i32;
                match_locs[array_idx + 1] = groups[0].1 as i32;
                array_idx += 2;
            }
            nmatches += 1;
        }
        prev_match_end = groups[0].1;

        if !re_flags.glob {
            break;
        }

        search_pos = prev_match_end as usize;
        if groups[0].0 == groups[0].1 {
            search_pos = regexp_alt::advance_one_char(orig_str, search_pos);
        }
    }

    match_locs[array_idx] = len as i32;

    Ok(RegexpMatchesCtx {
        mcx,
        orig_str,
        nmatches,
        npatterns,
        match_locs,
        next_match: 0,
        wide_str: None,
        byte_offsets: true,
    })
}

#[cold]
#[inline(never)]
fn too_many_matches() -> PgError {
    PgError::error("too many regular expression matches")
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

// C: single-byte path is DirectFunctionCall3(text_substr, str, so+1, eo-so);
// chars are bytes there, so the byte slice is the same text.
fn fetch_chars<'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &[u8],
    wide_str: &Option<PgVec<'mcx, PgWChar>>,
    so: i32,
    eo: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    match wide_str {
        Some(wide) => mbutils::pg_wchar2mb_with_len(mcx, &wide[so as usize..eo as usize]),
        None => slice_in(mcx, &orig_str[so as usize..eo as usize]),
    }
}

// C: build_regexp_match_result — fill the Datum/nulls workspace for the
// current match; the caller feeds it to construct_md_array.
pub fn build_regexp_match_result<'mcx>(
    matchctx: &RegexpMatchesCtx<'_, 'mcx>,
    mut push: impl FnMut(Option<PgVec<'mcx, u8>>) -> PgResult<()>,
) -> PgResult<()> {
    let mcx = matchctx.mcx;
    let mut loc = (matchctx.next_match * matchctx.npatterns * 2) as usize;
    for _ in 0..matchctx.npatterns {
        let so = matchctx.match_locs[loc];
        let eo = matchctx.match_locs[loc + 1];
        loc += 2;

        if so < 0 || eo < 0 {
            push(None)?;
        } else {
            push(Some(fetch_chars(mcx, matchctx.orig_str, &matchctx.wide_str, so, eo)?))?;
        }
    }
    Ok(())
}

pub fn build_regexp_split_result<'mcx>(
    splitctx: &RegexpMatchesCtx<'_, 'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let startpos = if splitctx.next_match > 0 {
        splitctx.match_locs[(splitctx.next_match * 2 - 1) as usize]
    } else {
        0
    };
    if startpos < 0 {
        return Err(PgError::error("invalid match ending position").into());
    }

    let endpos = splitctx.match_locs[(splitctx.next_match * 2) as usize];
    if endpos < startpos {
        return Err(PgError::error("invalid match starting position").into());
    }

    fetch_chars(splitctx.mcx, splitctx.orig_str, &splitctx.wide_str, startpos, endpos)
}

pub fn regexp_count(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    start: Option<i32>,
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<i32> {
    let start = check_start(start)?;

    let mut re_flags = parse_re_flags(flags)?;
    if re_flags.glob {
        return Err(global_unsupported("regexp_count()").into());
    }
    re_flags.glob = true;

    let matchctx =
        setup_regexp_matches(mcx, str, pattern, &re_flags, start - 1, collation, false, false)?;

    Ok(matchctx.nmatches)
}

#[allow(clippy::too_many_arguments)]
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
    let start = check_start(start)?;
    let n = match n {
        Some(n) if n <= 0 => return Err(invalid_param("n", n).into()),
        Some(n) => n,
        None => 1,
    };
    let endoption = match endoption {
        Some(e) if e != 0 && e != 1 => return Err(invalid_param("endoption", e).into()),
        Some(e) => e,
        None => 0,
    };
    let subexpr = match subexpr {
        Some(s) if s < 0 => return Err(invalid_param("subexpr", s).into()),
        Some(s) => s,
        None => 0,
    };

    let mut re_flags = parse_re_flags(flags)?;
    if re_flags.glob {
        return Err(global_unsupported("regexp_instr()").into());
    }
    re_flags.glob = true;

    let matchctx = setup_regexp_matches(
        mcx,
        str,
        pattern,
        &re_flags,
        start - 1,
        collation,
        subexpr > 0,
        false,
    )?;

    if n > matchctx.nmatches {
        return Ok(0);
    }
    if subexpr > matchctx.npatterns {
        return Ok(0);
    }

    let mut pos = (n - 1) * matchctx.npatterns;
    if subexpr > 0 {
        pos += subexpr - 1;
    }
    pos *= 2;
    if endoption == 1 {
        pos += 1;
    }

    if matchctx.match_locs[pos as usize] >= 0 {
        matchctx.one_based_position(matchctx.match_locs[pos as usize])
    } else {
        Ok(0)
    }
}

pub fn regexp_like(
    mcx: Mcx<'_>,
    str: &[u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<bool> {
    let re_flags = parse_re_flags(flags)?;
    if re_flags.glob {
        return Err(global_unsupported("regexp_like()").into());
    }

    crate::RE_compile_and_execute(mcx, pattern, str, re_flags.cflags, collation, &mut [])
}

pub fn regexp_match<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<Option<RegexpMatchesCtx<'a, 'mcx>>> {
    let re_flags = parse_re_flags(flags)?;
    if re_flags.glob {
        return Err(global_unsupported("regexp_match()")
            .with_hint("Use the regexp_matches function instead.")
            .into());
    }

    let matchctx =
        setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, true, false)?;

    if matchctx.nmatches == 0 {
        return Ok(None);
    }
    debug_assert_eq!(matchctx.nmatches, 1);

    Ok(Some(matchctx))
}

pub fn regexp_matches_setup<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
) -> PgResult<RegexpMatchesCtx<'a, 'mcx>> {
    let re_flags = parse_re_flags(flags)?;
    setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, true, false)
}

pub fn regexp_split_setup<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    orig_str: &'a [u8],
    pattern: &[u8],
    flags: Option<&[u8]>,
    collation: Oid,
    fname: &str,
) -> PgResult<RegexpMatchesCtx<'a, 'mcx>> {
    let mut re_flags = parse_re_flags(flags)?;
    if re_flags.glob {
        return Err(global_unsupported(fname).into());
    }
    re_flags.glob = true;
    setup_regexp_matches(mcx, orig_str, pattern, &re_flags, 0, collation, false, true)
}

#[allow(clippy::too_many_arguments)]
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
    let start = check_start(start)?;
    let n = match n {
        Some(n) if n <= 0 => return Err(invalid_param("n", n).into()),
        Some(n) => n,
        None => 1,
    };
    let subexpr = match subexpr {
        Some(s) if s < 0 => return Err(invalid_param("subexpr", s).into()),
        Some(s) => s,
        None => 0,
    };

    let mut re_flags = parse_re_flags(flags)?;
    if re_flags.glob {
        return Err(global_unsupported("regexp_substr()").into());
    }
    re_flags.glob = true;

    let matchctx = setup_regexp_matches(
        mcx,
        str,
        pattern,
        &re_flags,
        start - 1,
        collation,
        subexpr > 0,
        false,
    )?;

    if n > matchctx.nmatches {
        return Ok(None);
    }
    if subexpr > matchctx.npatterns {
        return Ok(None);
    }

    let mut pos = (n - 1) * matchctx.npatterns;
    if subexpr > 0 {
        pos += subexpr - 1;
    }
    pos *= 2;
    let so = matchctx.match_locs[pos as usize];
    let eo = matchctx.match_locs[(pos + 1) as usize];

    if so < 0 || eo < 0 {
        return Ok(None);
    }

    fetch_chars(mcx, str, &matchctx.wide_str, so, eo).map(Some)
}
