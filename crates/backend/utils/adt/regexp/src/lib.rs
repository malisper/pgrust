#![allow(non_snake_case)]

use core::cell::RefCell;
use core::mem::ManuallyDrop;

use ::mcx::{slice_in, Mcx, MemoryContext, PgVec};
use ::regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexecResult, RegprefixResult, REG_ADVANCED,
    REG_ICASE, REG_NOSUB,
};
use ::types_core::{Oid, PgWChar};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};
use regex_core::regex_export_free_error as engine;

pub mod builtins;

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
                Box::leak(Box::new(MemoryContext::new("RegexpCacheMemoryContext"))).mcx();
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

fn RE_wchar_execute(
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
