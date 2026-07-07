//! regex_engine A/B experiment (regex-engine-ab branch, MEASUREMENT ONLY):
//! alternate regexp engines for replace_text_regexp, selected by the
//! `regex_engine` GUC. The default (spencer) never reaches this crate; the
//! non-default engines are allowed to diverge semantically from the Spencer
//! ARE port. Known deltas, not bridged:
//! - leftmost-first (Perl) match preference instead of Spencer's
//!   preference rules; agrees on all-greedy patterns like Q29's;
//! - no backrefs/lookaround (compile error names the engine);
//! - collation-blind: Unicode ctype regardless of collation (e.g. \w under
//!   the C collation is ASCII-only in Spencer, Unicode-wide here);
//! - REG_NLSTOP ('n'/'p' flags) maps only onto `.` (dot_nl), not onto
//!   negated bracket classes; REG_EXPANDED unsupported on re2;
//! - only ARE ('advanced') and quoted ('q') modes; 'b'/'e' modes refuse.

use core::cell::RefCell;

use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use ::regex_spencer::{
    REG_ADVANCED, REG_EXPANDED, REG_ICASE, REG_NLANCH, REG_NLSTOP, REG_QUOTE,
};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};

pub use guc_tables::consts::{REGEX_ENGINE_RE2, REGEX_ENGINE_RUST, REGEX_ENGINE_SPENCER};

guc_tables::session_guc_cluster!(RegexpAltGucs, REGEXP_ALT_GUCS:
    (regex_engine_cell, i32, regex_engine, set_regex_engine, guc_tables::consts::REGEX_ENGINE_SPENCER),
);

pub fn install() {
    guc_tables::vars::regex_engine.install_if_absent(guc_tables::GucVarAccessors {
        get: regex_engine,
        set: set_regex_engine,
    });
}

fn engine_name(engine: i32) -> &'static str {
    match engine {
        REGEX_ENGINE_RE2 => "re2",
        REGEX_ENGINE_RUST => "rust",
        _ => "spencer",
    }
}

#[cold]
#[inline(never)]
fn engine_error(engine: i32, message: &str) -> PgError {
    PgError::error(format!("regex_engine={}: {message}", engine_name(engine)))
        .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
}

// Group 0 = whole match; -1/-1 = did not participate. Byte offsets.
const MAX_GROUPS: usize = 10;
type Groups = [(i64, i64); MAX_GROUPS];

#[derive(Clone)]
enum AltRe {
    Rust(regex_rs::bytes::Regex),
    #[allow(dead_code)]
    Re2(std::rc::Rc<re2::Re2Re>),
}

impl AltRe {
    // want_groups includes group 0. Returns the number of groups filled.
    fn match_at(
        &self,
        hay: &[u8],
        start: usize,
        want_groups: usize,
        out: &mut Groups,
    ) -> PgResult<Option<usize>> {
        match self {
            AltRe::Rust(re) => {
                let mut locs = re.capture_locations();
                let n = want_groups.min(locs.len());
                match re.captures_read_at(&mut locs, hay, start) {
                    None => Ok(None),
                    Some(_) => {
                        for i in 0..n {
                            out[i] = match locs.get(i) {
                                Some((s, e)) => (s as i64, e as i64),
                                None => (-1, -1),
                            };
                        }
                        Ok(Some(n))
                    }
                }
            }
            #[cfg(have_re2)]
            AltRe::Re2(re) => re.match_at(hay, start, want_groups, out),
            #[cfg(not(have_re2))]
            AltRe::Re2(_) => unreachable!("re2 handle without have_re2"),
        }
    }
}

#[cfg(have_re2)]
mod re2 {
    use super::{engine_error, Groups, PgResult, REGEX_ENGINE_RE2};
    use core::ffi::{c_char, c_int, c_longlong, c_void};

    extern "C" {
        fn pgr_re2_compile(
            pat: *const c_char,
            len: c_int,
            literal: c_int,
            errbuf: *mut c_char,
            errbuf_len: c_int,
        ) -> *mut c_void;
        fn pgr_re2_free(re: *mut c_void);
        fn pgr_re2_ngroups(re: *mut c_void) -> c_int;
        fn pgr_re2_match(
            re: *mut c_void,
            text: *const c_char,
            len: c_int,
            startpos: c_int,
            ngroups: c_int,
            groups: *mut c_longlong,
        ) -> c_int;
    }

    pub struct Re2Re {
        ptr: *mut c_void,
        ngroups: usize,
    }

    impl Drop for Re2Re {
        fn drop(&mut self) {
            unsafe { pgr_re2_free(self.ptr) };
        }
    }

    pub fn compile(pattern: &[u8]) -> PgResult<Re2Re> {
        let mut errbuf = [0u8; 256];
        let ptr = unsafe {
            pgr_re2_compile(
                pattern.as_ptr().cast(),
                pattern.len() as c_int,
                0,
                errbuf.as_mut_ptr().cast(),
                errbuf.len() as c_int,
            )
        };
        if ptr.is_null() {
            let end = errbuf.iter().position(|&b| b == 0).unwrap_or(errbuf.len());
            let msg = String::from_utf8_lossy(&errbuf[..end]).into_owned();
            return Err(engine_error(
                REGEX_ENGINE_RE2,
                &format!("invalid regular expression: {msg}"),
            )
            .into());
        }
        let ngroups = unsafe { pgr_re2_ngroups(ptr) } as usize;
        Ok(Re2Re { ptr, ngroups })
    }

    impl Re2Re {
        pub fn match_at(
            &self,
            hay: &[u8],
            start: usize,
            want_groups: usize,
            out: &mut Groups,
        ) -> PgResult<Option<usize>> {
            let n = want_groups.min(self.ngroups + 1).max(1);
            let mut raw = [0i64; 2 * super::MAX_GROUPS];
            let matched = unsafe {
                pgr_re2_match(
                    self.ptr,
                    hay.as_ptr().cast(),
                    hay.len() as c_int,
                    start as c_int,
                    n as c_int,
                    raw.as_mut_ptr(),
                )
            };
            if matched == 0 {
                return Ok(None);
            }
            for i in 0..n {
                out[i] = (raw[2 * i], raw[2 * i + 1]);
            }
            Ok(Some(n))
        }
    }
}

#[cfg(not(have_re2))]
mod re2 {
    pub struct Re2Re;
}

fn compile_engine(pattern: &[u8], cflags: i32, engine: i32) -> PgResult<AltRe> {
    let quoted = cflags & REG_QUOTE != 0;
    if !quoted && (cflags & REG_ADVANCED) != REG_ADVANCED {
        return Err(engine_error(
            engine,
            "only advanced ('advanced'/ARE) or literal ('q') patterns are supported",
        )
        .into());
    }
    let pat_str = core::str::from_utf8(pattern)
        .map_err(|_| engine_error(engine, "pattern is not valid UTF-8"))?;
    let pat_owned;
    let pat_str = if quoted {
        pat_owned = regex_rs::escape(pat_str);
        &pat_owned
    } else {
        pat_str
    };

    match engine {
        REGEX_ENGINE_RUST => {
            let re = regex_rs::bytes::RegexBuilder::new(pat_str)
                .case_insensitive(cflags & REG_ICASE != 0)
                .dot_matches_new_line(cflags & REG_NLSTOP == 0)
                .multi_line(cflags & REG_NLANCH != 0)
                .ignore_whitespace(!quoted && cflags & REG_EXPANDED != 0)
                .unicode(true)
                .build()
                .map_err(|e| {
                    engine_error(engine, &format!("invalid regular expression: {e}")).with_hint(
                        "The rust engine does not support backreferences or lookaround.",
                    )
                })?;
            Ok(AltRe::Rust(re))
        }
        REGEX_ENGINE_RE2 => {
            #[cfg(have_re2)]
            {
                if cflags & REG_EXPANDED != 0 {
                    return Err(engine_error(
                        engine,
                        "the expanded ('x') flag is not supported",
                    )
                    .into());
                }
                let mut full = String::new();
                if cflags & REG_ICASE != 0 {
                    full.push_str("(?i)");
                }
                if cflags & REG_NLSTOP == 0 {
                    full.push_str("(?s)");
                }
                if cflags & REG_NLANCH != 0 {
                    full.push_str("(?m)");
                }
                full.push_str(pat_str);
                let re = re2::compile(full.as_bytes())?;
                Ok(AltRe::Re2(std::rc::Rc::new(re)))
            }
            #[cfg(not(have_re2))]
            Err(engine_error(
                engine,
                "engine not built in (libre2 development files were absent at compile time)",
            )
            .into())
        }
        _ => Err(engine_error(engine, "unknown regex engine").into()),
    }
}

const MAX_CACHED: usize = 32;

struct CachedAlt {
    pat: Vec<u8>,
    cflags: i32,
    engine: i32,
    re: AltRe,
}

thread_local! {
    static ALT_CACHE: RefCell<Vec<CachedAlt>> = const { RefCell::new(Vec::new()) };
}

// Keyed (pattern, cflags, engine); collation-blind by engine semantics.
fn compile_and_cache(pattern: &[u8], cflags: i32, engine: i32) -> PgResult<AltRe> {
    let hit = ALT_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        let i = cache.iter().position(|e| {
            e.engine == engine && e.cflags == cflags && e.pat.as_slice() == pattern
        })?;
        if i > 0 {
            let entry = cache.remove(i);
            cache.insert(0, entry);
        }
        Some(cache[0].re.clone())
    });
    if let Some(re) = hit {
        return Ok(re);
    }
    let re = compile_engine(pattern, cflags, engine)?;
    ALT_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        if cache.len() >= MAX_CACHED {
            cache.pop();
        }
        cache.insert(
            0,
            CachedAlt { pat: pattern.to_vec(), cflags, engine, re: re.clone() },
        );
    });
    Ok(re)
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

// Byte-offset analogue of varlena's append_regexp_substr: PG replacement
// escapes (\1..\9, \&, \\; unknown escapes keep the backslash).
fn append_replacement(
    buf: &mut PgVec<'_, u8>,
    replace_text: &[u8],
    groups: &Groups,
    ngroups: usize,
    src: &[u8],
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
        let c = replace_text[p];
        let (so, eo) = if (b'1'..=b'9').contains(&c) {
            let idx = (c - b'0') as usize;
            p += 1;
            if idx < ngroups {
                groups[idx]
            } else {
                (-1, -1)
            }
        } else if c == b'&' {
            p += 1;
            groups[0]
        } else if c == b'\\' {
            buf.push(b'\\');
            p += 1;
            continue;
        } else {
            buf.push(b'\\');
            continue;
        };
        if so >= 0 && eo >= 0 {
            vec_append_bytes(buf, &src[so as usize..eo as usize])?;
        }
    }
    Ok(())
}

fn advance_one_char(src: &[u8], pos: usize) -> usize {
    if pos >= src.len() {
        pos + 1
    } else {
        pos + mbutils::pg_mblen(&src[pos..]).max(1) as usize
    }
}

fn char_off_to_byte(src: &[u8], nchars: i32) -> usize {
    if mbutils::pg_database_encoding_max_length() == 1 {
        (nchars as usize).min(src.len())
    } else {
        let mut off = 0usize;
        let mut remaining = nchars;
        while remaining > 0 && off < src.len() {
            off += mbutils::pg_mblen(&src[off..]).max(1) as usize;
            remaining -= 1;
        }
        off
    }
}

// replace_text_regexp with the same n/search_start semantics as the Spencer
// path, driven by byte offsets. search_start is a CHARACTER offset (the SQL
// start parameter minus one). Payload in, payload out.
#[allow(clippy::too_many_arguments)]
pub fn replace_text_regexp_alt<'mcx>(
    mcx: Mcx<'mcx>,
    src_text: &[u8],
    pattern_text: &[u8],
    replace_text: &[u8],
    cflags: i32,
    _collation: ::types_core::Oid,
    search_start: i32,
    n: i32,
    engine: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let re = compile_and_cache(pattern_text, cflags, engine)?;

    let escape_status = check_replace_text_has_escape(replace_text);
    let want_groups = if escape_status < 2 { 1 } else { MAX_GROUPS };

    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, src_text.len())?;
    let mut groups: Groups = [(-1, -1); MAX_GROUPS];
    let mut nmatches: i32 = 0;
    let mut search_pos = char_off_to_byte(src_text, search_start);
    let mut copied = 0usize;

    while search_pos <= src_text.len() {
        postgres_seams::check_for_interrupts::call()?;

        let Some(ngroups) = re.match_at(src_text, search_pos, want_groups, &mut groups)? else {
            break;
        };
        let (m_so, m_eo) = (groups[0].0 as usize, groups[0].1 as usize);

        nmatches += 1;
        if n > 0 && nmatches != n {
            search_pos = m_eo;
            if m_so == m_eo {
                search_pos = advance_one_char(src_text, search_pos);
            }
            continue;
        }

        if m_so > copied {
            vec_append_bytes(&mut buf, &src_text[copied..m_so])?;
        }
        if escape_status > 0 {
            append_replacement(&mut buf, replace_text, &groups, ngroups, src_text)?;
        } else {
            vec_append_bytes(&mut buf, replace_text)?;
        }
        copied = m_eo;

        if n > 0 {
            break;
        }
        search_pos = m_eo;
        if m_so == m_eo {
            search_pos = advance_one_char(src_text, search_pos);
        }
    }

    if copied < src_text.len() {
        vec_append_bytes(&mut buf, &src_text[copied..])?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests;
