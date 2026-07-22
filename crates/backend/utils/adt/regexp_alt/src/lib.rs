//! Product RE2 regexp engine and its compile-time dispatch.
//!
//! The `regex_engine` GUC selects: auto (default — patterns the FAIL-CLOSED
//! classifier proves compatible run on RE2 in POSIX longest-match mode,
//! everything else runs on the Spencer ARE port exactly as before), spencer
//! (force, escape hatch), re2 (force, testing; skips the classifier and maps
//! ICASE/NLSTOP/NLANCH onto inline groups with the documented deltas —
//! see docs/design/regex-engine-ab-verdict.md).
//!
//! Dispatch is decided once per (pattern, cflags, mode) and cached with the
//! compiled RE2 pattern (LRU, MAX 32). In auto mode an RE2 compile failure
//! also fails closed to Spencer and the Spencer verdict is cached, so error
//! surfaces are always Spencer's own.
//!
//! Dispatch is DATA-fail-closed as well as pattern-fail-closed: the Spencer
//! path views the subject through pg_mb2wchar_with_len (C parity — stops at
//! the first NUL, decodes invalid UTF-8 bytewise), while RE2 matches raw
//! bytes and never matches invalid UTF-8. Auto therefore re-routes every
//! evaluation whose subject contains NUL or is not valid UTF-8 to Spencer,
//! per subject, regardless of the pattern's cached verdict (forced re2 is
//! the testing knob and stays raw). Caught live by the regress encoding
//! suite: regexp_replace on 'café\0dcba' and 'caf\xc3\x00dcba'.

use core::cell::RefCell;
use std::rc::Rc;

use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
#[cfg_attr(not(have_re2), allow(unused_imports))]
use ::regex_spencer::{
    REG_ADVANCED, REG_EXPANDED, REG_ICASE, REG_NLANCH, REG_NLSTOP, REG_NOSUB, REG_QUOTE,
};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};

pub use guc_tables::consts::{REGEX_ENGINE_AUTO, REGEX_ENGINE_RE2, REGEX_ENGINE_SPENCER};

mod classify;
pub mod program;
pub use classify::{classify as classify_pattern, re2_compatible, Classification, Compat};

guc_tables::session_guc_cluster!(RegexpAltGucs, REGEXP_ALT_GUCS:
    (regex_engine_cell, i32, regex_engine, set_regex_engine, guc_tables::consts::REGEX_ENGINE_AUTO),
    // pgrust.regex_pattern_program: the anchored pattern-program fast tier
    // under the auto RE2 arm (program.rs). OFF = the exact pre-tier RE2
    // behavior; the toggle exists for the four-engine differential gate and
    // as an escape hatch, and is read per exec so flips act on cached
    // patterns immediately.
    (regex_pattern_program_cell, bool, regex_pattern_program, set_regex_pattern_program, true),
);

pub fn install() {
    guc_tables::vars::regex_engine.install_if_absent(guc_tables::GucVarAccessors {
        get: regex_engine,
        set: set_regex_engine,
    });
    guc_tables::vars::pgrust_regex_pattern_program.install_if_absent(
        guc_tables::GucVarAccessors {
            get: regex_pattern_program,
            set: set_regex_pattern_program,
        },
    );
    guc_tables::vars::pgrust_regex_re2_linked.install_if_absent(guc_tables::GucVarAccessors {
        get: re2_available,
        set: set_re2_linked_noop,
    });
}

pub fn re2_available() -> bool {
    cfg!(have_re2)
}

// pgrust.regex_re2_linked is a build property (PGC_INTERNAL preset): the
// getter is the cfg constant and writes have nothing to store.
fn set_re2_linked_noop(_: bool) {}

#[cold]
#[inline(never)]
fn re2_error(message: &str) -> PgError {
    PgError::error(format!("regex_engine=re2: {message}"))
        .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
}

// Group 0 = whole match; -1/-1 = did not participate. Byte offsets.
pub type GroupSpan = (i64, i64);

#[derive(Clone)]
pub struct Re2Pattern {
    // Debug elides the compiled automaton.
    #[allow(dead_code)]
    inner: Rc<re2::Re2Re>,
    capture_safe: bool,
    // The anchored pattern-program fast tier (program.rs): compiled once at
    // auto-dispatch time for CaptureSafe patterns inside the program subset,
    // consulted by exec for start-of-subject evaluations, RE2 otherwise.
    program: Option<Rc<program::Program>>,
}

impl core::fmt::Debug for Re2Pattern {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Re2Pattern")
    }
}

impl Re2Pattern {
    // Whether capture POSITIONS are proven to match Spencer's. Callers that
    // consume submatches (\N replacement escapes, regexp_match arrays,
    // subexpr arguments, substring(from)-group-1) must fall back to Spencer
    // when this is false; whole-match spans are safe regardless.
    pub fn capture_safe(&self) -> bool {
        self.capture_safe
    }

    // Whether the anchored pattern-program fast tier compiled for this
    // pattern (observability + tests).
    pub fn has_program(&self) -> bool {
        self.program.is_some()
    }

    // Capture group count, excluding group 0.
    pub fn ngroups(&self) -> usize {
        #[cfg(have_re2)]
        {
            self.inner.ngroups()
        }
        #[cfg(not(have_re2))]
        unreachable!("Re2Pattern constructed without have_re2")
    }

    // Fills out (group 0 first) and returns true on match. out may be empty
    // for a boolean match. start is a byte offset into hay.
    //
    // The pattern-program fast tier answers start-of-subject evaluations
    // when it was compiled for this pattern (anchored subset) and the GUC
    // keeps it on; a budget bail (pathological backtracking) or a nonzero
    // start falls through to the RE2 arm — the tier can refuse, never
    // answer differently.
    pub fn exec(&self, hay: &[u8], start: usize, out: &mut [GroupSpan]) -> bool {
        if start == 0 {
            if let Some(prog) = &self.program {
                if regex_pattern_program() {
                    if let Some(matched) = prog.exec(hay, out) {
                        return matched;
                    }
                }
            }
        }
        #[cfg(have_re2)]
        {
            self.inner.match_at(hay, start, out)
        }
        #[cfg(not(have_re2))]
        {
            let _ = (hay, start, out);
            unreachable!("Re2Pattern constructed without have_re2")
        }
    }

    pub fn is_match(&self, hay: &[u8], start: usize) -> bool {
        self.exec(hay, start, &mut [])
    }
}

#[cfg(have_re2)]
mod re2 {
    use super::{re2_error, GroupSpan, PgResult};
    use core::ffi::{c_char, c_int, c_longlong, c_void};

    extern "C" {
        fn pgr_re2_compile(
            pat: *const c_char,
            len: c_int,
            literal: c_int,
            longest: c_int,
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

    // longest selects POSIX leftmost-longest matching — needed only when
    // alternation exists; without `|` Perl first-match order is identical
    // and keeps RE2's faster capture paths.
    pub fn compile(pattern: &[u8], literal: bool, longest: bool) -> PgResult<Re2Re> {
        let mut errbuf = [0u8; 256];
        let ptr = unsafe {
            pgr_re2_compile(
                pattern.as_ptr().cast(),
                pattern.len() as c_int,
                literal as c_int,
                longest as c_int,
                errbuf.as_mut_ptr().cast(),
                errbuf.len() as c_int,
            )
        };
        if ptr.is_null() {
            let end = errbuf.iter().position(|&b| b == 0).unwrap_or(errbuf.len());
            let msg = String::from_utf8_lossy(&errbuf[..end]).into_owned();
            return Err(re2_error(&format!("invalid regular expression: {msg}")).into());
        }
        let ngroups = unsafe { pgr_re2_ngroups(ptr) } as usize;
        Ok(Re2Re { ptr, ngroups })
    }

    impl Re2Re {
        pub fn ngroups(&self) -> usize {
            self.ngroups
        }

        pub fn match_at(&self, hay: &[u8], start: usize, out: &mut [GroupSpan]) -> bool {
            let n = out.len().min(self.ngroups + 1);
            let mut stack = [0i64; 32];
            let mut heap: Vec<i64>;
            let raw: &mut [i64] = if 2 * n <= stack.len() {
                &mut stack
            } else {
                heap = vec![0i64; 2 * n];
                &mut heap
            };
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
                return false;
            }
            for (i, slot) in out.iter_mut().enumerate().take(n) {
                *slot = (raw[2 * i], raw[2 * i + 1]);
            }
            for slot in out.iter_mut().skip(n) {
                *slot = (-1, -1);
            }
            true
        }
    }
}

#[cfg(not(have_re2))]
mod re2 {
    pub struct Re2Re;
}

// Escapes every ASCII non-word byte; multibyte UTF-8 passes through. Used
// for forced-re2 quoted patterns that also carry inline flags.
#[cfg(have_re2)]
fn re2_escape(pattern: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pattern.len() * 2);
    for &b in pattern {
        if b.is_ascii() && !b.is_ascii_alphanumeric() && b != b'_' {
            out.push(b'\\');
        }
        out.push(b);
    }
    out
}

// The auto path: classifier-admitted patterns only, so the flag mapping is
// fixed — ARE with PG's newline-insensitive default ((?s)), or pure literal.
#[cfg(have_re2)]
fn compile_auto(
    pattern: &[u8],
    cflags: i32,
    capture_safe: bool,
    needs_longest: bool,
) -> PgResult<Re2Pattern> {
    let quoted = (cflags & !REG_NOSUB) == REG_QUOTE;
    if quoted {
        return Ok(Re2Pattern {
            inner: Rc::new(re2::compile(pattern, true, false)?),
            capture_safe,
            program: None,
        });
    }
    let mut full = Vec::with_capacity(pattern.len() + 4);
    full.extend_from_slice(b"(?s)");
    full.extend_from_slice(pattern);
    let inner = Rc::new(re2::compile(&full, false, needs_longest)?);
    // The pattern-program fast tier: only for CaptureSafe, alternation-free
    // (needs_longest=false, so the RE2 arm it mirrors is leftmost-first)
    // patterns inside program.rs's anchored subset. The group-count check is
    // belt-and-suspenders — the subset guarantees it.
    let program = if capture_safe && !needs_longest {
        program::compile(pattern)
            .filter(|p| p.ngroups() == inner.ngroups())
            .map(Rc::new)
    } else {
        None
    };
    Ok(Re2Pattern { inner, capture_safe, program })
}

// The forced path (regex_engine=re2): no classifier; ICASE/NLSTOP/NLANCH map
// onto inline groups with the documented deltas.
fn compile_forced(pattern: &[u8], cflags: i32) -> PgResult<Re2Pattern> {
    #[cfg(not(have_re2))]
    {
        let _ = (pattern, cflags);
        Err(re2_error(
            "engine not built in (libre2 development files were absent at compile time)",
        )
        .into())
    }
    #[cfg(have_re2)]
    {
        let quoted = cflags & REG_QUOTE != 0;
        if !quoted && (cflags & REG_ADVANCED) != REG_ADVANCED {
            return Err(re2_error(
                "only advanced ('advanced'/ARE) or literal ('q') patterns are supported",
            )
            .into());
        }
        if cflags & REG_EXPANDED != 0 && !quoted {
            return Err(re2_error("the expanded ('x') flag is not supported").into());
        }
        if core::str::from_utf8(pattern).is_err() {
            return Err(re2_error("pattern is not valid UTF-8").into());
        }
        if quoted && cflags & REG_ICASE == 0 {
            return Ok(Re2Pattern {
                inner: Rc::new(re2::compile(pattern, true, false)?),
                capture_safe: true,
                program: None,
            });
        }
        let mut full = Vec::with_capacity(pattern.len() + 12);
        if cflags & REG_ICASE != 0 {
            full.extend_from_slice(b"(?i)");
        }
        if cflags & REG_NLSTOP == 0 {
            full.extend_from_slice(b"(?s)");
        }
        if cflags & REG_NLANCH != 0 {
            full.extend_from_slice(b"(?m)");
        }
        if quoted {
            full.extend_from_slice(&re2_escape(pattern));
        } else {
            full.extend_from_slice(pattern);
        }
        // Forced mode is the testing knob: it exposes RE2 semantics whole,
        // capture handling included; longest mode matches the auto arm's
        // alternation semantics. No pattern program: forced re2 stays pure
        // RE2 so tests can differentiate the arms.
        Ok(Re2Pattern {
            inner: Rc::new(re2::compile(&full, false, true)?),
            capture_safe: true,
            program: None,
        })
    }
}

const MAX_CACHED: usize = 32;

struct CachedDispatch {
    pat: Vec<u8>,
    cflags: i32,
    forced: bool,
    // None = classified/failed to Spencer (auto mode only).
    verdict: Option<Re2Pattern>,
}

thread_local! {
    static DISPATCH_CACHE: RefCell<Vec<CachedDispatch>> = const { RefCell::new(Vec::new()) };
}

fn cache_get(pattern: &[u8], cflags: i32, forced: bool) -> Option<Option<Re2Pattern>> {
    DISPATCH_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        let i = cache.iter().position(|e| {
            e.cflags == cflags && e.forced == forced && e.pat.as_slice() == pattern
        })?;
        if i > 0 {
            let entry = cache.remove(i);
            cache.insert(0, entry);
        }
        Some(cache[0].verdict.clone())
    })
}

fn cache_put(pattern: &[u8], cflags: i32, forced: bool, verdict: Option<Re2Pattern>) {
    DISPATCH_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        if cache.len() >= MAX_CACHED {
            cache.pop();
        }
        cache.insert(0, CachedDispatch { pat: pattern.to_vec(), cflags, forced, verdict });
    });
}

// Whether RE2 and Spencer provably see the same subject. Spencer's
// pg_mb2wchar view stops at the first NUL and decodes invalid UTF-8
// bytewise; RE2's byte view diverges on both, so such subjects must run
// Spencer. Checked per evaluation, only after the pattern verdict says RE2
// (Spencer-class evaluations never pay the scan).
pub fn subject_compatible(subject: &[u8]) -> bool {
    !subject.contains(&0) && core::str::from_utf8(subject).is_ok()
}

// The single dispatch decision point. Some(re) = run the RE2 path; None =
// run the Spencer path untouched. The classification is cache-keyed on
// (pattern, cflags, mode); the classifier admits only collation-independent
// constructs, so collation does not participate in the key. The subject
// participates only in the per-evaluation data guard, never in the cache.
// Errors surface only under forced re2.
pub fn dispatch(pattern: &[u8], cflags: i32, subject: &[u8]) -> PgResult<Option<Re2Pattern>> {
    let engine = regex_engine();
    if engine == REGEX_ENGINE_SPENCER {
        return Ok(None);
    }
    if !re2_available() && engine != REGEX_ENGINE_RE2 {
        return Ok(None);
    }
    let forced = engine == REGEX_ENGINE_RE2;
    let verdict = match cache_get(pattern, cflags, forced) {
        Some(v) => v,
        None => {
            let v = if forced {
                Some(compile_forced(pattern, cflags)?)
            } else {
                match classify_pattern(pattern, cflags) {
                    Classification { tier: Compat::Incompatible, .. } => None,
                    #[cfg(have_re2)]
                    c => compile_auto(
                        pattern,
                        cflags,
                        c.tier == Compat::CaptureSafe,
                        c.needs_longest,
                    )
                    .ok(),
                    #[cfg(not(have_re2))]
                    _ => None,
                }
            };
            cache_put(pattern, cflags, forced, v.clone());
            v
        }
    };
    if verdict.is_some() && !forced && !subject_compatible(subject) {
        return Ok(None);
    }
    Ok(verdict)
}

// 0: no backslash escapes; 1: escapes but no \1..\9 submatch; 2: submatch.
pub fn check_replace_text_has_escape(replace_text: &[u8]) -> i32 {
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

const REPLACE_GROUPS: usize = 10;

// Byte-offset analogue of varlena's append_regexp_substr: PG replacement
// escapes (\1..\9, \&, \\; unknown escapes keep the backslash).
fn append_replacement(
    buf: &mut PgVec<'_, u8>,
    replace_text: &[u8],
    groups: &[GroupSpan],
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
            if idx < groups.len() {
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

pub fn advance_one_char(src: &[u8], pos: usize) -> usize {
    if pos >= src.len() {
        pos + 1
    } else {
        pos + mbutils::pg_mblen(&src[pos..]).max(1) as usize
    }
}

// Returns src.len() + 1 when nchars lies beyond the last character, so that
// `pos <= src.len()` loop guards skip matching entirely — mirroring the
// Spencer paths, where a start offset past the end never matches (not even
// the empty match at the end of the string).
pub fn char_off_to_byte(src: &[u8], nchars: i32) -> usize {
    if mbutils::pg_database_encoding_max_length() == 1 {
        nchars as usize
    } else {
        let mut off = 0usize;
        let mut remaining = nchars;
        while remaining > 0 && off < src.len() {
            off += mbutils::pg_mblen(&src[off..]).max(1) as usize;
            remaining -= 1;
        }
        if remaining > 0 {
            src.len() + 1
        } else {
            off
        }
    }
}

// replace_text_regexp with the same n/search_start semantics as the Spencer
// path, driven by byte offsets. search_start is a CHARACTER offset (the SQL
// start parameter minus one). Payload in, payload out.
pub fn replace_text_regexp_re2<'mcx>(
    mcx: Mcx<'mcx>,
    re: &Re2Pattern,
    src_text: &[u8],
    replace_text: &[u8],
    search_start: i32,
    n: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let escape_status = check_replace_text_has_escape(replace_text);
    let want_groups = if escape_status < 2 { 1 } else { REPLACE_GROUPS };

    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, src_text.len())?;
    let mut groups: [GroupSpan; REPLACE_GROUPS] = [(-1, -1); REPLACE_GROUPS];
    let mut nmatches: i32 = 0;
    let mut search_pos = char_off_to_byte(src_text, search_start);
    let mut copied = 0usize;

    while search_pos <= src_text.len() {
        postgres_seams::check_for_interrupts::call()?;

        if !re.exec(src_text, search_pos, &mut groups[..want_groups]) {
            break;
        }
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
            append_replacement(&mut buf, replace_text, &groups[..want_groups], src_text)?;
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
