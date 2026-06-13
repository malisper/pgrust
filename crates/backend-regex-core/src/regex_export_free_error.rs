//! Family: **regex-export-free-error** — `regexport.c` (the 11 `pg_reg_get*`
//! NFA/color exporters), `regfree.c` (`pg_regfree`), `regerror.c`
//! (`pg_regerror` message table), plus the opaque-[`RegexHandle`] seam adapter
//! and the per-backend `thread_local!` handle registry that the four inward
//! seams marshal through.
//!
//! # The opaque-handle boundary
//!
//! Across the public seam ([`backend_regex_core_seams`]) the compiled regex is
//! the opaque [`types_regex::RegexHandle`] token (see `types-regex` for why
//! that opacity is inherited from the engine's compile/exec split, not
//! introduced). The real owned [`RegexT`] lives in this crate; the registry
//! maps a handle to its `RegexT`. `pg_regcomp` registers and returns a handle;
//! `pg_regexec`/`pg_regprefix` look it up; `pg_regfree` removes it.
//!
//! The registry is **per-backend** (`thread_local!`): every backend compiles
//! its own regexes (the ADT cache is itself per-backend), so a shared static
//! would cross-link sessions — forbidden by AGENTS.md backend-global rules.

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::{slice_in, Mcx, MemoryContext};
use types_core::PgWChar;
use types_error::PgResult;
use types_regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexFailure, RegexHandle, RegexecResult,
    RegprefixResult,
};

use crate::regex_consts::{
    REG_ATOI, REG_BADBR, REG_BADOPT, REG_BADPAT, REG_BADRPT, REG_ASSERT, REG_ECOLLATE,
    REG_ECOLORS, REG_ECTYPE, REG_EBRACE, REG_EBRACK, REG_EESCAPE, REG_EPAREN, REG_ERANGE,
    REG_ESPACE, REG_ESUBREG, REG_ETOOBIG, REG_EXACT, REG_INVARG, REG_ITOA, REG_MIXED,
    REG_NOMATCH, REG_OKAY, REG_PREFIX, REMAGIC,
};
use crate::regguts::{chr, RegexT, COLORLESS, CHR_MIN, MAX_SIMPLE_CHR, PSEUDO};

// ===========================================================================
// per-backend handle registry
// ===========================================================================

thread_local! {
    /// Maps a live [`RegexHandle`] to its owned [`RegexT`]. Per-backend; see
    /// the module docs for why this is `thread_local!`, not a shared static.
    static REGEX_REGISTRY: RefCell<HashMap<u64, RegexT>> = RefCell::new(HashMap::new());
    /// Monotonic handle id allocator (C uses the `regex_t *` pointer identity;
    /// here a per-backend counter mints the opaque token).
    static NEXT_HANDLE: RefCell<u64> = const { RefCell::new(1) };
}

/// Register an owned [`RegexT`], returning the opaque handle the public seam
/// hands back to the ADT layer.
fn register(re: RegexT) -> RegexHandle {
    let id = NEXT_HANDLE.with(|n| {
        let mut n = n.borrow_mut();
        let id = *n;
        *n += 1;
        id
    });
    REGEX_REGISTRY.with(|reg| {
        reg.borrow_mut().insert(id, re);
    });
    RegexHandle(id)
}

/// Run `f` with a shared reference to the [`RegexT`] behind `handle` (the
/// exec/prefix/export read path). Returns the registry-miss case to the caller.
fn with_regex<R>(handle: RegexHandle, f: impl FnOnce(&RegexT) -> R) -> Option<R> {
    REGEX_REGISTRY.with(|reg| reg.borrow().get(&handle.0).map(f))
}

// ===========================================================================
// public seam adapters (installed by `crate::init_seams`)
// ===========================================================================

/// Adapter for the `pg_regcomp` inward seam: compile the pattern (compile
/// family), register the result, and return the handle as `RegcompResult`. The
/// non-OK `REG_*` code is mapped through [`pg_regerror`] into
/// [`RegexFailure`].
pub fn seam_pg_regcomp(
    pattern: &[PgWChar],
    cflags: i32,
    collation: types_core::Oid,
) -> PgResult<RegcompResult> {
    // C: `pg_regcomp` palloc's the compiled RE in the caller's current memory
    // context (regexp.c sets up a dedicated per-regexp context). The owned
    // `RegexT` does not borrow from this context, so a transient context that
    // is dropped here is sufficient to charge the compile-time allocations.
    let cx = MemoryContext::new("RegexpCompileContext");
    match crate::regex_compile::pg_regcomp(cx.mcx(), pattern, cflags, collation) {
        Ok(re) => {
            let re_nsub = re.re_nsub;
            let handle = register(re);
            Ok(RegcompResult::Compiled(RegexCompiled { handle, re_nsub }))
        }
        Err(e) => Ok(RegcompResult::Failed(RegexFailure {
            message: pg_regerror(e.code()),
        })),
    }
}

/// Adapter for the `pg_regexec` inward seam: look up `handle`, run the matcher
/// (exec family), fill `pmatch` in place, and map the result code.
pub fn seam_pg_regexec(
    handle: RegexHandle,
    data: &[PgWChar],
    search_start: i32,
    pmatch: &mut [RegMatch],
) -> PgResult<RegexecResult> {
    // C: `pg_regexec(re, string, len, search_start, NULL, nmatch, pmatch, 0)`.
    // The matcher palloc's its DFA tables / per-call vars in the caller's
    // current context; use a transient context (the filled `pmatch` slots and
    // the result code do not borrow it).
    let cx = MemoryContext::new("RegexpExecContext");

    // `with_regex` returns None on a registry miss (a freed/never-registered
    // handle). C dereferences the `regex_t *` directly; a miss here is a
    // can't-happen, mirrored as REG_ASSERT.
    let res = with_regex(handle, |re| {
        let guts = re
            .re_guts
            .as_ref()
            .expect("pg_regexec: compiled regex has no guts");
        crate::regex_exec::pg_regexec(cx.mcx(), guts, data, search_start, pmatch, 0)
    });

    match res {
        Some(Ok(true)) => Ok(RegexecResult::Matched),
        Some(Ok(false)) => Ok(RegexecResult::NoMatch),
        Some(Err(e)) if e.code() == REG_NOMATCH => Ok(RegexecResult::NoMatch),
        Some(Err(e)) => Ok(RegexecResult::Failed(RegexFailure {
            message: pg_regerror(e.code()),
        })),
        None => Ok(RegexecResult::Failed(RegexFailure {
            message: pg_regerror(REG_ASSERT),
        })),
    }
}

/// Adapter for the `pg_regprefix` inward seam: look up `handle`, run the prefix
/// extractor (exec family), and copy the prefix into `mcx`.
pub fn seam_pg_regprefix<'mcx>(
    mcx: Mcx<'mcx>,
    handle: RegexHandle,
) -> PgResult<RegprefixResult<'mcx>> {
    let res = with_regex(handle, |re| {
        let guts = re
            .re_guts
            .as_ref()
            .expect("pg_regprefix: compiled regex has no guts");
        crate::regex_exec::pg_regprefix(mcx, guts)
    });

    match res {
        Some(Ok(pr)) => match pr.code {
            REG_PREFIX => {
                let v = slice_in(mcx, &pr.prefix)?;
                Ok(RegprefixResult::Prefix(v))
            }
            REG_EXACT => {
                let v = slice_in(mcx, &pr.prefix)?;
                Ok(RegprefixResult::Exact(v))
            }
            REG_NOMATCH => Ok(RegprefixResult::NoMatch),
            code => Ok(RegprefixResult::Failed(RegexFailure {
                message: pg_regerror(code),
            })),
        },
        Some(Err(e)) if e.code() == REG_NOMATCH => Ok(RegprefixResult::NoMatch),
        Some(Err(e)) => Ok(RegprefixResult::Failed(RegexFailure {
            message: pg_regerror(e.code()),
        })),
        None => Ok(RegprefixResult::Failed(RegexFailure {
            message: pg_regerror(REG_ASSERT),
        })),
    }
}

/// Adapter for the `pg_regfree` inward seam: remove `handle` from the registry,
/// dropping the owned [`RegexT`] (which frees the engine state).
pub fn seam_pg_regfree(handle: RegexHandle) {
    // C: `pg_regfree(NULL)` is a no-op; here a missing handle simply removes
    // nothing. The removed `RegexT` is dropped, which runs `pg_regfree`'s
    // effect (the owned guts are freed by Rust ownership).
    let removed = REGEX_REGISTRY.with(|reg| reg.borrow_mut().remove(&handle.0));
    if let Some(re) = removed {
        pg_regfree(re);
    }
}

// ===========================================================================
// regfree.c
// ===========================================================================

/// `pg_regfree(regex_t *re)` — free a compiled RE. Under Rust ownership this
/// drops the owned [`RegexT`] (and thus its `guts`); the registry removal is
/// done by [`seam_pg_regfree`].
pub fn pg_regfree(re: RegexT) {
    // C: `if (re == NULL) return;` — the NULL guard is handled by the caller
    // (`seam_pg_regfree` only calls this for a present handle). C then dispatches
    // `(*re->re_fns->free)(re)` to the RE-specific freer, which frees `re_guts`.
    // Here that destructor has no analogue: dropping the owned `RegexT` frees
    // its `Box<Guts>` and all arena Vecs.
    drop(re);
}

// ===========================================================================
// regerror.c
// ===========================================================================

/// One row of the `REG_*` code → name/explain table (regerror.c: `struct rerr`,
/// built from `regerrs.h`). The sentinel `code == -1` row is special-cased in
/// [`pg_regerror`] and is therefore not stored in this table.
struct Rerr {
    code: i32,
    name: &'static str,
    explain: &'static str,
}

/// The `rerrs[]` table (regerror.c `#include "regex/regerrs.h"`), minus the
/// trailing `{-1, "", "oops"}` sentinel (special-cased in code).
static RERRS: &[Rerr] = &[
    Rerr { code: REG_OKAY, name: "REG_OKAY", explain: "no errors detected" },
    Rerr { code: REG_NOMATCH, name: "REG_NOMATCH", explain: "failed to match" },
    Rerr { code: REG_BADPAT, name: "REG_BADPAT", explain: "invalid regexp (reg version 0.8)" },
    Rerr { code: REG_ECOLLATE, name: "REG_ECOLLATE", explain: "invalid collating element" },
    Rerr { code: REG_ECTYPE, name: "REG_ECTYPE", explain: "invalid character class" },
    Rerr { code: REG_EESCAPE, name: "REG_EESCAPE", explain: "invalid escape \\ sequence" },
    Rerr { code: REG_ESUBREG, name: "REG_ESUBREG", explain: "invalid backreference number" },
    Rerr { code: REG_EBRACK, name: "REG_EBRACK", explain: "brackets [] not balanced" },
    Rerr { code: REG_EPAREN, name: "REG_EPAREN", explain: "parentheses () not balanced" },
    Rerr { code: REG_EBRACE, name: "REG_EBRACE", explain: "braces {} not balanced" },
    Rerr { code: REG_BADBR, name: "REG_BADBR", explain: "invalid repetition count(s)" },
    Rerr { code: REG_ERANGE, name: "REG_ERANGE", explain: "invalid character range" },
    Rerr { code: REG_ESPACE, name: "REG_ESPACE", explain: "out of memory" },
    Rerr { code: REG_BADRPT, name: "REG_BADRPT", explain: "quantifier operand invalid" },
    Rerr { code: REG_ASSERT, name: "REG_ASSERT", explain: "\"cannot happen\" -- you found a bug" },
    Rerr { code: REG_INVARG, name: "REG_INVARG", explain: "invalid argument to regex function" },
    Rerr { code: REG_MIXED, name: "REG_MIXED", explain: "character widths of regex and string differ" },
    Rerr { code: REG_BADOPT, name: "REG_BADOPT", explain: "invalid embedded option" },
    Rerr { code: REG_ETOOBIG, name: "REG_ETOOBIG", explain: "regular expression is too complex" },
    Rerr { code: REG_ECOLORS, name: "REG_ECOLORS", explain: "too many colors" },
];

/// `pg_regerror(int errcode, const regex_t *preg, char *errbuf, size_t
/// errbuf_size)` — format a `REG_*` code into its human-readable message.
///
/// The scaffold seam adapters only ever take the "real, normal error code"
/// path (the `default:` arm of C's switch): they pass a concrete `REG_*` code
/// and want the explanation text. They never use `REG_ATOI`/`REG_ITOA`, which
/// require a caller-supplied name/number string in `errbuf`. This function
/// therefore implements the `default:` arm faithfully (table lookup; the
/// unknown-code fallback message) and, for completeness/fidelity, the
/// `REG_ATOI`/`REG_ITOA` specials are still mapped — they degenerate without an
/// input `errbuf` to the same shape C produces from an empty buffer.
///
/// C returns the full message and truncates into the caller's `errbuf`; here we
/// return the whole [`String`] and the seam consumer (which has no fixed-size
/// buffer) keeps it intact.
pub fn pg_regerror(errcode: i32) -> String {
    // C `default:` arm: scan the table for a matching code.
    let msg: String = match errcode {
        REG_ATOI => {
            // C: convert a name (in errbuf) to its number. With no input name
            // available across the seam, this degenerates to the sentinel
            // "-1" that C's loop produces when no name matches.
            "-1".to_string()
        }
        REG_ITOA => {
            // C: convert a number (in errbuf) to its name; with no input
            // number available, C's atoi("") yields 0 → "REG_OKAY".
            match RERRS.iter().find(|r| r.code == 0) {
                Some(r) => r.name.to_string(),
                None => "REG_0".to_string(),
            }
        }
        _ => match RERRS.iter().find(|r| r.code == errcode) {
            Some(r) => r.explain.to_string(),
            // C: unknown code → sprintf(convbuf, unk, errcode), where
            // unk = "*** unknown regex error code 0x%x ***".
            None => format!("*** unknown regex error code 0x{:x} ***", errcode),
        },
    };
    msg
}

// ===========================================================================
// regexport.c — NFA / color exporters  (regexport.h)
// ===========================================================================

/// `regex_arc_t` (regexport.h) — one exported NFA arc.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RegexArc {
    /// `co` — label (character-set color) of the arc.
    pub co: i32,
    /// `to` — next state number.
    pub to: i32,
}

/// Fetch the `guts` behind a compiled regex, asserting `re_magic == REMAGIC`
/// (C: `assert(regex != NULL && regex->re_magic == REMAGIC)`). The guts pointer
/// is always present on a valid compiled regex.
#[inline]
fn guts_of(re: &RegexT) -> &crate::regguts::Guts {
    debug_assert!(re.re_magic == REMAGIC);
    re.re_guts
        .as_ref()
        .expect("regexport: compiled regex has no guts")
}

/// `pg_reg_getnumstates(const regex_t *regex)`.
pub fn pg_reg_getnumstates(re: &RegexT) -> i32 {
    let cnfa = &guts_of(re).search;
    cnfa.nstates
}

/// `pg_reg_getinitialstate(const regex_t *regex)`.
pub fn pg_reg_getinitialstate(re: &RegexT) -> i32 {
    let cnfa = &guts_of(re).search;
    cnfa.pre
}

/// `pg_reg_getfinalstate(const regex_t *regex)`.
pub fn pg_reg_getfinalstate(re: &RegexT) -> i32 {
    let cnfa = &guts_of(re).search;
    cnfa.post
}

/// `traverse_lacons(struct cnfa *cnfa, int st, int *arcs_count, regex_arc_t
/// *arcs, int arcs_len)` (regexport.c) — recursive subroutine used by both
/// exported out-arc functions. LACON arcs are treated as automatically
/// satisfied and recursed through; reachable ordinary arcs are counted in
/// `*arcs_count` and, as far as `arcs.len()` allows, emitted into `arcs`.
fn traverse_lacons(
    cnfa: &crate::regguts::Cnfa,
    st: i32,
    arcs_count: &mut i32,
    arcs: &mut [RegexArc],
) {
    // C: check_stack_depth() — guards against runaway LACON-loop recursion.
    // Routed through the stack-depth owner's seam; the ereport `Err` aborts the
    // call exactly as C's longjmp does (the exporter signatures, like C's,
    // cannot carry the error).
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()
        .expect("traverse_lacons: stack too deep");

    // C: for (ca = cnfa->states[st]; ca->co != COLORLESS; ca++)
    let range = cnfa.states[st as usize].clone();
    for idx in range {
        let ca = cnfa.arcs[idx];
        if ca.co == COLORLESS {
            // List terminator (the dummy carc); C's loop condition stops here.
            break;
        }
        if (ca.co as i32) < cnfa.ncolors {
            // Ordinary arc, so count and possibly emit it.
            let ndx = *arcs_count;
            *arcs_count += 1;
            if ndx < arcs.len() as i32 {
                arcs[ndx as usize].co = ca.co as i32;
                arcs[ndx as usize].to = ca.to;
            }
        } else {
            // LACON arc --- assume it's satisfied and recurse...
            // ... but first, assert it doesn't lead directly to post state.
            debug_assert!(ca.to != cnfa.post);
            traverse_lacons(cnfa, ca.to, arcs_count, arcs);
        }
    }
}

/// `pg_reg_getnumoutarcs(const regex_t *regex, int st)`.
pub fn pg_reg_getnumoutarcs(re: &RegexT, st: i32) -> i32 {
    let cnfa = &guts_of(re).search;

    if st < 0 || st >= cnfa.nstates {
        return 0;
    }
    let mut arcs_count = 0;
    traverse_lacons(cnfa, st, &mut arcs_count, &mut []);
    arcs_count
}

/// `pg_reg_getoutarcs(const regex_t *regex, int st, regex_arc_t *arcs, int
/// arcs_len)` — fill `arcs` (up to `arcs.len()`) with state `st`'s out-arcs.
pub fn pg_reg_getoutarcs(re: &RegexT, st: i32, arcs: &mut [RegexArc]) {
    let cnfa = &guts_of(re).search;

    if st < 0 || st >= cnfa.nstates || arcs.is_empty() {
        return;
    }
    let mut arcs_count = 0;
    traverse_lacons(cnfa, st, &mut arcs_count, arcs);
}

/// `pg_reg_getnumcolors(const regex_t *regex)`.
pub fn pg_reg_getnumcolors(re: &RegexT) -> i32 {
    let cm = &guts_of(re).cmap;
    // C: `return cm->max + 1;` — `max` is the highest color in use.
    cm.max as i32 + 1
}

/// `pg_reg_colorisbegin(const regex_t *regex, int co)`.
pub fn pg_reg_colorisbegin(re: &RegexT, co: i32) -> bool {
    let cnfa = &guts_of(re).search;
    co == cnfa.bos[0] as i32 || co == cnfa.bos[1] as i32
}

/// `pg_reg_colorisend(const regex_t *regex, int co)`.
pub fn pg_reg_colorisend(re: &RegexT, co: i32) -> bool {
    let cnfa = &guts_of(re).search;
    co == cnfa.eos[0] as i32 || co == cnfa.eos[1] as i32
}

/// `pg_reg_getnumcharacters(const regex_t *regex, int co)`.
pub fn pg_reg_getnumcharacters(re: &RegexT, co: i32) -> i32 {
    let cm = &guts_of(re).cmap;

    // C: `if (co <= 0 || co > cm->max)` — <= 0 rejects WHITE and RAINBOW.
    if co <= 0 || co as usize > cm.max {
        return -1;
    }
    // C: `if (cm->cd[co].flags & PSEUDO)` — also pseudocolors (BOS etc).
    if cm.cd[co as usize].flags & PSEUDO != 0 {
        return -1;
    }
    // C: if the color appears in the high colormap, its number of members is
    // uncertain.
    if cm.cd[co as usize].nuchrs != 0 {
        return -1;
    }
    // OK, return the known number of member chrs.
    cm.cd[co as usize].nschrs
}

/// `pg_reg_getcharacters(const regex_t *regex, int co, pg_wchar *chars, int
/// chars_len)` — fill `chars` (up to `chars.len()`) with the characters of
/// color `co`.
pub fn pg_reg_getcharacters(re: &RegexT, co: i32, chars: &mut [PgWChar]) {
    let cm = &guts_of(re).cmap;

    // C: `if (co <= 0 || co > cm->max || chars_len <= 0) return;`
    if co <= 0 || co as usize > cm.max || chars.is_empty() {
        return;
    }
    if cm.cd[co as usize].flags & PSEUDO != 0 {
        return;
    }

    // C: only the low character map need be examined.
    //   for (c = CHR_MIN; c <= MAX_SIMPLE_CHR; c++)
    //       if (cm->locolormap[c - CHR_MIN] == co) { *chars++ = c; if (--chars_len == 0) break; }
    let mut chars_len = chars.len();
    let mut out = 0usize;
    let mut c: chr = CHR_MIN;
    loop {
        if cm.locolormap[(c - CHR_MIN) as usize] as i32 == co {
            chars[out] = c;
            out += 1;
            chars_len -= 1;
            if chars_len == 0 {
                break;
            }
        }
        if c == MAX_SIMPLE_CHR {
            break;
        }
        c += 1;
    }
}
