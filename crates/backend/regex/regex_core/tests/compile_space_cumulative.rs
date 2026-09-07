//! Regression witness for audit-18.6 row
//! a186-candidate-fp-regex-regc_nfa-p1-61c51c272f220c219121-1 (batch
//! b272-backend-regex-regex-core-1): REG_MAX_COMPILE_SPACE is charged per
//! COMPILATION in C, not per NFA.
//!
//! C regc_nfa.c newstate (line 165) and allocarc (line 389) test
//! `nfa->v->spaceused >= REG_MAX_COMPILE_SPACE` before every new state/arc
//! batch, and `nfa->v->spaceused` is the `struct vars` counter shared by the
//! primary NFA and every sub-NFA compiled from it (regcomp.c nfanode, line
//! 2371: `newnfa(v, v->cm, v->nfa)`); freenfa (regc_nfa.c:117/124) gives a
//! sub-NFA's batches back. So while nfatree builds the top tree node's own
//! NFA — a near-complete duplicate of the (already optimized) primary NFA —
//! the budget covers primary + duplicate, and a pattern whose primary NFA
//! alone fits comfortably still fails with REG_ETOOBIG ("regular expression
//! is too complex", SQLSTATE 2201B) once the pair exceeds the limit.
//!
//! The witness is a wide, shallow alternation of N distinct 4-letter words
//! (`aaaa|aaab|...`): per-branch cost is fixed, the NFA is a few states
//! deep (no stack-guard interference — the ETOOBIG here is the accounting,
//! not STACK_TOO_DEEP), and the top-level '|' node's nfanode duplicates the
//! whole thing. The exact boundary was measured on live C 18.6
//! (x86_64, LP64: sizeof(struct state) 56, sizeof(struct arc) 72, batch
//! header 16; REG_MAX_COMPILE_SPACE = 172,000,000): N = 120649 compiles,
//! N = 120650 is REG_ETOOBIG. Before the fix pgrust charged each NFA
//! separately (per-state bytes, no batch rounding) and compiled these up to
//! roughly N = 241000.

use regex::RegcompResult;
use regex_core::regex_export_free_error::seam_pg_regcomp;
use types_core::C_COLLATION_OID;

/// Largest N whose primary + top-node duplicate fit under
/// REG_MAX_COMPILE_SPACE on C 18.6 (measured by bisection on the live pair).
const LAST_FITTING_N: usize = 120649;

/// `aaaa|aaab|...`: N distinct four-letter lowercase words joined by '|'.
/// Word i is the base-26 spelling of i, most significant letter first
/// (the SQL twin: chr(97+(i/17576)%26)||chr(97+(i/676)%26)||
/// chr(97+(i/26)%26)||chr(97+i%26)).
fn wide_alternation(n: usize) -> Vec<u32> {
    let mut pat: Vec<u32> = Vec::with_capacity(n * 5);
    for i in 0..n {
        if i != 0 {
            pat.push(u32::from(b'|'));
        }
        for div in [17576usize, 676, 26, 1] {
            pat.push(u32::from(b'a') + ((i / div) % 26) as u32);
        }
    }
    pat
}

/// Compile `pat` (REG_ADVANCED) on a wide-stack thread with the stack
/// guard set far past anything this shallow NFA can reach, so the only
/// REG_ETOOBIG source left is compile-space accounting. `None` = compiled.
fn compile_msg(pat: Vec<u32>) -> Option<String> {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            stack_depth::set_stack_base();
            stack_depth::set_max_stack_depth(30720);
            stack_depth::assign_max_stack_depth(30720);
            match seam_pg_regcomp(&pat, 0o3, C_COLLATION_OID).unwrap() {
                RegcompResult::Compiled(_) => None,
                RegcompResult::Failed(f) => Some(f.message),
            }
        })
        .unwrap()
        .join()
        .unwrap()
}

/// One past the measured C boundary: the primary NFA fits, but primary +
/// the top node's duplicate does not — C reports REG_ETOOBIG at compile
/// time, and so must we.
#[test]
fn primary_plus_subnfa_exceeding_limit_is_etoobig() {
    assert_eq!(
        compile_msg(wide_alternation(LAST_FITTING_N + 1)).as_deref(),
        Some("regular expression is too complex"),
        "C 18.6 rejects N={} at compile time (regc_nfa.c:165/389 charge \
         nfa->v->spaceused across the primary NFA and the nfanode duplicate)",
        LAST_FITTING_N + 1
    );
}

/// The measured C boundary itself compiles: the accounting must be the C
/// batch accounting (regguts.h STATEBATCHSIZE/ARCBATCHSIZE, FIRSTSBSIZE 32
/// / FIRSTABSIZE 64 doubling to 1024, LP64 struct sizes), not merely "some"
/// cumulative charge — an over-eager port would fail here.
#[test]
fn last_fitting_pattern_still_compiles() {
    assert_eq!(
        compile_msg(wide_alternation(LAST_FITTING_N)),
        None,
        "C 18.6 compiles N={} (one batch short of REG_MAX_COMPILE_SPACE)",
        LAST_FITTING_N
    );
}
