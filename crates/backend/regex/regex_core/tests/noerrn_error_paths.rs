//! p1-regexpanic REGRESSION (release blocker): C's nfa subroutines record
//! failures in v->err via NERR() (regc_nfa.c: `#define NERR(e) VERR(nfa->v,
//! (e))`), so parse()'s NOERRN() checks see them and pg_regcomp's CNOERR()
//! returns the error. The Rust parse family instead DISCARDED the RegResult
//! error (`Err(_) => return None`), so parse() returned None with v.err
//! unset, `cnoerr!()` passed, and `v.tree.unwrap()` -- guarded only by a
//! debug_assert -- was a reachable RELEASE panic (regex_compile.rs:2832 at
//! a40088a894, `called Option::unwrap() on a None value`). pgrust is
//! thread-per-backend, so that panic kills every session.
//!
//! Repro shape: nested bounded quantifiers. `repeat()` duplicates the
//! sub-NFA via dupnfa() DURING parse, so a few nesting levels of {250}
//! multiply NFA space past REG_MAX_COMPILE_SPACE and newstate()/newarc()
//! return REG_ETOOBIG -- shallow recursion, so the byte-based stack guard
//! (which does record its error) never fires first. Deep `(((...)))`
//! nesting does NOT reach the defect: it trips the DepthGuard, which
//! records its error correctly.
//!
//! C parity witness (docker pg-stock183, PostgreSQL 18.3):
//!   SELECT 'a' ~ '(?:(?:(?:a{250}){250}){250}){250}';
//!   ERROR:  2201B: invalid regular expression: regular expression is too
//!   complex   (= REG_ETOOBIG)

use ::mcx::MemoryContext;
use regex_core::regex_compile::pg_regcomp;
use regex_core::regex_consts::REG_ADVANCED;

const REG_ETOOBIG: i32 = 19;

/// Compile on a dedicated thread so a panic surfaces as a join error
/// (witnessing "compiler must not panic") instead of failing the harness.
fn compile(pat: &str) -> Result<(), i32> {
    let w: Vec<u32> = pat.chars().map(|c| c as u32).collect();
    let h = std::thread::Builder::new()
        .stack_size(8 << 20)
        .spawn(move || {
            ::stack_depth::set_stack_base();
            ::stack_depth::assign_max_stack_depth(7680);
            let ctx = MemoryContext::new("t");
            pg_regcomp(ctx.mcx(), &w, REG_ADVANCED, 0)
                .map(|_| ())
                .map_err(|e| e.0)
        })
        .unwrap();
    h.join()
        .expect("regex compiler must not panic (release blocker if it does)")
}

#[test]
fn nested_bounded_quantifiers_error_etoobig_noncapturing() {
    // Pre-fix: panicked at regex_compile.rs:2832 unwrap on None (release).
    assert_eq!(
        compile("(?:(?:(?:a{250}){250}){250}){250}"),
        Err(REG_ETOOBIG)
    );
}

#[test]
fn nested_bounded_quantifiers_error_etoobig_capturing() {
    // Same defect through the messy (capturing) parseqatom path.
    assert_eq!(compile("(((a{250}){250}){250}){250}"), Err(REG_ETOOBIG));
}

#[test]
fn backref_dup_blowup_still_etoobig() {
    // Backref dupnfa path: already reported REG_ETOOBIG pre-fix (its
    // NISERR-check ordering happened to cover it); must keep doing so.
    assert_eq!(compile("(a{250})\\1{250}(b{250})\\2{250}"), Err(REG_ETOOBIG));
}

#[test]
fn ordinary_patterns_still_compile() {
    assert_eq!(compile("(a|b)*c{2,5}[x-z]+"), Ok(()));
    assert_eq!(compile("(?:ab){10}"), Ok(()));
}
