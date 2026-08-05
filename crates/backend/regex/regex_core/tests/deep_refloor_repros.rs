//! Regression tests for the two release-effective process aborts found by
//! the 2026-08-01 deep-plane refloor on adt/regexp (fleet job
//! pgrust-fuzz-campaign-1785565698-7083-42609, run at main crates
//! 016a105188 which predates this branch's VERR-funnel fix).
//!
//! Both inputs drove `regex_compile.rs`'s post-parse `v.tree.unwrap()` on a
//! `None` tree in RELEASE builds (the `debug_assert!(v.tree.is_some())` one
//! line above is compiled out — the debug-assert-masking class). Root cause:
//! ~74 parse-layer `Err -> None` conversions dropped C's VERR side effects
//! (record first error, force nexttype = EOS), fixed on this branch by
//! funnelling every conversion through `v.seterr(..)` (C regcomp.c VERR).
//!
//! Ground truth (real PostgreSQL 18.3, banked in the refloor evidence,
//! branch `proofs/deep-plane-floors`, docs/verification/
//! deep-plane-refloor-2026-08-01.md section 6):
//!   substring('abc' FROM '(()(w(|){97}){96})(\u{0480}\u{0480}\1\)')
//!     => clean ERROR 2201B "regular expression is too complex"
//!   regexp_split_to_array('abc', '{(\=(w(|){62}){98})*')
//!     => {abc} (pattern compiles; no match in 'abc')
//! pgrust at main ABORTS the process on both.
//!
//! What these tests pin is the ABORT CLASS: every outcome must be a clean
//! engine result (Compiled or Failed), never a panic/process abort.
//! Which clean outcome appears at the server-default 2048 kB budget is
//! environment-dependent through the RATIFIED stack-band non-surface
//! (REG_ETOOBIG's stack-guard trip point is a function of machine frame
//! sizes — debug-Rust frames are far larger than release-C frames, so a
//! debug test build trips where real PG parses on; see the carve note at
//! fuzz/core/src/regex_diff.rs::is_etoobig). Measured attribution at
//! authoring time (debug build, macOS aarch64):
//!   - 8 MiB thread / 2048 kB armed budget: repro A -> REG_ETOOBIG (matches
//!     real PG 18.3 at the same budget), repro B -> REG_ETOOBIG (real PG
//!     compiles; stack-band carve, debug frames).
//!   - 64 MiB thread / 60 MB budget (guard out of the picture): repro A ->
//!     "parentheses () not balanced" (the pattern's true syntax error),
//!     repro B -> Compiled + NoMatch on 'abc' (matches real PG exactly).
//! Reverting the VERR-funnel commit makes both repros panic again
//! (mutation-checked at authoring time).

use regex::{RegcompResult, RegexecResult};
use regex_core::regex_consts::REG_ADVANCED;
use regex_core::regex_export_free_error::{seam_pg_regcomp, seam_pg_regexec};
use types_core::C_COLLATION_OID;

fn to_w(s: &str) -> Vec<u32> {
    s.chars().map(|c| c as u32).collect()
}

/// Run `body` the way a real backend runs a statement: on a thread with the
/// production 8 MiB stack, with the per-thread stack guard armed at the
/// 2048 kB server-default budget (`stack_depth` state is thread-local; an
/// unarmed test thread has NO effective guard, so deep patterns would
/// overflow the native stack spuriously instead of raising REG_ETOOBIG).
fn with_backend_stack<F: FnOnce() + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            stack_depth::set_stack_base();
            stack_depth::set_max_stack_depth(2048);
            stack_depth::assign_max_stack_depth(2048);
            body()
        })
        .expect("spawn")
        .join()
        .expect("repro body panicked or aborted — the refloor abort class is back");
}

/// Refloor crash #2 (crash-441ade03..., 29 bytes minimized): nested bounded
/// quantifiers + an unbalanced `\)` after a backreference. Real PG 18.3 at
/// the same budget: REG_ETOOBIG ("regular expression is too complex",
/// sqlstate 2201B). Either clean failure class is acceptable (stack-band
/// carve decides which fires first); a Compiled result or a panic is a
/// failure.
#[test]
fn refloor_crash_backref_after_failed_parse_is_clean_error() {
    with_backend_stack(|| {
        let pat = "(()(w(|){97}){96})(\u{0480}\u{0480}\\1\\)";
        match seam_pg_regcomp(&to_w(pat), REG_ADVANCED, C_COLLATION_OID)
            .expect("compile must not hard-error the session")
        {
            RegcompResult::Failed(f) => {
                assert!(
                    f.message.contains("too complex") || f.message.contains("not balanced"),
                    "unexpected failure class for refloor repro A: {:?}",
                    f
                );
            }
            RegcompResult::Compiled(_) => {
                panic!("this pattern has an unbalanced \\); compiling it is a divergence")
            }
        }
    });
}

/// Refloor crash #1 (crash-15661b4e..., 22 bytes minimized): `{`-led ARE
/// (a literal brace when not a valid bound) over nested empty-alternation
/// quantifiers. Real PG 18.3: compiles; regexp_split_to_array('abc', ...)
/// = {abc} (no match). In a debug build at 2048 kB the stack guard trips
/// first (REG_ETOOBIG, ratified stack-band carve) — also acceptable. A
/// panic, a hard error, a match, or any other failure class is a failure.
#[test]
fn refloor_crash_brace_led_quantified_group_no_abort() {
    with_backend_stack(|| {
        let pat = "{(\\=(w(|){62}){98})*";
        match seam_pg_regcomp(&to_w(pat), REG_ADVANCED, C_COLLATION_OID)
            .expect("compile must not hard-error the session")
        {
            RegcompResult::Compiled(re) => {
                let dw = to_w("abc");
                match seam_pg_regexec(&re, &dw, 0, &mut []).expect("exec must not hard-error") {
                    RegexecResult::NoMatch => {}
                    other => panic!(
                        "PG 18.3 regexp_split_to_array('abc', pat) = {{abc}} (no match); got {:?}",
                        other
                    ),
                }
            }
            RegcompResult::Failed(f) => {
                assert!(
                    f.message.contains("too complex"),
                    "only the stack-band REG_ETOOBIG carve is acceptable here; got {:?}",
                    f
                );
            }
        }
    });
}
