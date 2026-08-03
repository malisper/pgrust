//! Attribution probe + regression pin for the r2/r3 CI cluster divergence class
//! "Rust REG_ETOOBIG vs C real syntax error" (campaign p1-regexcore, task
//! #69; CI cluster artifacts crash-04cf69b5, crash-1cc6391a, crash-1e9d853e,
//! crash-a17fc732, crash-fb86624b).
//!
//! Both engines FAIL to compile these patterns, but the r2/r3 CI cluster builds
//! saw the Rust side report "regular expression is too complex" where the
//! verbatim 18.3 C engine reports the true syntax error ("parentheses ()
//! not balanced" / "invalid escape \\ sequence"). The mechanism question
//! this file answers at every run: is the Rust ETOOBIG the ENVIRONMENTAL
//! stack-band guard (ratified platform non-surface) or the DETERMINISTIC
//! compile-space accounting (REG_MAX_COMPILE_SPACE — would be a real
//! parity defect)?
//!
//! Verdict pinned here: at a WIDE stack budget (30 MiB) the Rust engine
//! must agree with C's syntax-error verdict — i.e. the ETOOBIG is the
//! stack guard firing mid-parse at the 2048kB server default, earlier
//! than clang-built C's guard trips at the same byte budget. That is the
//! ratified stack-band asymmetry (frame-size environmental), surfacing on
//! the error-vs-error plane instead of the error-vs-success plane the
//! fuzz driver's is_etoobig carve already tolerates.

use regex::RegcompResult;
use regex_core::regex_export_free_error::seam_pg_regcomp;
use types_core::C_COLLATION_OID;

/// pattern chrs from crash report (94 chrs, REG_ADVANCED)
const PAT_EPAREN: &[u32] = &[
    0x63, 0x64, 0x2a, 0x78, 0x79, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x24,
    0x27, 0x5c, 0x79, 0x2e, 0x7c, 0x29, 0x7b, 0x36, 0x33, 0x7d, 0x7c, 0x2e,
    0x7c, 0x101, 0x1, 0x29, 0x7b, 0x36, 0x32, 0x7d, 0x3, 0x0, 0x1, 0x3f,
    0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x61, 0x2b, 0x7c, 0x29, 0x4d, 0x7c, 0x29,
    0x2b, 0x7c, 0x29, 0x2b, 0x24, 0x7c, 0x29, 0x2b, 0x2e, 0x7c, 0x28, 0x28,
    0x28, 0x28, 0x62, 0x63, 0x64, 0x2a, 0x28, 0x28, 0x28, 0x28, 0x130,
    0xf5, 0x130, 0x5c, 0x41, 0x28, 0x63, 0x130, 0x65, 0x64, 0x24, 0x78,
    0x62, 0x63, 0x64, 0x2a, 0x78, 0x79, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
    0x24,
];

/// pattern chrs from crash report (62 chrs, REG_ADVANCED)
const PAT_EESCAPE: &[u32] = &[
    0x28, 0x4e, 0x5c, 0x28, 0x2e, 0x2, 0x3, 0x54, 0x69, 0x4e, 0x4e, 0x6e,
    0x25, 0x43, 0x43, 0x7a, 0x43, 0x43, 0x43, 0x41, 0x43, 0x28, 0x5c, 0x79,
    0x2e, 0x43, 0x43, 0x43, 0x43, 0x28, 0x27, 0x5c, 0x79, 0x2e, 0x7c, 0x29,
    0x7b, 0x38, 0x39, 0x7d, 0x7c, 0x2e, 0x7c, 0x29, 0x7b, 0x33, 0x30, 0x7d,
    0x7c, 0x7c, 0x3a, 0x5c, 0x77, 0x2b, 0x76, 0x50, 0x29, 0x7b, 0x31, 0x36,
    0x7d, 0x69, 0x5c,
];

fn compile_msg_at_budget(pat: &[u32], budget_kb: i32) -> Option<String> {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn({
            let pat = pat.to_vec();
            move || {
                stack_depth::set_stack_base();
                stack_depth::set_max_stack_depth(budget_kb);
                stack_depth::assign_max_stack_depth(budget_kb);
                match seam_pg_regcomp(&pat, 0o3, C_COLLATION_OID).unwrap() {
                    RegcompResult::Compiled(_) => None,
                    RegcompResult::Failed(f) => Some(f.message),
                }
            }
        })
        .unwrap()
        .join()
        .unwrap()
}

/// At a wide budget the true syntax error must win — the CI-witnessed
/// ETOOBIG is the environmental stack guard, not compile-space accounting.
#[test]
fn syntax_error_wins_at_wide_stack_budget() {
    assert_eq!(
        compile_msg_at_budget(PAT_EPAREN, 30 * 1024).as_deref(),
        Some("parentheses () not balanced"),
        "crash-04cf69b5 pattern at 30MiB budget"
    );
    assert_eq!(
        compile_msg_at_budget(PAT_EESCAPE, 30 * 1024).as_deref(),
        Some("invalid escape \\ sequence"),
        "crash-fb86624b pattern at 30MiB budget"
    );
}

/// Attribution record (not a parity assertion — the 2048kB verdict is the
/// ratified ENVIRONMENTAL band): at the server-default budget the guard
/// may fire first on either side; this test only pins that the failure is
/// CLEAN (an error verdict, never an abort) whichever way it lands.
#[test]
fn server_default_budget_fails_clean() {
    for (pat, name) in [(PAT_EPAREN, "crash-04cf69b5"), (PAT_EESCAPE, "crash-fb86624b")] {
        let msg = compile_msg_at_budget(pat, 2048);
        assert!(
            matches!(
                msg.as_deref(),
                Some("regular expression is too complex")
                    | Some("parentheses () not balanced")
                    | Some("invalid escape \\ sequence")
            ),
            "{name} at 2048kB: unexpected outcome {msg:?}"
        );
    }
}
