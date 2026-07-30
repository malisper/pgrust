//! Kani C≡Rust divergence census / equivalence proofs for JSON string escaping.
//!
//! Rust side: SHIPPED adt_json::escape_json (crates/backend/utils/adt/json).
//! C side: vendored escape_json family from postgres master json.c (c/pg_escape.c).
//!
//! HISTORY: the original harnesses (pre proof_support mcx-stubs) pulled the
//! full mcx arena into symex and died on the CBMC-6.8 silent self-abort
//! (status 15 after 8-25min at 2-4GB); the len<=4 census was adjudicated by
//! native differential instead (src/bin/*, TRIAGE "JSON ESCAPE CENSUS":
//! ZERO divergences vs escape_json_with_len; the only cstr-variant class is
//! NUL truncation, RATIFIED-UNREACHABLE per the NUL audit).
//!
//! 2026-07-28 (mcx-stubs upgrade attempt, measured on a HEAVILY shared
//! laptop, load avg 5-13 from concurrent proof agents): the full recipe —
//! `Mcx::allocate`/`grow`/`deallocate` → static bump / fresh-copy / no-op,
//! `env::var` → "0", `OnceLock::get_or_init` → recompute, PgError::error +
//! fmt::format stubs, no `.unwrap()` on PgResult (ERROR-DROP TRAP: Box<PgError>
//! Debug+drop glue), stack ctx + mem::forget, LOCAL-array-backed StringInfo,
//! tiny-proof-heap, PG_BUFSZ 32, assert! not assert_eq!, tight per-length
//! unwind — BREAKS the old symex wall: symex now completes in 43-108s
//! (was: CBMC 6.8 silent self-abort after 8-25min). The grow stub is
//! LOAD-BEARING here where hex didn't need it: PgVec::try_reserve has a
//! reachable `Mcx::grow` branch that otherwise drags the real arena +
//! Acct::subtree_sum recursion into symex.
//!
//! RESIDUAL WALL (new TRIAGE class, distinct from the self-abort): the
//! SYMBOLIC harnesses die in CBMC's propositional reduction at >6 GiB RSS
//! (measured kills 6.3-13.7 GB; RSS watchdog per memory protocol). Mechanism
//! is WIDTH-bound: escape output length is data-dependent, so every
//! StringInfo/C-shim store lands at a SYMBOLIC offset; the concrete-input
//! probe (probe_len1_concrete) is GREEN in 114s, and kani::assume class
//! splits do NOT constant-fold the offsets (same VCC count split vs unsplit
//! — the interval-cmp literal-vs-assumed lesson). Neither solver choice,
//! --no-assertion-reach-checks, buffer shrinking, nor unwind tightening
//! moves it. Formal verdict for the symbolic len1-4 census stays BLOCKED:
//! wall(memory >6GiB, propositional reduction, symbolic-offset stores);
//! the len<=4 equivalence census stands on the native differential
//! (tested(differential), zero divergences — src/bin/*).
//!
//! Run recipe:
//!   timeout 300 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_escape.c \
//!       --harness <h> --exact [--no-assertion-reach-checks] [--solver kissat]
//! (DEFAULT solver for must-fail controls — kissat never terminates on
//! failing harnesses.) ONE solve at a time + 6 GiB RSS watchdog, mandatory.
//!
//! Measured results (2026-07-28, load 5-13):
//! - census_len{1..4}_with_len, census_len1_{plain,twochar,u04x},
//!   census_len1_cstr(+_r2): wall(memory >6GiB in propositional reduction);
//!   symex completes ~43-108s — the mcx wall itself is broken.
//! - probe_len1_concrete: PROVED (green), 114s wall under load —
//!   end-to-end witness that the rig + stub set is sound.
//! - control_cstr_nul_concrete_must_fail: FAILED as required, 158s, on
//!   exactly the "length divergence" assert — the ratified cstr
//!   NUL-truncation class, machine-checked; live non-vacuity control.
//! - census_len1_split_coverage: PROVED, 0.007s (the three escape-class
//!   predicates partition all u8 values).

extern "C" {
    fn pg_run_escape_json_with_len(str_: *const u8, len: i32, out: *mut u8) -> i32;
    fn pg_run_escape_json_cstr(str_: *const u8, out: *mut u8) -> i32;
}

#[cfg(kani)]
use proof_support::{mcx_stubs, stubs};

/// Worst-case output for len<=4 input: 4*6 escaped + 2 quotes + NUL = 27.
#[cfg(kani)]
const CAP: usize = 32;

/// Run C escape into c_out, then shipped escape_json into a fresh
/// StringInfo, and compare bytes IN PLACE — an extra copy-out buffer
/// doubles the symbolic-offset store cost in the CNF.
#[cfg(kani)]
fn check_escaped_eq(s: &[u8], c_out: &[u8; CAP], c_len: usize) {
    // bump backend: pointer-bump arena, far cheaper for CBMC than the aset
    // freelists — and under the mcx-stubs recipe allocation is a static bump
    // buffer anyway. mem::forget both at the end (hex pattern): teardown
    // accounting walks (AcctWeak release, subtree_sum recursion) explode
    // CBMC symex and are not part of the escaping claim.
    let ctx = mcx::MemoryContext::new_bump("kani");
    // Back the StringInfo with a LOCAL array instead of the proof heap:
    // escape output offsets are data-dependent (symbolic), and every
    // symbolic-offset store SSA-versions the WHOLE backing array in the
    // CNF — a 32-byte local is 64x cheaper than the 2 KiB static heap
    // (measured: heap-backed StringInfo blew propositional reduction past
    // 13 GB RSS). The PgVec still carries the real Mcx handle; regrow/drop
    // paths are statically dead (CAP >= worst-case output; both forgotten).
    let mut backing = [0u8; CAP];
    // SAFETY(harness): fresh exclusive block, len 0 <= CAP; never dropped
    // (mem::forget below), so the Mcx never tries to free a stack pointer.
    let v = unsafe {
        mcx::PgVec::from_raw_parts_in(backing.as_mut_ptr(), 0, CAP, ctx.mcx())
    };
    // No .unwrap(): Debug-formatting + drop glue of Box<PgError> wall symex
    // (ERROR-DROP TRAP). Err arms are statically dead here (capacity CAP >=
    // worst-case output) — forget + static panic.
    let mut buf = match stringinfo::StringInfo::from_vec(v) {
        Ok(b) => b,
        Err(e) => {
            core::mem::forget(e);
            panic!("from_vec failed");
        }
    };
    let r = adt_json::escape_json(&mut buf, s);
    if r.is_err() {
        core::mem::forget(r);
        panic!("escape_json failed");
    }
    let b = buf.as_bytes();
    assert!(b.len() == c_len, "length divergence");
    for i in 0..b.len() {
        assert!(b[i] == c_out[i], "byte divergence");
    }
    core::mem::forget(buf);
    core::mem::forget(ctx);
}

#[cfg(kani)]
fn check_eq_with_len(s: &[u8]) {
    let mut c_out = [0u8; CAP];
    let c_len = unsafe { pg_run_escape_json_with_len(s.as_ptr(), s.len() as i32, c_out.as_mut_ptr()) } as usize;
    check_escaped_eq(s, &c_out, c_len);
}

/// Round 1: exhaustive single byte vs escape_json_with_len (the semantic counterpart).
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(10)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len1_with_len() {
    let b: u8 = kani::any();
    check_eq_with_len(&[b]);
}

/// Escape-class predicates for the per-class case split (ladder step 4).
/// Within one class every byte takes the same escape arm, so output length
/// and all store offsets constant-fold during symex — the symbolic-offset
/// store blowup (measured >6-13 GB in propositional reduction on the
/// unsplit harness) never reaches the SAT formula.
#[cfg(kani)]
fn is_twochar(b: u8) -> bool {
    matches!(b, 0x08 | 0x0c | b'\n' | b'\r' | b'\t' | b'"' | b'\\')
}
#[cfg(kani)]
fn is_u04x(b: u8) -> bool {
    b < 0x20 && !is_twochar(b)
}
#[cfg(kani)]
fn is_plain(b: u8) -> bool {
    b >= 0x20 && !is_twochar(b)
}

/// len1 case split, class: plain byte (output "x" -> 3 bytes).
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(10)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len1_plain() {
    let b: u8 = kani::any();
    kani::assume(is_plain(b));
    check_eq_with_len(&[b]);
}

/// len1 case split, class: two-char escape (\b \f \n \r \t \" \\ -> 4 bytes).
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(10)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len1_twochar() {
    let b: u8 = kani::any();
    kani::assume(is_twochar(b));
    check_eq_with_len(&[b]);
}

/// len1 case split, class: \u00XX escape (other control bytes -> 8 bytes).
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(10)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len1_u04x() {
    let b: u8 = kani::any();
    kani::assume(is_u04x(b));
    check_eq_with_len(&[b]);
}

/// MUST FAIL — concrete witness of the ratified cstr NUL-truncation
/// divergence class (C's cstring escape_json stops at NUL -> "\"\""; the
/// shipped Rust slice API escapes it as "\"\\u0000\""), and the family's
/// live non-vacuity control: a green run here means the rig (FFI + stub
/// set + comparison) went blind. Concrete input keeps it under the
/// symbolic-offset memory wall that blocks the symbolic cstr harnesses.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(10)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn control_cstr_nul_concrete_must_fail() {
    let s = [0u8, 0u8]; // a single NUL byte, NUL-terminated for C
    let mut c_out = [0u8; CAP];
    let c_len = unsafe { pg_run_escape_json_cstr(s.as_ptr(), c_out.as_mut_ptr()) } as usize;
    check_escaped_eq(&s[..1], &c_out, c_len);
}

/// TRIAGE PROBE (width-1): fully CONCRETE input byte. If this still blows
/// propositional reduction, the cost is the harness machinery (StringInfo/
/// mcx construction circuit), not the symbolic byte — depth-bound signature.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(10)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn probe_len1_concrete() {
    check_eq_with_len(&[b'a']);
}

/// MANDATORY union-coverage companion for the len1 case split: the three
/// class predicates partition ALL byte values (or the gate silently
/// weakens). Pure arithmetic, no FFI/mcx — no stubs needed.
#[cfg(kani)]
#[kani::proof]
fn census_len1_split_coverage() {
    let b: u8 = kani::any();
    assert!(is_plain(b) || is_twochar(b) || is_u04x(b));
}

/// Length 2, all byte pairs, vs escape_json_with_len.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(18)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len2_with_len() {
    let s: [u8; 2] = kani::any();
    check_eq_with_len(&s);
}

/// Length 3 vs escape_json_with_len.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(22)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len3_with_len() {
    let s: [u8; 3] = kani::any();
    check_eq_with_len(&s);
}

/// Length 4 vs escape_json_with_len.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(28)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len4_with_len() {
    let s: [u8; 4] = kani::any();
    check_eq_with_len(&s);
}

/// Census vs the CSTRING variant escape_json (used in C for attname/outputstr):
/// expected divergence class = embedded NUL (C truncates at NUL, Rust slice does not).
/// MUST FAIL with a NUL counterexample — also the family's non-vacuity control.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(18)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len1_cstr() {
    let b: u8 = kani::any();
    let s = [b, 0u8]; // NUL-terminated for C; Rust sees only &s[..1]
    let mut c_out = [0u8; CAP];
    let c_len = unsafe { pg_run_escape_json_cstr(s.as_ptr(), c_out.as_mut_ptr()) } as usize;
    check_escaped_eq(&s[..1], &c_out, c_len);
}

/// Round 2 of the cstr census: exclude the NUL divergence class, look for the next one.
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(18)]
#[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
#[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
#[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
#[kani::stub(std::env::var, stubs::stub_env_var_zero)]
#[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
#[kani::stub(std::fmt::format, stubs::stub_format)]
fn census_len1_cstr_r2() {
    let b: u8 = kani::any();
    kani::assume(b != 0); // divergence class 1: NUL truncation (ratified)
    let s = [b, 0u8];
    let mut c_out = [0u8; CAP];
    let c_len = unsafe { pg_run_escape_json_cstr(s.as_ptr(), c_out.as_mut_ptr()) } as usize;
    check_escaped_eq(&s[..1], &c_out, c_len);
}
