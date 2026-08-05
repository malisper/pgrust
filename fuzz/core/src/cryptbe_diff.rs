//! crypt_be_diff: differential fuzz driver — shipped Rust
//! `crypt` (crates/backend/libpq/crypt) vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C (csrc/cryptbe/pg_cryptbe_io.c +
//! whole verbatim saslprep.c/unicode_norm.c, assembled by
//! csrc/gen/assemble_cryptbe.sh).
//!
//! Census carve (unit of record): file-grain crypt.c MINUS
//! get_role_password — the syscache rolpassword lookup + rolvaliduntil
//! clock read are catalog/session environment, OUT of the unit's
//! denominator; it is neither vendored nor driven here. IN:
//! get_password_type / encrypt_password / md5_crypt_verify /
//! plain_crypt_verify, pure over (role, password, salt) bytes given the
//! two pinned GUCs and the pinned salt.
//!
//! Comparison planes:
//!   1. value — the encrypted secret bytes / STATUS_OK-STATUS_ERROR
//!      verdict / PasswordType discriminant, bit-exact;
//!   2. error-verdict + errcode/sqlstate — C oracle errclass 1 <-> 54000
//!      (program_limit_exceeded), 2 <-> XX000 (internal_error, C's
//!      implicit elog(ERROR) code); message text out of scope;
//!   3. warning — C errclass 3 <-> Rust WARNING with sqlstate 01P01
//!      (deprecated_feature), captured via elog's emit-log hook; fired-ness
//!      AND code compared (the md5_password_warnings pin gates it);
//!   4. logdetail — the C psprintf logdetail STRING compared byte-for-byte
//!      with the Rust `logdetail` Option (identical format strings; _() is
//!      identity in the shim), None <-> NULL.
//!
//! stub pins (per fuzz/STUBS.md, all derived once from leading input
//! bytes, both sides):
//!   - stub:guc `md5_password_warnings` (bit0 of the pin byte; Rust = the
//!     crypt session cell via its GUC accessor, C = pg_stub channel);
//!   - stub:guc `scram_iterations` (pinned to 1..=64 or the boot 4096 —
//!     fuzz-domain bound: PBKDF2 cost is linear in the count; the
//!     iteration-count PLUMBING, parse and re-emission is the compared
//!     surface, and seeds cover the 4096 boot value);
//!   - stub:prng scram salt — the 16-byte pg_strong_random read inside
//!     pg_be_scram_build_secret; C = pg_stub_scram_salt, Rust = the
//!     shipped PGRUST_SCRAM_FIXED_SALT_B64 determinism hook (the real seam
//!     the shipped code reads).
//!
//! Input layout: [sel][pins][iters][salt16][saltsel][role u8-len][role]
//! [s1 u16-len][s1][s2 = rest]; sel % 7 picks the arm:
//!   0 get_password_type(s1)
//!   1 encrypt_password(MD5, role, s1)
//!   2 encrypt_password(SCRAM-SHA-256, role, s1) under pinned salt+iters
//!   3 encrypt_password(PLAINTEXT, role, s1) — the elog(ERROR) arm when s1
//!     is plaintext; pass-through (+ possible too-long / md5-warning
//!     planes) when s1 is already an md5/scram secret
//!   4 md5_crypt_verify(role, s1, s2, salt[..1+saltsel%16])
//!   5 plain_crypt_verify(role, s1, s2)
//!   6 round-trip: secret = encrypt_password(MD5|SCRAM by pin bit1, role,
//!     s1) then plain_crypt_verify(role, secret, s1) — compared on both
//!     sides at every step (agreement witness: when s1 was plaintext the
//!     verify MUST be STATUS_OK on both sides)
//!
//! Fuzz-domain bounds (documented, both sides see identical inputs):
//!   - role/s1/s2 are VALID UTF-8 without interior NUL: the shipped crate
//!     API takes &str (Rust-level CString/str constraint) and the C oracle
//!     takes NUL-terminated char*. Non-UTF8 shadow secrets cannot reach
//!     crypt.c through pgrust (rolpassword is text). saslprep's
//!     invalid-UTF8 arm is auth_scram/saslprep coverage, not this unit's.
//!   - strings capped at 4 KiB (MAX_ENCRYPTED_PASSWORD_LEN is 512; 8x
//!     headroom keeps the too-long arm hot without wasting execs).
//!   - md5_salt is 1..=16 bytes (never empty: C Assert(md5_salt_len > 0)
//!     is compiled out in release; Rust carries a release-effective
//!     assert; the live protocol always sends 4 bytes).
//!   - scram_iterations pinned to {1..=64, 4096} (see stub pins above).
//!
//! SKIPPED / one-sided notes (recorded per the fuzzuproof-crate exception
//! rules):
//!   - C ereport(LOG) "invalid SCRAM secret for user" inside
//!     scram_verify_plain_password is UNREACHABLE from plain_crypt_verify:
//!     it is guarded by get_password_type == SCRAM, i.e. a successful
//!     parse of the same string, and parsing is deterministic. The oracle
//!     still RECORDS it (pg_cryptbe_logfired_get) and the driver treats a
//!     fired LOG as a divergence-grade surprise (executes-witness for the
//!     "impossible" C branch).
//!   - pg_md5_encrypt/scram key derivation failure arms are OOM/engine
//!     failure only (frontend cryptohash): C wrapper reports -100-class,
//!     driver aborts as harness-fatal; the Rust sides cannot fail. No
//!     reachable behavior is carved.

use std::cell::Cell;
use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::Once;

use crypt::{PasswordType, STATUS_ERROR, STATUS_OK};
use types_error::{
    PgError, SqlState, ERRCODE_INTERNAL_ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_WARNING_DEPRECATED_FEATURE, WARNING,
};

use crate::stubs;

extern "C" {
    fn pg_cryptbe_reset();
    fn pg_cryptbe_warnclass_get() -> i32;
    fn pg_cryptbe_logfired_get() -> i32;
    fn pg_cryptbe_w_get_password_type(shadow_pass: *const c_char) -> i32;
    fn pg_cryptbe_w_encrypt_password(
        target_type: i32,
        role: *const c_char,
        password: *const c_char,
        out: *mut c_char,
        cap: usize,
    ) -> i32;
    fn pg_cryptbe_w_md5_crypt_verify(
        role: *const c_char,
        shadow_pass: *const c_char,
        client_pass: *const c_char,
        md5_salt: *const u8,
        md5_salt_len: i32,
        logdetail_out: *mut *const c_char,
    ) -> i32;
    fn pg_cryptbe_w_plain_crypt_verify(
        role: *const c_char,
        shadow_pass: *const c_char,
        client_pass: *const c_char,
        logdetail_out: *mut *const c_char,
    ) -> i32;
}

/// Oracle errcode classes (csrc/cryptbe/pg_cryptbe_io.c).
const C_ECLASS_PROGRAM_LIMIT: i32 = 1;
const C_ECLASS_INTERNAL: i32 = 2;
const C_ECLASS_DEPRECATED: i32 = 3;

const MAX_STR: usize = 4096;

thread_local! {
    static WARN_SQLSTATE: Cell<Option<SqlState>> = const { Cell::new(None) };
}

fn emit_hook(e: &PgError, output_to_server: &mut bool) {
    if e.level == WARNING {
        WARN_SQLSTATE.with(|c| c.set(Some(e.sqlstate)));
    }
    // Nothing renders the log line; the plane is the capture above.
    *output_to_server = false;
}

fn ensure_thread_hook() {
    thread_local! {
        static HOOK_SET: Cell<bool> = const { Cell::new(false) };
    }
    HOOK_SET.with(|f| {
        if !f.get() {
            // The emit-log-hook slot is thread-local; install per thread
            // (one thread per fuzz process; every test thread needs its own).
            elog::set_emit_log_hook(Some(emit_hook));
            f.set(true);
        }
    });
}

fn init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Idempotence guards: the shared cargo test binary may host other
        // modules' installs of the same seams.
        let _ = std::panic::catch_unwind(elog::init_seams);
        let _ = std::panic::catch_unwind(crypt::init_seams);
        let _ = std::panic::catch_unwind(auth_scram::init_seams);
        if !postgres_seams::check_for_interrupts::is_installed() {
            let _ =
                std::panic::catch_unwind(|| postgres_seams::check_for_interrupts::set(|| Ok(())));
        }
    });
    ensure_thread_hook();
}

/// Map a C oracle ERROR class to the sqlstate the Rust side must carry.
fn eclass_sqlstate(class: i32) -> SqlState {
    match class {
        C_ECLASS_PROGRAM_LIMIT => ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        C_ECLASS_INTERNAL => ERRCODE_INTERNAL_ERROR,
        other => panic!("crypt_be_diff: unknown C errclass {other}"),
    }
}

fn c_logdetail(p: *const c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    let s = unsafe { std::ffi::CStr::from_ptr(p) };
    Some(String::from_utf8_lossy(s.to_bytes()).into_owned())
}

fn take_rust_warn() -> Option<SqlState> {
    WARN_SQLSTATE.with(|c| c.replace(None))
}

/// Compare the warning plane after any paired call.
fn check_warn_plane(ctx: &str) {
    let cwarn = unsafe { pg_cryptbe_warnclass_get() };
    let rwarn = take_rust_warn();
    match (cwarn, rwarn) {
        (0, None) => {}
        (c, Some(ss)) if c == C_ECLASS_DEPRECATED && ss == ERRCODE_WARNING_DEPRECATED_FEATURE => {}
        (c, r) => panic!("crypt_be_diff[{ctx}]: warning plane diverged: C class {c}, Rust {r:?}"),
    }
    // Executes-witness for the "unreachable" C LOG branch (see header).
    assert!(
        unsafe { pg_cryptbe_logfired_get() } == 0,
        "crypt_be_diff[{ctx}]: C ereport(LOG) branch fired — supposedly unreachable"
    );
}

struct Cstrs {
    role: CString,
    s1: CString,
    s2: CString,
}

/// Parse the string section. Returns None (early exit, documented
/// fuzz-domain bound) on non-UTF8 or interior-NUL material.
fn parse_strings(data: &[u8]) -> Option<(Cstrs, String, String, String)> {
    let (&role_len, rest) = data.split_first()?;
    let role_len = role_len as usize % 65;
    if rest.len() < role_len + 2 {
        return None;
    }
    let (role_b, rest) = rest.split_at(role_len);
    let (l, rest) = rest.split_at(2);
    let s1_len = (u16::from_le_bytes([l[0], l[1]]) as usize).min(MAX_STR);
    if rest.len() < s1_len {
        return None;
    }
    let (s1_b, s2_b) = rest.split_at(s1_len);
    let s2_b = &s2_b[..s2_b.len().min(MAX_STR)];

    let role = std::str::from_utf8(role_b).ok()?.to_owned();
    let s1 = std::str::from_utf8(s1_b).ok()?.to_owned();
    let s2 = std::str::from_utf8(s2_b).ok()?.to_owned();
    let c = Cstrs {
        role: CString::new(role.as_bytes()).ok()?,
        s1: CString::new(s1.as_bytes()).ok()?,
        s2: CString::new(s2.as_bytes()).ok()?,
    };
    Some((c, role, s1, s2))
}

/// Paired encrypt_password call: compare value/error/warning planes and
/// return the secret when BOTH sides succeeded.
fn diff_encrypt(
    target: PasswordType,
    c: &Cstrs,
    role: &str,
    password: &str,
    ctx: &str,
) -> Option<String> {
    let mut out = vec![0u8; MAX_ENCRYPTED_OUT];
    let rc = unsafe {
        pg_cryptbe_w_encrypt_password(
            target as i32,
            c.role.as_ptr(),
            c.s1.as_ptr(),
            out.as_mut_ptr().cast(),
            out.len(),
        )
    };
    debug_assert_eq!(password, unsafe {
        std::ffi::CStr::from_ptr(c.s1.as_ptr()).to_str().unwrap()
    });

    let cx = mcx::MemoryContext::new("crypt_be_diff");
    let r = crypt::encrypt_password(cx.mcx(), target, role, password);

    let result = match (rc, &r) {
        (0, Ok(secret)) => {
            let nul = out.iter().position(|&b| b == 0).unwrap();
            assert_eq!(
                secret.as_str().as_bytes(),
                &out[..nul],
                "crypt_be_diff[{ctx}]: secret bytes diverged (C {:?} vs Rust {:?})",
                String::from_utf8_lossy(&out[..nul]),
                secret.as_str(),
            );
            Some(secret.as_str().to_owned())
        }
        (rc, Err(e)) if rc < 0 => {
            let want = eclass_sqlstate(-rc);
            assert_eq!(
                e.sqlstate, want,
                "crypt_be_diff[{ctx}]: error sqlstate diverged (C class {}, Rust {:?})",
                -rc, e.sqlstate
            );
            None
        }
        (rc, r) => panic!(
            "crypt_be_diff[{ctx}]: verdict diverged: C rc {rc}, Rust {:?}",
            r.as_ref().map(|s| s.as_str())
        ),
    };
    check_warn_plane(ctx);
    result
}

const MAX_ENCRYPTED_OUT: usize = MAX_STR + 16;

fn diff_plain_verify(c_shadow: &CString, shadow: &str, c: &Cstrs, role: &str, client: &str) -> i32 {
    let mut ld: *const c_char = std::ptr::null();
    let cst = unsafe {
        pg_cryptbe_w_plain_crypt_verify(c.role.as_ptr(), c_shadow.as_ptr(), c.s2.as_ptr(), &mut ld)
    };
    debug_assert_eq!(client, unsafe {
        std::ffi::CStr::from_ptr(c.s2.as_ptr()).to_str().unwrap()
    });
    assert!(cst >= STATUS_ERROR, "crypt_be_diff[plain]: C oracle ereport(ERROR) {cst} — OOM-shaped, harness-fatal");

    let cx = mcx::MemoryContext::new("crypt_be_diff");
    let mut rld = None;
    let rst = crypt::plain_crypt_verify(cx.mcx(), role, shadow, client, &mut rld)
        .unwrap_or_else(|e| panic!("crypt_be_diff[plain]: Rust errored {:?}", e.sqlstate));
    assert_eq!(cst, rst, "crypt_be_diff[plain]: status diverged (C {cst} vs Rust {rst})");
    assert_eq!(
        c_logdetail(ld),
        rld,
        "crypt_be_diff[plain]: logdetail diverged"
    );
    check_warn_plane("plain");
    rst
}

pub fn crypt_be_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init();
    if data.len() < 20 {
        return;
    }
    let sel = data[0];
    let pins = data[1];
    let iters_b = data[2];
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&data[3..19]);
    let saltsel = data[19];

    // Declare the pins (both sides) before anything runs.
    stubs::guc::pin_md5_password_warnings(pins);
    stubs::guc::pin_scram_iterations(iters_b);
    stubs::prng::pin_scram_salt(salt);
    unsafe { pg_cryptbe_reset() };
    let _ = take_rust_warn();

    let Some((c, role, s1, s2)) = parse_strings(&data[20..]) else {
        return;
    };

    match sel % 7 {
        0 => {
            let ct = unsafe { pg_cryptbe_w_get_password_type(c.s1.as_ptr()) };
            let rt = crypt::get_password_type(&s1) as i32;
            assert_eq!(ct, rt, "crypt_be_diff[gpt]: PasswordType diverged for {s1:?}");
            check_warn_plane("gpt");
        }
        1 => {
            diff_encrypt(PasswordType::Md5, &c, &role, &s1, "enc-md5");
        }
        2 => {
            diff_encrypt(PasswordType::ScramSha256, &c, &role, &s1, "enc-scram");
        }
        3 => {
            diff_encrypt(PasswordType::Plaintext, &c, &role, &s1, "enc-plain");
        }
        4 => {
            let salt_len = 1 + (saltsel as usize % 16);
            let mut ld: *const c_char = std::ptr::null();
            let cst = unsafe {
                pg_cryptbe_w_md5_crypt_verify(
                    c.role.as_ptr(),
                    c.s1.as_ptr(),
                    c.s2.as_ptr(),
                    salt.as_ptr(),
                    salt_len as i32,
                    &mut ld,
                )
            };
            assert!(cst >= STATUS_ERROR, "crypt_be_diff[md5v]: C oracle ereport(ERROR) {cst}");
            let mut rld = None;
            let rst = crypt::md5_crypt_verify(&role, &s1, &s2, &salt[..salt_len], &mut rld)
                .unwrap_or_else(|e| panic!("crypt_be_diff[md5v]: Rust errored {:?}", e.sqlstate));
            assert_eq!(cst, rst, "crypt_be_diff[md5v]: status diverged");
            assert_eq!(c_logdetail(ld), rld, "crypt_be_diff[md5v]: logdetail diverged");
            check_warn_plane("md5v");
        }
        5 => {
            diff_plain_verify(&c.s1, &s1, &c, &role, &s2);
        }
        6 => {
            // Round-trip: encrypt s1, then verify s1 against the secret.
            let target = if pins & 2 != 0 {
                PasswordType::ScramSha256
            } else {
                PasswordType::Md5
            };
            let was_plaintext = crypt::get_password_type(&s1) == PasswordType::Plaintext;
            let Some(secret) = diff_encrypt(target, &c, &role, &s1, "rt-enc") else {
                return;
            };
            let c_secret = CString::new(secret.as_bytes()).unwrap();
            // s2 plays the client password: use s1 itself (must verify OK
            // when s1 was plaintext) — the single-field witness seeds
            // supply the mismatch halves.
            let c2 = Cstrs {
                role: c.role.clone(),
                s1: c_secret.clone(),
                s2: c.s1.clone(),
            };
            let st = diff_plain_verify(&c_secret, &secret, &c2, &role, &s1);
            if was_plaintext {
                assert_eq!(
                    st, STATUS_OK,
                    "crypt_be_diff[rt]: round-trip verify must succeed (BOTH sides agreed on {st})"
                );
            }
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scram-salt pin rides a process-global env var while everything
    /// else is thread-local: tests that pin must not interleave.
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// fuzzer runs; also the CI regression rail for banked divergences).
    #[test]
    fn replay_corpus() {
        let _g = env_guard();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/crypt_be_diff");
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        let mut n = 0;
        for e in rd {
            let p = e.unwrap().path();
            if p.is_file() {
                crypt_be_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        eprintln!("replayed {n} crypt_be_diff seeds");
    }

    fn exec(sel: u8, pins: u8, iters: u8, role: &str, s1: &str, s2: &str) {
        let mut v = vec![sel, pins, iters];
        v.extend_from_slice(&[7u8; 16]); // salt
        v.push(3); // saltsel
        v.push(role.len() as u8);
        v.extend_from_slice(role.as_bytes());
        v.extend_from_slice(&(s1.len() as u16).to_le_bytes());
        v.extend_from_slice(s1.as_bytes());
        v.extend_from_slice(s2.as_bytes());
        crypt_be_diff(&v);
    }

    const MD5_SECRET: &str = "md553f48b7c4b76a86ce72276c5755f217d";

    /// Smoke: every arm runs green on representative inputs (the compare
    /// planes are the asserts inside the driver).
    #[test]
    fn arms_smoke() {
        let _g = env_guard();
        for sel in 0..7 {
            for pins in [0u8, 1, 2, 3] {
                exec(sel, pins, 0xFF, "postgres", "secret", "hunter2");
                exec(sel, pins, 5, "r", MD5_SECRET, MD5_SECRET);
                exec(sel, pins, 5, "", "", "");
            }
        }
    }

    /// Must-fail control for the md5_password_warnings pin (STUBS.md law):
    /// (a) parity under matched pins; (b) a deliberate one-sided mismatch
    /// MUST be seen by the warning plane.
    #[test]
    fn control_guc_md5_password_warnings_pin() {
        // Lock order everywhere in this file: env_guard() FIRST, then the
        // oracle guard (the driver takes oracle_serial() under the same
        // ordering when exec() is called with env_guard held).
        let _g = env_guard();
        let _oracle = crate::c_oracle_serial();
        init();
        // (a) matched: warnings ON, md5 target — both sides warn (the
        // driver's check_warn_plane would panic on divergence).
        exec(1, 1, 0xFF, "postgres", "secret", "");
        // (b) mismatch: pin OFF both sides, then flip ONLY the C side back
        // on. The C oracle warns, Rust does not — the plane must catch it.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::stubs::guc::pin_md5_password_warnings(0);
            unsafe { crate::stubs::craw::pg_stub_set_md5_password_warnings(1) };
            let c = CString::new("postgres").unwrap();
            let pw = CString::new("secret").unwrap();
            let mut out = vec![0u8; MAX_ENCRYPTED_OUT];
            unsafe {
                pg_cryptbe_reset();
                pg_cryptbe_w_encrypt_password(
                    PasswordType::Md5 as i32,
                    c.as_ptr(),
                    pw.as_ptr(),
                    out.as_mut_ptr().cast(),
                    out.len(),
                );
            }
            let cx = mcx::MemoryContext::new("ctl");
            let _ = take_rust_warn();
            let _ = crypt::encrypt_password(cx.mcx(), PasswordType::Md5, "postgres", "secret");
            check_warn_plane("ctl-md5warn");
        }))
        .is_err();
        assert!(caught, "md5_password_warnings pin is DEAD: one-sided mismatch not detected");
    }

    /// Must-fail control for the scram salt pin: (a) parity — the pinned
    /// salt round-trips into byte-identical secrets; (b) a one-sided salt
    /// change MUST flip the compared secret bytes.
    #[test]
    fn control_prng_scram_salt_pin() {
        let _g = env_guard();
        let _oracle = crate::c_oracle_serial();
        init();
        // (a) matched pins through the full differential arm.
        exec(2, 1, 0xFF, "scramuser", "secret", "");
        // (b) mismatch: pin salt A both sides, then set ONLY the C channel
        // to salt B — the secret byte plane must catch it.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut v = vec![2u8, 1, 0xFF];
            v.extend_from_slice(&[9u8; 16]);
            v.push(0);
            v.push(1);
            v.extend_from_slice(b"u");
            v.extend_from_slice(&6u16.to_le_bytes());
            v.extend_from_slice(b"secret");
            // Pre-poison the C channel AFTER the driver pins by running the
            // mismatch manually:
            crate::stubs::prng::pin_scram_salt([9u8; 16]);
            unsafe {
                crate::stubs::craw::pg_stub_set_scram_salt([1u8; 16].as_ptr())
            };
            let c = CString::new("u").unwrap();
            let pw = CString::new("secret").unwrap();
            let mut out = vec![0u8; MAX_ENCRYPTED_OUT];
            let rc = unsafe {
                pg_cryptbe_reset();
                pg_cryptbe_w_encrypt_password(
                    PasswordType::ScramSha256 as i32,
                    c.as_ptr(),
                    pw.as_ptr(),
                    out.as_mut_ptr().cast(),
                    out.len(),
                )
            };
            assert_eq!(rc, 0);
            let cx = mcx::MemoryContext::new("ctl");
            let secret = crypt::encrypt_password(cx.mcx(), PasswordType::ScramSha256, "u", "secret")
                .unwrap();
            let nul = out.iter().position(|&b| b == 0).unwrap();
            assert_eq!(secret.as_str().as_bytes(), &out[..nul], "secret bytes diverged");
        }))
        .is_err();
        assert!(caught, "scram salt pin is DEAD: one-sided mismatch not detected");
    }

    /// Must-fail control for the scram_iterations pin.
    #[test]
    fn control_guc_scram_iterations_pin() {
        let _g = env_guard();
        let _oracle = crate::c_oracle_serial();
        init();
        exec(2, 1, 17, "scramuser", "secret", "");
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::stubs::guc::pin_scram_iterations(9);
            crate::stubs::prng::pin_scram_salt([4u8; 16]);
            // pin(9) derives 1+9%64 = 10; poison the C side with a value
            // OUTSIDE the pin's derivation image of this byte.
            unsafe { crate::stubs::craw::pg_stub_set_scram_iterations(999) };
            let c = CString::new("u").unwrap();
            let pw = CString::new("pw").unwrap();
            let mut out = vec![0u8; MAX_ENCRYPTED_OUT];
            let rc = unsafe {
                pg_cryptbe_reset();
                pg_cryptbe_w_encrypt_password(
                    PasswordType::ScramSha256 as i32,
                    c.as_ptr(),
                    pw.as_ptr(),
                    out.as_mut_ptr().cast(),
                    out.len(),
                )
            };
            assert_eq!(rc, 0);
            let cx = mcx::MemoryContext::new("ctl");
            let secret =
                crypt::encrypt_password(cx.mcx(), PasswordType::ScramSha256, "u", "pw").unwrap();
            let nul = out.iter().position(|&b| b == 0).unwrap();
            assert_eq!(secret.as_str().as_bytes(), &out[..nul], "secret bytes diverged");
        }))
        .is_err();
        assert!(caught, "scram_iterations pin is DEAD: one-sided mismatch not detected");
    }
}
