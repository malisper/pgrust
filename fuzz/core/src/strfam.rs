//! Target: strfam_diff — the p1-lanec string-family batch (common/string,
//! common/archive, common/percentrepl, common/relpath, common/wait_error)
//! shipped Rust vs vendored PostgreSQL 18.3 C (csrc/pg_strfam.c) in-process.
//!
//! Comparison planes (harness contract): value bytes + error-verdict +
//! errcode/sqlstate. Any mismatch panics, so a libFuzzer crash artifact is a
//! C/Rust divergence reproducer.
//!
//! Domain carves (documented, ratified non-surfaces):
//!   - C strings cannot represent interior NUL: every text input is truncated
//!     at the first NUL before BOTH sides (the shipped Rust helpers that take
//!     &[u8] apply the same TextDatumGetCString truncation themselves;
//!     &str-taking entry points are fed the truncated prefix).
//!   - &str-taking entry points (pg_clean_ascii, percentrepl, archive) have a
//!     type-enforced valid-UTF-8 domain; non-UTF-8 fuzz bytes are skipped for
//!     them (the C side accepts any bytes, but the shipped Rust surface
//!     cannot be called with them — domain difference is the TYPE, not logic).
//!   - GetRelationPath: fork pinned to the 4 valid forks and the
//!     (global => dbOid==0, procNumber==INVALID) invariants held — the C
//!     build compiles those Asserts out but indexes forkNames[] with the fork
//!     number, so an invalid fork is C UB, not a comparable behavior.
//!   - wait_result_to_str(-1): reads errno; both sides get the same pinned
//!     errno value (plumbing pin, not a computation mock).
//!   - strsignal/strerror message text: both sides call the SAME host libc in
//!     the same (default C) locale, so full-string compare is exact; this is
//!     an on-host equivalence claim, ground-truthed vs postgres:18.3 Docker.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use mcx::MemoryContext;
use types_core::{ForkNumber, INVALID_PROC_NUMBER, MAIN_FORKNUM};
use types_error::ERRCODE_INVALID_PARAMETER_VALUE;
use types_storage::RelFileLocator;

extern "C" {
    fn pg_diff_strtoint10_strict(s: *const c_char, out: *mut i32) -> c_int;
    fn pg_diff_strtoul_base0(s: *const c_char, consumed: *mut usize, range_err: *mut c_int)
        -> u64;
    fn pg_diff_clean_ascii(s: *const c_char) -> *mut c_char;
    fn pg_diff_percentrepl(
        instr: *const c_char,
        param_name: *const c_char,
        letters: *const c_char,
        v0: *const c_char,
        v1: *const c_char,
        v2: *const c_char,
        out: *mut *mut c_char,
    ) -> c_int;
    fn pg_diff_build_restore_command(
        cmd: *const c_char,
        xlogpath: *const c_char,
        xlogfname: *const c_char,
        restartname: *const c_char,
        out: *mut *mut c_char,
    ) -> c_int;
    fn pg_diff_get_database_path(db_oid: u32, spc_oid: u32) -> *mut c_char;
    fn pg_diff_get_relation_path(
        db_oid: u32,
        spc_oid: u32,
        rel_number: u32,
        proc_number: c_int,
        fork_number: c_int,
        out: *mut c_char,
    );
    fn pg_diff_wait_result_to_str(exitstatus: c_int, errno_pin: c_int) -> *mut c_char;
    fn pg_diff_wait_result_is_signal(exit_status: c_int, signum: c_int) -> c_int;
    fn pg_diff_wait_result_is_any_signal(exit_status: c_int, include_cnf: c_int) -> c_int;
    fn pg_diff_wait_result_to_exit_code(exit_status: c_int) -> c_int;
    fn pg_diff_pg_strsignal(signum: c_int) -> *const c_char;
    fn pg_diff_wifexited(status: c_int) -> c_int;
    fn pg_diff_wexitstatus(status: c_int) -> c_int;
    fn pg_diff_wifsignaled(status: c_int) -> c_int;
    fn pg_diff_wtermsig(status: c_int) -> c_int;
    fn pg_diff_isspace(c: c_int) -> c_int;
    fn free(p: *mut std::ffi::c_void);
}

/* C errcode ids in pg_strfam.c */
const C_ERR_INVALID_PARAMETER_VALUE: c_int = 3;

/// NUL-truncate (the C-string domain; see header) and CString it.
fn cstr(bytes: &[u8]) -> CString {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    CString::new(&bytes[..end]).expect("truncated at first NUL")
}

fn take_c_string(p: *mut c_char) -> String {
    assert!(!p.is_null(), "C oracle returned NULL");
    // SAFETY: NUL-terminated malloc'd string from the oracle.
    let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    unsafe { free(p.cast()) };
    s
}

fn set_errno(v: i32) {
    #[cfg(target_os = "macos")]
    // SAFETY: __error() is the per-thread errno slot.
    unsafe {
        *libc::__error() = v;
    }
    #[cfg(target_os = "linux")]
    // SAFETY: __errno_location() is the per-thread errno slot.
    unsafe {
        *libc::__errno_location() = v;
    }
}

/// Split `data` into at most N fields on 0xFF (0xFF never appears in valid
/// UTF-8 text, so the separator costs the fuzzer nothing in the text domain).
fn fields<const N: usize>(data: &[u8]) -> [&[u8]; N] {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut it = data.splitn(N, |&b| b == 0xFF);
    for slot in out.iter_mut() {
        match it.next() {
            Some(f) => *slot = f,
            None => break,
        }
    }
    out
}

fn diff_strtoint10(payload: &[u8]) {
    let c_in = cstr(payload);
    let mut c_val: i32 = 0;
    // SAFETY: NUL-terminated input; out is a valid i32 slot.
    let c_ok = unsafe { pg_diff_strtoint10_strict(c_in.as_ptr(), &mut c_val) } != 0;
    let rust = pg_string::strtoint10_strict(payload);
    match (c_ok, rust) {
        (true, Some(r)) => assert_eq!(r, c_val, "strtoint10 value diverged"),
        (false, None) => {}
        (c, r) => panic!("strtoint10 verdict diverged: C accepted={c}, Rust={r:?}"),
    }
}

fn diff_strtoul_base0(payload: &[u8]) {
    let c_in = cstr(payload);
    let mut consumed: usize = 0;
    let mut range_err: c_int = 0;
    // SAFETY: NUL-terminated input; out params valid.
    let c_val = unsafe { pg_diff_strtoul_base0(c_in.as_ptr(), &mut consumed, &mut range_err) };
    let r = pg_string::strtoul_base0(payload);
    assert_eq!(r.value, c_val, "strtoul_base0 value diverged");
    assert_eq!(r.consumed, consumed, "strtoul_base0 endptr diverged");
    assert_eq!(r.range_err, range_err != 0, "strtoul_base0 ERANGE diverged");
}

fn diff_clean_ascii(payload: &[u8]) {
    let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
    let Ok(s) = std::str::from_utf8(&payload[..end]) else {
        return; // &str domain (see header)
    };
    let c_in = CString::new(s).expect("truncated at first NUL");
    // SAFETY: NUL-terminated input.
    let c_out = take_c_string(unsafe { pg_diff_clean_ascii(c_in.as_ptr()) });
    let r = pg_string::pg_clean_ascii(s, 0).expect("Rust side is infallible");
    assert_eq!(r, c_out, "pg_clean_ascii diverged");
}

/// letters "frp" with per-slot nullability driven by a control byte, matching
/// the BuildRestoreCommand call shape (the family's only production letter
/// spec) plus NULL-value arms.
fn diff_percentrepl(ctl: u8, payload: &[u8]) {
    let [instr, f, r, p] = fields::<4>(payload);
    let end = instr.iter().position(|&b| b == 0).unwrap_or(instr.len());
    let Ok(instr_s) = std::str::from_utf8(&instr[..end]) else {
        return;
    };
    let (Ok(f_s), Ok(r_s), Ok(p_s)) = (
        std::str::from_utf8(f),
        std::str::from_utf8(r),
        std::str::from_utf8(p),
    ) else {
        return;
    };
    if f.contains(&0) || r.contains(&0) || p.contains(&0) {
        return; // value slots are C strings too
    }
    let f_opt = (ctl & 1 == 0).then_some(f_s);
    let r_opt = (ctl & 2 == 0).then_some(r_s);
    let p_opt = (ctl & 4 == 0).then_some(p_s);

    let c_instr = CString::new(instr_s).unwrap();
    let c_f = f_opt.map(|s| CString::new(s).unwrap());
    let c_r = r_opt.map(|s| CString::new(s).unwrap());
    let c_p = p_opt.map(|s| CString::new(s).unwrap());
    let name = CString::new("restore_command").unwrap();
    let letters = CString::new("frp").unwrap();
    let null = std::ptr::null();
    let mut c_out: *mut c_char = std::ptr::null_mut();
    // SAFETY: all pointers NUL-terminated or NULL; out slot valid.
    let c_rc = unsafe {
        pg_diff_percentrepl(
            c_instr.as_ptr(),
            name.as_ptr(),
            letters.as_ptr(),
            c_f.as_ref().map_or(null, |s| s.as_ptr()),
            c_r.as_ref().map_or(null, |s| s.as_ptr()),
            c_p.as_ref().map_or(null, |s| s.as_ptr()),
            &mut c_out,
        )
    };

    let values = [('f', f_opt), ('r', r_opt), ('p', p_opt)];
    let rust = percentrepl::replace_percent_placeholders(instr_s, "restore_command", &values);
    match (c_rc, rust) {
        (0, Ok(r)) => assert_eq!(r, take_c_string(c_out), "percentrepl value diverged"),
        (C_ERR_INVALID_PARAMETER_VALUE, Err(e)) => assert_eq!(
            e.sqlstate(),
            ERRCODE_INVALID_PARAMETER_VALUE,
            "percentrepl errcode diverged"
        ),
        (c, r) => panic!("percentrepl verdict diverged: C rc={c}, Rust={r:?}"),
    }
}

fn diff_build_restore_command(payload: &[u8]) {
    let [cmd, path, fname, restart] = fields::<4>(payload);
    let end = cmd.iter().position(|&b| b == 0).unwrap_or(cmd.len());
    let Ok(cmd_s) = std::str::from_utf8(&cmd[..end]) else {
        return;
    };
    let (Ok(path_s), Ok(fname_s), Ok(restart_s)) = (
        std::str::from_utf8(path),
        std::str::from_utf8(fname),
        std::str::from_utf8(restart),
    ) else {
        return;
    };
    if path.contains(&0) || fname.contains(&0) || restart.contains(&0) {
        return;
    }
    let c_cmd = CString::new(cmd_s).unwrap();
    let c_path = CString::new(path_s).unwrap();
    let c_fname = CString::new(fname_s).unwrap();
    let c_restart = CString::new(restart_s).unwrap();
    let mut c_out: *mut c_char = std::ptr::null_mut();
    // SAFETY: all pointers NUL-terminated; out slot valid.
    let c_rc = unsafe {
        pg_diff_build_restore_command(
            c_cmd.as_ptr(),
            c_path.as_ptr(),
            c_fname.as_ptr(),
            c_restart.as_ptr(),
            &mut c_out,
        )
    };
    let rust = archive::BuildRestoreCommand(cmd_s, path_s, fname_s, restart_s);
    match (c_rc, rust) {
        (0, Ok(r)) => assert_eq!(r, take_c_string(c_out), "BuildRestoreCommand diverged"),
        (C_ERR_INVALID_PARAMETER_VALUE, Err(e)) => assert_eq!(
            e.sqlstate(),
            ERRCODE_INVALID_PARAMETER_VALUE,
            "BuildRestoreCommand errcode diverged"
        ),
        (c, r) => panic!("BuildRestoreCommand verdict diverged: C rc={c}, Rust={r:?}"),
    }
}

/// One-time seam registration through the SHIPPED init_seams entry points —
/// the seam-dispatch wrappers (relpathbackend/relpathperm) are then exercised
/// below through the seam, exactly as backend callers reach them.
fn init_seams_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        pg_string::init_seams();
        relpath::init_seams();
    });
}

fn diff_relpath(payload: &[u8]) {
    init_seams_once();
    if payload.len() < 14 {
        return;
    }
    let u32_at = |i: usize| u32::from_le_bytes(payload[i..i + 4].try_into().unwrap());
    let mut db_oid = u32_at(0);
    // Bias the tablespace draw toward the three C branches.
    let spc_oid = match payload[13] % 4 {
        0 => relpath::GLOBALTABLESPACE_OID,
        1 => relpath::DEFAULTTABLESPACE_OID,
        _ => u32_at(4),
    };
    let rel_number = u32_at(8);
    let mut proc_number: i32 = match payload[12] % 3 {
        0 => INVALID_PROC_NUMBER,
        1 => (payload[12] as i32) * 7 + 1,
        _ => i32::from_le_bytes(payload[4..8].try_into().unwrap()).max(0),
    };
    let fork_byte = payload[12] % 4;
    let fork = match fork_byte {
        0 => ForkNumber::MAIN_FORKNUM,
        1 => ForkNumber::FSM_FORKNUM,
        2 => ForkNumber::VISIBILITYMAP_FORKNUM,
        _ => ForkNumber::INIT_FORKNUM,
    };
    if spc_oid == relpath::GLOBALTABLESPACE_OID {
        // C Asserts (compiled out) = domain invariants (see header).
        db_oid = 0;
        proc_number = INVALID_PROC_NUMBER;
    }

    let mut c_buf = [0u8; 129];
    // SAFETY: fork is one of the 4 valid forks; out buffer holds
    // REL_PATH_STR_MAXLEN + 1 bytes.
    unsafe {
        pg_diff_get_relation_path(
            db_oid,
            spc_oid,
            rel_number,
            proc_number,
            fork_byte as c_int,
            c_buf.as_mut_ptr().cast(),
        )
    };
    let c_path = CStr::from_bytes_until_nul(&c_buf).unwrap().to_str().unwrap();
    // forkname_chars over the drawn fork (all four valid cells; the MAIN cell
    // is never reached through GetRelationPath, which skips the suffix).
    // Byte-equivalence to the C forkNames table is the Kani theorem
    // (eq_forkname_chars_*); the executed sanity floor here is non-emptiness.
    assert!(!relpath::forkname_chars(fork).is_empty());

    let rloc = RelFileLocator::new(spc_oid, db_oid, rel_number);
    let r_path = relpath::GetRelationPath(rloc, proc_number, fork);
    assert_eq!(r_path, c_path, "GetRelationPath diverged");

    // The shipped seam-dispatch wrappers over the same draw (relpathbackend /
    // relpathperm are how backend callers reach GetRelationPath).
    assert_eq!(
        relpath_seams::relpathbackend::call(rloc, proc_number, fork),
        c_path,
        "relpathbackend seam diverged"
    );
    if proc_number == INVALID_PROC_NUMBER {
        assert_eq!(
            relpath_seams::relpathperm::call(rloc, fork),
            c_path,
            "relpathperm seam diverged"
        );
    }

    // GetDatabasePath over the same draw (global => dbOid 0 held above).
    let ctx = MemoryContext::new("strfam_diff");
    let db = relpath::GetDatabasePath(ctx.mcx(), db_oid, spc_oid).expect("alloc");
    // SAFETY: plain scalar args.
    let c_db = take_c_string(unsafe { pg_diff_get_database_path(db_oid, spc_oid) });
    assert_eq!(db.as_str(), c_db, "GetDatabasePath diverged");
    let _ = MAIN_FORKNUM; // fork constants linked
}

fn diff_wait_error(payload: &[u8]) {
    if payload.len() < 7 {
        return;
    }
    let mut status = i32::from_le_bytes(payload[0..4].try_into().unwrap());
    let signum = (payload[4] % 65) as i32; // 0..=64 covers all real signals + off-table
    let include_cnf = payload[5] & 1 != 0;
    if payload[5] & 2 != 0 {
        status = -1; // exercise the errno arm deliberately, not 1-in-2^32
    }
    let errno_pin = (payload[6] % 100) as i32 + 1;

    // SAFETY: scalar args only.
    unsafe {
        assert_eq!(
            wait_error::WIFEXITED(status),
            pg_diff_wifexited(status) != 0,
            "WIFEXITED diverged"
        );
        assert_eq!(
            wait_error::WIFSIGNALED(status),
            pg_diff_wifsignaled(status) != 0,
            "WIFSIGNALED diverged"
        );
        assert_eq!(
            wait_error::WEXITSTATUS(status),
            pg_diff_wexitstatus(status),
            "WEXITSTATUS diverged"
        );
        assert_eq!(
            wait_error::WTERMSIG(status),
            pg_diff_wtermsig(status),
            "WTERMSIG diverged"
        );
        assert_eq!(
            wait_error::wait_result_is_signal(status, signum),
            pg_diff_wait_result_is_signal(status, signum) != 0,
            "wait_result_is_signal diverged"
        );
        assert_eq!(
            wait_error::wait_result_is_any_signal(status, include_cnf),
            pg_diff_wait_result_is_any_signal(status, include_cnf as c_int) != 0,
            "wait_result_is_any_signal diverged"
        );
        assert_eq!(
            wait_error::wait_result_to_exit_code(status),
            pg_diff_wait_result_to_exit_code(status),
            "wait_result_to_exit_code diverged"
        );
        let c_sig = CStr::from_ptr(pg_diff_pg_strsignal(signum)).to_string_lossy().into_owned();
        assert_eq!(wait_error::pg_strsignal(signum), c_sig, "pg_strsignal diverged");

        // wait_result_to_str last: both sides under the same pinned errno
        // (Rust reads errno on the -1 arm; the asserts above may clobber it).
        let c_str = pg_diff_wait_result_to_str(status, errno_pin);
        set_errno(errno_pin);
        let r_str = wait_error::wait_result_to_str(status);
        assert_eq!(r_str, take_c_string(c_str), "wait_result_to_str diverged");
    }
}

fn diff_isspace(payload: &[u8]) {
    let Some(&b) = payload.first() else { return };
    // SAFETY: scalar arg.
    let c = unsafe { pg_diff_isspace(b as c_int) } != 0;
    assert_eq!(pg_string::isspace_c_locale(b), c, "isspace_c_locale diverged on 0x{b:02x}");
}

/// Entry: first byte selects the family member (float_in_diff pattern).
pub fn strfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    match sel % 8 {
        0 => diff_strtoint10(rest),
        1 => diff_strtoul_base0(rest),
        2 => diff_clean_ascii(rest),
        3 => {
            let Some((&ctl, rest2)) = rest.split_first() else {
                return;
            };
            diff_percentrepl(ctl, rest2);
        }
        4 => diff_build_restore_command(rest),
        5 => diff_relpath(rest),
        6 => diff_wait_error(rest),
        _ => diff_isspace(rest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CI replay rail: every committed corpus unit replays clean through the
    /// differential on stable (the banked corpus is the regression suite —
    /// any C/Rust divergence or harness panic fails this test per-commit).
    #[test]
    fn strfam_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/strfam_diff");
        let mut n = 0usize;
        for entry in std::fs::read_dir(dir).expect("committed corpus present") {
            let p = entry.unwrap().path();
            if p.is_file() {
                strfam_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n > 1000, "corpus unexpectedly small: {n} units");
    }

    /// Deterministic smoke sweep on stable: drive every selector with
    /// hand-picked inputs covering both verdict planes of each member.
    #[test]
    fn strfam_smoke() {
        let _serial = crate::c_oracle_serial();
        // strtoint10: accept, reject, ERANGE, whitespace, NUL-truncation.
        for s in [
            &b"0\x001"[..],
            b" 42",
            b"\x0b-7",
            b"2147483648",
            b"-2147483649",
            b"1a",
            b"",
            b"+",
            b"007",
        ] {
            let mut d = vec![0u8];
            d.extend_from_slice(s);
            strfam_diff(&d);
        }
        // strtoul base 0: hex, octal, wrap-negation, ERANGE, garbage.
        for s in [
            &b"0x10"[..],
            b"010",
            b"08",
            b"-1",
            b"18446744073709551616",
            b"abc",
            b"0x",
            b" \t0XfF junk",
            b"+5",
            b"12\x0034",
        ] {
            let mut d = vec![1u8];
            d.extend_from_slice(s);
            strfam_diff(&d);
        }
        // pg_clean_ascii: clean, control chars, multibyte UTF-8, DEL.
        for s in [&b"psql"[..], b"a\x1fb", b"\x7f", "café".as_bytes(), b"\t\n"] {
            let mut d = vec![2u8];
            d.extend_from_slice(s);
            strfam_diff(&d);
        }
        // percentrepl: all placeholders, %%, trailing %, unknown %z, NULL slot.
        for (ctl, s) in [
            (0u8, &b"cp %p /a/%f r=%r"[..]),
            (0, b"100%%"),
            (0, b"abc%"),
            (0, b"%z"),
            (4, b"cp %p"), // p NULL => error
            (7, b"plain"),
        ] {
            let mut d = vec![3u8, ctl];
            d.extend_from_slice(s);
            d.extend_from_slice(&[0xFF, b'F', 0xFF, b'R', 0xFF, b'P']);
            strfam_diff(&d);
        }
        // BuildRestoreCommand: full command + error arm.
        for cmd in [&b"cp %p /archive/%f (r %r)"[..], b"cp %z", b"end%"] {
            let mut d = vec![4u8];
            d.extend_from_slice(cmd);
            d.extend_from_slice(&[0xFF]);
            d.extend_from_slice(b"pg_wal/RECOVERYXLOG");
            d.extend_from_slice(&[0xFF]);
            d.extend_from_slice(b"000000010000000000000001");
            d.extend_from_slice(&[0xFF]);
            d.extend_from_slice(b"000000010000000000000000");
            strfam_diff(&d);
        }
        // relpath: all three tablespace branches x fork x proc arms.
        for sel13 in 0u8..4 {
            for sel12 in 0u8..12 {
                let mut d = vec![5u8];
                d.extend_from_slice(&5u32.to_le_bytes());
                d.extend_from_slice(&16385u32.to_le_bytes());
                d.extend_from_slice(&16400u32.to_le_bytes());
                d.push(sel12);
                d.push(sel13);
                strfam_diff(&d);
            }
        }
        // wait_error: exited/signaled/stopped/unrecognized/-1 arms.
        for (st, b5) in [
            (0i32, 0u8),
            (3 << 8, 0),
            (126 << 8, 0),
            (127 << 8, 0),
            (129 << 8, 1),
            (15, 0),
            (9, 1),
            (0x7f, 0),
            (0xdead, 0),
            (0, 2), /* -1 arm */
        ] {
            let mut d = vec![6u8];
            d.extend_from_slice(&st.to_le_bytes());
            d.push(30); // signum draw
            d.push(b5);
            d.push(4); // errno pin draw
            strfam_diff(&d);
        }
        // Fleet LSan regression (leak-00a6eea8): BuildRestoreCommand error
        // path leaked the shim-side nativePath past the ereport longjmp.
        strfam_diff(&[0x04, 0x25, 0xe3, 0x91, 0x87]);
        // isspace: full byte sweep.
        for b in 0u8..=255 {
            strfam_diff(&[7, b]);
        }
    }
}
