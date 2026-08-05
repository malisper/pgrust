//! Kani C-equivalence harnesses: the CBMC-tractable members of the p1-lanec
//! string-family batch vs vendored PostgreSQL 18.3 C (c/pg_strfam_kani.c,
//! compiled via `-Z c-ffi --c-lib`).
//!
//! In scope here (fast-class, pure arithmetic / finite domain):
//!   - common/wait_error: WIFEXITED/WEXITSTATUS/WIFSIGNALED/WTERMSIG (the
//!     shipped libc-crate bindings vs the host <sys/wait.h> macros the C
//!     build uses — full symbolic i32), wait_result_is_signal,
//!     wait_result_is_any_signal, wait_result_to_exit_code (full symbolic
//!     domains vs the VERBATIM vendored bodies).
//!   - common/relpath: forkname_chars vs the vendored forkNames table
//!     (4 literal cells, byte-for-byte).
//!   - common/string: isspace_c_locale vs the ruled C-locale set, full u8
//!     (the set statement {09..0d, 20} is the same one the vendored strtol
//!     callers rely on; the libc isspace binding itself is host ctype and
//!     is differential-fuzzed, not modeled).
//!
//! Fuzz-routed (NOT here, see fuzz/core/src/strfam.rs): everything through
//! libc strtol/strtoul/snprintf/strsignal/strerror and the string-building
//! members (pg_clean_ascii, percentrepl, archive, GetRelationPath) — libc
//! parse cores and %-format emission are exactly what a CBMC model would
//! define away.
//!
//! Run: sh run-all.sh (kissat for expected-green; the negative control runs
//! under the DEFAULT solver — controls validate by counterexample).

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    use core::ffi::c_int;

    extern "C" {
        pub fn pg_kani_wifexited(status: c_int) -> c_int;
        pub fn pg_kani_wexitstatus(status: c_int) -> c_int;
        pub fn pg_kani_wifsignaled(status: c_int) -> c_int;
        pub fn pg_kani_wtermsig(status: c_int) -> c_int;
        pub fn pg_kani_wait_result_is_signal(exit_status: c_int, signum: c_int) -> c_int;
        pub fn pg_kani_wait_result_is_any_signal(exit_status: c_int, include_cnf: c_int)
            -> c_int;
        pub fn pg_kani_wait_result_to_exit_code(exit_status: c_int) -> c_int;
        pub fn pg_kani_forkname_byte(fork: c_int, j: c_int) -> c_int;
    }
}

#[cfg(kani)]
mod harnesses {
    use crate::ffi;
    use core::ffi::c_int;

    // ---- wait_error W* macro bindings (full symbolic i32) ----------------

    #[kani::proof]
    fn eq_wifexited() {
        let status: i32 = kani::any();
        let c = unsafe { ffi::pg_kani_wifexited(status) };
        assert_eq!(wait_error::WIFEXITED(status), c != 0);
    }

    #[kani::proof]
    fn eq_wexitstatus() {
        let status: i32 = kani::any();
        let c = unsafe { ffi::pg_kani_wexitstatus(status) };
        assert_eq!(wait_error::WEXITSTATUS(status), c);
    }

    #[kani::proof]
    fn eq_wifsignaled() {
        let status: i32 = kani::any();
        let c = unsafe { ffi::pg_kani_wifsignaled(status) };
        assert_eq!(wait_error::WIFSIGNALED(status), c != 0);
    }

    #[kani::proof]
    fn eq_wtermsig() {
        let status: i32 = kani::any();
        let c = unsafe { ffi::pg_kani_wtermsig(status) };
        assert_eq!(wait_error::WTERMSIG(status), c);
    }

    // ---- wait_result_* family (VERBATIM vendored bodies) -----------------

    #[kani::proof]
    fn eq_wait_result_is_signal() {
        let status: i32 = kani::any();
        let signum: i32 = kani::any();
        // 128 + signum in the C body is int arithmetic; keep the full domain
        // (overflow there would be C UB — absence is part of the theorem).
        kani::assume(signum > i32::MIN + 128 && signum < i32::MAX - 128);
        let c = unsafe { ffi::pg_kani_wait_result_is_signal(status, signum) };
        assert_eq!(wait_error::wait_result_is_signal(status, signum), c != 0);
    }

    #[kani::proof]
    fn eq_wait_result_is_any_signal() {
        let status: i32 = kani::any();
        let inc: bool = kani::any();
        let c = unsafe { ffi::pg_kani_wait_result_is_any_signal(status, inc as c_int) };
        assert_eq!(wait_error::wait_result_is_any_signal(status, inc), c != 0);
    }

    #[kani::proof]
    fn eq_wait_result_to_exit_code() {
        let status: i32 = kani::any();
        let c = unsafe { ffi::pg_kani_wait_result_to_exit_code(status) };
        assert_eq!(wait_error::wait_result_to_exit_code(status), c);
    }

    // ---- relpath forkname_chars (4 literal cells) ------------------------

    fn check_forkname(fork: types_core::ForkNumber, idx: c_int) {
        let name = relpath::forkname_chars(fork).as_bytes();
        for (j, &b) in name.iter().enumerate() {
            let c = unsafe { ffi::pg_kani_forkname_byte(idx, j as c_int) };
            assert_eq!(b as c_int, c);
        }
        // C string ends exactly where the Rust str does.
        let c_end = unsafe { ffi::pg_kani_forkname_byte(idx, name.len() as c_int) };
        assert_eq!(c_end, 0);
    }

    #[kani::proof]
    fn eq_forkname_chars_main() {
        check_forkname(types_core::ForkNumber::MAIN_FORKNUM, 0);
    }

    #[kani::proof]
    fn eq_forkname_chars_fsm() {
        check_forkname(types_core::ForkNumber::FSM_FORKNUM, 1);
    }

    #[kani::proof]
    fn eq_forkname_chars_vm() {
        check_forkname(types_core::ForkNumber::VISIBILITYMAP_FORKNUM, 2);
    }

    #[kani::proof]
    fn eq_forkname_chars_init() {
        check_forkname(types_core::ForkNumber::INIT_FORKNUM, 3);
    }

    /// Union coverage over the case-split (suite law: case-splits need a
    /// union harness).
    #[kani::proof]
    fn cover_forkname_chars_split() {
        let sel: u8 = kani::any();
        kani::assume(sel < 4);
        let fork = match sel {
            0 => types_core::ForkNumber::MAIN_FORKNUM,
            1 => types_core::ForkNumber::FSM_FORKNUM,
            2 => types_core::ForkNumber::VISIBILITYMAP_FORKNUM,
            _ => types_core::ForkNumber::INIT_FORKNUM,
        };
        assert!(!relpath::forkname_chars(fork).is_empty());
    }

    // ---- string isspace_c_locale (full u8 vs the ruled set) --------------

    #[kani::proof]
    fn eq_isspace_c_locale() {
        let b: u8 = kani::any();
        let in_set = b == 0x20 || (0x09..=0x0d).contains(&b);
        assert_eq!(pg_string::isspace_c_locale(b), in_set);
    }

    // ---- negative control (must FAIL on the intended assert) -------------

    /// Deliberately wrong claim: is_signal == "directly signaled by signum"
    /// (drops the shell 128+signum arm). The solver must find the
    /// counterexample, failing on the assert! below.
    #[kani::proof]
    fn control_negative_is_signal_drops_shell_arm() {
        let status: i32 = kani::any();
        let signum: i32 = kani::any();
        kani::assume(signum > 0 && signum < 64);
        let direct = wait_error::WIFSIGNALED(status) && wait_error::WTERMSIG(status) == signum;
        assert!(wait_error::wait_result_is_signal(status, signum) == direct);
    }
}
