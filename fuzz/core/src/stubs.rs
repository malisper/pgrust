//! stubs — the shared stub-pin facility for differential fuzz targets
//! (Michael-ratified 2026-08-01). C half: csrc/stubshims/pg_stub_state.c;
//! usage contract: fuzz/STUBS.md.
//!
//! A differential target that consumes session state (GUCs, the current
//! timestamp, the global prng, work_mem ceilings) declares its pins by
//! calling the `pin_*` functions here with bytes drawn from the fuzz
//! input. Each `pin_*`:
//!
//!   1. derives the value ONCE, bounded to the setting's legal range
//!      (ranges from the shipped guc_tables / type domains) — one
//!      derivation means out-of-range fuzz bytes clamp identically on both
//!      sides by construction;
//!   2. sets the SHIPPED Rust side through the real session seam (the same
//!      thread-local cell / GUC accessor the shipped code reads);
//!   3. sets the C-oracle side through the pg_stub_* setters
//!      (csrc/stubshims/pg_stub_state.c), whose globals stub-aware oracle
//!      TUs read;
//!   4. returns the canonical value so the target can log/carve on it.
//!
//! BOTH-SIDES DISCIPLINE: a pinned value is part of the compared input;
//! never let one side default.
//!
//! MUST-FAIL CONTROLS: every facility ships a control test (this module's
//! tests) that (a) proves parity under matched pins through a REAL
//! verbatim vendored consumer, and (b) deliberately mismatches the two
//! sides and asserts the differential DETECTS the divergence. A pin whose
//! control cannot fail is vacuous (harness-detection-power law). The
//! controls are plain `assert!`s — release-effective, no debug_assert
//! (debug-assert-masking law).

use std::cell::Cell;
use std::ffi::c_char;

extern "C" {
    // stub:guc
    fn pg_stub_set_extra_float_digits(v: i32);
    fn pg_stub_set_datestyle(style: i32, order: i32);
    fn pg_stub_set_intervalstyle(istyle: i32);
    fn pg_stub_set_standard_conforming_strings(on: i32);
    // stub:clock
    fn pg_stub_set_current_timestamp(usecs: i64);
    fn pg_stub_set_mono_ns(ns: u64);
    // stub:prng
    fn pg_stub_prng_seed(seed: u64);
    fn pg_stub_set_scram_salt(salt16: *const u8);
    // stub:guc — cryptbe family channels
    fn pg_stub_set_md5_password_warnings(on: i32);
    fn pg_stub_set_scram_iterations(iters: i32);
    // stub:workmem
    fn pg_stub_set_work_mem(work_mem_kb: i32, maintenance_work_mem_kb: i32);
}

// Raw C-side handles the controls (and only the controls) use to read back
// the C plane and to manufacture DELIBERATE one-sided divergence. Targets
// never call these: pins always go through `pin_*`.
#[cfg(test)]
pub(crate) mod craw {
    use std::ffi::c_char;
    extern "C" {
        pub fn pg_stub_set_extra_float_digits(v: i32);
        pub fn pg_stub_get_extra_float_digits() -> i32;
        pub fn pg_stub_set_datestyle(style: i32, order: i32);
        pub fn pg_stub_set_intervalstyle(istyle: i32);
        pub fn pg_stub_set_standard_conforming_strings(on: i32);
        pub fn pg_stub_get_standard_conforming_strings() -> i32;
        pub fn pg_stub_set_current_timestamp(usecs: i64);
        pub fn pg_stub_get_current_timestamp() -> i64;
        pub fn pg_stub_prng_seed(seed: u64);
        pub fn pg_stub_set_scram_salt(salt16: *const u8);
        pub fn pg_stub_set_md5_password_warnings(on: i32);
        pub fn pg_stub_set_scram_iterations(iters: i32);
        pub fn pg_stub_set_work_mem(wm: i32, mwm: i32);
        pub fn pg_stub_get_work_mem() -> i32;
        pub fn pg_stub_float8out_guc(num: f64, buf32: *mut c_char) -> i32;
        pub fn pg_stub_timestamp_out_guc(ts: i64, buf: *mut c_char) -> i32;
        pub fn pg_stub_interval_out_guc(t: i64, day: i32, month: i32, buf: *mut c_char) -> i32;
        pub fn pg_stub_bloom_m_guc(total_elems: i64, seed: u64) -> u64;
    }
}

// C consumer wrappers targets DO use (the demo wiring in diff.rs drives
// pg_stub_float8out_guc): vendored consumers under the pinned globals.
extern "C" {
    pub fn pg_stub_float8out_guc(num: f64, buf32: *mut c_char) -> i32;
    pub fn pg_stub_timestamp_out_guc(ts: i64, buf: *mut c_char) -> i32;
    pub fn pg_stub_interval_out_guc(t: i64, day: i32, month: i32, buf: *mut c_char) -> i32;
    pub fn pg_stub_bloom_m_guc(total_elems: i64, seed: u64) -> u64;
    pub fn pg_stub_prng_u64() -> u64;
    pub fn pg_stub_prng_double() -> f64;
    pub fn pg_stub_get_current_timestamp() -> i64;
}

// ---------------------------------------------------------------------------
// stub:guc — GUC pinned scalars
// ---------------------------------------------------------------------------
pub mod guc {
    use adt_datetime::consts::{
        INTSTYLE_ISO_8601, INTSTYLE_POSTGRES, INTSTYLE_POSTGRES_VERBOSE, INTSTYLE_SQL_STANDARD,
    };
    use adt_datetime::{
        DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD, USE_GERMAN_DATES, USE_ISO_DATES,
        USE_POSTGRES_DATES, USE_SQL_DATES, USE_XSD_DATES,
    };

    /// extra_float_digits: legal range [-15, 3] (guc_tables). Rust side =
    /// the adt_float session cell (what the shipped float4out/float8out
    /// read); C side = pg_stub_extra_float_digits.
    pub fn pin_extra_float_digits(b: u8) -> i32 {
        let v = 3 - (b % 19) as i32;
        adt_float::set_extra_float_digits(v);
        unsafe { super::pg_stub_set_extra_float_digits(v) };
        v
    }

    /// DateStyle x DateOrder: the 5x3 legal enum members (the DateStyle GUC
    /// assign hook only ever produces these pairs; pinning the parsed pair
    /// is the family convention — datetime_io_diff/timestamp_diff). Rust
    /// side = the adt_datetime session cells; C side = pg_stub_DateStyle/
    /// pg_stub_DateOrder.
    pub fn pin_date_style(b: u8) -> (i32, i32) {
        let style = match b % 5 {
            0 => USE_POSTGRES_DATES,
            1 => USE_ISO_DATES,
            2 => USE_SQL_DATES,
            3 => USE_GERMAN_DATES,
            _ => USE_XSD_DATES,
        };
        let order = match (b / 5) % 3 {
            0 => DATEORDER_YMD,
            1 => DATEORDER_DMY,
            _ => DATEORDER_MDY,
        };
        adt_datetime::set_date_style(style);
        adt_datetime::set_date_order(order);
        unsafe { super::pg_stub_set_datestyle(style, order) };
        (style, order)
    }

    /// IntervalStyle: the 4 legal enum members.
    pub fn pin_interval_style(b: u8) -> i32 {
        let s = match b % 4 {
            0 => INTSTYLE_POSTGRES,
            1 => INTSTYLE_POSTGRES_VERBOSE,
            2 => INTSTYLE_SQL_STANDARD,
            _ => INTSTYLE_ISO_8601,
        };
        adt_datetime::set_interval_style(s);
        unsafe { super::pg_stub_set_intervalstyle(s) };
        s
    }

    /// standard_conforming_strings (bool). Rust side = the scan_fgram
    /// session cell (what the shipped lexer reads); C side =
    /// pg_stub_standard_conforming_strings. NOTE: csrc/ has no vendored C
    /// lexer yet, so unlike the pins above this one has transport-level
    /// controls only; the first scanner differential target owns the
    /// consumer-level control.
    pub fn pin_standard_conforming_strings(b: u8) -> bool {
        let v = b & 1 == 1;
        scan_fgram::gucs::set_standard_conforming_strings(v);
        unsafe { super::pg_stub_set_standard_conforming_strings(v as i32) };
        v
    }

    /// md5_password_warnings (bool, boot true; crypt.c). Rust side = the
    /// crypt crate's session cell through its installed GUC accessor
    /// (crypt::init_seams, installed lazily here); C side =
    /// pg_stub_md5_password_warnings, which the cryptbe oracle's verbatim
    /// `md5_password_warnings` reads map onto. First consumer + must-fail
    /// control: crypt_be_diff / `control_guc_md5_password_warnings_pin`.
    pub fn pin_md5_password_warnings(b: u8) -> bool {
        if !guc_tables::vars::md5_password_warnings.installed() {
            let _ = std::panic::catch_unwind(crypt::init_seams);
        }
        let v = b & 1 == 1;
        guc_tables::vars::md5_password_warnings.write(v);
        unsafe { super::pg_stub_set_md5_password_warnings(v as i32) };
        v
    }

    /// scram_iterations (int, boot 4096, legal range [1, i32::MAX];
    /// auth-scram.c scram_sha_256_iterations). The pin folds the fuzz byte
    /// into a SMALL subset of the legal range — 1..=64, plus the boot
    /// default 4096 on 0xFF — a fuzz-domain bound (documented in the
    /// consuming target header): PBKDF2 cost is linear in the count and
    /// the iteration-count plumbing, not the loop count, is the compared
    /// surface. Rust side = the auth_scram session cell via its installed
    /// GUC accessor; C side = pg_stub_scram_iterations.
    pub fn pin_scram_iterations(b: u8) -> i32 {
        if !guc_tables::vars::scram_sha_256_iterations.installed() {
            let _ = std::panic::catch_unwind(auth_scram::init_seams);
        }
        let v = if b == 0xFF { 4096 } else { 1 + (b % 64) as i32 };
        guc_tables::vars::scram_sha_256_iterations.write(v);
        unsafe { super::pg_stub_set_scram_iterations(v) };
        v
    }
}

// ---------------------------------------------------------------------------
// stub:clock — GetCurrentTimestamp pinned to a fuzzed TimestampTz
// ---------------------------------------------------------------------------
pub mod clock {
    use super::Cell;

    /// Valid timestamp[tz] domain (adt_timestamp: IS_VALID_TIMESTAMP is
    /// MIN <= ts < END).
    pub const MIN_TIMESTAMP: i64 = adt_timestamp::MIN_TIMESTAMP;
    pub const END_TIMESTAMP: i64 = adt_timestamp::END_TIMESTAMP;

    std::thread_local! {
        static NOW_USECS: Cell<i64> = const { Cell::new(0) };
    }

    /// Fold an arbitrary fuzzed i64 into the valid timestamp domain
    /// [MIN_TIMESTAMP, END_TIMESTAMP). One derivation for both sides.
    pub fn clamp(raw: i64) -> i64 {
        let range = END_TIMESTAMP as i128 - MIN_TIMESTAMP as i128;
        let off = (raw as i128 - MIN_TIMESTAMP as i128).rem_euclid(range);
        (MIN_TIMESTAMP as i128 + off) as i64
    }

    /// Pin "now" on both sides from a fuzzed i64 (clamped). Returns the
    /// pinned TimestampTz.
    pub fn pin_now(raw: i64) -> i64 {
        let v = clamp(raw);
        NOW_USECS.with(|c| c.set(v));
        unsafe { super::pg_stub_set_current_timestamp(v) };
        v
    }

    /// The Rust-side pinned clock read (what the timestamp seam below
    /// resolves to).
    pub fn now_usecs() -> i64 {
        NOW_USECS.with(|c| c.get())
    }

    /// stub:clock MONOTONIC half — pin the monotonic reading on both sides
    /// from a fuzz-derived nanosecond value: the shipped Rust side through
    /// `pg_clock::fuzz_mono_pin` (default-off feature this fuzz workspace
    /// enables; `pg_clock::mono_ns()` returns the pin) and the C side
    /// through `pg_stub_set_mono_ns` (an oracle TU #defines its
    /// INSTR_TIME_SET_CURRENT to read `pg_stub_get_mono_ns()`). Every u64
    /// is legal — the value is an opaque monotonic reading; a target that
    /// compares elapsed arithmetic must keep its pinned sequence
    /// NON-DECREASING and bounded below i64 wrap (document the bound in
    /// the target header). First consumer: tsm_system_time_diff.
    pub fn pin_mono_ns(ns: u64) {
        pg_clock::fuzz_mono_pin::set(ns);
        unsafe { super::pg_stub_set_mono_ns(ns) };
    }

    /// Route the shipped `timestamp_seams::get_current_timestamp` seam to
    /// the pinned cell, so GetCurrentTimestamp-shaped reads inside shipped
    /// pure code see the pin. First-wins and NON-PANICKING on a lost race
    /// (install_detoast_seam_once precedent): several legacy targets
    /// install their own constant impl with an unguarded `set()`, so this
    /// must only be called from a fuzz target's own init (one target per
    /// process) — never from the shared `cargo test` binary, where a legacy
    /// target's later unguarded set() would panic. Returns whether THIS
    /// call performed the install; a caller that requires the seam must
    /// check.
    pub fn install_timestamp_seam() -> bool {
        if timestamp_seams::get_current_timestamp::is_installed() {
            return false;
        }
        std::panic::catch_unwind(|| timestamp_seams::get_current_timestamp::set(now_usecs)).is_ok()
    }
}

// ---------------------------------------------------------------------------
// stub:prng — pg_global_prng_state seeded identically both sides
// ---------------------------------------------------------------------------
pub mod prng {
    use super::Cell;
    use pg_prng::PgPrng;

    std::thread_local! {
        static STATE: Cell<PgPrng> = const { Cell::new(PgPrng::from_raw(0, 0)) };
    }

    /// Seed both sides' "global prng" from the fuzz input (every u64 is
    /// legal; seeding runs the same splitmix64 expansion both sides —
    /// shipped pg_prng vs verbatim vendored pg_prng.c).
    pub fn pin_seed(seed: u64) {
        STATE.with(|c| c.set(PgPrng::seeded(seed)));
        unsafe { super::pg_stub_prng_seed(seed) };
    }

    /// Rust-side global-prng draws (shipped xoroshiro128**). The C-side
    /// counterparts are `pg_stub_prng_u64` / `pg_stub_prng_double`.
    pub fn rust_u64() -> u64 {
        STATE.with(|c| {
            let mut st = c.get();
            let v = st.next_u64();
            c.set(st);
            v
        })
    }

    pub fn rust_double() -> f64 {
        STATE.with(|c| {
            let mut st = c.get();
            let v = st.next_f64();
            c.set(st);
            v
        })
    }

    /// stub:prng scram-salt channel — pin the 16-byte pg_strong_random
    /// read inside pg_be_scram_build_secret identically on both sides:
    /// the C oracle through pg_stub_set_scram_salt (the cryptbe TU's
    /// pg_strong_random shim copies from it) and the shipped Rust side
    /// through the crypt/auth_scram determinism hook
    /// PGRUST_SCRAM_FIXED_SALT_B64 — the REAL seam the shipped
    /// pg_be_scram_build_secret reads (a sanctioned test-only divergence,
    /// see auth_scram::test_fixed_salt). The entropy read is ENVIRONMENT;
    /// every byte derived from the salt afterwards is compared verbatim
    /// computation. First consumer + must-fail control: crypt_be_diff /
    /// `control_prng_scram_salt_pin`.
    pub fn pin_scram_salt(salt: [u8; 16]) -> [u8; 16] {
        let cap = pg_b64::pg_b64_enc_len(16);
        let mut enc = vec![0u8; cap as usize];
        let n = pg_b64::pg_b64_encode(&salt, 16, &mut enc, cap);
        assert!(n > 0, "b64 of a fixed 16-byte salt cannot fail");
        enc.truncate(n as usize);
        // One fuzz target per process; the shared cargo test binary only
        // touches this from the serial control test.
        std::env::set_var(
            "PGRUST_SCRAM_FIXED_SALT_B64",
            std::str::from_utf8(&enc).unwrap(),
        );
        unsafe { super::pg_stub_set_scram_salt(salt.as_ptr()) };
        salt
    }
}

// ---------------------------------------------------------------------------
// stub:workmem — work_mem / maintenance_work_mem ceilings
// ---------------------------------------------------------------------------
pub mod workmem {
    use super::Cell;

    /// guc_tables: work_mem/maintenance_work_mem are [64, MAX_KILOBYTES] kB.
    pub const MIN_KB: i32 = 64;
    pub const MAX_KB: i32 = guc_tables::consts::MAX_KILOBYTES;

    std::thread_local! {
        static WORK_MEM: Cell<i32> = const { Cell::new(4096) };
        static MAINTENANCE_WORK_MEM: Cell<i32> = const { Cell::new(65536) };
    }

    /// Fold an arbitrary fuzzed i32 into the legal [64, MAX_KILOBYTES]
    /// range. One derivation for both sides. Targets that ALLOCATE
    /// work_mem-sized state must additionally bound their own consumption
    /// (the pin owns legality, not the target's memory budget).
    pub fn clamp(raw: i32) -> i32 {
        let span = MAX_KB as i64 - MIN_KB as i64 + 1;
        (MIN_KB as i64 + (raw as i64 - MIN_KB as i64).rem_euclid(span)) as i32
    }

    /// Pin both ceilings on both sides from fuzzed i32s (clamped).
    pub fn pin(raw_work_mem: i32, raw_maintenance_work_mem: i32) -> (i32, i32) {
        let wm = clamp(raw_work_mem);
        let mwm = clamp(raw_maintenance_work_mem);
        WORK_MEM.with(|c| c.set(wm));
        MAINTENANCE_WORK_MEM.with(|c| c.set(mwm));
        unsafe { super::pg_stub_set_work_mem(wm, mwm) };
        (wm, mwm)
    }

    /// Rust-side reads: the values a stub-aware driver passes into shipped
    /// APIs that take a work_mem ceiling (bloomfilter/tuplesort-style
    /// sizing math).
    pub fn work_mem() -> i32 {
        WORK_MEM.with(|c| c.get())
    }

    pub fn maintenance_work_mem() -> i32 {
        MAINTENANCE_WORK_MEM.with(|c| c.get())
    }
}

// ---------------------------------------------------------------------------
// Must-fail controls. Each control (a) proves parity through a real
// vendored consumer under matched pins, and (b) mismatches the sides on
// purpose and asserts the comparator sees the divergence — if a pin plane
// were dead (setter no-op, consumer not reading the global), phase (a) or
// (b) fails.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn c_str_image(buf: &[u8]) -> &[u8] {
        &buf[..buf.iter().position(|&b| b == 0).expect("NUL")]
    }

    fn rust_float8out(v: f64) -> Vec<u8> {
        let mut rbuf = [0u8; 64];
        let n = adt_float::float8out(v, &mut rbuf); // reads the efd session cell
        rbuf[..n].to_vec()
    }

    fn c_float8out_guc(v: f64) -> Vec<u8> {
        let mut cbuf = [0u8; 40];
        let n = unsafe { craw::pg_stub_float8out_guc(v, cbuf.as_mut_ptr().cast()) } as usize;
        cbuf[..n].to_vec()
    }

    /// stub:guc / extra_float_digits. Consumers: shipped float8out (reads
    /// the adt_float session cell) vs verbatim float8out_internal_efd
    /// (reads pg_stub_extra_float_digits via the wrapper).
    #[test]
    fn control_guc_efd_pin() {
        let _g = crate::c_oracle_serial();
        // (a) matched pins across the whole legal range, on a value whose
        // image depends on efd (0.1 renders differently at -15..3).
        for b in 0..=255u8 {
            let efd = guc::pin_extra_float_digits(b);
            assert!((-15..=3).contains(&efd), "efd {efd} outside legal range");
            assert_eq!(
                rust_float8out(std::f64::consts::PI),
                c_float8out_guc(std::f64::consts::PI),
                "efd={efd}: matched pins must agree"
            );
        }
        // (b) deliberate one-sided divergence: Rust pinned 3, C poked to
        // -15. A dead C plane (setter no-op or consumer ignoring the
        // global) leaves the images equal and this assert FAILS.
        guc::pin_extra_float_digits(0); // efd = 3 both sides
        unsafe { craw::pg_stub_set_extra_float_digits(-15) };
        assert_eq!(unsafe { craw::pg_stub_get_extra_float_digits() }, -15);
        assert_ne!(
            rust_float8out(std::f64::consts::PI),
            c_float8out_guc(std::f64::consts::PI),
            "mismatched efd pins MUST diverge (pin plane dead?)"
        );
        guc::pin_extra_float_digits(0); // restore matched state
    }

    /// stub:guc / DateStyle+DateOrder. Consumers: shipped
    /// adt_timestamp::timestamp_out (reads the adt_datetime session cells)
    /// vs verbatim EncodeDateTime via pg_stub_timestamp_out_guc.
    #[test]
    fn control_guc_datestyle_pin() {
        let _g = crate::c_oracle_serial();
        // 2004-02-29 13:14:15.123456: month/day/order-sensitive image.
        let ts: i64 = (1520 * 86_400_000_000) + 47_655_000_000 + 123_456;
        let rust_out = |ts: i64| {
            let mut rbuf = [0u8; adt_datetime::MAXDATELEN + 1];
            let n = adt_timestamp::timestamp_out(ts, &mut rbuf).expect("valid ts");
            rbuf[..n].to_vec()
        };
        let c_out = |ts: i64| {
            let mut cbuf = [0u8; 160];
            let err = unsafe { craw::pg_stub_timestamp_out_guc(ts, cbuf.as_mut_ptr().cast()) };
            assert_eq!(err, 0, "C timestamp_out errored");
            c_str_image(&cbuf).to_vec()
        };
        // (a) matched pins over all 15 legal (style, order) pairs.
        for b in 0..15u8 {
            let (style, order) = guc::pin_date_style(b);
            assert_eq!(
                rust_out(ts),
                c_out(ts),
                "style={style} order={order}: matched pins must agree"
            );
        }
        // (b) Rust pinned ISO, C poked to Postgres/DMY: must diverge.
        guc::pin_date_style(1); // ISO/YMD both sides
        unsafe { craw::pg_stub_set_datestyle(adt_datetime::USE_POSTGRES_DATES, adt_datetime::DATEORDER_DMY) };
        assert_ne!(
            rust_out(ts),
            c_out(ts),
            "mismatched DateStyle pins MUST diverge (pin plane dead?)"
        );
        guc::pin_date_style(1);
    }

    /// stub:guc / IntervalStyle. Consumers: shipped interval_out (reads the
    /// adt_datetime session cell) vs verbatim EncodeInterval via
    /// pg_stub_interval_out_guc.
    #[test]
    fn control_guc_intervalstyle_pin() {
        let _g = crate::c_oracle_serial();
        // 1 mon -1 day +01:02:03.000004: style-sensitive rendering.
        let (t, d, m) = (3_723_000_004i64, -1i32, 1i32);
        let rust_out = || {
            let iv = adt_datetime::Interval { time: t, day: d, month: m };
            let mut rbuf = [0u8; adt_datetime::MAXDATELEN + 1];
            let n = adt_timestamp::interval::interval_out(&iv, &mut rbuf);
            rbuf[..n].to_vec()
        };
        let c_out = || {
            let mut cbuf = [0u8; 160];
            let err = unsafe { craw::pg_stub_interval_out_guc(t, d, m, cbuf.as_mut_ptr().cast()) };
            assert_eq!(err, 0, "C interval_out errored");
            c_str_image(&cbuf).to_vec()
        };
        // (a) matched pins over all 4 legal styles.
        for b in 0..4u8 {
            let s = guc::pin_interval_style(b);
            assert_eq!(rust_out(), c_out(), "istyle={s}: matched pins must agree");
        }
        // (b) Rust pinned iso_8601, C poked to sql_standard: must diverge.
        guc::pin_interval_style(3);
        unsafe { craw::pg_stub_set_intervalstyle(adt_datetime::consts::INTSTYLE_SQL_STANDARD) };
        assert_ne!(
            rust_out(),
            c_out(),
            "mismatched IntervalStyle pins MUST diverge (pin plane dead?)"
        );
        guc::pin_interval_style(3);
    }

    /// stub:guc / standard_conforming_strings. Transport-level control
    /// only (csrc/ has no vendored C lexer yet — documented in stubs.rs and
    /// STUBS.md): proves the pin writes BOTH planes and a mismatch is
    /// visible; the Rust plane is the real scan_fgram session cell the
    /// shipped lexer reads.
    #[test]
    fn control_guc_scs_pin() {
        let _g = crate::c_oracle_serial();
        for b in 0..4u8 {
            let v = guc::pin_standard_conforming_strings(b);
            assert_eq!(v, b & 1 == 1);
            assert_eq!(scan_fgram::gucs::standard_conforming_strings(), v);
            assert_eq!(unsafe { craw::pg_stub_get_standard_conforming_strings() } != 0, v);
        }
        // Deliberate mismatch: C poked opposite of Rust — the planes must
        // disagree (a dead C setter would leave them equal after re-pin).
        guc::pin_standard_conforming_strings(1); // true both sides
        unsafe { craw::pg_stub_set_standard_conforming_strings(0) };
        assert_ne!(
            scan_fgram::gucs::standard_conforming_strings(),
            unsafe { craw::pg_stub_get_standard_conforming_strings() } != 0,
            "mismatched scs pins MUST be visible (pin plane dead?)"
        );
        guc::pin_standard_conforming_strings(1);
    }

    /// stub:clock. Both planes pinned from one clamped derivation; clamp
    /// keeps every fuzzed i64 inside the valid timestamp domain; a
    /// deliberate one-sided poke must be visible.
    #[test]
    fn control_clock_pin() {
        let _g = crate::c_oracle_serial();
        for raw in [0i64, -1, i64::MIN, i64::MAX, 1_234_567_890_123_456] {
            let v = clock::pin_now(raw);
            assert!(
                (clock::MIN_TIMESTAMP..clock::END_TIMESTAMP).contains(&v),
                "clamp escaped the valid timestamp domain: {v}"
            );
            assert_eq!(clock::now_usecs(), v, "Rust clock plane dead");
            assert_eq!(
                unsafe { craw::pg_stub_get_current_timestamp() },
                v,
                "C clock plane dead"
            );
        }
        // In-range values pin exactly (no gratuitous remap).
        assert_eq!(clock::pin_now(42), 42);
        // Deliberate mismatch: C poked to a different instant.
        clock::pin_now(1000);
        unsafe { craw::pg_stub_set_current_timestamp(2000) };
        assert_ne!(
            clock::now_usecs(),
            unsafe { craw::pg_stub_get_current_timestamp() },
            "mismatched clock pins MUST be visible (pin plane dead?)"
        );
        clock::pin_now(1000);
    }

    /// stub:prng. Consumers: shipped pg_prng xoroshiro128** vs the verbatim
    /// vendored engine (pg_pg_prng_io.c) over the shim-held global state —
    /// a genuine differential in itself. Same seed => same stream; a
    /// one-sided reseed must diverge on the next draw.
    #[test]
    fn control_prng_pin() {
        let _g = crate::c_oracle_serial();
        for seed in [0u64, 1, 0xDEAD_BEEF, u64::MAX] {
            prng::pin_seed(seed);
            for i in 0..16 {
                let r = prng::rust_u64();
                let c = unsafe { pg_stub_prng_u64() };
                assert_eq!(r, c, "seed={seed:#x} draw {i}: matched seeds must agree");
            }
            let rd = prng::rust_double();
            let cd = unsafe { pg_stub_prng_double() };
            assert_eq!(rd.to_bits(), cd.to_bits(), "double draw diverged");
        }
        // Deliberate mismatch: reseed C only; first u64 draw must differ
        // (distinct splitmix64 expansions).
        prng::pin_seed(0x1234);
        unsafe { craw::pg_stub_prng_seed(0x9999) };
        assert_ne!(
            prng::rust_u64(),
            unsafe { pg_stub_prng_u64() },
            "mismatched prng seeds MUST diverge (pin plane dead?)"
        );
    }

    /// stub:workmem. Consumers: shipped bloomfilter sizing vs verbatim
    /// bloom_create (pg_libfam_io.c) under pg_stub_work_mem — bitset size
    /// is the work_mem-dependent observable (total_elems chosen so the
    /// work_mem term binds).
    #[test]
    fn control_workmem_pin() {
        let _g = crate::c_oracle_serial();
        const TOTAL: i64 = 10_000_000; // 2*total = 20MB >> tested ceilings
        const SEED: u64 = 0xfeed_beef;
        let rust_m = |wm: i32| {
            let cx = mcx::MemoryContext::new("stub_workmem_control");
            let m = bloomfilter::BloomFilter::create_in(cx.mcx(), TOTAL, wm, SEED)
                .expect("bloom create")
                .bitset_bits();
            m
        };
        // (a) matched pins: clamp legality + m parity at several ceilings.
        for raw in [64i32, 1024, 2048, 7777, i32::MIN, i32::MAX] {
            let (wm, mwm) = workmem::pin(raw, raw);
            assert!((workmem::MIN_KB..=workmem::MAX_KB).contains(&wm));
            assert_eq!(wm, mwm);
            // Keep the C-side allocation bounded for the control run.
            if wm <= 16 * 1024 {
                assert_eq!(
                    rust_m(workmem::work_mem()),
                    unsafe { craw::pg_stub_bloom_m_guc(TOTAL, SEED) },
                    "work_mem={wm}: matched pins must agree"
                );
            }
        }
        // (b) deliberate mismatch: Rust holds 1024kB, C poked to 4096kB —
        // the sizing observable must diverge.
        workmem::pin(1024, 1024);
        unsafe { craw::pg_stub_set_work_mem(4096, 4096) };
        assert_eq!(unsafe { craw::pg_stub_get_work_mem() }, 4096);
        assert_ne!(
            rust_m(workmem::work_mem()),
            unsafe { craw::pg_stub_bloom_m_guc(TOTAL, SEED) },
            "mismatched work_mem pins MUST diverge (pin plane dead?)"
        );
        workmem::pin(1024, 1024);
    }

    /// Clamp edge behavior is total and in-range (both derivations are the
    /// single source for both sides, so identical clamping is structural;
    /// this pins the arithmetic itself).
    #[test]
    fn clamp_edges() {
        for raw in [i64::MIN, i64::MAX, 0, -1, clock::MIN_TIMESTAMP, clock::END_TIMESTAMP - 1] {
            let v = clock::clamp(raw);
            assert!((clock::MIN_TIMESTAMP..clock::END_TIMESTAMP).contains(&v));
        }
        assert_eq!(clock::clamp(clock::MIN_TIMESTAMP), clock::MIN_TIMESTAMP);
        assert_eq!(clock::clamp(clock::END_TIMESTAMP - 1), clock::END_TIMESTAMP - 1);
        assert_eq!(clock::clamp(clock::END_TIMESTAMP), clock::MIN_TIMESTAMP);
        for raw in [i32::MIN, i32::MAX, 0, 63, 64, 65] {
            let v = workmem::clamp(raw);
            assert!((workmem::MIN_KB..=workmem::MAX_KB).contains(&v));
        }
        assert_eq!(workmem::clamp(64), 64);
        assert_eq!(workmem::clamp(workmem::MAX_KB), workmem::MAX_KB);
        assert_eq!(workmem::clamp(63), workmem::MAX_KB);
    }
}
