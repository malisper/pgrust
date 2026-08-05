//! pg_prng_diff: differential fuzz driver — shipped Rust `pg_prng` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_pg_prng_io.c). Crate under test: crates/common/pg_prng.
//!
//! Comparison planes: value bits (exact; f64 payloads compared as raw bits)
//! + post-state (s0, s1) after every call. There is no error plane: neither
//! side allocates or raises (pg_diff_errcode is asserted 0 anyway).
//!
//! Input layout: [selector][payload]; selector % 14 picks the arm. Payload is
//! consumed as little-endian fixed-width u64 fields (short payloads read as
//! zero-extended — every input length is valid):
//!   0  seed          field0 = seed                        -> post-state
//!   1  fseed         field0 = f64 bits (carve below)      -> post-state
//!   2  seed_check    field0 = s0, field1 = s1             -> bool + state
//!   3  next_u64      s0, s1                               -> u64 + state
//!   4  u64_range     s0, s1, rmin, rmax                   -> u64 + state
//!   5  next_i64      s0, s1                               -> i64 + state
//!   6  next_nonnegative_i64  s0, s1                       -> i64 + state
//!   7  i64_range     s0, s1, rmin i64, rmax i64           -> i64 + state
//!   8  next_u32      s0, s1                               -> u32 + state
//!   9  next_i32      s0, s1                               -> i32 + state
//!   10 next_nonnegative_i32  s0, s1                       -> i32 + state
//!   11 next_f64      s0, s1                               -> f64 bits + state
//!   12 normal_f64    s0, s1                               -> f64 bits + state
//!   13 next_bool     s0, s1                               -> bool + state
//!
//! State construction routes through PgPrng::from_raw / .raw() (and arm 0
//! through seeded()), so the pgrust-only accessors execute every iteration
//! with the C post-state as their oracle.
//!
//! DOMAIN CARVES (C caller-contract / UB fences — the carve fences C UB,
//! never pgrust behavior):
//!   - fseed (arm 1): C computes `int64 seed = ((double)((1<<52)-1)) * fseed`;
//!     non-finite or |fseed| > 1.0 overflows the double->int64 cast, UB in C
//!     (Rust `as` saturates — deliberate, documented divergence).
//!     pg_prng_fseed's documented contract is "a double in the range
//!     [-1.0, 1.0]"; the driver folds raw bits into that domain and skips
//!     non-finite values.
//!   - normal_f64 (arm 12): both sides call the SAME in-process libm
//!     (log/sin; sqrt is IEEE-exact); bit-exact equality asserted. This is
//!     an in-process statement about "identical wrapper logic over one
//!     libm", which is exactly what real PostgreSQL computes on the same
//!     platform.
//!
//! FC-WRAPPER PLANE: not applicable — crates/common/pg_prng has no
//! builtins.rs / fc_* surface (non-SQL common helper crate).
//!
//! SKIPPED (phase-1 filter, per the claims-row carve):
//!   - global_prng / init_seams: thread-local session state + pgrust-only
//!     seam glue.
//!   - C pg_prng_strong_seed: OS-entropy macro, not ported by the crate.

#![allow(dead_code)]

use pg_prng::PgPrng;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;

    fn pg_diff_prng_u64(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> u64;
    fn pg_diff_prng_seed(seed: u64, out_s0: *mut u64, out_s1: *mut u64);
    fn pg_diff_prng_fseed(fseed: f64, out_s0: *mut u64, out_s1: *mut u64);
    fn pg_diff_prng_seed_check(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> i32;
    fn pg_diff_prng_u64_range(
        s0: u64,
        s1: u64,
        rmin: u64,
        rmax: u64,
        out_s0: *mut u64,
        out_s1: *mut u64,
    ) -> u64;
    fn pg_diff_prng_i64(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> i64;
    fn pg_diff_prng_i64p(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> i64;
    fn pg_diff_prng_i64_range(
        s0: u64,
        s1: u64,
        rmin: i64,
        rmax: i64,
        out_s0: *mut u64,
        out_s1: *mut u64,
    ) -> i64;
    fn pg_diff_prng_u32(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> u32;
    fn pg_diff_prng_i32(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> i32;
    fn pg_diff_prng_i32p(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> i32;
    fn pg_diff_prng_double(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> f64;
    fn pg_diff_prng_double_normal(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64)
        -> f64;
    fn pg_diff_prng_bool(s0: u64, s1: u64, out_s0: *mut u64, out_s1: *mut u64) -> i32;
}

/// Little-endian u64 field reader; missing bytes read as zero so every
/// payload length is a valid input (short inputs still fuzz the arms).
fn u64_at(payload: &[u8], idx: usize) -> u64 {
    let mut b = [0u8; 8];
    let off = idx * 8;
    for (i, slot) in b.iter_mut().enumerate() {
        if let Some(&v) = payload.get(off + i) {
            *slot = v;
        }
    }
    u64::from_le_bytes(b)
}

/// Post-state plane: Rust PgPrng vs the C out-params, plus quiet-oracle check.
fn assert_state(rust: PgPrng, c_s0: u64, c_s1: u64, arm: &str) {
    let (r0, r1) = rust.raw();
    assert_eq!((r0, r1), (c_s0, c_s1), "post-state diverged in {arm}");
    assert_eq!(unsafe { pg_diff_errcode_get() }, 0, "oracle raised in {arm}");
}

pub fn pg_prng_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let s0 = u64_at(payload, 0);
    let s1 = u64_at(payload, 1);
    let a = u64_at(payload, 2);
    let b = u64_at(payload, 3);
    let (mut c0, mut c1) = (0u64, 0u64);

    match sel % 14 {
        0 => {
            // seed: exercises seeded() + seed() + ensure_seeded().
            let st = PgPrng::seeded(s0);
            unsafe { pg_diff_prng_seed(s0, &mut c0, &mut c1) };
            assert_state(st, c0, c1, "seed");
        }
        1 => {
            // fseed: fold bits into the contract domain [-1.0, 1.0].
            let raw = f64::from_bits(s0);
            if !raw.is_finite() {
                return;
            }
            let f = if raw.abs() <= 1.0 { raw } else { 1.0 / raw };
            let mut st = PgPrng::from_raw(0, 0);
            st.fseed(f);
            unsafe { pg_diff_prng_fseed(f, &mut c0, &mut c1) };
            assert_state(st, c0, c1, "fseed");
        }
        2 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let r = st.ensure_seeded();
            let cr = unsafe { pg_diff_prng_seed_check(s0, s1, &mut c0, &mut c1) };
            assert_eq!(r as i32, cr, "seed_check verdict");
            assert_state(st, c0, c1, "seed_check");
        }
        3 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_u64();
            let cv = unsafe { pg_diff_prng_u64(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v, cv, "next_u64 value");
            assert_state(st, c0, c1, "next_u64");
        }
        4 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.u64_range(a, b);
            let cv = unsafe { pg_diff_prng_u64_range(s0, s1, a, b, &mut c0, &mut c1) };
            assert_eq!(v, cv, "u64_range value");
            assert_state(st, c0, c1, "u64_range");
        }
        5 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_i64();
            let cv = unsafe { pg_diff_prng_i64(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v, cv, "next_i64 value");
            assert_state(st, c0, c1, "next_i64");
        }
        6 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_nonnegative_i64();
            let cv = unsafe { pg_diff_prng_i64p(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v, cv, "next_nonnegative_i64 value");
            assert!(v >= 0, "next_nonnegative_i64 sign");
            assert_state(st, c0, c1, "next_nonnegative_i64");
        }
        7 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let (rmin, rmax) = (a as i64, b as i64);
            let v = st.i64_range(rmin, rmax);
            let cv =
                unsafe { pg_diff_prng_i64_range(s0, s1, rmin, rmax, &mut c0, &mut c1) };
            assert_eq!(v, cv, "i64_range value");
            assert_state(st, c0, c1, "i64_range");
        }
        8 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_u32();
            let cv = unsafe { pg_diff_prng_u32(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v, cv, "next_u32 value");
            assert_state(st, c0, c1, "next_u32");
        }
        9 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_i32();
            let cv = unsafe { pg_diff_prng_i32(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v, cv, "next_i32 value");
            assert_state(st, c0, c1, "next_i32");
        }
        10 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_nonnegative_i32();
            let cv = unsafe { pg_diff_prng_i32p(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v, cv, "next_nonnegative_i32 value");
            assert!(v >= 0, "next_nonnegative_i32 sign");
            assert_state(st, c0, c1, "next_nonnegative_i32");
        }
        11 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_f64();
            let cv = unsafe { pg_diff_prng_double(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v.to_bits(), cv.to_bits(), "next_f64 bits");
            assert_state(st, c0, c1, "next_f64");
        }
        12 => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.normal_f64();
            let cv = unsafe { pg_diff_prng_double_normal(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v.to_bits(), cv.to_bits(), "normal_f64 bits");
            assert_state(st, c0, c1, "normal_f64");
        }
        _ => {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_bool();
            let cv = unsafe { pg_diff_prng_bool(s0, s1, &mut c0, &mut c1) };
            assert_eq!(v as i32, cv, "next_bool value");
            assert_state(st, c0, c1, "next_bool");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed sweep: every arm executes against the C oracle with real
    /// values on every `cargo test` run (link + shim smoke).
    #[test]
    fn arm_sweep() {
        let _serial = crate::c_oracle_serial();
        let states: [(u64, u64); 6] = [
            (0, 0),
            (1, 0),
            (0, 1),
            (0xdead_beef_cafe_f00d, 0x0123_4567_89ab_cdef),
            (u64::MAX, u64::MAX),
            (0x5851_f42d_4c95_7f2d, 0x1405_7b7e_f767_814f),
        ];
        for sel in 0u8..14 {
            for (s0, s1) in states {
                for (a, b) in [(0u64, 0), (0, 1), (1, u64::MAX), (5, 10), (u64::MAX, 0)] {
                    let mut data = vec![sel];
                    data.extend_from_slice(&s0.to_le_bytes());
                    data.extend_from_slice(&s1.to_le_bytes());
                    data.extend_from_slice(&a.to_le_bytes());
                    data.extend_from_slice(&b.to_le_bytes());
                    pg_prng_diff(&data);
                }
            }
        }
    }

    /// Single-field witness pairs (skill obligation): (s0, s1) merge into
    /// xoroshiro's state update — each field's low and high bits must
    /// independently steer the (value, post-state) image.
    #[test]
    fn single_field_witness_pairs() {
        let _serial = crate::c_oracle_serial();
        let base = (0x0102_0304_0506_0708u64, 0x1112_1314_1516_1718u64);
        let mut images = std::collections::HashSet::new();
        for (s0, s1) in [
            base,
            (base.0 ^ 1, base.1),
            (base.0, base.1 ^ 1),
            (base.0 ^ (1 << 63), base.1),
            (base.0, base.1 ^ (1 << 63)),
        ] {
            let mut st = PgPrng::from_raw(s0, s1);
            let v = st.next_u64();
            let (p0, p1) = st.raw();
            assert!(images.insert((v, p0, p1)), "field delta not witnessed");
            let mut data = vec![3u8];
            data.extend_from_slice(&s0.to_le_bytes());
            data.extend_from_slice(&s1.to_le_bytes());
            pg_prng_diff(&data);
        }
    }

    /// Replay every checked-in seed (catches shim/link drift before the
    /// fleet campaign). Corpus is COMMITTED.
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/pg_prng_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/pg_prng_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                pg_prng_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}
