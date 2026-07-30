//! Kani C≡Rust equivalence: the interval comparator family (pg_proc rows
//! 1162–1167 interval_{eq,ne,lt,le,ge,gt}, 1315 interval_cmp, 1197/1198
//! interval_{smaller,larger}).
//!
//! Rust side: the SHIPPED fmgr wrappers `adt_date::builtins::fc_interval_*`
//! invoked through a real `LocalFcinfo<2>` frame, so the proof covers the
//! whole shipped path: by-ref datum unwrap (`arg_interval`'s field-wise
//! 16-byte image read) → `adt_timestamp::interval::interval_cmp_internal`
//! (i128 microsecond-span fold) → `Datum::from_bool/from_i32` /
//! winning-input-datum passthrough. C side: vendored timestamp.c +
//! int128.h native-int128 arm (c/pg_interval_cmp.c) — CBMC models
//! `__int128` directly.
//!
//! ## Domain (per row, three standing harnesses)
//!
//! The i128 constant-multiply fold `time + (month*30 + day)*86_400_000_000`
//! is a measured SLOPED WALL when month and day are BOTH symbolic at full
//! width (see proofs/TRIAGE.md, this family's entry). Coverage is therefore
//! split into proved planes + spots, each honest about its bounds:
//!
//! - `*_m0` — months LITERALLY 0 both sides (the m*30 term constant-folds),
//!   day full symbolic i32, time full symbolic i64. Covers every
//!   day/time-precision interval (e.g. justify-free timestamp differences).
//! - `*_band` — both-symbolic |day|,|month| <= 1_000, time full symbolic
//!   i64. Covers all real-calendar intervals within ±83 years of months
//!   and ±2.7 years of days, at any time value.
//! - `*_spots` — corner spots: one input concrete at (i64::MAX, i32::MAX,
//!   i32::MAX) then (i64::MIN, i32::MIN, i32::MIN), other input FULLY
//!   symbolic (full time/day/month).
//!
//! REMAINDER (recorded as wall, not proved): both inputs with symbolic
//! month != 0 and |day| or |month| beyond the band. Measured boundary:
//! both-banded 1e6 solves ~34s (calibration harness `cal_interval_cmp_band1e6`,
//! release-gate tier); 1e7 and full-i32 wall (>60s); month-symbolic ×
//! full-width day walls even at |month| <= 1000 (the two-contributor
//! multiplicand is the wall face, not the multiply width itself: a
//! disjunctive m==0 assume also walls because the fold only happens for a
//! LITERAL zero). No fences: both sides are total over the full struct
//! domain (|month*30+day| < 2^37, ×86.4e9 < 2^74 ≪ i128::MAX), so Kani's
//! overflow checks prove totality as a side condition on every harness.
//!
//! interval_larger/smaller: C returns the winning INPUT POINTER, the shipped
//! wrapper the winning input datum. The C shim reports the winning-arg index
//! from the verbatim selection; the harness asserts the returned datum is
//! the matching input image pointer (datetime-cmp timetz_larger precedent).
//!
//! Negative control: control_interval_lt_vs_c_le pits fc_interval_lt against
//! C interval_le — must FAIL (counterexample at span1 == span2). Run it with
//! the DEFAULT solver; expected-green harnesses with kissat.

#[cfg(kani)]
mod proofs {
    use proof_support::call2_ok;

    use std::os::raw::c_int;

    extern "C" {
        fn pg_interval_eq(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_ne(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_lt(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_gt(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_le(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_ge(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_cmp(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        /// winning-arg index (0 = first input, 1 = second) — shim, see C header
        fn pg_interval_smaller(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
        fn pg_interval_larger(t1: i64, d1: i32, m1: i32, t2: i64, d2: i32, m2: i32) -> c_int;
    }

    const BAND: i32 = 1_000;

    /// interval rides by-ref: the shipped wrapper reads time/day/month from a
    /// 16-byte image through arg_ptr (read_unaligned), exactly the on-disk/
    /// fmgr layout adt_date::builtins::arg_interval expects (time @0,
    /// day @8, month @12 — layout-asserted in adt_datetime::consts).
    fn iv_img(time: i64, day: i32, month: i32) -> [u8; 16] {
        let mut img = [0u8; 16];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..12].copy_from_slice(&day.to_ne_bytes());
        img[12..].copy_from_slice(&month.to_ne_bytes());
        img
    }

    /// One dual-execution check: shipped fc_* wrapper vs vendored C, output
    /// compared through `$extract`.
    macro_rules! check {
        ($fc:ident / $pg:ident, ($t1:expr, $d1:expr, $m1:expr), ($t2:expr, $d2:expr, $m2:expr),
         $extract:ident as $cast:ty) => {{
            let i1 = iv_img($t1, $d1, $m1);
            let i2 = iv_img($t2, $d2, $m2);
            let r = call2_ok(adt_date::builtins::$fc, i1.as_ptr(), i2.as_ptr());
            let c = unsafe { $pg($t1, $d1, $m1, $t2, $d2, $m2) };
            assert!(r.$extract() as $cast == c);
        }};
    }

    /// Winning-input-datum identity check for larger/smaller.
    macro_rules! check_minmax {
        ($fc:ident / $pg:ident, ($t1:expr, $d1:expr, $m1:expr), ($t2:expr, $d2:expr, $m2:expr)) => {{
            let i1 = iv_img($t1, $d1, $m1);
            let i2 = iv_img($t2, $d2, $m2);
            let r = call2_ok(adt_date::builtins::$fc, i1.as_ptr(), i2.as_ptr());
            let c = unsafe { $pg($t1, $d1, $m1, $t2, $d2, $m2) };
            let want = if c == 0 { i1.as_ptr() } else { i2.as_ptr() } as usize;
            assert!(r.as_usize() == want);
        }};
    }

    /// The three standing harnesses for one comparator row. `$bw` is the
    /// row's both-symbolic band half-width (1000 default; interval_le is a
    /// measured solver outlier and stands at 500 — see module doc).
    macro_rules! row {
        ($m0:ident, $band:ident, $spots:ident: $fc:ident / $pg:ident $extract:ident as $cast:ty, $bw:expr) => {
            /// months == 0 plane (literal 0: the m*30 term must constant-fold
            /// or the harness walls); full symbolic day + time.
            #[kani::proof]
            fn $m0() {
                let (t1, d1): (i64, i32) = (kani::any(), kani::any());
                let (t2, d2): (i64, i32) = (kani::any(), kani::any());
                check!($fc / $pg, (t1, d1, 0), (t2, d2, 0), $extract as $cast);
            }

            /// both-symbolic band |day|,|month| <= 1000; full symbolic time.
            #[kani::proof]
            fn $band() {
                let (t1, d1, m1): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                kani::assume((-$bw..=$bw).contains(&d1) && (-$bw..=$bw).contains(&m1));
                kani::assume((-$bw..=$bw).contains(&d2) && (-$bw..=$bw).contains(&m2));
                check!($fc / $pg, (t1, d1, m1), (t2, d2, m2), $extract as $cast);
            }

            /// corner spots: concrete extreme input vs FULLY symbolic other.
            #[kani::proof]
            fn $spots() {
                let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                check!(
                    $fc / $pg,
                    (i64::MAX, i32::MAX, i32::MAX),
                    (t2, d2, m2),
                    $extract as $cast
                );
                check!(
                    $fc / $pg,
                    (i64::MIN, i32::MIN, i32::MIN),
                    (t2, d2, m2),
                    $extract as $cast
                );
            }
        };
    }

    row!(eq_interval_eq_m0, eq_interval_eq_band, eq_interval_eq_spots:
        fc_interval_eq / pg_interval_eq as_bool as c_int, 1000);
    row!(eq_interval_ne_m0, eq_interval_ne_band, eq_interval_ne_spots:
        fc_interval_ne / pg_interval_ne as_bool as c_int, 1000);
    row!(eq_interval_lt_m0, eq_interval_lt_band, eq_interval_lt_spots:
        fc_interval_lt / pg_interval_lt as_bool as c_int, 1000);
    row!(eq_interval_gt_m0, eq_interval_gt_band, eq_interval_gt_spots:
        fc_interval_gt / pg_interval_gt as_bool as c_int, 1000);
    row!(eq_interval_le_m0, eq_interval_le_band, eq_interval_le_spots:
        fc_interval_le / pg_interval_le as_bool as c_int, 500);
    row!(eq_interval_ge_m0, eq_interval_ge_band, eq_interval_ge_spots:
        fc_interval_ge / pg_interval_ge as_bool as c_int, 1000);
    row!(eq_interval_cmp_m0, eq_interval_cmp_band, eq_interval_cmp_spots:
        fc_interval_cmp / pg_interval_cmp as_i32 as i32, 1000);

    /// The three standing harnesses for one larger/smaller row
    /// (winning-input-datum identity).
    macro_rules! row_minmax {
        ($m0:ident, $band:ident, $spots:ident: $fc:ident / $pg:ident) => {
            #[kani::proof]
            fn $m0() {
                let (t1, d1): (i64, i32) = (kani::any(), kani::any());
                let (t2, d2): (i64, i32) = (kani::any(), kani::any());
                check_minmax!($fc / $pg, (t1, d1, 0), (t2, d2, 0));
            }

            #[kani::proof]
            fn $band() {
                let (t1, d1, m1): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                kani::assume((-BAND..=BAND).contains(&d1) && (-BAND..=BAND).contains(&m1));
                kani::assume((-BAND..=BAND).contains(&d2) && (-BAND..=BAND).contains(&m2));
                check_minmax!($fc / $pg, (t1, d1, m1), (t2, d2, m2));
            }

            #[kani::proof]
            fn $spots() {
                let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                check_minmax!($fc / $pg, (i64::MAX, i32::MAX, i32::MAX), (t2, d2, m2));
                check_minmax!($fc / $pg, (i64::MIN, i32::MIN, i32::MIN), (t2, d2, m2));
            }
        };
    }

    row_minmax!(eq_interval_smaller_m0, eq_interval_smaller_band, eq_interval_smaller_spots:
        fc_interval_smaller / pg_interval_smaller);
    row_minmax!(eq_interval_larger_m0, eq_interval_larger_band, eq_interval_larger_spots:
        fc_interval_larger / pg_interval_larger);

    // ---------- calibration (release-gate tier, NOT the standing suite) ----------

    /// Measured wall-boundary witness: both-symbolic |day|,|month| <= 1e6
    /// solves ~34s (kissat, idle laptop) — over the 10s standing budget but
    /// under the 30s+slack kill line. 1e7 and full-i32 wall (>60s). Keep as
    /// a release-gate-tier calibration proof (encoding/unicode precedent).
    #[kani::proof]
    fn cal_interval_cmp_band1e6() {
        let (t1, d1, m1): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        const B: i32 = 1_000_000;
        kani::assume((-B..=B).contains(&d1) && (-B..=B).contains(&m1));
        kani::assume((-B..=B).contains(&d2) && (-B..=B).contains(&m2));
        check!(fc_interval_cmp / pg_interval_cmp, (t1, d1, m1), (t2, d2, m2), as_i32 as i32);
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_interval_lt vs C interval_le. MUST
    /// fail with a counterexample at span1 == span2. DEFAULT solver (kissat
    /// is non-incremental and effectively never terminates on failing
    /// harnesses).
    #[kani::proof]
    fn control_interval_lt_vs_c_le() {
        let (t1, d1): (i64, i32) = (kani::any(), kani::any());
        let (t2, d2): (i64, i32) = (kani::any(), kani::any());
        let i1 = iv_img(t1, d1, 0);
        let i2 = iv_img(t2, d2, 0);
        let r = call2_ok(adt_date::builtins::fc_interval_lt, i1.as_ptr(), i2.as_ptr());
        let c = unsafe { pg_interval_le(t1, d1, 0, t2, d2, 0) };
        assert!(r.as_bool() as c_int == c);
    }
}
