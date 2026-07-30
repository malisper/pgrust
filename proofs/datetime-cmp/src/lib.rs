//! Kani C≡Rust equivalence: the date/time/timetz/timestamp[tz] comparator
//! family (~35 pg_proc rows).
//!
//! Rust side: the SHIPPED fmgr wrappers — `adt_date::builtins::fc_{date,time,
//! timetz}_{eq,ne,lt,le,gt,ge,cmp}` and `adt_timestamp::builtins::
//! fc_timestamp_*` — invoked through a real `LocalFcinfo<2>` frame, so the
//! proof covers the whole shipped path: datum unwrap (arg_i32/arg_i64/
//! arg_ptr timetz image read) → core comparison → Datum::from_bool/from_i32.
//! C side: vendored date.c/timestamp.c (c/pg_datetime_cmp.c).
//!
//! The timestamptz comparator rows (oids 1152–1157, 2045) are the SAME
//! functions on both sides: pg_proc maps them onto C timestamp_eq..timestamp_
//! cmp, and pgrust registers fc_timestamp_* for those oids; TimestampTz is
//! int64 micros like Timestamp. The timestamp harnesses therefore discharge
//! both oid sets.
//!
//! Domains: full symbolic i32 (date), i64 (time, timestamp). timetz: full
//! symbolic (time: i64, zone: i32) fenced to non-overflowing
//! `time + zone*1_000_000` — C compiles with -fwrapv and wraps outside the
//! fence while Kani flags the Rust `+` overflow; every constructible timetz
//! value (time in [0, USECS_PER_DAY], |zone| < TZDISP_LIMIT=57600, enforced
//! at timetz_in/timetz_recv) sits far inside the fence. A contract-domain
//! harness (eq_timetz_cmp_contract) proves the datatype-invariant domain
//! explicitly.
//!
//! Negative control: control_date_lt_vs_c_le pits fc_date_lt against C
//! date_le — must FAIL (counterexample at a == b). Run it with the DEFAULT
//! solver, expected-green harnesses with kissat.
//!
//! EXTENSION (dt-minmax): larger/smaller rows and the date<->timestamp[tz]
//! cross-type comparator families, again through the shipped fc_* wrappers.
//!
//! - {date,time,timestamp}_{larger,smaller}: full symbolic domains. The
//!   timestamp harnesses discharge BOTH the timestamp rows (1195/1196) and
//!   the timestamptz rows (2035/2036): pg_proc maps both oid pairs onto the
//!   same C timestamp_smaller/larger, and pgrust registers the same
//!   fc_timestamp_smaller/larger for both.
//! - timetz_{larger,smaller}: C returns the winning input pointer; the
//!   shipped wrapper returns the winning input datum. C side is shimmed to
//!   a winning-arg index and the harness checks the returned datum is the
//!   matching input pointer. Same tz_fence as the timetz comparators.
//! - date_*_timestamp / timestamp_*_date: full symbolic i32 x i64 fenced by
//!   `date_ts_fence`: the date->timestamp promotion multiplies
//!   date * USECS_PER_DAY, which overflows i64 for dates <= -106_751_992
//!   (C -fwrapv wraps, Rust release wraps identically, Kani flags the `*`).
//!   Every constructible date (IS_VALID_DATE, |date| < 2_936_963 julian
//!   window) sits far inside the fence; dates >= 106_751_983 take the
//!   overflow arm BEFORE the multiply and are fully covered, as are both
//!   infinity sentinels.
//! - date_*_timestamptz / timestamptz_*_date: the promotion routes through
//!   session-timezone state. C and Rust share the identical seam shape
//!   (j2date(date + POSTGRES_EPOCH_JDATE) -> DetermineTimeZoneOffset(tm,
//!   session_timezone) -> date*USECS_PER_DAY + tz*USECS_PER_SEC), so the
//!   seam is fenced to a shared SYMBOLIC offset model: the C side reads
//!   pg_model_tz_offset, the Rust side stubs adt_datetime::tz::
//!   DetermineTimeZoneOffset to return the same value (and the session
//!   timezone is set to a dummy fixed zone so the shipped
//!   session_timezone() lookup succeeds). The offset is universally
//!   quantified over (-SECS_PER_DAY, SECS_PER_DAY) — a superset of every
//!   offset the real seam can produce (tzcode invariant: |gmtoff| < 24h) —
//!   so the proof covers every possible seam output; the seam INTERNALS
//!   (j2date + DetermineTimeZoneOffset themselves) are outside the proof.
//!   Ledger wording: proved modulo shared tz-seam model. The extra
//!   `date_tstz_fence` add-clause fences the i64 `+ tz*USECS_PER_SEC`
//!   overflow the same way (only non-constructible dates are excluded).
//!   Negative control control_tz_model_skew feeds the two sides DIFFERENT
//!   offsets and must FAIL — witness that the model offset is load-bearing
//!   on both sides.

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        fn pg_date_eq(a: i32, b: i32) -> c_int;
        fn pg_date_ne(a: i32, b: i32) -> c_int;
        fn pg_date_lt(a: i32, b: i32) -> c_int;
        fn pg_date_le(a: i32, b: i32) -> c_int;
        fn pg_date_gt(a: i32, b: i32) -> c_int;
        fn pg_date_ge(a: i32, b: i32) -> c_int;
        fn pg_date_cmp(a: i32, b: i32) -> c_int;

        fn pg_time_eq(a: i64, b: i64) -> c_int;
        fn pg_time_ne(a: i64, b: i64) -> c_int;
        fn pg_time_lt(a: i64, b: i64) -> c_int;
        fn pg_time_le(a: i64, b: i64) -> c_int;
        fn pg_time_gt(a: i64, b: i64) -> c_int;
        fn pg_time_ge(a: i64, b: i64) -> c_int;
        fn pg_time_cmp(a: i64, b: i64) -> c_int;

        fn pg_timetz_eq(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_ne(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_lt(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_le(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_gt(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_ge(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_cmp(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;

        // dt-minmax extension
        fn pg_date_larger(a: i32, b: i32) -> i32;
        fn pg_date_smaller(a: i32, b: i32) -> i32;
        fn pg_time_larger(a: i64, b: i64) -> i64;
        fn pg_time_smaller(a: i64, b: i64) -> i64;
        fn pg_timetz_larger(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timetz_smaller(t1: i64, z1: i32, t2: i64, z2: i32) -> c_int;
        fn pg_timestamp_larger(a: i64, b: i64) -> i64;
        fn pg_timestamp_smaller(a: i64, b: i64) -> i64;

        fn pg_date_eq_timestamp(d: i32, ts: i64) -> c_int;
        fn pg_date_ne_timestamp(d: i32, ts: i64) -> c_int;
        fn pg_date_lt_timestamp(d: i32, ts: i64) -> c_int;
        fn pg_date_gt_timestamp(d: i32, ts: i64) -> c_int;
        fn pg_date_le_timestamp(d: i32, ts: i64) -> c_int;
        fn pg_date_ge_timestamp(d: i32, ts: i64) -> c_int;
        fn pg_date_cmp_timestamp(d: i32, ts: i64) -> c_int;

        fn pg_timestamp_eq_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamp_ne_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamp_lt_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamp_gt_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamp_le_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamp_ge_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamp_cmp_date(ts: i64, d: i32) -> c_int;

        fn pg_date_eq_timestamptz(d: i32, ts: i64) -> c_int;
        fn pg_date_ne_timestamptz(d: i32, ts: i64) -> c_int;
        fn pg_date_lt_timestamptz(d: i32, ts: i64) -> c_int;
        fn pg_date_gt_timestamptz(d: i32, ts: i64) -> c_int;
        fn pg_date_le_timestamptz(d: i32, ts: i64) -> c_int;
        fn pg_date_ge_timestamptz(d: i32, ts: i64) -> c_int;
        fn pg_date_cmp_timestamptz(d: i32, ts: i64) -> c_int;

        fn pg_timestamptz_eq_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamptz_ne_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamptz_lt_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamptz_gt_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamptz_le_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamptz_ge_date(ts: i64, d: i32) -> c_int;
        fn pg_timestamptz_cmp_date(ts: i64, d: i32) -> c_int;

        /// shared tz-seam model offset for the timestamptz arms
        static mut pg_model_tz_offset: i32;

        fn pg_timestamp_eq(a: i64, b: i64) -> c_int;
        fn pg_timestamp_ne(a: i64, b: i64) -> c_int;
        fn pg_timestamp_lt(a: i64, b: i64) -> c_int;
        fn pg_timestamp_le(a: i64, b: i64) -> c_int;
        fn pg_timestamp_gt(a: i64, b: i64) -> c_int;
        fn pg_timestamp_ge(a: i64, b: i64) -> c_int;
        fn pg_timestamp_cmp(a: i64, b: i64) -> c_int;
    }

    const USECS_PER_SEC: i64 = 1_000_000;
    const USECS_PER_DAY: i64 = 86_400_000_000;
    const TZDISP_LIMIT: i32 = 16 * 3600; // (MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR

    fn fci2(a: Datum, b: Datum) -> LocalFcinfo<2> {
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        f
    }

    /// Run a shipped fc_* wrapper on a 2-arg frame; the comparators never
    /// error, so the Err arm is statically dead.
    fn call<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
        b: Datum,
    ) -> Datum {
        let mut f = fci2(a, b);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("comparator errored"),
        }
    }

    // ---------- date: full symbolic i32 × i32 ----------

    macro_rules! date_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: i32 = kani::any();
                let b: i32 = kani::any();
                let r = call(adt_date::builtins::$fc, Datum::from_i32(a), Datum::from_i32(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    date_op! {
        eq_date_eq: fc_date_eq / pg_date_eq;
        eq_date_ne: fc_date_ne / pg_date_ne;
        eq_date_lt: fc_date_lt / pg_date_lt;
        eq_date_le: fc_date_le / pg_date_le;
        eq_date_gt: fc_date_gt / pg_date_gt;
        eq_date_ge: fc_date_ge / pg_date_ge;
    }

    #[kani::proof]
    fn eq_date_cmp() {
        let a: i32 = kani::any();
        let b: i32 = kani::any();
        let r = call(adt_date::builtins::fc_date_cmp, Datum::from_i32(a), Datum::from_i32(b));
        let c = unsafe { pg_date_cmp(a, b) };
        assert!(r.as_i32() == c);
    }

    // ---------- time + timestamp[tz]: full symbolic i64 × i64 ----------

    macro_rules! i64_op {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: i64 = kani::any();
                let b: i64 = kani::any();
                let r = call($krate::builtins::$fc, Datum::from_i64(a), Datum::from_i64(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    i64_op! {
        eq_time_eq: adt_date::fc_time_eq / pg_time_eq;
        eq_time_ne: adt_date::fc_time_ne / pg_time_ne;
        eq_time_lt: adt_date::fc_time_lt / pg_time_lt;
        eq_time_le: adt_date::fc_time_le / pg_time_le;
        eq_time_gt: adt_date::fc_time_gt / pg_time_gt;
        eq_time_ge: adt_date::fc_time_ge / pg_time_ge;
        eq_timestamp_eq: adt_timestamp::fc_timestamp_eq / pg_timestamp_eq;
        eq_timestamp_ne: adt_timestamp::fc_timestamp_ne / pg_timestamp_ne;
        eq_timestamp_lt: adt_timestamp::fc_timestamp_lt / pg_timestamp_lt;
        eq_timestamp_le: adt_timestamp::fc_timestamp_le / pg_timestamp_le;
        eq_timestamp_gt: adt_timestamp::fc_timestamp_gt / pg_timestamp_gt;
        eq_timestamp_ge: adt_timestamp::fc_timestamp_ge / pg_timestamp_ge;
    }

    #[kani::proof]
    fn eq_time_cmp() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let r = call(adt_date::builtins::fc_time_cmp, Datum::from_i64(a), Datum::from_i64(b));
        let c = unsafe { pg_time_cmp(a, b) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    fn eq_timestamp_cmp() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let r = call(
            adt_timestamp::builtins::fc_timestamp_cmp,
            Datum::from_i64(a),
            Datum::from_i64(b),
        );
        let c = unsafe { pg_timestamp_cmp(a, b) };
        assert!(r.as_i32() == c);
    }

    // ---------- timetz: (i64 time, i32 zone) pairs as 12-byte images ----------

    /// timetz rides by-ref: the shipped wrapper reads time/zone from a
    /// 12-byte image through arg_ptr (read_unaligned), exactly the on-disk/
    /// fmgr layout adt_date::builtins::arg_timetz expects.
    fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
        let mut img = [0u8; 12];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..].copy_from_slice(&zone.to_ne_bytes());
        img
    }

    /// Fence: the effective-time addition must not overflow i64. C (-fwrapv)
    /// wraps outside this; Rust release wraps identically but Kani's overflow
    /// check would flag it. Unreachable for real timetz values.
    fn tz_fence(time: i64, zone: i32) -> bool {
        time.checked_add(zone as i64 * USECS_PER_SEC).is_some()
    }

    macro_rules! timetz_op {
        ($($h:ident: $fc:ident / $pg:ident $extract:ident $cast:ty;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (t1, z1): (i64, i32) = (kani::any(), kani::any());
                let (t2, z2): (i64, i32) = (kani::any(), kani::any());
                kani::assume(tz_fence(t1, z1) && tz_fence(t2, z2));
                let i1 = timetz_img(t1, z1);
                let i2 = timetz_img(t2, z2);
                let r = call(
                    adt_date::builtins::$fc,
                    Datum::from_usize(i1.as_ptr() as usize),
                    Datum::from_usize(i2.as_ptr() as usize),
                );
                let c = unsafe { $pg(t1, z1, t2, z2) };
                assert!(r.$extract() as $cast == c);
            }
        )*};
    }

    timetz_op! {
        eq_timetz_eq: fc_timetz_eq / pg_timetz_eq as_bool c_int;
        eq_timetz_ne: fc_timetz_ne / pg_timetz_ne as_bool c_int;
        eq_timetz_lt: fc_timetz_lt / pg_timetz_lt as_bool c_int;
        eq_timetz_le: fc_timetz_le / pg_timetz_le as_bool c_int;
        eq_timetz_gt: fc_timetz_gt / pg_timetz_gt as_bool c_int;
        eq_timetz_ge: fc_timetz_ge / pg_timetz_ge as_bool c_int;
        eq_timetz_cmp: fc_timetz_cmp / pg_timetz_cmp as_i32 i32;
    }

    /// Contract-domain harness: the invariants timetz_in/timetz_recv enforce
    /// on every constructible value (time in [0, USECS_PER_DAY], |zone| <
    /// TZDISP_LIMIT). Strictly inside the fenced domain above; stands as the
    /// documented in-contract proof.
    #[kani::proof]
    fn eq_timetz_cmp_contract() {
        let (t1, z1): (i64, i32) = (kani::any(), kani::any());
        let (t2, z2): (i64, i32) = (kani::any(), kani::any());
        kani::assume((0..=USECS_PER_DAY).contains(&t1) && (0..=USECS_PER_DAY).contains(&t2));
        kani::assume(z1 > -TZDISP_LIMIT && z1 < TZDISP_LIMIT);
        kani::assume(z2 > -TZDISP_LIMIT && z2 < TZDISP_LIMIT);
        let i1 = timetz_img(t1, z1);
        let i2 = timetz_img(t2, z2);
        let r = call(
            adt_date::builtins::fc_timetz_cmp,
            Datum::from_usize(i1.as_ptr() as usize),
            Datum::from_usize(i2.as_ptr() as usize),
        );
        let c = unsafe { pg_timetz_cmp(t1, z1, t2, z2) };
        assert!(r.as_i32() == c);
    }

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: shipped fc_date_lt vs C date_le. MUST fail with a
    /// counterexample at a == b. Run with the DEFAULT solver (kissat is
    /// non-incremental and effectively never terminates on failing
    /// harnesses).
    #[kani::proof]
    fn control_date_lt_vs_c_le() {
        let a: i32 = kani::any();
        let b: i32 = kani::any();
        let r = call(adt_date::builtins::fc_date_lt, Datum::from_i32(a), Datum::from_i32(b));
        let c = unsafe { pg_date_le(a, b) };
        assert!(r.as_bool() as c_int == c);
    }

    // ================= dt-minmax extension =================

    // ---------- larger/smaller ----------

    #[kani::proof]
    fn eq_date_larger() {
        let (a, b): (i32, i32) = (kani::any(), kani::any());
        let r = call(adt_date::builtins::fc_date_larger, Datum::from_i32(a), Datum::from_i32(b));
        let c = unsafe { pg_date_larger(a, b) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    fn eq_date_smaller() {
        let (a, b): (i32, i32) = (kani::any(), kani::any());
        let r = call(adt_date::builtins::fc_date_smaller, Datum::from_i32(a), Datum::from_i32(b));
        let c = unsafe { pg_date_smaller(a, b) };
        assert!(r.as_i32() == c);
    }

    macro_rules! i64_minmax {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (a, b): (i64, i64) = (kani::any(), kani::any());
                let r = call($krate::builtins::$fc, Datum::from_i64(a), Datum::from_i64(b));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_i64() == c);
            }
        )*};
    }

    i64_minmax! {
        eq_time_larger: adt_date::fc_time_larger / pg_time_larger;
        eq_time_smaller: adt_date::fc_time_smaller / pg_time_smaller;
        // discharges timestamp rows 1195/1196 AND timestamptz rows 2035/2036
        // (same C function, same registered fc wrapper for both oid pairs)
        eq_timestamp_larger: adt_timestamp::fc_timestamp_larger / pg_timestamp_larger;
        eq_timestamp_smaller: adt_timestamp::fc_timestamp_smaller / pg_timestamp_smaller;
    }

    /// timetz larger/smaller return the winning INPUT datum (C: the winning
    /// input pointer); C shim reports the winning arg index.
    macro_rules! timetz_minmax {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let (t1, z1): (i64, i32) = (kani::any(), kani::any());
                let (t2, z2): (i64, i32) = (kani::any(), kani::any());
                kani::assume(tz_fence(t1, z1) && tz_fence(t2, z2));
                let i1 = timetz_img(t1, z1);
                let i2 = timetz_img(t2, z2);
                let r = call(
                    adt_date::builtins::$fc,
                    Datum::from_usize(i1.as_ptr() as usize),
                    Datum::from_usize(i2.as_ptr() as usize),
                );
                let c = unsafe { $pg(t1, z1, t2, z2) };
                let want = if c == 0 { i1.as_ptr() } else { i2.as_ptr() } as usize;
                assert!(r.as_usize() == want);
            }
        )*};
    }

    timetz_minmax! {
        eq_timetz_larger: fc_timetz_larger / pg_timetz_larger;
        eq_timetz_smaller: fc_timetz_smaller / pg_timetz_smaller;
    }

    // ---------- date vs timestamp cross-type ----------

    const POSTGRES_EPOCH_JDATE: i32 = 2_451_545;
    const TIMESTAMP_END_JULIAN: i32 = 109_203_528;
    /// dates >= this take the overflow arm before any multiply
    const DATE_TS_UPPER: i32 = TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE;

    /// Fence: the date->timestamp promotion `date * USECS_PER_DAY` must not
    /// overflow i64 (C -fwrapv wraps, Rust release wraps identically, Kani
    /// flags it). Only dates <= -106_751_992 — far outside IS_VALID_DATE —
    /// are excluded; the infinity sentinel and the >= DATE_TS_UPPER overflow
    /// arm never reach the multiply and stay covered.
    /// i64::MAX / USECS_PER_DAY = 106_751_991: |date| at or below this cannot
    /// overflow `date * USECS_PER_DAY`.
    const DATE_MUL_SAFE: i32 = (i64::MAX / USECS_PER_DAY) as i32;

    fn date_ts_fence(d: i32) -> bool {
        d == i32::MIN || d >= -DATE_MUL_SAFE
    }

    macro_rules! date_ts_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let d: i32 = kani::any();
                let ts: i64 = kani::any();
                kani::assume(date_ts_fence(d));
                let r = call(adt_date::builtins::$fc, Datum::from_i32(d), Datum::from_i64(ts));
                let c = unsafe { $pg(d, ts) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    macro_rules! ts_date_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let ts: i64 = kani::any();
                let d: i32 = kani::any();
                kani::assume(date_ts_fence(d));
                let r = call(adt_date::builtins::$fc, Datum::from_i64(ts), Datum::from_i32(d));
                let c = unsafe { $pg(ts, d) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    date_ts_op! {
        eq_date_eq_timestamp: fc_date_eq_timestamp / pg_date_eq_timestamp;
        eq_date_ne_timestamp: fc_date_ne_timestamp / pg_date_ne_timestamp;
        eq_date_lt_timestamp: fc_date_lt_timestamp / pg_date_lt_timestamp;
        eq_date_gt_timestamp: fc_date_gt_timestamp / pg_date_gt_timestamp;
        eq_date_le_timestamp: fc_date_le_timestamp / pg_date_le_timestamp;
        eq_date_ge_timestamp: fc_date_ge_timestamp / pg_date_ge_timestamp;
    }

    ts_date_op! {
        eq_timestamp_eq_date: fc_timestamp_eq_date / pg_timestamp_eq_date;
        eq_timestamp_ne_date: fc_timestamp_ne_date / pg_timestamp_ne_date;
        eq_timestamp_lt_date: fc_timestamp_lt_date / pg_timestamp_lt_date;
        eq_timestamp_gt_date: fc_timestamp_gt_date / pg_timestamp_gt_date;
        eq_timestamp_le_date: fc_timestamp_le_date / pg_timestamp_le_date;
        eq_timestamp_ge_date: fc_timestamp_ge_date / pg_timestamp_ge_date;
    }

    #[kani::proof]
    fn eq_date_cmp_timestamp() {
        let d: i32 = kani::any();
        let ts: i64 = kani::any();
        kani::assume(date_ts_fence(d));
        let r = call(
            adt_date::builtins::fc_date_cmp_timestamp,
            Datum::from_i32(d),
            Datum::from_i64(ts),
        );
        let c = unsafe { pg_date_cmp_timestamp(d, ts) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    fn eq_timestamp_cmp_date() {
        let ts: i64 = kani::any();
        let d: i32 = kani::any();
        kani::assume(date_ts_fence(d));
        let r = call(
            adt_date::builtins::fc_timestamp_cmp_date,
            Datum::from_i64(ts),
            Datum::from_i32(d),
        );
        let c = unsafe { pg_timestamp_cmp_date(ts, d) };
        assert!(r.as_i32() == c);
    }

    // ---------- date vs timestamptz cross-type (shared tz-seam model) ----------

    const SECS_PER_DAY_I32: i32 = 86_400;

    /// dummy fixed zone so the shipped session_timezone() lookup succeeds;
    /// never read (the seam consuming it is stubbed to the model).
    static MODEL_TZ: localtime::PgTz = localtime::PgTz {
        tzname: [0; localtime::TZ_STRLEN_MAX + 1],
        state: localtime::TzState::new(),
    };

    static MODEL_TZ_OFF: core::sync::atomic::AtomicI32 = core::sync::atomic::AtomicI32::new(0);

    /// Stub for adt_datetime::tz::DetermineTimeZoneOffset: the shared
    /// symbolic seam offset. tm is deliberately ignored — the C side's model
    /// ignores it identically.
    fn model_tz_offset(_tm: &mut adt_datetime::pg_tm, _tzp: &localtime::PgTz) -> i32 {
        MODEL_TZ_OFF.load(core::sync::atomic::Ordering::Relaxed)
    }

    /// Stub for adt_datetime::calendar::j2date. tm feeds ONLY the (also
    /// stubbed) DetermineTimeZoneOffset, so this widens nothing: the modeled
    /// seam is the whole j2date -> DetermineTimeZoneOffset block, exactly
    /// what the C shim replaces. Keeping the real j2date live would drag its
    /// /146097 divider chain into every check AND trip Kani's overflow
    /// checks on its u32 wrapping arithmetic for far-out-of-range dates
    /// (C unsigned wrap is defined; Rust release wraps identically).
    fn model_j2date(_jd: i32, year: &mut i32, month: &mut i32, day: &mut i32) {
        *year = 2000;
        *month = 1;
        *day = 1;
    }

    /// Arm both sides of the seam model with the same offset.
    fn set_tz_model(off: i32) {
        MODEL_TZ_OFF.store(off, core::sync::atomic::Ordering::Relaxed);
        unsafe { pg_model_tz_offset = off };
        pgtz::set_session_timezone(Some(&MODEL_TZ));
    }

    /// tz-arm fence: multiply fence plus the `+ tz*USECS_PER_SEC` add on the
    /// promoted value (same wrap-vs-Kani-flag reasoning as date_ts_fence).
    /// Two extra days of headroom guarantee the subsequent `+ tz*USECS_PER_SEC`
    /// (|tz| < SECS_PER_DAY) cannot overflow either; in-range dates never
    /// reach the multiply from above (overflow arm at DATE_TS_UPPER).
    fn date_tstz_fence(d: i32, _off: i32) -> bool {
        d == i32::MIN || d >= -(DATE_MUL_SAFE - 2)
    }

    macro_rules! date_tstz_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
            #[kani::stub(adt_datetime::calendar::j2date, model_j2date)]
            fn $h() {
                let off: i32 = kani::any();
                kani::assume(off > -SECS_PER_DAY_I32 && off < SECS_PER_DAY_I32);
                set_tz_model(off);
                let d: i32 = kani::any();
                let ts: i64 = kani::any();
                kani::assume(date_tstz_fence(d, off));
                let r = call(adt_date::builtins::$fc, Datum::from_i32(d), Datum::from_i64(ts));
                let c = unsafe { $pg(d, ts) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    macro_rules! tstz_date_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
            #[kani::stub(adt_datetime::calendar::j2date, model_j2date)]
            fn $h() {
                let off: i32 = kani::any();
                kani::assume(off > -SECS_PER_DAY_I32 && off < SECS_PER_DAY_I32);
                set_tz_model(off);
                let ts: i64 = kani::any();
                let d: i32 = kani::any();
                kani::assume(date_tstz_fence(d, off));
                let r = call(adt_date::builtins::$fc, Datum::from_i64(ts), Datum::from_i32(d));
                let c = unsafe { $pg(ts, d) };
                assert!(r.as_bool() as c_int == c);
            }
        )*};
    }

    date_tstz_op! {
        eq_date_eq_timestamptz: fc_date_eq_timestamptz / pg_date_eq_timestamptz;
        eq_date_ne_timestamptz: fc_date_ne_timestamptz / pg_date_ne_timestamptz;
        eq_date_lt_timestamptz: fc_date_lt_timestamptz / pg_date_lt_timestamptz;
        eq_date_gt_timestamptz: fc_date_gt_timestamptz / pg_date_gt_timestamptz;
        eq_date_le_timestamptz: fc_date_le_timestamptz / pg_date_le_timestamptz;
        eq_date_ge_timestamptz: fc_date_ge_timestamptz / pg_date_ge_timestamptz;
    }

    tstz_date_op! {
        eq_timestamptz_eq_date: fc_timestamptz_eq_date / pg_timestamptz_eq_date;
        eq_timestamptz_ne_date: fc_timestamptz_ne_date / pg_timestamptz_ne_date;
        eq_timestamptz_lt_date: fc_timestamptz_lt_date / pg_timestamptz_lt_date;
        eq_timestamptz_gt_date: fc_timestamptz_gt_date / pg_timestamptz_gt_date;
        eq_timestamptz_le_date: fc_timestamptz_le_date / pg_timestamptz_le_date;
        eq_timestamptz_ge_date: fc_timestamptz_ge_date / pg_timestamptz_ge_date;
    }

    #[kani::proof]
    #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
    #[kani::stub(adt_datetime::calendar::j2date, model_j2date)]
    fn eq_date_cmp_timestamptz() {
        let off: i32 = kani::any();
        kani::assume(off > -SECS_PER_DAY_I32 && off < SECS_PER_DAY_I32);
        set_tz_model(off);
        let d: i32 = kani::any();
        let ts: i64 = kani::any();
        kani::assume(date_tstz_fence(d, off));
        let r = call(
            adt_date::builtins::fc_date_cmp_timestamptz,
            Datum::from_i32(d),
            Datum::from_i64(ts),
        );
        let c = unsafe { pg_date_cmp_timestamptz(d, ts) };
        assert!(r.as_i32() == c);
    }

    #[kani::proof]
    #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
    #[kani::stub(adt_datetime::calendar::j2date, model_j2date)]
    fn eq_timestamptz_cmp_date() {
        let off: i32 = kani::any();
        kani::assume(off > -SECS_PER_DAY_I32 && off < SECS_PER_DAY_I32);
        set_tz_model(off);
        let ts: i64 = kani::any();
        let d: i32 = kani::any();
        kani::assume(date_tstz_fence(d, off));
        let r = call(
            adt_date::builtins::fc_timestamptz_cmp_date,
            Datum::from_i64(ts),
            Datum::from_i32(d),
        );
        let c = unsafe { pg_timestamptz_cmp_date(ts, d) };
        assert!(r.as_i32() == c);
    }

    // ---------- timestamp vs timestamptz cross-type (oids 2520-2533) ----------
    // Same shared tz-seam model. The Rust conversion
    // (timestamp2timestamptz_opt_overflow) reaches the seam through
    // timestamp2tm, whose outputs feed ONLY the (stubbed)
    // DetermineTimeZoneOffset — so timestamp2tm is stubbed to a fixed tm
    // (widens nothing; keeping it live would drag its /USECS_PER_DAY +
    // j2date divider chains into every check). Its pro-forma failure arm is
    // always-success under the model on BOTH sides (documented in the C
    // file): that arm — where C ereports and the shipped Rust would panic
    // via .expect — is out of proof. dt2local's i64 wrap is compared
    // verbatim (wrapping_sub vs -fwrapv), so timestamps are fully symbolic:
    // no fence needed.

    extern "C" {
        fn pg_timestamp_eq_timestamptz(t: i64, tz: i64) -> c_int;
        fn pg_timestamp_ne_timestamptz(t: i64, tz: i64) -> c_int;
        fn pg_timestamp_lt_timestamptz(t: i64, tz: i64) -> c_int;
        fn pg_timestamp_gt_timestamptz(t: i64, tz: i64) -> c_int;
        fn pg_timestamp_le_timestamptz(t: i64, tz: i64) -> c_int;
        fn pg_timestamp_ge_timestamptz(t: i64, tz: i64) -> c_int;
        fn pg_timestamp_cmp_timestamptz(t: i64, tz: i64) -> i32;
        fn pg_timestamptz_eq_timestamp(tz: i64, t: i64) -> c_int;
        fn pg_timestamptz_ne_timestamp(tz: i64, t: i64) -> c_int;
        fn pg_timestamptz_lt_timestamp(tz: i64, t: i64) -> c_int;
        fn pg_timestamptz_gt_timestamp(tz: i64, t: i64) -> c_int;
        fn pg_timestamptz_le_timestamp(tz: i64, t: i64) -> c_int;
        fn pg_timestamptz_ge_timestamp(tz: i64, t: i64) -> c_int;
        fn pg_timestamptz_cmp_timestamp(tz: i64, t: i64) -> i32;
    }

    /// Stub for adt_timestamp::timestamp2tm: fixed tm + success. The tm
    /// feeds only the stubbed DetermineTimeZoneOffset (which ignores it),
    /// mirroring the C shim's seam block exactly.
    fn model_timestamp2tm(
        _dt: i64,
        _tzp: Option<&mut i32>,
        tm: &mut adt_datetime::pg_tm,
        _fsec: &mut adt_datetime::consts::fsec_t,
        _tzn: Option<&mut Option<&'static str>>,
        _attimezone: Option<&'static localtime::PgTz>,
    ) -> Result<(), ()> {
        tm.tm_year = 2000;
        tm.tm_mon = 1;
        tm.tm_mday = 1;
        Ok(())
    }

    macro_rules! ts_tstz_cross_op {
        ($($h:ident: $fc:ident / $pg:ident, $extract:ident, $cast:ty;)*) => {$(
            #[kani::proof]
            #[kani::stub(adt_timestamp::timestamp2tm, model_timestamp2tm)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
            fn $h() {
                let off: i32 = kani::any();
                kani::assume(off > -SECS_PER_DAY_I32 && off < SECS_PER_DAY_I32);
                set_tz_model(off);
                let a: i64 = kani::any();
                let b: i64 = kani::any();
                let r = call(
                    adt_timestamp::builtins::$fc,
                    Datum::from_i64(a),
                    Datum::from_i64(b),
                );
                let c = unsafe { $pg(a, b) };
                assert!(r.$extract() as $cast == c);
            }
        )*};
    }

    ts_tstz_cross_op! {
        eq_timestamp_eq_timestamptz: fc_timestamp_eq_timestamptz / pg_timestamp_eq_timestamptz, as_bool, c_int;
        eq_timestamp_ne_timestamptz: fc_timestamp_ne_timestamptz / pg_timestamp_ne_timestamptz, as_bool, c_int;
        eq_timestamp_lt_timestamptz: fc_timestamp_lt_timestamptz / pg_timestamp_lt_timestamptz, as_bool, c_int;
        eq_timestamp_gt_timestamptz: fc_timestamp_gt_timestamptz / pg_timestamp_gt_timestamptz, as_bool, c_int;
        eq_timestamp_le_timestamptz: fc_timestamp_le_timestamptz / pg_timestamp_le_timestamptz, as_bool, c_int;
        eq_timestamp_ge_timestamptz: fc_timestamp_ge_timestamptz / pg_timestamp_ge_timestamptz, as_bool, c_int;
        eq_timestamp_cmp_timestamptz: fc_timestamp_cmp_timestamptz / pg_timestamp_cmp_timestamptz, as_i32, i32;
        eq_timestamptz_eq_timestamp: fc_timestamptz_eq_timestamp / pg_timestamptz_eq_timestamp, as_bool, c_int;
        eq_timestamptz_ne_timestamp: fc_timestamptz_ne_timestamp / pg_timestamptz_ne_timestamp, as_bool, c_int;
        eq_timestamptz_lt_timestamp: fc_timestamptz_lt_timestamp / pg_timestamptz_lt_timestamp, as_bool, c_int;
        eq_timestamptz_gt_timestamp: fc_timestamptz_gt_timestamp / pg_timestamptz_gt_timestamp, as_bool, c_int;
        eq_timestamptz_le_timestamp: fc_timestamptz_le_timestamp / pg_timestamptz_le_timestamp, as_bool, c_int;
        eq_timestamptz_ge_timestamp: fc_timestamptz_ge_timestamp / pg_timestamptz_ge_timestamp, as_bool, c_int;
        eq_timestamptz_cmp_timestamp: fc_timestamptz_cmp_timestamp / pg_timestamptz_cmp_timestamp, as_i32, i32;
    }

    /// Negative control for the ts/tstz seam model: skewed offsets must
    /// FAIL, witnessing the model offset is load-bearing on both sides of
    /// the timestamp conversion too. DEFAULT solver.
    #[kani::proof]
    #[kani::stub(adt_timestamp::timestamp2tm, model_timestamp2tm)]
    #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
    fn control_ts_tstz_model_skew() {
        set_tz_model(3600);
        unsafe { pg_model_tz_offset = 7200 };
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        let r = call(
            adt_timestamp::builtins::fc_timestamp_cmp_timestamptz,
            Datum::from_i64(a),
            Datum::from_i64(b),
        );
        let c = unsafe { pg_timestamp_cmp_timestamptz(a, b) };
        assert!(r.as_i32() == c);
    }

    /// Negative control for the tz-seam model: C and Rust get DIFFERENT
    /// offsets — must FAIL, witnessing the model offset is load-bearing on
    /// both sides. DEFAULT solver.
    #[kani::proof]
    #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, model_tz_offset)]
    #[kani::stub(adt_datetime::calendar::j2date, model_j2date)]
    fn control_tz_model_skew() {
        set_tz_model(3600);
        unsafe { pg_model_tz_offset = 7200 };
        let d: i32 = kani::any();
        let ts: i64 = kani::any();
        kani::assume(date_tstz_fence(d, 3600) && date_tstz_fence(d, 7200));
        let r = call(
            adt_date::builtins::fc_date_cmp_timestamptz,
            Datum::from_i32(d),
            Datum::from_i64(ts),
        );
        let c = unsafe { pg_date_cmp_timestamptz(d, ts) };
        assert!(r.as_i32() == c);
    }
}

/// WAVE-6 EXTENSION: date/time/interval arithmetic (checked-op lattices +
/// the /USECS_PER_DAY divider family).
///
/// Rust side: the SHIPPED fmgr wrappers `adt_date::builtins::fc_*` through a
/// real `LocalFcinfo` frame. Interval/timetz RESULTS ride the shipped by-ref
/// path (`interval_result`/`timetz_result` → `byref_result` → Mcx allocate),
/// proven modulo the static-buffer allocator model (proof_support mcx-stubs;
/// the result frame is armed with an opaque dummy context, brin-minmax
/// precedent). C side: vendored REL_18_STABLE date.c/timestamp.c bodies
/// (this file's wave-6 C section) with results as out-params and
/// ereport(ERROR) as a returned verdict flag.
///
/// Fallible rows follow the cash precedent: value parity on Ok, verdict +
/// sqlstate + level parity on Err (sqlstate set by the SHIPPED with_sqlstate
/// call; PgError::error stubbed so message text/Location leave the proof),
/// `mem::forget` on the Err box (drop-glue trap), `kani::cover!` witnesses
/// on both arms.
///
/// Domains (per-harness, honest bounds):
/// - interval_um/pl/mi, date_pli/mii: FULL symbolic domains (checked-op
///   lattice, no dividers).
/// - time/timetz ± interval, justify_hours/days/interval, timestamp_mi:
///   contain the `/ USECS_PER_DAY` (86_400_000_000) constant divider on an
///   i64 dividend — full-domain probed first; per-band fallbacks recorded in
///   the ledger if the full domain walls (the divider is a single
///   expression, not a loop: magnitude bands cannot shrink the circuit, so
///   the honest fallbacks are contract-domain + spots or wall(divider)).
/// - timestamp ± interval: proven on the plane where the body is
///   checked-op only — span.month == 0 && span.day == 0 (LITERAL zeros, so
///   the month/day julian walk is constant-folded out of the formula) plus
///   the two infinity-sentinel planes (literal NOBEGIN/NOEND spans).
///   The month!=0/day!=0 finite planes run the j2date/date2j divider chain
///   (measured wall) and stay OUT of proof: the C side traps them loudly
///   (flag 99) and the Rust side stubs timestamp2tm to a panic — if either
///   side ever reached them the harness FAILS rather than passing vacuously.
///
/// Negative control: control_interval_pl_vs_c_mi (shipped interval_pl vs C
/// interval_mi) MUST fail — also witnesses the by-ref result read-back rig
/// is non-vacuous. DEFAULT solver for the control, kissat elsewhere.
#[cfg(kani)]
mod arith_proofs {
    use datum::Datum;
    use proof_support::fcinfo::{fci, FcFn};
    use proof_support::{mcx_stubs, stubs};
    use types_error::{ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERROR};

    use std::os::raw::c_int;

    extern "C" {
        fn pg_date_pli(date: i32, days: i32, result: *mut i32) -> c_int;
        fn pg_date_mii(date: i32, days: i32, result: *mut i32) -> c_int;
        fn pg_time_pl_interval(time: i64, st: i64, sd: i32, sm: i32, result: *mut i64) -> c_int;
        fn pg_time_mi_interval(time: i64, st: i64, sd: i32, sm: i32, result: *mut i64) -> c_int;
        fn pg_timetz_pl_interval(
            tt: i64,
            tz: i32,
            st: i64,
            sd: i32,
            sm: i32,
            rt: *mut i64,
            rz: *mut i32,
        ) -> c_int;
        fn pg_timetz_mi_interval(
            tt: i64,
            tz: i32,
            st: i64,
            sd: i32,
            sm: i32,
            rt: *mut i64,
            rz: *mut i32,
        ) -> c_int;
        fn pg_interval_um(
            t: i64,
            d: i32,
            m: i32,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_interval_pl(
            t1: i64,
            d1: i32,
            m1: i32,
            t2: i64,
            d2: i32,
            m2: i32,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_interval_mi(
            t1: i64,
            d1: i32,
            m1: i32,
            t2: i64,
            d2: i32,
            m2: i32,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_interval_justify_hours(
            t: i64,
            d: i32,
            m: i32,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_interval_justify_days(
            t: i64,
            d: i32,
            m: i32,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_interval_justify_interval(
            t: i64,
            d: i32,
            m: i32,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_timestamp_mi(
            dt1: i64,
            dt2: i64,
            rt: *mut i64,
            rd: *mut i32,
            rm: *mut i32,
        ) -> c_int;
        fn pg_timestamp_pl_interval(
            ts: i64,
            st: i64,
            sd: i32,
            sm: i32,
            result: *mut i64,
        ) -> c_int;
        fn pg_timestamp_mi_interval(
            ts: i64,
            st: i64,
            sd: i32,
            sm: i32,
            result: *mut i64,
        ) -> c_int;
    }

    /// interval rides by-ref: on-disk/fmgr image (time @0, day @8, month
    /// @12), exactly what arg_interval read_unaligned expects
    /// (interval-cmp precedent).
    fn iv_img(time: i64, day: i32, month: i32) -> [u8; 16] {
        let mut img = [0u8; 16];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..12].copy_from_slice(&day.to_ne_bytes());
        img[12..].copy_from_slice(&month.to_ne_bytes());
        img
    }

    fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
        let mut img = [0u8; 12];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..].copy_from_slice(&zone.to_ne_bytes());
        img
    }

    /// Opaque dummy context for the result frame (brin-minmax precedent):
    /// with Mcx::{allocate,grow,deallocate} stubbed to the proof heap, no
    /// code in the theorem reads the pointee.
    fn dummy_mcx() -> mcx::Mcx<'static> {
        const _: () = assert!(core::mem::size_of::<mcx::MemoryContext>() <= 1024);
        const _: () = assert!(core::mem::align_of::<mcx::MemoryContext>() <= 16);
        #[repr(align(16))]
        struct DummySlot([u8; 1024]);
        // SAFETY: the slot is never read or written through (see above).
        unsafe impl Sync for DummySlot {}
        static SLOT: DummySlot = DummySlot([0u8; 1024]);
        // SAFETY: never dereferenced — every Allocator entry point is
        // stubbed and nothing in these wrappers reads context state.
        let ctx: &'static mcx::MemoryContext =
            unsafe { &*(SLOT.0.as_ptr() as *const mcx::MemoryContext) };
        ctx.mcx()
    }

    /// Invoke a shipped fc_* wrapper with the result frame armed (by-ref
    /// interval/timetz results allocate 16/12 bytes via the stubbed Mcx).
    fn call_mcx<const N: usize, E>(fc: FcFn<E>, args: [Datum; N]) -> Result<Datum, E> {
        let mut f = fci(args);
        // SAFETY: the dummy context is 'static; outlives the call.
        unsafe { f.set_result_mcx(dummy_mcx()) };
        fc(None, &mut f)
    }

    /// Read an interval result image back from the returned by-ref datum.
    fn read_iv(d: Datum) -> (i64, i32, i32) {
        let p = d.as_usize() as *const u8;
        // SAFETY: the wrapper just wrote a 16-byte interval image there
        // (proof-heap allocation, live for the harness).
        unsafe {
            (
                (p as *const i64).read_unaligned(),
                (p.add(8) as *const i32).read_unaligned(),
                (p.add(12) as *const i32).read_unaligned(),
            )
        }
    }

    fn read_timetz(d: Datum) -> (i64, i32) {
        let p = d.as_usize() as *const u8;
        // SAFETY: 12-byte timetz image just written by the wrapper.
        unsafe {
            ((p as *const i64).read_unaligned(), (p.add(8) as *const i32).read_unaligned())
        }
    }

    /// Loud out-of-plane stub for adt_timestamp::timestamp2tm on the
    /// timestamp±interval harnesses: the month!=0/day!=0 julian walk is out
    /// of this proof's plane (j2date divider wall). The harnesses pin
    /// span.month/day to literal 0 or a sentinel, so this is statically
    /// unreachable; if constant-folding ever failed, the harness FAILS
    /// loudly instead of walling or passing vacuously (stub_datum_copy_byval
    /// precedent).
    fn stub_timestamp2tm_out_of_plane(
        _dt: i64,
        _tzp: Option<&mut i32>,
        _tm: &mut adt_datetime::pg_tm,
        _fsec: &mut adt_datetime::consts::fsec_t,
        _tzn: Option<&mut Option<&'static str>>,
        _attimezone: Option<&'static localtime::PgTz>,
    ) -> Result<(), ()> {
        panic!("wave-6 plane violation: month/day walk reached");
    }

    /// Adjudicate one dual execution where the Rust result is an INTERVAL
    /// image and the C result is (rt, rd, rm) + verdict flag.
    macro_rules! check_iv_result {
        ($r:expr, $cerr:expr, $rt:expr, $rd:expr, $rm:expr) => {
            match $r {
                Ok(d) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!($cerr == 0);
                    let (t, dd, m) = read_iv(d);
                    assert!(t == $rt && dd == $rd && m == $rm);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!($cerr == 1);
                    assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        };
    }

    /// Same for a scalar (i32/i64 datum) Rust result.
    macro_rules! check_scalar_result {
        ($r:expr, $cerr:expr, $cval:expr, $extract:ident, $want_flag:expr) => {
            match $r {
                Ok(d) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!($cerr == 0);
                    assert!(d.$extract() == $cval);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!($cerr == $want_flag);
                    assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        };
    }

    // ---------- interval_um / interval_pl / interval_mi: full symbolic ----------

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_interval_um() {
        let (t, d, m): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
        let cerr = unsafe { pg_interval_um(t, d, m, &mut rt, &mut rd, &mut rm) };
        let img = iv_img(t, d, m);
        let r = call_mcx(
            adt_date::builtins::fc_interval_um,
            [Datum::from_usize(img.as_ptr() as usize)],
        );
        check_iv_result!(r, cerr, rt, rd, rm);
    }

    macro_rules! iv2_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            fn $h() {
                let (t1, d1, m1): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
                let cerr = unsafe { $pg(t1, d1, m1, t2, d2, m2, &mut rt, &mut rd, &mut rm) };
                let i1 = iv_img(t1, d1, m1);
                let i2 = iv_img(t2, d2, m2);
                let r = call_mcx(
                    adt_date::builtins::$fc,
                    [
                        Datum::from_usize(i1.as_ptr() as usize),
                        Datum::from_usize(i2.as_ptr() as usize),
                    ],
                );
                check_iv_result!(r, cerr, rt, rd, rm);
            }
        )*};
    }

    iv2_op! {
        eq_interval_pl: fc_interval_pl / pg_interval_pl;
        eq_interval_mi: fc_interval_mi / pg_interval_mi;
    }

    // ---------- date_pli / date_mii: full symbolic i32 × i32 ----------

    macro_rules! date_pm_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (date, days): (i32, i32) = (kani::any(), kani::any());
                let mut cval: i32 = 0;
                let cerr = unsafe { $pg(date, days, &mut cval) };
                let r = proof_support::call2(
                    adt_date::builtins::$fc, date, days,
                );
                check_scalar_result!(r, cerr, cval, as_i32, 1);
            }
        )*};
    }

    date_pm_op! {
        eq_date_pli: fc_date_pli / pg_date_pli;
        eq_date_mii: fc_date_mii / pg_date_mii;
    }

    // ---------- time ± interval: full symbolic (single /USECS_PER_DAY) ----------

    macro_rules! time_iv_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let time: i64 = kani::any();
                let (st, sd, sm): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(time, st, sd, sm, &mut cval) };
                let img = iv_img(st, sd, sm);
                let r = proof_support::call2(
                    adt_date::builtins::$fc,
                    time,
                    Datum::from_usize(img.as_ptr() as usize),
                );
                check_scalar_result!(r, cerr, cval, as_i64, 1);
            }
        )*};
    }

    time_iv_op! {
        eq_time_pl_interval: fc_time_pl_interval / pg_time_pl_interval;
        eq_time_mi_interval: fc_time_mi_interval / pg_time_mi_interval;
    }

    // ---------- timetz ± interval ----------

    macro_rules! timetz_iv_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            fn $h() {
                let (tt, tz): (i64, i32) = (kani::any(), kani::any());
                let (st, sd, sm): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let (mut rt, mut rz): (i64, i32) = (0, 0);
                let cerr = unsafe { $pg(tt, tz, st, sd, sm, &mut rt, &mut rz) };
                let targ = timetz_img(tt, tz);
                let sarg = iv_img(st, sd, sm);
                let r = call_mcx(
                    adt_date::builtins::$fc,
                    [
                        Datum::from_usize(targ.as_ptr() as usize),
                        Datum::from_usize(sarg.as_ptr() as usize),
                    ],
                );
                match r {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(cerr == 0);
                        let (t, z) = read_timetz(d);
                        assert!(t == rt && z == rz);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(cerr == 1);
                        assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    timetz_iv_op! {
        eq_timetz_pl_interval: fc_timetz_pl_interval / pg_timetz_pl_interval;
        eq_timetz_mi_interval: fc_timetz_mi_interval / pg_timetz_mi_interval;
    }

    // ---------- justify family: full symbolic input interval ----------

    macro_rules! justify_op {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            fn $h() {
                let (t, d, m): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
                let cerr = unsafe { $pg(t, d, m, &mut rt, &mut rd, &mut rm) };
                let img = iv_img(t, d, m);
                let r = call_mcx(
                    adt_date::builtins::$fc,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                check_iv_result!(r, cerr, rt, rd, rm);
            }
        )*};
    }

    justify_op! {
        eq_interval_justify_hours: fc_interval_justify_hours / pg_interval_justify_hours;
        eq_interval_justify_days: fc_interval_justify_days / pg_interval_justify_days;
        eq_interval_justify_interval: fc_interval_justify_interval / pg_interval_justify_interval;
    }

    // ---------- timestamp_mi: full symbolic i64 × i64 ----------

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_timestamp_mi() {
        let (dt1, dt2): (i64, i64) = (kani::any(), kani::any());
        let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
        let cerr = unsafe { pg_timestamp_mi(dt1, dt2, &mut rt, &mut rd, &mut rm) };
        let r = call_mcx(
            adt_date::builtins::fc_timestamp_mi,
            [Datum::from_i64(dt1), Datum::from_i64(dt2)],
        );
        check_iv_result!(r, cerr, rt, rd, rm);
    }

    // ---------- timestamp ± interval: checked-op planes ----------
    // Plane 1 (m0d0): span.month == 0 && span.day == 0 as LITERALS — the
    // julian month/day walk constant-folds away on both sides; span.time and
    // the timestamp fully symbolic (covers the whole time-only-interval
    // domain incl. both timestamp sentinels + the overflow/validity Err arm).
    // Planes 2/3: span == literal NOBEGIN / NOEND sentinel (infinity
    // lattice), timestamp fully symbolic.

    macro_rules! ts_iv_planes {
        ($($m0:ident, $nb:ident, $ne:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, stub_timestamp2tm_out_of_plane)]
            fn $m0() {
                let ts: i64 = kani::any();
                let st: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, st, 0, 0, &mut cval) };
                assert!(cerr != 99); // C-side plane trap must be dead
                let img = iv_img(st, 0, 0);
                let r = proof_support::call2(
                    adt_date::builtins::$fc,
                    ts,
                    Datum::from_usize(img.as_ptr() as usize),
                );
                check_scalar_result!(r, cerr, cval, as_i64, 1);
            }

            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, stub_timestamp2tm_out_of_plane)]
            fn $nb() {
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, i64::MIN, i32::MIN, i32::MIN, &mut cval) };
                assert!(cerr != 99);
                let img = iv_img(i64::MIN, i32::MIN, i32::MIN);
                let r = proof_support::call2(
                    adt_date::builtins::$fc,
                    ts,
                    Datum::from_usize(img.as_ptr() as usize),
                );
                check_scalar_result!(r, cerr, cval, as_i64, 1);
            }

            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, stub_timestamp2tm_out_of_plane)]
            fn $ne() {
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, i64::MAX, i32::MAX, i32::MAX, &mut cval) };
                assert!(cerr != 99);
                let img = iv_img(i64::MAX, i32::MAX, i32::MAX);
                let r = proof_support::call2(
                    adt_date::builtins::$fc,
                    ts,
                    Datum::from_usize(img.as_ptr() as usize),
                );
                check_scalar_result!(r, cerr, cval, as_i64, 1);
            }
        )*};
    }

    ts_iv_planes! {
        eq_timestamp_pl_interval_m0d0, eq_timestamp_pl_interval_nobegin,
            eq_timestamp_pl_interval_noend:
            fc_timestamp_pl_interval / pg_timestamp_pl_interval;
        eq_timestamp_mi_interval_m0d0, eq_timestamp_mi_interval_nobegin,
            eq_timestamp_mi_interval_noend:
            fc_timestamp_mi_interval / pg_timestamp_mi_interval;
    }

    // ---------- divider-row SPOT proofs ----------
    // The full symbolic domains of the / USECS_PER_DAY rows are a measured
    // SAT wall (dual 64-bit constant-division equivalence; symex completes,
    // the assertion batch exceeds 560s on both solvers). These spot
    // harnesses run the SAME dual execution on concrete boundary grids —
    // every division constant-folds at symex, so they stand as cheap
    // per-commit gates. The symbolic remainder is covered census-grade by
    // src/bin/native_diff_dividers.rs (tested(differential)).
    // Grid intent per row: nominal case, negative-remainder fixup, wrap
    // (i64 overflow of the initial +/-), Err arm (infinite span / overflow),
    // sentinel passthrough.

    const UPD: i64 = 86_400_000_000;

    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn spot_time_pl_mi_interval() {
        const CASES: [(i64, i64); 6] = [
            (0, 0),
            (1, -2),
            (UPD - 1, 1),
            (i64::MAX, 1),
            (i64::MIN, -1),
            (43_200_000_000, 7_777_777_777),
        ];
        for (time, st) in CASES {
            for pl in [true, false] {
                let mut cval: i64 = 0;
                let cerr = unsafe {
                    if pl {
                        pg_time_pl_interval(time, st, 0, 0, &mut cval)
                    } else {
                        pg_time_mi_interval(time, st, 0, 0, &mut cval)
                    }
                };
                let img = iv_img(st, 0, 0);
                let fc = if pl {
                    adt_date::builtins::fc_time_pl_interval
                } else {
                    adt_date::builtins::fc_time_mi_interval
                };
                let r = proof_support::call2(fc, time, Datum::from_usize(img.as_ptr() as usize));
                check_scalar_result!(r, cerr, cval, as_i64, 1);
            }
        }
        // Err arm: infinite span
        let img = iv_img(i64::MIN, i32::MIN, i32::MIN);
        let r = proof_support::call2(
            adt_date::builtins::fc_time_pl_interval,
            0i64,
            Datum::from_usize(img.as_ptr() as usize),
        );
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_time_pl_interval(0, i64::MIN, i32::MIN, i32::MIN, &mut cval) };
        assert!(cerr == 1 && r.is_err());
        if let Err(e) = r {
            core::mem::forget(e);
        }
    }

    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn spot_timetz_pl_mi_interval() {
        const CASES: [(i64, i32, i64); 5] = [
            (0, 3600, 0),
            (1, -3600, -2),
            (UPD - 1, 0, 1),
            (i64::MAX, 57_599, 1),
            (i64::MIN, -57_599, -1),
        ];
        for (tt, tz, st) in CASES {
            for pl in [true, false] {
                let (mut rt, mut rz): (i64, i32) = (0, 0);
                let cerr = unsafe {
                    if pl {
                        pg_timetz_pl_interval(tt, tz, st, 0, 0, &mut rt, &mut rz)
                    } else {
                        pg_timetz_mi_interval(tt, tz, st, 0, 0, &mut rt, &mut rz)
                    }
                };
                let targ = timetz_img(tt, tz);
                let sarg = iv_img(st, 0, 0);
                let fc = if pl {
                    adt_date::builtins::fc_timetz_pl_interval
                } else {
                    adt_date::builtins::fc_timetz_mi_interval
                };
                let r = call_mcx(
                    fc,
                    [
                        Datum::from_usize(targ.as_ptr() as usize),
                        Datum::from_usize(sarg.as_ptr() as usize),
                    ],
                );
                match r {
                    Ok(d) => {
                        assert!(cerr == 0);
                        let (t, z) = read_timetz(d);
                        assert!(t == rt && z == rz);
                    }
                    Err(e) => {
                        assert!(cerr == 1);
                        core::mem::forget(e);
                    }
                }
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(11)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn spot_justify_hours_interval() {
        const CASES: [(i64, i32, i32); 8] = [
            (0, 0, 0),
            (UPD + 1, 0, 0),
            (-UPD - 1, 1, 0),           // negative-time fixup
            (UPD, i32::MAX, 0),          // day-add overflow -> Err
            (i64::MAX, -5, 3),
            (i64::MAX, 62, 1),           // pre-justify path (justify_interval)
            (i64::MIN, i32::MIN, i32::MIN), // NOBEGIN passthrough
            (i64::MAX, i32::MAX, i32::MAX), // NOEND passthrough
        ];
        for (t, d, m) in CASES {
            for hours in [true, false] {
                let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
                let cerr = unsafe {
                    if hours {
                        pg_interval_justify_hours(t, d, m, &mut rt, &mut rd, &mut rm)
                    } else {
                        pg_interval_justify_interval(t, d, m, &mut rt, &mut rd, &mut rm)
                    }
                };
                let img = iv_img(t, d, m);
                let fc = if hours {
                    adt_date::builtins::fc_interval_justify_hours
                } else {
                    adt_date::builtins::fc_interval_justify_interval
                };
                let r = call_mcx(fc, [Datum::from_usize(img.as_ptr() as usize)]);
                check_iv_result!(r, cerr, rt, rd, rm);
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn spot_timestamp_mi() {
        const CASES: [(i64, i64); 9] = [
            (0, 0),
            (1, -1),
            (UPD + 1, 0),                    // justify carry
            (-UPD - 1, 0),                   // negative fixup
            (i64::MIN, 5),                   // NOBEGIN - finite
            (5, i64::MIN),                   // finite - NOBEGIN
            (i64::MIN, i64::MIN),            // inf - inf -> Err
            (i64::MAX - 1, i64::MIN + 2),    // sub overflow -> Err
            (i64::MAX, 7),                   // NOEND - finite
        ];
        for (dt1, dt2) in CASES {
            let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
            let cerr = unsafe { pg_timestamp_mi(dt1, dt2, &mut rt, &mut rd, &mut rm) };
            let r = call_mcx(
                adt_date::builtins::fc_timestamp_mi,
                [Datum::from_i64(dt1), Datum::from_i64(dt2)],
            );
            check_iv_result!(r, cerr, rt, rd, rm);
        }
    }

    // ---------- negative control ----------

    /// Deliberate mismatch: shipped fc_interval_pl vs C interval_mi — MUST
    /// fail, witnessing both the harness rig and the by-ref result
    /// read-back are non-vacuous. DEFAULT solver.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn control_interval_pl_vs_c_mi() {
        let (t1, d1, m1): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let (t2, d2, m2): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let (mut rt, mut rd, mut rm): (i64, i32, i32) = (0, 0, 0);
        let cerr = unsafe { pg_interval_mi(t1, d1, m1, t2, d2, m2, &mut rt, &mut rd, &mut rm) };
        let i1 = iv_img(t1, d1, m1);
        let i2 = iv_img(t2, d2, m2);
        let r = call_mcx(
            adt_date::builtins::fc_interval_pl,
            [Datum::from_usize(i1.as_ptr() as usize), Datum::from_usize(i2.as_ptr() as usize)],
        );
        match r {
            Ok(d) => {
                assert!(cerr == 0);
                let (t, dd, m) = read_iv(d);
                assert!(t == rt && dd == rd && m == rm);
            }
            Err(e) => {
                assert!(cerr == 1);
                core::mem::forget(e);
            }
        }
    }
}

/// WAVE-7 EXTENSION: the timestamp/timestamptz remainder rows —
/// finite/overlaps/hash/send, timestamptz ± interval (checked-op planes,
/// incl. the at-zone wrappers), timestamp<->timestamptz conversions
/// (tz-seam model), zone/izone, typmod scale, and the in_range family.
///
/// Rust side: the SHIPPED fmgr wrappers `adt_timestamp::builtins::fc_*`
/// through real `LocalFcinfo` frames (overlaps drives the real null-flag
/// protocol). C side: vendored REL_18_STABLE bodies (wave-7 section of
/// c/pg_datetime_cmp.c; provenance + every shim documented there).
///
/// Planes and seams (per-harness, honest bounds):
/// - finite/hash/overlaps: FULL symbolic domains, no seams.
/// - send: full symbolic i64 through the shipped pqformat path, modulo the
///   static-buffer allocator model (int-arith send recipe).
/// - timestamptz ± interval (incl at-zone): the wave-6 three-plane scheme —
///   span.month==0 && span.day==0 as LITERALS, plus the NOBEGIN/NOEND
///   sentinel planes; the julian month/day arms are loud-trapped on both
///   sides (C flag 99 / Rust panicking timestamp2tm stub). The
///   session-timezone / lookup_timezone resolution is consumed only by the
///   trapped arms; the at-zone harnesses stub lookup_timezone to a dummy
///   zone (name-decode seam out of proof, value provably unused on-plane).
/// - timestamp_timestamptz (+ timestamp_at_local alias): proved modulo the
///   SHARED tz-seam model (dt-minmax pattern): both sides read one symbolic
///   offset universally quantified over (-86400, 86400) ⊇ any real tz;
///   timestamp2tm feeds only the stubbed DetermineTimeZoneOffset. Value
///   parity on Ok, verdict+sqlstate 22008 parity on Err.
/// - timestamptz_timestamp (+ timestamptz_at_local alias): decompose seam —
///   timestamp2tm modeled as literal tm (2000-01-01 00:00:00) + one shared
///   SYMBOLIC fsec (full i32 ⊇ the real fsec range) + dead tz out; the
///   RECOMPOSE (shipped tm2timestamp incl. date2j) stays in the theorem on
///   both sides. Non-finite passthrough full; decompose-failure arm out of
///   proof (unreachable under the model).
/// - zone rows: zone-name-decode seam — DecodeTimezoneName stubbed to
///   TzLookup::FixedOffset(val) with val universally quantified over
///   (-86400, 86400); timestamp fully symbolic; DynTz/full-zone arms are
///   out-of-plane (Rust panicking stubs / C structural pin). A skew control
///   (different val per side) must FAIL.
/// - izone rows: value plane = ONE-SYMBOLIC-INDEX grid over literal
///   zone.time cells (the /USECS_PER_SEC divider constant-folds per cell;
///   full-symbolic zone.time is the loop-free band-immune divider class —
///   spots + native differential per TRIAGE) x fully symbolic timestamp;
///   error planes = fully symbolic non-finite / months-days zones,
///   verdict+sqlstate 22023 parity (message text incl. interval_out image
///   stubbed out of proof).
/// - scale rows: passthrough/error planes fully symbolic (non-finite time,
///   typmod -1/6, out-of-range typmod incl. 22023 parity); the finite
///   rounding arm's /10^k divider is the band-immune class -> ONE-SYMBOLIC-
///   INDEX literal (typmod, time) grid + native differential.
/// - in_range rows: offset month==day==0 LITERAL plane (+ the literal-NOEND
///   offset plane for the infinity shortcut); interval version additionally
///   pins val/base month/day to 0 (time-only plane) so interval_cmp_value's
///   two-contributor i128 multiply stays out of the formula.
#[cfg(kani)]
mod wave7_proofs {
    use datum::{Datum, NullableDatum};
    use proof_support::fcinfo::{fci, FcFn};
    use proof_support::{mcx_stubs, stubs};
    use types_error::{
        ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INVALID_PARAMETER_VALUE,
        ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERROR,
    };
    use types_fmgr::LocalFcinfo;

    use std::os::raw::c_int;

    extern "C" {
        fn pg_timestamp_finite(ts: i64) -> c_int;
        fn pg_timestamp_hash(val: i64) -> u32;
        fn pg_timestamp_hash_extended(val: i64, seed: u64) -> u64;
        fn pg_timestamp_send(ts: i64, out: *mut u8) -> i32;
        fn pg_overlaps_timestamp(
            ts1: i64,
            n1: c_int,
            te1: i64,
            n2: c_int,
            ts2: i64,
            n3: c_int,
            te2: i64,
            n4: c_int,
            result: *mut c_int,
        ) -> c_int;
        fn pg_timestamptz_pl_interval(ts: i64, st: i64, sd: i32, sm: i32, r: *mut i64) -> c_int;
        fn pg_timestamptz_mi_interval(ts: i64, st: i64, sd: i32, sm: i32, r: *mut i64) -> c_int;
        fn pg_timestamp_timestamptz(ts: i64, r: *mut i64) -> c_int;
        fn pg_timestamptz_timestamp(ts: i64, r: *mut i64) -> c_int;
        fn pg_timestamp_zone(ts: i64, r: *mut i64) -> c_int;
        fn pg_timestamptz_zone(ts: i64, r: *mut i64) -> c_int;
        fn pg_timestamp_izone(zt: i64, zd: i32, zm: i32, ts: i64, r: *mut i64) -> c_int;
        fn pg_timestamptz_izone(zt: i64, zd: i32, zm: i32, ts: i64, r: *mut i64) -> c_int;
        fn pg_timestamp_scale(ts: i64, typmod: i32, r: *mut i64) -> c_int;
        fn pg_in_range_timestamp_interval(
            val: i64,
            base: i64,
            ot: i64,
            od: i32,
            om: i32,
            sub: c_int,
            less: c_int,
            result: *mut c_int,
        ) -> c_int;
        fn pg_in_range_timestamptz_interval(
            val: i64,
            base: i64,
            ot: i64,
            od: i32,
            om: i32,
            sub: c_int,
            less: c_int,
            result: *mut c_int,
        ) -> c_int;
        fn pg_in_range_interval_interval(
            vt: i64,
            vd: i32,
            vm: i32,
            bt: i64,
            bd: i32,
            bm: i32,
            ot: i64,
            od: i32,
            om: i32,
            sub: c_int,
            less: c_int,
            result: *mut c_int,
        ) -> c_int;

        static mut pg_model_tz_offset: i32;
        static mut pg_model_fsec: i32;
        static mut pg_model_tzname_val: i32;
    }

    const SECS_PER_DAY_I32: i32 = 86_400;

    // ---------- shared model statics / stubs ----------

    /// dummy fixed zone so shipped session_timezone()/lookup_timezone
    /// consumers succeed; never read (every consumer of its state is
    /// stubbed or out-of-plane).
    static W7_TZ: localtime::PgTz = localtime::PgTz {
        tzname: [0; localtime::TZ_STRLEN_MAX + 1],
        state: localtime::TzState::new(),
    };

    static W7_TZ_OFF: core::sync::atomic::AtomicI32 = core::sync::atomic::AtomicI32::new(0);
    static W7_FSEC: core::sync::atomic::AtomicI32 = core::sync::atomic::AtomicI32::new(0);
    static W7_ZVAL: core::sync::atomic::AtomicI32 = core::sync::atomic::AtomicI32::new(0);

    fn w7_model_tz_offset(_tm: &mut adt_datetime::pg_tm, _tzp: &localtime::PgTz) -> i32 {
        W7_TZ_OFF.load(core::sync::atomic::Ordering::Relaxed)
    }

    /// timestamp2tm model for the timestamp->timestamptz direction: fixed tm
    /// feeding ONLY the (stubbed) DetermineTimeZoneOffset.
    fn w7_model_timestamp2tm(
        _dt: i64,
        _tzp: Option<&mut i32>,
        tm: &mut adt_datetime::pg_tm,
        _fsec: &mut adt_datetime::consts::fsec_t,
        _tzn: Option<&mut Option<&'static str>>,
        _attimezone: Option<&'static localtime::PgTz>,
    ) -> Result<(), ()> {
        tm.tm_year = 2000;
        tm.tm_mon = 1;
        tm.tm_mday = 1;
        Ok(())
    }

    /// timestamp2tm DECOMPOSE model for the timestamptz->timestamp
    /// direction: literal tm (2000-01-01 00:00:00) + the shared symbolic
    /// fsec; tz out-param set to 0 (dead: tm2timestamp gets tzp == None).
    fn w7_model_decompose(
        _dt: i64,
        tzp: Option<&mut i32>,
        tm: &mut adt_datetime::pg_tm,
        fsec: &mut adt_datetime::consts::fsec_t,
        tzn: Option<&mut Option<&'static str>>,
        _attimezone: Option<&'static localtime::PgTz>,
    ) -> Result<(), ()> {
        tm.tm_year = 2000;
        tm.tm_mon = 1;
        tm.tm_mday = 1;
        tm.tm_hour = 0;
        tm.tm_min = 0;
        tm.tm_sec = 0;
        *fsec = W7_FSEC.load(core::sync::atomic::Ordering::Relaxed);
        if let Some(t) = tzp {
            *t = 0;
        }
        if let Some(slot) = tzn {
            *slot = None;
        }
        Ok(())
    }

    /// Loud out-of-plane stub (wave-6 pattern): the julian month/day walk
    /// and the full-zone/DynTz arms are outside every wave-7 plane.
    fn w7_timestamp2tm_out_of_plane(
        _dt: i64,
        _tzp: Option<&mut i32>,
        _tm: &mut adt_datetime::pg_tm,
        _fsec: &mut adt_datetime::consts::fsec_t,
        _tzn: Option<&mut Option<&'static str>>,
        _attimezone: Option<&'static localtime::PgTz>,
    ) -> Result<(), ()> {
        panic!("wave-7 plane violation: timestamp2tm reached");
    }

    fn w7_dtzo_out_of_plane(_tm: &mut adt_datetime::pg_tm, _tzp: &localtime::PgTz) -> i32 {
        panic!("wave-7 plane violation: DetermineTimeZoneOffset reached");
    }

    /// lookup_timezone seam for the at-zone wrappers: the name-decode is out
    /// of proof; the returned zone is consumed only by out-of-plane arms.
    fn w7_stub_lookup_timezone(
        _zone: &[u8],
    ) -> Result<&'static localtime::PgTz, Box<types_error::PgError>> {
        Ok(&W7_TZ)
    }

    /// DecodeTimezoneName seam for the zone rows: fixed-offset arm with the
    /// shared symbolic offset value.
    fn w7_stub_decode_tzname(
        _tzname: &[u8],
    ) -> Result<adt_timestamp::TzLookup, Box<types_error::PgError>> {
        Ok(adt_timestamp::TzLookup::FixedOffset(
            W7_ZVAL.load(core::sync::atomic::Ordering::Relaxed),
        ))
    }

    /// interval_out is message-text machinery on the izone error arms; its
    /// image (and the failing zone's rendering) leaves the proof.
    fn w7_stub_interval_out(
        _span: &adt_datetime::consts::Interval,
        _buf: &mut adt_timestamp::TsBuf,
    ) -> usize {
        0
    }

    fn iv_img(time: i64, day: i32, month: i32) -> [u8; 16] {
        let mut img = [0u8; 16];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..12].copy_from_slice(&day.to_ne_bytes());
        img[12..].copy_from_slice(&month.to_ne_bytes());
        img
    }

    /// One-byte-header (short) inline text varlena for the zone-name
    /// argument, built with the shipped header encoder (text-cmp pattern).
    fn zone_text_img() -> [u8; 2] {
        let mut img = [0u8; 2];
        img[1] = b'x';
        // SAFETY: img is 2 bytes, len 1 + short header.
        unsafe {
            types_tuple::varatt::set_varsize_short(
                img.as_mut_ptr(),
                1 + types_tuple::varatt::VARHDRSZ_SHORT,
            )
        };
        img
    }

    fn dummy_mcx() -> mcx::Mcx<'static> {
        const _: () = assert!(core::mem::size_of::<mcx::MemoryContext>() <= 1024);
        #[repr(align(16))]
        struct DummySlot([u8; 1024]);
        // SAFETY: never read through (all allocator entry points stubbed).
        unsafe impl Sync for DummySlot {}
        static SLOT: DummySlot = DummySlot([0u8; 1024]);
        // SAFETY: never dereferenced.
        let ctx: &'static mcx::MemoryContext =
            unsafe { &*(SLOT.0.as_ptr() as *const mcx::MemoryContext) };
        ctx.mcx()
    }

    fn call_mcx<const N: usize, E>(fc: FcFn<E>, args: [Datum; N]) -> Result<Datum, E> {
        let mut f = fci(args);
        // SAFETY: 'static dummy context outlives the call.
        unsafe { f.set_result_mcx(dummy_mcx()) };
        fc(None, &mut f)
    }

    /// Scalar-result adjudication: value parity on Ok; verdict + sqlstate +
    /// level parity on Err (both arms cover-witnessed by callers that can
    /// reach both).
    macro_rules! check_i64_result {
        ($r:expr, $cerr:expr, $cval:expr, $sqlstate_of_flag:expr) => {
            match $r {
                Ok(d) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!($cerr == 0);
                    assert!(d.as_i64() == $cval);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!($cerr != 0 && $cerr != 99);
                    assert!(e.sqlstate == $sqlstate_of_flag($cerr));
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        };
    }

    fn sql_22008(_flag: c_int) -> types_error::SqlState {
        ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
    }

    fn sql_izone(flag: c_int) -> types_error::SqlState {
        if flag == 2 {
            ERRCODE_INVALID_PARAMETER_VALUE
        } else {
            ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
        }
    }

    // ---------- timestamp_finite (1389, 2048): full i64 ----------

    #[kani::proof]
    fn eq_timestamp_finite() {
        let ts: i64 = kani::any();
        let r = proof_support::call1_ok(adt_timestamp::builtins::fc_timestamp_finite, ts);
        let c = unsafe { pg_timestamp_finite(ts) };
        assert!(r.as_bool() as c_int == c);
    }

    // ---------- timestamp[tz]_hash[_extended] (2039/6425, 3411/6426) ----------

    #[kani::proof]
    fn eq_timestamp_hash() {
        let ts: i64 = kani::any();
        let r = proof_support::call1_ok(adt_timestamp::builtins::fc_timestamp_hash, ts);
        let c = unsafe { pg_timestamp_hash(ts) };
        assert!(r.as_i32() == c as i32);
    }

    #[kani::proof]
    fn eq_timestamp_hash_extended() {
        let ts: i64 = kani::any();
        let seed: i64 = kani::any();
        let r = proof_support::call2_ok(
            adt_timestamp::builtins::fc_timestamp_hash_extended,
            ts,
            seed,
        );
        let c = unsafe { pg_timestamp_hash_extended(ts, seed as u64) };
        assert!(r.as_i64() == c as i64);
    }

    // ---------- timestamp[tz]_send (2475, 2477): full i64 ----------
    // int-arith send recipe: shipped pqformat path over a real bump context
    // with the proof-heap allocator model.

    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_timestamp_send() {
        let ts: i64 = kani::any();
        let mut cbuf = [0u8; 12];
        let clen = unsafe { pg_timestamp_send(ts, cbuf.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-ts-send");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_i64(ts));
        let d = match adt_timestamp::builtins::fc_timestamp_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("send errored")
            }
        };
        // SAFETY: varlena_result leaks the image; datum points at its start.
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 12) };
        assert!(clen == 12);
        let mut i = 0;
        while i < 12 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    // ---------- overlaps_timestamp (1304, 2041): full 4xi64 + null cube ----

    #[kani::proof]
    fn eq_overlaps_timestamp() {
        let (ts1, te1, ts2, te2): (i64, i64, i64, i64) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let (n1, n2, n3, n4): (bool, bool, bool, bool) =
            (kani::any(), kani::any(), kani::any(), kani::any());

        let mut cres: c_int = -1;
        let cnull = unsafe {
            pg_overlaps_timestamp(
                ts1, n1 as c_int, te1, n2 as c_int, ts2, n3 as c_int, te2, n4 as c_int, &mut cres,
            )
        };

        let mut f = LocalFcinfo::<4>::new(0);
        f.args[0] = NullableDatum { value: Datum::from_i64(ts1), isnull: n1 };
        f.args[1] = NullableDatum { value: Datum::from_i64(te1), isnull: n2 };
        f.args[2] = NullableDatum { value: Datum::from_i64(ts2), isnull: n3 };
        f.args[3] = NullableDatum { value: Datum::from_i64(te2), isnull: n4 };
        let d = match adt_timestamp::builtins::fc_overlaps_timestamp(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("overlaps errored"),
        };
        kani::cover!(f.isnull, "NULL result reachable");
        kani::cover!(!f.isnull, "non-NULL result reachable");
        assert!(f.isnull as c_int == cnull);
        if cnull == 0 {
            assert!(d.as_bool() as c_int == cres);
        }
    }

    // ---------- timestamptz ± interval planes (1189/6221, 1190/6223) -------

    macro_rules! tstz_iv_planes {
        ($($m0:ident, $nb:ident, $ne:ident: $fc:ident / $pg:ident, $sess:literal;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
            #[kani::stub(adt_timestamp::lookup_timezone, w7_stub_lookup_timezone)]
            fn $m0() {
                if $sess {
                    pgtz::set_session_timezone(Some(&W7_TZ));
                }
                let ts: i64 = kani::any();
                let st: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, st, 0, 0, &mut cval) };
                assert!(cerr != 99); // C-side plane trap must be dead
                let img = iv_img(st, 0, 0);
                let zimg = zone_text_img();
                let r = if $sess {
                    proof_support::call2(
                        adt_timestamp::builtins::$fc,
                        ts,
                        Datum::from_usize(img.as_ptr() as usize),
                    )
                } else {
                    proof_support::fcinfo::call(
                        adt_timestamp::builtins::$fc,
                        [
                            Datum::from_i64(ts),
                            Datum::from_usize(img.as_ptr() as usize),
                            Datum::from_usize(zimg.as_ptr() as usize),
                        ],
                    )
                };
                check_i64_result!(r, cerr, cval, sql_22008);
            }

            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
            #[kani::stub(adt_timestamp::lookup_timezone, w7_stub_lookup_timezone)]
            fn $nb() {
                if $sess {
                    pgtz::set_session_timezone(Some(&W7_TZ));
                }
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, i64::MIN, i32::MIN, i32::MIN, &mut cval) };
                assert!(cerr != 99);
                let img = iv_img(i64::MIN, i32::MIN, i32::MIN);
                let zimg = zone_text_img();
                let r = if $sess {
                    proof_support::call2(
                        adt_timestamp::builtins::$fc,
                        ts,
                        Datum::from_usize(img.as_ptr() as usize),
                    )
                } else {
                    proof_support::fcinfo::call(
                        adt_timestamp::builtins::$fc,
                        [
                            Datum::from_i64(ts),
                            Datum::from_usize(img.as_ptr() as usize),
                            Datum::from_usize(zimg.as_ptr() as usize),
                        ],
                    )
                };
                check_i64_result!(r, cerr, cval, sql_22008);
            }

            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
            #[kani::stub(adt_timestamp::lookup_timezone, w7_stub_lookup_timezone)]
            fn $ne() {
                if $sess {
                    pgtz::set_session_timezone(Some(&W7_TZ));
                }
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, i64::MAX, i32::MAX, i32::MAX, &mut cval) };
                assert!(cerr != 99);
                let img = iv_img(i64::MAX, i32::MAX, i32::MAX);
                let zimg = zone_text_img();
                let r = if $sess {
                    proof_support::call2(
                        adt_timestamp::builtins::$fc,
                        ts,
                        Datum::from_usize(img.as_ptr() as usize),
                    )
                } else {
                    proof_support::fcinfo::call(
                        adt_timestamp::builtins::$fc,
                        [
                            Datum::from_i64(ts),
                            Datum::from_usize(img.as_ptr() as usize),
                            Datum::from_usize(zimg.as_ptr() as usize),
                        ],
                    )
                };
                check_i64_result!(r, cerr, cval, sql_22008);
            }
        )*};
    }

    tstz_iv_planes! {
        eq_tstz_pl_interval_m0d0, eq_tstz_pl_interval_nobegin, eq_tstz_pl_interval_noend:
            fc_timestamptz_pl_interval / pg_timestamptz_pl_interval, true;
        eq_tstz_mi_interval_m0d0, eq_tstz_mi_interval_nobegin, eq_tstz_mi_interval_noend:
            fc_timestamptz_mi_interval / pg_timestamptz_mi_interval, true;
        eq_tstz_pl_interval_at_zone_m0d0, eq_tstz_pl_interval_at_zone_nobegin,
            eq_tstz_pl_interval_at_zone_noend:
            fc_timestamptz_pl_interval_at_zone / pg_timestamptz_pl_interval, false;
        eq_tstz_mi_interval_at_zone_m0d0, eq_tstz_mi_interval_at_zone_nobegin,
            eq_tstz_mi_interval_at_zone_noend:
            fc_timestamptz_mi_interval_at_zone / pg_timestamptz_mi_interval, false;
    }

    // ---------- timestamp <-> timestamptz (2027/6334, 2028/6335) ----------

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(adt_timestamp::timestamp2tm, w7_model_timestamp2tm)]
    #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_model_tz_offset)]
    fn eq_timestamp_timestamptz() {
        let off: i32 = kani::any();
        kani::assume(off > -SECS_PER_DAY_I32 && off < SECS_PER_DAY_I32);
        W7_TZ_OFF.store(off, core::sync::atomic::Ordering::Relaxed);
        unsafe { pg_model_tz_offset = off };
        pgtz::set_session_timezone(Some(&W7_TZ));
        let ts: i64 = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_timestamp_timestamptz(ts, &mut cval) };
        let r = proof_support::call1(adt_timestamp::builtins::fc_timestamp_timestamptz, ts);
        check_i64_result!(r, cerr, cval, sql_22008);
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(adt_timestamp::timestamp2tm, w7_model_decompose)]
    fn eq_timestamptz_timestamp() {
        let fsec: i32 = kani::any();
        W7_FSEC.store(fsec, core::sync::atomic::Ordering::Relaxed);
        unsafe { pg_model_fsec = fsec };
        let ts: i64 = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_timestamptz_timestamp(ts, &mut cval) };
        let r = proof_support::call1(adt_timestamp::builtins::fc_timestamptz_timestamp, ts);
        check_i64_result!(r, cerr, cval, sql_22008);
    }

    // ---------- zone rows (2069, 1159): fixed-offset seam plane ----------

    macro_rules! zone_fixed {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::DecodeTimezoneName, w7_stub_decode_tzname)]
            #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
            fn $h() {
                let val: i32 = kani::any();
                kani::assume(val > -SECS_PER_DAY_I32 && val < SECS_PER_DAY_I32);
                W7_ZVAL.store(val, core::sync::atomic::Ordering::Relaxed);
                unsafe { pg_model_tzname_val = val };
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(ts, &mut cval) };
                assert!(cerr != 99);
                let zimg = zone_text_img();
                let r = proof_support::fcinfo::call(
                    adt_timestamp::builtins::$fc,
                    [Datum::from_usize(zimg.as_ptr() as usize), Datum::from_i64(ts)],
                );
                check_i64_result!(r, cerr, cval, sql_22008);
            }
        )*};
    }

    zone_fixed! {
        eq_timestamp_zone_fixed: fc_timestamp_zone / pg_timestamp_zone;
        eq_timestamptz_zone_fixed: fc_timestamptz_zone / pg_timestamptz_zone;
    }

    /// Skew control for the zone-name-decode seam: C and Rust get DIFFERENT
    /// fixed offsets — MUST FAIL (DEFAULT solver).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(adt_timestamp::DecodeTimezoneName, w7_stub_decode_tzname)]
    #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
    #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
    fn control_zone_seam_skew() {
        W7_ZVAL.store(3600, core::sync::atomic::Ordering::Relaxed);
        unsafe { pg_model_tzname_val = 7200 };
        let ts: i64 = kani::any();
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_timestamp_zone(ts, &mut cval) };
        assert!(cerr != 99);
        let zimg = zone_text_img();
        let r = proof_support::fcinfo::call(
            adt_timestamp::builtins::fc_timestamp_zone,
            [Datum::from_usize(zimg.as_ptr() as usize), Datum::from_i64(ts)],
        );
        check_i64_result!(r, cerr, cval, sql_22008);
    }

    // ---------- izone rows (2070, 1026) ----------

    /// Value plane: literal zone.time cells (one symbolic index; the
    /// /USECS_PER_SEC divider constant-folds per cell) x full symbolic ts.
    const IZONE_ZT: [i64; 10] = [
        0,
        1,
        -1,
        999_999,
        -999_999,
        1_000_000,
        -1_000_000,
        57_600_000_000,  // 16h, the timezone displacement limit
        -57_600_000_000,
        7_777_777_777_777,
    ];

    macro_rules! izone_value {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(12)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let idx: usize = kani::any();
                kani::assume(idx < IZONE_ZT.len());
                let zt = IZONE_ZT[idx];
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(zt, 0, 0, ts, &mut cval) };
                let img = iv_img(zt, 0, 0);
                let r = proof_support::call2(
                    adt_timestamp::builtins::$fc,
                    Datum::from_usize(img.as_ptr() as usize),
                    ts,
                );
                check_i64_result!(r, cerr, cval, sql_izone);
            }
        )*};
    }

    izone_value! {
        eq_timestamp_izone_value: fc_timestamp_izone / pg_timestamp_izone;
        eq_timestamptz_izone_value: fc_timestamptz_izone / pg_timestamptz_izone;
    }

    /// Error planes: fully symbolic zone fenced OFF the ok-plane (months/
    /// days present or non-finite), full symbolic ts. Message text (incl.
    /// the interval_out rendering of the zone) leaves the proof.
    macro_rules! izone_err {
        ($($h:ident: $fc:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            #[kani::stub(adt_timestamp::interval::interval_out, w7_stub_interval_out)]
            fn $h() {
                let (zt, zd, zm): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
                kani::assume(zd != 0 || zm != 0);
                let ts: i64 = kani::any();
                let mut cval: i64 = 0;
                let cerr = unsafe { $pg(zt, zd, zm, ts, &mut cval) };
                let img = iv_img(zt, zd, zm);
                let r = proof_support::call2(
                    adt_timestamp::builtins::$fc,
                    Datum::from_usize(img.as_ptr() as usize),
                    ts,
                );
                check_i64_result!(r, cerr, cval, sql_izone);
            }
        )*};
    }

    izone_err! {
        eq_timestamp_izone_err: fc_timestamp_izone / pg_timestamp_izone;
        eq_timestamptz_izone_err: fc_timestamptz_izone / pg_timestamptz_izone;
    }

    // ---------- scale rows (1961, 1967) ----------

    /// Passthrough + error planes, fully symbolic where no divider runs:
    /// non-finite time (any typmod), typmod -1/6 passthrough, out-of-range
    /// typmod -> 22023.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_timestamp_scale_planes() {
        let ts: i64 = kani::any();
        let typmod: i32 = kani::any();
        kani::assume(
            ts == i64::MIN
                || ts == i64::MAX
                || typmod == -1
                || typmod == 6
                || typmod < 0
                || typmod > 6,
        );
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_timestamp_scale(ts, typmod, &mut cval) };
        let r = proof_support::call2(adt_timestamp::builtins::fc_timestamp_scale, ts, typmod);
        check_i64_result!(r, cerr, cval, sql_izone);
    }

    /// Rounding-arm spots: literal (typmod, time) cells — the /10^k divider
    /// is the loop-free band-immune class (TRIAGE), so the symbolic
    /// remainder is covered by the native differential, not by bands.
    const SCALE_CELLS: [(i32, i64); 16] = [
        (0, 0),
        (0, 499_999),
        (0, 500_000),
        (0, -499_999),
        (0, -500_000),
        (0, 1_755_555_555_123_456),
        (1, 49_999),
        (1, -50_000),
        (2, 4_999),
        (3, 1_755_555_555_123_456),
        (4, -1_755_555_555_123_456),
        (5, 4),
        (5, 5),
        (5, -5),
        // valid-timestamp extremes; the |time| near i64::MAX wrap region
        // (C -fwrapv wrap == Rust release wrap, but Kani flags the shipped
        // `+`) is covered by the native differential instead.
        (0, 9_223_371_331_199_999_999), // END_TIMESTAMP - 1
        (0, -211_813_488_000_000_000),  // MIN_TIMESTAMP
    ];

    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn spot_timestamp_scale() {
        let idx: usize = kani::any();
        kani::assume(idx < SCALE_CELLS.len());
        let (typmod, ts) = SCALE_CELLS[idx];
        let mut cval: i64 = 0;
        let cerr = unsafe { pg_timestamp_scale(ts, typmod, &mut cval) };
        let r = proof_support::call2(adt_timestamp::builtins::fc_timestamp_scale, ts, typmod);
        check_i64_result!(r, cerr, cval, sql_izone);
    }

    // ---------- in_range rows (4134, 4135, 4136) ----------

    fn sql_in_range(flag: c_int) -> types_error::SqlState {
        if flag == 3 {
            ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
        } else {
            ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
        }
    }

    macro_rules! in_range_ts {
        ($($m0:ident, $ne:ident: $fc:ident / $pg:ident;)*) => {$(
            /// offset month==day==0 LITERAL plane; val/base/sub/less full.
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
            fn $m0() {
                pgtz::set_session_timezone(Some(&W7_TZ));
                let (val, base, ot): (i64, i64, i64) = (kani::any(), kani::any(), kani::any());
                let (sub, less): (bool, bool) = (kani::any(), kani::any());
                let mut cres: c_int = -1;
                let cerr = unsafe {
                    $pg(val, base, ot, 0, 0, sub as c_int, less as c_int, &mut cres)
                };
                assert!(cerr != 99);
                let img = iv_img(ot, 0, 0);
                let r = proof_support::fcinfo::call(
                    adt_timestamp::builtins::$fc,
                    [
                        Datum::from_i64(val),
                        Datum::from_i64(base),
                        Datum::from_usize(img.as_ptr() as usize),
                        Datum::from_bool(sub),
                        Datum::from_bool(less),
                    ],
                );
                match r {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(cerr == 0);
                        assert!(d.as_bool() as c_int == cres);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(cerr != 0 && cerr != 99);
                        assert!(e.sqlstate == sql_in_range(cerr));
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }

            /// literal-NOEND offset plane (the infinity shortcut + sentinel
            /// pl/mi composition); val/base/sub/less full.
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(adt_timestamp::timestamp2tm, w7_timestamp2tm_out_of_plane)]
            #[kani::stub(adt_datetime::tz::DetermineTimeZoneOffset, w7_dtzo_out_of_plane)]
            fn $ne() {
                pgtz::set_session_timezone(Some(&W7_TZ));
                let (val, base): (i64, i64) = (kani::any(), kani::any());
                let (sub, less): (bool, bool) = (kani::any(), kani::any());
                let mut cres: c_int = -1;
                let cerr = unsafe {
                    $pg(val, base, i64::MAX, i32::MAX, i32::MAX,
                        sub as c_int, less as c_int, &mut cres)
                };
                assert!(cerr != 99);
                let img = iv_img(i64::MAX, i32::MAX, i32::MAX);
                let r = proof_support::fcinfo::call(
                    adt_timestamp::builtins::$fc,
                    [
                        Datum::from_i64(val),
                        Datum::from_i64(base),
                        Datum::from_usize(img.as_ptr() as usize),
                        Datum::from_bool(sub),
                        Datum::from_bool(less),
                    ],
                );
                match r {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(cerr == 0);
                        assert!(d.as_bool() as c_int == cres);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(cerr != 0 && cerr != 99);
                        assert!(e.sqlstate == sql_in_range(cerr));
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    in_range_ts! {
        eq_in_range_timestamp_interval_m0d0, eq_in_range_timestamp_interval_noend:
            fc_in_range_timestamp_interval / pg_in_range_timestamp_interval;
        eq_in_range_timestamptz_interval_m0d0, eq_in_range_timestamptz_interval_noend:
            fc_in_range_timestamptz_interval / pg_in_range_timestamptz_interval;
    }

    /// interval/interval: time-only plane — month/day of val, base AND
    /// offset all LITERAL 0, so interval_cmp_value's two-contributor i128
    /// multiply constant-folds on both sides. Verdict-only on the pl/mi Err
    /// arm (C 22015 interval-out-of-range is unreachable on this plane;
    /// kept as verdict parity).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_in_range_interval_interval_t0() {
        let (vt, bt, ot): (i64, i64, i64) = (kani::any(), kani::any(), kani::any());
        let (sub, less): (bool, bool) = (kani::any(), kani::any());
        let mut cres: c_int = -1;
        let cerr = unsafe {
            pg_in_range_interval_interval(
                vt, 0, 0, bt, 0, 0, ot, 0, 0, sub as c_int, less as c_int, &mut cres,
            )
        };
        let vimg = iv_img(vt, 0, 0);
        let bimg = iv_img(bt, 0, 0);
        let oimg = iv_img(ot, 0, 0);
        let r = proof_support::fcinfo::call(
            adt_timestamp::builtins::fc_in_range_interval_interval,
            [
                Datum::from_usize(vimg.as_ptr() as usize),
                Datum::from_usize(bimg.as_ptr() as usize),
                Datum::from_usize(oimg.as_ptr() as usize),
                Datum::from_bool(sub),
                Datum::from_bool(less),
            ],
        );
        match r {
            Ok(d) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(cerr == 0);
                assert!(d.as_bool() as c_int == cres);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(cerr != 0 && cerr != 99);
                if cerr == 3 {
                    assert!(e.sqlstate == ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE);
                }
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }
}
