//! datetime-b proof family — lane B/C datetime tail (pre-built, compile-gated;
//! ZERO solves run in this lane).
//!
//! Rows (proofs/USER_FACING_FUNCTIONS.tsv):
//!   1968 time_scale / 1969 timetz_scale        — fc-level; the /10^k face of
//!        AdjustTimeForTypmod is a LOOP-FREE constant-divider chain =
//!        band-immune per the wave-6 law, so it gets literal-plane +
//!        symbolic-index spot harnesses + the native differential bin, NOT
//!        magnitude ladders.
//!   2903 intervaltypmodin / 2904 intervaltypmodout — core-level (the out
//!        wrapper's OUT_SCRATCH thread_local and the in wrapper's array
//!        decode stay out; array_get_integer_typmods is a separate row).
//!        typmodout's %10-//10 digit-emission LOOP shrinks with magnitude:
//!        banded per the intout sloped law with exact unwinds + a mandatory
//!        union-coverage harness. Representative-range cells (MINUTE +
//!        FULL_RANGE) carry the digit loop; the 14-way fieldstr table is
//!        proven separately with the loop pruned (precision literal FULL).
//!   3846 make_date / 3847 make_time / 3461 make_timestamp — date2j's /100
//!        and /4 are small-constant LOOP-FREE dividers: the full-domain
//!        harness is the honest screen (LADDER), and the year-band harnesses
//!        use LITERAL-MASKED constructors (base + masked symbolic offset =
//!        concrete high bits, NOT assumes — assumes never constant-fold)
//!        + union coverage + boundary spots. The seconds argument's
//!        *USECS_PER_SEC is a 53-bit constant multiply (float law: wall):
//!        seconds ride a literal grid behind one symbolic index; the
//!        symbolic-seconds face belongs to the native differential.
//!   1843 interval_avg_accum / 3549 interval_avg_accum_inv /
//!   3325 interval_avg_combine — CORE-level over fully symbolic states
//!        (wrappers are agg-context plumbing, out of these rows per triage).
//!   6326 interval_sum / 1844 interval_avg — fc wrapper level; avg's mean
//!        arm (interval_div, 53-bit float divide) is fenced out by LITERAL
//!        plane fields (wave-6 literal-fold law) + a concrete spot; the
//!        symbolic mean arm belongs to the native differential.
//!   6324 interval_avg_serialize / 6325 interval_avg_deserialize — fc-level,
//!        fixed 40-byte BE image at literal offsets (not the
//!        data-dependent-width wall class); deserialize stubs
//!        fcinfo.agg_context to a dummy context (aggregate plumbing out of
//!        proof, both sides — the C shim drops AggCheckCallContext).
//!   1273 timetz_part (float form) — ONLY the pure-arithmetic tz field arms,
//!        as per-cell LITERAL field selectors: the Rust side decodes the
//!        literal unit token in-theorem; the C side takes the decoded val
//!        directly (documented seam).
//!
//! Soundness notes carried into ledger wording at run time:
//!   - PgError::error stubbed field-identically (message text/location out
//!     of proof; sqlstate/level parity asserted on Err arms).
//!   - std format machinery stubbed (stub_format).
//!   - mcx by-ref results ride proof_support's static-buffer allocator model
//!     ("modulo static-buffer allocator model").
//!   - intervaltypmodin's WARNING arm: elog::message_level_is_interesting is
//!     stubbed to `level >= ERROR`, so the shipped WARNING builder
//!     self-suppresses; C drops the ereport(WARNING). WARNING emission is
//!     out of proof BOTH sides; the clamped VALUE stays in-theorem.
//!   - out-of-plane trap: the C side's ValidateDate DOY arm sets a trap flag
//!     (asserted 0) instead of walking j2date — fmask is always the literal
//!     DTK_DATE_M here, so the arm must fold away; if it doesn't, harnesses
//!     FAIL loudly instead of walling or passing vacuously.

use std::os::raw::c_int;

use adt_datetime::consts::Interval;

/// C-layout mirror of timestamp.c's IntervalAggState (N, sumX, pInfcount,
/// nInfcount) — the shipped Rust IntervalAggState is not repr(C), so the
/// harness constructs both sides from the same symbolic values.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CIntervalAggState {
    pub n: i64,
    pub sum: Interval,
    pub p_infcount: i64,
    pub n_infcount: i64,
}

extern "C" {
    pub fn pg_reset_out_of_plane() -> c_int;
    pub fn pg_out_of_plane_reached() -> c_int;

    pub fn pg_time_scale(time: i64, typmod: i32, out: *mut i64) -> c_int;
    pub fn pg_timetz_scale(t_time: i64, t_zone: i32, typmod: i32, rt: *mut i64, rz: *mut i32)
        -> c_int;

    pub fn pg_make_date(y: i32, m: i32, d: i32, out: *mut i32, err: *mut c_int) -> c_int;
    pub fn pg_make_time(h: i32, m: i32, sec: f64, out: *mut i64, err: *mut c_int) -> c_int;
    pub fn pg_make_timestamp(
        y: i32,
        m: i32,
        d: i32,
        h: i32,
        mi: i32,
        sec: f64,
        out: *mut i64,
        err: *mut c_int,
    ) -> c_int;

    pub fn pg_intervaltypmodin(tl: *const i32, n: c_int, out: *mut i32, err: *mut c_int)
        -> c_int;
    pub fn pg_intervaltypmodout(typmod: i32, res: *mut u8, err: *mut c_int) -> c_int;

    pub fn pg_do_interval_accum(
        state: *mut CIntervalAggState,
        newval: *const Interval,
        err: *mut c_int,
    ) -> c_int;
    pub fn pg_do_interval_discard(
        state: *mut CIntervalAggState,
        newval: *const Interval,
        err: *mut c_int,
    ) -> c_int;
    pub fn pg_interval_avg_combine(
        state1: *mut CIntervalAggState,
        state2: *const CIntervalAggState,
        err: *mut c_int,
    ) -> c_int;
    pub fn pg_interval_avg(
        state: *const CIntervalAggState,
        result: *mut Interval,
        isnull: *mut c_int,
        err: *mut c_int,
    ) -> c_int;
    pub fn pg_interval_sum(
        state: *const CIntervalAggState,
        result: *mut Interval,
        isnull: *mut c_int,
        err: *mut c_int,
    ) -> c_int;
    pub fn pg_interval_avg_serialize(state: *const CIntervalAggState, out40: *mut u8) -> c_int;
    pub fn pg_interval_avg_deserialize(
        data: *const u8,
        len: c_int,
        result: *mut CIntervalAggState,
        err: *mut c_int,
    ) -> c_int;

    pub fn pg_timetz_part_units_float(
        t_time: i64,
        t_zone: i32,
        val: i32,
        out: *mut f64,
        err: *mut c_int,
    ) -> c_int;
}

/// C datetime.h DTK_* value tokens for the timetz_part cells (verbatim).
pub const C_DTK_TZ: i32 = 4;
pub const C_DTK_TZ_HOUR: i32 = 34;
pub const C_DTK_TZ_MINUTE: i32 = 35;

/// Short-varlena text images for the literal unit tokens (1-byte header,
/// little-endian: header = ((len + 1) << 1) | 1).
pub const UNITS_TIMEZONE: [u8; 9] = {
    let mut img = [0u8; 9];
    img[0] = ((9u8) << 1) | 1;
    let s = *b"timezone";
    let mut i = 0;
    while i < 8 {
        img[1 + i] = s[i];
        i += 1;
    }
    img
};
pub const UNITS_TIMEZONE_HOUR: [u8; 14] = {
    let mut img = [0u8; 14];
    img[0] = ((14u8) << 1) | 1;
    let s = *b"timezone_hour";
    let mut i = 0;
    while i < 13 {
        img[1 + i] = s[i];
        i += 1;
    }
    img
};
pub const UNITS_TIMEZONE_MINUTE: [u8; 16] = {
    let mut img = [0u8; 16];
    img[0] = ((16u8) << 1) | 1;
    let s = *b"timezone_minute";
    let mut i = 0;
    while i < 15 {
        img[1 + i] = s[i];
        i += 1;
    }
    img
};

#[cfg(kani)]
mod proofs {
    use super::*;
    use adt_timestamp::interval::{
        do_interval_accum, do_interval_discard, interval_agg_combine, intervaltypmodin,
        intervaltypmodout, IntervalAggState, INTERVAL_FULL_PRECISION,
    };
    use datum::Datum;
    use proof_support::fcinfo::{fci, FcFn};
    use proof_support::{mcx_stubs, stubs};
    use types_error::{
        PgError, ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
        ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
    };

    type PgResultDatum = Result<Datum, Box<PgError>>;

    /// Opaque dummy context for by-ref result frames (datetime-cmp wave-6
    /// precedent): with Mcx::{allocate,grow,deallocate} stubbed to the proof
    /// heap, no code in the theorem reads the pointee.
    fn dummy_mcx() -> mcx::Mcx<'static> {
        const _: () = assert!(core::mem::size_of::<mcx::MemoryContext>() <= 1024);
        const _: () = assert!(core::mem::align_of::<mcx::MemoryContext>() <= 16);
        #[repr(align(16))]
        struct DummySlot([u8; 1024]);
        // SAFETY: the slot is never read or written through.
        unsafe impl Sync for DummySlot {}
        static SLOT: DummySlot = DummySlot([0u8; 1024]);
        // SAFETY: never dereferenced — every Allocator entry point is
        // stubbed and nothing in these wrappers reads context state.
        let ctx: &'static mcx::MemoryContext =
            unsafe { &*(SLOT.0.as_ptr() as *const mcx::MemoryContext) };
        ctx.mcx()
    }

    /// Stub for `elog::message_level_is_interesting` (aclcheck precedent):
    /// WARNING output leaves the proof; ERROR-level throws keep working.
    fn model_level_interesting(elevel: types_error::ErrorLevel) -> bool {
        elevel >= ERROR
    }

    /// Unreachable at runtime here (the only builder-level report is the
    /// intervaltypmodin precision WARNING, suppressed by
    /// model_level_interesting); present so reachability codegen never
    /// enters the elog report subtree (ipc_seams::proc_exit::call ICEs the
    /// Kani 0.67 goto codegen — TRIAGE "KANI ICE #3", aclcheck precedent).
    fn model_throw_error_data(edata: PgError) -> types_error::PgResult<()> {
        if edata.level >= ERROR {
            Err(Box::new(edata))
        } else {
            Ok(())
        }
    }

    /// Stub for `types_fmgr::FunctionCallInfoBaseData::agg_context`
    /// (interval_avg_deserialize's aggregate-context guard): aggregate
    /// plumbing is out of proof both sides (the C shim drops
    /// AggCheckCallContext); the returned context is the stubbed dummy.
    ///
    /// # Safety
    /// mirrors the stubbed method's contract; the dummy context is 'static.
    unsafe fn stub_agg_context<'a>(
        _f: &types_fmgr::FunctionCallInfoBaseData,
    ) -> Option<mcx::Mcx<'a>>
    where
        'a: 'a,
    {
        Some(dummy_mcx())
    }

    /// Invoke a shipped fc_* wrapper with the result frame armed; returns
    /// the result plus the frame's isnull flag (PG_RETURN_NULL parity).
    fn call_fc<const N: usize>(fc: FcFn<Box<PgError>>, args: [Datum; N]) -> (PgResultDatum, bool) {
        let mut f = fci(args);
        // SAFETY: the dummy context is 'static; outlives the call.
        unsafe { f.set_result_mcx(dummy_mcx()) };
        let r = fc(None, &mut f);
        let isnull = f.isnull;
        (r, isnull)
    }

    fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
        let mut img = [0u8; 12];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..].copy_from_slice(&zone.to_ne_bytes());
        img
    }

    fn read_timetz(d: Datum) -> (i64, i32) {
        let p = d.as_usize() as *const u8;
        // SAFETY: 12-byte timetz image just written by the wrapper into the
        // proof heap.
        unsafe { ((p as *const i64).read_unaligned(), (p.add(8) as *const i32).read_unaligned()) }
    }

    fn read_iv(d: Datum) -> (i64, i32, i32) {
        let p = d.as_usize() as *const u8;
        // SAFETY: 16-byte interval image just written by the wrapper.
        unsafe {
            (
                (p as *const i64).read_unaligned(),
                (p.add(8) as *const i32).read_unaligned(),
                (p.add(12) as *const i32).read_unaligned(),
            )
        }
    }

    // =====================================================================
    // 1968 time_scale / 1969 timetz_scale
    // =====================================================================

    /// typmod < 0 plane (sign bit forced by LITERAL OR — not an assume):
    /// AdjustTimeForTypmod is a structural no-op, time rides the full i64
    /// domain. Expect fast; if the typmod>=0 guard fails to fold the divider
    /// face enters the formula (band-immune) — fall to spots, never ladder.
    #[kani::proof]
    fn eq_time_scale_typmod_neg() {
        let time: i64 = kani::any();
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        let mut c_out: i64 = 0;
        unsafe { pg_time_scale(time, typmod, &mut c_out) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_scale, [
            Datum::from_i64(time),
            Datum::from_i32(typmod),
        ]);
        match r {
            Ok(d) => assert!(d.as_i64() == c_out),
            Err(_) => panic!("time_scale is infallible"),
        }
    }

    /// typmod = 7 plane (> MAX_TIME_PRECISION, literal): no-op both sides.
    #[kani::proof]
    fn eq_time_scale_typmod_over() {
        let time: i64 = kani::any();
        let typmod: i32 = 7;
        let mut c_out: i64 = 0;
        unsafe { pg_time_scale(time, typmod, &mut c_out) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_scale, [
            Datum::from_i64(time),
            Datum::from_i32(typmod),
        ]);
        assert!(r.is_ok_and(|d| d.as_i64() == c_out));
    }

    /// typmod = 6 plane (scale 1, offset 0): the divide is by the literal 1.
    /// Fence: time != i64::MIN (the -*time negation is fwrapv-defined in C,
    /// panic-on-overflow in Rust — out-of-contract plane, crash-and-restart
    /// posture per the panic-fatality ruling; time values from time_in are
    /// always in [0, USECS_PER_DAY]).
    #[kani::proof]
    fn eq_time_scale_p6() {
        let time: i64 = kani::any();
        kani::assume(time != i64::MIN);
        let mut c_out: i64 = 0;
        unsafe { pg_time_scale(time, 6, &mut c_out) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_scale, [
            Datum::from_i64(time),
            Datum::from_i32(6),
        ]);
        assert!(r.is_ok_and(|d| d.as_i64() == c_out));
    }

    /// Concrete spot grid for the p0..p5 divider face (one symbolic index
    /// into a concrete table — geo-cmp lesson: never loop through the
    /// wrapper). Rounding boundaries (offset-1/offset), both signs, whole
    /// scale multiples, max valid time.
    #[kani::proof]
    fn spot_time_scale() {
        const T: &[(i64, i32)] = &[
            (0, 0),
            (499_999, 0),
            (500_000, 0),
            (1_499_999, 0),
            (-499_999, 0),
            (-500_000, 0),
            (86_399_999_999, 0),
            (49_999, 1),
            (50_000, 1),
            (-1_234_567, 1),
            (4_999, 2),
            (5_000, 2),
            (86_399_995_000, 2),
            (499, 3),
            (500, 3),
            (-86_399_999_999, 3),
            (49, 4),
            (50, 4),
            (12_345_678, 4),
            (4, 5),
            (5, 5),
            (86_399_999_994, 5),
            (86_399_999_995, 5),
            (-6, 5),
        ];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        let (time, typmod) = T[idx];
        let mut c_out: i64 = 0;
        unsafe { pg_time_scale(time, typmod, &mut c_out) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_scale, [
            Datum::from_i64(time),
            Datum::from_i32(typmod),
        ]);
        assert!(r.is_ok_and(|d| d.as_i64() == c_out));
    }

    /// Negative control (MUST FAIL, default solver): C rounds at typmod 2,
    /// Rust at typmod 3 — the rig must produce a decodable counterexample.
    #[kani::proof]
    fn control_time_scale_typmod_skew() {
        let time: i64 = kani::any();
        kani::assume((0..86_400_000_000).contains(&time));
        let mut c_out: i64 = 0;
        unsafe { pg_time_scale(time, 2, &mut c_out) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_scale, [
            Datum::from_i64(time),
            Datum::from_i32(3),
        ]);
        match r {
            Ok(d) => assert!(d.as_i64() == c_out),
            Err(_) => panic!("time_scale is infallible"),
        }
    }

    /// timetz_scale typmod<0 plane: time+zone fully symbolic; the zone
    /// passthrough and the by-ref result image are in-theorem (mcx stubs).
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_timetz_scale_typmod_neg() {
        let time: i64 = kani::any();
        let zone: i32 = kani::any();
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        let (mut rt, mut rz): (i64, i32) = (0, 0);
        unsafe { pg_timetz_scale(time, zone, typmod, &mut rt, &mut rz) };
        let img = timetz_img(time, zone);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_scale, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(typmod),
        ]);
        match r {
            Ok(d) => {
                let (t, z) = read_timetz(d);
                assert!(t == rt && z == rz);
            }
            Err(_) => panic!("timetz_scale is infallible"),
        }
    }

    /// timetz_scale spot cells: concrete (time, typmod) at rounding
    /// boundaries with the zone FULLY SYMBOLIC (untouched by the core —
    /// passthrough parity stays universally quantified).
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn spot_timetz_scale() {
        const T: &[(i64, i32)] = &[
            (499_999, 0),
            (500_000, 0),
            (49_999, 1),
            (5_000, 2),
            (500, 3),
            (49, 4),
            (5, 5),
            (86_399_999_999, 6),
            (-500_000, 0),
        ];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        let (time, typmod) = T[idx];
        let zone: i32 = kani::any();
        let (mut rt, mut rz): (i64, i32) = (0, 0);
        unsafe { pg_timetz_scale(time, zone, typmod, &mut rt, &mut rz) };
        let img = timetz_img(time, zone);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_scale, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(typmod),
        ]);
        match r {
            Ok(d) => {
                let (t, z) = read_timetz(d);
                assert!(t == rt && z == rz);
            }
            Err(_) => panic!("timetz_scale is infallible"),
        }
    }

    // =====================================================================
    // 2904 intervaltypmodout (core-level: (typmod, &mut [u8; 64]) -> len)
    // =====================================================================

    /// Fieldstr-selection theorem: precision pinned to the LITERAL
    /// INTERVAL_FULL_PRECISION low half (digit loop structurally pruned),
    /// range fully symbolic — 14-way fieldstr table, the typmod<0 empty
    /// plane, and the invalid-range error arm (XX000) all in-theorem.
    /// Whole-64-byte image compare (both buffers zero-initialized).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_intervaltypmodout_fieldstr_fullprec() {
        let range: i32 = kani::any();
        let typmod: i32 = (range << 16) | (INTERVAL_FULL_PRECISION & 0xFFFF);
        let mut c_buf = [0u8; 64];
        let mut c_err: c_int = 0;
        let c_len = unsafe { pg_intervaltypmodout(typmod, c_buf.as_mut_ptr(), &mut c_err) };
        let mut r_buf = [0u8; 64];
        match intervaltypmodout(typmod, &mut r_buf) {
            Ok(len) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(len as c_int == c_len);
                assert!(r_buf == c_buf);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_INTERNAL_ERROR);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    /// Digit-emission bands (intout sloped law: the loop SHRINKS with
    /// magnitude — assumes + EXACT unwind are structural here). Range pinned
    /// to the literal " minute" cell so every image offset is concrete;
    /// range coverage comes from eq_intervaltypmodout_fieldstr_fullprec.
    macro_rules! typmodout_prec_band {
        ($($h:ident: $lo:literal ..= $hi:literal, unwind $u:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let p: i32 = kani::any();
                kani::assume(($lo..=$hi).contains(&p));
                // INTERVAL_MASK(MINUTE) == 1 << 11 == 2048 (literal cell)
                let typmod: i32 = (2048 << 16) | p;
                let mut c_buf = [0u8; 64];
                let mut c_err: c_int = 0;
                let c_len =
                    unsafe { pg_intervaltypmodout(typmod, c_buf.as_mut_ptr(), &mut c_err) };
                let mut r_buf = [0u8; 64];
                let len = intervaltypmodout(typmod, &mut r_buf)
                    .unwrap_or_else(|_| panic!("valid literal range cell errored"));
                assert!(c_err == 0);
                assert!(len as c_int == c_len);
                assert!(r_buf == c_buf);
            }
        )*};
    }

    // unwind 66 for every band: the binding loop bound is NOT the digit
    // loop but (a) the whole-image r_buf == c_buf compare (64-byte memcmp
    // -> 65 iterations) and (b) the emit_str/fieldstr copy loops — fleet
    // 31ad423d failed d1-d3 "Not unwinding pg_proof_emit_str" at the
    // digit-only bounds, and +17 still truncated the 64-byte memcmp
    // (measured locally). Digit-band split still prices the divide chain.
    typmodout_prec_band! {
        eq_intervaltypmodout_prec_d1: 0 ..= 9, unwind 66;
        eq_intervaltypmodout_prec_d2: 10 ..= 99, unwind 66;
        eq_intervaltypmodout_prec_d3: 100 ..= 999, unwind 66;
        eq_intervaltypmodout_prec_d4: 1000 ..= 9999, unwind 66;
        eq_intervaltypmodout_prec_d5: 10000 ..= 65534, unwind 66;
    }

    /// Second representative range cell for the digit loop: FULL_RANGE
    /// (empty fieldstr, paren at offset 0), 2-digit band.
    /// unwind 66 (64-byte image memcmp + emit_str loops; see band comment).
    #[kani::proof]
    #[kani::unwind(66)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_intervaltypmodout_fullrange_prec_d2() {
        let p: i32 = kani::any();
        kani::assume((10..=99).contains(&p));
        // INTERVAL_FULL_RANGE == 0x7FFF (literal cell)
        let typmod: i32 = (0x7FFF << 16) | p;
        let mut c_buf = [0u8; 64];
        let mut c_err: c_int = 0;
        let c_len = unsafe { pg_intervaltypmodout(typmod, c_buf.as_mut_ptr(), &mut c_err) };
        let mut r_buf = [0u8; 64];
        let len = intervaltypmodout(typmod, &mut r_buf)
            .unwrap_or_else(|_| panic!("valid literal range cell errored"));
        assert!(c_err == 0);
        assert!(len as c_int == c_len);
        assert!(r_buf == c_buf);
    }

    /// MANDATORY union coverage for the precision bands: every non-FULL
    /// precision value (0..=0xFFFE) lies in exactly one band.
    #[kani::proof]
    fn cover_intervaltypmodout_prec_bands() {
        let p: i32 = kani::any();
        kani::assume((0..=0xFFFE).contains(&p));
        assert!(
            (0..=9).contains(&p)
                || (10..=99).contains(&p)
                || (100..=999).contains(&p)
                || (1000..=9999).contains(&p)
                || (10000..=65534).contains(&p)
        );
    }

    // =====================================================================
    // 2903 intervaltypmodin (core-level: &[i32] -> i32)
    // =====================================================================

    macro_rules! check_typmodin {
        ($tl:expr, $c_tl:expr) => {{
            let mut c_out: i32 = 0;
            let mut c_err: c_int = 0;
            unsafe {
                pg_intervaltypmodin($c_tl.as_ptr(), $c_tl.len() as c_int, &mut c_out, &mut c_err)
            };
            match intervaltypmodin($tl) {
                Ok(v) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!(c_err == 0);
                    assert!(v == c_out);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!(c_err == 1);
                    assert!(e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE);
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        }};
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
    #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
    fn eq_intervaltypmodin_n0() {
        let tl: [i32; 0] = [];
        check_typmodin!(&tl, tl);
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
    #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
    fn eq_intervaltypmodin_n1() {
        let tl: [i32; 1] = [kani::any()];
        check_typmodin!(&tl, tl);
    }

    /// n=2 includes the precision arms: negative -> 22023 error, > MAX ->
    /// clamp with the WARNING suppressed both sides (see module doc).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
    #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
    fn eq_intervaltypmodin_n2() {
        let tl: [i32; 2] = [kani::any(), kani::any()];
        check_typmodin!(&tl, tl);
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
    #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
    fn eq_intervaltypmodin_n3() {
        let tl: [i32; 3] = [kani::any(), kani::any(), kani::any()];
        check_typmodin!(&tl, tl);
    }

    // =====================================================================
    // 3846 make_date (fc-level)
    // =====================================================================

    macro_rules! check_make_date {
        ($y:expr, $m:expr, $d:expr) => {{
            unsafe { pg_reset_out_of_plane() };
            let mut c_out: i32 = 0;
            let mut c_err: c_int = 0;
            unsafe { pg_make_date($y, $m, $d, &mut c_out, &mut c_err) };
            assert!(unsafe { pg_out_of_plane_reached() } == 0, "DOY plane violation");
            let (r, _) = call_fc(adt_date::builtins::fc_make_date, [
                Datum::from_i32($y),
                Datum::from_i32($m),
                Datum::from_i32($d),
            ]);
            match r {
                Ok(dd) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!(c_err == 0);
                    assert!(dd.as_i32() == c_out);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!(c_err == 1);
                    // both C arms are sqlstate 22008
                    assert!(
                        e.sqlstate == ERRCODE_DATETIME_FIELD_OVERFLOW
                            || e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
                    );
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        }};
    }

    /// AD year band via LITERAL-MASKED constructor (high bits concretely
    /// zero — the loop-free /100 and /4 stay in a narrowed circuit; assumes
    /// would not fold). Covers [1, 0x800000] ⊇ [1, JULIAN_MAXYEAR].
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_make_date_ad_band() {
        let raw: u32 = kani::any();
        let year: i32 = 1 + (raw & 0x007F_FFFF) as i32;
        let (month, day): (i32, i32) = (kani::any(), kani::any());
        check_make_date!(year, month, day);
    }

    /// BC/zero year band: [-0x7FFFFF, 0]; includes the BC fold and the
    /// year-zero DTERR arm.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_make_date_bc_band() {
        let raw: u32 = kani::any();
        let year: i32 = -((raw & 0x007F_FFFF) as i32);
        let (month, day): (i32, i32) = (kani::any(), kani::any());
        check_make_date!(year, month, day);
    }

    /// Honest full-domain screen of the loop-free date2j dividers (LADDER:
    /// may wall per the band-immune law — the band harnesses + spots +
    /// native differential are the standing fallback).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_make_date_full() {
        let (year, month, day): (i32, i32, i32) = (kani::any(), kani::any(), kani::any());
        check_make_date!(year, month, day);
    }

    /// Union coverage for the year bands: every year the bands do not carry
    /// is IS_VALID_JULIAN-rejected on BOTH sides' shared bound (pure
    /// compares; the BC fold shifts by one and stays out of range).
    #[kani::proof]
    fn cover_make_date_year_bands() {
        let y: i32 = kani::any();
        if (-0x007F_FFFF..=0x0080_0000).contains(&y) {
            // carried by the ad/bc band constructors
            let in_ad = (1..=0x0080_0000).contains(&y);
            let in_bc = (-0x007F_FFFF..=0).contains(&y);
            assert!(in_ad || in_bc);
        } else if y == i32::MIN {
            // make_date: pg_neg_s32_overflow -> 22008 before any julian
            // logic; make_timestamp: wrapping neg -> year<=0 DTERR. Both
            // sides error before date2j (spot_make_date pins this cell).
        } else {
            // outside the bands: the (BC-folded) internal year is beyond
            // the IS_VALID_JULIAN window [-4713, 5874898] — both sides
            // reject before any divider. (fold: y<0 => internal = y+1;
            // JULIAN_MAXYEAR = 5874898 < 0x800000.)
            let internal = if y < 0 { y + 1 } else { y };
            assert!(internal < -4713 || internal > 5_874_898);
        }
    }

    /// Boundary spots (symbolic index into a concrete table): leap-year
    /// grid, Julian range edges, year zero, extreme i32s.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn spot_make_date() {
        const T: &[(i32, i32, i32)] = &[
            (2024, 2, 29),
            (2023, 2, 29),
            (2000, 2, 29),
            (1900, 2, 29),
            (1, 1, 1),
            (0, 1, 1),
            (-1, 2, 29),      // 2 BC -> internal year 1? (BC fold parity)
            (-4713, 11, 24),  // JULIAN_MINYEAR boundary
            (-4713, 11, 23),
            (-4714, 12, 31),
            (5874897, 12, 31),
            (5874898, 6, 3),  // JULIAN_MAXYEAR/MAXMONTH edge
            (2147483647, 1, 1),
            (-2147483648, 1, 1),
            (2024, 13, 1),
            (2024, 0, 1),
            (2024, 1, 32),
            (2024, 1, 0),
        ];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        let (y, m, d) = T[idx];
        check_make_date!(y, m, d);
    }

    /// Negative control (MUST FAIL, default solver): day skew.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_make_date_day_skew() {
        let day: i32 = kani::any();
        kani::assume((1..=27).contains(&day));
        unsafe { pg_reset_out_of_plane() };
        let mut c_out: i32 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_make_date(2024, 1, day + 1, &mut c_out, &mut c_err) };
        let (r, _) = call_fc(adt_date::builtins::fc_make_date, [
            Datum::from_i32(2024),
            Datum::from_i32(1),
            Datum::from_i32(day),
        ]);
        match r {
            Ok(dd) => assert!(c_err == 0 && dd.as_i32() == c_out),
            Err(e) => {
                assert!(c_err == 1);
                core::mem::forget(e);
            }
        }
    }

    // =====================================================================
    // 3847 make_time (fc-level; seconds on a literal grid — the 53-bit
    // constant multiply of a symbolic f64 is the float-law wall)
    // =====================================================================

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_make_time_secgrid() {
        const SECS: &[f64] = &[
            0.0,
            0.5,
            1.5,
            59.499999,
            59.5,
            59.999999,
            59.9999995,
            60.0,
            60.000001,
            -0.25,
            -1e10,
            1e10,
            12.345678,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        let idx: usize = kani::any();
        kani::assume(idx < SECS.len());
        let sec = SECS[idx];
        let (hour, min): (i32, i32) = (kani::any(), kani::any());
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_make_time(hour, min, sec, &mut c_out, &mut c_err) };
        let (r, _) = call_fc(adt_date::builtins::fc_make_time, [
            Datum::from_i32(hour),
            Datum::from_i32(min),
            Datum::from_f64(sec),
        ]);
        match r {
            Ok(d) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(d.as_i64() == c_out);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_FIELD_OVERFLOW);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    // =====================================================================
    // 3461 make_timestamp (fc-level; year bands as make_date, seconds grid)
    // =====================================================================

    macro_rules! check_make_timestamp {
        ($y:expr, $mo:expr, $d:expr, $h:expr, $mi:expr, $s:expr) => {{
            unsafe { pg_reset_out_of_plane() };
            let mut c_out: i64 = 0;
            let mut c_err: c_int = 0;
            unsafe { pg_make_timestamp($y, $mo, $d, $h, $mi, $s, &mut c_out, &mut c_err) };
            assert!(unsafe { pg_out_of_plane_reached() } == 0, "DOY plane violation");
            let (r, _) = call_fc(adt_timestamp::builtins::fc_make_timestamp, [
                Datum::from_i32($y),
                Datum::from_i32($mo),
                Datum::from_i32($d),
                Datum::from_i32($h),
                Datum::from_i32($mi),
                Datum::from_f64($s),
            ]);
            match r {
                Ok(dd) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!(c_err == 0);
                    assert!(dd.as_i64() == c_out);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!(c_err == 1);
                    assert!(
                        e.sqlstate == ERRCODE_DATETIME_FIELD_OVERFLOW
                            || e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
                    );
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        }};
    }

    const TS_SECS: &[f64] = &[0.0, 59.999999, 60.0, f64::NAN];

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_make_timestamp_ad_band() {
        let raw: u32 = kani::any();
        let year: i32 = 1 + (raw & 0x007F_FFFF) as i32;
        let (month, day, hour, min): (i32, i32, i32, i32) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let sidx: usize = kani::any();
        kani::assume(sidx < TS_SECS.len());
        check_make_timestamp!(year, month, day, hour, min, TS_SECS[sidx]);
    }

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_make_timestamp_bc_band() {
        let raw: u32 = kani::any();
        let year: i32 = -((raw & 0x007F_FFFF) as i32);
        let (month, day, hour, min): (i32, i32, i32, i32) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let sidx: usize = kani::any();
        kani::assume(sidx < TS_SECS.len());
        check_make_timestamp!(year, month, day, hour, min, TS_SECS[sidx]);
    }

    /// Timestamp range edges (END_TIMESTAMP is stricter than the date
    /// range): concrete spots.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn spot_make_timestamp() {
        const T: &[(i32, i32, i32, i32, i32, f64)] = &[
            (294276, 12, 31, 23, 59, 59.999999),
            (294277, 1, 1, 0, 0, 0.0),
            (294276, 12, 31, 24, 0, 0.0),
            (-4713, 11, 24, 0, 0, 0.0),
            (-4714, 11, 24, 0, 0, 0.0),
            (2000, 1, 1, 0, 0, 0.0),
            (1970, 1, 1, 0, 0, 0.0),
            (0, 1, 1, 0, 0, 0.0),
            (2024, 2, 29, 24, 0, 0.0),
            (2024, 2, 29, 23, 60, 0.0),
        ];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        let (y, mo, d, h, mi, s) = T[idx];
        check_make_timestamp!(y, mo, d, h, mi, s);
    }

    // =====================================================================
    // 1843 / 3549 / 3325: aggregate transition cores (fully symbolic)
    // =====================================================================

    /// Counter fence: aggregate counters (N / pInfcount / nInfcount) are
    /// row counts — nonnegative, and bounded far below overflow-adjacent
    /// magnitudes. 0 <= c < 2^62 keeps every counter add/sub (accum +1,
    /// combine c1+c2, final pInf+nInf) inside i64 with headroom, killing
    /// the fleet 31ad423d "attempt to add/subtract with overflow" Kani
    /// checks on UNREACHABLE states (real states grow by +-1 per row from
    /// 0; C is -fwrapv on the same plane). The fence is part of the
    /// recorded bounds. Bound is 2^61 (not 2^62): interval_sum_final sums
    /// THREE counters (N + pInfcount + nInfcount) — 3 * 2^61 < i64::MAX.
    fn any_counter() -> i64 {
        let v: i64 = kani::any();
        kani::assume(v >= 0 && v < (1i64 << 61));
        v
    }

    /// Discard-side fence: the C caller contract (Assert(state->N == 0)
    /// after decrement, i.e. values are only discarded if previously
    /// accumulated) requires every counter a discard can decrement to be
    /// >= 1. The N==1 -> 0 reset branch stays in-theorem.
    fn any_counter_ge1() -> i64 {
        let v = any_counter();
        kani::assume(v >= 1);
        v
    }

    fn any_state() -> (IntervalAggState, CIntervalAggState) {
        let n: i64 = any_counter();
        let p: i64 = any_counter();
        let ni: i64 = any_counter();
        let sum = Interval { time: kani::any(), day: kani::any(), month: kani::any() };
        (
            IntervalAggState { N: n, pInfcount: p, nInfcount: ni, sumX: sum },
            CIntervalAggState { n, sum, p_infcount: p, n_infcount: ni },
        )
    }

    fn assert_state_eq(r: &IntervalAggState, c: &CIntervalAggState) {
        assert!(r.N == c.n);
        assert!(r.pInfcount == c.p_infcount);
        assert!(r.nInfcount == c.n_infcount);
        assert!(r.sumX.time == c.sum.time);
        assert!(r.sumX.day == c.sum.day);
        assert!(r.sumX.month == c.sum.month);
    }

    macro_rules! agg_core_op {
        ($($h:ident: $core:ident / $pg:ident;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let (mut r_state, mut c_state) = any_state();
                let newval = Interval {
                    time: kani::any(),
                    day: kani::any(),
                    month: kani::any(),
                };
                let mut c_err: c_int = 0;
                unsafe { $pg(&mut c_state, &newval, &mut c_err) };
                match $core(&mut r_state, &newval) {
                    Ok(()) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(c_err == 0);
                        assert_state_eq(&r_state, &c_state);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(c_err == 1);
                        assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    agg_core_op! {
        eq_interval_avg_accum_core: do_interval_accum / pg_do_interval_accum;
    }

    /// discard core: same claim as the macro rows but on the DISCARD
    /// caller-contract plane (counters >= 1; see any_counter_ge1).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_interval_avg_accum_inv_core() {
        let n = any_counter_ge1();
        let p = any_counter_ge1();
        let ni = any_counter_ge1();
        let sum = Interval { time: kani::any(), day: kani::any(), month: kani::any() };
        let mut r_state = IntervalAggState { N: n, pInfcount: p, nInfcount: ni, sumX: sum };
        let mut c_state = CIntervalAggState { n, sum, p_infcount: p, n_infcount: ni };
        let newval = Interval { time: kani::any(), day: kani::any(), month: kani::any() };
        let mut c_err: c_int = 0;
        unsafe { pg_do_interval_discard(&mut c_state, &newval, &mut c_err) };
        match do_interval_discard(&mut r_state, &newval) {
            Ok(()) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert_state_eq(&r_state, &c_state);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    /// combine: both-states-non-null path (the null-state arms are fcinfo
    /// plumbing in the shipped wrapper, not this core row).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_interval_avg_combine_core() {
        let (mut r1, mut c1) = any_state();
        let (r2, c2) = any_state();
        let mut c_err: c_int = 0;
        unsafe { pg_interval_avg_combine(&mut c1, &c2, &mut c_err) };
        match interval_agg_combine(&mut r1, &r2) {
            Ok(()) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert_state_eq(&r1, &c1);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    // =====================================================================
    // 6326 interval_sum / 1844 interval_avg (fc wrapper level)
    // =====================================================================

    /// interval_sum final: fully symbolic state — pure lattice, no divider.
    /// Null verdict + by-ref result image + err parity all in-theorem.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_interval_sum_fc() {
        let (r_state, c_state) = any_state();
        let mut c_res = Interval::default();
        let (mut c_isnull, mut c_err): (c_int, c_int) = (0, 0);
        unsafe { pg_interval_sum(&c_state, &mut c_res, &mut c_isnull, &mut c_err) };
        let (r, isnull) = call_fc(adt_timestamp::builtins::fc_interval_sum, [
            Datum::from_usize(&r_state as *const IntervalAggState as usize),
        ]);
        match r {
            Ok(d) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(isnull == (c_isnull == 1));
                if !isnull {
                    kani::cover!(true, "non-null result reachable");
                    let (t, dd, m) = read_iv(d);
                    assert!(t == c_res.time && dd == c_res.day && m == c_res.month);
                }
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    /// interval_avg planes: the fencing fields are LITERAL (wave-6
    /// literal-fold law — literal struct fields fold through by-ref reads
    /// and prune the interval_div mean arm structurally). The mean arm
    /// itself is the 53-bit float divide: concrete spot + native
    /// differential.
    macro_rules! avg_plane {
        ($($h:ident: n=$n:expr, p=$p:expr, ni=$ni:expr;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            fn $h() {
                let sum = Interval {
                    time: kani::any(),
                    day: kani::any(),
                    month: kani::any(),
                };
                let r_state = IntervalAggState {
                    N: $n,
                    pInfcount: $p,
                    nInfcount: $ni,
                    sumX: sum,
                };
                let c_state = CIntervalAggState {
                    n: $n,
                    sum,
                    p_infcount: $p,
                    n_infcount: $ni,
                };
                let mut c_res = Interval::default();
                let (mut c_isnull, mut c_err): (c_int, c_int) = (0, 0);
                unsafe { pg_interval_avg(&c_state, &mut c_res, &mut c_isnull, &mut c_err) };
                let (r, isnull) = call_fc(adt_timestamp::builtins::fc_interval_avg, [
                    Datum::from_usize(&r_state as *const IntervalAggState as usize),
                ]);
                match r {
                    Ok(d) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(c_err == 0);
                        assert!(isnull == (c_isnull == 1));
                        if !isnull {
                            let (t, dd, m) = read_iv(d);
                            assert!(t == c_res.time && dd == c_res.day && m == c_res.month);
                        }
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(c_err == 1);
                        assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    avg_plane! {
        eq_interval_avg_fc_empty: n=0, p=0, ni=0;
        eq_interval_avg_fc_pinf: n=any_counter(), p=1, ni=0;
        eq_interval_avg_fc_ninf: n=any_counter(), p=0, ni=1;
        eq_interval_avg_fc_conflict: n=any_counter(), p=1, ni=1;
    }

    /// Mean-arm concrete spot (everything literal, interval_div folds).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn spot_interval_avg_fc_mean() {
        let sum = Interval { time: 3_600_000_000, day: 5, month: 7 };
        let r_state = IntervalAggState { N: 4, pInfcount: 0, nInfcount: 0, sumX: sum };
        let c_state = CIntervalAggState { n: 4, sum, p_infcount: 0, n_infcount: 0 };
        let mut c_res = Interval::default();
        let (mut c_isnull, mut c_err): (c_int, c_int) = (0, 0);
        unsafe { pg_interval_avg(&c_state, &mut c_res, &mut c_isnull, &mut c_err) };
        let (r, isnull) = call_fc(adt_timestamp::builtins::fc_interval_avg, [
            Datum::from_usize(&r_state as *const IntervalAggState as usize),
        ]);
        match r {
            Ok(d) => {
                assert!(c_err == 0 && c_isnull == 0 && !isnull);
                let (t, dd, m) = read_iv(d);
                assert!(t == c_res.time && dd == c_res.day && m == c_res.month);
            }
            Err(_) => panic!("concrete mean spot errored"),
        }
    }

    // =====================================================================
    // 6324 interval_avg_serialize / 6325 interval_avg_deserialize (fc)
    // =====================================================================

    /// serialize: fixed 40-byte BE image at literal offsets (+ the 4-byte
    /// varlena header the shipped wrapper writes).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_interval_avg_serialize_fc() {
        let (r_state, c_state) = any_state();
        let mut c_img = [0u8; 40];
        unsafe { pg_interval_avg_serialize(&c_state, c_img.as_mut_ptr()) };
        let (r, _) = call_fc(adt_timestamp::builtins::fc_interval_avg_serialize, [
            Datum::from_usize(&r_state as *const IntervalAggState as usize),
        ]);
        match r {
            Ok(d) => {
                let p = d.as_usize() as *const u8;
                // SAFETY: the wrapper just wrote a 44-byte bytea image.
                let img = unsafe { core::slice::from_raw_parts(p, 44) };
                // 4-byte uncompressed varlena header, little-endian: len<<2.
                let hdr = u32::from_le_bytes([img[0], img[1], img[2], img[3]]);
                assert!(hdr == (44u32) << 2);
                let mut i = 0;
                while i < 40 {
                    assert!(img[4 + i] == c_img[i]);
                    i += 1;
                }
            }
            Err(_) => panic!("serialize errored (proof-heap alloc is infallible here)"),
        }
    }

    /// Negative control (MUST FAIL, default solver): C serialize of a state
    /// with day/month swapped must diverge whenever day != month.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn control_interval_avg_serialize_swap() {
        let (r_state, mut c_state) = any_state();
        core::mem::swap(&mut c_state.sum.day, &mut c_state.sum.month);
        let mut c_img = [0u8; 40];
        unsafe { pg_interval_avg_serialize(&c_state, c_img.as_mut_ptr()) };
        let (r, _) = call_fc(adt_timestamp::builtins::fc_interval_avg_serialize, [
            Datum::from_usize(&r_state as *const IntervalAggState as usize),
        ]);
        match r {
            Ok(d) => {
                let p = d.as_usize() as *const u8;
                // SAFETY: as eq_interval_avg_serialize_fc.
                let img = unsafe { core::slice::from_raw_parts(p, 44) };
                let mut i = 0;
                while i < 40 {
                    assert!(img[4 + i] == c_img[i]);
                    i += 1;
                }
            }
            Err(_) => panic!("serialize errored"),
        }
    }

    /// deserialize: symbolic 40-byte payload behind a literal exact-length
    /// varlena header; agg_context stubbed (see module doc). Field-level
    /// state parity through the returned pointer datum.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(types_fmgr::FunctionCallInfoBaseData::agg_context, stub_agg_context)]
    fn eq_interval_avg_deserialize_fc() {
        let payload: [u8; 40] = kani::any();
        let mut img = [0u8; 44];
        // 4-byte uncompressed varlena header (LE): total length 44 << 2.
        img[..4].copy_from_slice(&((44u32) << 2).to_le_bytes());
        img[4..].copy_from_slice(&payload);
        let mut c_state = CIntervalAggState {
            n: 0,
            sum: Interval::default(),
            p_infcount: 0,
            n_infcount: 0,
        };
        let mut c_err: c_int = 0;
        unsafe { pg_interval_avg_deserialize(payload.as_ptr(), 40, &mut c_state, &mut c_err) };
        let (r, _) = call_fc(adt_timestamp::builtins::fc_interval_avg_deserialize, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(d) => {
                assert!(c_err == 0);
                // SAFETY: the wrapper returned a pointer to a fresh
                // IntervalAggState in the (stubbed) proof heap.
                let out = unsafe { &*(d.as_usize() as *const IntervalAggState) };
                assert_state_eq(out, &c_state);
            }
            Err(e) => {
                // exact-length image: no error arm on either side
                assert!(c_err == 1);
                core::mem::forget(e);
            }
        }
    }

    /// SCREENED DIVERGENCE PROBE (expected FAIL, default solver): a 41-byte
    /// payload — C's pq_getmsgend rejects trailing bytes, the shipped Rust
    /// wrapper reads fixed offsets and ignores the tail. A FAIL here is the
    /// witness; ground-truth against real glibc PG 18 before recording
    /// anything (internal serial format, reachable only through the
    /// aggregate-combine protocol).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(types_fmgr::FunctionCallInfoBaseData::agg_context, stub_agg_context)]
    fn witness_interval_avg_deserialize_trailing() {
        let payload: [u8; 41] = kani::any();
        let mut img = [0u8; 45];
        img[..4].copy_from_slice(&((45u32) << 2).to_le_bytes());
        img[4..].copy_from_slice(&payload);
        let mut c_state = CIntervalAggState {
            n: 0,
            sum: Interval::default(),
            p_infcount: 0,
            n_infcount: 0,
        };
        let mut c_err: c_int = 0;
        unsafe { pg_interval_avg_deserialize(payload.as_ptr(), 41, &mut c_state, &mut c_err) };
        let (r, _) = call_fc(adt_timestamp::builtins::fc_interval_avg_deserialize, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        // parity assertion — expected to FAIL (C errors, Rust succeeds)
        match r {
            Ok(_) => assert!(c_err == 0),
            Err(e) => {
                assert!(c_err == 1);
                core::mem::forget(e);
            }
        }
    }

    // =====================================================================
    // 1273 timetz_part — tz field arms (per-cell literal selectors)
    // =====================================================================

    macro_rules! timetz_part_cell {
        ($($h:ident: $units:ident, $val:expr, $time:expr;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            // env/OnceLock pair: routes mcx pool traffic off the
            // thread_local-destructor arm — the Linux toolchain's TLS
            // destructor registration crashes CBMC (status 6,
            // pthread_key_create type mismatch; fleet 31ad423d).
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            fn $h() {
                let zone: i32 = kani::any();
                // Contract fence (lane doctrine): validated timetz zone
                // displacement, |zone| < 16h. Outside it, DTK_TZ's -zone
                // negate wraps (C -fwrapv vs Kani's overflow check on
                // shipped release-wrap code — the artifact class; zone =
                // i32::MIN fired "attempt to negate with overflow").
                kani::assume(zone > -57_600 && zone < 57_600);
                let time: i64 = $time;
                let mut c_out: f64 = 0.0;
                let mut c_err: c_int = 0;
                unsafe { pg_timetz_part_units_float(time, zone, $val, &mut c_out, &mut c_err) };
                let timg = timetz_img(time, zone);
                let (r, isnull) = call_fc(adt_date::builtins::fc_timetz_part, [
                    Datum::from_usize($units.as_ptr() as usize),
                    Datum::from_usize(timg.as_ptr() as usize),
                ]);
                match r {
                    Ok(d) => {
                        assert!(c_err == 0);
                        assert!(!isnull);
                        assert!(d.as_f64().to_bits() == c_out.to_bits());
                    }
                    Err(e) => {
                        // tz arms have no error path; a FAIL here means the
                        // Rust decode of the literal unit token diverged —
                        // itself a finding.
                        assert!(c_err == 1);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    timetz_part_cell! {
        eq_timetz_part_tz: UNITS_TIMEZONE, C_DTK_TZ, 0;
        eq_timetz_part_tz_hour: UNITS_TIMEZONE_HOUR, C_DTK_TZ_HOUR, 0;
        eq_timetz_part_tz_minute: UNITS_TIMEZONE_MINUTE, C_DTK_TZ_MINUTE, 0;
        spot_timetz_part_tz_time_nonzero: UNITS_TIMEZONE, C_DTK_TZ, 45_296_123_456;
    }
}

// =========================================================================
// Lane D: adt_date remainder rows (see the C file's lane-D section header
// for the row list + shim inventory). Hosted here per lane charter; the
// original datetime-b runqueue above is untouched (its rows stay owned by
// the pre-build lane).
//
// Soundness wording for the ledger (beyond the crate-header notes):
//   - contract fences (documented C caller contracts, all spelled with
//     CONSTANT bounds per the dt-minmax fence law):
//       time values      [0, USECS_PER_DAY]   (time_in/time_recv invariant)
//       timetz zones     (-TZDISP_LIMIT, TZDISP_LIMIT)
//       dates            >= -POSTGRES_EPOCH_JDATE (-2451545) union NOBEGIN
//                        (i32::MIN) — the sub-lower-bound multiply region is
//                        C -fwrapv == Rust release-wrap parity and belongs
//                        to the native differential, not to Kani (wave-7
//                        overflow-check lesson).
//   - WARNING emission (anytime_typmod_check precision clamp) out of proof
//     BOTH sides (message_level_is_interesting stub / dropped ereport).
// =========================================================================

extern "C" {
    pub fn pg_adr_hashdate(date: i32) -> u32;
    pub fn pg_adr_hashdate_extended(date: i32, seed: u64) -> u64;
    pub fn pg_adr_time_hash(t: i64) -> u32;
    pub fn pg_adr_time_hash_extended(t: i64, seed: u64) -> u64;
    pub fn pg_adr_timetz_hash(t_time: i64, t_zone: i32) -> u32;
    pub fn pg_adr_timetz_hash_extended(t_time: i64, t_zone: i32, seed: u64) -> u64;
    pub fn pg_adr_interval_hash(t: i64, d: i32, m: i32) -> u32;
    pub fn pg_adr_interval_hash_extended(t: i64, d: i32, m: i32, seed: u64) -> u64;

    pub fn pg_adr_date_finite(date: i32) -> c_int;
    pub fn pg_adr_interval_finite(t: i64, d: i32, m: i32) -> c_int;
    pub fn pg_adr_date_mi(d1: i32, d2: i32, out: *mut i32, err: *mut c_int) -> c_int;

    pub fn pg_adr_date_timestamp(date: i32, out: *mut i64, err: *mut c_int) -> c_int;
    pub fn pg_adr_datetime_timestamp(date: i32, time: i64, out: *mut i64, err: *mut c_int)
        -> c_int;
    pub fn pg_adr_datetimetz_timestamptz(
        date: i32,
        t_time: i64,
        t_zone: i32,
        out: *mut i64,
        err: *mut c_int,
    ) -> c_int;

    pub fn pg_adr_time_interval(time: i64, result: *mut Interval) -> c_int;
    pub fn pg_adr_interval_time(t: i64, d: i32, m: i32, out: *mut i64, err: *mut c_int) -> c_int;
    pub fn pg_adr_time_mi_time(t1: i64, t2: i64, result: *mut Interval) -> c_int;
    pub fn pg_adr_timetz_time(t_time: i64, t_zone: i32) -> i64;

    pub fn pg_adr_overlaps_time(
        ts1: i64, n1: c_int, te1: i64, n2: c_int,
        ts2: i64, n3: c_int, te2: i64, n4: c_int,
        result: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_overlaps_timetz(
        t1t: i64, t1z: i32, n1: c_int,
        e1t: i64, e1z: i32, n2: c_int,
        t2t: i64, t2z: i32, n3: c_int,
        e2t: i64, e2z: i32, n4: c_int,
        result: *mut c_int,
    ) -> c_int;

    pub fn pg_adr_anytime_typmod_check(istz: c_int, typmod: i32, out: *mut i32, err: *mut c_int)
        -> c_int;
    pub fn pg_adr_anytime_typmodout(istz: c_int, typmod: i32, res: *mut u8) -> c_int;

    pub fn pg_adr_in_range_time_interval(
        val: i64, base: i64, ot: i64, od: i32, om: i32,
        sub: c_int, less: c_int, result: *mut c_int, err: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_in_range_timetz_interval(
        vt: i64, vz: i32, bt: i64, bz: i32, ot: i64, od: i32, om: i32,
        sub: c_int, less: c_int, result: *mut c_int, err: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_in_range_date_interval(
        val: i32, base: i32, ot: i64, od: i32, om: i32,
        sub: c_int, less: c_int, result: *mut c_int, err: *mut c_int,
    ) -> c_int;

    pub fn pg_adr_timetz_izone(
        zt: i64, zd: i32, zm: i32, t_time: i64, t_zone: i32,
        out_time: *mut i64, out_zone: *mut i32, err: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_interval_scale(
        t: i64, d: i32, m: i32, typmod: i32, result: *mut Interval, err: *mut c_int,
    ) -> c_int;

    pub fn pg_adr_date_recv(data: *const u8, len: c_int, out: *mut i32, err: *mut c_int) -> c_int;
    pub fn pg_adr_time_recv(
        data: *const u8, len: c_int, typmod: i32, out: *mut i64, err: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_timetz_recv(
        data: *const u8, len: c_int, typmod: i32,
        out_time: *mut i64, out_zone: *mut i32, err: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_interval_recv(
        data: *const u8, len: c_int, typmod: i32, interval: *mut Interval, err: *mut c_int,
    ) -> c_int;

    pub fn pg_adr_date_send(date: i32, out: *mut u8) -> i32;
    pub fn pg_adr_time_send(time: i64, out: *mut u8) -> i32;
    pub fn pg_adr_timetz_send(t_time: i64, t_zone: i32, out: *mut u8) -> i32;
    pub fn pg_adr_interval_send(t: i64, d: i32, m: i32, out: *mut u8) -> i32;
}

#[cfg(kani)]
mod rem {
    use super::*;
    use datum::{Datum, NullableDatum};
    use proof_support::fcinfo::{fci, FcFn};
    use proof_support::{mcx_stubs, stubs};
    use types_error::{
        PgError, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INTERNAL_ERROR,
        ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
        ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE, ERROR,
    };
    use types_fmgr::LocalFcinfo;

    type PgResultDatum = Result<Datum, Box<PgError>>;

    const USECS_PER_DAY: i64 = 86_400_000_000;
    const TZDISP_LIMIT: i32 = 16 * 3600;
    const MIN_DATE: i32 = -2_451_545; // -POSTGRES_EPOCH_JDATE

    fn dummy_mcx() -> mcx::Mcx<'static> {
        const _: () = assert!(core::mem::size_of::<mcx::MemoryContext>() <= 1024);
        #[repr(align(16))]
        struct DummySlot([u8; 1024]);
        // SAFETY: never read or written through.
        unsafe impl Sync for DummySlot {}
        static SLOT: DummySlot = DummySlot([0u8; 1024]);
        // SAFETY: never dereferenced — every Allocator entry point is
        // stubbed and nothing in these wrappers reads context state.
        let ctx: &'static mcx::MemoryContext =
            unsafe { &*(SLOT.0.as_ptr() as *const mcx::MemoryContext) };
        ctx.mcx()
    }

    fn call_fc<const N: usize>(fc: FcFn<Box<PgError>>, args: [Datum; N]) -> (PgResultDatum, bool) {
        let mut f = fci(args);
        // SAFETY: the dummy context is 'static; outlives the call.
        unsafe { f.set_result_mcx(dummy_mcx()) };
        let r = fc(None, &mut f);
        let isnull = f.isnull;
        (r, isnull)
    }

    fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
        let mut img = [0u8; 12];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..].copy_from_slice(&zone.to_ne_bytes());
        img
    }

    fn iv_img(time: i64, day: i32, month: i32) -> [u8; 16] {
        let mut img = [0u8; 16];
        img[..8].copy_from_slice(&time.to_ne_bytes());
        img[8..12].copy_from_slice(&day.to_ne_bytes());
        img[12..].copy_from_slice(&month.to_ne_bytes());
        img
    }

    fn read_timetz(d: Datum) -> (i64, i32) {
        let p = d.as_usize() as *const u8;
        // SAFETY: 12-byte timetz image just written by the wrapper.
        unsafe { ((p as *const i64).read_unaligned(), (p.add(8) as *const i32).read_unaligned()) }
    }

    fn read_iv(d: Datum) -> (i64, i32, i32) {
        let p = d.as_usize() as *const u8;
        // SAFETY: 16-byte interval image just written by the wrapper.
        unsafe {
            (
                (p as *const i64).read_unaligned(),
                (p.add(8) as *const i32).read_unaligned(),
                (p.add(12) as *const i32).read_unaligned(),
            )
        }
    }

    /// Message-text-only stub (wave-7 precedent): izone's error builder
    /// formats the zone interval via interval_out purely for the message;
    /// text leaves the proof, the divider chain leaves symex.
    fn model_interval_out(_span: &Interval, _buf: &mut adt_timestamp::TsBuf) -> usize {
        0
    }

    /// aclcheck-precedent elog stubs (WARNING suppression, ICE #3 shield).
    fn model_level_interesting(elevel: types_error::ErrorLevel) -> bool {
        elevel >= ERROR
    }

    fn model_throw_error_data(edata: PgError) -> types_error::PgResult<()> {
        if edata.level >= ERROR {
            Err(Box::new(edata))
        } else {
            Ok(())
        }
    }

    // ---------- 1373 date_finite / 1390 interval_finite (full domain) -----

    #[kani::proof]
    fn eq_date_finite() {
        let date: i32 = kani::any();
        let c = unsafe { pg_adr_date_finite(date) };
        let (r, _) = call_fc(adt_date::builtins::fc_date_finite, [Datum::from_i32(date)]);
        match r {
            Ok(d) => assert!(d.as_bool() as c_int == c),
            Err(_) => panic!("date_finite is infallible"),
        }
    }

    #[kani::proof]
    fn eq_interval_finite() {
        let (t, d, m): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let c = unsafe { pg_adr_interval_finite(t, d, m) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_finite, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(dd) => assert!(dd.as_bool() as c_int == c),
            Err(_) => panic!("interval_finite is infallible"),
        }
    }

    // ---------- 1140 date_mi (full i32 x i32 + Err 22008 parity) ----------

    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_date_mi() {
        let (d1, d2): (i32, i32) = (kani::any(), kani::any());
        let mut c_out: i32 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_date_mi(d1, d2, &mut c_out, &mut c_err) };
        let (r, _) = call_fc(adt_date::builtins::fc_date_mi, [
            Datum::from_i32(d1),
            Datum::from_i32(d2),
        ]);
        match r {
            Ok(d) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(d.as_i32() == c_out);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    // ---------- 2046 timetz_time / 1690 time_mi_time / 1370 time_interval -

    #[kani::proof]
    fn eq_timetz_time() {
        let (t, z): (i64, i32) = (kani::any(), kani::any());
        let c = unsafe { pg_adr_timetz_time(t, z) };
        let img = timetz_img(t, z);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_time, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(d) => assert!(d.as_i64() == c),
            Err(_) => panic!("timetz_time is infallible"),
        }
    }

    /// time contract fence [0, USECS_PER_DAY] both args (the subtraction is
    /// C -fwrapv == Rust release-wrap outside it; native differential owns
    /// the wrap region).
    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_time_mi_time() {
        let (t1, t2): (i64, i64) = (kani::any(), kani::any());
        kani::assume((0..=USECS_PER_DAY).contains(&t1));
        kani::assume((0..=USECS_PER_DAY).contains(&t2));
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        unsafe { pg_adr_time_mi_time(t1, t2, &mut c_res) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_mi_time, [
            Datum::from_i64(t1),
            Datum::from_i64(t2),
        ]);
        match r {
            Ok(d) => {
                let (t, dd, m) = read_iv(d);
                assert!(t == c_res.time && dd == c_res.day && m == c_res.month);
            }
            Err(_) => panic!("time_mi_time is infallible"),
        }
    }

    #[kani::proof]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_time_interval() {
        let time: i64 = kani::any();
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        unsafe { pg_adr_time_interval(time, &mut c_res) };
        let (r, _) = call_fc(adt_date::builtins::fc_time_interval, [Datum::from_i64(time)]);
        match r {
            Ok(d) => {
                let (t, dd, m) = read_iv(d);
                assert!(t == c_res.time && dd == c_res.day && m == c_res.month);
            }
            Err(_) => panic!("time_interval is infallible"),
        }
    }

    // ---------- 1419 interval_time: honest screen + spots ----------

    /// Full-domain screen of the loop-free `span.time % USECS_PER_DAY` face
    /// (band-immune class: if this walls, do NOT case-split — the Err plane,
    /// spots and the native differential are the standing coverage).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_interval_time_screen() {
        let (t, d, m): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_time(t, d, m, &mut c_out, &mut c_err) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_time, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(dd) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(dd.as_i64() == c_out);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    /// Err plane full-symbolic (not_finite lattice; no divider reached).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_interval_time_err_plane() {
        let nobegin: bool = kani::any();
        let (t, d, m) = if nobegin {
            (i64::MIN, i32::MIN, i32::MIN)
        } else {
            (i64::MAX, i32::MAX, i32::MAX)
        };
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_time(t, d, m, &mut c_out, &mut c_err) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_time, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(_) => panic!("infinite interval must error"),
            Err(e) => {
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                core::mem::forget(e);
            }
        }
    }

    /// Concrete spot grid for the % face (one symbolic index).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn spot_interval_time() {
        const T: &[(i64, i32, i32)] = &[
            (0, 0, 0),
            (86_399_999_999, 0, 0),
            (86_400_000_000, 0, 0),
            (86_400_000_001, 5, 7),
            (-1, 0, 0),
            (-86_400_000_000, 0, 0),
            (-86_400_000_001, 0, 0),
            (-7_200_000_000, 3, 1),
            (i64::MIN, 0, 0),
            (i64::MAX, 0, 0),
            (i64::MIN + 1, i32::MIN, i32::MIN),
            (123_456_789_012_345, -9, -9),
        ];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        let (t, d, m) = T[idx];
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_time(t, d, m, &mut c_out, &mut c_err) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_time, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(dd) => {
                assert!(c_err == 0);
                assert!(dd.as_i64() == c_out);
            }
            Err(e) => {
                assert!(c_err == 1);
                core::mem::forget(e);
            }
        }
    }

    // ---------- hash rows (full domains) ----------

    #[kani::proof]
    fn eq_hashdate() {
        let date: i32 = kani::any();
        let c = unsafe { pg_adr_hashdate(date) };
        let r = proof_support::call1_ok(adt_date::builtins::fc_hashdate, date);
        assert!(r.as_i32() == c as i32);
    }

    #[kani::proof]
    fn eq_hashdate_extended() {
        let date: i32 = kani::any();
        let seed: i64 = kani::any();
        let c = unsafe { pg_adr_hashdate_extended(date, seed as u64) };
        let r = proof_support::call2_ok(adt_date::builtins::fc_hashdateextended, date, seed);
        assert!(r.as_i64() == c as i64);
    }

    #[kani::proof]
    fn eq_time_hash() {
        let t: i64 = kani::any();
        let c = unsafe { pg_adr_time_hash(t) };
        let r = proof_support::call1_ok(adt_date::builtins::fc_time_hash, t);
        assert!(r.as_i32() == c as i32);
    }

    #[kani::proof]
    fn eq_time_hash_extended() {
        let t: i64 = kani::any();
        let seed: i64 = kani::any();
        let c = unsafe { pg_adr_time_hash_extended(t, seed as u64) };
        let r = proof_support::call2_ok(adt_date::builtins::fc_time_hash_extended, t, seed);
        assert!(r.as_i64() == c as i64);
    }

    #[kani::proof]
    fn eq_timetz_hash() {
        let (t, z): (i64, i32) = (kani::any(), kani::any());
        let c = unsafe { pg_adr_timetz_hash(t, z) };
        let img = timetz_img(t, z);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_hash, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(d) => assert!(d.as_i32() == c as i32),
            Err(_) => panic!("timetz_hash is infallible"),
        }
    }

    #[kani::proof]
    fn eq_timetz_hash_extended() {
        let (t, z): (i64, i32) = (kani::any(), kani::any());
        let seed: i64 = kani::any();
        let c = unsafe { pg_adr_timetz_hash_extended(t, z, seed as u64) };
        let img = timetz_img(t, z);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_hash_extended, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i64(seed),
        ]);
        match r {
            Ok(d) => assert!(d.as_i64() == c as i64),
            Err(_) => panic!("timetz_hash_extended is infallible"),
        }
    }

    /// interval_hash m==0 d==0 LITERAL plane (interval-cmp precedent: the
    /// i128 two-contributor multiply is pruned by literal zeros).
    #[kani::proof]
    fn eq_interval_hash_m0d0() {
        let t: i64 = kani::any();
        let c = unsafe { pg_adr_interval_hash(t, 0, 0) };
        let img = iv_img(t, 0, 0);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_hash, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(d) => assert!(d.as_i32() == c as i32),
            Err(_) => panic!("interval_hash is infallible"),
        }
    }

    /// interval_hash |d|,|m| <= 1000 band, full time (interval-cmp band).
    #[kani::proof]
    fn eq_interval_hash_band() {
        let t: i64 = kani::any();
        let (d, m): (i32, i32) = (kani::any(), kani::any());
        kani::assume((-1000..=1000).contains(&d));
        kani::assume((-1000..=1000).contains(&m));
        let c = unsafe { pg_adr_interval_hash(t, d, m) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_hash, [
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        match r {
            Ok(dd) => assert!(dd.as_i32() == c as i32),
            Err(_) => panic!("interval_hash is infallible"),
        }
    }

    /// extended sibling on the m0d0 plane (same cmp_value composition).
    #[kani::proof]
    fn eq_interval_hash_ext_m0d0() {
        let t: i64 = kani::any();
        let seed: i64 = kani::any();
        let c = unsafe { pg_adr_interval_hash_extended(t, 0, 0, seed as u64) };
        let img = iv_img(t, 0, 0);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_hash_extended, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i64(seed),
        ]);
        match r {
            Ok(d) => assert!(d.as_i64() == c as i64),
            Err(_) => panic!("interval_hash_extended is infallible"),
        }
    }

    // ---------- 1308 overlaps_time / 1271 overlaps_timetz ----------

    /// full 4xi64 x null cube through the real fcinfo null protocol
    /// (wave-7 overlaps_timestamp precedent).
    #[kani::proof]
    fn eq_overlaps_time() {
        let (ts1, te1, ts2, te2): (i64, i64, i64, i64) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let (n1, n2, n3, n4): (bool, bool, bool, bool) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let mut cres: c_int = -1;
        let cnull = unsafe {
            pg_adr_overlaps_time(
                ts1, n1 as c_int, te1, n2 as c_int, ts2, n3 as c_int, te2, n4 as c_int, &mut cres,
            )
        };
        let mut f = LocalFcinfo::<4>::new(0);
        f.args[0] = NullableDatum { value: Datum::from_i64(ts1), isnull: n1 };
        f.args[1] = NullableDatum { value: Datum::from_i64(te1), isnull: n2 };
        f.args[2] = NullableDatum { value: Datum::from_i64(ts2), isnull: n3 };
        f.args[3] = NullableDatum { value: Datum::from_i64(te2), isnull: n4 };
        let d = match adt_date::builtins::fc_overlaps_time(None, &mut f) {
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

    /// Negative control (MUST FAIL, default solver): C sees interval 1's
    /// endpoints swapped relative to Rust.
    #[kani::proof]
    fn control_overlaps_time_swap_skew() {
        let (ts1, te1, ts2, te2): (i64, i64, i64, i64) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let mut cres: c_int = -1;
        let cnull = unsafe {
            pg_adr_overlaps_time(te1, 0, ts1, 0, ts2, 0, te2, 1, &mut cres)
        };
        let mut f = LocalFcinfo::<4>::new(0);
        f.args[0] = NullableDatum { value: Datum::from_i64(ts1), isnull: false };
        f.args[1] = NullableDatum { value: Datum::from_i64(te1), isnull: true };
        f.args[2] = NullableDatum { value: Datum::from_i64(ts2), isnull: false };
        f.args[3] = NullableDatum { value: Datum::from_i64(te2), isnull: true };
        let d = match adt_date::builtins::fc_overlaps_time(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("overlaps errored"),
        };
        assert!(f.isnull as c_int == cnull);
        if cnull == 0 {
            assert!(d.as_bool() as c_int == cres);
        }
    }

    /// contract-fenced values (times in [0,USECS_PER_DAY], zones inside
    /// TZDISP_LIMIT — the timetz_cmp add/multiply is total there), full
    /// null cube.
    #[kani::proof]
    fn eq_overlaps_timetz() {
        let (t1t, e1t, t2t, e2t): (i64, i64, i64, i64) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let (t1z, e1z, t2z, e2z): (i32, i32, i32, i32) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        for t in [t1t, e1t, t2t, e2t] {
            kani::assume((0..=USECS_PER_DAY).contains(&t));
        }
        for z in [t1z, e1z, t2z, e2z] {
            kani::assume(z > -TZDISP_LIMIT && z < TZDISP_LIMIT);
        }
        let (n1, n2, n3, n4): (bool, bool, bool, bool) =
            (kani::any(), kani::any(), kani::any(), kani::any());
        let mut cres: c_int = -1;
        let cnull = unsafe {
            pg_adr_overlaps_timetz(
                t1t, t1z, n1 as c_int,
                e1t, e1z, n2 as c_int,
                t2t, t2z, n3 as c_int,
                e2t, e2z, n4 as c_int,
                &mut cres,
            )
        };
        let i1 = timetz_img(t1t, t1z);
        let i2 = timetz_img(e1t, e1z);
        let i3 = timetz_img(t2t, t2z);
        let i4 = timetz_img(e2t, e2z);
        let mut f = LocalFcinfo::<4>::new(0);
        f.args[0] = NullableDatum { value: Datum::from_usize(i1.as_ptr() as usize), isnull: n1 };
        f.args[1] = NullableDatum { value: Datum::from_usize(i2.as_ptr() as usize), isnull: n2 };
        f.args[2] = NullableDatum { value: Datum::from_usize(i3.as_ptr() as usize), isnull: n3 };
        f.args[3] = NullableDatum { value: Datum::from_usize(i4.as_ptr() as usize), isnull: n4 };
        let d = match adt_date::builtins::fc_overlaps_timetz(None, &mut f) {
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

    // ---------- conversion rows 2024 / 1272+2025 / 1297+1359 ----------

    macro_rules! check_ts_result {
        ($r:expr, $cerr:expr, $cval:expr) => {
            match $r {
                Ok(d) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!($cerr == 0);
                    assert!(d.as_i64() == $cval);
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

    /// date >= -POSTGRES_EPOCH_JDATE union NOBEGIN fence (see module doc);
    /// upper overflow arm (>= END julian) fully in-theorem.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_date_timestamp() {
        let date: i32 = kani::any();
        kani::assume(date == i32::MIN || date >= MIN_DATE);
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_date_timestamp(date, &mut c_out, &mut c_err) };
        let (r, _) = call_fc(adt_date::builtins::fc_date_timestamp, [Datum::from_i32(date)]);
        check_ts_result!(r, c_err, c_out);
    }

    /// date fence as above; time contract [0, USECS_PER_DAY].
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_datetime_timestamp() {
        let date: i32 = kani::any();
        let time: i64 = kani::any();
        kani::assume(date == i32::MIN || date >= MIN_DATE);
        kani::assume((0..=USECS_PER_DAY).contains(&time));
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_datetime_timestamp(date, time, &mut c_out, &mut c_err) };
        let (r, _) = call_fc(adt_date::builtins::fc_datetime_timestamp, [
            Datum::from_i32(date),
            Datum::from_i64(time),
        ]);
        check_ts_result!(r, c_err, c_out);
    }

    /// date + time + zone fences per module doc; pure arithmetic (no tz
    /// lookup — the zone comes from the timetz value itself).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_datetimetz_timestamptz() {
        let date: i32 = kani::any();
        let (t, z): (i64, i32) = (kani::any(), kani::any());
        kani::assume(date == i32::MIN || date >= MIN_DATE);
        kani::assume((0..=USECS_PER_DAY).contains(&t));
        kani::assume(z > -TZDISP_LIMIT && z < TZDISP_LIMIT);
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_datetimetz_timestamptz(date, t, z, &mut c_out, &mut c_err) };
        let img = timetz_img(t, z);
        let (r, _) = call_fc(adt_date::builtins::fc_datetimetz_timestamptz, [
            Datum::from_i32(date),
            Datum::from_usize(img.as_ptr() as usize),
        ]);
        check_ts_result!(r, c_err, c_out);
    }

    // ---------- 2909/2911 typmodin core + 2910/2912 typmodout core --------

    macro_rules! typmod_check {
        ($($h:ident: $istz:literal / $core_istz:expr;)*) => {$(
            /// core-level (array decode is fcinfo plumbing, out of these
            /// rows); full i32 typmod incl the 22023 arm and the clamp arm
            /// (WARNING out of proof both sides).
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
            #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
            fn $h() {
                let typmod: i32 = kani::any();
                let mut c_out: i32 = 0;
                let mut c_err: c_int = 0;
                unsafe { pg_adr_anytime_typmod_check($istz, typmod, &mut c_out, &mut c_err) };
                match adt_date::anytime_typmod_check($core_istz, typmod) {
                    Ok(v) => {
                        kani::cover!(true, "Ok arm reachable");
                        assert!(c_err == 0);
                        assert!(v == c_out);
                    }
                    Err(e) => {
                        kani::cover!(true, "Err arm reachable");
                        assert!(c_err == 2);
                        assert!(e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                    }
                }
            }
        )*};
    }

    typmod_check! {
        eq_timetypmodin_check: 0 / false;
        eq_timetztypmodin_check: 1 / true;
    }

    fn check_typmodout(istz: bool, typmod: i32) {
        // 32 bytes: max image is "(2147483647)" + " without time zone" + NUL
        let mut c_buf = [0u8; 32];
        let c_len = unsafe {
            pg_adr_anytime_typmodout(istz as c_int, typmod, c_buf.as_mut_ptr())
        };
        let mut r_buf = [0u8; 32];
        let suffix: &[u8] = if istz { b" with time zone" } else { b" without time zone" };
        let len = adt_timestamp::builtins::typmod_paren_suffix_out(typmod, suffix, &mut r_buf);
        assert!(len as c_int == c_len);
        let mut i = 0;
        while i < 32 {
            assert!(r_buf[i] == c_buf[i] || i > len);
            i += 1;
        }
    }

    /// typmod < 0 plane (no digit loop; suffix copy only).
    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_timetypmodout_neg() {
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        check_typmodout(false, typmod);
    }

    #[kani::proof]
    #[kani::unwind(34)]
    fn eq_timetztypmodout_neg() {
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        check_typmodout(true, typmod);
    }

    /// digit-emission bands (intout sloped law; catalog domain is 0..=6,
    /// carried entirely by d1).
    macro_rules! typmodout_band {
        ($($h:ident: $istz:literal, $lo:literal ..= $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(34)]
            fn $h() {
                let typmod: i32 = kani::any();
                kani::assume(($lo..=$hi).contains(&typmod));
                check_typmodout($istz, typmod);
            }
        )*};
    }

    typmodout_band! {
        eq_timetypmodout_d1: false, 0 ..= 9;
        eq_timetypmodout_d2: false, 10 ..= 99;
        eq_timetypmodout_d3: false, 100 ..= 999;
        eq_timetztypmodout_d1: true, 0 ..= 9;
    }

    /// wide-magnitude spots (one symbolic index).
    #[kani::proof]
    #[kani::unwind(34)]
    fn spot_typmodout() {
        const T: &[i32] = &[1000, 65535, 1_000_000, i32::MAX, 6, 0];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        check_typmodout(false, T[idx]);
        check_typmodout(true, T[idx]);
    }

    // ---------- in_range rows 4137 / 4138 / 4133 ----------

    fn sql_in_range(flag: c_int) -> types_error::SqlState {
        if flag == 3 {
            ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
        } else {
            ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
        }
    }

    macro_rules! check_bool_result {
        ($r:expr, $cerr:expr, $cres:expr) => {
            match $r {
                Ok(d) => {
                    kani::cover!(true, "Ok arm reachable");
                    assert!($cerr == 0);
                    assert!(d.as_bool() as c_int == $cres);
                }
                Err(e) => {
                    kani::cover!(true, "Err arm reachable");
                    assert!($cerr != 0 && $cerr != 99);
                    assert!(e.sqlstate == sql_in_range($cerr));
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
        };
    }

    /// val/base contract-fenced; offset.time fully symbolic (sign arm,
    /// checked-add saturate arm, both compare directions in-theorem).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_in_range_time_interval() {
        let (val, base): (i64, i64) = (kani::any(), kani::any());
        kani::assume((0..=USECS_PER_DAY).contains(&val));
        kani::assume((0..=USECS_PER_DAY).contains(&base));
        let (ot, od, om): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let (sub, less): (bool, bool) = (kani::any(), kani::any());
        let mut cres: c_int = -1;
        let mut cerr: c_int = 0;
        unsafe {
            pg_adr_in_range_time_interval(
                val, base, ot, od, om, sub as c_int, less as c_int, &mut cres, &mut cerr,
            )
        };
        let img = iv_img(ot, od, om);
        let r = proof_support::fcinfo::call(adt_date::builtins::fc_in_range_time_interval, [
            Datum::from_i64(val),
            Datum::from_i64(base),
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_bool(sub),
            Datum::from_bool(less),
        ]);
        check_bool_result!(r, cerr, cres);
    }

    /// timetz sibling; offset.time additionally fenced away from the
    /// sum+zone -fwrapv wrap corner (<= i64::MAX - 60e9; wrap region ->
    /// native differential).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_in_range_timetz_interval() {
        let (vt, bt): (i64, i64) = (kani::any(), kani::any());
        let (vz, bz): (i32, i32) = (kani::any(), kani::any());
        kani::assume((0..=USECS_PER_DAY).contains(&vt));
        kani::assume((0..=USECS_PER_DAY).contains(&bt));
        kani::assume(vz > -TZDISP_LIMIT && vz < TZDISP_LIMIT);
        kani::assume(bz > -TZDISP_LIMIT && bz < TZDISP_LIMIT);
        let (ot, od, om): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        // Fence BOTH wrap corners of sum.time +- zone*1e6 inside
        // timetz_cmp (add arm: base+ot near i64::MAX; sub arm: base-ot near
        // i64::MIN): the excluded ~1.5e11 sliver at the top of offset space
        // is C -fwrapv == Rust release-wrap parity (native differential).
        kani::assume(ot <= i64::MAX - 150_000_000_000);
        let (sub, less): (bool, bool) = (kani::any(), kani::any());
        let mut cres: c_int = -1;
        let mut cerr: c_int = 0;
        unsafe {
            pg_adr_in_range_timetz_interval(
                vt, vz, bt, bz, ot, od, om, sub as c_int, less as c_int, &mut cres, &mut cerr,
            )
        };
        let vimg = timetz_img(vt, vz);
        let bimg = timetz_img(bt, bz);
        let oimg = iv_img(ot, od, om);
        let r = proof_support::fcinfo::call(adt_date::builtins::fc_in_range_timetz_interval, [
            Datum::from_usize(vimg.as_ptr() as usize),
            Datum::from_usize(bimg.as_ptr() as usize),
            Datum::from_usize(oimg.as_ptr() as usize),
            Datum::from_bool(sub),
            Datum::from_bool(less),
        ]);
        check_bool_result!(r, cerr, cres);
    }

    /// date row: offset m==0 d==0 LITERAL plane (wave-7 in_range pattern);
    /// dates fenced per module doc; the julian arms are trap-fenced 99.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_in_range_date_interval_m0d0() {
        let (val, base): (i32, i32) = (kani::any(), kani::any());
        kani::assume(val == i32::MIN || val >= MIN_DATE);
        kani::assume(base == i32::MIN || base >= MIN_DATE);
        let ot: i64 = kani::any();
        let (sub, less): (bool, bool) = (kani::any(), kani::any());
        let mut cres: c_int = -1;
        let mut cerr: c_int = 0;
        let trap = unsafe {
            pg_adr_in_range_date_interval(
                val, base, ot, 0, 0, sub as c_int, less as c_int, &mut cres, &mut cerr,
            )
        };
        assert!(trap != 99, "julian plane violation");
        let img = iv_img(ot, 0, 0);
        let r = proof_support::fcinfo::call(adt_date::builtins::fc_in_range_date_interval, [
            Datum::from_i32(val),
            Datum::from_i32(base),
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_bool(sub),
            Datum::from_bool(less),
        ]);
        check_bool_result!(r, cerr, cres);
    }

    /// literal-NOEND offset plane (infinity shortcut + sentinel pl/mi).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_in_range_date_interval_noend() {
        let (val, base): (i32, i32) = (kani::any(), kani::any());
        kani::assume(val == i32::MIN || val >= MIN_DATE);
        kani::assume(base == i32::MIN || base >= MIN_DATE);
        let (sub, less): (bool, bool) = (kani::any(), kani::any());
        let mut cres: c_int = -1;
        let mut cerr: c_int = 0;
        let trap = unsafe {
            pg_adr_in_range_date_interval(
                val, base, i64::MAX, i32::MAX, i32::MAX,
                sub as c_int, less as c_int, &mut cres, &mut cerr,
            )
        };
        assert!(trap != 99, "julian plane violation");
        let img = iv_img(i64::MAX, i32::MAX, i32::MAX);
        let r = proof_support::fcinfo::call(adt_date::builtins::fc_in_range_date_interval, [
            Datum::from_i32(val),
            Datum::from_i32(base),
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_bool(sub),
            Datum::from_bool(less),
        ]);
        check_bool_result!(r, cerr, cres);
    }

    // ---------- 2038 timetz_izone ----------

    /// Err planes full-symbolic (finiteness + months/days), 22023 parity.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(adt_timestamp::interval::interval_out, model_interval_out)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    #[kani::unwind(5)]
    fn eq_timetz_izone_err_planes() {
        let (zt, zd, zm): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        // pin to the error planes: infinite zone, or months/days nonzero
        kani::assume(
            (zt == i64::MIN && zd == i32::MIN && zm == i32::MIN)
                || (zt == i64::MAX && zd == i32::MAX && zm == i32::MAX)
                || zd != 0
                || zm != 0,
        );
        let (t, z): (i64, i32) = (kani::any(), kani::any());
        let (mut c_t, mut c_z): (i64, i32) = (0, 0);
        let mut c_err: c_int = 0;
        unsafe { pg_adr_timetz_izone(zt, zd, zm, t, z, &mut c_t, &mut c_z, &mut c_err) };
        let zimg = iv_img(zt, zd, zm);
        let timg = timetz_img(t, z);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_izone, [
            Datum::from_usize(zimg.as_ptr() as usize),
            Datum::from_usize(timg.as_ptr() as usize),
        ]);
        match r {
            Ok(_) => panic!("error plane must error"),
            Err(e) => {
                assert!(c_err == 2);
                assert!(e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    /// Ok-arm literal zone cells x contract-fenced symbolic time (the
    /// /USECS_PER_SEC divider + rotate % are pinned by the literal zone;
    /// the rotate's dividend stays bounded by the fences). LADDER: if the
    /// residual % on the bounded dividend walls, spots + differential stand.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(adt_timestamp::interval::interval_out, model_interval_out)]
    #[kani::unwind(5)]
    fn eq_timetz_izone_cells() {
        const ZT: &[i64] = &[
            0,
            3_600_000_000,           // +01:00
            -19_800_000_000,         // -05:30
            57_599_000_000,          // just under +16:00
            -57_599_000_000,
            999_999,                 // sub-second zone (truncates to 0)
            -999_999,
            86_400_000_000,          // a full day as a zone interval
        ];
        let idx: usize = kani::any();
        kani::assume(idx < ZT.len());
        let zt = ZT[idx];
        let (t, z): (i64, i32) = (kani::any(), kani::any());
        kani::assume((0..USECS_PER_DAY).contains(&t));
        kani::assume(z > -TZDISP_LIMIT && z < TZDISP_LIMIT);
        let (mut c_t, mut c_z): (i64, i32) = (0, 0);
        let mut c_err: c_int = 0;
        unsafe { pg_adr_timetz_izone(zt, 0, 0, t, z, &mut c_t, &mut c_z, &mut c_err) };
        let zimg = iv_img(zt, 0, 0);
        let timg = timetz_img(t, z);
        let (r, _) = call_fc(adt_date::builtins::fc_timetz_izone, [
            Datum::from_usize(zimg.as_ptr() as usize),
            Datum::from_usize(timg.as_ptr() as usize),
        ]);
        match r {
            Ok(d) => {
                assert!(c_err == 0);
                let (rt, rz) = read_timetz(d);
                assert!(rt == c_t && rz == c_z);
            }
            Err(_) => panic!("izone Ok plane errored"),
        }
    }

    // ---------- 1200 interval_scale ----------

    /// typmod < 0 plane: identity both sides, interval fully symbolic.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn eq_interval_scale_typmod_neg() {
        let (t, d, m): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_scale(t, d, m, typmod, &mut c_res, &mut c_err) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_scale, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(typmod),
        ]);
        match r {
            Ok(dd) => {
                assert!(c_err == 0);
                let (rt, rd, rm) = read_iv(dd);
                assert!(rt == c_res.time && rd == c_res.day && rm == c_res.month);
            }
            Err(_) => panic!("typmod<0 plane is infallible"),
        }
    }

    /// concrete spot grid over the range/precision arms (the range
    /// truncation dividers are band-immune on symbolic time, so cells are
    /// fully concrete): every range family, rounding both signs, the
    /// precision-overflow arm, invalid precision, unrecognized range.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    fn spot_interval_scale() {
        // (time, day, month, typmod)
        const MASK_YEAR: i32 = 1 << 2;
        const MASK_MONTH: i32 = 1 << 1;
        const MASK_DAY: i32 = 1 << 3;
        const MASK_HOUR: i32 = 1 << 10;
        const MASK_MINUTE: i32 = 1 << 11;
        const MASK_SECOND: i32 = 1 << 12;
        const FULL_RANGE: i32 = 0x7FFF;
        const FULL_PREC: i32 = 0xFFFF;
        const fn tm(range: i32, prec: i32) -> i32 {
            (range << 16) | (prec & 0xFFFF)
        }
        const T: &[(i64, i32, i32, i32)] = &[
            (1_234_567, 5, 26, tm(MASK_YEAR, FULL_PREC)),
            (1_234_567, 5, -26, tm(MASK_YEAR, FULL_PREC)),
            (1_234_567, 5, 26, tm(MASK_MONTH, FULL_PREC)),
            (1_234_567, 5, 26, tm(MASK_YEAR | MASK_MONTH, FULL_PREC)),
            (1_234_567, 5, 26, tm(MASK_DAY, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_HOUR, FULL_PREC)),
            (-7_512_345_678, 5, 26, tm(MASK_HOUR, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_MINUTE, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_SECOND, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_DAY | MASK_HOUR, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_DAY | MASK_HOUR | MASK_MINUTE, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_DAY | MASK_HOUR | MASK_MINUTE | MASK_SECOND, 3)),
            (7_512_345_678, 5, 26, tm(MASK_HOUR | MASK_MINUTE, FULL_PREC)),
            (7_512_345_678, 5, 26, tm(MASK_HOUR | MASK_MINUTE | MASK_SECOND, 0)),
            (7_512_345_678, 5, 26, tm(MASK_MINUTE | MASK_SECOND, 5)),
            (1_500_000, 0, 0, tm(FULL_RANGE, 0)),
            (-1_500_000, 0, 0, tm(FULL_RANGE, 0)),
            (1_499_999, 0, 0, tm(FULL_RANGE, 0)),
            (i64::MAX - 3, 0, 0, tm(FULL_RANGE, 0)),       // offset add overflow -> 22008
            (i64::MIN + 3, 0, 0, tm(FULL_RANGE, 0)),       // offset sub overflow -> 22008
            (1_234_567, 0, 0, tm(FULL_RANGE, 7)),          // invalid precision -> 22023
            (1_234_567, 0, 0, tm(MASK_YEAR | MASK_DAY, FULL_PREC)), // unrecognized -> internal
            (0, 0, 0, tm(FULL_RANGE, FULL_PREC)),
            (i64::MAX, i32::MAX, i32::MAX, tm(MASK_YEAR, 0)), // NOEND: untouched
            (i64::MIN, i32::MIN, i32::MIN, tm(MASK_YEAR, 0)), // NOBEGIN: untouched
        ];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        let (t, d, m, typmod) = T[idx];
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_scale(t, d, m, typmod, &mut c_res, &mut c_err) };
        let img = iv_img(t, d, m);
        let (r, _) = call_fc(adt_date::builtins::fc_interval_scale, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(typmod),
        ]);
        match r {
            Ok(dd) => {
                assert!(c_err == 0);
                let (rt, rd, rm) = read_iv(dd);
                assert!(rt == c_res.time && rd == c_res.day && rm == c_res.month);
            }
            Err(e) => {
                assert!(c_err != 0);
                let expect = match c_err {
                    1 => ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
                    2 => ERRCODE_INVALID_PARAMETER_VALUE,
                    _ => ERRCODE_INTERNAL_ERROR,
                };
                assert!(e.sqlstate == expect);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
    }

    // ---------- recv rows (core-level over a real StringInfo; the
    // pointer-datum recv ABI wall does not apply — no datum round trip) ----

    macro_rules! recv_si {
        ($ctx:ident, $si:ident, $payload:expr) => {
            let $ctx = mcx::MemoryContext::new_bump("kani-adr-recv");
            let mut $si = match stringinfo::StringInfo::with_capacity_in($ctx.mcx(), 32) {
                Ok(s) => s,
                Err(e) => {
                    core::mem::forget(e);
                    panic!("stringinfo alloc failed")
                }
            };
            match $si.append_bytes(&$payload) {
                Ok(()) => {}
                Err(e) => {
                    core::mem::forget(e);
                    panic!("append failed")
                }
            }
        };
    }

    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_date_recv() {
        let payload: [u8; 4] = kani::any();
        let mut c_out: i32 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_date_recv(payload.as_ptr(), 4, &mut c_out, &mut c_err) };
        recv_si!(ctx, si, payload);
        match adt_date::date_recv(&mut si) {
            Ok(v) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(v == c_out);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// typmod = -1 plane (AdjustTimeForTypmod no-op; the typmod face is
    /// datetime-b's time_scale rows).
    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_time_recv_typmod_neg() {
        let payload: [u8; 8] = kani::any();
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_time_recv(payload.as_ptr(), 8, -1, &mut c_out, &mut c_err) };
        recv_si!(ctx, si, payload);
        match adt_date::time_recv(&mut si, -1) {
            Ok(v) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(v == c_out);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1);
                assert!(e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_timetz_recv_typmod_neg() {
        let payload: [u8; 12] = kani::any();
        let (mut c_t, mut c_z): (i64, i32) = (0, 0);
        let mut c_err: c_int = 0;
        unsafe { pg_adr_timetz_recv(payload.as_ptr(), 12, -1, &mut c_t, &mut c_z, &mut c_err) };
        recv_si!(ctx, si, payload);
        match adt_date::timetz_recv(&mut si, -1) {
            Ok(v) => {
                kani::cover!(true, "Ok arm reachable");
                assert!(c_err == 0);
                assert!(v.time == c_t && v.zone == c_z);
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable");
                assert!(c_err == 1 || c_err == 5);
                let expect = if c_err == 5 {
                    ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE
                } else {
                    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
                };
                assert!(e.sqlstate == expect);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    #[kani::proof]
    #[kani::unwind(22)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_interval_recv_typmod_neg() {
        let payload: [u8; 16] = kani::any();
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_recv(payload.as_ptr(), 16, -1, &mut c_res, &mut c_err) };
        recv_si!(ctx, si, payload);
        match adt_timestamp::interval::interval_recv(&mut si, -1) {
            Ok(v) => {
                assert!(c_err == 0);
                assert!(v.time == c_res.time && v.day == c_res.day && v.month == c_res.month);
            }
            Err(e) => {
                core::mem::forget(e);
                panic!("typmod<0 interval_recv is infallible")
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    // ---------- send rows (int-arith send recipe) ----------

    macro_rules! send_harness {
        ($($h:ident: $fc:ident, $args:tt, $c:expr, $len:literal, unwind $u:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                send_harness!(@body $fc, $args, $c, $len);
            }
        )*};
        (@body $fc:ident, [i32], $c:expr, $len:literal) => {
            let v: i32 = kani::any();
            let mut cbuf = [0u8; $len];
            let clen = unsafe { $c(v, cbuf.as_mut_ptr()) };
            let args = [Datum::from_i32(v)];
            send_harness!(@compare $fc, args, cbuf, clen, $len);
        };
        (@body $fc:ident, [i64], $c:expr, $len:literal) => {
            let v: i64 = kani::any();
            let mut cbuf = [0u8; $len];
            let clen = unsafe { $c(v, cbuf.as_mut_ptr()) };
            let args = [Datum::from_i64(v)];
            send_harness!(@compare $fc, args, cbuf, clen, $len);
        };
        (@body $fc:ident, [timetz], $c:expr, $len:literal) => {
            let (t, z): (i64, i32) = (kani::any(), kani::any());
            let mut cbuf = [0u8; $len];
            let clen = unsafe { $c(t, z, cbuf.as_mut_ptr()) };
            let img = timetz_img(t, z);
            let args = [Datum::from_usize(img.as_ptr() as usize)];
            send_harness!(@compare $fc, args, cbuf, clen, $len);
        };
        (@body $fc:ident, [interval], $c:expr, $len:literal) => {
            let (t, d, m): (i64, i32, i32) = (kani::any(), kani::any(), kani::any());
            let mut cbuf = [0u8; $len];
            let clen = unsafe { $c(t, d, m, cbuf.as_mut_ptr()) };
            let img = iv_img(t, d, m);
            let args = [Datum::from_usize(img.as_ptr() as usize)];
            send_harness!(@compare $fc, args, cbuf, clen, $len);
        };
        (@compare $fc:ident, $args:expr, $cbuf:expr, $clen:expr, $len:literal) => {
            let ctx = mcx::MemoryContext::new_bump("kani-adr-send");
            let mut f = fci($args);
            // SAFETY: ctx outlives the call (forgotten, never freed).
            unsafe { f.set_result_mcx(ctx.mcx()) };
            let d = match adt_date::builtins::$fc(None, &mut f) {
                Ok(d) => d,
                Err(e) => {
                    core::mem::forget(e);
                    panic!("send errored")
                }
            };
            // SAFETY: varlena_result leaks the image; datum points at it.
            let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, $len) };
            assert!($clen == $len);
            let mut i = 0;
            while i < $len {
                assert!(img[i] == $cbuf[i]);
                i += 1;
            }
            core::mem::forget(ctx);
        };
    }

    send_harness! {
        eq_date_send: fc_date_send, [i32], pg_adr_date_send, 8, unwind 10;
        eq_time_send: fc_time_send, [i64], pg_adr_time_send, 12, unwind 14;
        eq_timetz_send: fc_timetz_send, [timetz], pg_adr_timetz_send, 16, unwind 18;
        eq_interval_send: fc_interval_send, [interval], pg_adr_interval_send, 20, unwind 22;
    }

    /// Negative control (MUST FAIL, default solver): C image built from a
    /// skewed date must diverge from the shipped date_send image
    /// (timetz-skew variant memory-walled before emitting a counterexample
    /// — a FAILED-less wall is not a witness, RVR law; the 8-byte sibling
    /// exercises the identical send pipeline).
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_date_send_skew() {
        let v: i32 = kani::any();
        kani::assume(v < i32::MAX);
        let mut cbuf = [0u8; 8];
        let clen = unsafe { pg_adr_date_send(v.wrapping_add(1), cbuf.as_mut_ptr()) };
        let ctx = mcx::MemoryContext::new_bump("kani-adr-send");
        let mut f = fci([Datum::from_i32(v)]);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        let d = match adt_date::builtins::fc_date_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("send errored")
            }
        };
        // SAFETY: as eq_date_send.
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 8) };
        assert!(clen == 8);
        let mut i = 0;
        while i < 8 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }
}
