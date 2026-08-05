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

    /// Kani stub for `adt_numeric::var::word_buf_take` — the shipped
    /// pool-miss arm (`pop()` on an empty pool -> `unwrap_or_default()`).
    /// Buffer recycling leaves the proof (numeric-probe recipe); see the
    /// timetz_part_cell macro comment for the Linux pthread_key_create
    /// status-6 mechanism this cuts.
    fn stub_word_buf_take() -> Vec<u16> {
        Vec::new()
    }

    /// Kani stub for `adt_numeric::var::word_buf_put` — the shipped
    /// `capacity() == 0` early-return arm (recycling out of proof).
    fn stub_word_buf_put(_v: Vec<u16>) {}

    /// Kani stub for `adt_numeric::var::digit_buf_heap_realloc`: the tz
    /// cells never feasibly construct a numeric (retnumeric=false, units
    /// literal), so reaching the digit heap arm is a harness defect —
    /// panic loudly (never a silent fence).
    fn stub_digit_buf_heap_realloc(_heap: &mut Vec<i16>, _n: usize) {
        panic!("DigitBuf heap arm reached in a tz-only timetz_part cell");
    }

    /// Kani stub for `adt_numeric::var::digit_buf_put` — drop-time pool
    /// return (recycling out of proof; numeric-probe precedent).
    fn stub_digit_buf_put(v: Vec<i16>) {
        core::mem::forget(v);
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
    /// unwind 66 (w2-timestamp repair 2026-07-30): without a bound the
    /// emit_str_paren_int do-loop unwinds unboundedly (symex hang at
    /// iteration 14000+); 66 = 64-byte image memcmp + 1, same binding
    /// bound as the prec_d* bands.
    #[kani::proof]
    #[kani::unwind(66)]
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
            // Numeric TLS-pool stub quartet (fleet 33d7d09d31 status-6
            // repair; numeric-probe recipe): part_result's statically-
            // present Numeric arms pull adt_numeric's NumericVar/
            // NumericImage machinery into the call graph, and its
            // DIGIT_POOL/WORD_POOL `thread_local!`s hold Drop-carrying
            // Vecs — the only Drop-carrying TLS these harnesses reach
            // (goto call graph: the sole callers of std's TLS-destructor
            // `register` are the two pool Storage initializers). On the
            // LINUX toolchain that registration path converts a call to
            // CBMC's builtin pthread_key_create, whose destructor
            // parameter type-mismatches Kani's declaration (struct_tag vs
            // pointer) -> CBMC status 6 -> reported "VERIFICATION FAILED"
            // with no property counterexample. macOS std registers TLS
            // destructors via _tlv_atexit instead, so laptop runs can't
            // reproduce — which is how the 2026-07-29 env/OnceLock repair
            // was banked green here while this TLS remained. The stubs
            // are the shipped pool-miss / no-recycle arms (numeric-probe
            // precedent): buffer recycling leaves the proof, values are
            // bit-identical. The tz cells never feasibly build a numeric
            // (retnumeric=false, units literal), so the realloc stub
            // panics loudly if ever reached.
            #[kani::stub(adt_numeric::var::word_buf_take, stub_word_buf_take)]
            #[kani::stub(adt_numeric::var::word_buf_put, stub_word_buf_put)]
            #[kani::stub(adt_numeric::var::digit_buf_heap_realloc, stub_digit_buf_heap_realloc)]
            #[kani::stub(adt_numeric::var::digit_buf_put, stub_digit_buf_put)]
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

    // wave-2: 2071 date_pl_interval / 2072 date_mi_interval
    pub fn pg_adr_date_pl_interval(
        date: i32, it: i64, id: i32, im: i32, out: *mut i64, err: *mut c_int,
    ) -> c_int;
    pub fn pg_adr_date_mi_interval(
        date: i32, it: i64, id: i32, im: i32, out: *mut i64, err: *mut c_int,
    ) -> c_int;
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

    // ==== w2-timestamp lane (2026-07-30): rows 2905-2908 ================
    // 2905/2907 core anytimestamp_typmod_check; 2906/2908 core
    // typmod_paren_suffix_out + " with(out) time zone" (same recipe as the
    // 2909-2912 lane-D rows above; C side vendored from timestamp.c).

    extern "C" {
        fn pg_ts_anytimestamp_typmod_check(
            istz: c_int,
            typmod: i32,
            out: *mut i32,
            err: *mut c_int,
        ) -> i32;
        fn pg_ts_anytimestamp_typmodout(istz: c_int, typmod: i32, res: *mut u8) -> c_int;
    }

    macro_rules! ts_typmod_check {
        ($($h:ident: $istz:literal / $core_istz:expr;)*) => {$(
            /// core-level (array decode is fcinfo plumbing, out of these
            /// rows); full i32 typmod incl the 22023 arm and the >6 clamp
            /// arm (WARNING out of proof both sides).
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
            #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
            fn $h() {
                let typmod: i32 = kani::any();
                let mut c_out: i32 = 0;
                let mut c_err: c_int = 0;
                unsafe { pg_ts_anytimestamp_typmod_check($istz, typmod, &mut c_out, &mut c_err) };
                match adt_timestamp::anytimestamp_typmod_check($core_istz, typmod) {
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

    ts_typmod_check! {
        eq_tstypmodin_check: 0 / false;
        eq_tstztypmodin_check: 1 / true;
    }

    fn check_ts_typmodout(istz: bool, typmod: i32) {
        // 40 bytes: max image is "(2147483647)" + " without time zone" + NUL
        let mut c_buf = [0u8; 40];
        let c_len =
            unsafe { pg_ts_anytimestamp_typmodout(istz as c_int, typmod, c_buf.as_mut_ptr()) };
        let mut r_buf = [0u8; 40];
        let suffix: &[u8] = if istz { b" with time zone" } else { b" without time zone" };
        let len = adt_timestamp::builtins::typmod_paren_suffix_out(typmod, suffix, &mut r_buf);
        assert!(len as c_int == c_len);
        let mut i = 0;
        while i < 40 {
            // i == len: C NUL vs Rust zero-init — both 0.
            assert!(r_buf[i] == c_buf[i] || i > len);
            i += 1;
        }
    }

    /// typmod < 0 plane (no digit loop; suffix copy only).
    #[kani::proof]
    #[kani::unwind(42)]
    fn eq_tstypmodout_neg() {
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        check_ts_typmodout(false, typmod);
    }

    #[kani::proof]
    #[kani::unwind(42)]
    fn eq_tstztypmodout_neg() {
        let typmod: i32 = kani::any::<i32>() | i32::MIN;
        check_ts_typmodout(true, typmod);
    }

    /// digit-emission bands (intout sloped law; catalog domain is 0..=6,
    /// carried entirely by d1).
    macro_rules! ts_typmodout_band {
        ($($h:ident: $istz:literal, $lo:literal ..= $hi:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(42)]
            fn $h() {
                let typmod: i32 = kani::any();
                kani::assume(($lo..=$hi).contains(&typmod));
                check_ts_typmodout($istz, typmod);
            }
        )*};
    }

    ts_typmodout_band! {
        eq_tstypmodout_d1: false, 0 ..= 9;
        eq_tstypmodout_d2: false, 10 ..= 99;
        eq_tstypmodout_d3: false, 100 ..= 999;
        eq_tstztypmodout_d1: true, 0 ..= 9;
    }

    /// wide-magnitude spots (one symbolic index), both suffixes.
    #[kani::proof]
    #[kani::unwind(42)]
    fn spot_ts_typmodout() {
        const T: &[i32] = &[1000, 65535, 1_000_000, i32::MAX, 6, 0];
        let idx: usize = kani::any();
        kani::assume(idx < T.len());
        check_ts_typmodout(false, T[idx]);
        check_ts_typmodout(true, T[idx]);
    }

    // ==== w2-timestamp lane (2026-07-30): row 1158 float8_timestamptz ====
    // Planes (nonfinite lattice, range reject) are pure float compares =
    // fast class; the in-range value arm multiplies a full-symbolic f64 by
    // USECS_PER_SEC (53-bit constant multiply = wall class, TRIAGE float
    // law) -> concrete spot grid + honest full screen (fleet-bound).

    extern "C" {
        fn pg_ts_float8_timestamptz(seconds: f64, out: *mut i64, err: *mut c_int) -> c_int;
    }

    // SECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)
    const F8TSTZ_LO: f64 = -210_866_803_200.0;
    // SECS_PER_DAY * (TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE)
    const F8TSTZ_HI: f64 = 9_224_318_016_000.0;

    /// Message-text-only stub (fmt_g6 feeds the out-of-range message; its
    /// string munging walls symex; text is out of proof).
    fn model_fmt_g6(_v: f64) -> String {
        String::new()
    }

    fn check_f8tstz(seconds: f64) {
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_ts_float8_timestamptz(seconds, &mut c_out, &mut c_err) };
        match adt_timestamp::float8_timestamptz(seconds) {
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
    }

    /// NaN (22008) + ±Inf (NOBEGIN/NOEND) plane — full nonfinite domain.
    #[kani::proof]
    #[kani::unwind(40)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_timestamp::fmt_g6, model_fmt_g6)]
    fn eq_f8tstz_nonfinite() {
        let s: f64 = kani::any();
        kani::assume(!s.is_finite());
        check_f8tstz(s);
    }

    /// Finite out-of-range reject plane (pure compares; both sides reject
    /// BEFORE the 53-bit multiply).
    #[kani::proof]
    #[kani::unwind(40)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_timestamp::fmt_g6, model_fmt_g6)]
    fn eq_f8tstz_range_reject() {
        let s: f64 = kani::any();
        kani::assume(s.is_finite());
        kani::assume(s < F8TSTZ_LO || s >= F8TSTZ_HI);
        check_f8tstz(s);
    }

    /// Value-arm spots (one symbolic index into a concrete grid: zero,
    /// subsecond ties, epoch, both range edges, near-END recheck band).
    #[kani::proof]
    #[kani::unwind(40)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_timestamp::fmt_g6, model_fmt_g6)]
    fn spot_f8tstz_value() {
        const S: &[f64] = &[
            0.0,
            -0.5,
            1.5e-6,          // sub-usec tie (rint ties-to-even)
            2.5e-6,
            946_684_800.0,   // PG epoch in unix seconds
            F8TSTZ_LO,       // exact lower edge (valid)
            F8TSTZ_LO + 0.25,
            F8TSTZ_HI - 1.0,
            F8TSTZ_HI - 0.002, // near-END: exercises the rint recheck band
            -1.0,
            86_400.000001,
        ];
        let idx: usize = kani::any();
        kani::assume(idx < S.len());
        check_f8tstz(S[idx]);
    }

    /// Honest full screen of the in-range value arm (53-bit constant
    /// multiply + rint over full-symbolic f64). EXPECTED WALL locally —
    /// authored for the fleet high-memory tier; if it walls there too the
    /// planes + spots + native differential stand.
    #[kani::proof]
    #[kani::unwind(40)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(adt_timestamp::fmt_g6, model_fmt_g6)]
    fn eq_f8tstz_value_screen() {
        let s: f64 = kani::any();
        kani::assume(s.is_finite());
        kani::assume(s >= F8TSTZ_LO && s < F8TSTZ_HI);
        check_f8tstz(s);
    }

    // ---------- wave-2: 2071 date_pl_interval / 2072 date_mi_interval ----
    // Composition rows: date2timestamp (upper-julian overflow arm
    // in-theorem) + timestamp_pl/mi_interval. Planes: m0d0 (month==day==0
    // LITERAL, time fully symbolic — the julian tm-walk arms are
    // trap-fenced 99 in the C and unreachable on this plane) + the two
    // infinite-span literal sentinels (infinity-minus-infinity error arm +
    // passthrough). Dates contract-fenced per the lane-D module doc.

    macro_rules! eq_date_iv {
        ($($h:ident: $fc:path, $cfn:ident, $it:expr, $id:expr, $im:expr;)*) => {$(
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let date: i32 = kani::any();
                kani::assume(date == i32::MIN || date >= MIN_DATE);
                let it: i64 = $it;
                let mut c_out: i64 = 0;
                let mut c_err: c_int = 0;
                let trap = unsafe {
                    $cfn(date, it, $id, $im, &mut c_out, &mut c_err)
                };
                assert!(trap != 99, "julian plane violation");
                let img = iv_img(it, $id, $im);
                let r = proof_support::fcinfo::call($fc, [
                    Datum::from_i32(date),
                    Datum::from_usize(img.as_ptr() as usize),
                ]);
                check_ts_result!(r, c_err, c_out);
            }
        )*};
    }

    eq_date_iv! {
        eq_date_pl_interval_m0d0:    adt_date::builtins::fc_date_pl_interval, pg_adr_date_pl_interval, kani::any(), 0, 0;
        eq_date_mi_interval_m0d0:    adt_date::builtins::fc_date_mi_interval, pg_adr_date_mi_interval, kani::any(), 0, 0;
        eq_date_pl_interval_nobegin: adt_date::builtins::fc_date_pl_interval, pg_adr_date_pl_interval, i64::MIN, i32::MIN, i32::MIN;
        eq_date_mi_interval_nobegin: adt_date::builtins::fc_date_mi_interval, pg_adr_date_mi_interval, i64::MIN, i32::MIN, i32::MIN;
        eq_date_pl_interval_noend:   adt_date::builtins::fc_date_pl_interval, pg_adr_date_pl_interval, i64::MAX, i32::MAX, i32::MAX;
        eq_date_mi_interval_noend:   adt_date::builtins::fc_date_mi_interval, pg_adr_date_mi_interval, i64::MAX, i32::MAX, i32::MAX;
    }
}

// ============================================================================
// hlp — p1-lanel adt_datetime pure-helper rows (2026-07-31).
//
// Dual-exec vs the verbatim C appended to c/pg_datetime_b.c (provenance in
// that file's p1-lanel section header). No allocator, no fmgr, no stubs:
// every function here is scalar/table arithmetic, so harnesses compare raw
// values only.
//
// Domain notes (fences are LITERAL-MASKED, never assumes):
//   - eq_dtk_m / eq_interval_mask: t = raw & 31 — the full VALID shift
//     domain; t >= 32 or t < 0 is shift-overflow UB in C and a Rust panic,
//     out of both sides' contract (callers pass token constants 0..=37 only
//     through DTK_M(t<=31) sites).
//   - j2day/date2j/j2date/isoweek2j run on full i32 (C compiled -fwrapv;
//     the Rust bodies use wrapping ops — bit-level equivalence is exactly
//     the claim).
//   - LADDER harnesses (_full over divider chains) may wall per the
//     band-immune law; the spot grids + the datetime_io_diff fuzz target
//     are the standing coverage for the bulk domain.
// ============================================================================
#[cfg(kani)]
mod hlp {
    use adt_datetime::calendar::{
        date2isoweek, date2isoyear, date2isoyearday, date2j, isleap, isoweek2date, isoweek2j,
        isoweekdate2date, j2date, j2day,
    };
    use adt_datetime::consts::{
        DateTkn, DTK_M, INTERVAL_MASK, IS_VALID_JULIAN, MICROSECOND, MILLISECOND, SECOND,
    };
    use adt_datetime::decode::{
        datebsearch, dt2time, float_time_overflows, time_overflows, CheckDateTokenTable,
        CheckDateTokenTables, DecodeSpecial, DecodeUnits,
    };
    use adt_datetime::tables::{DATETKTBL, DELTATKTBL};
    use adt_datetime::Interval;
    use proof_support::stubs;
    use adt_date::{
        interval_time, time_mi_interval, time_pl_interval, timetz_mi_interval, timetz_pl_interval,
        TimeTzADT,
    };
    use std::os::raw::{c_char, c_int};

    extern "C" {
        fn pg_hlp_dtk_m(t: c_int) -> c_int;
        fn pg_hlp_interval_mask(b: c_int) -> c_int;
        fn pg_hlp_is_valid_julian(y: c_int, m: c_int, d: c_int) -> c_int;
        fn pg_hlp_isleap(y: c_int) -> c_int;
        fn pg_hlp_j2day(date: c_int) -> c_int;
        fn pg_hlp_j2date(jd: c_int, year: *mut c_int, month: *mut c_int, day: *mut c_int) -> c_int;
        fn pg_hlp_date2j(y: c_int, m: c_int, d: c_int) -> c_int;
        fn pg_hlp_dt2time(jd: i64, hour: *mut c_int, min: *mut c_int, sec: *mut c_int, fsec: *mut i32) -> c_int;
        fn pg_hlp_time_overflows(hour: c_int, min: c_int, sec: c_int, fsec: i32) -> c_int;
        fn pg_hlp_isoweek2j(year: c_int, week: c_int) -> c_int;
        fn pg_hlp_isoweek2date(woy: c_int, year: *mut c_int, mon: *mut c_int, mday: *mut c_int) -> c_int;
        fn pg_hlp_isoweekdate2date(
            isoweek: c_int,
            wday: c_int,
            year: *mut c_int,
            mon: *mut c_int,
            mday: *mut c_int,
        ) -> c_int;
        fn pg_hlp_date2isoweek(year: c_int, mon: c_int, mday: c_int) -> c_int;
        fn pg_hlp_date2isoyear(year: c_int, mon: c_int, mday: c_int) -> c_int;
        fn pg_hlp_date2isoyearday(year: c_int, mon: c_int, mday: c_int) -> c_int;
        fn pg_hlp_datebsearch_date(key: *const c_char, idx: *mut c_int) -> c_int;
        fn pg_hlp_datebsearch_delta(key: *const c_char, idx: *mut c_int) -> c_int;
        fn pg_hlp_decode_special(field: c_int, lowtoken: *const c_char, val: *mut c_int) -> c_int;
        fn pg_hlp_decode_units(field: c_int, lowtoken: *const c_char, val: *mut c_int) -> c_int;
        fn pg_hlp_check_date_token_tables() -> c_int;
        fn pg_hlp_check_date_token_table_one(which: c_int) -> c_int;
        fn pg_hlp_float_time_overflows(hour: c_int, min: c_int, sec: f64) -> c_int;
        fn pg_hlp_interval_time(
            sp_time: i64,
            sp_day: i32,
            sp_month: i32,
            out: *mut i64,
        ) -> c_int;
        fn pg_hlp_time_pl_interval(
            time: i64,
            sp_time: i64,
            sp_day: i32,
            sp_month: i32,
            out: *mut i64,
        ) -> c_int;
        fn pg_hlp_time_mi_interval(
            time: i64,
            sp_time: i64,
            sp_day: i32,
            sp_month: i32,
            out: *mut i64,
        ) -> c_int;
        fn pg_hlp_timetz_pl_interval(
            time: i64,
            zone: i32,
            sp_time: i64,
            sp_day: i32,
            sp_month: i32,
            out_time: *mut i64,
            out_zone: *mut i32,
        ) -> c_int;
        fn pg_hlp_timetz_mi_interval(
            time: i64,
            zone: i32,
            sp_time: i64,
            sp_day: i32,
            sp_month: i32,
            out_time: *mut i64,
            out_zone: *mut i32,
        ) -> c_int;
    }

    /// Full valid shift domain (see module header) + the composite masks.
    #[kani::proof]
    fn eq_dtk_m_shiftdom() {
        let raw: u8 = kani::any();
        let t: i32 = (raw & 31) as i32;
        assert!(DTK_M(t) == unsafe { pg_hlp_dtk_m(t) });
        // composite mask constants ride the same C macro
        let c_all_secs = unsafe {
            pg_hlp_dtk_m(SECOND) | pg_hlp_dtk_m(MILLISECOND) | pg_hlp_dtk_m(MICROSECOND)
        };
        assert!(adt_datetime::consts::DTK_ALL_SECS_M == c_all_secs);
    }

    /// Full valid shift domain.
    #[kani::proof]
    fn eq_interval_mask_shiftdom() {
        let raw: u8 = kani::any();
        let b: i32 = (raw & 31) as i32;
        assert!(INTERVAL_MASK(b) == unsafe { pg_hlp_interval_mask(b) });
    }

    /// Full-domain: pure compares, no dividers.
    #[kani::proof]
    fn eq_is_valid_julian_full() {
        let (y, m, d): (i32, i32, i32) = (kani::any(), kani::any(), kani::any());
        assert!(IS_VALID_JULIAN(y, m, d) == (unsafe { pg_hlp_is_valid_julian(y, m, d) } != 0));
    }

    /// Full-i32: %4/%100/%400 small-constant mods.
    #[kani::proof]
    fn eq_isleap_full() {
        let y: i32 = kani::any();
        assert!(isleap(y) == (unsafe { pg_hlp_isleap(y) } != 0));
    }

    /// Full-i32 incl. the INT_MAX wrap (+1 under -fwrapv) and negative fixup.
    #[kani::proof]
    fn eq_j2day_full() {
        let d: i32 = kani::any();
        assert!(j2day(d) == unsafe { pg_hlp_j2day(d) });
    }

    /// LADDER (honest full-domain screen over the /100 and /4 dividers).
    #[kani::proof]
    fn eq_date2j_full() {
        let (y, m, d): (i32, i32, i32) = (kani::any(), kani::any(), kani::any());
        assert!(date2j(y, m, d) == unsafe { pg_hlp_date2j(y, m, d) });
    }

    /// LADDER (unsigned /146097, /1461, %365/%366 chain).
    #[kani::proof]
    fn eq_j2date_full() {
        let jd: i32 = kani::any();
        let (mut ry, mut rm, mut rd) = (0i32, 0i32, 0i32);
        j2date(jd, &mut ry, &mut rm, &mut rd);
        let (mut cy, mut cm, mut cd) = (0i32, 0i32, 0i32);
        unsafe { pg_hlp_j2date(jd, &mut cy, &mut cm, &mut cd) };
        assert!(ry == cy && rm == cm && rd == cd);
    }

    /// Julian boundary spots (symbolic index over a concrete grid).
    #[kani::proof]
    fn eq_j2date_spots() {
        const G: &[i32] = &[
            0, 1, -1, 32044, -32045, 146096, 146097, 1461, 1460, 2451545, 2440588, 1721426,
            2361222, 2147483493, 2147483494, i32::MAX, i32::MIN, 60, 59,
        ];
        let idx: usize = kani::any();
        kani::assume(idx < G.len());
        let jd = G[idx];
        let (mut ry, mut rm, mut rd) = (0i32, 0i32, 0i32);
        j2date(jd, &mut ry, &mut rm, &mut rd);
        let (mut cy, mut cm, mut cd) = (0i32, 0i32, 0i32);
        unsafe { pg_hlp_j2date(jd, &mut cy, &mut cm, &mut cd) };
        assert!(ry == cy && rm == cm && rd == cd);
    }

    /// LADDER (64-bit /USECS_PER_HOUR//MINUTE//SEC divider chain).
    #[kani::proof]
    fn eq_dt2time_full() {
        let jd: i64 = kani::any();
        let (mut rh, mut rmin, mut rs, mut rf) = (0i32, 0i32, 0i32, 0i32);
        dt2time(jd, &mut rh, &mut rmin, &mut rs, &mut rf);
        let (mut ch, mut cmin, mut cs, mut cf) = (0i32, 0i32, 0i32, 0i32);
        unsafe { pg_hlp_dt2time(jd, &mut ch, &mut cmin, &mut cs, &mut cf) };
        assert!(rh == ch && rmin == cmin && rs == cs && rf == cf);
    }

    /// Divider-chain boundary spots.
    #[kani::proof]
    fn eq_dt2time_spots() {
        const G: &[i64] = &[
            0,
            1,
            -1,
            999_999,
            1_000_000,
            59_999_999,
            60_000_000,
            3_599_999_999,
            3_600_000_000,
            43_200_000_000,
            86_399_999_999,
            86_400_000_000,
            12_345_678_901_234,
            i64::MAX,
            i64::MIN,
        ];
        let idx: usize = kani::any();
        kani::assume(idx < G.len());
        let jd = G[idx];
        let (mut rh, mut rmin, mut rs, mut rf) = (0i32, 0i32, 0i32, 0i32);
        dt2time(jd, &mut rh, &mut rmin, &mut rs, &mut rf);
        let (mut ch, mut cmin, mut cs, mut cf) = (0i32, 0i32, 0i32, 0i32);
        unsafe { pg_hlp_dt2time(jd, &mut ch, &mut cmin, &mut cs, &mut cf) };
        assert!(rh == ch && rmin == cmin && rs == cs && rf == cf);
    }

    /// Full 4xi32 domain: individual range checks fence the i64 total.
    #[kani::proof]
    fn eq_time_overflows_full() {
        let (h, m, s, f): (i32, i32, i32, i32) = (kani::any(), kani::any(), kani::any(), kani::any());
        assert!(time_overflows(h, m, s, f) == (unsafe { pg_hlp_time_overflows(h, m, s, f) } != 0));
    }

    /// AD-band year (literal-masked, covers the Julian-valid window) x full
    /// symbolic week (the (week-1)*7 face wraps on both sides).
    #[kani::proof]
    fn eq_isoweek2j_ad_band() {
        let raw: u32 = kani::any();
        let year: i32 = 1 + (raw & 0x007F_FFFF) as i32;
        let week: i32 = kani::any();
        assert!(isoweek2j(year, week) == unsafe { pg_hlp_isoweek2j(year, week) });
    }

    /// BC/zero-band year x full symbolic week.
    #[kani::proof]
    fn eq_isoweek2j_bc_band() {
        let raw: u32 = kani::any();
        let year: i32 = -((raw & 0x007F_FFFF) as i32);
        let week: i32 = kani::any();
        assert!(isoweek2j(year, week) == unsafe { pg_hlp_isoweek2j(year, week) });
    }

    // ---- PANIC-FREEDOM ladders (salvage from the walled equality ladders) ----
    //
    // The five `_full`/`_band` equality ladders above wall in CBMC's
    // per-property refinement phase (symex is trivial at ~0.03s; the SAT
    // instance decides in ms, then per-property classification does not
    // terminate inside 600s on the fleet under cadical). Bisecting those
    // instances property-by-property showed every Kani-inserted
    // arithmetic-overflow / division / subtraction check passing INDIVIDUALLY
    // and only the cross-implementation equality assertion undecidable.
    //
    // These harnesses isolate exactly that decidable half: no C call and no
    // equality claim, so the ONLY properties are Kani's panic checks. The
    // resulting theorem is full-domain panic-freedom — precisely the property
    // class this lane found SIX real -fwrapv defects in (j2date, dt2time,
    // ValidateDate DOY, isoweek2j, date2j, display_year), each of which was a
    // ported-in `panic!` where C wraps. Value parity for the bulk domain stays
    // with the spot grids + the CGF targets; panic-freedom is now PROVED rather
    // than deferred to a wall.
    #[kani::proof]
    fn panicfree_date2j_full() {
        let (y, m, d): (i32, i32, i32) = (kani::any(), kani::any(), kani::any());
        std::hint::black_box(date2j(y, m, d));
    }

    #[kani::proof]
    fn panicfree_j2date_full() {
        let jd: i32 = kani::any();
        let (mut y, mut m, mut d) = (0i32, 0i32, 0i32);
        j2date(jd, &mut y, &mut m, &mut d);
        std::hint::black_box((y, m, d));
    }

    #[kani::proof]
    fn panicfree_dt2time_full() {
        let jd: i64 = kani::any();
        let (mut h, mut mi, mut s, mut f) = (0i32, 0i32, 0i32, 0i32);
        dt2time(jd, &mut h, &mut mi, &mut s, &mut f);
        std::hint::black_box((h, mi, s, f));
    }

    #[kani::proof]
    fn panicfree_isoweek2j_full() {
        let (year, week): (i32, i32) = (kani::any(), kani::any());
        std::hint::black_box(isoweek2j(year, week));
    }

    /// NEGATIVE CONTROL for the panic-freedom plane: a deliberately checked
    /// `week - 1` reproduces the exact -fwrapv defect datetime_engine_diff
    /// found in isoweek2j, so the plane demonstrably catches that defect class
    /// rather than passing vacuously.
    #[kani::proof]
    fn control_panicfree_isoweek2j_checked_sub() {
        let week: i32 = kani::any();
        // The pre-fix shape: checked subtraction panics at week == i32::MIN.
        std::hint::black_box((week - 1).wrapping_mul(7));
    }

    const ISO_GRID: &[(i32, i32, i32)] = &[
        (2005, 1, 1),   // ISO week 53 of 2004
        (2005, 1, 2),
        (2005, 1, 3),
        (2006, 1, 1),
        (2008, 12, 29), // ISO week 1 of 2009
        (2008, 12, 28),
        (2004, 12, 31),
        (2010, 1, 3),
        (2010, 1, 4),
        (2024, 2, 29),
        (2000, 2, 29),
        (1, 1, 1),
        (-4713, 11, 24),
        (5874897, 12, 31),
        (1981, 12, 31),
        (1982, 1, 1),
    ];

    /// ISO-week rollover spots + a masked mday face on two cells.
    #[kani::proof]
    fn eq_date2isoweek_spots() {
        let idx: usize = kani::any();
        kani::assume(idx < ISO_GRID.len());
        let (y, m, d) = ISO_GRID[idx];
        assert!(date2isoweek(y, m, d) == unsafe { pg_hlp_date2isoweek(y, m, d) });
    }

    #[kani::proof]
    fn eq_date2isoyear_spots() {
        let idx: usize = kani::any();
        kani::assume(idx < ISO_GRID.len());
        let (y, m, d) = ISO_GRID[idx];
        assert!(date2isoyear(y, m, d) == unsafe { pg_hlp_date2isoyear(y, m, d) });
    }

    #[kani::proof]
    fn eq_date2isoyearday_spots() {
        let idx: usize = kani::any();
        kani::assume(idx < ISO_GRID.len());
        let (y, m, d) = ISO_GRID[idx];
        assert!(date2isoyearday(y, m, d) == unsafe { pg_hlp_date2isoyearday(y, m, d) });
    }

    /// Masked-mday face: concrete (y, m) cells x mday in [-16, 47].
    #[kani::proof]
    fn eq_date2isoweek_mday_face() {
        let cell: bool = kani::any();
        let (y, m) = if cell { (2005, 1) } else { (2008, 12) };
        let raw: u8 = kani::any();
        let d: i32 = (raw & 63) as i32 - 16;
        assert!(date2isoweek(y, m, d) == unsafe { pg_hlp_date2isoweek(y, m, d) });
        assert!(date2isoyear(y, m, d) == unsafe { pg_hlp_date2isoyear(y, m, d) });
    }

    /// isoweek2date over (year, woy) spot cells incl. out-of-convention woy.
    #[kani::proof]
    fn eq_isoweek2date_spots() {
        const G: &[(i32, i32)] = &[
            (2004, 53),
            (2005, 1),
            (2005, 52),
            (2009, 1),
            (2009, 53),
            (2020, 10),
            (1, 1),
            (2024, 0),
            (2024, -5),
            (2024, 54),
            (-100, 2),
        ];
        let idx: usize = kani::any();
        kani::assume(idx < G.len());
        let (iso_year, woy) = G[idx];
        let (mut ry, mut rm, mut rd) = (iso_year, 0i32, 0i32);
        isoweek2date(woy, &mut ry, &mut rm, &mut rd);
        let (mut cy, mut cm, mut cd) = (iso_year, 0i32, 0i32);
        unsafe { pg_hlp_isoweek2date(woy, &mut cy, &mut cm, &mut cd) };
        assert!(ry == cy && rm == cm && rd == cd);
    }

    /// isoweekdate2date: concrete (year, week) cells x masked wday band
    /// [-4, 11] (covers the 1..=7 convention + both out-of-convention
    /// sides; FULL symbolic wday shifts j2date's whole divider chain
    /// symbolic and walls — LADDER attempt recorded 500s timeout, kissat).
    #[kani::proof]
    fn eq_isoweekdate2date_wday_band() {
        let cell: bool = kani::any();
        let (iso_year, isoweek) = if cell { (2005, 1) } else { (2009, 53) };
        let raw: u8 = kani::any();
        let wday: i32 = (raw & 15) as i32 - 4;
        let (mut ry, mut rm, mut rd) = (iso_year, 0i32, 0i32);
        isoweekdate2date(isoweek, wday, &mut ry, &mut rm, &mut rd);
        let (mut cy, mut cm, mut cd) = (iso_year, 0i32, 0i32);
        unsafe { pg_hlp_isoweekdate2date(isoweek, wday, &mut cy, &mut cm, &mut cd) };
        assert!(ry == cy && rm == cm && rd == cd);
    }

    /// WALL (recorded 2026-07-31): a fully/partially SYMBOLIC key makes
    /// datebsearch's data-dependent binary-search loop unboundable for
    /// CBMC — the unwinding assertion fails at unwind 16 AND 80 (> table
    /// size) because merged states lose the interval-shrinkage argument;
    /// SYMEX/unwinding phase, solver-irrelevant. Named remedy per TRIAGE =
    /// concrete-cell spots (below; every cell folds the search concrete)
    /// + the datetime_io_diff CGF target (DecodeSpecial/DecodeUnits sit on
    /// every parse path, C side sancov'd), which owns the bulk key domain.
    const DATE_KEYS: &[&[u8]] = &[
        b"+infinity\0",
        b"allballs\0\0",
        b"am\0\0\0\0\0\0\0\0",
        b"apr\0\0\0\0\0\0\0",
        b"april\0\0\0\0\0",
        b"aprila\0\0\0\0",
        b"bc\0\0\0\0\0\0\0\0",
        b"j\0\0\0\0\0\0\0\0\0",
        b"yesterday\0",
        b"zulu\0\0\0\0\0\0",
        b"z\0\0\0\0\0\0\0\0\0",
        b"\0\0\0\0\0\0\0\0\0\0",
        b"\xff\xfe\0\0\0\0\0\0\0\0",
        b"septembers",
    ];
    const DELTA_KEYS: &[&[u8]] = &[
        b"@\0\0\0\0\0\0\0\0\0",
        b"ago\0\0\0\0\0\0\0",
        b"c\0\0\0\0\0\0\0\0\0",
        b"centuries\0",
        b"hr\0\0\0\0\0\0\0\0",
        b"usecond\0\0\0",
        b"w\0\0\0\0\0\0\0\0\0",
        b"week\0\0\0\0\0\0",
        b"yr\0\0\0\0\0\0\0\0",
        b"yrs\0\0\0\0\0\0\0",
        b"xyz\0\0\0\0\0\0\0",
        b"\0\0\0\0\0\0\0\0\0\0",
    ];

    fn check_datebsearch_cell(
        key10: &[u8],
        table: &'static [DateTkn],
        c_side: unsafe extern "C" fn(*const c_char, *mut c_int) -> c_int,
    ) {
        let mut kbuf = [0u8; 11];
        kbuf[..10].copy_from_slice(&key10[..10]);
        let r = datebsearch(&kbuf, table);
        let mut c_idx: c_int = -1;
        let c_hit = unsafe { c_side(kbuf.as_ptr() as *const c_char, &mut c_idx) };
        match r {
            None => assert!(c_hit == 0),
            Some(tp) => {
                assert!(c_hit == 1);
                let r_idx = (tp as *const DateTkn as usize - table.as_ptr() as usize)
                    / core::mem::size_of::<DateTkn>();
                assert!(r_idx == c_idx as usize);
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(16)]
    fn eq_datebsearch_date_cells() {
        // Sequential CONCRETE calls, not a symbolic index over the grid: a
        // symbolic index merges the loop states and CBMC then cannot bound
        // the data-dependent bsearch loop at all (the unwinding assertion
        // for `while (last >= base)` FAILED at unwind 16 AND 80, > table
        // size 61/72, on fleet job pgrust-kani-suite-1785496407 — and a
        // failed unwinding assertion makes every downstream check garbage:
        // it reported a bogus pointer-OOB in strncmp and a bogus hit/miss
        // mismatch that were pure loop-truncation artifacts, NOT a
        // Rust-vs-C divergence). Concrete keys fold the loop; 0.43s/cell.
        let mut i = 0;
        while i < DATE_KEYS.len() {
            match i {
                0 => check_datebsearch_cell(DATE_KEYS[0], &DATETKTBL, pg_hlp_datebsearch_date),
                1 => check_datebsearch_cell(DATE_KEYS[1], &DATETKTBL, pg_hlp_datebsearch_date),
                2 => check_datebsearch_cell(DATE_KEYS[2], &DATETKTBL, pg_hlp_datebsearch_date),
                3 => check_datebsearch_cell(DATE_KEYS[3], &DATETKTBL, pg_hlp_datebsearch_date),
                4 => check_datebsearch_cell(DATE_KEYS[4], &DATETKTBL, pg_hlp_datebsearch_date),
                5 => check_datebsearch_cell(DATE_KEYS[5], &DATETKTBL, pg_hlp_datebsearch_date),
                6 => check_datebsearch_cell(DATE_KEYS[6], &DATETKTBL, pg_hlp_datebsearch_date),
                7 => check_datebsearch_cell(DATE_KEYS[7], &DATETKTBL, pg_hlp_datebsearch_date),
                8 => check_datebsearch_cell(DATE_KEYS[8], &DATETKTBL, pg_hlp_datebsearch_date),
                9 => check_datebsearch_cell(DATE_KEYS[9], &DATETKTBL, pg_hlp_datebsearch_date),
                10 => check_datebsearch_cell(DATE_KEYS[10], &DATETKTBL, pg_hlp_datebsearch_date),
                11 => check_datebsearch_cell(DATE_KEYS[11], &DATETKTBL, pg_hlp_datebsearch_date),
                12 => check_datebsearch_cell(DATE_KEYS[12], &DATETKTBL, pg_hlp_datebsearch_date),
                _ => check_datebsearch_cell(DATE_KEYS[13], &DATETKTBL, pg_hlp_datebsearch_date),
            }
            i += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(16)]
    fn eq_datebsearch_delta_cells() {
        // Same concrete-cell discipline as the date row above.
        let mut i = 0;
        while i < DELTA_KEYS.len() {
            match i {
                0 => check_datebsearch_cell(DELTA_KEYS[0], &DELTATKTBL, pg_hlp_datebsearch_delta),
                1 => check_datebsearch_cell(DELTA_KEYS[1], &DELTATKTBL, pg_hlp_datebsearch_delta),
                2 => check_datebsearch_cell(DELTA_KEYS[2], &DELTATKTBL, pg_hlp_datebsearch_delta),
                3 => check_datebsearch_cell(DELTA_KEYS[3], &DELTATKTBL, pg_hlp_datebsearch_delta),
                4 => check_datebsearch_cell(DELTA_KEYS[4], &DELTATKTBL, pg_hlp_datebsearch_delta),
                5 => check_datebsearch_cell(DELTA_KEYS[5], &DELTATKTBL, pg_hlp_datebsearch_delta),
                6 => check_datebsearch_cell(DELTA_KEYS[6], &DELTATKTBL, pg_hlp_datebsearch_delta),
                7 => check_datebsearch_cell(DELTA_KEYS[7], &DELTATKTBL, pg_hlp_datebsearch_delta),
                8 => check_datebsearch_cell(DELTA_KEYS[8], &DELTATKTBL, pg_hlp_datebsearch_delta),
                9 => check_datebsearch_cell(DELTA_KEYS[9], &DELTATKTBL, pg_hlp_datebsearch_delta),
                10 => check_datebsearch_cell(DELTA_KEYS[10], &DELTATKTBL, pg_hlp_datebsearch_delta),
                _ => check_datebsearch_cell(DELTA_KEYS[11], &DELTATKTBL, pg_hlp_datebsearch_delta),
            }
            i += 1;
        }
    }

    fn check_decode_special_cell(key10: &[u8]) {
        let mut kbuf = [0u8; 11];
        kbuf[..10].copy_from_slice(&key10[..10]);
        // cache-miss then cache-hit path
        for _ in 0..2 {
            let mut r_val: i32 = -99;
            let r_type = DecodeSpecial(0, &kbuf, &mut r_val);
            let mut c_val: c_int = -99;
            let c_type =
                unsafe { pg_hlp_decode_special(0, kbuf.as_ptr() as *const c_char, &mut c_val) };
            assert!(r_type == c_type && r_val == c_val);
        }
    }

    fn check_decode_units_cell(key10: &[u8]) {
        let mut kbuf = [0u8; 11];
        kbuf[..10].copy_from_slice(&key10[..10]);
        for _ in 0..2 {
            let mut r_val: i32 = -99;
            let r_type = DecodeUnits(0, &kbuf, &mut r_val);
            let mut c_val: c_int = -99;
            let c_type =
                unsafe { pg_hlp_decode_units(0, kbuf.as_ptr() as *const c_char, &mut c_val) };
            assert!(r_type == c_type && r_val == c_val);
        }
    }

    #[kani::proof]
    #[kani::unwind(16)]
    fn eq_decode_special_cells() {
        let mut i = 0;
        while i < DATE_KEYS.len() {
            match i {
                0 => check_decode_special_cell(DATE_KEYS[0]),
                1 => check_decode_special_cell(DATE_KEYS[1]),
                2 => check_decode_special_cell(DATE_KEYS[2]),
                3 => check_decode_special_cell(DATE_KEYS[3]),
                4 => check_decode_special_cell(DATE_KEYS[4]),
                5 => check_decode_special_cell(DATE_KEYS[5]),
                6 => check_decode_special_cell(DATE_KEYS[6]),
                7 => check_decode_special_cell(DATE_KEYS[7]),
                8 => check_decode_special_cell(DATE_KEYS[8]),
                9 => check_decode_special_cell(DATE_KEYS[9]),
                10 => check_decode_special_cell(DATE_KEYS[10]),
                11 => check_decode_special_cell(DATE_KEYS[11]),
                12 => check_decode_special_cell(DATE_KEYS[12]),
                _ => check_decode_special_cell(DATE_KEYS[13]),
            }
            i += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(16)]
    fn eq_decode_units_cells() {
        let mut i = 0;
        while i < DELTA_KEYS.len() {
            match i {
                0 => check_decode_units_cell(DELTA_KEYS[0]),
                1 => check_decode_units_cell(DELTA_KEYS[1]),
                2 => check_decode_units_cell(DELTA_KEYS[2]),
                3 => check_decode_units_cell(DELTA_KEYS[3]),
                4 => check_decode_units_cell(DELTA_KEYS[4]),
                5 => check_decode_units_cell(DELTA_KEYS[5]),
                6 => check_decode_units_cell(DELTA_KEYS[6]),
                7 => check_decode_units_cell(DELTA_KEYS[7]),
                8 => check_decode_units_cell(DELTA_KEYS[8]),
                9 => check_decode_units_cell(DELTA_KEYS[9]),
                10 => check_decode_units_cell(DELTA_KEYS[10]),
                _ => check_decode_units_cell(DELTA_KEYS[11]),
            }
            i += 1;
        }
    }

    /// Concrete: both sides accept the SHIPPED tables (ordering + length).
    #[kani::proof]
    #[kani::unwind(80)]
    fn check_date_token_tables_concrete() {
        assert!(CheckDateTokenTables());
        assert!(unsafe { pg_hlp_check_date_token_tables() } == 1);
    }

    /// Per-table variant: each SHIPPED table individually (the composite
    /// row above ANDs them, so a single-table regression could in principle
    /// hide behind the other's verdict).
    #[kani::proof]
    #[kani::unwind(80)]
    fn check_date_token_table_one_concrete() {
        assert!(CheckDateTokenTable(&DATETKTBL));
        assert!(unsafe { pg_hlp_check_date_token_table_one(0) } == 1);
        assert!(CheckDateTokenTable(&DELTATKTBL));
        assert!(unsafe { pg_hlp_check_date_token_table_one(1) } == 1);
    }

    /// float_time_overflows int-field plane: hour/min FULL symbolic with
    /// `sec` pinned to a literal cell, so the rint(sec * USECS_PER_SEC)
    /// 53-bit face stays concrete while every integer range arm and the
    /// (hour*60+min)*60*1e6 total-time arm is quantified. Literal pins, not
    /// assume-pins: a symbolic-index concrete grid folds per cell.
    #[kani::proof]
    fn eq_float_time_overflows_intfields() {
        const SECS: &[f64] = &[0.0, 1.0, 59.999999, 60.0, 60.000001, -0.0000001, 0.5];
        let idx: usize = kani::any();
        kani::assume(idx < SECS.len());
        let sec = SECS[idx];
        let (h, m): (i32, i32) = (kani::any(), kani::any());
        assert!(
            float_time_overflows(h, m, sec)
                == (unsafe { pg_hlp_float_time_overflows(h, m, sec) } != 0)
        );
    }

    /// float_time_overflows `sec` plane: concrete (hour, min) cells x a
    /// literal `sec` grid covering NaN, both infinities, the rint
    /// ties-to-even boundary, the 60s cap and negative underflow. FULL
    /// symbolic f64 `sec` is a 53-bit WALL per TRIAGE (rint over a
    /// symbolic double); the datetime_io_diff CGF target owns the bulk
    /// domain (make_time / make_timestamp arms).
    #[kani::proof]
    fn eq_float_time_overflows_sec_cells() {
        const SECS: &[f64] = &[
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0000005,
            0.0000015,
            59.9999995,
            60.0,
            60.0000004,
            60.0000005,
            -1.0,
            -0.0000004,
            86400.0,
            1e300,
        ];
        let si: usize = kani::any();
        kani::assume(si < SECS.len());
        let sec = SECS[si];
        let cell: u8 = kani::any();
        kani::assume(cell < 4);
        let (h, m) = match cell {
            0 => (0, 0),
            1 => (24, 0),
            2 => (23, 59),
            _ => (24, 59),
        };
        assert!(
            float_time_overflows(h, m, sec)
                == (unsafe { pg_hlp_float_time_overflows(h, m, sec) } != 0)
        );
    }
















    /// Negative control (MUST FAIL, default solver): j2day off-by-one skew.
    #[kani::proof]
    fn control_hlp_j2day_skew() {
        assert!(j2day(5) == unsafe { pg_hlp_j2day(5) } + 1);
    }

    /// Negative control (MUST FAIL): float_time_overflows with the `sec`
    /// rounding cell shifted one ulp-class, proving the sec plane above is
    /// not vacuous (its verdict does depend on the pinned value).
    #[kani::proof]
    fn control_hlp_float_time_overflows_sec_skew() {
        assert!(
            float_time_overflows(24, 0, 60.0000005)
                == (unsafe { pg_hlp_float_time_overflows(24, 0, 0.0) } != 0)
        );
    }

    // ---- time/timetz +- interval kernels (routes: kernel+spots+fuzz) ----
    //
    // These five share one divider chain: `result -= result / USECS_PER_DAY *
    // USECS_PER_DAY` followed by the `< 0` wrap. The full-domain ladders over
    // that expression are RECORDED WALLS for this family (601s, CBMC
    // per-property refinement, both solvers), so the proof obligation the
    // routes rows ask for is discharged as SPOT cells here and the
    // remaining domain by the datetime_convert_diff CGF target.
    //
    // Every cell is a LITERAL, never a `G[symbolic_idx]` draw: literal pins
    // constant-fold and the divider disappears, assume-pins do not — the
    // 6+-confirmation law in proofs/TRIAGE.md, and precisely the rebuild that
    // turned this family's 600s timeouts into seconds elsewhere.

    /// One cell of the time+-interval planes: value + ereport-verdict parity.
    macro_rules! eq_time_pm_iv_cell {
        ($(#[$attr:meta])* $name:ident, $time:expr, $sp_time:expr, $sp_day:expr, $sp_month:expr) => {
            #[kani::proof]
            $(#[$attr])*
            fn $name() {
                const TIME: i64 = $time;
                const SP_TIME: i64 = $sp_time;
                const SP_DAY: i32 = $sp_day;
                const SP_MONTH: i32 = $sp_month;
                let span = Interval { time: SP_TIME, day: SP_DAY, month: SP_MONTH };

                // pl
                let mut c: i64 = 0;
                let crc = unsafe { pg_hlp_time_pl_interval(TIME, SP_TIME, SP_DAY, SP_MONTH, &mut c) };
                match time_pl_interval(TIME, &span) {
                    Ok(r) => {
                        assert!(crc == 0);
                        assert!(r == c);
                    }
                    Err(_) => assert!(crc != 0),
                }

                // mi
                let mut c2: i64 = 0;
                let crc2 = unsafe { pg_hlp_time_mi_interval(TIME, SP_TIME, SP_DAY, SP_MONTH, &mut c2) };
                match time_mi_interval(TIME, &span) {
                    Ok(r) => {
                        assert!(crc2 == 0);
                        assert!(r == c2);
                    }
                    Err(_) => assert!(crc2 != 0),
                }

                // interval_time shares the family's `%`/wrap face
                let mut c3: i64 = 0;
                let crc3 = unsafe { pg_hlp_interval_time(SP_TIME, SP_DAY, SP_MONTH, &mut c3) };
                match interval_time(&span) {
                    Ok(r) => {
                        assert!(crc3 == 0);
                        assert!(r == c3);
                    }
                    Err(_) => assert!(crc3 != 0),
                }
            }
        };
    }

    /// One cell of the timetz+-interval planes (adds the zone-passthrough).
    macro_rules! eq_timetz_pm_iv_cell {
        ($(#[$attr:meta])* $name:ident, $time:expr, $zone:expr, $sp_time:expr, $sp_day:expr, $sp_month:expr) => {
            #[kani::proof]
            $(#[$attr])*
            fn $name() {
                const TIME: i64 = $time;
                const ZONE: i32 = $zone;
                const SP_TIME: i64 = $sp_time;
                const SP_DAY: i32 = $sp_day;
                const SP_MONTH: i32 = $sp_month;
                let span = Interval { time: SP_TIME, day: SP_DAY, month: SP_MONTH };
                let arg = TimeTzADT { time: TIME, zone: ZONE };

                let (mut ct, mut cz) = (0i64, 0i32);
                let crc = unsafe {
                    pg_hlp_timetz_pl_interval(TIME, ZONE, SP_TIME, SP_DAY, SP_MONTH, &mut ct, &mut cz)
                };
                match timetz_pl_interval(&arg, &span) {
                    Ok(r) => {
                        assert!(crc == 0);
                        assert!(r.time == ct && r.zone == cz);
                    }
                    Err(_) => assert!(crc != 0),
                }

                let (mut ct2, mut cz2) = (0i64, 0i32);
                let crc2 = unsafe {
                    pg_hlp_timetz_mi_interval(TIME, ZONE, SP_TIME, SP_DAY, SP_MONTH, &mut ct2, &mut cz2)
                };
                match timetz_mi_interval(&arg, &span) {
                    Ok(r) => {
                        assert!(crc2 == 0);
                        assert!(r.time == ct2 && r.zone == cz2);
                    }
                    Err(_) => assert!(crc2 != 0),
                }
            }
        };
    }

    // Boundary grid the routes rows name: the 24h wrap (exact USECS_PER_DAY
    // multiples, both signs), the fold-back sign flip, negative intervals, the
    // i64 usec extremes where C relies on -fwrapv, and the two
    // INTERVAL_NOT_FINITE sentinels that select the ereport arm.
    eq_time_pm_iv_cell!(eq_time_pm_iv_zero, 0, 0, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_one, 0, 1, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_negone, 0, -1, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_day, 0, 86_400_000_000, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_negday, 0, -86_400_000_000, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_daym1, 0, 86_399_999_999, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_twoday, 0, 172_800_000_000, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_noon_day, 43_200_000_000, 86_400_000_000, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_eod_one, 86_400_000_000, 1, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_eod_negone, 86_400_000_000, -1, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_i64max, 0, i64::MAX, 0, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_i64max_eod, 86_400_000_000, i64::MAX, 0, 0);
    // The not-finite arms construct adt_date's #[track_caller] #[cold] error
    // helper, and Kani cannot codegen caller_location (kani#374 — observed as
    // "caller_location is not currently supported"). Stubbing the PgError
    // constructor it calls is the crate-wide precedent (see the
    // intervaltypmodout rows above): only the message TEXT leaves the theorem,
    // while the verdict plane (Err vs Ok) and the arm's single sqlstate stay
    // in — and the verdict is exactly what these cells assert.
    eq_time_pm_iv_cell!(#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)] eq_time_pm_iv_notfinite_max, 0, i64::MAX, i32::MAX, i32::MAX);
    eq_time_pm_iv_cell!(#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)] eq_time_pm_iv_notfinite_min, 0, i64::MIN, i32::MIN, i32::MIN);
    eq_time_pm_iv_cell!(eq_time_pm_iv_days_only, 43_200_000_000, 0, 3, 0);
    eq_time_pm_iv_cell!(eq_time_pm_iv_months_only, 43_200_000_000, 0, 0, 5);

    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_zero, 0, 0, 0, 0, 0);
    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_one, 0, 3600, 1, 0, 0);
    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_negone, 0, -3600, -1, 0, 0);
    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_day, 43_200_000_000, 57_599, 86_400_000_000, 0, 0);
    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_negday, 43_200_000_000, -57_599, -86_400_000_000, 0, 0);
    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_eod, 86_400_000_000, 0, 1, 0, 0);
    eq_timetz_pm_iv_cell!(eq_timetz_pm_iv_i64max, 0, 3600, i64::MAX, 0, 0);
    eq_timetz_pm_iv_cell!(#[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)] eq_timetz_pm_iv_notfinite, 0, 3600, i64::MIN, i32::MIN, i32::MIN);

    /// MUST-FAIL CONTROL for the +-interval family: asserts the WRONG wrap
    /// (C's `< 0` arm removed), so a vacuous plane cannot pass. Expected
    /// verdict FAILED on `assert!(r == wrong)`.
    #[kani::proof]
    fn control_time_pl_interval_no_wrap() {
        const TIME: i64 = 0;
        const SP_TIME: i64 = -1;
        let span = Interval { time: SP_TIME, day: 0, month: 0 };
        let r = time_pl_interval(TIME, &span).expect("finite span");
        // the un-wrapped value: what the kernel would return without C's
        // `if (result < 0) result += USECS_PER_DAY`
        let wrong: i64 = -1;
        assert!(r == wrong);
    }

}
