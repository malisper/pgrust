//! `fmgr`-callable wrappers for the `internal`-transtype numeric aggregates
//! (`numeric.c`): the `NumericAggState` family (`numeric_avg_accum` /
//! `numeric_accum` + `numeric_avg` / `numeric_sum` / `numeric_var_*` /
//! `numeric_stddev_*`) and the 128-bit `Int128AggState` "poly" family
//! (`int2_accum` / `int4_accum` + `numeric_poly_*`).
//!
//! ## The `internal` transition state crosses the fmgr boundary
//!
//! C's transition state is a `void *` to a struct living in the per-aggregate
//! `MemoryContext`. Here it rides the canonical `Datum::Internal(Box<dyn Any>)`
//! arm (`RefPayload::Internal`): the executor moves the box in/out of the call
//! frame, the transfn mutates it in place, and returns the same box.
//!
//! ## `'mcx`-bound state (the frame-carrier resolution)
//!
//! `NumericAggState<'mcx>` carries context-charged `PgVec`s, so it cannot be a
//! `'static` `Box<dyn Any>` directly. The transfn owns the per-aggregate context
//! itself: [`NumericAggInternal`] holds a leaked `&'static MemoryContext` and the
//! `'static`-laundered state allocated in it. Every accumulation grows the
//! state's own (leaked-context) buffers — `accum_sum_*` reads the allocator off
//! the existing `PgVec` — so the state stays self-consistent across rows with no
//! dependence on the per-call scratch context. The leak models C's aggcontext,
//! whose `pfree`/reset nodeAgg owns (the by-ref free is a deferred TODO across
//! this repo). `Int128AggState` is `Copy`/`'static` and needs no context.

use mcx::MemoryContext;
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::FunctionCallInfoBaseData;
use types_numeric::var::NumericAggState;
use types_numeric::Int128AggState;

use crate::aggregate;

/// `(Node *) aggcontext`-charged `NumericAggState`. The leaked context backs the
/// state's charged buffers; the state is laundered to `'static` because that
/// context lives for the whole backend (C resets it per-group; we leak).
struct NumericAggInternal {
    /// The leaked per-aggregate `MemoryContext` (`&'static` so the state's
    /// `PgVec`s outlive every transfn call without a borrow).
    ctx: &'static MemoryContext,
    /// The running transition state, allocated in `ctx`.
    state: NumericAggState<'static>,
}

impl NumericAggInternal {
    /// A fresh state in a newly-leaked per-aggregate context.
    fn new(calc_sum_x2: bool) -> Box<NumericAggInternal> {
        let ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("numeric agg state")));
        let state = NumericAggState::new(ctx.mcx(), calc_sum_x2);
        Box::new(NumericAggInternal { ctx, state })
    }
}

/// Re-raise a builtin's `ereport(ERROR)` through the one dispatch point
/// (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err)
}

#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `PG_GETARG_NUMERIC(i)` — the on-disk `numeric` image (FULL varlena, with
/// header — the `numeric` cores read `numeric_sign` at `VARHDRSZ`). The
/// `internal`-transtype aggregate's `numeric` argument crosses on the by-ref lane
/// header-ful: its `pg_proc.proargtypes[1]` resolves through the polymorphic
/// `internal` first arg so `proc_arg_typlens` reports typlen 0 and the header is
/// left in place (unlike a scalar `numeric_add` arg, which the typed bridge
/// strips). Reconstruct the header only if the payload arrived header-less.
fn arg_numeric(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    let payload = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("numeric agg fn: by-ref `numeric` arg missing from by-ref lane");
    numeric_with_header(payload)
}

/// Ensure a `numeric` byte image carries its 4-byte varlena header. A header-ful
/// image has a length word matching its own length; otherwise prepend one.
fn numeric_with_header(bytes: &[u8]) -> Vec<u8> {
    if numeric_has_header(bytes) {
        return bytes.to_vec();
    }
    let total = types_datum::varlena::VARHDRSZ + bytes.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
    buf.extend_from_slice(bytes);
    buf
}

/// True if `bytes` begins with a 4-byte varlena length word equal to its length
/// (a header-ful image), distinguishing it from a bare header-less payload.
fn numeric_has_header(bytes: &[u8]) -> bool {
    if bytes.len() < types_datum::varlena::VARHDRSZ {
        return false;
    }
    // VARATT_IS_1B (low bit set) means a 1-byte short header — not produced for
    // an aggregate's numeric input here, so treat it as header-less.
    if bytes[0] & 0x01 != 0 {
        return false;
    }
    // A 4-byte `VARATT_IS_4B_U` header encodes `len << 2` (little-endian); a
    // header-ful image's encoded total length equals its own byte length.
    let word = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let encoded = if cfg!(target_endian = "little") {
        (word >> 2) as usize
    } else {
        (word & 0x3FFF_FFFF) as usize
    };
    encoded == bytes.len()
}

/// Take the `internal` transition state out of `args[0]`, downcast to the
/// concrete carrier. `None` is C's `PG_ARGISNULL(0)` (first call).
fn take_numeric_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<NumericAggInternal>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(
            b.downcast::<NumericAggInternal>()
                .unwrap_or_else(|_| panic!("numeric agg fn: args[0] internal state is not a NumericAggInternal")),
        ),
        Some(other) => panic!("numeric agg fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// Take the 128-bit `internal` transition state out of `args[0]`.
fn take_poly_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<Int128AggState>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(
            b.downcast::<Int128AggState>()
                .unwrap_or_else(|_| panic!("poly agg fn: args[0] internal state is not an Int128AggState")),
        ),
        Some(other) => panic!("poly agg fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// `PG_RETURN_POINTER(state)` — hand the transition state back as `internal`.
fn ret_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    Datum::from_usize(0)
}

/// `PG_RETURN_NUMERIC(image)` — by-ref numeric result. Header-ful everywhere:
/// the `RefPayload::Varlena` result lane carries the complete header-ful varlena
/// image verbatim (the numeric core's full image; `numeric_with_header` is
/// idempotent if it already carries the 4-byte length word).
fn ret_numeric(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(numeric_with_header(&image)));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Run a numeric final `f(mcx)` in a fresh scratch context, copying its result
/// image out by value (C: the palloc'd result lives in the caller's context).
fn run_final(
    f: impl for<'mcx> FnOnce(mcx::Mcx<'mcx>) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>>,
) -> Option<Vec<u8>> {
    let m = crate::fmgr_builtins::scratch_mcx();
    ok(f(m.mcx())).map(|image| image.as_slice().to_vec())
}

// ===========================================================================
// NumericAggState transition + final functions.
// ===========================================================================

/// `numeric_avg_accum`(2858) / `numeric_accum`(1833): accumulate one `numeric`
/// input into the running state (`calc_sum_x2` selects var/stddev).
fn numeric_accum_common(fcinfo: &mut FunctionCallInfoBaseData, calc_sum_x2: bool) -> Datum {
    // state = PG_ARGISNULL(0) ? makeNumericAggState(fcinfo, calcSumX2)
    //                         : (NumericAggState *) PG_GETARG_POINTER(0);
    let mut carrier = take_numeric_state(fcinfo).unwrap_or_else(|| NumericAggInternal::new(calc_sum_x2));

    // if (!PG_ARGISNULL(1)) do_numeric_accum(state, PG_GETARG_NUMERIC(1));
    if !arg_isnull(fcinfo, 1) {
        let newval = arg_numeric(fcinfo, 1);
        let ctx_mcx = carrier.ctx.mcx();
        ok(aggregate::do_numeric_accum(ctx_mcx, &mut carrier.state, &newval));
    }
    ret_internal(fcinfo, carrier)
}

fn fc_numeric_avg_accum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    numeric_accum_common(fcinfo, false)
}

fn fc_numeric_accum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    numeric_accum_common(fcinfo, true)
}

fn fc_numeric_avg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match take_numeric_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(carrier) => match run_final(|m| aggregate::numeric_avg(m, &carrier.state)) {
            Some(image) => ret_numeric(fcinfo, image),
            None => ret_null(fcinfo),
        },
    }
}

fn fc_numeric_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match take_numeric_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(carrier) => match run_final(|m| aggregate::numeric_sum(m, &carrier.state)) {
            Some(image) => ret_numeric(fcinfo, image),
            None => ret_null(fcinfo),
        },
    }
}

fn numeric_stddev_final(
    fcinfo: &mut FunctionCallInfoBaseData,
    variance: bool,
    sample: bool,
) -> Datum {
    match take_numeric_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(carrier) => match run_final(|m| {
            aggregate::numeric_stddev_internal(m, &carrier.state, variance, sample)
        }) {
            Some(image) => ret_numeric(fcinfo, image),
            None => ret_null(fcinfo),
        },
    }
}

fn fc_numeric_var_pop(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    numeric_stddev_final(fcinfo, true, false)
}
fn fc_numeric_var_samp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    numeric_stddev_final(fcinfo, true, true)
}
fn fc_numeric_stddev_pop(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    numeric_stddev_final(fcinfo, false, false)
}
fn fc_numeric_stddev_samp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    numeric_stddev_final(fcinfo, false, true)
}

// ===========================================================================
// Int128AggState (poly) transition + final functions.
// ===========================================================================

/// `int2_accum`(1834) / `int4_accum`(1835): accumulate one int input into the
/// 128-bit state (`calc_sum_x2 = true`, for var/stddev).
fn poly_accum_common(fcinfo: &mut FunctionCallInfoBaseData, width: u8) -> Datum {
    let mut state = take_poly_state(fcinfo)
        .unwrap_or_else(|| Box::new(aggregate::make_int128_agg_state(true)));
    if !arg_isnull(fcinfo, 1) {
        let v = fcinfo.arg(1).expect("poly agg: missing arg 1").value;
        let newval: i128 = match width {
            2 => i128::from(v.as_i16()),
            _ => i128::from(v.as_i32()),
        };
        aggregate::do_int128_accum(&mut state, newval);
    }
    ret_internal(fcinfo, state)
}

fn fc_int2_accum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    poly_accum_common(fcinfo, 2)
}
fn fc_int4_accum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    poly_accum_common(fcinfo, 4)
}

fn fc_numeric_poly_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match take_poly_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(state) => match run_final(|m| aggregate::numeric_poly_sum(m, &state)) {
            Some(image) => ret_numeric(fcinfo, image),
            None => ret_null(fcinfo),
        },
    }
}

fn fc_numeric_poly_avg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match take_poly_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(state) => match run_final(|m| aggregate::numeric_poly_avg(m, &state)) {
            Some(image) => ret_numeric(fcinfo, image),
            None => ret_null(fcinfo),
        },
    }
}

fn poly_stddev_final(
    fcinfo: &mut FunctionCallInfoBaseData,
    variance: bool,
    sample: bool,
) -> Datum {
    match take_poly_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(state) => match run_final(|m| {
            aggregate::numeric_poly_stddev_internal(m, &state, variance, sample)
        }) {
            Some(image) => ret_numeric(fcinfo, image),
            None => ret_null(fcinfo),
        },
    }
}

fn fc_numeric_poly_var_pop(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    poly_stddev_final(fcinfo, true, false)
}
fn fc_numeric_poly_var_samp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    poly_stddev_final(fcinfo, true, true)
}
fn fc_numeric_poly_stddev_pop(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    poly_stddev_final(fcinfo, false, false)
}
fn fc_numeric_poly_stddev_samp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    poly_stddev_final(fcinfo, false, true)
}

// ===========================================================================
// Registration (C: their `fmgr_builtins[]` rows; OIDs/nargs from pg_proc.dat —
// transition/final functions are `proisstrict => 'f'` because they handle the
// NULL `internal` running state, except the int8 finals which are strict).
// ===========================================================================

pub fn register_numeric_agg_builtins() {
    use crate::fmgr_builtins::builtin;
    backend_utils_fmgr_core::register_builtins([
        // NumericAggState transitions.
        builtin(2858, "numeric_avg_accum", 2, false, false, fc_numeric_avg_accum),
        builtin(1833, "numeric_accum", 2, false, false, fc_numeric_accum),
        // NumericAggState finals.
        builtin(1837, "numeric_avg", 1, false, false, fc_numeric_avg),
        builtin(3178, "numeric_sum", 1, false, false, fc_numeric_sum),
        builtin(2514, "numeric_var_pop", 1, false, false, fc_numeric_var_pop),
        builtin(1838, "numeric_var_samp", 1, false, false, fc_numeric_var_samp),
        builtin(2596, "numeric_stddev_pop", 1, false, false, fc_numeric_stddev_pop),
        builtin(1839, "numeric_stddev_samp", 1, false, false, fc_numeric_stddev_samp),
        // Int128AggState (poly) transitions.
        builtin(1834, "int2_accum", 2, false, false, fc_int2_accum),
        builtin(1835, "int4_accum", 2, false, false, fc_int4_accum),
        // Int128AggState (poly) finals.
        builtin(3388, "numeric_poly_sum", 1, false, false, fc_numeric_poly_sum),
        builtin(3389, "numeric_poly_avg", 1, false, false, fc_numeric_poly_avg),
        builtin(3390, "numeric_poly_var_pop", 1, false, false, fc_numeric_poly_var_pop),
        builtin(3391, "numeric_poly_var_samp", 1, false, false, fc_numeric_poly_var_samp),
        builtin(3392, "numeric_poly_stddev_pop", 1, false, false, fc_numeric_poly_stddev_pop),
        builtin(3393, "numeric_poly_stddev_samp", 1, false, false, fc_numeric_poly_stddev_samp),
    ]);
}
