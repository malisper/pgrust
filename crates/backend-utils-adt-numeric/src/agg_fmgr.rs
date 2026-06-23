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

/// Un-pack a SHORT (1-byte low-bit-set, non-1-byte-toast) varlena header into the
/// canonical 4-byte-header image, mirroring C's `PG_DETOAST_DATUM` short arm
/// (`detoast_attr`: `SET_VARSIZE(new, data_size + VARHDRSZ); copy VARDATA_SHORT`).
/// A stored `numeric` column value arrives short-headed once
/// `SHORT_VARLENA_PACKING` is on, but C's `PG_GETARG_NUMERIC` detoasts (un-packs)
/// it before the cores read the struct; the numeric byte-view accessors expect a
/// header (4-byte OR short), so we normalise to 4-byte here. No-op while the flag
/// is off (no stored value is short).
///
/// Also reused by [`crate::fmgr_builtins`] for the `_int8` AVG/SUM transition
/// array args (`int4_avg_accum` / `int8_avg` / ...), whose `ArrayType` header is
/// likewise mis-read 3 bytes off when the stored varlena arrives short-packed.
pub(crate) fn unpack_short_to_4b(bytes: &[u8]) -> Option<Vec<u8>> {
    // VARATT_IS_1B_E (0x01) is a 1-byte external TOAST pointer — never a packed
    // inline short header — so exclude it; any other low-bit-set first byte is a
    // short (VARATT_IS_SHORT) header whose length is the high 7 bits.
    let h = *bytes.first()?;
    if h == 0x01 || (h & 0x01) == 0 {
        return None;
    }
    let short_len = ((h >> 1) & 0x7f) as usize; // VARSIZE_SHORT == (header >> 1) & 0x7F
    let data_size = short_len.saturating_sub(1); // minus VARHDRSZ_SHORT (1)
    let total = types_datum::varlena::VARHDRSZ + data_size;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
    buf.extend_from_slice(&bytes[1..1 + data_size]);
    Some(buf)
}

/// Ensure a `numeric` byte image carries a canonical 4-byte varlena header. A
/// 4-byte-header-ful image has a length word matching its own length and is
/// returned verbatim; a SHORT-headed image is un-packed to 4-byte (C's
/// `PG_GETARG_NUMERIC` detoast); a bare header-less payload gets a header
/// prepended.
fn numeric_with_header(bytes: &[u8]) -> Vec<u8> {
    if numeric_has_header(bytes) {
        return bytes.to_vec();
    }
    if let Some(unpacked) = unpack_short_to_4b(bytes) {
        return unpacked;
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
    take_numeric_state_at(fcinfo, 0)
}

/// Take the `internal` transition state out of `args[i]`.
fn take_numeric_state_at(
    fcinfo: &mut FunctionCallInfoBaseData,
    i: usize,
) -> Option<Box<NumericAggInternal>> {
    if arg_isnull(fcinfo, i) {
        return None;
    }
    match fcinfo.take_ref_arg(i) {
        Some(RefPayload::Internal(b)) => Some(
            b.downcast::<NumericAggInternal>()
                .unwrap_or_else(|_| panic!("numeric agg fn: args[{i}] internal state is not a NumericAggInternal")),
        ),
        Some(other) => panic!("numeric agg fn: args[{i}] is not an internal state ({other:?})"),
        None => None,
    }
}

/// Take the 128-bit `internal` transition state out of `args[0]`.
fn take_poly_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<Int128AggState>> {
    take_poly_state_at(fcinfo, 0)
}

/// Take the 128-bit `internal` transition state out of `args[i]`.
fn take_poly_state_at(
    fcinfo: &mut FunctionCallInfoBaseData,
    i: usize,
) -> Option<Box<Int128AggState>> {
    if arg_isnull(fcinfo, i) {
        return None;
    }
    match fcinfo.take_ref_arg(i) {
        Some(RefPayload::Internal(b)) => Some(
            b.downcast::<Int128AggState>()
                .unwrap_or_else(|_| panic!("poly agg fn: args[{i}] internal state is not an Int128AggState")),
        ),
        Some(other) => panic!("poly agg fn: args[{i}] is not an internal state ({other:?})"),
        None => None,
    }
}

/// `PG_RETURN_POINTER(state)` — hand the transition state back as `internal`.
fn ret_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    Datum::from_usize(0)
}

/// Restore an `internal` transition state into `args[0]` after a *final*
/// function read it.  C's `PG_GETARG_POINTER(0)` does NOT consume the state: a
/// finalfn only reads it, and when several aggregates share one transition state
/// (e.g. `sum(numeric)` and `avg(numeric)`, which share `numeric_avg_accum`),
/// each one's finalfn reads the same live state in turn.  The owned model carries
/// that state as a move-only `Box<dyn Any>` on `ref_args[0]`; `take_*_state`
/// moved it out to downcast, so put it back here so the owned-finalfn seam can
/// hand it back to the executor for the next sharing aggregate's finalfn.
fn keep_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) {
    fcinfo.set_ref_arg(0, RefPayload::Internal(state));
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
) -> types_error::PgResult<Option<Vec<u8>>> {
    let m = crate::fmgr_builtins::scratch_mcx();
    let out = f(m.mcx())?.map(|image| image.as_slice().to_vec());
    Ok(out)
}

// ===========================================================================
// NumericAggState transition + final functions.
// ===========================================================================

/// `numeric_avg_accum`(2858) / `numeric_accum`(1833): accumulate one `numeric`
/// input into the running state (`calc_sum_x2` selects var/stddev).
fn numeric_accum_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    calc_sum_x2: bool,
) -> types_error::PgResult<Datum> {
    // state = PG_ARGISNULL(0) ? makeNumericAggState(fcinfo, calcSumX2)
    //                         : (NumericAggState *) PG_GETARG_POINTER(0);
    let mut carrier = take_numeric_state(fcinfo).unwrap_or_else(|| NumericAggInternal::new(calc_sum_x2));

    // if (!PG_ARGISNULL(1)) do_numeric_accum(state, PG_GETARG_NUMERIC(1));
    if !arg_isnull(fcinfo, 1) {
        let newval = arg_numeric(fcinfo, 1);
        let ctx_mcx = carrier.ctx.mcx();
        aggregate::do_numeric_accum(ctx_mcx, &mut carrier.state, &newval)?;
    }
    Ok(ret_internal(fcinfo, carrier))
}

fn fc_numeric_avg_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_accum_common(fcinfo, false)
}

fn fc_numeric_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_accum_common(fcinfo, true)
}

fn fc_numeric_avg(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match take_numeric_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            let out = run_final(|m| aggregate::numeric_avg(m, &carrier.state))?;
            // C `PG_GETARG_POINTER(0)` does not consume the state; restore it for
            // any aggregate sharing this transition state (e.g. sum + avg).
            keep_internal(fcinfo, carrier);
            Ok(match out {
                Some(image) => ret_numeric(fcinfo, image),
                None => ret_null(fcinfo),
            })
        }
    }
}

fn fc_numeric_sum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match take_numeric_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            let out = run_final(|m| aggregate::numeric_sum(m, &carrier.state))?;
            keep_internal(fcinfo, carrier);
            Ok(match out {
                Some(image) => ret_numeric(fcinfo, image),
                None => ret_null(fcinfo),
            })
        }
    }
}

fn numeric_stddev_final(
    fcinfo: &mut FunctionCallInfoBaseData,
    variance: bool,
    sample: bool,
) -> types_error::PgResult<Datum> {
    match take_numeric_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            let out = run_final(|m| {
                aggregate::numeric_stddev_internal(m, &carrier.state, variance, sample)
            })?;
            keep_internal(fcinfo, carrier);
            Ok(match out {
                Some(image) => ret_numeric(fcinfo, image),
                None => ret_null(fcinfo),
            })
        }
    }
}

fn fc_numeric_var_pop(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_stddev_final(fcinfo, true, false)
}
fn fc_numeric_var_samp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_stddev_final(fcinfo, true, true)
}
fn fc_numeric_stddev_pop(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_stddev_final(fcinfo, false, false)
}
fn fc_numeric_stddev_samp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_stddev_final(fcinfo, false, true)
}

// ===========================================================================
// Int128AggState (poly) transition + final functions.
// ===========================================================================

/// `int2_accum`(1834) / `int4_accum`(1835): accumulate one int input into the
/// 128-bit state (`calc_sum_x2 = true`, for var/stddev).
fn poly_accum_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    width: u8,
) -> types_error::PgResult<Datum> {
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
    Ok(ret_internal(fcinfo, state))
}

fn fc_int2_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_accum_common(fcinfo, 2)
}
fn fc_int4_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_accum_common(fcinfo, 4)
}

/// `int2_accum_inv`(3567) / `int4_accum_inv`(3568): inverse transition for
/// moving-window SUM/var/stddev over the 128-bit poly state. C: errors on NULL
/// state, then (HAVE_INT128) `do_int128_discard(state, (int128) arg)`.
fn poly_accum_inv_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    width: u8,
) -> types_error::PgResult<Datum> {
    let mut state = match take_poly_state(fcinfo) {
        Some(s) => s,
        None => {
            let name = if width == 2 {
                "int2_accum_inv called with NULL state"
            } else {
                "int4_accum_inv called with NULL state"
            };
            return Err(types_error::PgError::error(name));
        }
    };
    if !arg_isnull(fcinfo, 1) {
        let v = fcinfo.arg(1).expect("poly agg inv: missing arg 1").value;
        let newval: i128 = match width {
            2 => i128::from(v.as_i16()),
            _ => i128::from(v.as_i32()),
        };
        aggregate::do_int128_discard(&mut state, newval);
    }
    Ok(ret_internal(fcinfo, state))
}

fn fc_int2_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_accum_inv_common(fcinfo, 2)
}
fn fc_int4_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_accum_inv_common(fcinfo, 4)
}

/// `numeric_accum_inv`(3548): inverse transition for moving-window
/// SUM/AVG/var/stddev over `numeric`. C: errors on NULL state; if the inverse
/// `do_numeric_discard` fails (a dscale-loss row left the window), `RETURN_NULL`.
fn fc_numeric_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut carrier = match take_numeric_state(fcinfo) {
        Some(c) => c,
        None => {
            return Err(types_error::PgError::error(
                "numeric_accum_inv called with NULL state",
            ))
        }
    };
    if !arg_isnull(fcinfo, 1) {
        let newval = arg_numeric(fcinfo, 1);
        let ctx_mcx = carrier.ctx.mcx();
        // If we fail to perform the inverse transition, return NULL.
        if !aggregate::do_numeric_discard(ctx_mcx, &mut carrier.state, &newval)? {
            fcinfo.set_result_null(true);
            return Ok(Datum::null());
        }
    }
    Ok(ret_internal(fcinfo, carrier))
}

/// `int8_accum`(1836): SUM/AVG over int8 with sumX2. The X² of an int8 can
/// overflow int128, so int8 uses the wider `NumericAggState` (not the poly
/// int128 path) — C: `state = makeNumericAggState(fcinfo, true)`;
/// `do_numeric_accum(state, int64_to_numeric(PG_GETARG_INT64(1)))`.
fn fc_int8_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut carrier = take_numeric_state(fcinfo).unwrap_or_else(|| NumericAggInternal::new(true));
    if !arg_isnull(fcinfo, 1) {
        let v = fcinfo.arg(1).expect("int8_accum: missing arg 1").value.as_i64();
        let ctx_mcx = carrier.ctx.mcx();
        let num = crate::convert::int64_to_numeric(ctx_mcx, v)?;
        aggregate::do_numeric_accum(ctx_mcx, &mut carrier.state, &num)?;
    }
    Ok(ret_internal(fcinfo, carrier))
}

/// `int8_accum_inv`(3568): inverse transition for moving-window SUM/AVG(int8).
/// C: errors on NULL state; `do_numeric_discard(state, int64_to_numeric(...))`
/// which never fails (all int inputs have dscale 0).
fn fc_int8_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut carrier = match take_numeric_state(fcinfo) {
        Some(c) => c,
        None => {
            return Err(types_error::PgError::error(
                "int8_accum_inv called with NULL state",
            ))
        }
    };
    if !arg_isnull(fcinfo, 1) {
        let v = fcinfo.arg(1).expect("int8_accum_inv: missing arg 1").value.as_i64();
        let ctx_mcx = carrier.ctx.mcx();
        let num = crate::convert::int64_to_numeric(ctx_mcx, v)?;
        if !aggregate::do_numeric_discard(ctx_mcx, &mut carrier.state, &num)? {
            return Err(types_error::PgError::error(
                "do_numeric_discard failed unexpectedly",
            ));
        }
    }
    Ok(ret_internal(fcinfo, carrier))
}

/// `int8_avg_accum`(2746): AVG(int8) transition (no sumX2). The int128 sumX can
/// hold the running sum of int8 inputs, so it uses the poly path with
/// `calc_sum_x2 = false` — C: `state = makePolyNumAggState(fcinfo, false)`;
/// `do_int128_accum(state, (int128) PG_GETARG_INT64(1))`.
fn fc_int8_avg_accum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut state = take_poly_state(fcinfo)
        .unwrap_or_else(|| Box::new(aggregate::make_int128_agg_state(false)));
    if !arg_isnull(fcinfo, 1) {
        let v = fcinfo.arg(1).expect("int8_avg_accum: missing arg 1").value.as_i64();
        aggregate::do_int128_accum(&mut state, i128::from(v));
    }
    Ok(ret_internal(fcinfo, state))
}

/// `int8_avg_accum_inv`(3387): inverse transition for moving-window AVG(int8).
fn fc_int8_avg_accum_inv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let mut state = match take_poly_state(fcinfo) {
        Some(s) => s,
        None => {
            return Err(types_error::PgError::error(
                "int8_avg_accum_inv called with NULL state",
            ))
        }
    };
    if !arg_isnull(fcinfo, 1) {
        let v = fcinfo.arg(1).expect("int8_avg_accum_inv: missing arg 1").value.as_i64();
        aggregate::do_int128_discard(&mut state, i128::from(v));
    }
    Ok(ret_internal(fcinfo, state))
}

fn fc_numeric_poly_sum(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match take_poly_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(state) => {
            let out = run_final(|m| aggregate::numeric_poly_sum(m, &state))?;
            keep_internal(fcinfo, state);
            Ok(match out {
                Some(image) => ret_numeric(fcinfo, image),
                None => ret_null(fcinfo),
            })
        }
    }
}

fn fc_numeric_poly_avg(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match take_poly_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(state) => {
            let out = run_final(|m| aggregate::numeric_poly_avg(m, &state))?;
            keep_internal(fcinfo, state);
            Ok(match out {
                Some(image) => ret_numeric(fcinfo, image),
                None => ret_null(fcinfo),
            })
        }
    }
}

fn poly_stddev_final(
    fcinfo: &mut FunctionCallInfoBaseData,
    variance: bool,
    sample: bool,
) -> types_error::PgResult<Datum> {
    match take_poly_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(state) => {
            let out = run_final(|m| {
                aggregate::numeric_poly_stddev_internal(m, &state, variance, sample)
            })?;
            keep_internal(fcinfo, state);
            Ok(match out {
                Some(image) => ret_numeric(fcinfo, image),
                None => ret_null(fcinfo),
            })
        }
    }
}

fn fc_numeric_poly_var_pop(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_stddev_final(fcinfo, true, false)
}
fn fc_numeric_poly_var_samp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_stddev_final(fcinfo, true, true)
}
fn fc_numeric_poly_stddev_pop(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_stddev_final(fcinfo, false, false)
}
fn fc_numeric_poly_stddev_samp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    poly_stddev_final(fcinfo, false, true)
}

// ===========================================================================
// Serialize / deserialize / combine (parallel-aggregation support).
// ===========================================================================

/// `PG_RETURN_BYTEA_P(result)` — the serialized state is a header-ful `bytea`
/// image (`end_typsend` set the 4-byte length word); hand it back verbatim on
/// the by-ref Varlena result lane.
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// `VARDATA_ANY(PG_GETARG_BYTEA_PP(0))` — the wire body of the `bytea`
/// deserialize argument, with any 4-byte varlena header stripped. The state
/// `bytea` crosses the by-ref lane; it may arrive header-ful (the result of a
/// peer's serialize) or already header-less, so strip only when a length word
/// matching the image length is present.
fn arg_bytea_body(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    let payload = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("agg deserialize fn: by-ref `bytea` arg missing from by-ref lane");
    if numeric_has_header(payload) {
        payload[types_datum::varlena::VARHDRSZ..].to_vec()
    } else {
        payload.to_vec()
    }
}

/// `numeric_serialize`(3335) / `numeric_avg_serialize`(2740): serialize a
/// `NumericAggState` (var/stddev family requires sumX2; avg/sum family does not).
fn numeric_serialize_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    with_sum_x2: bool,
) -> types_error::PgResult<Datum> {
    // strict: the `internal` state arg is always present.
    let carrier = take_numeric_state(fcinfo)
        .expect("numeric serialize fn: NULL internal state (strict aggregate)");
    let out = run_final(|m| {
        let img = if with_sum_x2 {
            aggregate::numeric_serialize(m, &carrier.state)?
        } else {
            aggregate::numeric_avg_serialize(m, &carrier.state)?
        };
        Ok(Some(img))
    })?;
    // PG_GETARG_POINTER(0) does not consume the state; restore it.
    keep_internal(fcinfo, carrier);
    Ok(ret_bytea(fcinfo, out.expect("serialize produced no image")))
}

fn fc_numeric_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_serialize_common(fcinfo, true)
}
fn fc_numeric_avg_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_serialize_common(fcinfo, false)
}

/// `numeric_deserialize`(3336) / `numeric_avg_deserialize`(2741): rebuild a
/// `NumericAggState` from the serialized `bytea`. The fresh state lives in a
/// leaked per-aggregate context (matching `makeNumericAggStateCurrentContext`).
fn numeric_deserialize_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    with_sum_x2: bool,
) -> types_error::PgResult<Datum> {
    let body = arg_bytea_body(fcinfo, 0);
    let ctx: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("numeric agg state")));
    let state = if with_sum_x2 {
        aggregate::numeric_deserialize(ctx.mcx(), &body)?
    } else {
        aggregate::numeric_avg_deserialize(ctx.mcx(), &body)?
    };
    let carrier = Box::new(NumericAggInternal { ctx, state });
    Ok(ret_internal(fcinfo, carrier))
}

fn fc_numeric_deserialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_deserialize_common(fcinfo, true)
}
fn fc_numeric_avg_deserialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_deserialize_common(fcinfo, false)
}

/// `numeric_combine`(3341) / `numeric_avg_combine`(3337): combine two
/// `NumericAggState` transition states. Both args are `internal`; either may be
/// NULL. The combined state is allocated in `state1`'s context (or a fresh
/// leaked context when `state1` is NULL), matching C's aggcontext.
fn numeric_combine_common(
    fcinfo: &mut FunctionCallInfoBaseData,
    with_sum_x2: bool,
) -> types_error::PgResult<Datum> {
    let carrier1 = take_numeric_state(fcinfo);
    let carrier2 = take_numeric_state_at(fcinfo, 1);

    // if (state2 == NULL) PG_RETURN_POINTER(state1);
    let carrier2 = match carrier2 {
        None => {
            return Ok(match carrier1 {
                Some(c1) => ret_internal(fcinfo, c1),
                None => ret_null(fcinfo),
            })
        }
        Some(c2) => c2,
    };

    // Result lives in state1's context, or a fresh leaked context if state1 is
    // NULL (C switches to agg_context for makeNumericAggStateCurrentContext).
    let ctx: &'static MemoryContext = match &carrier1 {
        Some(c1) => c1.ctx,
        None => Box::leak(Box::new(MemoryContext::new("numeric agg state"))),
    };
    let state1_in = carrier1.map(|c1| c1.state);
    let combined = if with_sum_x2 {
        aggregate::numeric_combine(ctx.mcx(), state1_in, &carrier2.state)?
    } else {
        aggregate::numeric_avg_combine(ctx.mcx(), state1_in, &carrier2.state)?
    };
    let carrier = Box::new(NumericAggInternal { ctx, state: combined });
    Ok(ret_internal(fcinfo, carrier))
}

fn fc_numeric_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_combine_common(fcinfo, true)
}
fn fc_numeric_avg_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    numeric_combine_common(fcinfo, false)
}

/// `numeric_poly_serialize`(3339): serialize an `Int128AggState` poly state.
fn fc_numeric_poly_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state = take_poly_state(fcinfo)
        .expect("numeric_poly_serialize: NULL internal state (strict aggregate)");
    let out = run_final(|m| Ok(Some(aggregate::numeric_poly_serialize(m, &state)?)))?;
    keep_internal(fcinfo, state);
    Ok(ret_bytea(fcinfo, out.expect("serialize produced no image")))
}

/// `numeric_poly_deserialize`(3340): rebuild an `Int128AggState` from `bytea`.
fn fc_numeric_poly_deserialize(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let body = arg_bytea_body(fcinfo, 0);
    // The deserialize converts NumericVar→int128 in a scratch context; the
    // resulting Int128AggState is Copy/'static and needs no leaked context.
    let m = crate::fmgr_builtins::scratch_mcx();
    let state = aggregate::numeric_poly_deserialize(m.mcx(), &body)?;
    Ok(ret_internal(fcinfo, Box::new(state)))
}

/// `numeric_poly_combine`(3338): combine two `Int128AggState` poly states.
fn fc_numeric_poly_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state1 = take_poly_state(fcinfo).map(|b| *b);
    let state2 = take_poly_state_at(fcinfo, 1);

    let state2 = match state2 {
        None => {
            // if (state2 == NULL) PG_RETURN_POINTER(state1);
            return Ok(match state1 {
                Some(s1) => ret_internal(fcinfo, Box::new(s1)),
                None => ret_null(fcinfo),
            });
        }
        Some(s2) => *s2,
    };

    let combined = aggregate::numeric_poly_combine(state1, &state2);
    Ok(ret_internal(fcinfo, Box::new(combined)))
}

/// `int8_avg_serialize`(2786): serialize an `Int128AggState` poly state for
/// AVG(int8) (N + sumX only, no sumX2).
fn fc_int8_avg_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state = take_poly_state(fcinfo)
        .expect("int8_avg_serialize: NULL internal state (strict aggregate)");
    let out = run_final(|m| Ok(Some(aggregate::int8_avg_serialize(m, &state)?)))?;
    keep_internal(fcinfo, state);
    Ok(ret_bytea(fcinfo, out.expect("serialize produced no image")))
}

/// `int8_avg_deserialize`(2787): rebuild an `Int128AggState` from `bytea` for
/// AVG(int8) (N + sumX only, no sumX2).
fn fc_int8_avg_deserialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let body = arg_bytea_body(fcinfo, 0);
    let m = crate::fmgr_builtins::scratch_mcx();
    let state = aggregate::int8_avg_deserialize(m.mcx(), &body)?;
    Ok(ret_internal(fcinfo, Box::new(state)))
}

/// `int8_avg_combine`(2785): combine two `Int128AggState` poly states for
/// AVG(int8) (N + sumX only, no sumX2).
fn fc_int8_avg_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state1 = take_poly_state(fcinfo).map(|b| *b);
    let state2 = take_poly_state_at(fcinfo, 1);

    let state2 = match state2 {
        None => {
            // if (state2 == NULL) PG_RETURN_POINTER(state1);
            return Ok(match state1 {
                Some(s1) => ret_internal(fcinfo, Box::new(s1)),
                None => ret_null(fcinfo),
            });
        }
        Some(s2) => *s2,
    };

    let combined = aggregate::int8_avg_combine(state1, &state2);
    Ok(ret_internal(fcinfo, Box::new(combined)))
}

// ===========================================================================
// Registration (C: their `fmgr_builtins[]` rows; OIDs/nargs from pg_proc.dat —
// transition/final functions are `proisstrict => 'f'` because they handle the
// NULL `internal` running state, except the int8 finals which are strict).
// ===========================================================================

pub fn register_numeric_agg_builtins() {
    use crate::fmgr_builtins::builtin;
    backend_utils_fmgr_core::register_builtins_native([
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
        builtin(3567, "int2_accum_inv", 2, false, false, fc_int2_accum_inv),
        builtin(3568, "int4_accum_inv", 2, false, false, fc_int4_accum_inv),
        builtin(3548, "numeric_accum_inv", 2, false, false, fc_numeric_accum_inv),
        // int8 SUM/AVG transitions: int8_accum uses the wider NumericAggState
        // (int8 X² overflows int128); int8_avg_accum uses the poly int128 path.
        builtin(1836, "int8_accum", 2, false, false, fc_int8_accum),
        builtin(3569, "int8_accum_inv", 2, false, false, fc_int8_accum_inv),
        builtin(2746, "int8_avg_accum", 2, false, false, fc_int8_avg_accum),
        builtin(3387, "int8_avg_accum_inv", 2, false, false, fc_int8_avg_accum_inv),
        // Int128AggState (poly) finals.
        builtin(3388, "numeric_poly_sum", 1, false, false, fc_numeric_poly_sum),
        builtin(3389, "numeric_poly_avg", 1, false, false, fc_numeric_poly_avg),
        builtin(3390, "numeric_poly_var_pop", 1, false, false, fc_numeric_poly_var_pop),
        builtin(3391, "numeric_poly_var_samp", 1, false, false, fc_numeric_poly_var_samp),
        builtin(3392, "numeric_poly_stddev_pop", 1, false, false, fc_numeric_poly_stddev_pop),
        builtin(3393, "numeric_poly_stddev_samp", 1, false, false, fc_numeric_poly_stddev_samp),
        // Parallel-aggregation serialize / deserialize / combine.
        // NumericAggState (var/stddev family, with sumX2).
        builtin(3335, "numeric_serialize", 1, true, false, fc_numeric_serialize),
        builtin(3336, "numeric_deserialize", 2, true, false, fc_numeric_deserialize),
        builtin(3341, "numeric_combine", 2, false, false, fc_numeric_combine),
        // NumericAggState (avg/sum family, no sumX2).
        builtin(2740, "numeric_avg_serialize", 1, true, false, fc_numeric_avg_serialize),
        builtin(2741, "numeric_avg_deserialize", 2, true, false, fc_numeric_avg_deserialize),
        builtin(3337, "numeric_avg_combine", 2, false, false, fc_numeric_avg_combine),
        // Int128AggState (poly) serialize / deserialize / combine.
        builtin(3339, "numeric_poly_serialize", 1, true, false, fc_numeric_poly_serialize),
        builtin(3340, "numeric_poly_deserialize", 2, true, false, fc_numeric_poly_deserialize),
        builtin(3338, "numeric_poly_combine", 2, false, false, fc_numeric_poly_combine),
        // Int128AggState (poly, AVG(int8) — N + sumX only, no sumX2).
        builtin(2786, "int8_avg_serialize", 1, true, false, fc_int8_avg_serialize),
        builtin(2787, "int8_avg_deserialize", 2, true, false, fc_int8_avg_deserialize),
        builtin(2785, "int8_avg_combine", 2, false, false, fc_int8_avg_combine),
    ]);
}
