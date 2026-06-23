//! `fmgr`-callable wrappers for the `internal`-transtype `array_agg` aggregate
//! (`array_userfuncs.c`): the scalar-element family `array_agg_transfn`(2333) /
//! `array_agg_finalfn`(2334), built on the `ArrayBuildState`-over-
//! `accumArrayResult` machinery already ported in [`crate::construct`].
//!
//! ## The `internal` transition state crosses the fmgr boundary
//!
//! C's transition state is a `void *` to an `ArrayBuildState` living in the
//! per-aggregate `MemoryContext`. Here it rides the canonical
//! `Datum::Internal(Box<dyn Any>)` arm (`RefPayload::Internal`): nodeAgg moves
//! the box in/out of the call frame, the transfn mutates it in place, and
//! returns the same box.
//!
//! ## `AggCheckCallContext` / `'mcx`-bound element copies (the frame-carrier)
//!
//! `array_agg_transfn` calls `AggCheckCallContext(fcinfo, &aggcontext)` and
//! `initArrayResult(arg1_typeid, aggcontext, false)` so the build state — and
//! every accumulated by-ref element copy — lives in the per-aggregate context,
//! not the per-tuple context. The by-OID transition dispatch does not thread the
//! fcinfo `(Node *) aggstate` context, so — exactly as the numeric internal-state
//! family does (`backend-utils-adt-numeric::agg_fmgr`) — [`ArrayAggInternal`]
//! owns a leaked `&'static MemoryContext` modeling that aggcontext, and every
//! `accumArrayResult` charges its element copies there. The `ArrayBuildState`'s
//! `dvalues`/`dnulls` columns are themselves global-allocator `Vec`s (so a
//! by-value element is fully self-contained), and the leaked context backs the
//! by-ref element copies (`PG_DETOAST_DATUM_COPY`). The leak models C's
//! aggcontext, whose `pfree`/reset nodeAgg owns (the by-ref free is a deferred
//! TODO across this repo).
//!
//! The element type `arg1_typeid` is `get_fn_expr_argtype(fcinfo->flinfo, 1)` —
//! the transfn's `flinfo->fn_expr` is the `build_aggregate_transfn_expr` call
//! node nodeAgg stamps (threaded through `function_call_invoke_datum_owned`), so
//! the polymorphic `anynonarray` input resolves to the concrete element type.

use ::mcx::MemoryContext;
use types_core::{Oid, OidIsValid};
use ::datum::array_build::ArrayBuildState;
use ::datum::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use crate::construct;

/// `(Node *) aggcontext`-charged `array_agg` build state. The leaked context
/// backs the state's by-ref element copies; the state is `'static` because that
/// context lives for the whole backend (C resets it per-group; we leak — the
/// repo-wide by-ref-free TODO).
struct ArrayAggInternal {
    /// The leaked per-aggregate `MemoryContext` (`&'static` so element copies
    /// outlive every transfn call without a borrow).
    ctx: &'static MemoryContext,
    /// The running `ArrayBuildState`, with element copies charged to `ctx`.
    state: ArrayBuildState,
}

impl ArrayAggInternal {
    /// `initArrayResult(element_type, aggcontext, false)` in a newly-leaked
    /// per-aggregate context.
    fn new(element_type: Oid) -> PgResult<Box<ArrayAggInternal>> {
        let ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("array_agg state")));
        // initArrayResult(element_type, rcontext, subcontext = false).
        let state = construct::init_array_result(element_type, false)?;
        Ok(Box::new(ArrayAggInternal { ctx, state }))
    }

    /// A carrier wrapping an already-built `state` (e.g. one produced by
    /// `array_agg_deserialize_state`), with a leaked agg context backing its
    /// by-ref element copies.
    fn from_state(state: ::datum::array_build::ArrayBuildState) -> Box<ArrayAggInternal> {
        let ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("array_agg state")));
        Box::new(ArrayAggInternal { ctx, state })
    }
}

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `get_fn_expr_argtype(fcinfo->flinfo, i)` — the resolved (polymorphic) type
/// OID of argument `i`, read off the stamped `flinfo->fn_expr`.
#[inline]
fn fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData, i: i32) -> Oid {
    fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i)
}

/// Take the `internal` transition state out of `args[0]`, downcast to the
/// concrete carrier. `None` is C's `PG_ARGISNULL(0)` (first call).
fn take_array_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<ArrayAggInternal>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(
            b.downcast::<ArrayAggInternal>().unwrap_or_else(|_| {
                panic!("array_agg fn: args[0] internal state is not an ArrayAggInternal")
            }),
        ),
        Some(other) => panic!("array_agg fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// `PG_RETURN_POINTER(state)` — hand the transition state back as `internal`.
fn ret_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    Datum::from_usize(0)
}

/// Restore an `internal` transition state into `args[0]` after a *final*
/// function read it. C's `array_agg_finalfn` reads its state with
/// `PG_GETARG_POINTER(0)` and builds the result with `makeMdArrayResult(...,
/// release = false)`: the read does NOT consume the running `ArrayBuildState`.
/// For a moving window frame (`ROWS n PRECEDING`) array_agg has no inverse
/// transfn, so eval_windowaggregates keeps adding to the same live state across
/// rows; finalizing one row must leave that state intact for the next row's
/// forward transition. The owned model carries the state as a move-only
/// `Box<dyn Any>` on `ref_args[0]`; `take_array_state` moved it out to downcast,
/// so put it back here so the owned-finalfn seam can hand it back to the
/// executor (otherwise the next row reinitializes from scratch and only the
/// newly-added element survives).
fn keep_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) {
    fcinfo.set_ref_arg(0, RefPayload::Internal(state));
}

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Set an `anyarray` (by-reference) result on the by-ref lane. Returns the
/// dummy by-value word.
fn ret_array(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    // `makeMdArrayResult` returns a full varlena (4-byte header + ARR_* body).
    // Under the header-ful-everywhere convention the `RefPayload::Varlena` result
    // lane carries that complete on-disk image verbatim (`ref_out_to_datum` no
    // longer strips or re-stamps), so hand it back unchanged.
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

// ===========================================================================
// array_agg_transfn(2333) / array_agg_finalfn(2334).
// ===========================================================================

/// `array_agg_transfn`(2333): accumulate one `anynonarray` element into the
/// running `ArrayBuildState`.
fn fc_array_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    let arg1_typeid = fn_expr_argtype(fcinfo, 1);
    if !OidIsValid(arg1_typeid) {
        return Err(::types_error::PgError::error("could not determine input data type")
            .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // AggCheckCallContext: the leaked context inside ArrayAggInternal models the
    // aggcontext the by-OID dispatch cannot thread (see module docs).

    // state = PG_ARGISNULL(0) ? initArrayResult(arg1_typeid, aggcontext, false)
    //                         : (ArrayBuildState *) PG_GETARG_POINTER(0);
    let mut carrier = match take_array_state(fcinfo) {
        Some(c) => c,
        None => ArrayAggInternal::new(arg1_typeid)?,
    };

    // elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
    let disnull = arg_isnull(fcinfo, 1);
    let ctx_mcx = carrier.ctx.mcx();
    let elem: Datum = if disnull {
        Datum::from_usize(0)
    } else {
        elem_datum(fcinfo, &carrier.state, ctx_mcx)?
    };

    // state = accumArrayResult(state, elem, PG_ARGISNULL(1), arg1_typeid,
    //                          aggcontext);
    let new_state = construct::accum_array_result(
        ctx_mcx,
        Some(core::mem::take(&mut carrier.state)),
        elem,
        disnull,
        arg1_typeid,
    )?;
    carrier.state = new_state;

    // PG_RETURN_POINTER(state).
    Ok(ret_internal(fcinfo, carrier))
}

/// `PG_GETARG_DATUM(1)` for the element argument. A by-value element (the
/// resolved `arg1_typeid` is pass-by-value, e.g. `int4`) arrives in the by-value
/// word; a by-ref element (`text`/`numeric`/`name`/…) arrives on the fmgr
/// by-reference lane (`fcinfo->args[1]` payload rides `FmgrArgRef`, not the bare
/// word) and is materialized into a `Datum` whose pointer word `accumArrayResult`'s
/// by-ref copy path (`PG_DETOAST_DATUM_COPY` / `datumCopy`) consumes.
fn elem_datum<'mcx>(
    fcinfo: &FunctionCallInfoBaseData,
    state: &::datum::array_build::ArrayBuildState,
    ctx_mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<Datum> {
    // For a by-value element, the boundary populates the by-value word.
    if state.typbyval {
        return Ok(fcinfo
            .arg(1)
            .map(|d| d.value)
            .unwrap_or_else(|| Datum::from_usize(0)));
    }
    // For a by-ref element, C's `PG_GETARG_DATUM(1)` yields a real pointer into
    // the argument image. The bare by-value word here is NOT that pointer (it is
    // unset/garbage for a by-ref arg); the payload rides the by-reference lane.
    // Copy the verbatim element image into the aggcontext and hand back a Datum
    // pointing at it, so `accumArrayResult`'s by-ref deref has a live pointer.
    match fcinfo.ref_arg(1) {
        // Varlena (`typlen == -1`) and fixed-length by-ref (`typlen > 0`) images
        // ride the `Varlena` lane verbatim — copy them as-is.
        Some(::fmgr::boundary::RefPayload::Varlena(b)) => {
            construct::byref_image_to_datum(ctx_mcx, b.as_slice())
        }
        // A composite element (`record`/row type) is a varlena-framed
        // `HeapTupleHeader` image; copy its flat bytes verbatim like a varlena.
        Some(::fmgr::boundary::RefPayload::Composite(b)) => {
            construct::byref_image_to_datum(ctx_mcx, b.as_slice())
        }
        // `cstring` (`typlen == -2`) elements: `accumArrayResult`'s by-ref copy
        // reads a NUL-terminated image, so append the terminator the `Cstring`
        // lane drops.
        Some(::fmgr::boundary::RefPayload::Cstring(s)) => {
            let mut img = s.clone().into_bytes();
            img.push(0);
            construct::byref_image_to_datum(ctx_mcx, &img)
        }
        // No by-ref payload seeded: same diagnostic the other by-ref accessors use.
        _ => Err(::types_error::PgError::error(
            "array_agg_transfn: arg 1 has no by-reference payload on the call frame \
             (the dispatcher did not seed ref_args[1] for a by-ref element)",
        )),
    }
}

/// `array_agg_finalfn`(2334): make a 1-D array of the accumulated elements.
fn fc_array_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
    match take_array_state(fcinfo) {
        // if (state == NULL) PG_RETURN_NULL();  (no input values)
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            // dims[0] = state->nelems; lbs[0] = 1;
            // result = makeMdArrayResult(state, 1, dims, lbs,
            //                            CurrentMemoryContext, false);
            let m = MemoryContext::new("array_agg finalfn");
            let astate = &carrier.state;
            let ndims = if astate.nelems > 0 { 1 } else { 0 };
            let dims = [astate.nelems];
            let lbs = [1];
            let image = construct::make_md_array_result(m.mcx(), astate, ndims, &dims, &lbs)?
                .as_slice()
                .to_vec();
            // makeMdArrayResult(..., release = false): the read is non-destructive,
            // so restore the running state for the next (window-frame) row.
            keep_internal(fcinfo, carrier);
            Ok(ret_array(fcinfo, image))
        }
    }
}

// ===========================================================================
// array_agg_array_transfn(4051) / array_agg_array_finalfn(4052).
//
// The `anyarray`-input variant: each input is a whole sub-array, accumulated
// into an `ArrayBuildStateArr` (an (n+1)-dimensional array whose first
// dimension counts the inputs). Mirrors `array_agg_array_transfn` /
// `array_agg_array_finalfn` (array_userfuncs.c), built on the
// `initArrayResultArr`/`accumArrayResultArr`/`makeArrayResultArr` machinery
// already ported in [`crate::construct`].
// ===========================================================================

/// `(Node *) aggcontext`-charged `array_agg(anyarray)` build state. Like
/// [`ArrayAggInternal`], the leaked context backs the state's by-ref sub-array
/// copies; the state is `'static` for the same reason (per-backend leaked
/// aggcontext, repo-wide by-ref-free TODO).
struct ArrayAggArrInternal {
    ctx: &'static MemoryContext,
    state: ::datum::array_build::ArrayBuildStateArr,
}

/// Take the `internal` `ArrayBuildStateArr` transition state out of `args[0]`.
fn take_array_arr_state(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Option<Box<ArrayAggArrInternal>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<ArrayAggArrInternal>().unwrap_or_else(
            |_| panic!("array_agg_array fn: args[0] internal state is not an ArrayAggArrInternal"),
        )),
        Some(other) => {
            panic!("array_agg_array fn: args[0] is not an internal state ({other:?})")
        }
        None => None,
    }
}

/// `array_agg_array_transfn`(4051): accumulate one `anyarray` sub-array into the
/// running `ArrayBuildStateArr`.
fn fc_array_agg_array_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);  (the array type)
    let arg1_typeid = fn_expr_argtype(fcinfo, 1);
    if !OidIsValid(arg1_typeid) {
        return Err(::types_error::PgError::error("could not determine input data type")
            .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // AggCheckCallContext: the leaked context inside ArrayAggArrInternal models
    // the aggcontext the by-OID dispatch cannot thread (see module docs).
    let mut carrier = match take_array_arr_state(fcinfo) {
        Some(c) => c,
        None => {
            let ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("array_agg_array state")));
            // initArrayResultArr(arg1_typeid, InvalidOid, aggcontext, false).
            let state = construct::init_array_result_arr(arg1_typeid, 0, false)?;
            Box::new(ArrayAggArrInternal { ctx, state })
        }
    };

    let ctx_mcx = carrier.ctx.mcx();
    let disnull = arg_isnull(fcinfo, 1);

    // PG_GETARG_DATUM(1): the input sub-array. accumArrayResultArr disallows a
    // NULL sub-array; on a NULL arg it raises before dereferencing, so the
    // pointer-word need only be live when the value is non-NULL. A non-NULL
    // anyarray rides the by-ref Varlena lane; materialize a pointer-word into a
    // held buffer that outlives the accum call (mirroring C's bare-pointer
    // Datum into the argument image, which accumArrayResultArr detoasts/copies).
    let (dvalue, _held): (Datum, Option<Vec<u8>>) = if disnull {
        (Datum::from_usize(0), None)
    } else {
        let bytes = match fcinfo.ref_arg(1) {
            Some(RefPayload::Varlena(b)) => b.as_slice().to_vec(),
            _ => {
                return Err(::types_error::PgError::error(
                    "array_agg_array_transfn: arg 1 has no by-reference payload on the call frame \
                     (the dispatcher did not seed ref_args[1] for an anyarray element)",
                ))
            }
        };
        let ptr = bytes.as_ptr() as usize;
        (Datum::from_usize(ptr), Some(bytes))
    };

    // state = accumArrayResultArr(state, PG_GETARG_DATUM(1), PG_ARGISNULL(1),
    //                             arg1_typeid, aggcontext);
    let new_state = construct::accum_array_result_arr(
        ctx_mcx,
        Some(core::mem::take(&mut carrier.state)),
        dvalue,
        disnull,
        arg1_typeid,
    )?;
    carrier.state = new_state;

    Ok(ret_internal(fcinfo, carrier))
}

/// `array_agg_array_finalfn`(4052): finalize the accumulated (n+1)-D array.
fn fc_array_agg_array_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    match take_array_arr_state(fcinfo) {
        // returns null iff no input values
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            // result = makeArrayResultArr(state, CurrentMemoryContext, false);
            let m = MemoryContext::new("array_agg_array finalfn");
            let image = construct::make_array_result_arr(m.mcx(), &carrier.state)?
                .as_slice()
                .to_vec();
            // makeArrayResultArr(..., release = false): non-destructive read, so
            // restore the running state for the next (window-frame) row.
            keep_internal(fcinfo, carrier);
            Ok(ret_array(fcinfo, image))
        }
    }
}

/// Take the `internal` `ArrayBuildStateArr` transition state out of `args[i]`.
fn take_array_arr_state_at(
    fcinfo: &mut FunctionCallInfoBaseData,
    i: usize,
) -> Option<Box<ArrayAggArrInternal>> {
    if arg_isnull(fcinfo, i) {
        return None;
    }
    match fcinfo.take_ref_arg(i) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<ArrayAggArrInternal>().unwrap_or_else(
            |_| panic!("array_agg_array fn: args[{i}] internal state is not an ArrayAggArrInternal"),
        )),
        Some(other) => panic!("array_agg_array fn: args[{i}] is not an internal state ({other:?})"),
        None => None,
    }
}

/// A fresh `ArrayAggArrInternal` carrier with a leaked agg context whose `state`
/// is overwritten by a combine/deserialize result.
fn new_arr_carrier(
    state: ::datum::array_build::ArrayBuildStateArr,
) -> Box<ArrayAggArrInternal> {
    let ctx: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("array_agg_array state")));
    Box::new(ArrayAggArrInternal { ctx, state })
}

/// `array_agg_array_combine`(6296): concatenate `state2`'s items onto `state1`.
/// `proisstrict => 'f'`. Mirrors `array_agg_array_combine`.
fn fc_array_agg_array_combine(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let state1 = take_array_arr_state_at(fcinfo, 0);
    let state2 = take_array_arr_state_at(fcinfo, 1);

    let state2 = match state2 {
        None => {
            return Ok(match state1 {
                None => ret_null(fcinfo),
                Some(s1) => ret_internal(fcinfo, s1),
            });
        }
        Some(s2) => s2,
    };

    let combined = match state1 {
        // state1 == NULL: copy state2's data into a fresh agg-context state.
        None => {
            let s2 = &state2.state;
            let copy = ::datum::array_build::ArrayBuildStateArr {
                data: s2.data.clone(),
                nullbitmap: s2.nullbitmap.clone(),
                nbytes: s2.nbytes,
                nitems: s2.nitems,
                ndims: s2.ndims,
                dims: s2.dims,
                lbs: s2.lbs,
                array_type: s2.array_type,
                element_type: s2.element_type,
                private_cxt: false,
            };
            new_arr_carrier(copy)
        }
        Some(mut s1) => {
            if state2.state.nitems > 0 {
                combine_arr_states(&mut s1.state, &state2.state)?;
            }
            s1
        }
    };

    Ok(ret_internal(fcinfo, combined))
}

/// Append `src`'s items onto `dst` (the `state2->nitems > 0` branch of
/// `array_agg_array_combine`), checking dimensional compatibility.
fn combine_arr_states(
    dst: &mut ::datum::array_build::ArrayBuildStateArr,
    src: &::datum::array_build::ArrayBuildStateArr,
) -> PgResult<()> {
    // Check the states are compatible (same dims ignoring the first).
    let dim_err = || {
        ::types_error::PgError::error("cannot accumulate arrays of different dimensionality")
            .with_sqlstate(::types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR)
    };
    if dst.ndims != src.ndims {
        return Err(dim_err());
    }
    for i in 1..dst.ndims as usize {
        if dst.dims[i] != src.dims[i] || dst.lbs[i] != src.lbs[i] {
            return Err(dim_err());
        }
    }

    // nullbitmap handling: if src has nulls, ensure dst has a bitmap covering
    // its existing (non-null) items, then copy src's bits in.
    if src.nullbitmap.is_some() {
        let newnitems = dst.nitems + src.nitems;
        if dst.nullbitmap.is_none() {
            // First input with nulls: previous dst items are all non-null.
            let aitems = (newnitems + 1).max(256);
            let size = ((aitems as usize) + 7) / 8;
            // All-zero except we set the existing dst.nitems bits to non-null
            // (1). array_bitmap_copy(dst, 0, NULL, 0, nitems) sets them to 1.
            let mut bm = vec![0u8; size];
            for i in 0..dst.nitems as usize {
                bm[i / 8] |= 1u8 << (i % 8);
            }
            dst.nullbitmap = Some(bm);
        }
        // Ensure capacity for newnitems bits.
        let needed = ((newnitems as usize) + 7) / 8;
        if let Some(bm) = dst.nullbitmap.as_mut() {
            if bm.len() < needed {
                bm.resize(needed, 0);
            }
        }
        // array_bitmap_copy(dst, dst.nitems, src, 0, src.nitems).
        let srcbm = src.nullbitmap.as_ref().unwrap();
        let dstbm = dst.nullbitmap.as_mut().unwrap();
        for i in 0..src.nitems as usize {
            let srcbit = (srcbm[i / 8] >> (i % 8)) & 1;
            let dstidx = dst.nitems as usize + i;
            if srcbit != 0 {
                dstbm[dstidx / 8] |= 1u8 << (dstidx % 8);
            } else {
                dstbm[dstidx / 8] &= !(1u8 << (dstidx % 8));
            }
        }
    }

    // Append data bytes.
    dst.data.extend_from_slice(&src.data[..src.nbytes as usize]);
    dst.nbytes += src.nbytes;
    dst.nitems += src.nitems;
    dst.dims[0] += src.dims[0];
    Ok(())
}

/// `array_agg_array_serialize`(6297): serialize the running `ArrayBuildStateArr`.
fn fc_array_agg_array_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let carrier = take_array_arr_state(fcinfo)
        .expect("array_agg_array_serialize: NULL internal state (strict aggregate)");
    let m = MemoryContext::new("array_agg_array serialize");
    let payload = crate::io::array_agg_array_serialize_state(m.mcx(), &carrier.state)?
        .as_slice()
        .to_vec();
    keep_internal(fcinfo, carrier);
    Ok(ret_bytea(fcinfo, payload))
}

/// `array_agg_array_deserialize`(6298): rebuild an `ArrayBuildStateArr`.
fn fc_array_agg_array_deserialize(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let body = arg_bytea(fcinfo, 0);
    let m = MemoryContext::new("array_agg_array deserialize");
    let state = crate::io::array_agg_array_deserialize_state(m.mcx(), body)?;
    Ok(ret_internal(fcinfo, new_arr_carrier(state)))
}

// ===========================================================================
// array_agg_combine(6293) / array_agg_serialize(6294) / array_agg_deserialize(6295).
//
// The parallel-aggregation half of scalar `array_agg`: a parallel worker
// accumulates its own `ArrayBuildState`, `array_agg_serialize`s it to a `bytea`,
// ships it to the leader, which `array_agg_deserialize`s it back and
// `array_agg_combine`s the worker states into one. Mirrors `array_userfuncs.c`.
// ===========================================================================

/// `array_agg_combine`(6293): concatenate `state2`'s elements onto `state1`.
/// `proisstrict => 'f'`. C: if state2 is NULL return state1; if state1 is NULL
/// copy state2's elements into the agg context; else append.
fn fc_array_agg_combine(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let state1 = take_array_state(fcinfo);
    let state2 = take_array_state_at(fcinfo, 1);

    // if (state2 == NULL) { if (state1==NULL) PG_RETURN_NULL(); PG_RETURN_POINTER(state1); }
    let state2 = match state2 {
        None => {
            return Ok(match state1 {
                None => ret_null(fcinfo),
                Some(s1) => ret_internal(fcinfo, s1),
            });
        }
        Some(s2) => s2,
    };

    let combined = match state1 {
        // state1 == NULL: copy state2's data into a fresh agg-context state.
        None => {
            let mut s1 = ArrayAggInternal::new(state2.state.element_type)?;
            append_state(&mut s1.state, &state2.state);
            s1
        }
        // else if (state2->nelems > 0): append state2's elements onto state1.
        Some(mut s1) => {
            if state2.state.nelems > 0 {
                debug_assert_eq!(s1.state.element_type, state2.state.element_type);
                append_state(&mut s1.state, &state2.state);
            }
            s1
        }
    };

    Ok(ret_internal(fcinfo, combined))
}

/// Append all of `src`'s elements (values + nulls + by-ref backing) onto `dst`.
/// `dst` and `src` share the same element type. For by-ref types the backing
/// boxes are cloned so `dst` owns stable storage and `dst.dvalues` point at it.
fn append_state(
    dst: &mut ::datum::array_build::ArrayBuildState,
    src: &::datum::array_build::ArrayBuildState,
) {
    if src.nelems == 0 {
        return;
    }
    // Carry over the storage metadata (a NULL state1 was just initialized).
    dst.typlen = src.typlen;
    dst.typbyval = src.typbyval;
    dst.typalign = src.typalign;

    let mut src_store = 0usize;
    for i in 0..src.nelems as usize {
        if src.dnulls[i] {
            dst.dvalues.push(Datum::null());
            dst.dnulls.push(true);
        } else if src.typbyval {
            dst.dvalues.push(src.dvalues[i]);
            dst.dnulls.push(false);
        } else {
            // Clone the by-ref backing box so dst owns it; point dvalues at it.
            let copy = src.byref_storage[src_store].clone();
            src_store += 1;
            let dword = Datum::from_usize(copy.as_ptr() as usize);
            dst.byref_storage.push(copy);
            dst.dvalues.push(dword);
            dst.dnulls.push(false);
        }
    }
    dst.nelems += src.nelems;
}

/// `array_agg_serialize`(6294): serialize the running `ArrayBuildState` into a
/// `bytea`. Strict (the running `internal` state is never NULL here).
fn fc_array_agg_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let carrier = take_array_state(fcinfo)
        .expect("array_agg_serialize: NULL internal state (strict aggregate)");
    let m = MemoryContext::new("array_agg serialize");
    let payload = crate::io::array_agg_serialize_state(m.mcx(), &carrier.state)?
        .as_slice()
        .to_vec();
    // C `PG_GETARG_POINTER(0)` does not consume the state; restore it.
    keep_internal(fcinfo, carrier);
    Ok(ret_bytea(fcinfo, payload))
}

/// `array_agg_deserialize`(6295): rebuild an `ArrayBuildState` from a `bytea`.
/// Strict.
fn fc_array_agg_deserialize(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let body = arg_bytea(fcinfo, 0);
    let m = MemoryContext::new("array_agg deserialize");
    let state = crate::io::array_agg_deserialize_state(m.mcx(), body)?;
    Ok(ret_internal(fcinfo, ArrayAggInternal::from_state(state)))
}

/// `PG_GETARG_BYTEA_PP(i)` body bytes (varlena header stripped). The serialized
/// state rides the by-ref `Varlena` lane.
fn arg_bytea<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("array_agg_deserialize: by-ref `bytea` arg missing from by-ref lane");
    if image.len() >= ::datum::varlena::VARHDRSZ {
        &image[::datum::varlena::VARHDRSZ..]
    } else {
        &[]
    }
}

/// `PG_RETURN_BYTEA_P(image)` — a by-ref `bytea` result with 4-byte varlena
/// framing.
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let mut image = Vec::with_capacity(payload.len() + ::datum::varlena::VARHDRSZ);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(
        payload.len() + ::datum::varlena::VARHDRSZ,
    ));
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Take the `internal` `ArrayAggInternal` transition state out of `args[i]`.
fn take_array_state_at(
    fcinfo: &mut FunctionCallInfoBaseData,
    i: usize,
) -> Option<Box<ArrayAggInternal>> {
    if arg_isnull(fcinfo, i) {
        return None;
    }
    match fcinfo.take_ref_arg(i) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<ArrayAggInternal>().unwrap_or_else(
            |_| panic!("array_agg fn: args[{i}] internal state is not an ArrayAggInternal"),
        )),
        Some(other) => panic!("array_agg fn: args[{i}] is not an internal state ({other:?})"),
        None => None,
    }
}

// Registration (C: their `fmgr_builtins[]` rows; transition/final functions are
// `proisstrict => 'f'` — they handle the NULL `internal` running state / NULL
// input themselves). `array_agg_finalfn` is declared `internal anynonarray`
// (the second arg is a polymorphism-resolution dummy nodeAgg pads with NULL).
// ===========================================================================

pub fn register_array_agg_builtins() {
    fmgr_core::register_builtins_native([
        builtin(2333, "array_agg_transfn", 2, fc_array_agg_transfn),
        builtin(2334, "array_agg_finalfn", 2, fc_array_agg_finalfn),
        builtin(4051, "array_agg_array_transfn", 2, fc_array_agg_array_transfn),
        builtin(4052, "array_agg_array_finalfn", 2, fc_array_agg_array_finalfn),
        builtin(6293, "array_agg_combine", 2, fc_array_agg_combine),
        builtin(6296, "array_agg_array_combine", 2, fc_array_agg_array_combine),
    ]);
    fmgr_core::register_builtins_native([
        builtin_strict(6294, "array_agg_serialize", 1, fc_array_agg_serialize),
        builtin_strict(6295, "array_agg_deserialize", 2, fc_array_agg_deserialize),
        builtin_strict(6297, "array_agg_array_serialize", 1, fc_array_agg_array_serialize),
        builtin_strict(6298, "array_agg_array_deserialize", 2, fc_array_agg_array_deserialize),
    ]);
}

/// A strict (`proisstrict => 't'`) native builtin row.
fn builtin_strict(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// A non-strict (`proisstrict => 'f'`) native builtin row (`func: None`; the
/// dispatch goes through the `NATIVE` overlay and threads `Err` with `?`).
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: false,
            retset: false,
            func: None,
        },
        native,
    )
}
