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

use mcx::MemoryContext;
use types_core::{Oid, OidIsValid};
use types_datum::array_build::ArrayBuildState;
use types_datum::datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

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
    fn new(element_type: Oid) -> Box<ArrayAggInternal> {
        let ctx: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("array_agg state")));
        // initArrayResult(element_type, rcontext, subcontext = false).
        let state = construct::init_array_result(element_type, false)
            .unwrap_or_else(|e| raise(e));
        Box::new(ArrayAggInternal { ctx, state })
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

/// `get_fn_expr_argtype(fcinfo->flinfo, i)` — the resolved (polymorphic) type
/// OID of argument `i`, read off the stamped `flinfo->fn_expr`.
#[inline]
fn fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData, i: i32) -> Oid {
    backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i)
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

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Set an `anyarray` (by-reference) result on the by-ref lane, header-stripped
/// (symmetric with how the array arg lane delivers `anyarray`). Returns the
/// dummy by-value word.
fn ret_array(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    // `makeMdArrayResult` returns a full varlena (4-byte header + ARR_* body).
    // The `RefPayload::Varlena` result lane carries the HEADER-LESS payload, which
    // `ref_out_to_datum` (`byref_element_ondisk_image`) re-stamps with a fresh
    // 4-byte `SET_VARSIZE` header into the canonical `ByRef` image. So strip the
    // header here (exactly as numeric's `ret_numeric` does) — handing the lane the
    // full header-ful image would double-stamp it and corrupt the array. The ARR
    // dims/dataoffset are relative to the array start, so the strip+restamp of an
    // identical 4-byte header preserves the layout.
    let off = types_datum::varlena::varhdrsz_of(&image);
    fcinfo.set_ref_result(RefPayload::Varlena(image[off..].to_vec()));
    Datum::from_usize(0)
}

// ===========================================================================
// array_agg_transfn(2333) / array_agg_finalfn(2334).
// ===========================================================================

/// `array_agg_transfn`(2333): accumulate one `anynonarray` element into the
/// running `ArrayBuildState`.
fn fc_array_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    let arg1_typeid = fn_expr_argtype(fcinfo, 1);
    if !OidIsValid(arg1_typeid) {
        raise(
            types_error::PgError::error("could not determine input data type")
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    // AggCheckCallContext: the leaked context inside ArrayAggInternal models the
    // aggcontext the by-OID dispatch cannot thread (see module docs).

    // state = PG_ARGISNULL(0) ? initArrayResult(arg1_typeid, aggcontext, false)
    //                         : (ArrayBuildState *) PG_GETARG_POINTER(0);
    let mut carrier =
        take_array_state(fcinfo).unwrap_or_else(|| ArrayAggInternal::new(arg1_typeid));

    // elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
    let disnull = arg_isnull(fcinfo, 1);
    let elem: Datum = if disnull {
        Datum::from_usize(0)
    } else {
        elem_datum(fcinfo, arg1_typeid)
    };

    // state = accumArrayResult(state, elem, PG_ARGISNULL(1), arg1_typeid,
    //                          aggcontext);
    let ctx_mcx = carrier.ctx.mcx();
    let new_state = ok(construct::accum_array_result(
        ctx_mcx,
        Some(core::mem::take(&mut carrier.state)),
        elem,
        disnull,
        arg1_typeid,
    ));
    carrier.state = new_state;

    // PG_RETURN_POINTER(state).
    ret_internal(fcinfo, carrier)
}

/// `PG_GETARG_DATUM(1)` for the element argument. A by-value element (the
/// resolved `arg1_typeid` is pass-by-value, e.g. `int4`) arrives in the by-value
/// word; a by-ref element arrives on the by-ref lane and is materialized into a
/// `Datum` whose word the `accumArrayResult` by-ref copy path consumes.
fn elem_datum(fcinfo: &FunctionCallInfoBaseData, _arg1_typeid: Oid) -> Datum {
    // The by-value word is always populated by the fmgr boundary for a by-value
    // type. (For a by-ref type the boundary delivers the payload on the by-ref
    // lane; `accumArrayResult`'s by-ref branch resolves it through the detoast
    // seam — the same substrate boundary every other by-ref element access in
    // this crate bottoms out on.)
    fcinfo
        .arg(1)
        .map(|d| d.value)
        .unwrap_or_else(|| Datum::from_usize(0))
}

/// `array_agg_finalfn`(2334): make a 1-D array of the accumulated elements.
fn fc_array_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
    match take_array_state(fcinfo) {
        // if (state == NULL) PG_RETURN_NULL();  (no input values)
        None => ret_null(fcinfo),
        Some(carrier) => {
            // dims[0] = state->nelems; lbs[0] = 1;
            // result = makeMdArrayResult(state, 1, dims, lbs,
            //                            CurrentMemoryContext, false);
            let m = MemoryContext::new("array_agg finalfn");
            let astate = &carrier.state;
            let ndims = if astate.nelems > 0 { 1 } else { 0 };
            let dims = [astate.nelems];
            let lbs = [1];
            let image =
                ok(construct::make_md_array_result(m.mcx(), astate, ndims, &dims, &lbs))
                    .as_slice()
                    .to_vec();
            ret_array(fcinfo, image)
        }
    }
}

// ===========================================================================
// Registration (C: their `fmgr_builtins[]` rows; transition/final functions are
// `proisstrict => 'f'` — they handle the NULL `internal` running state / NULL
// input themselves). `array_agg_finalfn` is declared `internal anynonarray`
// (the second arg is a polymorphism-resolution dummy nodeAgg pads with NULL).
// ===========================================================================

pub fn register_array_agg_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2333, "array_agg_transfn", 2, fc_array_agg_transfn),
        builtin(2334, "array_agg_finalfn", 2, fc_array_agg_finalfn),
    ]);
}

/// A non-strict (`proisstrict => 'f'`) builtin row.
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: false,
        retset: false,
        func: Some(func),
    }
}
