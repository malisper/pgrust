//! `unnest(anyarray)` (OID 2331) registered as an executor-frame set-returning
//! function.
//!
//! `arrayfuncs.c`'s `array_unnest` is a value-per-call SRF: it carries an
//! `array_unnest_fctx` (an `array_iter` cursor + `nextelem`/`numelems` and the
//! element storage triple) across SRF calls and emits one element per call via
//! `array_iter_next`, terminating with `SRF_RETURN_DONE` when exhausted. The
//! element deconstruction core (the flat-array `array_iter_setup` +
//! `array_iter_next` walk, NULL-bitmap handling, by-value vs by-ref element
//! window) is ported in `backend-utils-adt-arrayfuncs::array_unnest`.
//!
//! Here that core is assembled into a [`nodes::execexpr::PGFunction`] (the
//! executor-frame ABI whose call frame carries the LIVE `ReturnSetInfo`) and
//! registered in this unit's executor-frame SRF table from
//! [`register_unnest`] (called by `init_seams`) — the executor-frame `fmgrtab.c`
//! analogue for `unnest`, exactly as `generate_series` is. It bypasses the
//! by-OID builtin registry (whose `fmgr::PGFunction` frame's `resultinfo`
//! is tag-only — the WONTFIX dual-home) so the function reads/writes a live
//! `ReturnSetInfo`. `SELECT unnest(ARRAY[1,2,3])` reaches this via
//! nodeProjectSet → ExecMakeFunctionResultSet; `SELECT * FROM unnest(...)` via
//! nodeFunctionscan → ExecMakeTableFunctionResult.
//!
//! The owned model materializes the whole element sequence in storage order on
//! the first call (mirroring C's per-call ordering and NULL flags), then emits
//! one per call. A by-reference element (text[], etc.) crosses as a header-ful
//! `Datum::ByRef` image — the element window as stored in the array buffer,
//! varlena header included — exactly the pointer C's `array_iter_next` returns.

use core::any::Any;

use mcx::{Mcx, PgBox};
use array::ArrayElementDatum;
use types_core::Oid;
use types_error::PgResult;
use nodes::execexpr::ExprDoneCond;
use nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::heaptuple::Datum;

use funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `unnest(anyarray)` (OID 2331).
const UNNEST: Oid = 2331;

/// Register `unnest` in the executor-frame SRF table.
pub(crate) fn register_unnest() {
    register_srf(UNNEST, unnest);
}

/// One materialized `unnest` element, stored in a `'static` (lifetime-free)
/// shape so the cross-call state can live behind the `dyn Any` (`Any: 'static`)
/// `user_fctx` carrier — the emitted `Datum<'mcx>` is rebuilt from it per call
/// (in the multi-call `Mcx`).
enum UnnestElem {
    /// A NULL element (C: `array_iter_next` returned `isNull`).
    Null,
    /// A pass-by-value element: the bare Datum word.
    ByValue(usize),
    /// A pass-by-reference element: the header-ful on-disk varlena image.
    ByRef(Vec<u8>),
}

/// The materialized cross-call state for `unnest` (C: `array_unnest_fctx` +
/// `funcctx->call_cntr`/`max_calls`). The whole element sequence is deconstructed
/// once on the first call (storage order, NULL flags preserved) and emitted one
/// element per call.
struct UnnestFctx {
    /// The elements in array storage order.
    elems: Vec<UnnestElem>,
    /// The next element index to emit (C: `funcctx->call_cntr`).
    next: usize,
}

/// Erase a `'static` cross-call state value into the `FuncCallContext.user_fctx`
/// carrier (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// `array_unnest(PG_FUNCTION_ARGS)` (arrayfuncs.c:6259) over the executor frame.
/// Drives the value-per-call protocol; `SRF_RETURN_NEXT` / `SRF_RETURN_DONE` are
/// the `isDone` writes + the multi-call teardown.
fn unnest<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("unnest: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        // C: arr = PG_GETARG_ANY_ARRAY_P(0). The array arrives header-ful on the
        // by-ref side channel (a varlena image; PG_GETARG_ARRAYTYPE_P).
        //
        // Deconstruct the whole array once (array_iter_setup + the per-call
        // array_iter_next walk), copying each element into the long-lived
        // multi-call context so the emitted Datums outlive the array-image
        // borrow of `fcinfo` (which must end before the mutable SRF calls below)
        // and the per-call argContext resets.
        // Deconstruct the whole array once (array_iter_setup + the per-call
        // array_iter_next walk), copying each element into a lifetime-free shape
        // (a bare word or an owned header-ful image) so the cross-call state can
        // live behind the `dyn Any` user_fctx carrier across the row series. The
        // immutable borrow of `fcinfo` (the array image + the borrowed element
        // windows) must end before the mutable SRF setup calls, so it is scoped.
        let elems: Vec<UnnestElem> = {
            // C: arr = PG_GETARG_ANY_ARRAY_P(0). The array arrives header-ful on
            // the by-ref side channel (a varlena image; PG_GETARG_ARRAYTYPE_P).
            let image = match fcinfo.ref_arg(0) {
                Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
                _ => panic!("unnest: array argument missing from by-ref lane"),
            };
            let materialized =
                arrayfuncs::sql::array_unnest(mcx, image)?;

            let mut elems: Vec<UnnestElem> = Vec::with_capacity(materialized.len());
            for (elem, isnull) in materialized.iter() {
                if *isnull {
                    elems.push(UnnestElem::Null);
                } else {
                    elems.push(match elem {
                        ArrayElementDatum::ByValue(v) => UnnestElem::ByValue(v.as_usize()),
                        // The header-ful on-disk window, copied to own bytes.
                        ArrayElementDatum::ByRef(window) => UnnestElem::ByRef(window.to_vec()),
                    });
                }
            }
            elems
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, UnnestFctx { elems, next: 0 });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut UnnestFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<UnnestFctx>()
        .expect("user_fctx is UnnestFctx");

    // C: if (funcctx->call_cntr < funcctx->max_calls) { ... SRF_RETURN_NEXT ... }
    if state.next < state.elems.len() {
        // Rebuild the emitted Datum in the multi-call Mcx from the lifetime-free
        // stored element (the by-ref image is copied into `mcx`, exactly as C's
        // per-call value lives in the multi-call context).
        let (value, isnull): (Datum<'mcx>, bool) = match &state.elems[state.next] {
            UnnestElem::Null => (Datum::null(), true),
            UnnestElem::ByValue(word) => (Datum::from_usize(*word), false),
            UnnestElem::ByRef(image) => {
                let mut buf = mcx::PgVec::new_in(mcx);
                buf.try_reserve(image.len())
                    .map_err(|_| mcx.oom(image.len()))?;
                buf.extend_from_slice(image);
                (Datum::ByRef(buf), false)
            }
        };
        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = isnull;
        Ok(value)
    } else {
        // SRF_RETURN_DONE(funcctx).
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::null())
    }
}

/// `rsi->isDone = cond` (the `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` write onto the
/// live `ReturnSetInfo` the executor frame carries).
fn set_isdone(fcinfo: &mut FunctionCallInfoBaseData<'_>, cond: ExprDoneCond) {
    fcinfo
        .resultinfo
        .as_mut()
        .expect("resultinfo present for an SRF call")
        .isDone = cond;
}
