//! `unnest(anymultirange)` (OID 1293) registered as an executor-frame
//! set-returning function.
//!
//! `multirangetypes.c`'s `multirange_unnest` is a value-per-call SRF: it carries
//! the multirange + member index across SRF calls and emits
//! `multirange_get_range(rngtype, mr, index)` for `index` in `0 .. rangeCount`,
//! terminating with `SRF_RETURN_DONE`. The member-range materialization core (the
//! `multirange_get_typcache(...)->rngtype` resolution + the per-index
//! `multirange_get_range` walk, serializing each member to its on-disk
//! `RangeType` varlena image) is ported in
//! `backend-utils-adt-multirangetypes::operators::multirange_unnest_images`.
//!
//! Here that core is driven over the executor frame: the whole member-range
//! sequence is materialized once on the first call (mirroring C's per-call
//! ordering), and one `anyrange` Datum is emitted per call. Registered from
//! [`register_multirange_unnest`] (called by `init_seams`) — the executor-frame
//! `fmgrtab.c` analogue, exactly as `array_unnest` is. It bypasses the by-OID
//! builtin registry (whose `fmgr::PGFunction` frame's `resultinfo` is
//! tag-only — the WONTFIX dual-home) so the function reads/writes a live
//! `ReturnSetInfo`. `SELECT unnest('{[1,2],[5,6]}'::int4multirange)` reaches this
//! via nodeProjectSet → ExecMakeFunctionResultSet; `SELECT * FROM unnest(...)`
//! via nodeFunctionscan → ExecMakeTableFunctionResult.

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::PgResult;
use ::nodes::execexpr::ExprDoneCond;
use ::nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `unnest(anymultirange)` (OID 1293).
const MULTIRANGE_UNNEST: Oid = 1293;

/// Register `unnest(anymultirange)` in the executor-frame SRF table.
pub(crate) fn register_multirange_unnest() {
    register_srf(MULTIRANGE_UNNEST, multirange_unnest);
}

/// The materialized cross-call state for `multirange_unnest` (C: the multirange
/// handle + `funcctx->call_cntr`). The member-range sequence is serialized once
/// on the first call; one on-disk `RangeType` image is emitted per call.
struct MultirangeUnnestFctx {
    /// The member-range images (serialized `RangeType` varlenas) in order.
    ranges: Vec<Vec<u8>>,
    /// The next member index to emit (C: `funcctx->call_cntr`).
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

/// `multirange_unnest(PG_FUNCTION_ARGS)` (multirangetypes.c:2714) over the
/// executor frame. Drives the value-per-call protocol; `SRF_RETURN_NEXT` /
/// `SRF_RETURN_DONE` are the `isDone` writes + the multi-call teardown.
fn multirange_unnest<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("multirange_unnest: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { ... DatumGetMultirangeTypeP(0); typcache;
    //    funcctx->user_fctx = mr; funcctx->max_calls = mr->rangeCount; }
    if fcinfo.fn_extra.is_none() {
        let ranges: Vec<Vec<u8>> = {
            // arr = PG_GETARG_MULTIRANGE_P(0): the header-ful on-disk multirange
            // varlena image on the by-ref side channel.
            let image = match fcinfo.ref_arg(0) {
                Some(FmgrArgRef::Varlena(b)) => b.as_slice().to_vec(),
                _ => panic!("multirange_unnest: multirange argument missing from by-ref lane"),
            };
            multirangetypes::operators::multirange_unnest_images(mcx, &image)?
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, MultirangeUnnestFctx { ranges, next: 0 });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); ...
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut MultirangeUnnestFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<MultirangeUnnestFctx>()
        .expect("user_fctx is MultirangeUnnestFctx");

    // C: if (index < mr->rangeCount) { ... SRF_RETURN_NEXT(funcctx,
    //    RangeTypePGetDatum(range)); }
    if state.next < state.ranges.len() {
        let image = &state.ranges[state.next];
        let mut buf = mcx::PgVec::new_in(mcx);
        buf.try_reserve(image.len())
            .map_err(|_| mcx.oom(image.len()))?;
        buf.extend_from_slice(image.as_slice());

        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        Ok(Datum::ByRef(buf))
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
