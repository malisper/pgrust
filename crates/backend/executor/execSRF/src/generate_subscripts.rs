//! `generate_subscripts(anyarray, int4 [, bool])` (OIDs 1191/1192) registered as
//! executor-frame set-returning functions.
//!
//! `arrayfuncs.c`'s `generate_subscripts` is a value-per-call SRF: it carries a
//! `generate_subscripts_fctx` (`{lower, upper, reverse}`) across SRF calls and
//! emits one `Int32GetDatum(lower++)` (or `upper--` when reversed) per call while
//! `lower <= upper`, terminating with `SRF_RETURN_DONE`. The subscript-range core
//! (the `AARR_NDIM`/dimension sanity checks and the bound computation) is ported
//! in `backend-utils-adt-arrayfuncs::sql::generate_subscripts`.
//!
//! Here that core is assembled into a [`nodes::execexpr::PGFunction`] (the
//! executor-frame ABI whose call frame carries the LIVE `ReturnSetInfo`) and
//! registered in this unit's executor-frame SRF table from
//! [`register_generate_subscripts`] (called by `init_seams`) — the executor-frame
//! `fmgrtab.c` analogue for these SRFs, exactly as `generate_series`/`unnest`
//! are. `SELECT generate_subscripts(ARRAY[1,2,3], 1)` reaches this via
//! nodeProjectSet → ExecMakeFunctionResultSet; `SELECT * FROM
//! generate_subscripts(...)` via nodeFunctionscan → ExecMakeTableFunctionResult.

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::PgResult;
use nodes::execexpr::ExprDoneCond;
use nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_tuple::heaptuple::Datum;

use funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `generate_subscripts(anyarray, int4, bool)` (OID 1191) and
/// `generate_subscripts(anyarray, int4)` (OID 1192).
const GENERATE_SUBSCRIPTS: Oid = 1191;
const GENERATE_SUBSCRIPTS_NODIR: Oid = 1192;

/// Register the `generate_subscripts` SRFs in the executor-frame SRF table.
pub(crate) fn register_generate_subscripts() {
    register_srf(GENERATE_SUBSCRIPTS, generate_subscripts);
    register_srf(GENERATE_SUBSCRIPTS_NODIR, generate_subscripts);
}

/// The materialized cross-call state for `generate_subscripts` (C:
/// `generate_subscripts_fctx`). The whole subscript range is produced once on the
/// first call (emission order — ascending, or descending when `reverse`), then
/// emitted one `int4` per call.
struct SubscriptsFctx {
    /// The subscripts in emission order.
    values: Vec<i32>,
    /// The next index to emit (C: `funcctx->call_cntr`).
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

/// `generate_subscripts(PG_FUNCTION_ARGS)` (arrayfuncs.c:5922) over the executor
/// frame. Drives the value-per-call protocol; `SRF_RETURN_NEXT` /
/// `SRF_RETURN_DONE` are the `isDone` writes + the multi-call teardown.
fn generate_subscripts<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("generate_subscripts: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { funcctx = SRF_FIRSTCALL_INIT(); ... }
    if fcinfo.fn_extra.is_none() {
        // C: v = PG_GETARG_ANY_ARRAY_P(0); reqdim = PG_GETARG_INT32(1);
        //    reverse = (PG_NARGS() == 3) ? PG_GETARG_BOOL(2) : false;
        // The array arrives header-ful on the by-ref side channel (a varlena
        // image; PG_GETARG_ARRAYTYPE_P); dim/reverse are by-value words.
        // Compute the whole subscript range once and copy it into the
        // lifetime-free cross-call state; the immutable borrow of `fcinfo` must
        // end before the mutable SRF setup calls, so it is scoped.
        let values: Vec<i32> = {
            let image = match fcinfo.ref_arg(0) {
                Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
                _ => panic!("generate_subscripts: array argument missing from by-ref lane"),
            };
            let dim = fcinfo.args[1].value.as_i32();
            let reverse = if fcinfo.nargs == 3 {
                fcinfo.args[2].value.as_usize() != 0
            } else {
                false
            };
            let materialized =
                arrayfuncs::sql::generate_subscripts(mcx, image, dim, reverse)?;
            materialized.iter().copied().collect()
        };

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, SubscriptsFctx { values, next: 0 });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP(); fctx = funcctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let state: &mut SubscriptsFctx = funcctx
        .user_fctx
        .as_mut()
        .expect("user_fctx present")
        .downcast_mut::<SubscriptsFctx>()
        .expect("user_fctx is SubscriptsFctx");

    // C: if (fctx->lower <= fctx->upper) { ... SRF_RETURN_NEXT ... }
    if state.next < state.values.len() {
        let result = state.values[state.next];
        state.next += 1;
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        Ok(Datum::from_i32(result))
    } else {
        // SRF_RETURN_DONE(funcctx).
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::from_i32(0))
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
