//! `pg_stat_get_backend_idset()` (OID 1936) registered as an executor-frame
//! value-per-call set-returning function — the set of proc numbers of all
//! backends known to the cumulative stats system (pgstatfuncs.c:204).
//!
//! C's `pg_stat_get_backend_idset` walks `fctx[0] = 1..=pgstat_fetch_stat_
//! numbackends()`, emitting `Int32GetDatum(pgstat_get_local_beentry_by_index
//! (fctx[0])->proc_number)` via `SRF_RETURN_NEXT` and `SRF_RETURN_DONE` at the
//! end. The value sequence (the per-index `proc_number`) is resolved here on the
//! first call into a Vec stashed in `funcctx->user_fctx` and walked by index;
//! `int4` is a by-value type. Registered from [`register_pg_stat_get_backend_idset`]
//! (called by `init_seams`); it bypasses the by-OID builtin registry whose
//! tag-only `resultinfo` cannot carry the live `ReturnSetInfo`.

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::execexpr::ExprDoneCond;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_activity_status as status;
use backend_utils_fmgr_funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `pg_stat_get_backend_idset()` (OID 1936).
const PG_STAT_GET_BACKEND_IDSET: Oid = 1936;

/// Register `pg_stat_get_backend_idset` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_backend_idset() {
    register_srf(PG_STAT_GET_BACKEND_IDSET, pg_stat_get_backend_idset);
}

/// `funcctx->isDone` write (the `SRF_RETURN_NEXT`/`SRF_RETURN_DONE` `isDone`
/// side-effect).
fn set_isdone(fcinfo: &mut FunctionCallInfoBaseData<'_>, cond: ExprDoneCond) {
    fcinfo
        .resultinfo
        .as_mut()
        .expect("resultinfo present for an SRF call")
        .isDone = cond;
}

/// Cross-call state: the resolved per-index `proc_number` sequence.
struct IdsetFctx {
    proc_numbers: Vec<i32>,
}

/// Erase a typed cross-call state into the `FuncCallContext.user_fctx` carrier.
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// `pg_stat_get_backend_idset(PG_FUNCTION_ARGS)` (pgstatfuncs.c:204) over the
/// executor frame.
fn pg_stat_get_backend_idset<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_backend_idset: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { ... fctx[0] = 0; }
    //
    // C rechecks pgstat_fetch_stat_numbackends() each call and walks one index
    // per call. Resolving the proc_number sequence once on the first call into
    // `user_fctx` is equivalent for the value-per-call walk (and matches the
    // already-snapshotted localBackendStatusTable, which is fetched once).
    if fcinfo.fn_extra.is_none() {
        let num_backends = status::pgstat_fetch_stat_numbackends();
        let mut proc_numbers = Vec::with_capacity(num_backends.max(0) as usize);
        for idx in 1..=num_backends {
            if let Some(local_beentry) = status::pgstat_get_local_beentry_by_index(idx) {
                // Int32GetDatum(local_beentry->proc_number).
                proc_numbers.push(local_beentry.proc_number);
            }
        }

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, IdsetFctx { proc_numbers });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: funcctx = SRF_PERCALL_SETUP();
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let call_cntr = funcctx.call_cntr as usize;
    let state: &IdsetFctx = funcctx
        .user_fctx
        .as_ref()
        .expect("user_fctx present")
        .downcast_ref::<IdsetFctx>()
        .expect("user_fctx is IdsetFctx");

    // C: if (fctx[0] <= pgstat_fetch_stat_numbackends()) SRF_RETURN_NEXT(...);
    if call_cntr < state.proc_numbers.len() {
        let value = state.proc_numbers[call_cntr];
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        Ok(Datum::from_i32(value))
    } else {
        // SRF_RETURN_DONE.
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::null())
    }
}
