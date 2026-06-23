//! `pg_snapshot_xip(pg_snapshot)` (OID 5064) registered as an executor-frame
//! value-per-call set-returning function.
//!
//! `xid8funcs.c`'s `pg_snapshot_xip` is a value-per-call SRF returning `setof
//! xid8`: on the first call it copies the user snapshot into the
//! `multi_call_memory_ctx`, then emits `snap->xip[call_cntr]` for each
//! `call_cntr < snap->nxip` via `SRF_RETURN_NEXT(FullTransactionIdGetDatum(...))`
//! and `SRF_RETURN_DONE` afterwards. The value sequence (the snapshot's
//! in-progress `xip[]`) is ported in
//! [`xid8funcs::pg_snapshot_xip`], which hands back a
//! `Vec<FullTransactionId>`.
//!
//! Here that core is driven over the executor frame: on the first call the
//! `xip[]` Vec is resolved once and stashed in `funcctx->user_fctx`; each
//! subsequent call returns the next element by value (`xid8` is a by-value
//! 64-bit type). Registered from [`register_pg_snapshot_xip`] (called by
//! `init_seams`); it bypasses the by-OID builtin registry whose tag-only
//! `resultinfo` cannot carry the live `ReturnSetInfo` (the WONTFIX dual-home).

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_core::{FullTransactionId, Oid};
use ::types_error::PgResult;
use ::nodes::execexpr::ExprDoneCond;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{
    end_MultiFuncCall, init_MultiFuncCall, per_MultiFuncCall,
};

use crate::register_srf;

/// `pg_snapshot_xip(pg_snapshot)` (OID 5064).
const PG_SNAPSHOT_XIP: Oid = 5064;

/// `txid_snapshot_xip(txid_snapshot)` (OID 2947) — the deprecated alias. Its
/// `pg_proc` entry (pg_proc.dat:10581) shares `prosrc => 'pg_snapshot_xip'` with
/// OID 5064; `txid_snapshot` is binary-compatible with `pg_snapshot` and `int8`
/// with `xid8` (both by-value 64-bit), so the same SRF body serves both OIDs.
/// Without this registration the call resolves to `pg_snapshot_xip` in the fmgr
/// internal lookup table and errors `internal function "pg_snapshot_xip" is not
/// in internal lookup table`.
const TXID_SNAPSHOT_XIP: Oid = 2947;

/// Register `pg_snapshot_xip` (and its `txid_snapshot_xip` alias) in the
/// executor-frame SRF table.
pub(crate) fn register_pg_snapshot_xip() {
    register_srf(PG_SNAPSHOT_XIP, pg_snapshot_xip);
    register_srf(TXID_SNAPSHOT_XIP, pg_snapshot_xip);
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

/// Cross-call state: the resolved `xip[]` sequence (C copies the whole snapshot
/// into `multi_call_memory_ctx` and walks `xip[call_cntr]`; the value sequence
/// is exactly this Vec).
struct SnapshotXipFctx {
    xip: Vec<FullTransactionId>,
}

/// Erase a typed cross-call state into the `FuncCallContext.user_fctx` carrier
/// (C: `funcctx->user_fctx = palloc(...)`).
fn erase_user_fctx<'mcx, T: Any>(mcx: Mcx<'mcx>, v: T) -> PgBox<'mcx, dyn Any> {
    let boxed = ::mcx::alloc_in(mcx, v).expect("alloc user_fctx");
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn Any` vtable.
    unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) }
}

/// `pg_snapshot_xip(PG_FUNCTION_ARGS)` (xid8funcs.c:594) over the executor frame.
fn pg_snapshot_xip<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_snapshot_xip: fn_mcxt set by the SRF caller");

    // C: if (SRF_IS_FIRSTCALL()) { ... make a copy of user snapshot ... }
    if fcinfo.fn_extra.is_none() {
        // C: arg = (pg_snapshot *) PG_GETARG_VARLENA_P(0). The `pg_snapshot` arg
        // crosses the by-ref lane as its header-ful varlena image.
        let image = fcinfo
            .ref_arg(0)
            .and_then(|p| p.as_varlena())
            .expect("pg_snapshot_xip: by-ref pg_snapshot arg missing from by-ref lane");
        let snap = match xid8funcs::PgSnapshot::from_varlena_bytes(image) {
            Some(snap) => snap,
            None => {
                return Err(::types_error::PgError::error(
                    "invalid pg_snapshot image".to_string(),
                ))
            }
        };
        // The value sequence (snap->xip[0..nxip]); copied here, mirroring C's
        // copy of the whole snapshot into multi_call_memory_ctx.
        let xip = xid8funcs::pg_snapshot_xip(&snap);

        init_MultiFuncCall(fcinfo).expect("init_MultiFuncCall");
        let fctx = erase_user_fctx(mcx, SnapshotXipFctx { xip });
        let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
        funcctx.user_fctx = Some(fctx);
    }

    // C: fctx = SRF_PERCALL_SETUP(); snap = fctx->user_fctx;
    let funcctx = per_MultiFuncCall(fcinfo).expect("per_MultiFuncCall");
    let call_cntr = funcctx.call_cntr as usize;
    let state: &SnapshotXipFctx = funcctx
        .user_fctx
        .as_ref()
        .expect("user_fctx present")
        .downcast_ref::<SnapshotXipFctx>()
        .expect("user_fctx is SnapshotXipFctx");

    // C: if (fctx->call_cntr < snap->nxip) { value = snap->xip[call_cntr]; ... }
    if call_cntr < state.xip.len() {
        let value = state.xip[call_cntr];
        // SRF_RETURN_NEXT(fctx, FullTransactionIdGetDatum(value)).
        funcctx.call_cntr += 1;
        set_isdone(fcinfo, ExprDoneCond::ExprMultipleResult);
        fcinfo.isnull = false;
        Ok(Datum::from_u64(value.to_u64()))
    } else {
        // SRF_RETURN_DONE(fctx).
        end_MultiFuncCall(fcinfo).expect("end_MultiFuncCall");
        set_isdone(fcinfo, ExprDoneCond::ExprEndResult);
        fcinfo.isnull = true;
        Ok(Datum::null())
    }
}
