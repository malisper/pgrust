//! `pg_lock_status()` (OID 1371) registered as an executor-frame
//! materialize-mode set-returning function — the `pg_locks` system view's
//! underlying function.
//!
//! `lockfuncs.c`'s `pg_lock_status` is a value-per-call SRF emitting one
//! `(locktype, database, relation, page, tuple, virtualxid, transactionid,
//! classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath,
//! waitstart)` row per held/awaited lock mode (snapshotting `GetLockStatusData`
//! for the regular locks, then `GetPredicateLockStatusData` for the SIREAD
//! predicate locks). The snapshot-and-project core — the per-PROCLOCK `holdMask`
//! expansion + the 16-column projection + the predicate leg — is ported in
//! [`backend_utils_adt_misc2::admin::pg_lock_status_rows`] (its rightful
//! lockfuncs.c owner), driven over the live lock-table / predicate-lock
//! snapshots via their seams.
//!
//! Here that core is driven over the executor frame in materialize mode (the
//! whole lock set is snapshotted once and the tuplestore filled, emitting the
//! identical rows the C per-call series would). `InitMaterializedSRF` with
//! `MAT_SRF_USE_EXPECTED_DESC` takes the executor's already-resolved 16-column
//! descriptor (skipping the catalog `get_call_result_type`); the rows are
//! appended via `materialized_srf_putvalues`, and the entry point returns SQL
//! NULL. Registered from [`register_pg_lock_status`] (called by `init_seams`);
//! it bypasses the by-OID builtin registry whose tag-only `resultinfo` cannot
//! carry the live `ReturnSetInfo` (the WONTFIX dual-home).

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_lock_status()` (OID 1371).
const PG_LOCK_STATUS: Oid = 1371;

/// Register `pg_lock_status` in the executor-frame SRF table.
pub(crate) fn register_pg_lock_status() {
    register_srf(PG_LOCK_STATUS, pg_lock_status);
}

/// `pg_lock_status(PG_FUNCTION_ARGS)` (lockfuncs.c) over the executor frame.
fn pg_lock_status<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_lock_status: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: GetLockStatusData() + GetPredicateLockStatusData() under their locks,
    // then the per-PROCLOCK holdMask expansion + 16-column projection. Run the
    // snapshot-and-project core (lockfuncs.c's owner).
    let rows = backend_utils_adt_misc2::admin::pg_lock_status_rows(mcx)?;

    // C: get_call_result_type → the 16-column row type. Take the executor's
    // already-resolved descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_lock_status: InitMaterializedSRF establishes fcinfo->resultinfo");

    for (values, nulls) in &rows {
        // C: heap_form_tuple(tupdesc, values, nulls); SRF_RETURN_NEXT(...).
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
