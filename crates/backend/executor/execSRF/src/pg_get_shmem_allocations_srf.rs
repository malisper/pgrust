//! Executor-frame registration of the materialize-mode
//! `pg_get_shmem_allocations()` set-returning function (`shmem.c`).
//!
//! The algorithm lives in `backend-storage-ipc-shmem`
//! ([`ipc_shmem::pg_get_shmem_allocations`]); it runs the
//! `InitMaterializedSRF` / `materialized_srf_putvalues` protocol over the
//! executor frame itself, iterating the live `ShmemIndex` under
//! `ShmemIndexLock`. This module is the thin executor-frame adapter that sources
//! the per-query memory context (`fcinfo->fn_mcxt`) the C body's
//! `CStringGetTextDatum` pallocs in, and dispatches the body, registering it
//! under its `pg_proc` OID so `srf_invoke_by_oid` resolves it (the executor-frame
//! counterpart of a `fmgr_builtins[]` row).

use ::types_core::Oid;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `pg_get_shmem_allocations()` (OID 5052).
const PG_GET_SHMEM_ALLOCATIONS: Oid = 5052;

/// Register the shmem-allocations SRF in the executor-frame SRF table.
pub(crate) fn register_pg_get_shmem_allocations() {
    register_srf(PG_GET_SHMEM_ALLOCATIONS, pg_get_shmem_allocations);
}

/// `pg_get_shmem_allocations(PG_FUNCTION_ARGS)` (shmem.c) over the executor
/// frame.
fn pg_get_shmem_allocations<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> types_error::PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_get_shmem_allocations: fn_mcxt set by ExecMakeTableFunctionResult");
    // The substrate body runs InitMaterializedSRF + the ShmemIndex walk and
    // returns `(Datum) 0`; re-tag the null result to the call lifetime.
    ipc_shmem::pg_get_shmem_allocations(mcx, fcinfo)?;
    Ok(Datum::null())
}
