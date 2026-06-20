//! Executor-frame registration of the materialize-mode
//! `pg_get_shmem_allocations_numa()` set-returning function (`shmem.c`).
//!
//! The algorithm lives in `backend-storage-ipc-shmem`
//! ([`backend_storage_ipc_shmem::pg_get_shmem_allocations_numa`]); it runs the
//! `InitMaterializedSRF` / `materialized_srf_putvalues` protocol over the
//! executor frame itself. This module is the thin executor-frame adapter that
//! sources the two ambient inputs the C body reads as globals — the per-query
//! memory context (`fcinfo->fn_mcxt`) and the `huge_pages_status` GUC — and
//! dispatches the body, registering it under its `pg_proc` OID so
//! `srf_invoke_by_oid` resolves it (the executor-frame counterpart of a
//! `fmgr_builtins[]` row).
//!
//! On non-NUMA builds (e.g. macOS) the body returns immediately with
//! `ereport(ERROR, "libnuma initialization failed or NUMA is not supported on
//! this platform")` — `pg_numa_init()` is the empty-wrapper `-1` — so it never
//! reaches the materialize protocol.

use types_core::Oid;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_storage::HugePagesStatus;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `pg_get_shmem_allocations_numa()` (OID 4100).
const PG_GET_SHMEM_ALLOCATIONS_NUMA: Oid = 4100;

/// Register the NUMA shmem-allocations SRF in the executor-frame SRF table.
pub(crate) fn register_shmem_numa_srf() {
    register_srf(
        PG_GET_SHMEM_ALLOCATIONS_NUMA,
        pg_get_shmem_allocations_numa,
    );
}

/// Read the live `huge_pages_status` GUC and decode it into the C enum the body
/// takes as an explicit parameter (C reads the `huge_pages_status` global). The
/// slot stores the `config_enum` index, which is exactly the `HugePagesStatus`
/// discriminant.
fn huge_pages_status() -> HugePagesStatus {
    match backend_utils_misc_guc_tables::vars::huge_pages_status.read() {
        0 => HugePagesStatus::HUGE_PAGES_OFF,
        1 => HugePagesStatus::HUGE_PAGES_ON,
        2 => HugePagesStatus::HUGE_PAGES_TRY,
        // 3 (and any out-of-range value defensively) => unknown.
        _ => HugePagesStatus::HUGE_PAGES_UNKNOWN,
    }
}

/// `pg_get_shmem_allocations_numa(PG_FUNCTION_ARGS)` (shmem.c) over the executor
/// frame.
fn pg_get_shmem_allocations_numa<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> types_error::PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_get_shmem_allocations_numa: fn_mcxt set by ExecMakeTableFunctionResult");
    backend_storage_ipc_shmem::pg_get_shmem_allocations_numa(mcx, fcinfo, huge_pages_status())
}
