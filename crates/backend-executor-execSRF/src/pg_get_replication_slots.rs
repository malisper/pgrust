//! `pg_get_replication_slots()` (OID 3781) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `slotfuncs.c`'s `pg_get_replication_slots` is the 20-column SRF backing the
//! `pg_replication_slots` view: it walks the replication-slot array under
//! `ReplicationSlotControlLock` and emits one row per in-use slot. The whole
//! body — the locked snapshot walk, the per-slot column projection, the
//! `wal_status`/`safe_wal_size` computation, and the
//! `InitMaterializedSRF`/`materialized_srf_putvalues` tuplestore fill — already
//! lives in [`backend_replication_slotfuncs::pg_get_replication_slots`] with the
//! executor-frame SRF body shape.
//!
//! Here that core is driven over the executor frame: the body itself runs
//! `InitMaterializedSRF` (building its own 20-column descriptor) and appends the
//! rows via `materialized_srf_putvalues`. Registered from
//! [`register_pg_get_replication_slots`] (called by `init_seams`); it bypasses
//! the by-OID builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home), exactly like `pg_listening_channels`
//! and `pg_lock_status`.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `pg_get_replication_slots()` (OID 3781) — `setof record` (the
/// `pg_replication_slots` view's 20 columns).
const PG_GET_REPLICATION_SLOTS: Oid = 3781;

/// Register `pg_get_replication_slots` in the executor-frame SRF table.
pub(crate) fn register_pg_get_replication_slots() {
    register_srf(PG_GET_REPLICATION_SLOTS, pg_get_replication_slots);
}

/// `pg_get_replication_slots(PG_FUNCTION_ARGS)` (slotfuncs.c) over the executor
/// frame. The value core does the entire SRF — including `InitMaterializedSRF`
/// and `materialized_srf_putvalues`; this thin wrapper only supplies the
/// per-call `Mcx` (C's `CurrentMemoryContext` for the per-row palloc, here
/// `fcinfo->fn_mcxt`).
fn pg_get_replication_slots<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_replication_slots: fn_mcxt set by ExecMakeTableFunctionResult");

    backend_replication_slotfuncs::pg_get_replication_slots(mcx, fcinfo)
}
