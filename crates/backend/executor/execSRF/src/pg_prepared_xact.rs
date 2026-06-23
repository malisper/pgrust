//! `pg_prepared_xact()` (OID 1065) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `twophase.c`'s `pg_prepared_xact` is a value-per-call SRF emitting one
//! `(transaction xid, gid text, prepared timestamptz, ownerid oid, dbid oid)`
//! row per *valid* gxact in the live `TwoPhaseState->prepXacts[]` array (it
//! snapshots the array under `TwoPhaseStateLock` via
//! `GetPreparedTransactionList`, skipping `!gxact->valid` entries, and reads each
//! gxact's `proc->xid`/`proc->databaseId` from its dummy PGPROC). The
//! snapshot-and-project core (the locked list copy + the `!valid` filter + the
//! per-row projection) is ported in
//! [`twophase::pg_prepared_xact_rows`], driven over the
//! live process-global `TwoPhaseStateData` via `with_twophase_state` (the owned
//! analogue of the `TwoPhaseState` shmem global stood up by `TwoPhaseShmemInit`).
//!
//! Here that core is driven over the executor frame in materialize mode (the
//! valid-gxact set is snapshotted once under the lock and the whole tuplestore
//! filled, emitting the identical rows the C per-call series would).
//! `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's
//! already-resolved `(xid, text, timestamptz, oid, oid)` descriptor (skipping
//! the catalog `get_call_result_type`); the rows are appended via
//! `materialized_srf_putvalues`, and the entry point returns SQL NULL.
//! Registered from [`register_pg_prepared_xact`] (called by `init_seams`); it
//! bypasses the by-OID builtin registry whose tag-only `resultinfo` cannot carry
//! the live `ReturnSetInfo` (the WONTFIX dual-home).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_prepared_xact()` (OID 1065).
const PG_PREPARED_XACT: Oid = 1065;

/// Register `pg_prepared_xact` in the executor-frame SRF table.
pub(crate) fn register_pg_prepared_xact() {
    register_srf(PG_PREPARED_XACT, pg_prepared_xact);
}

/// `pg_prepared_xact(PG_FUNCTION_ARGS)` (twophase.c) over the executor frame.
fn pg_prepared_xact<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_prepared_xact: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: GetPreparedTransactionList(&status->array) under TwoPhaseStateLock, then
    // the per-row projection with the `!gxact->valid` entries filtered out. Run
    // the snapshot-and-project core over the live process-global TwoPhaseState.
    let rows = twophase::with_twophase_state(|state| {
        twophase::pg_prepared_xact_rows(state)
    })?;

    // C: get_call_result_type → the (xid, text, timestamptz, oid, oid) row type.
    // Take the executor's already-resolved descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_prepared_xact: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        // values[0] = TransactionIdGetDatum(proc->xid); values[1] =
        // CStringGetTextDatum(gxact->gid); values[2] =
        // TimestampTzGetDatum(gxact->prepared_at); values[3] =
        // ObjectIdGetDatum(gxact->owner); values[4] =
        // ObjectIdGetDatum(proc->databaseId). All non-NULL.
        let gid = varlena_seams::cstring_to_text_v::call(mcx, &row.gid)?;
        let values = [
            Datum::from_transaction_id(row.transaction),
            gid,
            Datum::from_i64(row.prepared),
            Datum::from_oid(row.ownerid),
            Datum::from_oid(row.dbid),
        ];
        let nulls = [false, false, false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
