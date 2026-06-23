//! `pg_stat_get_recovery_prefetch()` (OID 6248) — the `pg_stat_recovery_prefetch`
//! view's underlying single-row materialize SRF (xlogprefetcher.c:823).
//!
//! Exposes the 10-column `XLogPrefetchStats` shared-memory counters
//! (`reset_time`, `prefetch`, `hit`, `skip_init`, `skip_new`, `skip_fpw`,
//! `skip_rep`, `wal_distance`, `block_distance`, `io_depth`). C builds the row
//! into `values`/`nulls` (all non-NULL) and appends it via `tuplestore_putvalues`
//! against the `InitMaterializedSRF`-prepared `ReturnSetInfo`; the owned model
//! takes the executor's already-resolved descriptor (`MAT_SRF_USE_EXPECTED_DESC`)
//! and appends one row through `materialized_srf_putvalues`.

use types_core::Oid;
use types_error::PgResult;
use nodes::fmgr::FunctionCallInfoBaseData;
use nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use xlogprefetcher::XLogPrefetchReadStats;
use funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_stat_get_recovery_prefetch()` (OID 6248).
const PG_STAT_GET_RECOVERY_PREFETCH: Oid = 6248;

const PG_STAT_GET_RECOVERY_PREFETCH_COLS: usize = 10;

/// Register `pg_stat_get_recovery_prefetch` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_recovery_prefetch() {
    register_srf(PG_STAT_GET_RECOVERY_PREFETCH, pg_stat_get_recovery_prefetch);
}

/// `pg_stat_get_recovery_prefetch(PG_FUNCTION_ARGS)` (xlogprefetcher.c:823) over
/// the executor frame.
fn pg_stat_get_recovery_prefetch<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // C: InitMaterializedSRF(fcinfo, 0). Take the executor's already-resolved
    // 10-column descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    // C: for (i = 0; i < COLS; ++i) nulls[i] = false;
    let nulls = [false; PG_STAT_GET_RECOVERY_PREFETCH_COLS];

    // C: values[0..9] = the SharedStats atomic reads.
    let s = XLogPrefetchReadStats();
    let values: [Datum<'mcx>; PG_STAT_GET_RECOVERY_PREFETCH_COLS] = [
        Datum::from_i64(s.reset_time), // TimestampTzGetDatum(reset_time)
        Datum::from_i64(s.prefetch),
        Datum::from_i64(s.hit),
        Datum::from_i64(s.skip_init),
        Datum::from_i64(s.skip_new),
        Datum::from_i64(s.skip_fpw),
        Datum::from_i64(s.skip_rep),
        Datum::from_i32(s.wal_distance),
        Datum::from_i32(s.block_distance),
        Datum::from_i32(s.io_depth),
    ];

    let rsinfo = fcinfo.resultinfo.as_mut().expect(
        "pg_stat_get_recovery_prefetch: InitMaterializedSRF establishes fcinfo->resultinfo",
    );
    // C: tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;

    // C: return (Datum) 0.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
