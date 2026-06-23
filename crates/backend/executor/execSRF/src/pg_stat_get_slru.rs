//! `pg_stat_get_slru()` (OID 2306) registered as an executor-frame
//! materialize-mode set-returning function — the `pg_stat_slru` view's
//! underlying function.
//!
//! `pgstatfuncs.c`'s `pg_stat_get_slru` materializes one 9-column row per SLRU
//! cache, fetching the cumulative SLRU snapshot via `pgstat_fetch_slru()` and
//! walking `pgstat_get_slru_name(i)` until it returns NULL (i.e. past
//! `SLRU_NUM_ELEMENTS`). Both the fetch and the name accessor are the
//! `backend-utils-activity-pgstat-slru` owner's; this module is the pgstatfuncs.c
//! projection over the executor frame.

extern crate alloc;
use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use pgstat_slru as slru;
use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_stat_get_slru()` (OID 2306).
const PG_STAT_GET_SLRU: Oid = 2306;

/// `PG_STAT_GET_SLRU_COLS` (pgstatfuncs.c:1717).
const PG_STAT_GET_SLRU_COLS: usize = 9;

/// Register `pg_stat_get_slru` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_slru() {
    register_srf(PG_STAT_GET_SLRU, pg_stat_get_slru);
}

/// `pg_stat_get_slru(PG_FUNCTION_ARGS)` (pgstatfuncs.c:1715) over the executor
/// frame.
fn pg_stat_get_slru<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_slru: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: stats = pgstat_fetch_slru();
    let stats = slru::pgstat_fetch_slru()?;

    let mut rows: Vec<([Datum<'mcx>; PG_STAT_GET_SLRU_COLS], [bool; PG_STAT_GET_SLRU_COLS])> =
        Vec::new();

    // C: for (i = 0;; i++) { name = pgstat_get_slru_name(i); if (!name) break; ... }
    let mut i: i32 = 0;
    loop {
        let name = match slru::pgstat_get_slru_name(i) {
            Some(name) => name,
            None => break,
        };
        let stat = &stats[i as usize];

        let values: [Datum<'mcx>; PG_STAT_GET_SLRU_COLS] = [
            varlena_seams::cstring_to_text_v::call(mcx, name)?,
            Datum::from_i64(stat.blocks_zeroed),
            Datum::from_i64(stat.blocks_hit),
            Datum::from_i64(stat.blocks_read),
            Datum::from_i64(stat.blocks_written),
            Datum::from_i64(stat.blocks_exists),
            Datum::from_i64(stat.flush),
            Datum::from_i64(stat.truncate),
            Datum::from_i64(stat.stat_reset_timestamp),
        ];
        let nulls = [false; PG_STAT_GET_SLRU_COLS];
        rows.push((values, nulls));

        i += 1;
    }

    // C: InitMaterializedSRF(fcinfo, 0).
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_stat_get_slru: InitMaterializedSRF establishes fcinfo->resultinfo");

    for (values, nulls) in &rows {
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // C: return (Datum) 0.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
