//! `pg_timezone_names()` (OID 2856) and `pg_timezone_abbrevs_zone()` (OID 6401)
//! registered as executor-frame materialize-mode set-returning functions — the
//! SRFs backing the `pg_timezone_names` and `pg_timezone_abbrevs` system views.
//!
//! `datetime.c`'s `pg_timezone_names` is a materialize-mode SRF emitting one
//! `(name text, abbrev text, utc_offset interval, is_dst bool)` row per IANA
//! zone; `pg_timezone_abbrevs_zone` is a value-per-call SRF emitting one
//! `(abbrev text, utc_offset interval, is_dst bool)` row per abbreviation the
//! current `session_timezone`'s IANA data defines. The row-source cores (the
//! `pg_tzenumerate_*` / `timestamp2tm` walk and the
//! `pg_get_next_timezone_abbrev` / `pg_interpret_timezone_abbrev` walk, plus the
//! `itmin2interval` `utc_offset` construction) live in
//! [`::adt_datetime::tz_views`].
//!
//! Here those cores are driven over the executor frame in materialize mode.
//! Registered from [`register_pg_timezone_srfs`] (called by `init_seams`); they
//! bypass the by-OID builtin registry whose tag-only `resultinfo` cannot carry
//! the live `ReturnSetInfo` (the WONTFIX dual-home).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::adt_datetime::Interval;
use ::types_datetime::USECS_PER_SEC;
use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_timezone_names()` (OID 2856).
const PG_TIMEZONE_NAMES: Oid = 2856;
/// `pg_timezone_abbrevs_zone()` (OID 6401).
const PG_TIMEZONE_ABBREVS_ZONE: Oid = 6401;
/// `pg_timezone_abbrevs_abbrevs()` (OID 2599).
const PG_TIMEZONE_ABBREVS_ABBREVS: Oid = 2599;

/// Register the timezone view SRFs in the executor-frame SRF table.
pub(crate) fn register_pg_timezone_srfs() {
    register_srf(PG_TIMEZONE_NAMES, pg_timezone_names);
    register_srf(PG_TIMEZONE_ABBREVS_ZONE, pg_timezone_abbrevs_zone);
    register_srf(PG_TIMEZONE_ABBREVS_ABBREVS, pg_timezone_abbrevs_abbrevs);
}

/// Build an `Interval` `utc_offset` from a GMT offset in seconds (C:
/// `itm_in.tm_usec = (int64) gmtoff * USECS_PER_SEC; itmin2interval(...)` — the
/// months/days fields stay 0, so the whole offset lands in `time`).
fn interval_from_secs(secs: i64) -> Interval {
    Interval {
        time: secs * USECS_PER_SEC,
        day: 0,
        month: 0,
    }
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `IntervalPGetDatum(resInterval)` — lower an `Interval` (16-byte fixed by-ref
/// type: `time:i64, day:i32, month:i32` little-endian) onto the by-reference
/// Datum lane.
fn interval_datum<'mcx>(mcx: Mcx<'mcx>, iv: &Interval) -> PgResult<Datum<'mcx>> {
    let mut img = [0u8; 16];
    img[0..8].copy_from_slice(&iv.time.to_le_bytes());
    img[8..12].copy_from_slice(&iv.day.to_le_bytes());
    img[12..16].copy_from_slice(&iv.month.to_le_bytes());
    Datum::from_byref_bytes_in(mcx, &img)
}

/// `pg_timezone_names(PG_FUNCTION_ARGS)` (datetime.c:5332) over the executor
/// frame.
fn pg_timezone_names<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_timezone_names: fn_mcxt set by ExecMakeTableFunctionResult");

    let rows = ::adt_datetime::tz_views::pg_timezone_names_rows()?;

    // C: InitMaterializedSRF(fcinfo, 0). The owned model takes the executor's
    // already-resolved `(text, text, interval, bool)` descriptor via
    // MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_timezone_names: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        let values = [
            text_datum(mcx, &row.name)?,
            text_datum(mcx, &row.abbrev)?,
            interval_datum(mcx, &row.utc_offset)?,
            Datum::from_bool(row.is_dst),
        ];
        let nulls = [false, false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `pg_timezone_abbrevs_zone(PG_FUNCTION_ARGS)` (datetime.c:5124) over the
/// executor frame.
fn pg_timezone_abbrevs_zone<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_timezone_abbrevs_zone: fn_mcxt set by ExecMakeTableFunctionResult");

    let rows = ::adt_datetime::tz_views::pg_timezone_abbrevs_zone_rows();

    // C drives this as a value-per-call SRF; the owned model materializes the
    // (fixed, known up front) row set. The descriptor `(abbrev, utc_offset,
    // is_dst)` comes from the executor via MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_timezone_abbrevs_zone: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        let values = [
            text_datum(mcx, &row.abbrev)?,
            interval_datum(mcx, &row.utc_offset)?,
            Datum::from_bool(row.is_dst),
        ];
        let nulls = [false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `pg_timezone_abbrevs_abbrevs(PG_FUNCTION_ARGS)` (datetime.c:5210) over the
/// executor frame — the abbreviations of the active `timezone_abbreviations`
/// GUC set.
fn pg_timezone_abbrevs_abbrevs<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_timezone_abbrevs_abbrevs: fn_mcxt set by ExecMakeTableFunctionResult");

    let rows =
        ::adt_datetime::tz_abbrev_install::pg_timezone_abbrevs_abbrevs_rows()?;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_timezone_abbrevs_abbrevs: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        let values = [
            text_datum(mcx, &row.abbrev)?,
            interval_datum(mcx, &interval_from_secs(row.gmtoffset as i64))?,
            Datum::from_bool(row.is_dst),
        ];
        let nulls = [false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}
