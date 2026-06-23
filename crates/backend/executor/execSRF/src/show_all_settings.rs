//! Executor-frame registration of the materialize-mode `show_all_settings()`
//! set-returning function (`utils/misc/guc_funcs.c`), the SRF backing the
//! `pg_settings` view and psql's `\dconfig`.
//!
//! The `GetConfigOptionValues` row projection over the live GUC registry lives in
//! [`::guc_funcs::pg_settings_rows`] (the `config_generic`
//! generic attributes + the per-`vartype` typed `min`/`max`/`enumvals`/`boot`/
//! `reset` reads + the `PGC_S_FILE` source-file/line security gate). This module
//! is the executor-frame adapter: it runs `InitMaterializedSRF` to establish
//! `fcinfo->resultinfo`, snapshots the `pg_settings` rows, and emits each as the
//! 17-column `(values, nulls)` pair `show_all_settings` produces with
//! `BuildTupleFromCStrings` in C (here the typed Datum assembly: `text` columns
//! via `cstring_to_text`, the `enumvals` `text[]` via `construct_text_array`, the
//! `sourceline` `int4`, and the `pending_restart` `bool`).

extern crate alloc;

use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use funcapi_seams::{materialized_srf_putvalues, InitMaterializedSRF};
use ::guc_funcs::PgSettingsRow;

use crate::register_srf;

/// `show_all_settings()` (OID 2084).
const SHOW_ALL_SETTINGS: Oid = 2084;

/// `NUM_PG_SETTINGS_ATTS` (guc_funcs.c) — 17 output columns.
const NUM_PG_SETTINGS_ATTS: usize = ::guc_funcs::NUM_PG_SETTINGS_ATTS;

/// Register the `pg_settings` SRF in the executor-frame SRF table.
pub(crate) fn register_show_all_settings() {
    register_srf(SHOW_ALL_SETTINGS, show_all_settings);
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// Wrap an owned array varlena image on the by-reference Datum lane (the pointer
/// C's `array_in`/`construct_array_builtin` returns).
fn byref_image<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum<'mcx>> {
    let mut buf = ::mcx::PgVec::new_in(mcx);
    buf.try_reserve(image.len()).map_err(|_| mcx.oom(image.len()))?;
    buf.extend_from_slice(image);
    Ok(Datum::ByRef(buf))
}

/// Append one [`PgSettingsRow`] as the 17-column `(values, nulls)` pair (the
/// `BuildTupleFromCStrings` Datum assembly guc_funcs.c does per row) into the
/// live materialized result.
fn put_settings_row<'mcx>(
    mcx: Mcx<'mcx>,
    rsinfo: &mut ::nodes::funcapi::ReturnSetInfo<'mcx>,
    row: &PgSettingsRow,
) -> PgResult<()> {
    let mut values: [Datum<'mcx>; NUM_PG_SETTINGS_ATTS] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NUM_PG_SETTINGS_ATTS];

    // [0] name — text
    values[0] = text_datum(mcx, &row.name)?;
    // [1] setting — text
    values[1] = text_datum(mcx, &row.setting)?;
    // [2] unit — text / NULL
    match &row.unit {
        Some(s) => values[2] = text_datum(mcx, s)?,
        None => nulls[2] = true,
    }
    // [3] category — text
    values[3] = text_datum(mcx, &row.category)?;
    // [4] short_desc — text / NULL
    match &row.short_desc {
        Some(s) => values[4] = text_datum(mcx, s)?,
        None => nulls[4] = true,
    }
    // [5] extra_desc — text / NULL
    match &row.extra_desc {
        Some(s) => values[5] = text_datum(mcx, s)?,
        None => nulls[5] = true,
    }
    // [6] context — text
    values[6] = text_datum(mcx, &row.context)?;
    // [7] vartype — text
    values[7] = text_datum(mcx, &row.vartype)?;
    // [8] source — text
    values[8] = text_datum(mcx, &row.source)?;
    // [9] min_val — text / NULL
    match &row.min_val {
        Some(s) => values[9] = text_datum(mcx, s)?,
        None => nulls[9] = true,
    }
    // [10] max_val — text / NULL
    match &row.max_val {
        Some(s) => values[10] = text_datum(mcx, s)?,
        None => nulls[10] = true,
    }
    // [11] enumvals — text[] / NULL
    match &row.enumvals {
        Some(opts) => {
            let elems: Vec<Vec<u8>> = opts.iter().map(|s| s.as_bytes().to_vec()).collect();
            let img =
                array_more_seams::construct_text_array::call(&elems)?;
            values[11] = byref_image(mcx, &img)?;
        }
        None => nulls[11] = true,
    }
    // [12] boot_val — text / NULL
    match &row.boot_val {
        Some(s) => values[12] = text_datum(mcx, s)?,
        None => nulls[12] = true,
    }
    // [13] reset_val — text / NULL
    match &row.reset_val {
        Some(s) => values[13] = text_datum(mcx, s)?,
        None => nulls[13] = true,
    }
    // [14] sourcefile — text / NULL
    match &row.sourcefile {
        Some(s) => values[14] = text_datum(mcx, s)?,
        None => nulls[14] = true,
    }
    // [15] sourceline — int4 / NULL
    match row.sourceline {
        Some(v) => values[15] = Datum::from_i32(v),
        None => nulls[15] = true,
    }
    // [16] pending_restart — bool
    values[16] = Datum::from_bool(row.pending_restart);

    materialized_srf_putvalues::call(rsinfo, &values, &nulls)
}

/// `show_all_settings(PG_FUNCTION_ARGS)` (guc_funcs.c:848) over the executor
/// frame.
fn show_all_settings<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("show_all_settings: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: InitMaterializedSRF(fcinfo, 0);
    InitMaterializedSRF::call(fcinfo, 0)?;

    // Snapshot the pg_settings rows (sorted, visible, non-NO_SHOW_ALL) from the
    // live GUC registry, then emit each into the live materialized result.
    let rows = ::guc_funcs::pg_settings_rows();
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");
    for row in &rows {
        put_settings_row(mcx, rsinfo, row)?;
    }

    // C: return (Datum) 0;
    Ok(Datum::null())
}
