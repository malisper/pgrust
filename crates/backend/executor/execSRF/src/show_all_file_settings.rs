//! `show_all_file_settings()` (OID 3329) over the executor frame — the
//! materialize-mode SRF backing the `pg_file_settings` system view.
//!
//! The config-file parse + apply-check core
//! (`ProcessConfigFileInternal(PGC_SIGHUP, false, DEBUG3)`) lives in the guc core
//! ([`misc_guc::show_all_file_settings_items`]). This module is the
//! executor-frame adapter: `InitMaterializedSRF` establishes
//! `fcinfo->resultinfo`, then each parsed entry is emitted as the 7-column
//! `(values, nulls)` pair C builds with `tuplestore_putvalues`
//! (sourcefile/name/setting/error text via `cstring_to_text`, sourceline/seqno
//! int4, applied bool).

use mcx::Mcx;
use types_core::Oid;
use types_error::{PgResult, DEBUG3};
use nodes::fmgr::FunctionCallInfoBaseData;
use nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `show_all_file_settings()` (OID 3329).
const SHOW_ALL_FILE_SETTINGS: Oid = 3329;

/// `NUM_PG_FILE_SETTINGS_ATTS` (guc_funcs.c) — 7 output columns.
const NUM_COLS: usize = 7;

/// Register the `pg_file_settings` SRF in the executor-frame SRF table.
pub(crate) fn register_show_all_file_settings() {
    register_srf(SHOW_ALL_FILE_SETTINGS, show_all_file_settings);
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `show_all_file_settings(PG_FUNCTION_ARGS)` (guc_funcs.c:984) over the executor
/// frame.
fn show_all_file_settings<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("show_all_file_settings: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: conf = ProcessConfigFileInternal(PGC_SIGHUP, false, DEBUG3);
    let items = misc_guc::show_all_file_settings_items(DEBUG3)?;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("show_all_file_settings: InitMaterializedSRF establishes fcinfo->resultinfo");

    // C: for (seqno = 1; conf != NULL; conf = conf->next, seqno++).
    for (idx, conf) in items.iter().enumerate() {
        let seqno = (idx as i32) + 1;
        let mut values: [Datum<'mcx>; NUM_COLS] = core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; NUM_COLS];

        // The owned ConfigItem stores `filename` as an empty string when the C
        // `conf->filename` would be NULL (no sourcefile); mirror C's NULL test.
        let has_file = !conf.filename.is_empty();

        // [0] sourcefile — text / NULL
        if has_file {
            values[0] = text_datum(mcx, &conf.filename)?;
        } else {
            nulls[0] = true;
        }
        // [1] sourceline — int4 / NULL (not meaningful without a sourcefile)
        if has_file {
            values[1] = Datum::from_i32(conf.sourceline);
        } else {
            nulls[1] = true;
        }
        // [2] seqno — int4
        values[2] = Datum::from_i32(seqno);
        // [3] name — text / NULL
        if conf.name.is_empty() {
            nulls[3] = true;
        } else {
            values[3] = text_datum(mcx, &conf.name)?;
        }
        // [4] setting — text / NULL
        if conf.value.is_empty() {
            nulls[4] = true;
        } else {
            values[4] = text_datum(mcx, &conf.value)?;
        }
        // [5] applied — bool
        values[5] = Datum::from_bool(conf.applied);
        // [6] error — text / NULL
        match &conf.errmsg {
            Some(e) => values[6] = text_datum(mcx, e)?,
            None => nulls[6] = true,
        }

        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}
