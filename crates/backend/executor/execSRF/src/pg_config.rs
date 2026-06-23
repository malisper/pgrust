//! `pg_config()` (OID 3400) over the executor frame — the materialize-mode SRF
//! backing the `pg_config` system view.
//!
//! `utils/misc/pg_config.c`'s `pg_config` is a thin materialize-mode SRF:
//! `InitMaterializedSRF(fcinfo, 0)`, then `get_configdata(my_exec_path, &len)`
//! (`common/config_info.c`) and `tuplestore_putvalues` of each `(name, setting)`
//! pair as two `CStringGetTextDatum` images. The row source (the
//! `get_configdata` walk) lives in [`misc_timeout::pg_config`];
//! this module is the executor-frame adapter that drives it, dispatched through
//! this crate's executor-frame SRF table because the by-OID fmgr home's
//! tag-only `resultinfo` cannot carry the live `ReturnSetInfo` (the WONTFIX
//! dual-home).

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_config()` (OID 3400).
const PG_CONFIG: Oid = 3400;

/// Register the `pg_config` view SRF in the executor-frame SRF table.
pub(crate) fn register_pg_config() {
    register_srf(PG_CONFIG, pg_config);
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `pg_config(PG_FUNCTION_ARGS)` (`pg_config.c`) over the executor frame.
fn pg_config<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_config: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: get_configdata(my_exec_path, &configdata_len).
    let rows = misc_timeout::pg_config::get_config_rows()?;

    // C: InitMaterializedSRF(fcinfo, 0). The owned model takes the executor's
    // already-resolved `(text, text)` descriptor via MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_config: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        // C: values[0] = CStringGetTextDatum(name);
        //    values[1] = CStringGetTextDatum(setting); nulls all false.
        let values = [text_datum(mcx, &row.name)?, text_datum(mcx, &row.setting)?];
        let nulls = [false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: return (Datum) 0; (the materialize-mode SRF returns a null result word).
    fcinfo.isnull = true;
    Ok(Datum::null())
}
