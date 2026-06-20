//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `to_ts*`
//! SQL-callable functions of `to_tsany.c`.
//!
//! `to_tsvector` args are `(regconfig oid, text)` -> `tsvector`; the
//! `to_tsquery` family is `(regconfig oid, text)` -> `tsquery`. A `tsvector`
//! and a `tsquery` result are flat **header-ful** varlena images, so they cross
//! VERBATIM on the by-ref `RefPayload::Varlena` lane. The `text` arg is read as
//! its `VARDATA_ANY` payload (header stripped); the `regconfig` arg is a
//! by-value `Oid`. `get_current_ts_config()` takes no args and returns an `Oid`.

use std::string::ToString;
use std::vec::Vec;

use backend_utils_fmgr_core::register_builtins_native;
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

const VARHDRSZ: usize = 4;

/// `PG_GETARG_OID(i)`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("to_tsany fn: missing oid arg")
        .value
        .as_oid()
}

/// `VARDATA_ANY(PG_GETARG_TEXT_PP(i))`: the payload bytes of a header-ful
/// `text` arg (after the 4-byte length word).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("to_tsany fn: by-ref text arg missing from by-ref lane");
    if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &[]
    }
}

/// Set a header-ful `tsvector`/`tsquery` varlena result on the by-ref lane.
#[inline]
fn ret_varlena_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("to_tsany fmgr scratch")
}

// ---------------------------------------------------------------------------
// to_tsvector
// ---------------------------------------------------------------------------

fn fc_to_tsvector_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let txt = arg_text(fcinfo, 1).to_vec();
    let img = crate::to_tsvector::to_tsvector_byid(cfg, &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_to_tsvector(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let txt = arg_text(fcinfo, 0).to_vec();
    let img = crate::to_tsvector::to_tsvector(&txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

// ---------------------------------------------------------------------------
// to_tsquery family
// ---------------------------------------------------------------------------

fn fc_to_tsquery_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let txt = arg_text(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::to_tsquery_byid(m.mcx(), cfg, &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_to_tsquery(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let txt = arg_text(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::to_tsquery(m.mcx(), &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_plainto_tsquery_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let txt = arg_text(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::plainto_tsquery_byid(m.mcx(), cfg, &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_plainto_tsquery(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let txt = arg_text(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::plainto_tsquery(m.mcx(), &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_phraseto_tsquery_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let txt = arg_text(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::phraseto_tsquery_byid(m.mcx(), cfg, &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_phraseto_tsquery(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let txt = arg_text(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::phraseto_tsquery(m.mcx(), &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_websearch_to_tsquery_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let txt = arg_text(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::websearch_to_tsquery_byid(m.mcx(), cfg, &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_websearch_to_tsquery(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let txt = arg_text(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsquery::websearch_to_tsquery(m.mcx(), &txt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

// ---------------------------------------------------------------------------
// get_current_ts_config
// ---------------------------------------------------------------------------

fn fc_get_current_ts_config(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // PG_RETURN_OID(getTSCurrentConfig(true)).
    let oid = backend_utils_cache_ts_cache::getTSCurrentConfig(true)?;
    Ok(Datum::from_oid(oid))
}

fn builtin(foid: u32, name: &str, nargs: i16, native: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the `to_ts*` fmgr builtins.
pub fn register_to_tsany_builtins() {
    register_builtins_native([
        builtin(3745, "to_tsvector_byid", 2, fc_to_tsvector_byid),
        builtin(3749, "to_tsvector", 1, fc_to_tsvector),
        builtin(3746, "to_tsquery_byid", 2, fc_to_tsquery_byid),
        builtin(3750, "to_tsquery", 1, fc_to_tsquery),
        builtin(3747, "plainto_tsquery_byid", 2, fc_plainto_tsquery_byid),
        builtin(3751, "plainto_tsquery", 1, fc_plainto_tsquery),
        builtin(5006, "phraseto_tsquery_byid", 2, fc_phraseto_tsquery_byid),
        builtin(5001, "phraseto_tsquery", 1, fc_phraseto_tsquery),
        builtin(5007, "websearch_to_tsquery_byid", 2, fc_websearch_to_tsquery_byid),
        builtin(5009, "websearch_to_tsquery", 1, fc_websearch_to_tsquery),
        builtin(3759, "get_current_ts_config", 0, fc_get_current_ts_config),
    ]);
}
