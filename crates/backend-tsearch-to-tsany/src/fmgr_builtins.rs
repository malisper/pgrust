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

/// `PG_GETARG_JSONB_P(i)`: the full jsonb varlena image (header included). The
/// jsonb-iteration helpers (`iterate_jsonb_values`, `parse_jsonb_index_flags`)
/// strip the `VARHDRSZ` header themselves to reach the root container.
#[inline]
fn arg_jsonb_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("to_tsany fn: by-ref jsonb arg missing from by-ref lane")
}

/// `PG_GETARG_TSQUERY(i)` / `PG_GETARG_JSONB_P(i)`: the full varlena image of a
/// by-ref arg (header included), passed verbatim to a core that reads the
/// header itself.
#[inline]
fn arg_varlena_full<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("to_tsany fn: by-ref arg missing from by-ref lane")
}

/// The optional options-`text` argument of a `ts_headline*_opt` variant:
/// `(PG_NARGS() > 3 && PG_GETARG_POINTER(3)) ? PG_GETARG_TEXT_PP(3) : NULL`.
/// Returns the FULL options `text` varlena image (header included) — the
/// `deserialize_deflist` seam performs its own `TextDatumGetCString` detoast —
/// or `None` when the argument is absent. (These functions are STRICT, so a
/// present argument is never SQL NULL.)
#[inline]
fn arg_opt_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> Option<&'a [u8]> {
    if fcinfo.nargs() <= i {
        return None;
    }
    fcinfo.ref_arg(i).and_then(|p| p.as_varlena())
}

/// Set a header-ful `tsvector`/`tsquery`/`jsonb` varlena result on the by-ref
/// lane.
#[inline]
fn ret_varlena_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Frame a header-stripped `text`/`json` payload as a 4-byte-header varlena and
/// set it as the by-ref result.
#[inline]
fn ret_text_payload(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let total = VARHDRSZ + payload.len();
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    image.extend_from_slice(&payload);
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
// jsonb(_string)_to_tsvector(_byid)
// ---------------------------------------------------------------------------

fn fc_jsonb_string_to_tsvector_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let jb = arg_jsonb_image(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsvector::jsonb_string_to_tsvector_byid(m.mcx(), cfg, &jb)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_jsonb_string_to_tsvector(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsvector::jsonb_string_to_tsvector(m.mcx(), &jb)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_jsonb_to_tsvector_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let jb = arg_jsonb_image(fcinfo, 1).to_vec();
    let jb_flags = arg_jsonb_image(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsvector::jsonb_to_tsvector_byid(m.mcx(), cfg, &jb, &jb_flags)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_jsonb_to_tsvector(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let jb_flags = arg_jsonb_image(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::to_tsvector::jsonb_to_tsvector(m.mcx(), &jb, &jb_flags)?;
    Ok(ret_varlena_image(fcinfo, img))
}

// ---------------------------------------------------------------------------
// json(_string)_to_tsvector(_byid)
// ---------------------------------------------------------------------------

fn fc_json_string_to_tsvector_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let json = arg_text(fcinfo, 1).to_vec();
    let img = crate::to_tsvector::json_string_to_tsvector_byid(cfg, &json)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_json_string_to_tsvector(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let json = arg_text(fcinfo, 0).to_vec();
    let img = crate::to_tsvector::json_string_to_tsvector(&json)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_json_to_tsvector_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let json = arg_text(fcinfo, 1).to_vec();
    let jb_flags = arg_jsonb_image(fcinfo, 2).to_vec();
    let img = crate::to_tsvector::json_to_tsvector_byid(cfg, &json, &jb_flags)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_json_to_tsvector(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let json = arg_text(fcinfo, 0).to_vec();
    let jb_flags = arg_jsonb_image(fcinfo, 1).to_vec();
    let img = crate::to_tsvector::json_to_tsvector(&json, &jb_flags)?;
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

// ---------------------------------------------------------------------------
// ts_headline (text) — wparser.c:287..364
// ---------------------------------------------------------------------------

fn fc_ts_headline_byid_opt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let input = arg_text(fcinfo, 1).to_vec();
    let query = arg_varlena_full(fcinfo, 2).to_vec();
    let opt = arg_opt_text(fcinfo, 3).map(<[u8]>::to_vec);
    let m = scratch_mcx();
    let img =
        crate::ts_headline::ts_headline_byid_opt(m.mcx(), cfg, &input, &query, opt.as_deref())?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_ts_headline_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let input = arg_text(fcinfo, 1).to_vec();
    let query = arg_varlena_full(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline_byid(m.mcx(), cfg, &input, &query)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_ts_headline(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let input = arg_text(fcinfo, 0).to_vec();
    let query = arg_varlena_full(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline(m.mcx(), &input, &query)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_ts_headline_opt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let input = arg_text(fcinfo, 0).to_vec();
    let query = arg_varlena_full(fcinfo, 1).to_vec();
    let opt = arg_varlena_full(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline_opt(m.mcx(), &input, &query, &opt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

// ---------------------------------------------------------------------------
// ts_headline (jsonb) — wparser.c:366..440
// ---------------------------------------------------------------------------

fn fc_ts_headline_jsonb_byid_opt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let jb = arg_jsonb_image(fcinfo, 1).to_vec();
    let query = arg_varlena_full(fcinfo, 2).to_vec();
    let opt = arg_opt_text(fcinfo, 3).map(<[u8]>::to_vec);
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline_jsonb_byid_opt(
        m.mcx(),
        cfg,
        &jb,
        &query,
        opt.as_deref(),
    )?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_ts_headline_jsonb_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let jb = arg_jsonb_image(fcinfo, 1).to_vec();
    let query = arg_varlena_full(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline_jsonb_byid(m.mcx(), cfg, &jb, &query)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_ts_headline_jsonb(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let query = arg_varlena_full(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline_jsonb(m.mcx(), &jb, &query)?;
    Ok(ret_varlena_image(fcinfo, img))
}

fn fc_ts_headline_jsonb_opt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let query = arg_varlena_full(fcinfo, 1).to_vec();
    let opt = arg_varlena_full(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let img = crate::ts_headline::ts_headline_jsonb_opt(m.mcx(), &jb, &query, &opt)?;
    Ok(ret_varlena_image(fcinfo, img))
}

// ---------------------------------------------------------------------------
// ts_headline (json) — wparser.c:442..516
// ---------------------------------------------------------------------------

fn fc_ts_headline_json_byid_opt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let json = arg_text(fcinfo, 1).to_vec();
    let query = arg_varlena_full(fcinfo, 2).to_vec();
    let opt = arg_opt_text(fcinfo, 3).map(<[u8]>::to_vec);
    let m = scratch_mcx();
    let payload = crate::ts_headline::ts_headline_json_byid_opt(
        m.mcx(),
        cfg,
        &json,
        &query,
        opt.as_deref(),
    )?;
    Ok(ret_text_payload(fcinfo, payload))
}

fn fc_ts_headline_json_byid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let cfg = arg_oid(fcinfo, 0);
    let json = arg_text(fcinfo, 1).to_vec();
    let query = arg_varlena_full(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let payload = crate::ts_headline::ts_headline_json_byid(m.mcx(), cfg, &json, &query)?;
    Ok(ret_text_payload(fcinfo, payload))
}

fn fc_ts_headline_json(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let json = arg_text(fcinfo, 0).to_vec();
    let query = arg_varlena_full(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let payload = crate::ts_headline::ts_headline_json(m.mcx(), &json, &query)?;
    Ok(ret_text_payload(fcinfo, payload))
}

fn fc_ts_headline_json_opt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let json = arg_text(fcinfo, 0).to_vec();
    let query = arg_varlena_full(fcinfo, 1).to_vec();
    let opt = arg_varlena_full(fcinfo, 2).to_vec();
    let m = scratch_mcx();
    let payload = crate::ts_headline::ts_headline_json_opt(m.mcx(), &json, &query, &opt)?;
    Ok(ret_text_payload(fcinfo, payload))
}

// ---------------------------------------------------------------------------
// Default text-search parser methods (wparser_def.c) — registered so C's eager
// `fmgr_info(prsobj->startOid/...)` in `lookup_ts_parser_cache` resolves.
//
// These functions use an internal pointer-passing ABI (`TParser *`,
// `HeadlineParsedText *`, `internal`): C dispatches them via `FunctionCallN`,
// but this port calls their typed Rust bodies directly from `parsetext` /
// `hlparsetext` / `prsd_headline`. The registry entries exist only to satisfy
// the eager `fmgr_info` resolution; the generic `Datum fn(PG_FUNCTION_ARGS)`
// lane cannot carry the internal pointers, so a dispatch through it is a port
// invariant violation rather than a reachable path.
// ---------------------------------------------------------------------------

fn fc_prsd_internal_only(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    panic!(
        "default text-search parser method dispatched through the generic fmgr \
         lane; this port calls prsd_start/prsd_nexttoken/prsd_end/prsd_headline/\
         prsd_lextype directly (internal pointer ABI) — the builtin is registered \
         only so fmgr_info resolution succeeds"
    );
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
        builtin(4211, "jsonb_string_to_tsvector_byid", 2, fc_jsonb_string_to_tsvector_byid),
        builtin(4209, "jsonb_string_to_tsvector", 1, fc_jsonb_string_to_tsvector),
        builtin(4214, "jsonb_to_tsvector_byid", 3, fc_jsonb_to_tsvector_byid),
        builtin(4213, "jsonb_to_tsvector", 2, fc_jsonb_to_tsvector),
        builtin(4212, "json_string_to_tsvector_byid", 2, fc_json_string_to_tsvector_byid),
        builtin(4210, "json_string_to_tsvector", 1, fc_json_string_to_tsvector),
        builtin(4216, "json_to_tsvector_byid", 3, fc_json_to_tsvector_byid),
        builtin(4215, "json_to_tsvector", 2, fc_json_to_tsvector),
        builtin(3746, "to_tsquery_byid", 2, fc_to_tsquery_byid),
        builtin(3750, "to_tsquery", 1, fc_to_tsquery),
        builtin(3747, "plainto_tsquery_byid", 2, fc_plainto_tsquery_byid),
        builtin(3751, "plainto_tsquery", 1, fc_plainto_tsquery),
        builtin(5006, "phraseto_tsquery_byid", 2, fc_phraseto_tsquery_byid),
        builtin(5001, "phraseto_tsquery", 1, fc_phraseto_tsquery),
        builtin(5007, "websearch_to_tsquery_byid", 2, fc_websearch_to_tsquery_byid),
        builtin(5009, "websearch_to_tsquery", 1, fc_websearch_to_tsquery),
        builtin(3759, "get_current_ts_config", 0, fc_get_current_ts_config),
        // ts_headline (text) — wparser.c
        builtin(3743, "ts_headline_byid_opt", 4, fc_ts_headline_byid_opt),
        builtin(3744, "ts_headline_byid", 3, fc_ts_headline_byid),
        builtin(3754, "ts_headline_opt", 3, fc_ts_headline_opt),
        builtin(3755, "ts_headline", 2, fc_ts_headline),
        // ts_headline (jsonb)
        builtin(4201, "ts_headline_jsonb_byid_opt", 4, fc_ts_headline_jsonb_byid_opt),
        builtin(4202, "ts_headline_jsonb_byid", 3, fc_ts_headline_jsonb_byid),
        builtin(4203, "ts_headline_jsonb_opt", 3, fc_ts_headline_jsonb_opt),
        builtin(4204, "ts_headline_jsonb", 2, fc_ts_headline_jsonb),
        // ts_headline (json)
        builtin(4205, "ts_headline_json_byid_opt", 4, fc_ts_headline_json_byid_opt),
        builtin(4206, "ts_headline_json_byid", 3, fc_ts_headline_json_byid),
        builtin(4207, "ts_headline_json_opt", 3, fc_ts_headline_json_opt),
        builtin(4208, "ts_headline_json", 2, fc_ts_headline_json),
        // Default parser methods (wparser_def.c): resolution-only registrations
        // (see fc_prsd_internal_only). hlparsetext/prsd_headline call the typed
        // Rust bodies directly; these satisfy lookup_ts_parser_cache's eager
        // fmgr_info on startOid/tokenOid/endOid/headlineOid/lextypeOid.
        builtin(3717, "prsd_start", 2, fc_prsd_internal_only),
        builtin(3718, "prsd_nexttoken", 3, fc_prsd_internal_only),
        builtin(3719, "prsd_end", 1, fc_prsd_internal_only),
        builtin(3720, "prsd_headline", 3, fc_prsd_internal_only),
        builtin(3721, "prsd_lextype", 1, fc_prsd_internal_only),
    ]);
}
