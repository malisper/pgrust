//! `ts_token_type(*)` / `ts_parse(*)` (wparser.c) registered as executor-frame
//! materialize-mode set-returning functions.
//!
//! In C these are ValuePerCall SRFs (`SRF_FIRSTCALL_INIT` /
//! `SRF_RETURN_NEXT` / `SRF_RETURN_DONE`) that build their result set in the
//! `multi_call_memory_ctx` on the first call (`tt_setup_firstcall` /
//! `prs_setup_firstcall`) and emit one tuple per call. The owned port drives the
//! same data-producing cores —
//! [`backend_tsearch_parse::tt_storage_list`] (the parser's `(lexid, alias,
//! descr)` token-type descriptors) and [`backend_tsearch_parse::prs_tokenize`]
//! (the `prsstart`/`prstoken`/`prsend` tokenization over the input text) — over
//! the materialize-mode SRF protocol: `InitMaterializedSRF` builds the
//! tuplestore from the executor's expected row descriptor, every row is appended
//! via `materialized_srf_putvalues`, and the entry returns SQL NULL.
//!
//! Parser dispatch: C calls `lookup_ts_parser_cache(prsid)` and then
//! `OidFunctionCall1(prs->lextypeOid, 0)` / `FunctionCall2(&prs->prsstart, ...)`.
//! Only the default word parser (OID 3722, whose `prsstart`/`prstoken`/`prsend`/
//! `prslextype` are `prsd_*` ported in `wparser_def.c`) is registered and
//! ported in this tree, so we resolve the cache entry and dispatch its OIDs to
//! the in-crate default-parser bodies; a non-default parser raises a clear error
//! (matching the port invariant that the generic fmgr lane never carries the
//! internal `TParser *`/`LexDescr *` ABI).
//!
//! Registered from [`register_ts_parse`] (called by `init_seams`) — the
//! executor-frame `fmgr_builtins[]` analogue, bypassing the by-OID builtin
//! registry whose tag-only `resultinfo` cannot carry the live `ReturnSetInfo`.

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::{Oid, OidIsValid};
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_error::{PgResult, ERROR};
use backend_utils_error::ereport;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};
use backend_utils_cache_ts_cache::lookup_ts_parser_cache;

use crate::register_srf;

/// `ts_token_type(oid)` / `ts_token_type_byid` (OID 3713).
const TS_TOKEN_TYPE_BYID: Oid = 3713;
/// `ts_token_type(text)` / `ts_token_type_byname` (OID 3714).
const TS_TOKEN_TYPE_BYNAME: Oid = 3714;
/// `ts_parse(oid, text)` / `ts_parse_byid` (OID 3715).
const TS_PARSE_BYID: Oid = 3715;
/// `ts_parse(text, text)` / `ts_parse_byname` (OID 3716).
const TS_PARSE_BYNAME: Oid = 3716;

/// `F_PRSD_LEXTYPE` (pg_proc.dat): the default parser's `prslextype` method.
const F_PRSD_LEXTYPE: Oid = 3721;
/// `F_PRSD_START` (pg_proc.dat): the default parser's `prsstart` method.
const F_PRSD_START: Oid = 3717;

/// Register the `ts_token_type` / `ts_parse` SRFs in the executor-frame table.
pub(crate) fn register_ts_parse() {
    register_srf(TS_TOKEN_TYPE_BYID, ts_token_type_byid);
    register_srf(TS_TOKEN_TYPE_BYNAME, ts_token_type_byname);
    register_srf(TS_PARSE_BYID, ts_parse_byid);
    register_srf(TS_PARSE_BYNAME, ts_parse_byname);
}

/// `PG_GETARG_OID(i)` on the by-value lane.
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.args[i].value.as_oid()
}

/// `VARDATA_ANY(PG_GETARG_TEXT_PP(i))`: the header-stripped payload of a `text`
/// arg on the by-ref lane.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    const VARHDRSZ: usize = 4;
    match fcinfo.ref_arg(i) {
        Some(FmgrArgRef::Varlena(b)) => {
            let image = b.as_slice();
            if image.len() >= VARHDRSZ {
                &image[VARHDRSZ..]
            } else {
                &[]
            }
        }
        _ => panic!("ts_parse/ts_token_type: text argument missing from the by-ref lane"),
    }
}

/// `get_ts_parser_oid(textToQualifiedNameList(prsname), false)`.
fn resolve_parser_byname(mcx: Mcx<'_>, name: &[u8]) -> PgResult<Oid> {
    let parts = backend_utils_adt_varlena_seams::text_to_qualified_name_list::call(mcx, name)?;
    let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
    backend_catalog_namespace_seams::get_ts_parser_oid::call(mcx, &refs, false)
}

/// Verify the parser cache entry's `prslextype` method is the default parser's
/// `prsd_lextype` (the only one ported); otherwise the generic fmgr lane cannot
/// carry the internal `LexDescr *` ABI.
fn require_default_lextype(prsid: Oid) -> PgResult<()> {
    let prs = lookup_ts_parser_cache(prsid)?;
    if !OidIsValid(prs.lextypeOid) {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "method lextype isn't defined for text search parser {prsid}"
            ))
            .into_error());
    }
    if prs.lextypeOid != F_PRSD_LEXTYPE {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "text search parser {prsid} uses an unported lextype method (OID {})",
                prs.lextypeOid
            ))
            .into_error());
    }
    Ok(())
}

/// Verify the parser cache entry's `prsstart` method is the default parser's
/// `prsd_start` (the only one ported).
fn require_default_start(prsid: Oid) -> PgResult<()> {
    let prs = lookup_ts_parser_cache(prsid)?;
    if prs.startOid != F_PRSD_START {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "text search parser {prsid} uses an unported start method (OID {})",
                prs.startOid
            ))
            .into_error());
    }
    Ok(())
}

/// Emit the `(tokid int4, alias text, description text)` token-type rows.
fn emit_token_type<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // tt_setup_firstcall: st->list = OidFunctionCall1(prs->lextypeOid, 0). For
    // the default parser that is prsd_lextype; the in-crate core returns the
    // list with the trailing lexid==0 sentinel dropped.
    let rows = backend_tsearch_parse::tt_storage_list();

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("ts_token_type: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in rows {
        // C: values[0]=txtid("%d", lexid), values[1]=alias, values[2]=descr,
        // all coerced through BuildTupleFromCStrings; the int4 input function
        // parses the integer. We build the typed Datums directly.
        let alias = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &row.alias)?;
        let descr = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &row.descr)?;
        let values = [Datum::from_i32(row.lexid), alias, descr];
        materialized_srf_putvalues(rsinfo, &values, &[false, false, false])?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `ts_token_type_byid(PG_FUNCTION_ARGS)` (wparser.c:105).
fn ts_token_type_byid<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("ts_token_type_byid: fn_mcxt set by ExecMakeTableFunctionResult");
    let prsid = arg_oid(fcinfo, 0);
    require_default_lextype(prsid)?;
    emit_token_type(fcinfo, mcx)
}

/// `ts_token_type_byname(PG_FUNCTION_ARGS)` (wparser.c:124).
fn ts_token_type_byname<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("ts_token_type_byname: fn_mcxt set by ExecMakeTableFunctionResult");
    let prsname = arg_text(fcinfo, 0).to_vec();
    let prsid = resolve_parser_byname(mcx, &prsname)?;
    require_default_lextype(prsid)?;
    emit_token_type(fcinfo, mcx)
}

/// Emit the `(tokid int4, token text)` rows from tokenizing `txt`.
fn emit_parse<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    mcx: Mcx<'mcx>,
    txt: &[u8],
) -> PgResult<Datum<'mcx>> {
    // prs_setup_firstcall: run prsstart/prstoken/prsend over txt, collecting
    // every (type, lexeme). The default parser's bodies live in wparser_def.c.
    let rows = backend_tsearch_parse::prs_tokenize(txt)?;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("ts_parse: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in rows {
        // C: values[0]=tid("%d", type), values[1]=lexeme (NUL-terminated copy).
        let token = backend_utils_adt_varlena_seams::cstring_to_text_v::call(
            mcx,
            core::str::from_utf8(&row.lexeme).expect("ts_parse: token is valid UTF-8 text"),
        )?;
        let values = [Datum::from_i32(row.type_), token];
        materialized_srf_putvalues(rsinfo, &values, &[false, false])?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `ts_parse_byid(PG_FUNCTION_ARGS)` (wparser.c:241).
fn ts_parse_byid<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("ts_parse_byid: fn_mcxt set by ExecMakeTableFunctionResult");
    let prsid = arg_oid(fcinfo, 0);
    let txt = arg_text(fcinfo, 1).to_vec();
    require_default_start(prsid)?;
    emit_parse(fcinfo, mcx, &txt)
}

/// `ts_parse_byname(PG_FUNCTION_ARGS)` (wparser.c:263).
fn ts_parse_byname<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("ts_parse_byname: fn_mcxt set by ExecMakeTableFunctionResult");
    let prsname = arg_text(fcinfo, 0).to_vec();
    let txt = arg_text(fcinfo, 1).to_vec();
    let prsid = resolve_parser_byname(mcx, &prsname)?;
    require_default_start(prsid)?;
    emit_parse(fcinfo, mcx, &txt)
}
