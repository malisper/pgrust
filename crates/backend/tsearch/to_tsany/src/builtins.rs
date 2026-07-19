use ::datum::{Datum, Varlena};
use ::mcx::Mcx;
use ::ts_parse::{parsetext, ParsedText};
use ::types_core::primitive::InvalidOid;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    function_call2_coll_in, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::cache_bind;
use crate::env::CacheEnv;
use crate::query_bind;
use crate::vector::make_tsvector;
use crate::{OP_AND, OP_PHRASE, P_TSQ_PLAIN, P_TSQ_WEB};

const F_TS_MATCH_VQ: Oid = 3634;

fn to_tsvector_image<'mcx>(mcx: Mcx<'mcx>, cfg: Oid, data: &[u8]) -> PgResult<Datum> {
    // Config resolution happens inside parsetext (prs_start), which C's
    // to_tsvector reaches unconditionally — even for empty text.
    let mut env = CacheEnv::new(mcx, cfg);
    let cap = (data.len() / 6).clamp(2, 1 << 20);
    let mut prs = ParsedText::with_capacity(mcx, cap)?;
    parsetext(mcx, &mut env, &mut prs, data)?;
    let img = make_tsvector(mcx, &mut prs)?;
    Ok(varlena_result(Varlena::from_image(img)))
}

pub(crate) fn text_data(fcinfo: &Fcinfo, i: usize) -> PgResult<&[u8]> {
    // SAFETY: catalog arg type of every registered fn at index `i` is text.
    Ok(unsafe { fcinfo.arg_varlena_packed(i) }?.data())
}

// The tsquery arg re-framed as its 4-byte-header varlena image.
pub(crate) fn tsquery_image<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &Fcinfo,
    i: usize,
) -> PgResult<&'mcx [u8]> {
    // SAFETY: strict fn: the tsquery arg is a non-null live varlena.
    let packed = unsafe { fcinfo.arg_varlena_packed(i) }?;
    if packed.is_short() {
        let data = packed.data();
        let mut img = ::mcx::vec_with_capacity_in(mcx, 4 + data.len())?;
        ::mcx::vec_append_bytes(&mut img, &::datum::varlena::set_varsize_4b(4 + data.len()))?;
        ::mcx::vec_append_bytes(&mut img, data)?;
        Ok(img.leak())
    } else {
        // SAFETY: 4B-header varlena spanning its varsize.
        Ok(unsafe {
            core::slice::from_raw_parts(
                packed.as_ptr(),
                ::types_tuple::varatt::varsize_any(packed.as_ptr()),
            )
        })
    }
}

pub fn fc_to_tsvector_byid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    to_tsvector_image(fcinfo.result_mcx(), cfg, text_data(fcinfo, 1)?)
}

pub fn fc_to_tsvector(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = cache_bind::current_config()?;
    to_tsvector_image(fcinfo.result_mcx(), cfg, text_data(fcinfo, 0)?)
}

fn tsquery_common(
    fcinfo: &mut Fcinfo,
    cfg: Option<Oid>,
    flags: i32,
    qoperator: i8,
) -> PgResult<Datum> {
    let (cfg, arg) = match cfg {
        Some(c) => (c, 1),
        None => (cache_bind::current_config()?, 0),
    };
    query_bind::morph_to_tsquery(
        fcinfo.result_mcx(),
        cfg,
        text_data(fcinfo, arg)?,
        flags,
        qoperator,
    )
}

pub fn fc_to_tsquery_byid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    tsquery_common(fcinfo, Some(cfg), 0, OP_PHRASE)
}

pub fn fc_to_tsquery(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    tsquery_common(fcinfo, None, 0, OP_PHRASE)
}

pub fn fc_plainto_tsquery_byid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    tsquery_common(fcinfo, Some(cfg), P_TSQ_PLAIN, OP_AND)
}

pub fn fc_plainto_tsquery(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    tsquery_common(fcinfo, None, P_TSQ_PLAIN, OP_AND)
}

pub fn fc_phraseto_tsquery_byid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    tsquery_common(fcinfo, Some(cfg), P_TSQ_PLAIN, OP_PHRASE)
}

pub fn fc_phraseto_tsquery(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    tsquery_common(fcinfo, None, P_TSQ_PLAIN, OP_PHRASE)
}

pub fn fc_websearch_to_tsquery_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    tsquery_common(fcinfo, Some(cfg), P_TSQ_WEB, OP_PHRASE)
}

pub fn fc_websearch_to_tsquery(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    tsquery_common(fcinfo, None, P_TSQ_WEB, OP_PHRASE)
}

// tsvector_op.c ts_match_tt/ts_match_tq, hosted here so the text->tsvector
// morphs need no adt_tsvector_core -> to_tsany dependency edge.
pub fn fc_ts_match_tt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let cfg = cache_bind::current_config()?;
    let v = to_tsvector_image(mcx, cfg, text_data(fcinfo, 0)?)?;
    let q = query_bind::morph_to_tsquery(mcx, cfg, text_data(fcinfo, 1)?, P_TSQ_PLAIN, OP_AND)?;
    let mut fi = ::fmgr_seams::fmgr_info::call(F_TS_MATCH_VQ)?;
    // C DirectFunctionCall2: the callee pallocs in the caller's context.
    function_call2_coll_in(&mut fi, InvalidOid, mcx, v, q)
}

pub fn fc_ts_match_tq(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let cfg = cache_bind::current_config()?;
    let v = to_tsvector_image(mcx, cfg, text_data(fcinfo, 0)?)?;
    // SAFETY: arg 1 is a tsquery varlena.
    let packed = unsafe { fcinfo.arg_varlena_packed(1) }?;
    let q = if packed.is_short() {
        // The value cores decode at fixed 4-byte-header offsets; re-frame.
        let data = packed.data();
        let mut img = ::mcx::vec_with_capacity_in(mcx, 4 + data.len())?;
        ::mcx::vec_append_bytes(&mut img, &[0u8; 4])?;
        ::mcx::vec_append_bytes(&mut img, data)?;
        varlena_result(Varlena::from_image(img))
    } else {
        Datum::from_usize(packed.as_ptr() as usize)
    };
    let mut fi = ::fmgr_seams::fmgr_info::call(F_TS_MATCH_VQ)?;
    // C DirectFunctionCall2: the callee pallocs in the caller's context.
    function_call2_coll_in(&mut fi, InvalidOid, mcx, v, q)
}

// wparser.c ts_headline family, hosted here (the CacheEnv parser drive and
// ts_cache live on this side; wparser_def's prsd_headline is reached through
// fmgr by the catalog prsheadline oid).
fn ts_headline_common(fcinfo: &mut Fcinfo, cfg: Option<Oid>, has_opts: bool) -> PgResult<Datum> {
    use ::ts_parse::headline::{generate_headline, hlparsetext, HeadlineParsedText};

    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (cfg, base) = match cfg {
        Some(c) => (c, 1usize),
        None => (cache_bind::current_config()?, 0usize),
    };
    let text = text_data(fcinfo, base)?;

    // The tsquery rides to prsd_headline as its 4-byte-header varlena image.
    let qimage = tsquery_image(mcx, fcinfo, base + 1)?;
    let query = ::adt_tsvector_core::query::TsQueryRef { payload: &qimage[4..] };

    // C (wparser.c ts_headline_byid_opt): the config and parser are resolved
    // BEFORE the options list is parsed — a bogus config must win over bogus
    // options (fnconf batch-1 ts-config family: cfg 0 + 'bogus' options →
    // 'cache lookup failed for text search configuration 0', not the
    // deflist error).
    let map = cache_bind::config_map(mcx, cfg)?;
    let prsentry = ::ts_cache::lookup_ts_parser_cache(map.prs_id)?;
    if prsentry.headline_oid == InvalidOid {
        return Err(Box::new(
            ::types_error::PgError::error(
                "text search parser does not support headline creation".to_string(),
            )
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let opts = if has_opts {
        Some(::ts_cache::deserialize_deflist(mcx, text_data(fcinfo, base + 2)?)?)
    } else {
        None
    };

    let mut prs = HeadlineParsedText::new(mcx);
    let mut env = CacheEnv::new(mcx, cfg);
    hlparsetext(mcx, &mut env, &mut prs, query, text)?;

    let opts_datum = match &opts {
        Some(v) => Datum::from_usize(v as *const _ as usize),
        None => Datum::from_usize(0),
    };
    let mut fi = ::fmgr_seams::fmgr_info::call(prsentry.headline_oid)?;
    // The parser's headline proc (prsd_headline) builds by-ref results
    // through its frame's result mcx; thread the outer call's context.
    ::types_fmgr::function_call3_coll_in(
        &mut fi,
        InvalidOid,
        mcx,
        Datum::from_usize(core::ptr::from_mut(&mut prs) as usize),
        opts_datum,
        Datum::from_usize(qimage.as_ptr() as usize),
    )?;

    let body = generate_headline(mcx, &prs)?;
    let mut img = ::mcx::vec_with_capacity_in(mcx, 4 + body.len())?;
    ::mcx::vec_append_bytes(&mut img, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut img, &body)?;
    Ok(varlena_result(Varlena::from_image(img)))
}

pub fn fc_ts_headline_byid_opt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    ts_headline_common(fcinfo, Some(cfg), true)
}

pub fn fc_ts_headline_byid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    ts_headline_common(fcinfo, Some(cfg), false)
}

pub fn fc_ts_headline_opt(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ts_headline_common(fcinfo, None, true)
}

pub fn fc_ts_headline(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ts_headline_common(fcinfo, None, false)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TO_TSANY_BUILTINS: &[FmgrBuiltin] = &[
    b(3745, "to_tsvector_byid", 2, fc_to_tsvector_byid),
    b(3746, "to_tsquery_byid", 2, fc_to_tsquery_byid),
    b(3747, "plainto_tsquery_byid", 2, fc_plainto_tsquery_byid),
    b(3749, "to_tsvector", 1, fc_to_tsvector),
    b(3750, "to_tsquery", 1, fc_to_tsquery),
    b(3751, "plainto_tsquery", 1, fc_plainto_tsquery),
    b(3760, "ts_match_tt", 2, fc_ts_match_tt),
    b(3761, "ts_match_tq", 2, fc_ts_match_tq),
    b(5001, "phraseto_tsquery", 1, fc_phraseto_tsquery),
    b(5006, "phraseto_tsquery_byid", 2, fc_phraseto_tsquery_byid),
    b(5007, "websearch_to_tsquery_byid", 2, fc_websearch_to_tsquery_byid),
    b(5009, "websearch_to_tsquery", 1, fc_websearch_to_tsquery),
    b(3743, "ts_headline_byid_opt", 4, fc_ts_headline_byid_opt),
    b(3744, "ts_headline_byid", 3, fc_ts_headline_byid),
    b(3754, "ts_headline_opt", 3, fc_ts_headline_opt),
    b(3755, "ts_headline", 2, fc_ts_headline),
    b(4201, "ts_headline_jsonb_byid_opt", 4, crate::json::fc_ts_headline_jsonb_byid_opt),
    b(4202, "ts_headline_jsonb_byid", 3, crate::json::fc_ts_headline_jsonb_byid),
    b(4203, "ts_headline_jsonb_opt", 3, crate::json::fc_ts_headline_jsonb_opt),
    b(4204, "ts_headline_jsonb", 2, crate::json::fc_ts_headline_jsonb),
    b(4205, "ts_headline_json_byid_opt", 4, crate::json::fc_ts_headline_json_byid_opt),
    b(4206, "ts_headline_json_byid", 3, crate::json::fc_ts_headline_json_byid),
    b(4207, "ts_headline_json_opt", 3, crate::json::fc_ts_headline_json_opt),
    b(4208, "ts_headline_json", 2, crate::json::fc_ts_headline_json),
    b(4209, "jsonb_string_to_tsvector", 1, crate::json::fc_jsonb_string_to_tsvector),
    b(4210, "json_string_to_tsvector", 1, crate::json::fc_json_string_to_tsvector),
    b(4211, "jsonb_string_to_tsvector_byid", 2, crate::json::fc_jsonb_string_to_tsvector_byid),
    b(4212, "json_string_to_tsvector_byid", 2, crate::json::fc_json_string_to_tsvector_byid),
    b(4213, "jsonb_to_tsvector", 2, crate::json::fc_jsonb_to_tsvector),
    b(4214, "jsonb_to_tsvector_byid", 3, crate::json::fc_jsonb_to_tsvector_byid),
    b(4215, "json_to_tsvector", 2, crate::json::fc_json_to_tsvector),
    b(4216, "json_to_tsvector_byid", 3, crate::json::fc_json_to_tsvector_byid),
];
