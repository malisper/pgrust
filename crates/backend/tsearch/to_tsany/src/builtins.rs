use ::datum::{Datum, Varlena};
use ::mcx::Mcx;
use ::ts_parse::{parsetext, ParsedText};
use ::types_core::primitive::InvalidOid;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    function_call2_coll, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::cache_bind;
use crate::env::CacheEnv;
use crate::query_bind;
use crate::vector::make_tsvector;
use crate::{OP_AND, OP_PHRASE, P_TSQ_PLAIN, P_TSQ_WEB};

const F_TS_MATCH_VQ: Oid = 3634;

fn to_tsvector_image<'mcx>(mcx: Mcx<'mcx>, cfg: Oid, data: &[u8]) -> PgResult<Datum> {
    let mut env = CacheEnv::new(mcx, cfg)?;
    let cap = (data.len() / 6).clamp(2, 1 << 20);
    let mut prs = ParsedText::with_capacity(mcx, cap)?;
    parsetext(mcx, &mut env, &mut prs, data)?;
    let img = make_tsvector(mcx, &mut prs)?;
    Ok(varlena_result(Varlena::from_image(img)))
}

fn text_data(fcinfo: &Fcinfo, i: usize) -> PgResult<&[u8]> {
    // SAFETY: catalog arg type of every registered fn at index `i` is text.
    Ok(unsafe { fcinfo.arg_varlena_packed(i) }?.data())
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
    function_call2_coll(&mut fi, InvalidOid, v, q)
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
    function_call2_coll(&mut fi, InvalidOid, v, q)
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
];
