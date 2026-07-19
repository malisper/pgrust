//! to_tsany.c json(b)_to_tsvector family + wparser.c ts_headline json(b)
//! variants, over the adt_jsonb jsonfuncs iterate/transform lanes.

use ::adt_jsonb::iterate::{
    iterate_json_values, iterate_jsonb_values, parse_jsonb_index_flags,
    transform_json_string_values, transform_jsonb_string_values, JTI_STRING,
};
use ::datum::{Datum, Varlena};
use ::mcx::{Mcx, PgVec};
use ::ts_parse::headline::{generate_headline, hlparsetext, HeadlineParsedText};
use ::ts_parse::{parsetext, ParsedText, TsParseEnv};
use ::types_core::primitive::InvalidOid;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};

use crate::builtins::{text_data, tsquery_image};
use crate::cache_bind;
use crate::env::CacheEnv;
use crate::vector::make_tsvector;

// C: add_to_tsvector — pos +1 between elements so phrase search never sees
// the last word of one element adjacent to the first of the next.
fn add_to_tsvector<'mcx, E: TsParseEnv<'mcx>>(
    mcx: Mcx<'mcx>,
    env: &mut E,
    prs: &mut ParsedText<'mcx>,
    elem: &[u8],
) -> PgResult<()> {
    let prevwords = prs.words.len();
    parsetext(mcx, env, prs, elem)?;
    if prs.words.len() > prevwords {
        prs.pos += 1;
    }
    Ok(())
}

pub(crate) fn jsonb_to_tsvector_worker<'mcx, E: TsParseEnv<'mcx>>(
    mcx: Mcx<'mcx>,
    env: &mut E,
    jb: &[u8],
    flags: u32,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut prs = ParsedText::with_capacity(mcx, 16)?;
    iterate_jsonb_values(mcx, jb, flags, &mut |elem| {
        add_to_tsvector(mcx, env, &mut prs, elem)
    })?;
    make_tsvector(mcx, &mut prs)
}

pub(crate) fn json_to_tsvector_worker<'mcx, E: TsParseEnv<'mcx>>(
    mcx: Mcx<'mcx>,
    env: &mut E,
    json: &[u8],
    flags: u32,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut prs = ParsedText::with_capacity(mcx, 16)?;
    iterate_json_values(mcx, json, flags, &mut |elem| {
        add_to_tsvector(mcx, env, &mut prs, elem)
    })?;
    make_tsvector(mcx, &mut prs)
}

fn jsonb_variant(fcinfo: &mut Fcinfo, cfg: Option<Oid>, flags_arg: Option<usize>) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (base, cfg_known) = match cfg {
        Some(c) => (1usize, Some(c)),
        None => (0usize, None),
    };
    // SAFETY: strict fn; the jsonb args are non-null live varlenas.
    let jb = unsafe { ::adt_jsonb::builtins::jsonb_payload_from_datum(mcx, fcinfo.arg(base)) }?;
    let flags = match flags_arg {
        Some(i) => {
            // SAFETY: strict fn; non-null jsonb flags arg.
            let fl = unsafe {
                ::adt_jsonb::builtins::jsonb_payload_from_datum(mcx, fcinfo.arg(i))
            }?;
            parse_jsonb_index_flags(mcx, fl.as_bytes())?
        }
        None => JTI_STRING,
    };
    let cfg = match cfg_known {
        Some(c) => c,
        None => cache_bind::current_config()?,
    };
    // C (to_tsany.c jsonb_to_tsvector_worker): the config is only resolved
    // when a json value actually reaches parsetext — '{}'::jsonb with a
    // bogus config succeeds with an empty tsvector (fnconf batch-1).
    let mut env = CacheEnv::new(mcx, cfg);
    let img = jsonb_to_tsvector_worker(mcx, &mut env, jb.as_bytes(), flags)?;
    Ok(varlena_result(Varlena::from_image(img)))
}

fn json_variant(fcinfo: &mut Fcinfo, cfg: Option<Oid>, flags_arg: Option<usize>) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (base, cfg_known) = match cfg {
        Some(c) => (1usize, Some(c)),
        None => (0usize, None),
    };
    let json = text_data(fcinfo, base)?;
    let flags = match flags_arg {
        Some(i) => {
            // SAFETY: strict fn; non-null jsonb flags arg.
            let fl = unsafe {
                ::adt_jsonb::builtins::jsonb_payload_from_datum(mcx, fcinfo.arg(i))
            }?;
            parse_jsonb_index_flags(mcx, fl.as_bytes())?
        }
        None => JTI_STRING,
    };
    let cfg = match cfg_known {
        Some(c) => c,
        None => cache_bind::current_config()?,
    };
    // Same laziness as the jsonb lane (C json_to_tsvector_worker).
    let mut env = CacheEnv::new(mcx, cfg);
    let img = json_to_tsvector_worker(mcx, &mut env, json, flags)?;
    Ok(varlena_result(Varlena::from_image(img)))
}

pub fn fc_jsonb_string_to_tsvector_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    jsonb_variant(fcinfo, Some(cfg), None)
}

pub fn fc_jsonb_string_to_tsvector(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_variant(fcinfo, None, None)
}

pub fn fc_jsonb_to_tsvector_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    jsonb_variant(fcinfo, Some(cfg), Some(2))
}

pub fn fc_jsonb_to_tsvector(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    jsonb_variant(fcinfo, None, Some(1))
}

pub fn fc_json_string_to_tsvector_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    json_variant(fcinfo, Some(cfg), None)
}

pub fn fc_json_string_to_tsvector(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    json_variant(fcinfo, None, None)
}

pub fn fc_json_to_tsvector_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    json_variant(fcinfo, Some(cfg), Some(2))
}

pub fn fc_json_to_tsvector(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    json_variant(fcinfo, None, Some(1))
}

// C: HeadlineJsonState + headline_json_value (wparser.c).
struct HeadlineJsonState<'mcx> {
    mcx: Mcx<'mcx>,
    env: CacheEnv<'mcx>,
    prs: HeadlineParsedText<'mcx>,
    opts: Option<PgVec<'mcx, ::ts_cache::DefListItem<'mcx>>>,
    headline_fi: FmgrInfo,
    qimage: &'mcx [u8],
}

impl<'mcx> HeadlineJsonState<'mcx> {
    fn setup(fcinfo: &mut Fcinfo, cfg: Option<Oid>, has_opts: bool) -> PgResult<Self> {
        // SAFETY: the armed result mcx outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let (cfg, base) = match cfg {
            Some(c) => (c, 1usize),
            None => (cache_bind::current_config()?, 0usize),
        };
        let qimage = tsquery_image(mcx, fcinfo, base + 1)?;
        // C (wparser.c ts_headline_jsonb_byid_opt): config + parser resolve
        // BEFORE the options list is parsed (fnconf batch-1 ts-config
        // family — a bogus config must win over bogus options).
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
            Some(::ts_cache::deserialize_deflist(
                mcx,
                text_data(fcinfo, base + 2)?,
            )?)
        } else {
            None
        };
        Ok(HeadlineJsonState {
            mcx,
            env: CacheEnv::new(mcx, cfg),
            prs: HeadlineParsedText::new(mcx),
            opts,
            headline_fi: ::fmgr_seams::fmgr_info::call(prsentry.headline_oid)?,
            qimage,
        })
    }

    // C: headline_json_value — prs->curwords = 0, then the text lane.
    fn value(&mut self, elem: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
        self.prs.words.clear();
        let query = ::adt_tsvector_core::query::TsQueryRef { payload: &self.qimage[4..] };
        hlparsetext(self.mcx, &mut self.env, &mut self.prs, query, elem)?;
        let opts_datum = match &self.opts {
            Some(v) => Datum::from_usize(v as *const _ as usize),
            None => Datum::from_usize(0),
        };
        // The parser's headline proc (prsd_headline) builds by-ref results
        // through its frame's result mcx; thread the outer call's context.
        ::types_fmgr::function_call3_coll_in(
            &mut self.headline_fi,
            InvalidOid,
            self.mcx,
            Datum::from_usize(core::ptr::from_mut(&mut self.prs) as usize),
            opts_datum,
            Datum::from_usize(self.qimage.as_ptr() as usize),
        )?;
        generate_headline(self.mcx, &self.prs)
    }
}

fn ts_headline_jsonb_common(
    fcinfo: &mut Fcinfo,
    cfg: Option<Oid>,
    has_opts: bool,
) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let base = if cfg.is_some() { 1usize } else { 0 };
    let mut st = HeadlineJsonState::setup(fcinfo, cfg, has_opts)?;
    // SAFETY: strict fn; non-null jsonb arg.
    let jb = unsafe { ::adt_jsonb::builtins::jsonb_payload_from_datum(mcx, fcinfo.arg(base)) }?;
    let img = transform_jsonb_string_values(mcx, jb.as_bytes(), &mut |elem| {
        Ok(st.value(elem)?.leak())
    })?;
    Ok(varlena_result(Varlena::from_image(img)))
}

fn ts_headline_json_common(
    fcinfo: &mut Fcinfo,
    cfg: Option<Oid>,
    has_opts: bool,
) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let base = if cfg.is_some() { 1usize } else { 0 };
    let mut st = HeadlineJsonState::setup(fcinfo, cfg, has_opts)?;
    let json = text_data(fcinfo, base)?;
    let body = transform_json_string_values(mcx, json, &mut |elem| st.value(elem))?;
    let mut img = ::mcx::vec_with_capacity_in(mcx, 4 + body.len())?;
    ::mcx::vec_append_bytes(&mut img, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut img, &body)?;
    Ok(varlena_result(Varlena::from_image(img)))
}

pub fn fc_ts_headline_jsonb_byid_opt(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    ts_headline_jsonb_common(fcinfo, Some(cfg), true)
}

pub fn fc_ts_headline_jsonb_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    ts_headline_jsonb_common(fcinfo, Some(cfg), false)
}

pub fn fc_ts_headline_jsonb_opt(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ts_headline_jsonb_common(fcinfo, None, true)
}

pub fn fc_ts_headline_jsonb(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ts_headline_jsonb_common(fcinfo, None, false)
}

pub fn fc_ts_headline_json_byid_opt(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    ts_headline_json_common(fcinfo, Some(cfg), true)
}

pub fn fc_ts_headline_json_byid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cfg = fcinfo.arg_oid(0);
    ts_headline_json_common(fcinfo, Some(cfg), false)
}

pub fn fc_ts_headline_json_opt(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ts_headline_json_common(fcinfo, None, true)
}

pub fn fc_ts_headline_json(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ts_headline_json_common(fcinfo, None, false)
}
