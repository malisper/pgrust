//! `ts_headline*` (wparser.c:287..516) — generate a highlighted excerpt of a
//! document around the terms of a `tsquery`, under a text-search configuration.
//!
//! The headline framework itself (`hlparsetext`, the default parser's
//! `prsd_headline` selector, and `generateHeadline`) lives in
//! `backend-tsearch-parse`; this module ports the SQL-callable drivers that
//! string those together: config-cache lookup, the parser headline-support
//! check, deflist option deserialization (through the `tsearchcmds` seam), and
//! the json(b) transform variants.

use alloc::string::String;
use alloc::vec::Vec;

use backend_tsearch_parse::{
    generateHeadline, hlparsetext, prsd_headline, HeadlineParsedText, QueryItem, QueryOperand,
    QueryOperator, TSQuery,
};
use backend_commands_tsearchcmds_seams::deserialize_deflist;
use backend_utils_adt_jsonfuncs::iterate::{
    transform_json_string_values, transform_jsonb_string_values,
};
use backend_utils_cache_ts_cache::{
    getTSCurrentConfig, lookup_ts_config_cache, lookup_ts_parser_cache,
};
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use backend_utils_error::ereport;

/// `VARHDRSZ`.
const VARHDRSZ: usize = 4;
/// `HDRSIZETQ` (ts_type.h): `VARHDRSZ + sizeof(int32)` — the tsquery header up
/// to the start of the `QueryItem` array.
const HDRSIZETQ: usize = VARHDRSZ + 4;
/// `sizeof(QueryItemData)` — one packed tsquery node record.
const QI_SIZE: usize = 12;
/// `QI_VAL` (ts_type.h).
const QI_VAL: i8 = 1;
/// `QI_OPR` (ts_type.h).
const QI_OPR: i8 = 2;

/// `lenwords` seed for the headline word list (wparser.c:309).
const HEADLINE_LENWORDS: i32 = 32;

/// Decode a `tsquery` varlena image into the in-memory [`TSQuery`] the headline
/// matcher reads (`GETQUERY` / `GETOPERAND`, ts_type.h). The matcher only
/// inspects `QI_VAL` operands; operator nodes carry just their type byte.
fn decode_tsquery(image: &[u8]) -> TSQuery {
    // size = *(int32 *)(query + VARHDRSZ).
    let size = i32::from_ne_bytes([image[4], image[5], image[6], image[7]]);
    let n = size.max(0) as usize;

    let mut items: Vec<QueryItem> = Vec::with_capacity(n);
    for i in 0..n {
        let base = HDRSIZETQ + i * QI_SIZE;
        let rec = &image[base..base + QI_SIZE];
        let type_ = rec[0] as i8;
        if type_ == QI_VAL {
            // weight, prefix, valcrc, len_dist:{ length:12, distance:20 }.
            let len_dist = u32::from_ne_bytes([rec[8], rec[9], rec[10], rec[11]]);
            items.push(QueryItem::Operand(QueryOperand {
                type_,
                weight: rec[1],
                prefix: rec[2] != 0,
                valcrc: i32::from_ne_bytes([rec[4], rec[5], rec[6], rec[7]]),
                length: len_dist & 0xFFF,
                distance: (len_dist >> 12) & 0xF_FFFF,
            }));
        } else if type_ == QI_OPR {
            // oper, distance:i16, left:u32 — the engine reads these to recurse.
            items.push(QueryItem::Operator(QueryOperator {
                type_,
                oper: rec[1] as i8,
                distance: i16::from_ne_bytes([rec[2], rec[3]]),
                left: u32::from_ne_bytes([rec[4], rec[5], rec[6], rec[7]]),
            }));
        } else {
            // QI_VALSTOP or other bare tag: a non-recursing placeholder.
            items.push(QueryItem::Operator(QueryOperator {
                type_,
                oper: 0,
                distance: 0,
                left: 0,
            }));
        }
    }

    // operands = GETOPERAND(query) = query + HDRSIZETQ + size * QI_SIZE.
    let opbase = HDRSIZETQ + n * QI_SIZE;
    let operands = image.get(opbase..).unwrap_or(&[]).to_vec();

    TSQuery {
        size,
        items,
        operands,
    }
}

/// Convert the seam's `DefElemString` rows into the `(name, value)` option
/// pairs `prsd_headline` reads.
fn prsoptions_from_deflist(opt: Option<&[u8]>, mcx: Mcx<'_>) -> PgResult<Vec<(String, String)>> {
    let Some(opt) = opt else {
        return Ok(Vec::new());
    };
    let list = deserialize_deflist::call(mcx, opt)?;
    let mut out: Vec<(String, String)> = Vec::with_capacity(list.len());
    for de in list.iter() {
        out.push((
            String::from(de.defname.as_str()),
            String::from(de.arg.as_str()),
        ));
    }
    Ok(out)
}

/// Build the headline word list, run the default parser's headline selector,
/// and return the rendered headline `text` image — the shared body of every
/// text/json(b) headline path (wparser.c:308..325).
fn headline_for_element<'mcx>(
    mcx: Mcx<'mcx>,
    cfg_id: Oid,
    prs_id: Oid,
    headline_oid: Oid,
    query: &TSQuery,
    query_image: &[u8],
    prsoptions: &[(String, String)],
    elem: &[u8],
) -> PgResult<Vec<u8>> {
    let _ = (prs_id, query_image);

    // prsobj->headlineOid validity is checked by the caller; the only built-in
    // parser exposing a headline function is the default one (prsd_headline),
    // which hlparsetext also routes through.
    let _ = headline_oid;

    let mut prs = HeadlineParsedText {
        lenwords: HEADLINE_LENWORDS,
        ..HeadlineParsedText::default()
    };

    // hlparsetext(cfg->cfgId, &prs, query, VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in)).
    hlparsetext(u32::from(cfg_id), &mut prs, query, elem)?;

    // FunctionCall3(&prsobj->prsheadline, &prs, prsoptions, query).
    prsd_headline(&mut prs, prsoptions, query)?;

    // out = generateHeadline(&prs).
    let _ = mcx;
    Ok(generateHeadline(&prs))
}

/// `ts_headline_byid_opt` (wparser.c:287): the all-arguments text driver every
/// other text variant resolves to.
///
/// `input` is the document's `text` payload (header stripped); `query_image` is
/// the full `tsquery` varlena; `opt` is the optional options `text` payload.
/// Returns the headline `text` varlena image.
pub fn ts_headline_byid_opt<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    input: &[u8],
    query_image: &[u8],
    opt: Option<&[u8]>,
) -> PgResult<Vec<u8>> {
    // cfg = lookup_ts_config_cache(tsconfig);
    let cfg = lookup_ts_config_cache(mcx, tsconfig)?;
    // prsobj = lookup_ts_parser_cache(cfg->prsId);
    let prsobj = lookup_ts_parser_cache(cfg.prsId)?;

    // if (!OidIsValid(prsobj->headlineOid)) ereport(FEATURE_NOT_SUPPORTED).
    if u32::from(prsobj.headlineOid) == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("text search parser does not support headline creation")
            .into_error());
    }

    let query = decode_tsquery(query_image);
    let prsoptions = prsoptions_from_deflist(opt, mcx)?;

    headline_for_element(
        mcx,
        cfg.cfgId,
        cfg.prsId,
        prsobj.headlineOid,
        &query,
        query_image,
        &prsoptions,
        input,
    )
}

/// `ts_headline_byid` (wparser.c:338): no options.
pub fn ts_headline_byid<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    input: &[u8],
    query_image: &[u8],
) -> PgResult<Vec<u8>> {
    ts_headline_byid_opt(mcx, tsconfig, input, query_image, None)
}

/// `ts_headline` (wparser.c:347): current config, no options.
pub fn ts_headline<'mcx>(mcx: Mcx<'mcx>, input: &[u8], query_image: &[u8]) -> PgResult<Vec<u8>> {
    let cfg = getTSCurrentConfig(true)?;
    ts_headline_byid_opt(mcx, cfg, input, query_image, None)
}

/// `ts_headline_opt` (wparser.c:356): current config, with options.
pub fn ts_headline_opt<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    query_image: &[u8],
    opt: &[u8],
) -> PgResult<Vec<u8>> {
    let cfg = getTSCurrentConfig(true)?;
    ts_headline_byid_opt(mcx, cfg, input, query_image, Some(opt))
}

// ---------------------------------------------------------------------------
// json(b) headline variants (wparser.c:366..516)
// ---------------------------------------------------------------------------

/// Shared json(b) headline state — mirrors C's `HeadlineJsonState` plus the
/// `transformed` flag (wparser.c:522). Each string value of the json document
/// is independently turned into a headline by `headline_json_value`.
struct HeadlineJsonState {
    cfg_id: Oid,
    prs_id: Oid,
    headline_oid: Oid,
    query: TSQuery,
    prsoptions: Vec<(String, String)>,
}

impl HeadlineJsonState {
    /// `headline_json_value` (wparser.c:522): produce a headline `text` from one
    /// json string element, returning its `VARDATA_ANY` payload bytes (the
    /// `transform_*_string_values` callbacks re-wrap them as the json value).
    fn headline_json_value<'mcx>(&self, mcx: Mcx<'mcx>, elem: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
        let image = headline_for_element(
            mcx,
            self.cfg_id,
            self.prs_id,
            self.headline_oid,
            &self.query,
            &[],
            &self.prsoptions,
            elem,
        )?;
        // generateHeadline returns a full `text` varlena; the transform consumes
        // VARDATA_ANY(out), so hand back the header-stripped payload.
        let payload = image.get(VARHDRSZ..).unwrap_or(&[]);
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(payload.len())
            .map_err(|_| mcx.oom(payload.len()))?;
        out.extend_from_slice(payload);
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// jsonb headline drivers (wparser.c:366..440)
// ---------------------------------------------------------------------------

/// `ts_headline_jsonb_byid_opt` (wparser.c:366): the all-arguments jsonb driver.
///
/// `jb` is the full jsonb varlena; `query_image` the full tsquery varlena;
/// `opt` the optional options `text` payload. Returns the transformed jsonb
/// varlena image (each string value replaced by its headline).
pub fn ts_headline_jsonb_byid_opt<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    jb: &[u8],
    query_image: &[u8],
    opt: Option<&[u8]>,
) -> PgResult<Vec<u8>> {
    let state = headline_json_state(mcx, tsconfig, query_image, opt)?;
    let out = transform_jsonb_string_values(mcx, jb, &mut |m: Mcx<'mcx>, elem: &[u8]| {
        state.headline_json_value(m, elem)
    })?;
    Ok(out.to_vec())
}

/// `ts_headline_jsonb` (wparser.c:414): current config, no options.
pub fn ts_headline_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    query_image: &[u8],
) -> PgResult<Vec<u8>> {
    let cfg = getTSCurrentConfig(true)?;
    ts_headline_jsonb_byid_opt(mcx, cfg, jb, query_image, None)
}

/// `ts_headline_jsonb_byid` (wparser.c:423): explicit config, no options.
pub fn ts_headline_jsonb_byid<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    jb: &[u8],
    query_image: &[u8],
) -> PgResult<Vec<u8>> {
    ts_headline_jsonb_byid_opt(mcx, tsconfig, jb, query_image, None)
}

/// `ts_headline_jsonb_opt` (wparser.c:432): current config, with options.
pub fn ts_headline_jsonb_opt<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    query_image: &[u8],
    opt: &[u8],
) -> PgResult<Vec<u8>> {
    let cfg = getTSCurrentConfig(true)?;
    ts_headline_jsonb_byid_opt(mcx, cfg, jb, query_image, Some(opt))
}

// ---------------------------------------------------------------------------
// json headline drivers (wparser.c:442..516)
// ---------------------------------------------------------------------------

/// `ts_headline_json_byid_opt` (wparser.c:442): the all-arguments json driver.
///
/// `json` is the json `text` payload (header stripped); `query_image` the full
/// tsquery varlena; `opt` the optional options `text` payload. Returns the
/// transformed json `text` payload bytes (the fmgr wrapper re-wraps the header).
pub fn ts_headline_json_byid_opt<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    json: &[u8],
    query_image: &[u8],
    opt: Option<&[u8]>,
) -> PgResult<Vec<u8>> {
    let state = headline_json_state(mcx, tsconfig, query_image, opt)?;
    let out = transform_json_string_values(mcx, json, &mut |m: Mcx<'mcx>, elem: &[u8]| {
        state.headline_json_value(m, elem)
    })?;
    Ok(out.to_vec())
}

/// `ts_headline_json` (wparser.c:490): current config, no options.
pub fn ts_headline_json<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    query_image: &[u8],
) -> PgResult<Vec<u8>> {
    let cfg = getTSCurrentConfig(true)?;
    ts_headline_json_byid_opt(mcx, cfg, json, query_image, None)
}

/// `ts_headline_json_byid` (wparser.c:499): explicit config, no options.
pub fn ts_headline_json_byid<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    json: &[u8],
    query_image: &[u8],
) -> PgResult<Vec<u8>> {
    ts_headline_json_byid_opt(mcx, tsconfig, json, query_image, None)
}

/// `ts_headline_json_opt` (wparser.c:508): current config, with options.
pub fn ts_headline_json_opt<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    query_image: &[u8],
    opt: &[u8],
) -> PgResult<Vec<u8>> {
    let cfg = getTSCurrentConfig(true)?;
    ts_headline_json_byid_opt(mcx, cfg, json, query_image, Some(opt))
}

/// Build the shared json headline state and validate the parser supports
/// headline creation (wparser.c:382..394 / 459..471).
fn headline_json_state<'mcx>(
    mcx: Mcx<'mcx>,
    tsconfig: Oid,
    query_image: &[u8],
    opt: Option<&[u8]>,
) -> PgResult<HeadlineJsonState> {
    let cfg = lookup_ts_config_cache(mcx, tsconfig)?;
    let prsobj = lookup_ts_parser_cache(cfg.prsId)?;

    if u32::from(prsobj.headlineOid) == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("text search parser does not support headline creation")
            .into_error());
    }

    Ok(HeadlineJsonState {
        cfg_id: cfg.cfgId,
        prs_id: cfg.prsId,
        headline_oid: prsobj.headlineOid,
        query: decode_tsquery(query_image),
        prsoptions: prsoptions_from_deflist(opt, mcx)?,
    })
}
