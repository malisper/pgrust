//! The `to_tsquery` family (to_tsany.c:491..727): `pushval_morph` and the
//! `to_tsquery` / `plainto_tsquery` / `phraseto_tsquery` / `websearch_to_tsquery`
//! SQL entry points (and their `_byid` variants).

use alloc::vec::Vec;

use backend_tsearch_parse::{parsetext, ParsedText};
use backend_utils_adt_tsquery_core::tsquery::{parse_tsquery_with_pushval, QueryBuilder};
use backend_utils_cache_ts_cache::getTSCurrentConfig;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_tsearch::tsearch::{OP_AND, OP_OR, OP_PHRASE, P_TSQ_PLAIN, P_TSQ_WEB};
use types_tsearch::TSL_PREFIX;

/// `MorphOpaque` (to_tsany.c:25): the data `pushval_morph` carries.
struct MorphOpaque {
    cfg_id: Oid,
    /// The operator used to connect adjacent words of a complex morph
    /// (`OP_PHRASE` or `OP_AND`).
    qoperator: i8,
}

/// `pushval_morph(opaque, state, strval, lenval, weight, prefix)`
/// (to_tsany.c:491): lexize `strval` through the config's dictionaries and push
/// the resulting words onto the parser stack. Words of one variant are ANDed;
/// variants are ORed; adjacent morph positions are connected by `qoperator`,
/// with `pushStop` placeholders for removed stop words.
fn pushval_morph(
    data: &MorphOpaque,
    builder: &mut QueryBuilder<'_, '_, '_>,
    strval: &[u8],
    _lenval: usize,
    weight: i16,
    prefix: bool,
) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("pushval_morph");
    let _mcx: Mcx<'_> = ctx.mcx();

    let mut prs = ParsedText {
        words: Vec::new(),
        lenwords: 4,
        curwords: 0,
        pos: 0,
    };

    parsetext(u32::from(data.cfg_id), &mut prs, strval)?;

    let words = &prs.words;
    let curwords = words.len();

    if curwords == 0 {
        builder.push_stop()?;
        return Ok(());
    }

    let mut count: usize = 0;
    let mut pos: u32 = 0;
    let mut cntpos: u32 = 0;

    while count < curwords {
        // Were any stop words removed? Fill the gap with placeholders linked by
        // the appropriate operator (C:518-529).
        if pos > 0 && pos + 1 < words[count].pos as u32 {
            while pos + 1 < words[count].pos as u32 {
                builder.push_stop()?;
                if cntpos != 0 {
                    builder.push_operator(data.qoperator, 1)?;
                }
                cntpos += 1;
                pos += 1;
            }
        }

        // Save current word's position.
        pos = words[count].pos as u32;

        // Go through all variants obtained from this token (C:535-561).
        let mut cntvar: u32 = 0;
        while count < curwords && pos == words[count].pos as u32 {
            let variant = words[count].nvariant;

            // Push all words belonging to the same variant (C:541-556).
            let mut cnt: u32 = 0;
            while count < curwords
                && pos == words[count].pos as u32
                && variant == words[count].nvariant
            {
                let w = &words[count];
                let word_prefix = (w.flags & TSL_PREFIX) != 0 || prefix;
                builder.push_value(&w.word, w.len as usize, weight, word_prefix)?;
                if cnt != 0 {
                    builder.push_operator(OP_AND, 0)?;
                }
                cnt += 1;
                count += 1;
            }

            if cntvar != 0 {
                builder.push_operator(OP_OR, 0)?;
            }
            cntvar += 1;
        }

        if cntpos != 0 {
            // distance may be useful
            builder.push_operator(data.qoperator, 1)?;
        }

        cntpos += 1;
    }

    Ok(())
}

/// Build a `tsquery` image from `input` using `pushval_morph` with the given
/// config, parser flags, and morph-connect operator.
fn parse_morph(
    mcx: Mcx<'_>,
    cfg_id: Oid,
    input: &[u8],
    flags: i32,
    qoperator: i8,
) -> PgResult<Vec<u8>> {
    let data = MorphOpaque { cfg_id, qoperator };
    let mut pushval =
        |b: &mut QueryBuilder<'_, '_, '_>, sv: &[u8], lv: usize, w: i16, p: bool| -> PgResult<()> {
            pushval_morph(&data, b, sv, lv, w, p)
        };
    // parse_tsquery returns None only under a soft-error context; here escontext
    // is None (hard errors), so a successful parse always yields Some.
    let res = parse_tsquery_with_pushval(mcx, input, flags, None, &mut pushval)?;
    Ok(res.expect("parse_tsquery_with_pushval returned None without a soft-error context"))
}

/// `to_tsquery_byid(cfgId, in)` (to_tsany.c:578): OP_PHRASE connect, standard
/// tokenizer.
pub fn to_tsquery_byid(mcx: Mcx<'_>, cfg_id: Oid, input: &[u8]) -> PgResult<Vec<u8>> {
    parse_morph(mcx, cfg_id, input, 0, OP_PHRASE)
}

/// `to_tsquery(in)` (to_tsany.c:604).
pub fn to_tsquery(mcx: Mcx<'_>, input: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    to_tsquery_byid(mcx, cfg_id, input)
}

/// `plainto_tsquery_byid(cfgId, in)` (to_tsany.c:617): plain tokenizer,
/// OP_AND connect.
pub fn plainto_tsquery_byid(mcx: Mcx<'_>, cfg_id: Oid, input: &[u8]) -> PgResult<Vec<u8>> {
    parse_morph(mcx, cfg_id, input, P_TSQ_PLAIN, OP_AND)
}

/// `plainto_tsquery(in)` (to_tsany.c:642).
pub fn plainto_tsquery(mcx: Mcx<'_>, input: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    plainto_tsquery_byid(mcx, cfg_id, input)
}

/// `phraseto_tsquery_byid(cfgId, in)` (to_tsany.c:655): plain tokenizer,
/// OP_PHRASE connect.
pub fn phraseto_tsquery_byid(mcx: Mcx<'_>, cfg_id: Oid, input: &[u8]) -> PgResult<Vec<u8>> {
    parse_morph(mcx, cfg_id, input, P_TSQ_PLAIN, OP_PHRASE)
}

/// `phraseto_tsquery(in)` (to_tsany.c:680).
pub fn phraseto_tsquery(mcx: Mcx<'_>, input: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    phraseto_tsquery_byid(mcx, cfg_id, input)
}

/// `websearch_to_tsquery_byid(cfgId, in)` (to_tsany.c:692): web tokenizer,
/// OP_PHRASE connect.
pub fn websearch_to_tsquery_byid(mcx: Mcx<'_>, cfg_id: Oid, input: &[u8]) -> PgResult<Vec<u8>> {
    parse_morph(mcx, cfg_id, input, P_TSQ_WEB, OP_PHRASE)
}

/// `websearch_to_tsquery(in)` (to_tsany.c:718).
pub fn websearch_to_tsquery(mcx: Mcx<'_>, input: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    websearch_to_tsquery_byid(mcx, cfg_id, input)
}
