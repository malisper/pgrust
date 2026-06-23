//! `to_tsvector_byid` / `to_tsvector` (to_tsany.c:243..279) plus the
//! `json(b)(_string)_to_tsvector(_byid)` workers (to_tsany.c:285..437).

use alloc::vec::Vec;

use ::parse::{parsetext, ParsedText};
use ::adt_jsonfuncs::iterate::{iterate_json_values, iterate_jsonb_values};
use ::adt_jsonfuncs::setops::parse_jsonb_index_flags;
use ::ts_cache::getTSCurrentConfig;
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;

use crate::make_tsvector::make_tsvector;

/// `jtiString` (jsonfuncs.h): the index-flag bit selecting string values.
const JTI_STRING: u32 = 0x02;

/// `MaxAllocSize` (memutils.h): `0x3FFFFFFF`.
const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;
/// `sizeof(ParsedWord)` — used only for the `lenwords` clamp parity (the value
/// matches C's struct size closely enough for the estimate; it just caps the
/// initial array seed).
const SIZEOF_PARSEDWORD: usize = 16;

/// `to_tsvector_byid(cfgId, txt)` (to_tsany.c:243): parse `in` under config
/// `cfgId` and build the `tsvector` image.
pub fn to_tsvector_byid(cfg_id: Oid, input: &[u8]) -> PgResult<Vec<u8>> {
    // prs.lenwords = VARSIZE_ANY_EXHDR(in) / 6 (estimate), clamped [2, cap].
    let mut lenwords = input.len() / 6;
    if lenwords < 2 {
        lenwords = 2;
    } else if lenwords > MAX_ALLOC_SIZE / SIZEOF_PARSEDWORD {
        lenwords = MAX_ALLOC_SIZE / SIZEOF_PARSEDWORD;
    }

    let mut prs = ParsedText {
        words: Vec::new(),
        lenwords: lenwords as i32,
        curwords: 0,
        pos: 0,
    };

    // parsetext(cfgId, &prs, VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in)).
    parsetext(u32::from(cfg_id), &mut prs, input)?;

    // out = make_tsvector(&prs).
    make_tsvector(&mut prs)
}

/// `to_tsvector(txt)` (to_tsany.c:270): resolve the current config and delegate.
pub fn to_tsvector(input: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    to_tsvector_byid(cfg_id, input)
}

/// `add_to_tsvector` (to_tsany.c:442): parse one json(b) string element under
/// `cfg_id`, appending its lexemes to `prs`, and advance `prs.pos` to create an
/// artificial break between elements when this element produced any words.
///
/// The C callback lazily initializes `prs->words` on first use (to a 16-slot
/// array); here `prs.words` is a `Vec` seeded empty by the worker, so the
/// lazy-init clamp is unnecessary — the only behavior that matters is the
/// post-parse `pos` advance.
fn add_to_tsvector(cfg_id: Oid, prs: &mut ParsedText, elem: &[u8]) -> PgResult<()> {
    // prevwords = prs->curwords;
    let prevwords = prs.curwords;

    // parsetext(state->cfgId, prs, elem_value, elem_len);
    parsetext(u32::from(cfg_id), prs, elem)?;

    // if (prs->curwords > prevwords) prs->pos += 1;
    if prs.curwords > prevwords {
        prs.pos += 1;
    }

    Ok(())
}

/// `jsonb_to_tsvector_worker` (to_tsany.c:284): iterate the json(b) values
/// selected by `flags`, lexize each into a shared [`ParsedText`], then build the
/// `tsvector` image.
///
/// `jb` is the full jsonb varlena image (header included); `iterate_jsonb_values`
/// strips the `VARHDRSZ` header to reach the root container.
pub fn jsonb_to_tsvector_worker<'mcx>(
    mcx: Mcx<'mcx>,
    cfg_id: Oid,
    jb: &[u8],
    flags: u32,
) -> PgResult<Vec<u8>> {
    // prs.words = NULL; prs.curwords = 0; (lenwords/pos default to 0)
    let mut prs = ParsedText {
        words: Vec::new(),
        lenwords: 0,
        curwords: 0,
        pos: 0,
    };

    // iterate_jsonb_values(jb, flags, &state, add_to_tsvector);
    iterate_jsonb_values(mcx, jb, flags, &mut |elem: &[u8]| {
        add_to_tsvector(cfg_id, &mut prs, elem)
    })?;

    // return make_tsvector(&prs);
    make_tsvector(&mut prs)
}

/// `json_to_tsvector_worker` (to_tsany.c:363): same as the jsonb worker but over
/// a json (text) document.
///
/// `json` is the text payload (the `VARDATA_ANY` bytes; the fmgr wrapper strips
/// the `VARHDRSZ` header), matching `iterate_json_values`.
pub fn json_to_tsvector_worker(cfg_id: Oid, json: &[u8], flags: u32) -> PgResult<Vec<u8>> {
    let mut prs = ParsedText {
        words: Vec::new(),
        lenwords: 0,
        curwords: 0,
        pos: 0,
    };

    // iterate_json_values(json, flags, &state, add_to_tsvector);
    iterate_json_values(json, flags, &mut |elem: &[u8]| {
        add_to_tsvector(cfg_id, &mut prs, elem)
    })?;

    make_tsvector(&mut prs)
}

// ---------------------------------------------------------------------------
// jsonb(_string)_to_tsvector(_byid) (to_tsany.c:300..358)
// ---------------------------------------------------------------------------

/// `jsonb_string_to_tsvector_byid` (to_tsany.c:300): iterate only string values.
pub fn jsonb_string_to_tsvector_byid<'mcx>(
    mcx: Mcx<'mcx>,
    cfg_id: Oid,
    jb: &[u8],
) -> PgResult<Vec<u8>> {
    jsonb_to_tsvector_worker(mcx, cfg_id, jb, JTI_STRING)
}

/// `jsonb_string_to_tsvector` (to_tsany.c:313): current config + string values.
pub fn jsonb_string_to_tsvector<'mcx>(mcx: Mcx<'mcx>, jb: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    jsonb_to_tsvector_worker(mcx, cfg_id, jb, JTI_STRING)
}

/// `jsonb_to_tsvector_byid` (to_tsany.c:327): flags come from the jsonb flag
/// array argument `jb_flags`.
pub fn jsonb_to_tsvector_byid<'mcx>(
    mcx: Mcx<'mcx>,
    cfg_id: Oid,
    jb: &[u8],
    jb_flags: &[u8],
) -> PgResult<Vec<u8>> {
    let flags = parse_jsonb_index_flags(jb_flags)?;
    jsonb_to_tsvector_worker(mcx, cfg_id, jb, flags)
}

/// `jsonb_to_tsvector` (to_tsany.c:343): current config + parsed flags.
pub fn jsonb_to_tsvector<'mcx>(mcx: Mcx<'mcx>, jb: &[u8], jb_flags: &[u8]) -> PgResult<Vec<u8>> {
    let flags = parse_jsonb_index_flags(jb_flags)?;
    let cfg_id = getTSCurrentConfig(true)?;
    jsonb_to_tsvector_worker(mcx, cfg_id, jb, flags)
}

// ---------------------------------------------------------------------------
// json(_string)_to_tsvector(_byid) (to_tsany.c:379..437)
// ---------------------------------------------------------------------------

/// `json_string_to_tsvector_byid` (to_tsany.c:379): iterate only string values.
pub fn json_string_to_tsvector_byid(cfg_id: Oid, json: &[u8]) -> PgResult<Vec<u8>> {
    json_to_tsvector_worker(cfg_id, json, JTI_STRING)
}

/// `json_string_to_tsvector` (to_tsany.c:392): current config + string values.
pub fn json_string_to_tsvector(json: &[u8]) -> PgResult<Vec<u8>> {
    let cfg_id = getTSCurrentConfig(true)?;
    json_to_tsvector_worker(cfg_id, json, JTI_STRING)
}

/// `json_to_tsvector_byid` (to_tsany.c:406): flags from the jsonb flag array.
pub fn json_to_tsvector_byid(cfg_id: Oid, json: &[u8], jb_flags: &[u8]) -> PgResult<Vec<u8>> {
    let flags = parse_jsonb_index_flags(jb_flags)?;
    json_to_tsvector_worker(cfg_id, json, flags)
}

/// `json_to_tsvector` (to_tsany.c:422): current config + parsed flags.
pub fn json_to_tsvector(json: &[u8], jb_flags: &[u8]) -> PgResult<Vec<u8>> {
    let flags = parse_jsonb_index_flags(jb_flags)?;
    let cfg_id = getTSCurrentConfig(true)?;
    json_to_tsvector_worker(cfg_id, json, flags)
}
