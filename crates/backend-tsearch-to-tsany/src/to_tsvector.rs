//! `to_tsvector_byid` / `to_tsvector` (to_tsany.c:243..279).

use alloc::vec::Vec;

use backend_tsearch_parse::{parsetext, ParsedText};
use backend_utils_cache_ts_cache::getTSCurrentConfig;
use types_core::Oid;
use types_error::PgResult;

use crate::make_tsvector::make_tsvector;

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
