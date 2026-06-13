//! FAMILY: substring / position / overlay / left / right / reverse, and the
//! literal `replace_text`.
//!
//! `text_substr`/`text_substr_no_len`/`text_substring`, `bytea_substr*`/
//! `bytea_substring`, the `text_position_*` Boyer-Moore-Horspool / char-aware
//! searcher + `textpos`, `text_overlay`/`textoverlay*`,
//! `text_left`/`text_right`/`text_reverse`, `pg_mbcharcliplen_chars`, and the
//! literal `replace_text`.
//!
//! Depends on the keystone for [`TextPositionState`](crate::keystone),
//! `charlen_to_bytelen`, `check_collation_set`. Reaches the mbutils seams for
//! char/byte clipping and the locale providers for nondeterministic-collation
//! matching.

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

/// C: `text_substring(Datum str, int32 start, int32 length, bool
/// length_not_specified)` — the SQL `substring` worker on character
/// positions. The owner seam `text_substr` routes here.
pub fn text_substring<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    start: i32,
    length: i32,
    length_not_specified: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port text_substring")
}

/// C: `bytea_substring(Datum str, int S, int L, bool length_not_specified)`.
pub fn bytea_substring<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    s: i32,
    l: i32,
    length_not_specified: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port bytea_substring")
}

/// C: `text_position(text *t1, text *t2, Oid collid)` — 1-based char position
/// of needle `t2` in haystack `t1`, 0 if absent. Wraps the
/// `text_position_setup`/`_next`/`_get_match_pos` state machine.
pub fn text_position(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    todo!("position_ops family: port text_position + TextPositionState machine")
}

/// C: `textpos(PG_FUNCTION_ARGS)`.
pub fn textpos(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    todo!("position_ops family: port textpos")
}

/// C: `text_overlay(text *t1, text *t2, int sp, int sl)`.
pub fn text_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port text_overlay")
}

/// C: `text_left(PG_FUNCTION_ARGS)`.
pub fn text_left<'mcx>(mcx: Mcx<'mcx>, t: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port text_left")
}

/// C: `text_right(PG_FUNCTION_ARGS)`.
pub fn text_right<'mcx>(mcx: Mcx<'mcx>, t: &[u8], n: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port text_right")
}

/// C: `text_reverse(PG_FUNCTION_ARGS)`.
pub fn text_reverse<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port text_reverse")
}

/// C: `replace_text(PG_FUNCTION_ARGS)` — literal (non-regex) replace-all of
/// `from` with `to` in `src`.
pub fn replace_text<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    from: &[u8],
    to: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("position_ops family: port replace_text")
}
