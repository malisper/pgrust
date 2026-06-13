//! FAMILY: name<->text comparison + text_pattern_ops.
//!
//! The `name`<->`text` comparison operators
//! (`nameeqtext`/`texteqname`/`namenetext`/`textnename`,
//! `btnametextcmp`/`bttextnamecmp`, the 8 ordering operators
//! `namelttext`..`textgename`) and the collation-independent
//! `text_pattern_ops` family (`internal_text_pattern_compare`,
//! `text_pattern_lt/le/ge/gt`, `bttext_pattern_cmp`).
//!
//! Depends on the keystone for the `name` carrier ([`NAMEDATALEN`]) and on the
//! comparison family for `text_cmp`.

#![allow(unused_variables)]

use types_core::Oid;
use types_error::PgResult;

/// Logical `NameStr` bytes: the `name` buffer up to the first NUL.
pub fn name_str(name: &[u8]) -> &[u8] {
    todo!("name_pattern family: port NameStr slice helper")
}

/// C: `nameeqtext(PG_FUNCTION_ARGS)`.
pub fn nameeqtext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("name_pattern family: port nameeqtext")
}

/// C: `texteqname(PG_FUNCTION_ARGS)`.
pub fn texteqname(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("name_pattern family: port texteqname")
}

/// C: `namenetext` / `textnename` and the 8 ordering ops
/// (`namelttext`/`nameletext`/`namegttext`/`namegetext`/
/// `textltname`/`textlename`/`textgtname`/`textgename`) follow the same
/// `text_cmp`-over-NameStr shape; filled here.
pub fn name_text_compare(name: &[u8], t: &[u8], collid: Oid) -> PgResult<i32> {
    todo!("name_pattern family: port btnametextcmp/bttextnamecmp core")
}

/// C: `internal_text_pattern_compare(text *arg1, text *arg2)` — raw `memcmp` +
/// length tiebreak (collation-independent).
pub fn internal_text_pattern_compare(a: &[u8], b: &[u8]) -> PgResult<i32> {
    todo!("name_pattern family: port internal_text_pattern_compare")
}

/// C: `text_pattern_lt(PG_FUNCTION_ARGS)`.
pub fn text_pattern_lt(a: &[u8], b: &[u8]) -> PgResult<bool> {
    todo!("name_pattern family: port text_pattern_lt")
}

/// C: `text_pattern_le(PG_FUNCTION_ARGS)`.
pub fn text_pattern_le(a: &[u8], b: &[u8]) -> PgResult<bool> {
    todo!("name_pattern family: port text_pattern_le")
}

/// C: `text_pattern_ge(PG_FUNCTION_ARGS)`.
pub fn text_pattern_ge(a: &[u8], b: &[u8]) -> PgResult<bool> {
    todo!("name_pattern family: port text_pattern_ge")
}

/// C: `text_pattern_gt(PG_FUNCTION_ARGS)`.
pub fn text_pattern_gt(a: &[u8], b: &[u8]) -> PgResult<bool> {
    todo!("name_pattern family: port text_pattern_gt")
}

/// C: `bttext_pattern_cmp(PG_FUNCTION_ARGS)`.
pub fn bttext_pattern_cmp(a: &[u8], b: &[u8]) -> PgResult<i32> {
    todo!("name_pattern family: port bttext_pattern_cmp")
}
