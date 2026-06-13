//! FAMILY: regexp-driven replacement (the `replace_text_regexp` owner seam
//! body) plus its `\N`/`\&` substitution helpers.
//!
//! `replace_text_regexp`, `check_replace_text_has_escape`,
//! `appendStringInfoRegexpSubstr`.
//!
//! The regex engine itself (compile/execute) is the genuinely-external
//! `backend-regex-core` owner, reached by `backend-utils-adt-regexp-seams`
//! (`regexp.c` is the immediate caller; this family is the body of the
//! `replace_text_regexp` owner seam declared in
//! `backend-utils-adt-varlena-seams`).

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

/// C: `replace_text_regexp(src_text, pattern_text, replace_text, cflags,
/// collation, search_start, n)` — owner seam body. `n = 0` replaces all,
/// `n > 0` only the n'th; `search_start` is a char offset.
#[allow(clippy::too_many_arguments)]
pub fn replace_text_regexp<'mcx>(
    mcx: Mcx<'mcx>,
    src_text: &[u8],
    pattern_text: &[u8],
    replace_text: &[u8],
    cflags: i32,
    collation: Oid,
    search_start: i32,
    n: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("replace_regexp family: port replace_text_regexp (regex owner seam)")
}

/// C: `check_replace_text_has_escape(const text *replace_text)` — does the
/// replacement contain any `\` escape (so we must scan vs. plain append)?
pub fn check_replace_text_has_escape(replace_text: &[u8]) -> i32 {
    todo!("replace_regexp family: port check_replace_text_has_escape")
}
