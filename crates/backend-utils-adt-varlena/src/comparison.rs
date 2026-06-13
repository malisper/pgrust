//! FAMILY: comparison + the collation core.
//!
//! `varstr_cmp`/`text_cmp` (the collation-aware 3-way comparator the whole
//! file leans on), the `text` relational operators (`texteq`/`textne`/
//! `text_lt`/`text_le`/`text_gt`/`text_ge`), `text_starts_with`, `bttextcmp`,
//! `btvarstrequalimage`, and `text_larger`/`text_smaller`.
//!
//! Depends on the keystone for [`check_collation_set`](crate::keystone) and
//! the carrier conventions; reaches the locale providers
//! (`pg_newlocale_from_collation`, `pg_strncoll`) through
//! `backend-utils-adt-pg-locale-seams` (collation/ICU owner, genuinely
//! external).

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

/// C: `varstr_cmp(arg1, len1, arg2, len2, collid)` — collation-aware 3-way
/// comparison. C-collation fast path is `memcmp` + length tiebreak; non-C
/// collations delegate to `pg_strncoll` via the locale seam. Carries
/// `check_collation_set` and the locale `ereport(ERROR)` on `Err`.
pub fn varstr_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    todo!("comparison family: port varstr_cmp (collation core, pg_locale seam)")
}

/// C: `text_cmp(text *arg1, text *arg2, Oid collid)` — `varstr_cmp` over two
/// `text` payloads.
pub fn text_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    todo!("comparison family: port text_cmp")
}

/// C: `texteq(PG_FUNCTION_ARGS)` — `text` equality (deterministic collations
/// short-circuit on length; nondeterministic go through `varstr_cmp`).
pub fn texteq(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port texteq")
}

/// C: `textne(PG_FUNCTION_ARGS)`.
pub fn textne(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port textne")
}

/// C: `text_lt(PG_FUNCTION_ARGS)`.
pub fn text_lt(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port text_lt")
}

/// C: `text_le(PG_FUNCTION_ARGS)`.
pub fn text_le(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port text_le")
}

/// C: `text_gt(PG_FUNCTION_ARGS)`.
pub fn text_gt(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port text_gt")
}

/// C: `text_ge(PG_FUNCTION_ARGS)`.
pub fn text_ge(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port text_ge")
}

/// C: `text_starts_with(PG_FUNCTION_ARGS)` — `^@` operator.
pub fn text_starts_with(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port text_starts_with")
}

/// C: `bttextcmp(PG_FUNCTION_ARGS)` — B-tree comparator (`text_cmp`).
pub fn bttextcmp(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    todo!("comparison family: port bttextcmp")
}

/// C: `btvarstrequalimage(PG_FUNCTION_ARGS)` — equalimage support (true only
/// for deterministic collations).
pub fn btvarstrequalimage(collid: Oid) -> PgResult<bool> {
    todo!("comparison family: port btvarstrequalimage")
}

/// C: `text_larger(PG_FUNCTION_ARGS)` — the greater of two `text`s.
pub fn text_larger<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("comparison family: port text_larger")
}

/// C: `text_smaller(PG_FUNCTION_ARGS)`.
pub fn text_smaller<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("comparison family: port text_smaller")
}
