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

use std::cmp::Ordering;

use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

use pg_locale_seams as locale;

use crate::keystone::check_collation_set;

/// Map a Rust [`Ordering`] to the C `memcmp`/`strcoll`-style sign.
fn cmp_to_i32(ord: Ordering) -> i32 {
    match ord {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// C-collation core of `varstr_cmp`:
/// `result = memcmp(arg1, arg2, Min(len1, len2));`
/// `if (result == 0 && len1 != len2) result = (len1 < len2) ? -1 : 1;`
fn byte_cmp(arg1: &[u8], arg2: &[u8]) -> i32 {
    let n = arg1.len().min(arg2.len());
    let result = cmp_to_i32(arg1[..n].cmp(&arg2[..n]));
    if result == 0 && arg1.len() != arg2.len() {
        if arg1.len() < arg2.len() {
            -1
        } else {
            1
        }
    } else {
        result
    }
}

/// C: `varstr_cmp(arg1, len1, arg2, len2, collid)` — collation-aware 3-way
/// comparison. C-collation fast path is `memcmp` + length tiebreak; non-C
/// collations delegate to `pg_strncoll` via the locale seam. Carries
/// `check_collation_set` and the locale `ereport(ERROR)` on `Err`.
pub fn varstr_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    // C: check_collation_set(collid); mylocale = pg_newlocale_from_collation(collid);
    check_collation_set(collid)?;

    // C: if (mylocale->collate_is_c) { memcmp + length tiebreak }
    if locale::collation_is_c::call(collid)? {
        return Ok(byte_cmp(arg1, arg2));
    }

    // C: else branch.
    //
    // memcmp() can't tell us which of two unequal strings sorts first, but
    // it's a cheap way to tell if they're equal:
    //   if (len1 == len2 && memcmp(arg1, arg2, len1) == 0) return 0;
    if arg1.len() == arg2.len() && arg1 == arg2 {
        return Ok(0);
    }

    // C: result = pg_strncoll(arg1, len1, arg2, len2, mylocale);
    let mut result = locale::pg_strncoll::call(collid, arg1, arg2)?;

    // C: Break tie if necessary.
    //   if (result == 0 && mylocale->deterministic) {
    //       result = memcmp(arg1, arg2, Min(len1, len2));
    //       if (result == 0 && len1 != len2) result = (len1 < len2) ? -1 : 1;
    //   }
    if result == 0 && locale::collation_is_deterministic::call(collid)? {
        result = byte_cmp(arg1, arg2);
    }

    Ok(result)
}

/// C: `text_cmp(text *arg1, text *arg2, Oid collid)` — `varstr_cmp` over two
/// `text` payloads.
pub fn text_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    // C: a1p = VARDATA_ANY(arg1); len1 = VARSIZE_ANY_EXHDR(arg1); ...
    // The carrier is already the header-less payload.
    varstr_cmp(arg1, arg2, collid)
}

/// C: `texteq(PG_FUNCTION_ARGS)` — `text` equality (deterministic collations
/// short-circuit on length; nondeterministic go through `varstr_cmp`).
pub fn texteq(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;

    if locale::collation_is_deterministic::call(collid)? {
        // C: avoid strcoll(); a length mismatch alone proves inequality
        // (toast_raw_datum_size on the framed datum; here it is the payload
        // length). Then a bitwise memcmp.
        if t1.len() != t2.len() {
            return Ok(false);
        }
        Ok(t1 == t2)
    } else {
        // C: result = (text_cmp(arg1, arg2, collid) == 0);
        Ok(text_cmp(t1, t2, collid)? == 0)
    }
}

/// C: `textne(PG_FUNCTION_ARGS)`.
pub fn textne(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;

    if locale::collation_is_deterministic::call(collid)? {
        // C: see comment in texteq().
        if t1.len() != t2.len() {
            return Ok(true);
        }
        Ok(t1 != t2)
    } else {
        Ok(text_cmp(t1, t2, collid)? != 0)
    }
}

/// C: `text_lt(PG_FUNCTION_ARGS)`.
pub fn text_lt(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? < 0)
}

/// C: `text_le(PG_FUNCTION_ARGS)`.
pub fn text_le(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? <= 0)
}

/// C: `text_gt(PG_FUNCTION_ARGS)`.
pub fn text_gt(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? > 0)
}

/// C: `text_ge(PG_FUNCTION_ARGS)`.
pub fn text_ge(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? >= 0)
}

/// C: `text_starts_with(PG_FUNCTION_ARGS)` — `^@` operator.
pub fn text_starts_with(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;

    // C: if (!mylocale->deterministic) ereport(ERROR, FEATURE_NOT_SUPPORTED).
    if !locale::collation_is_deterministic::call(collid)? {
        return Err(PgError::error(
            "nondeterministic collations are not supported for substring searches",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // C: len1 = toast_raw_datum_size(arg1); len2 = toast_raw_datum_size(arg2);
    // The carrier is the detoasted payload, so len is the payload length.
    let len1 = t1.len();
    let len2 = t2.len();
    if len2 > len1 {
        Ok(false)
    } else {
        // C: targ1 = text_substring(arg1, 1, len2, false);
        //    memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2), VARSIZE_ANY_EXHDR(targ2)) == 0
        // i.e. compare the first len2 bytes of t1 with t2.
        Ok(t1[..len2] == *t2)
    }
}

/// C: `bttextcmp(PG_FUNCTION_ARGS)` — B-tree comparator (`text_cmp`).
pub fn bttextcmp(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    text_cmp(t1, t2, collid)
}

/// C: `btvarstrequalimage(PG_FUNCTION_ARGS)` — equalimage support (true only
/// for deterministic collations).
pub fn btvarstrequalimage(collid: Oid) -> PgResult<bool> {
    // C: check_collation_set(collid); locale = pg_newlocale_from_collation(collid);
    //    PG_RETURN_BOOL(locale->deterministic);
    check_collation_set(collid)?;
    locale::collation_is_deterministic::call(collid)
}

/// C: `text_larger(PG_FUNCTION_ARGS)` — the greater of two `text`s.
///
/// C returns one of the input pointers (`PG_RETURN_TEXT_P`); the carrier owns
/// its bytes, so we charge a fresh copy of the winning payload to `mcx`. Ties
/// keep `arg1` (`> 0 ? arg1 : arg2`).
pub fn text_larger<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let winner = if text_cmp(t1, t2, collid)? > 0 { t1 } else { t2 };
    ::mcx::slice_in(mcx, winner)
}

/// C: `text_smaller(PG_FUNCTION_ARGS)`.
///
/// Ties keep `arg2` (`< 0 ? arg1 : arg2`).
pub fn text_smaller<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let winner = if text_cmp(t1, t2, collid)? < 0 { t1 } else { t2 };
    ::mcx::slice_in(mcx, winner)
}
