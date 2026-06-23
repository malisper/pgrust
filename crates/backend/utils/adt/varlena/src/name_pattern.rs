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
//! comparison family for `varstr_cmp`.

use types_core::catalog::C_COLLATION_OID;
use types_core::Oid;
use types_error::PgResult;

use crate::comparison::varstr_cmp;
use crate::keystone::check_collation_set;

/// Logical `NameStr` bytes: the `name` buffer up to the first NUL.
///
/// C: `NameStr(*name)` is `name->data`, a NUL-terminated C string inside the
/// fixed [`NAMEDATALEN`](crate::keystone::NAMEDATALEN)-byte buffer; its logical
/// length is `strlen(NameStr(*name))`. This returns the slice of `name` up to
/// (but not including) the first NUL byte.
pub fn name_str(name: &[u8]) -> &[u8] {
    let len = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    &name[..len]
}

/// C: `nameeqtext(PG_FUNCTION_ARGS)`.
pub fn nameeqtext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    let arg1 = name_str(name);
    let len1 = arg1.len();
    let len2 = t.len();

    check_collation_set(collid)?;

    let result = if collid == C_COLLATION_OID {
        len1 == len2 && arg1 == t
    } else {
        varstr_cmp(arg1, t, collid)? == 0
    };

    Ok(result)
}

/// C: `texteqname(PG_FUNCTION_ARGS)`.
pub fn texteqname(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    let arg2 = name_str(name);
    let len1 = t.len();
    let len2 = arg2.len();

    check_collation_set(collid)?;

    let result = if collid == C_COLLATION_OID {
        len1 == len2 && t == arg2
    } else {
        varstr_cmp(t, arg2, collid)? == 0
    };

    Ok(result)
}

/// C: `namenetext(PG_FUNCTION_ARGS)`.
pub fn namenetext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    let arg1 = name_str(name);
    let len1 = arg1.len();
    let len2 = t.len();

    check_collation_set(collid)?;

    let result = if collid == C_COLLATION_OID {
        !(len1 == len2 && arg1 == t)
    } else {
        !(varstr_cmp(arg1, t, collid)? == 0)
    };

    Ok(result)
}

/// C: `textnename(PG_FUNCTION_ARGS)`.
pub fn textnename(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    let arg2 = name_str(name);
    let len1 = t.len();
    let len2 = arg2.len();

    check_collation_set(collid)?;

    let result = if collid == C_COLLATION_OID {
        !(len1 == len2 && t == arg2)
    } else {
        !(varstr_cmp(t, arg2, collid)? == 0)
    };

    Ok(result)
}

/// C: `btnametextcmp(PG_FUNCTION_ARGS)` — 3-way compare of a `name` (arg1)
/// against a `text` (arg2).
pub fn btnametextcmp(name: &[u8], t: &[u8], collid: Oid) -> PgResult<i32> {
    varstr_cmp(name_str(name), t, collid)
}

/// C: `bttextnamecmp(PG_FUNCTION_ARGS)` — 3-way compare of a `text` (arg1)
/// against a `name` (arg2).
pub fn bttextnamecmp(t: &[u8], name: &[u8], collid: Oid) -> PgResult<i32> {
    varstr_cmp(t, name_str(name), collid)
}

// ---------------------------------------------------------------------------
// The 8 ordering operators. In C these expand the `CmpCall(cmpfunc)` macro,
// which dispatches `btnametextcmp` (name-first) or `bttextnamecmp` (text-first)
// via `DirectFunctionCall2Coll` and compares the result against 0.
// ---------------------------------------------------------------------------

/// C: `namelttext(PG_FUNCTION_ARGS)` — `CmpCall(btnametextcmp) < 0`.
pub fn namelttext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(name, t, collid)? < 0)
}

/// C: `nameletext(PG_FUNCTION_ARGS)` — `CmpCall(btnametextcmp) <= 0`.
pub fn nameletext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(name, t, collid)? <= 0)
}

/// C: `namegttext(PG_FUNCTION_ARGS)` — `CmpCall(btnametextcmp) > 0`.
pub fn namegttext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(name, t, collid)? > 0)
}

/// C: `namegetext(PG_FUNCTION_ARGS)` — `CmpCall(btnametextcmp) >= 0`.
pub fn namegetext(name: &[u8], t: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(name, t, collid)? >= 0)
}

/// C: `textltname(PG_FUNCTION_ARGS)` — `CmpCall(bttextnamecmp) < 0`.
pub fn textltname(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(t, name, collid)? < 0)
}

/// C: `textlename(PG_FUNCTION_ARGS)` — `CmpCall(bttextnamecmp) <= 0`.
pub fn textlename(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(t, name, collid)? <= 0)
}

/// C: `textgtname(PG_FUNCTION_ARGS)` — `CmpCall(bttextnamecmp) > 0`.
pub fn textgtname(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(t, name, collid)? > 0)
}

/// C: `textgename(PG_FUNCTION_ARGS)` — `CmpCall(bttextnamecmp) >= 0`.
pub fn textgename(t: &[u8], name: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(t, name, collid)? >= 0)
}

// ---------------------------------------------------------------------------
// text_pattern_ops: collation-independent (raw byte) comparison, for indexes
// that support LIKE clauses.
// ---------------------------------------------------------------------------

/// C: `internal_text_pattern_compare(text *arg1, text *arg2)` — raw `memcmp`
/// over `Min(len1, len2)` bytes, with a length tiebreak (collation-independent).
pub fn internal_text_pattern_compare(a: &[u8], b: &[u8]) -> PgResult<i32> {
    let len1 = a.len();
    let len2 = b.len();
    let n = len1.min(len2);

    // memcmp(a, b, n)
    let result = match a[..n].cmp(&b[..n]) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Equal => 0,
    };

    if result != 0 {
        Ok(result)
    } else if len1 < len2 {
        Ok(-1)
    } else if len1 > len2 {
        Ok(1)
    } else {
        Ok(0)
    }
}

/// C: `text_pattern_lt(PG_FUNCTION_ARGS)`.
pub fn text_pattern_lt(a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(internal_text_pattern_compare(a, b)? < 0)
}

/// C: `text_pattern_le(PG_FUNCTION_ARGS)`.
pub fn text_pattern_le(a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(internal_text_pattern_compare(a, b)? <= 0)
}

/// C: `text_pattern_ge(PG_FUNCTION_ARGS)`.
pub fn text_pattern_ge(a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(internal_text_pattern_compare(a, b)? >= 0)
}

/// C: `text_pattern_gt(PG_FUNCTION_ARGS)`.
pub fn text_pattern_gt(a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(internal_text_pattern_compare(a, b)? > 0)
}

/// C: `bttext_pattern_cmp(PG_FUNCTION_ARGS)`.
pub fn bttext_pattern_cmp(a: &[u8], b: &[u8]) -> PgResult<i32> {
    internal_text_pattern_compare(a, b)
}
