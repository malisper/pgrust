#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

use keywords::{KeywordCategory, ScanKeywordCategories, ScanKeywordLookup, ScanKeywords};
use syscache_seams::PgTypeTypcacheShape;
use types_core::catalog::{
    FirstNormalObjectId, BITOID, BOOLOID, BPCHAROID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, INTERVALOID, JSONOID, NUMERICOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
    VARBITOID, VARCHAROID,
};
use types_core::Oid;
use types_error::{PgError, PgResult};

const TYPSTORAGE_PLAIN: i8 = b'p' as i8;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("{what} unported — unit backend-utils-adt-format-type")
}

#[cold]
#[inline(never)]
fn type_lookup_failed(typid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for type {typid}")))
}

fn lookup(type_oid: Oid) -> PgResult<PgTypeTypcacheShape> {
    syscache_seams::lookup_pg_type_typcache_shape::call(type_oid)?
        .ok_or_else(|| type_lookup_failed(type_oid))
}

/// C `format_type_be` = `format_type_extended(type_oid, -1, 0)`.
pub fn format_type_be(type_oid: Oid) -> PgResult<String> {
    let mut shape = lookup(type_oid)?;
    let mut named_oid = type_oid;
    let mut is_array = false;
    if lsyscache::typ::is_true_array_type(shape.typelem, shape.typsubscript)
        && shape.typstorage != TYPSTORAGE_PLAIN
    {
        named_oid = shape.typelem;
        shape = lookup(named_oid)?;
        is_array = true;
    }

    // Built-in special cases; with_typemod is always false on this path.
    let special: Option<&str> = match named_oid {
        BITOID => Some("bit"),
        BOOLOID => Some("boolean"),
        BPCHAROID => Some("character"),
        FLOAT4OID => Some("real"),
        FLOAT8OID => Some("double precision"),
        INT2OID => Some("smallint"),
        INT4OID => Some("integer"),
        INT8OID => Some("bigint"),
        NUMERICOID => Some("numeric"),
        INTERVALOID => Some("interval"),
        TIMEOID => Some("time without time zone"),
        TIMETZOID => Some("time with time zone"),
        TIMESTAMPOID => Some("timestamp without time zone"),
        TIMESTAMPTZOID => Some("timestamp with time zone"),
        VARBITOID => Some("bit varying"),
        VARCHAROID => Some("character varying"),
        JSONOID => Some("json"),
        _ => None,
    };

    let mut buf = match special {
        Some(name) => name.to_string(),
        None => {
            // C schema-qualifies when !TypeIsVisible; unported, so only
            // builtin (pg_catalog, visible barring shadowing) types render.
            if named_oid >= FirstNormalObjectId {
                unported("format_type_extended (format_type.c): TypeIsVisible/schema qualification for user types");
            }
            let name = core::str::from_utf8(shape.typname.name_str())
                .expect("pg_type.typname is ASCII for builtin types");
            quote_identifier(name).into_owned()
        }
    };
    if is_array {
        buf.push_str("[]");
    }
    Ok(buf)
}

/// C `quote_identifier` (ruleutils.c) minus the quote_all_identifiers GUC.
pub fn quote_identifier(ident: &str) -> std::borrow::Cow<'_, str> {
    let bytes = ident.as_bytes();
    let mut safe = matches!(bytes.first(), Some(b'a'..=b'z' | b'_'));
    if safe {
        safe = bytes
            .iter()
            .all(|&b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'));
    }
    if safe {
        let kwnum = ScanKeywordLookup(bytes, &ScanKeywords);
        if kwnum >= 0 && ScanKeywordCategories[kwnum as usize] != KeywordCategory::Unreserved {
            safe = false;
        }
    }
    if safe {
        return std::borrow::Cow::Borrowed(ident);
    }
    let mut quoted = String::with_capacity(ident.len() + 2);
    quoted.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    std::borrow::Cow::Owned(quoted)
}
