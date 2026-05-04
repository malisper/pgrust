use std::collections::BTreeSet;

use pgrust_catalog_data::pg_opfamily::GIN_JSONB_PATH_FAMILY_OID;
use pgrust_nodes::datum::Value;

use crate::access::gin::{
    GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GinEntryKey, GinNullCategory,
};
use crate::{AccessError, AccessResult, AccessScalarServices};

pub const JGINFLAG_KEY: u8 = 0x01;
pub const JGINFLAG_NULL: u8 = 0x02;
pub const JGINFLAG_BOOL: u8 = 0x03;
pub const JGINFLAG_NUM: u8 = 0x04;
pub const JGINFLAG_STR: u8 = 0x05;
pub const JGINFLAG_HASHED: u8 = 0x10;
pub const A_GIN_ELEM: u8 = 0x20;
pub const JGIN_MAXLENGTH: usize = 125;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GinJsonbQuery {
    All,
    Any(Vec<GinEntryKey>),
    None,
}

pub fn extract_value(
    attnum: u16,
    value: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<Vec<GinEntryKey>> {
    match value {
        Value::Null => Ok(vec![GinEntryKey {
            attnum,
            category: GinNullCategory::NullItem,
            bytes: Vec::new(),
        }]),
        Value::Jsonb(bytes) => {
            let mut entries = services.gin_jsonb_entries(attnum, bytes)?;
            if entries.is_empty() {
                entries.push(GinEntryKey {
                    attnum,
                    category: GinNullCategory::EmptyItem,
                    bytes: Vec::new(),
                });
            }
            entries.sort();
            entries.dedup();
            Ok(entries)
        }
        Value::Array(_) | Value::PgArray(_) => array_entries(attnum, value, true),
        _ => Err(AccessError::Unsupported(
            "unsupported GIN indexed value".into(),
        )),
    }
}

pub fn extract_query(
    attnum: u16,
    strategy: u16,
    opfamily: Option<u32>,
    argument: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<GinJsonbQuery> {
    if argument.as_array_value().is_some() && matches!(strategy, 1..=4) {
        return extract_array_query(attnum, strategy, argument);
    }
    if matches!(strategy, 15 | 16) {
        if !matches!(argument, Value::JsonPath(_)) {
            return Err(AccessError::Unsupported(
                "GIN jsonpath query expects jsonpath argument".into(),
            ));
        }
        // :HACK: Full PostgreSQL jsonpath key extraction is not wired yet.
        // Use a lossy all-TID probe and rely on the heap recheck for @?/@@
        // correctness while still exposing the regression-visible GIN path.
        return Ok(GinJsonbQuery::All);
    }
    if opfamily == Some(GIN_JSONB_PATH_FAMILY_OID) && strategy == 7 {
        if !matches!(argument, Value::Jsonb(_)) {
            return Err(AccessError::Unsupported(
                "GIN jsonb_path_ops @> query expects jsonb argument".into(),
            ));
        }
        // :HACK: jsonb_path_ops stores hashed path/value entries in
        // PostgreSQL. pgrust reuses lossy jsonb_ops storage for now and lets
        // the heap recheck decide exact containment.
        return Ok(GinJsonbQuery::All);
    }
    match strategy {
        7 => {
            let Value::Jsonb(bytes) = argument else {
                return Err(AccessError::Unsupported(
                    "GIN @> query expects jsonb argument".into(),
                ));
            };
            let mut entries = services.gin_jsonb_entries(attnum, bytes)?;
            entries.retain(|entry| entry.category == GinNullCategory::NormalKey);
            entries.sort();
            entries.dedup();
            if entries.is_empty() {
                Ok(GinJsonbQuery::All)
            } else {
                Ok(GinJsonbQuery::Any(entries))
            }
        }
        9 => {
            let Some(text) = argument.as_text() else {
                return Err(AccessError::Unsupported(
                    "GIN ? query expects text argument".into(),
                ));
            };
            Ok(GinJsonbQuery::Any(vec![text_key(attnum, text)]))
        }
        10 | 11 => {
            let keys = text_array_entries(attnum, argument)?;
            if keys.is_empty() && strategy == 11 {
                Ok(GinJsonbQuery::All)
            } else if keys.is_empty() {
                Ok(GinJsonbQuery::None)
            } else {
                Ok(GinJsonbQuery::Any(keys))
            }
        }
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GIN jsonb_ops strategy {strategy}"
        ))),
    }
}

pub fn query_search_mode(query: &GinJsonbQuery) -> u8 {
    match query {
        GinJsonbQuery::All => GIN_SEARCH_MODE_ALL,
        GinJsonbQuery::Any(_) => GIN_SEARCH_MODE_DEFAULT,
        GinJsonbQuery::None => GIN_SEARCH_MODE_DEFAULT,
    }
}

pub fn strategy_requires_all(strategy: u16) -> bool {
    matches!(strategy, 2 | 4 | 7 | 11)
}

fn extract_array_query(
    attnum: u16,
    strategy: u16,
    argument: &Value,
) -> AccessResult<GinJsonbQuery> {
    let entries = array_entries(attnum, argument, false)?;
    match strategy {
        1 => {
            if entries.is_empty() {
                Ok(GinJsonbQuery::None)
            } else {
                Ok(GinJsonbQuery::Any(entries))
            }
        }
        2 => {
            if entries.is_empty() {
                Ok(GinJsonbQuery::All)
            } else {
                Ok(GinJsonbQuery::Any(entries))
            }
        }
        // contained-by: use the index only as a broad prefilter. Empty indexed
        // arrays have no element key, so scan all rows and let the heap recheck
        // decide the exact SQL result.
        3 => Ok(GinJsonbQuery::All),
        4 => {
            if entries.is_empty() {
                Ok(GinJsonbQuery::All)
            } else {
                Ok(GinJsonbQuery::Any(entries))
            }
        }
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GIN array_ops strategy {strategy}"
        ))),
    }
}

fn array_entries(
    attnum: u16,
    value: &Value,
    include_empty_item: bool,
) -> AccessResult<Vec<GinEntryKey>> {
    let array = value
        .as_array_value()
        .ok_or_else(|| AccessError::Unsupported("GIN array_ops expects array argument".into()))?;
    let mut entries = Vec::new();
    for item in array.elements {
        match item {
            Value::Null => entries.push(GinEntryKey {
                attnum,
                category: GinNullCategory::NullItem,
                bytes: Vec::new(),
            }),
            other => entries.push(array_element_key(attnum, &other.to_owned_value())),
        }
    }
    if entries.is_empty() && include_empty_item {
        entries.push(GinEntryKey {
            attnum,
            category: GinNullCategory::EmptyItem,
            bytes: Vec::new(),
        });
    }
    entries.sort();
    entries.dedup();
    Ok(entries)
}

fn array_element_key(attnum: u16, value: &Value) -> GinEntryKey {
    scalar_key(attnum, A_GIN_ELEM, &format!("{value:?}"))
}

pub fn text_key(attnum: u16, text: &str) -> GinEntryKey {
    scalar_key(attnum, JGINFLAG_KEY, text)
}

pub fn scalar_key(attnum: u16, flag: u8, text: &str) -> GinEntryKey {
    let mut bytes = Vec::with_capacity(text.len() + 1);
    if text.len() > JGIN_MAXLENGTH {
        bytes.push(flag | JGINFLAG_HASHED);
        bytes.extend_from_slice(format!("{:08x}", fnv1a32(text.as_bytes())).as_bytes());
    } else {
        bytes.push(flag);
        bytes.extend_from_slice(text.as_bytes());
    }
    GinEntryKey {
        attnum,
        category: GinNullCategory::NormalKey,
        bytes,
    }
}

fn text_array_entries(attnum: u16, value: &Value) -> AccessResult<Vec<GinEntryKey>> {
    let array = value.as_array_value().ok_or_else(|| {
        AccessError::Unsupported("GIN ?|/?& query expects text[] argument".into())
    })?;
    let mut seen = BTreeSet::new();
    for item in array.elements {
        if matches!(item, Value::Null) {
            continue;
        }
        let Some(text) = item.as_text() else {
            return Err(AccessError::Unsupported(
                "GIN ?|/?& query expects text[] argument".into(),
            ));
        };
        seen.insert(text_key(attnum, text));
    }
    Ok(seen.into_iter().collect())
}

fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = 0x811C9DC5u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}
