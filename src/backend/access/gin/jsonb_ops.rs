use std::collections::BTreeSet;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::jsonb::{JsonbValue, decode_jsonb, render_temporal_jsonb_value};
use crate::include::access::gin::{
    GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GinEntryKey, GinNullCategory,
};
use crate::include::catalog::GIN_JSONB_PATH_FAMILY_OID;
use crate::include::nodes::datum::Value;

const JGINFLAG_KEY: u8 = 0x01;
const JGINFLAG_NULL: u8 = 0x02;
const JGINFLAG_BOOL: u8 = 0x03;
const JGINFLAG_NUM: u8 = 0x04;
const JGINFLAG_STR: u8 = 0x05;
const JGINFLAG_HASHED: u8 = 0x10;
const A_GIN_ELEM: u8 = 0x20;
const JGIN_MAXLENGTH: usize = 125;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GinJsonbQuery {
    All,
    Any(Vec<GinEntryKey>),
    None,
}

pub(crate) fn extract_value(attnum: u16, value: &Value) -> Result<Vec<GinEntryKey>, CatalogError> {
    match value {
        Value::Null => Ok(vec![GinEntryKey {
            attnum,
            category: GinNullCategory::NullItem,
            bytes: Vec::new(),
        }]),
        Value::Jsonb(bytes) => {
            let jsonb = decode_jsonb(bytes)
                .map_err(|err| CatalogError::Io(format!("GIN jsonb decode failed: {err:?}")))?;
            let mut entries = Vec::new();
            extract_jsonb_entries(attnum, &jsonb, &mut entries);
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
        _ => Err(CatalogError::Io("unsupported GIN indexed value".into())),
    }
}

pub(crate) fn extract_query(
    attnum: u16,
    strategy: u16,
    opfamily: Option<u32>,
    argument: &Value,
) -> Result<GinJsonbQuery, CatalogError> {
    if argument.as_array_value().is_some() && matches!(strategy, 1..=4) {
        return extract_array_query(attnum, strategy, argument);
    }
    if matches!(strategy, 15 | 16) {
        if !matches!(argument, Value::JsonPath(_)) {
            return Err(CatalogError::Io(
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
            return Err(CatalogError::Io(
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
                return Err(CatalogError::Io(
                    "GIN @> query expects jsonb argument".into(),
                ));
            };
            let jsonb = decode_jsonb(bytes).map_err(|err| {
                CatalogError::Io(format!("GIN jsonb query decode failed: {err:?}"))
            })?;
            let mut entries = Vec::new();
            extract_jsonb_entries(attnum, &jsonb, &mut entries);
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
                return Err(CatalogError::Io("GIN ? query expects text argument".into()));
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
        _ => Err(CatalogError::Io(format!(
            "unsupported GIN jsonb_ops strategy {strategy}"
        ))),
    }
}

pub(crate) fn query_search_mode(query: &GinJsonbQuery) -> u8 {
    match query {
        GinJsonbQuery::All => GIN_SEARCH_MODE_ALL,
        GinJsonbQuery::Any(_) => GIN_SEARCH_MODE_DEFAULT,
        GinJsonbQuery::None => GIN_SEARCH_MODE_DEFAULT,
    }
}

pub(crate) fn strategy_requires_all(strategy: u16) -> bool {
    matches!(strategy, 2 | 4 | 7 | 11)
}

fn extract_array_query(
    attnum: u16,
    strategy: u16,
    argument: &Value,
) -> Result<GinJsonbQuery, CatalogError> {
    let entries = array_entries(attnum, argument, false)?;
    match strategy {
        // overlap
        1 => {
            if entries.is_empty() {
                Ok(GinJsonbQuery::None)
            } else {
                Ok(GinJsonbQuery::Any(entries))
            }
        }
        // contains
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
        // equals
        4 => {
            if entries.is_empty() {
                Ok(GinJsonbQuery::All)
            } else {
                Ok(GinJsonbQuery::Any(entries))
            }
        }
        _ => Err(CatalogError::Io(format!(
            "unsupported GIN array_ops strategy {strategy}"
        ))),
    }
}

fn array_entries(
    attnum: u16,
    value: &Value,
    include_empty_item: bool,
) -> Result<Vec<GinEntryKey>, CatalogError> {
    let array = value
        .as_array_value()
        .ok_or_else(|| CatalogError::Io("GIN array_ops expects array argument".into()))?;
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

fn extract_jsonb_entries(attnum: u16, value: &JsonbValue, out: &mut Vec<GinEntryKey>) {
    match value {
        JsonbValue::Object(items) => {
            for (key, child) in items {
                out.push(text_key(attnum, key));
                extract_jsonb_entries(attnum, child, out);
            }
        }
        JsonbValue::Array(items) => {
            for child in items {
                if let JsonbValue::String(text) = child {
                    out.push(text_key(attnum, text));
                } else {
                    extract_jsonb_entries(attnum, child, out);
                }
            }
        }
        JsonbValue::Null => out.push(scalar_key(attnum, JGINFLAG_NULL, "")),
        JsonbValue::Bool(value) => out.push(scalar_key(
            attnum,
            JGINFLAG_BOOL,
            if *value { "true" } else { "false" },
        )),
        JsonbValue::Numeric(value) => out.push(scalar_key(
            attnum,
            JGINFLAG_NUM,
            &value.normalize_display_scale().render(),
        )),
        JsonbValue::String(value) => out.push(scalar_key(attnum, JGINFLAG_STR, value)),
        JsonbValue::Date(_)
        | JsonbValue::Time(_)
        | JsonbValue::TimeTz(_)
        | JsonbValue::Timestamp(_)
        | JsonbValue::TimestampTz(_)
        | JsonbValue::TimestampTzWithOffset(_, _) => out.push(scalar_key(
            attnum,
            JGINFLAG_STR,
            &render_temporal_jsonb_value(value),
        )),
    }
}

fn text_key(attnum: u16, text: &str) -> GinEntryKey {
    scalar_key(attnum, JGINFLAG_KEY, text)
}

fn scalar_key(attnum: u16, flag: u8, text: &str) -> GinEntryKey {
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

fn text_array_entries(attnum: u16, value: &Value) -> Result<Vec<GinEntryKey>, CatalogError> {
    let array = value
        .as_array_value()
        .ok_or_else(|| CatalogError::Io("GIN ?|/?& query expects text[] argument".into()))?;
    let mut seen = BTreeSet::new();
    for item in array.elements {
        if matches!(item, Value::Null) {
            continue;
        }
        let Some(text) = item.as_text() else {
            return Err(CatalogError::Io(
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

#[cfg(test)]
mod tests {
    use crate::backend::executor::jsonb::parse_jsonb_text;

    use super::*;

    #[test]
    fn jsonb_ops_extracts_object_keys_and_array_strings_as_keys() {
        let value = Value::Jsonb(parse_jsonb_text(r#"{"a": 1, "b": ["x", 2]}"#).unwrap());
        let entries = extract_value(1, &value).unwrap();
        let key_texts = entries
            .iter()
            .filter_map(|entry| {
                (entry.bytes.first().copied() == Some(JGINFLAG_KEY))
                    .then(|| String::from_utf8(entry.bytes[1..].to_vec()).unwrap())
            })
            .collect::<Vec<_>>();

        assert!(key_texts.contains(&"a".to_string()));
        assert!(key_texts.contains(&"b".to_string()));
        assert!(key_texts.contains(&"x".to_string()));
    }

    #[test]
    fn jsonb_ops_empty_container_emits_empty_item() {
        let value = Value::Jsonb(parse_jsonb_text("{}").unwrap());
        let entries = extract_value(1, &value).unwrap();
        assert_eq!(entries[0].category, GinNullCategory::EmptyItem);
    }
}
