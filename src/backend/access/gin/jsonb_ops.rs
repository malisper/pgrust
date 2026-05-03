// :HACK: root compatibility shim while GIN JSONB support lives in `pgrust_access`.
use pgrust_access::gin::jsonb_ops as access_jsonb_ops;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::access::gin::GinEntryKey;
use crate::include::nodes::datum::Value;

pub(crate) use access_jsonb_ops::GinJsonbQuery;

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Interrupted(reason) => CatalogError::Interrupted(reason),
        AccessError::Io(message) => CatalogError::Io(message),
        AccessError::UniqueViolation(message) => CatalogError::UniqueViolation(message),
        AccessError::Scalar(message) | AccessError::Unsupported(message) => {
            CatalogError::Io(message)
        }
    }
}

fn catalog_result<T>(result: AccessResult<T>) -> Result<T, CatalogError> {
    result.map_err(catalog_error)
}

pub(crate) fn extract_value(attnum: u16, value: &Value) -> Result<Vec<GinEntryKey>, CatalogError> {
    catalog_result(access_jsonb_ops::extract_value(
        attnum,
        value,
        &RootAccessServices,
    ))
}

pub(crate) fn extract_query(
    attnum: u16,
    strategy: u16,
    opfamily: Option<u32>,
    argument: &Value,
) -> Result<GinJsonbQuery, CatalogError> {
    catalog_result(access_jsonb_ops::extract_query(
        attnum,
        strategy,
        opfamily,
        argument,
        &RootAccessServices,
    ))
}

pub(crate) fn query_search_mode(query: &GinJsonbQuery) -> u8 {
    access_jsonb_ops::query_search_mode(query)
}

pub(crate) fn strategy_requires_all(strategy: u16) -> bool {
    access_jsonb_ops::strategy_requires_all(strategy)
}

#[cfg(test)]
mod tests {
    use crate::backend::executor::jsonb::parse_jsonb_text;
    use crate::include::access::gin::GinNullCategory;

    use super::*;
    use pgrust_access::gin::jsonb_ops::JGINFLAG_KEY;

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
