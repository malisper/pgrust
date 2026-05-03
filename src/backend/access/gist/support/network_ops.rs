// :HACK: root compatibility shim while GiST network support lives in `pgrust_access`.
use pgrust_access::gist::support::network_ops as access_network_ops;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::Value;

use super::{GistColumnPickSplit, GistConsistentResult};

#[cfg(test)]
use access_network_ops::{
    RT_LESS_STRATEGY, RT_OVERLAP_STRATEGY, RT_SUB_EQUAL_STRATEGY, RT_SUB_STRATEGY,
    RT_SUPER_EQUAL_STRATEGY, RT_SUPER_STRATEGY,
};

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

pub(crate) fn consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    catalog_result(access_network_ops::consistent(
        strategy,
        key,
        query,
        is_leaf,
        &RootAccessServices,
    ))
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    catalog_result(access_network_ops::union(values, &RootAccessServices))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    catalog_result(access_network_ops::penalty(
        original,
        candidate,
        &RootAccessServices,
    ))
}

pub(crate) fn picksplit(values: &[Value]) -> Result<GistColumnPickSplit, CatalogError> {
    catalog_result(access_network_ops::picksplit(values, &RootAccessServices))
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    catalog_result(access_network_ops::same(left, right, &RootAccessServices))
}

#[cfg(test)]
mod tests {
    use crate::backend::executor::{parse_cidr_text, parse_inet_text};
    use crate::include::nodes::datum::Value;

    use super::*;

    fn inet(text: &str) -> Value {
        Value::Inet(parse_inet_text(text).unwrap())
    }

    fn cidr(text: &str) -> Value {
        Value::Cidr(parse_cidr_text(text).unwrap())
    }

    fn leaf_match(strategy: u16, key: Value, query: Value) -> bool {
        consistent(strategy, &key, &query, true).unwrap().matches
    }

    #[test]
    fn network_gist_uses_catalog_strategy_numbers() {
        let query = cidr("192.168.1.0/24");

        assert!(leaf_match(
            RT_SUB_STRATEGY,
            inet("192.168.1.0/25"),
            query.clone()
        ));
        assert!(leaf_match(
            RT_SUB_EQUAL_STRATEGY,
            inet("192.168.1.0/24"),
            query.clone()
        ));
        assert!(leaf_match(
            RT_OVERLAP_STRATEGY,
            inet("192.168.1.255/25"),
            query.clone()
        ));
        assert!(leaf_match(
            RT_SUPER_EQUAL_STRATEGY,
            inet("192.168.1.0/24"),
            query.clone()
        ));
        assert!(!leaf_match(
            RT_SUPER_STRATEGY,
            inet("192.168.1.0/24"),
            query.clone()
        ));
        assert!(leaf_match(RT_LESS_STRATEGY, inet("10.1.2.3/8"), query));
    }
}
