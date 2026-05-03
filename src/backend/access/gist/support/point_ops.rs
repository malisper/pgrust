use std::cmp::Ordering;

// :HACK: root compatibility shim while GiST point support lives in `pgrust_access`.
use pgrust_access::gist::support::point_ops as access_point_ops;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::Value;

use super::{GistColumnPickSplit, GistConsistentResult, GistDistanceResult};

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
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
    catalog_result(access_point_ops::consistent(
        strategy,
        key,
        query,
        is_leaf,
        &RootAccessServices,
    ))
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    catalog_result(access_point_ops::union(values))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    catalog_result(access_point_ops::penalty(original, candidate))
}

pub(crate) fn distance(
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistDistanceResult, CatalogError> {
    catalog_result(access_point_ops::distance(key, query, is_leaf))
}

pub(crate) fn picksplit(values: &[Value]) -> Result<GistColumnPickSplit, CatalogError> {
    catalog_result(access_point_ops::picksplit(values))
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    catalog_result(access_point_ops::same(left, right))
}

pub(crate) fn sort_compare(left: &Value, right: &Value) -> Ordering {
    access_point_ops::sort_compare(left, right)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::include::nodes::datum::{GeoPoint, Value};

    use super::{picksplit, sort_compare};

    fn point(x: f64, y: f64) -> Value {
        Value::Point(GeoPoint { x, y })
    }

    #[test]
    fn picksplit_balances_identical_points() {
        let values = vec![point(0.0, 0.0); 32];

        let split = picksplit(&values).unwrap();

        assert!(!split.left.is_empty());
        assert!(!split.right.is_empty());
        assert!(split.left.len().abs_diff(split.right.len()) <= 1);
    }

    #[test]
    fn sortsupport_uses_stable_z_order() {
        let mut values = vec![
            point(1.0, 1.0),
            point(-1.0, -1.0),
            point(0.0, 1.0),
            point(1.0, 0.0),
            Value::Null,
        ];

        values.sort_by(sort_compare);

        assert_eq!(values[0], Value::Null);
        assert!(
            values
                .windows(2)
                .all(|pair| sort_compare(&pair[0], &pair[1]) != Ordering::Greater)
        );
        assert_eq!(
            sort_compare(&point(0.0, 0.0), &point(-0.0, 0.0)),
            Ordering::Equal
        );
    }
}
