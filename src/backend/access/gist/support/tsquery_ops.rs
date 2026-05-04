// :HACK: root compatibility shim while GiST tsquery support lives in `pgrust_access`.
use pgrust_access::gist::support::tsquery_ops as access_tsquery_ops;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::Value;

use super::{GistColumnPickSplit, GistConsistentResult};

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
    catalog_result(access_tsquery_ops::consistent(
        strategy, key, query, is_leaf,
    ))
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    catalog_result(access_tsquery_ops::union(values))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    catalog_result(access_tsquery_ops::penalty(original, candidate))
}

pub(crate) fn picksplit(values: &[Value]) -> Result<GistColumnPickSplit, CatalogError> {
    catalog_result(access_tsquery_ops::picksplit(values))
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    catalog_result(access_tsquery_ops::same(left, right))
}
