// :HACK: root compatibility shim while GiST box support lives in `pgrust_access`.
use pgrust_access::gist::support::box_ops as access_box_ops;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::Value;

use super::{GistColumnPickSplit, GistConsistentResult, GistDistanceResult};

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Interrupted(reason) => CatalogError::Interrupted(reason),
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
    catalog_result(access_box_ops::consistent(
        strategy,
        key,
        query,
        is_leaf,
        &RootAccessServices,
    ))
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    catalog_result(access_box_ops::union(values, &RootAccessServices))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    catalog_result(access_box_ops::penalty(
        original,
        candidate,
        &RootAccessServices,
    ))
}

pub(crate) fn picksplit(values: &[Value]) -> Result<GistColumnPickSplit, CatalogError> {
    catalog_result(access_box_ops::picksplit(values, &RootAccessServices))
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    catalog_result(access_box_ops::same(left, right, &RootAccessServices))
}

pub(crate) fn distance(
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistDistanceResult, CatalogError> {
    catalog_result(access_box_ops::distance(
        key,
        query,
        is_leaf,
        &RootAccessServices,
    ))
}
