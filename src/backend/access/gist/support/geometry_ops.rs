// :HACK: root compatibility shim while GiST geometry support lives in `pgrust_access`.
use pgrust_access::gist::support::geometry_ops as access_geometry_ops;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::Value;

use super::{GistColumnPickSplit, GistConsistentResult, GistDistanceResult};

pub(crate) use access_geometry_ops::GeometryKind;

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
    kind: GeometryKind,
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    catalog_result(access_geometry_ops::consistent(
        kind,
        strategy,
        key,
        query,
        is_leaf,
        &RootAccessServices,
    ))
}

pub(crate) fn union(kind: GeometryKind, values: &[Value]) -> Result<Value, CatalogError> {
    catalog_result(access_geometry_ops::union(
        kind,
        values,
        &RootAccessServices,
    ))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    catalog_result(access_geometry_ops::penalty(
        original,
        candidate,
        &RootAccessServices,
    ))
}

pub(crate) fn picksplit(
    kind: GeometryKind,
    values: &[Value],
) -> Result<GistColumnPickSplit, CatalogError> {
    catalog_result(access_geometry_ops::picksplit(
        kind,
        values,
        &RootAccessServices,
    ))
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    catalog_result(access_geometry_ops::same(left, right, &RootAccessServices))
}

pub(crate) fn distance(
    kind: GeometryKind,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistDistanceResult, CatalogError> {
    catalog_result(access_geometry_ops::distance(
        kind,
        key,
        query,
        is_leaf,
        &RootAccessServices,
    ))
}
