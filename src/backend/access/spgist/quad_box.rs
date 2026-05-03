// :HACK: root compatibility shim while SP-GiST quad-box support lives in `pgrust_access`.
use pgrust_access::spgist::quad_box as access_quad_box;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::{GeoBox, Value};

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

pub(crate) fn quadrant(centroid: &GeoBox, geo_box: &GeoBox) -> u8 {
    access_quad_box::quadrant(centroid, geo_box)
}

pub(crate) fn median_centroid(values: &[Value]) -> Result<Option<GeoBox>, CatalogError> {
    catalog_result(access_quad_box::median_centroid(values))
}

pub(crate) fn leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    catalog_result(access_quad_box::leaf_consistent(
        strategy,
        key,
        query,
        &RootAccessServices,
    ))
}

pub(crate) fn order_distance(key: &Value, query: &Value) -> Result<Option<f64>, CatalogError> {
    catalog_result(access_quad_box::order_distance(
        key,
        query,
        &RootAccessServices,
    ))
}

pub(crate) fn choose(proc_oid: u32, centroid: &Value, leaf: &Value) -> Result<u8, CatalogError> {
    catalog_result(access_quad_box::choose(proc_oid, centroid, leaf))
}

pub(crate) fn picksplit(
    proc_oid: u32,
    values: &[Value],
) -> Result<Option<(GeoBox, Vec<u8>)>, CatalogError> {
    catalog_result(access_quad_box::picksplit(proc_oid, values))
}
