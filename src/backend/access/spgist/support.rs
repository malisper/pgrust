// :HACK: root compatibility shim while SP-GiST support dispatch lives in `pgrust_access`.
use pgrust_access::spgist::support as access_support;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::{GeoBox, Value};

pub(crate) use access_support::SpgistConfigResult;

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

pub(crate) fn config(proc_oid: u32) -> Result<SpgistConfigResult, CatalogError> {
    catalog_result(access_support::config(proc_oid))
}

pub(crate) fn choose(proc_oid: u32, centroid: &Value, leaf: &Value) -> Result<u8, CatalogError> {
    catalog_result(access_support::choose(proc_oid, centroid, leaf))
}

pub(crate) fn picksplit(
    proc_oid: u32,
    values: &[Value],
) -> Result<Option<(GeoBox, Vec<u8>)>, CatalogError> {
    catalog_result(access_support::picksplit(proc_oid, values))
}

pub(crate) fn inner_consistent(
    proc_oid: u32,
    prefix: &Value,
    strategies: &[(u16, Value)],
) -> Result<Vec<u8>, CatalogError> {
    catalog_result(access_support::inner_consistent(
        proc_oid, prefix, strategies,
    ))
}

pub(crate) fn leaf_consistent(
    proc_oid: u32,
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    catalog_result(access_support::leaf_consistent(
        proc_oid,
        strategy,
        key,
        query,
        &RootAccessServices,
    ))
}

pub(crate) fn order_distance(
    proc_oid: u32,
    key: &Value,
    query: &Value,
) -> Result<Option<f64>, CatalogError> {
    catalog_result(access_support::order_distance(
        proc_oid,
        key,
        query,
        &RootAccessServices,
    ))
}

#[cfg(test)]
mod network_tests {
    use pgrust_catalog_data::pg_proc::SPG_NETWORK_LEAF_CONSISTENT_PROC_OID;

    use crate::backend::executor::{parse_cidr_text, parse_inet_text};
    use crate::include::nodes::datum::Value;

    use super::*;

    const RT_OVERLAP_STRATEGY: u16 = 3;
    const RT_LESS_STRATEGY: u16 = 20;
    const RT_SUB_STRATEGY: u16 = 24;
    const RT_SUB_EQUAL_STRATEGY: u16 = 25;
    const RT_SUPER_STRATEGY: u16 = 26;
    const RT_SUPER_EQUAL_STRATEGY: u16 = 27;

    fn inet(text: &str) -> Value {
        Value::Inet(parse_inet_text(text).unwrap())
    }

    fn cidr(text: &str) -> Value {
        Value::Cidr(parse_cidr_text(text).unwrap())
    }

    fn leaf_match(strategy: u16, key: Value, query: Value) -> bool {
        leaf_consistent(SPG_NETWORK_LEAF_CONSISTENT_PROC_OID, strategy, &key, &query).unwrap()
    }

    #[test]
    fn network_spgist_uses_catalog_strategy_numbers() {
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
