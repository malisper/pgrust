use crate::backend::catalog::CatalogError;
use crate::backend::executor::{compare_network_values, network_contains};
use crate::include::catalog::{
    SPG_BOX_QUAD_CHOOSE_PROC_OID, SPG_BOX_QUAD_CONFIG_PROC_OID,
    SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID,
    SPG_BOX_QUAD_PICKSPLIT_PROC_OID, SPG_NETWORK_CHOOSE_PROC_OID, SPG_NETWORK_CONFIG_PROC_OID,
    SPG_NETWORK_INNER_CONSISTENT_PROC_OID, SPG_NETWORK_LEAF_CONSISTENT_PROC_OID,
    SPG_NETWORK_PICKSPLIT_PROC_OID,
};
use crate::include::nodes::datum::{GeoBox, InetValue, Value};

use super::quad_box;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpgistConfigResult {
    pub(crate) can_return_data: bool,
}

pub(crate) fn config(proc_oid: u32) -> Result<SpgistConfigResult, CatalogError> {
    match proc_oid {
        SPG_BOX_QUAD_CONFIG_PROC_OID | SPG_NETWORK_CONFIG_PROC_OID => Ok(SpgistConfigResult {
            can_return_data: true,
        }),
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST config proc {proc_oid}"
        ))),
    }
}

pub(crate) fn choose(proc_oid: u32, centroid: &Value, leaf: &Value) -> Result<u8, CatalogError> {
    match proc_oid {
        SPG_BOX_QUAD_CHOOSE_PROC_OID => quad_box::choose(proc_oid, centroid, leaf),
        SPG_NETWORK_CHOOSE_PROC_OID => Ok(0),
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST choose proc {proc_oid}"
        ))),
    }
}

pub(crate) fn picksplit(
    proc_oid: u32,
    values: &[Value],
) -> Result<Option<(GeoBox, Vec<u8>)>, CatalogError> {
    match proc_oid {
        SPG_BOX_QUAD_PICKSPLIT_PROC_OID => quad_box::picksplit(proc_oid, values),
        SPG_NETWORK_PICKSPLIT_PROC_OID => Ok(None),
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST picksplit proc {proc_oid}"
        ))),
    }
}

pub(crate) fn inner_consistent(
    proc_oid: u32,
    _prefix: &Value,
    _strategies: &[(u16, Value)],
) -> Result<Vec<u8>, CatalogError> {
    match proc_oid {
        // :HACK: The native SP-GiST boundary is now separate from GiST, but the
        // first box-only runtime still keeps a flat leaf-chain layout. A real
        // inner-node quadtree walk can replace this once page splitting grows
        // beyond append-only leaf pages.
        SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID | SPG_NETWORK_INNER_CONSISTENT_PROC_OID => {
            Ok((0u8..16).collect())
        }
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST inner consistent proc {proc_oid}"
        ))),
    }
}

pub(crate) fn leaf_consistent(
    proc_oid: u32,
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    match proc_oid {
        SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID => quad_box::leaf_consistent(strategy, key, query),
        SPG_NETWORK_LEAF_CONSISTENT_PROC_OID => network_leaf_consistent(strategy, key, query),
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST leaf consistent proc {proc_oid}"
        ))),
    }
}

fn expect_network(value: &Value) -> Result<&InetValue, CatalogError> {
    match value {
        Value::Inet(value) | Value::Cidr(value) => Ok(value),
        Value::Null => Err(CatalogError::Io(
            "SP-GiST network support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "SP-GiST network support expected inet/cidr value, got {other:?}"
        ))),
    }
}

fn network_overlap(left: &InetValue, right: &InetValue) -> bool {
    network_contains(left, right, false) || network_contains(right, left, false)
}

fn network_leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    let key = expect_network(key)?;
    let query = expect_network(query)?;
    Ok(match strategy {
        1 => network_contains(query, key, true),
        2 => network_contains(query, key, false),
        3 => network_contains(key, query, true),
        4 => network_contains(key, query, false),
        5 => network_overlap(key, query),
        6 => compare_network_values(key, query) == std::cmp::Ordering::Equal,
        _ => {
            return Err(CatalogError::Corrupt(
                "unsupported SP-GiST network strategy",
            ));
        }
    })
}
