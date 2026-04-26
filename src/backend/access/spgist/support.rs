use crate::backend::catalog::CatalogError;
use crate::backend::executor::{
    compare_network_values,
    expr_geometry::{GEOMETRY_EPSILON, box_contains_point},
    network_contains,
};
use crate::include::catalog::{
    SPG_BOX_QUAD_CHOOSE_PROC_OID, SPG_BOX_QUAD_CONFIG_PROC_OID,
    SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID,
    SPG_BOX_QUAD_PICKSPLIT_PROC_OID, SPG_KD_CHOOSE_PROC_OID, SPG_KD_CONFIG_PROC_OID,
    SPG_KD_INNER_CONSISTENT_PROC_OID, SPG_KD_PICKSPLIT_PROC_OID, SPG_NETWORK_CHOOSE_PROC_OID,
    SPG_NETWORK_CONFIG_PROC_OID, SPG_NETWORK_INNER_CONSISTENT_PROC_OID,
    SPG_NETWORK_LEAF_CONSISTENT_PROC_OID, SPG_NETWORK_PICKSPLIT_PROC_OID, SPG_QUAD_CHOOSE_PROC_OID,
    SPG_QUAD_CONFIG_PROC_OID, SPG_QUAD_INNER_CONSISTENT_PROC_OID,
    SPG_QUAD_LEAF_CONSISTENT_PROC_OID, SPG_QUAD_PICKSPLIT_PROC_OID, SPG_TEXT_CHOOSE_PROC_OID,
    SPG_TEXT_CONFIG_PROC_OID, SPG_TEXT_INNER_CONSISTENT_PROC_OID,
    SPG_TEXT_LEAF_CONSISTENT_PROC_OID, SPG_TEXT_PICKSPLIT_PROC_OID,
};
use crate::include::nodes::datum::{GeoBox, GeoPoint, InetValue, Value};

use super::quad_box;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpgistConfigResult {
    pub(crate) can_return_data: bool,
}

pub(crate) fn config(proc_oid: u32) -> Result<SpgistConfigResult, CatalogError> {
    match proc_oid {
        SPG_BOX_QUAD_CONFIG_PROC_OID
        | SPG_NETWORK_CONFIG_PROC_OID
        | SPG_QUAD_CONFIG_PROC_OID
        | SPG_KD_CONFIG_PROC_OID
        | SPG_TEXT_CONFIG_PROC_OID => Ok(SpgistConfigResult {
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
        SPG_NETWORK_CHOOSE_PROC_OID
        | SPG_QUAD_CHOOSE_PROC_OID
        | SPG_KD_CHOOSE_PROC_OID
        | SPG_TEXT_CHOOSE_PROC_OID => Ok(0),
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
        SPG_NETWORK_PICKSPLIT_PROC_OID
        | SPG_QUAD_PICKSPLIT_PROC_OID
        | SPG_KD_PICKSPLIT_PROC_OID
        | SPG_TEXT_PICKSPLIT_PROC_OID => Ok(None),
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
        SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID
        | SPG_NETWORK_INNER_CONSISTENT_PROC_OID
        | SPG_QUAD_INNER_CONSISTENT_PROC_OID
        | SPG_KD_INNER_CONSISTENT_PROC_OID
        | SPG_TEXT_INNER_CONSISTENT_PROC_OID => Ok((0u8..16).collect()),
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
        SPG_QUAD_LEAF_CONSISTENT_PROC_OID => point_leaf_consistent(strategy, key, query),
        SPG_TEXT_LEAF_CONSISTENT_PROC_OID => text_leaf_consistent(strategy, key, query),
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST leaf consistent proc {proc_oid}"
        ))),
    }
}

pub(crate) fn order_distance(
    proc_oid: u32,
    key: &Value,
    query: &Value,
) -> Result<Option<f64>, CatalogError> {
    match proc_oid {
        SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID => quad_box::order_distance(key, query),
        SPG_QUAD_LEAF_CONSISTENT_PROC_OID => point_order_distance(key, query),
        _ => Err(CatalogError::Io(format!(
            "unsupported SP-GiST order-by proc {proc_oid}"
        ))),
    }
}

fn expect_point(value: &Value) -> Result<&GeoPoint, CatalogError> {
    match value {
        Value::Point(value) => Ok(value),
        Value::Null => Err(CatalogError::Io(
            "SP-GiST point support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "SP-GiST point support expected point value, got {other:?}"
        ))),
    }
}

fn point_eq(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() <= GEOMETRY_EPSILON
}

fn point_same(left: &GeoPoint, right: &GeoPoint) -> bool {
    if left.x.is_nan() || left.y.is_nan() || right.x.is_nan() || right.y.is_nan() {
        return left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits();
    }
    point_eq(left.x, right.x) && point_eq(left.y, right.y)
}

fn point_leaf_consistent(strategy: u16, key: &Value, query: &Value) -> Result<bool, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    let key = expect_point(key)?;
    Ok(match strategy {
        1 => key.x + GEOMETRY_EPSILON < expect_point(query)?.x,
        5 => key.x > expect_point(query)?.x + GEOMETRY_EPSILON,
        6 => point_same(key, expect_point(query)?),
        8 => match query {
            Value::Box(geo_box) => box_contains_point(geo_box, key),
            other => {
                return Err(CatalogError::Io(format!(
                    "SP-GiST point contained-by strategy expected box value, got {other:?}"
                )));
            }
        },
        10 | 29 => key.y + GEOMETRY_EPSILON < expect_point(query)?.y,
        11 | 30 => key.y > expect_point(query)?.y + GEOMETRY_EPSILON,
        _ => {
            return Err(CatalogError::Corrupt("unsupported SP-GiST point strategy"));
        }
    })
}

fn point_order_distance(key: &Value, query: &Value) -> Result<Option<f64>, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(None);
    }
    let key = expect_point(key)?;
    let query = expect_point(query)?;
    Ok(Some((key.x - query.x).hypot(key.y - query.y)))
}

fn expect_text(value: &Value) -> Result<&str, CatalogError> {
    value.as_text().ok_or_else(|| {
        CatalogError::Io(format!(
            "SP-GiST text support expected text value, got {value:?}"
        ))
    })
}

fn text_leaf_consistent(strategy: u16, key: &Value, query: &Value) -> Result<bool, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    let key = expect_text(key)?;
    let query = expect_text(query)?;
    let cmp = key.as_bytes().cmp(query.as_bytes());
    Ok(match strategy {
        1 | 11 => cmp.is_lt(),
        2 | 12 => cmp.is_le(),
        3 => cmp.is_eq(),
        4 | 14 => cmp.is_ge(),
        5 | 15 => cmp.is_gt(),
        28 => key.starts_with(query),
        _ => {
            return Err(CatalogError::Corrupt("unsupported SP-GiST text strategy"));
        }
    })
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
