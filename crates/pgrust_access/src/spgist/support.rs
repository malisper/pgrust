use std::cmp::Ordering;

use pgrust_catalog_data::pg_proc::{
    SPG_BOX_QUAD_CHOOSE_PROC_OID, SPG_BOX_QUAD_CONFIG_PROC_OID,
    SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID,
    SPG_BOX_QUAD_PICKSPLIT_PROC_OID, SPG_KD_CHOOSE_PROC_OID, SPG_KD_CONFIG_PROC_OID,
    SPG_KD_INNER_CONSISTENT_PROC_OID, SPG_KD_PICKSPLIT_PROC_OID, SPG_NETWORK_CHOOSE_PROC_OID,
    SPG_NETWORK_CONFIG_PROC_OID, SPG_NETWORK_INNER_CONSISTENT_PROC_OID,
    SPG_NETWORK_LEAF_CONSISTENT_PROC_OID, SPG_NETWORK_PICKSPLIT_PROC_OID, SPG_QUAD_CHOOSE_PROC_OID,
    SPG_QUAD_CONFIG_PROC_OID, SPG_QUAD_INNER_CONSISTENT_PROC_OID,
    SPG_QUAD_LEAF_CONSISTENT_PROC_OID, SPG_QUAD_PICKSPLIT_PROC_OID, SPG_RANGE_CHOOSE_PROC_OID,
    SPG_RANGE_CONFIG_PROC_OID, SPG_RANGE_INNER_CONSISTENT_PROC_OID,
    SPG_RANGE_LEAF_CONSISTENT_PROC_OID, SPG_RANGE_PICKSPLIT_PROC_OID, SPG_TEXT_CHOOSE_PROC_OID,
    SPG_TEXT_CONFIG_PROC_OID, SPG_TEXT_INNER_CONSISTENT_PROC_OID,
    SPG_TEXT_LEAF_CONSISTENT_PROC_OID, SPG_TEXT_PICKSPLIT_PROC_OID,
};
use pgrust_nodes::datum::{GeoBox, GeoPoint, InetValue, RangeValue, Value};
use pgrust_nodes::primnodes::BuiltinScalarFunction;

use crate::{AccessError, AccessResult, AccessScalarServices};

use super::quad_box;

const GEOMETRY_EPSILON: f64 = 1.0e-6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpgistConfigResult {
    pub can_return_data: bool,
}

pub fn config(proc_oid: u32) -> AccessResult<SpgistConfigResult> {
    match proc_oid {
        SPG_BOX_QUAD_CONFIG_PROC_OID
        | SPG_NETWORK_CONFIG_PROC_OID
        | SPG_QUAD_CONFIG_PROC_OID
        | SPG_KD_CONFIG_PROC_OID
        | SPG_RANGE_CONFIG_PROC_OID
        | SPG_TEXT_CONFIG_PROC_OID => Ok(SpgistConfigResult {
            can_return_data: true,
        }),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported SP-GiST config proc {proc_oid}"
        ))),
    }
}

pub fn choose(proc_oid: u32, centroid: &Value, leaf: &Value) -> AccessResult<u8> {
    match proc_oid {
        SPG_BOX_QUAD_CHOOSE_PROC_OID => quad_box::choose(proc_oid, centroid, leaf),
        SPG_NETWORK_CHOOSE_PROC_OID
        | SPG_QUAD_CHOOSE_PROC_OID
        | SPG_KD_CHOOSE_PROC_OID
        | SPG_RANGE_CHOOSE_PROC_OID
        | SPG_TEXT_CHOOSE_PROC_OID => {
            let _ = (centroid, leaf);
            Ok(0)
        }
        _ => Err(AccessError::Unsupported(format!(
            "unsupported SP-GiST choose proc {proc_oid}"
        ))),
    }
}

pub fn picksplit(proc_oid: u32, values: &[Value]) -> AccessResult<Option<(GeoBox, Vec<u8>)>> {
    match proc_oid {
        SPG_BOX_QUAD_PICKSPLIT_PROC_OID => quad_box::picksplit(proc_oid, values),
        SPG_NETWORK_PICKSPLIT_PROC_OID
        | SPG_QUAD_PICKSPLIT_PROC_OID
        | SPG_KD_PICKSPLIT_PROC_OID
        | SPG_RANGE_PICKSPLIT_PROC_OID
        | SPG_TEXT_PICKSPLIT_PROC_OID => {
            let _ = values;
            Ok(None)
        }
        _ => Err(AccessError::Unsupported(format!(
            "unsupported SP-GiST picksplit proc {proc_oid}"
        ))),
    }
}

pub fn inner_consistent(
    proc_oid: u32,
    _prefix: &Value,
    _strategies: &[(u16, Value)],
) -> AccessResult<Vec<u8>> {
    match proc_oid {
        // :HACK: The native SP-GiST boundary is now separate from GiST, but the
        // first box-only runtime still keeps a flat leaf-chain layout. A real
        // inner-node quadtree walk can replace this once page splitting grows
        // beyond append-only leaf pages.
        SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID
        | SPG_NETWORK_INNER_CONSISTENT_PROC_OID
        | SPG_QUAD_INNER_CONSISTENT_PROC_OID
        | SPG_KD_INNER_CONSISTENT_PROC_OID
        | SPG_RANGE_INNER_CONSISTENT_PROC_OID
        | SPG_TEXT_INNER_CONSISTENT_PROC_OID => Ok((0u8..16).collect()),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported SP-GiST inner consistent proc {proc_oid}"
        ))),
    }
}

pub fn leaf_consistent(
    proc_oid: u32,
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    match proc_oid {
        SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID => {
            quad_box::leaf_consistent(strategy, key, query, services)
        }
        SPG_NETWORK_LEAF_CONSISTENT_PROC_OID => {
            network_leaf_consistent(strategy, key, query, services)
        }
        SPG_QUAD_LEAF_CONSISTENT_PROC_OID => point_leaf_consistent(strategy, key, query, services),
        SPG_RANGE_LEAF_CONSISTENT_PROC_OID => range_leaf_consistent(strategy, key, query, services),
        SPG_TEXT_LEAF_CONSISTENT_PROC_OID => text_leaf_consistent(strategy, key, query),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported SP-GiST leaf consistent proc {proc_oid}"
        ))),
    }
}

pub fn order_distance(
    proc_oid: u32,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<Option<f64>> {
    match proc_oid {
        SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID => quad_box::order_distance(key, query, services),
        SPG_QUAD_LEAF_CONSISTENT_PROC_OID => point_order_distance(key, query),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported SP-GiST order-by proc {proc_oid}"
        ))),
    }
}

fn expect_point(value: &Value) -> AccessResult<&GeoPoint> {
    match value {
        Value::Point(value) => Ok(value),
        Value::Null => Err(AccessError::Unsupported(
            "SP-GiST point support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
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

fn point_leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    let key = expect_point(key)?;
    Ok(match strategy {
        1 => key.x + GEOMETRY_EPSILON < expect_point(query)?.x,
        5 => key.x > expect_point(query)?.x + GEOMETRY_EPSILON,
        6 => point_same(key, expect_point(query)?),
        8 => match query {
            Value::Box(geo_box) => services.box_contains_point(geo_box, key),
            other => {
                return Err(AccessError::Unsupported(format!(
                    "SP-GiST point contained-by strategy expected box value, got {other:?}"
                )));
            }
        },
        10 | 29 => key.y + GEOMETRY_EPSILON < expect_point(query)?.y,
        11 | 30 => key.y > expect_point(query)?.y + GEOMETRY_EPSILON,
        _ => return Err(AccessError::Corrupt("unsupported SP-GiST point strategy")),
    })
}

fn point_order_distance(key: &Value, query: &Value) -> AccessResult<Option<f64>> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(None);
    }
    let key = expect_point(key)?;
    let query = expect_point(query)?;
    Ok(Some((key.x - query.x).hypot(key.y - query.y)))
}

fn expect_text(value: &Value) -> AccessResult<&str> {
    value.as_text().ok_or_else(|| {
        AccessError::Unsupported(format!(
            "SP-GiST text support expected text value, got {value:?}"
        ))
    })
}

fn text_leaf_consistent(strategy: u16, key: &Value, query: &Value) -> AccessResult<bool> {
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
        _ => return Err(AccessError::Corrupt("unsupported SP-GiST text strategy")),
    })
}

fn expect_network(value: &Value) -> AccessResult<&InetValue> {
    match value {
        Value::Inet(value) | Value::Cidr(value) => Ok(value),
        Value::Null => Err(AccessError::Unsupported(
            "SP-GiST network support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "SP-GiST network support expected inet/cidr value, got {other:?}"
        ))),
    }
}

fn network_overlap(
    left: &InetValue,
    right: &InetValue,
    services: &dyn AccessScalarServices,
) -> bool {
    services.network_contains(left, right, false) || services.network_contains(right, left, false)
}

const RT_OVERLAP_STRATEGY: u16 = 3;
const RT_EQUAL_STRATEGY: u16 = 18;
const RT_NOT_EQUAL_STRATEGY: u16 = 19;
const RT_LESS_STRATEGY: u16 = 20;
const RT_LESS_EQUAL_STRATEGY: u16 = 21;
const RT_GREATER_STRATEGY: u16 = 22;
const RT_GREATER_EQUAL_STRATEGY: u16 = 23;
const RT_SUB_STRATEGY: u16 = 24;
const RT_SUB_EQUAL_STRATEGY: u16 = 25;
const RT_SUPER_STRATEGY: u16 = 26;
const RT_SUPER_EQUAL_STRATEGY: u16 = 27;

fn network_leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    let key = expect_network(key)?;
    let query = expect_network(query)?;
    Ok(match strategy {
        RT_OVERLAP_STRATEGY => network_overlap(key, query, services),
        RT_EQUAL_STRATEGY => services.compare_network_values(key, query) == Ordering::Equal,
        RT_NOT_EQUAL_STRATEGY => services.compare_network_values(key, query) != Ordering::Equal,
        RT_LESS_STRATEGY => services.compare_network_values(key, query) == Ordering::Less,
        RT_LESS_EQUAL_STRATEGY => services.compare_network_values(key, query) != Ordering::Greater,
        RT_GREATER_STRATEGY => services.compare_network_values(key, query) == Ordering::Greater,
        RT_GREATER_EQUAL_STRATEGY => services.compare_network_values(key, query) != Ordering::Less,
        RT_SUB_STRATEGY => services.network_contains(query, key, true),
        RT_SUB_EQUAL_STRATEGY => services.network_contains(query, key, false),
        RT_SUPER_STRATEGY => services.network_contains(key, query, true),
        RT_SUPER_EQUAL_STRATEGY => services.network_contains(key, query, false),
        _ => return Err(AccessError::Corrupt("unsupported SP-GiST network strategy")),
    })
}

fn expect_range(value: &Value) -> AccessResult<&RangeValue> {
    match value {
        Value::Range(range) => Ok(range),
        Value::Null => Err(AccessError::Unsupported(
            "SP-GiST range support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "SP-GiST range support expected range value, got {other:?}"
        ))),
    }
}

fn range_leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    if matches!(query, Value::Multirange(_)) {
        return range_multirange_leaf_consistent(strategy, key, query, services);
    }
    let key = expect_range(key)?;
    Ok(match strategy {
        1 => services.range_strict_left(key, expect_range(query)?),
        2 => {
            services.compare_upper_bounds(key.upper.as_ref(), expect_range(query)?.upper.as_ref())
                != Ordering::Greater
        }
        3 => services.range_overlap(key, expect_range(query)?),
        4 => {
            services.compare_lower_bounds(key.lower.as_ref(), expect_range(query)?.lower.as_ref())
                != Ordering::Less
        }
        5 => services.range_strict_right(key, expect_range(query)?),
        6 => services.range_adjacent(key, expect_range(query)?),
        7 => services.range_contains_range(key, expect_range(query)?),
        8 => services.range_contains_range(expect_range(query)?, key),
        16 => services.range_contains_element(key, query)?,
        18 => services.compare_range_values(key, expect_range(query)?) == Ordering::Equal,
        _ => return Err(AccessError::Corrupt("unsupported SP-GiST range strategy")),
    })
}

fn range_multirange_leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    let func = match strategy {
        1 => BuiltinScalarFunction::RangeStrictLeft,
        2 => BuiltinScalarFunction::RangeOverLeft,
        3 => BuiltinScalarFunction::RangeOverlap,
        4 => BuiltinScalarFunction::RangeOverRight,
        5 => BuiltinScalarFunction::RangeStrictRight,
        6 => BuiltinScalarFunction::RangeAdjacent,
        7 => BuiltinScalarFunction::RangeContains,
        8 => BuiltinScalarFunction::RangeContainedBy,
        _ => {
            return Err(AccessError::Corrupt(
                "unsupported SP-GiST range multirange strategy",
            ));
        }
    };
    services.eval_multirange_bool(func, key, query)
}
