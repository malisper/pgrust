pub mod box_ops;
pub mod geometry_ops;
pub mod multirange_ops;
pub mod network_ops;
pub mod point_ops;
pub mod range_ops;
pub mod tsquery_ops;
pub mod tsvector_ops;

use std::cmp::Ordering;

use pgrust_catalog_data::{
    GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID, GIST_BOX_PENALTY_PROC_OID,
    GIST_BOX_PICKSPLIT_PROC_OID, GIST_BOX_SAME_PROC_OID, GIST_BOX_UNION_PROC_OID,
    GIST_CIRCLE_CONSISTENT_PROC_OID, GIST_CIRCLE_DISTANCE_PROC_OID, GIST_CIRCLE_PENALTY_PROC_OID,
    GIST_CIRCLE_PICKSPLIT_PROC_OID, GIST_CIRCLE_SAME_PROC_OID, GIST_CIRCLE_UNION_PROC_OID,
    GIST_NETWORK_CONSISTENT_PROC_OID, GIST_NETWORK_PENALTY_PROC_OID,
    GIST_NETWORK_PICKSPLIT_PROC_OID, GIST_NETWORK_SAME_PROC_OID, GIST_NETWORK_UNION_PROC_OID,
    GIST_POINT_CONSISTENT_PROC_OID, GIST_POINT_DISTANCE_PROC_OID, GIST_POINT_PENALTY_PROC_OID,
    GIST_POINT_PICKSPLIT_PROC_OID, GIST_POINT_SAME_PROC_OID, GIST_POINT_SORTSUPPORT_PROC_OID,
    GIST_POINT_UNION_PROC_OID, GIST_POLY_CONSISTENT_PROC_OID, GIST_POLY_DISTANCE_PROC_OID,
    GIST_POLY_PENALTY_PROC_OID, GIST_POLY_PICKSPLIT_PROC_OID, GIST_POLY_SAME_PROC_OID,
    GIST_POLY_UNION_PROC_OID, GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
    GIST_TSQUERY_CONSISTENT_PROC_OID, GIST_TSQUERY_PENALTY_PROC_OID,
    GIST_TSQUERY_PICKSPLIT_PROC_OID, GIST_TSQUERY_SAME_PROC_OID, GIST_TSQUERY_UNION_PROC_OID,
    GIST_TSVECTOR_CONSISTENT_PROC_OID, GIST_TSVECTOR_PENALTY_PROC_OID,
    GIST_TSVECTOR_PICKSPLIT_PROC_OID, GIST_TSVECTOR_SAME_PROC_OID, GIST_TSVECTOR_UNION_PROC_OID,
    MULTIRANGE_GIST_CONSISTENT_PROC_OID, MULTIRANGE_GIST_PENALTY_PROC_OID,
    MULTIRANGE_GIST_PICKSPLIT_PROC_OID, MULTIRANGE_GIST_SAME_PROC_OID,
    MULTIRANGE_GIST_UNION_PROC_OID, MULTIRANGE_SORTSUPPORT_PROC_OID,
    RANGE_GIST_CONSISTENT_PROC_OID, RANGE_GIST_PENALTY_PROC_OID, RANGE_GIST_PICKSPLIT_PROC_OID,
    RANGE_GIST_SAME_PROC_OID, RANGE_GIST_UNION_PROC_OID, RANGE_SORTSUPPORT_PROC_OID,
};
use pgrust_nodes::datum::Value;

use crate::{AccessError, AccessResult, AccessScalarServices};

#[derive(Debug, Clone, PartialEq)]
pub struct GistConsistentResult {
    pub matches: bool,
    pub recheck: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GistDistanceResult {
    pub value: Option<f64>,
    pub recheck: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GistColumnPickSplit {
    pub left: Vec<usize>,
    pub right: Vec<usize>,
    pub left_union: Value,
    pub right_union: Value,
}

pub type GistSortComparator = fn(&Value, &Value, &dyn AccessScalarServices) -> Ordering;

fn has_multirange_value(values: &[Value]) -> bool {
    values
        .iter()
        .any(|value| matches!(value, Value::Multirange(_)))
}

pub fn consistent(
    proc_oid: u32,
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
    services: &dyn AccessScalarServices,
) -> AccessResult<GistConsistentResult> {
    match proc_oid {
        GIST_BOX_CONSISTENT_PROC_OID => {
            box_ops::consistent(strategy, key, query, is_leaf, services)
        }
        GIST_POINT_CONSISTENT_PROC_OID => {
            point_ops::consistent(strategy, key, query, is_leaf, services)
        }
        GIST_POLY_CONSISTENT_PROC_OID => geometry_ops::consistent(
            geometry_ops::GeometryKind::Polygon,
            strategy,
            key,
            query,
            is_leaf,
            services,
        ),
        GIST_CIRCLE_CONSISTENT_PROC_OID => geometry_ops::consistent(
            geometry_ops::GeometryKind::Circle,
            strategy,
            key,
            query,
            is_leaf,
            services,
        ),
        RANGE_GIST_CONSISTENT_PROC_OID => {
            range_ops::consistent(strategy, key, query, is_leaf, services)
        }
        GIST_NETWORK_CONSISTENT_PROC_OID => {
            network_ops::consistent(strategy, key, query, is_leaf, services)
        }
        GIST_TSVECTOR_CONSISTENT_PROC_OID => {
            tsvector_ops::consistent(strategy, key, query, is_leaf)
        }
        GIST_TSQUERY_CONSISTENT_PROC_OID => tsquery_ops::consistent(strategy, key, query, is_leaf),
        MULTIRANGE_GIST_CONSISTENT_PROC_OID => {
            multirange_ops::consistent(strategy, key, query, is_leaf, services)
        }
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST consistent proc {proc_oid}"
        ))),
    }
}

pub fn union(
    proc_oid: u32,
    values: &[Value],
    services: &dyn AccessScalarServices,
) -> AccessResult<Value> {
    match proc_oid {
        GIST_BOX_UNION_PROC_OID => box_ops::union(values, services),
        GIST_POINT_UNION_PROC_OID => point_ops::union(values),
        RANGE_GIST_UNION_PROC_OID if has_multirange_value(values) => {
            multirange_ops::union(values, services)
        }
        GIST_POLY_UNION_PROC_OID => {
            geometry_ops::union(geometry_ops::GeometryKind::Polygon, values, services)
        }
        GIST_CIRCLE_UNION_PROC_OID => {
            geometry_ops::union(geometry_ops::GeometryKind::Circle, values, services)
        }
        RANGE_GIST_UNION_PROC_OID => range_ops::union(values, services),
        GIST_NETWORK_UNION_PROC_OID => network_ops::union(values, services),
        GIST_TSVECTOR_UNION_PROC_OID => tsvector_ops::union(values),
        GIST_TSQUERY_UNION_PROC_OID => tsquery_ops::union(values),
        MULTIRANGE_GIST_UNION_PROC_OID => multirange_ops::union(values, services),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST union proc {proc_oid}"
        ))),
    }
}

pub fn penalty(
    proc_oid: u32,
    original: &Value,
    candidate: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<f32> {
    match proc_oid {
        GIST_BOX_PENALTY_PROC_OID => box_ops::penalty(original, candidate, services),
        GIST_POINT_PENALTY_PROC_OID => point_ops::penalty(original, candidate),
        RANGE_GIST_PENALTY_PROC_OID
            if matches!(original, Value::Multirange(_))
                || matches!(candidate, Value::Multirange(_)) =>
        {
            multirange_ops::penalty(original, candidate, services)
        }
        GIST_POLY_PENALTY_PROC_OID | GIST_CIRCLE_PENALTY_PROC_OID => {
            geometry_ops::penalty(original, candidate, services)
        }
        RANGE_GIST_PENALTY_PROC_OID => range_ops::penalty(original, candidate, services),
        GIST_NETWORK_PENALTY_PROC_OID => network_ops::penalty(original, candidate, services),
        GIST_TSVECTOR_PENALTY_PROC_OID => tsvector_ops::penalty(original, candidate),
        GIST_TSQUERY_PENALTY_PROC_OID => tsquery_ops::penalty(original, candidate),
        MULTIRANGE_GIST_PENALTY_PROC_OID => multirange_ops::penalty(original, candidate, services),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST penalty proc {proc_oid}"
        ))),
    }
}

pub fn picksplit(
    proc_oid: u32,
    values: &[Value],
    services: &dyn AccessScalarServices,
) -> AccessResult<GistColumnPickSplit> {
    match proc_oid {
        GIST_BOX_PICKSPLIT_PROC_OID => box_ops::picksplit(values, services),
        GIST_POINT_PICKSPLIT_PROC_OID => point_ops::picksplit(values),
        RANGE_GIST_PICKSPLIT_PROC_OID if has_multirange_value(values) => {
            multirange_ops::picksplit(values, services)
        }
        GIST_POLY_PICKSPLIT_PROC_OID => {
            geometry_ops::picksplit(geometry_ops::GeometryKind::Polygon, values, services)
        }
        GIST_CIRCLE_PICKSPLIT_PROC_OID => {
            geometry_ops::picksplit(geometry_ops::GeometryKind::Circle, values, services)
        }
        RANGE_GIST_PICKSPLIT_PROC_OID => range_ops::picksplit(values, services),
        GIST_NETWORK_PICKSPLIT_PROC_OID => network_ops::picksplit(values, services),
        GIST_TSVECTOR_PICKSPLIT_PROC_OID => tsvector_ops::picksplit(values),
        GIST_TSQUERY_PICKSPLIT_PROC_OID => tsquery_ops::picksplit(values),
        MULTIRANGE_GIST_PICKSPLIT_PROC_OID => multirange_ops::picksplit(values, services),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST picksplit proc {proc_oid}"
        ))),
    }
}

pub fn same(
    proc_oid: u32,
    left: &Value,
    right: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    match proc_oid {
        GIST_BOX_SAME_PROC_OID => box_ops::same(left, right, services),
        GIST_POINT_SAME_PROC_OID => point_ops::same(left, right),
        RANGE_GIST_SAME_PROC_OID
            if matches!(left, Value::Multirange(_)) || matches!(right, Value::Multirange(_)) =>
        {
            multirange_ops::same(left, right, services)
        }
        GIST_POLY_SAME_PROC_OID | GIST_CIRCLE_SAME_PROC_OID => {
            geometry_ops::same(left, right, services)
        }
        RANGE_GIST_SAME_PROC_OID => range_ops::same(left, right, services),
        GIST_NETWORK_SAME_PROC_OID => network_ops::same(left, right, services),
        GIST_TSVECTOR_SAME_PROC_OID => tsvector_ops::same(left, right),
        GIST_TSQUERY_SAME_PROC_OID => tsquery_ops::same(left, right),
        MULTIRANGE_GIST_SAME_PROC_OID => multirange_ops::same(left, right, services),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST same proc {proc_oid}"
        ))),
    }
}

pub fn distance(
    proc_oid: u32,
    key: &Value,
    query: &Value,
    is_leaf: bool,
    services: &dyn AccessScalarServices,
) -> AccessResult<GistDistanceResult> {
    match proc_oid {
        GIST_BOX_DISTANCE_PROC_OID => box_ops::distance(key, query, is_leaf, services),
        GIST_POINT_DISTANCE_PROC_OID => point_ops::distance(key, query, is_leaf),
        GIST_POLY_DISTANCE_PROC_OID => geometry_ops::distance(
            geometry_ops::GeometryKind::Polygon,
            key,
            query,
            is_leaf,
            services,
        ),
        GIST_CIRCLE_DISTANCE_PROC_OID => geometry_ops::distance(
            geometry_ops::GeometryKind::Circle,
            key,
            query,
            is_leaf,
            services,
        ),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST distance proc {proc_oid}"
        ))),
    }
}

fn point_sort_compare(
    left: &Value,
    right: &Value,
    _services: &dyn AccessScalarServices,
) -> Ordering {
    point_ops::sort_compare(left, right)
}

fn range_sort_compare(
    left: &Value,
    right: &Value,
    services: &dyn AccessScalarServices,
) -> Ordering {
    range_ops::sort_compare(left, right, services)
}

fn multirange_sort_compare(
    left: &Value,
    right: &Value,
    services: &dyn AccessScalarServices,
) -> Ordering {
    multirange_ops::sort_compare(left, right, services)
}

pub fn sortsupport(proc_oid: u32) -> Option<GistSortComparator> {
    match proc_oid {
        RANGE_SORTSUPPORT_PROC_OID => Some(range_sort_compare),
        GIST_POINT_SORTSUPPORT_PROC_OID => Some(point_sort_compare),
        MULTIRANGE_SORTSUPPORT_PROC_OID => Some(multirange_sort_compare),
        _ => None,
    }
}

pub fn translate_cmptype(proc_oid: u32, cmp: Ordering) -> AccessResult<Ordering> {
    match proc_oid {
        GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID => Ok(cmp),
        _ => Err(AccessError::Unsupported(format!(
            "unsupported GiST translate cmptype proc {proc_oid}"
        ))),
    }
}
