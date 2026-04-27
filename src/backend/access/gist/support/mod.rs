mod box_ops;
mod geometry_ops;
mod multirange_ops;
mod network_ops;
mod point_ops;
mod range_ops;

// :HACK: The generic GiST core is wired for opclass dispatch, but this module
// intentionally exposes only the merge-bar families for now: box_ops exact+KNN,
// point/network exact search, range_ops exact search, and multirange_ops exact
// search. Additional families and range KNN stay deferred until their support
// procs/operators are implemented end to end.

use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::include::catalog::{
    GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID, GIST_BOX_PENALTY_PROC_OID,
    GIST_BOX_PICKSPLIT_PROC_OID, GIST_BOX_SAME_PROC_OID, GIST_BOX_UNION_PROC_OID,
    GIST_CIRCLE_CONSISTENT_PROC_OID, GIST_CIRCLE_DISTANCE_PROC_OID, GIST_CIRCLE_PENALTY_PROC_OID,
    GIST_CIRCLE_PICKSPLIT_PROC_OID, GIST_CIRCLE_SAME_PROC_OID, GIST_CIRCLE_UNION_PROC_OID,
    GIST_NETWORK_CONSISTENT_PROC_OID, GIST_NETWORK_PENALTY_PROC_OID,
    GIST_NETWORK_PICKSPLIT_PROC_OID, GIST_NETWORK_SAME_PROC_OID, GIST_NETWORK_UNION_PROC_OID,
    GIST_POINT_CONSISTENT_PROC_OID, GIST_POINT_PENALTY_PROC_OID, GIST_POINT_PICKSPLIT_PROC_OID,
    GIST_POINT_SAME_PROC_OID, GIST_POINT_UNION_PROC_OID, GIST_POLY_CONSISTENT_PROC_OID,
    GIST_POLY_DISTANCE_PROC_OID, GIST_POLY_PENALTY_PROC_OID, GIST_POLY_PICKSPLIT_PROC_OID,
    GIST_POLY_SAME_PROC_OID, GIST_POLY_UNION_PROC_OID, GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
    MULTIRANGE_GIST_CONSISTENT_PROC_OID, MULTIRANGE_GIST_PENALTY_PROC_OID,
    MULTIRANGE_GIST_PICKSPLIT_PROC_OID, MULTIRANGE_GIST_SAME_PROC_OID,
    MULTIRANGE_GIST_UNION_PROC_OID, MULTIRANGE_SORTSUPPORT_PROC_OID,
    RANGE_GIST_CONSISTENT_PROC_OID, RANGE_GIST_PENALTY_PROC_OID, RANGE_GIST_PICKSPLIT_PROC_OID,
    RANGE_GIST_SAME_PROC_OID, RANGE_GIST_UNION_PROC_OID, RANGE_SORTSUPPORT_PROC_OID,
};
use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GistConsistentResult {
    pub(crate) matches: bool,
    pub(crate) recheck: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GistDistanceResult {
    pub(crate) value: Option<f64>,
    pub(crate) recheck: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GistColumnPickSplit {
    pub(crate) left: Vec<usize>,
    pub(crate) right: Vec<usize>,
    pub(crate) left_union: Value,
    pub(crate) right_union: Value,
}

pub(crate) type GistSortComparator = fn(&Value, &Value) -> Ordering;

fn has_multirange_value(values: &[Value]) -> bool {
    values
        .iter()
        .any(|value| matches!(value, Value::Multirange(_)))
}

pub(crate) fn consistent(
    proc_oid: u32,
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    match proc_oid {
        GIST_BOX_CONSISTENT_PROC_OID => box_ops::consistent(strategy, key, query, is_leaf),
        GIST_POINT_CONSISTENT_PROC_OID => point_ops::consistent(strategy, key, query, is_leaf),
        GIST_POLY_CONSISTENT_PROC_OID => geometry_ops::consistent(
            geometry_ops::GeometryKind::Polygon,
            strategy,
            key,
            query,
            is_leaf,
        ),
        GIST_CIRCLE_CONSISTENT_PROC_OID => geometry_ops::consistent(
            geometry_ops::GeometryKind::Circle,
            strategy,
            key,
            query,
            is_leaf,
        ),
        RANGE_GIST_CONSISTENT_PROC_OID => range_ops::consistent(strategy, key, query, is_leaf),
        GIST_NETWORK_CONSISTENT_PROC_OID => network_ops::consistent(strategy, key, query, is_leaf),
        MULTIRANGE_GIST_CONSISTENT_PROC_OID => {
            multirange_ops::consistent(strategy, key, query, is_leaf)
        }
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST consistent proc {proc_oid}"
        ))),
    }
}

pub(crate) fn union(proc_oid: u32, values: &[Value]) -> Result<Value, CatalogError> {
    match proc_oid {
        GIST_BOX_UNION_PROC_OID => box_ops::union(values),
        GIST_POINT_UNION_PROC_OID => point_ops::union(values),
        // :HACK: PostgreSQL catalogs expose the range union/penalty/picksplit/same
        // support procs for the multirange GiST opfamily. Dispatch by runtime
        // value shape until pgrust's range and multirange support procs share
        // one implementation.
        RANGE_GIST_UNION_PROC_OID if has_multirange_value(values) => multirange_ops::union(values),
        GIST_POLY_UNION_PROC_OID => {
            geometry_ops::union(geometry_ops::GeometryKind::Polygon, values)
        }
        GIST_CIRCLE_UNION_PROC_OID => {
            geometry_ops::union(geometry_ops::GeometryKind::Circle, values)
        }
        RANGE_GIST_UNION_PROC_OID => range_ops::union(values),
        GIST_NETWORK_UNION_PROC_OID => network_ops::union(values),
        MULTIRANGE_GIST_UNION_PROC_OID => multirange_ops::union(values),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST union proc {proc_oid}"
        ))),
    }
}

pub(crate) fn penalty(
    proc_oid: u32,
    original: &Value,
    candidate: &Value,
) -> Result<f32, CatalogError> {
    match proc_oid {
        GIST_BOX_PENALTY_PROC_OID => box_ops::penalty(original, candidate),
        GIST_POINT_PENALTY_PROC_OID => point_ops::penalty(original, candidate),
        RANGE_GIST_PENALTY_PROC_OID
            if matches!(original, Value::Multirange(_))
                || matches!(candidate, Value::Multirange(_)) =>
        {
            multirange_ops::penalty(original, candidate)
        }
        GIST_POLY_PENALTY_PROC_OID | GIST_CIRCLE_PENALTY_PROC_OID => {
            geometry_ops::penalty(original, candidate)
        }
        RANGE_GIST_PENALTY_PROC_OID => range_ops::penalty(original, candidate),
        GIST_NETWORK_PENALTY_PROC_OID => network_ops::penalty(original, candidate),
        MULTIRANGE_GIST_PENALTY_PROC_OID => multirange_ops::penalty(original, candidate),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST penalty proc {proc_oid}"
        ))),
    }
}

pub(crate) fn picksplit(
    proc_oid: u32,
    values: &[Value],
) -> Result<GistColumnPickSplit, CatalogError> {
    match proc_oid {
        GIST_BOX_PICKSPLIT_PROC_OID => box_ops::picksplit(values),
        GIST_POINT_PICKSPLIT_PROC_OID => point_ops::picksplit(values),
        RANGE_GIST_PICKSPLIT_PROC_OID if has_multirange_value(values) => {
            multirange_ops::picksplit(values)
        }
        GIST_POLY_PICKSPLIT_PROC_OID => {
            geometry_ops::picksplit(geometry_ops::GeometryKind::Polygon, values)
        }
        GIST_CIRCLE_PICKSPLIT_PROC_OID => {
            geometry_ops::picksplit(geometry_ops::GeometryKind::Circle, values)
        }
        RANGE_GIST_PICKSPLIT_PROC_OID => range_ops::picksplit(values),
        GIST_NETWORK_PICKSPLIT_PROC_OID => network_ops::picksplit(values),
        MULTIRANGE_GIST_PICKSPLIT_PROC_OID => multirange_ops::picksplit(values),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST picksplit proc {proc_oid}"
        ))),
    }
}

pub(crate) fn same(proc_oid: u32, left: &Value, right: &Value) -> Result<bool, CatalogError> {
    match proc_oid {
        GIST_BOX_SAME_PROC_OID => box_ops::same(left, right),
        GIST_POINT_SAME_PROC_OID => point_ops::same(left, right),
        RANGE_GIST_SAME_PROC_OID
            if matches!(left, Value::Multirange(_)) || matches!(right, Value::Multirange(_)) =>
        {
            multirange_ops::same(left, right)
        }
        GIST_POLY_SAME_PROC_OID | GIST_CIRCLE_SAME_PROC_OID => geometry_ops::same(left, right),
        RANGE_GIST_SAME_PROC_OID => range_ops::same(left, right),
        GIST_NETWORK_SAME_PROC_OID => network_ops::same(left, right),
        MULTIRANGE_GIST_SAME_PROC_OID => multirange_ops::same(left, right),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST same proc {proc_oid}"
        ))),
    }
}

pub(crate) fn distance(
    proc_oid: u32,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistDistanceResult, CatalogError> {
    match proc_oid {
        GIST_BOX_DISTANCE_PROC_OID => box_ops::distance(key, query, is_leaf),
        GIST_POLY_DISTANCE_PROC_OID => {
            geometry_ops::distance(geometry_ops::GeometryKind::Polygon, key, query, is_leaf)
        }
        GIST_CIRCLE_DISTANCE_PROC_OID => {
            geometry_ops::distance(geometry_ops::GeometryKind::Circle, key, query, is_leaf)
        }
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST distance proc {proc_oid}"
        ))),
    }
}

pub(crate) fn sortsupport(proc_oid: u32) -> Option<GistSortComparator> {
    match proc_oid {
        RANGE_SORTSUPPORT_PROC_OID => Some(range_ops::sort_compare),
        MULTIRANGE_SORTSUPPORT_PROC_OID => Some(multirange_ops::sort_compare),
        _ => None,
    }
}

pub(crate) fn translate_cmptype(proc_oid: u32, cmp: Ordering) -> Result<Ordering, CatalogError> {
    match proc_oid {
        GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID => Ok(cmp),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST translate cmptype proc {proc_oid}"
        ))),
    }
}
