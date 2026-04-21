mod box_ops;
mod range_ops;

// :HACK: The generic GiST core is wired for opclass dispatch, but this module
// intentionally exposes only the merge-bar families for now: box_ops exact+KNN
// and range_ops exact search. Additional families and range KNN stay deferred
// until their support procs/operators are implemented end to end.

use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::include::catalog::{
    GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID, GIST_BOX_PENALTY_PROC_OID,
    GIST_BOX_PICKSPLIT_PROC_OID, GIST_BOX_SAME_PROC_OID, GIST_BOX_UNION_PROC_OID,
    GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID, RANGE_GIST_CONSISTENT_PROC_OID,
    RANGE_GIST_PENALTY_PROC_OID, RANGE_GIST_PICKSPLIT_PROC_OID, RANGE_GIST_SAME_PROC_OID,
    RANGE_GIST_UNION_PROC_OID, RANGE_SORTSUPPORT_PROC_OID,
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

pub(crate) fn consistent(
    proc_oid: u32,
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    match proc_oid {
        GIST_BOX_CONSISTENT_PROC_OID => box_ops::consistent(strategy, key, query, is_leaf),
        RANGE_GIST_CONSISTENT_PROC_OID => range_ops::consistent(strategy, key, query, is_leaf),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST consistent proc {proc_oid}"
        ))),
    }
}

pub(crate) fn union(proc_oid: u32, values: &[Value]) -> Result<Value, CatalogError> {
    match proc_oid {
        GIST_BOX_UNION_PROC_OID => box_ops::union(values),
        RANGE_GIST_UNION_PROC_OID => range_ops::union(values),
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
        RANGE_GIST_PENALTY_PROC_OID => range_ops::penalty(original, candidate),
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
        RANGE_GIST_PICKSPLIT_PROC_OID => range_ops::picksplit(values),
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST picksplit proc {proc_oid}"
        ))),
    }
}

pub(crate) fn same(proc_oid: u32, left: &Value, right: &Value) -> Result<bool, CatalogError> {
    match proc_oid {
        GIST_BOX_SAME_PROC_OID => box_ops::same(left, right),
        RANGE_GIST_SAME_PROC_OID => range_ops::same(left, right),
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
        _ => Err(CatalogError::Io(format!(
            "unsupported GiST distance proc {proc_oid}"
        ))),
    }
}

pub(crate) fn sortsupport(proc_oid: u32) -> Option<GistSortComparator> {
    match proc_oid {
        RANGE_SORTSUPPORT_PROC_OID => Some(range_ops::sort_compare),
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
