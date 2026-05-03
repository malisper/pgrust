pub mod network_ops;
pub mod tsquery_ops;
pub mod tsvector_ops;

use std::cmp::Ordering;

use pgrust_nodes::datum::Value;

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

pub type GistSortComparator = fn(&Value, &Value) -> Ordering;
