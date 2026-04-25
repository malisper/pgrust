use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::ExecError;
use crate::backend::executor::expr_multirange::{
    compare_multirange_values, multirange_adjacent_multirange, multirange_adjacent_range,
    multirange_contains_element, multirange_contains_multirange, multirange_contains_range,
    multirange_from_range, multirange_overlaps_multirange, multirange_overlaps_range,
    range_contains_multirange, span_multirange,
};
use crate::backend::executor::expr_range::{
    compare_lower_bounds, compare_range_values, compare_scalar_values, compare_upper_bounds,
    range_contains_range, range_merge, range_over_left_bounds, range_over_right_bounds,
    range_strict_left, range_strict_right,
};
use crate::include::nodes::datum::{MultirangeValue, RangeValue, Value};

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_multirange(value: &Value) -> Result<&MultirangeValue, CatalogError> {
    match value {
        Value::Multirange(multirange) => Ok(multirange),
        Value::Null => Err(CatalogError::Io(
            "GiST multirange support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST multirange support expected multirange value, got {other:?}"
        ))),
    }
}

fn exec_result<T>(result: Result<T, ExecError>) -> Result<T, CatalogError> {
    result.map_err(|err| CatalogError::Io(format!("gist multirange support failed: {err:?}")))
}

fn value_span(value: &Value) -> Option<RangeValue> {
    match value {
        Value::Range(range) => Some(range.clone()),
        Value::Multirange(multirange) => Some(span_multirange(multirange)),
        _ => None,
    }
}

fn same_range_kind(left: &RangeValue, right: &RangeValue) -> bool {
    left.range_type.type_oid() == right.range_type.type_oid()
}

fn span_match(
    key: &MultirangeValue,
    query: &Value,
    op: impl FnOnce(&RangeValue, &RangeValue) -> bool,
) -> bool {
    let key_span = span_multirange(key);
    let Some(query_span) = value_span(query) else {
        return false;
    };
    same_range_kind(&key_span, &query_span) && op(&key_span, &query_span)
}

pub(crate) fn consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(GistConsistentResult {
            matches: false,
            recheck: false,
        });
    }
    let key = expect_multirange(key)?;
    if !is_leaf {
        return Ok(GistConsistentResult {
            matches: true,
            recheck: true,
        });
    }
    let matches = match strategy {
        1 => span_match(key, query, range_strict_left),
        2 => span_match(key, query, range_over_left_bounds),
        3 => match query {
            Value::Range(range) => multirange_overlaps_range(key, range),
            Value::Multirange(other) => multirange_overlaps_multirange(key, other),
            _ => false,
        },
        4 => span_match(key, query, range_over_right_bounds),
        5 => span_match(key, query, range_strict_right),
        6 => match query {
            Value::Range(range) => multirange_adjacent_range(key, range),
            Value::Multirange(other) => multirange_adjacent_multirange(key, other),
            _ => false,
        },
        7 => match query {
            Value::Range(range) => multirange_contains_range(key, range),
            Value::Multirange(other) => multirange_contains_multirange(key, other),
            _ => exec_result(multirange_contains_element(key, query))?,
        },
        8 => match query {
            Value::Range(range) => range_contains_multirange(range, key),
            Value::Multirange(other) => multirange_contains_multirange(other, key),
            _ => false,
        },
        16 => exec_result(multirange_contains_element(key, query))?,
        18 => match query {
            Value::Multirange(other) => compare_multirange_values(key, other) == Ordering::Equal,
            _ => false,
        },
        _ => {
            return Err(CatalogError::Corrupt(
                "unsupported GiST multirange strategy",
            ));
        }
    };
    Ok(GistConsistentResult {
        matches,
        recheck: false,
    })
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    let mut iter = values.iter().filter(|value| !matches!(value, Value::Null));
    let Some(first) = iter.next() else {
        return Ok(Value::Null);
    };
    let first = expect_multirange(first)?;
    let mut out = span_multirange(first);
    for value in iter {
        let multirange = expect_multirange(value)?;
        if !same_range_kind(&out, &span_multirange(multirange)) {
            return Err(CatalogError::Io(
                "GiST multirange union saw mixed range kinds".into(),
            ));
        }
        out = range_merge(&out, &span_multirange(multirange));
    }
    Ok(Value::Multirange(exec_result(multirange_from_range(&out))?))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = span_multirange(expect_multirange(original)?);
    let candidate = span_multirange(expect_multirange(candidate)?);
    if !same_range_kind(&original, &candidate) {
        return Ok(0.0);
    }
    let merged = range_merge(&original, &candidate);
    let mut penalty = 0.0f32;
    if compare_lower_bounds(merged.lower.as_ref(), original.lower.as_ref()) != Ordering::Equal {
        penalty += 1.0;
    }
    if compare_upper_bounds(merged.upper.as_ref(), original.upper.as_ref()) != Ordering::Equal {
        penalty += 1.0;
    }
    if !range_contains_range(&original, &candidate) {
        penalty += 0.5;
    }
    Ok(penalty)
}

pub(crate) fn picksplit(values: &[Value]) -> Result<GistColumnPickSplit, CatalogError> {
    if values.len() <= 1 {
        return Ok(GistColumnPickSplit {
            left: vec![0],
            right: Vec::new(),
            left_union: values.first().cloned().unwrap_or(Value::Null),
            right_union: Value::Null,
        });
    }
    let mut indexes = (0..values.len()).collect::<Vec<_>>();
    indexes.sort_by(|left_idx, right_idx| {
        let left = span_multirange(expect_multirange(&values[*left_idx]).unwrap());
        let right = span_multirange(expect_multirange(&values[*right_idx]).unwrap());
        compare_lower_bounds(left.lower.as_ref(), right.lower.as_ref())
            .then_with(|| compare_upper_bounds(left.upper.as_ref(), right.upper.as_ref()))
    });
    let split_at = (indexes.len() / 2).max(1);
    let left = indexes[..split_at].to_vec();
    let right = indexes[split_at..].to_vec();
    let left_union = union(
        &left
            .iter()
            .map(|index| values[*index].clone())
            .collect::<Vec<_>>(),
    )?;
    let right_union = union(
        &right
            .iter()
            .map(|index| values[*index].clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(GistColumnPickSplit {
        left,
        right,
        left_union,
        right_union,
    })
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(true),
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Ok(
            compare_multirange_values(expect_multirange(left)?, expect_multirange(right)?)
                == Ordering::Equal,
        ),
    }
}

pub(crate) fn sort_compare(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Multirange(left), Value::Multirange(right)) => {
            compare_multirange_values(left, right)
        }
        (Value::Range(left), Value::Range(right)) => compare_range_values(left, right),
        _ => compare_scalar_values(left, right),
    }
}
