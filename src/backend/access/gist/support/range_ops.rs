use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::expr_range::{
    compare_lower_bounds, compare_range_values, compare_scalar_values, compare_upper_bounds,
    range_adjacent, range_contains_element, range_contains_range, range_merge,
    range_over_left_bounds, range_over_right_bounds, range_overlap, range_strict_left,
    range_strict_right,
};
use crate::include::nodes::datum::{RangeValue, Value};
use crate::include::nodes::primnodes::BuiltinScalarFunction;

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_range(value: &Value) -> Result<&RangeValue, CatalogError> {
    match value {
        Value::Range(range) => Ok(range),
        Value::Null => Err(CatalogError::Io(
            "GiST range support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST range support expected range value, got {other:?}"
        ))),
    }
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
    if matches!(query, Value::Multirange(_)) {
        return consistent_multirange_query(strategy, key, query, is_leaf);
    }
    let key = expect_range(key)?;
    if strategy == 16 {
        let matches = range_contains_element(key, query)
            .map_err(|err| CatalogError::Io(format!("gist range contains failed: {err:?}")))?;
        return Ok(GistConsistentResult {
            matches,
            recheck: false,
        });
    }
    let query = expect_range(query)?;
    let matches = if is_leaf {
        range_leaf_matches(strategy, key, query)?
    } else {
        range_internal_matches(strategy, key, query)?
    };
    Ok(GistConsistentResult {
        matches,
        recheck: false,
    })
}

fn range_leaf_matches(
    strategy: u16,
    key: &RangeValue,
    query: &RangeValue,
) -> Result<bool, CatalogError> {
    Ok(match strategy {
        1 => range_strict_left(key, query),
        2 => compare_upper_bounds(key.upper.as_ref(), query.upper.as_ref()) != Ordering::Greater,
        3 => range_overlap(key, query),
        4 => compare_lower_bounds(key.lower.as_ref(), query.lower.as_ref()) != Ordering::Less,
        5 => range_strict_right(key, query),
        6 => range_adjacent(key, query),
        7 => range_contains_range(key, query),
        8 => range_contains_range(query, key),
        16 => {
            return Err(CatalogError::Corrupt(
                "GiST range element strategy requires element query",
            ));
        }
        18 => compare_range_values(key, query) == Ordering::Equal,
        _ => return Err(CatalogError::Corrupt("unsupported GiST range strategy")),
    })
}

fn range_internal_matches(
    strategy: u16,
    key: &RangeValue,
    query: &RangeValue,
) -> Result<bool, CatalogError> {
    Ok(match strategy {
        1 => !key.empty && !query.empty && !range_over_right_bounds(key, query),
        2 => !key.empty && !query.empty && !range_strict_right(key, query),
        3 => range_overlap(key, query),
        4 => !key.empty && !query.empty && !range_strict_left(key, query),
        5 => !key.empty && !query.empty && !range_over_left_bounds(key, query),
        6 => {
            !key.empty && !query.empty && (range_adjacent(key, query) || range_overlap(key, query))
        }
        7 => range_contains_range(key, query),
        // :HACK: PostgreSQL's GiST range key tracks whether a subtree contains
        // empty ranges. pgrust's simplified key does not, so contained-by must
        // be conservative to avoid pruning empty rows from mixed pages.
        8 => key.empty || range_overlap(key, query),
        16 => {
            return Err(CatalogError::Corrupt(
                "GiST range element strategy requires element query",
            ));
        }
        18 => {
            if query.empty {
                key.empty
            } else {
                range_contains_range(key, query)
            }
        }
        _ => return Err(CatalogError::Corrupt("unsupported GiST range strategy")),
    })
}

fn consistent_multirange_query(
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    let matches = if is_leaf {
        multirange_leaf_matches(strategy, key, query)?
    } else {
        multirange_internal_matches(strategy, key, query)?
    };
    Ok(GistConsistentResult {
        matches,
        recheck: false,
    })
}

fn multirange_leaf_matches(
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
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
            return Err(CatalogError::Corrupt(
                "unsupported GiST range multirange strategy",
            ));
        }
    };
    eval_multirange_bool(func, key, query)
}

fn multirange_internal_matches(
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    let key_range = expect_range(key)?;
    Ok(match strategy {
        1 => !key_range.empty && multirange_not(BuiltinScalarFunction::RangeOverRight, key, query)?,
        2 => {
            !key_range.empty && multirange_not(BuiltinScalarFunction::RangeStrictRight, key, query)?
        }
        3 => eval_multirange_bool(BuiltinScalarFunction::RangeOverlap, key, query)?,
        4 => {
            !key_range.empty && multirange_not(BuiltinScalarFunction::RangeStrictLeft, key, query)?
        }
        5 => !key_range.empty && multirange_not(BuiltinScalarFunction::RangeOverLeft, key, query)?,
        6 => {
            !key_range.empty
                && (eval_multirange_bool(BuiltinScalarFunction::RangeAdjacent, key, query)?
                    || eval_multirange_bool(BuiltinScalarFunction::RangeOverlap, key, query)?)
        }
        7 => eval_multirange_bool(BuiltinScalarFunction::RangeContains, key, query)?,
        // See the range contained-by case above: the simplified internal key
        // cannot distinguish pages containing empty ranges.
        8 => {
            key_range.empty
                || eval_multirange_bool(BuiltinScalarFunction::RangeOverlap, key, query)?
        }
        _ => {
            return Err(CatalogError::Corrupt(
                "unsupported GiST range multirange strategy",
            ));
        }
    })
}

fn multirange_not(
    func: BuiltinScalarFunction,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    eval_multirange_bool(func, key, query).map(|value| !value)
}

fn eval_multirange_bool(
    func: BuiltinScalarFunction,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    let value = crate::backend::executor::expr_multirange::eval_multirange_function(
        func,
        &[key.clone(), query.clone()],
        None,
        false,
    )
    .ok_or(CatalogError::Corrupt(
        "unsupported GiST range multirange function",
    ))?
    .map_err(|err| CatalogError::Io(format!("gist range multirange failed: {err:?}")))?;
    let matches = match value {
        Value::Bool(value) => value,
        other => {
            return Err(CatalogError::Io(format!(
                "gist range multirange expected bool, got {other:?}"
            )));
        }
    };
    Ok(matches)
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    let mut iter = values.iter().filter(|value| !matches!(value, Value::Null));
    let Some(first) = iter.next() else {
        return Ok(Value::Null);
    };
    let mut out = expect_range(first)?.clone();
    for value in iter {
        out = range_merge(&out, expect_range(value)?);
    }
    Ok(Value::Range(out))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = expect_range(original)?;
    let candidate = expect_range(candidate)?;
    let merged = range_merge(original, candidate);
    let mut penalty = 0.0f32;
    if compare_lower_bounds(merged.lower.as_ref(), original.lower.as_ref()) != Ordering::Equal {
        penalty += 1.0;
    }
    if compare_upper_bounds(merged.upper.as_ref(), original.upper.as_ref()) != Ordering::Equal {
        penalty += 1.0;
    }
    if !range_contains_range(original, candidate) {
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
        let left = expect_range(&values[*left_idx]).unwrap();
        let right = expect_range(&values[*right_idx]).unwrap();
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
        _ => Ok(compare_range_values(expect_range(left)?, expect_range(right)?) == Ordering::Equal),
    }
}

pub(crate) fn sort_compare(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Range(left), Value::Range(right)) => compare_range_values(left, right),
        _ => compare_scalar_values(left, right),
    }
}
