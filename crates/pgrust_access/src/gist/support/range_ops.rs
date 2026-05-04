use std::cmp::Ordering;

use pgrust_nodes::datum::{RangeValue, Value};
use pgrust_nodes::primnodes::BuiltinScalarFunction;

use crate::{AccessError, AccessResult, AccessScalarServices};

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_range(value: &Value) -> AccessResult<&RangeValue> {
    match value {
        Value::Range(range) => Ok(range),
        Value::Null => Err(AccessError::Unsupported(
            "GiST range support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "GiST range support expected range value, got {other:?}"
        ))),
    }
}

pub fn consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
    services: &dyn AccessScalarServices,
) -> AccessResult<GistConsistentResult> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(GistConsistentResult {
            matches: false,
            recheck: false,
        });
    }
    if matches!(query, Value::Multirange(_)) {
        return consistent_multirange_query(strategy, key, query, is_leaf, services);
    }
    let key = expect_range(key)?;
    if strategy == 16 {
        let matches = services.range_contains_element(key, query)?;
        return Ok(GistConsistentResult {
            matches,
            recheck: false,
        });
    }
    let query = expect_range(query)?;
    let matches = if is_leaf {
        range_leaf_matches(strategy, key, query, services)?
    } else {
        range_internal_matches(strategy, key, query, services)?
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
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    Ok(match strategy {
        1 => services.range_strict_left(key, query),
        2 => {
            services.compare_upper_bounds(key.upper.as_ref(), query.upper.as_ref())
                != Ordering::Greater
        }
        3 => services.range_overlap(key, query),
        4 => {
            services.compare_lower_bounds(key.lower.as_ref(), query.lower.as_ref())
                != Ordering::Less
        }
        5 => services.range_strict_right(key, query),
        6 => services.range_adjacent(key, query),
        7 => services.range_contains_range(key, query),
        8 => services.range_contains_range(query, key),
        16 => {
            return Err(AccessError::Corrupt(
                "GiST range element strategy requires element query",
            ));
        }
        18 => services.compare_range_values(key, query) == Ordering::Equal,
        _ => return Err(AccessError::Corrupt("unsupported GiST range strategy")),
    })
}

fn range_internal_matches(
    strategy: u16,
    key: &RangeValue,
    query: &RangeValue,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    Ok(match strategy {
        1 => !key.empty && !query.empty && !services.range_over_right_bounds(key, query),
        2 => !key.empty && !query.empty && !services.range_strict_right(key, query),
        3 => services.range_overlap(key, query),
        4 => !key.empty && !query.empty && !services.range_strict_left(key, query),
        5 => !key.empty && !query.empty && !services.range_over_left_bounds(key, query),
        6 => {
            !key.empty
                && !query.empty
                && (services.range_adjacent(key, query) || services.range_overlap(key, query))
        }
        7 => services.range_contains_range(key, query),
        // :HACK: PostgreSQL's GiST range key tracks whether a subtree contains
        // empty ranges. pgrust's simplified key does not, so contained-by must
        // be conservative to avoid pruning empty rows from mixed pages.
        8 => key.empty || services.range_overlap(key, query),
        16 => {
            return Err(AccessError::Corrupt(
                "GiST range element strategy requires element query",
            ));
        }
        18 => {
            if query.empty {
                key.empty
            } else {
                services.range_contains_range(key, query)
            }
        }
        _ => return Err(AccessError::Corrupt("unsupported GiST range strategy")),
    })
}

fn consistent_multirange_query(
    strategy: u16,
    key: &Value,
    query: &Value,
    is_leaf: bool,
    services: &dyn AccessScalarServices,
) -> AccessResult<GistConsistentResult> {
    let matches = if is_leaf {
        multirange_leaf_matches(strategy, key, query, services)?
    } else {
        multirange_internal_matches(strategy, key, query, services)?
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
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    let func = match strategy {
        1 => BuiltinScalarFunction::RangeStrictLeft,
        2 => BuiltinScalarFunction::RangeOverLeft,
        3 => BuiltinScalarFunction::RangeOverlap,
        4 => BuiltinScalarFunction::RangeOverRight,
        5 => BuiltinScalarFunction::RangeStrictRight,
        6 => BuiltinScalarFunction::RangeAdjacent,
        7 | 16 => BuiltinScalarFunction::RangeContains,
        8 => BuiltinScalarFunction::RangeContainedBy,
        18 => return Ok(false),
        _ => {
            return Err(AccessError::Corrupt(
                "unsupported GiST range multirange strategy",
            ));
        }
    };
    services.eval_multirange_bool(func, key, query)
}

fn multirange_internal_matches(
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    let key_range = expect_range(key)?;
    Ok(match strategy {
        1 => {
            !key_range.empty
                && !services.eval_multirange_bool(
                    BuiltinScalarFunction::RangeOverRight,
                    key,
                    query,
                )?
        }
        2 => {
            !key_range.empty
                && !services.eval_multirange_bool(
                    BuiltinScalarFunction::RangeStrictRight,
                    key,
                    query,
                )?
        }
        3 => services.eval_multirange_bool(BuiltinScalarFunction::RangeOverlap, key, query)?,
        4 => {
            !key_range.empty
                && !services.eval_multirange_bool(
                    BuiltinScalarFunction::RangeStrictLeft,
                    key,
                    query,
                )?
        }
        5 => {
            !key_range.empty
                && !services.eval_multirange_bool(
                    BuiltinScalarFunction::RangeOverLeft,
                    key,
                    query,
                )?
        }
        6 => {
            !key_range.empty
                && (services.eval_multirange_bool(
                    BuiltinScalarFunction::RangeAdjacent,
                    key,
                    query,
                )? || services.eval_multirange_bool(
                    BuiltinScalarFunction::RangeOverlap,
                    key,
                    query,
                )?)
        }
        7 | 16 => {
            services.eval_multirange_bool(BuiltinScalarFunction::RangeContains, key, query)?
        }
        // See the range contained-by case above: the simplified internal key
        // cannot distinguish pages containing empty ranges.
        8 => {
            key_range.empty
                || services.eval_multirange_bool(BuiltinScalarFunction::RangeOverlap, key, query)?
        }
        18 => false,
        _ => {
            return Err(AccessError::Corrupt(
                "unsupported GiST range multirange strategy",
            ));
        }
    })
}

pub fn union(values: &[Value], services: &dyn AccessScalarServices) -> AccessResult<Value> {
    let mut iter = values.iter().filter(|value| !matches!(value, Value::Null));
    let Some(first) = iter.next() else {
        return Ok(Value::Null);
    };
    let mut out = expect_range(first)?.clone();
    for value in iter {
        out = services.range_merge(&out, expect_range(value)?);
    }
    Ok(Value::Range(out))
}

pub fn penalty(
    original: &Value,
    candidate: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<f32> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = expect_range(original)?;
    let candidate = expect_range(candidate)?;
    let merged = services.range_merge(original, candidate);
    let mut penalty = 0.0f32;
    if services.compare_lower_bounds(merged.lower.as_ref(), original.lower.as_ref())
        != Ordering::Equal
    {
        penalty += 1.0;
    }
    if services.compare_upper_bounds(merged.upper.as_ref(), original.upper.as_ref())
        != Ordering::Equal
    {
        penalty += 1.0;
    }
    if !services.range_contains_range(original, candidate) {
        penalty += 0.5;
    }
    Ok(penalty)
}

pub fn picksplit(
    values: &[Value],
    services: &dyn AccessScalarServices,
) -> AccessResult<GistColumnPickSplit> {
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
        services
            .compare_lower_bounds(left.lower.as_ref(), right.lower.as_ref())
            .then_with(|| services.compare_upper_bounds(left.upper.as_ref(), right.upper.as_ref()))
    });
    let split_at = (indexes.len() / 2).max(1);
    let left = indexes[..split_at].to_vec();
    let right = indexes[split_at..].to_vec();
    let left_union = union(
        &left
            .iter()
            .map(|index| values[*index].clone())
            .collect::<Vec<_>>(),
        services,
    )?;
    let right_union = union(
        &right
            .iter()
            .map(|index| values[*index].clone())
            .collect::<Vec<_>>(),
        services,
    )?;
    Ok(GistColumnPickSplit {
        left,
        right,
        left_union,
        right_union,
    })
}

pub fn same(
    left: &Value,
    right: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(true),
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Ok(
            services.compare_range_values(expect_range(left)?, expect_range(right)?)
                == Ordering::Equal,
        ),
    }
}

pub fn sort_compare(left: &Value, right: &Value, services: &dyn AccessScalarServices) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Range(left), Value::Range(right)) => services.compare_range_values(left, right),
        _ => services.compare_scalar_values(left, right),
    }
}
