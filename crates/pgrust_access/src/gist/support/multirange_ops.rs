use std::cmp::Ordering;

use pgrust_nodes::datum::{MultirangeValue, RangeValue, Value};

use crate::{AccessError, AccessResult, AccessScalarServices};

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_multirange(value: &Value) -> AccessResult<&MultirangeValue> {
    match value {
        Value::Multirange(multirange) => Ok(multirange),
        Value::Null => Err(AccessError::Unsupported(
            "GiST multirange support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "GiST multirange support expected multirange value, got {other:?}"
        ))),
    }
}

fn value_span(value: &Value, services: &dyn AccessScalarServices) -> Option<RangeValue> {
    match value {
        Value::Range(range) => Some(range.clone()),
        Value::Multirange(multirange) => Some(services.span_multirange(multirange)),
        _ => None,
    }
}

fn same_range_kind(left: &RangeValue, right: &RangeValue) -> bool {
    left.range_type.type_oid() == right.range_type.type_oid()
}

fn span_match(
    key: &MultirangeValue,
    query: &Value,
    services: &dyn AccessScalarServices,
    op: impl FnOnce(&RangeValue, &RangeValue) -> bool,
) -> bool {
    let key_span = services.span_multirange(key);
    let Some(query_span) = value_span(query, services) else {
        return false;
    };
    same_range_kind(&key_span, &query_span) && op(&key_span, &query_span)
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
    let key = expect_multirange(key)?;
    if !is_leaf {
        return Ok(GistConsistentResult {
            matches: true,
            recheck: true,
        });
    }
    let matches = match strategy {
        1 => span_match(key, query, services, |left, right| {
            services.range_strict_left(left, right)
        }),
        2 => span_match(key, query, services, |left, right| {
            services.range_over_left_bounds(left, right)
        }),
        3 => match query {
            Value::Range(range) => services.multirange_overlaps_range(key, range),
            Value::Multirange(other) => services.multirange_overlaps_multirange(key, other),
            _ => false,
        },
        4 => span_match(key, query, services, |left, right| {
            services.range_over_right_bounds(left, right)
        }),
        5 => span_match(key, query, services, |left, right| {
            services.range_strict_right(left, right)
        }),
        6 => match query {
            Value::Range(range) => services.multirange_adjacent_range(key, range),
            Value::Multirange(other) => services.multirange_adjacent_multirange(key, other),
            _ => false,
        },
        7 => match query {
            Value::Range(range) => services.multirange_contains_range(key, range),
            Value::Multirange(other) => services.multirange_contains_multirange(key, other),
            _ => services.multirange_contains_element(key, query)?,
        },
        8 => match query {
            Value::Range(range) => services.range_contains_multirange(range, key),
            Value::Multirange(other) => services.multirange_contains_multirange(other, key),
            _ => false,
        },
        16 => services.multirange_contains_element(key, query)?,
        18 => match query {
            Value::Multirange(other) => {
                services.compare_multirange_values(key, other) == Ordering::Equal
            }
            _ => false,
        },
        _ => return Err(AccessError::Corrupt("unsupported GiST multirange strategy")),
    };
    Ok(GistConsistentResult {
        matches,
        recheck: false,
    })
}

pub fn union(values: &[Value], services: &dyn AccessScalarServices) -> AccessResult<Value> {
    let mut iter = values.iter().filter(|value| !matches!(value, Value::Null));
    let Some(first) = iter.next() else {
        return Ok(Value::Null);
    };
    let first = expect_multirange(first)?;
    let mut out = services.span_multirange(first);
    for value in iter {
        let multirange = expect_multirange(value)?;
        if !same_range_kind(&out, &services.span_multirange(multirange)) {
            return Err(AccessError::Unsupported(
                "GiST multirange union saw mixed range kinds".into(),
            ));
        }
        out = services.range_merge(&out, &services.span_multirange(multirange));
    }
    Ok(Value::Multirange(services.multirange_from_range(&out)?))
}

pub fn penalty(
    original: &Value,
    candidate: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<f32> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = services.span_multirange(expect_multirange(original)?);
    let candidate = services.span_multirange(expect_multirange(candidate)?);
    if !same_range_kind(&original, &candidate) {
        return Ok(0.0);
    }
    let merged = services.range_merge(&original, &candidate);
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
    if !services.range_contains_range(&original, &candidate) {
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
        let left = services.span_multirange(expect_multirange(&values[*left_idx]).unwrap());
        let right = services.span_multirange(expect_multirange(&values[*right_idx]).unwrap());
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
        _ => Ok(services
            .compare_multirange_values(expect_multirange(left)?, expect_multirange(right)?)
            == Ordering::Equal),
    }
}

pub fn sort_compare(left: &Value, right: &Value, services: &dyn AccessScalarServices) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Multirange(left), Value::Multirange(right)) => {
            services.compare_multirange_values(left, right)
        }
        (Value::Range(left), Value::Range(right)) => services.compare_range_values(left, right),
        _ => services.compare_scalar_values(left, right),
    }
}
