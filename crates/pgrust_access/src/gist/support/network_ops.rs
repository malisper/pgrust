use std::cmp::Ordering;

use pgrust_nodes::datum::{InetValue, Value};

use crate::{AccessError, AccessResult, AccessScalarServices};

use super::{GistColumnPickSplit, GistConsistentResult};

pub const RT_OVERLAP_STRATEGY: u16 = 3;
pub const RT_EQUAL_STRATEGY: u16 = 18;
pub const RT_NOT_EQUAL_STRATEGY: u16 = 19;
pub const RT_LESS_STRATEGY: u16 = 20;
pub const RT_LESS_EQUAL_STRATEGY: u16 = 21;
pub const RT_GREATER_STRATEGY: u16 = 22;
pub const RT_GREATER_EQUAL_STRATEGY: u16 = 23;
pub const RT_SUB_STRATEGY: u16 = 24;
pub const RT_SUB_EQUAL_STRATEGY: u16 = 25;
pub const RT_SUPER_STRATEGY: u16 = 26;
pub const RT_SUPER_EQUAL_STRATEGY: u16 = 27;

fn expect_network(value: &Value) -> AccessResult<&InetValue> {
    match value {
        Value::Inet(value) | Value::Cidr(value) => Ok(value),
        Value::Null => Err(AccessError::Unsupported(
            "GiST network support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "GiST network support expected inet/cidr value, got {other:?}"
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
    let key = expect_network(key)?;
    let query = expect_network(query)?;
    if !is_leaf {
        return Ok(GistConsistentResult {
            matches: true,
            recheck: true,
        });
    }
    let matches = match strategy {
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
        _ => return Err(AccessError::Corrupt("unsupported GiST network strategy")),
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
    let mut out = expect_network(first)?.clone();
    for value in iter {
        out = services.network_merge(&out, expect_network(value)?);
    }
    Ok(Value::Cidr(out))
}

pub fn penalty(
    original: &Value,
    candidate: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<f32> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = expect_network(original)?;
    let candidate = expect_network(candidate)?;
    let merged = services.network_merge(original, candidate);
    let widening = original.bits.saturating_sub(merged.bits) as f32;
    let family_change = (std::mem::discriminant(&original.addr)
        != std::mem::discriminant(&candidate.addr)) as u8 as f32;
    Ok(widening + family_change)
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
        services.compare_network_values(
            expect_network(&values[*left_idx]).unwrap(),
            expect_network(&values[*right_idx]).unwrap(),
        )
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
            services.compare_network_values(expect_network(left)?, expect_network(right)?)
                == Ordering::Equal,
        ),
    }
}
