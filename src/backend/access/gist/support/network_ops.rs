use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::{compare_network_values, network_contains, network_merge};
use crate::include::nodes::datum::{InetValue, Value};

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_network(value: &Value) -> Result<&InetValue, CatalogError> {
    match value {
        Value::Inet(value) | Value::Cidr(value) => Ok(value),
        Value::Null => Err(CatalogError::Io(
            "GiST network support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST network support expected inet/cidr value, got {other:?}"
        ))),
    }
}

fn network_overlap(left: &InetValue, right: &InetValue) -> bool {
    network_contains(left, right, false) || network_contains(right, left, false)
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
    let key = expect_network(key)?;
    let query = expect_network(query)?;
    if !is_leaf {
        return Ok(GistConsistentResult {
            matches: network_overlap(key, query),
            recheck: true,
        });
    }
    let matches = match strategy {
        1 => network_contains(query, key, true),
        2 => network_contains(query, key, false),
        3 => network_contains(key, query, true),
        4 => network_contains(key, query, false),
        5 => network_overlap(key, query),
        6 => compare_network_values(key, query) == Ordering::Equal,
        _ => return Err(CatalogError::Corrupt("unsupported GiST network strategy")),
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
    let mut out = expect_network(first)?.clone();
    for value in iter {
        out = network_merge(&out, expect_network(value)?);
    }
    Ok(Value::Cidr(out))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = expect_network(original)?;
    let candidate = expect_network(candidate)?;
    let merged = network_merge(original, candidate);
    let widening = original.bits.saturating_sub(merged.bits) as f32;
    let family_change = (std::mem::discriminant(&original.addr)
        != std::mem::discriminant(&candidate.addr)) as u8 as f32;
    Ok(widening + family_change)
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
        compare_network_values(
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
            compare_network_values(expect_network(left)?, expect_network(right)?)
                == Ordering::Equal,
        ),
    }
}
