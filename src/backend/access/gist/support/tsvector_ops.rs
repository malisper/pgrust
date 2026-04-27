use crate::backend::catalog::CatalogError;
use crate::include::nodes::datum::Value;
use crate::include::nodes::tsearch::TsVector;

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_tsvector(value: &Value) -> Result<&TsVector, CatalogError> {
    match value {
        Value::TsVector(value) => Ok(value),
        Value::Null => Err(CatalogError::Io(
            "GiST tsvector support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST tsvector support expected tsvector value, got {other:?}"
        ))),
    }
}

pub(crate) fn consistent(
    _strategy: u16,
    key: &Value,
    query: &Value,
    _is_leaf: bool,
) -> Result<GistConsistentResult, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(GistConsistentResult {
            matches: false,
            recheck: false,
        });
    }
    if !matches!(query, Value::TsQuery(_)) {
        return Err(CatalogError::Corrupt("unsupported GiST tsvector query"));
    }
    Ok(GistConsistentResult {
        matches: true,
        recheck: true,
    })
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    let mut saw_value = false;
    for value in values.iter().filter(|value| !matches!(value, Value::Null)) {
        expect_tsvector(value)?;
        saw_value = true;
    }
    // :HACK: This lossy GiST tsvector opclass always heap-rechecks @@, so its
    // internal union key only needs to preserve non-NULL-ness. Storing the full
    // lexeme union can exceed a GiST page on the PostgreSQL tsearch fixture.
    Ok(if saw_value {
        Value::TsVector(TsVector::default())
    } else {
        Value::Null
    })
}

pub(crate) fn penalty(_original: &Value, _candidate: &Value) -> Result<f32, CatalogError> {
    Ok(0.0)
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
    let split_at = (values.len() / 2).max(1);
    let left = (0..split_at).collect::<Vec<_>>();
    let right = (split_at..values.len()).collect::<Vec<_>>();
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
        _ => Ok(expect_tsvector(left)? == expect_tsvector(right)?),
    }
}
