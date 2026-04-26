use crate::backend::catalog::CatalogError;
use crate::backend::executor::expr_geometry::{
    bound_box, box_area, box_box_distance, box_contains_box, box_overlap, box_same,
};
use crate::include::nodes::datum::{GeoBox, Value};

use super::{GistColumnPickSplit, GistConsistentResult, GistDistanceResult};

fn expect_box(value: &Value) -> Result<&GeoBox, CatalogError> {
    match value {
        Value::Box(geo_box) => Ok(geo_box),
        Value::Null => Err(CatalogError::Io(
            "GiST box support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST box support expected box value, got {other:?}"
        ))),
    }
}

fn box_left(left: &GeoBox, right: &GeoBox) -> bool {
    left.high.x < right.low.x
}

fn box_over_left(left: &GeoBox, right: &GeoBox) -> bool {
    left.high.x <= right.high.x
}

fn box_right(left: &GeoBox, right: &GeoBox) -> bool {
    left.low.x > right.high.x
}

fn box_over_right(left: &GeoBox, right: &GeoBox) -> bool {
    left.low.x >= right.low.x
}

fn box_below(left: &GeoBox, right: &GeoBox) -> bool {
    left.high.y < right.low.y
}

fn box_over_below(left: &GeoBox, right: &GeoBox) -> bool {
    left.high.y <= right.high.y
}

fn box_above(left: &GeoBox, right: &GeoBox) -> bool {
    left.low.y > right.high.y
}

fn box_over_above(left: &GeoBox, right: &GeoBox) -> bool {
    left.low.y >= right.low.y
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
    let key = expect_box(key)?;
    let query = expect_box(query)?;
    let matches = if is_leaf {
        match strategy {
            1 => box_left(key, query),
            2 => box_over_left(key, query),
            3 => box_overlap(key, query),
            4 => box_over_right(key, query),
            5 => box_right(key, query),
            6 => box_same(key, query),
            7 => box_contains_box(key, query),
            8 => box_contains_box(query, key),
            9 => box_over_below(key, query),
            10 => box_below(key, query),
            11 => box_above(key, query),
            12 => box_over_above(key, query),
            _ => return Err(CatalogError::Corrupt("unsupported GiST box strategy")),
        }
    } else {
        match strategy {
            1 => !box_over_right(key, query),
            2 => !box_right(key, query),
            3 => box_overlap(key, query),
            4 => !box_left(key, query),
            5 => !box_over_left(key, query),
            6 | 7 => box_contains_box(key, query),
            8 => box_overlap(key, query),
            9 => !box_above(key, query),
            10 => !box_over_above(key, query),
            11 => !box_over_below(key, query),
            12 => !box_below(key, query),
            _ => return Err(CatalogError::Corrupt("unsupported GiST box strategy")),
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
    let mut out = expect_box(first)?.clone();
    for value in iter {
        out = bound_box(&out, expect_box(value)?);
    }
    Ok(Value::Box(out))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = expect_box(original)?;
    let candidate = expect_box(candidate)?;
    let merged = bound_box(original, candidate);
    Ok((box_area(&merged) - box_area(original)).max(0.0) as f32)
}

fn box_center(geo_box: &GeoBox) -> (f64, f64) {
    (
        (geo_box.low.x + geo_box.high.x) / 2.0,
        (geo_box.low.y + geo_box.high.y) / 2.0,
    )
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

    let boxes = values
        .iter()
        .map(expect_box)
        .collect::<Result<Vec<_>, _>>()?;
    let mut seed_pair = (0usize, 1usize, -1.0f64);
    for left in 0..boxes.len() {
        for right in (left + 1)..boxes.len() {
            let left_center = box_center(boxes[left]);
            let right_center = box_center(boxes[right]);
            let dx = left_center.0 - right_center.0;
            let dy = left_center.1 - right_center.1;
            let distance = dx * dx + dy * dy;
            if distance > seed_pair.2 {
                seed_pair = (left, right, distance);
            }
        }
    }

    let mut left = vec![seed_pair.0];
    let mut right = vec![seed_pair.1];
    let mut left_union_box = boxes[seed_pair.0].clone();
    let mut right_union_box = boxes[seed_pair.1].clone();

    for index in 0..boxes.len() {
        if index == seed_pair.0 || index == seed_pair.1 {
            continue;
        }
        let left_bound = bound_box(&left_union_box, boxes[index]);
        let right_bound = bound_box(&right_union_box, boxes[index]);
        let left_penalty = box_area(&left_bound) - box_area(&left_union_box);
        let right_penalty = box_area(&right_bound) - box_area(&right_union_box);
        if left_penalty < right_penalty
            || (left_penalty == right_penalty
                && (box_area(&left_union_box) < box_area(&right_union_box)
                    || (box_area(&left_union_box) == box_area(&right_union_box)
                        && left.len() <= right.len())))
        {
            left.push(index);
            left_union_box = left_bound;
        } else {
            right.push(index);
            right_union_box = right_bound;
        }
    }

    if right.is_empty() {
        let moved = left.pop().unwrap();
        right.push(moved);
        left_union_box = union(
            &left
                .iter()
                .map(|index| Value::Box(boxes[*index].clone()))
                .collect::<Vec<_>>(),
        )?
        .into_box()
        .unwrap();
        right_union_box = boxes[moved].clone();
    }

    Ok(GistColumnPickSplit {
        left,
        right,
        left_union: Value::Box(left_union_box),
        right_union: Value::Box(right_union_box),
    })
}

trait IntoBoxValue {
    fn into_box(self) -> Option<GeoBox>;
}

impl IntoBoxValue for Value {
    fn into_box(self) -> Option<GeoBox> {
        match self {
            Value::Box(geo_box) => Some(geo_box),
            _ => None,
        }
    }
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(true),
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Ok(box_same(expect_box(left)?, expect_box(right)?)),
    }
}

pub(crate) fn distance(
    key: &Value,
    query: &Value,
    _is_leaf: bool,
) -> Result<GistDistanceResult, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(GistDistanceResult {
            value: None,
            recheck: false,
        });
    }
    let key = expect_box(key)?;
    let query = expect_box(query)?;
    Ok(GistDistanceResult {
        value: Some(box_box_distance(key, query)),
        recheck: false,
    })
}
