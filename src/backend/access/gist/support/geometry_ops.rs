use crate::backend::catalog::CatalogError;
use crate::backend::executor::expr_geometry::{
    GEOMETRY_EPSILON, bound_box, box_area, box_contains_box, box_overlap, box_same,
    point_polygon_distance, polygon_contains_polygon, polygon_overlap, polygon_same,
};
use crate::include::nodes::datum::{GeoBox, GeoCircle, GeoPoint, GeoPolygon, Value};

use super::{GistColumnPickSplit, GistConsistentResult, GistDistanceResult};

#[derive(Debug, Clone, Copy)]
pub(crate) enum GeometryKind {
    Polygon,
    Circle,
}

fn fp_eq(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() <= GEOMETRY_EPSILON
}

fn fp_le(left: f64, right: f64) -> bool {
    left <= right + GEOMETRY_EPSILON
}

fn point_same(left: &GeoPoint, right: &GeoPoint) -> bool {
    if left.x.is_nan() || left.y.is_nan() || right.x.is_nan() || right.y.is_nan() {
        return left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits();
    }
    fp_eq(left.x, right.x) && fp_eq(left.y, right.y)
}

fn point_distance(left: &GeoPoint, right: &GeoPoint) -> f64 {
    (left.x - right.x).hypot(left.y - right.y)
}

fn circle_same(left: &GeoCircle, right: &GeoCircle) -> bool {
    ((left.radius.is_nan() && right.radius.is_nan()) || fp_eq(left.radius, right.radius))
        && point_same(&left.center, &right.center)
}

fn circle_overlap(left: &GeoCircle, right: &GeoCircle) -> bool {
    fp_le(
        point_distance(&left.center, &right.center),
        left.radius + right.radius,
    )
}

fn circle_contains_circle(outer: &GeoCircle, inner: &GeoCircle) -> bool {
    fp_le(
        point_distance(&outer.center, &inner.center) + inner.radius,
        outer.radius,
    )
}

fn circle_point_distance(circle: &GeoCircle, point: &GeoPoint) -> f64 {
    (point_distance(&circle.center, point) - circle.radius).max(0.0)
}

fn circle_bound_box(circle: &GeoCircle) -> GeoBox {
    let radius = circle.radius.abs();
    GeoBox {
        high: GeoPoint {
            x: circle.center.x + radius,
            y: circle.center.y + radius,
        },
        low: GeoPoint {
            x: circle.center.x - radius,
            y: circle.center.y - radius,
        },
    }
}

fn polygon_from_box(geo_box: GeoBox) -> GeoPolygon {
    GeoPolygon {
        bound_box: geo_box.clone(),
        points: vec![
            GeoPoint {
                x: geo_box.low.x,
                y: geo_box.low.y,
            },
            GeoPoint {
                x: geo_box.low.x,
                y: geo_box.high.y,
            },
            GeoPoint {
                x: geo_box.high.x,
                y: geo_box.high.y,
            },
            GeoPoint {
                x: geo_box.high.x,
                y: geo_box.low.y,
            },
        ],
    }
}

fn circle_from_box(geo_box: GeoBox) -> GeoCircle {
    let center = GeoPoint {
        x: (geo_box.low.x + geo_box.high.x) / 2.0,
        y: (geo_box.low.y + geo_box.high.y) / 2.0,
    };
    GeoCircle {
        radius: point_distance(&center, &geo_box.high),
        center,
    }
}

fn value_from_box(kind: GeometryKind, geo_box: GeoBox) -> Value {
    match kind {
        GeometryKind::Polygon => Value::Polygon(polygon_from_box(geo_box)),
        GeometryKind::Circle => Value::Circle(circle_from_box(geo_box)),
    }
}

fn bounding_box(value: &Value) -> Result<GeoBox, CatalogError> {
    match value {
        Value::Box(geo_box) => Ok(geo_box.clone()),
        Value::Polygon(poly) => Ok(poly.bound_box.clone()),
        Value::Circle(circle) => Ok(circle_bound_box(circle)),
        Value::Null => Err(CatalogError::Io(
            "GiST geometry support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST geometry support expected polygon or circle value, got {other:?}"
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

fn box_consistent(strategy: u16, key: &GeoBox, query: &GeoBox, is_leaf: bool) -> bool {
    if is_leaf {
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
            _ => false,
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
            _ => false,
        }
    }
}

fn exact_polygon_consistent(strategy: u16, key: &GeoPolygon, query: &GeoPolygon) -> Option<bool> {
    Some(match strategy {
        3 => polygon_overlap(key, query),
        6 => polygon_same(key, query),
        7 => polygon_contains_polygon(key, query),
        8 => polygon_contains_polygon(query, key),
        _ => return None,
    })
}

fn exact_circle_consistent(strategy: u16, key: &GeoCircle, query: &GeoCircle) -> Option<bool> {
    Some(match strategy {
        3 => circle_overlap(key, query),
        6 => circle_same(key, query),
        7 => circle_contains_circle(key, query),
        8 => circle_contains_circle(query, key),
        _ => return None,
    })
}

pub(crate) fn consistent(
    kind: GeometryKind,
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
    let key_box = bounding_box(key)?;
    let query_box = bounding_box(query)?;
    if !is_leaf {
        return Ok(GistConsistentResult {
            matches: box_consistent(strategy, &key_box, &query_box, false),
            recheck: false,
        });
    }

    let exact = match (kind, key, query) {
        (GeometryKind::Polygon, Value::Polygon(key), Value::Polygon(query)) => {
            exact_polygon_consistent(strategy, key, query)
        }
        (GeometryKind::Circle, Value::Circle(key), Value::Circle(query)) => {
            exact_circle_consistent(strategy, key, query)
        }
        _ => None,
    };
    let matches = exact.unwrap_or_else(|| box_consistent(strategy, &key_box, &query_box, true));
    Ok(GistConsistentResult {
        matches,
        recheck: exact.is_none() && matches!(strategy, 3 | 6 | 7 | 8),
    })
}

pub(crate) fn union(kind: GeometryKind, values: &[Value]) -> Result<Value, CatalogError> {
    let mut iter = values.iter().filter(|value| !matches!(value, Value::Null));
    let Some(first) = iter.next() else {
        return Ok(Value::Null);
    };
    let mut out = bounding_box(first)?;
    for value in iter {
        out = bound_box(&out, &bounding_box(value)?);
    }
    Ok(value_from_box(kind, out))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    let original = bounding_box(original)?;
    let candidate = bounding_box(candidate)?;
    let merged = bound_box(&original, &candidate);
    Ok((box_area(&merged) - box_area(&original)).max(0.0) as f32)
}

fn box_center(geo_box: &GeoBox) -> (f64, f64) {
    (
        (geo_box.low.x + geo_box.high.x) / 2.0,
        (geo_box.low.y + geo_box.high.y) / 2.0,
    )
}

pub(crate) fn picksplit(
    kind: GeometryKind,
    values: &[Value],
) -> Result<GistColumnPickSplit, CatalogError> {
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
        .map(bounding_box)
        .collect::<Result<Vec<_>, _>>()?;
    let mut seed_pair = (0usize, 1usize, -1.0f64);
    for left in 0..boxes.len() {
        for right in (left + 1)..boxes.len() {
            let left_center = box_center(&boxes[left]);
            let right_center = box_center(&boxes[right]);
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
        let left_bound = bound_box(&left_union_box, &boxes[index]);
        let right_bound = bound_box(&right_union_box, &boxes[index]);
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
        left_union_box = left
            .iter()
            .fold(None, |acc: Option<GeoBox>, index| {
                let current = boxes[*index].clone();
                Some(match acc {
                    Some(acc) => bound_box(&acc, &current),
                    None => current,
                })
            })
            .unwrap_or_else(|| boxes[moved].clone());
        right_union_box = boxes[moved].clone();
    }

    Ok(GistColumnPickSplit {
        left,
        right,
        left_union: value_from_box(kind, left_union_box),
        right_union: value_from_box(kind, right_union_box),
    })
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(true),
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        (Value::Polygon(left), Value::Polygon(right)) => Ok(polygon_same(left, right)),
        (Value::Circle(left), Value::Circle(right)) => Ok(circle_same(left, right)),
        _ => Ok(box_same(&bounding_box(left)?, &bounding_box(right)?)),
    }
}

fn box_point_distance(geo_box: &GeoBox, point: &GeoPoint) -> f64 {
    let dx = if point.x < geo_box.low.x {
        geo_box.low.x - point.x
    } else if point.x > geo_box.high.x {
        point.x - geo_box.high.x
    } else {
        0.0
    };
    let dy = if point.y < geo_box.low.y {
        geo_box.low.y - point.y
    } else if point.y > geo_box.high.y {
        point.y - geo_box.high.y
    } else {
        0.0
    };
    dx.hypot(dy)
}

pub(crate) fn distance(
    kind: GeometryKind,
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
    let Value::Point(query) = query else {
        return Err(CatalogError::Corrupt(
            "unsupported GiST geometry distance query",
        ));
    };
    let value = match (kind, key) {
        (GeometryKind::Polygon, Value::Polygon(poly)) => point_polygon_distance(query, poly),
        (GeometryKind::Circle, Value::Circle(circle)) => circle_point_distance(circle, query),
        _ => box_point_distance(&bounding_box(key)?, query),
    };
    Ok(GistDistanceResult {
        value: Some(value),
        recheck: false,
    })
}
