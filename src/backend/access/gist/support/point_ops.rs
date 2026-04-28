use std::cmp::Ordering;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::expr_geometry::GEOMETRY_EPSILON;
use crate::include::nodes::datum::{GeoBox, GeoCircle, GeoPoint, GeoPolygon, Value};

use super::{GistColumnPickSplit, GistConsistentResult};

fn expect_point(value: &Value) -> Result<&GeoPoint, CatalogError> {
    match value {
        Value::Point(point) => Ok(point),
        Value::Null => Err(CatalogError::Io(
            "GiST point support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "GiST point support expected point value, got {other:?}"
        ))),
    }
}

fn normalize_box(geo_box: &GeoBox) -> (f64, f64, f64, f64) {
    let min_x = geo_box.low.x.min(geo_box.high.x);
    let max_x = geo_box.low.x.max(geo_box.high.x);
    let min_y = geo_box.low.y.min(geo_box.high.y);
    let max_y = geo_box.low.y.max(geo_box.high.y);
    (min_x, max_x, min_y, max_y)
}

fn point_same(left: &GeoPoint, right: &GeoPoint) -> bool {
    if left.x.is_nan() || left.y.is_nan() || right.x.is_nan() || right.y.is_nan() {
        return left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits();
    }
    fp_eq(left.x, right.x) && fp_eq(left.y, right.y)
}

fn fp_eq(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() <= GEOMETRY_EPSILON
}

fn point_distance(left: &GeoPoint, right: &GeoPoint) -> f64 {
    (left.x - right.x).hypot(left.y - right.y)
}

fn point_in_box(point: &GeoPoint, geo_box: &GeoBox) -> bool {
    let (min_x, max_x, min_y, max_y) = normalize_box(geo_box);
    point.x >= min_x && point.x <= max_x && point.y >= min_y && point.y <= max_y
}

fn point_in_circle(point: &GeoPoint, circle: &GeoCircle) -> bool {
    point_distance(point, &circle.center) <= circle.radius
}

fn point_in_polygon(point: &GeoPoint, polygon: &GeoPolygon) -> bool {
    if polygon.points.len() < 3 || !point_in_box(point, &polygon.bound_box) {
        return false;
    }

    let mut inside = false;
    let mut prev = polygon.points.last().expect("polygon length checked");
    for current in &polygon.points {
        let crosses_y = (current.y > point.y) != (prev.y > point.y);
        if crosses_y {
            let x_intersect =
                (prev.x - current.x) * (point.y - current.y) / (prev.y - current.y) + current.x;
            if point.x <= x_intersect {
                inside = !inside;
            }
        }
        prev = current;
    }
    inside
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
    if !is_leaf {
        return Ok(GistConsistentResult {
            // :HACK: GiST tuple storage is not opckeytype-aware yet, so point_ops
            // cannot store PostgreSQL's internal box keys. Keep internal matches
            // conservative to preserve correctness until that refactor lands.
            matches: true,
            recheck: false,
        });
    }

    let key = expect_point(key)?;
    let matches = match (strategy, query) {
        (1, Value::Point(query)) => key.x < query.x,
        (5, Value::Point(query)) => key.x > query.x,
        (6, Value::Point(query)) => point_same(key, query),
        (10 | 29, Value::Point(query)) => key.y < query.y,
        (11 | 30, Value::Point(query)) => key.y > query.y,
        (8 | 28, Value::Box(query)) => point_in_box(key, query),
        (7 | 8 | 48, Value::Polygon(query)) => point_in_polygon(key, query),
        (7 | 8 | 68, Value::Circle(query)) => point_in_circle(key, query),
        _ => return Err(CatalogError::Corrupt("unsupported GiST point strategy")),
    };
    Ok(GistConsistentResult {
        matches,
        recheck: false,
    })
}

pub(crate) fn union(values: &[Value]) -> Result<Value, CatalogError> {
    let points = values
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(expect_point)
        .collect::<Result<Vec<_>, _>>()?;
    if points.is_empty() {
        return Ok(Value::Null);
    }

    let (sum_x, sum_y) = points.iter().fold((0.0, 0.0), |(sum_x, sum_y), point| {
        (sum_x + point.x, sum_y + point.y)
    });
    let count = points.len() as f64;
    Ok(Value::Point(GeoPoint {
        x: sum_x / count,
        y: sum_y / count,
    }))
}

pub(crate) fn penalty(original: &Value, candidate: &Value) -> Result<f32, CatalogError> {
    if matches!(original, Value::Null) || matches!(candidate, Value::Null) {
        return Ok(0.0);
    }
    Ok(point_distance(expect_point(original)?, expect_point(candidate)?) as f32)
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

    let points = values
        .iter()
        .map(expect_point)
        .collect::<Result<Vec<_>, _>>()?;
    let mut seed_pair = (0usize, 1usize, -1.0f64);
    for left in 0..points.len() {
        for right in (left + 1)..points.len() {
            let distance = point_distance(points[left], points[right]);
            if distance > seed_pair.2 {
                seed_pair = (left, right, distance);
            }
        }
    }

    let mut left = vec![seed_pair.0];
    let mut right = vec![seed_pair.1];
    let mut left_center = points[seed_pair.0].clone();
    let mut right_center = points[seed_pair.1].clone();

    for index in 0..points.len() {
        if index == seed_pair.0 || index == seed_pair.1 {
            continue;
        }
        let left_penalty = point_distance(&left_center, points[index]);
        let right_penalty = point_distance(&right_center, points[index]);
        if left_penalty < right_penalty
            || (left_penalty == right_penalty && left.len() <= right.len())
        {
            left.push(index);
            left_center = union(
                &left
                    .iter()
                    .map(|index| Value::Point(points[*index].clone()))
                    .collect::<Vec<_>>(),
            )?
            .into_point()
            .expect("point union must return point");
        } else {
            right.push(index);
            right_center = union(
                &right
                    .iter()
                    .map(|index| Value::Point(points[*index].clone()))
                    .collect::<Vec<_>>(),
            )?
            .into_point()
            .expect("point union must return point");
        }
    }

    Ok(GistColumnPickSplit {
        left,
        right,
        left_union: Value::Point(left_center),
        right_union: Value::Point(right_center),
    })
}

trait IntoPointValue {
    fn into_point(self) -> Option<GeoPoint>;
}

impl IntoPointValue for Value {
    fn into_point(self) -> Option<GeoPoint> {
        match self {
            Value::Point(point) => Some(point),
            _ => None,
        }
    }
}

pub(crate) fn same(left: &Value, right: &Value) -> Result<bool, CatalogError> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(true),
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Ok(point_same(expect_point(left)?, expect_point(right)?)),
    }
}

pub(crate) fn sort_compare(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Point(left), Value::Point(right)) => compare_points_zorder(left, right),
        _ => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}

fn compare_points_zorder(left: &GeoPoint, right: &GeoPoint) -> Ordering {
    if left.x == right.x && left.y == right.y {
        return Ordering::Equal;
    }
    point_zorder(left)
        .cmp(&point_zorder(right))
        .then_with(|| left.x.total_cmp(&right.x))
        .then_with(|| left.y.total_cmp(&right.y))
}

fn point_zorder(point: &GeoPoint) -> u64 {
    let x = ieee_float32_to_uint32(point.x as f32);
    let y = ieee_float32_to_uint32(point.y as f32);
    part_bits32_by2(x) | (part_bits32_by2(y) << 1)
}

fn ieee_float32_to_uint32(value: f32) -> u32 {
    if value.is_nan() {
        return 0xFFFF_FFFF;
    }
    let mut bits = value.to_bits();
    if bits & 0x8000_0000 != 0 {
        bits ^= 0xFFFF_FFFF;
    } else {
        bits |= 0x8000_0000;
    }
    bits
}

fn part_bits32_by2(value: u32) -> u64 {
    let mut n = u64::from(value);
    n = (n | (n << 16)) & 0x0000_FFFF_0000_FFFF;
    n = (n | (n << 8)) & 0x00FF_00FF_00FF_00FF;
    n = (n | (n << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    n = (n | (n << 2)) & 0x3333_3333_3333_3333;
    n = (n | (n << 1)) & 0x5555_5555_5555_5555;
    n
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::include::nodes::datum::{GeoPoint, Value};

    use super::{picksplit, sort_compare};

    fn point(x: f64, y: f64) -> Value {
        Value::Point(GeoPoint { x, y })
    }

    #[test]
    fn picksplit_balances_identical_points() {
        let values = vec![point(0.0, 0.0); 32];

        let split = picksplit(&values).unwrap();

        assert!(!split.left.is_empty());
        assert!(!split.right.is_empty());
        assert!(split.left.len().abs_diff(split.right.len()) <= 1);
    }

    #[test]
    fn sortsupport_uses_stable_z_order() {
        let mut values = vec![
            point(1.0, 1.0),
            point(-1.0, -1.0),
            point(0.0, 1.0),
            point(1.0, 0.0),
            Value::Null,
        ];

        values.sort_by(sort_compare);

        assert_eq!(values[0], Value::Null);
        assert!(
            values
                .windows(2)
                .all(|pair| sort_compare(&pair[0], &pair[1]) != Ordering::Greater)
        );
        assert_eq!(
            sort_compare(&point(0.0, 0.0), &point(-0.0, 0.0)),
            Ordering::Equal
        );
    }
}
