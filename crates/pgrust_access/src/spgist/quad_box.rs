use pgrust_nodes::datum::{GeoBox, GeoPoint, Value};

use crate::{AccessError, AccessResult, AccessScalarServices};

fn expect_box(value: &Value) -> AccessResult<&GeoBox> {
    match value {
        Value::Box(geo_box) => Ok(geo_box),
        Value::Null => Err(AccessError::Unsupported(
            "SP-GiST box support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "SP-GiST box support expected box value, got {other:?}"
        ))),
    }
}

fn expect_point(value: &Value) -> AccessResult<&GeoPoint> {
    match value {
        Value::Point(point) => Ok(point),
        Value::Null => Err(AccessError::Unsupported(
            "SP-GiST box ORDER BY cannot use NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "SP-GiST box ORDER BY expected point value, got {other:?}"
        ))),
    }
}

fn expect_box_or_polygon_bounds(value: &Value) -> AccessResult<&GeoBox> {
    match value {
        Value::Box(geo_box) => Ok(geo_box),
        Value::Polygon(poly) => Ok(&poly.bound_box),
        Value::Null => Err(AccessError::Unsupported(
            "SP-GiST quad support cannot index NULL".into(),
        )),
        other => Err(AccessError::Unsupported(format!(
            "SP-GiST quad support expected box or polygon value, got {other:?}"
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

pub fn quadrant(centroid: &GeoBox, geo_box: &GeoBox) -> u8 {
    let mut quadrant = 0u8;
    if geo_box.low.x > centroid.low.x {
        quadrant |= 0x8;
    }
    if geo_box.high.x > centroid.high.x {
        quadrant |= 0x4;
    }
    if geo_box.low.y > centroid.low.y {
        quadrant |= 0x2;
    }
    if geo_box.high.y > centroid.high.y {
        quadrant |= 0x1;
    }
    quadrant
}

pub fn median_centroid(values: &[Value]) -> AccessResult<Option<GeoBox>> {
    if values.is_empty() {
        return Ok(None);
    }
    let boxes = values
        .iter()
        .map(expect_box_or_polygon_bounds)
        .collect::<AccessResult<Vec<_>>>()?;
    let mut low_xs = boxes
        .iter()
        .map(|geo_box| geo_box.low.x)
        .collect::<Vec<_>>();
    let mut high_xs = boxes
        .iter()
        .map(|geo_box| geo_box.high.x)
        .collect::<Vec<_>>();
    let mut low_ys = boxes
        .iter()
        .map(|geo_box| geo_box.low.y)
        .collect::<Vec<_>>();
    let mut high_ys = boxes
        .iter()
        .map(|geo_box| geo_box.high.y)
        .collect::<Vec<_>>();
    low_xs.sort_by(f64::total_cmp);
    high_xs.sort_by(f64::total_cmp);
    low_ys.sort_by(f64::total_cmp);
    high_ys.sort_by(f64::total_cmp);
    let median = boxes.len() / 2;
    Ok(Some(GeoBox {
        low: GeoPoint {
            x: low_xs[median],
            y: low_ys[median],
        },
        high: GeoPoint {
            x: high_xs[median],
            y: high_ys[median],
        },
    }))
}

pub fn leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<bool> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    Ok(match (key, query) {
        (Value::Polygon(key_poly), Value::Polygon(query_poly)) => match strategy {
            1 => box_left(&key_poly.bound_box, &query_poly.bound_box),
            2 => box_over_left(&key_poly.bound_box, &query_poly.bound_box),
            3 => services.polygon_overlap(key_poly, query_poly),
            4 => box_over_right(&key_poly.bound_box, &query_poly.bound_box),
            5 => box_right(&key_poly.bound_box, &query_poly.bound_box),
            6 => services.polygon_same(key_poly, query_poly),
            7 => services.polygon_contains_polygon(key_poly, query_poly),
            8 => services.polygon_contains_polygon(query_poly, key_poly),
            9 => box_over_below(&key_poly.bound_box, &query_poly.bound_box),
            10 => box_below(&key_poly.bound_box, &query_poly.bound_box),
            11 => box_above(&key_poly.bound_box, &query_poly.bound_box),
            12 => box_over_above(&key_poly.bound_box, &query_poly.bound_box),
            _ => return Err(AccessError::Corrupt("unsupported SP-GiST polygon strategy")),
        },
        _ => {
            let key = expect_box(key)?;
            let query = expect_box(query)?;
            match strategy {
                1 => box_left(key, query),
                2 => box_over_left(key, query),
                3 => services.box_overlap(key, query),
                4 => box_over_right(key, query),
                5 => box_right(key, query),
                6 => services.box_same(key, query),
                7 => services.box_contains_box(key, query),
                8 => services.box_contains_box(query, key),
                9 => box_over_below(key, query),
                10 => box_below(key, query),
                11 => box_above(key, query),
                12 => box_over_above(key, query),
                _ => return Err(AccessError::Corrupt("unsupported SP-GiST box strategy")),
            }
        }
    })
}

pub fn order_distance(
    key: &Value,
    query: &Value,
    services: &dyn AccessScalarServices,
) -> AccessResult<Option<f64>> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(None);
    }
    Ok(Some(match (key, query) {
        (Value::Polygon(poly), Value::Point(point)) => services.point_polygon_distance(point, poly),
        (Value::Box(key_box), Value::Point(point)) => point_box_distance(point, key_box),
        (Value::Box(key_box), Value::Box(query_box)) => {
            services.box_box_distance(key_box, query_box)
        }
        (Value::Polygon(_), other) => {
            let _ = expect_point(other)?;
            unreachable!()
        }
        (_, other) => {
            return Err(AccessError::Unsupported(format!(
                "SP-GiST quad ORDER BY expected point or box value, got {other:?}"
            )));
        }
    }))
}

fn point_box_distance(point: &GeoPoint, geo_box: &GeoBox) -> f64 {
    let closest_x = point.x.clamp(geo_box.low.x, geo_box.high.x);
    let closest_y = point.y.clamp(geo_box.low.y, geo_box.high.y);
    let dx = point.x - closest_x;
    let dy = point.y - closest_y;
    dx.hypot(dy)
}

pub fn choose(_proc_oid: u32, centroid: &Value, leaf: &Value) -> AccessResult<u8> {
    Ok(quadrant(
        expect_box_or_polygon_bounds(centroid)?,
        expect_box_or_polygon_bounds(leaf)?,
    ))
}

pub fn picksplit(_proc_oid: u32, values: &[Value]) -> AccessResult<Option<(GeoBox, Vec<u8>)>> {
    let Some(centroid) = median_centroid(values)? else {
        return Ok(None);
    };
    let assignments = values
        .iter()
        .map(|value| Ok(quadrant(&centroid, expect_box_or_polygon_bounds(value)?)))
        .collect::<AccessResult<Vec<_>>>()?;
    Ok(Some((centroid, assignments)))
}
