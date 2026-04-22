use crate::backend::catalog::CatalogError;
use crate::backend::executor::expr_geometry::{
    box_box_distance, box_contains_box, box_overlap, box_same,
};
use crate::include::nodes::datum::{GeoBox, GeoPoint, Value};

fn expect_box(value: &Value) -> Result<&GeoBox, CatalogError> {
    match value {
        Value::Box(geo_box) => Ok(geo_box),
        Value::Null => Err(CatalogError::Io(
            "SP-GiST box support cannot index NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "SP-GiST box support expected box value, got {other:?}"
        ))),
    }
}

fn expect_point(value: &Value) -> Result<&GeoPoint, CatalogError> {
    match value {
        Value::Point(point) => Ok(point),
        Value::Null => Err(CatalogError::Io(
            "SP-GiST box ORDER BY cannot use NULL".into(),
        )),
        other => Err(CatalogError::Io(format!(
            "SP-GiST box ORDER BY expected point value, got {other:?}"
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

pub(crate) fn quadrant(centroid: &GeoBox, geo_box: &GeoBox) -> u8 {
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

pub(crate) fn median_centroid(values: &[Value]) -> Result<Option<GeoBox>, CatalogError> {
    if values.is_empty() {
        return Ok(None);
    }
    let boxes = values.iter().map(expect_box).collect::<Result<Vec<_>, _>>()?;
    let mut low_xs = boxes.iter().map(|geo_box| geo_box.low.x).collect::<Vec<_>>();
    let mut high_xs = boxes.iter().map(|geo_box| geo_box.high.x).collect::<Vec<_>>();
    let mut low_ys = boxes.iter().map(|geo_box| geo_box.low.y).collect::<Vec<_>>();
    let mut high_ys = boxes.iter().map(|geo_box| geo_box.high.y).collect::<Vec<_>>();
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

pub(crate) fn leaf_consistent(
    strategy: u16,
    key: &Value,
    query: &Value,
) -> Result<bool, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(false);
    }
    let key = expect_box(key)?;
    let query = expect_box(query)?;
    Ok(match strategy {
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
        _ => return Err(CatalogError::Corrupt("unsupported SP-GiST box strategy")),
    })
}

pub(crate) fn order_distance(key: &Value, query: &Value) -> Result<Option<f64>, CatalogError> {
    if matches!(key, Value::Null) || matches!(query, Value::Null) {
        return Ok(None);
    }
    let key_box = expect_box(key)?;
    Ok(Some(match query {
        Value::Point(point) => point_box_distance(point, key_box),
        Value::Box(query_box) => box_box_distance(key_box, query_box),
        other => {
            return Err(CatalogError::Io(format!(
                "SP-GiST box ORDER BY expected point or box value, got {other:?}"
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

pub(crate) fn choose(proc_oid: u32, centroid: &Value, leaf: &Value) -> Result<u8, CatalogError> {
    let _ = proc_oid;
    Ok(quadrant(expect_box(centroid)?, expect_box(leaf)?))
}

pub(crate) fn picksplit(
    proc_oid: u32,
    values: &[Value],
) -> Result<Option<(GeoBox, Vec<u8>)>, CatalogError> {
    let _ = proc_oid;
    let Some(centroid) = median_centroid(values)? else {
        return Ok(None);
    };
    let assignments = values
        .iter()
        .map(|value| Ok(quadrant(&centroid, expect_box(value)?)))
        .collect::<Result<Vec<_>, CatalogError>>()?;
    Ok(Some((centroid, assignments)))
}
