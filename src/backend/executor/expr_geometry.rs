use std::f64::consts::PI;

use super::ExecError;
use super::expr_casts::parse_pg_float;
use super::node_types::{
    BuiltinScalarFunction, GeoBox, GeoCircle, GeoLine, GeoLseg, GeoPath, GeoPoint, GeoPolygon,
    SqlType, Value,
};
use crate::backend::libpq::pqformat::{FloatFormatOptions, format_float8_text};
use crate::backend::parser::SqlTypeKind;

pub(crate) const GEOMETRY_EPSILON: f64 = 1.0e-6;
const DEFAULT_CIRCLE_POLYGON_POINTS: i32 = 12;

#[derive(Clone, Copy)]
struct Bounds {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
}

pub(crate) fn parse_geometry_text(text: &str, ty: SqlTypeKind) -> Result<Value, ExecError> {
    match ty {
        SqlTypeKind::Point => parse_point_text(text).map(Value::Point),
        SqlTypeKind::Lseg => parse_lseg_text(text).map(Value::Lseg),
        SqlTypeKind::Path => parse_path_text(text).map(Value::Path),
        SqlTypeKind::Line => parse_line_text(text).map(Value::Line),
        SqlTypeKind::Box => parse_box_text(text).map(Value::Box),
        SqlTypeKind::Polygon => parse_polygon_text(text).map(Value::Polygon),
        SqlTypeKind::Circle => parse_circle_text(text).map(Value::Circle),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Text(text.into()),
            right: Value::Null,
        }),
    }
}

pub(crate) fn render_geometry_text(value: &Value, options: FloatFormatOptions) -> Option<String> {
    match value {
        Value::Point(point) => Some(render_point(point, options)),
        Value::Lseg(lseg) => Some(render_lseg(lseg, options)),
        Value::Path(path) => Some(render_path(path, options)),
        Value::Line(line) => Some(render_line(line, options)),
        Value::Box(geo_box) => Some(render_box(geo_box, options)),
        Value::Polygon(poly) => Some(render_polygon(poly, options)),
        Value::Circle(circle) => Some(render_circle(circle, options)),
        _ => None,
    }
}

pub(crate) fn cast_geometry_value(value: Value, ty: SqlType) -> Option<Result<Value, ExecError>> {
    if ty.is_array {
        return None;
    }
    let result = match value {
        Value::Point(point) => cast_from_point(point, ty.kind),
        Value::Lseg(lseg) => cast_from_lseg(lseg, ty.kind),
        Value::Path(path) => cast_from_path(path, ty.kind),
        Value::Line(line) => cast_from_line(line, ty.kind),
        Value::Box(geo_box) => cast_from_box(geo_box, ty.kind),
        Value::Polygon(poly) => cast_from_polygon(poly, ty.kind),
        Value::Circle(circle) => cast_from_circle(circle, ty.kind),
        _ => return None,
    };
    Some(result)
}

pub(crate) fn encode_path_bytes(path: &GeoPath) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + path.points.len() * 16);
    bytes.push(u8::from(path.closed));
    bytes.extend_from_slice(&(path.points.len() as u32).to_le_bytes());
    for point in &path.points {
        bytes.extend_from_slice(&point.x.to_le_bytes());
        bytes.extend_from_slice(&point.y.to_le_bytes());
    }
    bytes
}

pub(crate) fn decode_path_bytes(bytes: &[u8]) -> Result<GeoPath, ExecError> {
    if bytes.len() < 5 {
        return Err(ExecError::InvalidStorageValue {
            column: "<path>".into(),
            details: "path payload too short".into(),
        });
    }
    let closed = bytes[0] != 0;
    let count = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
    if bytes.len() != 5 + count * 16 {
        return Err(ExecError::InvalidStorageValue {
            column: "<path>".into(),
            details: "path payload has wrong length".into(),
        });
    }
    let mut points = Vec::with_capacity(count);
    let mut offset = 5usize;
    for _ in 0..count {
        let x = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let y = f64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        points.push(GeoPoint { x, y });
        offset += 16;
    }
    Ok(GeoPath { closed, points })
}

pub(crate) fn encode_polygon_bytes(poly: &GeoPolygon) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(36 + poly.points.len() * 16);
    bytes.extend_from_slice(&(poly.points.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&poly.bound_box.high.x.to_le_bytes());
    bytes.extend_from_slice(&poly.bound_box.high.y.to_le_bytes());
    bytes.extend_from_slice(&poly.bound_box.low.x.to_le_bytes());
    bytes.extend_from_slice(&poly.bound_box.low.y.to_le_bytes());
    for point in &poly.points {
        bytes.extend_from_slice(&point.x.to_le_bytes());
        bytes.extend_from_slice(&point.y.to_le_bytes());
    }
    bytes
}

pub(crate) fn decode_polygon_bytes(bytes: &[u8]) -> Result<GeoPolygon, ExecError> {
    if bytes.len() < 36 {
        return Err(ExecError::InvalidStorageValue {
            column: "<polygon>".into(),
            details: "polygon payload too short".into(),
        });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() != 36 + count * 16 {
        return Err(ExecError::InvalidStorageValue {
            column: "<polygon>".into(),
            details: "polygon payload has wrong length".into(),
        });
    }
    let bound_box = GeoBox {
        high: GeoPoint {
            x: f64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            y: f64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        },
        low: GeoPoint {
            x: f64::from_le_bytes(bytes[20..28].try_into().unwrap()),
            y: f64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        },
    };
    let mut points = Vec::with_capacity(count);
    let mut offset = 36usize;
    for _ in 0..count {
        let x = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let y = f64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        points.push(GeoPoint { x, y });
        offset += 16;
    }
    Ok(GeoPolygon { bound_box, points })
}

pub(crate) fn eval_geometry_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    let result = match func {
        BuiltinScalarFunction::GeoPoint => eval_geo_point(values),
        BuiltinScalarFunction::GeoBox => eval_geo_box(values),
        BuiltinScalarFunction::GeoLine => eval_geo_line(values),
        BuiltinScalarFunction::GeoLseg => eval_geo_lseg(values),
        BuiltinScalarFunction::GeoPath => eval_geo_path(values),
        BuiltinScalarFunction::GeoPolygon => eval_geo_polygon(values),
        BuiltinScalarFunction::GeoCircle => eval_geo_circle(values),
        BuiltinScalarFunction::GeoArea => eval_geo_area(values),
        BuiltinScalarFunction::GeoCenter | BuiltinScalarFunction::GeoPolyCenter => {
            eval_geo_center(values)
        }
        BuiltinScalarFunction::GeoBoundBox => eval_geo_bound_box(values),
        BuiltinScalarFunction::GeoDiagonal => eval_geo_diagonal(values),
        BuiltinScalarFunction::GeoLength => eval_geo_length(values),
        BuiltinScalarFunction::GeoRadius => eval_geo_radius(values),
        BuiltinScalarFunction::GeoDiameter => eval_geo_diameter(values),
        BuiltinScalarFunction::GeoNpoints => eval_geo_npoints(values),
        BuiltinScalarFunction::GeoPclose => eval_geo_pclose(values),
        BuiltinScalarFunction::GeoPopen => eval_geo_popen(values),
        BuiltinScalarFunction::GeoIsOpen => eval_geo_is_open(values),
        BuiltinScalarFunction::GeoIsClosed => eval_geo_is_closed(values),
        BuiltinScalarFunction::GeoSlope => eval_geo_slope(values),
        BuiltinScalarFunction::GeoIsVertical => eval_geo_is_vertical(values),
        BuiltinScalarFunction::GeoIsHorizontal => eval_geo_is_horizontal(values),
        BuiltinScalarFunction::GeoHeight => eval_geo_height(values),
        BuiltinScalarFunction::GeoWidth => eval_geo_width(values),
        BuiltinScalarFunction::GeoEq => eval_geo_eq(values),
        BuiltinScalarFunction::GeoNe => eval_geo_ne(values),
        BuiltinScalarFunction::GeoLt => eval_geo_lt(values),
        BuiltinScalarFunction::GeoLe => eval_geo_le(values),
        BuiltinScalarFunction::GeoGt => eval_geo_gt(values),
        BuiltinScalarFunction::GeoGe => eval_geo_ge(values),
        BuiltinScalarFunction::GeoSame => eval_geo_same(values),
        BuiltinScalarFunction::GeoDistance => eval_geo_distance(values),
        BuiltinScalarFunction::GeoClosestPoint => eval_geo_closest_point(values),
        BuiltinScalarFunction::GeoIntersection => eval_geo_intersection(values),
        BuiltinScalarFunction::GeoIntersects => eval_geo_intersects(values),
        BuiltinScalarFunction::GeoParallel => eval_geo_parallel(values),
        BuiltinScalarFunction::GeoPerpendicular => eval_geo_perpendicular(values),
        BuiltinScalarFunction::GeoContains => eval_geo_contains(values),
        BuiltinScalarFunction::GeoContainedBy => eval_geo_contained_by(values),
        BuiltinScalarFunction::GeoOverlap => eval_geo_overlap(values),
        BuiltinScalarFunction::GeoLeft => eval_geo_left(values),
        BuiltinScalarFunction::GeoOverLeft => eval_geo_over_left(values),
        BuiltinScalarFunction::GeoRight => eval_geo_right(values),
        BuiltinScalarFunction::GeoOverRight => eval_geo_over_right(values),
        BuiltinScalarFunction::GeoBelow => eval_geo_below(values),
        BuiltinScalarFunction::GeoOverBelow => eval_geo_over_below(values),
        BuiltinScalarFunction::GeoAbove => eval_geo_above(values),
        BuiltinScalarFunction::GeoOverAbove => eval_geo_over_above(values),
        BuiltinScalarFunction::GeoAdd => eval_geo_add(values),
        BuiltinScalarFunction::GeoSub => eval_geo_sub(values),
        BuiltinScalarFunction::GeoMul => eval_geo_mul(values),
        BuiltinScalarFunction::GeoDiv => eval_geo_div(values),
        BuiltinScalarFunction::GeoBoxHigh => eval_geo_box_point(values, 0),
        BuiltinScalarFunction::GeoBoxLow => eval_geo_box_point(values, 1),
        BuiltinScalarFunction::GeoPointX => eval_geo_point_coord(values, 0),
        BuiltinScalarFunction::GeoPointY => eval_geo_point_coord(values, 1),
        _ => return None,
    };
    Some(result)
}

fn parse_point_text(text: &str) -> Result<GeoPoint, ExecError> {
    let mut parser = GeometryParser::new(text, "point");
    let point = parser.parse_point_pair()?;
    parser.finish()?;
    Ok(point)
}

fn parse_lseg_text(text: &str) -> Result<GeoLseg, ExecError> {
    let (_closed, points) = parse_path_like(text, "lseg")?;
    if points.len() != 2 {
        return Err(invalid_geometry_input("lseg", text));
    }
    Ok(GeoLseg {
        p: [points[0].clone(), points[1].clone()],
    })
}

fn parse_box_text(text: &str) -> Result<GeoBox, ExecError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(invalid_geometry_input("box", text));
    }
    if trimmed.contains('(') && trimmed.matches('(').count() >= 2 {
        let mut parser = GeometryParser::new(text, "box");
        let mut outer = false;
        parser.skip_ws();
        if parser.consume('(') {
            parser.skip_ws();
            if parser.peek() == Some('(') {
                outer = true;
            } else {
                parser.idx = 0;
            }
        }
        if !outer {
            parser.idx = 0;
        }
        let first = parser.parse_point_pair()?;
        parser.consume(',');
        let second = parser.parse_point_pair()?;
        if outer {
            parser.expect(')')?;
        }
        parser.finish()?;
        return Ok(canonical_box(first, second));
    }
    let mut parser = GeometryParser::new(text, "box");
    let mut outer = false;
    parser.skip_ws();
    if parser.consume('(') {
        outer = true;
    }
    let x1 = parser.parse_number()?;
    parser.expect(',')?;
    let y1 = parser.parse_number()?;
    parser.expect(',')?;
    let x2 = parser.parse_number()?;
    parser.expect(',')?;
    let y2 = parser.parse_number()?;
    if outer {
        parser.expect(')')?;
    }
    parser.finish()?;
    Ok(canonical_box(
        GeoPoint { x: x1, y: y1 },
        GeoPoint { x: x2, y: y2 },
    ))
}

fn parse_line_text(text: &str) -> Result<GeoLine, ExecError> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        let mut parser = GeometryParser::new(text, "line");
        parser.expect('{')?;
        let a = parser.parse_number()?;
        parser.expect(',')?;
        let b = parser.parse_number()?;
        parser.expect(',')?;
        let c = parser.parse_number()?;
        parser.expect('}')?;
        parser.finish()?;
        if fp_zero(a) && fp_zero(b) {
            return Err(invalid_geometry_input("line", text));
        }
        return Ok(GeoLine { a, b, c });
    }
    if trimmed.contains('(') {
        let mut parser = GeometryParser::new(text, "line");
        let wrapped = parser.consume('[');
        let first = parser.parse_point_pair()?;
        parser.expect(',')?;
        let second = parser.parse_point_pair()?;
        if wrapped {
            parser.expect(']')?;
        }
        parser.finish()?;
        if point_same(&first, &second) {
            return Err(invalid_geometry_input("line", text));
        }
        return line_from_points(first, second).map_err(|_| invalid_geometry_input("line", text));
    }
    let mut parser = GeometryParser::new(text, "line");
    let mut wrapped = false;
    parser.skip_ws();
    if parser.consume('[') {
        wrapped = true;
    }
    let x1 = parser.parse_number()?;
    parser.expect(',')?;
    let y1 = parser.parse_number()?;
    parser.expect(',')?;
    let x2 = parser.parse_number()?;
    parser.expect(',')?;
    let y2 = parser.parse_number()?;
    if wrapped {
        parser.expect(']')?;
    }
    parser.finish()?;
    line_from_points(GeoPoint { x: x1, y: y1 }, GeoPoint { x: x2, y: y2 })
        .map_err(|_| invalid_geometry_input("line", text))
}

fn parse_path_text(text: &str) -> Result<GeoPath, ExecError> {
    parse_path_like(text, "path").map(|(closed, points)| GeoPath { closed, points })
}

fn parse_polygon_text(text: &str) -> Result<GeoPolygon, ExecError> {
    let points = match parse_path_like(text, "polygon") {
        Ok((_closed, points)) => points,
        Err(_) => parse_point_sequence(text, "polygon")?,
    };
    if points.is_empty() {
        return Err(invalid_geometry_input("polygon", text));
    }
    Ok(make_polygon(points))
}

fn parse_circle_text(text: &str) -> Result<GeoCircle, ExecError> {
    let mut parser = GeometryParser::new(text, "circle");
    parser.skip_ws();
    let style = parser.peek();
    if style == Some('<') {
        parser.expect('<')?;
        let center = parser.parse_point_pair()?;
        parser.expect(',')?;
        let radius = parser.parse_number()?;
        parser.expect('>')?;
        parser.finish()?;
        return make_circle(center, radius).map_err(|_| invalid_geometry_input("circle", text));
    }
    if style == Some('(') {
        parser.expect('(')?;
        parser.skip_ws();
        if parser.peek() == Some('(') {
            let center = parser.parse_point_pair()?;
            parser.expect(',')?;
            let radius = parser.parse_number()?;
            parser.expect(')')?;
            parser.finish()?;
            return make_circle(center, radius).map_err(|_| invalid_geometry_input("circle", text));
        }
    }
    let mut parser = GeometryParser::new(text, "circle");
    let center = parser.parse_point_pair()?;
    parser.expect(',')?;
    let radius = parser.parse_number()?;
    parser.finish()?;
    make_circle(center, radius).map_err(|_| invalid_geometry_input("circle", text))
}

fn parse_path_like(text: &str, ty: &'static str) -> Result<(bool, Vec<GeoPoint>), ExecError> {
    let mut parser = GeometryParser::new(text, ty);
    let mut closed = true;
    let mut wrapped = false;
    parser.skip_ws();
    if parser.consume('[') {
        wrapped = true;
        closed = false;
    } else if parser.leading_paren_is_path_wrapper() {
        wrapped = true;
        closed = true;
    }
    parser.skip_ws();
    let mut points = Vec::new();
    if wrapped && parser.peek().is_some_and(|ch| ch == ']' || ch == ')') {
        return Err(invalid_geometry_input(ty, text));
    }
    if parser.peek() == Some('(') {
        loop {
            points.push(parser.parse_point_pair()?);
            parser.skip_ws();
            if !wrapped && parser.peek().is_none() {
                break;
            }
            if wrapped {
                let end = if closed { ')' } else { ']' };
                if parser.consume(end) {
                    break;
                }
            }
            parser.expect(',')?;
            parser.skip_ws();
        }
    } else {
        let mut numbers = Vec::new();
        loop {
            numbers.push(parser.parse_number()?);
            parser.skip_ws();
            if !wrapped && parser.peek().is_none() {
                break;
            }
            if wrapped {
                let end = if closed { ')' } else { ']' };
                if parser.consume(end) {
                    break;
                }
            }
            parser.expect(',')?;
        }
        if numbers.len() < 2 || numbers.len() % 2 != 0 {
            return Err(invalid_geometry_input(ty, text));
        }
        for chunk in numbers.chunks_exact(2) {
            points.push(GeoPoint {
                x: chunk[0],
                y: chunk[1],
            });
        }
    }
    parser.finish()?;
    Ok((closed, points))
}

fn parse_point_sequence(text: &str, ty: &'static str) -> Result<Vec<GeoPoint>, ExecError> {
    let mut parser = GeometryParser::new(text, ty);
    let mut points = Vec::new();
    loop {
        points.push(parser.parse_point_pair()?);
        parser.skip_ws();
        if parser.peek().is_none() {
            break;
        }
        parser.expect(',')?;
    }
    parser.finish()?;
    Ok(points)
}

fn render_number(value: f64, options: FloatFormatOptions) -> String {
    format_float8_text(value, options)
}

fn render_point(point: &GeoPoint, options: FloatFormatOptions) -> String {
    format!(
        "({},{})",
        render_number(point.x, options.clone()),
        render_number(point.y, options)
    )
}

fn render_lseg(lseg: &GeoLseg, options: FloatFormatOptions) -> String {
    format!(
        "[{},{}]",
        render_point(&lseg.p[0], options.clone()),
        render_point(&lseg.p[1], options)
    )
}

fn render_path(path: &GeoPath, options: FloatFormatOptions) -> String {
    let mut out = String::new();
    out.push(if path.closed { '(' } else { '[' });
    for (idx, point) in path.points.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&render_point(point, options.clone()));
    }
    out.push(if path.closed { ')' } else { ']' });
    out
}

fn render_line(line: &GeoLine, options: FloatFormatOptions) -> String {
    format!(
        "{{{},{},{}}}",
        render_number(line.a, options.clone()),
        render_number(line.b, options.clone()),
        render_number(line.c, options)
    )
}

fn render_box(geo_box: &GeoBox, options: FloatFormatOptions) -> String {
    format!(
        "{},{}",
        render_point(&geo_box.high, options.clone()),
        render_point(&geo_box.low, options)
    )
}

fn render_polygon(poly: &GeoPolygon, options: FloatFormatOptions) -> String {
    let mut out = String::from("(");
    for (idx, point) in poly.points.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(&render_point(point, options.clone()));
    }
    out.push(')');
    out
}

fn render_circle(circle: &GeoCircle, options: FloatFormatOptions) -> String {
    format!(
        "<{},{}>",
        render_point(&circle.center, options.clone()),
        render_number(circle.radius, options)
    )
}

fn cast_from_point(point: GeoPoint, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Point => Ok(Value::Point(point)),
        SqlTypeKind::Box => Ok(Value::Box(point_to_box(&point))),
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_point(&point, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Point(point),
            right: Value::Null,
        }),
    }
}

fn cast_from_lseg(lseg: GeoLseg, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Lseg => Ok(Value::Lseg(lseg)),
        SqlTypeKind::Point => Ok(Value::Point(lseg_center(&lseg))),
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_lseg(&lseg, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Lseg(lseg),
            right: Value::Null,
        }),
    }
}

fn cast_from_path(path: GeoPath, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Path => Ok(Value::Path(path)),
        SqlTypeKind::Polygon => path_to_polygon(&path).map(Value::Polygon),
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_path(&path, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Path(path),
            right: Value::Null,
        }),
    }
}

fn cast_from_line(line: GeoLine, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Line => Ok(Value::Line(line)),
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_line(&line, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Line(line),
            right: Value::Null,
        }),
    }
}

fn cast_from_box(geo_box: GeoBox, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Box => Ok(Value::Box(geo_box)),
        SqlTypeKind::Point => Ok(Value::Point(box_center(&geo_box))),
        SqlTypeKind::Circle => Ok(Value::Circle(box_to_circle(&geo_box))),
        SqlTypeKind::Polygon => Ok(Value::Polygon(box_to_polygon(&geo_box))),
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_box(&geo_box, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Box(geo_box),
            right: Value::Null,
        }),
    }
}

fn cast_from_polygon(poly: GeoPolygon, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Polygon => Ok(Value::Polygon(poly)),
        SqlTypeKind::Point => Ok(Value::Point(poly_center(&poly))),
        SqlTypeKind::Box => Ok(Value::Box(poly.bound_box.clone())),
        SqlTypeKind::Path => Ok(Value::Path(GeoPath {
            closed: true,
            points: poly.points.clone(),
        })),
        SqlTypeKind::Circle => Ok(Value::Circle(polygon_to_circle(&poly))),
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_polygon(&poly, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Polygon(poly),
            right: Value::Null,
        }),
    }
}

fn cast_from_circle(circle: GeoCircle, target: SqlTypeKind) -> Result<Value, ExecError> {
    match target {
        SqlTypeKind::Circle => Ok(Value::Circle(circle)),
        SqlTypeKind::Point => Ok(Value::Point(circle.center.clone())),
        SqlTypeKind::Box => Ok(Value::Box(circle_to_box(&circle))),
        SqlTypeKind::Polygon => {
            circle_to_polygon(DEFAULT_CIRCLE_POLYGON_POINTS, &circle).map(Value::Polygon)
        }
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
            Value::Text(render_circle(&circle, FloatFormatOptions::default()).into()),
        ),
        _ => Err(ExecError::TypeMismatch {
            op: "::geometry",
            left: Value::Circle(circle),
            right: Value::Null,
        }),
    }
}

fn eval_geo_point(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Float64(x), Value::Float64(y)] => Ok(Value::Point(GeoPoint { x: *x, y: *y })),
        [Value::Int16(x), Value::Int16(y)] => Ok(Value::Point(GeoPoint {
            x: *x as f64,
            y: *y as f64,
        })),
        [Value::Int32(x), Value::Int32(y)] => Ok(Value::Point(GeoPoint {
            x: *x as f64,
            y: *y as f64,
        })),
        [Value::Int64(x), Value::Int64(y)] => Ok(Value::Point(GeoPoint {
            x: *x as f64,
            y: *y as f64,
        })),
        [Value::Numeric(x), Value::Numeric(y)] => Ok(Value::Point(GeoPoint {
            x: x.render().parse().unwrap_or(0.0),
            y: y.render().parse().unwrap_or(0.0),
        })),
        [Value::Point(point)] => Ok(Value::Point(point.clone())),
        [Value::Box(geo_box)] => Ok(Value::Point(box_center(geo_box))),
        [Value::Circle(circle)] => Ok(Value::Point(circle.center.clone())),
        [Value::Polygon(poly)] => Ok(Value::Point(poly_center(poly))),
        [Value::Lseg(lseg)] => Ok(Value::Point(lseg_center(lseg))),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "point",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => Err(ExecError::TypeMismatch {
            op: "point",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_box(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Point(left), Value::Point(right)] => {
            Ok(Value::Box(canonical_box(left.clone(), right.clone())))
        }
        [Value::Point(point)] => Ok(Value::Box(point_to_box(point))),
        [Value::Polygon(poly)] => Ok(Value::Box(poly.bound_box.clone())),
        [Value::Circle(circle)] => Ok(Value::Box(circle_to_box(circle))),
        [Value::Box(geo_box)] => Ok(Value::Box(geo_box.clone())),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "box",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => Err(ExecError::TypeMismatch {
            op: "box",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_line(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Point(left), Value::Point(right)] => {
            Ok(Value::Line(line_from_points(left.clone(), right.clone())?))
        }
        [Value::Line(line)] => Ok(Value::Line(line.clone())),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "line",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => Err(ExecError::TypeMismatch {
            op: "line",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_lseg(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Point(left), Value::Point(right)] => Ok(Value::Lseg(GeoLseg {
            p: [left.clone(), right.clone()],
        })),
        [Value::Lseg(lseg)] => Ok(Value::Lseg(lseg.clone())),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "lseg",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => Err(ExecError::TypeMismatch {
            op: "lseg",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_path(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Path(path)] => Ok(Value::Path(path.clone())),
        [Value::Polygon(poly)] => Ok(Value::Path(GeoPath {
            closed: true,
            points: poly.points.clone(),
        })),
        [other] => Err(ExecError::TypeMismatch {
            op: "path",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_polygon(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Box(geo_box)] => Ok(Value::Polygon(box_to_polygon(geo_box))),
        [Value::Path(path)] => path_to_polygon(path).map(Value::Polygon),
        [Value::Circle(circle)] => {
            circle_to_polygon(DEFAULT_CIRCLE_POLYGON_POINTS, circle).map(Value::Polygon)
        }
        [Value::Polygon(poly)] => Ok(Value::Polygon(poly.clone())),
        [Value::Int32(count), Value::Circle(circle)] => {
            circle_to_polygon(*count, circle).map(Value::Polygon)
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "polygon",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => Err(ExecError::TypeMismatch {
            op: "polygon",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_circle(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Point(center), Value::Float64(radius)] => {
            Ok(Value::Circle(make_circle(center.clone(), *radius)?))
        }
        [Value::Point(center), Value::Int16(radius)] => {
            Ok(Value::Circle(make_circle(center.clone(), *radius as f64)?))
        }
        [Value::Point(center), Value::Int32(radius)] => {
            Ok(Value::Circle(make_circle(center.clone(), *radius as f64)?))
        }
        [Value::Point(center), Value::Int64(radius)] => {
            Ok(Value::Circle(make_circle(center.clone(), *radius as f64)?))
        }
        [Value::Point(center), Value::Numeric(radius)] => {
            let radius = radius
                .render()
                .parse::<f64>()
                .map_err(|_| ExecError::TypeMismatch {
                    op: "circle",
                    left: Value::Point(center.clone()),
                    right: Value::Numeric(radius.clone()),
                })?;
            Ok(Value::Circle(make_circle(center.clone(), radius)?))
        }
        [Value::Box(geo_box)] => Ok(Value::Circle(box_to_circle(geo_box))),
        [Value::Polygon(poly)] => Ok(Value::Circle(polygon_to_circle(poly))),
        [Value::Circle(circle)] => Ok(Value::Circle(circle.clone())),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "circle",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => Err(ExecError::TypeMismatch {
            op: "circle",
            left: other.clone(),
            right: Value::Null,
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_area(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "area", |value| match value {
        Value::Box(geo_box) => Ok(Value::Float64(box_area(geo_box))),
        Value::Path(path) => Ok(if path.closed {
            Value::Float64(polygon_area_points(&path.points))
        } else {
            Value::Null
        }),
        Value::Polygon(poly) => Ok(Value::Float64(polygon_area(poly))),
        Value::Circle(circle) => Ok(Value::Float64(circle_area(circle))),
        other => type_mismatch_unary("area", other),
    })
}

fn eval_geo_center(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "center", |value| match value {
        Value::Box(geo_box) => Ok(Value::Point(box_center(geo_box))),
        Value::Circle(circle) => Ok(Value::Point(circle.center.clone())),
        Value::Polygon(poly) => Ok(Value::Point(poly_center(poly))),
        Value::Lseg(lseg) => Ok(Value::Point(lseg_center(lseg))),
        other => type_mismatch_unary("center", other),
    })
}

fn eval_geo_bound_box(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Box(left), Value::Box(right)] => Ok(Value::Box(bound_box(left, right))),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "bound_box",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_diagonal(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "diagonal", |value| match value {
        Value::Box(geo_box) => Ok(Value::Lseg(GeoLseg {
            p: [geo_box.high.clone(), geo_box.low.clone()],
        })),
        other => type_mismatch_unary("diagonal", other),
    })
}

fn eval_geo_length(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "@-@", |value| match value {
        Value::Lseg(lseg) => Ok(Value::Float64(lseg_length(lseg))),
        Value::Path(path) => Ok(Value::Float64(
            path_segments(path).iter().map(lseg_length).sum(),
        )),
        other => type_mismatch_unary("@-@", other),
    })
}

fn eval_geo_radius(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "radius", |value| match value {
        Value::Circle(circle) => Ok(Value::Float64(circle.radius)),
        other => type_mismatch_unary("radius", other),
    })
}

fn eval_geo_diameter(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "diameter", |value| match value {
        Value::Circle(circle) => Ok(Value::Float64(circle.radius * 2.0)),
        other => type_mismatch_unary("diameter", other),
    })
}

fn eval_geo_npoints(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "npoints", |value| match value {
        Value::Path(path) => Ok(Value::Int32(path.points.len() as i32)),
        Value::Polygon(poly) => Ok(Value::Int32(poly.points.len() as i32)),
        other => type_mismatch_unary("npoints", other),
    })
}

fn eval_geo_pclose(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "pclose", |value| match value {
        Value::Path(path) => Ok(Value::Path(GeoPath {
            closed: true,
            points: path.points.clone(),
        })),
        other => type_mismatch_unary("pclose", other),
    })
}

fn eval_geo_popen(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "popen", |value| match value {
        Value::Path(path) => Ok(Value::Path(GeoPath {
            closed: false,
            points: path.points.clone(),
        })),
        other => type_mismatch_unary("popen", other),
    })
}

fn eval_geo_is_open(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "isopen", |value| match value {
        Value::Path(path) => Ok(Value::Bool(!path.closed)),
        other => type_mismatch_unary("isopen", other),
    })
}

fn eval_geo_is_closed(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "isclosed", |value| match value {
        Value::Path(path) => Ok(Value::Bool(path.closed)),
        other => type_mismatch_unary("isclosed", other),
    })
}

fn eval_geo_slope(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Point(left), Value::Point(right)] => Ok(Value::Float64(point_slope(left, right))),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "slope",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_is_vertical(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Line(line)] => Ok(Value::Bool(fp_zero(line.b))),
        [Value::Lseg(lseg)] => Ok(Value::Bool(point_same_x(&lseg.p[0], &lseg.p[1]))),
        [Value::Point(left), Value::Point(right)] => Ok(Value::Bool(point_same_x(left, right))),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "?|",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => type_mismatch_unary("?|", other),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_is_horizontal(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(is_null) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Line(line)] => Ok(Value::Bool(fp_zero(line.a))),
        [Value::Lseg(lseg)] => Ok(Value::Bool(point_same_y(&lseg.p[0], &lseg.p[1]))),
        [Value::Point(left), Value::Point(right)] => Ok(Value::Bool(point_same_y(left, right))),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "?-",
            left: left.clone(),
            right: right.clone(),
        }),
        [other] => type_mismatch_unary("?-", other),
        _ => Ok(Value::Null),
    }
}

fn eval_geo_height(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "height", |value| match value {
        Value::Box(geo_box) => Ok(Value::Float64(geo_box.high.y - geo_box.low.y)),
        other => type_mismatch_unary("height", other),
    })
}

fn eval_geo_width(values: &[Value]) -> Result<Value, ExecError> {
    unary_geometry(values, "width", |value| match value {
        Value::Box(geo_box) => Ok(Value::Float64(geo_box.high.x - geo_box.low.x)),
        other => type_mismatch_unary("width", other),
    })
}

fn eval_geo_eq(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "=", |left, right| match (left, right) {
        (Value::Line(left), Value::Line(right)) => Ok(Value::Bool(line_same(left, right))),
        (Value::Box(left), Value::Box(right)) => {
            Ok(Value::Bool(fp_eq(box_area(left), box_area(right))))
        }
        (Value::Lseg(left), Value::Lseg(right)) => Ok(Value::Bool(lseg_eq(left, right))),
        (Value::Path(left), Value::Path(right)) => {
            Ok(Value::Bool(left.points.len() == right.points.len()))
        }
        (Value::Circle(left), Value::Circle(right)) => {
            Ok(Value::Bool(fp_eq(circle_area(left), circle_area(right))))
        }
        (Value::Point(left), Value::Point(right)) => Ok(Value::Bool(point_same(left, right))),
        _ => Err(ExecError::TypeMismatch {
            op: "=",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_ne(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "<>", |left, right| match (left, right) {
        (Value::Line(left), Value::Line(right)) => Ok(Value::Bool(!line_same(left, right))),
        (Value::Box(left), Value::Box(right)) => {
            Ok(Value::Bool(fp_ne(box_area(left), box_area(right))))
        }
        (Value::Lseg(left), Value::Lseg(right)) => Ok(Value::Bool(
            !point_same(&left.p[0], &right.p[0]) || !point_same(&left.p[1], &right.p[1]),
        )),
        (Value::Path(left), Value::Path(right)) => {
            Ok(Value::Bool(left.points.len() != right.points.len()))
        }
        (Value::Circle(left), Value::Circle(right)) => {
            Ok(Value::Bool(fp_ne(circle_area(left), circle_area(right))))
        }
        (Value::Point(left), Value::Point(right)) => Ok(Value::Bool(!point_same(left, right))),
        _ => Err(ExecError::TypeMismatch {
            op: "<>",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_lt(values: &[Value]) -> Result<Value, ExecError> {
    ordered_binary(values, "<", |left, right| match (left, right) {
        (Value::Box(left), Value::Box(right)) => Ok(fp_lt(box_area(left), box_area(right))),
        (Value::Circle(left), Value::Circle(right)) => {
            Ok(fp_lt(circle_area(left), circle_area(right)))
        }
        (Value::Lseg(left), Value::Lseg(right)) => Ok(fp_lt(lseg_length(left), lseg_length(right))),
        (Value::Path(left), Value::Path(right)) => Ok(left.points.len() < right.points.len()),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "<",
                left: left.clone(),
                right: right.clone(),
            });
        }
    })
}

fn eval_geo_le(values: &[Value]) -> Result<Value, ExecError> {
    ordered_binary(values, "<=", |left, right| match (left, right) {
        (Value::Box(left), Value::Box(right)) => Ok(fp_le(box_area(left), box_area(right))),
        (Value::Circle(left), Value::Circle(right)) => {
            Ok(fp_le(circle_area(left), circle_area(right)))
        }
        (Value::Lseg(left), Value::Lseg(right)) => Ok(fp_le(lseg_length(left), lseg_length(right))),
        (Value::Path(left), Value::Path(right)) => Ok(left.points.len() <= right.points.len()),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "<=",
                left: left.clone(),
                right: right.clone(),
            });
        }
    })
}

fn eval_geo_gt(values: &[Value]) -> Result<Value, ExecError> {
    ordered_binary(values, ">", |left, right| match (left, right) {
        (Value::Box(left), Value::Box(right)) => Ok(fp_gt(box_area(left), box_area(right))),
        (Value::Circle(left), Value::Circle(right)) => {
            Ok(fp_gt(circle_area(left), circle_area(right)))
        }
        (Value::Lseg(left), Value::Lseg(right)) => Ok(fp_gt(lseg_length(left), lseg_length(right))),
        (Value::Path(left), Value::Path(right)) => Ok(left.points.len() > right.points.len()),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: ">",
                left: left.clone(),
                right: right.clone(),
            });
        }
    })
}

fn eval_geo_ge(values: &[Value]) -> Result<Value, ExecError> {
    ordered_binary(values, ">=", |left, right| match (left, right) {
        (Value::Box(left), Value::Box(right)) => Ok(fp_ge(box_area(left), box_area(right))),
        (Value::Circle(left), Value::Circle(right)) => {
            Ok(fp_ge(circle_area(left), circle_area(right)))
        }
        (Value::Lseg(left), Value::Lseg(right)) => Ok(fp_ge(lseg_length(left), lseg_length(right))),
        (Value::Path(left), Value::Path(right)) => Ok(left.points.len() >= right.points.len()),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: ">=",
                left: left.clone(),
                right: right.clone(),
            });
        }
    })
}

fn eval_geo_same(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "~=", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => Ok(Value::Bool(point_same(left, right))),
        (Value::Box(left), Value::Box(right)) => Ok(Value::Bool(box_same(left, right))),
        (Value::Polygon(left), Value::Polygon(right)) => Ok(Value::Bool(polygon_same(left, right))),
        (Value::Circle(left), Value::Circle(right)) => Ok(Value::Bool(circle_same(left, right))),
        _ => Err(ExecError::TypeMismatch {
            op: "~=",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_distance(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "<->", |left, right| {
        let distance = match (left, right) {
            (Value::Point(left), Value::Point(right)) => point_distance(left, right),
            (Value::Point(point), Value::Line(line)) | (Value::Line(line), Value::Point(point)) => {
                point_line_distance(point, line)
            }
            (Value::Point(point), Value::Lseg(lseg)) | (Value::Lseg(lseg), Value::Point(point)) => {
                point_lseg_distance(point, lseg)
            }
            (Value::Point(point), Value::Box(geo_box))
            | (Value::Box(geo_box), Value::Point(point)) => point_box_distance(point, geo_box),
            (Value::Point(point), Value::Path(path)) | (Value::Path(path), Value::Point(point)) => {
                point_path_distance(point, path)
            }
            (Value::Point(point), Value::Polygon(poly))
            | (Value::Polygon(poly), Value::Point(point)) => point_polygon_distance(point, poly),
            (Value::Point(point), Value::Circle(circle))
            | (Value::Circle(circle), Value::Point(point)) => point_circle_distance(point, circle),
            (Value::Line(left), Value::Line(right)) => line_distance(left, right),
            (Value::Lseg(left), Value::Line(right)) | (Value::Line(right), Value::Lseg(left)) => {
                lseg_line_distance(left, right)
            }
            (Value::Lseg(left), Value::Lseg(right)) => lseg_lseg_distance(left, right),
            (Value::Lseg(left), Value::Box(right)) | (Value::Box(right), Value::Lseg(left)) => {
                lseg_box_distance(left, right)
            }
            (Value::Box(left), Value::Box(right)) => box_box_distance(left, right),
            (Value::Path(left), Value::Path(right)) => path_path_distance(left, right),
            (Value::Polygon(left), Value::Polygon(right)) => polygon_polygon_distance(left, right),
            (Value::Circle(left), Value::Circle(right)) => circle_circle_distance(left, right),
            (Value::Circle(circle), Value::Polygon(poly))
            | (Value::Polygon(poly), Value::Circle(circle)) => {
                circle_polygon_distance(circle, poly)
            }
            _ => {
                return Err(ExecError::TypeMismatch {
                    op: "<->",
                    left: left.clone(),
                    right: right.clone(),
                });
            }
        };
        Ok(Value::Float64(distance))
    })
}

fn eval_geo_closest_point(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "##", |left, right| match (left, right) {
        (Value::Point(point), Value::Line(line)) => {
            Ok(closest_point_value(line_closept_point(line, point)))
        }
        (Value::Point(point), Value::Lseg(lseg)) => {
            Ok(closest_point_value(lseg_closept_point(lseg, point)))
        }
        (Value::Point(point), Value::Box(geo_box)) => {
            Ok(closest_point_value(box_closept_point(geo_box, point)))
        }
        (Value::Line(line), Value::Lseg(lseg)) => {
            if lseg_slope(lseg) == line_slope(line) {
                Ok(Value::Null)
            } else {
                Ok(closest_point_value(lseg_closept_line(lseg, line)))
            }
        }
        (Value::Lseg(left), Value::Lseg(right)) => {
            if lseg_slope(left) == lseg_slope(right) {
                Ok(Value::Null)
            } else {
                Ok(closest_point_value(lseg_closept_lseg(right, left)))
            }
        }
        (Value::Lseg(lseg), Value::Box(geo_box)) => {
            Ok(closest_point_value(box_closept_lseg(geo_box, lseg)))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "##",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_intersection(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "#", |left, right| match (left, right) {
        (Value::Line(left), Value::Line(right)) => Ok(line_intersection(left, right)
            .map(Value::Point)
            .unwrap_or(Value::Null)),
        (Value::Lseg(left), Value::Lseg(right)) => Ok(lseg_intersection(left, right)
            .map(Value::Point)
            .unwrap_or(Value::Null)),
        (Value::Box(left), Value::Box(right)) => Ok(box_intersection(left, right)
            .map(Value::Box)
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::TypeMismatch {
            op: "#",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_intersects(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "?#", |left, right| match (left, right) {
        (Value::Line(left), Value::Line(right)) => {
            Ok(Value::Bool(line_intersection(left, right).is_some()))
        }
        (Value::Line(line), Value::Box(geo_box)) | (Value::Box(geo_box), Value::Line(line)) => {
            Ok(Value::Bool(line_intersects_box(line, geo_box)))
        }
        (Value::Lseg(lseg), Value::Line(line)) | (Value::Line(line), Value::Lseg(lseg)) => {
            Ok(Value::Bool(lseg_intersects_line(lseg, line)))
        }
        (Value::Lseg(lseg), Value::Box(geo_box)) | (Value::Box(geo_box), Value::Lseg(lseg)) => {
            Ok(Value::Bool(lseg_intersects_box(lseg, geo_box)))
        }
        (Value::Lseg(left), Value::Lseg(right)) => {
            Ok(Value::Bool(lseg_intersection(left, right).is_some()))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "?#",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_parallel(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "?||", |left, right| match (left, right) {
        (Value::Line(left), Value::Line(right)) => Ok(Value::Bool(line_parallel(left, right))),
        (Value::Lseg(left), Value::Lseg(right)) => Ok(Value::Bool(lseg_parallel(left, right))),
        _ => Err(ExecError::TypeMismatch {
            op: "?||",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_perpendicular(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "?-|", |left, right| match (left, right) {
        (Value::Line(left), Value::Line(right)) => Ok(Value::Bool(line_perpendicular(left, right))),
        (Value::Lseg(left), Value::Lseg(right)) => Ok(Value::Bool(lseg_perpendicular(left, right))),
        _ => Err(ExecError::TypeMismatch {
            op: "?-|",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_contains(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "@>", |left, right| match (left, right) {
        (Value::Box(outer), Value::Point(point)) => {
            Ok(Value::Bool(box_contains_point(outer, point)))
        }
        (Value::Box(outer), Value::Box(inner)) => Ok(Value::Bool(box_contains_box(outer, inner))),
        (Value::Polygon(poly), Value::Point(point)) => {
            Ok(Value::Bool(point_in_polygon(point, poly) != 0))
        }
        (Value::Polygon(outer), Value::Polygon(inner)) => {
            Ok(Value::Bool(polygon_contains_polygon(outer, inner)))
        }
        (Value::Circle(outer), Value::Circle(inner)) => {
            Ok(Value::Bool(circle_contains_circle(outer, inner)))
        }
        (Value::Circle(circle), Value::Point(point)) => {
            Ok(Value::Bool(point_circle_distance(point, circle) == 0.0))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "@>",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_contained_by(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "<@", |left, right| match (left, right) {
        (Value::Point(point), Value::Box(geo_box)) => {
            Ok(Value::Bool(box_contains_point(geo_box, point)))
        }
        (Value::Point(point), Value::Path(path)) => Ok(Value::Bool(point_in_path(point, path))),
        (Value::Point(point), Value::Polygon(poly)) => {
            Ok(Value::Bool(point_in_polygon(point, poly) != 0))
        }
        (Value::Point(point), Value::Circle(circle)) => {
            Ok(Value::Bool(point_circle_distance(point, circle) == 0.0))
        }
        (Value::Point(point), Value::Line(line)) => {
            Ok(Value::Bool(line_contains_point(line, point)))
        }
        (Value::Point(point), Value::Lseg(lseg)) => {
            Ok(Value::Bool(lseg_contains_point(lseg, point)))
        }
        (Value::Box(inner), Value::Box(outer)) => Ok(Value::Bool(box_contains_box(outer, inner))),
        (Value::Lseg(lseg), Value::Line(line)) => Ok(Value::Bool(
            line_contains_point(line, &lseg.p[0]) && line_contains_point(line, &lseg.p[1]),
        )),
        (Value::Lseg(lseg), Value::Box(geo_box)) => Ok(Value::Bool(
            box_contains_point(geo_box, &lseg.p[0]) && box_contains_point(geo_box, &lseg.p[1]),
        )),
        (Value::Polygon(inner), Value::Polygon(outer)) => {
            Ok(Value::Bool(polygon_contains_polygon(outer, inner)))
        }
        (Value::Circle(inner), Value::Circle(outer)) => {
            Ok(Value::Bool(circle_contains_circle(outer, inner)))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "<@",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_overlap(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "&&", |left, right| match (left, right) {
        (Value::Box(left), Value::Box(right)) => Ok(Value::Bool(box_overlap(left, right))),
        (Value::Polygon(left), Value::Polygon(right)) => {
            Ok(Value::Bool(polygon_overlap(left, right)))
        }
        (Value::Circle(left), Value::Circle(right)) => Ok(Value::Bool(circle_overlap(left, right))),
        _ => Err(ExecError::TypeMismatch {
            op: "&&",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_left(values: &[Value]) -> Result<Value, ExecError> {
    bbox_relation(
        values,
        "<<",
        |left, right| fp_lt(left.max_x, right.min_x),
        point_left_relation,
    )
}

fn eval_geo_over_left(values: &[Value]) -> Result<Value, ExecError> {
    bbox_relation(
        values,
        "&<",
        |left, right| fp_le(left.max_x, right.max_x),
        point_over_left_relation,
    )
}

fn eval_geo_right(values: &[Value]) -> Result<Value, ExecError> {
    bbox_relation(
        values,
        ">>",
        |left, right| fp_gt(left.min_x, right.max_x),
        point_right_relation,
    )
}

fn eval_geo_over_right(values: &[Value]) -> Result<Value, ExecError> {
    bbox_relation(
        values,
        "&>",
        |left, right| fp_ge(left.min_x, right.min_x),
        point_over_right_relation,
    )
}

fn eval_geo_below(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "<<|", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => {
            Ok(Value::Bool(point_below_relation(left, right)))
        }
        (Value::Box(left), Value::Box(right)) => Ok(Value::Bool(fp_le(left.high.y, right.low.y))),
        _ => match (value_bounds(left), value_bounds(right)) {
            (Some(left), Some(right)) => Ok(Value::Bool(fp_lt(left.max_y, right.min_y))),
            _ => Err(ExecError::TypeMismatch {
                op: "<<|",
                left: left.clone(),
                right: right.clone(),
            }),
        },
    })
}

fn eval_geo_over_below(values: &[Value]) -> Result<Value, ExecError> {
    bbox_relation(
        values,
        "&<|",
        |left, right| fp_le(left.max_y, right.max_y),
        point_over_below_relation,
    )
}

fn eval_geo_above(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "|>>", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => {
            Ok(Value::Bool(point_above_relation(left, right)))
        }
        (Value::Box(left), Value::Box(right)) => Ok(Value::Bool(fp_ge(left.low.y, right.high.y))),
        _ => match (value_bounds(left), value_bounds(right)) {
            (Some(left), Some(right)) => Ok(Value::Bool(fp_gt(left.min_y, right.max_y))),
            _ => Err(ExecError::TypeMismatch {
                op: "|>>",
                left: left.clone(),
                right: right.clone(),
            }),
        },
    })
}

fn eval_geo_over_above(values: &[Value]) -> Result<Value, ExecError> {
    bbox_relation(
        values,
        "|&>",
        |left, right| fp_ge(left.min_y, right.min_y),
        point_over_above_relation,
    )
}

fn eval_geo_add(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "+", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => Ok(Value::Point(point_add(left, right))),
        (Value::Box(geo_box), Value::Point(point)) => {
            Ok(Value::Box(box_translate(geo_box, point, true)))
        }
        (Value::Path(left), Value::Path(right)) => Ok(path_add(left, right)),
        (Value::Path(path), Value::Point(point)) => {
            Ok(Value::Path(path_transform(path, point, GeoTransform::Add)?))
        }
        (Value::Circle(circle), Value::Point(point)) => Ok(Value::Circle(circle_transform(
            circle,
            point,
            GeoTransform::Add,
        )?)),
        _ => Err(ExecError::TypeMismatch {
            op: "+",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_sub(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "-", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => Ok(Value::Point(point_sub(left, right))),
        (Value::Box(geo_box), Value::Point(point)) => {
            Ok(Value::Box(box_translate(geo_box, point, false)))
        }
        (Value::Path(path), Value::Point(point)) => {
            Ok(Value::Path(path_transform(path, point, GeoTransform::Sub)?))
        }
        (Value::Circle(circle), Value::Point(point)) => Ok(Value::Circle(circle_transform(
            circle,
            point,
            GeoTransform::Sub,
        )?)),
        _ => Err(ExecError::TypeMismatch {
            op: "-",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_mul(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "*", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => Ok(Value::Point(point_mul(left, right)?)),
        (Value::Box(geo_box), Value::Point(point)) => {
            Ok(Value::Box(box_scale(geo_box, point, GeoTransform::Mul)?))
        }
        (Value::Path(path), Value::Point(point)) => {
            Ok(Value::Path(path_transform(path, point, GeoTransform::Mul)?))
        }
        (Value::Circle(circle), Value::Point(point)) => Ok(Value::Circle(circle_transform(
            circle,
            point,
            GeoTransform::Mul,
        )?)),
        _ => Err(ExecError::TypeMismatch {
            op: "*",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_div(values: &[Value]) -> Result<Value, ExecError> {
    compare_binary(values, "/", |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => Ok(Value::Point(point_div(left, right)?)),
        (Value::Box(geo_box), Value::Point(point)) => {
            Ok(Value::Box(box_scale(geo_box, point, GeoTransform::Div)?))
        }
        (Value::Path(path), Value::Point(point)) => {
            Ok(Value::Path(path_transform(path, point, GeoTransform::Div)?))
        }
        (Value::Circle(circle), Value::Point(point)) => Ok(Value::Circle(circle_transform(
            circle,
            point,
            GeoTransform::Div,
        )?)),
        _ => Err(ExecError::TypeMismatch {
            op: "/",
            left: left.clone(),
            right: right.clone(),
        }),
    })
}

fn eval_geo_point_coord(values: &[Value], index: i32) -> Result<Value, ExecError> {
    unary_geometry(values, "[]", |value| match value {
        Value::Point(point) => Ok(Value::Float64(if index == 0 { point.x } else { point.y })),
        other => type_mismatch_unary("[]", other),
    })
}

fn eval_geo_box_point(values: &[Value], index: i32) -> Result<Value, ExecError> {
    unary_geometry(values, "[]", |value| match value {
        Value::Box(geo_box) => Ok(Value::Point(if index == 0 {
            geo_box.high.clone()
        } else {
            geo_box.low.clone()
        })),
        other => type_mismatch_unary("[]", other),
    })
}

fn unary_geometry(
    values: &[Value],
    _op: &'static str,
    func: impl FnOnce(&Value) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    func(value)
}

fn compare_binary(
    values: &[Value],
    _op: &'static str,
    func: impl FnOnce(&Value, &Value) -> Result<Value, ExecError>,
) -> Result<Value, ExecError> {
    let Some(left) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(right) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    func(left, right)
}

fn ordered_binary(
    values: &[Value],
    _op: &'static str,
    func: impl FnOnce(&Value, &Value) -> Result<bool, ExecError>,
) -> Result<Value, ExecError> {
    compare_binary(values, _op, |left, right| {
        func(left, right).map(Value::Bool)
    })
}

fn bbox_relation(
    values: &[Value],
    op: &'static str,
    bbox_pred: impl Fn(Bounds, Bounds) -> bool,
    point_pred: fn(&GeoPoint, &GeoPoint) -> bool,
) -> Result<Value, ExecError> {
    compare_binary(values, op, |left, right| match (left, right) {
        (Value::Point(left), Value::Point(right)) => Ok(Value::Bool(point_pred(left, right))),
        _ => match (value_bounds(left), value_bounds(right)) {
            (Some(left), Some(right)) => Ok(Value::Bool(bbox_pred(left, right))),
            _ => Err(ExecError::TypeMismatch {
                op,
                left: left.clone(),
                right: right.clone(),
            }),
        },
    })
}

fn point_left_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_lt(left.x, right.x)
}

fn point_over_left_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_le(left.x, right.x)
}

fn point_right_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_gt(left.x, right.x)
}

fn point_over_right_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_ge(left.x, right.x)
}

fn point_below_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_lt(left.y, right.y)
}

fn point_over_below_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_le(left.y, right.y)
}

fn point_above_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_gt(left.y, right.y)
}

fn point_over_above_relation(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_ge(left.y, right.y)
}

fn value_bounds(value: &Value) -> Option<Bounds> {
    match value {
        Value::Box(geo_box) => Some(Bounds {
            min_x: geo_box.low.x,
            max_x: geo_box.high.x,
            min_y: geo_box.low.y,
            max_y: geo_box.high.y,
        }),
        Value::Polygon(poly) => Some(Bounds {
            min_x: poly.bound_box.low.x,
            max_x: poly.bound_box.high.x,
            min_y: poly.bound_box.low.y,
            max_y: poly.bound_box.high.y,
        }),
        Value::Circle(circle) => Some(Bounds {
            min_x: circle.center.x - circle.radius,
            max_x: circle.center.x + circle.radius,
            min_y: circle.center.y - circle.radius,
            max_y: circle.center.y + circle.radius,
        }),
        Value::Point(point) => Some(Bounds {
            min_x: point.x,
            max_x: point.x,
            min_y: point.y,
            max_y: point.y,
        }),
        _ => None,
    }
}

fn point_same(left: &GeoPoint, right: &GeoPoint) -> bool {
    if left.x.is_nan() || left.y.is_nan() || right.x.is_nan() || right.y.is_nan() {
        return left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits();
    }
    fp_eq(left.x, right.x) && fp_eq(left.y, right.y)
}

fn point_same_x(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_eq(left.x, right.x)
}

fn point_same_y(left: &GeoPoint, right: &GeoPoint) -> bool {
    fp_eq(left.y, right.y)
}

fn point_distance(left: &GeoPoint, right: &GeoPoint) -> f64 {
    (left.x - right.x).hypot(left.y - right.y)
}

fn point_slope(left: &GeoPoint, right: &GeoPoint) -> f64 {
    if point_same_x(left, right) {
        f64::INFINITY
    } else if point_same_y(left, right) {
        0.0
    } else {
        (left.y - right.y) / (left.x - right.x)
    }
}

fn point_add(left: &GeoPoint, right: &GeoPoint) -> GeoPoint {
    GeoPoint {
        x: left.x + right.x,
        y: left.y + right.y,
    }
}

fn point_sub(left: &GeoPoint, right: &GeoPoint) -> GeoPoint {
    GeoPoint {
        x: left.x - right.x,
        y: left.y - right.y,
    }
}

fn point_mul(left: &GeoPoint, right: &GeoPoint) -> Result<GeoPoint, ExecError> {
    let x = checked_mul_sub(left.x, right.x, left.y, right.y)?;
    let y = checked_mul_add(left.x, right.y, left.y, right.x)?;
    Ok(GeoPoint { x, y })
}

fn point_div(left: &GeoPoint, right: &GeoPoint) -> Result<GeoPoint, ExecError> {
    let div = checked_sum(
        checked_mul(right.x, right.x)?,
        checked_mul(right.y, right.y)?,
    )?;
    let x = checked_div(checked_mul_add(left.x, right.x, left.y, right.y)?, div)?;
    let y = checked_div(checked_mul_sub(left.y, right.x, left.x, right.y)?, div)?;
    Ok(GeoPoint { x, y })
}

fn canonical_box(first: GeoPoint, second: GeoPoint) -> GeoBox {
    GeoBox {
        high: GeoPoint {
            x: pg_float8_max(first.x, second.x),
            y: pg_float8_max(first.y, second.y),
        },
        low: GeoPoint {
            x: pg_float8_min(first.x, second.x),
            y: pg_float8_min(first.y, second.y),
        },
    }
}

fn point_to_box(point: &GeoPoint) -> GeoBox {
    GeoBox {
        high: point.clone(),
        low: point.clone(),
    }
}

fn box_center(geo_box: &GeoBox) -> GeoPoint {
    GeoPoint {
        x: (geo_box.high.x + geo_box.low.x) / 2.0,
        y: (geo_box.high.y + geo_box.low.y) / 2.0,
    }
}

pub(crate) fn box_area(geo_box: &GeoBox) -> f64 {
    (geo_box.high.x - geo_box.low.x).abs() * (geo_box.high.y - geo_box.low.y).abs()
}

pub(crate) fn bound_box(left: &GeoBox, right: &GeoBox) -> GeoBox {
    GeoBox {
        high: GeoPoint {
            x: pg_float8_max(left.high.x, right.high.x),
            y: pg_float8_max(left.high.y, right.high.y),
        },
        low: GeoPoint {
            x: pg_float8_min(left.low.x, right.low.x),
            y: pg_float8_min(left.low.y, right.low.y),
        },
    }
}

pub(crate) fn box_same(left: &GeoBox, right: &GeoBox) -> bool {
    point_same(&left.high, &right.high) && point_same(&left.low, &right.low)
}

pub(crate) fn box_overlap(left: &GeoBox, right: &GeoBox) -> bool {
    fp_le(left.low.x, right.high.x)
        && fp_le(right.low.x, left.high.x)
        && fp_le(left.low.y, right.high.y)
        && fp_le(right.low.y, left.high.y)
}

pub(crate) fn box_contains_box(outer: &GeoBox, inner: &GeoBox) -> bool {
    fp_ge(outer.high.x, inner.high.x)
        && fp_le(outer.low.x, inner.low.x)
        && fp_ge(outer.high.y, inner.high.y)
        && fp_le(outer.low.y, inner.low.y)
}

pub(crate) fn box_contains_point(geo_box: &GeoBox, point: &GeoPoint) -> bool {
    geo_box.high.x >= point.x
        && geo_box.low.x <= point.x
        && geo_box.high.y >= point.y
        && geo_box.low.y <= point.y
}

fn box_intersection(left: &GeoBox, right: &GeoBox) -> Option<GeoBox> {
    if !box_overlap(left, right) {
        return None;
    }
    Some(GeoBox {
        high: GeoPoint {
            x: left.high.x.min(right.high.x),
            y: left.high.y.min(right.high.y),
        },
        low: GeoPoint {
            x: left.low.x.max(right.low.x),
            y: left.low.y.max(right.low.y),
        },
    })
}

fn box_to_polygon(geo_box: &GeoBox) -> GeoPolygon {
    make_polygon(vec![
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
    ])
}

fn box_to_circle(geo_box: &GeoBox) -> GeoCircle {
    let center = box_center(geo_box);
    GeoCircle {
        radius: point_distance(&center, &geo_box.high),
        center,
    }
}

fn circle_to_box(circle: &GeoCircle) -> GeoBox {
    let delta = circle.radius / 2.0_f64.sqrt();
    GeoBox {
        high: GeoPoint {
            x: circle.center.x + delta,
            y: circle.center.y + delta,
        },
        low: GeoPoint {
            x: circle.center.x - delta,
            y: circle.center.y - delta,
        },
    }
}

pub(crate) fn circle_bound_box(circle: &GeoCircle) -> GeoBox {
    GeoBox {
        high: GeoPoint {
            x: circle.center.x + circle.radius,
            y: circle.center.y + circle.radius,
        },
        low: GeoPoint {
            x: circle.center.x - circle.radius,
            y: circle.center.y - circle.radius,
        },
    }
}

fn make_circle(center: GeoPoint, radius: f64) -> Result<GeoCircle, ExecError> {
    if radius < 0.0 {
        return Err(invalid_geometry_input(
            "circle",
            &render_point(&center, FloatFormatOptions::default()),
        ));
    }
    Ok(GeoCircle { center, radius })
}

fn circle_area(circle: &GeoCircle) -> f64 {
    PI * circle.radius * circle.radius
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

fn circle_circle_distance(left: &GeoCircle, right: &GeoCircle) -> f64 {
    clamp_nonnegative(point_distance(&left.center, &right.center) - (left.radius + right.radius))
}

fn circle_to_polygon(npts: i32, circle: &GeoCircle) -> Result<GeoPolygon, ExecError> {
    if fp_zero(circle.radius) {
        return Err(ExecError::InvalidStorageValue {
            column: String::new(),
            details: "cannot convert circle with radius zero to polygon".into(),
        });
    }
    if npts < 2 {
        return Err(ExecError::InvalidStorageValue {
            column: String::new(),
            details: "must request at least 2 points".into(),
        });
    }
    let step = 2.0 * PI / npts as f64;
    let mut points = Vec::with_capacity(npts as usize);
    for idx in 0..npts {
        let angle = step * idx as f64;
        points.push(GeoPoint {
            x: circle.center.x - circle.radius * angle.cos(),
            y: circle.center.y + circle.radius * angle.sin(),
        });
    }
    Ok(make_polygon(points))
}

fn make_polygon(points: Vec<GeoPoint>) -> GeoPolygon {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for point in &points {
        min_x = pg_float8_min(point.x, min_x);
        max_x = pg_float8_max(point.x, max_x);
        min_y = pg_float8_min(point.y, min_y);
        max_y = pg_float8_max(point.y, max_y);
    }
    GeoPolygon {
        bound_box: GeoBox {
            high: GeoPoint { x: max_x, y: max_y },
            low: GeoPoint { x: min_x, y: min_y },
        },
        points,
    }
}

fn polygon_area(poly: &GeoPolygon) -> f64 {
    polygon_area_points(&poly.points)
}

fn polygon_area_points(points: &[GeoPoint]) -> f64 {
    if points.len() < 2 {
        return 0.0;
    }
    let mut area = 0.0;
    for idx in 0..points.len() {
        let next = (idx + 1) % points.len();
        area += points[idx].x * points[next].y;
        area -= points[idx].y * points[next].x;
    }
    area.abs() / 2.0
}

fn polygon_to_circle(poly: &GeoPolygon) -> GeoCircle {
    let center = poly_center(poly);
    let mut radius = 0.0;
    for point in &poly.points {
        radius += point_distance(point, &center);
    }
    if !poly.points.is_empty() {
        radius /= poly.points.len() as f64;
    }
    GeoCircle { center, radius }
}

fn poly_center(poly: &GeoPolygon) -> GeoPoint {
    if poly.points.is_empty() {
        return GeoPoint { x: 0.0, y: 0.0 };
    }
    let mut center = GeoPoint { x: 0.0, y: 0.0 };
    for point in &poly.points {
        center.x += point.x;
        center.y += point.y;
    }
    let npts = poly.points.len() as f64;
    center.x /= npts;
    center.y /= npts;
    center
}

pub(crate) fn polygon_same(left: &GeoPolygon, right: &GeoPolygon) -> bool {
    if left.points.len() != right.points.len() {
        return false;
    }
    plist_same(&left.points, &right.points)
}

pub(crate) fn polygon_overlap(left: &GeoPolygon, right: &GeoPolygon) -> bool {
    if !box_overlap(&left.bound_box, &right.bound_box) {
        return false;
    }
    if edges_intersect(&left.points, true, &right.points, true) {
        return true;
    }
    point_in_polygon(&left.points[0], right) != 0 || point_in_polygon(&right.points[0], left) != 0
}

pub(crate) fn polygon_contains_polygon(outer: &GeoPolygon, inner: &GeoPolygon) -> bool {
    if outer.points.is_empty() || inner.points.is_empty() {
        return false;
    }
    if !box_contains_box(&outer.bound_box, &inner.bound_box) {
        return false;
    }
    let mut previous = &inner.points[inner.points.len() - 1];
    for point in &inner.points {
        if !lseg_inside_polygon(previous, point, outer, 0) {
            return false;
        }
        previous = point;
    }
    true
}

fn point_in_polygon(point: &GeoPoint, poly: &GeoPolygon) -> i32 {
    point_inside(point, &poly.points)
}

// PostgreSQL polygon containment checks every contained edge so boundary
// touches count as contained while true crossings are rejected.
fn touched_lseg_inside_polygon(
    a: &GeoPoint,
    b: &GeoPoint,
    segment: &GeoLseg,
    poly: &GeoPolygon,
    start: usize,
) -> bool {
    let test_segment = GeoLseg {
        p: [a.clone(), b.clone()],
    };

    if point_same(a, &segment.p[0]) {
        if lseg_contains_point(&test_segment, &segment.p[1]) {
            return lseg_inside_polygon(b, &segment.p[1], poly, start);
        }
    } else if point_same(a, &segment.p[1]) {
        if lseg_contains_point(&test_segment, &segment.p[0]) {
            return lseg_inside_polygon(b, &segment.p[0], poly, start);
        }
    } else if lseg_contains_point(&test_segment, &segment.p[0]) {
        return lseg_inside_polygon(b, &segment.p[0], poly, start);
    } else if lseg_contains_point(&test_segment, &segment.p[1]) {
        return lseg_inside_polygon(b, &segment.p[1], poly, start);
    }

    true
}

fn lseg_inside_polygon(a: &GeoPoint, b: &GeoPoint, poly: &GeoPolygon, start: usize) -> bool {
    if poly.points.is_empty() {
        return false;
    }
    let mut result = true;
    let mut intersection = false;
    let test_segment = GeoLseg {
        p: [a.clone(), b.clone()],
    };
    let previous_idx = if start == 0 {
        poly.points.len() - 1
    } else {
        start - 1
    };
    let mut poly_segment = GeoLseg {
        p: [
            poly.points[previous_idx].clone(),
            poly.points[previous_idx].clone(),
        ],
    };

    for idx in start..poly.points.len() {
        if !result {
            break;
        }
        poly_segment.p[1] = poly.points[idx].clone();

        if lseg_contains_point(&poly_segment, &test_segment.p[0]) {
            if lseg_contains_point(&poly_segment, &test_segment.p[1]) {
                return true;
            }
            result = touched_lseg_inside_polygon(
                &test_segment.p[0],
                &test_segment.p[1],
                &poly_segment,
                poly,
                idx + 1,
            );
        } else if lseg_contains_point(&poly_segment, &test_segment.p[1]) {
            result = touched_lseg_inside_polygon(
                &test_segment.p[1],
                &test_segment.p[0],
                &poly_segment,
                poly,
                idx + 1,
            );
        } else if let Some(intersection_point) = lseg_intersection(&test_segment, &poly_segment) {
            intersection = true;
            result = lseg_inside_polygon(&test_segment.p[0], &intersection_point, poly, idx + 1);
            if result {
                result =
                    lseg_inside_polygon(&test_segment.p[1], &intersection_point, poly, idx + 1);
            }
        }

        poly_segment.p[0] = poly_segment.p[1].clone();
    }

    if result && !intersection {
        let midpoint = GeoPoint {
            x: (test_segment.p[0].x + test_segment.p[1].x) / 2.0,
            y: (test_segment.p[0].y + test_segment.p[1].y) / 2.0,
        };
        result = point_in_polygon(&midpoint, poly) != 0;
    }

    result
}

fn path_to_polygon(path: &GeoPath) -> Result<GeoPolygon, ExecError> {
    if !path.closed {
        return Err(ExecError::InvalidStorageValue {
            column: String::new(),
            details: "open path cannot be converted to polygon".into(),
        });
    }
    Ok(make_polygon(path.points.clone()))
}

fn point_in_path(point: &GeoPoint, path: &GeoPath) -> bool {
    if !path.closed {
        return path
            .points
            .windows(2)
            .any(|segment| point_on_segment(point, &segment[0], &segment[1]));
    }
    point_inside(point, &path.points) != 0
}

fn path_add(left: &GeoPath, right: &GeoPath) -> Value {
    if left.closed || right.closed {
        return Value::Null;
    }
    let mut points = left.points.clone();
    points.extend(right.points.clone());
    Value::Path(GeoPath {
        closed: false,
        points,
    })
}

fn path_transform(
    path: &GeoPath,
    point: &GeoPoint,
    transform: GeoTransform,
) -> Result<GeoPath, ExecError> {
    let mut points = Vec::with_capacity(path.points.len());
    for current in &path.points {
        points.push(match transform {
            GeoTransform::Add => point_add(current, point),
            GeoTransform::Sub => point_sub(current, point),
            GeoTransform::Mul => point_mul(current, point)?,
            GeoTransform::Div => point_div(current, point)?,
        });
    }
    Ok(GeoPath {
        closed: path.closed,
        points,
    })
}

fn circle_transform(
    circle: &GeoCircle,
    point: &GeoPoint,
    transform: GeoTransform,
) -> Result<GeoCircle, ExecError> {
    let center = match transform {
        GeoTransform::Add => point_add(&circle.center, point),
        GeoTransform::Sub => point_sub(&circle.center, point),
        GeoTransform::Mul => point_mul(&circle.center, point)?,
        GeoTransform::Div => point_div(&circle.center, point)?,
    };
    let radius = match transform {
        GeoTransform::Add | GeoTransform::Sub => circle.radius,
        GeoTransform::Mul => checked_mul(
            circle.radius,
            point_distance(&GeoPoint { x: 0.0, y: 0.0 }, point),
        )?,
        GeoTransform::Div => checked_div(
            circle.radius,
            point_distance(&GeoPoint { x: 0.0, y: 0.0 }, point),
        )?,
    };
    Ok(GeoCircle { center, radius })
}

fn box_translate(geo_box: &GeoBox, point: &GeoPoint, add: bool) -> GeoBox {
    let high = if add {
        point_add(&geo_box.high, point)
    } else {
        point_sub(&geo_box.high, point)
    };
    let low = if add {
        point_add(&geo_box.low, point)
    } else {
        point_sub(&geo_box.low, point)
    };
    canonical_box(high, low)
}

fn box_scale(
    geo_box: &GeoBox,
    point: &GeoPoint,
    transform: GeoTransform,
) -> Result<GeoBox, ExecError> {
    let high = match transform {
        GeoTransform::Mul => point_mul(&geo_box.high, point)?,
        GeoTransform::Div => point_div(&geo_box.high, point)?,
        _ => unreachable!(),
    };
    let low = match transform {
        GeoTransform::Mul => point_mul(&geo_box.low, point)?,
        GeoTransform::Div => point_div(&geo_box.low, point)?,
        _ => unreachable!(),
    };
    Ok(canonical_box(high, low))
}

fn lseg_center(lseg: &GeoLseg) -> GeoPoint {
    GeoPoint {
        x: (lseg.p[0].x + lseg.p[1].x) / 2.0,
        y: (lseg.p[0].y + lseg.p[1].y) / 2.0,
    }
}

fn lseg_eq(left: &GeoLseg, right: &GeoLseg) -> bool {
    point_same(&left.p[0], &right.p[0]) && point_same(&left.p[1], &right.p[1])
}

fn lseg_length(lseg: &GeoLseg) -> f64 {
    point_distance(&lseg.p[0], &lseg.p[1])
}

fn line_from_points(first: GeoPoint, second: GeoPoint) -> Result<GeoLine, ExecError> {
    if point_same(&first, &second) {
        return Err(ExecError::InvalidStorageValue {
            column: String::new(),
            details: "invalid line specification: must be two distinct points".into(),
        });
    }
    let slope = point_slope(&first, &second);
    if slope.is_infinite() {
        Ok(GeoLine {
            a: -1.0,
            b: 0.0,
            c: first.x,
        })
    } else if slope == 0.0 {
        Ok(GeoLine {
            a: 0.0,
            b: -1.0,
            c: first.y,
        })
    } else {
        let c = first.y - slope * first.x;
        Ok(GeoLine {
            a: slope,
            b: -1.0,
            c: if c == 0.0 { 0.0 } else { c },
        })
    }
}

fn line_same(left: &GeoLine, right: &GeoLine) -> bool {
    if left.a.is_nan()
        || left.b.is_nan()
        || left.c.is_nan()
        || right.a.is_nan()
        || right.b.is_nan()
        || right.c.is_nan()
    {
        return left.a.to_bits() == right.a.to_bits()
            && left.b.to_bits() == right.b.to_bits()
            && left.c.to_bits() == right.c.to_bits();
    }
    let ratio = if !fp_zero(right.a) {
        left.a / right.a
    } else if !fp_zero(right.b) {
        left.b / right.b
    } else if !fp_zero(right.c) {
        left.c / right.c
    } else {
        1.0
    };
    fp_eq(left.a, ratio * right.a)
        && fp_eq(left.b, ratio * right.b)
        && fp_eq(left.c, ratio * right.c)
}

fn line_parallel(left: &GeoLine, right: &GeoLine) -> bool {
    line_intersection(left, right).is_none()
}

fn line_perpendicular(left: &GeoLine, right: &GeoLine) -> bool {
    if fp_zero(left.a) {
        return fp_zero(right.b);
    }
    if fp_zero(right.a) {
        return fp_zero(left.b);
    }
    if fp_zero(left.b) {
        return fp_zero(right.a);
    }
    if fp_zero(right.b) {
        return fp_zero(left.a);
    }
    fp_eq((left.a * right.a) / (left.b * right.b), -1.0)
}

fn line_contains_point(line: &GeoLine, point: &GeoPoint) -> bool {
    fp_zero(line.a * point.x + line.b * point.y + line.c)
}

fn normalized_line_eval(line: &GeoLine, point: &GeoPoint) -> f64 {
    line.a * point.x + line.b * point.y + line.c
}

fn line_intersection(left: &GeoLine, right: &GeoLine) -> Option<GeoPoint> {
    let (x, y) = if !fp_zero(left.b) {
        if fp_eq(right.a, left.a * (right.b / left.b)) {
            return None;
        }
        let x =
            ((left.b * right.c) - (right.b * left.c)) / ((left.a * right.b) - (right.a * left.b));
        let y = -((left.a * x) + left.c) / left.b;
        (x, y)
    } else if !fp_zero(right.b) {
        if fp_eq(left.a, right.a * (left.b / right.b)) {
            return None;
        }
        let x =
            ((right.b * left.c) - (left.b * right.c)) / ((right.a * left.b) - (left.a * right.b));
        let y = -((right.a * x) + right.c) / right.b;
        (x, y)
    } else {
        return None;
    };
    Some(GeoPoint {
        x: if x == 0.0 { 0.0 } else { x },
        y: if y == 0.0 { 0.0 } else { y },
    })
}

fn line_distance(left: &GeoLine, right: &GeoLine) -> f64 {
    if line_intersection(left, right).is_some() {
        return 0.0;
    }
    let ratio = if !fp_zero(left.a) && !left.a.is_nan() && !fp_zero(right.a) && !right.a.is_nan() {
        left.a / right.a
    } else if !fp_zero(left.b) && !left.b.is_nan() && !fp_zero(right.b) && !right.b.is_nan() {
        left.b / right.b
    } else {
        1.0
    };
    (left.c - ratio * right.c).abs() / (left.a * left.a + left.b * left.b).sqrt()
}

fn clean_float_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn clean_point_zero(point: GeoPoint) -> GeoPoint {
    GeoPoint {
        x: clean_float_zero(point.x),
        y: clean_float_zero(point.y),
    }
}

fn clamp_nonnegative(value: f64) -> f64 {
    if value < 0.0 { 0.0 } else { value }
}

fn closest_point_value((distance, point): (f64, Option<GeoPoint>)) -> Value {
    if distance.is_nan() {
        return Value::Null;
    }
    point
        .map(clean_point_zero)
        .map(Value::Point)
        .unwrap_or(Value::Null)
}

fn line_from_point_slope(point: &GeoPoint, slope: f64) -> GeoLine {
    if slope.is_infinite() {
        GeoLine {
            a: -1.0,
            b: 0.0,
            c: point.x,
        }
    } else if slope == 0.0 {
        GeoLine {
            a: 0.0,
            b: -1.0,
            c: point.y,
        }
    } else {
        let c = point.y - slope * point.x;
        GeoLine {
            a: slope,
            b: -1.0,
            c: clean_float_zero(c),
        }
    }
}

fn line_slope(line: &GeoLine) -> f64 {
    if fp_zero(line.a) {
        0.0
    } else if fp_zero(line.b) {
        f64::INFINITY
    } else {
        line.a / -line.b
    }
}

fn line_inverse_slope(line: &GeoLine) -> f64 {
    if fp_zero(line.a) {
        f64::INFINITY
    } else if fp_zero(line.b) {
        0.0
    } else {
        line.b / line.a
    }
}

fn lseg_slope(lseg: &GeoLseg) -> f64 {
    point_slope(&lseg.p[0], &lseg.p[1])
}

fn point_line_distance(point: &GeoPoint, line: &GeoLine) -> f64 {
    line_closept_point(line, point).0
}

fn line_closept_point(line: &GeoLine, point: &GeoPoint) -> (f64, Option<GeoPoint>) {
    let tmp = line_from_point_slope(point, line_inverse_slope(line));
    let Some(closept) = line_intersection(&tmp, line) else {
        return (f64::NAN, Some(point.clone()));
    };
    (point_distance(&closept, point), Some(closept))
}

fn lseg_line(lseg: &GeoLseg) -> GeoLine {
    line_from_point_slope(&lseg.p[0], lseg_slope(lseg))
}

fn lseg_intersection_line(lseg: &GeoLseg, line: &GeoLine) -> Option<GeoPoint> {
    let tmp = lseg_line(lseg);
    let interpt = line_intersection(&tmp, line)?;
    if !lseg_contains_point(lseg, &interpt) {
        return None;
    }
    if point_same(&lseg.p[0], &interpt) {
        Some(lseg.p[0].clone())
    } else if point_same(&lseg.p[1], &interpt) {
        Some(lseg.p[1].clone())
    } else {
        Some(interpt)
    }
}

fn point_on_segment(point: &GeoPoint, start: &GeoPoint, end: &GeoPoint) -> bool {
    fp_eq(
        point_distance(start, point) + point_distance(point, end),
        point_distance(start, end),
    )
}

fn lseg_contains_point(lseg: &GeoLseg, point: &GeoPoint) -> bool {
    point_on_segment(point, &lseg.p[0], &lseg.p[1])
}

fn lseg_closept_line(lseg: &GeoLseg, line: &GeoLine) -> (f64, Option<GeoPoint>) {
    if let Some(point) = lseg_intersection_line(lseg, line) {
        return (0.0, Some(point));
    }
    let dist1 = line_closept_point(line, &lseg.p[0]).0;
    let dist2 = line_closept_point(line, &lseg.p[1]).0;
    if dist1 < dist2 {
        (dist1, Some(lseg.p[0].clone()))
    } else {
        (dist2, Some(lseg.p[1].clone()))
    }
}

fn lseg_closept_point(lseg: &GeoLseg, point: &GeoPoint) -> (f64, Option<GeoPoint>) {
    let tmp = line_from_point_slope(point, inverse_slope(&lseg.p[0], &lseg.p[1]));
    let (_, closept) = lseg_closept_line(lseg, &tmp);
    let Some(closept) = closept else {
        return (f64::NAN, None);
    };
    (point_distance(&closept, point), Some(closept))
}

fn point_lseg_distance(point: &GeoPoint, lseg: &GeoLseg) -> f64 {
    lseg_closept_point(lseg, point).0
}

fn box_closept_point(geo_box: &GeoBox, point: &GeoPoint) -> (f64, Option<GeoPoint>) {
    if box_contains_point(geo_box, point) {
        return (0.0, Some(point.clone()));
    }
    let mut best_distance = 0.0;
    let mut best_point = None;
    let mut have_min = false;
    for edge in box_edges_pg(geo_box) {
        let (distance, closept) = lseg_closept_point(&edge, point);
        if !have_min || distance < best_distance {
            best_distance = distance;
            best_point = closept;
            have_min = true;
        }
    }
    (best_distance, best_point)
}

fn point_box_distance(point: &GeoPoint, geo_box: &GeoBox) -> f64 {
    box_closept_point(geo_box, point).0
}

fn point_circle_distance(point: &GeoPoint, circle: &GeoCircle) -> f64 {
    clamp_nonnegative(point_distance(point, &circle.center) - circle.radius)
}

fn point_path_distance(point: &GeoPoint, path: &GeoPath) -> f64 {
    let mut best = 0.0;
    let mut have_min = false;
    for segment in path_segments(path) {
        let distance = point_lseg_distance(point, &segment);
        if !have_min || distance < best {
            best = distance;
            have_min = true;
        }
    }
    best
}

pub(crate) fn point_polygon_distance(point: &GeoPoint, poly: &GeoPolygon) -> f64 {
    if point_in_polygon(point, poly) != 0 {
        return 0.0;
    }
    let mut best = 0.0;
    let mut have_min = false;
    for segment in closed_segments(&poly.points) {
        let distance = point_lseg_distance(point, &segment);
        if !have_min || distance < best {
            best = distance;
            have_min = true;
        }
    }
    best
}

fn lseg_intersection(left: &GeoLseg, right: &GeoLseg) -> Option<GeoPoint> {
    let tmp = lseg_line(right);
    let interpt = lseg_intersection_line(left, &tmp)?;
    lseg_contains_point(right, &interpt).then_some(interpt)
}

fn lseg_parallel(left: &GeoLseg, right: &GeoLseg) -> bool {
    fp_eq(
        point_slope(&left.p[0], &left.p[1]),
        point_slope(&right.p[0], &right.p[1]),
    )
}

fn lseg_perpendicular(left: &GeoLseg, right: &GeoLseg) -> bool {
    fp_eq(
        point_slope(&left.p[0], &left.p[1]),
        inverse_slope(&right.p[0], &right.p[1]),
    )
}

fn inverse_slope(left: &GeoPoint, right: &GeoPoint) -> f64 {
    if point_same_x(left, right) {
        0.0
    } else if point_same_y(left, right) {
        f64::INFINITY
    } else {
        (left.x - right.x) / (right.y - left.y)
    }
}

fn lseg_line_distance(lseg: &GeoLseg, line: &GeoLine) -> f64 {
    lseg_closept_line(lseg, line).0
}

fn lseg_lseg_distance(left: &GeoLseg, right: &GeoLseg) -> f64 {
    lseg_closept_lseg(left, right).0
}

fn lseg_box_distance(lseg: &GeoLseg, geo_box: &GeoBox) -> f64 {
    box_closept_lseg(geo_box, lseg).0
}

pub(crate) fn box_box_distance(left: &GeoBox, right: &GeoBox) -> f64 {
    point_distance(&box_center(left), &box_center(right))
}

fn path_path_distance(left: &GeoPath, right: &GeoPath) -> f64 {
    let left_segments = path_segments(left);
    let right_segments = path_segments(right);
    let mut best = 0.0;
    let mut have_min = false;
    for left in &left_segments {
        for right in &right_segments {
            let distance = lseg_lseg_distance(left, right);
            if !have_min || distance < best {
                best = distance;
                have_min = true;
            }
        }
    }
    best
}

fn polygon_polygon_distance(left: &GeoPolygon, right: &GeoPolygon) -> f64 {
    if polygon_overlap(left, right)
        || point_in_polygon(&left.points[0], right) != 0
        || point_in_polygon(&right.points[0], left) != 0
    {
        return 0.0;
    }
    let mut best = 0.0;
    let mut have_min = false;
    for left in closed_segments(&left.points) {
        for right in closed_segments(&right.points) {
            let distance = lseg_lseg_distance(&left, &right);
            if !have_min || distance < best {
                best = distance;
                have_min = true;
            }
        }
    }
    best
}

fn circle_polygon_distance(circle: &GeoCircle, poly: &GeoPolygon) -> f64 {
    clamp_nonnegative(point_polygon_distance(&circle.center, poly) - circle.radius)
}

fn line_intersects_box(line: &GeoLine, geo_box: &GeoBox) -> bool {
    box_edges(geo_box)
        .iter()
        .any(|edge| lseg_intersects_line(edge, line))
}

fn lseg_intersects_line(lseg: &GeoLseg, line: &GeoLine) -> bool {
    lseg_intersection_line(lseg, line).is_some()
}

fn lseg_intersects_box(lseg: &GeoLseg, geo_box: &GeoBox) -> bool {
    box_interpt_lseg(geo_box, lseg).0
}

fn lseg_closept_lseg(on_lseg: &GeoLseg, to_lseg: &GeoLseg) -> (f64, Option<GeoPoint>) {
    if let Some(point) = lseg_intersection(on_lseg, to_lseg) {
        return (0.0, Some(point));
    }

    let (mut dist, mut result) = lseg_closept_point(on_lseg, &to_lseg.p[0]);
    let (d, point) = lseg_closept_point(on_lseg, &to_lseg.p[1]);
    if d < dist {
        dist = d;
        result = point;
    }

    let d = lseg_closept_point(to_lseg, &on_lseg.p[0]).0;
    if d < dist {
        dist = d;
        result = Some(on_lseg.p[0].clone());
    }
    let d = lseg_closept_point(to_lseg, &on_lseg.p[1]).0;
    if d < dist {
        dist = d;
        result = Some(on_lseg.p[1].clone());
    }

    (dist, result)
}

fn box_interpt_lseg(geo_box: &GeoBox, lseg: &GeoLseg) -> (bool, Option<GeoPoint>) {
    let lbox = canonical_box(lseg.p[0].clone(), lseg.p[1].clone());
    if !box_overlap(&lbox, geo_box) {
        return (false, None);
    }

    let center = box_center(geo_box);
    let result = lseg_closept_point(lseg, &center).1;

    if box_contains_point(geo_box, &lseg.p[0]) || box_contains_point(geo_box, &lseg.p[1]) {
        return (true, result);
    }

    for edge in box_edges_pg(geo_box) {
        if lseg_intersection(&edge, lseg).is_some() {
            return (true, result);
        }
    }

    (false, None)
}

fn box_closept_lseg(geo_box: &GeoBox, lseg: &GeoLseg) -> (f64, Option<GeoPoint>) {
    let (intersects, point) = box_interpt_lseg(geo_box, lseg);
    if intersects {
        return (0.0, point);
    }

    let mut best_distance = 0.0;
    let mut best_point = None;
    let mut have_min = false;
    for edge in box_edges_pg(geo_box) {
        let (distance, closept) = lseg_closept_lseg(&edge, lseg);
        if !have_min || distance < best_distance {
            best_distance = distance;
            best_point = closept;
            have_min = true;
        }
    }
    (best_distance, best_point)
}

fn path_segments(path: &GeoPath) -> Vec<GeoLseg> {
    let mut segments = Vec::new();
    for idx in 0..path.points.len() {
        let prev = if idx > 0 {
            idx - 1
        } else if path.closed {
            path.points.len().saturating_sub(1)
        } else {
            continue;
        };
        segments.push(GeoLseg {
            p: [path.points[prev].clone(), path.points[idx].clone()],
        });
    }
    segments
}

fn closed_segments(points: &[GeoPoint]) -> Vec<GeoLseg> {
    let path = GeoPath {
        closed: true,
        points: points.to_vec(),
    };
    path_segments(&path)
}

fn box_edges(geo_box: &GeoBox) -> [GeoLseg; 4] {
    let p1 = geo_box.low.clone();
    let p2 = GeoPoint {
        x: geo_box.low.x,
        y: geo_box.high.y,
    };
    let p3 = geo_box.high.clone();
    let p4 = GeoPoint {
        x: geo_box.high.x,
        y: geo_box.low.y,
    };
    [
        GeoLseg {
            p: [p1.clone(), p2.clone()],
        },
        GeoLseg {
            p: [p2, p3.clone()],
        },
        GeoLseg {
            p: [p3, p4.clone()],
        },
        GeoLseg { p: [p4, p1] },
    ]
}

fn box_edges_pg(geo_box: &GeoBox) -> [GeoLseg; 4] {
    let upper_left = GeoPoint {
        x: geo_box.low.x,
        y: geo_box.high.y,
    };
    let lower_right = GeoPoint {
        x: geo_box.high.x,
        y: geo_box.low.y,
    };
    [
        GeoLseg {
            p: [geo_box.low.clone(), upper_left.clone()],
        },
        GeoLseg {
            p: [geo_box.high.clone(), upper_left],
        },
        GeoLseg {
            p: [geo_box.low.clone(), lower_right.clone()],
        },
        GeoLseg {
            p: [geo_box.high.clone(), lower_right],
        },
    ]
}

fn edges_intersect(
    left_points: &[GeoPoint],
    left_closed: bool,
    right_points: &[GeoPoint],
    right_closed: bool,
) -> bool {
    let left = GeoPath {
        closed: left_closed,
        points: left_points.to_vec(),
    };
    let right = GeoPath {
        closed: right_closed,
        points: right_points.to_vec(),
    };
    for left in path_segments(&left) {
        for right in path_segments(&right) {
            if lseg_intersection(&left, &right).is_some() {
                return true;
            }
        }
    }
    false
}

fn plist_same(left: &[GeoPoint], right: &[GeoPoint]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    for idx in 0..right.len() {
        if point_same(&right[idx], &left[0]) {
            let mut forward = true;
            for offset in 1..left.len() {
                let right_idx = (idx + offset) % right.len();
                if !point_same(&right[right_idx], &left[offset]) {
                    forward = false;
                    break;
                }
            }
            if forward {
                return true;
            }
            let mut backward = true;
            for offset in 1..left.len() {
                let right_idx = (idx + right.len() - offset) % right.len();
                if !point_same(&right[right_idx], &left[offset]) {
                    backward = false;
                    break;
                }
            }
            if backward {
                return true;
            }
        }
    }
    false
}

const POINT_ON_POLYGON: i32 = i32::MAX;

fn point_inside(point: &GeoPoint, points: &[GeoPoint]) -> i32 {
    if points.is_empty() {
        return 0;
    }
    let first_x = points[0].x - point.x;
    let first_y = points[0].y - point.y;
    let mut prev_x = first_x;
    let mut prev_y = first_y;
    let mut total_cross = 0;
    for current in points.iter().skip(1) {
        let x = current.x - point.x;
        let y = current.y - point.y;
        let cross = lseg_crossing(x, y, prev_x, prev_y);
        if cross == POINT_ON_POLYGON {
            return 2;
        }
        total_cross += cross;
        prev_x = x;
        prev_y = y;
    }
    let cross = lseg_crossing(first_x, first_y, prev_x, prev_y);
    if cross == POINT_ON_POLYGON {
        return 2;
    }
    total_cross += cross;
    if total_cross != 0 { 1 } else { 0 }
}

fn lseg_crossing(x: f64, y: f64, prev_x: f64, prev_y: f64) -> i32 {
    if fp_zero(y) {
        if fp_zero(x) {
            return POINT_ON_POLYGON;
        }
        if fp_gt(x, 0.0) {
            if fp_zero(prev_y) {
                return if fp_gt(prev_x, 0.0) {
                    0
                } else {
                    POINT_ON_POLYGON
                };
            }
            return if fp_lt(prev_y, 0.0) { 1 } else { -1 };
        }
        if fp_zero(prev_y) {
            return if fp_lt(prev_x, 0.0) {
                0
            } else {
                POINT_ON_POLYGON
            };
        }
        return 0;
    }

    let y_sign = if fp_gt(y, 0.0) { 1 } else { -1 };
    if fp_zero(prev_y) {
        return if fp_lt(prev_x, 0.0) { 0 } else { y_sign };
    }
    if (y_sign < 0 && fp_lt(prev_y, 0.0)) || (y_sign > 0 && fp_gt(prev_y, 0.0)) {
        return 0;
    }
    if fp_ge(x, 0.0) && fp_gt(prev_x, 0.0) {
        return 2 * y_sign;
    }
    if fp_lt(x, 0.0) && fp_le(prev_x, 0.0) {
        return 0;
    }
    let z = (x - prev_x) * y - (y - prev_y) * x;
    if fp_zero(z) {
        return POINT_ON_POLYGON;
    }
    if (y_sign < 0 && fp_lt(z, 0.0)) || (y_sign > 0 && fp_gt(z, 0.0)) {
        return 0;
    }
    2 * y_sign
}

fn fp_zero(value: f64) -> bool {
    value.abs() <= GEOMETRY_EPSILON
}

fn fp_eq(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() <= GEOMETRY_EPSILON
}

fn fp_ne(left: f64, right: f64) -> bool {
    left != right && (left - right).abs() > GEOMETRY_EPSILON
}

fn fp_lt(left: f64, right: f64) -> bool {
    left + GEOMETRY_EPSILON < right
}

fn fp_le(left: f64, right: f64) -> bool {
    left <= right + GEOMETRY_EPSILON
}

fn fp_gt(left: f64, right: f64) -> bool {
    left > right + GEOMETRY_EPSILON
}

fn fp_ge(left: f64, right: f64) -> bool {
    left + GEOMETRY_EPSILON >= right
}

fn pg_float8_lt(left: f64, right: f64) -> bool {
    !left.is_nan() && (right.is_nan() || left < right)
}

fn pg_float8_gt(left: f64, right: f64) -> bool {
    !right.is_nan() && (left.is_nan() || left > right)
}

fn pg_float8_min(left: f64, right: f64) -> f64 {
    if pg_float8_lt(left, right) {
        left
    } else {
        right
    }
}

fn pg_float8_max(left: f64, right: f64) -> f64 {
    if pg_float8_gt(left, right) {
        left
    } else {
        right
    }
}

fn checked_mul(left: f64, right: f64) -> Result<f64, ExecError> {
    let result = left * right;
    if result.is_infinite() && !left.is_infinite() && !right.is_infinite() {
        return Err(ExecError::FloatOverflow);
    }
    if result == 0.0 && left != 0.0 && right != 0.0 {
        return Err(ExecError::FloatUnderflow);
    }
    Ok(result)
}

fn checked_div(numer: f64, denom: f64) -> Result<f64, ExecError> {
    if denom == 0.0 && !numer.is_nan() {
        return Err(ExecError::DivisionByZero("/"));
    }
    let result = numer / denom;
    if result.is_infinite() && !numer.is_infinite() {
        return Err(ExecError::FloatOverflow);
    }
    if result == 0.0 && numer != 0.0 && !denom.is_infinite() {
        return Err(ExecError::FloatUnderflow);
    }
    Ok(result)
}

fn checked_mul_add(a: f64, b: f64, c: f64, d: f64) -> Result<f64, ExecError> {
    let left = checked_mul(a, b)?;
    let right = checked_mul(c, d)?;
    checked_sum(left, right)
}

fn checked_mul_sub(a: f64, b: f64, c: f64, d: f64) -> Result<f64, ExecError> {
    let left = checked_mul(a, b)?;
    let right = checked_mul(c, d)?;
    checked_sum(left, -right)
}

fn checked_sum(left: f64, right: f64) -> Result<f64, ExecError> {
    let result = left + right;
    if result.is_infinite() && !left.is_infinite() && !right.is_infinite() {
        return Err(ExecError::FloatOverflow);
    }
    Ok(result)
}

fn invalid_geometry_input(ty: &'static str, value: &str) -> ExecError {
    ExecError::InvalidGeometryInput {
        ty,
        value: value.to_string(),
    }
}

pub(crate) fn geometry_input_error_message(ty: &str, value: &str) -> Option<String> {
    match ty {
        "line" => line_input_error_message(value),
        _ => None,
    }
}

fn line_input_error_message(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        let mut parser = GeometryParser::new(text, "line");
        parser.expect('{').ok()?;
        let a = parser.parse_number().ok()?;
        parser.expect(',').ok()?;
        let b = parser.parse_number().ok()?;
        parser.expect(',').ok()?;
        let _c = parser.parse_number().ok()?;
        parser.expect('}').ok()?;
        parser.finish().ok()?;
        if fp_zero(a) && fp_zero(b) {
            return Some("invalid line specification: A and B cannot both be zero".into());
        }
        return None;
    }

    let mut parser = GeometryParser::new(text, "line");
    let wrapped = parser.consume('[');
    let first = parser.parse_point_pair().ok()?;
    parser.expect(',').ok()?;
    let second = parser.parse_point_pair().ok()?;
    if wrapped {
        parser.expect(']').ok()?;
    }
    parser.finish().ok()?;
    if point_same(&first, &second) {
        return Some("invalid line specification: must be two distinct points".into());
    }
    None
}

fn type_mismatch_unary(op: &'static str, value: &Value) -> Result<Value, ExecError> {
    Err(ExecError::TypeMismatch {
        op,
        left: value.clone(),
        right: Value::Null,
    })
}

fn is_null(value: &Value) -> bool {
    matches!(value, Value::Null)
}

#[derive(Clone, Copy)]
enum GeoTransform {
    Add,
    Sub,
    Mul,
    Div,
}

struct GeometryParser<'a> {
    text: &'a str,
    idx: usize,
    ty: &'static str,
}

impl<'a> GeometryParser<'a> {
    fn new(text: &'a str, ty: &'static str) -> Self {
        Self { text, idx: 0, ty }
    }

    fn finish(&mut self) -> Result<(), ExecError> {
        self.skip_ws();
        if self.idx == self.text.len() {
            Ok(())
        } else {
            Err(invalid_geometry_input(self.ty, self.text))
        }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek() {
            if !ch.is_ascii_whitespace() {
                break;
            }
            self.idx += ch.len_utf8();
        }
    }

    fn peek(&self) -> Option<char> {
        self.text[self.idx..].chars().next()
    }

    fn consume(&mut self, ch: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(ch) {
            self.idx += ch.len_utf8();
            true
        } else {
            false
        }
    }

    fn leading_paren_is_path_wrapper(&mut self) -> bool {
        self.skip_ws();
        if self.peek() != Some('(') {
            return false;
        }

        let rest = &self.text[self.idx + '('.len_utf8()..];
        let trimmed_rest = rest.trim_start_matches(|ch: char| ch.is_ascii_whitespace());
        if trimmed_rest.starts_with('(') || !rest.contains('(') {
            self.idx += '('.len_utf8();
            return true;
        }

        false
    }

    fn expect(&mut self, ch: char) -> Result<(), ExecError> {
        if self.consume(ch) {
            Ok(())
        } else {
            Err(invalid_geometry_input(self.ty, self.text))
        }
    }

    fn parse_number(&mut self) -> Result<f64, ExecError> {
        self.skip_ws();
        let start = self.idx;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_whitespace() || matches!(ch, ',' | ')' | ']' | '>' | '}') {
                break;
            }
            self.idx += ch.len_utf8();
        }
        let token = self.text[start..self.idx].trim();
        if token.is_empty() {
            return Err(invalid_geometry_input(self.ty, self.text));
        }
        parse_pg_float(token, SqlTypeKind::Float8).map_err(|err| match err {
            ExecError::InvalidFloatInput { .. } => invalid_geometry_input(self.ty, self.text),
            other => other,
        })
    }

    fn parse_point_pair(&mut self) -> Result<GeoPoint, ExecError> {
        self.skip_ws();
        let wrapped = self.consume('(');
        let x = self.parse_number()?;
        self.expect(',')?;
        let y = self.parse_number()?;
        if wrapped {
            self.expect(')')?;
        }
        Ok(GeoPoint { x, y })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        circle_to_polygon, eval_geo_closest_point, eval_geo_diagonal, eval_geo_distance,
        eval_geo_intersection, eval_geo_intersects, eval_geo_ne, make_polygon, parse_lseg_text,
        point_path_distance, point_polygon_distance, polygon_contains_polygon,
    };
    use crate::include::nodes::datum::{
        GeoBox, GeoCircle, GeoLine, GeoLseg, GeoPath, GeoPoint, Value,
    };

    #[test]
    fn parse_lseg_accepts_flat_text_input() {
        let lseg = parse_lseg_text("(4.1,4.1,3.1,3.1)").unwrap();
        assert_eq!(lseg.p[0].x, 4.1);
        assert_eq!(lseg.p[0].y, 4.1);
        assert_eq!(lseg.p[1].x, 3.1);
        assert_eq!(lseg.p[1].y, 3.1);
    }

    #[test]
    fn parse_lseg_accepts_unwrapped_point_pairs() {
        let lseg = parse_lseg_text("(0,0),(6,6)").unwrap();
        assert_eq!(lseg.p[0].x, 0.0);
        assert_eq!(lseg.p[0].y, 0.0);
        assert_eq!(lseg.p[1].x, 6.0);
        assert_eq!(lseg.p[1].y, 6.0);
    }

    #[test]
    fn polygon_contains_boundary_touching_regression_rows() {
        let outer = make_polygon(vec![
            GeoPoint { x: 300.0, y: 300.0 },
            GeoPoint { x: 400.0, y: 600.0 },
            GeoPoint { x: 600.0, y: 500.0 },
            GeoPoint { x: 700.0, y: 200.0 },
        ]);

        for (center, radius) in [
            (GeoPoint { x: 480.0, y: 260.0 }, 5.0),
            (GeoPoint { x: 540.0, y: 250.0 }, 10.0),
        ] {
            let inner = circle_to_polygon(12, &GeoCircle { center, radius }).unwrap();
            assert!(polygon_contains_polygon(&outer, &inner));
        }
    }

    #[test]
    fn closed_one_point_path_and_polygon_have_zero_length_segment_distance() {
        let point = GeoPoint { x: 13.0, y: 24.0 };
        let only = GeoPoint { x: 10.0, y: 20.0 };
        let path = GeoPath {
            closed: true,
            points: vec![only.clone()],
        };
        let polygon = make_polygon(vec![only]);

        assert_eq!(point_path_distance(&point, &path), 5.0);
        assert_eq!(point_polygon_distance(&point, &polygon), 5.0);
    }

    #[test]
    fn coincident_lsegs_match_postgres_intersection_and_closest_semantics() {
        let lseg = Value::Lseg(GeoLseg {
            p: [GeoPoint { x: 0.0, y: 0.0 }, GeoPoint { x: 10.0, y: 10.0 }],
        });

        assert_eq!(
            eval_geo_intersection(&[lseg.clone(), lseg.clone()]).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_geo_intersects(&[lseg.clone(), lseg.clone()]).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_geo_distance(&[lseg.clone(), lseg.clone()]).unwrap(),
            Value::Float64(0.0)
        );
        assert_eq!(
            eval_geo_closest_point(&[lseg.clone(), lseg]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn nan_closest_point_returns_sql_null() {
        let result = eval_geo_closest_point(&[
            Value::Point(GeoPoint { x: 1.0, y: 2.0 }),
            Value::Line(GeoLine {
                a: f64::NAN,
                b: -1.0,
                c: 0.0,
            }),
        ])
        .unwrap();

        assert_eq!(result, Value::Null);
    }

    #[test]
    fn circle_nan_not_equal_uses_fpne_semantics() {
        let circle = Value::Circle(GeoCircle {
            center: GeoPoint { x: 0.0, y: 0.0 },
            radius: f64::NAN,
        });

        assert_eq!(
            eval_geo_ne(&[circle.clone(), circle]).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn box_diagonal_renders_high_to_low() {
        let diagonal = eval_geo_diagonal(&[Value::Box(GeoBox {
            high: GeoPoint { x: 3.0, y: 4.0 },
            low: GeoPoint { x: 1.0, y: 2.0 },
        })])
        .unwrap();

        assert_eq!(
            diagonal,
            Value::Lseg(GeoLseg {
                p: [GeoPoint { x: 3.0, y: 4.0 }, GeoPoint { x: 1.0, y: 2.0 },],
            })
        );
    }
}
