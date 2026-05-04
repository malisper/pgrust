use super::ExecError;
use super::node_types::{
    BuiltinScalarFunction, GeoBox, GeoCircle, GeoPath, GeoPoint, GeoPolygon, SqlType, Value,
};
use crate::backend::libpq::pqformat::FloatFormatOptions;
use crate::backend::parser::SqlTypeKind;

pub(crate) const GEOMETRY_EPSILON: f64 = pgrust_expr::expr_geometry::GEOMETRY_EPSILON;

// :HACK: Keep the historical root executor module path while geometry scalar
// helpers live in `pgrust_expr`.
pub(crate) fn parse_geometry_text(text: &str, ty: SqlTypeKind) -> Result<Value, ExecError> {
    pgrust_expr::expr_geometry::parse_geometry_text(text, ty).map_err(Into::into)
}

pub(crate) fn render_geometry_text(value: &Value, options: FloatFormatOptions) -> Option<String> {
    pgrust_expr::expr_geometry::render_geometry_text(value, options)
}

pub(crate) fn cast_geometry_value(value: Value, ty: SqlType) -> Option<Result<Value, ExecError>> {
    pgrust_expr::expr_geometry::cast_geometry_value(value, ty)
        .map(|result| result.map_err(Into::into))
}

pub(crate) fn encode_path_bytes(path: &GeoPath) -> Vec<u8> {
    pgrust_expr::expr_geometry::encode_path_bytes(path)
}

pub(crate) fn decode_path_bytes(bytes: &[u8]) -> Result<GeoPath, ExecError> {
    pgrust_expr::expr_geometry::decode_path_bytes(bytes).map_err(Into::into)
}

pub(crate) fn encode_polygon_bytes(poly: &GeoPolygon) -> Vec<u8> {
    pgrust_expr::expr_geometry::encode_polygon_bytes(poly)
}

pub(crate) fn decode_polygon_bytes(bytes: &[u8]) -> Result<GeoPolygon, ExecError> {
    pgrust_expr::expr_geometry::decode_polygon_bytes(bytes).map_err(Into::into)
}

pub(crate) fn eval_geometry_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Option<Result<Value, ExecError>> {
    pgrust_expr::expr_geometry::eval_geometry_function(func, values)
        .map(|result| result.map_err(Into::into))
}

pub(crate) fn box_area(geo_box: &GeoBox) -> f64 {
    pgrust_expr::expr_geometry::box_area(geo_box)
}

pub(crate) fn bound_box(left: &GeoBox, right: &GeoBox) -> GeoBox {
    pgrust_expr::expr_geometry::bound_box(left, right)
}

pub(crate) fn box_same(left: &GeoBox, right: &GeoBox) -> bool {
    pgrust_expr::expr_geometry::box_same(left, right)
}

pub(crate) fn box_overlap(left: &GeoBox, right: &GeoBox) -> bool {
    pgrust_expr::expr_geometry::box_overlap(left, right)
}

pub(crate) fn box_contains_box(outer: &GeoBox, inner: &GeoBox) -> bool {
    pgrust_expr::expr_geometry::box_contains_box(outer, inner)
}

pub(crate) fn box_contains_point(geo_box: &GeoBox, point: &GeoPoint) -> bool {
    pgrust_expr::expr_geometry::box_contains_point(geo_box, point)
}

pub(crate) fn circle_bound_box(circle: &GeoCircle) -> GeoBox {
    pgrust_expr::expr_geometry::circle_bound_box(circle)
}

pub(crate) fn polygon_same(left: &GeoPolygon, right: &GeoPolygon) -> bool {
    pgrust_expr::expr_geometry::polygon_same(left, right)
}

pub(crate) fn polygon_overlap(left: &GeoPolygon, right: &GeoPolygon) -> bool {
    pgrust_expr::expr_geometry::polygon_overlap(left, right)
}

pub(crate) fn polygon_contains_polygon(outer: &GeoPolygon, inner: &GeoPolygon) -> bool {
    pgrust_expr::expr_geometry::polygon_contains_polygon(outer, inner)
}

pub(crate) fn point_in_polygon(point: &GeoPoint, poly: &GeoPolygon) -> i32 {
    pgrust_expr::expr_geometry::point_in_polygon(point, poly)
}

pub(crate) fn point_polygon_distance(point: &GeoPoint, poly: &GeoPolygon) -> f64 {
    pgrust_expr::expr_geometry::point_polygon_distance(point, poly)
}

pub(crate) fn box_box_distance(left: &GeoBox, right: &GeoBox) -> f64 {
    pgrust_expr::expr_geometry::box_box_distance(left, right)
}

pub(crate) fn geometry_input_error_message(ty: &str, value: &str) -> Option<String> {
    pgrust_expr::expr_geometry::geometry_input_error_message(ty, value)
}
