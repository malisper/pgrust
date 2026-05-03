use std::cmp::Ordering;

use pgrust_access::access::gin::GinEntryKey;
use pgrust_access::access::htup::TupleValue;
use pgrust_access::gin::jsonb_ops::{
    JGINFLAG_BOOL, JGINFLAG_NULL, JGINFLAG_NUM, JGINFLAG_STR, scalar_key, text_key,
};
use pgrust_access::{AccessError, AccessResult, AccessScalarServices};
use pgrust_nodes::datum::{
    GeoBox, GeoPoint, GeoPolygon, InetValue, MultirangeValue, RangeBound, RangeValue, Value,
};
use pgrust_nodes::primnodes::{BuiltinScalarFunction, ColumnDesc};
use pgrust_nodes::tsearch::{TsQuery, TsVector};

pub(crate) struct RootAccessServices;

impl AccessScalarServices for RootAccessServices {
    fn compare_order_values(
        &self,
        left: &Value,
        right: &Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> AccessResult<Ordering> {
        crate::backend::executor::compare_order_values(
            left,
            right,
            collation_oid,
            nulls_first,
            descending,
        )
        .map_err(|err| AccessError::Scalar(format!("{err:?}")))
    }

    fn encode_value(&self, column: &ColumnDesc, value: &Value) -> AccessResult<TupleValue> {
        crate::backend::executor::value_io::encode_value(column, value)
            .map_err(|err| AccessError::Scalar(format!("{err:?}")))
    }

    fn decode_value(&self, column: &ColumnDesc, raw: Option<&[u8]>) -> AccessResult<Value> {
        crate::backend::executor::value_io::decode_value(column, raw)
            .map_err(|err| AccessError::Scalar(format!("{err:?}")))
    }

    fn compare_range_values(&self, left: &RangeValue, right: &RangeValue) -> Ordering {
        crate::backend::executor::compare_range_values(left, right)
    }

    fn compare_multirange_values(
        &self,
        left: &MultirangeValue,
        right: &MultirangeValue,
    ) -> Ordering {
        crate::backend::executor::compare_multirange_values(left, right)
    }

    fn compare_scalar_values(&self, left: &Value, right: &Value) -> Ordering {
        crate::backend::executor::expr_range::compare_scalar_values(left, right)
    }

    fn compare_lower_bounds(
        &self,
        left: Option<&RangeBound>,
        right: Option<&RangeBound>,
    ) -> Ordering {
        crate::backend::executor::expr_range::compare_lower_bounds(left, right)
    }

    fn compare_upper_bounds(
        &self,
        left: Option<&RangeBound>,
        right: Option<&RangeBound>,
    ) -> Ordering {
        crate::backend::executor::expr_range::compare_upper_bounds(left, right)
    }

    fn range_adjacent(&self, left: &RangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_adjacent(left, right)
    }

    fn range_contains_element(&self, range: &RangeValue, value: &Value) -> AccessResult<bool> {
        crate::backend::executor::expr_range::range_contains_element(range, value)
            .map_err(|err| AccessError::Scalar(format!("{err:?}")))
    }

    fn range_contains_range(&self, outer: &RangeValue, inner: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_contains_range(outer, inner)
    }

    fn range_merge(&self, left: &RangeValue, right: &RangeValue) -> RangeValue {
        crate::backend::executor::expr_range::range_merge(left, right)
    }

    fn range_over_left_bounds(&self, left: &RangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_over_left_bounds(left, right)
    }

    fn range_over_right_bounds(&self, left: &RangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_over_right_bounds(left, right)
    }

    fn range_overlap(&self, left: &RangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_overlap(left, right)
    }

    fn range_strict_left(&self, left: &RangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_strict_left(left, right)
    }

    fn range_strict_right(&self, left: &RangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_range::range_strict_right(left, right)
    }

    fn eval_multirange_bool(
        &self,
        func: BuiltinScalarFunction,
        key: &Value,
        query: &Value,
    ) -> AccessResult<bool> {
        let value = crate::backend::executor::expr_multirange::eval_multirange_function(
            func,
            &[key.clone(), query.clone()],
            None,
            false,
        )
        .ok_or(AccessError::Corrupt(
            "unsupported access multirange function",
        ))?
        .map_err(|err| AccessError::Scalar(format!("{err:?}")))?;
        match value {
            Value::Bool(value) => Ok(value),
            other => Err(AccessError::Scalar(format!(
                "access multirange expected bool, got {other:?}"
            ))),
        }
    }

    fn span_multirange(&self, multirange: &MultirangeValue) -> RangeValue {
        crate::backend::executor::expr_multirange::span_multirange(multirange)
    }

    fn multirange_from_range(&self, range: &RangeValue) -> AccessResult<MultirangeValue> {
        crate::backend::executor::expr_multirange::multirange_from_range(range)
            .map_err(|err| AccessError::Scalar(format!("{err:?}")))
    }

    fn multirange_adjacent_multirange(
        &self,
        left: &MultirangeValue,
        right: &MultirangeValue,
    ) -> bool {
        crate::backend::executor::expr_multirange::multirange_adjacent_multirange(left, right)
    }

    fn multirange_adjacent_range(&self, left: &MultirangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_multirange::multirange_adjacent_range(left, right)
    }

    fn multirange_contains_element(
        &self,
        multirange: &MultirangeValue,
        value: &Value,
    ) -> AccessResult<bool> {
        crate::backend::executor::expr_multirange::multirange_contains_element(multirange, value)
            .map_err(|err| AccessError::Scalar(format!("{err:?}")))
    }

    fn multirange_contains_multirange(
        &self,
        outer: &MultirangeValue,
        inner: &MultirangeValue,
    ) -> bool {
        crate::backend::executor::expr_multirange::multirange_contains_multirange(outer, inner)
    }

    fn multirange_contains_range(&self, outer: &MultirangeValue, inner: &RangeValue) -> bool {
        crate::backend::executor::expr_multirange::multirange_contains_range(outer, inner)
    }

    fn multirange_overlaps_multirange(
        &self,
        left: &MultirangeValue,
        right: &MultirangeValue,
    ) -> bool {
        crate::backend::executor::expr_multirange::multirange_overlaps_multirange(left, right)
    }

    fn multirange_overlaps_range(&self, left: &MultirangeValue, right: &RangeValue) -> bool {
        crate::backend::executor::expr_multirange::multirange_overlaps_range(left, right)
    }

    fn range_contains_multirange(&self, range: &RangeValue, multirange: &MultirangeValue) -> bool {
        crate::backend::executor::expr_multirange::range_contains_multirange(range, multirange)
    }

    fn compare_network_values(&self, left: &InetValue, right: &InetValue) -> Ordering {
        crate::backend::executor::compare_network_values(left, right)
    }

    fn network_contains(&self, container: &InetValue, value: &InetValue, strict: bool) -> bool {
        crate::backend::executor::network_contains(container, value, strict)
    }

    fn network_merge(&self, left: &InetValue, right: &InetValue) -> InetValue {
        crate::backend::executor::network_merge(left, right)
    }

    fn compare_tsquery(&self, left: &TsQuery, right: &TsQuery) -> Ordering {
        crate::backend::executor::compare_tsquery(left, right)
    }

    fn compare_tsvector(&self, left: &TsVector, right: &TsVector) -> Ordering {
        crate::backend::executor::compare_tsvector(left, right)
    }

    fn compare_jsonb_bytes(&self, left: &[u8], right: &[u8]) -> Option<Ordering> {
        let left = crate::backend::executor::jsonb::decode_jsonb(left).ok()?;
        let right = crate::backend::executor::jsonb::decode_jsonb(right).ok()?;
        Some(crate::backend::executor::jsonb::compare_jsonb(
            &left, &right,
        ))
    }

    fn gin_jsonb_entries(&self, attnum: u16, bytes: &[u8]) -> AccessResult<Vec<GinEntryKey>> {
        let jsonb = crate::backend::executor::jsonb::decode_jsonb(bytes)
            .map_err(|err| AccessError::Scalar(format!("GIN jsonb decode failed: {err:?}")))?;
        let mut entries = Vec::new();
        extract_jsonb_entries(attnum, &jsonb, &mut entries);
        Ok(entries)
    }

    fn bound_box(&self, left: &GeoBox, right: &GeoBox) -> GeoBox {
        crate::backend::executor::expr_geometry::bound_box(left, right)
    }

    fn box_area(&self, geo_box: &GeoBox) -> f64 {
        crate::backend::executor::expr_geometry::box_area(geo_box)
    }

    fn box_overlap(&self, left: &GeoBox, right: &GeoBox) -> bool {
        crate::backend::executor::expr_geometry::box_overlap(left, right)
    }

    fn box_contains_box(&self, outer: &GeoBox, inner: &GeoBox) -> bool {
        crate::backend::executor::expr_geometry::box_contains_box(outer, inner)
    }

    fn box_contains_point(&self, geo_box: &GeoBox, point: &GeoPoint) -> bool {
        crate::backend::executor::expr_geometry::box_contains_point(geo_box, point)
    }

    fn box_same(&self, left: &GeoBox, right: &GeoBox) -> bool {
        crate::backend::executor::expr_geometry::box_same(left, right)
    }

    fn box_box_distance(&self, left: &GeoBox, right: &GeoBox) -> f64 {
        crate::backend::executor::expr_geometry::box_box_distance(left, right)
    }

    fn polygon_overlap(&self, left: &GeoPolygon, right: &GeoPolygon) -> bool {
        crate::backend::executor::expr_geometry::polygon_overlap(left, right)
    }

    fn polygon_same(&self, left: &GeoPolygon, right: &GeoPolygon) -> bool {
        crate::backend::executor::expr_geometry::polygon_same(left, right)
    }

    fn polygon_contains_polygon(&self, outer: &GeoPolygon, inner: &GeoPolygon) -> bool {
        crate::backend::executor::expr_geometry::polygon_contains_polygon(outer, inner)
    }

    fn point_in_polygon(&self, point: &GeoPoint, poly: &GeoPolygon) -> i32 {
        crate::backend::executor::expr_geometry::point_in_polygon(point, poly)
    }

    fn point_polygon_distance(&self, point: &GeoPoint, poly: &GeoPolygon) -> f64 {
        crate::backend::executor::expr_geometry::point_polygon_distance(point, poly)
    }
}

fn extract_jsonb_entries(
    attnum: u16,
    value: &crate::backend::executor::jsonb::JsonbValue,
    out: &mut Vec<GinEntryKey>,
) {
    use crate::backend::executor::jsonb::{JsonbValue, render_temporal_jsonb_value};

    match value {
        JsonbValue::Object(items) => {
            for (key, child) in items {
                out.push(text_key(attnum, key));
                extract_jsonb_entries(attnum, child, out);
            }
        }
        JsonbValue::Array(items) => {
            for child in items {
                if let JsonbValue::String(text) = child {
                    out.push(text_key(attnum, text));
                } else {
                    extract_jsonb_entries(attnum, child, out);
                }
            }
        }
        JsonbValue::Null => out.push(scalar_key(attnum, JGINFLAG_NULL, "")),
        JsonbValue::Bool(value) => out.push(scalar_key(
            attnum,
            JGINFLAG_BOOL,
            if *value { "true" } else { "false" },
        )),
        JsonbValue::Numeric(value) => out.push(scalar_key(
            attnum,
            JGINFLAG_NUM,
            &value.normalize_display_scale().render(),
        )),
        JsonbValue::String(value) => out.push(scalar_key(attnum, JGINFLAG_STR, value)),
        JsonbValue::Date(_)
        | JsonbValue::Time(_)
        | JsonbValue::TimeTz(_)
        | JsonbValue::Timestamp(_)
        | JsonbValue::TimestampTz(_)
        | JsonbValue::TimestampTzWithOffset(_, _) => out.push(scalar_key(
            attnum,
            JGINFLAG_STR,
            &render_temporal_jsonb_value(value),
        )),
    }
}
