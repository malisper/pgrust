use std::cmp::Ordering;

use pgrust_nodes::datum::{
    GeoBox, GeoPoint, GeoPolygon, InetValue, MultirangeValue, RangeBound, RangeValue, Value,
};
use pgrust_nodes::primnodes::BuiltinScalarFunction;
use pgrust_nodes::tsearch::{TsQuery, TsVector};

use crate::AccessResult;
use crate::access::gin::GinEntryKey;

pub trait AccessScalarServices {
    fn compare_order_values(
        &self,
        left: &Value,
        right: &Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> AccessResult<Ordering>;

    fn compare_range_values(&self, left: &RangeValue, right: &RangeValue) -> Ordering;

    fn compare_multirange_values(
        &self,
        left: &MultirangeValue,
        right: &MultirangeValue,
    ) -> Ordering;

    fn compare_scalar_values(&self, left: &Value, right: &Value) -> Ordering;

    fn compare_lower_bounds(
        &self,
        left: Option<&RangeBound>,
        right: Option<&RangeBound>,
    ) -> Ordering;

    fn compare_upper_bounds(
        &self,
        left: Option<&RangeBound>,
        right: Option<&RangeBound>,
    ) -> Ordering;

    fn range_adjacent(&self, left: &RangeValue, right: &RangeValue) -> bool;

    fn range_contains_element(&self, range: &RangeValue, value: &Value) -> AccessResult<bool>;

    fn range_contains_range(&self, outer: &RangeValue, inner: &RangeValue) -> bool;

    fn range_merge(&self, left: &RangeValue, right: &RangeValue) -> RangeValue;

    fn range_over_left_bounds(&self, left: &RangeValue, right: &RangeValue) -> bool;

    fn range_over_right_bounds(&self, left: &RangeValue, right: &RangeValue) -> bool;

    fn range_overlap(&self, left: &RangeValue, right: &RangeValue) -> bool;

    fn range_strict_left(&self, left: &RangeValue, right: &RangeValue) -> bool;

    fn range_strict_right(&self, left: &RangeValue, right: &RangeValue) -> bool;

    fn eval_multirange_bool(
        &self,
        func: BuiltinScalarFunction,
        key: &Value,
        query: &Value,
    ) -> AccessResult<bool>;

    fn span_multirange(&self, multirange: &MultirangeValue) -> RangeValue;

    fn multirange_from_range(&self, range: &RangeValue) -> AccessResult<MultirangeValue>;

    fn multirange_adjacent_multirange(
        &self,
        left: &MultirangeValue,
        right: &MultirangeValue,
    ) -> bool;

    fn multirange_adjacent_range(&self, left: &MultirangeValue, right: &RangeValue) -> bool;

    fn multirange_contains_element(
        &self,
        multirange: &MultirangeValue,
        value: &Value,
    ) -> AccessResult<bool>;

    fn multirange_contains_multirange(
        &self,
        outer: &MultirangeValue,
        inner: &MultirangeValue,
    ) -> bool;

    fn multirange_contains_range(&self, outer: &MultirangeValue, inner: &RangeValue) -> bool;

    fn multirange_overlaps_multirange(
        &self,
        left: &MultirangeValue,
        right: &MultirangeValue,
    ) -> bool;

    fn multirange_overlaps_range(&self, left: &MultirangeValue, right: &RangeValue) -> bool;

    fn range_contains_multirange(&self, range: &RangeValue, multirange: &MultirangeValue) -> bool;

    fn compare_network_values(&self, left: &InetValue, right: &InetValue) -> Ordering;

    fn network_contains(&self, container: &InetValue, value: &InetValue, strict: bool) -> bool;

    fn network_merge(&self, left: &InetValue, right: &InetValue) -> InetValue;

    fn compare_tsquery(&self, left: &TsQuery, right: &TsQuery) -> Ordering;

    fn compare_tsvector(&self, left: &TsVector, right: &TsVector) -> Ordering;

    fn compare_jsonb_bytes(&self, left: &[u8], right: &[u8]) -> Option<Ordering>;

    fn gin_jsonb_entries(&self, attnum: u16, bytes: &[u8]) -> AccessResult<Vec<GinEntryKey>>;

    fn bound_box(&self, left: &GeoBox, right: &GeoBox) -> GeoBox;

    fn box_area(&self, geo_box: &GeoBox) -> f64;

    fn box_overlap(&self, left: &GeoBox, right: &GeoBox) -> bool;

    fn box_contains_box(&self, outer: &GeoBox, inner: &GeoBox) -> bool;

    fn box_contains_point(&self, geo_box: &GeoBox, point: &GeoPoint) -> bool;

    fn box_same(&self, left: &GeoBox, right: &GeoBox) -> bool;

    fn box_box_distance(&self, left: &GeoBox, right: &GeoBox) -> f64;

    fn polygon_overlap(&self, left: &GeoPolygon, right: &GeoPolygon) -> bool;

    fn polygon_same(&self, left: &GeoPolygon, right: &GeoPolygon) -> bool;

    fn polygon_contains_polygon(&self, outer: &GeoPolygon, inner: &GeoPolygon) -> bool;

    fn point_in_polygon(&self, point: &GeoPoint, poly: &GeoPolygon) -> i32;

    fn point_polygon_distance(&self, point: &GeoPoint, poly: &GeoPolygon) -> f64;
}

pub trait AccessIndexServices {}

pub trait AccessToastServices {}
