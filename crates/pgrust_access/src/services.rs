use std::cmp::Ordering;

use pgrust_nodes::datum::{
    GeoBox, GeoPoint, GeoPolygon, InetValue, MultirangeValue, RangeValue, Value,
};
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

    fn box_same(&self, left: &GeoBox, right: &GeoBox) -> bool;

    fn box_box_distance(&self, left: &GeoBox, right: &GeoBox) -> f64;

    fn point_in_polygon(&self, point: &GeoPoint, poly: &GeoPolygon) -> i32;
}

pub trait AccessIndexServices {}

pub trait AccessToastServices {}
