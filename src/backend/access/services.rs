use std::cmp::Ordering;

use pgrust_access::access::gin::GinEntryKey;
use pgrust_access::gin::jsonb_ops::{
    JGINFLAG_BOOL, JGINFLAG_NULL, JGINFLAG_NUM, JGINFLAG_STR, scalar_key, text_key,
};
use pgrust_access::{AccessError, AccessResult, AccessScalarServices};
use pgrust_nodes::datum::{
    GeoBox, GeoPoint, GeoPolygon, InetValue, MultirangeValue, RangeValue, Value,
};
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

    fn box_same(&self, left: &GeoBox, right: &GeoBox) -> bool {
        crate::backend::executor::expr_geometry::box_same(left, right)
    }

    fn box_box_distance(&self, left: &GeoBox, right: &GeoBox) -> f64 {
        crate::backend::executor::expr_geometry::box_box_distance(left, right)
    }

    fn point_in_polygon(&self, point: &GeoPoint, poly: &GeoPolygon) -> i32 {
        crate::backend::executor::expr_geometry::point_in_polygon(point, poly)
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
