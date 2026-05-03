use std::cmp::Ordering;

use pgrust_access::{AccessError, AccessResult, AccessScalarServices};
use pgrust_nodes::datum::{InetValue, MultirangeValue, RangeValue, Value};
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
}
