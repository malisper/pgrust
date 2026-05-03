use std::cmp::Ordering;

use pgrust_nodes::datum::{InetValue, MultirangeValue, RangeValue, Value};
use pgrust_nodes::tsearch::{TsQuery, TsVector};

use crate::AccessResult;

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
}

pub trait AccessIndexServices {}

pub trait AccessToastServices {}
