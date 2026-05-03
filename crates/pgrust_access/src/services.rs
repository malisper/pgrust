use std::cmp::Ordering;

use pgrust_core::{
    CommandId, INVALID_TRANSACTION_ID, InterruptReason, Lsn, RelFileLocator, Snapshot,
    TransactionId, TransactionStatus,
};
use pgrust_nodes::SqlType;
use pgrust_nodes::datum::{
    ArrayValue, GeoBox, GeoPoint, GeoPolygon, InetValue, MultirangeValue, RangeBound, RangeValue,
    Value,
};
use pgrust_nodes::primnodes::{BuiltinScalarFunction, ColumnDesc};
use pgrust_nodes::relcache::IndexRelCacheEntry;
use pgrust_nodes::tsearch::{TsQuery, TsVector};
use pgrust_storage::BufferTag;

use crate::AccessResult;
use crate::access::gin::GinEntryKey;
use crate::access::htup::HeapTuple;
use crate::access::htup::TupleValue;
use crate::access::itemptr::ItemPointerData;

pub trait AccessInterruptServices {
    fn check_interrupts(&self) -> Result<(), InterruptReason>;
}

pub trait AccessTransactionServices {
    fn transaction_status(&self, xid: TransactionId) -> Option<TransactionStatus>;

    fn oldest_active_xid(&self) -> TransactionId {
        INVALID_TRANSACTION_ID
    }

    fn combo_command_pair(
        &self,
        xid: TransactionId,
        combocid: CommandId,
    ) -> Option<(CommandId, CommandId)>;

    fn wait_for_transaction(&self, xid: TransactionId) -> AccessResult<()>;
}

pub trait AccessHeapServices {
    fn for_each_heap_tuple(
        &self,
        rel: RelFileLocator,
        visit: &mut dyn FnMut(ItemPointerData, HeapTuple) -> AccessResult<()>,
    ) -> AccessResult<u64>;

    fn for_each_visible_heap_tuple(
        &self,
        rel: RelFileLocator,
        snapshot: Snapshot,
        visit: &mut dyn FnMut(ItemPointerData, HeapTuple) -> AccessResult<()>,
    ) -> AccessResult<u64>;

    fn fetch_heap_tuple(
        &self,
        rel: RelFileLocator,
        tid: ItemPointerData,
    ) -> AccessResult<HeapTuple>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessWalBlockRef {
    pub tag: BufferTag,
    pub flags: u8,
    pub data: Vec<u8>,
    pub buffer_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessWalRecord {
    pub xid: TransactionId,
    pub rmid: u8,
    pub info: u8,
    pub payload: Vec<u8>,
    pub blocks: Vec<AccessWalBlockRef>,
}

pub trait AccessWalServices {
    fn log_access_record(&self, record: AccessWalRecord) -> AccessResult<Lsn>;
}

pub trait AccessScalarServices {
    fn compare_order_values(
        &self,
        left: &Value,
        right: &Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> AccessResult<Ordering>;

    fn encode_value(&self, column: &ColumnDesc, value: &Value) -> AccessResult<TupleValue>;

    fn decode_value(&self, column: &ColumnDesc, raw: Option<&[u8]>) -> AccessResult<Value>;

    fn format_unique_key_detail(&self, columns: &[ColumnDesc], values: &[Value]) -> String;

    fn format_vector_array_storage_text(
        &self,
        sql_type: SqlType,
        array: &ArrayValue,
    ) -> AccessResult<String>;

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

    fn hash_index_value(&self, value: &Value, opclass: Option<u32>) -> AccessResult<Option<u32>>;

    fn hash_values_equal(&self, left: &Value, right: &Value, opclass: Option<u32>) -> bool;

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

pub trait AccessIndexServices {
    fn project_index_row(
        &mut self,
        index_meta: &IndexRelCacheEntry,
        row_values: &[Value],
        heap_tid: ItemPointerData,
    ) -> AccessResult<Option<Vec<Value>>>;
}

pub trait AccessToastServices {
    fn resolve_external_toast(&self, value: &Value) -> AccessResult<Value>;
}
