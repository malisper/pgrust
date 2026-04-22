use std::cmp::Ordering;

use crate::backend::executor::{compare_multirange_values, compare_order_values};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;

pub fn compare_bt_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int16(a), Value::Int16(b)) => a.cmp(b),
        (Value::Int16(a), Value::Int32(b)) => i32::from(*a).cmp(b),
        (Value::Int16(a), Value::Int64(b)) => i64::from(*a).cmp(b),
        (Value::Int32(a), Value::Int16(b)) => a.cmp(&i32::from(*b)),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int64(b)) => i64::from(*a).cmp(b),
        (Value::Int64(a), Value::Int16(b)) => a.cmp(&i64::from(*b)),
        (Value::Int64(a), Value::Int32(b)) => a.cmp(&i64::from(*b)),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::TextRef(_, _), Value::TextRef(_, _)) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
        (Value::Bit(a), Value::Bit(b)) => a
            .bytes
            .cmp(&b.bytes)
            .then_with(|| a.bit_len.cmp(&b.bit_len)),
        (Value::Array(_) | Value::PgArray(_), Value::Array(_) | Value::PgArray(_)) => {
            compare_order_values(left, right, None, None, false)
                .expect("btree array comparisons use implicit default collation")
        }
        (Value::Multirange(a), Value::Multirange(b)) => compare_multirange_values(a, b),
        (Value::Numeric(a), Value::Numeric(b)) => a.render().cmp(&b.render()),
        (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
        _ => Ordering::Equal,
    }
}

pub fn compare_item_pointers(left: &ItemPointerData, right: &ItemPointerData) -> Ordering {
    left.block_number
        .cmp(&right.block_number)
        .then_with(|| left.offset_number.cmp(&right.offset_number))
}

pub fn compare_bt_keyspace(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
) -> Ordering {
    for (left, right) in left_keys.iter().zip(right_keys) {
        let ord = compare_bt_values(left, right);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    compare_item_pointers(left_tid, right_tid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bt_keyspace_uses_heap_tid_as_final_tiebreak() {
        let a = ItemPointerData {
            block_number: 1,
            offset_number: 2,
        };
        let b = ItemPointerData {
            block_number: 1,
            offset_number: 3,
        };
        assert_eq!(
            compare_bt_keyspace(&[Value::Int32(10)], &a, &[Value::Int32(10)], &b),
            Ordering::Less
        );
    }
}
