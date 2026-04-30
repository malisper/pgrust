use std::cmp::Ordering;

use crate::backend::executor::expr_range::compare_range_values;
use crate::backend::executor::{
    compare_multirange_values, compare_network_values, compare_order_values, compare_tsquery,
    compare_tsvector,
};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::{NumericValue, Value};

pub const BT_DESC_FLAG: i16 = 0x0001;
pub const BT_NULLS_FIRST_FLAG: i16 = 0x0002;

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
        (Value::EnumOid(a), Value::EnumOid(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int64(b)) => i64::from(*a).cmp(b),
        (Value::Int64(a), Value::Int16(b)) => a.cmp(&i64::from(*b)),
        (Value::Int64(a), Value::Int32(b)) => a.cmp(&i64::from(*b)),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Xid8(a), Value::Xid8(b)) => a.cmp(b),
        (Value::Tid(a), Value::Tid(b)) => a.cmp(b),
        (Value::Int16(a), Value::Float64(b)) => pg_float_cmp(f64::from(*a), *b),
        (Value::Int32(a), Value::Float64(b)) => pg_float_cmp(f64::from(*a), *b),
        (Value::Int64(a), Value::Float64(b)) => pg_float_cmp(*a as f64, *b),
        (Value::Float64(a), Value::Int16(b)) => pg_float_cmp(*a, f64::from(*b)),
        (Value::Float64(a), Value::Int32(b)) => pg_float_cmp(*a, f64::from(*b)),
        (Value::Float64(a), Value::Int64(b)) => pg_float_cmp(*a, *b as f64),
        (Value::Text(_) | Value::TextRef(_, _), Value::Text(_) | Value::TextRef(_, _)) => left
            .as_text()
            .expect("text-family btree value should expose text")
            .cmp(
                right
                    .as_text()
                    .expect("text-family btree value should expose text"),
            ),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
        (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
        (Value::Bit(a), Value::Bit(b)) => a
            .bytes
            .cmp(&b.bytes)
            .then_with(|| a.bit_len.cmp(&b.bit_len)),
        (Value::Array(_) | Value::PgArray(_), Value::Array(_) | Value::PgArray(_)) => {
            compare_order_values(left, right, None, None, false)
                .expect("btree array comparisons use implicit default collation")
        }
        (Value::InternalChar(a), Value::InternalChar(b)) => a.cmp(b),
        (Value::Range(a), Value::Range(b)) => compare_range_values(a, b),
        (Value::Interval(a), Value::Interval(b)) => a.cmp_key().cmp(&b.cmp_key()),
        (Value::Multirange(a), Value::Multirange(b)) => compare_multirange_values(a, b),
        (Value::TsQuery(a), Value::TsQuery(b)) => compare_tsquery(a, b),
        (Value::TsVector(a), Value::TsVector(b)) => compare_tsvector(a, b),
        (Value::Inet(a) | Value::Cidr(a), Value::Inet(b) | Value::Cidr(b)) => {
            compare_network_values(a, b)
        }
        (Value::Record(_), Value::Record(_)) => {
            compare_order_values(left, right, None, None, false)
                .expect("btree record comparisons use implicit default collation")
        }
        (a, b) if numeric_key_value(a).is_some() && numeric_key_value(b).is_some() => {
            numeric_key_value(a)
                .unwrap()
                .cmp(&numeric_key_value(b).unwrap())
        }
        (Value::Float64(a), Value::Float64(b)) => pg_float_cmp(*a, *b),
        _ => Ordering::Equal,
    }
}

fn pg_float_cmp(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

pub fn compare_bt_values_with_options(left: &Value, right: &Value, option: i16) -> Ordering {
    let nulls_first = option & BT_NULLS_FIRST_FLAG != 0;
    let ord = match (left, right) {
        (Value::Null, Value::Null) => return Ordering::Equal,
        (Value::Null, _) => {
            return if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (_, Value::Null) => {
            return if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        _ => compare_bt_values(left, right),
    };
    if option & BT_DESC_FLAG != 0 {
        ord.reverse()
    } else {
        ord
    }
}

fn numeric_key_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int32(value) => Some(NumericValue::from_i64(i64::from(*value))),
        Value::Int64(value) => Some(NumericValue::from_i64(*value)),
        Value::Xid8(value) => Some(NumericValue::finite((*value).into(), 0)),
        Value::Numeric(value) => Some(value.clone()),
        _ => None,
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
    compare_bt_keyspace_with_options(left_keys, left_tid, right_keys, right_tid, &[])
}

pub fn compare_bt_keyspace_with_options(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
    indoption: &[i16],
) -> Ordering {
    for (index, (left, right)) in left_keys.iter().zip(right_keys).enumerate() {
        let option = indoption.get(index).copied().unwrap_or_default();
        let ord = compare_bt_values_with_options(left, right, option);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    compare_item_pointers(left_tid, right_tid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::expr_range::parse_range_text;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::nodes::datum::{ArrayDimension, ArrayValue};

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

    #[test]
    fn bt_keyspace_honors_desc_and_nulls_first_options() {
        let a = ItemPointerData {
            block_number: 1,
            offset_number: 1,
        };
        let b = ItemPointerData {
            block_number: 1,
            offset_number: 2,
        };
        assert_eq!(
            compare_bt_keyspace_with_options(
                &[Value::Int32(10)],
                &a,
                &[Value::Int32(5)],
                &b,
                &[BT_DESC_FLAG],
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_bt_keyspace_with_options(
                &[Value::Null],
                &a,
                &[Value::Int32(5)],
                &b,
                &[BT_NULLS_FIRST_FLAG],
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_bt_keyspace_with_options(
                &[Value::Null],
                &a,
                &[Value::Int32(5)],
                &b,
                &[BT_DESC_FLAG | BT_NULLS_FIRST_FLAG],
            ),
            Ordering::Less
        );
    }

    #[test]
    fn bt_compare_orders_text_search_values() {
        let left =
            Value::TsQuery(crate::include::nodes::tsearch::TsQuery::parse("moscow").unwrap());
        let right =
            Value::TsQuery(crate::include::nodes::tsearch::TsQuery::parse("new <-> york").unwrap());
        assert_ne!(compare_bt_values(&left, &right), Ordering::Equal);

        let left =
            Value::TsVector(crate::include::nodes::tsearch::TsVector::parse("'aaa':1").unwrap());
        let right =
            Value::TsVector(crate::include::nodes::tsearch::TsVector::parse("'bbb':1").unwrap());
        assert_eq!(compare_bt_values(&left, &right), Ordering::Less);
    }

    #[test]
    fn bt_array_comparison_uses_pg_array_shape_rules() {
        let lower_bounds_first = Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 0,
                length: 2,
            }],
            vec![Value::Int32(1), Value::Int32(2)],
        ));
        let lower_bounds_second = Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 2,
            }],
            vec![Value::Int32(1), Value::Int32(2)],
        ));
        assert_eq!(
            compare_bt_values(&lower_bounds_first, &lower_bounds_second),
            Ordering::Less
        );

        let with_null = Value::PgArray(ArrayValue::from_1d(vec![Value::Int32(1), Value::Null]));
        let without_null =
            Value::PgArray(ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)]));
        assert_eq!(
            compare_bt_values(&with_null, &without_null),
            Ordering::Greater
        );

        let array_ten = Value::PgArray(ArrayValue::from_1d(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(10),
        ]));
        let array_five = Value::PgArray(ArrayValue::from_1d(vec![
            Value::Int32(1),
            Value::Int32(5),
            Value::Int32(3),
        ]));
        assert_eq!(compare_bt_values(&array_ten, &array_five), Ordering::Less);
    }

    #[test]
    fn bt_range_comparison_uses_range_ordering() {
        let range_type = SqlType::new(SqlTypeKind::Int4Range);
        let empty = parse_range_text("empty", range_type).unwrap();
        let non_empty = parse_range_text("[1,5)", range_type).unwrap();

        assert_eq!(compare_bt_values(&empty, &empty), Ordering::Equal);
        assert_eq!(compare_bt_values(&empty, &non_empty), Ordering::Less);
        assert_eq!(compare_bt_values(&non_empty, &empty), Ordering::Greater);
    }
}
