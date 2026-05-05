// :HACK: root compatibility shim while btree comparison logic lives in
// `pgrust_access` and calls back into root scalar services.
use std::cmp::Ordering;

use pgrust_access::nbtree::nbtcompare as access_nbtcompare;

use crate::backend::access::RootAccessServices;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;

pub use access_nbtcompare::{BT_DESC_FLAG, BT_NULLS_FIRST_FLAG, compare_item_pointers};

pub fn compare_bt_values(left: &Value, right: &Value) -> Ordering {
    access_nbtcompare::compare_bt_values_with_services(left, right, &RootAccessServices)
        .expect("btree comparisons should be scalar-service compatible")
}

pub fn compare_bt_values_with_options(left: &Value, right: &Value, option: i16) -> Ordering {
    access_nbtcompare::compare_bt_values_with_options_and_services(
        left,
        right,
        option,
        &RootAccessServices,
    )
    .expect("btree comparisons should be scalar-service compatible")
}

pub fn compare_bt_keyspace(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
) -> Ordering {
    access_nbtcompare::compare_bt_keyspace_with_services(
        left_keys,
        left_tid,
        right_keys,
        right_tid,
        &RootAccessServices,
    )
    .expect("btree keyspace comparisons should be scalar-service compatible")
}

pub fn compare_bt_keyspace_with_options(
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
    indoption: &[i16],
) -> Ordering {
    access_nbtcompare::compare_bt_keyspace_with_options_and_services(
        left_keys,
        left_tid,
        right_keys,
        right_tid,
        indoption,
        &RootAccessServices,
    )
    .expect("btree keyspace comparisons should be scalar-service compatible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::nodes::datum::{ArrayDimension, ArrayValue};
    use pgrust_expr::expr_range::parse_range_text;

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
