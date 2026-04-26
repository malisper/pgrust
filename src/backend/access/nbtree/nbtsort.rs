use std::cmp::Ordering;

use crate::backend::access::nbtree::nbtcompare::{compare_bt_values, compare_item_pointers};
use crate::backend::access::nbtree::nbtutils::BtSortTuple;
use crate::backend::parser::SqlTypeKind;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::ColumnDesc;

#[derive(Debug, Default)]
pub struct BtSpool {
    tuples: Vec<BtSortTuple>,
}

impl BtSpool {
    pub fn push(&mut self, tuple: BtSortTuple) {
        self.tuples.push(tuple);
    }

    pub fn finish(mut self, columns: &[ColumnDesc], key_count: usize) -> Vec<BtSortTuple> {
        self.tuples.sort_by(|left, right| {
            compare_keyspace_with_columns(
                columns,
                &left.key_values[..left.key_values.len().min(key_count)],
                &left.tuple.t_tid,
                &right.key_values[..right.key_values.len().min(key_count)],
                &right.tuple.t_tid,
            )
        });
        self.tuples
    }
}

fn fixed_vector_items(value: &Value) -> Option<Vec<i64>> {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|value| match value {
                Value::Int16(value) => Some(i64::from(*value)),
                Value::Int32(value) => Some(i64::from(*value)),
                Value::Int64(value) => Some(*value),
                _ => None,
            })
            .collect(),
        Value::PgArray(array) => array
            .elements
            .iter()
            .map(|value| match value {
                Value::Int16(value) => Some(i64::from(*value)),
                Value::Int32(value) => Some(i64::from(*value)),
                Value::Int64(value) => Some(*value),
                _ => None,
            })
            .collect(),
        value => value.as_text().and_then(|text| {
            text.split_ascii_whitespace()
                .map(|part| part.parse::<i64>().ok())
                .collect()
        }),
    }
}

fn compare_keyspace_with_columns(
    columns: &[ColumnDesc],
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
) -> Ordering {
    for (idx, (left, right)) in left_keys.iter().zip(right_keys.iter()).enumerate() {
        if columns.get(idx).is_some_and(|column| {
            matches!(
                column.sql_type.kind,
                SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
            )
        }) && let (Some(left_items), Some(right_items)) =
            (fixed_vector_items(left), fixed_vector_items(right))
        {
            let ord = left_items.cmp(&right_items);
            if ord != Ordering::Equal {
                return ord;
            }
            continue;
        }
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
    use crate::include::access::itup::IndexTupleData;

    #[test]
    fn bt_spool_orders_by_keys_then_tid() {
        let mut spool = BtSpool::default();
        spool.push(BtSortTuple {
            tuple: IndexTupleData::new_raw(
                ItemPointerData {
                    block_number: 1,
                    offset_number: 2,
                },
                false,
                false,
                false,
                vec![1],
            ),
            key_values: vec![Value::Int32(5)],
        });
        spool.push(BtSortTuple {
            tuple: IndexTupleData::new_raw(
                ItemPointerData {
                    block_number: 1,
                    offset_number: 1,
                },
                false,
                false,
                false,
                vec![1],
            ),
            key_values: vec![Value::Int32(5)],
        });
        let tuples = spool.finish(&[], 1);
        assert_eq!(tuples[0].tuple.t_tid.offset_number, 1);
    }
}
