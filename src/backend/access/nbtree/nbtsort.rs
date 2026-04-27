use std::cmp::Ordering;

use crate::backend::access::nbtree::nbtcompare::{
    BT_DESC_FLAG, compare_bt_values_with_options, compare_item_pointers,
};
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

    pub fn finish(
        mut self,
        columns: &[ColumnDesc],
        key_count: usize,
        indoption: &[i16],
    ) -> Vec<BtSortTuple> {
        self.tuples.sort_by(|left, right| {
            compare_keyspace_with_columns_and_options(
                columns,
                &left.key_values[..left.key_values.len().min(key_count)],
                &left.tuple.t_tid,
                &right.key_values[..right.key_values.len().min(key_count)],
                &right.tuple.t_tid,
                indoption,
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

fn compare_keyspace_with_columns_and_options(
    columns: &[ColumnDesc],
    left_keys: &[Value],
    left_tid: &ItemPointerData,
    right_keys: &[Value],
    right_tid: &ItemPointerData,
    indoption: &[i16],
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
            let mut ord = left_items.cmp(&right_items);
            if indoption
                .get(idx)
                .is_some_and(|option| option & BT_DESC_FLAG != 0)
            {
                ord = ord.reverse();
            }
            if ord != Ordering::Equal {
                return ord;
            }
            continue;
        }
        let ord = compare_bt_values_with_options(
            left,
            right,
            indoption.get(idx).copied().unwrap_or_default(),
        );
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
        let tuples = spool.finish(&[], 1, &[]);
        assert_eq!(tuples[0].tuple.t_tid.offset_number, 1);
    }
}
