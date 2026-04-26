use crate::backend::access::nbtree::nbtcompare::compare_bt_keyspace;
use crate::backend::access::nbtree::nbtutils::BtSortTuple;

#[derive(Debug, Default)]
pub struct BtSpool {
    tuples: Vec<BtSortTuple>,
}

impl BtSpool {
    pub fn push(&mut self, tuple: BtSortTuple) {
        self.tuples.push(tuple);
    }

    pub fn finish(mut self, key_count: usize) -> Vec<BtSortTuple> {
        self.tuples.sort_by(|left, right| {
            compare_bt_keyspace(
                &left.key_values[..left.key_values.len().min(key_count)],
                &left.tuple.t_tid,
                &right.key_values[..right.key_values.len().min(key_count)],
                &right.tuple.t_tid,
            )
        });
        self.tuples
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::access::itemptr::ItemPointerData;
    use crate::include::access::itup::IndexTupleData;
    use crate::include::nodes::datum::Value;

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
        let tuples = spool.finish(1);
        assert_eq!(tuples[0].tuple.t_tid.offset_number, 1);
    }
}
