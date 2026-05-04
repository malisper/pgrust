use crate::access::itup::IndexTupleData;
use crate::nbtree::nbtsplitloc::choose_split_index;

pub fn find_insert_offset<T>(items: &[T], mut cmp: impl FnMut(&T) -> std::cmp::Ordering) -> usize {
    crate::nbtree::nbtsearch::lower_bound_by(items, |item| cmp(item))
}

pub fn split_sorted_tuples(
    mut items: Vec<IndexTupleData>,
    split_index: Option<usize>,
) -> (Vec<IndexTupleData>, Vec<IndexTupleData>) {
    let split = split_index.unwrap_or_else(|| choose_split_index(&items, None));
    let right = items.split_off(split);
    (items, right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::itemptr::ItemPointerData;

    #[test]
    fn split_sorted_tuples_keeps_both_halves_nonempty() {
        let items = vec![
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![1]),
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![2]),
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![3]),
        ];
        let (left, right) = split_sorted_tuples(items, None);
        assert!(!left.is_empty());
        assert!(!right.is_empty());
    }
}
