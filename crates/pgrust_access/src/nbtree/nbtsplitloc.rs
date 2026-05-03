use crate::access::itup::IndexTupleData;

pub fn choose_split_index(items: &[IndexTupleData], incoming: Option<&IndexTupleData>) -> usize {
    let total_bytes = items.iter().map(|item| item.size()).sum::<usize>()
        + incoming.map_or(0, |item| item.size());
    let target = total_bytes / 2;
    let mut seen = 0usize;
    for (idx, item) in items.iter().enumerate() {
        seen += item.size();
        if seen >= target {
            return (idx + 1).clamp(1, items.len().saturating_sub(1).max(1));
        }
    }
    (items.len() / 2).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::itemptr::ItemPointerData;

    #[test]
    fn split_index_prefers_balanced_bytes() {
        let items = vec![
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![0; 10]),
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![0; 10]),
            IndexTupleData::new_raw(ItemPointerData::default(), false, false, false, vec![0; 40]),
        ];
        let split = choose_split_index(&items, None);
        assert!(split >= 1);
        assert!(split <= 2);
    }
}
