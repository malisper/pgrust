use std::collections::{BTreeMap, BTreeSet};

use crate::backend::storage::page::bufpage::OffsetNumber;
use crate::include::access::itemptr::ItemPointerData;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TidBitmap {
    pages: BTreeMap<u32, TidBitmapPage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TidBitmapPage {
    Exact(BTreeSet<OffsetNumber>),
    Lossy,
}

impl TidBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_page(&mut self, block: u32) {
        self.pages.insert(block, TidBitmapPage::Lossy);
    }

    pub fn add_tid(&mut self, tid: ItemPointerData) {
        match self.pages.entry(tid.block_number) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(TidBitmapPage::Exact(BTreeSet::from([tid.offset_number])));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if let TidBitmapPage::Exact(offsets) = entry.get_mut() {
                    offsets.insert(tid.offset_number);
                }
            }
        }
    }

    pub fn add_range(&mut self, start_block: u32, page_count: u32) {
        for block in start_block..start_block.saturating_add(page_count) {
            self.add_page(block);
        }
    }

    pub fn extend<I>(&mut self, pages: I)
    where
        I: IntoIterator<Item = u32>,
    {
        for page in pages {
            self.add_page(page);
        }
    }

    pub fn contains(&self, block: u32) -> bool {
        self.pages.contains_key(&block)
    }

    pub fn exact_offsets(&self, block: u32) -> Option<&BTreeSet<OffsetNumber>> {
        match self.pages.get(&block) {
            Some(TidBitmapPage::Exact(offsets)) => Some(offsets),
            _ => None,
        }
    }

    pub fn is_lossy(&self, block: u32) -> bool {
        matches!(self.pages.get(&block), Some(TidBitmapPage::Lossy))
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn clear(&mut self) {
        self.pages.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.pages.keys().copied()
    }
}

impl IntoIterator for TidBitmap {
    type Item = u32;
    type IntoIter = std::vec::IntoIter<u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.pages.into_keys().collect::<Vec<_>>().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::TidBitmap;

    #[test]
    fn tidbitmap_orders_and_deduplicates_pages() {
        let mut bitmap = TidBitmap::new();
        bitmap.add_page(9);
        bitmap.add_page(2);
        bitmap.add_page(2);
        bitmap.add_range(4, 3);
        bitmap.add_tid(crate::include::access::itemptr::ItemPointerData {
            block_number: 8,
            offset_number: 2,
        });

        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![2, 4, 5, 6, 8, 9]);
        assert_eq!(
            bitmap
                .exact_offsets(8)
                .unwrap()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![2]
        );
    }
}
