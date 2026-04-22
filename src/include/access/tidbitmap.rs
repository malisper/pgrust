use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TidBitmap {
    pages: BTreeSet<u32>,
}

impl TidBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_page(&mut self, block: u32) {
        self.pages.insert(block);
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
        self.pages.extend(pages);
    }

    pub fn contains(&self, block: u32) -> bool {
        self.pages.contains(&block)
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
        self.pages.iter().copied()
    }
}

impl IntoIterator for TidBitmap {
    type Item = u32;
    type IntoIter = std::collections::btree_set::IntoIter<u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.pages.into_iter()
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

        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![2, 4, 5, 6, 9]);
    }
}
