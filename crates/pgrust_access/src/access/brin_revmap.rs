use crate::access::brin_page::REVMAP_PAGE_MAXITEMS;

pub fn heap_blk_to_revmap_blk(pages_per_range: u32, heap_blk: u32) -> u32 {
    ((heap_blk / pages_per_range) as usize / REVMAP_PAGE_MAXITEMS) as u32
}

pub fn heap_blk_to_revmap_index(pages_per_range: u32, heap_blk: u32) -> usize {
    (heap_blk / pages_per_range) as usize % REVMAP_PAGE_MAXITEMS
}

pub fn normalize_range_start(pages_per_range: u32, heap_blk: u32) -> u32 {
    (heap_blk / pages_per_range) * pages_per_range
}
