use crate::backend::storage::page::bufpage::{
    MAXALIGN, PageError, page_special, page_special_mut, SIZE_OF_PAGE_HEADER_DATA,
};
use crate::backend::storage::smgr::BLCKSZ;

pub const BRIN_PAGETYPE_META: u16 = 0xF091;
pub const BRIN_PAGETYPE_REVMAP: u16 = 0xF092;
pub const BRIN_PAGETYPE_REGULAR: u16 = 0xF093;

pub const BRIN_EVACUATE_PAGE: u16 = 1 << 0;

pub const BRIN_CURRENT_VERSION: u32 = 1;
pub const BRIN_META_MAGIC: u32 = 0xA810_9CFA;
pub const BRIN_METAPAGE_BLKNO: u32 = 0;

pub const BRIN_SPECIAL_SIZE: usize = MAXALIGN;
pub const BRIN_SPECIAL_WORDS: usize = BRIN_SPECIAL_SIZE / std::mem::size_of::<u16>();
pub const BRIN_FLAGS_WORD_INDEX: usize = BRIN_SPECIAL_WORDS - 2;
pub const BRIN_TYPE_WORD_INDEX: usize = BRIN_SPECIAL_WORDS - 1;

pub const fn brin_maxalign_const(len: usize) -> usize {
    (len + (MAXALIGN - 1)) & !(MAXALIGN - 1)
}

pub const BRIN_PAGE_CONTENT_OFFSET: usize = brin_maxalign_const(SIZE_OF_PAGE_HEADER_DATA);
pub const REVMAP_ENTRY_SIZE: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct BrinMetaPageData {
    pub brin_magic: u32,
    pub brin_version: u32,
    pub pages_per_range: u32,
    pub last_revmap_page: u32,
}

impl BrinMetaPageData {
    pub const SIZE: usize = 16;
}

pub const REVMAP_CONTENT_SIZE: usize = BLCKSZ - BRIN_PAGE_CONTENT_OFFSET - BRIN_SPECIAL_SIZE;
pub const REVMAP_PAGE_MAXITEMS: usize = REVMAP_CONTENT_SIZE / REVMAP_ENTRY_SIZE;

pub const BRIN_MAX_ITEM_SIZE: usize =
    (BLCKSZ - ((SIZE_OF_PAGE_HEADER_DATA + 4 + (MAXALIGN - 1)) & !(MAXALIGN - 1)) - BRIN_SPECIAL_SIZE)
        & !(MAXALIGN - 1);

fn special_word(special: &[u8], word_index: usize) -> u16 {
    let offset = word_index * std::mem::size_of::<u16>();
    u16::from_le_bytes([special[offset], special[offset + 1]])
}

fn set_special_word(special: &mut [u8], word_index: usize, value: u16) {
    let offset = word_index * std::mem::size_of::<u16>();
    special[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub fn brin_page_type(page: &[u8; BLCKSZ]) -> Result<u16, PageError> {
    Ok(special_word(page_special(page)?, BRIN_TYPE_WORD_INDEX))
}

pub fn brin_page_flags(page: &[u8; BLCKSZ]) -> Result<u16, PageError> {
    Ok(special_word(page_special(page)?, BRIN_FLAGS_WORD_INDEX))
}

pub fn set_brin_page_type(page: &mut [u8; BLCKSZ], page_type: u16) -> Result<(), PageError> {
    let special = page_special_mut(page)?;
    set_special_word(special, BRIN_TYPE_WORD_INDEX, page_type);
    Ok(())
}

pub fn set_brin_page_flags(page: &mut [u8; BLCKSZ], flags: u16) -> Result<(), PageError> {
    let special = page_special_mut(page)?;
    set_special_word(special, BRIN_FLAGS_WORD_INDEX, flags);
    Ok(())
}

pub fn brin_is_meta_page(page: &[u8; BLCKSZ]) -> Result<bool, PageError> {
    Ok(brin_page_type(page)? == BRIN_PAGETYPE_META)
}

pub fn brin_is_revmap_page(page: &[u8; BLCKSZ]) -> Result<bool, PageError> {
    Ok(brin_page_type(page)? == BRIN_PAGETYPE_REVMAP)
}

pub fn brin_is_regular_page(page: &[u8; BLCKSZ]) -> Result<bool, PageError> {
    Ok(brin_page_type(page)? == BRIN_PAGETYPE_REGULAR)
}

pub const fn revmap_entry_offset(index: usize) -> usize {
    BRIN_PAGE_CONTENT_OFFSET + (index * REVMAP_ENTRY_SIZE)
}
