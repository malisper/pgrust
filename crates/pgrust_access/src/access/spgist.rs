use crate::access::gist::GistPageOpaqueData;
use crate::access::itup::IndexTupleData;
use pgrust_storage::page::bufpage::{
    PageError, page_add_item, page_get_item, page_get_max_offset_number, page_init, page_special,
    page_special_mut,
};
use pgrust_storage::smgr::BLCKSZ;

pub const SPGIST_ROOT_BLKNO: u32 = 0;
pub const SPGIST_INVALID_BLOCKNO: u32 = u32::MAX;
pub const SPGIST_PAGE_ID: u16 = 0xFF82;
pub const SPGIST_PAGE_FORMAT_VERSION: u16 = 1;
pub const SPGIST_REBUILD_REQUIRED: &str =
    "legacy or incompatible SP-GiST index format detected; drop and recreate the index";

pub const SPGIST_CONFIG_PROC: i16 = 1;
pub const SPGIST_CHOOSE_PROC: i16 = 2;
pub const SPGIST_PICKSPLIT_PROC: i16 = 3;
pub const SPGIST_INNER_CONSISTENT_PROC: i16 = 4;
pub const SPGIST_LEAF_CONSISTENT_PROC: i16 = 5;
pub const SPGIST_COMPRESS_PROC: i16 = 6;
pub const SPGIST_OPTIONS_PROC: i16 = 7;

pub const F_LEAF: u16 = 1 << 0;
pub const F_DELETED: u16 = 1 << 1;
pub const F_TUPLES_DELETED: u16 = 1 << 2;
pub const F_HAS_GARBAGE: u16 = 1 << 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SpgistPageOpaqueData {
    pub rightlink: u32,
    pub flags: u16,
    pub spgist_page_id: u16,
    pub format_version: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpgistPageError {
    Page(PageError),
    Corrupt(&'static str),
    Tuple(crate::access::itup::IndexTupleError),
}

impl From<PageError> for SpgistPageError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

impl From<crate::access::itup::IndexTupleError> for SpgistPageError {
    fn from(value: crate::access::itup::IndexTupleError) -> Self {
        Self::Tuple(value)
    }
}

impl SpgistPageOpaqueData {
    pub const SIZE: usize = 12;

    pub fn new(flags: u16) -> Self {
        Self {
            rightlink: SPGIST_INVALID_BLOCKNO,
            flags,
            spgist_page_id: SPGIST_PAGE_ID,
            format_version: SPGIST_PAGE_FORMAT_VERSION,
            reserved: 0,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.flags & F_LEAF != 0
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & F_DELETED != 0
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.rightlink.to_le_bytes());
        out[4..6].copy_from_slice(&self.flags.to_le_bytes());
        out[6..8].copy_from_slice(&self.spgist_page_id.to_le_bytes());
        out[8..10].copy_from_slice(&self.format_version.to_le_bytes());
        out[10..12].copy_from_slice(&self.reserved.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SpgistPageError> {
        if bytes.len() < Self::SIZE {
            return Err(SpgistPageError::Corrupt(SPGIST_REBUILD_REQUIRED));
        }
        if bytes.len() >= GistPageOpaqueData::SIZE && GistPageOpaqueData::decode(bytes).is_ok() {
            return Err(SpgistPageError::Corrupt(SPGIST_REBUILD_REQUIRED));
        }
        let opaque = Self {
            rightlink: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            flags: u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            spgist_page_id: u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
            format_version: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            reserved: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
        };
        if opaque.spgist_page_id != SPGIST_PAGE_ID {
            return Err(SpgistPageError::Corrupt("spgist page id mismatch"));
        }
        if opaque.format_version != SPGIST_PAGE_FORMAT_VERSION {
            return Err(SpgistPageError::Corrupt(SPGIST_REBUILD_REQUIRED));
        }
        Ok(opaque)
    }
}

pub fn spgist_page_get_opaque(
    page: &[u8; BLCKSZ],
) -> Result<SpgistPageOpaqueData, SpgistPageError> {
    SpgistPageOpaqueData::decode(page_special(page)?)
}

pub fn spgist_page_set_opaque(
    page: &mut [u8; BLCKSZ],
    opaque: SpgistPageOpaqueData,
) -> Result<(), SpgistPageError> {
    page_special_mut(page)?.copy_from_slice(&opaque.encode());
    Ok(())
}

pub fn spgist_page_init(page: &mut [u8; BLCKSZ], flags: u16) -> Result<(), SpgistPageError> {
    page_init(page, SpgistPageOpaqueData::SIZE);
    spgist_page_set_opaque(page, SpgistPageOpaqueData::new(flags))
}

pub fn spgist_page_items(page: &[u8; BLCKSZ]) -> Result<Vec<IndexTupleData>, SpgistPageError> {
    Ok(spgist_page_items_with_offsets(page)?
        .into_iter()
        .map(|(_, tuple)| tuple)
        .collect())
}

pub fn spgist_page_items_with_offsets(
    page: &[u8; BLCKSZ],
) -> Result<Vec<(u16, IndexTupleData)>, SpgistPageError> {
    let max_offset = page_get_max_offset_number(page)?;
    let mut items = Vec::with_capacity(max_offset as usize);
    for offset in 1..=max_offset {
        items.push((offset, IndexTupleData::parse(page_get_item(page, offset)?)?));
    }
    Ok(items)
}

pub fn spgist_page_append_tuple(
    page: &mut [u8; BLCKSZ],
    tuple: &IndexTupleData,
) -> Result<u16, SpgistPageError> {
    Ok(page_add_item(page, &tuple.serialize())?)
}

pub fn spgist_page_replace_items(
    page: &mut [u8; BLCKSZ],
    tuples: &[IndexTupleData],
    opaque: SpgistPageOpaqueData,
) -> Result<(), SpgistPageError> {
    page_init(page, SpgistPageOpaqueData::SIZE);
    spgist_page_set_opaque(page, opaque)?;
    for tuple in tuples {
        spgist_page_append_tuple(page, tuple)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::BLCKSZ;
    use crate::access::gist::gist_page_init;

    use super::{F_LEAF, SPGIST_REBUILD_REQUIRED, spgist_page_get_opaque};

    #[test]
    fn spgist_reader_rejects_legacy_gist_page_format() {
        let mut page = [0u8; BLCKSZ];
        gist_page_init(&mut page, F_LEAF).expect("gist page init");

        let err = spgist_page_get_opaque(&page).expect_err("legacy gist page should fail");
        assert_eq!(
            err,
            super::SpgistPageError::Corrupt(SPGIST_REBUILD_REQUIRED)
        );
    }
}
