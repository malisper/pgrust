use crate::backend::storage::page::bufpage::{
    PageError, page_add_item, page_get_item, page_get_max_offset_number, page_init, page_special,
    page_special_mut,
};
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GistBufferingMode {
    Auto,
    On,
    Off,
}

impl Default for GistBufferingMode {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GistOptions {
    #[serde(default = "default_gist_fillfactor")]
    pub fillfactor: u16,
    #[serde(default)]
    pub buffering_mode: GistBufferingMode,
}

impl Default for GistOptions {
    fn default() -> Self {
        Self {
            fillfactor: default_gist_fillfactor(),
            buffering_mode: GistBufferingMode::Auto,
        }
    }
}

const fn default_gist_fillfactor() -> u16 {
    90
}

pub const GIST_ROOT_BLKNO: u32 = 0;
pub const GIST_INVALID_BLOCKNO: u32 = u32::MAX;
pub const GIST_PAGE_ID: u16 = 0xFF81;
pub const GIST_PAGE_FORMAT_VERSION: u16 = 1;
pub const GIST_REBUILD_REQUIRED: &str =
    "legacy GiST index format detected; drop and recreate the index";

pub const F_LEAF: u16 = 1 << 0;
pub const F_DELETED: u16 = 1 << 1;
pub const F_TUPLES_DELETED: u16 = 1 << 2;
pub const F_FOLLOW_RIGHT: u16 = 1 << 3;
pub const F_HAS_GARBAGE: u16 = 1 << 4;

pub const GIST_CONSISTENT_PROC: i16 = 1;
pub const GIST_UNION_PROC: i16 = 2;
pub const GIST_COMPRESS_PROC: i16 = 3;
pub const GIST_DECOMPRESS_PROC: i16 = 4;
pub const GIST_PENALTY_PROC: i16 = 5;
pub const GIST_PICKSPLIT_PROC: i16 = 6;
pub const GIST_EQUAL_PROC: i16 = 7;
pub const GIST_DISTANCE_PROC: i16 = 8;
pub const GIST_FETCH_PROC: i16 = 9;
pub const GIST_OPTIONS_PROC: i16 = 10;
pub const GIST_SORTSUPPORT_PROC: i16 = 11;
pub const GIST_TRANSLATE_CMPTYPE_PROC: i16 = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct GistPageOpaqueData {
    pub nsn: u64,
    pub rightlink: u32,
    pub flags: u16,
    pub gist_page_id: u16,
    pub format_version: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GistPageError {
    Page(PageError),
    Corrupt(&'static str),
    Tuple(crate::include::access::itup::IndexTupleError),
}

impl From<PageError> for GistPageError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

impl From<crate::include::access::itup::IndexTupleError> for GistPageError {
    fn from(value: crate::include::access::itup::IndexTupleError) -> Self {
        Self::Tuple(value)
    }
}

impl GistPageOpaqueData {
    pub const SIZE: usize = 20;

    pub fn new(flags: u16) -> Self {
        Self {
            nsn: 0,
            rightlink: GIST_INVALID_BLOCKNO,
            flags,
            gist_page_id: GIST_PAGE_ID,
            format_version: GIST_PAGE_FORMAT_VERSION,
            reserved: 0,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.flags & F_LEAF != 0
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & F_DELETED != 0
    }

    pub fn follows_right(&self) -> bool {
        self.flags & F_FOLLOW_RIGHT != 0
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..8].copy_from_slice(&self.nsn.to_le_bytes());
        out[8..12].copy_from_slice(&self.rightlink.to_le_bytes());
        out[12..14].copy_from_slice(&self.flags.to_le_bytes());
        out[14..16].copy_from_slice(&self.gist_page_id.to_le_bytes());
        out[16..18].copy_from_slice(&self.format_version.to_le_bytes());
        out[18..20].copy_from_slice(&self.reserved.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, GistPageError> {
        if bytes.len() < Self::SIZE {
            return Err(GistPageError::Corrupt(GIST_REBUILD_REQUIRED));
        }
        let opaque = Self {
            nsn: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            rightlink: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            flags: u16::from_le_bytes(bytes[12..14].try_into().unwrap()),
            gist_page_id: u16::from_le_bytes(bytes[14..16].try_into().unwrap()),
            format_version: u16::from_le_bytes(bytes[16..18].try_into().unwrap()),
            reserved: u16::from_le_bytes(bytes[18..20].try_into().unwrap()),
        };
        if opaque.gist_page_id != GIST_PAGE_ID {
            return Err(GistPageError::Corrupt("gist page id mismatch"));
        }
        if opaque.format_version != GIST_PAGE_FORMAT_VERSION {
            return Err(GistPageError::Corrupt(GIST_REBUILD_REQUIRED));
        }
        Ok(opaque)
    }
}

pub fn gist_page_get_opaque(page: &[u8; BLCKSZ]) -> Result<GistPageOpaqueData, GistPageError> {
    GistPageOpaqueData::decode(page_special(page)?)
}

pub fn gist_page_set_opaque(
    page: &mut [u8; BLCKSZ],
    opaque: GistPageOpaqueData,
) -> Result<(), GistPageError> {
    page_special_mut(page)?.copy_from_slice(&opaque.encode());
    Ok(())
}

pub fn gist_page_init(page: &mut [u8; BLCKSZ], flags: u16) -> Result<(), GistPageError> {
    page_init(page, GistPageOpaqueData::SIZE);
    gist_page_set_opaque(page, GistPageOpaqueData::new(flags))
}

pub fn gist_page_items(page: &[u8; BLCKSZ]) -> Result<Vec<IndexTupleData>, GistPageError> {
    Ok(gist_page_items_with_offsets(page)?
        .into_iter()
        .map(|(_, tuple)| tuple)
        .collect())
}

pub fn gist_page_items_with_offsets(
    page: &[u8; BLCKSZ],
) -> Result<Vec<(u16, IndexTupleData)>, GistPageError> {
    let max_offset = page_get_max_offset_number(page)?;
    let mut items = Vec::with_capacity(max_offset as usize);
    for offset in 1..=max_offset {
        items.push((offset, IndexTupleData::parse(page_get_item(page, offset)?)?));
    }
    Ok(items)
}

pub fn gist_page_append_tuple(
    page: &mut [u8; BLCKSZ],
    tuple: &IndexTupleData,
) -> Result<u16, GistPageError> {
    Ok(page_add_item(page, &tuple.serialize())?)
}

pub fn gist_page_replace_items(
    page: &mut [u8; BLCKSZ],
    tuples: &[IndexTupleData],
    opaque: GistPageOpaqueData,
) -> Result<(), GistPageError> {
    page_init(page, GistPageOpaqueData::SIZE);
    gist_page_set_opaque(page, opaque)?;
    for tuple in tuples {
        gist_page_append_tuple(page, tuple)?;
    }
    Ok(())
}

pub fn gist_tuple_is_downlink(tuple: &IndexTupleData) -> bool {
    tuple.t_tid.offset_number == 0
}

pub fn gist_downlink_block(tuple: &IndexTupleData) -> Option<u32> {
    gist_tuple_is_downlink(tuple).then_some(tuple.t_tid.block_number)
}

pub fn gist_make_leaf_tid(tid: ItemPointerData) -> ItemPointerData {
    tid
}

pub fn gist_make_downlink_tid(block: u32) -> ItemPointerData {
    ItemPointerData {
        block_number: block,
        offset_number: 0,
    }
}
