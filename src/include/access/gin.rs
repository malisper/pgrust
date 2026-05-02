use crate::backend::storage::page::bufpage::{
    OffsetNumber, PageError, PageHeaderData, SIZE_OF_PAGE_HEADER_DATA, max_align, page_add_item,
    page_get_item, page_get_max_offset_number, page_header, page_init, page_special,
    page_special_mut,
};
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::itemptr::ItemPointerData;
pub use pgrust_nodes::access::GinOptions;

pub const GIN_METAPAGE_BLKNO: u32 = 0;
pub const GIN_ROOT_BLKNO: u32 = 1;
pub const GIN_INVALID_BLOCKNO: u32 = u32::MAX;
pub const GIN_PAGE_ID: u16 = 0xFF82;
pub const GIN_PAGE_FORMAT_VERSION: u16 = 1;
pub const GIN_CURRENT_VERSION: u32 = 2;
pub const GIN_REBUILD_REQUIRED: &str =
    "legacy GIN index format detected; drop and recreate the index";

pub const GIN_DATA: u16 = 1 << 0;
pub const GIN_LEAF: u16 = 1 << 1;
pub const GIN_DELETED: u16 = 1 << 2;
pub const GIN_META: u16 = 1 << 3;
pub const GIN_LIST: u16 = 1 << 4;
pub const GIN_ENTRY: u16 = 1 << 5;

pub const GIN_COMPARE_PROC: i16 = 1;
pub const GIN_EXTRACTVALUE_PROC: i16 = 2;
pub const GIN_EXTRACTQUERY_PROC: i16 = 3;
pub const GIN_CONSISTENT_PROC: i16 = 4;
pub const GIN_COMPARE_PARTIAL_PROC: i16 = 5;
pub const GIN_TRICONSISTENT_PROC: i16 = 6;
pub const GIN_OPTIONS_PROC: i16 = 7;

pub const GIN_SEARCH_MODE_DEFAULT: u8 = 0;
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: u8 = 1;
pub const GIN_SEARCH_MODE_ALL: u8 = 2;
pub const GIN_SEARCH_MODE_EVERYTHING: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i8)]
pub enum GinNullCategory {
    NormalKey = 0,
    NullKey = 1,
    EmptyItem = 2,
    NullItem = 3,
    EmptyQuery = -1,
}

impl GinNullCategory {
    pub fn from_i8(value: i8) -> Option<Self> {
        match value {
            0 => Some(Self::NormalKey),
            1 => Some(Self::NullKey),
            2 => Some(Self::EmptyItem),
            3 => Some(Self::NullItem),
            -1 => Some(Self::EmptyQuery),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct GinMetaPageData {
    pub pending_head: u32,
    pub pending_tail: u32,
    pub tail_free_size: u32,
    pub n_pending_pages: u32,
    pub n_pending_heap_tuples: u64,
    pub n_total_pages: u32,
    pub n_entry_pages: u32,
    pub n_data_pages: u32,
    pub n_entries: u64,
    pub gin_version: u32,
    pub fastupdate: u8,
    pub pending_list_limit_kb: u32,
}

impl GinMetaPageData {
    pub const SIZE: usize = 56;

    pub fn new(options: &GinOptions) -> Self {
        Self {
            pending_head: GIN_INVALID_BLOCKNO,
            pending_tail: GIN_INVALID_BLOCKNO,
            tail_free_size: 0,
            n_pending_pages: 0,
            n_pending_heap_tuples: 0,
            n_total_pages: 2,
            n_entry_pages: 1,
            n_data_pages: 0,
            n_entries: 0,
            gin_version: GIN_CURRENT_VERSION,
            fastupdate: u8::from(options.fastupdate),
            pending_list_limit_kb: options.pending_list_limit_kb,
        }
    }

    pub fn options(&self) -> GinOptions {
        GinOptions {
            fastupdate: self.fastupdate != 0,
            pending_list_limit_kb: self.pending_list_limit_kb,
        }
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.pending_head.to_le_bytes());
        out[4..8].copy_from_slice(&self.pending_tail.to_le_bytes());
        out[8..12].copy_from_slice(&self.tail_free_size.to_le_bytes());
        out[12..16].copy_from_slice(&self.n_pending_pages.to_le_bytes());
        out[16..24].copy_from_slice(&self.n_pending_heap_tuples.to_le_bytes());
        out[24..28].copy_from_slice(&self.n_total_pages.to_le_bytes());
        out[28..32].copy_from_slice(&self.n_entry_pages.to_le_bytes());
        out[32..36].copy_from_slice(&self.n_data_pages.to_le_bytes());
        out[36..44].copy_from_slice(&self.n_entries.to_le_bytes());
        out[44..48].copy_from_slice(&self.gin_version.to_le_bytes());
        out[48] = self.fastupdate;
        out[49..52].copy_from_slice(&[0, 0, 0]);
        out[52..56].copy_from_slice(&self.pending_list_limit_kb.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, GinPageError> {
        if bytes.len() < Self::SIZE {
            return Err(GinPageError::Corrupt(GIN_REBUILD_REQUIRED));
        }
        let meta = Self {
            pending_head: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            pending_tail: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            tail_free_size: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            n_pending_pages: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            n_pending_heap_tuples: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            n_total_pages: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            n_entry_pages: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
            n_data_pages: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
            n_entries: u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
            gin_version: u32::from_le_bytes(bytes[44..48].try_into().unwrap()),
            fastupdate: bytes[48],
            pending_list_limit_kb: u32::from_le_bytes(bytes[52..56].try_into().unwrap()),
        };
        if meta.gin_version != GIN_CURRENT_VERSION {
            return Err(GinPageError::Corrupt(GIN_REBUILD_REQUIRED));
        }
        Ok(meta)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct GinPageOpaqueData {
    pub rightlink: u32,
    pub flags: u16,
    pub gin_page_id: u16,
    pub format_version: u16,
    pub reserved: u16,
}

impl GinPageOpaqueData {
    pub const SIZE: usize = 12;

    pub fn new(flags: u16) -> Self {
        Self {
            rightlink: GIN_INVALID_BLOCKNO,
            flags,
            gin_page_id: GIN_PAGE_ID,
            format_version: GIN_PAGE_FORMAT_VERSION,
            reserved: 0,
        }
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.rightlink.to_le_bytes());
        out[4..6].copy_from_slice(&self.flags.to_le_bytes());
        out[6..8].copy_from_slice(&self.gin_page_id.to_le_bytes());
        out[8..10].copy_from_slice(&self.format_version.to_le_bytes());
        out[10..12].copy_from_slice(&self.reserved.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, GinPageError> {
        if bytes.len() < Self::SIZE {
            return Err(GinPageError::Corrupt(GIN_REBUILD_REQUIRED));
        }
        let opaque = Self {
            rightlink: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            flags: u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            gin_page_id: u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
            format_version: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            reserved: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
        };
        if opaque.gin_page_id != GIN_PAGE_ID {
            return Err(GinPageError::Corrupt("gin page id mismatch"));
        }
        if opaque.format_version != GIN_PAGE_FORMAT_VERSION {
            return Err(GinPageError::Corrupt(GIN_REBUILD_REQUIRED));
        }
        Ok(opaque)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GinPageError {
    Page(PageError),
    Corrupt(&'static str),
}

impl From<PageError> for GinPageError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GinEntryKey {
    pub attnum: u16,
    pub category: GinNullCategory,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GinEntryTupleData {
    pub key: GinEntryKey,
    pub posting_root: Option<u32>,
    pub tids: Vec<ItemPointerData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GinPostingTupleData {
    pub tids: Vec<ItemPointerData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GinPendingTupleData {
    pub tid: ItemPointerData,
    pub entries: Vec<GinEntryKey>,
}

impl GinEntryTupleData {
    pub const HEADER_SIZE: usize = 16;

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::HEADER_SIZE + self.key.bytes.len());
        out.extend_from_slice(&self.key.attnum.to_le_bytes());
        out.push(self.key.category as i8 as u8);
        out.push(0);
        out.extend_from_slice(&(self.key.bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(
            &self
                .posting_root
                .unwrap_or(GIN_INVALID_BLOCKNO)
                .to_le_bytes(),
        );
        out.extend_from_slice(&(self.tids.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.key.bytes);
        for tid in &self.tids {
            encode_tid(&mut out, *tid);
        }
        out
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, GinPageError> {
        if bytes.len() < Self::HEADER_SIZE {
            return Err(GinPageError::Corrupt("gin entry tuple too short"));
        }
        let attnum = u16::from_le_bytes(bytes[0..2].try_into().unwrap());
        let category = GinNullCategory::from_i8(bytes[2] as i8)
            .ok_or(GinPageError::Corrupt("invalid gin entry category"))?;
        let key_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let posting_root = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let ntids = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let key_start = Self::HEADER_SIZE;
        let key_end = key_start.saturating_add(key_len);
        let tids_end = key_end.saturating_add(ntids.saturating_mul(6));
        if tids_end > bytes.len() {
            return Err(GinPageError::Corrupt("gin entry tuple truncated"));
        }
        Ok(Self {
            key: GinEntryKey {
                attnum,
                category,
                bytes: bytes[key_start..key_end].to_vec(),
            },
            posting_root: (posting_root != GIN_INVALID_BLOCKNO).then_some(posting_root),
            tids: decode_tids(&bytes[key_end..tids_end])?,
        })
    }
}

impl GinPostingTupleData {
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.tids.len() * 6);
        out.extend_from_slice(&(self.tids.len() as u32).to_le_bytes());
        for tid in &self.tids {
            encode_tid(&mut out, *tid);
        }
        out
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, GinPageError> {
        if bytes.len() < 4 {
            return Err(GinPageError::Corrupt("gin posting tuple too short"));
        }
        let ntids = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let end = 4usize.saturating_add(ntids.saturating_mul(6));
        if end > bytes.len() {
            return Err(GinPageError::Corrupt("gin posting tuple truncated"));
        }
        Ok(Self {
            tids: decode_tids(&bytes[4..end])?,
        })
    }
}

impl GinPendingTupleData {
    pub fn serialized_len(&self) -> usize {
        10 + self
            .entries
            .iter()
            .map(|entry| 8 + entry.bytes.len())
            .sum::<usize>()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        self.serialize_into(&mut out);
        out
    }

    pub fn serialize_into(&self, out: &mut Vec<u8>) {
        encode_tid(out, self.tid);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.attnum.to_le_bytes());
            out.push(entry.category as i8 as u8);
            out.push(0);
            out.extend_from_slice(&(entry.bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&entry.bytes);
        }
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, GinPageError> {
        if bytes.len() < 10 {
            return Err(GinPageError::Corrupt("gin pending tuple too short"));
        }
        let tid = ItemPointerData {
            block_number: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            offset_number: u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
        };
        let nentries = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
        let mut offset = 10usize;
        let mut entries = Vec::with_capacity(nentries);
        for _ in 0..nentries {
            if offset + 8 > bytes.len() {
                return Err(GinPageError::Corrupt("gin pending entry truncated"));
            }
            let attnum = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
            let category = GinNullCategory::from_i8(bytes[offset + 2] as i8)
                .ok_or(GinPageError::Corrupt("invalid gin pending category"))?;
            let key_len =
                u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            offset += 8;
            if offset + key_len > bytes.len() {
                return Err(GinPageError::Corrupt("gin pending key truncated"));
            }
            entries.push(GinEntryKey {
                attnum,
                category,
                bytes: bytes[offset..offset + key_len].to_vec(),
            });
            offset += key_len;
        }
        Ok(Self { tid, entries })
    }
}

pub fn gin_page_get_opaque(page: &[u8; BLCKSZ]) -> Result<GinPageOpaqueData, GinPageError> {
    GinPageOpaqueData::decode(page_special(page)?)
}

pub fn gin_page_set_opaque(
    page: &mut [u8; BLCKSZ],
    opaque: GinPageOpaqueData,
) -> Result<(), GinPageError> {
    page_special_mut(page)?.copy_from_slice(&opaque.encode());
    Ok(())
}

pub fn gin_page_init(page: &mut [u8; BLCKSZ], flags: u16) -> Result<(), GinPageError> {
    page_init(page, GinPageOpaqueData::SIZE);
    gin_page_set_opaque(page, GinPageOpaqueData::new(flags))
}

pub fn gin_metapage_init(
    page: &mut [u8; BLCKSZ],
    options: &GinOptions,
) -> Result<(), GinPageError> {
    gin_page_init(page, GIN_META)?;
    let meta = GinMetaPageData::new(options);
    gin_metapage_set_data(page, &meta)
}

pub fn gin_metapage_data(page: &[u8; BLCKSZ]) -> Result<GinMetaPageData, GinPageError> {
    let meta_start = max_align(SIZE_OF_PAGE_HEADER_DATA);
    GinMetaPageData::decode(
        page.get(meta_start..meta_start + GinMetaPageData::SIZE)
            .ok_or(GinPageError::Corrupt("gin metapage truncated"))?,
    )
}

pub fn gin_metapage_set_data(
    page: &mut [u8; BLCKSZ],
    meta: &GinMetaPageData,
) -> Result<(), GinPageError> {
    let meta_start = max_align(SIZE_OF_PAGE_HEADER_DATA);
    let meta_end = meta_start + GinMetaPageData::SIZE;
    page[meta_start..meta_end].copy_from_slice(&meta.encode());
    let mut header = page_header(page)?;
    header.pd_lower = meta_end as u16;
    gin_write_page_header(page, header);
    Ok(())
}

fn gin_write_page_header(page: &mut [u8; BLCKSZ], header: PageHeaderData) {
    page[0..8].copy_from_slice(&header.pd_lsn.to_le_bytes());
    page[8..10].copy_from_slice(&header.pd_checksum.to_le_bytes());
    page[10..12].copy_from_slice(&header.pd_flags.to_le_bytes());
    page[12..14].copy_from_slice(&header.pd_lower.to_le_bytes());
    page[14..16].copy_from_slice(&header.pd_upper.to_le_bytes());
    page[16..18].copy_from_slice(&header.pd_special.to_le_bytes());
    page[18..20].copy_from_slice(&header.pd_pagesize_version.to_le_bytes());
    page[20..24].copy_from_slice(&header.pd_prune_xid.to_le_bytes());
}

pub fn gin_page_append_item(
    page: &mut [u8; BLCKSZ],
    item: &[u8],
) -> Result<OffsetNumber, GinPageError> {
    Ok(page_add_item(page, item)?)
}

pub fn gin_page_items(page: &[u8; BLCKSZ]) -> Result<Vec<&[u8]>, GinPageError> {
    let max_offset = page_get_max_offset_number(page)?;
    let mut items = Vec::with_capacity(max_offset as usize);
    for offset in 1..=max_offset {
        items.push(page_get_item(page, offset)?);
    }
    Ok(items)
}

fn encode_tid(out: &mut Vec<u8>, tid: ItemPointerData) {
    out.extend_from_slice(&tid.block_number.to_le_bytes());
    out.extend_from_slice(&tid.offset_number.to_le_bytes());
}

fn decode_tids(bytes: &[u8]) -> Result<Vec<ItemPointerData>, GinPageError> {
    if bytes.len() % 6 != 0 {
        return Err(GinPageError::Corrupt("gin tid array has invalid size"));
    }
    Ok(bytes
        .chunks_exact(6)
        .map(|chunk| ItemPointerData {
            block_number: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            offset_number: u16::from_le_bytes(chunk[4..6].try_into().unwrap()),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gin_entry_tuple_roundtrips() {
        let tuple = GinEntryTupleData {
            key: GinEntryKey {
                attnum: 1,
                category: GinNullCategory::NormalKey,
                bytes: b"k".to_vec(),
            },
            posting_root: Some(42),
            tids: vec![ItemPointerData {
                block_number: 7,
                offset_number: 3,
            }],
        };
        assert_eq!(GinEntryTupleData::parse(&tuple.serialize()).unwrap(), tuple);
    }

    #[test]
    fn gin_pending_tuple_serialized_len_matches_bytes() {
        let tuple = GinPendingTupleData {
            tid: ItemPointerData {
                block_number: 9,
                offset_number: 4,
            },
            entries: vec![
                GinEntryKey {
                    attnum: 1,
                    category: GinNullCategory::NormalKey,
                    bytes: b"alpha".to_vec(),
                },
                GinEntryKey {
                    attnum: 2,
                    category: GinNullCategory::EmptyItem,
                    bytes: Vec::new(),
                },
            ],
        };

        let serialized = tuple.serialize();
        assert_eq!(tuple.serialized_len(), serialized.len());
        assert_eq!(GinPendingTupleData::parse(&serialized).unwrap(), tuple);
    }
}
