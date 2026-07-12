use types_core::{BlockNumber, BLCKSZ};
use types_hnsw::{HNSW_HEAPTIDS, HNSW_PAGE_ID};
use types_tuple::itemptr::ItemPointerData;

pub const MAXALIGN: usize = 8;
pub const SIZE_OF_PAGE_HEADER: usize = 24;
pub const SIZE_OF_ITEM_ID: usize = 4;
pub const SIZE_OF_ITEM_POINTER: usize = 6;

pub const HNSW_PAGE_OPAQUE_SIZE: usize = 8;

// HNSW_MAX_SIZE (hnsw.h).
pub const HNSW_MAX_SIZE: usize =
    BLCKSZ - SIZE_OF_PAGE_HEADER - HNSW_PAGE_OPAQUE_SIZE - SIZE_OF_ITEM_ID;
pub const HNSW_TUPLE_ALLOC_SIZE: usize = BLCKSZ;

// offsetof(HnswElementTupleData, data): 4 flag bytes + 10 heaptids + neighbortid + u16.
pub const ELEMENT_DATA_OFFSET: usize = 4 + HNSW_HEAPTIDS * SIZE_OF_ITEM_POINTER + SIZE_OF_ITEM_POINTER + 2;
// offsetof(HnswNeighborTupleData, indextids).
pub const NEIGHBOR_TIDS_OFFSET: usize = 4;

pub const fn maxalign(n: usize) -> usize {
    (n + MAXALIGN - 1) & !(MAXALIGN - 1)
}

pub const fn element_tuple_size(value_size: usize) -> usize {
    maxalign(ELEMENT_DATA_OFFSET + value_size)
}

pub const fn neighbor_tuple_size(level: u8, m: i32) -> usize {
    maxalign(NEIGHBOR_TIDS_OFFSET + (level as usize + 2) * (m as usize) * SIZE_OF_ITEM_POINTER)
}

// HnswGetMaxLevel (hnsw.h) — integer division order preserved.
pub fn hnsw_get_max_level(m: i32) -> i32 {
    let v = (BLCKSZ - SIZE_OF_PAGE_HEADER - HNSW_PAGE_OPAQUE_SIZE - NEIGHBOR_TIDS_OFFSET
        - SIZE_OF_ITEM_ID)
        / SIZE_OF_ITEM_POINTER
        / (m as usize);
    ((v as i32) - 2).min(255)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetaPage {
    pub magic_number: u32,
    pub version: u32,
    pub dimensions: u32,
    pub m: u16,
    pub ef_construction: u16,
    pub entry_blkno: BlockNumber,
    pub entry_offno: u16,
    pub entry_level: i16,
    pub insert_page: BlockNumber,
}

pub const METAPAGE_SIZE: usize = 28;

impl MetaPage {
    pub fn read(contents: &[u8]) -> MetaPage {
        let g4 = |o: usize| u32::from_ne_bytes(contents[o..o + 4].try_into().unwrap());
        let g2 = |o: usize| u16::from_ne_bytes(contents[o..o + 2].try_into().unwrap());
        MetaPage {
            magic_number: g4(0),
            version: g4(4),
            dimensions: g4(8),
            m: g2(12),
            ef_construction: g2(14),
            entry_blkno: g4(16),
            entry_offno: g2(20),
            entry_level: g2(22) as i16,
            insert_page: g4(24),
        }
    }

    pub fn write(&self, contents: &mut [u8]) {
        contents[0..4].copy_from_slice(&self.magic_number.to_ne_bytes());
        contents[4..8].copy_from_slice(&self.version.to_ne_bytes());
        contents[8..12].copy_from_slice(&self.dimensions.to_ne_bytes());
        contents[12..14].copy_from_slice(&self.m.to_ne_bytes());
        contents[14..16].copy_from_slice(&self.ef_construction.to_ne_bytes());
        contents[16..20].copy_from_slice(&self.entry_blkno.to_ne_bytes());
        contents[20..22].copy_from_slice(&self.entry_offno.to_ne_bytes());
        contents[22..24].copy_from_slice(&(self.entry_level as u16).to_ne_bytes());
        contents[24..28].copy_from_slice(&self.insert_page.to_ne_bytes());
    }
}

// ItemPointerData byte helpers (block hi/lo u16 pair + offset, C layout).
pub fn itemptr_encode(blkno: BlockNumber, offno: u16) -> [u8; 6] {
    let mut b = [0u8; 6];
    b[0..2].copy_from_slice(&((blkno >> 16) as u16).to_ne_bytes());
    b[2..4].copy_from_slice(&((blkno & 0xFFFF) as u16).to_ne_bytes());
    b[4..6].copy_from_slice(&offno.to_ne_bytes());
    b
}

pub fn itemptr_decode(b: &[u8]) -> (BlockNumber, u16) {
    let hi = u16::from_ne_bytes(b[0..2].try_into().unwrap()) as u32;
    let lo = u16::from_ne_bytes(b[2..4].try_into().unwrap()) as u32;
    let off = u16::from_ne_bytes(b[4..6].try_into().unwrap());
    ((hi << 16) | lo, off)
}

pub const INVALID_BLOCK: BlockNumber = 0xFFFF_FFFF;
pub const INVALID_OFFSET: u16 = 0;

pub fn itemptr_is_valid(b: &[u8]) -> bool {
    let (blk, off) = itemptr_decode(b);
    blk != INVALID_BLOCK && off != INVALID_OFFSET
}

pub fn ipd_to_bytes(t: &ItemPointerData) -> [u8; 6] {
    itemptr_encode(
        types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(t),
        types_tuple::itemptr::ItemPointerGetOffsetNumberNoCheck(t),
    )
}

pub fn ipd_from_bytes(b: &[u8]) -> ItemPointerData {
    let (blk, off) = itemptr_decode(b);
    ItemPointerData::new(blk, off)
}

// Borrowed view of an on-page element tuple.
pub struct ElementTupleView<'a> {
    pub bytes: &'a [u8],
}

impl<'a> ElementTupleView<'a> {
    pub fn tuple_type(&self) -> u8 {
        self.bytes[0]
    }
    pub fn level(&self) -> u8 {
        self.bytes[1]
    }
    pub fn deleted(&self) -> u8 {
        self.bytes[2]
    }
    pub fn version(&self) -> u8 {
        self.bytes[3]
    }
    pub fn heaptid_bytes(&self, i: usize) -> &'a [u8] {
        &self.bytes[4 + i * 6..4 + i * 6 + 6]
    }
    pub fn neighbortid(&self) -> (BlockNumber, u16) {
        itemptr_decode(&self.bytes[4 + HNSW_HEAPTIDS * 6..])
    }
    // The stored value varlena image (4B header).
    pub fn value_image(&self) -> &'a [u8] {
        let d = &self.bytes[ELEMENT_DATA_OFFSET..];
        let size = (u32::from_ne_bytes(d[0..4].try_into().unwrap()) >> 2) as usize;
        &d[..size]
    }
}

pub struct NeighborTupleView<'a> {
    pub bytes: &'a [u8],
}

impl<'a> NeighborTupleView<'a> {
    pub fn tuple_type(&self) -> u8 {
        self.bytes[0]
    }
    pub fn version(&self) -> u8 {
        self.bytes[1]
    }
    pub fn count(&self) -> u16 {
        u16::from_ne_bytes(self.bytes[2..4].try_into().unwrap())
    }
    pub fn indextid_bytes(&self, i: usize) -> &'a [u8] {
        &self.bytes[NEIGHBOR_TIDS_OFFSET + i * 6..NEIGHBOR_TIDS_OFFSET + i * 6 + 6]
    }
}

pub fn page_opaque_nextblkno(page: &[u8]) -> BlockNumber {
    let off = BLCKSZ - HNSW_PAGE_OPAQUE_SIZE;
    u32::from_ne_bytes(page[off..off + 4].try_into().unwrap())
}

pub fn page_opaque_set_nextblkno(page: &mut [u8], blkno: BlockNumber) {
    let off = BLCKSZ - HNSW_PAGE_OPAQUE_SIZE;
    page[off..off + 4].copy_from_slice(&blkno.to_ne_bytes());
}

pub fn page_opaque_init(page: &mut [u8]) {
    let off = BLCKSZ - HNSW_PAGE_OPAQUE_SIZE;
    page[off..off + 4].copy_from_slice(&INVALID_BLOCK.to_ne_bytes());
    page[off + 4..off + 6].copy_from_slice(&0u16.to_ne_bytes());
    page[off + 6..off + 8].copy_from_slice(&HNSW_PAGE_ID.to_ne_bytes());
}
