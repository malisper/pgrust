use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};

pub const WAL_PAGE_SIZE: usize = PAGE_SIZE;
pub const XLOG_RECORD_HEADER: usize = 24;
pub const XLOG_BLOCK_HEADER: usize = 4;
pub const XLOG_BLOCK_IMAGE_HEADER: usize = 5;
pub const XLOG_RECORD_DATA_HEADER_SHORT: usize = 2;
pub const XLOG_RECORD_DATA_HEADER_LONG: usize = 5;
pub const CRC_OFFSET: usize = 20;
pub const WAL_RECORD_LEN: usize = XLOG_RECORD_HEADER + XLOG_BLOCK_HEADER + PAGE_SIZE;
pub const XLOG_PAGE_MAGIC: u16 = 0xD118;
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLP_LONG_HEADER: u16 = 0x0002;
pub const XLOG_SHORT_PHD: usize = 24;
pub const XLOG_LONG_PHD: usize = 40;

pub const XLR_MAX_BLOCK_ID: u8 = 32;
pub const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: u8 = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

pub const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
pub const BKPBLOCK_HAS_DATA: u8 = 0x20;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40;
pub const BKPBLOCK_SAME_REL: u8 = 0x80;
pub const BKPBLOCK_STANDARD: u8 = 1 << 0;

pub const BKPIMAGE_HAS_HOLE: u8 = 0x01;
pub const BKPIMAGE_APPLY: u8 = 0x02;

#[derive(Debug, Clone)]
pub struct DecodedBkpBlock {
    pub block_id: u8,
    pub tag: BufferTag,
    pub flags: u8,
    pub data: Vec<u8>,
    pub image: Option<Box<[u8; PAGE_SIZE]>>,
    pub hole_offset: u16,
    pub hole_length: u16,
}

impl DecodedBkpBlock {
    pub fn has_image(&self) -> bool {
        self.flags & BKPBLOCK_HAS_IMAGE != 0
    }

    pub fn will_init(&self) -> bool {
        self.flags & BKPBLOCK_WILL_INIT != 0
    }
}

#[derive(Debug, Clone)]
pub struct DecodedXLogRecord {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub total_len: u32,
    pub xid: u32,
    pub prev: u64,
    pub rmid: u8,
    pub info: u8,
    pub origin: Option<u32>,
    pub top_level_xid: Option<u32>,
    pub blocks: Vec<DecodedBkpBlock>,
    pub main_data: Vec<u8>,
}

impl DecodedXLogRecord {
    pub fn block_ref(&self, block_id: u8) -> Option<&DecodedBkpBlock> {
        self.blocks.iter().find(|block| block.block_id == block_id)
    }
}
