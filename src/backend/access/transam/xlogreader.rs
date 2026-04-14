use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};

pub const XLOG_RECORD_HEADER: usize = 32;
pub const XLOG_BLOCK_HEADER: usize = 32;
pub const CRC_OFFSET: usize = 24;
pub const WAL_RECORD_LEN: usize = XLOG_RECORD_HEADER + XLOG_BLOCK_HEADER + PAGE_SIZE;

pub const BKPBLOCK_HAS_IMAGE: u8 = 1 << 0;
pub const BKPBLOCK_HAS_DATA: u8 = 1 << 1;
pub const BKPBLOCK_WILL_INIT: u8 = 1 << 2;
pub const BKPBLOCK_STANDARD: u8 = 1 << 3;

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
    pub end_lsn: u64,
    pub total_len: u32,
    pub xid: u32,
    pub prev: u64,
    pub rmid: u8,
    pub info: u8,
    pub blocks: Vec<DecodedBkpBlock>,
    pub main_data: Vec<u8>,
}

impl DecodedXLogRecord {
    pub fn block_ref(&self, block_id: u8) -> Option<&DecodedBkpBlock> {
        self.blocks.iter().find(|block| block.block_id == block_id)
    }
}
