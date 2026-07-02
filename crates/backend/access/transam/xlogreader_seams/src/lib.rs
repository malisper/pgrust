#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use types_core::{BlockNumber, Buffer, ForkNumber, TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::PgResult;
use types_storage::RelFileLocator;

pub const XLOG_BLCKSZ: usize = 8192;
pub const XLR_MAX_BLOCK_ID: usize = 32;

pub const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
pub const BKPBLOCK_HAS_DATA: u8 = 0x20;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40;
pub const BKPBLOCK_SAME_REL: u8 = 0x80;

// WALOpenSegment (xlogreader.h); ws_file is the OS fd, -1 when closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WALOpenSegment {
    pub ws_file: i32,
    pub ws_segno: XLogSegNo,
    pub ws_tli: TimeLineID,
}

impl Default for WALOpenSegment {
    fn default() -> Self {
        WALOpenSegment {
            ws_file: -1,
            ws_segno: 0,
            ws_tli: 0,
        }
    }
}

// WALSegmentContext trimmed to ws_segsize (ws_dir unused by current ports).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WALSegmentContext {
    pub ws_segsize: i32,
}

// DecodedBkpBlock (xlogreader.h) trimmed to the tag/flag fields the redo
// fetchers consume; the xlogreader port widens it as decoding lands.
#[derive(Clone, Copy, Debug)]
pub struct DecodedBkpBlock {
    pub in_use: bool,
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blkno: BlockNumber,
    pub prefetch_buffer: Buffer,
    pub flags: u8,
    pub has_image: bool,
    pub apply_image: bool,
}

impl DecodedBkpBlock {
    pub const EMPTY: DecodedBkpBlock = DecodedBkpBlock {
        in_use: false,
        rlocator: RelFileLocator::new(0, 0, 0),
        forknum: ForkNumber::MAIN_FORKNUM,
        blkno: 0,
        prefetch_buffer: 0,
        flags: 0,
        has_image: false,
        apply_image: false,
    };
}

impl Default for DecodedBkpBlock {
    fn default() -> Self {
        DecodedBkpBlock::EMPTY
    }
}

// DecodedXLogRecord trimmed to the block references.
#[derive(Clone, Copy, Debug)]
pub struct DecodedXLogRecord {
    pub max_block_id: i8,
    pub blocks: [DecodedBkpBlock; XLR_MAX_BLOCK_ID + 1],
}

impl Default for DecodedXLogRecord {
    fn default() -> Self {
        DecodedXLogRecord {
            max_block_id: -1,
            blocks: [DecodedBkpBlock::EMPTY; XLR_MAX_BLOCK_ID + 1],
        }
    }
}

// XLogReaderState (xlogreader.h) trimmed to what rmgr callbacks and
// xlogutils read/write. `private_end_of_wal` is the
// ReadLocalXLogPageNoWaitPrivate.end_of_wal flag C reaches via private_data.
#[derive(Clone, Copy, Debug, Default)]
pub struct XLogReaderState {
    pub ReadRecPtr: XLogRecPtr,
    pub EndRecPtr: XLogRecPtr,
    pub record: Option<DecodedXLogRecord>,
    pub seg: WALOpenSegment,
    pub segcxt: WALSegmentContext,
    pub segoff: u32,
    pub readLen: u32,
    pub currTLI: TimeLineID,
    pub currTLIValidUntil: XLogRecPtr,
    pub nextTLI: TimeLineID,
    pub private_end_of_wal: bool,
}

impl XLogReaderState {
    // XLogRecHasBlockRef(record, block_id) (xlogreader.h inline).
    pub fn has_block_ref(&self, block_id: u8) -> bool {
        match &self.record {
            Some(r) => (block_id as i8) <= r.max_block_id && r.blocks[block_id as usize].in_use,
            None => false,
        }
    }

    // XLogRecGetBlock(record, i) (xlogreader.h inline).
    pub fn block(&self, block_id: u8) -> &DecodedBkpBlock {
        &self
            .record
            .as_ref()
            .expect("XLogRecGetBlock on a reader with no decoded record")
            .blocks[block_id as usize]
    }

    // XLogRecGetBlockTagExtended (xlogreader.c); None is the C `false`.
    pub fn block_tag_extended(
        &self,
        block_id: u8,
    ) -> Option<(RelFileLocator, ForkNumber, BlockNumber, Buffer)> {
        if !self.has_block_ref(block_id) {
            return None;
        }
        let blk = self.block(block_id);
        Some((blk.rlocator, blk.forknum, blk.blkno, blk.prefetch_buffer))
    }

    // XLogRecHasBlockImage(record, block_id) (xlogreader.h inline).
    pub fn has_block_image(&self, block_id: u8) -> bool {
        self.block(block_id).has_image
    }

    // XLogRecBlockImageApply(record, block_id) (xlogreader.h inline).
    pub fn block_image_apply(&self, block_id: u8) -> bool {
        self.block(block_id).apply_image
    }
}

// WALReadError (xlogreader.h); wre_seg carries the segment state at failure.
#[derive(Clone, Copy, Debug)]
pub struct WALReadError {
    pub wre_errno: i32,
    pub wre_off: i32,
    pub wre_req: i32,
    pub wre_read: i32,
    pub wre_seg: WALOpenSegment,
}

seam_core::seam!(
    // RestoreBlockImage(record, block_id, page) (xlogreader.c): decompress /
    // copy the full-page image onto the buffer's page. Inner Err carries C's
    // `false` + record->errormsg_buf; the page is reached through `buf` on the
    // bufmgr side. Outer Err is unused by C (kept for the seam ABI).
    pub fn restore_block_image(
        record: &XLogReaderState,
        block_id: u8,
        buf: Buffer,
    ) -> PgResult<Result<(), String>>
);

seam_core::seam!(
    // WALRead(state, buf, startptr, count, tli, &errinfo) (xlogreader.c).
    // Inner Err is C's `false` + errinfo; outer Err carries the segment_open
    // callback's ereport surface.
    pub fn wal_read<'a>(
        state: &'a mut XLogReaderState,
        buf: &'a mut [u8],
        startptr: XLogRecPtr,
        count: usize,
        tli: TimeLineID,
    ) -> PgResult<Result<(), WALReadError>>
);
