#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use types_core::{
    BlockNumber, Buffer, ForkNumber, RepOriginId, RmgrId, TimeLineID, TransactionId, XLogRecPtr,
    XLogSegNo,
};
use types_error::{PgError, PgResult};
use types_storage::RelFileLocator;

pub const XLOG_BLCKSZ: usize = 8192;
pub const XLR_MAX_BLOCK_ID: usize = 32;

pub const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
pub const BKPBLOCK_HAS_DATA: u8 = 0x20;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40;
pub const BKPBLOCK_SAME_REL: u8 = 0x80;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WALSegmentContext {
    pub ws_segsize: i32,
}

// `bkp_image`/`data`: C's `char *` payloads; valid while the reader's current record is unchanged.
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
    pub bkp_image: *const u8,
    pub hole_offset: u16,
    pub hole_length: u16,
    pub bimg_len: u16,
    pub bimg_info: u8,
    pub has_data: bool,
    pub data: *const u8,
    pub data_len: u16,
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
        bkp_image: core::ptr::null(),
        hole_offset: 0,
        hole_length: 0,
        bimg_len: 0,
        bimg_info: 0,
        has_data: false,
        data: core::ptr::null(),
        data_len: 0,
    };

    /// # Safety
    /// The owning reader's current record must still be the one this block
    /// was marshaled from (the pointers target its decode buffer).
    pub unsafe fn bkp_image_bytes(&self) -> &[u8] {
        if self.bkp_image.is_null() {
            return &[];
        }
        core::slice::from_raw_parts(self.bkp_image, self.bimg_len as usize)
    }

    /// # Safety
    /// Same contract as [`Self::bkp_image_bytes`].
    pub unsafe fn data_bytes(&self) -> &[u8] {
        if self.data.is_null() {
            return &[];
        }
        core::slice::from_raw_parts(self.data, self.data_len as usize)
    }
}

impl Default for DecodedBkpBlock {
    fn default() -> Self {
        DecodedBkpBlock::EMPTY
    }
}

// The consumer-facing projection of C's DecodedXLogRecord.
#[derive(Clone, Copy, Debug)]
pub struct DecodedXLogRecord {
    pub lsn: XLogRecPtr,
    pub next_lsn: XLogRecPtr,
    pub xl_tot_len: u32,
    pub xl_xid: TransactionId,
    pub xl_prev: XLogRecPtr,
    pub xl_info: u8,
    pub xl_rmid: RmgrId,
    pub record_origin: RepOriginId,
    pub toplevel_xid: TransactionId,
    pub main_data: *const u8,
    pub main_data_len: u32,
    pub max_block_id: i8,
    pub blocks: [DecodedBkpBlock; XLR_MAX_BLOCK_ID + 1],
}

impl DecodedXLogRecord {
    /// # Safety
    /// Same contract as [`DecodedBkpBlock::bkp_image_bytes`].
    pub unsafe fn main_data_bytes(&self) -> &[u8] {
        if self.main_data.is_null() {
            return &[];
        }
        core::slice::from_raw_parts(self.main_data, self.main_data_len as usize)
    }
}

impl Default for DecodedXLogRecord {
    fn default() -> Self {
        DecodedXLogRecord {
            lsn: 0,
            next_lsn: 0,
            xl_tot_len: 0,
            xl_xid: 0,
            xl_prev: 0,
            xl_info: 0,
            xl_rmid: 0,
            record_origin: 0,
            toplevel_xid: 0,
            main_data: core::ptr::null(),
            main_data_len: 0,
            max_block_id: -1,
            blocks: [DecodedBkpBlock::EMPTY; XLR_MAX_BLOCK_ID + 1],
        }
    }
}

// Trimmed to what rmgr callbacks and xlogutils touch; `private_end_of_wal`
// is C's ReadLocalXLogPageNoWaitPrivate.end_of_wal reached via private_data.
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
    pub nonblocking: bool,
}

impl XLogReaderState {
    pub fn has_block_ref(&self, block_id: u8) -> bool {
        match &self.record {
            Some(r) => {
                i32::from(block_id) <= i32::from(r.max_block_id) && r.blocks[block_id as usize].in_use
            }
            None => false,
        }
    }

    pub fn block(&self, block_id: u8) -> &DecodedBkpBlock {
        &self
            .record
            .as_ref()
            .expect("XLogRecGetBlock on a reader with no decoded record")
            .blocks[block_id as usize]
    }

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

    /// XLogRecGetBlockTag (xlogreader.c:1993-2008): like `block_tag_extended`
    /// except that the block reference must exist and there is no access to
    /// prefetch_buffer; an absent reference is elog(ERROR, "could not locate
    /// backup block with ID %d in WAL record") (xlogreader.c:2001) — a
    /// catchable ERROR, never a panic.
    pub fn block_tag(&self, block_id: u8) -> PgResult<(RelFileLocator, ForkNumber, BlockNumber)> {
        match self.block_tag_extended(block_id) {
            Some((rlocator, forknum, blkno, _)) => Ok((rlocator, forknum, blkno)),
            None => Err(missing_block_tag(block_id)),
        }
    }

    pub fn has_block_image(&self, block_id: u8) -> bool {
        self.block(block_id).has_image
    }

    pub fn block_image_apply(&self, block_id: u8) -> bool {
        self.block(block_id).apply_image
    }
}

// XLogRecGetBlockTag (xlogreader.c:2001): elog(ERROR, "could not locate backup
// block with ID %d in WAL record") — XX000 at ERROR level.
#[cold]
#[inline(never)]
fn missing_block_tag(block_id: u8) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "could not locate backup block with ID {block_id} in WAL record"
    )))
}

#[derive(Clone, Copy, Debug)]
pub struct WALReadError {
    pub wre_errno: i32,
    pub wre_off: i32,
    pub wre_req: i32,
    pub wre_read: i32,
    pub wre_seg: WALOpenSegment,
}

seam_core::seam!(
    // RestoreBlockImage: inner Err is C's `false` + errormsg; page reached via `buf` (bufmgr).
    pub fn restore_block_image(
        record: &XLogReaderState,
        block_id: u8,
        buf: Buffer,
    ) -> PgResult<Result<(), String>>
);

seam_core::seam!(
    // WALRead: inner Err is C's `false` + errinfo; outer Err is segment_open's ereport surface.
    pub fn wal_read<'a>(
        state: &'a mut XLogReaderState,
        buf: &'a mut [u8],
        startptr: XLogRecPtr,
        count: usize,
        tli: TimeLineID,
    ) -> PgResult<Result<(), WALReadError>>
);

#[cfg(test)]
mod block_tag_tests {
    use super::*;

    // XLogRecGetBlockTag (xlogreader.c:1993-2008): an absent block reference is
    // elog(ERROR, "could not locate backup block with ID %d in WAL record");
    // a present one yields its (rlocator, forknum, blkno) without the
    // prefetch buffer (audit-18.6 w2-053, row xlogreader-3a26fc89).
    #[test]
    fn block_tag_is_c_exact() {
        let mut rec = DecodedXLogRecord::default();
        rec.max_block_id = 0;
        rec.blocks[0] = DecodedBkpBlock {
            in_use: true,
            rlocator: RelFileLocator::new(1663, 5, 42),
            forknum: ForkNumber::MAIN_FORKNUM,
            blkno: 3,
            prefetch_buffer: 9,
            ..DecodedBkpBlock::EMPTY
        };
        let record = XLogReaderState { record: Some(rec), ..Default::default() };
        assert_eq!(
            record.block_tag(0).unwrap(),
            (RelFileLocator::new(1663, 5, 42), ForkNumber::MAIN_FORKNUM, 3)
        );
        let err = record.block_tag(1).expect_err("block 1 is not registered");
        assert_eq!(err.message(), "could not locate backup block with ID 1 in WAL record");
        assert_eq!(err.level(), types_error::ERROR);
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);

        // No decoded record at all: still the C error, never a panic.
        let empty = XLogReaderState::default();
        let err = empty.block_tag(0).expect_err("no record");
        assert_eq!(err.message(), "could not locate backup block with ID 0 in WAL record");
    }

    // XLogRecHasBlockRef (xlogreader.h:426-429): `block_id <= max_block_id` in
    // int arithmetic, so ids 128..=255 are simply absent; the pre-fix `as i8`
    // cast made them negative (always <= max_block_id) and indexed past the
    // 33-entry blocks array. Detail bug_1203e574.
    #[test]
    fn has_block_ref_treats_high_block_ids_as_absent() {
        let mut rec = DecodedXLogRecord::default();
        rec.max_block_id = 0;
        rec.blocks[0] = DecodedBkpBlock { in_use: true, ..DecodedBkpBlock::EMPTY };
        let record = XLogReaderState { record: Some(rec), ..Default::default() };
        assert!(record.has_block_ref(0));
        assert!(!record.has_block_ref(1));
        assert!(!record.has_block_ref(128));
        assert!(!record.has_block_ref(255));
        assert!(!XLogReaderState::default().has_block_ref(0));
    }
}
