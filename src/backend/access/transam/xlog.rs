//! Write-ahead log writer and generic record reader.
//!
//! pgrust keeps PostgreSQL's overall WAL shape:
//! generic record assembly and decoding with block references, then
//! resource-manager-specific replay during recovery.
//!
//! The surrounding storage remains intentionally simpler than PostgreSQL:
//! one append-only `wal.log`, no segment rotation, and no control file.

use parking_lot::{Condvar, Mutex};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::backend::access::transam::xloginsert::RegisteredXLogRecord;
use crate::backend::access::transam::xlogreader::{
    BKPBLOCK_FORK_MASK, BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE, BKPBLOCK_SAME_REL,
    BKPBLOCK_WILL_INIT, BKPIMAGE_APPLY, BKPIMAGE_HAS_HOLE, CRC_OFFSET, DecodedBkpBlock,
    DecodedXLogRecord, WAL_PAGE_SIZE, XLOG_BLOCK_HEADER, XLOG_BLOCK_IMAGE_HEADER, XLOG_LONG_PHD,
    XLOG_PAGE_MAGIC, XLOG_RECORD_DATA_HEADER_LONG, XLOG_RECORD_DATA_HEADER_SHORT,
    XLOG_RECORD_HEADER, XLOG_SHORT_PHD, XLP_FIRST_IS_CONTRECORD, XLP_LONG_HEADER,
    XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT, XLR_BLOCK_ID_ORIGIN,
    XLR_BLOCK_ID_TOPLEVEL_XID, XLR_MAX_BLOCK_ID,
};
use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator};

pub mod replay {
    pub use crate::backend::access::transam::xlogrecovery::*;
}

pub use crate::backend::access::transam::xlogreader::WAL_RECORD_LEN;

/// WAL buffer size — accumulate this many bytes before flushing to the kernel.
const WAL_BUF_SIZE: usize = 64 * 1024;
/// Threshold of buffered WAL data that wakes the background writer.
const BG_FLUSH_THRESHOLD: u64 = 1024 * 1024;
const WAL_RECORD_ALIGN: u64 = 8;
const WAL_TIMELINE_ID: u32 = 1;
const WAL_SYSID: u64 = 0;
const WAL_SEG_SIZE: u32 = 16 * 1024 * 1024;

pub type Lsn = u64;
pub const INVALID_LSN: Lsn = 0;

pub const XLOG_FPI: u8 = 0;
pub const XLOG_HEAP_INSERT: u8 = 1;
pub const XLOG_XACT_COMMIT: u8 = 0;

pub const RM_HEAP_ID: u8 = 0;
pub const RM_XACT_ID: u8 = 1;
pub const RM_BTREE_ID: u8 = 2;

pub const REGBUF_STANDARD: u8 = 1 << 0;
pub const REGBUF_WILL_INIT: u8 = 1 << 1;
pub const REGBUF_FORCE_IMAGE: u8 = 1 << 2;

pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x10;
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x11;
pub const XLOG_BTREE_INSERT_META: u8 = 0x12;
pub const XLOG_BTREE_SPLIT_L: u8 = 0x13;
pub const XLOG_BTREE_SPLIT_R: u8 = 0x14;
pub const XLOG_BTREE_NEWROOT: u8 = 0x15;
pub const XLOG_BTREE_VACUUM: u8 = 0x16;
pub const XLOG_BTREE_DELETE: u8 = 0x17;
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0x18;
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x19;
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x1a;
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0x1b;

fn align_up(value: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    (value + (align - 1)) & !(align - 1)
}

fn page_start(lsn: u64) -> u64 {
    (lsn / WAL_PAGE_SIZE as u64) * WAL_PAGE_SIZE as u64
}

fn page_header_size(page_start_lsn: u64) -> usize {
    if page_start_lsn == 0 {
        XLOG_LONG_PHD
    } else {
        XLOG_SHORT_PHD
    }
}

fn encode_page_header(page_start_lsn: u64, info: u16, rem_len: u32) -> Vec<u8> {
    let size = page_header_size(page_start_lsn);
    let mut raw = vec![0u8; size];
    let mut page_info = info;
    if page_start_lsn == 0 {
        page_info |= XLP_LONG_HEADER;
    }
    raw[0..2].copy_from_slice(&XLOG_PAGE_MAGIC.to_le_bytes());
    raw[2..4].copy_from_slice(&page_info.to_le_bytes());
    raw[4..8].copy_from_slice(&WAL_TIMELINE_ID.to_le_bytes());
    raw[8..16].copy_from_slice(&page_start_lsn.to_le_bytes());
    raw[16..20].copy_from_slice(&rem_len.to_le_bytes());
    if page_start_lsn == 0 {
        raw[24..32].copy_from_slice(&WAL_SYSID.to_le_bytes());
        raw[32..36].copy_from_slice(&WAL_SEG_SIZE.to_le_bytes());
        raw[36..40].copy_from_slice(&(WAL_PAGE_SIZE as u32).to_le_bytes());
    }
    raw
}

#[derive(Clone, Copy)]
struct WalPageHeader {
    info: u16,
    pageaddr: u64,
    rem_len: u32,
    size: usize,
}

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    Corrupt(String),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {e}"),
            WalError::Corrupt(msg) => write!(f, "WAL corrupt: {msg}"),
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(value: std::io::Error) -> Self {
        WalError::Io(value)
    }
}

/// Legacy adapter used by existing tests and heap replay code while the rest
/// of the tree migrates to decoded generic WAL records.
#[derive(Debug)]
pub enum WalRecord {
    FullPageImage {
        xid: u32,
        tag: BufferTag,
        page: Box<[u8; PAGE_SIZE]>,
    },
    BtreePageImage {
        xid: u32,
        tag: BufferTag,
        page: Box<[u8; PAGE_SIZE]>,
    },
    HeapInsert {
        xid: u32,
        tag: BufferTag,
        offset_number: u16,
        tuple_data: Vec<u8>,
    },
    XactCommit {
        xid: u32,
    },
}

pub struct WalReader {
    file: std::io::BufReader<File>,
    position: u64,
    file_size: u64,
}

impl WalReader {
    pub fn open(wal_dir: &Path) -> Result<Self, WalError> {
        let path = wal_dir.join("wal.log");
        let file = File::open(&path).map_err(WalError::Io)?;
        let file_size = file.metadata().map_err(WalError::Io)?.len();
        Ok(Self {
            file: std::io::BufReader::new(file),
            position: 0,
            file_size,
        })
    }

    fn read_page_header(&mut self, page_start_lsn: u64) -> Result<Option<WalPageHeader>, WalError> {
        if page_start_lsn >= self.file_size {
            return Ok(None);
        }
        let header_len = if page_start_lsn == 0 {
            XLOG_LONG_PHD
        } else {
            XLOG_SHORT_PHD
        };
        if page_start_lsn + header_len as u64 > self.file_size {
            return Ok(None);
        }
        let mut raw = vec![0u8; header_len];
        self.file.seek(SeekFrom::Start(page_start_lsn))?;
        if self.file.read_exact(&mut raw).is_err() {
            return Ok(None);
        }
        let magic = u16::from_le_bytes(raw[0..2].try_into().unwrap());
        if magic != XLOG_PAGE_MAGIC {
            return Err(WalError::Corrupt("invalid WAL page magic".into()));
        }
        let info = u16::from_le_bytes(raw[2..4].try_into().unwrap());
        let size = if info & XLP_LONG_HEADER != 0 {
            XLOG_LONG_PHD
        } else {
            XLOG_SHORT_PHD
        };
        if size != header_len {
            return Err(WalError::Corrupt("unexpected WAL page header size".into()));
        }
        let pageaddr = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        if pageaddr != page_start_lsn {
            return Err(WalError::Corrupt("WAL page address mismatch".into()));
        }
        let rem_len = u32::from_le_bytes(raw[16..20].try_into().unwrap());
        Ok(Some(WalPageHeader {
            info,
            pageaddr,
            rem_len,
            size,
        }))
    }

    fn next_record_start(&mut self, mut lsn: u64) -> Result<Option<u64>, WalError> {
        loop {
            if lsn >= self.file_size {
                return Ok(None);
            }
            let header = match self.read_page_header(page_start(lsn))? {
                Some(header) => header,
                None => return Ok(None),
            };
            let data_start = header.pageaddr + header.size as u64;
            if lsn < data_start {
                lsn = data_start;
            }
            let aligned = align_up(lsn, WAL_RECORD_ALIGN);
            let page_end = header.pageaddr + WAL_PAGE_SIZE as u64;
            if aligned >= page_end {
                lsn = page_end;
                continue;
            }
            return Ok(Some(aligned));
        }
    }

    fn read_record_bytes(
        &mut self,
        start_lsn: u64,
        len: usize,
    ) -> Result<(Vec<u8>, u64), WalError> {
        let mut out = Vec::with_capacity(len);
        let mut current = start_lsn;
        let mut remaining = len;
        let mut continued = false;

        while remaining > 0 {
            let header = self
                .read_page_header(page_start(current))?
                .ok_or_else(|| WalError::Corrupt("truncated WAL page header".into()))?;
            if continued && header.info & XLP_FIRST_IS_CONTRECORD == 0 {
                return Err(WalError::Corrupt(
                    "missing continuation page header for WAL record".into(),
                ));
            }
            if continued && header.rem_len < remaining as u32 {
                return Err(WalError::Corrupt(
                    "WAL continuation header remaining length mismatch".into(),
                ));
            }
            let data_start = header.pageaddr + header.size as u64;
            if current < data_start {
                current = data_start;
            }
            let page_end = header.pageaddr + WAL_PAGE_SIZE as u64;
            let available = (page_end - current) as usize;
            let chunk_len = available.min(remaining);
            let mut chunk = vec![0u8; chunk_len];
            self.file.seek(SeekFrom::Start(current))?;
            self.file.read_exact(&mut chunk)?;
            out.extend_from_slice(&chunk);
            current += chunk_len as u64;
            remaining -= chunk_len;
            if remaining > 0 {
                current = page_end;
                continued = true;
            }
        }
        Ok((out, current))
    }

    pub fn next_decoded_record(&mut self) -> Result<Option<DecodedXLogRecord>, WalError> {
        let decode = (|| -> Result<Option<DecodedXLogRecord>, WalError> {
            let Some(record_start) = self.next_record_start(self.position)? else {
                return Ok(None);
            };

            let (header, _) = self.read_record_bytes(record_start, XLOG_RECORD_HEADER)?;
            let total_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            if total_len < XLOG_RECORD_HEADER {
                return Ok(None);
            }
            let (mut raw, record_end) = self.read_record_bytes(record_start, total_len)?;

            let stored_crc =
                u32::from_le_bytes(raw[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
            raw[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&[0, 0, 0, 0]);
            if crc32c::crc32c(&raw) != stored_crc {
                return Ok(None);
            }
            raw[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&stored_crc.to_le_bytes());

            let xid = u32::from_le_bytes(raw[4..8].try_into().unwrap());
            let prev = u64::from_le_bytes(raw[8..16].try_into().unwrap());
            let info = raw[16];
            let rmid = raw[17];

            #[derive(Clone)]
            struct PendingBlock {
                block_id: u8,
                tag: BufferTag,
                flags: u8,
                data_len: usize,
                image_len: usize,
                hole_offset: u16,
                hole_length: u16,
            }

            let mut offset = XLOG_RECORD_HEADER;
            let mut data_total = 0usize;
            let mut main_data_len = 0usize;
            let mut origin = None;
            let mut top_level_xid = None;
            let mut blocks = Vec::<PendingBlock>::new();
            let mut previous_rel: Option<RelFileLocator> = None;

            while total_len - offset > data_total {
                let block_id = raw[offset];
                offset += 1;
                match block_id {
                    XLR_BLOCK_ID_DATA_SHORT => {
                        if offset + 1 > raw.len() {
                            return Err(WalError::Corrupt(
                                "truncated WAL short data header".into(),
                            ));
                        }
                        main_data_len = raw[offset] as usize;
                        offset += XLOG_RECORD_DATA_HEADER_SHORT - 1;
                        data_total += main_data_len;
                        break;
                    }
                    XLR_BLOCK_ID_DATA_LONG => {
                        if offset + 4 > raw.len() {
                            return Err(WalError::Corrupt("truncated WAL long data header".into()));
                        }
                        main_data_len =
                            u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap())
                                as usize;
                        offset += XLOG_RECORD_DATA_HEADER_LONG - 1;
                        data_total += main_data_len;
                        break;
                    }
                    XLR_BLOCK_ID_ORIGIN => {
                        if offset + 4 > raw.len() {
                            return Err(WalError::Corrupt("truncated WAL origin header".into()));
                        }
                        origin = Some(u32::from_le_bytes(
                            raw[offset..offset + 4].try_into().unwrap(),
                        ));
                        offset += 4;
                    }
                    XLR_BLOCK_ID_TOPLEVEL_XID => {
                        if offset + 4 > raw.len() {
                            return Err(WalError::Corrupt(
                                "truncated WAL toplevel xid header".into(),
                            ));
                        }
                        top_level_xid = Some(u32::from_le_bytes(
                            raw[offset..offset + 4].try_into().unwrap(),
                        ));
                        offset += 4;
                    }
                    id if id <= XLR_MAX_BLOCK_ID => {
                        if offset + (XLOG_BLOCK_HEADER - 1) > raw.len() {
                            return Err(WalError::Corrupt("truncated WAL block header".into()));
                        }
                        let fork_flags = raw[offset];
                        let mut flags = fork_flags & !BKPBLOCK_FORK_MASK;
                        let fork = ForkNumber::from_u8(fork_flags & BKPBLOCK_FORK_MASK);
                        let data_len =
                            u16::from_le_bytes(raw[offset + 1..offset + 3].try_into().unwrap())
                                as usize;
                        offset += XLOG_BLOCK_HEADER - 1;
                        data_total += data_len;

                        let mut image_len = 0usize;
                        let mut hole_offset = 0u16;
                        let mut hole_length = 0u16;
                        if flags & BKPBLOCK_HAS_IMAGE != 0 {
                            if offset + XLOG_BLOCK_IMAGE_HEADER > raw.len() {
                                return Err(WalError::Corrupt(
                                    "truncated WAL block image header".into(),
                                ));
                            }
                            image_len =
                                u16::from_le_bytes(raw[offset..offset + 2].try_into().unwrap())
                                    as usize;
                            hole_offset =
                                u16::from_le_bytes(raw[offset + 2..offset + 4].try_into().unwrap());
                            let bimg_info = raw[offset + 4];
                            offset += XLOG_BLOCK_IMAGE_HEADER;
                            data_total += image_len;
                            if bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                                if hole_offset as usize > PAGE_SIZE || image_len >= PAGE_SIZE {
                                    return Err(WalError::Corrupt(
                                        "invalid WAL hole-compressed image".into(),
                                    ));
                                }
                                hole_length = (PAGE_SIZE - image_len) as u16;
                            }
                            if bimg_info & BKPIMAGE_APPLY == 0 {
                                flags &= !BKPBLOCK_HAS_IMAGE;
                            }
                        }

                        let rel = if flags & BKPBLOCK_SAME_REL != 0 {
                            previous_rel.ok_or_else(|| {
                                WalError::Corrupt("BKPBLOCK_SAME_REL without prior rel".into())
                            })?
                        } else {
                            if offset + 12 > raw.len() {
                                return Err(WalError::Corrupt("truncated WAL rel locator".into()));
                            }
                            let rel = RelFileLocator {
                                spc_oid: u32::from_le_bytes(
                                    raw[offset..offset + 4].try_into().unwrap(),
                                ),
                                db_oid: u32::from_le_bytes(
                                    raw[offset + 4..offset + 8].try_into().unwrap(),
                                ),
                                rel_number: u32::from_le_bytes(
                                    raw[offset + 8..offset + 12].try_into().unwrap(),
                                ),
                            };
                            offset += 12;
                            previous_rel = Some(rel);
                            rel
                        };

                        if offset + 4 > raw.len() {
                            return Err(WalError::Corrupt("truncated WAL block number".into()));
                        }
                        let block = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap());
                        offset += 4;

                        blocks.push(PendingBlock {
                            block_id: id,
                            tag: BufferTag { rel, fork, block },
                            flags,
                            data_len,
                            image_len,
                            hole_offset,
                            hole_length,
                        });
                    }
                    other => {
                        return Err(WalError::Corrupt(format!("invalid WAL block id {other}")));
                    }
                }
            }

            if total_len - offset != data_total {
                return Err(WalError::Corrupt("WAL header/data length mismatch".into()));
            }

            let mut payload_offset = offset;
            let mut decoded_blocks = Vec::with_capacity(blocks.len());
            for block in blocks {
                let image = if block.image_len > 0 {
                    if payload_offset + block.image_len > raw.len() {
                        return Err(WalError::Corrupt("truncated WAL block image".into()));
                    }
                    let compressed = &raw[payload_offset..payload_offset + block.image_len];
                    payload_offset += block.image_len;
                    let mut page = Box::new([0u8; PAGE_SIZE]);
                    if block.hole_length > 0 {
                        let hole_start = block.hole_offset as usize;
                        let hole_size = block.hole_length as usize;
                        if hole_start + hole_size > PAGE_SIZE
                            || compressed.len() + hole_size != PAGE_SIZE
                        {
                            return Err(WalError::Corrupt(
                                "invalid WAL hole-compressed image".into(),
                            ));
                        }
                        page[..hole_start].copy_from_slice(&compressed[..hole_start]);
                        page[hole_start + hole_size..].copy_from_slice(&compressed[hole_start..]);
                    } else if compressed.len() == PAGE_SIZE {
                        page.copy_from_slice(compressed);
                    } else {
                        return Err(WalError::Corrupt("invalid WAL page image size".into()));
                    }
                    Some(page)
                } else {
                    None
                };

                let data = if block.data_len > 0 {
                    if payload_offset + block.data_len > raw.len() {
                        return Err(WalError::Corrupt("truncated WAL block data".into()));
                    }
                    let bytes = raw[payload_offset..payload_offset + block.data_len].to_vec();
                    payload_offset += block.data_len;
                    bytes
                } else {
                    Vec::new()
                };

                decoded_blocks.push(DecodedBkpBlock {
                    block_id: block.block_id,
                    tag: block.tag,
                    flags: block.flags,
                    data,
                    image,
                    hole_offset: block.hole_offset,
                    hole_length: block.hole_length,
                });
            }

            if payload_offset + main_data_len > raw.len() {
                return Err(WalError::Corrupt("truncated WAL main data".into()));
            }
            let main_data = raw[payload_offset..payload_offset + main_data_len].to_vec();

            Ok(Some(DecodedXLogRecord {
                start_lsn: record_start,
                end_lsn: record_end,
                total_len: total_len as u32,
                xid,
                prev,
                rmid,
                info,
                origin,
                top_level_xid,
                blocks: decoded_blocks,
                main_data,
            }))
        })();

        match decode {
            Ok(Some(record)) => {
                self.position = record.end_lsn;
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(WalError::Corrupt(_)) => {
                self.position = self.file_size;
                Ok(None)
            }
            Err(WalError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.position = self.file_size;
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub fn next_record(&mut self) -> Result<Option<(Lsn, WalRecord)>, WalError> {
        let Some(decoded) = self.next_decoded_record()? else {
            return Ok(None);
        };
        let end_lsn = decoded.end_lsn;
        let record = match (decoded.rmid, decoded.info) {
            (RM_HEAP_ID, XLOG_FPI) => {
                let block = decoded
                    .blocks
                    .first()
                    .ok_or_else(|| WalError::Corrupt("heap FPI missing block ref".into()))?;
                let page = block
                    .image
                    .as_ref()
                    .ok_or_else(|| WalError::Corrupt("heap FPI missing page image".into()))?
                    .clone();
                WalRecord::FullPageImage {
                    xid: decoded.xid,
                    tag: block.tag,
                    page,
                }
            }
            (RM_BTREE_ID, XLOG_FPI)
            | (RM_BTREE_ID, XLOG_BTREE_INSERT_LEAF)
            | (RM_BTREE_ID, XLOG_BTREE_INSERT_UPPER)
            | (RM_BTREE_ID, XLOG_BTREE_INSERT_META)
            | (RM_BTREE_ID, XLOG_BTREE_SPLIT_L)
            | (RM_BTREE_ID, XLOG_BTREE_SPLIT_R)
            | (RM_BTREE_ID, XLOG_BTREE_NEWROOT)
            | (RM_BTREE_ID, XLOG_BTREE_VACUUM)
            | (RM_BTREE_ID, XLOG_BTREE_DELETE)
            | (RM_BTREE_ID, XLOG_BTREE_MARK_PAGE_HALFDEAD)
            | (RM_BTREE_ID, XLOG_BTREE_UNLINK_PAGE)
            | (RM_BTREE_ID, XLOG_BTREE_UNLINK_PAGE_META)
            | (RM_BTREE_ID, XLOG_BTREE_REUSE_PAGE) => {
                let block = decoded
                    .blocks
                    .first()
                    .ok_or_else(|| WalError::Corrupt("btree record missing block ref".into()))?;
                let page = block
                    .image
                    .as_ref()
                    .ok_or_else(|| WalError::Corrupt("btree record missing page image".into()))?
                    .clone();
                WalRecord::BtreePageImage {
                    xid: decoded.xid,
                    tag: block.tag,
                    page,
                }
            }
            (RM_HEAP_ID, XLOG_HEAP_INSERT) => {
                let block = decoded
                    .block_ref(0)
                    .or_else(|| decoded.blocks.first())
                    .ok_or_else(|| WalError::Corrupt("heap insert missing block ref".into()))?;
                if block.data.len() < 4 {
                    return Err(WalError::Corrupt("heap insert block data too short".into()));
                }
                let offset_number = u16::from_le_bytes(block.data[0..2].try_into().unwrap());
                let tuple_len = u16::from_le_bytes(block.data[2..4].try_into().unwrap()) as usize;
                if block.data.len() < 4 + tuple_len {
                    return Err(WalError::Corrupt("heap insert tuple data truncated".into()));
                }
                WalRecord::HeapInsert {
                    xid: decoded.xid,
                    tag: block.tag,
                    offset_number,
                    tuple_data: block.data[4..4 + tuple_len].to_vec(),
                }
            }
            (RM_XACT_ID, XLOG_XACT_COMMIT) => WalRecord::XactCommit { xid: decoded.xid },
            _ => {
                return Err(WalError::Corrupt(format!(
                    "unknown WAL record: rmid={} info={}",
                    decoded.rmid, decoded.info
                )));
            }
        };
        Ok(Some((end_lsn, record)))
    }
}

fn scan_existing_wal_state(wal_dir: &Path) -> Result<(u64, u64), WalError> {
    let path = wal_dir.join("wal.log");
    if !path.exists() {
        return Ok((0, INVALID_LSN));
    }
    if std::fs::metadata(&path)?.len() == 0 {
        return Ok((0, INVALID_LSN));
    }
    let mut reader = WalReader::open(wal_dir)?;
    let mut last_start = INVALID_LSN;
    let mut last_end = 0;
    while let Some(record) = reader.next_decoded_record()? {
        last_start = record.start_lsn;
        last_end = record.end_lsn;
    }
    Ok((last_end, last_start))
}

struct WalWriterInner {
    file: BufWriter<File>,
    insert_lsn: Lsn,
    written_lsn: Lsn,
    flushed_lsn: Lsn,
    last_record_ptr: Lsn,
    pages_with_image: HashSet<BufferTag>,
}

pub struct WalWriter {
    inner: Mutex<WalWriterInner>,
    bg_wake: Condvar,
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        let mut guard = self.inner.lock();
        let _ = guard.file.flush();
        let _ = crate::backend::storage::fsync_file(guard.file.get_ref());
    }
}

impl WalWriter {
    pub fn new(wal_dir: &Path) -> Result<Self, WalError> {
        std::fs::create_dir_all(wal_dir)?;
        let path = wal_dir.join("wal.log");
        let (size, last_record_ptr) = scan_existing_wal_state(wal_dir)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut file = file;
        file.set_len(size)?;
        file.seek(SeekFrom::Start(size))?;
        Ok(Self {
            inner: Mutex::new(WalWriterInner {
                file: BufWriter::with_capacity(WAL_BUF_SIZE, file),
                insert_lsn: size,
                written_lsn: size,
                flushed_lsn: size,
                last_record_ptr,
                pages_with_image: HashSet::new(),
            }),
            bg_wake: Condvar::new(),
        })
    }

    pub fn write_record(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Lsn, WalError> {
        self.write_record_with_rmgr(xid, tag, page, RM_HEAP_ID)
    }

    pub fn write_btree_record(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Lsn, WalError> {
        self.write_record_with_rmgr(xid, tag, page, RM_BTREE_ID)
    }

    pub fn write_record_with_rmgr(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
        rmid: u8,
    ) -> Result<Lsn, WalError> {
        crate::backend::access::transam::xloginsert::xlog_begin_insert();
        crate::backend::access::transam::xloginsert::xlog_register_buffer(
            0,
            tag,
            REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
        );
        crate::backend::access::transam::xloginsert::xlog_register_buffer_image(0, page);
        let lsn =
            crate::backend::access::transam::xloginsert::xlog_insert(self, xid, rmid, XLOG_FPI)?;
        self.inner.lock().pages_with_image.insert(tag);
        Ok(lsn)
    }

    pub fn write_insert(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
        offset_number: u16,
        tuple_data: &[u8],
    ) -> Result<Lsn, WalError> {
        if !self.inner.lock().pages_with_image.contains(&tag) {
            return self.write_record(xid, tag, page);
        }

        let mut payload = Vec::with_capacity(4 + tuple_data.len());
        payload.extend_from_slice(&offset_number.to_le_bytes());
        payload.extend_from_slice(&(tuple_data.len() as u16).to_le_bytes());
        payload.extend_from_slice(tuple_data);

        crate::backend::access::transam::xloginsert::xlog_begin_insert();
        crate::backend::access::transam::xloginsert::xlog_register_buffer(0, tag, REGBUF_STANDARD);
        crate::backend::access::transam::xloginsert::xlog_register_buf_data(0, &payload);
        crate::backend::access::transam::xloginsert::xlog_insert(
            self,
            xid,
            RM_HEAP_ID,
            XLOG_HEAP_INSERT,
        )
    }

    pub fn write_commit(&self, xid: u32) -> Result<Lsn, WalError> {
        crate::backend::access::transam::xloginsert::xlog_begin_insert();
        crate::backend::access::transam::xloginsert::xlog_insert(
            self,
            xid,
            RM_XACT_ID,
            XLOG_XACT_COMMIT,
        )
    }

    pub fn insert_registered_record(
        &self,
        xid: u32,
        rmid: u8,
        info: u8,
        record: RegisteredXLogRecord,
    ) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock();
        let lsn = Self::append_registered_record(&mut guard, xid, rmid, info, record)?;
        self.maybe_wake_bg(&guard);
        Ok(lsn)
    }

    fn append_registered_record(
        guard: &mut parking_lot::MutexGuard<'_, WalWriterInner>,
        xid: u32,
        rmid: u8,
        info: u8,
        record: RegisteredXLogRecord,
    ) -> Result<Lsn, WalError> {
        let mut header = Vec::new();
        let mut payload = Vec::new();
        let main_data_len = record.main_data.len();
        let origin = record.origin;
        let top_level_xid = record.top_level_xid;
        let mut previous_rel: Option<RelFileLocator> = None;

        for (_, block) in record.blocks {
            if block.data.len() > u16::MAX as usize {
                return Err(WalError::Corrupt("WAL block data too large".into()));
            }
            let mut encoded = EncodedBlock::new(block.tag, block.block_id, block.flags, block.data);
            if let Some(image) = block.page_image {
                let (hole_offset, hole_length, compressed_image) = compress_page_image(&image);
                encoded.hole_offset = hole_offset;
                encoded.hole_length = hole_length;
                encoded.image = compressed_image;
            }

            let same_rel = previous_rel == Some(encoded.tag.rel);
            let mut fork_flags = encoded.tag.fork.as_u8() & BKPBLOCK_FORK_MASK;
            if !encoded.image.is_empty() {
                fork_flags |= BKPBLOCK_HAS_IMAGE;
            }
            if !encoded.data.is_empty() {
                fork_flags |= BKPBLOCK_HAS_DATA;
            }
            if encoded.reg_flags & REGBUF_WILL_INIT != 0 {
                fork_flags |= BKPBLOCK_WILL_INIT;
            }
            if same_rel {
                fork_flags |= BKPBLOCK_SAME_REL;
            } else {
                previous_rel = Some(encoded.tag.rel);
            }

            header.push(encoded.block_id);
            header.push(fork_flags);
            header.extend_from_slice(&(encoded.data.len() as u16).to_le_bytes());

            if !encoded.image.is_empty() {
                let mut bimg_info = BKPIMAGE_APPLY;
                if encoded.hole_length > 0 {
                    bimg_info |= BKPIMAGE_HAS_HOLE;
                }
                header.extend_from_slice(&(encoded.image.len() as u16).to_le_bytes());
                header.extend_from_slice(&encoded.hole_offset.to_le_bytes());
                header.push(bimg_info);
            }
            if !same_rel {
                header.extend_from_slice(&encoded.tag.rel.spc_oid.to_le_bytes());
                header.extend_from_slice(&encoded.tag.rel.db_oid.to_le_bytes());
                header.extend_from_slice(&encoded.tag.rel.rel_number.to_le_bytes());
            }
            header.extend_from_slice(&encoded.tag.block.to_le_bytes());

            if !encoded.image.is_empty() {
                payload.extend_from_slice(&encoded.image);
                guard.pages_with_image.insert(encoded.tag);
            }
            if !encoded.data.is_empty() {
                payload.extend_from_slice(&encoded.data);
            }
        }

        if let Some(origin) = origin {
            header.push(XLR_BLOCK_ID_ORIGIN);
            header.extend_from_slice(&origin.to_le_bytes());
        }

        if let Some(top_level_xid) = top_level_xid {
            header.push(XLR_BLOCK_ID_TOPLEVEL_XID);
            header.extend_from_slice(&top_level_xid.to_le_bytes());
        }

        if main_data_len > 0 {
            if main_data_len < 256 {
                header.push(XLR_BLOCK_ID_DATA_SHORT);
                header.push(main_data_len as u8);
            } else {
                header.push(XLR_BLOCK_ID_DATA_LONG);
                header.extend_from_slice(&(main_data_len as u32).to_le_bytes());
            }
            payload.extend_from_slice(&record.main_data);
        }

        let total_len = XLOG_RECORD_HEADER + header.len() + payload.len();
        let start_lsn = Self::align_to_record_start(guard)?;
        let prev_ptr = guard.last_record_ptr;

        let mut raw = vec![0u8; total_len];
        raw[0..4].copy_from_slice(&(total_len as u32).to_le_bytes());
        raw[4..8].copy_from_slice(&xid.to_le_bytes());
        raw[8..16].copy_from_slice(&prev_ptr.to_le_bytes());
        raw[16] = info;
        raw[17] = rmid;
        raw[18..20].fill(0);
        let header_end = XLOG_RECORD_HEADER + header.len();
        raw[XLOG_RECORD_HEADER..header_end].copy_from_slice(&header);
        raw[header_end..].copy_from_slice(&payload);
        let crc = crc32c::crc32c(&raw);
        raw[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        let end_lsn = Self::write_record_payload(guard, &raw)?;
        guard.last_record_ptr = start_lsn;
        Ok(end_lsn)
    }

    fn align_to_record_start(
        guard: &mut parking_lot::MutexGuard<'_, WalWriterInner>,
    ) -> Result<Lsn, WalError> {
        loop {
            let current_page = page_start(guard.insert_lsn);
            if guard.insert_lsn == current_page {
                let header = encode_page_header(current_page, 0, 0);
                guard.file.write_all(&header)?;
                guard.insert_lsn += header.len() as u64;
                continue;
            }

            let aligned = align_up(guard.insert_lsn, WAL_RECORD_ALIGN);
            let current_page_end = current_page + WAL_PAGE_SIZE as u64;
            if aligned >= current_page_end {
                let padding_len = (current_page_end - guard.insert_lsn) as usize;
                if padding_len > 0 {
                    guard.file.write_all(&vec![0u8; padding_len])?;
                }
                guard.insert_lsn = current_page_end;
                continue;
            }

            let padding_len = (aligned - guard.insert_lsn) as usize;
            if padding_len > 0 {
                guard.file.write_all(&vec![0u8; padding_len])?;
                guard.insert_lsn = aligned;
            }
            return Ok(guard.insert_lsn);
        }
    }

    fn write_record_payload(
        guard: &mut parking_lot::MutexGuard<'_, WalWriterInner>,
        raw: &[u8],
    ) -> Result<Lsn, WalError> {
        let mut written = 0usize;
        while written < raw.len() {
            let current_page = page_start(guard.insert_lsn);
            let current_page_end = current_page + WAL_PAGE_SIZE as u64;
            let available = (current_page_end - guard.insert_lsn) as usize;
            let chunk_len = available.min(raw.len() - written);
            guard.file.write_all(&raw[written..written + chunk_len])?;
            guard.insert_lsn += chunk_len as u64;
            written += chunk_len;

            if written < raw.len() {
                debug_assert_eq!(guard.insert_lsn, current_page_end);
                let continuation = encode_page_header(
                    guard.insert_lsn,
                    XLP_FIRST_IS_CONTRECORD,
                    (raw.len() - written) as u32,
                );
                guard.file.write_all(&continuation)?;
                guard.insert_lsn += continuation.len() as u64;
            }
        }
        Ok(guard.insert_lsn)
    }

    pub fn flush(&self) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock();
        Self::flush_inner(&mut guard)
    }

    pub fn flush_to(&self, target_lsn: Lsn) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock();
        if guard.flushed_lsn >= target_lsn {
            return Ok(guard.flushed_lsn);
        }
        Self::flush_inner(&mut guard)
    }

    fn flush_inner(
        guard: &mut parking_lot::MutexGuard<'_, WalWriterInner>,
    ) -> Result<Lsn, WalError> {
        if guard.flushed_lsn < guard.insert_lsn {
            if guard.written_lsn < guard.insert_lsn {
                guard.file.flush()?;
                guard.written_lsn = guard.insert_lsn;
            }
            crate::backend::storage::fsync_file(guard.file.get_ref())?;
            guard.flushed_lsn = guard.insert_lsn;
        }
        Ok(guard.flushed_lsn)
    }

    pub fn bg_flush(&self) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock();
        if guard.written_lsn < guard.insert_lsn {
            guard.file.flush()?;
            guard.written_lsn = guard.insert_lsn;
        }
        Ok(guard.written_lsn)
    }

    pub fn bg_wait(&self, interval: Duration) {
        let mut guard = self.inner.lock();
        if guard.insert_lsn - guard.written_lsn >= BG_FLUSH_THRESHOLD {
            return;
        }
        self.bg_wake.wait_for(&mut guard, interval);
    }

    fn maybe_wake_bg(&self, guard: &parking_lot::MutexGuard<'_, WalWriterInner>) {
        if guard.insert_lsn - guard.written_lsn >= BG_FLUSH_THRESHOLD {
            self.bg_wake.notify_one();
        }
    }

    pub fn insert_lsn(&self) -> Lsn {
        self.inner.lock().insert_lsn
    }

    pub fn flushed_lsn(&self) -> Lsn {
        self.inner.lock().flushed_lsn
    }
}

struct EncodedBlock {
    tag: BufferTag,
    block_id: u8,
    reg_flags: u8,
    data: Vec<u8>,
    image: Vec<u8>,
    hole_offset: u16,
    hole_length: u16,
}

impl EncodedBlock {
    fn new(tag: BufferTag, block_id: u8, flags: u8, data: Vec<u8>) -> Self {
        Self {
            tag,
            block_id,
            reg_flags: flags,
            data,
            image: Vec::new(),
            hole_offset: 0,
            hole_length: 0,
        }
    }
}

fn compress_page_image(page: &[u8; PAGE_SIZE]) -> (u16, u16, Vec<u8>) {
    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    let pd_upper = u16::from_le_bytes([page[14], page[15]]) as usize;
    if pd_upper > pd_lower && pd_upper <= PAGE_SIZE && pd_lower > 0 {
        let mut compressed = Vec::with_capacity(PAGE_SIZE - (pd_upper - pd_lower));
        compressed.extend_from_slice(&page[..pd_lower]);
        compressed.extend_from_slice(&page[pd_upper..]);
        (pd_lower as u16, (pd_upper - pd_lower) as u16, compressed)
    } else {
        (0, 0, page.to_vec())
    }
}

pub struct WalBgWriter {
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl WalBgWriter {
    pub fn start(wal: Arc<WalWriter>, interval: Duration) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_flag = Arc::clone(&shutdown);
        let handle = std::thread::Builder::new()
            .name("wal-bg-writer".into())
            .spawn(move || {
                while !shutdown_flag.load(Ordering::Relaxed) {
                    wal.bg_wait(interval);
                    let _ = wal.bg_flush();
                }
            })
            .expect("failed to spawn WAL background writer thread");
        Self {
            shutdown,
            handle: Some(handle),
        }
    }
}

impl Drop for WalBgWriter {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::transam::xloginsert::{
        xlog_begin_insert, xlog_insert, xlog_register_buf_data, xlog_register_buffer,
        xlog_register_buffer_image, xlog_register_data,
    };
    use std::fs;

    fn test_dir(label: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("pgrust_wal_test_{label}"));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn test_tag(block: u32) -> BufferTag {
        BufferTag {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 42,
            },
            fork: ForkNumber::Main,
            block,
        }
    }

    #[test]
    fn generic_record_roundtrip_decodes_blocks_and_main_data() {
        let dir = test_dir("generic_roundtrip");
        let wal = WalWriter::new(&dir).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        page[100] = 0xaa;
        page[PAGE_SIZE - 1] = 0xbb;

        xlog_begin_insert();
        xlog_register_buffer(0, test_tag(7), REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
        xlog_register_buffer_image(0, &page);
        xlog_register_buf_data(0, &[1, 2, 3, 4]);
        xlog_register_data(&[9, 8, 7]);
        let end_lsn = xlog_insert(&wal, 44, RM_BTREE_ID, XLOG_BTREE_SPLIT_L).unwrap();
        wal.flush().unwrap();

        let mut reader = WalReader::open(&dir).unwrap();
        let record = reader.next_decoded_record().unwrap().unwrap();
        assert_eq!(record.end_lsn, end_lsn);
        assert_eq!(record.xid, 44);
        assert_eq!(record.rmid, RM_BTREE_ID);
        assert_eq!(record.info, XLOG_BTREE_SPLIT_L);
        assert_eq!(record.main_data, vec![9, 8, 7]);
        assert_eq!(record.blocks.len(), 1);
        assert_eq!(record.blocks[0].data, vec![1, 2, 3, 4]);
        assert_eq!(record.blocks[0].image.as_ref().unwrap()[100], 0xaa);
        assert_eq!(
            record.blocks[0].image.as_ref().unwrap()[PAGE_SIZE - 1],
            0xbb
        );
    }

    #[test]
    fn legacy_heap_insert_adapter_still_roundtrips() {
        let dir = test_dir("heap_insert_adapter");
        let wal = WalWriter::new(&dir).unwrap();
        let page = [0u8; PAGE_SIZE];
        wal.write_record(1, test_tag(0), &page).unwrap();
        wal.write_insert(1, test_tag(0), &page, 3, &[4, 5, 6])
            .unwrap();
        wal.flush().unwrap();

        let mut reader = WalReader::open(&dir).unwrap();
        assert!(matches!(
            reader.next_record().unwrap().unwrap().1,
            WalRecord::FullPageImage { .. }
        ));
        match reader.next_record().unwrap().unwrap().1 {
            WalRecord::HeapInsert {
                xid,
                tag,
                offset_number,
                tuple_data,
            } => {
                assert_eq!(xid, 1);
                assert_eq!(tag.block, 0);
                assert_eq!(offset_number, 3);
                assert_eq!(tuple_data, vec![4, 5, 6]);
            }
            other => panic!("unexpected WAL record: {other:?}"),
        }
    }

    #[test]
    fn record_crc_matches_on_disk_bytes() {
        let dir = test_dir("crc");
        let wal = WalWriter::new(&dir).unwrap();
        let page = [0u8; PAGE_SIZE];
        wal.write_record(7, test_tag(1), &page).unwrap();
        wal.flush().unwrap();

        let mut reader = WalReader::open(&dir).unwrap();
        let record_start = reader.next_record_start(0).unwrap().unwrap();
        let (raw, _) = reader
            .read_record_bytes(record_start, WAL_RECORD_LEN)
            .unwrap();
        let total_len = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
        let mut check = reader.read_record_bytes(record_start, total_len).unwrap().0;
        let crc = u32::from_le_bytes(check[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
        check[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&[0, 0, 0, 0]);
        assert_eq!(crc32c::crc32c(&check), crc);
    }
}
