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
    BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE, BKPBLOCK_STANDARD, BKPBLOCK_WILL_INIT, CRC_OFFSET,
    DecodedBkpBlock, DecodedXLogRecord, XLOG_BLOCK_HEADER,
};
use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator};

pub mod replay {
    pub use crate::backend::access::transam::xlogrecovery::*;
}

pub use crate::backend::access::transam::xlogreader::{WAL_RECORD_LEN, XLOG_RECORD_HEADER};

/// WAL buffer size — accumulate this many bytes before flushing to the kernel.
const WAL_BUF_SIZE: usize = 64 * 1024;
/// Threshold of buffered WAL data that wakes the background writer.
const BG_FLUSH_THRESHOLD: u64 = 1024 * 1024;

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

    pub fn next_decoded_record(&mut self) -> Result<Option<DecodedXLogRecord>, WalError> {
        if self.position >= self.file_size {
            return Ok(None);
        }

        let remaining = self.file_size - self.position;
        if remaining < XLOG_RECORD_HEADER as u64 {
            return Ok(None);
        }

        let mut header = [0u8; XLOG_RECORD_HEADER];
        if self.file.read_exact(&mut header).is_err() {
            return Ok(None);
        }

        let total_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        if total_len < XLOG_RECORD_HEADER || self.position + total_len as u64 > self.file_size {
            return Ok(None);
        }

        let mut raw = vec![0u8; total_len];
        raw[..XLOG_RECORD_HEADER].copy_from_slice(&header);
        if self.file.read_exact(&mut raw[XLOG_RECORD_HEADER..]).is_err() {
            return Ok(None);
        }

        let stored_crc = u32::from_le_bytes(raw[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
        raw[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&[0, 0, 0, 0]);
        if crc32c::crc32c(&raw) != stored_crc {
            return Ok(None);
        }
        raw[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&stored_crc.to_le_bytes());

        let xid = u32::from_le_bytes(raw[4..8].try_into().unwrap());
        let prev = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        let main_data_len = u32::from_le_bytes(raw[16..20].try_into().unwrap()) as usize;
        let block_count = raw[20] as usize;
        let rmid = raw[21];
        let info = raw[22];
        let record_end = self.position + total_len as u64;

        let mut offset = XLOG_RECORD_HEADER;
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            if offset + XLOG_BLOCK_HEADER > raw.len() {
                return Err(WalError::Corrupt("truncated WAL block header".into()));
            }
            let block_id = raw[offset];
            let flags = raw[offset + 1];
            let fork = ForkNumber::from_u8(raw[offset + 2]);
            let tag = BufferTag {
                rel: RelFileLocator {
                    spc_oid: u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().unwrap()),
                    db_oid: u32::from_le_bytes(raw[offset + 8..offset + 12].try_into().unwrap()),
                    rel_number: u32::from_le_bytes(
                        raw[offset + 12..offset + 16].try_into().unwrap(),
                    ),
                },
                fork,
                block: u32::from_le_bytes(raw[offset + 16..offset + 20].try_into().unwrap()),
            };
            let data_len = u32::from_le_bytes(raw[offset + 20..offset + 24].try_into().unwrap())
                as usize;
            let image_len = u32::from_le_bytes(raw[offset + 24..offset + 28].try_into().unwrap())
                as usize;
            let hole_offset =
                u16::from_le_bytes(raw[offset + 28..offset + 30].try_into().unwrap());
            let hole_length =
                u16::from_le_bytes(raw[offset + 30..offset + 32].try_into().unwrap());
            offset += XLOG_BLOCK_HEADER;

            let image = if flags & BKPBLOCK_HAS_IMAGE != 0 {
                if offset + image_len > raw.len() {
                    return Err(WalError::Corrupt("truncated WAL block image".into()));
                }
                let compressed = &raw[offset..offset + image_len];
                offset += image_len;
                let mut page = Box::new([0u8; PAGE_SIZE]);
                if hole_length > 0 {
                    let hole_start = hole_offset as usize;
                    let hole_size = hole_length as usize;
                    if hole_start + hole_size > PAGE_SIZE || compressed.len() + hole_size != PAGE_SIZE
                    {
                        return Err(WalError::Corrupt("invalid WAL hole-compressed image".into()));
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

            let data = if flags & BKPBLOCK_HAS_DATA != 0 {
                if offset + data_len > raw.len() {
                    return Err(WalError::Corrupt("truncated WAL block data".into()));
                }
                let bytes = raw[offset..offset + data_len].to_vec();
                offset += data_len;
                bytes
            } else {
                Vec::new()
            };

            blocks.push(DecodedBkpBlock {
                block_id,
                tag,
                flags,
                data,
                image,
                hole_offset,
                hole_length,
            });
        }

        if offset + main_data_len > raw.len() {
            return Err(WalError::Corrupt("truncated WAL main data".into()));
        }
        let main_data = raw[offset..offset + main_data_len].to_vec();
        self.position = record_end;

        Ok(Some(DecodedXLogRecord {
            end_lsn: record_end,
            total_len: total_len as u32,
            xid,
            prev,
            rmid,
            info,
            blocks,
            main_data,
        }))
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

struct WalWriterInner {
    file: BufWriter<File>,
    insert_lsn: Lsn,
    written_lsn: Lsn,
    flushed_lsn: Lsn,
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
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut file = file;
        let size = file.metadata()?.len();
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            inner: Mutex::new(WalWriterInner {
                file: BufWriter::with_capacity(WAL_BUF_SIZE, file),
                insert_lsn: size,
                written_lsn: size,
                flushed_lsn: size,
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
        let lsn = crate::backend::access::transam::xloginsert::xlog_insert(
            self, xid, rmid, XLOG_FPI,
        )?;
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
        let main_data_len = record.main_data.len();
        let mut total_len = XLOG_RECORD_HEADER + main_data_len;
        let mut encoded_blocks = Vec::with_capacity(record.blocks.len());

        for (_, block) in record.blocks {
            let mut encoded =
                EncodedBlock::new(block.tag, block.block_id, block.flags, block.data);
            if let Some(image) = block.page_image {
                let (hole_offset, hole_length, compressed_image) = compress_page_image(&image);
                encoded.hole_offset = hole_offset;
                encoded.hole_length = hole_length;
                encoded.image = compressed_image;
            }
            total_len += XLOG_BLOCK_HEADER + encoded.image.len() + encoded.data.len();
            encoded_blocks.push(encoded);
        }

        let prev_lsn = guard.insert_lsn;
        let end_lsn = prev_lsn + total_len as Lsn;

        let mut raw = vec![0u8; total_len];
        raw[0..4].copy_from_slice(&(total_len as u32).to_le_bytes());
        raw[4..8].copy_from_slice(&xid.to_le_bytes());
        raw[8..16].copy_from_slice(&prev_lsn.to_le_bytes());
        raw[16..20].copy_from_slice(&(main_data_len as u32).to_le_bytes());
        raw[20] = encoded_blocks.len() as u8;
        raw[21] = rmid;
        raw[22] = info;
        raw[23] = 0;

        let mut offset = XLOG_RECORD_HEADER;
        for block in encoded_blocks {
            let mut flags = 0u8;
            if !block.image.is_empty() {
                flags |= BKPBLOCK_HAS_IMAGE;
            }
            if !block.data.is_empty() {
                flags |= BKPBLOCK_HAS_DATA;
            }
            if block.reg_flags & REGBUF_WILL_INIT != 0 {
                flags |= BKPBLOCK_WILL_INIT;
            }
            if block.reg_flags & REGBUF_STANDARD != 0 {
                flags |= BKPBLOCK_STANDARD;
            }
            raw[offset] = block.block_id;
            raw[offset + 1] = flags;
            raw[offset + 2] = block.tag.fork.as_u8();
            raw[offset + 4..offset + 8].copy_from_slice(&block.tag.rel.spc_oid.to_le_bytes());
            raw[offset + 8..offset + 12].copy_from_slice(&block.tag.rel.db_oid.to_le_bytes());
            raw[offset + 12..offset + 16]
                .copy_from_slice(&block.tag.rel.rel_number.to_le_bytes());
            raw[offset + 16..offset + 20].copy_from_slice(&block.tag.block.to_le_bytes());
            raw[offset + 20..offset + 24].copy_from_slice(&(block.data.len() as u32).to_le_bytes());
            raw[offset + 24..offset + 28]
                .copy_from_slice(&(block.image.len() as u32).to_le_bytes());
            raw[offset + 28..offset + 30].copy_from_slice(&block.hole_offset.to_le_bytes());
            raw[offset + 30..offset + 32].copy_from_slice(&block.hole_length.to_le_bytes());
            offset += XLOG_BLOCK_HEADER;

            if !block.image.is_empty() {
                raw[offset..offset + block.image.len()].copy_from_slice(&block.image);
                offset += block.image.len();
            }
            if !block.data.is_empty() {
                raw[offset..offset + block.data.len()].copy_from_slice(&block.data);
                offset += block.data.len();
            }
            if !block.image.is_empty() {
                guard.pages_with_image.insert(block.tag);
            }
        }

        raw[offset..offset + main_data_len].copy_from_slice(&record.main_data);
        let crc = crc32c::crc32c(&raw);
        raw[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        guard.file.write_all(&raw)?;
        guard.insert_lsn = end_lsn;
        Ok(end_lsn)
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

    fn flush_inner(guard: &mut parking_lot::MutexGuard<'_, WalWriterInner>) -> Result<Lsn, WalError> {
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
        (
            pd_lower as u16,
            (pd_upper - pd_lower) as u16,
            compressed,
        )
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
    use std::io::Read;

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
        assert_eq!(record.blocks[0].image.as_ref().unwrap()[PAGE_SIZE - 1], 0xbb);
    }

    #[test]
    fn legacy_heap_insert_adapter_still_roundtrips() {
        let dir = test_dir("heap_insert_adapter");
        let wal = WalWriter::new(&dir).unwrap();
        let page = [0u8; PAGE_SIZE];
        wal.write_record(1, test_tag(0), &page).unwrap();
        wal.write_insert(1, test_tag(0), &page, 3, &[4, 5, 6]).unwrap();
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

        let mut raw = Vec::new();
        fs::File::open(dir.join("wal.log"))
            .unwrap()
            .read_to_end(&mut raw)
            .unwrap();

        let crc = u32::from_le_bytes(raw[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
        let mut check = raw.clone();
        check[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&[0, 0, 0, 0]);
        assert_eq!(crc32c::crc32c(&check), crc);
    }
}
