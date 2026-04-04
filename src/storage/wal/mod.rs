//! Write-Ahead Log writer.
//!
//! Provides append-only WAL recording with explicit flush control. Each record
//! is fixed-size: a 32-byte header followed by a full page image (8192 bytes),
//! totalling 8224 bytes per record.
//!
//! The LSN (Log Sequence Number) assigned to a record is the byte offset
//! immediately *after* the record in the WAL file — i.e. the position of the
//! next record. LSN 0 (`INVALID_LSN`) means no WAL has been written.
//!
//! ## Durability contract
//!
//! Callers must call `flush()` before marking a transaction as committed.
//! Once WAL is durable, data pages can be written to storage without an
//! fsync — the WAL can be replayed to recover any page lost in a crash.
//!
//! ## Record layout (8224 bytes)
//!
//! ```text
//!  bytes  0-3  : xl_tot_len   (u32 LE) — total length of entire record
//!  bytes  4-7  : xl_xid       (u32 LE) — transaction id
//!  bytes  8-15 : xl_prev      (u64 LE) — LSN of previous record
//!  byte  16    : xl_info      (u8)     — flag bits
//!  byte  17    : xl_rmid      (u8)     — resource manager id
//!  bytes 18-19 : _pad         (2 bytes, zero)
//!  bytes 20-23 : xl_crc       (u32 LE) — CRC32C of entire record (with crc field zeroed)
//!  --- block header (simplified, one block per record) ---
//!  bytes 24-27 : spc_oid      (u32 LE)
//!  bytes 28-31 : db_oid       (u32 LE)
//!  bytes 32-35 : rel_number   (u32 LE)
//!  byte  36    : fork         (u8)
//!  bytes 37-39 : _pad2        (3 bytes, zero)
//!  bytes 40-43 : block        (u32 LE)
//!  bytes 44..  : page data    (PAGE_SIZE = 8192 bytes)
//! ```

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::storage::buffer::{BufferTag, PAGE_SIZE};

/// A Log Sequence Number — the byte offset immediately after the record.
/// `INVALID_LSN` (0) means no WAL has been written.
pub type Lsn = u64;
pub const INVALID_LSN: Lsn = 0;

/// XLogRecord header: tot_len(4) + xid(4) + prev(8) + info(1) + rmid(1) + pad(2) + crc(4) = 24
const XLOG_RECORD_HEADER: usize = 24;
/// Block header: spc_oid(4) + db_oid(4) + rel_number(4) + fork(1) + pad(3) + block(4) = 20
const BLOCK_HEADER: usize = 20;
const WAL_RECORD_HEADER: usize = XLOG_RECORD_HEADER + BLOCK_HEADER; // 44
pub const WAL_RECORD_LEN: usize = WAL_RECORD_HEADER + PAGE_SIZE;
/// Offset of the CRC field within the record.
const CRC_OFFSET: usize = 20;

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {e}"),
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

struct WalWriterInner {
    file: File,
    /// Byte offset immediately after the last inserted record.
    /// Zero if no records have been written yet.
    insert_lsn: Lsn,
    /// Byte offset up to which the WAL file has been fsynced.
    flushed_lsn: Lsn,
}

pub struct WalWriter {
    inner: Mutex<WalWriterInner>,
}

impl WalWriter {
    /// Open (or create) the WAL file in `wal_dir`.
    ///
    /// If the file already exists (e.g. after a restart), the insert/flushed
    /// LSN is initialised to the current file size so new records are appended
    /// after the existing ones.
    pub fn new(wal_dir: &Path) -> Result<Self, WalError> {
        std::fs::create_dir_all(wal_dir)?;
        let path = wal_dir.join("wal.log");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let size = file.metadata()?.len();
        Ok(WalWriter {
            inner: Mutex::new(WalWriterInner {
                file,
                insert_lsn: size,
                flushed_lsn: size,
            }),
        })
    }

    /// Append a full-page-image WAL record for `tag` modified by transaction
    /// `xid`. Returns the LSN assigned to this record (byte offset after it).
    pub fn write_record(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();

        let prev_lsn = guard.insert_lsn;
        let lsn = prev_lsn + WAL_RECORD_LEN as Lsn;

        let mut record = [0u8; WAL_RECORD_LEN];

        // XLogRecord header
        record[0..4].copy_from_slice(&(WAL_RECORD_LEN as u32).to_le_bytes()); // xl_tot_len
        record[4..8].copy_from_slice(&xid.to_le_bytes());                      // xl_xid
        record[8..16].copy_from_slice(&prev_lsn.to_le_bytes());                // xl_prev
        record[16] = 0;                                                         // xl_info
        record[17] = 0;                                                         // xl_rmid
        // bytes 18-19: padding (already zero)
        // bytes 20-23: xl_crc — filled below after computing CRC

        // Block header
        record[24..28].copy_from_slice(&tag.rel.spc_oid.to_le_bytes());
        record[28..32].copy_from_slice(&tag.rel.db_oid.to_le_bytes());
        record[32..36].copy_from_slice(&tag.rel.rel_number.to_le_bytes());
        record[36] = tag.fork.as_u8();
        // bytes 37-39: padding (already zero)
        record[40..44].copy_from_slice(&tag.block.to_le_bytes());

        // Page data
        record[WAL_RECORD_HEADER..].copy_from_slice(page);

        // Compute CRC32C over entire record with crc field zeroed (it already is).
        let crc = crc32c::crc32c(&record);
        record[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        guard.file.seek(SeekFrom::End(0))?;
        guard.file.write_all(&record)?;
        guard.insert_lsn = lsn;

        Ok(lsn)
    }

    /// Ensure all records up to the current insert LSN are durable on disk
    /// (fdatasync). Returns the flushed LSN. A no-op if already up to date.
    pub fn flush(&self) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();
        if guard.flushed_lsn < guard.insert_lsn {
            guard.file.sync_data()?;
            guard.flushed_lsn = guard.insert_lsn;
        }
        Ok(guard.flushed_lsn)
    }

    /// The LSN of the last inserted (but not necessarily flushed) record.
    pub fn insert_lsn(&self) -> Lsn {
        self.inner.lock().unwrap().insert_lsn
    }

    /// The LSN up to which records are durably on disk.
    pub fn flushed_lsn(&self) -> Lsn {
        self.inner.lock().unwrap().flushed_lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::{BufferTag, PAGE_SIZE};
    use crate::storage::smgr::{ForkNumber, RelFileLocator};
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
            rel: RelFileLocator { spc_oid: 0, db_oid: 1, rel_number: 42 },
            fork: ForkNumber::Main,
            block,
        }
    }

    /// Each `write_record` call advances the LSN by exactly `WAL_RECORD_LEN`.
    #[test]
    fn lsn_advances_by_record_len_per_write() {
        let dir = test_dir("lsn_advances");
        let wal = WalWriter::new(&dir).unwrap();

        assert_eq!(wal.insert_lsn(), INVALID_LSN);

        let page = [0xAAu8; PAGE_SIZE];
        let lsn1 = wal.write_record(1, test_tag(0), &page).unwrap();
        assert_eq!(lsn1, WAL_RECORD_LEN as Lsn);

        let lsn2 = wal.write_record(1, test_tag(1), &page).unwrap();
        assert_eq!(lsn2, 2 * WAL_RECORD_LEN as Lsn);

        assert_eq!(wal.insert_lsn(), lsn2);
    }

    /// The WAL file size equals `insert_lsn` after writes.
    #[test]
    fn file_size_matches_insert_lsn() {
        let dir = test_dir("file_size");
        let wal = WalWriter::new(&dir).unwrap();
        let page = [0u8; PAGE_SIZE];

        wal.write_record(1, test_tag(0), &page).unwrap();
        wal.write_record(2, test_tag(1), &page).unwrap();

        let file_len = fs::metadata(dir.join("wal.log")).unwrap().len();
        assert_eq!(file_len, wal.insert_lsn());
    }

    /// `flush()` advances `flushed_lsn` to `insert_lsn`.
    #[test]
    fn flush_advances_flushed_lsn() {
        let dir = test_dir("flush_lsn");
        let wal = WalWriter::new(&dir).unwrap();
        let page = [0u8; PAGE_SIZE];

        let lsn = wal.write_record(1, test_tag(0), &page).unwrap();
        // Before flush: flushed_lsn is still 0 (no fsync yet).
        assert_eq!(wal.flushed_lsn(), INVALID_LSN);

        let returned = wal.flush().unwrap();
        assert_eq!(returned, lsn);
        assert_eq!(wal.flushed_lsn(), lsn);
    }

    /// Calling `flush()` twice is a no-op on the second call.
    #[test]
    fn flush_is_idempotent() {
        let dir = test_dir("flush_idempotent");
        let wal = WalWriter::new(&dir).unwrap();
        let page = [0u8; PAGE_SIZE];

        wal.write_record(1, test_tag(0), &page).unwrap();
        let lsn = wal.flush().unwrap();
        let lsn2 = wal.flush().unwrap();
        assert_eq!(lsn, lsn2);
    }

    /// The binary layout of a written record matches the documented format.
    #[test]
    fn record_binary_layout_is_correct() {
        let dir = test_dir("binary_layout");
        let wal = WalWriter::new(&dir).unwrap();

        let xid: u32 = 0x1234_5678;
        let tag = BufferTag {
            rel: RelFileLocator { spc_oid: 0xAA, db_oid: 0xBB, rel_number: 0xCC },
            fork: ForkNumber::Main,
            block: 0xDD,
        };
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0x42;
        page[PAGE_SIZE - 1] = 0x99;

        let lsn = wal.write_record(xid, tag, &page).unwrap();

        let mut raw = Vec::new();
        fs::File::open(dir.join("wal.log"))
            .unwrap()
            .read_to_end(&mut raw)
            .unwrap();

        assert_eq!(raw.len(), WAL_RECORD_LEN);

        // XLogRecord header
        // xl_tot_len at bytes 0-3
        assert_eq!(u32::from_le_bytes(raw[0..4].try_into().unwrap()), WAL_RECORD_LEN as u32);
        // xl_xid at bytes 4-7
        assert_eq!(u32::from_le_bytes(raw[4..8].try_into().unwrap()), xid);
        // xl_prev at bytes 8-15 (previous LSN = 0 for first record)
        assert_eq!(u64::from_le_bytes(raw[8..16].try_into().unwrap()), 0);
        // xl_info at byte 16
        assert_eq!(raw[16], 0);
        // xl_rmid at byte 17
        assert_eq!(raw[17], 0);
        // xl_crc at bytes 20-23 (non-zero)
        let crc = u32::from_le_bytes(raw[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
        assert_ne!(crc, 0, "CRC should be computed");
        // Verify CRC: zero out the crc field and recompute
        let mut check = raw.clone();
        check[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&[0, 0, 0, 0]);
        assert_eq!(crc32c::crc32c(&check), crc, "CRC mismatch");

        // Block header
        // spc_oid at bytes 24-27
        assert_eq!(u32::from_le_bytes(raw[24..28].try_into().unwrap()), 0xAA);
        // db_oid at bytes 28-31
        assert_eq!(u32::from_le_bytes(raw[28..32].try_into().unwrap()), 0xBB);
        // rel_number at bytes 32-35
        assert_eq!(u32::from_le_bytes(raw[32..36].try_into().unwrap()), 0xCC);
        // fork at byte 36
        assert_eq!(raw[36], ForkNumber::Main.as_u8());
        // block at bytes 40-43
        assert_eq!(u32::from_le_bytes(raw[40..44].try_into().unwrap()), 0xDD);

        // page data starts at byte 44
        assert_eq!(raw[WAL_RECORD_HEADER], 0x42);
        assert_eq!(raw[WAL_RECORD_LEN - 1], 0x99);

        // LSN should equal the record length (first record starts at offset 0)
        assert_eq!(lsn, WAL_RECORD_LEN as u64);
    }

    /// A freshly-opened `WalWriter` on an existing file resumes from the
    /// current file size (new records are appended, not overwritten).
    #[test]
    fn reopen_resumes_at_file_end() {
        let dir = test_dir("reopen");
        let page = [0x11u8; PAGE_SIZE];

        let lsn1 = {
            let wal = WalWriter::new(&dir).unwrap();
            wal.write_record(1, test_tag(0), &page).unwrap()
        };

        // Reopen — insert_lsn should pick up from where we left off.
        let wal2 = WalWriter::new(&dir).unwrap();
        assert_eq!(wal2.insert_lsn(), lsn1);

        let lsn2 = wal2.write_record(2, test_tag(1), &page).unwrap();
        assert_eq!(lsn2, 2 * WAL_RECORD_LEN as Lsn);

        let file_len = fs::metadata(dir.join("wal.log")).unwrap().len();
        assert_eq!(file_len, lsn2);
    }
}
