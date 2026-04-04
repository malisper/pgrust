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
//!  bytes  0-7  : lsn        (u64 LE) — byte offset after this record
//!  bytes  8-11 : xid        (u32 LE) — transaction id
//!  bytes 12-15 : spc_oid    (u32 LE)
//!  bytes 16-19 : db_oid     (u32 LE)
//!  bytes 20-23 : rel_number (u32 LE)
//!  byte  24    : fork       (u8)
//!  bytes 25-27 : _pad       ([u8; 3])
//!  bytes 28-31 : block      (u32 LE)
//!  bytes 32..  : page data  (PAGE_SIZE = 8192 bytes)
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

const WAL_RECORD_HEADER: usize = 32;
pub const WAL_RECORD_LEN: usize = WAL_RECORD_HEADER + PAGE_SIZE;

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

        let lsn = guard.insert_lsn + WAL_RECORD_LEN as Lsn;

        let mut record = [0u8; WAL_RECORD_LEN];
        record[0..8].copy_from_slice(&lsn.to_le_bytes());
        record[8..12].copy_from_slice(&xid.to_le_bytes());
        record[12..16].copy_from_slice(&tag.rel.spc_oid.to_le_bytes());
        record[16..20].copy_from_slice(&tag.rel.db_oid.to_le_bytes());
        record[20..24].copy_from_slice(&tag.rel.rel_number.to_le_bytes());
        record[24] = tag.fork.as_u8();
        // bytes 25-27: padding (already zero)
        record[28..32].copy_from_slice(&tag.block.to_le_bytes());
        record[WAL_RECORD_HEADER..].copy_from_slice(page);

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

        // lsn at bytes 0-7
        assert_eq!(u64::from_le_bytes(raw[0..8].try_into().unwrap()), lsn);
        // xid at bytes 8-11
        assert_eq!(u32::from_le_bytes(raw[8..12].try_into().unwrap()), xid);
        // spc_oid at bytes 12-15
        assert_eq!(u32::from_le_bytes(raw[12..16].try_into().unwrap()), 0xAA);
        // db_oid at bytes 16-19
        assert_eq!(u32::from_le_bytes(raw[16..20].try_into().unwrap()), 0xBB);
        // rel_number at bytes 20-23
        assert_eq!(u32::from_le_bytes(raw[20..24].try_into().unwrap()), 0xCC);
        // fork at byte 24
        assert_eq!(raw[24], ForkNumber::Main.as_u8());
        // block at bytes 28-31
        assert_eq!(u32::from_le_bytes(raw[28..32].try_into().unwrap()), 0xDD);
        // page data starts at byte 32
        assert_eq!(raw[WAL_RECORD_HEADER], 0x42);
        assert_eq!(raw[WAL_RECORD_LEN - 1], 0x99);
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
