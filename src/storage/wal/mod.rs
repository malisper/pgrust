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
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// WAL buffer size — accumulate this many bytes before flushing to kernel.
/// Matches ~8 full-page WAL records.
const WAL_BUF_SIZE: usize = 64 * 1024;

use std::collections::HashSet;

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
/// Maximum FPI record size (no hole compression). Used by tests.
pub const WAL_RECORD_LEN: usize = WAL_RECORD_HEADER + FPI_HOLE_META + PAGE_SIZE;
/// Hole metadata: hole_offset(2) + hole_length(2)
const FPI_HOLE_META: usize = 4;
/// Offset of the CRC field within the record.
const CRC_OFFSET: usize = 20;

/// xl_info values
const XLOG_FPI: u8 = 0;           // Full page image
const XLOG_HEAP_INSERT: u8 = 1;   // Row-level insert delta
const XLOG_XACT_COMMIT: u8 = 0;   // Transaction commit

/// xl_rmid values (resource manager IDs)
const RM_HEAP_ID: u8 = 0;
const RM_XACT_ID: u8 = 1;

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
    file: BufWriter<File>,
    /// Byte offset immediately after the last inserted record.
    /// Zero if no records have been written yet.
    insert_lsn: Lsn,
    /// Byte offset up to which BufWriter has been flushed to the kernel
    /// (but not necessarily fsynced). Updated by the background writer.
    written_lsn: Lsn,
    /// Byte offset up to which the WAL file has been fsynced.
    flushed_lsn: Lsn,
    /// Pages that have had a full page image written since WAL open.
    /// Subsequent writes to these pages use row-level deltas instead.
    pages_with_image: HashSet<BufferTag>,
}

/// Threshold of unbuffered WAL data that triggers an early wake of the
/// background writer (matches PG's walwriter behavior).
const BG_FLUSH_THRESHOLD: u64 = 1024 * 1024; // 1 MB

pub struct WalWriter {
    inner: Mutex<WalWriterInner>,
    /// Signalled when insert_lsn - written_lsn >= BG_FLUSH_THRESHOLD.
    bg_wake: std::sync::Condvar,
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Flush the BufWriter so all buffered records reach the file.
        let mut guard = self.inner.lock().unwrap();
        let _ = guard.file.flush();
        let _ = guard.file.get_ref().sync_all();
    }
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
        let mut file = file;
        let size = file.metadata()?.len();
        // Seek to end once so subsequent writes append without per-write seeks.
        file.seek(SeekFrom::End(0))?;
        Ok(WalWriter {
            inner: Mutex::new(WalWriterInner {
                file: BufWriter::with_capacity(WAL_BUF_SIZE, file),
                insert_lsn: size,
                written_lsn: size,
                flushed_lsn: size,
                pages_with_image: HashSet::new(),
            }),
            bg_wake: std::sync::Condvar::new(),
        })
    }

    /// Append a full-page-image WAL record for `tag` modified by transaction
    /// `xid`. Returns the LSN assigned to this record (byte offset after it).
    /// Also marks the page as having a backup, so future writes can use deltas.
    pub fn write_record(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();
        let lsn = Self::write_fpi(&mut guard, xid, tag, page)?;
        guard.pages_with_image.insert(tag);
        self.maybe_wake_bg(&guard);
        Ok(lsn)
    }

    /// Write a WAL record for an insert. If the page already has a full image
    /// in the WAL, writes only the tuple delta (~100 bytes). Otherwise writes
    /// the full page image first.
    ///
    /// `tuple_data` is the serialized tuple bytes. `offset_number` is the
    /// line pointer offset where the tuple was placed on the page.
    pub fn write_insert(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
        offset_number: u16,
        tuple_data: &[u8],
    ) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();

        if !guard.pages_with_image.contains(&tag) {
            // First write to this page — write full page image.
            let lsn = Self::write_fpi(&mut guard, xid, tag, page)?;
            guard.pages_with_image.insert(tag);
            self.maybe_wake_bg(&guard);
            return Ok(lsn);
        }

        // Page already has a backup — write just the insert delta.
        let data_len = tuple_data.len();
        let record_len = WAL_RECORD_HEADER + 4 + data_len; // header + offset(2) + len(2) + data

        let mut record = vec![0u8; record_len];

        let prev_lsn = guard.insert_lsn;
        let lsn = prev_lsn + record_len as Lsn;

        // XLogRecord header
        record[0..4].copy_from_slice(&(record_len as u32).to_le_bytes());
        record[4..8].copy_from_slice(&xid.to_le_bytes());
        record[8..16].copy_from_slice(&prev_lsn.to_le_bytes());
        record[16] = XLOG_HEAP_INSERT;
        record[17] = RM_HEAP_ID;

        // Block header
        record[24..28].copy_from_slice(&tag.rel.spc_oid.to_le_bytes());
        record[28..32].copy_from_slice(&tag.rel.db_oid.to_le_bytes());
        record[32..36].copy_from_slice(&tag.rel.rel_number.to_le_bytes());
        record[36] = tag.fork.as_u8();
        record[40..44].copy_from_slice(&tag.block.to_le_bytes());

        // Insert data: offset_number(2) + tuple_len(2) + tuple_data
        record[WAL_RECORD_HEADER..WAL_RECORD_HEADER + 2]
            .copy_from_slice(&offset_number.to_le_bytes());
        record[WAL_RECORD_HEADER + 2..WAL_RECORD_HEADER + 4]
            .copy_from_slice(&(data_len as u16).to_le_bytes());
        record[WAL_RECORD_HEADER + 4..].copy_from_slice(tuple_data);

        // CRC
        let crc = crc32c::crc32c(&record);
        record[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        guard.file.write_all(&record)?;
        guard.insert_lsn = lsn;
        self.maybe_wake_bg(&guard);

        Ok(lsn)
    }

    /// Write a commit record to WAL for the given transaction. This must be
    /// called before flushing WAL at commit time so that recovery can replay
    /// the commit and update the CLOG.
    ///
    /// The commit record is a minimal XLogRecord header (24 bytes) with no
    /// block reference — just xl_rmid=RM_XACT_ID and xl_info=XLOG_XACT_COMMIT.
    pub fn write_commit(&self, xid: u32) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();

        let record_len = XLOG_RECORD_HEADER; // 24 bytes, no block data
        let prev_lsn = guard.insert_lsn;
        let lsn = prev_lsn + record_len as Lsn;

        let mut record = [0u8; XLOG_RECORD_HEADER];

        record[0..4].copy_from_slice(&(record_len as u32).to_le_bytes());
        record[4..8].copy_from_slice(&xid.to_le_bytes());
        record[8..16].copy_from_slice(&prev_lsn.to_le_bytes());
        record[16] = XLOG_XACT_COMMIT;
        record[17] = RM_XACT_ID;

        // CRC
        let crc = crc32c::crc32c(&record);
        record[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        guard.file.write_all(&record)?;
        guard.insert_lsn = lsn;

        Ok(lsn)
    }

    /// Internal: write a full page image record with hole compression.
    ///
    /// The "hole" is the range of zero bytes between `pd_lower` and `pd_upper`
    /// in the page header. Omitting it can shrink an FPI from 8KB to a few
    /// hundred bytes for a nearly-empty page.
    fn write_fpi(
        guard: &mut std::sync::MutexGuard<'_, WalWriterInner>,
        xid: u32,
        tag: BufferTag,
        page: &[u8; PAGE_SIZE],
    ) -> Result<Lsn, WalError> {
        // Compute hole from page header: pd_lower at bytes 12-13, pd_upper at 14-15.
        let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
        let pd_upper = u16::from_le_bytes([page[14], page[15]]) as usize;

        let (hole_offset, hole_length) = if pd_upper > pd_lower && pd_upper <= PAGE_SIZE && pd_lower > 0 {
            (pd_lower as u16, (pd_upper - pd_lower) as u16)
        } else {
            (0u16, 0u16)
        };

        let page_data_len = PAGE_SIZE - hole_length as usize;
        let record_len = WAL_RECORD_HEADER + FPI_HOLE_META + page_data_len;

        let prev_lsn = guard.insert_lsn;
        let lsn = prev_lsn + record_len as Lsn;

        let mut record = vec![0u8; record_len];

        // XLogRecord header
        record[0..4].copy_from_slice(&(record_len as u32).to_le_bytes());
        record[4..8].copy_from_slice(&xid.to_le_bytes());
        record[8..16].copy_from_slice(&prev_lsn.to_le_bytes());
        record[16] = XLOG_FPI;
        record[17] = RM_HEAP_ID;

        // Block header
        record[24..28].copy_from_slice(&tag.rel.spc_oid.to_le_bytes());
        record[28..32].copy_from_slice(&tag.rel.db_oid.to_le_bytes());
        record[32..36].copy_from_slice(&tag.rel.rel_number.to_le_bytes());
        record[36] = tag.fork.as_u8();
        record[40..44].copy_from_slice(&tag.block.to_le_bytes());

        // Hole metadata
        let hole_start = WAL_RECORD_HEADER;
        record[hole_start..hole_start + 2].copy_from_slice(&hole_offset.to_le_bytes());
        record[hole_start + 2..hole_start + 4].copy_from_slice(&hole_length.to_le_bytes());

        // Page data (before hole + after hole)
        let data_start = WAL_RECORD_HEADER + FPI_HOLE_META;
        if hole_length > 0 {
            let ho = hole_offset as usize;
            let hl = hole_length as usize;
            record[data_start..data_start + ho].copy_from_slice(&page[..ho]);
            record[data_start + ho..].copy_from_slice(&page[ho + hl..]);
        } else {
            record[data_start..].copy_from_slice(page);
        }

        // CRC
        let crc = crc32c::crc32c(&record);
        record[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        guard.file.write_all(&record)?;
        guard.insert_lsn = lsn;

        Ok(lsn)
    }

    /// Ensure all records up to the current insert LSN are durable on disk
    /// (fdatasync). Returns the flushed LSN. A no-op if already up to date.
    pub fn flush(&self) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();
        Self::flush_inner(&mut guard)
    }

    /// Ensure WAL is durable up to at least `target_lsn`. If the WAL has
    /// already been flushed past that point, this is a no-op. Returns the
    /// flushed LSN.
    pub fn flush_to(&self, target_lsn: Lsn) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();
        if guard.flushed_lsn >= target_lsn {
            return Ok(guard.flushed_lsn);
        }
        Self::flush_inner(&mut guard)
    }

    fn flush_inner(
        guard: &mut std::sync::MutexGuard<'_, WalWriterInner>,
    ) -> Result<Lsn, WalError> {
        if guard.flushed_lsn < guard.insert_lsn {
            // Only flush BufWriter if the background writer hasn't already
            // pushed everything to the kernel.
            if guard.written_lsn < guard.insert_lsn {
                guard.file.flush()?;
                guard.written_lsn = guard.insert_lsn;
            }
            guard.file.get_ref().sync_data()?;
            guard.flushed_lsn = guard.insert_lsn;
        }
        Ok(guard.flushed_lsn)
    }

    /// Flush the BufWriter to the kernel (no fsync). Called by the background
    /// writer thread. This is cheap — just a write() syscall if there's
    /// buffered data.
    pub fn bg_flush(&self) -> Result<Lsn, WalError> {
        let mut guard = self.inner.lock().unwrap();
        if guard.written_lsn < guard.insert_lsn {
            guard.file.flush()?;
            guard.written_lsn = guard.insert_lsn;
        }
        Ok(guard.written_lsn)
    }

    /// Sleep until either `interval` elapses or enough WAL has accumulated.
    /// Called by the background writer between flushes.
    pub fn bg_wait(&self, interval: Duration) {
        let guard = self.inner.lock().unwrap();
        if guard.insert_lsn - guard.written_lsn >= BG_FLUSH_THRESHOLD {
            return; // Already above threshold, flush immediately.
        }
        let _ = self.bg_wake.wait_timeout(guard, interval);
    }

    /// Notify the background writer if enough WAL has accumulated.
    fn maybe_wake_bg(&self, guard: &std::sync::MutexGuard<'_, WalWriterInner>) {
        if guard.insert_lsn - guard.written_lsn >= BG_FLUSH_THRESHOLD {
            self.bg_wake.notify_one();
        }
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

/// Background WAL writer that periodically flushes the BufWriter to the
/// kernel. This means that at commit time, only an `fdatasync` is needed
/// (the data is already in kernel buffers).
pub struct WalBgWriter {
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl WalBgWriter {
    /// Start a background writer that flushes the WAL BufWriter every `interval`
    /// or when 1MB of WAL has accumulated, whichever comes first.
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
        WalBgWriter {
            shutdown,
            handle: Some(handle),
        }
    }
}

impl Drop for WalBgWriter {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
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
        wal.flush().unwrap(); // flush BufWriter so file size reflects writes

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
        wal.flush().unwrap(); // flush BufWriter so file contents are readable

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

        // Hole metadata at bytes 44-47 (no hole since page header is invalid)
        assert_eq!(u16::from_le_bytes(raw[44..46].try_into().unwrap()), 0, "hole_offset");
        assert_eq!(u16::from_le_bytes(raw[46..48].try_into().unwrap()), 0, "hole_length");

        // page data starts at byte 48 (after header + hole meta)
        let data_start = WAL_RECORD_HEADER + FPI_HOLE_META;
        assert_eq!(raw[data_start], 0x42);
        assert_eq!(raw[raw.len() - 1], 0x99);

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
            let lsn = wal.write_record(1, test_tag(0), &page).unwrap();
            wal.flush().unwrap(); // ensure data is on disk before reopen
            lsn
        };

        // Reopen — insert_lsn should pick up from where we left off.
        let wal2 = WalWriter::new(&dir).unwrap();
        assert_eq!(wal2.insert_lsn(), lsn1);

        let lsn2 = wal2.write_record(2, test_tag(1), &page).unwrap();
        assert_eq!(lsn2, 2 * WAL_RECORD_LEN as Lsn);

        wal2.flush().unwrap(); // flush BufWriter before checking file size
        let file_len = fs::metadata(dir.join("wal.log")).unwrap().len();
        assert_eq!(file_len, lsn2);
    }

    /// FPI hole compression omits zeros between pd_lower and pd_upper.
    #[test]
    fn fpi_hole_compression_reduces_record_size() {
        use crate::storage::page;

        let dir = test_dir("hole_compress");
        let wal = WalWriter::new(&dir).unwrap();

        // Create a properly initialized page with a small tuple.
        let mut page_buf = [0u8; PAGE_SIZE];
        page::page_init(&mut page_buf, 0);

        // Add a small item so pd_lower advances and pd_upper retreats.
        let item = [0xABu8; 32];
        page::page_add_item(&mut page_buf, &item).unwrap();

        let header = page::page_header(&page_buf).unwrap();
        let pd_lower = header.pd_lower as usize;
        let pd_upper = header.pd_upper as usize;
        let hole_len = pd_upper - pd_lower;
        assert!(hole_len > 0, "page should have a hole");

        let lsn = wal.write_record(1, test_tag(0), &page_buf).unwrap();
        wal.flush().unwrap();

        // Record size should be header + hole_meta + (PAGE_SIZE - hole)
        let expected_size = WAL_RECORD_HEADER + FPI_HOLE_META + PAGE_SIZE - hole_len;
        assert_eq!(lsn, expected_size as u64);

        let file_len = fs::metadata(dir.join("wal.log")).unwrap().len();
        assert_eq!(file_len, expected_size as u64);

        // Verify the record is much smaller than an uncompressed FPI.
        assert!(expected_size < WAL_RECORD_LEN,
            "compressed FPI ({expected_size}) should be smaller than max ({WAL_RECORD_LEN})");

        // Read back and verify hole metadata.
        let mut raw = Vec::new();
        fs::File::open(dir.join("wal.log")).unwrap().read_to_end(&mut raw).unwrap();
        let hole_offset = u16::from_le_bytes(raw[44..46].try_into().unwrap());
        let hole_length = u16::from_le_bytes(raw[46..48].try_into().unwrap());
        assert_eq!(hole_offset as usize, pd_lower);
        assert_eq!(hole_length as usize, hole_len);

        // Verify CRC
        let crc = u32::from_le_bytes(raw[CRC_OFFSET..CRC_OFFSET + 4].try_into().unwrap());
        let mut check = raw.clone();
        check[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&[0, 0, 0, 0]);
        assert_eq!(crc32c::crc32c(&check), crc, "CRC mismatch");
    }
}
