//! M3.5 spill substrate (design authority: docs/design/m3.5-spill.md §2).
//!
//! One [`SpillSet`] per spill-eligible engagement, owned by the engagement
//! payload: a query-owned fd `FileSet` whose Drop deletes every file on
//! every exit path (success, refusal, cancel, error). Inside it, per-worker
//! SINGLE-WRITER spill files with a partition directory:
//!
//! - A [`SpillFile`] is plain data between flush events (`Send` — it rides
//!   a sink `Local` and moves through SEAL). It holds NO open handle: file
//!   handles are VFD-thread-local (fd/src/vfd.rs — the design doc's §6.2
//!   hazard), so every I/O burst opens, works, and closes within one event
//!   on one thread.
//! - Writers append whole EPOCHS (one budget-crossing flush = one epoch):
//!   partition-contiguous segments recorded as per-partition extents. The
//!   file is complete on disk at the end of every epoch (BufFile close
//!   flushes); an epoch abandoned mid-write is simply never committed to
//!   the directory, and the next epoch reopens at the committed length,
//!   overwriting the tail.
//! - Readers (combine tasks, other threads) open the frozen file BY NAME
//!   via `BufFileOpenFileSet` on their own thread's VFD cache and stream
//!   one partition's extents. Frozen-before-read is the caller's deps-DAG
//!   obligation (a reader task set depends on the writer task set).
//! - Every syscall-bearing region brackets
//!   [`runtime::blocking_io_section`] (design §6.1): armed permit donation
//!   on pool workers, no-op on pinned-regime helpers and the leader.
//!
//! Record CONTENTS are operator-owned byte contracts (agg (key, state)
//! rows, DistinctSet value records, STS batch tuples, sort runs); this
//! crate moves bytes and never interprets them.

use std::sync::Arc;

use ::fd::buffile::{BufFile, BufFileCreateFileSet, BufFileOpenFileSet, SEEK_SET};
use ::fd::fileset::FileSet;
use ::mcx::Mcx;
use ::types_error::PgResult;

const MAX_PHYSICAL_FILESIZE: i64 = 0x4000_0000; // buffile.rs segment size

/// Query-owned set of spill files for ONE engagement. Arc'd into the
/// engagement payload; Drop (via the inner `FileSet`) deletes everything.
pub struct SpillSet {
    fileset: FileSet,
}

impl SpillSet {
    /// Create the engagement's spill set. Caller must be a thread with
    /// temp-file access up (leader at admission time, or a bound helper).
    pub fn create() -> PgResult<Arc<SpillSet>> {
        Ok(Arc::new(SpillSet { fileset: FileSet::init()? }))
    }

    /// Canonical spill-file name: unique per (purpose, generation, worker)
    /// within this set. Purpose is operator-chosen ("agg", "dst", "run",
    /// join batches use STS with its own naming).
    pub fn file_name(purpose: &str, generation: u64, worker: usize) -> String {
        format!("m35-{purpose}-g{generation}-w{worker}")
    }

    fn fileset(&self) -> &FileSet {
        &self.fileset
    }
}

/// One partition extent: `len` bytes at logical BufFile offset `offset`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Extent {
    pub offset: u64,
    pub len: u64,
}

/// One committed epoch: for each partition, at most one extent (an epoch's
/// partitions are written contiguously and ascending).
struct EpochDir {
    /// (partition, extent), partition strictly ascending; empty partitions
    /// are absent.
    parts: Box<[(u32, Extent)]>,
}

/// A single-writer spill file with a partition directory. Plain data
/// between flush events; owned by exactly one sink `Local` (single-toucher
/// by the sink contract) and immutable ("frozen") once its owner stops
/// writing — the caller's deps DAG separates the last write from the first
/// read.
pub struct SpillFile {
    set: Arc<SpillSet>,
    name: String,
    nparts: u32,
    /// Committed logical length (== next epoch's start offset).
    len: u64,
    created: bool,
    epochs: Vec<EpochDir>,
}

impl SpillFile {
    /// Lazy: no file exists until the first epoch is written.
    pub fn new(set: Arc<SpillSet>, name: String, nparts: u32) -> SpillFile {
        assert!(nparts > 0);
        SpillFile { set, name, nparts, len: 0, created: false, epochs: Vec::new() }
    }

    pub fn nparts(&self) -> u32 {
        self.nparts
    }

    /// Committed bytes on disk (spill accounting / gate-record counters).
    pub fn spilled_bytes(&self) -> u64 {
        self.len
    }

    pub fn epochs(&self) -> usize {
        self.epochs.len()
    }

    /// Total committed bytes of one partition across all epochs.
    pub fn part_len(&self, part: u32) -> u64 {
        debug_assert!(part < self.nparts);
        self.epochs
            .iter()
            .flat_map(|e| e.parts.iter())
            .filter(|(p, _)| *p == part)
            .map(|(_, x)| x.len)
            .sum()
    }

    /// One partition's committed extents, epoch order (M3.5 join batches:
    /// each extent is one morsel-claim unit — extents are RECORD-aligned by
    /// construction, every epoch writes whole records).
    pub fn part_extents(&self, part: u32) -> Vec<Extent> {
        debug_assert!(part < self.nparts);
        self.epochs
            .iter()
            .flat_map(|e| e.parts.iter())
            .filter(|(p, _)| *p == part)
            .map(|(_, x)| *x)
            .collect()
    }

    /// Stream ONE extent (from [`SpillFile::part_extents`]) of the frozen
    /// file. Any thread with temp-file access; same deps-DAG obligation as
    /// [`SpillFile::read_part`].
    pub fn read_extent<'mcx>(&self, mcx: Mcx<'mcx>, extent: Extent) -> PgResult<PartReader<'mcx>> {
        debug_assert!(self.created, "read_extent on a never-written file");
        let _io = runtime::blocking_io_section();
        let bf = BufFileOpenFileSet(mcx, self.set.fileset(), &self.name, true)?;
        drop(_io);
        Ok(PartReader { bf, extents: vec![extent], cur: 0, pos_in_cur: 0, seeked: false })
    }

    /// Begin one epoch write. MUST be called by the file's owning worker
    /// thread (single-writer law). The returned writer holds an open
    /// BufFile; every syscall-bearing call (open here, `write_part`,
    /// `finish`) enters its own declared blocking section. The epoch
    /// commits only at [`EpochWriter::finish`].
    pub fn begin_epoch<'a, 'mcx>(&'a mut self, mcx: Mcx<'mcx>) -> PgResult<EpochWriter<'a, 'mcx>> {
        let _io = runtime::blocking_io_section();
        let bf = if self.created {
            let mut bf = BufFileOpenFileSet(mcx, self.set.fileset(), &self.name, false)?;
            // Seek to the committed logical length (segmented offsets).
            let fileno = (self.len as i64) / MAX_PHYSICAL_FILESIZE;
            let off = (self.len as i64) % MAX_PHYSICAL_FILESIZE;
            let r = bf.seek(fileno as i32, off, SEEK_SET)?;
            debug_assert_eq!(r, 0, "seek to committed EOF cannot fail");
            bf
        } else {
            BufFileCreateFileSet(mcx, self.set.fileset(), &self.name)?
        };
        self.created = true;
        // Drop the section while the writer stages in-memory state; each
        // write_part / finish enters its own.
        drop(_io);
        Ok(EpochWriter { file: self, bf, cursor_valid: true, staged: Vec::new(), written: 0 })
    }

    /// Stream one partition's committed bytes. Any thread with temp-file
    /// access; the file must be frozen (caller's deps-DAG obligation).
    /// Returns None if the partition has no bytes (or the file was never
    /// created).
    pub fn read_part<'mcx>(&self, mcx: Mcx<'mcx>, part: u32) -> PgResult<Option<PartReader<'mcx>>> {
        debug_assert!(part < self.nparts);
        let extents: Vec<Extent> = self
            .epochs
            .iter()
            .flat_map(|e| e.parts.iter())
            .filter(|(p, _)| *p == part)
            .map(|(_, x)| *x)
            .collect();
        if extents.is_empty() || !self.created {
            return Ok(None);
        }
        let _io = runtime::blocking_io_section();
        let bf = BufFileOpenFileSet(mcx, self.set.fileset(), &self.name, true)?;
        drop(_io);
        Ok(Some(PartReader { bf, extents, cur: 0, pos_in_cur: 0, seeked: false }))
    }
}

/// In-flight epoch write: partitions strictly ascending, one contiguous
/// extent per partition. Byte layout is the caller's contract; this type
/// records where each partition's bytes landed.
pub struct EpochWriter<'a, 'mcx> {
    file: &'a mut SpillFile,
    bf: BufFile<'mcx>,
    cursor_valid: bool,
    staged: Vec<(u32, Extent)>,
    written: u64,
}

impl EpochWriter<'_, '_> {
    /// Logical file offset the next byte will land at (fence indexes for
    /// sort runs read this).
    pub fn offset(&self) -> u64 {
        self.file.len + self.written
    }

    /// Append bytes to `part`. Parts must be written in ascending order;
    /// consecutive calls for the SAME part extend its extent.
    pub fn write_part(&mut self, part: u32, bytes: &[u8]) -> PgResult<()> {
        debug_assert!(part < self.file.nparts);
        debug_assert!(self.cursor_valid);
        if bytes.is_empty() {
            return Ok(());
        }
        let at = self.file.len + self.written;
        match self.staged.last_mut() {
            Some((p, x)) if *p == part => {
                debug_assert_eq!(x.offset + x.len, at, "extents are contiguous");
                x.len += bytes.len() as u64;
            }
            last => {
                assert!(
                    last.map_or(true, |(p, _)| *p < part),
                    "partitions must be written in ascending order"
                );
                self.staged.push((part, Extent { offset: at, len: bytes.len() as u64 }));
            }
        }
        let _io = runtime::blocking_io_section();
        self.bf.write(bytes)?;
        self.written += bytes.len() as u64;
        Ok(())
    }

    /// Flush + close + commit the epoch into the directory. Not calling
    /// this (drop/unwind) abandons the epoch: nothing is committed and the
    /// next epoch overwrites the tail.
    pub fn finish(self) -> PgResult<()> {
        let EpochWriter { file, bf, staged, written, .. } = self;
        {
            let _io = runtime::blocking_io_section();
            bf.close()?;
        }
        if written > 0 {
            file.epochs.push(EpochDir { parts: staged.into_boxed_slice() });
            file.len += written;
        }
        Ok(())
    }
}

/// Streaming reader of one partition (its extents across all epochs, in
/// epoch order). Owns its thread-local BufFile handle; close it on the
/// reading thread. Callers pass large buffers (each `read` call is one
/// declared blocking section).
pub struct PartReader<'mcx> {
    bf: BufFile<'mcx>,
    extents: Vec<Extent>,
    cur: usize,
    pos_in_cur: u64,
    seeked: bool,
}

impl PartReader<'_> {
    pub fn total_len(&self) -> u64 {
        self.extents.iter().map(|x| x.len).sum()
    }

    /// Read up to `buf.len()` bytes; 0 = end of partition.
    pub fn read(&mut self, buf: &mut [u8]) -> PgResult<usize> {
        let mut filled = 0usize;
        let _io = runtime::blocking_io_section();
        while filled < buf.len() {
            let Some(x) = self.extents.get(self.cur) else { break };
            if self.pos_in_cur == x.len {
                self.cur += 1;
                self.pos_in_cur = 0;
                self.seeked = false;
                continue;
            }
            if !self.seeked {
                let at = (x.offset + self.pos_in_cur) as i64;
                let r = self.bf.seek(
                    (at / MAX_PHYSICAL_FILESIZE) as i32,
                    at % MAX_PHYSICAL_FILESIZE,
                    SEEK_SET,
                )?;
                debug_assert_eq!(r, 0, "extent lies within the committed file");
                self.seeked = true;
            }
            let want = ((x.len - self.pos_in_cur) as usize).min(buf.len() - filled);
            self.bf.read_exact(&mut buf[filled..filled + want])?;
            filled += want;
            self.pos_in_cur += want as u64;
        }
        Ok(filled)
    }

    /// Read the whole remaining partition (tests / small partitions).
    pub fn read_to_end(&mut self) -> PgResult<Vec<u8>> {
        let mut out = Vec::new();
        let mut chunk = vec![0u8; 256 * 1024];
        loop {
            let n = self.read(&mut chunk)?;
            if n == 0 {
                return Ok(out);
            }
            out.extend_from_slice(&chunk[..n]);
        }
    }

    pub fn close(self) -> PgResult<()> {
        let _io = runtime::blocking_io_section();
        self.bf.close()
    }
}

#[cfg(test)]
mod tests;
