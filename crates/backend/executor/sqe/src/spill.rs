//! Grouped-agg spill seam (memory governance): the byte-level substrate
//! the partition-owned grouped arms spill through when their accounted
//! bytes cross the E18 budget (spill-design.md).
//!
//! The engine stays free of any file-layer dependency: the HOST registers
//! a store factory (the server registers the fd/pgsql_tmp-backed set at
//! the same seam that arms pool workers for file access; the rig registers
//! the plain std temp store). No factory registered = spill unavailable —
//! the planner's budget law then refuses typed instead of engaging an arm
//! that would stumble mid-I/O (fail-closed, the pool arming contract).
//!
//! Media are PLAIN DATA between events: one `append` call is one write
//! event (open/write/close inside the call, on the calling thread); one
//! `read_at` call is one read event over committed bytes. Nothing here
//! holds an open handle across events, so workers and owners exchange
//! only `Send + Sync` objects.

use std::sync::{Arc, OnceLock};

/// One spill file: append-only committed chunks, positioned reads.
pub trait SpillMedium: Send + Sync {
    /// One append event: `bytes` land at the committed tail; returns the
    /// offset the chunk starts at.
    fn append(&self, bytes: &[u8]) -> std::io::Result<u64>;
    /// One read event over committed bytes (exact length).
    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()>;
}

/// One statement engagement's spill namespace: hands out private files
/// (one writer per (purpose, worker ordinal) — the collision-free naming
/// law). Dropping the store deletes every file it handed out.
pub trait SpillStore: Send + Sync {
    fn file(&self, purpose: &'static str, worker: usize) -> std::io::Result<Box<dyn SpillMedium>>;
}

/// Host-registered store factory (`None` = the host cannot provide temp
/// files right now — the caller treats spill as unavailable).
type Factory = fn() -> Option<Arc<dyn SpillStore>>;

static FACTORY: OnceLock<Factory> = OnceLock::new();

/// Register the host's store factory (idempotent, first writer wins —
/// the `set_worker_arm_hook` idiom).
pub fn set_store_factory(f: Factory) {
    let _ = FACTORY.set(f);
}

/// Is a spill substrate registered at all? (The planner's availability
/// probe: registration happens at host boot, before any statement.)
pub fn available() -> bool {
    FACTORY.get().is_some()
}

/// Mint one statement's spill store.
pub fn new_store() -> Option<Arc<dyn SpillStore>> {
    FACTORY.get().and_then(|f| f())
}

// ---------------------------------------------------------------------------
// Std-backed store (rig/tests only): unlinked temp files under the OS
// temp dir — the kernel owns cleanup, no name survives creation. The
// server NEVER uses this arm (its files must ride fd/pgsql_tmp for
// temp_file_limit parity); it is compiled only where no server exists.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "rig", feature = "teststore"))]
mod std_store {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct StdMedium {
        f: File,
        /// Committed tail (append offset). One writer per file by the
        /// naming law; the lock is the &self seam, not real contention.
        tail: Mutex<u64>,
    }

    impl SpillMedium for StdMedium {
        fn append(&self, bytes: &[u8]) -> std::io::Result<u64> {
            let mut tail = self.tail.lock().unwrap();
            let off = *tail;
            self.f.write_all_at(bytes, off)?;
            *tail = off + bytes.len() as u64;
            Ok(off)
        }
        fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
            self.f.read_exact_at(buf, off)
        }
    }

    struct StdStore;

    impl SpillStore for StdStore {
        fn file(
            &self,
            purpose: &'static str,
            worker: usize,
        ) -> std::io::Result<Box<dyn SpillMedium>> {
            static CTR: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "sqe-spill-{}-{purpose}-w{worker}-{}",
                std::process::id(),
                CTR.fetch_add(1, Ordering::Relaxed),
            ));
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?;
            // Unlink immediately: the handle is the only name.
            std::fs::remove_file(&path)?;
            Ok(Box::new(StdMedium { f, tail: Mutex::new(0) }))
        }
    }

    pub fn std_store_factory() -> Option<Arc<dyn SpillStore>> {
        Some(Arc::new(StdStore))
    }
}

#[cfg(any(test, feature = "rig", feature = "teststore"))]
pub use std_store::std_store_factory;

/// Register the std-backed store (rig/test harness boot).
#[cfg(any(test, feature = "rig", feature = "teststore"))]
pub fn register_std_store() {
    set_store_factory(std_store_factory);
}

// ---------------------------------------------------------------------------
// Chunk cursor: bounded-memory sequential decode over one committed chunk
// (fixed-width records; the arm owns the record layout — R2: the format
// is a compile-time constant of the arm that wrote it).
// ---------------------------------------------------------------------------

/// Read slab: bounded read-ahead per open cursor (accounted by callers as
/// `SLAB_BYTES` per live cursor).
pub const SLAB_BYTES: usize = 256 * 1024;

/// [spill-2] Typed spill I/O failure: raised through the `RunRefusal`
/// unwind (the finalize-refusal transport) instead of the v1 PANIC — a
/// failed temp-file event is the STATEMENT's error (I/O class), never a
/// server death. One seam for every spill medium event.
#[cold]
pub fn io_fail(op: &'static str, e: std::io::Error) -> ! {
    crate::refuse::raise_runtime(crate::refuse::Refuse::SpillIo { op, detail: e.to_string() })
}

pub struct ChunkCursor<'a> {
    m: &'a dyn SpillMedium,
    /// Next unread byte (file offset).
    off: u64,
    /// One past the chunk's last byte.
    end: u64,
    rec: usize,
    slab: usize,
    buf: Vec<u8>,
    /// Consumed prefix of `buf`.
    pos: usize,
}

impl<'a> ChunkCursor<'a> {
    /// Cursor over `[off, off + nrec * rec)`; `rec` is the fixed record
    /// width in bytes; `slab` bounds the cursor's read-ahead (callers
    /// account `slab` bytes per live cursor — many-run merges shrink it).
    pub fn new(
        m: &'a dyn SpillMedium,
        off: u64,
        nrec: u64,
        rec: usize,
        slab: usize,
    ) -> ChunkCursor<'a> {
        let slab = slab.max(rec);
        ChunkCursor { m, off, end: off + nrec * rec as u64, rec, slab, buf: Vec::new(), pos: 0 }
    }

    /// The next record's bytes, or `None` at chunk end. A failed read is
    /// the statement's typed I/O error ([`io_fail`] — a spill file is
    /// process-private committed state; a torn read is never a condition
    /// to answer around).
    pub fn next(&mut self) -> Option<&[u8]> {
        if self.pos + self.rec > self.buf.len() {
            let left = (self.end - self.off) as usize;
            if left == 0 {
                return None;
            }
            let want = left.min(self.slab / self.rec * self.rec);
            self.buf.resize(want, 0);
            self.m
                .read_at(self.off, &mut self.buf)
                .unwrap_or_else(|e| io_fail("read", e));
            self.off += want as u64;
            self.pos = 0;
        }
        let r = &self.buf[self.pos..self.pos + self.rec];
        self.pos += self.rec;
        Some(r)
    }
}

// ---------------------------------------------------------------------------
// [spill-2] Byte-extent cursor: bounded-memory sequential decode over one
// committed chunk of SELF-DELIMITING records (the byte-key arms' varlen
// spill records — R2 still holds: the record layout is a compile-time
// constant of the arm that wrote it; only the LENGTHS are per-record).
// ---------------------------------------------------------------------------

pub struct ByteCursor<'a> {
    m: &'a dyn SpillMedium,
    /// Next unread file byte.
    off: u64,
    /// One past the chunk's last byte.
    end: u64,
    slab: usize,
    buf: Vec<u8>,
    /// Consumed prefix of `buf`.
    pos: usize,
}

impl<'a> ByteCursor<'a> {
    /// Cursor over `[off, off + len)`; `slab` bounds steady-state
    /// read-ahead (a single record longer than the slab still reads
    /// whole — the buffer grows to the record, the accounted bound is
    /// `max(slab, longest record)`).
    pub fn new(m: &'a dyn SpillMedium, off: u64, len: u64, slab: usize) -> ByteCursor<'a> {
        ByteCursor { m, off, end: off + len, slab: slab.max(64), buf: Vec::new(), pos: 0 }
    }

    /// Bytes left in the extent (unread file bytes + buffered tail).
    pub fn remaining(&self) -> u64 {
        (self.end - self.off) + (self.buf.len() - self.pos) as u64
    }

    /// The next `n` bytes, or `None` when the extent is fully consumed
    /// (asking past a non-empty tail is a caller bug — records are
    /// whole by the writer's law).
    pub fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.remaining() == 0 && n > 0 {
            return None;
        }
        if self.pos + n > self.buf.len() {
            // Compact the unread tail, then refill at slab grain (or to
            // the record, whichever is larger).
            self.buf.copy_within(self.pos.., 0);
            self.buf.truncate(self.buf.len() - self.pos);
            self.pos = 0;
            let left = (self.end - self.off) as usize;
            let want = left.min(self.slab.max(n - self.buf.len()));
            assert!(
                self.buf.len() + want >= n,
                "sqe spill byte cursor: record crosses the chunk end (writer law violated)"
            );
            let base = self.buf.len();
            self.buf.resize(base + want, 0);
            self.m
                .read_at(self.off, &mut self.buf[base..])
                .unwrap_or_else(|e| io_fail("read", e));
            self.off += want as u64;
        }
        let r = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Some(r)
    }
}
