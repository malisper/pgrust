//! Spill file lifecycle (O-M2-3): an fd `FileSet` under `pgsql_tmp`,
//! named per-worker files, event-scoped writers/readers, and the plain-data
//! extent directories that ride sink Locals.
//!
//! ## The residency ruling, inherited not rebuilt
//!
//! Every file here is created through `PathNameCreateTemporaryFile` /
//! opened through `PathNameOpenTemporaryFile` (via `FileSet` segment
//! helpers), which is what buys, with zero code in this crate:
//!
//! - **temp_file_limit**: `FD_TEMP_FILE_LIMIT` is set on every created
//!   segment; enforcement lives in `fd::io::FileWrite` and accounts on the
//!   CREATING thread (per-thread == per-participant under
//!   thread-per-worker — the m3.5 inc-1 parity answer). Reader opens do
//!   not double-count.
//! - **resowner belt**: created/opened VFDs are registered
//!   `FD_CLOSE_AT_EOXACT` with the current resource owner — a mid-event
//!   unwind that skips our Drop still loses its handle at abort cleanup.
//! - **pg_stat + log_temp_files**: deletion reports through
//!   `ReportTemporaryFileUsage` exactly as C's temp files do.
//! - **crash cleanup**: the directory is `pgsql_tmp<pid>.<set>.fileset`
//!   under the temp dir; `fd::RemovePgTempFiles` reaps it at startup.
//!
//! ## Event discipline (m3.5 §2, inc-1 amendment)
//!
//! No handle survives an I/O event. A [`SpillWriter`] / [`SpillReader`] IS
//! one event: created on the acting thread, holds at most one open segment
//! VFD, and closes it at `finish`/`close` (Drop is the abandon path — it
//! closes, commits nothing, and is a guarded no-op during proc_exit).
//! Between events a [`SpillFile`] is plain `Send + Sync` data.
//!
//! ## Cross-thread law
//!
//! Writers write their OWN file on their own thread (single-writer by the
//! [`spill_file_name`] naming law); readers open the FROZEN file by name on
//! their own thread. Frozen-before-read is the deps-DAG happens-before
//! edge the sink contract already provides — this crate adds no
//! synchronization and no shared mutable state, it just refuses to read
//! past the committed watermark its descriptor snapshot carries.

use std::sync::Arc;

use ::elog::ereport;
use ::types_error::{PgResult, ERROR};
use ::types_storage::File;

use fd::io::{FileClose, FileRead, FileWrite};
use fd::{get_errno, FileSet};

use crate::io::io_event;
use crate::page::{loc, spill_err, PAGE_SIZE};

/// Production segment size: 1 GiB, the BufFile `MAX_PHYSICAL_FILESIZE` law
/// (segment `i` of file `name` lives at `<fileset>/<name>.<i>`). A
/// [`PAGE_SIZE`] multiple so pool pages never straddle segments.
pub const SEG_BYTES: u64 = 0x4000_0000;

const _: () = assert!(SEG_BYTES % PAGE_SIZE as u64 == 0);

/// The per-file naming law (m3.5 §2): one writer per (node, generation,
/// purpose, worker) — collisions are structurally impossible when every
/// writer uses its own worker ordinal, and re-published generations get
/// fresh names (stale spill data is unreachable by construction).
pub fn spill_file_name(node: u32, generation: u64, purpose: &str, worker: u32) -> String {
    debug_assert!(!purpose.contains('/') && !purpose.contains('.'));
    format!("spill-{node}-{generation}-{purpose}-w{worker}")
}

/// The engagement's spill namespace: an fd `FileSet` under `pgsql_tmp`.
/// Rides the engagement payload as an `Arc`; the LAST drop deletes the
/// whole directory tree (`FileSet::drop = delete_all` — teardown paths 1–4
/// of the crate contract; path 5 is the startup reaper).
pub struct SpillSet {
    fs: FileSet,
}

impl SpillSet {
    /// Create the namespace. FAIL-CLOSED, release-effective: a thread
    /// without initialized temp-file access (M0 pool workers — m3.5 §6.2)
    /// gets an ERROR here, at admission, never a stumble inside an event
    /// (the debug-assert-masking law: fd's own belts are debug-only).
    pub fn create() -> PgResult<Arc<SpillSet>> {
        if !fd::TempFileAccessReady() {
            ereport(ERROR)
                .errmsg_internal(
                    "spill substrate requires temporary file access on this thread".to_string(),
                )
                .finish(loc("SpillSet::create"))?;
        }
        let fs = FileSet::init()?;
        Ok(Arc::new(SpillSet { fs }))
    }

    /// Delete every file and directory now (idempotent; Drop repeats it
    /// harmlessly). The explicit form exists for consumers that tear down
    /// eagerly on the error path before the payload itself drops.
    pub fn delete_all(&self) -> PgResult<()> {
        self.fs.delete_all()
    }

    fn seg_path(&self, name: &str, seg: u32) -> String {
        format!("{}.{seg}", self.fs.name_path(name))
    }

    /// Open-or-create segment `seg` of `name` for writing (O_RDWR; create
    /// on ENOENT — never O_TRUNC over existing bytes).
    fn open_seg_rw(&self, name: &str, seg: u32) -> PgResult<File> {
        let path = self.seg_path(name, seg);
        let f = self.fs.open_seg(&path, libc::O_RDWR)?;
        if f.0 > 0 {
            return Ok(f);
        }
        self.fs.create_seg(name, &path)
    }

    /// Open segment `seg` of `name` read-only; ERROR when absent (readers
    /// only ever chase committed bytes, so a missing segment is torn state,
    /// not a probe).
    fn open_seg_ro(&self, name: &str, seg: u32) -> PgResult<File> {
        let path = self.seg_path(name, seg);
        let f = self.fs.open_seg(&path, libc::O_RDONLY)?;
        if f.0 <= 0 {
            ereport(ERROR)
                .with_saved_errno(get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open temporary file \"{path}\": %m"))
                .finish(loc("SpillSet::open_seg_ro"))?;
        }
        Ok(f)
    }
}

/// One named spill file: PLAIN DATA between events (`Send + Sync`, Clone).
/// The owner (a sink Local) holds the writable descriptor; readers hold
/// clones whose `committed` snapshot freezes what they may see.
#[derive(Clone)]
pub struct SpillFile {
    set: Arc<SpillSet>,
    name: String,
    seg_bytes: u64,
    committed: u64,
}

/// One extent of committed bytes within a [`SpillFile`] (logical offsets;
/// segment mapping is internal). Plain data; layout-pinned (16 bytes) —
/// directories of these ride every spilling Local.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpillExtent {
    pub off: u64,
    pub len: u64,
}

const _: () = assert!(core::mem::size_of::<SpillExtent>() == 16);

impl SpillFile {
    /// A new (empty) named file in `set`. Nothing touches disk until the
    /// first writer flush.
    pub fn new(set: Arc<SpillSet>, name: String) -> SpillFile {
        SpillFile { set, name, seg_bytes: SEG_BYTES, committed: 0 }
    }

    /// Test-only geometry override (small segments exercise the split
    /// walk); production code has exactly one segment size.
    #[cfg(test)]
    pub(crate) fn new_with_seg_bytes(set: Arc<SpillSet>, name: String, seg_bytes: u64) -> SpillFile {
        assert!(seg_bytes > 0 && seg_bytes % PAGE_SIZE as u64 == 0);
        SpillFile { set, name, seg_bytes, committed: 0 }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The committed watermark: bytes a reader may see. Moves ONLY at
    /// [`SpillWriter::finish`] — an abandoned event commits nothing.
    pub fn committed(&self) -> u64 {
        self.committed
    }

    /// Begin one append event at the committed watermark (single-writer:
    /// the &mut receiver IS the discipline — clones cannot append).
    pub fn append(&mut self) -> PgResult<SpillWriter<'_>> {
        Ok(SpillWriter {
            pos: self.committed,
            file: self,
            buf: Vec::with_capacity(PAGE_SIZE),
            cur_seg: None,
            extent_start: None,
            bytes_written: 0,
            extents_written: 0,
            finished: false,
            _not_send: core::marker::PhantomData,
        })
    }

    /// Begin one read event over the committed snapshot of THIS descriptor
    /// (clone the descriptor across threads after the writer finished; the
    /// deps DAG provides the happens-before edge).
    pub fn open_read(&self) -> SpillReader {
        SpillReader {
            file: self.clone(),
            cur_seg: None,
            bytes_read: 0,
            _not_send: core::marker::PhantomData,
        }
    }

    /// One-shot positioned write event (the pool's page unload). Bounded
    /// callers only: opens and closes the touched segment(s) within the
    /// call, under the blocking-section facade.
    pub(crate) fn write_at(&self, off: u64, data: &[u8]) -> PgResult<()> {
        io_event(|| {
            let mut cur: Option<(u32, File)> = None;
            let r = write_span(&self.set, &self.name, self.seg_bytes, &mut cur, off, data);
            close_cached(&mut cur, r)
        })
    }

    /// One-shot positioned exact read event (the pool's page reload).
    /// Unlike stream reads this trusts the POOL's extent table, not
    /// `committed` (pool extents are valid the moment their write event
    /// returns).
    pub(crate) fn read_at_unchecked(&self, off: u64, buf: &mut [u8]) -> PgResult<()> {
        io_event(|| {
            let mut cur: Option<(u32, File)> = None;
            let r = read_span(&self.set, &self.name, self.seg_bytes, &mut cur, off, buf);
            close_cached(&mut cur, r)
        })
    }
}

/// Close a cached segment handle, preferring the primary result's error.
fn close_cached(cur: &mut Option<(u32, File)>, primary: PgResult<()>) -> PgResult<()> {
    let close_r = match cur.take() {
        Some((_, f)) => FileClose(f),
        None => Ok(()),
    };
    primary.and(close_r)
}

/// Write `data` at logical `off`, splitting across segment files; reuses
/// (and re-targets) the caller's cached open segment.
fn write_span(
    set: &SpillSet,
    name: &str,
    seg_bytes: u64,
    cur: &mut Option<(u32, File)>,
    mut off: u64,
    mut data: &[u8],
) -> PgResult<()> {
    while !data.is_empty() {
        let seg = (off / seg_bytes) as u32;
        let in_off = off % seg_bytes;
        let n = ((seg_bytes - in_off) as usize).min(data.len());
        let f = cached_seg(set, name, cur, seg, /* rw */ true)?;
        let mut done = 0usize;
        while done < n {
            let w = FileWrite(f, &data[done..n], (in_off + done as u64) as i64, 0)?;
            if w <= 0 {
                return spill_err(format!(
                    "could not write to spill file \"{name}\" segment {seg}"
                ));
            }
            done += w as usize;
        }
        off += n as u64;
        data = &data[n..];
    }
    Ok(())
}

/// Exact read of `buf` at logical `off`; short reads FAIL CLOSED.
fn read_span(
    set: &SpillSet,
    name: &str,
    seg_bytes: u64,
    cur: &mut Option<(u32, File)>,
    mut off: u64,
    mut buf: &mut [u8],
) -> PgResult<()> {
    while !buf.is_empty() {
        let seg = (off / seg_bytes) as u32;
        let in_off = off % seg_bytes;
        let n = ((seg_bytes - in_off) as usize).min(buf.len());
        let f = cached_seg(set, name, cur, seg, /* rw */ false)?;
        let mut done = 0usize;
        while done < n {
            let r = FileRead(f, &mut buf[done..n], (in_off + done as u64) as i64, 0)?;
            if r <= 0 {
                return spill_err(format!(
                    "could not read from spill file \"{name}\" segment {seg}: \
                     read {done} of {n} bytes"
                ));
            }
            done += r as usize;
        }
        off += n as u64;
        buf = &mut buf[n..];
    }
    Ok(())
}

/// Resolve the cached handle for `seg`, rotating (close + open) when the
/// span moved on.
fn cached_seg(
    set: &SpillSet,
    name: &str,
    cur: &mut Option<(u32, File)>,
    seg: u32,
    rw: bool,
) -> PgResult<File> {
    if let Some((s, f)) = *cur {
        if s == seg {
            return Ok(f);
        }
        cur.take();
        FileClose(f)?;
    }
    let f = if rw { set.open_seg_rw(name, seg)? } else { set.open_seg_ro(name, seg)? };
    *cur = Some((seg, f));
    Ok(f)
}

/// One append EVENT on a [`SpillFile`]: buffered sequential writes with
/// extent bookkeeping. Byref class `OpenHandle`: thread-affine,
/// event-scoped, closed at [`SpillWriter::finish`] (or by the Drop abandon
/// path — which commits nothing).
///
/// Envelope note (m3.5 §7 "spill buffers" row): one [`PAGE_SIZE`] buffer
/// per open event, a CONSTANT the consumer's admission envelope counts.
///
/// `!Send` by construction — VFD indexes are meaningful only in the
/// opening thread's fd cache:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// fn f(w: sqe_spill::SpillWriter<'static>) { assert_send::<sqe_spill::SpillWriter>(); }
/// ```
pub struct SpillWriter<'a> {
    file: &'a mut SpillFile,
    /// Logical offset of the buffer's start.
    pos: u64,
    buf: Vec<u8>,
    cur_seg: Option<(u32, File)>,
    extent_start: Option<u64>,
    bytes_written: u64,
    extents_written: u64,
    finished: bool,
    /// Byref class `OpenHandle`: thread-affine, never crosses threads.
    _not_send: core::marker::PhantomData<*const ()>,
}

impl SpillWriter<'_> {
    /// Logical write position (== the next byte's offset).
    pub fn tell(&self) -> u64 {
        self.pos + self.buf.len() as u64
    }

    /// Buffered append.
    pub fn write(&mut self, mut data: &[u8]) -> PgResult<()> {
        while !data.is_empty() {
            let room = PAGE_SIZE - self.buf.len();
            let n = room.min(data.len());
            self.buf.extend_from_slice(&data[..n]);
            data = &data[n..];
            if self.buf.len() == PAGE_SIZE {
                self.flush()?;
            }
        }
        Ok(())
    }

    /// Mark the start of a partition-contiguous extent (m3.5 §2: one flush
    /// event = partition-contiguous segments + directory entries).
    pub fn begin_extent(&mut self) {
        debug_assert!(self.extent_start.is_none(), "nested begin_extent");
        self.extent_start = Some(self.tell());
    }

    /// Close the open extent and return its directory entry.
    pub fn end_extent(&mut self) -> SpillExtent {
        let start = self.extent_start.take().expect("end_extent without begin_extent");
        self.extents_written += 1;
        SpillExtent { off: start, len: self.tell() - start }
    }

    /// Bytes flushed to disk so far by this event (metrics feed).
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Extents closed by this event (metrics feed).
    pub fn extents_written(&self) -> u64 {
        self.extents_written
    }

    fn flush(&mut self) -> PgResult<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let (set, name, seg_bytes) = (&self.file.set, &self.file.name, self.file.seg_bytes);
        let (pos, cur, buf) = (self.pos, &mut self.cur_seg, &self.buf);
        io_event(|| write_span(set, name, seg_bytes, cur, pos, buf))?;
        self.pos += self.buf.len() as u64;
        self.bytes_written += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }

    /// Flush, close, COMMIT: the watermark moves here and only here.
    /// Returns the new committed length.
    pub fn finish(mut self) -> PgResult<u64> {
        debug_assert!(self.extent_start.is_none(), "finish inside an open extent");
        self.flush()?;
        let r = io_event(|| close_cached(&mut self.cur_seg, Ok(())));
        self.finished = true;
        let committed = self.pos;
        r?;
        self.file.committed = committed;
        Ok(committed)
    }
}

impl Drop for SpillWriter<'_> {
    /// The ABANDON path (ERROR/cancel unwind): close the handle, commit
    /// nothing — the next append event overwrites the torn tail. During
    /// proc_exit the resowner release already freed every temp VFD, so
    /// closing here would touch dead state: skip (the buffile precedent).
    fn drop(&mut self) {
        if self.finished || ::elog::config::proc_exit_inprogress() {
            return;
        }
        if let Some((_, f)) = self.cur_seg.take() {
            let _ = FileClose(f);
        }
    }
}

/// One read EVENT over a [`SpillFile`] descriptor's committed snapshot.
/// Byref class `OpenHandle`, same discipline (and the same `!Send` law)
/// as the writer:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// fn f() { assert_send::<sqe_spill::SpillReader>(); }
/// ```
pub struct SpillReader {
    file: SpillFile,
    cur_seg: Option<(u32, File)>,
    bytes_read: u64,
    /// Byref class `OpenHandle`: thread-affine, never crosses threads.
    _not_send: core::marker::PhantomData<*const ()>,
}

impl SpillReader {
    /// Exact positioned read, fail-closed at the committed watermark: a
    /// stream reader can never observe an uncommitted (torn) tail.
    pub fn read_at(&mut self, off: u64, buf: &mut [u8]) -> PgResult<()> {
        if off + buf.len() as u64 > self.file.committed {
            return spill_err(format!(
                "spill read past committed watermark: {} + {} > {} in \"{}\"",
                off,
                buf.len(),
                self.file.committed,
                self.file.name
            ));
        }
        let (set, name, seg_bytes) = (&self.file.set, &self.file.name, self.file.seg_bytes);
        let cur = &mut self.cur_seg;
        io_event(|| read_span(set, name, seg_bytes, cur, off, buf))?;
        self.bytes_read += buf.len() as u64;
        Ok(())
    }

    /// Read one directory extent into `out` (replacing its contents).
    pub fn read_extent(&mut self, ext: SpillExtent, out: &mut Vec<u8>) -> PgResult<()> {
        out.clear();
        out.resize(ext.len as usize, 0);
        self.read_at(ext.off, out)
    }

    /// Bytes read by this event (metrics feed).
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Close the event's handle explicitly (error-checked form of Drop).
    pub fn close(mut self) -> PgResult<()> {
        io_event(|| close_cached(&mut self.cur_seg, Ok(())))
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        if ::elog::config::proc_exit_inprogress() {
            return;
        }
        if let Some((_, f)) = self.cur_seg.take() {
            let _ = FileClose(f);
        }
    }
}

// ---------------------------------------------------------------------------
// Extent directories (plain Send data riding Locals — byref class
// `PlainDirectory`)
// ---------------------------------------------------------------------------

/// One flush epoch's per-partition extents: `parts[p]` is partition `p`'s
/// contiguous bytes within the owning Local's file (len 0 = partition
/// empty this epoch).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EpochDir {
    parts: Vec<SpillExtent>,
}

impl EpochDir {
    pub fn new(nparts: u32) -> EpochDir {
        EpochDir { parts: vec![SpillExtent::default(); nparts as usize] }
    }

    pub fn set_part(&mut self, part: u32, ext: SpillExtent) {
        self.parts[part as usize] = ext;
    }

    pub fn part(&self, part: u32) -> SpillExtent {
        self.parts[part as usize]
    }

    pub fn nparts(&self) -> u32 {
        self.parts.len() as u32
    }
}

/// A Local's whole spill directory: every epoch it flushed, uniform
/// partition count. The combine path for partition `p` walks
/// [`StreamDir::part_extents`] in epoch order (slice order — the
/// reproducibility nicety, never a gate surface).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamDir {
    nparts: u32,
    epochs: Vec<EpochDir>,
}

impl StreamDir {
    pub fn new(nparts: u32) -> StreamDir {
        StreamDir { nparts, epochs: Vec::new() }
    }

    pub fn push_epoch(&mut self, epoch: EpochDir) {
        assert_eq!(epoch.nparts(), self.nparts, "epoch partition-count drift");
        self.epochs.push(epoch);
    }

    pub fn nparts(&self) -> u32 {
        self.nparts
    }

    pub fn nepochs(&self) -> usize {
        self.epochs.len()
    }

    /// Partition `p`'s non-empty extents across epochs, in epoch order.
    pub fn part_extents(&self, part: u32) -> impl Iterator<Item = SpillExtent> + '_ {
        self.epochs
            .iter()
            .map(move |e| e.part(part))
            .filter(|e| e.len > 0)
    }

    /// Total spilled bytes recorded in this directory.
    pub fn total_bytes(&self) -> u64 {
        self.epochs
            .iter()
            .flat_map(|e| e.parts.iter())
            .map(|e| e.len)
            .sum()
    }
}
