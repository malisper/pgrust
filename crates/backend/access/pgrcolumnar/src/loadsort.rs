//! load-r2 L3-1 step (b): spill-run codec + k-way merge for the parallel
//! load sort (docs: notes/load-r2-lane.md "L3-1 design").
//!
//! Parallel-COPY workers parse+convert rows, encode a fixed-width memcmp
//! sort key per row (`crate::sortkey`), and accumulate `(key, rowbytes)`
//! entries in a bounded `SortBatch`; each full batch is key-sorted and
//! spilled as one RUN file. After input ends, `RunMerge` streams every run
//! in global key order (binary min-heap on the fixed-width keys — proven
//! merge algebra: merge-of-sorted-runs == global sort, fleet wave-1/4
//! byte-identity legs), feeding 65,536-row spans to the RG encoders.
//!
//! Run file bytes: repeated entries `[key: KW][rowlen: u32 le][rowbytes]`.
//! Row bytes (`RowCodec`, by ColType): I16 2B le | I32/Date 4B le |
//! I64/Timestamp 8B le | Text u32 le payload len + payload. NULLs never
//! reach a run (pgrcolumnar refuses NULLs at ingest).

use crate::format::ColType;
use crate::varlena_bytes;
use ::datum::Datum;
use ::types_error::{PgError, PgResult};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read, Write};

fn io_err(what: &str, e: std::io::Error) -> Box<PgError> {
    Box::new(PgError::error(format!("parallel load-sort {what}: {e}")))
}

/// DST P4 dataplane arm (was DST P1 WS-B's "until the sim read arm lands"
/// carve-out): run files are vfs descriptors END TO END. Open, pread/pwrite
/// and close all go through the same provider (`vfs::*`), so under
/// `--cfg pgrust_sim` the whole spill/merge data plane lands in SimVfs and
/// P4 fault plans gate every step (Open / PWriteV / PReadV / Close).
///
/// Off-sim this is what the `std::fs::File` it replaces did, at the syscall
/// level: open(2) with the same parity flags (O_CLOEXEC always, mode 0o666,
/// umask applies); pread(2)/pwrite(2) at an owned cursor instead of
/// read(2)/write(2) on the kernel file offset (same syscall count, and the
/// descriptors are exclusively owned, so positional-vs-offset IO is
/// behaviorally identical — one register add per buffered-IO call is the
/// whole delta); close(2) from the [`vfs::VfsFd`] guard's drop, the same
/// single ignored-result close File's drop made.
///
/// C provenance / durability: spill runs are statement-scoped temp files
/// (the PG temp-file contract — see the lz4 corruption note below): no
/// fsync, no rename. Create/write/read/close IS the whole sequence, so
/// routing these four ops is the complete sim arm for this site.
struct RunFile {
    fd: vfs::VfsFd,
    /// Sequential cursor (the file offset std's read/write used to advance).
    pos: u64,
}

impl std::os::fd::AsRawFd for RunFile {
    #[inline]
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.fd)
    }
}

impl Read for RunFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::os::fd::AsRawFd;
        let n = vfs::pread(self.fd.as_raw_fd(), buf, self.pos as vfs::off_t);
        if n < 0 {
            return Err(std::io::Error::from_raw_os_error(vfs::get_errno()));
        }
        self.pos += n as u64;
        Ok(n as usize)
    }
}

impl Write for RunFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use std::os::fd::AsRawFd;
        let n = vfs::pwrite(self.fd.as_raw_fd(), buf, self.pos as vfs::off_t);
        if n < 0 {
            return Err(std::io::Error::from_raw_os_error(vfs::get_errno()));
        }
        self.pos += n as u64;
        Ok(n as usize)
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        // No userspace buffer here — std::fs::File's flush was the same no-op.
        Ok(())
    }
}

fn vfs_open_file(
    path: &std::path::Path,
    flags: libc::c_int,
    what: &str,
) -> PgResult<RunFile> {
    #[cfg(not(target_family = "wasm"))]
    use std::os::unix::ffi::OsStrExt;
    // wasm32: same as_bytes surface, wasi's spelling of the trait home.
    #[cfg(target_family = "wasm")]
    use std::os::wasi::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io_err(
            what,
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "file name contained an unexpected NUL byte",
            ),
        )
    })?;
    // std parity: open(2) EINTR-retried (cvt_r), O_CLOEXEC, mode 0o666.
    let fd = loop {
        let fd = vfs::open(&c, flags | libc::O_CLOEXEC, 0o666);
        if fd >= 0 {
            break fd;
        }
        if vfs::get_errno() != libc::EINTR {
            return Err(io_err(what, std::io::Error::from_raw_os_error(vfs::get_errno())));
        }
    };
    // SAFETY: fresh descriptor minted by vfs::open above, exclusively owned
    // by the returned guard.
    Ok(RunFile { fd: unsafe { vfs::VfsFd::from_raw(fd) }, pos: 0 })
}

// ---- row codec -------------------------------------------------------------

pub struct RowCodec {
    pub coltypes: Vec<ColType>,
}

impl RowCodec {
    pub fn new(coltypes: Vec<ColType>) -> RowCodec {
        RowCodec { coltypes }
    }

    /// Append the row image to `out`.
    pub fn serialize_row(&self, values: &[Datum], out: &mut Vec<u8>) -> PgResult<()> {
        for (c, t) in self.coltypes.iter().enumerate() {
            match t {
                ColType::I16 => out.extend_from_slice(&values[c].as_i16().to_le_bytes()),
                ColType::I32 | ColType::Date => {
                    out.extend_from_slice(&values[c].as_i32().to_le_bytes())
                }
                ColType::I64 | ColType::Timestamp => {
                    out.extend_from_slice(&values[c].as_i64().to_le_bytes())
                }
                ColType::Text => {
                    let b = varlena_bytes(values[c])?;
                    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    out.extend_from_slice(b);
                }
            }
        }
        Ok(())
    }

    /// Rebuild the row's datums from a row image. Text datums are 4B-U
    /// varlena images built in `arena`; they stay valid until the arena is
    /// cleared or grown — the caller consumes the datums immediately (the
    /// encoder append copies) and resets the arena per row.
    pub fn deserialize_row(
        &self,
        row: &[u8],
        arena: &mut Vec<u8>,
        values: &mut [Datum],
    ) -> PgResult<()> {
        // Pre-scan text payload sizes so arena pushes cannot reallocate
        // mid-row (a realloc would invalidate this row's earlier datums).
        let mut off = 0usize;
        let mut text_total = 0usize;
        for t in &self.coltypes {
            match t {
                ColType::I16 => off += 2,
                ColType::I32 | ColType::Date => off += 4,
                ColType::I64 | ColType::Timestamp => off += 8,
                ColType::Text => {
                    let len = u32::from_le_bytes(
                        row.get(off..off + 4)
                            .ok_or_else(|| {
                                io_err("row image", std::io::ErrorKind::UnexpectedEof.into())
                            })?
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    off += 4 + len;
                    text_total += 4 + len;
                }
            }
        }
        if off != row.len() {
            return Err(Box::new(PgError::error(format!(
                "parallel load-sort row image length mismatch: {} != {}",
                off,
                row.len()
            ))));
        }
        arena.reserve(text_total);
        let mut off = 0usize;
        for (c, t) in self.coltypes.iter().enumerate() {
            match t {
                ColType::I16 => {
                    values[c] =
                        Datum::from_i16(i16::from_le_bytes(row[off..off + 2].try_into().unwrap()));
                    off += 2;
                }
                ColType::I32 | ColType::Date => {
                    values[c] =
                        Datum::from_i32(i32::from_le_bytes(row[off..off + 4].try_into().unwrap()));
                    off += 4;
                }
                ColType::I64 | ColType::Timestamp => {
                    values[c] =
                        Datum::from_i64(i64::from_le_bytes(row[off..off + 8].try_into().unwrap()));
                    off += 8;
                }
                ColType::Text => {
                    let len =
                        u32::from_le_bytes(row[off..off + 4].try_into().unwrap()) as usize;
                    off += 4;
                    let start = arena.len();
                    arena.extend_from_slice(&(((len + 4) as u32) << 2).to_le_bytes());
                    arena.extend_from_slice(&row[off..off + len]);
                    off += len;
                    values[c] = Datum::from_usize(arena[start..].as_ptr() as usize);
                }
            }
        }
        Ok(())
    }
}

// ---- bounded in-memory batch -> sorted run ---------------------------------

/// Bounded (key, row) accumulator. `sort()` orders entries by memcmp key.
///
/// GL-LOADDET-1: key-equal entries are NOT byte-equal entries (the payload
/// columns differ), so their relative order is an output byte surface. A
/// `stable` batch breaks those ties by ARRIVAL order; the legacy batch leaves
/// them to pdqsort's pivot walk over whatever the batch happened to hold.
pub struct SortBatch {
    key_w: usize,
    arena: Vec<u8>,
    /// (entry offset, entry len incl. key) in arena.
    index: Vec<(u64, u32)>,
    /// GL-LOADDET-1: break key ties by arrival order (see `sort`).
    stable: bool,
}

impl SortBatch {
    /// Legacy (key-only, unstable) tie order. Kept as the default so the
    /// determinism kill switch reproduces the pre-GL-LOADDET-1 byte image
    /// exactly, and so the DST/unit corpora keep their recorded orders.
    pub fn new(key_w: usize) -> SortBatch {
        SortBatch { key_w, arena: Vec::new(), index: Vec::new(), stable: false }
    }

    /// GL-LOADDET-1: arrival-order tie break — the deterministic batch.
    pub fn new_stable(key_w: usize) -> SortBatch {
        SortBatch { stable: true, ..SortBatch::new(key_w) }
    }

    pub fn bytes(&self) -> usize {
        self.arena.len() + self.index.len() * std::mem::size_of::<(u64, u32)>()
    }

    pub fn rows(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn push(&mut self, key: &[u8], row: &[u8]) {
        debug_assert_eq!(key.len(), self.key_w);
        let off = self.arena.len() as u64;
        self.arena.extend_from_slice(key);
        self.arena.extend_from_slice(row);
        self.index.push((off, (self.key_w + row.len()) as u32));
    }

    fn key_of(arena: &[u8], key_w: usize, e: (u64, u32)) -> &[u8] {
        &arena[e.0 as usize..e.0 as usize + key_w]
    }

    /// Sort the batch into key order.
    ///
    /// GL-LOADDET-1 (`stable`): the arena offset is monotone in push order
    /// (every push appends), so `(key, offset)` is a TOTAL order that
    /// reproduces a stable sort without storing a second index — the tie
    /// compare is reached only on an actual key tie.
    pub fn sort(&mut self) {
        let (arena, kw) = (&self.arena, self.key_w);
        if self.stable {
            self.index.sort_unstable_by(|a, b| {
                Self::key_of(arena, kw, *a).cmp(Self::key_of(arena, kw, *b)).then(a.0.cmp(&b.0))
            });
        } else {
            self.index.sort_unstable_by(|a, b| {
                Self::key_of(arena, kw, *a).cmp(Self::key_of(arena, kw, *b))
            });
        }
    }

    /// Write the (sorted) batch as one run and clear the batch for reuse.
    pub fn spill_run(&mut self, path: &std::path::Path) -> PgResult<()> {
        self.spill_run_opts(path, false).map(|_| ())
    }

    /// Spill the sorted batch as one run file; returns the file bytes
    /// written. `lz4` (loadcommit RUNLZ4 lever) writes the SAME logical
    /// entry stream inside lz4-compressed ~512 KB chunks: `RLZ4` magic,
    /// then frames `[raw_len u32 le][comp_len u32 le][lz4 block]`. Chunk
    /// boundaries are byte-level (entries may straddle); the reader
    /// reassembles a byte stream identical to the raw format, so the
    /// merge is unaffected by construction.
    pub fn spill_run_opts(&mut self, path: &std::path::Path, lz4: bool) -> PgResult<u64> {
        let f =
            vfs_open_file(path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, "run create")?;
        let mut w = BufWriter::with_capacity(1 << 20, f);
        let mut written: u64 = 0;
        if !lz4 {
            for &(off, len) in &self.index {
                let e = &self.arena[off as usize..off as usize + len as usize];
                let rowlen = (len as usize - self.key_w) as u32;
                w.write_all(&e[..self.key_w]).map_err(|e| io_err("run write", e))?;
                w.write_all(&rowlen.to_le_bytes()).map_err(|e| io_err("run write", e))?;
                w.write_all(&e[self.key_w..]).map_err(|e| io_err("run write", e))?;
                written += self.key_w as u64 + 4 + rowlen as u64;
            }
        } else {
            w.write_all(&RUN_LZ4_MAGIC).map_err(|e| io_err("run write", e))?;
            written += 4;
            let mut chunk: Vec<u8> = Vec::with_capacity(RUN_CHUNK + 4096);
            let mut comp: Vec<u8> = Vec::new();
            for &(off, len) in &self.index {
                let e = &self.arena[off as usize..off as usize + len as usize];
                let rowlen = (len as usize - self.key_w) as u32;
                chunk.extend_from_slice(&e[..self.key_w]);
                chunk.extend_from_slice(&rowlen.to_le_bytes());
                chunk.extend_from_slice(&e[self.key_w..]);
                if chunk.len() >= RUN_CHUNK {
                    written += write_lz4_frame(&mut w, &mut chunk, &mut comp)?;
                }
            }
            written += write_lz4_frame(&mut w, &mut chunk, &mut comp)?;
        }
        w.flush().map_err(|e| io_err("run flush", e))?;
        self.arena.clear();
        self.index.clear();
        Ok(written)
    }
}

// ---- loadcommit RUNLZ4: run-file compression framing ------------------------

const RUN_LZ4_MAGIC: [u8; 4] = *b"RLZ4";
/// Target raw bytes per compressed chunk (entries may straddle chunks).
const RUN_CHUNK: usize = 512 << 10;
/// Corruption guard: no sane frame exceeds this (chunks are ~512 KB plus
/// one row; rows are bounded by the varlena cap long before this).
const RUN_FRAME_CAP: usize = 1 << 30;

/// Compress + write `chunk` as one frame, clearing it; returns file bytes.
/// Empty chunk = no frame (0 bytes). Generic over the sink so the same
/// framing serves run FILES (BufWriter) and in-memory runs (frame Vecs) —
/// one encoder, byte-identical frames on both destinations.
fn write_lz4_frame(
    w: &mut impl Write,
    chunk: &mut Vec<u8>,
    comp: &mut Vec<u8>,
) -> PgResult<u64> {
    if chunk.is_empty() {
        return Ok(0);
    }
    let max = lz4_flex::block::get_maximum_output_size(chunk.len());
    if comp.len() < max {
        comp.resize(max, 0);
    }
    let n = lz4_flex::block::compress_into(chunk, comp)
        .map_err(|e| Box::new(PgError::error(format!("run lz4 compress: {e}"))))?;
    w.write_all(&(chunk.len() as u32).to_le_bytes()).map_err(|e| io_err("run write", e))?;
    w.write_all(&(n as u32).to_le_bytes()).map_err(|e| io_err("run write", e))?;
    w.write_all(&comp[..n]).map_err(|e| io_err("run write", e))?;
    chunk.clear();
    Ok(8 + n as u64)
}

// ---- copyfast lever 1: in-memory runs ---------------------------------------
//
// The single-pass-sort lever: a run kept as lz4 frames IN MEMORY instead of
// a spill file. Frame bytes are produced by the same `write_lz4_frame`
// encoder, so an overflow run flushed to disk is byte-identical to a
// directly-spilled file, and the merge consumes the identical entry stream
// from either home. Budgeted by `MemRunStore` (sized from ACTUAL headroom by
// the copy driver, never a blind constant); every refusal is today's file
// spill.

/// Global (per-statement) budget for in-memory runs. Reserve on keep,
/// release per frame as the merge consumes it (memory returns to the store
/// progressively during the fill, exactly when the writer's dict/stitch
/// memory grows).
///
/// Accounting seam: when engine-estate charging (mcx global-footprint
/// accounting, in flight on a sibling lane) lands, `try_reserve`/`release`
/// are the two lines that charge/uncharge it — keep all bookkeeping here.
pub struct MemRunStore {
    cap: std::sync::atomic::AtomicU64,
    used: std::sync::atomic::AtomicU64,
}

impl MemRunStore {
    pub fn new(cap: u64) -> std::sync::Arc<MemRunStore> {
        std::sync::Arc::new(MemRunStore {
            cap: std::sync::atomic::AtomicU64::new(cap),
            used: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Reserve `n` bytes against the cap; false = caller must spill to disk.
    pub fn try_reserve(&self, n: u64) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let cap = self.cap.load(Relaxed);
        let mut cur = self.used.load(Relaxed);
        loop {
            if cur.saturating_add(n) > cap {
                return false;
            }
            match self.used.compare_exchange_weak(cur, cur + n, Relaxed, Relaxed) {
                Ok(_) => return true,
                Err(c) => cur = c,
            }
        }
    }

    pub fn release(&self, n: u64) {
        self.used.fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn used(&self) -> u64 {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn cap(&self) -> u64 {
        self.cap.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// One sorted run held in memory: the run's lz4 frames (each frame = its
/// exact file encoding `[raw u32 le][comp u32 le][block]`; no magic — the
/// home is self-describing). `store`-attached runs release their remaining
/// bytes on drop; the merge reader releases per frame as it consumes.
pub struct MemRun {
    frames: std::collections::VecDeque<Vec<u8>>,
    bytes: u64,
    store: Option<std::sync::Arc<MemRunStore>>,
}

impl MemRun {
    /// Total frame bytes currently held (the accounting unit).
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Attach the store whose reservation covers this run (caller reserved).
    pub fn attach(mut self, store: std::sync::Arc<MemRunStore>) -> MemRun {
        self.store = Some(store);
        self
    }

    /// Overflow path: write the run to `path` as a spill file — magic +
    /// frames verbatim, byte-identical to `spill_run_opts(path, true)` of
    /// the same batch by construction. Returns file bytes written.
    pub fn write_to_file(&self, path: &std::path::Path) -> PgResult<u64> {
        let f =
            vfs_open_file(path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, "run create")?;
        let mut w = BufWriter::with_capacity(1 << 20, f);
        w.write_all(&RUN_LZ4_MAGIC).map_err(|e| io_err("run write", e))?;
        let mut written = 4u64;
        for fr in &self.frames {
            w.write_all(fr).map_err(|e| io_err("run write", e))?;
            written += fr.len() as u64;
        }
        w.flush().map_err(|e| io_err("run flush", e))?;
        Ok(written)
    }
}

impl Drop for MemRun {
    fn drop(&mut self) {
        if let Some(store) = &self.store {
            store.release(self.bytes);
        }
    }
}

impl SortBatch {
    /// Compress the (sorted) batch into an in-memory run and clear the
    /// batch for reuse — the same entry stream and lz4 framing as
    /// `spill_run_opts(_, true)`, one frame Vec per ~RUN_CHUNK of raw bytes.
    pub fn spill_run_mem(&mut self) -> PgResult<MemRun> {
        let mut frames = std::collections::VecDeque::new();
        let mut bytes = 0u64;
        let mut chunk: Vec<u8> = Vec::with_capacity(RUN_CHUNK + 4096);
        let mut comp: Vec<u8> = Vec::new();
        let mut emit =
            |chunk: &mut Vec<u8>, comp: &mut Vec<u8>| -> PgResult<()> {
                if chunk.is_empty() {
                    return Ok(());
                }
                let mut fb: Vec<u8> = Vec::new();
                bytes += write_lz4_frame(&mut fb, chunk, comp)?;
                frames.push_back(fb);
                Ok(())
            };
        for &(off, len) in &self.index {
            let e = &self.arena[off as usize..off as usize + len as usize];
            let rowlen = (len as usize - self.key_w) as u32;
            chunk.extend_from_slice(&e[..self.key_w]);
            chunk.extend_from_slice(&rowlen.to_le_bytes());
            chunk.extend_from_slice(&e[self.key_w..]);
            if chunk.len() >= RUN_CHUNK {
                emit(&mut chunk, &mut comp)?;
            }
        }
        emit(&mut chunk, &mut comp)?;
        self.arena.clear();
        self.index.clear();
        Ok(MemRun { frames, bytes, store: None })
    }
}

/// Read one lz4 frame from `r` into `raw` (resized to the frame's raw
/// length). Returns Ok(false) on clean EOF at a frame boundary; any other
/// truncation or length corruption is an error. `comp` is reused scratch.
/// On success adds the frame's file bytes to `*disk_bytes`.
fn read_lz4_frame(
    r: &mut impl Read,
    comp: &mut Vec<u8>,
    raw: &mut Vec<u8>,
    disk_bytes: &mut u64,
) -> std::io::Result<bool> {
    let mut hdr = [0u8; 8];
    let mut got = 0usize;
    while got < 8 {
        let n = r.read(&mut hdr[got..])?;
        if n == 0 {
            if got == 0 {
                return Ok(false);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated run frame header",
            ));
        }
        got += n;
    }
    let raw_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let comp_len = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as usize;
    if raw_len == 0 || raw_len > RUN_FRAME_CAP || comp_len == 0 || comp_len > RUN_FRAME_CAP {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "corrupt run frame lengths",
        ));
    }
    comp.resize(comp_len, 0);
    r.read_exact(comp)?;
    raw.resize(raw_len, 0);
    let n = lz4_flex::block::decompress_into(comp, raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    if n != raw_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "run frame decompressed length mismatch",
        ));
    }
    *disk_bytes += 8 + comp_len as u64;
    Ok(true)
}

/// Consume + verify the RLZ4 magic (the reader mode comes from the knob,
/// never from sniffing; a mismatch fails loud).
fn check_run_magic(r: &mut impl Read) -> std::io::Result<()> {
    let mut m = [0u8; 4];
    r.read_exact(&mut m)?;
    if m != RUN_LZ4_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "run file lacks RLZ4 magic (spill/merge lz4 mode mismatch)",
        ));
    }
    Ok(())
}

// ---- run reader + k-way merge ----------------------------------------------

/// loadcommit C2a: non-blocking kernel readahead hint for a run file's
/// upcoming window (pure hint — zero effect on the bytes read). Routed
/// through the Vfs boundary (DST P1); the platform split lives inside
/// PosixVfs (posix_fadvise WILLNEED on Linux). Errors ignored, as before.
fn fadvise_willneed(f: &RunFile, off: u64, len: u64) {
    // std::os::fd is the portable trait home (present on wasi; unix::io
    // merely re-exports it) — wasm-port-t26 census row 1 precedent.
    use std::os::fd::AsRawFd;
    vfs::fadvise_willneed(f.as_raw_fd(), off as libc::off_t, len as libc::off_t);
}

// ---- loadcommit C2b: explicit bounded run prefetch --------------------------
//
// The C2a fadvise probe refuted page-cache-mediated readahead under cgroup
// pressure (advance 23.9 -> 31.6 s @100M): pages fetched ahead of use get
// reclaimed before consumption. This is the consume-on-arrival shape
// instead: N pool threads read 512 KB chunks per run into a bounded
// (capacity-2) channel; the pump consumes chunks as a byte stream. Bytes
// are identical by construction (same files, same order); only WHO issues
// the read() changes. Peak buffered memory ~= runs x 2.5 chunks (~360 MB
// at 290 runs), replacing the per-run 1 MB BufReader.

/// Prefetched chunk size. (SIMVFS-SHARED: the C2b feeder machinery is now
/// sim-REACHABLE — feeders adopt the pump's shared SimVfs universe; the old
/// compile fence became `open_prefetch`'s runtime shared-universe branch.)
const PRE_CHUNK: usize = 512 << 10;

/// loadcommit RUNLZ4: pump-side decode source (no prefetch) — frames are
/// read + decompressed on demand, one bounded chunk resident per reader.
struct Lz4Source {
    r: BufReader<RunFile>,
    raw: Vec<u8>,
    pos: usize,
    comp: Vec<u8>,
    eof: bool,
    disk: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl std::io::Read for Lz4Source {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.raw.len() {
            if self.eof {
                return Ok(0);
            }
            let mut d = 0u64;
            match read_lz4_frame(&mut self.r, &mut self.comp, &mut self.raw, &mut d) {
                Ok(true) => {
                    self.pos = 0;
                    self.disk.fetch_add(d, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(false) => {
                    self.eof = true;
                    return Ok(0);
                }
                Err(e) => {
                    self.eof = true;
                    return Err(e);
                }
            }
        }
        let n = buf.len().min(self.raw.len() - self.pos);
        buf[..n].copy_from_slice(&self.raw[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// copyfast lever 1: byte source over an in-memory run — decodes the same
/// lz4 frames `Lz4Source` reads from disk (shared `read_lz4_frame` decode,
/// each frame fed back through it as a byte slice), freeing each frame and
/// releasing its store reservation the moment it is consumed.
struct MemLz4Source {
    run: MemRun,
    raw: Vec<u8>,
    pos: usize,
    comp: Vec<u8>,
}

impl std::io::Read for MemLz4Source {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.raw.len() {
            let Some(frame) = self.run.frames.pop_front() else { return Ok(0) };
            let mut r: &[u8] = &frame;
            let mut d = 0u64;
            match read_lz4_frame(&mut r, &mut self.comp, &mut self.raw, &mut d)? {
                true => {
                    if !r.is_empty() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "in-memory run frame carries trailing bytes",
                        ));
                    }
                    self.pos = 0;
                }
                false => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "empty in-memory run frame",
                    ))
                }
            }
            self.run.bytes -= frame.len() as u64;
            if let Some(store) = &self.run.store {
                store.release(frame.len() as u64);
            }
        }
        let n = buf.len().min(self.raw.len() - self.pos);
        buf[..n].copy_from_slice(&self.raw[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Consumer side of one run's prefetch channel: a plain byte stream.
/// (Row 8, SIMVFS-SHARED: the raw `std::sync::mpsc` channel converted to
/// the sanctioned `pgsync::mailbox` — under the permit scheduler the pump's
/// recv is a hooked park, native arm is the same protocol class.)
struct PrefetchSource {
    rx: pgsync::MailboxReceiver<std::io::Result<Vec<u8>>>,
    cur: Vec<u8>,
    pos: usize,
    eof: bool,
}

impl std::io::Read for PrefetchSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.cur.len() {
            if self.eof {
                return Ok(0);
            }
            match self.rx.recv() {
                Some(Ok(chunk)) => {
                    self.cur = chunk;
                    self.pos = 0;
                }
                Some(Err(e)) => {
                    self.eof = true;
                    return Err(e);
                }
                None => {
                    // feeder dropped the sender = clean EOF
                    self.eof = true;
                    return Ok(0);
                }
            }
        }
        let n = buf.len().min(self.cur.len() - self.pos);
        buf[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// One run's feeder-side state (owned by a pool thread).
struct FeedRun {
    f: BufReader<RunFile>,
    tx: Option<pgsync::MailboxSender<std::io::Result<Vec<u8>>>>,
    /// Chunk read but not yet accepted by the (full) channel.
    pending: Option<std::io::Result<Vec<u8>>>,
    eof: bool,
    /// loadcommit RUNLZ4: decode frames in the feeder (the pump then
    /// consumes decompressed bytes exactly as in raw mode).
    lz4: bool,
    comp: Vec<u8>,
    disk: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

/// Pool of prefetch threads; joined (after `stop`) on drop.
///
/// permit-s5 row 16: pgsync::thread handles — the feeder spawns route
/// through the pgsync::thread wrapper (the spawn door for identity-less
/// utility threads; native arm = the std re-export, byte-identical), so the
/// feeders are door-registered and the drop-path join is a hooked Join
/// park. SIMVFS-SHARED discharged the old fence: feeders now ADOPT the
/// pump's shared SimVfs universe (captured parent-side in `open_prefetch`),
/// so a feeder-side `vfs::pread` reads the same simulated disk the pump
/// writes — one fd table per simulated process, the C thread semantics.
pub struct PrefetchPool {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    threads: Vec<pgsync::thread::JoinHandle<()>>,
}

impl Drop for PrefetchPool {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn prefetch_feed(mut runs: Vec<FeedRun>, stop: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    use std::io::Read;
    loop {
        let mut progress = false;
        let mut alive = false;
        for r in &mut runs {
            let Some(tx) = &r.tx else { continue };
            alive = true;
            // Refill the pending slot, then try to hand it over.
            loop {
                if r.pending.is_none() {
                    if r.eof {
                        r.tx = None; // dropping the sender = EOF downstream
                        progress = true;
                        break;
                    }
                    if r.lz4 {
                        let mut raw = Vec::new();
                        let mut d = 0u64;
                        match read_lz4_frame(&mut r.f, &mut r.comp, &mut raw, &mut d) {
                            Ok(true) => {
                                r.disk.fetch_add(d, std::sync::atomic::Ordering::Relaxed);
                                r.pending = Some(Ok(raw));
                            }
                            Ok(false) => {
                                r.eof = true;
                                continue; // clean boundary EOF: close on next pass
                            }
                            Err(e) => {
                                r.pending = Some(Err(e));
                                r.eof = true;
                            }
                        }
                    } else {
                        let mut chunk = vec![0u8; PRE_CHUNK];
                        let mut got = 0usize;
                        let mut err = None;
                        while got < chunk.len() {
                            match r.f.read(&mut chunk[got..]) {
                                Ok(0) => break,
                                Ok(n) => got += n,
                                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                                Err(e) => {
                                    err = Some(e);
                                    break;
                                }
                            }
                        }
                        if let Some(e) = err {
                            r.pending = Some(Err(e));
                            r.eof = true;
                        } else {
                            if got < chunk.len() {
                                r.eof = true;
                            }
                            if got == 0 {
                                continue; // clean boundary EOF: close on next pass
                            }
                            chunk.truncate(got);
                            r.pending = Some(Ok(chunk));
                        }
                    }
                }
                let p = r.pending.take().unwrap();
                let was_err = p.is_err();
                match tx.try_send(p) {
                    Ok(()) => {
                        progress = true;
                        if was_err {
                            r.tx = None;
                            break;
                        }
                    }
                    Err(pgsync::TrySend::Full(p)) => {
                        r.pending = Some(p);
                        break;
                    }
                    Err(pgsync::TrySend::Disconnected(_)) => {
                        r.tx = None;
                        break;
                    }
                }
            }
        }
        if !alive || stop.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        if !progress {
            // Row 8 (SIMVFS-SHARED): pgsync sleep — a TimedPark on virtual
            // time under the permit scheduler (a raw std sleep would hold
            // the permit across the poll and starve the pump); the native
            // arm is the std re-export, byte-identical.
            pgsync::thread::sleep(std::time::Duration::from_micros(300));
        }
    }
}

/// Byte source for a run: direct buffered file reads (default) or the
/// C2b prefetch channel.
enum RunSrc {
    Buf(BufReader<RunFile>),
    Lz4(Lz4Source),
    Pre(PrefetchSource),
    Mem(MemLz4Source),
}

impl std::io::Read for RunSrc {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            RunSrc::Buf(r) => r.read(buf),
            RunSrc::Lz4(z) => z.read(buf),
            RunSrc::Pre(p) => p.read(buf),
            RunSrc::Mem(m) => m.read(buf),
        }
    }
}

/// copyfast lever 1: where a sorted run lives. The merge consumes the
/// identical entry stream from either home.
pub enum RunInput {
    File(std::path::PathBuf),
    Mem(MemRun),
}

struct RunReader {
    r: RunSrc,
    key_w: usize,
    /// Current entry (valid when `live`).
    key: Vec<u8>,
    row: Vec<u8>,
    live: bool,
    /// loadcommit C2a (PGRUST_PARALLEL_COPY_FILL_FADV): consumed bytes,
    /// readahead window (0 = off), the consumption mark that re-arms the
    /// advise, and the high-water offset already advised.
    pos: u64,
    fadv_win: u64,
    fadv_mark: u64,
    advised_to: u64,
}

impl RunReader {
    fn open(path: &std::path::Path, key_w: usize) -> PgResult<RunReader> {
        let f = vfs_open_file(path, libc::O_RDONLY, "run open")?;
        Self::from_src(RunSrc::Buf(BufReader::with_capacity(1 << 20, f)), key_w)
    }

    fn from_src(src: RunSrc, key_w: usize) -> PgResult<RunReader> {
        let mut rr = RunReader {
            r: src,
            key_w,
            key: vec![0; key_w],
            row: Vec::new(),
            live: false,
            pos: 0,
            fadv_win: 0,
            fadv_mark: 0,
            advised_to: 0,
        };
        rr.advance()?;
        Ok(rr)
    }

    /// loadcommit C2a: arm the sliding readahead window and advise the
    /// first stretch immediately (the open() above already consumed the
    /// first entry, so `pos` is live). No-op on a prefetch source.
    fn set_fadvise(&mut self, win: u64) {
        let RunSrc::Buf(r) = &self.r else { return };
        if win == 0 {
            return;
        }
        self.fadv_win = win;
        fadvise_willneed(r.get_ref(), self.pos, win);
        self.advised_to = self.pos + win;
        self.fadv_mark = self.pos + (1 << 20);
    }

    /// Slide the advised window ahead of consumption; re-armed every ~1 MB
    /// consumed (one branch per row otherwise).
    #[inline]
    fn fadv_tick(&mut self) {
        if self.fadv_win == 0 || self.pos < self.fadv_mark {
            return;
        }
        let RunSrc::Buf(r) = &self.r else { return };
        let end = self.pos + self.fadv_win;
        if end > self.advised_to {
            fadvise_willneed(r.get_ref(), self.advised_to, end - self.advised_to);
            self.advised_to = end;
        }
        self.fadv_mark = self.pos + (1 << 20);
    }

    fn advance(&mut self) -> PgResult<()> {
        // key (EOF here = clean end of run)
        let mut got = 0usize;
        while got < self.key_w {
            let n = self.r.read(&mut self.key[got..]).map_err(|e| io_err("run read", e))?;
            if n == 0 {
                if got == 0 {
                    self.live = false;
                    return Ok(());
                }
                return Err(io_err("run read", std::io::ErrorKind::UnexpectedEof.into()));
            }
            got += n;
        }
        let mut lenb = [0u8; 4];
        self.r.read_exact(&mut lenb).map_err(|e| io_err("run read", e))?;
        let rowlen = u32::from_le_bytes(lenb) as usize;
        self.row.resize(rowlen, 0);
        self.r.read_exact(&mut self.row).map_err(|e| io_err("run read", e))?;
        self.live = true;
        self.pos += (self.key_w + 4 + rowlen) as u64;
        self.fadv_tick();
        Ok(())
    }
}

/// Heap entry: min-heap by (key, run index) — the run index breaks key ties.
///
/// GL-LOADDET-1: this comment used to add "(none exist on the benchmark key
/// set)". They do exist, and the run index only makes the merge deterministic
/// if the run ORDER is itself deterministic — which it was not, because the
/// registry was appended to in worker-completion order. The caller
/// (`merge_sorted_runs`) now orders the inputs by input coordinate, so this
/// tiebreak is input-major; see `PGRUST_PARALLEL_COPY_SORT_DETERMINISTIC`.
struct HeapEntry {
    key: Vec<u8>,
    run: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.run == other.run
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed: BinaryHeap is a max-heap; we want the smallest key out.
        (other.key.as_slice(), other.run).cmp(&(self.key.as_slice(), self.run))
    }
}

/// Fill-phase decomposition accumulators (loadcommit C0). `t_advance`
/// (run read+decode inside the merge) is only accumulated when `timed`
/// (PGRUST_PARALLEL_COPY_FILL_SPLIT=1 via the copy driver) — two clock
/// reads per row that are otherwise a single untaken branch. `bytes`
/// (run bytes consumed: key + len prefix + row) is one add per row,
/// always accumulated.
#[derive(Default)]
struct FillStats {
    timed: bool,
    t_advance: std::time::Duration,
    bytes: u64,
}

impl FillStats {
    #[inline]
    fn advance(&mut self, rr: &mut RunReader) -> PgResult<()> {
        if self.timed {
            let t0 = std::time::Instant::now();
            let r = rr.advance();
            self.t_advance += t0.elapsed();
            r
        } else {
            rr.advance()
        }
    }
}

pub struct RunMerge {
    readers: Vec<RunReader>,
    heap: BinaryHeap<HeapEntry>,
    stats: FillStats,
}

impl RunMerge {
    pub fn open(paths: &[std::path::PathBuf], key_w: usize) -> PgResult<RunMerge> {
        let mut readers = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::with_capacity(paths.len());
        for (i, p) in paths.iter().enumerate() {
            let rr = RunReader::open(p, key_w)?;
            if rr.live {
                heap.push(HeapEntry { key: rr.key.clone(), run: i });
            }
            readers.push(rr);
        }
        Ok(RunMerge { readers, heap, stats: FillStats::default() })
    }

    /// loadcommit C0: arm the per-row advance timer (default off).
    pub fn set_timed(&mut self, on: bool) {
        self.stats.timed = on;
    }

    /// loadcommit C2a: arm per-run sliding kernel readahead (0 = off).
    pub fn set_fadvise(&mut self, win: u64) {
        for rr in &mut self.readers {
            rr.set_fadvise(win);
        }
    }

    /// (advance seconds — 0.0 unless timed, logical run bytes consumed,
    /// compressed disk bytes — always 0 for the raw-only heap merge).
    pub fn fill_stats(&self) -> (f64, u64, u64) {
        (self.stats.t_advance.as_secs_f64(), self.stats.bytes, 0)
    }

    /// Copy the next entry (global key order) into the caller's buffers.
    /// Returns false at end of merge.
    pub fn next_entry(&mut self, key: &mut Vec<u8>, row: &mut Vec<u8>) -> PgResult<bool> {
        let Some(top) = self.heap.pop() else { return Ok(false) };
        let rr = &mut self.readers[top.run];
        key.clear();
        key.extend_from_slice(&rr.key);
        row.clear();
        row.extend_from_slice(&rr.row);
        self.stats.bytes += (rr.key_w + 4 + rr.row.len()) as u64;
        self.stats.advance(rr)?;
        let rr = &self.readers[top.run];
        if rr.live {
            self.heap.push(HeapEntry { key: rr.key.clone(), run: top.run });
        }
        Ok(true)
    }
}

// ---- loser-tree k-way merge (loadcommit C1, opt-in fill V2) -----------------

/// Merge order shared by `RunMerge` (heap) and `RunMergeV2` (loser tree):
/// live runs ascending by (key bytes, run index); exhausted runs rank
/// last (mutually ordered by run index — deterministic, never emitted).
/// Identical total order => identical emitted row sequence => the V2 fill
/// is byte-identity-safe by construction (oracle:
/// `v2_matches_heap_reference`).
#[inline]
fn v2_less(readers: &[RunReader], a: u32, b: u32) -> bool {
    let ra = &readers[a as usize];
    let rb = &readers[b as usize];
    match (ra.live, rb.live) {
        (true, false) => true,
        (false, true) => false,
        (false, false) => a < b,
        (true, true) => (ra.key.as_slice(), a) < (rb.key.as_slice(), b),
    }
}

/// loadcommit C1 (PGRUST_PARALLEL_COPY_FILL_V2=1, default OFF): loser-tree
/// k-way merge replacing the BinaryHeap fill. Differences from `RunMerge`,
/// none of them order-affecting:
///   - zero per-row allocation (the heap clones the key `Vec` per row);
///   - ~log2(k) key comparisons per row (heap: ~2*log2(k)), each against
///     the reader-resident key (no copies);
///   - rows are appended straight into the caller's batch arena (the heap
///     path copies key+row into pump-local buffers first).
pub struct RunMergeV2 {
    /// Declared before `_pool`: dropping the readers first closes the
    /// prefetch receivers, so pool threads see Disconnected and exit.
    readers: Vec<RunReader>,
    /// Tournament tree over k = readers.len() leaves: internal node x in
    /// 1..k holds the LOSER run index of the match played there; node x's
    /// children are 2x and 2x+1; run i's leaf is node k+i. `winner` is the
    /// current overall winner (u32::MAX when k == 0).
    losers: Vec<u32>,
    winner: u32,
    stats: FillStats,
    /// loadcommit RUNLZ4: compressed bytes actually read from disk
    /// (fill_stats third field; 0 in raw mode).
    disk: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// loadcommit C2b: keeps the prefetch threads alive; joined on drop.
    _pool: Option<PrefetchPool>,
}

impl RunMergeV2 {
    pub fn open(paths: &[std::path::PathBuf], key_w: usize) -> PgResult<RunMergeV2> {
        Self::open_opts(paths, key_w, false)
    }

    /// Direct (pump-thread) sources; `lz4` = RUNLZ4-framed run files.
    pub fn open_opts(
        paths: &[std::path::PathBuf],
        key_w: usize,
        lz4: bool,
    ) -> PgResult<RunMergeV2> {
        Self::open_mixed(paths.iter().cloned().map(RunInput::File).collect(), key_w, 0, lz4)
    }

    /// loadcommit C2b (PGRUST_PARALLEL_COPY_FILL_PREFETCH=<n>): prefetch-fed
    /// merge — `threads` pool threads stream chunks per run through bounded
    /// channels (decoding RUNLZ4 frames feeder-side when `lz4`); byte
    /// stream identical to the direct open by construction (oracle extends
    /// v2_matches_heap_reference).
    pub fn open_prefetch(
        paths: &[std::path::PathBuf],
        key_w: usize,
        threads: usize,
        lz4: bool,
    ) -> PgResult<RunMergeV2> {
        Self::open_mixed(
            paths.iter().cloned().map(RunInput::File).collect(),
            key_w,
            threads.clamp(1, 16),
            lz4,
        )
    }

    /// copyfast lever 1: mixed-home merge. In-memory runs decode inline
    /// with per-frame free; file runs keep the direct/lz4/prefetch paths
    /// (`lz4` describes FILE runs only — memory runs are self-describing).
    /// Reader order = `inputs` order (the merge tiebreak index) = the spill
    /// sites' registration order, exactly as the all-file constructors.
    pub fn open_mixed(
        inputs: Vec<RunInput>,
        key_w: usize,
        prefetch_threads: usize,
        lz4: bool,
    ) -> PgResult<RunMergeV2> {
        // Sim arm (SIMVFS-SHARED — the old compile fence, now a RUNTIME
        // branch): prefetch under sim requires the pump to be bound to a
        // SHARED SimVfs universe (feeders adopt it below; the fd table and
        // the run bytes are then process-shared, C thread semantics). An
        // unbound pump (scheduler-off sim runs, unit batteries) degrades to
        // the direct pump-thread source exactly as before — the byte stream
        // is identical by the C2b construction (only WHO issues the read
        // changes; `v2_matches_heap_reference` is the standing oracle).
        #[cfg(pgrust_sim)]
        let prefetch_threads = if vfs::sim::SimVfs::shared_universe_active() {
            prefetch_threads
        } else {
            0
        };
        // Parent-side universe capture (the spawn-door inheritance shape:
        // process identity flows down the spawn tree like an inherited fd
        // table); each feeder adopts it as its first act below.
        #[cfg(pgrust_sim)]
        let sim_universe = vfs::sim::SimVfs::current_universe_id();
        let threads = prefetch_threads.min(16);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let disk = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut buckets: Vec<Vec<FeedRun>> = (0..threads).map(|_| Vec::new()).collect();
        // Per-input source, realized into readers IN INPUT ORDER below
        // (prefetch feeders must all be spawned before the first blocking
        // recv a reader's first advance() performs).
        let mut pend: Vec<RunSrc> = Vec::with_capacity(inputs.len());
        let mut nfile = 0usize;
        for input in inputs {
            match input {
                RunInput::Mem(run) => pend.push(RunSrc::Mem(MemLz4Source {
                    run,
                    raw: Vec::new(),
                    pos: 0,
                    comp: Vec::new(),
                })),
                RunInput::File(p) if threads > 0 => {
                    let f = vfs_open_file(&p, libc::O_RDONLY, "run open")?;
                    let mut f =
                        BufReader::with_capacity(if lz4 { 64 << 10 } else { 8 << 10 }, f);
                    if lz4 {
                        check_run_magic(&mut f).map_err(|e| io_err("run magic", e))?;
                    }
                    // Row 8 (SIMVFS-SHARED): pgsync mailbox, sync_channel(2)
                    // semantics — hooked parks under the permit scheduler.
                    let (tx, rx) = pgsync::mailbox::<std::io::Result<Vec<u8>>>(Some(2));
                    buckets[nfile % threads].push(FeedRun {
                        f,
                        tx: Some(tx),
                        pending: None,
                        eof: false,
                        lz4,
                        comp: Vec::new(),
                        disk: std::sync::Arc::clone(&disk),
                    });
                    pend.push(RunSrc::Pre(PrefetchSource {
                        rx,
                        cur: Vec::new(),
                        pos: 0,
                        eof: false,
                    }));
                    nfile += 1;
                }
                RunInput::File(p) if lz4 => {
                    let f = vfs_open_file(&p, libc::O_RDONLY, "run open")?;
                    let mut r = BufReader::with_capacity(1 << 20, f);
                    check_run_magic(&mut r).map_err(|e| io_err("run magic", e))?;
                    pend.push(RunSrc::Lz4(Lz4Source {
                        r,
                        raw: Vec::new(),
                        pos: 0,
                        comp: Vec::new(),
                        eof: false,
                        disk: std::sync::Arc::clone(&disk),
                    }));
                }
                RunInput::File(p) => {
                    let f = vfs_open_file(&p, libc::O_RDONLY, "run open")?;
                    pend.push(RunSrc::Buf(BufReader::with_capacity(1 << 20, f)));
                }
            }
        }
        let mut handles = Vec::with_capacity(threads);
        for runs in buckets {
            if runs.is_empty() {
                continue;
            }
            let stop2 = std::sync::Arc::clone(&stop);
            // permit-s5 row 16: door-routed via the pgsync::thread wrapper.
            let h = pgsync::thread::Builder::new()
                .name("cb-run-prefetch".into())
                .spawn(move || {
                    // SIMVFS-SHARED: adopt the pump's universe FIRST — every
                    // later vfs op on this thread reads the process's disk.
                    #[cfg(pgrust_sim)]
                    if let Some(id) = sim_universe {
                        vfs::sim::SimVfs::adopt_universe(id);
                    }
                    prefetch_feed(runs, stop2)
                })
                .map_err(|e| io_err("prefetch spawn", e))?;
            handles.push(h);
        }
        let pool = if handles.is_empty() {
            None
        } else {
            Some(PrefetchPool { stop, threads: handles })
        };
        let mut readers = Vec::with_capacity(pend.len());
        for src in pend {
            readers.push(RunReader::from_src(src, key_w)?);
        }
        Self::from_readers(readers, pool, disk)
    }

    /// Is this merge actually prefetch-fed (feeder pool live)? Harness
    /// probe: the sim corpus asserts the prefetch path was structurally
    /// taken, not silently degraded.
    pub fn is_prefetching(&self) -> bool {
        self._pool.is_some()
    }

    fn from_readers(
        readers: Vec<RunReader>,
        pool: Option<PrefetchPool>,
        disk: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> PgResult<RunMergeV2> {
        let k = readers.len();
        let mut losers = vec![u32::MAX; k.max(1)];
        let winner = if k == 0 {
            u32::MAX
        } else {
            Self::build(&readers, &mut losers, k, 1)
        };
        Ok(RunMergeV2 {
            readers,
            losers,
            winner,
            stats: FillStats::default(),
            disk,
            _pool: pool,
        })
    }

    /// Play the tournament under node x, storing losers; returns the winner.
    fn build(readers: &[RunReader], losers: &mut [u32], k: usize, x: usize) -> u32 {
        if x >= k {
            return (x - k) as u32;
        }
        let a = Self::build(readers, losers, k, 2 * x);
        let b = Self::build(readers, losers, k, 2 * x + 1);
        if v2_less(readers, a, b) {
            losers[x] = b;
            a
        } else {
            losers[x] = a;
            b
        }
    }

    /// loadcommit C0: arm the per-row advance timer (default off).
    pub fn set_timed(&mut self, on: bool) {
        self.stats.timed = on;
    }

    /// loadcommit C2a: arm per-run sliding kernel readahead (0 = off).
    pub fn set_fadvise(&mut self, win: u64) {
        for rr in &mut self.readers {
            rr.set_fadvise(win);
        }
    }

    /// (advance seconds — 0.0 unless timed, logical run bytes consumed,
    /// compressed disk bytes read — 0 in raw mode).
    pub fn fill_stats(&self) -> (f64, u64, u64) {
        (
            self.stats.t_advance.as_secs_f64(),
            self.stats.bytes,
            self.disk.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Append the next row (global key order) to `arena`; returns its
    /// byte length, or None at end of merge.
    pub fn next_row_into(&mut self, arena: &mut Vec<u8>) -> PgResult<Option<u32>> {
        if self.winner == u32::MAX || !self.readers[self.winner as usize].live {
            return Ok(None);
        }
        let w = self.winner as usize;
        let k = self.readers.len();
        {
            let rr = &self.readers[w];
            arena.extend_from_slice(&rr.row);
            self.stats.bytes += (rr.key_w + 4 + rr.row.len()) as u64;
        }
        let len = self.readers[w].row.len() as u32;
        self.stats.advance(&mut self.readers[w])?;
        // Replay the path from run w's leaf to the root.
        let mut cur = self.winner;
        let mut node = (k + w) >> 1;
        while node >= 1 {
            if v2_less(&self.readers, self.losers[node], cur) {
                std::mem::swap(&mut self.losers[node], &mut cur);
            }
            node >>= 1;
        }
        self.winner = cur;
        Ok(Some(len))
    }
}

// ---------------------------------------------------------------------------
// SIMVFS-SHARED corpus body (permit-sched e2e P8): sim-only, no product
// surface. Proves the loadsort-prefetch unlock: run files written by the
// pump land in the SHARED SimVfs universe; door-registered feeder threads
// ADOPT that universe and read real bytes back through vfs; the prefetch-fed
// merge byte-matches the direct merge; and a bidirectional share-proof
// (child writes a file the pump reads back, and vice versa AFTER the child's
// adoption) catches both deliberately-broken sharing shapes
// (`SimVfs::arm_red_adoption`: Empty = the pre-lane bug, Stale = a frozen
// snapshot).
// ---------------------------------------------------------------------------

#[cfg(pgrust_sim)]
pub mod sim_demo {
    use super::*;

    fn cpath(p: &str) -> std::ffi::CString {
        std::ffi::CString::new(p).expect("no NUL in demo paths")
    }

    fn sim_mkdir_p(path: &str) {
        let mut acc = String::new();
        for comp in path.split('/').filter(|c| !c.is_empty()) {
            acc.push('/');
            acc.push_str(comp);
            let _ = vfs::mkdir(&cpath(&acc), 0o700);
        }
    }

    fn sim_write(path: &str, bytes: &[u8]) -> Result<(), String> {
        let c = cpath(path);
        let fd = vfs::open(&c, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY, 0o600);
        if fd < 0 {
            return Err(format!("write open {path}: errno {}", vfs::get_errno()));
        }
        let mut off = 0usize;
        while off < bytes.len() {
            let n = vfs::pwrite(fd, &bytes[off..], off as libc::off_t);
            if n <= 0 {
                vfs::close(fd);
                return Err(format!("pwrite {path}: errno {}", vfs::get_errno()));
            }
            off += n as usize;
        }
        vfs::close(fd);
        Ok(())
    }

    fn sim_read(path: &str) -> Result<Vec<u8>, String> {
        let c = cpath(path);
        let fd = vfs::open(&c, libc::O_RDONLY, 0);
        if fd < 0 {
            return Err(format!("read open {path}: errno {}", vfs::get_errno()));
        }
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = vfs::pread(fd, &mut buf, out.len() as libc::off_t);
            if n < 0 {
                vfs::close(fd);
                return Err(format!("pread {path}: errno {}", vfs::get_errno()));
            }
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n as usize]);
        }
        vfs::close(fd);
        Ok(out)
    }

    /// The P8 corpus. Returns the grep-stable PASS line, or `Err` with the
    /// property that failed (the red arms assert on the failure text).
    pub fn run_prefetch_corpus() -> Result<String, String> {
        if !vfs::sim::SimVfs::shared_universe_active() {
            return Err("pump thread is not bound to a shared universe".into());
        }
        let uni = vfs::sim::SimVfs::current_universe_id()
            .ok_or_else(|| "bound thread has no universe id".to_string())?;

        // --- 1. spill 3 runs into the SHARED universe (LCG rows, seeded:
        //        the printed line is byte-stable across same-seed replays).
        const KW: usize = 4;
        const RUNS: usize = 3;
        const ROWS_PER_RUN: usize = 200;
        sim_mkdir_p("/loadsort-demo");
        let mut paths = Vec::new();
        let mut lcg: u64 = 0x5EED_1234_ABCD_0001;
        let mut total_rows = 0usize;
        for r in 0..RUNS {
            let mut batch = SortBatch::new(KW);
            for _ in 0..ROWS_PER_RUN {
                lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let key = ((lcg >> 32) as u32).to_be_bytes();
                let rowlen = 8 + (lcg % 48) as usize;
                let mut row = Vec::with_capacity(rowlen);
                while row.len() < rowlen {
                    row.push((lcg >> (row.len() % 8)) as u8);
                }
                batch.push(&key, &row);
                total_rows += 1;
            }
            batch.sort();
            let p = std::path::PathBuf::from(format!("/loadsort-demo/run-{r}"));
            batch.spill_run(&p).map_err(|e| format!("spill run-{r}: {}", e.message()))?;
            paths.push(p);
        }

        // --- 2. direct merge = the reference stream.
        let mut direct = RunMergeV2::open(&paths, KW)
            .map_err(|e| format!("direct open: {}", e.message()))?;
        let mut ref_arena = Vec::new();
        let mut ref_lens = Vec::new();
        while let Some(l) = direct
            .next_row_into(&mut ref_arena)
            .map_err(|e| format!("direct merge: {}", e.message()))?
        {
            ref_lens.push(l);
        }

        // --- 3. prefetch-fed merge: 2 door-registered feeders ADOPT the
        //        pump's universe and read the run bytes through vfs.
        let mut pre = RunMergeV2::open_prefetch(&paths, KW, 2, false)
            .map_err(|e| format!("prefetch open: {}", e.message()))?;
        if !pre.is_prefetching() {
            return Err("prefetch path silently degraded to the direct source".into());
        }
        let mut arena = Vec::new();
        let mut lens = Vec::new();
        while let Some(l) = pre
            .next_row_into(&mut arena)
            .map_err(|e| format!("prefetch merge: {}", e.message()))?
        {
            lens.push(l);
        }
        if lens != ref_lens || arena != ref_arena {
            return Err(format!(
                "prefetch stream diverges from direct: {}x{} vs {}x{} bytes",
                lens.len(),
                arena.len(),
                ref_lens.len(),
                ref_arena.len()
            ));
        }
        drop(pre); // joins the feeder pool (hooked Join parks)

        // --- 4. bidirectional share-proof: the child's post-adoption view
        //        must be LIVE, not a snapshot. The go-signal orders the
        //        pump's write strictly after the child's adoption, so a
        //        Stale adoption cannot contain /main-proof; and the pump
        //        reading /child-proof catches both Stale and Empty (the
        //        child wrote into a private copy).
        let (go_tx, go_rx) = pgsync::mailbox::<()>(Some(1));
        let helper = pgsync::thread::Builder::new()
            .name("simvfs-share-proof".into())
            .spawn(move || -> Result<(), String> {
                vfs::sim::SimVfs::adopt_universe(uni);
                if go_rx.recv().is_none() {
                    return Err("go channel closed early".into());
                }
                let got = sim_read("/loadsort-demo/main-proof")
                    .map_err(|e| format!("child cannot see the pump's post-adoption write ({e})"))?;
                if got != b"written-by-pump" {
                    return Err("child read stale bytes for /main-proof".into());
                }
                sim_write("/loadsort-demo/child-proof", b"written-by-child")
            })
            .map_err(|e| format!("share-proof spawn: {e}"))?;
        sim_write("/loadsort-demo/main-proof", b"written-by-pump")?;
        let _ = go_tx.send(());
        let child_verdict = helper.join().map_err(|_| "share-proof helper panicked".to_string())?;
        child_verdict.map_err(|e| format!("share-proof(child): {e}"))?;
        let got = sim_read("/loadsort-demo/child-proof")
            .map_err(|e| format!("share-proof(pump): child's write invisible ({e})"))?;
        if got != b"written-by-child" {
            return Err("share-proof(pump): wrong bytes in /child-proof".into());
        }

        Ok(format!(
            "LOADSORT runs={RUNS} threads=2 rows={total_rows} bytes={} prefetch=1 identical=1 shareproof=ok",
            ref_arena.len()
        ))
    }
}

// ---- tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sortkey::{encode_sort_key, fixed_key_width};
    use ::tuplesort_seams::CbSortKeyKind;

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("cb-loadsort-{}-{}", std::process::id(), name));
        #[cfg(not(pgrust_sim))]
        {
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
        }
        #[cfg(pgrust_sim)]
        {
            // The run-file data plane lands in the thread's SimVfs universe
            // (fresh per test thread, no cwd, empty namespace): create the
            // directory chain through the same provider.
            let mut cur = std::path::PathBuf::new();
            for comp in d.components() {
                cur.push(comp);
                if cur.as_os_str().len() > 1 {
                    let rc = vfs::mkdir(&cpath(&cur), 0o700);
                    assert!(
                        rc == 0 || vfs::get_errno() == libc::EEXIST,
                        "vfs mkdir {cur:?} failed: errno {}",
                        vfs::get_errno()
                    );
                }
            }
        }
        d
    }

    fn cpath(p: &std::path::Path) -> std::ffi::CString {
        use std::os::unix::ffi::OsStrExt;
        std::ffi::CString::new(p.as_os_str().as_bytes()).unwrap()
    }

    // Cfg-split file helpers: run files live on the real filesystem off-sim
    // and in the thread's SimVfs universe under --cfg pgrust_sim — the tests
    // must look where the data plane wrote.
    fn file_len(p: &std::path::Path) -> u64 {
        #[cfg(not(pgrust_sim))]
        {
            std::fs::metadata(p).unwrap().len()
        }
        #[cfg(pgrust_sim)]
        {
            let mut fi = vfs::FileInfo::zeroed();
            assert_eq!(vfs::stat(&cpath(p), &mut fi), 0, "vfs stat {p:?}");
            fi.size as u64
        }
    }

    fn read_file(p: &std::path::Path) -> Vec<u8> {
        #[cfg(not(pgrust_sim))]
        {
            std::fs::read(p).unwrap()
        }
        #[cfg(pgrust_sim)]
        {
            let fd = vfs::open(&cpath(p), libc::O_RDONLY, 0);
            assert!(fd >= 0, "vfs open {p:?}: errno {}", vfs::get_errno());
            let mut out = vec![0u8; file_len(p) as usize];
            let mut got = 0usize;
            while got < out.len() {
                let n = vfs::pread(fd, &mut out[got..], got as vfs::off_t);
                assert!(n > 0, "vfs pread {p:?}: errno {}", vfs::get_errno());
                got += n as usize;
            }
            vfs::close(fd);
            out
        }
    }

    fn write_file(p: &std::path::Path, bytes: &[u8]) {
        #[cfg(not(pgrust_sim))]
        {
            std::fs::write(p, bytes).unwrap();
        }
        #[cfg(pgrust_sim)]
        {
            let fd =
                vfs::open(&cpath(p), libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o666);
            assert!(fd >= 0, "vfs open {p:?}: errno {}", vfs::get_errno());
            let mut off = 0usize;
            while off < bytes.len() {
                let n = vfs::pwrite(fd, &bytes[off..], off as vfs::off_t);
                assert!(n > 0, "vfs pwrite {p:?}: errno {}", vfs::get_errno());
                off += n as usize;
            }
            vfs::close(fd);
        }
    }

    /// DST P4 dataplane arm (site: loadsort run files). The spill data plane
    /// must land in the SAME provider that minted the fd: write a run
    /// through `SortBatch::spill_run` under sim, prove the bytes live in
    /// SimVfs (read back through `vfs::*`) and never touched the real
    /// filesystem, then consume them back through the merge read arm.
    /// RED at base: `vfs::open` minted a sim fd but the data plane was
    /// `std::fs::File` — kernel write(2)/close(2) on an fd >= SIM_FD_BASE.
    #[cfg(pgrust_sim)]
    #[test]
    fn sim_spill_data_plane_lands_in_simvfs() {
        let dir = tmpdir("simarm");
        let codec = RowCodec::new(vec![ColType::I32, ColType::I64, ColType::Text]);
        let keys = [(0u16, CbSortKeyKind::Int32), (1, CbSortKeyKind::Int64)];
        let kw = fixed_key_width(&keys).unwrap();
        let mut keep = Vec::new();
        let mut batch = SortBatch::new(kw);
        let rows: [(i32, i64, &[u8]); 2] = [(7, 1, b"alpha"), (3, 2, b"beta")];
        for r in &rows {
            let vals = [Datum::from_i32(r.0), Datum::from_i64(r.1), text_datum(r.2, &mut keep)];
            let mut key = Vec::with_capacity(kw);
            encode_sort_key(&keys, &vals, &mut key);
            let mut img = Vec::new();
            codec.serialize_row(&vals, &mut img).unwrap();
            batch.push(&key, &img);
        }
        batch.sort();
        let p = dir.join("run-0");
        batch.spill_run(&p).unwrap();

        // (a) The bytes are in the sim universe, readable through vfs, and
        // structurally a run: entries of [key: kw][rowlen: u32 le][row].
        let bytes = read_file(&p);
        assert!(bytes.len() > kw + 4);
        let rowlen = u32::from_le_bytes(bytes[kw..kw + 4].try_into().unwrap()) as usize;
        assert!(kw + 4 + rowlen <= bytes.len(), "corrupt first run entry");
        // (b) The real filesystem never saw the file.
        assert!(std::fs::metadata(&p).is_err(), "run file leaked to the real fs");
        // (c) The read arm consumes it back through the same provider, in
        // key order (the (3,2) row sorts first).
        let mut m = RunMerge::open(&[p], kw).unwrap();
        let (mut key, mut row) = (Vec::new(), Vec::new());
        let mut arena = Vec::new();
        let mut vals = vec![Datum::null(); 3];
        let mut got = Vec::new();
        while m.next_entry(&mut key, &mut row).unwrap() {
            arena.clear();
            codec.deserialize_row(&row, &mut arena, &mut vals).unwrap();
            got.push((
                vals[0].as_i32(),
                vals[1].as_i64(),
                varlena_bytes(vals[2]).unwrap().to_vec(),
            ));
        }
        assert_eq!(got, vec![(3, 2, b"beta".to_vec()), (7, 1, b"alpha".to_vec())]);
    }

    #[test]
    fn codec_roundtrip_all_coltypes() {
        let codec = RowCodec::new(vec![
            ColType::I16,
            ColType::I32,
            ColType::Date,
            ColType::I64,
            ColType::Timestamp,
            ColType::Text,
            ColType::Text,
        ]);
        let mut keep = Vec::new();
        let vals = [
            Datum::from_i16(-7),
            Datum::from_i32(i32::MIN),
            Datum::from_i32(8036),
            Datum::from_i64(-2461439046089301801),
            Datum::from_i64(i64::MAX),
            text_datum(b"", &mut keep),
            text_datum("URL with \tescapes and \u{00fc}nicode".as_bytes(), &mut keep),
        ];
        let mut img = Vec::new();
        codec.serialize_row(&vals, &mut img).unwrap();
        let mut arena = Vec::new();
        let mut out = vec![Datum::null(); 7];
        codec.deserialize_row(&img, &mut arena, &mut out).unwrap();
        assert_eq!(out[0].as_i16(), -7);
        assert_eq!(out[1].as_i32(), i32::MIN);
        assert_eq!(out[2].as_i32(), 8036);
        assert_eq!(out[3].as_i64(), -2461439046089301801);
        assert_eq!(out[4].as_i64(), i64::MAX);
        assert_eq!(varlena_bytes(out[5]).unwrap(), b"");
        assert_eq!(
            varlena_bytes(out[6]).unwrap(),
            "URL with \tescapes and \u{00fc}nicode".as_bytes()
        );
        // Re-serializing the rebuilt datums reproduces the image byte-exactly.
        let mut img2 = Vec::new();
        codec.serialize_row(&out, &mut img2).unwrap();
        assert_eq!(img, img2);
    }

    // The unit-scale mirror of the fleet merge oracle: rows chunked across
    // "workers", batch-sorted, spilled as runs, k-way merged — the merged
    // sequence equals the globally key-sorted sequence, rows byte-exact.
    #[test]
    fn chunked_runs_merge_to_global_sort() {
        let dir = tmpdir("merge");
        // hits-shaped mini schema: (counterid i32, userid i64, url text)
        let codec = RowCodec::new(vec![ColType::I32, ColType::I64, ColType::Text]);
        let keys =
            [(0u16, CbSortKeyKind::Int32), (1, CbSortKeyKind::Int64)];
        let kw = fixed_key_width(&keys).unwrap();

        let mut x: u64 = 42;
        let mut step = || {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x
        };
        let n = 10_000;
        let mut keep = Vec::new();
        let rows: Vec<(i32, i64, Vec<u8>)> = (0..n)
            .map(|i| {
                (
                    (step() as i32) % 500,
                    step() as i64,
                    format!("http://example/{}/{}", step() % 97, i).into_bytes(),
                )
            })
            .collect();

        // 4 "workers" x batches of 800 rows -> sorted runs on disk.
        let mut paths = Vec::new();
        for (w, chunk) in rows.chunks(n / 4).enumerate() {
            let mut batch = SortBatch::new(kw);
            let mut ri = 0;
            for r in chunk {
                let vals =
                    [Datum::from_i32(r.0), Datum::from_i64(r.1), text_datum(&r.2, &mut keep)];
                let mut key = Vec::with_capacity(kw);
                encode_sort_key(&keys, &vals, &mut key);
                let mut img = Vec::new();
                codec.serialize_row(&vals, &mut img).unwrap();
                batch.push(&key, &img);
                if batch.rows() == 800 {
                    batch.sort();
                    let p = dir.join(format!("run-{w}-{ri}"));
                    batch.spill_run(&p).unwrap();
                    paths.push(p);
                    ri += 1;
                }
            }
            if !batch.is_empty() {
                batch.sort();
                let p = dir.join(format!("run-{w}-{ri}"));
                batch.spill_run(&p).unwrap();
                paths.push(p);
            }
        }
        assert!(paths.len() > 8, "expected multiple runs, got {}", paths.len());

        // Merge and decode.
        let mut merge = RunMerge::open(&paths, kw).unwrap();
        let (mut key, mut row) = (Vec::new(), Vec::new());
        let mut got: Vec<(i32, i64, Vec<u8>)> = Vec::new();
        let mut prev_key: Option<Vec<u8>> = None;
        let mut arena = Vec::new();
        let mut vals = vec![Datum::null(); 3];
        while merge.next_entry(&mut key, &mut row).unwrap() {
            if let Some(pk) = &prev_key {
                assert!(pk.as_slice() <= key.as_slice(), "merge emitted keys out of order");
            }
            prev_key = Some(key.clone());
            arena.clear();
            codec.deserialize_row(&row, &mut arena, &mut vals).unwrap();
            got.push((
                vals[0].as_i32(),
                vals[1].as_i64(),
                varlena_bytes(vals[2]).unwrap().to_vec(),
            ));
        }
        assert_eq!(got.len(), n);

        let mut want = rows.clone();
        // Global order: (counterid, userid); ties impossible (userid is a
        // 64-bit LCG draw) — assert anyway via full-tuple compare stability.
        want.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
            assert_eq!((g.0, g.1), (w.0, w.1), "key order diverged at row {i}");
            assert_eq!(g.2, w.2, "row payload diverged at row {i}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- loadcommit C1: the V2 fill oracle ---------------------------------

    /// Spill `runs` sorted runs of `(i32, i64, text)` rows; `dup_keys`
    /// collapses the key domain so exact key duplicates appear ACROSS runs
    /// (the run-index tiebreak leg). Returns (paths, key_w).
    fn spill_random_runs(
        dir: &std::path::Path,
        runs: usize,
        rows_per_run: usize,
        dup_keys: bool,
        seed: u64,
    ) -> (Vec<std::path::PathBuf>, usize) {
        spill_random_runs_opts(dir, runs, rows_per_run, dup_keys, seed, false)
    }

    /// Deterministic sorted batches (same LCG stream for a given seed) —
    /// the shared row source for file runs AND in-memory runs, so the two
    /// homes are provably fed identical entries.
    fn random_sorted_batches(
        runs: usize,
        rows_per_run: usize,
        dup_keys: bool,
        seed: u64,
    ) -> (Vec<SortBatch>, usize) {
        let codec = RowCodec::new(vec![ColType::I32, ColType::I64, ColType::Text]);
        let keys = [(0u16, CbSortKeyKind::Int32), (1, CbSortKeyKind::Int64)];
        let kw = fixed_key_width(&keys).unwrap();
        let mut x: u64 = seed;
        let mut step = || {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x
        };
        let mut keep = Vec::new();
        let mut batches = Vec::new();
        for r in 0..runs {
            let mut batch = SortBatch::new(kw);
            for i in 0..rows_per_run {
                let (a, b) = if dup_keys {
                    // 5x3 key domain: every key occurs in essentially every
                    // run; payload still distinguishes provenance.
                    ((step() % 5) as i32, (step() % 3) as i64)
                } else {
                    ((step() as i32) % 1000, step() as i64)
                };
                let vals = [
                    Datum::from_i32(a),
                    Datum::from_i64(b),
                    text_datum(format!("r{r}/{i}").as_bytes(), &mut keep),
                ];
                let mut key = Vec::with_capacity(kw);
                encode_sort_key(&keys, &vals, &mut key);
                let mut img = Vec::new();
                codec.serialize_row(&vals, &mut img).unwrap();
                batch.push(&key, &img);
            }
            batch.sort();
            batches.push(batch);
        }
        (batches, kw)
    }

    fn spill_random_runs_opts(
        dir: &std::path::Path,
        runs: usize,
        rows_per_run: usize,
        dup_keys: bool,
        seed: u64,
        lz4: bool,
    ) -> (Vec<std::path::PathBuf>, usize) {
        let (batches, kw) = random_sorted_batches(runs, rows_per_run, dup_keys, seed);
        let mut paths = Vec::new();
        for (r, mut batch) in batches.into_iter().enumerate() {
            let p = dir.join(format!("run-{r}"));
            batch.spill_run_opts(&p, lz4).unwrap();
            paths.push(p);
        }
        (paths, kw)
    }

    /// Byte-stream both merges over the same runs; assert identical row
    /// sequences (concatenated bytes AND per-row lens) and identical
    /// run-bytes accounting. V1 (BinaryHeap) is the reference.
    fn assert_v2_matches_v1(paths: &[std::path::PathBuf], kw: usize) {
        let mut v1 = RunMerge::open(paths, kw).unwrap();
        v1.set_timed(true);
        v1.set_fadvise(1 << 20); // C2a hint path (no-op off-Linux)
        let (mut key, mut row) = (Vec::new(), Vec::new());
        let mut ref_arena: Vec<u8> = Vec::new();
        let mut ref_lens: Vec<u32> = Vec::new();
        while v1.next_entry(&mut key, &mut row).unwrap() {
            ref_arena.extend_from_slice(&row);
            ref_lens.push(row.len() as u32);
        }
        let mut v2 = RunMergeV2::open(paths, kw).unwrap();
        v2.set_timed(true);
        v2.set_fadvise(1 << 20);
        let mut arena: Vec<u8> = Vec::new();
        let mut lens: Vec<u32> = Vec::new();
        while let Some(l) = v2.next_row_into(&mut arena).unwrap() {
            lens.push(l);
        }
        assert_eq!(lens, ref_lens, "V2 row lengths diverge from the heap reference");
        assert_eq!(arena, ref_arena, "V2 row bytes diverge from the heap reference");
        assert_eq!(v2.fill_stats().1, v1.fill_stats().1, "run-bytes accounting diverges");
        // C2b: the prefetch-fed merge must emit the identical stream.
        let mut vp = RunMergeV2::open_prefetch(paths, kw, 3, false).unwrap();
        let mut p_arena: Vec<u8> = Vec::new();
        let mut p_lens: Vec<u32> = Vec::new();
        while let Some(l) = vp.next_row_into(&mut p_arena).unwrap() {
            p_lens.push(l);
        }
        assert_eq!(p_lens, ref_lens, "prefetch V2 row lengths diverge");
        assert_eq!(p_arena, ref_arena, "prefetch V2 row bytes diverge");
    }

    /// GL-LOADDET-1: the stable batch orders key-tied entries by ARRIVAL, and
    /// the legacy batch demonstrably does not — a determinism knob whose two
    /// arms cannot be told apart would be a knob over nothing.
    #[test]
    fn stable_batch_breaks_key_ties_by_arrival_and_legacy_does_not() {
        const KW: usize = 4;
        const N: u32 = 4096;
        // 8 distinct key values, INTERLEAVED across arrival order, so the sort
        // genuinely has to move entries (an all-equal comparator leaves
        // pdqsort's input untouched and would witness nothing). The payload
        // records arrival position, so the expected stable answer is exact.
        let key_of_i = |i: u32| -> [u8; KW] { (i.wrapping_mul(2654435761) % 8).to_be_bytes() };
        let drain = |mut b: SortBatch| -> Vec<(u32, u32)> {
            for i in 0..N {
                b.push(&key_of_i(i), &i.to_be_bytes());
            }
            b.sort();
            let (arena, kw) = (&b.arena, b.key_w);
            b.index
                .iter()
                .map(|e| {
                    let s = e.0 as usize;
                    let k = u32::from_be_bytes(arena[s..s + kw].try_into().unwrap());
                    let p = u32::from_be_bytes(
                        arena[s + kw..s + e.1 as usize].try_into().unwrap(),
                    );
                    (k, p)
                })
                .collect()
        };
        // Expected stable answer: key-major, arrival-minor.
        let mut want: Vec<(u32, u32)> = (0..N)
            .map(|i| (u32::from_be_bytes(key_of_i(i)), i))
            .collect();
        want.sort_by_key(|&(k, p)| (k, p));

        let stable = drain(SortBatch::new_stable(KW));
        assert_eq!(stable, want, "stable batch did not order key-major / arrival-minor");
        // The legacy arm is deterministic for a fixed input but NOT arrival
        // order — that difference is the whole reason the bank bytes move.
        let legacy = drain(SortBatch::new(KW));
        assert_ne!(
            legacy, want,
            "legacy batch happened to reproduce arrival order — this test can no longer \
             witness the difference the knob exists to make (pick a harder permutation)"
        );
        // Both arms must still be correctly key-ordered.
        for arm in [&stable, &legacy] {
            assert!(arm.windows(2).all(|w| w[0].0 <= w[1].0), "key order broken");
        }
        // Whatever the order, no entry may be lost or duplicated.
        let mut a = stable.clone();
        let mut b = legacy.clone();
        a.sort();
        b.sort();
        assert_eq!(a, b, "the two arms disagree on the MULTISET, not just the order");
    }

    #[test]
    fn v2_matches_heap_reference() {
        let dir = tmpdir("v2ref");
        let (paths, kw) = spill_random_runs(&dir, 13, 700, false, 42);
        assert_v2_matches_v1(&paths, kw);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn v2_matches_heap_reference_on_cross_run_key_ties() {
        let dir = tmpdir("v2tie");
        let (paths, kw) = spill_random_runs(&dir, 9, 400, true, 7);
        assert_v2_matches_v1(&paths, kw);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// loadcommit RUNLZ4: the lz4-framed runs must reproduce the raw
    /// reference byte stream through BOTH decode paths (pump-side
    /// Lz4Source and feeder-side prefetch decode), including cross-run
    /// key ties, and the compressed files must actually be smaller.
    #[test]
    fn lz4_runs_match_raw_reference() {
        for (dup, seed) in [(false, 42u64), (true, 7u64)] {
            let draw = tmpdir(&format!("lzraw{seed}"));
            let dlz = tmpdir(&format!("lzlz{seed}"));
            let (raw_paths, kw) = spill_random_runs_opts(&draw, 9, 500, dup, seed, false);
            let (lz_paths, kw2) = spill_random_runs_opts(&dlz, 9, 500, dup, seed, true);
            assert_eq!(kw, kw2);
            // identical logical rows (same LCG stream), so the raw merge is
            // the reference for both lz4 decode paths.
            let mut r = RunMergeV2::open(&raw_paths, kw).unwrap();
            let mut ref_arena = Vec::new();
            let mut ref_lens = Vec::new();
            while let Some(l) = r.next_row_into(&mut ref_arena).unwrap() {
                ref_lens.push(l);
            }
            let raw_disk: u64 = raw_paths.iter().map(|p| file_len(p)).sum();
            let lz_disk: u64 = lz_paths.iter().map(|p| file_len(p)).sum();
            assert!(
                lz_disk < raw_disk,
                "lz4 runs not smaller: {lz_disk} vs {raw_disk}"
            );
            for prefetch in [0usize, 3] {
                let mut m = if prefetch == 0 {
                    RunMergeV2::open_opts(&lz_paths, kw, true).unwrap()
                } else {
                    RunMergeV2::open_prefetch(&lz_paths, kw, prefetch, true).unwrap()
                };
                let mut arena = Vec::new();
                let mut lens = Vec::new();
                while let Some(l) = m.next_row_into(&mut arena).unwrap() {
                    lens.push(l);
                }
                assert_eq!(lens, ref_lens, "lz4 lens diverge (prefetch={prefetch})");
                assert_eq!(arena, ref_arena, "lz4 bytes diverge (prefetch={prefetch})");
                let (_, logical, disk) = m.fill_stats();
                assert_eq!(logical, {
                    let (_, l2, _) = r.fill_stats();
                    l2
                });
                assert!(disk > 0 && disk <= lz_disk, "disk accounting: {disk} vs {lz_disk}");
            }
            let _ = std::fs::remove_dir_all(&draw);
            let _ = std::fs::remove_dir_all(&dlz);
        }
    }

    /// Truncation and corruption must surface as errors (never a short
    /// silent stream, never a panic/hang) through both decode paths.
    #[test]
    fn lz4_truncation_and_corruption_error() {
        let d = tmpdir("lztrunc");
        let (paths, kw) = spill_random_runs_opts(&d, 1, 400, false, 5, true);
        let whole = read_file(&paths[0]);
        let consume = |paths: &[std::path::PathBuf], prefetch: usize| -> PgResult<u64> {
            let mut m = if prefetch == 0 {
                RunMergeV2::open_opts(paths, kw, true)?
            } else {
                RunMergeV2::open_prefetch(paths, kw, prefetch, true)?
            };
            let mut arena = Vec::new();
            let mut n = 0u64;
            while let Some(_l) = m.next_row_into(&mut arena)? {
                n += 1;
                arena.clear();
            }
            Ok(n)
        };
        // sanity: intact file merges fully
        assert_eq!(consume(&paths, 0).unwrap(), 400);
        for cut in [2usize, 7, 40, whole.len() - 3] {
            let p = d.join(format!("cut{cut}"));
            write_file(&p, &whole[..cut]);
            let ps = vec![p];
            for prefetch in [0usize, 2] {
                match consume(&ps, prefetch) {
                    Err(_) => {}
                    Ok(n) => panic!("truncated at {cut} (prefetch={prefetch}) yielded Ok({n})"),
                }
            }
        }
        // Corrupt the first frame's raw_len (byte 4, after the magic):
        // detected as a decompressed-length mismatch. (A flipped literal
        // byte INSIDE an lz4 block is undetectable at this layer — lz4
        // blocks carry no checksum, matching PG temp-file behavior; the
        // statement-scoped run files share that contract.)
        let mut bad = whole.clone();
        bad[4] ^= 0x40;
        let p = d.join("corrupt");
        write_file(&p, &bad);
        for prefetch in [0usize, 2] {
            assert!(
                consume(&[p.clone()], prefetch).is_err(),
                "corrupt frame length accepted (prefetch={prefetch})"
            );
        }
        // raw file handed to the lz4 reader = loud magic mismatch
        let (rawp, _) = spill_random_runs_opts(&d.join("."), 1, 10, false, 9, false);
        assert!(consume(&rawp, 0).is_err(), "raw file accepted as lz4");
        let _ = std::fs::remove_dir_all(&d);
    }

    // ---- copyfast lever 1: in-memory run oracles ----------------------------

    /// The three homes must emit the identical row stream: all-file lz4
    /// (reference), all-memory, and MIXED alternating file/memory — with
    /// and without prefetch feeders on the file subset. Also proves the
    /// store accounting drains to zero as the merge consumes.
    #[test]
    fn mem_runs_match_file_reference() {
        for (dup, seed) in [(false, 42u64), (true, 7u64)] {
            let dir = tmpdir(&format!("memref{seed}"));
            let (paths, kw) = spill_random_runs_opts(&dir, 8, 500, dup, seed, true);
            let mut r = RunMergeV2::open_opts(&paths, kw, true).unwrap();
            let mut ref_arena = Vec::new();
            let mut ref_lens = Vec::new();
            while let Some(l) = r.next_row_into(&mut ref_arena).unwrap() {
                ref_lens.push(l);
            }
            // Same batches (same seed) as memory runs, store-attached.
            let make_mem = |store: &std::sync::Arc<MemRunStore>| -> Vec<MemRun> {
                let (batches, kw2) = random_sorted_batches(8, 500, dup, seed);
                assert_eq!(kw, kw2);
                batches
                    .into_iter()
                    .map(|mut b| {
                        let m = b.spill_run_mem().unwrap();
                        assert!(store.try_reserve(m.bytes()), "test store sized to fit");
                        m.attach(std::sync::Arc::clone(store))
                    })
                    .collect()
            };
            // (a) all-memory.
            let store = MemRunStore::new(1 << 30);
            let inputs: Vec<RunInput> =
                make_mem(&store).into_iter().map(RunInput::Mem).collect();
            let held = store.used();
            assert!(held > 0, "mem runs reserved nothing");
            let mut m = RunMergeV2::open_mixed(inputs, kw, 0, true).unwrap();
            let mut arena = Vec::new();
            let mut lens = Vec::new();
            while let Some(l) = m.next_row_into(&mut arena).unwrap() {
                lens.push(l);
            }
            assert_eq!(lens, ref_lens, "all-mem lens diverge (dup={dup})");
            assert_eq!(arena, ref_arena, "all-mem bytes diverge (dup={dup})");
            // Logical run-bytes accounting matches the file merge.
            assert_eq!(m.fill_stats().1, r.fill_stats().1);
            drop(m);
            assert_eq!(store.used(), 0, "store not drained after merge+drop");
            // (b) mixed homes, alternating, with and without prefetch.
            for prefetch in [0usize, 2] {
                let store = MemRunStore::new(1 << 30);
                let mems = make_mem(&store);
                let inputs: Vec<RunInput> = mems
                    .into_iter()
                    .zip(paths.iter())
                    .enumerate()
                    .map(|(i, (m, p))| {
                        if i % 2 == 0 {
                            RunInput::Mem(m)
                        } else {
                            RunInput::File(p.clone())
                        }
                    })
                    .collect();
                let mut m =
                    RunMergeV2::open_mixed(inputs, kw, prefetch, true).unwrap();
                let mut arena = Vec::new();
                let mut lens = Vec::new();
                while let Some(l) = m.next_row_into(&mut arena).unwrap() {
                    lens.push(l);
                }
                assert_eq!(lens, ref_lens, "mixed lens diverge (prefetch={prefetch})");
                assert_eq!(arena, ref_arena, "mixed bytes diverge (prefetch={prefetch})");
                drop(m);
                assert_eq!(store.used(), 0, "mixed store not drained");
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// An overflow run flushed via `MemRun::write_to_file` must be
    /// byte-identical to the direct `spill_run_opts(_, true)` file of the
    /// same batch — the graceful-degradation contract.
    #[test]
    fn mem_run_overflow_file_is_byte_identical() {
        let dir = tmpdir("memflush");
        let (paths, _kw) = spill_random_runs_opts(&dir, 3, 400, false, 11, true);
        let (batches, _) = random_sorted_batches(3, 400, false, 11);
        for (i, mut b) in batches.into_iter().enumerate() {
            let m = b.spill_run_mem().unwrap();
            let p = dir.join(format!("flush-{i}"));
            let written = m.write_to_file(&p).unwrap();
            let direct = read_file(&paths[i]);
            let flushed = read_file(&p);
            assert_eq!(written, flushed.len() as u64);
            assert_eq!(flushed, direct, "flushed run {i} diverges from direct spill");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Store cap arithmetic: reserve up to the cap, refuse past it,
    /// release restores capacity; drop of an attached run releases the rest.
    #[test]
    fn mem_run_store_budget() {
        let store = MemRunStore::new(1000);
        assert!(store.try_reserve(600));
        assert!(!store.try_reserve(500), "over-cap reserve admitted");
        assert!(store.try_reserve(400));
        assert_eq!(store.used(), 1000);
        assert!(!store.try_reserve(1), "full store admitted more");
        store.release(250);
        assert_eq!(store.used(), 750);
        assert!(store.try_reserve(250));
        // Attached-run drop releases exactly its remaining bytes.
        let (batches, _kw) = random_sorted_batches(1, 50, false, 3);
        let mut batches = batches;
        let m = batches[0].spill_run_mem().unwrap();
        let store2 = MemRunStore::new(1 << 20);
        assert!(store2.try_reserve(m.bytes()));
        let held = store2.used();
        assert_eq!(held, m.bytes());
        let m = m.attach(std::sync::Arc::clone(&store2));
        drop(m);
        assert_eq!(store2.used(), 0);
    }

    #[test]
    fn v2_edges_empty_single_and_uneven_runs() {
        // No runs at all.
        let mut v2 = RunMergeV2::open(&[], 12).unwrap();
        let mut arena = Vec::new();
        assert!(v2.next_row_into(&mut arena).unwrap().is_none());
        assert!(arena.is_empty());
        // Single run (no internal tournament nodes).
        let dir = tmpdir("v2edge");
        let (paths, kw) = spill_random_runs(&dir, 1, 257, false, 3);
        assert_v2_matches_v1(&paths, kw);
        // Non-power-of-two run counts sweep the uneven-depth tree shapes.
        for runs in [2usize, 3, 5, 6, 7] {
            let d2 = tmpdir(&format!("v2edge{runs}"));
            let (paths, kw) = spill_random_runs(&d2, runs, 100 + runs * 17, true, runs as u64);
            assert_v2_matches_v1(&paths, kw);
            let _ = std::fs::remove_dir_all(&d2);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
