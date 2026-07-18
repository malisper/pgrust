//! md.c-compatible segment fan-out for the part's byte stream: logical byte
//! offsets over 1 GiB segment files (`path`, `path.1`, ...) so smgr's
//! mdnblocks/mdunlink stay valid on pgrcolumnar relations.

use std::fs::{File, OpenOptions};
#[cfg(not(target_family = "wasm"))]
use std::os::unix::fs::FileExt;
// wasm32: WASI has positioned reads/writes (fd_pread/fd_pwrite); the ext
// trait is unstable (wasi_ext) — gated at the crate root for the pinned
// nightly the wasm build runs on.
#[cfg(target_family = "wasm")]
use std::os::wasi::fs::FileExt;

use ::types_error::{PgError, PgResult};

pub const SEG_BYTES: u64 = 1 << 30;
pub const BLCKSZ: u64 = 8192;

fn io_err(path: &str, e: std::io::Error) -> Box<PgError> {
    Box::new(PgError::error(format!("cbstore io error on \"{path}\": {e}")))
}

pub fn seg_path(base: &str, segno: usize) -> String {
    if segno == 0 {
        base.to_string()
    } else {
        format!("{base}.{segno}")
    }
}

pub struct SegFile {
    base: String,
    segs: Vec<File>,
}

impl SegFile {
    pub fn open_rw(base: &str) -> PgResult<SegFile> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(base)
            .map_err(|e| io_err(base, e))?;
        let mut sf = SegFile { base: base.to_string(), segs: vec![f] };
        loop {
            let p = seg_path(&sf.base, sf.segs.len());
            match OpenOptions::new().read(true).write(true).open(&p) {
                Ok(f) => sf.segs.push(f),
                Err(_) => break,
            }
        }
        Ok(sf)
    }

    fn seg_for(&mut self, segno: usize, create: bool) -> PgResult<&File> {
        while self.segs.len() <= segno {
            let p = seg_path(&self.base, self.segs.len());
            let f = if create {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&p)
                    .map_err(|e| io_err(&p, e))?
            } else {
                OpenOptions::new().read(true).write(true).open(&p).map_err(|e| io_err(&p, e))?
            };
            self.segs.push(f);
        }
        Ok(&self.segs[segno])
    }

    pub fn write_all_at(&mut self, mut buf: &[u8], mut off: u64) -> PgResult<()> {
        while !buf.is_empty() {
            let segno = (off / SEG_BYTES) as usize;
            let seg_off = off % SEG_BYTES;
            let take = ((SEG_BYTES - seg_off) as usize).min(buf.len());
            let base = self.base.clone();
            let f = self.seg_for(segno, true)?;
            f.write_all_at(&buf[..take], seg_off).map_err(|e| io_err(&base, e))?;
            buf = &buf[take..];
            off += take as u64;
        }
        Ok(())
    }

    pub fn read_exact_at(&mut self, mut buf: &mut [u8], mut off: u64) -> PgResult<()> {
        while !buf.is_empty() {
            let segno = (off / SEG_BYTES) as usize;
            let seg_off = off % SEG_BYTES;
            let take = ((SEG_BYTES - seg_off) as usize).min(buf.len());
            let base = self.base.clone();
            let f = self.seg_for(segno, false)?;
            f.read_exact_at(&mut buf[..take], seg_off).map_err(|e| io_err(&base, e))?;
            buf = &mut buf[take..];
            off += take as u64;
        }
        Ok(())
    }

    pub fn total_len(&self) -> u64 {
        let mut total = 0u64;
        for f in &self.segs {
            total += f.metadata().map(|m| m.len()).unwrap_or(0);
        }
        total
    }

    /// Advisory readahead over the logical byte range [off, off+len):
    /// POSIX_FADV_WILLNEED per underlying segment span, so a following
    /// `read_exact_at` sequence overlaps its later ranges' disk fetches
    /// with the earlier ranges' synchronous reads. Purely advisory —
    /// kernel errors ignored, missing segments end the walk, no-op off
    /// Linux (cold-readahead lane).
    pub fn advise_willneed(&mut self, off: u64, len: u64) {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let (mut off, mut len) = (off, len);
            while len > 0 {
                let segno = (off / SEG_BYTES) as usize;
                let seg_off = off % SEG_BYTES;
                let take = (SEG_BYTES - seg_off).min(len);
                let Ok(f) = self.seg_for(segno, false) else { return };
                // SAFETY: plain libc call on a live fd; advisory only.
                unsafe {
                    libc::posix_fadvise(
                        f.as_raw_fd(),
                        seg_off as libc::off_t,
                        take as libc::off_t,
                        libc::POSIX_FADV_WILLNEED,
                    );
                }
                off += take;
                len -= take;
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (off, len);
        }
    }

    /// md contract: every non-last segment is exactly SEG_BYTES; pad every
    /// segment's tail to a BLCKSZ multiple so mdnblocks' division is exact.
    pub fn pad_and_sync(&mut self, logical_end: u64) -> PgResult<()> {
        let nsegs = (logical_end.div_ceil(SEG_BYTES) as usize).max(1);
        for segno in 0..nsegs {
            let want = if segno + 1 < nsegs {
                SEG_BYTES
            } else {
                (logical_end - segno as u64 * SEG_BYTES).div_ceil(BLCKSZ) * BLCKSZ
            };
            let base = self.base.clone();
            let f = self.seg_for(segno, true)?;
            let cur = f.metadata().map(|m| m.len()).unwrap_or(0);
            if cur < want {
                f.set_len(want).map_err(|e| io_err(&base, e))?;
            }
            f.sync_data().map_err(|e| io_err(&base, e))?;
        }
        Ok(())
    }

    pub fn sync_data(&mut self) -> PgResult<()> {
        for f in &self.segs {
            f.sync_data().map_err(|e| io_err(&self.base, e))?;
        }
        Ok(())
    }
}

/// Contiguous read-only view over all segments: PROT_NONE reservation +
/// MAP_FIXED per segment, so chunk framing can span segment boundaries.
#[cfg(not(target_family = "wasm"))]
pub struct SegMap {
    ptr: *const u8,
    maplen: usize,
}

// wasm32: no mmap exists on wasm32-wasip1 (wasi-libc has no sys_mmap; the
// address space is a single linear memory). The wasm arm materializes the
// sealed part bytes into an owned heap buffer via positioned reads — same
// bytes() contract, no address-space tricks. Cost: whole-part reads and
// resident copies (fine at boot-increment scale; a paged reader is the
// structural fix if cbstore-on-wasm ever needs to be cheap).
#[cfg(target_family = "wasm")]
pub struct SegMap {
    buf: Vec<u8>,
}

// SAFETY (coldio lane, process-shared mappings): the mapping is PROT_READ
// over sealed segment bytes for its whole lifetime — no &mut access exists
// (bytes() hands out &[u8] only) and Drop's munmap runs exactly once when
// the last holder goes away. Concurrent readers on any thread are the mmap
// contract itself.
#[cfg(not(target_family = "wasm"))]
unsafe impl Send for SegMap {}
#[cfg(not(target_family = "wasm"))]
unsafe impl Sync for SegMap {}

// Process-shared SegMap registry (coldio lane): one live mapping per
// (seg0 dev, seg0 ino, maplen). Motivation (measured, notes/coldio-lane.md
// step-1): the part cache is THREAD-local, so every runtime pool worker
// used to mmap the same part privately — N workers = N mappings = each page
// cache page taking up to N separate minor faults (one per mapping's PTEs)
// serialized on the process's mmap_lock, and re-taken across queries/reps
// as morsel assignment shifts. q33@100M cold: 48.6s sys / ~1.1M minflt on
// the official 32GB substrate, 28.2s sys with memory pressure removed —
// mapping fan-out, not reads (majflt ~0). One shared mapping = one PTE fill
// per page per process, persistent for as long as any part-cache entry
// holds the Arc. Keyed by maplen so a published append (longer file) mints
// a new mapping; the old dies with its last holder. The registry holds
// Weak — it never extends a mapping's lifetime. Kill switch:
// PGRUST_CBSTORE_SHARED_MAP=0/off (historical private mapping per open).
static SHARED_MAPS: std::sync::Mutex<
    Option<std::collections::HashMap<(u64, u64, usize), std::sync::Weak<SegMap>>>,
> = std::sync::Mutex::new(None);

fn shared_map_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_SHARED_MAP").as_deref(),
            Ok("0") | Ok("off") | Ok("OFF"),
        )
    })
}

impl SegMap {
    // Stat pass shared by open/open_shared: the segment files plus the
    // reservation length ((nsegs-1)*SEG_BYTES + last seg len). None = empty.
    fn stat_segs(base: &str) -> PgResult<Option<(Vec<File>, Vec<u64>, usize)>> {
        let mut files: Vec<File> = Vec::new();
        loop {
            let p = seg_path(base, files.len());
            match File::open(&p) {
                Ok(f) => files.push(f),
                Err(e) => {
                    if files.is_empty() {
                        return Err(io_err(&p, e));
                    }
                    break;
                }
            }
        }
        let mut lens = Vec::with_capacity(files.len());
        let mut total = 0u64;
        for f in &files {
            let l = f.metadata().map_err(|e| io_err(base, e))?.len();
            lens.push(l);
            total += l;
        }
        if total == 0 {
            return Ok(None);
        }
        let maplen = ((files.len() - 1) as u64 * SEG_BYTES + *lens.last().unwrap()) as usize;
        Ok(Some((files, lens, maplen)))
    }

    /// Process-shared open: return the live mapping for this part state if
    /// any thread already holds one, else map and register. `dev`/`ino` are
    /// seg0's (the part-cache identity vocabulary — the caller already
    /// stat'd them; a recreate changes ino, a published append changes
    /// maplen, so equal keys imply the identical sealed byte image).
    pub fn open_shared(
        base: &str,
        dev: u64,
        ino: u64,
    ) -> PgResult<Option<std::sync::Arc<SegMap>>> {
        let Some((files, lens, maplen)) = SegMap::stat_segs(base)? else { return Ok(None) };
        if !shared_map_enabled() {
            return Ok(Some(std::sync::Arc::new(SegMap::map_segs(&files, &lens, maplen)?)));
        }
        let key = (dev, ino, maplen);
        let mut guard = SHARED_MAPS.lock().unwrap();
        let map = guard.get_or_insert_with(Default::default);
        if let Some(live) = map.get(&key).and_then(std::sync::Weak::upgrade) {
            return Ok(Some(live));
        }
        // Map under the lock (mmap manipulates only the address space — no
        // I/O) so two racing opens never double-map the same key.
        let fresh = std::sync::Arc::new(SegMap::map_segs(&files, &lens, maplen)?);
        map.retain(|_, w| w.strong_count() > 0);
        map.insert(key, std::sync::Arc::downgrade(&fresh));
        Ok(Some(fresh))
    }

    pub fn open(base: &str) -> PgResult<Option<SegMap>> {
        let Some((files, lens, maplen)) = SegMap::stat_segs(base)? else { return Ok(None) };
        Ok(Some(SegMap::map_segs(&files, &lens, maplen)?))
    }

    // wasm32 twin: read every segment into the contiguous buffer at its
    // logical offset (i*SEG_BYTES), zero-fill between (mmap's reservation
    // shape). Sealed bytes: no coherence hazard vs the writer's pwrites.
    #[cfg(target_family = "wasm")]
    fn map_segs(files: &[File], lens: &[u64], maplen: usize) -> PgResult<SegMap> {
        let mut buf = vec![0u8; maplen];
        for (i, f) in files.iter().enumerate() {
            if lens[i] == 0 {
                continue;
            }
            let at = i * SEG_BYTES as usize;
            f.read_exact_at(&mut buf[at..at + lens[i] as usize], 0)
                .map_err(|e| io_err("cbstore segment", e))?;
        }
        Ok(SegMap { buf })
    }

    #[cfg(not(target_family = "wasm"))]
    fn map_segs(files: &[File], lens: &[u64], maplen: usize) -> PgResult<SegMap> {
        unsafe {
            let reserve = libc::mmap(
                std::ptr::null_mut(),
                maplen,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );
            if reserve == libc::MAP_FAILED {
                return Err(Box::new(PgError::error("cbstore: mmap reserve failed".to_string())));
            }
            for (i, f) in files.iter().enumerate() {
                if lens[i] == 0 {
                    continue;
                }
                let at = (reserve as *mut u8).add(i * SEG_BYTES as usize);
                let p = libc::mmap(
                    at as *mut libc::c_void,
                    lens[i] as usize,
                    libc::PROT_READ,
                    libc::MAP_SHARED | libc::MAP_FIXED,
                    std::os::fd::AsRawFd::as_raw_fd(f),
                    0,
                );
                if p == libc::MAP_FAILED {
                    libc::munmap(reserve, maplen);
                    return Err(Box::new(PgError::error(
                        "cbstore: mmap segment failed".to_string(),
                    )));
                }
                #[cfg(target_os = "linux")]
                libc::madvise(p, lens[i] as usize, libc::MADV_SEQUENTIAL);
            }
            Ok(SegMap { ptr: reserve as *const u8, maplen })
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.maplen) }
    }

    #[cfg(target_family = "wasm")]
    pub fn bytes(&self) -> &[u8] {
        &self.buf
    }
}

// wasm32: the owned buffer drops itself.
#[cfg(not(target_family = "wasm"))]
impl Drop for SegMap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.maplen);
        }
    }
}
