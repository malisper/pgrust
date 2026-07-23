//! md.c-compatible segment fan-out for the part's byte stream: logical byte
//! offsets over 1 GiB segment files (`path`, `path.1`, ...) so smgr's
//! mdnblocks/mdunlink stay valid on pgrcolumnar relations.
//!
//! DST P1 (WS-B): every CbIo entry point here fronts the `vfs` boundary —
//! opens via `vfs::open`, the pread/pwrite data plane, fstat sizing,
//! ftruncate padding, fdatasync durability, fadvise hints, and fd lifecycle
//! (`SegFd` closes via the `vfs::VfsFd` guard — same close code path as
//! `vfs::close`, TLS-teardown-safe under sim). Semantics are byte-identical to the
//! pre-P1 `std::fs` path by construction: same open flags (O_CLOEXEC, mode
//! 0o666, no O_TRUNC on the create arm), same syscall order, the same
//! EINTR-retry loops `std::os::unix::fs::FileExt` ran, and the same error
//! text (errno rendered through `io::Error::from_raw_os_error`). The ONE
//! carve-out is `SegMap::map_segs`: the mmap read arm stays raw libc until
//! the segmap pread arm lands (contract §3 item 5, allowlist rows tagged
//! `# pending: segmap-pread-arm`).

use std::ffi::CString;

use ::types_error::{PgError, PgResult};

pub const SEG_BYTES: u64 = 1 << 30;
pub const BLCKSZ: u64 = 8192;

/// Effective segment size. Production builds const-fold the 1 GiB law; test
/// and sim builds admit a process-global override so multi-segment parts
/// (and their crash trajectories) are cheap to mint.
#[cfg(not(any(test, pgrust_sim)))]
#[inline(always)]
pub fn seg_bytes() -> u64 {
    SEG_BYTES
}

#[cfg(any(test, pgrust_sim))]
pub fn seg_bytes() -> u64 {
    *seg_bytes_cell().get_or_init(|| SEG_BYTES)
}

/// TEST SUPPORT (crash batteries): shrink the segment size for this process.
/// First initialization wins (OnceLock) — call before the battery's first
/// SegFile op; a repeat call with the same value is a no-op, a conflicting
/// value asserts. Multiples of BLCKSZ only (pad_and_sync's md contract).
#[cfg(any(test, pgrust_sim))]
#[doc(hidden)]
pub fn set_seg_bytes_for_tests(v: u64) {
    assert!(v > 0 && v.is_multiple_of(BLCKSZ), "test segment size must be a BLCKSZ multiple");
    let installed = *seg_bytes_cell().get_or_init(|| v);
    assert!(installed == v, "seg_bytes already initialized to {installed}");
}

#[cfg(any(test, pgrust_sim))]
fn seg_bytes_cell() -> &'static std::sync::OnceLock<u64> {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    &V
}

fn io_err(path: &str, e: std::io::Error) -> Box<PgError> {
    Box::new(PgError::error(format!("cbstore io error on \"{path}\": {e}")))
}

/// The errno of the vfs op that just failed, rendered exactly as the old
/// std::fs path rendered it (io::Error Display for raw OS errors).
fn io_err_errno(path: &str) -> Box<PgError> {
    io_err(path, std::io::Error::from_raw_os_error(vfs::get_errno()))
}

fn cpath(path: &str) -> PgResult<CString> {
    CString::new(path).map_err(|_| {
        // std::fs parity: the message std returns for NUL-bearing paths.
        io_err(
            path,
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "file name contained an unexpected NUL byte",
            ),
        )
    })
}

pub fn seg_path(base: &str, segno: usize) -> String {
    if segno == 0 {
        base.to_string()
    } else {
        format!("{base}.{segno}")
    }
}

/// RAII segment descriptor: opened via `vfs::open`, closed by the
/// [`vfs::VfsFd`] guard on Drop — the whole fd lifecycle travels through the
/// Vfs boundary. The guard's drop arm is the same close code path as
/// `vfs::close` while the thread's vfs universe is alive (posix arm: literally
/// `close(2)`, identical to the direct `vfs::close` Drop this replaces), and
/// is additionally safe if a SegFile/SegFd drops during thread-exit TLS
/// teardown under `--cfg pgrust_sim`, where a direct `vfs::close` would hit
/// the panicking `SIM.with` arm inside a TLS destructor.
struct SegFd(vfs::VfsFd);

impl SegFd {
    #[inline]
    fn raw(&self) -> libc::c_int {
        use std::os::fd::AsRawFd;
        self.0.as_raw_fd()
    }
}

/// std::fs::OpenOptions parity: O_CLOEXEC always, mode 0o666 (umask
/// applies), open(2) EINTR-retried (std's cvt_r); callers add O_CREAT
/// (never O_TRUNC — `truncate(false)`).
fn open_fd(path: &str, flags: libc::c_int) -> PgResult<SegFd> {
    let c = cpath(path)?;
    loop {
        let fd = vfs::open(&c, flags | libc::O_CLOEXEC, 0o666);
        if fd >= 0 {
            // SAFETY: fresh descriptor minted by the active vfs on this
            // thread, exclusively owned by the guard.
            return Ok(SegFd(unsafe { vfs::VfsFd::from_raw(fd) }));
        }
        if vfs::get_errno() != libc::EINTR {
            return Err(io_err_errno(path));
        }
    }
}

/// std File::sync_data parity through the Vfs boundary: fdatasync, EINTR
/// retried. (Linux — the official substrate — is instruction-identical;
/// on macOS this is fdatasync(2), the fd-crate pg_fsync posture, where std
/// used F_FULLFSYNC.)
fn sync_fd(fd: libc::c_int, path: &str) -> PgResult<()> {
    while vfs::fdatasync(fd) < 0 {
        if vfs::get_errno() != libc::EINTR {
            return Err(io_err_errno(path));
        }
    }
    Ok(())
}

pub struct SegFile {
    base: String,
    segs: Vec<SegFd>,
    /// A segment file may have been CREATED since the last parent-directory
    /// fsync (any create-armed open of a previously-unopened segno —
    /// conservative: O_CREAT does not report whether the name existed). Its
    /// directory entry is volatile until [`SegFile::sync_dir_if_minted`]
    /// runs: fdatasync of the segment persists the file's DATA, not the
    /// dirent that names it, so a crash can keep the bytes and the published
    /// footer pointer while dropping the file itself (GH issue #2).
    minted: bool,
}

impl SegFile {
    pub fn open_rw(base: &str) -> PgResult<SegFile> {
        let f = open_fd(base, libc::O_RDWR)?;
        let mut sf = SegFile { base: base.to_string(), segs: vec![f], minted: false };
        loop {
            let p = seg_path(&sf.base, sf.segs.len());
            match open_fd(&p, libc::O_RDWR) {
                Ok(f) => sf.segs.push(f),
                Err(_) => break,
            }
        }
        Ok(sf)
    }

    fn seg_for(&mut self, segno: usize, create: bool) -> PgResult<libc::c_int> {
        while self.segs.len() <= segno {
            let p = seg_path(&self.base, self.segs.len());
            let flags = if create { libc::O_RDWR | libc::O_CREAT } else { libc::O_RDWR };
            self.segs.push(open_fd(&p, flags)?);
            if create {
                self.minted = true;
            }
        }
        Ok(self.segs[segno].raw())
    }

    /// Directory-entry durability for minted segments — the parent-dir half
    /// of the create recipe (fsync the file, then fsync its directory; the
    /// SLRU segment-create convention, fd.c fsync_fname(dir, true)). Callers
    /// MUST run this after the segments' own syncs and BEFORE publishing any
    /// pointer that reaches into a minted segment (writer.rs finish: the
    /// header's footer_off). Synchronous on the commit path by design:
    /// segment creates are not WAL-logged and sit in no checkpointer fsync
    /// set, so nothing else ever persists the dirent. No-op unless a segment
    /// was minted since the last call (once per 1 GiB spill, not per
    /// commit).
    pub fn sync_dir_if_minted(&mut self) -> PgResult<()> {
        if !self.minted {
            return Ok(());
        }
        let dir = match self.base.rfind('/') {
            Some(0) => "/",
            Some(i) => &self.base[..i],
            None => ".",
        };
        // fd.c fsync_fname_ext dir posture: open O_RDONLY (directories
        // refuse writable opens); an fsync that the platform refuses for
        // directories (EBADF/EINVAL) is ignorable — the open+retry loop
        // matches the segment arms above otherwise.
        let f = open_fd(dir, libc::O_RDONLY)?;
        while vfs::fsync(f.raw()) < 0 {
            let en = vfs::get_errno();
            if en == libc::EINTR {
                continue;
            }
            if en == libc::EBADF || en == libc::EINVAL {
                break;
            }
            return Err(io_err_errno(dir));
        }
        self.minted = false;
        Ok(())
    }

    /// Is a minted segment's dirent still awaiting its parent-dir fsync?
    /// (Test observability for the mint→publish ordering contract.)
    #[doc(hidden)]
    pub fn dirents_pending(&self) -> bool {
        self.minted
    }

    pub fn write_all_at(&mut self, mut buf: &[u8], mut off: u64) -> PgResult<()> {
        while !buf.is_empty() {
            let segno = (off / seg_bytes()) as usize;
            let seg_off = off % seg_bytes();
            let take = ((seg_bytes() - seg_off) as usize).min(buf.len());
            let fd = self.seg_for(segno, true)?;
            // FileExt::write_all_at parity: pwrite loop, EINTR retried,
            // zero-progress write => WriteZero with std's message text.
            let mut done = 0usize;
            while done < take {
                let n =
                    vfs::pwrite(fd, &buf[done..take], (seg_off + done as u64) as libc::off_t);
                if n < 0 {
                    if vfs::get_errno() == libc::EINTR {
                        continue;
                    }
                    return Err(io_err_errno(&self.base));
                }
                if n == 0 {
                    return Err(io_err(
                        &self.base,
                        std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        ),
                    ));
                }
                done += n as usize;
            }
            buf = &buf[take..];
            off += take as u64;
        }
        Ok(())
    }

    pub fn read_exact_at(&mut self, mut buf: &mut [u8], mut off: u64) -> PgResult<()> {
        while !buf.is_empty() {
            let segno = (off / seg_bytes()) as usize;
            let seg_off = off % seg_bytes();
            let take = ((seg_bytes() - seg_off) as usize).min(buf.len());
            let fd = self.seg_for(segno, false)?;
            // FileExt::read_exact_at parity: pread loop, EINTR retried, EOF
            // before the span fills => UnexpectedEof with std's message text.
            let mut done = 0usize;
            while done < take {
                let n =
                    vfs::pread(fd, &mut buf[done..take], (seg_off + done as u64) as libc::off_t);
                if n < 0 {
                    if vfs::get_errno() == libc::EINTR {
                        continue;
                    }
                    return Err(io_err_errno(&self.base));
                }
                if n == 0 {
                    return Err(io_err(
                        &self.base,
                        std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        ),
                    ));
                }
                done += n as usize;
            }
            buf = &mut buf[take..];
            off += take as u64;
        }
        Ok(())
    }

    pub fn total_len(&self) -> u64 {
        let mut total = 0u64;
        for f in &self.segs {
            // metadata().len() parity: fstat; an fstat failure contributes 0
            // (the old unwrap_or(0)).
            let mut fi = vfs::FileInfo::zeroed();
            if vfs::fstat(f.raw(), &mut fi) == 0 {
                total += fi.size as u64;
            }
        }
        total
    }

    /// Advisory readahead over the logical byte range [off, off+len):
    /// POSIX_FADV_WILLNEED per underlying segment span, so a following
    /// `read_exact_at` sequence overlaps its later ranges' disk fetches
    /// with the earlier ranges' synchronous reads. Purely advisory —
    /// kernel errors ignored, missing segments end the walk, no-op off
    /// Linux (cold-readahead lane). The cfg gate stays HERE (not just in
    /// PosixVfs) so the off-Linux no-op keeps its exact pre-P1 shape:
    /// no lazy segment opens either.
    pub fn advise_willneed(&mut self, off: u64, len: u64) {
        #[cfg(target_os = "linux")]
        {
            let (mut off, mut len) = (off, len);
            while len > 0 {
                let segno = (off / seg_bytes()) as usize;
                let seg_off = off % seg_bytes();
                let take = (seg_bytes() - seg_off).min(len);
                let Ok(fd) = self.seg_for(segno, false) else { return };
                // Hint via the Vfs boundary; same spans, same order (the
                // binding no-reorder/no-coalesce rule), errors ignored.
                vfs::fadvise_willneed(fd, seg_off as libc::off_t, take as libc::off_t);
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
        let nsegs = (logical_end.div_ceil(seg_bytes()) as usize).max(1);
        for segno in 0..nsegs {
            let want = if segno + 1 < nsegs {
                seg_bytes()
            } else {
                (logical_end - segno as u64 * seg_bytes()).div_ceil(BLCKSZ) * BLCKSZ
            };
            let fd = self.seg_for(segno, true)?;
            let mut fi = vfs::FileInfo::zeroed();
            let cur = if vfs::fstat(fd, &mut fi) == 0 { fi.size as u64 } else { 0 };
            if cur < want {
                // std File::set_len parity: ftruncate, EINTR retried.
                while vfs::ftruncate(fd, want as libc::off_t) < 0 {
                    if vfs::get_errno() != libc::EINTR {
                        return Err(io_err_errno(&self.base));
                    }
                }
            }
            sync_fd(fd, &self.base)?;
        }
        Ok(())
    }

    pub fn sync_data(&mut self) -> PgResult<()> {
        for f in &self.segs {
            sync_fd(f.raw(), &self.base)?;
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
// as morsel assignment shifts. A ~10M-group two-int-key top-n @100M cold: 48.6s sys / ~1.1M minflt on
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
    // Fronted (DST P1): opens via vfs::open (O_RDONLY|O_CLOEXEC — the
    // File::open flags), sizes via vfs::fstat, close via SegFd/vfs::close.
    fn stat_segs(base: &str) -> PgResult<Option<(Vec<SegFd>, Vec<u64>, usize)>> {
        let mut files: Vec<SegFd> = Vec::new();
        loop {
            let p = seg_path(base, files.len());
            match open_fd(&p, libc::O_RDONLY) {
                Ok(f) => files.push(f),
                Err(e) => {
                    if files.is_empty() {
                        return Err(e);
                    }
                    break;
                }
            }
        }
        let mut lens = Vec::with_capacity(files.len());
        let mut total = 0u64;
        for f in &files {
            let mut fi = vfs::FileInfo::zeroed();
            if vfs::fstat(f.raw(), &mut fi) < 0 {
                return Err(io_err_errno(base));
            }
            lens.push(fi.size as u64);
            total += fi.size as u64;
        }
        if total == 0 {
            return Ok(None);
        }
        let maplen = ((files.len() - 1) as u64 * seg_bytes() + *lens.last().unwrap()) as usize;
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
    // Merge composition (dst/p4-simnet): the p5 twin read via std::fs::File;
    // the substrate rewired segments onto vfs::VfsFd, so the twin now preads
    // through the same vfs data plane (EINTR loop matching SegFile::read).
    #[cfg(target_family = "wasm")]
    fn map_segs(files: &[SegFd], lens: &[u64], maplen: usize) -> PgResult<SegMap> {
        let mut buf = vec![0u8; maplen];
        for (i, f) in files.iter().enumerate() {
            if lens[i] == 0 {
                continue;
            }
            let at = i * seg_bytes() as usize;
            let dst = &mut buf[at..at + lens[i] as usize];
            let mut done = 0usize;
            while done < dst.len() {
                let n = vfs::pread(f.raw(), &mut dst[done..], done as libc::off_t);
                if n < 0 {
                    if vfs::get_errno() == libc::EINTR {
                        continue;
                    }
                    return Err(io_err_errno("cbstore segment"));
                }
                if n == 0 {
                    return Err(io_err(
                        "cbstore segment",
                        std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "failed to fill whole buffer",
                        ),
                    ));
                }
                done += n as usize;
            }
        }
        Ok(SegMap { buf })
    }

    // CARVE-OUT (DST P1 contract §3 item 5): the mmap read arm stays RAW
    // libc — mmap/madvise/munmap are out of Vfs scope until the segmap
    // pread arm lands. P0 allowlist rows retained, tagged
    // "# pending: segmap-pread-arm". The mapping-mode kill switch above and
    // the coldio SegMap registry are untouched. Sim-scope callers must not
    // reach this arm (SimVfs fds cannot back a MAP_SHARED mapping).
    #[cfg(not(target_family = "wasm"))]
    fn map_segs(files: &[SegFd], lens: &[u64], maplen: usize) -> PgResult<SegMap> {
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
                let at = (reserve as *mut u8).add(i * seg_bytes() as usize);
                let p = libc::mmap(
                    at as *mut libc::c_void,
                    lens[i] as usize,
                    libc::PROT_READ,
                    libc::MAP_SHARED | libc::MAP_FIXED,
                    f.raw(),
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

// Mint→publish dirent-durability contract (GH issue #2). The crash-cut
// batteries live in tests/sim_dirsync.rs (SimVfs models dirent loss); these
// native tests pin the flag semantics the writer's publish ordering rides.
#[cfg(test)]
mod dirsync_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-dirsync-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(format!("{}.1", p.display()));
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    // A write spanning the segment boundary mints `base.1` (sparse file —
    // nothing materializes the 1 GiB) and raises the pending-dirent flag;
    // sync_dir_if_minted clears it, and a second call is a no-op.
    #[test]
    fn mint_sets_flag_and_dir_sync_clears_it() {
        let base = tmp("mint");
        let mut f = SegFile::open_rw(&base).unwrap();
        assert!(!f.dirents_pending(), "no mint yet");
        f.write_all_at(b"abc", seg_bytes() - 1).unwrap();
        assert!(f.dirents_pending(), "boundary-crossing write minted base.1");
        f.sync_dir_if_minted().unwrap();
        assert!(!f.dirents_pending(), "dir fsync retires the pending mint");
        f.sync_dir_if_minted().unwrap(); // idempotent no-op
        assert!(!f.dirents_pending());

        // The bytes really do span the seam.
        let mut buf = [0u8; 3];
        f.read_exact_at(&mut buf, seg_bytes() - 1).unwrap();
        assert_eq!(&buf, b"abc");
        drop(f);

        // Reopening an existing multi-segment part mints nothing.
        let f2 = SegFile::open_rw(&base).unwrap();
        assert!(!f2.dirents_pending(), "open_rw of existing segments is not a mint");
        drop(f2);
        std::fs::remove_file(&base).unwrap();
        std::fs::remove_file(format!("{base}.1")).unwrap();
    }

    // Single-segment traffic never raises the flag: the commit path owes no
    // dir fsync unless a segment file was actually created.
    #[test]
    fn no_mint_no_pending_dirents() {
        let base = tmp("nomint");
        let mut f = SegFile::open_rw(&base).unwrap();
        f.write_all_at(b"hello", 0).unwrap();
        f.pad_and_sync(5).unwrap();
        assert!(!f.dirents_pending(), "seg 0 existed already; nothing minted");
        f.sync_dir_if_minted().unwrap(); // no-op, must not error
        drop(f);
        std::fs::remove_file(&base).unwrap();
    }
}
