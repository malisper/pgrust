//! md.c-compatible segment fan-out for the part's byte stream: logical byte
//! offsets over 1 GiB segment files (`path`, `path.1`, ...) so smgr's
//! mdnblocks/mdunlink stay valid on cbstore relations.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;

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
pub struct SegMap {
    ptr: *const u8,
    maplen: usize,
}

impl SegMap {
    pub fn open(base: &str) -> PgResult<Option<SegMap>> {
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
            Ok(Some(SegMap { ptr: reserve as *const u8, maplen }))
        }
    }

    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.maplen) }
    }
}

impl Drop for SegMap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.maplen);
        }
    }
}
