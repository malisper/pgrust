//! [fmt-layout lane] Bank-grain stats plane — READ SIDE ONLY at P1-1
//! (port-study/port-map.md §3: generation/build is P5-1's job; the PoC
//! builder + seal-verify stay in the reference tree).
//!
//! `Plane::open(dir, manifest)` validates the meta region against the
//! resolved manifest (gen + schema fingerprint + full per-part identity
//! vector). ANY mismatch → typed refusal → the caller falls back to
//! per-part sections. Consults fault a column's whole payload in ONE
//! pread on first touch (lazily), crc-check it, then serve every part
//! from the resident buffer. Arming is an OpenOpts decision (no env).

use pgrc2_format::bankstats as fb;
use std::io::Read;
#[cfg(not(target_family = "wasm"))]
use std::os::unix::fs::FileExt;
#[cfg(target_family = "wasm")]
use std::os::wasi::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// The open plane: validated meta + lazily-faulted column payloads.
pub struct Plane {
    file: std::fs::File,
    header: fb::BankStatsHeader,
    cols: Vec<fb::ColDirEntry>,
    /// attno -> index into `cols` (attno is small; direct map).
    by_attno: Vec<Option<usize>>,
    /// Lazily-faulted column payload buffers (crc-checked on fault).
    /// OnceLock per column: concurrent openers (par_parts workers) fault
    /// the column exactly ONCE — the one-pread-per-column law.
    bufs: Vec<OnceLock<Option<Arc<Vec<u8>>>>>,
    pub faults: AtomicU64,
    pub fault_bytes: AtomicU64,
}

impl Plane {
    /// Open + validate against the resolved manifest. `None` (with a
    /// witness line) on any refusal — the caller stays on per-part
    /// sections.
    pub fn open(bank_dir: &str, m: &pgrc2_format::manifest::Manifest) -> Option<Arc<Plane>> {
        let gen = m.header.gen;
        let path = format!("{}/{}", bank_dir, fb::bankstats_file_name(gen));
        let mut f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                println!("BANKSTATS|absent|path={path}");
                return None;
            }
        };
        // Meta region: header first (48 B), then the rest of meta_len.
        let mut hdr = vec![0u8; fb::BANKSTATS_HEADER_LEN];
        if f.read_exact(&mut hdr).is_err() {
            println!("BANKSTATS|stale|reason=short-header|path={path}");
            return None;
        }
        // meta_len sits at offset 32.
        let meta_len = u64::from_le_bytes(hdr[32..40].try_into().expect("len 8")) as usize;
        if meta_len < fb::BANKSTATS_HEADER_LEN + 4 || meta_len > 1 << 30 {
            println!("BANKSTATS|stale|reason=bad-meta-len|path={path}");
            return None;
        }
        let mut meta = hdr;
        meta.resize(meta_len, 0);
        if f.read_exact(&mut meta[fb::BANKSTATS_HEADER_LEN..]).is_err() {
            println!("BANKSTATS|stale|reason=short-meta|path={path}");
            return None;
        }
        let (header, pidents, cols) = match fb::decode_meta(&meta) {
            Ok(v) => v,
            Err(e) => {
                println!("BANKSTATS|stale|reason=decode:{e:?}|path={path}");
                return None;
            }
        };
        // Validity witness: gen + schema fingerprint + full identity vector.
        let ok = header.gen == gen
            && header.schema_fingerprint == m.header.schema_fingerprint
            && header.part_count as usize == m.parts.len()
            && pidents.iter().zip(m.parts.iter()).all(|(a, b)| {
                a.part_no == b.part_no
                    && a.granule_count == b.granule_count
                    && a.band_count == b.band_count
                    && a.rows == b.rows
                    && a.footer_off == b.footer_off
            });
        if !ok {
            println!("BANKSTATS|stale|reason=identity-mismatch|path={path}");
            return None;
        }
        let max_attno = cols.iter().map(|c| c.attno as usize).max().unwrap_or(0);
        let mut by_attno = vec![None; max_attno + 1];
        for (i, c) in cols.iter().enumerate() {
            by_attno[c.attno as usize] = Some(i);
        }
        let n = cols.len();
        println!(
            "BANKSTATS|armed|path={path}|gen={gen}|cols={n}|parts={}",
            header.part_count
        );
        Some(Arc::new(Plane {
            file: f,
            header,
            cols,
            by_attno,
            bufs: (0..n).map(|_| OnceLock::new()).collect(),
            faults: AtomicU64::new(0),
            fault_bytes: AtomicU64::new(0),
        }))
    }

    /// The column payload buffer: ONE pread + crc on first touch.
    fn col_buf(&self, attno: u32) -> Option<(Arc<Vec<u8>>, usize)> {
        let ci = (*self.by_attno.get(attno as usize)?)?;
        let slot = self.bufs[ci].get_or_init(|| {
            let t0 = std::time::Instant::now();
            let e = &self.cols[ci];
            // The per-column payload length is an untrusted on-disk u64
            // (decode_meta bounds nothing but `pad == 0`). Validate the
            // [off, off+len) extent against the sidecar's ACTUAL size —
            // the external truth — before committing any memory, mirroring
            // Plane::open's fail-closed meta_len posture. A crafted length
            // (multi-GiB / wrapping) would otherwise drive an infallible
            // `vec![0u8; len]` whose alloc_zeroed failure aborts the whole
            // single-process server. Any violation → typed refusal → the
            // caller falls back to per-part sections.
            let file_len = match self.file.metadata() {
                Ok(md) => md.len(),
                Err(_) => {
                    println!("BANKSTATS|fault-fail|attno={attno}|reason=stat");
                    return None;
                }
            };
            match e.off.checked_add(e.len) {
                Some(end) if end <= file_len => {}
                _ => {
                    println!(
                        "BANKSTATS|stale|reason=bad-col-len|attno={attno}|off={}|len={}|file={file_len}",
                        e.off, e.len
                    );
                    return None;
                }
            }
            // Bounded by file_len above; the fallible reserve is a final
            // guard so no path can reach an infallible huge allocation.
            let mut buf = Vec::new();
            if buf.try_reserve_exact(e.len as usize).is_err() {
                println!("BANKSTATS|stale|reason=col-alloc|attno={attno}|len={}", e.len);
                return None;
            }
            buf.resize(e.len as usize, 0);
            if self.file.read_exact_at(&mut buf, e.off).is_err() {
                println!("BANKSTATS|fault-fail|attno={attno}");
                return None;
            }
            // Validate shape + crc once (ColPayloadRef holds the law).
            if fb::ColPayloadRef::new(e, self.header.part_count, &buf).is_err() {
                println!("BANKSTATS|stale|reason=col-crc|attno={attno}");
                return None;
            }
            self.faults.fetch_add(1, Ordering::Relaxed);
            self.fault_bytes.fetch_add(e.len, Ordering::Relaxed);
            println!(
                "BANKSTATS|fault|attno={attno}|bytes={}|off={}|ms={:.3}",
                e.len,
                e.off,
                t0.elapsed().as_secs_f64() * 1e3
            );
            Some(Arc::new(buf))
        });
        slot.as_ref().map(|b| (b.clone(), ci))
    }

    /// The part's unwrapped §8.1 Stats body for the column. OUTER `None`
    /// = the plane cannot serve this column (no dir row / fault failure)
    /// — the caller must FALL BACK to per-part sections; inner `None` =
    /// the plane authoritatively says the part has no Stats section.
    pub fn try_stats_body(&self, attno: u32, pi: usize) -> Option<Option<Vec<u8>>> {
        let (buf, ci) = self.col_buf(attno)?;
        let e = &self.cols[ci];
        let view = fb::ColPayloadRef::new_validated(e, self.header.part_count, &buf).ok()?;
        Some(view.stats_body(pi).map(|b| b.to_vec()))
    }

    /// The part's raw §8.6 PartDigest record bytes. Same outer/inner
    /// `None` contract as [`Plane::try_stats_body`].
    pub fn try_digest(&self, attno: u32, pi: usize) -> Option<Option<[u8; 24]>> {
        let (buf, ci) = self.col_buf(attno)?;
        let e = &self.cols[ci];
        let view = fb::ColPayloadRef::new_validated(e, self.header.part_count, &buf).ok()?;
        Some(view.digest(pi).and_then(|b| <[u8; 24]>::try_from(b).ok()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plane_with_col(path: &std::path::Path, entry: fb::ColDirEntry) -> Plane {
        let file = std::fs::File::open(path).expect("open sidecar");
        let header = fb::BankStatsHeader {
            gen: 0,
            schema_fingerprint: 0,
            part_count: 1,
            ncols: 1,
            meta_len: 0,
            flags: 0,
        };
        let attno = entry.attno as usize;
        let mut by_attno = vec![None; attno + 1];
        by_attno[attno] = Some(0);
        Plane {
            file,
            header,
            cols: vec![entry],
            by_attno,
            bufs: vec![OnceLock::new()],
            faults: AtomicU64::new(0),
            fault_bytes: AtomicU64::new(0),
        }
    }

    /// A crafted ColDirEntry whose declared payload length dwarfs (or
    /// wraps past) the sidecar's actual size must be refused BEFORE any
    /// allocation — no `vec![0u8; huge]` abort of the single-process
    /// server. col_buf returns None so the caller falls back to per-part
    /// sections, matching the module's fail-closed posture.
    #[test]
    fn crafted_col_len_refused_without_alloc() {
        // A tiny real sidecar file (16 bytes) as the external truth.
        let path = std::env::temp_dir().join(format!(
            "pgrust-bankstats-test-{}-{}.pgrc2bs",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, [0u8; 16]).expect("write sidecar");

        // Multi-gibibyte length far beyond the 16-byte file.
        let huge = plane_with_col(
            &path,
            fb::ColDirEntry {
                attno: 1,
                flags: 0,
                off: 0,
                len: 1u64 << 62,
                stats_len: 0,
                crc: 0,
            },
        );
        assert!(huge.col_buf(1).is_none(), "huge column length must be refused");

        // Wrapping extent: off + len overflows u64.
        let wrap = plane_with_col(
            &path,
            fb::ColDirEntry {
                attno: 1,
                flags: 0,
                off: u64::MAX - 3,
                len: 64,
                stats_len: 0,
                crc: 0,
            },
        );
        assert!(wrap.col_buf(1).is_none(), "wrapping extent must be refused");

        let _ = std::fs::remove_file(&path);
    }
}
