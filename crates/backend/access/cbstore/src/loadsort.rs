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
//! reach a run (cbstore refuses NULLs at ingest).

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

/// Bounded (key, row) accumulator. `sort()` orders entries by memcmp key —
/// the total order proven equal to the recipe order (no ties on the
/// benchmark key set; the identity gate stays the enforcement).
pub struct SortBatch {
    key_w: usize,
    arena: Vec<u8>,
    /// (entry offset, entry len incl. key) in arena.
    index: Vec<(u64, u32)>,
}

impl SortBatch {
    pub fn new(key_w: usize) -> SortBatch {
        SortBatch { key_w, arena: Vec::new(), index: Vec::new() }
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

    pub fn sort(&mut self) {
        let (arena, kw) = (&self.arena, self.key_w);
        self.index.sort_unstable_by(|a, b| {
            Self::key_of(arena, kw, *a).cmp(Self::key_of(arena, kw, *b))
        });
    }

    /// Write the (sorted) batch as one run and clear the batch for reuse.
    pub fn spill_run(&mut self, path: &std::path::Path) -> PgResult<()> {
        let f = std::fs::File::create(path).map_err(|e| io_err("run create", e))?;
        let mut w = BufWriter::with_capacity(1 << 20, f);
        for &(off, len) in &self.index {
            let e = &self.arena[off as usize..off as usize + len as usize];
            let rowlen = (len as usize - self.key_w) as u32;
            w.write_all(&e[..self.key_w]).map_err(|e| io_err("run write", e))?;
            w.write_all(&rowlen.to_le_bytes()).map_err(|e| io_err("run write", e))?;
            w.write_all(&e[self.key_w..]).map_err(|e| io_err("run write", e))?;
        }
        w.flush().map_err(|e| io_err("run flush", e))?;
        self.arena.clear();
        self.index.clear();
        Ok(())
    }
}

// ---- run reader + k-way merge ----------------------------------------------

/// loadcommit C2a: non-blocking kernel readahead hint for a run file's
/// upcoming window (pure hint — zero effect on the bytes read). Linux
/// only; a no-op elsewhere.
#[cfg(target_os = "linux")]
fn fadvise_willneed(f: &std::fs::File, off: u64, len: u64) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::posix_fadvise(
            f.as_raw_fd(),
            off as libc::off_t,
            len as libc::off_t,
            libc::POSIX_FADV_WILLNEED,
        );
    }
}
#[cfg(not(target_os = "linux"))]
fn fadvise_willneed(_f: &std::fs::File, _off: u64, _len: u64) {}

struct RunReader {
    r: BufReader<std::fs::File>,
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
        let f = std::fs::File::open(path).map_err(|e| io_err("run open", e))?;
        let mut rr = RunReader {
            r: BufReader::with_capacity(1 << 20, f),
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
    /// first entry, so `pos` is live).
    fn set_fadvise(&mut self, win: u64) {
        if win == 0 {
            return;
        }
        self.fadv_win = win;
        fadvise_willneed(self.r.get_ref(), self.pos, win);
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
        let end = self.pos + self.fadv_win;
        if end > self.advised_to {
            fadvise_willneed(self.r.get_ref(), self.advised_to, end - self.advised_to);
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

/// Heap entry: min-heap by (key, run index) — the run index tiebreak makes
/// the merge deterministic even under key ties (none exist on the
/// benchmark key set; determinism is still a gate-friendly property).
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

    /// (advance seconds — 0.0 unless timed, run bytes consumed).
    pub fn fill_stats(&self) -> (f64, u64) {
        (self.stats.t_advance.as_secs_f64(), self.stats.bytes)
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
    readers: Vec<RunReader>,
    /// Tournament tree over k = readers.len() leaves: internal node x in
    /// 1..k holds the LOSER run index of the match played there; node x's
    /// children are 2x and 2x+1; run i's leaf is node k+i. `winner` is the
    /// current overall winner (u32::MAX when k == 0).
    losers: Vec<u32>,
    winner: u32,
    stats: FillStats,
}

impl RunMergeV2 {
    pub fn open(paths: &[std::path::PathBuf], key_w: usize) -> PgResult<RunMergeV2> {
        let mut readers = Vec::with_capacity(paths.len());
        for p in paths {
            readers.push(RunReader::open(p, key_w)?);
        }
        let k = readers.len();
        let mut losers = vec![u32::MAX; k.max(1)];
        let winner = if k == 0 {
            u32::MAX
        } else {
            Self::build(&readers, &mut losers, k, 1)
        };
        Ok(RunMergeV2 { readers, losers, winner, stats: FillStats::default() })
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

    /// (advance seconds — 0.0 unless timed, run bytes consumed).
    pub fn fill_stats(&self) -> (f64, u64) {
        (self.stats.t_advance.as_secs_f64(), self.stats.bytes)
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
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
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
        let codec = RowCodec::new(vec![ColType::I32, ColType::I64, ColType::Text]);
        let keys = [(0u16, CbSortKeyKind::Int32), (1, CbSortKeyKind::Int64)];
        let kw = fixed_key_width(&keys).unwrap();
        let mut x: u64 = seed;
        let mut step = || {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x
        };
        let mut keep = Vec::new();
        let mut paths = Vec::new();
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
            let p = dir.join(format!("run-{r}"));
            batch.spill_run(&p).unwrap();
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
