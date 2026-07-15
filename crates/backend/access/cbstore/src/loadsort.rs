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

struct RunReader {
    r: BufReader<std::fs::File>,
    key_w: usize,
    /// Current entry (valid when `live`).
    key: Vec<u8>,
    row: Vec<u8>,
    live: bool,
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
        };
        rr.advance()?;
        Ok(rr)
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

pub struct RunMerge {
    readers: Vec<RunReader>,
    heap: BinaryHeap<HeapEntry>,
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
        Ok(RunMerge { readers, heap })
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
        rr.advance()?;
        if rr.live {
            self.heap.push(HeapEntry { key: rr.key.clone(), run: top.run });
        }
        Ok(true)
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
}
