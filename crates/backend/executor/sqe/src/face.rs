//! The Bank/Faces seam trait (heap-face.md Q7): a statement-scoped scan
//! source the fold stencils consume at LANE grain — the stencils see
//! filled word/validity lanes, never the source. The sealed-bank engine
//! keeps its concrete `Bank` paths untouched; this seam exists for
//! sources whose planes are per-statement scratch (heap v1: granule =
//! fixed block run, visibility resolved at fill under the statement
//! snapshot, ALL caching planes disabled — §1.3's "faces are
//! per-statement scratch" mode).

use crate::bank::Face;
use crate::engine::{PopulatePolicy, SqeConfig};

/// A fill-time failure (never a wrong answer): the face met data it has
/// no lane law for. Surfaced as the statement's typed error by the shell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaceError {
    pub attno: u32,
    pub what: &'static str,
}

/// One filled column lane of a granule: raw datum words positionally
/// aligned to the granule's visible rows, plus the validity bitmap.
/// Word extraction happens at consume time through `Face::word_key`
/// (the fold law's i64 embed).
///
/// [heap rung 3] `Face::Varlena` columns fill BYTE lanes instead: the
/// detoasted payload bytes land in `bytes` and `spans[r] = (off, len)`
/// addresses row `r`'s slice — the engine's text currency (VarPredTerm
/// eval, byte-keyed grouping) consumes `bytes_at`. A byte lane's `words`
/// stays empty; validity rides the same bitmap.
pub struct FillCol {
    pub attno: u32,
    pub face: Face,
    pub words: Vec<u64>,
    /// Byte-lane arena (Varlena faces only).
    bytes: Vec<u8>,
    /// Byte-lane row spans (offset, len) into `bytes`; (0, 0) for NULL /
    /// unwritten (late-mat pass-2 skips) rows.
    spans: Vec<(u32, u32)>,
    /// Validity bitmap (bit set = row NOT NULL). Consulted only when
    /// `all_valid` is false.
    vwords: Vec<u64>,
    nulls: u32,
}

impl FillCol {
    #[inline(always)]
    pub fn all_valid(&self) -> bool {
        self.nulls == 0
    }
    #[inline(always)]
    pub fn row_valid(&self, r: usize) -> bool {
        self.vwords[r >> 6] >> (r & 63) & 1 != 0
    }
    /// Whether this lane carries bytes (Varlena face) rather than words.
    #[inline(always)]
    pub fn is_bytes(&self) -> bool {
        matches!(self.face, Face::Varlena)
    }
    /// Row `r`'s payload bytes (byte lanes; NULL/unwritten rows read the
    /// empty slice — consumers gate on validity first, 3VL).
    #[inline(always)]
    pub fn bytes_at(&self, r: usize) -> &[u8] {
        let (o, l) = self.spans[r];
        &self.bytes[o as usize..o as usize + l as usize]
    }
}

/// Per-granule fill target: per-worker persistent, truncate-refill
/// (law 11 — buffers survive across granules and statements).
#[derive(Default)]
pub struct FaceFill {
    pub rows: u32,
    pub cols: Vec<FillCol>,
}

impl FaceFill {
    pub fn new() -> FaceFill {
        FaceFill::default()
    }

    /// Rebind to a column set (truncate-refill; capacity retained).
    pub fn reset(&mut self, cols: &[(u32, Face)]) {
        self.rows = 0;
        self.cols.truncate(cols.len());
        while self.cols.len() < cols.len() {
            self.cols.push(FillCol {
                attno: 0,
                face: Face::Bool,
                words: Vec::new(),
                bytes: Vec::new(),
                spans: Vec::new(),
                vwords: Vec::new(),
                nulls: 0,
            });
        }
        for (c, &(attno, face)) in self.cols.iter_mut().zip(cols) {
            c.attno = attno;
            c.face = face;
            c.words.clear();
            c.bytes.clear();
            c.spans.clear();
            c.vwords.clear();
            c.nulls = 0;
        }
    }

    /// Append one row's cell to column `ci` (row-major writers scatter
    /// per column; every column must receive every row, in order).
    #[inline(always)]
    pub fn push(&mut self, ci: usize, datum: u64, isnull: bool) {
        let c = &mut self.cols[ci];
        let r = c.words.len();
        c.words.push(if isnull { 0 } else { datum });
        let wi = r >> 6;
        if wi >= c.vwords.len() {
            c.vwords.push(0);
        }
        if isnull {
            c.nulls += 1;
        } else {
            c.vwords[wi] |= 1u64 << (r & 63);
        }
    }

    /// Append one row's payload to BYTE lane `ci` (row-major writers;
    /// `None` = SQL NULL). The byte twin of `push`.
    #[inline(always)]
    pub fn push_bytes(&mut self, ci: usize, payload: Option<&[u8]>) {
        let c = &mut self.cols[ci];
        debug_assert!(c.is_bytes(), "push_bytes on a word lane");
        let r = c.spans.len();
        let wi = r >> 6;
        if wi >= c.vwords.len() {
            c.vwords.push(0);
        }
        match payload {
            Some(b) => {
                let o = c.bytes.len() as u32;
                c.bytes.extend_from_slice(b);
                c.spans.push((o, b.len() as u32));
                c.vwords[wi] |= 1u64 << (r & 63);
            }
            None => {
                c.spans.push((0, 0));
                c.nulls += 1;
            }
        }
    }

    /// Bulk append one page's column slice (the SoA-deform scatter path:
    /// values already column-major; NULL cells store 0 like `push`).
    pub fn extend(&mut self, ci: usize, vals: &[u64], isnull: &[bool]) {
        debug_assert_eq!(vals.len(), isnull.len());
        let c = &mut self.cols[ci];
        let base = c.words.len();
        c.words.extend_from_slice(vals);
        let need = (base + vals.len()).div_ceil(64).max(1);
        if c.vwords.len() < need {
            c.vwords.resize(need, 0);
        }
        for (k, &nul) in isnull.iter().enumerate() {
            let r = base + k;
            if nul {
                c.nulls += 1;
                c.words[r] = 0;
            } else {
                c.vwords[r >> 6] |= 1u64 << (r & 63);
            }
        }
    }

    /// Presize every lane to `rows` cells for POSITIONAL writers (the
    /// pack deform): cells start 0/invalid; a writer that skips a row
    /// (late materialization pass 2) leaves it invalid, and consumers
    /// read only rows their sel covers. `nulls` counts NULL cells the
    /// writer actually wrote, so `all_valid` stays sound for the rows a
    /// sel-driven reader touches.
    pub fn begin_rows(&mut self, rows: u32) {
        self.rows = rows;
        let n = rows as usize;
        let nw = n.div_ceil(64).max(1);
        for c in &mut self.cols {
            if c.is_bytes() {
                c.bytes.clear();
                c.spans.clear();
                c.spans.resize(n, (0, 0));
            } else {
                c.words.clear();
                c.words.resize(n, 0);
            }
            c.vwords.clear();
            c.vwords.resize(nw, 0);
            c.nulls = 0;
        }
    }

    /// One lane's raw parts for a positional bulk writer: (value words,
    /// validity words, written-NULL counter). `begin_rows` first.
    #[inline(always)]
    pub fn lane_mut(&mut self, ci: usize) -> (&mut [u64], &mut [u64], &mut u32) {
        let c = &mut self.cols[ci];
        debug_assert!(!c.is_bytes(), "lane_mut on a byte lane");
        (&mut c.words, &mut c.vwords, &mut c.nulls)
    }

    /// One BYTE lane's raw parts for a positional writer: (arena, spans,
    /// validity words, written-NULL counter). `begin_rows` first; a
    /// writer appends payloads to the arena and points `spans[r]` at
    /// them (any append order — spans are positional).
    #[inline(always)]
    pub fn bytes_lane_mut(
        &mut self,
        ci: usize,
    ) -> (&mut Vec<u8>, &mut [(u32, u32)], &mut [u64], &mut u32) {
        let c = &mut self.cols[ci];
        debug_assert!(c.is_bytes(), "bytes_lane_mut on a word lane");
        (&mut c.bytes, &mut c.spans, &mut c.vwords, &mut c.nulls)
    }

    /// Seal the granule at `rows` visible rows (every column filled).
    pub fn seal(&mut self, rows: u32) {
        self.rows = rows;
        for c in &self.cols {
            if c.is_bytes() {
                debug_assert_eq!(c.spans.len(), rows as usize, "ragged face fill");
            } else {
                debug_assert_eq!(c.words.len(), rows as usize, "ragged face fill");
            }
        }
    }
}

/// Set validity bits `[start, end)` (word-blit with edge masks — the
/// no-null page fast arm of the pack deform).
#[inline]
pub fn set_valid_range(vwords: &mut [u64], start: usize, end: usize) {
    if start >= end {
        return;
    }
    let (w0, b0) = (start >> 6, start & 63);
    let (w1, b1) = ((end - 1) >> 6, (end - 1) & 63);
    let lo = !0u64 << b0;
    let hi = !0u64 >> (63 - b1);
    if w0 == w1 {
        vwords[w0] |= lo & hi;
    } else {
        vwords[w0] |= lo;
        for w in &mut vwords[w0 + 1..w1] {
            *w = !0;
        }
        vwords[w1] |= hi;
    }
}

/// The seam contract (engine audit §2.1, reduced to the v1 fold planes):
/// geometry from metadata alone; lanes at fill; witnesses that survive
/// any snapshot (catalog constraints, type domains). No dict face, no
/// stats/zone face, no word-stream face — `None`/must-scan by
/// construction (heap-face.md §2.3's degradation map).
pub trait ScanFace {
    /// Granule count (heap: ceil(nblocks / B) block runs, a pure
    /// function of nblocks at scan open).
    fn n_units(&self) -> usize;
    /// The decode face of a column (heap: minted from the catalog — the
    /// face owns the SIGNED-flag risk the sealed writer normally owns).
    fn face(&self, attno: u32) -> Face;
    /// Null-freedom witness sound under ANY snapshot (heap: catalog
    /// `attnotnull` only; unknown => false = Mixed).
    fn null_free(&self, attno: u32) -> bool;
    /// Exact total rows under the statement snapshot, if the source can
    /// witness it without a scan (heap: None — reltuples is an estimate;
    /// the MetadataAnswer refusal law, heap-face.md §3).
    fn rows_total(&self) -> Option<u64>;
    /// Fill granule `unit`'s lanes for `cols`: visibility resolved here,
    /// once, under the statement snapshot — the engine never sees an
    /// invisible tuple. `out.rows` = actual visible rows (capacity
    /// geometry publishes the true count at fill; short granules legal).
    fn fill(&mut self, unit: usize, cols: &[u32], out: &mut FaceFill)
        -> Result<(), FaceError>;
}

/// The heap cache law (heap-face.md §1.3): nothing outlives the
/// statement that produced it. A face-fold run refuses any config with a
/// persistent plane armed — the born-RED leg proves this check is
/// load-bearing.
pub fn heap_lawful(cfg: &SqeConfig) -> bool {
    cfg.populate == PopulatePolicy::Never
        && !cfg.stats_cache
        && !cfg.vw_cache
        && !cfg.fpcache
}

impl SqeConfig {
    /// The v1 heap-face configuration of record: every caching plane
    /// disabled/per-statement (condcache Never, stats/walk/sma
    /// per-statement, no verdict tables, no fp planes).
    pub fn heap_v1(threads: usize) -> SqeConfig {
        SqeConfig {
            threads,
            populate: PopulatePolicy::Never,
            stats_cache: false,
            vw_cache: false,
            fpcache: false,
            ..SqeConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_v1_config_is_lawful() {
        assert!(heap_lawful(&SqeConfig::heap_v1(4)));
    }

    #[test]
    fn wrongly_enabled_caches_are_unlawful() {
        let mut c = SqeConfig::heap_v1(4);
        c.stats_cache = true;
        assert!(!heap_lawful(&c));
        let mut c = SqeConfig::heap_v1(4);
        c.populate = PopulatePolicy::Second;
        assert!(!heap_lawful(&c));
        let mut c = SqeConfig::heap_v1(4);
        c.vw_cache = true;
        assert!(!heap_lawful(&c));
        let mut c = SqeConfig::heap_v1(4);
        c.fpcache = true;
        assert!(!heap_lawful(&c));
    }

    #[test]
    fn face_fill_truncate_refill_and_validity() {
        let mut f = FaceFill::new();
        f.reset(&[(1, Face::SignedWord(4)), (2, Face::SignedWord(8))]);
        f.push(0, 7, false);
        f.push(1, 0, true);
        f.push(0, 5, false);
        f.push(1, 9, false);
        f.seal(2);
        assert!(f.cols[0].all_valid());
        assert!(!f.cols[1].all_valid());
        assert!(!f.cols[1].row_valid(0));
        assert!(f.cols[1].row_valid(1));
        // refill: capacity retained, state cleared.
        f.reset(&[(1, Face::SignedWord(4))]);
        assert_eq!(f.cols.len(), 1);
        f.push(0, 3, false);
        f.seal(1);
        assert!(f.cols[0].all_valid());
        assert_eq!(f.cols[0].words, vec![3]);
    }
}
