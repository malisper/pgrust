//! Morsel-parallel COPY FROM (load-speed lane L2/L3 —
//! docs/design/load-speed-2026-07.md §5 lever 1, the measured 7.33x@k8
//! ceiling with flat CPU).
//!
//! Shape (the ClickHouse pipeline translated to this runtime):
//!  1. SEGMENTATOR (leader): stream the COPY input, find row boundaries
//!     cheaply (memchr terminator scan + backslash-run parity — never the
//!     full parse), publish whole-RG chunk descriptors (65,536 rows each —
//!     RG seams fall exactly where the serial writer's would) as claimable
//!     granules of a runtime [`runtime::StreamSource`].
//!  2. WORKERS (full-identity parallel helpers, the vacuum-morsels
//!     ceremony): claim chunks off the pinned RG, parse+convert through the
//!     UNCHANGED per-chunk COPY machinery (CopySrc::Chunk), run the serial
//!     path's exec_constraints, and encode whole RGs via
//!     [`pgrcolumnar::RgChunkEncoder`].
//!  3. ORDERED COMMITTER (leader): commit encoded RGs in INPUT ORDER into
//!     the one [`pgrcolumnar::CbWriter`] — the part is BYTE-IDENTICAL to a
//!     serial COPY of the same stream (the acceptance oracle).
//!
//! Error semantics: workers record chunk-indexed errors (context lines
//! attached with the worker's exact cur_lineno); chunks past the lowest
//! erroring index drain; the leader re-raises the minimum-index error after
//! completion — first-error-in-input-order, exactly like serial.
//!
//! Admission is FAIL-CLOSED (every refusal is today's serial COPY,
//! byte-identically): PGRUST_PARALLEL_COPY=1 + a live runtime; pgrcolumnar AM
//! only; text (non-CSV, non-binary) format; no triggers, no WHERE clause,
//! no defaults, no ON_ERROR ignore, no header, no transcoding, no
//! cluster_key, no indexes, no generated columns.

use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use pgsync::{Mutex, OnceLock};

use elog::ereport;
use mcx::{vec_from_elem_in, Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult, ERROR, WARNING};
use types_fmgr::FmgrInfo;
use types_rel::Relation;

use backend_progress::progress::{PROGRESS_COPY_BYTES_PROCESSED, PROGRESS_COPY_TUPLES_PROCESSED};
use backend_progress::pgstat_progress_update_param;

use crate::from::{copy_from_error_context, CopyFromState, CopySrc};
use crate::fromparse::EolType;
use crate::{CopyFormatOptions, CopyHeaderChoice, CopyOnErrorChoice};

// ---------------------------------------------------------------------------
// Knobs (env: new real GUCs are barred by pg_settings byte-identity — the
// runtime-lane precedent).
// ---------------------------------------------------------------------------

fn flag_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARALLEL_COPY").is_ok_and(|v| v.trim() == "1"))
}

/// Engagement/refusal trace (PGRUST_PARALLEL_COPY_TRACE=1): the e2e
/// battery's engagement oracle channel. Default-off, zero cost.
fn ptrace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARALLEL_COPY_TRACE").is_ok_and(|v| v.trim() == "1"))
}

fn ptrace(msg: &str) {
    if ptrace_enabled() {
        eprintln!("parallel-copy: {msg}");
    }
}

macro_rules! refuse {
    ($why:expr) => {{
        ptrace(&format!("refused: {}", $why));
        return Ok(None);
    }};
}

/// load-r2 L3-1: PGRUST_PARALLEL_COPY_SORT=1 lets a PGRUST_COPY_PRESORT
/// load engage the PARALLEL sort pipeline (workers spill memcmp-key runs,
/// leader k-way merges into the plain writer). Default OFF: presort loads
/// refuse to the serial sort-on-ingest path verbatim.
fn sort_flag_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARALLEL_COPY_SORT").is_ok_and(|v| v.trim() == "1"))
}

/// Per-worker in-memory (key,row) batch budget before a run spill
/// (PGRUST_PARALLEL_COPY_SORT_MEM, MB; default 256, floor 1 — the floor
/// exists for the e2e battery's multi-run coverage, not for production).
fn sort_budget() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_SORT_MEM")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(256)
            .max(1)
            * (1 << 20)
    })
}

/// Worker gang size: PGRUST_PARALLEL_COPY_DOP, default = the runtime pool's
/// execution width, clamped to the external-lane budget.
fn dop(rt: &runtime::Runtime) -> i32 {
    static N: OnceLock<Option<u64>> = OnceLock::new();
    let req = *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_DOP").ok().and_then(|v| v.trim().parse().ok())
    });
    let d = req.unwrap_or(rt.config().workers as u64);
    d.clamp(1, (runtime::MAX_EXTERNAL_LANES as u64).min(32)) as i32
}

/// In-flight chunk window (published − committed): bounds leader read-ahead
/// memory (a chunk is ~1 RG of raw input, ~50 MB on wide-events analytics rows).
fn window(k: i32) -> u64 {
    static N: OnceLock<Option<u64>> = OnceLock::new();
    let req = *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_WINDOW").ok().and_then(|v| v.trim().parse().ok())
    });
    req.unwrap_or((2 * k as u64) + 4).max(2)
}

/// Sort-merge encode threads (PGRUST_PARALLEL_COPY_SORT_ENCODERS,
/// default = the COPY dop).
fn sort_encoders(rt: &runtime::Runtime) -> usize {
    static N: OnceLock<Option<usize>> = OnceLock::new();
    let req = *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_SORT_ENCODERS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
    });
    req.unwrap_or(dop(rt) as usize).clamp(1, 32)
}

/// load-r3 M2: column-sharded stitch pool threads
/// (PGRUST_PARALLEL_COPY_STITCH_POOL=<n>, default 0 = inline stitch —
/// measured 2026-07-15: inline stitch = 44.7 s intern on the ordered-commit
/// path + 35.5 s rank/blob at finish, of the 165 s 100M parsort wall).
fn stitch_pool_threads() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_STITCH_POOL")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0)
    })
}

/// loadcommit C1: PGRUST_PARALLEL_COPY_FILL_V2=1 — loser-tree merge fill
/// (zero per-row allocation, single-copy row emission). Default OFF; the
/// emitted row sequence is byte-identical by construction (loadsort.rs
/// `v2_less` + the v2_matches_heap_reference oracle).
fn fill_v2() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_FILL_V2").is_ok_and(|v| v.trim() == "1")
    })
}

/// loadcommit C0: PGRUST_PARALLEL_COPY_FILL_SPLIT=1 — per-row advance
/// (run read+decode) timing inside the merge fill. Diagnostic arm only;
/// default OFF = one untaken branch per row.
fn fill_split() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_FILL_SPLIT").is_ok_and(|v| v.trim() == "1")
    })
}

/// loadcommit C2a: PGRUST_PARALLEL_COPY_FILL_FADV=<MB> — per-run sliding
/// POSIX_FADV_WILLNEED window on the merge fill's run files (kernel
/// readahead overlapping the advance I/O with merge CPU). Pure hint,
/// zero effect on bytes; default 0 = off; capped 64 MB/run. Page-cache
/// pressure = runs x window (291 x 4 MB ≈ 1.2 GB at 100M defaults).
fn fill_fadv_bytes() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_FILL_FADV")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
            .min(64)
            * (1 << 20)
    })
}

/// loadcommit C2b: PGRUST_PARALLEL_COPY_FILL_PREFETCH=<threads> — explicit
/// bounded run prefetch for the merge fill (512 KB chunks, capacity-2
/// channels, consume-on-arrival: the shape the C2a fadvise refutation
/// points at). Requires FILL_V2=1; default 0 = off.
fn fill_prefetch() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_FILL_PREFETCH")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0)
            .min(16)
    })
}

/// loadcommit RUNLZ4: PGRUST_COPY_RUNLZ4=1 — lz4-compress the sort run
/// files (spill write side + merge read side; ~74 -> ~30 GB of NVMe
/// traffic EACH WAY at 100M — the bandwidth-law lever). Requires
/// FILL_V2=1 (the decode seam lives in the V2 sources); default OFF,
/// fail-closed: without V2 the spill/merge both stay raw.
fn run_lz4() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_COPY_RUNLZ4").is_ok_and(|v| v.trim() == "1"))
}

/// The single spill+merge mode decision (both sides MUST agree).
fn run_lz4_effective() -> bool {
    run_lz4() && fill_v2()
}

/// GL-PARQUET-1 inc-2: PGRUST_PARQUET_PARALLEL=1 lets a FORMAT 'parquet'
/// COPY engage row-group-major parallel decode INSIDE the parallel sort
/// pipeline (workers decode whole parquet row groups and spill sorted runs;
/// the merge/fill/stitch back half is the byte-proven text-path machinery).
/// Default OFF: parquet loads refuse to the serial reader verbatim.
/// Requires the sort mode (a presort key + PGRUST_PARALLEL_COPY_SORT=1) —
/// order-preserving parquet parallelism would move cbstore RG seams off the
/// serial writer's and is refused by design.
fn parquet_parallel_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARQUET_PARALLEL").is_ok_and(|v| v.trim() == "1"))
}

/// In-flight decode budget, compressed bytes (PGRUST_PARQUET_BUDGET_MB,
/// default 2048): each worker holds at most one row group's compressed
/// chunks, so the launched gang is clamped to budget / max-RG-bytes.
fn parquet_budget_bytes() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARQUET_BUDGET_MB")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(2048)
            .max(64)
            * (1 << 20)
    })
}


/// copyfast lever 1: PGRUST_PARALLEL_COPY_SORT_MEMRUNS — keep sorted runs
/// in MEMORY (same lz4 frames as the run files) instead of spilling, so a
/// fitting load sorts in one pass: no run write, no merge re-read. Budgeted
/// against ACTUAL headroom, never a blind constant; a budget-refused run
/// falls to the file spill (bytes identical by construction), so degradation
/// is graceful mid-load. Requires FILL_V2+RUNLZ4 (the composed load stack);
/// default OFF — the load vehicle arms it explicitly.
///   unset | "0"        off
///   "1" | "auto"       auto-size from headroom (cgroup v2/v1, meminfo)
///   "<N>" (MB)         explicit cap
#[derive(Clone, Copy)]
enum MemRuns {
    Off,
    Auto,
    CapMb(u64),
}

fn memruns_knob() -> MemRuns {
    static M: OnceLock<MemRuns> = OnceLock::new();
    *M.get_or_init(|| match std::env::var("PGRUST_PARALLEL_COPY_SORT_MEMRUNS") {
        Ok(v) => match v.trim() {
            "" | "0" => MemRuns::Off,
            "1" | "auto" => MemRuns::Auto,
            v => v.parse::<u64>().map(MemRuns::CapMb).unwrap_or(MemRuns::Off),
        },
        Err(_) => MemRuns::Off,
    })
}

/// GL-LOADDET-1: PGRUST_PARALLEL_COPY_SORT_DETERMINISTIC — make the loaded
/// bytes a function of the INPUT ALONE. **DEFAULT ON**; `0`/`off`/`false` is
/// the kill switch back to the legacy bytes.
///
/// Two independent races decided the output byte image before this:
///
///  1. **Run boundaries.** A worker's sort batch carried ACROSS the morsels it
///     won from the shared claim cursor and was cut when accumulated bytes
///     crossed the run budget — so which rows shared a run was a function of
///     which morsels that worker happened to win, i.e. of wall-clock. Armed,
///     a run never spans a morsel: batches are flushed at every morsel
///     boundary, so a run's content is exactly (a deterministic prefix split
///     of) one morsel's rows, whatever the worker count or the claim order.
///  2. **Registry order = the merge's tiebreak index.** Runs were appended to
///     a shared Vec as each worker finished one. Armed, the merge sorts the
///     registry by the run's input coordinate `(morsel, sub)` instead, which
///     is input-major (a strict strengthening of worker-major: worker-major is
///     only reproducible at a FIXED worker count, because the worker->morsel
///     map is not).
///
/// With the batch sort made stable on arrival order (`SortBatch::sort`), the
/// three together make the emitted row order the stable sort of the input by
/// the presort key — reproducible across repeats, worker counts, run-home
/// (memory/file) postures, encoder counts and node timing. Rows that are NOT
/// key-tied were never at risk: a tie-free key pins the permutation by itself,
/// which is why every pre-existing byte-identity gate is tie-free and stayed
/// green through the whole non-deterministic era.
fn sort_deterministic() -> bool {
    static D: OnceLock<bool> = OnceLock::new();
    *D.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_PARALLEL_COPY_SORT_DETERMINISTIC")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

/// Memory headroom for the auto-sized mem-run budget: the container-aware
/// cgroup-hierarchy walk in memheadroom.rs (GL-COPYFAST-1 §3 defect fix —
/// a leaf reading "max" no longer falls through to node meminfo when an
/// ancestor slice carries the limit or the hierarchy is namespaced-hidden).
/// None = no trustworthy signal (auto stays off; the explicit-MB knob is
/// the trump and still works).
fn memory_headroom_bytes() -> Option<u64> {
    crate::memheadroom::memory_headroom_bytes()
}

/// copyfast lever 3: PGRUST_COPY_ANALYZE_INLINE=1 — analyze-during-load.
/// The merge pump reservoir-samples the sorted row stream (the data is
/// already transiting RAM) and, after the part publishes, the standard
/// ANALYZE compute/write half runs on that sample — same pg_statistic /
/// relstats content a post-load ANALYZE would produce, without re-reading
/// the table (the post-load step drops to plain VACUUM). Requires the
/// parallel sort pipeline (the merge pump is the sampling site); default
/// OFF, fail-closed: refused postures leave statistics to a later ANALYZE
/// exactly as today. Part bytes are untouched by construction — sampling
/// only copies row images out of the stream.
fn analyze_inline_flag() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_COPY_ANALYZE_INLINE").is_ok_and(|v| v.trim() == "1")
    })
}

/// Lever-3 per-statement plan: targrows resolved via the ANALYZE examine
/// pass at admission (catalog access, pre-parallel-mode) and the reservoir
/// seed drawn from the backend PRNG on the leader (the pump thread never
/// touches backend state).
#[derive(Clone, Copy)]
struct InlineAnalyzePlan {
    targrows: i32,
    seed: u64,
}

/// copyfast lever 3: Vitter reservoir over the merge pump's row stream —
/// C acquire_sample_rows' per-row loop verbatim (keep-below-target, then
/// skip-S / replace-random-slot), carrying (ord, row image) so the final
/// sort restores stream order (C sorts its sample by TID for the same
/// reason: the correlation stat reads rows order). Cost per non-sampled
/// row: one float compare + decrement.
struct StreamSampler {
    targrows: usize,
    rstate: commands_analyze::sampling::ReservoirStateData,
    sample: Vec<(u64, Vec<u8>)>,
    samplerows: f64,
    rowstoskip: f64,
    ord: u64,
}

impl StreamSampler {
    fn new(targrows: i32, seed: u64) -> StreamSampler {
        StreamSampler {
            targrows: targrows.max(1) as usize,
            rstate: commands_analyze::sampling::reservoir_init_selection_state(
                seed,
                targrows.max(1) as u32,
            ),
            sample: Vec::new(),
            samplerows: 0.0,
            rowstoskip: -1.0,
            ord: 0,
        }
    }

    #[inline]
    fn offer(&mut self, row: &[u8]) {
        if self.sample.len() < self.targrows {
            self.sample.push((self.ord, row.to_vec()));
        } else {
            if self.rowstoskip < 0.0 {
                self.rowstoskip = commands_analyze::sampling::reservoir_get_next_s(
                    &mut self.rstate,
                    self.samplerows,
                    self.targrows as u32,
                );
            }
            if self.rowstoskip <= 0.0 {
                let k = (self.targrows as f64
                    * commands_analyze::sampling::sampler_random_fract(
                        &mut self.rstate.randstate,
                    )) as usize;
                debug_assert!(k < self.targrows);
                let slot = &mut self.sample[k];
                slot.0 = self.ord;
                slot.1.clear();
                slot.1.extend_from_slice(row);
            }
            self.rowstoskip -= 1.0;
        }
        self.ord += 1;
        self.samplerows += 1.0;
    }

    /// Stream order restored (replacements land at random slots).
    fn finish(mut self) -> Vec<Vec<u8>> {
        self.sample.sort_unstable_by_key(|&(o, _)| o);
        self.sample.into_iter().map(|(_, img)| img).collect()
    }
}

/// Resolve the mem-run budget in bytes at admission (0 = not engageable).
fn memrun_budget(k: i32) -> u64 {
    match memruns_knob() {
        MemRuns::Off => 0,
        MemRuns::CapMb(n) => n << 20,
        MemRuns::Auto => {
            let Some(headroom) = memory_headroom_bytes() else {
                ptrace("memruns auto refused: no memory headroom signal");
                return 0;
            };
            // Reserve what the rest of the pipeline provably holds: the
            // worker batch arenas (k x SORT_MEM raw, capacity retained
            // across spills), x1.5 for the transient compression frames,
            // plus a fixed floor for the writer/encoder/stitch estate.
            let reserve = (k as u64) * (sort_budget() as u64) * 3 / 2 + (2u64 << 30);
            let budget = headroom.saturating_sub(reserve) * 4 / 5;
            if budget < (1 << 30) {
                ptrace(&format!("memruns auto refused: headroom {headroom} reserve {reserve}"));
                return 0;
            }
            budget
        }
    }
}

/// Segmentator read-block bytes.
const READ_BLOCK: usize = 4 << 20;

/// File-source engagement floor (bytes): tiny loads keep the serial path
/// (frontend streams engage regardless — their size is unknowable).
fn file_floor() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_PARALLEL_COPY_MIN_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(4 << 20)
    })
}

// ---------------------------------------------------------------------------
// Chunk plumbing: descriptors over refcounted read buffers.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ChunkSeg {
    pub(crate) buf: Arc<Vec<u8>>,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// Worker-side sequential reader over a chunk's segments (CopySrc::Chunk).
pub(crate) struct ChunkCursor {
    segs: Vec<ChunkSeg>,
    seg: usize,
    off: usize,
}

impl ChunkCursor {
    pub(crate) fn new(segs: Vec<ChunkSeg>) -> ChunkCursor {
        let off = segs.first().map(|s| s.start).unwrap_or(0);
        ChunkCursor { segs, seg: 0, off }
    }

    pub(crate) fn read(&mut self, dst: &mut [u8]) -> usize {
        let mut filled = 0usize;
        while filled < dst.len() && self.seg < self.segs.len() {
            let s = &self.segs[self.seg];
            let avail = s.end - self.off;
            if avail == 0 {
                self.seg += 1;
                self.off = self.segs.get(self.seg).map(|s| s.start).unwrap_or(0);
                continue;
            }
            let n = avail.min(dst.len() - filled);
            dst[filled..filled + n].copy_from_slice(&s.buf[self.off..self.off + n]);
            self.off += n;
            filled += n;
        }
        filled
    }
}

struct ChunkDesc {
    /// 1-based line number of the chunk's first row (workers preset
    /// cur_lineno so error contexts carry exact input line numbers).
    first_lineno: u64,
    segs: Vec<ChunkSeg>,
}

// ---------------------------------------------------------------------------
// The segmentator: cheap row-boundary scan (TEXT format).
// ---------------------------------------------------------------------------
//
// Rules (mirror copy_read_line_text, non-CSV):
//  * a terminator byte is a row boundary iff the run of consecutive
//    backslashes immediately before it has EVEN length (a backslash consumes
//    the next byte; consumption chains only inside contiguous runs);
//  * EOL style is decided by the FIRST unescaped terminator (Nl / Cr /
//    Crnl); later inconsistent terminators are NOT boundaries here — the
//    owning worker raises the exact serial error (literal newline/carriage
//    return) at the exact line;
//  * a line starting with `\.` ends the input: the marker LINE goes into
//    the final chunk (the worker replays every marker validation error);
//    bytes past it are never segmented (frontend streams drain protocol-
//    level, files stop early — serial behavior).

#[derive(Clone, Copy, PartialEq)]
enum SegEol {
    Unknown,
    Nl,
    Cr,
    Crnl,
}

struct Segmentator {
    eol: SegEol,
    rows_per_chunk: u32,
    // Current chunk accumulation.
    segs: Vec<ChunkSeg>,
    rows: u32,
    first_lineno: u64,
    rows_total: u64,
    // Cross-buffer carry state.
    /// Backslash run length ending at the previous buffer's last byte.
    trailing_bs: u32,
    /// First up-to-2 bytes of the in-progress line (for `\.` detection when
    /// the line started in an earlier buffer). len = bytes captured so far.
    line_head: [u8; 2],
    line_head_len: u8,
    /// Bytes seen in the in-progress line (caps line_head capture).
    line_len: u64,
    /// Previous buffer ended in '\r' with EOL Unknown pending the
    /// lookahead byte (Cr vs Crnl decision).
    pending_cr: bool,
    /// Decided-Crnl mode: previous buffer's last byte was an UNESCAPED
    /// '\r' — a '\n' at the next buffer's start pairs into a boundary.
    prev_ended_cr: bool,
    /// Escape state for the detect phase (odd backslash run in progress).
    detect_esc: bool,
    /// End-of-copy marker seen: stop segmenting.
    eoc: bool,
}

impl Segmentator {
    fn new(rows_per_chunk: u32) -> Segmentator {
        Segmentator {
            eol: SegEol::Unknown,
            rows_per_chunk,
            segs: Vec::new(),
            rows: 0,
            first_lineno: 1,
            rows_total: 0,
            trailing_bs: 0,
            line_head: [0; 2],
            line_head_len: 0,
            line_len: 0,
            pending_cr: false,
            prev_ended_cr: false,
            detect_esc: false,
            eoc: false,
        }
    }

    fn eol_type(&self) -> EolType {
        match self.eol {
            SegEol::Unknown => EolType::Unknown,
            SegEol::Nl => EolType::Nl,
            SegEol::Cr => EolType::Cr,
            SegEol::Crnl => EolType::Crnl,
        }
    }

    /// Backslash-run parity before `pos` (run may extend into the previous
    /// buffer iff it reaches offset `base`).
    fn bs_parity_even(&self, data: &[u8], base: usize, pos: usize) -> bool {
        let mut k = pos;
        while k > base && data[k - 1] == b'\\' {
            k -= 1;
        }
        let mut run = (pos - k) as u32;
        if k == base {
            run += self.trailing_bs;
        }
        run % 2 == 0
    }

    /// The in-progress line's first two bytes, given the line started at
    /// `start` in `data` (or earlier — then line_head carries them).
    fn line_first2(&self, data: &[u8], start: usize, upto: usize) -> [Option<u8>; 2] {
        let mut out = [None, None];
        let mut n = 0usize;
        for i in 0..self.line_head_len as usize {
            out[n] = Some(self.line_head[i]);
            n += 1;
        }
        let mut i = start;
        while n < 2 && i < upto {
            out[n] = Some(data[i]);
            n += 1;
            i += 1;
        }
        out
    }

    /// Feed one read buffer (`data[..len]` of `buf`). Emits completed chunk
    /// descriptors into `out`. Returns the number of bytes CONSUMED — less
    /// than `len` only when the end-of-copy marker line ended inside the
    /// buffer (the rest of the stream is not COPY data).
    fn feed(&mut self, buf: &Arc<Vec<u8>>, len: usize, out: &mut Vec<ChunkDesc>) -> usize {
        assert!(!self.eoc, "feed after the end-of-copy marker");
        let data = &buf[..len];
        // Start of the not-yet-chunked region of THIS buffer.
        let mut chunk_start = 0usize;
        // Start of the in-progress line within this buffer (line_head covers
        // bytes from earlier buffers).
        let mut line_start = 0usize;
        let mut i = 0usize;

        // Resolve a pending CR lookahead from the previous buffer (EOL was
        // Unknown; the \r at the edge decides Cr vs Crnl by this byte).
        if self.pending_cr {
            self.pending_cr = false;
            self.eol = if data.first() == Some(&b'\n') { SegEol::Crnl } else { SegEol::Cr };
            if self.eol == SegEol::Crnl {
                i = 1;
            }
            // The \r (+\n) terminated a row.
            if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                return i;
            }
        } else if self.prev_ended_cr && self.eol == SegEol::Crnl && data.first() == Some(&b'\n')
        {
            // Decided-Crnl mode, \r|\n split across the buffer edge.
            self.prev_ended_cr = false;
            i = 1;
            if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                return i;
            }
        }
        self.prev_ended_cr = false;

        while i < len {
            match self.eol {
                SegEol::Unknown => {
                    // Detect phase: scalar scan honoring escapes until the
                    // first unescaped terminator.
                    let b = data[i];
                    if self.detect_esc {
                        self.detect_esc = false;
                        i += 1;
                        continue;
                    }
                    match b {
                        b'\\' => {
                            self.detect_esc = true;
                            i += 1;
                        }
                        b'\n' => {
                            self.eol = SegEol::Nl;
                            i += 1;
                            if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                                return i;
                            }
                        }
                        b'\r' => {
                            if i + 1 < len {
                                self.eol =
                                    if data[i + 1] == b'\n' { SegEol::Crnl } else { SegEol::Cr };
                                i += if self.eol == SegEol::Crnl { 2 } else { 1 };
                                if self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out) {
                                    return i;
                                }
                            } else {
                                // Buffer edge: defer the Cr/Crnl decision.
                                self.pending_cr = true;
                                i += 1;
                            }
                        }
                        _ => i += 1,
                    }
                }
                SegEol::Nl | SegEol::Crnl => {
                    let Some(j) = memchr::memchr(b'\n', &data[i..len]) else {
                        break;
                    };
                    let pos = i + j;
                    i = pos + 1;
                    let boundary = match self.eol {
                        SegEol::Nl => self.bs_parity_even(data, 0, pos),
                        SegEol::Crnl => {
                            // \r\n pair with even parity before the \r. A
                            // lone \n (or an escaped \r) is data here — the
                            // owning worker errors serial-exactly.
                            if pos == 0 {
                                // \n at buffer start: the \r (if any) was the
                                // previous buffer's last byte — handled by
                                // pending_cr above, so this \n is bare.
                                false
                            } else {
                                data[pos - 1] == b'\r' && self.bs_parity_even(data, 0, pos - 1)
                            }
                        }
                        _ => unreachable!(),
                    };
                    if boundary
                        && self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out)
                    {
                        return i;
                    }
                }
                SegEol::Cr => {
                    let Some(j) = memchr::memchr(b'\r', &data[i..len]) else {
                        break;
                    };
                    let pos = i + j;
                    i = pos + 1;
                    if self.bs_parity_even(data, 0, pos)
                        && self.row_boundary(buf, data, &mut chunk_start, i, &mut line_start, out)
                    {
                        return i;
                    }
                }
            }
        }

        // Buffer exhausted: carry the tail into the current chunk + state.
        if chunk_start < len {
            self.segs.push(ChunkSeg { buf: Arc::clone(buf), start: chunk_start, end: len });
        }
        // Trailing backslash run (for parity across the edge). The detect
        // phase tracks escapes itself; boundary modes use run parity.
        let mut k = len;
        while k > 0 && data[k - 1] == b'\\' {
            k -= 1;
        }
        let run = (len - k) as u32;
        let carry_in = if k == 0 { self.trailing_bs } else { 0 };
        // Decided-Crnl mode: an unescaped \r as the buffer's last byte may
        // pair with a \n at the next buffer's start.
        self.prev_ended_cr = self.eol == SegEol::Crnl
            && len > 0
            && data[len - 1] == b'\r'
            && self.bs_parity_even(data, 0, len - 1);
        self.trailing_bs = carry_in + run;
        // Line-head capture for a line spilling past the buffer.
        let mut idx = line_start;
        while self.line_head_len < 2 && idx < len {
            self.line_head[self.line_head_len as usize] = data[idx];
            self.line_head_len += 1;
            idx += 1;
        }
        self.line_len += (len - line_start) as u64;
        len
    }

    /// A row boundary just closed at `end` (exclusive, includes its EOL
    /// bytes). Counts the row, checks the `\.` marker, cuts a chunk at
    /// rows_per_chunk. Returns true ⇔ the end-of-copy marker line closed
    /// (caller stops consuming).
    fn row_boundary(
        &mut self,
        buf: &Arc<Vec<u8>>,
        data: &[u8],
        chunk_start: &mut usize,
        end: usize,
        line_start: &mut usize,
        out: &mut Vec<ChunkDesc>,
    ) -> bool {
        let first2 = self.line_first2(data, *line_start, end);
        let is_eoc = first2[0] == Some(b'\\') && first2[1] == Some(b'.');
        self.rows += 1;
        self.rows_total += 1;
        self.line_head_len = 0;
        self.line_len = 0;
        *line_start = end;
        self.trailing_bs = 0;
        if is_eoc {
            // The marker line itself goes to the final chunk; the worker
            // replays serial marker validation (aloneness, EOL style).
            self.eoc = true;
            if *chunk_start < end {
                self.segs.push(ChunkSeg { buf: Arc::clone(buf), start: *chunk_start, end });
            }
            *chunk_start = end;
            self.cut_chunk(out);
            return true;
        }
        if self.rows >= self.rows_per_chunk {
            self.segs.push(ChunkSeg { buf: Arc::clone(buf), start: *chunk_start, end });
            *chunk_start = end;
            self.cut_chunk(out);
        }
        false
    }

    fn cut_chunk(&mut self, out: &mut Vec<ChunkDesc>) {
        if self.segs.is_empty() {
            self.rows = 0;
            self.first_lineno = self.rows_total + 1;
            return;
        }
        out.push(ChunkDesc {
            first_lineno: self.first_lineno,
            segs: std::mem::take(&mut self.segs),
        });
        self.rows = 0;
        self.first_lineno = self.rows_total + 1;
    }

    /// Stream EOF: cut whatever remains (a trailing unterminated line is a
    /// row — serial parses it too).
    fn finish(&mut self, out: &mut Vec<ChunkDesc>) {
        self.cut_chunk(out);
    }
}

// ---------------------------------------------------------------------------
// Shared statement state (the parallel context's private payload AND the
// task set's work body — the vacuum-morsels shape).
// ---------------------------------------------------------------------------

pub(crate) struct ParCopyShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    source: Arc<runtime::StreamSource>,
    relid: Oid,
    relname: String,
    // Parse plan.
    delim: u8,
    null_print: String,
    freeze: bool,
    file_encoding: i32,
    /// EOL preset per chunk index: chunk 0 detects itself (Unknown), later
    /// chunks inherit the segmentator's decision. Encoded as the SegEol the
    /// leader publishes BEFORE the first chunk past the decision.
    eol: Mutex<EolPre>,
    attnumlist: Vec<i16>,
    // Encode plan.
    plan: Arc<pgrcolumnar::ParallelIngestPlan>,
    // Chunk registry: leader inserts BEFORE publishing the watermark past
    // the index; the claiming worker removes.
    chunks: Mutex<HashMap<u64, ChunkDesc>>,
    // Completed encodes, keyed by chunk index; the leader commits in order.
    done: Mutex<BTreeMap<u64, Option<pgrcolumnar::EncodedRg>>>,
    // First-error-in-input-order protocol: chunk-indexed error records;
    // claims for chunks ABOVE the floor drain (chunks below still parse, so
    // an earlier error can still surface and win).
    errors: Mutex<BTreeMap<u64, Box<PgError>>>,
    error_floor: AtomicU64,
    /// Hard failure (worker panic / non-data error): abort the RG now.
    failed_hard: AtomicBool,
    hard_error: Mutex<Option<Box<PgError>>>,
    refused: AtomicUsize,
    started: AtomicUsize,
    leader_proc: types_core::ProcNumber,
    /// load-r2 L3-1 sort mode (parallel load sort): Some = workers spill
    /// sorted (key,row) runs instead of encoding RGs; the leader merges
    /// after the RG completes. None = the landed encode pipeline verbatim.
    sort: Option<ParCopySort>,
    /// Registered runs (file paths pushed BEFORE their spill starts so every
    /// file is cleanup-tracked; memory runs free on drop); leader takes them
    /// for the merge.
    ///
    /// GL-LOADDET-1: each entry carries its INPUT COORDINATE `(task, sub)` —
    /// the morsel index the run was cut from and the run's ordinal within
    /// that morsel. The merge sorts on it, so the merge's tiebreak index is a
    /// function of the input alone. Push order (which is worker-race order:
    /// "finished compressing" for memory runs, "started writing" for file
    /// runs, interleaved under a partly-exhausted memstore) is no longer the
    /// merge order. Under `PGRUST_PARALLEL_COPY_SORT_DETERMINISTIC=0` the
    /// coordinates are still recorded but the merge keeps push order.
    sort_runs: Mutex<Vec<(u64, u32, RunLoc)>>,
    sort_run_seq: AtomicU64,
    /// GL-PARQUET-1 inc-2: Some = the morsels are parquet ROW GROUPS
    /// (workers decode columns and feed the sort pipeline); the text
    /// segmentator/chunk plumbing is bypassed entirely.
    parquet: Option<ParquetPar>,
    /// copyfast lever 3: analyze-during-load (Some = the merge pump samples
    /// the stream and the leader writes the stats after publish).
    inline_analyze: Option<InlineAnalyzePlan>,
}

/// Parquet parallel-decode plan: the shared file handle (positioned reads
/// only), the parsed footer, and the schema-match/conversion plan.
struct ParquetPar {
    file: std::fs::File,
    meta: std::sync::Arc<parquet_read::FileMeta>,
    path: String,
    plan: std::sync::Arc<crate::fromparquet::ParquetPlan>,
    /// Non-empty row groups in file order; morsel g decodes rg_order[g].
    rg_order: Vec<usize>,
    /// Global first row (0-based) of each task (error contexts report
    /// 1-based file row numbers, serial-identical).
    row_base: Vec<u64>,
    /// Compressed chunk bytes read so far (leader publishes progress).
    bytes_read: AtomicU64,
}

/// copyfast lever 1: where a registered run lives.
enum RunLoc {
    File(std::path::PathBuf),
    Mem(pgrcolumnar::loadsort::MemRun),
}

/// Sort-mode plan: the presort key spec in memcmp-key terms.
struct ParCopySort {
    keys: Vec<(u16, pgrcolumnar::sortkey::CbSortKeyKind)>,
    key_w: usize,
    budget: usize,
    /// Statement-unique run-file name component.
    nonce: u64,
    /// copyfast lever 1: in-memory run budget (None = every run spills).
    memstore: Option<Arc<pgrcolumnar::loadsort::MemRunStore>>,
}

#[derive(Clone, Copy)]
struct EolPre {
    /// EolType for chunks >= 1 (chunk 0 always starts Unknown, exactly like
    /// serial's first line).
    later: EolType,
}

impl ParCopyShared {
    fn record_error(&self, chunk: u64, e: Box<PgError>) {
        self.errors.lock().unwrap_or_else(|p| p.into_inner()).insert(chunk, e);
        self.error_floor.fetch_min(chunk, Ordering::SeqCst);
        self.wake_leader();
    }

    fn fail_hard(&self, e: Box<PgError>) {
        {
            let mut g = self.hard_error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed_hard.store(true, Ordering::SeqCst);
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
        self.wake_leader();
    }

    fn wake_leader(&self) {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(self.leader_proc));
    }

    fn take_min_error(&self) -> Option<Box<PgError>> {
        let mut g = self.errors.lock().unwrap_or_else(|p| p.into_inner());
        let k = *g.keys().next()?;
        g.remove(&k)
    }

    fn take_hard_error(&self) -> Option<Box<PgError>> {
        self.hard_error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

impl runtime::TaskSetWork for ParCopyShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail_hard(e),
            Err(_panic) => self.fail_hard(
                PgError::new(ERROR, "parallel COPY worker panicked in a chunk").into(),
            ),
        }
    }

    fn finalize(&self) {
        // Results live in the done/errors maps; the LEADER commits/raises.
    }
}

// ---------------------------------------------------------------------------
// Worker side.
// ---------------------------------------------------------------------------

/// Per-helper parse/encode context, on the entry-task frame around
/// drive_pinned; run_morsel reaches it through the thread-local pointer
/// (this thread is the only driver of its lane) — the vacuum-morsels shape.
struct ParCopyWorkerCx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    rel: &'a Relation<'mcx>,
    st: CopyFromState<'mcx, 'a>,
    slot: types_slot::SlotData<'mcx>,
    check_exprs: Option<PgVec<'mcx, nodemodifytable::CheckExpr<'mcx>>>,
    virtual_nn: Option<PgVec<'mcx, nodemodifytable::VirtualNnExpr<'mcx>>>,
    inserted_cols: types_nodes::Bitmapset<'mcx>,
    /// Per-row datum arena, reset after every appended row.
    row_cx: MemoryContext,
    /// load-r2 L3-1 sort mode: this worker's (key,row) batch + codec.
    sort_state: Option<WorkerSortState>,
    /// GL-PARQUET-1 inc-2: reusable per-column decode batches.
    pq_batches: Option<Vec<parquet_read::ColumnBatch>>,
}

struct WorkerSortState {
    batch: pgrcolumnar::loadsort::SortBatch,
    codec: pgrcolumnar::loadsort::RowCodec,
    keybuf: Vec<u8>,
    rowbuf: Vec<u8>,
    /// Phase-wall accumulators (load-r3 M0; ptrace-gated — stay zero and
    /// untouched per-row when tracing is off).
    t_parse: std::time::Duration,
    t_key: std::time::Duration,
    t_spill: std::time::Duration,
    /// Run-file bytes written by this worker (compressed size under RUNLZ4).
    spill_bytes: u64,
    rows: u64,
    runs: u32,
    /// copyfast lever 1: runs kept in memory (subset of `runs`) + their
    /// frame bytes at keep time.
    mem_runs: u32,
    mem_bytes: u64,
    /// GL-LOADDET-1: the input coordinate this worker is currently filling —
    /// the morsel index it claimed and the next run ordinal within it. Every
    /// registered run is stamped with it, and the merge orders on it.
    cur_task: u64,
    cur_sub: u32,
}

thread_local! {
    static WORKER_CX: std::cell::Cell<*mut ParCopyWorkerCx<'static, 'static>> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

impl ParCopyShared {
    fn morsel_body(&self, _worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        let p = WORKER_CX.with(|c| c.get());
        if p.is_null() {
            return Err(PgError::new(ERROR, "parallel COPY chunk without a bound worker").into());
        }
        // SAFETY: set by THIS thread's entry frame around drive_pinned; the
        // frame outlives the drive, and run_morsel only executes on the
        // claiming thread.
        let wcx: &mut ParCopyWorkerCx<'_, '_> = unsafe { &mut *p };
        for g in range {
            self.run_chunk(wcx, g)?;
        }
        Ok(())
    }

    fn run_chunk(&self, wcx: &mut ParCopyWorkerCx<'_, '_>, g: u64) -> PgResult<()> {
        if self.parquet.is_some() {
            // Parquet mode: g indexes rg_order (no chunk registry). Drain
            // claims past the lowest erroring task, exactly like text.
            if g > self.error_floor.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.begin_task(wcx, g);
            match self.parquet_decode_rg(wcx, g) {
                Ok(()) => {
                    // GL-LOADDET-1: cut BEFORE publishing `done` — the run is
                    // the morsel's own, and the leader must never see a task
                    // complete while part of its rows are still in a batch.
                    self.cut_run_at_task_end(wcx)?;
                    self.done.lock().unwrap_or_else(|p| p.into_inner()).insert(g, None);
                    self.wake_leader();
                }
                Err(e) => {
                    self.record_error(g, copy_from_error_context(&wcx.st, e));
                }
            }
            return Ok(());
        }
        let chunk = {
            let mut m = self.chunks.lock().unwrap_or_else(|p| p.into_inner());
            m.remove(&g)
        };
        let Some(chunk) = chunk else {
            return Err(PgError::new(ERROR, "parallel COPY chunk claimed before publish").into());
        };
        // Drain claims past the lowest erroring chunk (chunks BELOW it keep
        // parsing so the first error in input order wins).
        if g > self.error_floor.load(Ordering::SeqCst) {
            return Ok(());
        }
        let eol = if g == 0 {
            EolType::Unknown
        } else {
            self.eol.lock().unwrap_or_else(|p| p.into_inner()).later
        };
        self.begin_task(wcx, g);
        match self.parse_encode_chunk(wcx, chunk, eol) {
            Ok(enc) => {
                self.cut_run_at_task_end(wcx)?;
                self.done.lock().unwrap_or_else(|p| p.into_inner()).insert(g, enc);
                self.wake_leader();
            }
            Err(e) => {
                // Data-shaped error: context attached with the worker's
                // exact line/column; recorded for the leader's ordered
                // re-raise. NOT a hard failure — earlier chunks finish.
                self.record_error(g, copy_from_error_context(&wcx.st, e));
            }
        }
        Ok(())
    }

    fn parse_encode_chunk(
        &self,
        wcx: &mut ParCopyWorkerCx<'_, '_>,
        chunk: ChunkDesc,
        eol: EolType,
    ) -> PgResult<Option<pgrcolumnar::EncodedRg>> {
        {
            let st = &mut wcx.st;
            st.src = CopySrc::Chunk(ChunkCursor::new(chunk.segs));
            st.raw_buf_index = 0;
            st.raw_buf_len = 0;
            st.raw_reached_eof = false;
            st.input_reached_eof = false;
            st.input_reached_error = false;
            st.input_buf_index = 0;
            st.input_buf_len = 0;
            st.line_buf.clear();
            st.line_buf_valid = false;
            st.eol_type = eol;
            st.cur_lineno = chunk.first_lineno - 1;
            st.cur_attidx = None;
            st.cur_attval_off = None;
        }

        let mut enc = if self.sort.is_none() {
            Some(pgrcolumnar::RgChunkEncoder::new(Arc::clone(&self.plan)))
        } else {
            None
        };
        // load-r3 M0 phase walls: per-row Instants only in sort mode AND
        // only when tracing is armed (the default path takes one branch).
        let trace = self.sort.is_some() && ptrace_enabled();
        let mut since_cfi = 0u32;
        loop {
            since_cfi += 1;
            if since_cfi >= 4096 {
                since_cfi = 0;
                postgres_seams::check_for_interrupts::call()?;
            }
            let t0 = if trace { Some(std::time::Instant::now()) } else { None };
            wcx.row_cx.reset();
            exectuples::exec_clear_tuple(&mut wcx.slot, wcx.mcx);
            // SAFETY (lifetime erasure): per-row datums land in row_cx and
            // are COPIED into the chunk encoder before the next reset;
            // nothing retains them past the row (the serial path's
            // statement-mcx contract, tightened to row scope).
            let row_mcx: Mcx<'_> = unsafe { core::mem::transmute(wcx.row_cx.mcx()) };
            {
                let base = wcx.slot.base_mut();
                if !wcx.st.next_copy_from(row_mcx, &mut base.tts_values, &mut base.tts_isnull)? {
                    break;
                }
            }
            exectuples::exec_store_virtual_tuple(&mut wcx.slot);
            wcx.slot.base_mut().tts_tableOid = self.relid;
            // The serial path's ExecConstraints, worker-side (identical
            // errors by construction — same function, same slot shape).
            nodemodifytable::exec_constraints(
                wcx.mcx,
                &mut wcx.check_exprs,
                &mut wcx.virtual_nn,
                wcx.rel,
                &mut wcx.slot,
                None,
                Some(&wcx.inserted_cols),
            )?;
            let base = wcx.slot.base();
            if let Some(enc) = enc.as_mut() {
                enc.append_row(&base.tts_values, &base.tts_isnull)?;
            } else {
                // Sort mode: (memcmp key, row image) into this worker's
                // batch; spill a sorted run at the budget. NULLs refuse
                // HERE with the worker's exact line context (serial cites
                // its buffered-flush line — the recorded parallel-copy
                // divergence rule 2 class; message/sqlstate identical).
                let sort = self.sort.as_ref().unwrap();
                let st = wcx
                    .sort_state
                    .as_mut()
                    .expect("sort mode without a worker sort state");
                let ncols = self.plan.coltypes.len();
                if base.tts_isnull[..ncols].iter().any(|&n| n) {
                    return Err(Box::new(
                        PgError::error("cbstore does not support NULL values".to_string())
                            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                let t1 = t0.map(|t0| {
                    let now = std::time::Instant::now();
                    st.t_parse += now - t0;
                    now
                });
                st.keybuf.clear();
                pgrcolumnar::sortkey::encode_sort_key(&sort.keys, &base.tts_values, &mut st.keybuf);
                st.rowbuf.clear();
                st.codec.serialize_row(&base.tts_values, &mut st.rowbuf)?;
                st.batch.push(&st.keybuf, &st.rowbuf);
                st.rows += 1;
                if let Some(t1) = t1 {
                    st.t_key += t1.elapsed();
                }
                if st.batch.bytes() >= sort.budget {
                    self.spill_worker_batch(st)?;
                }
            }
        }
        let Some(enc) = enc else { return Ok(None) };
        if enc.rows() == 0 {
            // A final chunk holding only the end-of-copy marker line (or an
            // empty stream tail): nothing to encode.
            return Ok(None);
        }
        Ok(Some(enc.seal()))
    }

    /// Sort + spill the worker's current batch as one run. Lever 1: when a
    /// mem-run store is armed, the compressed run stays in memory if the
    /// budget admits it; otherwise (and in every non-armed posture) it
    /// lands in a run file whose path is registered BEFORE the write so
    /// teardown can always unlink it.
    fn spill_worker_batch(&self, st: &mut WorkerSortState) -> PgResult<()> {
        if st.batch.is_empty() {
            return Ok(());
        }
        let sort = self.sort.as_ref().expect("spill without sort mode");
        // GL-LOADDET-1: this run's input coordinate, consumed here so every
        // registration path (memory keep, memory-refused overflow, plain file
        // spill) stamps the same one and the sub ordinal advances exactly once
        // per run.
        let (task, sub) = (st.cur_task, st.cur_sub);
        st.cur_sub += 1;
        // copyfast lever 1: the in-memory home. Compress first (identical
        // frames either way), reserve the ACTUAL byte count; a refusal
        // flushes the already-built frames to the run file verbatim.
        if let Some(store) = &sort.memstore {
            let t0 = std::time::Instant::now();
            st.batch.sort();
            let mem = st.batch.spill_run_mem()?;
            let bytes = mem.bytes();
            if store.try_reserve(bytes) {
                let mem = mem.attach(Arc::clone(store));
                let seq = self.sort_run_seq.fetch_add(1, Ordering::SeqCst);
                self.sort_runs
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push((task, sub, RunLoc::Mem(mem)));
                st.t_spill += t0.elapsed();
                st.runs += 1;
                st.mem_runs += 1;
                st.mem_bytes += bytes;
                st.spill_bytes += bytes;
                ptrace(&format!(
                    "sort run kept in memory seq={seq} task={task}.{sub} bytes={bytes}"
                ));
                return Ok(());
            }
            let (seq, path) = self.register_run_file(sort, task, sub)?;
            let written = mem.write_to_file(&path)?;
            st.spill_bytes += written;
            st.t_spill += t0.elapsed();
            st.runs += 1;
            ptrace(&format!(
                "sort run spilled seq={seq} task={task}.{sub} (mem budget exhausted)"
            ));
            return Ok(());
        }
        let (seq, path) = self.register_run_file(sort, task, sub)?;
        let t0 = std::time::Instant::now();
        st.batch.sort();
        st.spill_bytes += st.batch.spill_run_opts(&path, run_lz4_effective())?;
        st.t_spill += t0.elapsed();
        st.runs += 1;
        ptrace(&format!("sort run spilled seq={seq} task={task}.{sub}"));
        Ok(())
    }

    /// GL-LOADDET-1: cut the current run at a morsel boundary. Called once per
    /// SUCCESSFULLY processed morsel in sort mode, so a run never spans two
    /// morsels and its content is input-determined. Disarmed, the batch simply
    /// carries into the next morsel (the legacy byte image).
    fn cut_run_at_task_end(&self, wcx: &mut ParCopyWorkerCx<'_, '_>) -> PgResult<()> {
        if !sort_deterministic() {
            return Ok(());
        }
        let Some(st) = wcx.sort_state.as_mut() else { return Ok(()) };
        if st.batch.is_empty() {
            return Ok(());
        }
        self.spill_worker_batch(st)
    }

    /// GL-LOADDET-1: bind the worker's sort state to the morsel it just
    /// claimed. Every run cut while this is in force carries `(task, sub)`.
    fn begin_task(&self, wcx: &mut ParCopyWorkerCx<'_, '_>, g: u64) {
        if let Some(st) = wcx.sort_state.as_mut() {
            st.cur_task = g;
            st.cur_sub = 0;
        }
    }

    /// Mint + REGISTER the next run file path (cleanup-tracked before any
    /// write), creating the temp dir on first use.
    fn register_run_file(
        &self,
        sort: &ParCopySort,
        task: u64,
        sub: u32,
    ) -> PgResult<(u64, std::path::PathBuf)> {
        // MakePGDirectory, EEXIST-tolerant: "base" always exists, so the one
        // missing component is pgsql_tmp itself (DST P1 inc-4 fence). EEXIST
        // is only usable when the existing path really is a directory
        // (pre-fence create_dir_all errored on a plain file here).
        let dir = std::path::Path::new("base/pgsql_tmp");
        if fd::MakePGDirectory("base/pgsql_tmp") < 0 {
            let en = fd::get_errno();
            let mut fi = fd::FileInfo::zeroed();
            let usable_dir = en == libc::EEXIST
                && fd::pg_stat("base/pgsql_tmp", &mut fi) == 0
                && fi.is_dir();
            if !usable_dir {
                let e = std::io::Error::from_raw_os_error(en);
                return Err(Box::new(PgError::error(format!(
                    "parallel load-sort temp dir: {e}"
                ))));
            }
        }
        let seq = self.sort_run_seq.fetch_add(1, Ordering::SeqCst);
        let path = dir.join(format!(
            "pgsql_tmp{}.parcopysort.{:x}.{}.run",
            init_small::globals::process_id(),
            sort.nonce,
            seq
        ));
        self.sort_runs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((task, sub, RunLoc::File(path.clone())));
        Ok((seq, path))
    }

    /// GL-PARQUET-1 inc-2: decode ONE parquet row group (coalesced range
    /// read + per-page kernel dispatch) and feed every row through the
    /// sort-mode body — constraints, NULL refusal, memcmp-key encode, run
    /// spill — identical to the text arm from the slot onward.
    fn parquet_decode_rg(&self, wcx: &mut ParCopyWorkerCx<'_, '_>, g: u64) -> PgResult<()> {
        let pq = self.parquet.as_ref().expect("parquet task without parquet mode");
        let sort = self.sort.as_ref().expect("parquet parallel is sort-mode only");
        let rg_idx = pq.rg_order[g as usize];
        let mut rgr = parquet_read::RowGroupReader::open(
            &pq.file,
            &pq.path,
            &pq.meta,
            rg_idx,
            &pq.plan.cols,
            &pq.plan.vutf8,
        )?;
        pq.bytes_read.fetch_add(rgr.compressed_bytes, Ordering::Relaxed);
        if wcx.pq_batches.is_none() {
            wcx.pq_batches = Some(pq.plan.make_batches(&pq.meta));
        }
        let trace = ptrace_enabled();
        let ncols = self.plan.coltypes.len();
        let mut row_global = pq.row_base[g as usize];
        const BATCH_ROWS: u64 = 1024;
        while rgr.rows_remaining() > 0 {
            postgres_seams::check_for_interrupts::call()?;
            let n = rgr.rows_remaining().min(BATCH_ROWS) as usize;
            let t0 = if trace { Some(std::time::Instant::now()) } else { None };
            rgr.read_batches(wcx.pq_batches.as_mut().expect("built above"), n)?;
            if let (Some(t0), Some(st)) = (t0, wcx.sort_state.as_mut()) {
                st.t_parse += t0.elapsed();
            }
            for k in 0..n {
                wcx.row_cx.reset();
                // 1-based file row number for error contexts (serial parity).
                wcx.st.cur_lineno = row_global + k as u64 + 1;
                exectuples::exec_clear_tuple(&mut wcx.slot, wcx.mcx);
                // SAFETY (lifetime erasure): per-row datums land in row_cx
                // and are consumed into the sort batch before the next
                // reset — the text arm's exact contract.
                let row_mcx: Mcx<'_> = unsafe { core::mem::transmute(wcx.row_cx.mcx()) };
                {
                    let batches = wcx.pq_batches.as_ref().expect("built above");
                    let base = wcx.slot.base_mut();
                    for b in pq.plan.bindings.iter() {
                        let batch = &batches[b.batch];
                        let m = b.attidx;
                        if batch.is_null(k) {
                            base.tts_values[m] = datum::Datum::null();
                            base.tts_isnull[m] = true;
                        } else {
                            wcx.st.cur_attidx = Some(m);
                            base.tts_values[m] =
                                crate::fromparquet::convert_cell(row_mcx, b.conv, batch, k)?;
                            base.tts_isnull[m] = false;
                        }
                    }
                    wcx.st.cur_attidx = None;
                }
                exectuples::exec_store_virtual_tuple(&mut wcx.slot);
                wcx.slot.base_mut().tts_tableOid = self.relid;
                nodemodifytable::exec_constraints(
                    wcx.mcx,
                    &mut wcx.check_exprs,
                    &mut wcx.virtual_nn,
                    wcx.rel,
                    &mut wcx.slot,
                    None,
                    Some(&wcx.inserted_cols),
                )?;
                let base = wcx.slot.base();
                let st = wcx
                    .sort_state
                    .as_mut()
                    .expect("parquet parallel without a worker sort state");
                if base.tts_isnull[..ncols].iter().any(|&x| x) {
                    return Err(Box::new(
                        PgError::error("cbstore does not support NULL values".to_string())
                            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                let t1 = if trace { Some(std::time::Instant::now()) } else { None };
                st.keybuf.clear();
                pgrcolumnar::sortkey::encode_sort_key(
                    &sort.keys,
                    &base.tts_values,
                    &mut st.keybuf,
                );
                st.rowbuf.clear();
                st.codec.serialize_row(&base.tts_values, &mut st.rowbuf)?;
                st.batch.push(&st.keybuf, &st.rowbuf);
                st.rows += 1;
                if let Some(t1) = t1 {
                    st.t_key += t1.elapsed();
                }
                if st.batch.bytes() >= sort.budget {
                    self.spill_worker_batch(st)?;
                }
            }
            row_global += n as u64;
        }
        Ok(())
    }
}

/// The launched entry task (vacuum-morsels ceremony: the substrate already
/// connected the helper to the leader's database, restored leader state,
/// and entered parallel mode).
fn parallel_copy_worker_main(pshared: &parallel::ParallelShared) -> PgResult<()> {
    let Some(private) = pshared.private() else { return Ok(()) };
    let Ok(shared) = private.downcast::<ParCopyShared>() else { return Ok(()) };

    let r = catch_unwind(AssertUnwindSafe(|| worker_drive(&shared)));
    let outcome = match r {
        Ok(o) => o,
        Err(unwind) => {
            shared.fail_hard(PgError::new(ERROR, "parallel COPY helper panicked").into());
            if parallel::standing::is_exit_unwind(&*unwind) {
                latch::SetLatch(types_storage::latch::LatchHandle::proc(
                    pshared.parallel_leader_proc_number,
                ));
                std::panic::resume_unwind(unwind);
            }
            Err(Box::new(PgError::new(ERROR, "parallel COPY worker failed (see leader error)")))
        }
    };
    latch::SetLatch(types_storage::latch::LatchHandle::proc(
        pshared.parallel_leader_proc_number,
    ));
    outcome
}

fn worker_drive(shared: &Arc<ParCopyShared>) -> PgResult<()> {
    let Some(rg) = shared.rg.get().and_then(|w| w.upgrade()) else {
        shared.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let Some(lane) = shared.rt.acquire_external_lane() else {
        shared.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let mut lane_local = lane.local();

    let ctx = MemoryContext::new("parallel COPY worker");
    let mcx = ctx.mcx();
    let rel = match table::table_open(mcx, shared.relid, types_rel::lock::RowExclusiveLock) {
        Ok(rel) => rel,
        Err(e) => {
            shared.fail_hard(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = shared.rt.drive_pinned(&mut lane_local, &rg);
            }
            return Ok(());
        }
    };

    let build = (|| -> PgResult<ParCopyWorkerCx<'_, '_>> {
        // Input-function resolution, BeginCopyFrom's loop verbatim.
        let mut in_functions: PgVec<'_, FmgrInfo> = PgVec::new_in(mcx);
        let mut typioparams: PgVec<'_, Oid> = PgVec::new_in(mcx);
        let mut atttypmods: PgVec<'_, i32> = PgVec::new_in(mcx);
        let mut attnames: PgVec<'_, types_tuple::NameData> = PgVec::new_in(mcx);
        let tup_desc = &rel.rd_att;
        let num_phys_attrs = tup_desc.natts as usize;
        let mut attnumlist: PgVec<'_, i16> = PgVec::new_in(mcx);
        for &a in &shared.attnumlist {
            attnumlist.push(a);
        }
        for &attnum in attnumlist.iter() {
            let att = tup_desc.attr(attnum as usize - 1);
            let (func_oid, typioparam) = lsyscache::typ::getTypeInputInfo(att.atttypid)?;
            in_functions.push(fmgr_core::fmgr_info(func_oid)?);
            typioparams.push(typioparam);
            atttypmods.push(att.atttypmod);
        }
        let mut defexprs: PgVec<'_, Option<mcx::PgBox<'_, execexpr::ExprState<'_>>>> =
            PgVec::new_in(mcx);
        for i in 0..num_phys_attrs {
            attnames.push(tup_desc.attr(i).attname);
            defexprs.push(None);
        }
        let inserted_cols = {
            const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
            let mut b = types_nodes::Bitmapset::empty();
            for &a in attnumlist.iter() {
                b.add_member(mcx, a as i32 - FLIHAN)?;
            }
            b
        };
        let max_fields = attnumlist.len();
        let st = CopyFromState {
            opts: CopyFormatOptions {
                file_encoding: shared.file_encoding,
                binary: false,
                csv_mode: false,
                parquet: shared.parquet.is_some(),
                // Worker-side placeholders: the conversion plan (which is
                // what coercion resolves into) is shared from the leader.
                parquet_match_by_name: false,
                parquet_coerce_epoch: false,
                freeze: shared.freeze,
                delim: shared.delim,
                quote: b'"',
                escape: b'"',
                null_print: &shared.null_print,
                default_print: None,
                header_line: CopyHeaderChoice::False,
                force_quote: None,
                force_quote_all: false,
                force_notnull: None,
                force_notnull_all: false,
                force_null: None,
                force_null_all: false,
                convert_selectively: false,
                convert_select: None,
                on_error: CopyOnErrorChoice::Stop,
                log_verbosity: crate::CopyLogVerbosityChoice::Default,
                reject_limit: 0,
            },
            src: CopySrc::Chunk(ChunkCursor::new(Vec::new())),
            raw_buf: vec_from_elem_in(mcx, 0u8, crate::fromparse::RAW_BUF_SIZE + 1),
            raw_buf_index: 0,
            raw_buf_len: 0,
            raw_reached_eof: false,
            input_reached_eof: false,
            input_reached_error: false,
            input_buf: None,
            input_buf_index: 0,
            input_buf_len: 0,
            line_buf: PgVec::new_in(mcx),
            line_buf_valid: false,
            attribute_buf: PgVec::new_in(mcx),
            binary_attr_buf: stringinfo::StringInfo::new_in(mcx)?,
            raw_fields: PgVec::new_in(mcx),
            max_fields,
            eol_type: EolType::Unknown,
            cur_lineno: 0,
            cur_attidx: None,
            cur_attval_off: None,
            file_encoding: shared.file_encoding,
            need_transcoding: false,
            conversion_proc: 0,
            convertcx: MemoryContext::new("parallel COPY convert (unused)"),
            attnumlist,
            in_functions,
            typioparams,
            atttypmods,
            attnames,
            force_notnull_flags: vec_from_elem_in(mcx, false, num_phys_attrs),
            force_null_flags: vec_from_elem_in(mcx, false, num_phys_attrs),
            convert_select_flags: None,
            defexprs,
            defmap: PgVec::new_in(mcx),
            defaults: vec_from_elem_in(mcx, false, num_phys_attrs),
            where_clause: types_nodes::NodeList::nil(),
            relname: shared.relname.clone(),
            escontext: None,
            num_errors: 0,
            bytes_processed: 0,
            volatile_defexprs: false,
        };
        let slot = tableam::table_slot_create(mcx, &rel)?;
        let sort_state = shared.sort.as_ref().map(|sp| WorkerSortState {
            // GL-LOADDET-1: arrival-order tie break inside the run, under the
            // same knob as the boundary/registry mechanisms — all three must
            // move together or the guarantee is partial.
            batch: if sort_deterministic() {
                pgrcolumnar::loadsort::SortBatch::new_stable(sp.key_w)
            } else {
                pgrcolumnar::loadsort::SortBatch::new(sp.key_w)
            },
            codec: pgrcolumnar::loadsort::RowCodec::new(shared.plan.coltypes.clone()),
            keybuf: Vec::with_capacity(sp.key_w),
            rowbuf: Vec::new(),
            t_parse: std::time::Duration::ZERO,
            t_key: std::time::Duration::ZERO,
            t_spill: std::time::Duration::ZERO,
            spill_bytes: 0,
            rows: 0,
            runs: 0,
            mem_runs: 0,
            mem_bytes: 0,
            cur_task: 0,
            cur_sub: 0,
        });
        Ok(ParCopyWorkerCx {
            mcx,
            rel: &rel,
            st,
            slot,
            check_exprs: None,
            virtual_nn: None,
            inserted_cols,
            row_cx: MemoryContext::new_bump("ParallelCopyRowEval"),
            sort_state,
            pq_batches: None,
        })
    })();
    let mut wcx = match build {
        Ok(w) => w,
        Err(e) => {
            shared.fail_hard(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = shared.rt.drive_pinned(&mut lane_local, &rg);
            }
            let _ = table::table_close(rel, types_rel::lock::RowExclusiveLock);
            return Ok(());
        }
    };
    shared.started.fetch_add(1, Ordering::SeqCst);

    // Publish the worker cx for run_morsel (this thread only), drive, clear.
    // SAFETY (lifetime erasure): wcx outlives the drive on this frame; the
    // pointer is cleared before wcx drops.
    WORKER_CX.with(|c| {
        c.set(unsafe {
            core::mem::transmute::<*mut ParCopyWorkerCx<'_, '_>, *mut ParCopyWorkerCx<'static, 'static>>(
                &mut wcx as *mut ParCopyWorkerCx<'_, '_>,
            )
        })
    });
    let _outcome = shared.rt.drive_pinned(&mut lane_local, &rg);
    WORKER_CX.with(|c| c.set(std::ptr::null_mut()));

    // Sort mode: flush this worker's final batch as its last run. Skipped
    // when the statement is already failing (hard error or any recorded
    // data error — the COPY raises regardless; the leader never merges).
    //
    // GL-LOADDET-1: armed, every morsel already cut its own run, so this is a
    // no-op on the success path (`spill_worker_batch` returns immediately on an
    // empty batch). It stays as the disarmed path's tail flush AND as the
    // belt-and-braces path for any future morsel exit that skips the cut — the
    // stamp it would carry, `(cur_task, cur_sub)`, is still the correct
    // coordinate for whatever remains.
    if let Some(st) = wcx.sort_state.as_mut() {
        if !shared.failed_hard.load(Ordering::SeqCst)
            && shared.error_floor.load(Ordering::SeqCst) == u64::MAX
        {
            if let Err(e) = shared.spill_worker_batch(st) {
                shared.fail_hard(e);
            }
        }
        // load-r3 M0: the per-worker phase decomposition of the parse+spill
        // pole (trace-gated; accumulators are zero when tracing is off).
        ptrace(&format!(
            "sort worker phases: parse {:.2}s key {:.2}s spill {:.2}s rows={} runs={} spillbytes={} memruns={} membytes={}",
            st.t_parse.as_secs_f64(),
            st.t_key.as_secs_f64(),
            st.t_spill.as_secs_f64(),
            st.rows,
            st.runs,
            st.spill_bytes,
            st.mem_runs,
            st.mem_bytes,
        ));
    }

    drop(wcx);
    table::table_close(rel, types_rel::lock::RowExclusiveLock)?;

    if shared.failed_hard.load(Ordering::SeqCst) {
        // A recorded hard error (possibly a sibling's): abort the worker
        // transaction so resowner releases residue; the leader rethrows the
        // recorded error, never this message.
        return Err(Box::new(PgError::new(
            ERROR,
            "parallel COPY worker failed (see leader error)",
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Leader: admission + the ceremony (segment/publish/commit loop).
// ---------------------------------------------------------------------------

/// Sort-mode phase 2: k-way merge every spilled run, encode in parallel,
/// commit in order (load-r2 L3-1 step d + lever-i overlap).
///
/// Three concurrent roles under one scope (measured 100M split @ dfb8d6115:
/// fill 43.1s / send-block 0.01s / done-wait 0.2s / commit 60.1s — the fill
/// pump and the ordered commit were serialized on one thread; overlapped
/// they cost ~max of the two):
///   PUMP (spawned): streams rows in global memcmp-key order out of the
///     RunMerge into exactly RG_ROWS-row batches -> bounded work channel.
///   ENCODERS (spawned pool): RgChunkEncoder per batch (the landed
///     worker-encode machinery, byte-proven by its seam oracle).
///   LEADER (this thread): drains the done channel, commits EncodedRgs in
///     batch order via commit_encoded_rg, CFIs per message.
/// Returns the merged row count plus, when lever 3 is armed, the pump's
/// stream-order reservoir sample (row images).
fn merge_sorted_runs(
    writer: &mut pgrcolumnar::CbWriter,
    shared: &Arc<ParCopyShared>,
) -> PgResult<(u64, Option<Vec<Vec<u8>>>)> {
    let sort = shared.sort.as_ref().expect("merge without sort mode");
    let mut stamped =
        std::mem::take(&mut *shared.sort_runs.lock().unwrap_or_else(|p| p.into_inner()));
    // GL-LOADDET-1: the merge's run index IS its tiebreak for key-equal rows,
    // so order the inputs by input coordinate, not by the worker race that
    // appended them. `(task, sub)` is unique (one worker owns a morsel) and
    // input-major, so this is a total, dop-independent order.
    let det = sort_deterministic();
    if det {
        stamped.sort_by_key(|(task, sub, _)| (*task, *sub));
    }
    let runs: Vec<RunLoc> = stamped.into_iter().map(|(_, _, r)| r).collect();
    let n_runs = runs.len();
    let n_mem = runs.iter().filter(|r| matches!(r, RunLoc::Mem(_))).count();
    let mem_bytes: u64 = runs
        .iter()
        .map(|r| match r {
            RunLoc::Mem(m) => m.bytes(),
            RunLoc::File(_) => 0,
        })
        .sum();
    // File paths cloned for the post-open eager unlink below.
    let file_paths: Vec<std::path::PathBuf> = runs
        .iter()
        .filter_map(|r| match r {
            RunLoc::File(p) => Some(p.clone()),
            RunLoc::Mem(_) => None,
        })
        .collect();
    // loadcommit C1: the merge kernel — BinaryHeap (default, byte-proven)
    // or the loser tree (opt-in, byte-identical order by construction).
    enum MergeKind {
        V1(pgrcolumnar::loadsort::RunMerge),
        V2(pgrcolumnar::loadsort::RunMergeV2),
    }
    let prefetch = if fill_v2() { fill_prefetch() } else { 0 };
    let lz4 = run_lz4_effective();
    if run_lz4() && !fill_v2() {
        ptrace("runlz4 refused: requires PGRUST_PARALLEL_COPY_FILL_V2=1 (raw runs used)");
    }
    let mut merge = if fill_v2() {
        // Lever 1: the V2 constructor takes runs from EITHER home, in
        // registration order; prefetch feeders serve the file subset only.
        let inputs: Vec<pgrcolumnar::loadsort::RunInput> = runs
            .into_iter()
            .map(|r| match r {
                RunLoc::File(p) => pgrcolumnar::loadsort::RunInput::File(p),
                RunLoc::Mem(m) => pgrcolumnar::loadsort::RunInput::Mem(m),
            })
            .collect();
        MergeKind::V2(pgrcolumnar::loadsort::RunMergeV2::open_mixed(
            inputs,
            sort.key_w,
            prefetch,
            lz4,
        )?)
    } else {
        // Mem runs exist only under FILL_V2 (memstore admission requires
        // it), so the V1 heap merge sees file runs by construction.
        let mut paths = Vec::with_capacity(runs.len());
        for r in runs {
            match r {
                RunLoc::File(p) => paths.push(p),
                RunLoc::Mem(_) => {
                    return Err(Box::new(PgError::new(
                        ERROR,
                        "parallel load-sort: in-memory run reached the v1 merge",
                    )))
                }
            }
        }
        MergeKind::V1(pgrcolumnar::loadsort::RunMerge::open(&paths, sort.key_w)?)
    };
    if fill_split() {
        match &mut merge {
            MergeKind::V1(m) => m.set_timed(true),
            MergeKind::V2(m) => m.set_timed(true),
        }
    }
    let fadv = fill_fadv_bytes();
    if fadv > 0 {
        match &mut merge {
            MergeKind::V1(m) => m.set_fadvise(fadv),
            MergeKind::V2(m) => m.set_fadvise(fadv),
        }
    }
    // Eager unlink: the open fds keep the data; a crash from here leaves
    // no orphan files.
    for p in &file_paths {
        let _ = fd::pg_unlink(&p.to_string_lossy());
    }
    let nenc = sort_encoders(shared.rt);
    ptrace(&format!(
        "sort merge over {n_runs} runs encoders={nenc} det={} fill={} fadv_mb={} prefetch={prefetch} runlz4={} memruns={n_mem} membytes={mem_bytes}",
        det as u8,
        match &merge {
            MergeKind::V1(_) => "v1",
            MergeKind::V2(_) => "v2",
        },
        fadv >> 20,
        lz4 as u8,
    ));

    const RG: usize = pgrcolumnar::format::RG_ROWS;
    struct Batch {
        idx: u64,
        arena: Vec<u8>,
        lens: Vec<u32>,
    }
    // permit-s4 row 5 (dst-p3-scheduler §3): both channels ride
    // pgsync::mailbox — work bounded at EXACTLY nenc+1 (the pump/encoder
    // blocking pattern is part of the abort topology), done unbounded. The
    // old shape wrapped one mpsc Receiver in a Mutex shared by every
    // encoder, which parked in `recv` INSIDE the guard — the known live
    // recv-under-mutex token-holder wedge (census §"Lock waits"). The
    // mailbox is MPMC by construction: receivers are shared by CLONING and
    // every park runs with the queue lock released (the mailbox law).
    let (work_tx, work_rx) = pgsync::mailbox::<Batch>(Some(nenc + 1));
    let (done_tx, done_rx) =
        pgsync::mailbox::<(u64, PgResult<pgrcolumnar::EncodedRg>)>(None);
    let abort = std::sync::atomic::AtomicBool::new(false);

    let key_w = sort.key_w;
    let t_merge = std::time::Instant::now();
    let mut first_err: Option<Box<PgError>> = None;
    let mut committed = 0u64;
    let mut t_commit = std::time::Duration::ZERO;

    let (n_rows, batches, sample) = std::thread::scope(|scope| {
        for _ in 0..nenc {
            let rx = work_rx.clone();
            let tx = done_tx.clone();
            let plan = Arc::clone(&shared.plan);
            scope.spawn(move || {
                let codec = pgrcolumnar::loadsort::RowCodec::new(plan.coltypes.clone());
                let ncols = plan.coltypes.len();
                let mut arena: Vec<u8> = Vec::new();
                let mut values = vec![::datum::Datum::null(); ncols];
                let isnull = vec![false; ncols];
                loop {
                    // Parks with the mailbox lock RELEASED; None = the pump
                    // dropped its sender (end of input / error / abort) and
                    // the queue is drained.
                    let Some(b) = rx.recv() else { break };
                    let r = catch_unwind(AssertUnwindSafe(
                        || -> PgResult<pgrcolumnar::EncodedRg> {
                            let mut enc =
                                pgrcolumnar::RgChunkEncoder::new(Arc::clone(&plan));
                            let mut off = 0usize;
                            for &l in &b.lens {
                                let l = l as usize;
                                arena.clear();
                                codec.deserialize_row(
                                    &b.arena[off..off + l],
                                    &mut arena,
                                    &mut values,
                                )?;
                                enc.append_row(&values, &isnull)?;
                                off += l;
                            }
                            Ok(enc.seal())
                        },
                    ));
                    let r = match r {
                        Ok(r) => r,
                        Err(_) => Err(Box::new(PgError::new(
                            ERROR,
                            "parallel load-sort encoder panicked",
                        ))),
                    };
                    let failed = r.is_err();
                    if tx.send((b.idx, r)).is_err() || failed {
                        break;
                    }
                }
            });
        }
        drop(done_tx); // encoder clones remain; done_rx closes when they exit
        // The encoder clones are the only work receivers now: if the pool
        // exits early, the pump's send observes the closed receive side
        // (Err) exactly as mpsc's disconnected SendError did.
        drop(work_rx);

        // PUMP: owns the RunMerge and the work sender; exits (dropping the
        // sender) at end of input, on error, or on the leader's abort flag.
        let abort_ref = &abort;
        let sampler_plan = shared.inline_analyze;
        let pump = scope.spawn(move || -> (PgResult<()>, u64, u64, Option<Vec<Vec<u8>>>) {
            let mut key: Vec<u8> = Vec::with_capacity(key_w);
            let mut row: Vec<u8> = Vec::new();
            let mut cur = Batch { idx: 0, arena: Vec::new(), lens: Vec::new() };
            let mut sent = 0u64;
            let mut n_rows = 0u64;
            let mut t_fill = std::time::Duration::ZERO;
            let mut t_send = std::time::Duration::ZERO;
            // copyfast lever 3: the pump sees every row of the final stream
            // in physical order — the sampling site.
            let mut sampler = sampler_plan.map(|p| StreamSampler::new(p.targrows, p.seed));
            let r = (|| -> PgResult<()> {
                loop {
                    if abort_ref.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let t0 = std::time::Instant::now();
                    let mut merge_done = false;
                    match &mut merge {
                        MergeKind::V1(m) => {
                            while cur.lens.len() < RG {
                                match m.next_entry(&mut key, &mut row) {
                                    Ok(true) => {
                                        cur.arena.extend_from_slice(&row);
                                        cur.lens.push(row.len() as u32);
                                        n_rows += 1;
                                        if let Some(s) = sampler.as_mut() {
                                            s.offer(&row);
                                        }
                                    }
                                    Ok(false) => {
                                        merge_done = true;
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                        MergeKind::V2(m) => {
                            // loadcommit C1: rows land straight in the
                            // batch arena — no pump-local key/row copies.
                            while cur.lens.len() < RG {
                                match m.next_row_into(&mut cur.arena) {
                                    Ok(Some(l)) => {
                                        cur.lens.push(l);
                                        n_rows += 1;
                                        if let Some(s) = sampler.as_mut() {
                                            let start = cur.arena.len() - l as usize;
                                            s.offer(&cur.arena[start..]);
                                        }
                                    }
                                    Ok(None) => {
                                        merge_done = true;
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                    }
                    t_fill += t0.elapsed();
                    if !cur.lens.is_empty() {
                        let t1 = std::time::Instant::now();
                        let full = std::mem::replace(
                            &mut cur,
                            Batch { idx: sent + 1, arena: Vec::new(), lens: Vec::new() },
                        );
                        if work_tx.send(full).is_err() {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "parallel load-sort encoder pool exited early",
                            )));
                        }
                        sent += 1;
                        t_send += t1.elapsed();
                    }
                    if merge_done {
                        return Ok(());
                    }
                }
            })();
            // loadcommit C0: fill decomposition (advance = run read+decode
            // inside the merge; 0.00 unless PGRUST_PARALLEL_COPY_FILL_SPLIT=1;
            // heap/copy share = fill − advance).
            let (adv_s, run_bytes, comp_bytes) = match &merge {
                MergeKind::V1(m) => m.fill_stats(),
                MergeKind::V2(m) => m.fill_stats(),
            };
            ptrace(&format!(
                "sort merge pump done: fill {:.2}s send-block {:.2}s rows={n_rows} batches={sent} \
                 fill split: advance {adv_s:.2}s runbytes={run_bytes} compbytes={comp_bytes}",
                t_fill.as_secs_f64(),
                t_send.as_secs_f64(),
            ));
            // Sample only meaningful on a clean, complete pump pass.
            let sample = match (&r, sampler) {
                (Ok(()), Some(s)) => Some(s.finish()),
                _ => None,
            };
            (r, n_rows, sent, sample)
        });
        // work_tx moved into the pump; when it finishes, the channel closes
        // and the encoders drain out, closing done_rx.

        // LEADER: ordered commits off the done channel.
        let mut pending: BTreeMap<u64, pgrcolumnar::EncodedRg> = BTreeMap::new();
        while let Some((idx, r)) = done_rx.recv() {
            if let Err(e) = postgres_seams::check_for_interrupts::call() {
                if first_err.is_none() {
                    first_err = Some(e);
                }
                abort.store(true, Ordering::SeqCst);
            }
            match r {
                Ok(enc) => {
                    pending.insert(idx, enc);
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    abort.store(true, Ordering::SeqCst);
                }
            }
            if first_err.is_some() {
                pending.clear();
                continue; // keep draining so the pool can exit
            }
            let t2 = std::time::Instant::now();
            while pending.keys().next() == Some(&committed) {
                let enc = pending.remove(&committed).unwrap();
                if let Err(e) = writer.commit_encoded_rg(enc) {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    abort.store(true, Ordering::SeqCst);
                    pending.clear();
                    break;
                }
                committed += 1;
                if committed % 16 == 0 {
                    pgstat_progress_update_param(
                        PROGRESS_COPY_TUPLES_PROCESSED,
                        (committed * RG as u64) as i64,
                    );
                }
            }
            t_commit += t2.elapsed();
        }
        let (pr, n_rows, sent, sample) = pump.join().unwrap_or_else(|_| {
            (
                Err(Box::new(PgError::new(ERROR, "parallel load-sort pump panicked"))),
                0,
                0,
                None,
            )
        });
        if let Err(e) = pr {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
        (n_rows, sent, sample)
    });

    if let Some(e) = first_err {
        return Err(e);
    }
    if committed != batches {
        return Err(Box::new(PgError::new(
            ERROR,
            "parallel load-sort merge lost batches (committed != sent)",
        )));
    }
    let (c_stitch, c_pwrite, c_meta, c_bytes) = writer.commit_phase_split();
    ptrace(&format!(
        "sort merge done: {:.2}s total (commit {:.2}s) rows={n_rows} rgs={committed} \
         commit split: stitch {c_stitch:.2}s pwrite {c_pwrite:.2}s meta {c_meta:.2}s bytes={c_bytes}",
        t_merge.elapsed().as_secs_f64(),
        t_commit.as_secs_f64(),
    ));
    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, n_rows as i64);
    Ok((n_rows, sample))
}

fn vacuum_style_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<ParCopyShared>() else { return };
    payload.source.close();
    payload.rt.notify_source_progress();
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        if rg.try_outcome().is_none() {
            drain_rg(payload.rt, &rg);
        }
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_parallel_copy_main",
            parallel_copy_worker_main,
        );
        parallel::register_parallel_private_shutdown(vacuum_style_shutdown);
    });
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive (the vacuum
/// drain shape). False = leaked (dead participant).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    rt.notify_source_progress();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else { return false };
    let mut local = lane.local();
    rt.try_drain_pinned(&mut local, rg, 4000).is_some()
}

/// Fail-closed admission. `Ok(None)` = serial COPY, byte-identically. All
/// checks are metadata-only: NO input is consumed before this passes.
/// Engagement decision: (runtime, gang size, sort mode, parquet mode).
type Admission = Option<(&'static Arc<runtime::Runtime>, i32, Option<ParCopySort>, Option<ParquetPar>)>;

fn admit<'mcx>(
    cstate: &CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    has_triggers: bool,
) -> PgResult<Admission> {
    // Macro-compatible shim: refuse! returns Ok(None) from THIS fn.
    if !flag_enabled() || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { refuse!("no runtime pool") };
    if parallel::IsParallelWorker() || !init_small::globals::IsUnderPostmaster() {
        refuse!("not a postmaster session leader");
    }
    if tableam_vocab::TableAm::of(rel) != Some(tableam_vocab::TableAm::Pgrcolumnar) {
        refuse!("not a cbstore relation");
    }
    if has_triggers {
        refuse!("relation has triggers");
    }
    if rel.rd_rel.relhasindex {
        refuse!("relation has indexes");
    }
    if rel
        .rd_att
        .constr
        .as_deref()
        .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual)
    {
        refuse!("generated columns");
    }
    let o = &cstate.opts;
    if o.binary {
        refuse!("binary format");
    }
    if o.csv_mode {
        refuse!("csv format (phase-1 is text-only)");
    }
    if o.parquet && !parquet_parallel_enabled() {
        refuse!("parquet format (PGRUST_PARQUET_PARALLEL=0; serial reader)");
    }
    if o.header_line != CopyHeaderChoice::False {
        refuse!("HEADER");
    }
    if o.on_error != CopyOnErrorChoice::Stop {
        refuse!("ON_ERROR ignore (row-dropping shifts RG seams)");
    }
    if o.default_print.is_some() || !cstate.defmap.is_empty() || cstate.volatile_defexprs {
        refuse!("column defaults");
    }
    if cstate.convert_select_flags.is_some() {
        refuse!("convert_select");
    }
    if !cstate.where_clause.is_nil() {
        refuse!("WHERE clause (row-dropping shifts RG seams)");
    }
    if cstate.need_transcoding {
        refuse!("encoding conversion");
    }
    if cstate.escontext.is_some() {
        refuse!("soft-error context");
    }
    // Every physical column must be COPY-listed (no defaults admitted, and
    // pgrcolumnar refuses NULLs anyway — but refuse here for the exact serial
    // error path).
    if cstate.attnumlist.len() != rel.rd_att.natts as usize {
        refuse!("partial column list");
    }
    // pgrcolumnar geometry: supported coltypes, no cluster key (sort-on-ingest
    // drains serially by construction).
    let coltypes = match pgrcolumnar::coltypes_of(rel) {
        Ok(t) => t,
        Err(_) => refuse!("unsupported cbstore column type (serial raises the error)"),
    };
    let sort = match pgrcolumnar::writer::writer_opts_of(rel, &coltypes) {
        Ok(opts) if opts.cluster_key.is_empty() && opts.presort_key.is_empty() => None,
        Ok(opts) if !opts.cluster_key.is_empty() => {
            refuse!("cluster_key (sort-on-ingest is serial)")
        }
        Ok(opts) => {
            // PGRUST_COPY_PRESORT: the parallel load-sort pipeline, behind
            // its own flag; int-class fixed-width keys only. Every refusal
            // = the serial sort-on-ingest path verbatim (L3-0, byte-proven).
            if !sort_flag_enabled() {
                refuse!("PGRUST_COPY_PRESORT (sort-on-ingest is serial; PGRUST_PARALLEL_COPY_SORT=1 engages the parallel sort)");
            }
            let Some(key_w) = pgrcolumnar::sortkey::fixed_key_width(&opts.presort_key) else {
                refuse!("PGRUST_COPY_PRESORT text key (parallel load-sort is int-class only)");
            };
            static SORT_NONCE: AtomicU64 = AtomicU64::new(1);
            Some(ParCopySort {
                keys: opts.presort_key,
                key_w,
                budget: sort_budget(),
                nonce: SORT_NONCE.fetch_add(1, Ordering::SeqCst),
                memstore: None, // resolved after dop, below
            })
        }
        Err(_) => refuse!("cbstore reloption error (serial raises it)"),
    };
    // Callback sources (tablesync's publisher COPY OUT stream) stay serial.
    if matches!(cstate.src, CopySrc::Callback { .. }) {
        refuse!("callback source (tablesync COPY is serial)");
    }
    // File-source size floor (frontend streams engage regardless).
    if let CopySrc::File { fd, .. } = &cstate.src {
        let size = fd::with_allocated_stdio(*fd, |f| f.metadata().map(|m| m.len()).unwrap_or(0))
            .unwrap_or(0);
        if size < file_floor() {
            refuse!(format!("file smaller than the {}B floor", file_floor()));
        }
    }
    let mut k = dop(rt);
    if k < 1 {
        refuse!("dop < 1");
    }

    // GL-PARQUET-1 inc-2: parquet row-group morsels. Sort mode is REQUIRED —
    // in the sort pipeline the columnar-store RG seams are decided at the
    // merge fill, so worker task shape cannot move them; order-preserving
    // parquet parallelism would cut RGs at parquet row-group boundaries
    // instead of the serial writer's and is refused by design.
    let parquet = if o.parquet {
        if sort.is_none() {
            refuse!(
                "parquet parallel requires the parallel sort pipeline \
                 (PGRUST_COPY_PRESORT + PGRUST_PARALLEL_COPY_SORT=1)"
            );
        }
        let CopySrc::Parquet(psrc) = &cstate.src else {
            refuse!("parquet source not initialized");
        };
        let reader = psrc.reader();
        if reader.file_len() < file_floor() {
            refuse!(format!("parquet file smaller than the {}B floor", file_floor()));
        }
        let meta = reader.meta_arc();
        let plan = psrc.plan_arc();
        let mut rg_order = Vec::new();
        let mut row_base = Vec::new();
        let mut rows = 0u64;
        let mut max_rg_bytes = 0u64;
        for (i, rg) in meta.row_groups.iter().enumerate() {
            if rg.num_rows == 0 {
                continue;
            }
            rg_order.push(i);
            row_base.push(rows);
            rows += rg.num_rows as u64;
            max_rg_bytes =
                max_rg_bytes.max(parquet_read::rg_compressed_bytes(&meta, i, &plan.cols));
        }
        if rg_order.len() < 2 {
            refuse!("parquet file with fewer than 2 row groups (serial reader)");
        }
        // In-flight decode memory is one row group's compressed chunks per
        // worker: clamp the gang to the compressed-bytes budget (and to the
        // task count).
        let by_budget = (parquet_budget_bytes() / max_rg_bytes.max(1)).max(1);
        k = (k as u64).min(by_budget).min(rg_order.len() as u64) as i32;
        let file = match reader.try_clone_file() {
            Ok(f) => f,
            Err(_) => refuse!("could not duplicate the parquet file handle"),
        };
        ptrace(&format!(
            "parquet parallel admitted rgs={} max_rg_mb={} k={k}",
            rg_order.len(),
            max_rg_bytes >> 20,
        ));
        Some(ParquetPar {
            file,
            meta,
            path: reader.path().to_string(),
            plan,
            rg_order,
            row_base,
            bytes_read: AtomicU64::new(0),
        })
    } else {
        None
    };

    let mut sort = sort;
    if let Some(sort) = sort.as_mut() {
        // copyfast lever 1 admission: budget sized against live headroom
        // AT THIS STATEMENT (dop-dependent reserve), fail-closed to the
        // file spill in every refusal posture.
        if !matches!(memruns_knob(), MemRuns::Off) {
            if !run_lz4_effective() {
                ptrace(
                    "memruns refused: requires PGRUST_PARALLEL_COPY_FILL_V2=1 + PGRUST_COPY_RUNLZ4=1",
                );
            } else {
                let budget = memrun_budget(k);
                if budget > 0 {
                    ptrace(&format!("memruns engaged budget={budget}"));
                    sort.memstore = Some(pgrcolumnar::loadsort::MemRunStore::new(budget));
                }
            }
        }
    }
    Ok(Some((rt, k, sort, parquet)))
}

enum Ceremony {
    /// Pre-consumption refusal (zero workers launched/participating):
    /// nothing read; serial takes over.
    Refused,
    /// Rows loaded + (lever 3) the pump's stream-order sample when
    /// analyze-during-load is armed.
    Done(u64, Option<Vec<Vec<u8>>>),
}

/// Morsel-parallel COPY FROM. `Ok(None)` = refused, run the serial path
/// (cstate untouched). `Ok(Some(n))` = n rows loaded and published.
/// Errors are FULLY CONTEXTED (worker line contexts attached) — the caller
/// must NOT wrap them in copy_from_error_context again.
pub(crate) fn copy_from_parallel<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    has_triggers: bool,
) -> PgResult<Option<u64>> {
    let Some((rt, k, sort, parquet)) = admit(cstate, rel, has_triggers)? else {
        return Ok(None);
    };

    // copyfast lever 3 admission: resolve the ANALYZE-equivalent sample
    // size while catalog access is open (pre-parallel-mode) and draw the
    // reservoir seed on the leader (the merge pump thread never touches
    // backend PRNG state). Fail-closed: refused = stats come from a later
    // ANALYZE, exactly as today.
    let inline_analyze = if analyze_inline_flag() {
        if sort.is_some() {
            let targrows = commands_analyze::inline_analyze_targrows(rel)?;
            let seed = pg_prng::global_prng(|p| p.next_u64());
            ptrace(&format!("analyze inline engaged targrows={targrows}"));
            Some(InlineAnalyzePlan { targrows, seed })
        } else {
            ptrace("analyze inline refused: requires the parallel sort pipeline");
            None
        }
    } else {
        None
    };

    // Writer open BEFORE EnterParallelMode (xid/cid assignment); identical
    // to the serial open (header init, freeze decision, append handling).
    // Sort mode opens PLAIN (the sort happens upstream in the workers; the
    // merged drain through append_row is the L3-0 byte-proven path).
    let mut writer = if sort.is_some() {
        pgrcolumnar::writer::begin_parallel_ingest_presorted(rel)?
    } else {
        pgrcolumnar::begin_parallel_ingest(rel)?
    };
    let Some(plan) = writer.parallel_ingest_plan() else {
        // Belt+braces: admission already refused cluster keys.
        return Ok(None);
    };
    // load-r3 M2: column-sharded stitch pool (opt-in). AFTER the plan — the
    // plan snapshots capture flags from the writer's live stitch builders.
    let sp = stitch_pool_threads();
    if sp > 0 {
        writer.install_stitch_pool(sp)?;
        ptrace(&format!("stitch pool requested threads={sp}"));
    }

    let shared = Arc::new(ParCopyShared {
        rt,
        rg: OnceLock::new(),
        source: Arc::new(runtime::StreamSource::new()),
        relid: rel.rd_id,
        relname: cstate.relname.clone(),
        delim: cstate.opts.delim,
        null_print: cstate.opts.null_print.to_string(),
        freeze: cstate.opts.freeze,
        file_encoding: cstate.file_encoding,
        eol: Mutex::new(EolPre { later: EolType::Unknown }),
        attnumlist: cstate.attnumlist.iter().copied().collect(),
        plan: Arc::new(plan),
        chunks: Mutex::new(HashMap::new()),
        done: Mutex::new(BTreeMap::new()),
        errors: Mutex::new(BTreeMap::new()),
        error_floor: AtomicU64::new(u64::MAX),
        failed_hard: AtomicBool::new(false),
        hard_error: Mutex::new(None),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        leader_proc: init_small::globals::MyProcNumber(),
        sort,
        sort_runs: Mutex::new(Vec::new()),
        sort_run_seq: AtomicU64::new(0),
        parquet,
        inline_analyze,
    });
    ensure_hooks_registered();

    // Sort-run cleanup on EVERY exit path (error unwind included): any
    // registered, not-yet-consumed run file is unlinked (missing = fine —
    // the merge eagerly unlinks after open); in-memory runs free (and
    // release their store reservation) on drop.
    struct RunCleanup(Arc<ParCopyShared>);
    impl Drop for RunCleanup {
        fn drop(&mut self) {
            let runs = std::mem::take(
                &mut *self.0.sort_runs.lock().unwrap_or_else(|p| p.into_inner()),
            );
            for (_, _, r) in runs {
                match r {
                    RunLoc::File(p) => {
                        let _ = fd::pg_unlink(&p.to_string_lossy());
                    }
                    RunLoc::Mem(m) => drop(m),
                }
            }
        }
    }
    let _run_cleanup = RunCleanup(Arc::clone(&shared));

    xact::EnterParallelMode();
    let r = ceremony(cstate, &mut writer, &shared, rt, k);
    xact::ExitParallelMode();

    match r? {
        Ceremony::Refused => Ok(None),
        Ceremony::Done(processed, inline_sample) => {
            // Publish (footer + header, durable) — the serial finish.
            let tf = std::time::Instant::now();
            writer.finish_parallel_ingest()?;
            let (f_stitch, f_footer, f_sync, f_blob_bytes) = writer.finish_phase_split();
            ptrace(&format!(
                "finish split: wall {:.2}s stitch {f_stitch:.2}s footer {f_footer:.2}s \
                 sync {f_sync:.2}s blob_bytes={f_blob_bytes}",
                tf.elapsed().as_secs_f64(),
            ));
            // copyfast lever 3: the stats write, AFTER publish (the footer
            // NDV override reads the just-published part footer — same
            // source a post-load ANALYZE would read).
            if let Some(sample) = inline_sample {
                let t0 = std::time::Instant::now();
                let n = sample.len();
                inline_analyze_apply(mcx, rel, &shared.plan, &sample, processed)?;
                ptrace(&format!(
                    "analyze inline stats written sample={n} totalrows={processed} wall={:.2}s",
                    t0.elapsed().as_secs_f64(),
                ));
            }
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed as i64);
            ptrace(&format!("done rows={processed}"));
            Ok(Some(processed))
        }
    }
}

/// copyfast lever 3: decode the sampled run-row images back into datums,
/// form heap tuples in the statement context, and hand ANALYZE the sample
/// (commands_analyze runs its standard compute/write half on it).
fn inline_analyze_apply<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    plan: &pgrcolumnar::ParallelIngestPlan,
    sample: &[Vec<u8>],
    totalrows: u64,
) -> PgResult<()> {
    let codec = pgrcolumnar::loadsort::RowCodec::new(plan.coltypes.clone());
    let tupdesc = rel.descr();
    let ncols = plan.coltypes.len();
    debug_assert_eq!(ncols, tupdesc.natts as usize, "parallel COPY admits full column lists only");
    let mut values = vec![::datum::Datum::null(); ncols];
    let isnull = vec![false; ncols];
    let mut arena: Vec<u8> = Vec::new();
    let mut rows: Vec<types_tuple::HeapTupleData<'mcx>> = Vec::with_capacity(sample.len());
    for img in sample {
        arena.clear();
        codec.deserialize_row(img, &mut arena, &mut values)?;
        let owned = heaptuple::heap_form_tuple(mcx, tupdesc, &values, &isnull)?;
        let (ptr, len, tid, oid) = (
            owned.image().as_ptr(),
            owned.as_tuple().t_len,
            owned.as_tuple().t_self,
            owned.as_tuple().t_tableOid,
        );
        core::mem::forget(owned);
        // SAFETY: the image was just formed in `mcx` (statement scope) and,
        // forgotten, lives until that context's teardown; the analyze call
        // below only reads it (pgrcolumnar_acquire_sample_rows' pattern).
        rows.push(unsafe { types_tuple::HeapTupleData::from_raw_parts(ptr, len, tid, oid) });
    }
    commands_analyze::analyze_rel_inline_sample(mcx, rel.rd_id, &rows, totalrows as f64)
}

#[allow(clippy::too_many_arguments)]
fn ceremony(
    cstate: &mut CopyFromState<'_, '_>,
    writer: &mut pgrcolumnar::CbWriter,
    shared: &Arc<ParCopyShared>,
    rt: &'static Arc<runtime::Runtime>,
    k: i32,
) -> PgResult<Ceremony> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_parallel_copy_main", k)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|submitted: &mut Option<runtime::RgHandle>| -> PgResult<Ceremony> {
        parallel::InitializeParallelDSM(pcxt)?;
        if parallel::nworkers(pcxt) <= 0 {
            return Ok(Ceremony::Refused);
        }
        parallel::set_private(pcxt, Arc::clone(shared) as _);

        static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(shared) as _;
        let source: Arc<dyn runtime::MorselSource> = Arc::clone(&shared.source) as _;
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst),
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        });
        shared
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once per statement payload"));
        *submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            drain_rg(rt, &rg);
            return Ok(Ceremony::Refused);
        }
        ptrace(&format!("engaged dop={launched} window={}", window(k)));

        // ---- the leader loop: segment/publish + ordered commit ----
        let mut seg = Segmentator::new(pgrcolumnar::format::RG_ROWS as u32);
        let mut published = 0u64;
        let mut next_commit = 0u64;
        let mut processed = 0u64;
        // copyfast lever 3: filled by the sort merge when armed.
        let mut inline_sample: Option<Vec<Vec<u8>>> = None;
        let mut input_done = false;
        let mut closed = false;
        let mut bytes_read = 0u64;
        // GL-PARQUET-1 inc-2: parquet morsels are row-group indexes known
        // from the footer — publish the whole task list up front and close
        // the source; the read pump below never runs (input_done).
        if let Some(pq) = &shared.parquet {
            published = pq.rg_order.len() as u64;
            shared.source.publish(published);
            shared.source.close();
            rt.notify_source_progress();
            input_done = true;
            closed = true;
            ptrace(&format!("parquet parallel engaged rgs={published}"));
        }
        // load-r3 M0: leader read-pump walls (block granularity — free).
        let t_loop = std::time::Instant::now();
        let mut t_read = std::time::Duration::ZERO;
        let mut t_seg = std::time::Duration::ZERO;
        let window = window(k);
        let mut ready: Vec<ChunkDesc> = Vec::new();
        let outcome = loop {
            // 1. Ordered commits of every ready RG.
            let mut committed_any = false;
            loop {
                let enc = {
                    let mut d = shared.done.lock().unwrap_or_else(|p| p.into_inner());
                    d.remove(&next_commit)
                };
                let Some(enc) = enc else { break };
                if let Some(enc) = enc {
                    processed += enc.nrows() as u64;
                    writer.commit_encoded_rg(enc)?;
                }
                next_commit += 1;
                committed_any = true;
            }
            if committed_any {
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed as i64);
            }
            if let Some(pq) = &shared.parquet {
                let b = pq.bytes_read.load(Ordering::Relaxed);
                if b != bytes_read {
                    bytes_read = b;
                    pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, b as i64);
                }
            }

            // 2. Read + segment + publish under the window (stop on error).
            let error_seen = shared.error_floor.load(Ordering::SeqCst) != u64::MAX;
            let mut read_any = false;
            if !input_done && !error_seen && published.saturating_sub(next_commit) < window {
                let mut buf = vec![0u8; READ_BLOCK];
                let tr = std::time::Instant::now();
                let n = cstate.copy_read_stream(&mut buf)?;
                t_read += tr.elapsed();
                bytes_read += n as u64;
                pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, bytes_read as i64);
                read_any = n > 0;
                if n > 0 {
                    buf.truncate(n);
                    let abuf = Arc::new(buf);
                    let ts = std::time::Instant::now();
                    let consumed = seg.feed(&abuf, n, &mut ready);
                    t_seg += ts.elapsed();
                    if seg.eoc {
                        // End-of-copy marker: never segment past it. A
                        // frontend stream drains protocol-level (serial's
                        // copy_read_line drain); files just stop.
                        let _ = consumed;
                        if matches!(cstate.src, CopySrc::Frontend { .. }) {
                            let mut sink = vec![0u8; READ_BLOCK];
                            while cstate.copy_read_stream(&mut sink)? > 0 {}
                        }
                        input_done = true;
                    }
                }
                if n == 0 && !input_done {
                    input_done = true;
                }
                if input_done {
                    seg.finish(&mut ready);
                }
                if !ready.is_empty() {
                    // EOL decided by now (any cut chunk saw a terminator);
                    // chunks >= 1 inherit it.
                    shared.eol.lock().unwrap_or_else(|p| p.into_inner()).later = seg.eol_type();
                    let mut m = shared.chunks.lock().unwrap_or_else(|p| p.into_inner());
                    for c in ready.drain(..) {
                        m.insert(published, c);
                        published += 1;
                    }
                    drop(m);
                    shared.source.publish(published);
                    rt.notify_source_progress();
                }
                if input_done && !closed {
                    shared.source.close();
                    rt.notify_source_progress();
                    closed = true;
                    ptrace(&format!(
                        "input closed chunks={published} rows={} bytes={bytes_read} read {:.2}s seg {:.2}s wall {:.2}s",
                        seg.rows_total,
                        t_read.as_secs_f64(),
                        t_seg.as_secs_f64(),
                        t_loop.elapsed().as_secs_f64(),
                    ));
                }
            } else if error_seen && !closed {
                // First error recorded: stop feeding; already-published
                // chunks above the floor drain in the workers.
                shared.source.close();
                rt.notify_source_progress();
                closed = true;
            }

            // 3. Completion / failure / cancel polling.
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                drain_rg(rt, &rg);
                return Err(e);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                let drained = drain_rg(rt, &rg);
                if claimed == 0 && drained && published == 0 && bytes_read == 0 {
                    return Ok(Ceremony::Refused);
                }
                if let Some(e) = shared.take_hard_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel COPY helpers exited before completing the load",
                )));
            }
            let refused = shared.refused.load(Ordering::SeqCst);
            let started = shared.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                drain_rg(rt, &rg);
                if bytes_read == 0 {
                    return Ok(Ceremony::Refused);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "parallel COPY: every helper refused participation mid-load",
                )));
            }

            // 4. Idle wait only when there is nothing to do (window full or
            // input done, and nothing committed this pass).
            if !committed_any && !read_any {
                if let Err(e) = parallel::wait_parallel_finish_quantum() {
                    drain_rg(rt, &rg);
                    return Err(e);
                }
            }
        };

        // RG complete: drain remaining ordered commits.
        loop {
            let enc = {
                let mut d = shared.done.lock().unwrap_or_else(|p| p.into_inner());
                d.remove(&next_commit)
            };
            let Some(enc) = enc else { break };
            if let Some(enc) = enc {
                processed += enc.nrows() as u64;
                writer.commit_encoded_rg(enc)?;
            }
            next_commit += 1;
        }

        if let Some(e) = shared.take_hard_error() {
            return Err(e);
        }
        // First-error-in-input-order: the minimum-chunk error wins (every
        // chunk below it completed or recorded its own, earlier error).
        if let Some(e) = shared.take_min_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "parallel COPY aborted")));
        }
        if shared.started.load(Ordering::SeqCst) == 0 {
            if bytes_read == 0 {
                return Ok(Ceremony::Refused);
            }
            return Err(Box::new(PgError::new(
                ERROR,
                "parallel COPY completed with no participating workers",
            )));
        }
        debug_assert_eq!(next_commit, published, "ordered commit hole");

        // load-r2 L3-1 sort mode: every parsed row lives in the run files;
        // workers flush their FINAL run post-drive (after the RG outcome),
        // so wait for actual worker exit, then k-way merge the runs into
        // the plain writer — the serial presort drain byte-path.
        if shared.sort.is_some() {
            let t_parse = std::time::Instant::now();
            while !parallel::parallel_workers_all_stopped(pcxt) {
                postgres_seams::check_for_interrupts::call()?;
                parallel::ProcessParallelMessages()?;
                parallel::wait_parallel_finish_quantum()?;
            }
            parallel::ProcessParallelMessages()?;
            if let Some(e) = shared.take_hard_error() {
                return Err(e);
            }
            ptrace(&format!("sort phase: worker drain {:.2}s", t_parse.elapsed().as_secs_f64()));
            let (n, sample) = merge_sorted_runs(writer, shared)?;
            processed = n;
            inline_sample = sample;
        }
        Ok(Ceremony::Done(processed, inline_sample))
    })(&mut submitted);

    // Teardown tail (every path): the RG must be COMPLETE before the
    // context is destroyed (helpers reference the payload until then).
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            shared.source.close();
            rt.notify_source_progress();
            if !drain_rg(rt, rg) {
                ereport(WARNING)
                    .errmsg("parallel COPY leaked a pinned resource group during teardown")
                    .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "ceremony"))?;
            }
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let out = body?;
    destroy?;
    Ok(out)
}

#[cfg(test)]
mod segmentator_tests {
    use super::*;

    fn segment(input: &[u8], rows_per_chunk: u32, block: usize) -> (Vec<ChunkDesc>, Segmentator) {
        let mut seg = Segmentator::new(rows_per_chunk);
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < input.len() && !seg.eoc {
            let hi = (off + block).min(input.len());
            let buf = Arc::new(input[off..hi].to_vec());
            let n = buf.len();
            seg.feed(&buf, n, &mut out);
            off = hi;
        }
        if !seg.eoc {
            seg.finish(&mut out);
        }
        (out, seg)
    }

    fn chunk_bytes(c: &ChunkDesc) -> Vec<u8> {
        let mut cur = ChunkCursor::new(c.segs.clone());
        let mut all = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = cur.read(&mut buf);
            if n == 0 {
                break;
            }
            all.extend_from_slice(&buf[..n]);
        }
        all
    }

    /// Chunks partition the input exactly, cut every rows_per_chunk rows,
    /// with 1-based first_lineno bookkeeping — at EVERY block size (buffer-
    /// edge carry states).
    #[test]
    fn partitions_lf_rows_exactly() {
        let mut input = Vec::new();
        for i in 0..25 {
            input.extend_from_slice(format!("row{i}\tv\n").as_bytes());
        }
        for block in [1, 2, 3, 7, 64, 4096] {
            let (chunks, seg) = segment(&input, 10, block);
            assert_eq!(seg.rows_total, 25, "block {block}");
            assert_eq!(chunks.len(), 3, "block {block}");
            assert_eq!(chunks[0].first_lineno, 1);
            assert_eq!(chunks[1].first_lineno, 11);
            assert_eq!(chunks[2].first_lineno, 21);
            let joined: Vec<u8> =
                chunks.iter().flat_map(|c| chunk_bytes(c)).collect();
            assert_eq!(joined, input, "block {block}");
        }
    }

    /// Escaped newlines are data: "a\<LF>b" is ONE row (odd backslash run),
    /// "a\\<LF>" ends a row (even run) — at every block size.
    #[test]
    fn backslash_parity_rules() {
        let input = b"a\\\nb\nc\\\\\nd\n".to_vec();
        // Rows: "a\<LF>b", "c\\", "d".
        for block in [1, 2, 3, 5, 64] {
            let (chunks, seg) = segment(&input, 1, block);
            assert_eq!(seg.rows_total, 3, "block {block}");
            assert_eq!(chunks.len(), 3, "block {block}");
            assert_eq!(chunk_bytes(&chunks[0]), b"a\\\nb\n".to_vec());
            assert_eq!(chunk_bytes(&chunks[1]), b"c\\\\\n".to_vec());
            assert_eq!(chunk_bytes(&chunks[2]), b"d\n".to_vec());
        }
    }

    /// CRLF detection + boundaries, including the \r|\n buffer-edge split.
    #[test]
    fn crlf_rows() {
        let input = b"a\r\nb\r\nc\r\n".to_vec();
        for block in [1, 2, 3, 4, 64] {
            let (chunks, seg) = segment(&input, 2, block);
            assert_eq!(seg.rows_total, 3, "block {block}");
            assert!(matches!(seg.eol, SegEol::Crnl));
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[1].first_lineno, 3);
        }
    }

    /// Classic-Mac CR rows.
    #[test]
    fn cr_rows() {
        let input = b"a\rb\rc\r".to_vec();
        for block in [1, 2, 64] {
            let (chunks, seg) = segment(&input, 10, block);
            assert_eq!(seg.rows_total, 3, "block {block}");
            assert!(matches!(seg.eol, SegEol::Cr));
            assert_eq!(chunks.len(), 1);
        }
    }

    /// A trailing unterminated line is a row (serial parses it too).
    #[test]
    fn trailing_partial_line() {
        let input = b"a\nb\nc-no-newline".to_vec();
        let (chunks, seg) = segment(&input, 10, 4);
        assert_eq!(seg.rows_total, 2, "boundaries only");
        assert_eq!(chunks.len(), 1);
        let joined = chunk_bytes(&chunks[0]);
        assert_eq!(joined, input);
    }

    /// End-of-copy marker: the marker LINE lands in the final chunk; bytes
    /// past it are never segmented.
    #[test]
    fn end_of_copy_marker() {
        let input = b"a\nb\n\\.\nGARBAGE AFTER".to_vec();
        for block in [1, 2, 3, 64] {
            let (chunks, seg) = segment(&input, 10, block);
            assert!(seg.eoc, "block {block}");
            let joined: Vec<u8> = chunks.iter().flat_map(|c| chunk_bytes(c)).collect();
            assert_eq!(joined, b"a\nb\n\\.\n".to_vec(), "block {block}");
        }
    }

    /// "\\." at line start is an escaped backslash + dot — NOT the marker.
    #[test]
    fn escaped_backslash_dot_is_not_eoc() {
        let input = b"\\\\.\nb\n".to_vec();
        let (chunks, seg) = segment(&input, 10, 2);
        assert!(!seg.eoc);
        assert_eq!(seg.rows_total, 2);
        assert_eq!(chunks.len(), 1);
    }

    /// Exact-RG cut: no empty trailing chunk when input ends on a boundary.
    #[test]
    fn no_empty_final_chunk() {
        let input = b"a\nb\n".to_vec();
        let (chunks, seg) = segment(&input, 2, 64);
        assert_eq!(seg.rows_total, 2);
        assert_eq!(chunks.len(), 1);
    }

    /// Chunk cursor reassembles multi-seg chunks byte-exactly.
    #[test]
    fn cursor_reassembles() {
        let mut input = Vec::new();
        for i in 0..100 {
            input.extend_from_slice(format!("{i}\tabcdefghij\n").as_bytes());
        }
        let (chunks, _) = segment(&input, 40, 17);
        let joined: Vec<u8> = chunks.iter().flat_map(|c| chunk_bytes(c)).collect();
        assert_eq!(joined, input);
    }
}
