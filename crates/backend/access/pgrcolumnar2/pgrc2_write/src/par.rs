//! Parallel ingest (chunk M3-I): ordered-commit parallel COPY on rtpool.
//!
//! ## The design (charter `pgrcolumnar-v2.md` §1 writer disciplines;
//! `lanev3-m3-chunks.md` §1.9 + §2/§5 M3-I)
//!
//! ONE seal implementation shared by serial and parallel ingest —
//! [`crate::seal::seal_part`] is the FROZEN boundary. Parallelism lives
//! entirely ABOVE it:
//!
//! 1. **Capture (leader).** The COPY leader copies incoming rows into
//!    [`RowChunk`]s — raw datum images, NO normalization (detoast is worker
//!    work). Chunk grain is fixed ([`ParIngestOpts::chunk_rows`], a multiple
//!    of 64 so chunk seams fall on validity-word boundaries).
//! 2. **Claim (the claim plane).** Chunks are MORSELS: deterministic
//!    fixed-grain units whose boundaries are a pure function of the input
//!    (never adaptive/arrival-dependent), published through a growing
//!    watermark and claimed by workers through the SHARED
//!    [`pgrc2_claim::ClaimCursor`] — the minimal XC-1/XC-2 substrate of the
//!    frozen parallel contract, of which parallel COPY is consumer #1 (the
//!    ruled COPY-on-morsels amendment: COPY is scheduled the same as the
//!    rest of the queries; the M3-L3 reader is consumer #2 on the same
//!    face, and the part-builder below is a SINK OPERATOR any pipeline can
//!    feed — CTAS/INSERT-SELECT land on it without a second path). Claim
//!    advance is the cursor's CAS; NO new thread population exists (§7.1
//!    law 6): work runs on pool workers, the leader only feeds and parks
//!    (the rtpool binding drives [`ParEngine::run_worker`]).
//! 3. **Normalize (workers).** A claimed chunk is normalized (detoast law:
//!    1B/4B-U/4B-C-pglz, external via the passed capability) into chunk-local
//!    [`ColBuffer`]s by the SAME `append_*` calls serial ingest uses.
//! 4. **Cut (in input order, deterministic).** A cut cursor consumes
//!    normalized chunk STATS strictly in input order and closes a part when
//!    the shared [`PartCutPolicy::should_cut`] predicate trips at a chunk
//!    boundary — a pure function of the input stream (schedule- and
//!    dop-independent), so the part partition is deterministic. Because the
//!    chunk grain IS the policy's cut granule and the serial writer asks the
//!    same predicate at the same granularity, the partition coincides with
//!    the serial writer's EXACTLY — under the byte budget as well as the row
//!    budget (M3-I; before the granule gate the byte-budget case legally
//!    diverged and only O-10 logical identity held). Whichever task advances the
//!    cursor over a closing boundary OWNS the closed part (the
//!    decrement-to-zero shape: one lock-section decides readiness AND
//!    admissibility; no task ever waits for another task).
//! 5. **Assemble + seal (workers, parallel across parts).** The owning task
//!    replays the part's chunks IN INPUT ORDER through
//!    [`ColBuffer::splice_chunk`] — the seam-replayed trackers (constancy,
//!    logical multiset hash, counters) — producing part-level `ColBuffer`s
//!    bit-identical to serial accumulation of the same rows, then calls the
//!    frozen `seal_part` with a pre-assigned seq. Byte-identical-parts law:
//!    `seal_part` is deterministic in (ColBuffers, spec, fxid, seq), and the
//!    merged ColBuffers are a pure function of the partition — so ANY claim
//!    schedule yields byte-identical part files (§1.9; witnessed by the
//!    permuted-schedule suite).
//! 6. **Ordered commit (leader).** Sealed parts gather by seq; the ordered
//!    seq list deposits into the [`crate::writer::TableWriter`], and publish
//!    stays the serial spec §13.3 path — strictly above the frozen boundary.
//!
//! ## Coordination state (the claim-channel discipline)
//!
//! All shared coordination state lives in ONE `pgsync::Mutex` ([`Coord`])
//! with a single Condvar on which ONLY THE LEADER ever waits (backpressure +
//! drain). Worker tasks never block on other TASKS — run-to-completion, so
//! the pool cannot wedge (the GL-GANGWEDGE class is structurally excluded).
//! The one sanctioned worker wait is the WAKE FACE (FIX-B, the
//! ingest-contention charter): a claim-STARVED worker parks on an
//! eventcount keyed to publish progress ([`ParEngine::park_wake`]) instead
//! of yield-polling the watermark; every wake source is leader/pump
//! progress or cancellation (publish, close, cancel, driver poke) — never
//! another worker — so the no-wedge argument is unchanged. The chunk claim
//! channel itself is the runtime scheduler's claim word; this module adds
//! no bare-atomic channel. The same faces run under loom (`tests/loom.rs`
//! — real faces, real fences, never mirror models; the parked-claimer
//! wake protocol is models 3–5).
//!
//! ## Teardown / byref enumeration (§7.1 law 2)
//!
//! On ANY failure (feed error, worker normalize error, seal error, vfs
//! crash): the first error is recorded (first-wins under the lock), the
//! session is cancelled, remaining claims drain as no-ops, the leader waits
//! for the runtime drain (no writer can be mid-file afterwards), then unlinks
//! every assigned part temp by name (`tmp-<fxid>-<seq>`, ENOENT-tolerant).
//! Stranded state classes at teardown: captured `RowChunk`s and normalized
//! chunk `ColBuffer`s (owned, dropped with the engine — no byref borrows
//! cross the session boundary; every datum was copied at capture), sealed
//! temp FILES (unlinked here; after kill -9 the recovery scan reclaims them
//! by name — the same `tmp-` discipline as serial, proven in the crash
//! matrix at dop>1). Nothing else escapes: providers hand out per-task
//! instances that die with the task.
//!
//! ## What this module deliberately does NOT do
//!
//! - No per-work-item ceremony (§7.1 law 4): per-CHUNK lane instantiation is
//!   a scratch Vec + one Box per 65k-row chunk — nothing RG/GUC/plan-shaped.
//! - No session/txn state: fxid, providers, policy all arrive as passed
//!   capabilities (the crate law). No thread-locals (census pinned).
//! - No new thread population and no std thread spawn anywhere in this
//!   module — the rtpool-only assertion pins this structurally (source-scan
//!   test in the `pgrc2_ingest_par` binding crate).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use pgrc2_claim::{ClaimCursor, ClaimOutcome, MorselSource, NoObserver};
use pgsync::{lock, Condvar, Mutex};

use pgrc2_format::abi::ColumnMetaBuilder;
use pgrc2_format::class::{ColSchema, StorageClass};
use pgrc2_format::dirlayout::temp_file_name;
use pgrc2_format::relopt::ShredOptions;

use crate::elect::CandidateSource;
use crate::ingest::{
    normalize_varlena, ColBuffer, DictEntries, ExternalDetoast, NoExternalDetoast, RawDatum,
    DICT_ROW_PLAIN,
};
use crate::seal::{seal_part, PartSpec, SealReport, SealedPart, VerifyResolver};
use crate::shred::{validate_lanes, NoShred, ShredLane, ShredLaneSource};
use crate::structural::StructuralPolicy;
use crate::writer::PartCutPolicy;
use crate::wvfs::{MemVfs, RealVfs, WriteVfs, WvFd};
use crate::{WriteError, WriteResult};

/// Default capture-chunk grain — pinned to the cut granule.
///
/// The cut cursor closes a part only at a chunk boundary, so the chunk
/// grain MUST equal [`PartCutPolicy::cut_granule_rows`]: a chunk coarser
/// than the granule leaves the serial writer's partition unreachable, which
/// is exactly the byte-budget divergence M3-I exists to close.
/// [`ParEngine::new`] enforces the equality. Must be a multiple of 64
/// (validity-word seam law); 8,192 is.
///
/// This is 8x finer than the previous 65,536 (one band). Coordination cost
/// scales with chunk count — one lock section per chunk, so 12,207 chunks
/// at 100M rows instead of 1,526 — which is negligible against per-chunk
/// normalize+seal work.
pub const DEFAULT_CHUNK_ROWS: u32 = crate::writer::DEFAULT_CUT_GRANULE_ROWS;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Parallel-session bounds. Chunk grain is FIXED per session — the part
/// partition is a pure function of (input stream, chunk_rows, policy),
/// independent of dop and claim schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParIngestOpts {
    /// Rows per capture chunk (% 64 == 0, > 0).
    pub chunk_rows: u32,
    /// Leader backpressure: captured-but-unclaimed chunk bound.
    pub max_chunks_in_flight: usize,
    /// Leader backpressure: cut-but-unsealed part bound (memory ceiling —
    /// each open part holds its whole-part buffers, exactly as serial does
    /// for its one part).
    ///
    /// This bound carries TWO roles (the ingest-contention charter's §5
    /// FIX-A finding): it is the open-part MEMORY ceiling and,
    /// accidentally, the seal CONCURRENCY cap — seal is single-threaded
    /// per part, so `max_parts_in_flight` slots ÷ seal cpu-s/part is a
    /// hard parts/s ceiling on the whole pipeline (measured: 8 slots ÷
    /// ~2.0 cpu-s = the 4.0 parts/s wall at 100m). Sessions with a real
    /// byte budget derive it via [`ParIngestOpts::parts_bound_for_budget`]
    /// so memory is bounded by admission arithmetic and seal parallelism
    /// is never capped below the worker count; the default stays the
    /// legacy 8 (the conservative posture for undirected sessions).
    pub max_parts_in_flight: usize,
}

/// The legacy open-parts bound (the glibc-retention-era sizing) — the
/// floor of the derived bound: no platform regresses below the posture
/// every shipped spec ran safely.
pub const LEGACY_MAX_PARTS_IN_FLIGHT: usize = 8;

impl ParIngestOpts {
    /// FIX-A (ingest-contention charter §5): derive the open-parts bound
    /// from a byte budget instead of a bare constant.
    ///
    /// Per-part in-flight footprint operand: an open part holds its
    /// normalized chunk buffers (≈ `policy.max_bytes` at cut — the cut
    /// predicate trips just past the byte budget) and, during assembly,
    /// their spliced whole-part twin (≈ `policy.max_bytes` again;
    /// [`ParEngine`]'s seal path frees each chunk as it splices, so the
    /// pair is the ceiling, not the steady state) plus encode transients —
    /// `2 × max_bytes` brackets the measured 0.39–0.61 GB per open part at
    /// the 100m record spec (`max_bytes` = 256 MiB → operand 512 MiB; the
    /// charter's §3.5/§5 pricing).
    ///
    /// The bound: how many such parts the caller's parts budget holds,
    /// floored at [`LEGACY_MAX_PARTS_IN_FLIGHT`] and ceiled at `2 × dop` —
    /// at most `dop` parts can be actively sealing (seal is worker work),
    /// and one further wave of closed-awaiting-seal parts keeps the cut
    /// cursor from stalling while every worker seals; slots beyond that
    /// buy no concurrency, only deepen the priced worst case.
    pub fn parts_bound_for_budget(
        policy: &crate::writer::PartCutPolicy,
        dop: usize,
        parts_budget_bytes: u64,
    ) -> usize {
        let per_part = policy.max_bytes.saturating_mul(2).max(1);
        let fit = (parts_budget_bytes / per_part).min(usize::MAX as u64) as usize;
        let ceil = dop.saturating_mul(2).max(LEGACY_MAX_PARTS_IN_FLIGHT);
        fit.clamp(LEGACY_MAX_PARTS_IN_FLIGHT, ceil)
    }
}

impl Default for ParIngestOpts {
    fn default() -> ParIngestOpts {
        ParIngestOpts {
            chunk_rows: DEFAULT_CHUNK_ROWS,
            // Chunks are 8x finer than they were, so hold 8x more of them
            // to keep the same ~1Mi-row in-flight capture window (and so a
            // dop-16 pool is never starved by the bound itself).
            max_chunks_in_flight: 128,
            max_parts_in_flight: LEGACY_MAX_PARTS_IN_FLIGHT,
        }
    }
}

// ---------------------------------------------------------------------------
// Provider seams (per-task instantiation — the M2-A face precedent)
// ---------------------------------------------------------------------------

/// Mints a Vfs handle per seal task / cleanup pass. Production: [`RealVfsProvider`]
/// (a ZST per handle — the kernel owns all state, zero contention).
/// Tests/crash-matrix: [`SharedMemVfsProvider`] over one locked [`MemVfs`]
/// universe.
pub trait VfsProvider: Send + Sync {
    fn make(&self) -> Box<dyn WriteVfs + Send>;
}

/// Mints an external-detoast capability per normalize task (COPY under
/// O-M3-1(a) feeds inline datums, so [`NoDetoastProvider`] is the M3
/// default, same as serial).
pub trait DetoastProvider: Send + Sync {
    fn make(&self) -> Box<dyn ExternalDetoast + Send>;
}

/// Mints a shred-lane source per part-assembly task.
pub trait ShredProvider: Send + Sync {
    fn make(&self) -> Box<dyn ShredLaneSource + Send>;
}

/// Production Vfs provider: every handle is a fresh [`RealVfs`] ZST.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealVfsProvider;

impl VfsProvider for RealVfsProvider {
    fn make(&self) -> Box<dyn WriteVfs + Send> {
        Box::new(RealVfs)
    }
}

/// The M3 default detoast provider (typed refusal, unreachable from COPY).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoDetoastProvider;

impl DetoastProvider for NoDetoastProvider {
    fn make(&self) -> Box<dyn ExternalDetoast + Send> {
        Box::new(NoExternalDetoast)
    }
}

/// The M3-D default shred provider (image lane only).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoShredProvider;

impl ShredProvider for NoShredProvider {
    fn make(&self) -> Box<dyn ShredLaneSource + Send> {
        Box::new(NoShred)
    }
}

/// One shared [`MemVfs`] universe behind a lock — the dop>1 test/crash
/// vehicle. File BYTES are schedule-independent (each seal task owns its own
/// temp file exclusively); only op-log interleaving varies. `crash_at_op`
/// composes: after the armed op, EVERY handle's ops fail (`killed`), exactly
/// a process-wide kill -9.
#[derive(Clone)]
pub struct SharedMemVfs(pub Arc<Mutex<MemVfs>>);

impl SharedMemVfs {
    pub fn new(v: MemVfs) -> SharedMemVfs {
        SharedMemVfs(Arc::new(Mutex::new(v)))
    }

    /// Run `f` over the underlying universe (arm crashes, read op logs,
    /// revive).
    pub fn with<R>(&self, f: impl FnOnce(&mut MemVfs) -> R) -> R {
        f(&mut lock(&self.0))
    }
}

impl WriteVfs for SharedMemVfs {
    fn create_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        lock(&self.0).create_rw(path)
    }
    fn open_rw(&mut self, path: &str) -> WriteResult<WvFd> {
        lock(&self.0).open_rw(path)
    }
    fn pwrite_at(&mut self, fd: &WvFd, off: u64, bytes: &[u8]) -> WriteResult<()> {
        lock(&self.0).pwrite_at(fd, off, bytes)
    }
    fn fsync_file(&mut self, fd: &WvFd) -> WriteResult<()> {
        lock(&self.0).fsync_file(fd)
    }
    fn close_file(&mut self, fd: WvFd) -> WriteResult<()> {
        lock(&self.0).close_file(fd)
    }
    fn read_full(&mut self, path: &str) -> WriteResult<Vec<u8>> {
        lock(&self.0).read_full(path)
    }
    fn rename_path(&mut self, from: &str, to: &str) -> WriteResult<()> {
        lock(&self.0).rename_path(from, to)
    }
    fn unlink_path(&mut self, path: &str) -> WriteResult<()> {
        lock(&self.0).unlink_path(path)
    }
    fn fsync_dir(&mut self, dir: &str) -> WriteResult<()> {
        lock(&self.0).fsync_dir(dir)
    }
    fn list_dir(&mut self, dir: &str) -> WriteResult<Vec<String>> {
        lock(&self.0).list_dir(dir)
    }
    fn mkdir_path(&mut self, dir: &str) -> WriteResult<()> {
        lock(&self.0).mkdir_path(dir)
    }
    fn exists_path(&mut self, path: &str) -> WriteResult<bool> {
        lock(&self.0).exists_path(path)
    }
}

/// Test provider over a [`SharedMemVfs`].
pub struct SharedMemVfsProvider(pub SharedMemVfs);

impl VfsProvider for SharedMemVfsProvider {
    fn make(&self) -> Box<dyn WriteVfs + Send> {
        Box::new(self.0.clone())
    }
}

/// Everything a parallel session's workers need, all `Send + Sync` owned
/// capabilities (`TaskSetWork` is `'static` — nothing borrows the leader's
/// stack).
pub struct ParProviders {
    pub vfs: Arc<dyn VfsProvider>,
    pub detoast: Arc<dyn DetoastProvider>,
    pub shred: Arc<dyn ShredProvider>,
    pub sources: Vec<Arc<dyn CandidateSource + Send + Sync>>,
    pub resolver: Arc<dyn VerifyResolver + Send + Sync>,
    pub shred_opts: ShredOptions,
    /// Structural-election facts (TY-1) — the parallel seal must elect
    /// exactly as serial (byte-identical-parts law). Default: none.
    pub structural: StructuralPolicy,
}

// ---------------------------------------------------------------------------
// RowChunk — leader-side raw capture
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Cell {
    Null,
    Word(u64),
    Bytes { off: u64, len: u64 },
}

/// One column's D2 side channel over a captured chunk: per-row
/// `(source, code)` slots plus the chunk-local source-dictionary table
/// (SEAL-SPEED-2; see [`crate::ingest::DictSide`]). Valid at normalize
/// only when it covers every chunk row — the coverage law.
#[derive(Default)]
struct DictLaneAcc {
    sources: Vec<std::sync::Arc<dyn DictEntries>>,
    rows: Vec<u64>,
}

impl DictLaneAcc {
    fn source_idx(&mut self, source: &std::sync::Arc<dyn DictEntries>) -> u32 {
        match self
            .sources
            .iter()
            .position(|s| std::sync::Arc::ptr_eq(s, source))
        {
            Some(i) => i as u32,
            None => {
                self.sources.push(source.clone());
                (self.sources.len() - 1) as u32
            }
        }
    }
}

/// One captured input chunk: raw datum images copied row-major into an
/// owned arena (Send-able; normalization deferred to the claiming worker).
pub struct RowChunk {
    ncols: usize,
    rows: u32,
    cells: Vec<Cell>,
    arena: Vec<u8>,
    /// D2 side channels, one slot per column (empty = no channel).
    dict_lanes: Vec<DictLaneAcc>,
}

impl RowChunk {
    /// Public for drivers and the loom/permutation suites (the capture
    /// currency); production capture goes through [`ParSession::append_row`].
    pub fn new(ncols: usize, chunk_rows: u32) -> RowChunk {
        RowChunk {
            ncols,
            rows: 0,
            cells: Vec::with_capacity(ncols * chunk_rows as usize),
            arena: Vec::new(),
            dict_lanes: (0..ncols).map(|_| DictLaneAcc::default()).collect(),
        }
    }

    /// D2: append one row's `(source, code)` slot to column `col`'s side
    /// channel (the pump's seam-stitch face). `code = None` = a PLAIN /
    /// code-free row within a dict-carrying source (the hybrid law).
    pub fn push_dict_code(
        &mut self,
        col: usize,
        source: &std::sync::Arc<dyn DictEntries>,
        code: Option<u32>,
    ) {
        let lane = &mut self.dict_lanes[col];
        match code {
            Some(c) => {
                let s = lane.source_idx(source);
                lane.rows.push((u64::from(s) << 32) | u64::from(c));
            }
            None => {
                // Source membership still registers (a chunk whose rows are
                // all fallback keeps its dict-carrying provenance).
                let _ = lane.source_idx(source);
                lane.rows.push(DICT_ROW_PLAIN);
            }
        }
    }

    /// D2: bulk-attach a whole aligned span of column `col`'s codes (the
    /// decode-worker face; `u32::MAX` slots = PLAIN rows). The span must
    /// align with the rows already pushed for this chunk.
    pub fn attach_dict_span(
        &mut self,
        col: usize,
        source: &std::sync::Arc<dyn DictEntries>,
        codes: &[u32],
    ) {
        let lane = &mut self.dict_lanes[col];
        let s = lane.source_idx(source);
        lane.rows.reserve(codes.len());
        for &c in codes {
            lane.rows.push(if c == u32::MAX {
                DICT_ROW_PLAIN
            } else {
                (u64::from(s) << 32) | u64::from(c)
            });
        }
    }

    pub fn rows(&self) -> u32 {
        self.rows
    }

    pub fn ncols(&self) -> usize {
        self.ncols
    }

    pub fn push_row(&mut self, row: &[RawDatum<'_>]) -> WriteResult<()> {
        if row.len() != self.ncols {
            return Err(WriteError::Contract {
                detail: "row width mismatch",
            });
        }
        for d in row {
            match d {
                RawDatum::Null => self.cells.push(Cell::Null),
                RawDatum::Word(w) => self.cells.push(Cell::Word(*w)),
                RawDatum::Bytes(b) => {
                    let off = self.arena.len() as u64;
                    self.arena.extend_from_slice(b);
                    self.cells.push(Cell::Bytes {
                        off,
                        len: b.len() as u64,
                    });
                }
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Worker-side normalization: replay the captured rows through the SAME
    /// per-class appends serial ingest uses (detoast law included) into
    /// chunk-local `ColBuffer`s.
    fn normalize(
        &self,
        schema: &[ColSchema],
        ext: &mut dyn ExternalDetoast,
    ) -> WriteResult<Vec<ColBuffer>> {
        let mut cols: Vec<ColBuffer> = schema.iter().map(|s| ColBuffer::new(*s)).collect();
        let mut scratch: Vec<u8> = Vec::new();
        for r in 0..self.rows as usize {
            for (c, col) in cols.iter_mut().enumerate() {
                let cell = self.cells[r * self.ncols + c];
                match (cell, col.schema.class) {
                    (Cell::Null, _) => col.append_null(),
                    (Cell::Word(w), StorageClass::ByvalWord { .. })
                    | (Cell::Word(w), StorageClass::F32)
                    | (Cell::Word(w), StorageClass::F64)
                    | (Cell::Word(w), StorageClass::Bool) => col.append_word(w)?,
                    (Cell::Bytes { off, len }, StorageClass::Fixed { .. }) => {
                        col.append_fixed(&self.arena[off as usize..(off + len) as usize])?
                    }
                    (Cell::Bytes { off, len }, StorageClass::VarlenaVerbatim) => {
                        let image = &self.arena[off as usize..(off + len) as usize];
                        let (payload, _) = normalize_varlena(image, ext, &mut scratch)?;
                        col.append_varlena_payload(payload)?;
                    }
                    _ => {
                        return Err(WriteError::Contract {
                            detail: "datum shape does not match column class",
                        })
                    }
                }
            }
        }
        // D2: hand covered side channels to the chunk buffers (the coverage
        // law — an uncovered lane never attaches; splice drops mixed parts).
        for (c, col) in cols.iter_mut().enumerate() {
            let lane = &self.dict_lanes[c];
            if !lane.sources.is_empty() && lane.rows.len() as u32 == self.rows {
                col.attach_dict_side(lane.sources.clone(), lane.rows.clone())?;
            }
        }
        Ok(cols)
    }
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

struct NormChunk {
    cols: Vec<ColBuffer>,
    rows: u64,
}

/// One closed part, owned by exactly one task (the cursor-advancer that
/// closed it).
struct PartJob {
    seq: u32,
    /// First input-chunk ordinal of the part — the error-ordinal currency
    /// (serial-replay error parity: errors report in INPUT order).
    first_chunk: u64,
    chunks: Vec<NormChunk>,
}

#[derive(Default)]
struct Coord {
    /// Captured chunks awaiting a worker claim, by chunk id.
    captured: BTreeMap<u64, RowChunk>,
    /// Normalized chunks awaiting the cut cursor, by chunk id.
    normalized: BTreeMap<u64, NormChunk>,
    /// Next chunk id the cut cursor consumes (ids below are assigned).
    cut_cursor: u64,
    part_start_chunk: u64,
    part_rows: u64,
    /// The open part's byte metric, kept SERIAL-EXACT (#597): the words +
    /// validity contribution, which is seam-invariant (chunk seams sit on
    /// 64-row validity words), sums flat here...
    part_flat_bytes: u64,
    /// ...while each column's heap accumulates through the same
    /// pad-to-8-then-add arithmetic the part assembler's splice runs at
    /// every interior chunk seam ([`ColBuffer::splice_chunk`]). Summing
    /// chunk-LOCAL `approx_bytes` instead — the pre-#597 metric — omits
    /// those seam pads (0-7 bytes per byref column per seam) and cuts a
    /// different granule boundary than serial once the skew straddles
    /// `max_bytes`. Indexed by column; sized at engine construction.
    part_heap_bytes: Vec<u64>,
    /// Parts closed but not yet sealed (backpressure input).
    parts_open: usize,
    /// High-water of `parts_open` (the FIX-A seal-concurrency witness:
    /// the parts-pace gate cell wants observed > the legacy 8).
    peak_parts_open: usize,
    /// Parts closed so far (seq = base_seq + i).
    assigned_parts: u32,
    /// Set at close_input: total published chunks.
    total_chunks: Option<u64>,
    /// Sealed gather, by seq.
    sealed: BTreeMap<u32, (SealedPart, SealReport)>,
    /// Lowest-INPUT-ORDINAL error wins (serial-replay error parity, IN-2):
    /// serial ingest surfaces the FIRST error in input order, so the
    /// parallel session reports the recorded error with the lowest chunk
    /// ordinal — for a session whose first defect is chunk-attributable,
    /// the surfaced error is exactly the serial one. Any error still
    /// cancels the session immediately (claims drain as no-ops), so a
    /// lower-ordinal defect sitting in a never-normalized chunk is not
    /// discovered — exact multi-defect parity is the binding driver's
    /// serial replay, documented at the seam.
    error: Option<(u64, WriteError)>,
    /// Witness: pool worker ordinals that ran ingest work.
    workers_seen: BTreeSet<usize>,
}

/// The parallel-session engine: coordination + assembly + seal, independent
/// of the runtime binding (loom drives these faces directly; the rtpool
/// binding drives [`ParEngine::run_worker`] on pool workers).
pub struct ParEngine {
    mu: Mutex<Coord>,
    /// Leader-only waits (backpressure); workers only ever notify.
    cv: Condvar,
    /// FIX-B cancel face (ingest-contention charter S1): the session-
    /// cancelled flag, readable WITHOUT the coordinator mutex — the claim
    /// loop's not-yet-published arm used to lock the GLOBAL mutex on
    /// every poll just to read this one bool (the measured poll-storm
    /// face). Written only inside [`ParEngine::record_error`] (which
    /// holds the Coord lock); monotone false → true.
    cancelled: AtomicBool,
    /// FIX-B wake face (the parked-claimer protocol, charter S6): a
    /// claim-starved worker parks here instead of yield-polling the
    /// claim plane (the measured ~5.5M sched_yield/s storm). Eventcount
    /// shape: `wake_seq` bumps BEFORE every notify and the parker
    /// re-checks it under `wake_mu`, so a poke landing anywhere after
    /// the caller's [`ParEngine::wake_seq`] snapshot makes
    /// [`ParEngine::park_wake`] return without sleeping — no lost wake
    /// (loom models 3–5 in `tests/loom.rs`). Pokers: `publish_chunk`
    /// (watermark moved), `close_input` (drain), `record_error` (cancel),
    /// and the driver's own faces via [`ParEngine::poke_wake`]. Workers
    /// still never wait on other WORKERS — every poke source is
    /// leader/pump progress or cancellation (the run-to-completion law;
    /// GL-GANGWEDGE stays structurally excluded).
    wake_seq: AtomicU64,
    wake_mu: Mutex<()>,
    wake_cv: Condvar,
    /// The shared claim cursor (PC-2.3): chunk-morsel claims ride its CAS;
    /// dispatch takes no locks.
    claim: ClaimCursor,
    /// Publish watermark (units `< published` are claimable). Monotone;
    /// bumped by the leader AFTER the captured chunk is visible.
    published_units: AtomicU64,
    /// Total chunk count once input closes; `u64::MAX` = still growing.
    closed_total: AtomicU64,
    providers: ParProviders,
    schema: Vec<ColSchema>,
    spec: PartSpec,
    table_dir: String,
    fxid: u64,
    policy: PartCutPolicy,
    opts: ParIngestOpts,
    base_seq: u32,
    /// SEAL-EARLY lead-in witnesses (print-only diagnosis marks; bank
    /// bytes cannot see them): the engine's birth instant plus first-event
    /// offsets in microseconds (0 = not yet happened) — first published
    /// chunk, first part close (cut), first completed seal. The measured
    /// law they witness: on a device-bound volume every second before the
    /// first completed seal (= the first device write) is a second of
    /// wall, one-for-one.
    born: std::time::Instant,
    mark_first_publish_us: AtomicU64,
    mark_first_cut_us: AtomicU64,
    mark_first_seal_us: AtomicU64,
}

/// Outcome of one worker claim probe ([`ParEngine::step_claim`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimStep {
    /// A span was claimed and run.
    Progress,
    /// Nothing claimable yet: the space is still growing and the cursor
    /// sits at the watermark. The caller may park on the wake face
    /// (snapshot [`ParEngine::wake_seq`] BEFORE probing, then
    /// [`ParEngine::park_wake`]).
    Starved,
    /// The morsel space is drained (or the session cancelled at the
    /// watermark) — no further claims will ever exist.
    Drained,
}

/// The COPY morsel space (consumer #1 of the claim plane): units are
/// capture chunks — deterministic fixed-grain morsels (boundary placement
/// is a pure function of the input: `chunk_rows` is fixed per session).
/// No interior hard boundaries: part edges are the SINK's (the cut cursor
/// decides them from accumulated bytes; claims are normalize-work, which
/// is part-agnostic).
struct ChunkMorsels<'e>(&'e ParEngine);

impl MorselSource for ChunkMorsels<'_> {
    fn total_units(&self) -> Option<u64> {
        let t = self.0.closed_total.load(Ordering::Acquire);
        if t == u64::MAX {
            None
        } else {
            Some(t)
        }
    }
    fn published(&self) -> u64 {
        self.0.published_units.load(Ordering::Acquire)
    }
    fn next_boundary_after(&self, _unit: u64) -> u64 {
        u64::MAX
    }
}

impl ParEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        providers: ParProviders,
        schema: Vec<ColSchema>,
        spec: PartSpec,
        table_dir: String,
        fxid: u64,
        policy: PartCutPolicy,
        opts: ParIngestOpts,
        base_seq: u32,
    ) -> WriteResult<ParEngine> {
        if opts.chunk_rows == 0 || opts.chunk_rows % 64 != 0 {
            return Err(WriteError::Contract {
                detail: "chunk_rows must be a positive multiple of 64",
            });
        }
        // The byte-identity invariant: the cursor can only close a part at a
        // chunk boundary, so the chunk grain must BE the cut granule. Any
        // other pairing silently reproduces the divergence M3-I closed.
        if opts.chunk_rows != policy.cut_granule_rows {
            return Err(WriteError::Contract {
                detail: "chunk_rows must equal the policy's cut_granule_rows",
            });
        }
        if opts.max_chunks_in_flight == 0 || opts.max_parts_in_flight == 0 {
            return Err(WriteError::Contract {
                detail: "parallel in-flight bounds must be positive",
            });
        }
        if schema.is_empty() {
            return Err(WriteError::Contract {
                detail: "table with zero columns",
            });
        }
        Ok(ParEngine {
            mu: Mutex::new(Coord {
                part_heap_bytes: vec![0; schema.len()],
                ..Coord::default()
            }),
            cv: Condvar::new(),
            cancelled: AtomicBool::new(false),
            wake_seq: AtomicU64::new(0),
            wake_mu: Mutex::new(()),
            wake_cv: Condvar::new(),
            claim: ClaimCursor::new(),
            published_units: AtomicU64::new(0),
            closed_total: AtomicU64::new(u64::MAX),
            providers,
            schema,
            spec,
            table_dir,
            fxid,
            policy,
            opts,
            base_seq,
            born: std::time::Instant::now(),
            mark_first_publish_us: AtomicU64::new(0),
            mark_first_cut_us: AtomicU64::new(0),
            mark_first_seal_us: AtomicU64::new(0),
        })
    }

    /// Stamp a first-event mark once (0 -> now; later calls no-op). A
    /// same-microsecond first event stores 1 — the marks are diagnosis
    /// grain, never arithmetic inputs.
    fn stamp_once(&self, mark: &AtomicU64) {
        if mark.load(Ordering::Relaxed) == 0 {
            let us = self.born.elapsed().as_micros().max(1) as u64;
            let _ = mark.compare_exchange(0, us, Ordering::Relaxed, Ordering::Relaxed);
        }
    }

    /// SEAL-EARLY lead-in marks, seconds since engine birth: (first
    /// publish, first part close, first completed seal); 0.0 = never
    /// happened. Diagnosis witnesses only.
    pub fn leadin_marks(&self) -> (f64, f64, f64) {
        let s = |m: &AtomicU64| m.load(Ordering::Relaxed) as f64 / 1e6;
        (
            s(&self.mark_first_publish_us),
            s(&self.mark_first_cut_us),
            s(&self.mark_first_seal_us),
        )
    }

    /// Leader: publish one captured chunk under backpressure. Returns the
    /// chunk id (the caller owes a `StreamSource::publish(id + 1)` + runtime
    /// wake AFTER this returns). Blocks while in-flight bounds are full;
    /// surfaces the session error if one is recorded.
    pub fn publish_chunk(&self, chunk: RowChunk, id: u64) -> WriteResult<()> {
        let mut c = lock(&self.mu);
        loop {
            if let Some((_, e)) = &c.error {
                return Err(e.clone());
            }
            if c.captured.len() < self.opts.max_chunks_in_flight
                && c.parts_open < self.opts.max_parts_in_flight
            {
                break;
            }
            c = self.cv.wait(c).unwrap_or_else(|e| e.into_inner());
        }
        c.captured.insert(id, chunk);
        drop(c);
        self.stamp_once(&self.mark_first_publish_us);
        // Publish watermark AFTER the chunk is visible under the lock:
        // a claimer that wins ordinal `id` must find it captured.
        self.published_units.fetch_max(id + 1, Ordering::AcqRel);
        // FIX-B: the watermark moved — wake parked claimers.
        self.poke_wake();
        Ok(())
    }

    /// Wake-face snapshot — sample BEFORE probing any claim face, then
    /// pass to [`ParEngine::park_wake`] if every face says "nothing yet".
    /// The no-lost-wake protocol rides this ordering: any poke after the
    /// snapshot moves the sequence, and `park_wake` re-checks it under
    /// the wake mutex before sleeping.
    pub fn wake_seq(&self) -> u64 {
        self.wake_seq.load(Ordering::Acquire)
    }

    /// Park until the wake sequence moves past `seen` (publish progress,
    /// input close, cancellation, or a driver poke). Returns immediately
    /// if it already has. The caller re-probes its claim faces on return
    /// (the wake is a hint, never a hand-off).
    pub fn park_wake(&self, seen: u64) {
        let mut g = lock(&self.wake_mu);
        while self.wake_seq.load(Ordering::Acquire) == seen {
            g = self.wake_cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Bump the wake face. Engine-internal pokes cover the engine's own
    /// faces (publish / close / cancel); this is public so a driver whose
    /// workers fold MORE faces into the same park (the sortload merge
    /// loop: ticket-queue growth, per-segment decode-window advance,
    /// driver-plane cancellation) can wake them when one of ITS faces
    /// grows. All poke sources are leader/pump progress or cancellation —
    /// never worker-on-worker waiting.
    pub fn poke_wake(&self) {
        // Bump BEFORE the notify, notify with the mutex held (the #941
        // wake-not-lost discipline): a parker that snapshotted before
        // this bump either re-checks under the mutex and skips the wait,
        // or is already registered on the condvar and receives it.
        self.wake_seq.fetch_add(1, Ordering::AcqRel);
        let _g = lock(&self.wake_mu);
        self.wake_cv.notify_all();
    }

    /// Record an error at input ordinal `ord` — LOWEST ordinal wins (the
    /// error-parity selection; see [`Coord::error`]). Always cancels.
    fn record_error(&self, c: &mut Coord, ord: u64, e: WriteError) {
        match &c.error {
            Some((prev, _)) if *prev <= ord => {}
            _ => c.error = Some((ord, e)),
        }
        // Cancel is the atomic flag (FIX-B: claim polls read it lock-free);
        // the store happens under the Coord lock the caller holds, so
        // lock-section readers keep their one consistent view.
        self.cancelled.store(true, Ordering::SeqCst);
        self.cv.notify_all();
        // Parked claimers must observe the cancel (the cancel-wake leg of
        // the parked-claimer protocol; loom model 4). Lock order is
        // Coord → wake only, never the reverse (park_wake holds only the
        // wake mutex; wake_seq() is lock-free).
        self.poke_wake();
    }

    /// Leader: record a session-fatal error (feed failure). Feed errors
    /// carry the ordinal of the chunk being fed (input order); a leader
    /// with no ordinal context passes `u64::MAX` (any chunk-attributed
    /// error is earlier in input order than an end-of-feed failure).
    pub fn fail(&self, e: WriteError) {
        self.fail_at(u64::MAX, e)
    }

    /// [`ParEngine::fail`] with an explicit input ordinal.
    pub fn fail_at(&self, ord: u64, e: WriteError) {
        let mut c = lock(&self.mu);
        self.record_error(&mut c, ord, e);
    }

    /// Leader: end of input. `total` = number of chunks published. Any
    /// final-part close whose chunks are ALREADY all normalized happens here
    /// and the returned jobs are the LEADER's to seal (via [`ParEngine::seal_jobs`]);
    /// otherwise the worker normalizing the last chunk closes it.
    pub fn close_input(&self, total: u64) -> Vec<u32> {
        // Close the morsel space FIRST (watermark == total at close is the
        // MorselSource contract), so parked claimers observe Drained.
        self.published_units.fetch_max(total, Ordering::AcqRel);
        self.closed_total.store(total, Ordering::Release);
        // FIX-B: wake parked claimers so they observe the drain (after
        // BOTH stores above — a woken claimer must see the closed space).
        self.poke_wake();
        let jobs = {
            let mut c = lock(&self.mu);
            c.total_chunks = Some(total);
            let jobs = self.advance_cut(&mut c);
            self.cv.notify_all();
            jobs
        };
        self.seal_jobs(jobs)
    }

    /// The morsel-scheduled worker loop — COPY as the claim plane's
    /// consumer #1 (PC-2.2 worker-local claiming; PC-2.3 shared-cursor
    /// dispatch): claim a chunk-morsel span, run it, repeat until the
    /// space drains. A claim-starved worker PARKS on the wake face
    /// (FIX-B: block-not-poll — the pre-fix yield-poll shape was the
    /// measured ~5.5M sched_yield/s storm) and re-wakes on publish
    /// progress, input close, or cancel. Ordering lives WHOLLY in the
    /// sink: the cut cursor consumes normalized morsels strictly in input
    /// order, so ANY claim schedule — including pathological worker-speed
    /// skew — yields byte-identical parts (the dirsha determinism law,
    /// witnessed by the skew probe in `tests/par_determinism.rs`).
    pub fn run_worker(&self, worker: usize) {
        loop {
            // Snapshot BEFORE the probe (the no-lost-wake ordering).
            let seen = self.wake_seq();
            match self.step_claim(worker) {
                ClaimStep::Progress => {}
                ClaimStep::Drained => return,
                ClaimStep::Starved => self.park_wake(seen),
            }
        }
    }

    /// One claim probe + run (the composable worker face): the caller
    /// owns the idle policy. The sortload merge loop folds this outcome
    /// into its own park decision (several claim families, one park);
    /// [`ParEngine::run_worker`] parks on the engine wake face;
    /// [`ParEngine::run_one_claim`] keeps the legacy yield-poll shape.
    pub fn step_claim(&self, worker: usize) -> ClaimStep {
        let src = ChunkMorsels(self);
        match self.claim.try_claim(&src, worker, &NoObserver) {
            ClaimOutcome::Claimed(span) => {
                for id in span.start..span.end {
                    self.run_chunk(worker, id);
                }
                ClaimStep::Progress
            }
            ClaimOutcome::Drained => ClaimStep::Drained,
            ClaimOutcome::NotYetPublished => {
                // FIX-B (charter S1): the cancel flag is an atomic — the
                // pre-fix shape locked the GLOBAL Coord mutex on every
                // poll just to read this bool.
                if self.cancelled.load(Ordering::SeqCst) {
                    ClaimStep::Drained
                } else {
                    ClaimStep::Starved
                }
            }
        }
    }

    /// One claim advance in the legacy poll shape — split out so a
    /// driver can pace BETWEEN claims (the worker-speed-skew determinism
    /// probe paces exactly here; the passthrough parquet driver's mixed
    /// loop still polls). Returns `false` when the morsel space is
    /// drained (or the session is cancelled at the watermark).
    pub fn run_one_claim(&self, worker: usize) -> bool {
        match self.step_claim(worker) {
            ClaimStep::Progress => true,
            ClaimStep::Drained => false,
            ClaimStep::Starved => {
                // pgsync::thread is the sanctioned surface (native: the
                // verbatim std re-export) — and the rtpool-only source pin
                // (pgrc2_ingest_par/tests/runtime_binding.rs leg 5) string-
                // scans this file for raw thread primitives.
                pgsync::thread::yield_now();
                true
            }
        }
    }

    /// Worker face: normalize chunk `id`, feed the cut cursor, seal any
    /// parts this task closed. Run-to-completion; never blocks on another
    /// task. Infallible by the `run_morsel` contract — errors are recorded
    /// and cancel the session.
    pub fn run_chunk(&self, worker: usize, id: u64) {
        let chunk = {
            let mut c = lock(&self.mu);
            c.workers_seen.insert(worker);
            let at_bound = c.captured.len() >= self.opts.max_chunks_in_flight;
            let chunk = c.captured.remove(&id);
            // Notify hygiene (FIX-B): the ONLY cv waiter is the leader in
            // `publish_chunk`, blocked while captured OR parts sit at
            // their bounds. This section changes only the captured arm —
            // notify exactly at the from-full transition (removals are
            // unit-stepped under the lock, so the crossing fires once);
            // the parts arm has its own crossing notify in `seal_jobs`,
            // and the error arm notifies inside `record_error`.
            if at_bound && chunk.is_some() {
                self.cv.notify_all();
            }
            if self.cancelled.load(Ordering::SeqCst) {
                return;
            }
            match chunk {
                Some(ch) => ch,
                None => {
                    self.record_error(
                        &mut c,
                        id,
                        WriteError::Contract {
                            detail: "claimed chunk was never captured",
                        },
                    );
                    return;
                }
            }
        };
        let mut ext = self.providers.detoast.make();
        let normalized = chunk.normalize(&self.schema, &mut *ext);
        let jobs = {
            let mut c = lock(&self.mu);
            match normalized {
                Err(e) => {
                    // Error parity: normalize errors carry their INPUT
                    // ordinal — the same row order serial `append_row`
                    // surfaces its first error in.
                    self.record_error(&mut c, id, e);
                    return;
                }
                Ok(cols) => {
                    let rows = cols.first().map(|cb| cb.rows()).unwrap_or(0);
                    c.normalized.insert(id, NormChunk { cols, rows });
                }
            }
            // No cv notify here (FIX-B hygiene): nothing the leader waits
            // on becomes truer in this section — captured is untouched and
            // `advance_cut` can only GROW parts_open (close_part); the
            // error arm notifies inside `record_error`.
            self.advance_cut(&mut c)
        };
        self.seal_jobs(jobs);
    }

    /// Advance the cut cursor over consecutively-normalized chunks (INPUT
    /// ORDER — the deterministic partition law). Must hold the lock. Closed
    /// parts belong to THIS caller.
    fn advance_cut(&self, c: &mut Coord) -> Vec<PartJob> {
        let mut jobs = Vec::new();
        if self.cancelled.load(Ordering::SeqCst) {
            return jobs;
        }
        loop {
            let Some(nc) = c.normalized.get(&c.cut_cursor) else {
                // The final part closes when every published chunk is
                // consumed and rows remain.
                if c.total_chunks == Some(c.cut_cursor) && c.part_rows > 0 {
                    jobs.push(self.close_part(c));
                }
                break;
            };
            c.part_rows += nc.rows;
            // #597: feed the predicate the SERIAL byte operand, exactly.
            // The chunk-local `approx_bytes` sum is blind to the heap pad8
            // the part assembler's splice inserts at each interior chunk
            // seam (`ColBuffer::splice_chunk`) — 0-7 bytes per byref column
            // per seam, enough to move a cut across `max_bytes` at a
            // granule boundary. Replaying the splice's pad arithmetic per
            // column makes the metric below equal
            // `TableWriter::buffered_bytes` at every granule boundary.
            for (i, cb) in nc.cols.iter().enumerate() {
                let heap = cb.heap_len();
                c.part_flat_bytes += cb.approx_bytes() - heap;
                if heap > 0 {
                    let h = &mut c.part_heap_bytes[i];
                    *h = h.next_multiple_of(8) + heap;
                }
            }
            c.cut_cursor += 1;
            let part_bytes = c.part_flat_bytes + c.part_heap_bytes.iter().sum::<u64>();
            // The ONE shared predicate (see `PartCutPolicy::should_cut`),
            // fed the ONE shared metric. `part_rows` is a multiple of
            // chunk_rows == cut_granule_rows at every chunk boundary, so
            // the granule gate always admits here and the decision matches
            // the serial writer's row for row — and, per above, byte for
            // byte.
            if self.policy.should_cut(c.part_rows, part_bytes) {
                jobs.push(self.close_part(c));
            }
        }
        jobs
    }

    fn close_part(&self, c: &mut Coord) -> PartJob {
        self.stamp_once(&self.mark_first_cut_us);
        let seq = self.base_seq + c.assigned_parts;
        c.assigned_parts += 1;
        c.parts_open += 1;
        if c.parts_open > c.peak_parts_open {
            c.peak_parts_open = c.parts_open;
        }
        let first_chunk = c.part_start_chunk;
        let ids: Vec<u64> = (c.part_start_chunk..c.cut_cursor).collect();
        let chunks = ids
            .into_iter()
            .map(|i| c.normalized.remove(&i).expect("cursor passed over it"))
            .collect();
        c.part_start_chunk = c.cut_cursor;
        c.part_rows = 0;
        c.part_flat_bytes = 0;
        for h in &mut c.part_heap_bytes {
            *h = 0;
        }
        PartJob {
            seq,
            first_chunk,
            chunks,
        }
    }

    /// Seal the given closed parts (caller owns them — worker or leader).
    fn seal_jobs(&self, jobs: Vec<PartJob>) -> Vec<u32> {
        let mut seqs = Vec::with_capacity(jobs.len());
        for job in jobs {
            seqs.push(job.seq);
            if self.cancelled.load(Ordering::SeqCst) {
                // Cancelled: drop the job; cleanup unlinks assigned temps by
                // name. parts_open stays high, which is fine — only the
                // leader consults it, and a cancelled leader exits its wait
                // via the error arm.
                continue;
            }
            let first_chunk = job.first_chunk;
            let sealed = self.seal_one(job);
            let mut c = lock(&self.mu);
            match sealed {
                Ok((seq, out)) => {
                    self.stamp_once(&self.mark_first_seal_us);
                    c.sealed.insert(seq, out);
                    c.parts_open -= 1;
                    // Notify hygiene (FIX-B): wake the leader exactly at
                    // the downward crossing of the parts bound. A burst of
                    // closes can overshoot the bound, but decrements are
                    // unit-stepped under the lock, so the crossing fires
                    // exactly once; above the bound the leader's predicate
                    // stays false and a wake would be spurious.
                    if c.parts_open + 1 == self.opts.max_parts_in_flight {
                        self.cv.notify_all();
                    }
                }
                Err(e) => {
                    // Seal errors attribute to the part's first input
                    // chunk (input-order error selection).
                    self.record_error(&mut c, first_chunk, e);
                }
            }
        }
        seqs
    }

    /// Assemble one part from its chunks (seam replay, input order) and run
    /// the FROZEN seal face. Mirrors `TableWriter::cut_part` exactly.
    fn seal_one(&self, mut job: PartJob) -> WriteResult<(u32, (SealedPart, SealReport))> {
        let mut cols: Vec<ColBuffer> = self.schema.iter().map(|s| ColBuffer::new(*s)).collect();
        // Consume-and-drop per chunk: after a chunk is spliced its buffers
        // are dead — freeing them here keeps the open part's footprint at
        // ONE whole-part image through encode instead of two (the FIX-A
        // budget's 2 × max_bytes per-part operand covers the splice-window
        // pair; this keeps the measured footprint under it with margin).
        // Same chunks, same input order — bytes cannot move.
        for chunk in std::mem::take(&mut job.chunks) {
            for (i, cb) in chunk.cols.iter().enumerate() {
                cols[i].splice_chunk(cb)?;
            }
        }
        let mut shred = self.providers.shred.make();
        let mut lanes: Vec<ShredLane> = Vec::new();
        for col in &cols {
            if col.schema.class == StorageClass::VarlenaVerbatim {
                let derived = shred.shred(col, &self.providers.shred_opts)?;
                validate_lanes(col, &derived, &self.providers.shred_opts)?;
                lanes.extend(derived);
            }
        }
        // The SAME factory the serial site uses (`seal_one` mirrors
        // `cut_part` exactly — including, now, the metadata plane, so
        // serial and parallel ingest keep producing byte-identical parts).
        let mut builders: Vec<Box<dyn ColumnMetaBuilder>> =
            crate::meta_wire::builders_for_streams(&cols, &lanes);
        let sources: Vec<&dyn CandidateSource> = self
            .providers
            .sources
            .iter()
            .map(|s| s.as_ref() as &dyn CandidateSource)
            .collect();
        let mut vfs = self.providers.vfs.make();
        let out = seal_part(
            &mut *vfs,
            &self.table_dir,
            &self.spec,
            &cols,
            &lanes,
            &mut builders,
            &sources,
            self.providers.resolver.as_ref(),
            &self.providers.structural,
            self.fxid,
            job.seq,
        )?;
        Ok((job.seq, out))
    }

    /// Leader, AFTER the runtime drain: collect the ordered seal results, or
    /// surface the session error (cleanup is the caller's next move).
    pub fn collect(&self) -> WriteResult<Vec<(SealedPart, SealReport)>> {
        let mut c = lock(&self.mu);
        if let Some((_, e)) = c.error.take() {
            return Err(e);
        }
        if c.sealed.len() as u32 != c.assigned_parts {
            return Err(WriteError::Contract {
                detail: "parallel drain left unsealed parts",
            });
        }
        let sealed = std::mem::take(&mut c.sealed);
        // BTreeMap iteration is seq order — the ordered commit.
        Ok(sealed.into_values().collect())
    }

    /// Leader, error path AFTER the drain: unlink every assigned part temp
    /// by name (ENOENT-tolerant — a cancelled task may never have created
    /// its file).
    pub fn cleanup_temps(&self) -> WriteResult<()> {
        let assigned = lock(&self.mu).assigned_parts;
        let mut vfs = self.providers.vfs.make();
        for i in 0..assigned {
            let name = temp_file_name(self.fxid, self.base_seq + i);
            let path = format!("{}/{}", self.table_dir, name);
            match vfs.unlink_path(&path) {
                Ok(()) => {}
                Err(WriteError::Io { errno, .. }) if errno == libc::ENOENT => {}
                // A crashed universe (killed vfs) cannot unlink; kill -9
                // residue is the recovery scan's to reclaim by name.
                Err(WriteError::Io { op: "killed", .. }) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Binding-crate accessors (the driver builds sessions from these).
    pub fn chunk_rows(&self) -> u32 {
        self.opts.chunk_rows
    }

    pub fn fxid(&self) -> u64 {
        self.fxid
    }

    pub fn ncols(&self) -> usize {
        self.schema.len()
    }

    /// Witness: pool worker ordinals that executed ingest work (the rtpool
    /// engagement witness for the §5 assertion).
    pub fn workers_seen(&self) -> BTreeSet<usize> {
        lock(&self.mu).workers_seen.clone()
    }

    /// Number of parts assigned (test observability).
    pub fn assigned_parts(&self) -> u32 {
        lock(&self.mu).assigned_parts
    }

    /// High-water of concurrently-open (closed-but-unsealed) parts — the
    /// FIX-A seal-concurrency witness (the parts-pace gate cell: observed
    /// peak must be able to exceed the legacy 8-slot cap).
    pub fn peak_parts_open(&self) -> usize {
        lock(&self.mu).peak_parts_open
    }
}

// The rtpool BINDING (task set, feeding session, driver) lives one crate up
// in `pgrc2_ingest_par`: since M3-H (#473) wired `pgrc2_am` → `pgrc2_write`
// into the table-AM cone, a `runtime` dependency here would close a cycle
// (runtime → parallel → … → tableam → pgrc2_am → pgrc2_write). This module
// is the complete coordination + assembly engine INCLUDING the claim path
// (`run_worker` claims chunk-morsels through the shared `pgrc2_claim`
// cursor — the COPY-on-morsels amendment); the binding crate parks/wakes
// pool workers around exactly the faces above (`publish_chunk` /
// `run_worker` / `close_input` / `collect` / `cleanup_temps` / `fail`).
// Error parity at the seam: the engine surfaces the LOWEST-input-ordinal
// recorded error; on a session error the binding driver owes the serial
// replay of the captured input when exact multi-defect parity with serial
// COPY is required (v2-14 #631's replay law) — single-defect inputs are
// exactly parity by the ordinal selection alone (witnessed in
// `tests/par_determinism.rs`).
