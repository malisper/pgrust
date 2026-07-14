//! M2 RUNTIME AGGREGATION SINK — the parallel GROUP BY engagement
//! (docs/design/m2-sinks.md §2 donor A, notes/m2-agg-sink.md).
//!
//! Shape (phase 1): a SERIAL-plan hashed Agg (AGGSPLIT_SIMPLE) over an
//! unprojected cbstore SeqScan, K2 single-int-key compact class, byval
//! whitelist transitions with catalog combine functions, identity emit —
//! executed as one runtime ParallelSink (ACCEPT + COMBINE task sets) at
//! DOP N on the M1 pinned-RG machinery. The plan surface stays the serial
//! plan; engagement is FORCED/explicit:
//!
//!   PGRUST_RUNTIME=1  (pool spawned at postmaster start, M0 kill switch)
//!   SET pgrust.runtime_agg_pool = <dop>   (never consulted by the planner)
//!
//! Execution model:
//!  * LEADER: admission (fail-closed — every refusal is the serial arm,
//!    byte-identically) → parallel context + query-task binding policy →
//!    submit a PINNED RG with the sink's two task sets → launch N helpers →
//!    park (WaitForParallelWorkersToFinish-shaped loop). On completion it
//!    adopts the published per-bucket EmitBufs and becomes a pure emitter.
//!  * HELPERS (bound, at POST_TASK_PARK): build a thread-local executor
//!    over the worker PlannedStmt (root = the Agg subtree), arm the SINK
//!    build (staging + K2 + compact table under the sink cap), then drive
//!    the pinned RG: ACCEPT morsels run the narrow ranged drain below
//!    (survivor collect → compact batch probe → whole-batch fold — the
//!    serial lane's own kernels over the claimed granule range); COMBINE
//!    morsels merge one radix bucket across all sealed Locals and
//!    finalize+project it (paremit) into a self-contained EmitBuf.
//!  * Local discipline (R3/R5): the worker's compact table lives in its
//!    sink Local between morsels (lend/reclaim by move); at the sink cap it
//!    flushes into a radix-partitioned SinkRun; table + run bytes are
//!    budgeted against `work_mem × hash_mem_multiplier` per Local — a
//!    crossing records a BUDGET REFUSAL, aborts the RG, and the leader
//!    falls back to the serial arm (whole-attempt rerun; nothing consumed
//!    twice).
//!
//! WFIN markers (M0 acceptance instrument contract): each helper prints
//! `MORSEL|WFIN|qid=..|pipe=..|worker=..|t_us=..|tasks=..|task_avg_us=..`
//! per pipe (0 = ACCEPT, 1 = COMBINE) at drive exit; `t_us` is the worker's
//! LAST task settle on that pipe (monotonic, process base), `tasks` counts
//! claimed morsels.

use core::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::EStateData;
use ::nodeagg::sink::{
    sink_build_emit_plan, sink_combine_bucket, sink_emit_bucket, sink_null_only_run,
    sink_partition_remainder, sink_remainder_null_block, sink_remainder_spill_bucket,
    sink_resolve_combines, sink_route_records, sink_run_from_spill, sink_run_spill_bucket,
    sink_spill_row_bytes, SinkCombineFn, SinkEmitBuf, SinkEmitPlan, SinkKeySpec, SinkLocalView,
    SinkPart, SinkRun, SinkTableHandle, SINK_NBUCKETS, SINK_NULL_BUCKET,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, ScanFeedShape, ScanK2Scratch};

// ---------------------------------------------------------------------------
// The sink: ParallelSink impl + engagement-shared control state.
// ---------------------------------------------------------------------------

/// Per-worker sink Local: flushed runs + the owned compact table between
/// morsels (+ its SEAL partition) + the M3.5 spill state (created lazily at
/// the first budget crossing when the spill arm is enabled).
#[derive(Default)]
pub(super) struct AggSinkLocal {
    runs: Vec<SinkRun>,
    run_bytes: usize,
    table: Option<SinkTableHandle>,
    part: Option<SinkPart>,
    spill: Option<AggSpillState>,
}

/// A Local's spill face: its single-writer spill file (epochs of
/// bucket-contiguous run records) plus the spilled epochs' NULL-group
/// blocks, which never touch the file (design §3). Plain data between
/// events; rides the Local through SEAL like everything else.
struct AggSpillState {
    file: ::spillset::SpillFile,
    null_blocks: Vec<Vec<u64>>,
}

/// Per-(pipe, worker) WFIN accounting.
#[derive(Default)]
struct WfinPipe {
    tasks: AtomicU64,
    busy_ns: AtomicU64,
    last_settle_us: AtomicU64,
}

impl WfinPipe {
    fn record(&self, dt_ns: u64) {
        self.tasks.fetch_add(1, Ordering::Relaxed);
        self.busy_ns.fetch_add(dt_ns, Ordering::Relaxed);
        self.last_settle_us.store(mono_us(), Ordering::Relaxed);
    }
}

fn mono_us() -> u64 {
    static BASE: OnceLock<std::time::Instant> = OnceLock::new();
    BASE.get_or_init(std::time::Instant::now).elapsed().as_micros() as u64
}

/// Which worker drain feeds the sink build.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SinkDrain {
    /// Unprojected scan, K2 single-int-key batch probe.
    K2,
    /// Projected scan, expr-key feed (Arith/TsTrunc/Reduced kinds) —
    /// `exprkey_sink_batch` per staged batch, fail-closed off-compact.
    ExprKey,
}

struct AggSink {
    drain: SinkDrain,
    /// Reduced-key shape (worker arm re-derives and must match; the emit
    /// plan's Derived columns came from it). None = single-key.
    red: Option<::nodeagg::RedShape>,
    cap: u32,
    /// Per-Local budget: work_mem × hash_mem_multiplier (R3 envelope).
    budget: usize,
    key_words: usize,
    state_bytes: usize,
    width: u8,
    combines: Vec<SinkCombineFn>,
    emit: SinkEmitPlan,
    /// 256 per-bucket outputs; slot b is written only by the combine task
    /// that claimed partition b (single writer by the sink contract).
    out_emit: Vec<UnsafeCell<SinkEmitBuf>>,
    /// finalize's published output (leader consumes after completion).
    published: Mutex<Option<Vec<SinkEmitBuf>>>,
    /// Abort/observability control (shared with the engagement payload).
    rg: OnceLock<runtime::WeakRgHandle>,
    failed: AtomicBool,
    error: Mutex<Option<Box<PgError>>>,
    /// A Local crossed its memory budget: not an error — the leader falls
    /// back to the serial arm (R5 whole-attempt rerun).
    budget_refused: AtomicBool,
    /// M3.5 spill arm: the engagement's spill set (None = spill disabled →
    /// budget crossings refuse exactly as before).
    spill_set: Option<Arc<::spillset::SpillSet>>,
    /// Spill observability (gate-record counters, R4 line).
    spill_epochs: AtomicU64,
    spilled_bytes: AtomicU64,
    /// Combine-split observability (inc-2b): split events, deepest level
    /// reached, and a per-sink uniquifier for split-file names.
    combine_splits: AtomicU64,
    split_depth_max: AtomicU64,
    split_uniq: AtomicU64,
    /// WFIN accounting, indexed by worker slot (pin-board lane).
    wfin: Vec<[WfinPipe; 2]>,
}

// SAFETY: out_emit cells are written only by the exclusive claimer of their
// partition (the runtime's exactly-once combine claim) and read only by
// finalize, which happens-after every combine by last-worker-out.
unsafe impl Sync for AggSink {}

impl AggSink {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn refuse_budget(&self) {
        self.budget_refused.store(true, Ordering::SeqCst);
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

impl runtime::ParallelSink for AggSink {
    type Local = AggSinkLocal;

    fn fork(&self, _worker: usize) -> AggSinkLocal {
        AggSinkLocal::default()
    }

    fn accept_local(&self, local: &mut AggSinkLocal, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let t0 = std::time::Instant::now();
        let r = catch_unwind(AssertUnwindSafe(|| accept_morsel_body(self, local, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(AcceptFail::Budget)) => {
                mark_self_errored();
                self.refuse_budget();
            }
            Ok(Err(AcceptFail::Error(e))) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(PgError::new(ERROR, "runtime agg sink worker panicked").into());
            }
        }
        if let Some(w) = self.wfin.get(worker) {
            w[0].record(t0.elapsed().as_nanos() as u64);
        }
    }

    /// SEAL: partition every Local's remainder table (single-threaded by
    /// the last-worker-out protocol; counting sort, one pass per Local).
    fn seal(&self, locals: &mut [AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        for l in locals.iter_mut() {
            l.part = l.table.as_ref().map(|t| sink_partition_remainder(t.table()));
        }
    }

    fn partitions(&self) -> u64 {
        SINK_NBUCKETS as u64
    }

    fn combine(&self, part: u64, worker: usize, locals: &[AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let t0 = std::time::Instant::now();
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<CombineOutcome> {
            let b = part as usize;
            let state_words = self.state_bytes / 8;
            let row_bytes = sink_spill_row_bytes(self.key_words, state_words);
            // Pre-build size check (M3.5 §3), from the DIRECTORY + in-memory
            // counts only — nothing is read from disk before this decision.
            // Rows over-count duplicates across faces, so the check is
            // conservative in the safe direction.
            let mut rows = 0usize;
            for l in locals {
                if let Some(sp) = &l.spill {
                    rows += sp.file.part_len(b as u32) as usize / row_bytes;
                }
                for r in &l.runs {
                    rows += (r.starts[b + 1] - r.starts[b]) as usize;
                }
                if let (Some(_), Some(p)) = (&l.table, &l.part) {
                    rows += (p.starts[b + 1] - p.starts[b]) as usize;
                }
            }
            if est_table_bytes(self, rows) > self.budget {
                // inc-2b: recursive combine-split by deeper hash bits —
                // stream every face through sub-bucket routing files and
                // combine each sub-partition bounded; depth cap → refusal.
                let Some(set) = &self.spill_set else {
                    return Ok(CombineOutcome::OverBudget);
                };
                let mut out = SinkEmitBuf::default();
                if !split_views_and_emit(self, b, set, locals, &mut out)? {
                    return Ok(CombineOutcome::OverBudget);
                }
                // SAFETY: partition `part` is claimed exactly once (runtime
                // contract); this is its single writer.
                unsafe { *self.out_emit[b].get() = out };
                return Ok(CombineOutcome::Done);
            }
            // In-memory path: rebuild each Local's spilled face for this
            // bucket — open-by-name on THIS thread (the file is frozen:
            // combine deps-follows accept), one synthesized run per Local
            // plus its in-memory NULL blocks in the NULL bucket.
            let mut synth: Vec<Vec<SinkRun>> = Vec::with_capacity(locals.len());
            for l in locals {
                let mut v: Vec<SinkRun> = Vec::new();
                if let Some(sp) = &l.spill {
                    let ctx = ::mcx::MemoryContext::new("m35-agg-spill-read");
                    if let Some(mut r) = sp.file.read_part(ctx.mcx(), b as u32)? {
                        let bytes = r.read_to_end()?;
                        r.close()?;
                        v.push(sink_run_from_spill(b, self.key_words, state_words, &bytes)?);
                    }
                    if b == SINK_NULL_BUCKET {
                        for nb in &sp.null_blocks {
                            v.push(sink_null_only_run(self.key_words, state_words, nb.clone()));
                        }
                    }
                }
                synth.push(v);
            }
            let views: Vec<SinkLocalView<'_>> = locals
                .iter()
                .zip(synth.iter())
                .map(|(l, s)| SinkLocalView {
                    spilled: s,
                    runs: &l.runs,
                    remainder: match (&l.table, &l.part) {
                        (Some(t), Some(p)) => Some((t.table(), p)),
                        _ => None,
                    },
                })
                .collect();
            let merged = sink_combine_bucket(
                b,
                self.key_words,
                self.state_bytes,
                &views,
                &self.combines,
            )?;
            let buf = sink_emit_bucket(&self.emit, &merged);
            // SAFETY: as above — exactly-once claim, single writer.
            unsafe { *self.out_emit[part as usize].get() = buf };
            Ok(CombineOutcome::Done)
        }));
        match r {
            Ok(Ok(CombineOutcome::Done)) => {}
            Ok(Ok(CombineOutcome::OverBudget)) => {
                lane_trace("runtime-agg: combine partition over budget (split depth cap or spill disarmed) — serial rerun");
                self.refuse_budget();
            }
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "runtime agg sink combine panicked").into())
            }
        }
        if let Some(w) = self.wfin.get(worker) {
            w[1].record(t0.elapsed().as_nanos() as u64);
        }
    }

    /// Publish: move the 256 emit buffers out (O(partitions), the §6
    /// contract). Locals drop with the plumbing right after.
    fn finalize(&self, _locals: &[AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let bufs: Vec<SinkEmitBuf> = self
            .out_emit
            .iter()
            .map(|c| {
                // SAFETY: all combine claims settled (last-worker-out);
                // finalize is the single reader.
                unsafe { std::mem::take(&mut *c.get()) }
            })
            .collect();
        *self.published.lock().unwrap_or_else(|p| p.into_inner()) = Some(bufs);
    }
}

enum CombineOutcome {
    Done,
    OverBudget,
}

enum AcceptFail {
    Budget,
    Error(Box<PgError>),
}

impl From<Box<PgError>> for AcceptFail {
    fn from(e: Box<PgError>) -> AcceptFail {
        AcceptFail::Error(e)
    }
}

// ---------------------------------------------------------------------------
// Worker-side executor (helper thread-local) + the narrow ranged drain.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim — runtime_scan precedent).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeAggShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    refused: AtomicUsize,
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (every exit path — refused
    /// bind, errored, drove to completion — bumps exactly once, by drop
    /// guard). Liveness reap input (inc-2c): a pinned RG is invisible to
    /// pool workers, so once `exited >= launched` with the RG incomplete,
    /// nobody will ever step it — the leader must reap or park forever.
    exited: AtomicUsize,
    sink: Arc<AggSink>,
    query_id: AtomicU64,
}

/// Bump-on-drop exit counter: rides `helper_drive`'s frame so EVERY exit
/// path (including a panic unwinding into the hook's catch_unwind) counts
/// exactly once. `pub(super)`: runtime_distinct's helper hook has the
/// identical liveness hole and shares this guard.
pub(super) struct ExitBump<'a>(pub(super) &'a AtomicUsize);

impl Drop for ExitBump<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
    /// Per-worker reusable drain scratch.
    k2s: ScanK2Scratch,
    idxs: Vec<u32>,
    groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    /// ExprKey drain state (SinkDrain::ExprKey only): the worker's own
    /// decide + the spill-replay stage slot.
    xk: Option<Box<super::ExprKeyState>>,
    stage_slot: Option<::executils::ExecSlotId>,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// One accept morsel: position the worker's scan on the claimed granule
/// range, lend the Local's table to the executor, run the narrow drain,
/// reclaim the table.
fn accept_morsel_body(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    worker: usize,
    range: runtime::MorselRange,
) -> Result<(), AcceptFail> {
    WORKER_EXEC.with(|cell| -> Result<(), AcceptFail> {
        let mut b = cell.borrow_mut();
        let Some(ex) = b.as_mut() else {
            return Err(AcceptFail::Error(Box::new(PgError::new(
                ERROR,
                "runtime agg morsel without a bound executor",
            ))));
        };
        let WorkerExec { qd, k2s, idxs, groups, xk, stage_slot, .. } = ex;
        let (k2s, idxs, groups) = (&mut *k2s, &mut *idxs, &mut *groups);
        let (xk, stage_slot) = (&mut *xk, &mut *stage_slot);
        crate::querydesc::with_qd(*qd, |q| {
            let x = q.exec.as_mut().expect("runtime agg worker executor state");
            x.with_mut(|d| -> Result<(), AcceptFail> {
                let estate = &mut d.estate;
                let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                else {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker plan root is not an Agg",
                    ))));
                };
                let aps = &mut **aps;
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker outer node is not a SeqScan",
                    ))));
                };
                if !::nodeseqscan::seq_scan_cb_set_granule_range(
                    ss,
                    estate,
                    range.start,
                    range.end,
                )? {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker scan is not cbstore",
                    ))));
                }
                // Lend the Local's table to the executor for this range
                // (first morsel: the armed table is already in place).
                if let Some(t) = local.table.take() {
                    ::nodeagg::sink::agg_sink_put_table(&mut aps.agg, t);
                }
                let drained = sink_drain_range(
                    sink, local, worker, &mut aps.agg, ss, k2s, idxs, groups, xk, stage_slot,
                    estate,
                );
                // Reclaim on EVERY path — the Local owns the table between
                // morsels and at SEAL.
                if let Some(t) = ::nodeagg::sink::agg_sink_take_table(&mut aps.agg) {
                    local.table = Some(t);
                }
                drained
            })
        })
    })
}

/// M3.5 accept-side spill (design §3): write the Local's accumulated runs
/// to its spill file as ONE epoch — buckets 0..255 contiguous (each run's
/// bucket rows are already counting-sorted), NULL blocks kept in memory —
/// then drop the runs. Runs on the owning worker thread only; the BufFile
/// handle lives inside this event (open-per-event, §2 amendment).
fn spill_epoch(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    set: &Arc<::spillset::SpillSet>,
    worker: usize,
) -> Result<(), Box<PgError>> {
    let sp = local.spill.get_or_insert_with(|| AggSpillState {
        file: ::spillset::SpillFile::new(
            Arc::clone(set),
            ::spillset::SpillSet::file_name("agg", 0, worker),
            SINK_NBUCKETS as u32,
        ),
        null_blocks: Vec::new(),
    });
    let before = sp.file.spilled_bytes();
    let ctx = ::mcx::MemoryContext::new("m35-agg-spill-write");
    let mut w = sp.file.begin_epoch(ctx.mcx())?;
    let mut buf: Vec<u8> = Vec::with_capacity(256 * 1024);
    for b in 0..SINK_NBUCKETS {
        buf.clear();
        for run in &local.runs {
            sink_run_spill_bucket(run, b, &mut buf);
        }
        w.write_part(b as u32, &buf)?;
    }
    w.finish()?;
    for mut run in local.runs.drain(..) {
        if let Some(nb) = run.null_states.take() {
            sp.null_blocks.push(nb);
        }
    }
    local.run_bytes = 0;
    sink.spill_epochs.fetch_add(1, Ordering::Relaxed);
    sink.spilled_bytes
        .fetch_add(sp.file.spilled_bytes() - before, Ordering::Relaxed);
    Ok(())
}

/// Merged-table byte estimate for `rows` input rows (entry overhead + key +
/// state, ×1.5 headroom) — the combine pre-build check and the split loop
/// read the SAME estimator.
fn est_table_bytes(sink: &AggSink, rows: usize) -> usize {
    rows.saturating_mul(sink.key_words * 8 + sink.state_bytes + 32)
        .saturating_mul(3)
        / 2
}

/// Combine-split depth cap: hash bytes below the top-8 the recursion may
/// consume (depth 1 = the first split). Default 3; clamped to the routing
/// vocabulary (≤6).
fn spill_split_depth_cap() -> u32 {
    static N: OnceLock<u32> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_AGG_SPILL_DEPTH")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(3)
            .clamp(1, 6)
    })
}

const SPLIT_FLUSH_BYTES: usize = 16 << 20;
const SPLIT_READ_CHUNK: usize = 1 << 20;

/// Bounded sub-bucket router (inc-2b): records absorb into 256 in-memory
/// buffers and epoch-flush to a combine-task-owned spill file when the
/// staged total crosses [`SPLIT_FLUSH_BYTES`] — partition-ascending per
/// epoch, extents accumulate across epochs (the substrate contract).
struct SubRouter {
    file: ::spillset::SpillFile,
    bufs: Vec<Vec<u8>>,
    staged: usize,
    key_words: usize,
    state_words: usize,
    depth: u32,
}

impl SubRouter {
    fn new(
        sink: &AggSink,
        set: &Arc<::spillset::SpillSet>,
        b: usize,
        depth: u32,
    ) -> SubRouter {
        let uniq = sink.split_uniq.fetch_add(1, Ordering::Relaxed);
        SubRouter {
            file: ::spillset::SpillFile::new(
                Arc::clone(set),
                format!("m35-cmb-p{b}-d{depth}-u{uniq}"),
                SINK_NBUCKETS as u32,
            ),
            bufs: vec![Vec::new(); SINK_NBUCKETS],
            staged: 0,
            key_words: sink.key_words,
            state_words: sink.state_bytes / 8,
            depth,
        }
    }

    fn absorb(&mut self, records: &[u8]) -> PgResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        sink_route_records(records, self.key_words, self.state_words, self.depth, &mut self.bufs)?;
        self.staged += records.len();
        if self.staged >= SPLIT_FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> PgResult<()> {
        if self.staged == 0 {
            return Ok(());
        }
        let ctx = ::mcx::MemoryContext::new("m35-agg-split-write");
        let mut w = self.file.begin_epoch(ctx.mcx())?;
        for (s, buf) in self.bufs.iter_mut().enumerate() {
            if !buf.is_empty() {
                w.write_part(s as u32, buf)?;
                buf.clear();
            }
        }
        w.finish()?;
        self.staged = 0;
        Ok(())
    }
}

/// Stream one spilled partition in ROW-ALIGNED chunks (fixed-width records;
/// a torn tail fails closed). `pub(super)`: the runtime-distinct combine
/// split (inc-3b) streams its fixed-width value records through the same
/// discipline.
pub(super) fn stream_part_rows(
    file: &::spillset::SpillFile,
    part: u32,
    row_bytes: usize,
    mut f: impl FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    let ctx = ::mcx::MemoryContext::new("m35-agg-split-read");
    let Some(mut rd) = file.read_part(ctx.mcx(), part)? else { return Ok(()) };
    let cap = (SPLIT_READ_CHUNK / row_bytes).max(1) * row_bytes;
    let mut buf = vec![0u8; cap];
    let mut filled = 0usize;
    loop {
        let n = rd.read(&mut buf[filled..])?;
        if n == 0 {
            rd.close()?;
            if filled != 0 {
                return Err(::nodeagg::sink::sink_shape_error(
                    "torn spill record (partial row) in split stream",
                ));
            }
            return Ok(());
        }
        filled += n;
        let usable = filled / row_bytes * row_bytes;
        if usable > 0 {
            f(&buf[..usable])?;
            buf.copy_within(usable..filled, 0);
            filled -= usable;
        }
    }
}

/// inc-2b top level: route every face of over-budget partition `b` into a
/// depth-1 sub-bucket file, then combine each sub-partition (recursing where
/// still too big), emitting into `out`. Returns false on depth-cap overflow
/// (the caller refuses → R5 serial rerun). NULL faces never route — they
/// merge through one bounded mini-combine at the end.
fn split_views_and_emit(
    sink: &AggSink,
    b: usize,
    set: &Arc<::spillset::SpillSet>,
    locals: &[AggSinkLocal],
    out: &mut SinkEmitBuf,
) -> PgResult<bool> {
    sink.combine_splits.fetch_add(1, Ordering::Relaxed);
    sink.split_depth_max.fetch_max(1, Ordering::Relaxed);
    let state_words = sink.state_bytes / 8;
    let row_bytes = sink_spill_row_bytes(sink.key_words, state_words);
    let mut router = SubRouter::new(sink, set, b, 1);
    let mut scratch: Vec<u8> = Vec::new();
    let mut null_runs: Vec<SinkRun> = Vec::new();
    for l in locals {
        for r in &l.runs {
            scratch.clear();
            sink_run_spill_bucket(r, b, &mut scratch);
            router.absorb(&scratch)?;
            if b == SINK_NULL_BUCKET {
                if let Some(nb) = &r.null_states {
                    null_runs.push(sink_null_only_run(sink.key_words, state_words, nb.clone()));
                }
            }
        }
        if let (Some(t), Some(p)) = (&l.table, &l.part) {
            scratch.clear();
            sink_remainder_spill_bucket(t.table(), p, b, &mut scratch);
            router.absorb(&scratch)?;
            if b == SINK_NULL_BUCKET {
                if let Some(nb) = sink_remainder_null_block(t.table()) {
                    null_runs.push(sink_null_only_run(sink.key_words, state_words, nb));
                }
            }
        }
        if let Some(sp) = &l.spill {
            stream_part_rows(&sp.file, b as u32, row_bytes, |chunk| router.absorb(chunk))?;
            if b == SINK_NULL_BUCKET {
                for nb in &sp.null_blocks {
                    null_runs.push(sink_null_only_run(sink.key_words, state_words, nb.clone()));
                }
            }
        }
    }
    router.flush()?;
    if !split_subparts_and_emit(sink, b, set, &router.file, 1, out)? {
        return Ok(false);
    }
    if !null_runs.is_empty() {
        // The NULL group: one bounded mini-combine over its blocks only.
        let view = [SinkLocalView { spilled: &null_runs, runs: &[], remainder: None }];
        let t = sink_combine_bucket(b, sink.key_words, sink.state_bytes, &view, &sink.combines)?;
        out.append(sink_emit_bucket(&sink.emit, &t));
    }
    Ok(true)
}

/// Combine each sub-partition of a routed split file; sub-partitions still
/// over budget recurse one hash byte deeper (fresh file), depth-capped.
fn split_subparts_and_emit(
    sink: &AggSink,
    b: usize,
    set: &Arc<::spillset::SpillSet>,
    file: &::spillset::SpillFile,
    depth: u32,
    out: &mut SinkEmitBuf,
) -> PgResult<bool> {
    let state_words = sink.state_bytes / 8;
    let row_bytes = sink_spill_row_bytes(sink.key_words, state_words);
    for s in 0..SINK_NBUCKETS {
        let blen = file.part_len(s as u32) as usize;
        if blen == 0 {
            continue;
        }
        let rows = blen / row_bytes;
        if est_table_bytes(sink, rows) > sink.budget {
            if depth + 1 > spill_split_depth_cap() {
                return Ok(false);
            }
            sink.combine_splits.fetch_add(1, Ordering::Relaxed);
            sink.split_depth_max.fetch_max((depth + 1) as u64, Ordering::Relaxed);
            let mut router = SubRouter::new(sink, set, b, depth + 1);
            stream_part_rows(file, s as u32, row_bytes, |chunk| router.absorb(chunk))?;
            router.flush()?;
            if !split_subparts_and_emit(sink, b, set, &router.file, depth + 1, out)? {
                return Ok(false);
            }
            continue;
        }
        let ctx = ::mcx::MemoryContext::new("m35-agg-split-read");
        let Some(mut rd) = file.read_part(ctx.mcx(), s as u32)? else { continue };
        let bytes = rd.read_to_end()?;
        rd.close()?;
        let synth = sink_run_from_spill(b, sink.key_words, state_words, &bytes)?;
        let view = [SinkLocalView {
            spilled: core::slice::from_ref(&synth),
            runs: &[],
            remainder: None,
        }];
        let t = sink_combine_bucket(b, sink.key_words, sink.state_bytes, &view, &sink.combines)?;
        out.append(sink_emit_bucket(&sink.emit, &t));
    }
    Ok(true)
}

/// The narrow sink drain over the positioned claim: per staged page batch —
/// cap-flush check, survivor collection, canonical key gather, compact
/// batch probe (never the C table), whole-batch fold.
#[allow(clippy::too_many_arguments)]
fn sink_drain_range<'mcx>(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    worker: usize,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    k2s: &mut ScanK2Scratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    xk: &mut Option<Box<super::ExprKeyState>>,
    stage_slot: &mut Option<::executils::ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> Result<(), AcceptFail> {
    let key_col = match sink.drain {
        SinkDrain::ExprKey => 0, // unused; the expr-key feed derives keys
        SinkDrain::K2 => ::nodeagg::agg_hash_staged_probe_col(agg).ok_or_else(|| {
            AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker build lost its staged key column",
            ))
        })? as usize,
    };
    loop {
        // Bounded-Local discipline: flush BEFORE the batch (no group pointer
        // held across this point), budget-check table + runs.
        if let Some(run) = ::nodeagg::sink::agg_sink_flush_if_due(agg, sink.cap) {
            local.run_bytes += run.bytes();
            local.runs.push(run);
            if local.run_bytes + ::nodeagg::sink::agg_sink_table_mem(agg) > sink.budget {
                // M3.5: the crossing SPILLS when the arm is enabled (the
                // accumulated runs go to the Local's file as one epoch);
                // disabled = today's R5 refusal exactly.
                match &sink.spill_set {
                    Some(set) => {
                        spill_epoch(sink, local, set, worker).map_err(AcceptFail::Error)?
                    }
                    None => return Err(AcceptFail::Budget),
                }
            }
        }
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        if sink.drain == SinkDrain::ExprKey {
            // Expr-key feed: keys derived per batch; fail-closed inside the
            // adapter (range guard / per-row route / compact disarm).
            let xk = xk.as_deref_mut().ok_or_else(|| {
                AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "expr-key drain without a worker decide",
                ))
            })?;
            super::exprkey::exprkey_sink_batch(
                agg, ss, xk, stage_slot, idxs, groups, n, estate,
            )?;
            continue;
        }
        // Fail-closed: a fallback row has no staged key — the sink cannot
        // route it (no C-table leg exists here).
        let all_lane = ::nodeseqscan::seq_scan_batch_soa(ss)
            .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
        if !all_lane {
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "fallback rows in a sink accept batch",
            )));
        }
        let ScanK2Scratch { rows, keys, knull, .. } = k2s;
        super::scan_collect_survivors(ss, estate, n, rows)?;
        keys.clear();
        knull.clear();
        {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("sink drain requires the armed SoA");
            let (kv, kn) = (soa.col_values(key_col), soa.col_isnull(key_col));
            for &i in rows.iter() {
                keys.push(kv[i as usize]);
                knull.push(kn[i as usize]);
            }
        }
        if !::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)? {
            // The compact table migrated (backstop) — unexportable. The
            // sink-mode backstop errors before this; belt-and-braces.
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker compact table disarmed mid-build",
            )));
        }
        idxs.clear();
        idxs.extend_from_slice(rows);
        let soa =
            ::nodeseqscan::seq_scan_batch_soa(ss).expect("sink drain requires the armed SoA");
        // SAFETY: every probed row is non-fallback (all-lane batch), so the
        // SoA lanes carry valid deformed values for every plan column; the
        // plan is unguarded (sink admission); each pergroup was installed by
        // the compact probe within this batch (agg_fold_staged contract).
        unsafe { super::agg_fold_staged(agg, soa, idxs, groups)? };
    }
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn runtime_agg_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_agg_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeAggShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.sink.fail(PgError::new(ERROR, "runtime agg helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeAggShared>) {
    let _ = shared;
    // Every launched helper bumps `exited` exactly once, on EVERY exit path
    // (the leader's liveness reap counts these against `launched`).
    let _exit = ExitBump(&payload.exited);
    let Some(target) = payload.pcxt_shared.get() else { return };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let worker = payload.rt.nthreads() + lane.ordinal();
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg, worker)
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                // Budget refusals are NOT query errors (the leader falls
                // back to the serial arm); the Err only routed the binder
                // through its abort-side cleanup.
                if !payload.sink.budget_refused.load(Ordering::SeqCst) {
                    payload.sink.fail(e);
                }
            } else {
                lane_trace(&format!("runtime-agg: helper bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    // WFIN marker emission (M0 instrument contract): one line per pipe this
    // worker touched, at drive exit.
    let qid = payload.query_id.load(Ordering::SeqCst);
    if let Some(w) = payload.sink.wfin.get(worker) {
        for (pipe, p) in w.iter().enumerate() {
            let tasks = p.tasks.load(Ordering::Relaxed);
            if tasks == 0 {
                continue;
            }
            let busy_us = p.busy_ns.load(Ordering::Relaxed) / 1_000;
            eprintln!(
                "MORSEL|WFIN|qid={qid}|pipe={pipe}|worker={worker}|t_us={}|tasks={tasks}|task_avg_us={}",
                p.last_settle_us.load(Ordering::Relaxed),
                busy_us / tasks.max(1),
            );
        }
    }
}

/// M3.5 P1 substrate probe (env-gated, inc-2 opening move): prove on a
/// REAL binder-bound helper thread that the fd substrate supports the spill
/// design — create a FileSet segment, write an epoch, read it back on this
/// thread, verify bytes. Emits one marker line the e2e tranche parses.
fn spill_substrate_probe(payload: &Arc<RuntimeAggShared>, worker: usize) {
    if std::env::var("PGRUST_SPILL_SUBSTRATE_PROBE").as_deref() != Ok("1") {
        return;
    }
    let Some(set) = payload.sink.spill_set.as_ref() else {
        eprintln!("M35|SPILLPROBE|worker={worker}|ok=0|why=no-spill-set");
        return;
    };
    let r = (|| -> PgResult<bool> {
        let ctx = ::mcx::MemoryContext::new("m35-spill-probe");
        let mut f = ::spillset::SpillFile::new(
            Arc::clone(set),
            ::spillset::SpillSet::file_name("probe", 0, worker),
            4,
        );
        let payload_bytes: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let mut w = f.begin_epoch(ctx.mcx())?;
        w.write_part(1, &payload_bytes)?;
        w.finish()?;
        let Some(mut r) = f.read_part(ctx.mcx(), 1)? else { return Ok(false) };
        let got = r.read_to_end()?;
        r.close()?;
        Ok(got == payload_bytes)
    })();
    match r {
        Ok(true) => eprintln!("M35|SPILLPROBE|worker={worker}|ok=1"),
        Ok(false) => eprintln!("M35|SPILLPROBE|worker={worker}|ok=0|why=mismatch"),
        Err(e) => eprintln!("M35|SPILLPROBE|worker={worker}|ok=0|why={}", e.message()),
    }
}

fn drive_bound(
    payload: &Arc<RuntimeAggShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    worker: usize,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    spill_substrate_probe(payload, worker);
    let _outcome = payload.rt.drive_pinned(local, rg);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // A released (not finished) executor may still hold registered
        // snapshots — the binder's NORMAL unbind asserts a cleared xmin, so
        // route through its transaction-ABORT path by returning an error
        // (observed live: snapmgr xmin assertion at worker slot teardown
        // after a budget refusal). The real error (if any) was recorded
        // first (fail() is first-wins); budget refusals record none and
        // helper_drive swallows this marker.
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime agg worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

/// Build + SINK-ARM this helper's executor over the shared worker
/// PlannedStmt. Divergence from the leader's admission is an ERROR (the
/// leader proved the shape; a worker that cannot reproduce it must not
/// silently build something else).
fn build_worker_exec(payload: &Arc<RuntimeAggShared>) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        if let Some(stale) = cell.borrow_mut().take() {
            crate::querydesc::release_query_desc_seam(stale.qd);
        }
        // SAFETY: leader-arena pstmt, alive until DestroyParallelContext
        // joins this helper (SendConst contract).
        let pstmt: &PlannedStmt<'_> = unsafe { &*payload.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &payload.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let armed = (|| -> PgResult<Option<Box<super::ExprKeyState>>> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime agg worker ExecutorStart");
                x.with_mut(|d| -> PgResult<Option<Box<super::ExprKeyState>>> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime agg worker plan root is not an Agg",
                        )));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime agg worker outer node is not a SeqScan",
                        )));
                    };
                    arm_sink_build(&payload.sink, &mut aps.agg, ss, estate)
                })
            })
        })();
        match armed {
            Ok(xk) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    errored: std::cell::Cell::new(false),
                    k2s: ScanK2Scratch::default(),
                    idxs: Vec::new(),
                    groups: Vec::new(),
                    xk,
                    stage_slot: None,
                });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

/// The worker's sink-build arm: the serial lane's own staging + key-shape +
/// compact arm sequence, under the sink cap, with every admission the leader
/// proved re-checked (divergence = error). Returns the ExprKey drain's
/// worker decide (None for the K2 drain).
fn arm_sink_build<'mcx>(
    sink: &AggSink,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Box<super::ExprKeyState>>> {
    let shape_err = ::nodeagg::sink::sink_shape_error;
    let plan_ok = ::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| !p.guarded && p.vguards.is_empty() && p.resid.is_empty());
    if !plan_ok || ::nodeagg::agg_lanefold_has_resid(agg) {
        return Err(shape_err("worker fold plan diverged from the leader's"));
    }
    if sink.drain == SinkDrain::ExprKey {
        // The worker's own decide (same plan tree — same census result).
        let Some(xk) = super::exprkey::decide_exprkey(agg, ss, estate) else {
            return Err(shape_err("worker expr-key decide diverged from the leader's"));
        };
        if xk.sink_refused() {
            return Err(shape_err("worker expr-key decide starts refused"));
        }
        let kind = xk.sink_key_kind();
        ::nodeagg::sink::agg_sink_set_cap(agg, sink.cap);
        match (&sink.red, kind) {
            (None, Some(None)) => {
                if ::nodeagg::agg_hash_compact_try_arm(agg) != ::nodeagg::CompactArm::Armed {
                    return Err(shape_err("worker compact arm refused under the sink cap"));
                }
            }
            (Some(shape), Some(Some(wshape))) => {
                if wshape.width != shape.width || wshape.keys.len() != shape.keys.len() {
                    return Err(shape_err("worker reduced shape diverged from the leader's"));
                }
                if ::nodeagg::agg_hash_compact_try_arm_reduced(agg, wshape)
                    != ::nodeagg::CompactArm::Armed
                {
                    return Err(shape_err("worker reduced arm refused under the sink cap"));
                }
            }
            _ => return Err(shape_err("worker expr-key kind diverged from the leader's")),
        }
        let spec_ok = match ::nodeagg::sink::agg_sink_key_spec(agg) {
            Some(SinkKeySpec::Single { width }) => sink.red.is_none() && width == sink.width,
            Some(SinkKeySpec::Reduced(sh)) => {
                sink.red.as_ref().is_some_and(|r| r.width == sh.width) && sh.width == sink.width
            }
            None => false,
        };
        if !spec_ok {
            return Err(shape_err("worker key spec diverged from the leader's"));
        }
        if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
            return Err(shape_err("worker state layout diverged from the leader's"));
        }
        return Ok(Some(xk));
    }
    super::arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
    if super::scan_k2_shape(agg, ss, estate).is_none() {
        return Err(shape_err("worker K2 shape diverged from the leader's"));
    }
    if ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some() {
        return Err(shape_err("dict-group staging on a sink worker"));
    }
    ::nodeagg::sink::agg_sink_set_cap(agg, sink.cap);
    if ::nodeagg::agg_hash_compact_try_arm(agg) != ::nodeagg::CompactArm::Armed {
        return Err(shape_err("worker compact arm refused under the sink cap"));
    }
    match ::nodeagg::sink::agg_sink_key_spec(agg) {
        Some(SinkKeySpec::Single { width }) if width == sink.width => {}
        _ => return Err(shape_err("worker key spec diverged from the leader's")),
    }
    if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
        return Err(shape_err("worker state layout diverged from the leader's"));
    }
    Ok(None)
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        let Some(ex) = cell.borrow_mut().take() else { return Ok(()) };
        if clean {
            let r = crate::execmain::executor_finish_seam(ex.qd)
                .and_then(|()| crate::execmain::executor_end_seam(ex.qd));
            match r {
                Ok(()) => {
                    crate::querydesc::free_query_desc_seam(ex.qd);
                    Ok(())
                }
                Err(e) => {
                    crate::querydesc::release_query_desc_seam(ex.qd);
                    Err(e)
                }
            }
        } else {
            crate::querydesc::release_query_desc_seam(ex.qd);
            Ok(())
        }
    })
}

fn runtime_agg_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeAggShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_agg_main",
            runtime_agg_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_agg_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_agg_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

/// Sink cap (worker table bound, entries). Default = the exchange cap class
/// (64K); env-tunable for triage.
fn sink_cap() -> u32 {
    static N: OnceLock<u32> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_AGG_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|&c| c >= 1024)
            .unwrap_or(1 << 16)
    })
}

/// M3.5 spill arm kill switch: ON by default when the sink engages
/// (refusal→engagement is the charter); `PGRUST_RUNTIME_AGG_SPILL=0`
/// restores the phase-1 budget refusal exactly.
fn agg_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_AGG_SPILL").as_deref() != Ok("0"))
}

/// Engagement floor (granules) — below it helper launches are pure overhead.
fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_AGG_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// Find the Node of `agg.plan` inside the leader's plan tree (worker pstmts
/// root at the Agg subtree; the Agg need not be the leader plan's root).
fn find_agg_node<'mcx>(
    root: Node<'mcx>,
    target: *const ::types_nodes::plannodes::Agg<'mcx>,
) -> Option<Node<'mcx>> {
    if let Some(a) = root.as_agg() {
        if core::ptr::eq(a, target) {
            return Some(root);
        }
    }
    let plan = root.as_plan()?;
    if let Some(l) = plan.lefttree {
        if let Some(n) = find_agg_node(l, target) {
            return Some(n);
        }
    }
    if let Some(r) = plan.righttree {
        if let Some(n) = find_agg_node(r, target) {
            return Some(n);
        }
    }
    None
}

/// The runtime aggregation-sink arm. `false` = not engaged (caller falls
/// through to the serial build, byte-identically — nothing was consumed).
/// `true` = the published parallel result was adopted; every retrieve path
/// drains it through `agg_hash_retrieve`'s sink branch.
pub(super) fn try_engage_hashagg_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: Option<&super::ExprKeyState>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_agg_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };

    // --- Plan shape gates (fail-closed).
    let refuse = |why: &str| {
        lane_trace(&format!("runtime-agg: refused ({why})"));
    };
    if !::nodeagg::sink::agg_sink_plan_shape_ok(agg) {
        refuse("plan shape");
        return Ok(false);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refuse("instrumented/EPQ");
        return Ok(false);
    }
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(ss) {
        refuse("scan not fusible cbstore");
        return Ok(false);
    }
    // Unprojected K2 class only in phase 1 (exprkey/Reduced/Multi are the
    // next cars); scan projection means the key is computed — refuse.
    let plan_ok = ::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| !p.guarded && p.vguards.is_empty() && p.resid.is_empty());
    if !plan_ok || ::nodeagg::agg_lanefold_has_resid(agg) {
        refuse("fold plan guarded/varlena/residual");
        return Ok(false);
    }
    // Drain mode: projected scans take the expr-key feed (Arith/TsTrunc/
    // Reduced kinds — the lane's decide already ran and is memoized in
    // `xk`); unprojected scans take the K2 single-int-key batch probe.
    let (drain, red, width);
    if ss.ss.ps_ProjInfo.is_some() {
        let Some(xk) = xk else {
            refuse("projected scan without an expr-key decide");
            return Ok(false);
        };
        if xk.sink_refused() {
            refuse("expr-key decide refused");
            return Ok(false);
        }
        let Some(kind) = xk.sink_key_kind() else {
            refuse("expr-key kind (dict/multi cars)");
            return Ok(false);
        };
        drain = SinkDrain::ExprKey;
        match kind {
            None => {
                let Some(w) = ::nodeagg::sink::agg_sink_key_width(agg) else {
                    refuse("key width");
                    return Ok(false);
                };
                red = None;
                width = w;
            }
            Some(shape) => {
                width = shape.width;
                red = Some(shape);
            }
        }
    } else {
        // The staging arm (idempotent — the serial fold feed re-arms the
        // same shape on fallback) + the K2 single-key decide.
        super::arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
        if super::scan_k2_shape(agg, ss, estate).is_none() {
            refuse("K2 shape");
            return Ok(false);
        }
        if ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some() {
            refuse("dict-group staging");
            return Ok(false);
        }
        let Some(w) = ::nodeagg::sink::agg_sink_key_width(agg) else {
            refuse("key width");
            return Ok(false);
        };
        drain = SinkDrain::K2;
        red = None;
        width = w;
    }
    // Combine + identity-emit qualification (fail-closed; catalog access).
    let Some(combines) = sink_resolve_combines(agg)? else {
        refuse("combine whitelist");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    };
    let key_spec = match &red {
        Some(shape) => SinkKeySpec::Reduced(shape.clone()),
        None => SinkKeySpec::Single { width },
    };
    let Some(emit) = sink_build_emit_plan(agg, &key_spec) else {
        refuse("identity emit");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    };
    let Some(state_bytes) = ::nodeagg::sink::agg_sink_state_bytes(agg) else {
        return Ok(false);
    };
    let Some(budget) = ::nodeagg::sink::agg_sink_hash_mem_limit(agg) else {
        return Ok(false);
    };
    // inc-2c: MIRROR the worker's compact-arm spill gate under the sink cap
    // BEFORE engaging (the leg-4d wedge class: at tiny work_mem the cap'd
    // group estimate is spill-eligible, so EVERY worker's `arm_sink_build`
    // must refuse — engaging such a shape is pure waste and, before the
    // liveness reap, wedged the leader forever). The predicate is READ-ONLY
    // (the leader's own build may already be serial-armed) and
    // single-sourced with the worker arm's arithmetic
    // (`single_word_spillrisk` in nodeagg::compact) — both drain modes (K2
    // and expr-key Single/Reduced) arm through that same gate.
    if ::nodeagg::agg_hash_compact_sink_would_refuse(agg, sink_cap()) {
        refuse("compact arm would refuse under the sink cap/budget — serial");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    }
    // --- Session/binder gates (the M1 set, verbatim).
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refuse("in parallel mode");
        return Ok(false);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse("extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(false) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse("exec params");
        return Ok(false);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refuse("binder policy");
        return Ok(false);
    }
    // Worker plan root: the Agg subtree's Node in the leader plan tree.
    let Some(root) = leader_pstmt.planTree else { return Ok(false) };
    let Some(agg_node) = find_agg_node(root, agg.plan) else {
        refuse("agg node not in plan tree");
        return Ok(false);
    };
    // The Agg's scan child must be the SeqScan (no intermediate nodes).
    if agg.plan.plan.lefttree.map(Node::node_tag) != Some(NodeTag::T_SeqScan) {
        refuse("scan child shape");
        return Ok(false);
    }

    // --- Geometry.
    let Some((total_granules, starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(false);
    };
    if total_granules < min_granules().max(2 * dop as u64) {
        refuse("granule floor");
        return Ok(false);
    }

    // --- Engage.
    // M3.5 spill arm: ON by default when the sink engages (this is the
    // refusal→engagement charter); PGRUST_RUNTIME_AGG_SPILL=0 restores the
    // phase-1 refusal exactly. SpillSet creation is leader-side (fd
    // substrate guaranteed); a creation failure fail-closes to refusal.
    let spill_set = if agg_spill_enabled() {
        match ::spillset::SpillSet::create() {
            Ok(s) => Some(s),
            Err(_) => {
                lane_trace("runtime-agg: spill set creation failed — spill disarmed");
                None
            }
        }
    } else {
        None
    };
    let sink = Arc::new(AggSink {
        drain,
        red,
        cap: sink_cap(),
        budget,
        key_words: 1,
        state_bytes,
        width,
        combines,
        emit,
        out_emit: (0..SINK_NBUCKETS).map(|_| UnsafeCell::new(SinkEmitBuf::default())).collect(),
        published: Mutex::new(None),
        rg: OnceLock::new(),
        failed: AtomicBool::new(false),
        error: Mutex::new(None),
        budget_refused: AtomicBool::new(false),
        spill_set,
        spill_epochs: AtomicU64::new(0),
        spilled_bytes: AtomicU64::new(0),
        combine_splits: AtomicU64::new(0),
        split_depth_max: AtomicU64::new(0),
        split_uniq: AtomicU64::new(0),
        wfin: (0..rt.nthreads() + runtime::MAX_EXTERNAL_LANES)
            .map(|_| [WfinPipe::default(), WfinPipe::default()])
            .collect(),
    });
    engage(agg, estate, rt, dop, total_granules, starts, agg_node, sink)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    agg_node: Node<'mcx>,
    sink: Arc<AggSink>,
) -> PgResult<bool> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;
    let payload = Arc::new(RuntimeAggShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (runtime_scan precedent).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        sink: Arc::clone(&sink),
        query_id: AtomicU64::new(0),
    });

    xact::EnterParallelMode();
    let engaged =
        engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload, &sink);
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeAggShared>,
    sink: &Arc<AggSink>,
) -> PgResult<bool> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_agg_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);

        // The sink's two task sets over the cbstore granule geometry.
        let source = Arc::new(CbstoreGranuleSource { starts });
        let runtime::SinkTaskSets { accept, combine, probe: _probe } = runtime::sink_tasksets(
            Arc::clone(sink),
            source,
            rt.nthreads(),
            0,
        );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let qid = NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64;
        payload.query_id.store(qid, Ordering::SeqCst);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: qid,
            tasksets: vec![accept, combine],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        sink.rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("sink rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-agg: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-agg: engaged dop={launched} granules={total_granules}"
        ));

        let mut all_exited_seen = false;
        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                lane_trace(&format!("runtime-agg: all {refused} helpers refused the bind"));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            // LIVENESS REAP (inc-2c, the leg-4d wedge class): a pinned RG is
            // invisible to pool workers (rg.rs — publication never sets the
            // global active bit), so once every launched helper has exited
            // without the RG completing, NOBODY will ever step it and the
            // leader parks forever. Reap: abort + drain the closed
            // generation ourselves; the next try_wait surfaces Aborted and
            // the existing error/budget/fallback handling below decides.
            // Two consecutive sightings before reaping let a mid-settlement
            // completion land first — belt only: a helper's exit bump
            // happens-after its drive's completion.complete(), and abort +
            // drive_pinned on a completed RG are benign no-ops.
            if payload.exited.load(Ordering::SeqCst) >= launched as usize {
                if all_exited_seen && waiter.try_wait().is_none() {
                    lane_trace(
                        "runtime-agg: all helpers exited without completing the RG — reaping",
                    );
                    rg.abort();
                    drain_rg(rt, &rg);
                    continue;
                }
                all_exited_seen = true;
            }
            parallel::wait_parallel_finish_quantum();
        };

        if let Some(e) = sink.take_error() {
            return Err(e);
        }
        if sink.budget_refused.load(Ordering::SeqCst) {
            // R5 degrade: whole-attempt rerun on the serial arm.
            lane_trace("runtime-agg: budget refusal — falling back to the serial arm");
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
            return Ok(EngageOutcome::Fallback);
        }
        if outcome == runtime::RgOutcome::Aborted {
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "runtime agg pipeline aborted")));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(EngageOutcome::Fallback);
        }
        Ok(EngageOutcome::Completed)
    })(&mut submitted);

    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    match outcome {
        EngageOutcome::Fallback => {
            lane_trace("runtime-agg: fallback to serial arm");
            Ok(false)
        }
        EngageOutcome::Completed => {
            let bufs = sink
                .published
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
                .ok_or_else(|| {
                    ::nodeagg::sink::sink_shape_error("completed sink published nothing")
                })?;
            let natts = sink.emit.cols.len();
            let rows = ::nodeagg::sink::sink_emit_rows(&bufs);
            let spill_epochs = sink.spill_epochs.load(Ordering::Relaxed);
            if spill_epochs > 0 {
                // The R4 spill-rate observability line (e2e + gate records).
                lane_trace(&format!(
                    "runtime-agg: SPILLED epochs={spill_epochs} bytes={}",
                    sink.spilled_bytes.load(Ordering::Relaxed)
                ));
            }
            let splits = sink.combine_splits.load(Ordering::Relaxed);
            if splits > 0 {
                lane_trace(&format!(
                    "runtime-agg: COMBINE-SPLIT splits={splits} max_depth={}",
                    sink.split_depth_max.load(Ordering::Relaxed)
                ));
            }
            lane_trace(&format!("runtime-agg: complete, groups={rows}"));
            ::nodeagg::sink::agg_sink_adopt_emit(agg, bufs, natts);
            Ok(true)
        }
    }
}

/// Reap a pinned RG no helper will drive (abort/fallback paths) — cleanup
/// driving, not leader execution (runtime_scan's drain, verbatim).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) {
    rg.abort();
    let lane = loop {
        if let Some(l) = rt.acquire_external_lane() {
            break l;
        }
        std::thread::yield_now();
    };
    let mut local = lane.local();
    let _ = rt.drive_pinned(&mut local, rg);
}

/// Granule-addressed morsel source over one cbstore part's geometry
/// (runtime_scan's source, module-local copy — claims never cross a
/// row-group/dict-epoch edge).
struct CbstoreGranuleSource {
    starts: Vec<u64>,
}

impl runtime::MorselSource for CbstoreGranuleSource {
    fn total_granules(&self) -> u64 {
        self.starts.last().copied().unwrap_or(0)
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        match self.starts.binary_search(&start) {
            Ok(i) => self.starts.get(i + 1).copied().unwrap_or_else(|| self.total_granules()),
            Err(i) => self.starts.get(i).copied().unwrap_or_else(|| self.total_granules()),
        }
    }

    fn startup_c0(&self) -> u64 {
        2
    }
}
