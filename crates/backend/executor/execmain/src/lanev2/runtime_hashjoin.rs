//! M3 RUNTIME HASH JOIN — the shared-build hash join on the morsel runtime
//! (docs/design/m3-joins.md; parent plan parallelism-redesign-2026-07 §5-M3)
//! + the M3.5 SPILL inc-4/5 batch arm (docs/design/m3.5-spill.md §5: batch
//! files on the SpillSet substrate, PLAN-BATCHES with exact sizes, chained
//! per-leaf build/probe task sets, recursive splits with a depth cap).
//!
//! Shape (phase 1): a SERIAL-plan plain Agg over a HashJoin over two
//! lane-fusible cbstore SeqScans. UNBATCHED (budget fits — the dormant
//! default) it is THREE runtime task sets:
//!
//!   [0] BUILD-ACCEPT   inner-scan granules → filter/project → per-worker
//!                      JoinBuildLocal (materialize + count; sink accept)
//!   [1] BUILD-COMBINE  256 partitions, deps=[0] — partitioned single-writer
//!                      table construction; finalize publishes the frozen
//!                      table (the ParallelSink pair via sink_tasksets)
//!   [2] PROBE          outer-scan granules, deps=[1] — per row: hash → tag
//!                      → chain → recheck → joinqual/otherqual → null-fill
//!                      arms → the plain-agg partial absorb (M1's
//!                      runtime_partial tail)
//!
//! BATCHED (M3.5, admission estimates nbatch > 1 and the spill arm is on),
//! the same RG grows the batch axis — a STATIC ladder (the runtime's task
//! sets and deps are fixed at submit; sources whose sizes only PLAN-BATCHES
//! knows are DEFERRED — their granule totals are set before their set can
//! publish, sequenced by the deps DAG):
//!
//!   [0] BUILD-ACCEPT   rows route by batch: batch 0 → the Local (as above);
//!                      batch k>0 → the worker's inner batch file (M3.5
//!                      batch records on SpillSet). Batch-0 budget crossing
//!                      DEMOTES batch 0 to a file batch (§5.2) instead of
//!                      refusing.
//!   [1] BUILD-COMBINE  batch-0 table (skipped when demoted).
//!   [2] PLAN-BATCHES   single task: EXACT per-batch sizes from the spill
//!                      directories + router counters → leaf map; batches
//!                      over the envelope enqueue SPLIT rounds.
//!   [3..3+R)           SPLIT-ROUND r: repartition oversized batches by
//!                      deeper remix bits into child files; still-oversized
//!                      children go to the next round; depth cap → refusal.
//!   [P]  PROBE(0)      outer rows route by the FROZEN leaf map: in-memory
//!                      leaf probes inline; file leaves append (hash, tuple)
//!                      to the worker's outer file. The map never changes
//!                      after this point — outer files are never
//!                      repartitioned (§5.2/§5.3).
//!   [P+1] FILL(0)      right-fill family only.
//!   then per leaf slot i (chained, ONE LIVE TABLE at a time — C parity):
//!      ACCEPT(i)       inner leaf extents → per-worker Locals
//!      COMBINE(i)      leaf table build + publish
//!      PROBE(i)        outer leaf extents → probe + absorb
//!      FILL(i)         right-fill family only; drops the leaf table.
//!
//! Engagement layering (identical to M1/M2): PGRUST_RUNTIME=1 +
//! `SET pgrust.runtime_hashjoin_pool = <dop>` + lane master switch, with
//! `PGRUST_RUNTIME_HASHJOIN=0` as the dedicated arm kill and
//! `PGRUST_RUNTIME_HASHJOIN_SPILL=0` restoring the phase-1 nbatch>1 refusal
//! exactly. The plan surface stays the serial plan; every refusal falls
//! through to the serial arms byte-identically (nothing consumed).
//!
//! Ordering contract (Michael's 2026-07-13 directive): order-insensitive
//! emission is the baseline; the probe feeds an order-insensitive plain-agg
//! partial tail, and the gates use tie-normalized comparison. Batch
//! processing order is a scheduling choice, not a semantic one (§9).
//!
//! Memory (§6/§7): admission decides batching with the C combined rule
//! (`exec_choose_hash_table_size_full(try_combined_hash_mem=true)`); each
//! live table — batch 0 and every file leaf — gets the RAW combined
//! envelope `get_hash_memory_limit() × (dop+1)` (one gang-shared table at
//! a time; exec_choose's rebalance-reduced `space_allowed` sized C's
//! per-worker tables and over-fanned splits — the train-14 inc-4/5 ledger
//! item). PLAN-BATCHES checks EXACT file bytes + tuple counts against an
//! exact-arithmetic capacity model (never estimates — the agg/distinct
//! duplicate-inflation class does not exist here); router buffers are
//! bounded constants per worker. Every spill syscall brackets the SpillIo
//! blocking-section facade (inside spillset).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ::executils::{EStateData, ExecSlotId};
use ::nodeagg::runtime_partial::{
    agg_runtime_combine, agg_runtime_export_partial_into, agg_runtime_partial_admissible,
    exec_agg_runtime_partials, RuntimePartial,
};
use ::nodehashjoin::batch::{
    batch_of, batch_record_push, estimate_batch_table_mem, split_child, BatchRecords, LeafMap,
    LEAF_INMEM,
};
use ::nodehashjoin::shared_build::{
    freeze, BudgetExceeded, CombinePlan, FrozenJoinTable, JoinBudget, JoinBuildLocal, PARTITIONS,
};
use ::nodehashjoin::shared_exec::{
    shared_build_accept, shared_build_hash_tuple, shared_fill_partition, shared_join_admissible,
    shared_probe_outer, shared_probe_outer_hash, shared_probe_outer_hashed,
    shared_saved_outer_slot,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;
use ::types_tuple::MinimalTupleData;

use super::router::{self, ArmClass, ArmCounter};
use super::runtime_agg::ExitBump;
use super::runtime_scan::{exprs_parallel_safe, CbstoreGranuleSource};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible};

// ---------------------------------------------------------------------------
// M3.5 knobs (env-gated, OnceLock like the agg/distinct arms).
// ---------------------------------------------------------------------------

/// Router flush threshold: staged record bytes per worker before an epoch
/// flush (bounded per-participant buffer memory, §7).
const HJ_ROUTER_FLUSH: usize = 8 << 20;
/// Leaf-build chunk growth ceiling: bounds the PLAN-BATCHES capacity
/// model's per-worker last-chunk waste term. SCALED to the envelope
/// (space/8 across the gang, clamped to the Local's 64KB..16MB ladder) —
/// a flat per-worker constant would doom small-budget engagements (the
/// waste term alone would exceed the envelope).
fn leaf_chunk_cap_bytes(space_allowed: usize, dop: u64) -> u64 {
    ((space_allowed as u64) / (8 * dop.max(1))).clamp(64 << 10, 1 << 20)
}
/// Split-node id space (= split-round file partition space).
const MAX_SPLIT_NODES: usize = 1024;

fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// The M3.5 join-batch spill arm: ON by default (the refusal→engagement
/// charter); `PGRUST_RUNTIME_HASHJOIN_SPILL=0` restores the phase-1
/// nbatch>1 refusal exactly.
fn hj_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL").map_or(true, |v| v.trim() != "0")
    })
}

/// Leaf cap = max file batches (level-0 and post-split combined) one
/// engagement may carry; beyond it the join refuses to the serial arm
/// (recorded honest limit: max spilled inner ≈ cap × combined envelope).
fn hj_spill_max_batches() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL_BATCHES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(32)
            .clamp(2, 128)
    })
}

/// Split rounds declared past the admission nbatch (§5.3 depth cap).
fn hj_spill_rounds() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL_DEPTH")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(2)
            .clamp(1, 4)
    })
}

/// TEST-ONLY underestimate forcing: engage batched with exactly this many
/// level-0 batches regardless of the admission estimate (the split-path e2e
/// leg needs a deterministic "estimate lied" shape). Absent in production.
fn hj_spill_force_batches() -> Option<u32> {
    static N: OnceLock<Option<u32>> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL_FORCE_BATCHES")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|&n| n >= 2)
            .map(|n| n.next_power_of_two())
    })
}

// ---------------------------------------------------------------------------
// Shared state: parallel-context private payload + probe task-set work body.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim — the M1 arm's discipline).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

fn lockm<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

// ---------------------------------------------------------------------------
// M3.5 batch spill state.
// ---------------------------------------------------------------------------

/// A single-writer batch-record router over one SpillFile: per-partition
/// staging buffers, epoch flushes (all non-empty partitions ascending — the
/// substrate contract), EXACT per-partition record counters. Plain data
/// between flush events (the SpillFile open-per-event law); the payload's
/// per-worker-slot Mutex is uncontended during writing (one owner) and
/// serializes later frozen reads.
struct BatchRouter {
    file: ::spillset::SpillFile,
    bufs: Vec<Vec<u8>>,
    counts: Vec<u64>,
    staged: usize,
}

impl BatchRouter {
    fn new(set: &Arc<::spillset::SpillSet>, name: String, nparts: u32) -> BatchRouter {
        BatchRouter {
            file: ::spillset::SpillFile::new(Arc::clone(set), name, nparts),
            bufs: vec![Vec::new(); nparts as usize],
            counts: vec![0; nparts as usize],
            staged: 0,
        }
    }

    fn put(&mut self, part: u32, hashvalue: u32, tuple: &[u8]) -> PgResult<()> {
        let before = self.bufs[part as usize].len();
        batch_record_push(&mut self.bufs[part as usize], hashvalue, tuple);
        self.counts[part as usize] += 1;
        self.staged += self.bufs[part as usize].len() - before;
        if self.staged >= HJ_ROUTER_FLUSH {
            self.flush()?;
        }
        Ok(())
    }

    /// Commit all staged bytes as one epoch (no-op when empty). MUST run on
    /// a thread with temp-file access; the caller's task/seal discipline
    /// guarantees no concurrent writer.
    fn flush(&mut self) -> PgResult<()> {
        if self.staged == 0 {
            return Ok(());
        }
        let ctx = ::mcx::MemoryContext::new("m35-hj-batch-write");
        let mut w = self.file.begin_epoch(ctx.mcx())?;
        for (p, buf) in self.bufs.iter_mut().enumerate() {
            if !buf.is_empty() {
                w.write_part(p as u32, buf)?;
                buf.clear();
            }
        }
        w.finish()?;
        self.staged = 0;
        Ok(())
    }
}

/// One inner-side claim: one committed extent of one router file. Extents
/// are record-aligned by construction (epochs write whole records).
#[derive(Clone, Copy)]
struct InnerClaim {
    src: InnerSrc,
    extent: ::spillset::Extent,
}

#[derive(Clone, Copy)]
enum InnerSrc {
    /// BUILD-ACCEPT inner file of worker `slot`, partition = level-0 batch.
    Accept { slot: usize },
    /// SPLIT-ROUND `round` file of worker `slot`, partition = node id.
    Round { round: usize, slot: usize },
}

/// PLAN-BATCHES mutable state (Mutex — mutated by the plan task and split
/// round finalizers only, all single-threaded by the deps DAG).
struct PlanState {
    map: Option<LeafMap>,
    /// Per assigned leaf slot: its inner claim list.
    leaves: Vec<Vec<InnerClaim>>,
    /// Splits pending for the NEXT round.
    pending: Vec<PendingSplit>,
    /// The shared EMPTY leaf (all zero-tuple batches map to one slot: no
    /// build rows means sharing merges nothing, and it keeps empty level-0
    /// batches from eating the leaf cap or — under tight envelopes where
    /// even the empty-table capacity model refuses — split-recursing).
    empty_leaf: Option<u16>,
}

struct PendingSplit {
    consumed: u32,
    jbits: u32,
    child_base: u32,
    claims: Vec<InnerClaim>,
}

/// One split round's frozen claim schedule.
struct RoundPlan {
    claims: Vec<RoundClaim>,
    /// The splits this round executes (finalize walks their children).
    entries: Vec<RoundEntry>,
}

#[derive(Clone, Copy)]
struct RoundClaim {
    claim: InnerClaim,
    consumed: u32,
    jbits: u32,
    child_base: u32,
}

#[derive(Clone, Copy)]
struct RoundEntry {
    consumed: u32,
    jbits: u32,
    child_base: u32,
}

/// The frozen PLAN-BATCHES output: leaf map + per-leaf inner claims.
struct FrozenPlan {
    map: LeafMap,
    leaves: Vec<Vec<InnerClaim>>,
}

/// Outer claim schedule, built at PROBE(0)'s finalize (outer files frozen).
struct OuterPlan {
    /// Per leaf slot: (worker slot, extent).
    leaves: Vec<Vec<(usize, ::spillset::Extent)>>,
}

/// Deferred morsel source: granule total filled in by an earlier task set's
/// finalize, strictly before this source's set can publish (deps DAG). One
/// claim = one extent (bounded by the router flush threshold).
struct DeferredSource {
    total: AtomicU64,
}

impl DeferredSource {
    fn new() -> Arc<DeferredSource> {
        Arc::new(DeferredSource { total: AtomicU64::new(0) })
    }
}

impl runtime::MorselSource for DeferredSource {
    fn total_granules(&self) -> u64 {
        self.total.load(Ordering::SeqCst)
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(self.total_granules())
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

/// Single-granule source (PLAN-BATCHES).
struct OneGranuleSource;

impl runtime::MorselSource for OneGranuleSource {
    fn total_granules(&self) -> u64 {
        1
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

/// The engagement's M3.5 batch-spill state (None = unbatched — the dormant
/// default; nothing below exists and the DAG is the M3 three-set shape).
struct HjSpill {
    set: Arc<::spillset::SpillSet>,
    /// Level-0 batch count (power of two ≥ 2).
    nbatch: u32,
    log2n: u32,
    /// Combined envelope per live table (C parity: every batch's build gets
    /// the whole combined budget).
    space_allowed: usize,
    dop: u64,
    fill_inner: bool,
    /// Envelope-scaled chunk ceiling for leaf builds (bytes).
    chunk_cap_bytes: u64,
    leaf_cap: usize,
    rounds_max: usize,
    batch0_demoted: AtomicBool,
    /// Per worker slot: BUILD-ACCEPT inner router (nparts = nbatch).
    inner: Vec<Mutex<Option<BatchRouter>>>,
    /// Per worker slot: PROBE(0) outer router (nparts = leaf_cap).
    outer: Vec<Mutex<Option<BatchRouter>>>,
    /// [round][worker slot]: split routers (nparts = MAX_SPLIT_NODES).
    rounds: Vec<Vec<Mutex<Option<BatchRouter>>>>,
    plan: Mutex<PlanState>,
    frozen: OnceLock<Arc<FrozenPlan>>,
    round_plans: Vec<OnceLock<Arc<RoundPlan>>>,
    outer_plan: OnceLock<Arc<OuterPlan>>,
    round_sources: Vec<Arc<DeferredSource>>,
    leaf_in_sources: Vec<Arc<DeferredSource>>,
    leaf_out_sources: Vec<Arc<DeferredSource>>,
    /// Observability (the R4 spill channel).
    splits: AtomicU64,
    max_round: AtomicU64,
    leaves_used: AtomicU64,
}

impl HjSpill {
    fn new(
        set: Arc<::spillset::SpillSet>,
        nbatch: u32,
        space_allowed: usize,
        dop: u64,
        fill_inner: bool,
    ) -> HjSpill {
        let leaf_cap = hj_spill_max_batches();
        let rounds_max = hj_spill_rounds();
        let slots = runtime::MAX_EXTERNAL_LANES;
        HjSpill {
            set,
            nbatch,
            log2n: nbatch.trailing_zeros(),
            space_allowed,
            dop,
            fill_inner,
            chunk_cap_bytes: leaf_chunk_cap_bytes(space_allowed, dop),
            leaf_cap,
            rounds_max,
            batch0_demoted: AtomicBool::new(false),
            inner: (0..slots).map(|_| Mutex::new(None)).collect(),
            outer: (0..slots).map(|_| Mutex::new(None)).collect(),
            rounds: (0..rounds_max)
                .map(|_| (0..slots).map(|_| Mutex::new(None)).collect())
                .collect(),
            plan: Mutex::new(PlanState {
                map: None,
                leaves: Vec::new(),
                pending: Vec::new(),
                empty_leaf: None,
            }),
            frozen: OnceLock::new(),
            round_plans: (0..rounds_max).map(|_| OnceLock::new()).collect(),
            outer_plan: OnceLock::new(),
            round_sources: (0..rounds_max).map(|_| DeferredSource::new()).collect(),
            leaf_in_sources: (0..leaf_cap).map(|_| DeferredSource::new()).collect(),
            leaf_out_sources: (0..leaf_cap).map(|_| DeferredSource::new()).collect(),
            splits: AtomicU64::new(0),
            max_round: AtomicU64::new(0),
            leaves_used: AtomicU64::new(0),
        }
    }

    fn est_fits(&self, bytes: u64, tuples: u64) -> bool {
        estimate_batch_table_mem(bytes, tuples, self.dop, self.chunk_cap_bytes)
            <= self.space_allowed as u64
    }

    /// Spilled-byte census for the completion trace (reads directories only).
    fn spilled_census(&self) -> (u64, u64, u64) {
        let sum = |routers: &Vec<Mutex<Option<BatchRouter>>>| -> u64 {
            routers
                .iter()
                .map(|m| lockm(m).as_ref().map_or(0, |r| r.file.spilled_bytes()))
                .sum()
        };
        let inner = sum(&self.inner);
        let outer = sum(&self.outer);
        let split: u64 = self.rounds.iter().map(sum).sum();
        (inner, outer, split)
    }
}

/// Read one inner claim's extent into an 8-aligned buffer (open-by-name on
/// THIS thread; the file is frozen — the reader task set deps-follows the
/// writer set).
fn read_inner_claim(spill: &HjSpill, claim: &InnerClaim) -> PgResult<AlignedBuf> {
    let guard = match claim.src {
        InnerSrc::Accept { slot } => lockm(&spill.inner[slot]),
        InnerSrc::Round { round, slot } => lockm(&spill.rounds[round][slot]),
    };
    let Some(router) = guard.as_ref() else {
        return Err(PgError::new(ERROR, "join batch claim without a router file").into());
    };
    read_extent_aligned(&router.file, claim.extent)
}

/// 8-aligned byte buffer (u64 backing) — the batch-record reader discipline:
/// every record's tuple image lands MAXALIGNed.
struct AlignedBuf {
    words: Vec<u64>,
    len: usize,
}

impl AlignedBuf {
    fn bytes(&self) -> &[u8] {
        // SAFETY: the word backing owns at least `len` initialized bytes.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }
}

fn read_extent_aligned(
    file: &::spillset::SpillFile,
    extent: ::spillset::Extent,
) -> PgResult<AlignedBuf> {
    let ctx = ::mcx::MemoryContext::new("m35-hj-batch-read");
    let mut rd = file.read_extent(ctx.mcx(), extent)?;
    let total = rd.total_len() as usize;
    let mut words = vec![0u64; total.div_ceil(8)];
    {
        // SAFETY: writing into the owned word backing's byte view.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), total)
        };
        let mut filled = 0usize;
        while filled < total {
            let n = rd.read(&mut bytes[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled != total {
            return Err(PgError::new(ERROR, "join batch extent short read").into());
        }
    }
    rd.close()?;
    Ok(AlignedBuf { words, len: total })
}

pub(super) struct RuntimeHjShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    pins_base: usize,
    refused: AtomicUsize,
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (every exit path bumps once,
    /// by drop guard) — the inc-2c liveness-reap input: a pinned RG is
    /// invisible to pool workers, so `exited >= launched` with the RG
    /// incomplete means nobody will ever step it.
    exited: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// §6 envelope crossing: abort → LEADER FALLBACK (serial rerun), not an
    /// error (R5). Set before the abort; checked on the Aborted outcome.
    budget_refused: AtomicBool,
    /// Per-ordinal cumulative probe partials (M1 overwrite discipline).
    partials: Vec<Mutex<Option<RuntimePartial>>>,
    /// The build sink (the ParallelSink of task sets [0]/[1]).
    sink: OnceLock<Arc<JoinBuildSink>>,
    /// M3.5 batch state (None = unbatched engagement — dormant default).
    spill: Option<Arc<HjSpill>>,
    /// Per-leaf frozen tables (batched engagements; the batch-0 table lives
    /// in the sink). Dropped by the last set that reads them (one live
    /// table, C parity).
    leaf_tables: Vec<Mutex<Option<Arc<FrozenJoinTable>>>>,
}

impl RuntimeHjShared {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = lockm(&self.error);
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

    fn refuse_budget_traced(&self, why: &str) {
        lane_trace(&format!("runtime-hashjoin: REFUSED ({why}) — serial rerun"));
        self.refuse_budget();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        lockm(&self.error).take()
    }

    fn table(&self) -> Option<Arc<FrozenJoinTable>> {
        self.sink.get().and_then(|s| lockm(&s.table).clone())
    }

    fn drop_table0(&self) {
        if let Some(s) = self.sink.get() {
            lockm(&s.table).take();
        }
    }

    fn worker_slot(&self, worker: usize) -> usize {
        worker - self.pins_base
    }
}

// ---------------------------------------------------------------------------
// The build sink: ParallelSink over the shared_build core. accept_local
// drives the bound helper's INNER scan over the claimed granule range;
// combine/finalize are pure core calls (no executor).
// ---------------------------------------------------------------------------

pub(super) struct JoinBuildSink {
    budget: Arc<JoinBudget>,
    /// Lazily planned at first combine (the SEAL happens inside the sink
    /// plumbing; the sink sees the sealed Locals only at combine time).
    plan: Mutex<Option<Arc<CombinePlan>>>,
    /// Published at finalize; the probe task set (deps=[combine]) reads it.
    table: Mutex<Option<Arc<FrozenJoinTable>>>,
    shared: Weak<RuntimeHjShared>,
}

impl JoinBuildSink {
    fn fail(&self, e: Box<PgError>) {
        if let Some(s) = self.shared.upgrade() {
            s.fail(e);
        }
    }

    fn failed(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| s.failed.load(Ordering::SeqCst))
    }

    fn demoted(&self) -> bool {
        self.shared
            .upgrade()
            .and_then(|s| s.spill.as_ref().map(|sp| sp.batch0_demoted.load(Ordering::SeqCst)))
            .unwrap_or(false)
    }

    /// The lazily-built combine plan (first combine wins the build; the
    /// mutex is held only for the plan/lookup, never across a partition).
    fn plan_for(&self, locals: &[JoinBuildLocal]) -> Option<Arc<CombinePlan>> {
        let mut g = lockm(&self.plan);
        if let Some(p) = g.as_ref() {
            return Some(Arc::clone(p));
        }
        match CombinePlan::plan(locals, &self.budget) {
            Ok(p) => {
                let p = Arc::new(p);
                *g = Some(Arc::clone(&p));
                Some(p)
            }
            Err(BudgetExceeded) => {
                drop(g);
                // With the spill arm on, the SEAL pre-check demotes before
                // this can trip (same arithmetic); this remains the
                // spill-disarmed refusal (R5, exact phase-1 posture).
                lane_trace("runtime-hashjoin: REFUSED (envelope crossed at seal) — serial rerun");
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget();
                }
                None
            }
        }
    }
}

impl runtime::ParallelSink for JoinBuildSink {
    type Local = JoinBuildLocal;

    fn fork(&self, worker: usize) -> JoinBuildLocal {
        JoinBuildLocal::new(worker, Arc::clone(&self.budget))
    }

    fn accept_local(&self, local: &mut JoinBuildLocal, worker: usize, range: runtime::MorselRange) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let r =
            catch_unwind(AssertUnwindSafe(|| build_morsel_body(&shared, local, worker, range)));
        match r {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                // §6 envelope crossing mid-accept, spill disarmed: refusal,
                // not error (with the arm on the crossing DEMOTES instead).
                lane_trace("runtime-hashjoin: REFUSED (envelope crossed in build) — serial rerun");
                shared.refuse_budget();
            }
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a build morsel")
                        .into(),
                );
            }
        }
    }

    /// SEAL (single-threaded, last-worker-out): the M3.5 batch-0 demote
    /// point — a batch 0 that crossed mid-accept, or whose bucket array
    /// would cross now, is DUMPED to the batch files (each Local's rows to
    /// its own worker's inner file, partition 0) and becomes an ordinary
    /// file batch (§5.2 "batch 0 is dumped"). Cross-Local file appends are
    /// safe here: accept has completed (no concurrent writer) and the
    /// substrate opens by name on this thread.
    fn seal(&self, locals: &mut [JoinBuildLocal]) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let Some(spill) = shared.spill.as_ref() else { return };
        let mut demote = spill.batch0_demoted.load(Ordering::SeqCst);
        if !demote {
            let total: u64 = locals.iter().map(|l| l.tuples()).sum();
            let buckets = 8 * total.max(1).next_power_of_two().clamp(1024, 1 << 31);
            if self.budget.used() as u64 + buckets > spill.space_allowed as u64 {
                spill.batch0_demoted.store(true, Ordering::SeqCst);
                demote = true;
                lane_trace(
                    "runtime-hashjoin: batch 0 crossed the envelope at seal — demoted to a file batch",
                );
            }
        }
        if !demote {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<()> {
            for l in locals.iter_mut() {
                let slot = shared.worker_slot(l.ordinal());
                let mut g = lockm(&spill.inner[slot]);
                let router = g.get_or_insert_with(|| {
                    BatchRouter::new(
                        &spill.set,
                        ::spillset::SpillSet::file_name("hj-in", 0, slot),
                        spill.nbatch,
                    )
                });
                l.drain_records(|h, payload| router.put(0, h, payload))?;
                router.flush()?;
                drop(g);
                l.reset();
            }
            Ok(())
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => self.fail(
                PgError::new(ERROR, "runtime hash-join batch-0 demote dump panicked").into(),
            ),
        }
    }

    fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[JoinBuildLocal]) {
        if self.failed() || self.demoted() {
            return;
        }
        if let Some(plan) = self.plan_for(locals) {
            plan.combine_partition(part, locals);
        }
    }

    fn finalize(&self, locals: &[JoinBuildLocal]) {
        if self.failed() || self.demoted() {
            return;
        }
        // Zero-granule inner side: no combine morsel ran (empty partition
        // space never happens — PARTITIONS is fixed — but a fully-refused
        // plan slot can be absent after refuse_budget).
        let Some(plan) = self.plan_for(locals) else { return };
        *lockm(&self.table) = Some(Arc::new(freeze(plan, locals)));
    }
}

// ---------------------------------------------------------------------------
// Worker-side executor (TLS): the whole serial Agg→HashJoin→scans subtree,
// built once per bound helper; build morsels position the INNER scan, probe
// morsels the OUTER scan.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static HJ_WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
    /// The probe payload for the currently-driving helper (set for the
    /// drive's duration; run_morsel bodies read it for the frozen table).
    static HJ_PAYLOAD: std::cell::RefCell<Option<Arc<RuntimeHjShared>>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    HJ_WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// Split the worker plan tree into (agg, hj_state, outer scan, hash state,
/// inner scan) and run `f`. All field borrows are disjoint.
fn with_join_tree<'a, 'mcx, R>(
    estate: &'a mut EStateData<'mcx>,
    planstate: &'a mut Option<crate::procnode::PlanStateNode<'mcx>>,
    f: impl FnOnce(
        &mut EStateData<'mcx>,
        &mut ::nodeagg::AggStateData<'mcx>,
        &mut ::nodehashjoin::HashJoinState<'mcx>,
        &mut ::nodeseqscan::SeqScanState<'mcx>,
        &mut ::nodehash::HashState<'mcx>,
        &mut ::nodeseqscan::SeqScanState<'mcx>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    let Some(crate::procnode::PlanStateNode::Agg(aps)) = planstate.as_mut() else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker plan is not a plain Agg root",
        )));
    };
    let aps: &mut crate::procnode::AggPlanState<'mcx> = aps;
    let crate::procnode::PlanStateNode::HashJoin(hjn) = &mut aps.outer else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker outer node is not a HashJoin",
        )));
    };
    let hjn: &mut crate::procnode::HashJoinNode<'mcx> = hjn;
    let crate::procnode::PlanStateNode::SeqScan(outer_ss) = &mut *hjn.outer else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker probe child is not a SeqScan",
        )));
    };
    let hash: &mut crate::procnode::HashSubNode<'mcx> = &mut hjn.hash;
    let crate::procnode::PlanStateNode::SeqScan(inner_ss) = &mut *hash.child else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker build child is not a SeqScan",
        )));
    };
    f(estate, &mut aps.agg, &mut hjn.state, outer_ss, &mut hash.state, inner_ss)
}

fn with_worker_exec<R>(
    ctx: &'static str,
    f: impl for<'mcx> FnOnce(
        &mut EStateData<'mcx>,
        &mut Option<crate::procnode::PlanStateNode<'mcx>>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    HJ_WORKER_EXEC.with(|cell| {
        let b = cell.borrow();
        let Some(ex) = b.as_ref() else {
            return Err(Box::new(PgError::new(ERROR, ctx)));
        };
        crate::querydesc::with_qd(ex.qd, |q| {
            let x = q.exec.as_mut().expect("runtime hash-join worker executor state");
            x.with_mut(|d| f(&mut d.estate, &mut d.planstate))
        })
    })
}

/// One BUILD-ACCEPT morsel: position the inner scan on the claimed granule
/// range and materialize every surviving row into the Local — or, batched,
/// route it (batch 0 → Local, others → the worker's inner batch file).
/// Ok(false) = envelope crossed with the spill arm OFF (refusal, not error).
fn build_morsel_body(
    shared: &Arc<RuntimeHjShared>,
    local: &mut JoinBuildLocal,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<bool> {
    let spill = shared.spill.clone();
    let slot = shared.worker_slot(worker);
    with_worker_exec("runtime hash-join build morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, _agg, _hj, _outer_ss, hstate, inner_ss| {
            // train-12 composition: AM-dispatched positioner (heap lane
            // rename); this arm admits only cbstore scans by construction.
            ::nodeseqscan::seq_scan_set_morsel_range(
                inner_ss,
                estate,
                range.start,
                range.end,
            )?;
            local.begin_run(range.start);
            // Batched: the worker's inner router, held across the morsel
            // (uncontended — this slot's single writer is this worker).
            let mut router_guard = spill.as_ref().map(|sp| lockm(&sp.inner[slot]));
            let mut crossed = false;
            loop {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(inner_ss, estate)?;
                if n == 0 {
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(inner_ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    break;
                }
                ::postgres_seams::check_for_interrupts::call()?;
                // Emit-dead word skip over the staged qual bitmap (see the
                // probe morsel): the surviving build stream is identical.
                let skip = {
                    let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                    ::nodeseqscan::seq_scan_batch_skip_sel(inner_ss).map(|s| {
                        w[..s.len()].copy_from_slice(s);
                        w
                    })
                };
                // Walk error carrier: `Some(e)` = a real error (rethrown);
                // `None` = the local build crossed its envelope (the loop's
                // former `break`).
                let walk = ::exectuples::for_each_live(
                    skip.as_ref().map(|w| &w[..]),
                    0,
                    n,
                    |i| -> Result<(), Option<Box<::types_error::PgError>>> {
                        let Some(slot_id) =
                            ::nodeseqscan::seq_scan_batch_emit(inner_ss, estate, i)
                                .map_err(Some)?
                        else {
                            return Ok(());
                        };
                        match (&spill, router_guard.as_mut()) {
                            (Some(sp), Some(guard)) => {
                                shared_build_hash_tuple(hstate, estate, slot_id, |h, bytes| {
                                    let b = batch_of(h, sp.log2n);
                                    if b == 0 && !sp.batch0_demoted.load(Ordering::Relaxed) {
                                        match local.push(h, bytes) {
                                            Ok(()) => return Ok(()),
                                            Err(BudgetExceeded) => {
                                                // §5.2: demote at the crossing
                                                // point; this row and later
                                                // batch-0 rows go to the file.
                                                sp.batch0_demoted.store(true, Ordering::SeqCst);
                                                lane_trace(
                                                    "runtime-hashjoin: batch 0 crossed the envelope — demoted to a file batch",
                                                );
                                            }
                                        }
                                    }
                                    let router = guard.get_or_insert_with(|| {
                                        BatchRouter::new(
                                            &sp.set,
                                            ::spillset::SpillSet::file_name("hj-in", 0, slot),
                                            sp.nbatch,
                                        )
                                    });
                                    router.put(b, h, bytes)
                                })
                                .map_err(Some)?;
                            }
                            _ => {
                                if shared_build_accept(hstate, estate, slot_id, local)
                                    .map_err(Some)?
                                    .is_err()
                                {
                                    return Err(None);
                                }
                            }
                        }
                        Ok(())
                    },
                );
                match walk {
                    Ok(()) => {}
                    Err(Some(e)) => return Err(e),
                    Err(None) => crossed = true,
                }
                if crossed {
                    break;
                }
            }
            // Frozen-before-read: PLAN-BATCHES reads the directory right
            // after this set completes — commit staged bytes per morsel.
            if let Some(mut guard) = router_guard {
                if let Some(router) = guard.as_mut() {
                    router.flush()?;
                }
            }
            local.end_run();
            Ok(!crossed)
        })
    })
}

/// Fetch one row's minimal-tuple bytes (outer side routing).
fn fetch_outer_tuple_bytes<'mcx, R>(
    hj_ecxt: ::executils::EcxtId,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    f: impl FnOnce(&[u8]) -> PgResult<R>,
) -> PgResult<R> {
    let query_mcx = estate.es_query_cxt;
    let (slot, scratch_mcx) = estate.slot_and_per_tuple_mcx(slot_id, hj_ecxt);
    let fetched = ::exectuples::exec_fetch_slot_minimal_tuple(slot, query_mcx, scratch_mcx)?;
    let (ptr, t_len): (*const u8, u32) = match &fetched {
        exectuples::FetchedMinimalTuple::Slot(m, _) => {
            // SAFETY: live stored image; header read.
            (m.as_ptr().cast_const().cast(), unsafe { m.as_ref().t_len })
        }
        exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len()),
    };
    // SAFETY: a minimal tuple image is t_len readable bytes.
    let bytes = unsafe { core::slice::from_raw_parts(ptr, t_len as usize) };
    f(bytes)
}

/// One PROBE(0) morsel: position the outer scan and stream every surviving
/// outer row — unbatched, straight against the frozen table; batched, route
/// by the FROZEN leaf map (in-memory leaf probes inline; file leaves append
/// to the worker's outer batch file). Exports the cumulative partial (M1
/// overwrite discipline — the worker's last export precedes its settle).
fn probe_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let spill = payload.spill.clone();
    let frozen = match &spill {
        Some(sp) => Some(Arc::clone(sp.frozen.get().ok_or_else(|| {
            Box::new(PgError::new(ERROR, "runtime hash-join probe without a frozen batch plan"))
        })?)),
        None => None,
    };
    let table = payload.table();
    if table.is_none() {
        // Legal only when batch 0 was demoted (no in-memory leaf).
        let demoted = spill
            .as_ref()
            .is_some_and(|sp| sp.batch0_demoted.load(Ordering::SeqCst));
        if !demoted {
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime hash-join probe ran without a published table",
            )));
        }
    }
    let slot = payload.worker_slot(worker);
    with_worker_exec("runtime hash-join probe morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, outer_ss, hstate, _inner_ss| {
            // train-12 composition: AM-dispatched positioner (heap lane
            // rename); this arm admits only cbstore scans by construction.
            ::nodeseqscan::seq_scan_set_morsel_range(
                outer_ss,
                estate,
                range.start,
                range.end,
            )?;
            let mut router_guard = spill.as_ref().map(|sp| lockm(&sp.outer[slot]));
            loop {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(outer_ss, estate)?;
                if n == 0 {
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(outer_ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    break;
                }
                ::postgres_seams::check_for_interrupts::call()?;
                // Emit-dead word skip over the staged qual bitmap: a cleared
                // skip-sel bit is a row `seq_scan_batch_emit` rejects with no
                // observable effect (definitive even under requal), so the
                // surviving probe stream is identical. Snapshot the words —
                // the emit below re-borrows the scan mutably.
                let skip = {
                    let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                    ::nodeseqscan::seq_scan_batch_skip_sel(outer_ss).map(|s| {
                        w[..s.len()].copy_from_slice(s);
                        w
                    })
                };
                let skip = skip.as_ref().map(|w| &w[..]);
                ::exectuples::for_each_live(skip, 0, n, |i| -> PgResult<()> {
                    let Some(slot_id) = ::nodeseqscan::seq_scan_batch_emit(outer_ss, estate, i)?
                    else {
                        return Ok(());
                    };
                    match (&spill, &frozen, router_guard.as_mut()) {
                        (Some(sp), Some(fp), Some(guard)) => {
                            let h = shared_probe_outer_hash(hj, estate, slot_id)?;
                            let leaf = fp.map.resolve(h);
                            if leaf == LEAF_INMEM {
                                shared_probe_outer_hashed(
                                    hj,
                                    hstate,
                                    estate,
                                    table.as_ref().expect("in-memory leaf requires table 0"),
                                    slot_id,
                                    h,
                                    &mut |_hj, estate, out| {
                                        ::nodeagg::agg_plain_build_accept(agg, estate, out)
                                    },
                                )?;
                            } else {
                                let ecxt = hj.ps_ExprContext;
                                fetch_outer_tuple_bytes(ecxt, estate, slot_id, |bytes| {
                                    let router = guard.get_or_insert_with(|| {
                                        BatchRouter::new(
                                            &sp.set,
                                            ::spillset::SpillSet::file_name("hj-out", 0, slot),
                                            sp.leaf_cap as u32,
                                        )
                                    });
                                    router.put(leaf as u32, h, bytes)
                                })?;
                            }
                        }
                        _ => {
                            shared_probe_outer(
                                hj,
                                hstate,
                                estate,
                                table.as_ref().expect("unbatched probe requires the table"),
                                slot_id,
                                &mut |_hj, estate, out| {
                                    ::nodeagg::agg_plain_build_accept(agg, estate, out)
                                },
                            )?;
                        }
                    }
                    Ok(())
                })?;
            }
            // Frozen-before-read for the leaf probe sets.
            if let Some(mut guard) = router_guard {
                if let Some(router) = guard.as_mut() {
                    router.flush()?;
                }
            }
            let pslot = worker - payload.pins_base;
            {
                // train-12 composition: export-into (retained capacity);
                // overwrite discipline preserved — the export rewrites the
                // slot's partial in place.
                let mut g = lockm(&payload.partials[pslot]);
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for RuntimeHjShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let payload = HJ_PAYLOAD.with(|c| c.borrow().clone());
        let Some(payload) = payload else {
            self.fail(PgError::new(ERROR, "runtime hash-join probe without a bound payload").into());
            return;
        };
        let r = catch_unwind(AssertUnwindSafe(|| probe_morsel_body(&payload, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a probe morsel")
                        .into(),
                );
            }
        }
    }

    /// PROBE(0) epilogue (last-worker-out, single-threaded): batched, the
    /// outer files are now frozen — build the outer claim schedule and, when
    /// no fill set follows, retire table 0 (one live table).
    fn finalize(&self) {
        let Some(spill) = self.spill.as_ref() else { return };
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| {
            let frozen = spill.frozen.get().expect("probe(0) ran after the freeze");
            let nleaves = frozen.leaves.len();
            let mut leaves: Vec<Vec<(usize, ::spillset::Extent)>> = vec![Vec::new(); nleaves];
            for slot in 0..spill.outer.len() {
                let g = lockm(&spill.outer[slot]);
                if let Some(router) = g.as_ref() {
                    for (leaf, out) in leaves.iter_mut().enumerate() {
                        for x in router.file.part_extents(leaf as u32) {
                            out.push((slot, x));
                        }
                    }
                }
            }
            for (leaf, claims) in leaves.iter().enumerate() {
                spill.leaf_out_sources[leaf]
                    .total
                    .store(claims.len() as u64, Ordering::SeqCst);
            }
            let _ = spill.outer_plan.set(Arc::new(OuterPlan { leaves }));
            if !spill.fill_inner {
                self.drop_table0();
            }
        }));
        if r.is_err() {
            self.fail(PgError::new(ERROR, "runtime hash-join outer plan build panicked").into());
        }
    }
}

/// The FILL task set's work (right-fill family only, deps=[probe]):
/// never-matched build tuples of one partition, null-extended, into the
/// same plain-agg tail. The probe set's last-worker-out completion is the
/// match-flag visibility barrier. `leaf` = None → the batch-0 table;
/// Some(i) → leaf table i (M3.5). Batched fills retire their table at
/// finalize (one live table).
struct FillWork {
    payload: Arc<RuntimeHjShared>,
    leaf: Option<usize>,
}

impl FillWork {
    fn table(&self) -> Option<Arc<FrozenJoinTable>> {
        match self.leaf {
            None => self.payload.table(),
            Some(i) => lockm(&self.payload.leaf_tables[i]).clone(),
        }
    }
}

fn fill_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    table: &Arc<FrozenJoinTable>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    with_worker_exec("runtime hash-join fill morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, _outer_ss, hstate, _inner_ss| {
            for part in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                shared_fill_partition(
                    hj,
                    hstate,
                    estate,
                    table,
                    part,
                    &mut |_hj, estate, out| ::nodeagg::agg_plain_build_accept(agg, estate, out),
                )?;
            }
            // Cumulative partial export (same slot as the probe morsels —
            // the worker's agg accumulates across phases; overwrite
            // discipline keeps the last export authoritative).
            let slot = worker - payload.pins_base;
            {
                let mut g = lockm(&payload.partials[slot]);
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for FillWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(table) = self.table() else {
            // The ONLY legal absence: a demoted batch 0 (its rows moved to
            // a leaf; nothing to fill). Everywhere else — unbatched fills
            // and leaf fills (published by the leaf combine, deps-ordered)
            // — a missing table is a shape error.
            let demoted_b0 = self.leaf.is_none()
                && self
                    .payload
                    .spill
                    .as_ref()
                    .is_some_and(|sp| sp.batch0_demoted.load(Ordering::SeqCst));
            let unused_leaf = self.leaf.is_some_and(|leaf| {
                self.payload
                    .spill
                    .as_ref()
                    .and_then(|sp| sp.frozen.get())
                    .is_none_or(|fp| leaf >= fp.leaves.len())
            });
            if !demoted_b0 && !unused_leaf {
                self.payload.fail(
                    PgError::new(ERROR, "runtime hash-join fill ran without a published table")
                        .into(),
                );
            }
            return;
        };
        let payload = &self.payload;
        let r =
            catch_unwind(AssertUnwindSafe(|| fill_morsel_body(payload, &table, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                payload.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                payload.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a fill morsel")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self) {
        // One live table (batched only): the fill is the table's last
        // reader.
        if self.payload.spill.is_some() {
            match self.leaf {
                None => self.payload.drop_table0(),
                Some(i) => {
                    lockm(&self.payload.leaf_tables[i]).take();
                }
            }
        }
    }
}

/// The fill set's morsel space: one claim per partition (the sink
/// plumbing's PartitionSource shape, re-stated here — it is private to
/// runtime::sink).
struct FillPartitionSource;

impl runtime::MorselSource for FillPartitionSource {
    fn total_granules(&self) -> u64 {
        PARTITIONS as u64
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(PARTITIONS as u64)
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

// ---------------------------------------------------------------------------
// M3.5 PLAN-BATCHES + split rounds + leaf task-set works.
// ---------------------------------------------------------------------------

/// Resolve one batch node: exact sizes → leaf assignment, or a pending
/// split for `next_round`. Caller holds the plan lock.
#[allow(clippy::too_many_arguments)]
fn resolve_node(
    payload: &RuntimeHjShared,
    spill: &HjSpill,
    st: &mut PlanState,
    node: u32,
    consumed: u32,
    level0: bool,
    claims: Vec<InnerClaim>,
    bytes: u64,
    tuples: u64,
    next_round: usize,
) -> bool {
    let map = st.map.as_mut().expect("plan map exists");
    if tuples == 0 && bytes == 0 {
        // Zero build rows: route to the shared empty leaf (probes against
        // an empty table; LEFT/outer null-fill arms behave exactly as an
        // empty batch must).
        let leaf = match st.empty_leaf {
            Some(l) => l,
            None => {
                let leaf = st.leaves.len();
                if leaf >= spill.leaf_cap {
                    payload.refuse_budget_traced("spill leaf cap exceeded");
                    return false;
                }
                st.leaves.push(Vec::new());
                st.empty_leaf = Some(leaf as u16);
                leaf as u16
            }
        };
        map.set_leaf(node, leaf);
        return true;
    }
    if spill.est_fits(bytes, tuples) {
        let leaf = st.leaves.len();
        if leaf >= spill.leaf_cap {
            payload.refuse_budget_traced("spill leaf cap exceeded");
            return false;
        }
        map.set_leaf(node, leaf as u16);
        st.leaves.push(claims);
        return true;
    }
    if next_round >= spill.rounds_max {
        payload.refuse_budget_traced("split depth cap — batch does not shrink");
        return false;
    }
    let est = estimate_batch_table_mem(bytes, tuples, spill.dop, spill.chunk_cap_bytes);
    let ratio = est.div_ceil(spill.space_allowed.max(1) as u64).max(2);
    // Children sized to fit with one-bit headroom; bounded fan (≤16-way).
    let jbits = (64 - ratio.leading_zeros()).clamp(1, 4);
    if map.node_count() + (1usize << jbits) > MAX_SPLIT_NODES {
        payload.refuse_budget_traced("split node space exhausted");
        return false;
    }
    let child_base = if level0 {
        map.split_node(node, jbits)
    } else {
        map.split_child_node(node, consumed, jbits)
    };
    spill.splits.fetch_add(1, Ordering::Relaxed);
    spill
        .max_round
        .fetch_max(next_round as u64 + 1, Ordering::Relaxed);
    st.pending.push(PendingSplit { consumed, jbits, child_base, claims });
    true
}

/// Arm split round `round` from the pending list (claims flattened), or —
/// when nothing is pending — FREEZE the plan (leaf sources armed).
fn arm_round_or_freeze(payload: &RuntimeHjShared, spill: &HjSpill, round: usize) {
    let mut st = lockm(&spill.plan);
    if st.pending.is_empty() {
        let map = st.map.take().expect("plan map exists");
        debug_assert!(map.fully_resolved(), "freeze with pending nodes");
        let leaves = std::mem::take(&mut st.leaves);
        for (i, claims) in leaves.iter().enumerate() {
            spill.leaf_in_sources[i]
                .total
                .store(claims.len() as u64, Ordering::SeqCst);
        }
        spill.leaves_used.store(leaves.len() as u64, Ordering::SeqCst);
        let _ = spill.frozen.set(Arc::new(FrozenPlan { map, leaves }));
        return;
    }
    if round >= spill.rounds_max {
        // Unreachable: resolve_node refuses before pending past the cap.
        payload.refuse_budget_traced("split depth cap — serial rerun");
        return;
    }
    let pending = std::mem::take(&mut st.pending);
    drop(st);
    let mut claims = Vec::new();
    let mut entries = Vec::new();
    for p in &pending {
        for &c in &p.claims {
            claims.push(RoundClaim {
                claim: c,
                consumed: p.consumed,
                jbits: p.jbits,
                child_base: p.child_base,
            });
        }
        entries.push(RoundEntry {
            consumed: p.consumed,
            jbits: p.jbits,
            child_base: p.child_base,
        });
    }
    spill.round_sources[round]
        .total
        .store(claims.len() as u64, Ordering::SeqCst);
    let _ = spill.round_plans[round].set(Arc::new(RoundPlan { claims, entries }));
}

/// Gather one level-0 batch's exact claims/bytes/tuples from the inner
/// accept files.
fn level0_batch_claims(spill: &HjSpill, part: u32) -> (Vec<InnerClaim>, u64, u64) {
    let mut claims = Vec::new();
    let (mut bytes, mut tuples) = (0u64, 0u64);
    for slot in 0..spill.inner.len() {
        let g = lockm(&spill.inner[slot]);
        if let Some(router) = g.as_ref() {
            for x in router.file.part_extents(part) {
                claims.push(InnerClaim { src: InnerSrc::Accept { slot }, extent: x });
            }
            bytes += router.file.part_len(part);
            tuples += router.counts[part as usize];
        }
    }
    (claims, bytes, tuples)
}

/// PLAN-BATCHES (§5.2 step [1]): one bookkeeping task over the EXACT
/// directory sizes; assigns leaves, enqueues round-1 splits, freezes when no
/// splits are needed. Runs strictly after BUILD-COMBINE (deps), so the
/// batch-0 demote decision is final and every inner file is frozen.
struct PlanBatchesWork(Arc<RuntimeHjShared>);

impl runtime::TaskSetWork for PlanBatchesWork {
    fn run_morsel(&self, _worker: usize, _range: runtime::MorselRange) {
        let payload = &self.0;
        if payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(spill) = payload.spill.as_ref() else { return };
        let r = catch_unwind(AssertUnwindSafe(|| {
            let demoted = spill.batch0_demoted.load(Ordering::SeqCst);
            {
                let mut st = lockm(&spill.plan);
                st.map = Some(LeafMap::new(spill.nbatch));
                for b in 0..spill.nbatch {
                    if b == 0 && !demoted {
                        st.map.as_mut().expect("just set").set_leaf(0, LEAF_INMEM);
                        continue;
                    }
                    let (claims, bytes, tuples) = level0_batch_claims(spill, b);
                    if !resolve_node(
                        payload, spill, &mut st, b, spill.log2n, true, claims, bytes, tuples, 0,
                    ) {
                        return;
                    }
                }
            }
            arm_round_or_freeze(payload, spill, 0);
        }));
        if r.is_err() {
            payload.fail(PgError::new(ERROR, "runtime hash-join PLAN-BATCHES panicked").into());
        }
    }

    fn finalize(&self) {}
}

/// SPLIT-ROUND r (§5.3): claims = parent extents; each record routes by the
/// next remix bits into the worker's round file (child node partition).
/// finalize (single-threaded) sizes the children EXACTLY and assigns leaves
/// or the next round.
struct SplitRoundWork {
    payload: Arc<RuntimeHjShared>,
    round: usize,
}

impl runtime::TaskSetWork for SplitRoundWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        let payload = &self.payload;
        if payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(spill) = payload.spill.as_ref() else { return };
        let Some(plan) = spill.round_plans[self.round].get() else {
            payload.fail(PgError::new(ERROR, "split round ran without a round plan").into());
            return;
        };
        let slot = payload.worker_slot(worker);
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<()> {
            let mut guard = lockm(&spill.rounds[self.round][slot]);
            for ordinal in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                let rc = &plan.claims[ordinal as usize];
                let buf = read_inner_claim(spill, &rc.claim)?;
                let mut it = BatchRecords::new(buf.bytes());
                while let Some((h, tuple)) = it.next_rec()? {
                    let child = rc.child_base + split_child(h, rc.consumed, rc.jbits);
                    let router = guard.get_or_insert_with(|| {
                        BatchRouter::new(
                            &spill.set,
                            ::spillset::SpillSet::file_name("hj-split", self.round as u64, slot),
                            MAX_SPLIT_NODES as u32,
                        )
                    });
                    router.put(child, h, tuple)?;
                }
            }
            if let Some(router) = guard.as_mut() {
                router.flush()?;
            }
            Ok(())
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => payload.fail(e),
            Err(_panic) => payload
                .fail(PgError::new(ERROR, "runtime hash-join split round panicked").into()),
        }
    }

    fn finalize(&self) {
        let payload = &self.payload;
        if payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(spill) = payload.spill.as_ref() else { return };
        let Some(plan) = spill.round_plans[self.round].get() else {
            // Unarmed round (no splits reached this depth): nothing to do;
            // the freeze already happened upstream.
            return;
        };
        let r = catch_unwind(AssertUnwindSafe(|| {
            {
                let mut st = lockm(&spill.plan);
                for e in &plan.entries {
                    for c in 0..(1u32 << e.jbits) {
                        let node = e.child_base + c;
                        let mut claims = Vec::new();
                        let (mut bytes, mut tuples) = (0u64, 0u64);
                        for slot in 0..spill.rounds[self.round].len() {
                            let g = lockm(&spill.rounds[self.round][slot]);
                            if let Some(router) = g.as_ref() {
                                for x in router.file.part_extents(node) {
                                    claims.push(InnerClaim {
                                        src: InnerSrc::Round { round: self.round, slot },
                                        extent: x,
                                    });
                                }
                                bytes += router.file.part_len(node);
                                tuples += router.counts[node as usize];
                            }
                        }
                        if !resolve_node(
                            payload,
                            spill,
                            &mut st,
                            node,
                            e.consumed + e.jbits,
                            false,
                            claims,
                            bytes,
                            tuples,
                            self.round + 1,
                        ) {
                            return;
                        }
                    }
                }
            }
            arm_round_or_freeze(payload, spill, self.round + 1);
        }));
        if r.is_err() {
            payload
                .fail(PgError::new(ERROR, "runtime hash-join split finalize panicked").into());
        }
    }
}

/// Per-leaf build sink: accept claims = frozen inner extents; combine =
/// the shared_build partitioned table; finalize publishes into the leaf
/// table slot. Fresh combined budget per leaf (C parity: one live table).
struct LeafBatchSink {
    shared: Weak<RuntimeHjShared>,
    leaf: usize,
    budget: Arc<JoinBudget>,
    plan: Mutex<Option<Arc<CombinePlan>>>,
}

impl LeafBatchSink {
    fn failed(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| s.failed.load(Ordering::SeqCst))
    }

    /// Leaf slots beyond the frozen plan's count are declared-but-unused
    /// ladder capacity: skip their combine plan/freeze entirely (their
    /// accept/probe sources are zero-granule already). Reads the frozen
    /// count, which is set strictly before any leaf set publishes.
    fn unused(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| {
            s.spill
                .as_ref()
                .and_then(|sp| sp.frozen.get())
                .is_none_or(|fp| self.leaf >= fp.leaves.len())
        })
    }

    fn plan_for(&self, locals: &[JoinBuildLocal]) -> Option<Arc<CombinePlan>> {
        let mut g = lockm(&self.plan);
        if let Some(p) = g.as_ref() {
            return Some(Arc::clone(p));
        }
        match CombinePlan::plan(locals, &self.budget) {
            Ok(p) => {
                let p = Arc::new(p);
                *g = Some(Arc::clone(&p));
                Some(p)
            }
            Err(BudgetExceeded) => {
                drop(g);
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget_traced("leaf build crossed the envelope at seal");
                }
                None
            }
        }
    }
}

impl runtime::ParallelSink for LeafBatchSink {
    type Local = JoinBuildLocal;

    fn fork(&self, worker: usize) -> JoinBuildLocal {
        let Some(shared) = self.shared.upgrade() else {
            return JoinBuildLocal::new(worker, Arc::clone(&self.budget));
        };
        let cap_words = shared
            .spill
            .as_ref()
            .map_or(1 << 17, |sp| (sp.chunk_cap_bytes / 8) as usize);
        JoinBuildLocal::with_chunk_cap(worker, Arc::clone(&self.budget), cap_words)
    }

    fn accept_local(&self, local: &mut JoinBuildLocal, _worker: usize, range: runtime::MorselRange) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let Some(spill) = shared.spill.as_ref() else { return };
        let Some(frozen) = spill.frozen.get() else {
            shared.fail(PgError::new(ERROR, "leaf accept without a frozen batch plan").into());
            return;
        };
        let claims = &frozen.leaves[self.leaf];
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<bool> {
            for ordinal in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                let buf = read_inner_claim(spill, &claims[ordinal as usize])?;
                local.begin_run(ordinal);
                let mut it = BatchRecords::new(buf.bytes());
                while let Some((h, tuple)) = it.next_rec()? {
                    if local.push(h, tuple).is_err() {
                        local.end_run();
                        return Ok(false);
                    }
                }
                local.end_run();
            }
            Ok(true)
        }));
        match r {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                // Exact-size model missed (fail-closed): refuse to serial —
                // the leader-side twin is PLAN-BATCHES' est_fits, the same
                // arithmetic source.
                shared.refuse_budget_traced("leaf build crossed the envelope in accept");
            }
            Ok(Err(e)) => {
                mark_self_errored();
                shared.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                shared.fail(
                    PgError::new(ERROR, "runtime hash-join leaf build panicked").into(),
                );
            }
        }
    }

    fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[JoinBuildLocal]) {
        if self.failed() || self.unused() {
            return;
        }
        if let Some(plan) = self.plan_for(locals) {
            plan.combine_partition(part, locals);
        }
    }

    fn finalize(&self, locals: &[JoinBuildLocal]) {
        if self.failed() || self.unused() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let Some(plan) = self.plan_for(locals) else { return };
        *lockm(&shared.leaf_tables[self.leaf]) = Some(Arc::new(freeze(plan, locals)));
    }
}

/// PROBE(i): outer leaf extents against the published leaf table, absorbed
/// into the same plain-agg tail; finalize retires the table when no fill
/// set follows.
struct LeafProbeWork {
    payload: Arc<RuntimeHjShared>,
    leaf: usize,
}

fn leaf_probe_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    leaf: usize,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let spill = payload.spill.as_ref().expect("leaf probe requires spill");
    let outer_plan = Arc::clone(spill.outer_plan.get().ok_or_else(|| {
        Box::new(PgError::new(ERROR, "leaf probe without an outer claim plan"))
    })?);
    let table = lockm(&payload.leaf_tables[leaf]).clone().ok_or_else(|| {
        Box::new(PgError::new(ERROR, "leaf probe without a published leaf table"))
    })?;
    with_worker_exec("runtime hash-join leaf probe without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, _outer_ss, hstate, _inner_ss| {
            let saved = shared_saved_outer_slot(hj);
            let mcx = estate.es_query_cxt;
            for ordinal in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                let (slot, extent) = outer_plan.leaves[leaf][ordinal as usize];
                let g = lockm(&spill.outer[slot]);
                let Some(router) = g.as_ref() else {
                    return Err(Box::new(PgError::new(
                        ERROR,
                        "leaf probe claim without an outer file",
                    )));
                };
                let buf = read_extent_aligned(&router.file, extent)?;
                drop(g);
                let mut it = BatchRecords::new(buf.bytes());
                while let Some((h, tuple)) = it.next_rec()? {
                    // SAFETY: the aligned buffer's record layout keeps the
                    // tuple image MAXALIGNed and live across this probe (the
                    // buffer outlives the loop; the slot is cleared below
                    // before the buffer drops).
                    unsafe {
                        let mtup = NonNull::new_unchecked(tuple.as_ptr() as *mut MinimalTupleData);
                        ::exectuples::exec_store_minimal_tuple_ptr(
                            &mut estate.es_tupleTable[saved.0 as usize],
                            mcx,
                            mtup,
                        );
                    }
                    shared_probe_outer_hashed(
                        hj,
                        hstate,
                        estate,
                        &table,
                        saved,
                        h,
                        &mut |_hj, estate, out| ::nodeagg::agg_plain_build_accept(agg, estate, out),
                    )?;
                }
                // The saved slot must not outlive the claim's buffer.
                ::exectuples::exec_clear_tuple(estate.slot_mut(saved), mcx);
            }
            let pslot = worker - payload.pins_base;
            {
                let mut g = lockm(&payload.partials[pslot]);
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for LeafProbeWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let payload = &self.payload;
        let leaf = self.leaf;
        let r = catch_unwind(AssertUnwindSafe(|| {
            leaf_probe_morsel_body(payload, leaf, worker, range)
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                payload.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                payload.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a leaf probe")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self) {
        // One live table: without a fill set this probe is the last reader.
        let fill = self.payload.spill.as_ref().is_some_and(|sp| sp.fill_inner);
        if !fill {
            lockm(&self.payload.leaf_tables[self.leaf]).take();
        }
    }
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn runtime_hj_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_hj_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeHjShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime hash-join helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeHjShared>) {
    let _ = shared;
    // Every launched helper bumps `exited` exactly once, on EVERY exit path
    // (the leader's liveness reap counts these against `launched`).
    let _exit = ExitBump(&payload.exited);
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving; the reap must convert it into
    // a prompt error (scripts/runtime-liveness-e2e.sh).
    super::test_helper_panic("hashjoin");
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-hashjoin: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-hashjoin: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-hashjoin: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg)
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                // Budget refusals are NOT query errors (the leader falls
                // back to the serial arm); the leader drops any recorded
                // secondary errors on that path.
                payload.fail(e);
                // F1 liveness (the agg-arm wedge mechanism, closed here
                // too): a helper that errored BEFORE joining the drive
                // (build_worker_exec failure) has aborted the RG via
                // fail() — but an aborted PINNED RG still needs a driver to
                // run invalidate/finalize/complete, or the leader parks on
                // its recheck cadence until the reap. Drive the closed
                // generation to completion here (pure protocol cleanup,
                // the drain_rg discipline); post-drive errors find it
                // already complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!(
                    "runtime-hashjoin: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeHjShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    HJ_PAYLOAD.with(|c| *c.borrow_mut() = Some(Arc::clone(payload)));
    let _outcome = payload.rt.drive_pinned(local, rg);
    HJ_PAYLOAD.with(|c| *c.borrow_mut() = None);
    let self_errored =
        HJ_WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    teardown_worker_exec(!self_errored)
}

fn build_worker_exec(payload: &Arc<RuntimeHjShared>) -> PgResult<()> {
    HJ_WORKER_EXEC.with(|cell| -> PgResult<()> {
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
        let armed = (|| -> PgResult<()> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime hash-join worker ExecutorStart");
                x.with_mut(|d| {
                    with_join_tree(&mut d.estate, &mut d.planstate, |estate, agg, hj, outer_ss, hstate, inner_ss| {
                        if !agg_runtime_partial_admissible(agg) {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime hash-join worker fold plan diverged from the leader's",
                            )));
                        }
                        if !shared_join_admissible(hj, hstate) {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime hash-join worker join shape diverged from the leader's",
                            )));
                        }
                        // Per-row drive staging on both scans (the census
                        // RowFeed shape: PREWHERE bitmap when kernel-shaped,
                        // stitched tiers on; per-row emits re-check quals).
                        super::arm_scan_staging(
                            outer_ss,
                            estate,
                            super::ScanFeedShape::RowFeed {
                                ctx: "runtime hash-join probe feed",
                                stitch: true,
                            },
                        )?;
                        super::arm_scan_staging(
                            inner_ss,
                            estate,
                            super::ScanFeedShape::RowFeed {
                                ctx: "runtime hash-join build feed",
                                stitch: true,
                            },
                        )?;
                        ::nodeagg::agg_plain_build_begin(agg, estate)?;
                        Ok(())
                    })
                })
            })
        })();
        match armed {
            Ok(()) => {
                *cell.borrow_mut() =
                    Some(WorkerExec { qd, errored: std::cell::Cell::new(false) });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    HJ_WORKER_EXEC.with(|cell| -> PgResult<()> {
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

fn runtime_hj_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeHjShared>() else { return };
    payload.abort_rg();
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_hashjoin_main",
            runtime_hj_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_hj_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_hj_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

/// The runtime hash-join arm. `None` = not engaged (caller falls through to
/// the serial arms byte-identically — nothing was consumed). `Some(row)` =
/// the plain agg's one finalized result row.
pub(super) fn try_own_agg_over_hash_join_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // M5-1: the router is the DOP source (bench GUC verbatim when set; else
    // engine=runtime arms at pgrust.runtime_dop; else 0 = today's path).
    let dop = router::arm_dop(ArmClass::HashJoin);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // Done-repulls (the post-completion pull that exits via agg_is_done
    // below) are not offers — see the scan arm's identical gate.
    if !::nodeagg::agg_is_done(agg) {
        router::tick(ArmClass::HashJoin, ArmCounter::Offered);
    }
    // M5-1 refusal funnel: every admission exit names its gate for the
    // router's consolidated taxonomy (previously silent early returns).
    fn refuse(reason: &'static str) {
        router::tick_refused(ArmClass::HashJoin, reason);
    }

    // --- Node shape: HashJoin over two lane-fusible cbstore SeqScans; a
    // fresh (untouched) join; phase-1 join types; subplan/param-free exprs.
    let crate::procnode::PlanStateNode::SeqScan(outer_ss) = &mut *hj.outer else {
        refuse("outer-not-seqscan");
        return Ok(None);
    };
    let hash = &mut *hj.hash;
    let crate::procnode::PlanStateNode::SeqScan(inner_ss) = &mut *hash.child else {
        refuse("inner-not-seqscan");
        return Ok(None);
    };
    if !shared_join_admissible(&hj.state, &hash.state) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        refuse("join-shape");
        return Ok(None);
    }
    if !::nodehashjoin::lane_join_untouched(&hj.state, &hash.state) {
        refuse("join-touched");
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        refuse("partials-not-order-insensitive-exact");
        return Ok(None);
    }
    if !seq_scan_fusible(outer_ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(outer_ss) {
        refuse("outer-scan-not-fusible");
        return Ok(None);
    }
    if !seq_scan_fusible(inner_ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(inner_ss) {
        refuse("inner-scan-not-fusible");
        return Ok(None);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refuse("instrumented-or-epq");
        return Ok(None);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refuse("in-parallel-mode");
        return Ok(None);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse("params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refuse("no-plannedstmt");
        return Ok(None);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse("params");
        return Ok(None);
    }
    // Agg must be the plan root; its child the HashJoin; the join's children
    // the two scans (the worker pstmt transfers the whole root subtree).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else { return Ok(None) };
    if !std::ptr::eq(root_agg, agg.plan) {
        return Ok(None);
    }
    let Some(join_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if join_node.node_tag() != NodeTag::T_HashJoin {
        return Ok(None);
    }
    let join_plan = join_node.as_hash_join().expect("HashJoin tag");
    let Some(outer_plan) = join_plan.join.plan.lefttree else { return Ok(None) };
    let Some(hash_plan_node) = join_plan.join.plan.righttree else { return Ok(None) };
    if outer_plan.node_tag() != NodeTag::T_SeqScan
        || hash_plan_node.node_tag() != NodeTag::T_Hash
    {
        return Ok(None);
    }
    let hash_plan = hash_plan_node.as_hash().expect("Hash tag");
    let Some(inner_plan) = hash_plan.plan.lefttree else { return Ok(None) };
    if inner_plan.node_tag() != NodeTag::T_SeqScan {
        return Ok(None);
    }
    // Parallel-safety walk over everything that runs on helpers.
    let outer_scan_plan = outer_plan.as_seq_scan().expect("SeqScan tag");
    let inner_scan_plan = inner_plan.as_seq_scan().expect("SeqScan tag");
    if !exprs_parallel_safe(outer_scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(outer_scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(inner_scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(inner_scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(join_plan.hashclauses.iter())?
        || !exprs_parallel_safe(join_plan.join.joinqual.iter())?
        || !exprs_parallel_safe(join_plan.join.plan.qual.iter())?
        || !exprs_parallel_safe(join_plan.join.plan.targetlist.iter())?
    {
        refuse("exprs-not-parallel-safe");
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refuse("non-mvcc-snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refuse("binder-policy");
        return Ok(None);
    }

    // --- Envelope sizing (§6): C's combined-budget rule. nbatch > 1 now
    // engages the M3.5 batch arm; the spill kill switch restores the
    // phase-1 refusal exactly.
    let (_, nbatch, _, space_allowed) = ::nodehash::exec_choose_hash_table_size_full(
        hash_plan.plan.plan_rows,
        hash_plan.plan.plan_width,
        false, // useskew: C PHJ parity — no skew in parallel
        true,  // try_combined_hash_mem: pooled participant budget
        dop,
    );
    // SPILL ENVELOPE (train-14 inc-4/5 ledger, open item 2): every live
    // table here — batch 0 and every file leaf — is ONE gang-shared table,
    // so its envelope is the raw combined limit `get_hash_memory_limit() ×
    // (dop+1)`. exec_choose's `space_allowed` is NOT that once nbatch > 1:
    // C recurses to PER-WORKER sizing (its PHJ builds per-worker batch
    // tables) and only partially restores it under the buckets-vs-batches
    // rebalance — the reduced envelope over-fanned splits (leg 6 landed
    // exactly at the 32-leaf cap at jbits=4) and manufactured admission
    // refusals a combined-envelope leaf absorbs. `max` keeps the envelope
    // monotonic vs the old value; at nbatch == 1 `space_allowed` IS the
    // combined limit, so unbatched/dormant engagements are bit-identical.
    let combined_limit =
        ::nodehash::get_hash_memory_limit().saturating_mul(dop.max(0) as usize + 1);
    let envelope = space_allowed.max(combined_limit);
    let mut spill_batches: Option<u32> = hj_spill_force_batches();
    if nbatch > 1 && spill_batches.is_none() {
        if !hj_spill_enabled() {
            lane_trace("runtime-hashjoin: REFUSED (estimated nbatch > 1) — serial arm");
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            // M5-1 refusal funnel: the spill kill switch restores the
            // phase-1 refusal — name the gate for the router taxonomy.
            refuse("estimated-multi-batch");
            return Ok(None);
        }
        // Headroomed sizing (§5.3: round-0 splits should be rare): level-0
        // batch count from the estimated inner footprint over the COMBINED
        // envelope each leaf actually gets — C's nbatch is relative to the
        // reduced per-worker budget and over-fans by ~(dop+1)×.
        let est_inner = ::nodehash::estimate_hash_inner_rel_bytes(
            hash_plan.plan.plan_rows,
            hash_plan.plan.plan_width,
        );
        let need = est_inner.div_ceil(envelope.max(1) as u64).max(1);
        let want = u32::try_from(need)
            .unwrap_or(u32::MAX / 2)
            .next_power_of_two()
            .saturating_mul(2)
            .max(2);
        if want as usize > hj_spill_max_batches() {
            lane_trace(&format!(
                "runtime-hashjoin: REFUSED (estimated batches {want} exceed the spill batch cap) — serial arm"
            ));
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            refuse("spill-batch-cap");
            return Ok(None);
        }
        spill_batches = Some(want);
    }

    // --- Geometry: the probe side pays the gang; the build side may be a
    // small dimension table (any nonzero geometry admits).
    let Some((outer_granules, outer_starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(outer_ss, estate)?
    else {
        refuse("geometry");
        return Ok(None);
    };
    let Some((_inner_granules, inner_starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(inner_ss, estate)?
    else {
        refuse("geometry");
        return Ok(None);
    };
    if outer_granules < min_granules().max(2 * dop as u64) {
        refuse("tiny-input-floor");
        return Ok(None);
    }
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill: PGRUST_RUNTIME_ELASTIC_DOP=0).
    let dop = super::runtime_scan::elastic_dop(dop, outer_granules);
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // Right-fill family (RIGHT/FULL/RIGHT_ANTI) adds the FILL task set(s).
    let fill_inner = matches!(
        join_plan.join.jointype,
        ::types_nodes::JoinType::JOIN_RIGHT
            | ::types_nodes::JoinType::JOIN_FULL
            | ::types_nodes::JoinType::JOIN_RIGHT_ANTI
    );

    // Router counter choke point (M5-1): Engaged = ceremony entered;
    // Completed = the runtime answered; Fallback = R5 serial rerun.
    router::tick(ArmClass::HashJoin, ArmCounter::Engaged);
    let r = engage(
        agg,
        estate,
        rt,
        dop,
        outer_granules,
        outer_starts,
        inner_starts,
        envelope,
        fill_inner,
        spill_batches,
    )?;
    router::tick(
        ArmClass::HashJoin,
        if r.is_some() { ArmCounter::Completed } else { ArmCounter::Fallback },
    );
    Ok(r)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    outer_granules: u64,
    outer_starts: Vec<u64>,
    inner_starts: Vec<u64>,
    // Combined gang envelope per live table (admission's `envelope`; equals
    // exec_choose's space_allowed exactly when nbatch == 1).
    envelope: usize,
    fill_inner: bool,
    spill_batches: Option<u32>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    // M3.5: the engagement's SpillSet (leader-side creation — fd substrate
    // guaranteed; a creation failure fail-closes to the phase-1 refusal).
    let spill = match spill_batches {
        Some(n) => match ::spillset::SpillSet::create() {
            Ok(set) => Some(Arc::new(HjSpill::new(
                set,
                n,
                envelope,
                dop as u64,
                fill_inner,
            ))),
            Err(_) => {
                lane_trace(
                    "runtime-hashjoin: spill set creation failed — refusing to the serial arm",
                );
                stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
                return Ok(None);
            }
        },
        None => None,
    };

    let agg_node = estate.es_plannedstmt.and_then(|p| p.planTree).expect("gated above");
    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;

    let leaf_cap = spill.as_ref().map_or(0, |s| s.leaf_cap);
    let payload = Arc::new(RuntimeHjShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (the M1 SendConst discipline).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        pins_base: rt.nthreads(),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        budget_refused: AtomicBool::new(false),
        partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        sink: OnceLock::new(),
        spill,
        leaf_tables: (0..leaf_cap).map(|_| Mutex::new(None)).collect(),
    });
    let sink = Arc::new(JoinBuildSink {
        budget: JoinBudget::new(envelope),
        plan: Mutex::new(None),
        table: Mutex::new(None),
        shared: Arc::downgrade(&payload),
    });
    payload.sink.set(Arc::clone(&sink)).unwrap_or_else(|_| unreachable!("sink set once"));

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        agg,
        estate,
        rt,
        dop,
        outer_granules,
        outer_starts,
        inner_starts,
        &payload,
        sink,
        fill_inner,
    );
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
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    outer_granules: u64,
    outer_starts: Vec<u64>,
    inner_starts: Vec<u64>,
    payload: &Arc<RuntimeHjShared>,
    sink: Arc<JoinBuildSink>,
    fill_inner: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_hashjoin_main", dop)?;
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

        // Task sets [0]/[1]: the batch-0 build sink pair.
        let runtime::SinkTaskSets { accept, combine, probe: _sink_probe } =
            runtime::sink_tasksets(
                sink,
                Arc::new(CbstoreGranuleSource {
                    starts: Arc::new(inner_starts),
                    // This arm feeds claims straight into set_granule_range
                    // (single-epoch contract); it does not subdivide
                    // multi-epoch claims — never coalesce.
                    coalesce: false,
                }),
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        let mut tasksets = vec![accept, combine];
        // M3.5 ladder: PLAN-BATCHES + split rounds precede PROBE(0).
        let mut probe0_deps = vec![1usize];
        if let Some(sp) = payload.spill.as_ref() {
            let plan_idx = tasksets.len();
            tasksets.push(runtime::TaskSetSpec {
                source: Arc::new(OneGranuleSource),
                work: Arc::new(PlanBatchesWork(Arc::clone(payload))),
                deps: vec![1],
            });
            let mut prev = plan_idx;
            for r in 0..sp.rounds_max {
                let idx = tasksets.len();
                tasksets.push(runtime::TaskSetSpec {
                    source: Arc::clone(&sp.round_sources[r]) as Arc<dyn runtime::MorselSource>,
                    work: Arc::new(SplitRoundWork { payload: Arc::clone(payload), round: r }),
                    deps: vec![prev],
                });
                prev = idx;
            }
            probe0_deps = vec![prev];
        }
        let probe0_idx = tasksets.len();
        tasksets.push(runtime::TaskSetSpec {
            source: Arc::new(CbstoreGranuleSource {
                starts: Arc::new(outer_starts),
                // As above: straight set_granule_range feed, never coalesce.
                coalesce: false,
            }),
            work: Arc::clone(payload) as Arc<dyn runtime::TaskSetWork>,
            deps: probe0_deps,
        });
        let mut tail = probe0_idx;
        if fill_inner {
            // Right-fill family: the unmatched-build walk, after the probe
            // barrier (the match-flag visibility edge).
            let idx = tasksets.len();
            tasksets.push(runtime::TaskSetSpec {
                source: Arc::new(FillPartitionSource),
                work: Arc::new(FillWork { payload: Arc::clone(payload), leaf: None }),
                deps: vec![tail],
            });
            tail = idx;
        }
        // M3.5 leaf ladders: chained ACCEPT(i)/COMBINE(i)/PROBE(i)/FILL(i)
        // — one live table at a time (C parity); unused slots run empty.
        if let Some(sp) = payload.spill.as_ref() {
            for leaf in 0..sp.leaf_cap {
                let leaf_sink = Arc::new(LeafBatchSink {
                    shared: Arc::downgrade(payload),
                    leaf,
                    budget: JoinBudget::new(sp.space_allowed),
                    plan: Mutex::new(None),
                });
                let accept_idx = tasksets.len();
                let runtime::SinkTaskSets { mut accept, combine, probe: _p } =
                    runtime::sink_tasksets(
                        leaf_sink,
                        Arc::clone(&sp.leaf_in_sources[leaf]) as Arc<dyn runtime::MorselSource>,
                        rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                        accept_idx,
                    );
                accept.deps = vec![tail];
                tasksets.push(accept);
                let combine_idx = tasksets.len();
                tasksets.push(combine);
                let probe_idx = tasksets.len();
                tasksets.push(runtime::TaskSetSpec {
                    source: Arc::clone(&sp.leaf_out_sources[leaf])
                        as Arc<dyn runtime::MorselSource>,
                    work: Arc::new(LeafProbeWork { payload: Arc::clone(payload), leaf }),
                    deps: vec![combine_idx],
                });
                tail = probe_idx;
                if fill_inner {
                    let idx = tasksets.len();
                    tasksets.push(runtime::TaskSetSpec {
                        source: Arc::new(FillPartitionSource),
                        work: Arc::new(FillWork {
                            payload: Arc::clone(payload),
                            leaf: Some(leaf),
                        }),
                        deps: vec![tail],
                    });
                    tail = idx;
                }
            }
        }
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned_with_affinity(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets,
        }, router::session_affinity_token());
        payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-hashjoin: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        match payload.spill.as_ref() {
            Some(sp) => lane_trace(&format!(
                "runtime-hashjoin: engaged dop={launched} outer_granules={outer_granules} nbatch={} (spill)",
                sp.nbatch
            )),
            None => lane_trace(&format!(
                "runtime-hashjoin: engaged dop={launched} outer_granules={outer_granules}"
            )),
        }

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
                lane_trace(&format!(
                    "runtime-hashjoin: all {refused} helpers refused the bind"
                ));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-hashjoin: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg(rt, &rg);
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime hash-join helpers exited before completing the join",
                )));
            }
            // LIVENESS REAP (m35-spill inc-2c port — the FLAG named this
            // arm class; the agg leg-4d wedge): a pinned RG is invisible to
            // pool workers, so once every launched helper has exited
            // without the RG completing, NOBODY will ever step it and the
            // leader parks forever (the all-stopped probe above cannot see
            // helpers that exited their drive but parked back to the pool).
            // Reap: abort + drain the closed generation ourselves; the next
            // try_wait surfaces Aborted and the existing error/budget/
            // fallback handling below decides. Two consecutive sightings
            // before reaping let a mid-settlement completion land first —
            // belt only: a helper's exit bump happens-after its drive's
            // completion, and abort + drive_pinned on a completed RG are
            // benign no-ops.
            if payload.exited.load(Ordering::SeqCst) >= launched as usize {
                if all_exited_seen && waiter.try_wait().is_none() {
                    lane_trace(
                        "runtime-hashjoin: all helpers exited without completing the RG — reaping",
                    );
                    rg.abort();
                    if !drain_rg(rt, &rg) {
                        // The one exit path with no give-up escape would be a
                        // leader hang: a reap that cannot drain (dead/stuck
                        // participant) surfaces an error instead of
                        // re-reaping forever.
                        if let Some(e) = payload.take_error() {
                            return Err(e);
                        }
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime hash-join helpers exited and the RG could not be drained",
                        )));
                    }
                    continue;
                }
                all_exited_seen = true;
            }
            // A raised cancel disposition (statement_timeout /
            // pg_cancel_backend) surfaces from the latch quantum as an Err
            // (F1 defect layer 2b): abort + drain the RG, then propagate —
            // exactly the CFI branch above. Discarding it made this park
            // loop uncancellable (the F1 chaos finding the quantum's
            // contract documents; fixed at the M5-2 consolidation).
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
        };

        if payload.budget_refused.load(Ordering::SeqCst) {
            // §6/R5: envelope crossing — whole-attempt rerun on the serial
            // arm. Drop any recorded secondary errors (the abort races
            // in-flight morsels); nothing was consumed on the leader.
            let _ = payload.take_error();
            lane_trace("runtime-hashjoin: envelope refusal — falling back to the serial arm");
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            return Ok(EngageOutcome::Fallback);
        }
        if let Some(e) = payload.take_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "runtime hash-join pipeline aborted")));
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
            lane_trace("runtime-hashjoin: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let parts: Vec<RuntimePartial> = payload
                .partials
                .iter()
                .filter_map(|m| lockm(m).take())
                .collect();
            let combined = agg_runtime_combine(agg, &parts)?;
            stats::tick_owned(ShapeClass::Join);
            if let Some(sp) = payload.spill.as_ref() {
                // The R4 spill channel: live counters at adopt.
                let (inner, outer, split) = sp.spilled_census();
                lane_trace(&format!(
                    "runtime-hashjoin: SPILLED batches={} leaves={} inner_bytes={inner} outer_bytes={outer} split_bytes={split} splits={} max_round={}",
                    sp.nbatch,
                    sp.leaves_used.load(Ordering::SeqCst),
                    sp.splits.load(Ordering::Relaxed),
                    sp.max_round.load(Ordering::Relaxed),
                ));
            }
            lane_trace(&format!("runtime-hashjoin: complete, partials={}", parts.len()));
            Ok(Some(exec_agg_runtime_partials(agg, estate, &combined)?))
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG nobody will drive (the M1 drain
/// discipline: cleanup driving, not leader work execution).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-hashjoin: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-hashjoin: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}
