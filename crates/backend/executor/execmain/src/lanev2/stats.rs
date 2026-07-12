//! Lane-v2 engagement/refusal accounting — the substrate of the lane honesty
//! gates (design doc "Definition of complete": engagement floor +
//! assert-refuse allowlist; `scripts/lane-gates.sh`).
//!
//! Structure: one process-global relaxed-atomic counter per (shape class) for
//! OWNED decisions, and per (shape class × refusal reason) for REFUSED
//! decisions. Backends are threads of one process here, so the totals
//! aggregate across all backends for free.
//!
//! Tick semantics (documented per class; the gate's floor file restates them):
//!   * `SeqScan` — one tick per memoized static verdict (≈ one per scan node
//!     per (re)init), plus per-call ticks for the dynamic EPQ/backward gates
//!     (which run before the memo and are rare).
//!   * `IndexScan` / `IndexOnlyScan` / `BitmapHeapScan` — the fusibility check
//!     is per `exec_proc_node` call (not node-memoized), so owned/refused
//!     ticks are per-pull decisions. Counts are large but deterministic for a
//!     fixed corpus.
//!   * `AggBuild` — one OWNED tick per lane-owned hash-agg build (the
//!     drain-the-scan-pipeline event); refusals per offered call.
//!   * `SortFeed` — one OWNED tick per lane-owned sort feed; structural
//!     refusals once per memoized verdict; dynamic EPQ/backward per call.
//!   * `Join` — one OWNED tick per lane-owned join build event; structural
//!     refusals once per memoized verdict; dynamic EPQ/backward,
//!     fused-probe-drive economics, and multi-batch spill refusals per call.
//!   * `Group` — one OWNED tick per lane-owned group-over-sort drive start
//!     (the underlying sort-feed event); refusals per offered call (the
//!     child-Sort verdict itself is memoized on the Sort node, so the
//!     per-call cascade is one flag load).
//!   * `ResultNode` — one OWNED tick per lane-owned Result execution (the
//!     no-FROM row / one-time-gate consumption, or the child feed event);
//!     refusals per offered call.
//!   * `SubqueryScan` — one OWNED tick per lane-owned feed event (the
//!     child sort feed for the bare hook; the agg build event for the
//!     agg-over-subquery composition); refusals per offered call.
//!
//! Overhead: with the lane OFF nothing here ever runs (the dispatch hooks gate
//! on `lanev2::enabled()` before any lane code). With the lane ON but
//! accounting disarmed (no `PGRUST_LANE_V2_STATS`), every tick is one cached
//! pointer load + branch. With accounting armed it is additionally one relaxed
//! `fetch_add` per lane-path decision — decisions, not rows (except the
//! per-pull index/IOS/bitmap classes noted above, which are exactly as
//! frequent as the fusibility checks the lane already runs there).
//!
//! Reporting: `PGRUST_LANE_V2_STATS=<dir>` arms the accounting; each backend
//! thread that ticked at least once dumps the *cumulative process-wide*
//! totals to `<dir>/lane-v2-stats.<pid>.tsv` when the thread exits (a TLS
//! drop guard — no exit-hook wiring, no new GUC, no change to `pg_settings`
//! byte-identity). Dumps overwrite atomically (tmp + rename) under a mutex,
//! so the last backend to exit leaves the final totals; the harness sums
//! across files (one per server *process*, i.e. per resume segment). NOT a
//! GUC by design — see the module doc of `lanev2` on `pg_settings`
//! byte-identity.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

/// Lane-ownable plan-shape classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ShapeClass {
    SeqScan = 0,
    IndexScan = 1,
    IndexOnlyScan = 2,
    BitmapHeapScan = 3,
    AggBuild = 4,
    SortFeed = 5,
    Join = 6,
    Group = 7,
    ResultNode = 8,
    SubqueryScan = 9,
}

const N_CLASSES: usize = 10;

impl ShapeClass {
    pub(super) const ALL: [ShapeClass; N_CLASSES] = [
        ShapeClass::SeqScan,
        ShapeClass::IndexScan,
        ShapeClass::IndexOnlyScan,
        ShapeClass::BitmapHeapScan,
        ShapeClass::AggBuild,
        ShapeClass::SortFeed,
        ShapeClass::Join,
        ShapeClass::Group,
        ShapeClass::ResultNode,
        ShapeClass::SubqueryScan,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            ShapeClass::SeqScan => "seqscan",
            ShapeClass::IndexScan => "indexscan",
            ShapeClass::IndexOnlyScan => "indexonlyscan",
            ShapeClass::BitmapHeapScan => "bitmapheapscan",
            ShapeClass::AggBuild => "aggbuild",
            ShapeClass::SortFeed => "sortfeed",
            ShapeClass::Join => "join",
            ShapeClass::Group => "group",
            ShapeClass::ResultNode => "result",
            ShapeClass::SubqueryScan => "subqueryscan",
        }
    }
}

/// Why the lane refused a shape it was offered. Every variant an actual
/// refusal site can tick MUST appear in `scripts/lane-gates.allowlist` —
/// adding a variant here without an allowlist entry makes the gate fail the
/// first time it is observed, which is the point: a new deliberate refusal is
/// a reviewed, documented act.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RefuseReason {
    /// Reserved: the master `PGRUST_LANE_V2` switch is checked by the dispatch
    /// hooks *before* any lane code runs, so this never ticks today; it exists
    /// so a future in-lane kill-switch has a stable name.
    #[allow(dead_code)]
    EnvOff = 0,
    /// EvalPlanQual re-check active (model-incompatible, §4).
    Epq = 1,
    /// Non-forward scan direction / backward index order.
    Backward = 2,
    /// Scrollable/backward/mark cursor eflags (`!batch_allowed`).
    ScrollMark = 3,
    /// EXPLAIN ANALYZE instrumentation (§4: refused by policy).
    Instrumented = 4,
    /// Parallel-aware node / worker shared state (Phase-2 worker-safety
    /// pending for the index/IOS/bitmap lanes).
    ParallelGate = 5,
    /// Qual/projection/recheck carries a SubPlan or exec-param dependency.
    SubplanParam = 6,
    /// Index runtime keys (exec-param-driven rescan keys).
    RuntimeKeys = 7,
    /// Non-btree index AM.
    NonBtree = 8,
    /// Non-MVCC snapshot (tidrun batching unsound).
    NonMvccSnapshot = 9,
    /// SeqScan Bloom variant.
    BloomVariant = 10,
    /// Table AM lacks the page-batch primitives.
    NoPageBatch = 11,
    /// amcanorderbyop reorder (`iss_OrderBy` / `ioss_OrderByKeys`).
    OrderByReorder = 12,
    /// Index/IOS/bitmap lanes admit only the bare-tuple shape today: a scan
    /// qual or projection refuses (Phase-2 breadth hosts them).
    ShapeQualProj = 13,
    /// Sort breaker: tuplesort random access required (REWIND/BACKWARD/MARK).
    RandomAccess = 14,
    /// Sort breaker: child is not a lane-fusible scan node type.
    NonScanChild = 15,
    /// Sort breaker: the child scan's own refuse-set refused (the specific
    /// reason is ticked under the child's class).
    ChildScanRefused = 16,
    /// Agg breaker: not batch-drainable (grouping sets / DISTINCT-or-ordered
    /// input / merge phase / subplan transitions / non-AGG_HASHED /
    /// initplan params).
    AggNotDrainable = 17,
    /// Admission economics (design §4): the legacy fused drive already owns
    /// this shape better than the v2 pipeline (fused agg batch drive; fused
    /// hash-join probe drive). Ticked once per memoized agg-lane choice, and
    /// per pull for the join dispatch arm.
    AdmissionEconomicsFusedDrive = 18,
    /// Admission economics (design §4): no batch-consuming parent, so v2
    /// ownership is pure adapter overhead (`STANDALONE_SCAN_NO_UPSIDE`).
    /// Ticked per pull at the standalone `try_own_*` scan hooks.
    AdmissionEconomicsNoConsumer = 19,
    /// Dynamic tiny-input row-floor (§4 endgame refuse-set).
    #[allow(dead_code)]
    TinyInputFloor = 20,
    /// Hash-join breaker: join-side shape refuse — non-INNER faces,
    /// joinqual/otherqual residuals, instrumented, subplan/param-bearing join
    /// exprs, or a node the row path already drove (whole-life ownership).
    JoinShape = 21,
    /// Hash-join breaker: the completed build's final nbatch > 1 (spill);
    /// the probe is refused before any lane tuple is emitted.
    MultiBatch = 22,
    /// Wave-4 streaming glue (Group / Result / SubqueryScan): the node's
    /// child is not a lane-owned pipeline this hook can chain onto — wrong
    /// node type, or a lane-ownable child whose own refuse-set refused (the
    /// specific reason ticks under the child's class).
    ChildNotLaneOwned = 23,
}

const N_REASONS: usize = 24;

impl RefuseReason {
    pub(super) fn name(self) -> &'static str {
        match self {
            RefuseReason::EnvOff => "env-off",
            RefuseReason::Epq => "epq",
            RefuseReason::Backward => "backward",
            RefuseReason::ScrollMark => "scroll-mark",
            RefuseReason::Instrumented => "instrumented",
            RefuseReason::ParallelGate => "parallel-gate",
            RefuseReason::SubplanParam => "subplan-param",
            RefuseReason::RuntimeKeys => "runtime-keys",
            RefuseReason::NonBtree => "non-btree",
            RefuseReason::NonMvccSnapshot => "non-mvcc-snapshot",
            RefuseReason::BloomVariant => "bloom-variant",
            RefuseReason::NoPageBatch => "no-pagebatch",
            RefuseReason::OrderByReorder => "order-by-reorder",
            RefuseReason::ShapeQualProj => "shape-qual-proj",
            RefuseReason::RandomAccess => "random-access",
            RefuseReason::NonScanChild => "non-scan-child",
            RefuseReason::ChildScanRefused => "child-scan-refused",
            RefuseReason::AggNotDrainable => "agg-not-drainable",
            RefuseReason::AdmissionEconomicsFusedDrive => "admission-economics-fused-drive",
            RefuseReason::AdmissionEconomicsNoConsumer => "admission-economics-no-consumer",
            RefuseReason::TinyInputFloor => "tiny-input-floor",
            RefuseReason::JoinShape => "join-shape",
            RefuseReason::MultiBatch => "multi-batch",
            RefuseReason::ChildNotLaneOwned => "child-not-lane-owned",
        }
    }

    fn from_index(i: usize) -> RefuseReason {
        use RefuseReason::*;
        [
            EnvOff,
            Epq,
            Backward,
            ScrollMark,
            Instrumented,
            ParallelGate,
            SubplanParam,
            RuntimeKeys,
            NonBtree,
            NonMvccSnapshot,
            BloomVariant,
            NoPageBatch,
            OrderByReorder,
            ShapeQualProj,
            RandomAccess,
            NonScanChild,
            ChildScanRefused,
            AggNotDrainable,
            AdmissionEconomicsFusedDrive,
            AdmissionEconomicsNoConsumer,
            TinyInputFloor,
            JoinShape,
            MultiBatch,
            ChildNotLaneOwned,
        ][i]
    }
}

static OWNED: [AtomicU64; N_CLASSES] = [const { AtomicU64::new(0) }; N_CLASSES];
#[allow(clippy::declare_interior_mutable_const)]
static REFUSED: [[AtomicU64; N_REASONS]; N_CLASSES] =
    [const { [const { AtomicU64::new(0) }; N_REASONS] }; N_CLASSES];

/// The accounting arm switch: `PGRUST_LANE_V2_STATS=<dir>`. Resolved once per
/// process, like `lanev2::enabled()`.
fn stats_dir() -> Option<&'static PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os("PGRUST_LANE_V2_STATS").map(PathBuf::from))
        .as_ref()
}

/// Record an OWNED decision for `class`. One cached load + branch when
/// accounting is disarmed.
#[inline]
pub(super) fn tick_owned(class: ShapeClass) {
    if stats_dir().is_none() {
        return;
    }
    OWNED[class as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record a REFUSED decision for `class` with its reason.
#[inline]
pub(super) fn tick_refused(class: ShapeClass, reason: RefuseReason) {
    if stats_dir().is_none() {
        return;
    }
    REFUSED[class as usize][reason as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// TLS drop guard: any backend thread that ticked dumps the cumulative totals
/// on its way out (backend exit = thread exit in this server). Dump-on-exit
/// keeps the hot path free of I/O and needs no exit-callback registration.
struct DumpOnThreadExit;

impl Drop for DumpOnThreadExit {
    fn drop(&mut self) {
        dump();
    }
}

#[inline]
fn arm_dump_on_thread_exit() {
    thread_local! {
        static GUARD: DumpOnThreadExit = const { DumpOnThreadExit };
    }
    // Touching the TLS key initializes it (arming the drop) on first use.
    GUARD.with(|_| {});
}

/// Write the cumulative process-wide totals to
/// `<dir>/lane-v2-stats.<pid>.tsv` (atomic tmp+rename; serialized so the last
/// writer's snapshot is also the latest one). Lines:
///   `owned\t<class>\t<count>`            (every class, zeros included)
///   `refused\t<class>\t<reason>\t<count>` (nonzero only)
fn dump() {
    let Some(dir) = stats_dir() else { return };
    static DUMP_LOCK: Mutex<()> = Mutex::new(());
    let _guard = match DUMP_LOCK.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut out = String::new();
    for class in ShapeClass::ALL {
        out.push_str(&format!(
            "owned\t{}\t{}\n",
            class.name(),
            OWNED[class as usize].load(Relaxed)
        ));
    }
    for class in ShapeClass::ALL {
        for (i, cell) in REFUSED[class as usize].iter().enumerate() {
            let n = cell.load(Relaxed);
            if n > 0 {
                out.push_str(&format!(
                    "refused\t{}\t{}\t{}\n",
                    class.name(),
                    RefuseReason::from_index(i).name(),
                    n
                ));
            }
        }
    }
    let pid = std::process::id();
    let final_path = dir.join(format!("lane-v2-stats.{pid}.tsv"));
    let tmp_path = dir.join(format!(
        ".lane-v2-stats.{pid}.{:?}.tmp",
        std::thread::current().id()
    ));
    // Best-effort by design: accounting must never turn into a query error.
    let _ = std::fs::create_dir_all(dir);
    if std::fs::write(&tmp_path, out).is_ok() {
        let _ = std::fs::rename(&tmp_path, &final_path);
    }
}
