//! M5 unified admission router (docs/design/m5-planner.md §2; increments
//! M5-0 + M5-1, branch m5-router).
//!
//! WHAT THE ROUTER IS (Option A, §2.1): one executor-startup decision layer
//! over the SERIAL plan that owns (1) the engine selection — legacy Gather
//! machinery vs runtime engagement vs serial, (2) the ARMING source for the
//! six runtime arm engagements (pgrcolumnar scan, heap source, agg sink,
//! distinct sink, hash join, sort), and (3) the consolidated
//! engagement/refusal/win-loss telemetry those arms feed. The six per-arm
//! fail-closed admission walks REMAIN the router's per-class predicates —
//! nothing is duplicated; the arms keep walking their own gates and every
//! refusal answers via the serial arm (R5 whole-attempt rerun), exactly as
//! before.
//!
//! Routing decision, per query (executor startup, `query_start`):
//!   * plan carries the legacy parallel machinery (`parallelModeNeeded`,
//!     i.e. the planner generated Gather/GatherMerge/PHJ) → the LEGACY
//!     engine executes it, byte-untouched (suppression of those paths under
//!     engine=runtime is M5-3's planner choke-point probe, not ours);
//!   * serial-shaped plan → the arm offers run at their existing sites; the
//!     router supplies the DOP (`arm_dop`) and counts the verdicts;
//!   * everything else → serial, as today.
//!
//! Arming layering (§2.2), top to bottom — `arm_dop` is THE single source:
//!   1. per-arm bench pool GUCs (`pgrust.runtime_{scan,agg,distinct,
//!      hashjoin,sort}_pool`) — the DEVELOPER/bench override layer; when set
//!      they win VERBATIM in both engine settings (arm recipes and manifests
//!      unchanged — the hard M5-0 requirement);
//!   2. otherwise, engine=runtime with a live pool arms covered offers at
//!      `pgrust.runtime_dop` (default cores) — the product path;
//!   3. otherwise 0 (unarmed; exactly today's serial behavior).
//!   Per-arm kill switches (`PGRUST_RUNTIME_*`) disarm their arm in BOTH
//!   layers; `PGRUST_RUNTIME=1` (pool exists) and the lane master GUC gate
//!   everything, unchanged.
//!
//! Composition-state laws (§2.4) the router contract carries — the train-13
//! merge-born invariants, restated where the dispatch lives:
//!   1. SPILL-ARMED RETAINED-RESULT LAW: the R3 retained-emit envelope
//!      applies ONLY when spill is DISARMED; a spill-armed engagement's
//!      memory law is the metered per-partition spill discipline. "Does this
//!      engagement fit the retained-emit envelope" is ARM-INTERNAL and
//!      spill-state-dependent — the router NEVER pre-refuses on it (the
//!      train's one measured composition defect; regression legs: agg e2e
//!      4/4d/4e, sort-b leg 7, run through this router every increment).
//!   2. Cross-car degrade guards live in the arms and surface here only as
//!      taxonomy: a SPILLED Local disqualifies pass-through/table-adopt; an
//!      armed top-N spec degrades the pass-through and combine-split arms
//!      (full drain, never a correctness change); canonical-bytes (text-key)
//!      engagements DISABLE the word-mode spill arm — their budget refusals
//!      are EXPECTED and must be attributed to the matrix gap flag
//!      (`gap:bytes-spill-record`), never misread as router bugs.
//!   3. The bytes-spill-record gap is a data-driven matrix flag
//!      (crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv), not a hardcoded rule — when the bytes
//!      spill record lands, the matrix cell flips and no router code moves.
//!
//! The living coverage matrix (crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv) is asserted
//! against this module's class list by `tests::coverage_matrix_is_consistent`
//! — M5-3's suppression probe is generated from the same file, so "covered"
//! claims and routing flags cannot drift apart silently.
//!
//! Telemetry: process-global counters per arm class — offered / engaged /
//! completed / fallback (R5 loss) / refused-by-reason (the arms' own static
//! reason vocabulary) — armed by the existing `PGRUST_LANE_V2_STATS=<dir>`
//! switch and dumped to `<dir>/m5-router-stats.<pid>.tsv` on backend-thread
//! exit (the stats.rs discipline: relaxed atomics, dump-on-exit, never a
//! query error). The SinkProbe remainder (stale_locals_dropped /
//! combine_refusals) folds in as router counters + a lane_trace line at RG
//! completion (`sink_probe_complete`).
//!
//! Off-path: with no bench GUC set and engine=legacy (the default), the only
//! router code on any hot path is `arm_dop`'s bench-getter call (replacing
//! the identical direct call the arms made before) and `query_start`'s one
//! TLS read + cmp at executor startup (measured +8 instr/q on select1).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

/// The six runtime arm classes the router dispatches to (§1.1 rows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArmClass {
    /// pgrcolumnar scan→fold/census TaskSets (M1-a + rowdrive).
    Scan = 0,
    /// heap block-range source (M1-b; engages through the scan arm's entry,
    /// classed apart because admission economics and EA policy differ).
    Heap = 1,
    /// hashed-agg sink (M2 + c3 text keys + m35 spill + adopt + top-N).
    Agg = 2,
    /// distinct sink (M2 + m35 spill).
    Distinct = 3,
    /// hash join (M3).
    HashJoin = 4,
    /// sort: bounded top-N + shape-(b) full sort (M3 + m3-sort-b).
    Sort = 5,
    /// NL-inner-index morsel arm (nlindex lane, GL-NLIDX-2 routing half):
    /// plain-Agg over NestLoop(heap SeqScan, btree IndexScan).
    NlIndex = 6,
}

const N_ARMS: usize = 7;

impl ArmClass {
    pub(super) const ALL: [ArmClass; N_ARMS] = [
        ArmClass::Scan,
        ArmClass::Heap,
        ArmClass::Agg,
        ArmClass::Distinct,
        ArmClass::HashJoin,
        ArmClass::Sort,
        ArmClass::NlIndex,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            ArmClass::Scan => "scan",
            ArmClass::Heap => "heap",
            ArmClass::Agg => "agg",
            ArmClass::Distinct => "distinct",
            ArmClass::HashJoin => "hashjoin",
            ArmClass::Sort => "sort",
            ArmClass::NlIndex => "nlindex",
        }
    }
}

// ---------------------------------------------------------------------------
// Engine selection (M5-0)
// ---------------------------------------------------------------------------

/// Is the runtime engine SELECTED AND SERVICEABLE for this session?
/// False in every legacy-default session at the cost of one TLS read + cmp
/// (the off-path discipline: default returns at the first check).
///
/// Degrade rule (§2.2): engine=runtime with no pool (PGRUST_RUNTIME unset,
/// or the global pool not started) behaves exactly as engine=legacy, with a
/// loud-once LOG line so the misconfiguration is visible in the server log
/// without perturbing client output (regress stays byte-identical: LOG is
/// server-log-only at default client_min_messages).
#[inline]
pub(crate) fn engine_runtime_active() -> bool {
    if !::guc_tables::runtime_pool::parallel_engine_is_runtime() {
        return false;
    }
    engine_runtime_pool_ready()
}

/// The pool-availability half of [`engine_runtime_active`], split out so the
/// loud-once emission is testable and off the inline path.
#[cold]
fn engine_runtime_pool_ready() -> bool {
    if runtime::runtime_enabled() && runtime::global().is_some() {
        return true;
    }
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        // Loud-once to stderr — the postmaster/server log, the same channel
        // the runtime lifecycle traces use (elog is deliberately not a lib
        // dependency of execmain). Server-log-only keeps client output — and
        // therefore regress under engine=runtime — byte-identical.
        eprintln!(
            "[m5-router] pgrust.parallel_engine=runtime but the runtime pool is \
             unavailable (pgrust.runtime=off / PGRUST_RUNTIME=0, or the pool did not \
             spawn); degrading to the legacy engine"
        );
    });
    false
}

// ---------------------------------------------------------------------------
// Arming (M5-1): the single DOP source for all six arms
// ---------------------------------------------------------------------------

/// The router's arming decision for one arm class. Bench pool GUC set →
/// verbatim (both engine settings; recipes unchanged). Else engine=runtime
/// with a live pool and the arm not killed → `pgrust.runtime_dop`. Else 0.
///
/// The per-arm kill switches and the lane master gate are honored in BOTH
/// layers: the bench getters embed them (unchanged), and the runtime-engine
/// layer consults the same predicates (`runtime_*_arm_enabled`). The sort
/// arm's `PGRUST_RUNTIME_SORT`/`_FULL` kills live at the arm site and keep
/// gating after the dop read, as before.
/// M5-4 × M5-1 seam: the leader-session affinity token every arm engagement
/// passes to [`runtime::Runtime::submit_pinned_with_affinity`]. INTEGRATION
/// FLAG (m5-integration, deliberate): ceremony-v2's sticky bind has not
/// landed POOL-SIDE — production pool workers never call
/// `WorkerLocal::set_session_token`, and every arm engagement is a PINNED
/// submission that bypasses the stride pick — so there is no token plumbing
/// to consume yet and this returns 0 (the None-equivalent; stride treats it
/// as a plain submit). When the pool-side sticky bind lands, derive the
/// stable ceremony-v2 session key here; every arm submission picks it up
/// through this one chokepoint with no call-site change.
#[inline]
pub(super) fn session_affinity_token() -> u64 {
    0
}

pub(super) fn arm_dop(class: ArmClass) -> i32 {
    use ::guc_tables::runtime_pool as rp;
    let bench = match class {
        ArmClass::Scan | ArmClass::Heap => rp::runtime_scan_pool_dop(),
        ArmClass::Agg => rp::runtime_agg_pool_dop(),
        ArmClass::Distinct => rp::runtime_distinct_pool_dop(),
        ArmClass::HashJoin => rp::runtime_hashjoin_pool_dop(),
        ArmClass::Sort => rp::runtime_sort_pool_dop(),
        ArmClass::NlIndex => rp::runtime_nlindex_pool_dop(),
    };
    if bench > 0 {
        return bench;
    }
    if !engine_runtime_active() {
        return 0;
    }
    let enabled = match class {
        ArmClass::Scan | ArmClass::Heap => rp::runtime_scan_arm_enabled(),
        ArmClass::Agg => rp::runtime_agg_arm_enabled(),
        ArmClass::Distinct => rp::runtime_distinct_arm_enabled(),
        ArmClass::HashJoin => rp::runtime_hashjoin_arm_enabled(),
        ArmClass::Sort => rp::runtime_sort_arm_enabled(),
        ArmClass::NlIndex => rp::runtime_nlindex_arm_enabled(),
    };
    if !enabled {
        return 0;
    }
    rp::runtime_dop()
}

// ---------------------------------------------------------------------------
// Query-start routing decision (the one walk)
// ---------------------------------------------------------------------------

/// Executor-startup routing (standard_executor_start): resolve the engine
/// once per query and count the top-level decision. The serial plan's arm
/// offers then run at their existing sites (the §1.1 admission walks ARE the
/// per-class predicates); `parallel_mode_needed` = the planner generated the
/// legacy Gather machinery, which the legacy engine executes byte-untouched
/// in both engine settings until M5-3's suppression lands.
#[inline]
pub(crate) fn query_start(parallel_mode_needed: bool) {
    if !engine_runtime_active() {
        return;
    }
    query_start_runtime(parallel_mode_needed);
}

#[cold]
fn query_start_runtime(parallel_mode_needed: bool) {
    if parallel_mode_needed {
        tick_global(GlobalCounter::RoutedLegacyParallel);
        super::lane_trace("m5-router: query start — legacy parallel plan (Gather machinery); runtime arms stand down");
    } else {
        tick_global(GlobalCounter::RoutedSerialShaped);
        if super::lane_trace_enabled() {
            super::lane_trace(&format!(
                "m5-router: query start — serial-shaped, engine=runtime dop={}",
                ::guc_tables::runtime_pool::runtime_dop()
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Telemetry: per-arm engagement/refusal/win-loss counters
// ---------------------------------------------------------------------------

/// Per-arm verdict counters, with a fixed algebra the refusal-rate report
/// cross-checks: `Offered` = an armed offer reached the arm's admission walk
/// (dop > 0, runtime on, pool live); `Engaged` = admission passed and the
/// engagement ceremony was entered; `Completed` = the runtime produced the
/// result (the win side); `Fallback` = ceremony entered but the query
/// answered via the serial arm (R5 whole-attempt rerun — the loss side;
/// zero-workers/standing-unavailable ceremonies count here too). So
/// `offered − engaged` ≈ taxonomy refusals and `engaged = completed +
/// fallback`. Refusals tick per (arm, reason) with the arm's own static
/// reason vocabulary.
#[derive(Clone, Copy)]
pub(super) enum ArmCounter {
    Offered = 0,
    Engaged = 1,
    Completed = 2,
    Fallback = 3,
    /// SinkProbe fold (agg/distinct): stale generation-keyed Locals dropped
    /// at combine — nonzero is abort-path evidence, not an error.
    StaleLocalsDropped = 4,
    /// SinkProbe fold (agg/distinct): combine-phase merge refusals.
    CombineRefusals = 5,
}

const N_COUNTERS: usize = 6;

impl ArmCounter {
    fn name(self) -> &'static str {
        match self {
            ArmCounter::Offered => "offered",
            ArmCounter::Engaged => "engaged",
            ArmCounter::Completed => "completed",
            ArmCounter::Fallback => "fallback",
            ArmCounter::StaleLocalsDropped => "stale-locals-dropped",
            ArmCounter::CombineRefusals => "combine-refusals",
        }
    }
    const ALL: [ArmCounter; N_COUNTERS] = [
        ArmCounter::Offered,
        ArmCounter::Engaged,
        ArmCounter::Completed,
        ArmCounter::Fallback,
        ArmCounter::StaleLocalsDropped,
        ArmCounter::CombineRefusals,
    ];
}

/// Query-level routing counters.
#[derive(Clone, Copy)]
enum GlobalCounter {
    RoutedLegacyParallel = 0,
    RoutedSerialShaped = 1,
}
const N_GLOBALS: usize = 2;

static ARM: [[AtomicU64; N_COUNTERS]; N_ARMS] =
    [const { [const { AtomicU64::new(0) }; N_COUNTERS] }; N_ARMS];
static GLOBALS: [AtomicU64; N_GLOBALS] = [const { AtomicU64::new(0) }; N_GLOBALS];

/// (arm, reason) → count. Refusals are per-decision cadence (cold next to
/// row work); a mutexed map keeps the reason vocabulary open — it is the
/// arms' own static strings, which IS the consolidated taxonomy (§1.1: the
/// unified negative space of the six walks).
static REFUSED: Mutex<Vec<((usize, &'static str), u64)>> = Mutex::new(Vec::new());

fn armed() -> bool {
    super::stats::armed()
}

#[inline]
pub(super) fn tick(class: ArmClass, c: ArmCounter) {
    if !armed() {
        return;
    }
    ARM[class as usize][c as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

#[inline]
pub(super) fn tick_n(class: ArmClass, c: ArmCounter, n: u64) {
    if !armed() || n == 0 {
        return;
    }
    ARM[class as usize][c as usize].fetch_add(n, Relaxed);
    arm_dump_on_thread_exit();
}

fn tick_global(c: GlobalCounter) {
    if !armed() {
        return;
    }
    GLOBALS[c as usize].fetch_add(1, Relaxed);
    arm_dump_on_thread_exit();
}

/// Record a refusal in the consolidated taxonomy. `reason` is the arm's own
/// static vocabulary string (the same one lane_trace / the EA transparency
/// line carries) — never formatted at record time.
#[inline]
pub(super) fn tick_refused(class: ArmClass, reason: &'static str) {
    if !armed() {
        return;
    }
    let mut map = match REFUSED.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let key = (class as usize, reason);
    if let Some(e) = map.iter_mut().find(|(k, _)| *k == key) {
        e.1 += 1;
    } else {
        map.push((key, 1));
    }
    drop(map);
    arm_dump_on_thread_exit();
}

/// Fold one completed sink engagement's SinkProbe into the router counters
/// and (trace-armed) a lane_trace line — the SinkProbe observability
/// remainder (§3.5): stale_locals_dropped / combine_refusals had no surface.
pub(super) fn sink_probe_complete(class: ArmClass, probe: &runtime::SinkProbe) {
    let stale = probe.stale_locals_dropped();
    let refusals = probe.combine_refusals();
    tick_n(class, ArmCounter::StaleLocalsDropped, stale);
    tick_n(class, ArmCounter::CombineRefusals, refusals);
    if (stale > 0 || refusals > 0) || super::lane_trace_enabled() {
        super::lane_trace(&format!(
            "m5-router: {} sink-probe stale_locals_dropped={stale} combine_refusals={refusals}",
            class.name()
        ));
    }
}

// ---------------------------------------------------------------------------
// Coverage-view snapshot accessors (lanev2/coverage.rs): relaxed loads,
// enumerated from the ALL tables — the router source of truth, never a
// hand-written list.
// ---------------------------------------------------------------------------

pub(super) const fn n_arm_counter_cells() -> usize {
    N_ARMS * N_COUNTERS
}

/// The query-level routing counters followed by every arm × counter cell
/// (zeros kept): (class, counter, count).
pub(super) fn arm_counter_snapshot() -> Vec<(&'static str, &'static str, u64)> {
    let mut v = Vec::with_capacity(N_GLOBALS + N_ARMS * N_COUNTERS);
    v.push((
        "query",
        "routed-legacy-parallel",
        GLOBALS[GlobalCounter::RoutedLegacyParallel as usize].load(Relaxed),
    ));
    v.push((
        "query",
        "routed-serial-shaped",
        GLOBALS[GlobalCounter::RoutedSerialShaped as usize].load(Relaxed),
    ));
    for class in ArmClass::ALL {
        for c in ArmCounter::ALL {
            v.push((class.name(), c.name(), ARM[class as usize][c as usize].load(Relaxed)));
        }
    }
    v
}

/// Every recorded (arm, reason, count) refusal — the open static-string
/// taxonomy, sorted for stable output.
pub(super) fn refused_snapshot() -> Vec<(&'static str, &'static str, u64)> {
    let map = match REFUSED.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut rows: Vec<(&'static str, &'static str, u64)> = map
        .iter()
        .map(|((class, reason), n)| (ArmClass::ALL[*class].name(), *reason, *n))
        .collect();
    rows.sort();
    rows
}

// ---------------------------------------------------------------------------
// Dump (the refusal-rate report substrate): <dir>/m5-router-stats.<pid>.tsv
// ---------------------------------------------------------------------------

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
    GUARD.with(|_| {});
}

/// Lines:
///   `router\tquery\t<decision>\t<count>`      (query-level routing)
///   `router\t<arm>\t<counter>\t<count>`       (every arm × counter, zeros kept)
///   `router-refused\t<arm>\t<reason>\t<count>` (nonzero only — the taxonomy)
fn dump() {
    let Some(dir) = super::stats::stats_dir() else { return };
    static DUMP_LOCK: Mutex<()> = Mutex::new(());
    let _guard = match DUMP_LOCK.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut out = String::new();
    out.push_str(&format!(
        "router\tquery\trouted-legacy-parallel\t{}\n",
        GLOBALS[GlobalCounter::RoutedLegacyParallel as usize].load(Relaxed)
    ));
    out.push_str(&format!(
        "router\tquery\trouted-serial-shaped\t{}\n",
        GLOBALS[GlobalCounter::RoutedSerialShaped as usize].load(Relaxed)
    ));
    for class in ArmClass::ALL {
        for c in ArmCounter::ALL {
            out.push_str(&format!(
                "router\t{}\t{}\t{}\n",
                class.name(),
                c.name(),
                ARM[class as usize][c as usize].load(Relaxed)
            ));
        }
    }
    {
        let map = match REFUSED.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut rows: Vec<_> = map.iter().collect();
        rows.sort_by_key(|((class, reason), _)| (*class, *reason));
        for ((class, reason), n) in rows {
            out.push_str(&format!(
                "router-refused\t{}\t{}\t{}\n",
                ArmClass::ALL[*class].name(),
                reason,
                n
            ));
        }
    }
    let pid = init_small::globals::process_id();
    let final_path = dir.join(format!("m5-router-stats.{pid}.tsv"));
    let tmp_path = dir.join(format!(
        ".m5-router-stats.{pid}.{:?}.tmp",
        std::thread::current().id()
    ));
    // Best-effort by design: telemetry must never turn into a query error.
    let _ = std::fs::create_dir_all(dir);
    if std::fs::write(&tmp_path, out).is_ok() {
        let _ = std::fs::rename(&tmp_path, &final_path);
    }
}

// ---------------------------------------------------------------------------
// The living coverage matrix is checked against this module
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv is the machine-readable §4.1 matrix that
    /// M5-3's suppression probe consumes. This test pins the contract: every
    /// data row parses, vocabulary values are closed sets, every router arm
    /// class appears at least once, and every runtime-routed row is a
    /// covered row (probe ⊂ walk, risk P1).
    #[test]
    fn coverage_matrix_is_consistent() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../crates/backend/executor/execmain/src/lanev2/m5-coverage.tsv"
        );
        let body = std::fs::read_to_string(path).expect("m5-coverage.tsv readable");
        let mut seen_arms = std::collections::BTreeSet::new();
        let mut rows = 0;
        for line in body.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.split('\t').collect();
            if cols[0] == "class" {
                assert_eq!(
                    cols,
                    [
                        "class",
                        "arm",
                        "spill_eligible",
                        "topn_composable",
                        "adopt_eligible",
                        "status",
                        "route_to",
                        "probe_key",
                        "notes"
                    ],
                    "header drift"
                );
                continue;
            }
            rows += 1;
            assert_eq!(cols.len(), 9, "row width: {line}");
            let (class, arm, spill, topn, adopt, status, route_to, probe_key) =
                (cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7]);
            assert!(!class.is_empty());
            assert!(
                ArmClass::ALL.iter().any(|a| a.name() == arm) || arm == "none",
                "unknown arm {arm} in {class}"
            );
            seen_arms.insert(arm.to_string());
            for q in [spill, topn, adopt] {
                assert!(
                    q == "yes"
                        || q == "no"
                        || q == "na"
                        || q.starts_with("gap:")
                        || q.starts_with("degrade:"),
                    "bad qualifier {q} in {class}"
                );
            }
            assert!(
                matches!(
                    status,
                    "covered" | "covered-partial" | "covered-losing" | "refused" | "not-covered"
                ),
                "bad status {status} in {class}"
            );
            assert!(
                matches!(route_to, "runtime" | "legacy"),
                "bad route_to {route_to} in {class}"
            );
            // Probe ⊂ walk (risk P1): only covered rows may route to runtime.
            if route_to == "runtime" {
                assert_eq!(status, "covered", "{class}: runtime-routed but not covered");
            }
            // probe_key vocabulary (m5-integration reconciliation): "-" =
            // not plan-time keyable, else an m5_suppress CoverClass ident.
            // Keyed rows must belong to a real arm — the probe only keys
            // shapes an arm walk admits (probe ⊂ walk). The key↔route
            // agreement is pinned crate-side by m5_suppress's drift test.
            assert!(
                probe_key == "-"
                    || probe_key.chars().all(|c| c.is_ascii_alphanumeric()),
                "bad probe_key {probe_key} in {class}"
            );
            if probe_key != "-" {
                assert_ne!(arm, "none", "{class}: probe-keyed row without an arm");
                assert_eq!(status, "covered", "{class}: probe-keyed but not covered");
            }
            // §2.4 law 2c pinned: the canonical-bytes text class carries the
            // bytes-spill-record gap until that record lands.
            if class == "agg-text-canonical-bytes" {
                assert_eq!(spill, "gap:bytes-spill-record");
            }
        }
        assert!(rows >= N_ARMS, "matrix suspiciously small ({rows} rows)");
        for a in ArmClass::ALL {
            assert!(
                seen_arms.contains(a.name()),
                "arm {} has no matrix row",
                a.name()
            );
        }
    }
}
