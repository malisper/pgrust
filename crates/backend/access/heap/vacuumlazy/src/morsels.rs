//! Phase-I vacuum morselization — inc-2 of docs/design/vacuum-morsels.md
//! (RATIFIED 2026-07-13, decisions of record 1-5 + the inc-1 claim-boundary
//! quiesce amendment).
//!
//! Shape (doc §3): default ON, `PGRUST_RUNTIME_VACUUM=0` kills (requires
//! `PGRUST_RUNTIME=1`; default OFF) the heap scan of one `heap_vacuum_rel`
//! invocation runs as an RG-PER-ROUND leader loop: each round builds a
//! [`VacuumBlockSource`] skip map (VM skip rules as granule admission, doc
//! §3.1), submits a PINNED resource group with one SCAN task set, launches K
//! full-identity helpers through the vacuumparallel `CreateParallelContext`
//! ceremony (doc §4 — the vacuum bodies WRITE, so phase 1 rides the helper
//! identity wholesale), and PARKS. Helpers claim block-granule morsels off
//! the runtime cursor and drive the UNCHANGED per-block serial bodies
//! (`lazy_scan_new_or_empty` / `lazy_scan_noprune` / `lazy_scan_prune`) into
//! per-worker [`VacScanLocal`]s with order-free folds (doc §3.2). INDEX and
//! REAP stay the ported serial paths this increment: the leader runs
//! `lazy_vacuum` between rounds exactly where C suspends its scan.
//!
//! Correctness walls carried here:
//!  * cutoffs/disposition computed ONCE by the leader in C's load-bearing
//!    order, published immutable in the round work struct; workers derive
//!    their own vistest at bind (>= the leader's horizon — doc §5.1);
//!  * round quiesce at CLAIM boundaries (byte accounting per block, the
//!    inc-1 amendment): the scanned set is exactly a contiguous granule
//!    prefix; overshoot <= K in-flight claims (doc §3.3);
//!  * §5.2 fail-closed coverage guard: any hole in the fold coverage
//!    suppresses relfrozenxid advancement (never folds a wrong minimum);
//!  * workers NEVER take the blocking cleanup-lock wait: the conditional +
//!    noprune ladder runs C-exact, the aggressive residue goes to the
//!    deferred ledger and the LEADER processes it serially between the SCAN
//!    and INDEX phases (doc §5.3);
//!  * failsafe: leader-checked on C's cadence (between rounds + every
//!    FAILSAFE_EVERY_PAGES via the shared scanned counter); a fire quiesces
//!    the round and the leader completes the vacuum SERIALLY with index
//!    vacuuming disabled (doc §5.5);
//!  * progress: workers publish claim completions into a shared frontier;
//!    the parked leader reports the MIN CONTIGUOUS frontier, never past
//!    holes (decision 3 — explicit divergence-with-rule);
//!  * cost pacing: C's parallel vacuum_delay_point discipline verbatim
//!    (shared balance, fair-share nap), helpers-not-pool (decision 2).
//!
//! Fail-closed admission: every refusal — env flag off, no runtime,
//! autovacuum (decision 5), temp rel, unported index AM, PARALLEL 0, no
//! maintenance workers, geometry floor — is today's serial vacuum,
//! byte-identically. Handoffs (quiesce fallback, failsafe, small tail,
//! zero-launch) resume the serial cursor machine at `resume_block`.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::commands_vacuum::{
    set_vacuum_cost_balance_local, set_vacuum_shared_cost, vacuum_delay_point,
    vacuum_xid_failsafe_check, VacuumSharedCost,
};
use ::mcx::MemoryContext;
use ::types_core::{BlockNumber, ForkNumber, OffsetNumber, Oid};
use ::types_error::{PgError, PgResult, ERROR, WARNING};
use ::types_rel::lock::ShareUpdateExclusiveLock;
use ::types_rel::RelationData;
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use ::types_storage::bufpage::PageRef;
use ::types_storage::ReadBufferMode;
use ::vacuum_morsels::{
    merge_dead_runs, resume_granule, verify_prefix_coverage, ClaimRecord, CoverageError,
    QuiesceState, ScanCounters, SkipMapParams, VacScanLocal, VacuumBlockSource, VM_ALL_FROZEN,
    VM_ALL_VISIBLE,
};
use ::visibilitymap::{
    visibilitymap_get_status, visibilitymap_pin, VmBuffer, VISIBILITYMAP_ALL_FROZEN,
    VISIBILITYMAP_ALL_VISIBLE,
};

use ::backend_progress::progress::*;
use ::backend_progress::pgstat_progress_update_param;
use ::bufmgr_seams::{BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};

use super::{
    apply_failsafe, dead_items_add, lazy_check_wraparound_failsafe, lazy_scan_new_or_empty,
    lazy_scan_noprune, lazy_scan_prune, lazy_vacuum, LVRelState, ScanEnv, ScanFolds,
    FAILSAFE_EVERY_PAGES,
};

use init_small::globals as g;

// ---------------------------------------------------------------------------
// Admission (fail-closed; every refusal is today's serial vacuum).
// ---------------------------------------------------------------------------

fn flag_enabled() -> bool {
    // Train-21 default flip (W1 closed per protocol; battery-gated): the
    // morsel arm is ON by default under the runtime master switch;
    // PGRUST_RUNTIME_VACUUM=0 is the kill switch. Layering unchanged:
    // PGRUST_RUNTIME=0 still disarms everything above this flag.
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_VACUUM").is_ok_and(|v| v.trim() == "0")
    })
}

/// M4.1 driver-swap switch (scratchpad/night/all-morsels-plan.md Track 3
/// item 1), FLIPPED-KILL since the train-40 flip (GL-M41-3: wall 1.009
/// parity 2-idx / 0.906 WIN 4-idx, OLTP flat, reclaim 15ms): the POOL
/// channel — PGPROC-leasing runtime pool workers engaged through the M2
/// inc-2 bound-descriptor gate at QoS class Utility — is the driver of
/// record; `pgrust.runtime_vacuum_pool = off` (env seed
/// PGRUST_RUNTIME_VACUUM_POOL=0|off) restores the launched bgworker gang
/// exactly. Fail-closed layering unchanged: pool channel
/// unavailable/refused ⇒ the launched gang, byte-exactly; layered under
/// PGRUST_RUNTIME_VACUUM and PGRUST_RUNTIME, and under the standing
/// module's own switches (PGRUST_RUNTIME_POOLBIND / PGRUST_RUNTIME_POOLDB
/// gate try_engage_pool — until the M2 lane flips POOLDB, default servers
/// keep the launched driver and this switch is polarity-ready).
fn pool_enabled() -> bool {
    guc_tables::backing::pgrust_runtime_vacuum_pool()
}

/// Engagement trace (PGRUST_VACUUM_TRACE=1): the §9 battery's engagement /
/// kill-switch oracle channel. Default-off, zero cost.
pub(crate) fn vtrace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_VACUUM_TRACE").is_ok_and(|v| v.trim() == "1"))
}

pub(crate) fn vtrace(msg: &str) {
    if vtrace_enabled() {
        eprintln!("vacuum-morsels: {msg}");
    }
}

/// §9 coverage-guard fault injection (test-only):
/// PGRUST_RUNTIME_VACUUM_FAULT=lose-local drops one worker Local at SCAN
/// finalize, so the §5.2 guard must fire (advancement suppressed, WARNING,
/// serial completion) — never a wrong fold.
fn fault_lose_local() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_VACUUM_FAULT").is_ok_and(|v| v.trim() == "lose-local")
    })
}

/// Geometry floor in granules (V9: keep small/TOAST vacuums serial; tuned at
/// inc-4). Effective floor = max(this, 2K).
fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_VACUUM_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// Some(K) admits the morsel arm at gang size K. Decision 5: manual VACUUM
/// only; K = max_parallel_maintenance_workers capped by the PARALLEL option
/// (doc §4); PARALLEL 0 disables; unported index AMs refuse fail-closed
/// (only nbtree is wired at this base — doc §3.5); temp relations refuse
/// (session-local buffers — doc §8).
pub(crate) fn admit(vacrel: &LVRelState<'_, '_>, nrequested: i32) -> Option<i32> {
    if !flag_enabled() || !runtime::runtime_enabled() {
        return None;
    }
    runtime::global()?;
    if miscinit::GetMyBackendType() == ::types_core::BackendType::AutovacWorker {
        return None;
    }
    if parallel::IsParallelWorker() || !g::IsUnderPostmaster() {
        return None;
    }
    if vacrel.rel.uses_local_buffers() {
        return None;
    }
    // Port convention (ExecVacuum): nworkers == 0 means NO PARALLEL option
    // (auto), -1 means PARALLEL 0 (parallelism disabled), > 0 is the
    // requested cap. (Inverted from C's raw option value — the port folds
    // the "-1 = not given" sentinel at parse time.)
    if nrequested < 0 {
        return None;
    }
    let mpmw = guc_tables::vars::max_parallel_maintenance_workers.read();
    if mpmw <= 0 {
        return None;
    }
    let k = if nrequested > 0 { mpmw.min(nrequested) } else { mpmw };
    // Fail-closed index-AM gate: the rounds' INDEX phase is the ported
    // serial/vacuumparallel path, wired for nbtree only.
    for ind in vacrel.indrels.iter() {
        if ind.rd_rel.relam != ::types_core::BTREE_AM_OID {
            return None;
        }
    }
    if vacrel.rel_pages == 0 {
        return None;
    }
    Some(k)
}

// ---------------------------------------------------------------------------
// The leader round loop.
// ---------------------------------------------------------------------------

pub(crate) enum ScanHandoff {
    /// Nothing consumed: the serial arm runs from block 0.
    Refused,
    /// Rounds ran; the serial cursor machine resumes at `block`
    /// (== rel_pages when the scan completed) with the FSM-vacuum cursor at
    /// `next_fsm`.
    Resume { block: BlockNumber, next_fsm: BlockNumber },
}

/// Build the round's skip map from `resume_block` (doc §3.1): one pass over
/// the VM fork evaluating C's exact skip rules. Eager decisions vs C's
/// cursor-lazy ones are the recorded §3.1 divergence (same observable
/// class; window bounded by the round).
fn build_source(vacrel: &mut LVRelState<'_, '_>, resume_block: BlockNumber) -> PgResult<VacuumBlockSource> {
    // Bit-layout parity between the pure units and the visibilitymap crate.
    const _: () = assert!(VM_ALL_VISIBLE == VISIBILITYMAP_ALL_VISIBLE);
    const _: () = assert!(VM_ALL_FROZEN == VISIBILITYMAP_ALL_FROZEN);

    let p = SkipMapParams {
        rel_pages: vacrel.rel_pages,
        resume_block,
        aggressive: vacrel.aggressive,
        skipwithvm: vacrel.skipwithvm,
    };
    let mut vmbuf = VmBuffer::new();
    let mut err: Option<Box<PgError>> = None;
    let rel = vacrel.rel;
    let source = VacuumBlockSource::build(&p, |b| {
        if err.is_some() {
            return 0;
        }
        match visibilitymap_get_status(rel, b, &mut vmbuf) {
            Ok(bits) => bits,
            Err(e) => {
                // Record + classify unskippable (never consumed: we fail
                // the build below).
                err = Some(e);
                0
            }
        }
    });
    vmbuf.release();
    match err {
        Some(e) => Err(e),
        None => Ok(source),
    }
}

/// Scan-phase summary trace (inc-3): the DOP-ladder's scan-phase read and
/// the one-gang-many-rounds ceremony-share evidence. `ceremony` = leader-side
/// per-round launch (CreateParallelContext + DSM init + LaunchParallelWorkers)
/// plus teardown (drain + DestroyParallelContext incl. worker-exit wait).
struct ScanPhaseClock {
    t0: std::time::Instant,
    ceremony_ns: u64,
    rounds_ns: u64,
    /// Serial-tail decomposition (the ladder's plateau attribution): skip-map
    /// build, leader finalize (locals fold + coverage + resume), and the
    /// dead-run merge into the round TidStore (risk V2).
    source_ns: u64,
    finalize_ns: u64,
    merge_ns: u64,
}

impl ScanPhaseClock {
    fn new() -> Self {
        ScanPhaseClock {
            t0: std::time::Instant::now(),
            ceremony_ns: 0,
            rounds_ns: 0,
            source_ns: 0,
            finalize_ns: 0,
            merge_ns: 0,
        }
    }

    fn emit(&self, rel: &str, rounds: u64, k: i32, outcome: &str) {
        if rounds == 0 {
            return;
        }
        vtrace(&format!(
            "scan phase done rel={rel} rounds={rounds} k={k} outcome={outcome} \
             t_scan_ms={} t_rounds_ms={} t_ceremony_us={} t_source_ms={} \
             t_finalize_ms={} t_merge_ms={}",
            self.t0.elapsed().as_millis(),
            self.rounds_ns / 1_000_000,
            self.ceremony_ns / 1_000,
            self.source_ns / 1_000_000,
            self.finalize_ns / 1_000_000,
            self.merge_ns / 1_000_000,
        ));
    }
}

pub(crate) fn scan_rounds(vacrel: &mut LVRelState<'_, '_>, k: i32) -> PgResult<ScanHandoff> {
    let rt = runtime::global().expect("admitted with a live runtime");
    ensure_hooks_registered();

    let mut resume_block: BlockNumber = 0;
    let mut next_fsm: BlockNumber = 0;
    let mut rounds: u64 = 0;
    let mut clock = ScanPhaseClock::new();
    let floor = min_granules().max(2 * k.max(1) as u64);

    loop {
        // Failsafe between rounds (C cadence; §5.5). The precheck in
        // heap_vacuum_rel already ran; a fire here (or in a previous round)
        // hands the remainder to the serial arm with index vacuuming off.
        if lazy_check_wraparound_failsafe(vacrel)? {
            clock.emit(vacrel.rel.name(), rounds, k, "failsafe-preround");
            return Ok(ScanHandoff::Resume { block: resume_block, next_fsm });
        }
        if resume_block >= vacrel.rel_pages {
            clock.emit(vacrel.rel.name(), rounds, k, "complete");
            return Ok(ScanHandoff::Resume { block: vacrel.rel_pages, next_fsm });
        }

        let t_source = std::time::Instant::now();
        let source = build_source(vacrel, resume_block)?;
        clock.source_ns += t_source.elapsed().as_nanos() as u64;
        let total = source.entries().len() as u64;
        if total == 0 {
            // Defensive: the last block is never skippable, so an empty map
            // means resume >= rel_pages (handled above).
            vacrel.skippedallvis |= source.skipsallvis();
            clock.emit(vacrel.rel.name(), rounds, k, "complete");
            return Ok(ScanHandoff::Resume { block: vacrel.rel_pages, next_fsm });
        }
        if total < floor {
            // Small (or tail) geometry: the serial arm wins outright (V9).
            clock.emit(vacrel.rel.name(), rounds, k, "small-tail");
            return Ok(if rounds == 0 {
                ScanHandoff::Refused
            } else {
                ScanHandoff::Resume { block: resume_block, next_fsm }
            });
        }

        let source = Arc::new(source);
        let shared = Arc::new(VacScanShared::new(vacrel, rt, Arc::clone(&source)));
        let t_round = std::time::Instant::now();
        match run_round(vacrel, rt, k, &shared, &mut clock)? {
            RoundOutcome::Refused => {
                // Pre-claim refusal (zero launch / all helpers refused):
                // nothing consumed; serial takes over.
                clock.emit(vacrel.rel.name(), rounds, k, "refused");
                return Ok(if rounds == 0 {
                    ScanHandoff::Refused
                } else {
                    ScanHandoff::Resume { block: resume_block, next_fsm }
                });
            }
            RoundOutcome::Completed => {}
        }
        rounds += 1;

        // Helper threads may have EXTENDED the relation's VM and FSM forks
        // during the round. The per-thread smgr fork-size cache is trusted
        // outside recovery (a recorded port divergence — C lseeks every
        // time), so the LEADER's cached sizes would stay stale and every
        // later leader-side read (next round's skip map, the epilogue's
        // visibilitymap_count, FSM range vacuum) would see a truncated
        // fork. Round-7 battery caught it: t_noidx relallvisible 0 with a
        // BYTE-IDENTICAL VM fork on disk. Invalidate; the next read
        // re-probes the real size.
        {
            let rlocator = bufmgr_seams::relation_smgr_locator::call(vacrel.rel);
            smgr_seams::smgr_set_cached_nblocks::call(
                rlocator,
                ForkNumber::VISIBILITYMAP_FORKNUM,
                ::types_core::InvalidBlockNumber,
            )?;
            smgr_seams::smgr_set_cached_nblocks::call(
                rlocator,
                ForkNumber::FSM_FORKNUM,
                ::types_core::InvalidBlockNumber,
            )?;
        }

        // ---- SCAN finalize, leader-side (doc §3.2 + §5.2) ----
        let t_finalize = std::time::Instant::now();
        let mut locals = shared.take_locals();
        if fault_lose_local() && !locals.is_empty() {
            // §9 fault-injection leg: simulate a lost worker Local.
            locals.pop();
        }
        let mut resume_g = resume_granule(&locals, total);
        let mut coverage_failed = false;
        if let Err(e) = verify_prefix_coverage(&locals, resume_g) {
            // §5.2 fail-closed: a lost Local / protocol breach. Suppress
            // relfrozenxid advancement (always-safe), warn loudly, and hand
            // the remainder to the SERIAL arm from the hole (re-scanning any
            // block is idempotent; counter folds above the hole may
            // overcount page visits, which reltuples estimation tolerates
            // and advancement no longer consumes).
            vacrel.coverage_hole = true;
            coverage_failed = true;
            resume_g = match e {
                CoverageError::HoleAt(gr) | CoverageError::OverlapAt(gr) => gr.min(resume_g),
                CoverageError::ScannedBeyondResume { .. } => resume_g,
            };
            elog::ereport(WARNING)
                .errmsg(format!(
                    "vacuum morsel scan coverage guard tripped on \"{}\": {e:?}; relfrozenxid advancement suppressed",
                    vacrel.rel.name()
                ))
                .finish(::types_error::ErrorLocation::new(file!(), line!() as i32, "scan_rounds"))?;
        }
        for l in &locals {
            vacrel.folds.counters.fold(&l.counters);
        }
        // Skip decisions below the resume point are consumed (jumped over
        // for good); later ones are re-decided by the next round's map.
        vacrel.skippedallvis |= source.skipsallvis_before(resume_g);
        clock.finalize_ns += t_finalize.elapsed().as_nanos() as u64;

        // Merge the per-worker dead-TID runs into the round authority store
        // (the ONE serial O(dead TIDs) step per round — doc §3.2).
        let t_merge = std::time::Instant::now();
        {
            let super::LVRelState { dead_items, dead_items_info, .. } = &mut *vacrel;
            for run in merge_dead_runs(&locals) {
                dead_items_add(
                    dead_items.as_mut().expect("dead_items live during scan"),
                    dead_items_info,
                    run.block,
                    &run.offsets,
                )?;
            }
        }
        clock.merge_ns += t_merge.elapsed().as_nanos() as u64;

        // W1 forensics (trace-gated): per-Local claim/run ledger + store
        // totals, so a phantom-index-entry recurrence attributes to a claim
        // (block == granule + resume offset when nothing is skipped; use
        // source.block_of to map exactly). Families discriminated:
        //   A (worker collection miss): store items < lpdead fold, gap maps
        //     to one claim's granule range;
        //   B (snapshot/search miss): store items == lpdead fold and the
        //     "index snapshot" line (lib.rs) reports unsorted or a removed
        //     shortfall.
        if vtrace_enabled() {
            for l in &locals {
                let mut claims = String::new();
                for c in &l.claims {
                    use std::fmt::Write as _;
                    let _ = write!(
                        claims,
                        "{}{}-{}{}",
                        if claims.is_empty() { "" } else { "," },
                        c.start,
                        c.end,
                        if c.scanned { "" } else { "!" }
                    );
                }
                let run_items: usize = l.runs.iter().map(|r| r.offsets.len()).sum();
                vtrace(&format!(
                    "w1 local slot={} claims={} runs={} run_items={} lpdead={} \
                     first_run_blk={} last_run_blk={} claim_ranges=[{}]",
                    l.worker,
                    l.claims.len(),
                    l.runs.len(),
                    run_items,
                    l.counters.lpdead_items,
                    l.runs.first().map(|r| r.block as i64).unwrap_or(-1),
                    l.runs.last().map(|r| r.block as i64).unwrap_or(-1),
                    claims,
                ));
            }
            vtrace(&format!(
                "w1 merge done locals={} store_items={} lpdead_fold={} shared_num_dead={}",
                locals.len(),
                vacrel.dead_items_info.num_items,
                vacrel.folds.counters.lpdead_items,
                shared.num_dead_items.load(Ordering::Relaxed),
            ));
        }

        // Deferred blocking-cleanup pages (§5.3): leader-serial, BEFORE the
        // round's INDEX phase, so the round's dead set is complete.
        let mut deferred: Vec<BlockNumber> =
            locals.iter().flat_map(|l| l.deferred_cleanup.iter().copied()).collect();
        deferred.sort_unstable();
        for blk in deferred {
            leader_deferred_block(vacrel, &source, blk)?;
        }

        // Exact round frontier (progress rule: min contiguous, final exact).
        if resume_g > 0 {
            pgstat_progress_update_param(
                PROGRESS_VACUUM_HEAP_BLKS_SCANNED,
                source.block_of(resume_g - 1).block as i64,
            );
        }

        let complete = resume_g >= total;
        resume_block =
            if complete { vacrel.rel_pages } else { source.block_of(resume_g).block };
        clock.rounds_ns += t_round.elapsed().as_nanos() as u64;
        vtrace(&format!(
            "round done rel={} granules={total} resume_g={resume_g} resume_block={resume_block} \
             tripped={} failsafe={} coverage_failed={coverage_failed} quiesce_bytes={} \
             t_round_ms={}",
            vacrel.rel.name(),
            shared.quiesce.tripped(),
            shared.failsafe.load(Ordering::Relaxed),
            shared.quiesce.bytes(),
            t_round.elapsed().as_millis(),
        ));

        if coverage_failed {
            // Fail-closed: never loop the morsel arm over unverifiable
            // coverage — the serial arm finishes from the hole.
            clock.emit(vacrel.rel.name(), rounds, k, "coverage-hole");
            return Ok(ScanHandoff::Resume { block: resume_block, next_fsm });
        }

        if shared.failsafe.load(Ordering::Acquire) {
            // The leader observed the fire in its park loop; the round has
            // quiesced. Apply C's failsafe body (once) and complete the
            // remaining vacuum serially, heap-only (doc §5.5).
            if !::commands_vacuum::VacuumFailsafeActive() {
                apply_failsafe(vacrel)?;
            }
            clock.emit(vacrel.rel.name(), rounds, k, "failsafe");
            return Ok(ScanHandoff::Resume { block: resume_block, next_fsm });
        }
        if complete {
            // Whole remaining scan done: the final lazy_vacuum runs in the
            // caller's epilogue, C-exactly (bypass arithmetic included).
            clock.emit(vacrel.rel.name(), rounds, k, "complete");
            return Ok(ScanHandoff::Resume { block: vacrel.rel_pages, next_fsm });
        }

        // Quiesce trip: C's suspension body (vacuumlazy.c:1269-1308).
        vacrel.consider_bypass_optimization = false;
        if vacrel.dead_items_info.num_items > 0 {
            lazy_vacuum(vacrel)?;
            freespace::FreeSpaceMapVacuumRange(vacrel.rel, next_fsm, resume_block)?;
            next_fsm = resume_block;
            pgstat_progress_update_param(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_SCAN_HEAP);
        }
    }
}

/// One deferred page (§5.3): the serial per-block ladder, blocking cleanup
/// wait ALLOWED (the leader's own PGPROC, C's own semantics and rarity).
/// The worker already counted the page in scanned_pages; everything else
/// about the page was left untouched.
fn leader_deferred_block(
    vacrel: &mut LVRelState<'_, '_>,
    source: &VacuumBlockSource,
    blkno: BlockNumber,
) -> PgResult<()> {
    let entries = source.entries();
    let av = entries
        .binary_search_by_key(&blkno, |e| e.block)
        .map(|i| entries[i].all_visible_according_to_vm)
        .unwrap_or(false);

    vacuum_delay_point(false)?;

    let buf = bufmgr_seams::read_buffer_extended::call(
        vacrel.rel,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::Normal,
        vacrel.bstrategy.clone(),
    )?;
    let mut vmbuffer = VmBuffer::new();
    visibilitymap_pin(vacrel.rel, blkno, &mut vmbuffer)?;

    let mut got_cleanup_lock = bufmgr_seams::conditional_lock_buffer_for_cleanup::call(buf)?;
    if !got_cleanup_lock {
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
    }
    // SAFETY: buffer pinned + at least share-locked above.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };

    let super::LVRelState {
        rel,
        cutoffs,
        vistest,
        aggressive,
        nindexes,
        do_index_vacuuming,
        folds,
        dead_items,
        dead_items_info,
        ..
    } = &mut *vacrel;
    let env = ScanEnv {
        rel: *rel,
        cutoffs: &*cutoffs,
        vistest: *vistest,
        aggressive: *aggressive,
        nindexes: *nindexes,
    };
    let mut sink = |blkno: BlockNumber, offsets: &[OffsetNumber]| {
        dead_items_add(dead_items.as_mut().unwrap(), dead_items_info, blkno, offsets)
    };

    if lazy_scan_new_or_empty(&env, folds, buf, blkno, page, !got_cleanup_lock, &vmbuffer)? {
        vmbuffer.release();
        return Ok(());
    }

    let mut has_lpdead_items = false;
    if !got_cleanup_lock
        && !lazy_scan_noprune(&env, folds, &mut sink, buf, blkno, page, &mut has_lpdead_items)?
    {
        debug_assert!(env.aggressive);
        // The blocking wait workers must never take (BM_PIN_COUNT_WAITER +
        // ProcWaitForSignal — single waiter, pgprocno-keyed): leader-only.
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::lock_buffer_for_cleanup::call(buf)?;
        got_cleanup_lock = true;
    }

    if got_cleanup_lock {
        lazy_scan_prune(
            &env,
            folds,
            &mut sink,
            buf,
            blkno,
            page,
            &mut vmbuffer,
            av,
            &mut has_lpdead_items,
        )?;
    }

    if env.nindexes == 0 || !*do_index_vacuuming || !has_lpdead_items {
        let freespace = page.heap_free_space();
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        freespace::RecordPageWithFreeSpace(env.rel, blkno, freespace)?;
    } else {
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
    }
    vmbuffer.release();
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared round state: the parallel context's private payload AND the SCAN
// task set's work body (one struct, one Arc — the runtime_scan shape).
// ---------------------------------------------------------------------------

/// Contiguous completed-claim frontier (decision 3): workers merge completed
/// claim ranges; `prefix` is the granule count of the contiguous prefix.
struct Frontier {
    done: Mutex<std::collections::BTreeMap<u64, u64>>,
    prefix: AtomicU64,
}

impl Frontier {
    fn new() -> Self {
        Frontier { done: Mutex::new(std::collections::BTreeMap::new()), prefix: AtomicU64::new(0) }
    }

    fn complete(&self, start: u64, end: u64) {
        let mut g = self.done.lock().unwrap_or_else(|p| p.into_inner());
        g.insert(start, end);
        let mut prefix = self.prefix.load(Ordering::Relaxed);
        while let Some(&e) = g.get(&prefix) {
            g.remove(&prefix);
            prefix = e;
        }
        self.prefix.store(prefix, Ordering::Relaxed);
    }
}

pub(crate) struct VacScanShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    /// M4.1 pool channel: the engagement's binder target (shared_for(pcxt),
    /// set once per round after InitializeParallelDSM) — the pool driver
    /// binds eagerly through it (with_query_task_binding).
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    /// M4.1 pool channel: the board-entry slot, held across the pool wait
    /// so the PRIVATE_SHUTDOWN hook can complete the standing join (close +
    /// await detach) on leader unwinds that never reach the wait loop's own
    /// cleanup (the arm-side shutdown_standing_join law).
    pool_board: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    source: Arc<VacuumBlockSource>,
    relid: Oid,
    /// Cutoffs computed once by the leader in C's load-bearing order
    /// (OldestXmin before rel_pages/vistest capture) — generation-owned,
    /// immutable (doc §5.1).
    cutoffs: ::tableam_vocab::VacuumCutoffs,
    aggressive: bool,
    nindexes: usize,
    do_index_vacuuming: bool,
    ring_nbuffers: i32,
    /// C's shared cost-balance discipline, verbatim (decision 2 / doc §7).
    cost: Arc<VacuumSharedCost>,
    /// §3.3 round quiesce (claim-boundary trip; byte accounting per block).
    quiesce: QuiesceState,
    /// §5.5 failsafe latch: set by the parked leader on C's cadence;
    /// observed by claims exactly like the quiesce trip, and stops worker
    /// cost delays immediately.
    failsafe: AtomicBool,
    /// Shared scanned-pages cadence counter (leader failsafe checks),
    /// seeded with the vacuum's running total.
    scanned_pages: AtomicU64,
    /// Dead-TID progress feed (leader publishes; workers count exact).
    num_dead_items: AtomicI64,
    frontier: Frontier,
    /// Pin-board base (= runtime nthreads): run_morsel's worker index minus
    /// this is the external-lane ordinal = the Locals slot.
    pins_base: usize,
    refused: AtomicUsize,
    started: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    locals: Vec<Mutex<Option<VacScanLocal>>>,
}

impl VacScanShared {
    fn new(
        vacrel: &LVRelState<'_, '_>,
        rt: &'static Arc<runtime::Runtime>,
        source: Arc<VacuumBlockSource>,
    ) -> VacScanShared {
        VacScanShared {
            rt,
            rg: OnceLock::new(),
            pcxt_shared: OnceLock::new(),
            pool_board: Mutex::new(None),
            source,
            relid: vacrel.rel.rd_id,
            cutoffs: vacrel.cutoffs,
            aggressive: vacrel.aggressive,
            nindexes: vacrel.nindexes,
            do_index_vacuuming: vacrel.do_index_vacuuming,
            ring_nbuffers: bufmgr_seams::get_access_strategy_buffer_count::call(&vacrel.bstrategy),
            cost: Arc::new(VacuumSharedCost {
                cost_balance: std::sync::atomic::AtomicU32::new(0),
                active_nworkers: std::sync::atomic::AtomicU32::new(0),
            }),
            quiesce: QuiesceState::new(vacrel.dead_items_info.max_bytes as u64),
            failsafe: AtomicBool::new(false),
            scanned_pages: AtomicU64::new(vacrel.folds.counters.scanned_pages),
            num_dead_items: AtomicI64::new(0),
            frontier: Frontier::new(),
            pins_base: rt.nthreads(),
            refused: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            error: Mutex::new(None),
            failed: AtomicBool::new(false),
            locals: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        }
    }

    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn take_locals(&self) -> Vec<VacScanLocal> {
        self.locals
            .iter()
            .filter_map(|m| m.lock().unwrap_or_else(|p| p.into_inner()).take())
            .collect()
    }
}

/// Approximate TidStore cost of one block's dead-TID run — the §3.2/§6 byte
/// estimator feeding the shared quiesce budget (the C accounting target is
/// TidStoreMemoryUsage of the merged store; overshoot only shifts round
/// boundaries, bounded by K in-flight claims, and the record reports it).
fn dead_run_bytes(noffsets: usize) -> u64 {
    64 + 2 * noffsets as u64
}

// ---------------------------------------------------------------------------
// Worker side: TaskSetWork + the entry-task drive (full-identity helper).
// ---------------------------------------------------------------------------

/// Per-helper scan context: the helper's OWN opened relation, buffer
/// strategy, vistest, VM buffer, and per-claim fold staging. Lives on the
/// entry-task frame around drive_pinned; run_morsel reaches it through the
/// thread-local pointer below (this thread is the only driver of its lane).
struct VacWorkerCx<'a, 'mcx> {
    rel: &'a RelationData<'mcx>,
    vistest: ::types_core::GlobalVisStateHandle,
    bstrategy: BufferAccessStrategy,
    vmbuffer: VmBuffer,
    /// Per-CLAIM folds, reseeded per claim and folded into the slot Local at
    /// the claim boundary (ScanCounters::fold — the gated inc-1 unit).
    folds: ScanFolds,
}

thread_local! {
    static WORKER_CX: std::cell::Cell<*mut VacWorkerCx<'static, 'static>> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

impl runtime::TaskSetWork for VacScanShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            // Already aborting: the claim drains without work.
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "vacuum scan worker panicked in a morsel").into())
            }
        }
    }

    fn finalize(&self) {
        // Locals live in the shared slots (installed per claim); the LEADER
        // folds/merges after completion — the merge target (the round
        // TidStore) is leader-arena state.
    }
}

impl VacScanShared {
    fn morsel_body(&self, worker: usize, range: runtime::MorselRange) -> PgResult<()> {
        let p = WORKER_CX.with(|c| c.get());
        if p.is_null() {
            return Err(PgError::new(ERROR, "vacuum scan morsel without a bound worker").into());
        }
        // SAFETY: set by THIS thread's entry frame around drive_pinned; the
        // frame outlives the drive, and run_morsel only executes on the
        // claiming thread.
        let wcx: &mut VacWorkerCx<'_, '_> = unsafe { &mut *p };

        let slot = worker - self.pins_base;
        let mut guard = self.locals[slot].lock().unwrap_or_else(|p| p.into_inner());
        let local = guard.get_or_insert_with(|| {
            VacScanLocal::new(slot, self.cutoffs.OldestXmin, self.cutoffs.OldestMxact)
        });

        // Claim-boundary quiesce observation (§3.3, the inc-1 amendment):
        // claims entered after the trip (or a failsafe fire) are no-ops.
        if self.quiesce.tripped() || self.failsafe.load(Ordering::Acquire) {
            local.claims.push(ClaimRecord { start: range.start, end: range.end, scanned: false });
            return Ok(());
        }

        wcx.folds.counters = ScanCounters::seed(self.cutoffs.OldestXmin, self.cutoffs.OldestMxact);
        for gidx in range.clone() {
            let sb = self.source.block_of(gidx);
            self.scan_block_worker(wcx, local, sb.block, sb.all_visible_according_to_vm)?;
        }
        local.counters.fold(&wcx.folds.counters);
        local.claims.push(ClaimRecord { start: range.start, end: range.end, scanned: true });
        self.scanned_pages.fetch_add(range.end - range.start, Ordering::Relaxed);
        self.frontier.complete(range.start, range.end);
        Ok(())
    }

    /// The serial loop's per-block body, worker form: same ladder, same
    /// per-block functions, folds into the claim staging; the blocking
    /// cleanup wait is replaced by the deferred ledger (§5.3). Buffer
    /// disposition is tracked so an error can never leak a content lock a
    /// sibling worker could be waiting on (FSM/VM pages are shared).
    fn scan_block_worker(
        &self,
        wcx: &mut VacWorkerCx<'_, '_>,
        local: &mut VacScanLocal,
        blkno: BlockNumber,
        all_visible_according_to_vm: bool,
    ) -> PgResult<()> {
        // Cost delay per block, C's parallel discipline (doc §7); failsafe
        // stops delays immediately (§5.5) but interrupts stay checked.
        if self.failsafe.load(Ordering::Relaxed) {
            postgres_seams::check_for_interrupts::call()?;
        } else {
            vacuum_delay_point(false)?;
        }

        let buf = bufmgr_seams::read_buffer_extended::call(
            wcx.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            wcx.bstrategy.clone(),
        )?;
        wcx.folds.counters.scanned_pages += 1;

        // Buffer disposition: 0 = pinned only, 1 = pinned+locked,
        // 2 = released (the per-block bodies release on their true paths).
        let mut buf_state = 0u8;
        let r = (|| -> PgResult<()> {
            visibilitymap_pin(wcx.rel, blkno, &mut wcx.vmbuffer)?;

            let got_cleanup_lock =
                bufmgr_seams::conditional_lock_buffer_for_cleanup::call(buf)?;
            if !got_cleanup_lock {
                bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
            }
            buf_state = 1;

            // SAFETY: buffer pinned + at least share-locked above.
            let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };

            let env = ScanEnv {
                rel: wcx.rel,
                cutoffs: &self.cutoffs,
                vistest: wcx.vistest,
                aggressive: self.aggressive,
                nindexes: self.nindexes,
            };
            let quiesce = &self.quiesce;
            let num_dead = &self.num_dead_items;
            let runs = &mut local.runs;
            let mut sink = |blk: BlockNumber, offsets: &[OffsetNumber]| -> PgResult<()> {
                debug_assert!(runs.last().is_none_or(|r| r.block < blk));
                runs.push(::vacuum_morsels::DeadRun { block: blk, offsets: offsets.to_vec() });
                num_dead.fetch_add(offsets.len() as i64, Ordering::Relaxed);
                // Byte accounting per block (C cadence); trip observed at
                // the NEXT claim boundary.
                quiesce.add_bytes(dead_run_bytes(offsets.len()));
                Ok(())
            };

            if lazy_scan_new_or_empty(
                &env,
                &mut wcx.folds,
                buf,
                blkno,
                page,
                !got_cleanup_lock,
                &wcx.vmbuffer,
            )? {
                buf_state = 2; // released inside on the true paths
                return Ok(());
            }

            let mut has_lpdead_items = false;
            if !got_cleanup_lock {
                if !lazy_scan_noprune(
                    &env,
                    &mut wcx.folds,
                    &mut sink,
                    buf,
                    blkno,
                    page,
                    &mut has_lpdead_items,
                )? {
                    // Aggressive-must-freeze residue: workers NEVER block on
                    // the cleanup lock — defer to the leader (§5.3).
                    debug_assert!(env.aggressive);
                    local.deferred_cleanup.push(blkno);
                    bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                    buf_state = 0;
                    bufmgr_seams::release_buffer::call(buf)?;
                    buf_state = 2;
                    return Ok(());
                }
            } else {
                lazy_scan_prune(
                    &env,
                    &mut wcx.folds,
                    &mut sink,
                    buf,
                    blkno,
                    page,
                    &mut wcx.vmbuffer,
                    all_visible_according_to_vm,
                    &mut has_lpdead_items,
                )?;
            }

            if env.nindexes == 0 || !self.do_index_vacuuming || !has_lpdead_items {
                let freespace = page.heap_free_space();
                bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                buf_state = 0;
                bufmgr_seams::release_buffer::call(buf)?;
                buf_state = 2;
                // NOTE: the serial one-pass FSM cadence (VACUUM_FSM_EVERY_
                // PAGES FreeSpaceMapVacuumRange) is a linear-cursor concept;
                // upper-level FSM propagation for morsel rounds happens at
                // round end / epilogue over the full span (same content,
                // deferred — doc §3/§5.4).
                freespace::RecordPageWithFreeSpace(env.rel, blkno, freespace)?;
            } else {
                bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                buf_state = 0;
                bufmgr_seams::release_buffer::call(buf)?;
                buf_state = 2;
            }
            Ok(())
        })();

        if r.is_err() {
            // Best-effort buffer recovery so no sibling wedges on a leaked
            // content lock; the worker transaction abort (entry task Err)
            // releases everything else via resowner.
            if buf_state == 1 {
                let _ = bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK);
            }
            if buf_state <= 1 {
                let _ = bufmgr_seams::release_buffer::call(buf);
            }
        }
        r
    }
}

/// The launched entry task (vacuumparallel_main-shaped identity ceremony +
/// the pinned drive): the substrate has already connected the helper to the
/// leader's database, restored leader state, and entered parallel mode.
fn vacuum_scan_worker_main(pshared: &parallel::ParallelShared) -> PgResult<()> {
    let Some(private) = pshared.private() else { return Ok(()) };
    let Ok(shared) = private.downcast::<VacScanShared>() else { return Ok(()) };

    let r = catch_unwind(AssertUnwindSafe(|| worker_drive(&shared)));
    let outcome = match r {
        Ok(o) => o,
        Err(unwind) => {
            shared.fail(PgError::new(ERROR, "vacuum scan helper panicked").into());
            if parallel::standing::is_exit_unwind(&*unwind) {
                latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                    pshared.parallel_leader_proc_number,
                ));
                std::panic::resume_unwind(unwind);
            }
            Err(Box::new(PgError::new(ERROR, "vacuum scan worker failed (see leader error)")))
        }
    };
    // Wake the parked leader: completion/refusal/error all re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        pshared.parallel_leader_proc_number,
    ));
    outcome
}

fn worker_drive(shared: &Arc<VacScanShared>) -> PgResult<()> {
    let Some(rg) = shared.rg.get().and_then(|w| w.upgrade()) else {
        shared.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    // Process-wide external-lane lease: exhaustion = fail-closed
    // non-participation (the leader's all-refused probe is the fallback).
    let Some(lane) = shared.rt.acquire_external_lane() else {
        shared.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    };
    let mut lane_local = lane.local();

    // Identity + cost ceremony (parallel_vacuum_main donor, verbatim class).
    let ctx = MemoryContext::new("vacuum morsel scan worker");
    let mcx = ctx.mcx();
    let rel = match table::table_open(mcx, shared.relid, ShareUpdateExclusiveLock) {
        Ok(rel) => rel,
        Err(e) => {
            // Clean pre-drive failure: record it and reap the RG (a pinned
            // RG needs SOME driver to run its protocol cleanup).
            shared.fail(e);
            if rg.try_outcome().is_none() {
                rg.abort();
                let _ = shared.rt.drive_pinned(&mut lane_local, &rg);
            }
            return Ok(());
        }
    };

    autovacuum_seams::vacuum_update_costs::call()?;
    g::SetVacuumCostBalance(0);
    set_vacuum_cost_balance_local(0);
    set_vacuum_shared_cost(Some(Arc::clone(&shared.cost)));
    shared.cost.active_nworkers.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let bstrategy = bufmgr_seams::get_access_strategy_with_size::call(
        BufferAccessStrategyType::BasVacuum,
        shared.ring_nbuffers * (::types_core::BLCKSZ as i32 / 1024),
    );
    // Worker-derived vistest, AFTER the leader's OldestXmin capture — every
    // removal decision within C's envelope (doc §5.1). GlobalVisTestFor
    // returns a handle to THREAD-LOCAL cached horizons, which on a reused
    // helper thread are as old as that thread's last refresh — the round-6
    // battery caught stale worker horizons classifying freshly-deleted
    // tuples RECENTLY_DEAD (unremoved; all_visible false; one-pass VM bits
    // never set). Refresh the horizons ON THIS THREAD first, exactly the
    // computation the leader's own capture ran (GetOldestNonRemovable ->
    // ComputeXidHorizons), so the derived state is >= the leader's.
    let _ = procarray_seams::get_oldest_non_removable_transaction_id::call(&rel)?;
    let vistest = procarray_seams::global_vis_test_for::call(&rel);

    let mut wcx = VacWorkerCx {
        rel: &rel,
        vistest,
        bstrategy,
        vmbuffer: VmBuffer::new(),
        folds: ScanFolds {
            counters: ScanCounters::seed(shared.cutoffs.OldestXmin, shared.cutoffs.OldestMxact),
            offnum: ::types_tuple::InvalidOffsetNumber,
        },
    };
    shared.started.fetch_add(1, Ordering::SeqCst);

    // Publish the worker cx for run_morsel (this thread only), drive, clear.
    // SAFETY (lifetime erasure): wcx outlives the drive on this frame; the
    // pointer is cleared before wcx drops.
    WORKER_CX.with(|c| {
        c.set(unsafe {
            core::mem::transmute::<*mut VacWorkerCx<'_, '_>, *mut VacWorkerCx<'static, 'static>>(
                &mut wcx as *mut VacWorkerCx<'_, '_>,
            )
        })
    });
    let _outcome = shared.rt.drive_pinned(&mut lane_local, &rg);
    WORKER_CX.with(|c| c.set(std::ptr::null_mut()));
    emit_wfin(lane.ordinal(), &lane_local, &rg);

    // Teardown (parallel_vacuum_main order).
    wcx.vmbuffer.release();
    shared.cost.active_nworkers.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    set_vacuum_shared_cost(None);
    let strategy = core::mem::take(&mut wcx.bstrategy);
    if strategy.is_some() {
        bufmgr_seams::free_access_strategy::call(strategy);
    }
    drop(wcx);
    table::table_close(rel, ShareUpdateExclusiveLock)?;

    if shared.failed.load(Ordering::SeqCst) {
        // A recorded error (possibly this worker's, possibly a sibling's
        // observed via shared state): abort the worker transaction so
        // resowner releases any residue (the leader rethrows the recorded
        // error; this message is never user-visible on that path).
        return Err(Box::new(PgError::new(
            ERROR,
            "vacuum scan worker failed (see leader error)",
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// M4.1 pool channel (driver swap): the round's helpers as PGPROC-leasing
// pool workers, engaged through the M2 inc-2 bound-descriptor gate.
// ---------------------------------------------------------------------------

/// The bound descriptor's serve: parallel::standing::pool_serve mapped onto
/// the runtime's verdict (the parallel crate cannot name runtime types —
/// the standing_channel adapter precedent, replicated here because the
/// vacuum driver lives outside execmain).
fn vacuum_pooldb_serve(
    payload: &Arc<dyn std::any::Any + Send + Sync>,
) -> runtime::BoundServe {
    match parallel::standing::pool_serve(payload) {
        parallel::standing::PoolServe::Served => runtime::BoundServe::Served,
        parallel::standing::PoolServe::Refused => runtime::BoundServe::Refused,
        parallel::standing::PoolServe::Closed => runtime::BoundServe::Closed,
    }
}

/// The standing/pool driver (StandingDriver.drive): runs ON a pool worker
/// inside serve_ticket — leased PGPROC identity live, parallel-worker
/// impersonation + lock-group membership installed. Owns the EAGER binder
/// wrap around the launched path's exact drive body (`worker_drive`): the
/// binder's statement half supplies what the launched substrate supplies a
/// bgworker (worker transaction, snapshots, identity, parallel mode), so
/// the vacuum bodies run identically on both channels. Exit-committed
/// unwinds (FATAL) are rethrown to the pool glue — a terminated worker
/// must die (swallowing one would resurrect it into the pool).
fn vacuum_scan_pool_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<VacScanShared>() else { return };
    let Some(target) = payload.pcxt_shared.get() else {
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let target = Arc::clone(target);
    // The submit → handle-store window: the publication wake can beat the
    // leader's rg-handle store; the board is live, so wait briefly (a
    // never-stored handle degrades to worker_drive's fail-closed refusal).
    let mut spins = 0u32;
    while payload.rg.get().is_none() && spins < 10_000 {
        std::thread::yield_now();
        spins += 1;
    }
    let entered = std::cell::Cell::new(false);
    let r = catch_unwind(AssertUnwindSafe(|| {
        parallel::with_query_task_binding(&target, || {
            entered.set(true);
            worker_drive(&payload)
        })
    }));
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            if !entered.get() {
                // Binder refusal (validate() gate): fail-closed
                // non-participation — the leader's nobody-participates
                // probe falls back to the launched gang.
                vtrace(&format!("pool bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            } else if !payload.failed.load(Ordering::SeqCst) {
                // An error escaped worker_drive without a recorded fail
                // (worker_drive records its own on every failure path;
                // this is the binder's own teardown surface).
                payload.fail(e);
            }
        }
        Err(unwind) => {
            payload.fail(
                PgError::new(ERROR, "vacuum scan pool executor panicked").into(),
            );
            latch::SetLatch(::types_storage::latch::LatchHandle::proc(
                shared.parallel_leader_proc_number,
            ));
            if parallel::standing::is_exit_unwind(&*unwind) {
                std::panic::resume_unwind(unwind);
            }
            return;
        }
    }
    // Wake the parked leader: completion/refusal/error all re-poll there.
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// Build the pool channel for one round: install the binder target +
/// standing driver on the fresh round pcxt, then the per-RG engagement
/// board + bound descriptor (returned pair rides the submission). None =
/// channel unavailable (kill switches, binder policy refusal, no pool
/// identity wiring, lock-group failure) — submit plain-pinned; the
/// launched gang is the driver, today exactly.
fn try_pool_channel(
    pcxt: parallel::ParallelContextId,
    shared: &Arc<VacScanShared>,
    k: i32,
) -> Option<(Arc<parallel::standing::StandingEngagement>, runtime::BoundDescriptor)> {
    if !pool_enabled() {
        return None;
    }
    // Binder-policy fail-closed admission (the M1 scan leader's law): any
    // set flag would refuse validate() on every serve anyway — don't build
    // a board nobody can serve. Vacuum never carries params.
    let policy = parallel::query_task_policy_probe();
    if policy.temp_state || policy.serializable || policy.pending_invalidations {
        vtrace("pool channel refused (binder policy probe)");
        return None;
    }
    if parallel::InstallQueryTaskBinding(pcxt, policy).is_err() {
        // Installed-twice is unreachable on a fresh round pcxt; refuse
        // fail-closed rather than unwind the round.
        vtrace("pool channel refused (binding install)");
        return None;
    }
    parallel::set_standing_driver(
        pcxt,
        parallel::standing::StandingDriver {
            drive: vacuum_scan_pool_driver,
            deferred_bind: false,
        },
    );
    let target = shared.pcxt_shared.get()?;
    let entry = parallel::standing::try_engage_pool(target, k.max(0) as usize)?;
    let payload: Arc<dyn std::any::Any + Send + Sync> = Arc::clone(&entry) as _;
    Some((entry, runtime::BoundDescriptor {
        serve: vacuum_pooldb_serve,
        payload,
        // POOL-QOS: utility engagements never charge interactive demand
        // (their RGs run at the utility stride weight, below the
        // interactive threshold once seeded); width 0 keeps them out of
        // the demand ledger entirely.
        width: 0,
    }))
}

fn wfin_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_WFIN").is_ok_and(|v| v.trim() == "1"))
}

/// WFIN marker (the m0-accept harness channel; §9 spread gate reads these).
fn emit_wfin(ordinal: usize, local: &runtime::WorkerLocal, rg: &runtime::RgHandle) {
    if !wfin_enabled() {
        return;
    }
    let d = local.drive;
    let task_avg_us = if d.tasks > 0 { d.busy_ns / d.tasks / 1000 } else { 0 };
    eprintln!(
        "MORSEL|WFIN|qid={}|pipe=0|worker={}|t_us={}|tasks={}|task_avg_us={}|first_us={}|busy_us={}|morsels={}|granules={}|chan=vacscan",
        rg.query_id(),
        ordinal,
        d.last_end_ns / 1000,
        d.tasks,
        task_avg_us,
        d.first_claim_ns / 1000,
        d.busy_ns / 1000,
        d.morsels,
        d.granules,
    );
}

// ---------------------------------------------------------------------------
// The round ceremony: CreateParallelContext + submit_pinned + park.
// ---------------------------------------------------------------------------

/// PRIVATE_SHUTDOWN hook (global): DestroyParallelContext — including the
/// transaction-abort path on leader unwinds — calls this BEFORE waiting for
/// worker exit. Abort + drain the RG so every helper's drive observes
/// completion and can exit (idempotent on completed RGs).
fn vacuum_scan_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<VacScanShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        if rg.try_outcome().is_none() {
            drain_rg(payload.rt, &rg);
        }
    }
    // M4.1 pool channel: a leader unwind between submit and the pool wait's
    // own cleanup leaves the board entry live — complete the standing join
    // (close + await detach) so pool participants never outlive the leader
    // arena (the arm-side shutdown_standing_join law).
    let board = payload.pool_board.lock().unwrap_or_else(|p| p.into_inner()).take();
    if let Some(entry) = board {
        parallel::standing::close_and_await(&entry);
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_vacuum_scan_main",
            vacuum_scan_worker_main,
        );
        parallel::register_parallel_private_shutdown(vacuum_scan_private_shutdown);
    });
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive (the
/// runtime_scan drain shape). False = leaked (dead participant) — callers
/// surface an error rather than wait forever.
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
    let Some(lane) = lane else { return false };
    let mut local = lane.local();
    rt.try_drain_pinned(&mut local, rg, 4000).is_some()
}

enum RoundOutcome {
    /// Pre-claim refusal: nothing consumed; serial takes over.
    Refused,
    /// The RG completed; Locals are ready to fold.
    Completed,
}

/// The parked leader's per-poll duty, shared by BOTH wait loops (launched
/// gang and M4.1 pool channel): progress publication (decision 3 — the min
/// contiguous claim frontier, never past holes; dead-TID counters from the
/// shared feed) and the failsafe cadence (§5.5 — the shared scanned counter
/// crossing FAILSAFE_EVERY_PAGES multiples, checked on the LEADER: its own
/// PGPROC, C's every-4GB rule).
struct LeaderCadence {
    last_failsafe_page: u64,
    progress_base_items: i64,
    progress_base_bytes: u64,
}

impl LeaderCadence {
    fn new(vacrel: &LVRelState<'_, '_>, shared: &VacScanShared) -> LeaderCadence {
        LeaderCadence {
            last_failsafe_page: shared.scanned_pages.load(Ordering::Relaxed),
            progress_base_items: vacrel.dead_items_info.num_items,
            progress_base_bytes: vacrel
                .dead_items
                .as_ref()
                .map(|d| d.memory_usage())
                .unwrap_or(0) as u64,
        }
    }

    fn tick(&mut self, shared: &VacScanShared) -> PgResult<()> {
        let f = shared.frontier.prefix.load(Ordering::Relaxed);
        if f > 0 {
            pgstat_progress_update_param(
                PROGRESS_VACUUM_HEAP_BLKS_SCANNED,
                shared.source.block_of(f - 1).block as i64,
            );
        }
        ::backend_progress::pgstat_progress_update_multi_param(
            &[PROGRESS_VACUUM_NUM_DEAD_ITEM_IDS, PROGRESS_VACUUM_DEAD_TUPLE_BYTES],
            &[
                self.progress_base_items + shared.num_dead_items.load(Ordering::Relaxed),
                (self.progress_base_bytes + shared.quiesce.bytes()) as i64,
            ],
        );

        let scanned = shared.scanned_pages.load(Ordering::Relaxed);
        if !shared.failsafe.load(Ordering::Relaxed)
            && scanned / FAILSAFE_EVERY_PAGES as u64
                > self.last_failsafe_page / FAILSAFE_EVERY_PAGES as u64
        {
            self.last_failsafe_page = scanned;
            if vacuum_xid_failsafe_check(&shared.cutoffs)? {
                // Quiesce the round (claims no-op; delays stop). The
                // C-body state change applies after the round completes.
                shared.failsafe.store(true, Ordering::Release);
            }
        }
        Ok(())
    }
}

/// M4.1 pool channel: one round's pool wait verdict.
enum PoolWait {
    /// The RG reached an outcome under pool participation.
    Done(runtime::RgOutcome),
    /// Pool path refused with the RG UNTOUCHED (started == 0, so no
    /// granule was claimed) — the launched gang takes over.
    Fallback,
}

/// First-claim deadline for the pool channel (the standing channel's
/// PGRUST_RUNTIME_GANG_CLAIM_MS, same knob): parked pool workers wake in
/// microseconds, so an unclaimed engagement after this long means the pool
/// cannot serve us — fall back to the launched gang (correctness never
/// depends on this).
fn pool_claim_deadline() -> std::time::Duration {
    static MS: OnceLock<u64> = OnceLock::new();
    std::time::Duration::from_millis(*MS.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_GANG_CLAIM_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(100)
    }))
}

/// The pool channel's submit-and-park (the standing channel's wait loop
/// shape + this arm's leader cadence): poll completion + interrupts +
/// progress/failsafe + board participation counters. Every exit path
/// closes the board entry and awaits participant detach (the leader arena
/// must outlive the workers' refs); Fallback returns with the RG untouched.
fn pool_wait(
    vacrel: &mut LVRelState<'_, '_>,
    rt: &'static Arc<runtime::Runtime>,
    shared: &Arc<VacScanShared>,
    entry: &Arc<parallel::standing::StandingEngagement>,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
) -> PgResult<PoolWait> {
    let take_board = || {
        shared.pool_board.lock().unwrap_or_else(|p| p.into_inner()).take();
    };
    let close = |why: &str| {
        take_board();
        parallel::standing::close_and_await(entry);
        if !why.is_empty() {
            vtrace(why);
        }
    };
    let mut cadence = LeaderCadence::new(vacrel, shared);
    // DST P2: the claim deadline is BEHAVIORAL (changes execution path) —
    // its clock is pg_clock (the standing_channel precedent).
    let t0 = pg_clock::MonoStamp::now();
    let mut traced = false;
    let granules = shared.source.entries().len();
    loop {
        if let Some(o) = waiter.try_wait() {
            close("");
            if !traced {
                vtrace(&format!(
                    "engaged pooldb dop={} granules={granules}",
                    entry.claimed()
                ));
            }
            return Ok(PoolWait::Done(o));
        }
        // Order matters on every error exit: abort THEN drain (the leader's
        // protocol cleanup completes the RG and releases workers parked in
        // their drives) THEN close-and-await.
        if let Err(e) = postgres_seams::check_for_interrupts::call() {
            rg.abort();
            drain_rg(rt, rg);
            close("");
            return Err(e);
        }
        if let Err(e) = cadence.tick(shared) {
            rg.abort();
            drain_rg(rt, rg);
            close("");
            return Err(e);
        }
        let claimed = entry.claimed();
        if !traced && claimed > 0 {
            vtrace(&format!("engaged pooldb dop={claimed} granules={granules}"));
            traced = true;
        }
        let started = shared.started.load(Ordering::SeqCst);
        let refused = entry.refused() + shared.refused.load(Ordering::SeqCst);
        // Nobody will participate: every ticket-holder refused pre-bind or
        // at the bind/lane stage, before any granule was claimed.
        if started == 0 && refused >= entry.tickets() {
            close(&format!("pooldb refused ({refused} refusals) — launched fallback"));
            return Ok(PoolWait::Fallback);
        }
        // Nothing driving and nothing pending within the deadline: the pool
        // cannot serve us (all busy / gated off). No granule was consumed;
        // the launched gang takes over. A straggler that claims right as we
        // close simply drives the same RG (morsel claims are atomic).
        if started == 0
            && entry.detached() >= claimed
            && std::time::Duration::from_nanos(t0.elapsed_ns()) > pool_claim_deadline()
        {
            close("pooldb claim deadline — launched fallback");
            return Ok(PoolWait::Fallback);
        }
        // Participants all detached yet the RG is incomplete and no error
        // was recorded: a worker died outside every catch layer.
        if claimed > 0 && started > 0 && entry.detached() >= claimed {
            if let Some(o) = waiter.try_wait() {
                close("");
                return Ok(PoolWait::Done(o));
            }
            if let Some(e) = shared.take_error() {
                rg.abort();
                drain_rg(rt, rg);
                close("");
                return Err(e);
            }
            rg.abort();
            drain_rg(rt, rg);
            close("");
            return Err(Box::new(PgError::new(
                ERROR,
                "vacuum scan pool executors exited before completing the round",
            )));
        }
        // Cancel dispositions (statement_timeout / pg_cancel_backend)
        // surface from the latch quantum.
        if let Err(e) = parallel::wait_parallel_finish_quantum() {
            rg.abort();
            drain_rg(rt, rg);
            close("");
            return Err(e);
        }
    }
}

fn run_round(
    vacrel: &mut LVRelState<'_, '_>,
    rt: &'static Arc<runtime::Runtime>,
    k: i32,
    shared: &Arc<VacScanShared>,
    clock: &mut ScanPhaseClock,
) -> PgResult<RoundOutcome> {
    xact::EnterParallelMode();
    let r = round_ceremony(vacrel, rt, k, shared, clock);
    xact::ExitParallelMode();
    r
}

fn round_ceremony(
    vacrel: &mut LVRelState<'_, '_>,
    rt: &'static Arc<runtime::Runtime>,
    k: i32,
    shared: &Arc<VacScanShared>,
    clock: &mut ScanPhaseClock,
) -> PgResult<RoundOutcome> {
    let t_launch = std::time::Instant::now();
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_vacuum_scan_main", k)?;

    // Every exit past submission must complete the pinned RG; held outside
    // the body closure so ?-propagated errors reap it too.
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|submitted: &mut Option<runtime::RgHandle>| -> PgResult<RoundOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        if parallel::nworkers(pcxt) <= 0 {
            return Ok(RoundOutcome::Refused);
        }
        parallel::set_private(pcxt, Arc::clone(shared) as _);
        shared
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once per round payload"));

        // M4.1 pool channel — built BEFORE submit (the bound descriptor
        // must ride the submission: publication keys the pool-visible
        // active bit off it). None = plain pinned submit + launched gang,
        // today exactly.
        let pool = try_pool_channel(pcxt, shared, k);

        // C's leader cost-balance carry-in (vacuumparallel discipline).
        // BEFORE the submission goes live on any channel: pool workers may
        // serve the instant the publication lands, and their shared-cost
        // fetch_adds must never race the carry-in stores. The parked leader
        // accrues nothing until the round's carry-back.
        shared
            .cost
            .cost_balance
            .store(g::VacuumCostBalance() as u32, std::sync::atomic::Ordering::SeqCst);
        shared.cost.active_nworkers.store(0, std::sync::atomic::Ordering::SeqCst);

        // Submit the pinned RG before launch: helpers find work immediately.
        static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
        let work: Arc<dyn runtime::TaskSetWork> = Arc::clone(shared) as _;
        let source: Arc<dyn runtime::MorselSource> = Arc::clone(&shared.source) as _;
        let spec = runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst),
            tasksets: vec![runtime::TaskSetSpec { source, work, deps: vec![] }],
        };
        let (rg, waiter) = match &pool {
            // QoS class Utility (Track-4 Q0/Q1): utility stride weight +
            // the capped ledger tier; width ceiling = the admitted K.
            Some((_, descriptor)) => rt.submit_pinned_bound_utility(
                spec,
                0,
                Some(runtime::WidthRequest::unbounded(k.max(1) as u32)),
                descriptor.clone(),
            ),
            None => rt.submit_pinned(spec),
        };
        shared
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once per round payload"));
        *submitted = Some(rg.clone());

        // Pool phase: park against the board; Done shares the launched
        // path's post-outcome tail below; Fallback leaves the RG untouched
        // (started == 0, nothing claimed) and the launched gang takes over.
        if let Some((entry, _)) = &pool {
            *shared.pool_board.lock().unwrap_or_else(|p| p.into_inner()) =
                Some(Arc::clone(entry));
            match pool_wait(vacrel, rt, shared, entry, &rg, &waiter)? {
                PoolWait::Done(outcome) => {
                    if let Some(e) = shared.take_error() {
                        return Err(e);
                    }
                    if outcome == runtime::RgOutcome::Aborted {
                        postgres_seams::check_for_interrupts::call()?;
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "vacuum morsel scan round aborted",
                        )));
                    }
                    if shared.started.load(Ordering::SeqCst) == 0 {
                        return Ok(RoundOutcome::Refused);
                    }
                    return Ok(RoundOutcome::Completed);
                }
                PoolWait::Fallback => {}
            }
        }
        // Pool-phase refusals must not poison the launched loop's
        // nobody-participates probe below (fresh helpers are still coming
        // up): count launched-phase refusals from this floor.
        let refused_base = shared.refused.load(Ordering::SeqCst);

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        clock.ceremony_ns += t_launch.elapsed().as_nanos() as u64;
        if launched <= 0 {
            drain_rg(rt, &rg);
            return Ok(RoundOutcome::Refused);
        }
        vtrace(&format!(
            "launched dop={launched} granules={}",
            shared.source.entries().len()
        ));
        g::SetVacuumCostBalance(0);
        set_vacuum_cost_balance_local(0);

        // Submit-and-park (WaitForParallelWorkersToFinish-shaped): completion
        // poll + parallel-message drain + CFI + progress publication +
        // failsafe cadence + bounded latch quantum.
        let mut cadence = LeaderCadence::new(vacrel, shared);
        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                drain_rg(rt, &rg);
                return Err(e);
            }

            // Progress (decision 3) + failsafe cadence (§5.5).
            cadence.tick(shared)?;

            // Helpers all gone with the RG incomplete: post-Terminate death.
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                let drained = drain_rg(rt, &rg);
                if claimed == 0 && drained {
                    return Ok(RoundOutcome::Refused);
                }
                if let Some(e) = shared.take_error() {
                    return Err(e);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "vacuum scan helpers exited before completing the round",
                )));
            }
            // All-refused with nothing claimed: fall back serial.
            let refused = shared.refused.load(Ordering::SeqCst) - refused_base;
            let started = shared.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                drain_rg(rt, &rg);
                return Ok(RoundOutcome::Refused);
            }
            // Cancel dispositions (statement_timeout / pg_cancel_backend)
            // surface from the latch quantum.
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                drain_rg(rt, &rg);
                return Err(e);
            }
        };

        if let Some(e) = shared.take_error() {
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            // Aborted without a recorded error: a cancel raced us — let the
            // pending interrupt surface; otherwise report the abort.
            postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(ERROR, "vacuum morsel scan round aborted")));
        }
        if shared.started.load(Ordering::SeqCst) == 0 {
            // Completed but nobody participated (only possible pre-claim).
            return Ok(RoundOutcome::Refused);
        }
        Ok(RoundOutcome::Completed)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // context is destroyed (helpers reference the payload until then).
    let t_teardown = std::time::Instant::now();
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    // C's cost-balance carry-back (workers' unspent balance returns to the
    // leader's serial accounting; on refused paths this is the carry-in).
    if submitted.is_some() {
        g::SetVacuumCostBalance(
            shared.cost.cost_balance.load(std::sync::atomic::Ordering::SeqCst) as i32
        );
    }
    clock.ceremony_ns += t_teardown.elapsed().as_nanos() as u64;
    let out = body?;
    destroy?;
    Ok(out)
}
