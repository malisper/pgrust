//! M2 DISTINCT SINK — parallel exact-DISTINCT / COUNT(DISTINCT) on the
//! morsel runtime (docs/design/m2-sinks.md §3 donor B re-homed;
//! docs/design/parallelism-redesign-2026-07.md §2.2/§5-M2).
//!
//! Shape: the SERIAL-plan grouped distinct pipeline `Agg(AGG_SORTED) ← Sort
//! ← SeqScan(cbstore)` (the ClickBench Q9/Q10 class), executed as one
//! SealedParallelSink on the runtime: ACCEPT (granule-morsel scan →
//! PREWHERE → per-worker `PdBuilder` partial: compact int group keys,
//! (acc,count) vocab words, exact `DistinctSet`s) → SEAL (parallel
//! per-worker freeze into `PdHandedTable`s) → COMBINE (256 group-partition
//! bucket-claim merges — disjoint partitions, single writer per output
//! cell) → finalize (concatenate buckets, publish). The parked leader
//! adopts the merged result through the UNCHANGED serial emit tail
//! (`agg_hashgroup_adopt_merged` → hashgroup emit): groups in the plan
//! Sort's prefix order, byte-identical to the serial arm by the donor's
//! identity argument (exact representational set equality;
//! order-insensitive-exact transitions; count/sum reassociation
//! unobservable).
//!
//! vs the Gather-era donor (pardistinct): the registry/handoff, the leader's
//! own partial, the stray-row queue drain, and the `spent` flag are all
//! GONE (no tuple queues exist); the vocabulary refusal is DROPPED — the
//! Q10 companion-agg shape rides the sink (the donor's refusal priced the
//! per-row vocab accept against the fused classic GatherMerge drives, a
//! comparison that no longer exists here).
//!
//! Budget law (m2-sinks.md R3/R5; M3.5 §4): each Local gets the derived
//! `worker_budget` (C-parity per participant; participants = launched
//! helpers ≤ dop, so the memory envelope is the plan-shaped one, never
//! nthreads-shaped). A worker CROSSING its budget SPILLS an epoch of its
//! set values to its FileSet spill file (grouped int-set shapes; the
//! docs/design/m3.5-spill.md §4 arm, `PGRUST_RUNTIME_DISTINCT_SPILL=0`
//! restores phase 1) and keeps accepting bounded; shapes the spill cannot
//! carry exactly — and combine partitions whose merged sets would cross
//! the budget (value-hash recursion is the later increment) — fall back to
//! the phase-1 law: the arm aborts the RG and the leader RERUNS THE SERIAL
//! ARM: exact, nothing consumed, bounded memory at every arm.
//!
//! Engagement layering (all cheap; absent = today's serial path, byte- and
//! perf-identical): PGRUST_RUNTIME=1 (pool spawned) + SET
//! pgrust.runtime_scan_pool = <dop> (the runtime DOP knob) +
//! PGRUST_RUNTIME_DISTINCT != 0 (arm kill switch). The plan surface stays
//! the serial plan; EXPLAIN unchanged; instrumented runs refuse (EXPLAIN
//! ANALYZE stays C-exact).

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::nodeagg::{
    pd_bucket_precount, pd_concat_buckets, pd_empty_grouped_table, pd_merge_bucket_refs,
    pd_spill_record_width, pd_table_from_spill, PdFeed, PdHandedTable, PdMerged, PdSinkLocal,
    PdSpec, PD_SINK_GROUP_PARTS,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, trace_feed};
use super::{drain_pipeline, BatchSink, SeqScanFilterProject, SeqScanSource, Sink, SinkFeed};

// ---------------------------------------------------------------------------
// Shared state: the parallel context's private payload AND the sink body
// (one struct, one Arc — the runtime_scan discipline).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

/// Per-worker sink Local: the donor `PdSinkLocal` plus the M3.5 spill face —
/// its single-writer spill file (epochs of partition-contiguous value
/// records), created lazily at the first budget crossing when the spill arm
/// is enabled. Plain data between flush events; rides SEAL like everything
/// else. `seen_null`, vocab words, and the group table itself never spill —
/// they stay inside the `PdSinkLocal` (design §4).
pub(super) struct DistinctSinkLocal {
    pd: PdSinkLocal,
    spill: Option<::spillset::SpillFile>,
}

/// A sealed Local: the frozen in-memory remainder + the (frozen) spill
/// directory the combine pre-counts and replays from (design §4: Sealed =
/// PdHandedTable + spill directory).
pub(super) struct DistinctSealed {
    table: PdHandedTable,
    spill: Option<::spillset::SpillFile>,
}

pub(super) struct RuntimeDistinctShared {
    rt: &'static Arc<runtime::Runtime>,
    /// Weak: the RG's task sets hold this struct as their sink — a strong
    /// handle here would leak the cycle.
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// The leader-derived build recipe (plain data; helpers fork Locals
    /// from it in-process — no DSM transfer).
    spec: Arc<PdSpec>,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// First worker-phase error (the entry-phase errors ride the ordinary
    /// parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    /// Set when any worker recorded an error (fast skip for later morsels).
    failed: AtomicBool,
    /// A worker budget crossed mid-accept: NOT an error — the RG aborts and
    /// the leader falls back to the serial arm (m2-sinks.md R5 phase 1).
    crossed: AtomicBool,
    /// M3.5 spill arm: the engagement's spill set (None = spill disabled →
    /// budget crossings refuse exactly as before).
    spill_set: Option<Arc<::spillset::SpillSet>>,
    /// Spill observability (gate-record counters, the R4 line).
    spill_epochs: AtomicU64,
    spilled_bytes: AtomicU64,
    /// Combine output cells, one per group partition. Single writer each:
    /// partition p is claimed exactly once by the combine task set.
    out: Vec<UnsafeCell<Option<PdMerged<'static>>>>,
    /// The published merged result (finalize writes, the leader takes).
    merged: Mutex<Option<PdMerged<'static>>>,
}

// SAFETY: (i) each `out` cell has a single writer — the sink contract
// visits every partition exactly once — and is read only by `finalize`,
// which the runtime's last-worker-out orders after every combine; (ii) the
// PdMerged values held in `out`/`merged` are never-spilled bucket-merge
// outputs (owned plain data — the PdHandedTable self-contained-buffer
// argument); (iii) every other member is Send/Sync by composition.
unsafe impl Send for RuntimeDistinctShared {}
unsafe impl Sync for RuntimeDistinctShared {}

impl RuntimeDistinctShared {
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

    /// Budget crossing: no degrade target under the runtime — abort the RG;
    /// the leader observes `crossed` and reruns the serial arm.
    fn cross(&self) {
        self.crossed.store(true, Ordering::SeqCst);
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

    fn take_merged(&self) -> Option<PdMerged<'static>> {
        self.merged.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

// ---------------------------------------------------------------------------
// The SealedParallelSink implementation. accept_local/seal are INFALLIBLE BY
// CONTRACT: errors and panics are caught, recorded (first wins), and turn
// into an RG abort — the runtime protocol never sees an unwind.
// ---------------------------------------------------------------------------

impl runtime::SealedParallelSink for RuntimeDistinctShared {
    type Local = DistinctSinkLocal;
    type Sealed = DistinctSealed;

    fn fork(&self, _worker: usize) -> DistinctSinkLocal {
        DistinctSinkLocal {
            pd: PdSinkLocal::new(Arc::clone(&self.spec), self.spec.worker_budget),
            spill: None,
        }
    }

    fn accept_local(
        &self,
        local: &mut DistinctSinkLocal,
        worker: usize,
        range: runtime::MorselRange,
    ) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            // Already aborting: drain the claim without work.
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(local, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime distinct worker panicked in a morsel").into(),
                );
            }
        }
    }

    fn seal(&self, _worker: usize, local: DistinctSinkLocal) -> DistinctSealed {
        let DistinctSinkLocal { pd, spill } = local;
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return DistinctSealed { table: pd_empty_grouped_table(&self.spec), spill: None };
        }
        let r = catch_unwind(AssertUnwindSafe(|| pd.freeze()));
        match r {
            // freeze() sees a never-spilled builder (its `!ever_spilled`
            // invariant holds: the M3.5 spill drains set VALUES only and
            // never touches the builder's own Mcx-bound machinery); the
            // spill directory rides alongside the frozen remainder.
            Ok(Ok(t)) => DistinctSealed { table: t, spill },
            Ok(Err(e)) => {
                self.fail(e);
                DistinctSealed { table: pd_empty_grouped_table(&self.spec), spill: None }
            }
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "runtime distinct worker panicked in seal").into());
                DistinctSealed { table: pd_empty_grouped_table(&self.spec), spill: None }
            }
        }
    }

    fn partitions(&self) -> u64 {
        PD_SINK_GROUP_PARTS
    }

    fn combine(&self, part: u64, sealed: &[DistinctSealed]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.combine_body(part as usize, sealed)));
        match r {
            Ok(Ok(DstCombine::Done(m))) => {
                // SAFETY: partition `part` is handed to this claimer alone
                // (sink contract); finalize reads happen-after every combine.
                unsafe { *self.out[part as usize].get() = Some(m) };
            }
            Ok(Ok(DstCombine::OverBudget)) => {
                // Bounded-memory refusal, not an error: the merged bucket
                // would cross the worker budget (the value-hash recursive
                // split is the chartered inc-3b) — abort to the serial
                // rerun, which spills through its own C-parity machinery.
                lane_trace(
                    "runtime-distinct: combine partition over budget (value-hash recursion pending) — serial rerun",
                );
                self.cross();
            }
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(
                    PgError::new(ERROR, "runtime distinct worker panicked in combine").into(),
                );
            }
        }
    }

    fn finalize(&self, _sealed: &[DistinctSealed]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        // SAFETY: single-threaded under last-worker-out, after every combine.
        let buckets: Vec<PdMerged<'static>> = self
            .out
            .iter()
            .filter_map(|c| unsafe { (*c.get()).take() })
            .collect();
        let merged = pd_concat_buckets(buckets);
        *self.merged.lock().unwrap_or_else(|p| p.into_inner()) = Some(merged);
    }
}

/// Combine verdict: `OverBudget` is the conservative directory-only refusal
/// (M3.5 §4/§7 — nothing is read from disk before the decision).
enum DstCombine {
    Done(PdMerged<'static>),
    OverBudget,
}

impl RuntimeDistinctShared {
    /// COMBINE(part b), the M3.5 spill-aware path: pre-count b's spilled
    /// bytes from the spill-file DIRECTORIES + the in-memory tables'
    /// partition indexes; refuse (→ serial rerun) if the merged bucket's
    /// estimated bytes cross the worker budget; otherwise read b's records
    /// (open-by-name on THIS thread — the files are frozen: combine
    /// deps-follows accept), rebuild them into merge-compatible tables
    /// through the donor builder kernel, and run the donor bucket merge
    /// over in-memory + synthesized tables. Set-insert idempotence makes
    /// replay order immaterial (cross-epoch duplicates re-dedup here).
    fn combine_body(&self, b: usize, sealed: &[DistinctSealed]) -> PgResult<DstCombine> {
        let spilled_bytes: u64 = sealed
            .iter()
            .filter_map(|s| s.spill.as_ref())
            .map(|f| f.part_len(b as u32))
            .sum();
        if spilled_bytes == 0 {
            // Nothing spilled into this partition: the donor merge verbatim.
            let refs: Vec<&PdHandedTable> = sealed.iter().map(|s| &s.table).collect();
            return Ok(DstCombine::Done(pd_merge_bucket_refs(&self.spec, &refs, b)));
        }
        // Pre-count size check (M3.5 §4): spilled record count from the
        // directory alone; in-memory groups/values from the partition
        // indexes. Every term over-counts duplicates, so this only ever
        // refuses conservatively. Estimate: values cost ~16B each in a
        // merged set (i64 + probe slot), spilled values are transiently
        // held TWICE (synth table + merged output), groups carry the
        // fixed per-group block; 3/2 headroom on the value term.
        let width = pd_spill_record_width(&self.spec) as u64;
        let spilled_vals = (spilled_bytes / width) as usize;
        let mut groups = 0usize;
        let mut inmem_vals = 0usize;
        for s in sealed {
            let (g, v) = pd_bucket_precount(&self.spec, &s.table, b);
            groups += g;
            inmem_vals += v;
        }
        let per_group = self.spec.nkeys() * 8
            + 2 * self.spec.vocab.len() * 8
            + self.spec.sets.len() * 48
            + 64;
        let est = (inmem_vals + 2 * spilled_vals)
            .saturating_mul(16)
            .saturating_mul(3)
            / 2
            + groups.saturating_mul(per_group);
        if est > self.spec.worker_budget {
            return Ok(DstCombine::OverBudget);
        }
        // Read + rebuild each Local's spilled partition, then merge.
        let ctx = ::mcx::MemoryContext::new("m35-dst-spill-read");
        let mut synth: Vec<PdHandedTable> = Vec::new();
        for s in sealed {
            let Some(f) = &s.spill else { continue };
            if let Some(mut r) = f.read_part(ctx.mcx(), b as u32)? {
                let bytes = r.read_to_end()?;
                r.close()?;
                synth.push(pd_table_from_spill(&self.spec, &bytes)?);
            }
        }
        let refs: Vec<&PdHandedTable> =
            sealed.iter().map(|s| &s.table).chain(synth.iter()).collect();
        Ok(DstCombine::Done(pd_merge_bucket_refs(&self.spec, &refs, b)))
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: thread-local executor + the accept morsel body.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    /// Per-helper detoast scratch context (reset per row when a bytes set
    /// detoasts into per-tuple memory).
    tmp: EcxtId,
    reset_tmp: bool,
    /// THIS helper contributed an error (take the release/abort teardown).
    errored: std::cell::Cell<bool>,
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

/// The per-morsel accept feed: rows into the worker's `PdSinkLocal`. A
/// budget crossing SPILLS an epoch when the M3.5 arm is on and the shape is
/// exactly spillable; otherwise it flips `crossed` and drops the remainder
/// of the morsel (the RG is aborting; nothing is emitted anywhere).
struct PdAcceptSink<'a> {
    shared: &'a RuntimeDistinctShared,
    local: &'a mut DistinctSinkLocal,
    worker: usize,
    tmp: EcxtId,
    reset_tmp: bool,
    crossed: bool,
}

impl PdAcceptSink<'_> {
    /// M3.5 accept-side spill (design §4): on `PdFeed::Crossed`, write the
    /// Local's accumulated set values to its spill file as ONE epoch —
    /// partitions 0..255 contiguous in the freeze partition law's order,
    /// `seen_null`/vocab/group table kept in memory — then reset the sets'
    /// values so accept continues bounded. `Ok(false)` = refused (arm off,
    /// or a shape/economics face we cannot spill exactly): the caller falls
    /// through to the phase-1 Crossed abort, fail-closed.
    fn try_spill_epoch(&mut self) -> PgResult<bool> {
        let Some(set) = &self.shared.spill_set else { return Ok(false) };
        let DistinctSinkLocal { pd, spill } = &mut *self.local;
        if !pd.pd_spill_eligible() {
            return Ok(false);
        }
        // Worthwhileness (fail-closed): a group-table-dominated crossing
        // cannot be helped by value spill — the epoch must move a
        // meaningful fraction of the budget to disk or the arm refuses.
        let budget = self.shared.spec.worker_budget;
        if pd.pd_spill_value_bytes() < budget / 4 {
            return Ok(false);
        }
        let file = spill.get_or_insert_with(|| {
            ::spillset::SpillFile::new(
                Arc::clone(set),
                ::spillset::SpillSet::file_name("dst", 0, self.worker),
                PD_SINK_GROUP_PARTS as u32,
            )
        });
        let before = file.spilled_bytes();
        // Open-per-event on the owning worker thread (§2 amendment): the
        // BufFile handle lives inside this flush event alone. Values reset
        // only after the epoch COMMITS — an error path loses nothing.
        let ctx = ::mcx::MemoryContext::new("m35-dst-spill-write");
        let mut w = file.begin_epoch(ctx.mcx())?;
        pd.pd_spill_emit(&mut |p, bytes| w.write_part(p, bytes))?;
        w.finish()?;
        pd.pd_spill_reset_values();
        self.shared.spill_epochs.fetch_add(1, Ordering::Relaxed);
        self.shared
            .spilled_bytes
            .fetch_add(file.spilled_bytes() - before, Ordering::Relaxed);
        Ok(true)
    }
}

impl<'mcx> Sink<'mcx> for PdAcceptSink<'_> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        if self.crossed {
            return Ok(SinkFeed::NeedMore);
        }
        let crossed = self.local.pd.accept(estate, tuple, self.tmp)? == PdFeed::Crossed;
        if self.reset_tmp {
            estate.reset_expr_context(self.tmp);
        }
        if crossed && !self.try_spill_epoch()? {
            self.crossed = true;
        }
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for PdAcceptSink<'_> {}

impl RuntimeDistinctShared {
    fn morsel_body(
        &self,
        local: &mut DistinctSinkLocal,
        worker: usize,
        range: runtime::MorselRange,
    ) -> PgResult<()> {
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime distinct morsel without a bound executor",
                )));
            };
            let (tmp, reset_tmp) = (ex.tmp, ex.reset_tmp);
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime distinct worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = distinct_worker_scan(d.planstate.as_mut())?;
                    if !::nodeseqscan::seq_scan_cb_set_granule_range(
                        ss,
                        estate,
                        range.start,
                        range.end,
                    )? {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime distinct worker scan is not cbstore",
                        )));
                    }
                    let mut sink = PdAcceptSink {
                        shared: self,
                        local,
                        worker,
                        tmp,
                        reset_tmp,
                        crossed: false,
                    };
                    let fed = drain_pipeline(
                        ss,
                        &mut SeqScanSource,
                        &mut SeqScanFilterProject,
                        &mut sink,
                        estate,
                    );
                    let crossed = sink.crossed;
                    fed?;
                    if crossed {
                        trace_feed(
                            "runtime distinct worker budget crossed; aborting to serial fallback",
                        );
                        self.cross();
                    }
                    Ok(())
                })
            })
        })
    }
}

/// The worker plan tree is the SCAN SUBTREE alone (workers never run the
/// Agg or the Sort — accept_local drives scan → PREWHERE → project into the
/// PdBuilder; the worker pstmt's planTree is the SeqScan node).
fn distinct_worker_scan<'a, 'mcx>(
    planstate: Option<&'a mut crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = planstate else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime distinct worker plan is not a SeqScan root",
        )));
    };
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Helper entry + POST_TASK_PARK drive (the runtime_scan ceremony, with this
// arm's payload type; the hook registries are multi-registrant and every
// hook no-ops on foreign payloads).
// ---------------------------------------------------------------------------

fn runtime_distinct_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_distinct_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeDistinctShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime distinct helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeDistinctShared>) {
    let _ = shared;
    let Some(target) = payload.pcxt_shared.get() else { return };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else { return };
    let Some(lane) = payload.rt.acquire_external_lane() else {
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
                payload.fail(e);
            } else {
                lane_trace(&format!(
                    "runtime-distinct: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeDistinctShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    teardown_worker_exec(!self_errored)
}

fn build_worker_exec(payload: &Arc<RuntimeDistinctShared>) -> PgResult<()> {
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
        let armed = (|| -> PgResult<EcxtId> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime distinct worker ExecutorStart");
                x.with_mut(|d| -> PgResult<EcxtId> {
                    let estate = &mut d.estate;
                    let ss = distinct_worker_scan(d.planstate.as_mut())?;
                    super::arm_scan_staging(
                        ss,
                        estate,
                        super::ScanFeedShape::RowFeed {
                            ctx: "runtime distinct worker feed",
                            stitch: true,
                        },
                    )?;
                    Ok(estate.exec_assign_expr_context())
                })
            })
        })();
        match armed {
            Ok(tmp) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    tmp,
                    reset_tmp: payload.spec.any_bytes_set(),
                    errored: std::cell::Cell::new(false),
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

fn runtime_distinct_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeDistinctShared>() else { return };
    if let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) {
        rg.abort();
    }
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_distinct_main",
            runtime_distinct_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_distinct_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_distinct_private_shutdown);
    });
}

/// `PGRUST_RUNTIME_DISTINCT` arm kill switch (default ON when the runtime
/// is armed; the runtime itself defaults OFF).
fn runtime_distinct_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_DISTINCT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// M3.5 spill arm kill switch: ON by default when the sink engages
/// (refusal→engagement is the charter); `PGRUST_RUNTIME_DISTINCT_SPILL=0`
/// restores the phase-1 budget refusal exactly.
fn distinct_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_DISTINCT_SPILL").as_deref() != Ok("0"))
}

// ---------------------------------------------------------------------------
// Leader-side engagement.
// ---------------------------------------------------------------------------

/// Refusal diagnosis trace (PGRUST_LANE_V2_TRACE only; emitted only once the
/// arm is ARMED — dop set + runtime on — so unarmed sessions stay silent).
#[cold]
fn refused(reason: &str) {
    lane_trace(&format!("runtime-distinct: refused ({reason})"));
}

/// The runtime distinct-sink arm, probed from the sorted-agg narrow branch
/// (set-mode already armed by the caller — the last-refusal ordering law is
/// satisfied there). `None` = refused or fell back (nothing consumed; the
/// serial arms run byte-identically). `Some(row)` = the arm owns the node
/// (merged result adopted; emit chain active).
pub(super) fn try_own_sorted_distinct_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    rd_shape_refused: &mut bool,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_scan_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() || !runtime_distinct_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // Static shape refusal memo: the plan-shape gates below cannot flip for
    // this node; skip the whole probe (incl. spec derivation) on re-pulls.
    if *rd_shape_refused {
        return Ok(None);
    }
    lane_trace("runtime-distinct: probed");

    // --- Shape + session gates (fail-closed; every refusal is the serial arm).
    let crate::procnode::PlanStateNode::SeqScan(ss) = outer else {
        refused("outer not SeqScan");
        return Ok(None);
    };
    if !seq_scan_fusible(ss, estate)? || !::nodeseqscan::seq_scan_is_cbstore(ss) {
        refused("scan not fusible/cbstore");
        return Ok(None);
    }
    // Instrumented runs refuse the sink (EXPLAIN ANALYZE stays C-exact) —
    // the caller's seam does not gate instrumentation for the serial arms.
    if estate.es_instrument != 0 || estate.es_epq_active {
        refused("instrumented/epq");
        return Ok(None);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refused("already in parallel machinery");
        return Ok(None);
    }
    // Agg-side admission: the hash-grouped arm's integer-key/exact-set
    // vocabulary and its density economics (a refusal falls back to the
    // serial arms, byte-identically). Vocab shapes (Q10 companions) are
    // ADMITTED — see the module doc.
    if !::nodeagg::agg_hashgroup_admissible(agg)
        || !::nodeagg::agg_hashgroup_economical(
            agg,
            super::pardistinct_force(),
            sort.plan.plan.plan_rows,
        )
    {
        refused("hashgroup admission/economics");
        return Ok(None);
    }
    let Some(order) = super::hashgroup_order_spec(agg, sort.plan, k) else {
        refused("order spec");
        *rd_shape_refused = true;
        return Ok(None);
    };
    let Some(desc) = outer_desc.as_ref() else {
        refused("no outer desc");
        return Ok(None);
    };
    let Some(spec) = ::nodeagg::pd_derive_spec(agg, desc) else {
        refused("spec derivation");
        *rd_shape_refused = true;
        return Ok(None);
    };
    if spec.max_att > desc.natts {
        refused("att bound");
        *rd_shape_refused = true;
        return Ok(None);
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused("extern params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused("exec params");
        return Ok(None);
    }
    // Plan shape below the Agg: exactly THIS Sort → SeqScan (the workers
    // receive the SCAN SUBTREE as their pstmt — the Agg need not be the
    // plan root, so ORDER BY/LIMIT above it, the real CB q9/q10 shape,
    // stays engageable).
    let Some(sort_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if sort_node.node_tag() != NodeTag::T_Sort
        || !std::ptr::eq(sort_node.as_sort().expect("Sort tag"), sort.plan)
    {
        refused("agg child not this Sort");
        *rd_shape_refused = true;
        return Ok(None);
    }
    let Some(scan_node) = sort.plan.plan.lefttree else { return Ok(None) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused("sort child not SeqScan");
        *rd_shape_refused = true;
        return Ok(None);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused("parallel-unsafe scan exprs");
        *rd_shape_refused = true;
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refused("non-MVCC snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refused("binder policy sources");
        return Ok(None);
    }

    // --- Geometry: enough granules to be worth a gang.
    let Some((total_granules, starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        return Ok(None);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused("granule floor");
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    engage(agg, estate, rt, dop, total_granules, starts, spec, order, scan_node)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    spec: Arc<PdSpec>,
    order: Vec<::nodeagg::HashGroupOrderKey>,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    // The worker pstmt carries ONLY the scan subtree (ExecSerializePlan's
    // fragment-transfer shape; the helpers drive scan → PREWHERE → project
    // into their PdBuilder Locals — no Agg, no Sort).
    let pstmt = crate::execparallel::build_worker_pstmt(estate, scan_node)?;

    // M3.5 spill arm: ON by default when the sink engages (the
    // refusal→engagement charter); PGRUST_RUNTIME_DISTINCT_SPILL=0 restores
    // the phase-1 refusal exactly. SpillSet creation is leader-side (fd
    // substrate guaranteed); a creation failure fail-closes to refusal.
    let spill_set = if distinct_spill_enabled() {
        match ::spillset::SpillSet::create() {
            Ok(s) => Some(s),
            Err(_) => {
                lane_trace("runtime-distinct: spill set creation failed — spill disarmed");
                None
            }
        }
    } else {
        None
    };

    let payload = Arc::new(RuntimeDistinctShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path.
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        spec: Arc::clone(&spec),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        crossed: AtomicBool::new(false),
        spill_set,
        spill_epochs: AtomicU64::new(0),
        spilled_bytes: AtomicU64::new(0),
        out: (0..PD_SINK_GROUP_PARTS as usize).map(|_| UnsafeCell::new(None)).collect(),
        merged: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged =
        engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload, spec, order);
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
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeDistinctShared>,
    spec: Arc<PdSpec>,
    order: Vec<::nodeagg::HashGroupOrderKey>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_distinct_main", dop)?;
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

        // Submit the pinned RG (accept → freeze → combine) before launch.
        let source = Arc::new(super::runtime_scan::CbstoreGranuleSource { starts });
        let runtime::SealedSinkTaskSets { accept, freeze, combine, probe: _probe } =
            runtime::sealed_sink_tasksets(
                Arc::clone(payload),
                source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![accept, freeze, combine],
        });
        payload
            .rg
            .set(rg.downgrade())
            .unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-distinct: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        lane_trace(&format!(
            "runtime-distinct: engaged dop={launched} granules={total_granules} vocab={} sets={}",
            spec.vocab.len(),
            spec.sets.len()
        ));

        // Submit-and-park (the WaitForParallelWorkersToFinish shape).
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
                    "runtime-distinct: all {refused} helpers refused the bind"
                ));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            parallel::wait_parallel_finish_quantum();
        };

        if let Some(e) = payload.take_error() {
            lane_trace(&format!(
                "runtime-distinct: worker-phase error: {}",
                e.message()
            ));
            return Err(e);
        }
        if outcome == runtime::RgOutcome::Aborted {
            if payload.crossed.load(Ordering::SeqCst) {
                // Worker budget crossed: bounded-memory refusal — rerun the
                // serial arm (nothing was emitted; the leader's scan is
                // untouched).
                lane_trace("runtime-distinct: worker budget crossed; serial fallback");
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
                return Ok(EngageOutcome::Fallback);
            }
            ::postgres_seams::check_for_interrupts::call()?;
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime distinct pipeline aborted",
            )));
        }
        if payload.started.load(Ordering::SeqCst) == 0 {
            return Ok(EngageOutcome::Fallback);
        }
        Ok(EngageOutcome::Completed)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // parallel context is destroyed and this frame's arena can unwind.
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
            lane_trace("runtime-distinct: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let Some(merged) = payload.take_merged() else {
                // Completed with participants but no published result: a
                // protocol violation, never silently wrong output.
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime distinct completed without a merged result",
                )));
            };
            stats::tick_owned(ShapeClass::AggBuild);
            let spill_epochs = payload.spill_epochs.load(Ordering::Relaxed);
            if spill_epochs > 0 {
                // The R4 spill-rate observability line (e2e + gate records).
                lane_trace(&format!(
                    "runtime-distinct: SPILLED epochs={spill_epochs} bytes={}",
                    payload.spilled_bytes.load(Ordering::Relaxed)
                ));
            }
            lane_trace(&format!(
                "runtime-distinct: complete, groups={}",
                merged.ngroups
            ));
            trace_feed("runtime distinct sink adopt + hashgroup emit engaged");
            ::nodeagg::agg_hashgroup_adopt_merged(
                agg,
                estate,
                merged.into_lt(),
                &spec.vocab,
                order,
            )?;
            Ok(Some(super::hashgroup_emit(agg, estate)?))
        }
    }
}

/// Reap a pinned RG no helper will drive (abort/fallback paths) — protocol
/// cleanup driving, not leader work execution (§2.5).
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
