//! MJSORT — the "merge join after sort" tier-2 car (m5-coverage row
//! merge-join-parallel; the Gather-elimination program's named car:
//! scratchpad all-morsels plan, Track 1).
//!
//! Shape: `MergeJoin(INNER, Sort(SeqScan(pgrcolumnar)), Sort(SeqScan(
//! pgrcolumnar)))` with the mergeclauses the ONLY join predicate (no
//! joinqual residue, no otherqual) — the sort-both-sides plan class where
//! the sorts dominate and the merge pass is the serial residue.
//!
//! Drive, three phases on the morsel runtime:
//!   1. OUTER full sort — the shape-(b) full-sort engagement
//!      (runtime_sort.rs) in PUBLISH mode: sealed per-worker runs +
//!      partition outputs returned to this arm, NO emit face installed on
//!      the Sort node (the child stays pristine — any later fallback runs
//!      the Volcano FSM over untouched children, byte-identically).
//!   2. INNER full sort — same, and `allow_ra`: the inner Sort's
//!      EXEC_FLAG_MARK randomAccess shape is admissible BECAUSE no
//!      read-back face exists to serve marks; the merge below never
//!      rewinds (group cross products replace the FSM's mark/restore).
//!   3. RANGE-PARTITIONED MERGE — the mjmerge kernels (nodesort): prefix
//!      boundaries sampled from both sides cut both sorted run sets into
//!      aligned key ranges; each range joins independently as one morsel
//!      of a PURE-COMPUTE task set (owned plain data, no executor / no
//!      session state) submitted UN-PINNED so pool threads execute it —
//!      the first production non-pinned RG; the leader parks on the
//!      waiter with the CFI cadence. Pair refs land per-partition
//!      (single-writer slots); concatenation in partition order is the
//!      whole join in the global outer-key order.
//!
//! EMIT: the leader adopts (both sides' runs + the pair lists) onto the
//! MergeJoinNode and serves one pair per pull — child rows materialize as
//! VIRTUAL tuples in the two Sort children's result slots (the
//! runtime-full emit idiom), then `nodemergejoin::mjsort_project` runs
//! the node's OWN projection (quals refused at admission). Output order =
//! outer-major, inner-minor within equal keys, both sides in the
//! (keys, rowref) canonical order — the serial FSM's cadence over
//! canonically-ordered children; within-tie order vs the serial
//! tuplesort children is NOT a surface (docs/conformance/tie-ordering.md
//! rules; the full-sort arm's standing law).
//!
//! Ordering/content law: boundaries gate BALANCE only (mjmerge kernel
//! property tests); NULL keys never join (strict SQL equality — the
//! kernel skips NULL-keyed groups; pgrcolumnar stores no NULLs today, so
//! this is a belt). Cross-side alignment law at admission: per-column
//! (desc, nulls_first, signed/unsigned class) identical, so equal key
//! VALUES have equal packed words on both sides and both sides run the
//! same direction.
//!
//! Memory: pair refs are 12 B; the shared pair budget is work_mem-scaled
//! (`dop × work_mem / 12`) — crossing it aborts the merge RG and the
//! whole engagement falls back to the serial arm from pristine children
//! (the R5 whole-attempt-rerun discipline; nothing was emitted). The two
//! sort phases carry their own per-participant budgets (design §7).
//!
//! Engagement layering (unarmed = today's path, byte-identical):
//! PGRUST_RUNTIME=1 + the sort arm's DOP source (this car's engagements
//! ARE sort engagements — `router::arm_dop(ArmClass::Sort)`, so the bench
//! GUC `pgrust.runtime_sort_pool` and engine=runtime both arm it) + the
//! car's kill `PGRUST_RUNTIME_MJSORT` (DEFAULT ON since the GL-MJSORT-1
//! flip; OFF iff exactly `0`/`off` — the flipped-kill exact-spelling
//! law; provenance + named debts at the knob below). Instrumented runs
//! refuse (EXPLAIN ANALYZE stays C-exact); EXPLAIN shape unchanged.
//!
//! Coverage: matrix row mergejoin-int-columnar-sorts (covered/runtime,
//! probe_key "-" — no Gather competes on this shape, so there is nothing
//! to suppress at plan time and no BOOTSTRAP_MATRIX class is minted; the
//! umbrella row merge-join-parallel keeps the honestly-named remainder).

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use ::executils::{EStateData, ExecSlotId};
use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::nodesort::fullsort::FullRun;
use ::nodesort::mjmerge::{self, MjPair, MjPrefix, PairBudget};
use ::types_error::{PgError, PgResult, ERROR};

use super::router::{self, ArmClass};
use super::runtime_sort::{
    full_sort_engage_publish, full_sort_probe_for_mjsort, FullPublish, MjSortSideProbe,
};
use super::lane_trace;
use crate::procnode::{MergeJoinNode, PlanStateNode};

/// Partition-parallel merge width (the FULLSORT_PARTS / sink-bucket
/// precedent — partition-count-agnostic content, tested in mjmerge).
const MJSORT_PARTS: usize = 256;

/// `PGRUST_RUNTIME_MJSORT` — **DEFAULT ON since the GL-MJSORT-1 flip**
/// (letter: scratchpad/night/fleet-ab-parallelism.md @ measured sha
/// df9301f2f; DOP ladder ALL GATES PASS every cell witnessed+parity —
/// uniq 7.6-9.9x / dup 13.1-14.7x / two_key 17.6-25.8x vs serial at dop
/// {4,8,16}; 43q flatness pair 0.9992 damped geomean with the knob OFF).
/// Flipped-kill, t35 exact-spelling law: OFF iff exactly `0`/`off`;
/// every other spelling (including unset) is ON. 0 = unresolved, 1 =
/// OFF, 2 = ON — the AtomicU8 same-process A/B idiom (lane_mergejoin.rs).
/// Env-var, not GUC, per the standing `pg_settings` byte-identity
/// discipline.
///
/// Named debts carried across the flip (the letter's pre-flip list,
/// resolved as flip-time-acceptable — engagement still requires the sort
/// arm's DOP source to arm, so default sessions are unaffected until the
/// router arms):
///   * the two sort phases run sequentially (no overlap) — inc-2;
///   * DOP source borrowed from the sort arm (`ArmClass::Sort`), no own
///     router class/floor yet;
///   * INNER + int-family keys only (the admitted shape; everything else
///     refuses by name to the serial FSM);
///   * leader-side adopted-pair emit is the serial residual (the
///     saturation ceiling at high DOP) — batched emit is the inc-2 lever.
static MJSORT: AtomicU8 = AtomicU8::new(0);

#[inline]
pub(super) fn mjsort_enabled() -> bool {
    match MJSORT.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => mjsort_resolve(),
    }
}

/// The flipped-kill spelling law, isolated for the unit pins: ON unless
/// EXACTLY `0`/`off` (GL-MJSORT-1 flip; the t35 exact-spelling idiom).
#[inline]
fn mjsort_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
}

#[cold]
#[inline(never)]
fn mjsort_resolve() -> bool {
    let on = mjsort_spelling_on(std::env::var("PGRUST_RUNTIME_MJSORT").ok().as_deref());
    MJSORT.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

#[cfg(test)]
mod knob_tests {
    use super::mjsort_spelling_on;

    /// GL-MJSORT-1 flip posture: DEFAULT ON, kill iff exactly `0`/`off`
    /// (the flipped-kill exact-spelling law).
    #[test]
    fn mjsort_flipped_kill_spelling() {
        assert!(mjsort_spelling_on(None), "unset must be ON (flipped default)");
        for v in ["1", "on", "", "true", "ON", "OFF", "yes"] {
            assert!(mjsort_spelling_on(Some(v)), "spelling {v:?} must stay ON");
        }
        assert!(!mjsort_spelling_on(Some("0")));
        assert!(!mjsort_spelling_on(Some("off")));
    }
}

/// The adopted result living on the MergeJoinNode: both sides' sealed
/// runs (Arc — the emitted virtual tuples' byref datums point into the
/// run arenas, held until node reset/end) + the per-partition pair lists
/// and the emit cursor.
pub struct MjSortAdopted {
    oruns: Vec<Arc<FullRun>>,
    iruns: Vec<Arc<FullRun>>,
    pairs: Vec<Vec<MjPair>>,
    part: usize,
    pos: usize,
}

impl MjSortAdopted {
    fn total_pairs(&self) -> usize {
        self.pairs.iter().map(Vec::len).sum()
    }

    /// Next pair in partition-concatenation order; `None` = drained.
    #[inline]
    fn next_pair(&mut self) -> Option<MjPair> {
        loop {
            let p = self.pairs.get(self.part)?;
            match p.get(self.pos) {
                Some(&pair) => {
                    self.pos += 1;
                    return Some(pair);
                }
                None => {
                    self.part += 1;
                    self.pos = 0;
                }
            }
        }
    }
}

/// Refusal diagnosis trace (PGRUST_LANE_V2_TRACE only; emitted only once
/// the knob armed — default-OFF sessions stay silent). No router taxonomy
/// mint: the car keeps route_to=legacy until its fleet letter (the
/// tier-2 knob-path-finish pattern, m5-coverage rows 66/91).
#[cold]
fn refused(reason: &str) {
    lane_trace(&format!("runtime-mjsort: refused ({reason})"));
}

/// The MJSORT drive: called from the mergejoin dispatch arm head. Returns
/// `None` = not owned (the caller falls through byte-identically);
/// `Some(row)` = the adopted emit face served one pull.
///
/// Probe-once law: only the FIRST pull of a node can engage (`mj.mjsort_
/// probed`) — a refused first pull is STICKY, so the FSM's stream can
/// never be double-fed by a mid-stream engagement. The children stay
/// pristine through probe AND both sort engagements (publish mode never
/// touches SortState), so every refusal/fallback path leaves the Volcano
/// FSM a virgin tree.
pub(crate) fn try_own_merge_join_mjsort<'mcx>(
    mj: &mut MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Knob head: one relaxed load + compare on the default path.
    if !mjsort_enabled() {
        return Ok(None);
    }
    if mj.mjsort.is_some() {
        return emit_next(mj, estate).map(Some);
    }
    if mj.mjsort_probed {
        return Ok(None);
    }
    mj.mjsort_probed = true;
    match probe_and_engage(mj, estate)? {
        true => emit_next(mj, estate).map(Some),
        false => Ok(None),
    }
}

/// Serve one adopted pair as the node's projected output row.
fn emit_next<'mcx>(
    mj: &mut MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // The FSM runs CFI at drive entry; the adopted face keeps the cadence.
    ::postgres_seams::check_for_interrupts::call()?;
    let adopted = mj.mjsort.as_mut().expect("adopted state present");
    let Some((orun, obr, irun, ibr)) = adopted.next_pair() else {
        return Ok(None);
    };
    let (oslot, islot) = {
        let PlanStateNode::Sort(o) = &*mj.outer else { unreachable!("admitted Sort outer") };
        let PlanStateNode::Sort(i) = &*mj.inner else { unreachable!("admitted Sort inner") };
        (o.state.ps_ResultTupleSlot, i.state.ps_ResultTupleSlot)
    };
    // Materialize both child rows as virtual tuples in the Sort children's
    // result slots (the runtime-full emit idiom: datum copy; byref cells
    // point into the adopted run arenas, alive until node reset/end).
    let mcx = estate.es_query_cxt;
    for (slot_id, runs, run, bufrow) in [
        (oslot, &adopted.oruns, orun, obr),
        (islot, &adopted.iruns, irun, ibr),
    ] {
        let (values, nulls) = runs[run as usize].buf.row(bufrow as usize);
        let natts = values.len();
        let slot = estate.slot_mut(slot_id);
        ::exectuples::exec_clear_tuple(slot, mcx);
        {
            let sb = slot.base_mut();
            sb.tts_values[..natts].copy_from_slice(values);
            sb.tts_isnull[..natts].copy_from_slice(nulls);
        }
        ::exectuples::exec_store_virtual_tuple(slot);
    }
    ::nodemergejoin::mjsort_project(&mut mj.state, estate, oslot, islot).map(Some)
}

/// The whole admission battery + three-phase engagement. `Ok(false)` =
/// refused or fell back — nothing consumed, no child state touched, the
/// caller's Volcano FSM runs byte-identically.
fn probe_and_engage<'mcx>(
    mj: &mut MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming (the try_own_sort layering; all cheap).
    let dop = router::arm_dop(ArmClass::Sort);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };
    lane_trace("runtime-mjsort: probed");

    // --- Session / dynamic gates (the sort arm's battery, probed once).
    if estate.es_instrument != 0 || estate.es_epq_active {
        refused("instrumented/epq");
        return Ok(false);
    }
    if estate.es_top_eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) != 0 {
        refused("rewind/backward/mark eflags (adopted emit is forward-once)");
        return Ok(false);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refused("already in parallel machinery");
        return Ok(false);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused("extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refused("no planned stmt");
        return Ok(false);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused("exec params");
        return Ok(false);
    }
    if !estate.es_snapshot.as_deref().is_some_and(::types_snapshot::IsMVCCSnapshot) {
        refused("non-MVCC snapshot");
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable || policy.pending_invalidations
    {
        refused("binder policy sources");
        return Ok(false);
    }

    // --- Join shape (inc-1 envelope).
    let plan = mj.state.plan;
    if plan.join.jointype != ::types_nodes::JoinType::JOIN_INNER {
        refused("jointype (INNER only at inc-1)");
        return Ok(false);
    }
    if ::nodemergejoin::mjsort_has_quals(&mj.state) {
        refused("joinqual/otherqual (mergeclauses-only shapes at inc-1)");
        return Ok(false);
    }
    let nkeys = plan.mergeclauses.iter().count();
    if nkeys == 0 || nkeys > ::nodesort::sink::TOPN_MAX_KEYS {
        refused("mergeclause arity");
        return Ok(false);
    }
    if plan.mergeReversals.iter().any(|&r| r) {
        refused("reversed mergeclause direction");
        return Ok(false);
    }

    // --- Children: two UNSTARTED Sort(SeqScan(pgrcolumnar)) full-sort
    // shapes whose sort keys are exactly the mergeclause pathkeys.
    let (Some(oprobe), Some(iprobe)) = (
        probe_side(&mut mj.outer, estate, dop, nkeys)?,
        probe_side(&mut mj.inner, estate, dop, nkeys)?,
    ) else {
        return Ok(false);
    };
    // Cross-side alignment law: per-column (desc, nulls_first, signed/
    // unsigned class) identical — equal key VALUES then have equal packed
    // words on both sides and both sides run the same direction, which is
    // what makes the prefix-aligned range cut and the group-equality
    // merge exact (mjmerge module doc).
    for k in 0..nkeys {
        let (ok, ik) = (&oprobe.spec_keys[k], &iprobe.spec_keys[k]);
        if ok.desc != ik.desc
            || ok.nulls_first != ik.nulls_first
            || ok.is_unsigned() != ik.is_unsigned()
        {
            refused("cross-side key encoding mismatch");
            return Ok(false);
        }
    }
    let nulls_first: Vec<bool> = oprobe.spec_keys[..nkeys].iter().map(|k| k.nulls_first).collect();

    // --- Pair budget (work_mem-scaled; admission estimate first).
    let pair_bytes = core::mem::size_of::<MjPair>() as u64; // 12 B
    let wm_bytes = (::init_small::globals::work_mem().max(64) as u64) * 1024;
    let dop_max = oprobe.dop.max(iprobe.dop).max(1) as u64;
    let cap = mjsort_pair_cap().unwrap_or(dop_max * wm_bytes / pair_bytes);
    if plan.join.plan.plan_rows.max(0.0) > cap as f64 {
        refused("join-size admission estimate exceeds the pair budget");
        return Ok(false);
    }

    // --- Phase 1 + 2: the two full-sort engagements, PUBLISH mode.
    lane_trace(&format!(
        "runtime-mjsort: engaged nkeys={nkeys} dop_o={} dop_i={} cap={cap}",
        oprobe.dop, iprobe.dop
    ));
    let Some(opub) = full_sort_engage_publish(estate, rt, oprobe)? else {
        refused("outer sort engagement fell back");
        return Ok(false);
    };
    let Some(ipub) = full_sort_engage_publish(estate, rt, iprobe)? else {
        refused("inner sort engagement fell back");
        return Ok(false);
    };

    // --- Phase 3: the range-partitioned merge on the pool.
    let Some(pairs) = merge_phase(rt, &opub, &ipub, nkeys, &nulls_first, cap)? else {
        refused("pair budget crossed / merge fell back");
        return Ok(false);
    };
    let adopted = MjSortAdopted { oruns: opub.runs, iruns: ipub.runs, pairs, part: 0, pos: 0 };
    lane_trace(&format!(
        "runtime-mjsort: complete, pairs={} parts={MJSORT_PARTS}",
        adopted.total_pairs()
    ));
    mj.mjsort = Some(Box::new(adopted));
    Ok(true)
}

/// `PGRUST_RUNTIME_MJSORT_MAXPAIRS` — the pair-budget override (tests /
/// diagnosis; unset = the work_mem-scaled default).
fn mjsort_pair_cap() -> Option<u64> {
    static CAP: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    crate::once_val(&CAP, || {
        std::env::var("PGRUST_RUNTIME_MJSORT_MAXPAIRS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

/// Probe one child as an UNSTARTED full-sort side; `Ok(None)` = refused
/// (traced by name).
fn probe_side<'mcx>(
    child: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    dop: i32,
    nkeys: usize,
) -> PgResult<Option<MjSortSideProbe<'mcx>>> {
    let PlanStateNode::Sort(s) = &mut *child else {
        refused("child not Sort");
        return Ok(None);
    };
    if !s.state.sort_virgin() {
        refused("child Sort already started");
        return Ok(None);
    }
    if s.state.plan.numCols as usize != nkeys {
        refused("sort keys != mergeclause keys");
        return Ok(None);
    }
    let Some(outer_desc) = s.outer_desc.clone() else {
        refused("child Sort has no outer desc");
        return Ok(None);
    };
    let PlanStateNode::SeqScan(ss) = &mut *s.outer else {
        refused("sort child not SeqScan");
        return Ok(None);
    };
    match full_sort_probe_for_mjsort(&s.state, ss, &outer_desc, estate, dop)? {
        Ok(probe) => Ok(Some(probe)),
        Err(reason) => {
            refused(reason);
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 3: the pure-compute merge RG (un-pinned; pool-thread execution).
// ---------------------------------------------------------------------------

/// The merge task set's shared body: owned plain data (Arc'd runs, the
/// boundary list, per-partition output slots). No executor, no session
/// state — pool threads execute it without any binding ceremony.
struct MjMergeShared {
    oruns: Vec<Arc<FullRun>>,
    iruns: Vec<Arc<FullRun>>,
    splitters: Vec<MjPrefix>,
    nkeys: usize,
    nulls_first: Vec<bool>,
    budget: PairBudget,
    /// Slot p written only by partition p's claimer (single-writer sink
    /// contract; the FullShared::out_parts argument, verbatim).
    out: Vec<UnsafeCell<Vec<MjPair>>>,
    /// Budget crossing or a kernel panic: sticky, aborts the RG — the
    /// caller falls back to the serial arm (R5; nothing was emitted).
    broke: AtomicBool,
    rg: std::sync::OnceLock<runtime::WeakRgHandle>,
    claimed: AtomicUsize,
}

// SAFETY: `out` slots follow the single-writer-per-partition contract
// (each partition index is claimed exactly once via the morsel cursor;
// the leader reads only after RG completion) — the FullShared argument.
unsafe impl Sync for MjMergeShared {}

impl MjMergeShared {
    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(runtime::WeakRgHandle::upgrade) {
            rg.abort();
        }
    }
}

impl runtime::TaskSetWork for MjMergeShared {
    fn run_morsel(&self, _worker: usize, range: runtime::MorselRange) {
        for p in range {
            if self.broke.load(Ordering::SeqCst) {
                return;
            }
            self.claimed.fetch_add(1, Ordering::Relaxed);
            let oviews: Vec<&[::nodesort::fullsort::RunEnt]> =
                self.oruns.iter().map(|r| r.entries.as_slice()).collect();
            let iviews: Vec<&[::nodesort::fullsort::RunEnt]> =
                self.iruns.iter().map(|r| r.entries.as_slice()).collect();
            let mut part_out: Vec<MjPair> = Vec::new();
            // The kernel is pure and must not unwind into the scheduler
            // (a stranded pin wedges finalization): a panic or a budget
            // crossing both break the engagement into the serial rerun.
            let ok = catch_unwind(AssertUnwindSafe(|| {
                mjmerge::mj_partition_pairs(
                    &oviews,
                    &iviews,
                    &self.splitters,
                    p as usize,
                    self.nkeys,
                    &self.nulls_first,
                    &self.budget,
                    &mut part_out,
                )
            }));
            match ok {
                Ok(true) => {
                    // SAFETY: single writer for slot p (sink contract).
                    unsafe { *self.out[p as usize].get() = part_out };
                }
                Ok(false) | Err(_) => {
                    self.broke.store(true, Ordering::SeqCst);
                    self.abort_rg();
                    return;
                }
            }
        }
    }

    fn finalize(&self) {}
}

/// One partition index per claim (P is small and partitions are uneven —
/// per-index claims keep the tail short; the sizer never coalesces past a
/// hard boundary).
struct PartsSource {
    parts: u64,
}

impl runtime::MorselSource for PartsSource {
    fn total_granules(&self) -> u64 {
        self.parts
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(self.parts)
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

/// Run the merge task set on the pool; `Ok(None)` = broke (budget/panic)
/// — the caller falls back to the serial arm.
fn merge_phase(
    rt: &'static Arc<runtime::Runtime>,
    opub: &FullPublish,
    ipub: &FullPublish,
    nkeys: usize,
    nulls_first: &[bool],
    cap: u64,
) -> PgResult<Option<Vec<Vec<MjPair>>>> {
    let oviews: Vec<&[::nodesort::fullsort::RunEnt]> =
        opub.runs.iter().map(|r| r.entries.as_slice()).collect();
    let iviews: Vec<&[::nodesort::fullsort::RunEnt]> =
        ipub.runs.iter().map(|r| r.entries.as_slice()).collect();
    let splitters = mjmerge::mj_splitters(&oviews, &iviews, MJSORT_PARTS);
    let shared = Arc::new(MjMergeShared {
        oruns: opub.runs.clone(),
        iruns: ipub.runs.clone(),
        splitters,
        nkeys,
        nulls_first: nulls_first.to_vec(),
        budget: PairBudget::new(cap),
        out: (0..MJSORT_PARTS).map(|_| UnsafeCell::new(Vec::new())).collect(),
        broke: AtomicBool::new(false),
        rg: std::sync::OnceLock::new(),
        claimed: AtomicUsize::new(0),
    });
    static NEXT_QUERY_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let (rg, waiter) = rt.submit_with_affinity(
        runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst),
            tasksets: vec![runtime::TaskSetSpec {
                source: Arc::new(PartsSource { parts: MJSORT_PARTS as u64 }),
                work: Arc::clone(&shared) as _,
                deps: vec![],
            }],
        },
        router::session_affinity_token(),
    );
    shared.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
    // Submit-and-park with the CFI cadence. The RG is UN-PINNED: pool
    // threads (which exist — runtime::global() gated admission) claim and
    // finish it; aborts settle at morsel boundaries, so both the error
    // and interrupt paths below wait for the completion to land before
    // returning (the shared body is Arc'd, but a settled RG is the clean
    // invariant every arm keeps).
    let outcome = loop {
        if let Some(o) = waiter.try_wait() {
            break o;
        }
        if let Err(e) = ::postgres_seams::check_for_interrupts::call() {
            rg.abort();
            let _ = waiter.wait();
            return Err(e);
        }
        std::thread::sleep(std::time::Duration::from_micros(200));
    };
    if outcome == runtime::RgOutcome::Aborted {
        if shared.broke.load(Ordering::SeqCst) {
            lane_trace(&format!(
                "runtime-mjsort: merge broke (budget/panic) after {} claims",
                shared.claimed.load(Ordering::Relaxed)
            ));
            return Ok(None);
        }
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "runtime mjsort merge aborted")));
    }
    lane_trace(&format!(
        "runtime-mjsort: merge complete ({} partition claims)",
        shared.claimed.load(Ordering::Relaxed)
    ));
    // Harvest the partition outputs by TAKE, not Arc::try_unwrap — the
    // settled RG's slot structures may still hold work Arcs until slot
    // reuse. SAFETY: the RG completed (every claim finalized, completion
    // synchronizes-with this thread via the waiter), so the single-writer
    // slots are quiescent and the leader is the sole reader/taker.
    Ok(Some(
        shared
            .out
            .iter()
            .map(|cell| unsafe { core::mem::take(&mut *cell.get()) })
            .collect(),
    ))
}
