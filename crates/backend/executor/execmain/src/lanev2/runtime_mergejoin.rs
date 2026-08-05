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

/// `PGRUST_RUNTIME_MJSORT_FOLD` — the duplicate-band fold lever
/// (GL-MJSORT-3 §3: partial-agg/consumer pushdown into the merge
/// partitions — the dup band's serial O(output) adopted-pair emit is the
/// bound the clean gather arm never pays; the fold moves the bound by
/// consuming the pairs INSIDE the pool). **DEFAULT OFF**; ON iff exactly
/// `1`/`on` (the t35 exact-spelling law), layered UNDER the car's kill —
/// FOLD without MJSORT is inert (the arm re-checks `mjsort_enabled`
/// first). 0 = unresolved, 1 = OFF, 2 = ON (the AtomicU8 same-process A/B
/// idiom). Env-var, not GUC, per the standing `pg_settings` byte-identity
/// discipline.
static MJFOLD: AtomicU8 = AtomicU8::new(0);

#[inline]
fn mjfold_enabled() -> bool {
    match MJFOLD.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => mjfold_resolve(),
    }
}

/// The default-OFF exact-spelling law, isolated for the unit pins: ON iff
/// EXACTLY `1`/`on` (every other spelling, including unset, is OFF).
#[inline]
fn mjfold_spelling_on(v: Option<&str>) -> bool {
    matches!(v, Some("1") | Some("on"))
}

#[cold]
#[inline(never)]
fn mjfold_resolve() -> bool {
    let on = mjfold_spelling_on(std::env::var("PGRUST_RUNTIME_MJSORT_FOLD").ok().as_deref());
    MJFOLD.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

#[cfg(test)]
mod knob_tests {
    use super::{mjfold_spelling_on, mjsort_spelling_on};

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

    /// GL-MJSORT-FOLD posture: DEFAULT OFF, ON iff exactly `1`/`on`
    /// (the t35 exact-spelling law — no other spelling arms).
    #[test]
    fn mjfold_default_off_spelling() {
        assert!(!mjfold_spelling_on(None), "unset must be OFF (default-OFF lever)");
        for v in ["0", "off", "", "true", "ON", "yes", "2", "On"] {
            assert!(!mjfold_spelling_on(Some(v)), "spelling {v:?} must stay OFF");
        }
        assert!(mjfold_spelling_on(Some("1")));
        assert!(mjfold_spelling_on(Some("on")));
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
    if super::runtime_in_parallel_role() {
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

// ---------------------------------------------------------------------------
// GL-MJSORT-FOLD — phase 4: partial-agg/consumer pushdown into the merge
// partitions (GL-MJSORT-3 §3/§3.1). An AGG-level ownership exactly like the
// hashjoin arm: the Agg dispatch offers its MergeJoin child here; phases
// 1-3 run the car's own engagement verbatim, then a SECOND un-pinned
// pure-compute task set folds each partition's pairs through the per-row
// transition program into ONE partial per partition; the leader combines
// the 256 partials, seeds the Agg's pergroup state through the EXPLICIT
// absorb (nodeagg::runtime_partial::exec_agg_explicit_partials), and the
// Agg emits its single row through its own projection. The MJ node is
// never pulled; children stay pristine (R5).
//
// THE FOLD-BREAK FALLBACK (the design discovery of record): on ANY
// fold-phase break (int4pl/int8pl/int4mi/int8mi overflow, kernel panic)
// the arm REINSTATES the adopted emit face on the MJ node (virgin —
// part==0 && pos==0, the fold never advances the cursor) and returns
// None. The per-tuple Agg then drives the MJ node, whose own dispatch
// hook sees the adopted face and serves the SAME pairs serially — the
// plain car path, byte-identical, and C's exact overflow error surfaces
// from the Agg's own transition program at the same input row. R5 purity
// holds WITHOUT rerunning the sorts, and no C error string is ever
// duplicated here.
// ---------------------------------------------------------------------------

use ::nodeagg::runtime_partial::{
    agg_mjfold_recognize, exec_agg_explicit_partials, MjFoldKind, MjFoldTrans,
    RuntimePartialTrans,
};
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::{INNER_VAR, OUTER_VAR};

// The admitted arg-arithmetic operators (pg_proc REL 18.3, verified
// vendored): C's checked int addition/subtraction — an overflow here is
// C's "integer out of range", which the fold NEVER raises itself (the
// fold-break fallback reruns per-tuple and C's own function errors at the
// same row).
const F_INT4PL: u32 = 177;
const F_INT4MI: u32 = 181;
const F_INT8PL: u32 = 463;
const F_INT8MI: u32 = 464;
const FOLD_INT2OID: u32 = 21;
const FOLD_INT4OID: u32 = 23;
const FOLD_INT8OID: u32 = 20;

/// A resolved run-row column read: which side's run arena, which column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FoldSrc {
    Outer(u16),
    Inner(u16),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FoldWidth {
    I16,
    I32,
    I64,
}

/// One transition's resolved argument program (plain data — pool threads
/// touch only owned run arenas + pairs + fold state, the merge RG's law
/// extended by the transition kernels).
#[derive(Clone, Copy, Debug)]
enum FoldArg {
    /// count(*) — no argument, every pair counts.
    None,
    /// A side column read, sign-extended at the Var's width. NULL skips
    /// (every admitted transfn is strict; count(x) counts non-NULL).
    Col { src: FoldSrc, width: FoldWidth },
    /// CHECKED two-column int arithmetic (int4pl/int4mi/int8pl/int8mi):
    /// either side NULL => arg NULL (strict operators); overflow BREAKS
    /// the fold into the per-tuple rerun.
    Op { sub: bool, wide: bool, a: FoldSrc, b: FoldSrc },
}

/// One transition's whole fold program.
#[derive(Clone, Copy, Debug)]
struct FoldSpec {
    transno: u16,
    kind: MjFoldKind,
    arg: FoldArg,
}

/// One partition's per-transition accumulator (the RuntimePartialTrans
/// vocabulary, pre-combine).
#[derive(Clone, Copy, Debug)]
enum FoldAcc {
    Count(i64),
    Sum { v: i64, present: bool },
    Int128 { n: i64, sum: i128, present: bool },
    MinMax { v: i64, present: bool },
}

fn acc_init(kind: MjFoldKind) -> FoldAcc {
    match kind {
        MjFoldKind::CountStar | MjFoldKind::CountAny => FoldAcc::Count(0),
        MjFoldKind::Sum => FoldAcc::Sum { v: 0, present: false },
        MjFoldKind::Int128 => FoldAcc::Int128 { n: 0, sum: 0, present: false },
        MjFoldKind::Min | MjFoldKind::Max => FoldAcc::MinMax { v: 0, present: false },
    }
}

/// Cross-partition combine (order-insensitive-exact for every admitted
/// kind — the runtime-partial reassociation law verbatim).
fn acc_combine(kind: MjFoldKind, dst: &mut FoldAcc, src: FoldAcc) {
    match (dst, src) {
        (FoldAcc::Count(x), FoldAcc::Count(y)) => *x = x.wrapping_add(y),
        (FoldAcc::Sum { v: x, present: px }, FoldAcc::Sum { v: y, present: py }) => {
            *x = x.wrapping_add(y);
            *px = *px || py;
        }
        (
            FoldAcc::Int128 { n: nx, sum: sx, present: px },
            FoldAcc::Int128 { n: ny, sum: sy, present: py },
        ) => {
            *nx += ny;
            *sx += sy;
            *px = *px || py;
        }
        (FoldAcc::MinMax { v: x, present: px }, FoldAcc::MinMax { v: y, present: py }) => {
            *x = match (*px, py) {
                (true, true) => match kind {
                    MjFoldKind::Min => (*x).min(y),
                    MjFoldKind::Max => (*x).max(y),
                    _ => unreachable!("minmax combine over a non-minmax kind"),
                },
                (true, false) => *x,
                (false, _) => y,
            };
            *px = *px || py;
        }
        _ => unreachable!("mismatched fold accumulator shapes for one transno"),
    }
}

/// The combined accumulator in the leader absorb's vocabulary.
fn acc_to_partial(acc: FoldAcc) -> RuntimePartialTrans {
    match acc {
        FoldAcc::Count(n) => RuntimePartialTrans::Count(n),
        FoldAcc::Sum { v, present } => RuntimePartialTrans::Sum { v, present },
        FoldAcc::Int128 { n, sum, present } => RuntimePartialTrans::Int128 { n, sum, present },
        FoldAcc::MinMax { v, present } => RuntimePartialTrans::Fold { v, present },
    }
}

/// Fold refusal: named in the router taxonomy under the car's class (the
/// borrowed DOP source is a NAMED debt of the car already — the fold
/// inherits it) + the armed trace channel.
#[cold]
fn fold_refused(reason: &'static str) {
    router::tick_refused(ArmClass::Sort, reason);
    lane_trace(&format!("runtime-mjsort-fold: refused ({reason})"));
}

/// MJ tlist entry k (1-based Var attno) — setrefs keeps the tlist in resno
/// order; the resno equality is the belt.
fn tlist_expr<'mcx>(
    tlist: &::types_nodes::list::NodeList<'mcx>,
    attno: i32,
) -> Option<Node<'mcx>> {
    if attno < 1 {
        return None;
    }
    let tle = tlist.iter().nth(attno as usize - 1)?.as_target_entry()?;
    if tle.resno as i32 != attno {
        return None;
    }
    Some(tle.expr)
}

/// A resolved SIDE Var — a Var over the MJ's children (the Sort outputs).
/// Run row col j-1 is exact: the car's own emit copies run rows 1:1 into
/// the Sort children's result slots and `mjsort_project` evaluates the MJ
/// tlist over them — the fold reads the same columns the projection would.
fn side_var(expr: Node<'_>) -> Option<(FoldSrc, FoldWidth, u32)> {
    let v = expr.as_var()?;
    if v.varlevelsup != 0 || v.varattno < 1 {
        return None;
    }
    let width = match v.vartype {
        FOLD_INT2OID => FoldWidth::I16,
        FOLD_INT4OID => FoldWidth::I32,
        FOLD_INT8OID => FoldWidth::I64,
        _ => return None,
    };
    let col = (v.varattno - 1) as u16;
    let src = match v.varno {
        OUTER_VAR => FoldSrc::Outer(col),
        INNER_VAR => FoldSrc::Inner(col),
        _ => return None,
    };
    Some((src, width, v.vartype))
}

/// One admitted OpExpr: int4pl/int4mi/int8pl/int8mi over two operands the
/// `resolve` closure lands on side Vars of the op's width.
fn classify_op<'mcx>(
    op: &::types_nodes::primnodes::OpExpr<'mcx>,
    resolve: impl Fn(Node<'mcx>) -> Option<(FoldSrc, FoldWidth, u32)>,
) -> Option<(FoldArg, u32)> {
    if op.opretset || op.args.len() != 2 {
        return None;
    }
    let (sub, wide, want) = match op.opfuncid {
        F_INT4PL => (false, false, FOLD_INT4OID),
        F_INT4MI => (true, false, FOLD_INT4OID),
        F_INT8PL => (false, true, FOLD_INT8OID),
        F_INT8MI => (true, true, FOLD_INT8OID),
        _ => return None,
    };
    if op.opresulttype != want {
        return None;
    }
    let mut it = op.args.iter();
    let (a, b) = (it.next()?, it.next()?);
    let (asrc, _, at) = resolve(a)?;
    let (bsrc, _, bt) = resolve(b)?;
    if at != want || bt != want {
        return None;
    }
    Some((FoldArg::Op { sub, wide, a: asrc, b: bsrc }, want))
}

/// GL-MJSORT-3 §3.1 seam 3 — resolve one agg argument against the MJ
/// tlist. Substitute Var(OUTER, k) with tlist entry k-1's expr — BOTH
/// planner spellings land on the same resolved form: an expression in the
/// agg arg over a simple-Var MJ tlist, AND the scanjoin-target-pushed
/// expression IN the MJ tlist. Classify the resolved tree: a side Var, or
/// an admitted OpExpr over two side Vars. Anything else refuses by name.
fn resolve_arg<'mcx>(
    expr: Node<'mcx>,
    tlist: &::types_nodes::list::NodeList<'mcx>,
) -> Option<(FoldArg, u32)> {
    // The substitution: an agg-level Var addresses the MJ tlist.
    let subst = |e: Node<'mcx>| -> Option<Node<'mcx>> {
        let v = e.as_var()?;
        if v.varno != OUTER_VAR || v.varlevelsup != 0 {
            return None;
        }
        tlist_expr(tlist, v.varattno as i32)
    };
    if expr.as_var().is_some() {
        // Bare Var over the MJ tlist: substitute, then classify — the
        // pushed spelling surfaces here (the tlist entry may itself be the
        // OpExpr, whose operand Vars are ALREADY side Vars).
        let sub = subst(expr)?;
        if sub.as_var().is_some() {
            let (src, width, t) = side_var(sub)?;
            return Some((FoldArg::Col { src, width }, t));
        }
        if let Some(op) = sub.as_op_expr() {
            return classify_op(op, side_var);
        }
        return None;
    }
    if let Some(op) = expr.as_op_expr() {
        // Expression in the agg arg: each operand is an agg-level Var to
        // substitute, and the substituted entry must be a simple side Var.
        return classify_op(op, |e| side_var(subst(e)?));
    }
    None
}

/// The whole per-transition resolution, fail-closed. `natts_o`/`natts_i`
/// bound the run-row column indices (a Var past the child's output would
/// read out of bounds — structurally impossible after setrefs, checked
/// anyway).
fn resolve_fold_args<'mcx>(
    recog: &[MjFoldTrans<'mcx>],
    tlist: &::types_nodes::list::NodeList<'mcx>,
    natts_o: usize,
    natts_i: usize,
) -> Option<Vec<FoldSpec>> {
    let src_ok = |s: FoldSrc| match s {
        FoldSrc::Outer(c) => (c as usize) < natts_o,
        FoldSrc::Inner(c) => (c as usize) < natts_i,
    };
    let arg_ok = |a: &FoldArg| match *a {
        FoldArg::None => true,
        FoldArg::Col { src, .. } => src_ok(src),
        FoldArg::Op { a, b, .. } => src_ok(a) && src_ok(b),
    };
    let mut out = Vec::with_capacity(recog.len());
    for t in recog {
        let arg = match (t.kind, t.arg) {
            (MjFoldKind::CountStar, None) => FoldArg::None,
            (MjFoldKind::CountStar, Some(_)) | (_, None) => return None,
            (kind, Some(e)) => {
                let (arg, rtype) = resolve_arg(e, tlist)?;
                // Type law: the resolved expression's type must be exactly
                // the aggregate's declared input. count(x) declares ANY —
                // there the int-family restriction inside the resolver IS
                // the envelope (only NULLness is read for Col args; Op
                // args evaluate checked).
                if kind != MjFoldKind::CountAny && rtype != t.arg_type {
                    return None;
                }
                arg
            }
        };
        if !arg_ok(&arg) {
            return None;
        }
        out.push(FoldSpec { transno: t.transno, kind: t.kind, arg });
    }
    Some(out)
}

/// One side column read, strict-NULL: `None` = the arg is NULL (skip).
#[inline]
fn read_col(
    src: FoldSrc,
    width: FoldWidth,
    ovals: &[::datum::Datum],
    onulls: &[bool],
    ivals: &[::datum::Datum],
    inulls: &[bool],
) -> Option<i64> {
    let (vals, nulls, col) = match src {
        FoldSrc::Outer(c) => (ovals, onulls, c as usize),
        FoldSrc::Inner(c) => (ivals, inulls, c as usize),
    };
    if nulls[col] {
        return None;
    }
    Some(match width {
        FoldWidth::I16 => vals[col].as_i16() as i64,
        FoldWidth::I32 => vals[col].as_i32() as i64,
        FoldWidth::I64 => vals[col].as_i64(),
    })
}

/// One partition's pairs through the whole transition program. `false` =
/// arithmetic break (overflow) — the engagement falls back to the
/// per-tuple rerun where C's own function raises at the same row.
fn fold_partition(
    oruns: &[Arc<FullRun>],
    iruns: &[Arc<FullRun>],
    pairs: &[MjPair],
    prog: &[FoldSpec],
    accs: &mut [FoldAcc],
) -> bool {
    for &(orun, obr, irun, ibr) in pairs {
        let (ovals, onulls) = oruns[orun as usize].buf.row(obr as usize);
        let (ivals, inulls) = iruns[irun as usize].buf.row(ibr as usize);
        for (spec, acc) in prog.iter().zip(accs.iter_mut()) {
            let v = match spec.arg {
                FoldArg::None => Some(0),
                FoldArg::Col { src, width } => {
                    read_col(src, width, ovals, onulls, ivals, inulls)
                }
                FoldArg::Op { sub, wide, a, b } => {
                    let w = if wide { FoldWidth::I64 } else { FoldWidth::I32 };
                    match (
                        read_col(a, w, ovals, onulls, ivals, inulls),
                        read_col(b, w, ovals, onulls, ivals, inulls),
                    ) {
                        // Strict operator: either side NULL => arg NULL.
                        (None, _) | (_, None) => None,
                        (Some(av), Some(bv)) => {
                            // C's exact checked arithmetic at the op's width.
                            let r = if wide {
                                if sub { av.checked_sub(bv) } else { av.checked_add(bv) }
                            } else {
                                let (a32, b32) = (av as i32, bv as i32);
                                let r32 =
                                    if sub { a32.checked_sub(b32) } else { a32.checked_add(b32) };
                                r32.map(|r| r as i64)
                            };
                            match r {
                                Some(r) => Some(r),
                                None => return false, // overflow => fold-break
                            }
                        }
                    }
                }
            };
            match (v, &mut *acc) {
                (None, _) => {} // strict transfn skips a NULL arg
                (Some(_), FoldAcc::Count(n)) => *n = n.wrapping_add(1),
                (Some(v), FoldAcc::Sum { v: s, present }) => {
                    // C int2_sum/int4_sum: UNCHECKED int8 accumulate.
                    *s = s.wrapping_add(v);
                    *present = true;
                }
                (Some(v), FoldAcc::Int128 { n, sum, present }) => {
                    // C do_int128_accum: n += 1, sum += (int128) v.
                    *n += 1;
                    *sum += v as i128;
                    *present = true;
                }
                (Some(v), FoldAcc::MinMax { v: m, present }) => {
                    *m = if !*present {
                        v
                    } else {
                        match spec.kind {
                            MjFoldKind::Min => (*m).min(v),
                            MjFoldKind::Max => (*m).max(v),
                            _ => unreachable!("minmax accumulate over a non-minmax kind"),
                        }
                    };
                    *present = true;
                }
            }
        }
    }
    true
}

/// The fold task set's shared body: owned plain data (Arc'd runs, the
/// per-partition pair lists + accumulator slots, the transition program).
/// No executor, no session state — pool threads execute it without any
/// binding ceremony (the MjMergeShared law extended by the transition
/// kernels).
struct MjFoldShared {
    oruns: Vec<Arc<FullRun>>,
    iruns: Vec<Arc<FullRun>>,
    prog: Vec<FoldSpec>,
    /// Slot p: (partition p's pairs [read-only during the RG], the
    /// claimer's accumulators). Written only by partition p's claimer; the
    /// leader takes back after the RG settles (the MjMergeShared
    /// single-writer/TAKE discipline).
    parts: Vec<UnsafeCell<(Vec<MjPair>, Vec<FoldAcc>)>>,
    /// Arithmetic break (overflow) or a kernel panic: sticky, aborts the
    /// RG — the caller reinstates the adopted emit face (fold-break
    /// fallback; nothing was emitted, R5 without rerunning the sorts).
    broke: AtomicBool,
    rg: std::sync::OnceLock<runtime::WeakRgHandle>,
    claimed: AtomicUsize,
}

// SAFETY: `parts` slots follow the single-accessor-per-partition contract
// (each partition index is claimed exactly once via the morsel cursor; the
// leader reads only after RG completion) — the MjMergeShared argument.
unsafe impl Sync for MjFoldShared {}

impl MjFoldShared {
    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(runtime::WeakRgHandle::upgrade) {
            rg.abort();
        }
    }
}

impl runtime::TaskSetWork for MjFoldShared {
    fn run_morsel(&self, _worker: usize, range: runtime::MorselRange) {
        for p in range {
            if self.broke.load(Ordering::SeqCst) {
                return;
            }
            self.claimed.fetch_add(1, Ordering::Relaxed);
            // SAFETY: single accessor for slot p (sink contract above).
            let slot = unsafe { &mut *self.parts[p as usize].get() };
            let mut accs: Vec<FoldAcc> = self.prog.iter().map(|s| acc_init(s.kind)).collect();
            // The kernel must not unwind into the scheduler (a stranded pin
            // wedges finalization): a panic and an arithmetic break both
            // break the engagement into the per-tuple rerun.
            let ok = catch_unwind(AssertUnwindSafe(|| {
                fold_partition(&self.oruns, &self.iruns, &slot.0, &self.prog, &mut accs)
            }));
            match ok {
                Ok(true) => slot.1 = accs,
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

/// The fold phase's outcome: combined per-transno partials, or the adopted
/// state handed back VIRGIN for the per-tuple rerun (fold-break fallback).
enum FoldOutcome {
    Folded(Vec<(u16, RuntimePartialTrans)>),
    Broke(Box<MjSortAdopted>),
}

/// Run the fold task set on the pool (mirrors `merge_phase` verbatim:
/// TaskSetWork over PartsSource{256}, un-pinned submit_with_affinity, CFI
/// park loop, abort => broke => the fallback outcome).
fn fold_phase(
    rt: &'static Arc<runtime::Runtime>,
    adopted: Box<MjSortAdopted>,
    prog: &[FoldSpec],
) -> PgResult<FoldOutcome> {
    let MjSortAdopted { oruns, iruns, pairs, .. } = *adopted;
    let shared = Arc::new(MjFoldShared {
        oruns,
        iruns,
        prog: prog.to_vec(),
        parts: pairs.into_iter().map(|p| UnsafeCell::new((p, Vec::new()))).collect(),
        broke: AtomicBool::new(false),
        rg: std::sync::OnceLock::new(),
        claimed: AtomicUsize::new(0),
    });
    static NEXT_QUERY_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let (rg, waiter) = rt.submit_with_affinity(
        runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst),
            tasksets: vec![runtime::TaskSetSpec {
                source: Arc::new(PartsSource { parts: shared.parts.len() as u64 }),
                work: Arc::clone(&shared) as _,
                deps: vec![],
            }],
        },
        router::session_affinity_token(),
    );
    shared.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
    // Submit-and-park with the CFI cadence (the merge RG's own loop; both
    // the error and interrupt paths wait for the completion to land).
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
    // SAFETY (all arms below): the RG settled (completion/abort
    // synchronizes-with this thread via the waiter), so the single-accessor
    // slots are quiescent and the leader is the sole reader/taker — the
    // merge_phase TAKE discipline.
    //
    // BROKE FIRST, outcome second: a break that lands after the last
    // partition was already CLAIMED can settle the RG as Completed (the
    // remaining claims observe `broke` and return without writing their
    // accumulator slots) — harvesting that world would absorb an empty
    // fold silently. Any broke, under either outcome, is the fallback.
    if shared.broke.load(Ordering::SeqCst) {
        lane_trace(&format!(
            "runtime-mjsort-fold: broke (overflow/panic) after {} claims",
            shared.claimed.load(Ordering::Relaxed)
        ));
        // Reassemble the adopted state VIRGIN (part==0 && pos==0 — the
        // fold never advances the emit cursor): the per-tuple Agg then
        // drives the SAME pairs through the plain car serially (the pairs
        // were never moved out of the slots, only read).
        let pairs: Vec<Vec<MjPair>> = shared
            .parts
            .iter()
            .map(|cell| unsafe { core::mem::take(&mut (*cell.get()).0) })
            .collect();
        return Ok(FoldOutcome::Broke(Box::new(MjSortAdopted {
            oruns: shared.oruns.clone(),
            iruns: shared.iruns.clone(),
            pairs,
            part: 0,
            pos: 0,
        })));
    }
    if outcome == runtime::RgOutcome::Aborted {
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "runtime mjsort fold aborted")));
    }
    // Clean completion: every partition claimed exactly once and every
    // claimer wrote its accumulators — combine the 256 partials
    // (order-insensitive-exact per kind) into one explicit layout.
    let mut totals: Vec<FoldAcc> = prog.iter().map(|s| acc_init(s.kind)).collect();
    for cell in shared.parts.iter() {
        let accs = unsafe { core::mem::take(&mut (*cell.get()).1) };
        if accs.len() != totals.len() {
            // A clean-completed RG with an unwritten slot is a scheduler
            // invariant break — never absorb it silently.
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime mjsort fold: missing partition accumulators",
            )));
        }
        for (i, (spec, a)) in prog.iter().zip(accs.into_iter()).enumerate() {
            acc_combine(spec.kind, &mut totals[i], a);
        }
    }
    Ok(FoldOutcome::Folded(
        prog.iter()
            .zip(totals.into_iter())
            .map(|(s, t)| (s.transno, acc_to_partial(t)))
            .collect(),
    ))
}

/// The GL-MJSORT-FOLD dispatch arm: AGG-level ownership of a plain Agg
/// over an admissible MergeJoin (the hashjoin arm's posture). Returns
/// `None` = not owned — the caller falls through byte-identically, and the
/// MJ node's own dispatch hook (the plain car) remains fully armed.
///
/// ARM ORDERING LAW (GL-MJSORT-3 §3.1 seam 5): (a) knob + kill layering,
/// (b) done-repulls are not offers, (c) ALL fold-specific gates BEFORE
/// touching the MJ probe — a fold refusal must leave `mjsort_probed`
/// unset so the plain car still engages via the MJ's own hook, (d) the
/// car's probe_and_engage (phases 1-3, existing admission verbatim),
/// (e) take the adopted result, fold, absorb, emit.
pub(crate) fn try_own_agg_over_merge_join<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    mj: &mut MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // (a) Knob head: the fold rides UNDER the car's kill (FOLD without
    // MJSORT is inert) — two relaxed loads on the armed path, one on the
    // default path (the knob is default OFF).
    if !mjfold_enabled() || !mjsort_enabled() {
        return Ok(None);
    }
    // Arming (the car's own source — the borrowed-DOP named debt): unarmed
    // sessions exit silently before any recognizer work.
    if router::arm_dop(ArmClass::Sort) <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // (b) Done-repulls are not offers (the post-completion pull exits via
    // exec_agg's own agg_done guard, byte-identically).
    if ::nodeagg::agg_is_done(agg) {
        return Ok(None);
    }
    // Repulls are not offers either: once the MJ node was probed (this
    // arm's own engagement path or the MJ hook's sticky probe) or carries
    // an adopted face (including a fold-break reinstatement), the fold's
    // decision for this node is made — exit SILENTLY. Grouped/streaming
    // parents repull the Agg once per output row; a per-pull refusal tick
    // would flood the taxonomy, and re-folding a reinstated face would
    // re-break (the fallback design wants the per-tuple drive). This also
    // enforces the virgin law: the fold only ever takes the adopted state
    // its own fresh engagement produced (part==0 && pos==0 by
    // construction — nothing was emitted, R5).
    if mj.mjsort_probed || mj.mjsort.is_some() {
        return Ok(None);
    }
    // (c) Fold-specific gates, ALL before the MJ probe.
    if !::nodeagg::agg_plain_perrow_admissible(agg) {
        fold_refused("fold: agg shape");
        return Ok(None);
    }
    let Some(recog) = agg_mjfold_recognize(agg)? else {
        fold_refused("fold: agg shape");
        return Ok(None);
    };
    // Peek the children for the column bound (Sort output widths) WITHOUT
    // touching them; non-Sort children refuse here and again, by the car's
    // own name, at its probe.
    let (natts_o, natts_i) = {
        let (PlanStateNode::Sort(o), PlanStateNode::Sort(i)) = (&*mj.outer, &*mj.inner) else {
            fold_refused("fold: arg resolution");
            return Ok(None);
        };
        (
            o.state.plan.plan.targetlist.len(),
            i.state.plan.plan.targetlist.len(),
        )
    };
    let tlist = &mj.state.plan.join.plan.targetlist;
    let Some(prog) = resolve_fold_args(&recog, tlist, natts_o, natts_i) else {
        fold_refused("fold: arg resolution");
        return Ok(None);
    };
    // (d) The car's whole admission battery + phases 1-3, verbatim. The
    // probe-once law is shared with the MJ hook: the repull gate above
    // proved this IS the node's first pull.
    mj.mjsort_probed = true;
    if !probe_and_engage(mj, estate)? {
        return Ok(None);
    }
    // (e) Take the adopted result, fold on the pool, absorb, emit.
    let adopted = mj.mjsort.take().expect("engaged adopted state present");
    // R5 virgin law: a fresh engagement never emitted anything.
    debug_assert!(adopted.part == 0 && adopted.pos == 0);
    let total_pairs = adopted.total_pairs();
    lane_trace(&format!("runtime-mjsort-fold: engaged pairs={total_pairs}"));
    match fold_phase(rt, adopted, &prog)? {
        FoldOutcome::Folded(combined) => {
            let r = exec_agg_explicit_partials(agg, estate, &combined)?;
            lane_trace(&format!("runtime-mjsort-fold: complete pairs={total_pairs}"));
            Ok(Some(r))
        }
        FoldOutcome::Broke(adopted) => {
            // FOLD-BREAK FALLBACK: reinstate the (virgin) adopted emit face
            // and decline ownership — the per-tuple Agg drives the same
            // pairs through the plain car serially, and C's exact error
            // (if the break was an overflow) surfaces from the Agg's own
            // transition program at the same input row.
            mj.mjsort = Some(adopted);
            fold_refused("fold: kernel broke");
            Ok(None)
        }
    }
}
