//! Lane-v2 hash-grouped exact-DISTINCT aggregation — the uniqexact2 grouped
//! narrow-sort arm's named follow-up (lane-v2-distincthash). For the
//! sorted grouped exact-DISTINCT plan shape — `Sort(group cols, distinct arg) →
//! GroupAggregate(aggpresorted DISTINCT)` — the narrow-sort arm already
//! deletes the distinct-arg SUFFIX compares, but the group-prefix sort over
//! ALL input rows remains the dominant cost. This arm deletes that sort too:
//! rows group through a hash table whose every entry owns the group's
//! order-insensitive transition state plus one exact-DISTINCT set per
//! internal-sort entry (`distinctset::DistinctSet`, reused wholesale), and
//! the finalize orders the GROUPS (not the rows) by the plan Sort's prefix
//! before emitting through the unchanged finalize/HAVING/project tail.
//!
//! Byte identity vs the C path (and vs the narrow-sort arm, which is itself
//! byte-identical to C):
//!   * same groups: the group hash key is the representational image of the
//!     grouping columns (admission requires `group_eq_representational` AND
//!     integer-or-text group columns). Integer keys: word equality == the
//!     grouping equality operator's verdict. TEXT/VARCHAR keys: the stored
//!     image is the detoasted content bytes, and the grouping equality is
//!     `texteq` under a DETERMINISTIC collation (the representational
//!     admission's texteq arm), which IS length+memcmp of those bytes — so
//!     byte equality == the operator's verdict. NULL keys collapse to
//!     same-group exactly as C's grouping equality does;
//!   * same group ORDER: groups emit sorted by the plan Sort's key prefix —
//!     integer keys under the exact btree integer order (signed word
//!     compare), text keys under `varstr_cmp` with the plan Sort's
//!     collation (C's bttextcmp/ssup authority; the ported comparator
//!     carries C's deterministic memcmp tie-break, so byte-distinct keys
//!     never compare equal) — with the plan's ASC/DESC + NULLS FIRST/LAST
//!     flags. The prefix covers every grouping column (the narrow arm's
//!     multiset check), and two DISTINCT groups cannot compare equal on all
//!     of them, so the order is total and equals the order C's row sort
//!     induces on group boundaries;
//!   * same values: every transition is order-insensitive-EXACT
//!     (`trans_order_insensitive` — counting / exact integer / Int128
//!     accumulation) and runs through the SAME compiled transition program /
//!     set-replay machinery; the sets dedup exactly;
//!   * same representative: the projected group representative is the
//!     group's FIRST ROW IN SCAN ORDER rather than C's first-in-sorted-order
//!     row, but the only columns an Agg output can reference are grouping
//!     columns (byte-equal across the group's rows — representational
//!     equality) and aggregates, so no projected byte can differ.
//!
//! Memory / spill (work_mem safety): the arm meters everything it holds —
//! group key words, representative tuples, per-group transition state, and
//! (capacity-based, like the set's own accounting) every per-group set —
//! against HALF the displaced tuplesort's budget. Crossing it DEGRADES the
//! whole node to the narrow-sort arm mid-build, exactly once: the narrowed
//! tuplesort is begun (the sort the plan wanted, comparator narrowed to the
//! group prefix — spill-safe on its own), every group's DEFERRED
//! representative row is fed to it, remaining input rows stream to it
//! directly, and the emit chain is the narrow-sort arm's, with one addition:
//! `initialize_aggregates` PRELOADS a beginning group's saved partial state
//! (pergroup + sets) from the residual table, so pre-degrade rows are never
//! lost or double-counted. Representative rows are deferred (stored, not
//! transitioned) precisely so the degrade can hand each resident group's
//! sort representative to the tuplesort without double-counting: a group's
//! saved state holds every row EXCEPT its representative, and the
//! representative rides the sort like any other row. The other half of the
//! budget stays free for the emit phase's per-set replay/spill machinery
//! (a preloaded set that keeps growing crosses the FULL per-set budget and
//! spills/degrades through the existing per-set levers, one group live at a
//! time).
//!
//! Aggcontext discipline: many groups' by-ref transvalues live in the
//! node's aggcontext SIMULTANEOUSLY here, so the per-group-boundary
//! aggcontext reset is SKIPPED while hash-arm state exists (the reset would
//! free other groups' live transvalues); it resumes once the residual table
//! drains. The no-degrade emit path never resets aggcontext either (same
//! reason); per-output allocations still reset per group via ps_ExprContext.

use core::ptr::NonNull;

use ::datum::Datum;
use ::execexpr::{exec_eval_expr, AggPerGroup, EvalSlots};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::heaptuple::MinimalTuple;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_slot::{SlotData, TupleSlotKind};

use crate::distinctset::{DistinctKeyKind, DistinctSet};

/// Kill switch for the stringhash single-text-key probe table (the
/// length-bucketed CH-StringHashMap-class map, crates/common/stringhash;
/// parity-proven vs CH's own table on the bench-stringhash branch). Default
/// ON where admissible; PGRUST_LANE_V2_STRINGHASH=0/off reverts the arm to
/// its generic span table.
fn stringhash_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_STRINGHASH").as_deref(),
            Ok("0") | Ok("off") | Ok("false")
        )
    })
}
use crate::{agg_sorted_emit, AggStateData};

/// One emit-order key: `key_idx` indexes the GROUP KEY WORDS (the admission
/// proved the prefix is a permutation of the grouping columns), `desc` /
/// `nulls_first` are the plan Sort's flags for that prefix position.
/// `collation` is the plan Sort's collation for that key — consulted only
/// for text keys (`varstr_cmp`'s authority); 0 for integer keys.
#[derive(Clone)]
pub struct HashGroupOrderKey {
    pub key_idx: usize,
    pub desc: bool,
    pub nulls_first: bool,
    pub collation: ::types_core::Oid,
}

/// Group-key representation. Integer kinds store the sign-extended value in
/// the key word, so word equality is the grouping operator's equality and
/// signed word order is the btree operator order. `Text` stores the
/// detoasted content bytes in the state's arena, the key word packing
/// `(arena offset << 32) | len`; byte equality is the grouping operator's
/// equality (deterministic-collation `texteq` — module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HgKeyKind {
    Int16,
    Int32,
    Int64,
    Text,
}

/// Pack an arena span into a key word (create time). Offsets stay < 4GiB
/// structurally: the arena is metered against the arm's budget (≤ half of
/// work_mem's tuplesort allowance, itself capped well under 4GiB) and a
/// single row overshoots by at most one detoasted value (≤ 1GiB varlena).
#[inline]
fn pack_span(off: usize, len: usize) -> i64 {
    debug_assert!(off <= u32::MAX as usize && len <= u32::MAX as usize);
    (((off as u64) << 32) | len as u64) as i64
}

#[inline]
fn unpack_span(word: i64) -> (usize, usize) {
    ((word as u64 >> 32) as usize, (word as u64 & 0xffff_ffff) as usize)
}

enum HgPhase {
    /// Feeding rows through the group table.
    Building,
    /// Build complete, groups ordered; emitting one group per call.
    Emit { order: Vec<u32>, pos: usize },
    /// Degraded to the narrow-sort arm: the table is now a RESIDUAL state
    /// store consumed by `residual_preload` as the sort read-back begins
    /// each group.
    Residual,
}

const INIT_TABLE: usize = 64;
/// Fixed per-group overhead estimate for the parts the exact counters skip
/// (table slot, hash, vec headers, consumed flag, set-mem cache).
const GROUP_FIXED_COST: usize = 48;

/// splitmix64 finalizer (distinctset.rs's mixer): any deterministic hash is
/// legal here — group equality is representational (module doc).
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

pub(crate) struct HashGroupedState<'mcx> {
    phase: HgPhase,
    /// Per group-key column: 0-based attno in the outer tuple + int kind.
    key_atts: Vec<u16>,
    key_kinds: Vec<HgKeyKind>,
    /// 1 + the largest key 0-based attno (the `slot_getsomeattrs` bound).
    max_att: i32,
    order_spec: Vec<HashGroupOrderKey>,
    nkeys: usize,
    numtrans: usize,
    nsort: usize,
    /// Open-addressing table: slot -> group index + 1; 0 = empty. Pow2 len.
    table: Vec<u32>,
    /// Per group: saved key hash (grow/probe prefilter).
    hashes: Vec<u64>,
    /// Group g's key words at `[g*nkeys .. (g+1)*nkeys]` (sign-extended
    /// integers, or packed arena spans for text keys).
    keys: Vec<i64>,
    /// Text-key content bytes (packed spans in `keys` index into this).
    arena: Vec<u8>,
    /// Per-row staging for the CURRENT row's text key bytes (probe side):
    /// `probe_spans[i]` spans `probe_buf` for text key column i. Rewritten
    /// by every `stage_row_keys`; meaningless for int/NULL columns.
    probe_buf: Vec<u8>,
    probe_spans: Vec<(u32, u32)>,
    /// Per group: NULL bitmask over the key columns (nkeys <= 32).
    keynulls: Vec<u32>,
    /// Per group: the DEFERRED representative row (first row in scan order,
    /// copied whole; its transitions run at finish — or it rides the
    /// narrowed tuplesort on degrade). `None` once consumed.
    reps: Vec<Option<MinimalTuple<'mcx>>>,
    /// Group g's transition state at `[g*numtrans ..]`.
    pergroup: Vec<AggPerGroup>,
    /// Group g's per-internal-sort-entry sets at `[g*nsort ..]`.
    dsets: Vec<Option<DistinctSet<'mcx>>>,
    /// Per group: cached set memory total (capacity-based), so the shared
    /// accounting updates by delta on the current group only.
    set_mem: Vec<usize>,
    /// Mixed-shape batched fast leg (fold admission): the node's NON-distinct
    /// transitions, classified into the exact-integer vocabulary
    /// (`pardistinct::vocab_kind`). Empty = all-distinct shape (v1) or fold
    /// admission off/refused — the batched accept then parks sets only.
    vocab: Vec<crate::pardistinct::PdVocab>,
    /// Sidecar fold states, two i64 words `(acc, count)` per vocab entry per
    /// group at `[g*2*vocab.len() ..]` — the pardistinct worker-state layout.
    /// Batch-absorbed rows fold HERE; per-row-path rows (deferred reps,
    /// fallbacks) advance `pergroup` through the transition program as
    /// always. `hg_fold_combine` merges the sidecar into `pergroup` exactly
    /// once, at finish (post rep-replay) or at degrade entry.
    fold: Vec<i64>,
    /// Residual phase: group already preloaded into the node (emitted).
    consumed: Vec<bool>,
    remaining: usize,
    /// The group whose state is LOADED into the node right now
    /// (pergroup_base + the pertrans `dset` slots).
    cur: Option<u32>,
    /// Everything but the sets (keys, reps, pergroup, table, fixed costs).
    base_mem: usize,
    /// Sum of the per-group cached set memories.
    total_set_mem: usize,
    budget: usize,
    /// Spare outer-format slot for deferred-rep replay and the degrade dump.
    rep_slot: SlotData<'mcx>,
    /// Degrade-dump cursor (`next_rep`).
    rep_cursor: usize,
    /// Single-text-key fast probe table (stringhash swap): engaged when the
    /// key is exactly one text column and the kill switch is on. `arena`
    /// stays the byte authority (emission comparator, spans, degrade); the
    /// map's long bucket references it. NULL keys live in `null_group`
    /// (outside the map), so map keys are exactly the non-NULL byte images.
    smap: Option<::stringhash::ExtIdMap>,
    null_group: Option<u32>,
    mcx: Mcx<'mcx>,
}

impl HashGroupedState<'_> {
    #[inline]
    fn ngroups(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    fn mem(&self) -> usize {
        self.base_mem
            + self.total_set_mem
            + self.arena.capacity()
            + self.smap.as_ref().map_or(0, |m| m.mem_bytes())
    }
}

const INT2OID: ::types_core::Oid = 21;
const INT4OID: ::types_core::Oid = 23;
const INT8OID: ::types_core::Oid = 20;
const TEXTOID: ::types_core::Oid = 25;
const VARCHAROID: ::types_core::Oid = 1043;

/// Structural admission for the hash-grouped arm, ON TOP of the narrow-sort
/// admission (`agg_sorted_distinct_narrow_admissible`, re-checked here):
/// every grouping column is int2/int4/int8 (word-packable) or text/varchar
/// (byte-imaged; the narrow admission's `group_eq_representational` texteq
/// arm already proved a DETERMINISTIC collation, so byte equality is the
/// grouping operator's verdict — bpchar never passes that admission), and
/// every internal-sort entry's set kind is an integer kind (no text sets in
/// v1 — the narrow-sort arm keeps those). Group-col count capped at 32 (the
/// NULL bitmask word).
pub fn agg_hashgroup_admissible(node: &AggStateData<'_>) -> bool {
    if !crate::agg_sorted_distinct_narrow_admissible(node) {
        return false;
    }
    let ncols = node.plan.grpColIdx.len();
    if ncols == 0 || ncols > 32 {
        return false;
    }
    let Some(ps) = node.persort.as_ref() else {
        return false;
    };
    let Some(desc) = ps.first_slot.base().tts_tupleDescriptor.as_ref() else {
        return false;
    };
    for &col in node.plan.grpColIdx {
        if col < 1 || (col as i32) > desc.natts {
            return false;
        }
        let t = desc.attr((col - 1) as usize).atttypid;
        if !matches!(t, INT2OID | INT4OID | INT8OID | TEXTOID | VARCHAROID) {
            return false;
        }
    }
    node.pertrans_sort.iter().all(|ps| {
        matches!(
            ps.set_kind,
            Some(DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64)
        )
    })
}

/// How many grouping columns are text/varchar (the lane drive's text-switch
/// / trace / economics input). Callable only where `persort` exists.
pub fn agg_hashgroup_text_key_count(node: &AggStateData<'_>) -> usize {
    let Some(ps) = node.persort.as_ref() else {
        return 0;
    };
    let Some(desc) = ps.first_slot.base().tts_tupleDescriptor.as_ref() else {
        return 0;
    };
    node.plan
        .grpColIdx
        .iter()
        .filter(|&&col| {
            col >= 1
                && (col as i32) <= desc.natts
                && matches!(desc.attr((col - 1) as usize).atttypid, TEXTOID | VARCHAROID)
        })
        .count()
}

/// Would `agg_hashgroup_begin` engage the stringhash table for this node?
/// (single text/varchar group key + switch on — mirrors the begin-side
/// engage condition so the economics tier prices the right probe machinery.)
fn smap_shape(node: &AggStateData<'_>) -> bool {
    if !stringhash_enabled() || node.plan.grpColIdx.len() != 1 {
        return false;
    }
    let Some(ps) = node.persort.as_ref() else { return false };
    let Some(desc) = ps.first_slot.base().tts_tupleDescriptor.as_ref() else { return false };
    let col = node.plan.grpColIdx[0];
    col >= 1
        && (col as i32) <= desc.natts
        && matches!(desc.attr((col - 1) as usize).atttypid, TEXTOID | VARCHAROID)
}

/// Density threshold for the stringhash shape. Default stays at the generic
/// 8.0 until the near-unique-key crossover A/B lands a measured value.
fn stringhash_min_rpg() -> f64 {
    static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_STRINGHASH_MINRPG")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8.0)
    })
}

/// The arm's build budget: HALF the displaced tuplesort's work_mem allowance
/// (`distinct_set_budget`) — the other half stays free for the emit phase's
/// per-group replay (whose sets can themselves spill/degrade under the full
/// per-set budget, one group live at a time).
fn hashgroup_budget() -> usize {
    crate::distinct_set_budget() / 2
}

/// Planner-estimate economics: the estimated group count (with 2x slack for
/// estimate error) must fit the arm's budget at a conservative fixed
/// per-group cost. Refusal falls back to the narrow-sort arm, which handles
/// any group count spill-safely. `force` (the e2e harness override) skips
/// the estimate check — the runtime degrade still bounds memory.
pub fn agg_hashgroup_economical(node: &AggStateData<'_>, force: bool, input_rows: f64) -> bool {
    if force {
        return true;
    }
    const PER_GROUP_EST: f64 = 256.0;
    /// Extra per-group estimate per TEXT key column (arena content bytes;
    /// conservative mean — the runtime degrade bounds the real usage).
    const PER_TEXT_KEY_EST: f64 = 64.0;
    /// DENSITY tier: the arm's win is collapsing many rows into few group
    /// states; near-unique group keys (measured case: ~1.35M input rows over ~690k
    /// estimated SearchPhrase groups) make the per-row group switch/create
    /// machinery COST vs the narrowed sort's adjacent dedup — measured
    /// 2.0s vs 1.44s serial with engage-then-degrade every rep (fleet
    /// 2026-07-12, 10M bank, work_mem=1GB). The winner shapes sit at 200x-100000x
    /// rows/group; near-unique-key shapes refuse here. `input_rows`
    /// is the plan Sort's row estimate (0.0 = unknown: tier skipped).
    const MIN_ROWS_PER_GROUP: f64 = 8.0;
    // The 8x tier was calibrated on the GENERIC span table's probe/create
    // cost. The stringhash-admissible shape (single text key, switch on)
    // reads its own threshold — re-priced by the near-unique A/B; the env override
    // is the measurement channel (PGRUST_LANE_V2_STRINGHASH_MINRPG).
    let min_rpg = if smap_shape(node) { stringhash_min_rpg() } else { MIN_ROWS_PER_GROUP };
    let est_groups = (node.plan.numGroups as f64).max(1.0);
    if input_rows > 0.0 && input_rows < min_rpg * est_groups {
        return false;
    }
    let per_group =
        PER_GROUP_EST + PER_TEXT_KEY_EST * agg_hashgroup_text_key_count(node) as f64;
    est_groups * per_group * 2.0 <= hashgroup_budget() as f64
}

/// The RUNTIME DISTINCT SINK's economics twin of
/// [`agg_hashgroup_economical`] (distinct-bytes car). The budget-fit term
/// is UNCHANGED (group tables never spill; only set values do): estimated
/// groups must fit the per-Local budget with 2x slack. Density reads its
/// own threshold (`PGRUST_RUNTIME_DISTINCT_MINRPG`) — MEASURED 2026-07-14
/// (job pgrust-m0-accept-1784045139-06ee, 10M v7u bank, wm=1GB,
/// condcache=on): with the tier at 1.0 the near-unique text-key class
/// (SearchPhrase text key, ~1.6 rows/group, 835k merged groups) ENGAGES
/// with full parity + morsel-elastic disturb (4.7%) but LOSES —
/// runtime16 1.739s vs ser 1.254s (0.72x): the build parallelizes but
/// the leader-side adopt/emit tail (concat + rep synthesis + order +
/// per-group emit over 835k groups) dominates, the 10M-scale
/// "emit/combine-bound near-unique" class. The serial-calibrated 8.0
/// therefore prices the ADOPT tail.
///
/// `paremit` (parallel-emit car): the caller proved the shape rides the
/// emission-in-combine fast path (`pd_paremit_cols` — ordered
/// per-partition emit buckets built by workers; the leader tail collapses
/// to the cross-bucket merge + datum memcpy), which removes exactly the
/// serial floor the 8.0 tier priced. Those shapes read their own default
/// (1.0; `PGRUST_RUNTIME_DISTINCT_PAREMIT_MINRPG` is the re-pricing
/// channel) so the near-unique text-key class engages by default. The
/// budget-fit term applies unchanged to both tiers.
///
/// `dop_budget` (near-unique-100M lane, the K2 100M admission): `Some((per_local
/// envelope bytes, dop))` when the caller proved the FULL bounded-memory
/// stack is armed — a live paremit recipe, a resolved K2 top-N selection
/// (leader retention ≤ bound×parts candidates, never the merged group
/// set), and the M3.5 spill arm (accept-side value pressure). Under that
/// stack the single-halved-serial-budget fit term (`hashgroup_budget`)
/// prices the WRONG machine: the runtime sink re-budgets every Local to
/// the full R3 envelope, the combine's union bound (`worker_budget ×
/// sealed.len()`) plus value-hash splits already bound each partition's
/// merged table dynamically, and per-partition tables are est/256-sized.
/// The fit term therefore becomes the same union bound the combine
/// enforces: `est_groups × per_group × 2 ≤ envelope × dop` (near-unique @100M:
/// 5.43M est × 640 = 3.48GB vs 512MB serial-halved → refused; vs 2GiB ×
/// 16 = 32GiB → admits; the 10M engagement admits under BOTH terms —
/// this face only ADDS engagements, never removes one). `None` = the
/// serial-halved term exactly (every non-topn caller, and the
/// `PGRUST_RUNTIME_DISTINCT_TOPN_DOPBUDGET=0` rollback channel).
pub fn agg_hashgroup_economical_sink(
    node: &AggStateData<'_>,
    force: bool,
    input_rows: f64,
    paremit: bool,
    dop_budget: Option<(usize, u32)>,
) -> bool {
    if force {
        return true;
    }
    const PER_GROUP_EST: f64 = 256.0;
    const PER_TEXT_KEY_EST: f64 = 64.0;
    fn sink_min_rpg() -> f64 {
        static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
        *V.get_or_init(|| {
            std::env::var("PGRUST_RUNTIME_DISTINCT_MINRPG")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8.0)
        })
    }
    fn paremit_min_rpg() -> f64 {
        static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
        *V.get_or_init(|| {
            std::env::var("PGRUST_RUNTIME_DISTINCT_PAREMIT_MINRPG")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0)
        })
    }
    let min_rpg = if paremit { paremit_min_rpg() } else { sink_min_rpg() };
    let est_groups = (node.plan.numGroups as f64).max(1.0);
    if input_rows > 0.0 && input_rows < min_rpg * est_groups {
        return false;
    }
    let per_group =
        PER_GROUP_EST + PER_TEXT_KEY_EST * agg_hashgroup_text_key_count(node) as f64;
    let fit_budget = match dop_budget {
        Some((envelope, dop)) => (envelope as f64) * f64::from(dop.max(1)),
        None => hashgroup_budget() as f64,
    };
    est_groups * per_group * 2.0 <= fit_budget
}

/// Whether the arm is mid-emit (the drive routes straight to
/// `agg_hashgroup_emit_next`, never touching the plan's Sort node).
pub fn agg_hashgroup_emitting(node: &AggStateData<'_>) -> bool {
    matches!(
        node.hashgroup.as_deref(),
        Some(HashGroupedState { phase: HgPhase::Emit { .. }, .. })
    )
}

/// Whether ANY hash-arm state exists (build, emit, or residual): the
/// per-group aggcontext reset must be skipped while it does — other groups'
/// by-ref transvalues live in aggcontext (module doc).
pub fn agg_hashgroup_state_active(node: &AggStateData<'_>) -> bool {
    node.hashgroup.is_some()
}

/// Whether degraded residual state exists (the narrow-sort emit chain's
/// group begins preload from it via `residual_preload`).
pub fn agg_hashgroup_residual_active(node: &AggStateData<'_>) -> bool {
    matches!(node.hashgroup.as_deref(), Some(HashGroupedState { phase: HgPhase::Residual, .. }))
}

/// Rescan/teardown: drop the whole arm state (sets release their memory via
/// `DistinctSet::clear`; nothing here lives in aggcontext except by-ref
/// transvalues, which the rescan's own aggcontext reset frees).
pub fn agg_hashgroup_reset(node: &mut AggStateData<'_>) {
    if let Some(mut hg) = node.hashgroup.take() {
        let bytes = hg.mem();
        let mcx = hg.mcx;
        exectuples::exec_clear_tuple(&mut hg.rep_slot, mcx);
        for d in hg.dsets.iter_mut().flatten() {
            d.clear();
        }
        // The node-side pertrans dset slots may hold the current group's
        // swapped-in sets; the group-boundary restart clears those.
        drop(hg);
        // The DISTINCT-sink half of 69b97573f's teardown discipline (the
        // high-cardinality-lane flag: "the same teardown release belongs in the distinct
        // sink"): the runtime distinct sink adopts its merged result HERE
        // (agg_hashgroup_adopt_merged), and the parallel build that produced
        // it churned a multi-GB per-worker working set in helper threads
        // that have already exited — all freed-but-retained by mimalloc.
        // mi_collect(force) purges those abandoned segments so a repeat
        // execution (1session try-2) rebuilds inside the same RSS envelope
        // instead of ratcheting toward the pod cgroup ceiling. Same >=64MB
        // engagement floor as the agg sink's release (the serial hashgroup
        // arm passes through here too; sub-64MB builds skip the collect).
        if bytes >= crate::SINK_RELEASE_MIN_BYTES {
            crate::hashagg_release_retained("hashgroup_teardown");
        }
    }
}

/// Begin the hash-grouped build. `order_spec` is the drive's resolved emit
/// order (the plan Sort's prefix keys mapped onto the grouping columns).
/// The caller must have armed `force_distinct_set` and verified
/// `agg_hashgroup_admissible`.
pub fn agg_hashgroup_begin<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    order_spec: Vec<HashGroupOrderKey>,
) -> PgResult<()> {
    debug_assert!(agg_hashgroup_admissible(node));
    debug_assert!(node.force_distinct_set);
    debug_assert!(node.hashgroup.is_none());
    let mcx = estate.es_query_cxt;
    let ps = node.persort.as_ref().expect("sorted Agg has persort");
    let desc = ps
        .first_slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("persort slots carry the outer desc")
        .clone();
    let mut key_atts = Vec::with_capacity(node.plan.grpColIdx.len());
    let mut key_kinds = Vec::with_capacity(node.plan.grpColIdx.len());
    let mut max_att = 0i32;
    for &col in node.plan.grpColIdx {
        key_atts.push((col - 1) as u16);
        max_att = max_att.max(col as i32);
        key_kinds.push(match desc.attr((col - 1) as usize).atttypid {
            INT2OID => HgKeyKind::Int16,
            INT4OID => HgKeyKind::Int32,
            TEXTOID | VARCHAROID => HgKeyKind::Text,
            _ => HgKeyKind::Int64,
        });
    }
    debug_assert_eq!(order_spec.len(), key_atts.len());
    debug_assert!(order_spec.iter().all(|k| k.key_idx < key_atts.len()));
    let nkeys = key_atts.len();
    let rep_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    let engage_smap = nkeys == 1 && key_kinds[0] == HgKeyKind::Text && stringhash_enabled();
    node.hashgroup = Some(Box::new(HashGroupedState {
        phase: HgPhase::Building,
        key_atts,
        key_kinds,
        max_att,
        order_spec,
        nkeys,
        numtrans: node.numtrans,
        nsort: node.pertrans_sort.len(),
        table: vec![0u32; INIT_TABLE],
        hashes: Vec::new(),
        keys: Vec::new(),
        arena: Vec::new(),
        probe_buf: Vec::new(),
        probe_spans: vec![(0, 0); nkeys],
        keynulls: Vec::new(),
        reps: Vec::new(),
        pergroup: Vec::new(),
        dsets: Vec::new(),
        set_mem: Vec::new(),
        vocab: Vec::new(),
        fold: Vec::new(),
        consumed: Vec::new(),
        remaining: 0,
        cur: None,
        base_mem: INIT_TABLE * core::mem::size_of::<u32>(),
        total_set_mem: 0,
        budget: hashgroup_budget(),
        rep_slot,
        rep_cursor: 0,
        smap: engage_smap.then(::stringhash::ExtIdMap::new),
        null_group: None,
        mcx,
    }));
    // The build starts with NO group loaded; the node's own pergroup array
    // is the swap scratch. Clear leftover pertrans set state (a rescan can
    // leave the last emitted group's set behind) — and DROP the slot:
    // `switch_to` owns the invariant that no set is loaded between groups
    // (its debug_assert fired on rescan re-engagement when this left a
    // cleared-but-Some slot; release builds silently overwrote it).
    for ps in node.pertrans_sort.iter_mut() {
        if let Some(mut d) = ps.dset.take() {
            d.clear();
        }
        debug_assert!(!ps.dset_degraded);
    }
    Ok(())
}

/// Phase A of key extraction (slot borrow only): sign-extended key words +
/// NULL bitmask for the slot's grouping columns. Text columns leave their
/// word 0 and stash the raw datum in `text_datums[i]` for phase B (the
/// detoast needs a per-tuple-memory borrow the slot borrow excludes).
fn read_key_datums(
    slot: &mut SlotData<'_>,
    key_atts: &[u16],
    key_kinds: &[HgKeyKind],
    max_att: i32,
    words: &mut [i64],
    text_datums: &mut [Datum],
) -> u32 {
    exectuples::slot_getsomeattrs(slot, max_att);
    let base = slot.base();
    let mut nulls = 0u32;
    for (i, (&att, &kind)) in key_atts.iter().zip(key_kinds.iter()).enumerate() {
        if base.tts_isnull[att as usize] {
            nulls |= 1 << i;
            words[i] = 0;
            continue;
        }
        let d = base.tts_values[att as usize];
        words[i] = match kind {
            HgKeyKind::Int16 => d.as_i16() as i64,
            HgKeyKind::Int32 => d.as_i32() as i64,
            HgKeyKind::Int64 => d.as_i64(),
            HgKeyKind::Text => {
                text_datums[i] = d;
                0
            }
        };
    }
    nulls
}

/// Fold a byte string into the running hash, 8 LE bytes per mix round plus
/// a length round (any deterministic hash is legal — module doc).
#[inline]
fn fold_bytes(mut h: u64, b: &[u8]) -> u64 {
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        h = mix64(h ^ u64::from_le_bytes(c.try_into().unwrap()));
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut last = [0u8; 8];
        last[..rem.len()].copy_from_slice(rem);
        h = mix64(h ^ u64::from_le_bytes(last));
    }
    mix64(h ^ b.len() as u64)
}

impl HashGroupedState<'_> {
    /// Phase B of key extraction: detoast each non-NULL text key into the
    /// probe staging (content bytes in `probe_buf`, span in `probe_spans`).
    /// Detoast copies land in per-tuple memory exactly as the per-row set
    /// collect's (`datum_varlena_packed`); the staged copy in `probe_buf`
    /// is what probe/create read, so the per-tuple lifetime never escapes.
    fn stage_text_keys(
        &mut self,
        estate: &EStateData<'_>,
        tmp: EcxtId,
        text_datums: &[Datum],
        nulls: u32,
    ) -> PgResult<()> {
        self.probe_buf.clear();
        for i in 0..self.nkeys {
            if self.key_kinds[i] != HgKeyKind::Text || nulls & (1 << i) != 0 {
                continue;
            }
            // SAFETY: non-null live text/varchar varlena — the admission
            // proved the column type.
            let v = unsafe {
                ::types_fmgr::datum_varlena_packed(
                    text_datums[i],
                    estate.ecxt(tmp).per_tuple_mcx(),
                )
            }?;
            let b = v.data();
            let off = self.probe_buf.len();
            self.probe_buf.extend_from_slice(b);
            self.probe_spans[i] = (off as u32, b.len() as u32);
        }
        Ok(())
    }

    /// Group-key hash over the staged row: integer keys mix their word,
    /// text keys fold their staged bytes (NULL text mixes word 0, like a
    /// NULL integer — the nulls seed already separates the bitmasks).
    fn key_hash(&self, words: &[i64], nulls: u32) -> u64 {
        let mut h = (nulls as u64) ^ 0x9e37_79b9_7f4a_7c15;
        for (i, &w) in words.iter().enumerate() {
            if self.key_kinds[i] == HgKeyKind::Text && nulls & (1 << i) == 0 {
                let (off, len) = self.probe_spans[i];
                h = fold_bytes(h, &self.probe_buf[off as usize..(off + len) as usize]);
            } else {
                h = mix64(h ^ (w as u64));
            }
        }
        h
    }

    /// Column-wise key equality of group `g` against the staged row (equal
    /// NULL bitmasks already checked): integer keys by word, text keys by
    /// arena-vs-staging byte compare (deterministic-collation texteq).
    fn keys_equal(&self, g: usize, words: &[i64], nulls: u32) -> bool {
        let base = g * self.nkeys;
        for i in 0..self.nkeys {
            match self.key_kinds[i] {
                HgKeyKind::Text => {
                    if nulls & (1 << i) != 0 {
                        continue;
                    }
                    let (off, len) = unpack_span(self.keys[base + i]);
                    let (poff, plen) = self.probe_spans[i];
                    if len != plen as usize
                        || self.arena[off..off + len]
                            != self.probe_buf[poff as usize..(poff + plen) as usize]
                    {
                        return false;
                    }
                }
                _ => {
                    if self.keys[base + i] != words[i] {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Probe for an existing group; on miss, also return the empty slot the
    /// insert must claim.
    fn probe(&self, words: &[i64], nulls: u32, h: u64) -> (Option<u32>, usize) {
        let mask = self.table.len() - 1;
        let mut slot = (h as usize) & mask;
        loop {
            match self.table[slot] {
                0 => return (None, slot),
                e => {
                    let g = (e - 1) as usize;
                    if self.hashes[g] == h
                        && self.keynulls[g] == nulls
                        && self.keys_equal(g, words, nulls)
                    {
                        return (Some(e - 1), slot);
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let new_len = self.table.len() * 2;
        self.base_mem += (new_len - self.table.len()) * core::mem::size_of::<u32>();
        let mask = new_len - 1;
        let mut table = vec![0u32; new_len];
        for (g, &h) in self.hashes.iter().enumerate() {
            let mut slot = (h as usize) & mask;
            while table[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            table[slot] = (g + 1) as u32;
        }
        self.table = table;
    }
}

/// Swap the CURRENT group's live state (node pergroup array + pertrans set
/// slots) back into storage.
fn switch_out(node: &mut AggStateData<'_>) {
    let AggStateData { hashgroup, pergroup_base, pertrans_sort, numtrans, .. } = node;
    let Some(hg) = hashgroup.as_deref_mut() else { return };
    let Some(c) = hg.cur.take() else { return };
    let c = c as usize;
    // SAFETY: both sides are once-allocated arrays of numtrans elements; the
    // base pointer is the node's sole pergroup access path (struct
    // invariant) and the storage vec was sized at group creation.
    unsafe {
        core::ptr::copy_nonoverlapping(
            pergroup_base.as_ptr(),
            hg.pergroup.as_mut_ptr().add(c * hg.numtrans),
            *numtrans,
        );
    }
    let mut sets = 0usize;
    for (j, ps) in pertrans_sort.iter_mut().enumerate() {
        let d = ps.dset.take();
        if let Some(d) = d.as_ref() {
            sets += d.mem_bytes();
        }
        hg.dsets[c * hg.nsort + j] = d;
    }
    hg.total_set_mem = hg.total_set_mem + sets - hg.set_mem[c];
    hg.set_mem[c] = sets;
}

/// Load group `g`'s state into the node (pergroup array + pertrans set
/// slots). The previous current group, if any, swaps out first.
fn switch_to(node: &mut AggStateData<'_>, g: u32) {
    if node.hashgroup.as_deref().is_some_and(|hg| hg.cur == Some(g)) {
        return;
    }
    switch_out(node);
    let AggStateData { hashgroup, pergroup_base, pertrans_sort, numtrans, .. } = node;
    let hg = hashgroup.as_deref_mut().expect("hashgroup state");
    let gi = g as usize;
    // SAFETY: as switch_out.
    unsafe {
        core::ptr::copy_nonoverlapping(
            hg.pergroup.as_ptr().add(gi * hg.numtrans),
            pergroup_base.as_ptr(),
            *numtrans,
        );
    }
    for (j, ps) in pertrans_sort.iter_mut().enumerate() {
        debug_assert!(ps.dset.is_none());
        ps.dset = hg.dsets[gi * hg.nsort + j].take();
    }
    hg.cur = Some(g);
}

/// Create a new group from the current row: push key/hash/rep/init-state
/// (text keys copy their staged bytes into the arena and pack the span into
/// their key word). The row itself is DEFERRED (module doc — the degrade
/// path's sort representative). Does NOT make the group current.
fn create_group<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    id: ExecSlotId,
    words: &mut [i64],
    nulls: u32,
    h: u64,
    // None = stringhash mode: the map already indexes the group; the arm's
    // generic table/hashes stay untouched (hashes still gets a placeholder —
    // ngroups() is its len).
    slot_idx: Option<usize>,
    // Some((off, len)) = stringhash mode already appended the key bytes to
    // the arena (single text key at word 0).
    prestored_span: Option<(u32, u32)>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // Group-init transvalues (initialize_aggregates' loop, retargeted at the
    // group's own storage; by-ref initvals copy into aggcontext exactly as
    // the per-group path does).
    let mut init_state: Vec<AggPerGroup> = Vec::with_capacity(node.numtrans);
    for (transno, init) in node.trans_init.iter().enumerate() {
        let typ = node.trans_typ[transno];
        let value = if !init.isnull && !typ.byval {
            // SAFETY: node-lifetime initval datum; agg_node is live, no &mut.
            unsafe {
                ::execexpr::agg_datum_copy(
                    node.agg_node.as_ref().aggcontext(),
                    init.value,
                    typ.len,
                )?
            }
        } else {
            init.value
        };
        init_state.push(AggPerGroup {
            trans_value: value,
            trans_value_is_null: init.isnull,
            no_trans_value: init.isnull,
        });
    }
    let slot = estate.slot_mut(id);
    let rep = exectuples::exec_copy_slot_minimal_tuple(slot, mcx, mcx, 0)?;
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    let rep_len = rep.t_len() as usize;
    // Text keys: land the staged bytes in the arena, pack the span (the
    // packing's u32 bounds hold structurally — `pack_span` doc). In
    // stringhash mode the map's insert already appended the bytes.
    if let Some((off, len)) = prestored_span {
        debug_assert!(hg.nkeys == 1 && hg.key_kinds[0] == HgKeyKind::Text && nulls == 0);
        words[0] = pack_span(off as usize, len as usize);
    } else {
        for i in 0..hg.nkeys {
            if hg.key_kinds[i] == HgKeyKind::Text && nulls & (1 << i) == 0 {
                let (poff, plen) = hg.probe_spans[i];
                let off = hg.arena.len();
                hg.arena
                    .extend_from_slice(&hg.probe_buf[poff as usize..(poff + plen) as usize]);
                words[i] = pack_span(off, plen as usize);
            }
        }
    }
    hg.hashes.push(h);
    hg.keynulls.push(nulls);
    hg.keys.extend_from_slice(words);
    hg.reps.push(Some(rep));
    hg.pergroup.extend(init_state);
    for _ in 0..hg.nsort {
        hg.dsets.push(None);
    }
    hg.set_mem.push(0);
    if !hg.vocab.is_empty() {
        hg.fold.resize(hg.fold.len() + 2 * hg.vocab.len(), 0);
    }
    hg.consumed.push(false);
    hg.remaining += 1;
    if let Some(slot_idx) = slot_idx {
        hg.table[slot_idx] = hg.ngroups() as u32;
    }
    hg.base_mem += hg.nkeys * 8
        + rep_len
        + hg.numtrans * core::mem::size_of::<AggPerGroup>()
        + hg.nsort * core::mem::size_of::<Option<DistinctSet<'_>>>()
        + hg.vocab.len() * 2 * core::mem::size_of::<i64>()
        + GROUP_FIXED_COST;
    // 7/8 load factor (generic table only; the stringhash map self-grows).
    if slot_idx.is_some() && (hg.ngroups() + 1) * 8 > hg.table.len() * 7 {
        hg.grow();
    }
    Ok(())
}

enum RowSlot {
    Estate(ExecSlotId),
    Rep,
}

/// Run one row of the CURRENT group through the compiled transition program
/// (non-distinct transitions advance in place; DISTINCT args park in the
/// pertrans scratch), then collect the parked args into the group's sets —
/// `collect_ordered_input`'s set arm WITHOUT the per-set overflow (the arm
/// meters a SHARED budget; crossing degrades the whole node instead).
fn run_row<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer: RowSlot,
) -> PgResult<()> {
    {
        let AggStateData { hashgroup, evaltrans, .. } = node;
        let et = evaltrans.as_mut().expect("lane admission requires evaltrans");
        match outer {
            RowSlot::Estate(id) => {
                let outer_slot = estate.slot_mut(id);
                let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                exec_eval_expr(et, &mut slots)?;
            }
            RowSlot::Rep => {
                let hg = hashgroup.as_deref_mut().expect("hashgroup state");
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(&mut hg.rep_slot) };
                exec_eval_expr(et, &mut slots)?;
            }
        }
    }
    // Set collect (the pertrans dset slots hold the CURRENT group's sets).
    for ps in node.pertrans_sort.iter_mut() {
        // SAFETY: once-allocated cells the trans program writes (steps.rs).
        if !unsafe { ps.flag.read() } {
            continue;
        }
        // SAFETY: as above.
        unsafe { ps.flag.write(false) };
        let kind = ps.set_kind.expect("hashgroup admission: set-mode pertrans");
        // SAFETY: scratch slot 0 written by the program this row.
        let nd = unsafe { ps.scratch.read() };
        let dset = ps.dset.get_or_insert_with(DistinctSet::new);
        if nd.isnull {
            dset.seen_null = true;
            continue;
        }
        match kind {
            DistinctKeyKind::Int16 => dset.insert_i64(nd.value.as_i16() as i64),
            DistinctKeyKind::Int32 => dset.insert_i64(nd.value.as_i32() as i64),
            DistinctKeyKind::Int64 => dset.insert_i64(nd.value.as_i64()),
            DistinctKeyKind::Bytes => unreachable!("hashgroup admission excludes byte sets"),
        }
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// Feed one input row. `Ok(true)` = within budget, keep feeding; `Ok(false)`
/// = the shared budget crossed AFTER this row was fully absorbed — the
/// caller must degrade to the narrow-sort arm (`next_rep` + `set_residual`).
pub fn agg_hashgroup_accept<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    id: ExecSlotId,
) -> PgResult<bool> {
    debug_assert!(matches!(
        node.hashgroup.as_deref(),
        Some(HashGroupedState { phase: HgPhase::Building, .. })
    ));
    let tmp = node.tmpcontext;
    let mut words = [0i64; 32];
    let mut text_datums = [Datum::null(); 32];
    let (found, slot_idx, h, nulls, nkeys) = {
        let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
        let nkeys = hg.nkeys;
        let nulls = {
            let slot = estate.slot_mut(id);
            read_key_datums(
                slot,
                &hg.key_atts,
                &hg.key_kinds,
                hg.max_att,
                &mut words[..nkeys],
                &mut text_datums[..nkeys],
            )
        };
        hg.stage_text_keys(estate, tmp, &text_datums[..nkeys], nulls)?;
        if hg.smap.is_some() {
            // stringhash mode: single text key. NULL rows key `null_group`;
            // non-NULL rows insert-or-get on the map, which appends new key
            // bytes to the arena and reports the span for the key word.
            let HashGroupedState { smap, null_group, arena, probe_buf, probe_spans, .. } = hg;
            let smap = smap.as_mut().expect("checked");
            let (found, span) = if nulls != 0 {
                (*null_group, None)
            } else {
                let (poff, plen) = probe_spans[0];
                let bytes = &probe_buf[poff as usize..(poff + plen) as usize];
                let (g, inserted, off) = smap.insert_or_get(bytes, arena);
                if inserted {
                    (None, Some((off, plen)))
                } else {
                    (Some(g), None)
                }
            };
            match found {
                Some(g) => {
                    switch_to(node, g);
                    run_row(node, estate, RowSlot::Estate(id))?;
                    let sets: usize = node
                        .pertrans_sort
                        .iter()
                        .map(|ps| ps.dset.as_ref().map_or(0, |d| d.mem_bytes()))
                        .sum();
                    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
                    let c = g as usize;
                    hg.total_set_mem = hg.total_set_mem + sets - hg.set_mem[c];
                    hg.set_mem[c] = sets;
                }
                None => {
                    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
                    if nulls != 0 {
                        // The map ids and null_group share ONE dense space:
                        // the null group takes the next creation slot and
                        // the map skips it.
                        let id = hg.smap.as_mut().expect("checked").reserve_id();
                        debug_assert_eq!(id as usize, hg.ngroups());
                        hg.null_group = Some(id);
                    }
                    create_group(node, estate, id, &mut words[..nkeys], nulls, 0, None, span)?;
                    estate.reset_expr_context(tmp);
                }
            }
            let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
            if hg.probe_buf.capacity() > (1 << 20) {
                hg.probe_buf = Vec::new();
            }
            return Ok(hg.mem() <= hg.budget);
        }
        let h = hg.key_hash(&words[..nkeys], nulls);
        let (found, slot_idx) = hg.probe(&words[..nkeys], nulls, h);
        (found, slot_idx, h, nulls, nkeys)
    };
    match found {
        Some(g) => {
            switch_to(node, g);
            run_row(node, estate, RowSlot::Estate(id))?;
            // Shared-accounting update: the current group's set delta
            // (mem_bytes is capacity-based and O(1)).
            let sets: usize = node
                .pertrans_sort
                .iter()
                .map(|ps| ps.dset.as_ref().map_or(0, |d| d.mem_bytes()))
                .sum();
            let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
            let c = g as usize;
            hg.total_set_mem = hg.total_set_mem + sets - hg.set_mem[c];
            hg.set_mem[c] = sets;
        }
        None => {
            create_group(node, estate, id, &mut words[..nkeys], nulls, h, Some(slot_idx), None)?;
            // The existing-group arm resets per-tuple memory inside run_row;
            // the create arm defers the row (no run_row), so any text-key
            // detoast copies reset here instead.
            estate.reset_expr_context(tmp);
        }
    }
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    // A one-off giant detoasted key must not pin probe staging capacity.
    if hg.probe_buf.capacity() > (1 << 20) {
        hg.probe_buf = Vec::new();
    }
    Ok(hg.mem() <= hg.budget)
}

/// Batch-feed shape (0-based OUTER attnos) for the lanev2 batched accept.
/// `fold_atts` lists the staged cells of the ARG-BEARING vocab entries, in
/// `vocab` order (CountStar consumes no cell) — the accept's `folds` slice
/// contract.
pub struct HgBatchShape {
    pub key_atts: Vec<u16>,
    pub set_args: Vec<u16>,
    pub fold_atts: Vec<u16>,
    pub vocab: Vec<crate::pardistinct::PdVocab>,
}

/// Batch-feed shape probe (the lanev2 batched accept, hot-levers-t11
/// lever 2 conversion 3): `Some(shape)` (0-based OUTER attnos) when EVERY
/// grouping key is an integer kind and EVERY transition of the node is
/// either (a) a set-mode pertrans with a bare integer-Var argument
/// (`direct_att`: single input, no FILTER) or (b) — mixed-shape fold
/// admission, `allow_fold` (the class with sums alongside the DISTINCT) — a
/// plain transition in the exact-integer vocabulary
/// (`pardistinct::vocab_kind`: count(*)/count(x)/sum(int2/4)/avg(int2/4),
/// single bare-Var argument, no FILTER/DISTINCT/ORDER). Under that shape
/// the transition program's whole per-row effect is "park each set
/// pertrans' arg + advance the vocab folds" — the batched accept reproduces
/// `run_row` exactly from staged column cells without projecting a slot or
/// running the program (folds accumulate in the sidecar words;
/// `hg_fold_combine` merges them into the real trans states at finish/
/// degrade). `allow_fold=false` = the historical all-set-mode gate exactly.
///
/// `allow_text` (named-kernels-distinct, the filtered grouped-distinct
/// batch feed): TEXT/VARCHAR grouping keys are admitted to the batched
/// accepts — the span/row arms stage each staged cell's inline varlena
/// content through the SAME `probe_buf`/`probe_spans` discipline as the
/// per-row `stage_text_keys` (identical bytes: the staged pgrcolumnar cell is
/// the decoded datum; non-inline images route to the per-row path, whose
/// detoast yields the same content), probe read-only (`smap.find` on the
/// stringhash single-key engagement / the generic content probe), and
/// defer every probe MISS to the per-row path (`NeedSlot` — group creation
/// order and rep bytes stay byte-identical). `allow_text=false` = the
/// historical int-keys-only gate exactly.
/// Callable only after `agg_hashgroup_begin`.
pub fn agg_hashgroup_batch_shape(
    node: &AggStateData<'_>,
    allow_fold: bool,
    allow_text: bool,
) -> Option<HgBatchShape> {
    let hg = node.hashgroup.as_deref()?;
    if !allow_text && hg.key_kinds.iter().any(|k| matches!(k, HgKeyKind::Text)) {
        return None;
    }
    debug_assert!(
        hg.smap.is_none() || (hg.nkeys == 1 && hg.key_kinds[0] == HgKeyKind::Text),
        "smap engages only on a single text key"
    );
    if hg.numtrans != node.pertrans_sort.len() && !allow_fold {
        return None;
    }
    let mut args = Vec::with_capacity(node.pertrans_sort.len());
    for ps in &node.pertrans_sort {
        if !matches!(
            ps.set_kind,
            Some(DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64)
        ) {
            return None;
        }
        args.push(ps.direct_att?);
    }
    let (vocab, fold_atts) = if hg.numtrans == node.pertrans_sort.len() {
        (Vec::new(), Vec::new())
    } else {
        hg_fold_vocab(node)?
    };
    Some(HgBatchShape { key_atts: hg.key_atts.clone(), set_args: args, fold_atts, vocab })
}

/// Classify the node's NON-distinct transitions into the exact-integer fold
/// vocabulary (`pd_derive_spec`'s vocab loop, the serial face): every
/// remaining transition must be a FILTER-less, DISTINCT-less, ORDER-less
/// aggregate over at most one bare OUTER-Var int2/int4/int8 argument whose
/// aggfnoid maps through `pardistinct::vocab_kind`. Returns the vocab (in
/// first-peragg order) plus the staged-cell atts of its arg-bearing entries
/// (same order — the accept's `folds` contract). `None` refuses the whole
/// mixed admission (the per-row program path stands).
fn hg_fold_vocab(
    node: &AggStateData<'_>,
) -> Option<(Vec<crate::pardistinct::PdVocab>, Vec<u16>)> {
    use crate::pardistinct::{PdInt, PdVocab};
    let desc = node.persort.as_ref()?.first_slot.base().tts_tupleDescriptor.as_ref()?;
    let int_kind = |t: ::types_core::Oid| match t {
        INT2OID => Some(PdInt::I16),
        INT4OID => Some(PdInt::I32),
        INT8OID => Some(PdInt::I64),
        _ => None,
    };
    // The aggregate's single plain-Var argument (0-based outer attno) —
    // `pd_derive_spec`'s arg extraction, verbatim.
    let arg_att = |ar: &::types_nodes::primnodes::Aggref<'_>| -> Option<u16> {
        if ar.aggfilter.is_some() || ar.args.len() != 1 {
            return None;
        }
        let tle = ar.args.iter().next()?.as_target_entry()?;
        let v = tle.expr.as_var()?;
        (v.varno == ::execexpr::OUTER_VAR
            && v.varlevelsup == 0
            && v.varattno >= 1
            && (v.varattno as i32) <= desc.natts)
            .then(|| (v.varattno - 1) as u16)
    };
    let mut seen: Vec<bool> = vec![false; node.numtrans];
    for ps in &node.pertrans_sort {
        seen[ps.transno] = true;
    }
    let mut vocab: Vec<PdVocab> = Vec::new();
    let mut fold_atts: Vec<u16> = Vec::new();
    for pa in node.peragg.iter() {
        let transno = pa.transno as usize;
        if seen[transno] {
            continue;
        }
        seen[transno] = true;
        let ar = pa.aggref;
        if !ar.aggdistinct.is_nil() || !ar.aggorder.is_nil() || ar.aggfilter.is_some() {
            return None;
        }
        let att = if ar.args.is_nil() {
            None
        } else {
            let a = arg_att(ar)?;
            let k = int_kind(desc.attr(a as usize).atttypid)?;
            Some((a, k))
        };
        let kind = crate::pardistinct::vocab_kind(ar.aggfnoid, att)?;
        if let Some((a, _)) = att {
            fold_atts.push(a);
        }
        vocab.push(PdVocab { transno: transno as u32, kind });
    }
    if !seen.iter().all(|&s| s) {
        // A transition no peragg names (shared/invisible): refuse.
        return None;
    }
    Some((vocab, fold_atts))
}

/// Arm the sidecar fold vocabulary on a just-begun (empty) build — called by
/// the lanev2 drive exactly when the batched mixed shape's column mapping
/// succeeded (an armed vocab whose batch never engages would only waste the
/// per-group sidecar words; the combine no-ops on all-zero folds).
pub fn agg_hashgroup_arm_fold(
    node: &mut AggStateData<'_>,
    vocab: Vec<crate::pardistinct::PdVocab>,
) {
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    debug_assert_eq!(hg.ngroups(), 0, "fold arms before the first group");
    debug_assert!(hg.fold.is_empty());
    hg.vocab = vocab;
}

/// One clean staged row through the batched fast leg. `key_datums`/`key_nulls`
/// are the row's grouping-key cells (one per key, `agg_hashgroup_batch_shape`
/// order) and `args` the row's DISTINCT-arg cells (one per pertrans). On a
/// probe hit this is the whole row: switch to the group and run `run_row`'s
/// set collect verbatim (park semantics — `direct_att` proved the program
/// parks exactly these cells), then the same shared-set accounting and
/// budget verdict as `agg_hashgroup_accept`. On a probe miss it does
/// NOTHING and returns `NeedSlot`: group creation defers the row as the
/// group's representative (a materialized slot), so the caller must emit
/// the row and route it through the per-row `agg_hashgroup_accept` —
/// byte-identical creation order and rep bytes.
pub enum HgBatchRow {
    /// Row fully absorbed; the payload is `mem() <= budget` (false = the
    /// caller must degrade, exactly as `agg_hashgroup_accept`'s `Ok(false)`).
    Absorbed(bool),
    /// Probe miss: materialize the row and feed it per-row.
    NeedSlot,
}

/// Content bytes of an INLINE (1B- or uncompressed-4B-header) varlena
/// datum, no detoast, no copy. `None` = external/compressed image — the
/// caller routes the row through the per-row path, whose
/// `datum_varlena_packed` detoast produces the identical content bytes.
///
/// # Safety
/// `d` is a non-null live varlena datum whose header (and, for inline
/// images, full body) is readable — the staged-cell contract of the
/// batched accepts (pgrcolumnar staged text cells are decoded in-window
/// images).
#[inline]
unsafe fn inline_varlena_bytes<'a>(d: Datum) -> Option<&'a [u8]> {
    use ::types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — header readable; sizes from the header
    // bound the body reads.
    unsafe {
        if varatt::varatt_is_1b(p) {
            Some(core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            ))
        } else if varatt::varatt_is_4b_u(p) {
            Some(core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ),
                varatt::varsize_4b(p) - varatt::VARHDRSZ,
            ))
        } else {
            None
        }
    }
}

/// Key-extraction outcome of the batched accepts' shared per-row key pass:
/// stage the row's key cells into `words` (+ text content into the probe
/// staging), or defer the row (`None` = a non-inline text image).
#[inline]
fn hg_stage_batch_keys(
    hg: &mut HashGroupedState<'_>,
    read: impl Fn(usize) -> (Datum, bool),
) -> Option<([i64; 32], u32)> {
    let mut words = [0i64; 32];
    let mut nulls = 0u32;
    let mut text_cleared = false;
    for i in 0..hg.nkeys {
        let (d, isnull) = read(i);
        if isnull {
            nulls |= 1 << i;
            words[i] = 0;
            continue;
        }
        match hg.key_kinds[i] {
            HgKeyKind::Int16 => words[i] = d.as_i16() as i64,
            HgKeyKind::Int32 => words[i] = d.as_i32() as i64,
            HgKeyKind::Int64 => words[i] = d.as_i64(),
            HgKeyKind::Text => {
                // SAFETY: staged-cell contract (fn doc above).
                let b = unsafe { inline_varlena_bytes(d) }?;
                if !text_cleared {
                    hg.probe_buf.clear();
                    text_cleared = true;
                }
                let off = hg.probe_buf.len();
                hg.probe_buf.extend_from_slice(b);
                hg.probe_spans[i] = (off as u32, b.len() as u32);
                words[i] = 0;
            }
        }
    }
    Some((words, nulls))
}

/// Read-only group probe over the staged key pass ([`hg_stage_batch_keys`]
/// already ran): the stringhash single-text-key map (`find` — never
/// inserts) or the generic content probe. `None` = probe miss (creation
/// defers to the per-row path).
#[inline]
fn hg_probe_staged(hg: &HashGroupedState<'_>, words: &[i64], nulls: u32) -> Option<u32> {
    if let Some(smap) = hg.smap.as_ref() {
        if nulls != 0 {
            hg.null_group
        } else {
            let (poff, plen) = hg.probe_spans[0];
            let bytes = &hg.probe_buf[poff as usize..(poff + plen) as usize];
            smap.find(bytes, &hg.arena)
        }
    } else {
        let h = hg.key_hash(words, nulls);
        hg.probe(words, nulls, h).0
    }
}

pub fn agg_hashgroup_accept_batch_row(
    node: &mut AggStateData<'_>,
    key_datums: &[Datum],
    key_nulls: &[bool],
    args: &[(Datum, bool)],
    folds: &[(Datum, bool)],
) -> HgBatchRow {
    debug_assert!(matches!(
        node.hashgroup.as_deref(),
        Some(HashGroupedState { phase: HgPhase::Building, .. })
    ));
    let found = {
        let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
        debug_assert_eq!(key_datums.len(), hg.nkeys);
        let Some((words, nulls)) =
            hg_stage_batch_keys(hg, |i| (key_datums[i], key_nulls[i]))
        else {
            // Non-inline text image: the per-row path detoasts it.
            return HgBatchRow::NeedSlot;
        };
        let nkeys = hg.nkeys;
        hg_probe_staged(hg, &words[..nkeys], nulls)
    };
    let Some(g) = found else {
        return HgBatchRow::NeedSlot;
    };
    switch_to(node, g);
    // run_row's set collect, verbatim semantics (flag is always set for a
    // direct_att pertrans: unconditional single-input park, no FILTER).
    for (ps, &(d, isnull)) in node.pertrans_sort.iter_mut().zip(args.iter()) {
        let kind = ps.set_kind.expect("batch shape: set-mode pertrans");
        let dset = ps.dset.get_or_insert_with(DistinctSet::new);
        if isnull {
            dset.seen_null = true;
            continue;
        }
        match kind {
            DistinctKeyKind::Int16 => dset.insert_i64(d.as_i16() as i64),
            DistinctKeyKind::Int32 => dset.insert_i64(d.as_i32() as i64),
            DistinctKeyKind::Int64 => dset.insert_i64(d.as_i64()),
            DistinctKeyKind::Bytes => unreachable!("batch shape excludes byte sets"),
        }
    }
    // Shared-accounting update (the accept's existing-group arm, verbatim).
    let sets: usize = node
        .pertrans_sort
        .iter()
        .map(|ps| ps.dset.as_ref().map_or(0, |d| d.mem_bytes()))
        .sum();
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    let c = g as usize;
    // Vocab folds (mixed-shape admission): the pardistinct worker fold,
    // verbatim, into the group's sidecar words. `folds` carries the staged
    // cells of the arg-bearing vocab entries in vocab order (the shape's
    // `fold_atts` contract).
    if !hg.vocab.is_empty() {
        let base = c * 2 * hg.vocab.len();
        let mut fi = 0usize;
        for (vi, v) in hg.vocab.iter().enumerate() {
            let (acc, cnt) = (base + 2 * vi, base + 2 * vi + 1);
            match v.kind {
                crate::pardistinct::PdVocabKind::CountStar => hg.fold[acc] += 1,
                crate::pardistinct::PdVocabKind::CountAny { .. } => {
                    let (_, isnull) = folds[fi];
                    fi += 1;
                    if !isnull {
                        hg.fold[acc] += 1;
                    }
                }
                crate::pardistinct::PdVocabKind::SumInt { kind, .. }
                | crate::pardistinct::PdVocabKind::AvgInt { kind, .. } => {
                    let (d, isnull) = folds[fi];
                    fi += 1;
                    if !isnull {
                        hg.fold[acc] += kind.read(d);
                        hg.fold[cnt] += 1;
                    }
                }
            }
        }
    }
    hg.total_set_mem = hg.total_set_mem + sets - hg.set_mem[c];
    hg.set_mem[c] = sets;
    HgBatchRow::Absorbed(hg.mem() <= hg.budget)
}

/// Where a batched span stopped (q9q10-serial lane increment 2).
pub enum HgSpanStop {
    /// Every candidate row in `[pos, n)` absorbed (`absorbed` = fast rows;
    /// sel-dead rows are skipped and counted nowhere, as in the per-row leg).
    Done { absorbed: u32 },
    /// Rows `[pos, at)` absorbed; row `at` needs the per-row path (probe
    /// miss — group creation defers the row as its representative — or a
    /// forced-fallback bit). The caller emits + accepts row `at`, then
    /// re-enters at `at + 1`.
    NeedSlot { at: u32, absorbed: u32 },
    /// The shared budget crossed AFTER row `at` was fully absorbed — the
    /// caller degrades (exactly `HgBatchRow::Absorbed(false)`), and the
    /// remaining rows ride the per-row post-degrade path.
    Budget { at: u32, absorbed: u32 },
}

/// Batched span accept: process staged rows `[pos, n)` in ONE call with the
/// loop-invariant state hoisted — no per-row hashgroup deref, no per-row
/// `switch_to` (set parks and vocab folds write the STORAGE arrays directly
/// under the `cur == None` invariant established by the entry `switch_out`),
/// no per-row call marshaling. Semantics are the per-row
/// [`agg_hashgroup_accept_batch_row`] loop, byte-for-byte: same row order,
/// same group-creation order (the stop/resume protocol keeps probe misses
/// at their exact positions), same per-row shared-budget accounting and
/// degrade point.
///
/// `views` carries one `(values, isnull)` cell-pointer pair per column in
/// shape order: `nkeys` grouping keys, then `nargs` DISTINCT args, then the
/// arg-bearing fold-vocab cells. `sel`/`fb` are the staged window's
/// whole-qual and forced-fallback bitmap words.
///
/// # Safety
/// Every pointer pair in `views` must span `n` staged rows and stay valid
/// for the duration of the call (the lanev2 staged-window snapshot
/// contract); `sel`/`fb` must cover bit `n - 1`.
pub unsafe fn agg_hashgroup_accept_batch_span(
    node: &mut AggStateData<'_>,
    views: &[(*const Datum, *const bool)],
    nargs: usize,
    sel: Option<&[u64]>,
    fb: &[u64],
    pos: u32,
    n: u32,
) -> HgSpanStop {
    debug_assert!(matches!(
        node.hashgroup.as_deref(),
        Some(HashGroupedState { phase: HgPhase::Building, .. })
    ));
    // Establish the storage invariant: no group's state loaded in the node.
    switch_out(node);
    let AggStateData { hashgroup, pertrans_sort, .. } = node;
    let hg = hashgroup.as_deref_mut().expect("hashgroup state");
    let nkeys = hg.nkeys;
    let nsort = hg.nsort;
    let nvocab = hg.vocab.len();
    debug_assert_eq!(views.len(), nkeys + nargs + hg_fold_cells(&hg.vocab));
    let mut absorbed = 0u32;
    let mut i = pos;
    while i < n {
        let (w, bit) = ((i / 64) as usize, 1u64 << (i % 64));
        if let Some(s) = sel {
            // Word skip (the qualed-scan survivor-walk precedent): an all-dead sel
            // word advances 64 rows in one test. Forced-fallback rows carry
            // a SET sel bit (the refsort contract), so no NeedSlot row is
            // ever skipped with its word.
            if i % 64 == 0 && i + 64 <= n && s[w] == 0 {
                i += 64;
                continue;
            }
            if s[w] & bit == 0 {
                i += 1;
                continue; // qual-filtered (exact whole-qual verdict)
            }
        }
        if fb[w] & bit != 0 {
            return HgSpanStop::NeedSlot { at: i, absorbed };
        }
        let ii = i as usize;
        let Some((words, nulls)) = hg_stage_batch_keys(hg, |j| {
            let (v, nl) = views[j];
            // SAFETY: caller contract — `views` spans `n` staged rows.
            unsafe { (*v.add(ii), *nl.add(ii)) }
        }) else {
            // Non-inline text image: the per-row path detoasts it.
            return HgSpanStop::NeedSlot { at: i, absorbed };
        };
        let Some(g) = hg_probe_staged(hg, &words[..nkeys], nulls) else {
            return HgSpanStop::NeedSlot { at: i, absorbed };
        };
        let c = g as usize;
        // Set parks, straight into storage (cur == None invariant).
        let mut sets = 0usize;
        for (j, ps) in pertrans_sort.iter().enumerate() {
            debug_assert!(ps.dset.is_none(), "cur == None: every set is in storage");
            let kind = ps.set_kind.expect("batch shape: set-mode pertrans");
            let (v, nl) = views[nkeys + j];
            // SAFETY: caller contract, as above.
            let (d, isnull) = unsafe { (*v.add(ii), *nl.add(ii)) };
            let dset = hg.dsets[c * nsort + j].get_or_insert_with(DistinctSet::new);
            if isnull {
                dset.seen_null = true;
            } else {
                match kind {
                    DistinctKeyKind::Int16 => dset.insert_i64(d.as_i16() as i64),
                    DistinctKeyKind::Int32 => dset.insert_i64(d.as_i32() as i64),
                    DistinctKeyKind::Int64 => dset.insert_i64(d.as_i64()),
                    DistinctKeyKind::Bytes => unreachable!("batch shape excludes byte sets"),
                }
            }
            sets += dset.mem_bytes();
        }
        hg.total_set_mem = hg.total_set_mem + sets - hg.set_mem[c];
        hg.set_mem[c] = sets;
        // Vocab folds (mixed-shape admission), sidecar words.
        if nvocab != 0 {
            let base = c * 2 * nvocab;
            let mut fi = nkeys + nargs;
            for (vi, v) in hg.vocab.iter().enumerate() {
                let (acc, cnt) = (base + 2 * vi, base + 2 * vi + 1);
                match v.kind {
                    crate::pardistinct::PdVocabKind::CountStar => hg.fold[acc] += 1,
                    crate::pardistinct::PdVocabKind::CountAny { .. } => {
                        let (_, nl) = views[fi];
                        fi += 1;
                        // SAFETY: caller contract, as above.
                        if !unsafe { *nl.add(ii) } {
                            hg.fold[acc] += 1;
                        }
                    }
                    crate::pardistinct::PdVocabKind::SumInt { kind, .. }
                    | crate::pardistinct::PdVocabKind::AvgInt { kind, .. } => {
                        let (vv, nl) = views[fi];
                        fi += 1;
                        // SAFETY: caller contract, as above.
                        let (d, isnull) = unsafe { (*vv.add(ii), *nl.add(ii)) };
                        if !isnull {
                            hg.fold[acc] += kind.read(d);
                            hg.fold[cnt] += 1;
                        }
                    }
                }
            }
        }
        absorbed += 1;
        if hg.mem() > hg.budget {
            return HgSpanStop::Budget { at: i, absorbed };
        }
        i += 1;
    }
    HgSpanStop::Done { absorbed }
}

/// Number of staged fold cells a vocab consumes (arg-bearing entries only).
pub fn hg_fold_cells(vocab: &[crate::pardistinct::PdVocab]) -> usize {
    vocab
        .iter()
        .filter(|v| !matches!(v.kind, crate::pardistinct::PdVocabKind::CountStar))
        .count()
}

/// Merge the sidecar fold words into the REAL per-group transition states —
/// once, with no group loaded (post `switch_out`): at build finish (after
/// the deferred-rep replay advanced `pergroup` through the program) or at
/// degrade entry (reps ride the narrowed sort instead). State arms mirror
/// `agg_hashgroup_adopt_merged`'s vocab materialization, COMBINING with the
/// program-accumulated state instead of overwriting:
/// - count(*)/count(x): int8inc's non-null i64 state; checked add with C's
///   exact "bigint out of range" surface (`count_distinct_apply`).
/// - sum(int2/4): int8 state, NULL iff no non-null input ever arrived (the
///   non-strict null-initval law) — a zero-count fold leaves it untouched.
/// - avg(int2/4): Int8TransTypeData int8[2] {count, sum}; a fresh array
///   image is copied into aggcontext (never mutated in place — the state
///   may still be the shared initval when no per-row row touched it).
/// Fold words zero after the merge (double-apply guard).
fn hg_fold_combine(node: &mut AggStateData<'_>) -> PgResult<()> {
    use crate::pardistinct::PdVocabKind;
    let AggStateData { hashgroup, agg_node, trans_typ, .. } = node;
    let Some(hg) = hashgroup.as_deref_mut() else {
        return Ok(());
    };
    if hg.vocab.is_empty() {
        return Ok(());
    }
    debug_assert!(hg.cur.is_none(), "fold combine runs with no group loaded");
    let nvocab = hg.vocab.len();
    let numtrans = hg.numtrans;
    let n = hg.ngroups();
    for g in 0..n {
        for (vi, v) in hg.vocab.iter().enumerate() {
            let acc = hg.fold[g * 2 * nvocab + 2 * vi];
            let cnt = hg.fold[g * 2 * nvocab + 2 * vi + 1];
            let pg = &mut hg.pergroup[g * numtrans + v.transno as usize];
            match v.kind {
                // NULL authority note: `trans_value_is_null` is the state's
                // SQL-null truth. `no_trans_value` is NOT — it is the
                // strict-init latch only (C's noTransValue), which the
                // non-strict transition steps (int2/int4_sum's class,
                // `agg_trans_byval`) faithfully never clear: a sum state
                // advanced by the rep replay still has `no_trans_value ==
                // true` from its NULL initval. Deciding replace-vs-add on
                // that stale latch overwrote the rep's contribution (the
                // mixed-fold parity bug: every group's sum short by exactly its
                // deferred representative's value).
                PdVocabKind::CountStar | PdVocabKind::CountAny { .. } => {
                    if acc != 0 {
                        debug_assert!(!pg.trans_value_is_null, "count initval 0 is never NULL");
                        // SAFETY: live pergroup slot holding the non-null
                        // by-val i64 count state (initval '0'; int8inc/
                        // int8inc_any only ever produce non-null i64).
                        unsafe { crate::count_distinct_apply(pg, acc)? };
                    }
                }
                PdVocabKind::SumInt { .. } => {
                    if cnt > 0 {
                        if pg.trans_value_is_null {
                            pg.trans_value = Datum::from_i64(acc);
                        } else {
                            pg.trans_value =
                                Datum::from_i64(pg.trans_value.as_i64().wrapping_add(acc));
                        }
                        pg.trans_value_is_null = false;
                        pg.no_trans_value = false;
                    }
                }
                PdVocabKind::AvgInt { .. } => {
                    if cnt > 0 {
                        // Read the current Int8TransTypeData image (initcond
                        // '{0,0}' — never NULL; every producer in this
                        // engine emits the canonical 40-byte 4B-header
                        // no-nulls int8[2]).
                        if pg.trans_value_is_null {
                            return Err(Box::new(::types_error::PgError::error(
                                "hashgroup fold: NULL avg transition state".to_string(),
                            )));
                        }
                        let hdr = unsafe {
                            core::slice::from_raw_parts(
                                pg.trans_value.as_usize() as *const u8,
                                4,
                            )
                        };
                        let word = u32::from_ne_bytes(hdr.try_into().expect("4 bytes"));
                        if word != ::types_tuple::varatt::set_varsize_4b_word(40) {
                            return Err(Box::new(::types_error::PgError::error(
                                "hashgroup fold: unexpected avg transition state image"
                                    .to_string(),
                            )));
                        }
                        let full = unsafe {
                            core::slice::from_raw_parts(
                                pg.trans_value.as_usize() as *const u8,
                                40,
                            )
                        };
                        let ocnt =
                            i64::from_ne_bytes(full[24..32].try_into().expect("8 bytes"));
                        let osum =
                            i64::from_ne_bytes(full[32..40].try_into().expect("8 bytes"));
                        let mut img = [0u8; 40];
                        img[0..4].copy_from_slice(
                            &::types_tuple::varatt::set_varsize_4b_word(40).to_ne_bytes(),
                        );
                        img[4..8].copy_from_slice(&1i32.to_ne_bytes()); // ndim
                        img[8..12].copy_from_slice(&0i32.to_ne_bytes()); // dataoffset
                        img[12..16].copy_from_slice(&INT8OID.to_ne_bytes()); // elemtype
                        img[16..20].copy_from_slice(&2i32.to_ne_bytes()); // dims[0]
                        img[20..24].copy_from_slice(&1i32.to_ne_bytes()); // lbound[0]
                        img[24..32].copy_from_slice(&ocnt.wrapping_add(cnt).to_ne_bytes());
                        img[32..40].copy_from_slice(&osum.wrapping_add(acc).to_ne_bytes());
                        let typ = trans_typ[v.transno as usize];
                        // SAFETY: `img` is a live, well-formed varlena image
                        // for the copy's duration; agg_node live, no &mut.
                        let copied = unsafe {
                            ::execexpr::agg_datum_copy(
                                agg_node.as_ref().aggcontext(),
                                Datum::from_usize(img.as_ptr() as usize),
                                typ.len,
                            )?
                        };
                        pg.trans_value = copied;
                        pg.trans_value_is_null = false;
                        pg.no_trans_value = false;
                    }
                }
            }
        }
    }
    hg.fold.iter_mut().for_each(|w| *w = 0);
    Ok(())
}

/// Build complete (input exhausted, no degrade): replay every group's
/// DEFERRED representative row through the transition program, then order
/// the groups by the plan Sort's prefix (module doc: total, C-identical
/// order) and flip to the emit phase. The rep replay grows each touched set
/// by at most one value past the budget check — bounded overshoot, tolerated
/// (the input is already fully consumed; nothing further accumulates).
pub fn agg_hashgroup_finish_build<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(matches!(
        node.hashgroup.as_deref(),
        Some(HashGroupedState { phase: HgPhase::Building, .. })
    ));
    let n = node.hashgroup.as_deref().expect("hashgroup state").ngroups();
    for g in 0..n {
        {
            let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
            let rep = hg.reps[g].as_ref().expect("unconsumed deferred representative");
            let mcx = hg.mcx;
            // SAFETY: the rep image is a live owned minimal tuple, borrowed
            // by the slot only for this replay (overwritten next iteration,
            // cleared after the loop).
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(
                    &mut hg.rep_slot,
                    mcx,
                    NonNull::new_unchecked(rep.as_ptr().cast_mut().cast()),
                );
            }
        }
        switch_to(node, g as u32);
        run_row(node, estate, RowSlot::Rep)?;
    }
    // Park the last group's state back into storage, merge the sidecar
    // folds into the real trans states (mixed-shape batch), then order.
    switch_out(node);
    hg_fold_combine(node)?;
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    let mcx = hg.mcx;
    exectuples::exec_clear_tuple(&mut hg.rep_slot, mcx);
    let order =
        order_groups(&hg.keys, &hg.keynulls, &hg.order_spec, hg.nkeys, &hg.key_kinds, &hg.arena, n)?;
    hg.phase = HgPhase::Emit { order, pos: 0 };
    Ok(())
}

/// Compare two group rows — possibly from DIFFERENT key stores — under the
/// plan Sort's prefix `spec` (total, C-identical order — module doc). Each
/// side is `(keys, keynulls, arena, row)` in the packed-span convention.
/// The ONE ordering authority shared by the build finish, the merged
/// adoption ([`order_groups`]), and the runtime distinct sink's paremit
/// bucket merge — byte-identity across those arms depends on this being
/// the same comparator. Distinct groups never compare Equal on the full
/// prefix (unique keys; deterministic collations tie-break by bytes).
pub(crate) fn cmp_group_rows(
    spec: &[HashGroupOrderKey],
    nkeys: usize,
    kinds: &[HgKeyKind],
    a: (&[i64], &[u32], &[u8], usize),
    b: (&[i64], &[u32], &[u8], usize),
) -> PgResult<core::cmp::Ordering> {
    let (akeys, anulls, aarena, ai) = a;
    let (bkeys, bnulls, barena, bi) = b;
    for k in spec.iter() {
        let (na, nb) = (
            anulls[ai] & (1 << k.key_idx) != 0,
            bnulls[bi] & (1 << k.key_idx) != 0,
        );
        let ord = match (na, nb) {
            (true, true) => core::cmp::Ordering::Equal,
            (true, false) => {
                if k.nulls_first {
                    core::cmp::Ordering::Less
                } else {
                    core::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if k.nulls_first {
                    core::cmp::Ordering::Greater
                } else {
                    core::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let (wa, wb) =
                    (akeys[ai * nkeys + k.key_idx], bkeys[bi * nkeys + k.key_idx]);
                let ord = if kinds[k.key_idx] == HgKeyKind::Text {
                    // C's text btree order: varstr_cmp under the plan
                    // Sort's collation (module doc — deterministic
                    // collations tie-break by bytes, so byte-distinct
                    // keys never compare equal).
                    let (oa, la) = unpack_span(wa);
                    let (ob, lb) = unpack_span(wb);
                    ::varlena::varstr_cmp(
                        &aarena[oa..oa + la],
                        &barena[ob..ob + lb],
                        k.collation,
                    )?
                    .cmp(&0)
                } else {
                    wa.cmp(&wb)
                };
                if k.desc {
                    ord.reverse()
                } else {
                    ord
                }
            }
        };
        if ord != core::cmp::Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(core::cmp::Ordering::Equal)
}

/// Order group indices by the plan Sort's prefix (total, C-identical order —
/// module doc). Shared by the build finish, the parallel-partials merged
/// adoption, and the runtime distinct sink's per-partition paremit bucket
/// build (the same comparator then drives the leader's cross-bucket merge).
pub(crate) fn order_groups(
    keys: &[i64],
    keynulls: &[u32],
    spec: &[HashGroupOrderKey],
    nkeys: usize,
    kinds: &[HgKeyKind],
    arena: &[u8],
    n: usize,
) -> PgResult<Vec<u32>> {
    let mut order: Vec<u32> = (0..n as u32).collect();
    // varstr_cmp is fallible (collation seams); the comparator parks the
    // first error and the sort result is discarded on Err below.
    let mut cmp_err = None;
    order.sort_unstable_by(|&a, &b| {
        let (a, b) = (a as usize, b as usize);
        match cmp_group_rows(
            spec,
            nkeys,
            kinds,
            (keys, keynulls, arena, a),
            (keys, keynulls, arena, b),
        ) {
            Ok(ord) => {
                debug_assert!(
                    ord != core::cmp::Ordering::Equal || a == b,
                    "distinct groups compare equal on the full prefix"
                );
                ord
            }
            Err(e) => {
                if cmp_err.is_none() {
                    cmp_err = Some(e);
                }
                core::cmp::Ordering::Equal
            }
        }
    });
    if let Some(e) = cmp_err {
        return Err(e);
    }
    Ok(order)
}

/// Emit the next group in prefix order through the UNCHANGED sorted-agg
/// finalize/HAVING/project tail. `Ok(None)` = stream end (`agg_done` set,
/// state dropped); `Ok(Some(None))` = HAVING rejected this group (caller
/// loops); `Ok(Some(Some(slot)))` = one group row.
pub fn agg_hashgroup_emit_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let g = {
        let hg = node.hashgroup.as_deref_mut().expect("hashgroup emit without state");
        let HgPhase::Emit { order, pos } = &mut hg.phase else {
            unreachable!("hashgroup emit outside the emit phase")
        };
        if *pos == order.len() {
            None
        } else {
            let g = order[*pos];
            *pos += 1;
            Some(g)
        }
    };
    let Some(g) = g else {
        // Stream end: C's agg_done arm. Clear the borrowed rep image out of
        // the first slot before the reps drop with the state.
        node.agg_done = true;
        let mcx = estate.es_query_cxt;
        if let Some(ps) = node.persort.as_mut() {
            exectuples::exec_clear_tuple(&mut ps.first_slot, mcx);
        }
        agg_hashgroup_reset(node);
        return Ok(None);
    };
    // Per-group output memory reset (the group begin's reset, WITHOUT the
    // aggcontext reset — other groups' by-ref transvalues live there).
    estate.reset_expr_context(node.ps_ExprContext);
    switch_to(node, g);
    {
        let AggStateData { hashgroup, persort, .. } = node;
        let hg = hashgroup.as_deref_mut().expect("hashgroup state");
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let rep = hg.reps[g as usize].as_ref().expect("unconsumed representative");
        let mcx = hg.mcx;
        // SAFETY: the rep image outlives the slot's use of it (the state —
        // and its reps — outlives this emit call; the end-of-stream arm
        // clears the slot before dropping them).
        unsafe {
            exectuples::exec_store_minimal_tuple_ptr(
                &mut ps.first_slot,
                mcx,
                NonNull::new_unchecked(rep.as_ptr().cast_mut().cast()),
            );
        }
    }
    let row = agg_sorted_emit(node, estate)?;
    Ok(Some(row))
}

/// Degrade step 1 (drive-side iteration): load the next group's deferred
/// representative into the arm's spare outer slot and hand it out for the
/// narrowed tuplesort put. `None` = all representatives dumped.
pub fn agg_hashgroup_next_rep<'a, 'mcx>(
    node: &'a mut AggStateData<'mcx>,
) -> Option<&'a mut SlotData<'mcx>> {
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    debug_assert!(matches!(hg.phase, HgPhase::Building));
    // Drop the previous rep (the caller's put copied it into the tuplesort).
    if hg.rep_cursor > 0 {
        hg.reps[hg.rep_cursor - 1] = None;
    }
    if hg.rep_cursor == hg.ngroups() {
        let mcx = hg.mcx;
        exectuples::exec_clear_tuple(&mut hg.rep_slot, mcx);
        return None;
    }
    let g = hg.rep_cursor;
    hg.rep_cursor += 1;
    let rep = hg.reps[g].as_ref().expect("undumped representative");
    let mcx = hg.mcx;
    // SAFETY: the rep image stays live until the next next_rep call, which
    // is after the caller's tuplesort put copied it.
    unsafe {
        exectuples::exec_store_minimal_tuple_ptr(
            &mut hg.rep_slot,
            mcx,
            NonNull::new_unchecked(rep.as_ptr().cast_mut().cast()),
        );
    }
    Some(&mut hg.rep_slot)
}

/// Degrade step 2: flip to the residual phase — the table becomes the
/// narrow-sort emit chain's partial-state store (`residual_preload`). The
/// CURRENT group's live state parks back into storage first, then the
/// sidecar folds (mixed-shape batch) merge into the stored trans states —
/// the resurrected partials must carry every batch-absorbed row.
pub fn agg_hashgroup_set_residual(node: &mut AggStateData<'_>) -> PgResult<()> {
    switch_out(node);
    hg_fold_combine(node)?;
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    debug_assert!(matches!(hg.phase, HgPhase::Building));
    debug_assert_eq!(hg.rep_cursor, hg.ngroups(), "every representative rides the sort");
    hg.phase = HgPhase::Residual;
    Ok(())
}

/// Lane-v2 pardistinct (pardistinct.rs): adopt a MERGED parallel-partial
/// result straight into this arm's Emit phase — the unchanged
/// finalize/HAVING/project tail then emits the merged groups in the plan
/// Sort's prefix order, replaying the merged exact-DISTINCT sets through
/// the real transfns exactly as the serial build would.
///
/// Group representatives are SYNTHESIZED: key columns from the merged key
/// words, every other column NULL. Sound for the same reason the serial
/// arm's first-row representative is: the only columns an Agg output can
/// reference are grouping columns (rebuilt byte-exactly — representational
/// equality) and aggregates.
///
/// Vocab states materialize as the REAL trans states the C build would
/// have left: count/sum → int8 datums; avg(int2/4) → an Int8TransTypeData
/// int8[2] array {count, sum} allocated in aggcontext.
pub fn agg_hashgroup_adopt_merged<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    merged: crate::pardistinct::PdMerged<'mcx>,
    vocab: &[crate::pardistinct::PdVocab],
    order_spec: Vec<HashGroupOrderKey>,
) -> PgResult<()> {
    use crate::pardistinct::PdVocabKind;
    const INT2OID: ::types_core::Oid = 21;
    const INT4OID: ::types_core::Oid = 23;
    const INT8OID: ::types_core::Oid = 20;
    const TEXTOID: ::types_core::Oid = 25;
    const VARCHAROID: ::types_core::Oid = 1043;
    debug_assert!(node.hashgroup.is_none());
    debug_assert!(node.force_distinct_set);
    let mcx = estate.es_query_cxt;
    let ps = node.persort.as_ref().expect("sorted Agg has persort");
    let desc = ps
        .first_slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("persort slots carry the outer desc")
        .clone();
    let mut key_atts = Vec::with_capacity(node.plan.grpColIdx.len());
    let mut key_kinds = Vec::with_capacity(node.plan.grpColIdx.len());
    let mut max_att = 0i32;
    for &col in node.plan.grpColIdx {
        key_atts.push((col - 1) as u16);
        max_att = max_att.max(col as i32);
        key_kinds.push(match desc.attr((col - 1) as usize).atttypid {
            INT2OID => HgKeyKind::Int16,
            INT4OID => HgKeyKind::Int32,
            // distinct-bytes car: text keys arrive as arena spans in the
            // merged result (`PdMerged::key_arena`) — the same packed-span
            // convention this arm's serial build uses.
            TEXTOID | VARCHAROID => HgKeyKind::Text,
            _ => HgKeyKind::Int64,
        });
    }
    let nkeys = key_atts.len();
    let numtrans = node.numtrans;
    let nsort = node.pertrans_sort.len();
    let n = merged.ngroups;
    debug_assert_eq!(merged.dsets.len(), n * nsort);
    let nvocab = vocab.len();
    // Synthesized representatives: a scratch virtual slot per the sort-dump
    // discipline (lib.rs collect degrade), copied minimal per group.
    let mut reps: Vec<Option<MinimalTuple<'mcx>>> = Vec::with_capacity(n);
    {
        let mut scratch =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
        let natts = desc.natts as usize;
        // Text-key scratch varlenas (distinct-bytes car): one 4B-header
        // text image per text key column, rebuilt per group and kept live
        // until the minimal-tuple copy below detaches it (the AvgInt
        // stack-image discipline).
        let mut text_bufs: Vec<Vec<u8>> = vec![Vec::new(); nkeys];
        for g in 0..n {
            exectuples::exec_clear_tuple(&mut scratch, mcx);
            {
                let base = scratch.base_mut();
                for i in 0..natts {
                    base.tts_isnull[i] = true;
                    base.tts_values[i] = ::datum::Datum::null();
                }
                for (i, (&att, &kind)) in key_atts.iter().zip(key_kinds.iter()).enumerate() {
                    if merged.keynulls[g] & (1 << i) != 0 {
                        continue;
                    }
                    let w = merged.keys[g * nkeys + i];
                    base.tts_isnull[att as usize] = false;
                    base.tts_values[att as usize] = match kind {
                        HgKeyKind::Int16 => ::datum::Datum::from_i16(w as i16),
                        HgKeyKind::Int32 => ::datum::Datum::from_i32(w as i32),
                        HgKeyKind::Int64 => ::datum::Datum::from_i64(w),
                        HgKeyKind::Text => {
                            let (off, len) = unpack_span(w);
                            let buf = &mut text_bufs[i];
                            buf.clear();
                            buf.extend_from_slice(
                                &::types_tuple::varatt::set_varsize_4b_word((len + 4) as u32)
                                    .to_ne_bytes(),
                            );
                            buf.extend_from_slice(&merged.key_arena[off..off + len]);
                            // Live until the minimal-tuple copy this
                            // iteration; rebuilt next group.
                            ::datum::Datum::from_usize(buf.as_ptr() as usize)
                        }
                    };
                }
            }
            exectuples::exec_store_virtual_tuple(&mut scratch);
            reps.push(Some(exectuples::exec_copy_slot_minimal_tuple(&mut scratch, mcx, mcx, 0)?));
        }
    }
    // Per-group trans states: init values, then the vocab overrides.
    let mut pergroup: Vec<AggPerGroup> = Vec::with_capacity(n * numtrans);
    for g in 0..n {
        for (transno, init) in node.trans_init.iter().enumerate() {
            let typ = node.trans_typ[transno];
            let value = if !init.isnull && !typ.byval {
                // SAFETY: node-lifetime initval datum; agg_node live, no &mut.
                unsafe {
                    ::execexpr::agg_datum_copy(
                        node.agg_node.as_ref().aggcontext(),
                        init.value,
                        typ.len,
                    )?
                }
            } else {
                init.value
            };
            pergroup.push(AggPerGroup {
                trans_value: value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
        for (vi, v) in vocab.iter().enumerate() {
            let acc = merged.states[g * 2 * nvocab + 2 * vi];
            let cnt = merged.states[g * 2 * nvocab + 2 * vi + 1];
            let pg = &mut pergroup[g * numtrans + v.transno as usize];
            match v.kind {
                PdVocabKind::CountStar | PdVocabKind::CountAny { .. } => {
                    pg.trans_value = ::datum::Datum::from_i64(acc);
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                }
                PdVocabKind::SumInt { .. } => {
                    // int2/4_sum: NULL iff no non-null input ever arrived.
                    if cnt > 0 {
                        pg.trans_value = ::datum::Datum::from_i64(acc);
                        pg.trans_value_is_null = false;
                        pg.no_trans_value = false;
                    }
                }
                PdVocabKind::AvgInt { .. } => {
                    // Int8TransTypeData {count, sum} — a 1-D no-nulls int8[2]
                    // array image, copied into aggcontext.
                    let mut img = [0u8; 40];
                    img[0..4].copy_from_slice(
                        &::types_tuple::varatt::set_varsize_4b_word(40).to_ne_bytes(),
                    );
                    img[4..8].copy_from_slice(&1i32.to_ne_bytes()); // ndim
                    img[8..12].copy_from_slice(&0i32.to_ne_bytes()); // dataoffset
                    img[12..16].copy_from_slice(&INT8OID.to_ne_bytes()); // elemtype
                    img[16..20].copy_from_slice(&2i32.to_ne_bytes()); // dims[0]
                    img[20..24].copy_from_slice(&1i32.to_ne_bytes()); // lbound[0]
                    img[24..32].copy_from_slice(&cnt.to_ne_bytes());
                    img[32..40].copy_from_slice(&acc.to_ne_bytes());
                    let typ = node.trans_typ[v.transno as usize];
                    // SAFETY: `img` is a live, well-formed varlena image for
                    // the copy's duration; agg_node live, no &mut.
                    let copied = unsafe {
                        ::execexpr::agg_datum_copy(
                            node.agg_node.as_ref().aggcontext(),
                            ::datum::Datum::from_usize(img.as_ptr() as usize),
                            typ.len,
                        )?
                    };
                    pg.trans_value = copied;
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                }
            }
        }
    }
    let hashes: Vec<u64> = (0..n)
        .map(|g| crate::pardistinct::key_hash(&merged.keys[g * nkeys..(g + 1) * nkeys], merged.keynulls[g]))
        .collect();
    let set_mem: Vec<usize> = (0..n)
        .map(|g| {
            merged.dsets[g * nsort..(g + 1) * nsort]
                .iter()
                .map(|d| d.as_ref().map_or(0, |s| s.mem_bytes()))
                .sum()
        })
        .collect();
    let total_set_mem = set_mem.iter().sum();
    let order = order_groups(
        &merged.keys,
        &merged.keynulls,
        &order_spec,
        nkeys,
        &key_kinds,
        &merged.key_arena,
        n,
    )?;
    let rep_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    node.hashgroup = Some(Box::new(HashGroupedState {
        phase: HgPhase::Emit { order, pos: 0 },
        key_atts,
        key_kinds,
        max_att,
        order_spec,
        nkeys,
        numtrans,
        nsort,
        table: Vec::new(),
        hashes,
        keys: merged.keys,
        // distinct-bytes car: text-key content rides the merged arena
        // (spans packed in `keys` — this arm's own convention).
        arena: merged.key_arena,
        probe_buf: Vec::new(),
        probe_spans: vec![(0, 0); nkeys],
        keynulls: merged.keynulls,
        reps,
        pergroup,
        dsets: merged.dsets,
        set_mem,
        // Adoption materializes REAL trans states above; no sidecar folds.
        vocab: Vec::new(),
        fold: Vec::new(),
        consumed: vec![false; n],
        remaining: n,
        cur: None,
        base_mem: 0,
        total_set_mem,
        budget: hashgroup_budget(),
        rep_slot,
        rep_cursor: 0,
        // The merged adoption emits only (no probing); the stringhash
        // table never engages here.
        smap: None,
        null_group: None,
        mcx,
    }));
    // The emit starts with NO group loaded; clear AND DROP leftover
    // pertrans set state (begin's discipline — switch_to asserts no set is
    // loaded between groups).
    for ps in node.pertrans_sort.iter_mut() {
        if let Some(mut d) = ps.dset.take() {
            d.clear();
        }
        debug_assert!(!ps.dset_degraded);
    }
    Ok(())
}

/// The residual-phase group-begin hook (called from `initialize_aggregates`
/// — the seam BOTH the lane emit chain and the C pull-loop fallback pass
/// through): if the beginning group (its first tuple already sits in
/// `persort.first_slot`) has saved partial state, install it — pergroup
/// values over the freshly initialized ones, sets into the pertrans slots —
/// so pre-degrade rows count exactly once. Drops the whole state once every
/// residual group has been consumed (the aggcontext reset then resumes).
pub(crate) fn residual_preload<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    if !agg_hashgroup_residual_active(node) {
        return Ok(());
    }
    let tmp = node.tmpcontext;
    let mut words = [0i64; 32];
    let mut text_datums = [Datum::null(); 32];
    let hit = {
        let AggStateData { hashgroup, persort, .. } = node;
        let hg = hashgroup.as_deref_mut().expect("residual state");
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let nkeys = hg.nkeys;
        let nulls = read_key_datums(
            &mut ps.first_slot,
            &hg.key_atts,
            &hg.key_kinds,
            hg.max_att,
            &mut words[..nkeys],
            &mut text_datums[..nkeys],
        );
        // Detoast copies land in per-tuple memory (reset by the group's row
        // processing, exactly as the accept path's).
        hg.stage_text_keys(estate, tmp, &text_datums[..nkeys], nulls)?;
        let found = if let Some(smap) = hg.smap.as_ref() {
            if nulls != 0 {
                hg.null_group
            } else {
                let (poff, plen) = hg.probe_spans[0];
                let bytes = &hg.probe_buf[poff as usize..(poff + plen) as usize];
                smap.find(bytes, &hg.arena)
            }
        } else {
            let h = hg.key_hash(&words[..nkeys], nulls);
            hg.probe(&words[..nkeys], nulls, h).0
        };
        found.filter(|&g| !hg.consumed[g as usize])
    };
    if let Some(g) = hit {
        let AggStateData { hashgroup, pergroup_base, pertrans_sort, numtrans, .. } = node;
        let hg = hashgroup.as_deref_mut().expect("residual state");
        let gi = g as usize;
        hg.consumed[gi] = true;
        hg.remaining -= 1;
        // SAFETY: as switch_out — once-allocated numtrans-element arrays.
        unsafe {
            core::ptr::copy_nonoverlapping(
                hg.pergroup.as_ptr().add(gi * hg.numtrans),
                pergroup_base.as_ptr(),
                *numtrans,
            );
        }
        for (j, ps) in pertrans_sort.iter_mut().enumerate() {
            // restart_pertrans_sortstates just cleared the slot's set; the
            // saved one replaces it.
            debug_assert!(ps.dset.as_ref().is_none_or(|d| d.len() == 0 && !d.seen_null));
            ps.dset = hg.dsets[gi * hg.nsort + j].take();
        }
    }
    if node.hashgroup.as_deref().is_some_and(|hg| hg.remaining == 0) {
        agg_hashgroup_reset(node);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_pack_roundtrip() {
        for &(off, len) in
            &[(0usize, 0usize), (1, 1), (0xdead_beef, 0x7fff_ffff), (u32::MAX as usize, 0)]
        {
            assert_eq!(unpack_span(pack_span(off, len)), (off, len));
        }
    }

    #[test]
    fn byte_fold_separates_length_and_content() {
        let h = |b: &[u8]| fold_bytes(0x1234_5678, b);
        // Equal bytes hash equal (the probe prefilter's requirement)...
        assert_eq!(h(b"search phrase"), h(b"search phrase"));
        assert_eq!(h(b""), h(b""));
        // ...and the padded tail must not collide with explicit zero bytes
        // or a shorter prefix (the length round).
        assert_ne!(h(b"abc"), h(b"abc\0"));
        assert_ne!(h(b"abc"), h(b"ab"));
        assert_ne!(h(b"12345678"), h(b"1234567"));
    }
}
