//! Lane-v2 hash-grouped exact-DISTINCT aggregation — the uniqexact2 grouped
//! narrow-sort arm's named follow-up (lane-v2-distincthash). For the
//! ClickBench Q9/Q10 plan shape — `Sort(group cols, distinct arg) →
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
//!     all-integer group columns, so word equality == the grouping equality
//!     operator's verdict, NULL keys collapsing to same-group exactly as C's
//!     grouping equality does);
//!   * same group ORDER: groups emit sorted by the plan Sort's key prefix
//!     under the exact btree integer order (signed word compare, the plan's
//!     ASC/DESC + NULLS FIRST/LAST flags). The prefix covers every grouping
//!     column (the narrow arm's multiset check), and two DISTINCT groups
//!     cannot compare equal on all of them, so the order is total and equals
//!     the order C's row sort induces on group boundaries;
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

use ::execexpr::{exec_eval_expr, AggPerGroup, EvalSlots};
use ::executils::{EStateData, ExecSlotId};
use ::heaptuple::MinimalTuple;
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_slot::{SlotData, TupleSlotKind};

use crate::distinctset::{DistinctKeyKind, DistinctSet};
use crate::{agg_sorted_emit, AggStateData};

/// One emit-order key: `key_idx` indexes the GROUP KEY WORDS (the admission
/// proved the prefix is a permutation of the grouping columns), `desc` /
/// `nulls_first` are the plan Sort's flags for that prefix position.
pub struct HashGroupOrderKey {
    pub key_idx: usize,
    pub desc: bool,
    pub nulls_first: bool,
}

/// Integer group-key representation (the admission's all-integer rule; the
/// stored word is the sign-extended value, so word equality is the grouping
/// operator's equality and signed word order is the btree operator order).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HgKeyKind {
    Int16,
    Int32,
    Int64,
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
    /// Group g's key words at `[g*nkeys .. (g+1)*nkeys]` (sign-extended).
    keys: Vec<i64>,
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
    mcx: Mcx<'mcx>,
}

impl HashGroupedState<'_> {
    #[inline]
    fn ngroups(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    fn mem(&self) -> usize {
        self.base_mem + self.total_set_mem
    }
}

/// Structural admission for the hash-grouped arm, ON TOP of the narrow-sort
/// admission (`agg_sorted_distinct_narrow_admissible`, re-checked here):
/// every grouping column is int2/int4/int8 (word-packable, representational
/// equality already proved) and every internal-sort entry's set kind is an
/// integer kind (no text sets in v1 — the narrow-sort arm keeps those).
/// Group-col count capped at 32 (the NULL bitmask word).
pub fn agg_hashgroup_admissible(node: &AggStateData<'_>) -> bool {
    const INT2OID: ::types_core::Oid = 21;
    const INT4OID: ::types_core::Oid = 23;
    const INT8OID: ::types_core::Oid = 20;
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
        if !matches!(t, INT2OID | INT4OID | INT8OID) {
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
pub fn agg_hashgroup_economical(node: &AggStateData<'_>, force: bool) -> bool {
    if force {
        return true;
    }
    const PER_GROUP_EST: f64 = 256.0;
    let est_groups = (node.plan.numGroups as f64).max(1.0);
    est_groups * PER_GROUP_EST * 2.0 <= hashgroup_budget() as f64
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
        let mcx = hg.mcx;
        exectuples::exec_clear_tuple(&mut hg.rep_slot, mcx);
        for d in hg.dsets.iter_mut().flatten() {
            d.clear();
        }
        // The node-side pertrans dset slots may hold the current group's
        // swapped-in sets; the group-boundary restart clears those.
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
    const INT2OID: ::types_core::Oid = 21;
    const INT4OID: ::types_core::Oid = 23;
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
            _ => HgKeyKind::Int64,
        });
    }
    debug_assert_eq!(order_spec.len(), key_atts.len());
    debug_assert!(order_spec.iter().all(|k| k.key_idx < key_atts.len()));
    let nkeys = key_atts.len();
    let rep_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
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
        keynulls: Vec::new(),
        reps: Vec::new(),
        pergroup: Vec::new(),
        dsets: Vec::new(),
        set_mem: Vec::new(),
        consumed: Vec::new(),
        remaining: 0,
        cur: None,
        base_mem: INIT_TABLE * core::mem::size_of::<u32>(),
        total_set_mem: 0,
        budget: hashgroup_budget(),
        rep_slot,
        rep_cursor: 0,
        mcx,
    }));
    // The build starts with NO group loaded; the node's own pergroup array
    // is the swap scratch. Clear leftover pertrans set state (a rescan can
    // leave a cut-short group's set behind).
    for ps in node.pertrans_sort.iter_mut() {
        if let Some(d) = ps.dset.as_mut() {
            d.clear();
        }
        debug_assert!(!ps.dset_degraded);
    }
    Ok(())
}

/// Sign-extended key words + NULL bitmask for the slot's grouping columns.
fn extract_keys(
    slot: &mut SlotData<'_>,
    key_atts: &[u16],
    key_kinds: &[HgKeyKind],
    max_att: i32,
    words: &mut [i64],
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
        };
    }
    nulls
}

#[inline]
fn key_hash(words: &[i64], nulls: u32) -> u64 {
    let mut h = (nulls as u64) ^ 0x9e37_79b9_7f4a_7c15;
    for &w in words {
        h = mix64(h ^ (w as u64));
    }
    h
}

impl HashGroupedState<'_> {
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
                        && &self.keys[g * self.nkeys..(g + 1) * self.nkeys] == words
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

/// Create a new group from the current row: push key/hash/rep/init-state.
/// The row itself is DEFERRED (module doc — the degrade path's sort
/// representative). Does NOT make the group current.
fn create_group<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    id: ExecSlotId,
    words: &[i64],
    nulls: u32,
    h: u64,
    slot_idx: usize,
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
    hg.hashes.push(h);
    hg.keynulls.push(nulls);
    hg.keys.extend_from_slice(words);
    hg.reps.push(Some(rep));
    hg.pergroup.extend(init_state);
    for _ in 0..hg.nsort {
        hg.dsets.push(None);
    }
    hg.set_mem.push(0);
    hg.consumed.push(false);
    hg.remaining += 1;
    hg.table[slot_idx] = hg.ngroups() as u32;
    hg.base_mem += hg.nkeys * 8
        + rep_len
        + hg.numtrans * core::mem::size_of::<AggPerGroup>()
        + hg.nsort * core::mem::size_of::<Option<DistinctSet<'_>>>()
        + GROUP_FIXED_COST;
    // 7/8 load factor.
    if (hg.ngroups() + 1) * 8 > hg.table.len() * 7 {
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
    let mut words = [0i64; 32];
    let (found, slot_idx, h, nulls, nkeys) = {
        let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
        let nkeys = hg.nkeys;
        let slot = estate.slot_mut(id);
        let nulls =
            extract_keys(slot, &hg.key_atts, &hg.key_kinds, hg.max_att, &mut words[..nkeys]);
        let h = key_hash(&words[..nkeys], nulls);
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
        None => create_group(node, estate, id, &words[..nkeys], nulls, h, slot_idx)?,
    }
    let hg = node.hashgroup.as_deref().expect("hashgroup state");
    Ok(hg.mem() <= hg.budget)
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
    // Park the last group's state back into storage, then order.
    switch_out(node);
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    let mcx = hg.mcx;
    exectuples::exec_clear_tuple(&mut hg.rep_slot, mcx);
    let mut order: Vec<u32> = (0..n as u32).collect();
    let (keys, keynulls, spec, nkeys) = (&hg.keys, &hg.keynulls, &hg.order_spec, hg.nkeys);
    order.sort_unstable_by(|&a, &b| {
        let (a, b) = (a as usize, b as usize);
        for k in spec.iter() {
            let (na, nb) = (
                keynulls[a] & (1 << k.key_idx) != 0,
                keynulls[b] & (1 << k.key_idx) != 0,
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
                    let (wa, wb) = (keys[a * nkeys + k.key_idx], keys[b * nkeys + k.key_idx]);
                    if k.desc {
                        wb.cmp(&wa)
                    } else {
                        wa.cmp(&wb)
                    }
                }
            };
            if ord != core::cmp::Ordering::Equal {
                return ord;
            }
        }
        debug_assert_eq!(a, b, "distinct groups compare equal on the full prefix");
        core::cmp::Ordering::Equal
    });
    hg.phase = HgPhase::Emit { order, pos: 0 };
    Ok(())
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
/// CURRENT group's live state parks back into storage first.
pub fn agg_hashgroup_set_residual(node: &mut AggStateData<'_>) {
    switch_out(node);
    let hg = node.hashgroup.as_deref_mut().expect("hashgroup state");
    debug_assert!(matches!(hg.phase, HgPhase::Building));
    debug_assert_eq!(hg.rep_cursor, hg.ngroups(), "every representative rides the sort");
    hg.phase = HgPhase::Residual;
}

/// The residual-phase group-begin hook (called from `initialize_aggregates`
/// — the seam BOTH the lane emit chain and the C pull-loop fallback pass
/// through): if the beginning group (its first tuple already sits in
/// `persort.first_slot`) has saved partial state, install it — pergroup
/// values over the freshly initialized ones, sets into the pertrans slots —
/// so pre-degrade rows count exactly once. Drops the whole state once every
/// residual group has been consumed (the aggcontext reset then resumes).
pub(crate) fn residual_preload(node: &mut AggStateData<'_>) -> PgResult<()> {
    if !agg_hashgroup_residual_active(node) {
        return Ok(());
    }
    let mut words = [0i64; 32];
    let hit = {
        let AggStateData { hashgroup, persort, .. } = node;
        let hg = hashgroup.as_deref_mut().expect("residual state");
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let nkeys = hg.nkeys;
        let nulls = extract_keys(
            &mut ps.first_slot,
            &hg.key_atts,
            &hg.key_kinds,
            hg.max_att,
            &mut words[..nkeys],
        );
        let h = key_hash(&words[..nkeys], nulls);
        let (found, _) = hg.probe(&words[..nkeys], nulls, h);
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
