//! Lane-v2 dict-code batched exact-DISTINCT grouping (lane-v2-q14feed) — the
//! textgroup lane's deferred "dict-code batch feed" for the near-unique
//! single-text-key shape the hash-grouped arm's density tier refuses (CB Q14:
//! `SELECT SearchPhrase, COUNT(DISTINCT UserID) … WHERE SearchPhrase <> ''
//! GROUP BY SearchPhrase`, ~1.35M qual survivors over ~689k groups).
//!
//! For that shape BOTH incumbents pay a per-survivor string cost: the
//! narrow-sort arm sorts every survivor row by the text prefix (sort 33% +
//! memcmp 21% of the warm profile), and the hash-grouped text arm pays
//! detoast+hash+memcmp per row into ~as many group states as rows (measured
//! LOSS — its density tier now refuses upfront). This arm exploits the
//! cbstore dict facts instead (the dictminmax foundation, verified in
//! writer.rs): every CB-bank text chunk is dict-encoded, per-epoch (row
//! group) dictionaries are DEDUPLICATED and BYTE-SORTED, and a dict-lane
//! window's codes satisfy `values[i] == table.datum(code(i))`.
//!
//! Build (per staged window, batch-wise): group ON THE (epoch, code) INTEGER
//! DOMAIN — a per-epoch direct-indexed `code → state` map (dense, ≤ 65,536
//! entries by the writer's dict admission); per surviving row ONE array
//! index plus one append of the DISTINCT arg value onto the state's chain
//! (a shared `(value, next)` pool — no per-state allocation). No hashing, no
//! detoast, no string compare, no transition program on the build path. The
//! state's key materializes ONCE per distinct (epoch, code): the dict
//! entry's varlena IMAGE is copied into the arm's arena (C-identical datum
//! bytes — the cbstore fill's Raw gather for a dict chunk publishes exactly
//! `dict[code]`, so this is the datum every row of the state carried).
//!
//! Emit (streamed, one group per pull): per-epoch states listed in CODE
//! order are byte-order runs (dicts are byte-sorted), so a k-way merge over
//! the epochs' runs yields all states in `memcmp + length-tiebreak` order —
//! which IS `varstr_cmp` order under the admitted memcmp-tier collation
//! (C/POSIX/DEFAULT→C, `lanefold::str_collation_safe`), i.e. the plan
//! Sort's ASC group order. Adjacent equal-content states (the same phrase in
//! different epochs) merge into ONE group: their chains union into the
//! pertrans exact-DISTINCT set (dedup exactly there), the group finalizes
//! through the UNCHANGED `agg_sorted_emit` tail (set replay through the real
//! transfn — the COUNT shortcut included — then HAVING + projection).
//!
//! Byte identity vs the C path (and vs both incumbent arms):
//!   * same groups: within an epoch distinct codes are distinct bytes
//!     (dict dedup) and equal codes are the same string; across epochs
//!     states merge exactly when their content bytes are equal — texteq's
//!     deterministic length+memcmp arm, the grouping operator's verdict
//!     (`group_eq_representational` admission);
//!   * same group order: memcmp content + C's length tiebreak on the merge
//!     == `varstr_cmp` under a memcmp-tier collation == the plan Sort's ASC
//!     text order (admission refuses DESC and non-memcmp collations). Two
//!     distinct groups never compare equal, so the order is total. NULL
//!     group keys cannot exist here (dict codes have no NULL representation
//!     and cbstore stores no NULLs; any non-dict window degrades BEFORE
//!     being absorbed);
//!   * same values: the same distinct multiset per group replays through
//!     the SAME set machinery (`process_ordered_aggregates`); the admission
//!     inherits `trans_order_insensitive` so replay order is invisible;
//!   * same representative bytes: the synthesized rep row carries the dict
//!     image datum for the key and NULL elsewhere — sound exactly as the
//!     hash arm's `adopt_merged` synthesis (an Agg output can only reference
//!     grouping columns and aggregates), and the key image is the very datum
//!     the C row path's slot carried for every row of the group.
//!
//! Memory / degrade: everything (arena, chain pool, state vecs, map) meters
//! against HALF the displaced tuplesort's budget. Crossing it — or meeting a
//! non-dict/unsorted-dict window, a fallback-bearing batch, a NULL arg, or a
//! non-inline dict image — DEGRADES to the narrow-sort arm exactly once: the
//! narrowed tuplesort begins late, every absorbed row REPLAYS into it as a
//! synthesized (key, arg) row (`agg_codedgroup_next_replay` — the exact
//! multiset of absorbed survivor rows; other columns NULL, unobservable as
//! above), the unconsumed remainder feeds it per-row, and the narrow-sort
//! emit chain runs unchanged. Unlike the hash arm, NO residual state
//! survives a degrade — every absorbed row rides the sort.
//!
//! Admission (v1, deliberately tight — the Q14 shape): the narrow-sort
//! admission, exactly ONE text/varchar grouping column, exactly ONE
//! transition which is a set-mode COUNT(DISTINCT <bare int Var>) pertrans
//! (`set_count_transfn` + `direct_att`), cbstore scan + dict staging armed
//! by the drive, single ASC memcmp-tier text sort prefix. Everything else
//! refuses to the incumbent arms, byte-identically.

use ::datum::Datum;
use ::execexpr::AggPerGroup;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::varatt;

use crate::distinctset::{DistinctKeyKind, DistinctSet};
use crate::{agg_sorted_emit, AggStateData};

const INT2OID: ::types_core::Oid = 21;
const INT4OID: ::types_core::Oid = 23;
const INT8OID: ::types_core::Oid = 20;
const TEXTOID: ::types_core::Oid = 25;
const VARCHAROID: ::types_core::Oid = 1043;

enum CgPhase {
    /// Absorbing staged windows (batch-wise, code domain).
    Building,
    /// Build complete; the k-way run merge streams one group per call.
    Emit,
}

/// Sentinel for "load the current replay state's chain head".
const REPLAY_HEAD: u32 = u32::MAX;

/// One accepted batch's outcome (transactional bookkeeping for the degrade):
/// `consumed` survivor rows were fully absorbed into states; `keep == false`
/// means the caller must degrade — replay every absorbed row into the
/// narrowed sort, then feed `rows[consumed..]` (and all later input) per-row.
pub struct CgAccept {
    pub consumed: usize,
    pub keep: bool,
}

pub(crate) struct CodedGroupState<'mcx> {
    phase: CgPhase,
    /// 0-based outer attno of the text grouping key / the DISTINCT arg.
    key_att: u16,
    arg_att: u16,
    kind: DistinctKeyKind,
    natts: usize,
    /// Per state: the key's varlena IMAGE span in `arena` (offset, len —
    /// header included; 8-aligned). Offsets stay < 4GiB structurally: the
    /// arena is metered against the arm's budget (≤ work_mem/2, itself
    /// capped « 4GiB) and one dict entry overshoots by < 1GiB.
    spans: Vec<(u32, u32)>,
    /// Per state: chain head (pool index + 1; never 0 — a state is created
    /// by the row that appends its first value).
    heads: Vec<u32>,
    /// Shared DISTINCT-arg chain pool: (sign-extended value, next + 1).
    pool: Vec<(i64, u32)>,
    arena: Vec<u8>,
    /// Per-epoch direct map: code → state index + 1 (0 = unseen). Dense
    /// (`ndict`-sized), rebuilt at every epoch roll.
    cur_epoch: Option<u64>,
    code_map: Vec<u32>,
    /// (code, state) pairs created in the CURRENT epoch, sorted by code at
    /// the epoch close — code order IS byte order (sorted dicts).
    epoch_pairs: Vec<(u32, u32)>,
    /// Closed epochs: `runs[e] = [start, end)` into `order`, whose entries
    /// are state indices in byte order within the epoch.
    runs: Vec<(u32, u32)>,
    order: Vec<u32>,
    /// Emit: per-run cursor into `order` + the min-heap of live run ids.
    cursor: Vec<u32>,
    heap: Vec<u32>,
    /// Scratch: the current merged group's state indices.
    gstates: Vec<u32>,
    /// Degrade replay cursor.
    replay_state: usize,
    replay_next: u32,
    /// Spare outer-format virtual slot: synthesized reps + degrade replay.
    rep_slot: SlotData<'mcx>,
    budget: usize,
    mcx: Mcx<'mcx>,
}

impl CodedGroupState<'_> {
    #[inline]
    fn nstates(&self) -> usize {
        self.spans.len()
    }

    /// Capacity-based accounting, mirroring the hash arm's discipline.
    #[inline]
    fn mem(&self) -> usize {
        self.arena.capacity()
            + self.pool.capacity() * core::mem::size_of::<(i64, u32)>()
            + self.spans.capacity() * 8
            + self.heads.capacity() * 4
            + self.code_map.capacity() * 4
            + self.epoch_pairs.capacity() * 8
            + self.order.capacity() * 4
            + self.runs.capacity() * 8
    }

    /// A state's key CONTENT bytes (after the 1B/4B varlena header). The
    /// arena stores only inline images (external tags degrade at absorb).
    #[inline]
    fn content(&self, s: u32) -> &[u8] {
        let (off, len) = self.spans[s as usize];
        let img = &self.arena[off as usize..(off + len) as usize];
        // SAFETY: `img` is a live inline varlena image (absorb copied it
        // whole and refused external tags), readable through its header.
        unsafe {
            if varatt::varatt_is_1b(img.as_ptr()) {
                &img[1..varatt::varsize_1b(img.as_ptr())]
            } else {
                &img[4..varatt::varsize_4b(img.as_ptr())]
            }
        }
    }

    /// Close the current epoch: sort its (code, state) pairs by code (byte
    /// order — sorted dicts) into one merge run.
    fn close_epoch(&mut self) {
        if self.epoch_pairs.is_empty() {
            return;
        }
        self.epoch_pairs.sort_unstable_by_key(|&(code, _)| code);
        let start = self.order.len() as u32;
        self.order.extend(self.epoch_pairs.iter().map(|&(_, s)| s));
        self.runs.push((start, self.order.len() as u32));
        self.epoch_pairs.clear();
    }

    /// The state at run `r`'s cursor.
    #[inline]
    fn head_state(&self, r: u32) -> u32 {
        self.order[self.cursor[r as usize] as usize]
    }

    /// Merge order: memcmp content, C's length tiebreak (varstr_cmp's
    /// memcmp-tier order — module doc), run id last (equal content across
    /// runs is absorbed at pop; the id keeps the heap order total).
    fn run_less(&self, a: u32, b: u32) -> bool {
        let (ca, cb) = (self.content(self.head_state(a)), self.content(self.head_state(b)));
        match ca.cmp(cb) {
            core::cmp::Ordering::Less => true,
            core::cmp::Ordering::Greater => false,
            core::cmp::Ordering::Equal => a < b,
        }
    }

    fn heap_sift_down(&mut self, mut i: usize) {
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut m = i;
            if l < self.heap.len() && self.run_less(self.heap[l], self.heap[m]) {
                m = l;
            }
            if r < self.heap.len() && self.run_less(self.heap[r], self.heap[m]) {
                m = r;
            }
            if m == i {
                return;
            }
            self.heap.swap(i, m);
            i = m;
        }
    }

    /// Advance the root run past its head (pop the run when exhausted).
    fn advance_root(&mut self) {
        let r = self.heap[0];
        let (_, end) = self.runs[r as usize];
        self.cursor[r as usize] += 1;
        if self.cursor[r as usize] >= end {
            let last = self.heap.len() - 1;
            self.heap.swap(0, last);
            self.heap.pop();
        }
        if !self.heap.is_empty() {
            self.heap_sift_down(0);
        }
    }

    /// Pop the next merged group into `gstates` (all runs' heads with equal
    /// content — at most one state per run: within an epoch distinct codes
    /// are distinct bytes). False = stream end.
    fn pop_group(&mut self) -> bool {
        self.gstates.clear();
        if self.heap.is_empty() {
            return false;
        }
        let first = self.head_state(self.heap[0]);
        self.gstates.push(first);
        self.advance_root();
        while !self.heap.is_empty() {
            let s = self.head_state(self.heap[0]);
            // Compare via spans to sidestep the double-borrow.
            if self.content(s) != self.content(first) {
                break;
            }
            self.gstates.push(s);
            self.advance_root();
        }
        true
    }

    /// Key image pointer datum for state `s` (stable: the arena never grows
    /// during emit/replay).
    #[inline]
    fn key_datum(&self, s: u32) -> Datum {
        let (off, _) = self.spans[s as usize];
        // SAFETY-free: pointer formation only; readers go through the live
        // arena bytes (emit/replay phases append nothing).
        Datum::from_usize(unsafe { self.arena.as_ptr().add(off as usize) } as usize)
    }

    /// The stored sign-extended value as the original argument datum.
    #[inline]
    fn arg_datum(&self, v: i64) -> Datum {
        match self.kind {
            DistinctKeyKind::Int16 => Datum::from_i16(v as i16),
            DistinctKeyKind::Int32 => Datum::from_i32(v as i32),
            DistinctKeyKind::Int64 => Datum::from_i64(v),
            DistinctKeyKind::Bytes => unreachable!("codedgroup admission is int-arg only"),
        }
    }

    /// Synthesize one outer-format row into `rep_slot`: the key column takes
    /// `key`, `arg` (when given) the DISTINCT arg column, everything else
    /// NULL (unobservable — module doc).
    fn build_row(&mut self, key: Datum, arg: Option<Datum>) {
        let mcx = self.mcx;
        exectuples::exec_clear_tuple(&mut self.rep_slot, mcx);
        {
            let base = self.rep_slot.base_mut();
            for i in 0..self.natts {
                base.tts_values[i] = Datum::null();
                base.tts_isnull[i] = true;
            }
            base.tts_values[self.key_att as usize] = key;
            base.tts_isnull[self.key_att as usize] = false;
            if let Some(a) = arg {
                base.tts_values[self.arg_att as usize] = a;
                base.tts_isnull[self.arg_att as usize] = false;
            }
        }
        exectuples::exec_store_virtual_tuple(&mut self.rep_slot);
    }
}

/// Structural admission (module doc). Deliberately requires the narrow-sort
/// admission — every refusal lands on a proven byte-identical path.
pub fn agg_codedgroup_admissible(node: &AggStateData<'_>) -> bool {
    if !crate::agg_sorted_distinct_narrow_admissible(node) {
        return false;
    }
    if node.plan.grpColIdx.len() != 1 {
        return false;
    }
    let Some(ps) = node.persort.as_ref() else {
        return false;
    };
    let Some(desc) = ps.first_slot.base().tts_tupleDescriptor.as_ref() else {
        return false;
    };
    let col = node.plan.grpColIdx[0];
    if col < 1 || (col as i32) > desc.natts {
        return false;
    }
    if !matches!(desc.attr((col - 1) as usize).atttypid, TEXTOID | VARCHAROID) {
        return false;
    }
    if node.numtrans != 1 || node.pertrans_sort.len() != 1 {
        return false;
    }
    let pt = &node.pertrans_sort[0];
    let Some(arg) = pt.direct_att else {
        return false;
    };
    // The arg must be a DIFFERENT column than the key (a dict-lane-answered
    // key column has stale value cells) and an integer lane the batch feed
    // can read directly.
    matches!(
        pt.set_kind,
        Some(DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64)
    ) && pt.set_count_transfn
        && pt.num_inputs == 1
        && arg != (col - 1) as u16
        && (arg as i32) < desc.natts
        && matches!(desc.attr(arg as usize).atttypid, INT2OID | INT4OID | INT8OID)
        // COUNT's init state is byval '0' — structural, but the per-group
        // emit re-init writes it verbatim, so prove it.
        && (node.trans_init[0].isnull || node.trans_typ[0].byval)
}

/// (key, arg) 0-based OUTER attnos. Callable only after `admissible`.
pub fn agg_codedgroup_key_arg_atts(node: &AggStateData<'_>) -> (u16, u16) {
    debug_assert!(agg_codedgroup_admissible(node));
    (
        (node.plan.grpColIdx[0] - 1) as u16,
        node.pertrans_sort[0].direct_att.expect("admission proved direct_att"),
    )
}

fn codedgroup_budget() -> usize {
    crate::distinct_set_budget() / 2
}

/// Planner-estimate economics: engage EXACTLY the density band the
/// hash-grouped arm's tier refuses (near-unique keys, the Q14 class) — the
/// higher-density single-text-key shapes keep the measured hash-arm text
/// path (Q11/Q12: -9%/-17% landed). `force` = the e2e harness override
/// (small tables never look near-unique); the runtime degrade still bounds
/// memory.
pub fn agg_codedgroup_economical(node: &AggStateData<'_>, force: bool, input_rows: f64) -> bool {
    if force {
        return true;
    }
    /// The hash arm's `MIN_ROWS_PER_GROUP` (hashgrouped.rs) — the two arms
    /// partition the density axis at the same cut.
    const MIN_ROWS_PER_GROUP: f64 = 8.0;
    let est_groups = (node.plan.numGroups as f64).max(1.0);
    if !(input_rows > 0.0 && input_rows < MIN_ROWS_PER_GROUP * est_groups) {
        return false;
    }
    // States are bounded by survivor rows; conservative per-state estimate
    // (span + head + pool entry + map/order shares) plus mean key bytes,
    // with 2x estimate slack — the runtime degrade owns the real bound.
    const PER_STATE_EST: f64 = 96.0;
    input_rows * PER_STATE_EST * 2.0 <= codedgroup_budget() as f64
}

/// Begin the coded build. The caller proved `agg_codedgroup_admissible`,
/// armed `force_distinct_set`, and owns the scan staging (dict lane on the
/// key column).
pub fn agg_codedgroup_begin<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(agg_codedgroup_admissible(node));
    debug_assert!(node.force_distinct_set);
    debug_assert!(node.codedgroup.is_none());
    debug_assert!(node.hashgroup.is_none());
    let mcx = estate.es_query_cxt;
    let (key_att, arg_att) = agg_codedgroup_key_arg_atts(node);
    let ps = node.persort.as_ref().expect("sorted Agg has persort");
    let desc = ps
        .first_slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("persort slots carry the outer desc")
        .clone();
    let natts = desc.natts as usize;
    let rep_slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
    let kind = node.pertrans_sort[0].set_kind.expect("admission proved a set kind");
    node.codedgroup = Some(Box::new(CodedGroupState {
        phase: CgPhase::Building,
        key_att,
        arg_att,
        kind,
        natts,
        spans: Vec::new(),
        heads: Vec::new(),
        pool: Vec::new(),
        arena: Vec::new(),
        cur_epoch: None,
        code_map: Vec::new(),
        epoch_pairs: Vec::new(),
        runs: Vec::new(),
        order: Vec::new(),
        cursor: Vec::new(),
        heap: Vec::new(),
        gstates: Vec::new(),
        replay_state: 0,
        replay_next: REPLAY_HEAD,
        rep_slot,
        budget: codedgroup_budget(),
        mcx,
    }));
    // Rescan hygiene (the hash arm's begin discipline): no set may be loaded
    // between groups; drop a leftover slot outright.
    for pt in node.pertrans_sort.iter_mut() {
        if let Some(mut d) = pt.dset.take() {
            d.clear();
        }
        debug_assert!(!pt.dset_degraded);
    }
    Ok(())
}

/// Absorb one dict-answered window's survivors, batch-wise. The caller
/// proved: `lane.table.sorted`, no fallback rows in the batch, `rows` are
/// the batch's qual survivors in ascending order, and `argv`/`argn` are the
/// DISTINCT-arg column's staged lane cells (valid at every selected row).
/// See `CgAccept` for the degrade contract; NULL args and non-inline dict
/// images stop absorption AT the offending row (never partially absorbed).
pub fn agg_codedgroup_accept_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    lane: ::exectuples::SoaDictLane,
    rows: &[u32],
    argv: &[Datum],
    argn: &[bool],
) -> CgAccept {
    let cg = node.codedgroup.as_deref_mut().expect("codedgroup state");
    debug_assert!(matches!(cg.phase, CgPhase::Building));
    debug_assert!(lane.table.sorted, "caller admits sorted dicts only");
    let ndict = lane.table.ndict as usize;
    if cg.cur_epoch != Some(lane.table.epoch) {
        cg.close_epoch();
        cg.cur_epoch = Some(lane.table.epoch);
        cg.code_map.clear();
        cg.code_map.resize(ndict, 0);
    }
    debug_assert!(cg.code_map.len() >= ndict, "dict size is fixed per epoch");
    for (idx, &i) in rows.iter().enumerate() {
        // NULL DISTINCT arg: C feeds it through seen_null; this arm keeps
        // v1 simple and degrades (unreachable on cbstore — no NULLs).
        if argn[i as usize] {
            return CgAccept { consumed: idx, keep: false };
        }
        let code = lane.code(i as usize) as usize;
        debug_assert!(code < ndict, "filler contract: code < ndict");
        let s = match cg.code_map[code] {
            0 => {
                // First surviving row of (epoch, code): land the dict
                // entry's varlena image in the arena (8-aligned).
                let p = lane.table.datum(code as u32).as_usize() as *const u8;
                // SAFETY: dict entries are live decoded varlena datums for
                // the pinned scan's lifetime (SoaDictTable contract),
                // readable through their header.
                let (external, len) = unsafe {
                    (varatt::varatt_is_1b_e(p), varatt::varsize_any(p))
                };
                if external {
                    // Non-inline image (never produced by the cbstore
                    // decode); refuse the row — the caller degrades.
                    return CgAccept { consumed: idx, keep: false };
                }
                while !cg.arena.len().is_multiple_of(8) {
                    cg.arena.push(0);
                }
                let off = cg.arena.len();
                // SAFETY: as above — `len` bytes readable at `p`.
                cg.arena.extend_from_slice(unsafe { core::slice::from_raw_parts(p, len) });
                cg.spans.push((off as u32, len as u32));
                cg.heads.push(0);
                let s = (cg.spans.len() - 1) as u32;
                cg.code_map[code] = s + 1;
                cg.epoch_pairs.push((code as u32, s));
                s
            }
            m => m - 1,
        };
        let d = argv[i as usize];
        let v = match cg.kind {
            DistinctKeyKind::Int16 => d.as_i16() as i64,
            DistinctKeyKind::Int32 => d.as_i32() as i64,
            DistinctKeyKind::Int64 => d.as_i64(),
            DistinctKeyKind::Bytes => unreachable!("codedgroup admission is int-arg only"),
        };
        cg.pool.push((v, cg.heads[s as usize]));
        cg.heads[s as usize] = cg.pool.len() as u32;
    }
    CgAccept { consumed: rows.len(), keep: cg.mem() <= cg.budget }
}

/// Input exhausted with no degrade: close the last epoch, seed the merge
/// heap, flip to Emit.
pub fn agg_codedgroup_finish_build(node: &mut AggStateData<'_>) {
    let cg = node.codedgroup.as_deref_mut().expect("codedgroup state");
    debug_assert!(matches!(cg.phase, CgPhase::Building));
    cg.close_epoch();
    cg.cursor = cg.runs.iter().map(|&(start, _)| start).collect();
    cg.heap.clear();
    for r in 0..cg.runs.len() as u32 {
        let (start, end) = cg.runs[r as usize];
        if start < end {
            cg.heap.push(r);
        }
    }
    // Heapify (sift-down from the last parent).
    let n = cg.heap.len();
    for i in (0..n / 2).rev() {
        cg.heap_sift_down(i);
    }
    cg.phase = CgPhase::Emit;
}

/// Whether the arm is mid-emit (the drive resumes here BEFORE the dynamic
/// gates — the scan is exhausted and the plan's Sort must never be fed).
pub fn agg_codedgroup_emitting(node: &AggStateData<'_>) -> bool {
    matches!(
        node.codedgroup.as_deref(),
        Some(CodedGroupState { phase: CgPhase::Emit, .. })
    )
}

/// Rescan/teardown: drop the arm's state (plain Rust memory; nothing of the
/// arm's lives in aggcontext — the single admitted transition is byval).
pub fn agg_codedgroup_reset(node: &mut AggStateData<'_>) {
    if let Some(mut cg) = node.codedgroup.take() {
        let mcx = cg.mcx;
        exectuples::exec_clear_tuple(&mut cg.rep_slot, mcx);
    }
}

/// Emit the next merged group through the UNCHANGED sorted-agg finalize/
/// HAVING/project tail. `Ok(None)` = stream end (`agg_done` set, state
/// dropped); `Ok(Some(None))` = HAVING rejected (caller loops);
/// `Ok(Some(Some(slot)))` = one group row.
pub fn agg_codedgroup_emit_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    {
        let cg = node.codedgroup.as_deref_mut().expect("codedgroup emit without state");
        debug_assert!(matches!(cg.phase, CgPhase::Emit));
        if !cg.pop_group() {
            // Stream end: C's agg_done arm.
            node.agg_done = true;
            agg_codedgroup_reset(node);
            return Ok(None);
        }
    }
    // Per-group output memory reset (the group begin's reset; aggcontext is
    // untouched during emit — the admitted transition state is byval, so
    // nothing accumulates there).
    estate.reset_expr_context(node.ps_ExprContext);
    let mcx = estate.es_query_cxt;
    {
        // Union the group's chains into the pertrans set (dedup exactly
        // there); the reused set cycles through ps.dset — the replay tail
        // clears it and puts it back.
        let AggStateData { codedgroup, pertrans_sort, .. } = node;
        let cg = codedgroup.as_deref_mut().expect("codedgroup state");
        let pt = &mut pertrans_sort[0];
        let mut set = pt.dset.take().unwrap_or_else(DistinctSet::new);
        debug_assert!(set.len() == 0 && !set.seen_null, "replay returns the set cleared");
        for gi in 0..cg.gstates.len() {
            let s = cg.gstates[gi];
            let mut link = cg.heads[s as usize];
            while link != 0 {
                let (v, next) = cg.pool[(link - 1) as usize];
                set.insert_i64(v);
                link = next;
            }
        }
        pt.dset = Some(set);
    }
    // Group-init transition state (initialize_aggregates' one-transition
    // body; byval/null by admission, so no aggcontext copy).
    {
        let init = node.trans_init[0];
        debug_assert!(init.isnull || node.trans_typ[0].byval);
        // SAFETY: transno 0 of the once-allocated pergroup array; the base
        // pointer is the node's sole access path (struct invariant).
        unsafe {
            node.pergroup_base.as_ptr().write(AggPerGroup {
                trans_value: init.value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
    }
    // Synthesized representative (adopt_merged's argument — module doc).
    {
        let AggStateData { codedgroup, persort, .. } = node;
        let cg = codedgroup.as_deref_mut().expect("codedgroup state");
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let key = cg.key_datum(cg.gstates[0]);
        cg.build_row(key, None);
        exectuples::exec_copy_slot(&mut ps.first_slot, &mut cg.rep_slot, mcx, mcx)?;
    }
    let row = agg_sorted_emit(node, estate)?;
    Ok(Some(row))
}

/// Degrade replay: one synthesized (key, arg) row per absorbed survivor row
/// into the spare outer slot — the caller puts each into the narrowed
/// tuplesort, then drops the arm (`agg_codedgroup_reset`). The rows carry
/// the exact multiset of (group key, DISTINCT arg) pairs the arm absorbed;
/// every other column is NULL (unobservable — module doc).
pub fn agg_codedgroup_next_replay<'a, 'mcx>(
    node: &'a mut AggStateData<'mcx>,
) -> Option<&'a mut SlotData<'mcx>> {
    let cg = node.codedgroup.as_deref_mut().expect("codedgroup state");
    debug_assert!(matches!(cg.phase, CgPhase::Building));
    loop {
        if cg.replay_state >= cg.nstates() {
            let mcx = cg.mcx;
            exectuples::exec_clear_tuple(&mut cg.rep_slot, mcx);
            return None;
        }
        let link = if cg.replay_next == REPLAY_HEAD {
            cg.heads[cg.replay_state]
        } else {
            cg.replay_next
        };
        if link == 0 {
            cg.replay_state += 1;
            cg.replay_next = REPLAY_HEAD;
            continue;
        }
        let (v, next) = cg.pool[(link - 1) as usize];
        cg.replay_next = next;
        let key = cg.key_datum(cg.replay_state as u32);
        let arg = cg.arg_datum(v);
        cg.build_row(key, Some(arg));
        return Some(&mut cg.rep_slot);
    }
}

#[cfg(test)]
mod tests {
    // The merge/chain machinery is exercised end-to-end by
    // scripts/lane-distinct-set-e2e.sh's codefeed battery; the unit surface
    // below covers the header-form reader the merge comparator relies on.
    use super::*;

    #[test]
    fn content_reads_both_header_forms() {
        // 4B-header image of "abc" + short-form image of "abc".
        let mut arena = Vec::new();
        let w = varatt::set_varsize_4b_word(4 + 3).to_ne_bytes();
        arena.extend_from_slice(&w);
        arena.extend_from_slice(b"abc");
        while arena.len() % 8 != 0 {
            arena.push(0);
        }
        let short_off = arena.len();
        arena.push(0);
        unsafe { varatt::set_varsize_short(arena.as_mut_ptr().add(short_off), 1 + 3) };
        arena.extend_from_slice(b"abc");
        let content = |arena: &[u8], off: usize| -> Vec<u8> {
            let img = &arena[off..];
            unsafe {
                if varatt::varatt_is_1b(img.as_ptr()) {
                    img[1..varatt::varsize_1b(img.as_ptr())].to_vec()
                } else {
                    img[4..varatt::varsize_4b(img.as_ptr())].to_vec()
                }
            }
        };
        assert_eq!(content(&arena, 0), b"abc");
        assert_eq!(content(&arena, short_off), b"abc");
    }
}
