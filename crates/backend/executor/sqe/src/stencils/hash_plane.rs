//! hash-plane owned-group stencil (parametrized from the hot-shape elected
//! kernel owned_l2p_par): pass 1 decodes the node's columns morsel-
//! parallel and SoA-scatters (packed key, packed payload) into P hash
//! partitions per worker; pass 2 gives each partition exactly ONE owner
//! (dynamic claim) folding an L2-resident SoA table — no merge exists
//! anywhere. P comes from the partition law over ndv_est (planner).
//!
//! Parametric levers:
//!   - group key: 1-2 byval columns packed into u128 by column width;
//!   - payload: (Sum col, Avg col) packed into one u32 when the stats
//!     PROVE the ranges fit (sum in {0,1}, avg-input in [0,65535]) —
//!     else an unpacked (u32, u64) pair path (correct, wider);
//!   - top-k render per owner when ORDER BY count DESC LIMIT k.

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::engine::SqeCtx;
use crate::ir::*;
use crate::planner::partition_count;
use crate::scan::{CurCache, Scratch};
use crate::stencils::{col_width, sx};
use crate::typmeta::TypMeta;

#[inline(always)]
fn hash128(key: u128) -> u64 {
    let x = (key as u64) ^ ((key >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x.wrapping_mul(0xD6E8_FEB8_6659_FD93)
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Row {
    key: u128,
    c: u64,
    sr: u64,
    sw: u64,
}

/// Insert into a ≤k-row selection under (c DESC, key ASC) — the same
/// total order the final render sorts by, so per-owner selections merge
/// exactly (the hot-shape render proof).
#[inline]
fn topk_consider(top: &mut Vec<Row>, k: usize, cand: Row) {
    let before = |a: &Row, b: &Row| a.c > b.c || (a.c == b.c && a.key < b.key);
    if top.len() == k {
        let last = top.last().unwrap();
        if !before(&cand, last) {
            return;
        }
        top.pop();
    }
    let mut i = top.len();
    top.push(cand);
    while i > 0 && before(&top[i], &top[i - 1]) {
        top.swap(i, i - 1);
        i -= 1;
    }
}

struct OaSoA {
    keys: Vec<u128>,
    /// u64 per-group row counter: group cardinality is bounded only by the
    /// bank row total (a u64 fact), so a single hot key with >=2^32 rows
    /// would wrap a u32 lane silently (release builds have no overflow
    /// checks) — corrupting COUNT(*) and the AVG denominator. Matches the
    /// non-sqe aggregate path (int8/u64) and the dense_direct arm's u64
    /// counters below.
    cnt: Vec<u64>,
    /// u64: generic SUM accumulator (the hot-shape shape's {0,1} inputs fit u32,
    /// but a mid-NDV group over a u16-range column overflows it — sqe-m4).
    srs: Vec<u64>,
    sws: Vec<u64>,
    mask: usize,
    /// Live entries (the spill arm's drain trigger reads this; the
    /// in-memory arm never consults it).
    len: usize,
}

const EMPTY: u128 = u128::MAX;

impl OaSoA {
    fn new(entries: usize) -> OaSoA {
        let cap = (entries * 2).next_power_of_two().max(16);
        OaSoA {
            keys: vec![EMPTY; cap],
            cnt: vec![0; cap],
            srs: vec![0; cap],
            sws: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }
    /// [sqe-m2] Reuse a parked table for a new partition: grow-only
    /// buffers, active window re-armed (fill beats fresh page faults —
    /// the hot-shape in-suite regression was rebuilding ~GBs of table pages
    /// per rep under a fragmented process heap).
    fn reset(&mut self, entries: usize) {
        let cap = (entries * 2).next_power_of_two().max(16);
        if self.keys.len() < cap {
            self.keys.resize(cap, EMPTY);
            self.cnt.resize(cap, 0);
            self.srs.resize(cap, 0);
            self.sws.resize(cap, 0);
        }
        self.keys[..cap].fill(EMPTY);
        self.cnt[..cap].fill(0);
        self.mask = cap - 1;
        self.len = 0;
    }
    #[inline(always)]
    fn add(&mut self, slot0: usize, key: u128, sr: u64, sw: u64) {
        let mut slot = slot0 & self.mask;
        loop {
            let k = self.keys[slot];
            if k == key {
                self.cnt[slot] += 1;
                self.srs[slot] += sr;
                self.sws[slot] += sw;
                return;
            }
            if k == EMPTY {
                self.keys[slot] = key;
                self.cnt[slot] = 1;
                self.srs[slot] = sr;
                self.sws[slot] = sw;
                self.len += 1;
                return;
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------------------
// [P6-1 spill] The grouped spill arm (spill-design.md §3): the SoA
// partition-owned byval route spills at partition grain — pass-1 scatter
// buckets flush to per-worker files at the E18b share; pass-2 tables cap
// at the per-owner share and drain key-sorted runs; finalize k-way merges
// runs + table under the same integer combine the fold uses. Answers are
// byte-identical spill vs no-spill (the render's (count DESC, key ASC)
// total order erases arrival order).
// ---------------------------------------------------------------------------

/// The one classification BOTH the planner's budget law and this
/// stencil's dispatch read (spill-design.md §4 — divergence here would be
/// an unbounded arm admitted as spillable, the loudest class):
/// `Bounded` = the direct-array election holds state under the budget;
/// `Spillable` = the SoA byval route (spill arm) serves the shape;
/// `SpillableBytes` = [spill-2] the byte-key spill route serves the
/// shape (varlena group keys — text128/int_gid/int_gid_filtered/gid_pair
/// vocabulary: CountStar-only, 1-2 keys with >=1 varlena, null-free
/// keys, no HAVING/filters — text keys spill BYTE-KEY runs);
/// `Unbounded` = no spill arm at this rung (frame/distinct/foundation-
/// cell shapes — §6 residue).
/// `SpillableDistinct` = [spill-2] the dense-domain distinct route
/// (`hash_group::dense_distinct`) with its PAIR plane spillable: dense
/// arrays witnessed under the budget (they cannot spill — same posture
/// as the §3.5 direct array), the O(rows) pair scatter + owner dedupe
/// tables flush/drain/merge. EXCLUDED from cap-retirement (no finalize
/// answer law on the dense arrays this rung — the witness gate stays).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SpillClass {
    Bounded,
    Spillable,
    SpillableBytes,
    SpillableDistinct,
    Unbounded,
}

/// Widest SoA scatter record, bytes/row (key u128 + wide payload u32+u64):
/// the E18 scatter-plane estimate's row width — one authority for the
/// planner law and the stencil trigger.
pub const SCATTER_ROW_BYTES: u64 = 28;

/// Sorted-run record, bytes/group: key u128 + count u64 + two u64 partials.
const RUN_REC_BYTES: usize = 40;

/// Pass-2 table bytes per capped entry: 2x pow2 slots per entry, 40 B/slot
/// (u128 key + u64 cnt + 2xu64 partials).
const OA_ENTRY_BYTES: u64 = 80;

/// Spill engagement census (rig visibility: gates prove the spill legs
/// actually ran — a vacuously green identity is worthless).
pub static SPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SPILL_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn spill_class(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> SpillClass {
    // Frame-walk route, expression keys, distinct legs: no spill arm.
    if !node.params.key_exprs.is_empty() {
        return SpillClass::Unbounded;
    }
    // [tpch-expr] pred/expr cells mixes never spill: Unbounded.
    if node.agg.iter().any(|a| a.expr.is_some())
        || node
            .pred
            .as_ref()
            .is_some_and(|p| !p.terms.is_empty() || !p.var_terms.is_empty())
    {
        return SpillClass::Unbounded;
    }
    if node.agg.iter().any(|a| {
        matches!(a.op, AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct)
    }) {
        // [spill-2] the dense-domain distinct route: pair plane
        // spillable when the shape is its exact vocabulary AND the
        // UNSPILLABLE dense arrays are witnessed under the E18 budget.
        return distinct_spill_class(bank, faces, node);
    }
    let g = &node.params.group_cols;
    if g.is_empty() || g.len() > 2 {
        return SpillClass::Unbounded;
    }
    let widths: Vec<u8> = g.iter().map(|&c| col_width(bank, c)).collect();
    if widths.iter().any(|&w| w == 0) {
        // [spill-2] varlena-key shapes: the byte-key spill route serves
        // exactly the tuned text arms' vocabulary — the key layouts the
        // dispatch routes to text128 ([0]), int_gid/int_gid_filtered
        // ([w,0]) and gid_pair ([0,0]); CountStar-only aggs (the tuned
        // arms' own contract), null-free keys (no 3VL byte pack this
        // rung), no HAVING, no FILTER legs. Everything else stays §6
        // residue (typed refusal over budget, witness gate intact).
        let shape_ok = matches!(widths.as_slice(), [0] | [0, 0])
            || matches!(widths.as_slice(), [w, 0] if *w > 0);
        let count_only = node.agg.iter().all(|a| a.op == AggOp::CountStar);
        let keys_null_free = g.iter().all(|&c| bank.null_free(c));
        let filters_none = node.params.agg_filters.iter().all(Option::is_none);
        if shape_ok
            && count_only
            && !node.agg.is_empty()
            && keys_null_free
            && node.params.having.is_none()
            && filters_none
        {
            return SpillClass::SpillableBytes;
        }
        // [q1-dictgroup] Dict-witnessed Bounded arm (the jsonbench q1
        // 100M refusal): a single varlena key whose EVERY part is
        // dict-backed carries the same witness-grade group bound the
        // emit-cap gate consumes (`Witness::key_count` — per-part dict
        // entry sums, +1 for the NULL group; the HLL estimate never
        // participates). The tuned text128 route's state on dict parts
        // is ENTRY-proportional, never row-proportional (per-(part,code)
        // counts + scatter records + cached fps + the pass-2 table), so
        // the E18 scatter price `rows × SCATTER_ROW_BYTES` over-charges
        // it by the rows/entries ratio — a ~10-group key refused at 86M
        // rows that runs in-milliseconds at 10M. Price the TRUE plane
        // under the witness and classify Bounded when it fits: state
        // under the law by construction. NULL keys are admitted (this is
        // exactly the byte-key arm's residue): text128's 3VL law folds
        // them into the one NULL group, and the witness's +1 counts it.
        // The bound must also sit under the emit cap, so [cap-retire]'s
        // posture is unchanged — this arm never serves an unwitnessed or
        // over-cap shape (text128 has no finalize answer law; the cap IS
        // its answer bound).
        if matches!(widths.as_slice(), [0])
            && count_only
            && !node.agg.is_empty()
            && node.params.having.is_none()
            && filters_none
        {
            if let Some(entries) =
                crate::witness::Witness::key_count(bank, faces, g[0]).map(|w| w.value())
            {
                // text128 dict-arm plane, bytes/entry: one scatter
                // record (28) + one cached fp (16) + pass-2 table slots
                // (OA_ENTRY_BYTES), plus each in-flight worker's
                // per-part counts vector (4 B/code, <= entries each).
                let per_entry = (28 + 16 + OA_ENTRY_BYTES) as u128
                    + 4 * faces.cfg.threads.max(1) as u128;
                let est = (entries as u128).saturating_mul(per_entry);
                if entries <= crate::planner::GROUP_ROW_CAP
                    && est <= faces.cfg.grouped_budget_bytes() as u128
                {
                    return SpillClass::Bounded;
                }
            }
        }
        return SpillClass::Unbounded; // varlena residue beyond the byte-key arm
    }
    // Direct-array election with state under budget: Bounded (mirrors the
    // election below, including the spill-mode budget condition).
    let nf0 = bank.null_free(g[0]);
    let fold_lanes =
        node.agg.iter().filter(|a| crate::fold::fold_op_of(a.op).is_some()).count();
    if faces.cfg.direct_array && g.len() == 1 && nf0 {
        let vocab_ok = node.agg.iter().all(|a| {
            matches!(a.op, AggOp::CountStar | AggOp::Sum | AggOp::Avg | AggOp::Min | AggOp::Max)
        });
        let avg_w8 = node
            .agg
            .iter()
            .any(|a| matches!(a.op, AggOp::Avg) && a.in_ty.map(|t| t.width == 8).unwrap_or(false));
        if vocab_ok && !avg_w8 {
            if let Some((_, dn)) =
                crate::planner::direct_array_domain(bank, faces, g[0], fold_lanes)
            {
                let sums_fit = node
                    .agg
                    .iter()
                    .filter(|a| matches!(a.op, AggOp::Sum | AggOp::Avg))
                    .all(|a| {
                        crate::planner::direct_sum_fits(
                            bank,
                            faces,
                            a.col.expect("sum/avg input"),
                            bank.rows_total(),
                        )
                    });
                let array_bytes = dn as u64 * 8 * (1 + 2 * fold_lanes as u64);
                if sums_fit && array_bytes <= faces.cfg.grouped_budget_bytes() {
                    return SpillClass::Bounded;
                }
            }
        }
    }
    // The SoA byval vocabulary (mirrors the foundation-route split below:
    // shapes it sends to `foundation_cells` have no spill arm yet).
    let n_of = |op: AggOp| node.agg.iter().filter(|a| a.op == op).count();
    let nullable_input =
        node.agg.iter().any(|a| a.col.map(|c| !bank.null_free(c)).unwrap_or(false));
    let fits = |col: Option<u32>, lo: i64, hi: i64| -> bool {
        match col {
            None => true,
            Some(c) => faces
                .stats(bank, c)
                .minmax_exact()
                .map(|(a, b)| a >= lo && b <= hi)
                .unwrap_or(false),
        }
    };
    let sum_col = node.agg.iter().find(|a| a.op == AggOp::Sum).and_then(|a| a.col);
    let avg_col = node.agg.iter().find(|a| a.op == AggOp::Avg).and_then(|a| a.col);
    if node.agg.iter().any(|a| !matches!(a.op, AggOp::CountStar | AggOp::Sum | AggOp::Avg))
        || n_of(AggOp::Sum) > 1
        || n_of(AggOp::Avg) > 1
        || nullable_input
        || node.params.having.is_some()
        || (sum_col.is_some() && !fits(sum_col, 0, u32::MAX as i64))
        || (avg_col.is_some() && !fits(avg_col, 0, i64::MAX))
    {
        return SpillClass::Unbounded;
    }
    SpillClass::Spillable
}

/// [spill-2] Distinct-leg classification (the `dense_distinct` route's
/// vocabulary): single byval null-free key with a witnessed dense-safe
/// domain, byval null-free fold/distinct inputs, one shared distinct
/// column, no HAVING/FILTER — and the UNSPILLABLE dense per-group arrays
/// priced under the E18 budget (they are shared-domain arrays, the §3.5
/// direct-array posture: elect only when resident state fits the law).
/// The PAIR plane (O(rows) scatter + owner dedupe tables) is what
/// spills. Anything outside stays Unbounded (typed refusal over budget).
fn distinct_spill_class(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> SpillClass {
    if !node.params.key_exprs.is_empty()
        || node.params.group_cols.len() != 1
        || node.params.having.is_some()
        || node.params.agg_filters.iter().any(Option::is_some)
    {
        return SpillClass::Unbounded;
    }
    let g0 = node.params.group_cols[0];
    if col_width(bank, g0) == 0 || !bank.null_free(g0) {
        return SpillClass::Unbounded;
    }
    let mut distinct_col: Option<u32> = None;
    let mut nsum = 0usize;
    for a in &node.agg {
        match a.op {
            AggOp::CountStar => {}
            AggOp::Sum | AggOp::Avg => nsum += 1,
            AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct => {
                if distinct_col.is_some() && distinct_col != a.col {
                    return SpillClass::Unbounded; // one shared distinct column
                }
                distinct_col = a.col;
            }
            _ => return SpillClass::Unbounded,
        }
        if let Some(c) = a.col {
            if col_width(bank, c) == 0 || !bank.null_free(c) {
                return SpillClass::Unbounded;
            }
        }
    }
    // Witnessed dense-safe key domain (one authority with the stencil's
    // own assert and the admission gate).
    let Some((lo, hi)) = faces.stats(bank, g0).minmax_exact() else {
        return SpillClass::Unbounded;
    };
    let range = (hi as i128 - lo as i128) + 1;
    if range <= 0 || range > crate::planner::DENSE_DISTINCT_DOMAIN as i128 {
        return SpillClass::Unbounded;
    }
    let dn = range as u64;
    let t = faces.cfg.threads.max(1) as u64;
    // The dense planes (cannot spill): per-worker pass-1 cnt+sum arrays,
    // per-owner distinct counts/first-seen sums, the single-thread merge
    // vectors. Priced conservatively against the E18 budget.
    let dense_bytes = t * dn * 8 * (1 + nsum as u64)
        + t * dn * 16
        + dn * (24 + 8 * nsum as u64);
    if dense_bytes > faces.cfg.grouped_budget_bytes() {
        return SpillClass::Unbounded;
    }
    SpillClass::SpillableDistinct
}

/// [spill-2] Should the dense-distinct route run its pair-plane spill
/// arm? One trigger for the planner law and the stencil (mirrors
/// `spill_engaged`).
pub fn distinct_spill_engaged(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    faces.cfg.spill
        && (bank.rows_total() as u128) * (SCATTER_ROW_BYTES as u128)
            > faces.cfg.grouped_budget_bytes() as u128
        && spill_class(bank, faces, node) == SpillClass::SpillableDistinct
        && crate::spill::available()
}

/// [cap-retire] Does the ruled 2^20-retirement serve this shape?
/// (RULED Michael 2026-08-19, spill-design.md §5: correctness-first —
/// any grouped shape a bounded-state hash-plane arm can serve is SERVED;
/// the exact finalize answer-bytes law replaces the group-count witness
/// cap for these shapes.) TRUE exactly when the dispatch below lands on
/// a bounded-state arm: the direct-array election under budget
/// (`Bounded`) or the SoA byval route with a registered spill substrate
/// (`Spillable`). The frame-walk route (predicate terms) has no spill
/// arm this rung and KEEPS the witness gate — `spill_class` does not see
/// predicates, so the pred check rides here, mirroring the dispatch
/// (pred-bearing shapes route to the frame path, never the SoA arm).
/// The kill switch (`PGRUST_SQE_SPILL=0`) keeps the legacy gate verbatim.
pub fn cap_retire_serves(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    if !faces.cfg.spill {
        return false;
    }
    if node
        .pred
        .as_ref()
        .is_some_and(|p| !p.terms.is_empty() || !p.var_terms.is_empty())
    {
        return false;
    }
    match spill_class(bank, faces, node) {
        SpillClass::Bounded => true,
        // [spill-2] the byte-key spill route joins the retirement map:
        // unwitnessed/over-cap varlena-key shapes in its vocabulary now
        // SERVE (routed to the byte-key arm by `bytes_spill_engaged` —
        // the dispatch mirror of this admission verdict) and answer the
        // TRUE group set or refuse typed under the exact answer-bytes
        // law the arm carries.
        SpillClass::Spillable | SpillClass::SpillableBytes => crate::spill::available(),
        // The dense-distinct route spills its PAIR plane but carries no
        // finalize answer law on its dense arrays this rung — the
        // witness gate stays (honest: excluded from the retirement map).
        SpillClass::SpillableDistinct => false,
        SpillClass::Unbounded => false,
    }
}

/// [spill-2] Should the varlena-key dispatch ride the byte-key spill
/// arm? ONE trigger for the planner law and the dispatch, mirroring
/// `cap_retire_serves`: the shape classifies `SpillableBytes` with a
/// registered substrate AND either (a) the exact row-count scatter
/// estimate crosses the E18 budget (the memory law — same trigger as
/// `spill_engaged`), or (b) the shape relies on the cap-retirement to
/// serve at all (unwitnessed or over-cap group count, no k-bounded
/// answer): those shapes MUST land on the bounded-state arm — the tuned
/// arms carry no finalize answer law and their emit cap could truncate.
pub fn bytes_spill_engaged(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    if !faces.cfg.spill
        || spill_class(bank, faces, node) != SpillClass::SpillableBytes
        || !crate::spill::available()
    {
        return false;
    }
    let over = (bank.rows_total() as u128) * (SCATTER_ROW_BYTES as u128)
        > faces.cfg.grouped_budget_bytes() as u128;
    if over {
        return true;
    }
    let k_bounded = node
        .params
        .topk
        .as_ref()
        .is_some_and(|t| t.n > 0 && t.n as u64 <= crate::planner::GROUP_ROW_CAP);
    !k_bounded
        && crate::planner::grouped_count_bound(bank, faces, node)
            .map_or(true, |b| b > crate::planner::GROUP_ROW_CAP)
}

/// [cap-retire] The finalize answer-bytes law (spill-design.md §3.4,
/// RULED 2026-08-19): exact accounting of the grouped answer plane this
/// arm is about to materialize — `groups` staging rows (the flatten
/// vector's exact element bytes) plus the rendered emit lanes over the
/// rows that actually emit — priced against the effective E17 answer-
/// face budget BEFORE the flatten. Every input is counted, never
/// estimated: `groups` is the owner-summed true group count in hand at
/// finalize; the lane widths are compile-time constants of this arm's
/// render (key lanes are i64 answer words +1 validity byte on the
/// nullable pack; CountStar renders i64, Sum an exact i128, Avg an
/// (i128, i64) ratio pair, Min/Max i64 moments). Over budget = typed
/// 53400 runtime refusal with the true counts carried
/// (`Refuse::GroupAnswerOverBudget`). The kill switch
/// (`PGRUST_SQE_SPILL=0`) is the legacy arm: no accounting, no refusal.
pub(crate) fn check_answer_budget(
    ctx: &SqeCtx,
    node: &PlanNode,
    groups: u64,
    stage_row_bytes: usize,
    nullable_keys: bool,
) {
    if !ctx.faces.cfg.spill {
        return;
    }
    // Rows that reach the render window: the full set, clipped by an
    // authored offset+limit and/or a pushed top-k bound.
    let mut emit = groups;
    let mut bounded = false;
    if node.params.limit != usize::MAX {
        emit = emit.min(node.params.offset.saturating_add(node.params.limit) as u64);
        bounded = true;
    }
    if let Some(t) = &node.params.topk {
        if t.n > 0 {
            emit = emit.min(t.n as u64);
            bounded = true;
        }
    }
    let nkeys = node.params.group_cols.len().max(1);
    let mut render_row = nkeys * (8 + usize::from(nullable_keys));
    for a in &node.agg {
        render_row += match a.op {
            AggOp::CountStar => 8,
            AggOp::Sum => 16,
            AggOp::Avg => 24,
            // Min/Max moments render i64; anything wider is outside
            // this family's render vocabulary (defensive widest cell).
            AggOp::Min | AggOp::Max => 8,
            _ => 24,
        };
    }
    // The staging (flatten) plane prices only UNBOUNDED answers — the
    // class the retirement newly admits. A bounded answer (authored
    // limit or pushed top-k) emits O(k) and its transient staging was
    // pre-ruling served behavior (the E17b occupancy philosophy) —
    // pricing it here would REMOVE servability, which the ruling never
    // does; its ANSWER plane still prices exactly below.
    let got = if bounded {
        emit.saturating_mul(render_row as u64)
    } else {
        groups
            .saturating_mul(stage_row_bytes as u64)
            .saturating_add(emit.saturating_mul(render_row as u64))
    };
    let budget = ctx.faces.cfg.answer_budget_bytes();
    if got > budget {
        crate::refuse::raise_runtime(crate::refuse::Refuse::GroupAnswerOverBudget {
            got,
            budget,
        });
    }
}

/// Should the SoA route run its spill arm? One trigger for the planner
/// law and the dispatch: budget crossed by the exact row-count scatter
/// estimate AND the shape classifies spillable.
pub fn spill_engaged(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    faces.cfg.spill
        && (bank.rows_total() as u128) * (SCATTER_ROW_BYTES as u128)
            > faces.cfg.grouped_budget_bytes() as u128
        && spill_class(bank, faces, node) == SpillClass::Spillable
}

/// Per-worker pass-1 spill writer: one private file, a chunk directory
/// `(partition, offset, rows)`, and one reusable slab buffer.
struct SpillW {
    m: Box<dyn crate::spill::SpillMedium>,
    chunks: Vec<(u32, u64, u32)>,
    buf: Vec<u8>,
    /// Rows resident in the owning worker's buckets since the last flush.
    resident_rows: usize,
    /// Chunk-start offset of the open chunk (single WRITER per medium, so
    /// successive appends are contiguous — the chunk is one extent).
    pending_off: Option<u64>,
}

impl SpillW {
    fn new(store: &dyn crate::spill::SpillStore, purpose: &'static str, w: usize) -> SpillW {
        let m = store
            .file(purpose, w)
            .unwrap_or_else(|e| crate::spill::io_fail("create", e));
        SpillW { m, chunks: Vec::new(), buf: Vec::new(), resident_rows: 0, pending_off: None }
    }

    /// Slab-buffered chunk append: `begin`, `push` records, `end` -> the
    /// chunk's start offset (read-ahead stays bounded at slab grain).
    fn begin(&mut self) {
        self.buf.clear();
        self.pending_off = None;
    }
    #[inline]
    fn push(&mut self, rec: &[u8]) {
        self.buf.extend_from_slice(rec);
        if self.buf.len() >= crate::spill::SLAB_BYTES {
            self.flush_slab();
        }
    }
    fn end(&mut self) -> u64 {
        self.flush_slab();
        self.pending_off.take().expect("spill chunk must not be empty")
    }
    fn flush_slab(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let off = self
            .m
            .append(&self.buf)
            .unwrap_or_else(|e| crate::spill::io_fail("append", e));
        if self.pending_off.is_none() {
            self.pending_off = Some(off);
        }
        self.buf.clear();
    }
}

/// Pass-2 partition owner's spill state: a lazily created private run
/// file, the CURRENT partition's run directory `(offset, groups)`, and
/// the drain/merge scratch row buffer.
struct P2Sp {
    store: Option<std::sync::Arc<dyn crate::spill::SpillStore>>,
    w: usize,
    rw: Option<SpillW>,
    runs: Vec<(u64, u64)>,
    tmp: Vec<Row>,
}

impl P2Sp {
    /// Drain the owner's table as one key-ASC sorted run and re-arm it
    /// (spill-design.md §3.2). Values ride untouched — the merge combine
    /// is the fold's own integer arithmetic, so run boundaries cannot
    /// change any answer byte.
    #[cold]
    fn drain(&mut self, oa: &mut OaSoA) {
        let rw = self.rw.get_or_insert_with(|| {
            SpillW::new(
                &**self.store.as_ref().expect("spill store armed when capped"),
                "runs",
                self.w,
            )
        });
        self.tmp.clear();
        for s in 0..=oa.mask {
            if oa.keys[s] != EMPTY {
                self.tmp.push(Row {
                    key: oa.keys[s],
                    c: oa.cnt[s],
                    sr: oa.srs[s],
                    sw: oa.sws[s],
                });
            }
        }
        self.tmp.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        rw.begin();
        for r in &self.tmp {
            let mut rec = [0u8; RUN_REC_BYTES];
            rec[..16].copy_from_slice(&r.key.to_ne_bytes());
            rec[16..24].copy_from_slice(&r.c.to_ne_bytes());
            rec[24..32].copy_from_slice(&r.sr.to_ne_bytes());
            rec[32..40].copy_from_slice(&r.sw.to_ne_bytes());
            rw.push(&rec);
        }
        let off = rw.end();
        self.runs.push((off, self.tmp.len() as u64));
        oa.keys[..=oa.mask].fill(EMPTY);
        oa.cnt[..=oa.mask].fill(0);
        oa.len = 0;
    }
}

/// Finalize a spilled partition: k-way merge the key-sorted runs plus
/// the final table (itself key-sorted), combining equal keys with the
/// fold's own `+` laws, streaming into the same top-k/full-rows sinks
/// the resident path uses (spill-design.md §3.3).
fn merge_runs(oa: &mut OaSoA, ps: &mut P2Sp, rows: &mut Vec<Row>, kk: usize, share: usize) {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    SPILL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let P2Sp { rw, runs, tmp, .. } = ps;
    tmp.clear();
    for s in 0..=oa.mask {
        if oa.keys[s] != EMPTY {
            tmp.push(Row { key: oa.keys[s], c: oa.cnt[s], sr: oa.srs[s], sw: oa.sws[s] });
        }
    }
    tmp.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    let m = &*rw.as_ref().expect("runs imply a run file").m;
    let nrun = runs.len();
    // Bounded merge read-ahead: the owner's share divided across live
    // cursors (floor one record).
    let slab = (share / (nrun + 1)).clamp(RUN_REC_BYTES, crate::spill::SLAB_BYTES);
    let mut curs: Vec<crate::spill::ChunkCursor> = runs
        .iter()
        .map(|&(off, g)| crate::spill::ChunkCursor::new(m, off, g, RUN_REC_BYTES, slab))
        .collect();
    let dec = |r: &[u8]| Row {
        key: u128::from_ne_bytes(r[..16].try_into().unwrap()),
        c: u64::from_ne_bytes(r[16..24].try_into().unwrap()),
        sr: u64::from_ne_bytes(r[24..32].try_into().unwrap()),
        sw: u64::from_ne_bytes(r[32..40].try_into().unwrap()),
    };
    // Stream heads: runs 0..nrun, the resident table stream at nrun.
    let mut heads: Vec<Option<Row>> = Vec::with_capacity(nrun + 1);
    let mut mi = 0usize;
    let mut heap: BinaryHeap<Reverse<(u128, usize)>> = BinaryHeap::with_capacity(nrun + 1);
    for (i, c) in curs.iter_mut().enumerate() {
        let h = c.next().map(dec);
        if let Some(row) = h {
            heap.push(Reverse((row.key, i)));
        }
        heads.push(h);
    }
    let mem_head = (mi < tmp.len()).then(|| {
        let r = tmp[mi];
        mi += 1;
        r
    });
    if let Some(row) = mem_head {
        heap.push(Reverse((row.key, nrun)));
    }
    heads.push(mem_head);
    while let Some(Reverse((key, src))) = heap.pop() {
        let mut cand = heads[src].take().expect("head present for heap entry");
        debug_assert_eq!(cand.key, key);
        let mut adv = |heads: &mut Vec<Option<Row>>,
                       heap: &mut BinaryHeap<Reverse<(u128, usize)>>,
                       s: usize| {
            let nx = if s < nrun {
                curs[s].next().map(dec)
            } else if mi < tmp.len() {
                let r = tmp[mi];
                mi += 1;
                Some(r)
            } else {
                None
            };
            if let Some(row) = nx {
                heap.push(Reverse((row.key, s)));
            }
            heads[s] = nx;
        };
        adv(&mut heads, &mut heap, src);
        while let Some(&Reverse((k2, s2))) = heap.peek() {
            if k2 != cand.key {
                break;
            }
            heap.pop();
            let other = heads[s2].take().expect("head present for heap entry");
            cand.c += other.c;
            cand.sr += other.sr;
            cand.sw += other.sw;
            adv(&mut heads, &mut heap, s2);
        }
        if kk == usize::MAX {
            rows.push(cand);
        } else {
            topk_consider(rows, kk, cand);
        }
    }
}

pub fn run_hash_plane_owned_group(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    // [famB M1] Predicate-bearing route: frame walk + composite KeyExpr
    // keys (the hot-shape shape) — partition-owned u128 fold over the shared
    // frame (condition-cache entry shared with hot-shape).
    if node.pred.as_ref().map(|p| !p.terms.is_empty() || !p.var_terms.is_empty()).unwrap_or(false)
        && !node.params.key_exprs.is_empty()
    {
        return frame_owned_u128(ctx, node);
    }
    // [tpch-wave-3] the (text, text) cells route.
    if crate::planner::text_pair_cells_shape(bank, node)
        && (node.pred.as_ref().is_some_and(|p| !p.terms.is_empty())
            || node.agg.len() != 1
            || node.agg.iter().any(|a| a.op != AggOp::CountStar))
    {
        return super::hash_group::gid_pair_cells(ctx, node);
    }
    // [tpch-expr] byval cells route: pred/expr shapes ride foundation.
    if crate::planner::byval_cells_shape(bank, node)
        && (node.agg.iter().any(|a| a.expr.is_some())
            || node.pred.as_ref().is_some_and(|p| !p.terms.is_empty()))
    {
        return foundation_cells(ctx, node);
    }
    // Beyond the frame route, int predicate terms never reach this family;
    // a `<> ''` conjunct on the varlena KEY column arrives as F_DROP_EMPTY_KEY
    // + ne_empty_cols (evaluated in gid domain by the filtered shapes).
    assert!(
        node.pred.as_ref().map(|p| p.terms.is_empty() && p.var_terms.is_empty()).unwrap_or(true),
        "hash-plane stencil takes no unrouted predicate terms"
    );
    // ---- famA shape dispatch (schema/stats facts, never query identity):
    // a CountDistinct leg elects the dense-domain + distinct-pipeline form;
    // a Minute key element elects the derived ord-pack shape (sqe-m1);
    // otherwise the group key's column-width signature routes among the
    // int / int+gid / string-hash / packed-pair shapes.
    let widths: Vec<u8> =
        node.params.group_cols.iter().map(|&c| col_width(bank, c)).collect();
    // [aggqual] FILTER-bearing legs: the Cells128 foundation is the one
    // filter-threaded body — it routes FIRST; the tuned arms never see
    // a filtered leg (admission gates the shape to 1-2 byval keys).
    if node.params.agg_filters.iter().any(Option::is_some) {
        return foundation_cells(ctx, node);
    }
    if node.agg.iter().any(|a| {
        matches!(a.op, AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct)
    }) {
        return super::hash_group::dense_distinct(ctx, node);
    }
    if node.params.key_exprs.iter().any(|e| matches!(e, KeyExpr::Minute(_))) {
        return super::hash_group::ord_pack(ctx, node);
    }
    let drop_empty = node.params.flags & F_DROP_EMPTY_KEY != 0;
    let count_only = node.agg.iter().all(|a| a.op == AggOp::CountStar);
    // [P6-1 spill] Budget trigger (one authority with the planner law):
    // over-budget spillable shapes skip the tuned no-spill elections and
    // ride the SoA arm's spill lanes below. Answers are byte-identical
    // across arms (the A/B identity gate), so this is an election.
    let sp_on = spill_engaged(bank, ctx.faces, node);
    // [spill-2] Byte-key spill election: over-budget (or cap-retire-
    // relied) varlena-key shapes skip the tuned text arms and ride the
    // byte-key spill route — same election law as the SoA arm below
    // (byte-identical answers, the A/B identity gate forces both arms).
    if bytes_spill_engaged(bank, ctx.faces, node) {
        debug_assert!(
            matches!(widths.as_slice(), [0] | [0, 0])
                || matches!(widths.as_slice(), [w, 0] if *w > 0),
            "SpillableBytes classification must match the dispatchable text layouts"
        );
        // [q3334 textgroup] the k-bounded single-text band rides the
        // fp128-first spilled combine (28 B priced records, lazy byte
        // resolve — the partmerge idiom) instead of the byte-payload
        // scatter; election floors + kill switch live with the arm.
        if matches!(widths.as_slice(), [0])
            && super::hash_group::text128_spill_elects(ctx, node)
        {
            return super::hash_group::text128_spill(ctx, node);
        }
        return super::hash_group::byte_spill(ctx, node);
    }
    match widths.as_slice() {
        // [sqe-m4] the tuned int_key kernel is CountStar-only AND
        // null-blind; a nullable key column rides the foundation path's
        // nullmask packing below (3VL: NULL is its own group).
        // [sqe-tpch-mech] a fused HAVING filters at the answer boundary
        // from the fold cells — the main-body routes below own it.
        [w] if *w > 0
            && count_only
            && bank.null_free(node.params.group_cols[0])
            && node.params.having.is_none()
            && !sp_on =>
        {
            return super::hash_group::int_key(ctx, node)
        }
        [w, 0] if *w > 0 && drop_empty => {
            return super::hash_group::int_gid_filtered(ctx, node)
        }
        [w, 0] if *w > 0 => return super::hash_group::int_gid(ctx, node),
        [0] => return super::hash_group::text128(ctx, node),
        // [sqe-m4] two varlena keys: gid-pair identity over the merge
        // registries (byte-ordered gids => packed-key ASC == bytes ASC).
        [0, 0] => return super::hash_group::gid_pair(ctx, node),
        _ => {} // 1-2 byval packed key: the foundation path below.
    }
    let g0 = node.params.group_cols[0];
    // [sqe-m4] the packed key is 1 OR 2 byval columns (w1_bits = 0 for a
    // single key column — the pack degenerates to the raw datum).
    let g1: Option<u32> = node.params.group_cols.get(1).copied();
    let w1_bits = g1.map(|c| 8 * col_width(bank, c) as u32).unwrap_or(0);
    // 3VL nullmask lanes (the lx4 KeyTable `(k0, k1, nullmask)` law):
    // nullable key columns carry a null BIT interleaved PER FIELD —
    // layout [n0][v0][n1][v1] high-to-low — so the packed-key unsigned
    // order is exactly the elementwise (value ASC, NULL greatest) tie
    // order of the oracle's KElem sequence. NULL zeroes its value field
    // (NULL keys collapse to one group). Null-free banks take the exact
    // PoC pack (no bits, no masks, no extra shifts — law 11).
    let w0_bits = 8 * col_width(bank, g0) as u32;
    let nf0 = bank.null_free(g0);
    let nf1 = g1.map(|c| bank.null_free(c)).unwrap_or(true);
    let nullable = !(nf0 && nf1);
    // nullable layout shifts (unused on the null-free path):
    let n1_shift = w1_bits; // n1 bit sits just above the v1 field
    let v0_shift = if g1.is_some() { w1_bits + 1 } else { 0 };
    let n0_shift = v0_shift + w0_bits;
    assert!(
        w0_bits + w1_bits <= if nullable { 125 } else { 127 },
        "packed group key must leave the u128 sentinel (and nullmask lanes) unreachable"
    );
    let mask1: u128 = if w1_bits == 0 { 0 } else { (1u128 << w1_bits) - 1 };
    // nullable-path v0 field mask (decoded datums are extension-
    // convention 64-bit words; the field must not spill into n0).
    let mask0: u128 = if w0_bits >= 64 { u64::MAX as u128 } else { (1u128 << w0_bits) - 1 };

    // agg spec: CountStar (+ optional Sum col + optional Avg col).
    let sum_col = node.agg.iter().find(|a| a.op == AggOp::Sum).and_then(|a| a.col);
    let avg_col = node.agg.iter().find(|a| a.op == AggOp::Avg).and_then(|a| a.col);
    // Payload-pack election, PROVEN by stats: Sum input in {0,1} (bit 31),
    // Avg input in [0,65535] (low 16). Falls back to the wide path.
    let fits = |col: Option<u32>, lo: i64, hi: i64| -> bool {
        match col {
            None => true,
            Some(c) => ctx
                .faces
                .stats(bank, c)
                .minmax_exact()
                .map(|(a, b)| a >= lo && b <= hi)
                .unwrap_or(false),
        }
    };
    // [sqe-grpfold] Cells128 fold route: Min/Max legs, multiple Sum/Avg
    // legs, nullable fold inputs, or sum/avg domains outside the tuned
    // u32/u64 payload lanes leave the packed/wide fast paths below — the
    // general per-group cell fold owns those shapes (exact i128 sums,
    // 3VL fold inputs, AVG = sum/count(nonnull) by the Ratio lane).
    let nullable_input =
        node.agg.iter().any(|a| a.col.map(|c| !bank.null_free(c)).unwrap_or(false));
    let n_of = |op: AggOp| node.agg.iter().filter(|a| a.op == op).count();
    // The tuned packed/wide payload lanes serve exactly CountStar +
    // (<=1 Sum) + (<=1 Avg); every other admitted fold op (Min/Max, the
    // variance family, bit folds) rides the Cells128 route.
    // [sqe-tpch-mech] direct-array grouped state: a witnessed dense-and-
    // bounded single null-free byval key elects a shared atomic
    // accumulator array indexed `key - lo` (the tpch-floor Q18 idiom)
    // in place of every hash arm below. Same fold law, same
    // (count DESC, key ASC) render, same cell renders — byte-identical
    // answers (the A/B identity gate forces both arms).
    if ctx.faces.cfg.direct_array && g1.is_none() && node.params.key_exprs.is_empty() && nf0 {
        let vocab_ok = node.agg.iter().all(|a| {
            matches!(a.op, AggOp::CountStar | AggOp::Sum | AggOp::Avg | AggOp::Min | AggOp::Max)
        });
        // The exact-decimal AVG render (width-8 inputs) lives on the
        // foundation arm only; keep it there for render identity.
        let avg_w8 = node
            .agg
            .iter()
            .any(|a| matches!(a.op, AggOp::Avg) && a.in_ty.map(|t| t.width == 8).unwrap_or(false));
        let fold_lanes =
            node.agg.iter().filter(|a| crate::fold::fold_op_of(a.op).is_some()).count();
        // [sqe-hugedom] answer-bound budget law: a bounded answer (fused
        // HAVING emits only survivors; a pushed top-k emits <= n rows)
        // with all-zero-init lanes (Sum/Count — no Min/Max sentinel
        // fill) elects under the occupancy-priced bounded budget; the
        // array rides lazily-zeroed pages, so resident bytes track the
        // keys actually present, never the witnessed domain width.
        let zero_init = node.agg.iter().all(|a| {
            matches!(
                crate::fold::fold_op_of(a.op),
                None | Some(crate::fold::AggFoldOp::Sum)
            )
        });
        let bounded_answer = node.params.having.is_some()
            || node
                .params
                .topk
                .as_ref()
                .is_some_and(|t| t.n > 0 && t.n as u64 <= crate::planner::GROUP_ROW_CAP);
        let cap = crate::planner::direct_array_bytes_cap(zero_init && bounded_answer);
        if vocab_ok && !avg_w8 {
            if let Some((lo, dn)) =
                crate::planner::direct_array_domain_capped(bank, ctx.faces, g0, fold_lanes, cap)
            {
                let sums_fit = node
                    .agg
                    .iter()
                    .filter(|a| matches!(a.op, AggOp::Sum | AggOp::Avg))
                    .all(|a| {
                        crate::planner::direct_sum_fits(
                            bank,
                            ctx.faces,
                            a.col.expect("sum/avg input"),
                            bank.rows_total(),
                        )
                    });
                // [P6-1 spill] the direct array cannot spill; under the
                // memory law it is elected only when its state sits
                // inside the E18 budget (vacuous at default budgets —
                // the array cap is far under the law's default).
                let array_bytes = dn as u64 * 8 * (1 + 2 * fold_lanes as u64);
                let in_budget = !ctx.faces.cfg.spill
                    || array_bytes <= ctx.faces.cfg.grouped_budget_bytes();
                if sums_fit && in_budget {
                    return dense_direct(ctx, node, lo, dn);
                }
            }
        }
    }
    if node.agg.iter().any(|a| !matches!(a.op, AggOp::CountStar | AggOp::Sum | AggOp::Avg))
        || n_of(AggOp::Sum) > 1
        || n_of(AggOp::Avg) > 1
        || nullable_input
        || node.params.having.is_some()
        || (sum_col.is_some() && !fits(sum_col, 0, u32::MAX as i64))
        || (avg_col.is_some() && !fits(avg_col, 0, i64::MAX))
    {
        return foundation_cells(ctx, node);
    }
    let packed = fits(sum_col, 0, 1) && fits(avg_col, 0, 0xFFFF);
    // wide-path per-row sum inputs ride a u32 lane; the ACCUMULATOR is
    // u64 (srs). Witnessed precondition from stats, never assumed.
    assert!(
        packed || fits(sum_col, 0, u32::MAX as i64),
        "hash_plane: sum input exceeds the u32 row lane (gap)"
    );

    // column order for decode: group cols then agg input cols.
    let mut cols: Vec<u32> = match g1 {
        Some(g1) => vec![g0, g1],
        None => vec![g0],
    };
    let mut sum_idx = usize::MAX;
    let mut avg_idx = usize::MAX;
    if let Some(c) = sum_col {
        sum_idx = cols.len();
        cols.push(c);
    }
    if let Some(c) = avg_col {
        avg_idx = cols.len();
        cols.push(c);
    }
    let ncols = cols.len();

    // partition law over the leading group col's ndv estimate.
    let ndv = ctx.faces.stats(bank, g0).ndv_est_sum() as usize;
    let p =
        partition_count(ndv.max(1), node.params.slot_bytes, node.params.l2_bytes, pool.threads());
    let shift = 64 - p.trailing_zeros();
    let units = ctx.faces.walk(bank, g0);
    let t = pool.threads();
    let rows_total = bank.rows_total() as usize;
    let per_bucket = (rows_total / t.max(1) / p) * 5 / 4 + 16;

    // [P6-1 spill] the statement's spill namespace + the E18b per-worker
    // share (spill-design.md §2-3). `None` = the in-memory arm exactly as
    // before — no accounting, no per-row cost.
    let store: Option<std::sync::Arc<dyn crate::spill::SpillStore>> = if sp_on {
        Some(crate::spill::new_store().unwrap_or_else(|| {
            panic!("sqe spill: substrate unavailable at run (planner admitted the shape)")
        }))
    } else {
        None
    };
    let budget = ctx.faces.cfg.grouped_budget_bytes();
    let share_bytes = ((budget / t.max(1) as u64).max(1)) as usize;
    let scat_rec = if packed { 20usize } else { 28usize };

    struct S {
        scr: Vec<Scratch>,
        cc: Vec<CurCache>,
        keys: Vec<Vec<u128>>,
        pays: Vec<Vec<u32>>,  // packed path
        pays_a: Vec<Vec<u32>>, // wide path: sum inputs
        pays_b: Vec<Vec<u64>>, // wide path: avg inputs
        sp: Option<SpillW>,
    }
    struct SK {
        keys: Vec<Vec<u128>>,
        pays: Vec<Vec<u32>>,
        pays_a: Vec<Vec<u32>>,
        pays_b: Vec<Vec<u64>>,
        /// Spilled scatter chunks: the medium + `(partition, off, rows)`.
        sp: Option<(Box<dyn crate::spill::SpillMedium>, Vec<(u32, u64, u32)>)>,
    }
    // [sqe-m2] PARKED scatter arenas (data-only — no column binding, so
    // the hot-shape cross-query trap does not apply): the ~GBs of per-worker
    // bucket vectors are allocated once per process, not once per rep.
    // The hot-shape in-suite 3x regression was this path re-faulting its
    // scatter pages under the post-hot-shape fragmented heap every rep.
    type Park32 = (Vec<Vec<u128>>, Vec<Vec<u32>>, Vec<Vec<u32>>, Vec<Vec<u64>>);
    // [persist-rehome] formerly uncapped — now under the shrink law.
    // [sqe-park-knobs] cap env-overridable (MB) for the allocator
    // trade-study grid; default unchanged.
    static PARK32: crate::stencils::statepark::StatePark<Park32> =
        crate::stencils::statepark::StatePark::new_env(256 << 20, "PGRUST_SQE_PARK32_MB");
    let t_p1 = std::time::Instant::now();
    let storer = &store;
    let pass1 = pool.run_finish(
        units.len(),
        |w| {
            let parked = PARK32.fetch();
            let (mut keys, mut pays, mut pays_a, mut pays_b) =
                parked.unwrap_or_default();
            let arm = |v: &mut Vec<Vec<u128>>| {
                if v.len() != p {
                    *v = (0..p).map(|_| Vec::with_capacity(per_bucket)).collect();
                } else {
                    v.iter_mut().for_each(|b| b.clear());
                }
            };
            arm(&mut keys);
            let arm32 = |v: &mut Vec<Vec<u32>>, want: usize| {
                if v.len() != want {
                    *v = (0..want).map(|_| Vec::new()).collect();
                } else {
                    v.iter_mut().for_each(|b| b.clear());
                }
            };
            let arm64 = |v: &mut Vec<Vec<u64>>, want: usize| {
                if v.len() != want {
                    *v = (0..want).map(|_| Vec::new()).collect();
                } else {
                    v.iter_mut().for_each(|b| b.clear());
                }
            };
            arm32(&mut pays, if packed { p } else { 0 });
            arm32(&mut pays_a, if packed { 0 } else { p });
            arm64(&mut pays_b, if packed { 0 } else { p });
            S {
                scr: (0..ncols).map(|_| crate::scan::scratch_fetch()).collect(),
                cc: cols.iter().map(|&a| CurCache::new(a)).collect(),
                keys,
                pays,
                pays_a,
                pays_b,
                sp: storer.as_ref().map(|st| SpillW::new(&**st, "scatter", w)),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let nkeys = if g1.is_some() { 2 } else { 1 };
            // decode all node columns for the granule (persistent scratch);
            // nullable KEY lanes also fetch the granule validity verdict
            // (AllValid short-circuits before any bitmap work).
            let mut gvs = [crate::scan::GranValid::AllValid; 2];
            let mut ds: Vec<&[u64]> = Vec::with_capacity(ncols);
            for (ci, (scr, cc)) in s.scr.iter_mut().zip(s.cc.iter_mut()).enumerate() {
                let cur = cc.get(bank, pi);
                if ci < nkeys && ((ci == 0 && !nf0) || (ci == 1 && !nf1)) {
                    gvs[ci] = scr.validity(cur, g, rows);
                }
                let d = scr.decode_full(cur, g, rows);
                // each slice lives in its own scratch until the next granule
                ds.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
            }
            // Two-loop variant splits on the loop-invariant `nullable`
            // and `packed` elections (R2: no per-row branch on a granule
            // constant); the key packs are byte-identical.
            let S { scr, keys, pays, pays_a, pays_b, sp, .. } = s;
            // NOT NULL fast path: the PoC pack.
            let key_nn = |k: usize| -> u128 {
                if w1_bits == 0 {
                    ds[0][k] as u128
                } else {
                    ((ds[0][k] as u128) << w1_bits) | (ds[1][k] as u128 & mask1)
                }
            };
            // 3VL pack ([n0][v0][n1][v1]): NULL zeroes its value field
            // and sets the FIELD's null bit — NULL keys collapse to ONE
            // group and sort greatest per element.
            let key_3vl = |k: usize| -> u128 {
                let ok0 = nf0 || gvs[0].all_valid() || scr[0].row_valid(k);
                let ok1 = g1.is_none() || nf1 || gvs[1].all_valid() || scr[1].row_valid(k);
                let mut key: u128 = 0;
                if ok0 {
                    key |= (ds[0][k] as u128 & mask0) << v0_shift;
                } else {
                    key |= 1u128 << n0_shift;
                }
                if g1.is_some() {
                    if ok1 {
                        key |= ds[1][k] as u128 & mask1;
                    } else {
                        key |= 1u128 << n1_shift;
                    }
                }
                key
            };
            let pay_of = |k: usize| -> (u32, u64) {
                let sr = if sum_idx != usize::MAX { ds[sum_idx][k] as u32 } else { 0 };
                let sw = if avg_idx != usize::MAX { ds[avg_idx][k] } else { 0 };
                (sr, sw)
            };
            match (nullable, packed) {
                (false, true) => {
                    for k in 0..rows {
                        let key = key_nn(k);
                        let b = (hash128(key) >> shift) as usize;
                        let (sr, sw) = pay_of(k);
                        keys[b].push(key);
                        pays[b].push((sr << 31) | (sw as u32 & 0xFFFF));
                    }
                }
                (false, false) => {
                    for k in 0..rows {
                        let key = key_nn(k);
                        let b = (hash128(key) >> shift) as usize;
                        let (sr, sw) = pay_of(k);
                        keys[b].push(key);
                        pays_a[b].push(sr);
                        pays_b[b].push(sw);
                    }
                }
                (true, true) => {
                    for k in 0..rows {
                        let key = key_3vl(k);
                        let b = (hash128(key) >> shift) as usize;
                        let (sr, sw) = pay_of(k);
                        keys[b].push(key);
                        pays[b].push((sr << 31) | (sw as u32 & 0xFFFF));
                    }
                }
                (true, false) => {
                    for k in 0..rows {
                        let key = key_3vl(k);
                        let b = (hash128(key) >> shift) as usize;
                        let (sr, sw) = pay_of(k);
                        keys[b].push(key);
                        pays_a[b].push(sr);
                        pays_b[b].push(sw);
                    }
                }
            }
            // [P6-1 spill] E18b flush: when this worker's resident
            // scatter rows cross its share, every bucket flushes to the
            // worker's spill file as one chunk per partition (capacity
            // kept — the arena stays parked-size; the flush is what
            // bounds resident bytes). Granule grain, never per row.
            if let Some(sp) = sp {
                sp.resident_rows += rows;
                if sp.resident_rows * scat_rec > share_bytes {
                    SPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    for b in 0..p {
                        let n = keys[b].len();
                        if n == 0 {
                            continue;
                        }
                        sp.begin();
                        if packed {
                            for i in 0..n {
                                let mut rec = [0u8; 20];
                                rec[..16].copy_from_slice(&keys[b][i].to_ne_bytes());
                                rec[16..20].copy_from_slice(&pays[b][i].to_ne_bytes());
                                sp.push(&rec);
                            }
                            pays[b].clear();
                        } else {
                            for i in 0..n {
                                let mut rec = [0u8; 28];
                                rec[..16].copy_from_slice(&keys[b][i].to_ne_bytes());
                                rec[16..20].copy_from_slice(&pays_a[b][i].to_ne_bytes());
                                rec[20..28].copy_from_slice(&pays_b[b][i].to_ne_bytes());
                                sp.push(&rec);
                            }
                            pays_a[b].clear();
                            pays_b[b].clear();
                        }
                        let off = sp.end();
                        sp.chunks.push((b as u32, off, n as u32));
                        keys[b].clear();
                    }
                    sp.resident_rows = 0;
                }
            }
        },
        |s| {
            s.scr.into_iter().for_each(crate::scan::scratch_park);
            SK {
                keys: s.keys,
                pays: s.pays,
                pays_a: s.pays_a,
                pays_b: s.pays_b,
                sp: s.sp.map(|sp| (sp.m, sp.chunks)),
            }
        },
    );

    // pass 2: one owner per partition; per-owner top-k when the order is
    // CountDesc LIMIT k, else full rows. A NATIVE pushed bound
    // (`params.topk`, the seam's down-pass) is the same contract as the
    // authored CountDesc/limit pair: the per-owner (count DESC, key ASC)
    // selection serves it directly — without this, the pushed bound only
    // trims AT the answer boundary and the full group set is flattened,
    // sorted, and rendered first (the near-unique-group serial tail).
    let kk = if node.params.order == OrderBy::CountDesc && node.params.limit != usize::MAX {
        node.params.offset + node.params.limit
    } else {
        match &node.params.topk {
            Some(t) if t.native => t.n,
            _ => usize::MAX,
        }
    };
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    // [persist-rehome] formerly uncapped — now under the shrink law.
    // [sqe-park-knobs] cap env-overridable (MB) for the allocator
    // trade-study grid; default unchanged.
    static PARKOA: crate::stencils::statepark::StatePark<OaSoA> =
        crate::stencils::statepark::StatePark::new_env(64 << 20, "PGRUST_SQE_PARKOA_MB");
    let scattered: Vec<&SK> = pass1.iter().collect();
    // [P6-1 spill] E18b per-owner table cap (entries) and the bounded
    // chunk read-ahead. The 128-entry floor is the drain grain (a
    // degenerate cap would shred the runs); its footprint is fixed and
    // tiny (~9 KiB/owner).
    let cap_entries = ((share_bytes as u64 / OA_ENTRY_BYTES).max(128)) as usize;
    let chunk_slab = crate::spill::SLAB_BYTES.min(share_bytes.max(scat_rec));
    let owned = pool.run(
        p,
        |w| {
            (
                Vec::new(),
                PARKOA.fetch().unwrap_or_else(|| OaSoA::new(16)),
                P2Sp { store: store.clone(), w, rw: None, runs: Vec::new(), tmp: Vec::new() },
            )
        },
        |(rows, oa, ps): &mut (Vec<Row>, OaSoA, P2Sp), part| {
            let n_mem: usize = scattered.iter().map(|b| b.keys[part].len()).sum();
            let n_sp: u64 = scattered
                .iter()
                .flat_map(|b| b.sp.iter())
                .flat_map(|(_, ch)| ch.iter())
                .filter(|c| c.0 as usize == part)
                .map(|c| c.2 as u64)
                .sum();
            let total = n_mem as u64 + n_sp;
            // The cap binds only when this partition's ROWS exceed it (a
            // low-group partition never drains: `len` stays under).
            let capped = sp_on && total > cap_entries as u64;
            oa.reset(if capped { cap_entries } else { total as usize });
            ps.runs.clear();
            // Spilled chunks first (sequential reads), then the resident
            // bucket residue — arrival order is erased by the render's
            // total order either way.
            for b in &scattered {
                if let Some((m, chunks)) = &b.sp {
                    for &(cb, off, n) in chunks {
                        if cb as usize != part {
                            continue;
                        }
                        let mut cur = crate::spill::ChunkCursor::new(
                            &**m, off, n as u64, scat_rec, chunk_slab,
                        );
                        while let Some(r) = cur.next() {
                            let key = u128::from_ne_bytes(r[..16].try_into().unwrap());
                            let (sr, sw) = if packed {
                                let pay = u32::from_ne_bytes(r[16..20].try_into().unwrap());
                                ((pay >> 31) as u64, (pay & 0xFFFF) as u64)
                            } else {
                                (
                                    u32::from_ne_bytes(r[16..20].try_into().unwrap()) as u64,
                                    u64::from_ne_bytes(r[20..28].try_into().unwrap()),
                                )
                            };
                            if capped && oa.len >= cap_entries {
                                ps.drain(oa);
                            }
                            oa.add(hash128(key) as usize, key, sr, sw);
                        }
                    }
                }
            }
            // Two-loop split on the loop-invariant `packed` election (and
            // on `capped` — the no-spill loops stay branch-free).
            for b in &scattered {
                let keys = &b.keys[part];
                match (packed, capped) {
                    (true, false) => {
                        let pays = &b.pays[part];
                        for i in 0..keys.len() {
                            let (key, pay) = (keys[i], pays[i]);
                            oa.add(
                                hash128(key) as usize,
                                key,
                                (pay >> 31) as u64,
                                (pay & 0xFFFF) as u64,
                            );
                        }
                    }
                    (false, false) => {
                        let (pa, pb) = (&b.pays_a[part], &b.pays_b[part]);
                        for i in 0..keys.len() {
                            let key = keys[i];
                            oa.add(hash128(key) as usize, key, pa[i] as u64, pb[i]);
                        }
                    }
                    (true, true) => {
                        let pays = &b.pays[part];
                        for i in 0..keys.len() {
                            let (key, pay) = (keys[i], pays[i]);
                            if oa.len >= cap_entries {
                                ps.drain(oa);
                            }
                            oa.add(
                                hash128(key) as usize,
                                key,
                                (pay >> 31) as u64,
                                (pay & 0xFFFF) as u64,
                            );
                        }
                    }
                    (false, true) => {
                        let (pa, pb) = (&b.pays_a[part], &b.pays_b[part]);
                        for i in 0..keys.len() {
                            let key = keys[i];
                            if oa.len >= cap_entries {
                                ps.drain(oa);
                            }
                            oa.add(hash128(key) as usize, key, pa[i] as u64, pb[i]);
                        }
                    }
                }
            }
            if ps.runs.is_empty() {
                for s in 0..=oa.mask {
                    if oa.keys[s] != EMPTY {
                        let cand = Row {
                            key: oa.keys[s],
                            c: oa.cnt[s],
                            sr: oa.srs[s],
                            sw: oa.sws[s],
                        };
                        if kk == usize::MAX {
                            rows.push(cand);
                        } else {
                            topk_consider(rows, kk, cand);
                        }
                    }
                }
            } else {
                merge_runs(oa, ps, rows, kk, share_bytes);
            }
        },
    );
    // [cap-retire] finalize answer-bytes law: the TRUE per-owner group
    // counts are in hand (counted, never estimated) — refuse typed
    // BEFORE flattening the answer plane.
    let groups_total: u64 = owned.iter().map(|(rows, _, _)| rows.len() as u64).sum();
    check_answer_budget(ctx, node, groups_total, std::mem::size_of::<Row>(), nullable);
    let mut all: Vec<Row> = Vec::new();
    {
        for (rows, oa, _ps) in owned {
            all.extend(rows);
            let b = oa.keys.capacity() * 16
                + oa.cnt.capacity() * 8
                + oa.srs.capacity() * 8
                + oa.sws.capacity() * 8;
            // [sqe-park-knobs] plain-data advisor: keys/cnt/srs/sws are
            // scalar buffers — lazy-releasable under OVERCAP=advise.
            PARKOA.park_with(oa, b, |oa| {
                use crate::stencils::statepark::lazyfree::advise_vec;
                advise_vec(&oa.keys);
                advise_vec(&oa.cnt);
                advise_vec(&oa.srs);
                advise_vec(&oa.sws);
            });
        }
    }
    drop(scattered);
    {
        use crate::stencils::statepark::nested_bytes;
        for s in pass1 {
            let item = (s.keys, s.pays, s.pays_a, s.pays_b);
            let b = nested_bytes(&item.0)
                + nested_bytes(&item.1)
                + nested_bytes(&item.2)
                + nested_bytes(&item.3);
            // [sqe-park-knobs] plain-data advisor: inner scalar buffers
            // only — the outer Vec headers are never advised.
            PARK32.park_with(item, b, |it| {
                use crate::stencils::statepark::lazyfree::advise_nested;
                advise_nested(&it.0);
                advise_nested(&it.1);
                advise_nested(&it.2);
                advise_nested(&it.3);
            });
        }
    }
    crate::engine::phn(node, "pass2", t_p2);

    // render: (count DESC, key ASC) canonical order; typed columns are the
    // group cols (sign-extended per width) then the agg outputs in order.
    let t_r = std::time::Instant::now();
    all.sort_by(|a, b| b.c.cmp(&a.c).then_with(|| a.key.cmp(&b.key)));
    if node.params.limit != usize::MAX {
        all.truncate(node.params.offset + node.params.limit);
    } else if kk != usize::MAX {
        all.truncate(kk);
    }
    let w0 = col_width(bank, g0);
    let w1 = g1.map(|c| col_width(bank, c)).unwrap_or(0);
    let window: Vec<&Row> = all.iter().skip(node.params.offset).collect();
    let mut cols_out: Vec<AnswerCol> = Vec::new();
    // Key emit: the null bit is the answer's validity leg (NULL group
    // keys render as SQL NULL); value bits un-pack per the elected
    // layout (null-free = the PoC pack, nullable = [n0][v0][n1][v1]).
    cols_out.push(AnswerCol::i64s_opt(
        node.ty_of(g0),
        window
            .iter()
            .map(|r| {
                if nullable {
                    (r.key >> n0_shift & 1 == 0).then(|| sx((r.key >> v0_shift) as u64, w0))
                } else {
                    Some(sx((r.key >> w1_bits) as u64, w0))
                }
            })
            .collect(),
    ));
    if let Some(g1c) = g1 {
        cols_out.push(AnswerCol::i64s_opt(
            node.ty_of(g1c),
            window
                .iter()
                .map(|r| {
                    if nullable && r.key >> n1_shift & 1 != 0 {
                        None
                    } else {
                        Some(sx((r.key & mask1) as u64, w1))
                    }
                })
                .collect(),
        ));
    }
    for a in &node.agg {
        match a.op {
            AggOp::CountStar => cols_out.push(AnswerCol::i64s(
                TypMeta::INT8,
                window.iter().map(|r| r.c as i64).collect(),
            )),
            AggOp::Sum => cols_out.push(AnswerCol::i128s(
                a.out,
                window.iter().map(|r| r.sr as i64 as i128).collect(),
            )),
            AggOp::Avg => cols_out.push(AnswerCol {
                ty: a.out,
                data: ColData::Ratio {
                    pairs: window.iter().map(|r| (r.sw as i128, r.c as i64)).collect(),
                    exact: false,
                },
                validity: Validity::AllValid,
            }),
            other => panic!("hash_plane: unsupported agg {other:?}"),
        }
    }
    crate::engine::phn(node, "render", t_r);
    AnswerSet::from_cols(cols_out)
}

// ---------------------------------------------------------------------------
// [sqe-grpfold] byval foundation, general fold route: per-group AccumCell
// blocks (fold.rs — the ONE scatter-fold law) in a Cells128 keyed on the
// SAME packed 1-2-column key layout as the fast paths above (null-free
// PoC pack / nullable [n0][v0][n1][v1]). Thread-local tables merged under
// the one combine law — correctness-first; the tuned scatter shapes keep
// the hot elections. 3VL: NULL fold inputs fold nothing; SUM/MIN/MAX over
// a group of only-NULL inputs answer NULL; AVG = sum / count(nonnull)
// rides the Ratio lane (count==0 renders NULL).
//
// Loop structure (R2: no interpretation in an execution path — fold-op
// dispatch at GRANULE-LANE grain, never row grain): each granule runs
// TWO passes. Pass A walks the rows once doing only key-pack +
// `tab.touch`, writing each row's slot into a per-worker scratch
// `Vec<u32>` (touches all happen here, so the table never grows under a
// live slot vector). Pass B walks the LANES: one `match l.op` per
// (granule, lane) selects a monomorphic closure over the constant op
// (`scatter_cell_fold` constant-folds), and `fold_lane` runs the tight
// single-op loop over the slot vector into `cells[slot*na + ai]`.
// ---------------------------------------------------------------------------

/// The monomorphic pass-B driver: one fold op, one lane, all rows of a
/// granule. `f` is the op-constant fold closure picked by the ONE match
/// per (granule, lane) at the call site.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn fold_lane(
    rows: usize,
    slots: &[u32],
    d: &[u64],
    face: crate::bank::Face,
    all_valid: bool,
    scr: &Scratch,
    cells: &mut [crate::fold::AccumCell],
    na: usize,
    ai: usize,
    fmask: Option<&[bool]>,
    f: impl Fn(&mut crate::fold::AccumCell, i64, bool),
) {
    match fmask {
        // The unfiltered loop stays the constant-folded PoC body
        // (law 11: the common case costs nothing).
        None => {
            for r in 0..rows {
                let ok = all_valid || scr.row_valid(r);
                let w = if ok { face.word_key(d[r]) } else { 0 };
                f(&mut cells[slots[r] as usize * na + ai], w, ok);
            }
        }
        // [aggqual] a FILTER-bearing lane: the fold input is
        // (word, valid && filter_pass) — the composed 3VL law.
        Some(m) => {
            for r in 0..rows {
                let ok = m[r] && (all_valid || scr.row_valid(r));
                let w = if ok { face.word_key(d[r]) } else { 0 };
                f(&mut cells[slots[r] as usize * na + ai], w, ok);
            }
        }
    }
}

fn foundation_cells(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::bank::Face;
    use crate::fold::{
        combine_cell_fold, fold_op_of, scatter_cell_fold, AggFoldOp,
    };
    use crate::grouped::Cells128;
    use crate::ir::FoldExpr;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let g0 = node.params.group_cols[0];
    let g1: Option<u32> = node.params.group_cols.get(1).copied();
    let w1_bits = g1.map(|c| 8 * col_width(bank, c) as u32).unwrap_or(0);
    let w0_bits = 8 * col_width(bank, g0) as u32;
    let nf0 = bank.null_free(g0);
    let nf1 = g1.map(|c| bank.null_free(c)).unwrap_or(true);
    let nullable = !(nf0 && nf1);
    let n1_shift = w1_bits;
    let v0_shift = if g1.is_some() { w1_bits + 1 } else { 0 };
    let n0_shift = v0_shift + w0_bits;
    assert!(
        w0_bits + w1_bits <= if nullable { 125 } else { 127 },
        "packed group key must leave the u128 sentinel (and nullmask lanes) unreachable"
    );
    let mask1: u128 = if w1_bits == 0 { 0 } else { (1u128 << w1_bits) - 1 };
    let mask0: u128 = if w0_bits >= 64 { u64::MAX as u128 } else { (1u128 << w0_bits) - 1 };

    // decode plan: group cols, then one lane per folding agg (CountStar
    // rides the group row count; planner witnessed word-embed faces).
    let nkeys = if g1.is_some() { 2 } else { 1 };
    let mut cols: Vec<u32> = match g1 {
        Some(g1) => vec![g0, g1],
        None => vec![g0],
    };
    struct FLane {
        ai: usize,
        ci: usize,
        op: AggFoldOp,
        face: Face,
        nf: bool,
        /// [aggqual] index into the statement's filter set (None = no
        /// FILTER on this leg).
        fi: Option<usize>,
        /// [tpch-expr] fused operand decode indices + form.
        ci2: usize,
        ci3: usize,
        ex: Option<FoldExpr>,
    }
    // [aggqual] the per-leg FILTER specs: (leg ai, terms). Term columns
    // join the decode set; each granule computes one survivor mask per
    // spec and the leg's lane folds through it. A FILTERED count(*)
    // cannot ride the touch count — it gets a CountCol lane whose 3VL
    // is the mask alone (render reads the cell, not the group count).
    let flt_specs: Vec<(usize, &[crate::ir::PredTerm])> = node
        .params
        .agg_filters
        .iter()
        .enumerate()
        .filter_map(|(ai, f)| f.as_ref().map(|p| (ai, p.terms.as_slice())))
        .collect();
    let flt_of = |ai: usize| flt_specs.iter().position(|&(i, _)| i == ai);
    let mut lanes: Vec<FLane> = Vec::new();
    for (ai, a) in node.agg.iter().enumerate() {
        let fi = flt_of(ai);
        match fold_op_of(a.op) {
            Some(op) => {
                let c = a.col.expect("planner: fold aggs carry an input column");
                let ci_of = |cols: &mut Vec<u32>, c: u32| {
                    cols.iter().position(|&x| x == c).unwrap_or_else(|| {
                        cols.push(c);
                        cols.len() - 1
                    })
                };
                let ci = ci_of(&mut cols, c);
                let ci2 = a.col2().map(|c2| ci_of(&mut cols, c2)).unwrap_or(usize::MAX);
                let ci3 = a.col3().map(|c3| ci_of(&mut cols, c3)).unwrap_or(usize::MAX);
                lanes.push(FLane {
                    ai,
                    ci,
                    op,
                    face: bank.face(c),
                    nf: bank.null_free(c),
                    fi,
                    ci2,
                    ci3,
                    ex: a.expr,
                });
            }
            None if a.op == AggOp::CountStar && fi.is_some() => {
                // count(*) FILTER: a mask-gated row-count lane over the
                // first key column's geometry (the word is never read;
                // nf=true hoists the lane's validity to the mask).
                lanes.push(FLane {
                    ai,
                    ci: 0,
                    op: AggFoldOp::CountCol,
                    face: bank.face(g0),
                    nf: true,
                    fi,
                    ci2: usize::MAX,
                    ci3: usize::MAX,
                    ex: None,
                });
            }
            None => continue,
        }
    }
    // Filter term columns join the decode set (per-term ci, aligned
    // with flt_specs).
    let flt_cis: Vec<Vec<usize>> = flt_specs
        .iter()
        .map(|(_, ts)| {
            ts.iter()
                .map(|t| {
                    cols.iter().position(|&x| x == t.col).unwrap_or_else(|| {
                        cols.push(t.col);
                        cols.len() - 1
                    })
                })
                .collect()
        })
        .collect();
    // [tpch-expr] statement pred: survivors gate group formation.
    let pred_terms: &[crate::ir::PredTerm] =
        node.pred.as_ref().map(|p| p.terms.as_slice()).unwrap_or(&[]);
    debug_assert!(
        node.pred
            .as_ref()
            .map(|p| p.var_terms.is_empty() && p.col_terms.is_empty())
            .unwrap_or(true),
        "cells arm admits word conjuncts only"
    );
    let pred_cis: Vec<usize> = pred_terms
        .iter()
        .map(|t| {
            cols.iter().position(|&x| x == t.col).unwrap_or_else(|| {
                cols.push(t.col);
                cols.len() - 1
            })
        })
        .collect();
    let has_pred = !pred_terms.is_empty();
    let col_nf: Vec<bool> = cols.iter().map(|&c| bank.null_free(c)).collect();
    let ncols = cols.len();
    let na = node.agg.len();
    let lanesr = &lanes;
    let colsr = &cols;
    let flt_specsr = &flt_specs;
    let flt_cisr = &flt_cis;
    let col_nfr = &col_nf;
    let pred_cisr = &pred_cis;

    let ndv = ctx.faces.stats(bank, g0).ndv_est_sum() as usize;
    let units = ctx.faces.walk(bank, g0);
    let t = pool.threads().max(1);
    let expect = ndv.max(16) / t + 16;

    struct S {
        scr: Vec<Scratch>,
        cc: Vec<CurCache>,
        tab: Cells128,
        /// Pass-A scratch: row -> group slot (valid for one granule).
        slots: Vec<u32>,
        /// [tpch-expr] statement-pred survivors (valid for one granule).
        live: Vec<bool>,
        /// [aggqual] per-filter survivor masks + the term-major
        /// selection scratch (valid for one granule).
        fmasks: Vec<Vec<bool>>,
        msel: Vec<u16>,
    }
    let pass1 = pool.run_finish(
        units.len(),
        |_| S {
            scr: (0..ncols).map(|_| crate::scan::scratch_fetch()).collect(),
            cc: colsr.iter().map(|&a| CurCache::new(a)).collect(),
            tab: Cells128::new(expect, na),
            slots: Vec::new(),
            live: Vec::new(),
            fmasks: flt_specsr.iter().map(|_| Vec::new()).collect(),
            msel: Vec::new(),
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let mut gvs: Vec<crate::scan::GranValid> = Vec::with_capacity(ncols);
            let mut ds: Vec<&[u64]> = Vec::with_capacity(ncols);
            for (ci, (scr, cc)) in s.scr.iter_mut().zip(s.cc.iter_mut()).enumerate() {
                let cur = cc.get(bank, pi);
                let need_v = (ci == 0 && !nf0)
                    || (ci == 1 && ci < nkeys && !nf1)
                    || lanesr.iter().any(|l| l.ci == ci && !l.nf)
                    || (!col_nfr[ci]
                        && (lanesr.iter().any(|l| l.ci2 == ci || l.ci3 == ci)
                            || pred_cisr.contains(&ci)
                            || flt_cisr.iter().any(|cis| cis.contains(&ci))));
                gvs.push(if need_v {
                    scr.validity(cur, g, rows)
                } else {
                    crate::scan::GranValid::AllValid
                });
                let d = scr.decode_full(cur, g, rows);
                // each slice lives in its own scratch until the next granule
                ds.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
            }
            if has_pred {
                debug_assert!(rows <= 1 << 16, "granule exceeds the u16 selection law");
                let S { scr, live, msel, .. } = s;
                msel.clear();
                msel.extend((0..rows).map(|r| r as u16));
                for (ti, t) in pred_terms.iter().enumerate() {
                    let ci = pred_cisr[ti];
                    let d = ds[ci];
                    let face = bank.face(colsr[ci]);
                    if col_nfr[ci] || gvs[ci].all_valid() {
                        t.filter_sel(msel, |_| true, |r| face.word_key(d[r]));
                    } else {
                        let sc = &scr[ci];
                        t.filter_sel(msel, |r| sc.row_valid(r), |r| face.word_key(d[r]));
                    }
                }
                if live.len() < rows {
                    live.resize(rows, false);
                }
                live[..rows].fill(false);
                for &r16 in msel.iter() {
                    live[r16 as usize] = true;
                }
            }
            // Pass A: key-pack + touch ONLY — row -> slot scratch.
            // reserve_batch first: growth REHASHES, so the whole
            // granule's touches must fit without growing or the slots
            // recorded earlier in the batch would go stale.
            s.tab.reserve_batch(rows);
            if s.slots.len() < rows {
                s.slots.resize(rows, 0);
            }
            for r in 0..rows {
                if has_pred && !s.live[r] {
                    continue;
                }
                let key: u128 = if !nullable {
                    if w1_bits == 0 {
                        ds[0][r] as u128
                    } else {
                        ((ds[0][r] as u128) << w1_bits) | (ds[1][r] as u128 & mask1)
                    }
                } else {
                    let ok0 = nf0 || gvs[0].all_valid() || s.scr[0].row_valid(r);
                    let ok1 =
                        g1.is_none() || nf1 || gvs[1].all_valid() || s.scr[1].row_valid(r);
                    let mut key = 0u128;
                    if ok0 {
                        key |= (ds[0][r] as u128 & mask0) << v0_shift;
                    } else {
                        key |= 1u128 << n0_shift;
                    }
                    if g1.is_some() {
                        if ok1 {
                            key |= ds[1][r] as u128 & mask1;
                        } else {
                            key |= 1u128 << n1_shift;
                        }
                    }
                    key
                };
                s.slots[r] = s.tab.touch(key, 1) as u32;
            }
            // [aggqual] per-filter survivor masks: seed all rows, run
            // the spec's conjuncts term-major (filter_sel — the same
            // de-interpretation law as the statement predicate), then
            // write the surviving rows into the granule's mask.
            {
                let S { scr, fmasks, msel, live, .. } = s;
                for (fi, (_, fterms)) in flt_specsr.iter().enumerate() {
                    debug_assert!(rows <= 1 << 16, "granule exceeds the u16 selection law");
                    msel.clear();
                    msel.extend((0..rows).map(|r| r as u16));
                    for (ti, t) in fterms.iter().enumerate() {
                        let ci = flt_cisr[fi][ti];
                        let d = ds[ci];
                        let face = bank.face(colsr[ci]);
                        let allv = col_nfr[ci] || gvs[ci].all_valid();
                        if allv {
                            t.filter_sel(msel, |_| true, |r| face.word_key(d[r]));
                        } else {
                            let sc = &scr[ci];
                            t.filter_sel(msel, |r| sc.row_valid(r), |r| face.word_key(d[r]));
                        }
                    }
                    let m = &mut fmasks[fi];
                    if m.len() < rows {
                        m.resize(rows, false);
                    }
                    m[..rows].fill(false);
                    for &r16 in msel.iter() {
                        m[r16 as usize] = true;
                    }
                    if has_pred {
                        for r in 0..rows {
                            m[r] = m[r] && live[r];
                        }
                    }
                }
            }
            // Pass B: one op dispatch per (granule, lane); monomorphic
            // single-op loops over the slot vector (loop inversion).
            let S { scr, tab, slots, fmasks, live, .. } = s;
            for l in lanesr {
                let all_valid = l.nf || gvs[l.ci].all_valid();
                let (d, sc, cells) = (ds[l.ci], &scr[l.ci], &mut tab.cells[..]);
                // [tpch-expr] dead rows carry no slot: fold via masks.
                let fm: Option<&[bool]> = match (l.fi, has_pred) {
                    (Some(fi), _) => Some(&fmasks[fi][..rows]),
                    (None, true) => Some(&live[..rows]),
                    (None, false) => None,
                };
                if let Some(e) = l.ex {
                    // fused lanes: one loop per shape, strict 3VL.
                    let cb = l.ci2;
                    let db = ds[cb];
                    let fb = bank.face(colsr[cb]);
                    let b_allv = col_nfr[cb] || gvs[cb].all_valid();
                    match e {
                        FoldExpr::MulCC { .. }
                        | FoldExpr::MulKSub { .. }
                        | FoldExpr::PackedMulK { .. } => {
                            let (k, sgn) = match e {
                                FoldExpr::MulCC { .. } => (0i64, 1i64),
                                FoldExpr::MulKSub { k, .. } => (k, -1),
                                FoldExpr::PackedMulK { k, sub, .. } => {
                                    (k, if sub { -1 } else { 1 })
                                }
                                FoldExpr::PackedMulK2 { .. } => unreachable!(),
                            };
                            for r in 0..rows {
                                if let Some(m) = fm {
                                    if !m[r] {
                                        continue;
                                    }
                                }
                                let ok = (all_valid || sc.row_valid(r))
                                    && (b_allv || scr[cb].row_valid(r));
                                if !ok {
                                    continue;
                                }
                                let w = l.face.word_key(d[r])
                                    * (k + sgn * fb.word_key(db[r]));
                                scatter_cell_fold(
                                    l.op,
                                    &mut cells[slots[r] as usize * na + l.ai],
                                    w,
                                    true,
                                );
                            }
                        }
                        FoldExpr::PackedMulK2 { k1, sub1, k2, sub2, .. } => {
                            let c3 = l.ci3;
                            let dc = ds[c3];
                            let fc = bank.face(colsr[c3]);
                            let c_allv = col_nfr[c3] || gvs[c3].all_valid();
                            let s1 = if sub1 { -1i64 } else { 1i64 };
                            let s2 = if sub2 { -1i64 } else { 1i64 };
                            for r in 0..rows {
                                if let Some(m) = fm {
                                    if !m[r] {
                                        continue;
                                    }
                                }
                                let ok = (all_valid || sc.row_valid(r))
                                    && (b_allv || scr[cb].row_valid(r))
                                    && (c_allv || scr[c3].row_valid(r));
                                if !ok {
                                    continue;
                                }
                                let w = l.face.word_key(d[r])
                                    * (k1 + s1 * fb.word_key(db[r]))
                                    * (k2 + s2 * fc.word_key(dc[r]));
                                scatter_cell_fold(
                                    l.op,
                                    &mut cells[slots[r] as usize * na + l.ai],
                                    w,
                                    true,
                                );
                            }
                        }
                    }
                    continue;
                }
                match l.op {
                    AggFoldOp::CountCol => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::CountCol, c, w, ok),
                    ),
                    AggFoldOp::Sum => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::Sum, c, w, ok),
                    ),
                    AggFoldOp::Min => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::Min, c, w, ok),
                    ),
                    AggFoldOp::Max => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::Max, c, w, ok),
                    ),
                    AggFoldOp::SumSq => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::SumSq, c, w, ok),
                    ),
                    AggFoldOp::BitAnd => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::BitAnd, c, w, ok),
                    ),
                    AggFoldOp::BitOr => fold_lane(
                        rows, slots, d, l.face, all_valid, sc, cells, na, l.ai, fm,
                        |c, w, ok| scatter_cell_fold(AggFoldOp::BitOr, c, w, ok),
                    ),
                }
            }
        },
        |s| {
            s.scr.into_iter().for_each(crate::scan::scratch_park);
            s.tab
        },
    );

    // merge: absorb into the biggest table under the one combine law.
    let mut tabs: Vec<Cells128> = pass1;
    let mut global = match tabs.iter().enumerate().max_by_key(|(_, tb)| tb.len) {
        Some((i, _)) => tabs.swap_remove(i),
        None => Cells128::new(16, na),
    };
    for tb in tabs {
        tb.for_each(|k, rows, cells| {
            let slot = global.touch(k, rows);
            // touch counted `rows` in; cells merge per fold op.
            let base = slot * na;
            // [aggqual] every LANE merges — incl. the filtered count(*)
            // lane fold_op_of cannot see (its op lives on the lane, not
            // the agg).
            for l in lanesr {
                combine_cell_fold(l.op, &mut global.cells[base + l.ai], &cells[l.ai]);
            }
        });
    }

    // render: shared answer tail (fused-HAVING filter, (count DESC, key
    // ASC) order, offset/limit window, the typed key + cell renders).
    let na2 = node.agg.len();
    let mut all: Vec<CRow> = Vec::with_capacity(global.len);
    let mut arena: Vec<crate::fold::AccumCell> = Vec::with_capacity(global.len * na2);
    global.for_each(|k, c, cells| {
        all.push(CRow { key: k, c, at: all.len() as u32 });
        arena.extend_from_slice(cells);
    });
    let lay = CrowLayout { g1, nullable, n0_shift, v0_shift, n1_shift, w1_bits, mask1 };
    crow_answer(bank, node, all, &arena, &lay)
}

/// One rendered group of the packed-byval-key arms: the packed key, the
/// group row count, and the group's index into the flat cell ARENA
/// (`na` cells per group — flat so million-group answers carry no
/// per-group heap block and the render sort moves 32-byte rows).
struct CRow {
    key: u128,
    c: u64,
    at: u32,
}

/// Key layout facts the packed render needs (null-free single keys pass
/// zeros everywhere — the pack degenerates to the raw datum).
struct CrowLayout {
    g1: Option<u32>,
    nullable: bool,
    n0_shift: u32,
    v0_shift: u32,
    n1_shift: u32,
    w1_bits: u32,
    mask1: u128,
}

/// [sqe-tpch-mech] The ONE fused-HAVING group filter for the packed
/// byval-key arms: the aggregate's ANSWER value per group, straight from
/// the fold cells (NULL aggregate = drop — HavingCmp::keep's 3VL law).
fn having_keep(
    node: &PlanNode,
    h: &crate::ir::HavingCmp,
    c: u64,
    cells: &[crate::fold::AccumCell],
) -> bool {
    let ai = h.agg as usize;
    let v: Option<i128> = match node.agg[ai].op {
        AggOp::CountStar => Some(c as i128),
        AggOp::Sum => (cells[ai].b > 0).then(|| cells[ai].a),
        AggOp::Min | AggOp::Max => {
            crate::fold::minmax_answer(&cells[ai]).map(|x| x as i128)
        }
        other => panic!("having over unsupported agg {other:?} (admission gap)"),
    };
    h.keep(v)
}

/// Shared answer tail of the packed-byval-key cell arms (hash foundation
/// AND the direct-array route): fused-HAVING filter, (count DESC, key
/// ASC) canonical order, offset/limit window, key emit identical to the
/// fast paths' (null bit -> validity leg). `arena` holds `na` cells per
/// group at `CRow.at * na`.
fn crow_answer(
    bank: &crate::bank::Bank,
    node: &PlanNode,
    mut all: Vec<CRow>,
    arena: &[crate::fold::AccumCell],
    lay: &CrowLayout,
) -> AnswerSet {
    use crate::fold::minmax_answer;
    let na = node.agg.len();
    let cells_of = |r: &CRow| &arena[r.at as usize * na..(r.at as usize + 1) * na];
    if let Some(h) = &node.params.having {
        all.retain(|r| having_keep(node, h, r.c, cells_of(r)));
    }
    // A NATIVE pushed bound (the seam's down-pass) rides the same
    // (count DESC, key ASC) total order the render sorts by: an O(G)
    // selection bounds the set BEFORE the full sort — the render then
    // touches n rows, never the whole group set (group filter above
    // already applied, so the bound windows the surviving groups).
    let cmp = |a: &CRow, b: &CRow| b.c.cmp(&a.c).then_with(|| a.key.cmp(&b.key));
    if let Some(t) = &node.params.topk {
        if t.native && t.n < all.len() {
            if t.n == 0 {
                all.clear();
            } else {
                all.select_nth_unstable_by(t.n - 1, cmp);
                all.truncate(t.n);
            }
        }
    }
    all.sort_by(cmp);
    if node.params.limit != usize::MAX {
        all.truncate(node.params.offset + node.params.limit);
    }
    let g0 = node.params.group_cols[0];
    let w0 = col_width(bank, g0);
    let w1 = lay.g1.map(|c| col_width(bank, c)).unwrap_or(0);
    let window: Vec<&CRow> = all.iter().skip(node.params.offset).collect();
    let mut cols_out: Vec<AnswerCol> = Vec::new();
    cols_out.push(AnswerCol::i64s_opt(
        node.ty_of(g0),
        window
            .iter()
            .map(|r| {
                if lay.nullable {
                    (r.key >> lay.n0_shift & 1 == 0)
                        .then(|| sx((r.key >> lay.v0_shift) as u64, w0))
                } else {
                    Some(sx((r.key >> lay.w1_bits) as u64, w0))
                }
            })
            .collect(),
    ));
    if let Some(g1c) = lay.g1 {
        cols_out.push(AnswerCol::i64s_opt(
            node.ty_of(g1c),
            window
                .iter()
                .map(|r| {
                    if lay.nullable && r.key >> lay.n1_shift & 1 != 0 {
                        None
                    } else {
                        Some(sx((r.key & lay.mask1) as u64, w1))
                    }
                })
                .collect(),
        ));
    }
    for (ai, a) in node.agg.iter().enumerate() {
        // [aggqual] a FILTERED count(*) leg reads ITS lane's cell count
        // (mask-gated CountCol) — never the group row count.
        let count_filtered = node
            .params
            .agg_filters
            .get(ai)
            .map(|f| f.is_some())
            .unwrap_or(false);
        cols_out.push(match a.op {
            AggOp::CountStar if count_filtered => AnswerCol::i64s(
                TypMeta::INT8,
                window.iter().map(|r| cells_of(r)[ai].b).collect(),
            ),
            AggOp::CountStar => AnswerCol::i64s(
                TypMeta::INT8,
                window.iter().map(|r| r.c as i64).collect(),
            ),
            AggOp::Sum => {
                let mask: Vec<bool> = window.iter().map(|r| cells_of(r)[ai].b > 0).collect();
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                AnswerCol {
                    ty: a.out,
                    data: ColData::I128(window.iter().map(|r| cells_of(r)[ai].a).collect()),
                    validity,
                }
            }
            AggOp::Avg => AnswerCol::ratios(
                a.out,
                window.iter().map(|r| (cells_of(r)[ai].a, cells_of(r)[ai].b)).collect(),
                a.avg_exact(),
            ),
            AggOp::Min | AggOp::Max | AggOp::BitAnd | AggOp::BitOr => AnswerCol::i64s_opt(
                a.out,
                window.iter().map(|r| minmax_answer(&cells_of(r)[ai])).collect(),
            ),
            AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
                let kind = crate::answer::MomentKind::of_op(a.op).expect("moment op");
                AnswerCol::moments(
                    a.out,
                    kind,
                    window
                        .iter()
                        .map(|r| (cells_of(r)[ai].b, cells_of(r)[ai].a, cells_of(r)[ai].a2))
                        .collect(),
                )
            }
            other => panic!("foundation_cells: unsupported agg {other:?}"),
        });
    }
    AnswerSet::from_cols(cols_out)
}

/// [sqe-tpch-mech] Direct-array grouped state (mechanism 1, single
/// relation): the group key's domain is witnessed dense-and-bounded, so
/// grouped state is ONE shared array indexed `key - lo` — a u64 count
/// lane plus (value, non-null count) atomic lanes per fold aggregate —
/// scattered with relaxed atomics (the tpch-floor Q18 idiom: constant-
/// time scatter, no hashing, no growth, no merge). The fold math is the
/// fold.rs law re-expressed on atomic lanes (Sum = fetch_add under the
/// planner's i64 overflow witness; Min/Max = fetch_min/fetch_max with
/// b > 0 as the Option-ness); the answer tail is the SAME `crow_answer`
/// as the hash foundation — byte-identical answers by construction.
fn dense_direct(ctx: &SqeCtx, node: &PlanNode, lo: i64, dn: usize) -> AnswerSet {
    use crate::bank::Face;
    use crate::fold::{fold_op_of, AccumCell, AggFoldOp};
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
    let (bank, pool) = (ctx.bank, ctx.pool);
    let g0 = node.params.group_cols[0];
    let kface = bank.face(g0);
    let w0 = col_width(bank, g0);
    let wmask: u64 = if w0 >= 8 { u64::MAX } else { (1u64 << (8 * w0 as u32)) - 1 };

    struct DLane {
        ai: usize,
        ci: usize,
        op: AggFoldOp,
        face: Face,
        nf: bool,
    }
    let mut cols: Vec<u32> = vec![g0];
    let mut lanes: Vec<DLane> = Vec::new();
    for (ai, a) in node.agg.iter().enumerate() {
        let Some(op) = fold_op_of(a.op) else { continue };
        assert!(
            matches!(op, AggFoldOp::Sum | AggFoldOp::Min | AggFoldOp::Max),
            "dense_direct: lane vocabulary gap (election bug)"
        );
        let c = a.col.expect("fold aggs carry an input column");
        let ci = cols.iter().position(|&x| x == c).unwrap_or_else(|| {
            cols.push(c);
            cols.len() - 1
        });
        lanes.push(DLane { ai, ci, op, face: bank.face(c), nf: bank.null_free(c) });
    }
    let ncols = cols.len();
    let na = node.agg.len();
    let lanesr = &lanes;
    let colsr = &cols;

    // the shared array: count lane + (value, non-null count) per lane.
    // Zero lanes ride alloc_zeroed pages (untouched until a group lands);
    // only Min/Max value lanes pay a real sentinel fill.
    use crate::grouped::{atomic_fill_i64, atomic_zeros_u64};
    let counts: Vec<AtomicU64> = atomic_zeros_u64(dn);
    struct ALane {
        vals: Vec<AtomicI64>,
        bs: Vec<AtomicU64>,
    }
    let alanes: Vec<ALane> = lanes
        .iter()
        .map(|l| {
            let init = match l.op {
                AggFoldOp::Min => i64::MAX,
                AggFoldOp::Max => i64::MIN,
                _ => 0,
            };
            ALane { vals: atomic_fill_i64(dn, init), bs: atomic_zeros_u64(dn) }
        })
        .collect();
    let (countsr, alanesr) = (&counts, &alanes);

    // [g16] E17 private-fold election: at tiny domains the shared
    // atomic array puts every worker's per-row RMWs on the same few
    // cache lines (the grouped-by-16 ledger anomaly: 619ms of pass1
    // where lanev2 serves in 7ms). When the per-worker slot footprint
    // fits the L2 bound (cost_params::dense_private_fit), each worker
    // folds into PRIVATE non-atomic lanes and merges once into the
    // shared array at generation finish — the same modular-integer
    // fold (wrapping add / min / max are associative and order-free),
    // so answers are byte-identical by construction.
    let private = crate::cost_params::target().dense_private_fit(dn, lanes.len());

    let units = ctx.faces.walk(bank, g0);
    let t_p1 = std::time::Instant::now();
    struct S {
        scr: Vec<Scratch>,
        cc: Vec<CurCache>,
        slots: Vec<u32>,
        // private per-worker lanes (empty on the shared-atomic path)
        pcnt: Vec<u64>,
        pvals: Vec<Vec<i64>>,
        pbs: Vec<Vec<u64>>,
    }
    pool.run_finish(
        units.len(),
        |_| S {
            scr: (0..ncols).map(|_| crate::scan::scratch_fetch()).collect(),
            cc: colsr.iter().map(|&a| CurCache::new(a)).collect(),
            slots: Vec::new(),
            pcnt: if private { vec![0; dn] } else { Vec::new() },
            pvals: if private {
                lanesr
                    .iter()
                    .map(|l| {
                        let init = match l.op {
                            AggFoldOp::Min => i64::MAX,
                            AggFoldOp::Max => i64::MIN,
                            _ => 0,
                        };
                        vec![init; dn]
                    })
                    .collect()
            } else {
                Vec::new()
            },
            pbs: if private { lanesr.iter().map(|_| vec![0u64; dn]).collect() } else { Vec::new() },
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let mut gvs: Vec<crate::scan::GranValid> = Vec::with_capacity(ncols);
            let mut ds: Vec<&[u64]> = Vec::with_capacity(ncols);
            for (ci, (scr, cc)) in s.scr.iter_mut().zip(s.cc.iter_mut()).enumerate() {
                let cur = cc.get(bank, pi);
                let need_v = lanesr.iter().any(|l| l.ci == ci && !l.nf);
                gvs.push(if need_v {
                    scr.validity(cur, g, rows)
                } else {
                    crate::scan::GranValid::AllValid
                });
                let d = scr.decode_full(cur, g, rows);
                ds.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
            }
            // pass A: key -> slot (the witness bounds every key; an
            // out-of-domain key is a LOUD panic, never a wrong slot).
            if s.slots.len() < rows {
                s.slots.resize(rows, 0);
            }
            let dk = ds[0];
            if private {
                for r in 0..rows {
                    let idx = kface.word_key(dk[r]).wrapping_sub(lo) as usize;
                    assert!(idx < dn, "dense_direct: key outside the witnessed domain");
                    s.slots[r] = idx as u32;
                    s.pcnt[idx] += 1;
                }
            } else {
                for r in 0..rows {
                    let idx = kface.word_key(dk[r]).wrapping_sub(lo) as usize;
                    assert!(idx < dn, "dense_direct: key outside the witnessed domain");
                    s.slots[r] = idx as u32;
                    countsr[idx].fetch_add(1, Relaxed);
                }
            }
            // pass B: one op dispatch per (granule, lane); monomorphic
            // single-op loops over the slot vector (the fold_lane idiom
            // on atomic lanes).
            for (li, l) in lanesr.iter().enumerate() {
                let all_valid = l.nf || gvs[l.ci].all_valid();
                let (d, sc) = (ds[l.ci], &s.scr[l.ci]);
                let slots = &s.slots[..rows];
                if private {
                    // private non-atomic twin of the atomic fold below:
                    // wrapping add matches fetch_add's modular law.
                    let (pv, pb) = (&mut s.pvals[li], &mut s.pbs[li]);
                    match l.op {
                        AggFoldOp::Sum => {
                            for (r, &slot) in slots.iter().enumerate() {
                                if all_valid || sc.row_valid(r) {
                                    let v = &mut pv[slot as usize];
                                    *v = v.wrapping_add(l.face.word_key(d[r]));
                                    pb[slot as usize] += 1;
                                }
                            }
                        }
                        AggFoldOp::Min => {
                            for (r, &slot) in slots.iter().enumerate() {
                                if all_valid || sc.row_valid(r) {
                                    let v = &mut pv[slot as usize];
                                    *v = (*v).min(l.face.word_key(d[r]));
                                    pb[slot as usize] += 1;
                                }
                            }
                        }
                        AggFoldOp::Max => {
                            for (r, &slot) in slots.iter().enumerate() {
                                if all_valid || sc.row_valid(r) {
                                    let v = &mut pv[slot as usize];
                                    *v = (*v).max(l.face.word_key(d[r]));
                                    pb[slot as usize] += 1;
                                }
                            }
                        }
                        _ => unreachable!("dense_direct lane vocabulary"),
                    }
                    continue;
                }
                let al = &alanesr[li];
                match l.op {
                    AggFoldOp::Sum => {
                        for (r, &slot) in slots.iter().enumerate() {
                            if all_valid || sc.row_valid(r) {
                                al.vals[slot as usize].fetch_add(l.face.word_key(d[r]), Relaxed);
                                al.bs[slot as usize].fetch_add(1, Relaxed);
                            }
                        }
                    }
                    AggFoldOp::Min => {
                        for (r, &slot) in slots.iter().enumerate() {
                            if all_valid || sc.row_valid(r) {
                                al.vals[slot as usize].fetch_min(l.face.word_key(d[r]), Relaxed);
                                al.bs[slot as usize].fetch_add(1, Relaxed);
                            }
                        }
                    }
                    AggFoldOp::Max => {
                        for (r, &slot) in slots.iter().enumerate() {
                            if all_valid || sc.row_valid(r) {
                                al.vals[slot as usize].fetch_max(l.face.word_key(d[r]), Relaxed);
                                al.bs[slot as usize].fetch_add(1, Relaxed);
                            }
                        }
                    }
                    _ => unreachable!("dense_direct lane vocabulary"),
                }
            }
        },
        |s| {
            // [g16] one merge per worker: the private lanes land in the
            // shared atomic array here (worker thread, after its claim
            // loop) — dn·(1+2·lanes) RMWs per WORKER, not per row.
            if private {
                for (i, &c) in s.pcnt.iter().enumerate() {
                    if c != 0 {
                        countsr[i].fetch_add(c, Relaxed);
                    }
                }
                for (li, l) in lanesr.iter().enumerate() {
                    let al = &alanesr[li];
                    for i in 0..dn {
                        let b = s.pbs[li][i];
                        if b == 0 {
                            continue;
                        }
                        al.bs[i].fetch_add(b, Relaxed);
                        match l.op {
                            AggFoldOp::Sum => {
                                al.vals[i].fetch_add(s.pvals[li][i], Relaxed);
                            }
                            AggFoldOp::Min => {
                                al.vals[i].fetch_min(s.pvals[li][i], Relaxed);
                            }
                            AggFoldOp::Max => {
                                al.vals[i].fetch_max(s.pvals[li][i], Relaxed);
                            }
                            _ => unreachable!("dense_direct lane vocabulary"),
                        }
                    }
                }
            }
            s.scr.into_iter().for_each(crate::scan::scratch_park)
        },
    );
    crate::engine::phn(node, "pass1", t_p1);

    // gather (pool-parallel over contiguous slot ranges): touched slots
    // -> CRows carrying the SAME cell semantics as the scatter-fold law
    // (Sum: a=Σ, b=n; Min/Max: a=value, valid=b>0). A fused HAVING
    // filters HERE — non-surviving groups never materialize.
    let t_g = std::time::Instant::now();
    // [cap-retire] finalize answer-bytes law, UNBOUNDED answers: count
    // the TRUE occupancy exactly (every occupied slot emits) and refuse
    // typed BEFORE the gather plane materializes. Bounded answers keep
    // their standing laws: a fused HAVING emits only survivors — its
    // exact survivor count is priced after the gather (below); a pushed
    // top-k emits O(k) and its gather plane is the E17b occupancy-priced
    // state plane (pre-ruling servability is never removed).
    let stage_bytes =
        std::mem::size_of::<CRow>() + na * std::mem::size_of::<crate::fold::AccumCell>();
    let unbounded_answer = node.params.having.is_none()
        && node.params.limit == usize::MAX
        && node.params.topk.is_none();
    if unbounded_answer {
        let groups: u64 = counts.iter().map(|c| (c.load(Relaxed) != 0) as u64).sum();
        check_answer_budget(ctx, node, groups, stage_bytes, false);
    }
    let nch = (pool.threads().max(1) * 8).min(dn.max(1));
    let chunk = dn.div_ceil(nch);
    type GPart = Vec<(usize, Vec<CRow>, Vec<crate::fold::AccumCell>, u64)>;
    let parts: Vec<GPart> = pool.run(
        nch,
        |_| GPart::new(),
        |acc: &mut GPart, ci| {
            let (s0, s1) = (ci * chunk, ((ci + 1) * chunk).min(dn));
            let mut rows: Vec<CRow> = Vec::new();
            let mut arena: Vec<AccumCell> = Vec::new();
            let mut covered = 0u64;
            let mut cells = vec![AccumCell::default(); na];
            for i in s0..s1 {
                let c = countsr[i].load(Relaxed);
                if c == 0 {
                    continue;
                }
                covered += c;
                for (li, l) in lanesr.iter().enumerate() {
                    let b = alanesr[li].bs[i].load(Relaxed) as i64;
                    let v = alanesr[li].vals[i].load(Relaxed);
                    cells[l.ai] = match l.op {
                        AggFoldOp::Sum => AccumCell { a: v as i128, a2: 0, b, valid: 0 },
                        AggFoldOp::Min | AggFoldOp::Max => AccumCell {
                            a: if b > 0 { v as i128 } else { 0 },
                            a2: 0,
                            b,
                            valid: (b > 0) as u8,
                        },
                        _ => unreachable!("dense_direct lane vocabulary"),
                    };
                }
                if let Some(h) = &node.params.having {
                    if !having_keep(node, h, c, &cells) {
                        continue;
                    }
                }
                let raw = ((lo.wrapping_add(i as i64)) as u64) & wmask;
                rows.push(CRow { key: raw as u128, c, at: rows.len() as u32 });
                arena.extend_from_slice(&cells);
            }
            acc.push((ci, rows, arena, covered));
        },
    );
    let mut flat: Vec<(usize, Vec<CRow>, Vec<AccumCell>, u64)> =
        parts.into_iter().flatten().collect();
    flat.sort_unstable_by_key(|p| p.0);
    // [cap-retire] fused-HAVING answers price on the exact survivor
    // count, now in hand (the unbounded case refused before the gather).
    if node.params.having.is_some() {
        let groups: u64 = flat.iter().map(|p| p.1.len() as u64).sum();
        check_answer_budget(ctx, node, groups, stage_bytes, false);
    }
    let mut all: Vec<CRow> = Vec::new();
    let mut arena: Vec<AccumCell> = Vec::new();
    let mut covered = 0u64;
    for (_, rows, ar, cov) in flat {
        let base = (arena.len() / na.max(1)) as u32;
        all.extend(rows.into_iter().map(|mut r| {
            r.at += base;
            r
        }));
        arena.extend(ar);
        covered += cov;
    }
    assert_eq!(covered, bank.rows_total(), "q{}: dense counts must cover the bank", node.q);
    crate::engine::phn(node, "gather", t_g);
    let lay = CrowLayout {
        g1: None,
        nullable: false,
        n0_shift: 0,
        v0_shift: 0,
        n1_shift: 0,
        w1_bits: 0,
        mask1: 0,
    };
    crow_answer(bank, node, all, &arena, &lay)
}

// ---------------------------------------------------------------------------
// [famB M1] hot-shape shape: frame walk + composite (ints, CASE-gated text,
// final text) key, PARTITION-OWNED u128 hash-plane fold. Ported from
// kernels_f6::opt_q39_global_owned_par (branch -r3/-hot-shape; the fold is
// byte-identical between the _par and _pool twins) via
// fam_frame::group_owned_u128. Key layout (the ron key_pack law):
// ints 16b each from bit 112 down | gated src_slot (40 bits, 0 = the
// CASE '' arm, else src gcode+1) at bit 40 | final dst gcode low.
// Grouping never leaves the integer domain; non-dict rows late-remap
// bytes -> gcode with a byte-keyed side map for misses (provably
// disjoint from every remapped key). Render: (count DESC, packed-key
// bytes ASC) rank window + the `-- groups= rows=` trailer, byte-
// identical to kernels_f6::q39_pack / its fmt_key.
// ---------------------------------------------------------------------------

use crate::grouped::{hash128 as ghash128, Cnt128};
use crate::kernels_f6::FxHasher;
use crate::scan::varlena_payload;
use crate::stencils::two_level::{frame_granules, FG};
use std::collections::HashMap;
type Fx = std::hash::BuildHasherDefault<FxHasher>;
type FxBytesMap = HashMap<Vec<u8>, u64, Fx>;

const NOSRC: u128 = 0;

fn q39_style_pack(ints: &[i16], src: &[u8], dst: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 * ints.len() + 4 + src.len() + dst.len());
    for &v in ints {
        k.extend_from_slice(&v.to_le_bytes());
    }
    k.extend_from_slice(&(src.len() as u32).to_le_bytes());
    k.extend_from_slice(src);
    k.extend_from_slice(dst);
    k
}

#[allow(unused_assignments)]
fn frame_owned_u128(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::ir::KeyExpr;
    use crate::stencils::two_level::{final_granules, publish_final};
    let (bank, pool) = (ctx.bank, ctx.pool);
    let pred = node.pred.as_ref().unwrap();

    // [sqe-m2] Warm final plane (frame + residues applied): hot reps skip
    // the residue decode+eval entirely; cold records + publishes it.
    let t_frame = std::time::Instant::now();
    let (granules, res_applied): (std::sync::Arc<Vec<FG>>, bool) = match final_granules(ctx, node, pred) {
        Some(g) => (g, true),
        None => (frame_granules(ctx, node, pred), false),
    };
    let res_terms: &[PredTerm] = if res_applied { &[] } else { pred.residues() };
    let record_final = !res_applied
        && !pred.residues().is_empty()
        && node.params.goal.fingerprints.iter().any(|f| *f == pred.full_fingerprint());
    let res_widths: Vec<u8> =
        res_terms.iter().map(|t| col_width(bank, t.col)).collect();
    crate::engine::phn(node, if res_applied { "frame_final" } else { "frame" }, t_frame);

    // Key shape from the plan's KeyExpr list: leading byval Cols are the
    // int lanes; CaseSrc is the gated text; the final varlena Col is the
    // dst text. (Shape assertions, not query identity.)
    let mut int_cols: Vec<u32> = Vec::new();
    let mut gated: Option<(u32, Vec<u32>)> = None;
    let mut final_text: Option<u32> = None;
    for ke in &node.params.key_exprs {
        match *ke {
            KeyExpr::Col(c) if col_width(bank, c) > 0 => int_cols.push(c),
            KeyExpr::Col(c) => final_text = Some(c),
            KeyExpr::CaseSrc { se, adv, referer } => gated = Some((referer, vec![se, adv])),
            other => panic!("hash_plane frame path: unsupported key expr {other:?}"),
        }
    }
    let (a_src, gates) = gated.expect("q39 shape needs a CASE-gated text key");
    let a_dst = final_text.expect("q39 shape needs a final text key");
    let nints = int_cols.len();
    assert!(nints <= 3, "u128 layout holds <= 3 int lanes");
    let gate_idx: Vec<usize> = gates
        .iter()
        .map(|g| int_cols.iter().position(|c| c == g).expect("gate must be an int key lane"))
        .collect();
    // [noglobaldict, risks.md §11] the key domain is PART-SCOPED only —
    // src/dst text lanes carry psk fields (no cross-part identity); byte
    // identity is restored at the COMBINE (group grain). The stitched
    // global-code arm is deleted at port.
    use crate::stencils::part_merge::{face_bytes, psk, psk_code, psk_part};
    crate::stencils::part_merge::assert_psk_fits(bank);
    // [coldstart] only the frame's parts (the bank-wide arm was the
    // measured-settled control).
    let t_faces = std::time::Instant::now();
    let (pf_src, pf_dst) = {
        let mut touched: Vec<usize> = granules.iter().map(|fg| fg.pi).collect();
        touched.sort_unstable();
        touched.dedup();
        (
            crate::stencils::part_merge::dict_faces_for(ctx, a_src, &touched),
            crate::stencils::part_merge::dict_faces_for(ctx, a_dst, &touched),
        )
    };
    crate::engine::phn(node, "faces", t_faces);

    const P: usize = 256;

    let ints_key = |ints: &[i16], src_slot: u128, dst: u128| -> u128 {
        let mut k = (src_slot << 40) | dst;
        for (j, &v) in ints.iter().enumerate() {
            k |= (v as u16 as u128) << (112 - 16 * j);
        }
        k
    };
    let unpack_pack = |ik: u128| -> Vec<u8> {
        let mut ints: Vec<i16> = Vec::with_capacity(nints);
        for j in 0..nints {
            ints.push((ik >> (112 - 16 * j)) as u16 as i16);
        }
        let src_slot = (ik >> 40) & 0xffff_ffff_ff;
        let src: &[u8] = if src_slot == NOSRC {
            b""
        } else {
            let k = src_slot as u64;
            face_bytes(&pf_src, psk_part(k), psk_code(k))
        };
        let dk = (ik & 0xff_ffff_ffff) as u64;
        q39_style_pack(&ints, src, face_bytes(&pf_dst, psk_part(dk), psk_code(dk)))
    };

    struct S {
        rs: Vec<Scratch>,
        rc: Vec<CurCache>,
        xs: Vec<Scratch>,
        xc: Vec<CurCache>,
        codes: Vec<u32>,
        codes2: Vec<u32>,
        buckets: Vec<Vec<u128>>,
        side: FxBytesMap,
        rec: Vec<(usize, Vec<u16>)>,
    }
    let aux: Vec<u32> = int_cols.iter().copied().chain([a_src, a_dst]).collect();
    let res_cols: Vec<u32> = res_terms.iter().map(|t| t.col).collect();
    // [persist-rehome] WHOLE-STATE persistence, rehomed: the former
    // PERSISTQ was an unbounded per-query static (its motive stands:
    // DROPPING the 96 pass-1 states cost a measured 10.3ms per rep —
    // large-buffer frees against a fragmented heap), but it parked the
    // rc/xc CURSOR CACHES across statements keyed only by `node.q` —
    // the distinct.rs wrong-answer class (a CurCache revalidates only
    // its part index, so a parked cursor can resurface another bank's
    // or column's stream on a later statement). Now: the scatter
    // buckets/side/codes park in a capped, query-agnostic StatePark
    // (contents cleared at both ends); decode scratches ride the worker
    // depot via the run_finish finish; cursors are built per engagement
    // and NEVER parked.
    static PARKQ: crate::stencils::statepark::StatePark<S> =
        crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<S>> =
        std::sync::Mutex::new(PARKQ.fetch_up_to(pool.threads()));
    let t_p1 = std::time::Instant::now();
    let mut pass1 = pool.run_finish(
        granules.len(),
        |_| {
            let mut st = parked.lock().unwrap().pop().unwrap_or_else(|| S {
                rs: Vec::new(),
                rc: Vec::new(),
                xs: Vec::new(),
                xc: Vec::new(),
                codes: Vec::new(),
                codes2: Vec::new(),
                buckets: (0..P).map(|_| Vec::new()).collect(),
                side: Default::default(),
                rec: Vec::new(),
            });
            // Both-ends hygiene + geometry arm (parks are query-agnostic).
            if st.buckets.len() != P {
                st.buckets = (0..P).map(|_| Vec::new()).collect();
            }
            st.buckets.iter_mut().for_each(|b| b.clear());
            st.side.clear();
            st.rec.clear();
            // Per-engagement decode state: scratches from this worker's
            // depot; cursors fresh (never at rest).
            st.rs = res_cols.iter().map(|_| crate::scan::scratch_fetch()).collect();
            st.rc = res_cols.iter().map(|&a| CurCache::new(a)).collect();
            st.xs = aux.iter().map(|_| crate::scan::scratch_fetch()).collect();
            st.xc = aux.iter().map(|&a| CurCache::new(a)).collect();
            st
        },
        |s: &mut S, gi| {
            let fg = &granules[gi];
            let rows = fg.rows as usize;
            // residue filter over the frame rowlist.
            let mut rcols: Vec<&[u64]> = Vec::with_capacity(res_cols.len());
            for (scr, cc) in s.rs.iter_mut().zip(s.rc.iter_mut()) {
                let d = scr.decode_full(cc.get(bank, fg.pi), fg.g, rows);
                rcols.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
            }
            // Term-major residue filter (R2: op match at granule-term
            // grain).
            let mut surv: Vec<u16> = fg.rl.clone();
            for (ti, tm) in res_terms.iter().enumerate() {
                let (d, w) = (rcols[ti], res_widths[ti]);
                tm.filter_sel(&mut surv, |_| true, |r| sx(d[r], w));
            }
            if record_final {
                s.rec.push((fg.ord, surv.clone()));
            }
            if surv.is_empty() {
                return;
            }
            let dictish = pf_src[fg.pi].dh.is_some() && pf_dst[fg.pi].dh.is_some();
            if dictish {
                // ints decoded FULL; text lanes as codes.
                let mut icols: Vec<&[u64]> = Vec::with_capacity(nints);
                for j in 0..nints {
                    let (scr, cc) = (&mut s.xs[j], &mut s.xc[j]);
                    let d = scr.decode_full(cc.get(bank, fg.pi), fg.g, rows);
                    icols.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
                }
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                    s.codes2.resize(rows, 0);
                }
                s.xc[nints]
                    .get(bank, fg.pi)
                    .decode_codes(fg.g, &mut s.codes[..rows])
                    .expect("codes");
                s.xc[nints + 1]
                    .get(bank, fg.pi)
                    .decode_codes(fg.g, &mut s.codes2[..rows])
                    .expect("codes");
                let mut ints = vec![0i16; nints];
                // Part-local codes straight off the unpack: no remap
                // load, no global space. The empty-string gate is the
                // part's OWN empty code (sorted dict: code 0).
                let es = pf_src[fg.pi].empty_code;
                for &r in &surv {
                    let r = r as usize;
                    for j in 0..nints {
                        ints[j] = sx(icols[j][r], 2) as i16;
                    }
                    let gate_open = gate_idx.iter().all(|&j| ints[j] == 0);
                    let src_slot = if gate_open {
                        let c = s.codes[r];
                        if Some(c) == es { NOSRC } else { psk(fg.pi, c) as u128 }
                    } else {
                        NOSRC
                    };
                    let key =
                        ints_key(&ints, src_slot, psk(fg.pi, s.codes2[r]) as u128);
                    s.buckets[(ghash128(key) >> 56) as usize].push(key);
                }
            } else {
                // Late remap: decode_sel every key lane for the survivors.
                let mut kcols: Vec<Vec<u64>> = Vec::with_capacity(aux.len());
                for j in 0..aux.len() {
                    let (scr, cc) = (&mut s.xs[j], &mut s.xc[j]);
                    kcols.push(scr.decode_sel(cc.get(bank, fg.pi), fg.g, &surv).to_vec());
                }
                let mut ints = vec![0i16; nints];
                for idx in 0..surv.len() {
                    for j in 0..nints {
                        ints[j] = sx(kcols[j][idx], 2) as i16;
                    }
                    let gate_open = gate_idx.iter().all(|&j| ints[j] == 0);
                    let src: &[u8] = if gate_open {
                        unsafe { varlena_payload(kcols[nints][idx]) }
                    } else {
                        b""
                    };
                    let dst = unsafe { varlena_payload(kcols[nints + 1][idx]) };
                    // No global lookup exists: non-dict rows carry their
                    // key BYTES to the combine (side map). Never the hot
                    // shape — the measured banks publish dicts.
                    let kb = q39_style_pack(&ints, src, dst);
                    *s.side.entry(kb).or_insert(0) += 1;
                }
            }
        },
        // Worker-side finish: decode scratches park on THIS worker's
        // depot; cursors DROP here — never at rest across statements.
        |mut s| {
            s.rs.drain(..).for_each(crate::scan::scratch_park);
            s.xs.drain(..).for_each(crate::scan::scratch_park);
            s.rc.clear();
            s.xc.clear();
            s
        },
    );

    if record_final {
        let units = ctx.faces.walk(bank, pred.frame()[0].col);
        let recs: Vec<(usize, Vec<u16>)> =
            pass1.iter_mut().flat_map(|s| s.rec.drain(..)).collect();
        publish_final(ctx, node, pred, units, recs);
    }
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let mut pass1v = pass1;
    let pass1 = &pass1v;
    // [persist-rehome] pass-2 count tables: data-only (reset per bucket)
    // but formerly uncapped — now under the shrink law.
    static PARKT: crate::stencils::statepark::StatePark<Cnt128> =
        crate::stencils::statepark::StatePark::new(64 << 20);
    let k = node.params.emit_cap();
    let owned = pool.run(
        P,
        |_| {
            (
                Vec::new(),
                0u64,
                0u64,
                PARKT.fetch().unwrap_or_else(|| Cnt128::new(16)),
            )
        },
        |(out, groups, rows, tbl): &mut (Vec<(u128, u64)>, u64, u64, Cnt128), p| {
            let n: usize = pass1.iter().map(|s| s.buckets[p].len()).sum();
            if n == 0 {
                return;
            }
            tbl.reset(n.max(16));
            for s in pass1.iter() {
                for &key in &s.buckets[p] {
                    tbl.add(key, 1);
                }
            }
            let mut all: Vec<(u128, u64)> = Vec::with_capacity(tbl.len);
            for i in 0..tbl.cap() {
                if tbl.cnt[i] != 0 {
                    all.push((tbl.keys[i], tbl.cnt[i] as u64));
                    *rows += tbl.cnt[i] as u64;
                }
            }
            *groups += all.len() as u64;
            // Per-owner tie-inclusive top-k is UNSOUND in the part-scoped
            // key domain (fragments of one group can each sit under an
            // owner's bar while their merged total clears it) — every
            // group flows to the string-keyed combine. (The global-code
            // arm that could prune here is deleted, risks.md §11.)
            out.extend(all);
        },
    );
    let mut side: FxBytesMap = Default::default();
    for s in pass1.iter() {
        for (kb, &c) in &s.side {
            *side.entry(kb.clone()).or_insert(0) += c;
        }
    }
    let mut groups: u64 = side.len() as u64;
    let mut total_rows: u64 = side.values().sum();
    let mut union: Vec<(u128, u64)> = Vec::new();
    {
        for (o, g, r, tbl) in owned {
            groups += g;
            total_rows += r;
            union.extend(o);
            let b = tbl.keys.capacity() * 16 + tbl.cnt.capacity() * 4;
            PARKT.park(tbl, b);
        }
    }
    let t_d = std::time::Instant::now();
    {
        // Park the plain-heap state under the byte cap, contents cleared
        // (both-ends reset). Scratches are already on the worker depot;
        // cursor vecs are empty — asserted, never parked.
        let bytes_of = |s: &S| -> usize {
            use crate::stencils::statepark::{nested_bytes, vec_bytes};
            vec_bytes(&s.codes)
                + vec_bytes(&s.codes2)
                + nested_bytes(&s.buckets)
                + s.side.capacity() * 48
                + s.rec.capacity() * std::mem::size_of::<(usize, Vec<u16>)>()
        };
        for mut s in pass1v.drain(..).chain(parked.into_inner().unwrap()) {
            debug_assert!(
                s.rs.is_empty() && s.rc.is_empty() && s.xs.is_empty() && s.xc.is_empty(),
                "cursors/scratches must never rest in the state park"
            );
            s.buckets.iter_mut().for_each(|b| b.clear());
            s.side.clear();
            s.rec.clear();
            let b = bytes_of(&s);
            PARKQ.park(s, b);
        }
    }
    crate::engine::phn(node, "state_park", t_d);
    let mut cand: Vec<(Vec<u8>, u64)> = Vec::new();
    {
        // STRING-KEYED COMBINE at GROUP grain, parallel:
        // every owner-folded fragment (part-scoped key, count) is re-
        // partitioned by a hash of its ACTUAL key bytes (ints + src + dst,
        // read straight off the dict faces — no allocation), then bucket
        // owners fold fragments on that hash with byte equality checked
        // ONLY on hash match (fragments of one group from different
        // parts). Strings are touched per GROUP fragment, never per row;
        // groups/rows are counted on the merged sets. Per-owner tie-
        // inclusive top-k is sound again here (groups are whole).
        let t_c = std::time::Instant::now();
        let side_v: Vec<(&Vec<u8>, u64)> = side.iter().map(|(k, &c)| (k, c)).collect();
        // group key view: Ik(u128) or Side(idx) — bytes-equal comparison
        // without materializing the packed key.
        let src_of = |ik: u128| -> &[u8] {
            let sl = (ik >> 40) & 0xffff_ffff_ff;
            if sl == NOSRC {
                b""
            } else {
                let k = sl as u64;
                face_bytes(&pf_src, psk_part(k), psk_code(k))
            }
        };
        let dst_of = |ik: u128| -> &[u8] {
            let dk = (ik & 0xff_ffff_ffff) as u64;
            face_bytes(&pf_dst, psk_part(dk), psk_code(dk))
        };
        let ints_of = |ik: u128, out: &mut [i16]| {
            for (j, o) in out.iter_mut().enumerate() {
                *o = (ik >> (112 - 16 * j)) as u16 as i16;
            }
        };
        // FNV-1a over the packed-key byte image (ints LE, src len LE, src, dst).
        let hash_ik = |ik: u128| -> u64 {
            let mut h = 0xcbf2_9ce4_8422_2325u64;
            let mut f = |b: u8| h = (h ^ b as u64).wrapping_mul(0x1000_0000_01b3);
            for j in 0..nints {
                let v = (ik >> (112 - 16 * j)) as u16 as i16;
                for b in v.to_le_bytes() {
                    f(b);
                }
            }
            let sb = src_of(ik);
            for b in (sb.len() as u32).to_le_bytes() {
                f(b);
            }
            for &b in sb {
                f(b);
            }
            for &b in dst_of(ik) {
                f(b);
            }
            h
        };
        let hash_side = |kb: &[u8]| -> u64 { crate::grouped::hash_bytes(kb) };
        // equality: Ik vs Ik / Ik vs Side / Side vs Side
        let ik_eq = |a: u128, b: u128| -> bool {
            let mask_ints: u128 = !((1u128 << 80) - 1);
            (a & mask_ints) == (b & mask_ints) && src_of(a) == src_of(b) && dst_of(a) == dst_of(b)
        };
        let ik_eq_side = |a: u128, kb: &[u8]| -> bool {
            let mut ints = vec![0i16; nints];
            ints_of(a, &mut ints);
            let mut off = 0usize;
            for &v in &ints {
                if kb.len() < off + 2 || i16::from_le_bytes([kb[off], kb[off + 1]]) != v {
                    return false;
                }
                off += 2;
            }
            if kb.len() < off + 4 {
                return false;
            }
            let sl = u32::from_le_bytes(kb[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let sb = src_of(a);
            if sb.len() != sl || kb.len() < off + sl || &kb[off..off + sl] != sb {
                return false;
            }
            &kb[off + sl..] == dst_of(a)
        };
        // pass A: hash + scatter fragments into 256 buckets (parallel over
        // fixed-size chunks of the union; side entries ride along).
        const CB: usize = 256;
        let chunk = 4096usize;
        let nchunk_u = union.len().div_ceil(chunk);
        let nchunk_s = side_v.len().div_ceil(chunk);
        let unionr = &union;
        let sider = &side_v;
        if ctx.faces.cfg.fpcombine {
            // [fpcombine] fold fragments on a 128-bit key fingerprint
            // (entry fingerprints of src/dst + an int-lane mix) — no byte
            // walks, no byte equality; side fragments compute the SAME mix
            // from their packed bytes so cross-domain folds stay exact
            // under the 128-bit convention.
            use crate::grouped::{hash128 as ghash128f, hash64};
            use crate::fp::entry_fp128;
            const NOSRC_FP: u128 = 0x9E37_79B9_7F4A_7C15_D6E8_FEB8_6659_FD93;
            let imix = |iw: u64| -> u128 {
                ((hash64(iw) as u128) << 64)
                    | hash64(iw ^ 0x0123_4567_89AB_CDEF) as u128
            };
            // Lazy per-fragment fingerprints. NEGATIVE RESULTS, measured on
            // the rig: (a) full build_fps over the touched parts' dict
            // domains = 2x worse than the byte-walk it replaced (12M
            // entries hashed for a 433K-fragment union); (b) dedupe-the-
            // union's-codes memo (sort + parallel hash + binary-search
            // mix) = 4x worse — the union has almost NO code sharing
            // (433K fragments over 273K src + 324K dst distinct codes).
            // One entry_fp128 per fragment lane is the floor.
            let fp_mix = |ik: u128| -> u128 {
                let sl = (ik >> 40) & 0xffff_ffff_ff;
                let fs = if sl == NOSRC {
                    NOSRC_FP
                } else {
                    let k = sl as u64;
                    entry_fp128(face_bytes(&pf_src, psk_part(k), psk_code(k)))
                };
                let dk = (ik & 0xff_ffff_ffff) as u64;
                let fd = entry_fp128(face_bytes(&pf_dst, psk_part(dk), psk_code(dk)));
                fs ^ fd.rotate_left(3) ^ imix((ik >> 80) as u64)
            };
            let side_mix = |kb: &[u8]| -> u128 {
                let mut iw: u64 = 0;
                let mut off = 0usize;
                for j in 0..nints {
                    let v = u16::from_le_bytes([kb[off], kb[off + 1]]) as u64;
                    iw |= v << (32 - 16 * j);
                    off += 2;
                }
                let sl = u32::from_le_bytes(kb[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let src = &kb[off..off + sl];
                let dst = &kb[off + sl..];
                let fs = if src.is_empty() { NOSRC_FP } else { entry_fp128(src) };
                fs ^ entry_fp128(dst).rotate_left(3) ^ imix(iw)
            };
            // [p2-phase-widening] combine scatter/fold arenas ride the
            // park (q39's `combine` +0.43 ms served vs rig was exactly
            // this leg's fresh-per-exec scratch; see statepark.rs note).
            use crate::stencils::statepark::{arm_buckets, scatter_park_on};
            static PARKCS: crate::stencils::statepark::StatePark<Vec<Vec<(u128, u64, u64)>>> =
                crate::stencils::statepark::StatePark::new(256 << 20);
            static PARKCF: crate::stencils::statepark::StatePark<Vec<(u128, u64, u64)>> =
                crate::stencils::statepark::StatePark::new(64 << 20);
            let scat = pool.run(
                nchunk_u + nchunk_s,
                |_| {
                    let b = if scatter_park_on() { PARKCS.fetch() } else { None };
                    arm_buckets(b.unwrap_or_default(), CB)
                },
                |b: &mut Vec<Vec<(u128, u64, u64)>>, ci| {
                    if ci < nchunk_u {
                        let lo = ci * chunk;
                        let hi = (lo + chunk).min(unionr.len());
                        for (i, &(ik, c)) in unionr[lo..hi].iter().enumerate() {
                            let m = fp_mix(ik);
                            b[(m >> 120) as usize].push((m, (lo + i) as u64, c));
                        }
                    } else {
                        let ci = ci - nchunk_u;
                        let lo = ci * chunk;
                        let hi = (lo + chunk).min(sider.len());
                        for (i, &(kb, c)) in sider[lo..hi].iter().enumerate() {
                            let m = side_mix(kb);
                            b[(m >> 120) as usize].push((m, (1u64 << 63) | (lo + i) as u64, c));
                        }
                    }
                },
            );
            let scatr = &scat;
            let folded = pool.run(
                CB,
                |_| {
                    let slots = if scatter_park_on() { PARKCF.fetch() } else { None };
                    (Vec::<(u64, u64)>::new(), 0u64, 0u64, slots.unwrap_or_default())
                },
                |(out, g, r, slots): &mut (Vec<(u64, u64)>, u64, u64, Vec<(u128, u64, u64)>), bk| {
                    let n: usize = scatr.iter().map(|b| b[bk].len()).sum();
                    if n == 0 {
                        return;
                    }
                    let cap = (n * 2).next_power_of_two().max(16);
                    let mask = cap - 1;
                    // Parked slots are capacity-only: arm the size, then
                    // reset the count lane (the emptiness witness — the
                    // same `e.2 = 0` law as PARKT's owner tables above).
                    if slots.len() < cap {
                        slots.resize(cap, (0, 0, 0));
                    }
                    for e in slots[..cap].iter_mut() {
                        e.2 = 0;
                    }
                    let tbl = &mut slots[..cap];
                    for b in scatr.iter() {
                        for &(m, rf, c) in &b[bk] {
                            let mut i = (ghash128f(m) as usize) & mask;
                            loop {
                                let e = &mut tbl[i];
                                if e.2 == 0 {
                                    *e = (m, rf, c);
                                    break;
                                }
                                if e.0 == m {
                                    e.2 += c;
                                    break;
                                }
                                i = (i + 1) & mask;
                            }
                        }
                    }
                    let mut all: Vec<(u64, u64)> = Vec::new();
                    for e in tbl.iter() {
                        if e.2 != 0 {
                            all.push((e.1, e.2));
                            *r += e.2;
                        }
                    }
                    *g += all.len() as u64;
                    if all.len() > k {
                        let (_, nth, _) = all.select_nth_unstable_by(k - 1, |a, b| b.1.cmp(&a.1));
                        let lt = nth.1;
                        all.retain(|e| e.1 >= lt);
                    }
                    out.extend(all);
                },
            );
            groups = 0;
            total_rows = 0;
            let mut cands: Vec<(u64, u64)> = Vec::new();
            for (o, g, r, slots) in folded {
                groups += g;
                total_rows += r;
                cands.extend(o);
                if scatter_park_on() {
                    let b = crate::stencils::statepark::vec_bytes(&slots);
                    PARKCF.park(slots, b);
                }
            }
            if scatter_park_on() {
                for b in scat {
                    let bytes = crate::stencils::statepark::nested_bytes(&b);
                    PARKCS.park(b, bytes);
                }
            }
            let mut counts: Vec<u64> = cands.iter().map(|e| e.1).collect();
            let cb = if counts.len() > k {
                let (_, nth, _) = counts.select_nth_unstable_by(k - 1, |a, b| b.cmp(a));
                *nth
            } else {
                0
            };
            for &(rf, c) in &cands {
                if c >= cb {
                    let kb = if rf >> 63 != 0 {
                        side_v[(rf & !(1u64 << 63)) as usize].0.clone()
                    } else {
                        unpack_pack(union[rf as usize].0)
                    };
                    cand.push((kb, c));
                }
            }
            crate::engine::phn(node, "combine", t_c);
        } else {
        let scat = pool.run(
            nchunk_u + nchunk_s,
            |_| (0..CB).map(|_| Vec::<(u64, u64, u64)>::new()).collect::<Vec<_>>(),
            |b: &mut Vec<Vec<(u64, u64, u64)>>, ci| {
                if ci < nchunk_u {
                    let lo = ci * chunk;
                    let hi = (lo + chunk).min(unionr.len());
                    for (i, &(ik, c)) in unionr[lo..hi].iter().enumerate() {
                        let h = hash_ik(ik);
                        // payload: (h, tag|index, count); tag bit 63 = side
                        b[(h >> 56) as usize].push((h, (lo + i) as u64, c));
                    }
                } else {
                    let ci = ci - nchunk_u;
                    let lo = ci * chunk;
                    let hi = (lo + chunk).min(sider.len());
                    for (i, &(kb, c)) in sider[lo..hi].iter().enumerate() {
                        let h = hash_side(kb);
                        b[(h >> 56) as usize].push((h, (1u64 << 63) | (lo + i) as u64, c));
                    }
                }
            },
        );
        let scatr = &scat;
        // pass B: bucket owners fold on hash; byte-equality on hash match.
        let bytes_eq = |x: u64, y: u64| -> bool {
            let (xs, ys) = (x >> 63 != 0, y >> 63 != 0);
            let (xi, yi) = ((x & !(1u64 << 63)) as usize, (y & !(1u64 << 63)) as usize);
            match (xs, ys) {
                (false, false) => ik_eq(unionr[xi].0, unionr[yi].0),
                (true, true) => sider[xi].0 == sider[yi].0,
                (false, true) => ik_eq_side(unionr[xi].0, sider[yi].0),
                (true, false) => ik_eq_side(unionr[yi].0, sider[xi].0),
            }
        };
        let folded = pool.run(
            CB,
            |_| (Vec::<(u64, u64)>::new(), 0u64, 0u64),
            |(out, g, r): &mut (Vec<(u64, u64)>, u64, u64), bk| {
                let n: usize = scatr.iter().map(|b| b[bk].len()).sum();
                if n == 0 {
                    return;
                }
                let cap = (n * 2).next_power_of_two().max(16);
                let mask = cap - 1;
                // slots: (hash, ref, count); count 0 = empty
                let mut slots: Vec<(u64, u64, u64)> = vec![(0, 0, 0); cap];
                for b in scatr.iter() {
                    for &(h, rf, c) in &b[bk] {
                        let mut i = (h as usize) & mask;
                        loop {
                            let e = &mut slots[i];
                            if e.2 == 0 {
                                *e = (h, rf, c);
                                break;
                            }
                            if e.0 == h && bytes_eq(e.1, rf) {
                                e.2 += c;
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
                let mut all: Vec<(u64, u64)> = Vec::new();
                for e in &slots {
                    if e.2 != 0 {
                        all.push((e.1, e.2));
                        *r += e.2;
                    }
                }
                *g += all.len() as u64;
                if all.len() > k {
                    let (_, nth, _) = all.select_nth_unstable_by(k - 1, |a, b| b.1.cmp(&a.1));
                    let lt = nth.1;
                    all.retain(|e| e.1 >= lt);
                }
                out.extend(all);
            },
        );
        groups = 0;
        total_rows = 0;
        let mut cands: Vec<(u64, u64)> = Vec::new();
        for (o, g, r) in folded {
            groups += g;
            total_rows += r;
            cands.extend(o);
        }
        let mut counts: Vec<u64> = cands.iter().map(|e| e.1).collect();
        let cb = if counts.len() > k {
            let (_, nth, _) = counts.select_nth_unstable_by(k - 1, |a, b| b.cmp(a));
            *nth
        } else {
            0
        };
        for &(rf, c) in &cands {
            if c >= cb {
                let kb = if rf >> 63 != 0 {
                    side_v[(rf & !(1u64 << 63)) as usize].0.clone()
                } else {
                    unpack_pack(union[rf as usize].0)
                };
                cand.push((kb, c));
            }
        }
        crate::engine::phn(node, "combine", t_c);
        }
    }
    cand.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    // Typed emit: unpack the packed key image into its int/src/dst lanes.
    let mut int_out: Vec<Vec<i64>> = (0..nints).map(|_| Vec::new()).collect();
    let mut src_out = BytesBuild::new();
    let mut dst_out = BytesBuild::new();
    let mut cnts: Vec<i64> = Vec::new();
    for (kb, c) in cand.iter().skip(node.params.offset).take(node.params.limit) {
        let mut off = 0usize;
        for v in int_out.iter_mut() {
            v.push(i16::from_le_bytes([kb[off], kb[off + 1]]) as i64);
            off += 2;
        }
        let sl = u32::from_le_bytes(kb[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        src_out.push(&kb[off..off + sl]);
        dst_out.push(&kb[off + sl..]);
        cnts.push(*c as i64);
    }
    let mut cols_out: Vec<AnswerCol> = Vec::new();
    for (j, v) in int_out.into_iter().enumerate() {
        cols_out.push(AnswerCol::i64s(node.ty_of(int_cols[j]), v));
    }
    cols_out.push(src_out.finish(node.ty_of(a_src)));
    cols_out.push(dst_out.finish(node.ty_of(a_dst)));
    cols_out.push(AnswerCol::i64s(TypMeta::INT8, cnts));
    let mut a = AnswerSet::from_cols(cols_out);
    a.note = Some(crate::render::footer_groups(groups as u64, total_rows as u64));
    crate::engine::phn(node, "pass2_render", t_p2);
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [idx-139] A single group with >=2^32 rows must not wrap its counter.
    /// The per-group `cnt` lane is u64 (group cardinality is bounded only by
    /// the bank row total, a u64 fact), so priming a slot at u32::MAX and
    /// adding once more crosses the 32-bit boundary cleanly instead of
    /// wrapping to zero (which a u32 lane would, silently corrupting COUNT).
    #[test]
    fn oa_group_counter_does_not_wrap_past_u32() {
        let mut oa = OaSoA::new(4);
        let key: u128 = 0x1234_5678_9abc;
        let slot0 = hash128(key) as usize;
        oa.add(slot0, key, 0, 0); // seed the slot (cnt == 1)
        // Locate the live slot and prime it just below the u32 boundary.
        let slot = (0..=oa.mask).find(|&s| oa.keys[s] == key).unwrap();
        oa.cnt[slot] = u32::MAX as u64;
        oa.add(slot0, key, 0, 0); // the 2^32-th row
        assert_eq!(oa.cnt[slot], u32::MAX as u64 + 1);
        // A u32 lane would have wrapped to zero here.
        assert_ne!(oa.cnt[slot], 0);
    }
}
