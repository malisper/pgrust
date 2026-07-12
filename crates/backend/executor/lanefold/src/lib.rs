// Lane-native aggregate transition fold — the whole-batch int-family
// transition kernels harvested from the cbstore branch's
// `nodeagg/src/lanefold.rs` (inter-query-scheduling worktree), delivered as a
// standalone crate for the lane-executor-v2 hash-agg breaker.
//
// # Harvest provenance (what was kept vs stripped)
//
// KEPT (the vertical slice's core):
// - The `classify_trans` transfn-oid whitelist: COUNT(*)/COUNT(any),
//   SUM/AVG(int2/int4), MIN/MAX(int2/int4/int8/date/timestamp/timestamptz),
//   over a bare outer Var or an admitted affine OpExpr
//   (`(v / divk) * mulk + addend` from int24pl/int42mi/int4mul/int24div/...).
// - The TYPE-level non-erroring proof (`safe_interval`/`type_proof`): an
//   admitted expression is only folded unchecked when every value of the
//   Var's type width provably lands inside int4 — otherwise the admission
//   carries a DATA-level `Guard` interval that `check_guards` must re-prove
//   per batch (zone-map or exact lane pass) before the fold may run; a failed
//   proof demotes the whole batch to the checked per-row program, which
//   raises C's error at C's row (the interval is exact, so a demoted batch
//   always raises).
// - The whole-batch fold kernels (`fold_batch`) and the grouped per-row-lane
//   fold (`fold_rows_grouped`), byte-parity contract intact: every kernel
//   folds a commutative, non-erroring transition whose result is independent
//   of row order (i64 wrapping addition, min/max), so batch-major evaluation
//   is bit-identical to C's row-major transition order.
// - The cross-aggregate CSE schedule (`build_cse`): SumBase groups
//   Sum/AvgAccum/CountAny over one (col, divk) into a single (count, raw-sum)
//   pass with per-member `mulk*S + addend*c` derivation (legal in the
//   mod-2^64 ring); MinMax groups structurally identical transforms into one
//   scan.
// - The int8[2] {count,sum} AVG transarray discipline (`avg_apply`,
//   `new_int8_transarray`) matching C numeric.c's Int8TransTypeData carrier.
//
// STRIPPED (cbstore/lane-v1 wiring that does not exist on lane-executor-v2):
// - Dict-coded windows: DictEval derived lanes, the dict-group memo, textlen
//   lanes, and the per-(code, group) text MIN/MAX memo (`TextMmLane`).
// - Metadata-answered transitions (`classify_meta`/`MetaTrans`): footer row
//   counts / zone-map / footer-sum answers are cbstore-only.
// - The exact-DISTINCT hash sets (`DistinctSet`) and the parallel
//   partial-distinct sharding — separate machinery from the transition fold.
// - The specialized group-probe `KeyTable`, projected-scan column remaps, the
//   residual ExprState, and the once-per-plan logging markers: those belong
//   to the consuming node, not the kernel.
//
// # Integration point for the hash-agg breaker
//
// At plan build: run `classify(mcx, specs)` over the pertrans specs. It
// returns a `LanePlan` when at least one transition admits; `plan.resid`
// lists the transnos that did NOT admit (the breaker keeps its per-row
// program for those), and `plan.cols` lists the lane columns the fold reads.
//
// Per batch of transition inputs:
// 1. If `plan.guarded`, run `check_guards(&plan, &cols, rows, zone_minmax)`;
//    on `GuardCheck::Demote` run the WHOLE batch through the checked per-row
//    program (never mix a partial fold with per-row transitions).
// 2. Ungrouped (one group): `fold_batch(&plan, &cols, rows, nrows, pergroup)`.
// 3. Grouped: after the per-row hash probe snapshots each row's pergroup
//    pointer, `fold_rows_grouped(&plan, &cols, &idxs, &groups)`.
// AvgAccum pergroups must be initialized with `new_int8_transarray` (C's
// non-null initval); Sum/Min/Max start `no_trans_value`/NULL per C.
//
// Anything not admitted classifies out (fail-open, per transition): the
// breaker falls back to its per-row transition program for that agg shape.

use core::ptr::NonNull;

use ::datum::Datum;
use ::execexpr::{AggPerGroup, AggTransSpec, OUTER_VAR};
use ::exectuples::SoaBatch;
use ::mcx::{Mcx, PgVec};
use ::types_core::catalog::{
    DATEOID, INT2OID, INT4OID, INT8OID, TIMESTAMPOID, TIMESTAMPTZOID,
};
use ::types_core::Oid;
use ::types_nodes::node_tree::Node;

#[cfg(test)]
mod tests;

const F_INT8INC: Oid = 1219;
const F_INT8INC_ANY: Oid = 2804;
const F_INT2_SUM: Oid = 1840;
const F_INT4_SUM: Oid = 1841;
const F_INT2_AVG_ACCUM: Oid = 1962;
const F_INT4_AVG_ACCUM: Oid = 1963;
const F_INT4LARGER: Oid = 768;
const F_INT4SMALLER: Oid = 769;
const F_INT2LARGER: Oid = 770;
const F_INT2SMALLER: Oid = 771;
const F_INT8LARGER: Oid = 1236;
const F_INT8SMALLER: Oid = 1237;
const F_DATE_LARGER: Oid = 1138;
const F_DATE_SMALLER: Oid = 1139;
// 1195/1196 are the timestamptz pair, 2035/2036 the timestamp pair (both
// share the C impl; the aggregates bind them per input type).
const F_TIMESTAMP_SMALLER: Oid = 2035;
const F_TIMESTAMP_LARGER: Oid = 2036;
const F_TIMESTAMPTZ_SMALLER: Oid = 1195;
const F_TIMESTAMPTZ_LARGER: Oid = 1196;

const F_INT4MUL: Oid = 141;
const F_INT24MUL: Oid = 170;
const F_INT42MUL: Oid = 171;
const F_INT24DIV: Oid = 172;
const F_INT4PL: Oid = 177;
const F_INT24PL: Oid = 178;
const F_INT42PL: Oid = 179;
const F_INT4MI: Oid = 181;
const F_INT24MI: Oid = 182;
const F_INT42MI: Oid = 183;

// numeric.c Int8TransTypeData carrier: 2-element no-nulls int8 array.
pub const ARR_OVERHEAD_NONULLS_1: usize = 24;
pub const INT8_TRANSARRAY_SIZE: usize = ARR_OVERHEAD_NONULLS_1 + 16;

/// The (values, isnull) column lanes a fold reads. Implemented for
/// `SoaBatch`; test harnesses (and any other batch container) provide their
/// own. `col_values(c)`/`col_isnull(c)` must cover every staged row for
/// every column in `LanePlan::cols`.
pub trait LaneCols {
    fn col_values(&self, c: usize) -> &[Datum];
    fn col_isnull(&self, c: usize) -> &[bool];
}

impl LaneCols for SoaBatch<'_> {
    #[inline(always)]
    fn col_values(&self, c: usize) -> &[Datum] {
        SoaBatch::col_values(self, c)
    }

    #[inline(always)]
    fn col_isnull(&self, c: usize) -> &[bool] {
        SoaBatch::col_isnull(self, c)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LaneWidth {
    I16,
    I32,
    I64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LaneKind {
    CountStar,
    CountAny,
    // int2_sum/int4_sum: i64 accumulate, NULL init, not strict.
    Sum,
    // int2/int4_avg_accum: in-place {count,sum} int8[2] transarray.
    AvgAccum,
    // strict byval larger/smaller with signed total order.
    Min,
    Max,
}

impl LaneWidth {
    fn range(self) -> (i64, i64) {
        match self {
            LaneWidth::I16 => (i16::MIN as i64, i16::MAX as i64),
            LaneWidth::I32 => (i32::MIN as i64, i32::MAX as i64),
            LaneWidth::I64 => (i64::MIN, i64::MAX),
        }
    }
}

// DATA-level admission (proof-carrying tier below the TYPE proof): the
// admitted expression is only overflow-free for lane values inside
// [lo, hi] (the exact safe_interval). Every batch must prove its selected
// non-null values sit inside the interval — from the zone map (granule
// min/max, a superset of the batch) or an exact lane pass — before the
// unchecked fold may run; a failed proof demotes the whole batch to the
// checked per-row program, which raises C's error at C's row.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Guard {
    pub lo: i64,
    pub hi: i64,
}

// Cold per-plan guard side-table entry (lanetrans-compact): the data-level
// Guard interval hoisted OUT of the per-lane LaneTrans. Touched only by
// check_guards — a separate once-per-batch pass that runs only when the plan
// is guarded — never by the per-row fold kernels. `col`/`width` mirror the
// guarded lane's read.
#[derive(Clone, Copy, Debug)]
pub struct GuardEntry {
    pub col: u16,
    pub width: LaneWidth,
    pub lo: i64,
    pub hi: i64,
}

// addend/mulk/divk are i32: every admitted coefficient is an int4 Const (or a
// ±1 identity), so the affine transform's stored coefficients provably fit
// i32; they widen to i64 at the use site, leaving the fold arithmetic
// byte-identical.
#[derive(Clone, Copy, Debug)]
pub struct LaneTrans {
    pub kind: LaneKind,
    pub col: u16,
    // Lane read width (the admitted Var's type) vs the transvalue store
    // width (the transfn's argument/result type — int4 for the int2-Var
    // OpExpr admissions). Min/Max must store at res_width or an in-range
    // int4 result truncates through the int2 datum constructor.
    pub width: LaneWidth,
    pub res_width: LaneWidth,
    // Admitted arg expression, per selected row: ((v / divk) * mulk) + addend
    // with v the lane value. Ops are exclusive (single OpExpr admission), so
    // composition order is never observable.
    pub addend: i32,
    pub mulk: i32,
    pub divk: i32,
    pub transno: u16,
}

const _: () = assert!(core::mem::size_of::<LaneTrans>() <= 24);

// Branchy on the loop-invariant transform fields so LLVM unswitches the
// per-row loops: the dominant addend-only shape must stay a bare add (an
// unconditional sdiv per row cost Q7-class min/max folds ~13% on CB).
#[inline(always)]
fn xform(t: &LaneTrans, v: i64) -> i64 {
    let v = if t.divk != 1 { v / t.divk as i64 } else { v };
    let v = if t.mulk != 1 { v.wrapping_mul(t.mulk as i64) } else { v };
    v.wrapping_add(t.addend as i64)
}

// Cross-aggregate CSE (agg-rewrite-cse): transitions sharing one base lane
// pass. SumBase groups Sum/AvgAccum/CountAny over one (col, divk) — a single
// (count, raw-sum) batch pass; each member's delta derives as
// mulk*S + addend*c. MinMax groups structurally identical Min/Max transforms
// — one batch scan advances every member. Derivation legality: wrapping i64
// ops are the mod-2^64 ring, where multiplication distributes over addition,
// so mulk*Σv' + addend*c bit-equals the per-row Σ(v'*mulk + addend) fold —
// and every per-row term is int4-proven (type/zone/data admission), so both
// equal C's checked per-row evaluation, accumulated with C's own unchecked
// int8 transvalue arithmetic. Groups only ever fold on a fully proven batch:
// check_guards demotes the WHOLE batch to the checked per-row program before
// fold_batch runs (and a demoted batch always raises — the interval is
// exact), so no partial CSE state ever combines with a per-row fold.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CseGroupKind {
    SumBase,
    MinMax,
}

#[derive(Clone, Copy, Debug)]
pub struct CseGroup {
    pub kind: CseGroupKind,
    pub start: u16,
    pub len: u16,
}

/// The classified lane plan: the admitted transitions, their CSE schedule,
/// the cold guard side-table, the lane columns the fold reads, and the
/// transnos that did NOT admit (the caller's per-row residual set).
pub struct LanePlan<'mcx> {
    pub trans: PgVec<'mcx, LaneTrans>,
    // CSE schedule: groups over cse_members (indices into trans); cse_skip is
    // parallel to trans and marks members. Only the ungrouped fold_batch
    // consumes it — grouped folds stay per-trans.
    pub cse: PgVec<'mcx, CseGroup>,
    pub cse_members: PgVec<'mcx, u16>,
    pub cse_skip: PgVec<'mcx, bool>,
    // Cold guard side-table: one entry per guarded lane (empty when the plan
    // is not guarded), in trans order, so check_guards' demote-on-first-fail
    // order is deterministic.
    pub guards: PgVec<'mcx, GuardEntry>,
    pub cols: PgVec<'mcx, u16>,
    // Transnos classify refused: the caller keeps its checked per-row
    // transition program for these.
    pub resid: PgVec<'mcx, usize>,
    // Any admitted transition carries a data-level Guard (check_guards must
    // run per batch before the fold).
    pub guarded: bool,
}

// Admitted arg shape: v |-> ((v / divk) * mulk) + addend over the lane Var v.
#[derive(Clone, Copy)]
struct LaneArg {
    col: u16,
    width: LaneWidth,
    addend: i64,
    mulk: i64,
    divk: i64,
    guard: Option<Guard>,
}

const PLAIN: (i64, i64, i64) = (0, 1, 1);

// True floor/ceil division for either divisor sign. (Port fix: the cbstore
// original used div_euclid-based forms that are only floor/ceil for b > 0;
// for a negative non-unit mulk with inexact division they widened the safe
// interval by one on each side, admitting the two boundary values whose
// checked C evaluation raises. Exercised by safe_interval_is_exact.)
fn floor_div(a: i64, b: i64) -> i64 {
    let (q, r) = (a / b, a % b);
    if r != 0 && ((r < 0) != (b < 0)) {
        q - 1
    } else {
        q
    }
}

fn ceil_div(a: i64, b: i64) -> i64 {
    let (q, r) = (a / b, a % b);
    if r != 0 && ((r < 0) == (b < 0)) {
        q + 1
    } else {
        q
    }
}

/// The v-interval on which the admitted OpExpr's checked C evaluation cannot
/// raise: the int4-typed result (v/divk)*mulk + addend must fit int4 for
/// every v inside it. Exact for these transforms (monotone in v), so a lane
/// whose values all sit inside the interval evaluates unchecked to the same
/// bytes C's checked per-row ops produce. divk is admitted only as int24div's
/// nonzero const (int2/int4 -> int4 cannot overflow and k != 0 rules out the
/// division-by-zero raise), so the div interval is unbounded.
pub fn safe_interval(addend: i64, mulk: i64, divk: i64) -> (i64, i64) {
    if divk != 1 {
        debug_assert!(mulk == 1 && addend == 0);
        return (i64::MIN, i64::MAX);
    }
    // i32::MIN <= v*mulk + addend <= i32::MAX, |addend| <= 2^31 so the
    // subtraction stays exact in i64.
    let a = i32::MIN as i64 - addend;
    let b = i32::MAX as i64 - addend;
    match mulk {
        0 => {
            if a <= 0 && 0 <= b {
                (i64::MIN, i64::MAX)
            } else {
                (1, 0)
            }
        }
        m if m > 0 => (ceil_div(a, m), floor_div(b, m)),
        m => (ceil_div(b, m), floor_div(a, m)),
    }
}

/// TYPE-level proof: every value of the Var's type width is inside the safe
/// interval (the int2 +/- const admission cites 2^15-bounded inputs; the
/// unchecked i64 SUM accumulation beneath it is safe because even 2^31-max
/// int4 terms over any feasible rowcount stay under 2^63: 2^31 * 10^8 < 2^63,
/// matching C's own unchecked int8 transvalue arithmetic).
pub fn type_proof(width: LaneWidth, addend: i64, mulk: i64, divk: i64) -> bool {
    let (lo, hi) = safe_interval(addend, mulk, divk);
    let (wmin, wmax) = width.range();
    lo <= wmin && wmax <= hi
}

fn width_of(vartype: Oid) -> Option<LaneWidth> {
    match vartype {
        INT2OID => Some(LaneWidth::I16),
        INT4OID | DATEOID => Some(LaneWidth::I32),
        INT8OID | TIMESTAMPOID | TIMESTAMPTZOID => Some(LaneWidth::I64),
        _ => None,
    }
}

// Outer-slot Var of a lane-readable type; the fused drive's outer slot is the
// scan tuple (no projection admitted with outer reads), so varattno-1 is the
// SoA column.
pub fn classify_var(expr: Node<'_>, expected: Oid) -> Option<(u16, LaneWidth)> {
    let v = expr.as_var()?;
    if v.varno != OUTER_VAR || v.varlevelsup != 0 || v.varattno < 1 || v.vartype != expected {
        return None;
    }
    Some((v.varattno as u16 - 1, width_of(v.vartype)?))
}

fn classify_arg(expr: Node<'_>, expected: Oid) -> Option<LaneArg> {
    if let Some((col, width)) = classify_var(expr, expected) {
        return Some(LaneArg { col, width, addend: 0, mulk: 1, divk: 1, guard: None });
    }
    if expected != INT4OID {
        return None;
    }
    let op = expr.as_op_expr()?;
    if op.opretset || op.args.len() != 2 {
        return None;
    }
    let mut it = op.args.iter();
    let (a, b) = (it.next()?, it.next()?);
    // (var operand, const operand, var type, transform builder). int42div
    // (const / var) is not a v-monotone affine transform and stays refused.
    let (var, konst, vartype, mk): (_, _, Oid, fn(i64) -> (i64, i64, i64)) = match op.opfuncid {
        F_INT24PL => (a, b, INT2OID, |k| (k, 1, 1)),
        F_INT42PL => (b, a, INT2OID, |k| (k, 1, 1)),
        F_INT24MI => (a, b, INT2OID, |k| (-k, 1, 1)),
        F_INT42MI => (b, a, INT2OID, |k| (k, -1, 1)),
        F_INT24MUL => (a, b, INT2OID, |k| (0, k, 1)),
        F_INT42MUL => (b, a, INT2OID, |k| (0, k, 1)),
        F_INT24DIV => (a, b, INT2OID, |k| (0, 1, k)),
        F_INT4PL => (a, b, INT4OID, |k| (k, 1, 1)),
        F_INT4MI => (a, b, INT4OID, |k| (-k, 1, 1)),
        F_INT4MUL => (a, b, INT4OID, |k| (0, k, 1)),
        _ => return None,
    };
    let (col, width) = classify_var(var, vartype)?;
    let c = konst.as_const()?;
    if c.constisnull || c.consttype != INT4OID {
        return None;
    }
    let k = c.constvalue.as_i32() as i64;
    let (addend, mulk, divk) = mk(k);
    if divk == 0 {
        // int24div by a zero const raises division-by-zero per row in C;
        // refusal keeps that raise on the per-row program.
        return None;
    }
    let guard = if type_proof(width, addend, mulk, divk) {
        None
    } else {
        let (lo, hi) = safe_interval(addend, mulk, divk);
        if lo > hi {
            return None;
        }
        Some(Guard { lo, hi })
    };
    Some(LaneArg { col, width, addend, mulk, divk, guard })
}

/// NULL-ness of an admitted arg equals the Var's NULL-ness (strict operators
/// over a non-null Const), so CountAny reads only the Var's isnull lane.
pub fn classify_trans(
    spec: &AggTransSpec<'_, '_>,
    transno: usize,
) -> Option<(LaneTrans, Option<GuardEntry>)> {
    if spec.combine || spec.ordered.is_some() || spec.aggfilter.is_some() || spec.cur_agg.is_some()
    {
        return None;
    }
    let transno = u16::try_from(transno).ok()?;
    let arg = |expected: Oid| -> Option<(LaneArg, LaneWidth)> {
        if spec.args.len() != 1 {
            return None;
        }
        let tle = spec.args.iter().next()?.as_target_entry()?;
        Some((classify_arg(tle.expr, expected)?, width_of(expected)?))
    };
    let mk = |kind, (a, res_width): (LaneArg, LaneWidth)| {
        let guard = a
            .guard
            .map(|g| GuardEntry { col: a.col, width: a.width, lo: g.lo, hi: g.hi });
        Some((
            LaneTrans {
                kind,
                col: a.col,
                width: a.width,
                res_width,
                addend: a.addend as i32,
                mulk: a.mulk as i32,
                divk: a.divk as i32,
                transno,
            },
            guard,
        ))
    };
    let plain = |col, width| {
        let (addend, mulk, divk) = PLAIN;
        (LaneArg { col, width, addend, mulk, divk, guard: None }, width)
    };
    match spec.transfn_oid {
        F_INT8INC if spec.args.is_nil() && !spec.init_value_is_null => {
            mk(LaneKind::CountStar, plain(0, LaneWidth::I64))
        }
        F_INT8INC_ANY if !spec.init_value_is_null => {
            if spec.args.len() != 1 {
                return None;
            }
            let tle = spec.args.iter().next()?.as_target_entry()?;
            let v = tle.expr.as_var()?;
            if v.varno != OUTER_VAR || v.varlevelsup != 0 || v.varattno < 1 {
                return None;
            }
            mk(LaneKind::CountAny, plain(v.varattno as u16 - 1, LaneWidth::I64))
        }
        F_INT2_SUM if spec.init_value_is_null => mk(LaneKind::Sum, arg(INT2OID)?),
        F_INT4_SUM if spec.init_value_is_null => mk(LaneKind::Sum, arg(INT4OID)?),
        F_INT2_AVG_ACCUM if !spec.init_value_is_null => mk(LaneKind::AvgAccum, arg(INT2OID)?),
        F_INT4_AVG_ACCUM if !spec.init_value_is_null => mk(LaneKind::AvgAccum, arg(INT4OID)?),
        F_INT2LARGER => mk(LaneKind::Max, arg(INT2OID)?),
        F_INT2SMALLER => mk(LaneKind::Min, arg(INT2OID)?),
        F_INT4LARGER => mk(LaneKind::Max, arg(INT4OID)?),
        F_INT4SMALLER => mk(LaneKind::Min, arg(INT4OID)?),
        F_INT8LARGER => mk(LaneKind::Max, arg(INT8OID)?),
        F_INT8SMALLER => mk(LaneKind::Min, arg(INT8OID)?),
        F_DATE_LARGER => mk(LaneKind::Max, arg(DATEOID)?),
        F_DATE_SMALLER => mk(LaneKind::Min, arg(DATEOID)?),
        F_TIMESTAMP_LARGER => mk(LaneKind::Max, arg(TIMESTAMPOID)?),
        F_TIMESTAMP_SMALLER => mk(LaneKind::Min, arg(TIMESTAMPOID)?),
        F_TIMESTAMPTZ_LARGER => mk(LaneKind::Max, arg(TIMESTAMPTZOID)?),
        F_TIMESTAMPTZ_SMALLER => mk(LaneKind::Min, arg(TIMESTAMPTZOID)?),
        _ => None,
    }
}

/// Min/max NULL-init strictness requires strict transfns; every admitted
/// larger/smaller is strict in the catalog, and the count/avg initvals are
/// non-null by catalog. classify() re-derives nothing from the catalog at run
/// time — the OID whitelist IS the semantic contract.
///
/// Returns None when no transition admits (the caller keeps its whole
/// per-row program); otherwise `resid` carries the refused transnos.
pub fn classify<'mcx>(
    mcx: Mcx<'mcx>,
    specs: &[AggTransSpec<'_, 'mcx>],
) -> Option<LanePlan<'mcx>> {
    let mut trans: PgVec<'mcx, LaneTrans> = PgVec::new_in(mcx);
    let mut guards: PgVec<'mcx, GuardEntry> = PgVec::new_in(mcx);
    let mut resid: PgVec<'mcx, usize> = PgVec::new_in(mcx);
    for (transno, spec) in specs.iter().enumerate() {
        match classify_trans(spec, transno) {
            Some((t, g)) => {
                trans.push(t);
                if let Some(g) = g {
                    guards.push(g);
                }
            }
            None => resid.push(transno),
        }
    }
    if trans.is_empty() {
        return None;
    }
    let mut cols: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    for t in trans.iter() {
        if t.kind != LaneKind::CountStar && !cols.contains(&t.col) {
            cols.push(t.col);
        }
    }
    let (cse, cse_members, cse_skip) = build_cse(mcx, &trans);
    let guarded = !guards.is_empty();
    Some(LanePlan { trans, cse, cse_members, cse_skip, guards, cols, resid, guarded })
}

/// CSE schedule over classified transitions. SumBase: Sum/AvgAccum cluster by
/// (col, divk) — addend/mulk live in the per-member derivation; a CountAny
/// joins any cluster on its col (the non-null count is transform-independent),
/// else CountAnys cluster by col alone. MinMax: exact structural duplicates
/// (same kind/col/transform) share one batch scan. Groups need >= 2 members
/// (a singleton saves nothing); residual transitions never reach classify, so
/// they can't join a group.
pub fn build_cse<'mcx>(
    mcx: Mcx<'mcx>,
    trans: &[LaneTrans],
) -> (PgVec<'mcx, CseGroup>, PgVec<'mcx, u16>, PgVec<'mcx, bool>) {
    #[derive(PartialEq)]
    enum Key {
        Sum { col: u16, divk: i32 },
        Count { col: u16 },
        // res_width is part of MinMax structural identity: a bare int2 Var
        // and an int2+0 OpExpr share coefficients but store transvalues at
        // different widths.
        MinMax { max: bool, col: u16, res_width: LaneWidth, addend: i32, mulk: i32, divk: i32 },
    }
    let mut clusters: Vec<(Key, Vec<u16>)> = Vec::new();
    let mut join = |key: Key, ti: u16| match clusters.iter_mut().find(|(k, _)| *k == key) {
        Some((_, v)) => v.push(ti),
        None => clusters.push((key, vec![ti])),
    };
    for (ti, t) in trans.iter().enumerate() {
        let ti = ti as u16;
        match t.kind {
            LaneKind::Sum | LaneKind::AvgAccum => join(Key::Sum { col: t.col, divk: t.divk }, ti),
            LaneKind::Min | LaneKind::Max => join(
                Key::MinMax {
                    max: t.kind == LaneKind::Max,
                    col: t.col,
                    res_width: t.res_width,
                    addend: t.addend,
                    mulk: t.mulk,
                    divk: t.divk,
                },
                ti,
            ),
            _ => {}
        }
    }
    for (ti, t) in trans.iter().enumerate() {
        if t.kind != LaneKind::CountAny {
            continue;
        }
        let col = t.col;
        match clusters.iter_mut().find(
            |(k, _)| matches!(k, Key::Sum { col: c, .. } | Key::Count { col: c } if *c == col),
        ) {
            Some((_, v)) => v.push(ti as u16),
            None => clusters.push((Key::Count { col }, vec![ti as u16])),
        }
    }
    let mut groups: PgVec<'mcx, CseGroup> = PgVec::new_in(mcx);
    let mut members: PgVec<'mcx, u16> = PgVec::new_in(mcx);
    let mut skip: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    for _ in trans {
        skip.push(false);
    }
    for (key, tis) in clusters {
        if tis.len() < 2 {
            continue;
        }
        let kind = match key {
            Key::MinMax { .. } => CseGroupKind::MinMax,
            _ => CseGroupKind::SumBase,
        };
        let start = members.len() as u16;
        for ti in tis {
            skip[ti as usize] = true;
            members.push(ti);
        }
        groups.push(CseGroup { kind, start, len: members.len() as u16 - start });
    }
    (groups, members, skip)
}

#[inline(always)]
fn lane_value(values: &[Datum], width: LaneWidth, i: usize) -> i64 {
    match width {
        LaneWidth::I16 => values[i].as_i16() as i64,
        LaneWidth::I32 => values[i].as_i32() as i64,
        LaneWidth::I64 => values[i].as_i64(),
    }
}

// (count, sum of transformed values) over selected non-null rows. The
// addend-only shape keeps the hoisted c*addend form (one multiply per batch);
// mul/div transforms fold per row — each transformed term is int4-proven, so
// the i64 batch sum stays exact.
#[inline(always)]
fn sum_selected(t: &LaneTrans, values: &[Datum], isnull: &[bool], rows: &[u64]) -> (i64, i64) {
    let mut c = 0i64;
    let mut s = 0i64;
    if t.mulk == 1 && t.divk == 1 {
        for_each_row(rows, |i| {
            if !isnull[i] {
                c += 1;
                s = s.wrapping_add(lane_value(values, t.width, i));
            }
        });
        s = s.wrapping_add(c.wrapping_mul(t.addend as i64));
    } else {
        for_each_row(rows, |i| {
            if !isnull[i] {
                c += 1;
                s = s.wrapping_add(xform(t, lane_value(values, t.width, i)));
            }
        });
    }
    (c, s)
}

#[inline(always)]
fn count_apply(pg: &mut AggPerGroup, c: i64) {
    pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(c));
    pg.trans_value_is_null = false;
    pg.no_trans_value = false;
}

#[inline(always)]
fn sum_apply(pg: &mut AggPerGroup, delta: i64) {
    let old = if pg.trans_value_is_null { 0 } else { pg.trans_value.as_i64() };
    pg.trans_value = Datum::from_i64(old.wrapping_add(delta));
    pg.trans_value_is_null = false;
}

#[inline(always)]
fn avg_apply(pg: &mut AggPerGroup, c: i64, delta: i64) {
    assert!(!pg.trans_value_is_null, "avg transarray is never NULL");
    let arr = pg.trans_value.as_usize() as *mut u8;
    // SAFETY: aggcontext-lived transarray, shape validated.
    unsafe {
        assert!(
            ::types_tuple::varatt::varatt_is_4b_u(arr)
                && ::types_tuple::varatt::varsize_4b(arr) == INT8_TRANSARRAY_SIZE
                && arr.add(8).cast::<i32>().read() == 0,
            "expected 2-element int8 array"
        );
        let td = arr.add(ARR_OVERHEAD_NONULLS_1).cast::<i64>();
        *td = (*td).wrapping_add(c);
        *td.add(1) = (*td.add(1)).wrapping_add(delta);
    }
}

/// One {count,sum} int8[2] transarray in `mcx`, header shaped exactly as C
/// construct_array produces it (4B varlena, 1-D, dataoffset 0 = no nulls,
/// int8 elems, dim 2, lbound 1) — the AvgAccum pergroup's non-null initval.
/// The consuming node must install this before the first fold touches an
/// AvgAccum transition (avg_apply validates the shape).
pub fn new_int8_transarray(mcx: Mcx<'_>) -> Datum {
    let mut buf: PgVec<'_, u64> = ::mcx::vec_from_elem_in(mcx, 0u64, 5);
    let p = buf.as_mut_ptr().cast::<u8>();
    // SAFETY: 40 in-bounds bytes, 8-aligned; leaked into the mcx arena below.
    unsafe {
        p.cast::<u32>().write((INT8_TRANSARRAY_SIZE as u32) << 2);
        p.add(4).cast::<i32>().write(1); // ndim
        p.add(8).cast::<i32>().write(0); // dataoffset (no nulls)
        p.add(12).cast::<u32>().write(20); // elemtype = int8
        p.add(16).cast::<i32>().write(2); // dim[0]
        p.add(20).cast::<i32>().write(1); // lbound[0]
    }
    let d = Datum::from_usize(p as usize);
    core::mem::forget(buf);
    d
}

// (count, Σv') over selected non-null rows with v' = v/divk — the shared
// base accumulator every SumBase member derives from.
#[inline(always)]
fn base_sum(
    width: LaneWidth,
    divk: i64,
    values: &[Datum],
    isnull: &[bool],
    rows: &[u64],
) -> (i64, i64) {
    let mut c = 0i64;
    let mut s = 0i64;
    for_each_row(rows, |i| {
        if !isnull[i] {
            c += 1;
            let v = lane_value(values, width, i);
            s = s.wrapping_add(if divk != 1 { v / divk } else { v });
        }
    });
    (c, s)
}

// Per-member delta off the shared base: mulk*S + addend*c, bit-equal to the
// per-row Σ(v'*mulk + addend) in mod-2^64 ring arithmetic (see CseGroup).
#[inline(always)]
fn cse_delta(t: &LaneTrans, c: i64, s: i64) -> i64 {
    let s = if t.mulk != 1 { s.wrapping_mul(t.mulk as i64) } else { s };
    s.wrapping_add(c.wrapping_mul(t.addend as i64))
}

/// Whole-batch ungrouped fold: apply every admitted transition over the
/// selected rows of the staged batch, CSE groups first, then the ungrouped
/// per-trans kernels.
///
/// # Safety
/// `pergroup_base` is the node's once-allocated pergroup array covering every
/// transno in the plan; rows selected by `rows` carry valid lane values in
/// `cols` for every plan column (`rows` has one bit per staged row,
/// `nrows <= rows.len() * 64`); AvgAccum pergroups hold a live
/// `new_int8_transarray`-shaped transvalue. If the plan is guarded, the
/// caller must have run `check_guards` on this batch and gotten `Pass`.
pub unsafe fn fold_batch(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    rows: &[u64],
    nrows: usize,
    pergroup_base: NonNull<AggPerGroup>,
) {
    let nsel: u32 = rows.iter().map(|w| w.count_ones()).sum();
    for g in plan.cse.iter() {
        let members = &plan.cse_members[g.start as usize..(g.start + g.len) as usize];
        // SAFETY: transno < pergroup length (caller contract).
        let pg_of = |transno: u16| unsafe { &mut *pergroup_base.as_ptr().add(transno as usize) };
        let t0 = &plan.trans[members[0] as usize];
        match g.kind {
            CseGroupKind::SumBase => {
                // Any Sum/AvgAccum member defines the base lane read; a
                // count-only group never reads the value lane (CountAny's
                // width field is not the column's width).
                let lane = members
                    .iter()
                    .map(|&m| &plan.trans[m as usize])
                    .find(|t| t.kind != LaneKind::CountAny);
                let (c, s) = match lane {
                    Some(l) => {
                        let (values, isnull) =
                            (cols.col_values(l.col as usize), cols.col_isnull(l.col as usize));
                        base_sum(l.width, l.divk as i64, values, isnull, rows)
                    }
                    None => {
                        let isnull = cols.col_isnull(t0.col as usize);
                        let mut c = 0i64;
                        for_each_row(rows, |i| c += !isnull[i] as i64);
                        (c, 0)
                    }
                };
                for &m in members {
                    let t = &plan.trans[m as usize];
                    debug_assert_eq!(t.col, t0.col);
                    let pg = pg_of(t.transno);
                    match t.kind {
                        LaneKind::CountAny => count_apply(pg, c),
                        LaneKind::Sum if c > 0 => sum_apply(pg, cse_delta(t, c, s)),
                        LaneKind::AvgAccum if c > 0 => avg_apply(pg, c, cse_delta(t, c, s)),
                        _ => {}
                    }
                }
            }
            CseGroupKind::MinMax => {
                let (values, isnull) =
                    (cols.col_values(t0.col as usize), cols.col_isnull(t0.col as usize));
                let mut m: Option<i64> = None;
                let want_max = t0.kind == LaneKind::Max;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = xform(t0, lane_value(values, t0.width, i));
                        m = Some(match m {
                            None => v,
                            Some(p) => {
                                if want_max {
                                    p.max(v)
                                } else {
                                    p.min(v)
                                }
                            }
                        });
                    }
                });
                if let Some(v) = m {
                    for &mi in members {
                        minmax_advance(t0, pg_of(plan.trans[mi as usize].transno), v, want_max);
                    }
                }
            }
        }
    }
    for (ti, t) in plan.trans.iter().enumerate() {
        if plan.cse_skip[ti] {
            continue;
        }
        // SAFETY: transno < pergroup length (caller contract).
        let pg = unsafe { &mut *pergroup_base.as_ptr().add(t.transno as usize) };
        if t.kind == LaneKind::CountStar {
            count_apply(pg, nsel as i64);
            continue;
        }
        let (values, isnull) =
            (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize));
        debug_assert!(values.len() >= nrows && isnull.len() >= nrows);
        match t.kind {
            LaneKind::CountStar => unreachable!(),
            LaneKind::CountAny => {
                let mut c = 0i64;
                for_each_row(rows, |i| {
                    c += !isnull[i] as i64;
                });
                count_apply(pg, c);
            }
            LaneKind::Sum => {
                let (c, s) = sum_selected(t, values, isnull, rows);
                if c > 0 {
                    sum_apply(pg, s);
                }
            }
            LaneKind::AvgAccum => {
                let (c, s) = sum_selected(t, values, isnull, rows);
                if c > 0 {
                    avg_apply(pg, c, s);
                }
            }
            LaneKind::Min | LaneKind::Max => {
                let mut m: Option<i64> = None;
                let want_max = t.kind == LaneKind::Max;
                for_each_row(rows, |i| {
                    if !isnull[i] {
                        let v = xform(t, lane_value(values, t.width, i));
                        m = Some(match m {
                            None => v,
                            Some(p) => {
                                if want_max {
                                    p.max(v)
                                } else {
                                    p.min(v)
                                }
                            }
                        });
                    }
                });
                if let Some(v) = m {
                    minmax_advance(t, pg, v, want_max);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum GuardCheck {
    // All guards proven for this batch; flags say which tier fired (zone =
    // granule min/max, data = exact lane pass).
    Pass { zone: bool, data: bool },
    // Some guarded lane holds an out-of-interval selected value: the batch
    // must run the checked per-row program (which raises C's error at C's
    // row — the interval is exact, so a demoted batch always raises).
    Demote,
}

/// Per-batch data-level proof for every guarded transition. The zone bounds
/// cover the staged window's whole granule (a superset of the selected rows),
/// so a zone pass is conservative; the lane pass is exact — its failure is
/// exactly "the would-error mask is non-empty".
pub fn check_guards(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    rows: &[u64],
    zone_minmax: impl Fn(u16) -> Option<(i64, i64)>,
) -> GuardCheck {
    let mut zone = false;
    let mut data = false;
    for g in plan.guards.iter() {
        if let Some((mn, mx)) = zone_minmax(g.col) {
            if g.lo <= mn && mx <= g.hi {
                zone = true;
                continue;
            }
        }
        let values = cols.col_values(g.col as usize);
        let isnull = cols.col_isnull(g.col as usize);
        let mut ok = true;
        for_each_row(rows, |i| {
            if !isnull[i] {
                let v = lane_value(values, g.width, i);
                ok &= g.lo <= v && v <= g.hi;
            }
        });
        if !ok {
            return GuardCheck::Demote;
        }
        data = true;
    }
    GuardCheck::Pass { zone, data }
}

// Strict larger/smaller advance against the stored transvalue, at the
// transfn's result width (int4 for the int2-Var OpExpr admissions — storing
// at the lane width truncated in-range int4 results through from_i16).
#[inline(always)]
fn minmax_advance(t: &LaneTrans, pg: &mut AggPerGroup, v: i64, want_max: bool) {
    let store = |v: i64| match t.res_width {
        LaneWidth::I16 => Datum::from_i16(v as i16),
        LaneWidth::I32 => Datum::from_i32(v as i32),
        LaneWidth::I64 => Datum::from_i64(v),
    };
    if pg.no_trans_value {
        pg.trans_value = store(v);
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    } else if !pg.trans_value_is_null {
        let old = match t.res_width {
            LaneWidth::I16 => pg.trans_value.as_i16() as i64,
            LaneWidth::I32 => pg.trans_value.as_i32() as i64,
            LaneWidth::I64 => pg.trans_value.as_i64(),
        };
        let next = if want_max { old.max(v) } else { old.min(v) };
        if next != old {
            pg.trans_value = store(next);
        }
    }
}

#[inline(always)]
fn for_each_row(rows: &[u64], mut f: impl FnMut(usize)) {
    for (w, &word) in rows.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            f(w * 64 + bits.trailing_zeros() as usize);
            bits &= bits - 1;
        }
    }
}

/// Grouped fold: the probe stays per-row (the hash lookup repoints the
/// pergroup cell; the caller snapshots it per row), transitions batch per
/// pergroup-pointer lane. Per-group accumulation order within a transition is
/// batch order = plan order; across transitions the fold is transition-major,
/// bit-identical for these commutative kernels.
///
/// # Safety
/// `groups[k]` is the live pergroup array the hash lookup installed for row
/// `idxs[k]` of this batch (entries are never moved or freed within a batch;
/// spill mode only redirects NEW groups); `idxs` rows carry valid lane values
/// for every plan column; AvgAccum pergroups hold a live transarray. Guarded
/// plans require a prior `check_guards` `Pass` on this batch.
pub unsafe fn fold_rows_grouped(
    plan: &LanePlan<'_>,
    cols: &impl LaneCols,
    idxs: &[u32],
    groups: &[NonNull<AggPerGroup>],
) {
    debug_assert_eq!(idxs.len(), groups.len());
    for t in plan.trans.iter() {
        let transno = t.transno as usize;
        if t.kind == LaneKind::CountStar {
            for &g in groups {
                // SAFETY: caller contract.
                let pg = unsafe { &mut *g.as_ptr().add(transno) };
                pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                pg.trans_value_is_null = false;
                pg.no_trans_value = false;
            }
            continue;
        }
        let (values, isnull) =
            (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize));
        for (&i, &g) in idxs.iter().zip(groups.iter()) {
            let i = i as usize;
            if isnull[i] {
                continue;
            }
            // SAFETY: caller contract.
            let pg = unsafe { &mut *g.as_ptr().add(transno) };
            let v = xform(t, lane_value(values, t.width, i));
            match t.kind {
                LaneKind::CountStar => unreachable!(),
                LaneKind::CountAny => count_apply(pg, 1),
                LaneKind::Sum => sum_apply(pg, v),
                LaneKind::AvgAccum => avg_apply(pg, 1, v),
                LaneKind::Min | LaneKind::Max => {
                    minmax_advance(t, pg, v, t.kind == LaneKind::Max);
                }
            }
        }
    }
}
