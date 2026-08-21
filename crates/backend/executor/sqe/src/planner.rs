//! sqe planner — three phases per ENGINE-PLAN's three-tier decomposition:
//!
//!   1. LOWER: the authoring plan (APlan — built by the rig's RON table
//!      or SQL front end; the server's plan-tree lowering at P2-1) into
//!      the engine PlanNode.
//!   2. REWRITE (tier 1): a DESTRUCTIVE normalization pass OUTSIDE any
//!      memo structure — answer-plane substitution, the frame/residue
//!      split (a stats election), and the condcache value-class election.
//!   3. ELECT (tier 2): physical attributes as FUNCTIONS over bank stats
//!      (partition law, agg tier, claim class) — never constants.
//!
//! P1-1 reshapes (port-study): refusals are the typed `Refuse` enum, not
//! `None` (risks.md §6); literal typing and the render-op elections come
//! from TypMeta, never from column names (`is_date_col` is dead) or from
//! literal string shape (`parse_const` no longer guesses); fingerprints
//! are computed typed values — the RON `fp` override strings are ignored
//! (both authoring paths now canonicalize to the same typed constant, so
//! the override has nothing left to fix; recorded deviation).

use crate::bank::Bank;
use crate::engine::Faces;
use crate::ir;
use crate::ir::{AggOp, ClaimClass, CmpOp, OrderBy, PlanNode, PredSpec, PredTerm};
use crate::refuse::Refuse;
use crate::statsview::StatsView;
use crate::typmeta::{oids, TypMeta, COLLATION_C};
use crate::witness::Witness;

// ---------------------------------------------------------------------------
// authoring-side plan types (the APlan vocabulary — fed by the rig's RON
// table / SQL lowerer today, the P2-1 plan-tree lowering tomorrow). The
// serde derives ride the rig feature only; the engine never parses RON.
// ---------------------------------------------------------------------------

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AFamily {
    MetadataAnswer,
    FusedFilterAgg,
    DenseDomainGroup,
    TwoLevelCodeAgg,
    HashPlaneOwnedGroup,
    DistinctPipeline,
    VerdictBitmapContains,
    ZoneOrderWalk,
    FrameWalkCodeGroup,
    WindowReplay,
    SurvivorGather,
    DerivedKeyFold,
    ScanServe,
    /// [sortgrp v1] tier-2 order-sensitive/holistic aggregates.
    SortGrouped,
    /// [winserve v1] SQL window functions over a served scan child.
    WindowServe,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum APred {
    And { legs: Vec<APred> },
    Not { leg: Box<APred> },
    NeZero { col: u32, fp: Option<String> },
    EqZero { col: u32, fp: Option<String> },
    /// General `col <> const` (rung B server lowering; NeZero stays the
    /// zero-witness authoring form).
    Ne { col: u32, val: String, fp: Option<String> },
    NeEmpty { col: u32, fp: Option<String> },
    Eq { col: u32, val: String, fp: Option<String> },
    In { col: u32, vals: Vec<String>, fp: Option<String> },
    Range { col: u32, lo: String, hi: String, fp: Option<String> },
    /// [packedpred] mantissa comparison over a PackedNumeric{`scale`}
    /// word lane: `lo`/`hi` are EXACT mantissas at `scale` (the seam's
    /// directional-rescale law — never a rounded compare). `op` ∈
    /// {eq,ne,range}. Lowering verifies the WITNESSED face scale equals
    /// the authored one; a demoted or re-scaled bank refuses typed
    /// (face law) instead of comparing mantissas at the wrong scale.
    /// Packed mantissa order == cmp_numerics order at a shared scale
    /// (the M3-E zone-soundness pin), so the ordinary word PredTerm +
    /// zone plane serve the compare exactly.
    Packed { col: u32, op: String, lo: String, hi: String, scale: i32, fp: Option<String> },
    Contains { col: u32, needle: String, fp: Option<String> },
    /// [R6d] general `col LIKE 'pattern'` / `col NOT LIKE 'pattern'`
    /// (backslash escape convention). The `%x%` class normalizes to
    /// Contains/NotContains at lowering (fingerprint stability).
    Like { col: u32, pattern: String, not: bool, fp: Option<String> },
    /// [type-vocab] byte-order comparison against a constant image
    /// (memcmp law). `op` ∈ {eq,ne,lt,le,gt,ge}; `val_hex` is the
    /// constant image, hex-encoded (byte fidelity through the String
    /// authoring surface). Admitted on Fixed faces whose image length
    /// matches (name: 64) and on C-collated varlena faces — memcmp IS
    /// the comparison law there; everything else refuses typed.
    CmpBytes { col: u32, op: String, val_hex: String, fp: Option<String> },
    /// [sqe-bpchar] byte-equality IN list: TRUE iff the column bytes
    /// equal ANY listed image (hex-encoded, same byte-fidelity authoring
    /// surface as CmpBytes). Same soundness gate as CmpBytes(eq): byte
    /// equality must BE the type's equality — C-collated varlena faces
    /// only (the seam's pad-aware bpchar law pads each image to the
    /// column's declared width before authoring). Arity-capped.
    InBytes { col: u32, vals_hex: Vec<String>, fp: Option<String> },
    /// [tpch-expr] byte-prefix IN (seam-proven char-prefix law).
    InPrefixBytes { col: u32, vals_hex: Vec<String>, fp: Option<String> },
    /// [colcmp] `col_a OP col_b` between two word columns of one scan
    /// (`op` ∈ {lt,le,gt,ge,eq,ne}). Served at the join grain as a side
    /// row-residue; single-relation lowering refuses it typed.
    ColCmp { a: u32, b: u32, op: String, fp: Option<String> },
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum AKeyExpr {
    Col(u32),
    Const1,
    Minute { col: u32 },
    TruncMinute { col: u32 },
    HostRegex { col: u32 },
    MinusConst { col: u32, k: i64 },
    CaseSrc { se: u32, adv: u32, referer: u32 },
    /// `off_s` None = zone offset pending the post-open domain proof
    /// (the shell resolves or refuses before planning).
    HourBucket { col: u32, off_s: Option<i64> },
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum AValExpr {
    Col(u32),
    OctetLength { col: u32 },
    /// `length(text)` — UTF-8 CHARACTER count (PG textlen). Lowers to
    /// AggOp::AvgCharLen under AVG; same shape law as OctetLength.
    CharLength { col: u32 },
    /// [P4-1] col + k (col - k authors k negated; k + col commutes here).
    /// `w` = the PG add/sub op's RESULT width (2/4/8) — the overflow-
    /// proof obligation (see ir::FoldExpr's overflow law).
    AddK { col: u32, k: i64, w: u8 },
    /// [P4-1] a * b over signed word columns.
    MulCC { a: u32, b: u32, w: u8 },
    /// [P4-1] a * (k - b) — the TPC-H revenue shape. `wi` = the inner
    /// subtraction's result width.
    MulKSub { a: u32, k: i64, b: u32, w: u8, wi: u8 },
    /// [scale-alg] `a · (k ± b)` over PackedNumeric word lanes (the
    /// WITNESSED FIXED-POINT ALGEBRA ruling): `sa`/`sb` are the
    /// RECOGNITION-grade declared typmod scales, `k` an EXACT mantissa
    /// at `sb` (seam digit walk, const dscale <= sb — so PG's inner
    /// add/sub dscale IS sb and the product's is sa+sb). Lowering is
    /// the witness gate: each face must be PackedNumeric at the
    /// authored scale AND the exact mantissa domains must prove the
    /// per-row product fits the i64 fold word — anything unproven is a
    /// typed refusal, never a wrapped fold. Plain `a·b` authors k=0,
    /// sub=false.
    PackedMulK { a: u32, sa: i32, k: i64, sub: bool, b: u32, sb: i32 },
    /// [scale-alg] `a · (k1 ± b) · (k2 ± c)` — the revenue×tax shape;
    /// render scale sa+sb+sc. The witness proves the INTERMEDIATE
    /// product (kernel's left-fold order) fits i64 too.
    PackedMulK2 {
        a: u32,
        sa: i32,
        k1: i64,
        sub1: bool,
        b: u32,
        sb: i32,
        k2: i64,
        sub2: bool,
        c: u32,
        sc: i32,
    },
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum AAgg {
    CountStar,
    Sum { e: AValExpr },
    Avg { e: AValExpr },
    Min { e: AValExpr },
    Max { e: AValExpr },
    MinBytes { e: AValExpr },
    CountDistinct { e: AValExpr },
    SumShifted { col: u32, k0: u64, n: u64 },
    /// The variance family over int words (var_samp = variance,
    /// stddev_samp = stddev — PG's historical aliases lower here too).
    VarSamp { e: AValExpr },
    VarPop { e: AValExpr },
    StddevSamp { e: AValExpr },
    StddevPop { e: AValExpr },
    /// bit_and/bit_or over int words. (bool_and/bool_or/every are NOT
    /// authoring ops: they lower to Min/Max over the bool 0/1 word lane
    /// at recognition — the existing fold law serves them whole.)
    BitAnd { e: AValExpr },
    BitOr { e: AValExpr },
    /// Emit the surviving rows' values of a column (row-returning scans:
    /// one leg per output column, answer columns in leg order).
    Emit { e: AValExpr },
    /// [sortgrp v1] string_agg(col, const-delim ORDER BY <APlan.sortagg
    /// keys>). `delim` None = SQL NULL delimiter (plain concatenation).
    StringAgg { col: u32, delim: Option<Vec<u8>> },
    /// [sortgrp v1] array_agg(col ORDER BY <APlan.sortagg keys>).
    ArrayAgg { col: u32 },
    /// [sortgrp v1] percentile_disc(frac) WITHIN GROUP (ORDER BY col).
    PercentileDisc { col: u32, frac: f64 },
    /// [sortgrp v1] percentile_cont(frac) WITHIN GROUP (ORDER BY col)
    /// — col int2/4/8, cast to float8 by the query (i2tod/i4tod/i8tod).
    PercentileCont { col: u32, frac: f64 },
    /// [sortgrp v1] mode() WITHIN GROUP (ORDER BY col).
    Mode { col: u32 },
    /// [aggqual] sum(DISTINCT col) — the per-group distinct value set
    /// feeds the sum fold.
    SumDistinct { e: AValExpr },
    /// [aggqual] avg(DISTINCT col) = distinct-sum / distinct-count.
    AvgDistinct { e: AValExpr },
    /// [aggqual] array_agg(DISTINCT col ORDER BY col) — SortGrouped's
    /// sorted run with adjacent-equal dedup.
    ArrayAggDistinct { col: u32 },
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum AOrderBy {
    CountDesc,
    KeyAsc,
    ColAsc { col: u32 },
    ColThenColAsc { a: u32, b: u32 },
    AggDesc { idx: u32 },
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct AOrderSpec {
    pub by: AOrderBy,
    pub limit: Option<u32>,
    pub offset: u32,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum ACmpOp {
    Gt,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct AHaving {
    pub agg: AAgg,
    pub op: ACmpOp,
    pub val: u64,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum AParamV {
    Fixed(i64),
    FixedStr(String),
    Elect { law: String, inputs: Vec<String> },
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct AParam {
    pub name: String,
    pub v: AParamV,
}

/// [sortgrp v1] One statement-level agg ORDER BY key over an INPUT column.
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ASortKey {
    pub col: u32,
    pub desc: bool,
    pub nulls_first: bool,
}

/// [winserve v1] One window function column in authoring form.
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AWinOp {
    RowNumber,
    Rank,
    DenseRank,
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
    Lead,
    Lag,
    FirstValue,
    LastValue,
    NthValue,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AWinFunc {
    pub op: AWinOp,
    /// Input column (None for the rank family and count(*)).
    pub col: Option<u32>,
    /// [winframes v2] lead/lag offset / nth_value's n; None = the op's
    /// default (lead/lag/nth: 1).
    #[cfg_attr(feature = "rig", serde(default))]
    pub off: Option<i64>,
}

/// [winframes v2] Authoring frame bound (offsets in the mode's domain;
/// Range offsets pre-scaled by the seam into the key-embed x scale
/// domain, saturating i64).
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AFrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AFrameMode {
    Rows,
    Range,
    Groups,
}

/// [winv3] Authoring EXCLUDE clause (per `ir::FrameExclusion`).
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AFrameExclusion {
    CurrentRow,
    Group,
    Ties,
}

/// [winframes v2] Authoring frame. `scale`/`band` per `ir::FrameSpec`
/// (both in the SCALED offset domain — datetime keys: usec).
/// [winv3] `cal_start`/`cal_end` = calendar (month-carrying) interval
/// offsets as (months, usecs); the matching bound then carries the
/// CONSERVATIVE magnitude (months <= 31 days) for band admission.
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AFrame {
    pub mode: AFrameMode,
    pub start: AFrameBound,
    pub end: AFrameBound,
    #[cfg_attr(feature = "rig", serde(default))]
    pub scale: Option<i64>,
    #[cfg_attr(feature = "rig", serde(default))]
    pub band: Option<(i64, i64)>,
    #[cfg_attr(feature = "rig", serde(default))]
    pub exclusion: Option<AFrameExclusion>,
    #[cfg_attr(feature = "rig", serde(default))]
    pub cal_start: Option<(i32, i64)>,
    #[cfg_attr(feature = "rig", serde(default))]
    pub cal_end: Option<(i32, i64)>,
}

/// [winv4] Authoring runCondition comparison (wfunc-on-left normal
/// form; the seam commutes const-on-left plans before authoring).
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ARunCmp {
    Lt,
    Le,
    Eq,
    Ge,
    Gt,
}

/// [winv4] One authoring runCondition leg: `funcs[func] OP val`.
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ARunCond {
    pub func: usize,
    pub op: ARunCmp,
    pub val: i64,
}

/// [winv4] The authoring 2-chain upper spec: ORDER BY = the first
/// `n_ord` keys of the bottom spec's `order`; same PARTITION BY.
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct AWinChain {
    pub n_ord: usize,
    #[cfg_attr(feature = "rig", serde(default))]
    pub frame: Option<AFrame>,
    pub funcs: Vec<AWinFunc>,
}

/// [winserve v1] The WindowServe statement spec: PARTITION BY columns,
/// window ORDER BY keys (with the child Sort's directions), the window
/// function list, and the pass-through output columns (answer order:
/// `emit` columns then `funcs` columns, [winv4] then `chain.funcs`).
#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct AWindow {
    pub part_cols: Vec<u32>,
    pub order: Vec<ASortKey>,
    pub funcs: Vec<AWinFunc>,
    pub emit: Vec<u32>,
    /// [winframes v2] None = the SQL default frame.
    #[cfg_attr(feature = "rig", serde(default))]
    pub frame: Option<AFrame>,
    /// [winv4] runCondition legs (ANDed; refuse with `chain`).
    #[cfg_attr(feature = "rig", serde(default))]
    pub run_conds: Vec<ARunCond>,
    /// [winv4] The stacked upper window spec (2-chain).
    #[cfg_attr(feature = "rig", serde(default))]
    pub chain: Option<AWinChain>,
}

/// [winv4] Lower one authoring function list against a type oracle
/// (`typ_of` — bank columns for the scan lane, answer columns for the
/// over-answer lane) under one spec level's tie-witness scope.
fn lower_awin_funcs(
    typ_of: &dyn Fn(u32) -> TypMeta,
    check_coll: &dyn Fn(u32) -> Result<(), Refuse>,
    part_cols: &[u32],
    order: &[ASortKey],
    afuncs: &[AWinFunc],
) -> Result<Vec<ir::WinFuncSpec>, Refuse> {
    let mut funcs = Vec::with_capacity(afuncs.len());
    for f in afuncs {
        let op = match f.op {
            AWinOp::RowNumber => ir::WinOp::RowNumber,
            AWinOp::Rank => ir::WinOp::Rank,
            AWinOp::DenseRank => ir::WinOp::DenseRank,
            AWinOp::CountStar => ir::WinOp::CountStar,
            AWinOp::Count => ir::WinOp::Count,
            AWinOp::Sum => ir::WinOp::Sum,
            AWinOp::Min => ir::WinOp::Min,
            AWinOp::Max => ir::WinOp::Max,
            AWinOp::Avg => ir::WinOp::Avg,
            AWinOp::Lead => ir::WinOp::Lead,
            AWinOp::Lag => ir::WinOp::Lag,
            AWinOp::FirstValue => ir::WinOp::FirstValue,
            AWinOp::LastValue => ir::WinOp::LastValue,
            AWinOp::NthValue => ir::WinOp::NthValue,
        };
        let needs_col = !matches!(
            op,
            ir::WinOp::RowNumber | ir::WinOp::Rank | ir::WinOp::DenseRank
                | ir::WinOp::CountStar
        );
        if needs_col != f.col.is_some() {
            return Err(Refuse::WinUnsupported { what: "func-arg-shape" });
        }
        if let Some(c) = f.col {
            if typ_of(c).is_varlena() {
                check_coll(c)?;
            }
        }
        // [winframes v2] tie law: value-function reads
        // (lead/lag/first/last/nth) answer ONE row's cell at a
        // POSITION — on tied order keys the position-to-row pairing
        // is PG-order-dependent (its qsort tie class, not ours).
        // The plan-time witness that the VALUE is tie-invariant:
        // the argument column is itself a window order key (its
        // value is equal across any peer permutation) or a
        // partition key (constant across the partition). Anything
        // else refuses typed (the heap executor owns PG's own tie
        // order).
        if matches!(
            op,
            ir::WinOp::Lead
                | ir::WinOp::Lag
                | ir::WinOp::FirstValue
                | ir::WinOp::LastValue
                | ir::WinOp::NthValue
        ) {
            let c = f.col.ok_or(Refuse::WinUnsupported { what: "func-arg-shape" })?;
            if !order.iter().any(|k| k.col == c) && !part_cols.contains(&c) {
                return Err(Refuse::WinUnsupported { what: "value-read-tie-witness" });
            }
        }
        let mut spec = ir::WinFuncSpec::new(op, f.col, f.col.map(typ_of));
        if let Some(off) = f.off {
            if !matches!(op, ir::WinOp::Lead | ir::WinOp::Lag | ir::WinOp::NthValue) {
                return Err(Refuse::WinUnsupported { what: "func-arg-shape" });
            }
            // nth_value's n >= 1 (C errors on n <= 0 — the refused
            // statement reproduces that error on the heap executor).
            if op == ir::WinOp::NthValue && off < 1 {
                return Err(Refuse::WinUnsupported { what: "func-arg-shape" });
            }
            spec = ir::WinFuncSpec::with_off(op, f.col, f.col.map(typ_of), off);
        }
        funcs.push(spec);
    }
    Ok(funcs)
}

/// [winframes v2] Lower one authoring frame against a type oracle:
/// non-negative offsets only (C errors on negatives); Range offsets
/// need exactly ONE word-comparable order key (C asserts ordNumCols ==
/// 1) — varlena keys have no in_range vocabulary.
fn lower_awin_frame(
    typ_of: &dyn Fn(u32) -> TypMeta,
    order: &[ASortKey],
    aframe: &Option<AFrame>,
) -> Result<ir::FrameSpec, Refuse> {
    let af = match aframe {
        None => return Ok(ir::FrameSpec::default()),
        Some(af) => af,
    };
    let bound = |b: &AFrameBound| -> Result<ir::FrameBound, Refuse> {
        Ok(match b {
            AFrameBound::UnboundedPreceding => ir::FrameBound::UnboundedPreceding,
            AFrameBound::CurrentRow => ir::FrameBound::CurrentRow,
            AFrameBound::UnboundedFollowing => ir::FrameBound::UnboundedFollowing,
            AFrameBound::Preceding(o) => {
                if *o < 0 {
                    return Err(Refuse::WinUnsupported { what: "frame-offset" });
                }
                ir::FrameBound::Preceding(*o as i128)
            }
            AFrameBound::Following(o) => {
                if *o < 0 {
                    return Err(Refuse::WinUnsupported { what: "frame-offset" });
                }
                ir::FrameBound::Following(*o as i128)
            }
        })
    };
    let mode = match af.mode {
        AFrameMode::Rows => ir::FrameMode::Rows,
        AFrameMode::Range => ir::FrameMode::Range,
        AFrameMode::Groups => ir::FrameMode::Groups,
    };
    let (start, end) = (bound(&af.start)?, bound(&af.end)?);
    let offsets = matches!(
        start,
        ir::FrameBound::Preceding(_) | ir::FrameBound::Following(_)
    ) || matches!(
        end,
        ir::FrameBound::Preceding(_) | ir::FrameBound::Following(_)
    );
    // A frame that can only START after it ENDS is a parse
    // error in C — fail closed.
    if matches!(start, ir::FrameBound::UnboundedFollowing)
        || matches!(end, ir::FrameBound::UnboundedPreceding)
    {
        return Err(Refuse::WinUnsupported { what: "frame-bound" });
    }
    if mode == ir::FrameMode::Range && offsets {
        if order.len() != 1 {
            return Err(Refuse::WinUnsupported { what: "range-offset-keys" });
        }
        if typ_of(order[0].col).is_varlena() {
            return Err(Refuse::WinUnsupported { what: "range-offset-key-type" });
        }
    }
    if mode == ir::FrameMode::Groups && offsets && order.is_empty() {
        return Err(Refuse::WinUnsupported { what: "groups-offset-keys" });
    }
    let scale = af.scale.unwrap_or(1);
    if scale < 1 {
        return Err(Refuse::WinUnsupported { what: "frame-scale" });
    }
    // [winv3] Calendar offsets: Range mode over ONE datetime
    // key with a witnessed band only, and only on a bound
    // that IS an offset bound (the linear payload there is
    // the conservative band magnitude).
    if af.cal_start.is_some() || af.cal_end.is_some() {
        if mode != ir::FrameMode::Range || af.band.is_none() {
            return Err(Refuse::WinUnsupported { what: "frame-cal-shape" });
        }
        if af.cal_start.is_some()
            && !matches!(
                start,
                ir::FrameBound::Preceding(_) | ir::FrameBound::Following(_)
            )
        {
            return Err(Refuse::WinUnsupported { what: "frame-cal-shape" });
        }
        if af.cal_end.is_some()
            && !matches!(
                end,
                ir::FrameBound::Preceding(_) | ir::FrameBound::Following(_)
            )
        {
            return Err(Refuse::WinUnsupported { what: "frame-cal-shape" });
        }
    }
    Ok(ir::FrameSpec {
        mode,
        start,
        end,
        scale: scale as i128,
        band: af.band.map(|(lo, hi)| (lo as i128, hi as i128)),
        exclusion: match af.exclusion {
            None => ir::FrameExclusion::None,
            Some(AFrameExclusion::CurrentRow) => ir::FrameExclusion::CurrentRow,
            Some(AFrameExclusion::Group) => ir::FrameExclusion::Group,
            Some(AFrameExclusion::Ties) => ir::FrameExclusion::Ties,
        },
        cal_start: af.cal_start.map(|(m, u)| ir::CalOff { months: m, usecs: u }),
        cal_end: af.cal_end.map(|(m, u)| ir::CalOff { months: m, usecs: u }),
    })
}

/// [winv4] Lower one full AWindow (both chain levels, run conditions)
/// against a type oracle. Shared by the scan lane (`plan_from_ap`,
/// oracle = bank column types) and the over-answer lane (the seam's
/// window-over-Agg hop, oracle = child answer column types).
pub fn lower_awindow(
    typ_of: &dyn Fn(u32) -> TypMeta,
    check_coll: &dyn Fn(u32) -> Result<(), Refuse>,
    w: &AWindow,
) -> Result<ir::WindowSpec, Refuse> {
    if w.funcs.is_empty() {
        return Err(Refuse::WinUnsupported { what: "no-funcs" });
    }
    // C-collation admission for every varlena cell the family
    // hashes/compares/emits.
    for &c in w.part_cols.iter().chain(w.emit.iter()) {
        if typ_of(c).is_varlena() {
            check_coll(c)?;
        }
    }
    for k in &w.order {
        if typ_of(k.col).is_varlena() {
            check_coll(k.col)?;
        }
    }
    let funcs = lower_awin_funcs(typ_of, check_coll, &w.part_cols, &w.order, &w.funcs)?;
    let frame = lower_awin_frame(typ_of, &w.order, &w.frame)?;
    // [winv4] The 2-chain upper level: same partition, order prefix,
    // its own frame/functions (validated under ITS order scope).
    let chain = match &w.chain {
        None => None,
        Some(c) => {
            if c.n_ord > w.order.len() {
                return Err(Refuse::WinUnsupported { what: "chain-order-prefix" });
            }
            if c.funcs.is_empty() {
                return Err(Refuse::WinUnsupported { what: "no-funcs" });
            }
            let ord = &w.order[..c.n_ord];
            Some(ir::WinChain {
                n_ord: c.n_ord,
                frame: lower_awin_frame(typ_of, ord, &c.frame)?,
                funcs: lower_awin_funcs(typ_of, check_coll, &w.part_cols, ord, &c.funcs)?,
            })
        }
    };
    // [winv4] runCondition legs: chain-free statements only (C's
    // non-top pass-through modes are outside the served vocabulary);
    // the target function must answer the FVal::N (rank/count) class —
    // exactly the monotonic vocabulary PG builds run conditions for.
    let mut run_conds = Vec::with_capacity(w.run_conds.len());
    if !w.run_conds.is_empty() && chain.is_some() {
        return Err(Refuse::WinUnsupported { what: "run-cond-chain" });
    }
    for rc in &w.run_conds {
        let Some(f) = funcs.get(rc.func) else {
            return Err(Refuse::WinUnsupported { what: "run-cond-target" });
        };
        if !matches!(
            f.op,
            ir::WinOp::RowNumber
                | ir::WinOp::Rank
                | ir::WinOp::DenseRank
                | ir::WinOp::CountStar
                | ir::WinOp::Count
        ) {
            return Err(Refuse::WinUnsupported { what: "run-cond-target" });
        }
        run_conds.push(ir::WinRunCond {
            func: rc.func,
            op: match rc.op {
                ARunCmp::Lt => ir::RunCmp::Lt,
                ARunCmp::Le => ir::RunCmp::Le,
                ARunCmp::Eq => ir::RunCmp::Eq,
                ARunCmp::Ge => ir::RunCmp::Ge,
                ARunCmp::Gt => ir::RunCmp::Gt,
            },
            val: rc.val,
        });
    }
    Ok(ir::WindowSpec {
        part_cols: w.part_cols.clone(),
        order: w
            .order
            .iter()
            .map(|k| ir::TopKKey { col: k.col, desc: k.desc, nulls_first: k.nulls_first, lo: None, trim: false })
            .collect(),
        funcs,
        emit: w.emit.clone(),
        frame,
        run_conds,
        chain,
    })
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug)]
pub struct APlan {
    pub q: u32,
    pub family: AFamily,
    #[cfg_attr(feature = "rig", serde(default))]
    pub tags: Vec<AFamily>,
    pub cols: Vec<u32>,
    pub pred: Option<APred>,
    pub group: Vec<AKeyExpr>,
    pub agg: Vec<AAgg>,
    pub order: Option<AOrderSpec>,
    /// [sortgrp v1] The SortGrouped statement sort spec over INPUT
    /// columns (attnos). Empty for every other family.
    #[cfg_attr(feature = "rig", serde(default))]
    pub sortagg_keys: Vec<ASortKey>,
    /// [winserve v1] The WindowServe spec. None for every other family.
    #[cfg_attr(feature = "rig", serde(default))]
    pub win: Option<AWindow>,
    /// [aggqual] Per-aggregate FILTER (WHERE ...) predicates, index-
    /// aligned with `agg` when non-empty (empty = no leg filters).
    /// Int-conjunct vocabulary only; anything else refuses at lowering.
    #[cfg_attr(feature = "rig", serde(default))]
    pub agg_filters: Vec<Option<APred>>,
    #[cfg_attr(feature = "rig", serde(default))]
    pub having: Option<AHaving>,
    pub params: Vec<AParam>,
    pub fingerprints: Vec<String>,
    pub kernel_oracle: String,
    pub notes: String,
    pub flags: Vec<String>,
}

#[cfg_attr(feature = "rig", derive(serde::Deserialize))]
#[derive(Debug)]
pub struct APlans {
    pub version: u32,
    pub attno_map: String,
    pub plans: Vec<APlan>,
}

// ---------------------------------------------------------------------------
// phase 1+2: lower + tier-1 rewrite (destructive, no memo)
// ---------------------------------------------------------------------------

/// Literal → typed datum, decided by the COLUMN's TypMeta (never by
/// string shape — the PoC's guess-a-date is dead). DATE columns accept
/// "YYYY-MM-DD" or a raw datum integer; word columns accept integers.
fn parse_const(bank: &Bank, col: u32, s: &str) -> Result<i64, Refuse> {
    let ty = bank.typ(col);
    if let Ok(v) = s.parse::<i64>() {
        return Ok(v);
    }
    if ty.oid == oids::DATE {
        let b: Vec<&str> = s.split('-').collect();
        if b.len() == 3 {
            if let (Ok(y), Ok(m), Ok(d)) = (b[0].parse(), b[1].parse(), b[2].parse()) {
                return Ok(crate::kernels_f6::pg_date(y, m, d));
            }
        }
    }
    Err(Refuse::ConstUnparsable { col, text: s.to_string() })
}

/// Collation admission for a text-typed operation (plan §6): the engine's
/// byte-ordered text machinery implements C collation only.
fn check_collation(bank: &Bank, col: u32) -> Result<TypMeta, Refuse> {
    let ty = bank.typ(col);
    if ty.is_varlena() && ty.collation != COLLATION_C {
        return Err(Refuse::CollationUnsupported { attno: col, collation: ty.collation });
    }
    Ok(ty)
}

/// Flatten the authoring predicate tree into int-comparison conjuncts +
/// varlena terms + the drop-empty-key marker. The RON `fp` overrides are
/// ignored: fingerprints are computed typed values (see module doc).
fn lower_pred(
    bank: &Bank,
    p: &APred,
    terms: &mut Vec<PredTerm>,
    var_terms: &mut Vec<ir::VarPredTerm>,
    drop_empty: &mut Vec<u32>,
) -> Result<(), Refuse> {
    match p {
        APred::And { legs } => {
            for l in legs {
                lower_pred(bank, l, terms, var_terms, drop_empty)?;
            }
        }
        APred::NeZero { col, .. } => {
            terms.push(PredTerm::new(*col, CmpOp::Ne, 0, 0, bank.typ(*col)))
        }
        APred::EqZero { col, .. } => {
            terms.push(PredTerm::new(*col, CmpOp::Eq, 0, 0, bank.typ(*col)))
        }
        APred::Eq { col, val, .. } => terms.push(PredTerm::new(
            *col,
            CmpOp::Eq,
            parse_const(bank, *col, val)?,
            0,
            bank.typ(*col),
        )),
        APred::Ne { col, val, .. } => terms.push(PredTerm::new(
            *col,
            CmpOp::Ne,
            parse_const(bank, *col, val)?,
            0,
            bank.typ(*col),
        )),
        APred::Range { col, lo, hi, .. } => terms.push(PredTerm::new(
            *col,
            CmpOp::Between,
            parse_const(bank, *col, lo)?,
            parse_const(bank, *col, hi)?,
            bank.typ(*col),
        )),
        // [packedpred] mantissa compare over a witnessed PackedNumeric
        // lane. The face check here is the WITNESS gate: the authored
        // scale must equal the storage-witnessed scale or the statement
        // refuses typed (a republish that demotes/re-scales the lane is
        // caught by engine currency — this lowering re-runs against the
        // fresh bank). The fingerprint carries no scale on purpose: the
        // verdict planes are ENGINE-scoped and a column's witnessed
        // scale is fixed for an engine's lifetime.
        APred::Packed { col, op, lo, hi, scale, .. } => {
            match bank.face(*col) {
                crate::bank::Face::PackedNumeric { scale: s } if s == *scale => {}
                _ => {
                    return Err(Refuse::FaceUnsupported {
                        attno: *col,
                        what: "packed-pred-scale",
                    })
                }
            }
            let parse = |s: &str| -> Result<i64, Refuse> {
                s.parse::<i64>().map_err(|_| Refuse::ConstUnparsable {
                    col: *col,
                    text: s.to_string(),
                })
            };
            let (cmp, l, h) = match op.as_str() {
                "eq" => (CmpOp::Eq, parse(lo)?, 0),
                "ne" => (CmpOp::Ne, parse(lo)?, 0),
                "range" => (CmpOp::Between, parse(lo)?, parse(hi)?),
                _ => return Err(Refuse::PredUnsupported { what: "packed-pred-op" }),
            };
            terms.push(PredTerm::new(*col, cmp, l, h, bank.typ(*col)));
        }
        APred::NeEmpty { col, .. } => {
            check_collation(bank, *col)?;
            drop_empty.push(*col);
        }
        // [famB M1] varlena predicates (verdict-bitmap lowering) + IN.
        APred::Contains { col, needle, .. } => {
            let ty = check_collation(bank, *col)?;
            var_terms.push(ir::VarPredTerm::new(
                *col,
                ir::VarOp::Contains,
                needle.as_bytes().to_vec(),
                ty,
            ))
        }
        // [R6d] LIKE lowering: validate, then normalize by pattern class —
        // `%x%` joins the EXISTING Contains/NotContains identity (shared
        // condcache planes); every other pattern is the general
        // entry-decidable matcher (like.rs). C collation only.
        APred::Like { col, pattern, not, .. } => {
            let ty = check_collation(bank, *col)?;
            let pat = pattern.as_bytes();
            crate::like::like_valid(pat)
                .map_err(|what| Refuse::PredUnsupported { what })?;
            let (op, needle) = match crate::like::classify(pat) {
                crate::like::LikeClass::Contains(inner) => (
                    if *not { ir::VarOp::NotContains } else { ir::VarOp::Contains },
                    inner,
                ),
                crate::like::LikeClass::General => (
                    if *not { ir::VarOp::NotLike } else { ir::VarOp::Like },
                    pat.to_vec(),
                ),
            };
            var_terms.push(ir::VarPredTerm::new(*col, op, needle, ty))
        }
        APred::Not { leg } => match &**leg {
            APred::Contains { col, needle, .. } => {
                let ty = check_collation(bank, *col)?;
                var_terms.push(ir::VarPredTerm::new(
                    *col,
                    ir::VarOp::NotContains,
                    needle.as_bytes().to_vec(),
                    ty,
                ))
            }
            // NOT over LIKE: flip the complement flag, same lowering.
            APred::Like { col, pattern, not, fp } => {
                let flipped = APred::Like {
                    col: *col,
                    pattern: pattern.clone(),
                    not: !*not,
                    fp: fp.clone(),
                };
                lower_pred(bank, &flipped, terms, var_terms, drop_empty)?;
            }
            _ => return Err(Refuse::PredUnsupported { what: "not-over-non-contains" }),
        },
        // [type-vocab] byte-order comparison (memcmp law). Face gate:
        // Fixed images compare whole (needle length must equal the face
        // length — name consts are NUL-padded to 64 by the seam);
        // varlena payloads compare under C collation only.
        APred::CmpBytes { col, op, val_hex, .. } => {
            // "<op>-trim" = the [bpchar-order] trimmed-basis compare:
            // rtrim_blanks(col) vs an already-trimmed needle (varlena
            // faces only — a fixed face never stores bpchar).
            let (opc, trim) = match op.as_str() {
                "eq" => (ir::BytesCmp::Eq, false),
                "ne" => (ir::BytesCmp::Ne, false),
                "lt" => (ir::BytesCmp::Lt, false),
                "le" => (ir::BytesCmp::Le, false),
                "gt" => (ir::BytesCmp::Gt, false),
                "ge" => (ir::BytesCmp::Ge, false),
                "lt-trim" => (ir::BytesCmp::Lt, true),
                "le-trim" => (ir::BytesCmp::Le, true),
                "gt-trim" => (ir::BytesCmp::Gt, true),
                "ge-trim" => (ir::BytesCmp::Ge, true),
                _ => return Err(Refuse::PredUnsupported { what: "bytes-cmp-op" }),
            };
            let needle = parse_hex(val_hex)
                .ok_or(Refuse::PredUnsupported { what: "bytes-cmp-hex" })?;
            if trim && needle.last() == Some(&b' ') {
                return Err(Refuse::PredUnsupported { what: "bytes-cmp-trim-needle" });
            }
            let ty = match bank.face(*col) {
                crate::bank::Face::Fixed(_) if trim => {
                    return Err(Refuse::FaceUnsupported { attno: *col, what: "bytes-cmp-face" })
                }
                crate::bank::Face::Fixed(len) if needle.len() == len as usize => bank.typ(*col),
                crate::bank::Face::Fixed(_) => {
                    return Err(Refuse::PredUnsupported { what: "bytes-cmp-len" })
                }
                crate::bank::Face::Varlena => check_collation(bank, *col)?,
                _ => {
                    return Err(Refuse::FaceUnsupported { attno: *col, what: "bytes-cmp-face" })
                }
            };
            let vop =
                if trim { ir::VarOp::CmpBytesTrim(opc) } else { ir::VarOp::CmpBytes(opc) };
            var_terms.push(ir::VarPredTerm::new(*col, vop, needle, ty))
        }
        // [sqe-bpchar] byte-equality IN: the images are exact byte
        // identities (the seam already padded/typed them), the face gate
        // is CmpBytes(eq)'s (C-collated varlena; Fixed images must match
        // the face length), and the needle is the CANONICAL sorted/
        // deduped encoding — fingerprint identity is list-order free.
        APred::InBytes { col, vals_hex, .. } => {
            if vals_hex.is_empty() || vals_hex.len() > IN_BYTES_MAX {
                return Err(Refuse::InListWidth { n: vals_hex.len() });
            }
            let mut images = Vec::with_capacity(vals_hex.len());
            for v in vals_hex {
                images
                    .push(parse_hex(v).ok_or(Refuse::PredUnsupported { what: "bytes-in-hex" })?);
            }
            let ty = match bank.face(*col) {
                crate::bank::Face::Fixed(len)
                    if images.iter().all(|i| i.len() == len as usize) =>
                {
                    bank.typ(*col)
                }
                crate::bank::Face::Fixed(_) => {
                    return Err(Refuse::PredUnsupported { what: "bytes-in-len" })
                }
                crate::bank::Face::Varlena => check_collation(bank, *col)?,
                _ => {
                    return Err(Refuse::FaceUnsupported { attno: *col, what: "bytes-in-face" })
                }
            };
            let needle = ir::encode_in_needles(images);
            var_terms.push(ir::VarPredTerm::new(*col, ir::VarOp::InBytes, needle, ty))
        }
        APred::InPrefixBytes { col, vals_hex, .. } => {
            if vals_hex.is_empty() || vals_hex.len() > IN_BYTES_MAX {
                return Err(Refuse::InListWidth { n: vals_hex.len() });
            }
            let mut images = Vec::with_capacity(vals_hex.len());
            for v in vals_hex {
                images
                    .push(parse_hex(v).ok_or(Refuse::PredUnsupported { what: "bytes-in-hex" })?);
            }
            let ty = match bank.face(*col) {
                crate::bank::Face::Varlena => check_collation(bank, *col)?,
                _ => {
                    return Err(Refuse::FaceUnsupported { attno: *col, what: "bytes-in-face" })
                }
            };
            let needle = ir::encode_in_needles(images);
            var_terms.push(ir::VarPredTerm::new(*col, ir::VarOp::InPrefix, needle, ty))
        }
        // [colcmp] no single-relation family carries the lane (yet).
        APred::ColCmp { .. } => return Err(Refuse::PredUnsupported { what: "col-cmp" }),
        APred::In { col, vals, .. } => {
            if vals.len() != 2 {
                return Err(Refuse::InListWidth { n: vals.len() });
            }
            terms.push(PredTerm::new(
                *col,
                CmpOp::In2,
                parse_const(bank, *col, &vals[0])?,
                parse_const(bank, *col, &vals[1])?,
                bank.typ(*col),
            ))
        }
    }
    Ok(())
}

/// One authored varlena conjunct as its `VarPredTerm` (the join sides'
/// per-side vocabulary — the single-relation lowering law, C collation
/// gated). Ok(None) = not a varlena conjunct shape.
pub fn lower_var_conjunct(bank: &Bank, ap: &APred) -> Result<Option<ir::VarPredTerm>, Refuse> {
    match ap {
        APred::NeEmpty { col, .. } => {
            let ty = check_collation(bank, *col)?;
            Ok(Some(ir::VarPredTerm::new(*col, ir::VarOp::NeEmpty, Vec::new(), ty)))
        }
        APred::Contains { .. }
        | APred::Like { .. }
        | APred::Not { .. }
        | APred::CmpBytes { .. }
        | APred::InBytes { .. }
        | APred::InPrefixBytes { .. } => {
            let mut terms = Vec::new();
            let mut vts = Vec::new();
            let mut de = Vec::new();
            lower_pred(bank, ap, &mut terms, &mut vts, &mut de)?;
            if vts.len() == 1 && terms.is_empty() && de.is_empty() {
                Ok(vts.pop())
            } else {
                Err(Refuse::PredUnsupported { what: "side-var-conjunct" })
            }
        }
        _ => Ok(None),
    }
}

/// [sqe-bpchar] InBytes arity cap: the same closed-vocabulary posture as
/// the word lane's In2 — big lists belong to a set-membership plane, not
/// a per-row needle walk. TPC-H's bpchar IN lists are 2-8 wide.
const IN_BYTES_MAX: usize = 16;

/// [type-vocab] Hex-decode a CmpBytes constant image (the byte-fidelity
/// authoring form). None on odd length or a non-hex digit.
fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if b.len() % 2 != 0 {
        return None;
    }
    let nib = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    b.chunks_exact(2)
        .map(|p| Some(nib(p[0])? << 4 | nib(p[1])?))
        .collect()
}

/// Frame/residue split — a stats election (PLANNER-SPEC §2.6): a term
/// joins the FRAME iff its flat SMA face erases at least a quarter of the
/// granules. Ordering is stable: frame terms first, residues after.
fn frame_split(
    bank: &Bank,
    faces: &Faces,
    terms: Vec<PredTerm>,
    var_terms: Vec<ir::VarPredTerm>,
) -> PredSpec {
    let mut frame = Vec::new();
    let mut residue = Vec::new();
    for t in terms {
        let units = faces.walk(bank, t.col);
        let sma = faces.sma(bank, t.col);
        let skipped = (0..units.len())
            .filter(|&i| !t.zone_may_pass(sma.mins[i], sma.maxs[i]))
            .count();
        if skipped * 4 >= units.len() {
            frame.push(t);
        } else {
            residue.push(t);
        }
    }
    // Degenerate guard: a frame-consuming stencil needs an anchor term;
    // when nothing prunes, the first term anchors a full (must-scan)
    // frame — correct, never the hot shape. (Var-term plans anchor on
    // the varlena verdict instead — no int anchor needed.)
    if frame.is_empty() && !residue.is_empty() && var_terms.is_empty() {
        frame.push(residue.remove(0));
    }
    let n = frame.len();
    frame.extend(residue);
    PredSpec { terms: frame, frame_terms: n, var_terms, col_terms: Vec::new() }
}

/// Condcache value class (PLANNER-SPEC §2.4): NEGATIVE when the predicate
/// columns are a subset of the consumer's decode set. The consumable
/// fingerprints go into the GOAL vector; an empty goal = recompute-only.
fn condcache_goal(node: &PlanNode) -> Vec<ir::ConjFp> {
    let Some(pred) = &node.pred else { return Vec::new() };
    if pred.frame_terms == 0 && pred.var_terms.is_empty() {
        return Vec::new();
    }
    // Consumer decode set = group keys + true aggregate inputs.
    // EmitMatches is the selection-is-answer class: replay makes
    // the scan disappear, so its column does NOT make the class negative.
    let consumer: Vec<u32> = node
        .params
        .group_cols
        .iter()
        .copied()
        .chain(
            node.agg
                .iter()
                .filter(|a| a.op != AggOp::EmitMatches)
                .filter_map(|a| a.col),
        )
        .collect();
    // [famB M1] varlena frame terms are always POSITIVE: the cached plane
    // carries the survivor rowlists (plus the code sidecar), so replay
    // skips the dict-verdict + unpack even when the column re-appears
    // downstream.
    let negative =
        pred.var_terms.is_empty() && pred.frame().iter().all(|t| consumer.contains(&t.col));
    if negative {
        Vec::new()
    } else {
        let mut fps = vec![pred.frame_fingerprint()];
        // [sqe-m2] The FINAL survivor plane (frame + residues): a second
        // consumable identity whenever residues exist — hot reps replaying
        // it skip the residue decode+eval entirely.
        let full = pred.full_fingerprint();
        if full != fps[0] {
            fps.push(full);
        }
        fps
    }
}

fn claim_class(family: ir::Family) -> ClaimClass {
    match family {
        ir::Family::HashPlaneOwnedGroup | ir::Family::DenseDomainGroup => ClaimClass::Counting,
        ir::Family::FusedFilterAgg
        | ir::Family::TwoLevelCodeAgg
        | ir::Family::FrameWalkCodeGroup
        | ir::Family::WindowReplay
        | ir::Family::SurvivorGather
        | ir::Family::VerdictBitmapContains => ClaimClass::SkipDominated,
        ir::Family::DistinctPipeline => ClaimClass::PairDistinct,
        ir::Family::ZoneOrderWalk | ir::Family::DerivedKeyFold => ClaimClass::BoundedWalk,
        ir::Family::MetadataAnswer => ClaimClass::BoundedWalk,
        ir::Family::ScanServe => ClaimClass::SkipDominated,
        // [sortgrp v1] pass A is a full-width scan/filter/scatter.
        ir::Family::SortGrouped => ClaimClass::Counting,
        // [winserve v1] pass A is the same full-width scan/filter/scatter.
        ir::Family::WindowServe => ClaimClass::Counting,
    }
}

fn lower_family(f: AFamily) -> ir::Family {
    match f {
        AFamily::MetadataAnswer => ir::Family::MetadataAnswer,
        AFamily::FusedFilterAgg => ir::Family::FusedFilterAgg,
        AFamily::DenseDomainGroup => ir::Family::DenseDomainGroup,
        AFamily::TwoLevelCodeAgg => ir::Family::TwoLevelCodeAgg,
        AFamily::HashPlaneOwnedGroup => ir::Family::HashPlaneOwnedGroup,
        AFamily::DistinctPipeline => ir::Family::DistinctPipeline,
        AFamily::VerdictBitmapContains => ir::Family::VerdictBitmapContains,
        AFamily::ZoneOrderWalk => ir::Family::ZoneOrderWalk,
        AFamily::FrameWalkCodeGroup => ir::Family::FrameWalkCodeGroup,
        AFamily::WindowReplay => ir::Family::WindowReplay,
        AFamily::SurvivorGather => ir::Family::SurvivorGather,
        AFamily::DerivedKeyFold => ir::Family::DerivedKeyFold,
        AFamily::ScanServe => ir::Family::ScanServe,
        AFamily::SortGrouped => ir::Family::SortGrouped,
        AFamily::WindowServe => ir::Family::WindowServe,
    }
}

/// Typed-currency admission (P1-1 3VL + face-gap closure), fail-closed:
///
/// FACES (the census face vocabulary, storage-class-derived — the sealed
/// writer's authority, never TypMeta):
///   - int predicate terms and byval GROUP keys admit signed words,
///     unsigned words (width <= 4), and bool — the faces with an
///     order-preserving i64 embed whose literals parse in the i64 domain;
///     float/fixed predicates and keys refuse (FaceUnsupported) until
///     their canonical-key lowering lands.
///   - SUM/AVG (and SumShifted) admit signed words only (the exact-i128
///     signed-sum law); MIN/MAX admit every word-foldable face plus
///     Fixed (uuid memcmp byte fold) on the metadata path.
///
/// NULLABILITY (the 3VL lattice): a column with a real validity stream
/// admits only where the body is null-threaded —
///   - MetadataAnswer: threaded (nonnull-based facts, validity-aware
///     decode fallback);
///   - FusedFilterAgg: threaded (eval_v + validity through the decode
///     faces; SWAR/word-face shortcuts disabled by the stencil);
///   - HashPlaneOwnedGroup byval-key foundation: threaded (nullmask bit
///     per key lane; NULL is its own group; nullable SUM/AVG/MIN/MAX
///     inputs ride the Cells128 fold route) — CountDistinct legs,
///     key_exprs, and varlena keys refuse;
///   - text128 (one varlena key, CountStar, no predicate) and the
///     WindowReplay var lane (key / var-term columns): threaded through
///     the validity stream at row grain — NULL keys group once, NULL
///     predicate inputs never survive (json-rung2);
///   - everything else: NullableUnsupported (refusal, never wrongness).
/// Nullable DICT columns refuse elsewhere (`NullableDict`) — the other
/// code-currency loops carry a zero-null proof until nullable-dict lands.
/// Null-free columns (no validity stream — the NOT NULL
/// exploitation) skip the whole lattice at plan time; hot loops are
/// untouched (law 11).
// ---------------------------------------------------------------------------
// [P4-1] fused-arithmetic admission (the overflow law — ir::FoldExpr doc)
// ---------------------------------------------------------------------------

/// The i128 interval every evaluated row of column `c` provably lies in:
/// the exact stats domain when the Witness membrane grants it, else the
/// column TYPE's range — both honest supersets of any filtered stream.
/// Non-int types carry no arithmetic vocabulary (typed refusal).
fn arith_col_domain(bank: &Bank, faces: &Faces, c: u32) -> Result<(i128, i128), Refuse> {
    let ty = bank.typ(c);
    if !matches!(ty.oid, oids::INT2 | oids::INT4 | oids::INT8) {
        return Err(Refuse::AggUnsupported { what: "agg-arith-input-type" });
    }
    if let Some(w) = Witness::exact_domain(&faces.stats(bank, c)) {
        let (lo, hi) = w.value();
        return Ok((lo as i128, hi as i128));
    }
    Ok(match ty.width {
        2 => (i16::MIN as i128, i16::MAX as i128),
        4 => (i32::MIN as i128, i32::MAX as i128),
        _ => (i64::MIN as i128, i64::MAX as i128),
    })
}

/// Does the interval fit the PG op result width (2/4/8)? PG errors on
/// per-row overflow of the OPERATOR's result type — an unproven shape is
/// a typed refusal, never a wrapped fold (identical answers include
/// identical errors; this engine never errors mid-fold).
fn arith_fits(w: u8, lo: i128, hi: i128) -> bool {
    let (tl, th) = match w {
        2 => (i16::MIN as i128, i16::MAX as i128),
        4 => (i32::MIN as i128, i32::MAX as i128),
        _ => (i64::MIN as i128, i64::MAX as i128),
    };
    lo >= tl && hi <= th
}

/// Exact interval product (corner extrema — i128 math on i64-class
/// corners never overflows).
fn arith_mul_iv((al, ah): (i128, i128), (bl, bh): (i128, i128)) -> (i128, i128) {
    let cs = [al * bl, al * bh, ah * bl, ah * bh];
    (*cs.iter().min().expect("4 corners"), *cs.iter().max().expect("4 corners"))
}

/// [scale-alg] The i128 interval every MANTISSA word of packed column
/// `c` provably lies in. The face is the scale-witness gate: it must be
/// PackedNumeric at the AUTHORED scale (a demoted or re-scaled bank
/// refuses typed — the packedpred face law), and the exact stats domain
/// (mantissa-keyed for packed lanes, KeyKind::Exact) must exist —
/// PackedNumeric has NO honest type-range fallback (numeric is
/// unbounded), so no witness ⇒ typed refusal, never a wrapped fold.
fn packed_col_domain(
    bank: &Bank,
    faces: &Faces,
    c: u32,
    authored_scale: i32,
) -> Result<(i128, i128), Refuse> {
    match bank.face(c) {
        crate::bank::Face::PackedNumeric { scale } if scale == authored_scale => {}
        _ => {
            return Err(Refuse::FaceUnsupported { attno: c, what: "packed-arith-scale" });
        }
    }
    let Some(w) = Witness::exact_domain(&faces.stats(bank, c)) else {
        return Err(Refuse::FaceUnsupported { attno: c, what: "numeric-scale-witness" });
    };
    let (lo, hi) = w.value();
    Ok((lo as i128, hi as i128))
}

/// [tpch-expr] Mixed lane: authored scale 0 over an int column rides
/// the int word domain; numeric lanes keep the packed law.
fn packed_or_word_domain(
    bank: &Bank,
    faces: &Faces,
    c: u32,
    authored_scale: i32,
) -> Result<(i128, i128), Refuse> {
    if authored_scale == 0
        && matches!(bank.typ(c).oid, oids::INT2 | oids::INT4 | oids::INT8)
    {
        return arith_col_domain(bank, faces, c);
    }
    packed_col_domain(bank, faces, c, authored_scale)
}

/// [scale-alg] Interval of the packed inner leg `k ± b` (mantissa units
/// at b's scale) — exact i128 corner algebra.
fn packed_inner_iv(k: i64, sub: bool, (bl, bh): (i128, i128)) -> (i128, i128) {
    if sub {
        (k as i128 - bh, k as i128 - bl)
    } else {
        (k as i128 + bl, k as i128 + bh)
    }
}

/// Admit `col + k` under result width `w` (the SumShifted lowering).
fn admit_arith_addk(bank: &Bank, faces: &Faces, col: u32, k: i64, w: u8) -> Result<(), Refuse> {
    let (lo, hi) = arith_col_domain(bank, faces, col)?;
    if !arith_fits(w, lo + k as i128, hi + k as i128) {
        return Err(Refuse::AggUnsupported { what: "agg-arith-overflow-unwitnessed" });
    }
    Ok(())
}

/// [P4-1] lower a Sum/Avg over a fused-arithmetic input, fail-closed on
/// the no-overflow witness.
fn lower_arith_agg(
    bank: &Bank,
    faces: &Faces,
    op: AggOp,
    e: &AValExpr,
) -> Result<ir::AggSpec, Refuse> {
    let refuse = || Refuse::AggUnsupported { what: "agg-arith-overflow-unwitnessed" };
    match *e {
        AValExpr::AddK { col, k, w } => {
            // Sum only (Σ(a+k) = Σa + k·n — the answer-time algebra).
            if op != AggOp::Sum {
                return Err(Refuse::AggUnsupported { what: "agg-expr-shape" });
            }
            admit_arith_addk(bank, faces, col, k, w)?;
            let mut s = ir::AggSpec::new(AggOp::SumShifted, Some(col), Some(bank.typ(col)));
            s.k = k as u64;
            Ok(s)
        }
        AValExpr::MulCC { a, b, w } => {
            let da = arith_col_domain(bank, faces, a)?;
            let db = arith_col_domain(bank, faces, b)?;
            let (lo, hi) = arith_mul_iv(da, db);
            if !arith_fits(w, lo, hi) {
                return Err(refuse());
            }
            Ok(ir::AggSpec::new(op, Some(a), Some(bank.typ(a)))
                .with_expr(ir::FoldExpr::MulCC { b, w }))
        }
        AValExpr::MulKSub { a, k, b, w, wi } => {
            let da = arith_col_domain(bank, faces, a)?;
            let (bl, bh) = arith_col_domain(bank, faces, b)?;
            // inner op: (k - b) must fit ITS result type on every row…
            let inner = (k as i128 - bh, k as i128 - bl);
            if !arith_fits(wi, inner.0, inner.1) {
                return Err(refuse());
            }
            // …then the product must fit the mul op's.
            let (lo, hi) = arith_mul_iv(da, inner);
            if !arith_fits(w, lo, hi) {
                return Err(refuse());
            }
            Ok(ir::AggSpec::new(op, Some(a), Some(bank.typ(a)))
                .with_expr(ir::FoldExpr::MulKSub { k, b, w, wi }))
        }
        // [scale-alg] packed-lane fused arithmetic: the scale-witness
        // admission. PG numeric arithmetic is exact and never
        // overflows, so the proof obligation is REPRESENTABILITY — the
        // witnessed mantissa domains must prove every per-row product
        // (and, for the three-lane shape, the kernel's intermediate
        // left-fold product) fits the i64 fold word. Unproven shapes
        // refuse typed (numeric-scale-witness), never fold wrapped.
        AValExpr::PackedMulK { a, sa, k, sub, b, sb } => {
            let da = packed_or_word_domain(bank, faces, a, sa)?;
            let db = packed_or_word_domain(bank, faces, b, sb)?;
            let unwitnessed =
                || Refuse::FaceUnsupported { attno: a, what: "numeric-scale-witness" };
            let inner = packed_inner_iv(k, sub, db);
            if !arith_fits(8, inner.0, inner.1) {
                return Err(unwitnessed());
            }
            let (lo, hi) = arith_mul_iv(da, inner);
            if !arith_fits(8, lo, hi) {
                return Err(unwitnessed());
            }
            Ok(ir::AggSpec::new(op, Some(a), Some(bank.typ(a)))
                .with_expr(ir::FoldExpr::PackedMulK { k, sub, b }))
        }
        AValExpr::PackedMulK2 { a, sa, k1, sub1, b, sb, k2, sub2, c, sc } => {
            let da = packed_or_word_domain(bank, faces, a, sa)?;
            let db = packed_or_word_domain(bank, faces, b, sb)?;
            let dc = packed_or_word_domain(bank, faces, c, sc)?;
            let unwitnessed =
                || Refuse::FaceUnsupported { attno: a, what: "numeric-scale-witness" };
            let inner1 = packed_inner_iv(k1, sub1, db);
            let inner2 = packed_inner_iv(k2, sub2, dc);
            if !arith_fits(8, inner1.0, inner1.1) || !arith_fits(8, inner2.0, inner2.1) {
                return Err(unwitnessed());
            }
            // The kernel folds left: (a·(k1±b)) first — that
            // intermediate must fit i64 for the i64 evaluation to
            // equal PG's exact product (and so the corner algebra
            // below stays inside i128).
            let p12 = arith_mul_iv(da, inner1);
            if !arith_fits(8, p12.0, p12.1) {
                return Err(unwitnessed());
            }
            let (lo, hi) = arith_mul_iv(p12, inner2);
            if !arith_fits(8, lo, hi) {
                return Err(unwitnessed());
            }
            Ok(ir::AggSpec::new(op, Some(a), Some(bank.typ(a)))
                .with_expr(ir::FoldExpr::PackedMulK2 { k1, sub1, b, k2, sub2, c }))
        }
        AValExpr::Col(_) | AValExpr::OctetLength { .. } | AValExpr::CharLength { .. } => {
            Err(Refuse::AggUnsupported { what: "agg-expr-shape" })
        }
    }
}

fn check_currency(bank: &Bank, node: &PlanNode) -> Result<(), Refuse> {
    use crate::bank::Face;
    // ---- face vocabulary per operation ----------------------------------
    let word_pred_face = |f: Face| {
        matches!(f, Face::SignedWord(_) | Face::Bool) || matches!(f, Face::UnsignedWord(w) if w <= 4)
    };
    if let Some(p) = &node.pred {
        for t in &p.terms {
            // [packedpred] a NUMERIC-typed word term is minted ONLY by
            // the APred::Packed lowering, which already verified the
            // witnessed face scale against this very bank (lowering and
            // currency share the engine; memo reuse re-lowers on any
            // engine change). Its mantissa words ride the ordinary
            // signed-word compare/zone paths (M3-E order pin).
            let f = bank.face(t.col);
            let packed_ok = t.fp.ty_oid == oids::NUMERIC
                && matches!(f, Face::PackedNumeric { .. });
            if !word_pred_face(f) && !packed_ok {
                return Err(Refuse::FaceUnsupported { attno: t.col, what: "int-pred-face" });
            }
        }
        // [type-vocab] varlena terms eval payload bytes; Fixed-face terms
        // are served ONLY by the scan family's face-dispatched eval —
        // every other stencil's vterm site reads varlena payloads, so a
        // Fixed vterm elsewhere refuses typed (fail closed).
        for t in &p.var_terms {
            match bank.face(t.col) {
                Face::Varlena => {}
                Face::Fixed(_)
                    if node.family == ir::Family::ScanServe
                        && matches!(t.op, ir::VarOp::CmpBytes(_)) => {}
                _ => {
                    return Err(Refuse::FaceUnsupported { attno: t.col, what: "var-pred-face" });
                }
            }
        }
    }
    // [aggqual] per-agg FILTER terms: the same word-comparable face law
    // as the statement predicate.
    for f in node.params.agg_filters.iter().flatten() {
        for t in &f.terms {
            if !word_pred_face(bank.face(t.col)) {
                return Err(Refuse::FaceUnsupported { attno: t.col, what: "int-pred-face" });
            }
        }
    }
    for &c in &node.params.group_cols {
        let f = bank.face(c);
        if !word_pred_face(f) && f != Face::Varlena {
            return Err(Refuse::FaceUnsupported { attno: c, what: "group-key-face" });
        }
    }
    for a in &node.agg {
        let Some(c) = a.col else { continue };
        let f = bank.face(c);
        match a.op {
            // [aggqual] distinct sums dedup and fold the SIGNED WORD
            // itself — the same face law as Sum/Avg minus the packed
            // lane (a scale algebra over the distinct SET is unruled).
            AggOp::SumDistinct | AggOp::AvgDistinct => {
                if !matches!(f, Face::SignedWord(_)) {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "sum-avg-face" });
                }
            }
            AggOp::Sum | AggOp::SumShifted | AggOp::Avg => {
                // [packednum] PackedNumeric words ARE exact scaled ints:
                // the same i128 sum law serves sum/avg(numeric).
                // [scale-alg] Fused packed arithmetic is RULED (the
                // WITNESSED FIXED-POINT ALGEBRA): mul adds scales,
                // add/sub aligns scales — the Packed* FoldExpr shapes
                // carry it under the planner's scale witness. A packed
                // lane under an INT-vocabulary shape (or SumShifted's
                // unscaled k) still refuses typed.
                let packed = matches!(f, Face::PackedNumeric { .. });
                if !matches!(f, Face::SignedWord(_)) && !packed {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "sum-avg-face" });
                }
                let packed_expr = matches!(
                    a.expr,
                    Some(ir::FoldExpr::PackedMulK { .. } | ir::FoldExpr::PackedMulK2 { .. })
                );
                if packed && (a.op == AggOp::SumShifted || (a.expr.is_some() && !packed_expr)) {
                    return Err(Refuse::FaceUnsupported {
                        attno: c,
                        what: "packed-numeric-expr",
                    });
                }
                if packed_expr && !packed && !matches!(f, Face::SignedWord(_)) {
                    // SignedWord primary = the scale-0 int lane.
                    return Err(Refuse::FaceUnsupported { attno: c, what: "packed-arith-scale" });
                }
                // [P4-1] expr legs: every further operand shares the
                // primary face law — signed words for the int shapes,
                // PackedNumeric lanes for the packed ones (the exact-
                // i128 signed-sum law covers both fused products).
                for c2 in [a.col2(), a.col3()].into_iter().flatten() {
                    let ok = if packed_expr {
                        matches!(
                            bank.face(c2),
                            Face::PackedNumeric { .. } | Face::SignedWord(_)
                        )
                    } else {
                        matches!(bank.face(c2), Face::SignedWord(_))
                    };
                    if !ok {
                        return Err(Refuse::FaceUnsupported { attno: c2, what: "sum-avg-face" });
                    }
                }
            }
            // The variance family rides the exact-i128 signed-sum law
            // (float/numeric inputs are PG's float8/numeric transition
            // lanes — out of this vocabulary, typed refusal).
            AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
                if !matches!(f, Face::SignedWord(_)) {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "var-stddev-face" });
                }
            }
            AggOp::BitAnd | AggOp::BitOr => {
                if !matches!(f, Face::SignedWord(_)) {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "bit-agg-face" });
                }
            }
            AggOp::Min | AggOp::Max => {
                let ok = f.word_foldable()
                    || (matches!(f, Face::Fixed(_)) && node.family == ir::Family::MetadataAnswer);
                if !ok {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "min-max-face" });
                }
            }
            AggOp::EmitMatches => {
                // The scan family emits byte-face payloads verbatim
                // (varlena payload bytes; Fixed raw images — [type-vocab]);
                // the selection-is-answer word shape keeps the fold law.
                // [packednum] a PackedNumeric emit would answer bare
                // mantissa words with no render scale downstream —
                // refuse until the scan render law carries the scale.
                let ok = !matches!(f, Face::PackedNumeric { .. })
                    && (f.word_foldable()
                        || (node.family == ir::Family::ScanServe
                            && matches!(f, Face::Varlena | Face::Fixed(_))));
                if !ok {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "emit-face" });
                }
            }
            // [sortgrp v1] tier-2 inputs: value-preserving word embeds
            // (signed/narrow-unsigned/bool) or varlena payload bytes.
            // percentile_cont needs the VALUE as float8 — int words only
            // (the seam recognizes the i2tod/i4tod/i8tod cast); float
            // faces' word embeds are order-preserving, not value-
            // preserving, so they refuse until a value lane lands.
            AggOp::StringAgg => {
                if f != Face::Varlena {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "string-agg-face" });
                }
            }
            AggOp::PercentileCont => {
                if !matches!(f, Face::SignedWord(_)) {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "percentile-cont-face" });
                }
            }
            AggOp::ArrayAgg | AggOp::ArrayAggDistinct | AggOp::PercentileDisc | AggOp::Mode => {
                if !matches!(
                    f,
                    Face::SignedWord(_) | Face::UnsignedWord(1..=4) | Face::Bool | Face::Varlena
                ) {
                    return Err(Refuse::FaceUnsupported { attno: c, what: "sortagg-face" });
                }
            }
            _ => {}
        }
    }
    // [sortgrp v1] statement sort-key faces: word-comparable or varlena
    // memcmp (C collation checked at lowering); float/fixed faces refuse.
    if let Some(sa) = &node.params.sortagg {
        for k in &sa.keys {
            if !matches!(
                bank.face(k.col),
                Face::SignedWord(_) | Face::UnsignedWord(1..=4) | Face::Bool | Face::Varlena
            ) {
                return Err(Refuse::FaceUnsupported { attno: k.col, what: "sortagg-key-face" });
            }
        }
    }

    // [winserve v1] window key / input / emit faces: value-preserving
    // word embeds or varlena payload bytes (collations checked at
    // lowering). Sum/Avg need the VALUE — signed words only (the
    // fold-law admission); Min/Max/Count and the keys take any
    // comparable cell face.
    if let Some(w) = &node.params.window {
        let cell_ok = |c: u32| {
            matches!(
                bank.face(c),
                Face::SignedWord(_) | Face::UnsignedWord(1..=4) | Face::Bool | Face::Varlena
            )
        };
        for &c in w.part_cols.iter().chain(w.emit.iter()) {
            if !cell_ok(c) {
                return Err(Refuse::FaceUnsupported { attno: c, what: "window-face" });
            }
        }
        for k in &w.order {
            if !cell_ok(k.col) {
                return Err(Refuse::FaceUnsupported { attno: k.col, what: "window-key-face" });
            }
        }
        for f in &w.funcs {
            let Some(c) = f.col else { continue };
            match f.op {
                ir::WinOp::Sum | ir::WinOp::Avg => {
                    if !matches!(bank.face(c), Face::SignedWord(_)) {
                        return Err(Refuse::FaceUnsupported { attno: c, what: "window-fold-face" });
                    }
                }
                ir::WinOp::Min | ir::WinOp::Max | ir::WinOp::Count | ir::WinOp::Lead
                | ir::WinOp::Lag | ir::WinOp::FirstValue | ir::WinOp::LastValue
                | ir::WinOp::NthValue => {
                    if !cell_ok(c) {
                        return Err(Refuse::FaceUnsupported { attno: c, what: "window-face" });
                    }
                }
                ir::WinOp::RowNumber | ir::WinOp::Rank | ir::WinOp::DenseRank
                | ir::WinOp::CountStar => {}
            }
        }
        // [winframes v2] Range-offset frames read the order key's VALUE
        // (in_range arithmetic) — word embeds only.
        if w.frame.mode == ir::FrameMode::Range
            && w.frame.has_offsets()
            && !w.order.is_empty()
            && !matches!(
                bank.face(w.order[0].col),
                Face::SignedWord(_) | Face::UnsignedWord(1..=4) | Face::Bool
            )
        {
            return Err(Refuse::FaceUnsupported {
                attno: w.order[0].col,
                what: "range-offset-key-face",
            });
        }
    }

    // ---- 3VL nullability lattice ----------------------------------------
    let mut cols: Vec<u32> = node.cols.clone();
    let note = |c: u32, cols: &mut Vec<u32>| {
        if !cols.contains(&c) {
            cols.push(c);
        }
    };
    for &c in &node.params.group_cols {
        note(c, &mut cols);
    }
    for &c in &node.params.ne_empty_cols {
        note(c, &mut cols);
    }
    if let Some(p) = &node.pred {
        for t in &p.terms {
            note(t.col, &mut cols);
        }
        for t in &p.var_terms {
            note(t.col, &mut cols);
        }
    }
    for a in &node.agg {
        if let Some(c) = a.col {
            note(c, &mut cols);
        }
    }
    // [aggqual] per-agg FILTER columns ride the lattice too (their
    // 3VL is the filter's eval_v — threaded only where the serving
    // stencil computes the per-leg mask).
    for f in node.params.agg_filters.iter().flatten() {
        for t in &f.terms {
            note(t.col, &mut cols);
        }
    }
    // [json-rung2] The nullable varlena arms (3VL-threaded at row grain
    // through the validity stream, dict or plain; a dict lane's NULL rows
    // carry placeholder codes the threaded loops never read):
    //   - HashPlaneOwnedGroup text128: ONE varlena key, CountStar legs,
    //     no predicate/HAVING/FILTER — the NULL key groups once, sorted
    //     NULLS-greatest; the byte-key spill route stays null-free (an
    //     over-budget shape serves through the dict-witnessed Bounded
    //     arm where the entry witness prices it under budget
    //     (spill-design.md §3.6) and refuses typed otherwise — never
    //     spills NULLs wrong);
    //   - WindowReplay var lane: a nullable GROUP KEY or var-term column
    //     (strict veto: NULL inputs never survive); aggregate inputs,
    //     FILTER legs and int terms keep the refusal.
    let text128_nullable_key = |c: u32| {
        node.family == ir::Family::HashPlaneOwnedGroup
            && node.params.group_cols.as_slice() == [c]
            && bank.face(c) == Face::Varlena
            && node.params.key_exprs.is_empty()
            && node.agg.iter().all(|a| a.op == AggOp::CountStar)
            && !node.agg.is_empty()
            && node.pred.is_none()
            && node.params.ne_empty_cols.is_empty()
            && node.params.having.is_none()
            && node.params.having_min_count == 0
            && node.params.agg_filters.iter().all(Option::is_none)
    };
    let var_lane_nullable = |c: u32| {
        let Some(p) = &node.pred else { return false };
        node.family == ir::Family::WindowReplay
            && !p.var_terms.is_empty()
            && bank.face(c) == Face::Varlena
            && (node.params.group_cols.as_slice() == [c]
                || node.params.key_exprs.iter().any(|e| matches!(e, ir::KeyExpr::Col(k) if *k == c))
                || p.var_terms.iter().any(|t| t.col == c))
            && node.agg.iter().all(|a| a.col != Some(c))
            && !p.terms.iter().any(|t| t.col == c)
            && node.params.agg_filters.iter().flatten().all(|f| f.terms.iter().all(|t| t.col != c))
    };
    // Var-lane word fold inputs consult validity per survivor (NULL folds nothing).
    let var_lane_nullable_word_input = |c: u32| {
        let Some(p) = &node.pred else { return false };
        node.family == ir::Family::WindowReplay
            && !p.var_terms.is_empty()
            && !node.params.group_cols.is_empty()
            && word_pred_face(bank.face(c))
            && node.agg.iter().all(|a| {
                a.col != Some(c) || matches!(a.op, AggOp::Min | AggOp::Max | AggOp::Sum)
            })
            && node.agg.iter().any(|a| a.col == Some(c))
            && !node.params.group_cols.contains(&c)
            && !p.terms.iter().any(|t| t.col == c)
            && !p.var_terms.iter().any(|t| t.col == c)
            && node.params.agg_filters.iter().flatten().all(|f| f.terms.iter().all(|t| t.col != c))
    };
    for &c in &cols {
        if bank.null_free(c) {
            continue;
        }
        if text128_nullable_key(c) || var_lane_nullable(c) || var_lane_nullable_word_input(c) {
            continue;
        }
        // Dict lanes: zero-null proof required until nullable-dict lands.
        if bank.face(c) == Face::Varlena
            && (0..bank.parts.len()).any(|pi| crate::scan::is_dict(bank, pi, c))
        {
            return Err(Refuse::NullableDict { attno: c });
        }
        match node.family {
            // ScanServe is fully validity-threaded: NULL predicate rows
            // fail eval_v, NULL output/order cells ride answer masks.
            // SortGrouped threads validity end-to-end: NULL predicate
            // rows fail eval_v, NULL key/sort/input cells ride Option
            // cells with the NULL-absolute comparator and per-agg null
            // policies (the design doc §2.5 normative table).
            // WindowServe threads validity end-to-end exactly as
            // SortGrouped: NULL predicate rows fail eval_v, NULL cells
            // ride Option cells + per-op null policies.
            ir::Family::MetadataAnswer
            | ir::Family::FusedFilterAgg
            | ir::Family::ScanServe
            | ir::Family::SortGrouped
            | ir::Family::WindowServe => {}
            ir::Family::HashPlaneOwnedGroup => {
                let plain_byval_keys = node.params.key_exprs.is_empty()
                    && node
                        .params
                        .group_cols
                        .iter()
                        .all(|&g| word_pred_face(bank.face(g)));
                // [sqe-grpfold] nullable SUM/AVG/MIN/MAX (and the
                // variance/bit fold) inputs are 3VL-threaded by the
                // Cells128 fold route (NULL folds nothing); every other
                // agg leg keeps the refusal.
                let nullable_agg_leg = node.agg.iter().any(|a| {
                    (a.col == Some(c)
                        && !matches!(
                            a.op,
                            AggOp::Sum
                                | AggOp::Avg
                                | AggOp::Min
                                | AggOp::Max
                                | AggOp::VarSamp
                                | AggOp::VarPop
                                | AggOp::StddevSamp
                                | AggOp::StddevPop
                                | AggOp::BitAnd
                                | AggOp::BitOr
                        ))
                        || matches!(
                            a.op,
                            AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct
                        )
                });
                // The nullmask lanes live at u128 bits 126/125: the
                // packed value fields must fit below them.
                let packed_bits: u32 = node
                    .params
                    .group_cols
                    .iter()
                    .map(|&g| 8 * crate::stencils::col_width(bank, g) as u32)
                    .sum();
                if !plain_byval_keys
                    || nullable_agg_leg
                    || node.pred.is_some()
                    || node.params.group_cols.len() > 2
                    || packed_bits > 125
                {
                    return Err(Refuse::NullableUnsupported { attno: c, family: node.family });
                }
            }
            // [sqe-avglen] entrylen arm: a nullable AvgLen INPUT is
            // 3VL-threaded by the hydrated-length fold (NULL folds
            // nothing — the `<> ''` law already excludes the row); dict
            // lanes refused above. Keys and every other lane keep the
            // refusal.
            ir::Family::TwoLevelCodeAgg
                if node.agg.iter().any(|a| a.op.is_avglen() && a.col == Some(c))
                    && !node.params.group_cols.contains(&c) => {}
            f => return Err(Refuse::NullableUnsupported { attno: c, family: f }),
        }
    }
    Ok(())
}

/// Lower + rewrite + elect from ANY authoring-plan value — the single
/// lowering pipeline shared by the rig's RON table and SQL lowerer (and
/// re-fed from server plan trees at P2-1), so structural plan diffs
/// compare the SAME machinery's output. Fail-closed: every non-lowerable
/// shape is a named Refuse.
pub fn plan_from_ap(bank: &Bank, faces: &Faces, ap: &APlan) -> Result<PlanNode, Refuse> {
    let q = ap.q;

    // ---- LOWER -----------------------------------------------------------
    let mut terms = Vec::new();
    let mut var_terms = Vec::new();
    let mut drop_empty_cols = Vec::new();
    if let Some(p) = &ap.pred {
        lower_pred(bank, p, &mut terms, &mut var_terms, &mut drop_empty_cols)?;
    }
    // [sqe-q2426] Range-conjunct canonicalization: multiple Between terms
    // on ONE column intersect into a single closed Between (the server's
    // `col >= lo AND col <= hi` lowers as two half-open ranges; the rig
    // authors one). Semantics are identical (range intersection; NULL
    // fails either way; an empty intersection is the always-false term) —
    // what changes is the IDENTITY: the frame pair sees the closed window
    // and the fingerprints match the authored-range twin, so hot-shape
    // condcache planes are shared across both authoring paths.
    {
        let mut i = 0;
        while i < terms.len() {
            if terms[i].op == CmpOp::Between {
                let col = terms[i].col;
                let (mut lo, mut hi) = (terms[i].lo, terms[i].hi);
                let mut merged = false;
                let mut j = i + 1;
                while j < terms.len() {
                    if terms[j].col == col && terms[j].op == CmpOp::Between {
                        lo = lo.max(terms[j].lo);
                        hi = hi.min(terms[j].hi);
                        terms.remove(j);
                        merged = true;
                    } else {
                        j += 1;
                    }
                }
                if merged {
                    terms[i] = PredTerm::new(col, CmpOp::Between, lo, hi, bank.typ(col));
                }
            }
            i += 1;
        }
    }
    // group DECODE set (distinct cols) + EMIT/tie order (key_exprs).
    let mut group_cols: Vec<u32> = Vec::new();
    let mut key_exprs = Vec::new();
    let mut plain_keys = true;
    let push_col = |group_cols: &mut Vec<u32>, c: u32| {
        if !group_cols.contains(&c) {
            group_cols.push(c);
        }
    };
    for k in &ap.group {
        match k {
            AKeyExpr::Col(c) => {
                push_col(&mut group_cols, *c);
                key_exprs.push(ir::KeyExpr::Col(*c));
            }
            AKeyExpr::TruncMinute { col } => {
                push_col(&mut group_cols, *col);
                key_exprs.push(ir::KeyExpr::TruncMinute(*col));
                plain_keys = false;
            }
            AKeyExpr::Minute { col } => {
                push_col(&mut group_cols, *col);
                key_exprs.push(ir::KeyExpr::Minute(*col));
                plain_keys = false;
            }
            AKeyExpr::HourBucket { col, off_s } => {
                push_col(&mut group_cols, *col);
                let off_s = off_s.expect("hour-bucket zone offset unresolved (lowering bug)");
                key_exprs.push(ir::KeyExpr::HourBucket { col: *col, off_s });
                plain_keys = false;
            }
            AKeyExpr::HostRegex { col } => {
                push_col(&mut group_cols, *col);
                key_exprs.push(ir::KeyExpr::HostRegex(*col));
                plain_keys = false;
            }
            AKeyExpr::MinusConst { col, k } => {
                push_col(&mut group_cols, *col);
                key_exprs.push(ir::KeyExpr::MinusConst(*col, *k));
                plain_keys = false;
            }
            AKeyExpr::CaseSrc { se, adv, referer } => {
                for c in [*se, *adv, *referer] {
                    push_col(&mut group_cols, c);
                }
                key_exprs.push(ir::KeyExpr::CaseSrc { se: *se, adv: *adv, referer: *referer });
                plain_keys = false;
            }
            AKeyExpr::Const1 => {
                key_exprs.push(ir::KeyExpr::Const1);
                plain_keys = false;
            }
        }
    }
    let mut agg = Vec::new();
    // [sortgrp v1] the tier-2 order-sensitive/holistic batch lowers as a
    // WHOLE statement (one sort spec, aligned delimiter slots) — mixes
    // with the fold vocabulary refuse typed (v1 scope, design doc §3).
    let is_t2 = |a: &AAgg| {
        matches!(
            a,
            AAgg::StringAgg { .. }
                | AAgg::ArrayAgg { .. }
                | AAgg::ArrayAggDistinct { .. }
                | AAgg::PercentileDisc { .. }
                | AAgg::PercentileCont { .. }
                | AAgg::Mode { .. }
        )
    };
    let mut sortagg: Option<ir::SortAggSpec> = None;
    if ap.agg.iter().any(is_t2) {
        if !ap.agg.iter().all(is_t2) || ap.family != AFamily::SortGrouped {
            return Err(Refuse::SortAggUnsupported { what: "agg-mix" });
        }
        // ORDER-BY-less string_agg/array_agg refuse in v1 (ruling Q4:
        // PG's answer is scan-order-defined; zero wrongness surface).
        if ap.sortagg_keys.is_empty() {
            return Err(Refuse::SortAggUnsupported { what: "order-missing" });
        }
        for k in &ap.sortagg_keys {
            if bank.typ(k.col).is_varlena() {
                check_collation(bank, k.col)?;
            }
        }
        let ordered_set_key_ok = |col: u32| {
            ap.sortagg_keys.len() == 1 && ap.sortagg_keys[0].col == col
        };
        let frac_ok = |f: f64| (0.0..=1.0).contains(&f) && !f.is_nan();
        let mut delims: Vec<ir::AggDelim> = Vec::new();
        for a in &ap.agg {
            match a {
                AAgg::StringAgg { col, delim } => {
                    check_collation(bank, *col)?;
                    agg.push(ir::AggSpec::new(AggOp::StringAgg, Some(*col), Some(bank.typ(*col))));
                    delims.push(match delim {
                        Some(d) => ir::AggDelim::Bytes(d.clone()),
                        None => ir::AggDelim::Null,
                    });
                }
                AAgg::ArrayAgg { col } => {
                    if bank.typ(*col).is_varlena() {
                        check_collation(bank, *col)?;
                    }
                    agg.push(ir::AggSpec::new(AggOp::ArrayAgg, Some(*col), Some(bank.typ(*col))));
                    delims.push(ir::AggDelim::None);
                }
                AAgg::ArrayAggDistinct { col } => {
                    // [aggqual] DISTINCT identity = the sort identity:
                    // the statement's ONE sort key must be the argument
                    // column itself (PG's parse law guarantees it for
                    // servable plans; lowered-elsewhere plans fail
                    // closed here).
                    if !ordered_set_key_ok(*col) {
                        return Err(Refuse::SortAggUnsupported { what: "distinct-key" });
                    }
                    if bank.typ(*col).is_varlena() {
                        check_collation(bank, *col)?;
                    }
                    agg.push(ir::AggSpec::new(
                        AggOp::ArrayAggDistinct,
                        Some(*col),
                        Some(bank.typ(*col)),
                    ));
                    delims.push(ir::AggDelim::None);
                }
                AAgg::PercentileDisc { col, frac } => {
                    if !ordered_set_key_ok(*col) {
                        return Err(Refuse::SortAggUnsupported { what: "ordered-set-key" });
                    }
                    if !frac_ok(*frac) {
                        return Err(Refuse::SortAggUnsupported { what: "fraction-range" });
                    }
                    if bank.typ(*col).is_varlena() {
                        check_collation(bank, *col)?;
                    }
                    agg.push(
                        ir::AggSpec::new(AggOp::PercentileDisc, Some(*col), Some(bank.typ(*col)))
                            .with_direct(*frac),
                    );
                    delims.push(ir::AggDelim::None);
                }
                AAgg::PercentileCont { col, frac } => {
                    if !ordered_set_key_ok(*col) {
                        return Err(Refuse::SortAggUnsupported { what: "ordered-set-key" });
                    }
                    if !frac_ok(*frac) {
                        return Err(Refuse::SortAggUnsupported { what: "fraction-range" });
                    }
                    agg.push(
                        ir::AggSpec::new(AggOp::PercentileCont, Some(*col), Some(bank.typ(*col)))
                            .with_direct(*frac),
                    );
                    delims.push(ir::AggDelim::None);
                }
                AAgg::Mode { col } => {
                    if !ordered_set_key_ok(*col) {
                        return Err(Refuse::SortAggUnsupported { what: "ordered-set-key" });
                    }
                    if bank.typ(*col).is_varlena() {
                        check_collation(bank, *col)?;
                    }
                    agg.push(
                        ir::AggSpec::new(AggOp::Mode, Some(*col), Some(bank.typ(*col))),
                    );
                    delims.push(ir::AggDelim::None);
                }
                _ => unreachable!("all-t2 checked above"),
            }
        }
        sortagg = Some(ir::SortAggSpec {
            keys: ap
                .sortagg_keys
                .iter()
                .map(|k| ir::TopKKey { col: k.col, desc: k.desc, nulls_first: k.nulls_first, lo: None, trim: false })
                .collect(),
            delims,
        });
        if !plain_keys {
            return Err(Refuse::SortAggUnsupported { what: "key-expr" });
        }
    } else if ap.family == AFamily::SortGrouped || !ap.sortagg_keys.is_empty() {
        return Err(Refuse::SortAggUnsupported { what: "agg-mix" });
    } else {
    for a in &ap.agg {
        match a {
            AAgg::CountStar => agg.push(ir::AggSpec::new(AggOp::CountStar, None, None)),
            AAgg::Sum { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::Sum, Some(*c), Some(bank.typ(*c))))
            }
            // [P4-1] fused arithmetic fold inputs — the closed shape
            // vocabulary, admitted under the no-overflow witness
            // (ir::FoldExpr's overflow law). Sum(col+k) lowers to the
            // answer-time algebra (SumShifted); the mul shapes carry a
            // real fused fold.
            AAgg::Sum {
                e:
                    e @ (AValExpr::AddK { .. }
                    | AValExpr::MulCC { .. }
                    | AValExpr::MulKSub { .. }
                    | AValExpr::PackedMulK { .. }
                    | AValExpr::PackedMulK2 { .. }),
            } => agg.push(lower_arith_agg(bank, faces, AggOp::Sum, e)?),
            AAgg::Avg {
                e:
                    e @ (AValExpr::MulCC { .. }
                    | AValExpr::MulKSub { .. }
                    | AValExpr::PackedMulK { .. }
                    | AValExpr::PackedMulK2 { .. }),
            } => agg.push(lower_arith_agg(bank, faces, AggOp::Avg, e)?),
            AAgg::Avg { e: AValExpr::Col(c) } => {
                // The op is Avg either way; the exact-decimal render for
                // w8 inputs (the wide-int-key law) is AggSpec::avg_exact(),
                // decided from in_ty at answer time.
                agg.push(ir::AggSpec::new(AggOp::Avg, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::Avg { e: AValExpr::OctetLength { col } } => {
                agg.push(ir::AggSpec::new(AggOp::AvgLen, Some(*col), Some(bank.typ(*col))))
            }
            AAgg::Avg { e: AValExpr::CharLength { col } } => {
                agg.push(ir::AggSpec::new(AggOp::AvgCharLen, Some(*col), Some(bank.typ(*col))))
            }
            AAgg::Min { e: AValExpr::Col(c) } => {
                // MinDate is dead: Min + DATE out TypMeta (the render law
                // lives in the one render seam).
                agg.push(ir::AggSpec::new(AggOp::Min, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::Max { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::Max, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::MinBytes { e: AValExpr::Col(c) } => {
                check_collation(bank, *c)?;
                agg.push(ir::AggSpec::new(AggOp::MinBytes, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::CountDistinct { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::CountDistinct, Some(*c), Some(bank.typ(*c))))
            }
            // [aggqual] distinct folds: bare-column inputs only (the
            // dedup identity is the column word — expression inputs
            // refuse typed).
            AAgg::SumDistinct { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::SumDistinct, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::AvgDistinct { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::AvgDistinct, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::VarSamp { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::VarSamp, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::VarPop { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::VarPop, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::StddevSamp { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::StddevSamp, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::StddevPop { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::StddevPop, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::BitAnd { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::BitAnd, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::BitOr { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::BitOr, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::Emit { e: AValExpr::Col(c) } => {
                agg.push(ir::AggSpec::new(AggOp::EmitMatches, Some(*c), Some(bank.typ(*c))))
            }
            AAgg::SumShifted { col, k0, n } => {
                // One AggSpec per shift: a 90-sum battery is 90 parametric
                // aggs over one plan, no per-query code anywhere. The RON
                // authoring law: the shift constant is an int4-class
                // literal, so the PG op result width is max(col, 4) —
                // each shift passes the same no-overflow admission as the
                // seam's AddK.
                let w = if crate::stencils::col_width(bank, *col) == 8 { 8 } else { 4 };
                for k in *k0..(*k0 + *n) {
                    let mut s = ir::AggSpec::new(AggOp::SumShifted, Some(*col), Some(bank.typ(*col)));
                    admit_arith_addk(bank, faces, *col, k as i64, w)?;
                    s.k = k;
                    agg.push(s);
                }
            }
            _ => return Err(Refuse::AggUnsupported { what: "agg-expr-shape" }),
        }
    }
    }
    // [winserve v1] the WindowServe statement lowers as a WHOLE: the
    // spec is the node's law; anything mixed refuses typed.
    let mut window: Option<ir::WindowSpec> = None;
    if let Some(w) = &ap.win {
        if ap.family != AFamily::WindowServe
            || !ap.agg.is_empty()
            || !ap.group.is_empty()
            || !ap.sortagg_keys.is_empty()
        {
            return Err(Refuse::WinUnsupported { what: "spec-drift" });
        }
        window = Some(lower_awindow(
            &|c| bank.typ(c),
            &|c| check_collation(bank, c).map(|_| ()),
            w,
        )?);
    } else if ap.family == AFamily::WindowServe {
        return Err(Refuse::WinUnsupported { what: "spec-missing" });
    }
    let (order, offset, limit) = match &ap.order {
        None => (OrderBy::None, 0usize, usize::MAX),
        Some(o) => (
            match o.by {
                AOrderBy::CountDesc => OrderBy::CountDesc,
                AOrderBy::KeyAsc => OrderBy::KeyAsc,
                AOrderBy::ColAsc { col } => OrderBy::ColAsc(col),
                AOrderBy::ColThenColAsc { a, b } => OrderBy::ColThenColAsc(a, b),
                AOrderBy::AggDesc { idx } => OrderBy::AggDesc(idx),
            },
            o.offset as usize,
            o.limit.map(|l| l as usize).unwrap_or(usize::MAX),
        ),
    };
    let mut having_min_count = 0u64;
    if let Some(h) = &ap.having {
        // The 43's only HAVING shape: COUNT(*) > k.
        match (&h.agg, &h.op) {
            (AAgg::CountStar, ACmpOp::Gt) => having_min_count = h.val,
            _ => return Err(Refuse::HavingUnsupported),
        }
    }
    let mut flags = 0u32;
    if !drop_empty_cols.is_empty() && drop_empty_cols.iter().all(|c| group_cols.contains(c)) {
        flags |= ir::F_DROP_EMPTY_KEY;
    }
    // [P4-1] expr legs: the second operand joins the plan's column set
    // (currency checks + the 3VL lattice read it) whether or not the
    // authoring layer staged it.
    let mut ap_cols = ap.cols.clone();
    for a in &agg {
        for c2 in [a.col2(), a.col3()].into_iter().flatten() {
            if !ap_cols.contains(&c2) {
                ap_cols.push(c2);
            }
        }
    }
    // [sortgrp v1] sort-key and agg-input columns join the plan's column
    // set (the stencil's decode set and the 3VL lattice read it).
    if let Some(sa) = &sortagg {
        for k in &sa.keys {
            if !ap_cols.contains(&k.col) {
                ap_cols.push(k.col);
            }
        }
        for a in &agg {
            if let Some(c) = a.col {
                if !ap_cols.contains(&c) {
                    ap_cols.push(c);
                }
            }
        }
    }
    // [aggqual] per-agg FILTER predicates: the SAME int-conjunct term
    // vocabulary as the statement predicate — varlena terms and the
    // drop-empty marker are OUTSIDE the filter vocabulary (typed
    // refusal, never a silent drop). Filter columns join the plan's
    // column set (decode set + 3VL lattice).
    let mut agg_filters: Vec<Option<ir::PredSpec>> = Vec::new();
    if ap.agg_filters.iter().any(Option::is_some) {
        if ap.agg_filters.len() != agg.len() {
            return Err(Refuse::AggUnsupported { what: "agg-filter-align" });
        }
        for f in &ap.agg_filters {
            match f {
                None => agg_filters.push(None),
                Some(p) => {
                    let mut fterms = Vec::new();
                    let mut fvar = Vec::new();
                    let mut fdrop = Vec::new();
                    lower_pred(bank, p, &mut fterms, &mut fvar, &mut fdrop)?;
                    if fterms.is_empty() || !fvar.is_empty() || !fdrop.is_empty() {
                        return Err(Refuse::AggUnsupported { what: "agg-filter-pred" });
                    }
                    for t in &fterms {
                        if !ap_cols.contains(&t.col) {
                            ap_cols.push(t.col);
                        }
                    }
                    agg_filters.push(Some(ir::PredSpec::all(fterms)));
                }
            }
        }
    }
    // [winserve v1] every window-touched column joins the plan's column
    // set (decode set + 3VL lattice).
    if let Some(w) = &window {
        for c in w
            .part_cols
            .iter()
            .chain(w.emit.iter())
            .chain(w.order.iter().map(|k| &k.col))
            .chain(w.funcs.iter().filter_map(|f| f.col.as_ref()))
        {
            if !ap_cols.contains(c) {
                ap_cols.push(*c);
            }
        }
    }
    let col_tys: Vec<TypMeta> = ap_cols.iter().map(|&c| bank.typ(c)).collect();
    let mut node = PlanNode {
        family: lower_family(ap.family),
        q,
        cols: ap_cols,
        col_tys,
        pred: if terms.is_empty() && var_terms.is_empty() {
            None
        } else {
            let n = terms.len();
            Some(ir::PredSpec { terms, frame_terms: n, var_terms, col_terms: Vec::new() })
        },
        agg,
        params: ir::Params {
            group_cols,
            order,
            offset,
            limit,
            flags,
            key_exprs: if plain_keys { Vec::new() } else { key_exprs },
            ne_empty_cols: drop_empty_cols,
            having_min_count,
            sortagg,
            window,
            agg_filters,
            ..ir::Params::default()
        },
    };

    // ---- REWRITE (tier 1, destructive, outside any memo) ------------------
    // Selection-is-answer reconstruction: physically the scan+emit runs in
    // the fused filter-agg stencil with an EmitMatches consumer.
    if node.family == ir::Family::MetadataAnswer && node.agg.is_empty() {
        if let Some(p) = &node.pred {
            if p.terms.len() == 1 && p.terms[0].op == CmpOp::Eq {
                let c = p.terms[0].col;
                node.family = ir::Family::FusedFilterAgg;
                node.agg = vec![ir::AggSpec::new(AggOp::EmitMatches, Some(c), Some(bank.typ(c)))];
            }
        }
    }
    // [famB M1] NeEmpty promotion: for the families whose survivor
    // planes the engine cache serves, `col <> ''` joins the FRAME as a
    // varlena term; lane-A families keep the drop-empty-only lowering.
    // Family-driven lowering policy, not query policy.
    if matches!(
        node.family,
        ir::Family::WindowReplay
            | ir::Family::SurvivorGather
            | ir::Family::ScanServe
            | ir::Family::SortGrouped
            | ir::Family::WindowServe
    ) {
        let ne: Vec<u32> = node.params.ne_empty_cols.clone();
        if !ne.is_empty() {
            let spec = node.pred.get_or_insert_with(|| ir::PredSpec {
                terms: Vec::new(),
                frame_terms: 0,
                var_terms: Vec::new(),
                col_terms: Vec::new(),
            });
            for c in ne {
                spec.var_terms.push(ir::VarPredTerm::new(
                    c,
                    ir::VarOp::NeEmpty,
                    Vec::new(),
                    bank.typ(c),
                ));
            }
        }
    }
    // Frame/residue split (stats election over the SMA faces).
    if let Some(p) = node.pred.take() {
        let ir::PredSpec { terms, var_terms, .. } = p;
        let mut spec = frame_split(bank, faces, terms, var_terms);
        // [sqe-m1/m2] Shared-frame promotion, family-driven (the shared-frame
        // law): frame-consuming families publish/replay the (Eq counter,
        // Between date) PAIR as the cached plane — the fingerprint must be
        // exactly that pair, shared across every query filtering the same
        // (counter, range), with every other conjunct a row-grain residue.
        // Fires on the SHAPE (one Eq!=0 + one Between), whatever the query.
        if matches!(
            node.family,
            ir::Family::WindowReplay
                | ir::Family::TwoLevelCodeAgg
                | ir::Family::HashPlaneOwnedGroup
                | ir::Family::DenseDomainGroup
                | ir::Family::SurvivorGather
        ) {
            let eq = spec.terms.iter().position(|t| t.op == CmpOp::Eq && t.lo != 0);
            let rng = spec.terms.iter().position(|t| t.op == CmpOp::Between);
            if let (Some(ei), Some(ri)) = (eq, rng) {
                let mut pair = Vec::new();
                let mut rest = Vec::new();
                for (i, t) in spec.terms.into_iter().enumerate() {
                    if i == ei || i == ri {
                        pair.push(t);
                    } else {
                        rest.push(t);
                    }
                }
                pair.extend(rest);
                spec = ir::PredSpec { terms: pair, frame_terms: 2, var_terms: spec.var_terms, col_terms: spec.col_terms };
            }
        }
        // Empty predicate: carry None, not an empty spec.
        node.pred = if spec.terms.is_empty() && spec.var_terms.is_empty() {
            None
        } else {
            Some(spec)
        };
    }
    // Typed-currency admission: face vocabulary per operation + the 3VL
    // nullability lattice (which shapes are null-threaded; dict lanes'
    // zero-null proof). Fail-closed, all causes named.
    check_currency(bank, &node)?;

    // Variance-family overflow witness (the Σx² exactness law): the cell
    // fold keeps Σx² exact in i128 iff |x| <= 2^31−1, giving
    // Σx² <= n·2^62 < 2^127 for any n < 2^64 rows. int2/int4 carry the
    // witness in their type domain; int8 inputs demand an exact stats
    // domain inside the int4-class band — no witness (or a wider domain)
    // is a typed refusal, never a wrapped fold. (PG accumulates int8 in
    // numeric; identical answers inside the witnessed domain.)
    for a in &node.agg {
        if matches!(
            a.op,
            AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop
        ) {
            let c = a.col.expect("variance legs carry an input column");
            if crate::stencils::col_width(bank, c) == 8 {
                let b = (1i64 << 31) - 1;
                // [ruling 2] domain proof through the Witness membrane:
                // exact part-record domains only, never an estimate.
                let witnessed = Witness::exact_domain(&faces.stats(bank, c))
                    .map(|w| {
                        let (lo, hi) = w.value();
                        lo >= -b && hi <= b
                    })
                    .unwrap_or(false);
                if !witnessed {
                    return Err(Refuse::AggUnsupported {
                        what: "var-stddev-int8-domain-unwitnessed",
                    });
                }
            }
        }
    }

    // Executability gate: the family must be registered.
    if crate::exec::lookup(node.family).is_none() {
        return Err(Refuse::FamilyUnregistered { family: node.family });
    }
    // FusedFilterAgg scalar fold (rung B): the ungrouped filtered-agg
    // shape admits int-comparison conjuncts only (no varlena terms) and
    // the fold vocabulary CountStar/Sum/Avg/Min/Max — fail-closed on
    // anything else (the stencil never panics on a lowered plan).
    if node.family == ir::Family::FusedFilterAgg
        && node.params.group_cols.is_empty()
        && !node.agg.iter().any(|a| a.op == AggOp::EmitMatches)
    {
        // [aggqual] per-agg FILTER conjuncts alone carry the shape too
        // (a statement predicate is no longer required).
        let has_flt = node.params.agg_filters.iter().any(Option::is_some);
        match &node.pred {
            None if !has_flt => {
                return Err(Refuse::PredUnsupported { what: "scalar-fold-needs-pred" });
            }
            None => {}
            Some(p) => {
                if p.terms.is_empty() && p.var_terms.is_empty() && !has_flt {
                    return Err(Refuse::PredUnsupported { what: "scalar-fold-needs-pred" });
                }
                // Varlena conjuncts compose only with the word/packed
                // fold vocabulary (row-grain eval_v in the fused pass).
                if !p.var_terms.is_empty()
                    && node.agg.iter().enumerate().any(|(ai, a)| {
                        !matches!(
                            a.op,
                            AggOp::CountStar
                                | AggOp::Sum
                                | AggOp::Avg
                                | AggOp::Min
                                | AggOp::Max
                        ) || a.expr.is_some()
                            || node
                                .params
                                .agg_filters
                                .get(ai)
                                .map(Option::is_some)
                                .unwrap_or(false)
                    })
                {
                    return Err(Refuse::PredUnsupported { what: "scalar-fold-var-terms" });
                }
            }
        }
        for a in &node.agg {
            if !matches!(
                a.op,
                AggOp::CountStar
                    | AggOp::Sum
                    | AggOp::SumShifted
                    | AggOp::Avg
                    | AggOp::Min
                    | AggOp::Max
                    | AggOp::VarSamp
                    | AggOp::VarPop
                    | AggOp::StddevSamp
                    | AggOp::StddevPop
                    | AggOp::BitAnd
                    | AggOp::BitOr
            ) {
                return Err(Refuse::AggUnsupported { what: "scalar-fold-agg" });
            }
        }
    }
    // [P4-1] fused-arithmetic homes: the fused filter-agg body and the
    // (text, text) grouped cells arm; any other family refuses typed.
    if node.agg.iter().any(|a| a.expr.is_some())
        && node.family != ir::Family::FusedFilterAgg
        && !(node.family == ir::Family::HashPlaneOwnedGroup
            && (text_pair_cells_shape(bank, &node) || byval_cells_shape(bank, &node)))
    {
        return Err(Refuse::AggUnsupported { what: "agg-arith-family" });
    }
    // A fused leg composes with no FILTER mask on the byval cells arm.
    if node.family == ir::Family::HashPlaneOwnedGroup
        && byval_cells_shape(bank, &node)
        && node.agg.iter().enumerate().any(|(ai, a)| {
            a.expr.is_some()
                && node.params.agg_filters.get(ai).map(Option::is_some).unwrap_or(false)
        })
    {
        return Err(Refuse::AggUnsupported { what: "agg-arith-filter" });
    }
    if node.agg.iter().any(|a| a.op == AggOp::SumShifted)
        && !matches!(node.family, ir::Family::MetadataAnswer | ir::Family::FusedFilterAgg)
    {
        return Err(Refuse::AggUnsupported { what: "agg-arith-family" });
    }
    // MetadataAnswer takes a predicate only when it is stats-answerable
    // (one `= 0` / `<> 0` term — the zero-count arithmetic).
    if node.family == ir::Family::MetadataAnswer {
        if let Some(p) = &node.pred {
            let ok = p.var_terms.is_empty()
                && p.terms.len() == 1
                && p.terms[0].lo == 0
                && matches!(p.terms[0].op, CmpOp::Eq | CmpOp::Ne);
            if !ok {
                return Err(Refuse::MetadataPredNotStatsAnswerable);
            }
        }
    }

    // ---- ELECT (tier 2): [spill-3] budget-aware cross-family
    // re-election (spill-design.md §6): over the E18 budget a grouped
    // pure count-distinct in the dense-distinct spill vocabulary (the
    // ONE `spill_class` authority) re-elects onto the hash plane and
    // SERVES instead of refusing; otherwise nothing changes.
    if node.family == ir::Family::DistinctPipeline
        && !node.params.group_cols.is_empty()
        && faces.cfg.spill
        && (bank.rows_total() as u128)
            * (crate::stencils::hash_plane::SCATTER_ROW_BYTES as u128)
            > faces.cfg.grouped_budget_bytes() as u128
        && node
            .pred
            .as_ref()
            .map(|p| p.terms.is_empty() && p.var_terms.is_empty())
            .unwrap_or(true)
        && node.params.ne_empty_cols.is_empty()
        && crate::spill::available()
        && crate::stencils::hash_plane::spill_class(bank, faces, &node)
            == crate::stencils::hash_plane::SpillClass::SpillableDistinct
    {
        if crate::engine::phase_on() {
            println!(
                "SQEELECT|lower|q{}|family=HashPlaneOwnedGroup|rule=distinct_budget_reelect|",
                node.q
            );
        }
        node.family = ir::Family::HashPlaneOwnedGroup;
    }

    // ---- ELECT (tier 2): partition-law inputs per shape (measured laws;
    // the stencil consumes them, never re-elects) -------------------------
    if node.family == ir::Family::DistinctPipeline
        && node.params.group_cols.is_empty()
        && node.agg.iter().any(|a| {
            a.op == AggOp::CountDistinct
                && a.col.map(|c| crate::stencils::col_width(bank, c) > 0).unwrap_or(false)
        })
    {
        // distinct-pipeline law: 8B key slots (key+1 encoding), 1MB effective-L2
        // budget (measured: P=512 beat 256/1024/2048 at 100m).
        node.params.slot_bytes = 8;
        node.params.l2_bytes = 1 << 20;
    }

    // ---- ELECT (tier 2): [R6d] survivor-plane grain for the verdict/
    // condcache lane. Guard failure lowers to the row-grain plane (flag),
    // never a refusal; the flag's only consumer is the WindowReplay var
    // lane (SurvivorGather keeps its own NeEmpty entry path this rung).
    if node.family == ir::Family::WindowReplay {
        if let Some(p) = &node.pred {
            if let Some(t0) = p.var_terms.first() {
                let g = R6dGuards::default();
                let grouped = !node.params.group_cols.is_empty();
                if elect_entry_grain(bank, faces, t0, grouped, &g) == EntryGrain::Row {
                    node.params.flags |= ir::F_ROW_GRAIN;
                }
            }
        }
    }

    // ---- ELECT (tier 2): goal/request vector ------------------------------
    node.params.goal.fingerprints = condcache_goal(&node);
    node.params.goal.claim_class = Some(claim_class(node.family));
    node.params.goal.canonicalize();
    Ok(node)
}

/// The grouped stencils' emit cap: without a pushed-down bound, every
/// grouped answer path truncates at `offset + limit.min(GROUP_ROW_CAP)`
/// — so serving is sound only under a group-count witness at or under
/// this cap, OR under a pushed-down bound at or under it (a k-bounded
/// answer cannot truncate), OR under a fused HAVING whose answer path
/// emits ONLY the surviving groups untruncated (the [sqe-tpch-mech]
/// exception below), OR — [cap-retire], RULED 2026-08-19 — when the
/// hash-plane family serves the shape under bounded state (direct-array
/// or spill arm): those shapes emit the FULL true group set and the
/// finalize answer-bytes law (`Refuse::GroupAnswerOverBudget`) replaces
/// this cap's answer-plane protection with an exact counted one.
pub const GROUP_ROW_CAP: u64 = 1 << 20;

/// The dense-domain distinct-mix stencil's key-domain cap (the flat
/// per-group arrays are sized `hi - lo + 1`): ONE authority shared by
/// the admission gate and the stencil's own assert — the same
/// one-authority law as `dense_domain` (F7 fix).
pub const DENSE_DISTINCT_DOMAIN: u64 = 1 << 22;

// key_count_witness moved behind the membrane: `Witness::key_count`
// (witness.rs) — same sources (dict entry sums, exact domains, narrow-word
// type domains), now the only way to mint the gate's input type.

/// [sqe-tpch-mech] Direct-array grouped-state budget (BYTES) for the
/// shared accumulator array: one u64 count lane plus two 8-byte lanes
/// (value + non-null count) per fold aggregate. Provenance: Q18's SF1
/// orderkey domain (6M) at one Sum lane = 144 MB — inside; the cap keeps
/// the array a fraction of laptop-class memory. Now a cost_params knob
/// (E17); this alias keeps the one authority readable at the call sites.
pub fn direct_array_bytes_cap(bounded: bool) -> usize {
    crate::cost_params::target().direct_array_cap(bounded)
}

/// [sqe-tpch-mech] Witnessed dense-and-bounded key-domain election for
/// the direct-array grouped state (tpch-convergence-1 §6 mechanism 1).
/// The domain is minted through the membrane (`Witness::exact_domain` —
/// exact part-record min/max only; estimates never decide). `fold_lanes`
/// = the number of cell-folding aggregates (the budget is a function of
/// the lane count, never a constant). The caller owns any nullability
/// requirement (single-table keys demand null-free; the join sink
/// threads NULL keys aside). None = hash arm.
pub fn direct_array_domain(
    bank: &Bank,
    faces: &Faces,
    col: u32,
    fold_lanes: usize,
) -> Option<(i64, usize)> {
    direct_array_domain_capped(bank, faces, col, fold_lanes, direct_array_bytes_cap(false))
}

/// [sqe-hugedom] The budgeted form: the caller picks the byte cap under
/// the answer-bound law — `direct_array_bytes_cap(false)` for unbounded
/// answers, `direct_array_bytes_cap(true)` for BOUNDED answers (pushed
/// top-k or fused HAVING) whose fold lanes are all zero-init
/// (Sum/Count), so the array rides lazily-zeroed pages and resident
/// bytes track occupancy, not domain width.
pub fn direct_array_domain_capped(
    bank: &Bank,
    faces: &Faces,
    col: u32,
    fold_lanes: usize,
    cap_bytes: usize,
) -> Option<(i64, usize)> {
    use crate::bank::Face;
    if !matches!(
        bank.face(col),
        Face::SignedWord(_) | Face::UnsignedWord(1..=4) | Face::Bool
    ) {
        return None;
    }
    let (lo, hi) = crate::witness::Witness::exact_domain(&faces.stats(bank, col))?.value();
    let dn = (hi as i128) - (lo as i128) + 1;
    let bytes_per_slot = 8i128 * (1 + 2 * fold_lanes as i128);
    if dn <= 0 || dn.saturating_mul(bytes_per_slot) > cap_bytes as i128 {
        return None;
    }
    Some((lo, dn as usize))
}

/// [sqe-tpch-mech] i64-sum overflow witness for a direct-array atomic
/// Sum lane: the witnessed |input| bound (membrane-minted exact domain)
/// times the fold-row bound stays strictly inside i64 (the hash arm's
/// exact-i128 cells need no such witness — an unwitnessed input keeps
/// the hash arm).
pub fn direct_sum_fits(bank: &Bank, faces: &Faces, col: u32, rows: u64) -> bool {
    crate::witness::Witness::exact_domain(&faces.stats(bank, col))
        .map(|w| {
            let (lo, hi) = w.value();
            let m = lo.unsigned_abs().max(hi.unsigned_abs()) as u128;
            m.saturating_mul(rows.max(1) as u128) < i64::MAX as u128
        })
        .unwrap_or(false)
}

/// [sqe-avglen] The entrylen consumption law: the one `<> ''` conjunct
/// names the varlena AvgLen input itself, so the fold's len!=0 gate IS
/// the predicate (empty/NULL rows fold nothing — count excludes them and
/// an empty entry adds 0 to the byte sum) — nothing drops silently. The
/// agg set is exactly one AvgLen plus COUNT(*) columns.
fn entrylen_ne_consumed(bank: &Bank, node: &PlanNode) -> bool {
    let mut avglens = node.agg.iter().filter(|a| a.op.is_avglen());
    let src = match (avglens.next().and_then(|a| a.col), avglens.next()) {
        (Some(c), None) => c,
        _ => return false,
    };
    crate::stencils::col_width(bank, src) == 0
        && node.agg.iter().all(|a| a.op.is_avglen() || a.op == AggOp::CountStar)
        && node.params.ne_empty_cols.as_slice() == [src]
        && node.pred.is_none()
}

/// SERVER-path grouped admission (P3 rung C), fail-closed: mirrors the
/// grouped stencils' internal contracts (pred routing, key signatures,
/// agg sets, stats-witnessed lane bounds) so an unservable lowered plan
/// refuses typed instead of panicking. Seam-only — the rig's curated
/// lanes carry the wider proven vocabulary and are not gated here.
pub fn check_server_grouped(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    use crate::ir::Family as F;
    match node.family {
        F::MetadataAnswer
        | F::FusedFilterAgg
        | F::HashPlaneOwnedGroup
        | F::TwoLevelCodeAgg
        | F::SurvivorGather
        | F::DistinctPipeline => {}
        // [sqe-expr-keys] derived-key families serve grouped server plans
        // too: the per-dict-entry derivation fold (regexp-extract class)
        // and the dense minute-domain fold (date_trunc class). Vocabulary
        // checks in their arms below; everything outside refuses typed.
        F::DerivedKeyFold | F::DenseDomainGroup => {}
        // [winagg] window-replay serves grouped too: the VAR lane (one
        // varlena key under LIKE/Contains conjuncts — the grouped-LIKE
        // deferral lifts) and the FRAME lane ((Eq!=0, Between) pair with
        // rank-window keys). Vocabulary checks in this function's
        // WindowReplay arm below; everything outside refuses typed.
        F::WindowReplay => {}
        // [sortgrp v1] the tier-2 order-sensitive family; budget + shape
        // admission in its arm below (grouped AND ungrouped).
        F::SortGrouped => {}
        f => return Err(Refuse::FamilyUnservedServer { family: f }),
    }
    // [aggqual] Per-agg FILTER admission (fail-closed, family-gated):
    // the composition is implemented in exactly two bodies — the
    // ungrouped filtered fold (FusedFilterAgg) and the grouped Cells128
    // fold route (HashPlaneOwnedGroup byval foundation). Everywhere
    // else a filter-bearing plan refuses typed; per-leg vocabulary is
    // the cell-fold set over bare columns (expr/shift/distinct legs
    // keep their refusals — a filtered leg the serving arm cannot gate
    // would silently fold unfiltered rows, the wrong-answer machine).
    if node.params.agg_filters.iter().any(Option::is_some) {
        if node.params.agg_filters.len() != node.agg.len() {
            return Err(Refuse::AggUnsupported { what: "agg-filter-align" });
        }
        let grouped = !node.params.group_cols.is_empty();
        let family_ok = if grouped {
            node.family == F::HashPlaneOwnedGroup
        } else {
            node.family == F::FusedFilterAgg
        };
        if !family_ok {
            return Err(Refuse::AggUnsupported { what: "agg-filter-family" });
        }
        if grouped {
            // Cells128 foundation shape only: 1-2 plain byval keys (the
            // packed-key fold route is the one filter-threaded body).
            let byval_keys = node.params.group_cols.len() <= 2
                && node
                    .params
                    .group_cols
                    .iter()
                    .all(|&g| crate::stencils::col_width(bank, g) > 0);
            if !byval_keys
                || node.params.having.is_some()
                || !node.params.key_exprs.is_empty()
            {
                return Err(Refuse::AggUnsupported { what: "agg-filter-shape" });
            }
        }
        for (a, f) in node.agg.iter().zip(&node.params.agg_filters) {
            if f.is_none() {
                continue;
            }
            let vocab_ok = matches!(
                a.op,
                AggOp::CountStar
                    | AggOp::Sum
                    | AggOp::Avg
                    | AggOp::Min
                    | AggOp::Max
                    | AggOp::VarSamp
                    | AggOp::VarPop
                    | AggOp::StddevSamp
                    | AggOp::StddevPop
                    | AggOp::BitAnd
                    | AggOp::BitOr
            ) && a.expr.is_none();
            if !vocab_ok {
                return Err(Refuse::AggUnsupported { what: "agg-filter-leg" });
            }
        }
    }
    // A zero-part bank never runs a stencil (the empty-bank identity
    // path answers) — its stats gates are vacuous.
    if bank.parts.is_empty() {
        return Ok(());
    }
    // [sortgrp v1] witnessed payload-byte budget — grouped AND ungrouped
    // (the ungrouped one-group sort holds the same survivor bytes).
    if node.family == F::SortGrouped {
        check_sortagg_budget(bank, faces, node)?;
    }
    // [P6-1 spill] the grouped memory law (spill-design.md §2/§4): the
    // hash-plane family's scatter plane is O(rows); over the E18 budget
    // a spill-served shape proceeds (the stencil's spill arm bounds it)
    // and a shape with no spill arm refuses typed — never an OOM. The
    // kill switch reverts to the legacy (unaccounted) arm.
    if node.family == F::HashPlaneOwnedGroup {
        check_grouped_budget(bank, faces, node)?;
    }
    // [spill-2] the two-level family's memory law: its one row-scaled
    // plane (the merge-join slot scatter) spills; the other routes hold
    // dict/domain-bounded state.
    if node.family == F::TwoLevelCodeAgg {
        check_two_level_budget(bank, faces, node)?;
    }
    // [spill-2] the distinct pipeline's memory law: every arm is
    // row-scaled with no spill path this rung — fail-closed over budget.
    if node.family == F::DistinctPipeline {
        check_distinct_budget(bank, faces, node)?;
    }
    // [spill-5] the condition-cache VAR lane's fp128 distinct plane.
    if node.family == F::WindowReplay {
        check_window_var_budget(bank, faces, node)?;
    }
    // The distinct pipeline carries server contracts on its ungrouped
    // arms too; every other ungrouped shape is admitted upstream.
    if node.params.group_cols.is_empty()
        && !matches!(node.family, F::DistinctPipeline | F::SortGrouped)
    {
        return Ok(());
    }
    if node.params.group_cols.is_empty() && node.family == F::SortGrouped {
        // Ungrouped: one group, no group-count gate; shape checks only.
        return check_sortagg_shape(node);
    }
    let refuse = |what: &'static str| Err(Refuse::GroupServeUnsupported { what });
    let nterms = node.pred.as_ref().map(|p| p.terms.len()).unwrap_or(0);
    let nvar = node.pred.as_ref().map(|p| p.var_terms.len()).unwrap_or(0);
    // Expression keys serve only through the families whose stencils
    // evaluate them (hash plane / derived-key fold / dense minute domain
    // — validated in each family's arm below); any other family would
    // silently render the SOURCE columns and drop the derivation — the
    // wrong-answer class, refused typed.
    if !node.params.key_exprs.is_empty()
        && !matches!(
            node.family,
            F::HashPlaneOwnedGroup | F::DerivedKeyFold | F::DenseDomainGroup | F::WindowReplay
        )
    {
        return refuse("grouped-key-expr");
    }
    let g = &node.params.group_cols;
    let widths: Vec<u8> = g.iter().map(|&c| crate::stencils::col_width(bank, c)).collect();
    // Silent-drop guard: a `<> ''` conjunct outside the group-key set is
    // honored only by the var-term-promoting families; anywhere else it
    // would vanish from the plan — refuse, never a wrong answer.
    let ne = &node.params.ne_empty_cols;
    let ne_on_keys = ne.iter().all(|c| g.contains(c));
    // WindowReplay honors var terms natively (its var lane IS the
    // verdict/condcache family) and NeEmpty conjuncts are promoted into
    // var terms by the shared lowering — neither drops silently.
    if !matches!(node.family, F::SurvivorGather | F::WindowReplay | F::SortGrouped) {
        if nvar != 0 {
            return refuse("grouped-var-pred");
        }
        // [sqe-avglen] exception: the entrylen arm CONSUMES its `<> ''`
        // conjunct as the fold's len!=0 law (re-validated in its arm).
        let entrylen_ne =
            node.family == F::TwoLevelCodeAgg && entrylen_ne_consumed(bank, node);
        if !ne.is_empty() && !ne_on_keys && !entrylen_ne {
            return refuse("grouped-ne-empty-residue");
        }
    }
    // Grouped-answer bound gate: every grouped stencil emits at most
    // GROUP_ROW_CAP rows, so admission demands a witness bounding the
    // true group count under the cap — an unwitnessed grouped shape
    // would risk silent truncation, the loudest wrong-answer class.
    // Sound witnesses only: exact stats domains, narrow-word type
    // domains, per-part dict entry sums, the minute-of-hour range.
    // The HLL ndv estimate never participates (its own law: estimates
    // never decide verdicts).
    //
    // EXCEPTION (the down-pass law): a shape whose answer is bounded by
    // a pushed-down offset+k <= GROUP_ROW_CAP is admitted regardless of
    // group-count witness — the cap protected ANSWER materialization,
    // and a k-bounded answer cannot truncate (every group is still
    // folded and finalized; only the top-k selection is emitted).
    // Unbounded grouped shapes keep the witness requirement unchanged.
    let k_bounded =
        node.params.topk.as_ref().is_some_and(|t| t.n > 0 && t.n as u64 <= GROUP_ROW_CAP);
    if !k_bounded {
        // [spill-2] ONE authority with the byte-key spill dispatch:
        // `grouped_count_bound` (extracted verbatim from the inline
        // computation this gate carried since the witness law landed).
        let bound = grouped_count_bound(bank, faces, node);
        let witnessed = bound.is_some();
        // [cap-retire] RULED (Michael 2026-08-19, spill-design.md §5):
        // correctness-first — a grouped shape the hash-plane family holds
        // under BOUNDED state (direct-array election under budget, or the
        // SoA spill arm with a registered substrate) is SERVED regardless
        // of the group-count witness: it answers the TRUE group set or
        // refuses typed at finalize under the exact answer-bytes law
        // (`GroupAnswerOverBudget`, E17 answer face) — never truncates,
        // never OOMs. Shapes outside spill coverage (other families, the
        // frame-walk route, §6 residue arms) KEEP the witness gate: they
        // could still grow state without bound, and honesty refuses them.
        let spill_served = || {
            node.family == F::HashPlaneOwnedGroup
                && crate::stencils::hash_plane::cap_retire_serves(bank, faces, node)
        };
        if !witnessed {
            if !spill_served() {
                return Err(Refuse::GroupCountUnwitnessed { what: "no-witness" });
            }
        } else if bound.expect("witnessed") > GROUP_ROW_CAP {
            // [sqe-tpch-mech] HAVING-fused exception: a grouped shape
            // whose HAVING filter is fused into the fold emits ONLY the
            // surviving groups and NEVER truncates (the foundation /
            // direct-array routes filter at the answer boundary with
            // limit = MAX), so the emit-cap witness is not required.
            // Admission instead demands the witnessed dense-and-bounded
            // key domain that keeps the fold STATE inside the direct-
            // array budget (the same witness serves the hash fallback:
            // its group count is bounded by the same domain).
            let fold_lanes =
                node.agg.iter().filter(|a| crate::fold::fold_op_of(a.op).is_some()).count();
            // [sqe-hugedom] a fused-HAVING answer is BOUNDED (only the
            // survivors emit), so the accumulator prices under the
            // bounded budget — provided every lane is zero-init
            // (Sum/Count ride lazily-zeroed pages; Min/Max sentinel
            // fills pay domain-width writes and keep the unbounded cap).
            let zero_init_lanes = node.agg.iter().all(|a| {
                matches!(crate::fold::fold_op_of(a.op), None | Some(crate::fold::AggFoldOp::Sum))
            });
            let having_fused = node.params.having.is_some()
                && node.family == F::HashPlaneOwnedGroup
                && node.params.key_exprs.is_empty()
                && g.len() == 1
                && widths[0] > 0
                && bank.null_free(g[0])
                && direct_array_domain_capped(
                    bank,
                    faces,
                    g[0],
                    fold_lanes,
                    direct_array_bytes_cap(zero_init_lanes),
                )
                .is_some();
            if !having_fused && !spill_served() {
                return Err(Refuse::GroupCountUnwitnessed { what: "bound-over-cap" });
            }
        }
    }
    // [ruling 2] lane-bound range proofs ride the membrane too: exact
    // part-record domains only.
    let fits = |col: u32, lo: i64, hi: i64| -> bool {
        Witness::exact_domain(&faces.stats(bank, col))
            .map(|w| {
                let (a, b) = w.value();
                a >= lo && b <= hi
            })
            .unwrap_or(false)
    };
    match node.family {
        F::DistinctPipeline => {
            // One COUNT(DISTINCT col) column over the stencil's width
            // signatures; predicates stay off this family's server arms.
            if nterms != 0 {
                return refuse("grouped-distinct-pred");
            }
            // [aggqual] the ungrouped byval arm (widths []) serves the
            // whole distinct-fold set over ONE shared input column
            // (count/sum/avg DISTINCT — int_set's one dedup feeds every
            // leg); the grouped arms stay COUNT(DISTINCT)-only.
            let d = if widths.is_empty() {
                let mut shared: Option<u32> = None;
                for a in &node.agg {
                    let dist_ok = matches!(
                        a.op,
                        AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct
                    );
                    if !dist_ok || a.col.is_none() {
                        return refuse("grouped-distinct-agg");
                    }
                    if matches!(a.op, AggOp::SumDistinct | AggOp::AvgDistinct)
                        && crate::stencils::col_width(bank, a.col.expect("col checked")) == 0
                    {
                        return refuse("grouped-distinct-input");
                    }
                    match shared {
                        None => shared = a.col,
                        Some(c) if a.col == Some(c) => {}
                        Some(_) => return refuse("grouped-distinct-agg"),
                    }
                }
                shared
            } else {
                match node.agg.as_slice() {
                    [a] if a.op == AggOp::CountDistinct => a.col,
                    _ => None,
                }
            };
            let Some(d) = d else {
                return refuse("grouped-distinct-agg");
            };
            let dw = crate::stencils::col_width(bank, d);
            match widths.as_slice() {
                [] => {
                    // [q5-fulldomain] Byval set: served over the FULL
                    // signed domain. The key+1 slot encoding's ONE
                    // colliding key (the all-ones word — value -1, whose
                    // key+1 is the empty slot) is handled out-of-band by
                    // the stencil's escape flag (int_set/int_set_spill
                    // divert it before scatter and fold it exactly once
                    // at finalize), so no domain witness is required —
                    // the same posture as the text arm. History: this
                    // arm demanded a witnessed non-negative domain
                    // ("grouped-distinct-domain"), which refused
                    // COUNT(DISTINCT UserID) on EVERY ClickBench hits
                    // bank (UserID is a full-range u64 hash: stored
                    // signed, min < 0 on real data — cbsubmit cell
                    // 20260821T151744Z, cause tier/step/group).
                    // The text set renders exactly one count column.
                    if dw == 0 && node.agg.len() != 1 {
                        return refuse("grouped-distinct-agg");
                    }
                }
                [w] if *w > 0 => {
                    if dw == 0 {
                        return refuse("grouped-distinct-input");
                    }
                    let dense = Witness::exact_domain(&faces.stats(bank, g[0]))
                        .map(|w| {
                            let (lo, hi) = w.value();
                            lo >= 0 && hi < 1 << 20
                        })
                        .unwrap_or(false);
                    // [spill-4] a non-dense domain serves through the
                    // grouped pair-spill arm (fail-closed without it);
                    // the group-count witness gate below still applies.
                    if !dense
                        && !(faces.cfg.spill
                            && crate::spill::available()
                            && crate::stencils::distinct::pair_spill_serves(bank, node))
                    {
                        return refuse("grouped-distinct-domain");
                    }
                }
                [0] => {
                    if dw == 0 {
                        return refuse("grouped-distinct-input");
                    }
                }
                [w, 0] if *w > 0 => {
                    if dw == 0 {
                        return refuse("grouped-distinct-input");
                    }
                    // The packed pair carries the int lane in 16 aux bits.
                    if 8 * (*w as u32) > 16 {
                        return refuse("grouped-distinct-key");
                    }
                }
                _ => return refuse("grouped-distinct-key"),
            }
        }
        F::TwoLevelCodeAgg => {
            // [sqe-avglen] dense-int entrylen arm: ONE small-domain byval
            // key, the {AvgLen(x), COUNT(*)} mix whose one conjunct is
            // `x <> ''` (consumed by the len!=0 fold law); answers fold
            // per-part dict entry lengths (hydrated 3VL fallback per
            // part). HAVING count and the exact-ratio/count emission
            // orders are the arm's own render vocabulary.
            if matches!(widths.as_slice(), [w] if *w > 0) {
                if nterms != 0 || nvar != 0 || !entrylen_ne_consumed(bank, node) {
                    return refuse("grouped-entrylen-shape");
                }
                if crate::stencils::code_agg::entrylen_dense_bounds(&faces.stats(bank, g[0]))
                    .is_none()
                {
                    return refuse("grouped-entrylen-domain");
                }
                match node.params.order {
                    crate::ir::OrderBy::None | crate::ir::OrderBy::CountDesc => {}
                    crate::ir::OrderBy::AggDesc(i)
                        if node.agg.get(i as usize).map(|a| a.op).is_some_and(|o| o.is_avglen()) => {}
                    _ => return refuse("grouped-entrylen-order"),
                }
                return Ok(());
            }
            // Single varlena key, one COUNT(*) column: the code-count
            // arms (whole-part dense count / frame merge-join).
            if widths.as_slice() != [0] {
                return refuse("grouped-twolevel-key");
            }
            if !(node.agg.len() == 1 && node.agg[0].op == AggOp::CountStar) {
                return refuse("grouped-twolevel-agg");
            }
            if node.params.having_min_count != 0 {
                return refuse("grouped-twolevel-having");
            }
        }
        F::SurvivorGather => {
            // The gather stencil's exact shape: byval keys, one NeEmpty
            // varlena frame term (no int residues), COUNT(*) + one SUM +
            // one AVG with stats-witnessed lane bounds.
            if nterms != 0 {
                return refuse("grouped-gather-pred");
            }
            let one_ne = node
                .pred
                .as_ref()
                .map(|p| {
                    p.var_terms.len() == 1
                        && p.var_terms[0].op == crate::ir::VarOp::NeEmpty
                })
                .unwrap_or(false);
            if !one_ne || ne.len() != 1 || ne_on_keys {
                return refuse("grouped-gather-pred");
            }
            if widths.iter().any(|&w| w == 0) {
                return refuse("grouped-gather-key");
            }
            let total_bits: u32 = widths.iter().map(|&w| 8 * w as u32).sum();
            // The packed-key sentinel (all-ones) must be unreachable.
            if total_bits == 64 || total_bits >= 128 {
                return refuse("grouped-gather-key");
            }
            let (mut nsum, mut navg) = (0usize, 0usize);
            for a in &node.agg {
                match a.op {
                    AggOp::CountStar => {}
                    AggOp::Sum => {
                        nsum += 1;
                        let Some(c) = a.col else {
                            return refuse("grouped-agg-input");
                        };
                        if !fits(c, 0, u32::MAX as i64) {
                            return refuse("grouped-sum-range");
                        }
                    }
                    AggOp::Avg => {
                        navg += 1;
                        let Some(c) = a.col else {
                            return refuse("grouped-agg-input");
                        };
                        if !fits(c, 0, i64::MAX) {
                            return refuse("grouped-avg-range");
                        }
                    }
                    AggOp::Min
                    | AggOp::Max
                    | AggOp::MinBytes
                    | AggOp::CountDistinct
                    | AggOp::SumDistinct
                    | AggOp::AvgDistinct
                    | AggOp::SumShifted
                    | AggOp::AvgLen
                    | AggOp::AvgCharLen
                    | AggOp::VarSamp
                    | AggOp::VarPop
                    | AggOp::StddevSamp
                    | AggOp::StddevPop
                    | AggOp::BitAnd
                    | AggOp::BitOr
                    | AggOp::EmitMatches
                    | AggOp::StringAgg
                    | AggOp::ArrayAgg
                    | AggOp::ArrayAggDistinct
                    | AggOp::PercentileDisc
                    | AggOp::PercentileCont
                    | AggOp::Mode => return refuse("grouped-agg"),
                }
            }
            if nsum != 1 || navg != 1 {
                return refuse("grouped-gather-agg");
            }
        }
        F::FusedFilterAgg => {
            if nterms != 1 {
                return refuse("grouped-filteragg-pred");
            }
            if g.len() != 1 || widths[0] == 0 || node.cols.first() != Some(&g[0]) {
                return refuse("grouped-filteragg-key");
            }
            if !(node.agg.len() == 1 && node.agg[0].op == AggOp::CountStar) {
                return refuse("grouped-filteragg-agg");
            }
            // [F7 fix] admission reads the SAME dense authority the
            // election and the stencil read (planner::dense_domain) —
            // no elect-then-refuse gap left to guard, but a plan lowered
            // elsewhere still refuses here instead of panicking.
            if dense_domain(&faces.stats(bank, g[0])).is_none() {
                return refuse("grouped-dense-domain");
            }
        }
        F::HashPlaneOwnedGroup => {
            // [sqe-expr-keys] the gated-text frame shape (CASE key) is the
            // one predicate-bearing route this family serves — every other
            // shape stays predicate-free.
            let has_case = node
                .params
                .key_exprs
                .iter()
                .any(|e| matches!(e, crate::ir::KeyExpr::CaseSrc { .. }));
            // [tpch-wave-3] the cells arm takes word conjuncts only.
            let cells_pred_ok = (text_pair_cells_shape(bank, node)
                || byval_cells_shape(bank, node))
                && nvar == 0
                && node.pred.as_ref().map(|p| p.col_terms.is_empty()).unwrap_or(true);
            if nterms != 0 && !has_case && !cells_pred_ok {
                return refuse("grouped-pred");
            }
            // Dense-domain distinct mix: a CountDistinct leg among plain
            // folds routes the stencil to the dense-domain + distinct-
            // pipeline form. Fail-closed mirror of that form's contract:
            // one byval key with an exact witnessed domain inside the
            // dense cap, exactly one byval CountDistinct input, the other
            // legs from its CountStar/Sum/Avg render set over bare signed
            // words with i64-provable sums (its accumulators are u64
            // lanes, not the exact-i128 cells), no fused HAVING (its
            // answer path carries no group filter).
            if node.agg.iter().any(|a| {
                matches!(a.op, AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct)
            }) {
                use crate::bank::Face;
                if !node.params.key_exprs.is_empty() {
                    return refuse("grouped-distinct-mix-key");
                }
                if node.params.having.is_some() {
                    return refuse("grouped-distinct-mix-having");
                }
                if widths.len() != 1 || widths[0] == 0 {
                    return refuse("grouped-distinct-mix-key");
                }
                let dense_ok = Witness::exact_domain(&faces.stats(bank, g[0]))
                    .map(|d| {
                        let (lo, hi) = d.value();
                        (hi as i128 - lo as i128) + 1 <= DENSE_DISTINCT_DOMAIN as i128
                    })
                    .unwrap_or(false);
                if !dense_ok {
                    return refuse("grouped-distinct-mix-domain");
                }
                // [aggqual] ONE distinct input column shared by every
                // distinct-class leg (count/sum/avg DISTINCT — the form
                // runs one pair-dedup; a second distinct column would
                // need a second scatter, refused typed). Distinct sums
                // ride the same u64 lanes as the plain sums — same
                // witnessed i64-provable bound (the distinct fold is a
                // subset of the rows).
                let mut d_shared: Option<u32> = None;
                for a in &node.agg {
                    match a.op {
                        AggOp::CountStar => {}
                        AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct => {
                            let Some(c) = a.col else {
                                return refuse("grouped-distinct-mix-input");
                            };
                            if crate::stencils::col_width(bank, c) == 0 {
                                return refuse("grouped-distinct-mix-input");
                            }
                            if matches!(a.op, AggOp::SumDistinct | AggOp::AvgDistinct)
                                && (!matches!(bank.face(c), Face::SignedWord(_))
                                    || !direct_sum_fits(bank, faces, c, bank.rows_total()))
                            {
                                return refuse("grouped-distinct-mix-sum");
                            }
                            match d_shared {
                                None => d_shared = Some(c),
                                Some(x) if x == c => {}
                                Some(_) => return refuse("grouped-distinct-mix-agg"),
                            }
                        }
                        AggOp::Sum | AggOp::Avg => {
                            let Some(c) = a.col else {
                                return refuse("grouped-agg-input");
                            };
                            if a.expr.is_some()
                                || !matches!(bank.face(c), Face::SignedWord(_))
                                || !direct_sum_fits(bank, faces, c, bank.rows_total())
                            {
                                return refuse("grouped-distinct-mix-sum");
                            }
                        }
                        _ => return refuse("grouped-distinct-mix-agg"),
                    }
                }
                if d_shared.is_none() {
                    return refuse("grouped-distinct-mix-agg");
                }
                return Ok(());
            }
            if has_case {
                // frame_owned_u128's exact contract: key_exprs =
                // [Col(int2) x 1..=3, CaseSrc, Col(varlena)] in that
                // order (answer columns emit in that order); the CASE
                // gates are int key lanes; one COUNT(*) column; the
                // promoted (Eq!=0, Between) frame pair anchors the walk.
                use crate::ir::KeyExpr as KE;
                let ke = &node.params.key_exprs;
                let n = ke.len();
                if n < 3 || n > 5 || nvar != 0 || !ne.is_empty() {
                    return refuse("grouped-key-expr");
                }
                let ints_ok = ke[..n - 2].iter().all(|e| {
                    matches!(e, KE::Col(c) if crate::stencils::col_width(bank, *c) == 2)
                });
                let KE::CaseSrc { se, adv, referer } = ke[n - 2] else {
                    return refuse("grouped-key-expr");
                };
                let dst_ok = matches!(ke[n - 1], KE::Col(c) if crate::stencils::col_width(bank, c) == 0);
                let gate_lane = |g: u32| ke[..n - 2].iter().any(|e| matches!(e, KE::Col(c) if *c == g));
                if !ints_ok
                    || !dst_ok
                    || crate::stencils::col_width(bank, referer) != 0
                    || !gate_lane(se)
                    || !gate_lane(adv)
                {
                    return refuse("grouped-key-expr");
                }
                if !(node.agg.len() == 1 && node.agg[0].op == AggOp::CountStar) {
                    return refuse("grouped-key-expr-agg");
                }
                let pair_ok = node.pred.as_ref().is_some_and(|p| {
                    let f = p.frame();
                    f.len() == 2
                        && f.iter().any(|t| t.op == CmpOp::Eq && t.lo != 0)
                        && f.iter().any(|t| t.op == CmpOp::Between)
                });
                if !pair_ok {
                    return refuse("grouped-key-expr-frame");
                }
                return Ok(());
            }
            // [sqe-expr-keys] injective-per-column keys (Col / col-minus-
            // const): grouping by the source columns IS the derived
            // grouping (every expr is injective in its column). Admission
            // = the plain-key checks below over the deduped source cols,
            // plus a no-overflow domain witness per MinusConst (PG errors
            // on int overflow; we refuse unless the exact domain proves
            // the shifted values stay in the column's type range).
            let plain_class = !node.params.key_exprs.is_empty()
                && node.params.key_exprs.iter().all(|e| {
                    matches!(e, crate::ir::KeyExpr::Col(_) | crate::ir::KeyExpr::MinusConst(..))
                });
            if plain_class {
                for e in &node.params.key_exprs {
                    if let crate::ir::KeyExpr::MinusConst(c, k) = e {
                        let w = crate::stencils::col_width(bank, *c);
                        let (ty_min, ty_max): (i128, i128) = match w {
                            2 => (i16::MIN as i128, i16::MAX as i128),
                            4 => (i32::MIN as i128, i32::MAX as i128),
                            8 => (i64::MIN as i128, i64::MAX as i128),
                            _ => return refuse("grouped-key-expr"),
                        };
                        let dom_ok = Witness::exact_domain(&faces.stats(bank, *c))
                            .map(|d| {
                                let (lo, hi) = d.value();
                                lo as i128 - *k as i128 >= ty_min
                                    && hi as i128 - *k as i128 <= ty_max
                            })
                            .unwrap_or(false);
                        if !dom_ok {
                            return refuse("minus-const-domain-unwitnessed");
                        }
                    }
                }
                // fall through to the plain-key checks below.
            } else if !node.params.key_exprs.is_empty() {
                // The ord-pack shape: {byval key, minute-of-timestamp,
                // varlena key}, whole-bank COUNT(*) only.
                use crate::ir::KeyExpr as KE;
                let ke = &node.params.key_exprs;
                let minutes: Vec<u32> = ke
                    .iter()
                    .filter_map(|e| match e {
                        KE::Minute(c) => Some(*c),
                        _ => None,
                    })
                    .collect();
                let plain: Vec<u32> = ke
                    .iter()
                    .filter_map(|e| match e {
                        KE::Col(c) => Some(*c),
                        _ => None,
                    })
                    .collect();
                let sig = ke.len() == 3
                    && minutes.len() == 1
                    && plain.len() == 2
                    && crate::stencils::col_width(bank, minutes[0]) == 8
                    && plain
                        .iter()
                        .filter(|&&c| {
                            c != minutes[0] && crate::stencils::col_width(bank, c) > 0
                        })
                        .count()
                        == 1
                    && plain
                        .iter()
                        .filter(|&&c| crate::stencils::col_width(bank, c) == 0)
                        .count()
                        == 1
                    && ne.is_empty()
                    && node.agg.iter().all(|a| a.op == AggOp::CountStar);
                if !sig {
                    return refuse("grouped-key-expr");
                }
                return Ok(());
            }
            if g.len() > 2 {
                return refuse("grouped-key-count");
            }
            let count_only = node.agg.iter().all(|a| a.op == AggOp::CountStar);
            match widths.as_slice() {
                [0] => {
                    if !count_only {
                        return refuse("grouped-text-agg");
                    }
                }
                [0, 0] => {
                    // Other mixes ride the cells twin ([tpch-wave-3]):
                    // word/packed folds, null-free keys, no FILTER.
                    if !(node.agg.len() == 1 && count_only) {
                        use crate::bank::Face;
                        if !node.params.agg_filters.iter().all(Option::is_none) {
                            return refuse("grouped-text-agg");
                        }
                        if !g.iter().all(|&c| bank.null_free(c)) {
                            return refuse("grouped-text-key-null");
                        }
                        for a in &node.agg {
                            match a.op {
                                AggOp::CountStar => {}
                                AggOp::Sum | AggOp::Avg | AggOp::Min | AggOp::Max => {
                                    let Some(c) = a.col else {
                                        return refuse("grouped-agg-input");
                                    };
                                    if !matches!(
                                        bank.face(c),
                                        Face::SignedWord(_)
                                            | Face::UnsignedWord(1..=4)
                                            | Face::Bool
                                            | Face::PackedNumeric { .. }
                                    ) {
                                        return refuse("grouped-fold-face");
                                    }
                                }
                                _ => return refuse("grouped-text-agg"),
                            }
                        }
                    }
                }
                [w, 0] if *w > 0 => {
                    if !count_only {
                        return refuse("grouped-text-agg");
                    }
                }
                [0, _] => return refuse("grouped-key-order"),
                _ => {
                    // byval foundation (sqe-grpfold): the fold vocabulary
                    // is COUNT(*)/SUM/MIN/MAX/AVG over word-embeddable
                    // inputs. The tuned packed/wide payload lanes keep
                    // their stats-witnessed elections INSIDE the stencil;
                    // every other admitted mix rides the Cells128 fold
                    // route (exact i128 sum cells — no range witness).
                    use crate::bank::Face;
                    for a in &node.agg {
                        match a.op {
                            AggOp::CountStar => {}
                            AggOp::Sum
                            | AggOp::Avg
                            | AggOp::Min
                            | AggOp::Max
                            | AggOp::VarSamp
                            | AggOp::VarPop
                            | AggOp::StddevSamp
                            | AggOp::StddevPop
                            | AggOp::BitAnd
                            | AggOp::BitOr => {
                                let Some(c) = a.col else {
                                    return refuse("grouped-agg-input");
                                };
                                // float/varlena/fixed fold inputs carry no
                                // exact order-preserving word embed on this
                                // arm — typed refusal, never a lossy fold.
                                // (check_currency already narrows the
                                // variance/bit families to signed words,
                                // and [packednum] admits the PackedNumeric
                                // word lane for sum/avg/min/max only.)
                                if !matches!(
                                    bank.face(c),
                                    Face::SignedWord(_)
                                        | Face::UnsignedWord(1..=4)
                                        | Face::Bool
                                        | Face::PackedNumeric { .. }
                                ) {
                                    return refuse("grouped-fold-face");
                                }
                            }
                            AggOp::MinBytes
                            | AggOp::CountDistinct
                            | AggOp::SumDistinct
                            | AggOp::AvgDistinct
                            | AggOp::SumShifted
                            | AggOp::AvgLen
                            | AggOp::AvgCharLen
                            | AggOp::EmitMatches
                            | AggOp::StringAgg
                            | AggOp::ArrayAgg
                            | AggOp::ArrayAggDistinct
                            | AggOp::PercentileDisc
                            | AggOp::PercentileCont
                            | AggOp::Mode => return refuse("grouped-agg"),
                        }
                    }
                }
            }
        }
        F::WindowReplay => {
            let nvar_terms = node.pred.as_ref().map(|p| p.var_terms.len()).unwrap_or(0);
            if nvar_terms > 0 {
                // VAR lane (Shape::Grouped): exactly one VARLENA group key
                // (the stencil decodes key payload bytes); aggs from the
                // lane's render vocabulary — MinBytes over a varlena
                // input, COUNT(*), COUNT(DISTINCT byval | varlena) (the
                // varlena set dedupes 128-bit entry fingerprints), and
                // MIN/MAX/SUM over a bare int word input.
                match node.params.key_exprs.as_slice() {
                    [] => {
                        if widths.as_slice() != [0] {
                            return refuse("grouped-window-var-key");
                        }
                    }
                    [crate::ir::KeyExpr::Col(t), crate::ir::KeyExpr::HourBucket { col, .. }]
                    | [crate::ir::KeyExpr::HourBucket { col, .. }, crate::ir::KeyExpr::Col(t)] => {
                        if crate::stencils::col_width(bank, *t) != 0
                            || crate::stencils::col_width(bank, *col) != 8
                            || !bank.null_free(*col)
                        {
                            return refuse("grouped-window-var-key");
                        }
                        let dom_ok = Witness::exact_domain(&faces.stats(bank, *col))
                            .map(|d| {
                                let (lo, hi) = d.value();
                                lo / 1_000_000 >= -210_866_803_200
                                    && hi / 1_000_000 < 9_224_318_016_000
                            })
                            .unwrap_or(false);
                        if !dom_ok {
                            return refuse("hour-bucket-domain-unwitnessed");
                        }
                    }
                    _ => return refuse("grouped-key-expr"),
                }
                for a in &node.agg {
                    let aw = a.col.map(|c| crate::stencils::col_width(bank, c));
                    match a.op {
                        AggOp::CountStar => {}
                        AggOp::MinBytes if aw == Some(0) => {}
                        AggOp::CountDistinct if aw.is_some() => {}
                        AggOp::Min | AggOp::Max | AggOp::Sum
                            if matches!(aw, Some(2 | 4 | 8)) && a.expr.is_none() => {}
                        _ => return refuse("grouped-window-var-agg"),
                    }
                }
                if let Some(t) = &node.params.topk {
                    let nkeys = node.params.key_exprs.len().max(1);
                    let word_leg = |c: u32| {
                        (c as usize)
                            .checked_sub(nkeys)
                            .and_then(|i| node.agg.get(i))
                            .is_some_and(|a| matches!(a.op, AggOp::Min | AggOp::Max | AggOp::Sum))
                    };
                    let ok = t.keys.iter().all(|k| match k.lo {
                        None => (k.col as usize) < nkeys + node.agg.len(),
                        Some(lo) => word_leg(k.col) && word_leg(lo),
                    });
                    if !ok {
                        return refuse("grouped-window-var-topk");
                    }
                }
            } else {
                if !node.params.key_exprs.is_empty() {
                    return refuse("grouped-key-expr");
                }
                // FRAME lane (Shape::FrameGroup): the cold path anchors on
                // the promoted shared-frame pair — its absence would panic
                // downstream, so it is an admission REQUIREMENT here.
                let pair_ok = node.pred.as_ref().is_some_and(|p| {
                    let f = p.frame();
                    f.len() == 2
                        && f.iter().any(|t| t.op == CmpOp::Eq && t.lo != 0)
                        && f.iter().any(|t| t.op == CmpOp::Between)
                });
                if !pair_ok {
                    return refuse("grouped-window-frame-pair");
                }
                // Rank-window render emits key fields + ONE count column.
                if !(node.agg.len() == 1 && node.agg[0].op == AggOp::CountStar) {
                    return refuse("grouped-window-frame-agg");
                }
                // Key packing law (key_bytes): at most one varlena field,
                // and only in the LAST key position.
                if widths[..widths.len() - 1].iter().any(|&w| w == 0) {
                    return refuse("grouped-window-frame-key");
                }
            }
        }
        // [sqe-expr-keys] per-dict-entry derivation fold: ONE derived
        // varlena key from the closed extract vocabulary (HostRegex), the
        // `<> ''` residue at entry grain, aggs from the stencil's render
        // set over the SAME source column (the derivation and the folds
        // read one entry byte image). Order/HAVING/slice apply shell-side
        // over the full emitted group set (the group-count witness above
        // bounds it) — EXCEPT the [q28-serve] k-bounded lane: the seam
        // may fuse a strict `COUNT(*) > k` HAVING into the stencil's
        // `having_min_count` law and push the ORDER/LIMIT as a non-
        // native `topk` (survivors filter at pass-2 emission, the
        // answer-boundary selection keeps the top n — HAVING-before-
        // topk by construction), which admits through the k_bounded
        // exception above without a group-count witness. The general
        // `params.having` comparator is NOT implemented by this stencil
        // and refuses fail-closed below.
        F::DerivedKeyFold => {
            if nterms != 0 || nvar != 0 {
                return refuse("grouped-derived-pred");
            }
            let src = match node.params.key_exprs.as_slice() {
                [crate::ir::KeyExpr::HostRegex(c)] => *c,
                _ => return refuse("grouped-derived-key"),
            };
            if g != &[src] || crate::stencils::col_width(bank, src) != 0 {
                return refuse("grouped-derived-key");
            }
            if node.cols.first() != Some(&src) {
                return refuse("grouped-derived-key");
            }
            for a in &node.agg {
                match a.op {
                    AggOp::CountStar => {}
                    AggOp::AvgLen | AggOp::AvgCharLen | AggOp::MinBytes if a.col == Some(src) => {}
                    _ => return refuse("grouped-derived-agg"),
                }
            }
            match node.params.order {
                crate::ir::OrderBy::None => {}
                crate::ir::OrderBy::AggDesc(i)
                    if node.agg.get(i as usize).map(|a| a.op).is_some_and(|o| o.is_avglen()) => {}
                _ => return refuse("grouped-derived-order"),
            }
            // [q28-serve] the stencil implements the COUNT(*)-only
            // `having_min_count` filter; the generalized `having`
            // comparator would be silently ignored — refuse fail-closed.
            if node.params.having.is_some() {
                return refuse("grouped-derived-having");
            }
        }
        // [sqe-expr-keys] dense minute-domain fold: GROUP BY
        // date_trunc('minute', ts) under the promoted (Eq!=0, Between-
        // over-DATE) frame pair; one COUNT(*) column; the flat array is
        // sized from the date conjunct, so its span must be bounded.
        F::DenseDomainGroup => {
            let src = match node.params.key_exprs.as_slice() {
                [crate::ir::KeyExpr::TruncMinute(c)] => *c,
                _ => return refuse("grouped-dense-key"),
            };
            if g != &[src] || crate::stencils::col_width(bank, src) != 8 {
                return refuse("grouped-dense-key");
            }
            if !(node.agg.len() == 1 && node.agg[0].op == AggOp::CountStar) {
                return refuse("grouped-dense-agg");
            }
            if node.params.having_min_count != 0 {
                return refuse("grouped-dense-having");
            }
            if !matches!(
                node.params.order,
                crate::ir::OrderBy::None | crate::ir::OrderBy::KeyAsc
            ) {
                return refuse("grouped-dense-order");
            }
            // Frame pair with the DATE interval bounded on BOTH sides:
            // the stencil allocates (hi-lo+3)*1440 u32 slots from the
            // INTERSECTION of every date-interval conjunct (the server
            // lowers `>= lo` / `<= hi` as two one-sided Between terms;
            // the rig authors one closed Range — both shapes intersect
            // to the same closed interval).
            let Some(p) = node.pred.as_ref() else {
                return refuse("grouped-dense-frame");
            };
            let f = p.frame();
            let eq_ok = f.iter().any(|t| t.op == CmpOp::Eq && t.lo != 0);
            if !eq_ok || f.len() != 2 || !f.iter().any(|t| t.op == CmpOp::Between) {
                return refuse("grouped-dense-frame");
            }
            let (mut dlo, mut dhi, mut seen) = (i64::MIN, i64::MAX, false);
            for t in p.terms.iter() {
                if t.op == CmpOp::Between && node.is_date(t.col) {
                    dlo = dlo.max(t.lo);
                    dhi = dhi.min(t.hi);
                    seen = true;
                }
            }
            if !seen || dlo == i64::MIN || dhi == i64::MAX {
                return refuse("grouped-dense-frame");
            }
            let span = (dhi as i128 - dlo as i128).max(0);
            if span + 3 > (1 << 22) / 1440 {
                return refuse("grouped-dense-span");
            }
        }
        F::MetadataAnswer => return refuse("grouped-metaanswer"),
        // [sortgrp v1] plain word/varlena keys (widths checked by
        // check_currency); the family evaluates int + var conjuncts
        // row-major, so no pred-shape narrowing beyond the shared gates.
        F::SortGrouped => return check_sortagg_shape(node),
        _ => unreachable!("family allowlist above"),
    }
    Ok(())
}

/// [sortgrp v1] Shape admission shared by the grouped and ungrouped
/// arms: a sortagg spec must exist, delimiter slots align, every agg is
/// tier-2 (plan_from_ap constructs exactly this — fail-closed against
/// plans lowered elsewhere).
fn check_sortagg_shape(node: &PlanNode) -> Result<(), Refuse> {
    let Some(sa) = node.params.sortagg.as_ref() else {
        return Err(Refuse::SortAggUnsupported { what: "spec-missing" });
    };
    if sa.keys.is_empty() {
        return Err(Refuse::SortAggUnsupported { what: "order-missing" });
    }
    if sa.delims.len() != node.agg.len() || node.agg.is_empty() {
        return Err(Refuse::SortAggUnsupported { what: "spec-missing" });
    }
    if !node.params.key_exprs.is_empty() {
        return Err(Refuse::SortAggUnsupported { what: "key-expr" });
    }
    for a in &node.agg {
        if !matches!(
            a.op,
            AggOp::StringAgg
                | AggOp::ArrayAgg
                | AggOp::ArrayAggDistinct
                | AggOp::PercentileDisc
                | AggOp::PercentileCont
                | AggOp::Mode
        ) || a.col.is_none()
        {
            return Err(Refuse::SortAggUnsupported { what: "agg-mix" });
        }
    }
    Ok(())
}

/// [sortgrp v1] The memory law (design doc §5): pass-A scatter + pass-B
/// sort hold O(survivor rows × cells) and the append arenas hold
/// O(survivor payload bytes) — both bounded by facts the stats faces
/// carry per column (byte_len_sum for varlena, nonnull × width for
/// words) plus the physical row count. Admission: witnessed estimate ≤
/// `SqeConfig::sortagg_budget_bytes` ⇒ serve; unwitnessed ⇒
/// `sortagg-bytes-unwitnessed`; over ⇒ `sortagg-over-budget`. A pushed
/// TopK does NOT relax this cap (a k-bounded ANSWER still folds every
/// row's bytes through the sort). Conservative: predicate selectivity
/// never discounts the estimate.
fn check_sortagg_budget(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    let Some(sa) = node.params.sortagg.as_ref() else {
        return Err(Refuse::SortAggUnsupported { what: "spec-missing" });
    };
    // The scattered cell set: group keys ++ sort keys ++ agg inputs.
    let mut cols: Vec<u32> = node.params.group_cols.clone();
    for k in &sa.keys {
        if !cols.contains(&k.col) {
            cols.push(k.col);
        }
    }
    for a in &node.agg {
        if let Some(c) = a.col {
            if !cols.contains(&c) {
                cols.push(c);
            }
        }
    }
    let mut rows: u64 = 0;
    let mut bytes: u128 = 0;
    for (ci, &c) in cols.iter().enumerate() {
        let sv = faces.stats(bank, c);
        let w = crate::stencils::col_width(bank, c);
        for pi in 0..bank.parts.len() {
            let Some(rec) = sv.part(pi) else {
                return Err(Refuse::SortAggBytesUnwitnessed { what: "part-record-missing" });
            };
            if rec.key_kind == pgrc2_format::meta::KeyKind::Absent.as_u8() {
                return Err(Refuse::SortAggBytesUnwitnessed { what: "stats-absent" });
            }
            bytes = bytes.saturating_add(if w == 0 {
                rec.byte_len_sum as u128
            } else {
                (rec.nonnull as u128) * (w as u128)
            });
            if ci == 0 {
                rows = rows.saturating_add(rec.nonnull as u64);
            }
        }
    }
    // Physical row bound (nulls included) from the walk plane; the
    // nonnull sums above only carry payload bytes.
    let units = faces.walk(bank, node.cols[0]);
    let walk_rows: u64 = units.iter().map(|u| u.2 as u64).sum();
    rows = rows.max(walk_rows);
    // Per-row structural overhead: the ingest ordinal + per-cell Cell
    // slots (enum + Vec headers) — the honest scatter-row cost.
    let ncells = cols.len() as u128;
    let est = bytes
        .saturating_add(rows as u128 * (64 + 40 * ncells))
        .min(u64::MAX as u128) as u64;
    let budget = faces.cfg.sortagg_budget_bytes;
    if est > budget {
        return Err(Refuse::SortAggOverBudget { est, budget });
    }
    Ok(())
}

/// [P6-1 spill] The grouped memory law (spill-design.md §2): scatter-
/// plane estimate = exact bank row count × the widest SoA scatter record
/// (rows are a bank fact, never an estimate; the width is deliberately
/// the one authority `hash_plane::SCATTER_ROW_BYTES`). Under budget ⇒
/// serve as today. Over budget: the spill classification decides —
/// `Bounded`/`Spillable` serve (state under the law by construction),
/// `Spillable` additionally demands a registered spill substrate
/// (fail-closed: no substrate ⇒ typed refusal at admission, never a
/// stumble mid-I/O); `Unbounded` refuses typed (§6 residue arms). The
/// pass-2 table plane rides the same order of magnitude as the scatter
/// plane on these arms and is absorbed by the E18 default; per-arm table
/// accounting is the next rung's residue.
/// The grouped emit-cap group-count witness, ONE authority for the
/// admission gate (`check_server_grouped`) and the byte-key spill
/// dispatch ([spill-2] `hash_plane::bytes_spill_engaged`): `Some(bound)`
/// = a SOUND witness bounds the true group count at `bound` (a saturated
/// product still refuses honestly at u64::MAX); `None` = unwitnessed.
/// [ruling 2] the bound accumulates in Witness<u64>: only witness-grade
/// sources can mint a factor — the HLL ndv estimate never participates.
/// [tpch-wave-3] The (text, text) grouped-cells shape: two plain
/// varlena group keys, no key expressions.
pub fn text_pair_cells_shape(bank: &Bank, node: &PlanNode) -> bool {
    node.params.key_exprs.is_empty()
        && node.params.group_cols.len() == 2
        && node
            .params
            .group_cols
            .iter()
            .all(|&c| crate::stencils::col_width(bank, c) == 0)
}

/// [tpch-expr] The 1-2 plain byval-key cells shape.
pub fn byval_cells_shape(bank: &Bank, node: &PlanNode) -> bool {
    node.params.key_exprs.is_empty()
        && matches!(node.params.group_cols.len(), 1 | 2)
        && node
            .params
            .group_cols
            .iter()
            .all(|&c| crate::stencils::col_width(bank, c) > 0)
        && !node.agg.iter().any(|a| {
            matches!(a.op, AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct)
        })
}

pub fn grouped_count_bound(bank: &Bank, faces: &Faces, node: &PlanNode) -> Option<u64> {
    let mut bound = Witness::one();
    let mut witnessed = true;
    if !node.params.key_exprs.is_empty() {
        // Expression keys carry EVERY key (plain keys ride as Col);
        // group_cols is the decode set and would double-count (and
        // hold raw expression SOURCE domains, e.g. the timestamp
        // under a minute bucket).
        for e in &node.params.key_exprs {
            use crate::ir::KeyExpr as KE;
            match e {
                KE::Col(c) => match Witness::key_count(bank, faces, *c) {
                    Some(b) => bound = bound.saturating_mul(b),
                    None => witnessed = false,
                },
                KE::Minute(_) => bound = bound.saturating_mul(Witness::minute_of_hour()),
                KE::HourBucket { .. } => bound = bound.saturating_mul(Witness::hour_of_day()),
                KE::Const1 => {}
                // A pure per-value derivation f(col) has at most as
                // many distinct outputs as distinct inputs — the
                // source column's key-count witness carries over
                // (MinusConst is even injective; HostRegex maps each
                // dict-distinct value to exactly one host key).
                KE::MinusConst(c, _) | KE::HostRegex(c) => {
                    match Witness::key_count(bank, faces, *c) {
                        Some(b) => bound = bound.saturating_mul(b),
                        None => witnessed = false,
                    }
                }
                // Bucketed derivation: minutes inside the exact
                // source domain (+2 for the floor widening).
                KE::TruncMinute(c) => {
                    match Witness::trunc_minute_count(&faces.stats(bank, *c)) {
                        Some(b) => bound = bound.saturating_mul(b),
                        None => witnessed = false,
                    }
                }
                // Gated text: outputs ⊆ {referer values} ∪ {''}.
                KE::CaseSrc { referer, .. } => {
                    match Witness::key_count(bank, faces, *referer) {
                        Some(b) => bound = bound.saturating_mul(b.plus_const_arm()),
                        None => witnessed = false,
                    }
                }
            }
        }
    } else {
        for &c in node.params.group_cols.iter() {
            match Witness::key_count(bank, faces, c) {
                Some(b) => bound = bound.saturating_mul(b),
                None => witnessed = false,
            }
        }
    }
    witnessed.then(|| bound.value())
}

/// [spill-2] The distinct pipeline's E18 law; [spill-3] the ungrouped
/// SET routes spill (`distinct::set_spill_serves`, one authority with
/// the dispatch): over budget they serve given a substrate, fail-closed
/// without. [spill-4] the [w]-grouped pair arm
/// (`distinct::pair_spill_serves`) serves the same way. The filtered +
/// byte-key grouped routes stay residue (typed refusal; the dense-safe
/// grouped pure count-distinct re-elects upstream where it can).
fn check_distinct_budget(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    use crate::stencils::hash_plane::SCATTER_ROW_BYTES;
    if !faces.cfg.spill {
        return Ok(()); // kill switch: the legacy arm, verbatim
    }
    let budget = faces.cfg.grouped_budget_bytes();
    let est128 = (bank.rows_total() as u128) * (SCATTER_ROW_BYTES as u128);
    if est128 <= budget as u128 {
        return Ok(());
    }
    let est = est128.min(u64::MAX as u128) as u64;
    // [spill-4] the [w]-grouped pair arm serves over budget too.
    if crate::stencils::distinct::set_spill_serves(bank, node)
        || crate::stencils::distinct::pair_spill_serves(bank, node)
    {
        return if crate::spill::available() {
            Ok(())
        } else {
            Err(Refuse::GroupedSpillUnavailable { what: "no-substrate", est, budget })
        };
    }
    Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", est, budget })
}

/// [spill-2] The two-level family's E18 law, mirroring its dispatch
/// (`run_two_level_code_agg`): the byval-key dense route and the
/// count-only no-pred route hold dict/domain-bounded state (their group
/// planes ride the emit-cap witness — Bounded posture, admitted); the
/// predicate merge-join route's ONE row-scaled plane is the slot scatter,
/// which spills at the E18b share — over budget it demands a registered
/// substrate (fail-closed), and with one it serves bounded. No route in
/// this family classifies Unbounded, so the law never removes
/// servability — it adds the bounded-memory guarantee.
fn check_two_level_budget(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    use crate::stencils::hash_plane::SCATTER_ROW_BYTES;
    if !faces.cfg.spill {
        return Ok(()); // kill switch: the legacy arm, verbatim
    }
    let budget = faces.cfg.grouped_budget_bytes();
    let est128 = (bank.rows_total() as u128) * (SCATTER_ROW_BYTES as u128);
    if est128 <= budget as u128 {
        return Ok(());
    }
    // Dispatch mirror: byval key -> dense-int route; count-only with no
    // predicate terms -> part_count. Both dict/domain-bounded.
    let byval = crate::stencils::col_width(bank, node.params.group_cols[0]) > 0;
    let count_nopred = node.agg.iter().all(|a| a.op == crate::ir::AggOp::CountStar)
        && node
            .pred
            .as_ref()
            .map(|p| p.terms.is_empty() && p.var_terms.is_empty())
            .unwrap_or(true)
        && node.params.having_min_count == 0;
    if byval || count_nopred {
        return Ok(());
    }
    if crate::spill::available() {
        Ok(())
    } else {
        Err(Refuse::GroupedSpillUnavailable {
            what: "no-substrate",
            est: est128.min(u64::MAX as u128) as u64,
            budget,
        })
    }
}

/// [spill-5] The VAR lane's fp128 distinct plane is O(rows); FAIL-CLOSED
/// under the kill switch too — no accounted legacy arm (jsonbench-bar §7).
fn check_window_var_budget(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    use crate::stencils::window_replay as wr;
    if !wr::distinct_fp_serves(bank, node) {
        return Ok(());
    }
    let budget = faces.cfg.grouped_budget_bytes();
    let est128 = (bank.rows_total() as u128) * (wr::DSET128_ENTRY_BYTES as u128);
    if est128 <= budget as u128 {
        return Ok(());
    }
    let est = est128.min(u64::MAX as u128) as u64;
    if faces.cfg.spill && crate::spill::available() {
        return Ok(());
    }
    Err(Refuse::GroupedSpillUnavailable { what: "no-substrate", est, budget })
}

fn check_grouped_budget(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    use crate::stencils::hash_plane::{spill_class, SpillClass, SCATTER_ROW_BYTES};
    if !faces.cfg.spill {
        return Ok(()); // kill switch: the legacy arm, verbatim
    }
    let budget = faces.cfg.grouped_budget_bytes();
    let est128 = (bank.rows_total() as u128) * (SCATTER_ROW_BYTES as u128);
    if est128 <= budget as u128 {
        return Ok(());
    }
    let est = est128.min(u64::MAX as u128) as u64;
    match spill_class(bank, faces, node) {
        SpillClass::Bounded => Ok(()),
        // [spill-2] the byte-key and dense-distinct pair-plane routes are
        // the same law as the SoA arm: spillable given a registered
        // substrate, fail-closed without.
        SpillClass::Spillable | SpillClass::SpillableBytes | SpillClass::SpillableDistinct => {
            if crate::spill::available() {
                Ok(())
            } else {
                Err(Refuse::GroupedSpillUnavailable { what: "no-substrate", est, budget })
            }
        }
        SpillClass::Unbounded => {
            Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", est, budget })
        }
    }
}

/// SERVER-path scan admission (the scan-answer witness law): a
/// row-returning scan's answer is O(surviving rows). A pushed bound at or
/// under the cap admits directly (a k-bounded answer cannot explode);
/// an UNBOUNDED scan needs a SOUND stats witness that the survivors fit
/// the cap — the zone planes' upper bound (rows of granules no int
/// conjunct excludes; varlena conjuncts prune nothing here). Anything
/// else is a typed refusal — never a surprise full-table answer.
pub fn check_server_scan(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    if bank.parts.is_empty() {
        return Ok(());
    }
    if node.params.topk.as_ref().is_some_and(|t| t.n as u64 <= GROUP_ROW_CAP) {
        return Ok(());
    }
    // [ruling 2] the survivor bound is minted behind the membrane (zone
    // planes + part-record row counts — sound upper bound, no estimate
    // can produce the gate's input type).
    let bound = Witness::scan_survivor_bound(bank, faces, node);
    if bound.value() > faces.cfg.scan_answer_cap() {
        // [scan-cap-retire] The q19 unrefusal (the q1/q5 idiom): on real
        // data the sound pre-scan bound can be structurally unreachable
        // (equality on a full-range hash column prunes no granule — the
        // zone bound is rows_total on EVERY bank), so the witness gate
        // refused shapes whose true answer is tiny. The retire arm
        // SERVES them under the answer-face law instead: the scan_serve
        // stencil counts the TRUE survivors and raises the typed
        // `scan-answer-over-cap` refusal the moment they exceed the cap
        // (bounded state, never truncates, never OOMs) — the grouped
        // cap-retire posture (RULED 2026-08-19) at the scan face. Kill
        // switch PGRUST_SQE_SCAN_CAP_RETIRE=0 restores this gate.
        if faces.cfg.scan_cap_retire {
            return Ok(());
        }
        return Err(Refuse::ScanRowsUnwitnessed { what: "bound-over-cap" });
    }
    Ok(())
}

/// [winserve v1] SERVER-path WindowServe admission: shape sanity
/// (fail-closed against plans lowered elsewhere) + the memory law. A
/// window answer is O(rows) and the pass-A scatter + pass-B sort hold
/// O(survivor rows x cells) — both bounded by the same stats faces the
/// SortGrouped budget reads (byte_len_sum / nonnull x width + physical
/// rows). Witnessed estimate <= `SqeConfig::window_budget_bytes` =>
/// serve; unwitnessed => `winagg-bytes-unwitnessed`; over =>
/// `winagg-over-budget`. Conservative: predicate selectivity never
/// discounts the estimate. No spill (P6-1), no OOM path.
pub fn check_server_window(bank: &Bank, faces: &Faces, node: &PlanNode) -> Result<(), Refuse> {
    let Some(w) = node.params.window.as_ref() else {
        return Err(Refuse::WinUnsupported { what: "spec-missing" });
    };
    // Empty `emit` is lawful (SELECT count(*) OVER () FROM t): the
    // answer is function columns only.
    if w.funcs.is_empty() {
        return Err(Refuse::WinUnsupported { what: "spec-missing" });
    }
    if !node.agg.is_empty()
        || !node.params.group_cols.is_empty()
        || !node.params.key_exprs.is_empty()
    {
        return Err(Refuse::WinUnsupported { what: "spec-drift" });
    }
    for f in w.all_funcs() {
        let needs_col = !matches!(
            f.op,
            ir::WinOp::RowNumber | ir::WinOp::Rank | ir::WinOp::DenseRank | ir::WinOp::CountStar
        );
        if needs_col != f.col.is_some() {
            return Err(Refuse::WinUnsupported { what: "func-arg-shape" });
        }
    }
    // [winv4] run-condition targets: bottom-level FVal::N funcs only,
    // and never together with a chain (the C pass-through modes).
    if !w.run_conds.is_empty() && w.chain.is_some() {
        return Err(Refuse::WinUnsupported { what: "run-cond-chain" });
    }
    for rc in &w.run_conds {
        let ok = w.funcs.get(rc.func).is_some_and(|f| {
            matches!(
                f.op,
                ir::WinOp::RowNumber
                    | ir::WinOp::Rank
                    | ir::WinOp::DenseRank
                    | ir::WinOp::CountStar
                    | ir::WinOp::Count
            )
        });
        if !ok {
            return Err(Refuse::WinUnsupported { what: "run-cond-target" });
        }
    }
    // [winv4] the chain's order prefix must exist in the bottom order.
    if let Some(c) = &w.chain {
        if c.n_ord > w.order.len() || c.funcs.is_empty() {
            return Err(Refuse::WinUnsupported { what: "chain-order-prefix" });
        }
    }
    // A zero-part bank never runs a stencil (the empty-bank identity
    // path answers it).
    if bank.parts.is_empty() {
        return Ok(());
    }
    // [winframes v2] band admission: a Range-offset frame over a
    // datetime key serves only when every witnessed key value +/- the
    // offset stays inside the valid scaled domain — outside it C's
    // in_range errors ("timestamp out of range") or bends around
    // infinities, and the refusal hands the statement to the executor
    // that owns those laws. SMA min/max witness the key range.
    if let Some((lo, hi)) = w.frame.band {
        if w.frame.mode == ir::FrameMode::Range && w.frame.has_offsets() {
            let key = w.order.first().map(|k| k.col).ok_or(Refuse::WinUnsupported {
                what: "range-offset-keys",
            })?;
            let off = w.frame.max_offset();
            let sma = faces.sma(bank, key);
            let units = faces.walk(bank, node.cols[0]);
            for (ui, u) in units.iter().enumerate() {
                if u.2 == 0 {
                    continue;
                }
                let mn = sma.mins[ui] as i128 * w.frame.scale;
                let mx = sma.maxs[ui] as i128 * w.frame.scale;
                if mn - off < lo || mx + off > hi || mn < lo || mx > hi {
                    return Err(Refuse::WinUnsupported { what: "range-offset-domain" });
                }
            }
        }
    }
    // ---- the memory law ---------------------------------------------------
    let mut cols: Vec<u32> = w.emit.clone();
    for c in w
        .part_cols
        .iter()
        .chain(w.order.iter().map(|k| &k.col))
        .chain(w.all_funcs().filter_map(|f| f.col.as_ref()))
    {
        if !cols.contains(c) {
            cols.push(*c);
        }
    }
    let mut rows: u64 = 0;
    let mut bytes: u128 = 0;
    for (ci, &c) in cols.iter().enumerate() {
        let sv = faces.stats(bank, c);
        let width = crate::stencils::col_width(bank, c);
        for pi in 0..bank.parts.len() {
            let Some(rec) = sv.part(pi) else {
                return Err(Refuse::WinBytesUnwitnessed { what: "part-record-missing" });
            };
            if rec.key_kind == pgrc2_format::meta::KeyKind::Absent.as_u8() {
                return Err(Refuse::WinBytesUnwitnessed { what: "stats-absent" });
            }
            bytes = bytes.saturating_add(if width == 0 {
                rec.byte_len_sum as u128
            } else {
                (rec.nonnull as u128) * (width as u128)
            });
            if ci == 0 {
                rows = rows.saturating_add(rec.nonnull as u64);
            }
        }
    }
    // Physical row bound (nulls included) from the walk plane.
    let units = faces.walk(bank, node.cols[0]);
    let walk_rows: u64 = units.iter().map(|u| u.2 as u64).sum();
    rows = rows.max(walk_rows);
    // Per-row structural overhead: the ingest ordinal + per-cell Cell
    // slots, plus the per-row answer cells (emit + funcs).
    let ncells = (cols.len() + w.n_funcs()) as u128;
    let est = bytes
        .saturating_add(rows as u128 * (64 + 40 * ncells))
        .min(u64::MAX as u128) as u64;
    let budget = faces.cfg.window_budget_bytes;
    if est > budget {
        return Err(Refuse::WinOverBudget { est, budget });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// tier-2 election laws (PLANNER-SPEC §1) — functions over stats.
// Constants carry provenance; the per-target re-cut is R3/P1-2 work.
// ---------------------------------------------------------------------------

/// The L2 partition law (§1.2, origin: the 40m-NDV group-by sweep): slots cost
/// `slot_bytes`, tables run at <=1/2 load => 2*slot_bytes per group;
/// P = next_pow2 bringing one partition under the budget. [ruling 3]
/// clamp floor is width-derived (owner grains per worker — the historical
/// 256 was next_pow2(2·96) frozen); ceiling is the per-target scatter cap.
pub fn partition_count(
    ndv_est: usize,
    slot_bytes: usize,
    l2_bytes: usize,
    width: usize,
) -> usize {
    let cp = crate::cost_params::target();
    let p = (ndv_est * 2 * slot_bytes).div_ceil(l2_bytes.max(1));
    p.next_power_of_two().clamp(cp.partition_floor(width), cp.scatter_partition_cap)
}

/// [ruling 3, F7 fix] The dense-domain single authority: `Some((lo, hi))`
/// iff the key's EXACT domain (witness-grade — part-record min/max only)
/// fits the dense count array's L2 budget (`cost_params::dense_domain_cap`,
/// the historical 256K). family.rs's R6a admit, `agg_tier`'s dense arm,
/// and check_server_grouped's dense gate all read THIS function — the
/// elect-then-refuse near-miss class (1<<20 vs 256K disagreement, audit
/// F7) is structurally gone.
pub fn dense_domain(sv: &StatsView) -> Option<(i64, i64)> {
    let w = Witness::exact_domain(sv)?;
    let (lo, hi) = w.value();
    let dom = (hi as i128 - lo as i128 + 1) as u128;
    (dom > 0 && dom <= crate::cost_params::target().dense_domain_cap()).then_some((lo, hi))
}

/// Aggregation-tier election (§1.2): dense array when the key domain is
/// exactly known and small (L2-resident count array), else hash plane.
pub enum AggTier {
    Dense { lo: i64, hi: i64 },
    HashPlane { p: usize },
}

pub fn agg_tier(sv: &StatsView, slot_bytes: usize, l2_bytes: usize, width: usize) -> AggTier {
    if let Some((lo, hi)) = dense_domain(sv) {
        return AggTier::Dense { lo, hi };
    }
    let ndv = sv.ndv_est_sum() as usize;
    AggTier::HashPlane { p: partition_count(ndv.max(1), slot_bytes, l2_bytes, width) }
}

/// Thread-claim election (§1.6). The class comes from the goal vector;
/// the count is a function of the survivor-work estimate and pool width.
/// [ruling 3] the serial cutoff is the derived law
/// `rows < rate · setup(width) · w/(w−1)` (cost_params::serial_cutoff_rows)
/// — at width 96 it evaluates to the historical
/// `SERIAL_CUTOFF_MS × ROWS_PER_MS = 21M` exactly (the safety gate).
pub fn elect_threads(
    class: ClaimClass,
    survivor_units: usize,
    survivor_rows_est: u64,
    width: usize,
) -> usize {
    match class {
        ClaimClass::BoundedWalk => 1,
        ClaimClass::Counting => width,
        ClaimClass::PairDistinct => (width / 2).max(1),
        ClaimClass::SkipDominated => {
            if survivor_rows_est < crate::cost_params::target().serial_cutoff_rows(width) {
                1
            } else {
                survivor_units.clamp(1, width)
            }
        }
    }
}

/// Condcache verdict-encoding election (§1.7): rowlist vs bitmap at the
/// byte crossover (survivors * 2B vs rows/8), plus the Skip/AllPass tags.
pub fn cache_rowlist(survivors: usize, rows: usize) -> bool {
    survivors * 16 < rows
}

// ---------------------------------------------------------------------------
// [R6d] dict-predicate survivor law — the two stats-witnessed guards
// (election-breadth-audit.md §2). Constants are PROVISIONAL with
// provenance notes; the per-target calibration is a CI cluster exercise per
// election-inputs-law.md (§5: every cut lands with its 100m cell — no
// 10m-elected threshold ships). Guard failure NEVER refuses: it lowers
// the conjunct to the row-grain plane (ir::F_ROW_GRAIN).
// ---------------------------------------------------------------------------

/// The R6d guard constants. A `Guards` value is an ELECTION INPUT
/// (constitution §5: elections are functions of stats, never bare
/// constants) — the defaults carry provisional provenance and the
/// calibration story rides election-inputs-law.md.
#[derive(Clone, Copy, Debug)]
pub struct R6dGuards {
    /// Build-budget fraction G_build: elect the entry-grain verdict build
    /// only when `Σ entries × cost_ratio <= g_build × rows` — the reuse
    /// test that keeps near-unique dict columns (entries ≈ rows,
    /// URL-class) from electing a verdict build costing as much as the
    /// scan it replaces. PROVISIONAL 0.5 (reuse factor >= 2 at
    /// cost_ratio 1.0); re-cut per target at 100m.
    pub g_build: f64,
    /// c_entry / c_row for the byte-matcher op class: measured per-entry
    /// vs per-row predicate eval cost ratio. PROVISIONAL 1.0 (dict entry
    /// bytes and row bytes run the same matcher; the row side also pays
    /// decode, so 1.0 is conservative toward the row plane).
    pub cost_ratio: f64,
    /// Survivor-selectivity bound G_sel: the survivor-set family is
    /// elected only when `touched_entries_est / Σ entries <= g_sel`;
    /// above it, identity resolution is exhaustive-class and the shape
    /// rides the row-grain plane. PROVISIONAL 0.5 (the measured 10x
    /// merge-join inversion is the boundary witness; the real cut comes
    /// from that cell + the selective hot-shape cell, re-validated at 100m).
    pub g_sel: f64,
    /// Plan-time touched-entry estimate for INFIX ops (Contains/Like
    /// with no closed form): `touched_est = sel_infix_est × entries`.
    /// PROVISIONAL 0.1 (the shipped infix hot shapes' selectivities are
    /// well under this); ops with closed forms never consult it.
    pub sel_infix_est: f64,
}

impl Default for R6dGuards {
    fn default() -> R6dGuards {
        R6dGuards { g_build: 0.5, cost_ratio: 1.0, g_sel: 0.5, sel_infix_est: 0.1 }
    }
}

/// The grain a varlena conjunct's survivor set resolves at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryGrain {
    /// Once per dict entry (VerdictWords), replayed by code.
    Entry,
    /// Once per row (fused eval over decoded bytes) — the guard-failure
    /// lowering; same answer, exhaustive-class cost.
    Row,
}

/// Plan-time touched-entries estimate for one varlena op over a dict of
/// `entries` entries. Closed forms where they exist (audit §2: NeEmpty =
/// entries minus the ''-entry); infix ops use the provisional estimate;
/// complements are `entries - est(positive)`.
pub fn touched_entries_est(op: ir::VarOp, entries: u64, g: &R6dGuards) -> f64 {
    let e = entries as f64;
    match op {
        ir::VarOp::NeEmpty => (e - 1.0).max(0.0),
        ir::VarOp::Contains | ir::VarOp::Like => e * g.sel_infix_est,
        ir::VarOp::NotContains | ir::VarOp::NotLike => e * (1.0 - g.sel_infix_est),
        // [type-vocab] byte-order comparison: an entry-grain closed form
        // needs entry-rank stats the dict lanes don't consult for it yet;
        // Eq touches ~1 entry, the order/complement ops use the coarse
        // half-split. CmpBytes never rides the dict lanes today (ScanServe
        // row-grain eval only) — this estimate is future-facing.
        ir::VarOp::CmpBytes(crate::ir::BytesCmp::Eq)
        | ir::VarOp::CmpBytesTrim(crate::ir::BytesCmp::Eq) => 1.0,
        ir::VarOp::CmpBytes(_) | ir::VarOp::CmpBytesTrim(_) => e * 0.5,
        // [sqe-bpchar] byte-equality IN touches at most its list width
        // in entries (each image matches <= 1 dict entry). Same
        // future-facing status as CmpBytes: row-grain eval only today.
        ir::VarOp::InBytes => (crate::planner::IN_BYTES_MAX as f64).min(e),
        ir::VarOp::InPrefix => e * g.sel_infix_est,
    }
}

/// The pure R6d grain law over its stats inputs — unit-testable without a
/// bank. `grouped` arms the selectivity guard (it elects the survivor-set
/// FAMILY consumption — grouped gather/merge identities; the ungrouped
/// verdict/condcache lane is bounded downstream by the §1.7 encoding and
/// condcache density laws instead).
pub fn entry_grain_law(
    op: ir::VarOp,
    entries: Witness<u64>,
    rows: Witness<u64>,
    grouped: bool,
    g: &R6dGuards,
) -> EntryGrain {
    // [ruling 2] the guards' entry counts and row counts are
    // witness-grade by signature: dict entry sums and manifest row
    // counts only — an `ndv_est` cannot even be passed here.
    let (entries, rows) = (entries.value(), rows.value());
    if entries == 0 {
        // No part publishes a dict: there is nothing to build at entry
        // grain — the row plane is the only plane.
        return EntryGrain::Row;
    }
    // Guard 1 (entry-grain cost): Σ entries × c_entry <= G_build × rows × c_row.
    if entries as f64 * g.cost_ratio > g.g_build * rows as f64 {
        return EntryGrain::Row;
    }
    // Guard 2 (survivor selectivity), grouped survivor-set consumers only.
    if grouped {
        let touched = touched_entries_est(op, entries, g);
        if touched > g.g_sel * entries as f64 {
            return EntryGrain::Row;
        }
    }
    EntryGrain::Entry
}

/// Bank-facing wrapper: gathers the stats witnesses (per-part dict entry
/// sums via the dict faces; the manifest row total) for the DRIVING
/// varlena term — only var_terms[0] gets the entry-grain verdict build;
/// later var conjuncts stage row-grain over stage-1 survivors regardless.
pub fn elect_entry_grain(
    bank: &Bank,
    faces: &Faces,
    term: &ir::VarPredTerm,
    grouped: bool,
    g: &R6dGuards,
) -> EntryGrain {
    let entries = Witness::dict_entry_sum(bank, faces, term.col);
    entry_grain_law(term.op, entries, Witness::rows_total(bank), grouped, g)
}

/// Sortedness witness (§1 run-collapse law, famA): decode up to 8 sample
/// granules spread over the walk and measure rows/runs; collapse pays
/// when the mean run length clears 2. Memoized per attno; re-derived per
/// query run (the ruling — cleared from reset_per_query).
static RUN_WITNESS_MEMO: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u32, bool>>> =
    std::sync::OnceLock::new();

/// [ruling] per-query-run: the witness is re-derived by every query.
pub fn clear_run_witness_memo() {
    if let Some(m) = RUN_WITNESS_MEMO.get() {
        m.lock().unwrap().clear();
    }
}

pub fn run_collapse_witness(
    bank: &Bank,
    attno: u32,
    units: &[crate::engine::Unit],
    threads: usize,
) -> bool {
    use std::collections::HashMap;
    use std::sync::Mutex;
    let memo = RUN_WITNESS_MEMO.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(&v) = memo.lock().unwrap().get(&attno) {
        return v;
    }
    let t0 = std::time::Instant::now();
    let samples = 8usize.min(units.len().max(1));
    let step = (units.len() / samples).max(1);
    // [coldstart] the 8 sample granules decode in parallel — 8 cold
    // preads spread across the bank were serial on the driving thread.
    let per: Vec<(u64, u64)> = crate::engine::par_parts(threads, samples, |s| {
        let mut cs = crate::kernels_g::ColState::new(attno);
        let i = (s * step).min(units.len().saturating_sub(1));
        let (pi, g, rows, _) = units[i];
        let d = cs.dec(bank, pi, g, rows as usize);
        let mut runs = 0u64;
        let mut prev = None;
        for &x in d {
            if prev != Some(x) {
                runs += 1;
                prev = Some(x);
            }
        }
        (rows as u64, runs.max(1))
    });
    let rows_total: u64 = per.iter().map(|x| x.0).sum();
    let runs_total: u64 = per.iter().map(|x| x.1).sum();
    let elect = rows_total >= 2 * runs_total;
    crate::coldledger::note(
        "run_witness",
        format!("attno={attno}|collapse={elect}"),
        t0,
        0,
        crate::coldledger::Reason::CheaperThanPlain,
    );
    memo.lock().unwrap().insert(attno, elect);
    elect
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{ConjFp, PredSpec};
    use crate::typmeta::TypMeta;

    #[test]
    fn fingerprints_canonical_and_shared() {
        // Frame fingerprint is order-independent (canonicalization law).
        let a = PredTerm::new(7, CmpOp::Eq, 62, 0, TypMeta::INT4);
        let b = PredTerm::new(
            6,
            CmpOp::Between,
            crate::kernels_f6::pg_date(2013, 7, 1),
            crate::kernels_f6::pg_date(2013, 7, 31),
            TypMeta::DATE,
        );
        let p1 = PredSpec {
            terms: vec![a.clone(), b.clone()],
            frame_terms: 2,
            var_terms: Vec::new(),
            col_terms: Vec::new(),
        };
        let p2 = PredSpec {
            terms: vec![b, a],
            frame_terms: 2,
            var_terms: Vec::new(),
            col_terms: Vec::new(),
        };
        assert_eq!(p1.frame_fingerprint(), p2.frame_fingerprint());
    }

    #[test]
    fn fingerprints_distinguish_type_and_collation() {
        // The collation-illegal-sharing fix: byte-identical needles under
        // different collations (or type resolutions) never share a key.
        use crate::ir::{VarOp, VarPredTerm};
        let c_coll = VarPredTerm::new(14, VarOp::Contains, b"google".to_vec(), TypMeta::TEXT_C);
        let icu = TypMeta { collation: 12345, ..TypMeta::TEXT_C };
        let icu_coll = VarPredTerm::new(14, VarOp::Contains, b"google".to_vec(), icu);
        assert_ne!(c_coll.fp, icu_coll.fp);
        let i4 = PredTerm::new(3, CmpOp::Eq, 5, 0, TypMeta::INT4);
        let i8t = PredTerm::new(3, CmpOp::Eq, 5, 0, TypMeta::INT8);
        assert_ne!(i4.fp, i8t.fp);
        // Same everything => same key (the intended equivalence class).
        let again = VarPredTerm::new(14, VarOp::Contains, b"google".to_vec(), TypMeta::TEXT_C);
        assert_eq!(c_coll.fp, again.fp);
    }

    #[test]
    fn goal_covers_partial_order() {
        let fa = PredTerm::new(1, CmpOp::Eq, 1, 0, TypMeta::INT4).fp;
        let fb = PredTerm::new(2, CmpOp::Eq, 2, 0, TypMeta::INT4).fp;
        let mut g1 = crate::ir::Goal {
            fingerprints: vec![ConjFp::new(vec![fb.clone()]), ConjFp::new(vec![fa.clone()])],
            claim_class: Some(ClaimClass::SkipDominated),
        };
        g1.canonicalize();
        let g2 = crate::ir::Goal {
            fingerprints: vec![ConjFp::new(vec![fa])],
            claim_class: None,
        };
        assert!(g1.covers(&g2));
        assert!(!g2.covers(&g1));
    }

    #[test]
    fn election_laws() {
        // partition law endpoints at rig width (origin: ndv 40m, 32B slots).
        assert_eq!(partition_count(40_000_000, 32, 512 * 1024, 96), 8192);
        assert_eq!(partition_count(100, 32, 512 * 1024, 96), 256);
        // claim election: serial gate + skip-dominated generic form.
        assert_eq!(elect_threads(ClaimClass::SkipDominated, 108, 882_000, 96), 1);
        assert_eq!(elect_threads(ClaimClass::SkipDominated, 91, 30_000_000, 96), 91);
        assert_eq!(elect_threads(ClaimClass::Counting, 3, 0, 96), 96);
        assert_eq!(elect_threads(ClaimClass::BoundedWalk, 5000, 1 << 40, 96), 1);
        // verdict encoding crossover (§1.7).
        assert!(cache_rowlist(10, 8192));
        assert!(!cache_rowlist(600, 8192));
    }

    /// [ruling 3, BINDING] safety gate at the election-consumer grain:
    /// the derived formulas drive elect_threads/partition_count to the
    /// EXACT historical decisions at width 96 (the historical constants'
    /// boundary values included).
    #[test]
    fn width96_election_behavior_frozen() {
        // E2: the serial boundary, exact on both sides — the 20260818
        // width-ladder cell's measured-warm cutoff (RULED adopted;
        // serial_cutoff_rows(96) = 4,181,113 — was the historical 21M
        // under the retired cold line).
        assert_eq!(elect_threads(ClaimClass::SkipDominated, 91, 4_181_112, 96), 1);
        assert_eq!(elect_threads(ClaimClass::SkipDominated, 91, 4_181_113, 96), 91);
        // E7: the [256, 8192] clamp at 96 wide.
        assert_eq!(partition_count(1, 32, 512 * 1024, 96), 256);
        assert_eq!(partition_count(100_000_000, 32, 512 * 1024, 96), 8192);
        // Off-design widths flip sizing, never answers: at laptop width
        // a shape just under the width-96 cutoff goes parallel (the
        // law-doc motivator; cutoff(8) = 589,120 under the 20260818
        // measured line).
        assert_eq!(elect_threads(ClaimClass::SkipDominated, 8, 4_181_112, 8), 8);
    }

    #[test]
    fn min_over_empty_is_null_in_answers() {
        // The oracle.rs:357 bug class, answer-side witness: an Option-
        // typed MIN column renders NULL (empty field), never 0.
        use crate::answer::{AnswerCol, AnswerSet};
        let col = AnswerCol::i64s_opt(TypMeta::INT8, vec![None]);
        let a = AnswerSet::from_cols(vec![col]);
        assert_eq!(crate::render::to_lines(&a), vec![String::new()]);
    }
}
