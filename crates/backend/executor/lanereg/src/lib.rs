//! # lanereg — the lane batch-function registry (design doc §3a)
//!
//! ONE place that answers, for a function/operator OID, **which lane batch
//! tiers can execute it and under what admission constraints**. Before this
//! crate, that knowledge was scattered: `execexpr::CmpOp::for_fn_oid` (the AOT
//! qual-bitmap comparator set), `execexpr::jit`'s `inline_op` (the JIT
//! arithmetic set), `lanefold::classify_trans` (the aggregate-transition fold
//! set) — plus, on side branches, `lanestitch`'s stencil vocabulary — each kept
//! its **own** OID table, so coverage drifted: a stencil could exist with no
//! matching AOT census, a fold with no matching qual-tier comparator, an
//! arithmetic OID inlined by the JIT but unknown to the fold's affine
//! admission. This registry is the single source those consumers query, and
//! the origin of the coverage-drift report (`coverage_report`).
//!
//! ## What this is (and is NOT)
//!
//! It is a **static, const census** of admission — no lazy init, no
//! thread-locals (the repo bans lazy-init `thread_local!` on hot paths). Every
//! query is a scan of a `&'static [BatchFn]`, done at **plan / arm time**
//! (classify / ready), never per batch — so consumers' hot paths stay
//! zero-cost. It does NOT own the concrete kernel *implementations* or the
//! consumer *enums* (`execexpr::CmpOp` is a shared comparator vocabulary used
//! well beyond the lane — nbtree, hashjoin, grouping — and stays in execexpr).
//! The registry owns the **admission mapping**: OID → { tiers, shape,
//! constraints }. A consumer decodes the registry's neutral `Shape` into its
//! own kernel selector (`CmpOp`, `InlineOp`, `LaneKind`).
//!
//! ## The safety contract carried per tier (design §3a)
//!
//! Immutable / side-effect-free only; error discipline is one of
//! [`GuardTier`]; strict-NULL per-row masks honored by the consumer; collation
//! baked per call site ([`CollGate`]). The registry records the *coarse* tier
//! and its guard/collation class; the consumer still does the shape-specific
//! work (affine decomposition, guard-interval derivation, per-batch re-proof).
//!
//! ## Migration recipe — how a pending tier's table slots in
//!
//! Side branches (`lane-v2-foldcov`, `lane-v2-textfold`, `lane-v2-stitchwire`,
//! `lane-v2-k2probe`, `lane-v2-projstitch`) each add coverage. Their tables are
//! ALREADY represented here as [`Avail::Pending`] rows (with the branch name),
//! so landing one is mechanical:
//!   1. flip the affected [`TierCov`] rows from `Pending { branch }` to
//!      `InTree` (delete the `branch`);
//!   2. point the consumer at the registry (e.g. the new fold OIDs' consumer
//!      calls `covers(oid, Tier::Fold)` / decodes [`fold_desc`]);
//!   3. the conformance test in that consumer's crate (see
//!      `execexpr`/`lanefold` tests) now binds the live table to the registry —
//!      it fails if they disagree, so drift cannot re-open.
//! Adding a brand-new OID a branch introduces = add a [`BatchFn`] row here in
//! the same edit that adds the consumer arm.

#![allow(clippy::manual_range_contains)]

use ::types_core::Oid;

/// A batch-execution tier — a consumer that can run a function/operator over a
/// batch instead of the scalar fmgr.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// `execexpr` AOT qual-bitmap kernel (`QualScanVarCmpConst` /
    /// `QualVarCmpVar`, selected by `CmpOp::for_fn_oid`).
    AotQualCmp,
    /// `execexpr::jit` copy-and-patch inlined arithmetic (`inline_op`).
    JitArith,
    /// `lanefold::classify_trans` aggregate-transition fold.
    Fold,
    /// `lanefold::classify_arg` affine-transform admission for a folded arg
    /// (`(v/divk)*mulk + addend`).
    FoldAffine,
    /// `lanestitch` comparator stencil (segment-compiler tier).
    StitchCmp,
    /// `lanestitch` arithmetic stencil.
    StitchArith,
    /// `lanestitch` ScalarArrayOp (IN-list) stencil.
    StitchSaop,
}

pub const ALL_TIERS: &[Tier] = &[
    Tier::AotQualCmp,
    Tier::JitArith,
    Tier::Fold,
    Tier::FoldAffine,
    Tier::StitchCmp,
    Tier::StitchArith,
    Tier::StitchSaop,
];

impl Tier {
    pub const fn short(self) -> &'static str {
        match self {
            Tier::AotQualCmp => "aot-cmp",
            Tier::JitArith => "jit-arith",
            Tier::Fold => "fold",
            Tier::FoldAffine => "fold-affine",
            Tier::StitchCmp => "stitch-cmp",
            Tier::StitchArith => "stitch-arith",
            Tier::StitchSaop => "stitch-saop",
        }
    }
}

/// Is a tier's coverage merged into this tree, or pending on a side branch?
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Avail {
    InTree,
    /// Coverage exists on a not-yet-merged branch; `branch` names it so the
    /// migration recipe can find it.
    Pending { branch: &'static str },
}

/// Error / trap discipline (design §3a contract).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardTier {
    /// Proven non-erroring for every input of the type (comparators; min/max).
    NonErroring,
    /// Non-erroring by a TYPE-level width proof (fold: every value of the
    /// Var's width lands in-range).
    TypeProof,
    /// Carries a DATA-level interval re-proven per batch; a failed proof
    /// demotes the whole batch to the checked per-row program (fold guarded
    /// affine transforms).
    DataGuard,
    /// Batch runs unchecked; on any detected trap the rows 0..k replay per-row
    /// via fmgr so the error fires on C's row (JIT overflow branch).
    ReplayOnErr,
}

/// Collation gate (design §3a: collation baked per call site).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollGate {
    /// No collation input (numeric / temporal / boolean).
    NotApplicable,
    /// Admitted only for deterministic collations (text min/max).
    Deterministic,
}

/// One tier's coverage of a `BatchFn`, with its admission constraints.
#[derive(Clone, Copy, Debug)]
pub struct TierCov {
    pub tier: Tier,
    pub avail: Avail,
    pub guard: GuardTier,
    pub coll: CollGate,
}

impl TierCov {
    const fn intree(tier: Tier, guard: GuardTier, coll: CollGate) -> TierCov {
        TierCov { tier, avail: Avail::InTree, guard, coll }
    }
    const fn pending(tier: Tier, branch: &'static str, guard: GuardTier, coll: CollGate) -> TierCov {
        TierCov { tier, avail: Avail::Pending { branch }, guard, coll }
    }
    pub fn is_intree(&self) -> bool {
        matches!(self.avail, Avail::InTree)
    }
}

/// Predicate of a comparator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpPred {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Operand-width family of a comparator. `I84`/`I48` are the mixed int8/int4
/// cross-width bodies; `I24`/`I42` the int2/int4 mixes; `Oid` is unsigned;
/// `F4`/`F8`/`F48`/`F84` the float bodies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpWidth {
    I2,
    I4,
    I8,
    I24,
    I42,
    I84,
    I48,
    Oid,
    F4,
    F8,
    F48,
    F84,
}

/// Neutral comparator shape a consumer decodes into its own comparator enum
/// (e.g. `execexpr::CmpOp`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CmpShape {
    pub width: CmpWidth,
    pub pred: CmpPred,
}

/// Arithmetic op kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithKind {
    Add,
    Sub,
    Mul,
    Div,
}

/// Arithmetic operand width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithWidth {
    /// int2 operand, int4 result (int24/int42 mixes fold into int4).
    W24,
    W4,
    W8,
}

/// Neutral arithmetic shape a consumer decodes into its own selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArithShape {
    pub width: ArithWidth,
    pub op: ArithKind,
}

/// Aggregate-transition fold kind (mirror of `lanefold::LaneKind`, kept neutral
/// so the registry has no lanefold dependency).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldKind {
    CountStar,
    CountAny,
    Sum,
    AvgAccum,
    Min,
    Max,
    // pending (foldcov):
    FMin,
    FMax,
    BoolAnd,
    BoolOr,
    BitAnd,
    BitOr,
}

/// The shape/realizer payload — determines the family and carries the neutral
/// descriptor a consumer decodes.
#[derive(Clone, Copy, Debug)]
pub enum Shape {
    Cmp(CmpShape),
    Arith(ArithShape),
    Fold(FoldKind),
}

/// One registered function/operator OID with its coverage across tiers.
#[derive(Clone, Copy, Debug)]
pub struct BatchFn {
    pub oid: Oid,
    /// The catalog proname (documentation / report only).
    pub name: &'static str,
    pub shape: Shape,
    pub cov: &'static [TierCov],
}

impl BatchFn {
    pub fn tier(&self, tier: Tier) -> Option<&TierCov> {
        self.cov.iter().find(|c| c.tier == tier)
    }
}

// ---------------------------------------------------------------------------
// The census. Grouped by family; each row's `cov` is the union of tiers that
// admit that OID, tagged InTree or Pending{branch}.
// ---------------------------------------------------------------------------

use CmpPred::*;
use CmpWidth::*;

// The distinct coverage patterns, as named 'static slices (const promotion of
// an inline `&[..]` inside a const fn is not allowed, and naming them dedups).
const COV_AOT_CMP: &[TierCov] = &[
    TierCov::intree(Tier::AotQualCmp, GuardTier::NonErroring, CollGate::NotApplicable),
    TierCov::pending(Tier::StitchCmp, "lane-v2-stitchwire", GuardTier::NonErroring, CollGate::NotApplicable),
];
const COV_STITCH_CMP: &[TierCov] =
    &[TierCov::pending(Tier::StitchCmp, "lane-v2-stitchwire", GuardTier::NonErroring, CollGate::NotApplicable)];
const COV_ARITH_INT4: &[TierCov] = &[
    TierCov::intree(Tier::JitArith, GuardTier::ReplayOnErr, CollGate::NotApplicable),
    TierCov::intree(Tier::FoldAffine, GuardTier::DataGuard, CollGate::NotApplicable),
    TierCov::pending(Tier::StitchArith, "lane-v2-stitchwire", GuardTier::ReplayOnErr, CollGate::NotApplicable),
];
const COV_ARITH_INT8_JIT: &[TierCov] =
    &[TierCov::intree(Tier::JitArith, GuardTier::ReplayOnErr, CollGate::NotApplicable)];
const COV_FOLD_AFFINE: &[TierCov] =
    &[TierCov::intree(Tier::FoldAffine, GuardTier::DataGuard, CollGate::NotApplicable)];
const COV_FOLD_IT: &[TierCov] =
    &[TierCov::intree(Tier::Fold, GuardTier::TypeProof, CollGate::NotApplicable)];

// A comparator the in-tree AOT qual tier admits AND the stitcher will admit.
const fn cmp_aot(oid: Oid, name: &'static str, width: CmpWidth, pred: CmpPred) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Cmp(CmpShape { width, pred }), cov: COV_AOT_CMP }
}

// A comparator only the (pending) stitcher admits — the AOT qual census does
// NOT cover it: the "stencil-but-no-census" drift class.
const fn cmp_stitch_only(oid: Oid, name: &'static str, width: CmpWidth, pred: CmpPred) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Cmp(CmpShape { width, pred }), cov: COV_STITCH_CMP }
}

pub static ENTRIES: &[BatchFn] = &[
    // === int4 comparators — AOT qual (in-tree) + stitch (pending) ===
    cmp_aot(65, "int4eq", I4, Eq),
    cmp_aot(144, "int4ne", I4, Ne),
    cmp_aot(66, "int4lt", I4, Lt),
    cmp_aot(149, "int4le", I4, Le),
    cmp_aot(147, "int4gt", I4, Gt),
    cmp_aot(150, "int4ge", I4, Ge),
    // === int8 comparators ===
    cmp_aot(467, "int8eq", I8, Eq),
    cmp_aot(468, "int8ne", I8, Ne),
    cmp_aot(469, "int8lt", I8, Lt),
    cmp_aot(471, "int8le", I8, Le),
    cmp_aot(470, "int8gt", I8, Gt),
    cmp_aot(472, "int8ge", I8, Ge),
    // === int2 comparators ===
    cmp_aot(63, "int2eq", I2, Eq),
    cmp_aot(145, "int2ne", I2, Ne),
    cmp_aot(64, "int2lt", I2, Lt),
    cmp_aot(148, "int2le", I2, Le),
    cmp_aot(146, "int2gt", I2, Gt),
    cmp_aot(151, "int2ge", I2, Ge),
    // === int84 (int8,int4) comparators ===
    cmp_aot(474, "int84eq", I84, Eq),
    cmp_aot(475, "int84ne", I84, Ne),
    cmp_aot(476, "int84lt", I84, Lt),
    cmp_aot(478, "int84le", I84, Le),
    cmp_aot(477, "int84gt", I84, Gt),
    cmp_aot(479, "int84ge", I84, Ge),
    // === int48 (int4,int8) comparators ===
    cmp_aot(852, "int48eq", I48, Eq),
    cmp_aot(853, "int48ne", I48, Ne),
    cmp_aot(854, "int48lt", I48, Lt),
    cmp_aot(856, "int48le", I48, Le),
    cmp_aot(855, "int48gt", I48, Gt),
    cmp_aot(857, "int48ge", I48, Ge),
    // === int24 (int2,int4) comparators — STITCH ONLY (no AOT census) ===
    cmp_stitch_only(158, "int24eq", I24, Eq),
    cmp_stitch_only(164, "int24ne", I24, Ne),
    cmp_stitch_only(160, "int24lt", I24, Lt),
    cmp_stitch_only(166, "int24le", I24, Le),
    cmp_stitch_only(162, "int24gt", I24, Gt),
    cmp_stitch_only(168, "int24ge", I24, Ge),
    // === int42 (int4,int2) comparators — STITCH ONLY ===
    cmp_stitch_only(159, "int42eq", I42, Eq),
    cmp_stitch_only(165, "int42ne", I42, Ne),
    cmp_stitch_only(161, "int42lt", I42, Lt),
    cmp_stitch_only(167, "int42le", I42, Le),
    cmp_stitch_only(163, "int42gt", I42, Gt),
    cmp_stitch_only(169, "int42ge", I42, Ge),
    // === oid comparators — STITCH ONLY (unsigned) ===
    cmp_stitch_only(184, "oideq", Oid, Eq),
    cmp_stitch_only(185, "oidne", Oid, Ne),
    cmp_stitch_only(716, "oidlt", Oid, Lt),
    cmp_stitch_only(717, "oidle", Oid, Le),
    cmp_stitch_only(1638, "oidgt", Oid, Gt),
    cmp_stitch_only(1639, "oidge", Oid, Ge),
    // === float4 comparators — STITCH ONLY ===
    cmp_stitch_only(287, "float4eq", F4, Eq),
    cmp_stitch_only(288, "float4ne", F4, Ne),
    cmp_stitch_only(289, "float4lt", F4, Lt),
    cmp_stitch_only(290, "float4le", F4, Le),
    cmp_stitch_only(291, "float4gt", F4, Gt),
    cmp_stitch_only(292, "float4ge", F4, Ge),
    // === float8 comparators — STITCH ONLY ===
    cmp_stitch_only(293, "float8eq", F8, Eq),
    cmp_stitch_only(294, "float8ne", F8, Ne),
    cmp_stitch_only(295, "float8lt", F8, Lt),
    cmp_stitch_only(296, "float8le", F8, Le),
    cmp_stitch_only(297, "float8gt", F8, Gt),
    cmp_stitch_only(298, "float8ge", F8, Ge),
    // === float48 comparators — STITCH ONLY ===
    cmp_stitch_only(299, "float48eq", F48, Eq),
    cmp_stitch_only(300, "float48ne", F48, Ne),
    cmp_stitch_only(301, "float48lt", F48, Lt),
    cmp_stitch_only(302, "float48le", F48, Le),
    cmp_stitch_only(303, "float48gt", F48, Gt),
    cmp_stitch_only(304, "float48ge", F48, Ge),
    // === float84 comparators — STITCH ONLY ===
    cmp_stitch_only(305, "float84eq", F84, Eq),
    cmp_stitch_only(306, "float84ne", F84, Ne),
    cmp_stitch_only(307, "float84lt", F84, Lt),
    cmp_stitch_only(308, "float84le", F84, Le),
    cmp_stitch_only(309, "float84gt", F84, Gt),
    cmp_stitch_only(310, "float84ge", F84, Ge),
    // === arithmetic: int4 add/sub/mul — JIT + fold-affine (in-tree) + stitch ===
    arith(177, "int4pl", ArithWidth::W4, ArithKind::Add, COV_ARITH_INT4),
    arith(181, "int4mi", ArithWidth::W4, ArithKind::Sub, COV_ARITH_INT4),
    arith(141, "int4mul", ArithWidth::W4, ArithKind::Mul, COV_ARITH_INT4),
    // === arithmetic: int8 add/sub/mul — JIT ONLY (no fold-affine census) ===
    arith(463, "int8pl", ArithWidth::W8, ArithKind::Add, COV_ARITH_INT8_JIT),
    arith(464, "int8mi", ArithWidth::W8, ArithKind::Sub, COV_ARITH_INT8_JIT),
    arith(465, "int8mul", ArithWidth::W8, ArithKind::Mul, COV_ARITH_INT8_JIT),
    // === arithmetic: int2/int4 mixes + int24div — FOLD-AFFINE ONLY (no JIT) ===
    fold_affine(178, "int24pl", ArithKind::Add),
    fold_affine(179, "int42pl", ArithKind::Add),
    fold_affine(182, "int24mi", ArithKind::Sub),
    fold_affine(183, "int42mi", ArithKind::Sub),
    fold_affine(170, "int24mul", ArithKind::Mul),
    fold_affine(171, "int42mul", ArithKind::Mul),
    fold_affine(172, "int24div", ArithKind::Div),
    // === aggregate transition folds — in-tree ===
    fold_it(1219, "int8inc", FoldKind::CountStar),
    fold_it(2804, "int8inc_any", FoldKind::CountAny),
    fold_it(1840, "int2_sum", FoldKind::Sum),
    fold_it(1841, "int4_sum", FoldKind::Sum),
    fold_it(1962, "int2_avg_accum", FoldKind::AvgAccum),
    fold_it(1963, "int4_avg_accum", FoldKind::AvgAccum),
    fold_it(768, "int4larger", FoldKind::Max),
    fold_it(769, "int4smaller", FoldKind::Min),
    fold_it(770, "int2larger", FoldKind::Max),
    fold_it(771, "int2smaller", FoldKind::Min),
    fold_it(1236, "int8larger", FoldKind::Max),
    fold_it(1237, "int8smaller", FoldKind::Min),
    fold_it(1138, "date_larger", FoldKind::Max),
    fold_it(1139, "date_smaller", FoldKind::Min),
    fold_it(2036, "timestamp_larger", FoldKind::Max),
    fold_it(2035, "timestamp_smaller", FoldKind::Min),
    fold_it(1196, "timestamptz_larger", FoldKind::Max),
    fold_it(1195, "timestamptz_smaller", FoldKind::Min),
    // === aggregate transition folds — pending (lane-v2-foldcov) ===
    fold_pending(209, "float4larger", FoldKind::FMax, "lane-v2-foldcov"),
    fold_pending(211, "float4smaller", FoldKind::FMin, "lane-v2-foldcov"),
    fold_pending(223, "float8larger", FoldKind::FMax, "lane-v2-foldcov"),
    fold_pending(224, "float8smaller", FoldKind::FMin, "lane-v2-foldcov"),
    fold_pending(2515, "booland_statefunc", FoldKind::BoolAnd, "lane-v2-foldcov"),
    fold_pending(2516, "boolor_statefunc", FoldKind::BoolOr, "lane-v2-foldcov"),
    fold_pending(1892, "int2and", FoldKind::BitAnd, "lane-v2-foldcov"),
    fold_pending(1893, "int2or", FoldKind::BitOr, "lane-v2-foldcov"),
    fold_pending(1898, "int4and", FoldKind::BitAnd, "lane-v2-foldcov"),
    fold_pending(1899, "int4or", FoldKind::BitOr, "lane-v2-foldcov"),
    fold_pending(1904, "int8and", FoldKind::BitAnd, "lane-v2-foldcov"),
    fold_pending(1905, "int8or", FoldKind::BitOr, "lane-v2-foldcov"),
    // === aggregate transition folds — pending (lane-v2-textfold, collation-gated) ===
    fold_pending_coll(458, "text_larger", FoldKind::Max, "lane-v2-textfold"),
    fold_pending_coll(459, "text_smaller", FoldKind::Min, "lane-v2-textfold"),
    fold_pending_coll(1063, "bpchar_larger", FoldKind::Max, "lane-v2-textfold"),
    fold_pending_coll(1064, "bpchar_smaller", FoldKind::Min, "lane-v2-textfold"),
];

const fn arith(oid: Oid, name: &'static str, width: ArithWidth, op: ArithKind, cov: &'static [TierCov]) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Arith(ArithShape { width, op }), cov }
}

const fn fold_affine(oid: Oid, name: &'static str, op: ArithKind) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Arith(ArithShape { width: ArithWidth::W24, op }), cov: COV_FOLD_AFFINE }
}

const fn fold_it(oid: Oid, name: &'static str, kind: FoldKind) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Fold(kind), cov: COV_FOLD_IT }
}

// Pending-fold cov slices are per-branch (the branch string differs), so they
// are built from named per-branch consts rather than a shared slice.
const COV_FOLD_FOLDCOV: &[TierCov] =
    &[TierCov::pending(Tier::Fold, "lane-v2-foldcov", GuardTier::TypeProof, CollGate::NotApplicable)];
const COV_FOLD_TEXTFOLD: &[TierCov] =
    &[TierCov::pending(Tier::Fold, "lane-v2-textfold", GuardTier::NonErroring, CollGate::Deterministic)];

const fn fold_pending(oid: Oid, name: &'static str, kind: FoldKind, _branch: &'static str) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Fold(kind), cov: COV_FOLD_FOLDCOV }
}

const fn fold_pending_coll(oid: Oid, name: &'static str, kind: FoldKind, _branch: &'static str) -> BatchFn {
    BatchFn { oid, name, shape: Shape::Fold(kind), cov: COV_FOLD_TEXTFOLD }
}

// ---------------------------------------------------------------------------
// Query API — all plan/arm-time; linear scans of a small static table.
// ---------------------------------------------------------------------------

/// The registry entry for `oid`, if any tier admits it.
pub fn entry(oid: Oid) -> Option<&'static BatchFn> {
    ENTRIES.iter().find(|e| e.oid == oid)
}

/// Does an **in-tree** tier admit `oid`?
pub fn covers(oid: Oid, tier: Tier) -> bool {
    entry(oid).and_then(|e| e.tier(tier)).is_some_and(|c| c.is_intree())
}

/// Does any tier (in-tree OR pending) admit `oid`?
pub fn covers_pending(oid: Oid, tier: Tier) -> bool {
    entry(oid).and_then(|e| e.tier(tier)).is_some()
}

/// AOT qual comparator shape for `oid`, if the in-tree AOT tier admits it.
/// `execexpr::CmpOp::for_fn_oid` decodes this into its own comparator enum.
pub fn aot_qual_cmp(oid: Oid) -> Option<CmpShape> {
    let e = entry(oid)?;
    e.tier(Tier::AotQualCmp).filter(|c| c.is_intree())?;
    match e.shape {
        Shape::Cmp(s) => Some(s),
        _ => None,
    }
}

/// JIT arithmetic shape for `oid`, if the in-tree JIT tier admits it.
/// `execexpr::jit`'s `inline_op` decodes this into its own selector.
pub fn jit_arith(oid: Oid) -> Option<ArithShape> {
    let e = entry(oid)?;
    e.tier(Tier::JitArith).filter(|c| c.is_intree())?;
    match e.shape {
        Shape::Arith(s) => Some(s),
        _ => None,
    }
}

/// Fold kind for `oid`, if the in-tree Fold tier admits it. `lanefold`'s
/// conformance test binds `classify_trans`' admitted OID set to this.
pub fn fold_desc(oid: Oid) -> Option<FoldKind> {
    let e = entry(oid)?;
    e.tier(Tier::Fold).filter(|c| c.is_intree())?;
    match e.shape {
        Shape::Fold(k) => Some(k),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Coverage-drift report — generated FROM the registry (deliverable §2).
// ---------------------------------------------------------------------------

/// A drift class surfaced by cross-tier analysis of one OID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Drift {
    /// Stitch comparator stencil exists (pending) but the AOT qual census does
    /// NOT admit the OID.
    StencilNoCensus,
    /// The fold admits this arithmetic OID as an affine transform but the JIT
    /// arithmetic tier does not inline it.
    FoldAffineNoJit,
    /// The JIT inlines this arithmetic OID but the fold's affine admission does
    /// not know it.
    JitNoFoldAffine,
    /// Coverage exists only on a side branch (nothing in-tree).
    PendingOnly,
}

impl Drift {
    pub fn label(self) -> &'static str {
        match self {
            Drift::StencilNoCensus => "stencil-but-no-census",
            Drift::FoldAffineNoJit => "fold-affine-but-no-jit",
            Drift::JitNoFoldAffine => "jit-but-no-fold-affine",
            Drift::PendingOnly => "pending-only",
        }
    }
}

/// Compute the drift classes for one entry.
pub fn drift_of(e: &BatchFn) -> Vec<Drift> {
    let mut out = Vec::new();
    let has = |t: Tier| e.tier(t).is_some();
    let has_it = |t: Tier| e.tier(t).is_some_and(|c| c.is_intree());
    if has(Tier::StitchCmp) && !has_it(Tier::AotQualCmp) {
        out.push(Drift::StencilNoCensus);
    }
    if has_it(Tier::FoldAffine) && !has_it(Tier::JitArith) {
        out.push(Drift::FoldAffineNoJit);
    }
    if has_it(Tier::JitArith) && !has_it(Tier::FoldAffine) {
        out.push(Drift::JitNoFoldAffine);
    }
    if !e.cov.iter().any(|c| c.is_intree()) {
        out.push(Drift::PendingOnly);
    }
    out
}

fn cell(e: &BatchFn, t: Tier) -> &'static str {
    match e.tier(t) {
        None => "-",
        Some(c) if c.is_intree() => "IN",
        Some(_) => "..",
    }
}

/// Render the full coverage-drift table as Markdown. `..` = pending (side
/// branch), `IN` = in-tree, `-` = no coverage. This is the checked-in report
/// source; the snapshot test in this crate pins it.
pub fn coverage_report() -> String {
    let mut s = String::new();
    s.push_str("# Lane batch-function registry — coverage-drift report\n\n");
    s.push_str("Generated from `lanereg::ENTRIES` (`lanereg::coverage_report`).\n");
    s.push_str("`IN` = in-tree, `..` = pending on a side branch, `-` = not covered.\n\n");
    s.push_str("| OID | name | aot-cmp | jit-arith | fold | fold-affine | stitch-cmp | stitch-arith | drift |\n");
    s.push_str("|----:|------|:-------:|:---------:|:----:|:-----------:|:----------:|:------------:|-------|\n");
    for e in ENTRIES {
        let drift = drift_of(e)
            .iter()
            .map(|d| d.label())
            .collect::<Vec<_>>()
            .join(", ");
        s.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            e.oid,
            e.name,
            cell(e, Tier::AotQualCmp),
            cell(e, Tier::JitArith),
            cell(e, Tier::Fold),
            cell(e, Tier::FoldAffine),
            cell(e, Tier::StitchCmp),
            cell(e, Tier::StitchArith),
            drift,
        ));
    }
    s.push_str("\n## Summary\n\n");
    let total = ENTRIES.len();
    let count = |d: Drift| ENTRIES.iter().filter(|e| drift_of(e).contains(&d)).count();
    s.push_str(&format!("- registered OIDs: {total}\n"));
    s.push_str(&format!(
        "- {}: {} (stitch comparator stencils with no AOT qual census)\n",
        Drift::StencilNoCensus.label(),
        count(Drift::StencilNoCensus)
    ));
    s.push_str(&format!(
        "- {}: {} (fold affine ops the JIT does not inline)\n",
        Drift::FoldAffineNoJit.label(),
        count(Drift::FoldAffineNoJit)
    ));
    s.push_str(&format!(
        "- {}: {} (JIT-inlined arith unknown to the fold affine admission)\n",
        Drift::JitNoFoldAffine.label(),
        count(Drift::JitNoFoldAffine)
    ));
    s.push_str(&format!(
        "- {}: {} (coverage only on a side branch)\n",
        Drift::PendingOnly.label(),
        count(Drift::PendingOnly)
    ));
    s
}

#[cfg(test)]
mod tests;
