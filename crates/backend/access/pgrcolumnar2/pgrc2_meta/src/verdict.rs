//! Ternary verdict evaluation (charter §1: **advisory-only** — a pruning
//! defect can lose speed, never rows; the verdict-vs-decode differential is
//! the enforcement) over one grain's `StatsRecord` + optional bloom
//! evidence.
//!
//! ## The coarse-key law, enforced here
//!
//! Range logic is split BY TYPE:
//!
//! - [`exact_eq`]/[`exact_cmp`] take [`ExactKey`]s and may return
//!   `Verdict::AllPass` (exact keys are equality-faithful and
//!   order-isomorphic);
//! - [`coarse_eq`] takes [`CoarseKey`]s and returns [`CoarseEqVerdict`] — an
//!   enum with **no AllPass variant**: a coarse Eq AllPass does not
//!   compile;
//! - [`coarse_cmp`] proves range verdicts from STRICT key inequalities only
//!   (monotone-with-ties: `k(a) < k(b) ⇒ a < b`; ties prove nothing).
//!
//! Every verdict leaves [`evaluate`] through the [`finalize`] chokepoint,
//! whose RELEASED `assert!` refuses an eq-flavored AllPass built on coarse
//! keys — the backstop behind the type wall, born-RED-tested.
//!
//! ## Measured-only law
//!
//! Key evidence requires BOTH the profile's derivation (the builder
//! computed keys for this column) AND the record's own `key_kind` witness
//! (the builder computed keys for THIS grain — numeric pack failure or an
//! all-null grain degrade to `Absent`). The typed aggregate accessors at
//! the bottom ([`sum_answer`], [`zero_count_answer`]) encode the same law
//! for metadata-answered aggregates, including the PackedI64 ↔ key-kind
//! validity coupling (`profile.rs`) — and, since #598, the on-part
//! COMPUTED-STATS WITNESS ([`STATSF_COMPUTED`]): the profile is a
//! catalog-time fact about what the builder WOULD compute, so a record
//! sealed by the pre-wire stand-in (exact `nonnull`, uncomputed zeros,
//! `flags == 0`, admissible forever) must be distinguishable ON PART from
//! a real-built record, or a metadata SUM answers 0 over real data. No
//! witness ⇒ decline (`None`) to the decode path; declining is always
//! sound. [`count_nonnull_answer`] is deliberately NOT witness-gated —
//! see its doc for why that asymmetry is load-bearing.
//!
//! ## Null semantics (SQL ternary)
//!
//! A value predicate is satisfied only when it evaluates TRUE; NULL rows
//! never pass. Hence: `nonnull == 0 ⇒ AllFail` for every value predicate,
//! and a range AllPass additionally requires `nonnull == rows` (the null
//! gate). `IsNull`/`IsNotNull` verdicts come from the always-computed
//! `nonnull` witness alone.

use crate::bloom::bloom_may_contain;
use crate::format::meta::{KeyKind, Sortedness, StatsRecord, Verdict, STATSF_COMPUTED};
use crate::key::{CoarseKey, ExactKey, TypedKey, TypedKeys};
use crate::lower::LoweredConst;
use crate::profile::{MetaProfile, SumKind, ZeroKind};

/// Closed-form facts about the grain under evaluation (spec §2: row counts
/// are never stored). u64 because part grains can exceed u32 rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrainFacts {
    pub rows: u64,
}

/// Bloom evidence for this grain (the armed block + the section header's
/// `k`), when the caller has it. Absent evidence is always legal.
#[derive(Debug, Clone, Copy)]
pub struct BloomEvidence<'a> {
    pub k: u32,
    pub block: &'a [u8],
}

/// One single-column zone predicate over lowered constants.
#[derive(Debug, Clone, Copy)]
pub enum ZonePredicate<'a> {
    Eq(LoweredConst<'a>),
    /// `<>` (M4-S5, planned-5's q7 neq derivation — the Ne arm born WITH
    /// the prune chokepoint). Coarse-key duality is REVERSED vs Eq: an Ne
    /// AllFail claims value EQUALITY (`min == max == c`) and is
    /// exact-keys-only by construction; an Ne AllPass claims value
    /// INEQUALITY, which any key kind proves from c-outside-[min,max]
    /// (same-value ⇒ same-key), and a bloom definite-absent proves for
    /// any kind.
    Ne(LoweredConst<'a>),
    Lt(LoweredConst<'a>),
    Le(LoweredConst<'a>),
    Gt(LoweredConst<'a>),
    Ge(LoweredConst<'a>),
    Between {
        lo: LoweredConst<'a>,
        lo_inc: bool,
        hi: LoweredConst<'a>,
        hi_inc: bool,
    },
    InSet(&'a [LoweredConst<'a>]),
    IsNull,
    IsNotNull,
}

impl ZonePredicate<'_> {
    /// Eq-flavored probes are the coarse-key law's guarded class. `Ne` is
    /// NOT a member: the guarded claim is an AllPass built from coarse key
    /// EQUALITY, while Ne's AllPass rests on key INEQUALITY (sound for any
    /// kind); Ne's unsound coarse claim would be AllFail, which
    /// [`ne_evidence`] makes unrepresentable (the exact arm alone can
    /// return it).
    pub fn eq_flavored(&self) -> bool {
        matches!(self, ZonePredicate::Eq(_) | ZonePredicate::InSet(_))
    }
}

// ---------------------------------------------------------------------------
// exact range logic (AllPass-capable)
// ---------------------------------------------------------------------------

/// Comparison operator for the cmp helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
}

/// Exact Eq: equality-faithful keys — `min == max == c` proves every
/// non-null value equals the constant (the null gate is applied by the
/// caller).
pub fn exact_eq(min: ExactKey, max: ExactKey, c: ExactKey) -> Verdict {
    if c < min || c > max {
        Verdict::AllFail
    } else if min == max && min == c {
        Verdict::AllPass
    } else {
        Verdict::Mixed
    }
}

/// Exact ordered comparison: order-isomorphic keys admit non-strict proofs.
pub fn exact_cmp(min: ExactKey, max: ExactKey, c: ExactKey, op: CmpOp) -> Verdict {
    match op {
        CmpOp::Lt => {
            if max < c {
                Verdict::AllPass
            } else if min >= c {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            }
        }
        CmpOp::Le => {
            if max <= c {
                Verdict::AllPass
            } else if min > c {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            }
        }
        CmpOp::Gt => {
            if min > c {
                Verdict::AllPass
            } else if max <= c {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            }
        }
        CmpOp::Ge => {
            if min >= c {
                Verdict::AllPass
            } else if max < c {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            }
        }
    }
}

// ---------------------------------------------------------------------------
// coarse range logic (the law: no Eq AllPass exists to return)
// ---------------------------------------------------------------------------

/// The coarse Eq verdict domain: **there is no AllPass variant.** This enum
/// is the compile-time form of the coarse-key law (spec §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoarseEqVerdict {
    /// `c` outside `[min, max]`: value equality implies key equality
    /// (deterministic transform), so no row can equal the constant. Sound.
    AllFail,
    /// Everything else — coarse key equality proves nothing.
    Mixed,
}

impl CoarseEqVerdict {
    pub fn verdict(self) -> Verdict {
        match self {
            CoarseEqVerdict::AllFail => Verdict::AllFail,
            CoarseEqVerdict::Mixed => Verdict::Mixed,
        }
    }
}

/// Coarse Eq (see [`CoarseEqVerdict`]).
pub fn coarse_eq(min: CoarseKey, max: CoarseKey, c: CoarseKey) -> CoarseEqVerdict {
    if c < min || c > max {
        CoarseEqVerdict::AllFail
    } else {
        CoarseEqVerdict::Mixed
    }
}

/// Coarse ordered comparison: STRICT key inequalities only. Monotone-with-
/// ties gives `k(a) < k(b) ⇒ a < b`; key ties prove nothing, so every
/// proof below uses `<` / `>` regardless of the operator's inclusivity
/// (e.g. `Le` AllPass needs `max_key < c_key`: a tie could hide a value
/// above the constant sharing its prefix).
pub fn coarse_cmp(min: CoarseKey, max: CoarseKey, c: CoarseKey, op: CmpOp) -> Verdict {
    match op {
        CmpOp::Lt | CmpOp::Le => {
            if max < c {
                Verdict::AllPass
            } else if min > c {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            }
        }
        CmpOp::Gt | CmpOp::Ge => {
            if min > c {
                Verdict::AllPass
            } else if max < c {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            }
        }
    }
}

// ---------------------------------------------------------------------------
// the chokepoint
// ---------------------------------------------------------------------------

/// The single verdict exit. The RELEASED assert is the coarse-key law's
/// backstop behind the type wall: an eq-flavored AllPass built on coarse
/// key evidence is a wrong-results defect and must never leave this
/// function. Public so the born-RED gate can prove the tooth fires.
pub fn finalize(eq_flavored: bool, used_coarse_keys: bool, v: Verdict) -> Verdict {
    assert!(
        !(eq_flavored && used_coarse_keys && v == Verdict::AllPass),
        "coarse-key law: eq-flavored probe claimed AllPass from coarse zone keys (spec §8.1)"
    );
    v
}

// ---------------------------------------------------------------------------
// evaluation
// ---------------------------------------------------------------------------

/// The grain's typed keys under the measured-only law: present only when
/// the profile derives keys AND the record's `key_kind` witnesses them AND
/// the two kinds agree ([`KeyKind::EnumRankReserved`] — the O-5 slot,
/// never written at M3 — reads as absent; a kind disagreement means a
/// foreign or tampered record: consult nothing).
pub fn grain_keys(profile: &MetaProfile, rec: &StatsRecord) -> TypedKeys {
    let deriv_kind = profile.key.kind();
    if deriv_kind == KeyKind::Absent {
        return TypedKeys::Absent;
    }
    let rec_kind = match KeyKind::from_u8(rec.key_kind) {
        Ok(k) => k,
        Err(_) => return TypedKeys::Absent,
    };
    if rec_kind == KeyKind::Absent || rec_kind == KeyKind::EnumRankReserved {
        return TypedKeys::Absent;
    }
    if rec_kind != deriv_kind {
        debug_assert!(
            false,
            "stats record key kind disagrees with profile derivation"
        );
        return TypedKeys::Absent;
    }
    TypedKeys::from_record_parts(rec.min_key, rec.max_key, rec_kind)
}

/// Key-evidence verdict for one comparison against one lowered constant.
/// Returns (verdict, used_coarse).
fn key_cmp_evidence(keys: TypedKeys, c: &LoweredConst<'_>, op: CmpOp) -> (Verdict, bool) {
    match (keys, c.key) {
        (TypedKeys::Exact { min, max }, Some(TypedKey::Exact(ck))) => {
            (exact_cmp(min, max, ck, op), false)
        }
        (TypedKeys::Coarse { min, max }, Some(TypedKey::Coarse(ck))) => {
            (coarse_cmp(min, max, ck, op), true)
        }
        (TypedKeys::Absent, _) | (_, None) => (Verdict::Mixed, false),
        // Kind mismatch between grain keys and a lowered constant cannot
        // happen through one profile; consult nothing.
        _ => {
            debug_assert!(false, "lowered constant kind disagrees with grain keys");
            (Verdict::Mixed, false)
        }
    }
}

/// Eq evidence from keys + bloom for one member constant.
/// Returns (verdict, used_coarse). The coarse arm can only contribute
/// `CoarseEqVerdict` (no AllPass — the type-level law); bloom absence
/// proofs are sound for any kind.
fn eq_member_evidence(
    profile: &MetaProfile,
    keys: TypedKeys,
    c: &LoweredConst<'_>,
    bloom: Option<BloomEvidence<'_>>,
) -> (Verdict, bool) {
    let (key_v, used_coarse) = match (keys, c.key) {
        (TypedKeys::Exact { min, max }, Some(TypedKey::Exact(ck))) => {
            (exact_eq(min, max, ck), false)
        }
        (TypedKeys::Coarse { min, max }, Some(TypedKey::Coarse(ck))) => {
            (coarse_eq(min, max, ck).verdict(), true)
        }
        (TypedKeys::Absent, _) | (_, None) => (Verdict::Mixed, false),
        _ => {
            debug_assert!(false, "lowered constant kind disagrees with grain keys");
            (Verdict::Mixed, false)
        }
    };
    if key_v == Verdict::AllFail {
        return (Verdict::AllFail, used_coarse);
    }
    // Bloom: only when the profile armed it (byte-eq == value-eq lattice)
    // and the constant lowered its equality bytes.
    if profile.eq_bloomable {
        if let (Some(ev), Some(eq)) = (bloom, c.eq_bytes()) {
            if !bloom_may_contain(ev.block, ev.k, eq) {
                return (Verdict::AllFail, used_coarse);
            }
        }
    }
    (key_v, used_coarse)
}

/// Ne evidence from keys + bloom (M4-S5; planned-5). The coarse-key law's
/// Ne dual, held BY CONSTRUCTION: only the exact arm can return AllFail
/// (the value-equality claim `min == max == c`); the coarse arm returns
/// AllPass (c outside [min, max] — key inequality proves value
/// inequality under the deterministic transform) or Mixed, never
/// AllFail. A bloom definite-absent upgrades Mixed to AllPass for any
/// kind (the constant is provably not present, so every non-null row
/// passes `<>`; the null gate rides the caller's generic AllPass check).
/// Returns (verdict, used_coarse).
fn ne_evidence(
    profile: &MetaProfile,
    keys: TypedKeys,
    c: &LoweredConst<'_>,
    bloom: Option<BloomEvidence<'_>>,
) -> (Verdict, bool) {
    let (key_v, used_coarse) = match (keys, c.key) {
        (TypedKeys::Exact { min, max }, Some(TypedKey::Exact(ck))) => {
            let v = if ck < min || ck > max {
                Verdict::AllPass
            } else if min == max && min == ck {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            };
            (v, false)
        }
        (TypedKeys::Coarse { min, max }, Some(TypedKey::Coarse(ck))) => {
            let v = if ck < min || ck > max { Verdict::AllPass } else { Verdict::Mixed };
            (v, true)
        }
        (TypedKeys::Absent, _) | (_, None) => (Verdict::Mixed, false),
        _ => {
            debug_assert!(false, "lowered constant kind disagrees with grain keys");
            (Verdict::Mixed, false)
        }
    };
    if key_v == Verdict::Mixed && profile.eq_bloomable {
        if let (Some(ev), Some(eq)) = (bloom, c.eq_bytes()) {
            if !bloom_may_contain(ev.block, ev.k, eq) {
                return (Verdict::AllPass, used_coarse);
            }
        }
    }
    (key_v, used_coarse)
}

/// Evaluate one predicate over one grain. Advisory-only, measured-only;
/// every path exits through [`finalize`].
pub fn evaluate(
    profile: &MetaProfile,
    facts: GrainFacts,
    rec: &StatsRecord,
    probe: &ZonePredicate<'_>,
    bloom: Option<BloomEvidence<'_>>,
) -> Verdict {
    let nonnull = rec.nonnull as u64;
    // Null-witness verdicts: `nonnull` is the one universally-computed stat
    // (two-witness law, spec §6.6).
    match probe {
        ZonePredicate::IsNull => {
            let v = if nonnull == 0 {
                Verdict::AllPass
            } else if nonnull == facts.rows {
                Verdict::AllFail
            } else {
                Verdict::Mixed
            };
            return finalize(false, false, v);
        }
        ZonePredicate::IsNotNull => {
            let v = if nonnull == 0 {
                Verdict::AllFail
            } else if nonnull == facts.rows {
                Verdict::AllPass
            } else {
                Verdict::Mixed
            };
            return finalize(false, false, v);
        }
        _ => {}
    }
    // Value predicates: an empty grain scans nothing; an all-null grain
    // passes nothing (SQL ternary).
    if facts.rows == 0 || nonnull == 0 {
        return finalize(probe.eq_flavored(), false, Verdict::AllFail);
    }
    let keys = grain_keys(profile, rec);
    let (v, used_coarse) = match probe {
        ZonePredicate::Eq(c) => eq_member_evidence(profile, keys, c, bloom),
        ZonePredicate::Ne(c) => ne_evidence(profile, keys, c, bloom),
        ZonePredicate::Lt(c) => key_cmp_evidence(keys, c, CmpOp::Lt),
        ZonePredicate::Le(c) => key_cmp_evidence(keys, c, CmpOp::Le),
        ZonePredicate::Gt(c) => key_cmp_evidence(keys, c, CmpOp::Gt),
        ZonePredicate::Ge(c) => key_cmp_evidence(keys, c, CmpOp::Ge),
        ZonePredicate::Between {
            lo,
            lo_inc,
            hi,
            hi_inc,
        } => {
            let (v_lo, c_lo) =
                key_cmp_evidence(keys, lo, if *lo_inc { CmpOp::Ge } else { CmpOp::Gt });
            let (v_hi, c_hi) =
                key_cmp_evidence(keys, hi, if *hi_inc { CmpOp::Le } else { CmpOp::Lt });
            let v = if v_lo == Verdict::AllFail || v_hi == Verdict::AllFail {
                Verdict::AllFail
            } else if v_lo == Verdict::AllPass && v_hi == Verdict::AllPass {
                Verdict::AllPass
            } else {
                Verdict::Mixed
            };
            (v, c_lo || c_hi)
        }
        ZonePredicate::InSet(members) => {
            // x IN () is never true; otherwise: all members AllFail ⇒
            // AllFail; any member's Eq AllPass ⇒ AllPass (exact keys only,
            // by construction); else Mixed.
            let mut all_fail = true;
            let mut any_pass = false;
            let mut used_coarse = false;
            for m in members.iter() {
                let (mv, mc) = eq_member_evidence(profile, keys, m, bloom);
                used_coarse |= mc;
                match mv {
                    Verdict::AllFail => {}
                    Verdict::AllPass => {
                        all_fail = false;
                        any_pass = true;
                    }
                    Verdict::Mixed => all_fail = false,
                }
            }
            let v = if members.is_empty() || all_fail {
                Verdict::AllFail
            } else if any_pass {
                Verdict::AllPass
            } else {
                Verdict::Mixed
            };
            (v, used_coarse)
        }
        ZonePredicate::IsNull | ZonePredicate::IsNotNull => unreachable!("handled above"),
    };
    // The null gate: AllPass over values requires every row non-null.
    let v = if v == Verdict::AllPass && nonnull != facts.rows {
        Verdict::Mixed
    } else {
        v
    };
    finalize(probe.eq_flavored(), used_coarse, v)
}

// ---------------------------------------------------------------------------
// measured-only aggregate accessors (metadata-answered aggregates ride these)
// ---------------------------------------------------------------------------

/// TRUE iff the record carries the on-part computed-stats witness (#598):
/// the REAL builder sealed it, so its profile-computed aggregate fields
/// hold computed values (still subject to the PackedI64 key-kind validity
/// coupling). Stand-in-vintage records — every part sealed before the
/// metadata wiring, admissible forever — read FALSE, and every
/// profile-computed aggregate accessor declines them.
pub fn computed_stats_witness(rec: &StatsRecord) -> bool {
    rec.flags & STATSF_COMPUTED != 0
}

/// `count(col)`'s record leg: `nonnull`, answerable WITHOUT the
/// computed-stats witness — it is exact under EVERY builder vintage (the
/// stand-in computed it exactly; it is one leg of the two-witness null law,
/// spec §6.6, cross-checked against the validity bitmap at seal). And
/// `count(*)` needs no record at all: row counts are closed-form (spec §2).
/// This asymmetry is deliberate and load-bearing — the COUNT
/// metadata-answer slice ships over pre-witness parts while SUM/ZeroCount
/// decline to the decode path.
pub fn count_nonnull_answer(rec: &StatsRecord) -> u64 {
    rec.nonnull as u64
}

/// `SUM` in the mergeable i128 form, iff the record's computed-stats
/// witness is present (#598 — a pre-wire record's `sum_i128` is an
/// uncomputed zero, not a fact) AND the profile computes it AND (for the
/// packed-numeric lane) the grain's key-kind witness says the pack
/// succeeded (the validity coupling — `profile.rs`).
pub fn sum_answer(profile: &MetaProfile, rec: &StatsRecord) -> Option<i128> {
    if !computed_stats_witness(rec) {
        return None;
    }
    match profile.sum {
        SumKind::None => None,
        SumKind::PackedI64 => {
            if KeyKind::from_u8(rec.key_kind) == Ok(KeyKind::Exact) {
                Some(rec.sum_i128)
            } else {
                None
            }
        }
        SumKind::SignedWord | SumKind::UnsignedWord | SumKind::BoolTrues => Some(rec.sum_i128),
    }
}

/// `zero_count`, same couplings as [`sum_answer`] (witness gate included).
pub fn zero_count_answer(profile: &MetaProfile, rec: &StatsRecord) -> Option<u64> {
    if !computed_stats_witness(rec) {
        return None;
    }
    match profile.zero {
        ZeroKind::None => None,
        ZeroKind::PackedI64 => {
            if KeyKind::from_u8(rec.key_kind) == Ok(KeyKind::Exact) {
                Some(rec.zero_count)
            } else {
                None
            }
        }
        ZeroKind::Word | ZeroKind::Float | ZeroKind::BoolFalse => Some(rec.zero_count),
    }
}

/// One grain's MIN/MAX evidence for a metadata-ANSWERED aggregate (the
/// metaagg footer legs — Ruling 4 item 12; #732-precedent witness posture).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinMaxKeys {
    /// The grain holds ZERO non-null values: it contributes nothing to a
    /// null-skipping MIN/MAX (SQL law), and `nonnull` is exact under EVERY
    /// builder vintage (spec §6.6) — skipping is a fact, not a guess.
    NoRows,
    /// Exact, witnessed min/max keys over the grain's non-null values
    /// (the builder computes them null-skipping by construction: keys are
    /// derived per VALUE and merged over contributing granules only).
    Keys { min: i64, max: i64 },
}

/// MIN/MAX keys for a metadata-answered aggregate, or decline (`None`).
///
/// This is deliberately STRICTER than the pruning path ([`grain_keys`],
/// which is self-witnessing via `key_kind` because a wrong verdict can only
/// lose speed): an ANSWER is a query result, so the full #598/#732 posture
/// applies —
///
/// 1. `nonnull == 0` answers [`MinMaxKeys::NoRows`] with no further gate
///    (exact under every vintage; one leg of the two-witness null law).
/// 2. Otherwise the record must carry the on-part computed-stats witness
///    ([`STATSF_COMPUTED`]) — belt over the key-kind witness, per the
///    charter's absence-poisoned bar; a stand-in-vintage record declines.
/// 3. The profile's derivation must be VALUE-FAITHFUL
///    (`KeyDerivation::value_faithful` — key↔datum-word bijection): float
///    and packed-numeric keys are Exact but collapse equality classes that
///    span byte images, so no key can reproduce the scan's byte choice.
/// 4. The grain's keys must re-enter typed as `TypedKeys::Exact` (the
///    measured-only law: profile derivation AND the record's own
///    `key_kind` witness agree — a poisoned or stand-in grain reads
///    Absent and declines).
///
/// Declining is always sound: the caller scans.
pub fn min_max_answer(profile: &MetaProfile, rec: &StatsRecord) -> Option<MinMaxKeys> {
    if rec.nonnull == 0 {
        return Some(MinMaxKeys::NoRows);
    }
    if !computed_stats_witness(rec) {
        return None;
    }
    if !profile.key.value_faithful() {
        return None;
    }
    match grain_keys(profile, rec) {
        TypedKeys::Exact { min, max } => Some(MinMaxKeys::Keys {
            min: min.raw(),
            max: max.raw(),
        }),
        _ => None,
    }
}

/// The grain's sortedness fact (Unknown on a corrupt byte — consult
/// nothing).
pub fn sortedness_answer(rec: &StatsRecord) -> Sortedness {
    Sortedness::from_u8(rec.sortedness).unwrap_or(Sortedness::Unknown)
}

// ---------------------------------------------------------------------------
// the verdict-vs-decode differential (the enforcement of advisory-only)
// ---------------------------------------------------------------------------

/// A differential violation: the wrong-results class, born-RED-tested in
/// both directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DifferentialViolation {
    /// AllPass was claimed but row `row` does not satisfy the predicate.
    FalseAllPass { row: u32 },
    /// AllFail was claimed but row `row` satisfies the predicate.
    FalseAllFail { row: u32 },
}

/// Check one grain's verdict against a decode-side oracle. `row_passes`
/// answers per row from DECODED values (`None` = the row is NULL, which
/// never passes a value predicate). Returns the number of rows checked —
/// the gate's second tooth: callers assert it equals the grain's row count,
/// so a differential that silently did not run fails.
pub fn check_verdict_against_decode(
    verdict: Verdict,
    rows: u32,
    mut row_passes: impl FnMut(u32) -> Option<bool>,
) -> Result<u32, DifferentialViolation> {
    let mut checked = 0u32;
    for r in 0..rows {
        let passes = row_passes(r).unwrap_or(false);
        match verdict {
            Verdict::AllPass if !passes => {
                return Err(DifferentialViolation::FalseAllPass { row: r })
            }
            Verdict::AllFail if passes => {
                return Err(DifferentialViolation::FalseAllFail { row: r })
            }
            _ => {}
        }
        checked += 1;
    }
    Ok(checked)
}
