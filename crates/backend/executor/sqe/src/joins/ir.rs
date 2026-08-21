//! Join plan-node IR; semantics = the ported nodehashjoin's (NULL keys
//! never match; `quals` are ON-residual joinquals deciding matched-ness).

use crate::fold::AggFoldOp;
use crate::ir::{PredSpec, PredTerm, VarPredTerm};
use crate::typmeta::TypMeta;

/// LEFT is outer=probe; FULL is deferred (needs both-side null extension).
///
/// [sqe-semi-anti] RightSemi/RightAnti are the PG JOIN_RIGHT_SEMI /
/// JOIN_RIGHT_ANTI classes: the (NOT) EXISTS SUBJECT is the BUILD side.
/// The probe pass emits nothing — it MARKS matched build entries
/// (per-worker match bitmaps OR-merged after the pool run); a post-probe
/// sweep then emits each matched (RightSemi) / unmatched (RightAnti)
/// build entry exactly ONCE. NULL build join keys never match; RightAnti
/// RETAINS null-keyed build rows (unlinked from the hash chains) and
/// emits them — NOT EXISTS over a NULL correlation key is TRUE.
///
/// [rightouter] Right composes the two: matched pairs emit like Inner
/// AND mark; the sweep emits each unmatched (or retained null-keyed)
/// build entry once, probe side null-extended. Keyed 2-way, no dims.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Semi,
    Anti,
    RightSemi,
    RightAnti,
}

/// `probe.probe_col = build.build_col`; NULL never matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinKey {
    pub build_col: u32,
    pub probe_col: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinCmp {
    Lt,
    Le,
    Gt,
    Ge,
    Ne,
    /// Keyless (nest-loop) nodes only: an equality evaluated per pair.
    /// Keyed nodes carry equalities as `JoinKey` lanes, never here.
    Eq,
}

/// `probe.probe_col <op> build.build_col`; NULL operand => never matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinQual {
    pub probe_col: u32,
    pub build_col: u32,
    pub op: JoinCmp,
}

impl JoinQual {
    #[inline(always)]
    pub fn eval_v(&self, pv: i64, p_ok: bool, bv: i64, b_ok: bool) -> bool {
        p_ok
            && b_ok
            && match self.op {
                JoinCmp::Lt => pv < bv,
                JoinCmp::Le => pv <= bv,
                JoinCmp::Gt => pv > bv,
                JoinCmp::Ge => pv >= bv,
                JoinCmp::Ne => pv != bv,
                JoinCmp::Eq => pv == bv,
            }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinSide {
    Build,
    Probe,
    /// [sqe-mech3] A dimension stage's relation (3-way composition):
    /// `Dim(i)` is `JoinNode::dims[i]`, folded into the BUILD pass —
    /// its payload columns ride the build table's payload lanes.
    Dim(u8),
}

/// Semi/anti project PROBE columns only; right-semi/right-anti project
/// BUILD columns only (both construction-checked).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinOut {
    pub side: JoinSide,
    pub col: u32,
}

/// [sqe-tpch-mech] Aggregate-result-as-set consumption: a survivor
/// key set (e.g. the group keys a fused-HAVING grouped fold kept —
/// `answer::survivor_keyset`) applied as a WHERE-level membership filter
/// on one side's rows ahead of the join, over any decoded word lane of
/// the side; `keys` are SORTED words. Membership 3VL: NULL never passes.
///
/// [subset] `negate` = `NOT (ANY (col = set))` with the exact 3VL law:
/// NOT keeps only FALSE, so a NULL in the subquery answer (`set_null`)
/// passes NO row, the empty set passes EVERY row (NULLs included), and
/// `unknown_false` (the planner's unknownEqFalse) reads UNKNOWN as
/// FALSE — the negated test then passes exactly the non-members.
#[derive(Clone, Debug)]
pub struct InSetFilter {
    pub col: u32,
    pub keys: std::sync::Arc<Vec<i64>>,
    pub negate: bool,
    pub unknown_false: bool,
    pub set_null: bool,
}

impl InSetFilter {
    pub fn member(col: u32, keys: std::sync::Arc<Vec<i64>>) -> InSetFilter {
        InSetFilter { col, keys, negate: false, unknown_false: false, set_null: false }
    }

    #[inline(always)]
    pub fn null_passes(&self) -> bool {
        if !self.negate {
            return false;
        }
        if self.unknown_false {
            return true;
        }
        !self.set_null && self.keys.is_empty()
    }

    #[inline(always)]
    pub fn word_passes(&self, w: i64) -> bool {
        let member = self.keys.binary_search(&w).is_ok();
        if !self.negate {
            return member;
        }
        if self.unknown_false {
            return !member;
        }
        !self.set_null && !member
    }
}

/// [sqe-join-depth] The relation a dimension equi-key's non-dim side
/// reads from: the BUILD scan (the 3-way law unchanged) or an EARLIER
/// dimension stage's matched entry (chained dims — the left-deep
/// snowflake law: `dim_i.key = dim_j.col`, j < i, resolved per match
/// combination during the build scatter).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DimSrc {
    Build,
    Dim(u8),
}

/// [sqe-mech3] One dimension equi-key: `src.build_col = dim.dim_col`
/// (word-embed faces only; NULL never matches). `src` names the bank
/// `build_col` resolves against ([sqe-join-depth]: Build, or an earlier
/// dim stage for chained keys).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DimKey {
    pub dim_col: u32,
    pub build_col: u32,
    pub src: DimSrc,
}

/// [sqe-mech3] Byte-equality text conjunct on a dimension scan
/// (`col = 'const'`, C collation — byte equality IS value equality).
/// 3VL: NULL never passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextEqTerm {
    pub col: u32,
    pub bytes: Vec<u8>,
}

/// [sqe-mech3] A dimension build stage (3-way composition, floor-study
/// §6.3): a SMALL relation joined INNER onto the build side by word
/// equi-keys, folded in during build pass 1 — matching dim rows extend
/// the build entry's payload lanes (duplicate dim keys multiply build
/// entries, the general hash-join law). The stage's structure is
/// elected at run: witnessed-dense single-key domain -> direct-array
/// heads; else an owned open-address map. Payload columns are the
/// `JoinOut { side: Dim(i) }` entries of `JoinNode::out`.
#[derive(Clone, Debug)]
pub struct DimStage {
    pub keys: Vec<DimKey>,
    /// Word conjuncts on the dim scan (same PredSpec law as the sides).
    pub pred: Option<PredSpec>,
    /// Text byte-equality conjuncts on the dim scan.
    pub text_eqs: Vec<TextEqTerm>,
    pub rows_hint: u64,
}

/// [crossdim-or] One term of a staged disjunction: `test` (the CaseTest
/// word/byte vocabulary, 3VL) over ONE column of `site`, belonging to
/// arm `arm`. A row satisfies an arm iff EVERY term of that arm is TRUE
/// on the joined row; the disjunction passes iff some arm is satisfied
/// (WHERE-filter law: an all-UNKNOWN row is filtered).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrTerm {
    pub site: JoinSide,
    pub col: u32,
    pub arm: u8,
    pub test: CaseTest,
}

/// [crossdim-or] A staged disjunction over side-resolved predicates,
/// evaluated as per-site arm bitmasks: each site's rows carry "arms
/// whose terms here all hold" (sites without a term in an arm leave the
/// bit set); a joined row passes iff the AND of its sites' masks is
/// non-zero. Dim/build masks resolve during the build pass — a match
/// combination whose accumulated mask is 0 is pruned at scatter, so the
/// entry never materializes. Inner keyed nodes only (WHERE semantics).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedOr {
    pub narms: u8,
    pub terms: Vec<OrTerm>,
}

impl StagedOr {
    #[inline(always)]
    pub fn full_mask(&self) -> u64 {
        (1u64 << self.narms) - 1
    }
}

/// [semianti-flt] `probe/build host_col = stage_col`; NULL never matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterKey {
    pub host_col: u32,
    pub stage_col: u32,
}

/// [semianti-flt] `host_col <op> stage_col`; NULL operand never matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterQual {
    pub host_col: u32,
    pub stage_col: u32,
    pub op: JoinCmp,
}

/// [semianti-flt] A semi/anti membership stage applied as a WHERE-level
/// row filter on ONE side (`host` = Build or Probe) ahead of the join:
/// the stage relation scans into its own hash table (keys + the quals'
/// stage columns as payload, `pred`/`text_eqs` as scan conjuncts); a
/// host row passes iff a key-equal stage row satisfying every qual
/// exists (`anti` inverts — NOT EXISTS over a NULL host key is TRUE).
/// Sound at any plan position because keys and quals read HOST columns
/// only, so the (NOT) EXISTS commutes with the node's inner joins.
#[derive(Clone, Debug)]
pub struct FilterStage {
    pub anti: bool,
    pub host: JoinSide,
    pub keys: Vec<FilterKey>,
    pub quals: Vec<FilterQual>,
    pub pred: Option<PredSpec>,
    pub text_eqs: Vec<TextEqTerm>,
    /// [mapstage] grouped collapse of the stage table (None = plain
    /// membership).
    pub fold: Option<StageFold>,
    /// [mapjoingoal] where the stage table comes from (Bank = its own
    /// relation scan, the standing law).
    pub src: StageSrc,
    pub rows_hint: u64,
}

/// [mapjoingoal] A stage table source. `Rows` = the pre-collapsed map:
/// the inner GOAL already grouped, so the sealed representative table
/// arrives as DATA per engagement — canonical key words (NULL-keyed
/// groups dropped at seal: a NULL host key never matches) plus the
/// numeric cell planes.
#[derive(Clone, Debug)]
pub enum StageSrc {
    Bank,
    Rows(std::sync::Arc<StageRows>),
}

/// [mapjoingoal] The pre-collapsed entries: `kw` = nk key words per
/// entry; `fs`/`fc` = the cell mantissa (at the fold's scale) and its
/// some-valid witness (0 = an all-NULL group, never matches).
#[derive(Clone, Debug, Default)]
pub struct StageRows {
    pub nk: usize,
    pub kw: Vec<i64>,
    pub fs: Vec<i128>,
    pub fc: Vec<u32>,
}

/// [mapstage] The grouped-build map law hosted at stage grain: key-equal
/// stage rows collapse into one representative at stage seal, so the
/// stage answers `key -> aggregate` and a host row's key meets at most
/// one entry. `Word`: the quals read the collapsed cell `col` (all quals
/// name it; `missing` = the count ops' empty-group answer for an absent
/// or NULL host key, None = NULL, never matches). `Num`: the stage cell
/// is the i128 numeric collapse and `qual` is the one comparison —
/// `qual.probe_col` names the HOST column. Semi verdict only (`anti`
/// excluded): an absent group answers NULL and NULL never matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageFold {
    Word(BuildFold),
    Num(NumFold),
}

/// [corrsubq] Aggregate-result-as-MAP: the build table is GROUPED by its
/// equi-key lanes at seal time — key-equal entries collapse into ONE
/// representative whose payload lane `col` holds the group's fold (the
/// fold.rs law: strict, NULL folds nothing; CountStar counts entries;
/// CountCol counts valid cells), so every probe key meets at most one
/// entry and a qual/out against `col` reads the group's aggregate.
/// `missing` = the value a probe key with NO entry answers (count's
/// empty-group law: 0); None = no entry, so a Semi never matches and a
/// Left emits NULL. Two-way Semi/Left nodes only (constructor-checked).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildFold {
    pub col: u32,
    pub op: JoinAggOp,
    pub missing: Option<i64>,
}

/// [corrnumcell] Cell laws: i128 sum, ported-avg finalize, mantissa
/// order-folds (one fixed lane scale, so mantissa order IS value order).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumCellOp {
    Sum,
    Avg,
    Min,
    Max,
}

/// [corrnumcell] The numeric grouped collapse: mantissa lane `col` folds
/// into a per-group i128 cell under `op`; `qual` reads the cell.
/// All-NULL groups answer NULL; no `missing` law. Semi only; exclusive
/// with `BuildFold`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NumFold {
    pub col: u32,
    pub op: NumCellOp,
    pub scale: i32,
    pub qual: NumCellQual,
}

/// [corrnumcell] `probe_col <op> k * cell` at authored scales, evaluated
/// exactly by cross-scaling (numcell.rs); bare aggregates carry k = 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NumCellQual {
    pub probe_col: u32,
    pub op: JoinCmp,
    pub k_m: i64,
    pub k_scale: i32,
    pub probe_scale: i32,
}

/// `out_tys` is index-aligned with `out`, resolved at construction.
#[derive(Clone, Debug)]
pub struct JoinNode {
    pub q: u32,
    pub join_type: JoinType,
    pub keys: Vec<JoinKey>,
    pub quals: Vec<JoinQual>,
    pub build_pred: Option<PredSpec>,
    pub probe_pred: Option<PredSpec>,
    /// [sqe-tpch-mech] survivor-set membership filters (None = none).
    pub build_in: Option<InSetFilter>,
    pub probe_in: Option<InSetFilter>,
    /// [corrsubq] grouped-build collapse (None = the plain table).
    pub build_fold: Option<BuildFold>,
    /// [corrnumcell] numeric grouped collapse (exclusive with the word
    /// fold; None = neither).
    pub num_fold: Option<NumFold>,
    /// [sqe-mech3] dimension build stages (empty = the 2-way family).
    pub dims: Vec<DimStage>,
    /// [crossdim-or] staged disjunction (None = conjunctive node).
    pub staged_or: Option<StagedOr>,
    /// [semianti-flt] semi/anti membership row filters (empty = none).
    pub filters: Vec<FilterStage>,
    pub out: Vec<JoinOut>,
    pub out_tys: Vec<TypMeta>,
    /// [packednum] Per-out staging class, resolved at construction from
    /// the FACE (not the type): `true` = byte lane (Face::Varlena),
    /// `false` = word lane — a witnessed PackedNumeric column is a WORD
    /// out (mantissas) even though its type is varlena.
    pub out_bytes: Vec<bool>,
    /// Build-rows estimate the planner decided sides on; never re-decided.
    pub build_rows_hint: u64,
    /// Exceeding it is a typed refusal, never an OOM (spill = P4-2d).
    pub build_budget_bytes: usize,
    pub l2_bytes: usize,
}

/// Ungrouped aggregate ops folded inside the probe loop (fold.rs law:
/// strict transitions, NULL folds nothing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinAggOp {
    CountStar,
    CountCol,
    Sum,
    Min,
    Max,
    /// Exact per-group distinct set over one word lane; grouped shapes
    /// only, composite sink only (no AccumCell — the set IS the state).
    CountDistinct,
}

impl JoinAggOp {
    /// None = rides the joined-row count, no word-cell fold.
    pub fn fold_op(self) -> Option<AggFoldOp> {
        match self {
            JoinAggOp::CountStar | JoinAggOp::CountDistinct => None,
            JoinAggOp::CountCol => Some(AggFoldOp::CountCol),
            JoinAggOp::Sum => Some(AggFoldOp::Sum),
            JoinAggOp::Min => Some(AggFoldOp::Min),
            JoinAggOp::Max => Some(AggFoldOp::Max),
        }
    }
}

/// [sqe-mech3] Fused-arithmetic fold input over the joined stream (the
/// P4-1 vocabulary composed onto the join fold loop): `AddK` = a + k,
/// `MulCC` = a * b, `MulKSub` = a * (k - b). `w`/`wi` are the PG ops'
/// RESULT widths — the no-overflow-proof obligation the admission
/// witness discharges (`check_join_agg`); an unproven shape refuses
/// typed, never wraps.
///
/// PRODUCT-BOUND LAW ([sqe-q9arith], the MulCC obligation composed
/// across the membrane): each operand's witnessed interval comes from
/// ITS OWN side's column domain (exact stats when the Witness membrane
/// grants them, else the face's type range — both honest supersets of
/// any filtered/joined stream, since a join never invents values and
/// NULL operands fold nothing). The product interval is the four-corner
/// extremum min/max{al·bl, al·bh, ah·bl, ah·bh} computed in i128 (i64-
/// class corners cannot overflow i128); it must fit the mul op's result
/// width `w` — for w=8 that is the |a|max·|b|max ≤ 2^63−1 corner check —
/// else the typed refusal (`agg-arith-overflow-unwitnessed`). MulKSub
/// proves the INNER (k−b) interval against `wi` first, then the product
/// of `a` with that interval against `w`. Inside an admitted proof every
/// per-row word satisfies |word| < 2^63, so the fold's i128 cell sum
/// stays exact for any n < 2^64 joined rows (the SumSq exactness
/// argument).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinArith {
    AddK { k: i64, w: u8 },
    MulCC { w: u8 },
    MulKSub { k: i64, w: u8, wi: u8 },
    /// [scale-alg] `a · (k ± b)` over two PackedNumeric mantissa lanes:
    /// the exact product mantissa at scale sa+sb (`k` at b's scale;
    /// `sub` = k − b). `sa`/`sb` are the authored scales the engine
    /// re-proves; representability in i64 is witnessed in `check_join_agg`.
    PackedMulK { k: i64, sub: bool, sa: i32, sb: i32 },
    /// [scale-alg] `a·(k ± b) − c·d`: exact products align to scale
    /// max(sa+sb, sc+sd) and difference there; `c`/`d` ride `join.out`.
    PackedMulKSubCC { k: i64, sub: bool, sa: i32, sb: i32, c: JoinOut, sc: i32, d: JoinOut, sd: i32 },
}

impl JoinArith {
    /// Shapes that carry a second operand column (`input2`).
    pub fn needs_input2(self) -> bool {
        !matches!(self, JoinArith::AddK { .. })
    }

    /// The subtracted product's operand lanes, when the shape has them.
    pub fn cd_lanes(self) -> Option<(JoinOut, JoinOut)> {
        match self {
            JoinArith::PackedMulKSubCC { c, d, .. } => Some((c, d)),
            _ => None,
        }
    }
}

/// [caseleg] The per-joined-row test of a `CASE WHEN test THEN x [ELSE
/// 0] END` fold input: ONE joined-stream column against constants — a
/// word compare (`PredTerm`, 3VL) or a byte test (`VarPredTerm`, C law).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaseTest {
    Word(PredTerm),
    Bytes(VarPredTerm),
    /// Word compare on the mantissa grid of a PackedNumeric{scale}
    /// lane: the authored scale (i32) must equal the WITNESSED face
    /// scale (admission), so the decoded word IS the term's grid.
    Packed(PredTerm, i32),
    /// Word IN-list (sorted, deduped); 3VL: NULL never passes, the
    /// empty list passes nothing.
    InWords(Vec<i64>),
    /// Every leaf TRUE (all over the leg's ONE column).
    And(Vec<CaseTest>),
}

/// [caseleg] A predicated fold leg: rows passing `test` stage the leg's
/// input; others stage `(0, else_zero)` (`ELSE 0` marks the answer
/// non-NULL; no ELSE stages NULL). `input == None` counts passing rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinCaseLeg {
    pub col: JoinOut,
    pub test: CaseTest,
    pub else_zero: bool,
}

/// `input` = the joined-stream column the agg folds (None only for
/// CountStar, or a CountCol case leg); `input2` + `arith` carry the
/// fused-arithmetic fold shape (`input (op) input2`; NULL either operand
/// folds nothing); `case` predicates the leg; `out` resolved at
/// construction (PG's rule set).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinAggSpec {
    pub op: JoinAggOp,
    pub input: Option<JoinOut>,
    pub input2: Option<JoinOut>,
    pub arith: Option<JoinArith>,
    pub case: Option<JoinCaseLeg>,
    pub out: TypMeta,
}

/// One requested aggregate leg (family-admission input currency).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinAggReq {
    pub op: JoinAggOp,
    pub input: Option<JoinOut>,
    pub input2: Option<JoinOut>,
    pub arith: Option<JoinArith>,
    pub case: Option<JoinCaseLeg>,
}

impl JoinAggReq {
    pub fn col(op: JoinAggOp, input: Option<JoinOut>) -> JoinAggReq {
        JoinAggReq { op, input, input2: None, arith: None, case: None }
    }
}

impl JoinAggSpec {
    pub fn out_ty(op: JoinAggOp, in_ty: Option<TypMeta>) -> TypMeta {
        match op {
            JoinAggOp::CountStar | JoinAggOp::CountCol | JoinAggOp::CountDistinct => {
                TypMeta::INT8
            }
            JoinAggOp::Sum => TypMeta::NUMERIC,
            JoinAggOp::Min | JoinAggOp::Max => in_ty.expect("min/max need an input type"),
        }
    }
}

/// Agg goal over a join: the probe loop's consumer is the accumulator —
/// the joined rows are never materialized. `join.out` carries exactly the
/// distinct agg inputs (plus the group keys when grouped); `agg_oi[i]` /
/// `agg_oi2[i]` index `join.out` for `aggs[i]` (usize::MAX for CountStar
/// / no second operand). `groups` are the composite group keys in key
/// order ([sqe-mech3]: any side incl. Dim; word keys or C-collation text
/// keys; NULL key components group as equal — SQL GROUP BY law); the
/// grouped fold vocabulary is the whole `JoinAggOp` set. `group_oi[j]`
/// indexes `join.out` (empty when ungrouped). The answer layout is
/// [keys in key order, aggs in spec order].
#[derive(Clone, Debug)]
pub struct JoinAggNode {
    pub join: JoinNode,
    pub aggs: Vec<JoinAggSpec>,
    pub agg_oi: Vec<usize>,
    pub agg_oi2: Vec<usize>,
    pub groups: Vec<JoinOut>,
    pub group_oi: Vec<usize>,
    /// [caseleg] `agg_oic[i]` indexes `join.out` for `aggs[i].case`'s
    /// column (usize::MAX when the leg carries no CASE).
    pub agg_oic: Vec<usize>,
    /// [yearkey] Per-group-key derivation applied to the staged word
    /// (empty = bare keys; else index-aligned with `groups`).
    pub group_xf: Vec<Option<KeyXf>>,
    /// [sqe-tpch-mech] Generalized HAVING fused into the grouped fold:
    /// `aggs[having.agg]`'s answer value compared per group at the
    /// answer boundary (grouped shapes only; the referenced aggregate
    /// may be a hidden, non-delivered answer column).
    pub having: Option<crate::ir::HavingCmp>,
}

/// [yearkey] A monotone word→word key derivation over a joined-stream
/// column: `Year` = calendar year of a DATE word (NULL stays NULL).
/// [textslice] `TextSlice` = the first-`len`-chars byte prefix of a
/// C-collation varlena image (1-based `from`, admission pins 1); the
/// single-byte-chars witness makes byte == char, short images keep
/// their whole payload (PG's substring clamp), NULL stays NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyXf {
    Year,
    TextSlice { from: u16, len: u16 },
}

impl KeyXf {
    #[inline(always)]
    pub fn apply(self, w: i64) -> i64 {
        match self {
            KeyXf::Year => crate::kernels_f6::pg_date_year(w),
            KeyXf::TextSlice { .. } => unreachable!("byte-lane derivation on a word key"),
        }
    }
    #[inline(always)]
    pub fn apply_bytes(self, b: &[u8]) -> &[u8] {
        match self {
            KeyXf::TextSlice { from, len } => {
                debug_assert_eq!(from, 1);
                &b[..b.len().min(len as usize)]
            }
            KeyXf::Year => unreachable!("word derivation on a byte lane"),
        }
    }
    pub fn out_ty(self) -> TypMeta {
        match self {
            KeyXf::Year => TypMeta::INT4,
            KeyXf::TextSlice { .. } => TypMeta::TEXT_C,
        }
    }
}

/// Module-local this phase; merges into `refuse::Refuse` at phase 2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinRefuse {
    BuildExceedsBudget { est_bytes: u64, budget: u64 },
    /// Keyless (nest-loop) product witness over budget: the exact
    /// post-predicate build cardinality times the probe side's witnessed
    /// row total exceeds the pair budget (P4-5 witness-gate law).
    ProductExceedsBudget { est_pairs: u64, budget: u64 },
    /// Grouped distinct-set bytes crossed the join build budget mid-run.
    DistinctExceedsBudget { bytes: u64, budget: u64 },
    Unsupported { what: &'static str },
    Collation { attno: u32, collation: u32 },
    /// [scale-alg] face-law refusal (`type/face/{what}` vocabulary).
    Face { attno: u32, what: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_out_ty_law() {
        assert_eq!(JoinAggSpec::out_ty(JoinAggOp::CountStar, None), TypMeta::INT8);
        assert_eq!(JoinAggSpec::out_ty(JoinAggOp::CountCol, Some(TypMeta::TEXT_C)), TypMeta::INT8);
        assert_eq!(JoinAggSpec::out_ty(JoinAggOp::Sum, Some(TypMeta::INT4)), TypMeta::NUMERIC);
        assert_eq!(JoinAggSpec::out_ty(JoinAggOp::Min, Some(TypMeta::INT4)), TypMeta::INT4);
        assert_eq!(JoinAggSpec::out_ty(JoinAggOp::Max, Some(TypMeta::INT8)), TypMeta::INT8);
        assert_eq!(
            JoinAggSpec::out_ty(JoinAggOp::CountDistinct, Some(TypMeta::INT4)),
            TypMeta::INT8
        );
    }

    #[test]
    fn key_xf_law() {
        assert_eq!(KeyXf::Year.out_ty(), TypMeta::INT4);
        let xf = KeyXf::TextSlice { from: 1, len: 2 };
        assert_eq!(xf.out_ty(), TypMeta::TEXT_C);
        assert_eq!(xf.apply_bytes(b"13-abc"), b"13");
        assert_eq!(xf.apply_bytes(b"a"), b"a");
        assert_eq!(xf.apply_bytes(b""), b"");
        assert_eq!(KeyXf::TextSlice { from: 1, len: 64 }.apply_bytes(b"ab"), b"ab");
    }

    #[test]
    fn agg_fold_op_law() {
        assert_eq!(JoinAggOp::CountStar.fold_op(), None);
        assert_eq!(JoinAggOp::CountCol.fold_op(), Some(AggFoldOp::CountCol));
        assert_eq!(JoinAggOp::Sum.fold_op(), Some(AggFoldOp::Sum));
        assert_eq!(JoinAggOp::Min.fold_op(), Some(AggFoldOp::Min));
        assert_eq!(JoinAggOp::Max.fold_op(), Some(AggFoldOp::Max));
        assert_eq!(JoinAggOp::CountDistinct.fold_op(), None);
    }
}

impl std::fmt::Display for JoinRefuse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinRefuse::BuildExceedsBudget { est_bytes, budget } => {
                write!(f, "join-build-exceeds-budget:est={est_bytes}:budget={budget}")
            }
            JoinRefuse::ProductExceedsBudget { est_pairs, budget } => {
                write!(f, "join-product-exceeds-budget:est={est_pairs}:budget={budget}")
            }
            JoinRefuse::DistinctExceedsBudget { bytes, budget } => {
                write!(f, "join-distinct-exceeds-budget:bytes={bytes}:budget={budget}")
            }
            JoinRefuse::Unsupported { what } => write!(f, "join-unsupported:{what}"),
            JoinRefuse::Collation { attno, collation } => {
                write!(f, "join-collation-unsupported:c{attno}:coll={collation}")
            }
            JoinRefuse::Face { attno, what } => write!(f, "join-face-unsupported:c{attno}:{what}"),
        }
    }
}
