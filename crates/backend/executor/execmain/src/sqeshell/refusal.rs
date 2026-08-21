//! The sqe typed-refusal lattice (P1-4 design, refusal-enum.md; built as
//! part of P2-1 per production-plan §2/§P2-1).
//!
//! Rules of record (each closes a named prior failure):
//! - Rule A (lanev2): the census key is the kebab-case STRING, never a
//!   discriminant. Variants that stop firing are DELETED with their
//!   allowlist row — no tombstones.
//! - Rule B (lanev4): no positional cause array. The enum + derived key
//!   IS the registry; merges conflict on a line of code, not a number.
//! - Rule C (lanev4): no `Untyped` variant, no side channel — `lower()`
//!   RETURNS the `Refusal` by value; a refusal without a cause is
//!   unrepresentable.
//! - Rule D (lanev3 AdmitCause): the tick site IS the error constructor.
//!   `Refusal` has a private witness field, so the ONLY constructor is
//!   `RefuseCause::refuse`, which ticks the census unconditionally on
//!   the production path. There is no instrumentation wiring to forget.
//!
//! R1 semantics: every refusal on a columnar table is a typed
//! user-visible ERROR (0A000 / 53400 by family); there is no fallback.
//! Heap tables never reach this module (the dispatch slot consults the
//! AM first) — on heap statements zero instructions here execute.
#![deny(clippy::wildcard_enum_match_arm)]

use types_error::PgError;
use types_nodes::NodeTag;

// ---------------------------------------------------------------------------
// Carried tag enums (closed; Copy; all matched without wildcards below)
// ---------------------------------------------------------------------------

/// Shape details for `NodeShape` — a small CLOSED enum per refusal site,
/// never a free string (strings would evade exhaustiveness+allowlisting).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShapeDetail {
    /// Agg with GROUPING SETS / chain.
    GroupingSets,
    /// Agg strategy outside the recognized set (plain/hash split rungs).
    Strategy,
    /// Agg split outside AGGSPLIT_SIMPLE (parallel split halves — the
    /// planner suppression should keep these away; defensive).
    Split,
    /// HAVING qual on the recognized node.
    HavingQual,
    /// initPlan on an admitted node.
    InitPlan,
    /// Unexpected right child.
    RightChild,
    /// Non-Aggref (or junk) entry in the Agg targetlist.
    NonAggrefTarget,
    /// Aggref qualifier outside the vocabulary (DISTINCT/ORDER/FILTER/
    /// direct args/levelsup/kind).
    AggQualifier,
    /// Scan carrying children/initPlan.
    ScanChildren,
    /// Grouped Agg with a group-key targetlist entry outside the plain-
    /// Var vocabulary (expressions, duplicated keys) — P4-1f grows this.
    GroupKey,
    /// Sort key does not map onto the served subtree's output (junk key,
    /// unsortable answer class such as AVG's exact ratio).
    SortKey,
    /// Sort operator oid outside the recognized ordering vocabulary.
    SortOperator,
    /// Junk-bearing targetlist (junk filter installed).
    JunkTlist,
    /// The plan's result descriptor disagrees with the lowered answer
    /// shape (arity/type pin at engagement).
    ResultArity,
    /// LIMIT ... WITH TIES (needs order semantics the slice lacks).
    LimitTies,
    /// LIMIT/OFFSET expression that is not a Const (un-folded volatile
    /// expressions and un-folded params — recognition runs before the
    /// run/bind-params residual, so a Param bound names THIS cause).
    LimitNonConst,
    /// Upper node's targetlist is not a positional passthrough of its
    /// child (projection above the served subtree).
    UpperTlist,
    /// Join type outside the served set (RIGHT/FULL and the right-side
    /// variants need build-side match flags).
    JoinType,
    /// Hash clause outside the Var = Var equi-key vocabulary.
    JoinClause,
    /// ON-residual joinqual outside the Var CMP Var vocabulary.
    JoinQual,
    /// Post-join filter qual on the join node itself.
    JoinFilter,
    /// Parameterized nest-loop inner (nestParams — index-driven rescans,
    /// the OLTP-residual class). PARAM_EXEC statements refuse upstream
    /// as run/bind-params, so this is the defensive backstop (the
    /// `Split` precedent).
    JoinParam,
    /// Join side scanning a non-columnar relation (mixed-AM statement).
    MixedAm,
    /// Gating Result (One-Time Filter) somewhere other than directly
    /// above the served SeqScan (gate over a join/agg subtree).
    GateChild,
    /// VALUES cell outside the Const vocabulary, or a values list whose
    /// rows disagree with the derived column schema.
    ValuesCell,
    /// [sqe-setops] Set-op/append child subtree outside the served
    /// single-relation goal classes (joins, nested set-ops, ragged or
    /// junk-bearing child targetlists). The keyless-const full-join
    /// children ride the same law (shape/merge-join/set-child).
    SetChild,
    /// [sqe-setops] Set-op key law breach: the dedup/cmp key set does
    /// not cover every delivered column (surviving-row residue would be
    /// nondeterministic), or uniq keys ride outside the asserted sort
    /// keys (adjacent dedup would be unsound).
    SetKey,
    /// [winserve v1] WindowAgg with a non-default frame (explicit
    /// ROWS/RANGE/GROUPS bounds beyond the rank-family ROWS rewrite) or
    /// frame-offset expressions.
    WindowFrame,
    /// [winserve v1] WindowAgg/WindowFunc carrying a run condition (the
    /// monotonic-qual pushdown shape).
    WindowRunCondition,
    /// [winserve v1] Stacked WindowAgg nodes — multiple window specs in
    /// one statement (per-spec re-sort is a chartered v2 rung).
    WindowChain,
    /// [sqe-subq] Un-pushed qual on a recognized pass-through node
    /// (SubqueryScan filtering its subquery's output).
    NodeQual,
    /// [sqe-generic-c] FunctionScan WITH ORDINALITY (the ordinal column
    /// is outside the series-bank vocabulary).
    Ordinality,
    /// [sqe-generic-c] SRF argument/shape outside the vocabulary:
    /// non-Const generate_series args (per-row bounds), multi-function
    /// FROM lists, more than one SRF in a ProjectSet targetlist.
    SrfArgs,
    /// [sqe-generic-c] CteScan whose subplan is not a RecursiveUnion
    /// (materialized non-recursive CTEs stay out), or a CteScan shape
    /// outside the recognized outer query.
    CtePlan,
    /// [sqe-generic-c] WITH RECURSIVE non-recursive term outside the
    /// served seed classes (Const Result row / served single-relation
    /// goal).
    RecursiveSeed,
    /// [sqe-generic-c] WITH RECURSIVE recursive term outside the served
    /// step classes (bare WorkTableScan program / inner equi-join of the
    /// worktable with one columnar scan).
    RecursiveTerm,
    /// [corrsubq] sub-plan shape details (correlated-subq-1.md §4).
    SubPlanLink,
    SubPlanParam,
    SubPlanInner,
    SubPlanKey,
    SubPlanAggOp,
    SubPlanNegated,
    SubPlanMulti,
    /// [corrsubq] rung 2: a non-equality correlated conjunct inside a
    /// keyed-map inner (the group would need per-probe filtering).
    SubPlanResidual,
}

/// Engine lowering-vocabulary steps (the engine's `sqe::refuse::Refuse`
/// causes surface through this tag — tier-vocabulary family).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepKind {
    /// Predicate leaf outside the lowering vocabulary (incl. the engine's
    /// `pred-unsupported:like-pattern-trailing-escape`).
    Pred,
    /// IN-list width outside the vocabulary.
    InList,
    /// Literal does not parse as the column's type.
    Const,
    /// Aggregate shape outside the engine vocabulary.
    Agg,
    /// HAVING vocabulary.
    Having,
    /// MetadataAnswer predicate not stats-answerable.
    StatsAnswer,
    /// No family-election rule fired for the shape (incl. the engine's
    /// `no-family-rule:r6d-consumer-shape` — LIKE consumers off-shape).
    FamilyRule,
    /// Elected family has no registered stencil.
    FamilyRegistry,
    /// Aggregate render (answer -> descriptor datum) outside the
    /// emitter's vocabulary.
    Render,
    /// Grouped shape outside the server-proven grouped vocabulary
    /// (P3 rung C admission — `sqe::planner::check_server_grouped`).
    Group,
    /// Elected family not yet proven on the server path (rig-only
    /// lanes: frame walks, gathers, two-level, distinct pipelines).
    FamilyServe,
    /// Join shape outside the engine's admission vocabulary
    /// (`sqe::joins::JoinRefuse::Unsupported`).
    Join,
    /// Grouped shape without a sound group-count witness under the
    /// stencils' emit cap (serving would risk silent truncation).
    GroupCountWitness,
    /// Unbounded row-returning scan without a stats witness bounding the
    /// surviving rows under the answer cap (the scan-answer law).
    ScanRowsWitness,
    /// [sortgrp v1] Order-sensitive aggregate shape outside the
    /// SortGrouped v1 admission (mixed per-agg sort specs, ORDER-BY-less
    /// string_agg/array_agg, non-const delimiters/fractions, agg mixes).
    SortAgg,
    /// [sortgrp v1] SortGrouped payload bytes unwitnessed by the stats
    /// faces — serving would risk unbounded memory.
    SortAggBytesWitness,
    /// [sortgrp v1] Witnessed SortGrouped payload bytes over the
    /// admission budget (spill is P6-1; typed refusal, never an OOM).
    SortAggBudget,
    /// [winserve v1] Window shape outside the WindowServe v1 engine
    /// admission (function/argument shapes, spec drift).
    WinAgg,
    /// [winserve v1] WindowServe payload bytes unwitnessed by the stats
    /// faces — serving would risk unbounded memory.
    WinAggBytesWitness,
    /// [winserve v1] Witnessed WindowServe payload bytes over the
    /// admission budget (spill is P6-1; typed refusal, never an OOM).
    WinAggBudget,
}

/// Heap-face v1 typed causes (heap-on-sqe, heap-face.md + the 2026-08-18
/// RULING: R1 applies to heap — a RECOGNIZED heap shape serves or ERRORs
/// typed/censused; unrecognized shapes route to the incumbent only until
/// their rung lands, so these fire only inside the landed rungs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeapDetail {
    /// Grouped fold whose group count has no sound witness (no stats
    /// plane on heap — only narrow-word type domains or pushed bounds
    /// admit under the 2^20 cap; heap-face.md §2.3).
    GroupCountUnwitnessed,
    /// A caching plane was armed on the per-statement heap face (the
    /// born-RED cache law, heap-face.md §1.3) — an internal invariant
    /// breach, never a capability gap.
    CacheLaw,
    /// The face met a column it has no v1 lane law for (varlena/fixed
    /// byref lanes land with the scan-serve rung).
    Face,
    /// Live groups exceeded the admitted witness (witness bug — fail
    /// loudly, never truncate; the cb43 q31 lesson).
    GroupCap,
    /// Recognized shape outside the measured 1.05x-of-the-row-engine
    /// admission band (Michael's per-shape admission ruling, 2026-08-18:
    /// under no-fallback a served-slower shape is a user-visible
    /// regression with no escape — fix or refuse, never serve slow).
    /// Lifted per shape as its parity fix lands; the census drives it.
    PerfUnadmitted,
    /// [rung 3] A text byte lane met an out-of-line TOAST pointer at
    /// fill: detoast crosses the toast relation (backend state) — an
    /// unlanded rung, refused with the true cause.
    TextOutOfLine,
    /// [rung 3] A text byte lane met an inline-compressed image whose
    /// method is not pglz (this build carries no LZ4, matching C without
    /// USE_LZ4).
    TextCompression,
    /// [joins] Typed residue at the recognized heap-join boundary (the
    /// charter list): the payload is the census key tail, e.g.
    /// "join-outer" / "join-non-equi" / "join-text" / "join-multiway".
    Join(&'static str),
}

impl HeapDetail {
    pub fn key(self) -> &'static str {
        match self {
            HeapDetail::GroupCountUnwitnessed => "group-count-unwitnessed",
            HeapDetail::CacheLaw => "cache-law",
            HeapDetail::Face => "face",
            HeapDetail::GroupCap => "group-cap",
            HeapDetail::PerfUnadmitted => "perf-unadmitted",
            HeapDetail::TextOutOfLine => "text-out-of-line",
            HeapDetail::TextCompression => "text-compression",
            HeapDetail::Join(what) => what,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    A,
    B,
}

/// Resource families (P6-1 currency; zero constructors pre-P4-2d — the
/// variants are the spec of what's coming, visible as zero census rows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FamilyKind {
    HashGroup,
    Distinct,
    Join,
    Sort,
}

// ---------------------------------------------------------------------------
// The cause enum
// ---------------------------------------------------------------------------

/// Why the sqe engine refused a statement over a columnar table. R1:
/// every refusal is a typed user-visible ERROR; there is no fallback.
/// R6: this enum is the spec of what's missing. NO wildcard arm may ever
/// match it and no `Refusal` may be constructed except through
/// [`RefuseCause::refuse`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefuseCause {
    /// Plan node type with no lowering recognizer. key: node/<tag>
    UnsupportedNode(NodeTag),
    /// Node type recognized, this plan's shape outside the landed
    /// recognizer. key: shape/<tag>/<detail>
    NodeShape(NodeTag, ShapeDetail),
    /// Expression node kind outside the vocabulary. key: expr/<tag>
    UnsupportedExpr(NodeTag),
    /// Function/operator not servable. key: expr/fn/<oid>
    UnsupportedFunction { fn_oid: u32 },
    /// Column/expression type without lane currency. key: type/<oid>
    UnsupportedType { type_oid: u32 },
    /// A nullable column on a shape whose body is not 3VL-threaded, or a
    /// nullable dict column (dict lanes carry a zero-null proof until
    /// nullable-dict lands — the engine-currency lattice).
    /// key: type/nullable/<dict|plain>
    NullableColumn { dict: bool },
    /// A storage face outside an operation's admitted set (SUM over
    /// float/unsigned/fixed, width-8 unsigned words...).
    /// key: type/face/<what>
    UnsupportedFace { what: &'static str },
    /// [sqe-bpchar] A bpchar shape outside the pad-aware law (Michael's
    /// ruling 2026-08-19): `bare` = typmod −1 (no uniform-padding
    /// witness), `order` = order/range comparison (bpcharcmp trims
    /// before comparing; padded-memcmp diverges below 0x20 — oracle trap
    /// T5b), `cross-width` / `join-key` = column-vs-column comparisons
    /// awaiting the trimmed-basis gate (case 2), `encoding` = a
    /// non-UTF8 multibyte server encoding (the pad law counts CHARS).
    /// key: type/bpchar/<what>
    BpcharLaw { what: &'static str },
    /// Non-byte-order collation on an order/group/compare path (DEFAULT
    /// resolves through the database locale — the collation-currency
    /// law; class C serves, every other class lands here).
    /// [json-rung1] A recognized jsonb path extraction whose shred lane
    /// the bank did not witness: path never elected / absent in some
    /// part / sealed with a different lane type than the form needs /
    /// unaddressable in the path grammar (dotted or empty key). The
    /// witness law is all-parts + right-type — anything less refuses
    /// typed, never a partial or image-fallback answer.
    /// key: type/jsonb/<what>
    JsonbLane { what: &'static str },
    /// Non-C collation on an order/group/compare path.
    /// key: collation/non-c/<oid>
    NonCCollation { coll_oid: u32 },
    /// Nondeterministic collation on an equality/grouping path.
    /// key: collation/nondeterministic
    NondeterministicCollation { coll_oid: u32 },
    /// Lowered step outside the engine's vocabulary (the engine-side
    /// `Refuse` causes surface here). key: tier/step/<step>
    TierVocab { step: StepKind },
    /// A tier kill switch is on and the survivor cannot serve the shape.
    /// key: tier/killed/<a|b>
    TierKilled { tier: Tier },
    /// The engine master switch is off while a columnar table is
    /// addressed (R1 consequence (b): a kill is a typed error, never a
    /// slow path). key: tier/engine-disarmed
    EngineDisarmed,
    /// Admission-time memory prior over budget, no spill path for the
    /// family. key: resource/memory-budget/<family>  SQLSTATE 53400
    MemoryBudget { family: FamilyKind },
    /// Spill demanded before the family's spill path lands.
    /// key: resource/spill-unavailable/<family>  SQLSTATE 53400
    SpillUnavailable { family: FamilyKind },
    /// The answer-bytes law (E17 answer face): a retained answer plane
    /// exceeds the answer-face budget — [sqe-cursors] the P6-4 cursor
    /// spool prices its full materialized answer here at open (correct
    /// answer or typed refusal; never truncation, never OOM). The
    /// cap-retire lane's finalize answer-bytes law shares this variant.
    /// key: resource/answer-bytes/<what>  SQLSTATE 53400
    AnswerBytes { what: &'static str },
    /// [spill-2] A spill substrate I/O event failed mid-statement
    /// (create/append/read on a temp spill file) — a RUNTIME error
    /// through the RunRefusal seam, not a capability refusal: the
    /// statement fails typed, the server lives, the store's drop still
    /// deletes the tree. key: run/spill-io/<op>  SQLSTATE 58030
    SpillIo { op: &'static str },
    /// Scrollable-cursor demand past the bounded spool (P6-4).
    /// key: resource/scrollable-cursor
    ScrollableCursor,
    /// EvalPlanQual recheck reached dispatch. key: run/epq
    EpqRecheck,
    /// Bounded portal drain (cursor FETCH cadence) pre-P6-4.
    /// key: run/cursor-cadence
    CursorCadence,
    /// SPI budgeted run pre-P6-4. key: run/spi-cadence
    SpiCadence,
    /// Bind params (extern bind values or subplan exec params) on a
    /// statement recognition otherwise serves, pre-fragment-cache (P6-4).
    /// The ONLY-blocker cause: it fires AFTER `lower()` succeeds, never
    /// preempting a true node/shape/expr cause — the EXPLAIN-vs-exec
    /// cause-consistency law. key: run/bind-params
    BindParams,
    /// Heap-face v1 causes (landed-rung refusals only). key: heap/<detail>
    Heap(HeapDetail),
}

fn kebab(tag: &str) -> String {
    // NodeTag debug form "T_SeqScan" -> "seq-scan".
    let raw = tag.strip_prefix("T_").unwrap_or(tag);
    let mut out = String::with_capacity(raw.len() + 4);
    for (i, c) in raw.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i != 0 {
                out.push('-');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

impl ShapeDetail {
    pub fn key(self) -> &'static str {
        match self {
            ShapeDetail::GroupingSets => "grouping-sets",
            ShapeDetail::Strategy => "strategy",
            ShapeDetail::Split => "split",
            ShapeDetail::HavingQual => "having",
            ShapeDetail::InitPlan => "init-plan",
            ShapeDetail::RightChild => "right-child",
            ShapeDetail::NonAggrefTarget => "non-aggref-target",
            ShapeDetail::AggQualifier => "agg-qualifier",
            ShapeDetail::ScanChildren => "scan-children",
            ShapeDetail::GroupKey => "group-key",
            ShapeDetail::SortKey => "sort-key",
            ShapeDetail::SortOperator => "sort-operator",
            ShapeDetail::JunkTlist => "junk-tlist",
            ShapeDetail::ResultArity => "result-arity",
            ShapeDetail::LimitTies => "with-ties",
            ShapeDetail::LimitNonConst => "non-const",
            ShapeDetail::UpperTlist => "upper-tlist",
            ShapeDetail::JoinType => "join-type",
            ShapeDetail::JoinClause => "join-clause",
            ShapeDetail::JoinQual => "join-qual",
            ShapeDetail::JoinFilter => "join-filter",
            ShapeDetail::JoinParam => "join-param",
            ShapeDetail::MixedAm => "mixed-am",
            ShapeDetail::GateChild => "gate-child",
            ShapeDetail::ValuesCell => "values-cell",
            ShapeDetail::SetChild => "set-child",
            ShapeDetail::SetKey => "set-key",
            ShapeDetail::WindowFrame => "frame",
            ShapeDetail::WindowRunCondition => "run-condition",
            ShapeDetail::WindowChain => "window-chain",
            ShapeDetail::NodeQual => "qual",
            ShapeDetail::Ordinality => "ordinality",
            ShapeDetail::SrfArgs => "srf-args",
            ShapeDetail::CtePlan => "cte-plan",
            ShapeDetail::RecursiveSeed => "recursive-seed",
            ShapeDetail::RecursiveTerm => "recursive-term",
            ShapeDetail::SubPlanLink => "link-type",
            ShapeDetail::SubPlanParam => "param-source",
            ShapeDetail::SubPlanInner => "inner-plan",
            ShapeDetail::SubPlanKey => "no-equi-key",
            ShapeDetail::SubPlanAggOp => "agg-op",
            ShapeDetail::SubPlanNegated => "negated",
            ShapeDetail::SubPlanMulti => "multi",
            ShapeDetail::SubPlanResidual => "agg-residual",
        }
    }
}

impl StepKind {
    pub fn key(self) -> &'static str {
        match self {
            StepKind::Pred => "pred",
            StepKind::InList => "in-list",
            StepKind::Const => "const",
            StepKind::Agg => "agg",
            StepKind::Having => "having",
            StepKind::StatsAnswer => "stats-answer",
            StepKind::FamilyRule => "family-rule",
            StepKind::FamilyRegistry => "family-registry",
            StepKind::Render => "render",
            StepKind::Group => "group",
            StepKind::FamilyServe => "family-serve",
            StepKind::Join => "join",
            StepKind::GroupCountWitness => "group-count-unwitnessed",
            StepKind::ScanRowsWitness => "scan-rows-unwitnessed",
            StepKind::SortAgg => "sortagg",
            StepKind::SortAggBytesWitness => "sortagg-bytes-unwitnessed",
            StepKind::SortAggBudget => "sortagg-over-budget",
            StepKind::WinAgg => "winagg",
            StepKind::WinAggBytesWitness => "winagg-bytes-unwitnessed",
            StepKind::WinAggBudget => "winagg-over-budget",
        }
    }
}

impl FamilyKind {
    pub fn key(self) -> &'static str {
        match self {
            FamilyKind::HashGroup => "hashgroup",
            FamilyKind::Distinct => "distinct",
            FamilyKind::Join => "join",
            FamilyKind::Sort => "sort",
        }
    }
}

impl RefuseCause {
    /// The full hierarchical census key `family/detail[/subdetail]` —
    /// the ONLY machine currency (Rule A).
    pub fn census_key(self) -> String {
        match self {
            RefuseCause::UnsupportedNode(t) => format!("node/{}", kebab(&format!("{t:?}"))),
            RefuseCause::NodeShape(t, d) => {
                format!("shape/{}/{}", kebab(&format!("{t:?}")), d.key())
            }
            RefuseCause::UnsupportedExpr(t) => format!("expr/{}", kebab(&format!("{t:?}"))),
            RefuseCause::UnsupportedFunction { fn_oid } => format!("expr/fn/{fn_oid}"),
            RefuseCause::UnsupportedType { type_oid } => format!("type/{type_oid}"),
            RefuseCause::NullableColumn { dict } => {
                format!("type/nullable/{}", if dict { "dict" } else { "plain" })
            }
            RefuseCause::UnsupportedFace { what } => format!("type/face/{what}"),
            RefuseCause::BpcharLaw { what } => format!("type/bpchar/{what}"),
            RefuseCause::JsonbLane { what } => format!("type/jsonb/{what}"),
            RefuseCause::NonCCollation { coll_oid } => format!("collation/non-c/{coll_oid}"),
            RefuseCause::NondeterministicCollation { .. } => {
                "collation/nondeterministic".to_string()
            }
            RefuseCause::TierVocab { step } => format!("tier/step/{}", step.key()),
            RefuseCause::TierKilled { tier } => format!(
                "tier/killed/{}",
                match tier {
                    Tier::A => "a",
                    Tier::B => "b",
                }
            ),
            RefuseCause::EngineDisarmed => "tier/engine-disarmed".to_string(),
            RefuseCause::MemoryBudget { family } => {
                format!("resource/memory-budget/{}", family.key())
            }
            RefuseCause::SpillUnavailable { family } => {
                format!("resource/spill-unavailable/{}", family.key())
            }
            RefuseCause::AnswerBytes { what } => format!("resource/answer-bytes/{what}"),
            RefuseCause::SpillIo { op } => format!("run/spill-io/{op}"),
            RefuseCause::ScrollableCursor => "resource/scrollable-cursor".to_string(),
            RefuseCause::EpqRecheck => "run/epq".to_string(),
            RefuseCause::CursorCadence => "run/cursor-cadence".to_string(),
            RefuseCause::SpiCadence => "run/spi-cadence".to_string(),
            RefuseCause::BindParams => "run/bind-params".to_string(),
            RefuseCause::Heap(d) => format!("heap/{}", d.key()),
        }
    }

    /// The variant-grain key (static; the EXPLAIN engine_record channel's
    /// `&'static str` currency and the census's zero-row seed grain).
    pub fn variant_key(self) -> &'static str {
        match self {
            RefuseCause::UnsupportedNode(_) => "node",
            RefuseCause::NodeShape(..) => "shape",
            RefuseCause::UnsupportedExpr(_) => "expr",
            RefuseCause::UnsupportedFunction { .. } => "expr/fn",
            RefuseCause::UnsupportedType { .. } => "type",
            RefuseCause::NullableColumn { .. } => "type/nullable",
            RefuseCause::UnsupportedFace { .. } => "type/face",
            RefuseCause::BpcharLaw { .. } => "type/bpchar",
            RefuseCause::JsonbLane { .. } => "type/jsonb",
            RefuseCause::NonCCollation { .. } => "collation/non-c",
            RefuseCause::NondeterministicCollation { .. } => "collation/nondeterministic",
            RefuseCause::TierVocab { .. } => "tier/step",
            RefuseCause::TierKilled { .. } => "tier/killed",
            RefuseCause::EngineDisarmed => "tier/engine-disarmed",
            RefuseCause::MemoryBudget { .. } => "resource/memory-budget",
            RefuseCause::SpillUnavailable { .. } => "resource/spill-unavailable",
            RefuseCause::AnswerBytes { .. } => "resource/answer-bytes",
            RefuseCause::SpillIo { .. } => "run/spill-io",
            RefuseCause::ScrollableCursor => "resource/scrollable-cursor",
            RefuseCause::EpqRecheck => "run/epq",
            RefuseCause::CursorCadence => "run/cursor-cadence",
            RefuseCause::SpiCadence => "run/spi-cadence",
            RefuseCause::BindParams => "run/bind-params",
            RefuseCause::Heap(_) => "heap",
        }
    }

    /// SQLSTATE policy (design §4): 0A000 feature_not_supported for the
    /// capability families; 53400 configuration_limit_exceeded for the
    /// resource-budget causes (raising the budget is a legitimate remedy
    /// — 0A000 would lie about what to fix).
    pub fn sqlstate(self) -> types_error::SqlState {
        match self {
            RefuseCause::MemoryBudget { .. }
            | RefuseCause::SpillUnavailable { .. }
            | RefuseCause::AnswerBytes { .. } => {
                types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
            }
            // The sortagg/window admission budgets are resource causes
            // (the remedy is the budget knob) — 53400 like the rest of
            // the class, not the capability 0A000 they rode in on.
            RefuseCause::TierVocab {
                step: StepKind::SortAggBudget | StepKind::WinAggBudget,
            } => types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            // A failed temp-file event is C's I/O class (58030), not a
            // capability or budget cause — nothing to "raise", the disk
            // failed.
            RefuseCause::SpillIo { .. } => types_error::ERRCODE_IO_ERROR,
            RefuseCause::UnsupportedNode(_)
            | RefuseCause::NodeShape(..)
            | RefuseCause::UnsupportedExpr(_)
            | RefuseCause::UnsupportedFunction { .. }
            | RefuseCause::UnsupportedType { .. }
            | RefuseCause::NullableColumn { .. }
            | RefuseCause::UnsupportedFace { .. }
            | RefuseCause::BpcharLaw { .. }
            | RefuseCause::JsonbLane { .. }
            | RefuseCause::NonCCollation { .. }
            | RefuseCause::NondeterministicCollation { .. }
            | RefuseCause::TierVocab { .. }
            | RefuseCause::TierKilled { .. }
            | RefuseCause::EngineDisarmed
            | RefuseCause::ScrollableCursor
            | RefuseCause::EpqRecheck
            | RefuseCause::CursorCadence
            | RefuseCause::SpiCadence
            | RefuseCause::BindParams => types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            // The cache law and a witness-cap breach are invariant
            // breaches (never a capability gap); the rest are 0A000.
            RefuseCause::Heap(HeapDetail::CacheLaw | HeapDetail::GroupCap) => {
                types_error::ERRCODE_INTERNAL_ERROR
            }
            RefuseCause::Heap(
                HeapDetail::GroupCountUnwitnessed
                | HeapDetail::Face
                | HeapDetail::PerfUnadmitted
                | HeapDetail::TextOutOfLine
                | HeapDetail::TextCompression
                | HeapDetail::Join(_),
            ) => types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        }
    }

    /// Taxonomy family (the census view's `family` column).
    pub fn family(self) -> &'static str {
        match self {
            RefuseCause::UnsupportedNode(_) => "node",
            RefuseCause::NodeShape(..) => "shape",
            RefuseCause::UnsupportedExpr(_)
            | RefuseCause::UnsupportedFunction { .. } => "expr",
            RefuseCause::UnsupportedType { .. }
            | RefuseCause::NullableColumn { .. }
            | RefuseCause::UnsupportedFace { .. }
            | RefuseCause::BpcharLaw { .. } => "type",
            | RefuseCause::JsonbLane { .. } => "type",
            RefuseCause::NonCCollation { .. }
            | RefuseCause::NondeterministicCollation { .. } => "collation",
            RefuseCause::TierVocab { .. }
            | RefuseCause::TierKilled { .. }
            | RefuseCause::EngineDisarmed => "tier",
            RefuseCause::MemoryBudget { .. }
            | RefuseCause::SpillUnavailable { .. }
            | RefuseCause::AnswerBytes { .. }
            | RefuseCause::ScrollableCursor => "resource",
            RefuseCause::EpqRecheck
            | RefuseCause::CursorCadence
            | RefuseCause::SpiCadence
            | RefuseCause::BindParams
            | RefuseCause::SpillIo { .. } => "run",
            RefuseCause::Heap(_) => "heap",
        }
    }

    /// THE tick site (Rule D). Census increment happens here,
    /// unconditionally, on the production path — not in the reporter,
    /// not under #[cfg(test)].
    #[cold]
    pub fn refuse(self, source_fp: u64) -> Refusal {
        super::stat::tick_refusal(self, source_fp);
        Refusal { cause: self, source_fp, _witnessed: Witnessed(()) }
    }

    /// The born-RED seed lever's constructor
    /// (`PGRUST_SQE_SEED_REFUSAL=<variant-key>`): the key must name a
    /// REAL variant — the same no-wildcard match that derives keys.
    pub fn from_seed_key(key: &str) -> Option<RefuseCause> {
        Some(match key {
            "node" => RefuseCause::UnsupportedNode(NodeTag::T_Invalid),
            "shape" => RefuseCause::NodeShape(NodeTag::T_Invalid, ShapeDetail::Strategy),
            "expr" => RefuseCause::UnsupportedExpr(NodeTag::T_Invalid),
            "expr/fn" => RefuseCause::UnsupportedFunction { fn_oid: 0 },
            "type" => RefuseCause::UnsupportedType { type_oid: 0 },
            "type/nullable" => RefuseCause::NullableColumn { dict: false },
            "type/face" => RefuseCause::UnsupportedFace { what: "seed" },
            "type/bpchar" => RefuseCause::BpcharLaw { what: "seed" },
            "type/jsonb" => RefuseCause::JsonbLane { what: "unshredded-path" },
            "collation/non-c" => RefuseCause::NonCCollation { coll_oid: 0 },
            "collation/nondeterministic" => {
                RefuseCause::NondeterministicCollation { coll_oid: 0 }
            }
            "tier/step" => RefuseCause::TierVocab { step: StepKind::Pred },
            "tier/killed" => RefuseCause::TierKilled { tier: Tier::A },
            "tier/engine-disarmed" => RefuseCause::EngineDisarmed,
            "resource/memory-budget" => {
                RefuseCause::MemoryBudget { family: FamilyKind::HashGroup }
            }
            "resource/spill-unavailable" => {
                RefuseCause::SpillUnavailable { family: FamilyKind::HashGroup }
            }
            "resource/answer-bytes" => RefuseCause::AnswerBytes { what: "group-emit" },
            "resource/scrollable-cursor" => RefuseCause::ScrollableCursor,
            "run/epq" => RefuseCause::EpqRecheck,
            "run/cursor-cadence" => RefuseCause::CursorCadence,
            "run/spi-cadence" => RefuseCause::SpiCadence,
            "run/bind-params" => RefuseCause::BindParams,
            "run/spill-io" => RefuseCause::SpillIo { op: "seed" },
            "heap" => RefuseCause::Heap(HeapDetail::GroupCountUnwitnessed),
            _ => return None,
        })
    }

    /// Variant-grain enumeration for the zero-row census seed + the key
    /// invariants test (built from the same no-wildcard vocabulary as
    /// `from_seed_key`).
    pub fn all_for_test() -> Vec<RefuseCause> {
        [
            "node",
            "shape",
            "expr",
            "expr/fn",
            "type",
            "type/nullable",
            "type/face",
            "type/bpchar",
            "type/jsonb",
            "collation/non-c",
            "collation/nondeterministic",
            "tier/step",
            "tier/killed",
            "tier/engine-disarmed",
            "resource/memory-budget",
            "resource/spill-unavailable",
            "resource/answer-bytes",
            "resource/scrollable-cursor",
            "run/epq",
            "run/cursor-cadence",
            "run/spi-cadence",
            "run/bind-params",
            "heap",
        ]
        .iter()
        .map(|k| RefuseCause::from_seed_key(k).expect("seed key names a real variant"))
        .collect()
    }
}

/// Map an engine-side lowering refusal (`sqe::refuse::Refuse`) onto the
/// shell lattice. Rig-only causes (RON plan misses, SQL front end) are
/// structurally unreachable from the server recognizer and map to the
/// nearest vocabulary step defensively.
pub fn from_engine(r: &sqe::refuse::Refuse) -> RefuseCause {
    use sqe::refuse::Refuse as R;
    match r {
        R::NoPlanForQuery { .. } | R::SqlUnsupported { .. } => {
            RefuseCause::TierVocab { step: StepKind::FamilyRule }
        }
        R::PredUnsupported { .. } => RefuseCause::TierVocab { step: StepKind::Pred },
        R::InListWidth { .. } => RefuseCause::TierVocab { step: StepKind::InList },
        R::ConstUnparsable { .. } => RefuseCause::TierVocab { step: StepKind::Const },
        R::AggUnsupported { .. } => RefuseCause::TierVocab { step: StepKind::Agg },
        R::HavingUnsupported => RefuseCause::TierVocab { step: StepKind::Having },
        R::MetadataPredNotStatsAnswerable => {
            RefuseCause::TierVocab { step: StepKind::StatsAnswer }
        }
        R::FamilyUnregistered { .. } => {
            RefuseCause::TierVocab { step: StepKind::FamilyRegistry }
        }
        R::NoFamilyRule { .. } => RefuseCause::TierVocab { step: StepKind::FamilyRule },
        R::CollationUnsupported { collation, .. } => {
            RefuseCause::NonCCollation { coll_oid: *collation }
        }
        R::TypeUnsupported { oid, .. } => RefuseCause::UnsupportedType { type_oid: *oid },
        R::NullableDict { .. } => RefuseCause::NullableColumn { dict: true },
        R::NullableUnsupported { .. } => RefuseCause::NullableColumn { dict: false },
        R::FaceUnsupported { what, .. } => RefuseCause::UnsupportedFace { what },
        R::GroupServeUnsupported { .. } => RefuseCause::TierVocab { step: StepKind::Group },
        R::FamilyUnservedServer { .. } => {
            RefuseCause::TierVocab { step: StepKind::FamilyServe }
        }
        R::GroupCountUnwitnessed { .. } => {
            RefuseCause::TierVocab { step: StepKind::GroupCountWitness }
        }
        R::ScanRowsUnwitnessed { .. } => {
            RefuseCause::TierVocab { step: StepKind::ScanRowsWitness }
        }
        R::SortAggUnsupported { .. } => RefuseCause::TierVocab { step: StepKind::SortAgg },
        R::SortAggBytesUnwitnessed { .. } => {
            RefuseCause::TierVocab { step: StepKind::SortAggBytesWitness }
        }
        R::SortAggOverBudget { .. } => {
            RefuseCause::TierVocab { step: StepKind::SortAggBudget }
        }
        R::WinUnsupported { .. } => RefuseCause::TierVocab { step: StepKind::WinAgg },
        R::WinBytesUnwitnessed { .. } => {
            RefuseCause::TierVocab { step: StepKind::WinAggBytesWitness }
        }
        R::WinOverBudget { .. } => {
            RefuseCause::TierVocab { step: StepKind::WinAggBudget }
        }
        // [P6-1 spill] over the E17 budget with no spill arm for the
        // elected grouped shape: 53400 (raising the budget is a
        // legitimate remedy — the resource lattice, not 0A000).
        R::GroupedSpillUnavailable { .. } => {
            RefuseCause::SpillUnavailable { family: FamilyKind::HashGroup }
        }
        // [cap-retire] the finalize answer-bytes law (RULED 2026-08-19):
        // the exact-counted grouped answer plane over the E17 answer-face
        // budget — 53400 (raising the budget is a legitimate remedy).
        R::GroupAnswerOverBudget { .. } => RefuseCause::AnswerBytes { what: "group-emit" },
        // [scan-cap-retire] the scan answer-face law (the q19 unrefusal):
        // an unwitnessed unbounded scan counted its TRUE survivors past
        // the answer-row cap — 53400-class (raising the cap is a
        // legitimate remedy), same lattice as the grouped answer plane.
        R::ScanAnswerOverCap { .. } => RefuseCause::AnswerBytes { what: "scan-emit" },
        // [spill-2] a failed spill temp-file event mid-statement: the
        // typed runtime I/O error (the RunRefusal transport; the OS
        // detail rides the engine-side Display into the server log).
        R::SpillIo { op, .. } => RefuseCause::SpillIo { op },
    }
}

/// Map an engine-side join refusal (`sqe::joins::JoinRefuse`) onto the
/// shell lattice (the phase-2 merge point).
pub fn from_join(r: &sqe::joins::JoinRefuse) -> RefuseCause {
    use sqe::joins::JoinRefuse as J;
    if std::env::var_os("PGRUST_SQE_TRACE").is_some() {
        eprintln!("SQETRACE|from_join|{r}");
    }
    match r {
        J::BuildExceedsBudget { .. }
        | J::ProductExceedsBudget { .. }
        | J::DistinctExceedsBudget { .. } => {
            RefuseCause::MemoryBudget { family: FamilyKind::Join }
        }
        J::Unsupported { .. } => RefuseCause::TierVocab { step: StepKind::Join },
        J::Collation { collation, .. } => RefuseCause::NonCCollation { coll_oid: *collation },
        J::Face { what, .. } => RefuseCause::UnsupportedFace { what: *what },
    }
}

// ---------------------------------------------------------------------------
// The witnessed refusal
// ---------------------------------------------------------------------------

/// Zero-sized private witness: non-constructible outside this module, so
/// `Refusal` literals cannot exist and `refuse()` is the one gate.
pub struct Witnessed(());

/// A witnessed (census-ticked) refusal. Consumed by `into_error` (the
/// one exit to the user) or read by `cause()` (EXPLAIN — the tick
/// already happened at `refuse()`).
#[must_use]
pub struct Refusal {
    cause: RefuseCause,
    source_fp: u64,
    _witnessed: Witnessed,
}

impl Refusal {
    pub fn cause(&self) -> RefuseCause {
        self.cause
    }

    /// The R1 typed ERROR (design §4): stable greppable message with the
    /// census key as the load-bearing token; DETAIL carries the fnv1a-64
    /// statement fingerprint so a census row and an error report join;
    /// HINT names the roadmap.
    pub fn into_error(self, relname: &str) -> Box<PgError> {
        let kind = if self.cause.family() == "heap" { "heap" } else { "columnar" };
        let key = self.cause.census_key();
        let sqlstate = self.cause.sqlstate();
        Box::new(
            PgError::error(format!(
                "sqe: statement over {kind} relation \"{relname}\" is not served \
                 (cause: {key})"
            ))
            .with_sqlstate(sqlstate)
            .with_detail(format!(
                "The sqe engine serves {kind} shapes with no fallback; an unserved \
                 shape fails loudly instead of running slowly (cause \"{key}\", \
                 fp={:016x}).",
                self.source_fp
            ))
            .with_hint(
                "Coverage grows by refusal-census frequency; see pg_stat_sqe_refusals \
                 (scripts/sqe-stat-views.sql) and the coverage matrix.",
            ),
        )
    }
}

/// fnv1a-64 of the statement source (the census/error join key —
/// lanev4 lx4seam.rs:980-987, carried verbatim).
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registry invariants (design §6): keys unique, kebab-grammar,
    /// family/sqlstate total.
    #[test]
    fn census_keys_are_unique_and_well_formed() {
        let all = RefuseCause::all_for_test();
        let mut keys: Vec<String> = all.iter().map(|c| c.census_key()).collect();
        let n = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), n, "duplicate census keys");
        for k in &keys {
            assert!(
                k.split('/').count() >= 1 && k.split('/').count() <= 3,
                "key depth: {k}"
            );
            assert!(
                k.chars().all(|c| c.is_ascii_lowercase()
                    || c.is_ascii_digit()
                    || c == '-'
                    || c == '/'),
                "key grammar: {k}"
            );
        }
        for c in &all {
            let _ = c.sqlstate();
            let _ = c.family();
            let _ = c.variant_key();
        }
    }

    /// Rule D witness: refusing ticks the census; the error carries the
    /// key and the fingerprint.
    #[test]
    fn refuse_ticks_and_error_carries_the_key() {
        let fp = fnv1a64(b"SELECT test");
        let before = crate::sqeshell::stat::refusal_count("run/epq");
        let r = RefuseCause::EpqRecheck.refuse(fp);
        assert_eq!(
            crate::sqeshell::stat::refusal_count("run/epq"),
            before + 1,
            "refuse() must tick"
        );
        let e = r.into_error("hits");
        assert!(e.message().contains("cause: run/epq"), "{}", e.message());
        assert_eq!(e.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    /// The resource family's 53400 divergence is deliberate.
    #[test]
    fn resource_family_raises_53400() {
        assert_eq!(
            RefuseCause::MemoryBudget { family: FamilyKind::HashGroup }.sqlstate(),
            types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
        );
        assert_eq!(
            RefuseCause::TierVocab { step: StepKind::SortAggBudget }.sqlstate(),
            types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            "over-budget is a resource cause"
        );
        assert_eq!(
            RefuseCause::TierVocab { step: StepKind::WinAggBudget }.sqlstate(),
            types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            "over-budget is a resource cause"
        );
        assert_eq!(
            RefuseCause::AnswerBytes { what: "cursor-spool" }.sqlstate(),
            types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
            "the answer-bytes law is a budget: raising it is a legitimate remedy"
        );
        assert_eq!(
            RefuseCause::ScrollableCursor.sqlstate(),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "scrollable-cursor is a capability gap, not a limit"
        );
    }

    #[test]
    fn seed_lever_rejects_unknown_keys() {
        assert!(RefuseCause::from_seed_key("node").is_some());
        assert!(RefuseCause::from_seed_key("no-such-cause").is_none());
    }
}
