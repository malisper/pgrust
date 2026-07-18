//! Production registry + k-path grammar coverage (H5 rung A).
//!
//! The generator is typed/procedural, not a CFG, so the k-path metric
//! (Havrikov & Zeller, *Systematically Covering Input Structure*, ASE 2019;
//! journal ext. ACM TOSEM 2022) is adapted honestly: the "grammar graph" is
//! defined from the generator's OWN production structure — statement-kind
//! choices, per-kind variant choices, and sub-variant choices (scalar-call
//! arms, SRF element types, join-qual alternatives, BEGIN isolation levels,
//! oracle properties). Every node the generator can emit is declared HERE, in
//! one static table (+ the oracle-property nodes derived from
//! `props::v1_set()` at registry build, so a new property self-registers).
//!
//! Anti-staleness discipline (the "metric can't silently go stale" charter
//! line): the emission sites in `gen::noise` index compile-time-sized arrays
//! of these node names (`[&str; N]` tied to the weight-array length), so
//! adding a generator variant without a registry name is a compile error at
//! the emission site; and the registry-lock unit tests assert (a) every
//! traced name over a large multi-profile corpus is registered and (b) every
//! default-reachable registered name is actually emitted by such a corpus.
//!
//! k-path semantics (verified traps carried from the research doc,
//! docs/research/fuzzer-exploration-2026-07-18.md):
//!   * NO subsumption across k (ASE'19 thesis §2.5.3) — k=1, k=2, k=3 are
//!     computed and reported JOINTLY, never only the largest.
//!   * epsilon-class productions (alternatives that emit no token, e.g. the
//!     "no WHERE qual" branch of the LEFT-join variant) cannot appear in
//!     traces — they are declared with `epsilon: true` and EXCLUDED from
//!     every denominator explicitly, and listed in the report.
//!   * k=1 here is symbol coverage (Def. 3, ASE'19): it does NOT subsume
//!     rule coverage; our nodes are generator DECISIONS, which for this
//!     generator coincide with its production applications.
//!
//! THE REACH GATE: any production whose gate evaluates reachable-by-default
//! under the campaign profile that never appears at k=1 makes the campaign
//! verdict RED (`reach-gap`, a harness-mechanics class distinct from bug
//! findings). This is the check that would have caught H3's 0/9 before any
//! seed ran: joins/functions/SRFs sat at 0% emission — a k=1 zero.

use std::collections::{BTreeMap, BTreeSet};

use crate::gen::profile::GenProfile;

// ---------------------------------------------------------------------------
// Node names (referenced from the emission sites in gen::noise / generator)
// ---------------------------------------------------------------------------

// statement kinds (children of the plan root)
pub const STMT_DDL: &str = "stmt:ddl";
pub const STMT_DML: &str = "stmt:dml";
pub const STMT_QUERY: &str = "stmt:query";
pub const STMT_TX: &str = "stmt:tx";
pub const STMT_ARM: &str = "stmt:arm";
pub const STMT_FAULT: &str = "stmt:fault";
pub const STMT_PROPERTY: &str = "stmt:property";

// ddl variants
pub const DDL_CREATE_TABLE: &str = "ddl:create-table";
pub const DDL_CREATE_INDEX: &str = "ddl:create-index";
pub const DDL_RENAME_TABLE: &str = "ddl:rename-table";
pub const DDL_DROP_TABLE: &str = "ddl:drop-table";
// H6 state arm: index-shape diversity + statistics + foreign-data state ops.
// "State op" = a DDL step whose point is to change what plans the planner can
// pick for LATER queries (an index, fresh statistics, a foreign table), not
// to be interesting by itself.
pub const DDL_CREATE_INDEX_MULTI: &str = "ddl:create-index-multi";
pub const DDL_CREATE_INDEX_EXPR: &str = "ddl:create-index-expr";
pub const DDL_CREATE_INDEX_PARTIAL: &str = "ddl:create-index-partial";
pub const DDL_CREATE_INDEX_BRIN: &str = "ddl:create-index-brin";
pub const DDL_DROP_INDEX: &str = "ddl:drop-index";
pub const DDL_ANALYZE: &str = "ddl:analyze";
/// BitmapAnd substrate chain: two single-column indexes on one table +
/// ANALYZE emitted as one decision (first statement traces this node; the
/// second and third trace ddl:create-index / ddl:analyze).
pub const DDL_INDEX_PAIR: &str = "ddl:index-pair";
// file_fdw foreign-table setup chain. One generator decision emits the whole
// chain as consecutive DDL steps (extension -> server -> csv COPY -> foreign
// table); each emitted statement gets its own trace node so statement/trace
// alignment holds. Later picks in the same plan skip the stages the session
// already has (IF NOT EXISTS keeps the SQL idempotent across seeds).
pub const DDL_FDW_EXTENSION: &str = "ddl:fdw-extension";
pub const DDL_FDW_SERVER: &str = "ddl:fdw-server";
pub const DDL_FDW_COPY: &str = "ddl:fdw-copy";
pub const DDL_FDW_TABLE: &str = "ddl:fdw-table";

// dml variants
pub const DML_INSERT: &str = "dml:insert";
pub const DML_UPDATE: &str = "dml:update";
pub const DML_DELETE: &str = "dml:delete";
pub const DML_TRUNCATE: &str = "dml:truncate";
/// H6 state arm: set-based INSERT ... SELECT over generate_series — the
/// cardinality lever (tiny/medium/larger tables make the planner cross its
/// seq-scan-vs-index cost thresholds).
pub const DML_BULK_INSERT: &str = "dml:bulk-insert";

// query variants (gen_query)
pub const Q_FULL_ORDERED: &str = "q:full-ordered";
pub const Q_COUNT_STAR: &str = "q:count-star";
pub const Q_EXACT_AGG: &str = "q:exact-agg";
pub const Q_FILTERED: &str = "q:filtered";
pub const Q_TOPK_LIMIT: &str = "q:topk-limit";
pub const Q_LIMIT_OFFSET: &str = "q:limit-offset";
pub const Q_GROUP_COUNT: &str = "q:group-count";
pub const Q_FLOAT_AGG: &str = "q:float-agg";
pub const Q_SRF_UNNEST: &str = "q:srf-unnest";
pub const Q_GENERATE_SERIES: &str = "q:generate-series";
pub const Q_SCALAR_CALL: &str = "q:scalar-call";
pub const Q_INNER_JOIN: &str = "q:inner-join";
pub const Q_LEFT_JOIN_COALESCE: &str = "q:left-join-coalesce";
pub const Q_OJ_NEST_COALESCE: &str = "q:oj-nest-coalesce";
// H6 state arm: query shapes that pay off once the state ops above ran.
/// ORDER BY <payload-col>, id — with an index on the payload column this is
/// the Incremental Sort elicitation shape (index gives the prefix order).
pub const Q_ORDER_PREFIX: &str = "q:order-prefix";
/// WHERE a = k1 AND b = k2 — with two single-column indexes + ANALYZE'd
/// stats this is the BitmapAnd elicitation shape.
pub const Q_TWO_COL_EQ: &str = "q:two-col-eq";
/// SELECT <col> ... WHERE <col> ... — projection covered by a single-column
/// index: the Index Only Scan elicitation shape.
pub const Q_COVERED_SELECT: &str = "q:covered-select";
/// SELECT over a file_fdw foreign table — the Foreign Scan elicitation shape.
pub const Q_FOREIGN_SCAN: &str = "q:foreign-scan";

// srf-unnest element types
pub const SRF_INT: &str = "srf:int";
pub const SRF_TEXT: &str = "srf:text";
pub const SRF_NUMERIC: &str = "srf:numeric";

// scalar-call arms
pub const SC_INT_ABS: &str = "sc:int-abs";
pub const SC_INT_MOD: &str = "sc:int-mod";
pub const SC_INT_COALESCE: &str = "sc:int-coalesce";
pub const SC_INT_NULLIF: &str = "sc:int-nullif";
pub const SC_TEXT_LENGTH: &str = "sc:text-length";
pub const SC_TEXT_UPPER: &str = "sc:text-upper";
pub const SC_TEXT_COALESCE: &str = "sc:text-coalesce";
pub const SC_NUMERIC_ABS: &str = "sc:numeric-abs";
pub const SC_NUMERIC_COALESCE: &str = "sc:numeric-coalesce";
pub const SC_FALLBACK_ABS_ID: &str = "sc:fallback-abs-id";

// left-join qual alternatives
pub const LJC_QUAL_COALESCE: &str = "ljc:qual-coalesce";
/// epsilon-class: the "no qual" alternative emits no token — it can never
/// appear in a trace and is excluded from every denominator (see module doc).
pub const LJC_NO_QUAL: &str = "ljc:no-qual";

// tx variants
pub const TX_BEGIN: &str = "tx:begin";
pub const TX_COMMIT: &str = "tx:commit";
pub const TX_ROLLBACK: &str = "tx:rollback";
pub const TX_SAVEPOINT: &str = "tx:savepoint";
pub const TX_ROLLBACK_TO: &str = "tx:rollback-to";

// BEGIN isolation levels
pub const ISO_RC: &str = "iso:read-committed";
pub const ISO_RR: &str = "iso:repeatable-read";
pub const ISO_SER: &str = "iso:serializable";

// arm variants
pub const ARM_APPLY_SET: &str = "arm:apply-set";
pub const ARM_RESET_ALL: &str = "arm:reset-all";

// fault variants (Disconnect+ReconnectServer emitted as one paired decision)
pub const FAULT_DISCONNECT_PAIR: &str = "fault:disconnect-pair";

/// Prefix for dynamically-registered oracle-property nodes: `prop:<name>`.
pub const PROP_PREFIX: &str = "prop:";

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Reachability gate, evaluated against the campaign's `GenProfile`. A node
/// whose gate is false is "gated-unreachable": excluded from denominators
/// (its absence is structural, not a reach gap) but LISTED with its reason —
/// a gated node the profile INTENDS to cover is itself a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    Always,
    WDdl,
    WDml,
    WQuery,
    WTx,
    WArm,
    WFault,
    WProperty,
    IsoRc,
    IsoRr,
    IsoSer,
    /// float aggregate: needs float_lenient AND float8 columns generatable.
    FloatAgg,
    /// scalar-call fallback fires only when a table can have zero columns.
    ColsCanBeEmpty,
    /// apply-set needs at least one NON-EMPTY arm set in the profile.
    NonEmptyArmSet,
    /// needs a table that can have >= 2 payload columns (two-col shapes).
    TwoPayloadCols,
    /// DDL-side two-column requirement (the index-pair chain).
    DdlTwoCols,
    /// foreign-table read: needs both query and ddl weighted (the foreign
    /// table only exists after the DDL-side fdw setup chain ran).
    ForeignScan,
}

#[derive(Debug, Clone)]
pub struct ProdDef {
    pub name: String,
    /// Parent context node; None = child of the (implicit) plan root.
    pub parent: Option<String>,
    pub gate: Gate,
    pub epsilon: bool,
}

const S: fn(&str) -> String = str::to_string;

fn def(name: &str, parent: Option<&str>, gate: Gate, epsilon: bool) -> ProdDef {
    ProdDef { name: S(name), parent: parent.map(S), gate, epsilon }
}

/// Build the full production registry for a campaign: the static generator
/// productions + one `prop:<name>` node per oracle property (self-registering
/// — a new property in `props::v1_set()` appears here automatically).
pub fn registry(property_names: &[&str]) -> Vec<ProdDef> {
    use Gate::*;
    let mut v = vec![
        // statement kinds. stmt:ddl is Always: the first plan item is always
        // CREATE TABLE regardless of the ddl weight (generator invariant).
        def(STMT_DDL, None, Always, false),
        def(STMT_DML, None, WDml, false),
        def(STMT_QUERY, None, WQuery, false),
        def(STMT_TX, None, WTx, false),
        def(STMT_ARM, None, WArm, false),
        def(STMT_FAULT, None, WFault, false),
        def(STMT_PROPERTY, None, WProperty, false),
        // ddl. create-table is Always (first-item rule); the others need the
        // ddl kind weighted.
        def(DDL_CREATE_TABLE, Some(STMT_DDL), Always, false),
        def(DDL_CREATE_INDEX, Some(STMT_DDL), WDdl, false),
        def(DDL_RENAME_TABLE, Some(STMT_DDL), WDdl, false),
        def(DDL_DROP_TABLE, Some(STMT_DDL), WDdl, false),
        // H6 state ops
        def(DDL_CREATE_INDEX_MULTI, Some(STMT_DDL), WDdl, false),
        def(DDL_CREATE_INDEX_EXPR, Some(STMT_DDL), WDdl, false),
        def(DDL_CREATE_INDEX_PARTIAL, Some(STMT_DDL), WDdl, false),
        def(DDL_CREATE_INDEX_BRIN, Some(STMT_DDL), WDdl, false),
        def(DDL_DROP_INDEX, Some(STMT_DDL), WDdl, false),
        def(DDL_ANALYZE, Some(STMT_DDL), WDdl, false),
        def(DDL_INDEX_PAIR, Some(STMT_DDL), DdlTwoCols, false),
        def(DDL_FDW_EXTENSION, Some(STMT_DDL), WDdl, false),
        def(DDL_FDW_SERVER, Some(STMT_DDL), WDdl, false),
        def(DDL_FDW_COPY, Some(STMT_DDL), WDdl, false),
        def(DDL_FDW_TABLE, Some(STMT_DDL), WDdl, false),
        // dml
        def(DML_INSERT, Some(STMT_DML), WDml, false),
        def(DML_UPDATE, Some(STMT_DML), WDml, false),
        def(DML_DELETE, Some(STMT_DML), WDml, false),
        def(DML_TRUNCATE, Some(STMT_DML), WDml, false),
        def(DML_BULK_INSERT, Some(STMT_DML), WDml, false),
        // query variants
        def(Q_FULL_ORDERED, Some(STMT_QUERY), WQuery, false),
        def(Q_COUNT_STAR, Some(STMT_QUERY), WQuery, false),
        def(Q_EXACT_AGG, Some(STMT_QUERY), WQuery, false),
        def(Q_FILTERED, Some(STMT_QUERY), WQuery, false),
        def(Q_TOPK_LIMIT, Some(STMT_QUERY), WQuery, false),
        def(Q_LIMIT_OFFSET, Some(STMT_QUERY), WQuery, false),
        def(Q_GROUP_COUNT, Some(STMT_QUERY), WQuery, false),
        def(Q_FLOAT_AGG, Some(STMT_QUERY), FloatAgg, false),
        def(Q_SRF_UNNEST, Some(STMT_QUERY), WQuery, false),
        def(Q_GENERATE_SERIES, Some(STMT_QUERY), WQuery, false),
        def(Q_SCALAR_CALL, Some(STMT_QUERY), WQuery, false),
        def(Q_INNER_JOIN, Some(STMT_QUERY), WQuery, false),
        def(Q_LEFT_JOIN_COALESCE, Some(STMT_QUERY), WQuery, false),
        def(Q_OJ_NEST_COALESCE, Some(STMT_QUERY), WQuery, false),
        // H6 state-arm query shapes
        def(Q_ORDER_PREFIX, Some(STMT_QUERY), WQuery, false),
        def(Q_TWO_COL_EQ, Some(STMT_QUERY), TwoPayloadCols, false),
        def(Q_COVERED_SELECT, Some(STMT_QUERY), WQuery, false),
        def(Q_FOREIGN_SCAN, Some(STMT_QUERY), ForeignScan, false),
        // srf element types
        def(SRF_INT, Some(Q_SRF_UNNEST), WQuery, false),
        def(SRF_TEXT, Some(Q_SRF_UNNEST), WQuery, false),
        def(SRF_NUMERIC, Some(Q_SRF_UNNEST), WQuery, false),
        // scalar-call arms
        def(SC_INT_ABS, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_INT_MOD, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_INT_COALESCE, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_INT_NULLIF, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_TEXT_LENGTH, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_TEXT_UPPER, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_TEXT_COALESCE, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_NUMERIC_ABS, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_NUMERIC_COALESCE, Some(Q_SCALAR_CALL), WQuery, false),
        def(SC_FALLBACK_ABS_ID, Some(Q_SCALAR_CALL), ColsCanBeEmpty, false),
        // left-join qual alternatives
        def(LJC_QUAL_COALESCE, Some(Q_LEFT_JOIN_COALESCE), WQuery, false),
        def(LJC_NO_QUAL, Some(Q_LEFT_JOIN_COALESCE), WQuery, true),
        // tx
        def(TX_BEGIN, Some(STMT_TX), WTx, false),
        def(TX_COMMIT, Some(STMT_TX), WTx, false),
        def(TX_ROLLBACK, Some(STMT_TX), WTx, false),
        def(TX_SAVEPOINT, Some(STMT_TX), WTx, false),
        def(TX_ROLLBACK_TO, Some(STMT_TX), WTx, false),
        def(ISO_RC, Some(TX_BEGIN), IsoRc, false),
        def(ISO_RR, Some(TX_BEGIN), IsoRr, false),
        def(ISO_SER, Some(TX_BEGIN), IsoSer, false),
        // arm
        def(ARM_APPLY_SET, Some(STMT_ARM), NonEmptyArmSet, false),
        def(ARM_RESET_ALL, Some(STMT_ARM), WArm, false),
        // fault
        def(FAULT_DISCONNECT_PAIR, Some(STMT_FAULT), WFault, false),
    ];
    for p in property_names {
        v.push(ProdDef {
            name: format!("{PROP_PREFIX}{p}"),
            parent: Some(S(STMT_PROPERTY)),
            gate: Gate::WProperty,
            epsilon: false,
        });
    }
    v
}

/// Gate evaluation. Returns Some(reason) when the node is NOT reachable by
/// default under this profile (None = reachable).
pub fn gate_reason(def_: &ProdDef, p: &GenProfile) -> Option<String> {
    let w = &p.statement_weights;
    let closed = |cond: bool, why: &str| if cond { None } else { Some(why.to_string()) };
    // NOTE: `p.test_disable_productions` is deliberately NOT consulted here.
    // The knob suppresses EMISSION only, while the gate keeps considering the
    // production reachable — exactly simulating "the generator silently lost
    // a production" (the H3 0/9 shape), which is what the reach-gate teeth
    // test asserts goes RED.
    match def_.gate {
        Gate::Always => None,
        Gate::WDdl => closed(w.ddl > 0, "statement_weights.ddl=0"),
        Gate::WDml => closed(w.dml > 0, "statement_weights.dml=0"),
        Gate::WQuery => closed(w.query > 0, "statement_weights.query=0"),
        Gate::WTx => closed(w.tx > 0, "statement_weights.tx=0"),
        Gate::WArm => closed(
            w.arm > 0 && !p.arm_sets.is_empty(),
            "statement_weights.arm=0 or arm_sets empty",
        ),
        Gate::WFault => {
            if w.fault == 0 {
                return Some("statement_weights.fault=0".into());
            }
            // Disconnect+Reconnect emit as a PAIR: the generator requires a
            // fault budget of >= 2. Reachability is decided by the REAL
            // largest-remainder allocation, simulated over every plan length
            // the profile allows (H5 find: fault weights 1-2 in the battery
            // profiles never allocate 2 at ANY length — the production was
            // silently dead; the honest gate says so instead of minting a
            // false reach-gap RED or, worse, staying silent).
            let reachable = (p.plan_len.min..=p.plan_len.max).any(|total| {
                crate::gen::budget::Budgets::allocate(&p.statement_weights, total, true)
                    .remaining(crate::gen::budget::Kind::Fault)
                    >= 2
            });
            closed(
                reachable,
                "fault budget never reaches 2 (the disconnect pair) at any plan length \
                 under largest-remainder allocation — raise the fault weight",
            )
        }
        Gate::WProperty => {
            if w.property == 0 {
                return Some("statement_weights.property=0".into());
            }
            if let Some(name) = def_.name.strip_prefix(PROP_PREFIX) {
                let pw = p.property_weights.get(name).copied().unwrap_or(1);
                return closed(pw > 0, &format!("property weight 0 ({name})"));
            }
            None
        }
        Gate::IsoRc => closed(w.tx > 0 && p.iso_mix.rc > 0, "iso_mix.rc=0 or tx=0"),
        Gate::IsoRr => closed(w.tx > 0 && p.iso_mix.rr > 0, "iso_mix.rr=0 or tx=0"),
        Gate::IsoSer => closed(w.tx > 0 && p.iso_mix.ser > 0, "iso_mix.ser=0 or tx=0"),
        Gate::FloatAgg => closed(
            w.query > 0 && p.float_lenient && p.table_shape.col_types.float8 > 0,
            "needs float_lenient AND col_types.float8>0 (float8 columns generatable)",
        ),
        Gate::ColsCanBeEmpty => closed(
            w.query > 0 && p.table_shape.min_cols == 0,
            "fires only when a table can have zero payload columns (min_cols=0)",
        ),
        Gate::NonEmptyArmSet => closed(
            w.arm > 0 && p.arm_sets.iter().any(|s| !s.is_empty()),
            "no non-empty arm set in profile",
        ),
        Gate::TwoPayloadCols => closed(
            w.query > 0 && p.table_shape.max_cols >= 2,
            "needs a table with >= 2 payload columns (table_shape.max_cols < 2)",
        ),
        Gate::DdlTwoCols => closed(
            w.ddl > 0 && p.table_shape.max_cols >= 2,
            "needs ddl weighted AND a table with >= 2 payload columns",
        ),
        Gate::ForeignScan => closed(
            w.query > 0 && w.ddl > 0,
            "needs statement_weights.query>0 AND ddl>0 (foreign table comes from \
             the DDL-side fdw setup chain)",
        ),
    }
}

// ---------------------------------------------------------------------------
// Trace aggregation -> k=1..3 coverage + the reach gate
// ---------------------------------------------------------------------------

/// One statement's production path, root-most first (e.g.
/// `["stmt:query", "q:scalar-call", "sc:int-abs"]`).
pub type ProdPath = Vec<String>;

/// Per-plan generation trace: the paths of every emitted statement, plus the
/// applied arm-set indexes (arm sets are PROFILE data, not grammar structure,
/// so their coverage is reported supplementarily, outside the k-path math).
#[derive(Debug, Clone, Default)]
pub struct GenTraces {
    pub paths: Vec<ProdPath>,
    pub arm_set_hits: Vec<usize>,
}

/// Accumulated k-path observations over a campaign.
#[derive(Debug, Clone, Default)]
pub struct KpathAccum {
    pub nodes: BTreeSet<String>,
    pub pairs: BTreeSet<(String, String)>,
    pub triples: BTreeSet<(String, String, String)>,
    pub statements: u64,
    pub arm_set_hits: BTreeMap<usize, u64>,
}

impl KpathAccum {
    pub fn add(&mut self, t: &GenTraces) {
        for path in &t.paths {
            self.statements += 1;
            for n in path {
                self.nodes.insert(n.clone());
            }
            for w in path.windows(2) {
                self.pairs.insert((w[0].clone(), w[1].clone()));
            }
            for w in path.windows(3) {
                self.triples.insert((w[0].clone(), w[1].clone(), w[2].clone()));
            }
        }
        for i in &t.arm_set_hits {
            *self.arm_set_hits.entry(*i).or_insert(0) += 1;
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct KLevel {
    pub covered: usize,
    pub total: usize,
    /// covered/total; 1.0 when total = 0.
    pub fraction: f64,
    pub uncovered: Vec<String>,
}

fn level(covered: usize, total: usize, uncovered: Vec<String>) -> KLevel {
    KLevel {
        covered,
        total,
        fraction: if total == 0 { 1.0 } else { covered as f64 / total as f64 },
        uncovered,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct KpathReport {
    /// k=1 / k=2 / k=3 reported JOINTLY (no-subsumption trap, ASE'19).
    pub k1: KLevel,
    pub k2: KLevel,
    pub k3: KLevel,
    /// THE REACH GATE: k=1-uncovered productions that the gate says are
    /// reachable-by-default. Non-empty => campaign verdict RED (`reach-gap`).
    pub reach_gap: Vec<String>,
    /// Gated-unreachable nodes with reasons (excluded from denominators;
    /// listed loudly — a gated node the profile intends is itself a finding).
    pub gated_unreachable: Vec<(String, String)>,
    /// epsilon-class productions (cannot appear in traces; excluded).
    pub epsilon_excluded: Vec<String>,
    pub statements: u64,
    pub arm_set_hits: BTreeMap<usize, u64>,
}

/// Evaluate the campaign accumulator against the registry under `profile`.
///
/// Denominators are enumerated programmatically from the registry graph:
/// k=1 = reachable non-epsilon nodes; k=2 = parent→child edges between such
/// nodes; k=3 = grandparent→parent→child chains. A chain is in-denominator
/// iff every node on it is reachable and non-epsilon.
pub fn evaluate(
    accum: &KpathAccum,
    registry: &[ProdDef],
    profile: &GenProfile,
) -> KpathReport {
    let mut reachable: BTreeMap<&str, bool> = BTreeMap::new();
    let mut gated = Vec::new();
    let mut epsilon = Vec::new();
    let by_name: BTreeMap<&str, &ProdDef> =
        registry.iter().map(|d| (d.name.as_str(), d)).collect();
    for d in registry {
        if d.epsilon {
            epsilon.push(d.name.clone());
            reachable.insert(&d.name, false);
            continue;
        }
        // A node is reachable only if its own gate AND its ancestors' gates
        // hold (child gates are written to imply parents here, but keep the
        // chain check so future entries can't silently widen).
        let mut ok = gate_reason(d, profile).is_none();
        let mut cur = d;
        while ok {
            match &cur.parent {
                None => break,
                Some(p) => {
                    let pd = by_name.get(p.as_str()).unwrap_or_else(|| {
                        panic!("registry: node '{}' has unregistered parent '{}'", d.name, p)
                    });
                    if pd.epsilon || gate_reason(pd, profile).is_some() {
                        ok = false;
                    }
                    cur = pd;
                }
            }
        }
        if !ok && !d.epsilon {
            if let Some(reason) = gate_reason(d, profile) {
                gated.push((d.name.clone(), reason));
            } else {
                gated.push((d.name.clone(), "ancestor gated".into()));
            }
        }
        reachable.insert(&d.name, ok);
    }

    // k=1 denominator + uncovered.
    let k1_all: Vec<&ProdDef> =
        registry.iter().filter(|d| *reachable.get(d.name.as_str()).unwrap()).collect();
    let k1_uncov: Vec<String> = k1_all
        .iter()
        .filter(|d| !accum.nodes.contains(&d.name))
        .map(|d| d.name.clone())
        .collect();
    let k1 = level(k1_all.len() - k1_uncov.len(), k1_all.len(), k1_uncov.clone());

    // k=2: parent->child edges among reachable nodes.
    let mut edges: Vec<(String, String)> = Vec::new();
    for d in &k1_all {
        if let Some(p) = &d.parent {
            if *reachable.get(p.as_str()).unwrap_or(&false) {
                edges.push((p.clone(), d.name.clone()));
            }
        }
    }
    let k2_uncov: Vec<String> = edges
        .iter()
        .filter(|e| !accum.pairs.contains(*e))
        .map(|(a, b)| format!("{a} > {b}"))
        .collect();
    let k2 = level(edges.len() - k2_uncov.len(), edges.len(), k2_uncov);

    // k=3: grandparent->parent->child chains among reachable nodes.
    let mut chains: Vec<(String, String, String)> = Vec::new();
    for d in &k1_all {
        if let Some(p) = &d.parent {
            if let Some(pd) = by_name.get(p.as_str()) {
                if let Some(gp) = &pd.parent {
                    if *reachable.get(p.as_str()).unwrap_or(&false)
                        && *reachable.get(gp.as_str()).unwrap_or(&false)
                    {
                        chains.push((gp.clone(), p.clone(), d.name.clone()));
                    }
                }
            }
        }
    }
    let k3_uncov: Vec<String> = chains
        .iter()
        .filter(|c| !accum.triples.contains(*c))
        .map(|(a, b, c)| format!("{a} > {b} > {c}"))
        .collect();
    let k3 = level(chains.len() - k3_uncov.len(), chains.len(), k3_uncov);

    KpathReport {
        k1,
        k2,
        k3,
        reach_gap: k1_uncov,
        gated_unreachable: gated,
        epsilon_excluded: epsilon,
        statements: accum.statements,
        arm_set_hits: accum.arm_set_hits.clone(),
    }
}
