//! Plan-selection / optimizer alternate-arm drain module (LD7): the
//! `optimizer-arms` chunk of docs/fuzzing/line-drain-queue.tsv (522 rows /
//! 4,461 hollow lines — pathnode.c add_path dominance + reparameterize
//! arms, joinpath.c/joinrels.c join-strategy selection, allpaths.c,
//! indxpath.c OR/boolean/partial-index matching, equivclass.c outer/full
//! join clause reconsideration, planner.c grouping-path construction,
//! prepjointree.c pullups, analyzejoins.c join removal, predtest.c
//! partial-index proofs, createplan.c alternate node emission). The
//! default corpus lets the planner pick ONE cheapest path per query, so
//! every alternate plan-selection arm stays dark.
//!
//! Mechanism: generate ONE deterministic query, then emit it under a
//! SWEEP of GUC "plan profiles" (enable_* toggles, cost-model extremes,
//! geqo, collapse limits, partitionwise/parallel forcing), each profile a
//! SET/RESET bracket applied IDENTICALLY on both differential sides. The
//! result set of a deterministic query is a pure function of table state
//! — it must be identical across ALL forced plans and across both
//! engines. Any A/B divergence under any profile is a real
//! planner/executor correctness finding (HIGH if the result set itself
//! differs across plans).
//!
//! Determinism laws honored (same discipline as crate::spill / crate::par):
//!   - every row-returning statement carries a TOTAL order (ORDER BY
//!     ending in pk or the full projected key list); everything else is
//!     aggregate-only with exact-typed (int8/numeric/text-length)
//!     accumulation-order-independent aggregates. No float aggregates (B1).
//!   - ANALYZE determinism: tables stay <= 24000 rows (exhaustive under
//!     the default_statistics_target 30000-row sample) — identical stats,
//!     identical cost inputs on both sides.
//!   - every bracket SET has its RESET in the same statement group,
//!     RESETs in reverse order; the runner's GucPinned wrapper re-applies
//!     the C-parity pin after RESETs identically on both sides.
//!   - write shapes (UPDATE/DELETE/INSERT/MERGE plan arms) run inside
//!     BEGIN ... ROLLBACK brackets with SET LOCAL profiles, so persistent
//!     state never changes and state probes stay stable; RETURNING flows
//!     through an ordered CTE wrapper (RETURNING row order is
//!     plan-shaped, so it is never compared raw).
//!   - geqo profiles pin geqo_seed=0 (both engines): the GEQO join-order
//!     search is then a deterministic function of the query.
//!
//! Fixture set (one live at a time, `fz_ps_*_N`):
//!   fz_ps_b_N   "big" 10,000 rows: pk PRIMARY KEY, a int4 0..200 (btree),
//!               b int4 0..50 (btree), c int4 0..1000, flag bool, num
//!               numeric, txt text (btree), pad text. Composite (a,b)
//!               index, partial index ON (c) WHERE flag, partial index
//!               ON (b) WHERE a > 100 (predtest proof fuel), expression
//!               index ON (abs(c)).
//!   fz_ps_s_N   "small" 200 rows: pk PRIMARY KEY 1..200, d int4, txt
//!               text — pk-unique join target (join-removal fuel).
//!   fz_ps_p1_N / fz_ps_p2_N  RANGE-partitioned twins, IDENTICAL bounds
//!               ([0,2500) x4 up to 10000), 6,000 rows each, partitioned
//!               pkey + partitioned (a) index — partitionwise join/agg
//!               and child-reparameterization fuel.
//!   fz_ps_g_N   2,000 rows with GENERATED ALWAYS AS ... VIRTUAL and
//!               STORED columns (expand_virtual_generated_columns), btree
//!               on the stored column.
//!   fz_ps_srf_N(int4) RETURNS SETOF int4, fz_ps_tab_N(int4) RETURNS
//!               TABLE(i int4, j int4), fz_ps_const_N() RETURNS int4 —
//!               inlinable LANGUAGE sql functions
//!               (inline_set_returning_function /
//!               pull_up_constant_function fuel).
//! Every value is a pure integer formula of the row number — identical on
//! both sides by construction.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// One live fixture set at a time (a create group is ~35 statements with
/// three bulk loads).
const MAX_LIVE_SETS: usize = 1;

const BIG_ROWS: i64 = 10000;
const PART_ROWS: i64 = 6000;
const GEN_ROWS: i64 = 2000;
const SMALL_ROWS: i64 = 200;

// ------------------------------------------------------------- profiles ---

/// A named GUC plan profile: SET each pair (in order), RESET in reverse,
/// all inside the emitting statement group.
#[derive(Clone, Copy)]
pub struct Profile {
    pub name: &'static str,
    pub gucs: &'static [(&'static str, &'static str)],
}

const P_DEFAULT: Profile = Profile { name: "default", gucs: &[] };
const P_SEQ: Profile = Profile {
    name: "seqscan",
    gucs: &[
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
        ("enable_bitmapscan", "off"),
    ],
};
const P_IDX: Profile = Profile {
    name: "idxscan",
    gucs: &[("enable_seqscan", "off"), ("enable_bitmapscan", "off")],
};
const P_BITMAP: Profile = Profile {
    name: "bitmap",
    gucs: &[
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
    ],
};
const P_IOS: Profile = Profile {
    name: "ios",
    gucs: &[
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_bitmapscan", "off"),
    ],
};
const P_HASHJ: Profile = Profile {
    name: "hashjoin",
    gucs: &[("enable_mergejoin", "off"), ("enable_nestloop", "off")],
};
const P_MERGEJ: Profile = Profile {
    name: "mergejoin",
    gucs: &[("enable_hashjoin", "off"), ("enable_nestloop", "off")],
};
const P_MERGEJ_NOMAT: Profile = Profile {
    name: "mergejoin_nomat",
    gucs: &[
        ("enable_hashjoin", "off"),
        ("enable_nestloop", "off"),
        ("enable_material", "off"),
    ],
};
const P_NESTL: Profile = Profile {
    name: "nestloop",
    gucs: &[("enable_hashjoin", "off"), ("enable_mergejoin", "off")],
};
const P_NESTL_NOMEMO: Profile = Profile {
    name: "nestloop_nomemo",
    gucs: &[
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_memoize", "off"),
        ("enable_material", "off"),
    ],
};
const P_HASHAGG: Profile = Profile { name: "hashagg", gucs: &[("enable_sort", "off")] };
const P_GROUPAGG: Profile = Profile { name: "groupagg", gucs: &[("enable_hashagg", "off")] };
const P_NOINCSORT: Profile = Profile {
    name: "noincsort",
    gucs: &[("enable_incremental_sort", "off")],
};
const P_CHEAPIDX: Profile = Profile {
    name: "cheapidx",
    gucs: &[("random_page_cost", "0.1"), ("cpu_index_tuple_cost", "0.0001")],
};
const P_DEARIDX: Profile = Profile {
    name: "dearidx",
    gucs: &[("random_page_cost", "10000"), ("cpu_index_tuple_cost", "0.5")],
};
const P_CPUCOST: Profile = Profile {
    name: "cpucost",
    gucs: &[("cpu_operator_cost", "0.05"), ("cpu_tuple_cost", "0.9")],
};
const P_COLLAPSE1: Profile = Profile {
    name: "collapse1",
    gucs: &[("join_collapse_limit", "1"), ("from_collapse_limit", "1")],
};
const P_GEQO: Profile = Profile {
    name: "geqo",
    gucs: &[
        ("geqo", "on"),
        ("geqo_threshold", "2"),
        ("geqo_effort", "1"),
        ("geqo_seed", "0"),
    ],
};
const P_PWISE: Profile = Profile {
    name: "pwise",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
    ],
};
const P_PWISE_NEST: Profile = Profile {
    name: "pwise_nest",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_memoize", "off"),
        ("enable_material", "off"),
    ],
};
const P_PWISE_MERGE: Profile = Profile {
    name: "pwise_merge",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_nestloop", "off"),
    ],
};
const P_PARALLEL: Profile = Profile {
    name: "parallel",
    gucs: &[
        ("parallel_setup_cost", "0"),
        ("parallel_tuple_cost", "0"),
        ("min_parallel_table_scan_size", "0"),
        ("min_parallel_index_scan_size", "0"),
    ],
};
const P_PARALLEL_AGG: Profile = Profile {
    name: "parallel_agg",
    gucs: &[
        ("parallel_setup_cost", "0"),
        ("parallel_tuple_cost", "0"),
        ("min_parallel_table_scan_size", "0"),
        ("min_parallel_index_scan_size", "0"),
        ("enable_partitionwise_aggregate", "on"),
    ],
};

const SCAN_POOL: &[Profile] =
    &[P_SEQ, P_IDX, P_BITMAP, P_IOS, P_CHEAPIDX, P_DEARIDX, P_CPUCOST, P_PARALLEL, P_DEFAULT];
const JOIN_POOL: &[Profile] = &[
    P_HASHJ,
    P_MERGEJ,
    P_MERGEJ_NOMAT,
    P_NESTL,
    P_NESTL_NOMEMO,
    P_COLLAPSE1,
    P_GEQO,
    P_CHEAPIDX,
    P_PARALLEL,
    P_DEFAULT,
];
const PWISE_POOL: &[Profile] =
    &[P_PWISE, P_PWISE_NEST, P_PWISE_MERGE, P_PARALLEL_AGG, P_GEQO, P_DEFAULT];
const AGG_POOL: &[Profile] =
    &[P_HASHAGG, P_GROUPAGG, P_NOINCSORT, P_PARALLEL_AGG, P_CPUCOST, P_DEFAULT];
const SETOP_POOL: &[Profile] = &[P_HASHAGG, P_GROUPAGG, P_PARALLEL, P_DEFAULT];
const SUBQ_POOL: &[Profile] =
    &[P_HASHJ, P_MERGEJ, P_NESTL, P_NESTL_NOMEMO, P_CHEAPIDX, P_DEFAULT];
const PREP_POOL: &[Profile] = &[P_DEFAULT, P_SEQ, P_IDX, P_MERGEJ, P_COLLAPSE1];
const JOINRM_POOL: &[Profile] = &[P_DEFAULT, P_NESTL, P_HASHJ];
const DML_POOL: &[Profile] = &[P_DEFAULT, P_SEQ, P_IDX, P_BITMAP, P_NESTL, P_HASHJ];

// ---------------------------------------------------------------- state ---

#[derive(Clone, Debug)]
pub struct PlanSet {
    pub n: u32,
    pub live: bool,
}

/// Session-persistent fixture-set model (swapped in and out of `Gen` by
/// the session loop exactly like `SpillState`).
#[derive(Clone, Debug, Default)]
pub struct PlanState {
    pub sets: Vec<PlanSet>,
    next_set: u32,
    events: Vec<DdlEvent>,
}

impl PlanState {
    pub fn new() -> PlanState {
        PlanState::default()
    }

    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_sets(&self) -> Vec<usize> {
        self.sets
            .iter()
            .enumerate()
            .filter(|(_, s)| s.live)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Table / function names for set N.
struct Names {
    big: String,
    small: String,
    p1: String,
    p2: String,
    gen: String,
    srf: String,
    tab: String,
    cf: String,
}

fn names(n: u32) -> Names {
    Names {
        big: format!("fz_ps_b_{n}"),
        small: format!("fz_ps_s_{n}"),
        p1: format!("fz_ps_p1_{n}"),
        p2: format!("fz_ps_p2_{n}"),
        gen: format!("fz_ps_g_{n}"),
        srf: format!("fz_ps_srf_{n}"),
        tab: format!("fz_ps_tab_{n}"),
        cf: format!("fz_ps_const_{n}"),
    }
}

// ------------------------------------------------------------- dispatch ---

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_plansel_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plansel");
    let action = g.weights.pick(
        g.rng,
        &[
            "plansel:create",
            "plansel:drop",
            "plansel:scan",
            "plansel:join",
            "plansel:pwise",
            "plansel:agg",
            "plansel:setop",
            "plansel:subq",
            "plansel:prep",
            "plansel:joinrm",
            "plansel:dml",
            "plansel:explain",
        ],
    );
    match action {
        "plansel:create" => gen_create(g),
        "plansel:drop" => gen_drop(g),
        "plansel:scan" => gen_scan(g),
        "plansel:join" => gen_join(g),
        "plansel:pwise" => gen_pwise(g),
        "plansel:agg" => gen_agg(g),
        "plansel:setop" => gen_setop(g),
        "plansel:subq" => gen_subq(g),
        "plansel:prep" => gen_prep(g),
        "plansel:joinrm" => gen_joinrm(g),
        "plansel:dml" => gen_dml(g),
        _ => gen_explain(g),
    }
}

// ------------------------------------------------------------- brackets ---

/// Wrap `body` in SET/RESET pairs for the profile, RESETs in reverse
/// order, all in ONE statement group so both sides always leave the
/// group with identical GUC state.
fn bracket(p: &Profile, body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts: Vec<StmtKind> = p
        .gucs
        .iter()
        .map(|(n, v)| StmtKind::Raw(format!("SET {n} = {v};")))
        .collect();
    stmts.extend(body);
    for (n, _) in p.gucs.iter().rev() {
        stmts.push(StmtKind::Raw(format!("RESET {n};")));
    }
    stmts
}

/// BEGIN + SET LOCAL profile + body + ROLLBACK: the write-shape bracket.
/// Table state never changes; SET LOCAL dies with the transaction.
fn rollback_bracket(p: &Profile, body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts = vec![StmtKind::Raw("BEGIN;".to_string())];
    for (n, v) in p.gucs {
        stmts.push(StmtKind::Raw(format!("SET LOCAL {n} = {v};")));
    }
    stmts.extend(body);
    stmts.push(StmtKind::Raw("ROLLBACK;".to_string()));
    stmts
}

/// Pick `k` distinct profiles from `pool` (order preserved); fires
/// `plansel:prof:<name>` per pick for observability.
fn pick_profiles(g: &mut Gen, pool: &[Profile], k: usize) -> Vec<Profile> {
    let k = k.min(pool.len());
    let mut idx: Vec<usize> = (0..pool.len()).collect();
    // Partial Fisher-Yates off the session PRNG.
    for i in 0..k {
        let j = i + g.rng.below_usize(idx.len() - i);
        idx.swap(i, j);
    }
    let mut out = Vec::with_capacity(k);
    for &i in idx.iter().take(k) {
        g.fire2("plansel:prof:", pool[i].name);
        out.push(pool[i]);
    }
    out
}

/// Emit `q` under `k` sampled profiles from `pool` as one statement group.
fn sweep(g: &mut Gen, pool: &[Profile], k: usize, q: &str) -> Vec<StmtKind> {
    let profs = pick_profiles(g, pool, k);
    let mut stmts = Vec::new();
    for p in &profs {
        stmts.extend(bracket(p, vec![StmtKind::Raw(q.to_string())]));
    }
    stmts
}

// --------------------------------------------------------------- create ---

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.plan.live_sets().len() >= MAX_LIVE_SETS {
        g.fire("plansel:cap:sets");
        return gen_drop(g);
    }
    g.fire("plansel:create");
    let n = g.plan.next_set;
    g.plan.next_set += 1;
    let nm = names(n);
    let (big, small, p1, p2, gen) = (&nm.big, &nm.small, &nm.p1, &nm.p2, &nm.gen);
    let (srf, tab, cf) = (&nm.srf, &nm.tab, &nm.cf);
    // autovacuum_enabled = off on every plansel fixture (round-18a, same
    // rule as exd RB-15 / par round-14): the 10000/6000/2000-row bulk
    // loads cross autovacuum_vacuum_insert_threshold, the rollback-bracket
    // DML churns n_mod_since_analyze afterwards, and an autovacuum landing
    // on exactly ONE engine flips a later compared EXPLAIN (COSTS OFF)
    // plan. Partitioned parents carry no storage (the reloption is
    // rejected there) — their partitions carry the pin.
    let mut stmts = vec![
        // big: the multi-index scan target.
        StmtKind::Raw(format!(
            "CREATE TABLE {big} (pk int4 PRIMARY KEY, a int4, b int4, c int4, \
             flag bool, num numeric, txt text, pad text) \
             WITH (autovacuum_enabled = off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {big} SELECT i, (i * 17) % 200, (i * 7) % 50, (i * 13) % 1000, \
             i % 3 = 0, (((i * 11) % 9973)::numeric) / 10, 'p' || ((i * 23) % 211), \
             repeat('x', 8 + (i % 5)::int) FROM generate_series(1, {BIG_ROWS}) i;"
        )),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_a ON {big} (a);")),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_b ON {big} (b);")),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_ab ON {big} (a, b);")),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_txt ON {big} (txt);")),
        // Partial + expression indexes: predtest proof / boolean-clause /
        // expression-index matching fuel (indxpath.c, predtest.c).
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_cf ON {big} (c) WHERE flag;")),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_bp ON {big} (b) WHERE a > 100;")),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_absc ON {big} (abs(c));")),
        StmtKind::Raw(format!("ANALYZE {big};")),
        // small: pk-unique join target (join removal / unique-ification).
        StmtKind::Raw(format!(
            "CREATE TABLE {small} (pk int4 PRIMARY KEY, d int4, txt text) \
             WITH (autovacuum_enabled = off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {small} SELECT i, (i * 3) % 40, 's' || (i % 37) \
             FROM generate_series(1, {SMALL_ROWS}) i;"
        )),
        StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_sd ON {small} (d);")),
        StmtKind::Raw(format!("ANALYZE {small};")),
    ];
    // Partitioned twins with IDENTICAL bounds (partitionwise join fuel).
    for (t, mult) in [(p1, 5i64), (p2, 9i64)] {
        stmts.push(StmtKind::Raw(format!(
            "CREATE TABLE {t} (pk int4 PRIMARY KEY, a int4, b int4, txt text) \
             PARTITION BY RANGE (pk);"
        )));
        for (i, (lo, hi)) in
            [(0, 2500), (2500, 5000), (5000, 7500), (7500, 10000)].iter().enumerate()
        {
            stmts.push(StmtKind::Raw(format!(
                "CREATE TABLE {t}_c{i} PARTITION OF {t} FOR VALUES FROM ({lo}) TO ({hi}) \
                 WITH (autovacuum_enabled = off);"
            )));
        }
        stmts.push(StmtKind::Raw(format!(
            "INSERT INTO {t} SELECT i, (i * {mult}) % 120, (i * 11) % 60, 'q' || (i % 53) \
             FROM generate_series(1, {PART_ROWS}) i;"
        )));
        stmts.push(StmtKind::Raw(format!("CREATE INDEX ON {t} (a);")));
        stmts.push(StmtKind::Raw(format!("ANALYZE {t};")));
    }
    // Generated-column table (virtual + stored; PG18 VIRTUAL).
    stmts.push(StmtKind::Raw(format!(
        "CREATE TABLE {gen} (pk int4 PRIMARY KEY, x int4, \
         gv int4 GENERATED ALWAYS AS (x * 2 + 1) VIRTUAL, \
         gs int4 GENERATED ALWAYS AS (x * 3) STORED) \
         WITH (autovacuum_enabled = off);"
    )));
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {gen} (pk, x) SELECT i, (i * 19) % 500 FROM generate_series(1, {GEN_ROWS}) i;"
    )));
    stmts.push(StmtKind::Raw(format!("CREATE INDEX fz_psi_{n}_gs ON {gen} (gs);")));
    stmts.push(StmtKind::Raw(format!("ANALYZE {gen};")));
    // Inlinable SQL functions (SRF inlining / constant-function pullup).
    stmts.push(StmtKind::Raw(format!(
        "CREATE FUNCTION {srf}(k int4) RETURNS SETOF int4 LANGUAGE sql IMMUTABLE \
         AS 'SELECT g FROM generate_series(1, k) g';"
    )));
    stmts.push(StmtKind::Raw(format!(
        "CREATE FUNCTION {tab}(k int4) RETURNS TABLE(i int4, j int4) LANGUAGE sql IMMUTABLE \
         AS 'SELECT g, g * 2 FROM generate_series(1, k) g';"
    )));
    stmts.push(StmtKind::Raw(format!(
        "CREATE FUNCTION {cf}() RETURNS int4 LANGUAGE sql IMMUTABLE AS 'SELECT 42';"
    )));
    // One-time load-sync probes for the secondary tables: their content
    // never changes after this group (plansel writes always ROLL BACK and
    // no other module targets fz_ps_* tables), so a single full ordered
    // read-back at create time gives the same A/B state-sync guarantee as
    // runner probes. Only `big` is event-registered for runner probes —
    // the session windows law requires a registered table's CREATE to be
    // its group's FIRST statement, which only `big` satisfies here.
    for t in [small, gen, p1, p2] {
        stmts.push(StmtKind::Raw(format!("SELECT * FROM {t} ORDER BY pk;")));
    }
    g.plan.sets.push(PlanSet { n, live: true });
    g.plan.events.push(DdlEvent {
        table: big.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.plan.live_sets();
    if live.is_empty() {
        // Never recurses: an empty population is below the cap by definition.
        g.fire("plansel:fallback:create");
        return gen_create(g);
    }
    g.fire("plansel:drop");
    let si = live[g.rng.below_usize(live.len())];
    let n = g.plan.sets[si].n;
    g.plan.sets[si].live = false;
    let nm = names(n);
    let mut stmts = Vec::new();
    for t in [&nm.big, &nm.small, &nm.p1, &nm.p2, &nm.gen] {
        stmts.push(StmtKind::Raw(format!("DROP TABLE {t};")));
    }
    g.plan.events.push(DdlEvent {
        table: nm.big.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    stmts.push(StmtKind::Raw(format!("DROP FUNCTION {}(int4);", nm.srf)));
    stmts.push(StmtKind::Raw(format!("DROP FUNCTION {}(int4);", nm.tab)));
    stmts.push(StmtKind::Raw(format!("DROP FUNCTION {}();", nm.cf)));
    stmts
}

fn pick_live(g: &mut Gen) -> Option<u32> {
    let live = g.plan.live_sets();
    if live.is_empty() {
        return None;
    }
    Some(g.plan.sets[live[g.rng.below_usize(live.len())]].n)
}

macro_rules! need_set {
    ($g:expr) => {
        match pick_live($g) {
            Some(n) => names(n),
            None => {
                $g.fire("plansel:fallback:create");
                return gen_create($g);
            }
        }
    };
}

// ----------------------------------------------------------------- scan ---

/// Single-table scan-method arms: seq vs index vs bitmap vs index-only
/// over OR-lists (group_similar_or_args / match_orclause_to_indexcol),
/// boolean clauses over partial indexes (match_boolean_index_clause,
/// operator_predicate_proof), expression indexes, IN-lists, composite
/// prefixes.
fn gen_scan(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:scan");
    let t = &nm.big;
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:scan:range",
            "plansel:scan:or",
            "plansel:scan:bool",
            "plansel:scan:partial",
            "plansel:scan:exprIdx",
            "plansel:scan:inlist",
            "plansel:scan:composite",
        ],
    );
    g.fire(shape);
    let q = match shape {
        "plansel:scan:range" => {
            let lo = g.rng.below(150);
            let w = 5 + g.rng.below(40);
            format!(
                "SELECT pk, a, b FROM {t} WHERE a >= {lo} AND a < {} ORDER BY pk;",
                lo + w
            )
        }
        // OR-list across one and several columns: OR-to-index matching +
        // similar-OR grouping + bitmap OR arms.
        "plansel:scan:or" => {
            let x = g.rng.below(200);
            let y = g.rng.below(200);
            let z = g.rng.below(50);
            format!(
                "SELECT count(*), sum(pk::int8) FROM {t} \
                 WHERE a = {x} OR a = {y} OR b = {z} OR (a = {} AND b = {});",
                g.rng.below(200),
                g.rng.below(50)
            )
        }
        // Boolean-column clauses hit the partial index (WHERE flag) and
        // match_boolean_index_clause's IS TRUE / NOT arms.
        "plansel:scan:bool" => {
            let c = g.rng.below(1000);
            let v = ["flag", "NOT flag", "flag IS TRUE", "flag IS NOT FALSE"]
                [g.rng.below_usize(4)];
            format!(
                "SELECT count(*), min(pk), max(pk) FROM {t} WHERE {v} AND c < {c};"
            )
        }
        // Quals that IMPLY the partial-index predicate (a > 100) without
        // repeating it verbatim: predicate_implied_by / operator_
        // predicate_proof btree-semantics arms.
        "plansel:scan:partial" => {
            let lo = 101 + g.rng.below(80);
            let b = g.rng.below(50);
            format!(
                "SELECT count(*), sum(b::int8) FROM {t} WHERE a >= {lo} AND b = {b};"
            )
        }
        "plansel:scan:exprIdx" => {
            let c = g.rng.below(1000);
            format!("SELECT pk, c FROM {t} WHERE abs(c) = {c} ORDER BY pk;")
        }
        "plansel:scan:inlist" => {
            let base = g.rng.below(180);
            format!(
                "SELECT pk, a FROM {t} WHERE a IN ({base}, {}, {}, {}) ORDER BY pk;",
                base + 3,
                base + 7,
                base + 11
            )
        }
        // Composite-index prefix and full-key shapes (index-only capable).
        _ => {
            let a = g.rng.below(200);
            if g.rng.chance(1, 2) {
                format!("SELECT a, b FROM {t} WHERE a = {a} ORDER BY a, b;")
            } else {
                format!(
                    "SELECT count(*) FROM {t} WHERE a = {a} AND b >= {};",
                    g.rng.below(50)
                )
            }
        }
    };
    sweep(g, SCAN_POOL, 3, &q)
}

// ----------------------------------------------------------------- join ---

/// Join-strategy selection arms: hash/merge/nestloop forcing over
/// inner/left/full/semi/anti shapes, FULL-join equivalence-clause
/// reconsideration (reconsider_full_join_clause via COALESCE quals),
/// collapse-limit and GEQO join-order search, lateral refs.
fn gen_join(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:join");
    let (b, s) = (&nm.big, &nm.small);
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:join:inner",
            "plansel:join:left",
            "plansel:join:full",
            "plansel:join:semi",
            "plansel:join:anti",
            "plansel:join:multi",
            "plansel:join:lateral",
        ],
    );
    g.fire(shape);
    let q = match shape {
        "plansel:join:inner" => {
            let lim = 20 + g.rng.below(120);
            format!(
                "SELECT x.pk, y.pk FROM {b} x JOIN {s} y ON x.a = y.pk \
                 WHERE x.pk <= {lim} * 4 AND y.d < 30 ORDER BY x.pk, y.pk;"
            )
        }
        "plansel:join:left" => format!(
            "SELECT count(*), count(y.pk), sum(x.b::int8) FROM {b} x \
             LEFT JOIN {s} y ON x.a = y.pk AND y.d > {};",
            g.rng.below(40)
        ),
        // FULL JOIN with a mergejoinable equality + COALESCE filter:
        // reconsider_full_join_clause / outer-join eclass arms.
        "plansel:join:full" => format!(
            "SELECT count(*), sum(COALESCE(x.a, 0)::int8), sum(COALESCE(y.pk, 0)::int8) \
             FROM {b} x FULL JOIN {s} y ON x.a = y.pk \
             WHERE COALESCE(x.b, y.d, 0) < {};",
            10 + g.rng.below(40)
        ),
        "plansel:join:semi" => format!(
            "SELECT count(*), sum(pk::int8) FROM {b} x \
             WHERE EXISTS (SELECT 1 FROM {s} y WHERE y.pk = x.a AND y.d < {});",
            g.rng.below(40)
        ),
        "plansel:join:anti" => format!(
            "SELECT count(*), min(pk) FROM {b} x \
             WHERE NOT EXISTS (SELECT 1 FROM {s} y WHERE y.pk = x.a AND y.txt = 's{}');",
            g.rng.below(37)
        ),
        // 4-rel chain: collapse-limit / GEQO join-order search space.
        "plansel:join:multi" => format!(
            "SELECT count(*), sum(x.pk::int8), sum(z.pk::int8) \
             FROM {b} x JOIN {s} y ON x.a = y.pk \
             JOIN {} z ON z.pk = y.pk * 30 JOIN {} w ON w.pk = z.pk \
             WHERE x.b < {};",
            nm.p1,
            nm.p2,
            5 + g.rng.below(20)
        ),
        // LATERAL subquery referencing the outer row: lateral join-info /
        // PHV-lateral arms (extract_lateral_vars_from_PHVs).
        _ => format!(
            "SELECT x.pk, l.cnt FROM {s} x, LATERAL \
             (SELECT count(*) AS cnt FROM {b} y WHERE y.a = x.pk AND y.b < {}) l \
             WHERE x.pk <= 60 ORDER BY x.pk;",
            5 + g.rng.below(45)
        ),
    };
    sweep(g, JOIN_POOL, 3, &q)
}

// ---------------------------------------------------------------- pwise ---

/// Partitionwise join/aggregate arms (try_partitionwise_join,
/// generate_partitionwise_join_paths, reparameterize_path_by_child under
/// pwise_nest, apply_scanjoin_target appendrel arms).
fn gen_pwise(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:pwise");
    let (p1, p2) = (&nm.p1, &nm.p2);
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:pwise:join",
            "plansel:pwise:leftjoin",
            "plansel:pwise:agg",
            "plansel:pwise:joinagg",
            "plansel:pwise:lateral",
        ],
    );
    g.fire(shape);
    let q = match shape {
        // Equi-join on the partition key: exact partitionwise match.
        "plansel:pwise:join" => {
            let lim = 100 + g.rng.below(400);
            format!(
                "SELECT x.pk, x.a, y.a FROM {p1} x JOIN {p2} y ON x.pk = y.pk \
                 WHERE x.pk <= {lim} ORDER BY x.pk;"
            )
        }
        "plansel:pwise:leftjoin" => format!(
            "SELECT count(*), count(y.pk), sum(x.b::int8) FROM {p1} x \
             LEFT JOIN {p2} y ON x.pk = y.pk AND y.b < {};",
            10 + g.rng.below(40)
        ),
        // GROUP BY on the partition key: partitionwise aggregate.
        "plansel:pwise:agg" => format!(
            "SELECT pk / 2500 AS grp, count(*), sum(a::int8) FROM {p1} \
             WHERE b < {} GROUP BY pk / 2500 ORDER BY grp;",
            20 + g.rng.below(40)
        ),
        "plansel:pwise:joinagg" => format!(
            "SELECT x.pk / 2500 AS grp, count(*), sum((x.a + y.b)::int8) \
             FROM {p1} x JOIN {p2} y ON x.pk = y.pk \
             GROUP BY x.pk / 2500 ORDER BY grp;"
        ),
        // Parameterized nestloop into a partitioned inner under
        // partitionwise join: reparameterize_path_by_child fuel.
        _ => format!(
            "SELECT x.pk, l.mx FROM {p1} x, LATERAL \
             (SELECT max(y.a) AS mx FROM {p2} y WHERE y.pk = x.pk) l \
             WHERE x.pk <= {} ORDER BY x.pk;",
            50 + g.rng.below(150)
        ),
    };
    sweep(g, PWISE_POOL, 3, &q)
}

// ------------------------------------------------------------------ agg ---

/// Grouping-path construction arms: hashed vs sorted vs incremental-sort
/// grouping, grouping sets (preprocess_grouping_sets), DISTINCT paths
/// (create_final_distinct_paths), partial/parallel grouping
/// (create_partial_grouping_paths).
fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:agg");
    let t = &nm.big;
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:agg:group",
            "plansel:agg:gsets",
            "plansel:agg:distinct",
            "plansel:agg:countd",
            "plansel:agg:having",
            "plansel:agg:ordered",
        ],
    );
    g.fire(shape);
    let q = match shape {
        "plansel:agg:group" => {
            let key = ["a", "b", "a, b", "a % 7", "txt"][g.rng.below_usize(5)];
            format!(
                "SELECT {key}, count(*), sum(c::int8) FROM {t} GROUP BY {key} \
                 ORDER BY {key};"
            )
        }
        "plansel:agg:gsets" => {
            let kind = ["ROLLUP (a, b)", "CUBE (a, b)", "GROUPING SETS ((a), (b), ())"]
                [g.rng.below_usize(3)];
            format!(
                "SELECT a, b, count(*), sum(pk::int8) FROM {t} WHERE a < {} \
                 GROUP BY {kind} ORDER BY a NULLS LAST, b NULLS LAST;",
                10 + g.rng.below(30)
            )
        }
        "plansel:agg:distinct" => {
            if g.rng.chance(1, 2) {
                format!("SELECT DISTINCT a, b FROM {t} WHERE c < 200 ORDER BY a, b;")
            } else {
                format!("SELECT DISTINCT ON (a) a, b, pk FROM {t} ORDER BY a, b, pk;")
            }
        }
        "plansel:agg:countd" => format!(
            "SELECT b, count(DISTINCT a), count(DISTINCT txt) FROM {t} \
             GROUP BY b ORDER BY b;"
        ),
        "plansel:agg:having" => format!(
            "SELECT a, count(*) FROM {t} GROUP BY a HAVING count(*) > {} \
             ORDER BY a;",
            40 + g.rng.below(20)
        ),
        // Ordered-set / ordered-input aggregates force sort-below-agg.
        _ => format!(
            "SELECT b, percentile_disc(0.5) WITHIN GROUP (ORDER BY a), \
             string_agg(txt, ',' ORDER BY pk) FILTER (WHERE pk % 97 = 0) \
             FROM {t} WHERE b < 10 GROUP BY b ORDER BY b;"
        ),
    };
    sweep(g, AGG_POOL, 3, &q)
}

// ---------------------------------------------------------------- setop ---

/// Set-operation path arms (generate_union_paths / generate_nonunion_paths
/// / recurse_set_operations): hashed vs sorted dedup, UNION ALL append.
fn gen_setop(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:setop");
    let (b, s, p1) = (&nm.big, &nm.small, &nm.p1);
    let op = ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]
        [g.rng.below_usize(6)];
    g.fire2("plansel:setop:", op);
    let lim = 50 + g.rng.below(150);
    let q = if g.rng.chance(1, 2) {
        format!(
            "SELECT * FROM (SELECT a, b FROM {b} WHERE pk <= {lim} {op} \
             SELECT a, b FROM {p1} WHERE pk <= {lim}) u ORDER BY a, b;"
        )
    } else {
        format!(
            "SELECT * FROM (SELECT pk % 40 AS k FROM {s} {op} \
             SELECT b AS k FROM {b} WHERE pk <= {lim}) u ORDER BY k;"
        )
    };
    sweep(g, SETOP_POOL, 3, &q)
}

// ----------------------------------------------------------------- subq ---

/// Sublink pullup + unique-ification + window run-condition + LIMIT arms:
/// pull_up_sublinks_qual_recurse (AND/OR sublink trees), create_unique_
/// path/plan (IN semijoin unique-ify under nestloop), find_window_run_
/// conditions (rank/row_number outer quals), preprocess_limit (non-const
/// LIMIT/OFFSET).
fn gen_subq(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:subq");
    let (b, s) = (&nm.big, &nm.small);
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:subq:in",
            "plansel:subq:ortree",
            "plansel:subq:scalar",
            "plansel:subq:winrun",
            "plansel:subq:limit",
            "plansel:subq:any",
        ],
    );
    g.fire(shape);
    let q = match shape {
        // IN over a non-unique inner: semijoin unique-ification
        // (create_unique_path under nestloop/merge profiles).
        "plansel:subq:in" => format!(
            "SELECT count(*), sum(pk::int8) FROM {b} \
             WHERE b IN (SELECT d FROM {s} WHERE txt < 's{}');",
            10 + g.rng.below(27)
        ),
        // Sublinks under AND/OR: qual_recurse OR-arm (not pulled up) next
        // to the AND-arm (pulled up).
        "plansel:subq:ortree" => format!(
            "SELECT count(*) FROM {b} x WHERE (x.a < {} OR EXISTS \
             (SELECT 1 FROM {s} y WHERE y.pk = x.a)) AND EXISTS \
             (SELECT 1 FROM {s} z WHERE z.d = x.b);",
            g.rng.below(50)
        ),
        "plansel:subq:scalar" => format!(
            "SELECT pk, a, (SELECT d FROM {s} y WHERE y.pk = x.a) AS sd \
             FROM {b} x WHERE x.pk <= {} ORDER BY pk;",
            40 + g.rng.below(160)
        ),
        // Monotonic window function bounded in the outer WHERE: the run
        // condition optimization (find_window_run_conditions).
        "plansel:subq:winrun" => {
            let wf = ["row_number()", "rank()", "count(*)"][g.rng.below_usize(3)];
            format!(
                "SELECT * FROM (SELECT pk, a, {wf} OVER (ORDER BY pk) AS rn \
                 FROM {b}) w WHERE rn <= {} ORDER BY pk;",
                20 + g.rng.below(80)
            )
        }
        "plansel:subq:limit" => {
            let kind = g.rng.below_usize(4);
            match kind {
                0 => format!(
                    "SELECT pk, a FROM {b} ORDER BY pk LIMIT (SELECT {}) OFFSET 3;",
                    10 + g.rng.below(40)
                ),
                1 => format!("SELECT pk, a FROM {b} ORDER BY pk LIMIT ALL OFFSET {};", BIG_ROWS - 5),
                2 => format!(
                    "SELECT * FROM (SELECT pk, b FROM {b} ORDER BY b \
                     FETCH FIRST {} ROWS WITH TIES) w ORDER BY b, pk;",
                    17 + g.rng.below(60)
                ),
                _ => format!("SELECT pk FROM {b} ORDER BY pk LIMIT 0;"),
            }
        }
        // ANY/ALL array + NOT IN (null-aware anti-join territory; d is
        // NOT NULL by construction so NOT IN stays deterministic).
        _ => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT count(*) FROM {b} WHERE a = ANY \
                     (SELECT pk FROM {s} WHERE d = {});",
                    g.rng.below(40)
                )
            } else {
                format!(
                    "SELECT count(*), max(pk) FROM {b} \
                     WHERE b NOT IN (SELECT d FROM {s} WHERE pk <= {});",
                    20 + g.rng.below(100)
                )
            }
        }
    };
    sweep(g, SUBQ_POOL, 2, &q)
}

// ----------------------------------------------------------------- prep ---

/// prepjointree/prepqual arms: subquery pullup with replace-vars,
/// constant-function pullup, VALUES pullup, useless-RESULT removal, SRF
/// inlining, NOT-negation, duplicate-OR factoring, virtual generated
/// columns.
fn gen_prep(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:prep");
    let (b, s, gt) = (&nm.big, &nm.small, &nm.gen);
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:prep:pullup",
            "plansel:prep:values",
            "plansel:prep:result",
            "plansel:prep:srf",
            "plansel:prep:constfn",
            "plansel:prep:negate",
            "plansel:prep:dupors",
            "plansel:prep:genvirt",
        ],
    );
    g.fire(shape);
    let q = match shape {
        // FROM-subquery pullup with expressions over the pulled vars
        // (pullup_replace_vars_callback wrap arms); the grouped variant is
        // NOT pulled up (convert_subquery_pathkeys under mergejoin).
        "plansel:prep:pullup" => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT u.k + 1, count(*) FROM \
                     (SELECT a + b AS k, pk FROM {b} WHERE c < {}) u \
                     GROUP BY u.k ORDER BY u.k + 1;",
                    100 + g.rng.below(400)
                )
            } else {
                format!(
                    "SELECT x.d, u.cnt FROM {s} x JOIN \
                     (SELECT a, count(*) AS cnt FROM {b} GROUP BY a) u \
                     ON u.a = x.pk WHERE x.pk <= {} ORDER BY x.d, u.cnt, x.pk;",
                    50 + g.rng.below(150)
                )
            }
        }
        "plansel:prep:values" => format!(
            "SELECT v.k, v.lbl, count(y.pk) FROM \
             (VALUES ({}, 'u'), ({}, 'v'), ({}, 'w')) v(k, lbl) \
             LEFT JOIN {s} y ON y.pk = v.k GROUP BY v.k, v.lbl ORDER BY v.k;",
            g.rng.below(250),
            g.rng.below(250),
            g.rng.below(250)
        ),
        // RTE_RESULT elision (remove_useless_results_recurse).
        "plansel:prep:result" => format!(
            "SELECT count(*) FROM (SELECT {} AS one) o, {s} y \
             WHERE y.d = o.one % 40;",
            g.rng.below(97)
        ),
        // SETOF SQL function in FROM: inline_set_returning_function; the
        // ROWS FROM / ORDINALITY variants keep the non-inlinable arms warm.
        "plansel:prep:srf" => {
            let k = 10 + g.rng.below(90);
            match g.rng.below_usize(3) {
                0 => format!(
                    "SELECT g, count(y.pk) FROM {}({k}) g LEFT JOIN {s} y ON y.pk = g \
                     GROUP BY g ORDER BY g;",
                    nm.srf
                ),
                1 => format!(
                    "SELECT f.i, f.j FROM {}({k}) f WHERE f.i % 3 = 0 ORDER BY f.i;",
                    nm.tab
                ),
                _ => format!(
                    "SELECT o.ord, o.g FROM ROWS FROM ({}({k}), {}({})) \
                     WITH ORDINALITY o(g, i, j, ord) ORDER BY o.ord;",
                    nm.srf,
                    nm.tab,
                    5 + g.rng.below(20)
                ),
            }
        }
        // Non-SRF stable function in FROM: pull_up_constant_function.
        "plansel:prep:constfn" => format!(
            "SELECT f.v, count(*) FROM {}() f(v), {s} y WHERE y.pk <= f.v \
             GROUP BY f.v ORDER BY f.v;",
            nm.cf
        ),
        "plansel:prep:negate" => format!(
            "SELECT count(*), sum(pk::int8) FROM {b} \
             WHERE NOT (a = {} OR b = {} OR NOT (c < {}));",
            g.rng.below(200),
            g.rng.below(50),
            200 + g.rng.below(700)
        ),
        // (X AND Y) OR (X AND Z): process_duplicate_ors factoring.
        "plansel:prep:dupors" => {
            let x = g.rng.below(200);
            format!(
                "SELECT count(*) FROM {b} WHERE (a = {x} AND b < {}) \
                 OR (a = {x} AND b > {});",
                5 + g.rng.below(20),
                30 + g.rng.below(18)
            )
        }
        // Virtual generated column expansion + stored-column index path.
        _ => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT pk, gv FROM {gt} WHERE gv > {} AND gv % 3 = 0 ORDER BY pk;",
                    500 + g.rng.below(400)
                )
            } else {
                format!(
                    "SELECT count(*), sum(gs::int8) FROM {gt} WHERE gs = {};",
                    (g.rng.below(500)) * 3
                )
            }
        }
    };
    sweep(g, PREP_POOL, 2, &q)
}

// --------------------------------------------------------------- joinrm ---

/// Join-removal / distinctness arms (analyzejoins.c): removable LEFT JOIN
/// on a unique key with unreferenced inner, unique-key semijoin reduction
/// (query_is_distinct_for), self-join elimination shapes.
fn gen_joinrm(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:joinrm");
    let (b, s) = (&nm.big, &nm.small);
    let shape = g.weights.pick(
        g.rng,
        &["plansel:joinrm:left", "plansel:joinrm:distinct", "plansel:joinrm:self"],
    );
    g.fire(shape);
    let q = match shape {
        // Inner side unique (pk), no inner vars used above the join:
        // remove_useless_left_joins / remove_rel_from_query.
        "plansel:joinrm:left" => format!(
            "SELECT x.pk, x.a FROM {b} x LEFT JOIN {s} y ON x.a = y.pk \
             WHERE x.pk <= {} ORDER BY x.pk;",
            50 + g.rng.below(200)
        ),
        // IN over DISTINCT / GROUP BY inners: query_is_distinct_for's
        // distinct-clause and grouping arms.
        "plansel:joinrm:distinct" => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT count(*) FROM {b} WHERE a IN \
                     (SELECT DISTINCT pk FROM {s} WHERE d < {});",
                    10 + g.rng.below(30)
                )
            } else {
                format!(
                    "SELECT count(*) FROM {b} WHERE a IN \
                     (SELECT pk FROM {s} GROUP BY pk HAVING count(*) = 1);"
                )
            }
        }
        // Self-join on the primary key (SJE-adjacent shapes; the planner
        // arms fire whether or not the join is eliminated).
        _ => format!(
            "SELECT x.pk, x.a, y.b FROM {b} x JOIN {b} y ON x.pk = y.pk \
             WHERE x.b < {} ORDER BY x.pk;",
            5 + g.rng.below(20)
        ),
    };
    sweep(g, JOINRM_POOL, 2, &q)
}

// ------------------------------------------------------------------ dml ---

/// Write-statement plan arms under forced scan/join methods, ALWAYS
/// inside BEGIN..ROLLBACK (table state never changes): ModifyTable
/// construction (make_modifytable), ON CONFLICT arbiter inference
/// (infer_arbiter_indexes), partitioned UPDATE/DELETE (appendinfo /
/// inherit expansion), MERGE planning, ordered-CTE RETURNING.
fn gen_dml(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:dml");
    let (b, s, p1) = (&nm.big, &nm.small, &nm.p1);
    let shape = g.weights.pick(
        g.rng,
        &[
            "plansel:dml:update",
            "plansel:dml:delete",
            "plansel:dml:conflict",
            "plansel:dml:partupd",
            "plansel:dml:merge",
        ],
    );
    g.fire(shape);
    let body = match shape {
        "plansel:dml:update" => {
            let a = g.rng.below(200);
            format!(
                "WITH w AS (UPDATE {b} SET c = c + 1, num = num + 1 \
                 WHERE a = {a} RETURNING pk, c) \
                 SELECT count(*), sum(pk::int8), sum(c::int8) FROM w;"
            )
        }
        "plansel:dml:delete" => format!(
            "WITH w AS (DELETE FROM {b} WHERE b = {} AND flag RETURNING pk) \
             SELECT count(*), min(pk), max(pk) FROM w;",
            g.rng.below(50)
        ),
        "plansel:dml:conflict" => {
            let k = 1 + g.rng.below(400); // half collide with 1..200
            if g.rng.chance(1, 2) {
                format!(
                    "WITH w AS (INSERT INTO {s} VALUES ({k}, {}, 'c') \
                     ON CONFLICT (pk) DO UPDATE SET d = {s}.d + 100 \
                     RETURNING pk, d) SELECT count(*), sum(d::int8) FROM w;",
                    g.rng.below(40)
                )
            } else {
                format!(
                    "WITH w AS (INSERT INTO {s} \
                     SELECT i, i % 40, 'n' FROM generate_series({k}, {}) i \
                     ON CONFLICT (pk) DO NOTHING RETURNING pk) \
                     SELECT count(*), min(pk) FROM w;",
                    k + 30
                )
            }
        }
        // Partitioned UPDATE incl. a cross-partition row-move variant
        // (rows 1..40 move to the empty 6001+ range of partition 3).
        "plansel:dml:partupd" => {
            if g.rng.chance(1, 3) {
                format!(
                    "WITH w AS (UPDATE {p1} SET pk = pk + 6000, a = a + 1 \
                     WHERE pk <= 40 RETURNING pk) \
                     SELECT count(*), min(pk), max(pk) FROM w;"
                )
            } else {
                format!(
                    "WITH w AS (UPDATE {p1} SET b = b + 1 WHERE a = {} \
                     RETURNING pk, b) SELECT count(*), sum(b::int8) FROM w;",
                    g.rng.below(120)
                )
            }
        }
        _ => {
            let lo = 150 + g.rng.below(100); // straddles the 200 boundary
            format!(
                "MERGE INTO {s} t USING \
                 (SELECT i AS k, (i * 7) % 40 AS nd FROM generate_series({lo}, {}) i) v \
                 ON t.pk = v.k \
                 WHEN MATCHED THEN UPDATE SET d = v.nd \
                 WHEN NOT MATCHED THEN INSERT (pk, d, txt) VALUES (v.k, v.nd, 'm');",
                lo + 40
            )
        }
    };
    let profs = pick_profiles(g, DML_POOL, 2);
    let mut stmts = Vec::new();
    for p in &profs {
        stmts.extend(rollback_bracket(p, vec![StmtKind::Raw(body.clone())]));
    }
    stmts
}

// -------------------------------------------------------------- explain ---

/// Plan-shape witnesses: EXPLAIN (COSTS OFF) of a scan or join shape under
/// two profiles. Different profiles are EXPECTED to show different nodes;
/// the A/B compare (same profile both sides) checks pgrust picks a
/// C-equivalent plan. Kept at low weight — result-identity sweeps are the
/// primary oracle.
fn gen_explain(g: &mut Gen) -> Vec<StmtKind> {
    let nm = need_set!(g);
    g.fire("plansel:explain");
    let (b, s) = (&nm.big, &nm.small);
    let (q, pool): (String, &[Profile]) = if g.rng.chance(1, 2) {
        (
            format!(
                "EXPLAIN (COSTS OFF) SELECT pk, a FROM {b} WHERE a = {} OR b = {};",
                g.rng.below(200),
                g.rng.below(50)
            ),
            SCAN_POOL,
        )
    } else {
        (
            format!(
                "EXPLAIN (COSTS OFF) SELECT count(*) FROM {b} x JOIN {s} y \
                 ON x.a = y.pk WHERE y.d < {};",
                g.rng.below(40)
            ),
            JOIN_POOL,
        )
    };
    sweep(g, pool, 2, &q)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut productions = Vec::new();
        let mut out = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &weights, &mut productions, 4);
        for _ in 0..n {
            let stmts = gen_plansel_module(&mut g);
            out.push(stmts.iter().map(|s| s.to_sql()).collect());
        }
        out
    }

    #[test]
    fn groups_are_set_reset_balanced() {
        // Every SET in a non-transactional group has a matching RESET in
        // the SAME group; BEGIN brackets close with ROLLBACK in-group.
        for group in gen_groups(11, 300) {
            let mut sets: Vec<String> = Vec::new();
            let mut open_txn = 0i32;
            for sql in &group {
                if sql == "BEGIN;" {
                    open_txn += 1;
                } else if sql == "ROLLBACK;" {
                    open_txn -= 1;
                } else if let Some(rest) = sql.strip_prefix("SET LOCAL ") {
                    assert!(open_txn > 0, "SET LOCAL outside txn: {sql}");
                    let _ = rest;
                } else if let Some(rest) = sql.strip_prefix("SET ") {
                    sets.push(rest.split(' ').next().unwrap().to_string());
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = sets
                        .iter()
                        .rposition(|s| *s == name)
                        .unwrap_or_else(|| panic!("RESET {name} without SET in {group:?}"));
                    sets.remove(pos);
                }
            }
            assert!(sets.is_empty(), "unRESET SETs {sets:?} in {group:?}");
            assert_eq!(open_txn, 0, "unclosed BEGIN in {group:?}");
        }
    }

    #[test]
    fn row_returning_statements_are_totally_ordered() {
        // Every row-returning SELECT sweep statement carries an ORDER BY;
        // aggregate-only shapes (count/sum/min/max projections) may skip
        // it. EXPLAIN output is plan text, exempt by design.
        for group in gen_groups(23, 400) {
            for sql in &group {
                if !sql.starts_with("SELECT ") && !sql.starts_with("WITH ") {
                    continue;
                }
                let aggregate_only = sql.starts_with("SELECT count(")
                    || sql.starts_with("WITH ")
                    || sql.contains(") SELECT count(");
                if !aggregate_only {
                    assert!(
                        sql.contains("ORDER BY"),
                        "row-returning statement without total order: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn writes_only_inside_rollback_brackets() {
        for group in gen_groups(37, 400) {
            let mut open = 0i32;
            for sql in &group {
                if sql == "BEGIN;" {
                    open += 1;
                } else if sql == "ROLLBACK;" {
                    open -= 1;
                }
                let is_write = sql.starts_with("WITH w AS (UPDATE")
                    || sql.starts_with("WITH w AS (DELETE")
                    || sql.starts_with("WITH w AS (INSERT")
                    || sql.starts_with("MERGE INTO");
                if is_write {
                    assert!(open > 0, "write outside rollback bracket: {sql}");
                }
            }
        }
    }

    #[test]
    fn deterministic_per_seed() {
        assert_eq!(gen_groups(5, 60), gen_groups(5, 60));
    }

    /// Round-18a, same rule as exd RB-15 / par round-14: every
    /// storage-bearing plansel CREATE TABLE pins autovacuum_enabled = off
    /// (partitioned parents are exempt — no storage; their partitions
    /// carry the pin). The 10000/6000/2000-row bulk loads cross the
    /// insert-autovacuum threshold and the module's whole surface is
    /// compared EXPLAIN (COSTS OFF) plan shape.
    #[test]
    fn creates_pin_autovacuum_off() {
        let mut seen = 0;
        for grp in gen_groups(0x18A, 300) {
            for sql in grp {
                if !sql.starts_with("CREATE TABLE ") || sql.contains(" PARTITION BY ") {
                    continue;
                }
                assert!(
                    sql.contains("autovacuum_enabled = off"),
                    "plansel fixture does not pin autovacuum off: `{sql}`"
                );
                seen += 1;
            }
        }
        assert!(seen > 0, "no CREATE TABLE generated in 300 groups");
    }

    #[test]
    fn fixture_lifecycle_caps_at_one_live_set() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(7);
        let mut productions = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &weights, &mut productions, 4);
        for _ in 0..200 {
            let _ = gen_plansel_module(&mut g);
            assert!(g.plan.live_sets().len() <= MAX_LIVE_SETS);
        }
        // Events must pair create/drop per registered table.
        let ev = g.plan.take_events();
        assert!(!ev.is_empty());
    }
}
