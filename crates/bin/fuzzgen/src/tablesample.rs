//! TABLESAMPLE + LIMIT/OFFSET/FETCH + DISTINCT ON drain module: the
//! Track-B SQL-drainable residue around the row-sampling and row-limiting
//! executor nodes — nodeSamplescan.c (SampleNext / tablesample_init /
//! tablesample_getnext / re-evaluated REPEATABLE args), the tsmapi handlers
//! bernoulli.c and system.c (BeginSampleScan / NextSampleBlock /
//! NextSampleTuple / SampleScanGetSampleSize / the 0..100 range guard),
//! nodeLimit.c (LIMIT/OFFSET/FETCH WITH TIES state machine), and the
//! DISTINCT ON first-row-per-group path in nodeUnique.c + the planner's
//! DISTINCT-ON / matching-leading-ORDER-BY analysis.
//!
//! Mechanism: a purpose-built, session-persistent fixture whose every value
//! is a pure integer formula of the row number (identical on both
//! differential sides by construction, inserted in one deterministic order
//! with autovacuum off so the physical heap layout — and therefore the
//! block/tuple sampling order — is byte-identical on both engines). Over it
//! we emit families of row-returning probes, each carrying a TOTAL ORDER BY
//! (ending in the unique pk) so the compared row stream is deterministic:
//!
//!   - BERNOULLI(p) / SYSTEM(p) REPEATABLE(seed): with a FIXED seed the
//!     sampled row SET is a deterministic function of (p, seed, heap
//!     layout). Same query text runs on both engines; any A/B divergence in
//!     the sampled set is a real sampler finding. p sweeps 0 (empty), 100
//!     (all), and fractional values; seeds sweep a fixed pool.
//!   - same-seed determinism probe: the symmetric difference of two
//!     identically-seeded samples must be empty (count 0) on BOTH engines.
//!     A non-zero count is same-seed non-determinism (nodeSamplescan
//!     re-evaluating the REPEATABLE argument differently across scans) —
//!     HIGH severity.
//!   - TABLESAMPLE on a join input and on an old-style inheritance parent
//!     (parent+child rows, disjoint pks).
//!   - LIMIT / OFFSET / LIMIT ALL / OFFSET-past-end / FETCH FIRST n ROWS
//!     ONLY / FETCH FIRST n ROWS WITH TIES (the ties path, wrapped in an
//!     outer total ORDER BY so the tie-group set is compared order-stably) /
//!     LIMIT with bind parameters (PREPARE/EXECUTE/DEALLOCATE).
//!   - DISTINCT ON (expr…) with a matching leading ORDER BY: first row per
//!     group, exact row comparison.
//!   - error arms (compared by SQLSTATE, standalone so no bracket is
//!     poisoned): sample percentage out of range (2202H), TABLESAMPLE on a
//!     non-table (subquery alias), REPEATABLE referencing a table column
//!     (the bug-129 re-evaluation class — must be rejected, never a
//!     reachable Var), DISTINCT ON not matching the leading ORDER BY, and
//!     negative LIMIT/OFFSET.
//!
//! The fixture is NOT registered into the shared effective catalog (only
//! Created/Dropped probe events), so no other module writes into it and the
//! sampling stays deterministic; the runner's pk-ordered state probes still
//! cover it.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// One live fixture set at a time (a create group is ~10 statements).
const MAX_LIVE_SETS: usize = 1;

const MAIN_ROWS: i64 = 400;
const JOIN_ROWS: i64 = 120;
const PARENT_ROWS: i64 = 100;
/// Child pks start past the parent range so pk stays unique across the
/// inheritance hierarchy (the pk-ordered state probe must be total).
const CHILD_LO: i64 = 101;
const CHILD_HI: i64 = 200;

/// Sample-size percentages: 0 (empty), 100 (all), and fractional/small
/// interior values.
const P_POOL: &[&str] = &["0", "100", "0.5", "12.5", "37", "5", "63.2", "1", "25"];
/// REPEATABLE seeds (int + one fractional).
const SEED_POOL: &[&str] = &["0", "1", "42", "7", "1234567", "0.5"];

// ----------------------------------------------------------------- state ---

#[derive(Clone, Debug)]
pub struct TsmSet {
    pub n: u32,
    pub live: bool,
}

/// Session-persistent fixture-set model (swapped in and out of `Gen` by the
/// session loop exactly like `SpillState` / `PlanState`).
#[derive(Clone, Debug, Default)]
pub struct TablesampleState {
    pub sets: Vec<TsmSet>,
    next_set: u32,
    next_prep: u32,
    events: Vec<DdlEvent>,
}

impl TablesampleState {
    pub fn new() -> TablesampleState {
        TablesampleState::default()
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

/// Table names for set N.
struct Names {
    main: String,
    join: String,
    parent: String,
    child: String,
}

fn names(n: u32) -> Names {
    Names {
        main: format!("fz_tsm_{n}"),
        join: format!("fz_tsj_{n}"),
        parent: format!("fz_tsp_{n}"),
        child: format!("fz_tsc_{n}"),
    }
}

// -------------------------------------------------------------- dispatch ---

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_tablesample_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tablesample");
    let action = g.weights.pick(
        g.rng,
        &[
            "tsm:create",
            "tsm:drop",
            "tsm:bernoulli",
            "tsm:system",
            "tsm:sameseed",
            "tsm:join",
            "tsm:inherit",
            "tsm:limit",
            "tsm:distincton",
            "tsm:err",
        ],
    );
    match action {
        "tsm:create" => gen_create(g),
        "tsm:drop" => gen_drop(g),
        "tsm:bernoulli" => gen_sample(g, "BERNOULLI"),
        "tsm:system" => gen_sample(g, "SYSTEM"),
        "tsm:sameseed" => gen_sameseed(g),
        "tsm:join" => gen_join(g),
        "tsm:inherit" => gen_inherit(g),
        "tsm:limit" => gen_limit(g),
        "tsm:distincton" => gen_distincton(g),
        _ => gen_err(g),
    }
}

// ---------------------------------------------------------------- helpers ---

fn pick<'p>(g: &mut Gen, pool: &[&'p str]) -> &'p str {
    pool[g.rng.below_usize(pool.len())]
}

/// A boundary-heavy small count for LIMIT/OFFSET/FETCH.
fn count(g: &mut Gen) -> u64 {
    match g.rng.below(5) {
        0 => 0,
        1 => 1,
        2 => 3,
        3 => 7,
        _ => 2 + g.rng.below(20) as u64,
    }
}

fn one(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

// ----------------------------------------------------------------- create ---

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.tsm.live_sets().len() >= MAX_LIVE_SETS {
        g.fire("tsm:cap:sets");
        return gen_drop(g);
    }
    g.fire("tsm:create");
    let n = g.tsm.next_set;
    g.tsm.next_set += 1;
    let nm = names(n);
    let (main, join, parent, child) = (&nm.main, &nm.join, &nm.parent, &nm.child);
    // The main table's CREATE MUST be the group's first statement: the
    // session windows law registers a table's probe window at its group's
    // first statement, so only the main table is event-registered. The
    // other tables get in-group pk-ordered read-backs instead (the same
    // create-time A/B state-sync guarantee the runner probes give).
    let stmts = vec![
        // Main sampling target: pk unique, a/b low-cardinality group keys.
        StmtKind::Raw(format!(
            "CREATE TABLE {main} (pk int4 PRIMARY KEY, a int4, b int4, txt text) \
             WITH (autovacuum_enabled=off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {main} SELECT i, i % 50, i % 7, 'r' || (i % 13) \
             FROM generate_series(1, {MAIN_ROWS}) i;"
        )),
        // Join input (shares the `a` domain with main).
        StmtKind::Raw(format!(
            "CREATE TABLE {join} (pk int4 PRIMARY KEY, a int4, txt text) \
             WITH (autovacuum_enabled=off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {join} SELECT i, i % 50, 'j' || (i % 11) \
             FROM generate_series(1, {JOIN_ROWS}) i;"
        )),
        // Old-style inheritance parent + child; pks disjoint so pk stays
        // unique across the hierarchy (total-order state probe).
        StmtKind::Raw(format!(
            "CREATE TABLE {parent} (pk int4, a int4, txt text) \
             WITH (autovacuum_enabled=off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {parent} SELECT i, i % 40, 'p' || (i % 9) \
             FROM generate_series(1, {PARENT_ROWS}) i;"
        )),
        StmtKind::Raw(format!(
            "CREATE TABLE {child} (pk int4, a int4, txt text) INHERITS ({parent}) \
             WITH (autovacuum_enabled=off);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {child} SELECT i, i % 40, 'c' || (i % 9) \
             FROM generate_series({CHILD_LO}, {CHILD_HI}) i;"
        )),
        StmtKind::Raw(format!("ANALYZE {main};")),
        StmtKind::Raw(format!("ANALYZE {parent};")),
        // Create-time read-backs (A/B state sync for the non-registered
        // tables; parent's SELECT * spans the inheritance children, whose
        // pks are disjoint so the pk order is total).
        StmtKind::Raw(format!("SELECT * FROM {join} ORDER BY pk;")),
        StmtKind::Raw(format!("SELECT * FROM {parent} ORDER BY pk;")),
        StmtKind::Raw(format!("SELECT * FROM {child} ORDER BY pk;")),
    ];
    g.tsm.sets.push(TsmSet { n, live: true });
    g.tsm.events.push(DdlEvent {
        table: main.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.tsm.live_sets();
    if live.is_empty() {
        // Never recurses: an empty population is below the cap by definition.
        g.fire("tsm:fallback:create");
        return gen_create(g);
    }
    g.fire("tsm:drop");
    let si = live[g.rng.below_usize(live.len())];
    g.tsm.sets[si].live = false;
    let n = g.tsm.sets[si].n;
    let nm = names(n);
    // Only the main table is event-registered, so its DROP must be the
    // group's FIRST statement (window.until law). Child drops with the
    // parent (CASCADE covers the inheritance link).
    g.tsm.events.push(DdlEvent {
        table: nm.main.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![
        StmtKind::Raw(format!("DROP TABLE IF EXISTS {} CASCADE;", nm.main)),
        StmtKind::Raw(format!("DROP TABLE IF EXISTS {} CASCADE;", nm.parent)),
        StmtKind::Raw(format!("DROP TABLE IF EXISTS {} CASCADE;", nm.join)),
    ]
}

fn pick_live(g: &mut Gen) -> Option<u32> {
    let live = g.tsm.live_sets();
    if live.is_empty() {
        return None;
    }
    Some(g.tsm.sets[live[g.rng.below_usize(live.len())]].n)
}

macro_rules! need_set {
    ($g:expr) => {
        match pick_live($g) {
            Some(n) => n,
            None => {
                $g.fire("tsm:fallback:create");
                return gen_create($g);
            }
        }
    };
}

// ------------------------------------------------------------- tablesample ---

/// BERNOULLI(p)/SYSTEM(p) REPEATABLE(seed) over the main table, total ORDER
/// BY pk. Fixed seed => deterministic sampled set; compared A vs B.
fn gen_sample(g: &mut Gen, method: &str) -> Vec<StmtKind> {
    let n = need_set!(g);
    let fam = if method == "BERNOULLI" { "tsm:bernoulli" } else { "tsm:system" };
    g.fire(fam);
    let t = names(n).main;
    let p = pick(g, P_POOL);
    let seed = pick(g, SEED_POOL);
    let shape = g.weights.pick(g.rng, &["tsm:samp:plain", "tsm:samp:where", "tsm:samp:proj"]);
    g.fire(shape);
    let sql = match shape {
        "tsm:samp:where" => format!(
            "SELECT pk, a FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) \
             WHERE a > 10 ORDER BY pk;"
        ),
        "tsm:samp:proj" => format!(
            "SELECT pk, a, b, txt FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) \
             ORDER BY pk;"
        ),
        _ => format!(
            "SELECT pk FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) ORDER BY pk;"
        ),
    };
    one(sql)
}

/// Same-seed determinism: the symmetric difference of two identically-seeded
/// samples must be empty on BOTH engines (count 0). Catches a REPEATABLE
/// argument re-evaluated differently across scans.
fn gen_sameseed(g: &mut Gen) -> Vec<StmtKind> {
    let n = need_set!(g);
    g.fire("tsm:sameseed");
    let t = names(n).main;
    let method = if g.rng.chance(1, 2) { "BERNOULLI" } else { "SYSTEM" };
    let p = pick(g, P_POOL);
    let seed = pick(g, SEED_POOL);
    // Symmetric difference via two EXCEPTs; count must be 0 (deterministic
    // scalar, no ORDER BY needed).
    let sql = format!(
        "SELECT count(*) FROM (\
         (SELECT pk FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) \
         EXCEPT SELECT pk FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed})) \
         UNION ALL \
         (SELECT pk FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) \
         EXCEPT SELECT pk FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}))) d;"
    );
    one(sql)
}

/// TABLESAMPLE on a join input.
fn gen_join(g: &mut Gen) -> Vec<StmtKind> {
    let n = need_set!(g);
    g.fire("tsm:join");
    let nm = names(n);
    let (t, j) = (&nm.main, &nm.join);
    let method = if g.rng.chance(1, 2) { "BERNOULLI" } else { "SYSTEM" };
    let p = pick(g, P_POOL);
    let seed = pick(g, SEED_POOL);
    let sql = format!(
        "SELECT s.pk, j.pk FROM {t} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) s \
         JOIN {j} j ON s.a = j.a ORDER BY s.pk, j.pk;"
    );
    one(sql)
}

/// TABLESAMPLE on an old-style inheritance parent (parent + child rows).
fn gen_inherit(g: &mut Gen) -> Vec<StmtKind> {
    let n = need_set!(g);
    g.fire("tsm:inherit");
    let nm = names(n);
    let parent = &nm.parent;
    let method = if g.rng.chance(1, 2) { "BERNOULLI" } else { "SYSTEM" };
    let p = pick(g, P_POOL);
    let seed = pick(g, SEED_POOL);
    // ONLY restricts to the parent's own heap; without ONLY the sample is
    // applied to the parent relation (inheritance children are separate
    // append members and are not themselves sampled by the parent's
    // TABLESAMPLE) — either way both engines must agree.
    let only = if g.rng.chance(1, 2) { "ONLY " } else { "" };
    let sql = format!(
        "SELECT pk FROM {only}{parent} TABLESAMPLE {method} ({p}) REPEATABLE ({seed}) ORDER BY pk;"
    );
    one(sql)
}

// -------------------------------------------------------------- nodeLimit ---

fn gen_limit(g: &mut Gen) -> Vec<StmtKind> {
    let n = need_set!(g);
    g.fire("tsm:limit");
    let t = names(n).main;
    let shape = g.weights.pick(
        g.rng,
        &[
            "tsm:limit:only",
            "tsm:limit:offset",
            "tsm:limit:both",
            "tsm:limit:all",
            "tsm:limit:offend",
            "tsm:limit:fetch",
            "tsm:limit:fetchoff",
            "tsm:limit:ties",
            "tsm:limit:param",
        ],
    );
    g.fire(shape);
    match shape {
        "tsm:limit:only" => {
            let k = count(g);
            one(format!("SELECT pk, a FROM {t} ORDER BY pk LIMIT {k};"))
        }
        "tsm:limit:offset" => {
            let m = count(g);
            one(format!("SELECT pk, a FROM {t} ORDER BY pk OFFSET {m};"))
        }
        "tsm:limit:both" => {
            let k = count(g);
            let m = count(g);
            one(format!("SELECT pk, a FROM {t} ORDER BY pk LIMIT {k} OFFSET {m};"))
        }
        "tsm:limit:all" => {
            let m = count(g);
            one(format!("SELECT pk, a FROM {t} ORDER BY pk LIMIT ALL OFFSET {m};"))
        }
        // OFFSET past the end => empty result on both engines.
        "tsm:limit:offend" => {
            one(format!("SELECT pk, a FROM {t} ORDER BY pk OFFSET {};", MAIN_ROWS + 5000))
        }
        "tsm:limit:fetch" => {
            let k = 1 + count(g);
            one(format!("SELECT pk, a FROM {t} ORDER BY pk FETCH FIRST {k} ROWS ONLY;"))
        }
        "tsm:limit:fetchoff" => {
            let k = 1 + count(g);
            let m = count(g);
            one(format!(
                "SELECT pk, a FROM {t} ORDER BY pk OFFSET {m} ROWS FETCH NEXT {k} ROWS ONLY;"
            ))
        }
        // WITH TIES: the inner ORDER BY is on a non-unique key (a), so the
        // returned SET is all rows tying with the boundary row's a-value —
        // deterministic as a set. The outer total ORDER BY (pk) makes the
        // compared stream order-stable regardless of intra-tie order.
        "tsm:limit:ties" => {
            let k = 1 + count(g);
            one(format!(
                "SELECT pk, a FROM (SELECT pk, a FROM {t} ORDER BY a FETCH FIRST {k} ROWS WITH TIES) s \
                 ORDER BY pk;"
            ))
        }
        // LIMIT with bind parameters through PREPARE/EXECUTE (the
        // limit_needed / parameterized-bound path).
        _ => {
            let pn = g.tsm.next_prep;
            g.tsm.next_prep += 1;
            let name = format!("fz_tsm_lp_{pn}");
            let k = count(g);
            let m = count(g);
            vec![
                StmtKind::Raw(format!(
                    "PREPARE {name} (int, int) AS SELECT pk, a FROM {t} ORDER BY pk LIMIT $1 OFFSET $2;"
                )),
                StmtKind::Raw(format!("EXECUTE {name} ({k}, {m});")),
                StmtKind::Raw(format!("DEALLOCATE {name};")),
            ]
        }
    }
}

// ------------------------------------------------------------ DISTINCT ON ---

fn gen_distincton(g: &mut Gen) -> Vec<StmtKind> {
    let n = need_set!(g);
    g.fire("tsm:distincton");
    let t = names(n).main;
    let shape = g.weights.pick(
        g.rng,
        &["tsm:don:single", "tsm:don:multi", "tsm:don:desc", "tsm:don:expr"],
    );
    g.fire(shape);
    let sql = match shape {
        // First row per a-group (lowest pk).
        "tsm:don:single" => {
            format!("SELECT DISTINCT ON (a) pk, a, b FROM {t} ORDER BY a, pk;")
        }
        // First row per (a,b) group.
        "tsm:don:multi" => {
            format!("SELECT DISTINCT ON (a, b) pk, a, b FROM {t} ORDER BY a, b, pk;")
        }
        // Highest pk per a-group (DESC tiebreak).
        "tsm:don:desc" => {
            format!("SELECT DISTINCT ON (a) pk, a FROM {t} ORDER BY a, pk DESC;")
        }
        // DISTINCT ON an expression with a matching leading ORDER BY.
        _ => format!("SELECT DISTINCT ON (a % 3) pk, a FROM {t} ORDER BY a % 3, pk;"),
    };
    one(sql)
}

// -------------------------------------------------------------- error arms ---

/// Error arms — compared by SQLSTATE. Each is a standalone statement (no
/// bracket to poison). A divergence here (one engine errors, the other
/// succeeds or panics) is a real finding.
fn gen_err(g: &mut Gen) -> Vec<StmtKind> {
    let n = need_set!(g);
    g.fire("tsm:err");
    let nm = names(n);
    let t = &nm.main;
    let arm = g.weights.pick(
        g.rng,
        &[
            "tsm:err:range",
            "tsm:err:nontable",
            "tsm:err:repvar",
            "tsm:err:distinct",
            "tsm:err:neglimit",
        ],
    );
    g.fire(arm);
    let sql = match arm {
        // Sample percentage out of [0,100] => ERRCODE 2202H.
        "tsm:err:range" => {
            let bad = match g.rng.below(3) {
                0 => "-1",
                1 => "101",
                _ => "150.5",
            };
            let method = if g.rng.chance(1, 2) { "BERNOULLI" } else { "SYSTEM" };
            format!("SELECT pk FROM {t} TABLESAMPLE {method} ({bad}) REPEATABLE (1) ORDER BY pk;")
        }
        // TABLESAMPLE on a non-table (subquery alias) => rejected in
        // transformRangeTableSample.
        "tsm:err:nontable" => {
            format!(
                "SELECT s.pk FROM (SELECT pk FROM {t}) s TABLESAMPLE BERNOULLI (10) REPEATABLE (1);"
            )
        }
        // REPEATABLE referencing a table column (the bug-129 re-evaluation
        // class): a Var in the REPEATABLE argument must be rejected, never
        // reachable at execution.
        "tsm:err:repvar" => {
            format!("SELECT pk FROM {t} TABLESAMPLE BERNOULLI (10) REPEATABLE (pk) ORDER BY pk;")
        }
        // DISTINCT ON not matching the initial ORDER BY.
        "tsm:err:distinct" => {
            format!("SELECT DISTINCT ON (a) pk FROM {t} ORDER BY pk;")
        }
        // Negative LIMIT / OFFSET.
        _ => {
            if g.rng.chance(1, 2) {
                format!("SELECT pk FROM {t} ORDER BY pk LIMIT -1;")
            } else {
                format!("SELECT pk FROM {t} ORDER BY pk OFFSET -1;")
            }
        }
    };
    one(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Drive the module `iters` times, threading ONE persistent
    /// TablesampleState across calls (as the session loop does), so the
    /// fixture survives and the query families actually fire.
    fn drain(seed: u64, iters: usize, w: &WeightTable) -> Vec<String> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut st = TablesampleState::new();
        let mut out = Vec::new();
        for _ in 0..iters {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 4);
            std::mem::swap(&mut g.tsm, &mut st);
            let kinds = gen_tablesample_module(&mut g);
            std::mem::swap(&mut g.tsm, &mut st);
            for s in kinds {
                out.push(s.to_sql());
            }
        }
        out
    }

    #[test]
    fn stream_is_seed_deterministic() {
        let w = WeightTable::defaults();
        assert_eq!(drain(0x7501, 200, &w), drain(0x7501, 200, &w));
    }

    #[test]
    fn every_statement_is_well_formed() {
        let w = WeightTable::defaults();
        for s in drain(0x7502, 600, &w) {
            assert!(s.ends_with(';'), "unterminated: {s}");
            assert!(!s.contains('\n'), "multi-line: {s}");
            assert_eq!(s.matches('(').count(), s.matches(')').count(), "unbalanced: {s}");
        }
    }

    /// With a persistent live fixture every family fires and emits the
    /// load-bearing keywords.
    #[test]
    fn families_reach_key_surfaces() {
        let w = WeightTable::defaults();
        let all = drain(0x7503, 3000, &w).join("\n");
        for kw in [
            "TABLESAMPLE",
            "BERNOULLI",
            "SYSTEM",
            "REPEATABLE",
            "FETCH FIRST",
            "WITH TIES",
            "LIMIT ALL",
            "DISTINCT ON",
            "PREPARE",
            "REPEATABLE (pk)",
            "INHERITS",
        ] {
            assert!(all.contains(kw), "family keyword never emitted: {kw}");
        }
    }
}
