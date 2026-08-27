//! Plan-cache / prepared-statement drain module (PLANCACHE): the
//! generic-vs-custom plan machinery of plancache.c (GetCachedPlan,
//! choose_custom_plan, cached_plan_cost — the "5 custom plans then evaluate
//! a generic plan" threshold), prepare.c (PREPARE/EXECUTE/DEALLOCATE with
//! $-params) and the `plan_cache_mode` GUC (force_generic_plan /
//! force_custom_plan). The cursor module (C1) already drains the *shape*
//! surface of PREPARE/EXECUTE and witnesses the custom->generic flip with
//! EXPLAIN EXECUTE; this module drains the part that shape-only probing
//! cannot reach:
//!
//!   - the RESULT of a prepared query must be identical whether the
//!     plancache picks a custom plan or a generic plan. `plan_cache_mode`
//!     lets us FORCE each choice on the same statement with the same
//!     arguments; a divergence between `force_generic_plan` and
//!     `force_custom_plan` on one query is a HIGH-severity finding (a
//!     generic-plan executor bug — most often in runtime partition pruning
//!     or in a param-bearing qual). The differential engine compares each
//!     EXECUTE against C per-statement, so a wrong generic plan on the
//!     pgrust side surfaces as an A/B divergence under the forcing bracket.
//!   - generic-plan RUNTIME partition pruning (execPartition.c
//!     ExecInitPartitionPruning / ExecFindMatchingSubPlans): a prepared
//!     query over a range-partitioned table with a `$1` partition-key qual
//!     prunes at PLAN time under a custom plan but at RUN time under a
//!     generic plan. We force the generic plan and EXECUTE with a spread of
//!     argument values (each selecting a different partition, a boundary,
//!     and the empty tail) so the runtime-pruning arm is exercised across
//!     its cases.
//!   - plancache INVALIDATION (PlanCacheRelCallback + RevalidateCachedQuery
//!     + replan): PREPARE, warm to a generic plan, then inside a
//!     BEGIN..ROLLBACK bracket run DDL that invalidates the cached plan
//!     (CREATE INDEX, ALTER TABLE ADD COLUMN) and re-EXECUTE — the plan is
//!     rebuilt and must return the same rows; the ROLLBACK itself
//!     invalidates again, and the post-rollback EXECUTE must replan against
//!     the original relation.
//!   - pg_prepared_statements introspection (prepare.c
//!     pg_prepared_statement SRF): name / statement / parameter_types /
//!     result_types / from_sql plus the generic_plans / custom_plans
//!     decision counters, read back under a fixed execution count.
//!
//! Determinism discipline (same laws as crate::plansel / crate::opt2):
//!   - fully SELF-CONTAINED groups: every fixture is created with a
//!     session-unique name (a `PlanCacheState` counter, like crate::plpg),
//!     used, and dropped in the same group; every prepared statement is
//!     DEALLOCATEd in the same group. Nothing persists across groups except
//!     the name counter, so a replayed name can never denote two objects
//!     and a group can never collide with a sibling module's fixtures.
//!   - EXECUTE results are compared as multisets (the EXECUTE text carries
//!     no ORDER BY), so row content — not plan-shaped order — is the
//!     oracle; every prepared SELECT still carries an internal ORDER BY so
//!     the underlying order is deterministic too. Aggregate probes use
//!     exact int8 accumulation (no float aggregates, B1).
//!   - every `SET plan_cache_mode` has its `RESET` in the same group;
//!     invalidation DDL and any prepared write run only inside
//!     BEGIN..ROLLBACK, so persistent table state never changes.
//!   - all data are pure integer formulas of the row number — identical on
//!     both engines by construction; tables stay <= 1200 rows (exhaustive
//!     under the default 30000-row ANALYZE sample => identical stats).

use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counter (objects are group-local; only the
/// counter persists — the crate::plpg pattern). Guarantees every fixture
/// and prepared-statement name is unique across the whole session, so an
/// aborted group can never leave a name that a later group collides with.
#[derive(Clone, Debug, Default)]
pub struct PlanCacheState {
    next: u32,
}

impl PlanCacheState {
    pub fn new() -> PlanCacheState {
        PlanCacheState::default()
    }
}

const PLAIN_ROWS: i64 = 500;
const PART_ROWS: i64 = 1199; // pk 1..1199, all inside [0,1200)

/// Group-unique object names.
struct Names {
    b: String,   // plain multi-index table
    p: String,   // range-partitioned table (key = pk)
    ps: String,  // prepared select
    pa: String,  // prepared aggregate
    pp: String,  // prepared over the partitioned table
}

fn names(n: u32) -> Names {
    Names {
        b: format!("fz_pc_b_{n}"),
        p: format!("fz_pc_p_{n}"),
        ps: format!("fzpc_{n}_s"),
        pa: format!("fzpc_{n}_a"),
        pp: format!("fzpc_{n}_p"),
    }
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

/// Take the next session-unique group id.
fn next_id(g: &mut Gen) -> u32 {
    let n = g.plancache.next;
    g.plancache.next += 1;
    n
}

// ------------------------------------------------------------- fixtures ---

/// CREATE + load + index + ANALYZE the plain multi-index table.
fn create_plain(nm: &Names) -> Vec<StmtKind> {
    let b = &nm.b;
    vec![
        raw(format!(
            "CREATE TABLE {b} (pk int4 PRIMARY KEY, a int4, b int4, c int4, \
             flag bool, txt text) WITH (autovacuum_enabled = off);"
        )),
        raw(format!(
            "INSERT INTO {b} SELECT i, (i * 17) % 200, (i * 7) % 50, (i * 13) % 1000, \
             i % 3 = 0, 'p' || ((i * 23) % 211) FROM generate_series(1, {PLAIN_ROWS}) i;"
        )),
        raw(format!("CREATE INDEX {b}_a ON {b} (a);")),
        raw(format!("CREATE INDEX {b}_ab ON {b} (a, b);")),
        raw(format!("CREATE INDEX {b}_cf ON {b} (c) WHERE flag;")),
        raw(format!("ANALYZE {b};")),
    ]
}

/// CREATE + load + index + ANALYZE the range-partitioned table (key = pk,
/// four equal 300-wide partitions over [0,1200)).
fn create_part(nm: &Names) -> Vec<StmtKind> {
    let p = &nm.p;
    let mut v = vec![raw(format!(
        "CREATE TABLE {p} (pk int4 PRIMARY KEY, a int4, b int4, txt text) \
         PARTITION BY RANGE (pk);"
    ))];
    for (i, (lo, hi)) in [(0, 300), (300, 600), (600, 900), (900, 1200)].iter().enumerate() {
        v.push(raw(format!(
            "CREATE TABLE {p}_c{i} PARTITION OF {p} FOR VALUES FROM ({lo}) TO ({hi}) \
             WITH (autovacuum_enabled = off);"
        )));
    }
    v.push(raw(format!(
        "INSERT INTO {p} SELECT i, (i * 5) % 120, (i * 11) % 60, 'q' || (i % 53) \
         FROM generate_series(1, {PART_ROWS}) i;"
    )));
    v.push(raw(format!("CREATE INDEX {p}_a ON {p} (a);")));
    v.push(raw(format!("ANALYZE {p};")));
    v
}

// ------------------------------------------------------------- dispatch ---

const SHAPES: &[&str] = &[
    "plancache:mode",
    "plancache:transition",
    "plancache:prune",
    "plancache:invalidate",
    "plancache:introspect",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_plancache_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plancache");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    match shape {
        "plancache:mode" => gen_mode(g),
        "plancache:transition" => gen_transition(g),
        "plancache:prune" => gen_prune(g),
        "plancache:invalidate" => gen_invalidate(g),
        _ => gen_introspect(g),
    }
}

/// Wrap one or more EXECUTEs of `stmt`(args) in a `SET plan_cache_mode`
/// bracket, RESET in the same group.
fn mode_bracket(mode: &str, execs: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![raw(format!("SET plan_cache_mode = {mode};"))];
    for e in execs {
        v.push(raw(e));
    }
    v.push(raw("RESET plan_cache_mode;".to_string()));
    v
}

// ----------------------------------------------------------------- mode ---

/// force_generic vs force_custom RESULT identity on the plain table: the
/// same prepared statement is EXECUTEd with the SAME arguments under both
/// forced plan choices (and under the default `auto`). Any A/B divergence
/// under either forcing is a HIGH generic/custom-plan finding.
fn gen_mode(g: &mut Gen) -> Vec<StmtKind> {
    let n = next_id(g);
    let nm = names(n);
    let b = &nm.b;
    let mut out = create_plain(&nm);

    // The prepared query: one of several $-param shapes, all pure functions
    // of table state (result identical across every plan choice).
    let qshape = g.weights.pick(
        g.rng,
        &[
            "plancache:q:rows",
            "plancache:q:agg",
            "plancache:q:partial",
            "plancache:q:in",
        ],
    );
    g.fire(qshape);
    let (decl, nargs, agg): (String, usize, bool) = match qshape {
        // Row set: index-range on a, ordered internally.
        "plancache:q:rows" => (
            format!(
                "PREPARE {ps}(int4, int4) AS SELECT pk, a, b FROM {b} \
                 WHERE a >= $1 AND a < $2 ORDER BY pk;",
                ps = nm.ps
            ),
            2,
            false,
        ),
        // Exact-typed aggregate (accumulation-order independent).
        "plancache:q:agg" => (
            format!(
                "PREPARE {ps}(int4) AS SELECT count(*), sum(pk::int8), min(b), max(c) \
                 FROM {b} WHERE a > $1;",
                ps = nm.ps
            ),
            1,
            true,
        ),
        // Partial-index-sensitive: the $1 qual over the WHERE-flag partial
        // index; a generic plan must still honour the predicate proof.
        "plancache:q:partial" => (
            format!(
                "PREPARE {ps}(int4) AS SELECT count(*), sum(c::int8) FROM {b} \
                 WHERE flag AND c < $1;",
                ps = nm.ps
            ),
            1,
            true,
        ),
        // SAOP over a $-param scalar-array (ANY): a generic plan keeps the
        // array as a parameter (no per-value plan-time expansion).
        _ => (
            format!(
                "PREPARE {ps}(int4) AS SELECT pk, a FROM {b} \
                 WHERE a = ANY (ARRAY[$1, $1 + 1, $1 + 7]) ORDER BY pk;",
                ps = nm.ps
            ),
            1,
            false,
        ),
    };
    out.push(raw(decl));

    // Draw two deterministic argument tuples; each is EXECUTEd under BOTH
    // forced modes (and auto) so custom and generic run identical inputs.
    let mk_args = |g: &mut Gen| -> String {
        if nargs == 2 {
            let lo = g.rng.below(180) as i64;
            format!("{}, {}", lo, lo + 5 + g.rng.below(40) as i64)
        } else if agg {
            format!("{}", g.rng.below(1000) as i64)
        } else {
            format!("{}", g.rng.below(200) as i64)
        }
    };
    let a1 = mk_args(g);
    let a2 = mk_args(g);
    for mode in ["force_custom_plan", "force_generic_plan"] {
        out.extend(mode_bracket(
            mode,
            vec![
                format!("EXECUTE {}({});", nm.ps, a1),
                format!("EXECUTE {}({});", nm.ps, a2),
            ],
        ));
    }
    // A few auto-mode executes (the default choose_custom_plan path).
    for _ in 0..1 + g.rng.below_usize(2) {
        let a = mk_args(g);
        out.push(raw(format!("EXECUTE {}({});", nm.ps, a)));
    }

    out.push(raw(format!("DEALLOCATE {};", nm.ps)));
    out.push(raw(format!("DROP TABLE {};", b)));
    out
}

// ----------------------------------------------------------- transition ---

/// The custom->generic transition itself: a single prepared statement is
/// EXECUTEd 6+ times with VARYING arguments (so the five custom plans have
/// genuinely different param values before cached_plan_cost decides the
/// generic plan is not more expensive on average at the 6th). Result of
/// every EXECUTE is compared A/B; an optional EXPLAIN witness at the flip
/// boundary shows folded params (custom) then `$n` (generic).
fn gen_transition(g: &mut Gen) -> Vec<StmtKind> {
    let n = next_id(g);
    let nm = names(n);
    let b = &nm.b;
    let mut out = create_plain(&nm);
    out.push(raw(format!(
        "PREPARE {pa}(int4) AS SELECT count(*), sum(pk::int8), max(b) FROM {b} \
         WHERE a > $1;",
        pa = nm.pa
    )));

    let explain = g.weights.pick(g.rng, &["plancache:explain", "plancache:explain:none"])
        == "plancache:explain";
    // 7 executes cross the 5-custom threshold with room to spare; each arg
    // is a distinct deterministic value.
    let nexec = 7;
    for i in 0..nexec {
        let arg = (g.rng.below(200)) as i64;
        // Witness the plan text on the first custom plan and just past the
        // flip (EXPLAIN EXECUTE without ANALYZE does not run the query).
        if explain && (i == 0 || i == nexec - 1) {
            g.fire("plancache:explain");
            out.push(raw(format!(
                "EXPLAIN (COSTS OFF, SUMMARY OFF) EXECUTE {}({});",
                nm.pa, arg
            )));
        }
        out.push(raw(format!("EXECUTE {}({});", nm.pa, arg)));
    }
    // Then pin each choice explicitly and re-run one shared arg under both,
    // so the post-transition generic plan is compared directly against the
    // forced-custom plan for identical input.
    let shared = (g.rng.below(200)) as i64;
    for mode in ["force_custom_plan", "force_generic_plan"] {
        out.extend(mode_bracket(mode, vec![format!("EXECUTE {}({});", nm.pa, shared)]));
    }

    out.push(raw(format!("DEALLOCATE {};", nm.pa)));
    out.push(raw(format!("DROP TABLE {};", b)));
    out
}

// ---------------------------------------------------------------- prune ---

/// Generic-plan RUNTIME partition pruning: force the generic plan on a
/// prepared query over the range-partitioned table and EXECUTE with a
/// spread of partition-key arguments (each landing in a different
/// partition, on a boundary, and off the end into the empty tail). Under a
/// generic plan the Append prunes at run time; the row set must match the
/// custom (plan-time-pruned) plan and C.
fn gen_prune(g: &mut Gen) -> Vec<StmtKind> {
    let n = next_id(g);
    let nm = names(n);
    let p = &nm.p;
    let mut out = create_part(&nm);

    let kind = g.weights.pick(g.rng, &["plancache:prune:range", "plancache:prune:eq"]);
    g.fire(kind);
    let (decl, args): (String, Vec<i64>) = if kind == "plancache:prune:range" {
        // Half-open window of width 250: prunes to 1-2 partitions; the
        // 1200 arg selects the empty tail.
        (
            format!(
                "PREPARE {pp}(int4) AS SELECT pk, a FROM {p} \
                 WHERE pk >= $1 AND pk < $1 + 250 ORDER BY pk;",
                pp = nm.pp
            ),
            vec![0, 150, 300, 600, 875, 900, 1100, 1200],
        )
    } else {
        // Equality on the partition key: prunes to exactly one partition
        // (or none for the off-the-end value).
        (
            format!(
                "PREPARE {pp}(int4) AS SELECT count(*), sum(a::int8), min(b) FROM {p} \
                 WHERE pk = $1;",
                pp = nm.pp
            ),
            vec![1, 299, 300, 601, 900, 1199, 5000],
        )
    };
    out.push(raw(decl));

    // The forced-generic sweep is the primary runtime-pruning oracle; a
    // forced-custom sweep over the same args is the plan-time-pruning
    // control (both must equal C and each other).
    let exec_args: Vec<String> =
        args.iter().map(|a| format!("EXECUTE {}({});", nm.pp, a)).collect();
    out.extend(mode_bracket("force_generic_plan", exec_args.clone()));
    out.extend(mode_bracket("force_custom_plan", exec_args));

    out.push(raw(format!("DEALLOCATE {};", nm.pp)));
    out.push(raw(format!("DROP TABLE {};", p)));
    out
}

// ----------------------------------------------------------- invalidate ---

/// plancache invalidation on DDL: PREPARE, warm to a generic plan, then run
/// invalidating DDL inside a BEGIN..ROLLBACK bracket and re-EXECUTE. The
/// cached plan must be rebuilt (RevalidateCachedQuery) and return identical
/// rows; the ROLLBACK invalidates again and the post-rollback EXECUTE must
/// replan against the original relation. Table state is unchanged (all DDL
/// is rolled back).
fn gen_invalidate(g: &mut Gen) -> Vec<StmtKind> {
    let n = next_id(g);
    let nm = names(n);
    let b = &nm.b;
    let mut out = create_plain(&nm);
    out.push(raw(format!(
        "PREPARE {ps}(int4) AS SELECT count(*), sum(pk::int8), min(a) FROM {b} \
         WHERE b >= $1;",
        ps = nm.ps
    )));

    let arg = (g.rng.below(50)) as i64;
    // Warm past the transition so a generic plan is cached before we
    // invalidate it.
    out.extend(mode_bracket(
        "force_generic_plan",
        (0..6).map(|_| format!("EXECUTE {}({});", nm.ps, arg)).collect(),
    ));

    // Invalidating DDL inside a rolled-back bracket: each ALTER/CREATE
    // INDEX fires the relcache invalidation callback that marks the cached
    // plan stale; the following EXECUTE must replan and still match.
    out.push(raw("BEGIN;".to_string()));
    out.push(raw(format!("CREATE INDEX {b}_binv ON {b} (b);")));
    out.push(raw(format!("EXECUTE {}({});", nm.ps, arg)));
    out.push(raw(format!("ALTER TABLE {b} ADD COLUMN zpad int DEFAULT 7;")));
    out.push(raw(format!("EXECUTE {}({});", nm.ps, arg)));
    // A generic-plan re-execute under the altered relation too.
    out.extend(mode_bracket(
        "force_generic_plan",
        vec![format!("EXECUTE {}({});", nm.ps, arg)],
    ));
    out.push(raw("ROLLBACK;".to_string()));

    // Post-rollback: the rollback invalidated the plan again; replan
    // against the original relation, same result.
    out.push(raw(format!("EXECUTE {}({});", nm.ps, arg)));

    out.push(raw(format!("DEALLOCATE {};", nm.ps)));
    out.push(raw(format!("DROP TABLE {};", b)));
    out
}

// ----------------------------------------------------------- introspect ---

/// pg_prepared_statements introspection: PREPARE two statements, EXECUTE
/// each a fixed number of times under a known plan_cache_mode (so the
/// generic_plans / custom_plans counters are deterministic), then read the
/// catalog SRF back with a total order. Filtered to this group's names so a
/// sibling module's pooled prepared statements never leak in.
fn gen_introspect(g: &mut Gen) -> Vec<StmtKind> {
    let n = next_id(g);
    let nm = names(n);
    let b = &nm.b;
    let mut out = create_plain(&nm);
    out.push(raw(format!(
        "PREPARE {ps}(int4) AS SELECT pk, a FROM {b} WHERE a >= $1 ORDER BY pk;",
        ps = nm.ps
    )));
    out.push(raw(format!(
        "PREPARE {pa}(int4, int4) AS SELECT count(*), sum(pk::int8) FROM {b} \
         WHERE b >= $1 AND c < $2;",
        pa = nm.pa
    )));

    // Deterministic counter states: force custom on ps (custom_plans bumps,
    // generic_plans stays 0) and force generic on pa (generic_plans bumps).
    let arg = (g.rng.below(50)) as i64;
    out.extend(mode_bracket(
        "force_custom_plan",
        (0..3).map(|_| format!("EXECUTE {}({});", nm.ps, arg)).collect(),
    ));
    out.extend(mode_bracket(
        "force_generic_plan",
        (0..3).map(|_| format!("EXECUTE {}({}, {});", nm.pa, arg, 900 + arg)).collect(),
    ));

    // The SRF read-back: every column here is a deterministic function of
    // the PREPARE text and the fixed execution counts above.
    out.push(raw(format!(
        "SELECT name, statement, parameter_types::text, result_types::text, \
         from_sql, generic_plans, custom_plans FROM pg_prepared_statements \
         WHERE name LIKE 'fzpc\\_{n}\\_%' ESCAPE '\\' ORDER BY name;"
    )));

    out.push(raw(format!("DEALLOCATE {};", nm.ps)));
    out.push(raw(format!("DEALLOCATE {};", nm.pa)));
    out.push(raw(format!("DROP TABLE {};", b)));
    out
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
        let mut state = PlanCacheState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            std::mem::swap(&mut g.plancache, &mut state);
            let stmts: Vec<String> =
                gen_plancache_module(&mut g).iter().map(|s| s.to_sql()).collect();
            std::mem::swap(&mut g.plancache, &mut state);
            out.push(stmts);
        }
        out
    }

    #[test]
    fn plan_cache_mode_set_reset_balanced() {
        // Every `SET plan_cache_mode` has a matching `RESET plan_cache_mode`
        // in the same group.
        for group in gen_groups(11, 400) {
            let mut open = 0i32;
            for sql in &group {
                if sql.starts_with("SET plan_cache_mode") {
                    open += 1;
                } else if sql == "RESET plan_cache_mode;" {
                    open -= 1;
                    assert!(open >= 0, "RESET without SET in {group:?}");
                }
            }
            assert_eq!(open, 0, "unbalanced plan_cache_mode SET/RESET in {group:?}");
        }
    }

    #[test]
    fn prepared_statements_are_deallocated() {
        // Every PREPARE in a group has a matching DEALLOCATE of that name in
        // the same group (self-contained: no prepared statement escapes).
        for group in gen_groups(23, 400) {
            let mut prepared: Vec<String> = Vec::new();
            for sql in &group {
                if let Some(rest) = sql.strip_prefix("PREPARE ") {
                    let name = rest
                        .split(|c| c == '(' || c == ' ')
                        .next()
                        .unwrap()
                        .to_string();
                    prepared.push(name);
                } else if let Some(rest) = sql.strip_prefix("DEALLOCATE ") {
                    let name = rest.trim_end_matches(';').to_string();
                    if let Some(pos) = prepared.iter().position(|p| *p == name) {
                        prepared.remove(pos);
                    }
                }
            }
            assert!(prepared.is_empty(), "un-deallocated {prepared:?} in {group:?}");
        }
    }

    /// Round-18a blanket rule (exd RB-15 lineage): every storage-bearing
    /// plancache CREATE TABLE pins autovacuum_enabled = off — the module's
    /// surface is compared EXPLAIN (COSTS OFF, SUMMARY OFF) EXECUTE plan
    /// shape, so fixture stats must change only via the group's own
    /// ANALYZE. Partitioned parents are exempt (no storage).
    #[test]
    fn creates_pin_autovacuum_off() {
        let mut seen = 0;
        for grp in gen_groups(0x18A, 400) {
            for sql in grp {
                if !sql.starts_with("CREATE TABLE ") || sql.contains(" PARTITION BY ") {
                    continue;
                }
                assert!(
                    sql.contains("autovacuum_enabled = off"),
                    "plancache fixture does not pin autovacuum off: `{sql}`"
                );
                seen += 1;
            }
        }
        assert!(seen > 0, "no CREATE TABLE generated in 400 groups");
    }

    #[test]
    fn fixtures_are_dropped() {
        // Every CREATE TABLE has a matching DROP TABLE in the same group
        // (partition children go with their parent's DROP).
        for group in gen_groups(29, 400) {
            let mut created: Vec<String> = Vec::new();
            for sql in &group {
                if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                    let name = rest.split(' ').next().unwrap().to_string();
                    // Partition children are dropped implicitly with parent.
                    if !name.contains("_c") {
                        created.push(name);
                    }
                } else if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                    let name = rest.trim_end_matches(';').to_string();
                    if let Some(pos) = created.iter().position(|c| *c == name) {
                        created.remove(pos);
                    }
                }
            }
            assert!(created.is_empty(), "un-dropped {created:?} in {group:?}");
        }
    }

    #[test]
    fn invalidation_ddl_only_inside_rollback() {
        // The ALTER / index-invalidation DDL that mutates a live fixture
        // runs only inside a BEGIN..ROLLBACK bracket, so persistent table
        // state never changes.
        for group in gen_groups(37, 400) {
            let mut open = 0i32;
            for sql in &group {
                if sql == "BEGIN;" {
                    open += 1;
                } else if sql == "ROLLBACK;" {
                    open -= 1;
                }
                let mutates_live = sql.starts_with("ALTER TABLE ")
                    || (sql.starts_with("CREATE INDEX ") && sql.contains("_binv"));
                if mutates_live {
                    assert!(open > 0, "invalidation DDL outside rollback: {sql}");
                }
            }
            assert_eq!(open, 0, "unclosed BEGIN in {group:?}");
        }
    }

    #[test]
    fn deterministic_per_seed() {
        assert_eq!(gen_groups(5, 80), gen_groups(5, 80));
    }

    #[test]
    fn prepared_names_are_session_unique() {
        // The persistent counter makes every prepared-statement name unique
        // across the whole session (no two groups share a name).
        let mut seen = std::collections::HashSet::new();
        for group in gen_groups(41, 200) {
            for sql in &group {
                if let Some(rest) = sql.strip_prefix("PREPARE ") {
                    let name =
                        rest.split(|c| c == '(' || c == ' ').next().unwrap().to_string();
                    assert!(seen.insert(name.clone()), "duplicate prepared name {name}");
                }
            }
        }
    }
}
