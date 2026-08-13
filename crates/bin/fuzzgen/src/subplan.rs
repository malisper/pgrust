//! SubPlan / InitPlan execution drain (SUBPLAN-1 bug class): drive the
//! nodeSubplan.c / subselect.c / execExpr SubPlan-opcode surface (hollow
//! cluster in docs/fuzzing/hollow-lines-007.tsv — ExecScanSubPlan 19,
//! ExecSetParamPlan / buildSubPlanHash / ExecHashSubPlan / InitPlan arms,
//! build_subplan, convert_{EXISTS,VALUES,ANY}_to_ANY/join, hash_ok_operator)
//! by placing a subquery-that-becomes-a-SubPlan-or-InitPlan into EVERY
//! expression context the planner can hang one off, then comparing result
//! identity AND error identity against C.
//!
//! Why a dedicated module: the standing `subq` module builds subqueries
//! through the AST expression generator, so a SubPlan only ever lands where
//! that generator drops one (SELECT-list / WHERE / a scalar-cmp RHS) and
//! only the common non-hashed correlated arm is exercised. The executor
//! differential oracle measured this surface at 25% and the CI cluster close-out
//! banked SUBPLAN-1 — an UNCORRELATED scalar subquery in HAVING raised a
//! pgrust XX000 where C returns a normal result. That is the whole quarry
//! here: ANY valid query where pgrust raises XX000 / execExpr /
//! internal-error while C returns a normal result or a normal SQLSTATE is a
//! HIGH-severity find of the SUBPLAN-1 family.
//!
//! Contexts driven (one probe family each): scalar subquery in SELECT-list,
//! WHERE, HAVING, GROUP BY, ORDER BY, CASE, function argument, VALUES,
//! LIMIT/OFFSET; correlated subquery at nesting depths 1-3 (SubPlan inside
//! SubPlan); hashed `= ANY (SELECT ...)` (uncorrelated, hashable — the
//! buildSubPlanHash / ExecHashSubPlan arm) vs non-hashed (correlated,
//! rescanned — the ExecScanSubPlan arm); `= ANY` / `= ALL` / `<> ALL` /
//! `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` sublinks; uncorrelated
//! InitPlan (once-evaluated, incl. multi-param row-expression InitPlans);
//! multi-param SubPlan referencing several outer columns; SubPlan inside an
//! aggregate argument; and SubPlan under a Gather (forced parallel).
//!
//! Determinism discipline (LD5 / B1 laws — this module emits raw
//! self-contained statements, so it owns its own compare-safety):
//!   - every probe returns EITHER a single aggregated scalar row OR a
//!     multi-row result carrying a TOTAL ORDER BY ending in the unique pk;
//!     heap/scan order is never a surface.
//!   - every scalar subquery is single-row by construction: an aggregate
//!     (count/max/min/sum/avg-over-int), a FROM-less SELECT, or an
//!     `ORDER BY <unique> LIMIT 1`. No probe can raise "more than one row".
//!   - integer / numeric surfaces only; no float reassociation surface (B1).
//!   - the fixture is 300 rows, far under the 30000-row statistics sample,
//!     so ANALYZE is exhaustive and plans are stats-identical on both sides.
//!   - every SET has its RESET in the same group; every group creates and
//!     drops every fz_sp*-prefixed object it touches.
//!   - forced-parallel probes return a single deterministic aggregate: the
//!     Gather worker split is launch-time state (Q1 ruling) and is never a
//!     surface, only the SubPlan value under it is.
//!
//! NOT a subquery-in-DEFAULT / CHECK constraint probe: Postgres rejects
//! subqueries in DEFAULT and CHECK expressions at DDL time (both sides,
//! same SQLSTATE) — those are a shared parse-error surface, not a SubPlan
//! execution surface, so the "DEFAULT/CHECK-shaped" InitPlan intent is
//! served here by uncorrelated once-evaluated InitPlans in executable
//! expression positions (WHERE / LIMIT / target list / row-comparison).

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

const SHAPES: &[&str] = &[
    "subplan:ctx",
    "subplan:sublink",
    "subplan:corr",
    "subplan:initplan",
    "subplan:agg",
    "subplan:gather",
];

/// Main scan/probe fixture: 300 rows, pk PK plus three secondary integer
/// columns with deterministic modular formulas (bounded distinct counts so
/// GROUP BY / hashed-SubPlan set sizes are small and stable), a low-null `a`
/// column, and a C-collated text column. ANALYZE'd (n << stats sample).
fn fixture(name: &str) -> Vec<StmtKind> {
    vec![
        raw(format!(
            "CREATE TABLE {name} (pk int PRIMARY KEY, a int, b int, c int, t text COLLATE \"C\");"
        )),
        raw(format!(
            "INSERT INTO {name} SELECT i, CASE WHEN i%97=0 THEN NULL ELSE (i*13)%50 END, \
             (i*7)%17, (i*11)%9, 'r'||((i*23)%40) FROM generate_series(1,300) i;"
        )),
        raw(format!("CREATE INDEX {name}_a ON {name}(a);")),
        raw(format!("CREATE INDEX {name}_b ON {name}(b);")),
        raw(format!("ANALYZE {name};")),
    ]
}

/// Small dimension table for sublink RHS sets: 20 rows, dk 1..20, dv a
/// bounded modular value (so `= ANY (SELECT dv ...)` has few distinct keys).
fn dim(name: &str) -> Vec<StmtKind> {
    vec![
        raw(format!("CREATE TABLE {name} (dk int PRIMARY KEY, dv int);")),
        raw(format!(
            "INSERT INTO {name} SELECT i, (i*3)%12 FROM generate_series(1,20) i;"
        )),
        raw(format!("ANALYZE {name};")),
    ]
}

fn drop(name: &str) -> StmtKind {
    raw(format!("DROP TABLE {name};"))
}

/// SUBPLAN-1 heartland: one uncorrelated scalar subquery (an aggregate over
/// the same fixture, so single-row and value-deterministic) placed into
/// each clause context in turn. The HAVING arm reproduces the banked
/// SUBPLAN-1 shape (uncorrelated aggregate subquery in HAVING).
fn ctx() -> Vec<StmtKind> {
    let t = "fz_sp_ctx";
    // The uncorrelated scalar subquery reused across contexts.
    let sub = format!("(SELECT avg(a)::int FROM {t})");
    // A second, differently-shaped uncorrelated scalar (max) for LIMIT/OFFSET
    // and VALUES where a small positive integer is wanted.
    let subn = format!("(SELECT count(*) / 100 FROM {t})"); // = 3
    let mut v = fixture(t);
    v.extend([
        // SELECT-list: SubPlan/InitPlan value projected next to grouped rows.
        raw(format!(
            "SELECT b, count(*), {sub} AS s FROM {t} GROUP BY b ORDER BY b;"
        )),
        // WHERE: uncorrelated -> InitPlan one-time filter.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE a > {sub};"
        )),
        // HAVING: the SUBPLAN-1 reproduction — uncorrelated aggregate
        // subquery in a HAVING qual over a grouped result.
        raw(format!(
            "SELECT b, count(*) FROM {t} GROUP BY b HAVING count(*) > {sub} ORDER BY b;"
        )),
        // HAVING against a nested uncorrelated aggregate-of-aggregate.
        raw(format!(
            "SELECT b, count(*) AS n FROM {t} GROUP BY b \
             HAVING count(*) >= (SELECT min(cnt) FROM (SELECT count(*) AS cnt FROM {t} GROUP BY c) q) \
             ORDER BY b;"
        )),
        // GROUP BY: group key IS the subquery-shifted column.
        raw(format!(
            "SELECT (a - {sub}) AS g, count(*) FROM {t} GROUP BY (a - {sub}) ORDER BY g NULLS LAST;"
        )),
        // ORDER BY: sort key references the subquery (stable — total order
        // still ends in pk).
        raw(format!(
            "SELECT pk, a FROM {t} ORDER BY abs(a - {sub}) NULLS LAST, pk LIMIT 20;"
        )),
        // CASE: subquery in both the WHEN test and a THEN arm.
        raw(format!(
            "SELECT count(*) FILTER (WHERE CASE WHEN a > {sub} THEN true ELSE false END) FROM {t};"
        )),
        // Function argument: subquery as an argument to a scalar function.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE a = least(b, {sub});"
        )),
        // VALUES: subquery inside a VALUES row scanned as a derived table.
        raw(format!(
            "SELECT x FROM (VALUES ({sub}), ({subn}), (0)) v(x) ORDER BY x NULLS LAST;"
        )),
        // LIMIT / OFFSET: subquery drives the count expressions.
        raw(format!(
            "SELECT pk FROM {t} ORDER BY pk LIMIT {subn} OFFSET {subn};"
        )),
        drop(t),
    ]);
    v
}

/// Sublink family: every ANY/ALL/IN/EXISTS spelling, and the hashed vs
/// non-hashed split. Uncorrelated hashable `= ANY (SELECT ...)` drives
/// buildSubPlanHash / ExecHashSubPlan; the correlated EXISTS/ANY arms drive
/// the rescanned ExecScanSubPlan path.
fn sublink() -> Vec<StmtKind> {
    let t = "fz_sp_sl";
    let d = "fz_sp_sld";
    let mut v = fixture(t);
    v.extend(dim(d));
    v.extend([
        // Hashed uncorrelated `= ANY (SELECT ...)`: hashable int eq, bounded
        // distinct RHS -> the hashed SubPlan arm.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE b = ANY (SELECT dv FROM {d});"
        )),
        // IN (SELECT) — same hashed arm via the IN spelling.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE b IN (SELECT dv FROM {d});"
        )),
        // NOT IN (SELECT) with a NULL-free RHS (deterministic three-valued
        // result; dv is never NULL).
        raw(format!(
            "SELECT count(*) FROM {t} WHERE b NOT IN (SELECT dv FROM {d});"
        )),
        // `= ALL (SELECT ...)` — a single-value RHS makes ALL meaningful.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE b = ALL (SELECT max(dv) FROM {d});"
        )),
        // `<> ALL (SELECT ...)` — equivalent-to-NOT-IN executor arm.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE b <> ALL (SELECT dv FROM {d});"
        )),
        // `> ANY` / `> ALL` — non-equality operators keep the SubPlan
        // non-hashable (ExecScanSubPlan comparator arm).
        raw(format!(
            "SELECT count(*) FROM {t} WHERE a > ANY (SELECT dv FROM {d});"
        )),
        raw(format!(
            "SELECT count(*) FROM {t} WHERE a > ALL (SELECT dv FROM {d});"
        )),
        // Correlated EXISTS / NOT EXISTS — rescanned per outer row.
        raw(format!(
            "SELECT count(*) FROM {t} x WHERE EXISTS (SELECT 1 FROM {d} WHERE dv = x.b);"
        )),
        raw(format!(
            "SELECT count(*) FROM {t} x WHERE NOT EXISTS (SELECT 1 FROM {d} WHERE dv = x.c);"
        )),
        // Correlated `= ANY` (references outer col) — cannot be hashed, must
        // rescan (ExecScanSubPlan) each outer row.
        raw(format!(
            "SELECT count(*) FROM {t} x WHERE x.c = ANY (SELECT dv FROM {d} WHERE dk > x.b);"
        )),
        // EXISTS in the target list as a projected boolean (SubPlan in
        // projection, not just a qual).
        raw(format!(
            "SELECT x.pk, EXISTS (SELECT 1 FROM {d} WHERE dv = x.b) AS e FROM {t} x ORDER BY x.pk LIMIT 20;"
        )),
        drop(d),
        drop(t),
    ]);
    v
}

/// Correlated SubPlan at nesting depths 1-3: a SubPlan whose body contains a
/// SubPlan whose body contains a SubPlan, each correlated to a different
/// enclosing level (SubPlan-in-SubPlan; process_sublinks_mutator recursion).
fn corr() -> Vec<StmtKind> {
    let t = "fz_sp_corr";
    let mut v = fixture(t);
    v.extend([
        // Depth 1: correlated scalar subquery in the target list.
        raw(format!(
            "SELECT x.pk, (SELECT count(*) FROM {t} y WHERE y.b = x.b) AS n \
             FROM {t} x ORDER BY x.pk LIMIT 20;"
        )),
        // Depth 2: scalar subquery whose body has a correlated scalar
        // subquery correlated to the OUTERMOST row.
        raw(format!(
            "SELECT x.pk, (SELECT max((SELECT count(*) FROM {t} z WHERE z.c = x.c AND z.b = y.b)) \
             FROM {t} y WHERE y.b = x.b) AS n FROM {t} x ORDER BY x.pk LIMIT 15;"
        )),
        // Depth 3: three nested correlated aggregates, each referencing a
        // distinct enclosing level's column.
        raw(format!(
            "SELECT x.pk FROM {t} x WHERE x.a IS NOT NULL AND x.a > \
             (SELECT avg((SELECT avg((SELECT count(*) FROM {t} w WHERE w.c = z.c) )::numeric \
             FROM {t} z WHERE z.b = y.b))::int FROM {t} y WHERE y.b = x.b) ORDER BY x.pk LIMIT 10;"
        )),
        // Correlated subquery inside a CASE inside the WHERE.
        raw(format!(
            "SELECT count(*) FROM {t} x WHERE CASE WHEN x.b > 8 \
             THEN (SELECT count(*) FROM {t} y WHERE y.c = x.c) > 30 ELSE x.a IS NULL END;"
        )),
        drop(t),
    ]);
    v
}

/// Uncorrelated InitPlan (once-evaluated) arms, including MULTI-PARAM /
/// multi-column row-expression InitPlans and an InitPlan reused in several
/// positions (the planner shares one InitPlan across references).
fn initplan() -> Vec<StmtKind> {
    let t = "fz_sp_ip";
    let mut v = fixture(t);
    v.extend([
        // Classic uncorrelated InitPlan: one-time WHERE filter.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE a >= (SELECT avg(a) FROM {t});"
        )),
        // The SAME uncorrelated subquery in two positions -> shared InitPlan.
        raw(format!(
            "SELECT (SELECT max(b) FROM {t}) AS m, count(*) FROM {t} \
             WHERE b < (SELECT max(b) FROM {t});"
        )),
        // Multi-column InitPlan: a row-valued uncorrelated subquery compared
        // as a whole row (get_first_col_type / multi-param arm).
        raw(format!(
            "SELECT count(*) FROM {t} WHERE (b, c) = (SELECT max(b), min(c) FROM {t});"
        )),
        // Row-comparison against an uncorrelated single-row subquery in the
        // target list.
        raw(format!(
            "SELECT ((SELECT min(b), max(c) FROM {t}) IS NOT NULL) AS ok;"
        )),
        // InitPlan feeding LIMIT (once-evaluated count expression).
        raw(format!(
            "SELECT pk FROM {t} ORDER BY pk LIMIT (SELECT count(*)/60 FROM {t});"
        )),
        // Boolean uncorrelated EXISTS as an InitPlan one-time filter (the
        // whole scan is gated on a single evaluation).
        raw(format!(
            "SELECT count(*) FROM {t} WHERE EXISTS (SELECT 1 FROM {t} WHERE a IS NULL);"
        )),
        drop(t),
    ]);
    v
}

/// SubPlan inside an aggregate argument: the aggregate transition sees a
/// per-row SubPlan value (a correlated scalar subquery fed straight into
/// sum/count/max).
fn agg() -> Vec<StmtKind> {
    let t = "fz_sp_agg";
    let mut v = fixture(t);
    v.extend([
        // Correlated scalar subquery as the argument to sum().
        raw(format!(
            "SELECT sum((SELECT count(*) FROM {t} y WHERE y.b = x.b)) FROM {t} x;"
        )),
        // Subquery inside max() over a grouped query.
        raw(format!(
            "SELECT x.c, max((SELECT count(*) FROM {t} y WHERE y.b = x.b AND y.c = x.c)) \
             FROM {t} x GROUP BY x.c ORDER BY x.c;"
        )),
        // Uncorrelated scalar subquery inside an aggregate arg -> the
        // subquery is an InitPlan, evaluated once, summed per row.
        raw(format!(
            "SELECT count(*) FILTER (WHERE a > (SELECT avg(a) FROM {t})) FROM {t};"
        )),
        // Subquery inside string_agg's argument (text surface, C-collated,
        // ordered so the aggregate is deterministic).
        raw(format!(
            "SELECT string_agg((SELECT max(t) FROM {t} y WHERE y.b = x.b), ',' ORDER BY x.b) FROM {t} x;"
        )),
        drop(t),
    ]);
    v
}

/// SubPlan under a Gather: force a parallel plan (min costs, low table-scan
/// threshold, workers available) so the SubPlan is evaluated inside a
/// parallel worker. Only a single deterministic aggregate is compared — the
/// worker split itself is never a surface. Every SET is RESET in-group.
fn gather() -> Vec<StmtKind> {
    let t = "fz_sp_par";
    let mut v = fixture(t);
    v.extend([
        raw("SET parallel_setup_cost = 0;"),
        raw("SET parallel_tuple_cost = 0;"),
        raw("SET min_parallel_table_scan_size = 0;"),
        raw("SET max_parallel_workers_per_gather = 2;"),
        // Uncorrelated InitPlan under a forced-parallel scan: the InitPlan is
        // evaluated once (leader) and the value shipped to workers.
        raw(format!(
            "SELECT count(*) FROM {t} WHERE a > (SELECT avg(a) FROM {t});"
        )),
        // Correlated SubPlan under a Gather: each worker rescans the inner
        // subquery for its slice of outer rows (ExecScanSubPlan in a worker).
        raw(format!(
            "SELECT sum((SELECT count(*) FROM {t} y WHERE y.b = x.b)) FROM {t} x;"
        )),
        // Hashed uncorrelated `= ANY` under forced parallelism.
        raw(format!(
            "SELECT count(*) FROM {t} x WHERE x.b = ANY (SELECT c FROM {t});"
        )),
        // Uncorrelated subquery in HAVING under a forced-parallel grouped
        // plan — the SUBPLAN-1 heartland crossed with the Gather: an
        // InitPlan referenced by a Finalize-Aggregate HAVING filter. This is
        // exactly the OPEN W4O-F1 class (parallel finalize-agg HAVING filter
        // compiled through a driver-less entry point -> XX000 on B where A
        // returns rows); the probe gives the fix lane a generator-driven
        // repro and is EXPECTED to diverge on current B until that lands.
        raw(format!(
            "SELECT b, count(*) FROM {t} GROUP BY b HAVING count(*) > (SELECT avg(cnt) FROM (SELECT count(*) AS cnt FROM {t} GROUP BY c) q) ORDER BY b;"
        )),
        raw("RESET max_parallel_workers_per_gather;"),
        raw("RESET min_parallel_table_scan_size;"),
        raw("RESET parallel_tuple_cost;"),
        raw("RESET parallel_setup_cost;"),
        drop(t),
    ]);
    v
}

pub fn gen_subplan_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("subplan");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    match shape {
        "subplan:ctx" => ctx(),
        "subplan:sublink" => sublink(),
        "subplan:corr" => corr(),
        "subplan:initplan" => initplan(),
        "subplan:agg" => agg(),
        "subplan:gather" => gather(),
        other => unreachable!("unknown subplan shape {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize) -> (Vec<Vec<StmtKind>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            groups.push(gen_subplan_module(&mut g));
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    fn flat(groups: &[Vec<StmtKind>]) -> Vec<String> {
        groups.iter().flatten().map(|s| s.to_sql()).collect()
    }

    /// Every shape fires, every statement is a single well-formed line, every
    /// group is balanced (create/drop and SET/RESET), and the surface fuel
    /// each context exists for is present in the emitted SQL.
    #[test]
    fn subplan_shapes_are_well_formed_and_varied() {
        let (groups, prods) = gen_many(0x5B_01, 1200);
        for p in [
            "subplan",
            "subplan:ctx",
            "subplan:sublink",
            "subplan:corr",
            "subplan:initplan",
            "subplan:agg",
            "subplan:gather",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        let sqls = flat(&groups);
        for sql in &sqls {
            assert!(!sql.contains('\n'), "multi-line statement: {sql}");
            assert!(sql.ends_with(';'), "statement not ;-terminated: {sql}");
            assert_eq!(
                sql.matches('(').count(),
                sql.matches(')').count(),
                "unbalanced parens: {sql}"
            );
        }
        // Per-group balance: every CREATE TABLE has a matching DROP TABLE in
        // the same group, and every SET has a matching RESET.
        for grp in &groups {
            let s: Vec<String> = grp.iter().map(|x| x.to_sql()).collect();
            let creates = s.iter().filter(|x| x.starts_with("CREATE TABLE ")).count();
            let drops = s.iter().filter(|x| x.starts_with("DROP TABLE ")).count();
            assert_eq!(creates, drops, "unbalanced CREATE/DROP in {s:?}");
            let sets = s.iter().filter(|x| x.starts_with("SET ")).count();
            let resets = s.iter().filter(|x| x.starts_with("RESET ")).count();
            assert_eq!(sets, resets, "unbalanced SET/RESET in {s:?}");
        }
        let all = sqls.join("\n");
        // SUBPLAN-1 heartland: a subquery in each clause context.
        for frag in [
            "GROUP BY b HAVING count(*) > (SELECT",     // HAVING (the banked bug)
            "GROUP BY (a - (SELECT",                    // GROUP BY
            "ORDER BY abs(a - (SELECT",                 // ORDER BY
            "WHERE a > (SELECT",                        // WHERE / InitPlan
            "CASE WHEN a > (SELECT",                    // CASE
            "least(b, (SELECT",                         // function argument
            "FROM (VALUES ((SELECT",                    // VALUES
            "LIMIT (SELECT",                            // LIMIT
        ] {
            assert!(all.contains(frag), "context fuel {frag:?} never generated");
        }
        // Sublink spellings.
        for frag in [
            "= ANY (SELECT",
            "IN (SELECT",
            "NOT IN (SELECT",
            "= ALL (SELECT",
            "<> ALL (SELECT",
            "> ANY (SELECT",
            "> ALL (SELECT",
            "EXISTS (SELECT",
            "NOT EXISTS (SELECT",
        ] {
            assert!(all.contains(frag), "sublink spelling {frag:?} never generated");
        }
        // Multi-param / row-expression InitPlan and nested SubPlan.
        assert!(all.contains("= (SELECT max(b), min(c) FROM"), "row-expr InitPlan missing");
        assert!(all.contains("(SELECT max((SELECT"), "nested SubPlan-in-SubPlan missing");
        // SubPlan inside an aggregate argument.
        assert!(all.contains("sum((SELECT count(*)"), "SubPlan-in-aggregate missing");
        // Forced-parallel Gather bracket.
        assert!(all.contains("SET parallel_setup_cost = 0;"), "gather bracket missing");
    }

    /// Deterministic: same seed reproduces the exact statement stream.
    #[test]
    fn subplan_generation_is_deterministic() {
        let a = flat(&gen_many(0xABCD, 400).0);
        let b = flat(&gen_many(0xABCD, 400).0);
        assert_eq!(a, b);
    }
}
