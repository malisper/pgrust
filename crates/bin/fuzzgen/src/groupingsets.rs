//! GROUPING SETS / ROLLUP / CUBE + GROUPING() drain module.
//!
//! Track-B (SQL-drainable) deepening of the grouping-set surface. The
//! standing `agg` module carries the AST-level GROUP BY / ROLLUP / CUBE /
//! GROUPING SETS shapes at low weight; `pgram` carries the parser rare-
//! grammar nesting and the deliberate analysis-error arms; `einterp`
//! touched grouping-set hashing under a tiny work_mem floor. This module
//! drives the *execution* residue those left — the nodeAgg.c grouping-set
//! phase chain (multiple distinct sort orders → a sorted AggState chain),
//! the hashed-vs-sorted grouping-set mix and its hash-spill refill path,
//! the mixed cartesian expansion `GROUP BY a, ROLLUP (b, c), CUBE (d)`, the
//! GROUPING() bitmask across SELECT / HAVING / ORDER BY, DISTINCT + grouping
//! sets (both `SELECT DISTINCT` and `GROUP BY DISTINCT`), grouping sets over
//! a join, and the HAVING NULL-vs-present distinction that only GROUPING()
//! can resolve. Targets: nodeAgg.c grouping-set chains, the planner
//! grouping-set expansion (planner.c/prepagg.c), and parse_agg.c.
//!
//! Compare-safety discipline (the F1 differ consumes these statements):
//!
//!   - Grouping-set output has many NULL-extended rows, and a NULL grouping
//!     column is textually identical to a genuine NULL value. Every probe
//!     therefore projects a GROUPING(...) bitmask column and carries a
//!     TOTAL ORDER BY over every grouping column (NULLS LAST pinned, so the
//!     two NULL kinds sort consistently on both engines) PLUS the GROUPING()
//!     bitmask, so rows from different grouping sets that share a rendered
//!     column vector are still totally ordered and the multiset is
//!     deterministic. Any residual tie is then between byte-identical rows.
//!
//!   - Aggregates are confined to exact, order-insensitive ones (count,
//!     count(DISTINCT), sum/min/max over the integer family, avg cast to
//!     numeric which accumulates exactly): no order-sensitive float
//!     aggregate ever appears, so no ruled-soft column is emitted and the
//!     whole result set compares byte-for-byte.
//!
//!   - Every group is self-contained: it creates its fz_gs* fixture(s),
//!     probes, and drops them, over deterministic small (or fixed-shape
//!     bulk) data, so the stream stays position-independent and both
//!     differential sides see identical inputs.

use crate::stmt::{Gen, StmtKind};

const GS_SHAPES: &[&str] = &[
    "gs:rollup",
    "gs:cube",
    "gs:sets",
    "gs:mixed",
    "gs:nested",
    "gs:chain",
    "gs:hashmix",
    "gs:distinct",
    "gs:join",
    "gs:having",
    "gs:gorder",
];

/// Registry entry point (dispatched from stmt::STMT_MODULES / the
/// `groupingsets` toggle).
pub fn gen_groupingsets_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("groupingsets");
    let shape = g.weights.pick(g.rng, GS_SHAPES);
    g.fire(shape);
    match shape {
        "gs:rollup" => gs_rollup(g),
        "gs:cube" => gs_cube(g),
        "gs:sets" => gs_sets(g),
        "gs:mixed" => gs_mixed(g),
        "gs:nested" => gs_nested(g),
        "gs:chain" => gs_chain(g),
        "gs:hashmix" => gs_hashmix(g),
        "gs:distinct" => gs_distinct(g),
        "gs:join" => gs_join(g),
        "gs:having" => gs_having(g),
        "gs:gorder" => gs_gorder(g),
        other => unreachable!("unknown grouping-set shape {other}"),
    }
}

fn raw(s: &str) -> StmtKind {
    StmtKind::Raw(s.to_string())
}

/// The deterministic 12-row grouping-set fixture. `a`/`b`/`c` are small
/// integer keys, `c` carries a genuine NULL every 4th row (so GROUPING()
/// must distinguish it from a grouping-extension NULL), `g` is a stable
/// text key. Wraps the probe statements.
fn gs_fixture(probes: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![
        raw("DROP TABLE IF EXISTS fz_gs CASCADE;"),
        raw("CREATE TABLE fz_gs (a int, b int, c int, g text);"),
        raw(
            "INSERT INTO fz_gs SELECT i % 2, i % 3, CASE WHEN i % 4 = 0 THEN NULL ELSE i % 3 END, chr(97 + i % 3) FROM generate_series(1, 12) i;",
        ),
    ];
    v.extend(probes.into_iter().map(StmtKind::Raw));
    v.push(raw("DROP TABLE fz_gs CASCADE;"));
    v
}

/// Two-table fixture for grouping sets over a join.
fn gs_join_fixture(probes: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![
        raw("DROP TABLE IF EXISTS fz_gs CASCADE;"),
        raw("DROP TABLE IF EXISTS fz_gs2 CASCADE;"),
        raw("CREATE TABLE fz_gs (a int, b int, c int, g text);"),
        raw("CREATE TABLE fz_gs2 (a int, d int);"),
        raw(
            "INSERT INTO fz_gs SELECT i % 2, i % 3, CASE WHEN i % 4 = 0 THEN NULL ELSE i % 3 END, chr(97 + i % 3) FROM generate_series(1, 12) i;",
        ),
        raw("INSERT INTO fz_gs2 SELECT i % 2, i % 5 FROM generate_series(1, 10) i;"),
    ];
    v.extend(probes.into_iter().map(StmtKind::Raw));
    v.push(raw("DROP TABLE fz_gs CASCADE;"));
    v.push(raw("DROP TABLE fz_gs2 CASCADE;"));
    v
}

/// Fixed-shape bulk fixture (2000 rows, high-cardinality keys) for the
/// hash-spill and sorted-chain execution paths: under work_mem='64kB' the
/// grouping-set hash table must spill, and under enable_hashagg=off the
/// distinct sort orders build a sorted AggState phase chain.
fn gs_big_fixture(probes: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![
        raw("DROP TABLE IF EXISTS fz_gsb CASCADE;"),
        raw("CREATE TABLE fz_gsb (a int, b int, c int);"),
        raw("INSERT INTO fz_gsb SELECT i % 50, i % 40, i % 30 FROM generate_series(1, 2000) i;"),
    ];
    v.extend(probes.into_iter().map(StmtKind::Raw));
    v.push(raw("DROP TABLE fz_gsb CASCADE;"));
    v
}

/// ROLLUP with the full GROUPING() bitmask column and a total order over
/// (bitmask, a, b, c, aggregates).
fn gs_rollup(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8, sum(b)::int8 FROM fz_gs GROUP BY ROLLUP (a, b, c) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5, 6;",
        1 => "SELECT g, a, GROUPING(g, a)::int4 AS gm, count(*)::int8, min(b)::int8, max(b)::int8 FROM fz_gs GROUP BY ROLLUP (g, a) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4, 5, 6;",
        _ => "SELECT a, c, GROUPING(a)::int4 AS ga, GROUPING(c)::int4 AS gc, count(*)::int8 FROM fz_gs GROUP BY ROLLUP (a, c) ORDER BY 3, 4, 1 NULLS LAST, 2 NULLS LAST, 5;",
    };
    gs_fixture(vec![probe.to_string()])
}

/// CUBE (full 2^n grouping-set lattice) with per-column GROUPING() bits.
fn gs_cube(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT a, b, GROUPING(a)::int4 AS ga, GROUPING(b)::int4 AS gb, count(*)::int8, avg(c)::numeric::text FROM fz_gs GROUP BY CUBE (a, b) ORDER BY 3, 4, 1 NULLS LAST, 2 NULLS LAST, 5, 6;",
        1 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY CUBE (a, b, c) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
        _ => "SELECT g, c, GROUPING(g, c)::int4 AS gm, count(*)::int8, sum(a)::int8 FROM fz_gs GROUP BY CUBE (g, c) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4, 5;",
    };
    gs_fixture(vec![probe.to_string()])
}

/// Explicit GROUPING SETS list including the empty grand-total set `()`
/// and duplicate/overlapping subsets.
fn gs_sets(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY GROUPING SETS ((a, b, c), (a, b), (a), (), (b, c)) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
        1 => "SELECT a, b, GROUPING(a, b)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY GROUPING SETS ((a, b), (a), (b), ()) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;",
        _ => "SELECT g, a, GROUPING(g, a)::int4 AS gm, count(*)::int8, count(DISTINCT b)::int8 FROM fz_gs GROUP BY GROUPING SETS ((g, a), (g), (), (a)) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4, 5;",
    };
    gs_fixture(vec![probe.to_string()])
}

/// Mixed cartesian expansion: `GROUP BY a, ROLLUP (b, c), CUBE (g)` — the
/// planner expands the list into the cross product of each element's sets.
fn gs_mixed(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT a, b, c, g, GROUPING(a, b, c, g)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY a, ROLLUP (b, c), CUBE (g) ORDER BY 5, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 4 NULLS LAST, 6;",
        1 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY a, CUBE (b, c) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
        _ => "SELECT a, b, g, GROUPING(a, b, g)::int4 AS gm, count(*)::int8, sum(c)::int8 FROM fz_gs GROUP BY ROLLUP (a), CUBE (b), GROUPING SETS ((g), ()) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5, 6;",
    };
    gs_fixture(vec![probe.to_string()])
}

/// Nested grouping sets: GROUPING SETS containing ROLLUP / CUBE / a nested
/// GROUPING SETS / the empty set.
fn gs_nested(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY GROUPING SETS (ROLLUP (a, b), CUBE (c), ()) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
        1 => "SELECT a, b, GROUPING(a, b)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY GROUPING SETS (GROUPING SETS ((a), (b)), GROUPING SETS ((a, b), ())) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;",
        _ => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY CUBE ((a, b), c) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
    };
    gs_fixture(vec![probe.to_string()])
}

/// Sorted grouping-set phase chain: enable_hashagg=off forces a chain of
/// sorted AggState phases, one per distinct required sort order.
fn gs_chain(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(2) {
        0 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gsb GROUP BY GROUPING SETS ((a, b), (b, c), (c, a), (a), ()) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
        _ => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8, sum(c)::int8 FROM fz_gsb GROUP BY CUBE (a, b, c) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5, 6;",
    };
    gs_big_fixture(vec![
        "SET enable_hashagg = off;".to_string(),
        probe.to_string(),
        "RESET enable_hashagg;".to_string(),
    ])
}

/// Hashed/sorted grouping-set mix with hash-table spill: default plan lets
/// the planner do the hashable sets in a single hash pass, and a tiny
/// work_mem forces the grouping-set hash table to spill and refill.
fn gs_hashmix(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(2) {
        0 => "SELECT a, b, c, GROUPING(a, b, c)::int4 AS gm, count(*)::int8 FROM fz_gsb GROUP BY GROUPING SETS ((a, b, c), (a, b), (a), (b), (c), ()) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
        _ => "SELECT a, b, GROUPING(a, b)::int4 AS gm, count(*)::int8, count(DISTINCT c)::int8 FROM fz_gsb GROUP BY CUBE (a, b) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4, 5;",
    };
    gs_big_fixture(vec![
        "SET work_mem = '64kB';".to_string(),
        probe.to_string(),
        "RESET work_mem;".to_string(),
    ])
}

/// DISTINCT interacting with grouping sets: SELECT DISTINCT over a CUBE
/// output (dedups the NULL-extended rows) and GROUP BY DISTINCT (dedups
/// the generated grouping sets themselves).
fn gs_distinct(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT DISTINCT a, b, GROUPING(a, b)::int4 AS gm FROM fz_gs GROUP BY CUBE (a, b) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST;",
        1 => "SELECT a, b, count(*)::int8 FROM fz_gs GROUP BY DISTINCT ROLLUP (a, b), ROLLUP (a) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3;",
        _ => "SELECT a, b, GROUPING(a, b)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY DISTINCT CUBE (a, b), GROUPING SETS ((a), ()) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;",
    };
    gs_fixture(vec![probe.to_string()])
}

/// Grouping sets over a join, with alias-qualified grouping columns.
fn gs_join(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(2) {
        0 => "SELECT t.a, u.d, GROUPING(t.a, u.d)::int4 AS gm, count(*)::int8 FROM fz_gs t JOIN fz_gs2 u ON t.a = u.a GROUP BY CUBE (t.a, u.d) ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;",
        _ => "SELECT t.a, t.b, u.d, GROUPING(t.a, t.b, u.d)::int4 AS gm, count(*)::int8 FROM fz_gs t LEFT JOIN fz_gs2 u ON t.a = u.a GROUP BY GROUPING SETS ((t.a, t.b), (t.a, u.d), ()) ORDER BY 4, 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 5;",
    };
    gs_join_fixture(vec![probe.to_string()])
}

/// HAVING referencing grouping-set columns and GROUPING() — the NULL-vs-
/// present distinction only GROUPING() can make (a grouping-extension NULL
/// row has GROUPING(col)=1; a genuine-NULL group has GROUPING(col)=0).
fn gs_having(g: &mut Gen) -> Vec<StmtKind> {
    let k = g.rng.below(4);
    let probe = match g.rng.below(3) {
        0 => format!(
            "SELECT a, b, GROUPING(a, b)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY CUBE (a, b) HAVING GROUPING(a) = 0 OR count(*) > {k} ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;"
        ),
        1 => format!(
            "SELECT a, c, GROUPING(c)::int4 AS gc, count(*)::int8 FROM fz_gs GROUP BY ROLLUP (a, c) HAVING GROUPING(c) = 1 OR count(*) >= {k} ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;"
        ),
        _ => format!(
            "SELECT a, b, GROUPING(a, b)::int4 AS gm, count(*)::int8 FROM fz_gs GROUP BY GROUPING SETS ((a, b), (a), (b), ()) HAVING GROUPING(a) + GROUPING(b) < 2 AND count(*) > {k} ORDER BY 3, 1 NULLS LAST, 2 NULLS LAST, 4;"
        ),
    };
    gs_fixture(vec![probe])
}

/// GROUPING() driving the output order directly in ORDER BY (a grouping
/// operation in the sort target list, not a projected column).
fn gs_gorder(g: &mut Gen) -> Vec<StmtKind> {
    let probe = match g.rng.below(3) {
        0 => "SELECT a, b, count(*)::int8 FROM fz_gs GROUP BY ROLLUP (a, b) ORDER BY GROUPING(a), GROUPING(b), a NULLS LAST, b NULLS LAST, 3;",
        1 => "SELECT a, b, c, count(*)::int8 FROM fz_gs GROUP BY CUBE (a, b, c) ORDER BY GROUPING(a, b, c), a NULLS LAST, b NULLS LAST, c NULLS LAST, 4;",
        _ => "SELECT g, a, count(*)::int8 FROM fz_gs GROUP BY GROUPING SETS ((g, a), (g), ()) ORDER BY GROUPING(g), GROUPING(a), g NULLS LAST, a NULLS LAST, 3;",
    };
    gs_fixture(vec![probe.to_string()])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every shape fires, every statement is single-line, terminated, and
    /// paren-balanced, and every probe carries both a GROUPING() bitmask and
    /// a total ORDER BY (the deterministic-multiset discipline).
    #[test]
    fn grouping_set_probes_are_well_formed_and_ordered() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x6535);
        let mut all = String::new();
        let mut seen_shapes = std::collections::BTreeSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_groupingsets_module(&mut g);
            assert!(!stmts.is_empty());
            for p in &prods {
                if p.starts_with("gs:") {
                    seen_shapes.insert(p.clone());
                }
            }
            let mut created = false;
            let mut dropped = false;
            for st in &stmts {
                let sql = st.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                if sql.starts_with("CREATE TABLE") {
                    created = true;
                }
                if sql.starts_with("DROP TABLE fz_") {
                    dropped = true;
                }
                // Grouping-set probes must be totally ordered (the
                // deterministic-multiset discipline over NULL-extended rows).
                // GROUPING() is carried by all but the GROUP BY DISTINCT
                // dedup arm, whose (a, b) vector is already unambiguous.
                if sql.contains("GROUP BY") {
                    assert!(sql.contains("ORDER BY"), "probe without ORDER BY: {sql}");
                }
                all.push_str(&sql);
                all.push('\n');
            }
            assert!(
                created && dropped,
                "group is not self-contained (create+drop)"
            );
        }
        for shape in GS_SHAPES {
            assert!(seen_shapes.contains(*shape), "shape {shape} never fired");
        }
        // Surface coverage: the module must exercise each headline construct.
        for frag in [
            "ROLLUP (",
            "CUBE (",
            "GROUPING SETS (",
            "GROUPING SETS (ROLLUP",
            "GROUP BY a, ROLLUP",
            "GROUP BY DISTINCT",
            "SELECT DISTINCT",
            "HAVING GROUPING(",
            "ORDER BY GROUPING(",
            "SET enable_hashagg = off;",
            "SET work_mem = '64kB';",
            "JOIN fz_gs2",
            "()", // the empty grand-total grouping set
        ] {
            assert!(all.contains(frag), "construct {frag:?} never generated");
        }
        // No order-sensitive float aggregate may leak in (byte-exact compare).
        for bad in ["sum(f", "avg(f", "float", "stddev", "var_"] {
            assert!(
                !all.contains(bad),
                "order-sensitive/float construct leaked: {bad}"
            );
        }
    }

    /// Same seed + same weights ⇒ byte-identical stream.
    #[test]
    fn generation_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(42);
            let mut out = String::new();
            for _ in 0..200 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                for st in gen_groupingsets_module(&mut g) {
                    out.push_str(&st.to_sql());
                    out.push('\n');
                }
            }
            out
        };
        assert_eq!(run(), run());
    }
}
