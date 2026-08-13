//! Recursive-CTE SEARCH/CYCLE planning + data-modifying CTE + sublink drain
//! module (Track-B). The recursive-union *execution* path was drained
//! elsewhere; the residue this module targets is distinct:
//!
//!   * the SEARCH BREADTH/DEPTH FIRST BY ... SET clause rewrite (parse_cte.c
//!     + rewrite/rewriteSearchCycle) — the injected search-order column and
//!     the ordering it induces (validated as ORDERED-compare: the SET column
//!     is projected and the outer query ORDERs BY it, so a wrong preorder /
//!     breadth-order on either engine is a real divergence);
//!   * the CYCLE col SET markcol [TO 'Y' DEFAULT 'N'] USING pathcol rewrite —
//!     both the boolean default-mark form and the explicit TO/DEFAULT text-
//!     mark form, over deterministic small graphs that DO contain cycles so
//!     the cycle-detection mark actually fires (a cycle-detection divergence
//!     is a HIGH finding);
//!   * UNION vs UNION ALL recursion (distinct-working-table dedup path);
//!   * downstream/nested/MATERIALIZED auxiliary CTEs over a recursive CTE;
//!   * data-modifying CTEs (INSERT/UPDATE/DELETE ... RETURNING in WITH,
//!     several in one statement, and the snapshot-visibility rule: a sibling
//!     SELECT of the modified table sees the PRE-modification rows);
//!   * the sublink surface distinct from subq.rs: = ANY / <> ALL / rowcmp
//!     subqueries, the scalar 0-or-1-row rule incl. the deliberate too-many-
//!     rows error (SQLSTATE 21000, error-identity compared), and the classic
//!     three-valued NOT IN / IN trap when the subquery yields a NULL (a wrong
//!     3VL answer on NOT IN is a HIGH finding).
//!
//! Determinism discipline (earm/pgram style):
//!   * every statement is SELF-CONTAINED — recursion + sublink probes run
//!     over inline VALUES, never the shared catalog; the data-modifying
//!     family creates its fixture under a fixed `fz_cr_dm` name inside a
//!     BEGIN..ROLLBACK bracket so it leaves no state behind;
//!   * every recursion carries a depth bound (WHERE depth < K) so it
//!     terminates regardless of graph shape / UNION mode / CYCLE clause;
//!   * SEARCH statements carry a total ORDER BY over the SET column (ordered
//!     compare — the whole point is the order); SEARCH-less recursions omit
//!     ORDER BY and lean on the differ's multiset compare (the row *set* is
//!     deterministic even when recursion order is not);
//!   * integer/text literal data only — identical on both differential sides.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// Directed graphs rooted at node 1, rendered inline as `VALUES` edge sets.
/// Acyclic shapes exercise the SEARCH ordering and the CYCLE "no cycle
/// found" arm; cyclic shapes make the CYCLE mark actually fire.
struct Graph {
    edges: &'static [(i32, i32)],
    cyclic: bool,
}

const GRAPHS: &[Graph] = &[
    // Tree.
    Graph { edges: &[(1, 2), (1, 3), (2, 4), (2, 5), (3, 6)], cyclic: false },
    // Diamond DAG: node 4 reached two ways (UNION dedups it, UNION ALL
    // keeps both paths).
    Graph { edges: &[(1, 2), (1, 3), (2, 4), (3, 4)], cyclic: false },
    // 3-cycle 1->2->3->1 plus a tail 3->4.
    Graph { edges: &[(1, 2), (2, 3), (3, 1), (3, 4)], cyclic: true },
    // 2-cycle 1<->2 plus a self-loop at 3.
    Graph { edges: &[(1, 2), (2, 1), (1, 3), (3, 3)], cyclic: true },
    // Self-loop at the root.
    Graph { edges: &[(1, 1), (1, 2), (2, 3)], cyclic: true },
];

fn render_edges(edges: &[(i32, i32)]) -> String {
    let mut s = String::new();
    for (i, (a, b)) in edges.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("({a}, {b})"));
    }
    s
}

/// One recursive-CTE statement over an inline graph, with a weighted choice
/// of UNION mode, SEARCH clause and CYCLE clause.
fn gen_graph_rec(g: &mut Gen) -> String {
    // CYCLE first: it decides whether we prefer a cyclic graph.
    let cycle = g.weights.pick(
        g.rng,
        &["cterec:cycle_none", "cterec:cycle_bool", "cterec:cycle_marked"],
    );
    g.fire(cycle);
    let want_cyclic = cycle != "cterec:cycle_none";
    // Pick a graph; when a CYCLE clause is present, bias to a cyclic graph
    // so the mark fires (but still allow acyclic — the "no cycle" arm).
    let gi = if want_cyclic && g.rng.chance(3, 4) {
        // choose among cyclic graphs
        let cyclic: Vec<usize> =
            (0..GRAPHS.len()).filter(|&i| GRAPHS[i].cyclic).collect();
        cyclic[g.rng.below_usize(cyclic.len())]
    } else {
        g.rng.below_usize(GRAPHS.len())
    };
    let graph = &GRAPHS[gi];

    let union = g.weights.pick(g.rng, &["cterec:union_all", "cterec:union"]);
    g.fire(union);
    let union_sql = if union == "cterec:union" { "UNION" } else { "UNION ALL" };

    let search = g.weights.pick(
        g.rng,
        &["cterec:search_none", "cterec:search_depth", "cterec:search_breadth"],
    );
    g.fire(search);
    // Occasionally a multi-column BY list (exercises the multi-key search
    // order rewrite).
    let by = if g.rng.chance(1, 3) { "node, depth" } else { "node" };
    let search_sql = match search {
        "cterec:search_depth" => format!(" SEARCH DEPTH FIRST BY {by} SET so"),
        "cterec:search_breadth" => format!(" SEARCH BREADTH FIRST BY {by} SET so"),
        _ => String::new(),
    };

    let cycle_sql = match cycle {
        "cterec:cycle_bool" => " CYCLE node SET ic USING pa".to_string(),
        "cterec:cycle_marked" => {
            " CYCLE node SET ic TO 'Y' DEFAULT 'N' USING pa".to_string()
        }
        _ => String::new(),
    };

    // Depth bound guarantees termination for every graph/union/cycle combo.
    let k = 4 + g.rng.below(5); // 4..8

    // Projection: base columns plus whatever the clauses injected.
    let mut proj = String::from("node, depth");
    if !search_sql.is_empty() {
        proj.push_str(", so");
    }
    if !cycle_sql.is_empty() {
        proj.push_str(", ic, pa");
    }

    // SEARCH -> ordered compare on the SET column; otherwise multiset.
    let order = if !search_sql.is_empty() {
        " ORDER BY so, node, depth".to_string()
    } else {
        String::new()
    };

    format!(
        "WITH RECURSIVE g(src, dst) AS (VALUES {edges}), \
         t(node, depth) AS (SELECT 1, 0 {union_sql} \
         SELECT g.dst, t.depth + 1 FROM t JOIN g ON g.src = t.node \
         WHERE t.depth < {k}){search_sql}{cycle_sql} \
         SELECT {proj} FROM t{order};",
        edges = render_edges(graph.edges),
    )
}

/// A recursive CTE feeding a downstream auxiliary CTE — either a plain
/// aggregate roll-up, a MATERIALIZED / NOT MATERIALIZED aggregate, or a
/// recursive CTE nested inside an outer WITH.
fn gen_aux_rec(g: &mut Gen) -> String {
    let graph = &GRAPHS[g.rng.below_usize(GRAPHS.len())];
    let edges = render_edges(graph.edges);
    let k = 4 + g.rng.below(4); // 4..7
    let rec = format!(
        "WITH RECURSIVE g(src, dst) AS (VALUES {edges}), \
         t(node, depth) AS (SELECT 1, 0 UNION ALL \
         SELECT g.dst, t.depth + 1 FROM t JOIN g ON g.src = t.node \
         WHERE t.depth < {k})"
    );
    match g.weights.pick(
        g.rng,
        &["cterec:aux_plain", "cterec:aux_mat", "cterec:aux_nested"],
    ) {
        "cterec:aux_mat" => {
            g.fire("cterec:aux_mat");
            let m = if g.rng.chance(1, 2) {
                "MATERIALIZED"
            } else {
                "NOT MATERIALIZED"
            };
            format!(
                "{rec}, agg AS {m} (SELECT depth, count(*) AS c, max(node) AS mx \
                 FROM t GROUP BY depth) SELECT depth, c, mx FROM agg ORDER BY depth;"
            )
        }
        "cterec:aux_nested" => {
            g.fire("cterec:aux_nested");
            // Recursive CTE nested inside an outer non-recursive WITH.
            format!(
                "WITH outer_cte AS ({rec} SELECT node, depth FROM t) \
                 SELECT depth, count(*) AS c FROM outer_cte GROUP BY depth ORDER BY depth;"
            )
        }
        _ => {
            g.fire("cterec:aux_plain");
            format!(
                "{rec}, agg AS (SELECT depth, count(*) AS c FROM t GROUP BY depth) \
                 SELECT depth, c FROM agg ORDER BY depth;"
            )
        }
    }
}

/// Plain (non-recursive) CTE with a MATERIALIZED / NOT MATERIALIZED hint —
/// the inlining-decision surface, isolated from recursion.
fn gen_mat(g: &mut Gen) -> String {
    g.fire("cterec:mat");
    let a = if g.rng.chance(1, 2) { "MATERIALIZED" } else { "NOT MATERIALIZED" };
    let b = if g.rng.chance(1, 2) { "MATERIALIZED" } else { "NOT MATERIALIZED" };
    format!(
        "WITH a AS {a} (SELECT x, x * 2 AS y FROM (VALUES (1), (2), (3), (4)) v(x)), \
         b AS {b} (SELECT x, x + 1 AS z FROM (VALUES (2), (3), (4), (5)) v(x)) \
         SELECT a.x, a.y, b.z FROM a JOIN b ON a.x = b.x ORDER BY a.x;"
    )
}

/// Data-modifying CTE bracket: a BEGIN..ROLLBACK window around a fixed
/// `fz_cr_dm` fixture, exercising RETURNING-fed chains, multiple modifying
/// CTEs in one statement, and the snapshot-visibility rule.
fn gen_dml(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = vec![
        raw("BEGIN;"),
        raw("CREATE TEMP TABLE fz_cr_dm (id int PRIMARY KEY, v int, tag text);"),
        raw(
            "INSERT INTO fz_cr_dm VALUES (1, 10, 'a'), (2, 20, 'b'), \
             (3, 30, 'c'), (4, 40, 'd');",
        ),
    ];
    match g.weights.pick(
        g.rng,
        &[
            "cterec:dml_move",
            "cterec:dml_multi",
            "cterec:dml_visibility",
            "cterec:dml_chain",
        ],
    ) {
        "cterec:dml_move" => {
            g.fire("cterec:dml_move");
            // DELETE ... RETURNING feeding an INSERT (row move).
            v.push(raw(
                "WITH moved AS (DELETE FROM fz_cr_dm WHERE id = 2 RETURNING id, v, tag) \
                 INSERT INTO fz_cr_dm SELECT id + 100, v + 1, tag FROM moved;",
            ));
        }
        "cterec:dml_multi" => {
            g.fire("cterec:dml_multi");
            // Several data-modifying CTEs in one statement; count the
            // RETURNING rows so the result is order-free and stable.
            v.push(raw(
                "WITH ins AS (INSERT INTO fz_cr_dm VALUES (5, 50, 'e'), (6, 60, 'f') \
                 RETURNING id), \
                 upd AS (UPDATE fz_cr_dm SET v = v + 1 WHERE id <= 2 RETURNING id), \
                 del AS (DELETE FROM fz_cr_dm WHERE id = 4 RETURNING id) \
                 SELECT (SELECT count(*) FROM ins) AS n_ins, \
                 (SELECT count(*) FROM upd) AS n_upd, \
                 (SELECT count(*) FROM del) AS n_del;",
            ));
        }
        "cterec:dml_visibility" => {
            g.fire("cterec:dml_visibility");
            // Snapshot-visibility: the sibling SELECT of the same table sees
            // the PRE-UPDATE rows (RETURNING shows the new ones).
            v.push(raw(
                "WITH u AS (UPDATE fz_cr_dm SET v = v * 10 WHERE id <= 2 RETURNING id, v) \
                 SELECT s.id, s.v FROM fz_cr_dm s ORDER BY s.id;",
            ));
        }
        _ => {
            g.fire("cterec:dml_chain");
            // INSERT ... RETURNING feeding a downstream aggregate.
            v.push(raw(
                "WITH ins AS (INSERT INTO fz_cr_dm SELECT g, g * 100, 'x' \
                 FROM generate_series(10, 13) g RETURNING id, v) \
                 SELECT count(*) AS n, sum(v) AS s FROM ins;",
            ));
        }
    }
    // Ordered probe of the final table state, then unwind.
    v.push(raw("SELECT id, v, tag FROM fz_cr_dm ORDER BY id;"));
    v.push(raw("ROLLBACK;"));
    v
}

/// The sublink surface: = ANY / <> ALL / row-comparison subqueries, the
/// scalar 0-or-1-row rule incl. the too-many-rows error, the three-valued
/// NOT IN / IN traps, and correlated forms over inline VALUES.
fn gen_sublink(g: &mut Gen) -> String {
    match g.weights.pick(
        g.rng,
        &[
            "cterec:any",
            "cterec:all",
            "cterec:rowcmp",
            "cterec:scalar",
            "cterec:toomany",
            "cterec:notin3vl",
            "cterec:in3vl",
            "cterec:corr_exists",
            "cterec:corr_in",
        ],
    ) {
        "cterec:any" => {
            g.fire("cterec:any");
            let (lhs, op, set) = ANY_BANK[g.rng.below_usize(ANY_BANK.len())];
            format!("SELECT {lhs} {op} ANY (SELECT x FROM (VALUES {set}) v(x)) AS r;")
        }
        "cterec:all" => {
            g.fire("cterec:all");
            let (lhs, op, set) = ALL_BANK[g.rng.below_usize(ALL_BANK.len())];
            format!("SELECT {lhs} {op} ALL (SELECT x FROM (VALUES {set}) v(x)) AS r;")
        }
        "cterec:rowcmp" => {
            g.fire("cterec:rowcmp");
            let (lhs, quant, set) = ROW_BANK[g.rng.below_usize(ROW_BANK.len())];
            format!(
                "SELECT {lhs} {quant} (SELECT a, b FROM (VALUES {set}) v(a, b)) AS r;"
            )
        }
        "cterec:scalar" => {
            g.fire("cterec:scalar");
            // 0-or-1-row scalar subquery: the WHERE arm can yield 0 rows
            // (result NULL) or exactly 1.
            let (body, tail) = SCALAR_BANK[g.rng.below_usize(SCALAR_BANK.len())];
            format!("SELECT (SELECT x FROM (VALUES {body}) v(x){tail}) AS r;")
        }
        "cterec:toomany" => {
            g.fire("cterec:toomany");
            // Deliberate SQLSTATE 21000 — compared for error identity.
            "SELECT (SELECT x FROM (VALUES (1), (2), (3)) v(x)) AS r;".to_string()
        }
        "cterec:notin3vl" => {
            g.fire("cterec:notin3vl");
            let (lhs, set) = NOTIN_BANK[g.rng.below_usize(NOTIN_BANK.len())];
            format!("SELECT {lhs} NOT IN (SELECT x FROM (VALUES {set}) v(x)) AS r;")
        }
        "cterec:in3vl" => {
            g.fire("cterec:in3vl");
            let (lhs, set) = IN_BANK[g.rng.below_usize(IN_BANK.len())];
            format!("SELECT {lhs} IN (SELECT x FROM (VALUES {set}) v(x)) AS r;")
        }
        "cterec:corr_exists" => {
            g.fire("cterec:corr_exists");
            // Correlated EXISTS: the inner WHERE references the outer row.
            "SELECT o.a, EXISTS (SELECT 1 FROM (VALUES (2), (4), (6)) w(b) \
             WHERE w.b = o.a * 2) AS e FROM (VALUES (1), (2), (5)) o(a) \
             ORDER BY o.a;"
                .to_string()
        }
        _ => {
            g.fire("cterec:corr_in");
            // Correlated IN, with a NULL in the outer set to also cross the
            // 3VL path per outer row.
            "SELECT o.a, o.a IN (SELECT b FROM (VALUES (2), (4)) w(b) \
             WHERE w.b <> o.a) AS r FROM (VALUES (2), (3), (4)) o(a) \
             ORDER BY o.a;"
                .to_string()
        }
    }
}

// (lhs, op, value-set) — value sets deliberately include NULL so ANY/ALL
// cross the three-valued arms (ANY over a set with no match but a NULL is
// NULL; ALL over a set with a NULL and no violation is NULL).
const ANY_BANK: &[(&str, &str, &str)] = &[
    ("5", "=", "(1), (5), (9)"),
    ("7", "=", "(1), (2), (3)"),
    ("5", "=", "(1), (NULL::int), (5)"),
    ("3", "=", "(1), (2), (NULL::int)"),
    ("4", ">", "(1), (2), (3)"),
    ("2", "<", "(NULL::int), (5)"),
    ("6", "<>", "(6), (6)"),
];

const ALL_BANK: &[(&str, &str, &str)] = &[
    ("7", ">=", "(1), (4), (7)"),
    ("3", "<>", "(1), (2), (4)"),
    ("3", "<>", "(1), (3), (5)"),
    ("5", ">", "(1), (2), (NULL::int)"),
    ("9", ">", "(1), (2), (3)"),
    ("2", "=", "(2), (2)"),
];

// Row-comparison subqueries: (lhs-row, quantifier+op, two-column value set).
const ROW_BANK: &[(&str, &str, &str)] = &[
    ("(1, 2)", "= ANY", "(1, 2), (3, 4)"),
    ("(1, 2)", "= ANY", "(3, 4), (5, 6)"),
    ("ROW(1, 2)", "IN", "(1, 2), (7, 8)"),
    ("(1, 2)", "< ALL", "(3, 4), (5, 6)"),
    ("(2, 3)", "<> ALL", "(1, 1), (2, 2)"),
];

// Scalar 0-or-1-row: (value-set, tail). A WHERE that keeps 0 rows -> NULL.
const SCALAR_BANK: &[(&str, &str)] = &[
    ("(42)", ""),
    ("(1)", " WHERE x > 5"),   // 0 rows -> NULL
    ("(7)", " WHERE x = 7"),   // exactly 1
    ("(3), (9)", " WHERE x = 9"),
    ("(5)", " WHERE x < 0"),   // 0 rows -> NULL
];

// NOT IN with a NULL in the set: a non-member yields NULL (NOT TRUE) — the
// classic 3VL trap. Definite cases (no NULL) are mixed in as controls.
const NOTIN_BANK: &[(&str, &str)] = &[
    ("3", "(1), (2), (NULL::int)"), // NULL, not TRUE
    ("2", "(1), (2), (NULL::int)"), // FALSE (member)
    ("3", "(1), (2)"),              // TRUE (control, no NULL)
    ("9", "(1), (NULL::int), (5)"), // NULL
    ("2", "(1), (3), (5)"),         // TRUE (control)
];

// IN with a NULL: a non-member yields NULL; a member yields TRUE.
const IN_BANK: &[(&str, &str)] = &[
    ("1", "(2), (NULL::int)"), // NULL
    ("1", "(1), (NULL::int)"), // TRUE
    ("4", "(1), (2), (3)"),    // FALSE (control)
    ("2", "(2), (NULL::int)"), // TRUE
];

pub fn gen_cterec_module(g: &mut Gen) -> Vec<StmtKind> {
    match g.weights.pick(
        g.rng,
        &[
            "cterec:graph",
            "cterec:aux",
            "cterec:mat",
            "cterec:dml",
            "cterec:sublink",
        ],
    ) {
        "cterec:graph" => {
            g.fire("cterec:graph");
            vec![raw(gen_graph_rec(g))]
        }
        "cterec:aux" => {
            g.fire("cterec:aux");
            vec![raw(gen_aux_rec(g))]
        }
        "cterec:mat" => {
            // fire happens inside gen_mat
            vec![raw(gen_mat(g))]
        }
        "cterec:dml" => {
            g.fire("cterec:dml");
            gen_dml(g)
        }
        _ => {
            g.fire("cterec:sublink");
            vec![raw(gen_sublink(g))]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Drive the module hard and collect every emitted statement + fired
    /// production over a wide seed sweep.
    fn sweep(n: usize) -> (Vec<String>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xC7E3EC);
        let mut sqls = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 4);
            for s in gen_cterec_module(&mut g) {
                sqls.push(s.to_sql());
            }
            prods.extend(p);
        }
        (sqls, prods)
    }

    #[test]
    fn statements_are_well_formed() {
        let (sqls, _) = sweep(4000);
        for sql in &sqls {
            assert!(!sql.contains('\n'), "multi-line: {sql}");
            assert!(sql.ends_with(';'), "unterminated: {sql}");
            assert_eq!(
                sql.matches('(').count(),
                sql.matches(')').count(),
                "unbalanced parens: {sql}"
            );
            // Recursion is always depth-bounded (termination invariant).
            if sql.contains("WITH RECURSIVE") {
                assert!(sql.contains("WHERE t.depth <"), "unbounded recursion: {sql}");
            }
        }
    }

    #[test]
    fn every_production_fires() {
        let (_, prods) = sweep(6000);
        for p in [
            "cterec:graph",
            "cterec:aux",
            "cterec:mat",
            "cterec:dml",
            "cterec:sublink",
            "cterec:union_all",
            "cterec:union",
            "cterec:search_none",
            "cterec:search_depth",
            "cterec:search_breadth",
            "cterec:cycle_none",
            "cterec:cycle_bool",
            "cterec:cycle_marked",
            "cterec:aux_plain",
            "cterec:aux_mat",
            "cterec:aux_nested",
            "cterec:dml_move",
            "cterec:dml_multi",
            "cterec:dml_visibility",
            "cterec:dml_chain",
            "cterec:any",
            "cterec:all",
            "cterec:rowcmp",
            "cterec:scalar",
            "cterec:toomany",
            "cterec:notin3vl",
            "cterec:in3vl",
            "cterec:corr_exists",
            "cterec:corr_in",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    #[test]
    fn key_surfaces_present() {
        let (sqls, _) = sweep(6000);
        let all = sqls.join("\n");
        for frag in [
            "SEARCH DEPTH FIRST BY",
            "SEARCH BREADTH FIRST BY",
            "CYCLE node SET ic USING pa",
            "CYCLE node SET ic TO 'Y' DEFAULT 'N' USING pa",
            "UNION SELECT",     // recursive UNION (distinct)
            "UNION ALL SELECT", // recursive UNION ALL
            "AS MATERIALIZED",
            "AS NOT MATERIALIZED",
            "DELETE FROM fz_cr_dm WHERE id = 2 RETURNING",
            "NOT IN (SELECT x FROM (VALUES",
            "= ANY (SELECT x FROM (VALUES",
            "<> ALL (SELECT x FROM (VALUES",
            "(1, 2) = ANY (SELECT a, b",
        ] {
            assert!(all.contains(frag), "surface {frag:?} never generated");
        }
    }

    #[test]
    fn seed_deterministic() {
        let (a, _) = sweep(500);
        let (b, _) = sweep(500);
        assert_eq!(a, b, "same seed must reproduce the stream");
    }
}
