//! Subqueries statement module: scalar subqueries in the select list and
//! WHERE (comparison against a subquery), IN / NOT IN / EXISTS / NOT EXISTS
//! (correlated and uncorrelated — correlation happens through colref:outer
//! leaf draws against the enclosing scope), derived tables in FROM, and
//! simple non-recursive CTEs referenced once or twice.
//!
//! Determinism-of-value discipline: scalar subqueries draw their FROM from
//! at-most-one-row tables (fixture hint) or use no FROM at all, so they can
//! never raise more-than-one-row errors or return an order-dependent value.
//! Derived-table and CTE bodies are uncorrelated (plain, non-LATERAL FROM
//! subqueries must not reference the enclosing query) and never carry
//! ORDER BY/LIMIT (the F1 differ's ordered-compare detection is textual and
//! statement-level).

use crate::catalog::{Column, SqlType, Table};
use crate::expr::Expr;
use crate::render::{FromItem, JoinCond, JoinKind, SelectItem, SelectStmt};
use crate::scope::{Scope, ScopeRel};
use crate::stmt::{finish_select, Gen, StmtKind};

/// How deep expression-level subqueries may nest inside this module's
/// statements (a scalar subquery inside a scalar subquery, and no further).
const SUBQ_NESTING: u32 = 2;

pub fn gen_subq_stmt(g: &mut Gen) -> SelectStmt {
    g.subq_depth = SUBQ_NESTING;
    let stmt = match g.weights.pick(g.rng, &["subq:plain", "subq:derived", "subq:cte"]) {
        "subq:derived" => {
            g.fire("subq:derived");
            let (body, columns) = gen_simple_body(g);
            let alias = g.next_alias();
            let rel = ScopeRel { alias: alias.clone(), columns: columns.clone() };
            let from = FromItem::Derived {
                body: Box::new(body),
                alias,
                columns: columns.iter().map(|c| c.name.clone()).collect(),
            };
            finish_select(g, &[rel], from, Vec::new())
        }
        "subq:cte" => {
            g.fire("subq:cte");
            let (body, columns) = gen_simple_body(g);
            let cte_name = g.next_cte_name();
            let ctes = vec![(cte_name.clone(), body)];
            let alias_a = g.next_alias();
            let rel_a = ScopeRel { alias: alias_a.clone(), columns: columns.clone() };
            let ref_a = FromItem::Table { name: cte_name.clone(), alias: alias_a };
            if g.weights.pick(g.rng, &["cte:once", "cte:twice"]) == "cte:twice" {
                g.fire("cte:twice");
                let alias_b = g.next_alias();
                let rel_b = ScopeRel { alias: alias_b.clone(), columns };
                let ref_b = FromItem::Table { name: cte_name, alias: alias_b };
                let rels = vec![rel_a, rel_b];
                let scope = Scope { rels: &rels, outer: None };
                let on = g.gen_bool(&scope, 2);
                let from = FromItem::Join {
                    left: Box::new(ref_a),
                    right: Box::new(ref_b),
                    kind: JoinKind::Inner,
                    cond: JoinCond::On(on),
                };
                finish_select(g, &rels, from, ctes)
            } else {
                g.fire("cte:once");
                finish_select(g, &[rel_a], ref_a, ctes)
            }
        }
        _ => {
            g.fire("subq:plain");
            let table = g.pick_table();
            let alias = g.next_alias();
            let rel = ScopeRel::from_table(table, alias.clone());
            let from = FromItem::Table { name: table.name.clone(), alias };
            finish_select(g, &[rel], from, Vec::new())
        }
    };
    g.subq_depth = 0;
    stmt
}

/// Small uncorrelated single-table SELECT used as a derived-table or CTE
/// body: 1-3 aliased output columns (c0, c1, ...), optional WHERE, no
/// subqueries inside, no ORDER BY/LIMIT. Returns the body plus the column
/// shape it exposes to the outer scope.
fn gen_simple_body(g: &mut Gen) -> (SelectStmt, Vec<Column>) {
    let saved = g.subq_depth;
    g.subq_depth = 0;
    let table = g.pick_table();
    let alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(table, alias.clone())];
    let scope = Scope { rels: &rels, outer: None };
    let ncols = 1 + g.rng.below_usize(3);
    let mut items = Vec::with_capacity(ncols);
    let mut columns = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let ty = g.any_type(&scope);
        let expr = g.gen_typed(&scope, ty, 2);
        let name = format!("c{}", i);
        items.push(SelectItem { expr, alias: Some(name.clone()), ty });
        columns.push(Column { name, ty, nullable: true, ddl_type: None });
    }
    let where_clause = if g.rng.chance(1, 2) { Some(g.gen_bool(&scope, 2)) } else { None };
    g.subq_depth = saved;
    let body = SelectStmt {
        items,
        from: Some(FromItem::Table { name: table.name.clone(), alias }),
        where_clause,
        ..Default::default()
    };
    (body, columns)
}

/// Recursive CTEs with SEARCH / CYCLE clauses (X1 gap: rewriteSearchAndCycle,
/// gap-report-004 rank 4), rendered as self-contained raw statements — no
/// fixture dependency, so every row multiset is deterministic by
/// construction and the differ's no-ORDER-BY multiset compare applies.
///
/// Two body families:
///   - a terminating chain `(n, v)` (n counts 1..depth) for plain recursion
///     and SEARCH DEPTH|BREADTH FIRST (SEARCH alone must not need cycle
///     detection to terminate);
///   - a modular walk `x -> (x % m) + 1` that always revisits a value, for
///     CYCLE (the CYCLE mark is what terminates it) and SEARCH+CYCLE
///     combined.
///
/// The SEARCH/CYCLE output columns (ord / is_c / path) are projected so
/// their row-composite renderings are differential surface too; both are
/// deterministic given the deterministic bodies.
pub fn gen_recursive_cte_stmt(g: &mut Gen) -> StmtKind {
    g.fire("subq:rec");
    let shape = g.weights.pick(
        g.rng,
        &["subq:rec:plain", "subq:rec:search", "subq:rec:cycle", "subq:rec:both"],
    );
    g.fire(shape);
    let pick_dir = |g: &mut Gen| {
        if g.weights.pick(g.rng, &["subq:rec:depth", "subq:rec:breadth"])
            == "subq:rec:depth"
        {
            g.fire("subq:rec:depth");
            "DEPTH"
        } else {
            g.fire("subq:rec:breadth");
            "BREADTH"
        }
    };
    match shape {
        "subq:rec:cycle" | "subq:rec:both" => {
            // Modular walk: SELECT s, then (x % m) + 1 forever; CYCLE stops
            // it at the first revisit. Rows: s, then the 1..m orbit.
            let m = 3 + g.rng.below(5); // orbit size 3..7
            let s = 1 + g.rng.below(m); // start inside the orbit
            let search = if shape == "subq:rec:both" {
                let dir = pick_dir(g);
                format!(" SEARCH {dir} FIRST BY x SET ord")
            } else {
                String::new()
            };
            let proj_path = g.rng.chance(1, 2);
            let mut cols = "x, is_c".to_string();
            if shape == "subq:rec:both" {
                cols.push_str(", ord");
            }
            if proj_path {
                cols.push_str(", p");
            }
            StmtKind::Raw(format!(
                "WITH RECURSIVE w0(x) AS (SELECT {s} UNION ALL \
                 SELECT (x % {m}) + 1 FROM w0){search} CYCLE x SET is_c USING p \
                 SELECT {cols} FROM w0;"
            ))
        }
        _ => {
            // Terminating chain: n counts up to depth, v accumulates.
            let depth = 3 + g.rng.below(10); // 3..12
            let start = g.rng.below(9);
            let step = ["v + n", "v * 2", "v - 3", "v + 7"]
                [g.rng.below_usize(4)];
            let (search, ord) = if shape == "subq:rec:search" {
                let dir = pick_dir(g);
                let by = if g.rng.chance(1, 2) { "n" } else { "n, v" };
                (format!(" SEARCH {dir} FIRST BY {by} SET ord"), ", ord")
            } else {
                (String::new(), "")
            };
            StmtKind::Raw(format!(
                "WITH RECURSIVE w0(n, v) AS (SELECT 1, {start} UNION ALL \
                 SELECT n + 1, {step} FROM w0 WHERE n < {depth}){search} \
                 SELECT n, v{ord} FROM w0;"
            ))
        }
    }
}

/// A subquery body's inner scope plus the outer chain, with the nesting
/// budget spent for the duration of `f`.
fn with_subq_body<R>(g: &mut Gen, f: impl FnOnce(&mut Gen) -> R) -> R {
    debug_assert!(g.subq_depth > 0);
    g.subq_depth -= 1;
    let r = f(g);
    g.subq_depth += 1;
    r
}

/// Scalar subquery of `ty`: `(SELECT <expr> [FROM one-row-table] [WHERE ...])`,
/// correlated via colref:outer draws against `outer`.
pub fn gen_scalar_subq(g: &mut Gen, outer: &Scope, ty: SqlType) -> Expr {
    g.fire("subq:scalar");
    let single_row: Vec<&Table> =
        g.catalog.tables.iter().filter(|t| t.at_most_one_row).collect();
    let use_table = !single_row.is_empty()
        && g.weights.pick(g.rng, &["subq:scalar_table", "subq:scalar_nofrom"])
            == "subq:scalar_table";
    let body = with_subq_body(g, |g| {
        if use_table {
            g.fire("subq:scalar_table");
            let table = single_row[g.rng.below_usize(single_row.len())];
            let alias = g.next_alias();
            let rels = vec![ScopeRel::from_table(table, alias.clone())];
            let scope = Scope { rels: &rels, outer: Some(outer) };
            let expr = g.gen_typed(&scope, ty, 2);
            let where_clause =
                if g.rng.chance(1, 2) { Some(g.gen_bool(&scope, 2)) } else { None };
            SelectStmt {
                items: vec![SelectItem { expr, alias: None, ty }],
                from: Some(FromItem::Table { name: table.name.clone(), alias }),
                where_clause,
                ..Default::default()
            }
        } else {
            g.fire("subq:scalar_nofrom");
            // FROM-less scalar subquery: one row by construction; leaves
            // draw literals or outer references (correlation).
            let scope = Scope { rels: &[], outer: Some(outer) };
            let expr = g.gen_typed(&scope, ty, 2);
            SelectStmt {
                items: vec![SelectItem { expr, alias: None, ty }],
                ..Default::default()
            }
        }
    });
    Expr::ScalarSubq { body: Box::new(body) }
}

/// `<expr> <cmp> (scalar subquery)` in a boolean position.
pub fn gen_cmp_subq(g: &mut Gen, outer: &Scope) -> Expr {
    g.fire("subq:cmp");
    let ty = g.any_type(outer);
    let lhs = g.gen_typed(outer, ty, 2);
    let op = g.pick_cmp_op();
    let rhs = gen_scalar_subq(g, outer, ty);
    Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
}

/// `<expr> [NOT] IN (SELECT <expr> FROM t [WHERE ...])`. NULLs on either
/// side exercise the classic three-valued IN semantics.
pub fn gen_in_subq(g: &mut Gen, outer: &Scope) -> Expr {
    let negated = g.weights.pick(g.rng, &["subq:in", "subq:not_in"]) == "subq:not_in";
    g.fire(if negated { "subq:not_in" } else { "subq:in" });
    let table = g.pick_table();
    let alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(table, alias.clone())];
    let (ty, body) = with_subq_body(g, |g| {
        let scope = Scope { rels: &rels, outer: Some(outer) };
        let ty = g.any_type(&scope);
        let expr = g.gen_typed(&scope, ty, 2);
        let where_clause =
            if g.rng.chance(1, 2) { Some(g.gen_bool(&scope, 2)) } else { None };
        let body = SelectStmt {
            items: vec![SelectItem { expr, alias: None, ty }],
            from: Some(FromItem::Table { name: table.name.clone(), alias: alias.clone() }),
            where_clause,
            ..Default::default()
        };
        (ty, body)
    });
    let lhs = g.gen_typed(outer, ty, 2);
    Expr::InSubq { lhs: Box::new(lhs), negated, body: Box::new(body) }
}

/// `[NOT] EXISTS (SELECT 1 FROM t [WHERE ...])`; the WHERE is where
/// correlation usually lands.
pub fn gen_exists_subq(g: &mut Gen, outer: &Scope) -> Expr {
    let negated =
        g.weights.pick(g.rng, &["subq:exists", "subq:not_exists"]) == "subq:not_exists";
    g.fire(if negated { "subq:not_exists" } else { "subq:exists" });
    let table = g.pick_table();
    let alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(table, alias.clone())];
    let body = with_subq_body(g, |g| {
        let scope = Scope { rels: &rels, outer: Some(outer) };
        let where_clause =
            if g.rng.chance(5, 6) { Some(g.gen_bool(&scope, 2)) } else { None };
        SelectStmt {
            items: vec![SelectItem {
                expr: Expr::Lit { sql: "1".to_string() },
                alias: None,
                ty: SqlType::Int4,
            }],
            from: Some(FromItem::Table { name: table.name.clone(), alias: alias.clone() }),
            where_clause,
            ..Default::default()
        }
    });
    Expr::Exists { negated, body: Box::new(body) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::render::scope_errors;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    #[test]
    fn subq_statements_are_scoped_and_varied() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(31337);
        let mut sqls = String::new();
        let mut prods_all: Vec<String> = Vec::new();
        for i in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmt = gen_subq_stmt(&mut g);
            let errs = scope_errors(&stmt, &cat);
            assert!(errs.is_empty(), "stmt {i}: {errs:?}\n{}", stmt.to_sql());
            sqls.push_str(&stmt.to_sql());
            sqls.push('\n');
            prods_all.extend(prods);
        }
        for frag in ["IN (SELECT", "NOT IN (SELECT", "EXISTS (SELECT", "WITH w", ") AS t"] {
            assert!(sqls.contains(frag), "subquery flavor {frag:?} never generated");
        }
        for p in [
            "subq:scalar",
            "subq:scalar_nofrom",
            "subq:scalar_table",
            "subq:cmp",
            "subq:in",
            "subq:exists",
            "subq:derived",
            "subq:cte",
            "cte:once",
            "cte:twice",
            "colref:outer",
        ] {
            assert!(
                prods_all.iter().any(|q| q == p),
                "production {p} never fired in 600 statements"
            );
        }
        // No ORDER BY ever leaks into a subquery: any ORDER BY present must
        // belong to the top level, i.e. appear after the last closing paren
        // of every nested SELECT. Cheap textual proxy: statements whose
        // ORDER BY is followed by another SELECT keyword are impossible.
        for line in sqls.lines() {
            if let Some(pos) = line.find(" ORDER BY ") {
                assert!(
                    !line[pos..].contains("SELECT"),
                    "ORDER BY inside a subquery: {line}"
                );
            }
        }
    }

    /// Recursive-CTE shapes (X1): every flavor fires, every statement is a
    /// single well-formed line, and — the load-bearing invariant — every
    /// body terminates. A non-terminating recursion would hang the
    /// differential: the plain/SEARCH family must carry a depth-bounding
    /// WHERE, and the CYCLE family must carry the CYCLE clause that stops it.
    #[test]
    fn recursive_ctes_terminate_and_cover_search_cycle() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x5EA4C);
        let mut sqls = Vec::new();
        let mut prods_all: Vec<String> = Vec::new();
        for _ in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            sqls.push(gen_recursive_cte_stmt(&mut g).to_sql());
            prods_all.extend(prods);
        }
        for sql in &sqls {
            assert!(sql.starts_with("WITH RECURSIVE w0("), "{sql}");
            assert!(sql.ends_with(';') && !sql.contains('\n'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            // Termination: either a depth bound or a CYCLE mark, never
            // neither.
            let bounded = sql.contains(" WHERE n < ");
            let cycled = sql.contains(" CYCLE x SET is_c USING p ");
            assert!(bounded || cycled, "unbounded recursion: {sql}");
            // The modular-walk body is the CYCLE family's alone: it never
            // terminates on its own.
            if sql.contains("(x % ") {
                assert!(cycled, "modular walk without CYCLE: {sql}");
            }
            // SEARCH always names a column it SETs and projects.
            if let Some(p) = sql.find(" SEARCH ") {
                assert!(sql[p..].contains(" FIRST BY "), "{sql}");
                assert!(sql[p..].contains(" SET ord"), "{sql}");
                assert!(sql.contains(", ord"), "SEARCH column not projected: {sql}");
            }
        }
        for p in [
            "subq:rec",
            "subq:rec:plain",
            "subq:rec:search",
            "subq:rec:cycle",
            "subq:rec:both",
            "subq:rec:depth",
            "subq:rec:breadth",
        ] {
            assert!(prods_all.iter().any(|q| q == p), "production {p} never fired");
        }
        for frag in [
            "SEARCH DEPTH FIRST BY ",
            "SEARCH BREADTH FIRST BY ",
            "CYCLE x SET is_c USING p",
            "UNION ALL",
        ] {
            assert!(sqls.iter().any(|s| s.contains(frag)), "flavor {frag:?} never generated");
        }
        // Deterministic.
        let mut rng2 = Rng::new(0x5EA4C);
        let mut again = Vec::new();
        for _ in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng2, &cat, &w, &mut prods, 4);
            again.push(gen_recursive_cte_stmt(&mut g).to_sql());
        }
        assert_eq!(sqls, again);
    }

    #[test]
    fn scalar_subqueries_use_single_row_sources_only() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        // Scalar subqueries only (IN/EXISTS/cmp and derived/CTE shapes off),
        // so every parenthesized SELECT in the output is a scalar subquery.
        let w = WeightTable::parse(
            "subq:scalar=50,subq:cmp=0,subq:in=0,subq:exists=0,\
             subq:derived=0,subq:cte=0,subq:plain=1",
        )
        .unwrap();
        let mut rng = Rng::new(2);
        let mut saw_scalar_from = false;
        for _ in 0..200 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmt = gen_subq_stmt(&mut g);
            let sql = stmt.to_sql();
            // Every scalar-subquery FROM names an at-most-one-row table.
            let mut rest = sql.as_str();
            while let Some(p) = rest.find("(SELECT ") {
                rest = &rest[p + 1..];
                let end = rest.find(')').unwrap_or(rest.len());
                if let Some(f) = rest[..end].find(" FROM ") {
                    let after = &rest[f + " FROM ".len()..];
                    let name: String = after
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect();
                    assert!(
                        ["fz_one", "fz_empty"].contains(&name.as_str()),
                        "scalar subquery over multi-row table {name} in {sql}"
                    );
                    saw_scalar_from = true;
                }
            }
            assert!(scope_errors(&stmt, &cat).is_empty());
        }
        assert!(saw_scalar_from, "no scalar subquery with FROM generated");
    }
}
