//! compose: composition templates for `sitediff smoke --compose <p>`.
//!
//! The generator (`session::run_session`) emits statements one feature
//! at a time; the real bugs of the 2026-09-10 appbench batch came from
//! feature PAIRS it never composes (a SECURITY DEFINER SQL SRF at
//! ExecutorEnd, a plpgsql record with a mixed-case field, an aggregate
//! under a pulled-up subquery, a WITH HOLD cursor across session exit, a
//! multi-statement simple-query message). This module wraps a generated
//! row-returning statement in one of those shells without touching the
//! generator: the wrapped statement keeps its production tags and gains
//! `compose:<template>`, so a finding still attributes to the inner
//! production and to the shell.
//!
//! Only `SELECT` / `VALUES` / `TABLE` shapes (`recipe::shape_of`) are
//! wrapped; DML, DDL, SET and everything else pass through unchanged.
//! Every template renders the inner query's rows through `q::text`
//! (composite text form), so the wrapped result has one text column and
//! compares on the rows plane like any other statement; the inner
//! `ORDER BY` is not preserved through a subquery / function / cursor
//! shell, so a wrapped step is `ordered: none` (multiset compare) except
//! the cursor and prepared-statement shells, which replay the statement
//! verbatim and keep the generator's order witness.

use crate::contracts::Ordered;
use crate::recipe::{shape_of, Shape};
use crate::rng::Rng;

/// One statement of the composed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Composed {
    pub sql: String,
    pub productions: Vec<String>,
    pub ordered: Ordered,
    /// Index of the generated statement this step derives from.
    pub source: usize,
}

/// The composition shells. Each wraps one `SELECT`-shaped statement in
/// two or more wire statements; the last one is the query whose result
/// exposes the composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Template {
    /// SQL-language `SECURITY DEFINER` set-returning function, called in
    /// FROM (the PostgREST PR #2095 shape).
    SecdefSrf,
    /// plpgsql DO block iterating a mixed-case quoted `record` variable
    /// over the query, counting rows into a NOTICE (PR #2094 shape).
    PlpgsqlRecord,
    /// `WITH "Cte" AS MATERIALIZED (...)` body.
    Cte,
    /// `PREPARE` / `EXECUTE` / `DEALLOCATE`.
    Prepared,
    /// `WITH HOLD` cursor declared in a transaction, fetched after
    /// `COMMIT` (PR #2092 shape).
    HoldCursor,
    /// Aggregate over the query as a FROM-subquery with a sublink
    /// referencing the same query (PR #2089 / #2090 shape).
    AggSublink,
    /// The query and a second statement in ONE simple-query message
    /// (multi-statement, PR #2093 shape).
    MultiStatement,
}

impl Template {
    pub const ALL: [Template; 7] = [
        Template::SecdefSrf,
        Template::PlpgsqlRecord,
        Template::Cte,
        Template::Prepared,
        Template::HoldCursor,
        Template::AggSublink,
        Template::MultiStatement,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Template::SecdefSrf => "secdef-srf",
            Template::PlpgsqlRecord => "plpgsql-record",
            Template::Cte => "cte",
            Template::Prepared => "prepared",
            Template::HoldCursor => "hold-cursor",
            Template::AggSublink => "agg-sublink",
            Template::MultiStatement => "multi-statement",
        }
    }

    pub fn parse(s: &str) -> Option<Template> {
        Template::ALL.iter().copied().find(|t| t.name() == s)
    }

    /// Render the shell around `inner` (a `;`-free SELECT-shaped query).
    /// `ordered` is the inner statement's own order witness.
    pub fn render(self, inner: &str, ordered: Ordered) -> Vec<(String, Ordered)> {
        let n = Ordered::None;
        match self {
            Template::SecdefSrf => vec![
                (
                    format!(
                        "CREATE OR REPLACE FUNCTION fz_comp_srf() RETURNS SETOF text LANGUAGE sql SECURITY DEFINER SET search_path = public AS $fzc$ SELECT q::text FROM ({inner}) q $fzc$;"
                    ),
                    n,
                ),
                ("SELECT * FROM fz_comp_srf() AS r(v);".to_string(), n),
                ("DROP FUNCTION fz_comp_srf();".to_string(), n),
            ],
            Template::PlpgsqlRecord => vec![(
                format!(
                    "DO $fzc$ DECLARE \"Rec\" record; \"nRows\" bigint := 0; \"lastRow\" text; BEGIN FOR \"Rec\" IN {inner} LOOP \"nRows\" := \"nRows\" + 1; \"lastRow\" := \"Rec\"::text; END LOOP; RAISE NOTICE 'fz_comp rows=% last_len=%', \"nRows\", length(\"lastRow\"); END $fzc$;"
                ),
                n,
            )],
            Template::Cte => vec![(format!("WITH \"Cte\" AS MATERIALIZED ({inner}) SELECT \"Cte\"::text AS v FROM \"Cte\";"), n)],
            Template::Prepared => vec![
                (format!("PREPARE fz_comp_p AS {inner};"), n),
                ("EXECUTE fz_comp_p;".to_string(), ordered),
                ("DEALLOCATE fz_comp_p;".to_string(), n),
            ],
            Template::HoldCursor => vec![
                ("BEGIN;".to_string(), n),
                (format!("DECLARE fz_comp_c CURSOR WITH HOLD FOR {inner};"), n),
                ("COMMIT;".to_string(), n),
                ("FETCH ALL FROM fz_comp_c;".to_string(), ordered),
                ("CLOSE fz_comp_c;".to_string(), n),
            ],
            Template::AggSublink => vec![(
                format!(
                    "SELECT count(*) AS n, count(DISTINCT q::text) AS nd, (SELECT count(*) FROM ({inner}) q2 WHERE q2::text = (SELECT min(q3::text) FROM ({inner}) q3)) AS nmin FROM ({inner}) q;"
                ),
                Ordered::Total,
            )],
            Template::MultiStatement => vec![(
                format!("SELECT q::text AS v FROM ({inner}) q; SELECT count(*) AS n FROM ({inner}) q;"),
                n,
            )],
        }
    }
}

/// A statement is wrappable when it is SELECT-shaped, single, and does
/// not already contain the shell's dollar tag or a SRF-only construct
/// the shells cannot host (`INTO`, `FOR UPDATE` inside a SQL function).
pub fn wrappable(sql: &str) -> bool {
    let body = sql.trim().trim_end_matches(';').trim();
    if body.is_empty() || body.contains("$fzc$") || body.contains(';') {
        return false;
    }
    let upper = body.to_ascii_uppercase();
    if upper.contains(" INTO ") || upper.contains("FOR UPDATE") || upper.contains("FOR SHARE") || upper.contains("FOR NO KEY") || upper.contains("FOR KEY SHARE") {
        return false;
    }
    matches!(shape_of(body), Shape::Select | Shape::Values | Shape::Table)
}

/// Wrap each wrappable statement with probability `p` (per mille) using a
/// template drawn from `templates` by `rng`; everything else passes
/// through. `source` is the index into the input stream.
pub fn compose(stmts: &[(String, Vec<String>, Ordered)], p_per_mille: u64, templates: &[Template], rng: &mut Rng) -> Vec<Composed> {
    let mut out = Vec::with_capacity(stmts.len());
    for (i, (sql, productions, ordered)) in stmts.iter().enumerate() {
        let wrap = !templates.is_empty() && wrappable(sql) && rng.chance(p_per_mille, 1000);
        if !wrap {
            out.push(Composed { sql: sql.clone(), productions: productions.clone(), ordered: *ordered, source: i });
            continue;
        }
        let t = *rng.pick(templates);
        let inner = sql.trim().trim_end_matches(';').trim();
        let mut prods = productions.clone();
        prods.push(format!("compose:{}", t.name()));
        prods.sort();
        prods.dedup();
        for (s, o) in t.render(inner, *ordered) {
            out.push(Composed { sql: s, productions: prods.clone(), ordered: o, source: i });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrappable_is_select_shaped_only() {
        assert!(wrappable("SELECT 1;"));
        assert!(wrappable("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(wrappable("VALUES (1), (2);"));
        assert!(wrappable("TABLE t"));
        assert!(!wrappable("INSERT INTO t VALUES (1);"));
        assert!(!wrappable("SET x = 1;"));
        assert!(!wrappable("SELECT 1; SELECT 2;"));
        assert!(!wrappable("SELECT a FROM t FOR UPDATE"));
        assert!(!wrappable("SELECT $fzc$x$fzc$"));
    }

    #[test]
    fn templates_render_the_inner_query_once_per_shell_and_tag() {
        let stmts = vec![
            ("SELECT 1 AS a;".to_string(), vec!["expr.const".to_string()], Ordered::Total),
            ("INSERT INTO t VALUES (1);".to_string(), vec!["dml.insert".to_string()], Ordered::None),
        ];
        for t in Template::ALL {
            let mut rng = Rng::new(7);
            let out = compose(&stmts, 1000, &[t], &mut rng);
            let shell = t.render("SELECT 1 AS a", Ordered::Total);
            assert_eq!(out.len(), shell.len() + 1, "{}", t.name());
            for c in &out[..shell.len()] {
                assert!(c.productions.contains(&format!("compose:{}", t.name())));
                assert!(c.productions.contains(&"expr.const".to_string()));
                assert_eq!(c.source, 0);
            }
            assert_eq!(out.last().unwrap().sql, "INSERT INTO t VALUES (1);");
            assert_eq!(out.last().unwrap().source, 1);
            let last = shell.last().unwrap();
            assert!(last.0.ends_with(';'), "{}", last.0);
        }
    }

    #[test]
    fn p_zero_passes_through_and_p_full_wraps_every_select() {
        let stmts: Vec<(String, Vec<String>, Ordered)> =
            (0..20).map(|i| (format!("SELECT {i};"), vec![], Ordered::Total)).collect();
        let mut rng = Rng::new(1);
        assert_eq!(compose(&stmts, 0, &Template::ALL, &mut rng).len(), 20);
        let mut rng = Rng::new(1);
        let all = compose(&stmts, 1000, &Template::ALL, &mut rng);
        assert!(all.iter().all(|c| c.productions.iter().any(|p| p.starts_with("compose:"))));
        // Deterministic under the seed.
        let mut rng2 = Rng::new(1);
        assert_eq!(compose(&stmts, 1000, &Template::ALL, &mut rng2), all);
    }

    #[test]
    fn parse_names() {
        for t in Template::ALL {
            assert_eq!(Template::parse(t.name()), Some(t));
        }
        assert_eq!(Template::parse("nope"), None);
    }
}
