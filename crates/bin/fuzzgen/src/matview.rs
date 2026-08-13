//! Materialized-view + view-DDL drain module (Track-B SQL-drainable): the
//! createas.c / matview.c / view.c surfaces that the pre-existing `views`
//! module does NOT reach. `views` owns the persistent registered-view DML
//! surface (cross-module rewriteTargetView traffic), a single flat matview
//! shape (`SELECT pk,k_int,k_text FROM src`) that ALWAYS carries its unique
//! index and is refreshed immediately, and the rule/deparse families. This
//! module is the complement — it targets the matview/CTAS/view arms `views`
//! leaves cold:
//!
//!   - matview CONTENT after refresh over joins / aggregates / DISTINCT
//!     (createas.c `create_ctas_internal` + the refresh recompute), compared
//!     under a total ORDER BY. A content divergence here is the HIGH-severity
//!     surface: the matview's stored heap disagreeing between engines;
//!   - matview scannability: `SELECT` from a `WITH NO DATA` matview must
//!     raise `55000` ("materialized view \"...\" has not been populated")
//!     — executils' scannability gate, never reached by `views` (it refreshes
//!     a no-data matview before ever reading it);
//!   - `refresh_by_match_merge` (matview.c): REFRESH ... CONCURRENTLY after
//!     the base table has been mutated (insert + update + delete), so the
//!     full-join diff/merge path actually has a diff to apply, then the
//!     content is compared. `views` refreshes concurrently but never churns
//!     the source between refreshes, so the merge always sees an empty diff;
//!   - the REFRESH CONCURRENTLY error arms `views` avoids by construction:
//!     concurrently WITHOUT a unique index (`0A000`/OBJECT_NOT_IN_
//!     PREREQUISITE_STATE, matview.c line ~173) and concurrently on a
//!     not-yet-populated matview (FEATURE_NOT_SUPPORTED, matview.c line
//!     ~117);
//!   - CREATE TABLE AS (WITH DATA / WITH NO DATA), SELECT ... INTO, and
//!     CREATE TABLE AS over an aggregate — the createas.c intorel receive
//!     path for plain relations, which `views` does not touch at all;
//!   - view.c reloption + column-set arms `views` skips: security_barrier /
//!     security_invoker views (option parse + reloptions storage + DML
//!     through a barrier view), CREATE OR REPLACE VIEW that APPENDS columns
//!     (view.c's checkViewColumns "may only add columns" path — `views`
//!     replace deliberately keeps the column list fixed), and CREATE
//!     RECURSIVE VIEW (the RECURSIVE-view rewrite into a WITH-RECURSIVE
//!     query).
//!
//! Every arm is GROUP-LOCAL: it creates its own base table(s) with
//! deterministic literal rows, builds the object, exercises it, compares its
//! reads, and drops everything inside the one returned statement group. That
//! keeps the module self-contained — no persistent catalog entry, no
//! cross-module dependency edge, no probe-window bookkeeping — so it can
//! never wedge a sibling module's DROP and the per-module isolation test
//! (`all_modules_generate_scoped_statements`) sees a clean create→drop group
//! every call. The only session-persistent state is a monotonic name
//! counter, so object names stay unique across groups.
//!
//! Determinism / parity disciplines (the differential runs identical SQL on
//! REL_18_3 C and pgrust — same seed ⇒ same SQL ⇒ identical results unless
//! the engines genuinely diverge):
//!   - every compared read carries a TOTAL ORDER BY over integer columns
//!     only (pk / group key), so no residual tie and no text-collation
//!     dependence can make an ordering nondeterministic (coll:pinned=0);
//!   - all data is small int4 literals: no float reassociation, no 22003
//!     overflow, no toast;
//!   - pks are dense from 1 and every mutation targets an in-range pk, so no
//!     23505 / 23502 noise; the deliberate error arms (scannability, the two
//!     CONCURRENTLY arms) are matched errors on both engines, exactly like
//!     the `views` module's WITH-CHECK-OPTION violations.

use crate::rng::Rng;
use crate::stmt::{Gen, StmtKind};

/// Session-persistent state: just the object-name counter (all objects are
/// group-local, so nothing else needs to survive across groups).
#[derive(Clone, Debug, Default)]
pub struct MatviewState {
    next: u32,
}

impl MatviewState {
    pub fn new() -> MatviewState {
        MatviewState::default()
    }

    fn fresh(&mut self) -> u32 {
        let n = self.next;
        self.next += 1;
        n
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_matview_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview");
    let action = g.weights.pick(
        g.rng,
        &[
            "matview:content",
            "matview:scan",
            "matview:merge",
            "matview:nouniq",
            "matview:ctas",
            "matview:secview",
            "matview:replace",
            "matview:recursive",
        ],
    );
    match action {
        "matview:content" => gen_content(g),
        "matview:scan" => gen_scan(g),
        "matview:merge" => gen_merge(g),
        "matview:nouniq" => gen_nouniq(g),
        "matview:ctas" => gen_ctas(g),
        "matview:secview" => gen_secview(g),
        "matview:replace" => gen_replace(g),
        _ => gen_recursive(g),
    }
}

// -------------------------------------------------------------- helpers ----

/// A base row's (g, val) literal, each independently maybe-NULL. Deterministic
/// per RNG draw; identical SQL text reaches both engines.
fn gval(g: &mut Gen) -> (String, String) {
    let gv = if g.rng.chance(1, 6) {
        "NULL".to_string()
    } else {
        g.rng.range_i64(-1, 3).to_string()
    };
    let vv = if g.rng.chance(1, 6) {
        "NULL".to_string()
    } else {
        g.rng.range_i64(-50, 50).to_string()
    };
    (gv, vv)
}

/// CREATE + seed a base table `(pk int4 PRIMARY KEY, g int4, val int4)` with
/// `n` dense rows (pk 1..=n). Returns the statements and `n` (the next free
/// pk is `n + 1`).
fn seed_base(g: &mut Gen, name: &str, n: i64) -> (Vec<StmtKind>, i64) {
    let mut stmts = vec![StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, g int4, val int4);",
        name
    ))];
    for pk in 1..=n {
        let (gv, vv) = gval(g);
        stmts.push(StmtKind::Raw(format!(
            "INSERT INTO {} VALUES ({}, {}, {});",
            name, pk, gv, vv
        )));
    }
    (stmts, n)
}

fn n_rows(rng: &mut Rng) -> i64 {
    4 + rng.range_i64(0, 4)
}

// ------------------------------------------------ matview content arms ----

/// Matview over a join / aggregate / DISTINCT, its content compared under a
/// total ORDER BY, then the base mutated and a plain REFRESH recomputes it —
/// the content-after-refresh HIGH-severity surface.
fn gen_content(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:content");
    let n = g.matview.fresh();
    let shape = g.weights.pick(
        g.rng,
        &["matview:content:join", "matview:content:agg", "matview:content:distinct"],
    );
    g.fire(shape);
    let mv = format!("fz_mvx_{}", n);
    match shape {
        "matview:content:join" => {
            let b1 = format!("fz_mvxb_{}a", n);
            let b2 = format!("fz_mvxb_{}b", n);
            let rows1 = n_rows(g.rng);
            let rows2 = n_rows(g.rng);
            let (mut stmts, c1) = seed_base(g, &b1, rows1);
            let (s2, _) = seed_base(g, &b2, rows2);
            stmts.extend(s2);
            stmts.push(StmtKind::Raw(format!(
                "CREATE MATERIALIZED VIEW {mv} AS SELECT a.pk AS p1, b.pk AS p2, (a.val + b.val) \
                 AS s FROM {b1} a JOIN {b2} b ON a.g = b.g;",
                mv = mv,
                b1 = b1,
                b2 = b2
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY p1, p2;",
                mv
            )));
            // Mutate a base, then a plain REFRESH must recompute identical
            // content on both engines.
            let (gv, vv) = gval(g);
            stmts.push(StmtKind::Raw(format!(
                "INSERT INTO {} VALUES ({}, {}, {});",
                b1,
                c1 + 1,
                gv,
                vv
            )));
            stmts.push(StmtKind::Raw(format!("REFRESH MATERIALIZED VIEW {};", mv)));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY p1, p2;",
                mv
            )));
            stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
            stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b1)));
            stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b2)));
            stmts
        }
        "matview:content:distinct" => {
            let b = format!("fz_mvxb_{}", n);
            let rows = n_rows(g.rng);
            let (mut stmts, c) = seed_base(g, &b, rows);
            stmts.push(StmtKind::Raw(format!(
                "CREATE MATERIALIZED VIEW {} AS SELECT DISTINCT g FROM {};",
                mv, b
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY g NULLS LAST;",
                mv
            )));
            let (gv, vv) = gval(g);
            stmts.push(StmtKind::Raw(format!(
                "INSERT INTO {} VALUES ({}, {}, {});",
                b,
                c + 1,
                gv,
                vv
            )));
            stmts.push(StmtKind::Raw(format!("REFRESH MATERIALIZED VIEW {};", mv)));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY g NULLS LAST;",
                mv
            )));
            stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
            stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
            stmts
        }
        _ => {
            // Aggregate: g is the (unique) group key, so ORDER BY g is total.
            let b = format!("fz_mvxb_{}", n);
            let rows = n_rows(g.rng);
            let (mut stmts, c) = seed_base(g, &b, rows);
            stmts.push(StmtKind::Raw(format!(
                "CREATE MATERIALIZED VIEW {} AS SELECT g, count(*) AS c, sum(val) AS s, \
                 min(val) AS mn, max(val) AS mx FROM {} GROUP BY g;",
                mv, b
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY g NULLS LAST;",
                mv
            )));
            let (gv, vv) = gval(g);
            stmts.push(StmtKind::Raw(format!(
                "INSERT INTO {} VALUES ({}, {}, {});",
                b,
                c + 1,
                gv,
                vv
            )));
            stmts.push(StmtKind::Raw(format!("REFRESH MATERIALIZED VIEW {};", mv)));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY g NULLS LAST;",
                mv
            )));
            stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
            stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
            stmts
        }
    }
}

// ------------------------------------------------- scannability arm -------

/// WITH NO DATA matview: reading it unpopulated must raise the scannability
/// error (55000) on both engines; a plain REFRESH then populates it and the
/// content is compared. A second variant attempts REFRESH ... CONCURRENTLY on
/// the still-unpopulated matview (matview.c: FEATURE_NOT_SUPPORTED) before the
/// plain refresh.
fn gen_scan(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:scan");
    let n = g.matview.fresh();
    let b = format!("fz_mvxb_{}", n);
    let mv = format!("fz_mvx_{}", n);
    let rows = n_rows(g.rng);
    let (mut stmts, _) = seed_base(g, &b, rows);
    stmts.push(StmtKind::Raw(format!(
        "CREATE MATERIALIZED VIEW {} AS SELECT pk, g, val FROM {} WITH NO DATA;",
        mv, b
    )));
    // Unpopulated read → 55000 (scannability gate).
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", mv)));
    let variant = g.weights.pick(g.rng, &["matview:scan:plain", "matview:scan:concurrent"]);
    g.fire(variant);
    if variant == "matview:scan:concurrent" {
        // CONCURRENTLY on a not-yet-populated matview → FEATURE_NOT_SUPPORTED
        // (a unique index is present so this reaches the populated check).
        stmts.push(StmtKind::Raw(format!(
            "CREATE UNIQUE INDEX {}_ux ON {} (pk);",
            mv, mv
        )));
        stmts.push(StmtKind::Raw(format!(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY {};",
            mv
        )));
    }
    // Plain refresh populates it; the content is then scannable and compared.
    stmts.push(StmtKind::Raw(format!("REFRESH MATERIALIZED VIEW {};", mv)));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", mv)));
    stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
    stmts
}

// --------------------------------------------- refresh_by_match_merge -----

/// Populated matview WITH a unique index; the base is churned (insert +
/// update + delete) between REFRESH ... CONCURRENTLY passes so the match-merge
/// full-join diff actually applies inserts, updates and deletes, and the
/// content is compared after each concurrent refresh — the diff/merge path
/// (matview.c refresh_by_match_merge).
fn gen_merge(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:merge");
    let n = g.matview.fresh();
    let b = format!("fz_mvxb_{}", n);
    let mv = format!("fz_mvx_{}", n);
    let rows = 5 + g.rng.range_i64(0, 3);
    let (mut stmts, mut c) = seed_base(g, &b, rows);
    stmts.push(StmtKind::Raw(format!(
        "CREATE MATERIALIZED VIEW {} AS SELECT pk, g, val FROM {};",
        mv, b
    )));
    stmts.push(StmtKind::Raw(format!(
        "CREATE UNIQUE INDEX {}_ux ON {} (pk);",
        mv, mv
    )));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", mv)));

    // Two churn+concurrent-refresh rounds, each with an insert, an update, and
    // a delete so the merge diff carries all three change kinds.
    for _ in 0..2 {
        let (gv, vv) = gval(g);
        stmts.push(StmtKind::Raw(format!(
            "INSERT INTO {} VALUES ({}, {}, {});",
            b,
            c + 1,
            gv,
            vv
        )));
        c += 1;
        let upd = 1 + g.rng.range_i64(0, c - 1);
        stmts.push(StmtKind::Raw(format!(
            "UPDATE {} SET val = {} WHERE pk = {};",
            b,
            g.rng.range_i64(-50, 50),
            upd
        )));
        let del = 1 + g.rng.range_i64(0, c - 1);
        stmts.push(StmtKind::Raw(format!("DELETE FROM {} WHERE pk = {};", b, del)));
        stmts.push(StmtKind::Raw(format!(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY {};",
            mv
        )));
        stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", mv)));
    }
    stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
    stmts
}

// ------------------------------------------ REFRESH CONCURRENTLY error -----

/// Populated matview WITHOUT a unique index: REFRESH ... CONCURRENTLY must
/// raise OBJECT_NOT_IN_PREREQUISITE_STATE ("cannot refresh ... concurrently")
/// on both engines; a plain refresh then succeeds and the content is compared.
fn gen_nouniq(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:nouniq");
    let n = g.matview.fresh();
    let b = format!("fz_mvxb_{}", n);
    let mv = format!("fz_mvx_{}", n);
    let rows = n_rows(g.rng);
    let (mut stmts, _) = seed_base(g, &b, rows);
    stmts.push(StmtKind::Raw(format!(
        "CREATE MATERIALIZED VIEW {} AS SELECT pk, g, val FROM {};",
        mv, b
    )));
    // No unique index → concurrent refresh is a matched error.
    stmts.push(StmtKind::Raw(format!(
        "REFRESH MATERIALIZED VIEW CONCURRENTLY {};",
        mv
    )));
    stmts.push(StmtKind::Raw(format!("REFRESH MATERIALIZED VIEW {};", mv)));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", mv)));
    stmts.push(StmtKind::Raw(format!("DROP MATERIALIZED VIEW {};", mv)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
    stmts
}

// --------------------------------------------- CREATE TABLE AS / INTO ------

/// createas.c intorel path for plain relations: CREATE TABLE AS (WITH DATA /
/// WITH NO DATA), SELECT ... INTO, and CREATE TABLE AS over an aggregate.
fn gen_ctas(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:ctas");
    let n = g.matview.fresh();
    let b = format!("fz_mvxb_{}", n);
    let t = format!("fz_mvxt_{}", n);
    let rows = n_rows(g.rng);
    let (mut stmts, _) = seed_base(g, &b, rows);
    let shape = g.weights.pick(
        g.rng,
        &[
            "matview:ctas:data",
            "matview:ctas:nodata",
            "matview:ctas:into",
            "matview:ctas:agg",
        ],
    );
    g.fire(shape);
    match shape {
        "matview:ctas:nodata" => {
            // WITH NO DATA on a plain table AS is scannable (unlike a matview)
            // and simply returns zero rows.
            stmts.push(StmtKind::Raw(format!(
                "CREATE TABLE {} AS SELECT pk, g, val FROM {} WITH NO DATA;",
                t, b
            )));
            stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", t)));
        }
        "matview:ctas:into" => {
            stmts.push(StmtKind::Raw(format!(
                "SELECT pk, g, val INTO {} FROM {} WHERE g >= 0;",
                t, b
            )));
            stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", t)));
        }
        "matview:ctas:agg" => {
            stmts.push(StmtKind::Raw(format!(
                "CREATE TABLE {} AS SELECT g, count(*) AS c, sum(val) AS s FROM {} GROUP BY g;",
                t, b
            )));
            stmts.push(StmtKind::Raw(format!(
                "SELECT * FROM {} ORDER BY g NULLS LAST;",
                t
            )));
        }
        _ => {
            stmts.push(StmtKind::Raw(format!(
                "CREATE TABLE {} AS SELECT pk, g, val FROM {};",
                t, b
            )));
            stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", t)));
        }
    }
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", t)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
    stmts
}

// --------------------------------------- security_barrier / _invoker -------

/// view.c reloption arms: security_barrier / security_invoker views, their
/// content read, a deparse probe, and (for the auto-updatable barrier shape) a
/// write through the view with the base state compared afterwards.
fn gen_secview(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:secview");
    let n = g.matview.fresh();
    let b = format!("fz_mvxb_{}", n);
    let v = format!("fz_mvxv_{}", n);
    let rows = n_rows(g.rng);
    let (mut stmts, _) = seed_base(g, &b, rows);
    let shape = g.weights.pick(
        g.rng,
        &["matview:sec:barrier", "matview:sec:invoker", "matview:sec:both"],
    );
    g.fire(shape);
    let opts = match shape {
        "matview:sec:invoker" => "security_invoker=true",
        "matview:sec:both" => "security_barrier=true, security_invoker=true",
        _ => "security_barrier=true",
    };
    stmts.push(StmtKind::Raw(format!(
        "CREATE VIEW {} WITH ({}) AS SELECT pk, g, val FROM {} WHERE g >= 0;",
        v, opts, b
    )));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", v)));
    stmts.push(StmtKind::Raw(format!(
        "SELECT pg_get_viewdef('{}'::regclass, true);",
        v
    )));
    // The view is a single-relation plain projection (auto-updatable even with
    // security_barrier); a write through it, then the base is compared.
    if g.weights.pick(g.rng, &["matview:sec:write", "matview:sec:read"]) == "matview:sec:write" {
        g.fire("matview:sec:write");
        stmts.push(StmtKind::Raw(format!(
            "UPDATE {} SET val = {} WHERE pk = 1;",
            v,
            g.rng.range_i64(-50, 50)
        )));
        stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", b)));
    }
    stmts.push(StmtKind::Raw(format!("DROP VIEW {};", v)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
    stmts
}

// ---------------------------------- CREATE OR REPLACE VIEW (add cols) -------

/// view.c CREATE OR REPLACE VIEW column-append path: a view is replaced with a
/// SELECT list that APPENDS a column (the only column-set change REPLACE
/// allows), then again with an added WHERE; the content is compared at each
/// step.
fn gen_replace(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:replace");
    let n = g.matview.fresh();
    let b = format!("fz_mvxb_{}", n);
    let v = format!("fz_mvxv_{}", n);
    let rows = n_rows(g.rng);
    let (mut stmts, _) = seed_base(g, &b, rows);
    stmts.push(StmtKind::Raw(format!(
        "CREATE VIEW {} AS SELECT pk, g FROM {};",
        v, b
    )));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", v)));
    // Append a column (existing columns must stay identical and in order).
    stmts.push(StmtKind::Raw(format!(
        "CREATE OR REPLACE VIEW {} AS SELECT pk, g, val FROM {};",
        v, b
    )));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", v)));
    // Same column list, add a WHERE (row set changes, columns do not).
    stmts.push(StmtKind::Raw(format!(
        "CREATE OR REPLACE VIEW {} AS SELECT pk, g, val FROM {} WHERE g >= 0;",
        v, b
    )));
    stmts.push(StmtKind::Raw(format!("SELECT * FROM {} ORDER BY pk;", v)));
    stmts.push(StmtKind::Raw(format!(
        "SELECT pg_get_viewdef('{}'::regclass, true);",
        v
    )));
    stmts.push(StmtKind::Raw(format!("DROP VIEW {};", v)));
    stmts.push(StmtKind::Raw(format!("DROP TABLE {};", b)));
    stmts
}

// ------------------------------------------------ CREATE RECURSIVE VIEW ----

/// view.c RECURSIVE-view rewrite (into a WITH RECURSIVE query): a bounded
/// counting recursion, single- or two-column, read under a total ORDER BY and
/// deparsed. Self-contained (no base table).
fn gen_recursive(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("matview:recursive");
    let n = g.matview.fresh();
    let v = format!("fz_mvxv_{}", n);
    let bound = 4 + g.rng.range_i64(0, 8);
    let shape = g.weights.pick(g.rng, &["matview:rec:count", "matview:rec:pair"]);
    g.fire(shape);
    let (create, read) = if shape == "matview:rec:pair" {
        (
            format!(
                "CREATE RECURSIVE VIEW {} (n, s) AS VALUES (1, 1) UNION ALL SELECT n + 1, \
                 s + n + 1 FROM {} WHERE n < {};",
                v, v, bound
            ),
            format!("SELECT * FROM {} ORDER BY n;", v),
        )
    } else {
        (
            format!(
                "CREATE RECURSIVE VIEW {} (n) AS VALUES (1) UNION ALL SELECT n + 1 FROM {} \
                 WHERE n < {};",
                v, v, bound
            ),
            format!("SELECT * FROM {} ORDER BY n;", v),
        )
    };
    vec![
        StmtKind::Raw(create),
        StmtKind::Raw(read),
        StmtKind::Raw(format!("SELECT pg_get_viewdef('{}'::regclass, true);", v)),
        StmtKind::Raw(format!("DROP VIEW {};", v)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::weights::WeightTable;

    /// Every arm fires and every emitted statement is a well-formed single
    /// line (matches the global module invariants), and each group both
    /// creates and drops its objects (group-local discipline).
    #[test]
    fn arms_fire_and_are_well_formed() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let wanted = [
            "matview:content",
            "matview:scan",
            "matview:merge",
            "matview:nouniq",
            "matview:ctas",
            "matview:secview",
            "matview:replace",
            "matview:recursive",
        ];
        let mut seen: Vec<String> = Vec::new();
        let mut rng = Rng::new(0xF4A);
        for _ in 0..2000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_matview_module(&mut g);
            assert!(!stmts.is_empty(), "empty group");
            let mut created = false;
            let mut dropped = false;
            for s in &stmts {
                let sql = s.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                if sql.starts_with("CREATE ") || sql.starts_with("SELECT ") && sql.contains(" INTO ")
                {
                    created = true;
                }
                if sql.starts_with("DROP ") {
                    dropped = true;
                }
            }
            assert!(created && dropped, "group not self-contained: {stmts:?}");
            seen.extend(prods);
        }
        for want in wanted {
            assert!(seen.iter().any(|p| p == want), "arm {want} never fired");
        }
    }
}
