//! MERGE / ModifyTable / rewrite residue drain (Track-B, lane MERGE): the
//! still-uncovered rewriteHandler.c + nodeModifyTable.c + ruleutils.c MERGE
//! arms that the standing `dml`/`merge`/`views` modules cannot reach because
//! the base fixture carries no generated columns, no real column DEFAULTs,
//! no array/composite subfield targets, and no SQL-body function bodies.
//! Every group here builds its own purpose-shaped objects, exercises exactly
//! one residual arm family, probes pk-ordered, and DROPs — the `opt2`/`nodes`
//! self-contained-battery discipline, no session state.
//!
//! Families and the C arms they target (c_postgres REL_18_3@62d6c7d,
//! gap-report-006):
//!   - `mergex:gencol` — STORED generated columns written on INSERT, UPDATE
//!     and both MERGE arms: `ExecComputeStoredGenerated` (nodeModifyTable.c),
//!     `expand_generated_columns_internal` / `build_generation_expression`
//!     (rewriteHandler.c). The generated columns are never SET directly (a
//!     428C9 error) — they are recomputed from the base columns the writes
//!     touch, so both engines materialize identical stored values.
//!   - `mergex:defcol` — multi-row `VALUES` with `DEFAULT` cells over a
//!     table carrying real column defaults: a column DEFAULT in *every* row
//!     drives `findDefaultOnlyColumns`, a no-default column DEFAULT in *some*
//!     rows drives `rewriteValuesRTEToNulls` (both rewriteHandler.c), plus an
//!     `UPDATE ... SET c = DEFAULT` for the update-side default arm.
//!   - `mergex:subfield` — `UPDATE ... SET arr[i] = ..., arr[j] = ...`, two
//!     subscript assignments to one column merged by `get_assignment_input`
//!     (rewriteHandler.c), exercised on a base table and again through an
//!     auto-updatable view (the view-rewrite path onto the same arm).
//!   - `mergex:wcte` — data-modifying CTEs (`WITH i AS (INSERT..RETURNING),
//!     u AS (UPDATE..RETURNING), d AS (DELETE..RETURNING) SELECT ...`): three
//!     ModifyTable subplans + executor RETURNING projection in one command,
//!     all reading the one pre-command snapshot so the result is a pure
//!     function of the fixed literals (compared as a totally ordered rowset).
//!   - `mergex:mergedef` — a `BEGIN ATOMIC` SQL-body function whose body is a
//!     MERGE, deparsed by `pg_get_functiondef`: the sole user-facing route to
//!     `get_merge_query_def` (ruleutils.c, the single largest MERGE-track gap
//!     at 86 lines). The deparse text is a byte-exact differential surface.
//!
//! Determinism discipline (crate::spill/heap house rules):
//!   - no float anywhere; every row-returning probe carries a TOTAL ORDER BY
//!     ending in the pk (or a unique tag,pk), so residual ties are between
//!     identical rows;
//!   - all data are fixed literals (a few small rng-chosen constants add
//!     spelling variety but the stream stays a pure function of the seed and
//!     is emitted once and applied identically to both engines);
//!   - fixed `fz_mx_*` object names, one family per group, each group opens
//!     with DROP ... IF EXISTS and closes with the plain DROP, so a group is
//!     self-healing and leaves no catalog residue for other modules to hit.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// The five residual-arm families; parallel to the `mergex:*` productions in
/// weights::PROD_WEIGHTS.
const FAMILIES: &[&str] = &[
    "mergex:gencol",
    "mergex:defcol",
    "mergex:subfield",
    "mergex:wcte",
    "mergex:mergedef",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_mergex_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("mergex");
    let family = g.weights.pick(g.rng, FAMILIES);
    g.fire(family);
    match family {
        "mergex:gencol" => gen_gencol(g),
        "mergex:defcol" => gen_defcol(g),
        "mergex:subfield" => gen_subfield(g),
        "mergex:wcte" => gen_wcte(g),
        _ => gen_mergedef(g),
    }
}

/// STORED generated columns recomputed on INSERT / UPDATE / MERGE.
fn gen_gencol(g: &mut Gen) -> Vec<StmtKind> {
    // Small spelling variety in the base data; behaviour is fixed.
    let a1 = 10 + g.rng.below(5) as i64;
    let a2 = 20 + g.rng.below(5) as i64;
    let a3 = 30 + g.rng.below(5) as i64;
    let bump = 1 + g.rng.below(3) as i64;
    let mut v = vec![
        raw("DROP TABLE IF EXISTS fz_mx_g;"),
        raw(
            "CREATE TABLE fz_mx_g (pk int4 PRIMARY KEY, a int4 NOT NULL, b int4, \
             gnum int4 GENERATED ALWAYS AS (a * 2 + COALESCE(b, 0)) STORED, \
             gtxt text GENERATED ALWAYS AS ('r' || a::text) STORED);",
        ),
        raw(format!(
            "INSERT INTO fz_mx_g (pk, a, b) VALUES (1, {a1}, 5), (2, {a2}, NULL), (3, {a3}, 7);"
        )),
        // UPDATE of a base column recomputes the dependents (build_generation_
        // expression / expand_generated_columns_internal on the update path).
        raw(format!("UPDATE fz_mx_g SET a = a + {bump} WHERE pk = 2;")),
        raw("UPDATE fz_mx_g SET b = 100 WHERE pk <= 2;"),
        // Both MERGE arms write base columns; the generated columns follow.
        raw(
            "MERGE INTO fz_mx_g t USING (VALUES (2, 55), (9, 90)) AS s(sk, sa) \
             ON t.pk = s.sk \
             WHEN MATCHED THEN UPDATE SET a = s.sa \
             WHEN NOT MATCHED THEN INSERT (pk, a, b) VALUES (s.sk, s.sa, 1);",
        ),
    ];
    // Occasionally a MERGE ... RETURNING that projects the generated columns
    // (the RETURNING computed after the stored generation).
    if g.rng.chance(1, 2) {
        g.fire("mergex:gencol:returning");
        v.push(raw(
            "MERGE INTO fz_mx_g t USING (VALUES (3, 7)) AS s(sk, sa) ON t.pk = s.sk \
             WHEN MATCHED THEN UPDATE SET a = t.a + s.sa \
             RETURNING t.pk, t.gnum, t.gtxt;",
        ));
    }
    v.push(raw("SELECT pk, a, b, gnum, gtxt FROM fz_mx_g ORDER BY pk;"));
    v.push(raw("DROP TABLE fz_mx_g;"));
    v
}

/// Column DEFAULTs in multi-row VALUES: findDefaultOnlyColumns +
/// rewriteValuesRTEToNulls, and the UPDATE-side SET x = DEFAULT arm.
fn gen_defcol(g: &mut Gen) -> Vec<StmtKind> {
    let da = 40 + g.rng.below(6) as i64;
    let db = 5 + g.rng.below(4) as i64;
    let mut v = vec![
        raw("DROP TABLE IF EXISTS fz_mx_d;"),
        raw(format!(
            "CREATE TABLE fz_mx_d (pk int4 PRIMARY KEY, a int4 DEFAULT {da}, \
             b int4 DEFAULT {db}, c int4);"
        )),
        // a is DEFAULT in every row -> findDefaultOnlyColumns replaces the
        // whole column; c has no default and is DEFAULT in some rows ->
        // rewriteValuesRTEToNulls turns those cells NULL; b mixes literal and
        // DEFAULT cells (the ordinary per-row default substitution).
        raw(
            "INSERT INTO fz_mx_d (pk, a, b, c) VALUES \
             (1, DEFAULT, 5, DEFAULT), (2, DEFAULT, DEFAULT, 9), \
             (3, DEFAULT, DEFAULT, DEFAULT);",
        ),
        raw("INSERT INTO fz_mx_d (pk, a, b, c) VALUES (4, 1, 2, 3);"),
        // UPDATE-side DEFAULT: reset to the column default expressions.
        raw("UPDATE fz_mx_d SET a = DEFAULT, c = DEFAULT WHERE pk = 4;"),
    ];
    // A single-row DEFAULT-only INSERT (all non-pk columns defaulted) also
    // routes through the default machinery.
    if g.rng.chance(1, 2) {
        g.fire("mergex:defcol:allrow");
        v.push(raw("INSERT INTO fz_mx_d (pk, a, b, c) VALUES (5, DEFAULT, DEFAULT, DEFAULT);"));
    }
    v.push(raw("SELECT pk, a, b, c FROM fz_mx_d ORDER BY pk;"));
    v.push(raw("DROP TABLE fz_mx_d;"));
    v
}

/// Subscript assignment merged by get_assignment_input, on a base table and
/// through an auto-updatable view.
fn gen_subfield(g: &mut Gen) -> Vec<StmtKind> {
    let e1 = 90 + g.rng.below(9) as i64;
    let e2 = 70 + g.rng.below(9) as i64;
    vec![
        raw("DROP VIEW IF EXISTS fz_mx_sv;"),
        raw("DROP TABLE IF EXISTS fz_mx_s;"),
        raw("CREATE TABLE fz_mx_s (pk int4 PRIMARY KEY, arr int4[], k int4);"),
        raw(
            "INSERT INTO fz_mx_s VALUES (1, ARRAY[10, 20, 30], 1), \
             (2, ARRAY[40, 50, 60], 2), (3, ARRAY[70, 80, 90], 3);",
        ),
        // Two subscript assignments to one column in one SET list: the merge
        // that get_assignment_input performs (base-table path).
        raw(format!("UPDATE fz_mx_s SET arr[1] = {e1}, arr[3] = {e2} WHERE pk = 1;")),
        // A subscript target reading the same column + another column.
        raw("UPDATE fz_mx_s SET arr[2] = arr[1] + k WHERE pk = 2;"),
        // Same arm reached through the auto-updatable view rewrite.
        raw("CREATE VIEW fz_mx_sv AS SELECT pk, arr, k FROM fz_mx_s;"),
        raw("UPDATE fz_mx_sv SET arr[1] = arr[2] + 1, arr[2] = 0 WHERE pk = 3;"),
        raw("SELECT pk, arr::text, k FROM fz_mx_s ORDER BY pk;"),
        raw("DROP VIEW fz_mx_sv;"),
        raw("DROP TABLE fz_mx_s;"),
    ]
}

/// Data-modifying CTEs: INSERT/UPDATE/DELETE RETURNING funnelled through one
/// SELECT, all against one pre-command snapshot.
fn gen_wcte(g: &mut Gen) -> Vec<StmtKind> {
    let bump = 1 + g.rng.below(3) as i64;
    vec![
        raw("DROP TABLE IF EXISTS fz_mx_w;"),
        raw("CREATE TABLE fz_mx_w (pk int4 PRIMARY KEY, v int4);"),
        raw("INSERT INTO fz_mx_w VALUES (1, 10), (2, 20), (3, 30);"),
        raw(format!(
            "WITH ins AS (INSERT INTO fz_mx_w VALUES (4, 40), (5, 50) RETURNING pk, v), \
             upd AS (UPDATE fz_mx_w SET v = v + {bump} WHERE pk <= 2 RETURNING pk, v), \
             del AS (DELETE FROM fz_mx_w WHERE pk = 3 RETURNING pk, v) \
             SELECT tag, pk, v FROM ( \
             SELECT 'i'::text AS tag, pk, v FROM ins \
             UNION ALL SELECT 'u', pk, v FROM upd \
             UNION ALL SELECT 'd', pk, v FROM del) q ORDER BY tag, pk;"
        )),
        raw("SELECT pk, v FROM fz_mx_w ORDER BY pk;"),
        raw("DROP TABLE fz_mx_w;"),
    ]
}

/// MERGE deparse via a BEGIN ATOMIC SQL-body function: get_merge_query_def.
fn gen_mergedef(g: &mut Gen) -> Vec<StmtKind> {
    // Vary which optional WHEN arms appear so the deparse covers the arm
    // branches of get_merge_query_def, not just one shape.
    let with_nmbs = g.rng.chance(1, 2);
    let with_cond = g.rng.chance(1, 2);
    let matched_cond = if with_cond { " AND t.v > 0" } else { "" };
    let nmbs_arm = if with_nmbs {
        " WHEN NOT MATCHED BY SOURCE THEN DELETE"
    } else {
        ""
    };
    if with_nmbs {
        g.fire("mergex:mergedef:nmbs");
    }
    let body = format!(
        "MERGE INTO fz_mx_m t USING (VALUES (1, 100), (2, 200)) AS s(k, val) ON t.pk = s.k \
         WHEN MATCHED{matched_cond} THEN UPDATE SET v = s.val \
         WHEN NOT MATCHED THEN INSERT (pk, v) VALUES (s.k, s.val){nmbs_arm};"
    );
    vec![
        raw("DROP TABLE IF EXISTS fz_mx_m;"),
        raw("CREATE TABLE fz_mx_m (pk int4 PRIMARY KEY, v int4);"),
        raw("INSERT INTO fz_mx_m VALUES (1, 1), (3, 3);"),
        raw(format!(
            "CREATE FUNCTION fz_mx_mf() RETURNS void LANGUAGE sql BEGIN ATOMIC {body} END;"
        )),
        // The deparse — the byte-exact ruleutils MERGE differential surface.
        raw("SELECT pg_get_functiondef('fz_mx_mf'::regproc);"),
        // Exercise the body too, then probe the mutated table.
        raw("SELECT fz_mx_mf();"),
        raw("SELECT pk, v FROM fz_mx_m ORDER BY pk;"),
        raw("DROP FUNCTION fz_mx_mf();"),
        raw("DROP TABLE fz_mx_m;"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            let stmts: Vec<String> = gen_mergex_module(&mut g).iter().map(|s| s.to_sql()).collect();
            groups.push(stmts);
            prods.extend(p);
        }
        (groups, prods)
    }

    #[test]
    fn mergex_is_deterministic() {
        let (a, _) = gen_many(21, 60, "");
        let (b, _) = gen_many(21, 60, "");
        assert_eq!(a, b);
        let (c, _) = gen_many(22, 60, "");
        assert_ne!(a, c);
    }

    #[test]
    fn every_family_fires_and_is_well_formed() {
        let (groups, prods) = gen_many(0x11EE, 1200, "");
        for family in FAMILIES {
            assert!(prods.iter().any(|p| p == family), "family {family} never fired");
        }
        // Deparse and generated-column surfaces must appear.
        let all: String = groups.iter().flatten().cloned().collect::<Vec<_>>().join("\n");
        for frag in [
            "GENERATED ALWAYS AS",
            "MERGE INTO fz_mx_g",
            "VALUES (1, DEFAULT, 5, DEFAULT)",
            "SET arr[1] =",
            "WITH ins AS (INSERT INTO fz_mx_w",
            "pg_get_functiondef('fz_mx_mf'::regproc)",
            "BEGIN ATOMIC MERGE INTO fz_mx_m",
        ] {
            assert!(all.contains(frag), "expected fragment {frag:?} never generated");
        }
        // Structural invariants on every emitted statement.
        for group in &groups {
            assert!(!group.is_empty(), "empty group");
            for sql in group {
                assert!(!sql.contains('\n'), "multi-line statement: {sql}");
                assert!(sql.ends_with(';'), "unterminated statement: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                // Generated columns are never assigned directly.
                assert!(!sql.contains("SET gnum"), "generated column SET: {sql}");
                assert!(!sql.contains("SET gtxt"), "generated column SET: {sql}");
            }
            // Every group creates and drops its own object (self-contained).
            let joined = group.join(" ");
            assert!(joined.contains("CREATE TABLE fz_mx_"), "group creates no table: {joined}");
            assert!(joined.contains("DROP TABLE fz_mx_"), "group drops no table: {joined}");
        }
    }

    #[test]
    fn groups_are_self_healing() {
        // Each group opens with a DROP ... IF EXISTS for the table it builds,
        // so a re-run never collides on the fixed name.
        let (groups, _) = gen_many(7, 300, "");
        for group in &groups {
            let opens_with_drop_if_exists = group
                .iter()
                .take(2)
                .any(|s| s.contains("DROP") && s.contains("IF EXISTS"));
            assert!(opens_with_drop_if_exists, "group without leading DROP IF EXISTS: {group:?}");
        }
    }
}
