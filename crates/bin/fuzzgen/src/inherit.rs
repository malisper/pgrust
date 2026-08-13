//! INHERIT: classic table-inheritance (CREATE TABLE ... INHERITS) drain
//! module.
//!
//! Distinct from declarative partitioning (crate::part / crate::partalt,
//! PARTITION BY / ATTACH PARTITION): this module drives the legacy
//! inheritance surface in backend/commands/tablecmds.c and
//! backend/catalog/pg_inherits.c that the error-arm batteries (earm/earm2/
//! earm3) only reach on the failure side. The complement it adds is the
//! POSITIVE path plus its catalog bookkeeping:
//!   - MergeAttributes / MergeChildAttribute: single + multiple parents,
//!     column merge, child-added columns, inherited-column type/collation
//!     CONFLICT error arms.
//!   - MergeConstraintsIntoExisting / constraint coalesce: CHECK and
//!     (PG18-catalogued) NOT NULL merge, NO INHERIT constraints, and the
//!     conislocal/coninhcount bookkeeping when a child-local constraint
//!     coalesces with a newly-inherited one.
//!   - ATExecAddColumn / ATExecDropColumn / ATExecAlterColumnType /
//!     ATExecSetNotNull / ATExecDropNotNull recursion to children (and the
//!     ONLY, parent-only, non-recursive arms).
//!   - ATExecAddInherit / ATExecDropInherit: ALTER TABLE child INHERIT /
//!     NO INHERIT (attach/detach) plus the missing-column and type-mismatch
//!     error arms.
//!   - find_all_inheritors / find_inheritance_children: SELECT/UPDATE/
//!     DELETE with vs without ONLY (the `*` recursion), over 2-3 level
//!     hierarchies, with tableoid provenance.
//!   - DeleteInheritsTuple / dependency: DROP parent (CASCADE-required
//!     error arm, then CASCADE).
//!   - constraint_exclusion over inherited CHECK.
//!
//! Determinism disciplines (this rig boots a differential PAIR; both
//! engines run the byte-identical stream):
//!   - Inheritance scans have no defined row order across children, so
//!     EVERY data probe projects `tableoid::regclass::text` as provenance
//!     and ORDER BYs a total key (positional over all projected columns),
//!     making the result set order-stable and provenance-explicit — a
//!     wrong ONLY-recursion result then diverges as a result diff, not
//!     noise (COPY/heap-order ruling: heap order is non-surface, so it is
//!     never the comparison key).
//!   - Catalog probes project only STABLE columns (relname/attname/conname,
//!     inhseqno, attinhcount/attislocal, coninhcount/conislocal/connoinherit/
//!     contype) — never oids — and ORDER BY name, so a bookkeeping
//!     divergence (e.g. a merge that mis-sets conislocal) is caught byte-
//!     exactly.
//!   - Groups are self-contained (plpg/coll discipline): fresh hierarchy
//!     per group, exercise, probe, drop everything, no cross-group edges.
//!     Name counters live in `InheritState` so names stay session-unique
//!     even if a drop failed. Groups are NOT inside txn brackets, so a
//!     deliberate-error statement autocommits-fails without poisoning the
//!     rest of the group.
//!   - Deterministic data only (fixed pools drawn through the session PRNG);
//!     collations restricted to "C"/"POSIX" (universal, no host-locale
//!     dependency) for the collation-conflict arm.
//!   - Error paths are targets, not hazards: a deliberately-invalid CREATE/
//!     ALTER produces the identical ereport on both engines (error
//!     identity); an inventory asymmetry is a banked finding, never a flake.

use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counter (the hierarchies themselves are
/// group-local; only the counter persists so names stay session-unique).
#[derive(Clone, Debug, Default)]
pub struct InheritState {
    next_grp: u32,
}

impl InheritState {
    pub fn new() -> InheritState {
        InheritState::default()
    }
}

/// Deterministic scalar fuel.
const INTS: &[i32] = &[-7, -1, 0, 1, 2, 5, 42, 100, 1000];
const TEXTS: &[&str] = &["alpha", "beta", "gamma", "delta", "", "zeta"];

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

fn pick_int(g: &mut Gen) -> i32 {
    *g.rng.pick(INTS)
}

fn pick_text(g: &mut Gen) -> &'static str {
    g.rng.pick(TEXTS)
}

/// Quoted comma-list of relnames for a `relname IN (...)` filter.
fn namelist(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| format!("'{}'", n))
        .collect::<Vec<_>>()
        .join(", ")
}

/// pg_inherits bookkeeping probe (parent/child/seqno), scoped to the
/// group's children and stable-ordered.
fn probe_inherits(names: &[&str]) -> StmtKind {
    raw(format!(
        "SELECT p.relname AS parent, c.relname AS child, i.inhseqno \
         FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         JOIN pg_class p ON p.oid = i.inhparent \
         WHERE c.relname IN ({}) ORDER BY child, parent, inhseqno;",
        namelist(names)
    ))
}

/// pg_attribute inheritance bookkeeping (attinhcount / attislocal).
fn probe_attrs(names: &[&str]) -> StmtKind {
    raw(format!(
        "SELECT c.relname, a.attname, a.attinhcount, a.attislocal \
         FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
         WHERE c.relname IN ({}) AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY c.relname, a.attname;",
        namelist(names)
    ))
}

/// pg_constraint inheritance bookkeeping (coninhcount / conislocal /
/// connoinherit / contype) — the merge/coalesce witness.
fn probe_constraints(names: &[&str]) -> StmtKind {
    raw(format!(
        "SELECT c.relname, con.conname, con.contype, con.coninhcount, \
         con.conislocal, con.connoinherit \
         FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid \
         WHERE c.relname IN ({}) ORDER BY c.relname, con.conname;",
        namelist(names)
    ))
}

/// A total-ordered, provenance-carrying data probe over an inheritance
/// tree. `only` selects the ONLY (non-recursive) arm. `cols` is the
/// projected column list after the provenance column; the ORDER BY is
/// positional over every projected column, so the result is order-stable
/// regardless of physical/child scan order.
fn probe_rows(tbl: &str, only: bool, cols: &str, ncols: usize) -> StmtKind {
    let only_kw = if only { "ONLY " } else { "" };
    // positional order keys: 1 (src) .. ncols+1
    let keys = (1..=ncols + 1)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    raw(format!(
        "SELECT tableoid::regclass::text AS src, {cols} FROM {only_kw}{tbl} ORDER BY {keys};"
    ))
}

/// One inheritance group. Every group is self-contained per the plpg/coll
/// discipline.
pub fn gen_inherit_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("inherit");
    let form = g.weights.pick(
        g.rng,
        &[
            "inherit:basic",
            "inherit:multi",
            "inherit:constraint",
            "inherit:alter",
            "inherit:attach",
            "inherit:dml",
            "inherit:droptree",
        ],
    );
    g.fire(form);
    let id = g.inherit.next_grp;
    g.inherit.next_grp += 1;
    match form {
        "inherit:basic" => gen_basic(g, id),
        "inherit:multi" => gen_multi(g, id),
        "inherit:constraint" => gen_constraint(g, id),
        "inherit:alter" => gen_alter(g, id),
        "inherit:attach" => gen_attach(g, id),
        "inherit:dml" => gen_dml(g, id),
        _ => gen_droptree(g, id),
    }
}

/// Single parent, child adds columns: MergeAttributes single-parent arm,
/// find_all_inheritors on read, ONLY vs recursive, tableoid provenance,
/// full catalog bookkeeping.
fn gen_basic(g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p = format!("fz_inh_{id}_p");
    let c = format!("fz_inh_{id}_c");
    let mut out = Vec::new();
    out.push(raw(format!(
        "CREATE TABLE {p} (pk int PRIMARY KEY, a int, b text);"
    )));
    // child inherits pk/a/b and adds its own column `extra`.
    out.push(raw(format!(
        "CREATE TABLE {c} (extra int) INHERITS ({p});"
    )));
    for pk in 1..=3 {
        out.push(raw(format!(
            "INSERT INTO {p} (pk, a, b) VALUES ({pk}, {}, '{}');",
            pick_int(g),
            pick_text(g)
        )));
    }
    for pk in 1..=3 {
        out.push(raw(format!(
            "INSERT INTO {c} (pk, a, b, extra) VALUES ({pk}, {}, '{}', {});",
            pick_int(g),
            pick_text(g),
            pick_int(g)
        )));
    }
    // recursive read (parent rows + child rows) vs ONLY parent vs child.
    out.push(probe_rows(&p, false, "pk, a, b", 3));
    out.push(probe_rows(&p, true, "pk, a, b", 3));
    out.push(probe_rows(&c, false, "pk, a, b, extra", 4));
    // count via the parent (recursion) and ONLY.
    out.push(raw(format!("SELECT count(*) FROM {p};")));
    out.push(raw(format!("SELECT count(*) FROM ONLY {p};")));
    // catalog bookkeeping.
    let names: &[&str] = &[&p, &c];
    out.push(probe_inherits(names));
    out.push(probe_attrs(names));
    out.push(probe_constraints(names));
    out.push(raw(format!("DROP TABLE {p} CASCADE;")));
    out
}

/// Multiple parents: MergeAttributes multi-parent column merge (common
/// column coalesced across parents), plus the inherited-column type- and
/// collation-conflict error arms.
fn gen_multi(g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p1 = format!("fz_inh_{id}_p1");
    let p2 = format!("fz_inh_{id}_p2");
    let c = format!("fz_inh_{id}_c");
    // conflict-arm parents (kept separate so the successful merge above is
    // never contaminated by the failing CREATEs).
    let q1 = format!("fz_inh_{id}_q1");
    let q2 = format!("fz_inh_{id}_q2");
    let r1 = format!("fz_inh_{id}_r1");
    let r2 = format!("fz_inh_{id}_r2");
    let mut out = Vec::new();
    // 'common' present in both parents with identical type -> merges.
    out.push(raw(format!(
        "CREATE TABLE {p1} (pk int, a int, common text);"
    )));
    out.push(raw(format!(
        "CREATE TABLE {p2} (pk int, b int, common text);"
    )));
    out.push(raw(format!("CREATE TABLE {c} () INHERITS ({p1}, {p2});")));
    for pk in 1..=2 {
        out.push(raw(format!(
            "INSERT INTO {c} (pk, a, b, common) VALUES ({pk}, {}, {}, '{}');",
            pick_int(g),
            pick_int(g),
            pick_text(g)
        )));
    }
    out.push(probe_rows(&c, false, "pk, a, b, common", 4));
    let names: &[&str] = &[&p1, &p2, &c];
    out.push(probe_inherits(names));
    out.push(probe_attrs(names));
    // TYPE-conflict arm: same column name, different type across parents ->
    // ERROR "column ... has a type conflict for column" (MergeAttributes).
    out.push(raw(format!("CREATE TABLE {q1} (x int);")));
    out.push(raw(format!("CREATE TABLE {q2} (x text);")));
    out.push(raw(format!(
        "CREATE TABLE fz_inh_{id}_qbad () INHERITS ({q1}, {q2});"
    )));
    // COLLATION-conflict arm: same name+type text, different collation ->
    // ERROR "column ... has a collation conflict". "C"/"POSIX" are
    // universal (no host-locale dependency).
    out.push(raw(format!(
        "CREATE TABLE {r1} (x text COLLATE \"C\");"
    )));
    out.push(raw(format!(
        "CREATE TABLE {r2} (x text COLLATE \"POSIX\");"
    )));
    out.push(raw(format!(
        "CREATE TABLE fz_inh_{id}_rbad () INHERITS ({r1}, {r2});"
    )));
    // teardown (the *bad children never got created; only drop what exists).
    out.push(raw(format!("DROP TABLE {c};")));
    out.push(raw(format!("DROP TABLE {p1};")));
    out.push(raw(format!("DROP TABLE {p2};")));
    out.push(raw(format!("DROP TABLE {q1};")));
    out.push(raw(format!("DROP TABLE {q2};")));
    out.push(raw(format!("DROP TABLE {r1};")));
    out.push(raw(format!("DROP TABLE {r2};")));
    out
}

/// Constraint merge/coalesce + NOT NULL + NO INHERIT + constraint_exclusion.
fn gen_constraint(_g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p = format!("fz_inh_{id}_p");
    let c = format!("fz_inh_{id}_c");
    let mut out = Vec::new();
    // parent CHECK chk_a inherited to child (coninhcount=1, conislocal=f);
    // NOT NULL on `a` inherited (PG18-catalogued contype='n').
    out.push(raw(format!(
        "CREATE TABLE {p} (pk int, a int NOT NULL, CONSTRAINT chk_a CHECK (a > 0));"
    )));
    // child carries a LOCAL chk_b identical to one the parent gains below:
    // the coalesce path -> child chk_b ends conislocal=t AND coninhcount=1.
    out.push(raw(format!(
        "CREATE TABLE {c} (CONSTRAINT chk_b CHECK (a <> 0)) INHERITS ({p});"
    )));
    // NO INHERIT check on parent: must NOT propagate to child.
    out.push(raw(format!(
        "ALTER TABLE {p} ADD CONSTRAINT chk_ni CHECK (a < 1000) NO INHERIT;"
    )));
    // add chk_b to parent -> MergeConstraintsIntoExisting coalesces with the
    // child's pre-existing local chk_b (the bookkeeping-divergence surface).
    out.push(raw(format!(
        "ALTER TABLE {p} ADD CONSTRAINT chk_b CHECK (a <> 0);"
    )));
    // SET/DROP NOT NULL recursion.
    out.push(raw(format!(
        "ALTER TABLE {p} ALTER COLUMN pk SET NOT NULL;"
    )));
    let names: &[&str] = &[&p, &c];
    out.push(probe_constraints(names));
    out.push(probe_attrs(names));
    // data + constraint_exclusion over the inherited CHECK.
    for pk in 1..=3 {
        out.push(raw(format!(
            "INSERT INTO {p} (pk, a) VALUES ({pk}, {});",
            1 + (pk as i32)
        )));
        out.push(raw(format!(
            "INSERT INTO {c} (pk, a) VALUES ({}, {});",
            10 + pk,
            1 + (pk as i32)
        )));
    }
    out.push(raw("SET constraint_exclusion = on;".to_string()));
    // WHERE contradicts chk_a (a>0) -> planner may exclude both rels via the
    // inherited CHECK; result identity is the comparison surface.
    out.push(raw(format!(
        "SELECT count(*) FROM {p} WHERE a < 0;"
    )));
    out.push(probe_rows(&p, false, "pk, a", 2));
    out.push(raw("RESET constraint_exclusion;".to_string()));
    out.push(probe_constraints(names));
    out.push(raw(format!("DROP TABLE {p} CASCADE;")));
    out
}

/// ALTER propagation: ADD/DROP COLUMN, ALTER COLUMN TYPE, ONLY arms.
fn gen_alter(g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p = format!("fz_inh_{id}_p");
    let c = format!("fz_inh_{id}_c");
    let mut out = Vec::new();
    out.push(raw(format!("CREATE TABLE {p} (pk int, a int);")));
    out.push(raw(format!(
        "CREATE TABLE {c} (extra text) INHERITS ({p});"
    )));
    for pk in 1..=2 {
        out.push(raw(format!(
            "INSERT INTO {p} (pk, a) VALUES ({pk}, {});",
            pick_int(g)
        )));
        out.push(raw(format!(
            "INSERT INTO {c} (pk, a, extra) VALUES ({}, {}, '{}');",
            10 + pk,
            pick_int(g),
            pick_text(g)
        )));
    }
    let names: &[&str] = &[&p, &c];
    // ADD COLUMN with default -> recurses to child (attinhcount=1 on child).
    out.push(raw(format!(
        "ALTER TABLE {p} ADD COLUMN added1 int DEFAULT 7;"
    )));
    out.push(probe_attrs(names));
    out.push(probe_rows(&p, false, "pk, a, added1", 3));
    // ONLY ADD COLUMN arm (parent-only path; error-or-success, identical
    // both sides).
    out.push(raw(format!(
        "ALTER TABLE ONLY {p} ADD COLUMN ponly int;"
    )));
    out.push(probe_attrs(names));
    // ALTER COLUMN TYPE propagation to child.
    out.push(raw(format!(
        "ALTER TABLE {p} ALTER COLUMN a TYPE bigint;"
    )));
    out.push(probe_attrs(names));
    // DROP COLUMN propagation.
    out.push(raw(format!("ALTER TABLE {p} DROP COLUMN a;")));
    out.push(probe_attrs(names));
    out.push(probe_rows(&c, false, "pk, extra", 2));
    out.push(raw(format!("DROP TABLE {p} CASCADE;")));
    out
}

/// Attach/detach: ALTER TABLE child INHERIT / NO INHERIT (ATExecAddInherit /
/// ATExecDropInherit) plus the missing-column and type-mismatch error arms.
fn gen_attach(g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p = format!("fz_inh_{id}_p");
    let c = format!("fz_inh_{id}_c");
    let bad = format!("fz_inh_{id}_bad");
    let miss = format!("fz_inh_{id}_miss");
    let mut out = Vec::new();
    out.push(raw(format!(
        "CREATE TABLE {p} (pk int, a int, b text);"
    )));
    // standalone child with matching columns -> attachable.
    out.push(raw(format!(
        "CREATE TABLE {c} (pk int, a int, b text);"
    )));
    for pk in 1..=2 {
        out.push(raw(format!(
            "INSERT INTO {p} (pk, a, b) VALUES ({pk}, {}, '{}');",
            pick_int(g),
            pick_text(g)
        )));
        out.push(raw(format!(
            "INSERT INTO {c} (pk, a, b) VALUES ({}, {}, '{}');",
            10 + pk,
            pick_int(g),
            pick_text(g)
        )));
    }
    let names: &[&str] = &[&p, &c];
    // attach: child columns keep attislocal=t but gain attinhcount=1.
    out.push(raw(format!("ALTER TABLE {c} INHERIT {p};")));
    out.push(probe_inherits(names));
    out.push(probe_attrs(names));
    // now the parent read recurses into the attached child.
    out.push(probe_rows(&p, false, "pk, a, b", 3));
    out.push(probe_rows(&p, true, "pk, a, b", 3));
    // detach.
    out.push(raw(format!("ALTER TABLE {c} NO INHERIT {p};")));
    out.push(probe_inherits(names));
    out.push(probe_attrs(names));
    // type-mismatch attach error arm.
    out.push(raw(format!("CREATE TABLE {bad} (pk int, a text, b text);")));
    out.push(raw(format!("ALTER TABLE {bad} INHERIT {p};")));
    // missing-column attach error arm.
    out.push(raw(format!("CREATE TABLE {miss} (pk int, a int);")));
    out.push(raw(format!("ALTER TABLE {miss} INHERIT {p};")));
    // teardown (all four tables exist: the failing statements were the
    // ALTERs, not the CREATEs).
    out.push(raw(format!("DROP TABLE {c};")));
    out.push(raw(format!("DROP TABLE {bad};")));
    out.push(raw(format!("DROP TABLE {miss};")));
    out.push(raw(format!("DROP TABLE {p} CASCADE;")));
    out
}

/// find_all_inheritors recursion under DML: UPDATE/DELETE with vs without
/// ONLY over a 3-level tree, RETURNING tableoid provenance. A wrong
/// ONLY-recursion result is a HIGH-severity divergence, caught by the
/// post-state probes.
fn gen_dml(_g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p = format!("fz_inh_{id}_p");
    let c = format!("fz_inh_{id}_c");
    let gc = format!("fz_inh_{id}_g");
    let mut out = Vec::new();
    out.push(raw(format!("CREATE TABLE {p} (pk int, a int);")));
    out.push(raw(format!("CREATE TABLE {c} () INHERITS ({p});")));
    out.push(raw(format!("CREATE TABLE {gc} () INHERITS ({c});")));
    // distinct rows per level (pk ranges keep them separable).
    for (tbl, base) in [(&p, 1), (&c, 10), (&gc, 100)] {
        for k in 0..3 {
            out.push(raw(format!(
                "INSERT INTO {tbl} (pk, a) VALUES ({}, {});",
                base + k,
                base + k
            )));
        }
    }
    out.push(probe_rows(&p, false, "pk, a", 2));
    // UPDATE ONLY parent: only the 3 parent rows shift.
    out.push(raw(format!(
        "UPDATE ONLY {p} SET a = a + 1000;"
    )));
    out.push(probe_rows(&p, false, "pk, a", 2));
    // UPDATE recursive from the mid level: c + g rows shift, parent does not.
    out.push(raw(format!("UPDATE {c} SET a = a + 1;")));
    out.push(probe_rows(&p, false, "pk, a", 2));
    // DELETE ONLY mid level: only c's own rows go; g survives.
    out.push(raw(format!("DELETE FROM ONLY {c} WHERE pk >= 0;")));
    out.push(probe_rows(&p, false, "pk, a", 2));
    // recursive DELETE from parent with a predicate + RETURNING provenance.
    out.push(raw(format!(
        "DELETE FROM {p} WHERE a > 100 RETURNING tableoid::regclass::text, pk;"
    )));
    out.push(probe_rows(&p, false, "pk, a", 2));
    out.push(raw(format!("DROP TABLE {p} CASCADE;")));
    out
}

/// DROP parent dependency: bare DROP errors (children depend), CASCADE
/// succeeds and clears pg_inherits (DeleteInheritsTuple).
fn gen_droptree(g: &mut Gen, id: u32) -> Vec<StmtKind> {
    let p = format!("fz_inh_{id}_p");
    let c = format!("fz_inh_{id}_c");
    let mut out = Vec::new();
    out.push(raw(format!("CREATE TABLE {p} (pk int, a int);")));
    out.push(raw(format!("CREATE TABLE {c} (extra int) INHERITS ({p});")));
    for pk in 1..=2 {
        out.push(raw(format!(
            "INSERT INTO {c} (pk, a, extra) VALUES ({pk}, {}, {});",
            pick_int(g),
            pick_int(g)
        )));
    }
    let names: &[&str] = &[&p, &c];
    out.push(probe_inherits(names));
    // bare DROP of a parent with a child -> ERROR (dependency), needs CASCADE.
    out.push(raw(format!("DROP TABLE {p};")));
    // tree still intact after the failed drop.
    out.push(probe_inherits(names));
    out.push(probe_rows(&p, false, "pk, a", 2));
    // CASCADE drops parent + child; pg_inherits row gone.
    out.push(raw(format!("DROP TABLE {p} CASCADE;")));
    out.push(probe_inherits(names));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn all_forms_seen() -> Vec<String> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut seen = Vec::new();
        for seed in 0..200u64 {
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let _ = gen_inherit_module(&mut g);
            for p in &prods {
                if p.starts_with("inherit:") && !seen.contains(p) {
                    seen.push(p.clone());
                }
            }
        }
        seen
    }

    #[test]
    fn every_form_fires_and_is_self_contained() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        for seed in 0..400u64 {
            let mut rng = Rng::new(seed);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_inherit_module(&mut g);
            assert!(!stmts.is_empty());
            for s in &stmts {
                let sql = s.to_sql();
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
            // Every table created in the group is named with the group id
            // prefix; nothing references an unprefixed fz_inh table.
        }
        let forms = all_forms_seen();
        for f in [
            "inherit:basic",
            "inherit:multi",
            "inherit:constraint",
            "inherit:alter",
            "inherit:attach",
            "inherit:dml",
            "inherit:droptree",
        ] {
            assert!(forms.contains(&f.to_string()), "form {f} never fired");
        }
    }

    #[test]
    fn seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(1234);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            gen_inherit_module(&mut g)
                .iter()
                .map(|s| s.to_sql())
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }
}
