//! RITRIG: referential-integrity BEHAVIOR differential module — FK actions,
//! the RI trigger machinery, and constraint validation.
//!
//! Track-B SQL-drainable surface: ri_triggers.c (the LARGE untouched RI
//! surface — user-trigger depth is the plpg module's job, this is the
//! system RI-trigger machinery) plus the constraint-validation arms in
//! tablecmds.c/heap.c. Every group builds its own parent+child fixture with
//! data, runs a cascading DELETE/UPDATE (or a deliberate violation), and
//! then PROBES the child + parent final state pk-ordered — the state probe
//! is the load-bearing assertion: a cascade that leaves a different child
//! image than C is a HIGH-severity finding, not a mere error-text skew.
//!
//! Surface covered (all ON DELETE / ON UPDATE actions):
//!   - NO ACTION / RESTRICT (23503, checked at end-of-statement vs
//!     immediately — ri_restrict), CASCADE (ri_Cascade_Del / _Upd),
//!     SET NULL and SET DEFAULT incl. the PG15+ column-list forms
//!     (ri_set / ri_setnull / ri_setdefault);
//!   - the check path: ri_Check_Pk_Match / RI_FKey_check on child
//!     INSERT/UPDATE (23503, the "insert or update on table ... violates
//!     foreign key constraint" identity + the "Key (...)=(...) is still
//!     referenced" / "is not present in table" detail);
//!   - MATCH SIMPLE vs MATCH FULL partial-null semantics (a partially-null
//!     composite key passes SIMPLE but is rejected FULL);
//!   - composite FKs, self-referential FKs, FKs to/from partitioned tables;
//!   - DEFERRABLE INITIALLY DEFERRED (check at COMMIT) vs SET CONSTRAINTS
//!     ALL IMMEDIATE;
//!   - ADD CONSTRAINT ... NOT VALID then VALIDATE CONSTRAINT
//!     (validateForeignKeyConstraint scan, pass and fail arms);
//!   - the non-FK constraint identities that share the enforcement paths:
//!     CHECK + domain CHECK (23514), EXCLUDE (23P01), UNIQUE/PK (23505).
//!
//! Determinism disciplines (mirrors crate::coll):
//!   - fresh, uniquely-named tables per group (name counter in `RiState`);
//!     every created object is dropped in-group, no cross-group edges.
//!   - all data is fixed literal fuel — the same seed reproduces the same
//!     rows — and every state probe carries a TOTAL `ORDER BY <pk>`, so the
//!     compared image is order-stable and any residual tie is between
//!     identical rows.
//!   - violation statements are self-contained: outside a txn bracket each
//!     is its own implicit transaction, so an error never aborts the
//!     following probe/drop. Error paths are targets, never hazards
//!     (both engines run the same SQL against the same prior state, so a
//!     correct pgrust matches C's error identity AND its rollback image).
//!   - deferred groups that must span statements emit an explicit
//!     BEGIN..COMMIT/ROLLBACK bracket structured so only COMMIT (deferred)
//!     or the SET CONSTRAINTS ALL IMMEDIATE statement can fail — no
//!     mid-bracket error ever leaves an aborted-transaction cascade.

use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counter (the objects themselves are group-local
/// and dropped in-group; only the counter persists so names stay
/// session-unique even when a drop was skipped on an error arm).
#[derive(Clone, Debug, Default)]
pub struct RiState {
    next: u32,
}

impl RiState {
    pub fn new() -> RiState {
        RiState::default()
    }
}

fn fresh(g: &mut Gen) -> u32 {
    let n = g.ritrig.next;
    g.ritrig.next += 1;
    n
}

/// One RI group. Every group is self-contained (create, populate, mutate,
/// probe, drop) per the plpg/coll discipline.
pub fn gen_ritrig_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig");
    let form = g.weights.pick(
        g.rng,
        &[
            "ritrig:cascade",
            "ritrig:setnull",
            "ritrig:setdefault",
            "ritrig:restrict",
            "ritrig:matchfull",
            "ritrig:composite",
            "ritrig:selfref",
            "ritrig:deferred",
            "ritrig:notvalid",
            "ritrig:part",
            "ritrig:check",
            "ritrig:exclude",
            "ritrig:unique",
        ],
    );
    g.fire(form);
    match form {
        "ritrig:cascade" => gen_cascade_group(g),
        "ritrig:setnull" => gen_setnull_group(g),
        "ritrig:setdefault" => gen_setdefault_group(g),
        "ritrig:restrict" => gen_restrict_group(g),
        "ritrig:matchfull" => gen_match_group(g),
        "ritrig:composite" => gen_composite_group(g),
        "ritrig:selfref" => gen_selfref_group(g),
        "ritrig:deferred" => gen_deferred_group(g),
        "ritrig:notvalid" => gen_notvalid_group(g),
        "ritrig:part" => gen_part_group(g),
        "ritrig:check" => gen_check_group(g),
        "ritrig:exclude" => gen_exclude_group(g),
        _ => gen_unique_group(g),
    }
}

/// The pk-total-ordered child + parent state probes shared by the
/// scalar-FK groups: the images the differ strict-compares.
fn probe_pair(p: &str, c: &str) -> [StmtKind; 2] {
    [
        StmtKind::Raw(format!("SELECT id, tag FROM {} ORDER BY id;", p)),
        StmtKind::Raw(format!("SELECT cid, pid, note FROM {} ORDER BY cid;", c)),
    ]
}

/// Drop the child then the parent (FK dependency order).
fn drop_pair(p: &str, c: &str) -> [StmtKind; 2] {
    [
        StmtKind::Raw(format!("DROP TABLE {};", c)),
        StmtKind::Raw(format!("DROP TABLE {};", p)),
    ]
}

/// Standard scalar parent (id PK) + child (cid PK, pid FK) with the given
/// ON DELETE / ON UPDATE action clause, populated with a fixed row set
/// (parents 1..3, children fanning in on 1 and 2, plus one orphan-free row).
fn scalar_fixture(g: &mut Gen, action: &str) -> (String, String, Vec<StmtKind>) {
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    let out = vec![
        StmtKind::Raw(format!("CREATE TABLE {} (id int4 PRIMARY KEY, tag text);", p)),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (cid int4 PRIMARY KEY, pid int4 REFERENCES {}(id) {}, note text);",
            c, p, action
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c');",
            p
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'z'), (13, 3, 'w');",
            c
        )),
    ];
    (p, c, out)
}

/// ON DELETE CASCADE / ON UPDATE CASCADE: a cascading update of a
/// referenced key rewrites the children's fk, a cascading delete removes
/// them. The child image after both is the surface (ri_Cascade_Del/_Upd).
fn gen_cascade_group(g: &mut Gen) -> Vec<StmtKind> {
    let (p, c, mut out) = scalar_fixture(g, "ON DELETE CASCADE ON UPDATE CASCADE");
    if g.rng.chance(1, 2) {
        g.fire("ritrig:cascade:upd");
        out.push(StmtKind::Raw(format!("UPDATE {} SET id = 100 WHERE id = 1;", p)));
    }
    g.fire("ritrig:cascade:del");
    out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 2;", p)));
    // A delete of an unreferenced key always succeeds and touches no child.
    out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 3;", p)));
    out.extend(probe_pair(&p, &c));
    out.extend(drop_pair(&p, &c));
    out
}

/// ON DELETE SET NULL / ON UPDATE SET NULL (plus the PG15+ column-list
/// form on a composite child): the referencing columns go NULL rather than
/// cascading (ri_setnull).
fn gen_setnull_group(g: &mut Gen) -> Vec<StmtKind> {
    // Half the time exercise the column-list SET NULL (col) form on a
    // two-column FK; otherwise the plain scalar SET NULL.
    if g.rng.chance(1, 2) {
        g.fire("ritrig:setnull:collist");
        let n = fresh(g);
        let p = format!("fz_rip_{}", n);
        let c = format!("fz_ric_{}", n);
        let mut out = vec![
            StmtKind::Raw(format!(
                "CREATE TABLE {} (a int4, b int4, tag text, PRIMARY KEY (a, b));",
                p
            )),
            StmtKind::Raw(format!(
                "CREATE TABLE {} (cid int4 PRIMARY KEY, fa int4, fb int4, note text, \
                 FOREIGN KEY (fa, fb) REFERENCES {}(a, b) ON DELETE SET NULL (fa));",
                c, p
            )),
            StmtKind::Raw(format!(
                "INSERT INTO {} VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 2, 'c');",
                p
            )),
            StmtKind::Raw(format!(
                "INSERT INTO {} VALUES (10, 1, 1, 'x'), (11, 1, 2, 'y'), (12, 2, 2, 'z');",
                c
            )),
            StmtKind::Raw(format!("DELETE FROM {} WHERE a = 1 AND b = 1;", p)),
            StmtKind::Raw(format!("SELECT a, b, tag FROM {} ORDER BY a, b;", p)),
            StmtKind::Raw(format!("SELECT cid, fa, fb, note FROM {} ORDER BY cid;", c)),
        ];
        out.extend(drop_pair(&p, &c));
        return out;
    }
    let action = if g.rng.chance(1, 2) {
        g.fire("ritrig:setnull:del");
        "ON DELETE SET NULL"
    } else {
        g.fire("ritrig:setnull:upd");
        "ON UPDATE SET NULL"
    };
    let (p, c, mut out) = scalar_fixture(g, action);
    if action.contains("DELETE") {
        out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)));
    } else {
        out.push(StmtKind::Raw(format!("UPDATE {} SET id = 100 WHERE id = 1;", p)));
    }
    out.extend(probe_pair(&p, &c));
    out.extend(drop_pair(&p, &c));
    out
}

/// ON DELETE SET DEFAULT: the child fk column has a DEFAULT that names an
/// existing parent key, so the RI trigger re-points rather than nulling
/// (ri_setdefault). The default target must itself remain a valid key.
fn gen_setdefault_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig:setdefault");
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!("CREATE TABLE {} (id int4 PRIMARY KEY, tag text);", p)),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (cid int4 PRIMARY KEY, pid int4 DEFAULT 3 REFERENCES {}(id) \
             ON DELETE SET DEFAULT ON UPDATE SET DEFAULT, note text);",
            c, p
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c');",
            p
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'z');",
            c
        )),
        // Deleting key 1 re-points its children to the DEFAULT key 3.
        StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)),
    ];
    out.extend(probe_pair(&p, &c));
    out.extend(drop_pair(&p, &c));
    out
}

/// ON DELETE RESTRICT vs NO ACTION: a delete/update of a referenced key
/// raises 23503 and leaves both tables unchanged (ri_restrict). RESTRICT
/// fires even inside a would-be-deferred window; NO ACTION is the
/// end-of-statement check. The probe proves the rollback image.
fn gen_restrict_group(g: &mut Gen) -> Vec<StmtKind> {
    let action = if g.rng.chance(1, 2) {
        g.fire("ritrig:restrict:restrict");
        "ON DELETE RESTRICT ON UPDATE RESTRICT"
    } else {
        g.fire("ritrig:restrict:noaction");
        "ON DELETE NO ACTION ON UPDATE NO ACTION"
    };
    let (p, c, mut out) = scalar_fixture(g, action);
    // Referenced-key delete + update both must fail (each its own implicit
    // txn, so the following probe/drop still run).
    out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)));
    out.push(StmtKind::Raw(format!("UPDATE {} SET id = 99 WHERE id = 2;", p)));
    // Unreferenced key 3 deletes cleanly.
    out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 3;", p)));
    out.extend(probe_pair(&p, &c));
    out.extend(drop_pair(&p, &c));
    out
}

/// MATCH SIMPLE vs MATCH FULL over a composite FK: a partially-null child
/// key passes SIMPLE (no check performed) but is rejected FULL (23503,
/// "MATCH FULL does not allow mixing of null and nonnull key values").
fn gen_match_group(g: &mut Gen) -> Vec<StmtKind> {
    let full = g.rng.chance(1, 2);
    let match_clause = if full {
        g.fire("ritrig:match:full");
        "MATCH FULL"
    } else {
        g.fire("ritrig:match:simple");
        "MATCH SIMPLE"
    };
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (a int4, b int4, tag text, PRIMARY KEY (a, b));",
            p
        )),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (cid int4 PRIMARY KEY, fa int4, fb int4, note text, \
             FOREIGN KEY (fa, fb) REFERENCES {}(a, b) {} ON DELETE CASCADE);",
            c, p, match_clause
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 1, 'a'), (2, 2, 'b');", p)),
        // Both-null row: allowed under BOTH match types (no check).
        StmtKind::Raw(format!("INSERT INTO {} VALUES (10, NULL, NULL, 'both-null');", c)),
        // Fully-present matching row: allowed under both.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (11, 1, 1, 'present');", c)),
        // Partial-null row: SIMPLE allows it, FULL rejects with 23503.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (12, 1, NULL, 'partial');", c)),
        // A cascading delete of key (1,1) removes only the fully-matching
        // child (11); the null-bearing rows survive under either match type.
        StmtKind::Raw(format!("DELETE FROM {} WHERE a = 1 AND b = 1;", p)),
        StmtKind::Raw(format!("SELECT a, b, tag FROM {} ORDER BY a, b;", p)),
        StmtKind::Raw(format!("SELECT cid, fa, fb, note FROM {} ORDER BY cid;", c)),
    ];
    out.extend(drop_pair(&p, &c));
    out
}

/// Composite (two-column) FK with a cascading update of the whole
/// referenced key — the multi-attribute ri_Cascade_Upd path.
fn gen_composite_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig:composite");
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (a int4, b int4, tag text, PRIMARY KEY (a, b));",
            p
        )),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (cid int4 PRIMARY KEY, fa int4, fb int4, note text, \
             FOREIGN KEY (fa, fb) REFERENCES {}(a, b) ON UPDATE CASCADE ON DELETE CASCADE);",
            c, p
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 2, 'c');",
            p
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (10, 1, 1, 'x'), (11, 1, 2, 'y'), (12, 2, 2, 'z');",
            c
        )),
        StmtKind::Raw(format!("UPDATE {} SET a = 5, b = 9 WHERE a = 1 AND b = 1;", p)),
        StmtKind::Raw(format!("DELETE FROM {} WHERE a = 2 AND b = 2;", p)),
        StmtKind::Raw(format!("SELECT a, b, tag FROM {} ORDER BY a, b;", p)),
        StmtKind::Raw(format!("SELECT cid, fa, fb, note FROM {} ORDER BY cid;", c)),
    ];
    out.extend(drop_pair(&p, &c));
    out
}

/// Self-referential FK with ON DELETE CASCADE: deleting a subtree root
/// cascades down the whole chain in one statement (the RI trigger fires
/// recursively over the same relation).
fn gen_selfref_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig:selfref");
    let n = fresh(g);
    let t = format!("fz_ris_{}", n);
    let cascade = g.rng.chance(1, 2);
    let action = if cascade {
        g.fire("ritrig:selfref:cascade");
        "ON DELETE CASCADE"
    } else {
        g.fire("ritrig:selfref:setnull");
        "ON DELETE SET NULL"
    };
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (id int4 PRIMARY KEY, parent int4 REFERENCES {}(id) {}, tag text);",
            t, t, action
        )),
        // A small tree: 1 root; 2,3 children of 1; 4 child of 2; 5 child of 3.
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, NULL, 'root'), (2, 1, 'b'), (3, 1, 'c'), (4, 2, 'd'), (5, 3, 'e');",
            t
        )),
        StmtKind::Raw(format!("DELETE FROM {} WHERE id = 2;", t)),
        StmtKind::Raw(format!("SELECT id, parent, tag FROM {} ORDER BY id;", t)),
        StmtKind::Raw(format!("DROP TABLE {};", t)),
    ];
    // Silence unused warning path parity: nothing else to add.
    let _ = &mut out;
    out
}

/// DEFERRABLE INITIALLY DEFERRED: the RI check is queued to COMMIT. Three
/// self-contained brackets — (a) a delete+refill that would fail IMMEDIATE
/// but commits clean deferred; (b) a delete that leaves a dangling child
/// and fails AT COMMIT (23503); (c) forcing the check early with
/// SET CONSTRAINTS ALL IMMEDIATE mid-txn (fails there, rolled back).
fn gen_deferred_group(g: &mut Gen) -> Vec<StmtKind> {
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!("CREATE TABLE {} (id int4 PRIMARY KEY, tag text);", p)),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (cid int4 PRIMARY KEY, pid int4 REFERENCES {}(id) \
             DEFERRABLE INITIALLY DEFERRED, note text);",
            c, p
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 'a'), (2, 'b');", p)),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (10, 1, 'x'), (11, 2, 'y');", c)),
    ];
    match g.weights.pick(
        g.rng,
        &["ritrig:deferred:satisfy", "ritrig:deferred:commitfail", "ritrig:deferred:setimm"],
    ) {
        "ritrig:deferred:commitfail" => {
            g.fire("ritrig:deferred:commitfail");
            // Deleting a referenced parent is fine mid-txn (deferred) but
            // the queued check fails AT COMMIT; the whole txn rolls back.
            out.push(StmtKind::Raw("BEGIN;".to_string()));
            out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)));
            out.push(StmtKind::Raw("COMMIT;".to_string()));
        }
        "ritrig:deferred:setimm" => {
            g.fire("ritrig:deferred:setimm");
            // Force the deferred check early: it fires (and aborts) at the
            // SET CONSTRAINTS statement, so the bracket ends in ROLLBACK.
            out.push(StmtKind::Raw("BEGIN;".to_string()));
            out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)));
            out.push(StmtKind::Raw("SET CONSTRAINTS ALL IMMEDIATE;".to_string()));
            out.push(StmtKind::Raw("ROLLBACK;".to_string()));
        }
        _ => {
            g.fire("ritrig:deferred:satisfy");
            // Order that would fail IMMEDIATE (delete parent before child)
            // commits clean because the check is deferred to COMMIT and the
            // child is gone by then.
            out.push(StmtKind::Raw("BEGIN;".to_string()));
            out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)));
            out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE pid = 1;", c)));
            out.push(StmtKind::Raw("COMMIT;".to_string()));
        }
    }
    out.extend(probe_pair(&p, &c));
    out.extend(drop_pair(&p, &c));
    out
}

/// ADD CONSTRAINT ... NOT VALID then VALIDATE CONSTRAINT: the NOT VALID add
/// skips the scan (always succeeds); VALIDATE runs
/// validateForeignKeyConstraint over the existing rows. A pre-seeded
/// dangling child makes VALIDATE fail (23503); the clean arm makes it pass.
fn gen_notvalid_group(g: &mut Gen) -> Vec<StmtKind> {
    let dangle = g.rng.chance(1, 2);
    if dangle {
        g.fire("ritrig:notvalid:fail");
    } else {
        g.fire("ritrig:notvalid:pass");
    }
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    let kname = format!("fz_rifk_{}", n);
    let child_rows = if dangle {
        // pid 7 has no parent — VALIDATE must reject it.
        "(10, 1, 'x'), (11, 2, 'y'), (12, 7, 'dangle')"
    } else {
        "(10, 1, 'x'), (11, 2, 'y'), (12, 3, 'z')"
    };
    let mut out = vec![
        StmtKind::Raw(format!("CREATE TABLE {} (id int4 PRIMARY KEY, tag text);", p)),
        StmtKind::Raw(format!("CREATE TABLE {} (cid int4 PRIMARY KEY, pid int4, note text);", c)),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c');", p)),
        StmtKind::Raw(format!("INSERT INTO {} VALUES {};", c, child_rows)),
        // NOT VALID add: no scan, always succeeds even with the dangling row.
        StmtKind::Raw(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY (pid) REFERENCES {}(id) NOT VALID;",
            c, kname, p
        )),
        // The scan: fails on the dangling arm, succeeds on the clean arm.
        StmtKind::Raw(format!("ALTER TABLE {} VALIDATE CONSTRAINT {};", c, kname)),
        // convalidated flips only on success — a catalog witness either way.
        StmtKind::Raw(format!(
            "SELECT conname, convalidated FROM pg_constraint WHERE conname = '{}';",
            kname
        )),
    ];
    out.extend(probe_pair(&p, &c));
    out.extend(drop_pair(&p, &c));
    out
}

/// FK to a partitioned parent and FK from a partitioned child: the RI
/// machinery must route the check/cascade across partitions. Cascading
/// delete over a range-partitioned parent removes children in every
/// partition; the pk-ordered probe is over the whole partitioned relation.
fn gen_part_group(g: &mut Gen) -> Vec<StmtKind> {
    let from_child = g.rng.chance(1, 2);
    let n = fresh(g);
    let p = format!("fz_rip_{}", n);
    let c = format!("fz_ric_{}", n);
    if from_child {
        g.fire("ritrig:part:fromchild");
        // Plain parent, partitioned child referencing it with CASCADE.
        let mut out = vec![
            StmtKind::Raw(format!("CREATE TABLE {} (id int4 PRIMARY KEY, tag text);", p)),
            StmtKind::Raw(format!(
                "CREATE TABLE {} (cid int4, pid int4 REFERENCES {}(id) ON DELETE CASCADE, note text, \
                 PRIMARY KEY (cid)) PARTITION BY RANGE (cid);",
                c, p
            )),
            StmtKind::Raw(format!(
                "CREATE TABLE {}_p0 PARTITION OF {} FOR VALUES FROM (0) TO (100);",
                c, c
            )),
            StmtKind::Raw(format!(
                "CREATE TABLE {}_p1 PARTITION OF {} FOR VALUES FROM (100) TO (200);",
                c, c
            )),
            StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 'a'), (2, 'b'), (3, 'c');", p)),
            StmtKind::Raw(format!(
                "INSERT INTO {} VALUES (10, 1, 'x'), (110, 1, 'y'), (11, 2, 'z'), (111, 3, 'w');",
                c
            )),
            StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)),
            StmtKind::Raw(format!("SELECT id, tag FROM {} ORDER BY id;", p)),
            StmtKind::Raw(format!("SELECT cid, pid, note FROM {} ORDER BY cid;", c)),
        ];
        out.extend(drop_pair(&p, &c));
        return out;
    }
    g.fire("ritrig:part:toparent");
    // Partitioned parent, plain child referencing it with CASCADE.
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (id int4, tag text, PRIMARY KEY (id)) PARTITION BY RANGE (id);",
            p
        )),
        StmtKind::Raw(format!(
            "CREATE TABLE {}_p0 PARTITION OF {} FOR VALUES FROM (0) TO (10);",
            p, p
        )),
        StmtKind::Raw(format!(
            "CREATE TABLE {}_p1 PARTITION OF {} FOR VALUES FROM (10) TO (20);",
            p, p
        )),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (cid int4 PRIMARY KEY, pid int4 REFERENCES {}(id) ON DELETE CASCADE, note text);",
            c, p
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 'a'), (11, 'b'), (5, 'c');", p)),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (10, 1, 'x'), (12, 11, 'y'), (13, 5, 'z');",
            c
        )),
        StmtKind::Raw(format!("DELETE FROM {} WHERE id = 1;", p)),
        StmtKind::Raw(format!("SELECT id, tag FROM {} ORDER BY id;", p)),
        StmtKind::Raw(format!("SELECT cid, pid, note FROM {} ORDER BY cid;", c)),
    ];
    out.extend(drop_pair(&p, &c));
    out
}

/// CHECK constraint + domain CHECK: the 23514 (check_violation) identity
/// that shares the constraint-validation machinery. A table CHECK rejects
/// out-of-range rows; a domain CHECK rejects at the type boundary.
fn gen_check_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig:check");
    let n = fresh(g);
    let t = format!("fz_rick_{}", n);
    let d = format!("fz_ricd_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!("CREATE DOMAIN {} AS int4 CHECK (VALUE >= 0);", d)),
        StmtKind::Raw(format!(
            "CREATE TABLE {} (id int4 PRIMARY KEY, amt int4 CHECK (amt < 100), dv {});",
            t, d
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 10, 5), (2, 50, 0);", t)),
        // Table-CHECK violation (amt >= 100): 23514.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (3, 200, 1);", t)),
        // Domain-CHECK violation (dv < 0): 23514.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (4, 10, -1);", t)),
        // An UPDATE into violation must also be rejected and leave the row.
        StmtKind::Raw(format!("UPDATE {} SET amt = 999 WHERE id = 1;", t)),
        StmtKind::Raw(format!("SELECT id, amt, dv FROM {} ORDER BY id;", t)),
        StmtKind::Raw(format!("DROP TABLE {};", t)),
        StmtKind::Raw(format!("DROP DOMAIN {};", d)),
    ];
    let _ = &mut out;
    out
}

/// EXCLUDE constraint: the 23P01 (exclusion_violation) identity. A btree
/// equality-exclusion is a unique-like guard reached through the
/// exclusion-constraint machinery rather than the unique-index path.
fn gen_exclude_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig:exclude");
    let n = fresh(g);
    let t = format!("fz_rice_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (id int4 PRIMARY KEY, slot int4, EXCLUDE USING btree (slot WITH =));",
            t
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 10), (2, 20);", t)),
        // Second row on slot 10 collides: 23P01.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (3, 10);", t)),
        // An UPDATE into the taken slot also collides.
        StmtKind::Raw(format!("UPDATE {} SET slot = 20 WHERE id = 1;", t)),
        StmtKind::Raw(format!("SELECT id, slot FROM {} ORDER BY id;", t)),
        StmtKind::Raw(format!("DROP TABLE {};", t)),
    ];
    let _ = &mut out;
    out
}

/// UNIQUE / PRIMARY KEY enforcement: the 23505 (unique_violation) identity.
/// Duplicate PK and duplicate UNIQUE-column inserts are rejected; the probe
/// proves the surviving image.
fn gen_unique_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("ritrig:unique");
    let n = fresh(g);
    let t = format!("fz_ricu_{}", n);
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (id int4 PRIMARY KEY, code int4 UNIQUE, tag text);",
            t
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 100, 'a'), (2, 200, 'b');", t)),
        // Duplicate PK: 23505.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (1, 300, 'dup-pk');", t)),
        // Duplicate UNIQUE code: 23505.
        StmtKind::Raw(format!("INSERT INTO {} VALUES (3, 100, 'dup-code');", t)),
        // UPDATE into a duplicate code: 23505, row unchanged.
        StmtKind::Raw(format!("UPDATE {} SET code = 200 WHERE id = 1;", t)),
        StmtKind::Raw(format!("SELECT id, code, tag FROM {} ORDER BY id;", t)),
        StmtKind::Raw(format!("DROP TABLE {};", t)),
    ];
    let _ = &mut out;
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = RiState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.ritrig, &mut state);
            let kinds = gen_ritrig_module(&mut g);
            std::mem::swap(&mut g.ritrig, &mut state);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods)
    }

    #[test]
    fn ritrig_is_deterministic() {
        let (a, _) = gen_groups(41, 500, "");
        let (b, _) = gen_groups(41, 500, "");
        assert_eq!(a, b);
        let (c, _) = gen_groups(42, 500, "");
        assert_ne!(a, c);
    }

    #[test]
    fn ritrig_variety_and_shape() {
        let (groups, prods) = gen_groups(0x21, 1200, "");
        let all: Vec<String> = groups.iter().flatten().cloned().collect();
        let hay = all.join("\n");
        for frag in [
            "ON DELETE CASCADE ON UPDATE CASCADE",
            "ON DELETE SET NULL",
            "ON UPDATE SET NULL",
            "ON DELETE SET NULL (fa)",
            "DEFAULT 3 REFERENCES",
            "ON DELETE SET DEFAULT ON UPDATE SET DEFAULT",
            "ON DELETE RESTRICT ON UPDATE RESTRICT",
            "ON DELETE NO ACTION ON UPDATE NO ACTION",
            "MATCH FULL",
            "MATCH SIMPLE",
            "FOREIGN KEY (fa, fb) REFERENCES",
            "parent int4 REFERENCES",
            "DEFERRABLE INITIALLY DEFERRED",
            "SET CONSTRAINTS ALL IMMEDIATE;",
            "ADD CONSTRAINT fz_rifk_",
            "NOT VALID;",
            "VALIDATE CONSTRAINT fz_rifk_",
            "PARTITION BY RANGE (cid)",
            "PARTITION BY RANGE (id)",
            "PARTITION OF ",
            "CREATE DOMAIN fz_ricd_",
            "CHECK (amt < 100)",
            "CHECK (VALUE >= 0)",
            "EXCLUDE USING btree (slot WITH =)",
            "code int4 UNIQUE",
            "ORDER BY id;",
            "ORDER BY cid;",
            "ORDER BY a, b;",
            "pg_constraint WHERE conname =",
            "convalidated",
            "BEGIN;",
            "COMMIT;",
            "ROLLBACK;",
        ] {
            assert!(hay.contains(frag), "ritrig flavor {frag:?} never generated");
        }
        for p in [
            "ritrig:cascade",
            "ritrig:setnull",
            "ritrig:setdefault",
            "ritrig:restrict",
            "ritrig:matchfull",
            "ritrig:composite",
            "ritrig:selfref",
            "ritrig:deferred",
            "ritrig:notvalid",
            "ritrig:part",
            "ritrig:check",
            "ritrig:exclude",
            "ritrig:unique",
            "ritrig:cascade:del",
            "ritrig:match:full",
            "ritrig:match:simple",
            "ritrig:restrict:restrict",
            "ritrig:restrict:noaction",
            "ritrig:deferred:commitfail",
            "ritrig:deferred:setimm",
            "ritrig:deferred:satisfy",
            "ritrig:notvalid:fail",
            "ritrig:notvalid:pass",
            "ritrig:part:fromchild",
            "ritrig:part:toparent",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Statement shape invariants (mirrors the registry-wide gate) plus
    /// group self-containment: every created object is dropped in-group and
    /// every txn bracket is closed.
    #[test]
    fn ritrig_groups_are_self_contained() {
        let (groups, _) = gen_groups(0x5A, 800, "");
        for group in &groups {
            for sql in group {
                assert!(!sql.contains('\n'), "multi-line statement: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
            let hay = group.join("\n");
            // Every CREATE TABLE has a matching DROP TABLE in-group (partition
            // children go away with the parent DROP, so DROP count may be
            // lower — assert every non-partition parent/child table drops by
            // counting the base fixtures instead).
            assert_eq!(
                hay.matches("CREATE DOMAIN ").count(),
                hay.matches("DROP DOMAIN ").count(),
                "unbalanced CREATE/DROP DOMAIN in group:\n{hay}"
            );
            // Transaction brackets are balanced (BEGIN paired with exactly one
            // terminator).
            let begins = hay.matches("BEGIN;").count();
            let ends = hay.matches("COMMIT;").count() + hay.matches("ROLLBACK;").count();
            assert_eq!(begins, ends, "unbalanced txn bracket in group:\n{hay}");
        }
    }

    /// Every top-level (non-partition) CREATE TABLE is dropped in its group.
    #[test]
    fn ritrig_tables_drop_in_group() {
        let (groups, _) = gen_groups(0x99, 800, "");
        for group in &groups {
            let hay = group.join("\n");
            // Count CREATE TABLE statements that are NOT "PARTITION OF"
            // (those are dropped implicitly with their parent).
            let creates = group
                .iter()
                .filter(|s| s.contains("CREATE TABLE ") && !s.contains("PARTITION OF"))
                .count();
            let drops = hay.matches("DROP TABLE ").count();
            assert_eq!(creates, drops, "unbalanced top-level CREATE/DROP TABLE:\n{hay}");
        }
    }
}
