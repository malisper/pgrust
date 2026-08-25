//! ACL/RLS drain module: the privilege-decision and row-security execution
//! surface that the standing `adtmisc` ACL families deliberately leave
//! hollow. adtmisc keeps every fuzz role owning nothing and its RLS bracket
//! minimal (one SELECT + one INSERT policy over PUBLIC); the biggest
//! uncovered spans in line-gap-report-004 are therefore the arms that only
//! fire when a role actually *owns* objects, when *many* policies of mixed
//! permissive/restrictive/command kinds are *applied* to a running query,
//! and when a grant is *delegated* down a grant-option chain:
//!
//!   - backend/rewrite/rowsecurity.c: get_row_security_policies (136),
//!     add_security_quals (26), add_with_check_options (38),
//!     get_policies_for_relation (32), check_role_for_policy,
//!     sort_policies_by_name, row_security_policy_cmp;
//!   - backend/commands/policy.c: CreatePolicy (76), AlterPolicy (88),
//!     rename_policy (33), RemovePolicyById, RemoveRoleFromObjectPolicy,
//!     RelationBuildRowSecurity (53), get_relation_policy_oid,
//!     relation_has_policies, policy_role_list_to_array;
//!   - backend/catalog/aclchk.c: ExecGrant_Relation (124),
//!     ExecGrant_Attribute (55), ExecGrant_common (59), ExecuteGrantStmt
//!     (80), restrict_and_check_grant, SetDefaultACL (89),
//!     ExecAlterDefaultPrivilegesStmt (68), get_user_default_acl,
//!     get_default_acl_internal, expand_col_privileges, merge_acl_with_grant;
//!   - backend/catalog/pg_shdepend.c: checkSharedDependencies (61),
//!     shdepDropOwned (39), shdepReassignOwned (20), shdepChangeDep (37),
//!     changeDependencyOnOwner, recordDependencyOnOwner, storeObjectDescription;
//!   - backend/utils/adt/acl.c: aclupdate, aclmerge, select_best_grantor,
//!     select_best_admin, recursive_revoke, roles_is_member_of,
//!     is_member_of_role, has_privs_of_role, aclparse, aclitemin/aclitemout,
//!     getid/putid (quoted-role parse), aclmembers, aclexplode, acldefault.
//!
//! Everything is emitted as `StmtKind::Raw`; each shape is a self-contained
//! bracket that CREATEs its own cluster-global roles + tables, exercises the
//! surface, and DROPs everything it created, in one group (no other module
//! interleaves within a group). No dependency on the objddl role pool.
//!
//! Differential-safety disciplines (the compare surface is result identity
//! under a total ORDER BY, error identity — especially 42501
//! insufficient_privilege and the RLS WITH CHECK violation — and, the
//! security-critical one, the permission DECISION itself: allowed vs denied
//! must match C exactly):
//!   - fixed object/role names throughout (fz_acl_*), so every name that
//!     reaches a comparable output (aclitem text, pg_policies.roles,
//!     dependency-error DETAIL, current_user inside a policy) is identical
//!     across engines; role/object *oids* differ across engines and are
//!     NEVER emitted — oid-bearing outputs (relacl, aclexplode grantor/
//!     grantee) are always cast through `::regrole::text` and ordered.
//!     FP-12 (round-10): roles are CLUSTER-global (pg_authid), so the
//!     fixed names raced concurrent driver instances' DROP/CREATE brackets
//!     (42704 `role "fz_acl_ud1" does not exist` on one side of a GRANT,
//!     2BP01 on the other side's DROP ROLE — the FP-1/RB-9 shared-namespace
//!     bleed, now on roles). The diffrunner therefore rewrites the whole
//!     module-owned `fz_acl_` namespace into the batch-unique
//!     `{db}_fz_acl_` prefix via [`rebase_role_names`] — identically on
//!     both sides, so name identity across engines is preserved —
//!     and helper_diffrun's teardown reclaims `{db}_`-prefixed roles;
//!   - the bootstrap superuser is named `postgres` on both engines (the
//!     REASSIGN OWNED / owner-restore target), matching the standing
//!     adtmisc assumption;
//!   - no explicit transaction bracket: statements are independent under
//!     autocommit, so a deliberate mid-bracket error (WITH CHECK violation,
//!     drop-role-with-deps) is isolated and every cleanup statement still
//!     runs; the SET ROLE window is always closed by a later RESET ROLE
//!     (a session GUC survives a statement error), and all DROPs that need
//!     ownership run *after* RESET ROLE;
//!   - each bracket is idempotent/self-healing: it leads with DROP TABLE
//!     IF EXISTS ... CASCADE (which also strips the table's ACL entries and
//!     policies) *before* DROP ROLE IF EXISTS, so a re-run over residue from
//!     a crashed prior group finds roles owning nothing and drops cleanly;
//!   - deterministic data only (no now()/random()); result rows always
//!     carry a total ORDER BY; row *sets* under RLS are a pure function of
//!     the fixed data and the policy logic, which is exactly the behavior
//!     under test;
//!   - the schema-public ACL is bare in the differential runner (no PUBLIC
//!     USAGE), so any bracket that SET ROLEs first GRANTs USAGE ON SCHEMA
//!     public to its roles and REVOKEs it at the end;
//!   - deliberate error fuel (WITH CHECK violations, drop-role dependency
//!     errors, denied writes under a plain role, bogus aclitem literals)
//!     rides one `aclrls:ok`/`aclrls:err` weight pair, biased away from
//!     both-sides-error per the findings-budget rule.

use crate::stmt::{Gen, StmtKind};

/// FP-12 (round-10): rewrite the module-owned `fz_acl_` namespace into
/// the batch-unique `{tag}_fz_acl_` namespace (tag = the batch's private
/// scratch-db name, exactly as gramwalk's FP-1 database rebase and the
/// RB-9 tablespace rebase). Roles live in the cluster-global pg_authid,
/// so two concurrent driver batches running this module's DROP/CREATE
/// brackets under fixed names race each other and the A/B interleavings
/// differ (observed as `A succeeded; B errored 42704 (role "fz_acl_ud1"
/// does not exist)` on a GRANT and the mirrored 2BP01 on a DROP ROLE,
/// run f13de995...-59-13, seeds 1357452453456421209 /
/// 3889503843230171335).
///
/// Unlike the tablespace rebase this is a PLAIN textual prefix rewrite,
/// deliberately including quoted material: the module embeds its role
/// names inside aclitem string literals (`aclitemin('"fz_acl_q1"=r*w/
/// postgres')`) and catalog-probe literals (`WHERE tablename =
/// 'fz_acl_rls_d'`), which must move with the identifiers to stay
/// coherent. The prefix is module-owned and collision-free by
/// convention, every occurrence is rewritten, and the statement TEXT
/// stays identical on both sides, so differential parity is untouched.
/// Module tables (also `fz_acl_`-prefixed) are db-local and need no
/// rebase, but ride along harmlessly and consistently. Names stay well
/// under the 63-byte identifier bound (tag <= ~30 bytes per
/// helper_diffrun, longest suffix ~12). Applied by the runner to the
/// WHOLE statement stream; helper_diffrun reclaims `{tag}_`-prefixed
/// roles at batch cleanup (DROP OWNED BY, then DROP ROLE).
pub fn rebase_role_names(sql: &str, tag: &str) -> String {
    const PREFIX: &str = "fz_acl_";
    if !sql.contains(PREFIX) {
        return sql.to_string();
    }
    sql.replace(PREFIX, &format!("{tag}_{PREFIX}"))
}

/// Top-level shape selection (registered in weights::PROD_WEIGHTS).
const SHAPES: &[&str] = &[
    "aclrls:rls:apply",
    "aclrls:rls:force",
    "aclrls:rls:ddl",
    "aclrls:grant:rel",
    "aclrls:grant:defacl",
    "aclrls:own:shdep",
    "aclrls:role:member",
    "aclrls:aclitem",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_aclrls_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls");
    match g.weights.pick(g.rng, SHAPES) {
        "aclrls:rls:apply" => gen_rls_apply(g),
        "aclrls:rls:force" => gen_rls_force(g),
        "aclrls:rls:ddl" => gen_rls_ddl(g),
        "aclrls:grant:rel" => gen_grant_rel(g),
        "aclrls:grant:defacl" => gen_grant_defacl(g),
        "aclrls:own:shdep" => gen_own_shdep(g),
        "aclrls:role:member" => gen_role_member(g),
        "aclrls:aclitem" => gen_aclitem(g),
        other => unreachable!("unknown aclrls shape {other}"),
    }
}

/// One-knob error-fuel bias: err arms host deliberate matched errors.
fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["aclrls:ok", "aclrls:err"]) == "aclrls:err" {
        g.fire("aclrls:err");
        true
    } else {
        false
    }
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(v: Vec<String>) -> Vec<StmtKind> {
    v.into_iter().map(StmtKind::Raw).collect()
}

// -------------------------------------------------------------- theme B ----
// Deep row-security policy application (rowsecurity.c). Many policies of
// mixed permissive/restrictive kind across every command, then SELECT /
// INSERT / UPDATE / DELETE / ON CONFLICT run under a non-privileged role so
// get_row_security_policies + add_security_quals + add_with_check_options
// build and apply the real quals. The permission DECISION (which rows are
// visible, which writes are allowed vs raise 42501) is the compare surface.

/// Fixed RLS fixture + role set. `tab`, `u1`, `u2` are the group-local
/// names; the caller varies the policy mix and the driven statements.
struct RlsFixture {
    tab: &'static str,
    u1: &'static str,
    u2: &'static str,
}

/// Prelude that (re)creates a fresh RLS table with deterministic rows whose
/// `owner_name` column names the two fuzz roles and `postgres`, so
/// current_user-based policies partition the rows predictably.
fn rls_prelude(fx: &RlsFixture) -> Vec<String> {
    vec![
        format!("DROP TABLE IF EXISTS {} CASCADE;", fx.tab),
        format!("DROP ROLE IF EXISTS {};", fx.u1),
        format!("DROP ROLE IF EXISTS {};", fx.u2),
        format!("CREATE ROLE {} NOLOGIN;", fx.u1),
        format!("CREATE ROLE {} NOLOGIN;", fx.u2),
        format!(
            "CREATE TABLE {} (id int PRIMARY KEY, owner_name text NOT NULL, val int NOT NULL, tag text);",
            fx.tab
        ),
        format!(
            "INSERT INTO {} VALUES (1, '{}', 3, 'a'), (2, '{}', 8, 'b'), (3, '{}', 50, 'c'), (4, 'postgres', 120, 'd'), (5, '{}', 15, 'e'), (6, '{}', 90, 'f');",
            fx.tab, fx.u1, fx.u2, fx.u1, fx.u2, fx.u1
        ),
        format!("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}, {};", fx.tab, fx.u1, fx.u2),
        format!("GRANT USAGE ON SCHEMA public TO {}, {};", fx.u1, fx.u2),
    ]
}

/// Cleanup that closes the SET ROLE window and drops everything (tables
/// first so DROP ROLE finds the roles owning nothing).
fn rls_cleanup(fx: &RlsFixture) -> Vec<String> {
    vec![
        "RESET ROLE;".to_string(),
        format!("DROP TABLE {};", fx.tab),
        format!("REVOKE USAGE ON SCHEMA public FROM {}, {};", fx.u1, fx.u2),
        format!("DROP ROLE {};", fx.u1),
        format!("DROP ROLE {};", fx.u2),
    ]
}

fn gen_rls_apply(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:rls:apply");
    let fx = RlsFixture { tab: "fz_acl_rls_a", u1: "fz_acl_ua1", u2: "fz_acl_ua2" };
    let mut out = rls_prelude(&fx);
    out.push(format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", fx.tab));
    // A mixed policy set: a per-role permissive SELECT, a broad permissive
    // SELECT, a restrictive ceiling FOR ALL, and per-command INSERT/UPDATE/
    // DELETE policies with WITH CHECK. sort_policies_by_name orders them;
    // get_row_security_policies partitions by command + permissive kind.
    let restrictive = g.rng.chance(2, 3);
    out.push(format!(
        "CREATE POLICY fz_p_sel_self ON {} FOR SELECT TO {} USING (owner_name = current_user);",
        fx.tab, fx.u1
    ));
    out.push(format!(
        "CREATE POLICY fz_p_sel_hi ON {} AS PERMISSIVE FOR SELECT TO {} USING (val >= 10);",
        fx.tab, fx.u2
    ));
    if restrictive {
        out.push(format!(
            "CREATE POLICY fz_p_all_ceil ON {} AS RESTRICTIVE FOR ALL TO PUBLIC USING (val < 100) WITH CHECK (val >= 0);",
            fx.tab
        ));
    }
    out.push(format!(
        "CREATE POLICY fz_p_ins ON {} FOR INSERT TO PUBLIC WITH CHECK (owner_name = current_user);",
        fx.tab
    ));
    out.push(format!(
        "CREATE POLICY fz_p_upd ON {} FOR UPDATE TO PUBLIC USING (owner_name = current_user) WITH CHECK (val < 1000);",
        fx.tab
    ));
    out.push(format!(
        "CREATE POLICY fz_p_del ON {} FOR DELETE TO PUBLIC USING (owner_name = current_user);",
        fx.tab
    ));
    // Drive the surface as u1 (its rows are ids 1,3,5).
    out.push(format!("SET ROLE {};", fx.u1));
    out.push(format!("SELECT id, owner_name, val, tag FROM {} ORDER BY id;", fx.tab));
    out.push(format!("SELECT count(*), coalesce(sum(val), 0) FROM {};", fx.tab));
    // FOR UPDATE row lock exercises the SELECT policy on a locking scan.
    out.push(format!(
        "SELECT id FROM {} WHERE val < 100 ORDER BY id FOR UPDATE;",
        fx.tab
    ));
    // INSERT: satisfies the INSERT WITH CHECK (owner_name = current_user).
    out.push(format!(
        "INSERT INTO {} VALUES (10, current_user, 20, 'ins') RETURNING id, val;",
        fx.tab
    ));
    // UPDATE within the USING (own rows) + WITH CHECK (val < 1000) window.
    out.push(format!(
        "UPDATE {} SET val = val + 1 WHERE id = 1 RETURNING id, val;",
        fx.tab
    ));
    if err_arm(g) {
        // WITH CHECK violation: raise val past the restrictive ceiling
        // (42501 "new row violates row-level security policy") when the
        // restrictive policy is present; else past the UPDATE ceiling is
        // unreachable, so violate the INSERT check with a foreign owner.
        if restrictive {
            out.push(format!("UPDATE {} SET val = 500 WHERE id = 3;", fx.tab));
        } else {
            out.push(format!(
                "INSERT INTO {} VALUES (11, '{}', 5, 'bad');",
                fx.tab, fx.u2
            ));
        }
    }
    // DELETE under the DELETE USING policy (own rows only).
    out.push(format!("DELETE FROM {} WHERE id = 5;", fx.tab));
    // INSERT ... ON CONFLICT DO UPDATE: the conflict-update WITH CHECK arm.
    out.push(format!(
        "INSERT INTO {} VALUES (1, current_user, 7, 'cf') ON CONFLICT (id) DO UPDATE SET val = excluded.val;",
        fx.tab
    ));
    out.push(format!("SELECT id, owner_name, val FROM {} ORDER BY id;", fx.tab));
    out.extend(rls_cleanup(&fx));
    raw(out)
}

fn gen_rls_force(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:rls:force");
    let fx = RlsFixture { tab: "fz_acl_rls_f", u1: "fz_acl_uf1", u2: "fz_acl_uf2" };
    let mut out = rls_prelude(&fx);
    // The table owner is postgres (creator); FORCE makes the owner subject
    // to the policies too, and check_role_for_policy / the owner-bypass arm
    // of get_row_security_policies both fire.
    out.push(format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", fx.tab));
    out.push(format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY;", fx.tab));
    out.push(format!(
        "CREATE POLICY fz_pf_all ON {} FOR ALL TO PUBLIC USING (val < 100) WITH CHECK (val < 100);",
        fx.tab
    ));
    out.push(format!(
        "CREATE POLICY fz_pf_sel ON {} AS RESTRICTIVE FOR SELECT TO PUBLIC USING (tag IS NOT NULL);",
        fx.tab
    ));
    // As the owner (postgres) under FORCE: the policies apply.
    out.push(format!("SELECT id, val FROM {} ORDER BY id;", fx.tab));
    out.push(format!(
        "UPDATE {} SET val = val WHERE id = 1 RETURNING id;",
        fx.tab
    ));
    if err_arm(g) {
        // Owner is forced through WITH CHECK too: this raises 42501.
        out.push(format!("UPDATE {} SET val = 999 WHERE id = 1;", fx.tab));
    }
    // NO FORCE restores the owner bypass; the owner now sees all rows.
    out.push(format!("ALTER TABLE {} NO FORCE ROW LEVEL SECURITY;", fx.tab));
    out.push(format!("SELECT count(*) FROM {};", fx.tab));
    out.push("RESET ROLE;".to_string());
    out.push(format!("DROP TABLE {};", fx.tab));
    out.push(format!("REVOKE USAGE ON SCHEMA public FROM {}, {};", fx.u1, fx.u2));
    out.push(format!("DROP ROLE {};", fx.u1));
    out.push(format!("DROP ROLE {};", fx.u2));
    raw(out)
}

// -------------------------------------------------------------- theme C ----
// Policy DDL lifecycle (policy.c): CREATE, ALTER (roles/USING/WITH CHECK),
// RENAME, DROP, plus RemoveRoleFromObjectPolicy via DROP OWNED of a role
// named in a policy. Probed through the deterministic pg_policies view.

fn gen_rls_ddl(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:rls:ddl");
    let fx = RlsFixture { tab: "fz_acl_rls_d", u1: "fz_acl_ud1", u2: "fz_acl_ud2" };
    let mut out = rls_prelude(&fx);
    out.push(format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", fx.tab));
    let permissive = if g.rng.chance(1, 2) { "PERMISSIVE" } else { "RESTRICTIVE" };
    let cmd = pick_str(g, &["ALL", "SELECT", "INSERT", "UPDATE", "DELETE"]);
    // CreatePolicy: FOR <cmd> with USING/WITH CHECK as the command allows.
    let mut create = format!(
        "CREATE POLICY fz_pd1 ON {} AS {} FOR {} TO {}",
        fx.tab, permissive, cmd, fx.u1
    );
    if cmd != "INSERT" {
        create.push_str(" USING (val < 50)");
    }
    if cmd == "INSERT" || cmd == "UPDATE" || cmd == "ALL" {
        create.push_str(" WITH CHECK (val >= 0)");
    }
    create.push(';');
    out.push(create);
    // A second policy so rename ordering + relation_has_policies see >1.
    out.push(format!(
        "CREATE POLICY fz_pd2 ON {} FOR SELECT TO PUBLIC USING (true);",
        fx.tab
    ));
    out.push(format!(
        "SELECT policyname, permissive, roles, cmd, qual, with_check FROM pg_policies WHERE tablename = '{}' ORDER BY policyname;",
        fx.tab
    ));
    // AlterPolicy: change the role list and the expressions.
    out.push(format!(
        "ALTER POLICY fz_pd1 ON {} TO {}, {};",
        fx.tab, fx.u1, fx.u2
    ));
    if cmd != "INSERT" {
        out.push(format!(
            "ALTER POLICY fz_pd1 ON {} USING (val < 80);",
            fx.tab
        ));
    }
    // rename_policy.
    out.push(format!(
        "ALTER POLICY fz_pd1 ON {} RENAME TO fz_pd1r;",
        fx.tab
    ));
    if err_arm(g) {
        // Duplicate policy name on the same relation: matched 42710.
        out.push(format!(
            "ALTER POLICY fz_pd2 ON {} RENAME TO fz_pd1r;",
            fx.tab
        ));
    }
    out.push(format!(
        "SELECT policyname, roles, cmd FROM pg_policies WHERE tablename = '{}' ORDER BY policyname;",
        fx.tab
    ));
    // RemoveRoleFromObjectPolicy: DROP OWNED strips u2 from fz_pd1r's roles.
    out.push(format!("DROP OWNED BY {};", fx.u2));
    out.push(format!(
        "SELECT policyname, roles FROM pg_policies WHERE tablename = '{}' ORDER BY policyname;",
        fx.tab
    ));
    // RemovePolicyById / RemoveRoleFromObjectPolicy via explicit drops.
    out.push(format!("DROP POLICY fz_pd1r ON {};", fx.tab));
    out.push(format!("DROP POLICY IF EXISTS fz_pd2 ON {};", fx.tab));
    out.push(format!("ALTER TABLE {} DISABLE ROW LEVEL SECURITY;", fx.tab));
    out.push(format!("DROP TABLE {};", fx.tab));
    out.push(format!("REVOKE USAGE ON SCHEMA public FROM {}, {};", fx.u1, fx.u2));
    out.push(format!("DROP ROLE {};", fx.u1));
    out.push(format!("DROP ROLE {};", fx.u2));
    raw(out)
}

// -------------------------------------------------------------- theme D ----
// Deep relation/column GRANT + grant-option delegation chain + cascade
// revoke (aclchk ExecGrant_Relation/Attribute/common, ExecuteGrantStmt;
// acl.c aclupdate/aclmerge/select_best_grantor/recursive_revoke/aclmembers).

fn gen_grant_rel(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:grant:rel");
    let tab = "fz_acl_grt";
    let (g1, g2, g3) = ("fz_acl_g1", "fz_acl_g2", "fz_acl_g3");
    let mut out = vec![
        format!("DROP TABLE IF EXISTS {} CASCADE;", tab),
        format!("DROP ROLE IF EXISTS {};", g1),
        format!("DROP ROLE IF EXISTS {};", g2),
        format!("DROP ROLE IF EXISTS {};", g3),
        format!("CREATE ROLE {} NOLOGIN;", g1),
        format!("CREATE ROLE {} NOLOGIN;", g2),
        format!("CREATE ROLE {} NOLOGIN;", g3),
        format!("CREATE TABLE {} (c1 int PRIMARY KEY, c2 text, c3 int);", tab),
        format!("INSERT INTO {} VALUES (1, 'x', 10), (2, 'y', 20);", tab),
        format!("GRANT USAGE ON SCHEMA public TO {}, {}, {};", g1, g2, g3),
    ];
    // Multi-privilege, column-list grant WITH GRANT OPTION to g1
    // (ExecGrant_Relation + ExecGrant_Attribute + expand_col_privileges).
    out.push(format!(
        "GRANT SELECT (c1, c2), INSERT, UPDATE (c1, c3), REFERENCES (c1) ON {} TO {} WITH GRANT OPTION;",
        tab, g1
    ));
    // g1 delegates down the grant-option chain (select_best_grantor,
    // restrict_and_check_grant, merge_acl_with_grant).
    out.push(format!("SET ROLE {};", g1));
    out.push(format!("GRANT SELECT (c1) ON {} TO {};", tab, g2));
    out.push(format!(
        "GRANT SELECT (c1, c2), UPDATE (c1) ON {} TO {} WITH GRANT OPTION;",
        tab, g3
    ));
    out.push("RESET ROLE;".to_string());
    // g3 re-delegates one column further (deeper grantor chain).
    out.push(format!("SET ROLE {};", g3));
    out.push(format!("GRANT SELECT (c1) ON {} TO {};", tab, g2));
    out.push("RESET ROLE;".to_string());
    // Deterministic ACL projection: names via ::regrole::text, total order.
    out.push(format!(
        "SELECT grantor::regrole::text, grantee::regrole::text, privilege_type, is_grantable FROM aclexplode((SELECT relacl FROM pg_class WHERE oid = '{}'::regclass)) ORDER BY 1, 2, 3, 4;",
        tab
    ));
    out.push(format!(
        "SELECT has_table_privilege('{}', '{}', 'SELECT'), has_column_privilege('{}', '{}', 'c1', 'SELECT'), has_column_privilege('{}', '{}', 'c3', 'UPDATE');",
        g2, tab, g3, tab, g2, tab
    ));
    if err_arm(g) {
        // Dependent-privilege guard: plain REVOKE with grants outstanding
        // downstream (matched 2BP01 "dependent privileges exist").
        out.push(format!("REVOKE GRANT OPTION FOR SELECT (c1) ON {} FROM {} RESTRICT;", tab, g1));
    }
    // Cascade revoke recurses through the whole delegation chain
    // (recursive_revoke, aclupdate).
    out.push(format!("REVOKE SELECT (c1) ON {} FROM {} CASCADE;", tab, g1));
    out.push(format!("REVOKE ALL PRIVILEGES ON {} FROM {} CASCADE;", tab, g1));
    out.push(format!(
        "SELECT grantor::regrole::text, grantee::regrole::text, privilege_type FROM aclexplode((SELECT relacl FROM pg_class WHERE oid = '{}'::regclass)) ORDER BY 1, 2, 3;",
        tab
    ));
    out.push(format!("DROP TABLE {};", tab));
    out.push(format!("REVOKE USAGE ON SCHEMA public FROM {}, {}, {};", g1, g2, g3));
    out.push(format!("DROP ROLE {};", g1));
    out.push(format!("DROP ROLE {};", g2));
    out.push(format!("DROP ROLE {};", g3));
    raw(out)
}

// -------------------------------------------------------------- theme E ----
// DEFAULT PRIVILEGES that actually APPLY at object creation (aclchk
// SetDefaultACL, ExecAlterDefaultPrivilegesStmt, get_user_default_acl,
// get_default_acl_internal, SetDefaultACLsInSchemas).

fn gen_grant_defacl(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:grant:defacl");
    let d1 = "fz_acl_d1";
    let dtab = "fz_acl_dtab";
    let dseq = "fz_acl_dseq";
    let in_schema = g.rng.chance(1, 2);
    let sc = if in_schema { " IN SCHEMA public" } else { "" };
    let mut out = vec![
        format!("DROP TABLE IF EXISTS {} CASCADE;", dtab),
        format!("DROP SEQUENCE IF EXISTS {};", dseq),
        format!("DROP ROLE IF EXISTS {};", d1),
        format!("CREATE ROLE {} NOLOGIN;", d1),
        format!("GRANT USAGE ON SCHEMA public TO {};", d1),
    ];
    // Install default privileges for tables + sequences, then create the
    // objects so the defaults are stamped onto their fresh ACLs.
    out.push(format!(
        "ALTER DEFAULT PRIVILEGES{} GRANT SELECT, INSERT ON TABLES TO {};",
        sc, d1
    ));
    out.push(format!(
        "ALTER DEFAULT PRIVILEGES{} GRANT USAGE ON SEQUENCES TO {};",
        sc, d1
    ));
    out.push(format!("SELECT count(*) FROM pg_default_acl;"));
    out.push(format!("CREATE TABLE {} (a int PRIMARY KEY, b text);", dtab));
    out.push(format!("CREATE SEQUENCE {};", dseq));
    // Probe: the default grant is present on the new objects.
    out.push(format!(
        "SELECT has_table_privilege('{}', '{}', 'SELECT'), has_table_privilege('{}', '{}', 'INSERT'), has_sequence_privilege('{}', '{}', 'USAGE');",
        d1, dtab, d1, dtab, d1, dseq
    ));
    out.push(format!(
        "SELECT grantee::regrole::text, privilege_type FROM aclexplode((SELECT relacl FROM pg_class WHERE oid = '{}'::regclass)) ORDER BY 1, 2;",
        dtab
    ));
    // Withdraw the defaults (SetDefaultACL to empty removes the row); a new
    // object no longer carries the grant.
    out.push(format!(
        "ALTER DEFAULT PRIVILEGES{} REVOKE SELECT, INSERT ON TABLES FROM {};",
        sc, d1
    ));
    out.push(format!(
        "ALTER DEFAULT PRIVILEGES{} REVOKE USAGE ON SEQUENCES FROM {};",
        sc, d1
    ));
    out.push(format!("DROP TABLE {};", dtab));
    out.push(format!("DROP SEQUENCE {};", dseq));
    out.push(format!("REVOKE USAGE ON SCHEMA public FROM {};", d1));
    out.push(format!("DROP ROLE {};", d1));
    raw(out)
}

// -------------------------------------------------------------- theme A ----
// Role ownership + shared-dependency machinery (pg_shdepend.c
// checkSharedDependencies, shdepDropOwned, shdepReassignOwned, shdepChangeDep,
// changeDependencyOnOwner, recordDependencyOnOwner, storeObjectDescription;
// aclchk RemoveRoleFromObjectACL). A role that OWNS objects is what adtmisc
// deliberately never builds.

fn gen_own_shdep(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:own:shdep");
    let owner = "fz_acl_own";
    let heir = "fz_acl_heir";
    let tab = "fz_acl_otab";
    let vw = "fz_acl_ovw";
    let seq = "fz_acl_oseq";
    let mut out = vec![
        format!("DROP VIEW IF EXISTS {} CASCADE;", vw),
        format!("DROP TABLE IF EXISTS {} CASCADE;", tab),
        format!("DROP SEQUENCE IF EXISTS {};", seq),
        format!("DROP ROLE IF EXISTS {};", owner),
        format!("DROP ROLE IF EXISTS {};", heir),
        format!("CREATE ROLE {} NOLOGIN;", owner),
        format!("CREATE ROLE {} NOLOGIN;", heir),
        format!("GRANT USAGE, CREATE ON SCHEMA public TO {};", owner),
        // Objects owned by the fuzz role (recordDependencyOnOwner /
        // recordSharedDependencyOn): created while SET ROLE owner.
        format!("SET ROLE {};", owner),
        format!("CREATE TABLE {} (a int PRIMARY KEY, b text);", tab),
        format!("INSERT INTO {} VALUES (1, 'p'), (2, 'q');", tab),
        format!("CREATE SEQUENCE {};", seq),
        format!("CREATE VIEW {} AS SELECT a, b FROM {};", vw, tab),
        "RESET ROLE;".to_string(),
    ];
    // Grant something to the heir so DROP OWNED / RemoveRoleFromObjectACL
    // has an ACL entry to strip too.
    out.push(format!("GRANT SELECT ON {} TO {};", tab, heir));
    if err_arm(g) {
        // DROP ROLE with owned objects: matched 2BP01 dependency error
        // whose DETAIL is the deterministic getObjectDescription text.
        out.push(format!("DROP ROLE {};", owner));
    }
    // Ownership audit before reassign (regrole::text, deterministic).
    out.push(format!(
        "SELECT relname, relkind, relowner::regrole::text FROM pg_class WHERE relname IN ('{}', '{}', '{}') ORDER BY relname;",
        tab, vw, seq
    ));
    if g.rng.chance(1, 2) {
        // REASSIGN OWNED (shdepReassignOwned + shdepChangeDep +
        // changeDependencyOnOwner): owner -> postgres.
        out.push(format!("REASSIGN OWNED BY {} TO postgres;", owner));
        out.push(format!(
            "SELECT relname, relowner::regrole::text FROM pg_class WHERE relname IN ('{}', '{}') ORDER BY relname;",
            tab, vw
        ));
        // The objects now belong to postgres; drop them directly.
        out.push(format!("DROP VIEW {};", vw));
        out.push(format!("DROP TABLE {};", tab));
        out.push(format!("DROP SEQUENCE {};", seq));
        // DROP OWNED now only revokes the owner's schema grants.
        out.push(format!("DROP OWNED BY {};", owner));
    } else {
        // DROP OWNED with real owned objects (shdepDropOwned drops the
        // table/view/sequence AND the heir's ACL entry along with them).
        out.push(format!("DROP OWNED BY {} CASCADE;", owner));
        out.push(format!(
            "SELECT count(*) FROM pg_class WHERE relname IN ('{}', '{}', '{}');",
            tab, vw, seq
        ));
    }
    // The owner now owns nothing; both roles drop cleanly.
    out.push(format!("REVOKE ALL ON SCHEMA public FROM {};", owner));
    out.push(format!("DROP ROLE {};", owner));
    out.push(format!("DROP ROLE {};", heir));
    raw(out)
}

// -------------------------------------------------------------- theme F1 ---
// Role membership graph + membership predicates (acl.c roles_is_member_of,
// is_member_of_role, has_privs_of_role, is_admin_of_role, select_best_admin,
// member_can_set_role; INHERIT/SET options). All boolean outputs.

fn gen_role_member(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:role:member");
    let (m1, m2, m3) = ("fz_acl_m1", "fz_acl_m2", "fz_acl_m3");
    let mut out = vec![
        format!("DROP ROLE IF EXISTS {};", m3),
        format!("DROP ROLE IF EXISTS {};", m2),
        format!("DROP ROLE IF EXISTS {};", m1),
        format!("CREATE ROLE {} NOLOGIN;", m1),
        format!("CREATE ROLE {} NOLOGIN;", m2),
        format!("CREATE ROLE {} NOLOGIN;", m3),
    ];
    // A two-level membership chain with an admin option at the top and a
    // mix of INHERIT/SET options (member_can_set_role, roles_is_member_of).
    let inherit = if g.rng.chance(1, 2) { "TRUE" } else { "FALSE" };
    let set_opt = if g.rng.chance(1, 2) { "TRUE" } else { "FALSE" };
    out.push(format!("GRANT {} TO {} WITH ADMIN OPTION;", m1, m2));
    out.push(format!(
        "GRANT {} TO {} WITH INHERIT {}, SET {};",
        m2, m3, inherit, set_opt
    ));
    // Membership predicates: MEMBER (roles_is_member_of) vs USAGE
    // (has_privs_of_role, honoring INHERIT).
    out.push(format!(
        "SELECT pg_has_role('{}', '{}', 'MEMBER'), pg_has_role('{}', '{}', 'USAGE'), pg_has_role('{}', '{}', 'MEMBER');",
        m3, m1, m3, m1, m2, m1
    ));
    out.push(format!(
        "SELECT has_privs_of_role('{}', '{}'::regrole::oid), pg_has_role('{}', '{}', 'SET'), pg_has_role('{}', '{}', 'USAGE WITH ADMIN OPTION');",
        m3, m1, m3, m2, m2, m1
    ));
    // m2 holds admin option on m1, so it can regrant m1 (select_best_admin).
    out.push(format!("SET ROLE {};", m2));
    out.push(format!("GRANT {} TO {};", m1, m3));
    out.push("RESET ROLE;".to_string());
    out.push(format!(
        "SELECT pg_has_role('{}', '{}', 'MEMBER') FROM (SELECT 1) s;",
        m3, m1
    ));
    if err_arm(g) {
        // Circular membership is rejected: matched error (roles form a DAG).
        out.push(format!("GRANT {} TO {};", m3, m1));
    }
    // Cascade revoke of the admin-granted membership + cleanup.
    out.push(format!("REVOKE {} FROM {} CASCADE;", m1, m2));
    out.push(format!("REVOKE {} FROM {};", m1, m3));
    out.push(format!("REVOKE {} FROM {};", m2, m3));
    out.push(format!("DROP ROLE {};", m3));
    out.push(format!("DROP ROLE {};", m2));
    out.push(format!("DROP ROLE {};", m1));
    raw(out)
}

// -------------------------------------------------------------- theme F2 ---
// aclitem text I/O + acl.c pure arms (aclparse, aclitemin, aclitemout,
// getid/putid via quoted role names, aclmembers, aclexplode, acldefault,
// aclcontains, makeaclitem). Literal-driven scalar SELECTs, all ::text.

fn gen_aclitem(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("aclrls:aclitem");
    let err = err_arm(g);
    // Multi-statement arm: a quoted role name with embedded specials drives
    // getid/putid's quoting branches. The role is transient and fixed-named,
    // so nothing engine-variant reaches an output.
    if g.rng.chance(1, 6) {
        let name = pick_str(g, &["fz_acl_q1", "fz_acl_q2"]);
        return raw(vec![
            format!("DROP ROLE IF EXISTS \"{}\";", name),
            format!("CREATE ROLE \"{}\" NOLOGIN;", name),
            format!("SELECT aclitemin('\"{}\"=r*w/postgres')::text;", name),
            format!("SELECT aclitemin('postgres=arw/\"{}\"')::text;", name),
            format!("DROP ROLE \"{}\";", name),
        ]);
    }
    // aclitem literals only ever name the bootstrap superuser (fixed on
    // both engines) or PUBLIC (empty grantee), never a fuzz-role oid.
    let sql = match g.rng.below(8) {
        0 => {
            // Full privilege alphabet round-trip through aclitemin/aclitemout
            // (aclparse consumes the '*' grant-option markers + grantor).
            let lit = if err {
                "postgres=Zzz/postgres"
            } else {
                pick_str(
                    g,
                    &[
                        "postgres=arwdDxtm/postgres",
                        "postgres=r*w*a*/postgres",
                        "=r/postgres",
                        "postgres=arwdDxtmURC/postgres",
                        "=arwdDxtm/postgres",
                    ],
                )
            };
            format!("SELECT aclitemin('{}')::text;", lit)
        }
        1 => {
            // acldefault for every object kind, expanded via aclexplode with
            // deterministic ordering (acldefault + aclmembers + aclexplode).
            let kind = pick_str(g, &["r", "c", "s", "f", "l", "L", "n", "t", "T", "d"]);
            format!(
                "SELECT a.grantee::regrole::text, a.privilege_type, a.is_grantable FROM aclexplode(acldefault('{}', 10)) a ORDER BY 1, 2, 3;",
                kind
            )
        }
        2 => {
            // makeaclitem multi-privilege + aclcontains membership probe.
            let p = if err {
                "BOGUS"
            } else {
                pick_str(g, &["SELECT", "SELECT, UPDATE", "INSERT, DELETE, TRUNCATE"])
            };
            let grantee = if g.rng.chance(1, 4) { 0 } else { 10 };
            format!(
                "SELECT makeaclitem({}, 10, '{}', {})::text;",
                grantee,
                p,
                if g.rng.chance(1, 2) { "true" } else { "false" }
            )
        }
        3 => {
            // aclmembers via the aclitemin/array path: build a 2-item acl and
            // check containment + equality (aclcontains, aclitem_eq).
            format!(
                "SELECT aclcontains(ARRAY[aclitemin('postgres=arwdDxtm/postgres'), aclitemin('=r/postgres')], aclitemin('=r/postgres'));"
            )
        }
        4 => {
            // acldefault ::text directly (acldefault + aclitemout array path).
            let kind = pick_str(g, &["r", "n", "s", "f"]);
            format!("SELECT acldefault('{}', 10)::text;", kind)
        }
        5 => {
            // hash_aclitem equality (deterministic; future-proof probe form).
            let p1 = pick_str(g, &["SELECT", "UPDATE", "DELETE"]);
            format!(
                "SELECT hash_aclitem(makeaclitem(10, 10, '{}', false)) = hash_aclitem(makeaclitem(10, 10, '{}', false));",
                p1, p1
            )
        }
        6 => {
            // aclitem comparison operators (aclitemComparator / aclitem_eq)
            // over a fixed pair.
            let op = pick_str(g, &["=", "<>"]);
            format!(
                "SELECT makeaclitem(10, 10, 'SELECT', false) {} makeaclitem(10, 10, 'UPDATE', false);",
                op
            )
        }
        _ => {
            // Bogus aclitem literal error fuel (aclparse error arms): a
            // malformed grant string. Balanced (no parens inside literal).
            let lit = if err {
                pick_str(g, &["postgres/postgres", "postgres=r", "=x/postgres"])
            } else {
                "postgres=w/postgres"
            };
            format!("SELECT aclitemin('{}')::text;", lit)
        }
    };
    raw(vec![sql])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FP-12 (round-10): every module-owned `fz_acl_` name — identifiers,
    /// quoted identifiers, aclitem string literals, catalog-probe string
    /// literals — moves into the batch-unique `{tag}_fz_acl_` namespace;
    /// everything else is untouched.
    #[test]
    fn rebase_role_names_rewrites_the_whole_module_namespace() {
        let t = "fuzz_mixed_123_1";
        assert_eq!(
            rebase_role_names("GRANT SELECT ON fz_acl_rls_d TO fz_acl_ud1, fz_acl_ud2;", t),
            "GRANT SELECT ON fuzz_mixed_123_1_fz_acl_rls_d \
             TO fuzz_mixed_123_1_fz_acl_ud1, fuzz_mixed_123_1_fz_acl_ud2;"
        );
        assert_eq!(
            rebase_role_names("DROP ROLE fz_acl_ud1;", t),
            "DROP ROLE fuzz_mixed_123_1_fz_acl_ud1;"
        );
        // Quoted identifiers and aclitem literals move too (they must stay
        // coherent with the CREATE ROLE they reference).
        assert_eq!(
            rebase_role_names("SELECT aclitemin('\"fz_acl_q1\"=r*w/postgres')::text;", t),
            "SELECT aclitemin('\"fuzz_mixed_123_1_fz_acl_q1\"=r*w/postgres')::text;"
        );
        assert_eq!(
            rebase_role_names(
                "SELECT policyname FROM pg_policies WHERE tablename = 'fz_acl_rls_d' ORDER BY policyname;",
                t
            ),
            "SELECT policyname FROM pg_policies \
             WHERE tablename = 'fuzz_mixed_123_1_fz_acl_rls_d' ORDER BY policyname;"
        );
        // Non-module statements pass through byte-identical.
        for sql in [
            "SELECT 1;",
            "CREATE ROLE other_role NOLOGIN;",
            "SELECT aclitemin('postgres=arwdDxtm/postgres')::text;",
        ] {
            assert_eq!(rebase_role_names(sql, t), sql);
        }
    }

    /// The longest module name under a helper_diffrun-shaped tag stays
    /// inside the 63-byte identifier bound (no silent truncation split
    /// between CREATE and later references).
    #[test]
    fn rebase_role_names_stays_under_identifier_bound() {
        // Worst realistic tag: fuzz_{label<=10}_{pid<=7}_{seq}.
        let t = "fuzz_downgrade_9999999_9999";
        let rebased = rebase_role_names("DROP TABLE IF EXISTS fz_acl_rls_d CASCADE;", t);
        for word in rebased.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
            assert!(word.len() < 64, "identifier over NAMEDATALEN: {word}");
        }
    }
}
