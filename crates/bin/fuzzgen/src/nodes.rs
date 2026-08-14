//! Node-serialization breadth module (Q3, sql-reachable-queue chunk
//! `nodes-serial`): self-contained utility-statement groups wrapped in
//! `SET debug_print_parse/debug_print_rewritten/debug_print_plan` brackets,
//! plus a `compute_query_id=on` weave.
//!
//! The mechanism: the debug_print GUCs make postgres dump the raw parse
//! tree / rewritten tree / plan tree through elog_node_display -> outfuncs
//! for EVERY statement parsed while they are on. The dump goes to the
//! SERVER LOG, not the client, so client-visible output is unchanged
//! (hand-verified on both engines before landing — see
//! docs/fuzzing/findings-q3-nodesobs.md) and the normal differ applies.
//! Coverage therefore multiplies with statement diversity: the bracket
//! bodies here are exactly the utility statements gap-report-009 showed as
//! generator gaps (_outAlterCollationStmt, _outCreateForeignServerStmt,
//! _outAlterRoleSetStmt, _outObjectWithArgs, _outLockStmt, ...), and the
//! `nodes:mix` shape wraps ordinary AST statements so the plan/rewritten
//! dumps ride the existing expression/aggregate/window variety too.
//! `compute_query_id=on` additionally routes every statement through the
//! queryjumblefuncs breadth (_jumble* of the same utility trees).
//!
//! Rules of the module:
//!   - every group is self-contained: it creates, exercises and DROPs its
//!     objects under fixed `ns_`-prefixed names, and every SET it opens is
//!     RESET before the group ends — no cross-group state, no collisions;
//!   - every statement was hand-verified byte-identical on C 18.3 and
//!     pgrust before landing; the one deliberate matched-error arm is
//!     `nodes:load` (LOAD of a nonexistent module errors identically);
//!   - ALTER ... DEPENDS ON EXTENSION is deliberately ABSENT: pgrust's
//!     grammar rejects it ("not yet implemented", opt_no) — banked as
//!     finding Q3-F1; the C-side coverage arm reaches it via the
//!     docs/fuzzing/deck-q3-nodes.sql post-SQL deck instead.
//!   - stored-view shapes (`nodes:storedview`) put SEARCH/CYCLE, GROUPING
//!     SETS, JSON_TABLE and FOR UPDATE bodies INSIDE view definitions so
//!     the later SELECT deserializes them from pg_rewrite (readfuncs:
//!     _readCTESearchClause/_readCTECycleClause/_readGroupingSet/
//!     _readJsonTablePath*/_readRowMarkClause).

use crate::agg::gen_agg_stmt;
use crate::stmt::{gen_expr_stmt, Gen, StmtKind};
use crate::win::gen_win_stmt;

const SHAPES: &[&str] = &[
    "nodes:collation",
    "nodes:fdw",
    "nodes:function",
    "nodes:atomic",
    "nodes:role",
    "nodes:operator",
    "nodes:schema",
    "nodes:stats",
    "nodes:system",
    "nodes:lock",
    "nodes:constraints",
    "nodes:returning",
    "nodes:load",
    "nodes:storedview",
    "nodes:mix",
    "nodes:prepared",
    "nodes:domain",
    "nodes:cluster",
    "nodes:do",
    "nodes:opfam",
    "nodes:objextra",
];

/// Lock modes for the `nodes:lock` bracket (full LockStmt mode surface).
const LOCK_MODES: &[&str] = &[
    "ACCESS SHARE",
    "ROW SHARE",
    "ROW EXCLUSIVE",
    "SHARE UPDATE EXCLUSIVE",
    "SHARE",
    "SHARE ROW EXCLUSIVE",
    "EXCLUSIVE",
    "ACCESS EXCLUSIVE",
];

/// ALTER SYSTEM value pool: (guc, value). Every entry is bracketed with an
/// ALTER SYSTEM RESET of the same GUC so postgresql.auto.conf never leaks
/// state past the group (the running config is untouched either way — no
/// pg_reload_conf here).
const ALTER_SYSTEM_GUCS: &[(&str, &str)] = &[
    ("autovacuum_naptime", "'90s'"),
    ("checkpoint_completion_target", "0.7"),
    ("log_min_duration_statement", "250"),
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// The debug-print bracket: SET a nonempty subset of the three debug_print
/// GUCs (plus compute_query_id half the time), and the matching RESETs.
/// Client output is unchanged by all four GUCs (hand-verified), so the
/// bracket is compare-transparent.
fn guc_bracket(g: &mut Gen) -> (Vec<StmtKind>, Vec<StmtKind>) {
    let mask = 1 + g.rng.below(7) as u32; // 1..=7: at least one GUC on
    let mut names: Vec<&'static str> = Vec::new();
    for (bit, name) in [
        (1u32, "debug_print_parse"),
        (2, "debug_print_rewritten"),
        (4, "debug_print_plan"),
    ] {
        if mask & bit != 0 {
            names.push(name);
        }
    }
    if g.rng.chance(1, 2) {
        g.fire("nodes:qid");
        names.push("compute_query_id");
    }
    // debug_pretty_print defaults ON; flipping it off routes the dump
    // through format_node_dump instead of pretty_format_node_dump.
    let mut sets: Vec<StmtKind> =
        names.iter().map(|n| raw(format!("SET {} TO on;", n))).collect();
    let mut resets: Vec<StmtKind> =
        names.iter().rev().map(|n| raw(format!("RESET {};", n))).collect();
    if g.rng.chance(1, 4) {
        g.fire("nodes:nopretty");
        sets.push(raw("SET debug_pretty_print TO off;"));
        resets.insert(0, raw("RESET debug_pretty_print;"));
    }
    (sets, resets)
}

fn body(g: &mut Gen, shape: &str) -> Vec<StmtKind> {
    match shape {
        "nodes:collation" => {
            let loc = if g.rng.chance(1, 2) { "C" } else { "POSIX" };
            vec![
                raw(format!("CREATE COLLATION ns_coll1 (locale = '{}');", loc)),
                raw("ALTER COLLATION ns_coll1 REFRESH VERSION;"),
                raw("ALTER COLLATION ns_coll1 RENAME TO ns_coll2;"),
                raw("DROP COLLATION ns_coll2;"),
            ]
        }
        "nodes:fdw" => vec![
            raw("CREATE FOREIGN DATA WRAPPER ns_fdw;"),
            raw("CREATE SERVER ns_srv FOREIGN DATA WRAPPER ns_fdw OPTIONS (host 'localhost', port '5432');"),
            raw("ALTER SERVER ns_srv OPTIONS (SET host 'h2', ADD dbname 'fuzz');"),
            raw("ALTER SERVER ns_srv VERSION '9.9';"),
            raw("CREATE USER MAPPING FOR CURRENT_USER SERVER ns_srv OPTIONS (user 'u1');"),
            raw("ALTER USER MAPPING FOR CURRENT_USER SERVER ns_srv OPTIONS (SET user 'u2');"),
            raw("DROP USER MAPPING FOR CURRENT_USER SERVER ns_srv;"),
            raw("DROP SERVER ns_srv;"),
            raw("DROP FOREIGN DATA WRAPPER ns_fdw;"),
        ],
        "nodes:function" => {
            let alter = if g.rng.chance(1, 2) {
                raw(format!("ALTER FUNCTION ns_f1(int) COST {};", 10 + g.rng.below(90)))
            } else {
                raw("ALTER FUNCTION ns_f1(int) STABLE PARALLEL SAFE;")
            };
            vec![
                raw("CREATE FUNCTION ns_f1(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1';"),
                alter,
                raw("ALTER FUNCTION ns_f1(int) RENAME TO ns_f2;"),
                raw("ALTER FUNCTION ns_f2(int) OWNER TO CURRENT_USER;"),
                raw("CREATE SCHEMA ns_fs;"),
                raw("ALTER FUNCTION ns_f2(int) SET SCHEMA ns_fs;"),
                raw("SELECT ns_fs.ns_f2(41);"),
                raw("DROP FUNCTION ns_fs.ns_f2(int);"),
                raw("DROP SCHEMA ns_fs;"),
            ]
        }
        "nodes:atomic" => {
            let n = 1 + g.rng.below(40);
            vec![
                raw(format!(
                    "CREATE FUNCTION ns_fa() RETURNS int LANGUAGE sql BEGIN ATOMIC RETURN {} + 1; END;",
                    n
                )),
                raw("SELECT ns_fa();"),
                raw("DROP FUNCTION ns_fa();"),
            ]
        }
        "nodes:role" => {
            let (guc, val) = if g.rng.chance(1, 2) {
                ("enable_seqscan", "off")
            } else {
                ("work_mem", "'4MB'")
            };
            vec![
                raw("CREATE ROLE ns_role1 LOGIN CONNECTION LIMIT 5;"),
                raw(format!("ALTER ROLE ns_role1 SET {} = {};", guc, val)),
                raw("ALTER ROLE ns_role1 IN DATABASE fuzz SET work_mem = '4MB';"),
                raw("ALTER ROLE ns_role1 RESET ALL;"),
                raw("ALTER ROLE ns_role1 IN DATABASE fuzz RESET ALL;"),
                raw("ALTER ROLE ns_role1 NOLOGIN NOSUPERUSER;"),
                raw("ALTER ROLE ns_role1 RENAME TO ns_role2;"),
                raw("DROP ROLE ns_role2;"),
            ]
        }
        "nodes:operator" => vec![
            raw("CREATE FUNCTION ns_opf(int, int) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT $1 < $2';"),
            raw("CREATE OPERATOR <<# (LEFTARG = int, RIGHTARG = int, FUNCTION = ns_opf);"),
            raw("ALTER OPERATOR <<# (int, int) SET (RESTRICT = eqsel, JOIN = eqjoinsel);"),
            raw("ALTER OPERATOR <<# (int, int) OWNER TO CURRENT_USER;"),
            raw("SELECT 3 <<# 4, 4 <<# 3;"),
            raw("DROP OPERATOR <<# (int, int);"),
            raw("DROP FUNCTION ns_opf(int, int);"),
        ],
        "nodes:schema" => vec![
            raw("CREATE SCHEMA ns_s1 CREATE TABLE ns_t1 (a int PRIMARY KEY) CREATE VIEW ns_v1 AS SELECT a FROM ns_t1;"),
            raw("ALTER TABLE ns_s1.ns_t1 RENAME TO ns_t2;"),
            raw("ALTER TABLE ns_s1.ns_t2 SET SCHEMA public;"),
            raw("ALTER TABLE public.ns_t2 OWNER TO CURRENT_USER;"),
            raw("ALTER VIEW ns_s1.ns_v1 RENAME TO ns_v2;"),
            raw("DROP VIEW ns_s1.ns_v2;"),
            raw("DROP TABLE public.ns_t2 CASCADE;"),
            raw("DROP SCHEMA ns_s1;"),
        ],
        "nodes:stats" => {
            let target = 10 + g.rng.below(90);
            vec![
                raw("CREATE TABLE ns_st (a int, b int);"),
                raw("INSERT INTO ns_st SELECT g, g % 7 FROM generate_series(1, 50) g;"),
                raw("CREATE STATISTICS ns_stat1 (dependencies, ndistinct) ON a, b FROM ns_st;"),
                raw(format!("ALTER STATISTICS ns_stat1 SET STATISTICS {};", target)),
                raw("ALTER STATISTICS ns_stat1 RENAME TO ns_stat2;"),
                raw("ANALYZE ns_st;"),
                raw("DROP TABLE ns_st CASCADE;"),
            ]
        }
        "nodes:system" => {
            let (guc, val) = ALTER_SYSTEM_GUCS[g.rng.below_usize(ALTER_SYSTEM_GUCS.len())];
            let mut out = vec![
                raw(format!("ALTER SYSTEM SET {} = {};", guc, val)),
                raw(format!("ALTER SYSTEM RESET {};", guc)),
            ];
            if g.rng.chance(1, 3) {
                out.push(raw("ALTER SYSTEM RESET ALL;"));
            }
            out
        }
        "nodes:lock" => {
            let t = g.pick_table().name.clone();
            let mut out = vec![raw("BEGIN;")];
            let n = 1 + g.rng.below(3);
            for _ in 0..n {
                let mode = LOCK_MODES[g.rng.below_usize(LOCK_MODES.len())];
                // Single-session rig: NOWAIT can never fail, so it is safe
                // to weave in (parses to LockStmt.nowait).
                let nowait = if g.rng.chance(1, 3) { " NOWAIT" } else { "" };
                out.push(raw(format!("LOCK TABLE {} IN {} MODE{};", t, mode, nowait)));
            }
            out.push(raw("COMMIT;"));
            out
        }
        "nodes:constraints" => vec![
            raw("CREATE TABLE ns_cc (a int UNIQUE DEFERRABLE INITIALLY DEFERRED, b int);"),
            raw("BEGIN;"),
            raw("SET CONSTRAINTS ALL DEFERRED;"),
            raw("INSERT INTO ns_cc VALUES (1, 1), (2, 2);"),
            raw("SET CONSTRAINTS ALL IMMEDIATE;"),
            raw("COMMIT;"),
            raw("DROP TABLE ns_cc;"),
        ],
        "nodes:returning" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ns_rt (a int PRIMARY KEY, b int);"),
                raw("INSERT INTO ns_rt SELECT g, g * 10 FROM generate_series(1, 3) g;"),
                raw(format!(
                    "UPDATE ns_rt SET b = b + 1 WHERE a = {} RETURNING WITH (OLD AS o, NEW AS n) o.a, o.b, n.b;",
                    k
                )),
                raw("DELETE FROM ns_rt RETURNING WITH (OLD AS o) o.a, o.b;"),
                raw("DROP TABLE ns_rt;"),
            ]
        }
        // Matched-error arm: LOAD of a nonexistent module raises the
        // identical 58P01 on both engines (hand-verified).
        "nodes:load" => vec![raw("LOAD 'ns_no_such_module';")],
        "nodes:storedview" => {
            let arm = g.weights.pick(
                g.rng,
                &[
                    "nodes:storedview:search",
                    "nodes:storedview:cycle",
                    "nodes:storedview:gsets",
                    "nodes:storedview:jsontable",
                    "nodes:storedview:forupdate",
                ],
            );
            g.fire(arm);
            let mut out = vec![
                raw("CREATE TABLE ns_vt (id int PRIMARY KEY, parent int);"),
                raw("INSERT INTO ns_vt VALUES (1, NULL), (2, 1), (3, 1), (4, 2);"),
            ];
            let (mkview, probe) = match arm {
                "nodes:storedview:search" => (
                    "CREATE VIEW ns_vv AS WITH RECURSIVE r AS (SELECT id, parent FROM ns_vt WHERE parent IS NULL UNION ALL SELECT t.id, t.parent FROM ns_vt t JOIN r ON t.parent = r.id) SEARCH DEPTH FIRST BY id SET ord SELECT id FROM r;",
                    "SELECT * FROM ns_vv ORDER BY id;",
                ),
                "nodes:storedview:cycle" => (
                    "CREATE VIEW ns_vv AS WITH RECURSIVE r AS (SELECT id, parent FROM ns_vt WHERE parent IS NULL UNION ALL SELECT t.id, t.parent FROM ns_vt t JOIN r ON t.parent = r.id) CYCLE id SET is_cycle USING path SELECT id FROM r;",
                    "SELECT * FROM ns_vv ORDER BY id;",
                ),
                "nodes:storedview:gsets" => (
                    "CREATE VIEW ns_vv AS SELECT parent, count(*) AS n FROM ns_vt GROUP BY GROUPING SETS ((parent), ());",
                    "SELECT * FROM ns_vv ORDER BY parent NULLS LAST, n;",
                ),
                "nodes:storedview:jsontable" => (
                    "CREATE VIEW ns_vv AS SELECT jt.* FROM JSON_TABLE('[{\"a\":1},{\"a\":2}]', '$[*]' COLUMNS (a int PATH '$.a')) jt;",
                    "SELECT * FROM ns_vv ORDER BY a;",
                ),
                _ => (
                    "CREATE VIEW ns_vv AS SELECT id FROM ns_vt FOR UPDATE;",
                    "SELECT * FROM ns_vv ORDER BY id;",
                ),
            };
            out.push(raw(mkview));
            if matches!(arm, "nodes:storedview:search" | "nodes:storedview:cycle") {
                // LD1-F1 (FIXED, PR fix-searchcycle-deparse): deparsing a
                // recursive-CTE SEARCH/CYCLE view whose recursive term JOINs
                // over the self-reference used to crash the backend (parser
                // dropped the hidden ordering/mark/path columns from the join
                // RTE, overrunning ruleutils colinfo). Now byte-identical to C,
                // so the viewdef sweep is un-skipped for these arms.
                out.push(raw("SELECT pg_get_viewdef('ns_vv'::regclass);"));
                out.push(raw("SELECT pg_get_viewdef('ns_vv'::regclass, true);"));
            }
            out.push(raw(probe));
            out.push(raw("DROP VIEW ns_vv;"));
            out.push(raw("DROP TABLE ns_vt;"));
            out
        }
        // PREPARE raw-wrapper arms: debug_print dumps the ANALYZED Query,
        // so raw grammar nodes (MergeStmt, OnConflictClause, WindowDef,
        // JSON constructors, MultiAssignRef, B'..' literals, grouping
        // sets) reach outfuncs only while embedded raw inside a utility
        // statement — PrepareStmt keeps its inner statement raw through
        // parse analysis. compute_query_id jumbles the same raw trees.
        "nodes:prepared" => {
            let arm = g.weights.pick(
                g.rng,
                &[
                    "nodes:prepared:merge",
                    "nodes:prepared:onconflict",
                    "nodes:prepared:multiassign",
                    "nodes:prepared:window",
                    "nodes:prepared:json",
                    "nodes:prepared:gsets",
                ],
            );
            g.fire(arm);
            let prep = match arm {
                "nodes:prepared:merge" => "PREPARE ns_pq AS MERGE INTO ns_pt t USING (VALUES (1, 5), (9, 9)) v(a, b) ON t.a = v.a WHEN MATCHED THEN UPDATE SET b = v.b WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (v.a, v.b, 'm');",
                "nodes:prepared:onconflict" => "PREPARE ns_pq AS INSERT INTO ns_pt (a, b) VALUES (3, 3) ON CONFLICT (a) DO UPDATE SET b = excluded.b + 1;",
                "nodes:prepared:multiassign" => "PREPARE ns_pq AS UPDATE ns_pt SET (b, c) = (SELECT 7, 'z') WHERE a = 1;",
                "nodes:prepared:window" => "PREPARE ns_pq AS SELECT count(*) OVER w, sum(b) OVER w, B'1011' FROM ns_pt WINDOW w AS (PARTITION BY c ORDER BY a);",
                "nodes:prepared:json" => "PREPARE ns_pq AS SELECT json_objectagg(a: b), json_arrayagg(a ORDER BY a), json_scalar(count(*)), json_serialize('{\"k\":1}'), json('{\"j\":2}') FROM ns_pt;",
                _ => "PREPARE ns_pq AS SELECT b, grouping(b) FROM ns_pt GROUP BY GROUPING SETS ((b), ()) ORDER BY b NULLS LAST, grouping(b);",
            };
            vec![
                raw("CREATE TABLE ns_pt (a int PRIMARY KEY, b int, c text);"),
                raw("INSERT INTO ns_pt VALUES (1, 1, 'x'), (2, 2, 'y');"),
                raw(prep),
                raw("EXECUTE ns_pq;"),
                raw("DEALLOCATE ns_pq;"),
                raw("DROP TABLE ns_pt;"),
            ]
        }
        "nodes:domain" => vec![
            raw("CREATE DOMAIN ns_dom AS int CHECK (VALUE > 0);"),
            raw("ALTER DOMAIN ns_dom SET NOT NULL;"),
            raw("ALTER DOMAIN ns_dom DROP NOT NULL;"),
            raw("ALTER DOMAIN ns_dom ADD CONSTRAINT ns_dom_c CHECK (VALUE < 1000);"),
            raw("ALTER DOMAIN ns_dom DROP CONSTRAINT ns_dom_c;"),
            raw("SELECT 5::ns_dom;"),
            raw("DROP DOMAIN ns_dom;"),
            raw("CREATE TYPE ns_enum AS ENUM ('a', 'b');"),
            raw("ALTER TYPE ns_enum ADD VALUE 'c' AFTER 'b';"),
            raw("ALTER TYPE ns_enum RENAME VALUE 'c' TO 'd';"),
            raw("SELECT 'd'::ns_enum;"),
            raw("DROP TYPE ns_enum;"),
        ],
        "nodes:cluster" => vec![
            raw("CREATE TABLE ns_cl (a int PRIMARY KEY, b int);"),
            raw("INSERT INTO ns_cl SELECT g, g % 5 FROM generate_series(1, 30) g;"),
            raw("CLUSTER ns_cl USING ns_cl_pkey;"),
            raw("CLUSTER ns_cl;"),
            raw("SELECT count(*) FROM ns_cl;"),
            raw("DROP TABLE ns_cl;"),
        ],
        "nodes:do" => vec![
            raw("DO 'begin perform 1; end';"),
            raw("CREATE TABLE ns_pt (a int PRIMARY KEY, b int, c text);"),
            raw("CREATE TABLE ns_like (LIKE ns_pt INCLUDING DEFAULTS INCLUDING CONSTRAINTS);"),
            raw("DROP TABLE ns_like;"),
            raw("DROP TABLE ns_pt;"),
        ],
        // Q6: CreateOpClassStmt / CreateOpFamilyStmt / AlterOpFamilyStmt
        // through the outfuncs/queryjumble/copyfuncs breadth (statements
        // hand-verified on both engines, q6-hv1 deck; self-contained).
        "nodes:opfam" => vec![
            raw("CREATE OPERATOR FAMILY ns_opf USING btree;"),
            raw(
                "CREATE OPERATOR CLASS ns_opc FOR TYPE int4 USING btree FAMILY ns_opf AS \
                 OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >, \
                 FUNCTION 1 btint4cmp(int4, int4);",
            ),
            raw(
                "ALTER OPERATOR FAMILY ns_opf USING btree ADD OPERATOR 1 < (int4, int8), \
                 FUNCTION 1 btint48cmp(int4, int8);",
            ),
            raw(
                "ALTER OPERATOR FAMILY ns_opf USING btree DROP OPERATOR 1 (int4, int8), \
                 FUNCTION 1 (int4, int8);",
            ),
            raw("DROP OPERATOR CLASS ns_opc USING btree;"),
            raw("DROP OPERATOR FAMILY ns_opf USING btree;"),
        ],
        // Q6: CreateAmStmt / CreateConversionStmt / CreateCastStmt /
        // CreatePLangStmt / CreateTransformStmt / SecLabelStmt raw trees
        // under the debug_print/compute_query_id brackets (the transform
        // and seclabel arms are deliberate matched errors, verified).
        "nodes:objextra" => {
            let arm = g.weights.pick(
                g.rng,
                &[
                    "nodes:objx:am",
                    "nodes:objx:conv",
                    "nodes:objx:cast",
                    "nodes:objx:plang",
                    "nodes:objx:seclabel",
                    "nodes:objx:xform",
                ],
            );
            g.fire(arm);
            match arm {
                "nodes:objx:am" => vec![
                    raw("CREATE ACCESS METHOD ns_am TYPE INDEX HANDLER gisthandler;"),
                    raw("COMMENT ON ACCESS METHOD ns_am IS 'ns am';"),
                    raw("DROP ACCESS METHOD ns_am;"),
                ],
                "nodes:objx:conv" => vec![
                    raw("CREATE CONVERSION ns_conv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;"),
                    raw("DROP CONVERSION ns_conv;"),
                ],
                "nodes:objx:cast" => vec![
                    raw("CREATE TYPE ns_mood AS ENUM ('sad', 'ok');"),
                    raw("CREATE CAST (ns_mood AS text) WITH INOUT;"),
                    raw("SELECT ('ok'::ns_mood)::text;"),
                    raw("DROP CAST (ns_mood AS text);"),
                    raw("DROP TYPE ns_mood;"),
                ],
                "nodes:objx:plang" => vec![
                    raw("CREATE TRUSTED LANGUAGE ns_lang HANDLER plpgsql_call_handler;"),
                    raw("DROP LANGUAGE ns_lang;"),
                ],
                "nodes:objx:seclabel" => {
                    vec![raw("SECURITY LABEL ON TYPE int4 IS 'ns label';")]
                }
                _ => vec![raw(
                    "CREATE TRANSFORM FOR int4 LANGUAGE plpgsql (FROM SQL WITH FUNCTION int4pl(int4, int4));",
                )],
            }
        }
        "nodes:mix" => {
            // Plan/rewritten dumps over the ordinary AST variety: 1-3
            // expression/aggregate/window statements inside the bracket.
            let n = 1 + g.rng.below(3);
            let mut out = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let s = match g.rng.below(3) {
                    0 => gen_expr_stmt(g),
                    1 => gen_agg_stmt(g),
                    _ => gen_win_stmt(g),
                };
                out.push(StmtKind::Select(Box::new(s)));
            }
            out
        }
        other => unreachable!("unknown nodes shape {other}"),
    }
}

/// Registry entry point (stmt::STMT_MODULES): one debug-print bracket group.
pub fn gen_nodes_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("nodes");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    let (sets, resets) = guc_bracket(g);
    let mut out = sets;
    out.extend(body(g, shape));
    out.extend(resets);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, w: &WeightTable) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            groups.push(
                gen_nodes_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>(),
            );
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    /// Every group: opens with >=1 debug-GUC SET, closes with the matching
    /// RESETs (LIFO), transaction brackets are balanced, and every object
    /// the body creates is dropped before the group ends.
    #[test]
    fn brackets_are_self_contained() {
        let (groups, prods) = gen_groups(0x0135, 800, &WeightTable::defaults());
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
            }
            // SET/RESET pairing over the group.
            let sets: Vec<&str> = group
                .iter()
                .filter_map(|s| s.strip_prefix("SET ").and_then(|r| r.split(' ').next()))
                // SET CONSTRAINTS is a constraint-mode statement inside the
                // nodes:constraints body, not a GUC bracket open.
                .filter(|n| *n != "CONSTRAINTS")
                .collect();
            let resets: Vec<&str> = group
                .iter()
                .filter_map(|s| s.strip_prefix("RESET ").map(|r| r.trim_end_matches(';')))
                .filter(|n| *n != "ALL")
                .collect();
            assert!(!sets.is_empty(), "no GUC bracket: {group:?}");
            let mut rev: Vec<&str> = resets.clone();
            rev.reverse();
            assert_eq!(sets, rev, "SET/RESET not LIFO-paired: {group:?}");
            for name in &sets {
                assert!(
                    [
                        "debug_print_parse",
                        "debug_print_rewritten",
                        "debug_print_plan",
                        "compute_query_id",
                        "debug_pretty_print"
                    ]
                    .contains(name),
                    "unexpected GUC {name} in nodes bracket"
                );
            }
            assert!(
                sets.iter().any(|n| n.starts_with("debug_print_")),
                "bracket without any debug_print GUC: {group:?}"
            );
            // BEGIN/COMMIT balance.
            let opens = group.iter().filter(|s| *s == "BEGIN;").count();
            let closes = group.iter().filter(|s| *s == "COMMIT;").count();
            assert_eq!(opens, closes, "unbalanced txn bracket: {group:?}");
            // Self-containment: creates and drops balance per object kind.
            for (create, drop) in [
                ("CREATE TABLE", "DROP TABLE"),
                ("CREATE ROLE", "DROP ROLE"),
                ("CREATE COLLATION", "DROP COLLATION"),
                ("CREATE SERVER", "DROP SERVER"),
                ("CREATE FOREIGN DATA WRAPPER", "DROP FOREIGN DATA WRAPPER"),
                ("CREATE USER MAPPING", "DROP USER MAPPING"),
                ("CREATE OPERATOR ", "DROP OPERATOR "),
            ] {
                // contains() on the create side: CREATE SCHEMA carries its
                // CREATE TABLE / CREATE VIEW subcommands inline.
                let c = group.iter().filter(|s| s.contains(create)).count();
                let d = group.iter().filter(|s| s.starts_with(drop)).count();
                assert_eq!(c, d, "{create} without {drop}: {group:?}");
            }
            // Functions/schemas/views are renamed/moved before drop, so
            // count statements, not names.
            let fc = group.iter().filter(|s| s.starts_with("CREATE FUNCTION")).count();
            let fd = group.iter().filter(|s| s.starts_with("DROP FUNCTION")).count();
            assert_eq!(fc, fd, "CREATE FUNCTION without DROP: {group:?}");
            // ALTER SYSTEM SET is always paired with a RESET of the same GUC.
            for sql in group {
                if let Some(rest) = sql.strip_prefix("ALTER SYSTEM SET ") {
                    let guc = rest.split(' ').next().unwrap();
                    assert!(
                        group.iter().any(|s| *s == format!("ALTER SYSTEM RESET {};", guc)),
                        "ALTER SYSTEM SET {guc} not reset: {group:?}"
                    );
                }
            }
            // The banked-unsupported construct never leaks into the stream.
            for sql in group {
                assert!(!sql.contains("DEPENDS ON EXTENSION"), "{sql}");
            }
        }
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        for p in [
            "nodes",
            "nodes:qid",
            "nodes:nopretty",
            "nodes:prepared:merge",
            "nodes:prepared:onconflict",
            "nodes:prepared:multiassign",
            "nodes:prepared:window",
            "nodes:prepared:json",
            "nodes:prepared:gsets",
            "nodes:storedview:search",
            "nodes:storedview:cycle",
            "nodes:storedview:gsets",
            "nodes:storedview:jsontable",
            "nodes:storedview:forupdate",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    #[test]
    fn nodes_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(5, 120, &w);
        let (b, _) = gen_groups(5, 120, &w);
        assert_eq!(a, b);
        let (c, _) = gen_groups(6, 120, &w);
        assert_ne!(a, c);
    }
}
