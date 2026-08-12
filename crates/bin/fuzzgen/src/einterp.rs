//! Expression-interpreter + parse-tree-walker drain module (LD2, line-drain
//! queue chunks `execexpr-steps` + `nodefuncs-walkers`).
//!
//! Two mechanisms, one module:
//!
//! 1. RAW-WALKER shapes (`einterp:raw:*`): `raw_expression_tree_walker_impl`
//!    has exactly one production entry family — parse_cte.c
//!    (makeDependencyGraphWalker / checkWellFormedRecursionWalker), which
//!    runs ONLY when a statement carries `WITH RECURSIVE`, and then walks
//!    every CTE body (and, for nested WITH statements found during the
//!    walk, the whole inner statement). The walk happens at
//!    transformWithClause time, BEFORE the CTE bodies are analyzed. So the
//!    drain recipe is: put every rare raw parse-node kind inside a CTE body
//!    under a `WITH RECURSIVE` clause (the RECURSIVE keyword is what arms
//!    the walker; actual self-reference is not required). Data-modifying
//!    CTEs cover the InsertStmt/UpdateStmt/DeleteStmt statement cases.
//!    (The `debug_raw_expression_coverage_test` GUC that walks every DML
//!    statement exists only under DEBUG_NODE_TESTS_ENABLED, i.e. cassert
//!    builds — not our coverage build, and not production surface.)
//!
//! 2. INTERPRETER shapes (`einterp:op:*`): every `EEOP_*` opcode arm of
//!    `ExecInterpExpr` (and the ExecEval* out-of-line siblings) that the
//!    corpus leaves dark. Operands are always table/VALUES columns, never
//!    bare literals, so eval_const_expressions cannot fold the expression
//!    away before it reaches the interpreter.
//!
//! The same statements also drain the analyzed-tree siblings
//! (`expression_tree_walker_impl` / `expression_tree_mutator_impl` /
//! `exprType`/`exprTypmod`/`exprCollation`/`exprSetCollation`): the rare
//! node kinds now EXIST in analyzed queries, so every planner/rewriter walk
//! (eval_const_expressions, view rewrite, setrefs) visits their cases; the
//! view shapes route them through the rewriter and ruleutils deliberately.
//!
//! Rules of the module (same discipline as crate::nodes):
//!   - every group is self-contained: objects live under fixed `ei_`
//!     prefixed names, created and dropped inside the group; every SET is
//!     bracket-RESET;
//!   - statements are valid on BOTH engines (hand-verified byte-identical
//!     before landing; see docs/fuzzing/findings-ld2.md). Shapes that C
//!     accepts but pgrust does not are NOT here — they live in the C-side
//!     coverage deck docs/fuzzing/deck-ld2-cov.sql (each with a banked
//!     finding);
//!   - time-of-day SQLValueFunctions (current_time & co) are never
//!     compared directly — only via pg_typeof / order-insensitive
//!     predicates that are stable across two servers started at different
//!     instants.

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "einterp:raw:dml",
    "einterp:raw:select",
    "einterp:raw:from",
    "einterp:raw:window",
    "einterp:raw:sublink",
    "einterp:raw:sqljson",
    "einterp:op:distinct",
    "einterp:op:rowcmp",
    "einterp:op:booltest",
    "einterp:op:sysvar",
    "einterp:op:oldnew",
    "einterp:op:fieldstore",
    "einterp:op:sbsref",
    "einterp:op:domain",
    "einterp:op:iocoerce",
    "einterp:op:aggvariants",
    "einterp:op:aggsorted",
    "einterp:op:nextval",
    "einterp:op:currentof",
    "einterp:op:hashsaop",
    "einterp:op:wholerow",
    "einterp:op:svf",
    "einterp:op:casetest",
    "einterp:op:mergeaction",
    "einterp:op:paramset",
    "einterp:op:runcond",
    "einterp:op:cycle",
    "einterp:op:fusage",
    "einterp:op:aggparallel",
    "einterp:op:retview",
    "einterp:op:partbound",
    "einterp:op:viewwalk",
    // W4-WALK lane additions (line-drain-queue residue LD2 left; every
    // statement hand-verified byte-identical on both engines via
    // scripts/deck-target-e2e.sh diff over docs/fuzzing/deck-w4walk.sql):
    "einterp:w4:typmod",
    "einterp:w4:hazard",
    "einterp:w4:wholerow",
    "einterp:w4:jsonret",
    "einterp:w4:plassign",
    "einterp:w4:ruledrv",
    "einterp:w4:saop",
    "einterp:w4:arraymd",
    "einterp:w4:errloc",
    "einterp:w4:collate",
    "einterp:w4:partition",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// A tiny deterministic VALUES relation with NULLs in every column:
/// (a int, b text, c bool). Used as the non-foldable operand source for
/// scalar opcode shapes.
fn vals(g: &mut Gen) -> String {
    let k = 1 + g.rng.below(7);
    format!(
        "(VALUES ({k}, 'v{k}', true), ({}, NULL, false), (NULL, 'w', NULL)) v(a, b, c)",
        k + 10
    )
}

fn body(g: &mut Gen, shape: &str) -> Vec<StmtKind> {
    match shape {
        // ---------------------------------------------------- raw walker ---
        // InsertStmt / UpdateStmt / DeleteStmt / OnConflictClause /
        // InferClause / IndexElem / ReturningClause / MultiAssignRef /
        // ResTarget-with-indirection as data-modifying CTEs under WITH
        // RECURSIVE (plus nested WITH inside the insert body for the
        // WalkInnerWith non-recursive path).
        "einterp:raw:dml" => {
            let k = 1 + g.rng.below(5);
            vec![
                raw("CREATE TABLE ei_d (a int PRIMARY KEY, b text, c int[]);"),
                raw("CREATE UNIQUE INDEX ei_d_expr ON ei_d (((a % 100) + 0));"),
                raw(format!(
                    "INSERT INTO ei_d SELECT g, 'r' || g, ARRAY[g, g + 1] FROM generate_series(1, {}) g;",
                    4 + k
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), ins AS (WITH src AS (SELECT s FROM ws) INSERT INTO ei_d (a, b) SELECT s, 'ins' FROM src ON CONFLICT (a) DO UPDATE SET b = excluded.b || '!' WHERE ei_d.c IS NOT DISTINCT FROM excluded.c RETURNING WITH (OLD AS o, NEW AS n) n.a, o.b, n.b) SELECT count(*) FROM ins, ws;"
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), ins2 AS (INSERT INTO ei_d (a) VALUES ((SELECT max(s) FROM ws) + 100) ON CONFLICT (((a % 100) + 0)) DO NOTHING RETURNING a) SELECT count(*) FROM ins2;"
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), upd AS (UPDATE ei_d SET (b, c) = (SELECT 'multi', ARRAY[9]), a = a + 0 WHERE a = (SELECT s FROM ws) RETURNING a, b, c) SELECT count(*) FROM upd;"
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), upd2 AS (UPDATE ei_d SET (b, c) = ROW('rowsrc', ARRAY[1, 2]) WHERE a = (SELECT s + 1 FROM ws) RETURNING old.b AS ob, new.b AS nb) SELECT count(*) FROM upd2;"
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), upd3 AS (UPDATE ei_d SET b = DEFAULT, c[1] = 7 WHERE a = (SELECT s + 2 FROM ws) RETURNING a, b, c) SELECT count(*) FROM upd3;"
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), del AS (DELETE FROM ei_d USING ws WHERE ei_d.a = ws.s + 2 RETURNING ei_d.*) SELECT count(*) FROM del;"
                )),
                // MERGE as a data-modifying CTE (legal since PG17): the
                // MergeStmt / MergeWhenClause / merge RETURNING raw-walk
                // cases, all three WHEN kinds + NOT MATCHED BY SOURCE.
                raw("CREATE TABLE ei_dm (a int, b int);"),
                raw(format!(
                    "INSERT INTO ei_dm SELECT g, g * {k} FROM generate_series(3, 12) g;"
                )),
                raw(format!(
                    "WITH RECURSIVE ws AS (SELECT {k} AS s), m AS (MERGE INTO ei_d t USING ei_dm s ON t.a = s.a WHEN MATCHED AND s.b > 20 THEN DELETE WHEN MATCHED THEN UPDATE SET b = t.b || '+' || s.b WHEN NOT MATCHED AND s.a < 11 THEN INSERT (a, b) VALUES (s.a, 'merged') WHEN NOT MATCHED THEN DO NOTHING WHEN NOT MATCHED BY SOURCE AND t.a > 900 THEN DELETE RETURNING merge_action(), t.a, s.a AS sa) SELECT count(*) FROM m, ws;"
                )),
                // SELECT INTO inside a CTE: the raw walk of IntoClause runs
                // at transformWithClause time, before parse analysis raises
                // the identical "SELECT ... INTO is not allowed here" on
                // both engines (matched-error arm, hand-verified).
                raw("WITH RECURSIVE p AS (SELECT 1 AS x INTO ei_nope) SELECT 1;"),
                raw("DROP TABLE ei_dm;"),
                raw("DROP TABLE ei_d;"),
            ]
        }
        // SelectStmt breadth inside WITH RECURSIVE bodies: set-ops, DISTINCT
        // ON, SortBy USING, grouping sets, FETCH WITH TIES, CASE-with-arg,
        // COALESCE/GREATEST/LEAST/NULLIF, ARRAY/ROW, A_Indirection,
        // A_Indices slices, TypeCast, CollateClause, BooleanTest,
        // NamedArgExpr, GroupingFunc, MinMaxExpr, LockingClause.
        "einterp:raw:select" => {
            let n = 1 + g.rng.below(4);
            vec![
                raw("CREATE FUNCTION ei_nf(x int, y int DEFAULT 3) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT x * 10 + y';"),
                raw("CREATE TABLE ei_s (a int, b text COLLATE \"C\", arr int[]);"),
                raw(format!(
                    "INSERT INTO ei_s SELECT g, chr(96 + (g % 26)), ARRAY[g, g + 1, g + 2] FROM generate_series(1, {}) g;",
                    8 + n
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT DISTINCT ON (s.a % 3) s.a, CASE s.a % 3 WHEN 0 THEN 'z' WHEN 1 THEN 'o' ELSE 'm' END AS cw, GREATEST(s.a, {n}) AS gr, LEAST(s.a, {n}) AS ls, COALESCE(NULLIF(s.b, 'a'), 'fb') AS co, ARRAY[s.a, {n}] AS ar, ROW(s.a, s.b) AS rw, (s.arr)[2] AS el, (s.arr)[1:2] AS sl, s.a::bigint AS tc, s.b COLLATE \"POSIX\" AS cl, (s.a > {n}) IS NOT FALSE AS bt, ei_nf(y => s.a, x => 2) AS na FROM ei_s s ORDER BY s.a % 3, s.a USING < NULLS FIRST) SELECT count(*) FROM p;"
                )),
                raw(format!(
                    "WITH RECURSIVE p AS ((SELECT a FROM ei_s WHERE a <= {n}) UNION (SELECT a + 1 FROM ei_s) INTERSECT ALL (SELECT a FROM ei_s) EXCEPT (SELECT a * 100 FROM ei_s)), pl AS (SELECT s.a FROM ei_s s WHERE s.a <= {n} FOR UPDATE OF s SKIP LOCKED) SELECT count(*) FROM p, pl;"
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT s.a % 2 AS m2, s.a % 3 AS m3, GROUPING(s.a % 2, s.a % 3) AS gg, count(*) AS ct FROM ei_s s GROUP BY GROUPING SETS ((s.a % 2), CUBE (s.a % 3), ROLLUP ((s.a % 2), (s.a % 3)), ()) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3, 4 OFFSET {n} ROWS FETCH FIRST 30 ROWS WITH TIES) SELECT count(*) FROM p;"
                )),
                raw("DROP TABLE ei_s;"),
                raw("DROP FUNCTION ei_nf(int, int);"),
            ]
        }
        // FROM-item breadth in CTE bodies: LATERAL RangeSubselect,
        // RangeFunction (ROWS FROM + coldeflist + ORDINALITY),
        // RangeTableSample REPEATABLE, JoinExpr variants (NATURAL / USING
        // alias / FULL / CROSS). XMLTABLE and the XmlExpr family live in
        // the C-side deck (deck-ld2-cov.sql): pgrust diverges from a
        // --without-libxml C build on the whole xml surface (findings
        // LD2-F1..F3).
        "einterp:raw:from" => {
            let pct = 40 + g.rng.below(50);
            vec![
                raw("CREATE TABLE ei_f (a int, b text);"),
                raw("INSERT INTO ei_f SELECT g, 'f' || g FROM generate_series(1, 40) g;"),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT f.a, l.x, r.n, r.v FROM ei_f f, LATERAL (SELECT f.a * 2 AS x) l, ROWS FROM (generate_series(1, 2), jsonb_to_recordset('[{{\"v\": 10, \"w\": \"a\"}}, {{\"v\": 20}}]'::jsonb) AS (v int, w text)) WITH ORDINALITY AS r(g, v, w, n) WHERE f.a <= {}) SELECT count(*) FROM p;",
                    2 + g.rng.below(4)
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT count(*) AS c FROM ei_f TABLESAMPLE BERNOULLI ({pct}) REPEATABLE (7)) SELECT c >= 0 FROM p;"
                )),
                raw("WITH RECURSIVE p AS (SELECT ju.a, f2.b FROM ei_f AS f1 FULL JOIN ei_f AS f2 USING (a, b) AS ju NATURAL LEFT JOIN (SELECT 1 AS a, 'f1' AS b) sj CROSS JOIN (VALUES (0)) cj(z)) SELECT count(*) FROM p;"),
                raw("DROP TABLE ei_f;"),
            ]
        }
        // WindowDef breadth: named windows + refname inheritance, RANGE /
        // ROWS / GROUPS frames with offsets, every EXCLUDE variant.
        "einterp:raw:window" => {
            let off = 1 + g.rng.below(3);
            let excl = *g.rng.pick(&["EXCLUDE CURRENT ROW", "EXCLUDE GROUP", "EXCLUDE TIES", "EXCLUDE NO OTHERS"]);
            vec![
                raw("CREATE TABLE ei_w (a int, b int);"),
                raw("INSERT INTO ei_w SELECT g % 4, g FROM generate_series(1, 24) g;"),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT sum(w.b) OVER w2 AS s2, rank() OVER w1 AS rk, count(*) OVER (PARTITION BY w.a ROWS BETWEEN {off} PRECEDING AND {off} FOLLOWING {excl}) AS cr, ntile(3) OVER (ORDER BY w.b GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS nt, lag(w.b, 1, 0) OVER w2 AS lg FROM ei_w w WINDOW w1 AS (PARTITION BY w.a), w2 AS (w1 ORDER BY w.b RANGE BETWEEN {off} PRECEDING AND UNBOUNDED FOLLOWING)) SELECT count(*) FROM p;"
                )),
                raw("DROP TABLE ei_w;"),
            ]
        }
        // SubLink kinds under the raw walker + EEOP_SUBPLAN /
        // EEOP_PARAM_EXEC at execution: EXISTS / ANY / ALL / scalar /
        // ARRAY / row-comparison sublinks, correlated and not.
        "einterp:raw:sublink" => {
            let n = 2 + g.rng.below(5);
            vec![
                raw("CREATE TABLE ei_sub (a int, b text);"),
                raw(format!(
                    "INSERT INTO ei_sub SELECT g, 's' || g FROM generate_series(1, {}) g;",
                    6 + n
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT s.a FROM ei_sub s WHERE EXISTS (SELECT 1 FROM ei_sub i WHERE i.a = s.a + 1) AND s.a = ANY (SELECT a FROM ei_sub WHERE a < {n} + 4) AND s.a <= ALL (SELECT a + 20 FROM ei_sub) AND ROW(s.a, s.b) <> (SELECT a, b FROM ei_sub ORDER BY a LIMIT 1) AND s.a IN (SELECT a FROM ei_sub WHERE b LIKE 's%')) SELECT count(*), (SELECT min(a) FROM ei_sub), ARRAY(SELECT a FROM ei_sub ORDER BY a DESC LIMIT 3) FROM p;"
                )),
                raw("DROP TABLE ei_sub;"),
            ]
        }
        // SQL/JSON constructor + query raw nodes (JsonObjectConstructor,
        // JsonArrayConstructor, JsonArrayQueryConstructor, JsonObjectAgg,
        // JsonArrayAgg, JsonAggConstructor, JsonKeyValue, JsonOutput,
        // JsonParseExpr, JsonScalarExpr, JsonSerializeExpr,
        // JsonIsPredicate, JsonFuncExpr, JsonBehavior, JsonArgument,
        // JsonTablePathSpec, JsonTable, JsonTableColumn) — plus their
        // executor arms (EEOP_JSON_CONSTRUCTOR, EEOP_IS_JSON,
        // EEOP_JSONEXPR_PATH, EEOP_JSONEXPR_COERCION[_FINISH]).
        "einterp:raw:sqljson" => {
            let n = 1 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_j (a int, doc jsonb);"),
                raw(format!(
                    "INSERT INTO ei_j SELECT g, jsonb_build_object('k', g, 'arr', jsonb_build_array(g, g + 1), 'nest', jsonb_build_object('deep', g * 2)) FROM generate_series(1, {}) g;",
                    3 + n
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT JSON_OBJECT('a' VALUE j.a, 'b': j.a + 1 ABSENT ON NULL WITH UNIQUE KEYS RETURNING jsonb) AS jo, JSON_ARRAY(j.a, NULL, j.a * 2 NULL ON NULL RETURNING text) AS ja, JSON_ARRAY(SELECT a FROM ei_j WHERE a <= {n} ORDER BY a RETURNING jsonb) AS jq, JSON(('{{\"x\": ' || j.a || '}}')::text) AS jp, JSON_SCALAR(j.a) AS js, JSON_SERIALIZE(j.doc RETURNING text) AS jser, j.doc IS JSON OBJECT AS ij, ('[' || j.a || ']') IS JSON ARRAY WITH UNIQUE KEYS AS ija FROM ei_j j) SELECT count(*) FROM p;"
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT JSON_OBJECTAGG(j.a: j.doc ABSENT ON NULL WITH UNIQUE KEYS) AS oa, JSON_ARRAYAGG(j.a ORDER BY j.a DESC NULL ON NULL RETURNING jsonb) AS aa FROM ei_j j WHERE j.a <= {n} + 2) SELECT count(*) FROM p;"
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT JSON_EXISTS(j.doc, '$.k ? (@ > $lo)' PASSING {n} AS lo) AS je, JSON_VALUE(j.doc, '$.k' RETURNING int DEFAULT 0 ON EMPTY ERROR ON ERROR) AS jv, JSON_VALUE(j.doc, '$.missing' RETURNING date DEFAULT '2024-01-02'::date ON EMPTY NULL ON ERROR) AS jvd, JSON_QUERY(j.doc, '$.arr' WITH CONDITIONAL ARRAY WRAPPER EMPTY ARRAY ON ERROR) AS jqw, JSON_QUERY(j.doc, '$.k' OMIT QUOTES EMPTY OBJECT ON EMPTY) AS jqo FROM ei_j j) SELECT count(*) FROM p;"
                )),
                raw(format!(
                    "WITH RECURSIVE p AS (SELECT jt.* FROM ei_j j, JSON_TABLE(j.doc, '$' PASSING {n} AS thr COLUMNS (ord FOR ORDINALITY, k int PATH '$.k' DEFAULT -1 ON EMPTY, ex bool EXISTS PATH '$.nest', fmt jsonb FORMAT JSON PATH '$.nest', NESTED PATH '$.arr[*]' AS na COLUMNS (elem int PATH '$'))) jt) SELECT count(*) FROM p;"
                )),
                raw("DROP TABLE ei_j;"),
            ]
        }
        // ------------------------------------------------- interpreter ---
        // EEOP_DISTINCT / EEOP_NOT_DISTINCT over every null combination.
        "einterp:op:distinct" => {
            let v = vals(g);
            let v2 = vals(g);
            vec![
                raw(format!(
                    "SELECT v.a IS DISTINCT FROM v.a, v.a IS DISTINCT FROM NULL, v.b IS NOT DISTINCT FROM v.b, v.b IS NOT DISTINCT FROM 'w', v.c IS DISTINCT FROM (NOT v.c), ROW(v.a, v.b) IS DISTINCT FROM ROW(v.a, 'q') FROM {v};"
                )),
                // NULLIF arms incl. both-null / one-null / equal-args-NULL
                // result.
                raw(format!(
                    "SELECT NULLIF(v.a, v.a), NULLIF(v.b, v.b), NULLIF(v.a, 11), NULLIF(v.b, NULL), NULLIF(v.c, false) FROM {v2};"
                )),
            ]
        }
        // EEOP_ROWCOMPARE_STEP / EEOP_ROWCOMPARE_FINAL, incl. the NULL
        // jump: rows compared column-by-column with NULLs mid-row.
        "einterp:op:rowcmp" => {
            let v = vals(g);
            let v2 = vals(g);
            vec![
                raw(format!(
                    "SELECT ROW(v.a, v.b) < ROW(v.a, 'zz'), ROW(v.a, v.b) >= ROW(0, v.b), ROW(v.b, v.a) <= ROW(v.b, NULL), ROW(v.a, v.b, v.c) > ROW(v.a, v.b, false), (v.a, v.b) = (v.a, v.b), (v.a, v.b) <> (1, 'x') FROM {v};"
                )),
                // NULL-jump arms: first-column NULL and equal-then-NULL
                // sequences over columns that are NULL in some rows.
                raw(format!(
                    "SELECT (v.a, v.b) < (v.a, v.b), (v.c, v.a) >= (v.c, 1), (v.b, v.a) > (v.b, 0), (v.a, v.c) <= (v.a, true) FROM {v2};"
                )),
            ]
        }
        // EEOP_BOOLTEST_* (all six) + the EEOP_QUAL null branch.
        "einterp:op:booltest" => {
            let v = vals(g);
            vec![
                raw(format!(
                    "SELECT v.c IS TRUE, v.c IS NOT TRUE, v.c IS FALSE, v.c IS NOT FALSE, v.c IS UNKNOWN, v.c IS NOT UNKNOWN FROM {v};"
                )),
                raw(format!("SELECT count(*) FROM {v} WHERE v.c;")),
                raw(format!("SELECT count(*) FROM {v} WHERE v.c AND v.a > 0;")),
                raw(format!(
                    "SELECT v.c AND v.a > 0 AND v.b = 'w', v.c OR v.a > 100 OR v.b IS NULL, NOT v.c, v.c AND NULL, v.c OR NULL FROM {v};"
                )),
            ]
        }
        // EEOP_INNER_SYSVAR / EEOP_OUTER_SYSVAR: system columns referenced
        // above a join, from both sides, under nestloop / hash / merge.
        "einterp:op:sysvar" => {
            let mut out = vec![
                raw("CREATE TABLE ei_sv (a int PRIMARY KEY, b text);"),
                raw("INSERT INTO ei_sv SELECT g, 'sv' || g FROM generate_series(1, 12) g;"),
            ];
            for (setup, reset) in [
                ("SET enable_hashjoin = off; SET enable_mergejoin = off;", "RESET enable_hashjoin; RESET enable_mergejoin;"),
                ("SET enable_nestloop = off; SET enable_mergejoin = off;", "RESET enable_nestloop; RESET enable_mergejoin;"),
                ("SET enable_nestloop = off; SET enable_hashjoin = off;", "RESET enable_nestloop; RESET enable_hashjoin;"),
            ] {
                for s in setup.split_inclusive(';') {
                    let s = s.trim();
                    if !s.is_empty() {
                        out.push(raw(s.to_string()));
                    }
                }
                out.push(raw(
                    "SELECT count(x.ctid) + count(x.tableoid) + count(y.ctid) + count(y.tableoid), min(x.cmin::text), min(y.cmax::text) FROM ei_sv x JOIN ei_sv y ON x.a = y.a;".to_string(),
                ));
                // Cross-rel system-column join qual: evaluated at the join
                // node with the scan slots as inner/outer, so the sysvar
                // fetch runs through EEOP_INNER_SYSVAR / EEOP_OUTER_SYSVAR.
                out.push(raw(
                    "SELECT count(*) FROM ei_sv x JOIN ei_sv y ON x.a = y.a AND x.ctid <= y.ctid AND x.xmin = y.xmin;".to_string(),
                ));
                for s in reset.split_inclusive(';') {
                    let s = s.trim();
                    if !s.is_empty() {
                        out.push(raw(s.to_string()));
                    }
                }
            }
            out.push(raw("DROP TABLE ei_sv;"));
            out
        }
        // EEOP_OLD_VAR / EEOP_NEW_VAR / EEOP_OLD_SYSVAR / EEOP_NEW_SYSVAR /
        // EEOP_OLD_FETCHSOME / EEOP_NEW_FETCHSOME / EEOP_RETURNINGEXPR:
        // OLD/NEW in RETURNING on UPDATE (both live), INSERT (OLD all-NULL
        // arm) and DELETE (NEW all-NULL arm), incl. system columns.
        "einterp:op:oldnew" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_on (a int PRIMARY KEY, b int);"),
                raw("INSERT INTO ei_on SELECT g, g * 10 FROM generate_series(1, 6) g;"),
                raw(format!(
                    "UPDATE ei_on SET b = b + 1 WHERE a = {k} RETURNING old.a, old.b, new.b, old.b + new.b, (old.ctid = new.ctid) IS NOT NULL, (old.xmax::text >= '0'), (new.xmin::text >= '0'), old.tableoid = new.tableoid;"
                )),
                raw(format!(
                    "INSERT INTO ei_on VALUES (100 + {k}, 0) RETURNING old.a, old.b, new.a, new.b, old.a IS NULL, COALESCE(old.b, -1) + new.b;"
                )),
                raw(format!(
                    "DELETE FROM ei_on WHERE a = {k} + 1 RETURNING WITH (OLD AS o, NEW AS n) o.a, n.a, n.b IS NULL, o.b - COALESCE(n.b, 0);"
                )),
                raw("DROP TABLE ei_on;"),
            ]
        }
        // EEOP_FIELDSTORE_DEFORM / EEOP_FIELDSTORE_FORM (composite-column
        // field assignment, incl. two fields in one statement and nested
        // composite), EEOP_FIELDSELECT over NULL rows and function results.
        "einterp:op:fieldstore" => {
            let k = 1 + g.rng.below(9);
            vec![
                raw("CREATE TYPE ei_pt AS (x int, y text);"),
                raw("CREATE TYPE ei_np AS (p ei_pt, tag int);"),
                raw("CREATE TABLE ei_fs (a int, cp ei_pt, np ei_np);"),
                raw(format!(
                    "INSERT INTO ei_fs VALUES (1, ROW({k}, 'p1'), ROW(ROW({k}, 'q'), 5)), (2, NULL, NULL);"
                )),
                raw(format!("UPDATE ei_fs SET cp.x = {k} + 1, cp.y = 'set' WHERE a = 1;")),
                raw("UPDATE ei_fs SET cp.x = 0 WHERE a = 2;"),
                raw(format!("UPDATE ei_fs SET np.p.y = 'deep', np.tag = {k} WHERE a = 1;")),
                raw("SELECT (f.cp).x, (f.cp).y, ((f.np).p).y, (f.np).tag, (NULL::ei_pt).x FROM ei_fs f ORDER BY f.a;"),
                raw("DROP TABLE ei_fs;"),
                raw("DROP TYPE ei_np;"),
                raw("DROP TYPE ei_pt;"),
            ]
        }
        // EEOP_SBSREF_SUBSCRIPTS (incl. NULL-subscript jump) /
        // EEOP_SBSREF_FETCH / slice + assignment arms, multi-dim.
        "einterp:op:sbsref" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_ar (a int, arr int[], m int[][]);"),
                raw(format!(
                    "INSERT INTO ei_ar VALUES (1, ARRAY[10, 20, 30, 40], ARRAY[[1, 2], [3, 4]]), (2, NULL, NULL);"
                )),
                raw(format!(
                    "SELECT r.arr[{k}], r.arr[r.a], r.arr[NULL::int], r.arr[2:3], r.arr[:2], r.arr[3:], r.m[2][1], r.m[1:2][2:2], r.arr[r.a:NULL] FROM ei_ar r ORDER BY r.a;"
                )),
                raw(format!("UPDATE ei_ar SET arr[{k}] = 99, m[1][2] = 42 WHERE a = 1;")),
                raw(format!("UPDATE ei_ar SET arr[2:3] = ARRAY[7, 8] WHERE a = {k} % 2 + 1;")),
                raw("SELECT r.arr, r.m FROM ei_ar r ORDER BY r.a;"),
                raw("DROP TABLE ei_ar;"),
            ]
        }
        // EEOP_DOMAIN_NOTNULL / EEOP_DOMAIN_CHECK / EEOP_DOMAIN_TESTVAL +
        // matched-error fuel for the check-violation ereport.
        "einterp:op:domain" => {
            let lim = 10 + g.rng.below(90);
            vec![
                raw(format!(
                    "CREATE DOMAIN ei_dom AS int NOT NULL CHECK (VALUE > 0 AND VALUE < {lim} AND VALUE <> {});",
                    lim / 2
                )),
                raw("CREATE DOMAIN ei_doma AS int[] CHECK (array_length(VALUE, 1) <= 3);"),
                raw("CREATE TABLE ei_dt (d ei_dom, da ei_doma);"),
                raw(format!("INSERT INTO ei_dt VALUES ({}, ARRAY[1, 2]);", lim / 2 + 1)),
                raw(format!("SELECT (v.a % {lim})::ei_dom + 0 FROM (VALUES (1), (2)) v(a) WHERE v.a % {lim} > 0;")),
                raw("SELECT d.d, d.da[1] FROM ei_dt d;"),
                // DOMAIN_TESTVAL_EXT: domain_in (IO-path domain cast) and
                // ALTER DOMAIN VALIDATE evaluate the CHECK with an
                // externally-supplied domain value (domains.c/typecmds.c).
                raw(format!("SELECT ('{}'::text)::ei_dom + 0;", lim / 2 + 2)),
                raw("ALTER DOMAIN ei_dom ADD CONSTRAINT ei_dom_c2 CHECK (VALUE <> -5) NOT VALID;"),
                raw("ALTER DOMAIN ei_dom VALIDATE CONSTRAINT ei_dom_c2;"),
                raw("DROP TABLE ei_dt;"),
                raw("DROP DOMAIN ei_doma;"),
                raw("DROP DOMAIN ei_dom;"),
            ]
        }
        // EEOP_IOCOERCE (+ EEOP_IOCOERCE_SAFE via SQL/JSON RETURNING
        // coercions) over non-foldable operands; EEOP_CONVERT_ROWTYPE via
        // an inheritance child with a dropped column.
        "einterp:op:iocoerce" => {
            let k = 1 + g.rng.below(200);
            vec![
                raw(format!(
                    "SELECT (v.t)::inet, (v.t)::cidr IS NOT NULL, (v.n::text)::money::text <> '', (v.n::text)::oid, (v.t2)::interval::text FROM (VALUES ('192.168.{}.1', {k}, 'PT1H2M'), ('10.0.0.0', 0, 'P1D')) v(t, n, t2);",
                    k % 250
                )),
                raw(format!(
                    "SELECT JSON_VALUE(('{{\"d\": \"2024-0' || v.m || '-03\"}}')::jsonb, '$.d' RETURNING date NULL ON ERROR), JSON_VALUE(('{{\"d\": \"not-a-date\"}}')::jsonb, '$.d' RETURNING date DEFAULT '1999-12-31'::date ON ERROR) FROM (VALUES (1), (2)) v(m);"
                )),
                // EEOP_IOCOERCE_SAFE: the ON ERROR / ON EMPTY default
                // expression is coerced to the RETURNING type through the
                // type IO routines under the soft-error context. A constant
                // DEFAULT is folded to a typed Const at plan time, so the
                // stable current_setting() arm below is what actually keeps
                // a CoerceViaIO under the escontext.
                raw("SET ei.defip = '10.9.8.7';"),
                raw(format!(
                    "SELECT JSON_VALUE(('{{\"k\": \"nope' || v.m || '\"}}')::jsonb, '$.k' RETURNING inet DEFAULT current_setting('ei.defip') ON ERROR) FROM (VALUES (1), (2)) v(m);"
                )),
                raw("RESET ei.defip;"),
                raw(format!(
                    "SELECT JSON_VALUE(('{{\"k\": \"zz' || v.m || '\"}}')::jsonb, '$.k' RETURNING inet DEFAULT '10.1.2.3' ON ERROR), JSON_VALUE(('{{}}')::jsonb, '$.k' RETURNING interval DEFAULT 'PT2H' ON EMPTY NULL ON ERROR), JSON_QUERY(('{{\"k\": \"q\"}}')::jsonb, '$.k' RETURNING text OMIT QUOTES DEFAULT 'dq' ON ERROR) FROM (VALUES (1), (2)) v(m);"
                )),
                raw("CREATE TABLE ei_par (a int, b text);"),
                raw("CREATE TABLE ei_chi (dropme int, extra int) INHERITS (ei_par);"),
                raw("ALTER TABLE ei_chi DROP COLUMN dropme;"),
                raw(format!("INSERT INTO ei_chi (a, b, extra) VALUES ({k}, 'c', 9);")),
                raw("SELECT (p.*)::text, row_to_json(p) FROM ei_par p ORDER BY p.a;"),
                raw("DROP TABLE ei_chi;"),
                raw("DROP TABLE ei_par;"),
            ]
        }
        // Aggregate transition variants: strict/non-strict, byval/byref,
        // init/no-init, DESERIALIZE-adjacent two-arg input checks, ordered
        // (DATUM and TUPLE), FILTER, DISTINCT (unsorted path), grouping
        // sets with hashed init (EEOP_HASHDATUM_SET_INITVAL /
        // EEOP_AGG_PLAIN_PERGROUP_NULLCHECK), null-fuel for the
        // STRICT_INPUT_CHECK arms.
        "einterp:op:aggvariants" => {
            let m = 2 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_ag (k int, n2 int2, n8 int8, f float8, nu numeric, iv interval, t text);"),
                raw(format!(
                    "INSERT INTO ei_ag SELECT g % {m}, (g % 5)::int2, g * 100000000000, g / 4.0, g * 1.5, make_interval(mins => g), CASE WHEN g % 4 = 0 THEN NULL ELSE 't' || g END FROM generate_series(1, 40) g;"
                )),
                raw("INSERT INTO ei_ag VALUES (0, NULL, NULL, NULL, NULL, NULL, NULL);"),
                raw("SELECT sum(a.n2), sum(a.n8), avg(a.f), sum(a.nu), avg(a.iv), min(a.t), bit_and(a.k), bit_or(a.n2), bool_and(a.k >= 0), bool_or(a.k > 1) FROM ei_ag a;"),
                raw("SELECT a.k, string_agg(a.t, ',' ORDER BY a.t), array_agg(a.n2 ORDER BY a.n2 DESC NULLS LAST), count(DISTINCT a.f), corr(a.f, a.k), covar_pop(a.f, a.nu::float8), regr_avgx(a.f, a.f) FROM ei_ag a GROUP BY a.k ORDER BY a.k;"),
                raw("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a.f), percentile_disc(ARRAY[0.25, 0.75]) WITHIN GROUP (ORDER BY a.nu), mode() WITHIN GROUP (ORDER BY a.k), rank(2, 't') WITHIN GROUP (ORDER BY a.k, a.t) FROM ei_ag a;"),
                raw("SELECT a.k % 2 AS k2, a.n2 % 2 AS n22, GROUPING(a.k % 2, a.n2 % 2), count(*) FILTER (WHERE a.t IS NOT NULL), sum(a.n8) FROM ei_ag a GROUP BY GROUPING SETS ((a.k % 2), (a.n2 % 2), ()) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3;"),
                raw("SET enable_sort = off;"),
                raw("SELECT a.k % 2 AS k2, a.n2 % 2 AS n22, count(*) FROM ei_ag a GROUP BY GROUPING SETS ((a.k % 2), (a.n2 % 2)) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3;"),
                raw("RESET enable_sort;"),
                // Non-strict byref transition (EEOP_AGG_PLAIN_TRANS_BYREF):
                // json_agg/jsonb_agg accumulate NULL inputs too.
                raw("SELECT json_agg(a.n2 ORDER BY a.n2 NULLS FIRST)::text <> '', jsonb_agg(a.t ORDER BY a.t NULLS LAST) IS NOT NULL, count(*) FROM ei_ag a;"),
                // EEOP_AGG_PLAIN_TRANS_BYREF needs a NON-strict transfn
                // with a BYREF transtype + non-null initcond; every
                // built-in non-strict agg carries an INTERNAL (byval
                // pointer) state, so a tiny custom aggregate supplies it.
                raw("CREATE FUNCTION ei_cat(text, int2) RETURNS text LANGUAGE sql IMMUTABLE AS $q$SELECT COALESCE($1, '') || COALESCE($2::text, 'n')$q$;"),
                raw("CREATE AGGREGATE ei_catagg (int2) (SFUNC = ei_cat, STYPE = text, INITCOND = '');"),
                raw("SELECT a.k, ei_catagg(a.n2 ORDER BY a.n2 NULLS LAST) FROM ei_ag a GROUP BY a.k ORDER BY a.k;"),
                raw("DROP AGGREGATE ei_catagg (int2);"),
                raw("DROP FUNCTION ei_cat(text, int2);"),
                // Hashagg spill (EEOP_AGG_PLAIN_PERGROUP_NULLCHECK): mixed
                // grouping-set hashing under a tiny work_mem floor.
                raw("SET work_mem = '64kB';"),
                raw("SET enable_sort = off;"),
                raw("SELECT a.k, a.n2, sum(a.n8), count(*) FROM ei_ag a GROUP BY GROUPING SETS ((a.k), (a.n2), (a.k, a.n2)) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3, 4;"),
                // Real hashagg SPILL (EEOP_AGG_PLAIN_PERGROUP_NULLCHECK is
                // compiled in only once spill mode engages): many groups
                // under the 64kB floor, checksum-projected.
                raw("CREATE TABLE ei_agsp (k int, n int8);"),
                raw("INSERT INTO ei_agsp SELECT g, g FROM generate_series(1, 12000) g;"),
                raw("SELECT count(*), sum(cs) FROM (SELECT k % 9000 AS grp, sum(n) AS cs FROM ei_agsp GROUP BY 1) s;"),
                raw("DROP TABLE ei_agsp;"),
                raw("RESET enable_sort;"),
                raw("RESET work_mem;"),
                raw("DROP TABLE ei_ag;"),
            ]
        }
        // EEOP_AGG_PRESORTED_DISTINCT_SINGLE / _MULTI +
        // EEOP_AGG_ORDERED_TRANS_DATUM / _TUPLE over an index-presorted
        // input path.
        "einterp:op:aggsorted" => {
            let m = 3 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_ps (a int, b int, t text);"),
                raw(format!(
                    "INSERT INTO ei_ps SELECT g % {m}, g % 3, 'p' || (g % {m}) FROM generate_series(1, 60) g;"
                )),
                raw("CREATE INDEX ei_ps_ab ON ei_ps (a, b);"),
                raw("ANALYZE ei_ps;"),
                raw("SET enable_seqscan = off;"),
                raw("SELECT count(DISTINCT p.a), array_agg(DISTINCT p.a ORDER BY p.a) FROM ei_ps p;"),
                raw("SELECT string_agg(DISTINCT p.a::text, ',' ORDER BY p.a::text), count(DISTINCT (p.a, p.b)) FROM ei_ps p;"),
                raw("SELECT p.a, string_agg(p.t, '|' ORDER BY p.t, p.b) FROM ei_ps p GROUP BY p.a ORDER BY p.a;"),
                raw("RESET enable_seqscan;"),
                raw("DROP TABLE ei_ps;"),
            ]
        }
        // EEOP_NEXTVALUEEXPR: identity columns (both kinds) + OVERRIDING.
        "einterp:op:nextval" => {
            let k = 1 + g.rng.below(50);
            vec![
                raw("CREATE TABLE ei_id (a int GENERATED BY DEFAULT AS IDENTITY, b int GENERATED ALWAYS AS IDENTITY (START WITH 7), c int);"),
                raw(format!("INSERT INTO ei_id (c) VALUES ({k}), ({k} + 1);")),
                raw(format!("INSERT INTO ei_id (a, c) OVERRIDING SYSTEM VALUE VALUES (500, {k});")),
                raw("INSERT INTO ei_id (b, c) OVERRIDING SYSTEM VALUE VALUES (600, 0);"),
                raw("SELECT i.a, i.b, i.c FROM ei_id i ORDER BY i.c, i.a;"),
                raw("DROP TABLE ei_id;"),
            ]
        }
        // WHERE CURRENT OF over a FOR UPDATE cursor (TidScan path) and the
        // EEOP_CURRENTOFEXPR generic-expression ereport arm via a
        // matched-error probe (CURRENT OF evaluated outside a supported
        // scan — identical 42P10/feature error on both engines is the bar;
        // hand-verified before landing).
        "einterp:op:currentof" => {
            let k = 1 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_cur (a int PRIMARY KEY, b int);"),
                raw("INSERT INTO ei_cur SELECT g, g * 2 FROM generate_series(1, 8) g;"),
                raw("BEGIN;"),
                raw(format!(
                    "DECLARE ei_c1 CURSOR FOR SELECT a, b FROM ei_cur WHERE a <= {} FOR UPDATE;",
                    4 + k
                )),
                raw("FETCH 2 FROM ei_c1;"),
                raw("UPDATE ei_cur SET b = b + 100 WHERE CURRENT OF ei_c1;"),
                raw("FETCH NEXT FROM ei_c1;"),
                raw("DELETE FROM ei_cur WHERE CURRENT OF ei_c1;"),
                raw("CLOSE ei_c1;"),
                raw("COMMIT;"),
                raw("SELECT c.a, c.b FROM ei_cur c ORDER BY c.a;"),
                raw("DROP TABLE ei_cur;"),
            ]
        }
        // EEOP_HASHED_SCALARARRAYOP (long IN lists, hashable, with and
        // without NULL) + strict/nonstrict SAOP arms.
        "einterp:op:hashsaop" => {
            let base = g.rng.below(3);
            let mk = |off: u64| {
                (0..12).map(|i| (base + off + i * 3).to_string()).collect::<Vec<_>>().join(", ")
            };
            let inl = mk(0);
            let inl2 = mk(1);
            vec![
                raw("CREATE TABLE ei_in (a int, t text);"),
                raw("INSERT INTO ei_in SELECT g, 'v' || (g % 10) FROM generate_series(1, 30) g;"),
                raw(format!("SELECT count(*) FROM ei_in i WHERE i.a IN ({inl});")),
                raw(format!("SELECT count(*) FROM ei_in i WHERE i.a NOT IN ({inl2});")),
                raw(format!("SELECT count(*) FROM ei_in i WHERE i.a IN ({inl}, NULL);")),
                raw("SELECT count(*) FROM ei_in i WHERE i.t IN ('v0', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8', 'vX', 'vY', 'vZ');"),
                raw("SELECT count(*) FROM ei_in i WHERE i.a = ANY (ARRAY[1, 2, NULL, 4]);"),
                raw("SELECT count(*) FROM ei_in i WHERE i.a <> ALL (ARRAY[5, 6, 7]);"),
                raw("DROP TABLE ei_in;"),
            ]
        }
        // EEOP_WHOLEROW arms (ExecEvalWholeRowVar): base table, view,
        // subquery, RECORD-typed whole-row, whole-row compare + ::text.
        "einterp:op:wholerow" => {
            let k = 1 + g.rng.below(6);
            vec![
                raw("CREATE TABLE ei_wr (a int, b text);"),
                raw(format!("INSERT INTO ei_wr SELECT g, 'wr' || g FROM generate_series(1, {}) g;", 3 + k)),
                raw("CREATE VIEW ei_wrv AS SELECT w.a, w.b FROM ei_wr w WHERE w.a > 0;"),
                // Whole-row values are projected through ::text/json only: a
                // bare composite output column carries the transient rowtype
                // OID in RowDescription, which drifts between engines
                // (allocation order), so it is not a comparison surface.
                raw("SELECT (t.*)::text, row_to_json(t), t = t, t IS NOT NULL FROM ei_wr t ORDER BY t.a;"),
                raw("SELECT (v.*)::text, v IS NOT NULL FROM ei_wrv v ORDER BY v.a;"),
                raw("SELECT (s.*)::text, s.x FROM (SELECT w.a AS x, w.b AS y FROM ei_wr w) s ORDER BY s.x;"),
                raw("SELECT count(*) FROM ei_wr x JOIN ei_wr y ON x.a = y.a WHERE x = y;"),
                raw("DROP VIEW ei_wrv;"),
                raw("DROP TABLE ei_wr;"),
            ]
        }
        // EEOP_SQLVALUEFUNCTION variants. Time-of-day results are never
        // compared directly (two servers, two clocks): only pg_typeof and
        // wide sanity predicates, which are stable. Name-class SVFs
        // (current_role & co) are engine-stable and compared exactly.
        "einterp:op:svf" => vec![
            raw("SELECT current_role, current_user, session_user, current_catalog, current_schema;"),
            raw("SELECT pg_typeof(current_date)::text, pg_typeof(current_time)::text, pg_typeof(current_time(2))::text, pg_typeof(current_timestamp)::text, pg_typeof(current_timestamp(1))::text, pg_typeof(localtime)::text, pg_typeof(localtime(3))::text, pg_typeof(localtimestamp)::text, pg_typeof(localtimestamp(4))::text;"),
            raw("SELECT current_date >= date '2020-01-01', localtimestamp >= timestamp '2020-01-01', current_time IS NOT NULL, localtime(0) IS NOT NULL, current_timestamp(2) >= timestamptz '2020-01-01';"),
        ],
        // EEOP_CASE_TESTVAL (CASE-with-arg over columns; the implicit-arg
        // equality rewrites into CaseTestExpr) incl. nested CASE and a
        // domain CHECK referencing VALUE twice (DOMAIN_TESTVAL reuse).
        "einterp:op:casetest" => {
            let v = vals(g);
            vec![
                raw(format!(
                    "SELECT CASE v.a WHEN 1 THEN 'one' WHEN NULL THEN 'null' ELSE 'other' END, CASE v.b WHEN 'w' THEN CASE v.a WHEN 11 THEN 'w11' ELSE 'w?' END ELSE COALESCE(v.b, '?') END, CASE WHEN v.c THEN v.a ELSE -v.a END FROM {v};"
                )),
                raw("CREATE DOMAIN ei_cd AS int CHECK (CASE VALUE WHEN 13 THEN false ELSE VALUE > -1000 END);"),
                raw("SELECT (v.a)::ei_cd FROM (VALUES (1), (7)) v(a);"),
                raw("DROP DOMAIN ei_cd;"),
            ]
        }
        // EEOP_MERGE_SUPPORT_FUNC: MERGE ... RETURNING merge_action() +
        // OLD/NEW across all three action kinds.
        "einterp:op:mergeaction" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_mt (a int PRIMARY KEY, b int);"),
                raw("CREATE TABLE ei_ms (a int, b int);"),
                raw(format!("INSERT INTO ei_mt SELECT g, g FROM generate_series(1, 6) g;")),
                raw(format!(
                    "INSERT INTO ei_ms SELECT g, g * {k} FROM generate_series(4, 9) g;"
                )),
                raw("MERGE INTO ei_mt t USING ei_ms s ON t.a = s.a WHEN MATCHED AND s.b > 10 THEN DELETE WHEN MATCHED THEN UPDATE SET b = t.b + s.b WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b) RETURNING merge_action(), t.a, old.b AS ob, new.b AS nb;"),
                raw("SELECT t.a, t.b FROM ei_mt t ORDER BY t.a;"),
                raw("DROP TABLE ei_ms;"),
                raw("DROP TABLE ei_mt;"),
            ]
        }
        // EEOP_PARAM_SET via MULTIEXPR SubPlan (UPDATE SET (x, y) =
        // (SELECT ...)) + EEOP_SUBPLAN correlated hashed/unhashed.
        "einterp:op:paramset" => {
            let k = 1 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_pm (a int PRIMARY KEY, x int, y text);"),
                raw("INSERT INTO ei_pm SELECT g, g, 'p' || g FROM generate_series(1, 8) g;"),
                raw(format!(
                    "UPDATE ei_pm SET (x, y) = (SELECT p2.x + 100, p2.y || '!' FROM ei_pm p2 WHERE p2.a = ei_pm.a) WHERE a <= {k};"
                )),
                raw(format!(
                    "UPDATE ei_pm SET (x, y) = (SELECT 0, NULL) WHERE a = {k} + 4;"
                )),
                raw("SELECT p.a, p.x, p.y, (SELECT count(*) FROM ei_pm i WHERE i.a <= p.a) FROM ei_pm p ORDER BY p.a;"),
                raw("DROP TABLE ei_pm;"),
            ]
        }
        // WindowFuncRunCondition (walker + mutator): monotonic window
        // function compared in an outer WHERE — the run-condition pushdown.
        "einterp:op:runcond" => {
            let k = 2 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_rc (a int, b int);"),
                raw("INSERT INTO ei_rc SELECT g, g % 5 FROM generate_series(1, 30) g;"),
                raw(format!(
                    "SELECT s.a, s.rn FROM (SELECT r.a, row_number() OVER (ORDER BY r.a) AS rn FROM ei_rc r) s WHERE s.rn <= {k} ORDER BY s.rn;"
                )),
                raw(format!(
                    "SELECT s.b, s.rk FROM (SELECT r.b, rank() OVER (PARTITION BY r.b ORDER BY r.a) AS rk, count(*) OVER (ORDER BY r.a) AS cn FROM ei_rc r) s WHERE s.rk < {k} AND s.cn <= 25 ORDER BY s.b, s.rk;"
                )),
                raw("DROP TABLE ei_rc;"),
            ]
        }
        // CTESearchClause / CTECycleClause (walker + mutator arms) in live
        // recursive queries AND stored in a view (readfuncs already owns
        // the deserialize side; the walk here is the rewriter/planner).
        "einterp:op:cycle" => {
            let n = 3 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_g (src int, dst int);"),
                raw(format!(
                    "INSERT INTO ei_g VALUES (1, 2), (2, 3), (3, 1), (3, {n}), ({n}, {n});"
                )),
                raw("WITH RECURSIVE walk(src, dst) AS (SELECT g.src, g.dst FROM ei_g g WHERE g.src = 1 UNION ALL SELECT w.dst, g.dst FROM walk w JOIN ei_g g ON g.src = w.dst) CYCLE dst SET is_cycle USING path SELECT count(*) FROM walk WHERE NOT is_cycle;"),
                raw("WITH RECURSIVE tree(n) AS (SELECT 1 UNION ALL SELECT t.n + 1 FROM tree t WHERE t.n < 5) SEARCH DEPTH FIRST BY n SET ord SELECT count(*) FROM tree;"),
                raw("CREATE VIEW ei_cyv AS WITH RECURSIVE walk(src, dst) AS (SELECT g.src, g.dst FROM ei_g g WHERE g.src = 1 UNION ALL SELECT w.dst, g.dst FROM walk w JOIN ei_g g ON g.src = w.dst) CYCLE dst SET looped USING pth SELECT src, dst FROM walk WHERE NOT looped;"),
                raw("SELECT count(*) FROM ei_cyv;"),
                raw("DROP VIEW ei_cyv;"),
                raw("DROP TABLE ei_g;"),
            ]
        }
        // EEOP_FUNCEXPR_FUSAGE / EEOP_FUNCEXPR_STRICT_FUSAGE: function
        // calls under track_functions = all (per-function stats wrapper).
        "einterp:op:fusage" => {
            let k = 1 + g.rng.below(9);
            vec![
                // plpgsql, deliberately: simple SQL-language bodies are inlined
                // by the planner and the FuncExpr never reaches the
                // interpreter's FUSAGE opcodes (probe-verified).
                raw("CREATE FUNCTION ei_fu1(int) RETURNS int LANGUAGE plpgsql STRICT AS 'BEGIN RETURN $1 + 1; END';"),
                raw("CREATE FUNCTION ei_fu2(int, int) RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN COALESCE($1, 0) + COALESCE($2, 0); END';"),
                raw("SET track_functions = 'all';"),
                raw(format!(
                    "SELECT ei_fu1(v.a), ei_fu1(NULL), ei_fu2(v.a, {k}), ei_fu2(NULL, NULL) FROM (VALUES ({k}), (NULL)) v(a);"
                )),
                raw("RESET track_functions;"),
                raw("DROP FUNCTION ei_fu2(int, int);"),
                raw("DROP FUNCTION ei_fu1(int);"),
            ]
        }
        // Parallel aggregate bracket: EEOP_HASHDATUM_SET_INITVAL (the
        // TupleHashTable per-worker hash IV), EEOP_AGG_DESERIALIZE and the
        // combine-side strict input checks.
        "einterp:op:aggparallel" => {
            let m = 5 + g.rng.below(5);
            vec![
                raw("CREATE TABLE ei_pa (k int, k2 int, n numeric, f float8);"),
                raw(format!(
                    "INSERT INTO ei_pa SELECT g % {m}, g % 3, g * 1.25, g / 8.0 FROM generate_series(1, 4000) g;"
                )),
                raw("SET parallel_setup_cost = 0;"),
                raw("SET parallel_tuple_cost = 0;"),
                raw("SET min_parallel_table_scan_size = 0;"),
                raw("SET max_parallel_workers_per_gather = 2;"),
                // Two-column group key on purpose: a single-key hash expr is
                // served by an ExecJust* fast path that bypasses the
                // dispatch-loop HASHDATUM arms (probe-verified).
                // numeric aggregates only: float aggregation under parallel
                // combine is reassociation-order-sensitive (B1 territory),
                // not a byte-comparison surface.
                raw("SELECT p.k, p.k2, sum(p.n), avg(p.n), count(*) FROM ei_pa p GROUP BY p.k, p.k2 ORDER BY p.k, p.k2;"),
                raw("SELECT sum(p.n), avg(p.n), var_samp(p.n), count(p.f) FROM ei_pa p;"),
                raw("RESET max_parallel_workers_per_gather;"),
                raw("RESET min_parallel_table_scan_size;"),
                raw("RESET parallel_tuple_cost;"),
                raw("RESET parallel_setup_cost;"),
                raw("DROP TABLE ei_pa;"),
            ]
        }
        // ReturningExpr (rewriter-injected wrapper for OLD/NEW expansion in
        // view RETURNING): DML through an auto-updatable view with
        // RETURNING old/new expressions — EEOP_RETURNINGEXPR nullflag jump
        // on the INSERT (no OLD) and DELETE (no NEW) actions — plus INSERT
        // ... ON CONFLICT through the view (OnConflictExpr walker arms).
        "einterp:op:retview" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_rv (a int PRIMARY KEY, b int);"),
                raw("INSERT INTO ei_rv SELECT g, g * 3 FROM generate_series(1, 6) g;"),
                // The bb expression column matters: OLD/NEW expansion wraps a
                // non-Var tlist item in ReturningExpr (rewriteManip
                // ReplaceVarFromTargetList), which is what arms the
                // EEOP_RETURNINGEXPR nullflag jump on INSERT/DELETE.
                raw("CREATE VIEW ei_rvv AS SELECT r.a, r.b, r.b * 2 AS bb FROM ei_rv r WHERE r.b >= 0;"),
                raw(format!(
                    "UPDATE ei_rvv SET b = b + 5 WHERE a = {k} RETURNING old.b, new.b, old.bb, new.bb, old.bb + new.bb, (old.*)::text, (new.*)::text;"
                )),
                raw(format!(
                    "INSERT INTO ei_rvv (a, b) VALUES (50 + {k}, 1) RETURNING old.b, new.b, old.bb, new.bb, COALESCE(old.bb, -1) * 2, old.b IS NULL, (old.*)::text;"
                )),
                raw(format!(
                    "INSERT INTO ei_rvv (a, b) VALUES ({k}, 9) ON CONFLICT (a) DO UPDATE SET b = excluded.b + 1 RETURNING old.b, new.b, old.bb, new.bb;"
                )),
                raw(format!(
                    "DELETE FROM ei_rvv WHERE a = {k} + 1 RETURNING old.b, new.b, old.bb, new.bb, old.bb * 10, new.bb IS NULL, (new.*)::text;"
                )),
                raw("DROP VIEW ei_rvv;"),
                raw("DROP TABLE ei_rv;"),
            ]
        }
        // PartitionBoundSpec / PartitionRangeDatum walker+mutator arms:
        // multi-column range bounds with MINVALUE/MAXVALUE, hash
        // modulus/remainder, DEFAULT partition — plus a generic-plan
        // EXECUTE for the runtime-pruning PartitionPruneStep arms.
        "einterp:op:partbound" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_pr (a int, b int, t text) PARTITION BY RANGE (a, b);"),
                raw("CREATE TABLE ei_pr0 PARTITION OF ei_pr FOR VALUES FROM (MINVALUE, MINVALUE) TO (10, 5);"),
                raw("CREATE TABLE ei_pr1 PARTITION OF ei_pr FOR VALUES FROM (10, 5) TO (20, MAXVALUE);"),
                raw("CREATE TABLE ei_prd PARTITION OF ei_pr DEFAULT;"),
                raw("CREATE TABLE ei_ph (h int, t text) PARTITION BY HASH (h);"),
                raw("CREATE TABLE ei_ph0 PARTITION OF ei_ph FOR VALUES WITH (MODULUS 2, REMAINDER 0);"),
                raw("CREATE TABLE ei_ph1 PARTITION OF ei_ph FOR VALUES WITH (MODULUS 2, REMAINDER 1);"),
                raw("INSERT INTO ei_pr SELECT g, g % 8, 'p' || g FROM generate_series(1, 30) g;"),
                raw("INSERT INTO ei_ph SELECT g, 'h' || g FROM generate_series(1, 20) g;"),
                raw(format!("PREPARE ei_ps1 (int) AS SELECT count(*) FROM ei_pr WHERE a < $1;")),
                raw(format!("EXECUTE ei_ps1({});", 5 + k)),
                raw("EXECUTE ei_ps1(15);"),
                raw("EXECUTE ei_ps1(25);"),
                raw("EXECUTE ei_ps1(15);"),
                raw("EXECUTE ei_ps1(25);"),
                raw("EXECUTE ei_ps1(5);"),
                raw("DEALLOCATE ei_ps1;"),
                raw(format!("SELECT count(*) FROM ei_ph WHERE h = {k};")),
                raw("DROP TABLE ei_ph;"),
                raw("DROP TABLE ei_pr;"),
            ]
        }
        // Analyzed-walker/mutator breadth through the rewriter: rare node
        // kinds stored in views, then queried (query_tree_mutator via
        // rewrite) and deparsed (ruleutils walks). Covers the walker arms
        // for JSON aggs/constructors, SetOperationStmt, WindowClause,
        // TableFunc (JSON_TABLE), ordered-set Aggref directargs.
        "einterp:op:viewwalk" => {
            let k = 1 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_vw (a int, b text, doc jsonb);"),
                raw(format!(
                    "INSERT INTO ei_vw SELECT g, 'v' || g, jsonb_build_object('k', g, 'arr', jsonb_build_array(g, g + 1)) FROM generate_series(1, {}) g;",
                    5 + k
                )),
                raw("CREATE VIEW ei_vw1 AS SELECT JSON_OBJECTAGG(w.a: w.b ABSENT ON NULL) AS oa, JSON_ARRAYAGG(w.a ORDER BY w.a DESC RETURNING jsonb) AS aa, JSON_OBJECT('n' VALUE count(*)) AS jo, JSON_ARRAY(min(w.a), max(w.a)) AS jr FROM ei_vw w;"),
                raw("CREATE VIEW ei_vw2 AS (SELECT w.a FROM ei_vw w) UNION ALL (SELECT w.a + 100 FROM ei_vw w) EXCEPT (SELECT 3);"),
                raw("CREATE VIEW ei_vw3 AS SELECT w.a, sum(w.a) OVER win2 AS s FROM ei_vw w WINDOW win1 AS (PARTITION BY w.a % 2), win2 AS (win1 ORDER BY w.a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW);"),
                raw("CREATE VIEW ei_vw5 AS SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY w.a) AS pc, rank(2) WITHIN GROUP (ORDER BY w.a) AS rk, mode() WITHIN GROUP (ORDER BY w.a % 3) AS md FROM ei_vw w HAVING count(*) FILTER (WHERE w.a > 1) > 0;"),
                raw("CREATE VIEW ei_vw4 AS SELECT jt.* FROM ei_vw w, JSON_TABLE(w.doc, '$' COLUMNS (k int PATH '$.k', NESTED PATH '$.arr[*]' AS na COLUMNS (e int PATH '$'))) jt;"),
                raw("SELECT v.oa IS NOT NULL, v.aa, v.jo, v.jr FROM ei_vw1 v;"),
                raw("SELECT count(*) FROM ei_vw2;"),
                raw("SELECT count(*) FROM ei_vw3;"),
                raw("SELECT count(*), sum(jt.k), sum(jt.e) FROM ei_vw4 jt;"),
                raw("SELECT v.pc, v.rk, v.md FROM ei_vw5 v;"),
                raw("SELECT pg_get_viewdef('ei_vw1'::regclass, true), pg_get_viewdef('ei_vw2'::regclass, true);"),
                raw("SELECT pg_get_viewdef('ei_vw3'::regclass, true), pg_get_viewdef('ei_vw4'::regclass, true), pg_get_viewdef('ei_vw5'::regclass, true);"),
                raw("DROP VIEW ei_vw5;"),
                raw("DROP VIEW ei_vw4;"),
                raw("DROP VIEW ei_vw3;"),
                raw("DROP VIEW ei_vw2;"),
                raw("DROP VIEW ei_vw1;"),
                raw("DROP TABLE ei_vw;"),
            ]
        }
        // ---------------------------------------------------- W4-WALK ---
        // exprTypmod all-branches-same-typmod arms (CASE 401-416, ARRAY
        // 437-450, COALESCE 465-478, MINMAX 493-506) + the mixed -1 arms.
        // exprTypmod runs on every output column at RowDescription time.
        "einterp:w4:typmod" => {
            let m = 5 + g.rng.below(4);
            vec![
                raw("CREATE TABLE ei_tm (a int, c varchar(5));"),
                raw(format!("INSERT INTO ei_tm SELECT g, ('c' || g)::varchar(5) FROM generate_series(1, {}) g;", 6 + g.rng.below(6))),
                raw(format!("SELECT CASE WHEN a > {m} THEN c ELSE 'z'::varchar(5) END, CASE a WHEN 1 THEN c ELSE 'y'::varchar(5) END, ARRAY[c, 'q'::varchar(5)], ARRAY[ARRAY[c], ARRAY['m'::varchar(5)]], COALESCE(c, 'n'::varchar(5)), GREATEST(c, 'g'::varchar(5)), LEAST(c, 'l'::varchar(5)), NULLIF(c, 'c1'::varchar(5)) FROM ei_tm ORDER BY a;")),
                raw(format!("SELECT CASE WHEN a > {m} THEN c ELSE 'z'::varchar(9) END, ARRAY[c, 'q'::varchar(9)], COALESCE(c, 'n'::varchar(9)), GREATEST(c, 'g'::varchar(9)), LEAST(c::varchar(9), 'l'::varchar(5)) FROM ei_tm ORDER BY a;")),
                raw("DROP TABLE ei_tm;"),
            ]
        }
        // Parallel-hazard beacon: max_parallel_hazard() walks the whole
        // parse tree of every plannable SELECT and returns TRUE up the
        // walk chain at the first PARALLEL UNSAFE function, lighting the
        // enclosing nodes' `if (WALK(field)) return true;` arms
        // (expression_tree_walker_impl 2138/2165/2294/2319/2321/2344/
        // 2346/2372/2375/2377/2385/2590/2642/2644, query_tree_walker_impl
        // 2725-2729/2760-2762, range_table_entry_walker_impl
        // 2836/2854/2858/2868/2873).
        "einterp:w4:hazard" => {
            let k = 1 + g.rng.below(4);
            let pct = 40 + g.rng.below(50);
            vec![
                raw("CREATE FUNCTION ei_pu(x int) RETURNS int LANGUAGE sql VOLATILE PARALLEL UNSAFE AS 'SELECT x + 0';"),
                raw("CREATE TABLE ei_hz (a int, b text, jb jsonb);"),
                raw(format!("INSERT INTO ei_hz SELECT g, 'b' || g, jsonb_build_object('n', g) FROM generate_series(1, {}) g;", 6 + k)),
                raw(format!("SELECT percentile_disc(0.5 * ei_pu(1)) WITHIN GROUP (ORDER BY a) FROM ei_hz;")),
                raw(format!("SELECT count(a + ei_pu(0)) FILTER (WHERE a > ei_pu({k})) FROM ei_hz;")),
                raw(format!("SELECT count(*) FILTER (WHERE a > ei_pu({k})) OVER (ORDER BY a) FROM ei_hz;")),
                raw(format!("SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN ei_pu({k}) PRECEDING AND CURRENT ROW) FROM ei_hz;")),
                raw(format!("SELECT sum(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND ei_pu({k}) FOLLOWING) FROM ei_hz;")),
                raw(format!("SELECT a FROM ei_hz WHERE (a + ei_pu(0), b) < ({}, 'zz');", 8 + k)),
                raw(format!("SELECT a FROM ei_hz WHERE CASE a + ei_pu(0) WHEN {k} THEN true ELSE false END;")),
                raw("SELECT t.a FROM ei_hz t JOIN ei_hz u ON t.a = u.a + ei_pu(1);"),
                raw(format!("SELECT count(*) FROM ei_hz TABLESAMPLE BERNOULLI ({pct} + ei_pu(0)) REPEATABLE (7);")),
                raw(format!("SELECT count(*) FROM ei_hz TABLESAMPLE SYSTEM (90) REPEATABLE (ei_pu({k}));")),
                raw(format!("SELECT * FROM (VALUES (ei_pu({k}), 'v1'), (2, 'v2')) v(x, y);")),
                raw("SELECT a + ei_pu(0), count(*) FROM ei_hz GROUP BY a + ei_pu(0);"),
                raw(format!("SELECT count(*) FROM ei_hz GROUP BY a HAVING count(*) < ei_pu({}0);", 1 + g.rng.below(4))),
                raw(format!("SELECT a FROM ei_hz ORDER BY a LIMIT {k} OFFSET ei_pu(0);")),
                raw(format!("SELECT a FROM ei_hz ORDER BY a LIMIT ei_pu({k});")),
                raw(format!("SELECT JSON_VALUE(jb, '$.n ? (@ > $x)' PASSING ei_pu({k}) AS x) FROM ei_hz;")),
                raw("SELECT JSON_VALUE(CASE WHEN ei_pu(1) = 1 THEN jb END, '$.n') FROM ei_hz;"),
                raw(format!("SELECT JSON_VALUE(jb, '$.missing' RETURNING int DEFAULT ei_pu({k}) ON EMPTY) FROM ei_hz;")),
                raw(format!("SELECT JSON_VALUE(jb, '$.nope' RETURNING int DEFAULT ei_pu({k}) ON ERROR) FROM ei_hz;")),
                raw(format!("SELECT jt.v FROM ei_hz, JSON_TABLE(jb, '$.n' PASSING ei_pu({k}) AS px COLUMNS (v int PATH '$', ex int EXISTS PATH '$ ? (@ > $px)')) jt WHERE a = {k};")),
                raw("DROP TABLE ei_hz;"),
                raw("DROP FUNCTION ei_pu(int);"),
            ]
        }
        // Whole-row vars: OLD/NEW RETURNING wholerow (ExecEvalWholeRowVar
        // 5369-5386 + init flags 3180/3182), resjunk-carrying subquery /
        // materialized-CTE wholerow (junk filter, 3203-3236 + 5393),
        // dropped-column wholerow, and the inheritance child→parent
        // rowtype cast under a USING join (expression_tree_mutator
        // ConvertRowtypeExpr 3260-3265 via flatten_join_alias_vars).
        "einterp:w4:wholerow" => {
            let n = 4 + g.rng.below(5);
            vec![
                raw("CREATE TABLE ei_wr (a int, b text);"),
                raw(format!("INSERT INTO ei_wr SELECT g, 'w' || g FROM generate_series(1, {n}) g;")),
                raw("INSERT INTO ei_wr VALUES (100, 'ins') RETURNING (old)::text, (new)::text;"),
                raw("UPDATE ei_wr SET b = b || '+' WHERE a = 100 RETURNING (old)::text, (new)::text;"),
                raw("DELETE FROM ei_wr WHERE a = 100 RETURNING (old)::text, (new)::text;"),
                raw(format!("SELECT (s.*)::text FROM (SELECT a, b FROM ei_wr ORDER BY a + 1 DESC LIMIT {n}) s;")),
                raw("WITH c AS MATERIALIZED (SELECT a, b FROM ei_wr ORDER BY a + 1 DESC LIMIT 3) SELECT (c.*)::text FROM c;"),
                raw("CREATE TABLE ei_wrd (a int, zap int, b text);"),
                raw("INSERT INTO ei_wrd SELECT g, g * 10, 'd' || g FROM generate_series(1, 4) g;"),
                raw("ALTER TABLE ei_wrd DROP COLUMN zap;"),
                raw("SELECT (d.*)::text FROM ei_wrd d ORDER BY a;"),
                raw("SELECT row_to_json(d) FROM ei_wrd d JOIN ei_wr u USING (a) ORDER BY a;"),
                raw("CREATE TABLE ei_wpar (a int, b text);"),
                raw("CREATE TABLE ei_wchi (extra int) INHERITS (ei_wpar);"),
                raw("INSERT INTO ei_wpar VALUES (1, 'p1'), (2, 'p2');"),
                raw("INSERT INTO ei_wchi VALUES (3, 'c3', 30), (4, 'c4', 40);"),
                raw("SELECT (p.*)::text FROM ei_wpar p ORDER BY a;"),
                raw("SELECT (c.*)::ei_wpar::text FROM ei_wchi c JOIN ei_wr t2 USING (a) ORDER BY 1;"),
                raw("DROP TABLE ei_wchi;"),
                raw("DROP TABLE ei_wpar;"),
                raw("DROP TABLE ei_wrd;"),
                raw("DROP TABLE ei_wr;"),
            ]
        }
        // SQL/JSON returning/behavior residue: datetime()-typed items
        // coerced to string (ExecGetJsonValueItemString 5066-5081),
        // RETURNING jsonb/json/domain (ExecEvalJsonExprPath 4906-4913),
        // EXISTS→int/domain coercion (ExecEvalJsonCoercion 5129-5144),
        // empty-result ON ERROR branch (4989-4997) and the matched
        // coercion/empty error arms (ExecEvalJsonCoercionFinish
        // 5204-5232).
        "einterp:w4:jsonret" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_jr (a int, jb jsonb);"),
                raw(format!("INSERT INTO ei_jr VALUES ({k}, jsonb '{{\"s\": \"str\", \"n\": 42, \"d\": \"2023-03-05\", \"t\": \"12:34:56\", \"tz\": \"12:34:56+05:30\", \"ts\": \"2023-03-05 12:34:56\", \"tstz\": \"2023-03-05 12:34:56+05:30\", \"o\": {{\"k\": 1}}}}');")),
                raw("SELECT JSON_VALUE(jb, '$.d.datetime(\"yyyy-mm-dd\")' RETURNING text), JSON_VALUE(jb, '$.t.datetime(\"HH24:MI:SS\")' RETURNING text), JSON_VALUE(jb, '$.tz.datetime(\"HH24:MI:SSTZH:TZM\")' RETURNING text) FROM ei_jr;"),
                raw("SELECT JSON_VALUE(jb, '$.ts.datetime(\"yyyy-mm-dd HH24:MI:SS\")' RETURNING text), JSON_VALUE(jb, '$.tstz.datetime(\"yyyy-mm-dd HH24:MI:SSTZH:TZM\")' RETURNING text) FROM ei_jr;"),
                raw("SELECT JSON_VALUE(jb, '$.n' RETURNING jsonb), JSON_VALUE(jb, '$.s' RETURNING json), JSON_VALUE(jsonb '[null]', '$[0]' RETURNING text) FROM ei_jr;"),
                raw("CREATE DOMAIN ei_jdom AS jsonb CHECK (VALUE IS NOT NULL);"),
                raw("SELECT JSON_VALUE(jb, '$.n' RETURNING ei_jdom) FROM ei_jr;"),
                raw("CREATE DOMAIN ei_bit AS int CHECK (VALUE IN (0, 1));"),
                raw("SELECT jt.* FROM ei_jr, JSON_TABLE(jb, '$' COLUMNS (ei int EXISTS PATH '$.o.k', ed ei_bit EXISTS PATH '$.nope')) jt;"),
                raw("SELECT JSON_QUERY(jb, '$.o' RETURNING json), JSON_QUERY(jb, '$.o' RETURNING text OMIT QUOTES), JSON_QUERY(jb, '$.s' OMIT QUOTES), JSON_QUERY(jb, '$.n' WITH WRAPPER) FROM ei_jr;"),
                raw("SELECT JSON_VALUE(jb, '$.missing' DEFAULT 'dfl' ON ERROR), JSON_QUERY(jb, '$.missing' EMPTY OBJECT ON ERROR), JSON_VALUE(jb, '$.s' RETURNING int) FROM ei_jr;"),
                raw("SELECT JSON_EXISTS(jb, 'strict $.miss.also' TRUE ON ERROR), JSON_EXISTS(jb, 'strict $.miss.also' UNKNOWN ON ERROR), JSON_EXISTS(jb, 'strict $.miss.also' FALSE ON ERROR) FROM ei_jr;"),
                raw("SELECT JSON_VALUE(jsonb '\"abc\"', '$' RETURNING int ERROR ON ERROR);"),
                raw("SELECT JSON_QUERY(jb, '$.missing' ERROR ON EMPTY ERROR ON ERROR) FROM ei_jr;"),
                raw("DROP DOMAIN ei_bit;"),
                raw("DROP DOMAIN ei_jdom;"),
                raw("DROP TABLE ei_jr;"),
            ]
        }
        // plpgsql assignment statements are planned Queries: the
        // parenthesized (r).f form forces a SQL FieldSelect over the
        // expanded record (ExecEvalFieldSelect 3749-3789); subscripted /
        // field assignments light SubscriptingRef + FieldStore init and
        // walker arms (ExecInitSubscriptingRef, isAssignmentIndirectionExpr,
        // expression_tree_walker 2192/2266).
        "einterp:w4:plassign" => {
            let k = 1 + g.rng.below(7);
            vec![
                raw("CREATE TYPE ei_pct AS (p int, q text);"),
                raw("CREATE FUNCTION ei_ppu(x int) RETURNS int LANGUAGE sql VOLATILE PARALLEL UNSAFE AS 'SELECT x + 0';"),
                raw(format!("DO $$ DECLARE r record; v int; s text; BEGIN SELECT {k} AS p, 'seven' AS q INTO r; v := (r).p; s := (r).q; RAISE NOTICE 'rec % %', v, s; END $$;")),
                raw(format!("DO $$ DECLARE arr int[]; c ei_pct; carr ei_pct[]; BEGIN arr := ARRAY[1, 2, 3]; arr[ei_ppu(2)] := ei_ppu({k}); c := ROW(1, 'one'); c.p := ei_ppu(3); carr := ARRAY[ROW(1, 'a')::ei_pct, ROW(2, 'b')::ei_pct]; carr[ei_ppu(1)].q := 'mut'; carr[2].p := ei_ppu({k}); RAISE NOTICE 'asg % % %', arr, c, carr; END $$;")),
                raw("DROP FUNCTION ei_ppu(int);"),
                raw("DROP TYPE ei_pct;"),
            ]
        }
        // DO ALSO rule rewriting: rewriteRuleAction's NEW substitution is
        // a full query_tree_mutator pass over the action, mutating the
        // CYCLE CTE (expression_tree_mutator CTECycleClause 3504-3510)
        // and the range table (JSON_TABLE / TABLESAMPLE / VALUES /
        // subquery arms of range_table_mutator).
        "einterp:w4:ruledrv" => {
            let k = 1 + g.rng.below(3);
            vec![
                raw("CREATE TABLE ei_rsrc (a int, b text);"),
                raw("CREATE TABLE ei_rlog (tag text, val int);"),
                raw("CREATE RULE ei_rule AS ON INSERT TO ei_rsrc DO ALSO INSERT INTO ei_rlog WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < 2) CYCLE x SET cyc USING pth SELECT 'w' || NEW.b, g.x + sub.z + jt.jv + sum(g.x) OVER (ORDER BY g.x ROWS BETWEEN (1 + 0) PRECEDING AND CURRENT ROW) FROM g, (SELECT max(v.q) AS z FROM (VALUES (1), (2)) v(q)) sub, JSON_TABLE(jsonb '[5]', '$[*]' COLUMNS (jv int PATH '$')) jt WHERE g.x <= NEW.a + jt.jv;"),
                // NOTE: REPEATABLE takes a CONSTANT here, never NEW.* — a
                // rule action with TABLESAMPLE ... REPEATABLE(NEW.a) crashes
                // C REL_18_3 (findings-w4walk-repro-f1.sql); kept out of the
                // generator on purpose (divergence, not a matched shape).
                raw("CREATE RULE ei_rule2 AS ON UPDATE TO ei_rsrc DO ALSO INSERT INTO ei_rlog SELECT min('u' || NEW.b), count(*)::int FROM ei_rsrc TABLESAMPLE BERNOULLI (80) REPEATABLE (7) GROUP BY ei_rsrc.a % (NEW.a + 1) HAVING count(*) >= 0;"),
                raw(format!("INSERT INTO ei_rsrc VALUES ({k}, 'r1'), ({}, 'r2');", k + 1)),
                raw("UPDATE ei_rsrc SET b = b || '+' WHERE a IS NOT NULL;"),
                raw("SELECT count(*) >= 0 FROM ei_rlog;"),
                raw("DROP RULE ei_rule2 ON ei_rsrc;"),
                raw("DROP RULE ei_rule ON ei_rsrc;"),
                raw("DROP TABLE ei_rlog;"),
                raw("DROP TABLE ei_rsrc;"),
            ]
        }
        // Hashed ScalarArrayOp: null-scalar arm (4247) + hashed IN /
        // NOT-IN lists over the minimum size, with and without NULLs.
        "einterp:w4:saop" => {
            let k = 1 + g.rng.below(5);
            vec![
                raw("CREATE TABLE ei_sa (a int, b text);"),
                raw(format!("INSERT INTO ei_sa SELECT g, 'b' || g FROM generate_series(1, {}) g;", 5 + k)),
                raw("INSERT INTO ei_sa VALUES (NULL, NULL);"),
                raw("SELECT a, b NOT IN ('x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8', NULL) FROM ei_sa ORDER BY a NULLS LAST;"),
                raw("SELECT a FROM ei_sa WHERE b IN ('b1', 'b2', 'i3', 'i4', 'i5', 'i6', 'i7', 'i8', 'i9', 'i10', 'i11', NULL) ORDER BY a;"),
                raw("SELECT a FROM ei_sa WHERE b NOT IN ('b1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 'q12') ORDER BY a;"),
                raw("DROP TABLE ei_sa;"),
            ]
        }
        // Multidimensional ArrayExpr over table columns: all-empty
        // subarrays, null-bitmap copy, and the two matched dimension
        // errors (ExecEvalArrayExpr 3469-3601).
        "einterp:w4:arraymd" => {
            let k = 1 + g.rng.below(6);
            vec![
                raw("CREATE TABLE ei_am (a int, ia int[], ea int[]);"),
                raw(format!("INSERT INTO ei_am VALUES (1, ARRAY[{k}, 2], '{{}}'), (2, ARRAY[3, {k}], '{{}}');")),
                raw("SELECT ARRAY[ea, ea] FROM ei_am ORDER BY a;"),
                raw("SELECT ARRAY[ia, ARRAY[NULL::int, a]] FROM ei_am ORDER BY a;"),
                raw("SELECT ARRAY[ARRAY[ia, ia], ARRAY[ia, ARRAY[NULL::int, 9]]] FROM ei_am ORDER BY a;"),
                raw("SELECT ARRAY[ea, ia] FROM ei_am ORDER BY a;"),
                raw("SELECT ARRAY[ia, ARRAY[5, 6, 7]] FROM ei_am ORDER BY a;"),
                raw("DROP TABLE ei_am;"),
            ]
        }
        // exprLocation raw-node error-position battery: every statement
        // errors with parser_errposition on a specific raw node kind; all
        // errors byte-identical on both engines (diff-verified).
        "einterp:w4:errloc" => {
            vec![
                raw("CREATE TABLE ei_el (a int, b text, arr int[]);"),
                raw("SELECT CAST(1 AS ei_no_such_type);"),
                raw("UPDATE ei_el SET nosuchcol = 1;"),
                raw("UPDATE ei_el SET (a, b) = (SELECT 1, 'x', 2.0);"),
                raw("SELECT count(*) OVER ei_nowin FROM ei_el;"),
                raw("SELECT * FROM ei_el TABLESAMPLE ei_nosuchmethod (10);"),
                raw("INSERT INTO ei_el VALUES (1, 'dup', NULL) ON CONFLICT (nosuchcol) DO NOTHING;"),
                raw("INSERT INTO ei_el VALUES (1, 'dup', NULL) ON CONFLICT DO UPDATE SET b = 'x';"),
                raw("WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < 3) SEARCH DEPTH FIRST BY nosuch SET seq SELECT * FROM g;"),
                raw("WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < 3) CYCLE nosuch SET is_c USING pathc SELECT * FROM g;"),
                raw("WITH RECURSIVE g(x) AS (SELECT x + 1 FROM g WHERE x < 3) SELECT * FROM g;"),
                raw("SELECT a FROM ei_el GROUP BY a HAVING ARRAY[a];"),
                // (Constraint 1730 / FunctionParameter 1733 exprLocation
                // arms are error-shaped CREATEs — they live only in the
                // deck, not here, to keep the group's create/drop pairing
                // invariant clean.)
                raw("SELECT CASE WHEN a THEN 1 ELSE 2 END FROM ei_el;"),
                raw("SELECT GREATEST(a, b) FROM ei_el;"),
                raw("SELECT a FROM ei_el WHERE row_number() OVER (ORDER BY a) < 3;"),
                raw("SELECT a IS JSON FROM ei_el;"),
                raw("SELECT JSON_OBJECT('k' VALUE 1, 'k' VALUE 2 WITH UNIQUE KEYS);"),
                raw("SELECT a FROM ei_el WHERE JSON_OBJECTAGG(b VALUE a) IS NOT NULL;"),
                raw("SELECT JSON_ARRAY(SELECT a INTO ei_elnope FROM ei_el);"),
                raw("DROP TABLE ei_el;"),
            ]
        }
        // Collation plumbing + deparse: inputcollid consumers
        // (exprInputCollation 1086-1108) via pg_get_viewdef over collated
        // aggregate/window/minmax/saop shapes; domain-over-collated-text
        // check errors stay matched.
        "einterp:w4:collate" => {
            let k = 1 + g.rng.below(5);
            vec![
                raw("CREATE TABLE ei_cl (a int, b text);"),
                raw(format!("INSERT INTO ei_cl SELECT g, 'b' || g FROM generate_series(1, {}) g;", 4 + k)),
                raw("SELECT b IS DISTINCT FROM 'b2' COLLATE \"C\", NULLIF(b COLLATE \"POSIX\", 'b3'), b < ANY (ARRAY['b4', 'b9'] COLLATE \"C\"), GREATEST(b COLLATE \"C\", 'a'), min(b COLLATE \"C\") OVER (ORDER BY a) FROM ei_cl ORDER BY a;"),
                raw("CREATE VIEW ei_vcoll AS SELECT max(b COLLATE \"C\") AS m, count(*) FILTER (WHERE b IS DISTINCT FROM 'x' COLLATE \"C\") AS f, min(b COLLATE \"POSIX\") OVER (ORDER BY a) AS w, NULLIF(b COLLATE \"C\", 'nn') AS nl, b < ANY (ARRAY['p', 'q'] COLLATE \"C\") AS sa, GREATEST(b COLLATE \"C\", 'gg') AS gr, lower(b COLLATE \"POSIX\") AS lo FROM ei_cl GROUP BY a, b;"),
                raw("SELECT pg_get_viewdef('ei_vcoll'::regclass, true);"),
                raw("SELECT count(*) >= 0 FROM ei_vcoll;"),
                raw("DROP VIEW ei_vcoll;"),
                raw("DROP TABLE ei_cl;"),
            ]
        }
        // Partition DDL with expression bounds + runtime pruning through
        // a prepared statement (generic-plan pruning steps), plus the two
        // matched bound errors (exprLocation PartitionRangeDatum /
        // PartitionElem arms).
        "einterp:w4:partition" => {
            let k = 1 + g.rng.below(6);
            vec![
                raw("CREATE TABLE ei_prt (a int, b text) PARTITION BY RANGE ((a % 100));"),
                raw("CREATE TABLE ei_prt_p1 PARTITION OF ei_prt FOR VALUES FROM (0 + 0) TO (5 * 2);"),
                raw("CREATE TABLE ei_prt_p2 PARTITION OF ei_prt FOR VALUES FROM (10) TO (20 + 5);"),
                raw(format!("INSERT INTO ei_prt SELECT g, 'pt' || g FROM generate_series(1, {}) g;", 15 + k)),
                raw("PREPARE ei_prep (int) AS SELECT count(*) FROM ei_prt WHERE (a % 100) < $1;"),
                raw(format!("EXECUTE ei_prep ({});", 2 + g.rng.below(8))),
                raw("EXECUTE ei_prep (12);"),
                raw("EXECUTE ei_prep (3);"),
                raw("EXECUTE ei_prep (18);"),
                raw("EXECUTE ei_prep (9);"),
                raw(format!("EXECUTE ei_prep ({k});")),
                raw("DEALLOCATE ei_prep;"),
                raw("CREATE TABLE ei_prt_bad PARTITION OF ei_prt FOR VALUES FROM (30) TO (25);"),
                // (the PartitionElem exprLocation arm — CREATE TABLE .. BY
                // RANGE(nosuchcol) — is a standalone error-shaped CREATE;
                // it lives only in the deck to keep create/drop pairing.)
                raw("DROP TABLE ei_prt;"),
            ]
        }
        other => unreachable!("unknown einterp shape {other}"),
    }
}

/// Registry entry point (stmt::STMT_MODULES): one self-contained
/// interpreter/walker drain group.
pub fn gen_einterp_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("einterp");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    body(g, shape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut groups = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            groups.push(
                gen_einterp_module(&mut g).iter().map(|s| s.to_sql()).collect::<Vec<_>>(),
            );
        }
        groups
    }

    /// Statements are single-line and ';'-terminated; every group is
    /// self-contained: ei_-prefixed objects created in a group are dropped
    /// in it, every SET has a matching RESET, BEGIN/COMMIT balance.
    #[test]
    fn groups_are_self_contained() {
        let groups = gen_groups(0x1d2, 600);
        for group in &groups {
            let mut depth = 0i32;
            let mut sets: Vec<String> = Vec::new();
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                if let Some(rest) = sql.strip_prefix("SET ") {
                    sets.push(rest.split_whitespace().next().unwrap().to_string());
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = sets.iter().rposition(|s| *s == name);
                    assert!(pos.is_some(), "RESET without SET: {sql}");
                    sets.remove(pos.unwrap());
                }
                if sql.starts_with("BEGIN") {
                    depth += 1;
                } else if sql.starts_with("COMMIT") || sql.starts_with("ROLLBACK") {
                    depth -= 1;
                }
            }
            assert_eq!(depth, 0, "unbalanced txn bracket in {group:?}");
            assert!(sets.is_empty(), "unreset GUCs {sets:?} in {group:?}");
            // create/drop pairing per object name. Partition children are
            // dropped implicitly with their parent.
            let mut created: Vec<String> = Vec::new();
            let mut partition_of: Vec<(String, String)> = Vec::new();
            for sql in group {
                for (pat, kind) in [
                    ("CREATE TABLE ", "t"),
                    ("CREATE VIEW ", "v"),
                    ("CREATE UNIQUE INDEX ", "i"),
                    ("CREATE INDEX ", "i"),
                    ("CREATE TYPE ", "y"),
                    ("CREATE DOMAIN ", "d"),
                    ("CREATE FUNCTION ", "f"),
                ] {
                    if let Some(rest) = sql.strip_prefix(pat) {
                        let name = rest
                            .split(|c: char| c == ' ' || c == '(')
                            .next()
                            .unwrap()
                            .to_string();
                        if kind != "i" {
                            if let Some(par) = sql
                                .split(" PARTITION OF ")
                                .nth(1)
                                .and_then(|r| r.split_whitespace().next())
                            {
                                partition_of.push((name.clone(), par.to_string()));
                            }
                            created.push(name);
                        }
                    }
                }
                if let Some(rest) = sql
                    .strip_prefix("DROP TABLE ")
                    .or_else(|| sql.strip_prefix("DROP VIEW "))
                    .or_else(|| sql.strip_prefix("DROP TYPE "))
                    .or_else(|| sql.strip_prefix("DROP DOMAIN "))
                    .or_else(|| sql.strip_prefix("DROP FUNCTION "))
                {
                    let name = rest
                        .split(|c: char| c == ';' || c == ' ' || c == '(')
                        .next()
                        .unwrap()
                        .to_string();
                    let pos = created.iter().rposition(|c| *c == name);
                    assert!(pos.is_some(), "DROP without CREATE: {sql}");
                    created.remove(pos.unwrap());
                    // Partition children go with the parent.
                    for (child, parent) in &partition_of {
                        if *parent == name {
                            if let Some(p) = created.iter().rposition(|c| c == child) {
                                created.remove(p);
                            }
                        }
                    }
                }
            }
            assert!(created.is_empty(), "undropped objects {created:?} in {group:?}");
        }
    }

    /// Same seed, same stream — the reproducibility witness.
    #[test]
    fn deterministic_by_seed() {
        assert_eq!(gen_groups(42, 120), gen_groups(42, 120));
    }

    /// Every shape is reachable from the default weight table.
    #[test]
    fn all_shapes_fire() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(7);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            gen_einterp_module(&mut g);
            for p in prods {
                seen.insert(p);
            }
        }
        for shape in SHAPES {
            assert!(seen.contains(*shape), "shape never fired: {shape}");
        }
    }

    /// The raw-walker shapes all carry the WITH RECURSIVE armer.
    #[test]
    fn raw_shapes_carry_recursive_with() {
        let groups = gen_groups(0xeeee, 800);
        for group in &groups {
            let joined = group.join(" ");
            if joined.contains("ei_d ") || joined.contains("XMLTABLE") {
                assert!(
                    joined.contains("WITH RECURSIVE"),
                    "raw shape without WITH RECURSIVE armer: {joined}"
                );
            }
        }
    }
}
