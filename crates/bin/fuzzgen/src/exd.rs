//! EXPLAIN plan-node/option drain module (LD4): line-gap-report-001 ranks
//! `ExplainNode` (287 unhit lines) and the explain.c family
//! (show_modifytable_info 88, show_memoize_info 48, ExplainOneUtility 40,
//! show_hashagg_info 38, show_incremental_sort_info 35, ExplainTargetRel
//! 32, ParseExplainOptionList 26, ExplainPrintSettings 23, ...) as the
//! biggest entered-but-hollow cluster after objectaddress/ruleutils. The
//! standing `explain` module only wraps generic SELECTs with a narrow
//! option set, so only the common plan-node and option arms ever fire.
//! This module force-instantiates the missing PLAN NODES (BitmapAnd/Or,
//! SetOp sorted+hashed, Group, LockRows, ProjectSet, Recursive Union /
//! WorkTable, Tid / Tid Range / Sample / Subquery / Values / Function /
//! Table Function scans, Memoize, Incremental Sort, MergeAppend, Named
//! Tuplestore, MERGE / ON CONFLICT ModifyTable, run-time-pruned Append)
//! and the missing OPTIONS (SETTINGS, GENERIC_PLAN, MEMORY, SERIALIZE
//! text/binary/off, WAL, SUMMARY ON, FORMAT XML) under the comparison
//! discipline below.
//!
//! Every statement family was hand-verified byte-identical on C REL_18_3
//! and pgrust@origin/main before landing (docs/fuzzing/findings-ld4.md;
//! deck: docs/fuzzing/deck-ld4-explain.sql) EXCEPT the three surfaces
//! where pgrust diverges today — those are banked as LD4-F1/F2/F3 in the
//! BUG-LEDGER and kept OUT of this module (they live in the C-side
//! coverage deck only) until the fixes land:
//!   LD4-F1  EXPLAIN ANALYZE INSERT .. ON CONFLICT errors on pgrust
//!           (ntuples2 accounting for Tuples Inserted / Conflicting
//!           Tuples unimplemented; the DO UPDATE .. WHERE spelling also
//!           lacks the conflict-filter show_instrumentation_count);
//!   LD4-F2  tuplestore "Storage:" lines missing under Recursive Union
//!           and Table Function Scan ANALYZE;
//!   LD4-F3  trigger instrumentation lines missing from EXPLAIN ANALYZE
//!           (report_triggers);
//!   LD4-F4  EXPLAIN (SETTINGS) prints every non-default-SOURCE GUC on
//!           pgrust where C prints only values differing from boot_val —
//!           under the rig's C-parity pin B always grows a Settings line.
//!
//! Comparison discipline (mask policy: crate::diff EXPLAIN_COUNTER_TOKENS):
//!   - COSTS OFF and TIMING OFF always. COSTS ON / TIMING ON print
//!     wall-clock or estimate floats inside `( .. )` parens that the
//!     token masker cannot reach — those arms are deck-only.
//!   - SUMMARY ON allowed: "Planning Time"/"Execution Time" values are
//!     masked (tokens), line PRESENCE is deterministic.
//!   - MEMORY allowed: "Memory Used"/"Memory Allocated"/TEXT "Memory:"
//!     values masked.
//!   - WAL allowed under ANALYZE, and only in JSON/YAML (every key prints
//!     unconditionally); values masked. TEXT WAL prints only the nonzero
//!     counters (structure = runtime state) — deck-only.
//!   - BUFFERS: JSON/YAML only, under ANALYZE, values masked (G2 ruling —
//!     TEXT line presence is cache-history state).
//!   - FORMAT XML: non-instrumented EXPLAIN only (no ANALYZE / WAL /
//!     MEMORY / SUMMARY / BUFFERS / SERIALIZE). XML prints counters as
//!     `<Tag>value</Tag>` — un-maskable by the colon-keyed masker — but a
//!     plain plan tree has no counters, so the whole XML formatter
//!     surface (ExplainXMLTag, list/dummy-group/open-close arms) is
//!     compared strictly.
//!   - SERIALIZE (TEXT/JSON/YAML): needs ANALYZE; the "Serialization:"
//!     line stays STRICTLY compared — output kB is a rounded function of
//!     the serialized row bytes, i.e. a wire-format conformance signal
//!     (hand-verified equal both sides for text and binary).
//!   - Memoize "Hits/Misses" stay strictly compared: cache keys per group
//!     are bounded (<=50 distinct over <=4000 inner rows) far below
//!     hash_mem, so eviction/overflow never triggers and the counts are
//!     pure data functions. "Memory Usage" etc. stay masked.
//!   - ANALYZE only ever wraps read-only statements at top level; DML
//!     under ANALYZE is bracketed BEGIN..ROLLBACK in the same group.
//!   - Parallel plans: EXPLAIN of parallel shapes is never ANALYZE here
//!     (worker sections/row splits are launch-time state; Q1 ruling) —
//!     ANALYZE-parallel worker instrumentation is deck-only. Note
//!     LD4-N1: pgrust plans Workers Planned: 3 vs C 2 at *default* GUCs
//!     (probe without the rig pin); under the rig's C-parity pin the
//!     planned count matched throughout.
//!   - every SET has its RESET in the same group; groups create and drop
//!     every `fz_xd`-prefixed object they touch.

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "exd:planshape",
    "exd:scans",
    "exd:joins",
    "exd:opterr",
    "exd:bitmap",
    "exd:setop",
    "exd:modify",
    "exd:exec",
    "exd:utility",
    "exd:formats",
    "exd:partition",
    "exd:namedts",
];

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// Small scan-target table: 300 rows, pk + two secondary indexes, C-collated
/// text. Deterministic pure-integer formulas; ANALYZE'd (n << the 30000-row
/// statistics sample, so both sides see identical stats — Q1 ruling).
fn t300(name: &str) -> Vec<StmtKind> {
    vec![
        raw(format!(
            "CREATE TABLE {name} (pk int PRIMARY KEY, a int, b int, t text COLLATE \"C\");"
        )),
        raw(format!(
            "INSERT INTO {name} SELECT i, (i*13)%50, (i*7)%60, 'x'||((i*23)%97) \
             FROM generate_series(1,300) i;"
        )),
        raw(format!("CREATE INDEX {name}_a ON {name}(a);")),
        raw(format!("CREATE INDEX {name}_b ON {name}(b);")),
        raw(format!("ANALYZE {name};")),
    ]
}

fn drop(name: &str) -> StmtKind {
    raw(format!("DROP TABLE {name};"))
}

/// Tableless plan-shape probes: ProjectSet, Recursive Union + WorkTable +
/// CTE Scan, Values Scan, Function Scan (VERBOSE -> Function Call +
/// ExplainTargetRel function-name arm), Table Function Scan via JSON_TABLE
/// (VERBOSE -> Table Function Call + "json_table" target arm). Plain
/// EXPLAIN only — the ANALYZE variants of Recursive Union / Table Function
/// Scan are LD4-F2 (deck-only until the pgrust storage-line fix lands).
fn planshape(g: &mut Gen) -> Vec<StmtKind> {
    let fmt = pick_fmt(g, "exd:planshape");
    vec![
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT generate_series(1,3);")),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) WITH RECURSIVE r(n) AS \
             (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 10) SELECT n FROM r;"
        )),
        raw("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             WITH c AS MATERIALIZED (SELECT i AS pk FROM generate_series(1,50) i) \
             SELECT count(*) FROM c JOIN c c2 USING (pk);"
            .to_string()),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT * FROM (VALUES (1),(2),(3)) v(x);")),
        raw(format!(
            "EXPLAIN (COSTS OFF, VERBOSE{fmt}) SELECT * FROM generate_series(1,5) gs(i);"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF, VERBOSE{fmt}) SELECT * FROM JSON_TABLE('[1,2]', '$[*]' \
             COLUMNS (v int PATH '$')) jt;"
        )),
    ]
}

/// Scan-variant probes over a 300-row table: Tid Scan, Tid Range Scan,
/// Sample Scan (+ show_tablesample deparse), Subquery Scan, LockRows,
/// backward index scan, show_sortorder_options (COLLATE .. DESC NULLS
/// FIRST, USING <op>), Index Only Scan ANALYZE (Heap Fetches / Index
/// Searches / Rows Removed by Filter — all data-deterministic on the
/// group-fresh table), and GENERIC_PLAN over $1.
fn scans(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_s";
    let mut v = t300(n);
    let fmt = pick_fmt(g, "exd:scans");
    v.extend([
        // Tid / Tid Range: on a 300-row table seq scan outprices tid
        // paths, so the bracket forces them (plus the multi-qual OR arm
        // and the ANALYZE filter counts — data-deterministic).
        raw("SET enable_seqscan = off;"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT * FROM {n} WHERE ctid = '(0,1)';")),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT * FROM {n} \
             WHERE ctid = '(0,1)' OR ctid = '(0,2)';"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT * FROM {n} \
             WHERE ctid > '(0,1)' AND ctid < '(2,0)';"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {n} WHERE ctid > '(0,1)' AND ctid < '(2,0)' AND a = 5;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {n} WHERE (ctid = '(0,1)' OR ctid = '(0,2)') AND a <> 999;"
        )),
        raw("RESET enable_seqscan;"),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT count(*) FROM {n} \
             TABLESAMPLE BERNOULLI (50) REPEATABLE (7);"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF, VERBOSE) SELECT count(*) FROM {n} \
             TABLESAMPLE SYSTEM (40) REPEATABLE (2);"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT * FROM (SELECT pk FROM {n} OFFSET 0) s \
             WHERE pk < 5;"
        )),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT * FROM {n} WHERE pk = 1 FOR UPDATE;")),
        raw(format!("EXPLAIN (COSTS OFF) SELECT * FROM {n} ORDER BY pk DESC LIMIT 3;")),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT * FROM {n} \
             ORDER BY t COLLATE \"C\" DESC NULLS FIRST LIMIT 3;"
        )),
        // Expression key defeats the ordered-index path so a real Sort
        // node prints; the USING spelling prints only for a NON-default
        // ordering operator (text pattern ops — C-collated, verified
        // identical), int `USING >` folds to plain DESC.
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT * FROM {n} ORDER BY (pk + 0) USING > LIMIT 3;"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT * FROM {n} ORDER BY t USING ~<~ LIMIT 3;"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF, FORMAT JSON) SELECT * FROM {n} ORDER BY pk DESC LIMIT 3;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {n} WHERE pk < 100 AND b = 5 ORDER BY pk;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT i FROM generate_series(1,20) g(i) WHERE i % 3 = 0;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT pk FROM {n} WHERE pk < 100 AND pk % 3 = 0;"
        )),
        raw(format!(
            "EXPLAIN (GENERIC_PLAN, COSTS OFF{fmt}) SELECT * FROM {n} WHERE pk = $1;"
        )),
        drop(n),
    ]);
    v
}

/// Semi/anti-join jointype arms (Semi, Anti, Right Semi, Right Anti +
/// "Rows Removed by Join Filter"). The big-outer EXISTS spellings and the
/// nestloop-forced small-outer spellings are hand-verified identical both
/// engines; the HASH small-outer EXISTS is NOT emitted — pgrust picks a
/// semi join where C unique-ifies the inner rel (LD4-F7, plan-shape
/// divergence, banked).
fn joins(_g: &mut Gen) -> Vec<StmtKind> {
    let b = "fz_xd_jb";
    let s = "fz_xd_js";
    vec![
        raw(format!(
            "CREATE TABLE {b} AS SELECT i AS pk, (i*13)%50 AS a FROM generate_series(1,4000) i;"
        )),
        raw(format!(
            "CREATE TABLE {s} AS SELECT i AS pk, (i*3)%50 AS a FROM generate_series(1,40) i;"
        )),
        raw(format!("ANALYZE {b};")),
        raw(format!("ANALYZE {s};")),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {b} \
             WHERE EXISTS (SELECT 1 FROM {s} WHERE {s}.a = {b}.a);"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {b} \
             WHERE NOT EXISTS (SELECT 1 FROM {s} WHERE {s}.a = {b}.a);"
        )),
        raw("SET enable_hashjoin = off;"),
        raw("SET enable_mergejoin = off;"),
        raw("SET enable_memoize = off;"),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {s} \
             WHERE EXISTS (SELECT 1 FROM {b} WHERE {b}.a = {s}.a);"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT count(*) FROM {s} \
             WHERE NOT EXISTS (SELECT 1 FROM {b} WHERE {b}.a = {s}.a AND {b}.pk > 100);"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT count(*) FROM {s} x JOIN {b} y ON y.a = x.a AND y.pk + x.pk > 10;"
        )),
        raw("RESET enable_memoize;"),
        raw("RESET enable_mergejoin;"),
        raw("RESET enable_hashjoin;"),
        drop(b),
        drop(s),
    ]
}

/// Bitmap machinery over a 20k-row two-index table: BitmapAnd, BitmapOr,
/// and the ANALYZE Bitmap Heap Scan ("Heap Blocks: exact=NN" — page count
/// of a group-fresh bulk-loaded table, deterministic both sides).
fn bitmap(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_ba";
    let fmt = pick_fmt(g, "exd:bitmap");
    vec![
        raw(format!(
            "CREATE TABLE {n} AS SELECT i AS pk, (i*13)%50 AS a, (i*17)%60 AS c \
             FROM generate_series(1,20000) i;"
        )),
        raw(format!("CREATE INDEX {n}_a ON {n}(a);")),
        raw(format!("CREATE INDEX {n}_c ON {n}(c);")),
        raw(format!("ANALYZE {n};")),
        raw("SET enable_seqscan = off;"),
        raw("SET enable_indexscan = off;"),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT count(*) FROM {n} WHERE a = 5 AND c = 7;"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT count(*) FROM {n} WHERE a = 5 OR c = 7;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT count(*) FROM {n} WHERE a = 5 AND c = 7;"
        )),
        raw("RESET enable_indexscan;"),
        raw("RESET enable_seqscan;"),
        drop(n),
    ]
}

/// SetOp (all four commands, hashed + sorted strategies) and the Group
/// node, plain + ANALYZE (HashSetOp / SetOp / Group counters are row
/// counts — deterministic).
fn setop(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_so";
    let mut v = t300(n);
    let fmt = pick_fmt(g, "exd:setop");
    let cmdp = g.weights.pick(
        g.rng,
        &[
            "exd:setop:intersect",
            "exd:setop:intersectall",
            "exd:setop:except",
            "exd:setop:exceptall",
        ],
    );
    g.fire(cmdp);
    let cmd = match cmdp {
        "exd:setop:intersectall" => "INTERSECT ALL",
        "exd:setop:except" => "EXCEPT",
        "exd:setop:exceptall" => "EXCEPT ALL",
        _ => "INTERSECT",
    };
    v.extend([
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT a FROM {n} {cmd} SELECT b FROM {n};"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT a FROM {n} {cmd} SELECT b FROM {n};"
        )),
        raw("SET enable_hashagg = off;"),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT a FROM {n} {cmd} SELECT b FROM {n};"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT a FROM {n} {cmd} SELECT b FROM {n};"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) SELECT a FROM {n} GROUP BY a HAVING a > 1;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT a FROM {n} GROUP BY a HAVING a > 1;"
        )),
        raw("RESET enable_hashagg;"),
        drop(n),
    ]);
    v
}

/// ModifyTable detail arms (show_modifytable_info): MERGE with all four
/// tuple paths (inserted/updated/deleted/skipped — data-deterministic
/// counts, strictly compared), INSERT .. ON CONFLICT DO NOTHING / DO
/// UPDATE (Conflict Resolution / Arbiter Indexes / Tuples Inserted /
/// Conflicting Tuples), and WAL under JSON (values masked). The DO UPDATE
/// .. WHERE conflict-filter ANALYZE arm is LD4-F1 (deck-only). All
/// ANALYZE-DML is bracketed BEGIN..ROLLBACK.
fn modify(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_m";
    let mut v = vec![
        raw(format!("CREATE TABLE {n} (pk int PRIMARY KEY, a int, v int);")),
        raw(format!(
            "INSERT INTO {n} SELECT i, (i*3)%50, i FROM generate_series(1,300) i;"
        )),
        raw(format!("ANALYZE {n};")),
    ];
    let fmt = pick_fmt(g, "exd:modify");
    v.extend([
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) MERGE INTO {n} d \
             USING (SELECT i AS pk, i%7 AS a FROM generate_series(1,400) i) s \
             ON d.pk = s.pk \
             WHEN MATCHED THEN UPDATE SET v = s.a \
             WHEN NOT MATCHED THEN INSERT VALUES (s.pk, s.a, 0);"
        )),
        raw("BEGIN;"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             MERGE INTO {n} d \
             USING (SELECT i AS pk, i%7 AS a FROM generate_series(1,400) i) s \
             ON d.pk = s.pk \
             WHEN MATCHED AND s.pk < 100 THEN UPDATE SET v = s.a \
             WHEN MATCHED AND s.pk < 200 THEN DELETE \
             WHEN NOT MATCHED AND s.pk < 350 THEN INSERT VALUES (s.pk, s.a, 0);"
        )),
        raw("ROLLBACK;"),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) INSERT INTO {n} VALUES (1,1,1) \
             ON CONFLICT (pk) DO NOTHING;"
        )),
        // NOTE (LD4-F1, broadened): EXPLAIN ANALYZE over ANY ON CONFLICT
        // arm errors on pgrust today ("Tuples Inserted/Conflicting Tuples
        // need ntuples2 accounting") — the ANALYZE ON CONFLICT probes are
        // deck-only until the fix lands; plain EXPLAIN (above) still
        // covers the Conflict Resolution / Arbiter Indexes arms.
        raw("BEGIN;"),
        raw(format!(
            "EXPLAIN (ANALYZE, WAL, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, \
             FORMAT JSON) INSERT INTO {n} SELECT i, 1, 1 FROM generate_series(1000,1010) i;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, \
             FORMAT JSON) MERGE INTO {n} d \
             USING (SELECT i AS pk, i%7 AS a FROM generate_series(1,400) i) s \
             ON d.pk = s.pk \
             WHEN MATCHED AND s.pk < 100 THEN UPDATE SET v = s.a \
             WHEN MATCHED AND s.pk < 200 THEN DELETE \
             WHEN NOT MATCHED AND s.pk < 350 THEN INSERT VALUES (s.pk, s.a, 0);"
        )),
        raw("ROLLBACK;"),
        raw("BEGIN;"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             UPDATE {n} SET v = v + 1 WHERE pk <= 10;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             DELETE FROM {n} WHERE pk > 290;"
        )),
        raw("ROLLBACK;"),
        drop(n),
    ]);
    v
}

/// Executor-detail show_* arms under GUC brackets: Memoize (plain +
/// ANALYZE — Hits/Misses strictly compared, memory masked), HashAgg spill
/// (Batches/Disk Usage masked), Incremental Sort (Full-sort / Pre-sorted
/// group info; method+memory tail masked via "Sort Method"), Sort disk
/// spill ("Sort Method: external merge" tail masked).
fn exec_details(_g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_e";
    let mut v = t300(n);
    // Bigger inner side for memoize/spill: 4000 rows, 50 distinct a.
    let b = "fz_xd_eb";
    v.extend([
        raw(format!(
            "CREATE TABLE {b} AS SELECT i AS pk, (i*13)%50 AS a, (i*7)%4000 AS bb, \
             'x'||(i%97) AS t FROM generate_series(1,4000) i;"
        )),
        raw(format!("CREATE INDEX {b}_a ON {b}(a);")),
        raw(format!("ANALYZE {b};")),
        raw("SET enable_hashjoin = off;"),
        raw("SET enable_mergejoin = off;"),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {n} x JOIN {b} y ON y.a = x.a;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT count(*) FROM {n} x JOIN {b} y ON y.a = x.a;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, \
             FORMAT JSON) SELECT count(*) FROM {n} x JOIN {b} y ON y.a = x.a;"
        )),
        raw("RESET enable_mergejoin;"),
        raw("RESET enable_hashjoin;"),
        raw("SET enable_indexonlyscan = off;"),
        raw("SET enable_indexscan = off;"),
        raw("SET enable_sort = off;"),
        raw("SET work_mem = '64kB';"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT bb, count(*) FROM {b} GROUP BY bb;"
        )),
        raw("RESET work_mem;"),
        raw("RESET enable_sort;"),
        raw("RESET enable_indexscan;"),
        raw("RESET enable_indexonlyscan;"),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT * FROM {b} ORDER BY a, bb LIMIT 10;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {b} ORDER BY a, bb LIMIT 10;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, \
             FORMAT JSON) SELECT * FROM {b} ORDER BY a, bb LIMIT 10;"
        )),
        // Upper-qual + Result-qual instrumentation counts (Group HAVING
        // filter, Result Filter under a volatile-but-always-true qual —
        // row outputs stay deterministic).
        raw("SET enable_hashagg = off;"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT a FROM {n} GROUP BY a HAVING sum(pk) > 50;"
        )),
        raw("RESET enable_hashagg;"),
        raw("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT 1 WHERE random() < 2;"),
        // Spilled CTE tuplestore ("Storage: Disk", masked value-wise) and
        // sorted grouping-sets keys (show_grouping_set_keys sorted path).
        raw("SET work_mem = '64kB';"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             WITH c AS MATERIALIZED (SELECT pk, t FROM {b}) \
             SELECT count(*) FROM c JOIN c c2 USING (pk);"
        )),
        raw("RESET work_mem;"),
        raw("SET enable_hashagg = off;"),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT t, count(*) FROM {n} GROUP BY ROLLUP (t);"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT t, count(*) FROM {n} GROUP BY ROLLUP (t);"
        )),
        raw("RESET enable_hashagg;"),
        raw("SET work_mem = '64kB';"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {b} ORDER BY t;"
        )),
        raw("RESET work_mem;"),
        drop(b),
        drop(n),
    ]);
    v
}

/// ExplainOneUtility + ExplainExecuteQuery + rewrite arms: EXPLAIN CREATE
/// TABLE AS / CREATE MATERIALIZED VIEW / IF NOT EXISTS short-circuit,
/// DECLARE CURSOR, EXECUTE (+ CREATE TABLE AS EXECUTE), rule rewrites
/// ("Query rewrites to nothing", NOTIFY, DO ALSO multi-plan separator),
/// SETTINGS, MEMORY, SERIALIZE, SUMMARY ON.
fn utility(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_u";
    let mut v = t300(n);
    let fmt = pick_fmt(g, "exd:utility");
    // MEMORY / SUMMARY print masked counters (colon-keyed masker): they
    // ride TEXT/JSON/YAML but never XML (module header).
    let mfmt = if fmt == ", FORMAT XML" { "" } else { fmt };
    v.extend([
        raw(format!("EXPLAIN (COSTS OFF{fmt}) CREATE TABLE fz_xd_ct AS SELECT * FROM {n};")),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) CREATE MATERIALIZED VIEW fz_xd_mv AS SELECT a FROM {n};"
        )),
        raw("CREATE TABLE fz_xd_ct AS SELECT 1 AS x;"),
        raw(format!(
            "EXPLAIN (COSTS OFF{fmt}) CREATE TABLE IF NOT EXISTS fz_xd_ct AS SELECT * FROM {n};"
        )),
        raw("DROP TABLE fz_xd_ct;"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) DECLARE fz_xd_cur CURSOR FOR SELECT * FROM {n};")),
        raw("CREATE TABLE fz_xd_rt (x int);"),
        raw("CREATE RULE fz_xd_r1 AS ON INSERT TO fz_xd_rt DO INSTEAD NOTHING;"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) INSERT INTO fz_xd_rt VALUES (1);")),
        raw("DROP RULE fz_xd_r1 ON fz_xd_rt;"),
        raw("CREATE RULE fz_xd_r2 AS ON INSERT TO fz_xd_rt DO INSTEAD NOTIFY fz_xd_chan;"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) INSERT INTO fz_xd_rt VALUES (1);")),
        raw("DROP RULE fz_xd_r2 ON fz_xd_rt;"),
        raw("CREATE TABLE fz_xd_rl (x int);"),
        raw("CREATE RULE fz_xd_r3 AS ON INSERT TO fz_xd_rt DO ALSO \
             INSERT INTO fz_xd_rl VALUES (new.x);"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) INSERT INTO fz_xd_rt VALUES (1);")),
        raw("DROP TABLE fz_xd_rt, fz_xd_rl;"),
        raw(format!("PREPARE fz_xd_p(int) AS SELECT * FROM {n} WHERE pk = $1;")),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) EXECUTE fz_xd_p(3);")),
        raw("EXPLAIN (COSTS OFF) CREATE TABLE fz_xd_ce AS EXECUTE fz_xd_p(3);"),
        raw("DEALLOCATE fz_xd_p;"),
        // SETTINGS is deck-only (LD4-F4): C's get_explain_guc_options
        // prints only GUCs whose VALUE differs from boot_val (guc.c
        // "options that are different from their boot values"), while
        // pgrust prints every non-default-SOURCE GUC — under the rig's
        // C-parity pin (explicit SETs to C defaults) B grows a Settings
        // line A never prints. Re-enable when the fix lands.
        raw(format!(
            "EXPLAIN (COSTS OFF, MEMORY, SUMMARY OFF{mfmt}) SELECT * FROM {n} WHERE pk = 1;"
        )),
        raw(format!(
            "EXPLAIN (SUMMARY ON, COSTS OFF{mfmt}) SELECT * FROM {n} WHERE pk = 1;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, SERIALIZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {n} WHERE pk < 5;"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, SERIALIZE BINARY, COSTS OFF, TIMING OFF, SUMMARY OFF, \
             BUFFERS OFF) SELECT * FROM {n} WHERE pk < 5;"
        )),
        raw("EXPLAIN (ANALYZE, SERIALIZE OFF, COSTS OFF, TIMING OFF, SUMMARY OFF, \
             BUFFERS OFF) SELECT 1;"),
        raw(format!(
            "EXPLAIN (ANALYZE, SERIALIZE TEXT, COSTS OFF, TIMING OFF, SUMMARY OFF, \
             BUFFERS OFF) SELECT 1;"
        )),
        raw("EXPLAIN (COSTS OFF, FORMAT TEXT) SELECT 1;"),
        // ExplainOneUtility long tail: matview IF NOT EXISTS
        // short-circuit, REFRESH ("Utility Statement" fallback arm),
        // ANALYZE CTAS WITH NO DATA (NoMovement direction).
        raw(format!("CREATE MATERIALIZED VIEW fz_xd_umv AS SELECT a FROM {n};")),
        raw(format!(
            "EXPLAIN (COSTS OFF{mfmt}) CREATE MATERIALIZED VIEW IF NOT EXISTS fz_xd_umv \
             AS SELECT a FROM {n};"
        )),
        raw(format!("EXPLAIN (COSTS OFF{mfmt}) REFRESH MATERIALIZED VIEW fz_xd_umv;")),
        raw("DROP MATERIALIZED VIEW fz_xd_umv;"),
        raw("BEGIN;"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             CREATE TABLE fz_xd_und AS SELECT * FROM {n} WITH NO DATA;"
        )),
        raw("ROLLBACK;"),
        // ExplainExecuteQuery option arms (MEMORY / ANALYZE+BUFFERS).
        raw(format!("PREPARE fz_xd_pq(int) AS SELECT * FROM {n} WHERE pk = $1;")),
        raw("EXPLAIN (MEMORY, COSTS OFF, SUMMARY OFF) EXECUTE fz_xd_pq(1);"),
        raw("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF, \
             FORMAT JSON) EXECUTE fz_xd_pq(1);"),
        raw("DEALLOCATE fz_xd_pq;"),
        drop(n),
    ]);
    v
}

/// ParseExplainOptionList error arms: every option-validation ereport as a
/// matched-error statement (hand-verified identical SQLSTATE + message on
/// both engines).
fn opterr(_g: &mut Gen) -> Vec<StmtKind> {
    vec![
        raw("EXPLAIN (WRONG_OPTION) SELECT 1;"),
        raw("EXPLAIN (FORMAT bogus) SELECT 1;"),
        raw("EXPLAIN (SERIALIZE bogus) SELECT 1;"),
        raw("EXPLAIN (TIMING ON) SELECT 1;"),
        raw("EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT $1::int;"),
        raw("EXPLAIN (SERIALIZE) SELECT 1;"),
    ]
}

/// Format-matrix probes: one canned query pushed through every format arm
/// of explain_format.c. XML rides only non-instrumented EXPLAIN (see
/// module header); JSON/YAML additionally carry the ANALYZE+BUFFERS
/// masked-counter ride (show_buffer_usage JSON/YAML key structure).
fn formats(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_f";
    let mut v = t300(n);
    let ana = g.weights.pick(g.rng, &["exd:formats:json", "exd:formats:yaml"]);
    let afmt = if ana == "exd:formats:json" { "JSON" } else { "YAML" };
    g.fire(ana);
    v.extend([
        raw(format!(
            "EXPLAIN (COSTS OFF, FORMAT XML) SELECT * FROM {n} WHERE pk = 1;"
        )),
        // ExplainPropertyList XML (<Item> arms) via Group Key under a
        // sorted Group node; DummyGroup XML via utility.
        raw("SET enable_hashagg = off;"),
        raw(format!(
            "EXPLAIN (COSTS OFF, FORMAT XML) SELECT a FROM {n} GROUP BY a ORDER BY a;"
        )),
        raw("RESET enable_hashagg;"),
        raw(format!("EXPLAIN (COSTS OFF, FORMAT XML) CREATE TABLE fz_xd_fx AS SELECT 1;")),
        raw(format!("EXPLAIN (COSTS OFF, VERBOSE, FORMAT XML) SELECT pk, a FROM {n} LIMIT 1;")),
        raw(format!("EXPLAIN (COSTS OFF, FORMAT YAML) CREATE TABLE fz_xd_fx AS SELECT 1;")),
        raw(format!("EXPLAIN (COSTS OFF, FORMAT JSON) CREATE TABLE fz_xd_fx AS SELECT 1;")),
        raw(format!(
            "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT {afmt}) \
             SELECT count(*) FROM {n};"
        )),
        raw(format!(
            "EXPLAIN (ANALYZE, WAL, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, \
             FORMAT {afmt}) SELECT count(*) FROM {n};"
        )),
        // Grouping-sets key lists ride ExplainPropertyListNested (the
        // YAML flow-sequence arm is otherwise dead).
        raw(format!(
            "EXPLAIN (COSTS OFF, FORMAT YAML) SELECT a, count(*) FROM {n} \
             GROUP BY GROUPING SETS ((a), ());"
        )),
        raw(format!(
            "EXPLAIN (COSTS OFF, FORMAT JSON) SELECT a, count(*) FROM {n} \
             GROUP BY GROUPING SETS ((a), (b), ());"
        )),
        drop(n),
    ]);
    v
}

/// Partitioned-table arms: Append plan-time + run-time pruning ("Subplans
/// Removed" under a forced generic plan — deterministic), MergeAppend
/// (per-part ordered indexes), GENERIC_PLAN over $1, and the
/// plan_is_disabled MergeAppend/Append child-accounting arms via
/// enable_sort/enable_seqscan brackets.
fn partition(g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_p";
    let fmt = pick_fmt(g, "exd:partition");
    vec![
        raw(format!("CREATE TABLE {n} (pk int, a int) PARTITION BY RANGE (pk);")),
        raw(format!(
            "CREATE TABLE {n}1 PARTITION OF {n} FOR VALUES FROM (0) TO (100);"
        )),
        raw(format!(
            "CREATE TABLE {n}2 PARTITION OF {n} FOR VALUES FROM (100) TO (200);"
        )),
        raw(format!(
            "INSERT INTO {n} SELECT i, i%7 FROM generate_series(0,199) i;"
        )),
        raw(format!("CREATE INDEX ON {n}1(pk);")),
        raw(format!("CREATE INDEX ON {n}2(pk);")),
        raw(format!("CREATE INDEX ON {n}1(a);")),
        raw(format!("CREATE INDEX ON {n}2(a);")),
        raw(format!("ANALYZE {n};")),
        // MergeAppend: ORDER BY the NON-partition key over per-part
        // (a)-indexes — ordered Append is invalid, each child feeds
        // sorted (show_merge_append_keys, ExplainPreScanNode/
        // ExplainMemberNodes MergeAppend arms).
        raw("SET enable_seqscan = off;"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT * FROM {n} ORDER BY a LIMIT 5;")),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             SELECT * FROM {n} ORDER BY a LIMIT 5;"
        )),
        raw("RESET enable_seqscan;"),
        // Multi-target ModifyTable (per-target labeling arms). TEXT only:
        // the structured-format Target Tables group asserts
        // FORMAT_TEXT on pgrust today (LD4-F5, deck-only).
        raw(format!("EXPLAIN (COSTS OFF) UPDATE {n} SET a = a + 1;")),
        raw(format!("EXPLAIN (COSTS OFF) DELETE FROM {n} WHERE a = 3;")),
        raw("BEGIN;"),
        raw(format!(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             UPDATE {n} SET a = a + 1 WHERE pk < 150;"
        )),
        raw("ROLLBACK;"),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT count(*) FROM {n} WHERE pk < 150;")),
        raw(format!("EXPLAIN (COSTS OFF{fmt}) SELECT * FROM {n} ORDER BY pk LIMIT 5;")),
        raw(format!(
            "EXPLAIN (GENERIC_PLAN, COSTS OFF{fmt}) SELECT * FROM {n} WHERE pk = $1;"
        )),
        raw("SET plan_cache_mode = force_generic_plan;"),
        raw(format!(
            "PREPARE fz_xd_pp(int) AS SELECT count(*) FROM {n} WHERE pk = $1;"
        )),
        raw("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) \
             EXECUTE fz_xd_pp(150);"),
        raw("DEALLOCATE fz_xd_pp;"),
        raw("RESET plan_cache_mode;"),
        raw("SET enable_sort = off;"),
        raw(format!("EXPLAIN (COSTS OFF) SELECT * FROM {n} ORDER BY pk LIMIT 5;")),
        raw("RESET enable_sort;"),
        raw("SET enable_seqscan = off;"),
        raw(format!("EXPLAIN (COSTS OFF) SELECT count(*) FROM {n}1;")),
        raw("RESET enable_seqscan;"),
        // Disabled-child accounting arms of plan_is_disabled: with every
        // scan type disabled the Append/MergeAppend/SubqueryScan parents
        // sum nonzero child disabled_nodes.
        raw("SET enable_seqscan = off;"),
        raw("SET enable_indexscan = off;"),
        raw("SET enable_bitmapscan = off;"),
        raw("SET enable_indexonlyscan = off;"),
        raw(format!("EXPLAIN (COSTS OFF) SELECT count(*) FROM {n} WHERE a = 3;")),
        raw(format!("EXPLAIN (COSTS OFF) SELECT * FROM {n} ORDER BY a LIMIT 3;")),
        raw(format!(
            "EXPLAIN (COSTS OFF) SELECT * FROM (SELECT pk FROM {n}1 OFFSET 0) s \
             WHERE pk < 5;"
        )),
        raw("RESET enable_indexonlyscan;"),
        raw("RESET enable_bitmapscan;"),
        raw("RESET enable_indexscan;"),
        raw("RESET enable_seqscan;"),
        raw(format!("DROP TABLE {n};")),
    ]
}

/// Named Tuplestore Scan (ExplainNode + ExplainTargetRel tuplestore arms):
/// an AFTER trigger with a transition table runs EXPLAIN over the ENR via
/// SPI. The trigger swallows the plan rows (no NOTICE — notice traffic is
/// not a compared surface); the statement outcomes are.
fn namedts(_g: &mut Gen) -> Vec<StmtKind> {
    let n = "fz_xd_nt";
    vec![
        raw(format!("CREATE TABLE {n} (pk int PRIMARY KEY, v int);")),
        raw(format!(
            "INSERT INTO {n} SELECT i, i FROM generate_series(1,20) i;"
        )),
        raw(format!(
            "CREATE FUNCTION fz_xd_ntf() RETURNS trigger LANGUAGE plpgsql AS $fzxd$ \
             DECLARE r record; c int := 0; BEGIN \
             FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) SELECT count(*) FROM nt' LOOP \
             c := c + 1; END LOOP; RETURN NULL; END $fzxd$;"
        )),
        raw(format!(
            "CREATE TRIGGER fz_xd_ntt AFTER UPDATE ON {n} \
             REFERENCING NEW TABLE AS nt FOR EACH STATEMENT \
             EXECUTE FUNCTION fz_xd_ntf();"
        )),
        raw("BEGIN;"),
        raw(format!("UPDATE {n} SET v = v + 1 WHERE pk <= 3;")),
        raw("ROLLBACK;"),
        raw(format!("DROP TABLE {n};")),
        raw("DROP FUNCTION fz_xd_ntf;"),
    ]
}

/// Format rider for plain (non-instrumented) EXPLAIN statements: TEXT
/// (spelled as nothing), JSON, YAML, XML. Instrumented statements never
/// consult this — their format set is arm-local (see module header).
fn pick_fmt(g: &mut Gen, _arm: &str) -> &'static str {
    let f = g.weights.pick(
        g.rng,
        &["exd:fmt:text", "exd:fmt:json", "exd:fmt:yaml", "exd:fmt:xml"],
    );
    g.fire(f);
    match f {
        "exd:fmt:json" => ", FORMAT JSON",
        "exd:fmt:yaml" => ", FORMAT YAML",
        "exd:fmt:xml" => ", FORMAT XML",
        _ => "",
    }
}

/// Registry entry point (stmt::STMT_MODULES): one EXPLAIN-drain group.
pub fn gen_exd_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("exd");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    match shape {
        "exd:planshape" => planshape(g),
        "exd:scans" => scans(g),
        "exd:joins" => joins(g),
        "exd:opterr" => opterr(g),
        "exd:bitmap" => bitmap(g),
        "exd:setop" => setop(g),
        "exd:modify" => modify(g),
        "exd:exec" => exec_details(g),
        "exd:utility" => utility(g),
        "exd:formats" => formats(g),
        "exd:partition" => partition(g),
        "exd:namedts" => namedts(g),
        other => unreachable!("unknown exd shape {other}"),
    }
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
                gen_exd_module(&mut g)
                    .iter()
                    .map(|s| s.to_sql())
                    .collect::<Vec<_>>(),
            );
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    fn explains(groups: &[Vec<String>]) -> Vec<String> {
        groups
            .iter()
            .flatten()
            .filter(|s| s.trim_start().to_ascii_uppercase().starts_with("EXPLAIN"))
            .cloned()
            .collect()
    }

    #[test]
    fn comparison_discipline_holds() {
        let (groups, prods) = gen_groups(0x1D4, 400, &WeightTable::defaults());
        // The opterr arm exists to ERROR identically on both engines —
        // its statements never produce output and are exempt from the
        // output-comparison discipline below.
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(0);
        let mut eprods = Vec::new();
        let w = WeightTable::defaults();
        let mut eg = Gen::new(&mut rng, &cat, &w, &mut eprods, 3);
        let err_stmts: Vec<String> = opterr(&mut eg).iter().map(|s| s.to_sql()).collect();
        for sql in explains(&groups) {
            if err_stmts.contains(&sql) {
                continue;
            }
            let up = sql.to_ascii_uppercase();
            // Cost estimates and per-node wall clock are never compared.
            assert!(up.contains("COSTS OFF"), "{sql}");
            assert!(!up.contains("TIMING ON"), "{sql}");
            if up.contains("ANALYZE") {
                assert!(up.contains("TIMING OFF"), "{sql}");
                // BUFFERS: bare (masked, deterministic key structure) only
                // on JSON/YAML; TEXT/XML spell BUFFERS OFF (G2).
                if !up.contains("BUFFERS OFF") {
                    assert!(
                        up.contains("BUFFERS") && (up.contains("FORMAT JSON") || up.contains("FORMAT YAML")),
                        "bare BUFFERS outside JSON/YAML: {sql}"
                    );
                }
                // XML cannot mask counters: never instrumented.
                assert!(!up.contains("FORMAT XML"), "ANALYZE under XML: {sql}");
            } else {
                assert!(!up.contains("BUFFERS"), "{sql}");
                assert!(!up.contains(" WAL"), "{sql}");
            }
            // WAL rides masked formats only.
            if up.contains(" WAL,") || up.contains(", WAL") {
                assert!(
                    up.contains("FORMAT JSON") || up.contains("FORMAT YAML"),
                    "WAL outside JSON/YAML: {sql}"
                );
            }
            // XML arms carry no instrumented/masked-only options at all.
            if up.contains("FORMAT XML") {
                for opt in ["ANALYZE", "MEMORY", "SUMMARY ON", " WAL", "SERIALIZE", "BUFFERS"] {
                    assert!(!up.contains(opt), "XML with {opt}: {sql}");
                }
            }
        }
        // Bracket + GUC hygiene: per group, BEGIN/ROLLBACK pair up and
        // every SET has a RESET.
        for grp in &groups {
            let mut depth = 0i32;
            let mut sets: Vec<String> = Vec::new();
            for s in grp {
                let up = s.trim().to_ascii_uppercase();
                if up == "BEGIN;" {
                    depth += 1;
                } else if up == "ROLLBACK;" {
                    depth -= 1;
                } else if let Some(rest) = up.strip_prefix("SET ") {
                    let name = rest.split([' ', '=']).next().unwrap().to_string();
                    if name != "LOCAL" {
                        sets.push(name);
                    }
                } else if let Some(rest) = up.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    if let Some(pos) = sets.iter().position(|s| *s == name) {
                        sets.remove(pos);
                    }
                }
                assert!(depth >= 0, "unbalanced ROLLBACK in group");
            }
            assert_eq!(depth, 0, "unclosed BEGIN in group: {grp:?}");
            assert!(sets.is_empty(), "un-RESET GUCs {sets:?} in group: {grp:?}");
        }
        // ANALYZE over DML only inside brackets.
        for grp in &groups {
            let mut depth = 0i32;
            for s in grp {
                let up = s.trim().to_ascii_uppercase();
                if up == "BEGIN;" {
                    depth += 1;
                } else if up == "ROLLBACK;" {
                    depth -= 1;
                } else if up.starts_with("EXPLAIN") && up.contains("ANALYZE") {
                    let dml = ["INSERT ", "UPDATE ", "DELETE ", "MERGE "]
                        .iter()
                        .any(|k| up.split_once(") ").map_or(false, |(_, t)| t.starts_with(k)));
                    if dml {
                        assert!(depth > 0, "unbracketed ANALYZE DML: {s}");
                    }
                }
            }
        }
        // Every arm production fires across the sweep.
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        for p in ["exd:fmt:text", "exd:fmt:json", "exd:fmt:yaml", "exd:fmt:xml"] {
            assert!(prods.iter().any(|q| q == p), "format {p} never fired");
        }
    }

    #[test]
    fn ld4_known_divergences_stay_out() {
        // LD4-F1/F2/F3 surfaces are deck-only until the pgrust fixes land:
        // no ANALYZE over ON CONFLICT .. WHERE, no ANALYZE recursive-CTE /
        // JSON_TABLE (storage lines), no ANALYZE on trigger-bearing tables
        // (the namedts trigger fires under a plain UPDATE, not EXPLAIN
        // ANALYZE).
        let (groups, _) = gen_groups(9, 400, &WeightTable::defaults());
        for sql in explains(&groups) {
            let up = sql.to_ascii_uppercase();
            if up.contains("ANALYZE") {
                assert!(!up.contains("ON CONFLICT"), "LD4-F1 surface: {sql}");
                assert!(!up.contains("WITH RECURSIVE"), "LD4-F2 surface: {sql}");
                assert!(!up.contains("JSON_TABLE"), "LD4-F2 surface: {sql}");
            }
            assert!(!up.contains("SETTINGS"), "LD4-F4 surface: {sql}");
        }
    }

    #[test]
    fn exd_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(42, 60, &w);
        let (b, _) = gen_groups(42, 60, &w);
        assert_eq!(a, b);
        let (c, _) = gen_groups(43, 60, &w);
        assert_ne!(a, c);
    }
}
