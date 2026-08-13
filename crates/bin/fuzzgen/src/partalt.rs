//! Partition-pruning / partitionwise ALT-PATH drain, W5-PART (line-drain
//! queue chunks `partitioning-arms` + the partition rows of
//! `optimizer-arms`/`executor-residue`; see docs/fuzzing/line-drain-queue.md).
//! The standing modules cover the surface's trunk: `part` owns the
//! partition DDL lifecycle, `exr:prune` owns generic-plan runtime pruning
//! over its fixed tree, `plansel`/`opt2` sweep partitionwise profiles over
//! IDENTICALLY-bounded range twins. What stayed dark is the ALT-PATH
//! residue this module force-engages, per a fresh read of REL_18_3
//! partprune.c / partbounds.c / joinrels.c / relnode.c / pathnode.c:
//!
//!   - match_clause_to_partition_key alternate clause forms: BOOLEAN
//!     partition keys (match_boolean_partition_clause IS [NOT] TRUE/FALSE
//!     inversion arms + IS [NOT] UNKNOWN nullness translation), commuted
//!     comparisons (literal op partkey), `<>` handled via the negator
//!     (list partitioning only), cross-type comparison procs (int4 key
//!     probed with int2/int8 literals -> get_opfamily_proc cross-type
//!     BTORDER/HASHEXTENDED lookups), SAOP arms (= ANY over int4[] and
//!     cross-type int8[] element types, <> ALL as the negated-saop list
//!     arm, NOT (key = x)), NullTest arms, and clause shapes that must be
//!     REJECTED (keys on both sides, volatile args, whole-row) without
//!     wrecking the result;
//!   - gen_prune_steps_from_opexps / get_steps_using_prefix multi-column
//!     range-key ladders (full-key eq, prefix-only, prefix eq + suffix
//!     range, contradictions) and get_matching_{list,range,hash}_bounds
//!     edge arms (empty IN pools, all-pruned, DEFAULT-only survivors,
//!     NULL routing);
//!   - runtime pruning residue: generic-plan initial pruning with SAOP
//!     params (= ANY($1)), NULL params (all-pruned), boolean-key params,
//!     multi-column prefix params, initplan params (`a < (SELECT ...)`),
//!     and per-rescan exec pruning via nestloop param joins driven into
//!     MULTI-LEVEL trees (hierarchical PartitionedRelPruneInfo,
//!     InitExecPartitionPruneContexts / ExecFindMatchingSubPlans);
//!   - multi-level partitioning throughout: a RANGE tree whose children
//!     are LIST- and HASH-subpartitioned, exercised by two-level static +
//!     runtime pruning, two-level tuple routing (INSERT + key-moving
//!     UPDATE across sublevels inside BEGIN..ROLLBACK), and top-level-only
//!     co-partitioning (the partitionwise join's child-join rels are then
//!     themselves partitioned -> Append/MergeAppend inner sides for
//!     reparameterize_path_by_child's T_AppendPath/T_MergeAppendPath arms);
//!   - partitionwise join bound MERGING (compute_partition_bounds ->
//!     partition_bounds_merge / merge_list_bounds / merge_range_bounds):
//!     co-partitioned pairs whose bounds do NOT match exactly (staggered
//!     range splits, differently-grouped list values, missing partitions,
//!     one-sided DEFAULTs) under inner/left/full pwise joins;
//!   - partitionwise aggregation arms (create_partitionwise_grouping_paths
//!     FULL vs PARTIAL): GROUP BY the partition key, a superset, the key
//!     expression, and a non-key column, swept with
//!     enable_partitionwise_aggregate x enable_hashagg;
//!   - reparameterize_path_by_child residue: bitmap-combine inner sides
//!     (BitmapOr from OR quals, BitmapAnd from two single-column indexes)
//!     under partitionwise nestloops with seq/index/indexonly scans off,
//!     plus a forced-parallel pwise sweep (T_GatherPath /
//!     T_GatherMergePath arms priced by zeroed parallel costs);
//!   - satisfies_hash_partition (partbounds.c:4783+, 43 hollow lines):
//!     the SQL-callable hash-routing checker's success matrix (matched and
//!     unmatched remainders, NULL values, cross-type coercion of variadic
//!     "any" args) and its ereport arms (bogus modulus/remainder, wrong
//!     arg count, non-hash parent, mismatched arg type) — plus
//!     pg_get_partition_constraintdef / pg_get_partkeydef deparse probes.
//!
//! Correctness bar (LD7/opt2 law): the RESULT SET of every deterministic
//! query is identical across every forced-plan profile and across both
//! engines — any divergence is a HIGH finding. No EXPLAIN anywhere;
//! coverage comes from planning + executing the real queries.
//!
//! Discipline (opt2 rules): self-contained groups over `fz_pa_*` fixtures
//! created and dropped in the same group; every SET has its RESET in
//! reverse order in-group; writes only inside BEGIN..ROLLBACK; every
//! row-returning statement carries a TOTAL order (pk-bearing ORDER BY);
//! everything else is exact-typed aggregates (no float aggregates, B1);
//! tables <= 1200 rows so ANALYZE samples exhaustively (identical stats,
//! identical plans both sides); pk columns are unique by construction
//! (generate_series) — declared PRIMARY KEY only where the partition key
//! permits it.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// A named GUC profile (opt2's shape; local copy keeps modules independent).
#[derive(Clone, Copy)]
struct Prof {
    name: &'static str,
    gucs: &'static [(&'static str, &'static str)],
}

const DEFAULTP: Prof = Prof { name: "default", gucs: &[] };
/// Pruning disabled: every partition is scanned — the ground truth the
/// pruned plans must reproduce byte-for-byte.
const PRUNE_OFF: Prof = Prof {
    name: "prune_off",
    gucs: &[("enable_partition_pruning", "off")],
};
/// Legacy constraint-exclusion route (relation_excluded_by_constraints
/// over the partition constraint, pruning off).
const CEX_PART: Prof = Prof {
    name: "cex_part",
    gucs: &[
        ("enable_partition_pruning", "off"),
        ("constraint_exclusion", "partition"),
    ],
};
const PWISE: Prof = Prof {
    name: "pwise",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
    ],
};
const PWISE_NESTL: Prof = Prof {
    name: "pwise_nestl",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
    ],
};
const PWISE_MERGEA: Prof = Prof {
    name: "pwise_mergea",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_nestloop", "off"),
    ],
};
/// Bitmap-combine inner sides under a partitionwise nestloop: with plain
/// scans off, the parameterized inner path per child partition is a
/// BitmapHeapPath over BitmapOr/BitmapAnd inputs —
/// reparameterize_path_by_child's T_BitmapHeapPath/T_BitmapOrPath/
/// T_BitmapAndPath arms.
const PWISE_BMP: Prof = Prof {
    name: "pwise_bmp",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
    ],
};
/// Forced-parallel partitionwise: prices Gather/GatherMerge paths into
/// child joins (T_GatherPath/T_GatherMergePath reparameterization arms;
/// parallel Append over child joins otherwise).
const PWISE_PAR: Prof = Prof {
    name: "pwise_par",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
        ("parallel_setup_cost", "0"),
        ("parallel_tuple_cost", "0"),
        ("min_parallel_table_scan_size", "0"),
        ("min_parallel_index_scan_size", "0"),
        ("max_parallel_workers_per_gather", "2"),
    ],
};
const NESTL: Prof = Prof {
    name: "nestl",
    gucs: &[("enable_hashjoin", "off"), ("enable_mergejoin", "off")],
};
const PWAGG_HASH_OFF: Prof = Prof {
    name: "pwagg_hash_off",
    gucs: &[
        ("enable_partitionwise_aggregate", "on"),
        ("enable_partitionwise_join", "on"),
        ("enable_hashagg", "off"),
    ],
};
const PWAGG_SORT_OFF: Prof = Prof {
    name: "pwagg_sort_off",
    gucs: &[
        ("enable_partitionwise_aggregate", "on"),
        ("enable_partitionwise_join", "on"),
        ("enable_sort", "off"),
    ],
};

/// SET/RESET bracket (RESETs reversed) around `body`, one group.
fn bracket(p: &Prof, body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut v: Vec<StmtKind> =
        p.gucs.iter().map(|(n, x)| raw(format!("SET {n} = {x};"))).collect();
    v.extend(body);
    for (n, _) in p.gucs.iter().rev() {
        v.push(raw(format!("RESET {n};")));
    }
    v
}

/// Emit every query under every profile (short lists, profile-specific
/// arms — the sweep is exhaustive on purpose, like opt2's).
fn sweep(g: &mut Gen, profs: &[Prof], queries: &[String]) -> Vec<StmtKind> {
    let mut v = Vec::new();
    for p in profs {
        g.fire2("partalt:prof:", p.name);
        v.extend(bracket(p, queries.iter().map(|q| raw(q.clone())).collect()));
    }
    v
}

const SHAPES: &[&str] = &[
    "partalt:clauses",
    "partalt:runtime",
    "partalt:multilevel",
    "partalt:pwjmerge",
    "partalt:pwagg",
    "partalt:reparam",
    "partalt:hashfn",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_partalt_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("partalt");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire2("partalt:shape:", &shape["partalt:".len()..]);
    match shape {
        "partalt:clauses" => gen_clauses(g),
        "partalt:runtime" => gen_runtime(g),
        "partalt:multilevel" => gen_multilevel(g),
        "partalt:pwjmerge" => gen_pwjmerge(g),
        "partalt:pwagg" => gen_pwagg(g),
        "partalt:reparam" => gen_reparam(g),
        _ => gen_hashfn(g),
    }
}

// ------------------------------------------------------- prune fixtures ---

/// Single-level pruning zoo: range, list (multi-valued groups + NULL),
/// boolean-keyed list, multi-column range, hash. Key columns are nullable
/// where a DEFAULT/NULL bound catches the misses; `pk` is unique by
/// construction everywhere and the total-order column of every
/// row-returning probe.
fn prune_zoo() -> Vec<StmtKind> {
    raws(&[
        // RANGE (a): finite ladder + DEFAULT (catches NULLs + both tails).
        "CREATE TABLE fz_pa_r (pk int4 NOT NULL, a int4, v int4) PARTITION BY RANGE (a);",
        "CREATE TABLE fz_pa_r_0 PARTITION OF fz_pa_r FOR VALUES FROM (0) TO (25);",
        "CREATE TABLE fz_pa_r_1 PARTITION OF fz_pa_r FOR VALUES FROM (25) TO (50);",
        "CREATE TABLE fz_pa_r_2 PARTITION OF fz_pa_r FOR VALUES FROM (50) TO (75);",
        "CREATE TABLE fz_pa_r_3 PARTITION OF fz_pa_r FOR VALUES FROM (75) TO (100);",
        "CREATE TABLE fz_pa_r_def PARTITION OF fz_pa_r DEFAULT;",
        "INSERT INTO fz_pa_r SELECT i, CASE WHEN i % 11 = 0 THEN NULL \
         ELSE (i * 7) % 110 END, i FROM generate_series(1, 600) i;",
        "ANALYZE fz_pa_r;",
        // LIST (c): multi-valued chunks, NULL listed, DEFAULT for residue.
        "CREATE TABLE fz_pa_l (pk int4 NOT NULL, c int4, v int4) PARTITION BY LIST (c);",
        "CREATE TABLE fz_pa_l_a PARTITION OF fz_pa_l FOR VALUES IN (1, 2, 3);",
        "CREATE TABLE fz_pa_l_b PARTITION OF fz_pa_l FOR VALUES IN (4, 5, NULL);",
        "CREATE TABLE fz_pa_l_c PARTITION OF fz_pa_l FOR VALUES IN (6, 7, 8, 9);",
        "CREATE TABLE fz_pa_l_def PARTITION OF fz_pa_l DEFAULT;",
        "INSERT INTO fz_pa_l SELECT i, CASE WHEN i % 13 = 0 THEN NULL \
         ELSE (i * 5) % 12 END, -i FROM generate_series(1, 500) i;",
        "ANALYZE fz_pa_l;",
        // LIST (flag) boolean key: the match_boolean_partition_clause fuel.
        "CREATE TABLE fz_pa_b (pk int4 NOT NULL, flag bool, v int4) PARTITION BY LIST (flag);",
        "CREATE TABLE fz_pa_b_t PARTITION OF fz_pa_b FOR VALUES IN (true);",
        "CREATE TABLE fz_pa_b_f PARTITION OF fz_pa_b FOR VALUES IN (false);",
        "CREATE TABLE fz_pa_b_def PARTITION OF fz_pa_b DEFAULT;",
        "INSERT INTO fz_pa_b SELECT i, CASE i % 3 WHEN 0 THEN true \
         WHEN 1 THEN false ELSE NULL END, i * 2 FROM generate_series(1, 300) i;",
        "ANALYZE fz_pa_b;",
        // RANGE (a, b) multi-column key: prefix-step fuel.
        "CREATE TABLE fz_pa_m (pk int4 NOT NULL, a int4, b int4, v int4) \
         PARTITION BY RANGE (a, b);",
        "CREATE TABLE fz_pa_m_0 PARTITION OF fz_pa_m FOR VALUES FROM (0, 0) TO (5, 50);",
        "CREATE TABLE fz_pa_m_1 PARTITION OF fz_pa_m FOR VALUES FROM (5, 50) TO (10, MINVALUE);",
        "CREATE TABLE fz_pa_m_2 PARTITION OF fz_pa_m FOR VALUES FROM (10, MINVALUE) TO (10, 60);",
        "CREATE TABLE fz_pa_m_3 PARTITION OF fz_pa_m FOR VALUES FROM (10, 60) TO (MAXVALUE, MAXVALUE);",
        "CREATE TABLE fz_pa_m_def PARTITION OF fz_pa_m DEFAULT;",
        "INSERT INTO fz_pa_m SELECT i, (i * 3) % 14, (i * 11) % 100, i \
         FROM generate_series(1, 500) i;",
        "ANALYZE fz_pa_m;",
        // HASH (h): nullable hash key, all remainders present.
        "CREATE TABLE fz_pa_h (pk int4 NOT NULL, h int4, v int4) PARTITION BY HASH (h);",
        "CREATE TABLE fz_pa_h_0 PARTITION OF fz_pa_h FOR VALUES WITH (MODULUS 4, REMAINDER 0);",
        "CREATE TABLE fz_pa_h_1 PARTITION OF fz_pa_h FOR VALUES WITH (MODULUS 4, REMAINDER 1);",
        "CREATE TABLE fz_pa_h_2 PARTITION OF fz_pa_h FOR VALUES WITH (MODULUS 4, REMAINDER 2);",
        "CREATE TABLE fz_pa_h_3 PARTITION OF fz_pa_h FOR VALUES WITH (MODULUS 4, REMAINDER 3);",
        "INSERT INTO fz_pa_h SELECT i, CASE WHEN i % 17 = 0 THEN NULL \
         ELSE (i * 13) % 200 END, i FROM generate_series(1, 400) i;",
        "ANALYZE fz_pa_h;",
    ])
}

fn drop_prune_zoo() -> Vec<StmtKind> {
    raws(&["DROP TABLE fz_pa_r, fz_pa_l, fz_pa_b, fz_pa_m, fz_pa_h;"])
}

// ------------------------------------------------- static clause matrix ---

fn gen_clauses(g: &mut Gen) -> Vec<StmtKind> {
    let s = 1 + g.rng.below(90) as i64; // range probe point
    let c = g.rng.below(12) as i64; // list probe value
    let mut v = prune_zoo();
    let queries: Vec<String> = vec![
        // --- range key: plain, commuted, cross-type, saop, contradiction.
        format!("SELECT pk, a FROM fz_pa_r WHERE a = {s} ORDER BY pk;"),
        format!("SELECT pk FROM fz_pa_r WHERE {s} = a ORDER BY pk;"), // commuted
        format!("SELECT pk FROM fz_pa_r WHERE {s} > a AND a >= 0 ORDER BY pk;"),
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a = {s}::int2;"),
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a < {s}::int8;"),
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a BETWEEN {s} AND {};", s + 26),
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a = ANY (ARRAY[{s}, {}, 205]);", s + 25),
        // Cross-type SAOP element type (int8[] over the int4 key).
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a = ANY (ARRAY[{s}, 60]::int8[]);"),
        "SELECT count(*)::int8 FROM fz_pa_r WHERE a IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_pa_r WHERE a IS NOT NULL AND a < 25;".into(),
        // Contradictions (whole-scan pruned; result must be empty/zero).
        "SELECT count(*)::int8 FROM fz_pa_r WHERE a < 10 AND a > 90;".into(),
        "SELECT count(*)::int8 FROM fz_pa_r WHERE a IS NULL AND a = 5;".into(),
        // OR arms (union of pruned sets) + a non-key OR disabling pruning.
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a < 10 OR a >= {};", 100 - s % 20),
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a = {s} OR v = 3;"),
        // Rejected shapes (UNSUPPORTED walks) that still run correctly.
        "SELECT count(*)::int8 FROM fz_pa_r t WHERE a = v;".into(),
        "SELECT count(*)::int8 FROM fz_pa_r WHERE a + 0 = 30;".into(),
        // --- list key: <>, NOT, <> ALL, saop, null-listed routing.
        format!("SELECT pk, c FROM fz_pa_l WHERE c = {c} ORDER BY pk;"),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE c <> {c};"),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE NOT (c = {c});"),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE c <> ALL (ARRAY[{c}, 4, 7]);"),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE c IN (1, 5, {c});"),
        "SELECT count(*)::int8 FROM fz_pa_l WHERE c = ANY ('{2,6,11}'::int4[]);".into(),
        "SELECT count(*)::int8 FROM fz_pa_l WHERE c IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_pa_l WHERE c IS NOT NULL;".into(),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE c = {c} AND c IS NULL;"),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE c < 4 OR c IS NULL;"),
        format!("SELECT count(*)::int8 FROM fz_pa_l WHERE c = {c}::int8;"),
        // --- boolean key: the full BooleanTest matrix.
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE NOT flag;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag = true;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag = false;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS TRUE;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS NOT TRUE;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS FALSE;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS NOT FALSE;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS UNKNOWN;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS NOT UNKNOWN;".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag IS NULL;".into(),
        "SELECT pk FROM fz_pa_b WHERE flag IS NOT TRUE ORDER BY pk LIMIT 20;".into(),
        // --- multi-column range key: prefix steps.
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a = 10 AND b = 60;".into(),
        format!("SELECT count(*)::int8 FROM fz_pa_m WHERE a = {} AND b < 55;", s % 14),
        format!("SELECT pk FROM fz_pa_m WHERE a = {} ORDER BY pk;", s % 14),
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a < 5;".into(),
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a <= 10 AND b >= 60;".into(),
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a > 10 OR (a = 10 AND b >= 60);".into(),
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a = 5 AND b IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_pa_m WHERE b = 30;".into(), // suffix-only: no prune
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a = 7 AND b = 20 AND a < 3;".into(),
        // --- hash key: eq / saop / null; only eq-family prunes hash.
        format!("SELECT pk FROM fz_pa_h WHERE h = {s} ORDER BY pk;"),
        format!("SELECT count(*)::int8 FROM fz_pa_h WHERE h = {s}::int2;"),
        format!("SELECT count(*)::int8 FROM fz_pa_h WHERE h IN ({s}, 42, 137);"),
        "SELECT count(*)::int8 FROM fz_pa_h WHERE h IS NULL;".into(),
        format!("SELECT count(*)::int8 FROM fz_pa_h WHERE h <> {s};"), // no prune (hash)
        format!("SELECT count(*)::int8 FROM fz_pa_h WHERE h < {s};"),  // no prune (hash)
    ];
    v.extend(sweep(g, &[DEFAULTP, PRUNE_OFF, CEX_PART], &queries));
    v.extend(drop_prune_zoo());
    v
}

// ------------------------------------------------------ runtime pruning ---

/// Generic-plan initial pruning + nestloop exec pruning over the zoo.
/// Every EXECUTE battery also runs under force_custom_plan so the pruned
/// generic plan's results are checked against re-planned ground truth.
fn gen_runtime(g: &mut Gen) -> Vec<StmtKind> {
    let s = 1 + g.rng.below(90) as i64;
    let mut v = prune_zoo();
    for mode in ["force_generic_plan", "force_custom_plan"] {
        g.fire2("partalt:cachemode:", mode);
        let mut body: Vec<StmtKind> = vec![
            raw(format!("SET plan_cache_mode = {mode};")),
            // Range key: param eq (one survivor), NULL (ALL pruned),
            // out-of-ladder (DEFAULT survivor).
            raw("PREPARE fz_pa_ps1 (int4) AS SELECT pk, a FROM fz_pa_r WHERE a = $1 ORDER BY pk;"),
            raw(format!("EXECUTE fz_pa_ps1({s});")),
            raw("EXECUTE fz_pa_ps1(NULL);"),
            raw("EXECUTE fz_pa_ps1(2000);"),
            raw("EXECUTE fz_pa_ps1(0);"),
            // SAOP param: = ANY($1) incl. empty and NULL arrays.
            raw("PREPARE fz_pa_ps2 (int4[]) AS SELECT count(*)::int8 FROM fz_pa_l WHERE c = ANY ($1);"),
            raw(format!("EXECUTE fz_pa_ps2(ARRAY[{s} % 12, 5]);")),
            raw("EXECUTE fz_pa_ps2('{}'::int4[]);"),
            raw("EXECUTE fz_pa_ps2(NULL);"),
            raw("EXECUTE fz_pa_ps2(ARRAY[1, NULL, 7]);"),
            // Boolean-key param.
            raw("PREPARE fz_pa_ps3 (bool) AS SELECT count(*)::int8 FROM fz_pa_b WHERE flag = $1;"),
            raw("EXECUTE fz_pa_ps3(true);"),
            raw("EXECUTE fz_pa_ps3(false);"),
            raw("EXECUTE fz_pa_ps3(NULL);"),
            // Multi-column prefix params.
            raw("PREPARE fz_pa_ps4 (int4, int4) AS SELECT count(*)::int8 FROM fz_pa_m \
                 WHERE a = $1 AND b < $2;"),
            raw(format!("EXECUTE fz_pa_ps4({}, 55);", s % 14)),
            raw("EXECUTE fz_pa_ps4(10, 60);"),
            raw("EXECUTE fz_pa_ps4(NULL, 10);"),
            // Hash-key param.
            raw("PREPARE fz_pa_ps5 (int4) AS SELECT count(*)::int8 FROM fz_pa_h WHERE h = $1;"),
            raw(format!("EXECUTE fz_pa_ps5({s});")),
            raw("EXECUTE fz_pa_ps5(NULL);"),
            // Range param twice (BETWEEN $1 AND $2 -> two initial steps).
            raw("PREPARE fz_pa_ps6 (int4, int4) AS SELECT count(*)::int8 FROM fz_pa_r \
                 WHERE a BETWEEN $1 AND $2;"),
            raw(format!("EXECUTE fz_pa_ps6({s}, {});", s + 30)),
            raw(format!("EXECUTE fz_pa_ps6({}, {s});", s + 30)), // inverted: all pruned
            raw("DEALLOCATE fz_pa_ps1;"),
            raw("DEALLOCATE fz_pa_ps2;"),
            raw("DEALLOCATE fz_pa_ps3;"),
            raw("DEALLOCATE fz_pa_ps4;"),
            raw("DEALLOCATE fz_pa_ps5;"),
            raw("DEALLOCATE fz_pa_ps6;"),
            raw("RESET plan_cache_mode;"),
        ];
        v.append(&mut body);
    }
    // Initplan param: `a < (SELECT ...)` prunes at executor startup
    // (PARTTARGET_EXEC initial pruning without a generic plan).
    let queries: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_pa_r WHERE a < (SELECT 26);".into(),
        format!("SELECT count(*)::int8 FROM fz_pa_r WHERE a = (SELECT {s});"),
        "SELECT count(*)::int8 FROM fz_pa_l WHERE c = (SELECT max(x) FROM (VALUES (2), (5)) t(x));"
            .into(),
        "SELECT count(*)::int8 FROM fz_pa_m WHERE a = (SELECT 10) AND b < (SELECT 61);".into(),
        "SELECT count(*)::int8 FROM fz_pa_h WHERE h = (SELECT 42);".into(),
        "SELECT count(*)::int8 FROM fz_pa_b WHERE flag = (SELECT true);".into(),
        // Exec-param (nestloop) pruning: per-outer-row re-pruning of the
        // partitioned inner. VALUES drivers keep rescan counts tiny.
        "SELECT count(*)::int8 FROM (VALUES (3), (30), (77), (NULL::int4)) d(x) \
         JOIN fz_pa_r p ON p.a = d.x;"
            .into(),
        "SELECT d.x, count(p.pk)::int8 FROM (VALUES (1), (5), (11), (200)) d(x) \
         LEFT JOIN fz_pa_l p ON p.c = d.x GROUP BY d.x ORDER BY d.x;"
            .into(),
        "SELECT count(*)::int8 FROM (VALUES (true), (false)) d(x) JOIN fz_pa_b p ON p.flag = d.x;"
            .into(),
        "SELECT count(*)::int8 FROM (VALUES (2, 10), (10, 60), (13, 99)) d(x, y) \
         JOIN fz_pa_m p ON p.a = d.x AND p.b = d.y;"
            .into(),
        "SELECT count(*)::int8 FROM (VALUES (7), (42), (NULL::int4)) d(x) \
         JOIN fz_pa_h p ON p.h = d.x;"
            .into(),
        // Mixed exec + static steps on the same scan.
        "SELECT count(*)::int8 FROM (VALUES (3), (30)) d(x) \
         JOIN fz_pa_r p ON p.a = d.x AND p.a < 50;"
            .into(),
    ];
    v.extend(sweep(g, &[NESTL, PRUNE_OFF, DEFAULTP], &queries));
    v.extend(drop_prune_zoo());
    v
}

// ------------------------------------------------------------ multilevel --

/// Two-level tree: RANGE (a) -> {LIST (c) sub-tree, HASH (pk) sub-tree,
/// leaf, DEFAULT}. Hierarchical static + runtime pruning, two-level tuple
/// routing, ordered per-partition scans.
fn multilevel_fixture() -> Vec<StmtKind> {
    raws(&[
        "CREATE TABLE fz_pa_ml (pk int4 NOT NULL, a int4 NOT NULL, c int4, v int4) \
         PARTITION BY RANGE (a);",
        // Sub-tree 1: LIST (c) with its own DEFAULT.
        "CREATE TABLE fz_pa_ml_p0 PARTITION OF fz_pa_ml FOR VALUES FROM (0) TO (40) \
         PARTITION BY LIST (c);",
        "CREATE TABLE fz_pa_ml_p0_a PARTITION OF fz_pa_ml_p0 FOR VALUES IN (0, 1, 2);",
        "CREATE TABLE fz_pa_ml_p0_b PARTITION OF fz_pa_ml_p0 FOR VALUES IN (3, 4, NULL);",
        "CREATE TABLE fz_pa_ml_p0_def PARTITION OF fz_pa_ml_p0 DEFAULT;",
        // Sub-tree 2: HASH (pk).
        "CREATE TABLE fz_pa_ml_p1 PARTITION OF fz_pa_ml FOR VALUES FROM (40) TO (80) \
         PARTITION BY HASH (pk);",
        "CREATE TABLE fz_pa_ml_p1_h0 PARTITION OF fz_pa_ml_p1 FOR VALUES WITH (MODULUS 2, REMAINDER 0);",
        "CREATE TABLE fz_pa_ml_p1_h1 PARTITION OF fz_pa_ml_p1 FOR VALUES WITH (MODULUS 2, REMAINDER 1);",
        // Plain leaf + top-level DEFAULT.
        "CREATE TABLE fz_pa_ml_p2 PARTITION OF fz_pa_ml FOR VALUES FROM (80) TO (120);",
        "CREATE TABLE fz_pa_ml_def PARTITION OF fz_pa_ml DEFAULT;",
        "CREATE INDEX ON fz_pa_ml (a);",
        "CREATE INDEX ON fz_pa_ml (v);",
        "INSERT INTO fz_pa_ml SELECT i, (i * 7) % 130, CASE WHEN i % 9 = 0 THEN NULL \
         ELSE (i * 3) % 7 END, i FROM generate_series(1, 700) i;",
        "ANALYZE fz_pa_ml;",
    ])
}

fn gen_multilevel(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.rng.below(130) as i64;
    let mut v = multilevel_fixture();
    let queries: Vec<String> = vec![
        // Two-level static pruning: top range step + sub-list/sub-hash step.
        format!("SELECT count(*)::int8 FROM fz_pa_ml WHERE a = {s};"),
        format!("SELECT pk, a, c FROM fz_pa_ml WHERE a < 40 AND c = 2 ORDER BY pk;"),
        "SELECT count(*)::int8 FROM fz_pa_ml WHERE a < 40 AND c IS NULL;".into(),
        format!("SELECT count(*)::int8 FROM fz_pa_ml WHERE a >= 40 AND a < 80 AND pk = {};", 1 + s),
        "SELECT count(*)::int8 FROM fz_pa_ml WHERE a BETWEEN 30 AND 90;".into(),
        "SELECT count(*)::int8 FROM fz_pa_ml WHERE a IN (5, 45, 85, 200);".into(),
        "SELECT count(*)::int8 FROM fz_pa_ml WHERE a < 40 AND c <> 3;".into(),
        "SELECT count(*)::int8 FROM fz_pa_ml WHERE (a < 20 AND c = 1) OR (a >= 90 AND a < 110);"
            .into(),
        // Ordered scan over the tree (Append-order vs MergeAppend arms).
        "SELECT pk, a FROM fz_pa_ml WHERE a BETWEEN 10 AND 70 ORDER BY a, pk LIMIT 30;".into(),
        "SELECT a FROM fz_pa_ml ORDER BY a DESC, pk LIMIT 15;".into(),
        // Aggregate down the tree.
        "SELECT a % 10, count(*)::int8 FROM fz_pa_ml GROUP BY 1 ORDER BY 1;".into(),
        // UNION ALL over sibling subtrees (appendrel flattening).
        "SELECT count(*)::int8 FROM (SELECT pk FROM fz_pa_ml_p0 UNION ALL \
         SELECT pk FROM fz_pa_ml_p1) u;"
            .into(),
        // Direct sub-parent scans (RelationBuildPartitionDesc on sub-trees).
        "SELECT count(*)::int8 FROM fz_pa_ml_p0 WHERE c = 4;".into(),
        "SELECT count(*)::int8 FROM fz_pa_ml_p1 WHERE pk % 2 = 0;".into(),
        "SELECT count(*)::int8 FROM ONLY fz_pa_ml;".into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, PRUNE_OFF, NESTL], &queries));
    // Generic-plan runtime pruning down BOTH levels + exec-param joins.
    let mut rt: Vec<StmtKind> = vec![
        raw("SET plan_cache_mode = force_generic_plan;"),
        raw("PREPARE fz_pa_mlp (int4, int4) AS SELECT count(*)::int8 FROM fz_pa_ml \
             WHERE a = $1 AND c = $2;"),
        raw(format!("EXECUTE fz_pa_mlp({}, 1);", s % 40)),
        raw("EXECUTE fz_pa_mlp(45, 3);"), // hash subtree: c is no key there
        raw("EXECUTE fz_pa_mlp(NULL, NULL);"),
        raw("PREPARE fz_pa_mlq (int4) AS SELECT pk FROM fz_pa_ml WHERE a = $1 ORDER BY pk;"),
        raw(format!("EXECUTE fz_pa_mlq({s});")),
        raw("EXECUTE fz_pa_mlq(300);"),
        raw("DEALLOCATE fz_pa_mlp;"),
        raw("DEALLOCATE fz_pa_mlq;"),
        raw("RESET plan_cache_mode;"),
    ];
    v.append(&mut rt);
    v.extend(bracket(
        &NESTL,
        vec![raw(
            "SELECT count(*)::int8 FROM (VALUES (3), (45), (85), (125)) d(x) \
             JOIN fz_pa_ml p ON p.a = d.x;",
        )],
    ));
    // Two-level routing: INSERT through both levels; key-moving UPDATEs
    // across sublevels and across top-level partitions, rolled back.
    let mut dml: Vec<StmtKind> = vec![
        raw("BEGIN;"),
        raw("INSERT INTO fz_pa_ml VALUES (10001, 5, 1, 0), (10002, 45, NULL, 0), \
             (10003, 85, 9, 0), (10004, 500, 2, 0), (10005, 12, 99, 0);"),
        // list-sub -> list-sub-default (c move), stays in p0.
        raw("UPDATE fz_pa_ml SET c = 6 WHERE pk = 10001;"),
        // p0 -> p1 (a move: list subtree to hash subtree).
        raw("UPDATE fz_pa_ml SET a = 50 WHERE pk = 10001;"),
        // p1 -> top DEFAULT.
        raw("UPDATE fz_pa_ml SET a = 400 WHERE pk = 10002;"),
        // DEFAULT -> plain leaf.
        raw("UPDATE fz_pa_ml SET a = 90 WHERE pk = 10004;"),
        raw("SELECT pk, a, c FROM fz_pa_ml WHERE pk > 10000 ORDER BY pk;"),
        raw("DELETE FROM fz_pa_ml WHERE pk > 10000 AND a < 60;"),
        raw("SELECT count(*)::int8 FROM fz_pa_ml WHERE pk > 10000;"),
        raw("ROLLBACK;"),
    ];
    v.append(&mut dml);
    v.push(raw("DROP TABLE fz_pa_ml;"));
    v
}

// ------------------------------------------------- pwise bound merging ----

/// Co-partitioned pairs with bounds that do NOT match exactly: the
/// partitionwise join must go through partition_bounds_merge
/// (merge_range_bounds / merge_list_bounds) instead of
/// partition_bounds_equal. Identity bar: pwise on == pwise off.
fn gen_pwjmerge(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        // Range pair, staggered splits (0|30|60|90 vs 0|45|90) + one-sided
        // DEFAULT.
        "CREATE TABLE fz_pa_j1 (pk int4 NOT NULL, k int4, v int4) PARTITION BY RANGE (k);",
        "CREATE TABLE fz_pa_j1_0 PARTITION OF fz_pa_j1 FOR VALUES FROM (0) TO (30);",
        "CREATE TABLE fz_pa_j1_1 PARTITION OF fz_pa_j1 FOR VALUES FROM (30) TO (60);",
        "CREATE TABLE fz_pa_j1_2 PARTITION OF fz_pa_j1 FOR VALUES FROM (60) TO (90);",
        "CREATE TABLE fz_pa_j2 (pk int4 NOT NULL, k int4, v int4) PARTITION BY RANGE (k);",
        "CREATE TABLE fz_pa_j2_0 PARTITION OF fz_pa_j2 FOR VALUES FROM (0) TO (45);",
        "CREATE TABLE fz_pa_j2_1 PARTITION OF fz_pa_j2 FOR VALUES FROM (45) TO (90);",
        "CREATE TABLE fz_pa_j2_def PARTITION OF fz_pa_j2 DEFAULT;",
        "INSERT INTO fz_pa_j1 SELECT i, (i * 7) % 90, i FROM generate_series(1, 400) i;",
        "INSERT INTO fz_pa_j2 SELECT i, (i * 11) % 120, -i FROM generate_series(1, 300) i;",
        "ANALYZE fz_pa_j1;",
        "ANALYZE fz_pa_j2;",
        // List pair, differently grouped values; j4 misses value 9 and has
        // a DEFAULT.
        "CREATE TABLE fz_pa_j3 (pk int4 NOT NULL, k int4, v int4) PARTITION BY LIST (k);",
        "CREATE TABLE fz_pa_j3_a PARTITION OF fz_pa_j3 FOR VALUES IN (0, 1, 2, 3);",
        "CREATE TABLE fz_pa_j3_b PARTITION OF fz_pa_j3 FOR VALUES IN (4, 5, 6);",
        "CREATE TABLE fz_pa_j3_c PARTITION OF fz_pa_j3 FOR VALUES IN (7, 8, 9);",
        "CREATE TABLE fz_pa_j4 (pk int4 NOT NULL, k int4, v int4) PARTITION BY LIST (k);",
        "CREATE TABLE fz_pa_j4_a PARTITION OF fz_pa_j4 FOR VALUES IN (0, 1);",
        "CREATE TABLE fz_pa_j4_b PARTITION OF fz_pa_j4 FOR VALUES IN (2, 3, 4, 5);",
        "CREATE TABLE fz_pa_j4_c PARTITION OF fz_pa_j4 FOR VALUES IN (6, 7, 8);",
        "CREATE TABLE fz_pa_j4_def PARTITION OF fz_pa_j4 DEFAULT;",
        "INSERT INTO fz_pa_j3 SELECT i, (i * 3) % 10, i FROM generate_series(1, 300) i;",
        "INSERT INTO fz_pa_j4 SELECT i, (i * 7) % 12, -i FROM generate_series(1, 240) i;",
        "ANALYZE fz_pa_j3;",
        "ANALYZE fz_pa_j4;",
    ]);
    let queries: Vec<String> = vec![
        // Range-merge arms: inner/left/full + agg above.
        "SELECT count(*)::int8, coalesce(sum(a.v + b.v), 0)::int8 FROM fz_pa_j1 a \
         JOIN fz_pa_j2 b ON a.k = b.k;"
            .into(),
        "SELECT count(*)::int8, count(b.pk)::int8 FROM fz_pa_j1 a \
         LEFT JOIN fz_pa_j2 b ON a.k = b.k AND b.v > -100;"
            .into(),
        "SELECT count(*)::int8, count(a.pk)::int8, count(b.pk)::int8 FROM fz_pa_j1 a \
         FULL JOIN fz_pa_j2 b ON a.k = b.k;"
            .into(),
        "SELECT a.pk, b.pk FROM fz_pa_j1 a JOIN fz_pa_j2 b ON a.k = b.k \
         WHERE a.pk < 20 ORDER BY a.pk, b.pk;"
            .into(),
        // Semi/anti over merged bounds.
        "SELECT count(*)::int8 FROM fz_pa_j1 a WHERE EXISTS \
         (SELECT 1 FROM fz_pa_j2 b WHERE b.k = a.k AND b.pk < 100);"
            .into(),
        "SELECT count(*)::int8 FROM fz_pa_j1 a WHERE NOT EXISTS \
         (SELECT 1 FROM fz_pa_j2 b WHERE b.k = a.k);"
            .into(),
        // Pruned pwise-merged join.
        "SELECT count(*)::int8 FROM fz_pa_j1 a JOIN fz_pa_j2 b ON a.k = b.k WHERE a.k < 40;"
            .into(),
        // List-merge arms.
        "SELECT count(*)::int8, coalesce(sum(a.v)::int8, 0) FROM fz_pa_j3 a \
         JOIN fz_pa_j4 b ON a.k = b.k;"
            .into(),
        "SELECT count(*)::int8, count(b.pk)::int8 FROM fz_pa_j3 a \
         LEFT JOIN fz_pa_j4 b ON a.k = b.k;"
            .into(),
        "SELECT count(*)::int8 FROM fz_pa_j3 a FULL JOIN fz_pa_j4 b ON a.k = b.k;".into(),
        "SELECT a.k, count(*)::int8 FROM fz_pa_j3 a JOIN fz_pa_j4 b ON a.k = b.k \
         GROUP BY a.k ORDER BY a.k;"
            .into(),
        // 3-way mixing exactly-matching (j3-j3 self) and merged (j3-j4).
        "SELECT count(*)::int8 FROM fz_pa_j3 a JOIN fz_pa_j3 b ON a.k = b.k \
         JOIN fz_pa_j4 c ON b.k = c.k WHERE a.pk < 150;"
            .into(),
    ];
    v.extend(sweep(g, &[PWISE, PWISE_NESTL, PWISE_MERGEA, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_pa_j1, fz_pa_j2, fz_pa_j3, fz_pa_j4;"));
    v
}

// -------------------------------------------------------- pwise agg -------

/// create_partitionwise_grouping_paths FULL vs PARTIAL arms over
/// identically-bounded twins (list-keyed so the key is also joinable) and
/// an expression-partitioned parent.
fn gen_pwagg(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pa_g1 (pk int4 NOT NULL, k int4, x int4, v int4) PARTITION BY LIST (k);",
        "CREATE TABLE fz_pa_g1_a PARTITION OF fz_pa_g1 FOR VALUES IN (0, 1, 2);",
        "CREATE TABLE fz_pa_g1_b PARTITION OF fz_pa_g1 FOR VALUES IN (3, 4, 5);",
        "CREATE TABLE fz_pa_g1_c PARTITION OF fz_pa_g1 FOR VALUES IN (6, 7);",
        "CREATE TABLE fz_pa_g2 (pk int4 NOT NULL, k int4, x int4, v int4) PARTITION BY LIST (k);",
        "CREATE TABLE fz_pa_g2_a PARTITION OF fz_pa_g2 FOR VALUES IN (0, 1, 2);",
        "CREATE TABLE fz_pa_g2_b PARTITION OF fz_pa_g2 FOR VALUES IN (3, 4, 5);",
        "CREATE TABLE fz_pa_g2_c PARTITION OF fz_pa_g2 FOR VALUES IN (6, 7);",
        "CREATE TABLE fz_pa_ge (pk int4 NOT NULL, k int4, v int4) PARTITION BY LIST ((k % 4));",
        "CREATE TABLE fz_pa_ge_a PARTITION OF fz_pa_ge FOR VALUES IN (0, 1);",
        "CREATE TABLE fz_pa_ge_b PARTITION OF fz_pa_ge FOR VALUES IN (2, 3);",
        "INSERT INTO fz_pa_g1 SELECT i, (i * 3) % 8, (i * 5) % 23, i FROM generate_series(1, 600) i;",
        "INSERT INTO fz_pa_g2 SELECT i, (i * 7) % 8, (i * 11) % 23, -i FROM generate_series(1, 450) i;",
        "INSERT INTO fz_pa_ge SELECT i, i % 40, i FROM generate_series(1, 300) i;",
        "ANALYZE fz_pa_g1;",
        "ANALYZE fz_pa_g2;",
        "ANALYZE fz_pa_ge;",
    ]);
    let queries: Vec<String> = vec![
        // FULL pwise agg: GROUP BY = the partition key.
        "SELECT k, count(*)::int8, sum(v)::int8 FROM fz_pa_g1 GROUP BY k ORDER BY k;".into(),
        // Superset of the key (still FULL).
        "SELECT k, x % 3, count(*)::int8 FROM fz_pa_g1 GROUP BY k, 2 ORDER BY k, 2;".into(),
        // PARTIAL pwise agg: grouping key not the partition key.
        "SELECT x, count(*)::int8 FROM fz_pa_g1 GROUP BY x ORDER BY x;".into(),
        "SELECT x % 5, sum(v)::int8 FROM fz_pa_g1 GROUP BY 1 ORDER BY 1;".into(),
        // HAVING above both arms.
        "SELECT k, count(*)::int8 FROM fz_pa_g1 GROUP BY k HAVING count(*) > 70 ORDER BY k;"
            .into(),
        "SELECT x, count(*)::int8 FROM fz_pa_g1 GROUP BY x HAVING sum(v) > 4000 ORDER BY x;"
            .into(),
        // DISTINCT aggregate above the tree (blocks partial paths).
        "SELECT k, count(DISTINCT x)::int8 FROM fz_pa_g1 GROUP BY k ORDER BY k;".into(),
        // Agg over a pwise JOIN: child-join grouped rels.
        "SELECT a.k, count(*)::int8, sum(a.v + b.v)::int8 FROM fz_pa_g1 a \
         JOIN fz_pa_g2 b ON a.k = b.k AND a.pk = b.pk GROUP BY a.k ORDER BY a.k;"
            .into(),
        "SELECT a.x, count(*)::int8 FROM fz_pa_g1 a JOIN fz_pa_g2 b ON a.k = b.k \
         WHERE b.pk < 200 GROUP BY a.x ORDER BY a.x;"
            .into(),
        // Expression key: GROUP BY the key expression (FULL) vs the raw
        // column (PARTIAL).
        "SELECT k % 4, count(*)::int8 FROM fz_pa_ge GROUP BY 1 ORDER BY 1;".into(),
        "SELECT k, count(*)::int8 FROM fz_pa_ge GROUP BY k ORDER BY k LIMIT 12;".into(),
        // Grouping sets over the partition key (falls back off pwise).
        "SELECT k, x % 2, count(*)::int8 FROM fz_pa_g1 \
         GROUP BY GROUPING SETS ((k), (k, x % 2)) ORDER BY 1, 2 NULLS FIRST;"
            .into(),
        // No GROUP BY: plain agg over the Append.
        "SELECT count(*)::int8, sum(v)::int8, min(x), max(x) FROM fz_pa_g1;".into(),
    ];
    v.extend(sweep(
        g,
        &[PWISE, PWAGG_HASH_OFF, PWAGG_SORT_OFF, PWISE_PAR, DEFAULTP],
        &queries,
    ));
    v.push(raw("DROP TABLE fz_pa_g1, fz_pa_g2, fz_pa_ge;"));
    v
}

// ------------------------------------------------ reparameterization ------

/// Identically-bounded range twins + a multi-level pair co-partitioned at
/// the TOP level only: under pwise nestloops the child joins' inner sides
/// carry BitmapHeapPath/BitmapOr/BitmapAnd (bmp profile), Append (the
/// sub-partitioned child), and Gather (par profile) paths through
/// reparameterize_path_by_child.
fn gen_reparam(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pa_t1 (pk int4 PRIMARY KEY, a int4, b int4, v int4) \
         PARTITION BY RANGE (pk);",
        "CREATE TABLE fz_pa_t1_0 PARTITION OF fz_pa_t1 FOR VALUES FROM (0) TO (300);",
        "CREATE TABLE fz_pa_t1_1 PARTITION OF fz_pa_t1 FOR VALUES FROM (300) TO (600);",
        "CREATE TABLE fz_pa_t1_2 PARTITION OF fz_pa_t1 FOR VALUES FROM (600) TO (900);",
        "CREATE TABLE fz_pa_t2 (pk int4 PRIMARY KEY, a int4, b int4, v int4) \
         PARTITION BY RANGE (pk);",
        "CREATE TABLE fz_pa_t2_0 PARTITION OF fz_pa_t2 FOR VALUES FROM (0) TO (300);",
        "CREATE TABLE fz_pa_t2_1 PARTITION OF fz_pa_t2 FOR VALUES FROM (300) TO (600);",
        "CREATE TABLE fz_pa_t2_2 PARTITION OF fz_pa_t2 FOR VALUES FROM (600) TO (900);",
        // Separate single-column indexes: BitmapAnd fuel (a AND b), and
        // OR quals across them: BitmapOr fuel.
        "CREATE INDEX ON fz_pa_t2 (a);",
        "CREATE INDEX ON fz_pa_t2 (b);",
        "CREATE INDEX ON fz_pa_t1 (a);",
        "INSERT INTO fz_pa_t1 SELECT i, (i * 7) % 40, (i * 3) % 25, i \
         FROM generate_series(1, 850) i;",
        "INSERT INTO fz_pa_t2 SELECT i, (i * 11) % 40, (i * 5) % 25, -i \
         FROM generate_series(1, 800) i;",
        "ANALYZE fz_pa_t1;",
        "ANALYZE fz_pa_t2;",
        // Multi-level pair: top RANGE(pk) bounds match t1's; children
        // sub-partitioned by HASH(pk) -> the top-level child join's sides
        // are themselves Appends.
        "CREATE TABLE fz_pa_t3 (pk int4 NOT NULL, a int4, v int4) PARTITION BY RANGE (pk);",
        "CREATE TABLE fz_pa_t3_0 PARTITION OF fz_pa_t3 FOR VALUES FROM (0) TO (300) \
         PARTITION BY HASH (pk);",
        "CREATE TABLE fz_pa_t3_0h0 PARTITION OF fz_pa_t3_0 FOR VALUES WITH (MODULUS 2, REMAINDER 0);",
        "CREATE TABLE fz_pa_t3_0h1 PARTITION OF fz_pa_t3_0 FOR VALUES WITH (MODULUS 2, REMAINDER 1);",
        "CREATE TABLE fz_pa_t3_1 PARTITION OF fz_pa_t3 FOR VALUES FROM (300) TO (600);",
        "CREATE TABLE fz_pa_t3_2 PARTITION OF fz_pa_t3 FOR VALUES FROM (600) TO (900);",
        "CREATE INDEX ON fz_pa_t3 (a);",
        "INSERT INTO fz_pa_t3 SELECT i, (i * 13) % 40, i FROM generate_series(1, 600) i;",
        "ANALYZE fz_pa_t3;",
    ]);
    let queries: Vec<String> = vec![
        // BitmapOr inner: OR across the two single-column t2 indexes,
        // parameterized by the outer child.
        "SELECT count(*)::int8 FROM fz_pa_t1 p1 JOIN fz_pa_t2 p2 \
         ON p1.pk = p2.pk AND (p2.a = p1.a OR p2.b = p1.b) WHERE p1.v < 120;"
            .into(),
        // BitmapAnd inner: both t2 indexes ANDed under the join.
        "SELECT count(*)::int8 FROM fz_pa_t1 p1 JOIN fz_pa_t2 p2 \
         ON p1.pk = p2.pk AND p2.a = p1.a AND p2.b = p1.b;"
            .into(),
        // Plain co-partitioned probe rows (identity anchor).
        "SELECT p1.pk, p2.v FROM fz_pa_t1 p1 JOIN fz_pa_t2 p2 ON p1.pk = p2.pk \
         WHERE p1.a < 4 ORDER BY p1.pk;"
            .into(),
        // Inner side = a JOIN (NestPath reparam) with bitmap-able quals.
        "SELECT count(*)::int8 FROM fz_pa_t1 p1 JOIN fz_pa_t2 p2 ON p1.pk = p2.pk \
         JOIN fz_pa_t1 p3 ON p2.pk = p3.pk AND (p3.a = p2.a OR p3.a = p2.b) \
         WHERE p1.v < 200;"
            .into(),
        // Multi-level inner: t3's matching top child is itself an Append
        // (T_AppendPath reparameterization).
        "SELECT count(*)::int8 FROM fz_pa_t1 p1 JOIN fz_pa_t3 p3 ON p1.pk = p3.pk \
         WHERE p1.a < 10;"
            .into(),
        "SELECT count(*)::int8 FROM fz_pa_t3 p3 JOIN fz_pa_t1 p1 ON p3.pk = p1.pk \
         AND p1.a = p3.a;"
            .into(),
        // LATERAL bitmap inner (SubqueryScan + bitmap below).
        "SELECT p1.pk, l.c FROM fz_pa_t1 p1, LATERAL (SELECT count(*)::int8 AS c \
         FROM fz_pa_t2 p2 WHERE p2.pk = p1.pk AND (p2.a = p1.a OR p2.b = 3)) l \
         WHERE p1.a < 3 ORDER BY p1.pk;"
            .into(),
        // Aggregated pwise join (partial agg above child joins under par).
        "SELECT count(*)::int8, coalesce(sum(p2.v), 0)::int8 FROM fz_pa_t1 p1 \
         JOIN fz_pa_t2 p2 ON p1.pk = p2.pk WHERE p1.b < 12;"
            .into(),
    ];
    v.extend(sweep(g, &[PWISE_BMP, PWISE_NESTL, PWISE_PAR, PWISE_MERGEA, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_pa_t1, fz_pa_t2, fz_pa_t3;"));
    v
}

// -------------------------------------------------- satisfies_hash_partition

/// The SQL-callable hash-routing checker (partbounds.c) success + ereport
/// matrix, plus partitioning deparse probes. Every statement is a scalar
/// SELECT with a deterministic result or a deterministic error identity.
fn gen_hashfn(g: &mut Gen) -> Vec<StmtKind> {
    let val = g.rng.below(500) as i64;
    let mut v = raws(&[
        "CREATE TABLE fz_pa_hf (k int4, s text, v int4) PARTITION BY HASH (k, s);",
        "CREATE TABLE fz_pa_hf_0 PARTITION OF fz_pa_hf FOR VALUES WITH (MODULUS 3, REMAINDER 0);",
        "CREATE TABLE fz_pa_hf_1 PARTITION OF fz_pa_hf FOR VALUES WITH (MODULUS 3, REMAINDER 1);",
        "CREATE TABLE fz_pa_hf_2 PARTITION OF fz_pa_hf FOR VALUES WITH (MODULUS 3, REMAINDER 2);",
        "INSERT INTO fz_pa_hf SELECT i % 50, 'w' || (i % 7), i FROM generate_series(1, 200) i;",
        "ANALYZE fz_pa_hf;",
        "CREATE TABLE fz_pa_plain (k int4);",
        "CREATE TABLE fz_pa_rr (k int4) PARTITION BY RANGE (k);",
        "CREATE TABLE fz_pa_rr_0 PARTITION OF fz_pa_rr FOR VALUES FROM (0) TO (10);",
    ]);
    let mut probes: Vec<StmtKind> = vec![
        // Success arms: every remainder, matched and unmatched values,
        // NULLs (hash of NULL contributes 0), cross-type coercion of the
        // variadic "any" args.
        raw(format!(
            "SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, {val}::int4, 'w1'::text);"
        )),
        raw(format!(
            "SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 1, {val}::int4, 'w2'::text);"
        )),
        raw(format!(
            "SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 2, {val}::int4, ''::text);"
        )),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, NULL::int4, NULL::text);"),
        raw(format!(
            "SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, {val}::int4, NULL::text);"
        )),
        // Sum over all remainders must be exactly the row count.
        raw("SELECT (SELECT count(*) FROM fz_pa_hf_0) + (SELECT count(*) FROM fz_pa_hf_1) \
             + (SELECT count(*) FROM fz_pa_hf_2) = (SELECT count(*) FROM fz_pa_hf);"),
        // Each leaf's rows satisfy their own remainder (aggregated bool).
        raw("SELECT bool_and(satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, k, s)) \
             FROM fz_pa_hf_0;"),
        raw("SELECT bool_and(satisfies_hash_partition('fz_pa_hf'::regclass, 3, 1, k, s)) \
             FROM fz_pa_hf_1;"),
        raw("SELECT bool_or(satisfies_hash_partition('fz_pa_hf'::regclass, 3, 2, k, s)) \
             FROM fz_pa_hf_0;"),
        // ereport arms (fixed SQLSTATEs; error identity is the oracle):
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 0, 0, 1::int4, 'x'::text);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, -3, 0, 1::int4, 'x'::text);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 5, 1::int4, 'x'::text);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, -1, 1::int4, 'x'::text);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, 1::int4);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, 1::int4, 'x'::text, 9);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, 'x'::text, 1::int4);"),
        raw("SELECT satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0, 1::int8, 'x'::text);"),
        raw("SELECT satisfies_hash_partition('fz_pa_plain'::regclass, 2, 0, 1::int4);"),
        raw("SELECT satisfies_hash_partition('fz_pa_rr'::regclass, 2, 0, 1::int4);"),
        raw("SELECT satisfies_hash_partition(0::oid::regclass, 2, 0, 1::int4);"),
        raw(format!(
            "SELECT satisfies_hash_partition('fz_pa_hf_0'::regclass, 3, 0, {val}::int4, 'q'::text);"
        )),
        // Deparse probes (ruleutils partition arms; text output compared).
        raw("SELECT pg_get_partkeydef('fz_pa_hf'::regclass);"),
        raw("SELECT pg_get_partkeydef('fz_pa_rr'::regclass);"),
        raw("SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c \
             WHERE c.relname = 'fz_pa_hf_1';"),
        raw("SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c \
             WHERE c.relname = 'fz_pa_rr_0';"),
        raw("SELECT pg_get_partition_constraintdef('fz_pa_hf_2'::regclass);"),
        raw("SELECT pg_get_partition_constraintdef('fz_pa_rr_0'::regclass);"),
        raw("SELECT pg_get_partition_constraintdef('fz_pa_plain'::regclass);"),
    ];
    v.append(&mut probes);
    v.push(raw("DROP TABLE fz_pa_hf, fz_pa_plain, fz_pa_rr;"));
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            let stmts = gen_partalt_module(&mut g);
            assert!(!stmts.is_empty());
            out.push(stmts.iter().map(|k| k.to_sql()).collect());
        }
        out
    }

    fn flat(seed: u64, n: usize) -> Vec<String> {
        gen_groups(seed, n).into_iter().flatten().collect()
    }

    #[test]
    fn deterministic_and_seed_sensitive() {
        assert_eq!(flat(5, 60), flat(5, 60));
        assert_ne!(flat(5, 60), flat(6, 60));
    }

    #[test]
    fn brackets_and_fixtures_are_group_local() {
        // Every SET has a RESET in the same group; every CREATE TABLE
        // fz_pa_* root fixture has a DROP in the same group; transactions
        // close in-group.
        for group in gen_groups(11, 300) {
            let mut open: Vec<String> = Vec::new();
            let mut in_txn = false;
            for sql in &group {
                if sql == "BEGIN;" {
                    in_txn = true;
                } else if sql == "COMMIT;" || sql == "ROLLBACK;" {
                    in_txn = false;
                }
                if let Some(rest) = sql.strip_prefix("SET ") {
                    let name = rest.split([' ', '=']).next().unwrap().to_string();
                    open.push(name);
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = open.iter().rposition(|n| *n == name);
                    assert!(pos.is_some(), "RESET {name} without SET: {sql}");
                    open.remove(pos.unwrap());
                }
            }
            assert!(open.is_empty(), "unclosed SETs at group end: {open:?}");
            assert!(!in_txn, "unclosed txn bracket at group end");
            let all = group.join("\n");
            for root in [
                "fz_pa_r ", "fz_pa_l ", "fz_pa_b ", "fz_pa_m ", "fz_pa_h ", "fz_pa_ml ",
                "fz_pa_j1 ", "fz_pa_g1 ", "fz_pa_t1 ", "fz_pa_hf ",
            ] {
                let created = all.contains(&format!("CREATE TABLE {root}"));
                if created {
                    let name = root.trim_end();
                    assert!(
                        group.iter().any(|s| s.starts_with("DROP TABLE ")
                            && s.contains(name)),
                        "{name} created but not dropped in-group"
                    );
                }
            }
        }
    }

    #[test]
    fn writes_are_rolled_back_and_ordered() {
        // DML only inside BEGIN..ROLLBACK; row-returning SELECTs carry a
        // total order (ORDER BY with pk) or are single-row scalar shapes.
        for group in gen_groups(17, 300) {
            let mut in_txn = false;
            for sql in &group {
                if sql == "BEGIN;" {
                    in_txn = true;
                } else if sql == "ROLLBACK;" {
                    in_txn = false;
                }
                if sql.starts_with("INSERT INTO fz_pa_ml VALUES")
                    || sql.starts_with("UPDATE ")
                    || sql.starts_with("DELETE ")
                {
                    assert!(in_txn, "write outside BEGIN..ROLLBACK: {sql}");
                }
                if sql.starts_with("SELECT pk") {
                    assert!(sql.contains("ORDER BY"), "unordered row probe: {sql}");
                }
            }
        }
    }

    #[test]
    fn prepared_statements_are_deallocated() {
        for group in gen_groups(23, 300) {
            let prepares = group.iter().filter(|s| s.starts_with("PREPARE ")).count();
            let deallocs = group.iter().filter(|s| s.starts_with("DEALLOCATE ")).count();
            assert_eq!(prepares, deallocs, "unbalanced PREPARE/DEALLOCATE: {group:?}");
        }
    }

    #[test]
    fn no_float_aggregates_or_explain() {
        for sql in flat(29, 200) {
            for bad in ["avg(", "stddev", "var_samp", "percentile_cont", "::float", "EXPLAIN"] {
                assert!(!sql.contains(bad), "forbidden form: {sql}");
            }
        }
    }

    #[test]
    fn all_shapes_and_key_arms_fire() {
        let stmts = flat(3, 500).join("\n");
        for needle in [
            // shapes' fixtures
            "PARTITION BY RANGE (a)",
            "PARTITION BY LIST (c)",
            "PARTITION BY LIST (flag)",
            "PARTITION BY RANGE (a, b)",
            "PARTITION BY HASH (h)",
            "PARTITION BY HASH (k, s)",
            "PARTITION BY LIST ((k % 4))",
            "PARTITION BY HASH (pk)",
            // clause-matrix arms
            "IS UNKNOWN",
            "IS NOT TRUE",
            "<> ALL",
            "= ANY (ARRAY[",
            "::int8[])",
            "::int2;",
            // runtime arms
            "SET plan_cache_mode = force_generic_plan;",
            "SET plan_cache_mode = force_custom_plan;",
            "EXECUTE fz_pa_ps1(NULL);",
            "EXECUTE fz_pa_ps2('{}'::int4[]);",
            "(VALUES (3), (30), (77), (NULL::int4))",
            // pwise arms
            "SET enable_partitionwise_join = on;",
            "SET enable_partitionwise_aggregate = on;",
            "FULL JOIN fz_pa_j2",
            "GROUPING SETS",
            // reparam arms
            "OR p2.b = p1.b",
            "SET max_parallel_workers_per_gather = 2;",
            // hashfn arms
            "satisfies_hash_partition('fz_pa_hf'::regclass, 3, 0",
            "satisfies_hash_partition('fz_pa_plain'::regclass",
            "pg_get_partition_constraintdef",
            "pg_get_partkeydef",
            // multilevel arms
            "PARTITION BY LIST (c);",
            "FOR VALUES FROM (40) TO (80)",
            "UPDATE fz_pa_ml SET a = 50 WHERE pk = 10001;",
            "SELECT count(*)::int8 FROM ONLY fz_pa_ml;",
        ] {
            assert!(stmts.contains(needle), "arm never fired in 500 groups: {needle}");
        }
    }

    #[test]
    fn set_reset_pairs_balance_textually() {
        // The sweep helper must emit RESETs for exactly the GUCs it SET,
        // stream-wide (guards profile-table typos).
        let stmts = flat(41, 400);
        use std::collections::HashMap;
        let mut counts: HashMap<String, i64> = HashMap::new();
        for sql in &stmts {
            if let Some(rest) = sql.strip_prefix("SET ") {
                let name = rest.split([' ', '=']).next().unwrap().to_string();
                *counts.entry(name).or_default() += 1;
            } else if let Some(rest) = sql.strip_prefix("RESET ") {
                let name = rest.trim_end_matches(';').to_string();
                *counts.entry(name).or_default() -= 1;
            }
        }
        for (name, n) in counts {
            assert_eq!(n, 0, "unbalanced SET/RESET for {name}");
        }
    }
}
