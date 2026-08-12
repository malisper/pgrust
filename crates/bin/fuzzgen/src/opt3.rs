//! Optimizer + executor residue drain, round 3 (W4-OPT; line-drain queue
//! chunks `optimizer-arms` + `executor-residue` after LD7/LD9/opt2 — see
//! docs/fuzzing/line-drain-queue.md and the in-lane BEFORE linegap in
//! docs/fuzzing/findings-w4opt.md). Targets were picked line-first: every
//! family below names the C functions whose unhit REGIONS it was written
//! against (REL_18_3@62d6c7d).
//!
//!   - fkjoin: match_foreign_keys_to_quals + get_foreign_key_join_
//!     selectivity (no earlier fixture had FOREIGN KEYs between join
//!     partners) and get_relation_statistics_worker (extended statistics
//!     objects: ndistinct/dependencies/mcv).
//!   - ojnest: deconstruct_distribute_oj_quals, make_outerjoininfo,
//!     join_is_legal, find_nonnullable_{vars,rels}_walker — nested and
//!     mixed outer joins with quals placed above/below the nullable
//!     sides.
//!   - constdeep: eval_const_expressions_mutator residue arms read from
//!     the BEFORE linegap: DistinctExpr/NullIfExpr const evaluation,
//!     row-form NullTest, BooleanTest over const, FieldSelect from
//!     RowExpr/composite, SQLValueFunction copy arm, CoerceToDomain.
//!   - reparam: reparameterize_path_by_child's NestPath/MergePath/
//!     HashPath/Append/Material/Memoize/Gather arms (the inner side of a
//!     partitionwise nestloop is itself a join / append / gather), and
//!     reparameterize_path's SeqScan/SampleScan/Bitmap/Result/Subquery/
//!     Material/Memoize children (appendrel common parameterization).
//!   - initplan: finalize_plan initplan/param-bitmap arms — initplans
//!     under Gather, WorkTableScan wtParam, MULTIEXPR SubPlans, CTE
//!     scan params.
//!   - winframe: update_frameheadpos/update_frametailpos/
//!     WinGetFuncArgInFrame/advance_windowaggregate_base — RANGE/GROUPS
//!     offset frames with every EXCLUDE variant, DESC/NULLS FIRST
//!     orderings, invertible and non-invertible aggregates.
//!   - routing: ExecInitPartitionInfo per-partition maps (attach-mapped
//!     partition with different attnos, routed ON CONFLICT DO UPDATE,
//!     WCO views over partition trees, per-partition RETURNING old/new),
//!     ExecInitMerge partitioned conversion arms,
//!     ExecCrossPartitionUpdateForeignKey (FK referencing a partitioned
//!     PK + cross-partition move).
//!   - idxkeys: ExecIndexBuildScanKeys / ExecIndexEvalArrayKeys /
//!     ExecIndexAdvanceArrayKeys — RowCompare keys, non-const and
//!     NULL-bearing SAOP arrays, IS [NOT] NULL keys, DESC/backward array
//!     scans under forced index scans.
//!   - sqlfn: check_sql_stmt_retval coercion shapes (RETURNS composite
//!     with column coercion, table rowtype with a dropped column, OUT
//!     params, DML-RETURNING tails, VOID tails, declared-mismatch error
//!     arms) + fmgr_sql.
//!   - scanmisc: ExecLimit backward/rescan arms, ExecReScan node
//!     dispatch, ExecScanSubPlan/ExecHashSubPlan null-handling — SCROLL
//!     cursors fetched backward through Limit/Sort/WindowAgg/MergeJoin,
//!     LATERAL rescans over odd inners, multi-column hashed subplans
//!     with NULLs on both sides.
//!
//! Correctness bar (LD7 law, unchanged): the RESULT SET of a
//! deterministic query is identical across every forced plan and across
//! both engines; any divergence is a HIGH planner/executor finding.
//! Plan shape may differ; no raw EXPLAIN output is emitted here.
//!
//! Discipline (opt2 rules): self-contained groups over fixed `fz_o3_*`
//! fixtures created/dropped in-group; every SET has its RESET in reverse
//! order in the same group; writes only inside BEGIN..ROLLBACK with SET
//! LOCAL; total ORDER BY (or LIMIT off a total order) on every
//! row-returning probe; exact-typed aggregates only; tables <= 1200 rows
//! (exhaustive ANALYZE sample — identical stats both sides); TABLESAMPLE
//! only at 100 percent with REPEATABLE; window value-picking functions
//! project only the ordering key, so peer choice cannot leak.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// A named GUC profile (opt2's shape; local copy keeps modules
/// independent).
#[derive(Clone, Copy)]
struct Prof {
    name: &'static str,
    gucs: &'static [(&'static str, &'static str)],
}

const DEFAULTP: Prof = Prof { name: "default", gucs: &[] };
const SEQ: Prof = Prof {
    name: "seq",
    gucs: &[
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
        ("enable_bitmapscan", "off"),
    ],
};
const IDX: Prof = Prof {
    name: "idx",
    gucs: &[("enable_seqscan", "off"), ("enable_bitmapscan", "off")],
};
const BITMAP: Prof = Prof {
    name: "bitmap",
    gucs: &[
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
    ],
};
const NESTL: Prof = Prof {
    name: "nestl",
    gucs: &[
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("random_page_cost", "0.5"),
    ],
};
const NESTL_BARE: Prof = Prof {
    name: "nestl_bare",
    gucs: &[
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_memoize", "off"),
        ("enable_material", "off"),
    ],
};
const MERGEJ: Prof = Prof {
    name: "mergej",
    gucs: &[("enable_hashjoin", "off"), ("enable_nestloop", "off")],
};
const HASHJ: Prof = Prof {
    name: "hashj",
    gucs: &[("enable_mergejoin", "off"), ("enable_nestloop", "off")],
};
/// Partitionwise nestloop at the TOP with hash/merge/material/memoize
/// still available INSIDE the lateral inner: the reparameterized inner
/// join can then be a HashPath/MergePath/MaterialPath/MemoizePath.
const PWISE_LAT: Prof = Prof {
    name: "pwise_lat",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_partitionwise_aggregate", "on"),
        ("random_page_cost", "0.5"),
    ],
};
const PWISE_NESTL: Prof = Prof {
    name: "pwise_nestl",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_memoize", "off"),
        ("enable_material", "off"),
    ],
};
const PWISE_MAT: Prof = Prof {
    name: "pwise_mat",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_memoize", "off"),
    ],
};
const PWISE_MEMO: Prof = Prof {
    name: "pwise_memo",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_material", "off"),
    ],
};
const PWISE_PAR: Prof = Prof {
    name: "pwise_par",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("parallel_setup_cost", "0"),
        ("parallel_tuple_cost", "0"),
        ("min_parallel_table_scan_size", "0"),
        ("min_parallel_index_scan_size", "0"),
    ],
};
const PARALLEL: Prof = Prof {
    name: "parallel",
    gucs: &[
        ("parallel_setup_cost", "0"),
        ("parallel_tuple_cost", "0"),
        ("min_parallel_table_scan_size", "0"),
        ("min_parallel_index_scan_size", "0"),
    ],
};
const CURSOR_FRAC: Prof = Prof {
    name: "cursor_frac",
    gucs: &[("cursor_tuple_fraction", "1.0")],
};

/// SET profile + body + RESET (reverse order), one statement group.
fn bracket(p: &Prof, body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts: Vec<StmtKind> = p
        .gucs
        .iter()
        .map(|(n, v)| raw(format!("SET {n} = {v};")))
        .collect();
    stmts.extend(body);
    for (n, _) in p.gucs.iter().rev() {
        stmts.push(raw(format!("RESET {n};")));
    }
    stmts
}

/// BEGIN + SET LOCAL profile + body + ROLLBACK (write shapes).
fn rollback_bracket(p: &Prof, body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts = vec![raw("BEGIN;")];
    for (n, v) in p.gucs {
        stmts.push(raw(format!("SET LOCAL {n} = {v};")));
    }
    stmts.extend(body);
    stmts.push(raw("ROLLBACK;"));
    stmts
}

/// Emit every query under every profile (short lists, profile-specific
/// arms — exhaustive like opt2).
fn sweep(g: &mut Gen, profs: &[Prof], queries: &[String]) -> Vec<StmtKind> {
    let mut v = Vec::new();
    for p in profs {
        g.fire2("opt3:prof:", p.name);
        v.extend(bracket(p, queries.iter().map(|q| raw(q.clone())).collect()));
    }
    v
}

const SHAPES: &[&str] = &[
    "opt3:fkjoin",
    "opt3:ojnest",
    "opt3:constdeep",
    "opt3:reparam",
    "opt3:initplan",
    "opt3:winframe",
    "opt3:routing",
    "opt3:idxkeys",
    "opt3:sqlfn",
    "opt3:scanmisc",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_opt3_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("opt3");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire2("opt3:shape:", &shape["opt3:".len()..]);
    match shape {
        "opt3:fkjoin" => gen_fkjoin(g),
        "opt3:ojnest" => gen_ojnest(g),
        "opt3:constdeep" => gen_constdeep(g),
        "opt3:reparam" => gen_reparam(g),
        "opt3:initplan" => gen_initplan(g),
        "opt3:winframe" => gen_winframe(g),
        "opt3:routing" => gen_routing(g),
        "opt3:idxkeys" => gen_idxkeys(g),
        "opt3:sqlfn" => gen_sqlfn(g),
        _ => gen_scanmisc(g),
    }
}

// ------------------------------------------------- FK joins + ext stats --

/// match_foreign_keys_to_quals walks every FK of every baserel looking
/// for join quals equating the FK columns; get_foreign_key_join_
/// selectivity then prices the join by FK semantics (incl. the semi/anti
/// arms and the not-all-clauses-matched fallbacks). Neither ran before:
/// no fixture had FKs between join partners. Extended-statistics objects
/// feed get_relation_statistics_worker + the dependencies/mcv/ndistinct
/// clauselist estimators.
fn gen_fkjoin(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_fp (pk int4 PRIMARY KEY, grp int4, pad text);",
        "CREATE TABLE fz_o3_fq (k1 int4, k2 int4, v int4, PRIMARY KEY (k1, k2));",
        "CREATE TABLE fz_o3_fc (pk int4 PRIMARY KEY, fk1 int4 NOT NULL REFERENCES fz_o3_fp, \
         fka int4 REFERENCES fz_o3_fp (pk), c1 int4, c2 int4, v int4, \
         FOREIGN KEY (c1, c2) REFERENCES fz_o3_fq (k1, k2));",
        "INSERT INTO fz_o3_fp SELECT i, i % 12, 'p' || (i % 7) FROM generate_series(1, 240) i;",
        "INSERT INTO fz_o3_fq SELECT i % 20, (i * 3) % 15, i FROM generate_series(1, 300) i \
         ON CONFLICT DO NOTHING;",
        "INSERT INTO fz_o3_fc SELECT i, (i % 240) + 1, CASE WHEN i % 3 = 0 THEN NULL \
         ELSE (i % 240) + 1 END, i % 20, (i * 3) % 15, i FROM generate_series(1, 600) i;",
        "CREATE INDEX ON fz_o3_fc (fk1);",
        // Extended stats on correlated columns of the fact table
        // (grp = pk % 12 correlation lives in fz_o3_es below).
        "CREATE TABLE fz_o3_es (pk int4 PRIMARY KEY, a int4, b int4, c int4);",
        "INSERT INTO fz_o3_es SELECT i, i % 20, (i % 20) / 2, (i * 7) % 100 \
         FROM generate_series(1, 900) i;",
        "CREATE STATISTICS fz_o3_st1 (ndistinct, dependencies, mcv) ON a, b FROM fz_o3_es;",
        "CREATE STATISTICS fz_o3_st2 ON (a % 5), c FROM fz_o3_es;",
        "ANALYZE fz_o3_fp;",
        "ANALYZE fz_o3_fq;",
        "ANALYZE fz_o3_fc;",
        "ANALYZE fz_o3_es;",
    ]);
    let queries: Vec<String> = vec![
        // Single-column FK equi-join (full FK match).
        "SELECT count(*)::int8, sum(c.v)::int8 FROM fz_o3_fc c JOIN fz_o3_fp p \
         ON c.fk1 = p.pk;"
            .into(),
        // Nullable FK column (fka has NULLs: the null_frac arm).
        "SELECT count(*)::int8 FROM fz_o3_fc c JOIN fz_o3_fp p ON c.fka = p.pk;".into(),
        // Composite FK: both clauses matched, then only ONE matched (the
        // not-all-columns fallback arm).
        "SELECT count(*)::int8 FROM fz_o3_fc c JOIN fz_o3_fq q \
         ON c.c1 = q.k1 AND c.c2 = q.k2;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_fc c JOIN fz_o3_fq q ON c.c1 = q.k1;".into(),
        // FK join + extra restriction (worth_dividing arms).
        "SELECT count(*)::int8 FROM fz_o3_fc c JOIN fz_o3_fp p ON c.fk1 = p.pk \
         WHERE p.grp < 4 AND c.v % 2 = 0;"
            .into(),
        // Semi and anti joins over the FK (get_foreign_key_join_selectivity
        // jointype arms).
        "SELECT count(*)::int8 FROM fz_o3_fp p WHERE EXISTS \
         (SELECT 1 FROM fz_o3_fc c WHERE c.fk1 = p.pk);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_fp p WHERE NOT EXISTS \
         (SELECT 1 FROM fz_o3_fc c WHERE c.fk1 = p.pk AND c.v > 590);"
            .into(),
        // Outer join over the FK.
        "SELECT count(*)::int8, count(c.pk)::int8 FROM fz_o3_fp p \
         LEFT JOIN fz_o3_fc c ON c.fk1 = p.pk AND c.v % 5 = 0;"
            .into(),
        // Three-way: FK join under another join (fkey selectivity inside
        // a larger joinrel).
        "SELECT count(*)::int8 FROM fz_o3_fc c JOIN fz_o3_fp p ON c.fk1 = p.pk \
         JOIN fz_o3_fq q ON c.c1 = q.k1 AND c.c2 = q.k2 WHERE p.grp = 3;"
            .into(),
        // Extended-stats consumers: dependencies (a determines b), mcv
        // multi-clause, ndistinct group estimation, expression stats.
        "SELECT count(*)::int8 FROM fz_o3_es WHERE a = 7 AND b = 3;".into(),
        "SELECT count(*)::int8 FROM fz_o3_es WHERE a = 4 AND b = 2 AND c < 50;".into(),
        "SELECT a, b, count(*)::int8 FROM fz_o3_es GROUP BY a, b ORDER BY a, b;".into(),
        "SELECT count(*)::int8 FROM fz_o3_es WHERE (a % 5) = 2 AND c = 21;".into(),
        "SELECT (a % 5) AS g, count(*)::int8 FROM fz_o3_es GROUP BY (a % 5) ORDER BY g;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_es e1 JOIN fz_o3_es e2 \
         ON e1.a = e2.a AND e1.b = e2.b WHERE e1.pk < 40;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, HASHJ, MERGEJ, NESTL], &queries));
    v.extend(raws(&[
        "DROP STATISTICS fz_o3_st1;",
        "DROP STATISTICS fz_o3_st2;",
        "DROP TABLE fz_o3_fc;",
        "DROP TABLE fz_o3_es;",
        "DROP TABLE fz_o3_fp;",
        "DROP TABLE fz_o3_fq;",
    ]));
    v
}

// -------------------------------------------------- nested outer joins ---

/// deconstruct_distribute_oj_quals / make_outerjoininfo residue: quals
/// that must be distributed to multiple versions of a nested outer
/// join's relids; join_is_legal identity-3 shapes; nonnullable-walker
/// arms over CASE/COALESCE/bool-op trees above nullable sides.
fn gen_ojnest(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_ja (pk int4 PRIMARY KEY, x int4, t text);",
        "CREATE TABLE fz_o3_jb (pk int4 PRIMARY KEY, x int4, y int4);",
        "CREATE TABLE fz_o3_jc (pk int4 PRIMARY KEY, x int4, z int4);",
        "CREATE TABLE fz_o3_jd (pk int4 PRIMARY KEY, x int4);",
        "INSERT INTO fz_o3_ja SELECT i, i % 30, 'a' || (i % 5) FROM generate_series(1, 300) i;",
        "INSERT INTO fz_o3_jb SELECT i, (i * 3) % 30, i % 7 FROM generate_series(1, 200) i;",
        "INSERT INTO fz_o3_jc SELECT i, (i * 7) % 30, i % 11 FROM generate_series(1, 150) i;",
        "INSERT INTO fz_o3_jd SELECT i, (i * 11) % 30 FROM generate_series(1, 80) i;",
        "CREATE INDEX ON fz_o3_jb (x);",
        "CREATE INDEX ON fz_o3_jc (x);",
        "ANALYZE fz_o3_ja;",
        "ANALYZE fz_o3_jb;",
        "ANALYZE fz_o3_jc;",
        "ANALYZE fz_o3_jd;",
    ]);
    let queries: Vec<String> = vec![
        // A LEFT (B LEFT C): the classic commute/associate identity fuel.
        "SELECT count(*)::int8, count(b.pk)::int8, count(c.pk)::int8 FROM fz_o3_ja a \
         LEFT JOIN (fz_o3_jb b LEFT JOIN fz_o3_jc c ON b.x = c.x) ON a.x = b.x;"
            .into(),
        // A LEFT (B JOIN C): inner join below an outer join.
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN \
         (fz_o3_jb b JOIN fz_o3_jc c ON b.x = c.x) ON a.x = b.x WHERE a.pk < 200;"
            .into(),
        // (A LEFT B) FULL (C LEFT D): full join over outer-join arms.
        "SELECT count(*)::int8, count(b.pk)::int8, count(d.pk)::int8 FROM \
         (fz_o3_ja a LEFT JOIN fz_o3_jb b ON a.x = b.x) FULL JOIN \
         (fz_o3_jc c LEFT JOIN fz_o3_jd d ON c.x = d.x) ON a.x = c.x;"
            .into(),
        // Qual above the nest referencing the innermost nullable rel:
        // deconstruct_distribute_oj_quals multiple-version territory.
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN \
         (fz_o3_jb b LEFT JOIN fz_o3_jc c ON b.x = c.x) ON a.x = b.x \
         WHERE c.z IS NULL OR c.z > 5;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN \
         (fz_o3_jb b LEFT JOIN fz_o3_jc c ON b.x = c.x AND b.y < 5) ON a.x = b.x \
         WHERE COALESCE(c.z, 0) < 8;"
            .into(),
        // Strict qual above the nullable side commutes the join away
        // (reduce_outer_joins + nonnullable walkers over bool trees).
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN fz_o3_jb b ON a.x = b.x \
         WHERE b.y + 1 > 0 AND (b.y < 3 OR b.y > 4);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN fz_o3_jb b ON a.x = b.x \
         WHERE CASE WHEN b.y IS NULL THEN 0 ELSE b.y END >= 0;"
            .into(),
        // RIGHT JOIN mixes (swapped make_outerjoininfo arms).
        "SELECT count(*)::int8 FROM fz_o3_jb b RIGHT JOIN fz_o3_ja a ON a.x = b.x \
         WHERE a.pk % 3 = 0;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_jc c RIGHT JOIN \
         (fz_o3_ja a LEFT JOIN fz_o3_jb b ON a.x = b.x) ON b.x = c.x;"
            .into(),
        // Anti-join via IS NULL over the nest.
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN fz_o3_jb b ON a.x = b.x \
         WHERE b.pk IS NULL;"
            .into(),
        // FULL JOIN with non-mergeable extra qual + filter above.
        "SELECT count(*)::int8, sum(COALESCE(a.x, 0))::int8 FROM fz_o3_ja a \
         FULL JOIN fz_o3_jb b ON a.x = b.x AND a.pk + b.pk < 300 \
         WHERE COALESCE(a.x, b.x, 0) < 12;"
            .into(),
        // Lateral reference out of a nested OJ (lateral + oj interplay).
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN LATERAL \
         (SELECT b.pk AS bpk FROM fz_o3_jb b WHERE b.x = a.x ORDER BY b.pk LIMIT 2) l \
         ON true WHERE a.pk < 120;"
            .into(),
        // 5-rel mixed nest under collapse pressure.
        "SELECT count(*)::int8 FROM fz_o3_ja a LEFT JOIN fz_o3_jb b ON a.x = b.x \
         LEFT JOIN fz_o3_jc c ON b.x = c.x LEFT JOIN fz_o3_jd d ON c.x = d.x \
         WHERE a.pk < 150;"
            .into(),
    ];
    let collapse1 = Prof {
        name: "collapse1",
        gucs: &[("join_collapse_limit", "1"), ("from_collapse_limit", "1")],
    };
    v.extend(sweep(g, &[DEFAULTP, NESTL, HASHJ, collapse1], &queries));
    v.push(raw("DROP TABLE fz_o3_ja, fz_o3_jb, fz_o3_jc, fz_o3_jd;"));
    v
}

// ----------------------------------------------- const-fold deep arms ----

/// eval_const_expressions_mutator residue arms (line-verified against
/// the BEFORE linegap): DistinctExpr/NullIfExpr constant evaluation,
/// row-form NullTest with mixed const/non-const fields, BooleanTest over
/// const NULL, FieldSelect from RowExpr and composite columns,
/// SQLValueFunction, CoerceToDomain.
fn gen_constdeep(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_cd (pk int4 PRIMARY KEY, x int4, y int4, s text);",
        "INSERT INTO fz_o3_cd SELECT i, i % 9, CASE WHEN i % 4 = 0 THEN NULL ELSE i % 6 END, \
         'c' || (i % 3) FROM generate_series(1, 120) i;",
        "ANALYZE fz_o3_cd;",
        "CREATE TYPE fz_o3_pair AS (u int4, w text);",
        "CREATE DOMAIN fz_o3_posint AS int4 CHECK (VALUE > 0);",
        "CREATE DOMAIN fz_o3_dnn AS int4 NOT NULL DEFAULT 7;",
    ]);
    let queries: Vec<String> = vec![
        // DistinctExpr const arms: all-null, one-null, both-const, mixed.
        "SELECT NULL IS DISTINCT FROM NULL, 1 IS DISTINCT FROM NULL, \
         NULL IS DISTINCT FROM 2, 3 IS DISTINCT FROM 3, 3 IS DISTINCT FROM 4;"
            .into(),
        "SELECT pk FROM fz_o3_cd WHERE x IS DISTINCT FROM 4 AND pk < 12 ORDER BY pk;".into(),
        "SELECT pk FROM fz_o3_cd WHERE y IS NOT DISTINCT FROM NULL ORDER BY pk LIMIT 8;"
            .into(),
        "SELECT pk FROM fz_o3_cd WHERE x IS DISTINCT FROM NULL AND pk < 10 ORDER BY pk;"
            .into(),
        // NullIfExpr const evaluation + mixed.
        "SELECT NULLIF(5, 5), NULLIF(5, 6), NULLIF(NULL::int4, 1), NULLIF(1, NULL);".into(),
        "SELECT pk, NULLIF(x, 4), NULLIF(3, x) FROM fz_o3_cd WHERE pk < 8 ORDER BY pk;"
            .into(),
        // Row-form NullTest: all-const rows (true/false/refuted) and
        // mixed const/var fields (per-field scalar NullTest expansion).
        "SELECT ROW(1, 2) IS NULL, ROW(NULL, NULL) IS NULL, ROW(1, NULL) IS NULL, \
         ROW(NULL, NULL) IS NOT NULL, ROW(1, 2) IS NOT NULL;"
            .into(),
        "SELECT pk FROM fz_o3_cd WHERE ROW(x, 1) IS NOT NULL AND pk < 10 ORDER BY pk;".into(),
        "SELECT pk FROM fz_o3_cd WHERE ROW(y, NULL::int4) IS NULL ORDER BY pk LIMIT 6;"
            .into(),
        "SELECT pk FROM fz_o3_cd WHERE ROW(x, y, 3) IS NOT NULL AND pk < 14 ORDER BY pk;"
            .into(),
        // BooleanTest over const NULL / const bools (all six testtypes).
        "SELECT NULL::bool IS TRUE, NULL::bool IS NOT TRUE, NULL::bool IS FALSE, \
         NULL::bool IS NOT FALSE, NULL::bool IS UNKNOWN, NULL::bool IS NOT UNKNOWN;"
            .into(),
        "SELECT true IS UNKNOWN, false IS NOT UNKNOWN, (1 = 1) IS TRUE, (1 = 2) IS NOT FALSE;"
            .into(),
        // FieldSelect from RowExpr / composite const / composite column.
        "SELECT (ROW(3, 'z')::fz_o3_pair).u, (ROW(x, s)::fz_o3_pair).w \
         FROM fz_o3_cd WHERE pk < 6 ORDER BY pk;"
            .into(),
        "SELECT ((1, 'q')::fz_o3_pair).*, (NULL::fz_o3_pair).u;".into(),
        "SELECT (CASE WHEN pk % 2 = 0 THEN ROW(pk, s)::fz_o3_pair \
         ELSE ROW(-pk, 'n')::fz_o3_pair END).u FROM fz_o3_cd WHERE pk < 7 ORDER BY pk;"
            .into(),
        // SQLValueFunction copy arm (value-independent projections only).
        "SELECT current_date IS NOT NULL, current_time IS NOT NULL, \
         localtimestamp IS NOT NULL, current_timestamp(2) IS NOT NULL;"
            .into(),
        // CoerceToDomain folding: const success, const failure (matched
        // 23514 both sides), domain over column, NOT NULL domain default.
        "SELECT 5::fz_o3_posint, (2 + 3)::fz_o3_posint;".into(),
        "SELECT (-1)::fz_o3_posint;".into(),
        "SELECT pk, (x + 1)::fz_o3_posint FROM fz_o3_cd WHERE pk < 6 ORDER BY pk;".into(),
        "SELECT NULL::fz_o3_dnn;".into(),
        // ArrayCoerce / CoerceViaIO over columns (non-const input arms).
        "SELECT pk, (ARRAY[x, y])::int8[], s::varchar(2)::text \
         FROM fz_o3_cd WHERE pk < 5 ORDER BY pk;"
            .into(),
        "SELECT (ARRAY[1, 2, NULL])::int8[]::text, '{7,8}'::int4[]::numeric[];".into(),
        // MinMax over consts + mixed (GREATEST/LEAST fold arms).
        "SELECT GREATEST(1, 2, NULL), LEAST(NULL::int4, NULL), GREATEST(3), \
         LEAST(4, NULL, 2);"
            .into(),
        "SELECT pk, GREATEST(x, y, 3), LEAST(x, NULL, y) FROM fz_o3_cd \
         WHERE pk < 6 ORDER BY pk;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, SEQ], &queries));
    v.extend(raws(&[
        "DROP DOMAIN fz_o3_posint;",
        "DROP DOMAIN fz_o3_dnn;",
        "DROP TYPE fz_o3_pair;",
        "DROP TABLE fz_o3_cd;",
    ]));
    v
}

// ---------------------------------------------- reparameterization -------

/// reparameterize_path_by_child join/append/material/memoize/gather arms
/// (the inner side of a partitionwise nestloop is itself a join or an
/// append), and reparameterize_path's per-child arms under appendrel
/// common parameterization (seqscan/samplescan/bitmap/result/subquery/
/// material/memoize children of a parameterized UNION ALL inner).
fn gen_reparam(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = Vec::new();
    // Co-partitioned triple (pwise fuel).
    for (t, rows, mult) in [
        ("fz_o3_p1", 900i64, 7i64),
        ("fz_o3_p2", 600, 11),
        ("fz_o3_p3", 300, 13),
    ] {
        v.push(raw(format!(
            "CREATE TABLE {t} (pk int4 PRIMARY KEY, a int4, b int4) PARTITION BY RANGE (pk);"
        )));
        for (i, (lo, hi)) in [(0, 400), (400, 800), (800, 1200)].iter().enumerate() {
            v.push(raw(format!(
                "CREATE TABLE {t}_c{i} PARTITION OF {t} FOR VALUES FROM ({lo}) TO ({hi});"
            )));
        }
        v.push(raw(format!("CREATE INDEX ON {t} (a);")));
        v.push(raw(format!(
            "INSERT INTO {t} SELECT i, (i * {mult}) % 40, (i * 3) % 17 \
             FROM generate_series(1, {rows}) i;"
        )));
        v.push(raw(format!("ANALYZE {t};")));
    }
    let lat_queries: Vec<String> = vec![
        // Lateral inner that is itself a JOIN: NestPath/MergePath/HashPath
        // reparameterization by child under the profile mix.
        "SELECT p1.pk, l.c FROM fz_o3_p1 p1, LATERAL (SELECT count(*)::int8 AS c \
         FROM fz_o3_p2 p2 JOIN fz_o3_p3 p3 ON p2.a = p3.a WHERE p2.pk = p1.pk) l \
         WHERE p1.b < 4 ORDER BY p1.pk;"
            .into(),
        "SELECT p1.pk, l.s FROM fz_o3_p1 p1, LATERAL (SELECT sum(p2.b + p3.b)::int8 AS s \
         FROM fz_o3_p2 p2 JOIN fz_o3_p3 p3 ON p2.pk = p3.pk \
         WHERE p2.pk = p1.pk) l WHERE p1.b < 5 ORDER BY p1.pk;"
            .into(),
        // Lateral inner over UNION ALL: AppendPath reparameterization.
        "SELECT p1.pk, l.c FROM fz_o3_p1 p1, LATERAL (SELECT count(*)::int8 AS c FROM \
         (SELECT pk, b FROM fz_o3_p2 UNION ALL SELECT pk, b FROM fz_o3_p3) u \
         WHERE u.pk = p1.pk) l WHERE p1.b < 4 ORDER BY p1.pk;"
            .into(),
        // Repeated-key lateral (memoize-friendly: MemoizePath arm under
        // pwise_memo, MaterialPath arm under pwise_mat).
        "SELECT p1.pk, l.m FROM fz_o3_p1 p1, LATERAL (SELECT max(p2.b)::int4 AS m \
         FROM fz_o3_p2 p2 WHERE p2.a = p1.a) l WHERE p1.pk < 300 ORDER BY p1.pk;"
            .into(),
        // Sample-scan T_Path arm (100 percent + REPEATABLE stays exact).
        "SELECT count(*)::int8 FROM fz_o3_p1 p1 JOIN \
         fz_o3_p2 s TABLESAMPLE BERNOULLI (100) REPEATABLE (5) ON p1.pk = s.pk \
         WHERE p1.b < 6;"
            .into(),
    ];
    v.extend(sweep(
        g,
        &[PWISE_LAT, PWISE_NESTL, PWISE_MAT, PWISE_MEMO, PWISE_PAR],
        &lat_queries,
    ));
    v.push(raw("DROP TABLE fz_o3_p1, fz_o3_p2, fz_o3_p3;"));

    // Appendrel common parameterization: plain UNION ALL inner of a
    // forced nestloop; child path types are steered by the scan profile
    // (seqscan/bitmap/index children, a Result child from a constant
    // SELECT, a SubqueryScan child from a LIMIT subquery, a SampleScan
    // child, nested appends).
    v.extend(raws(&[
        "CREATE TABLE fz_o3_u1 (pk int4 PRIMARY KEY, k int4, w int4);",
        "CREATE TABLE fz_o3_u2 (pk int4 PRIMARY KEY, k int4, w int4);",
        "CREATE TABLE fz_o3_ud (pk int4 PRIMARY KEY, k int4);",
        "CREATE INDEX ON fz_o3_u1 (k);",
        "CREATE INDEX ON fz_o3_u2 (k);",
        "INSERT INTO fz_o3_u1 SELECT i, (i * 5) % 50, i FROM generate_series(1, 400) i;",
        "INSERT INTO fz_o3_u2 SELECT i, (i * 9) % 50, -i FROM generate_series(1, 300) i;",
        "INSERT INTO fz_o3_ud SELECT i, (i * 3) % 50 FROM generate_series(1, 60) i;",
        "ANALYZE fz_o3_u1;",
        "ANALYZE fz_o3_u2;",
        "ANALYZE fz_o3_ud;",
    ]));
    let app_queries: Vec<String> = vec![
        // Plain two-table appendrel inner (seq/bitmap/index children per
        // profile).
        "SELECT d.pk, u.w FROM fz_o3_ud d, \
         (SELECT k, w FROM fz_o3_u1 UNION ALL SELECT k, w FROM fz_o3_u2) u \
         WHERE u.k = d.k AND d.pk < 10 ORDER BY d.pk, u.w;"
            .into(),
        // Result child: constant SELECT arm in the appendrel.
        "SELECT d.pk, count(u.w)::int8 FROM fz_o3_ud d LEFT JOIN \
         (SELECT k, w FROM fz_o3_u1 WHERE w % 2 = 0 \
          UNION ALL SELECT 3 AS k, 0 AS w \
          UNION ALL SELECT k, w FROM fz_o3_u2) u ON u.k = d.k \
         WHERE d.pk < 12 GROUP BY d.pk ORDER BY d.pk;"
            .into(),
        // SubqueryScan child (LIMIT keeps the child unflattened).
        "SELECT d.pk, count(u.w)::int8 FROM fz_o3_ud d LEFT JOIN \
         (SELECT k, w FROM fz_o3_u1 UNION ALL \
          SELECT k, w FROM (SELECT k, w FROM fz_o3_u2 ORDER BY pk LIMIT 150) z) u \
         ON u.k = d.k WHERE d.pk < 9 GROUP BY d.pk ORDER BY d.pk;"
            .into(),
        // SampleScan child at 100 percent.
        "SELECT d.pk, count(u.w)::int8 FROM fz_o3_ud d LEFT JOIN \
         (SELECT k, w FROM fz_o3_u1 TABLESAMPLE SYSTEM (100) REPEATABLE (2) \
          UNION ALL SELECT k, w FROM fz_o3_u2) u ON u.k = d.k \
         WHERE d.pk < 8 GROUP BY d.pk ORDER BY d.pk;"
            .into(),
        // Nested UNION ALL (append below append after flattening limits).
        "SELECT d.pk, count(u.w)::int8 FROM fz_o3_ud d LEFT JOIN \
         (SELECT k, w FROM fz_o3_u1 UNION ALL \
          (SELECT k, w FROM fz_o3_u2 UNION ALL SELECT k, -k FROM fz_o3_ud)) u \
         ON u.k = d.k WHERE d.pk < 7 GROUP BY d.pk ORDER BY d.pk;"
            .into(),
    ];
    let nestl_seq = Prof {
        name: "nestl_seq",
        gucs: &[
            ("enable_hashjoin", "off"),
            ("enable_mergejoin", "off"),
            ("enable_indexscan", "off"),
            ("enable_indexonlyscan", "off"),
            ("enable_bitmapscan", "off"),
        ],
    };
    let nestl_bmp = Prof {
        name: "nestl_bmp",
        gucs: &[
            ("enable_hashjoin", "off"),
            ("enable_mergejoin", "off"),
            ("enable_indexscan", "off"),
            ("enable_indexonlyscan", "off"),
            ("enable_material", "off"),
        ],
    };
    v.extend(sweep(g, &[NESTL, NESTL_BARE, nestl_seq, nestl_bmp], &app_queries));
    v.push(raw("DROP TABLE fz_o3_u1, fz_o3_u2, fz_o3_ud;"));
    v
}

// --------------------------------------------------- initplan bitmaps ----

/// finalize_plan residue: initplans attached above/below Gather,
/// WorkTableScan wtParam propagation, CTE scan params, MULTIEXPR
/// SubPlan finalization, nested initplan chains.
fn gen_initplan(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_ip (pk int4 PRIMARY KEY, a int4, b int4);",
        "CREATE TABLE fz_o3_iq (pk int4 PRIMARY KEY, m int4, n int4);",
        "INSERT INTO fz_o3_ip SELECT i, (i * 7) % 100, (i * 3) % 40 \
         FROM generate_series(1, 800) i;",
        "INSERT INTO fz_o3_iq SELECT i, (i * 5) % 100, i % 9 FROM generate_series(1, 120) i;",
        "CREATE INDEX ON fz_o3_ip (a);",
        "ANALYZE fz_o3_ip;",
        "ANALYZE fz_o3_iq;",
    ]);
    let queries: Vec<String> = vec![
        // Initplan under a parallelizable scan (finalize_plan gather_param
        // arms fire under the parallel profile).
        "SELECT count(*)::int8 FROM fz_o3_ip WHERE a > (SELECT avg(m)::int4 FROM fz_o3_iq);"
            .into(),
        // Two stacked initplans + one correlated subplan.
        "SELECT count(*)::int8 FROM fz_o3_ip t WHERE a > (SELECT min(m) FROM fz_o3_iq) \
         AND b < (SELECT max(n) * 5 FROM fz_o3_iq) \
         AND EXISTS (SELECT 1 FROM fz_o3_iq q WHERE q.m = t.a);"
            .into(),
        // Initplan inside a HAVING clause.
        "SELECT b, count(*)::int8 FROM fz_o3_ip GROUP BY b \
         HAVING count(*) > (SELECT count(*)::int8 / 60 FROM fz_o3_ip) ORDER BY b;"
            .into(),
        // Recursive CTE with an initplan in the recursive term
        // (WorkTableScan wtParam + initplan bitmap union).
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r \
         WHERE n < (SELECT max(n) FROM fz_o3_iq) + 4) SELECT count(*)::int8, sum(n)::int8 FROM r;"
            .into(),
        // Recursive CTE joined back to a table (worktable param scans).
        "WITH RECURSIVE walk(pk, a) AS (\
           SELECT pk, a FROM fz_o3_ip WHERE pk = 1 \
           UNION ALL \
           SELECT p.pk, p.a FROM fz_o3_ip p JOIN walk w ON p.pk = w.pk + 40 \
           WHERE p.pk <= 800) \
         SELECT count(*)::int8, sum(a)::int8 FROM walk;"
            .into(),
        // Materialized CTE referenced twice (CTE scan params both sides).
        "WITH c AS MATERIALIZED (SELECT pk, a FROM fz_o3_ip WHERE b = 3) \
         SELECT count(*)::int8 FROM c x JOIN c y ON x.a = y.a;"
            .into(),
        // Initplan feeding a LIMIT/OFFSET (limit params).
        "SELECT pk FROM fz_o3_ip ORDER BY pk \
         LIMIT (SELECT count(*) / 100 FROM fz_o3_ip) \
         OFFSET (SELECT min(n) FROM fz_o3_iq);"
            .into(),
        // ANY-subplan under parallel restriction + hashed subplan.
        "SELECT count(*)::int8 FROM fz_o3_ip WHERE b IN (SELECT n FROM fz_o3_iq);".into(),
        "SELECT count(*)::int8 FROM fz_o3_ip WHERE a <> ALL \
         (SELECT m FROM fz_o3_iq WHERE n < 3);"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, PARALLEL, NESTL], &queries));
    // MULTIEXPR SubPlan finalization (UPDATE SET (..) = (SELECT ..)),
    // rolled back; RETURNING flows through an ordered CTE.
    for p in [DEFAULTP, PARALLEL] {
        v.extend(rollback_bracket(
            &p,
            vec![
                raw("WITH w AS (UPDATE fz_o3_ip SET (a, b) = \
                     (SELECT q.m, q.n FROM fz_o3_iq q WHERE q.pk = fz_o3_ip.pk % 120 + 1) \
                     WHERE pk <= 40 RETURNING pk, a, b) \
                     SELECT count(*)::int8, sum(a)::int8, sum(b)::int8 FROM w;"),
                raw("WITH w AS (UPDATE fz_o3_ip SET (a, b) = \
                     (SELECT max(m), (SELECT min(n) FROM fz_o3_iq) FROM fz_o3_iq) \
                     WHERE pk <= 10 RETURNING pk, a, b) \
                     SELECT count(*)::int8, sum(a)::int8, sum(b)::int8 FROM w;"),
            ],
        ));
    }
    v.push(raw("DROP TABLE fz_o3_ip, fz_o3_iq;"));
    v
}

// ------------------------------------------------------- window frames ---

/// update_frameheadpos / update_frametailpos / WinGetFuncArgInFrame /
/// advance_windowaggregate_base residue: RANGE and GROUPS offset frames
/// (int + numeric offsets, ASC/DESC, NULLS FIRST/LAST) crossed with the
/// EXCLUDE variants, invertible (sum) and non-invertible (max) moving
/// aggregates, in-frame positional functions projecting only the
/// ordering key (peer-invariant by construction), and no-ORDER-BY RANGE
/// frames.
fn gen_winframe(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_w (pk int4 PRIMARY KEY, g int4, k int4, kn numeric, v int4);",
        // k is non-unique (peer groups of ~3); kn mirrors k as numeric;
        // one NULL k row per group (NULLS FIRST/LAST arms).
        "INSERT INTO fz_o3_w SELECT i, i % 4, CASE WHEN i % 37 = 0 THEN NULL \
         ELSE (i / 3) % 25 END, CASE WHEN i % 37 = 0 THEN NULL \
         ELSE ((i / 3) % 25)::numeric / 2 END, (i * 7) % 50 \
         FROM generate_series(1, 444) i;",
        "ANALYZE fz_o3_w;",
    ]);
    let frames: &[&str] = &[
        "RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING",
        "RANGE BETWEEN 5 PRECEDING AND 1 PRECEDING",
        "RANGE BETWEEN 1 FOLLOWING AND 6 FOLLOWING",
        "RANGE BETWEEN CURRENT ROW AND 4 FOLLOWING",
        "RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING",
        "GROUPS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
        "GROUPS BETWEEN 3 PRECEDING AND 1 PRECEDING",
        "GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING",
        "ROWS BETWEEN 4 PRECEDING AND 2 FOLLOWING",
    ];
    let excludes: &[&str] = &[
        "",
        " EXCLUDE CURRENT ROW",
        " EXCLUDE GROUP",
        " EXCLUDE TIES",
        " EXCLUDE NO OTHERS",
    ];
    let mut queries: Vec<String> = Vec::new();
    // Sample frame x exclude x ordering combinations off the session
    // PRNG (full cross is ~180 queries; take a rotating 12 per group).
    for _ in 0..12 {
        let f = frames[g.rng.below_usize(frames.len())];
        let e = excludes[g.rng.below_usize(excludes.len())];
        let ord = ["k ASC", "k DESC", "k ASC NULLS FIRST", "k DESC NULLS LAST"]
            [g.rng.below_usize(4)];
        let agg = ["sum(v)", "count(*)", "max(v)", "min(v)", "sum(kn)", "avg(v * 1000)"]
            [g.rng.below_usize(6)];
        queries.push(format!(
            "SELECT sum(fv)::numeric, count(fv)::int8 FROM (SELECT ({agg} OVER \
             (PARTITION BY g ORDER BY {ord} {f}{e}))::numeric AS fv FROM fz_o3_w) z;"
        ));
    }
    // Numeric-offset RANGE frames over the numeric mirror column.
    queries.push(
        "SELECT sum(fv)::int8 FROM (SELECT (count(*) OVER \
         (PARTITION BY g ORDER BY kn RANGE BETWEEN 1.5 PRECEDING AND 2.5 FOLLOWING \
          EXCLUDE TIES))::int8 AS fv FROM fz_o3_w) z;"
            .into(),
    );
    queries.push(
        "SELECT sum(fv)::int8 FROM (SELECT (sum(v) OVER \
         (PARTITION BY g ORDER BY kn DESC RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING\
         ))::int8 AS fv FROM fz_o3_w) z;"
            .into(),
    );
    // In-frame positional functions projecting the ordering key only
    // (peer-invariant): first/last/nth over offset frames + exclusions.
    queries.push(
        "SELECT sum(fv)::int8, count(*)::int8 FROM (SELECT first_value(k) OVER \
         (PARTITION BY g ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING \
          EXCLUDE CURRENT ROW) AS fv FROM fz_o3_w) z;"
            .into(),
    );
    queries.push(
        "SELECT sum(fv)::int8 FROM (SELECT last_value(k) OVER \
         (PARTITION BY g ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
          EXCLUDE GROUP) AS fv FROM fz_o3_w) z;"
            .into(),
    );
    queries.push(
        "SELECT sum(fv)::int8 FROM (SELECT nth_value(k, 3) OVER \
         (PARTITION BY g ORDER BY k RANGE BETWEEN 4 PRECEDING AND 1 FOLLOWING \
          EXCLUDE TIES) AS fv FROM fz_o3_w) z;"
            .into(),
    );
    queries.push(
        "SELECT sum(fv)::int8 FROM (SELECT nth_value(k, 2) OVER \
         (PARTITION BY g ORDER BY k DESC NULLS FIRST \
          GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS fv \
         FROM fz_o3_w) z;"
            .into(),
    );
    // No ORDER BY: all rows are peers (the ordNumCols == 0 arms).
    queries.push(
        "SELECT sum(fv)::int8 FROM (SELECT (count(*) OVER \
         (PARTITION BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
          EXCLUDE CURRENT ROW))::int8 AS fv FROM fz_o3_w) z;"
            .into(),
    );
    // Frame options as error arms (matched SQLSTATEs): negative offset,
    // RANGE offset without ORDER BY, GROUPS without ORDER BY.
    queries.push(
        "SELECT count(*) FROM (SELECT sum(v) OVER (ORDER BY k \
         RANGE BETWEEN -1 PRECEDING AND CURRENT ROW) FROM fz_o3_w) z;"
            .into(),
    );
    queries.push(
        "SELECT count(*) FROM (SELECT sum(v) OVER (\
         RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM fz_o3_w) z;"
            .into(),
    );
    queries.push(
        "SELECT count(*) FROM (SELECT sum(v) OVER (PARTITION BY g \
         GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM fz_o3_w) z;"
            .into(),
    );
    v.extend(sweep(g, &[DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_o3_w;"));
    v
}

// --------------------------------------------------- partition routing ---

/// ExecInitPartitionInfo deep-init arms: routed inserts into an
/// ATTACH-mapped partition (different attnos via a dropped column),
/// routed ON CONFLICT DO UPDATE (arbiter mapping + conversion), WCO via
/// a CHECK OPTION view over the tree, RETURNING old/new over routed
/// DML, MERGE into the tree (ExecInitMerge conversion arms), and
/// cross-partition UPDATE with an FK referencing the partitioned PK
/// (ExecCrossPartitionUpdateForeignKey).
fn gen_routing(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_r (pk int4 NOT NULL, a int4, t text, PRIMARY KEY (pk)) \
         PARTITION BY RANGE (pk);",
        "CREATE TABLE fz_o3_r_c0 PARTITION OF fz_o3_r FOR VALUES FROM (0) TO (200);",
        "CREATE TABLE fz_o3_r_c1 PARTITION OF fz_o3_r FOR VALUES FROM (200) TO (400);",
        // Attach-mapped partition: born with an extra column, dropped
        // before ATTACH -> attribute numbers differ from the root.
        "CREATE TABLE fz_o3_r_x (dropme int8, pk int4 NOT NULL, a int4, t text);",
        "ALTER TABLE fz_o3_r_x DROP COLUMN dropme;",
        "ALTER TABLE fz_o3_r ATTACH PARTITION fz_o3_r_x FOR VALUES FROM (400) TO (600);",
        // ODD pks only (1..499): even pks and 500..599 stay free, so
        // routed-insert probes can pick collision-free keys while ON
        // CONFLICT probes deliberately target odd ones.
        "INSERT INTO fz_o3_r SELECT i * 2 - 1, i % 30, 'r' || (i % 9) \
         FROM generate_series(1, 250) i;",
        // Per-partition extra: a local index only on the attach-mapped
        // partition (per-partition init state).
        "CREATE INDEX ON fz_o3_r_x (a);",
        "ANALYZE fz_o3_r;",
        // CHECK OPTION view over the tree (WCO mapping per partition).
        "CREATE VIEW fz_o3_rv AS SELECT * FROM fz_o3_r WHERE a >= 0 \
         WITH CHECK OPTION;",
        // FK referencing the partitioned PK (cross-partition update FK
        // machinery) — only pks 1..50 are referenced.
        "CREATE TABLE fz_o3_ref (pk int4 PRIMARY KEY, r int4 REFERENCES fz_o3_r (pk));",
        "INSERT INTO fz_o3_ref SELECT i, i * 2 - 1 FROM generate_series(1, 25) i;",
        "ANALYZE fz_o3_ref;",
    ]);
    let bodies: Vec<Vec<StmtKind>> = vec![
        // Routed inserts incl. the attach-mapped partition, RETURNING
        // old/new through the ordered CTE.
        vec![raw(
            "WITH w AS (INSERT INTO fz_o3_r VALUES (595, 1, 'nx'), (150, 2, 'nb'), \
             (398, 3, 'nc') RETURNING pk, a, t) \
             SELECT count(*)::int8, sum(pk)::int8 FROM w;",
        )],
        vec![raw(
            "WITH w AS (INSERT INTO fz_o3_r SELECT i + 500, i, 'bulk' \
             FROM generate_series(1, 40) i RETURNING pk) \
             SELECT count(*)::int8, min(pk), max(pk) FROM w;",
        )],
        // Routed ON CONFLICT DO UPDATE (arbiter mapped into the
        // attach-mapped partition; 450 inserts fresh, 451 collides) +
        // DO NOTHING.
        vec![raw(
            "WITH w AS (INSERT INTO fz_o3_r VALUES (450, 9, 'cf'), (451, 8, 'cg') \
             ON CONFLICT (pk) DO UPDATE SET a = fz_o3_r.a + 100, t = excluded.t \
             RETURNING pk, a, t) SELECT count(*)::int8, sum(a)::int8 FROM w;",
        )],
        vec![raw(
            "WITH w AS (INSERT INTO fz_o3_r SELECT i, 0, 'dn' FROM generate_series(440, 470) i \
             ON CONFLICT (pk) DO NOTHING RETURNING pk) \
             SELECT count(*)::int8, min(pk) FROM w;",
        )],
        // PG18 RETURNING old/new over routed DML.
        vec![raw(
            "WITH w AS (INSERT INTO fz_o3_r VALUES (455, 5, 'on') \
             ON CONFLICT (pk) DO UPDATE SET a = excluded.a + 1 \
             RETURNING old.a AS oa, new.a AS na, pk) \
             SELECT count(*)::int8, sum(na - COALESCE(oa, 0))::int8 FROM w;",
        )],
        // WCO view: passing and failing routed inserts (44000 at tail).
        vec![
            raw(
                "WITH w AS (INSERT INTO fz_o3_rv VALUES (596, 7, 'wv') RETURNING pk) \
                 SELECT count(*)::int8 FROM w;",
            ),
            raw("INSERT INTO fz_o3_rv VALUES (597, -1, 'bad');"),
        ],
        // Cross-partition moves: unreferenced row succeeds (incl. into
        // the attach-mapped partition), referenced row errors 23503 at
        // the bracket tail.
        vec![
            raw(
                "WITH w AS (UPDATE fz_o3_r SET pk = pk + 441 WHERE pk BETWEEN 60 AND 70 \
                 RETURNING pk) SELECT count(*)::int8, min(pk), max(pk) FROM w;",
            ),
            raw(
                "WITH w AS (UPDATE fz_o3_r SET pk = pk + 201 WHERE pk BETWEEN 210 AND 214 \
                 RETURNING pk) SELECT count(*)::int8 FROM w;",
            ),
            // Referenced row, collision-free target -> the FK arm (23503)
            // fires, not the pkey.
            raw("UPDATE fz_o3_r SET pk = pk + 301 WHERE pk = 21;"),
        ],
        // MERGE into the partitioned tree: matched update, matched
        // delete, not-matched insert (routed incl. attach-mapped), NOT
        // MATCHED BY SOURCE, merge_action() RETURNING.
        vec![raw(
            "WITH w AS (MERGE INTO fz_o3_r t USING \
             (SELECT i * 7 AS k, i FROM generate_series(1, 90) i) v ON t.pk = v.k \
             WHEN MATCHED AND v.i % 10 = 0 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET a = t.a + v.i \
             WHEN NOT MATCHED AND v.k < 600 THEN INSERT VALUES (v.k, v.i, 'mg') \
             RETURNING merge_action() AS act, t.pk) \
             SELECT act, count(*)::int8 FROM w GROUP BY act ORDER BY act;",
        )],
        vec![raw(
            "WITH w AS (MERGE INTO fz_o3_r t USING \
             (SELECT i * 11 AS k FROM generate_series(1, 30) i) v ON t.pk = v.k \
             WHEN MATCHED THEN UPDATE SET t = t.t || '+' \
             WHEN NOT MATCHED BY SOURCE AND t.pk < 25 THEN UPDATE SET a = t.a - 1 \
             RETURNING t.pk) SELECT count(*)::int8, min(pk) FROM w;",
        )],
        // Multi-partition UPDATE with per-partition RETURNING old/new.
        vec![raw(
            "WITH w AS (UPDATE fz_o3_r SET a = a + 1 WHERE pk % 97 = 0 \
             RETURNING old.a AS oa, new.a AS na) \
             SELECT count(*)::int8, sum(na - oa)::int8 FROM w;",
        )],
    ];
    for body in bodies {
        let p = [DEFAULTP, SEQ, IDX][g.rng.below_usize(3)];
        g.fire2("opt3:prof:", p.name);
        v.extend(rollback_bracket(&p, body));
    }
    v.extend(raws(&[
        "DROP TABLE fz_o3_ref;",
        "DROP VIEW fz_o3_rv;",
        "DROP TABLE fz_o3_r;",
    ]));
    v
}

// ------------------------------------------------------ index scan keys --

/// ExecIndexBuildScanKeys / ExecIndexEvalArrayKeys /
/// ExecIndexAdvanceArrayKeys residue: RowCompare keys against composite
/// (incl. DESC) indexes, non-const SAOP arrays (runtime-evaluated),
/// NULL-bearing arrays, IS [NOT] NULL keys, backward array scans.
fn gen_idxkeys(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_k (pk int4 PRIMARY KEY, a int4, b int4, n int4, s text);",
        "INSERT INTO fz_o3_k SELECT i, (i * 13) % 60, (i * 7) % 25, \
         CASE WHEN i % 6 = 0 THEN NULL ELSE i % 15 END, 'k' || (i % 31) \
         FROM generate_series(1, 700) i;",
        "CREATE INDEX fz_o3_ki_ab ON fz_o3_k (a, b);",
        "CREATE INDEX fz_o3_ki_ad ON fz_o3_k (a DESC, b ASC);",
        "CREATE INDEX fz_o3_ki_n ON fz_o3_k (n);",
        "CREATE INDEX fz_o3_ki_s ON fz_o3_k (s);",
        "ANALYZE fz_o3_k;",
    ]);
    let queries: Vec<String> = vec![
        // RowCompare against the ASC composite (> and <, tight and open).
        "SELECT count(*)::int8 FROM fz_o3_k WHERE (a, b) > (30, 10) AND (a, b) < (45, 20);"
            .into(),
        "SELECT pk FROM fz_o3_k WHERE (a, b) >= (55, 0) ORDER BY pk;".into(),
        // RowCompare against the DESC composite index.
        "SELECT count(*)::int8 FROM fz_o3_k WHERE (a, b) < (10, 5);".into(),
        // Non-const SAOP array (runtime ExecIndexEvalArrayKeys): array
        // from a scalar initplan (cast makes it the ANY(array-expression)
        // form, not ANY(subquery)).
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = ANY \
         (CAST((SELECT array_agg(DISTINCT (x * 3) % 60) FROM generate_series(1, 8) x) \
          AS int4[]));"
            .into(),
        // NULL elements and empty arrays in SAOP keys.
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = ANY (ARRAY[3, NULL, 17, NULL, 55]);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = ANY (ARRAY[NULL::int4, NULL]);".into(),
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = ANY ('{}'::int4[]);".into(),
        // Multi-SAOP (advance-array-keys interplay) + saop on second col.
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = ANY ('{5,18,31,44}'::int4[]) \
         AND b = ANY ('{1,7,13,19}'::int4[]);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = 5 AND b = ANY ('{2,4,6,8}'::int4[]);"
            .into(),
        // Backward/ordered scans over array keys.
        "SELECT a, b FROM fz_o3_k WHERE a = ANY ('{9,22,35}'::int4[]) \
         ORDER BY a DESC, b DESC, pk DESC LIMIT 12;"
            .into(),
        "SELECT a FROM fz_o3_k WHERE a = ANY ('{48,9,22}'::int4[]) \
         ORDER BY a, pk LIMIT 10;"
            .into(),
        // IS NULL / IS NOT NULL scan keys.
        "SELECT count(*)::int8 FROM fz_o3_k WHERE n IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_o3_k WHERE n IS NOT NULL AND n < 4;".into(),
        "SELECT n FROM fz_o3_k WHERE n IS NULL OR n = 7 ORDER BY n NULLS FIRST, pk LIMIT 9;"
            .into(),
        // Cross-type comparisons (int8/int2 consts against int4 cols).
        "SELECT count(*)::int8 FROM fz_o3_k WHERE a = 33::int8 AND b >= 4::int2;".into(),
        "SELECT count(*)::int8 FROM fz_o3_k WHERE (a, b) > (20::int8, 5::int8);".into(),
        // Text index with row-compare and prefix arms.
        "SELECT count(*)::int8 FROM fz_o3_k WHERE s > 'k2' AND s < 'k28';".into(),
        "SELECT count(*)::int8 FROM fz_o3_k WHERE s = ANY ('{k1,k11,k21}'::text[]);".into(),
    ];
    v.extend(sweep(g, &[IDX, BITMAP, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_o3_k;"));
    v
}

// -------------------------------------------------- SQL function retval --

/// check_sql_stmt_retval coercion shapes + fmgr_sql arms: composite
/// returns needing per-column coercion, table rowtypes with a DROPPED
/// column, OUT params, polymorphic returns, DML-RETURNING final
/// statements, VOID tails, and the declared-mismatch error arms
/// (matched 42P13).
fn gen_sqlfn(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_sf (pk int4 PRIMARY KEY, a int2, b int8, t text);",
        "INSERT INTO fz_o3_sf SELECT i, (i % 100)::int2, i * 10, 'f' || (i % 6) \
         FROM generate_series(1, 150) i;",
        // Rowtype target with a dropped column (the attnum-remap arm).
        "CREATE TABLE fz_o3_sd (pk int4, gone int8, v int4);",
        "ALTER TABLE fz_o3_sd DROP COLUMN gone;",
        "INSERT INTO fz_o3_sd SELECT i, i * 2 FROM generate_series(1, 40) i;",
        "ANALYZE fz_o3_sf;",
        "ANALYZE fz_o3_sd;",
        "CREATE TYPE fz_o3_ct AS (x int8, y text);",
        // Composite return whose SELECT emits int4/unknown -> coercion.
        "CREATE FUNCTION fz_o3_f1(k int4) RETURNS fz_o3_ct LANGUAGE sql STABLE \
         AS 'SELECT k + 1, ''lit'' FROM fz_o3_sf WHERE pk = k';",
        // SETOF composite with column coercion in each arm of a UNION ALL
        // (setop dummy-tlist arm: tlist not modifiable).
        "CREATE FUNCTION fz_o3_f2(k int4) RETURNS SETOF fz_o3_ct LANGUAGE sql STABLE \
         AS 'SELECT pk::int8, t FROM fz_o3_sf WHERE pk <= k UNION ALL \
             SELECT 0::int8, ''z'' ORDER BY 1';",
        // Table rowtype with dropped column.
        "CREATE FUNCTION fz_o3_f3(k int4) RETURNS SETOF fz_o3_sd LANGUAGE sql STABLE \
         AS 'SELECT pk, v FROM fz_o3_sd WHERE pk <= k ORDER BY pk';",
        // OUT params (record shape derived from OUTs) + coercion.
        "CREATE FUNCTION fz_o3_f4(IN k int4, OUT s int8, OUT c int8) LANGUAGE sql STABLE \
         AS 'SELECT sum(b), count(*) FROM fz_o3_sf WHERE pk <= k';",
        // Polymorphic return.
        "CREATE FUNCTION fz_o3_f5(x anyelement) RETURNS anyelement LANGUAGE sql \
         IMMUTABLE AS 'SELECT x';",
        // DML-RETURNING tails (INSERT / UPDATE / MERGE RETURNING feed the
        // retval through returningList).
        "CREATE FUNCTION fz_o3_f6(k int4) RETURNS int8 LANGUAGE sql VOLATILE \
         AS 'INSERT INTO fz_o3_sf VALUES (k, 1, 2, ''ins'') RETURNING b + a';",
        "CREATE FUNCTION fz_o3_f7(k int4) RETURNS SETOF fz_o3_sf LANGUAGE sql VOLATILE \
         AS 'UPDATE fz_o3_sf SET a = a + 1 WHERE pk <= k RETURNING *';",
        "CREATE FUNCTION fz_o3_f8(k int4) RETURNS int4 LANGUAGE sql VOLATILE \
         AS 'MERGE INTO fz_o3_sd t USING (SELECT k AS kk) v ON t.pk = v.kk \
             WHEN MATCHED THEN UPDATE SET v = t.v + 1 \
             WHEN NOT MATCHED THEN INSERT VALUES (v.kk, 0) RETURNING t.v';",
        // VOID tail: final SELECT result discarded.
        "CREATE FUNCTION fz_o3_f9() RETURNS void LANGUAGE sql STABLE \
         AS 'SELECT count(*) FROM fz_o3_sf';",
        // BEGIN ATOMIC body (parsed eagerly, retval checked at CREATE).
        "CREATE FUNCTION fz_o3_fa(k int4) RETURNS int8 LANGUAGE sql IMMUTABLE \
         BEGIN ATOMIC SELECT (k * 3)::int8; END;",
    ]);
    // Probes (reads swept under two profiles; writes rolled back).
    let queries: Vec<String> = vec![
        "SELECT (fz_o3_f1(7)).x, (fz_o3_f1(7)).y;".into(),
        "SELECT x, y FROM fz_o3_f2(5) ORDER BY x, y;".into(),
        "SELECT pk, v FROM fz_o3_f3(9) ORDER BY pk;".into(),
        "SELECT s, c FROM fz_o3_f4(60);".into(),
        "SELECT fz_o3_f5(41), fz_o3_f5('tx'::text), fz_o3_f5(ARRAY[1, 2])::text;".into(),
        "SELECT fz_o3_f9() IS NULL;".into(),
        "SELECT fz_o3_fa(14);".into(),
        "SELECT count(*)::int8 FROM fz_o3_sf s WHERE s.b > (fz_o3_f1(s.pk % 20 + 1)).x;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, PARALLEL], &queries));
    v.extend(rollback_bracket(
        &DEFAULTP,
        vec![
            raw("SELECT fz_o3_f6(900);"),
            raw("SELECT count(*)::int8, sum(a)::int8 FROM fz_o3_f7(25) q;"),
            raw("SELECT fz_o3_f8(11), fz_o3_f8(999);"),
        ],
    ));
    // Declared-mismatch error arms (matched 42P13 both sides): wrong
    // column count, un-coercible column type, utility-tail function.
    v.extend(raws(&[
        "CREATE FUNCTION fz_o3_bad1() RETURNS fz_o3_ct LANGUAGE sql \
         AS 'SELECT 1::int8';",
        "CREATE FUNCTION fz_o3_bad2() RETURNS int4 LANGUAGE sql \
         AS 'SELECT ''(1,2)''::point';",
        "CREATE FUNCTION fz_o3_bad3() RETURNS int4 LANGUAGE sql \
         AS 'CREATE TABLE fz_o3_never (x int4)';",
    ]));
    v.extend(raws(&[
        "DROP FUNCTION fz_o3_f1, fz_o3_f2, fz_o3_f3, fz_o3_f4, fz_o3_f5, fz_o3_f6, \
         fz_o3_f7, fz_o3_f8, fz_o3_f9, fz_o3_fa;",
        "DROP TYPE fz_o3_ct;",
        "DROP TABLE fz_o3_sf, fz_o3_sd;",
    ]));
    v
}

// -------------------------------------------------- limit/rescan/subplan --

/// ExecLimit backward + rescan arms (SCROLL cursors through Limit /
/// Sort / WindowAgg / MergeJoin), ExecReScan dispatch over odd LATERAL
/// inners, and ExecScanSubPlan/ExecHashSubPlan null-handling (hashed
/// multi-column subplans with NULLs on both sides, unhashable
/// row-comparison subplans).
fn gen_scanmisc(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o3_m (pk int4 PRIMARY KEY, a int4, b int4, nn int4, mm int4);",
        "INSERT INTO fz_o3_m SELECT i, (i * 3) % 40, (i * 7) % 15, \
         CASE WHEN i % 5 = 0 THEN NULL ELSE i % 12 END, \
         CASE WHEN i % 7 = 0 THEN NULL ELSE i % 9 END FROM generate_series(1, 500) i;",
        "CREATE INDEX ON fz_o3_m (a);",
        "ANALYZE fz_o3_m;",
    ]);
    // SCROLL cursors: forward/backward/absolute/relative through Limit,
    // Sort, WindowAgg and MergeJoin plans (ExecLimit direction-change
    // arms need SCROLL + backward fetches).
    let cursor_bodies: Vec<Vec<String>> = vec![
        vec![
            "DECLARE fz_o3_cur1 SCROLL CURSOR FOR SELECT pk, a FROM fz_o3_m \
             ORDER BY pk LIMIT 40 OFFSET 5;"
                .into(),
            "FETCH FORWARD 15 FROM fz_o3_cur1;".into(),
            "FETCH BACKWARD 7 FROM fz_o3_cur1;".into(),
            "FETCH ABSOLUTE 30 FROM fz_o3_cur1;".into(),
            "FETCH BACKWARD ALL IN fz_o3_cur1;".into(),
            "FETCH ALL FROM fz_o3_cur1;".into(),
            "CLOSE fz_o3_cur1;".into(),
        ],
        vec![
            "DECLARE fz_o3_cur2 SCROLL CURSOR FOR SELECT pk, \
             count(*) OVER (PARTITION BY b ORDER BY pk) AS c FROM fz_o3_m ORDER BY pk;"
                .into(),
            "FETCH 25 FROM fz_o3_cur2;".into(),
            "FETCH BACKWARD 10 FROM fz_o3_cur2;".into(),
            "FETCH RELATIVE -5 IN fz_o3_cur2;".into(),
            "MOVE FORWARD 100 IN fz_o3_cur2;".into(),
            "FETCH BACKWARD 3 FROM fz_o3_cur2;".into(),
            "CLOSE fz_o3_cur2;".into(),
        ],
        vec![
            "DECLARE fz_o3_cur3 SCROLL CURSOR FOR SELECT x.pk, y.pk FROM fz_o3_m x \
             JOIN fz_o3_m y ON x.a = y.a WHERE x.b = 3 ORDER BY x.pk, y.pk;"
                .into(),
            "FETCH 20 FROM fz_o3_cur3;".into(),
            "FETCH BACKWARD 12 FROM fz_o3_cur3;".into(),
            "FETCH FIRST FROM fz_o3_cur3;".into(),
            "FETCH LAST FROM fz_o3_cur3;".into(),
            "FETCH BACKWARD 5 FROM fz_o3_cur3;".into(),
            "CLOSE fz_o3_cur3;".into(),
        ],
        vec![
            "DECLARE fz_o3_cur4 SCROLL CURSOR FOR SELECT pk FROM fz_o3_m \
             ORDER BY pk FETCH FIRST 30 ROWS WITH TIES;"
                .into(),
            "FETCH ALL FROM fz_o3_cur4;".into(),
            "FETCH BACKWARD ALL FROM fz_o3_cur4;".into(),
            "FETCH 10 FROM fz_o3_cur4;".into(),
            "CLOSE fz_o3_cur4;".into(),
        ],
    ];
    for (i, body) in cursor_bodies.into_iter().enumerate() {
        let p = if i % 2 == 0 { CURSOR_FRAC } else { MERGEJ };
        g.fire2("opt3:prof:", p.name);
        v.extend(rollback_bracket(&p, body.into_iter().map(raw).collect()));
    }
    // LATERAL rescans over odd inners (ExecReScan dispatch arms) and
    // subplan null-handling.
    let queries: Vec<String> = vec![
        // Rescanned SetOp / grouping-sets / window inners.
        "SELECT d.b, l.c FROM (SELECT DISTINCT b FROM fz_o3_m) d, LATERAL \
         (SELECT count(*)::int8 AS c FROM \
          (SELECT a FROM fz_o3_m WHERE b = d.b INTERSECT SELECT a FROM fz_o3_m \
           WHERE b = (d.b + 1) % 15) z) l ORDER BY d.b;"
            .into(),
        "SELECT d.b, l.s FROM (SELECT DISTINCT b FROM fz_o3_m WHERE b < 6) d, LATERAL \
         (SELECT sum(cnt)::int8 AS s FROM (SELECT count(*)::int8 AS cnt FROM fz_o3_m \
          WHERE b = d.b GROUP BY GROUPING SETS ((a), (a, pk % 2), ())) gs) l ORDER BY d.b;"
            .into(),
        "SELECT d.b, l.m FROM (SELECT DISTINCT b FROM fz_o3_m WHERE b < 5) d, LATERAL \
         (SELECT max(rn)::int8 AS m FROM (SELECT row_number() OVER (ORDER BY pk) AS rn \
          FROM fz_o3_m WHERE b = d.b) w) l ORDER BY d.b;"
            .into(),
        // Hashed subplan, single col, NULLs both sides (IN / NOT IN
        // three-valued arms).
        "SELECT count(*)::int8 FROM fz_o3_m WHERE nn IN (SELECT mm FROM fz_o3_m WHERE pk < 90);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_m WHERE nn NOT IN \
         (SELECT mm FROM fz_o3_m WHERE pk < 40 AND mm IS NOT NULL);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_m WHERE nn NOT IN \
         (SELECT mm FROM fz_o3_m WHERE pk < 40);"
            .into(),
        // Multi-column hashed subplan with NULL components (the
        // findPartialMatch / slotNoNulls arms of nodeSubplan).
        "SELECT count(*)::int8 FROM fz_o3_m WHERE (nn, mm) IN \
         (SELECT mm, nn FROM fz_o3_m WHERE pk < 120);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_m t WHERE (t.nn, t.b) NOT IN \
         (SELECT mm, b FROM fz_o3_m WHERE pk < 60);"
            .into(),
        // Unhashable subplan (row comparison <>): ExecScanSubPlan arms.
        "SELECT count(*)::int8 FROM fz_o3_m t WHERE ROW(t.a, t.b) <> ALL \
         (SELECT a, b FROM fz_o3_m WHERE pk < 25);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o3_m t WHERE t.a < ANY \
         (SELECT b * 3 FROM fz_o3_m WHERE pk < 30);"
            .into(),
        // Correlated EXISTS inside CASE (subplan under a conditional).
        "SELECT count(*)::int8 FROM fz_o3_m t WHERE CASE WHEN t.b < 7 THEN \
         EXISTS (SELECT 1 FROM fz_o3_m i WHERE i.a = t.a AND i.pk <> t.pk) ELSE false END;"
            .into(),
        // LIMIT edge arms: OFFSET beyond end, LIMIT 0 rescanned in a
        // lateral, WITH TIES boundary.
        "SELECT count(*)::int8 FROM fz_o3_m t, LATERAL \
         (SELECT pk FROM fz_o3_m i WHERE i.a = t.a ORDER BY pk LIMIT 0) l;"
            .into(),
        "SELECT pk FROM fz_o3_m ORDER BY pk OFFSET 495;".into(),
        "SELECT count(*)::int8 FROM (SELECT b FROM fz_o3_m ORDER BY b \
         FETCH FIRST 37 ROWS WITH TIES) z;"
            .into(),
    ];
    let hashmem = Prof {
        name: "hash_squeeze",
        gucs: &[("hash_mem_multiplier", "1"), ("work_mem", "'64kB'")],
    };
    v.extend(sweep(g, &[DEFAULTP, hashmem, NESTL_BARE], &queries));
    v.push(raw("DROP TABLE fz_o3_m;"));
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
        let mut productions = Vec::new();
        let mut out = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &weights, &mut productions, 4);
        for _ in 0..n {
            let stmts = gen_opt3_module(&mut g);
            out.push(stmts.iter().map(|s| s.to_sql()).collect());
        }
        out
    }

    #[test]
    fn groups_are_set_reset_balanced_and_self_contained() {
        for group in gen_groups(41, 200) {
            let mut sets: Vec<String> = Vec::new();
            let mut open_txn = 0i32;
            let joined = group.join("\n");
            for sql in &group {
                if sql == "BEGIN;" {
                    open_txn += 1;
                } else if sql == "ROLLBACK;" {
                    open_txn -= 1;
                } else if let Some(rest) = sql.strip_prefix("SET LOCAL ") {
                    assert!(open_txn > 0, "SET LOCAL outside txn: {sql}");
                    let _ = rest;
                } else if let Some(rest) = sql.strip_prefix("SET ") {
                    sets.push(rest.split(' ').next().unwrap().to_string());
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = sets
                        .iter()
                        .rposition(|s| *s == name)
                        .unwrap_or_else(|| panic!("RESET {name} without SET in {group:?}"));
                    sets.remove(pos);
                }
                if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                    let t = rest.split_whitespace().next().unwrap();
                    // Partition children (_cN and the attach-mapped _x)
                    // are dropped via their parent.
                    if !t.contains("_c") && !t.ends_with("_x") {
                        assert!(
                            joined.contains(&format!("DROP TABLE {t}"))
                                || joined.contains(&format!("DROP TABLE {t},"))
                                || joined.contains(&format!("{t};"))
                                    && joined.contains("DROP TABLE")
                                || joined.contains(&format!(", {t}")),
                            "no in-group DROP for {t} in {group:?}"
                        );
                    }
                }
            }
            assert!(sets.is_empty(), "unRESET SETs {sets:?} in {group:?}");
            assert_eq!(open_txn, 0, "unclosed BEGIN in {group:?}");
        }
    }

    #[test]
    fn writes_and_cursors_only_inside_rollback_brackets() {
        // Probe-time writes (WITH w AS (INSERT/UPDATE/DELETE/MERGE ...),
        // bare MERGE/UPDATE/INSERT error arms, volatile-fn probes) and
        // all cursor DECLARE/FETCH traffic stay inside BEGIN..ROLLBACK.
        // Fixture bulk loads (INSERT INTO fz_o3_* SELECT ... FROM
        // generate_series) run outside and are exempt: they are part of
        // create-fixture state, identical on both sides.
        for group in gen_groups(43, 200) {
            let mut open = 0i32;
            for sql in &group {
                if sql == "BEGIN;" {
                    open += 1;
                } else if sql == "ROLLBACK;" {
                    open -= 1;
                }
                let is_probe_write = sql.starts_with("WITH w AS (INSERT")
                    || sql.starts_with("WITH w AS (UPDATE")
                    || sql.starts_with("WITH w AS (DELETE")
                    || sql.starts_with("WITH w AS (MERGE")
                    || sql.starts_with("UPDATE ")
                    || sql.starts_with("DECLARE ")
                    || sql.starts_with("FETCH ")
                    || sql.starts_with("MOVE ")
                    || (sql.starts_with("INSERT INTO fz_o3_rv"));
                if is_probe_write {
                    assert!(open > 0, "write/cursor outside rollback bracket: {sql}");
                }
            }
            assert_eq!(open, 0, "unclosed BEGIN in {group:?}");
        }
    }

    #[test]
    fn row_returning_sweep_statements_are_totally_ordered() {
        for group in gen_groups(47, 300) {
            for sql in &group {
                if !sql.starts_with("SELECT ") && !sql.starts_with("WITH ") {
                    continue;
                }
                let aggregate_only = sql.starts_with("SELECT count(")
                    || sql.starts_with("SELECT sum(")
                    || sql.starts_with("SELECT NULL")
                    || sql.starts_with("SELECT ROW(")
                    || sql.starts_with("SELECT NULLIF(")
                    || sql.starts_with("SELECT GREATEST(")
                    || sql.starts_with("SELECT true ")
                    || sql.starts_with("SELECT current_date")
                    || sql.starts_with("SELECT (")
                    || sql.starts_with("SELECT 5::")
                    || sql.starts_with("SELECT (-1)::")
                    || sql.starts_with("SELECT fz_o3_")
                    || sql.starts_with("SELECT s, c FROM fz_o3_f4")
                    || sql.starts_with("WITH w AS ")
                    || sql.starts_with("WITH RECURSIVE r(n) AS ")
                    || sql.starts_with("WITH RECURSIVE walk")
                    || sql.starts_with("WITH c AS MATERIALIZED")
                    || !sql.contains(" FROM ");
                if !aggregate_only {
                    assert!(
                        sql.contains("ORDER BY") || sql.contains("LIMIT"),
                        "row-returning statement without total order: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn stream_is_seed_deterministic() {
        assert_eq!(gen_groups(53, 80), gen_groups(53, 80));
    }

    #[test]
    fn every_family_reachable() {
        let mut seen = std::collections::HashSet::new();
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(3);
        let mut productions = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &weights, &mut productions, 4);
        for _ in 0..400 {
            let _ = gen_opt3_module(&mut g);
        }
        for p in productions.iter() {
            if let Some(s) = p.strip_prefix("opt3:shape:") {
                seen.insert(s.to_string());
            }
        }
        assert!(
            seen.len() >= SHAPES.len() - 1,
            "families never picked: {seen:?}"
        );
    }
}
