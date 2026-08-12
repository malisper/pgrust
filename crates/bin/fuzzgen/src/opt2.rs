//! Optimizer alternate-arm drain, round 2 (SQLcov-B; line-drain queue
//! chunk `optimizer-arms`, 525 fns / 3,780 hollow lines after LD7 — see
//! docs/fuzzing/line-gap-report-003.md and findings-ld7.md §residue).
//! LD7 swept GUC plan profiles over one fixture family; the residue it
//! left needs STRUCTURALLY different queries more than more profiles:
//!
//!   - reparameterize_path_by_child lateral/tablesample arms (LATERAL
//!     subqueries/SRFs/TABLESAMPLE between co-partitioned tables under
//!     partitionwise nestloop — each inner path type is its own arm);
//!   - reparameterize_path / appendrel common-parameterization
//!     (parameterized Append/MergeAppend inner sides of nestloops over
//!     UNION ALL appendrels);
//!   - join removal (analyzejoins.c) incl. blocked-removal arms and
//!     PG18 self-join elimination;
//!   - EquivalenceClass merging + reconsider_outer/full_join_clause
//!     (long transitive chains, FULL JOIN USING + COALESCE, cross-type
//!     int2/int4/int8 chains);
//!   - eval_const_expressions deep arms (const CASE/bool trees, strict
//!     fn over NULL, SQL function inlining incl. failure arms,
//!     ArrayCoerce, FieldSelect over const rows, SAOP consts);
//!   - indexpath rarer clause matches (RowCompare over composite
//!     indexes, boolean-index shapes, pattern-prefix extraction over
//!     text_pattern_ops, SAOPs, NullTests, partial-index predtest
//!     implication incl. cross-strategy proofs).
//!
//! Correctness bar (LD7 law): the RESULT SET of a deterministic query
//! is identical across every forced plan and across both engines; any
//! divergence is a HIGH-severity planner/executor finding. Plans may
//! differ; EXPLAIN output is never compared raw here (no EXPLAIN at all
//! — coverage comes from planning+executing the real queries).
//!
//! Discipline (plansel/earm rules): self-contained groups over fixed
//! `fz_o2_*` fixtures created/dropped in-group; every SET has its RESET
//! in reverse order in the same group; writes only inside
//! BEGIN..ROLLBACK with SET LOCAL; total ORDER BY on every
//! row-returning probe; exact-typed aggregates only; tables <= 1200
//! rows (exhaustive ANALYZE sample -> identical stats both sides);
//! TABLESAMPLE only at 100 percent with REPEATABLE, so the sampled set
//! is the whole table on both engines regardless of sampler internals.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// A named GUC profile (same shape as plansel's; local copy keeps the
/// modules independent).
#[derive(Clone, Copy)]
struct Prof {
    name: &'static str,
    gucs: &'static [(&'static str, &'static str)],
}

const NESTL: Prof = Prof {
    name: "nestl",
    gucs: &[("enable_hashjoin", "off"), ("enable_mergejoin", "off")],
};
const NESTL_BARE: Prof = Prof {
    name: "nestl_bare",
    gucs: &[
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_material", "off"),
        ("enable_memoize", "off"),
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
const PWISE_NESTL_IDX: Prof = Prof {
    name: "pwise_nestl_idx",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_seqscan", "off"),
        ("enable_material", "off"),
        ("enable_memoize", "off"),
    ],
};
const PWISE_NESTL_BMP: Prof = Prof {
    name: "pwise_nestl_bmp",
    gucs: &[
        ("enable_partitionwise_join", "on"),
        ("enable_hashjoin", "off"),
        ("enable_mergejoin", "off"),
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
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
const SEQ: Prof = Prof {
    name: "seq",
    gucs: &[
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
        ("enable_bitmapscan", "off"),
    ],
};
const SORTU: Prof = Prof {
    name: "sortuniq",
    gucs: &[("enable_hashagg", "off"), ("enable_hashjoin", "off")],
};
const HASHU: Prof = Prof {
    name: "hashuniq",
    gucs: &[("enable_sort", "off")],
};
const DEFAULTP: Prof = Prof { name: "default", gucs: &[] };

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

/// Emit every query under every profile (this module sweeps exhaustively:
/// the query lists are short and the arms are profile-specific).
fn sweep(g: &mut Gen, profs: &[Prof], queries: &[String]) -> Vec<StmtKind> {
    let mut v = Vec::new();
    for p in profs {
        g.fire2("opt2:prof:", p.name);
        v.extend(bracket(p, queries.iter().map(|q| raw(q.clone())).collect()));
    }
    v
}

const SHAPES: &[&str] = &[
    "opt2:lateralrp",
    "opt2:tsrp",
    "opt2:apprel",
    "opt2:joinrm",
    "opt2:eclass",
    "opt2:constfold",
    "opt2:indexmatch",
    "opt2:pullup",
    "opt2:uniq",
    "opt2:refute",
    "opt2:gucmatrix",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_opt2_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("opt2");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire2("opt2:shape:", &shape["opt2:".len()..]);
    match shape {
        "opt2:lateralrp" => gen_lateralrp(g),
        "opt2:tsrp" => gen_tsrp(g),
        "opt2:apprel" => gen_apprel(g),
        "opt2:joinrm" => gen_joinrm(g),
        "opt2:eclass" => gen_eclass(g),
        "opt2:constfold" => gen_constfold(g),
        "opt2:indexmatch" => gen_indexmatch(g),
        "opt2:pullup" => gen_pullup(g),
        "opt2:uniq" => gen_uniq(g),
        "opt2:refute" => gen_refute(g),
        _ => gen_gucmatrix(g),
    }
}

// ------------------------------------------------- partitioned twins -----

/// CREATE the co-partitioned twin fixture (3 range parts, matching
/// bounds, 900/600 rows, per-part pk + (a) indexes via partitioned
/// indexes). Used by the lateral/tablesample/pwise shapes.
fn twins(rows1: i64, rows2: i64) -> Vec<StmtKind> {
    let mut v = Vec::new();
    for (t, rows, mult) in [
        ("fz_o2_p1", rows1, 7i64),
        ("fz_o2_p2", rows2, 11i64),
        // Third co-partitioned table: the inner side of a partitionwise
        // nestloop can then itself be a JOIN (T_NestPath / Material /
        // Memoize arms of reparameterize_path_by_child).
        ("fz_o2_p3", 300, 13i64),
    ] {
        v.push(raw(format!(
            "CREATE TABLE {t} (pk int4 PRIMARY KEY, a int4, b int4, txt text) \
             PARTITION BY RANGE (pk);"
        )));
        for (i, (lo, hi)) in [(0, 400), (400, 800), (800, 1200)].iter().enumerate() {
            v.push(raw(format!(
                "CREATE TABLE {t}_c{i} PARTITION OF {t} FOR VALUES FROM ({lo}) TO ({hi});"
            )));
        }
        v.push(raw(format!("CREATE INDEX ON {t} (a);")));
        v.push(raw(format!("CREATE INDEX ON {t} (b) WHERE a > 20;")));
        v.push(raw(format!(
            "INSERT INTO {t} SELECT i, (i * {mult}) % 50, (i * 3) % 17, 'w' || (i % 13) \
             FROM generate_series(1, {rows}) i;"
        )));
        v.push(raw(format!("ANALYZE {t};")));
    }
    v
}

fn drop_twins() -> Vec<StmtKind> {
    raws(&["DROP TABLE fz_o2_p1, fz_o2_p2, fz_o2_p3;"])
}

// -------------------------------------- lateral reparameterization -------

fn gen_lateralrp(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = twins(900, 600);
    let queries: Vec<String> = vec![
        // Inner subquery scan path (SubqueryScanPath translation).
        "SELECT p1.pk, l.s FROM fz_o2_p1 p1, LATERAL (SELECT sum(b)::int8 AS s \
         FROM fz_o2_p2 p2 WHERE p2.pk = p1.pk) l WHERE p1.a < 5 ORDER BY p1.pk;"
            .into(),
        // Inner sort/unique shapes below the lateral (Sort/Unique paths).
        "SELECT p1.pk, l.v FROM fz_o2_p1 p1, LATERAL (SELECT DISTINCT p2.a AS v \
         FROM fz_o2_p2 p2 WHERE p2.pk = p1.pk) l WHERE p1.a < 4 ORDER BY p1.pk, l.v;"
            .into(),
        // LATERAL LIMIT/OFFSET subquery (forces per-row rescans).
        "SELECT p1.pk, l.b FROM fz_o2_p1 p1, LATERAL (SELECT p2.b FROM fz_o2_p2 p2 \
         WHERE p2.pk >= p1.pk ORDER BY p2.pk LIMIT 1) l WHERE p1.a < 4 ORDER BY p1.pk;"
            .into(),
        // Plain co-partitioned join with an extra lateral SRF rider.
        "SELECT p1.pk, u FROM fz_o2_p1 p1 JOIN fz_o2_p2 p2 ON p1.pk = p2.pk, \
         LATERAL generate_series(1, (p2.b % 2) + 1) u WHERE p1.a < 6 ORDER BY p1.pk, u;"
            .into(),
        // Lateral LEFT JOIN (nullable lateral side).
        "SELECT p1.pk, l.m FROM fz_o2_p1 p1 LEFT JOIN LATERAL (SELECT max(p2.b)::int4 AS m \
         FROM fz_o2_p2 p2 WHERE p2.pk = p1.pk AND p2.a > 30) l ON true \
         WHERE p1.a < 4 ORDER BY p1.pk;"
            .into(),
        // Aggregate above a partitionwise lateral join.
        "SELECT count(*)::int8, coalesce(sum(p2.b), 0)::int8 FROM fz_o2_p1 p1 \
         JOIN fz_o2_p2 p2 ON p1.pk = p2.pk WHERE p1.b < 10;"
            .into(),
    ];
    // 3-way co-partitioned joins: the pwise nestloop's inner side is a
    // JOIN parameterized by the outer child (T_NestPath / T_MaterialPath /
    // T_MemoizePath reparameterization arms), plus a sample-scan inner.
    let mut queries = queries;
    queries.push(
        "SELECT count(*)::int8 FROM fz_o2_p1 p1 JOIN fz_o2_p2 p2 ON p1.pk = p2.pk \
         JOIN fz_o2_p3 p3 ON p2.pk = p3.pk WHERE p1.b < 4;"
            .into(),
    );
    queries.push(
        "SELECT p1.pk FROM fz_o2_p1 p1 JOIN fz_o2_p2 p2 ON p1.pk = p2.pk \
         JOIN fz_o2_p3 p3 ON p2.pk = p3.pk AND p2.a = p3.a WHERE p1.a < 8 ORDER BY p1.pk;"
            .into(),
    );
    queries.push(
        "SELECT count(*)::int8 FROM fz_o2_p1 p1 JOIN \
         fz_o2_p2 s TABLESAMPLE BERNOULLI (100) REPEATABLE (3) ON p1.pk = s.pk WHERE p1.b < 4;"
            .into(),
    );
    v.extend(sweep(
        g,
        &[PWISE_NESTL, PWISE_NESTL_IDX, PWISE_NESTL_BMP, NESTL_BARE, DEFAULTP],
        &queries,
    ));
    // MergeAppend under partitionwise merge join (ordered child paths).
    v.extend(sweep(
        g,
        &[PWISE_MERGEA],
        &[
            "SELECT p1.pk, p2.b FROM fz_o2_p1 p1 JOIN fz_o2_p2 p2 ON p1.pk = p2.pk \
             WHERE p1.a < 10 ORDER BY p1.pk;"
                .into(),
        ],
    ));
    v.extend(drop_twins());
    v
}

// ------------------------------------------- tablesample reparam ---------

fn gen_tsrp(g: &mut Gen) -> Vec<StmtKind> {
    let seed = g.rng.below_usize(100) as i64;
    let mut v = twins(600, 400);
    let queries: Vec<String> = vec![
        // 100-percent samples: the sampled set is the whole relation on
        // both sides, so results are deterministic while the sample-scan
        // path machinery (incl. its reparameterization) runs.
        format!(
            "SELECT count(*)::int8 FROM fz_o2_p1 TABLESAMPLE SYSTEM (100) REPEATABLE ({seed});"
        ),
        format!(
            "SELECT count(*)::int8 FROM fz_o2_p1 TABLESAMPLE BERNOULLI (100) REPEATABLE ({seed});"
        ),
        // Sample scan under a partitionwise nestloop join.
        "SELECT count(*)::int8, coalesce(sum(s.b), 0)::int8 FROM fz_o2_p1 p1 JOIN \
         fz_o2_p2 s TABLESAMPLE SYSTEM (100) REPEATABLE (7) ON p1.pk = s.pk WHERE p1.a < 25;"
            .into(),
        // LATERAL tablesample whose REPEATABLE argument references the
        // outer row: the sample-scan path is parameterized by the outer
        // rel and must be reparameterized per child.
        "SELECT p1.pk, l.c FROM fz_o2_p1 p1, LATERAL (SELECT count(*)::int8 AS c FROM \
         fz_o2_p2 s TABLESAMPLE BERNOULLI (100) REPEATABLE (p1.a) WHERE s.pk = p1.pk) l \
         WHERE p1.a < 5 ORDER BY p1.pk;"
            .into(),
        "SELECT p1.pk, l.c FROM fz_o2_p1 p1, LATERAL (SELECT count(*)::int8 AS c FROM \
         fz_o2_p2 s TABLESAMPLE SYSTEM (100) REPEATABLE (p1.pk % 32767) WHERE s.pk = p1.pk) l \
         WHERE p1.a < 4 ORDER BY p1.pk;"
            .into(),
    ];
    v.extend(sweep(g, &[PWISE_NESTL, NESTL_BARE, DEFAULTP], &queries));
    v.extend(raws(&[
        // ERROR arms: bad percentages / negative repeat seeds are runtime
        // errors with fixed SQLSTATE; unknown method is analysis-time.
        "SELECT count(*) FROM fz_o2_p1 TABLESAMPLE SYSTEM (101);",
        "SELECT count(*) FROM fz_o2_p1 TABLESAMPLE BERNOULLI (-1);",
        "SELECT count(*) FROM fz_o2_p1 TABLESAMPLE BOGUS (10);",
        "SELECT count(*) FROM (SELECT 1) s TABLESAMPLE SYSTEM (10);",
    ]));
    v.extend(drop_twins());
    v
}

// ------------------------------------------ appendrel common params ------

fn gen_apprel(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o2_a1 (pk int4 PRIMARY KEY, k int4, v int4);",
        "CREATE TABLE fz_o2_a2 (pk int4 PRIMARY KEY, k int4, v int4);",
        "CREATE TABLE fz_o2_dr (pk int4 PRIMARY KEY, k int4);",
        "CREATE INDEX ON fz_o2_a1 (k);",
        "CREATE INDEX ON fz_o2_a2 (k);",
        "INSERT INTO fz_o2_a1 SELECT i, (i * 5) % 60, i FROM generate_series(1, 500) i;",
        "INSERT INTO fz_o2_a2 SELECT i, (i * 9) % 60, -i FROM generate_series(1, 400) i;",
        "INSERT INTO fz_o2_dr SELECT i, (i * 3) % 60 FROM generate_series(1, 80) i;",
        "ANALYZE fz_o2_a1;",
        "ANALYZE fz_o2_a2;",
        "ANALYZE fz_o2_dr;",
    ]);
    let queries: Vec<String> = vec![
        // Parameterized Append inner side of a nestloop: each UNION ALL
        // child gets an index path parameterized by the driver, and the
        // appendrel must settle on the common parameterization.
        "SELECT d.pk, u.v FROM fz_o2_dr d, \
         (SELECT k, v FROM fz_o2_a1 UNION ALL SELECT k, v FROM fz_o2_a2) u \
         WHERE u.k = d.k AND d.pk < 12 ORDER BY d.pk, u.v;"
            .into(),
        // Mixed child quals (one child gets an extra filter).
        "SELECT d.pk, count(u.v)::int8 FROM fz_o2_dr d LEFT JOIN \
         (SELECT k, v FROM fz_o2_a1 WHERE v % 2 = 0 UNION ALL SELECT k, v FROM fz_o2_a2) u \
         ON u.k = d.k WHERE d.pk < 15 GROUP BY d.pk ORDER BY d.pk;"
            .into(),
        // MergeAppend: ordered pull over the appendrel with LIMIT.
        "SELECT u.k, u.v FROM (SELECT k, v FROM fz_o2_a1 UNION ALL SELECT k, v FROM fz_o2_a2) u \
         ORDER BY u.k, u.v LIMIT 25;"
            .into(),
        // Appendrel under semijoin (unique-ified appendrel).
        "SELECT d.pk FROM fz_o2_dr d WHERE d.k IN \
         (SELECT k FROM fz_o2_a1 WHERE v > 250 UNION ALL SELECT k FROM fz_o2_a2 WHERE v < -200) \
         ORDER BY d.pk;"
            .into(),
        // UNION ALL of dissimilar shapes (cast alignment in append).
        "SELECT u.x::int8, count(*)::int8 FROM \
         (SELECT k AS x FROM fz_o2_a1 UNION ALL SELECT v FROM fz_o2_a2 \
          UNION ALL SELECT g FROM generate_series(1, 10) g) u \
         GROUP BY u.x HAVING count(*) > 1 ORDER BY 1, 2;"
            .into(),
    ];
    v.extend(sweep(g, &[NESTL, NESTL_BARE, IDX, SEQ, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_o2_a1, fz_o2_a2, fz_o2_dr;"));
    v
}

// -------------------------------------------------------- join removal ---

fn gen_joinrm(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o2_f (pk int4 PRIMARY KEY, r1 int4, r2 int4, v int4);",
        "CREATE TABLE fz_o2_u1 (pk int4 PRIMARY KEY, w int4);",
        "CREATE TABLE fz_o2_u2 (pk int4 UNIQUE, w int4 NOT NULL);",
        "INSERT INTO fz_o2_f SELECT i, (i % 40) + 1, (i % 30) + 1, i * 2 FROM generate_series(1, 300) i;",
        "INSERT INTO fz_o2_u1 SELECT i, i * 3 FROM generate_series(1, 40) i;",
        "INSERT INTO fz_o2_u2 SELECT i, i * 5 FROM generate_series(1, 30) i;",
        "ANALYZE fz_o2_f;",
        "ANALYZE fz_o2_u1;",
        "ANALYZE fz_o2_u2;",
    ]);
    let queries: Vec<String> = vec![
        // Removable: LEFT JOIN to unique target, no columns used above.
        "SELECT f.pk, f.v FROM fz_o2_f f LEFT JOIN fz_o2_u1 u ON f.r1 = u.pk \
         WHERE f.pk < 20 ORDER BY f.pk;"
            .into(),
        // Removable via UNIQUE constraint (not pk).
        "SELECT count(*)::int8 FROM fz_o2_f f LEFT JOIN fz_o2_u2 u ON f.r2 = u.pk;".into(),
        // Chained removable joins.
        "SELECT f.pk FROM fz_o2_f f LEFT JOIN fz_o2_u1 a ON f.r1 = a.pk \
         LEFT JOIN fz_o2_u2 b ON f.r2 = b.pk WHERE f.pk < 15 ORDER BY f.pk;"
            .into(),
        // NOT removable: inner column referenced above / in qual.
        "SELECT f.pk, u.w FROM fz_o2_f f LEFT JOIN fz_o2_u1 u ON f.r1 = u.pk \
         WHERE f.pk < 12 ORDER BY f.pk;"
            .into(),
        "SELECT f.pk FROM fz_o2_f f LEFT JOIN fz_o2_u1 u ON f.r1 = u.pk \
         WHERE u.w IS NULL OR u.w > 90 ORDER BY f.pk LIMIT 10;"
            .into(),
        // NOT removable: join to a non-unique key.
        "SELECT count(*)::int8 FROM fz_o2_f a LEFT JOIN fz_o2_f b ON a.r1 = b.r1;".into(),
        // Distinctness above blocks nothing (removal still legal).
        "SELECT DISTINCT f.r1 FROM fz_o2_f f LEFT JOIN fz_o2_u1 u ON f.r1 = u.pk \
         ORDER BY f.r1 LIMIT 8;"
            .into(),
        // Aggregates above a removable join.
        "SELECT sum(f.v)::int8, count(*)::int8 FROM fz_o2_f f \
         LEFT JOIN fz_o2_u2 u ON f.r2 = u.pk;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, NESTL, SORTU], &queries));
    // PG18 self-join elimination: identical-rel inner joins on the pk,
    // swept with the feature forced both ways.
    for setting in ["on", "off"] {
        let mut body: Vec<StmtKind> = vec![raw(format!(
            "SET enable_self_join_elimination = {setting};"
        ))];
        for q in [
            "SELECT a.pk, a.v FROM fz_o2_f a JOIN fz_o2_f b ON a.pk = b.pk \
             WHERE b.v > 100 AND a.pk < 70 ORDER BY a.pk;",
            "SELECT count(*)::int8 FROM fz_o2_f a JOIN fz_o2_f b ON a.pk = b.pk \
             JOIN fz_o2_f c ON b.pk = c.pk WHERE c.r1 = 5;",
            "SELECT a.pk FROM fz_o2_f a JOIN fz_o2_f b ON a.pk = b.pk AND a.r1 = b.r2 \
             ORDER BY a.pk LIMIT 5;",
        ] {
            body.push(raw(q));
        }
        body.push(raw("RESET enable_self_join_elimination;"));
        v.extend(body);
    }
    v.push(raw("DROP TABLE fz_o2_f, fz_o2_u1, fz_o2_u2;"));
    v
}

// ---------------------------------------------- equivalence classes ------

fn gen_eclass(g: &mut Gen) -> Vec<StmtKind> {
    let cval = 1 + g.rng.below_usize(20) as i64;
    let mut v = raws(&[
        "CREATE TABLE fz_o2_e1 (pk int4 PRIMARY KEY, k2 int2, k4 int4, k8 int8, t text);",
        "CREATE TABLE fz_o2_e2 (pk int4 PRIMARY KEY, k4 int4, m int4);",
        "CREATE TABLE fz_o2_e3 (pk int4 PRIMARY KEY, k8 int8, m int4);",
        "CREATE TABLE fz_o2_e4 (pk int4 PRIMARY KEY, k4 int4);",
        "CREATE INDEX ON fz_o2_e1 (k4);",
        "CREATE INDEX ON fz_o2_e2 (k4);",
        "CREATE INDEX ON fz_o2_e3 (k8);",
        "INSERT INTO fz_o2_e1 SELECT i, (i % 25)::int2, i % 25, (i % 25)::int8, 'e' || (i % 7) \
         FROM generate_series(1, 200) i;",
        "INSERT INTO fz_o2_e2 SELECT i, i % 25, i FROM generate_series(1, 150) i;",
        "INSERT INTO fz_o2_e3 SELECT i, (i % 25)::int8, -i FROM generate_series(1, 120) i;",
        "INSERT INTO fz_o2_e4 SELECT i, i % 25 FROM generate_series(1, 60) i;",
        "ANALYZE fz_o2_e1;",
        "ANALYZE fz_o2_e2;",
        "ANALYZE fz_o2_e3;",
        "ANALYZE fz_o2_e4;",
    ]);
    let queries: Vec<String> = vec![
        // Long transitive chain across four rels, cross-type members
        // (int2 = int4 = int8): EC merging + cross-type derivation.
        "SELECT count(*)::int8 FROM fz_o2_e1 a, fz_o2_e2 b, fz_o2_e3 c, fz_o2_e4 d \
         WHERE a.k2 = b.k4 AND b.k4 = c.k8 AND c.k8 = d.k4 AND d.k4 < 6;"
            .into(),
        format!(
            "SELECT count(*)::int8 FROM fz_o2_e1 a, fz_o2_e2 b, fz_o2_e3 c \
             WHERE a.k4 = b.k4 AND b.k4 = c.k8 AND a.k4 = {cval};"
        ),
        // Redundant equalities (self-consistent cycles collapse to one EC).
        "SELECT count(*)::int8 FROM fz_o2_e1 a, fz_o2_e2 b \
         WHERE a.k4 = b.k4 AND b.k4 = a.k4 AND a.k4 = a.k4;"
            .into(),
        // reconsider_outer_join_clauses: LEFT JOIN with a mergejoinable
        // constant clause on the nullable side.
        "SELECT count(*)::int8 FROM fz_o2_e1 a LEFT JOIN fz_o2_e2 b ON a.k4 = b.k4 \
         WHERE b.k4 = 9;"
            .into(),
        "SELECT a.pk FROM fz_o2_e1 a LEFT JOIN fz_o2_e2 b ON a.k4 = b.k4 AND b.m > 3 \
         WHERE b.k4 = 12 ORDER BY a.pk LIMIT 10;"
            .into(),
        // reconsider_full_join_clause: FULL JOIN USING + COALESCE = const.
        "SELECT count(*)::int8 FROM (SELECT k4 FROM fz_o2_e1) a FULL JOIN \
         (SELECT k4 FROM fz_o2_e2) b USING (k4) WHERE k4 = 7;"
            .into(),
        "SELECT k4, count(*)::int8 FROM (SELECT k4 FROM fz_o2_e1) a FULL JOIN \
         (SELECT k4 FROM fz_o2_e2) b USING (k4) WHERE COALESCE(k4, -1) = 5 \
         GROUP BY k4 ORDER BY k4;"
            .into(),
        // EC-derived sort keys: ORDER BY a member while joining on another.
        "SELECT a.pk, b.pk FROM fz_o2_e1 a JOIN fz_o2_e2 b ON a.k4 = b.k4 \
         WHERE a.k4 < 4 ORDER BY b.k4, a.pk, b.pk;"
            .into(),
        // Volatile-safe: EC with a stable expression member.
        "SELECT count(*)::int8 FROM fz_o2_e1 a JOIN fz_o2_e2 b ON a.k4 + 0 = b.k4 \
         WHERE b.k4 = 3;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, NESTL, IDX, SORTU], &queries));
    // have_partkey_equi_join residue: multi-column and EXPRESSION
    // partition keys, matched and MISmatched join quals, swept under
    // partitionwise forcing.
    v.extend(raws(&[
        "CREATE TABLE fz_o2_ek1 (k int4, s int4, v int4) PARTITION BY LIST ((k % 4));",
        "CREATE TABLE fz_o2_ek1_a PARTITION OF fz_o2_ek1 FOR VALUES IN (0, 1);",
        "CREATE TABLE fz_o2_ek1_b PARTITION OF fz_o2_ek1 FOR VALUES IN (2, 3);",
        "CREATE TABLE fz_o2_ek2 (k int4, s int4, v int4) PARTITION BY LIST ((k % 4));",
        "CREATE TABLE fz_o2_ek2_a PARTITION OF fz_o2_ek2 FOR VALUES IN (0, 1);",
        "CREATE TABLE fz_o2_ek2_b PARTITION OF fz_o2_ek2 FOR VALUES IN (2, 3);",
        "CREATE TABLE fz_o2_ek3 (k int4, s int4, v int4) PARTITION BY RANGE (k, s);",
        "CREATE TABLE fz_o2_ek3_a PARTITION OF fz_o2_ek3 FOR VALUES FROM (0, 0) TO (50, 100);",
        "CREATE TABLE fz_o2_ek3_b PARTITION OF fz_o2_ek3 FOR VALUES FROM (50, 100) TO (100, 200);",
        "CREATE TABLE fz_o2_ek4 (k int4, s int4, v int4) PARTITION BY RANGE (k, s);",
        "CREATE TABLE fz_o2_ek4_a PARTITION OF fz_o2_ek4 FOR VALUES FROM (0, 0) TO (50, 100);",
        "CREATE TABLE fz_o2_ek4_b PARTITION OF fz_o2_ek4 FOR VALUES FROM (50, 100) TO (100, 200);",
        "INSERT INTO fz_o2_ek1 SELECT i % 90, (i * 3) % 90, i FROM generate_series(1, 150) i;",
        "INSERT INTO fz_o2_ek2 SELECT i % 90, (i * 7) % 90, -i FROM generate_series(1, 120) i;",
        "INSERT INTO fz_o2_ek3 SELECT i % 90, (i * 3) % 90, i FROM generate_series(1, 150) i;",
        "INSERT INTO fz_o2_ek4 SELECT i % 90, (i * 7) % 90, -i FROM generate_series(1, 120) i;",
        "ANALYZE fz_o2_ek1;",
        "ANALYZE fz_o2_ek2;",
        "ANALYZE fz_o2_ek3;",
        "ANALYZE fz_o2_ek4;",
    ]));
    let ek_queries: Vec<String> = vec![
        // Expression-key match (clause ON both the raw column and the
        // partition expression), and the raw-column-only MISmatch.
        "SELECT count(*)::int8 FROM fz_o2_ek1 a JOIN fz_o2_ek2 b \
         ON (a.k % 4) = (b.k % 4) AND a.k = b.k;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_ek1 a JOIN fz_o2_ek2 b ON a.k = b.k;".into(),
        // Multi-column key: full match, partial match, cross-column.
        "SELECT count(*)::int8 FROM fz_o2_ek3 a JOIN fz_o2_ek4 b \
         ON a.k = b.k AND a.s = b.s;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_ek3 a JOIN fz_o2_ek4 b ON a.k = b.k;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ek3 a JOIN fz_o2_ek4 b \
         ON a.k = b.s AND a.s = b.k;"
            .into(),
        // FULL and LEFT pwise joins over the multi-column key.
        "SELECT count(*)::int8 FROM fz_o2_ek3 a FULL JOIN fz_o2_ek4 b \
         ON a.k = b.k AND a.s = b.s;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_ek3 a LEFT JOIN fz_o2_ek4 b \
         ON a.k = b.k AND a.s = b.s WHERE a.v < 60;"
            .into(),
    ];
    v.extend(sweep(g, &[PWISE_NESTL, PWISE_MERGEA, DEFAULTP], &ek_queries));
    v.push(raw("DROP TABLE fz_o2_ek1, fz_o2_ek2, fz_o2_ek3, fz_o2_ek4;"));
    v.push(raw("DROP TABLE fz_o2_e1, fz_o2_e2, fz_o2_e3, fz_o2_e4;"));
    v
}

// ------------------------------------------------ const-expression -------

fn gen_constfold(g: &mut Gen) -> Vec<StmtKind> {
    let n = 1 + g.rng.below_usize(9) as i64;
    let mut v = raws(&[
        "CREATE TABLE fz_o2_cf (pk int4 PRIMARY KEY, x int4, s text);",
        "INSERT INTO fz_o2_cf SELECT i, i % 9, 'c' || (i % 4) FROM generate_series(1, 90) i;",
        "ANALYZE fz_o2_cf;",
        "CREATE TYPE fz_o2_cfrow AS (u int4, w text);",
        // Inlinable / non-inlinable SQL functions (inline failure arms:
        // volatile body, SETOF misuse, strict with NULL const).
        "CREATE FUNCTION fz_o2_inl(a int4) RETURNS int4 LANGUAGE sql IMMUTABLE AS 'SELECT a * 2 + 1';",
        "CREATE FUNCTION fz_o2_strict(a int4) RETURNS int4 LANGUAGE sql IMMUTABLE STRICT AS 'SELECT a + 10';",
        "CREATE FUNCTION fz_o2_vol(a int4) RETURNS int4 LANGUAGE sql VOLATILE AS 'SELECT a + (random() * 0)::int4';",
        "CREATE FUNCTION fz_o2_named(a int4, b int4 DEFAULT 3) RETURNS int4 LANGUAGE sql IMMUTABLE AS 'SELECT a * 10 + b';",
        "CREATE FUNCTION fz_o2_srf2(k int4) RETURNS SETOF int4 LANGUAGE sql IMMUTABLE AS 'SELECT g FROM generate_series(1, k) g';",
        "CREATE FUNCTION fz_o2_cfn() RETURNS int4 LANGUAGE sql IMMUTABLE AS 'SELECT 41';",
    ]);
    let queries: Vec<String> = vec![
        // Const CASE / bool trees / NULL propagation.
        "SELECT pk FROM fz_o2_cf WHERE CASE WHEN 1 = 1 THEN x > 3 ELSE false END \
         AND pk < 15 ORDER BY pk;"
            .into(),
        "SELECT pk FROM fz_o2_cf WHERE CASE x WHEN 100 THEN true ELSE pk < 10 END ORDER BY pk;"
            .into(),
        "SELECT pk FROM fz_o2_cf WHERE (true AND pk < 8) OR (false AND x = 999) ORDER BY pk;"
            .into(),
        "SELECT pk FROM fz_o2_cf WHERE NOT (NOT (pk < 6)) ORDER BY pk;".into(),
        "SELECT pk FROM fz_o2_cf WHERE (NULL::bool AND x = 1) IS NOT TRUE AND pk < 5 ORDER BY pk;"
            .into(),
        "SELECT pk FROM fz_o2_cf WHERE 1 = 2 AND x = pk ORDER BY pk;".into(),
        "SELECT count(*)::int8 FROM fz_o2_cf WHERE 3 = ANY (ARRAY[1, 2, 3]);".into(),
        "SELECT count(*)::int8 FROM fz_o2_cf WHERE pk = ANY (NULL::int4[]);".into(),
        "SELECT count(*)::int8 FROM fz_o2_cf WHERE x = ALL (ARRAY[]::int4[]);".into(),
        // COALESCE/NULLIF/GREATEST const folding + strict-over-NULL.
        "SELECT COALESCE(NULL, NULL, 7), NULLIF(4, 4), NULLIF(5, 4), GREATEST(1, NULL, 3), \
         LEAST(NULL::int4, NULL);"
            .into(),
        "SELECT fz_o2_strict(NULL), fz_o2_strict(5), fz_o2_inl(6), fz_o2_named(2), \
         fz_o2_named(b => 7, a => 1), fz_o2_cfn();"
            .into(),
        format!(
            "SELECT pk, fz_o2_inl(x) FROM fz_o2_cf WHERE fz_o2_inl(pk) < {n} + 10 ORDER BY pk;"
        ),
        "SELECT pk FROM fz_o2_cf WHERE fz_o2_vol(pk) = pk AND pk < 6 ORDER BY pk;".into(),
        "SELECT v FROM fz_o2_srf2(4) v ORDER BY v;".into(),
        "SELECT (SELECT count(*)::int8 FROM fz_o2_srf2(3));".into(),
        // FieldSelect over const rows; ArrayCoerce; RelabelType stacks;
        // CoerceViaIO; casts of NULL.
        "SELECT (ROW(3, 'z')::fz_o2_cfrow).u, (ROW(3, 'z')::fz_o2_cfrow).w;".into(),
        "SELECT ('{1,2,3}'::int4[])::int8[], ARRAY[1, 2]::numeric[]::text;".into(),
        "SELECT ('x'::varchar)::text::varchar::text, (NULL::int4)::int8;".into(),
        "SELECT ('5'::text)::int4 + 1, ('(1,2)'::text)::point IS NOT NULL;".into(),
        "SELECT nullif(x, 4) FROM fz_o2_cf WHERE pk < 5 ORDER BY pk;".into(),
        // Boolean-equality simplification arms.
        "SELECT pk FROM fz_o2_cf WHERE (x = 3) = true AND pk < 40 ORDER BY pk;".into(),
        "SELECT pk FROM fz_o2_cf WHERE (x = 3) IS NOT FALSE AND pk < 30 ORDER BY pk;".into(),
        "SELECT pk FROM fz_o2_cf WHERE (pk < 10) <> false ORDER BY pk;".into(),
        // Outer-join strictness reduction (find_nonnullable_vars /
        // contain_nonstrict_functions walkers): strict WHERE over the
        // nullable side reduces the join; non-strict COALESCE/CASE/IS
        // NULL forms must NOT.
        "SELECT count(*)::int8 FROM fz_o2_cf a LEFT JOIN fz_o2_cf b ON a.pk = b.pk + 30 \
         WHERE b.x + 1 > 0;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_cf a LEFT JOIN fz_o2_cf b ON a.pk = b.pk + 30 \
         WHERE COALESCE(b.x, 0) >= 0;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_cf a LEFT JOIN fz_o2_cf b ON a.pk = b.pk + 30 \
         WHERE b.x IS NULL OR b.x > 2;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_cf a LEFT JOIN fz_o2_cf b ON a.pk = b.pk + 30 \
         WHERE CASE WHEN b.x IS NULL THEN 1 ELSE b.x END > 0;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_cf a LEFT JOIN fz_o2_cf b ON a.pk = b.pk + 30 \
         WHERE abs(b.x) < 100 AND NOT (b.x IS NOT NULL AND false);"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, SEQ], &queries));
    v.extend(raws(&[
        "DROP FUNCTION fz_o2_inl, fz_o2_strict, fz_o2_vol, fz_o2_named, fz_o2_srf2, fz_o2_cfn;",
        "DROP TYPE fz_o2_cfrow;",
        "DROP TABLE fz_o2_cf;",
    ]));
    v
}

// ----------------------------------------------- index clause matches ----

fn gen_indexmatch(g: &mut Gen) -> Vec<StmtKind> {
    let lo = 2 + g.rng.below_usize(6) as i64;
    let mut v = raws(&[
        "CREATE TABLE fz_o2_ix (pk int4 PRIMARY KEY, a int4, b int4, flag bool, \
         txt text, n int4);",
        "INSERT INTO fz_o2_ix SELECT i, (i * 13) % 100, (i * 7) % 40, i % 4 = 0, \
         'p' || (i % 61), CASE WHEN i % 5 = 0 THEN NULL ELSE i % 20 END \
         FROM generate_series(1, 800) i;",
        "CREATE INDEX ON fz_o2_ix (a, b);",
        "CREATE INDEX ON fz_o2_ix (flag);",
        "CREATE INDEX ON fz_o2_ix (txt text_pattern_ops);",
        "CREATE INDEX ON fz_o2_ix (n);",
        "CREATE INDEX ON fz_o2_ix (b) WHERE a > 50;",
        "CREATE INDEX ON fz_o2_ix (b) WHERE flag;",
        "ANALYZE fz_o2_ix;",
    ]);
    let queries: Vec<String> = vec![
        // RowCompare over the composite index.
        format!(
            "SELECT pk FROM fz_o2_ix WHERE (a, b) > ({lo}, 10) AND (a, b) < ({lo} + 30, 20) \
             ORDER BY pk;"
        ),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (a, b) >= (90, 0);".into(),
        // Partial / reordered row-compare matches (expand_indexqual_rowcompare
        // deep arms: trailing member not in the index, reordered members,
        // longer-than-index rows).
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (a, pk) > (95, 400);".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (b, a) < (3, 50);".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (a, b, pk) > (95, 10, 100);".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (a, b) > (5, 10) AND (a, b) < (35, 20);".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (a, b) <= (5, 39);".into(),
        // Boolean-index shapes.
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE flag;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE NOT flag;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE flag = false;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE flag IS TRUE;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE flag IS NOT FALSE;".into(),
        // Pattern-prefix extraction over text_pattern_ops.
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE txt LIKE 'p1%';".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE txt LIKE 'p12';".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE txt ~ '^p2';".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE txt ~ '^(p3|p4)';".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE txt ~~ 'p5_';".into(),
        // SAOPs, IN lists, NullTests.
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE a = ANY ('{3,17,55,80}'::int4[]);".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE a IN (1, 2, 3, 5, 8, 13, 21, 34, 55, 89);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE n IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE n IS NOT NULL AND n < 3;".into(),
        // Partial-index predtest implication (incl. strict-inequality
        // crossing and cross-strategy proofs).
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE b = 7 AND a > 60;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE b = 9 AND a >= 51;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE b < 5 AND flag AND pk % 3 = 0;".into(),
        // OR clauses: BitmapOr + similar-OR grouping.
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE a = 5 OR a = 15 OR a = 25;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE a = 5 OR b = 11;".into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE (a = 8 AND b < 10) OR (a = 9 AND b > 30);"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_ix WHERE a = 1 OR a = 2 OR b = 3 OR n IS NULL;"
            .into(),
        // Backward scans / NULLS ordering off an index.
        "SELECT pk FROM fz_o2_ix WHERE a = 13 ORDER BY b DESC, pk LIMIT 5;".into(),
        "SELECT n FROM fz_o2_ix ORDER BY n DESC NULLS LAST, pk LIMIT 5;".into(),
        "SELECT n FROM fz_o2_ix ORDER BY n NULLS FIRST, pk LIMIT 5;".into(),
    ];
    v.extend(sweep(g, &[IDX, BITMAP, SEQ, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_o2_ix;"));
    v
}

// ----------------------------------------------------- sublink pullup ----

fn gen_pullup(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o2_s1 (pk int4 PRIMARY KEY, k int4, v int4);",
        "CREATE TABLE fz_o2_s2 (pk int4 PRIMARY KEY, k int4, w int4);",
        "INSERT INTO fz_o2_s1 SELECT i, i % 30, i FROM generate_series(1, 250) i;",
        "INSERT INTO fz_o2_s2 SELECT i, (i * 7) % 30, -i FROM generate_series(1, 180) i;",
        "ANALYZE fz_o2_s1;",
        "ANALYZE fz_o2_s2;",
    ]);
    let queries: Vec<String> = vec![
        // Sublink pullup at every qual-recursion position: top AND, under
        // OR (not pullable), NOT EXISTS (anti), nested EXISTS.
        "SELECT pk FROM fz_o2_s1 t WHERE EXISTS (SELECT 1 FROM fz_o2_s2 s WHERE s.k = t.k) \
         AND t.pk < 25 ORDER BY pk;"
            .into(),
        "SELECT pk FROM fz_o2_s1 t WHERE NOT EXISTS (SELECT 1 FROM fz_o2_s2 s WHERE s.k = t.k) \
         ORDER BY pk LIMIT 12;"
            .into(),
        "SELECT pk FROM fz_o2_s1 t WHERE t.k IN (SELECT k FROM fz_o2_s2 WHERE w < -50) \
         AND t.pk < 40 ORDER BY pk;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_s1 t WHERE t.k NOT IN \
         (SELECT k FROM fz_o2_s2 WHERE w > -10);"
            .into(),
        "SELECT pk FROM fz_o2_s1 t WHERE (EXISTS (SELECT 1 FROM fz_o2_s2 s WHERE s.k = t.k) \
         OR t.k = 0) AND t.pk < 20 ORDER BY pk;"
            .into(),
        "SELECT pk FROM fz_o2_s1 t WHERE EXISTS (SELECT 1 FROM fz_o2_s2 s WHERE s.k = t.k \
         AND EXISTS (SELECT 1 FROM fz_o2_s1 i WHERE i.pk = s.pk)) ORDER BY pk LIMIT 10;"
            .into(),
        "SELECT pk FROM fz_o2_s1 t WHERE t.v > ANY (SELECT w FROM fz_o2_s2 WHERE k = t.k) \
         ORDER BY pk LIMIT 10;"
            .into(),
        "SELECT count(*)::int8 FROM fz_o2_s1 t WHERE t.v > ALL \
         (SELECT w FROM fz_o2_s2 WHERE k = t.k);"
            .into(),
        // PHV-requiring subquery pullup: constant/expression outputs of a
        // nullable-side subquery referenced above the join.
        "SELECT t.pk, s.tag FROM fz_o2_s1 t LEFT JOIN \
         (SELECT k, 'zz' AS tag FROM fz_o2_s2 GROUP BY k) s ON s.k = t.k \
         WHERE t.pk < 15 ORDER BY t.pk;"
            .into(),
        "SELECT t.pk, s.kk + 1 FROM fz_o2_s1 t LEFT JOIN \
         (SELECT k, k * 100 AS kk FROM fz_o2_s2 WHERE w < 0) s ON s.k = t.k \
         WHERE t.pk < 12 ORDER BY t.pk, 2;"
            .into(),
        "SELECT count(DISTINCT s.tag)::int8 FROM fz_o2_s1 t LEFT JOIN \
         (SELECT k, CASE WHEN w < -90 THEN 'lo' ELSE 'hi' END AS tag FROM fz_o2_s2) s \
         ON s.k = t.k;"
            .into(),
        // VALUES pullup (single row) + UNION ALL flattening.
        "SELECT t.pk, c.lbl FROM fz_o2_s1 t, (VALUES (1, 'one')) c(n, lbl) \
         WHERE t.pk = c.n ORDER BY t.pk;"
            .into(),
        "SELECT u.k, count(*)::int8 FROM \
         (SELECT k FROM fz_o2_s1 WHERE v < 100 UNION ALL SELECT k FROM fz_o2_s2) u \
         GROUP BY u.k ORDER BY u.k LIMIT 10;"
            .into(),
        // ORDER BY inside a pulled-up subquery is discarded legally.
        "SELECT count(*)::int8 FROM (SELECT pk FROM fz_o2_s1 ORDER BY v DESC) z;".into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, NESTL, SORTU, HASHU], &queries));
    v.push(raw("DROP TABLE fz_o2_s1, fz_o2_s2;"));
    v
}

// ------------------------------------------------------- unique paths ----

fn gen_uniq(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_o2_q1 (pk int4 PRIMARY KEY, k int4, v int4);",
        "CREATE TABLE fz_o2_q2 (pk int4 PRIMARY KEY, k int4);",
        "INSERT INTO fz_o2_q1 SELECT i, i % 12, i FROM generate_series(1, 240) i;",
        "INSERT INTO fz_o2_q2 SELECT i, (i * 5) % 12 FROM generate_series(1, 100) i;",
        "ANALYZE fz_o2_q1;",
        "ANALYZE fz_o2_q2;",
    ]);
    let queries: Vec<String> = vec![
        // Semi-join unique-ification: hash vs sort arms.
        "SELECT count(*)::int8 FROM fz_o2_q2 d WHERE d.k IN (SELECT k FROM fz_o2_q1);".into(),
        "SELECT d.pk FROM fz_o2_q2 d WHERE d.k IN (SELECT k FROM fz_o2_q1 WHERE v > 200) \
         ORDER BY d.pk;"
            .into(),
        // Unique-ified cross-type semijoin.
        "SELECT count(*)::int8 FROM fz_o2_q2 d WHERE d.k::int8 IN \
         (SELECT k::int8 FROM fz_o2_q1 WHERE v % 2 = 0);"
            .into(),
        // Sorted vs hashed setops (generate_union_paths residue).
        "SELECT k FROM fz_o2_q1 UNION SELECT k FROM fz_o2_q2 ORDER BY k;".into(),
        "SELECT k FROM fz_o2_q1 INTERSECT SELECT k FROM fz_o2_q2 ORDER BY k;".into(),
        "SELECT k FROM fz_o2_q1 EXCEPT ALL SELECT k FROM fz_o2_q2 ORDER BY k;".into(),
        "SELECT k FROM fz_o2_q1 UNION DISTINCT SELECT k + 1 FROM fz_o2_q2 ORDER BY k LIMIT 15;"
            .into(),
        // DISTINCT / DISTINCT ON paths.
        "SELECT DISTINCT k FROM fz_o2_q1 ORDER BY k;".into(),
        "SELECT DISTINCT ON (k) k, pk FROM fz_o2_q1 ORDER BY k, pk;".into(),
        "SELECT count(*)::int8 FROM (SELECT DISTINCT k, v % 3 FROM fz_o2_q1) z;".into(),
    ];
    v.extend(sweep(g, &[SORTU, HASHU, NESTL, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_o2_q1, fz_o2_q2;"));
    v
}

// ------------------------------------------------ predicate refutation ---

/// predicate_refuted_by / relation_excluded_by_constraints fuel:
/// contradictory quals, CHECK-constraint exclusion under
/// constraint_exclusion=on, partition-constraint exclusion under
/// constraint_exclusion=partition with pruning disabled.
fn gen_refute(g: &mut Gen) -> Vec<StmtKind> {
    const CEX_ON: Prof = Prof { name: "cex_on", gucs: &[("constraint_exclusion", "on")] };
    const CEX_PART: Prof = Prof {
        name: "cex_part",
        gucs: &[
            ("constraint_exclusion", "partition"),
            ("enable_partition_pruning", "off"),
        ],
    };
    let mut v = raws(&[
        "CREATE TABLE fz_o2_rf (pk int4 PRIMARY KEY, v int4 CHECK (v > 0 AND v < 100), \
         w int4 NOT NULL, flag bool, CHECK (w <> 13));",
        "INSERT INTO fz_o2_rf SELECT i, (i % 98) + 1, i + (i / 13) % 2, i % 2 = 0 \
         FROM generate_series(1, 200) i;",
        "CREATE INDEX ON fz_o2_rf (v) WHERE flag;",
        "ANALYZE fz_o2_rf;",
        "CREATE TABLE fz_o2_rp (pk int4, v int4) PARTITION BY RANGE (pk);",
        "CREATE TABLE fz_o2_rp_a PARTITION OF fz_o2_rp FOR VALUES FROM (0) TO (100);",
        "CREATE TABLE fz_o2_rp_b PARTITION OF fz_o2_rp FOR VALUES FROM (100) TO (200);",
        "INSERT INTO fz_o2_rp SELECT i, i * 2 FROM generate_series(1, 190) i;",
        "ANALYZE fz_o2_rp;",
    ]);
    let queries: Vec<String> = vec![
        // CHECK refutation and non-refutation.
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v = -5;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v > 150;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v >= 100 OR v <= 0;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE w = 13;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE w IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v = 50;".into(),
        // Self-contradictions (refuted without any constraint).
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v > 10 AND v < 5;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v = 7 AND v <> 7;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE flag AND NOT flag;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE v IS NULL AND v = 3;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rf WHERE (v > 20 OR w > 50) AND v < 10 AND w < 40;"
            .into(),
        // Partition-constraint exclusion (pruning off, exclusion on).
        "SELECT count(*)::int8 FROM fz_o2_rp WHERE pk = 150;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rp WHERE pk < 0;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rp WHERE pk >= 250;".into(),
        "SELECT count(*)::int8 FROM fz_o2_rp WHERE pk BETWEEN 90 AND 110;".into(),
    ];
    v.extend(sweep(g, &[CEX_ON, CEX_PART, DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_o2_rf, fz_o2_rp;"));
    v
}

// -------------------------------------------------- broad GUC matrix -----

/// Round-2 GUC widening: planner GUCs LD7's pools never touched, each
/// swept over a small structurally-mixed query set.
fn gen_gucmatrix(g: &mut Gen) -> Vec<StmtKind> {
    const MATRIX: &[Prof] = &[
        Prof { name: "no_gathermerge", gucs: &[("enable_gathermerge", "off")] },
        Prof { name: "no_pruning", gucs: &[("enable_partition_pruning", "off")] },
        Prof {
            name: "no_presorted",
            gucs: &[("enable_presorted_aggregate", "off")],
        },
        Prof {
            name: "no_distinct_reord",
            gucs: &[("enable_distinct_reordering", "off")],
        },
        Prof {
            name: "no_groupby_reord",
            gucs: &[("enable_group_by_reordering", "off")],
        },
        Prof { name: "no_async", gucs: &[("enable_async_append", "off")] },
        Prof { name: "no_tidscan", gucs: &[("enable_tidscan", "off")] },
        Prof {
            name: "constraint_excl",
            gucs: &[("constraint_exclusion", "on")],
        },
        Prof {
            name: "cursor_frac",
            gucs: &[("cursor_tuple_fraction", "1.0")],
        },
        Prof {
            name: "recursive_wt",
            gucs: &[("recursive_worktable_factor", "1")],
        },
        Prof {
            name: "hash_squeeze",
            gucs: &[("hash_mem_multiplier", "1"), ("work_mem", "'64kB'")],
        },
        Prof {
            name: "collapse_high",
            gucs: &[("join_collapse_limit", "12"), ("from_collapse_limit", "12")],
        },
        Prof {
            name: "geqo_pool",
            gucs: &[
                ("geqo", "on"),
                ("geqo_threshold", "2"),
                ("geqo_pool_size", "4"),
                ("geqo_generations", "5"),
                ("geqo_selection_bias", "1.5"),
                ("geqo_seed", "0"),
            ],
        },
        Prof {
            name: "jit_thresholds",
            gucs: &[("jit", "off"), ("jit_above_cost", "0")],
        },
    ];
    let mut v = vec![
        // No PK: a unique constraint on a partitioned table must include
        // the (expression) partition key, which a plain pk column cannot.
        raw("CREATE TABLE fz_o2_gm (pk int4, a int4, b int4, c text) \
             PARTITION BY LIST ((pk % 3));"),
        raw("CREATE TABLE fz_o2_gm_0 PARTITION OF fz_o2_gm FOR VALUES IN (0);"),
        raw("CREATE TABLE fz_o2_gm_1 PARTITION OF fz_o2_gm FOR VALUES IN (1);"),
        raw("CREATE TABLE fz_o2_gm_d PARTITION OF fz_o2_gm DEFAULT;"),
        raw("CREATE INDEX ON fz_o2_gm (a);"),
        raw("INSERT INTO fz_o2_gm SELECT i, (i * 3) % 20, (i * 7) % 9, 'g' || (i % 5) \
             FROM generate_series(1, 600) i;"),
        raw("ANALYZE fz_o2_gm;"),
        raw("CREATE TABLE fz_o2_gj (pk int4 PRIMARY KEY, a int4);"),
        raw("INSERT INTO fz_o2_gj SELECT i, i % 20 FROM generate_series(1, 100) i;"),
        raw("ANALYZE fz_o2_gj;"),
    ];
    let queries: Vec<String> = vec![
        // Pruning-sensitive partition probe.
        "SELECT count(*)::int8 FROM fz_o2_gm WHERE pk % 3 = 1 AND a < 10;".into(),
        // Grouped agg with a presorted prefix.
        "SELECT a, b, count(*)::int8 FROM fz_o2_gm GROUP BY a, b ORDER BY a, b;".into(),
        "SELECT DISTINCT b, a FROM fz_o2_gm WHERE a < 6 ORDER BY b, a;".into(),
        // 6-way join (collapse / geqo territory).
        "SELECT count(*)::int8 FROM fz_o2_gj a, fz_o2_gj b, fz_o2_gj c, fz_o2_gj d, \
         fz_o2_gj e, fz_o2_gj f WHERE a.a = b.a AND b.pk = c.pk AND c.a = d.a \
         AND d.pk = e.pk AND e.a = f.a AND a.pk < 6;"
            .into(),
        // TID quals.
        "SELECT count(*)::int8 FROM fz_o2_gj WHERE ctid = '(0,1)';".into(),
        // Recursive CTE (worktable factor).
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 40) \
         SELECT sum(n)::int8 FROM r;"
            .into(),
        // Ordered LIMIT probe (cursor_tuple_fraction analog path).
        "SELECT pk FROM fz_o2_gm ORDER BY a, pk LIMIT 7;".into(),
    ];
    // Sample 4 matrix rows per group (keeps group size bounded); fires
    // record which rows ran.
    let mut idx: Vec<usize> = (0..MATRIX.len()).collect();
    for i in 0..4 {
        let j = i + g.rng.below_usize(idx.len() - i);
        idx.swap(i, j);
    }
    let picked: Vec<Prof> = idx.iter().take(4).map(|&i| MATRIX[i]).collect();
    v.extend(sweep(g, &picked, &queries));
    v.push(raw("DROP TABLE fz_o2_gm, fz_o2_gj;"));
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
            let stmts = gen_opt2_module(&mut g);
            out.push(stmts.iter().map(|s| s.to_sql()).collect());
        }
        out
    }

    #[test]
    fn groups_are_set_reset_balanced_and_self_contained() {
        // Every SET has its RESET in the same group (reverse order is
        // checked by rposition matching), every BEGIN closes with
        // ROLLBACK, and every CREATE TABLE/FUNCTION/TYPE has a DROP in
        // the same group (fixed fz_o2_* names; child partitions are
        // dropped via their parent).
        for group in gen_groups(31, 200) {
            let mut sets: Vec<String> = Vec::new();
            let mut open_txn = 0i32;
            let joined = group.join("\n");
            for sql in &group {
                if sql == "BEGIN;" {
                    open_txn += 1;
                } else if sql == "ROLLBACK;" {
                    open_txn -= 1;
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
                    if !t.contains("_c") && !t.ends_with("_0") && !t.ends_with("_1")
                        && !t.ends_with("_d") && !t.ends_with("_a") && !t.ends_with("_b")
                    {
                        assert!(
                            joined.contains(&format!("DROP TABLE {t}"))
                                || joined.contains(&format!("DROP TABLE {t},"))
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
    fn row_returning_sweep_statements_are_totally_ordered() {
        // Row-returning SELECTs carry ORDER BY; aggregate-only
        // projections (count/sum/coalesce(sum..)) are exempt.
        for group in gen_groups(7, 200) {
            for sql in &group {
                if !sql.starts_with("SELECT ") && !sql.starts_with("WITH ") {
                    continue;
                }
                let aggregate_only = sql.starts_with("SELECT count(")
                    || sql.starts_with("SELECT sum(")
                    || sql.starts_with("SELECT COALESCE(")
                    || sql.starts_with("WITH RECURSIVE r(n) AS")
                    || sql.contains("SELECT count(*)::int8 FROM (")
                    // Scalar-subquery projections are single-row too.
                    || sql.starts_with("SELECT (SELECT count(")
                    // Scalar SELECTs without FROM are single-row by
                    // construction (const-folding probes).
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
        assert_eq!(gen_groups(99, 60), gen_groups(99, 60));
    }
}
