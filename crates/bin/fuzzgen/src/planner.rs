//! Planner path / plan-shape drain module (fuzz-planner): the
//! `optimizer-arms` residue still cold after LD7/opt2/opt3/par in the
//! server gap map (docs/fuzzing/gap-report-006.tsv, modules=<all-on> —
//! so these are genuine residue no existing module reliably reaches).
//! Every family below names the REL_18_3@62d6c7d C functions whose unhit
//! whole-body regions it was written against; each is a *distinct plan
//! shape* the default corpus never forces:
//!
//!   - sample: SampleScan path/plan/exec — set_tablesample_rel_size,
//!     set_tablesample_rel_pathlist, cost_samplescan, create_samplescan_
//!     path, create_samplescan_plan, make_samplescan (path/costsize/
//!     createplan/allpaths). No corpus query uses TABLESAMPLE at all.
//!   - tidrange: TidRangeScan — cost_tidrangescan, create_tidrangescan_
//!     path, create_tidrangescan_plan, make_tidrangescan. Needs a ctid
//!     range qual AND the scan chosen over seqscan/index/bitmap.
//!   - groupsort: sorted Group node (GROUP BY with NO aggregate under
//!     enable_hashagg=off) — create_group_path, cost_group,
//!     create_group_plan, make_group. The corpus' grouped queries all
//!     carry aggregates (Agg node) or hash (HashAggregate), never the
//!     plain Group node.
//!   - setop: SetOp node incl. ALL variants and multi-way —
//!     create_setop_path, create_setop_plan, make_setop,
//!     subpath_is_hashable, generate_nonunion_paths, under sorted and
//!     hashed strategies.
//!   - winrun: WindowAgg run-condition qual pushdown —
//!     find_window_run_conditions (76 lines) + check_and_push_window_
//!     quals (allpaths.c). Reached only by an outer qual on a monotonic
//!     window function output (row_number/rank/dense_rank/count/ntile).
//!   - geqo: the whole genetic query optimizer — geqo_main.geqo,
//!     geqo_eval, gimme_tree/merge_clump/desirable_join, and the
//!     geqo_erx/geqo_pool/geqo_selection/geqo_recombination/geqo_copy/
//!     geqo_random machinery. Engaged only with geqo=on at a low
//!     geqo_threshold over a many-table join. geqo_seed=0 makes the
//!     join-order search a deterministic function of the query on each
//!     engine (see determinism note below).
//!   - bitmapor: BitmapOr / BitmapAnd path construction and the OR-clause
//!     index matching — make_bitmap_paths_for_or_group, create_bitmap_
//!     or_path, cost_bitmap_or_node, make_bitmap_or, make_bitmap_and,
//!     consider_new_or_clause (orclauses.c), or_arg_index_match_cmp[_
//!     group], is_pseudo_constant_for_index. Needs multi-column OR/AND
//!     over separately-indexed columns with the bitmap plan forced.
//!
//! Correctness bar (LD7/opt2 law): the RESULT SET of a deterministic
//! query is identical across every forced plan and across both engines;
//! any A/B divergence is a HIGH-severity planner/executor finding. Plans
//! may differ — no EXPLAIN is emitted here (coverage comes from planning
//! and executing the real queries), so a geqo/bitmap join-order or
//! strategy difference between the engines is never mistaken for a bug.
//!
//! Determinism discipline (plansel/opt2 rules):
//!   - Self-contained groups: fixed `fz_pl_*` fixtures CREATEd and
//!     DROPped inside the same statement group (both engines apply a
//!     group as a unit, so fixed names never collide across groups).
//!   - Every value is a pure integer formula of the generate_series
//!     index — identical on both sides by construction.
//!   - Every row-returning statement carries a TOTAL order (ORDER BY
//!     ending in the primary key); everything else is aggregate-only
//!     with exact-typed (int8) accumulation-order-independent aggregates.
//!     No float aggregates (B1).
//!   - Tables stay tiny (<= 400 rows), far under the ANALYZE sample, so
//!     stats and costs are identical on both sides.
//!   - Every bracket SET has its RESET in the same group, RESETs in
//!     reverse order; the diffrunner's GucPinned wrapper re-applies the
//!     C-parity pin after RESETs identically on both sides.
//!   - TABLESAMPLE is used ONLY at 100 percent with REPEATABLE, so the
//!     sampled set is the whole table on both engines regardless of the
//!     sampler internals (opt2 rule) — the SampleScan planner/exec arms
//!     still fire, but the result is deterministic.
//!   - ctid range quals are FULL-COVERING (>= '(0,0)', < a max block),
//!     so a TidRangeScan returns the whole heap regardless of physical
//!     layout — heap order is a non-surface (COPY order ruling), so the
//!     range is never a proper sub-slice of it.
//!   - GEQO uses aggregate-only probes: the join result is order-
//!     independent, so a different geqo-chosen join order on each engine
//!     still yields identical counts/sums.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// A named GUC profile (same shape as opt2/plansel; local copy keeps the
/// modules independent).
#[derive(Clone, Copy)]
struct Prof {
    name: &'static str,
    gucs: &'static [(&'static str, &'static str)],
}

const DEFAULTP: Prof = Prof { name: "default", gucs: &[] };
/// Force the plain sorted Group node / sorted SetOp (no hashing).
const NOHASHAGG: Prof = Prof { name: "nohashagg", gucs: &[("enable_hashagg", "off")] };
/// Force the hashed SetOp strategy (subpath_is_hashable arm).
const NOSORT: Prof = Prof { name: "nosort", gucs: &[("enable_sort", "off")] };
/// Force a TidRangeScan: every other base-rel scan type disabled, tidscan
/// stays on (default).
const TIDRANGE: Prof = Prof {
    name: "tidrange",
    gucs: &[
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
        ("enable_bitmapscan", "off"),
    ],
};
/// Force bitmap plans: seqscan and plain index scans disabled, bitmap on.
const BITMAP: Prof = Prof {
    name: "bitmap",
    gucs: &[
        ("enable_seqscan", "off"),
        ("enable_indexscan", "off"),
        ("enable_indexonlyscan", "off"),
    ],
};
/// Engage the genetic query optimizer over the whole join problem.
const GEQO: Prof = Prof {
    name: "geqo",
    gucs: &[
        ("geqo", "on"),
        ("geqo_threshold", "2"),
        ("geqo_seed", "0"),
        ("geqo_effort", "5"),
        ("join_collapse_limit", "20"),
        ("from_collapse_limit", "20"),
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

/// Emit every query under every profile (query lists are short and the
/// arms are profile-specific, so the sweep is exhaustive).
fn sweep(g: &mut Gen, profs: &[Prof], queries: &[String]) -> Vec<StmtKind> {
    let mut v = Vec::new();
    for p in profs {
        g.fire2("planner:prof:", p.name);
        v.extend(bracket(p, queries.iter().map(|q| raw(q.clone())).collect()));
    }
    v
}

const SHAPES: &[&str] = &[
    "planner:sample",
    "planner:tidrange",
    "planner:groupsort",
    "planner:setop",
    "planner:winrun",
    "planner:geqo",
    "planner:bitmapor",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_planner_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("planner");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire2("planner:shape:", &shape["planner:".len()..]);
    match shape {
        "planner:sample" => gen_sample(g),
        "planner:tidrange" => gen_tidrange(g),
        "planner:groupsort" => gen_groupsort(g),
        "planner:setop" => gen_setop(g),
        "planner:winrun" => gen_winrun(g),
        "planner:geqo" => gen_geqo(g),
        _ => gen_bitmapor(g),
    }
}

// ---------------------------------------------------------------- sample --

/// TABLESAMPLE SYSTEM/BERNOULLI at 100% REPEATABLE: the SampleScan path
/// (set_tablesample_rel_size/pathlist, cost_samplescan, create_samplescan_
/// path), plan (create_samplescan_plan) and executor node (make_samplescan)
/// all fire; the 100% sample makes the result the whole table on both
/// engines regardless of the tablesample method internals.
fn gen_sample(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pl_s (pk int4 PRIMARY KEY, a int4, t text);",
        "INSERT INTO fz_pl_s SELECT i, (i * 7) % 40, 'v' || (i % 13) \
         FROM generate_series(1, 300) i;",
        "ANALYZE fz_pl_s;",
    ]);
    let queries: Vec<String> = vec![
        // SYSTEM sampler, aggregate probe (order-independent).
        "SELECT count(*)::int8, sum(a::int8) FROM fz_pl_s TABLESAMPLE SYSTEM (100) \
         REPEATABLE (1);"
            .into(),
        // BERNOULLI sampler, total-ordered row probe.
        "SELECT pk, a FROM fz_pl_s TABLESAMPLE BERNOULLI (100) REPEATABLE (7) ORDER BY pk;"
            .into(),
        // SampleScan under a qual (cost_samplescan with restriction quals).
        "SELECT pk, a FROM fz_pl_s TABLESAMPLE SYSTEM (100) REPEATABLE (3) \
         WHERE a < 20 ORDER BY pk;"
            .into(),
        // SampleScan as the inner side of a join (reparameterization-free,
        // but the sampled rel path still feeds join costing).
        "SELECT count(*)::int8 FROM fz_pl_s s \
         JOIN fz_pl_s TABLESAMPLE BERNOULLI (100) REPEATABLE (5) x ON s.pk = x.pk;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_pl_s;"));
    v
}

// -------------------------------------------------------------- tidrange --

/// TidRangeScan: full-covering ctid range quals with every alternative
/// scan disabled. cost_tidrangescan / create_tidrangescan_path /
/// create_tidrangescan_plan / make_tidrangescan. The range covers the
/// whole heap, so the result is the full table (layout-independent).
fn gen_tidrange(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pl_t (pk int4 PRIMARY KEY, a int4);",
        "INSERT INTO fz_pl_t SELECT i, (i * 3) % 50 FROM generate_series(1, 300) i;",
        "ANALYZE fz_pl_t;",
    ]);
    let queries: Vec<String> = vec![
        // Lower-bounded range covering the whole heap.
        "SELECT count(*)::int8, sum(a::int8) FROM fz_pl_t WHERE ctid >= '(0,0)';".into(),
        // Upper-bounded range beyond every block, total-ordered rows.
        "SELECT pk, a FROM fz_pl_t WHERE ctid < '(4294967294,0)' ORDER BY pk;".into(),
        // Two-sided full-covering range.
        "SELECT count(*)::int8 FROM fz_pl_t \
         WHERE ctid >= '(0,0)' AND ctid <= '(4294967294,0)';"
            .into(),
        // TidRangeScan combined with a non-ctid filter (the qual rides on
        // the range scan as a filter).
        "SELECT pk FROM fz_pl_t WHERE ctid >= '(0,0)' AND a < 25 ORDER BY pk;".into(),
    ];
    v.extend(sweep(g, &[TIDRANGE], &queries));
    v.push(raw("DROP TABLE fz_pl_t;"));
    v
}

// ------------------------------------------------------------- groupsort --

/// Plain sorted Group node: GROUP BY with NO aggregate under
/// enable_hashagg=off yields a Sort + Group (not Agg / HashAggregate).
/// create_group_path / cost_group / create_group_plan / make_group.
fn gen_groupsort(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pl_g (pk int4 PRIMARY KEY, k int4, k2 int4);",
        "INSERT INTO fz_pl_g SELECT i, i % 12, (i * 5) % 7 \
         FROM generate_series(1, 300) i;",
        "ANALYZE fz_pl_g;",
    ]);
    let queries: Vec<String> = vec![
        // Single-key grouping, no aggregate -> Group node.
        "SELECT k FROM fz_pl_g GROUP BY k ORDER BY k;".into(),
        // Multi-key grouping, no aggregate.
        "SELECT k, k2 FROM fz_pl_g GROUP BY k, k2 ORDER BY k, k2;".into(),
        // Group node feeding an outer count (still a bare Group inside).
        "SELECT count(*)::int8 FROM (SELECT k FROM fz_pl_g GROUP BY k) s;".into(),
        // HAVING with no aggregate in the target list (Group + filter).
        "SELECT k FROM fz_pl_g GROUP BY k HAVING k > 3 ORDER BY k;".into(),
    ];
    v.extend(sweep(g, &[NOHASHAGG], &queries));
    v.push(raw("DROP TABLE fz_pl_g;"));
    v
}

// ---------------------------------------------------------------- setop ---

/// SetOp node: INTERSECT/EXCEPT (+ ALL, + multi-way) under sorted and
/// hashed strategies. create_setop_path / create_setop_plan / make_setop /
/// subpath_is_hashable / generate_nonunion_paths.
fn gen_setop(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pl_u1 (pk int4 PRIMARY KEY, k int4);",
        "CREATE TABLE fz_pl_u2 (pk int4 PRIMARY KEY, k int4);",
        "INSERT INTO fz_pl_u1 SELECT i, i % 20 FROM generate_series(1, 200) i;",
        "INSERT INTO fz_pl_u2 SELECT i, (i * 3) % 25 FROM generate_series(1, 150) i;",
        "ANALYZE fz_pl_u1;",
        "ANALYZE fz_pl_u2;",
    ]);
    let queries: Vec<String> = vec![
        "SELECT k FROM fz_pl_u1 INTERSECT SELECT k FROM fz_pl_u2 ORDER BY k;".into(),
        "SELECT k FROM fz_pl_u1 EXCEPT SELECT k FROM fz_pl_u2 ORDER BY k;".into(),
        "SELECT k FROM fz_pl_u1 INTERSECT ALL SELECT k FROM fz_pl_u2 ORDER BY k;".into(),
        "SELECT k FROM fz_pl_u1 EXCEPT ALL SELECT k FROM fz_pl_u2 ORDER BY k;".into(),
        // Multi-way setop (nested SetOp nodes).
        "SELECT k FROM (SELECT k FROM fz_pl_u1 INTERSECT SELECT k FROM fz_pl_u2) s \
         EXCEPT SELECT k + 1 FROM fz_pl_u1 ORDER BY k;"
            .into(),
        // Two-column setop (subpath_is_hashable over a composite target).
        "SELECT k, k % 2 FROM fz_pl_u1 INTERSECT SELECT k, k % 2 FROM fz_pl_u2 \
         ORDER BY k;"
            .into(),
    ];
    // Sorted (nohashagg) and hashed (nosort) strategies plus default.
    v.extend(sweep(g, &[DEFAULTP, NOHASHAGG, NOSORT], &queries));
    v.push(raw("DROP TABLE fz_pl_u1, fz_pl_u2;"));
    v
}

// --------------------------------------------------------------- winrun ---

/// WindowAgg run-condition pushdown: an outer qual on a monotonic window
/// function output. find_window_run_conditions + check_and_push_window_
/// quals. Every window ordering ends in pk so ranks are total (no ties),
/// making the qualified subset deterministic.
fn gen_winrun(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pl_w (pk int4 PRIMARY KEY, g int4, val int4);",
        "INSERT INTO fz_pl_w SELECT i, i % 5, (i * 11) % 100 \
         FROM generate_series(1, 300) i;",
        "ANALYZE fz_pl_w;",
    ]);
    let queries: Vec<String> = vec![
        // row_number monotonic-increasing run condition.
        "SELECT pk, g FROM (SELECT pk, g, row_number() OVER (ORDER BY pk) AS rn \
         FROM fz_pl_w) s WHERE rn <= 5 ORDER BY pk;"
            .into(),
        // rank over a total order (val, pk) -> no ties.
        "SELECT pk, rk FROM (SELECT pk, rank() OVER (ORDER BY val, pk) AS rk \
         FROM fz_pl_w) s WHERE rk < 10 ORDER BY rk, pk;"
            .into(),
        // dense_rank run condition.
        "SELECT pk, g FROM (SELECT pk, g, dense_rank() OVER (ORDER BY g, pk) AS dr \
         FROM fz_pl_w) s WHERE dr <= 30 ORDER BY pk;"
            .into(),
        // count(*) as a monotonic window aggregate run condition.
        "SELECT pk FROM (SELECT pk, count(*) OVER (ORDER BY pk) AS cnt \
         FROM fz_pl_w) s WHERE cnt <= 4 ORDER BY pk;"
            .into(),
        // ntile run condition.
        "SELECT pk, nt FROM (SELECT pk, ntile(4) OVER (ORDER BY pk) AS nt \
         FROM fz_pl_w) s WHERE nt <= 2 ORDER BY pk, nt;"
            .into(),
        // PARTITION BY variant (per-partition run condition).
        "SELECT pk, g FROM (SELECT pk, g, row_number() OVER \
         (PARTITION BY g ORDER BY pk) AS rn FROM fz_pl_w) s WHERE rn <= 3 \
         ORDER BY pk;"
            .into(),
    ];
    v.extend(sweep(g, &[DEFAULTP], &queries));
    v.push(raw("DROP TABLE fz_pl_w;"));
    v
}

// ----------------------------------------------------------------- geqo ---

/// Genetic query optimizer over a many-table join. geqo=on at
/// geqo_threshold=2 with a high collapse limit hands the whole join
/// problem to geqo_main.geqo; aggregate-only probes keep the result
/// order-independent so a different geqo-chosen join order per engine
/// still yields identical counts/sums.
fn gen_geqo(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = Vec::new();
    // Six chain-joinable tables; k in 0..30 keeps join fan-out bounded.
    for (n, rows) in [(1, 60), (2, 60), (3, 60), (4, 60), (5, 60), (6, 60)] {
        v.push(raw(format!(
            "CREATE TABLE fz_pl_j{n} (pk int4 PRIMARY KEY, k int4);"
        )));
        v.push(raw(format!(
            "INSERT INTO fz_pl_j{n} SELECT i, i % 30 FROM generate_series(1, {rows}) i;"
        )));
        v.push(raw(format!("ANALYZE fz_pl_j{n};")));
    }
    let queries: Vec<String> = vec![
        // 4-way join (geqo pool/generation sizing for rel count 4).
        "SELECT count(*)::int8, sum(a.pk::int8) FROM fz_pl_j1 a \
         JOIN fz_pl_j2 b ON a.k = b.k JOIN fz_pl_j3 c ON b.k = c.k \
         JOIN fz_pl_j4 d ON c.k = d.k;"
            .into(),
        // 5-way join with filters (more edges for the ERX crossover).
        "SELECT count(*)::int8 FROM fz_pl_j1 a \
         JOIN fz_pl_j2 b ON a.k = b.k JOIN fz_pl_j3 c ON b.k = c.k \
         JOIN fz_pl_j4 d ON c.k = d.k JOIN fz_pl_j5 e ON d.k = e.k \
         WHERE a.k < 20;"
            .into(),
        // 6-way join (largest chromosome; more generations).
        "SELECT count(*)::int8, sum(f.pk::int8) FROM fz_pl_j1 a \
         JOIN fz_pl_j2 b ON a.k = b.k JOIN fz_pl_j3 c ON b.k = c.k \
         JOIN fz_pl_j4 d ON c.k = d.k JOIN fz_pl_j5 e ON d.k = e.k \
         JOIN fz_pl_j6 f ON e.k = f.k;"
            .into(),
        // Star-ish shape (j1 joins each other table) -> different edge table.
        "SELECT count(*)::int8 FROM fz_pl_j1 a \
         JOIN fz_pl_j2 b ON a.k = b.k JOIN fz_pl_j3 c ON a.k = c.k \
         JOIN fz_pl_j4 d ON a.k = d.k JOIN fz_pl_j5 e ON a.k = e.k;"
            .into(),
    ];
    v.extend(sweep(g, &[GEQO], &queries));
    v.push(raw("DROP TABLE fz_pl_j1, fz_pl_j2, fz_pl_j3, fz_pl_j4, fz_pl_j5, fz_pl_j6;"));
    v
}

// -------------------------------------------------------------- bitmapor --

/// BitmapOr / BitmapAnd path construction and OR-clause index matching.
/// make_bitmap_paths_for_or_group, create_bitmap_or_path, cost_bitmap_or_
/// node, make_bitmap_or, make_bitmap_and, consider_new_or_clause,
/// or_arg_index_match_cmp[_group], is_pseudo_constant_for_index.
fn gen_bitmapor(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_pl_o (pk int4 PRIMARY KEY, a int4, b int4, c int4);",
        "INSERT INTO fz_pl_o SELECT i, i % 50, (i * 3) % 60, (i * 7) % 40 \
         FROM generate_series(1, 400) i;",
        "CREATE INDEX fz_pl_o_a ON fz_pl_o (a);",
        "CREATE INDEX fz_pl_o_b ON fz_pl_o (b);",
        "CREATE INDEX fz_pl_o_c ON fz_pl_o (c);",
        "ANALYZE fz_pl_o;",
        // Second table for the join-context OR extraction (orclauses.c).
        "CREATE TABLE fz_pl_o2 (pk int4 PRIMARY KEY, d int4);",
        "INSERT INTO fz_pl_o2 SELECT i, i % 30 FROM generate_series(1, 200) i;",
        "ANALYZE fz_pl_o2;",
    ]);
    let queries: Vec<String> = vec![
        // Two-index BitmapOr.
        "SELECT pk FROM fz_pl_o WHERE a = 3 OR b = 5 ORDER BY pk;".into(),
        // Three-index BitmapOr.
        "SELECT pk FROM fz_pl_o WHERE a = 1 OR b = 2 OR c = 3 ORDER BY pk;".into(),
        // BitmapAnd of two selective index conditions.
        "SELECT pk FROM fz_pl_o WHERE a < 10 AND b < 10 ORDER BY pk;".into(),
        // OR of IN-lists across two indexed columns (SAOP bitmap arms).
        "SELECT pk FROM fz_pl_o WHERE a IN (1, 2, 3) OR b IN (4, 5) ORDER BY pk;".into(),
        // Same-column OR group (or_arg_index_match_cmp_group / SAOP fold).
        "SELECT pk FROM fz_pl_o WHERE a = 1 OR a = 2 OR a = 7 OR a = 9 ORDER BY pk;".into(),
        // OR clause in a join (consider_new_or_clause extracts the
        // index-optimizable restriction from the joined OR).
        "SELECT o.pk FROM fz_pl_o o JOIN fz_pl_o2 x ON o.pk = x.pk \
         WHERE o.a = 4 OR o.b = 8 ORDER BY o.pk;"
            .into(),
        // BitmapAnd combined with a BitmapOr child.
        "SELECT pk FROM fz_pl_o WHERE c < 15 AND (a = 2 OR b = 3) ORDER BY pk;".into(),
    ];
    v.extend(sweep(g, &[BITMAP], &queries));
    v.push(raw("DROP TABLE fz_pl_o;"));
    v.push(raw("DROP TABLE fz_pl_o2;"));
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::stmt::Gen;

    /// Every shape emits a self-contained group: balanced CREATE/DROP for
    /// each fixture it touches, and every row-returning statement carries
    /// an ORDER BY (total-order discipline) unless it is aggregate-only.
    #[test]
    fn shapes_are_self_contained_and_ordered() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        for shape in SHAPES {
            // Drive the specific shape by pinning the weight table to it.
            let wt = crate::weights::WeightTable::defaults();
            let mut rng = Rng::new(1);
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &wt, &mut prods, 4);
            let stmts: Vec<StmtKind> = match *shape {
                "planner:sample" => gen_sample(&mut g),
                "planner:tidrange" => gen_tidrange(&mut g),
                "planner:groupsort" => gen_groupsort(&mut g),
                "planner:setop" => gen_setop(&mut g),
                "planner:winrun" => gen_winrun(&mut g),
                "planner:geqo" => gen_geqo(&mut g),
                _ => gen_bitmapor(&mut g),
            };
            let sql: Vec<String> = stmts.iter().map(|s| s.to_sql()).collect();
            let text = sql.join("\n");
            // Balanced CREATE TABLE / DROP TABLE (count table names dropped).
            let creates = sql.iter().filter(|s| s.starts_with("CREATE TABLE ")).count();
            let drop_names: usize = sql
                .iter()
                .filter(|s| s.starts_with("DROP TABLE "))
                .map(|s| s.matches(',').count() + 1)
                .sum();
            assert_eq!(
                creates, drop_names,
                "{shape}: {creates} CREATE TABLE vs {drop_names} dropped names\n{text}"
            );
            // Every SET has a matching RESET in the group.
            let sets = sql.iter().filter(|s| s.starts_with("SET ")).count();
            let resets = sql.iter().filter(|s| s.starts_with("RESET ")).count();
            assert_eq!(sets, resets, "{shape}: {sets} SET vs {resets} RESET\n{text}");
            // Every SELECT that projects rows (not a bare count/sum) ends
            // with ORDER BY before its semicolon.
            for s in &sql {
                if s.starts_with("SELECT ") && !s.contains("count(*)") && !s.starts_with("SELECT count") {
                    assert!(
                        s.contains("ORDER BY"),
                        "{shape}: row-returning select without ORDER BY: {s}"
                    );
                }
            }
        }
    }
}
