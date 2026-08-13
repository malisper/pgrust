//! Statistics build + estimation drain module (STATS lane): the
//! extended-statistics build/apply machinery
//! (backend/statistics/{extended_stats,dependencies,mcv,mvdistinct,
//! attribute_stats,relation_stats,stat_utils}.c), the per-column
//! ANALYZE stats-build arms (backend/commands/analyze.c —
//! compute_scalar_stats / compute_distinct_stats / compute_trivial_stats /
//! compute_index_stats / analyze_mcv_list), and the selfuncs.c estimation
//! arms that CONSUME those stats (backend/utils/adt/selfuncs.c —
//! eqsel/scalarineqsel/booltestsel/nulltestsel/scalararraysel/
//! rowcomparesel/join selectivity + the *costestimate index cost
//! functions). line-gap-report-004 ranks this whole family at 5,784 unhit
//! lines: the default corpus builds tables but never CREATE STATISTICS,
//! never shapes data for the MCV/histogram/dependency arms, and never
//! forces the index-cost estimators.
//!
//! Mechanism (plansel/opt2 discipline): each pick is a SELF-CONTAINED
//! group over fixed `fz_st_*` fixtures created + dropped in-group. Build a
//! table whose every value is a pure integer formula of the row number
//! (identical on both engines by construction), shape the distribution to
//! exercise a specific stats path (skewed → MCV, all-distinct → ndistinct
//! = -1, correlated → dependencies, high-null → null_frac, too-wide text →
//! the WIDTH_THRESHOLD arm), CREATE STATISTICS + ANALYZE, then:
//!   - introspect pg_stats / pg_stats_ext on STABLE columns (structural
//!     surfaces only — MCV/histogram cardinalities, the ndistinct and
//!     dependencies canonical text, all-distinct / has-nulls booleans;
//!     never the float freq/correlation columns, which the B1 float ruling
//!     keeps off the exact-compare path). A structural divergence here is
//!     a real stats-BUILD bug (analyze.c / statext build).
//!   - run the actual estimate-consuming queries (result set compared,
//!     total ORDER BY on every probe) under a sweep of enable_* / cost GUC
//!     profiles, plus EXPLAIN (COSTS OFF) for the plan shape. COSTS OFF
//!     keeps the never-compared cost/row estimates off the wire (v1
//!     ruling) while the planner still RUNS the selfuncs estimation code to
//!     choose the plan — so the arms are covered and a systematic estimate
//!     divergence surfaces as a plan-shape diff, not sampling noise.
//!
//! Determinism laws (same as plansel/opt2):
//!   - tables stay small (<= 5,000 rows) so the default 30,000-row ANALYZE
//!     sample is EXHAUSTIVE: every row is sampled, so MCV lists, histogram
//!     bounds, ndistinct, dependencies and the multivariate stats are a
//!     pure function of the table state — byte-identical on both sides.
//!   - every row-returning probe carries a TOTAL order (ORDER BY ending in
//!     the full projected key or pk); aggregates are exact-typed (int8 /
//!     bool / text) — no float aggregates (B1).
//!   - introspection SELECTs filter to the group's own uniquely-named
//!     fixture and ORDER BY the stable name column.
//!   - every SET has its RESET in reverse order in the same group; the
//!     stats-manipulation arm runs writes only inside BEGIN..ROLLBACK.

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// A named GUC profile (same shape as opt2's local `Prof`); SET each pair
/// in order, RESET in reverse, all inside the emitting group.
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
    gucs: &[("enable_seqscan", "off"), ("enable_indexscan", "off"), ("enable_indexonlyscan", "off")],
};
const HASHJ: Prof = Prof {
    name: "hashj",
    gucs: &[("enable_mergejoin", "off"), ("enable_nestloop", "off")],
};
const MERGEJ: Prof = Prof {
    name: "mergej",
    gucs: &[("enable_hashjoin", "off"), ("enable_nestloop", "off")],
};
const NESTL: Prof = Prof {
    name: "nestl",
    gucs: &[("enable_hashjoin", "off"), ("enable_mergejoin", "off"), ("enable_memoize", "off")],
};
const HASHAGG: Prof = Prof { name: "hashagg", gucs: &[("enable_sort", "off")] };
const GROUPAGG: Prof = Prof { name: "groupagg", gucs: &[("enable_hashagg", "off")] };

/// SET/body/RESET bracket, RESETs in reverse order, one group.
fn bracket(p: &Prof, body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut v: Vec<StmtKind> =
        p.gucs.iter().map(|(n, val)| raw(format!("SET {n} = {val};"))).collect();
    v.extend(body);
    for (n, _) in p.gucs.iter().rev() {
        v.push(raw(format!("RESET {n};")));
    }
    v
}

/// Emit each query under each profile (fires `stats:prof:<name>` per pick).
fn sweep(g: &mut Gen, profs: &[Prof], queries: &[String]) -> Vec<StmtKind> {
    let mut v = Vec::new();
    for p in profs {
        g.fire2("stats:prof:", p.name);
        v.extend(bracket(p, queries.iter().map(|q| raw(q.clone())).collect()));
    }
    v
}

/// EXPLAIN (COSTS OFF) wrapper: plan shape is compared, cost/row estimates
/// never ride the wire (v1 ruling), but the planner still runs the
/// selfuncs estimation code to pick the plan.
fn ex(q: &str) -> String {
    format!("EXPLAIN (COSTS OFF) {q}")
}

const ARMS: &[&str] = &[
    "stats:dep",
    "stats:ndist",
    "stats:mcv",
    "stats:analyze",
    "stats:sel",
    "stats:index",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_stats_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("stats");
    let arm = g.weights.pick(g.rng, ARMS);
    g.fire2("stats:arm:", &arm["stats:".len()..]);
    match arm {
        "stats:dep" => gen_dep(g),
        "stats:ndist" => gen_ndist(g),
        "stats:mcv" => gen_mcv(g),
        "stats:analyze" => gen_analyze(g),
        "stats:index" => gen_index(g),
        _ => gen_sel(g),
    }
}

// --------------------------------------------------------- dependencies ---

/// Functional-dependency extended stats: `a` fully determines `b`
/// (b = a/10) and `grp` (grp = a%5 is NOT a dependency target but shares
/// the table); multi-equality queries on (a,b) let dependencies.c collapse
/// the naive independent-selectivity overestimate. Also an expression
/// dependency ON ((a+b), grp). Drives statext_dependencies_build/serialize/
/// deserialize, dependency_degree, dependencies_clauselist_selectivity,
/// dependency_is_compatible_clause/expression, statext_is_compatible_clause.
fn gen_dep(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_st_dep (pk int4 PRIMARY KEY, a int4, b int4, grp int4, txt text);",
        // a in 0..99; b = a/10 (0..9) so a -> b holds exactly; grp = a%5.
        // txt is a deterministic label of a so (a -> txt) also holds.
        "INSERT INTO fz_st_dep \
         SELECT i, i % 100, (i % 100) / 10, (i % 100) % 5, 'k' || ((i % 100) / 10) \
         FROM generate_series(1, 4000) i;",
        "CREATE STATISTICS fz_st_dep_s (dependencies) ON a, b, grp FROM fz_st_dep;",
        "CREATE STATISTICS fz_st_dep_e (dependencies) ON (a + b), grp FROM fz_st_dep;",
        "ANALYZE fz_st_dep;",
        // Build-side introspection: pg_dependencies canonical text is
        // deterministic under exhaustive sample (a stats-build divergence
        // here is a real dependencies.c bug).
        "SELECT statistics_name, dependencies::text FROM pg_stats_ext \
         WHERE tablename = 'fz_st_dep' AND dependencies IS NOT NULL \
         ORDER BY statistics_name;",
    ]);
    // Apply-side: consistent multi-equality clauses (dependency collapses
    // the overestimate), an inconsistent pair (a=37 forces b=3; a=37,b=9
    // is near-empty), and the expression-dependency clause.
    let queries: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_st_dep WHERE a = 37 AND b = 3;".into(),
        "SELECT count(*)::int8 FROM fz_st_dep WHERE a = 37 AND b = 9;".into(),
        "SELECT count(*)::int8 FROM fz_st_dep WHERE a = 42 AND grp = 2;".into(),
        "SELECT count(*)::int8 FROM fz_st_dep WHERE (a + b) = 44 AND grp = 4;".into(),
        "SELECT pk FROM fz_st_dep WHERE a = 55 AND b = 5 AND grp = 0 ORDER BY pk;".into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, SEQ, IDX], &queries));
    v.push(raw(ex("SELECT count(*) FROM fz_st_dep WHERE a = 37 AND b = 3 AND grp = 2;")));
    v.push(raw("DROP TABLE fz_st_dep;"));
    v
}

// -------------------------------------------------------------- ndistinct ---

/// Multi-column ndistinct extended stats: `x` (0..49) and `y` (0..49) are
/// correlated (y = x/2 region) so the JOINT ndistinct is far below the
/// product; GROUP BY x,y then reads the multivariate estimate. Drives
/// statext_ndistinct_build/serialize/deserialize, ndistinct_for_combination,
/// estimate_multivariate_ndistinct, estimate_num_groups, add_unique_group_var.
fn gen_ndist(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_st_nd (pk int4 PRIMARY KEY, x int4, y int4, z int4);",
        "INSERT INTO fz_st_nd \
         SELECT i, i % 50, (i % 50) / 2, i % 7 \
         FROM generate_series(1, 5000) i;",
        "CREATE STATISTICS fz_st_nd_s (ndistinct) ON x, y, z FROM fz_st_nd;",
        "ANALYZE fz_st_nd;",
        "SELECT statistics_name, n_distinct::text FROM pg_stats_ext \
         WHERE tablename = 'fz_st_nd' AND n_distinct IS NOT NULL \
         ORDER BY statistics_name;",
    ]);
    let queries: Vec<String> = vec![
        "SELECT x, y, count(*)::int8 FROM fz_st_nd GROUP BY x, y ORDER BY x, y;".into(),
        "SELECT x, y, z, count(*)::int8 FROM fz_st_nd GROUP BY x, y, z ORDER BY x, y, z;".into(),
        "SELECT count(*)::int8 FROM (SELECT DISTINCT x, y FROM fz_st_nd) s;".into(),
    ];
    v.extend(sweep(g, &[HASHAGG, GROUPAGG], &queries));
    v.push(raw(ex("SELECT x, y FROM fz_st_nd GROUP BY x, y;")));
    v.push(raw("DROP TABLE fz_st_nd;"));
    v
}

// ------------------------------------------------------------------- mcv ---

/// MCV extended stats over correlated skewed columns. `hot` is heavily
/// skewed (most rows share value 0, a short tail carries the rest) so a
/// per-combination MCV list is built and consulted; clauses mix =, <, >,
/// IN and IS NULL (with a NULL-bearing column) and OR to walk the MCV
/// match arms. Drives statext_mcv_build/serialize/deserialize,
/// mcv_get_match_bitmap, statext_mcv_clauselist_selectivity,
/// mcv_clause_selectivity_or, mcv_match_expression, build_sorted_items,
/// build_column_frequencies, get_mincount_for_mcv_list.
fn gen_mcv(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_st_mcv (pk int4 PRIMARY KEY, hot int4, cat int4, opt int4);",
        // hot: ~80% zeros, tail 1..9 (skewed -> MCV built). cat tracks hot
        // (cat = hot, so (hot -> cat)). opt has NULLs.
        "INSERT INTO fz_st_mcv \
         SELECT i, \
                CASE WHEN i % 5 <> 0 THEN 0 ELSE (i % 9) + 1 END, \
                CASE WHEN i % 5 <> 0 THEN 0 ELSE (i % 9) + 1 END, \
                CASE WHEN i % 4 = 0 THEN NULL ELSE i % 3 END \
         FROM generate_series(1, 5000) i;",
        "CREATE STATISTICS fz_st_mcv_s (mcv) ON hot, cat, opt FROM fz_st_mcv;",
        "ANALYZE fz_st_mcv;",
        // MCV list length is a structural build surface (deterministic
        // under exhaustive sample); the per-item float freqs stay off wire.
        "SELECT statistics_name, array_length(most_common_vals, 1) AS n_mcv \
         FROM pg_stats_ext WHERE tablename = 'fz_st_mcv' AND most_common_vals IS NOT NULL \
         ORDER BY statistics_name;",
    ]);
    let queries: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_st_mcv WHERE hot = 0 AND cat = 0;".into(),
        "SELECT count(*)::int8 FROM fz_st_mcv WHERE hot = 3 AND cat = 3;".into(),
        "SELECT count(*)::int8 FROM fz_st_mcv WHERE hot > 0 AND cat < 5;".into(),
        "SELECT count(*)::int8 FROM fz_st_mcv WHERE hot IN (0, 2, 4) AND opt IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_st_mcv WHERE (hot = 0 OR cat = 5) AND opt = 1;".into(),
        "SELECT pk FROM fz_st_mcv WHERE hot = 7 AND cat = 7 AND opt = 2 ORDER BY pk;".into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, SEQ], &queries));
    v.push(raw(ex("SELECT count(*) FROM fz_st_mcv WHERE hot = 0 AND cat = 0 AND opt IS NULL;")));
    v.push(raw("DROP TABLE fz_st_mcv;"));
    v
}

// -------------------------------------------------------- per-column build ---

/// Per-column ANALYZE stats-build drain: one table whose columns each
/// select a different std_typanalyze arm — a heavily-skewed int
/// (compute_scalar_stats MCV + histogram + analyze_mcv_list), an
/// all-distinct int (ndistinct = -1), a high-null int (null_frac), an
/// xid column (eq but no ordering -> compute_distinct_stats), a json
/// column (no eq -> compute_trivial_stats), and a very wide text column
/// (> WIDTH_THRESHOLD -> the too-wide arm of compute_scalar_stats). Then
/// introspect the STRUCTURAL pg_stats surfaces. Drives std_typanalyze,
/// compute_scalar_stats, compute_distinct_stats, compute_trivial_stats,
/// analyze_mcv_list, update_attstats, examine_attribute, std_fetch_func.
fn gen_analyze(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_st_an (\
            pk int4 PRIMARY KEY, \
            skew int4, \
            uniq int4, \
            nul int4, \
            xq xid, \
            j json, \
            wide text);",
        // skew: 90% value 0, tail 1..9 -> MCV + histogram + trivial-remainder.
        // uniq = pk (all distinct). nul: 1/3 NULL. xq: eq-only type.
        // j: json has no '=' -> trivial. wide: >1KB for i%50=0 else short.
        "INSERT INTO fz_st_an \
         SELECT i, \
                CASE WHEN i % 10 <> 0 THEN 0 ELSE (i % 9) + 1 END, \
                i, \
                CASE WHEN i % 3 = 0 THEN NULL ELSE i % 11 END, \
                (i % 500)::text::xid, \
                ('{\"k\":' || (i % 10) || '}')::json, \
                CASE WHEN i % 50 = 0 THEN repeat('w', 1100) ELSE 's' || (i % 20) END \
         FROM generate_series(1, 3000) i;",
        "ANALYZE fz_st_an;",
        // Structural, exact-compare surfaces only (no float columns): the
        // all-distinct verdict, has-nulls verdict, and MCV / histogram
        // cardinalities. A divergence is a real analyze.c stats-build bug.
        "SELECT attname, \
                (n_distinct = -1) AS all_distinct, \
                (null_frac > 0) AS has_nulls, \
                array_length(most_common_vals, 1) AS n_mcv, \
                array_length(histogram_bounds, 1) AS n_hist \
         FROM pg_stats WHERE tablename = 'fz_st_an' ORDER BY attname;",
    ]);
    // A couple of estimate-consuming probes over the built stats.
    let queries: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_st_an WHERE skew = 0;".into(),
        "SELECT count(*)::int8 FROM fz_st_an WHERE nul IS NULL;".into(),
        "SELECT count(*)::int8 FROM fz_st_an WHERE uniq < 500;".into(),
    ];
    v.extend(sweep(g, &[DEFAULTP, SEQ], &queries));
    v.push(raw("DROP TABLE fz_st_an;"));
    v
}

// ---------------------------------------------------- selfuncs estimation ---

/// selfuncs.c restriction + join estimation arms over two analyzed tables.
/// Walks booltestsel, nulltestsel, scalararraysel (= ANY / <> ALL / IN),
/// scalarineqsel/ineq_histogram_selectivity (range), rowcomparesel
/// (RowCompare), var_eq_const/var_eq_non_const (eqsel), and the join
/// selectivity family (eqjoinsel, neqjoinsel, mergejoinscansel via forced
/// merge join, scalar*joinsel via range join) under a plan-forcing sweep.
fn gen_sel(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_st_r (\
            pk int4 PRIMARY KEY, a int4, b int4, f bool, t timestamp, n numeric, s text);",
        "INSERT INTO fz_st_r \
         SELECT i, i % 200, i % 50, (i % 3 = 0), \
                timestamp '2020-01-01' + (i % 365) * interval '1 day', \
                (i % 1000)::numeric / 10, \
                's' || (i % 100) \
         FROM generate_series(1, 5000) i;",
        "CREATE INDEX fz_st_r_a ON fz_st_r (a);",
        "CREATE INDEX fz_st_r_t ON fz_st_r (t);",
        "ANALYZE fz_st_r;",
        "CREATE TABLE fz_st_j (pk int4 PRIMARY KEY, a int4, k int4);",
        "INSERT INTO fz_st_j SELECT i, i % 200, i % 80 FROM generate_series(1, 2000) i;",
        "CREATE INDEX fz_st_j_a ON fz_st_j (a);",
        "ANALYZE fz_st_j;",
    ]);
    // Restriction probes (single table): each hits a distinct selfuncs arm.
    let restr: Vec<String> = vec![
        // booltestsel: IS TRUE / FALSE / NOT TRUE / UNKNOWN.
        "SELECT count(*)::int8 FROM fz_st_r WHERE f IS TRUE;".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE f IS NOT TRUE;".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE f IS FALSE;".into(),
        // nulltestsel.
        "SELECT count(*)::int8 FROM fz_st_r WHERE s IS NOT NULL;".into(),
        // scalararraysel: = ANY / IN / <> ALL.
        "SELECT count(*)::int8 FROM fz_st_r WHERE a = ANY (ARRAY[1, 7, 42, 99, 150]);".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE a IN (3, 6, 9, 12, 15, 18);".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE a <> ALL (ARRAY[0, 1, 2]);".into(),
        // scalarineqsel / ineq_histogram_selectivity on several types.
        "SELECT count(*)::int8 FROM fz_st_r WHERE a < 40;".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE a BETWEEN 20 AND 120;".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE t < timestamp '2020-06-01';".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE n > 25.0;".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE s < 's5';".into(),
        // var_eq_const / var_eq_non_const.
        "SELECT count(*)::int8 FROM fz_st_r WHERE a = 37;".into(),
        "SELECT count(*)::int8 FROM fz_st_r WHERE a = (b + 0);".into(),
        // rowcomparesel.
        "SELECT count(*)::int8 FROM fz_st_r WHERE (a, b) < (50, 10);".into(),
    ];
    v.extend(sweep(g, &[SEQ, IDX, BITMAP], &restr));
    // Join probes: hash / merge / nestloop force the eq + mergejoinscansel
    // + scalar-inequality join arms.
    let joins: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_st_r r JOIN fz_st_j j ON r.a = j.a;".into(),
        "SELECT count(*)::int8 FROM fz_st_r r JOIN fz_st_j j ON r.a = j.a WHERE r.b < 10;".into(),
        "SELECT count(*)::int8 FROM fz_st_r r JOIN fz_st_j j ON r.a < j.a AND r.a > j.a - 3;"
            .into(),
        "SELECT count(*)::int8 FROM fz_st_r r WHERE r.a <> ALL (SELECT a FROM fz_st_j);".into(),
    ];
    v.extend(sweep(g, &[HASHJ, MERGEJ, NESTL], &joins));
    v.push(raw(ex(
        "SELECT r.pk FROM fz_st_r r JOIN fz_st_j j ON r.a = j.a WHERE r.f IS TRUE;",
    )));
    v.push(raw("DROP TABLE fz_st_r;"));
    v.push(raw("DROP TABLE fz_st_j;"));
    v
}

// ------------------------------------------------------- index estimators ---

/// Index cost-estimator drain: a btree (plain + expression + partial), a
/// GIN index over an int[] column (core array_ops), and a BRIN index, then
/// force each access path so its *costestimate runs. Drives btcostestimate,
/// btcost_correlation, genericcostestimate, gincostestimate, gincost_*,
/// brincostestimate, index_other_operands_eval_cost, get_quals_from_index
/// clauses, add_predicate_to_index_quals, compute_index_stats.
fn gen_index(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_st_ix (pk int4 PRIMARY KEY, a int4, b int4, arr int4[]);",
        "INSERT INTO fz_st_ix \
         SELECT i, i % 300, i % 40, ARRAY[i % 10, (i % 10) + 1, (i % 7) + 20] \
         FROM generate_series(1, 5000) i;",
        "CREATE INDEX fz_st_ix_a ON fz_st_ix (a);",
        // Expression index -> compute_index_stats builds stats on abs(b-20).
        "CREATE INDEX fz_st_ix_e ON fz_st_ix (abs(b - 20));",
        // Partial index -> add_predicate_to_index_quals / predicate proof.
        "CREATE INDEX fz_st_ix_p ON fz_st_ix (a) WHERE b > 20;",
        "CREATE INDEX fz_st_ix_g ON fz_st_ix USING gin (arr);",
        "CREATE INDEX fz_st_ix_br ON fz_st_ix USING brin (a) WITH (pages_per_range = 4);",
        "ANALYZE fz_st_ix;",
    ]);
    // btree plain + expression + partial-predicate matched.
    let bt: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_st_ix WHERE a = 42;".into(),
        "SELECT count(*)::int8 FROM fz_st_ix WHERE a BETWEEN 10 AND 60;".into(),
        "SELECT count(*)::int8 FROM fz_st_ix WHERE abs(b - 20) < 5;".into(),
        "SELECT count(*)::int8 FROM fz_st_ix WHERE a = 15 AND b > 20;".into(),
    ];
    v.extend(sweep(g, &[IDX, BITMAP], &bt));
    // GIN over int[] (array_ops): containment + overlap operators.
    let gin: Vec<String> = vec![
        "SELECT count(*)::int8 FROM fz_st_ix WHERE arr @> ARRAY[3];".into(),
        "SELECT count(*)::int8 FROM fz_st_ix WHERE arr && ARRAY[1, 2, 3];".into(),
    ];
    v.extend(sweep(g, &[BITMAP], &gin));
    // BRIN range scan (bitmap only).
    v.extend(sweep(
        g,
        &[BITMAP],
        &["SELECT count(*)::int8 FROM fz_st_ix WHERE a BETWEEN 100 AND 140;".into()],
    ));
    v.push(raw(ex("SELECT count(*) FROM fz_st_ix WHERE arr @> ARRAY[3];")));
    v.push(raw("DROP TABLE fz_st_ix;"));
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_many(seed: u64, n: usize, w: &WeightTable) -> (Vec<String>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut sqls = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, w, &mut prods, 3);
            for s in gen_stats_module(&mut g) {
                sqls.push(s.to_sql());
            }
            prods_all.extend(prods);
        }
        (sqls, prods_all)
    }

    #[test]
    fn every_arm_fires_and_creates_then_drops() {
        let (_sqls, prods) = gen_many(0x57A7, 600, &WeightTable::defaults());
        for arm in [
            "stats:arm:dep",
            "stats:arm:ndist",
            "stats:arm:mcv",
            "stats:arm:analyze",
            "stats:arm:sel",
            "stats:arm:index",
        ] {
            assert!(prods.iter().any(|p| p == arm), "arm {arm} never fired");
        }
        assert!(prods.iter().any(|p| p == "stats"));
    }

    #[test]
    fn every_group_is_self_contained_create_drop() {
        // Each pick creates and drops the same number of tables in-group:
        // no fixture survives the group (single-session parity + no
        // cross-group residue). Count CREATE TABLE vs DROP TABLE per group.
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(9);
        for _ in 0..400 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let group = gen_stats_module(&mut g);
            let mut created: Vec<String> = Vec::new();
            let mut dropped: Vec<String> = Vec::new();
            for s in &group {
                let sql = s.to_sql();
                if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                    let name = rest.split(|c: char| c == ' ' || c == '(').next().unwrap();
                    created.push(name.to_string());
                }
                if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                    for name in rest.trim_end_matches(';').split(',') {
                        dropped.push(name.trim().to_string());
                    }
                }
            }
            created.sort();
            dropped.sort();
            assert_eq!(created, dropped, "unbalanced create/drop in group: {group:?}");
        }
    }

    #[test]
    fn sets_are_balanced_by_resets() {
        // Every SET in a group has a matching RESET (GUC state law): the
        // group leaves the session with identical GUC state on both sides.
        let (sqls, _) = gen_many(0xB0B, 500, &WeightTable::defaults());
        // Re-generate per group to count within-group balance.
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xB0B);
        for _ in 0..500 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let group = gen_stats_module(&mut g);
            let mut set = 0i32;
            let mut reset = 0i32;
            for s in &group {
                let sql = s.to_sql();
                if sql.starts_with("SET ") {
                    set += 1;
                }
                if sql.starts_with("RESET ") {
                    reset += 1;
                }
            }
            assert_eq!(set, reset, "unbalanced SET/RESET");
        }
        // Sanity: the corpus is non-trivial.
        assert!(sqls.len() > 500);
    }

    #[test]
    fn every_probe_query_is_ordered_or_aggregate() {
        // Determinism: any bare row-returning SELECT (not an aggregate,
        // not an introspection ORDER BY, not EXPLAIN/DDL/DML) must carry a
        // total ORDER BY. We assert every SELECT that projects a column
        // list without an aggregate ends in ORDER BY.
        let (sqls, _) = gen_many(0x0DDE_u64, 400, &WeightTable::defaults());
        for sql in &sqls {
            if !sql.starts_with("SELECT ") {
                continue;
            }
            let lower = sql.to_lowercase();
            let is_agg = lower.contains("count(") || lower.contains("array_length(");
            let is_meta = lower.contains("pg_stats");
            if is_agg || is_meta {
                continue;
            }
            assert!(
                lower.contains("order by"),
                "row-returning probe without total ORDER BY: {sql}"
            );
        }
    }

    #[test]
    fn deterministic_stream() {
        let w = WeightTable::defaults();
        let (a, _) = gen_many(2026, 60, &w);
        let (b, _) = gen_many(2026, 60, &w);
        assert_eq!(a, b);
        let (c, _) = gen_many(2027, 60, &w);
        assert_ne!(a, c);
    }
}
