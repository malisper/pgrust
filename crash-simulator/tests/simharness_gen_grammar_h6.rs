//! H6 grammar-arm unit tests: one shape assertion per new gen_query
//! production (plus the MERGE DML form and the widened census EXPLAIN gate).
//!
//! Method: drive `noise::gen_query` directly over a fixed schema with a
//! seeded RNG until every new production (and sub-arm) has emitted, then
//! check each captured SQL text for the load-bearing syntax of its target
//! plan shape. This is deliberately text-level: the differential oracle owns
//! semantics; these tests pin that the generator EMITS the form at all (the
//! H3 0/9 lesson) and that each form carries its screen flags.

use std::collections::BTreeMap;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use simharness::gen::noise;
use simharness::gen::prodreg as pr;
use simharness::gen::profile::{GenProfile, StatementWeights};
use simharness::gen::schema::SchemaState;
use simharness::plan::{Mark, Sql};
use simharness::runner::driver::{
    execute_plan, BasicCheckEval, BasicDiffClassifier, ExecOptions,
};
use simharness::runner::planface;

#[path = "testutil/mod.rs"]
mod testutil;

fn default_gen_profile() -> GenProfile {
    let bytes = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/gen_profiles/default.json"),
    )
    .unwrap();
    GenProfile::from_bytes(&bytes).unwrap().0
}

/// Two-table schema (self-join-friendly) with all exact col types present.
fn schema_with_tables(rng: &mut ChaCha8Rng, profile: &GenProfile) -> SchemaState {
    let mut s = SchemaState::default();
    s.create_table(rng, &profile.table_shape);
    s.create_table(rng, &profile.table_shape);
    s
}

/// Drive gen_query until every registered sub-path has been seen (bounded);
/// returns node-name -> one sample (sql, trace-of-that-statement).
fn corpus() -> BTreeMap<String, (Sql, Vec<String>)> {
    let profile = default_gen_profile();
    let mut rng = ChaCha8Rng::seed_from_u64(60660);
    let mut schema_rng = ChaCha8Rng::seed_from_u64(1);
    let schema = schema_with_tables(&mut schema_rng, &profile);
    let mut seen: BTreeMap<String, (Sql, Vec<String>)> = BTreeMap::new();
    for _ in 0..40_000 {
        let mut trace = Vec::new();
        let Some(q) = noise::gen_query(&schema, &profile, &mut rng, &mut trace) else {
            panic!("gen_query returned None with tables present");
        };
        for node in &trace {
            seen.entry(node.clone()).or_insert_with(|| (q.clone(), trace.clone()));
        }
    }
    seen
}

#[test]
fn h6_every_new_query_production_emits_with_the_target_syntax() {
    let seen = corpus();
    // (production node, load-bearing syntax fragment of the plan shape)
    let expect: &[(&str, &str)] = &[
        (pr::Q_EXISTS_SEMI, "WHERE EXISTS (SELECT 1 FROM "),
        (pr::Q_NOT_EXISTS_ANTI, "WHERE NOT EXISTS (SELECT 1 FROM "),
        (pr::Q_IN_SUBQ, " IN (SELECT "),
        (pr::SSQ_CORRELATED_COUNT, "(SELECT count(*) FROM "),
        (pr::SSQ_INITPLAN_MAX, "(SELECT COALESCE(max("),
        (pr::Q_CTE_MATERIALIZED, "WITH c AS MATERIALIZED (SELECT "),
        (pr::Q_CTE_RECURSIVE, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < "),
        (pr::SO_UNION, " UNION SELECT "),
        (pr::SO_UNION_ALL, " UNION ALL SELECT "),
        (pr::SO_INTERSECT, " INTERSECT SELECT "),
        (pr::SO_EXCEPT, " EXCEPT SELECT "),
        (pr::Q_UNION_ALL_TOPK, " UNION ALL SELECT id FROM "),
        (pr::Q_HAVING, " HAVING count(*) >= "),
        (pr::Q_GROUP_NOAGG, " GROUP BY "),
        (pr::Q_DISTINCT, "SELECT DISTINCT "),
        (pr::Q_DISTINCT_ON, "SELECT DISTINCT ON ("),
        (pr::GS_ROLLUP, "GROUP BY ROLLUP("),
        (pr::GS_CUBE, "GROUP BY CUBE("),
        (pr::GS_SETS, "GROUP BY GROUPING SETS (("),
        (pr::W_ROW_NUMBER, "row_number() OVER (ORDER BY id)"),
        (pr::W_RANK, "rank() OVER (ORDER BY id)"),
        (pr::W_SUM_OVER, "sum(id) OVER (ORDER BY id)"),
        (pr::W_SUM_PARTITION, "sum(id) OVER (PARTITION BY "),
        (pr::Q_VALUES_SCAN, "FROM (VALUES ("),
        (pr::Q_SUBQUERY_SCAN, " FROM (SELECT id FROM "),
        (pr::Q_PROJECT_SET, ", generate_series(1, "),
        (pr::RES_NO_FROM, " + "),
        (pr::RES_WHERE_FALSE, "WHERE false"),
        (pr::RES_MINMAX, "(id) FROM "),
        (pr::TID_POINT, "WHERE ctid = '(0,"),
        (pr::TID_RANGE, "WHERE ctid > '(0,0)'::tid AND ctid < '(4096,0)'::tid"),
        (pr::SMP_BERNOULLI, "TABLESAMPLE BERNOULLI (100) REPEATABLE ("),
        (pr::SMP_SYSTEM, "TABLESAMPLE SYSTEM (100) REPEATABLE ("),
        (pr::Q_JSON_TABLE, "FROM json_table('["),
        (pr::Q_FULL_JOIN, " FULL JOIN "),
        (pr::Q_OR_QUAL, " OR "),
        (pr::Q_FOR_UPDATE, " FOR UPDATE"),
    ];
    for (node, frag) in expect {
        let Some((sql, trace)) = seen.get(*node) else {
            panic!("production '{node}' never emitted in a 40k-draw corpus");
        };
        assert!(
            sql.text().contains(frag),
            "production '{node}' sample lacks its target syntax '{frag}': {} (trace {trace:?})",
            sql.text()
        );
        assert_eq!(sql.mark, Mark::Read, "'{node}' must be a compared read");
    }
}

#[test]
fn h6_project_set_is_order_underdetermined_and_others_are_not() {
    let seen = corpus();
    // ProjectSet expands rows under a tied ORDER BY id — the ladder must
    // sort-normalize, so the flag is required.
    let (ps, _) = seen.get(pr::Q_PROJECT_SET).expect("project-set emitted");
    assert!(ps.flags.order_underdetermined, "project-set must carry the flag");
    // The new set-returning-free reads promise full determinism.
    for node in [pr::Q_EXISTS_SEMI, pr::Q_SETOP, pr::Q_DISTINCT_ON, pr::Q_FULL_JOIN] {
        let (q, _) = seen.get(node).expect("emitted");
        assert!(!q.flags.order_underdetermined, "'{node}' should be fully ordered");
        assert!(!q.flags.float_lenient, "'{node}' has no float aggregate");
    }
}

#[test]
fn h6_merge_dml_emits_key_addressed_guarded_form() {
    let profile = default_gen_profile();
    let mut rng = ChaCha8Rng::seed_from_u64(60661);
    let mut schema_rng = ChaCha8Rng::seed_from_u64(2);
    let mut schema = schema_with_tables(&mut schema_rng, &profile);
    let mut found = None;
    for _ in 0..5_000 {
        let mut trace = Vec::new();
        if let Some(sql) = noise::gen_dml(&mut schema, &mut rng, &profile, &mut trace) {
            if trace.first().map(String::as_str) == Some(pr::DML_MERGE) {
                found = Some(sql);
                break;
            }
        }
    }
    let sql = found.expect("dml:merge never emitted in 5k draws");
    assert!(sql.text().starts_with("MERGE INTO "), "{}", sql.text());
    assert!(sql.text().contains(" USING (VALUES ("), "{}", sql.text());
    assert!(
        sql.text().contains("WHEN NOT MATCHED THEN DO NOTHING"),
        "not-matched arm must be inert (key-addressed subset): {}",
        sql.text()
    );
    assert_eq!(sql.mark, Mark::Mutation);
}

/// The H5 registry-lock discipline extended: every H6 node is registered and
/// reachable-by-default under the checked-in default profile.
#[test]
fn h6_productions_registered_and_default_reachable() {
    let prop_names: Vec<String> = simharness::oracle::props::v1_set()
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    let refs: Vec<&str> = prop_names.iter().map(|s| s.as_str()).collect();
    let reg = pr::registry(&refs);
    let profile = default_gen_profile();
    for node in [
        pr::Q_EXISTS_SEMI, pr::Q_NOT_EXISTS_ANTI, pr::Q_IN_SUBQ, pr::Q_SCALAR_SUBQ,
        pr::Q_CTE_MATERIALIZED, pr::Q_CTE_RECURSIVE, pr::Q_SETOP, pr::Q_UNION_ALL_TOPK,
        pr::Q_HAVING, pr::Q_GROUP_NOAGG, pr::Q_DISTINCT, pr::Q_DISTINCT_ON,
        pr::Q_GROUPING_SETS, pr::Q_WINDOW, pr::Q_VALUES_SCAN, pr::Q_SUBQUERY_SCAN,
        pr::Q_PROJECT_SET, pr::Q_RESULT, pr::Q_TID, pr::Q_TABLESAMPLE,
        pr::Q_JSON_TABLE, pr::Q_FULL_JOIN, pr::Q_OR_QUAL, pr::Q_FOR_UPDATE,
        pr::DML_MERGE,
    ] {
        let def = reg.iter().find(|d| d.name == node).unwrap_or_else(|| {
            panic!("H6 production '{node}' missing from the registry")
        });
        assert!(
            pr::gate_reason(def, &profile).is_none(),
            "H6 production '{node}' gated-unreachable under the default profile"
        );
    }
}

// ---------------------------------------------------------------------------
// Census EXPLAIN gate widening (driver.rs): WITH-headed reads and DML heads
// are fingerprinted; utility statements are still excluded.
// ---------------------------------------------------------------------------

fn fsql(text: &str, mark: planface::Mark) -> planface::Sql {
    planface::Sql { text: text.into(), mark, meta: planface::SqlMeta::default() }
}

#[test]
fn h6_census_gate_admits_with_and_dml_explains_and_still_excludes_utilities() {
    let plan = planface::Plan {
        header: testutil::header(60662),
        steps: vec![
            planface::Step::Query(fsql("SELECT 1", planface::Mark::Read)),
            planface::Step::Query(fsql(
                "WITH c AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM c ORDER BY x",
                planface::Mark::Read,
            )),
            planface::Step::Dml(fsql("INSERT INTO t (id) VALUES (1)", planface::Mark::Mutation)),
            planface::Step::Dml(fsql("DELETE FROM t WHERE id = 1", planface::Mark::Mutation)),
            planface::Step::Dml(fsql(
                "MERGE INTO t USING (VALUES (1)) AS s(mid) ON t.id = s.mid WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN DO NOTHING",
                planface::Mark::Mutation,
            )),
            planface::Step::Dml(fsql("TRUNCATE t", planface::Mark::Mutation)),
            // X4-style Query-marked utility: must never be EXPLAINed.
            planface::Step::Query(fsql("PREPARE p1 AS SELECT 1", planface::Mark::Passthrough)),
        ],
    };
    let mut dut = testutil::MockSession::ok("dut");
    let opts = ExecOptions { explain_every: 1, ..ExecOptions::default() };
    let report =
        execute_plan(&plan, &mut dut, None, &BasicCheckEval, &BasicDiffClassifier, &opts);
    assert!(report.failure.is_none(), "{:?}", report.failure);
    let explains: Vec<&String> =
        dut.calls.iter().filter(|c| c.starts_with("EXPLAIN (COSTS OFF) ")).collect();
    let heads: Vec<&str> = explains
        .iter()
        .map(|c| {
            c.trim_start_matches("EXPLAIN (COSTS OFF) ")
                .split_whitespace()
                .next()
                .unwrap_or("")
        })
        .collect();
    assert_eq!(
        heads,
        vec!["SELECT", "WITH", "INSERT", "DELETE", "MERGE"],
        "census gate must admit SELECT/WITH reads + INSERT/DELETE/MERGE DML \
         (plan-only) and exclude TRUNCATE + utilities; explains: {explains:?}"
    );
    // The DML statements themselves executed exactly once (EXPLAIN never
    // re-runs a mutation).
    assert_eq!(dut.calls.iter().filter(|c| c.as_str() == "DELETE FROM t WHERE id = 1").count(), 1);
}
