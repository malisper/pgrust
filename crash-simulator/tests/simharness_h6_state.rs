//! H6 state-arm tests: index/ANALYZE/cardinality state ops, the file_fdw
//! foreign-table chain, the fault pair-floor (H5 find 1 fix), and the
//! col-type bridge passthrough (H5 find 2 fix).

use std::collections::BTreeMap;

use simharness::bridge;
use simharness::gen::budget::{Budgets, Kind};
use simharness::gen::prodreg;
use simharness::gen::profile::{GenProfile, StatementWeights};
use simharness::plan::{Plan, PlanItem, Step};

fn battery_profile(name: &str) -> GenProfile {
    let json = format!(
        r#"{{
            "name": "{name}",
            "plan_len": {{"min": 24, "max": 64}},
            "statement_weights": {{"ddl": 10, "dml": 24, "query": 30, "tx": 10,
                                   "arm": 4, "fault": 2, "property": 20}},
            "table_shape": {{"min_cols": 3, "max_cols": 3}},
            "iso_mix": {{"rc": 60, "rr": 30, "ser": 10}},
            "arm_sets": [[], [["work_mem", "4MB"]]]
        }}"#
    );
    GenProfile::from_bytes(json.as_bytes()).unwrap().0
}

fn corpus(profile: &GenProfile, seeds: std::ops::Range<u64>) -> Vec<Plan> {
    seeds
        .map(|s| bridge::generate_plan_with_ctx_traced(s, profile, "00", "t").0)
        .collect()
}

fn sql_texts(plan: &Plan) -> Vec<String> {
    let mut out = Vec::new();
    let steps = plan.items.iter().flat_map(|it| match it {
        PlanItem::Step(s) => std::slice::from_ref(s).iter(),
        PlanItem::Property { steps, .. } => steps.iter(),
    });
    for s in steps {
        if let Step::Ddl(q) | Step::Dml(q) | Step::Query(q) = s {
            out.push(q.text().to_string());
        }
    }
    out
}

// ------------------------------------------------------------- state ops

/// Every new state-op statement shape appears in a modest corpus, exactly in
/// its intended SQL form.
#[test]
fn h6_state_ops_all_emitted_with_expected_shapes() {
    let p = battery_profile("h6shapes");
    let plans = corpus(&p, 5000..5300);
    let all: Vec<String> = plans.iter().flat_map(|pl| sql_texts(pl)).collect();
    let has = |pred: &dyn Fn(&str) -> bool, what: &str| {
        assert!(all.iter().any(|s| pred(s)), "no statement matched: {what}");
    };
    has(&|s| s.starts_with("ANALYZE "), "ANALYZE <table>");
    has(&|s| s.starts_with("DROP INDEX "), "DROP INDEX");
    has(&|s| s.contains(" USING brin ("), "brin index");
    has(&|s| s.starts_with("CREATE INDEX ") && s.contains(" WHERE "), "partial index");
    has(
        &|s| s.starts_with("CREATE INDEX ") && (s.contains("((abs(") || s.contains("((lower(")),
        "expression index",
    );
    has(
        &|s| {
            s.starts_with("CREATE INDEX ")
                && !s.contains(" USING ")
                && !s.contains(" WHERE ")
                && !s.contains("((")
                && s.split_once('(').is_some_and(|(_, cols)| cols.contains(", "))
        },
        "multi-column index",
    );
    has(
        &|s| s.starts_with("INSERT INTO ") && s.contains("FROM generate_series("),
        "bulk insert",
    );
    has(&|s| s.starts_with("CREATE EXTENSION IF NOT EXISTS file_fdw"), "fdw extension");
    has(&|s| s.starts_with("CREATE SERVER IF NOT EXISTS simharness_fsrv"), "fdw server");
    has(&|s| s.starts_with("COPY (SELECT "), "fdw csv copy");
    has(&|s| s.starts_with("CREATE FOREIGN TABLE "), "foreign table");
    // The new query shapes.
    has(&|s| s.contains("ORDER BY c") && s.contains(", id"), "order-prefix query");
    has(
        &|s| s.starts_with("SELECT id FROM ") && s.contains(" AND "),
        "two-col equality query",
    );
    has(&|s| s.contains(" FROM ft"), "foreign scan query");
}

/// The fdw chain is ordered within a plan: extension before server before
/// COPY before CREATE FOREIGN TABLE, and any foreign-table read comes after
/// its CREATE FOREIGN TABLE.
#[test]
fn h6_fdw_chain_is_ordered_and_reads_follow_creation() {
    let p = battery_profile("h6fdworder");
    let mut saw_chain = false;
    for plan in corpus(&p, 5000..5200) {
        let texts = sql_texts(&plan);
        let pos = |pred: &dyn Fn(&str) -> bool| texts.iter().position(|s| pred(s));
        let ext = pos(&|s| s.starts_with("CREATE EXTENSION IF NOT EXISTS file_fdw"));
        let srv = pos(&|s| s.starts_with("CREATE SERVER IF NOT EXISTS"));
        let ft = pos(&|s| s.starts_with("CREATE FOREIGN TABLE "));
        if let (Some(e), Some(v), Some(f)) = (ext, srv, ft) {
            saw_chain = true;
            assert!(e < v && v < f, "fdw chain out of order: ext={e} srv={v} ft={f}");
            let copy = pos(&|s| s.starts_with("COPY (SELECT ")).unwrap();
            assert!(copy < f, "csv COPY must precede the foreign table");
        }
        // Reads of ftN require an earlier CREATE FOREIGN TABLE ftN (unless a
        // rollback removed it — then the model would not generate the read;
        // the coherence sweep enforces this globally, here we spot-check the
        // common case).
        for (i, s) in texts.iter().enumerate() {
            if s.starts_with("SELECT ") && s.contains(" FROM ft") {
                let name = s
                    .split(" FROM ft")
                    .nth(1)
                    .and_then(|r| r.split([' ', ';']).next())
                    .unwrap();
                let created = texts[..i]
                    .iter()
                    .any(|t| t.starts_with(&format!("CREATE FOREIGN TABLE ft{name} ")));
                assert!(created, "foreign read before creation: {s}");
            }
        }
    }
    assert!(saw_chain, "corpus never completed an fdw chain");
}

// -------------------------------------------------- fault pair floor (find 1)

/// H5 find 1 fix: any profile that declares fault weight > 0 gets a fault
/// budget of at least 2 (the disconnect pair) at every plan length, and the
/// floor never changes the total.
#[test]
fn h6_fault_pair_floor_holds_at_every_length() {
    let w = StatementWeights { ddl: 10, dml: 24, query: 30, tx: 10, arm: 4, fault: 2, property: 20 };
    for total in 2..=120u64 {
        let b = Budgets::allocate(&w, total, true);
        assert!(
            b.remaining(Kind::Fault) >= 2,
            "fault budget {} < 2 at total {total}",
            b.remaining(Kind::Fault)
        );
        assert_eq!(b.total_remaining(), total, "floor changed the total at {total}");
    }
    // fault weight 0: no floor, no fault budget.
    let w0 = StatementWeights { fault: 0, ..w };
    for total in [10u64, 60] {
        let b = Budgets::allocate(&w0, total, true);
        assert_eq!(b.remaining(Kind::Fault), 0);
        assert_eq!(b.total_remaining(), total);
    }
}

/// End-to-end: with the floor in place, the disconnect pair is actually
/// EMITTED under battery-shaped weights (the H5-dead configuration), and the
/// reach gate sees it at k=1.
#[test]
fn h6_disconnect_pair_emitted_under_battery_weights() {
    let p = battery_profile("h6fault");
    let mut acc = prodreg::KpathAccum::default();
    let mut pairs = 0u64;
    for seed in 5000..5300u64 {
        let (plan, _c, traces) = bridge::generate_plan_with_ctx_traced(seed, &p, "00", "t");
        acc.add(&traces);
        for it in &plan.items {
            if matches!(
                it,
                PlanItem::Step(Step::Fault(simharness::plan::FaultPoint::Disconnect))
            ) {
                pairs += 1;
            }
        }
    }
    assert!(pairs > 0, "no disconnect emitted in 300 battery-shaped seeds");
    assert!(acc.nodes.contains(prodreg::FAULT_DISCONNECT_PAIR));
    let names: Vec<String> = simharness::oracle::props::v1_set()
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let reg = prodreg::registry(&refs);
    let r = prodreg::evaluate(&acc, &reg, &p);
    assert!(
        !r.gated_unreachable.iter().any(|(n, _)| n == prodreg::STMT_FAULT),
        "stmt:fault still gated under the pair floor"
    );
    assert!(!r.k1.uncovered.contains(&prodreg::FAULT_DISCONNECT_PAIR.to_string()));
}

// ------------------------------------------------ col-type bridge (find 2)

/// H5 find 2 fix: runner-profile col_types flow through the bridge; the
/// float-lenient battery profile now really generates float8 columns and the
/// q:float-agg family emits.
#[test]
fn h6_col_types_bridge_and_float_agg_reachable() {
    // Explicit map: missing keys weigh 0.
    let mut p = serde_json::from_str::<simharness::runner::profile::Profile>(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json"),
        )
        .unwrap(),
    )
    .unwrap();
    p.table_shape.col_types = BTreeMap::from([("float8".to_string(), 7u32), ("int".to_string(), 1)]);
    let gp = bridge::runner_profile_to_gen(&p);
    assert_eq!(gp.table_shape.col_types.float8, 7);
    assert_eq!(gp.table_shape.col_types.int, 1);
    assert_eq!(gp.table_shape.col_types.text, 0, "missing key must weigh 0");
    // Empty map keeps generator defaults.
    p.table_shape.col_types = BTreeMap::new();
    let gp = bridge::runner_profile_to_gen(&p);
    assert_eq!(gp.table_shape.col_types.float8, 0);
    assert!(gp.table_shape.col_types.int > 0);
    assert_eq!(gp.table_shape.rows_max, p.table_shape.rows_max);

    // The checked-in float-lenient profile: q:float-agg is reachable AND
    // emitted (it was vacuous before the bridge fix).
    let lp = simharness::runner::profile::load_profile(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("profiles/float-lenient.json")
            .to_str()
            .unwrap(),
    )
    .unwrap();
    let gp = bridge::runner_profile_to_gen(&lp.profile);
    assert!(gp.table_shape.col_types.float8 > 0);
    let mut acc = prodreg::KpathAccum::default();
    for seed in 5000..5200u64 {
        let (_p, _c, traces) = bridge::generate_plan_with_ctx_traced(seed, &gp, "00", "t");
        acc.add(&traces);
    }
    assert!(
        acc.nodes.contains(prodreg::Q_FLOAT_AGG),
        "q:float-agg never emitted under float-lenient (still vacuous)"
    );
}

// --------------------------------------------------------- validator guards

#[test]
fn h6_profile_validator_rejects_bad_col_types() {
    let mut p = serde_json::from_str::<simharness::runner::profile::Profile>(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json"),
        )
        .unwrap(),
    )
    .unwrap();
    p.table_shape.col_types = BTreeMap::from([("uuid".to_string(), 3u32)]);
    let err = simharness::runner::profile::validate(&p).unwrap_err();
    assert!(err.contains("unknown col type 'uuid'"), "got: {err}");
    p.table_shape.col_types = BTreeMap::from([("int".to_string(), 0u32)]);
    let err = simharness::runner::profile::validate(&p).unwrap_err();
    assert!(err.contains("all weights are zero"), "got: {err}");
}
