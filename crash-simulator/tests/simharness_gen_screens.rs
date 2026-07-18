//! G-G1 (part): screen unit tests — R2 / R3R6 / R7, each with planted-red
//! cases (contract §2.3).

use simharness::gen::screens::lint_plan;
use simharness::plan::{Mark, Plan, PlanHeader, PlanItem, Sql, SqlFlags, Step};

fn plan_with(steps: Vec<Step>) -> Plan {
    Plan {
        header: PlanHeader {
            seed: 1,
            profile: "screens-test".into(),
            profile_sha256: "ab".into(),
            generator: "g".into(),
        },
        items: steps.into_iter().map(PlanItem::Step).collect(),
    }
}

fn query(text: &str, flags: SqlFlags) -> Step {
    Step::Query(Sql::new(text, Mark::Read, flags).unwrap())
}

fn ddl(text: &str) -> Step {
    Step::Ddl(Sql::new(text, Mark::Mutation, SqlFlags::default()).unwrap())
}

// ---------------------------------------------------------------------- R2

#[test]
fn r2_red_limit_without_order_by() {
    let p = plan_with(vec![query("SELECT c1 FROM t1 LIMIT 3;", SqlFlags::default())]);
    let v = lint_plan(&p);
    assert!(v.iter().any(|v| v.rule == "R2"), "planted LIMIT-without-ORDER-BY must fire: {v:?}");
}

#[test]
fn r2_red_subquery_limit_at_different_depth() {
    // ORDER BY at depth 0 does NOT excuse a LIMIT at depth 1 (same-depth law).
    let p = plan_with(vec![query(
        "SELECT id, (SELECT c1 FROM t2 LIMIT 1) FROM t1 ORDER BY id;",
        SqlFlags::default(),
    )]);
    let v = lint_plan(&p);
    assert!(v.iter().any(|v| v.rule == "R2"), "subquery LIMIT without same-depth ORDER BY: {v:?}");
}

#[test]
fn r2_green_limit_with_same_depth_order_by() {
    let p = plan_with(vec![
        query("SELECT id FROM t1 ORDER BY id LIMIT 3;", SqlFlags::default()),
        query("SELECT id FROM t1 ORDER BY id LIMIT 3 OFFSET 2;", SqlFlags::default()),
        query(
            "SELECT id, (SELECT c1 FROM t2 ORDER BY c1 LIMIT 1) FROM t1 ORDER BY id;",
            SqlFlags::default(),
        ),
    ]);
    assert!(lint_plan(&p).is_empty(), "{:?}", lint_plan(&p));
}

#[test]
fn r2_green_order_underdetermined_flag_excuses() {
    let p = plan_with(vec![query(
        "SELECT c1 FROM t1 LIMIT 3;",
        SqlFlags { order_underdetermined: true, float_lenient: false },
    )]);
    assert!(lint_plan(&p).is_empty());
}

#[test]
fn r2_green_limit_inside_string_literal_ignored() {
    let p = plan_with(vec![query("SELECT c1 FROM t1 WHERE c1 = 'LIMIT 3';", SqlFlags::default())]);
    assert!(lint_plan(&p).is_empty(), "quote-aware scan must ignore literals");
}

// ------------------------------------------------------------------- R3/R6

#[test]
fn r3_red_volatile_function() {
    let p = plan_with(vec![query("SELECT random() FROM t1;", SqlFlags::default())]);
    assert!(lint_plan(&p).iter().any(|v| v.rule == "R3R6"));
}

#[test]
fn r6_red_metadata_function() {
    let p = plan_with(vec![query("SELECT version();", SqlFlags::default())]);
    assert!(lint_plan(&p).iter().any(|v| v.rule == "R3R6"));
}

#[test]
fn r3_red_lo_creat_lesson() {
    // The two functions that each cost an adjudication round (spec §3.2.3).
    let p = plan_with(vec![query("SELECT lo_creat(-1) FROM t1;", SqlFlags::default())]);
    assert!(lint_plan(&p).iter().any(|v| v.rule == "R3R6"));
}

#[test]
fn r3_green_plain_query_and_word_boundary() {
    let p = plan_with(vec![
        query("SELECT c1 FROM t1;", SqlFlags::default()),
        // 'nowhere' contains 'now' but is not a call of now().
        query("SELECT c1 FROM t1 WHERE c1 = 'nowhere';", SqlFlags::default()),
        // column named version_c1 is not version().
        query("SELECT version_c1 FROM t1;", SqlFlags::default()),
    ]);
    assert!(lint_plan(&p).is_empty(), "{:?}", lint_plan(&p));
}

// ---------------------------------------------------------------------- R7

fn float_table() -> Step {
    ddl("CREATE TABLE t1 (id bigint PRIMARY KEY, c1 float8, c2 int);")
}

#[test]
fn r7_red_float_sum_without_tag() {
    let p = plan_with(vec![float_table(), query("SELECT sum(c1) FROM t1;", SqlFlags::default())]);
    assert!(lint_plan(&p).iter().any(|v| v.rule == "R7"), "{:?}", lint_plan(&p));
}

#[test]
fn r7_red_explicit_float8_cast() {
    let p = plan_with(vec![float_table(), query("SELECT avg(c2::float8) FROM t1;", SqlFlags::default())]);
    assert!(lint_plan(&p).iter().any(|v| v.rule == "R7"));
}

#[test]
fn r7_green_float_sum_with_tag() {
    let p = plan_with(vec![
        float_table(),
        query(
            "SELECT sum(c1) FROM t1;",
            SqlFlags { order_underdetermined: false, float_lenient: true },
        ),
    ]);
    assert!(lint_plan(&p).is_empty());
}

#[test]
fn r7_green_exact_type_aggregate() {
    let p = plan_with(vec![float_table(), query("SELECT sum(c2) FROM t1;", SqlFlags::default())]);
    assert!(lint_plan(&p).is_empty());
}

#[test]
fn r7_green_order_insensitive_max_over_float() {
    let p = plan_with(vec![float_table(), query("SELECT max(c1) FROM t1;", SqlFlags::default())]);
    assert!(lint_plan(&p).is_empty(), "max/min are order-insensitive, not R7 targets");
}

#[test]
fn r7_rename_chased() {
    // Rename must not lose the column-type knowledge.
    let p = plan_with(vec![
        float_table(),
        ddl("ALTER TABLE t1 RENAME TO t1_r1;"),
        query("SELECT sum(c1) FROM t1_r1;", SqlFlags::default()),
    ]);
    assert!(lint_plan(&p).iter().any(|v| v.rule == "R7"));
}
