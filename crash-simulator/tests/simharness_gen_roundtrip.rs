//! G-G1 (part): plan round-trip property test — parse(render(p)) == p over
//! randomized plans covering the whole IR (incl. reserved tags), plus refusal
//! paths for every reserved tag.

use std::collections::BTreeSet;

use rand::Rng;
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use simharness::plan::{
    parse, property_table_deps, render, ArmCtl, Check, FaultPoint, IsoLevel, Mark, Plan,
    PlanHeader, PlanItem, Sql, SqlFlags, Step, TxCtl,
};

fn rand_mark(rng: &mut ChaCha8Rng) -> Mark {
    match rng.gen_range(0..3) {
        0 => Mark::Read,
        1 => Mark::Mutation,
        _ => Mark::Passthrough,
    }
}

fn rand_sql(rng: &mut ChaCha8Rng) -> Sql {
    let flags = SqlFlags {
        order_underdetermined: rng.gen_bool(0.3),
        float_lenient: rng.gen_bool(0.3),
    };
    let text = format!(
        "SELECT c{} FROM t{} WHERE c{} = {};",
        rng.gen_range(1..9),
        rng.gen_range(1..9),
        rng.gen_range(1..9),
        rng.gen_range(0..100)
    );
    Sql::new(text, rand_mark(rng), flags).unwrap()
}

fn rand_step(rng: &mut ChaCha8Rng) -> Step {
    match rng.gen_range(0..12) {
        0 => Step::Ddl(rand_sql(rng)),
        1 => Step::Dml(rand_sql(rng)),
        2 => Step::Query(rand_sql(rng)),
        3 => Step::Tx(match rng.gen_range(0..5) {
            0 => TxCtl::Begin(match rng.gen_range(0..3) {
                0 => IsoLevel::ReadCommitted,
                1 => IsoLevel::RepeatableRead,
                _ => IsoLevel::Serializable,
            }),
            1 => TxCtl::Commit,
            2 => TxCtl::Rollback,
            3 => TxCtl::Savepoint(format!("sp{}", rng.gen_range(1..99))),
            _ => TxCtl::RollbackTo(format!("sp{}", rng.gen_range(1..99))),
        }),
        4 => Step::Arm(ArmCtl::SetGuc("work_mem".into(), "64kB".into())),
        5 => Step::Arm(ArmCtl::ResetAll),
        6 => Step::Assumption(
            Check::new(format!("{{\"kind\":\"rows-at-least\",\"n\":{}}}", rng.gen_range(0..9)))
                .unwrap(),
        ),
        7 => Step::Assertion(
            Check::new(format!("{{\"kind\":\"multiset-equal\",\"top\":{}}}", rng.gen_range(1..4)))
                .unwrap(),
        ),
        8 => Step::Fault(FaultPoint::Disconnect),
        9 => Step::Fault(FaultPoint::ReconnectServer),
        // Reserved tags must render+parse (execution refusal is the runner's).
        10 => Step::Fault(match rng.gen_range(0..3) {
            0 => FaultPoint::Crash(format!("walwrite-{}", rng.gen_range(0..9))),
            1 => FaultPoint::TornWrite,
            _ => FaultPoint::Env("enospc".into()),
        }),
        _ => Step::Arm(ArmCtl::SetGuc("enable_hashjoin".into(), "off".into())),
    }
}

fn rand_plan(seed: u64) -> Plan {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let header = PlanHeader {
        seed: rng.next_u64(),
        profile: "roundtrip-test".into(),
        profile_sha256: "ab12cd34ef56".into(),
        generator: "deadbeef0123".into(),
    };
    let n = rng.gen_range(0..40);
    let mut items = Vec::new();
    let mut seq = 1u32;
    for _ in 0..n {
        if rng.gen_bool(0.25) {
            let mut tables = BTreeSet::new();
            for _ in 0..rng.gen_range(0..4) {
                tables.insert(format!("t{}", rng.gen_range(1..9)));
            }
            let steps = (0..rng.gen_range(1..6)).map(|_| rand_step(&mut rng)).collect();
            items.push(PlanItem::Property {
                name: ["InsertSelect", "Tlp", "ArmEquivalence"][rng.gen_range(0..3)].to_string(),
                seq,
                tables,
                steps,
            });
            seq += 1;
        } else {
            items.push(PlanItem::Step(rand_step(&mut rng)));
        }
    }
    Plan { header, items }
}

#[test]
fn roundtrip_property_500_seeds() {
    for seed in 0..500u64 {
        let plan = rand_plan(seed);
        let text = render(&plan);
        let parsed = parse(&text).unwrap_or_else(|e| panic!("seed {seed}: parse failed: {e}\n{text}"));
        assert_eq!(parsed, plan, "seed {seed}: round-trip IR mismatch");
        // Byte-idempotence: render(parse(render(p))) == render(p).
        assert_eq!(render(&parsed), text, "seed {seed}: render not byte-stable");
    }
}

#[test]
fn table_dependency_api_reads_property_blocks() {
    let plan = rand_plan(7);
    let deps = property_table_deps(&plan);
    let expected: usize = plan
        .items
        .iter()
        .filter(|i| matches!(i, PlanItem::Property { .. }))
        .count();
    assert_eq!(deps.len(), expected);
    for item in &plan.items {
        if let PlanItem::Property { seq, tables, .. } = item {
            assert_eq!(deps.get(seq), Some(tables));
        }
    }
}

#[test]
fn session_switch_is_reserved_parse_error() {
    let text = "-- simharness plan v1 (serial single-session)\n\
                -- seed: 1 profile: p profile-sha256: ab generator: g\n\
                \n\
                -- SESSION s2\n";
    let err = parse(text).unwrap_err();
    assert!(
        err.msg.contains("reserved: multi-session"),
        "SessionSwitch must be a hard reserved error, got: {err}"
    );
}

#[test]
fn reserved_fault_tags_render_parse_but_refuse_execution() {
    for fp in [
        FaultPoint::Crash("walwrite-3".into()),
        FaultPoint::TornWrite,
        FaultPoint::Env("enospc".into()),
    ] {
        assert!(!fp.executable_v1(), "{fp:?} must refuse v1 execution");
        let plan = Plan {
            header: PlanHeader {
                seed: 1,
                profile: "p".into(),
                profile_sha256: "ab".into(),
                generator: "g".into(),
            },
            items: vec![PlanItem::Step(Step::Fault(fp.clone()))],
        };
        let parsed = parse(&render(&plan)).unwrap();
        assert_eq!(parsed, plan);
    }
    assert!(FaultPoint::Disconnect.executable_v1());
    assert!(FaultPoint::ReconnectServer.executable_v1());
}

#[test]
fn unknown_version_line_is_hard_error() {
    let err = parse("-- simharness plan v2 (something)\n-- seed: 1 ...\n").unwrap_err();
    assert!(err.msg.contains("unsupported plan format version"));
}

#[test]
fn bare_sql_without_annotation_is_error() {
    let text = "-- simharness plan v1 (serial single-session)\n\
                -- seed: 1 profile: p profile-sha256: ab generator: g\n\
                \n\
                SELECT 1;\n";
    let err = parse(text).unwrap_err();
    assert!(err.msg.contains("bare SQL"));
}

#[test]
fn nested_property_blocks_are_error() {
    let text = "-- simharness plan v1 (serial single-session)\n\
                -- seed: 1 profile: p profile-sha256: ab generator: g\n\
                \n\
                -- begin property 'A' seq=1\n\
                -- begin property 'B' seq=2\n\
                -- end property seq=2\n\
                -- end property seq=1\n";
    let err = parse(text).unwrap_err();
    assert!(err.msg.contains("nested property"));
}

#[test]
fn unsorted_tables_list_is_error() {
    let text = "-- simharness plan v1 (serial single-session)\n\
                -- seed: 1 profile: p profile-sha256: ab generator: g\n\
                \n\
                -- begin property 'A' seq=1 tables=t2,t1\n\
                -- end property seq=1\n";
    let err = parse(text).unwrap_err();
    assert!(err.msg.contains("sorted"));
}

#[test]
fn sql_constructor_rejects_malformed_text() {
    assert!(Sql::new("", Mark::Read, SqlFlags::default()).is_err());
    assert!(Sql::new("SELECT 1", Mark::Read, SqlFlags::default()).is_err()); // no ';'
    assert!(Sql::new("SELECT\n1;", Mark::Read, SqlFlags::default()).is_err()); // newline
    assert!(Sql::new("-- nope;", Mark::Read, SqlFlags::default()).is_err()); // comment
    assert!(Check::new("{not json").is_err());
    assert!(Check::new("{\"a\":\n1}").is_err());
}
