//! G-R1: bugbase round-trip — bank -> list -> matching filter -> plan
//! reload; runs.json fields survive; deterministic listing order.

use simharness::runner::bugbase::{BugBase, RunsJson};
use simharness::runner::driver::Signature;
use simharness::runner::planface::*;
use simharness::runner::profile::load_profile;
use simharness::runner::runloop::gen_plan;

fn runs(seed: u64, class: &str, site: &str) -> RunsJson {
    RunsJson {
        seed,
        commit_sha: "testsha".into(),
        timestamp_utc: "epoch:0".into(),
        signature: Signature { class: class.into(), sqlstate: "".into(), site: site.into() },
        class: class.into(),
        sev: "P1".into(),
        detail: "planted".into(),
        step_idx: 3,
        cli: vec!["simharness".into(), "run".into()],
        profile_name: "default".into(),
        profile_sha256: "x".repeat(64),
        replay_attempts: 3,
        replay_refails: 3,
    }
}

#[test]
fn bank_list_reload_round_trip() {
    let tmp = std::env::temp_dir().join(format!("simharness-bb-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let bb = BugBase::new(&tmp);

    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json");
    let lp = load_profile(dir.to_str().unwrap()).unwrap();
    let plan = gen_plan(4242, &lp, "testgen");
    let text = plan.render();
    // A structurally-valid strict prefix: never cut a property block open
    // (H6: the seed-4242 plan shape changed; a fixed [..2] slice landed
    // inside a property, which does not render/parse round-trip).
    let mut cut = 1;
    let mut depth = 0i32;
    for (i, s) in plan.steps.iter().enumerate() {
        match s {
            Step::BeginProperty { .. } => depth += 1,
            Step::EndProperty { .. } => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            cut = i + 1;
            if cut >= 2 && cut < plan.steps.len() {
                break;
            }
        }
    }
    let shrunk = Plan { header: plan.header.clone(), steps: plan.steps[..cut].to_vec() };

    bb.bank(4242, &text, Some(&shrunk.render()), &runs(4242, "property-violation", "SELECT v FROM st_# WHERE k = #"))
        .unwrap();
    bb.bank(7, &text, None, &runs(7, "rust-crash", "INSERT INTO st_#")).unwrap();

    // Listing is seed-sorted (deterministic).
    let entries = bb.entries().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 7);
    assert_eq!(entries[1].0, 4242);
    assert_eq!(entries[1].1.class, "property-violation");
    assert_eq!(entries[1].1.replay_refails, 3);

    // Signature filter.
    let m = bb.matching("rust-crash").unwrap();
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].0, 7);
    assert_eq!(bb.matching("").unwrap().len(), 2);
    assert_eq!(bb.matching("no-such-sig").unwrap().len(), 0);

    // Plans reload byte-identical and re-parse.
    let loaded = bb.load_plan(4242, false).unwrap();
    assert_eq!(loaded, text);
    let re = Plan::parse(&loaded).unwrap();
    assert_eq!(re, plan);
    let loaded_shrunk = bb.load_plan(4242, true).unwrap();
    assert_eq!(Plan::parse(&loaded_shrunk).unwrap(), shrunk);
    // Seed 7 banked without a shrunk plan.
    assert!(bb.load_plan(7, true).is_err());

    let _ = std::fs::remove_dir_all(&tmp);
}
