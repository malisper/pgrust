//! G-O2 (decision-logic leg): Rust classifier vs pinned triage.py on the
//! pinned repro corpora — per-statement screens and the full classify()
//! ladder over the synthetic outcome-pair matrix must agree exactly.
//! Fixtures are triage.py's own answers (tests/parity/gen_fixtures.py,
//! regenerated + drift-checked by tests/parity/run-parity.sh).

use std::path::Path;

use serde_json::Value as J;

use simharness::oracle::classifier::{
    classify, is_nondet, is_volatile, underdetermined, Digest, RunStatus,
};

/// The cross-language outcome vocabulary — must mirror OUTCOMES in
/// gen_fixtures.py exactly.
fn outcome(name: &str) -> RunStatus {
    let ok = |ncols: u32, nrows: i64, h: &str, capped: bool| {
        RunStatus::Ok(Digest {
            ncols,
            nrows,
            norm_hash: h.to_string(),
            capped,
            raw_hash: None,
        })
    };
    let err = |state: Option<&str>, msg: &str| RunStatus::Error {
        sqlstate: state.map(|s| s.to_string()),
        msg: msg.to_string(),
    };
    match name {
        "ok_a" => ok(1, 3, "h1", false),
        "ok_b" => ok(1, 3, "h2", false),
        "ok_capped" => ok(2, -1, "capped", true),
        "err_syntax" => err(Some("42601"), "syntax error at or near \"x\""),
        "err_syntax2" => err(Some("42601"), "syntax error at or near \"y\""),
        "err_div" => err(Some("22012"), "division by zero"),
        "err_undef" => err(Some("42P01"), "relation does not exist"),
        "err_cov" => err(Some("XX000"), "not yet ported: foo"),
        "err_xx_other" => err(Some("XX000"), "weird failure mode"),
        "err_timeout" => err(Some("57014"), "canceling statement due to statement timeout"),
        "err_none" => err(None, "strange"),
        "crash" => RunStatus::Crash { msg: "server closed the connection unexpectedly".into() },
        "fetch" => RunStatus::Fetch { msg: "year 0 is out of range".into() },
        other => panic!("unknown outcome name {other}"),
    }
}

#[test]
fn screens_and_ladder_match_pinned_triage() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/parity/fixtures-screens.jsonl");
    let text = std::fs::read_to_string(&fixtures).unwrap_or_else(|e| {
        panic!(
            "missing {} ({e}); generate via tests/parity/run-parity.sh --refresh",
            fixtures.display()
        )
    });

    let mut stmts = 0usize;
    let mut matrix_checks = 0usize;
    let mut mismatches: Vec<String> = Vec::new();

    for line in text.lines() {
        let rec: J = serde_json::from_str(line).expect("fixture line parses");
        if rec["kind"] == "header" {
            continue;
        }
        let stmt = rec["stmt"].as_str().expect("stmt");
        let i = rec["i"].as_i64().unwrap();
        stmts += 1;

        // Screen parity.
        let want_ud = rec["screens"]["underdetermined"].as_bool().unwrap();
        let want_vol = rec["screens"]["volatile"].as_bool().unwrap();
        let want_nd = rec["screens"]["nondet"].as_bool().unwrap();
        if underdetermined(stmt) != want_ud {
            mismatches.push(format!("stmt {i}: underdetermined {} != {want_ud}", !want_ud));
        }
        if is_volatile(stmt) != want_vol {
            mismatches.push(format!("stmt {i}: volatile {} != {want_vol}", !want_vol));
        }
        if is_nondet(stmt) != want_nd {
            mismatches.push(format!("stmt {i}: nondet {} != {want_nd}", !want_nd));
        }

        // Ladder parity.
        if let Some(matrix) = rec.get("matrix").and_then(|m| m.as_object()) {
            for (pair, want) in matrix {
                let (rn, cn) = pair.split_once('|').expect("pair key");
                let (cls, sev) = classify(stmt, &outcome(rn), &outcome(cn));
                let want_cls = want[0].as_str().unwrap();
                let want_sev = want[1].as_str().unwrap();
                if cls.as_str() != want_cls || sev.as_str() != want_sev {
                    mismatches.push(format!(
                        "stmt {i} pair {pair}: rust {}/{} != triage {}/{}",
                        cls.as_str(),
                        sev.as_str(),
                        want_cls,
                        want_sev
                    ));
                }
                matrix_checks += 1;
            }
        }
    }

    assert!(stmts > 50, "fixture corpus suspiciously small: {stmts}");
    assert!(matrix_checks > 500, "matrix suspiciously small: {matrix_checks}");
    assert!(
        mismatches.is_empty(),
        "G-O2 parity FAIL ({} of {} checks):\n{}",
        mismatches.len(),
        stmts * 3 + matrix_checks,
        mismatches.join("\n")
    );
    println!(
        "G-O2 parity: {stmts} statements x screens + {matrix_checks} ladder cells, all agree"
    );
}
