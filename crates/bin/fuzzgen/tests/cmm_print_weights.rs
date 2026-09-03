//! CLI-level checks for the sitediff `cmm:` session co-draw (lane L0.4):
//! `fuzzgen --print-weights` lists the new productions, and a seeded
//! stream with the co-draw pinned exclusive emits the SET plus its
//! log-level companions.

use std::process::Command;

fn fuzzgen(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_fuzzgen"))
        .args(args)
        .output()
        .expect("spawn fuzzgen");
    assert!(
        out.status.success(),
        "fuzzgen {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8 stdout")
}

#[test]
fn print_weights_lists_cmm_productions() {
    let dump = fuzzgen(&["--print-weights"]);
    for name in ["util:cmm", "cfgm:cmm", "cmm:notice", "cmm:log", "cmm:debug1", "cmm:debug2"] {
        assert!(
            dump.lines().any(|l| l.starts_with(&format!("{name}="))),
            "--print-weights lacks {name}"
        );
    }
}

#[test]
fn seeded_stream_contains_cmm_codraw() {
    // util pinned to the cmm shape only; seed fixed -> deterministic stream.
    let sql = fuzzgen(&[
        "--seed", "7", "--count", "2000", "--modules", "util=on:500",
        "--weight",
        "util:set=0,util:reset=0,util:reset_all=0,util:show=0,util:discard:plans=0,\
         util:discard:sequences=0,util:vacuum=0,util:analyze=0,util:checkpoint=0,\
         util:comment:table=0,util:comment:column=0,util:sysview=0,util:prepare=0,util:cmm=1",
    ]);
    let lines: Vec<&str> = sql.lines().collect();
    let sets: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("SET client_min_messages = "))
        .map(|(i, _)| i)
        .collect();
    assert!(!sets.is_empty(), "no cmm SET in the stream:\n{sql}");
    let mut saw_log = false;
    for i in sets {
        let level = lines[i]
            .strip_prefix("SET client_min_messages = ")
            .and_then(|r| r.strip_suffix(';'))
            .unwrap();
        assert!(
            ["notice", "log", "debug1", "debug2"].contains(&level),
            "bad level {level}"
        );
        if level == "log" {
            saw_log = true;
            assert_eq!(lines[i + 1], "SET log_statement = 'all';");
            assert_eq!(lines[i + 2], "SET log_min_duration_statement = 0;");
        }
    }
    assert!(saw_log, "cmm:log never fired in 2000 statements at seed 7");
    // The jsonl mode carries the production names for the witness.
    let jsonl = fuzzgen(&["--seed", "7", "--count", "400", "--format", "jsonl", "--modules", "util=on:500",
                          "--weight", "util:cmm=50"]);
    assert!(jsonl.contains("\"cmm\""), "jsonl productions lack cmm:\n{jsonl}");
}
