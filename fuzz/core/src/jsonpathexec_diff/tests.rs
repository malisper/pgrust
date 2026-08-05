//! Unit tier: replay regress-derived (path, doc) vectors through the full
//! differential (both engines + all planes). The fleet fuzz campaign is the
//! volume tier; this tier locks the harness wiring and the charter's named
//! shapes (lax/strict, filters, like_regex flags, arithmetic incl. 22012,
//! .double() edges, .keyvalue(), wildcards, last, slices, variables,
//! silent, @? / @@ NULL semantics, deep nesting).

use super::*;

/// Drive one (arm, silent, tz, vars) cell through the full differential.
fn cell(arm: u8, silent: bool, tz: bool, path: &str, doc: &str, vars: Option<&str>) {
    let mut sel = arm & 0x07;
    if silent {
        sel |= 0x08;
    }
    if tz {
        sel |= 0x10;
    }
    let vars_text = vars.unwrap_or("");
    if vars.is_some() {
        sel |= 0x20;
    }
    let mut data = vec![sel];
    data.extend_from_slice(&(path.len() as u16).to_le_bytes());
    data.extend_from_slice(&(doc.len() as u16).to_le_bytes());
    data.extend_from_slice(path.as_bytes());
    data.extend_from_slice(doc.as_bytes());
    data.extend_from_slice(vars_text.as_bytes());
    jsonpathexec_diff(&data);
}

/// All arms x silent x tz over one (path, doc, vars) triple.
fn all_arms(path: &str, doc: &str, vars: Option<&str>) {
    for arm in 0..7u8 {
        for silent in [false, true] {
            for tz in [false, true] {
                cell(arm, silent, tz, path, doc, vars);
            }
        }
    }
}

const DOCS: &[&str] = &[
    "null",
    "true",
    "1",
    "-2.5",
    "\"abc\"",
    "[]",
    "{}",
    "[1, 2, 3, 4, 5]",
    "[1, \"2\", {}, [3], null, true]",
    "{\"a\": 10}",
    "{\"a\": {\"b\": {\"c\": 1}}}",
    "{\"a\": [1, 2, 3], \"b\": [4, 5]}",
    "[{\"a\": 1}, {\"a\": 2}, {\"a\": \"x\"}, {\"b\": 3}]",
    "{\"g\": {\"x\": 2, \"y\": [1, 2, 3]}}",
    "[\"string\", \"str\", \"s\", \"\"]",
    "[0.1, 1e10, -1e-10, 123456789012345678901234567890]",
    "{\"a\": \"abc def\", \"b\": \"abdef\", \"c\": \"aBdEf\"}",
    "[[1, [2, 3]], [[4], 5]]",
    "{\"key\": 1, \"another key\": [2], \"\": null}",
    "[\"1.0\", \"nan\", \"NaN\", \"inf\", \"-Infinity\", \"1e1000\", \"x\"]",
];

const PATHS: &[&str] = &[
    "$",
    "strict $",
    "lax $",
    "$.a",
    "strict $.a",
    "$.a.b.c",
    "$.*",
    "$[*]",
    "strict $[*]",
    "$[0]",
    "$[last]",
    "$[0 to 2]",
    "$[last to 1]",
    "$[1, 0 to last]",
    "$[5]",
    "strict $[5]",
    "$[-1]",
    "$.a[*]",
    "$.**",
    "$.**{2}",
    "$.**{0 to last}.b",
    "$.a + 1",
    "$[0] + $[1]",
    "1 / $[0]",
    "$[0] / 0",
    "$[1] % 2",
    "-$[*]",
    "+$.a",
    "$.a * 2 - 1",
    "$ ? (@ > 1)",
    "$ ? (@.a == 1)",
    "strict $ ? (@.a == 1)",
    "$ ? (@.a == 1 || @.b == 3)",
    "$ ? (@.a == 1 && @.b == 3)",
    "$ ? (!(@.a == 1))",
    "$ ? ((@.a > 0) is unknown)",
    "$ ? (exists (@.a))",
    "$ ? (@.a like_regex \"^a.c$\")",
    "$ ? (@ like_regex \"^ab.*f\" flag \"i\")",
    "$ ? (@ like_regex \"a b\" flag \"x\")",
    "$ ? (@ like_regex \"a.c\" flag \"q\")",
    "$ ? (@ like_regex \"^s\" flag \"m\")",
    "$ ? (@ like_regex \".\" flag \"s\")",
    "$ ? (@ starts with \"str\")",
    "$ ? (@ starts with $prefix)",
    "$.type()",
    "$[*].type()",
    "$.size()",
    "$.double()",
    "$[*].double()",
    "$.abs()",
    "$[*].floor()",
    "$[*].ceiling()",
    "$.keyvalue()",
    "$.keyvalue().key",
    "$.keyvalue().value",
    "$.keyvalue().id",
    "$.bigint()",
    "$[*].bigint()",
    "$.integer()",
    "$.number()",
    "$.decimal()",
    "$.decimal(5)",
    "$.decimal(5, 2)",
    "$.decimal(1000, -5)",
    "$.decimal(100000, 0)",
    "$.string()",
    "$[*].string()",
    "$.boolean()",
    "$[*].boolean()",
    "$a",
    "$a + $b",
    "$ ? (@.a == $x)",
    "$undefined",
    "$.a[$idx]",
    "last",
    "$.a ? (@ > $limit)",
];

#[test]
fn regress_matrix_all_arms() {
    // A broad (path x doc) product through every arm; vars models both
    // absent and present-with-typical-bindings.
    for path in PATHS {
        for doc in DOCS.iter().take(8) {
            all_arms(path, doc, None);
        }
        all_arms(
            path,
            DOCS[7],
            Some("{\"a\": 1, \"b\": 2, \"x\": 1, \"idx\": 0, \"limit\": 2, \"prefix\": \"str\"}"),
        );
        all_arms(path, DOCS[12], None);
        all_arms(path, DOCS[19], None);
    }
}

#[test]
fn error_shapes() {
    // Structural errors (strict), singleton requirements, div-by-zero
    // (22012), numeric conversion errors, variable errors (present /
    // missing / non-object vars), silent suppression on all of them.
    let cells: &[(&str, &str, Option<&str>)] = &[
        ("strict $.a", "{\"b\": 1}", None),
        ("strict $[0]", "{}", None),
        ("strict $.*", "[]", None),
        ("$[0] / 0", "[1, 0]", None),
        ("$[0] / $[1]", "[1, 0]", None),
        ("$[0] % 0", "[3]", None),
        ("$.a + $.b", "{\"a\": [1, 2], \"b\": 1}", None),
        ("$.double()", "[\"x\"]", None),
        ("$.double()", "[\"inf\"]", None),
        ("$.double()", "[\"NaN\"]", None),
        ("$.double()", "[\"1e400\"]", None),
        ("$.bigint()", "[\"9223372036854775808\"]", None),
        ("$.integer()", "[\"2147483648\"]", None),
        ("$.decimal(0)", "[1]", None),
        ("$.decimal(1001)", "[1]", None),
        ("$.decimal(5, 2)", "[\"1234.567\"]", None),
        ("$.number()", "[\"bad\"]", None),
        ("$.boolean()", "[\"maybe\"]", None),
        ("$.boolean()", "[2]", None),
        ("$.keyvalue()", "[1]", None),
        ("$.size()", "\"abc\"", None),
        ("$x", "1", None),
        ("$x", "1", Some("{\"y\": 1}")),
        ("$x", "1", Some("[1]")),
        ("$x", "1", Some("{\"x\": {\"a\": 1}}")),
        ("$x", "1", Some("{\"x\": null}")),
        ("$ ? (@ like_regex \"(\")", "\"a\"", None),
        ("strict $.a.b.c", "{\"a\": 1}", None),
        ("$[10000000000000000]", "[1]", None),
        ("$[1 to 0]", "[1, 2]", None),
        ("strict $[1 to 0]", "[1, 2]", None),
    ];
    for (path, doc, vars) in cells {
        all_arms(path, doc, *vars);
    }
}

#[test]
fn match_null_semantics() {
    // @@ / jsonb_path_match: non-boolean or multi-result -> NULL (silent)
    // or 22038-style errors; exercise both operators' NULL planes.
    for (path, doc) in [
        ("$[*] > 1", "[1, 2, 3]"),
        ("$[*]", "[true]"),
        ("$[*]", "[1]"),
        ("$[*]", "[true, false]"),
        ("$.a", "{\"a\": null}"),
        ("$.missing", "{}"),
    ] {
        all_arms(path, doc, None);
    }
}

#[test]
fn datetime_carve_filter() {
    // Carved paths must be skipped BEFORE either engine runs (the C
    // sentinel would abort the process if one leaked through); keys named
    // like the methods stay in-domain.
    // Walker verdicts checked directly on parsed images (the global carve
    // counters are shared across parallel tests, so they are not asserted
    // on here); the full-differential replay below additionally proves no
    // carved input reaches the C sentinel (which would abort the process).
    let cx = mcx::MemoryContext::new("carve_test");
    let m = cx.mcx();
    let parsed = |p: &str| {
        adt_jsonpath::path::jsonpath_in(m, p.as_bytes(), None)
            .expect("parses")
            .expect("hard mode")
    };
    for path in [
        "$.datetime()",
        "$.datetime(\"HH24:MI\")",
        "$.date()",
        "$.time()",
        "$.time(2)",
        "$.time_tz()",
        "$.timestamp()",
        "$.timestamp_tz()",
        "$ ? (@.datetime() > @.datetime())",
        "$[*] ? (exists (@.time()))",
        "$.a.date().type()",
        "$.a[$.b.time()]",
        "$ ? (@ == 1 || @.timestamp() > 1)",
        "-$.date()",
    ] {
        assert!(
            path_has_datetime_item(&parsed(path)),
            "walker must carve {path:?}"
        );
        all_arms(path, "\"2023-01-01\"", None);
    }
    // keys named after the methods are NOT carved (walker, not text scan)
    for (path, doc) in [
        ("$.datetime", "{\"datetime\": 1}"),
        ("$.\"date()\"", "{\"date()\": 2}"),
        ("$ ? (@.a starts with \"datetime(\")", "{\"a\": \"datetime(x\"}"),
        ("\"timestamp()\"", "null"),
    ] {
        assert!(
            !path_has_datetime_item(&parsed(path)),
            "walker must NOT carve {path:?}"
        );
        all_arms(path, doc, None);
    }
}

#[test]
fn deep_nesting_within_cap() {
    for depth in [4usize, 16, 40] {
        let path = format!("${}", ".a".repeat(depth));
        let mut doc = String::from("1");
        for _ in 0..depth {
            doc = format!("{{\"a\": {doc}}}");
        }
        if path.len() <= MAX_PATH && doc.len() <= MAX_DOC {
            all_arms(&path, &doc, None);
        }
        let filt = format!("$ {}", "? (@.a > 0) ".repeat(depth));
        if filt.len() <= MAX_PATH {
            all_arms(&filt, "{\"a\": 1}", None);
        }
    }
}

/// Replay every checked-in seed (catches shim/link drift before the
/// campaign; also measures the corpus carve hit-rate printed on demand).
#[test]
fn seed_corpus_replays_clean() {
    // The campaign's deep-nesting seeds sit just below the driver's input
    // caps, which are tuned against libFuzzer's 8 MiB main-thread stack;
    // debug-build frames on the (smaller) libtest thread need explicit
    // headroom, so replay on a thread that matches that reality + margin.
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(|| {
            let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/jsonpathexec_diff");
            let mut n = 0;
            for e in std::fs::read_dir(dir).expect("corpus/jsonpathexec_diff missing") {
                let p = e.unwrap().path();
                if p.is_file() {
                    jsonpathexec_diff(&std::fs::read(&p).unwrap());
                    n += 1;
                }
            }
            assert!(n >= 30, "expected >=30 seeds, found {n}");
            let total = EXEC_TOTAL.load(Ordering::Relaxed);
            let carved = CARVE_HITS.load(Ordering::Relaxed);
            println!(
                "corpus replay: {n} seeds, {total} in-domain execs, {carved} datetime-carve hits"
            );
        })
        .expect("spawn replay thread")
        .join()
        .expect("replay thread panicked");
}

/// Task-B recursion probe: with the server's stack guard ARMED on this
/// thread (set_stack_base + the default max_stack_depth), no jsonpath_exec
/// recursion shape may abort the process — deep docs driven through the
/// crate's recursive walkers (.** = execute_any_item, .keyvalue() =
/// build_value_from_container, plain exec = execute_item) must come back as
/// Ok or a clean PgError (54001 once the guard engages). The fuzz driver
/// itself carves this plane via input caps (the C oracle has no armed
/// guard); this probe checks the shipped crate's own guards fire.
#[test]
fn recursion_guard_probe() {
    std::thread::Builder::new()
        .stack_size(32 << 20)
        .spawn(|| {
            setup();
            let _base = stack_depth::set_stack_base();
            let mut guard_fired_in_exec = false;
            for n in [16usize, 128, 1024, 8192, 65536] {
                for (open, close) in [("[", "]"), ("{\"a\":", "}")] {
                    let doc = format!("{}1{}", open.repeat(n), close.repeat(n));
                    let cx = mcx::MemoryContext::new("recursion_probe");
                    let m = cx.mcx();
                    // Asymmetric budgets make the probe frame-size-independent
                    // (stack-guard-bounds-in-bytes law: a ladder that needs
                    // exec frames to outweigh parse frames is alive in debug
                    // and dead in release — the fleet rail baseline and any
                    // macOS --release run hit exactly that). PARSE under the
                    // production 2048kB budget so deep docs survive to exec;
                    // EXEC under the 100kB GUC floor so its guard must fire
                    // on any platform once the doc out-recurses ~100kB.
                    // assign_* is the setter the guard actually reads
                    // (MAX_STACK_DEPTH_BYTES); set_* alone only changes the
                    // kB value echoed in the errhint.
                    stack_depth_core::set_max_stack_depth(2048);
                    stack_depth_core::assign_max_stack_depth(2048);
                    let parsed = adt_jsonb::io::jsonb_in(m, doc.as_bytes(), None);
                    stack_depth_core::set_max_stack_depth(100);
                    stack_depth_core::assign_max_stack_depth(100);
                    let Ok(Some(doc_image)) = parsed else {
                        // Doc-parse guard (adt_jsonb's plane) bounded the
                        // input first; exec can never see a deeper doc.
                        continue;
                    };
                    for path in ["$.**.size()", "$.keyvalue()", "strict $.**{last}", "$"] {
                        let p = adt_jsonpath::path::jsonpath_in(m, path.as_bytes(), None)
                            .expect("shallow path parses")
                            .expect("non-null");
                        let r = adt_jsonpath_exec::jsonb_path_query_core(
                            m,
                            &doc_image[4..],
                            &p[..],
                            adt_jsonpath_exec::JsonPathVars::None,
                            false,
                            false,
                        );
                        // Ok and clean domain errors (e.g. keyvalue-on-array)
                        // are both fine; what must never happen is a process
                        // abort (the join() below would see it). Record when
                        // the stack guard itself is the error source.
                        if let Err(e) = r {
                            if e.sqlstate().0 == types_error::ERRCODE_STATEMENT_TOO_COMPLEX.0 {
                                guard_fired_in_exec = true;
                            }
                        }
                    }
                }
            }
            assert!(
                guard_fired_in_exec,
                "probe never engaged the exec stack guard — deepen the ladder"
            );
        })
        .expect("spawn probe thread")
        .join()
        .expect("probe thread panicked (process-abort class recursion defect)");
}

/// Witness pairs (single-dimension deltas — seeding obligation): the same
/// cells are also committed as corpus seeds by gen_seeds_jsonpathexec.py;
/// this test drives the generator's base set through the differential.
#[test]
fn witness_pairs() {
    let base_doc = "{\"a\": [1, 2, 3], \"b\": \"str\"}";
    // same doc, path differing by one array index
    for p in ["$.a[0]", "$.a[1]", "$.a[2]", "$.a[3]"] {
        all_arms(p, base_doc, None);
    }
    // same path, doc differing in one leaf
    for d in [
        "{\"a\": [1, 2, 3], \"b\": \"str\"}",
        "{\"a\": [1, 2, 4], \"b\": \"str\"}",
        "{\"a\": [1, 2, 3], \"b\": \"st\"}",
        "{\"a\": [1, 2, 3], \"b\": null}",
    ] {
        all_arms("$.a[2]", d, None);
        all_arms("$.b", d, None);
    }
    // silent flipped is inside all_arms; lax vs strict prefix:
    for p in ["$.c", "lax $.c", "strict $.c"] {
        all_arms(p, base_doc, None);
    }
    // vars present vs absent
    for v in [None, Some("{\"x\": 2}"), Some("{\"x\": 3}"), Some("{}")] {
        all_arms("$.a[*] ? (@ >= $x)", base_doc, v);
    }
}
