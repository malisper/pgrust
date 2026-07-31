//! Unit tier: replay regress-derived (path, doc) vectors through the full
//! differential (both engines + all planes). The CI cluster fuzz campaign is the
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
    println!("corpus replay: {n} seeds, {total} in-domain execs, {carved} datetime-carve hits");
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
