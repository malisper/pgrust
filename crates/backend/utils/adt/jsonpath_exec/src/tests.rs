//! Executor vectors seeded from C 18.3 regress expected/jsonb_jsonpath.out;
//! the byte-identical matrix vs live C runs on the fleet e2e harness.

use mcx::MemoryContext;

use crate::{
    jsonb_path_exists_core, jsonb_path_match_core, jsonb_path_query_array_core,
    jsonb_path_query_core, jsonb_path_query_first_core, JsonPathVars,
};

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // SAFETY: single-threaded test init, before any getenv.
        unsafe { std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo") };
        let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
        mbutils::init_seams();
        pgtz::init_seams();
        adt_timestamp::init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        postgres_seams::check_for_interrupts::set(|| Ok(()));
    });
    adt_datetime::tz::pg_timezone_initialize();
    let z = adt_datetime::tz::pg_tzset(b"GMT").expect("zone loads");
    adt_datetime::tz::set_session_timezone(Some(z));
}

fn jb_payload<'mcx>(mcx: mcx::Mcx<'mcx>, json: &str) -> mcx::PgVec<'mcx, u8> {
    adt_jsonb::io::jsonb_in(mcx, json.as_bytes(), None)
        .unwrap_or_else(|e| panic!("jsonb_in({json:?}): {}", e.message()))
        .expect("hard path returns Some")
}

fn jp_image<'mcx>(mcx: mcx::Mcx<'mcx>, path: &str) -> mcx::PgVec<'mcx, u8> {
    adt_jsonpath::path::jsonpath_in(mcx, path.as_bytes(), None)
        .unwrap_or_else(|e| panic!("jsonpath_in({path:?}): {}", e.message()))
        .expect("hard path returns Some")
}

fn out(mcx: mcx::Mcx<'_>, image_payload: &[u8]) -> String {
    let v = adt_jsonb::io::jsonb_out(mcx, image_payload).expect("jsonb_out");
    String::from_utf8(v[..v.len() - 1].to_vec()).expect("utf8")
}

fn query(json: &str, path: &str, silent: bool, tz: bool) -> Result<Vec<String>, String> {
    setup();
    let cx = MemoryContext::new("jsonpath exec test");
    let mcx = cx.mcx();
    let jb = jb_payload(mcx, json);
    let jp = jp_image(mcx, path);
    match jsonb_path_query_core(mcx, &jb[4..], &jp, JsonPathVars::None, silent, tz) {
        Ok(rows) => Ok(rows.iter().map(|img| out(mcx, &img[4..])).collect()),
        Err(e) => Err(e.message().to_string()),
    }
}

fn q(json: &str, path: &str) -> Vec<String> {
    query(json, path, false, false).unwrap_or_else(|e| panic!("{json} @ {path}: {e}"))
}

fn q_err(json: &str, path: &str) -> String {
    match query(json, path, false, false) {
        Err(e) => e,
        Ok(rows) => panic!("{json} @ {path}: expected error, got {rows:?}"),
    }
}

fn q_tz(json: &str, path: &str) -> Vec<String> {
    query(json, path, false, true).unwrap_or_else(|e| panic!("{json} @ {path}: {e}"))
}

fn exists(json: &str, path: &str) -> Option<bool> {
    setup();
    let cx = MemoryContext::new("jsonpath exec test");
    let mcx = cx.mcx();
    let jb = jb_payload(mcx, json);
    let jp = jp_image(mcx, path);
    jsonb_path_exists_core(mcx, &jb[4..], &jp, JsonPathVars::None, true, false)
        .expect("silent exists never errors")
}

fn exists_vars(json: &str, path: &str, vars: &str) -> Option<bool> {
    setup();
    let cx = MemoryContext::new("jsonpath exec test");
    let mcx = cx.mcx();
    let jb = jb_payload(mcx, json);
    let jp = jp_image(mcx, path);
    let vars_jb = jb_payload(mcx, vars);
    jsonb_path_exists_core(mcx, &jb[4..], &jp, JsonPathVars::Jsonb(&vars_jb[4..]), true, false)
        .expect("silent exists never errors")
}

fn matches(json: &str, path: &str, silent: bool) -> Result<Option<bool>, String> {
    setup();
    let cx = MemoryContext::new("jsonpath exec test");
    let mcx = cx.mcx();
    let jb = jb_payload(mcx, json);
    let jp = jp_image(mcx, path);
    jsonb_path_match_core(mcx, &jb[4..], &jp, JsonPathVars::None, silent, false)
        .map_err(|e| e.message().to_string())
}

fn query_array(json: &str, path: &str) -> String {
    setup();
    let cx = MemoryContext::new("jsonpath exec test");
    let mcx = cx.mcx();
    let jb = jb_payload(mcx, json);
    let jp = jp_image(mcx, path);
    let img =
        jsonb_path_query_array_core(mcx, &jb[4..], &jp, JsonPathVars::None, true, false).unwrap();
    out(mcx, &img[4..])
}

fn query_first(json: &str, path: &str) -> Option<String> {
    setup();
    let cx = MemoryContext::new("jsonpath exec test");
    let mcx = cx.mcx();
    let jb = jb_payload(mcx, json);
    let jp = jp_image(mcx, path);
    jsonb_path_query_first_core(mcx, &jb[4..], &jp, JsonPathVars::None, true, false)
        .unwrap()
        .map(|img| out(mcx, &img[4..]))
}

#[test]
fn accessors_and_wildcards() {
    assert_eq!(q("{\"a\": 12}", "$.a"), ["12"]);
    assert_eq!(q("{\"a\": 12}", "$"), ["{\"a\": 12}"]);
    assert_eq!(q("[1, 2, 3]", "$[*]"), ["1", "2", "3"]);
    assert_eq!(q("[1, 2, 3]", "$[1]"), ["2"]);
    assert_eq!(q("[1, 2, 3]", "$[1 to 2]"), ["2", "3"]);
    assert_eq!(q("[1, 2, 3]", "$[last]"), ["3"]);
    assert_eq!(
        q("{\"a\": {\"b\": 1, \"c\": 2}}", "$.a.*"),
        ["1", "2"]
    );
    assert_eq!(exists("{\"a\": 12}", "$.b"), Some(false));
    assert_eq!(exists("{\"a\": 12}", "$.a"), Some(true));
    assert_eq!(exists("{\"a\": {\"b\": 12}}", "$.a.b"), Some(true));
    // lax auto-unwrap on member access over arrays
    assert_eq!(q("[{\"a\": 1}, {\"a\": 2}]", "$[*].a"), ["1", "2"]);
    assert_eq!(q("[{\"a\": 1}, {\"a\": 2}]", "$.a"), ["1", "2"]);
}

#[test]
fn strict_mode_structural_errors() {
    assert_eq!(
        q_err("{\"a\": 12}", "strict $.b"),
        "JSON object does not contain key \"b\""
    );
    assert_eq!(
        q_err("[1, 2, 3]", "strict $.a"),
        "jsonpath member accessor can only be applied to an object"
    );
    assert_eq!(
        q_err("{\"a\": 12}", "strict $[0]"),
        "jsonpath array accessor can only be applied to an array"
    );
    assert_eq!(
        q_err("[1, 2, 3]", "strict $[4]"),
        "jsonpath array subscript is out of bounds"
    );
    assert_eq!(
        q_err("{\"a\": 12}", "strict $.a[*]"),
        "jsonpath wildcard array accessor can only be applied to an array"
    );
    // lax swallows the same shapes
    assert_eq!(q("{\"a\": 12}", "$.b"), Vec::<String>::new());
    assert_eq!(q("[1, 2, 3]", "$[4]"), Vec::<String>::new());
    // lax auto-wraps for subscript 0
    assert_eq!(q("{\"a\": 12}", "$[0]"), ["{\"a\": 12}"]);
    assert_eq!(q("{\"a\": 12}", "$[0].a"), ["12"]);
}

#[test]
fn any_recursive_descent() {
    let doc = "{\"a\": {\"b\": [1, 2], \"c\": {\"d\": 3}}}";
    assert_eq!(
        q(doc, "$.**"),
        [
            "{\"a\": {\"b\": [1, 2], \"c\": {\"d\": 3}}}",
            "{\"b\": [1, 2], \"c\": {\"d\": 3}}",
            "[1, 2]",
            "1",
            "2",
            "{\"d\": 3}",
            "3",
        ]
    );
    assert_eq!(q(doc, "$.**{2}"), ["[1, 2]", "{\"d\": 3}"]);
    assert_eq!(q(doc, "$.**{2 to last}"), ["[1, 2]", "1", "2", "{\"d\": 3}", "3"]);
}

#[test]
fn filters_and_three_valued_logic() {
    let doc = "[{\"a\": 1}, {\"a\": 2}, {\"a\": 3}]";
    assert_eq!(q(doc, "$[*] ? (@.a > 1)"), ["{\"a\": 2}", "{\"a\": 3}"]);
    assert_eq!(q(doc, "$[*] ? (@.a == 2).a"), ["2"]);
    assert_eq!(
        q("[1, \"2\", null]", "$[*] ? (@ == null)"),
        ["null"]
    );
    // unknown from mixed-type comparison is not an error, just filtered out
    assert_eq!(q("[1, \"a\"]", "$[*] ? (@ > 0)"), ["1"]);
    assert_eq!(
        q("[1, \"a\"]", "$[*] ? ((@ > 0) is unknown)"),
        ["\"a\""]
    );
    assert_eq!(
        q("[1, 2, 3]", "$[*] ? (@ > 1 && @ < 3)"),
        ["2"]
    );
    assert_eq!(
        q("[1, 2, 3]", "$[*] ? (@ == 1 || @ == 3)"),
        ["1", "3"]
    );
    assert_eq!(q("[1, 2, 3]", "$[*] ? (!(@ == 2))"), ["1", "3"]);
    assert_eq!(
        q("{\"a\": [1, 2, 3]}", "$ ? (exists (@.a[*] ? (@ > 2)))"),
        ["{\"a\": [1, 2, 3]}"]
    );
}

#[test]
fn string_predicates() {
    assert_eq!(
        q("[\"abc\", \"abd\", \"xbc\"]", "$[*] ? (@ starts with \"ab\")"),
        ["\"abc\"", "\"abd\""]
    );
    // like_regex resolves the DEFAULT collation via pg_locale
    // (init_database_collation needs a booted catalog); covered by the fleet
    // regress/e2e gates.
}

#[test]
fn arithmetic() {
    assert_eq!(q("[2]", "$[0] + 3"), ["5"]);
    assert_eq!(q("[2]", "-$[0]"), ["-2"]);
    assert_eq!(q("[2.5, 3.5]", "$[0] * $[1]"), ["8.75"]);
    assert_eq!(q("[10, 3]", "$[0] % $[1]"), ["1"]);
    assert_eq!(q("[10, 4]", "$[0] / $[1]"), ["2.5000000000000000"]);
    assert_eq!(q("[1, 2, 3]", "-$[*]"), ["-1", "-2", "-3"]);
    assert_eq!(q_err("[1, 0]", "$[0] / $[1]"), "division by zero");
    // silent mode suppresses the arithmetic error
    assert_eq!(
        query("[1, 0]", "$[0] / $[1]", true, false).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        q_err("[\"a\", 1]", "$[0] + $[1]"),
        "left operand of jsonpath operator + is not a single numeric value"
    );
}

#[test]
fn item_methods() {
    assert_eq!(q("[-1.5, 2.3]", "$[*].abs()"), ["1.5", "2.3"]);
    assert_eq!(q("[-1.5, 2.3]", "$[*].floor()"), ["-2", "2"]);
    assert_eq!(q("[-1.5, 2.3]", "$[*].ceiling()"), ["-1", "3"]);
    assert_eq!(q("[1, \"2\", {}]", "$[*].type()"), ["\"number\"", "\"string\"", "\"object\""]);
    assert_eq!(q("[1, 2, 3]", "$.size()"), ["3"]);
    assert_eq!(q("{\"a\": 1}", "$.size()"), ["1"]);
    assert_eq!(q("[\"1.5\", 2]", "$[*].double()"), ["1.5", "2"]);
    assert_eq!(
        q_err("[\"err\"]", "$[0].double()"),
        "argument \"err\" of jsonpath item method .double() is invalid for type double precision"
    );
    assert_eq!(q("[\"123\", 456]", "$[*].bigint()"), ["123", "456"]);
    assert_eq!(q("[\"12\", 34.0]", "$[*].integer()"), ["12", "34"]);
    assert_eq!(
        q("[\"12.34\", 56]", "$[*].number()"),
        ["12.34", "56"]
    );
    assert_eq!(q("[\"12.345\"]", "$[0].decimal(5, 2)"), ["12.35"]);
    assert_eq!(
        q("[\"true\", \"false\", 1, 0, true]", "$[*].boolean()"),
        ["true", "false", "true", "false", "true"]
    );
    assert_eq!(
        q("[1.23, \"xyz\", false]", "$[*].string()"),
        ["\"1.23\"", "\"xyz\"", "\"false\""]
    );
    assert_eq!(q("[12]", "$[0].string().double()"), ["12"]);
}

#[test]
fn keyvalue_method() {
    assert_eq!(
        q("{\"a\": 1, \"b\": [1, 2]}", "$.keyvalue()"),
        [
            "{\"id\": 0, \"key\": \"a\", \"value\": 1}",
            "{\"id\": 0, \"key\": \"b\", \"value\": [1, 2]}",
        ]
    );
    assert_eq!(
        q("{\"a\": 1}", "$.keyvalue().key"),
        ["\"a\""]
    );
    assert_eq!(
        q_err("[1]", "strict $.keyvalue()"),
        "jsonpath item method .keyvalue() can only be applied to an object"
    );
}

#[test]
fn datetime_methods() {
    assert_eq!(
        q("[\"2023-08-15\"]", "$[0].datetime()"),
        ["\"2023-08-15\""]
    );
    assert_eq!(
        q("[\"2023-08-15\"]", "$[0].date()"),
        ["\"2023-08-15\""]
    );
    assert_eq!(
        q("[\"12:34:56\"]", "$[0].time()"),
        ["\"12:34:56\""]
    );
    assert_eq!(
        q("[\"2023-08-15 12:34:56\"]", "$[0].timestamp()"),
        ["\"2023-08-15T12:34:56\""]
    );
    assert_eq!(
        q("[\"2023-08-15 12:34:56+05:30\"]", "$[0].timestamp_tz()"),
        ["\"2023-08-15T12:34:56+05:30\""]
    );
    assert_eq!(
        q("[\"15-08-2023\"]", "$[0].datetime(\"dd-mm-yyyy\")"),
        ["\"2023-08-15\""]
    );
    assert_eq!(
        q_err("[\"garbage\"]", "$[0].datetime()"),
        "datetime format is not recognized: \"garbage\""
    );
    // timezone-dependent cast is gated on the _tz variants
    assert_eq!(
        q_err("[\"2023-08-15\"]", "$[0].timestamp_tz()"),
        "cannot convert value from date to timestamptz without time zone usage"
    );
    assert_eq!(
        q_tz("[\"2023-08-15\"]", "$[0].timestamp_tz()"),
        ["\"2023-08-15T00:00:00+00:00\""]
    );
    // datetime comparison inside filters
    assert_eq!(
        q(
            "[\"2023-08-15\", \"2023-09-01\"]",
            "$[*].datetime() ? (@ < \"2023-08-20\".datetime())"
        ),
        ["\"2023-08-15\""]
    );
}

#[test]
fn match_and_first() {
    assert_eq!(matches("{\"a\": 1}", "$.a == 1", false).unwrap(), Some(true));
    assert_eq!(matches("{\"a\": 1}", "$.a == 2", false).unwrap(), Some(false));
    assert_eq!(
        matches("{\"a\": 1}", "$.a", false).unwrap_err(),
        "single boolean result is expected"
    );
    assert_eq!(matches("{\"a\": 1}", "$.a", true).unwrap(), None);
    assert_eq!(query_array("[1, 2, 3]", "$[*] ? (@ > 1)"), "[2, 3]");
    assert_eq!(query_first("[1, 2, 3]", "$[*] ? (@ > 1)"), Some("2".into()));
    assert_eq!(query_first("[1, 2, 3]", "$[*] ? (@ > 5)"), None);
}

#[test]
fn variables() {
    assert_eq!(
        exists_vars("[1, 2, 3]", "$[*] ? (@ > $x)", "{\"x\": 2}"),
        Some(true)
    );
    assert_eq!(
        exists_vars("[1, 2, 3]", "$[*] ? (@ > $x)", "{\"x\": 5}"),
        Some(false)
    );
    let err = query("[1]", "$[*] ? (@ > $x)", true, false).unwrap_err();
    assert_eq!(err, "could not find jsonpath variable \"x\"");
}

#[test]
fn last_and_bool_results() {
    assert_eq!(q("[1, 2, 3]", "$[last]"), ["3"]);
    assert_eq!(q("[1, 2, 3]", "$[last - 1]"), ["2"]);
    // @@-style top-level predicate renders as jsonb bool / null
    assert_eq!(q("[1, 2, 3]", "$[*] > 2"), ["true"]);
    assert_eq!(q("[1, 2, 3]", "$[*] > 5"), ["false"]);
    assert_eq!(q("[1, \"a\"]", "$[*] > 1"), ["null"]);
}

fn jt_path_node<'mcx>(mcx: mcx::Mcx<'mcx>, path: &str) -> types_nodes::Node<'mcx> {
    use types_nodes::primnodes::JsonTablePath;
    let img: &[u8] = jp_image(mcx, path).leak();
    let c = types_nodes::Node::mk_const(
        mcx,
        4072,
        -1,
        0,
        -1,
        datum::Datum::from_usize(img.as_ptr() as usize),
        false,
        false,
    )
    .unwrap();
    types_nodes::Node::mk(mcx, JsonTablePath { value: Some(c), name: None }).unwrap()
}

fn jt_scan<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    path: &str,
    cols: Option<(i32, i32)>,
    child: Option<types_nodes::Node<'mcx>>,
    error_on_error: bool,
) -> types_nodes::Node<'mcx> {
    use types_nodes::primnodes::JsonTablePathScan;
    types_nodes::Node::mk(
        mcx,
        JsonTablePathScan {
            path: Some(jt_path_node(mcx, path)),
            errorOnError: error_on_error,
            child,
            colMin: cols.map_or(-1, |c| c.0),
            colMax: cols.map_or(-1, |c| c.1),
        },
    )
    .unwrap()
}

fn jt_row(
    jt: &crate::json_table::JsonTableExecContext<'_>,
    mcx: mcx::Mcx<'_>,
    ncols: usize,
) -> Vec<(Option<String>, i32)> {
    (0..ncols)
        .map(|c| {
            let (img, ord) = jt.current_row(c);
            (img.map(|i| out(mcx, &i[4..])), ord)
        })
        .collect()
}

#[test]
fn json_table_single_path_scan() {
    setup();
    let cx = MemoryContext::new("json_table test");
    let mcx = cx.mcx();
    let plan = jt_scan(mcx, "$.a[*]", Some((0, 0)), None, false);
    let mut jt =
        crate::json_table::JsonTableExecContext::init(mcx, plan, mcx::PgVec::new_in(mcx), 1)
            .unwrap();
    let doc = jb_payload(mcx, "{\"a\": [1, 2, 3]}");
    jt.set_document(&doc[4..]).unwrap();
    let mut got = Vec::new();
    while jt.fetch_row().unwrap() {
        got.extend(jt_row(&jt, mcx, 1));
    }
    assert_eq!(
        got,
        [
            (Some("1".to_string()), 1),
            (Some("2".to_string()), 2),
            (Some("3".to_string()), 3)
        ]
    );
    // second document through the same context (rescan shape)
    let doc2 = jb_payload(mcx, "{\"a\": [7]}");
    jt.set_document(&doc2[4..]).unwrap();
    assert!(jt.fetch_row().unwrap());
    assert_eq!(jt_row(&jt, mcx, 1), [(Some("7".to_string()), 1)]);
    assert!(!jt.fetch_row().unwrap());
}

#[test]
fn json_table_nested_outer_join() {
    setup();
    let cx = MemoryContext::new("json_table test");
    let mcx = cx.mcx();
    let child = jt_scan(mcx, "$.ys[*]", Some((1, 1)), None, false);
    let root = jt_scan(mcx, "$[*]", Some((0, 0)), Some(child), false);
    let mut jt =
        crate::json_table::JsonTableExecContext::init(mcx, root, mcx::PgVec::new_in(mcx), 2)
            .unwrap();
    let doc = jb_payload(mcx, "[{\"x\": 1, \"ys\": [10, 11]}, {\"x\": 2, \"ys\": []}]");
    jt.set_document(&doc[4..]).unwrap();
    let mut got = Vec::new();
    while jt.fetch_row().unwrap() {
        got.push(jt_row(&jt, mcx, 2));
    }
    let p1 = "{\"x\": 1, \"ys\": [10, 11]}".to_string();
    let p2 = "{\"x\": 2, \"ys\": []}".to_string();
    assert_eq!(
        got,
        [
            vec![(Some(p1.clone()), 1), (Some("10".to_string()), 1)],
            vec![(Some(p1), 1), (Some("11".to_string()), 2)],
            // nested path found no rows: outer-join NULL side
            vec![(Some(p2), 2), (None, 0)],
        ]
    );
}

#[test]
fn json_table_sibling_join_union() {
    setup();
    let cx = MemoryContext::new("json_table test");
    let mcx = cx.mcx();
    let l = jt_scan(mcx, "$.a[*]", Some((1, 1)), None, false);
    let r = jt_scan(mcx, "$.b[*]", Some((2, 2)), None, false);
    let join = types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::JsonTableSiblingJoin { lplan: Some(l), rplan: Some(r) },
    )
    .unwrap();
    let root = jt_scan(mcx, "$", Some((0, 0)), Some(join), false);
    let mut jt =
        crate::json_table::JsonTableExecContext::init(mcx, root, mcx::PgVec::new_in(mcx), 3)
            .unwrap();
    let doc = jb_payload(mcx, "{\"a\": [1, 2], \"b\": [3]}");
    jt.set_document(&doc[4..]).unwrap();
    let mut got = Vec::new();
    while jt.fetch_row().unwrap() {
        let row = jt_row(&jt, mcx, 3);
        got.push((row[1].clone(), row[2].clone()));
    }
    assert_eq!(
        got,
        [
            ((Some("1".to_string()), 1), (None, 0)),
            ((Some("2".to_string()), 2), (None, 0)),
            // exhausted left keeps its ordinal but reports a null row pattern
            ((None, 2), (Some("3".to_string()), 1)),
        ]
    );
}

#[test]
fn json_table_ordinal_resets_per_parent_row() {
    setup();
    let cx = MemoryContext::new("json_table test");
    let mcx = cx.mcx();
    let child = jt_scan(mcx, "$.ys[*]", Some((1, 1)), None, false);
    let root = jt_scan(mcx, "$[*]", Some((0, 0)), Some(child), false);
    let mut jt =
        crate::json_table::JsonTableExecContext::init(mcx, root, mcx::PgVec::new_in(mcx), 2)
            .unwrap();
    let doc = jb_payload(mcx, "[{\"ys\": [10, 11]}, {\"ys\": [20, 21]}]");
    jt.set_document(&doc[4..]).unwrap();
    let mut got = Vec::new();
    while jt.fetch_row().unwrap() {
        let row = jt_row(&jt, mcx, 2);
        got.push((row[0].1, row[1].1, row[1].0.clone()));
    }
    assert_eq!(
        got,
        [
            (1, 1, Some("10".to_string())),
            (1, 2, Some("11".to_string())),
            (2, 1, Some("20".to_string())),
            (2, 2, Some("21".to_string())),
        ]
    );
}

#[test]
fn json_table_row_pattern_errors() {
    setup();
    let cx = MemoryContext::new("json_table test");
    let mcx = cx.mcx();
    let doc = jb_payload(mcx, "{\"a\": 1}");

    // errorOnError = false: jperIsError leaves found empty (zero rows)
    let plan = jt_scan(mcx, "strict $.missing", Some((0, 0)), None, false);
    let mut jt =
        crate::json_table::JsonTableExecContext::init(mcx, plan, mcx::PgVec::new_in(mcx), 1)
            .unwrap();
    jt.set_document(&doc[4..]).unwrap();
    assert!(!jt.fetch_row().unwrap());

    // errorOnError = true: the jsonpath error surfaces
    let plan = jt_scan(mcx, "strict $.missing", Some((0, 0)), None, true);
    let mut jt =
        crate::json_table::JsonTableExecContext::init(mcx, plan, mcx::PgVec::new_in(mcx), 1)
            .unwrap();
    let err = jt.set_document(&doc[4..]).unwrap_err();
    assert_eq!(
        err.message(),
        "JSON object does not contain key \"missing\""
    );
}
