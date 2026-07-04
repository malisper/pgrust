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
    assert_eq!(
        q(
            "[\"abc\", \"abd\", \"xbc\"]",
            "$[*] ? (@ like_regex \"^ab.*c\")"
        ),
        ["\"abc\""]
    );
    assert_eq!(
        q(
            "[\"abc\", \"ABC\"]",
            "$[*] ? (@ like_regex \"^abc$\" flag \"i\")"
        ),
        ["\"abc\"", "\"ABC\""]
    );
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
        ["\"2023-08-15T07:04:56+00:00\""]
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
