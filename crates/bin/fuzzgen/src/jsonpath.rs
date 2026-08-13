//! JSONPATH execution-engine drain (gap-006 jsonpath cluster: the
//! jsonpath_exec.c / jsonpath.c / jsonpath_gram.y / jsonpath_scan.l arms the
//! J1 sqljson module leaves dark). sqljson covers the SQL/JSON *function*
//! surface (constructors, JSON_TABLE, item methods) but never touches the raw
//! jsonpath operators, the variable-binding paths, the filter predicate
//! grammar (like_regex / starts with / is unknown / arithmetic), the unicode
//! scanner, the recursive accessor, cross-type datetime comparison, or the
//! planner-side mutability walk. This module targets exactly those.
//!
//! Productions (weights in weights.rs, all `jsonpath:` prefixed):
//!   opexists   `jsonb @? jsonpath` (jsonb_path_exists_opr) over literals and
//!              the fz_rich jsonb columns (pk-ordered sweeps).
//!   opmatch    `jsonb @@ jsonpath` (jsonb_path_match_opr): boolean predicate
//!              paths — the appendBoolResult / single-boolean route.
//!   exists     jsonb_path_exists{,_tz}(jb, path[, vars, silent]) — the 2-arg
//!              and 4-arg (vars + silent) forms.
//!   match      jsonb_path_match{,_tz}(jb, path[, vars, silent]) — predicate
//!              paths yielding a single boolean.
//!   vars       variable binding both ways: the jsonb `vars` argument
//!              (getJsonPathVariableFromJsonb) and the JSON_VALUE/QUERY/EXISTS
//!              PASSING model (GetJsonPathVar / JsonItemFromDatum /
//!              JsonbValueInitNumericDatum / makeItemVariable) with paths that
//!              actually reference `$name`.
//!   arith      binary (+ - * / %) and unary (- +) arithmetic in filters and
//!              at top level (executeBinaryArithmExpr / executeUnaryArithmExpr)
//!              — numeric-valued only (no `.double()`; float-text law).
//!   cmp        the comparison + connective grammar (== != <> < <= > >= && || !),
//!              `is unknown`, and `exists(...)` in filters.
//!   likeregex  `like_regex "pat" [flag "imsxq"]` (makeItemLikeRegex /
//!              jspConvertRegexFlags / executeLikeRegex / compareStrings).
//!   startswith `starts with` (executeStartsWith), literal and $var operand.
//!   recursive  the `.**` accessor incl. `.**{lo to hi}` level bounds.
//!   unicode    `\uXXXX` and surrogate-pair escapes in jsonpath string
//!              literals and member keys (parseUnicode / addUnicode /
//!              addUnicodeChar / hexval / parseHexChar).
//!   dtcmp      cross-type datetime comparison in filters via the _tz family
//!              (cmpDateToTimestamp{,Tz} / cmpTimestampToTimestampTz /
//!              castTimeToTimeTz) — TimeZone pinned UTC on both sides
//!              (runner::DATETIME_GUC_PIN).
//!   mutidx     expression indexes + a STORED generated column over
//!              JSON_VALUE/EXISTS/QUERY with varied literal paths — the
//!              DefineIndex/immutability route into jspIsMutable /
//!              jspIsMutableWalker / jspGetBool, plus one deliberately-mutable
//!              path (matched IMMUTABLE-required error).
//!   err        matched jsonpath parse/exec errors (yyerror / bad regex /
//!              type errors), low weight, hand-shaped to raise byte-identical
//!              errors on both engines.
//!
//! Determinism: literal-driven except the operator sweeps over fz_rich, which
//! are always `ORDER BY pk`. Every item-returning path is funnelled through
//! jsonb_path_query_array (a single deterministic array per row) or
//! jsonb_path_query_first; the bare set-returning jsonb_path_query is never
//! used, so no statement is ever multi-row-unordered. No now()/current_*; no
//! `.double()` (engine-computed float text stays off the raw-compare surface).
//! All documents/paths are single-line, and no jsonb document or jsonpath
//! string literal contains a parenthesis, so the arity check's paren count
//! only ever sees balanced structural SQL parens.

use crate::stmt::{Gen, StmtKind};

/// jsonb documents with numeric fields/arrays for arithmetic + comparison.
/// Paren-free by construction.
const DOCS_NUM: &[&str] = &[
    "{\"a\": 5, \"b\": 2, \"c\": [1, 2, 3, 4]}",
    "{\"a\": -3, \"b\": 4, \"c\": []}",
    "{\"x\": 10, \"y\": 3, \"z\": 0}",
    "{\"a\": 1.5, \"b\": 2.5, \"c\": [10, 20, 30]}",
    "{\"nums\": [1, 2, 3, 4, 5], \"n\": 6, \"a\": 7}",
    "[1, 2, 3, 4, 5]",
    "[10, -5, 7, 0, 3]",
];

/// jsonb documents with string fields/arrays for like_regex / starts with.
const DOCS_STR: &[&str] = &[
    "{\"s\": \"hello world\", \"t\": \"abcABC\"}",
    "[\"apple\", \"banana\", \"cherry\", \"avocado\"]",
    "{\"name\": \"postgres\", \"tags\": [\"sql\", \"json\", \"path\"]}",
    "[\"foo123\", \"bar456\", \"foobar\", \"baz\"]",
    "{\"a\": \"CaseTest\", \"b\": \"lower\", \"c\": [\"x1\", \"x2\"]}",
];

/// Nested jsonb documents for the recursive `.**` accessor.
const DOCS_NEST: &[&str] = &[
    "{\"a\": {\"b\": {\"c\": [1, 2, 3]}}, \"x\": 5}",
    "{\"a\": [1, {\"b\": 2}, [3, {\"c\": 4}]], \"d\": 6}",
    "[{\"k\": 1}, {\"k\": {\"k\": 2}}, 3]",
    "{\"p\": {\"q\": 7, \"r\": {\"q\": 8}}}",
];

/// Mixed documents for the operators and generic existence paths.
const DOCS_ALL: &[&str] = &[
    "{\"a\": 1, \"b\": \"x\", \"c\": [1, 2, 3]}",
    "{\"a\": {\"b\": {\"c\": 9}}}",
    "[1, \"two\", null, true, {\"k\": \"v\"}]",
    "{\"s\": \"hello\", \"n\": 42, \"arr\": [5, 6, 7]}",
    "[]",
    "{}",
];

/// Existence-style paths (any items match => `@?` true). Paren-balanced.
const EXISTS_PATHS: &[&str] = &[
    "$.a",
    "$.c[*]",
    "$.a.b.c",
    "$[*]",
    "strict $.a",
    "lax $.arr[*] ? (@ > 5)",
    "$.** ? (@ > 2)",
    "$.c[*] ? (@ >= 2)",
    "$[*] ? (@ starts with \"h\")",
    "$.s ? (@ like_regex \"ell\")",
];

/// Top-level boolean predicate paths (for `@@`, jsonb_path_match). Each yields
/// a single boolean. Paren-balanced.
const PRED_PATHS: &[&str] = &[
    "$.a == 1",
    "$.n > 0",
    "$.a >= 1 && $.a <= 100",
    "exists($.c)",
    "exists($.missing)",
    "$.arr[*] > 4",
    "$.s starts with \"he\"",
    "$.s like_regex \"^h\"",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_jsonpath_module(g: &mut Gen) -> Vec<StmtKind> {
    let choice = g.weights.pick(
        g.rng,
        &[
            "jsonpath:opexists",
            "jsonpath:opmatch",
            "jsonpath:exists",
            "jsonpath:match",
            "jsonpath:vars",
            "jsonpath:arith",
            "jsonpath:cmp",
            "jsonpath:likeregex",
            "jsonpath:startswith",
            "jsonpath:recursive",
            "jsonpath:unicode",
            "jsonpath:dtcmp",
            "jsonpath:mutidx",
            "jsonpath:err",
        ],
    );
    // The mutidx shape emits a whole self-contained bracket.
    if choice == "jsonpath:mutidx" {
        return gen_mutidx(g);
    }
    let sql = match choice {
        "jsonpath:opexists" => gen_opexists(g),
        "jsonpath:opmatch" => gen_opmatch(g),
        "jsonpath:exists" => gen_exists(g),
        "jsonpath:match" => gen_match(g),
        "jsonpath:vars" => gen_vars(g),
        "jsonpath:arith" => gen_arith(g),
        "jsonpath:cmp" => gen_cmp(g),
        "jsonpath:likeregex" => gen_likeregex(g),
        "jsonpath:startswith" => gen_startswith(g),
        "jsonpath:recursive" => gen_recursive(g),
        "jsonpath:unicode" => gen_unicode(g),
        "jsonpath:dtcmp" => gen_dtcmp(g),
        _ => gen_err(g),
    };
    vec![StmtKind::Raw(sql)]
}

/// `jsonb @? jsonpath` — jsonb_path_exists_opr. Literal or fz_rich sweep.
fn gen_opexists(g: &mut Gen) -> String {
    g.fire("jsonpath:opexists");
    let p = *g.rng.pick(EXISTS_PATHS);
    if g.rng.chance(1, 3) {
        let col = *g.rng.pick(&["r_jsonb", "r_jsonb_b"]);
        format!("SELECT pk, {} @? '{}' FROM fz_rich ORDER BY pk;", col, p)
    } else {
        let d = *g.rng.pick(DOCS_ALL);
        format!("SELECT jsonb '{}' @? '{}';", d, p)
    }
}

/// `jsonb @@ jsonpath` — jsonb_path_match_opr (single boolean predicate).
fn gen_opmatch(g: &mut Gen) -> String {
    g.fire("jsonpath:opmatch");
    let p = *g.rng.pick(PRED_PATHS);
    if g.rng.chance(1, 3) {
        let col = *g.rng.pick(&["r_jsonb", "r_jsonb_b"]);
        format!("SELECT pk, {} @@ '{}' FROM fz_rich ORDER BY pk;", col, p)
    } else {
        let d = *g.rng.pick(DOCS_ALL);
        format!("SELECT jsonb '{}' @@ '{}';", d, p)
    }
}

/// vars object literal + a path referencing its variables.
fn vars_and_path() -> &'static [(&'static str, &'static str)] {
    &[
        ("{\"min\": 2}", "$.c[*] ? (@ > $min)"),
        ("{\"lo\": 1, \"hi\": 4}", "$.c[*] ? (@ >= $lo && @ <= $hi)"),
        ("{\"s\": \"apple\"}", "$[*] ? (@ == $s)"),
        ("{\"n\": 7}", "$ ? (@.a == $n)"),
        ("{\"pref\": \"ba\"}", "$[*] ? (@ starts with $pref)"),
        ("{\"k\": 3}", "$.nums[*] ? (@ >= $k)"),
    ]
}

/// jsonb_path_exists{,_tz}(jb, path[, vars, silent]).
fn gen_exists(g: &mut Gen) -> String {
    g.fire("jsonpath:exists");
    match g.rng.below(4) {
        // 2-arg form.
        0 => {
            let d = *g.rng.pick(DOCS_ALL);
            let p = *g.rng.pick(EXISTS_PATHS);
            format!("SELECT jsonb_path_exists(jsonb '{}', '{}');", d, p)
        }
        // 4-arg form with a jsonb vars object + silent flag.
        1 | 2 => {
            let (vars, p) = *g.rng.pick(vars_and_path());
            let d = *g.rng.pick(DOCS_NUM);
            let silent = if g.rng.chance(1, 2) { "true" } else { "false" };
            format!(
                "SELECT jsonb_path_exists(jsonb '{}', '{}', '{}', {});",
                d, p, vars, silent
            )
        }
        // _tz variant over a datetime path.
        _ => {
            let d = "[\"2020-01-01\", \"2024-06-15\"]";
            format!(
                "SELECT jsonb_path_exists_tz(jsonb '{}', '$[*] ? (@.datetime() > \"2019-01-01\".datetime())');",
                d
            )
        }
    }
}

/// jsonb_path_match{,_tz}(jb, path[, vars, silent]) — predicate => boolean.
fn gen_match(g: &mut Gen) -> String {
    g.fire("jsonpath:match");
    match g.rng.below(4) {
        0 | 1 => {
            let d = *g.rng.pick(DOCS_NUM);
            let p = *g.rng.pick(PRED_PATHS);
            format!("SELECT jsonb_path_match(jsonb '{}', '{}');", d, p)
        }
        2 => {
            let (vars, base) = *g.rng.pick(vars_and_path());
            let d = *g.rng.pick(DOCS_NUM);
            // Wrap the filter into a top-level existence predicate.
            let silent = if g.rng.chance(1, 2) { "true" } else { "false" };
            format!(
                "SELECT jsonb_path_match(jsonb '{}', 'exists({})', '{}', {});",
                d, base, vars, silent
            )
        }
        _ => format!(
            "SELECT jsonb_path_match_tz(jsonb '[\"2024-02-29\"]', 'exists($[*] ? (@.date() == \"2024-02-29\".date()))');"
        ),
    }
}

/// Variable binding: the jsonb `vars` argument and the PASSING model.
fn gen_vars(g: &mut Gen) -> String {
    g.fire("jsonpath:vars");
    match g.rng.below(6) {
        // jsonb vars => jsonb_path_query_array (deterministic single array).
        0 | 1 => {
            let (vars, p) = *g.rng.pick(vars_and_path());
            let d = *g.rng.pick(DOCS_NUM);
            format!(
                "SELECT jsonb_path_query_array(jsonb '{}', '{}', '{}');",
                d, p, vars
            )
        }
        // jsonb vars => jsonb_path_query_first.
        2 => {
            let (vars, p) = *g.rng.pick(vars_and_path());
            let d = *g.rng.pick(DOCS_NUM);
            format!(
                "SELECT jsonb_path_query_first(jsonb '{}', '{}', '{}');",
                d, p, vars
            )
        }
        // JSON_VALUE PASSING model (GetJsonPathVar / JsonItemFromDatum).
        3 | 4 => {
            let (val, name, path, ret) = *g.rng.pick(&[
                ("3", "x", "$.a ? (@ > $x)", "int"),
                ("2", "lo", "$.c[*] ? (@ >= $lo)", "int"),
                ("'apple'", "s", "$[*] ? (@ == $s)", "text"),
                ("5.5", "n", "$.a ? (@ < $n)", "numeric"),
            ]);
            format!(
                "SELECT JSON_VALUE(jsonb '{}', '{}' PASSING {} AS {} RETURNING {} NULL ON ERROR);",
                if ret == "text" { "[\"apple\", \"pear\"]" } else { "{\"a\": 4, \"c\": [1, 2, 3]}" },
                path, val, name, ret
            )
        }
        // JSON_QUERY PASSING model with a variable in the filter.
        _ => format!(
            "SELECT JSON_QUERY(jsonb '{{\"c\": [1, 2, 3, 4]}}', '$.c[*] ? (@ > $lo && @ <= $hi)' PASSING 1 AS lo, 3 AS hi WITH ARRAY WRAPPER)::text;"
        ),
    }
}

/// Arithmetic in filters and at top level (numeric-valued only).
fn gen_arith(g: &mut Gen) -> String {
    g.fire("jsonpath:arith");
    let d = *g.rng.pick(DOCS_NUM);
    let p = *g.rng.pick(&[
        // Top-level binary arithmetic (executeBinaryArithmExpr).
        "$.a + $.b",
        "$.a - $.b",
        "$.a * $.b",
        "$.a / $.b",
        // Unary arithmetic (executeUnaryArithmExpr).
        "-$.a",
        "+$.b",
        "-$.c[0]",
        // Arithmetic inside filters.
        "$.c[*] ? (@ + 1 > 3)",
        "$.c[*] ? (@ * 2 <= 6)",
        "$.c[*] ? (@ - 1 == 1)",
        "$ ? (@.a * 2 == 10)",
        "$ ? (-@.a < 0)",
        // Parenthesised arithmetic.
        "$.c[*] ? ((@ + $.a) > 6)",
    ]);
    format!("SELECT jsonb_path_query_array(jsonb '{}', '{}');", d, p)
}

/// Integer modulo needs an all-integer document; kept separate so `%` never
/// meets a numeric-with-scale operand.
fn gen_arith_mod(g: &mut Gen) -> String {
    let p = *g.rng.pick(&[
        "$.c[*] ? (@ % 2 == 1)",
        "$.a % $.b",
        "$.c[*] ? (@ % 3 == 0)",
    ]);
    format!("SELECT jsonb_path_query_array(jsonb '{{\"a\": 7, \"b\": 3, \"c\": [1, 2, 3, 4, 5, 6]}}', '{}');", p)
}

/// Comparison operators, connectives, `is unknown`, `exists(...)`.
fn gen_cmp(g: &mut Gen) -> String {
    g.fire("jsonpath:cmp");
    // 1/6: route to the integer-modulo arm (shares the arith surface).
    if g.rng.chance(1, 6) {
        return gen_arith_mod(g);
    }
    let d = *g.rng.pick(DOCS_NUM);
    match g.rng.below(3) {
        // Filter with a comparison / connective / negation.
        0 | 1 => {
            let p = *g.rng.pick(&[
                "$.c[*] ? (@ == 2)",
                "$.c[*] ? (@ != 2)",
                "$.c[*] ? (@ <> 3)",
                "$.c[*] ? (@ < 3)",
                "$.c[*] ? (@ <= 3)",
                "$.c[*] ? (@ > 1)",
                "$.c[*] ? (@ >= 2)",
                "$.c[*] ? (@ > 1 && @ < 4)",
                "$.c[*] ? (@ < 2 || @ > 3)",
                "$.c[*] ? (!(@ == 2))",
            ]);
            format!("SELECT jsonb_path_query_array(jsonb '{}', '{}');", d, p)
        }
        // Top-level predicate incl. `is unknown` and `exists()`.
        _ => {
            let p = *g.rng.pick(&[
                "($.a > 0) is unknown",
                "($.missing > 0) is unknown",
                "exists($.c)",
                "exists($.missing)",
                "$.a >= 0",
                "!($.a == $.b)",
            ]);
            format!("SELECT jsonb_path_query_array(jsonb '{}', '{}');", d, p)
        }
    }
}

/// like_regex with flags (makeItemLikeRegex / jspConvertRegexFlags /
/// executeLikeRegex). Patterns are paren-free.
fn gen_likeregex(g: &mut Gen) -> String {
    g.fire("jsonpath:likeregex");
    let d = *g.rng.pick(DOCS_STR);
    let p = *g.rng.pick(&[
        "$.s ? (@ like_regex \"world\")",
        "$[*] ? (@ like_regex \"^a\")",
        "$[*] ? (@ like_regex \"a.*o\")",
        "$[*] ? (@ like_regex \"[0-9]+\")",
        "$[*] ? (@ like_regex \"AN\" flag \"i\")",
        "$.t ? (@ like_regex \"abc\" flag \"i\")",
        "$[*] ? (@ like_regex \"^A\" flag \"i\")",
        "$.name ? (@ like_regex \"p.*s\" flag \"ix\")",
        "$[*] ? (@ like_regex \"foo\" flag \"q\")",
        "$.s ? (@ like_regex \"o.w\" flag \"s\")",
    ]);
    if g.rng.chance(1, 4) {
        // Operator form: predicate over the whole document.
        let pred = *g.rng.pick(&[
            "$.s like_regex \"hello\"",
            "$.name like_regex \"^post\"",
        ]);
        format!("SELECT jsonb '{}' @@ '{}';", d, pred)
    } else {
        format!("SELECT jsonb_path_query_array(jsonb '{}', '{}');", d, p)
    }
}

/// `starts with` (executeStartsWith), literal and $var operand.
fn gen_startswith(g: &mut Gen) -> String {
    g.fire("jsonpath:startswith");
    let d = *g.rng.pick(DOCS_STR);
    match g.rng.below(3) {
        0 | 1 => {
            let p = *g.rng.pick(&[
                "$[*] ? (@ starts with \"a\")",
                "$[*] ? (@ starts with \"ba\")",
                "$.name ? (@ starts with \"post\")",
                "$.s ? (@ starts with \"hello\")",
                "$.tags[*] ? (@ starts with \"j\")",
            ]);
            format!("SELECT jsonb_path_query_array(jsonb '{}', '{}');", d, p)
        }
        _ => format!(
            "SELECT jsonb_path_query_array(jsonb '[\"apple\", \"apricot\", \"banana\"]', '$[*] ? (@ starts with $pref)', '{{\"pref\": \"ap\"}}');"
        ),
    }
}

/// The recursive `.**` accessor incl. level bounds.
fn gen_recursive(g: &mut Gen) -> String {
    g.fire("jsonpath:recursive");
    let d = *g.rng.pick(DOCS_NEST);
    let p = *g.rng.pick(&[
        "$.**",
        "$.**{1}",
        "$.**{0 to 2}",
        "$.**{1 to last}",
        "$.**.q",
        "$.** ? (@ > 2)",
        "$.**{2}",
        "$.a.**",
    ]);
    format!("SELECT jsonb_path_query_array(jsonb '{}', '{}');", d, p)
}

/// `\uXXXX` and surrogate-pair escapes in jsonpath string literals + keys.
/// Both the jsonb document and the jsonpath escape to the same code points, so
/// the match is exact and the source stays ASCII-only.
fn gen_unicode(g: &mut Gen) -> String {
    g.fire("jsonpath:unicode");
    let s = *g.rng.pick(&[
        // \uXXXX key access.
        "SELECT jsonb_path_query_array(jsonb '{\"caf\\u00e9\": 1, \"na\\u00efve\": 2}', '$.\"caf\\u00e9\"');",
        // \uXXXX value comparison in a filter.
        "SELECT jsonb_path_query_array(jsonb '[\"\\u00e9\", \"e\", \"\\u00e8\"]', '$[*] ? (@ == \"\\u00e9\")');",
        // surrogate-pair key (emoji U+1F600).
        "SELECT jsonb_path_query_array(jsonb '{\"\\ud83d\\ude00\": true}', '$.\"\\ud83d\\ude00\"');",
        // surrogate-pair value comparison.
        "SELECT jsonb_path_query_array(jsonb '[\"\\ud83d\\ude00\", \"x\"]', '$[*] ? (@ == \"\\ud83d\\ude00\")');",
        // mixed ASCII + \uXXXX in a starts-with operand.
        "SELECT jsonb_path_query_array(jsonb '[\"\\u00e9tude\", \"study\"]', '$[*] ? (@ starts with \"\\u00e9\")');",
        // low code point via \u000X.
        "SELECT jsonb_path_query_array(jsonb '{\"a\\u0062c\": 5}', '$.\"a\\u0062c\"');",
    ]);
    s.to_string()
}

/// Cross-type datetime comparison in filters via the _tz family.
fn gen_dtcmp(g: &mut Gen) -> String {
    g.fire("jsonpath:dtcmp");
    let s = *g.rng.pick(&[
        // date vs date.
        "SELECT jsonb_path_query_array(jsonb '[\"2020-01-01\", \"2024-06-15\", \"1999-12-31\"]', '$[*] ? (@.date() < \"2024-01-01\".date())');",
        // date vs timestamp (cmpDateToTimestamp).
        "SELECT jsonb_path_query_array(jsonb '[\"2024-02-29\", \"2024-03-01\"]', '$[*] ? (@.datetime() == \"2024-02-29 00:00:00\".datetime())');",
        // date vs timestamptz via _tz (cmpDateToTimestampTz).
        "SELECT jsonb_path_query_array_tz(jsonb '[\"2024-02-29\", \"2024-06-15\"]', '$[*] ? (@.datetime() < \"2024-03-01 00:00:00+00\".datetime())');",
        // timestamp vs timestamptz (cmpTimestampToTimestampTz).
        "SELECT jsonb_path_query_array_tz(jsonb '[\"2024-02-29 12:00:00\"]', '$[*] ? (@.timestamp() < \"2024-03-01 00:00:00+00\".datetime())');",
        // time vs timetz (castTimeToTimeTz).
        "SELECT jsonb_path_query_array_tz(jsonb '[\"12:00:00\", \"23:00:00\"]', '$[*] ? (@.time() < \"20:00:00+00\".time_tz())');",
        // exists over a datetime filter (jsonb_path_exists_tz route).
        "SELECT jsonb_path_exists_tz(jsonb '[\"2024-01-01\"]', '$[*] ? (@.date() >= \"2024-01-01\".date())');",
    ]);
    s.to_string()
}

/// Expression indexes + a generated column over JSON_VALUE/EXISTS/QUERY: the
/// DefineIndex/immutability route into jspIsMutable / jspIsMutableWalker /
/// jspGetBool. One deliberately-mutable path raises the matched
/// IMMUTABLE-required error. Fixed-name, self-contained bracket.
fn gen_mutidx(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("jsonpath:mutidx");
    // Immutable index-expression paths spanning the jsonpath node kinds the
    // walker recurses (accessors, wildcard, recursive, filter, comparison,
    // arithmetic, like_regex, starts with, exists, item method, variable).
    let imm_paths: &[(&str, &str)] = &[
        ("JSON_VALUE(j, '$.a' RETURNING int)", "a int"),
        ("JSON_VALUE(j, '$.b' RETURNING text)", "b text"),
        ("JSON_EXISTS(j, '$.c[*] ? (@ > 1)')", "e1 bool"),
        ("JSON_EXISTS(j, '$.** ? (@ starts with \"a\")')", "e2 bool"),
        ("JSON_EXISTS(j, '$.a ? (@ + 1 == 2)')", "e3 bool"),
        ("JSON_EXISTS(j, '$.b ? (@ like_regex \"x\" flag \"i\")')", "e4 bool"),
        ("JSON_VALUE(j, '$.c[0]' RETURNING int)", "c0 int"),
        ("JSON_VALUE(j, '$.a ? (@ > $x)' PASSING 0 AS x RETURNING int)", "vp int"),
    ];
    let mut out = vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_jp_mut;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_jp_mut (id int PRIMARY KEY, j jsonb);".to_string()),
        StmtKind::Raw(
            "INSERT INTO fz_jp_mut VALUES (1, '{\"a\": 1, \"b\": \"abc\", \"c\": [1, 2, 3]}'), (2, '{\"a\": 2, \"b\": \"xyz\", \"c\": []}');"
                .to_string(),
        ),
    ];
    // Two or three distinct immutable indexes.
    let n = 2 + g.rng.below(2) as usize;
    let mut used = Vec::new();
    for i in 0..n {
        let (expr, _) = imm_paths[(g.rng.below_usize(imm_paths.len()) + i) % imm_paths.len()];
        if used.contains(&expr) {
            continue;
        }
        used.push(expr);
        out.push(StmtKind::Raw(format!(
            "CREATE INDEX fz_jp_mi{} ON fz_jp_mut (({}));",
            i, expr
        )));
    }
    // A STORED generated column over an immutable JSON_EXISTS (also walks the
    // immutability check).
    if g.rng.chance(1, 2) {
        out.push(StmtKind::Raw(
            "ALTER TABLE fz_jp_mut ADD COLUMN g bool GENERATED ALWAYS AS (JSON_EXISTS(j, '$.c[*] ? (@ >= 2)')) STORED;".to_string(),
        ));
    }
    // One deliberately-mutable path (datetime method) — matched error:
    // "functions in index expression must be marked IMMUTABLE".
    if g.rng.chance(1, 3) {
        out.push(StmtKind::Raw(
            "CREATE INDEX fz_jp_bad ON fz_jp_mut ((JSON_VALUE(j, '$.b.datetime()' RETURNING text)));".to_string(),
        ));
    }
    // Deterministic probe then teardown.
    out.push(StmtKind::Raw(
        "SELECT id, JSON_VALUE(j, '$.a' RETURNING int) AS a FROM fz_jp_mut ORDER BY id;".to_string(),
    ));
    out.push(StmtKind::Raw("DROP TABLE fz_jp_mut;".to_string()));
    out
}

/// Matched jsonpath parse/exec errors (hand-shaped, byte-identical on both
/// engines); low weight.
fn gen_err(g: &mut Gen) -> String {
    g.fire("jsonpath:err");
    let s = *g.rng.pick(&[
        // Parse errors (jsonpath_yyerror / fprintf_to_ereport).
        "SELECT jsonb '{}' @? '$.';",
        "SELECT jsonb '{}' @? '$[abc]';",
        "SELECT jsonb_path_query_array(jsonb '{}', '$ ? (@ ==)');",
        // Bad regex flag / pattern.
        "SELECT jsonb_path_query_array(jsonb '[\"a\"]', '$[*] ? (@ like_regex \"[\")');",
        "SELECT jsonb_path_query_array(jsonb '[\"a\"]', '$[*] ? (@ like_regex \"a\" flag \"z\")');",
        // starts with a non-string operand.
        "SELECT jsonb_path_query_array(jsonb '[\"a\"]', '$[*] ? (@ starts with 1)');",
        // strict structural error surfaced (not silent).
        "SELECT jsonb_path_query(jsonb '{}', 'strict $.a');",
        // bad unicode escape.
        "SELECT jsonb_path_query_array(jsonb '{}', '$.\"\\uZZZZ\"');",
        // undefined variable.
        "SELECT jsonb_path_query_array(jsonb '{\"a\": 1}', '$ ? (@.a > $undef)');",
    ]);
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Textual invariants for every jsonpath production: single line,
    /// ';'-terminated, balanced parens, no nondeterministic time sources, no
    /// `.double()` (float-text law), and every fz_rich sweep is pk-ordered.
    #[test]
    fn jsonpath_statements_are_deterministic_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x1F5017);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..6000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_jsonpath_module(&mut g);
            assert!(!stmts.is_empty());
            for stmt in &stmts {
                let sql = stmt.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                let low = sql.to_ascii_lowercase();
                assert!(!low.contains("now("), "nondeterministic now(): {sql}");
                assert!(!low.contains("current_"), "nondeterministic current_*: {sql}");
                assert!(!low.contains(".double("), ".double() float-text law: {sql}");
                if low.contains("from fz_rich") {
                    assert!(low.contains("order by"), "unordered fz_rich rowset: {sql}");
                }
            }
            for p in &prods {
                seen.insert(p.clone());
            }
        }
        for p in [
            "jsonpath:opexists",
            "jsonpath:opmatch",
            "jsonpath:exists",
            "jsonpath:match",
            "jsonpath:vars",
            "jsonpath:arith",
            "jsonpath:cmp",
            "jsonpath:likeregex",
            "jsonpath:startswith",
            "jsonpath:recursive",
            "jsonpath:unicode",
            "jsonpath:dtcmp",
            "jsonpath:mutidx",
            "jsonpath:err",
        ] {
            assert!(seen.contains(p), "production {p} never fired");
        }
    }

    /// Same seed -> byte-identical statement stream (the reproducibility law).
    #[test]
    fn jsonpath_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(9317);
            let mut out = Vec::new();
            for _ in 0..800 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                let stmts = gen_jsonpath_module(&mut g);
                out.push(stmts.iter().map(|s| s.to_sql()).collect::<Vec<_>>().join("|"));
            }
            out
        };
        assert_eq!(run(), run());
    }
}
