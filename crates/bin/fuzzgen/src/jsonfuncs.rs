//! JSONFUNCS: the general json/jsonb function + operator surface that the
//! J1 `sqljson` module (SQL/JSON grammar + the Q2 tweaks) does NOT reach —
//! the builder/accessor/operator/expand/aggregate long tail in json.c,
//! jsonb.c, jsonb_op.c, jsonb_util.c and jsonfuncs.c (jsonfuncs.c was 21%
//! covered after J1; get_object/get_array callbacks, the populate/each/
//! elements SRF workers and the jsonb_op.c operator family were the bulk
//! of the residue). The jsonpath *execution* engine is the other lane —
//! nothing here calls jsonb_path_*.
//!
//! Productions (weights in weights.rs, all `jsonfuncs:` prefixed):
//!   jacc     jsonb accessors `->` `->>` `#>` `#>>` (jsonb.c
//!            jsonb_object_field{,_text}/jsonb_array_element{,_text}/
//!            get_jsonb_path_all) — the jsonb side, distinct from the
//!            json.c text-parser accessors J1's `jops` exercised
//!   contain  containment `@>` / `<@` (jsonb_op.c jsonb_contains/contained
//!            → jsonb_util.c JsonbDeepContains), boolean output
//!   exist    key/element existence `?` / `?|` / `?&` (jsonb_op.c
//!            jsonb_exists{,_any,_all}), boolean output
//!   concat   `||` concatenation (jsonb.c jsonb_concat) + the `-` text[]
//!            multi-key delete form (jsonb_delete_array) — the array-path
//!            spellings J1's `mut` did not carry
//!   each     json/jsonb_each + _text as a FROM SRF, ordered to a total
//!            order (each_worker + get_object callbacks)
//!   elems    json/jsonb_array_elements + _text WITH ORDINALITY, ordered
//!            (elements_worker + get_array_element callbacks)
//!   keys     json/jsonb_object_keys WITH ORDINALITY, ordered
//!   extract  json/jsonb_extract_path + _text variadic text path
//!            (get_path — the get_worker path-array machinery)
//!   meta     jsonb_array_length / jsonb_typeof (JsonbTypeName) / json_*
//!            metadata over the jsonb container edges
//!   agg      the CLASSIC aggregates json_agg / jsonb_agg / json_object_agg
//!            / jsonb_object_agg (json.c/jsonb.c *_transfn_worker) over
//!            deterministically ORDERed sources — not JSON_ARRAYAGG (J1)
//!   sub      jsonb subscripting `j['k']` read + a self-contained
//!            UPDATE..SET j[...] = ... bracket (jsonbsubs.c)
//!   err      matched-error fuel, low weight: shapes that raise
//!            byte-identical errors on both engines
//!
//! Determinism law (identical to sqljson): literal-driven except the
//! pk-pinned single-row fz_rich subquery form; every set-returning form
//! carries an ORDER BY to a total order (key+value text, or WITH
//! ORDINALITY) so the order-sensitive differ compares byte-for-byte; no
//! now()/current_*; no engine-computed float text on the compare surface
//! (all numerics are exact jsonb numerics or ::text of integer/text/bool).

use crate::stmt::{Gen, StmtKind};

/// jsonb/json object documents (no single quotes / backslashes inside so
/// they drop verbatim into single-quoted SQL). Duplicate keys included:
/// json keeps both, jsonb keeps last — a real divergence surface.
const OBJS: &[&str] = &[
    "{}",
    "{\"a\": 1, \"b\": 2, \"c\": 3}",
    "{\"a\": 1, \"a\": 2}",
    "{\"a\": {\"b\": {\"c\": 42}}}",
    "{\"a\": [1, 2, 3], \"b\": {\"x\": 9}, \"k\": null}",
    "{\"b\": \"x\", \"a\": \"y\", \"c\": \"z\"}",
    "{\"num\": 123.450, \"big\": 9223372036854775807, \"neg\": -7}",
    "{\"one\": 1, \"two\": {\"three\": [true, false, null]}}",
];

/// jsonb/json array documents.
const ARRS: &[&str] = &[
    "[]",
    "[1, 2, 3]",
    "[1, \"two\", null, true, {\"k\": \"v\"}]",
    "[[1, 2], [3], []]",
    "[{\"a\": 1}, {\"a\": 2}, {\"b\": 3}]",
    "[10, 20, 30, 40, 50]",
    "[null, null, 0]",
];

/// Any document (objects, arrays, scalars) for the operator/type edges.
const ANY: &[&str] = &[
    "{\"a\": 1, \"b\": 2}",
    "[1, 2, 3]",
    "\"scalar\"",
    "42",
    "3.14",
    "true",
    "null",
    "{\"a\": {\"b\": [1, 2]}}",
    "[{\"a\": 1}, {\"b\": 2}]",
];

/// Text keys for the existence/accessor arms; a couple absent ('zz','q0')
/// to drive the not-found branches.
const KEYS: &[&str] = &["a", "b", "c", "k", "num", "one", "zz", "q0"];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_jsonfuncs_module(g: &mut Gen) -> Vec<StmtKind> {
    match g.weights.pick(
        g.rng,
        &[
            "jsonfuncs:jacc",
            "jsonfuncs:contain",
            "jsonfuncs:exist",
            "jsonfuncs:concat",
            "jsonfuncs:each",
            "jsonfuncs:elems",
            "jsonfuncs:keys",
            "jsonfuncs:extract",
            "jsonfuncs:meta",
            "jsonfuncs:agg",
            "jsonfuncs:sub",
            "jsonfuncs:err",
        ],
    ) {
        "jsonfuncs:sub" => return gen_sub(g),
        "jsonfuncs:jacc" => vec![StmtKind::Raw(gen_jacc(g))],
        "jsonfuncs:contain" => vec![StmtKind::Raw(gen_contain(g))],
        "jsonfuncs:exist" => vec![StmtKind::Raw(gen_exist(g))],
        "jsonfuncs:concat" => vec![StmtKind::Raw(gen_concat(g))],
        "jsonfuncs:each" => vec![StmtKind::Raw(gen_each(g))],
        "jsonfuncs:elems" => vec![StmtKind::Raw(gen_elems(g))],
        "jsonfuncs:keys" => vec![StmtKind::Raw(gen_keys(g))],
        "jsonfuncs:extract" => vec![StmtKind::Raw(gen_extract(g))],
        "jsonfuncs:meta" => vec![StmtKind::Raw(gen_meta(g))],
        "jsonfuncs:agg" => vec![StmtKind::Raw(gen_agg(g))],
        _ => vec![StmtKind::Raw(gen_err(g))],
    }
}

/// A jsonb operand: literal 4/5 of the time, else a pk-pinned single-row
/// fz_rich column subquery (deterministic, single row).
fn jb(g: &mut Gen, pool: &[&str]) -> String {
    if g.rng.chance(1, 5) {
        let col = *g.rng.pick(&["r_jsonb", "r_jsonb_b"]);
        let pk = 1 + g.rng.below(8);
        format!("(SELECT {} FROM fz_rich WHERE pk = {})", col, pk)
    } else {
        format!("jsonb '{}'", g.rng.pick(pool))
    }
}

/// jsonb `->` `->>` `#>` `#>>` (jsonb.c accessors). Whole-column sweeps go
/// through fz_rich (ORDER BY pk); scalar forms cast the jsonb result
/// ::text for byte-exact comparison.
fn gen_jacc(g: &mut Gen) -> String {
    g.fire("jsonfuncs:jacc");
    // 1/4: whole-column sweep over fz_rich (jsonb + json side by side).
    if g.rng.chance(1, 4) {
        let k = *g.rng.pick(KEYS);
        return format!(
            "SELECT pk, (r_jsonb -> '{k}')::text, r_jsonb ->> '{k}', (r_json -> '{k}')::text, r_json ->> '{k}' FROM fz_rich ORDER BY pk;"
        );
    }
    match g.rng.below(5) {
        // object field by key
        0 => {
            let d = *g.rng.pick(OBJS);
            let k = *g.rng.pick(KEYS);
            format!("SELECT (jsonb '{d}' -> '{k}')::text, jsonb '{d}' ->> '{k}';")
        }
        // array element by index (incl. negative + out-of-range → NULL)
        1 => {
            let d = *g.rng.pick(ARRS);
            let i = *g.rng.pick(&["0", "1", "2", "-1", "-2", "9"]);
            format!("SELECT (jsonb '{d}' -> {i})::text, jsonb '{d}' ->> {i};")
        }
        // #> / #>> text-array path
        2 | 3 => {
            let d = *g.rng.pick(&[
                "{\"a\": {\"b\": {\"c\": 42}}}",
                "{\"a\": [1, 2, 3], \"b\": {\"x\": 9}}",
                "[{\"a\": 1}, {\"a\": 2}]",
                "{\"a\": {\"b\": [10, 20]}}",
            ]);
            let p = *g.rng.pick(&[
                "{a}", "{a,b}", "{a,b,c}", "{a,0}", "{b,x}", "{0,a}", "{a,b,1}", "{zz}", "{}",
            ]);
            format!("SELECT (jsonb '{d}' #> '{p}')::text, jsonb '{d}' #>> '{p}';")
        }
        // chained -> then ->>
        _ => {
            let d = *g.rng.pick(&[
                "{\"a\": {\"b\": 7}}",
                "{\"a\": [{\"b\": 1}, {\"b\": 2}]}",
                "{\"one\": {\"two\": {\"three\": 3}}}",
            ]);
            let (k1, k2) = *g.rng.pick(&[("a", "b"), ("a", "0"), ("one", "two")]);
            format!("SELECT (jsonb '{d}' -> '{k1}') ->> '{k2}';")
        }
    }
}

/// Containment `@>` / `<@` (jsonb_op.c → JsonbDeepContains). Boolean out.
fn gen_contain(g: &mut Gen) -> String {
    g.fire("jsonfuncs:contain");
    // 1/4: fz_rich sweep (both directions against a literal probe).
    if g.rng.chance(1, 4) {
        let probe = *g.rng.pick(&["{\"a\": 1}", "{\"k\": \"v\"}", "[]", "{}", "[1]"]);
        return format!(
            "SELECT pk, r_jsonb @> jsonb '{probe}', r_jsonb <@ jsonb '{probe}' FROM fz_rich ORDER BY pk;"
        );
    }
    let l = jb(g, ANY);
    let r = format!("jsonb '{}'", g.rng.pick(ANY));
    if g.rng.chance(1, 2) {
        format!("SELECT {l} @> {r}, {l} <@ {r};")
    } else {
        // Structured containment pairs (true/false mix through DeepContains).
        let (a, b) = *g.rng.pick(&[
            ("{\"a\": 1, \"b\": 2}", "{\"a\": 1}"),
            ("{\"a\": {\"b\": [1, 2, 3]}}", "{\"a\": {\"b\": [2]}}"),
            ("[1, 2, {\"a\": 1}]", "[{\"a\": 1}]"),
            ("[1, 2, 3]", "[3, 1]"),
            ("{\"a\": [1, 2]}", "{\"a\": [1, 2, 3]}"),
        ]);
        format!("SELECT jsonb '{a}' @> jsonb '{b}', jsonb '{a}' <@ jsonb '{b}';")
    }
}

/// Existence `?` / `?|` / `?&` (jsonb_op.c jsonb_exists{,_any,_all}). For
/// objects tests key presence; for arrays tests string-element presence.
fn gen_exist(g: &mut Gen) -> String {
    g.fire("jsonfuncs:exist");
    if g.rng.chance(1, 4) {
        let k = *g.rng.pick(KEYS);
        let set = *g.rng.pick(&["array['a','b']", "array['k','zz']", "array['a']"]);
        return format!(
            "SELECT pk, r_jsonb ? '{k}', r_jsonb ?| {set}, r_jsonb ?& {set} FROM fz_rich ORDER BY pk;"
        );
    }
    let d = *g.rng.pick(&[
        "{\"a\": 1, \"b\": 2, \"c\": 3}",
        "{\"a\": 1, \"a\": 2}",
        "[\"a\", \"b\", \"x\"]",
        "{\"k\": null, \"z\": 1}",
        "[]",
        "{}",
    ]);
    match g.rng.below(3) {
        0 => {
            let k = *g.rng.pick(KEYS);
            format!("SELECT jsonb '{d}' ? '{k}';")
        }
        1 => {
            let set = *g.rng.pick(&["array['a','x']", "array['zz','q0']", "array['b']", "array[]::text[]"]);
            format!("SELECT jsonb '{d}' ?| {set};")
        }
        _ => {
            let set = *g.rng.pick(&["array['a','b']", "array['a','zz']", "array['a']", "array[]::text[]"]);
            format!("SELECT jsonb '{d}' ?& {set};")
        }
    }
}

/// `||` concatenation (jsonb_concat) + `-` text[] multi-key delete
/// (jsonb_delete_array). Output ::text.
fn gen_concat(g: &mut Gen) -> String {
    g.fire("jsonfuncs:concat");
    match g.rng.below(6) {
        // object || object (right wins on key clash)
        0 | 1 => {
            let a = *g.rng.pick(&["{\"a\": 1, \"b\": 2}", "{}", "{\"a\": {\"x\": 1}}"]);
            let b = *g.rng.pick(&["{\"b\": 9, \"c\": 3}", "{\"a\": 99}", "{}"]);
            format!("SELECT (jsonb '{a}' || jsonb '{b}')::text;")
        }
        // array || array, array || scalar (scalar wrapped)
        2 => {
            let a = *g.rng.pick(&["[1, 2]", "[]", "[{\"a\": 1}]"]);
            let b = *g.rng.pick(&["[3, 4]", "5", "\"x\"", "[]"]);
            format!("SELECT (jsonb '{a}' || jsonb '{b}')::text;")
        }
        // object - text[] (delete several keys at once)
        3 => {
            let d = *g.rng.pick(&["{\"a\": 1, \"b\": 2, \"c\": 3}", "{\"a\": 1}", "{}"]);
            let ks = *g.rng.pick(&["array['a','c']", "array['b']", "array['zz']", "array['a','b','c']"]);
            format!("SELECT (jsonb '{d}' - {ks})::text;")
        }
        // array - text[] is not defined; use array - int (element delete)
        4 => {
            let d = *g.rng.pick(&["[10, 20, 30]", "[\"x\", \"y\"]", "[1]"]);
            let i = *g.rng.pick(&["0", "1", "-1", "5"]);
            format!("SELECT (jsonb '{d}' - {i})::text;")
        }
        // object - text (single key), chained with ||
        _ => {
            let d = *g.rng.pick(&["{\"a\": 1, \"b\": 2}", "{\"k\": \"v\", \"z\": 0}"]);
            let k = *g.rng.pick(KEYS);
            format!("SELECT (jsonb '{d}' - '{k}' || jsonb '{{\"n\": 1}}')::text;")
        }
    }
}

/// json/jsonb_each + _text as a FROM SRF, ordered to a total order
/// (key, then value text) so the differ compares row-for-row.
fn gen_each(g: &mut Gen) -> String {
    g.fire("jsonfuncs:each");
    let d = *g.rng.pick(OBJS);
    match g.rng.below(4) {
        0 => format!(
            "SELECT key, value::text FROM jsonb_each(jsonb '{d}') ORDER BY key, value::text;"
        ),
        1 => format!(
            "SELECT key, value FROM jsonb_each_text(jsonb '{d}') ORDER BY key, value;"
        ),
        2 => format!(
            "SELECT key, value::text FROM json_each(json '{d}') ORDER BY key, value::text;"
        ),
        _ => format!(
            "SELECT key, value FROM json_each_text(json '{d}') ORDER BY key, value;"
        ),
    }
}

/// json/jsonb_array_elements + _text WITH ORDINALITY (ordinality is the
/// deterministic order key).
fn gen_elems(g: &mut Gen) -> String {
    g.fire("jsonfuncs:elems");
    let d = *g.rng.pick(ARRS);
    match g.rng.below(4) {
        0 => format!(
            "SELECT ord, value::text FROM jsonb_array_elements(jsonb '{d}') WITH ORDINALITY AS t(value, ord) ORDER BY ord;"
        ),
        1 => format!(
            "SELECT ord, value FROM jsonb_array_elements_text(jsonb '{d}') WITH ORDINALITY AS t(value, ord) ORDER BY ord;"
        ),
        2 => format!(
            "SELECT ord, value::text FROM json_array_elements(json '{d}') WITH ORDINALITY AS t(value, ord) ORDER BY ord;"
        ),
        _ => format!(
            "SELECT ord, value FROM json_array_elements_text(json '{d}') WITH ORDINALITY AS t(value, ord) ORDER BY ord;"
        ),
    }
}

/// json/jsonb_object_keys WITH ORDINALITY (json keeps document order incl.
/// duplicates; jsonb is sorted-deduped — both deterministic, ordinality
/// orders the rowset).
fn gen_keys(g: &mut Gen) -> String {
    g.fire("jsonfuncs:keys");
    let d = *g.rng.pick(OBJS);
    if g.rng.chance(1, 2) {
        format!(
            "SELECT ord, k FROM jsonb_object_keys(jsonb '{d}') WITH ORDINALITY AS t(k, ord) ORDER BY ord;"
        )
    } else {
        format!(
            "SELECT ord, k FROM json_object_keys(json '{d}') WITH ORDINALITY AS t(k, ord) ORDER BY ord;"
        )
    }
}

/// json/jsonb_extract_path + _text (variadic text path → get_path).
fn gen_extract(g: &mut Gen) -> String {
    g.fire("jsonfuncs:extract");
    let d = *g.rng.pick(&[
        "{\"a\": {\"b\": {\"c\": 42}}}",
        "{\"a\": [1, 2, {\"d\": 5}]}",
        "[{\"a\": 1}, {\"a\": 2}]",
        "{\"a\": {\"b\": [10, 20]}, \"k\": null}",
    ]);
    // Variadic path components (numeric strings index arrays).
    let path = *g.rng.pick(&[
        "'a'",
        "'a', 'b'",
        "'a', 'b', 'c'",
        "'a', '2', 'd'",
        "'a', '0'",
        "'0', 'a'",
        "'zz'",
        "'k'",
    ]);
    let jb = if g.rng.chance(1, 2) { "jsonb" } else { "json" };
    format!(
        "SELECT {jb}_extract_path({jb} '{d}', {path})::text, {jb}_extract_path_text({jb} '{d}', {path});"
    )
}

/// jsonb_array_length / jsonb_typeof (JsonbTypeName) and json metadata over
/// the container edges.
fn gen_meta(g: &mut Gen) -> String {
    g.fire("jsonfuncs:meta");
    match g.rng.below(4) {
        0 => {
            let d = *g.rng.pick(&["[1, 2, 3]", "[]", "[[1], [2, 3]]", "[null, null]"]);
            format!("SELECT jsonb_array_length(jsonb '{d}'), json_array_length(json '{d}');")
        }
        1 => {
            let d = *g.rng.pick(ANY);
            format!("SELECT jsonb_typeof(jsonb '{d}'), json_typeof(json '{d}');")
        }
        // typeof over every jsonb scalar/container kind in one deterministic sweep
        2 => "SELECT jsonb_typeof(v), json_typeof(v::text::json) FROM (VALUES (jsonb '{}'), (jsonb '[]'), (jsonb '\"s\"'), (jsonb '1'), (jsonb '1.5'), (jsonb 'true'), (jsonb 'null')) AS t(v) ORDER BY v::text;".to_string(),
        // key count via object_keys over fz_rich (deterministic per pk)
        _ => "SELECT pk, (SELECT count(*) FROM jsonb_object_keys(r_jsonb)) FROM fz_rich WHERE jsonb_typeof(r_jsonb) = 'object' ORDER BY pk;".to_string(),
    }
}

/// CLASSIC aggregates: json_agg / jsonb_agg / json_object_agg /
/// jsonb_object_agg over deterministically ORDERed sources. Value types
/// are int/text only (no engine-computed float text on the surface).
fn gen_agg(g: &mut Gen) -> String {
    g.fire("jsonfuncs:agg");
    match g.rng.below(6) {
        // json_agg / jsonb_agg over ordered VALUES
        0 | 1 => {
            let rows = *g.rng.pick(&[
                "(1, 'a'), (2, 'b'), (3, 'c')",
                "(3, 'x'), (1, 'y'), (2, 'z')",
                "(1, NULL), (2, 'b')",
            ]);
            let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
            let expr = *g.rng.pick(&["v", "n", "json_build_object('n', n, 'v', v)"]);
            format!(
                "SELECT {jb}_agg({expr} ORDER BY n)::text FROM (VALUES {rows}) AS t(n, v);"
            )
        }
        // json_object_agg / jsonb_object_agg over ordered VALUES (dup keys:
        // json keeps all, jsonb keeps last — ORDER BY k, v makes both
        // deterministic)
        2 | 3 => {
            let rows = *g.rng.pick(&[
                "('a', 1), ('b', 2), ('c', 3)",
                "('a', 1), ('a', 2), ('b', 3)",
                "('k', 0), ('z', 9)",
            ]);
            let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
            format!(
                "SELECT {jb}_object_agg(k, v ORDER BY k, v)::text FROM (VALUES {rows}) AS t(k, v);"
            )
        }
        // over fz_rich keyed by pk
        4 => {
            let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
            let col = *g.rng.pick(&["pk", "k_int", "k_text"]);
            format!("SELECT {jb}_agg({col} ORDER BY pk)::text FROM fz_rich;")
        }
        // object_agg keyed by k_text (NULL keys filtered — a NULL object
        // key is the err production's job)
        _ => {
            let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
            format!(
                "SELECT {jb}_object_agg(k_text, pk ORDER BY k_text, pk)::text FROM fz_rich WHERE k_text IS NOT NULL;"
            )
        }
    }
}

/// jsonb subscripting: read `j['k']` / `j[n]` (chained), then a
/// self-contained UPDATE..SET j[...] = ... bracket over a temp fixture
/// (jsonbsubs.c fetch + assign paths). Fixed name, one group.
fn gen_sub(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("jsonfuncs:sub");
    // 1/2: pure read expressions (no bracket).
    if g.rng.chance(1, 2) {
        let sql = match g.rng.below(4) {
            0 => {
                let d = *g.rng.pick(&["{\"a\": {\"b\": 1}}", "{\"a\": 1, \"k\": \"v\"}"]);
                let k = *g.rng.pick(KEYS);
                format!("SELECT (jsonb '{d}')['{k}']::text;")
            }
            1 => {
                let d = *g.rng.pick(&["[10, 20, 30]", "[[1, 2], [3]]", "[{\"a\": 1}]"]);
                let i = *g.rng.pick(&["0", "1", "2", "5"]);
                format!("SELECT (jsonb '{d}')[{i}]::text;")
            }
            2 => format!(
                "SELECT (jsonb '{}')['a']['b']::text;",
                *g.rng.pick(&["{\"a\": {\"b\": 7}}", "{\"a\": {\"c\": 1}}"])
            ),
            _ => format!(
                "SELECT (jsonb '{}')['a'][0]::text;",
                *g.rng.pick(&["{\"a\": [9, 8, 7]}", "{\"a\": []}"])
            ),
        };
        return vec![StmtKind::Raw(sql)];
    }
    // UPDATE-target bracket: create, seed (deterministic), mutate via
    // subscript, read back pk-ordered, drop.
    let assign = match g.rng.below(4) {
        0 => "j['a'] = '99'",
        1 => "j['a']['b'] = '\"deep\"'",
        2 => "j['new'] = 'true'",
        _ => "j['a'] = jsonb_build_object('x', pk)::text::jsonb",
    };
    vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_jf_sub;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_jf_sub (pk int PRIMARY KEY, j jsonb);".to_string()),
        StmtKind::Raw(
            "INSERT INTO fz_jf_sub VALUES (1, '{\"a\": {\"b\": 1}}'), (2, '{\"a\": 5}'), (3, '{}');"
                .to_string(),
        ),
        StmtKind::Raw(format!("UPDATE fz_jf_sub SET {assign};")),
        StmtKind::Raw("SELECT pk, j::text FROM fz_jf_sub ORDER BY pk;".to_string()),
        StmtKind::Raw("DROP TABLE fz_jf_sub;".to_string()),
    ]
}

/// Matched-error fuel (standard PG 18 messages, byte-identical on both
/// engines); low weight.
fn gen_err(g: &mut Gen) -> String {
    g.fire("jsonfuncs:err");
    (*g.rng.pick(&[
        // 22023 cannot get array length of a non-array
        "SELECT jsonb_array_length(jsonb '{\"a\": 1}');",
        "SELECT json_array_length(json '5');",
        // cannot call jsonb_each / _keys / elements on the wrong container
        "SELECT * FROM jsonb_each(jsonb '[1, 2]') ORDER BY 1;",
        "SELECT * FROM jsonb_object_keys(jsonb '[1, 2]') ORDER BY 1;",
        "SELECT * FROM jsonb_array_elements(jsonb '{\"a\": 1}') ORDER BY 1;",
        "SELECT * FROM json_each(json '\"scalar\"') ORDER BY 1;",
        // invalid concatenation (object || scalar)
        "SELECT jsonb '{\"a\": 1}' || jsonb '5';",
        "SELECT jsonb '{\"a\": 1}' || jsonb '\"x\"';",
        // jsonb_object array shape errors
        "SELECT jsonb_object('{a,b,c}');",
        "SELECT jsonb_object('{{a,1},{b}}');",
        // ?| / ?& want text[] not a scalar mismatch → invalid input
        "SELECT jsonb '[1,2]' ?& array['1', '2'];",
        // extract_path numeric-into-object edge is fine, but a bad #> index
        // on a scalar returns NULL; a genuine error: - on scalar jsonb
        "SELECT jsonb '5' - 'a';",
        "SELECT jsonb '\"x\"' - 0;",
        // subscript assignment onto a scalar path
        "SELECT (jsonb '5')['a'] IS NULL;",
    ]))
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Textual invariants for every jsonfuncs production: single line,
    /// ';'-terminated, balanced parens, no nondeterministic time sources,
    /// and every multi-row fz_rich rowset carries ORDER BY.
    #[test]
    fn jsonfuncs_statements_are_deterministic_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x7501);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..6000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_jsonfuncs_module(&mut g);
            assert!(!stmts.is_empty());
            for stmt in &stmts {
                let sql = stmt.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced: {sql}"
                );
                let low = sql.to_ascii_lowercase();
                assert!(!low.contains("now("), "nondeterministic now(): {sql}");
                assert!(!low.contains("current_"), "nondeterministic current_*: {sql}");
                // Every SELECT..FROM fz_rich (multi-row) must be ordered;
                // the pk-pinned scalar subquery form is single-row/exempt.
                if low.contains("from fz_rich") && !low.contains("where pk =") {
                    assert!(low.contains("order by"), "unordered fz_rich rowset: {sql}");
                }
                // Every set-returning FROM over an each/elements/keys SRF
                // must be ordered.
                if (low.contains("_each") || low.contains("_array_elements") || low.contains("_object_keys"))
                    && low.starts_with("select")
                    && low.contains(" from ")
                {
                    assert!(low.contains("order by"), "unordered SRF rowset: {sql}");
                }
            }
            for p in &prods {
                seen.insert(p.clone());
            }
        }
        for p in [
            "jsonfuncs:jacc", "jsonfuncs:contain", "jsonfuncs:exist",
            "jsonfuncs:concat", "jsonfuncs:each", "jsonfuncs:elems",
            "jsonfuncs:keys", "jsonfuncs:extract", "jsonfuncs:meta",
            "jsonfuncs:agg", "jsonfuncs:sub", "jsonfuncs:err",
        ] {
            assert!(seen.contains(p), "production {p} never fired");
        }
    }

    /// Same seed -> byte-identical statements (the reproducibility law).
    #[test]
    fn jsonfuncs_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(31337);
            let mut out = Vec::new();
            for _ in 0..800 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                out.push(
                    gen_jsonfuncs_module(&mut g)
                        .iter()
                        .map(|s| s.to_sql())
                        .collect::<Vec<_>>()
                        .join("|"),
                );
            }
            out
        };
        assert_eq!(run(), run());
    }
}
