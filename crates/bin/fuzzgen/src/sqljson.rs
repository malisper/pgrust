//! J1 SQL/JSON grammar: the PG16-18 SQL/JSON surface (gap-006 cluster:
//! transformJsonFuncExpr / ExecInitJsonExpr / ExecEvalJsonExprPath,
//! executeDateTimeMethod, jsonb_object_agg_transfn_worker, JSON_TABLE).
//!
//! Productions (weights in weights.rs, all `sqljson:` prefixed):
//!   obj     JSON_OBJECT (key:value and KEY..VALUE spellings, NULL/ABSENT
//!           ON NULL, WITH/WITHOUT UNIQUE KEYS, FORMAT JSON values,
//!           RETURNING json/jsonb/text/bytea)
//!   arr     JSON_ARRAY (value list + subquery form, ON NULL, RETURNING)
//!   agg     JSON_OBJECTAGG / JSON_ARRAYAGG over ordered deterministic
//!           sources (VALUES rows in fixed order; fz_rich keyed by pk;
//!           JSON_ARRAYAGG always carries ORDER BY)
//!   ctor    JSON() / JSON_SCALAR / JSON_SERIALIZE with FORMAT/UNIQUE-KEYS/
//!           RETURNING variants
//!   exists  JSON_EXISTS (PASSING, TRUE/FALSE/UNKNOWN/ERROR ON ERROR)
//!   value   JSON_VALUE (RETURNING types, DEFAULT/ERROR/NULL ON EMPTY and
//!           ON ERROR; no float8 RETURNING — engine-computed float text
//!           stays off the raw-compare surface, same law as dtm)
//!   query   JSON_QUERY (WITH/WITHOUT [ARRAY|CONDITIONAL] WRAPPER,
//!           KEEP/OMIT QUOTES, EMPTY ARRAY/OBJECT/DEFAULT/ERROR ON
//!           EMPTY/ON ERROR, RETURNING)
//!   isjson  IS [NOT] JSON [VALUE|SCALAR|ARRAY|OBJECT] [WITH/WITHOUT
//!           UNIQUE KEYS] over literals and fz_rich text/json columns
//!   table   JSON_TABLE in FROM: typed columns, FOR ORDINALITY, PATH,
//!           EXISTS, FORMAT JSON, per-column ON EMPTY/ON ERROR, one
//!           NESTED PATH level; literal-document form and the LATERAL
//!           join against fz_rich (ORDER BY pk + ordinality columns —
//!           every rowset is deterministically ordered)
//!   pathdt  jsonpath datetime item methods (.datetime()/.datetime(tmpl)/
//!           .date()/.time()/.time_tz()/.timestamp()/.timestamp_tz(),
//!           with precision forms) via jsonb_path_query{,_array,_first}
//!           and the _tz function family (session TimeZone is pinned UTC
//!           on both sides — runner::DATETIME_GUC_PIN), plus datetime
//!           filter comparisons — the executeDateTimeMethod target
//!   pathm   scalar item methods (.number()/.decimal(p,s)/.bigint()/
//!           .integer()/.boolean()/.string()/.abs()/.ceiling()/.floor()/
//!           .type()/.size()/.keyvalue()) — numeric/text-valued only,
//!           .double() is deliberately absent (float-text law)
//!   err     matched-error fuel, low weight: shapes hand-verified to
//!           raise byte-identical errors on both engines
//!
//! Determinism: literal-driven except the fz_rich-sourced forms, which
//! always ORDER BY pk (+ ordinality columns for JSON_TABLE). No now()/
//! current_*; json (not jsonb) output preserves construction order, which
//! is identical on both sides by construction. All documents/paths are
//! quote-free single-line strings with balanced parens.

use crate::stmt::{Gen, StmtKind};

/// jsonb/json document pool: objects, arrays, scalars, nesting, datetime
/// strings, duplicate keys, numeric edges. Single-quoted verbatim into
/// SQL; no single quotes or backslashes inside.
const DOCS: &[&str] = &[
    "{}",
    "[]",
    "null",
    "true",
    "0",
    "-1.5e2",
    "\"scalar\"",
    "{\"a\": 1, \"b\": \"x\"}",
    "{\"a\": {\"b\": {\"c\": [1, 2, 3]}}}",
    "{\"a\": [1, 2], \"b\": [], \"k\": null}",
    "{\"a\": 1, \"a\": 2}",
    "[{\"a\": 1}, {\"a\": 2}, {\"b\": 3}]",
    "[[1, 2], [3], []]",
    "[1, \"two\", null, true, {\"k\": \"v\"}]",
    "{\"num\": 123.456, \"big\": 9223372036854775807, \"neg\": -0.001}",
    "{\"d\": \"2024-02-29\", \"t\": \"12:34:56\", \"ts\": \"2024-02-29 12:00:00\", \"tstz\": \"2024-02-29 12:00:00+05:30\"}",
    "[\"2020-01-01\", \"2024-06-15\", \"1999-12-31\"]",
    "{\"k\": \"caf\\u00e9\", \"emp\": \"\", \"ws\": \" \"}",
    "[0.1, 0.2, 0.30000000000000004]",
];

/// Datetime strings for the executeDateTimeMethod arm: dates, times with
/// fractional seconds, offsets (incl. 30/45-minute), BC, extremes, and a
/// couple of non-ISO spellings for the template form.
const DT_STRINGS: &[&str] = &[
    "2024-02-29",
    "1970-01-01",
    "0001-01-01",
    "9999-12-31",
    "12:34:56",
    "23:59:59.999999",
    "24:00:00",
    "12:34:56.789",
    "12:34:56+05:30",
    "00:00:01-08:00",
    "2024-02-29 12:00:00",
    "2038-01-19 03:14:08",
    "2024-02-29 12:00:00.123456",
    "2021-11-07 01:30:00+00",
    "2019-12-31 18:30:00+05:45",
    "2024-06-01T08:30:00Z",
];

/// Scalar value expressions for constructor arguments (typed literals
/// only; no engine-computed floats).
const VALS: &[&str] = &[
    "1",
    "-42",
    "2.5",
    "12345678901234567890",
    "'x'",
    "'two words'",
    "''",
    "true",
    "false",
    "NULL",
    "DATE '2024-02-29'",
    "TIMESTAMP '2020-06-01 12:00:00'",
    "123.45::numeric(6,2)",
    "'k'::varchar(3)",
];

/// Object keys; 'dup' appears twice as often to manufacture duplicate-key
/// traffic for the UNIQUE KEYS arms.
const KEYS: &[&str] = &["a", "b", "k", "key0", "dup", "dup"];

/// RETURNING clause pool for the constructor family (hand-verified on
/// both engines, incl. bytea).
const RETURNING: &[&str] = &["json", "jsonb", "text", "bytea", "varchar(64)"];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_sqljson_module(g: &mut Gen) -> Vec<StmtKind> {
    let sql = match g.weights.pick(
        g.rng,
        &[
            "sqljson:obj",
            "sqljson:arr",
            "sqljson:agg",
            "sqljson:ctor",
            "sqljson:exists",
            "sqljson:value",
            "sqljson:query",
            "sqljson:isjson",
            "sqljson:table",
            "sqljson:pathdt",
            "sqljson:pathm",
            "sqljson:err",
            // Q2 sqljson-tweaks breadth (sql-reachable-queue chunk, 204
            // fns): populate_record family, json (not jsonb) operator
            // parity, jsonb mutation paths, jsonb scalar casts, ts-over-
            // json transforms, SQL/JSON constructors duplicated across
            // identical WINDOW specs (raw-tree equality), and JSON_TABLE
            // views + pg_get_viewdef deparse. Hand-verified byte-identical
            // on both engines (scratchpad hv-sj leg, 2026-08-11).
            "sqljson:pop",
            "sqljson:jops",
            "sqljson:mut",
            "sqljson:cast",
            "sqljson:jts",
            "sqljson:windup",
            "sqljson:viewdef",
        ],
    ) {
        // Q2 shapes that emit whole statement groups return early.
        "sqljson:pop" => return gen_pop(g),
        "sqljson:viewdef" => return gen_viewdef(g),
        "sqljson:obj" => gen_obj(g),
        "sqljson:arr" => gen_arr(g),
        "sqljson:agg" => gen_agg(g),
        "sqljson:ctor" => gen_ctor(g),
        "sqljson:exists" => gen_exists(g),
        "sqljson:value" => gen_value(g),
        "sqljson:query" => gen_query(g),
        "sqljson:isjson" => gen_isjson(g),
        "sqljson:table" => gen_table(g),
        "sqljson:pathdt" => gen_pathdt(g),
        "sqljson:pathm" => gen_pathm(g),
        "sqljson:jops" => gen_jops(g),
        "sqljson:mut" => gen_mut(g),
        "sqljson:cast" => gen_cast(g),
        "sqljson:jts" => gen_jts(g),
        "sqljson:windup" => gen_windup(g),
        _ => gen_err(g),
    };
    vec![StmtKind::Raw(sql)]
}

/// A jsonb document operand: literal, or (1/5) the fz_rich jsonb columns
/// via a pk-pinned scalar subquery (single row, deterministic).
fn doc(g: &mut Gen) -> String {
    if g.rng.chance(1, 5) {
        let col = *g.rng.pick(&["r_jsonb", "r_jsonb_b"]);
        let pk = 1 + g.rng.below(8);
        format!("(SELECT {} FROM fz_rich WHERE pk = {})", col, pk)
    } else {
        format!("jsonb '{}'", g.rng.pick(DOCS))
    }
}

/// Context-item jsonpath over the DOCS shapes; reuses the T1 jsonpath
/// generator (rich::gen_jsonpath) 1/3 of the time for cross-pollination.
fn path(g: &mut Gen) -> String {
    if g.rng.chance(1, 3) {
        return g.gen_jsonpath();
    }
    let mode = match g.rng.below(4) {
        0 => "lax ",
        1 => "strict ",
        _ => "",
    };
    let body = *g.rng.pick(&[
        "$",
        "$.a",
        "$.a.b.c",
        "$.a[1]",
        "$[*]",
        "$.a[*]",
        "$[last]",
        "$.*",
        "$.k",
        "$.d",
        "$.num",
        "$[0 to 1]",
        "$.a ? (@ > 1)",
        "$[*] ? (@.a == 1)",
    ]);
    format!("{}{}", mode, body)
}

/// ON EMPTY / ON ERROR suffix shared by value/query (behavior keyword set
/// differs per caller).
fn on_clause(g: &mut Gen, behaviors: &[&str], when: &str) -> String {
    if g.rng.chance(1, 2) {
        format!(" {} ON {}", g.rng.pick(behaviors), when)
    } else {
        String::new()
    }
}

fn gen_obj(g: &mut Gen) -> String {
    g.fire("sqljson:obj");
    let n = g.rng.below(4);
    let mut entries = Vec::new();
    for _ in 0..n {
        let k = *g.rng.pick(KEYS);
        let v = if g.rng.chance(1, 6) {
            format!("'{}' FORMAT JSON", g.rng.pick(DOCS))
        } else {
            g.rng.pick(VALS).to_string()
        };
        if g.rng.chance(1, 4) {
            entries.push(format!("'{}' VALUE {}", k, v));
        } else {
            entries.push(format!("'{}': {}", k, v));
        }
    }
    let mut s = entries.join(", ");
    if n > 0 && g.rng.chance(1, 3) {
        s.push_str(if g.rng.chance(1, 2) { " NULL ON NULL" } else { " ABSENT ON NULL" });
    }
    if n > 0 && g.rng.chance(1, 4) {
        s.push_str(if g.rng.chance(1, 3) { " WITH UNIQUE KEYS" } else { " WITHOUT UNIQUE KEYS" });
    }
    if g.rng.chance(1, 3) {
        if !s.is_empty() {
            s.push(' ');
        }
        s.push_str(&format!("RETURNING {}", g.rng.pick(RETURNING)));
    }
    format!("SELECT JSON_OBJECT({});", s)
}

fn gen_arr(g: &mut Gen) -> String {
    g.fire("sqljson:arr");
    let mut s = if g.rng.chance(1, 4) {
        // Subquery form (single column, deterministically ordered source).
        match g.rng.below(3) {
            0 => "SELECT i FROM generate_series(1, 4) i".to_string(),
            1 => "SELECT v FROM (VALUES (3), (1), (2)) AS t (v) ORDER BY v".to_string(),
            _ => "SELECT pk FROM fz_rich ORDER BY pk".to_string(),
        }
    } else {
        let n = g.rng.below(5);
        let mut items = Vec::new();
        for _ in 0..n {
            if g.rng.chance(1, 6) {
                items.push(format!("'{}' FORMAT JSON", g.rng.pick(DOCS)));
            } else {
                items.push(g.rng.pick(VALS).to_string());
            }
        }
        let mut s = items.join(", ");
        if n > 0 && g.rng.chance(1, 3) {
            s.push_str(if g.rng.chance(1, 2) { " NULL ON NULL" } else { " ABSENT ON NULL" });
        }
        s
    };
    if g.rng.chance(1, 3) {
        if !s.is_empty() {
            s.push(' ');
        }
        s.push_str(&format!("RETURNING {}", g.rng.pick(RETURNING)));
    }
    format!("SELECT JSON_ARRAY({});", s)
}

fn gen_agg(g: &mut Gen) -> String {
    g.fire("sqljson:agg");
    match g.rng.below(6) {
        // JSON_OBJECTAGG over fixed-order VALUES (key order = input order
        // on both sides; duplicate keys feed the UNIQUE KEYS arms).
        0 | 1 => {
            let rows = *g.rng.pick(&[
                "('a', 1), ('b', 2), ('c', 3)",
                "('a', 1), ('a', 2), ('b', NULL)",
                "('k', NULL), ('b', 0)",
            ]);
            let mut s = if g.rng.chance(1, 4) { "k VALUE v".to_string() } else { "k: v".to_string() };
            if g.rng.chance(1, 3) {
                s.push_str(if g.rng.chance(1, 2) { " NULL ON NULL" } else { " ABSENT ON NULL" });
            }
            if g.rng.chance(1, 4) {
                s.push_str(if g.rng.chance(1, 2) { " WITH UNIQUE KEYS" } else { " WITHOUT UNIQUE KEYS" });
            }
            if g.rng.chance(1, 3) {
                s.push_str(&format!(" RETURNING {}", if g.rng.chance(1, 2) { "jsonb" } else { "text" }));
            }
            format!("SELECT JSON_OBJECTAGG({}) FROM (VALUES {}) AS t (k, v);", s, rows)
        }
        // JSON_OBJECTAGG over fz_rich (text keys, pk-deduped, NULL keys
        // filtered — a NULL object key is the err production's job).
        2 => format!(
            "SELECT JSON_OBJECTAGG(k_text VALUE pk {}) FROM (SELECT k_text, pk FROM fz_rich WHERE k_text IS NOT NULL ORDER BY pk) AS s;",
            if g.rng.chance(1, 2) { "ABSENT ON NULL" } else { "NULL ON NULL" },
        ),
        // JSON_ARRAYAGG over VALUES, always ORDER BY (v distinct).
        3 | 4 => {
            let rows = *g.rng.pick(&[
                "(3), (1), (2)",
                "(10), (NULL), (-5)",
                "(0), (7), (NULL), (42)",
            ]);
            let onnull = match g.rng.below(3) {
                0 => " NULL ON NULL",
                1 => " ABSENT ON NULL",
                _ => "",
            };
            let ret = if g.rng.chance(1, 3) {
                format!(" RETURNING {}", *g.rng.pick(&["jsonb", "text"]))
            } else {
                String::new()
            };
            format!(
                "SELECT JSON_ARRAYAGG(v ORDER BY v{}{}) FROM (VALUES {}) AS t (v);",
                onnull, ret, rows
            )
        }
        // JSON_ARRAYAGG over fz_rich keyed by pk.
        _ => {
            let col = *g.rng.pick(&["pk", "k_int", "k_text", "r_jsonb"]);
            format!(
                "SELECT JSON_ARRAYAGG({} ORDER BY pk{}) FROM fz_rich;",
                col,
                if g.rng.chance(1, 3) { " RETURNING jsonb" } else { "" },
            )
        }
    }
}

fn gen_ctor(g: &mut Gen) -> String {
    g.fire("sqljson:ctor");
    match g.rng.below(6) {
        // JSON(text [FORMAT JSON] [WITH/WITHOUT UNIQUE KEYS])
        0 | 1 => {
            let d = *g.rng.pick(DOCS);
            let fmt = if g.rng.chance(1, 3) { " FORMAT JSON" } else { "" };
            let uniq = match g.rng.below(4) {
                0 => " WITH UNIQUE KEYS",
                1 => " WITHOUT UNIQUE KEYS",
                _ => "",
            };
            format!("SELECT JSON('{}'{}{});", d, fmt, uniq)
        }
        // JSON over bytea (UTF8 document bytes)
        2 => format!("SELECT JSON('{}'::bytea FORMAT JSON);", g.rng.pick(&["123", "[1,2]", "{\"a\":1}"])),
        // JSON_SCALAR over the typed-literal pool
        3 | 4 => format!("SELECT JSON_SCALAR({});", g.rng.pick(VALS)),
        // JSON_SERIALIZE with RETURNING text/bytea/varchar
        _ => {
            let src = if g.rng.chance(1, 3) {
                format!("JSON_SCALAR({})", g.rng.pick(VALS))
            } else {
                format!("'{}'", g.rng.pick(DOCS))
            };
            let ret = if g.rng.chance(1, 2) {
                format!(" RETURNING {}", *g.rng.pick(&["text", "bytea", "varchar(32)"]))
            } else {
                String::new()
            };
            format!("SELECT JSON_SERIALIZE({}{});", src, ret)
        }
    }
}

/// PASSING clause: typed literal variables usable from paths ($x, $y).
fn passing(g: &mut Gen) -> String {
    if g.rng.chance(1, 3) {
        let v = *g.rng.pick(&["0", "1", "2", "'x'", "true", "jsonb '{\"a\": 1}'"]);
        format!(" PASSING {} AS x", v)
    } else {
        String::new()
    }
}

fn gen_exists(g: &mut Gen) -> String {
    g.fire("sqljson:exists");
    // Table-sourced 1/3 of the time: whole-column sweep ordered by pk.
    if g.rng.chance(1, 3) {
        let col = *g.rng.pick(&["r_jsonb", "r_jsonb_b"]);
        let p = path(g);
        return format!(
            "SELECT pk, JSON_EXISTS({}, '{}') FROM fz_rich ORDER BY pk;",
            col, p
        );
    }
    let d = doc(g);
    let p = if g.rng.chance(1, 4) { "strict $.missing.deeper".to_string() } else { path(g) };
    let pass = passing(g);
    let onerr = on_clause(g, &["TRUE", "FALSE", "UNKNOWN", "ERROR"], "ERROR");
    format!("SELECT JSON_EXISTS({}, '{}'{}{});", d, p, pass, onerr)
}

fn gen_value(g: &mut Gen) -> String {
    g.fire("sqljson:value");
    let d = doc(g);
    let p = if g.rng.chance(1, 3) {
        // Scalar-yielding paths (JSON_VALUE wants a scalar).
        (*g.rng.pick(&["$.a", "$.num", "$.d", "$.ts", "$[0]", "$.k", "strict $.a.b"])).to_string()
    } else {
        path(g)
    };
    let ret = if g.rng.chance(1, 2) {
        format!(
            " RETURNING {}",
            *g.rng.pick(&["int", "bigint", "numeric", "numeric(6,2)", "text", "date", "timestamp", "boolean", "jsonb"])
        )
    } else {
        String::new()
    };
    let onempty = if g.rng.chance(1, 3) {
        format!(" {} ON EMPTY", *g.rng.pick(&["NULL", "ERROR", "DEFAULT 42", "DEFAULT 'd'"]))
    } else {
        String::new()
    };
    let onerr = on_clause(g, &["NULL", "ERROR", "DEFAULT 0", "DEFAULT 'e'"], "ERROR");
    format!("SELECT JSON_VALUE({}, '{}'{}{}{}{});", d, p, passing(g), ret, onempty, onerr)
}

fn gen_query(g: &mut Gen) -> String {
    g.fire("sqljson:query");
    let d = doc(g);
    let p = path(g);
    let wrapper = match g.rng.below(6) {
        0 => " WITH WRAPPER",
        1 => " WITH ARRAY WRAPPER",
        2 => " WITH CONDITIONAL WRAPPER",
        3 => " WITHOUT WRAPPER",
        4 => " WITHOUT ARRAY WRAPPER",
        _ => "",
    };
    // QUOTES only combines with non-wrapping forms.
    let quotes = if wrapper.is_empty() && g.rng.chance(1, 3) {
        *g.rng.pick(&[" KEEP QUOTES", " OMIT QUOTES", " OMIT QUOTES ON SCALAR STRING"])
    } else {
        ""
    };
    let ret = if g.rng.chance(1, 4) {
        format!(" RETURNING {}", *g.rng.pick(&["jsonb", "json", "text"]))
    } else {
        String::new()
    };
    let onempty = if g.rng.chance(1, 3) {
        format!(
            " {} ON EMPTY",
            *g.rng.pick(&["NULL", "ERROR", "EMPTY ARRAY", "EMPTY OBJECT", "DEFAULT jsonb '{\"d\": 1}'"])
        )
    } else {
        String::new()
    };
    let onerr = if g.rng.chance(1, 2) {
        format!(
            " {} ON ERROR",
            *g.rng.pick(&["NULL", "ERROR", "EMPTY ARRAY", "EMPTY OBJECT", "DEFAULT jsonb '[]'"])
        )
    } else {
        String::new()
    };
    format!(
        "SELECT JSON_QUERY({}, '{}'{}{}{}{}{}{});",
        d, p, passing(g), ret, wrapper, quotes, onempty, onerr
    )
}

fn gen_isjson(g: &mut Gen) -> String {
    g.fire("sqljson:isjson");
    let kind = *g.rng.pick(&["", " VALUE", " SCALAR", " ARRAY", " OBJECT"]);
    let not = if g.rng.chance(1, 4) { " NOT" } else { "" };
    let uniq = match g.rng.below(4) {
        0 => " WITH UNIQUE KEYS",
        1 => " WITHOUT UNIQUE KEYS",
        _ => "",
    };
    if g.rng.chance(1, 3) {
        let col = *g.rng.pick(&["k_text", "r_json", "r_jsonb", "r_bytea"]);
        format!(
            "SELECT pk, ({}) IS{} JSON{}{} FROM fz_rich ORDER BY pk;",
            col, not, kind, uniq
        )
    } else {
        let s = *g.rng.pick(&[
            "{\"a\": 1}",
            "{\"a\": 1, \"a\": 2}",
            "[1, 2]",
            "12.5",
            "\"str\"",
            "null",
            "not json at all",
            "{\"unterminated\": ",
            "",
            "  [true, false] ",
        ]);
        format!("SELECT ('{}') IS{} JSON{}{};", s, not, kind, uniq)
    }
}

fn gen_table(g: &mut Gen) -> String {
    g.fire("sqljson:table");
    // Column menu (hand-verified shapes; ordinality always present so the
    // rowset can be deterministically ordered).
    let coldefs = |g: &mut Gen| -> String {
        let mut cols = vec!["ord FOR ORDINALITY".to_string()];
        cols.push(match g.rng.below(4) {
            0 => "a int PATH '$.a'".to_string(),
            1 => "a int PATH '$.a' DEFAULT -1 ON EMPTY".to_string(),
            2 => "a numeric PATH '$.a' NULL ON ERROR".to_string(),
            _ => "a bigint PATH '$.num'".to_string(),
        });
        cols.push(match g.rng.below(4) {
            0 => "b text PATH '$.b'".to_string(),
            1 => "b text PATH '$.b' OMIT QUOTES".to_string(),
            2 => "b varchar(16) PATH '$.k' KEEP QUOTES".to_string(),
            _ => "b text PATH '$.d' DEFAULT 'none' ON EMPTY".to_string(),
        });
        if g.rng.chance(1, 2) {
            cols.push("e boolean EXISTS PATH '$.a'".to_string());
        }
        if g.rng.chance(1, 2) {
            cols.push(match g.rng.below(3) {
                0 => "j jsonb FORMAT JSON PATH '$'".to_string(),
                1 => "j jsonb FORMAT JSON PATH '$.a' WITH WRAPPER".to_string(),
                _ => "j json FORMAT JSON PATH '$.b' EMPTY OBJECT ON ERROR".to_string(),
            });
        }
        cols.join(", ")
    };
    match g.rng.below(4) {
        // Literal-document form (document order; ORDER BY ord anyway).
        0 | 1 => {
            let d = *g.rng.pick(&[
                "[{\"a\": 1, \"b\": \"x\"}, {\"a\": 2, \"b\": \"y\"}, {\"b\": \"z\"}]",
                "[{\"a\": 1, \"num\": 9007199254740993, \"d\": \"2020-01-01\"}]",
                "[{\"a\": \"oops\", \"b\": 7}, {}, {\"a\": 3.5, \"k\": \"q\"}]",
                "[]",
            ]);
            format!(
                "SELECT jt.* FROM JSON_TABLE(jsonb '{}', '$[*]' COLUMNS ({})) AS jt ORDER BY jt.ord;",
                d,
                coldefs(g)
            )
        }
        // NESTED PATH one level (parent + nested ordinality orders it).
        2 => {
            let d = *g.rng.pick(&[
                "{\"rows\": [{\"x\": [1, 2], \"b\": \"p\"}, {\"x\": [], \"b\": \"q\"}, {\"b\": \"r\"}]}",
                "{\"rows\": [{\"x\": [10], \"a\": 1}, {\"x\": [20, 30], \"a\": 2}]}",
            ]);
            format!(
                "SELECT jt.* FROM JSON_TABLE(jsonb '{}', '$.rows[*]' COLUMNS (ord FOR ORDINALITY, b text PATH '$.b', e boolean EXISTS PATH '$.x[0]', NESTED PATH '$.x[*]' COLUMNS (nord FOR ORDINALITY, xv int PATH '$'))) AS jt ORDER BY jt.ord, jt.nord;",
                d
            )
        }
        // LATERAL join against fz_rich (jsonb column drives the rows).
        _ => {
            let col = *g.rng.pick(&["r_jsonb", "r_jsonb_b"]);
            let join = if g.rng.chance(1, 2) {
                format!(
                    "FROM fz_rich AS r LEFT JOIN LATERAL JSON_TABLE(r.{}, '$[*]' COLUMNS ({})) AS jt ON true",
                    col,
                    coldefs(g)
                )
            } else {
                format!(
                    "FROM fz_rich AS r CROSS JOIN LATERAL JSON_TABLE(r.{}, 'lax $[*]' COLUMNS ({})) AS jt",
                    col,
                    coldefs(g)
                )
            };
            format!("SELECT r.pk, jt.* {} ORDER BY r.pk, jt.ord;", join)
        }
    }
}

fn gen_pathdt(g: &mut Gen) -> String {
    g.fire("sqljson:pathdt");
    let method = *g.rng.pick(&[
        ".datetime()",
        ".datetime(\"DD-MM-YYYY\")",
        ".datetime(\"YYYY-MM-DD HH24:MI:SS\")",
        ".date()",
        ".time()",
        ".time(2)",
        ".time_tz()",
        ".time_tz(3)",
        ".timestamp()",
        ".timestamp(3)",
        ".timestamp_tz()",
        ".timestamp_tz(0)",
    ]);
    match g.rng.below(6) {
        // jsonb_path_query family over a datetime string.
        0 | 1 => {
            let f = *g.rng.pick(&[
                "jsonb_path_query",
                "jsonb_path_query_array",
                "jsonb_path_query_first",
                "jsonb_path_query_tz",
                "jsonb_path_query_array_tz",
                "jsonb_path_query_first_tz",
            ]);
            let s = if method.starts_with(".datetime(\"DD") {
                "29-02-2024"
            } else {
                *g.rng.pick(DT_STRINGS)
            };
            format!("SELECT {}(jsonb '\"{}\"', '${}');", f, s, method)
        }
        // Whole-array sweep with a chained conversion.
        2 => {
            let chain = *g.rng.pick(&["", ".string()", ".type()"]);
            format!(
                "SELECT jsonb_path_query_array(jsonb '[\"2020-01-01\", \"2024-06-15\", \"1999-12-31\"]', '$[*]{}{}');",
                method, chain
            )
        }
        // Datetime filter comparison (the comparison arm of
        // executeDateTimeMethod; _tz variant exercises the UTC pin).
        3 => {
            let f = if g.rng.chance(1, 2) { "jsonb_path_query_array" } else { "jsonb_path_query_array_tz" };
            let bound = *g.rng.pick(&["2023-12-31", "2020-01-01", "2024-06-15"]);
            format!(
                "SELECT {}(jsonb '[\"2020-01-01\", \"2024-06-15\", \"1999-12-31\"]', '$[*] ? (@.datetime() >= \"{}\".datetime())');",
                f, bound
            )
        }
        // JSON_VALUE / JSON_QUERY carrying the datetime method (the
        // JsonExpr executor route into executeDateTimeMethod).
        4 => {
            let s = *g.rng.pick(DT_STRINGS);
            let ret = *g.rng.pick(&["", " RETURNING text", " RETURNING date", " RETURNING timestamp"]);
            let onerr = *g.rng.pick(&["", " NULL ON ERROR", " ERROR ON ERROR"]);
            format!("SELECT JSON_VALUE(jsonb '\"{}\"', '${}.string()'{}{});", s, method, ret, onerr)
        }
        _ => {
            let s = *g.rng.pick(DT_STRINGS);
            format!(
                "SELECT JSON_QUERY(jsonb '[\"{}\"]', '$[*]{}.string()' WITH WRAPPER);",
                s, method
            )
        }
    }
}

fn gen_pathm(g: &mut Gen) -> String {
    g.fire("sqljson:pathm");
    let (d, m): (&str, &str) = match g.rng.below(10) {
        0 => ("\"123.45\"", ".number()"),
        1 => ("\"123\"", ".bigint()"),
        2 => ("\"-7\"", ".integer()"),
        3 => ("\"12.9\"", ".decimal(4,1)"),
        4 => ("123.456", ".decimal(6,2)"),
        5 => ("[1, \"true\", \"false\", 0]", "[*].boolean()"),
        6 => ("[42, true, \"s\"]", "[*].string()"),
        7 => ("{\"a\": 1, \"b\": [2], \"k\": null}", ".keyvalue()"),
        8 => ("[-2.5, 1.3, -0.5]", "[*].abs().ceiling().floor()"),
        _ => ("[1, \"x\", {}, [], null, true]", "[*].type()"),
    };
    let f = *g.rng.pick(&["jsonb_path_query", "jsonb_path_query_array"]);
    if g.rng.chance(1, 6) {
        // .size() over structures.
        return format!("SELECT jsonb_path_query(jsonb '{}', '$.size()');", *g.rng.pick(&["[1, 2, 3]", "{}", "\"s\"", "[]"]));
    }
    format!("SELECT {}(jsonb '{}', '${}');", f, d, m)
}

/// Matched-error surfaces (every shape hand-verified byte-identical on
/// both engines before banking); low weight.
fn gen_err(g: &mut Gen) -> String {
    g.fire("sqljson:err");
    let s = *g.rng.pick(&[
        // 22032 invalid json text
        "SELECT JSON('{\"broken');",
        "SELECT JSON('[1, 2' FORMAT JSON);",
        // duplicate keys under WITH UNIQUE KEYS
        "SELECT JSON('{\"a\": 1, \"a\": 2}' WITH UNIQUE KEYS);",
        "SELECT JSON_OBJECT('dup': 1, 'dup': 2 WITH UNIQUE KEYS);",
        "SELECT JSON_OBJECTAGG(k: v WITH UNIQUE KEYS) FROM (VALUES ('a', 1), ('a', 2)) AS t (k, v);",
        // null object key
        "SELECT JSON_OBJECTAGG(k: v) FROM (VALUES ('a', 1), (NULL, 2)) AS t (k, v);",
        // RETURNING type mismatches
        "SELECT JSON_VALUE(jsonb '{\"a\": \"x\"}', '$.a' RETURNING int ERROR ON ERROR);",
        "SELECT JSON_VALUE(jsonb '\"2024-02-30\"', '$' RETURNING date ERROR ON ERROR);",
        "SELECT JSON_QUERY(jsonb '{\"a\": 1}', '$.a' RETURNING date ERROR ON ERROR);",
        "SELECT JSON_SERIALIZE('{\"a\": 1}' RETURNING int);",
        // structural errors under ERROR ON ERROR
        "SELECT JSON_VALUE(jsonb '[1, 2]', '$' ERROR ON ERROR);",
        "SELECT JSON_QUERY(jsonb '{}', 'strict $.a' ERROR ON ERROR);",
        "SELECT JSON_EXISTS(jsonb '{\"a\": 1}', 'strict $.b.c' ERROR ON ERROR);",
        "SELECT * FROM JSON_TABLE(jsonb '[1, \"e\"]', '$[*]' COLUMNS (ord FOR ORDINALITY, v int PATH '$' ERROR ON ERROR)) AS jt ORDER BY jt.ord;",
        // jsonpath syntax / conversion errors
        "SELECT JSON_EXISTS(jsonb '{}', '$.a[abc]');",
        "SELECT jsonb_path_query(jsonb '\"not-a-date\"', '$.datetime()');",
        "SELECT jsonb_path_query(jsonb '\"2024-02-30\"', '$.date()');",
        "SELECT jsonb_path_query(jsonb '\"99999999999999999999\"', '$.bigint()');",
        "SELECT jsonb_path_query(jsonb '\"x\"', '$.number()');",
        "SELECT jsonb_path_query(jsonb '\"12345\"', '$.decimal(3)');",
        // tz-less string into a tz method without the _tz family
        "SELECT jsonb_path_query(jsonb '\"2024-02-29\"', '$.timestamp_tz()');",
        "SELECT jsonb_path_query(jsonb '\"12:00:00\"', '$.time_tz()');",
    ]);
    s.to_string()
}

// ================================================================ Q2 ====
// sqljson-tweaks breadth. All literal-driven; brackets (pop/viewdef) are
// self-contained fixed-name groups; every family hand-verified on both
// engines (hv-sj leg) before banking.

/// json documents shaped for record population (objects with a/b/c/d/n
/// fields; arrays of such objects for the recordset forms).
const POP_OBJS: &[&str] = &[
    "{\"a\": 1, \"b\": \"x\", \"c\": [1, 2]}",
    "{\"a\": -7, \"c\": [3]}",
    "{\"b\": \"only-b\"}",
    "{\"a\": 1, \"b\": null, \"c\": null}",
    "{\"a\": 2, \"b\": \"y\", \"c\": [9, 8], \"extra\": true}",
    "{}",
];
const POP_SETS: &[&str] = &[
    "[{\"a\": 1}, {\"b\": \"y\"}]",
    "[{\"a\": 1, \"c\": [9, 8]}, {}, {\"a\": 3, \"b\": \"z\"}]",
    "[]",
];

/// populate_record family over a fuzz composite type (int/text/int[]
/// fields, then a domain + nested-composite variant), plus the
/// json(b)_to_record coldeflist forms. One self-contained bracket.
fn gen_pop(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("sqljson:pop");
    let jb = if g.rng.chance(1, 2) { "jsonb" } else { "json" };
    let obj = *g.rng.pick(POP_OBJS);
    let set = *g.rng.pick(POP_SETS);
    match g.rng.below(4) {
        0 => vec![
            StmtKind::Raw("DROP TYPE IF EXISTS fz_sj_ct CASCADE;".to_string()),
            StmtKind::Raw("CREATE TYPE fz_sj_ct AS (a int, b text, c int[]);".to_string()),
            StmtKind::Raw(format!(
                "SELECT ({}_populate_record(NULL::fz_sj_ct, '{}')).*;",
                jb, obj
            )),
            StmtKind::Raw(format!(
                "SELECT * FROM {}_populate_recordset(NULL::fz_sj_ct, '{}');",
                jb, set
            )),
            StmtKind::Raw(format!(
                "SELECT jsonb_populate_record_valid(NULL::fz_sj_ct, '{}')::text;",
                *g.rng.pick(&["{\"a\": \"notanint\"}", "{\"a\": 5}"])
            )),
            StmtKind::Raw("DROP TYPE fz_sj_ct;".to_string()),
        ],
        1 => {
            // Domain + nested-composite fields (incl. a failing domain
            // check on the err path 1/4 of the time — matched 23514).
            let doc = if g.rng.chance(1, 4) {
                "{\"d\": -5}"
            } else {
                "{\"d\": 5, \"n\": {\"x\": 2}}"
            };
            vec![
                StmtKind::Raw("DROP TYPE IF EXISTS fz_sj_ct2 CASCADE;".to_string()),
                StmtKind::Raw("DROP TYPE IF EXISTS fz_sj_ct3 CASCADE;".to_string()),
                StmtKind::Raw("DROP DOMAIN IF EXISTS fz_sj_dom CASCADE;".to_string()),
                StmtKind::Raw("CREATE DOMAIN fz_sj_dom AS int CHECK (VALUE > 0);".to_string()),
                StmtKind::Raw("CREATE TYPE fz_sj_ct3 AS (x int);".to_string()),
                StmtKind::Raw("CREATE TYPE fz_sj_ct2 AS (d fz_sj_dom, n fz_sj_ct3);".to_string()),
                StmtKind::Raw(format!(
                    "SELECT ({}_populate_record(NULL::fz_sj_ct2, '{}')).*;",
                    jb, doc
                )),
                StmtKind::Raw("DROP TYPE fz_sj_ct2;".to_string()),
                StmtKind::Raw("DROP TYPE fz_sj_ct3;".to_string()),
                StmtKind::Raw("DROP DOMAIN fz_sj_dom;".to_string()),
            ]
        }
        2 => vec![StmtKind::Raw(format!(
            "SELECT * FROM {}_to_record('{}') AS r(a int, b text, c int[]);",
            jb, obj
        ))],
        _ => vec![StmtKind::Raw(format!(
            "SELECT * FROM {}_to_recordset('{}') AS r(a int, b text);",
            jb, set
        ))],
    }
}

/// json (not jsonb) operator/SRF parity + shared function breadth:
/// -> ->> #> #>>, array_length, typeof, strip_nulls, to_json family,
/// build_array/object (incl zero-arg), json_object(text[]).
fn gen_jops(g: &mut Gen) -> String {
    g.fire("sqljson:jops");
    let d = *g.rng.pick(DOCS);
    match g.rng.below(8) {
        0 => {
            let k = *g.rng.pick(&["'a'", "'k'", "'missing'", "0", "2", "-1"]);
            format!("SELECT (json '{}' -> {})::text, (json '{}' ->> {});", d, k, d, k)
        }
        1 => {
            let p = *g.rng.pick(&["{a}", "{a,b}", "{a,b,c}", "{a,0}", "{k}", "{}"]);
            format!(
                "SELECT (json '{}' #> '{}')::text, (json '{}' #>> '{}');",
                d, p, d, p
            )
        }
        2 => format!(
            "SELECT json_array_length(json '{}'), json_typeof(json '{}');",
            *g.rng.pick(&["[1, 2, 3]", "[]", "[[1], [2]]"]),
            d
        ),
        3 => {
            let strip = *g.rng.pick(&[
                "{\"a\": 1, \"b\": null, \"c\": {\"d\": null}}",
                "[{\"a\": null}, null]",
                "{\"a\": null}",
            ]);
            let flag = *g.rng.pick(&["", ", true", ", false"]);
            format!(
                "SELECT json_strip_nulls('{}'{})::text, jsonb_strip_nulls('{}'{})::text;",
                strip, flag, strip, flag
            )
        }
        4 => format!(
            "SELECT to_json(ROW({}, '{}'))::text, row_to_json(ROW({}, 'x'), {})::text, array_to_json(ARRAY[{}, {}], {})::text;",
            g.rng.below(9),
            *g.rng.pick(&["a", "b"]),
            g.rng.below(9),
            *g.rng.pick(&["true", "false"]),
            g.rng.below(9),
            g.rng.below(9),
            *g.rng.pick(&["true", "false"])
        ),
        5 => {
            let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
            if g.rng.chance(1, 3) {
                format!("SELECT {}_build_array()::text, {}_build_object()::text;", jb, jb)
            } else {
                format!(
                    "SELECT {}_build_array(1, 'a', NULL, true)::text, {}_build_object('k', {}, 'j', NULL)::text;",
                    jb, jb, g.rng.below(9)
                )
            }
        }
        6 => {
            let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
            if g.rng.chance(1, 2) {
                format!("SELECT {}_object('{{a,1,b,2}}')::text;", jb)
            } else {
                format!("SELECT {}_object('{{a,b}}', '{{1,2}}')::text;", jb)
            }
        }
        _ => format!(
            "SELECT to_jsonb(ROW({}, '{}'))::text, to_jsonb(ARRAY[ROW(1, 'a')])::text, jsonb_pretty(jsonb '{}');",
            g.rng.below(9),
            *g.rng.pick(&["a", "b"]),
            *g.rng.pick(&["{\"a\": [1, {\"b\": 2}]}", "[]", "{\"k\": null}"])
        ),
    }
}

/// jsonb mutation paths: jsonb_set/set_lax/insert/delete(path)/#- and
/// the - operators.
fn gen_mut(g: &mut Gen) -> String {
    g.fire("sqljson:mut");
    let base = *g.rng.pick(&[
        "{\"a\": [1, 2]}",
        "{\"a\": 1, \"b\": {\"c\": 2}}",
        "[\"x\", \"y\", \"z\"]",
        "{\"a\": {\"b\": [1, 2, 3]}}",
    ]);
    let path = *g.rng.pick(&["{a}", "{a,1}", "{a,b}", "{b,c}", "{a,-1}", "{0}"]);
    let newval = *g.rng.pick(&["9", "\"nv\"", "null", "[7]", "{\"z\": 1}"]);
    match g.rng.below(6) {
        0 => format!(
            "SELECT jsonb_set('{}', '{}', '{}'{})::text;",
            base,
            path,
            newval,
            *g.rng.pick(&["", ", true", ", false"])
        ),
        1 => format!(
            "SELECT jsonb_set_lax('{}', '{}', NULL, true, '{}')::text;",
            base,
            path,
            *g.rng.pick(&["use_json_null", "delete_key", "return_target"])
        ),
        2 => format!(
            "SELECT jsonb_insert('{}', '{}', '{}'{})::text;",
            base,
            path,
            newval,
            *g.rng.pick(&["", ", true", ", false"])
        ),
        3 => format!(
            "SELECT ('{}'::jsonb - '{}')::text, ('{}'::jsonb - {})::text;",
            base,
            *g.rng.pick(&["a", "b", "zz"]),
            "[\"x\", \"y\"]",
            g.rng.below(3)
        ),
        4 => format!("SELECT ('{}'::jsonb #- '{}')::text;", base, path),
        _ => format!("SELECT jsonb_delete_path('{}', '{}')::text;", base, path),
    }
}

/// jsonb scalar casts (bool/int2/int4/int8/float4/float8/numeric), incl.
/// the matched error paths (string->int, overflow).
fn gen_cast(g: &mut Gen) -> String {
    g.fire("sqljson:cast");
    if g.rng.chance(1, 5) {
        // Matched 22023: cannot cast type / out of range.
        return (*g.rng.pick(&[
            "SELECT ('\"x\"'::jsonb)::int;",
            "SELECT ('[1]'::jsonb)::numeric;",
            "SELECT ('99999999999'::jsonb)::int2;",
            "SELECT ('true'::jsonb)::numeric;",
            "SELECT ('{}'::jsonb)::bool;",
        ]))
        .to_string();
    }
    let (v, t) = match g.rng.below(7) {
        0 => ("true", "bool"),
        1 => ("42", "int"),
        2 => ("1.5", "numeric"),
        3 => ("3.25", "float8"),
        4 => ("32000", "int2"),
        5 => ("9999999999", "int8"),
        _ => ("2.5", "float4"),
    };
    format!("SELECT ('{}'::jsonb)::{}::text;", v, t)
}

/// Text-search transforms over json(b): to_tsvector/json(b)_to_tsvector
/// with filters, ts_headline with/without config + options.
fn gen_jts(g: &mut Gen) -> String {
    g.fire("sqljson:jts");
    let jb = if g.rng.chance(1, 2) { "json" } else { "jsonb" };
    let doc = *g.rng.pick(&[
        "{\"a\": \"the quick brown fox\", \"b\": [{\"c\": \"lazy dog\"}]}",
        "{\"a\": \"jumping foxes\", \"n\": 5}",
        "{\"a\": \"fox\", \"b\": true}",
    ]);
    match g.rng.below(5) {
        0 => format!("SELECT to_tsvector('{}'::{})::text;", doc, jb),
        1 => format!("SELECT to_tsvector('english', '{}'::{})::text;", doc, jb),
        2 => {
            let filter = *g.rng.pick(&["[\"string\"]", "[\"string\", \"numeric\"]", "[\"all\"]", "[\"key\", \"boolean\"]"]);
            format!(
                "SELECT {}_to_tsvector('english', '{}'::{}, '{}')::text;",
                jb, doc, jb, filter
            )
        }
        3 => {
            let filter = *g.rng.pick(&["[\"string\"]", "[\"all\"]"]);
            format!("SELECT {}_to_tsvector('{}'::{}, '{}')::text;", jb, doc, jb, filter)
        }
        _ => {
            let q = *g.rng.pick(&["'fox'::tsquery", "to_tsquery('english', 'fox')", "'dog'::tsquery"]);
            let opts = *g.rng.pick(&["", ", 'StartSel=<<, StopSel=>>'", ", 'MaxWords=5, MinWords=2'"]);
            if g.rng.chance(1, 2) {
                format!("SELECT ts_headline('{}'::{}, {}{})::text;", doc, jb, q, opts)
            } else {
                format!("SELECT ts_headline('english', '{}'::{}, {}{})::text;", doc, jb, q, opts)
            }
        }
    }
}

/// SQL/JSON constructors duplicated across identical WINDOW specs: the
/// raw-tree equality path (transformWindowDefinitions dedup) plus the
/// JSON_TABLE-subquery variant.
fn gen_windup(g: &mut Gen) -> String {
    g.fire("sqljson:windup");
    match g.rng.below(4) {
        0 => format!(
            "SELECT count(*) OVER w1, count(*) OVER w2 FROM (VALUES (1), (2), (3)) t(x) WINDOW w1 AS (PARTITION BY JSON_OBJECT('k': x)), w2 AS (PARTITION BY JSON_OBJECT('k': x)) ORDER BY x;"
        ),
        1 => format!(
            "SELECT max(x) OVER w1, min(x) OVER w2 FROM (VALUES (1), (2), (3)) t(x) WINDOW w1 AS (ORDER BY JSON_ARRAY(x{r}) ROWS UNBOUNDED PRECEDING), w2 AS (ORDER BY JSON_ARRAY(x{r}) ROWS UNBOUNDED PRECEDING) ORDER BY x;",
            r = *g.rng.pick(&["", " RETURNING jsonb"])
        ),
        2 => format!(
            "SELECT sum(x) OVER w1, count(*) OVER w2 FROM (VALUES (1), (2)) t(x) WINDOW w1 AS (PARTITION BY JSON_SCALAR(x)), w2 AS (PARTITION BY JSON_SCALAR(x)) ORDER BY x;"
        ),
        _ => format!(
            "SELECT sum(a) OVER w1, count(*) OVER w2 FROM (SELECT a FROM JSON_TABLE('[{{\"a\": 1}}, {{\"a\": 2}}]', '$[*]' COLUMNS (a int PATH '$.a')) jt) s WINDOW w1 AS (ORDER BY a), w2 AS (ORDER BY a) ORDER BY a;"
        ),
    }
}

/// Views over JSON_TABLE / SQL-JSON constructors / json_to_record
/// coldeflists + pg_get_viewdef deparse (plain and pretty), then a
/// deterministic SELECT through the view. Fixed name, one bracket.
fn gen_viewdef(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("sqljson:viewdef");
    let (create, select) = match g.rng.below(4) {
        0 => (
            "CREATE VIEW fz_sj_vw AS SELECT jt.*, JSON_OBJECT('k': jt.a) AS o FROM JSON_TABLE('[{\"a\": 1, \"b\": [1, 2]}]', '$[*]' COLUMNS (ord FOR ORDINALITY, a int PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b int PATH '$'))) jt;",
            "SELECT * FROM fz_sj_vw ORDER BY ord, b;",
        ),
        1 => (
            "CREATE VIEW fz_sj_vw AS SELECT * FROM json_to_record('{\"a\": 1, \"b\": \"x\"}') AS r(a int, b text);",
            "SELECT * FROM fz_sj_vw;",
        ),
        2 => (
            "CREATE VIEW fz_sj_vw AS SELECT JSON_ARRAY(1, 'a' NULL ON NULL) AS arr, JSON_OBJECT('x': 1 RETURNING jsonb) AS obj, JSON_EXISTS(jsonb '{\"a\": 1}', '$.a') AS ex;",
            "SELECT arr::text, obj::text, ex FROM fz_sj_vw;",
        ),
        _ => (
            "CREATE VIEW fz_sj_vw AS SELECT JSON_VALUE(jsonb '{\"a\": 7}', '$.a' RETURNING int DEFAULT -1 ON ERROR) AS v, JSON_QUERY(jsonb '{\"a\": [1, 2]}', '$.a' WITH ARRAY WRAPPER) AS q;",
            "SELECT v, q::text FROM fz_sj_vw;",
        ),
    };
    let mut out = vec![
        StmtKind::Raw("DROP VIEW IF EXISTS fz_sj_vw;".to_string()),
        StmtKind::Raw(create.to_string()),
        StmtKind::Raw("SELECT pg_get_viewdef('fz_sj_vw'::regclass);".to_string()),
    ];
    if g.rng.chance(1, 2) {
        out.push(StmtKind::Raw(
            "SELECT pg_get_viewdef('fz_sj_vw'::regclass, true);".to_string(),
        ));
    }
    out.push(StmtKind::Raw(select.to_string()));
    out.push(StmtKind::Raw("DROP VIEW fz_sj_vw;".to_string()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Textual invariants for every sqljson production: single line,
    /// ';'-terminated, balanced parens, no nondeterministic time sources,
    /// and every multi-row table-sourced statement carries ORDER BY.
    #[test]
    fn sqljson_statements_are_deterministic_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x1A51);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_sqljson_module(&mut g);
            // Q2 bracket shapes (pop/viewdef) emit whole groups; every
            // other shape is exactly one statement.
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
                // Every FROM over fz_rich (multi-row) must be ordered; the
                // pk-pinned scalar subquery form is single-row and exempt.
                if low.contains("from fz_rich") && !low.contains("where pk =") {
                    assert!(low.contains("order by"), "unordered fz_rich rowset: {sql}");
                }
            }
            for p in &prods {
                seen.insert(p.clone());
            }
        }
        for p in [
            "sqljson:obj", "sqljson:arr", "sqljson:agg", "sqljson:ctor",
            "sqljson:exists", "sqljson:value", "sqljson:query",
            "sqljson:isjson", "sqljson:table", "sqljson:pathdt",
            "sqljson:pathm", "sqljson:err",
            "sqljson:pop", "sqljson:jops", "sqljson:mut", "sqljson:cast",
            "sqljson:jts", "sqljson:windup", "sqljson:viewdef",
        ] {
            assert!(seen.contains(p), "production {p} never fired");
        }
    }

    /// Same seed -> byte-identical statements (the reproducibility law).
    #[test]
    fn sqljson_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(4242);
            let mut out = Vec::new();
            for _ in 0..500 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                out.push(gen_sqljson_module(&mut g)[0].to_sql());
            }
            out
        };
        assert_eq!(run(), run());
    }
}
