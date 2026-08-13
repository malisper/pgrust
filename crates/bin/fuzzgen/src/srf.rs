//! Set-returning-function / nodeFunctionscan drain module (Track-B SRF):
//! the generate_series / generate_subscripts / unnest / ROWS FROM / SRF-in-
//! target-list surface that the exec-residue lane (tablefunc/projectset via
//! JSON_TABLE + transition tables) left untouched. Targets
//! backend/executor/nodeFunctionscan.c (single-func value-per-call fast
//! path and multi-func / SETOF materialize path), funcapi.c
//! ValuePerCall/Materialize modes, and the SRF builtins themselves:
//! generate_series (int4/int8/timestamp/timestamptz+interval),
//! generate_subscripts, unnest (single, nested-array flatten, parallel
//! multi-array), string_to_table, plus the parser's SRF-context guards
//! (transformCaseExpr / COALESCE etc. -> 0A000).
//!
//! Determinism laws (spill/earm discipline):
//!   - every row-returning statement is either naturally ordered
//!     (generate_series emits ascending/descending by construction, matched
//!     on both engines) or carries a TOTAL order — FROM-item SRFs whose row
//!     order is not intrinsically total (unnest, ROWS FROM, parallel
//!     unnest) always ride WITH ORDINALITY and ORDER BY the ordinality
//!     column, which is a strictly-increasing bigint over the whole result.
//!   - all scalar outputs are cast ::text so the compare is byte-exact.
//!   - timestamp/timestamptz generate_series is deterministic under the
//!     session pin (runner::DATETIME_GUC_PIN: TimeZone=UTC, DateStyle=
//!     'ISO, MDY', IntervalStyle='postgres', applied to BOTH sides), so no
//!     self-bracket is needed; interval literals never span a DST edge
//!     (UTC has none) and every zone name is explicit.
//!   - SRF-in-target-list rows (no FROM) are emitted in the engine's
//!     deterministic set-expansion order (PG10+ lockstep-to-longest with
//!     NULL padding of the shorter columns — the ROWS FROM semantics); both
//!     engines run the identical algorithm, so the row sequence is
//!     byte-identical by construction (numx:series precedent — trust the
//!     builtin's natural order).
//!   - error arms (zero/NaN step, SRF in CASE/COALESCE) are part of the
//!     surface: emitted deliberately and matched on SQLSTATE (diff::classify)
//!     — 22023 for step==0 / non-finite step, 0A000 for the parser SRF
//!     guards.
//!
//! Numeric generate_series is deliberately NOT emitted here: it is the
//! numx module's `numx:series` production (LD9). This module owns the
//! int4/int8/timestamp/timestamptz arms plus the whole functionscan /
//! unnest / ROWS FROM / ordinality surface, which is disjoint from numx.
//!
//! Stateless: every pick is one self-contained statement group over
//! literal inputs; no fixtures, no catalog dependence.

use crate::stmt::{Gen, StmtKind};

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

/// Top-level production families (registered in weights::PROD_WEIGHTS).
const SHAPES: &[&str] = &[
    "srf:gs_int",       // generate_series int4/int8, +/- step, empty
    "srf:gs_ts",        // generate_series timestamp[tz] + interval
    "srf:gs_err",       // step==0 / non-finite -> 22023
    "srf:subscripts",   // generate_subscripts(anyarray, dim [, reverse])
    "srf:unnest1",      // unnest(array) single [+ WITH ORDINALITY]
    "srf:unnest_multi", // nested-array flatten + parallel unnest(a,b,...)
    "srf:rowsfrom",     // ROWS FROM (f1(), f2()) NULL-pad [+ ORDINALITY]
    "srf:tlist",        // SRF in target list (set-per-row / lockstep)
    "srf:lateral",      // LATERAL SRF over a correlated column
    "srf:s2t",          // string_to_table(text, delim [, null])
    "srf:case_err",     // SRF in CASE/COALESCE -> 0A000
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_srf_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("srf");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire(shape);
    match shape {
        "srf:gs_int" => gen_gs_int(g),
        "srf:gs_ts" => gen_gs_ts(g),
        "srf:gs_err" => gen_gs_err(g),
        "srf:subscripts" => gen_subscripts(g),
        "srf:unnest1" => gen_unnest1(g),
        "srf:unnest_multi" => gen_unnest_multi(g),
        "srf:rowsfrom" => gen_rowsfrom(g),
        "srf:tlist" => gen_tlist(g),
        "srf:lateral" => gen_lateral(g),
        "srf:s2t" => gen_s2t(g),
        _ => gen_case_err(g),
    }
}

// ---------------------------------------------------------------------------
// generate_series over integers (int4 fast path + int8), +/- step, empty.
// ---------------------------------------------------------------------------

fn gen_gs_int(g: &mut Gen) -> Vec<StmtKind> {
    // (start, stop, step-or-none, cast) tuples spanning: 2-arg default
    // step, positive/negative step, single-row, empty (start>stop with
    // positive step and vice versa), and the int8 arm.
    let cast = pick_str(g, &["int", "bigint"]);
    let (a, b, step): (&str, &str, Option<&str>) = match g.rng.below(9) {
        0 => ("1", "5", None),          // default step 1
        1 => ("0", "10", Some("2")),    // positive step
        2 => ("10", "1", Some("-3")),   // negative step, descending
        3 => ("5", "5", None),          // single row
        4 => ("5", "1", Some("1")),     // empty (asc step, start>stop)
        5 => ("1", "5", Some("-1")),    // empty (desc step, start<stop)
        6 => ("-4", "4", Some("1")),    // spans zero
        7 => ("-9223372036854775808", "-9223372036854775800", Some("2")), // int8 boundary
        _ => ("1", "7", Some("3")),     // ragged last step
    };
    let step_arg = step.map(|s| format!(", {s}::{cast}")).unwrap_or_default();
    // generate_series in FROM is ascending/descending by construction;
    // natural order is byte-identical across engines (numx:series law).
    vec![raw(format!(
        "SELECT g::text FROM generate_series({a}::{cast}, {b}::{cast}{step_arg}) g;"
    ))]
}

// ---------------------------------------------------------------------------
// generate_series over timestamp / timestamptz with an interval step.
// ---------------------------------------------------------------------------

fn gen_gs_ts(g: &mut Gen) -> Vec<StmtKind> {
    // Deterministic under the UTC session pin. Interval steps stay whole so
    // the row count is fixed; forward and backward walks both covered.
    let cast = pick_str(g, &["timestamp", "timestamptz"]);
    let (a, b, step) = match g.rng.below(6) {
        0 => ("2020-01-01 00:00:00", "2020-01-01 06:00:00", "2 hours"),
        1 => ("2021-03-01", "2021-03-05", "1 day"),
        2 => ("2020-01-01 06:00:00", "2020-01-01 00:00:00", "-2 hours"), // descending
        3 => ("2000-01-31", "2000-04-30", "1 month"),                    // month arithmetic
        4 => ("2019-12-31 23:00:00", "2020-01-01 02:00:00", "30 minutes"),
        _ => ("2020-02-28", "2020-03-02", "1 day"),                      // leap-year edge
    };
    vec![raw(format!(
        "SELECT g::text FROM generate_series('{a}'::{cast}, '{b}'::{cast}, interval '{step}') g;"
    ))]
}

// ---------------------------------------------------------------------------
// generate_series error arms (SQLSTATE-matched).
// ---------------------------------------------------------------------------

fn gen_gs_err(g: &mut Gen) -> Vec<StmtKind> {
    let e = pick_str(
        g,
        &[
            "SELECT generate_series(1::int, 5::int, 0::int);",          // 22023 step==0
            "SELECT generate_series(1::bigint, 5::bigint, 0::bigint);", // 22023 step==0
            "SELECT generate_series('2020-01-01'::timestamp, '2020-01-02'::timestamp, interval '0');", // 22023
            "SELECT generate_series('2020-01-01'::timestamptz, '2020-01-02'::timestamptz, interval '0 days');", // 22023
        ],
    );
    vec![raw(e.to_string())]
}

// ---------------------------------------------------------------------------
// generate_subscripts(anyarray, dim [, reverse]).
// ---------------------------------------------------------------------------

fn gen_subscripts(g: &mut Gen) -> Vec<StmtKind> {
    // Arrays with non-1 lower bounds and multiple dims exercise the
    // AARR_LBOUND / dim-out-of-range arms; reverse flag covers both walks.
    let arr = pick_str(
        g,
        &[
            "ARRAY[10,20,30]",
            "ARRAY[[1,2,3],[4,5,6]]",
            "'[3:5]={7,8,9}'::int[]",   // custom lower bound
            "'{}'::int[]",              // empty -> zero rows
            "ARRAY['a','b','c','d']",
        ],
    );
    let dim = pick_str(g, &["1", "2"]);
    let reverse = pick_str(g, &["", ", true", ", false"]);
    vec![raw(format!(
        "SELECT s::text FROM generate_subscripts({arr}, {dim}{reverse}) s ORDER BY s;"
    ))]
}

// ---------------------------------------------------------------------------
// unnest(array), single, with optional WITH ORDINALITY.
// ---------------------------------------------------------------------------

fn gen_unnest1(g: &mut Gen) -> Vec<StmtKind> {
    let arr = pick_str(
        g,
        &[
            "ARRAY[3,1,2]",
            "ARRAY['x','y',NULL,'z']",
            "'{}'::int[]",                 // empty
            "ARRAY[10.5, -2.25, 0]::numeric[]",
            "ARRAY[true, false, NULL]",
        ],
    );
    if g.rng.chance(1, 2) {
        // WITH ORDINALITY -> total order on the ordinality column.
        vec![raw(format!(
            "SELECT u.v::text, u.ord::text FROM unnest({arr}) WITH ORDINALITY AS u(v, ord) ORDER BY u.ord;"
        ))]
    } else {
        // Plain unnest: value order == array order, byte-identical.
        vec![raw(format!(
            "SELECT v::text FROM unnest({arr}) v;"
        ))]
    }
}

// ---------------------------------------------------------------------------
// nested-array flatten + parallel unnest(a, b, ...) with NULL-pad.
// ---------------------------------------------------------------------------

fn gen_unnest_multi(g: &mut Gen) -> Vec<StmtKind> {
    match g.rng.below(3) {
        0 => {
            // unnest of a multi-dim array flattens in row-major order.
            let arr = pick_str(
                g,
                &["ARRAY[[1,2],[3,4],[5,6]]", "ARRAY[['a','b'],['c','d']]"],
            );
            vec![raw(format!(
                "SELECT u.v::text, u.ord::text FROM unnest({arr}) WITH ORDINALITY AS u(v, ord) ORDER BY u.ord;"
            ))]
        }
        1 => {
            // Parallel unnest of unequal-length arrays: the shorter is
            // NULL-padded to the longest (ROWS FROM semantics).
            vec![raw(
                "SELECT u.a::text, u.b::text, u.ord::text \
                 FROM unnest(ARRAY[1,2,3,4], ARRAY['x','y']) WITH ORDINALITY AS u(a, b, ord) \
                 ORDER BY u.ord;"
                    .to_string(),
            )]
        }
        _ => {
            // Three parallel arrays, mixed lengths incl. an empty one.
            vec![raw(
                "SELECT u.a::text, u.b::text, u.c::text, u.ord::text \
                 FROM unnest(ARRAY[10,20], ARRAY[1.5,2.5,3.5]::numeric[], '{}'::text[]) \
                 WITH ORDINALITY AS u(a, b, c, ord) ORDER BY u.ord;"
                    .to_string(),
            )]
        }
    }
}

// ---------------------------------------------------------------------------
// ROWS FROM (f1(), f2()) with differing row counts -> NULL-pad.
// ---------------------------------------------------------------------------

fn gen_rowsfrom(g: &mut Gen) -> Vec<StmtKind> {
    match g.rng.below(3) {
        0 => vec![raw(
            "SELECT r.g::text, r.u::text, r.ord::text \
             FROM ROWS FROM (generate_series(1, 4), unnest(ARRAY['a','b'])) \
             WITH ORDINALITY AS r(g, u, ord) ORDER BY r.ord;"
                .to_string(),
        )],
        1 => vec![raw(
            "SELECT r.g::text, r.s::text, r.ord::text \
             FROM ROWS FROM (generate_series(10, 12), generate_subscripts(ARRAY[[1,2],[3,4]], 1)) \
             WITH ORDINALITY AS r(g, s, ord) ORDER BY r.ord;"
                .to_string(),
        )],
        _ => vec![raw(
            "SELECT r.a::text, r.b::text, r.ord::text \
             FROM ROWS FROM (unnest(ARRAY[1,2,3]), string_to_table('p,q', ',')) \
             WITH ORDINALITY AS r(a, b, ord) ORDER BY r.ord;"
                .to_string(),
        )],
    }
}

// ---------------------------------------------------------------------------
// SRF in the target list (set-per-row / lockstep-to-longest semantics).
// ---------------------------------------------------------------------------

fn gen_tlist(g: &mut Gen) -> Vec<StmtKind> {
    match g.rng.below(4) {
        0 => vec![raw(
            // Single SRF in the target list, no FROM: deterministic order.
            "SELECT generate_series(1, 4)::text;".to_string(),
        )],
        1 => vec![raw(
            // Two SRFs of unequal cardinality run in lockstep to the
            // longest, the shorter emitting NULL past its end (PG10+).
            "SELECT generate_series(1, 4)::text AS a, generate_series(1, 2)::text AS b;".to_string(),
        )],
        2 => vec![raw(
            // SRF over a per-row driver (outer generate_series): the SRF is
            // re-evaluated per outer row (value-per-call rescan).
            "SELECT i::text AS i, g::text AS g \
             FROM generate_series(1, 3) i, generate_series(1, i) g ORDER BY i, g;"
                .to_string(),
        )],
        _ => vec![raw(
            // unnest in the target list alongside a scalar.
            "SELECT 'k'::text AS k, unnest(ARRAY[5,6,7])::text AS v;".to_string(),
        )],
    }
}

// ---------------------------------------------------------------------------
// LATERAL SRF over a correlated column (ExecReScan per outer row).
// ---------------------------------------------------------------------------

fn gen_lateral(g: &mut Gen) -> Vec<StmtKind> {
    match g.rng.below(3) {
        0 => vec![raw(
            "SELECT v.x::text AS x, g::text AS g \
             FROM (VALUES (1), (3), (0)) AS v(x), LATERAL generate_series(1, v.x) g \
             ORDER BY v.x, g;"
                .to_string(),
        )],
        1 => vec![raw(
            "SELECT v.a::text AS a, u.v::text AS v, u.ord::text AS ord \
             FROM (VALUES (ARRAY[1,2]), (ARRAY[9])) AS v(a), \
             LATERAL unnest(v.a) WITH ORDINALITY AS u(v, ord) ORDER BY v.a, u.ord;"
                .to_string(),
        )],
        _ => vec![raw(
            // LEFT JOIN LATERAL: outer row with an empty SRF result is
            // preserved (NULL-extended), unlike the comma form.
            "SELECT v.x::text AS x, g::text AS g \
             FROM (VALUES (2), (0)) AS v(x) \
             LEFT JOIN LATERAL generate_series(1, v.x) g ON true ORDER BY v.x, g;"
                .to_string(),
        )],
    }
}

// ---------------------------------------------------------------------------
// string_to_table(text, delimiter [, null_string]).
// ---------------------------------------------------------------------------

fn gen_s2t(g: &mut Gen) -> Vec<StmtKind> {
    let (s, d, n): (&str, &str, Option<&str>) = match g.rng.below(6) {
        0 => ("'a,b,c'", "','", None),
        1 => ("'a,b,,d'", "','", Some("'b'")),     // null_string maps to NULL
        2 => ("'xxTyyTzz'", "'T'", None),
        3 => ("'abc'", "NULL", None),               // NULL delim -> per-char
        4 => ("''", "','", None),                   // empty input -> one empty row
        _ => ("'a,,c'", "','", Some("''")),         // empty null_string
    };
    let null_arg = n.map(|x| format!(", {x}")).unwrap_or_default();
    vec![raw(format!(
        "SELECT t.v::text, t.ord::text \
         FROM string_to_table({s}, {d}{null_arg}) WITH ORDINALITY AS t(v, ord) ORDER BY t.ord;"
    ))]
}

// ---------------------------------------------------------------------------
// SRF in a disallowed context -> 0A000 (parser guard).
// ---------------------------------------------------------------------------

fn gen_case_err(g: &mut Gen) -> Vec<StmtKind> {
    let e = pick_str(
        g,
        &[
            "SELECT CASE WHEN true THEN generate_series(1, 3) ELSE 0 END;", // 0A000 in CASE
            "SELECT COALESCE(generate_series(1, 3), 0);",                    // 0A000 in COALESCE
            "SELECT GREATEST(generate_series(1, 3), 5);",                    // 0A000 in GREATEST
            "SELECT 1 WHERE generate_series(1, 3) > 1;",                     // 0A000 in WHERE
            "SELECT nullif(generate_series(1, 3), 2);",                      // 0A000 in NULLIF
        ],
    );
    vec![raw(e.to_string())]
}
