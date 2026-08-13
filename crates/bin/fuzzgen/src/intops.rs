//! Integer-arithmetic drain module (INTOPS): the int.c / int8.c residue of
//! docs/fuzzing/line-drain-queue.tsv — the overflow / error / edge arms of
//! the int2/int4/int8 operator surface that the general expr grammar reaches
//! only at mid-range values. Every probe is a self-contained, single-
//! statement group of literal-driven boundary operands cast ::text for
//! byte-exact comparison.
//!
//! Targeted hollow lines (line-drain-queue rows): int2div/int4div/int8div and
//! int2mod/int4mod/int8mod (div-by-zero 22012 + INT_MIN/-1 overflow 22003),
//! int24pl/int24mi/int28mul/int48mul/int84mul (cross-width overflow), the
//! narrowing casts i4toi2/int82/int84/i8tooid, int4inc/int8inc/int8dec,
//! int4gcd_internal/int8gcd_internal/int4lcm/int8lcm (gcd/lcm overflow),
//! generate_series_step_int4/int8 (step 0/edge), in_range_int4_int4 and the
//! cross-width in_range arms (window RANGE frame offset overflow), and the
//! cross-width division arms int42div/int82div/int84div/int24div/int28div/
//! int48div.
//!
//! Comparison law: INTEGER ARITHMETIC IS EXACT. Every result value must be
//! byte-identical across engines and every error must match on SQLSTATE
//! (diff::classify); any value or error-identity divergence is a real
//! HIGH-severity bug, never noise. Overflow / divide-by-zero / parse arms are
//! emitted deliberately — the error IS the surface. Deterministic boundary
//! literals only (no PRNG-dependent values on the wire), so same seed +
//! toggles = byte-identical stream.

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

const SHAPES: &[&str] = &[
    "intops:arith",
    "intops:div",
    "intops:unary",
    "intops:bit",
    "intops:gcdlcm",
    "intops:cast",
    "intops:parse",
    "intops:cmp",
    "intops:series",
    "intops:agg",
    "intops:inrange",
    "intops:misc",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_intops_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops");
    match g.weights.pick(g.rng, SHAPES) {
        "intops:arith" => gen_arith(g),
        "intops:div" => gen_div(g),
        "intops:unary" => gen_unary(g),
        "intops:bit" => gen_bit(g),
        "intops:gcdlcm" => gen_gcdlcm(g),
        "intops:cast" => gen_cast(g),
        "intops:parse" => gen_parse(g),
        "intops:cmp" => gen_cmp(g),
        "intops:series" => gen_series(g),
        "intops:agg" => gen_agg(g),
        "intops:inrange" => gen_inrange(g),
        _ => gen_misc(g),
    }
}

// --------------------------------------------------------------- operands ----

// Boundary operand pools, one per width. INT_MIN is spelled parenthesized so
// the cast binds the negated literal (32768 / 2147483648 / 9223372036854775808
// don't fit their own signed type, but the unary-minus form casts cleanly).
const I2: &[&str] = &[
    "(0)::int2", "(1)::int2", "(-1)::int2", "(2)::int2", "(-2)::int2",
    "(181)::int2", "(-181)::int2", "(32766)::int2", "(32767)::int2",
    "(-32767)::int2", "(-32768)::int2",
];
const I4: &[&str] = &[
    "(0)::int4", "(1)::int4", "(-1)::int4", "(2)::int4", "(-2)::int4",
    "(46341)::int4", "(-46341)::int4", "(2147483646)::int4", "(2147483647)::int4",
    "(-2147483647)::int4", "(-2147483648)::int4",
];
const I8: &[&str] = &[
    "(0)::int8", "(1)::int8", "(-1)::int8", "(2)::int8", "(-2)::int8",
    "(3037000500)::int8", "(-3037000500)::int8", "(9223372036854775806)::int8",
    "(9223372036854775807)::int8", "(-9223372036854775807)::int8",
    "(-9223372036854775808)::int8",
];

fn op(g: &mut Gen, width: u8) -> &'static str {
    match width {
        2 => pick_str(g, I2),
        4 => pick_str(g, I4),
        _ => pick_str(g, I8),
    }
}

/// Draw a width, biased toward int4 (the widest hollow surface) but hitting
/// all three so int2/int8 arms fire too.
fn width(g: &mut Gen) -> u8 {
    match g.rng.below(3) {
        0 => 2,
        1 => 4,
        _ => 8,
    }
}

// ---------------------------------------------------------------- arith ----

/// int2/int4/int8 `+ - *` across boundary operands and cross-width mixes,
/// including the deterministic overflow arms (22003).
fn gen_arith(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:arith");
    let shape = g.weights.pick(
        g.rng,
        &["intops:ar:same", "intops:ar:cross", "intops:ar:over"],
    );
    g.fire(shape);
    let sql = match shape {
        "intops:ar:same" => {
            let w = width(g);
            let a = op(g, w);
            let b = op(g, w);
            format!(
                "SELECT ({a} + {b})::text, ({a} - {b})::text, ({a} * {b})::text;"
            )
        }
        "intops:ar:cross" => {
            // int24/int42/int28/int82/int48/int84 pl/mi/mul arms.
            let a = op(g, 2);
            let b = op(g, 4);
            let c = op(g, 8);
            format!(
                "SELECT ({a} + {b})::text, ({b} - {a})::text, ({a} * {b})::text, \
                 ({a} + {c})::text, ({c} - {a})::text, ({a} * {c})::text, \
                 ({b} + {c})::text, ({c} - {b})::text, ({b} * {c})::text;"
            )
        }
        // Deterministic overflow arms: each matched on 22003.
        _ => pick_str(g, &[
            "SELECT ((2147483647)::int4 + (1)::int4)::text;",
            "SELECT ((-2147483648)::int4 - (1)::int4)::text;",
            "SELECT ((2147483647)::int4 * (2)::int4)::text;",
            "SELECT ((-2147483648)::int4 * (-1)::int4)::text;",
            "SELECT ((32767)::int2 + (1)::int2)::text;",
            "SELECT ((-32768)::int2 - (1)::int2)::text;",
            "SELECT ((32767)::int2 * (2)::int2)::text;",
            "SELECT ((9223372036854775807)::int8 + (1)::int8)::text;",
            "SELECT ((-9223372036854775808)::int8 - (1)::int8)::text;",
            "SELECT ((9223372036854775807)::int8 * (2)::int8)::text;",
            "SELECT ((-9223372036854775808)::int8 * (-1)::int8)::text;",
            "SELECT ((32767)::int2 * (2147483647)::int4)::text;",
            "SELECT ((32767)::int2 * (9223372036854775807)::int8)::text;",
            "SELECT ((2147483647)::int4 * (9223372036854775807)::int8)::text;",
            "SELECT ((-32768)::int2 * (-9223372036854775808)::int8)::text;",
        ]).to_string(),
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------------ div ----

/// `/` `%` and the div()/mod() functions across widths, including the
/// divide-by-zero arm (22012) and the INT_MIN/-1 overflow arm (22003), plus
/// the cross-width division functions.
fn gen_div(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:div");
    let shape = g.weights.pick(
        g.rng,
        &["intops:dv:op", "intops:dv:cross", "intops:dv:zero", "intops:dv:over"],
    );
    g.fire(shape);
    let sql = match shape {
        "intops:dv:op" => {
            let w = width(g);
            let a = op(g, w);
            // divisor never zero here (zero/over arms cover those).
            let b = pick_str(g, match w {
                2 => &["(1)::int2", "(-1)::int2", "(2)::int2", "(-3)::int2", "(181)::int2", "(32767)::int2"],
                4 => &["(1)::int4", "(-1)::int4", "(2)::int4", "(-3)::int4", "(46341)::int4", "(2147483647)::int4"],
                _ => &["(1)::int8", "(-1)::int8", "(2)::int8", "(-3)::int8", "(3037000500)::int8", "(9223372036854775807)::int8"],
            });
            format!(
                "SELECT ({a} / {b})::text, ({a} % {b})::text, \
                 div({a}, {b})::text, mod({a}, {b})::text;"
            )
        }
        "intops:dv:cross" => {
            // int24div/int42div/int28div/int82div/int48div/int84div.
            let a2 = op(g, 2);
            let a4 = op(g, 4);
            let a8 = op(g, 8);
            let d2 = pick_str(g, &["(1)::int2", "(-1)::int2", "(3)::int2", "(181)::int2"]);
            let d4 = pick_str(g, &["(1)::int4", "(-1)::int4", "(3)::int4", "(46341)::int4"]);
            let d8 = pick_str(g, &["(1)::int8", "(-1)::int8", "(3)::int8", "(3037000500)::int8"]);
            format!(
                "SELECT ({a2} / {d4})::text, ({a4} / {d2})::text, \
                 ({a2} / {d8})::text, ({a8} / {d2})::text, \
                 ({a4} / {d8})::text, ({a8} / {d4})::text;"
            )
        }
        // divide / modulo by zero: 22012.
        "intops:dv:zero" => pick_str(g, &[
            "SELECT ((1)::int2 / (0)::int2)::text;",
            "SELECT ((1)::int2 % (0)::int2)::text;",
            "SELECT ((2147483647)::int4 / (0)::int4)::text;",
            "SELECT ((-2147483648)::int4 % (0)::int4)::text;",
            "SELECT ((9223372036854775807)::int8 / (0)::int8)::text;",
            "SELECT ((1)::int8 % (0)::int8)::text;",
            "SELECT div((5)::int4, (0)::int4)::text;",
            "SELECT mod((5)::int8, (0)::int8)::text;",
            "SELECT ((5)::int4 / (0)::int2)::text;",
            "SELECT ((5)::int8 / (0)::int4)::text;",
        ]).to_string(),
        // INT_MIN / -1 overflow (22003); INT_MIN % -1 hits the =-1 fast arm
        // returning 0 (exact-value probe).
        _ => pick_str(g, &[
            "SELECT ((-32768)::int2 / (-1)::int2)::text;",
            "SELECT ((-32768)::int2 % (-1)::int2)::text;",
            "SELECT ((-2147483648)::int4 / (-1)::int4)::text;",
            "SELECT ((-2147483648)::int4 % (-1)::int4)::text;",
            "SELECT ((-9223372036854775808)::int8 / (-1)::int8)::text;",
            "SELECT ((-9223372036854775808)::int8 % (-1)::int8)::text;",
            "SELECT div((-2147483648)::int4, (-1)::int4)::text;",
            "SELECT div((-9223372036854775808)::int8, (-1)::int8)::text;",
        ]).to_string(),
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------------- unary ----

/// Unary minus, abs() and the `@` absolute-value operator across widths,
/// including the INT_MIN overflow arms (int2um/int4um/int8um/int4abs).
fn gen_unary(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:unary");
    let shape = g.weights.pick(g.rng, &["intops:un:ok", "intops:un:over"]);
    g.fire(shape);
    let sql = match shape {
        "intops:un:ok" => {
            let w = width(g);
            let a = op(g, w);
            format!("SELECT (- {a})::text, abs({a})::text, (@ {a})::text, (+ {a})::text;")
        }
        // negate / abs at INT_MIN overflows (22003).
        _ => pick_str(g, &[
            "SELECT (- (-32768)::int2)::text;",
            "SELECT abs((-32768)::int2)::text;",
            "SELECT (@ (-32768)::int2)::text;",
            "SELECT (- (-2147483648)::int4)::text;",
            "SELECT abs((-2147483648)::int4)::text;",
            "SELECT (@ (-2147483648)::int4)::text;",
            "SELECT (- (-9223372036854775808)::int8)::text;",
            "SELECT abs((-9223372036854775808)::int8)::text;",
            "SELECT (@ (-9223372036854775808)::int8)::text;",
        ]).to_string(),
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------------ bit ----

/// Bitwise `& | # ~ << >>` across widths, including shift counts >= the type
/// width and negative shifts (the C shift arms are defined per-width).
fn gen_bit(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:bit");
    let shape = g.weights.pick(g.rng, &["intops:bt:logic", "intops:bt:shift"]);
    g.fire(shape);
    let w = width(g);
    let a = op(g, w);
    let b = op(g, w);
    let sql = match shape {
        "intops:bt:logic" => {
            format!("SELECT ({a} & {b})::text, ({a} | {b})::text, ({a} # {b})::text, (~ {a})::text;")
        }
        // shift amounts spanning below/at/above the type width and negative.
        _ => {
            let s = pick_str(g, &[
                "0", "1", "7", "15", "16", "17", "31", "32", "33", "62", "63", "64", "65",
                "-1", "-16", "-64",
            ]);
            format!("SELECT ({a} << {s})::text, ({a} >> {s})::text;")
        }
    };
    vec![raw(sql)]
}

// --------------------------------------------------------------- gcdlcm ----

/// gcd()/lcm() across widths, including gcd(INT_MIN, 0) / gcd(INT_MIN, -1)
/// and the lcm overflow arms (22003) — int4gcd_internal/int8gcd_internal,
/// int4lcm/int8lcm.
fn gen_gcdlcm(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:gcdlcm");
    let shape = g.weights.pick(g.rng, &["intops:gl:ok", "intops:gl:over"]);
    g.fire(shape);
    let sql = match shape {
        "intops:gl:ok" => {
            let w = if g.rng.chance(1, 2) { 4 } else { 8 };
            let a = op(g, w);
            let b = op(g, w);
            format!("SELECT gcd({a}, {b})::text, lcm({a}, {b})::text;")
        }
        // gcd(INT_MIN,0)/gcd(INT_MIN,-1) overflow (abs INT_MIN) + lcm overflow.
        _ => pick_str(g, &[
            "SELECT gcd((-2147483648)::int4, (0)::int4)::text;",
            "SELECT gcd((0)::int4, (-2147483648)::int4)::text;",
            "SELECT gcd((-2147483648)::int4, (-2147483648)::int4)::text;",
            "SELECT gcd((-9223372036854775808)::int8, (0)::int8)::text;",
            "SELECT gcd((0)::int8, (-9223372036854775808)::int8)::text;",
            "SELECT gcd((-9223372036854775808)::int8, (-9223372036854775808)::int8)::text;",
            "SELECT lcm((2147483647)::int4, (2147483646)::int4)::text;",
            "SELECT lcm((-2147483648)::int4, (1)::int4)::text;",
            "SELECT lcm((9223372036854775807)::int8, (9223372036854775806)::int8)::text;",
            "SELECT lcm((-9223372036854775808)::int8, (1)::int8)::text;",
            "SELECT lcm((0)::int4, (-2147483648)::int4)::text;",
        ]).to_string(),
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------------- cast ----

/// Integer narrowing / widening casts, int<->bool, and int8->oid — including
/// the narrowing-overflow arms i4toi2/i8toi2/i8toi4/i8tooid (22003).
fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:cast");
    let shape = g.weights.pick(
        g.rng,
        &["intops:ca:widen", "intops:ca:narrow", "intops:ca:bool", "intops:ca:err"],
    );
    g.fire(shape);
    let sql = match shape {
        // in-range narrowing + widening round-trips (exact-value).
        "intops:ca:widen" => {
            let w = width(g);
            let a = op(g, w);
            format!(
                "SELECT ({a})::int8::text, ({a})::int4::text, ({a})::int2::text, \
                 ({a})::oid::text;"
            )
        }
        "intops:ca:narrow" => {
            // values chosen to sit inside the target range (no overflow).
            let v = pick_str(g, &["(0)::int8", "(1)::int8", "(-1)::int8", "(181)::int8",
                                  "(-32768)::int8", "(32767)::int8", "(46341)::int8"]);
            format!("SELECT ({v})::int4::text, ({v})::int2::text;")
        }
        "intops:ca:bool" => {
            let a = pick_str(g, &["(0)::int4", "(1)::int4", "(-1)::int4", "(42)::int4"]);
            format!("SELECT ({a}::bool)::text, (true::int4)::text, (false::int4)::text;")
        }
        // narrowing / oid overflow (22003).
        _ => pick_str(g, &[
            "SELECT ((2147483647)::int4)::int2::text;",
            "SELECT ((-2147483648)::int4)::int2::text;",
            "SELECT ((40000)::int4)::int2::text;",
            "SELECT ((9223372036854775807)::int8)::int2::text;",
            "SELECT ((9223372036854775807)::int8)::int4::text;",
            "SELECT ((-9223372036854775808)::int8)::int4::text;",
            "SELECT ((3000000000)::int8)::int4::text;",
            "SELECT ((-1)::int8)::oid::text;",
            "SELECT ((9223372036854775807)::int8)::oid::text;",
        ]).to_string(),
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------------- parse ----

/// int2/int4/int8 input (text -> int) edge cases and int -> text output.
/// Parse edges: leading/trailing space, +sign, empty, overflow, non-numeric
/// (22P02) and out-of-range (22003).
fn gen_parse(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:parse");
    let shape = g.weights.pick(g.rng, &["intops:pa:ok", "intops:pa:err", "intops:pa:out"]);
    g.fire(shape);
    let sql = match shape {
        "intops:pa:ok" => {
            let s = pick_str(g, &[
                "  42  ", "+7", "-7", "0", "+0", "-0", " 000123 ", "2147483647",
                "-2147483648", "9223372036854775807", "-9223372036854775808", "32767",
            ]);
            let ty = pick_str(g, &["int2", "int4", "int8"]);
            format!("SELECT '{s}'::{ty}::text;")
        }
        // int -> text output across widths.
        "intops:pa:out" => {
            let w = width(g);
            let a = op(g, w);
            format!("SELECT ({a})::text, ({a})::text::int8::text;")
        }
        // malformed (22P02) or out-of-range (22003), matched on SQLSTATE.
        _ => pick_str(g, &[
            "SELECT ''::int4;",
            "SELECT '   '::int4;",
            "SELECT 'abc'::int4;",
            "SELECT '1 2'::int4;",
            "SELECT '1.5'::int4;",
            "SELECT '+'::int4;",
            "SELECT '-'::int4;",
            "SELECT '0x1F'::int4;",
            "SELECT '99999'::int2;",
            "SELECT '-99999'::int2;",
            "SELECT '3000000000'::int4;",
            "SELECT '99999999999999999999'::int8;",
            "SELECT '12abc'::int8;",
            "SELECT '  '::int2;",
        ]).to_string(),
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------------ cmp ----

/// Same-width and cross-width comparison of int2/int4/int8 (btintXYcmp and
/// the operator forms), returning the full six-way comparison as booleans.
fn gen_cmp(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:cmp");
    let shape = g.weights.pick(g.rng, &["intops:cm:same", "intops:cm:cross"]);
    g.fire(shape);
    let sql = match shape {
        "intops:cm:same" => {
            let w = width(g);
            let a = op(g, w);
            let b = op(g, w);
            format!(
                "SELECT {a} < {b}, {a} <= {b}, {a} = {b}, {a} >= {b}, {a} > {b}, {a} <> {b};"
            )
        }
        _ => {
            // every cross-width pair: 2v4, 2v8, 4v8.
            let a2 = op(g, 2);
            let a4 = op(g, 4);
            let a8 = op(g, 8);
            format!(
                "SELECT {a2} < {a4}, {a2} = {a4}, {a2} > {a4}, \
                 {a2} < {a8}, {a2} = {a8}, {a2} > {a8}, \
                 {a4} < {a8}, {a4} = {a8}, {a4} > {a8}, \
                 least({a2}, {a4}, {a8})::text, greatest({a2}, {a4}, {a8})::text;"
            )
        }
    };
    vec![raw(sql)]
}

// --------------------------------------------------------------- series ----

/// generate_series(int, int, step) across widths, including the step-edge
/// arms: step 0 (22023), descending steps, empty ranges, single-element
/// ranges, and INT_MAX/INT_MIN bounds (loop-termination overflow guard).
fn gen_series(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:series");
    let shape = g.weights.pick(g.rng, &["intops:se:ok", "intops:se:edge", "intops:se:err"]);
    g.fire(shape);
    match shape {
        "intops:se:ok" => {
            let ty = if g.rng.chance(1, 2) { "int4" } else { "int8" };
            let (a, b, s) = match g.rng.below(6) {
                0 => ("1", "10", "1"),
                1 => ("10", "1", "-1"),
                2 => ("0", "10", "3"),
                3 => ("5", "5", "1"),   // single element
                4 => ("5", "1", "1"),   // empty
                _ => ("-6", "6", "4"),
            };
            vec![raw(format!(
                "SELECT array_agg(g)::text FROM generate_series({a}::{ty}, {b}::{ty}, {s}::{ty}) g;"
            ))]
        }
        // near-INT_MAX / INT_MIN bounds: the internal current+step overflow
        // guard must terminate the loop rather than wrap.
        "intops:se:edge" => vec![raw(pick_str(g, &[
            "SELECT count(*) FROM generate_series(2147483645::int4, 2147483647::int4, 1::int4);",
            "SELECT count(*) FROM generate_series(2147483647::int4, 2147483645::int4, -1::int4);",
            "SELECT count(*) FROM generate_series((-2147483648)::int4, (-2147483646)::int4, 1::int4);",
            "SELECT count(*) FROM generate_series(2147483000::int4, 2147483647::int4, 2000000000::int4);",
            "SELECT count(*) FROM generate_series(9223372036854775805::int8, 9223372036854775807::int8, 1::int8);",
            "SELECT count(*) FROM generate_series(9223372036854775807::int8, 9223372036854775805::int8, -1::int8);",
            "SELECT count(*) FROM generate_series((-9223372036854775808)::int8, (-9223372036854775806)::int8, 1::int8);",
        ]).to_string())],
        // step 0 (22023) across widths.
        _ => vec![raw(pick_str(g, &[
            "SELECT generate_series(1::int4, 5::int4, 0::int4);",
            "SELECT generate_series(1::int8, 5::int8, 0::int8);",
            "SELECT generate_series(5::int4, 1::int4, 0::int4);",
        ]).to_string())],
    }
}

// ------------------------------------------------------------------ agg ----

/// Integer aggregates over a literal generate_series source (no fixture):
/// sum/avg (int2/int4 accumulate into int8/numeric; int8 into numeric) and
/// the bit_and/bit_or/bit_xor aggregates, plus min/max/count.
fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:agg");
    let shape = g.weights.pick(g.rng, &["intops:ag:sum", "intops:ag:bit"]);
    g.fire(shape);
    let ty = pick_str(g, &["int2", "int4", "int8"]);
    let n = pick_str(g, &["10", "100", "1000"]);
    let sql = match shape {
        // sum/avg over a signed range straddling zero (int2/int4 sum -> int8,
        // int8 sum -> numeric; avg -> numeric); also plain min/max/count.
        "intops:ag:sum" => format!(
            "SELECT sum(v)::text, avg(v)::text, min(v)::text, max(v)::text, count(v)::text \
             FROM (SELECT (g - {n})::{ty} AS v FROM generate_series(0, 2 * {n}) g) s;"
        ),
        // bit_and/bit_or/bit_xor over a small integer set.
        _ => format!(
            "SELECT bit_and(v)::text, bit_or(v)::text, bit_xor(v)::text \
             FROM (SELECT (g * 7 + 1)::{ty} AS v FROM generate_series(0, 20) g) s;"
        ),
    };
    vec![raw(sql)]
}

// -------------------------------------------------------------- inrange ----

/// Window RANGE frames ordered by an integer column drive the in_range
/// support functions (in_range_int4_int4 and the cross-width int2/int8
/// combinations). Large PRECEDING/FOLLOWING offsets exercise the offset-sum
/// overflow guards. The window source is a literal generate_series subquery,
/// so the group stays single-statement and stateless.
fn gen_inrange(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:inrange");
    // order-column width x offset-width -> in_range_int{order}_int{offset}.
    let ordty = pick_str(g, &["int2", "int4", "int8"]);
    let off = pick_str(g, &[
        "1", "2", "100", "2147483647", "9223372036854775807",
    ]);
    // offset type: use int8 for the large offsets, else small int4.
    let offty = if off.len() > 10 { "int8" } else { "int4" };
    let base = pick_str(g, &["0", "-10", "2147483640"]);
    let sql = format!(
        "SELECT v::text, \
         sum(v) OVER (ORDER BY v RANGE BETWEEN {off}::{offty} PRECEDING AND {off}::{offty} FOLLOWING)::text, \
         count(*) OVER (ORDER BY v RANGE BETWEEN {off}::{offty} PRECEDING AND CURRENT ROW)::text \
         FROM (SELECT ({base} + g * 3)::{ordty} AS v FROM generate_series(0, 12) g) s \
         ORDER BY v;"
    );
    vec![raw(sql)]
}

// ----------------------------------------------------------------- misc ----

/// Leftover int.c / int8.c surface: int4inc/int8inc/int8dec (the sequence /
/// btree increment support), factorial (int8 -> numeric), and the integer
/// hash functions hashint2/hashint4/hashint8 (self-consistency probes).
fn gen_misc(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("intops:misc");
    let shape = g.weights.pick(g.rng, &["intops:mi:incdec", "intops:mi:fac", "intops:mi:hash"]);
    g.fire(shape);
    let sql = match shape {
        // int4inc/int8inc/int8dec via the exposed int4pl/int8pl increment
        // forms and the overflow edge (INT_MAX + 1 -> 22003).
        "intops:mi:incdec" => pick_str(g, &[
            "SELECT ((2147483646)::int4 + 1)::text, ((2147483647)::int4 + 1)::text;",
            "SELECT ((9223372036854775806)::int8 + 1)::text, ((9223372036854775807)::int8 + 1)::text;",
            "SELECT ((-9223372036854775807)::int8 - 1)::text, ((-9223372036854775808)::int8 - 1)::text;",
            "SELECT ((0)::int4 + 1)::text, ((0)::int8 + 1)::text, ((0)::int8 - 1)::text;",
        ]).to_string(),
        // factorial(int8) -> numeric, including 0!, negative (returns 1 in
        // PG's numeric factorial for n<1) and the large / overflow inputs.
        "intops:mi:fac" => {
            let x = pick_str(g, &["0", "1", "5", "20", "33", "-1", "-5"]);
            format!("SELECT factorial({x}::int8)::text;")
        }
        // integer hash self-consistency (must be identical across engines).
        _ => {
            let a2 = op(g, 2);
            let a4 = op(g, 4);
            let a8 = op(g, 8);
            format!(
                "SELECT hashint2({a2}) = hashint2({a2}), \
                 hashint4({a4}) = hashint4({a4}), \
                 hashint8({a8}) = hashint8({a8}), \
                 hashint4extended({a4}, 0) = hashint4extended({a4}, 0);"
            )
        }
    };
    vec![raw(sql)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every shape produces a single, well-formed, terminated statement with
    /// balanced parens on one line (the stmt-level invariants), and every
    /// shape is reachable under the default weights.
    #[test]
    fn shapes_are_wellformed_and_reachable() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x1470);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_intops_module(&mut g);
            assert!(!stmts.is_empty());
            for s in &stmts {
                let sql = s.to_sql();
                assert!(!sql.contains('\n'), "multi-line: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                assert!(sql.starts_with("SELECT"), "unexpected stmt: {sql}");
            }
            for p in &prods {
                if let Some(sh) = p.strip_prefix("intops:") {
                    if !sh.contains(':') {
                        seen.insert(sh.to_string());
                    }
                }
            }
        }
        for sh in SHAPES {
            let name = sh.strip_prefix("intops:").unwrap();
            assert!(seen.contains(name), "shape {sh} never fired");
        }
    }
}
