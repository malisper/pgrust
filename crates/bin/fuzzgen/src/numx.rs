//! ADT-numeric drain module (LD9): the `adt-numeric` chunk of
//! docs/fuzzing/line-drain-queue.tsv (160 rows / ~1,083 hollow lines in
//! numeric.c + the float.c/int8.c riders): div_var slow/fast paths,
//! power/exp/ln/log/sqrt boundary arms, numeric_in non-decimal +
//! underscore literals, to_char/to_number format pictures, rounding /
//! typmod display-scale arms, moving-aggregate inverse transitions
//! (do_numeric_discard incl. NaN/Inf tracking), int128 accumulators,
//! width_bucket, factorial, gcd/lcm boundaries, numeric abbrev-sort
//! (incl. the abort heuristic), generate_series numeric, and the
//! float4in/float8in/dpow/regr riders.
//!
//! Comparison law: NUMERIC IS EXACT — every numeric result must be
//! byte-identical across engines; any divergence is a real HIGH-severity
//! bug, never noise. Float-typed scalar expressions also compare
//! byte-identical as text (adtmisc law: B1 reassociation applies to
//! aggregate plan order only, and the float aggregate probes here feed
//! exact-representable integer inputs or NaN/Inf poison, both
//! order-independent). Errors are part of the surface: overflow /
//! invalid-input arms are emitted deliberately and matched on SQLSTATE
//! (diff::classify). random(numeric, numeric) executes under a
//! range-predicate wrapper only (engine PRNG streams differ; xnum:rand
//! precedent).
//!
//! Stateless: every probe is a self-contained one-statement group except
//! the sort/window families, which create-and-drop a `fz_nx` fixture
//! in-group (earm discipline).

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

const SHAPES: &[&str] = &[
    "numx:div",
    "numx:pow",
    "numx:instr",
    "numx:fmt",
    "numx:round",
    "numx:cast",
    "numx:agg",
    "numx:wb",
    "numx:misc",
    "numx:sort",
    "numx:series",
    "numx:float",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_numx_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx");
    match g.weights.pick(g.rng, SHAPES) {
        "numx:div" => gen_div(g),
        "numx:pow" => gen_pow(g),
        "numx:instr" => gen_instr(g),
        "numx:fmt" => gen_fmt(g),
        "numx:round" => gen_round(g),
        "numx:cast" => gen_cast(g),
        "numx:agg" => gen_agg(g),
        "numx:wb" => gen_wb(g),
        "numx:misc" => gen_misc(g),
        "numx:sort" => gen_sort(g),
        "numx:series" => gen_series(g),
        _ => gen_float(g),
    }
}

/// Numeric operand pools spanning the digit-count / scale regimes that
/// pick div_var's fast (1-2 divisor digits, int64 sizes) vs full paths.
const N_SMALL: &[&str] = &["0", "1", "-1", "2", "7", "-9", "0.5", "-0.25", "3.999"];
const N_MID: &[&str] = &[
    "12345.6789",
    "-9999.9999",
    "0.000123456",
    "73786976294.838206464",
    "-123456789012345.678901",
    "99999999999999999999.9999",
];
const N_BIG: &[&str] = &[
    "123456789012345678901234567890123456789",
    "-98765432109876543210987654321098765432109876543210",
    "1e100",
    "-1.234567890123456789012345678901234567890123e77",
    "9.99999e130",
];
const N_TINY: &[&str] = &["1e-100", "-2.5e-90", "0.00000000000000000001", "-1e-130"];
const N_SPECIAL: &[&str] = &["'NaN'::numeric", "'Infinity'::numeric", "'-Infinity'::numeric"];

/// One operand drawn across regimes (never the special pool — callers add
/// specials deliberately).
fn num_operand(g: &mut Gen) -> &'static str {
    match g.rng.below(4) {
        0 => pick_str(g, N_SMALL),
        1 => pick_str(g, N_MID),
        2 => pick_str(g, N_BIG),
        _ => pick_str(g, N_TINY),
    }
}

fn num_or_special(g: &mut Gen) -> String {
    if g.rng.chance(1, 5) {
        pick_str(g, N_SPECIAL).to_string()
    } else {
        format!("'{}'::numeric", num_operand(g))
    }
}

// ----------------------------------------------------------------- div ----

/// div_var / mod / div() / div_mod boundary matrix across digit regimes,
/// signs and specials, plus the divide-by-zero arms (matched 22012).
fn gen_div(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:div");
    let shape = g.weights.pick(g.rng, &["numx:div:op", "numx:div:fn", "numx:div:zero", "numx:div:special"]);
    g.fire(shape);
    let a = num_or_special(g);
    let b = format!("'{}'::numeric", num_operand(g));
    let sql = match shape {
        "numx:div:op" => {
            let a = format!("'{}'::numeric", num_operand(g));
            format!(
                "SELECT ({a} / {b})::text, ({a} % {b})::text, ({a} * {b})::text, \
                 ({a} + {b})::text, ({a} - {b})::text;"
            )
        }
        "numx:div:fn" => format!(
            "SELECT div({a}, {b})::text, mod({a}, {b})::text, gcd({}::numeric, {}::numeric)::text;",
            pick_str(g, &["0", "36", "-48", "123456789012345678", "7.0"]),
            pick_str(g, &["0", "60", "-18", "987654321098765432", "2.0"])
        ),
        "numx:div:zero" => {
            let f = pick_str(g, &["/", "%"]);
            let z = pick_str(g, &["0", "0.000", "0::numeric"]);
            format!("SELECT ({a} {f} {z})::text;") // ERROR 22012 (or NaN path)
        }
        _ => {
            let s = pick_str(g, N_SPECIAL);
            format!(
                "SELECT ({s} / {b})::text, ({b} / {s})::text, ({s} % {b})::text, \
                 ({s} * {s})::text, ({s} + {b})::text;"
            )
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------------- pow ----

/// power/exp/ln/log/sqrt/factorial boundary + error arms.
fn gen_pow(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:pow");
    let shape = g.weights.pick(
        g.rng,
        &["numx:pow:pow", "numx:pow:int", "numx:pow:explog", "numx:pow:sqrt", "numx:pow:err", "numx:pow:fac"],
    );
    g.fire(shape);
    let sql = match shape {
        "numx:pow:pow" => {
            let b = pick_str(g, &[
                "0", "1", "-1", "2", "0.5", "-2.5", "10", "0.0001", "9999999999",
                "'NaN'", "'Infinity'", "'-Infinity'",
            ]);
            let e = pick_str(g, &[
                "0", "1", "-1", "2", "3", "0.5", "-0.5", "12.345", "-77", "400",
                "'NaN'", "'Infinity'", "'-Infinity'", "0.000001",
            ]);
            format!("SELECT power({b}::numeric, {e}::numeric)::text;")
        }
        // power_var_int: integral exponents across magnitude regimes.
        "numx:pow:int" => {
            let b = pick_str(g, &["2", "-3", "1.0001", "0.99", "123456.789", "1e-10", "10"]);
            let e = pick_str(g, &["0", "1", "2", "3", "-1", "-2", "17", "-30", "1000", "131072"]);
            format!("SELECT power({b}::numeric, {e}::numeric)::text;")
        }
        "numx:pow:explog" => {
            let x = pick_str(g, &[
                "0", "1", "-1", "0.00001", "-0.00001", "1.00000000001", "0.99999999",
                "42", "-40", "5999.999", "1e-90", "123456.789",
            ]);
            format!(
                "SELECT exp({x}::numeric)::text, ln(abs({x}::numeric) + 1e-120)::text, \
                 log(abs({x}::numeric) + 1e-120)::text, \
                 log({}::numeric, abs({x}::numeric) + 1.5)::text;",
                pick_str(g, &["2", "10", "0.5", "16"])
            )
        }
        "numx:pow:sqrt" => {
            let x = pick_str(g, &[
                "0", "2", "9", "1e-130", "1e100", "0.000000000000000000000001",
                "99999999999999999999999999999999999999", "2.25",
            ]);
            format!("SELECT sqrt({x}::numeric)::text;")
        }
        "numx:pow:err" => {
            let e = pick_str(g, &[
                "SELECT power(0::numeric, -2.5::numeric)::text;",       // 2201F
                "SELECT power(-8::numeric, 0.5::numeric)::text;",       // 2201F
                "SELECT ln(0::numeric)::text;",                          // 2201E
                "SELECT ln(-4::numeric)::text;",                         // 2201E
                "SELECT log(-1::numeric)::text;",                        // 2201E
                "SELECT log(0::numeric, 10::numeric)::text;",            // 2201E/22012
                "SELECT log(1::numeric, 7::numeric)::text;",             // 22012? div by ln(1)
                "SELECT sqrt(-1::numeric)::text;",                       // 2201F
                "SELECT exp(90000::numeric)::text;",                     // 22003 overflow
                "SELECT power(10::numeric, 200000::numeric)::text;",     // 22003
            ]);
            e.to_string()
        }
        _ => {
            let x = pick_str(g, &["0", "1", "5", "20", "100", "-3", "33"]);
            format!("SELECT factorial({x}::int8)::text;")
        }
    };
    vec![raw(sql)]
}

// --------------------------------------------------------------- instr ----

/// numeric_in arms: exponent forms, non-decimal integer literals with
/// underscore separators, specials, and matched malformed-input errors.
fn gen_instr(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:instr");
    let shape = g.weights.pick(g.rng, &["numx:in:ok", "numx:in:nondec", "numx:in:err"]);
    g.fire(shape);
    let sql = match shape {
        "numx:in:ok" => {
            let s = pick_str(g, &[
                "  1234  ", "+0.5", "-.75", "1.", "1e10", "1E-10", "1.5e+300",
                "0.0000e5", "inf", "+inf", "-INFINITY", "nan", "NaN", "-0.0",
                "1_000_000", "1_2_3.4", "9_99e1_0",
            ]);
            format!("SELECT '{s}'::numeric::text;")
        }
        "numx:in:nondec" => {
            let s = pick_str(g, &[
                "0x2A", "-0xff", "0XdeadBEEF", "0o777", "-0O17", "0b101101", "-0B11",
                "0xffffffffffffffffffffffffffffffff", "0x1_00", "0o7_7", "0b1_0_1",
                "0x0", "-0b0",
            ]);
            format!("SELECT '{s}'::numeric::text, '{s}'::numeric % 7;")
        }
        _ => {
            let s = pick_str(g, &[
                "", "  ", "abc", "1..2", "1e", "e5", "1e+", "0x", "0xg", "0o8", "0b2",
                "1__0", "_1", "1_", "1.5_", "0x1.5", "12-3", "++1", "NaNx", "in",
                "1e1000000000",
            ]);
            format!("SELECT '{s}'::numeric::text;") // ERROR 22P02/22003 (matched)
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------------- fmt ----

/// to_char(numeric)/to_number format-picture arms (formatting.c numeric
/// pictures; C locale on both sides pins L/G/D).
fn gen_fmt(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:fmt");
    let shape = g.weights.pick(g.rng, &["numx:fmt:tochar", "numx:fmt:tonum", "numx:fmt:err"]);
    g.fire(shape);
    let sql = match shape {
        "numx:fmt:tochar" => {
            let v = pick_str(g, &[
                "0", "1", "-1", "0.5", "-0.5", "4.85", "-125.8", "1234567.897",
                "-9999999.99", "0.001", "148", "3999", "12345678901234567890",
                "'NaN'", "'Infinity'", "'-Infinity'",
            ]);
            let f = pick_str(g, &[
                "9G999G999D999", "FM9999999D99", "S9999D99", "MI9999D99", "9999D99PR",
                "L99G999D99", "RN", "FMRN", "EEEE9.999", "9.99EEEE", "099999", "9999TH",
                "9999th", "FM9999.00", "B9999.99", "99V999", "SG9999", "PL9999",
                "\"lit\"9999\"tail\"", "9 9 9 9", "FM99999999999999999999",
            ]);
            format!("SELECT to_char({v}::numeric, '{f}');")
        }
        "numx:fmt:tonum" => {
            let (s, f) = match g.rng.below(10) {
                0 => ("1234.56", "9999D99"),
                1 => ("-1234.56", "S9999D99"),
                2 => (" 12,345.6", "99G999D9"),
                3 => ("<1234.56>", "9999.99PR"),
                4 => ("1234.56-", "9999.99MI"),
                5 => ("$1,234.56", "L9G999D99"),
                6 => ("42nd", "999th"),
                7 => ("0.123", "FM9.999"),
                8 => ("1 2 3", "9 9 9"),
                _ => ("+5432", "SG9999"),
            };
            format!("SELECT to_number('{s}', '{f}')::text;")
        }
        _ => {
            let e = pick_str(g, &[
                "SELECT to_char(4000::numeric, 'RN');",   // overflow -> ### fill
                "SELECT to_char(-5::numeric, 'RN');",
                "SELECT to_char(123::numeric, '99');",    // ## overflow fill
                "SELECT to_number('abc', '999');",         // ERROR
                "SELECT to_number('', '999');",            // ERROR
                "SELECT to_number('123', 'RN');",          // ERROR: RN unsupported
                "SELECT to_char(1.5::numeric, 'EEEE');",   // ERROR: bad EEEE combo
                "SELECT to_char(12::numeric, '9V9V9');",   // ERROR: double V
                "SELECT to_number('1.5e2', '9.9EEEE');",   // EEEE to_number arm
            ]);
            e.to_string()
        }
    };
    vec![raw(sql)]
}

// --------------------------------------------------------------- round ----

/// Rounding / typmod / display-scale arms: round/trunc at extreme scales,
/// ceil/floor, min_scale/trim_scale, numeric(p,s) casts incl. negative
/// scale typmods and matched 22003 overflow arms.
fn gen_round(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:round");
    let shape = g.weights.pick(g.rng, &["numx:rd:round", "numx:rd:typmod", "numx:rd:scale", "numx:rd:err"]);
    g.fire(shape);
    let v = num_operand(g);
    let sql = match shape {
        "numx:rd:round" => {
            let s = pick_str(g, &["-2000", "-40", "-5", "-1", "0", "1", "3", "17", "600", "2000"]);
            format!(
                "SELECT round('{v}'::numeric, {s})::text, trunc('{v}'::numeric, {s})::text, \
                 ceil('{v}'::numeric)::text, floor('{v}'::numeric)::text, \
                 round('{v}'::numeric)::text, trunc('{v}'::numeric)::text;"
            )
        }
        "numx:rd:typmod" => {
            let tm = pick_str(g, &[
                "numeric(1,0)", "numeric(5,2)", "numeric(38,20)", "numeric(10,10)",
                "numeric(5,-2)", "numeric(3,-8)", "numeric(20,25)", "numeric(1000,500)",
            ]);
            let sv = pick_str(g, &["0", "0.44445", "12345.6789", "-0.5", "99.995", "449", "-451", "0.0000049"]);
            format!("SELECT ({sv}::{tm})::text, pg_typeof({sv}::{tm});")
        }
        "numx:rd:scale" => format!(
            "SELECT min_scale('{v}'::numeric), trim_scale('{v}'::numeric)::text, \
             scale('{v}'::numeric), width_bucket('{v}'::numeric, -1e10, 1e10, 7);"
        ),
        _ => {
            let e = pick_str(g, &[
                "SELECT (123.45::numeric(4,2))::text;",     // 22003
                "SELECT (1e10::numeric(5,-2))::text;",      // 22003
                "SELECT ('NaN'::numeric)::numeric(5,2)::text;",   // NaN passes typmod
                "SELECT ('Infinity'::numeric)::numeric(5,2)::text;", // 22003
                "SELECT (0.5::numeric(1,0))::text;",        // rounds to 1, fits
                "SELECT (0.5::numeric(1,1))::text;",
                "SELECT 1::numeric(1500,600);",             // ERROR: precision limit
                "SELECT 1::numeric(0,0);",                  // ERROR: precision 0
            ]);
            e.to_string()
        }
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------------- cast ----

/// numeric <-> int2/int4/int8/float boundary casts (rounding at .5,
/// min/max edges, matched 22003 arms), int128/uint conversions, pg_lsn
/// arithmetic with numeric, int8 gcd/lcm boundaries.
fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:cast");
    let shape = g.weights.pick(g.rng, &["numx:ca:int", "numx:ca:float", "numx:ca:err", "numx:ca:lsn", "numx:ca:gcd"]);
    g.fire(shape);
    let sql = match shape {
        "numx:ca:int" => {
            let v = pick_str(g, &[
                "0.5", "-0.5", "1.5", "2.5", "-2.5", "32766.6", "-32767.5",
                "2147483646.5", "-2147483647.5", "9223372036854775806.5",
                "-9223372036854775807.5", "0.49999999999",
            ]);
            let ty = pick_str(g, &["int2", "int4", "int8"]);
            format!("SELECT ({v}::numeric)::{ty};")
        }
        "numx:ca:float" => {
            let v = pick_str(g, &[
                "'NaN'::numeric", "'Infinity'::numeric", "'-Infinity'::numeric",
                "1.5::numeric", "-0.000001::numeric", "1e300::numeric", "1e-300::numeric",
                "3.4028235e38::numeric", "1.7976931348623157e308::numeric",
            ]);
            let back = pick_str(g, &[
                "'NaN'::float8", "'Infinity'::float8", "'-Infinity'::float8",
                "0.1::float8", "-2.5::float4", "'NaN'::float4", "1e-40::float4",
            ]);
            format!("SELECT ({v}::float8)::text, ({v}::float4)::text, ({back}::numeric)::text;")
        }
        "numx:ca:err" => {
            let e = pick_str(g, &[
                "SELECT (1e40::numeric)::float4::text;",           // 22003
                "SELECT (1e310::numeric)::float8::text;",          // 22003
                "SELECT (40000::numeric)::int2;",                  // 22003
                "SELECT (3000000000::numeric)::int4;",             // 22003
                "SELECT (1e19::numeric)::int8;",                   // 22003
                "SELECT ('NaN'::numeric)::int4;",                  // 22P05/0A000-class (matched)
                "SELECT ('Infinity'::numeric)::int8;",             // 22003
                "SELECT ('NaN'::float8)::numeric::text;",          // NaN ok
                "SELECT ('Infinity'::float4)::numeric::text;",     // Infinity ok
            ]);
            e.to_string()
        }
        "numx:ca:lsn" => {
            let b = pick_str(g, &["0", "1.5", "16.25", "4294967296", "-3"]);
            format!(
                "SELECT ('AB/CDEF1234'::pg_lsn + {b}::numeric)::text, \
                 ('AB/CDEF1234'::pg_lsn - {b}::numeric)::text;"
            )
        }
        _ => {
            let a = pick_str(g, &["0", "12", "-9223372036854775808", "9223372036854775807", "270", "-192"]);
            let b = pick_str(g, &["0", "-1", "18", "-9223372036854775808", "64"]);
            format!("SELECT gcd({a}::int8, {b}::int8), lcm({a}::int8, {b}::int8);")
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------------- agg ----

/// Accumulator arms: numeric/int8/int2 sum/avg/stddev with moving-window
/// inverse transitions (do_numeric_discard, int128 discard), NaN/Inf
/// entering AND leaving frames, and plain grouped accumulators.
fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:agg");
    let shape = g.weights.pick(g.rng, &["numx:ag:win", "numx:ag:special", "numx:ag:int", "numx:ag:stat"]);
    g.fire(shape);
    let t = "fz_nxa";
    let rows = 400 + g.rng.below(300);
    let stride = 2 + g.rng.below(7);
    let frame = 3 + g.rng.below(20);
    let mk = match shape {
        // plain exact numerics through a sliding frame
        "numx:ag:win" => format!(
            "CREATE TABLE {t} (i int4 PRIMARY KEY, v numeric, w int8, s int2);\n\
             INSERT INTO {t} SELECT i, (((i * {stride}) % 1000)::numeric) / 8, \
             (i::int8 - 200) * 1000003, ((i * 7) % 200 - 100)::int2 \
             FROM generate_series(1, {rows}) i;"
        ),
        // NaN / +Inf / -Inf sprinkled: the discard path must track specials
        _ => format!(
            "CREATE TABLE {t} (i int4 PRIMARY KEY, v numeric, w int8, s int2);\n\
             INSERT INTO {t} SELECT i, CASE \
               WHEN i % 97 = 13 THEN 'NaN'::numeric \
               WHEN i % 89 = 7 THEN 'Infinity'::numeric \
               WHEN i % 83 = 11 THEN '-Infinity'::numeric \
               ELSE (((i * {stride}) % 1000)::numeric) / 8 END, \
             (i::int8 - 200) * 1000003, ((i * 7) % 200 - 100)::int2 \
             FROM generate_series(1, {rows}) i;"
        ),
    };
    let probe = match shape {
        "numx:ag:int" => format!(
            "SELECT i, sum(w) OVER f, avg(w) OVER f, sum(s) OVER f, avg(s) OVER f, \
             sum(i) OVER f, avg(i) OVER f FROM {t} \
             WINDOW f AS (ORDER BY i ROWS BETWEEN {frame} PRECEDING AND CURRENT ROW) \
             ORDER BY i;"
        ),
        "numx:ag:stat" => format!(
            "SELECT i % 4 AS gk, sum(v)::text, avg(v)::text, \
             var_samp(v)::text, var_pop(v)::text, stddev_samp(v)::text, stddev_pop(v)::text, \
             sum(w), avg(w)::text, var_pop(w)::text \
             FROM {t} GROUP BY 1 ORDER BY 1;"
        ),
        _ => format!(
            "SELECT i, sum(v) OVER f, avg(v) OVER f, count(v) OVER f FROM {t} \
             WINDOW f AS (ORDER BY i ROWS BETWEEN {frame} PRECEDING AND CURRENT ROW) \
             ORDER BY i;"
        ),
    };
    let mut stmts: Vec<StmtKind> = Vec::new();
    for part in mk.split('\n') {
        stmts.push(raw(part.to_string()));
    }
    stmts.push(raw(probe));
    if shape != "numx:ag:int" && g.rng.chance(1, 2) {
        g.fire("numx:ag:both");
        // two-ended moving frame: discard fires on both frame edges
        stmts.push(raw(format!(
            "SELECT i, sum(v) OVER f, sum(w) OVER f FROM {t} \
             WINDOW f AS (ORDER BY i ROWS BETWEEN {frame} PRECEDING AND {} FOLLOWING) \
             ORDER BY i;",
            frame / 2
        )));
    }
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------------------ wb ----

/// width_bucket numeric + float8 arms.
fn gen_wb(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:wb");
    let shape = g.weights.pick(g.rng, &["numx:wb:num", "numx:wb:f8", "numx:wb:err"]);
    g.fire(shape);
    let sql = match shape {
        "numx:wb:num" => {
            let op = pick_str(g, &["-5.5", "0", "5", "9.999", "10", "15", "'NaN'", "'-Infinity'", "'Infinity'"]);
            let (lo, hi) = if g.rng.chance(1, 2) { ("0", "10") } else { ("10", "0") };
            let cnt = pick_str(g, &["1", "5", "1073741823"]);
            format!("SELECT width_bucket({op}::numeric, {lo}, {hi}, {cnt});")
        }
        "numx:wb:f8" => {
            let op = pick_str(g, &["-2.5", "0", "3.75", "100", "'NaN'", "'Infinity'", "'-Infinity'"]);
            let (lo, hi) = if g.rng.chance(1, 2) { ("-10", "10") } else { ("10", "-10") };
            format!("SELECT width_bucket({op}::float8, {lo}::float8, {hi}::float8, {});", 1 + g.rng.below(50))
        }
        _ => {
            let e = pick_str(g, &[
                "SELECT width_bucket(5::numeric, 0, 10, 0);",             // 2201G
                "SELECT width_bucket(5::numeric, 0, 10, -3);",            // 2201G
                "SELECT width_bucket(5::numeric, 3, 3, 5);",              // 2201G
                "SELECT width_bucket(5::numeric, 'NaN', 10, 5);",         // 2201G
                "SELECT width_bucket(5::float8, 'NaN'::float8, 10, 5);",  // 2201G
                "SELECT width_bucket(1::float8, 'Infinity'::float8, 2, 5);", // 2201G
                "SELECT width_bucket(5::float8, 0, 10, 2147483647);",
            ]);
            e.to_string()
        }
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------------- misc ----

/// Leftover numeric.c surface: numeric_inc, abs/sign, comparisons across
/// regimes, random(numeric) range wrapper, numeric hash arms.
fn gen_misc(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:misc");
    let shape = g.weights.pick(g.rng, &["numx:mi:unary", "numx:mi:cmp", "numx:mi:rand", "numx:mi:hash"]);
    g.fire(shape);
    let a = num_or_special(g);
    let b = num_or_special(g);
    let sql = match shape {
        "numx:mi:unary" => format!(
            "SELECT abs({a})::text, sign({a})::text, (-{a})::text, (+{a})::text, \
             numeric_inc({a})::text;"
        ),
        "numx:mi:cmp" => format!(
            "SELECT {a} < {b}, {a} <= {b}, {a} = {b}, {a} >= {b}, {a} > {b}, {a} <> {b}, \
             numeric_cmp({a}, {b}), numeric_larger({a}, {b})::text, numeric_smaller({a}, {b})::text;"
        ),
        "numx:mi:rand" => {
            let (lo, hi) = ("-1000.5", "1000.5");
            format!(
                "SELECT random({lo}::numeric, {hi}::numeric) BETWEEN {lo} AND {hi}, \
                 scale(random({lo}::numeric, {hi}::numeric)) <= 1;"
            )
        }
        _ => format!(
            "SELECT hash_numeric({a}) = hash_numeric({a}), \
             hash_numeric_extended({a}, 0) = hash_numeric_extended({a}, 0), \
             hash_numeric(0.0::numeric) = hash_numeric(0::numeric);"
        ),
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------------- sort ----

/// numeric abbreviated-key sort incl. the abbrev-abort heuristic: a
/// low-cardinality prefix-heavy column over enough rows that the
/// sampling logic engages; results are exact and totally ordered.
fn gen_sort(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:sort");
    let t = "fz_nxs";
    let rows = 12000 + g.rng.below(6000);
    let shape = g.weights.pick(g.rng, &["numx:st:lowcard", "numx:st:mixed"]);
    g.fire(shape);
    let expr = match shape {
        // ~8 distinct values over 12-18k rows: abbrev abort territory
        "numx:st:lowcard" => "(((i % 8)::numeric) / 4) + 1000000",
        // full-range keys with specials: abbreviation stays productive
        _ => "CASE WHEN i % 501 = 7 THEN 'NaN'::numeric \
              WHEN i % 503 = 11 THEN 'Infinity'::numeric \
              ELSE ((i * 37 % 100000)::numeric) / 1000 END",
    };
    vec![
        raw(format!(
            "CREATE TABLE {t} (i int4 PRIMARY KEY, v numeric);"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT i, {expr} FROM generate_series(1, {rows}) i;"
        )),
        raw(format!(
            "SELECT v::text, count(*), min(i), max(i) FROM {t} GROUP BY v ORDER BY v NULLS LAST LIMIT 40;"
        )),
        raw(format!(
            "SELECT i, v::text FROM {t} ORDER BY v, i LIMIT 25;"
        )),
        raw(format!(
            "SELECT i, v::text FROM {t} ORDER BY v DESC NULLS FIRST, i DESC LIMIT 25;"
        )),
        raw(format!("DROP TABLE {t};")),
    ]
}

// -------------------------------------------------------------- series ----

/// generate_series(numeric) arms incl. the planner support function and
/// the matched error arms (zero/NaN/Inf step or bounds).
fn gen_series(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:series");
    let shape = g.weights.pick(g.rng, &["numx:se:ok", "numx:se:err", "numx:se:plan"]);
    g.fire(shape);
    match shape {
        "numx:se:ok" => {
            let (a, b, s) = match g.rng.below(5) {
                0 => ("0", "10", "1"),
                1 => ("1.5", "-3.5", "-0.25"),
                2 => ("-2", "2", "0.7"),
                3 => ("5", "1", "1"), // empty
                _ => ("0.0000001", "0.000001", "0.0000001"),
            };
            vec![raw(format!(
                "SELECT g::text FROM generate_series({a}::numeric, {b}::numeric, {s}::numeric) g;"
            ))]
        }
        "numx:se:err" => {
            let e = pick_str(g, &[
                "SELECT generate_series(1::numeric, 5::numeric, 0::numeric);",       // 22023
                "SELECT generate_series(1::numeric, 5::numeric, 'NaN'::numeric);",   // 22023
                "SELECT generate_series('NaN'::numeric, 5::numeric, 1::numeric);",   // 22023
                "SELECT generate_series(1::numeric, 'Infinity'::numeric, 1::numeric);", // 22023
                "SELECT generate_series('-Infinity'::numeric, 5::numeric, 1::numeric);", // 22023
            ]);
            vec![raw(e.to_string())]
        }
        _ => vec![raw(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM generate_series(1::numeric, 500::numeric, 0.5::numeric);"
                .to_string(),
        )],
    }
}

// --------------------------------------------------------------- float ----

/// float.c riders from the chunk: float4in/float8in boundary strings,
/// dpow arms, regression accumulators over exact/poisoned inputs,
/// exp/overflow arms — scalar text compares (byte-identical law).
fn gen_float(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("numx:float");
    let shape = g.weights.pick(g.rng, &["numx:fl:in", "numx:fl:inerr", "numx:fl:pow", "numx:fl:regr"]);
    g.fire(shape);
    match shape {
        "numx:fl:in" => {
            let s = pick_str(g, &[
                "0", "-0", "  1.5  ", "inf", "+Inf", "-infinity", "nan", "-NaN", "NAN",
                "1e-45", "1.4e-45", "3.4028235e38", "1e-323", "2.2250738585072014e-308",
                "1.7976931348623157e308", ".5", "5.", "1e0", "1E+2",
            ]);
            let ty = pick_str(g, &["float4", "float8"]);
            vec![raw(format!("SELECT '{s}'::{ty}::text;"))]
        }
        "numx:fl:inerr" => {
            let s = pick_str(g, &[
                "", " ", "abc", "1e", "e1", "1.5x", "in", "infin", "n", "1e400",
                "-1e400", "1e-400x", "0x", "++1",
            ]);
            let ty = pick_str(g, &["float4", "float8"]);
            vec![raw(format!("SELECT '{s}'::{ty}::text;"))] // ERROR 22P02/22003 (matched)
        }
        "numx:fl:pow" => {
            let e = pick_str(g, &[
                "SELECT power(0::float8, -2::float8)::text;",             // 2201F
                "SELECT power(-8::float8, 0.5::float8)::text;",           // 2201F
                "SELECT power(0::float8, 0::float8)::text;",
                "SELECT power(1::float8, 'Infinity'::float8)::text;",
                "SELECT power(-1::float8, 'Infinity'::float8)::text;",
                "SELECT power('Infinity'::float8, -2::float8)::text;",
                "SELECT power('-Infinity'::float8, 3::float8)::text;",
                "SELECT power('-Infinity'::float8, 2.5::float8)::text;",
                "SELECT power('NaN'::float8, 0::float8)::text;",
                "SELECT power(1e300::float8, 10::float8)::text;",          // 22003
                "SELECT exp(1000::float8)::text;",                         // 22003
                "SELECT exp(-1000::float8)::text;",
                "SELECT sqrt(-1::float8)::text;",                          // 2201F
                "SELECT ln(0::float8)::text;",                             // 2201E
            ]);
            vec![raw(e.to_string())]
        }
        _ => {
            // exact int-valued inputs (order-independent sums) + specials
            let poison = pick_str(g, &["1", "0"]);
            vec![raw(format!(
                "SELECT regr_count(y, x), regr_slope(y, x)::text, regr_intercept(y, x)::text, \
                 regr_r2(y, x)::text, corr(y, x)::text, covar_pop(y, x)::text, \
                 regr_avgx(y, x)::text, regr_sxx(y, x)::text \
                 FROM (SELECT i::float8 AS x, \
                       CASE WHEN {poison} = 1 AND i = 3 THEN 'NaN'::float8 \
                            ELSE (i * 2 + 1)::float8 END AS y \
                       FROM generate_series(1, 6) i) s;"
            ))]
        }
    }
}
