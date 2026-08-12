//! Q2 xnum module: the cross-type numeric operator matrix
//! (sql-reachable-queue chunk `expr-numeric`, 172 fns — int2/int4/int8
//! cross-type arithmetic/comparison/bitwise operators, money (cash.c),
//! oid/oidvector comparisons, pg_snapshot/xid8 machinery, numeric/float
//! builtin breadth: trig-in-degrees, hyperbolics, width_bucket/round/
//! trunc arity, generate_series numeric/bigint, gcd/lcm, pg_lsn
//! arithmetic, non-decimal integer literals).
//!
//! Every family below was hand-verified byte-identical on both engines
//! (scratchpad hv-xnum leg, 2026-08-11, A = pinned REL_18_3, B = pgrust
//! origin/main@9dd02888bf7) — including cot/exp/degrees/sind/sinh float
//! text, money output under C locale, numeric pow/overflow errors, and
//! oidvector output from pg_proc. The ONE family that diverged —
//! seeded random(): setseed() then random() yields engine-different
//! streams — is emitted as range predicates only (`random(lo, hi)
//! BETWEEN lo AND hi`), so the C fns still execute while the compare
//! surface stays engine-stable.
//!
//! Scalar float/numeric outputs always cast ::text (adtmisc law: scalar
//! arithmetic compares byte-identical, B1 reassociation applies to
//! aggregate plan order only). Overflow/underflow fuel (matched 22003)
//! rides the xnum:ok/xnum:err knob, biased away from errors.

use crate::stmt::{Gen, StmtKind};

const SHAPES: &[&str] = &[
    "xnum:xint",
    "xnum:bit",
    "xnum:money",
    "xnum:oid",
    "xnum:snap",
    "xnum:math",
    "xnum:bucket",
    "xnum:series",
    "xnum:lsn",
    "xnum:lit",
    "xnum:rand",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_xnum_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("xnum");
    let sql = match g.weights.pick(g.rng, SHAPES) {
        "xnum:xint" => gen_xint(g),
        "xnum:bit" => gen_bit(g),
        "xnum:money" => gen_money(g),
        "xnum:oid" => gen_oid(g),
        "xnum:snap" => gen_snap(g),
        "xnum:math" => gen_math(g),
        "xnum:bucket" => gen_bucket(g),
        "xnum:series" => gen_series(g),
        "xnum:lsn" => gen_lsn(g),
        "xnum:lit" => gen_lit(g),
        "xnum:rand" => gen_rand(g),
        other => unreachable!("unknown xnum shape {other}"),
    };
    vec![StmtKind::Raw(sql)]
}

fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["xnum:ok", "xnum:err"]) == "xnum:err" {
        g.fire("xnum:err");
        true
    } else {
        false
    }
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

/// Typed integer/float/numeric operand pools (in-range values).
const I2: &[&str] = &["0", "1", "-1", "7", "-32767", "32767", "255", "-100"];
const I4: &[&str] = &["0", "2", "-3", "100000", "-2147483647", "2147483647", "65536"];
const I8: &[&str] = &["0", "5", "-7", "9223372036854775807", "-9223372036854775807", "4294967296", "-123456789012"];
const F4: &[&str] = &["0", "1.5", "-2.25", "3.4028e37", "-1.1755e-38", "100"];
const F8: &[&str] = &["0", "2.5", "-0.125", "1.7976e307", "2.2250e-308", "-64"];
const NUM: &[&str] = &["0", "1.5", "-42.42", "99999999999999999999.9999", "0.00000000001", "123456789012345678901234567890"];

/// Overflow-fuel operands (matched 22003 when combined by the err ops).
const I2_BIG: &[&str] = &["32767", "32000", "30000"];
const I8_BIG: &[&str] = &["9223372036854775807", "9223372036854775806", "4611686018427387904"];

const ARITH: &[&str] = &["+", "-", "*", "/", "%"];
const CMP: &[&str] = &["<", "<=", "=", ">=", ">", "<>"];

fn typed(g: &mut Gen, ty: &str) -> String {
    let pool = match ty {
        "int2" => I2,
        "int4" => I4,
        "int8" => I8,
        "float4" => F4,
        "float8" => F8,
        _ => NUM,
    };
    format!("{}::{}", pick_str(g, pool), ty)
}

/// Cross-type int2/int4/int8/float4/float8/numeric operator sweep: every
/// pg_operator cross-type pair (int24* int28* int42* int48* int82* int84*
/// float48* float84* and the numeric-vs-int implicit-cast paths).
fn gen_xint(g: &mut Gen) -> String {
    g.fire("xnum:xint");
    let types = ["int2", "int4", "int8", "float4", "float8", "numeric"];
    let lt = pick_str(g, &types);
    let rt = pick_str(g, &types);
    if err_arm(g) {
        // Matched 22003/22012: overflow or division by zero on the
        // exact-width int ops.
        return match g.rng.below(4) {
            0 => format!("SELECT ({}::int2 * 2::int2)::text;", pick_str(g, I2_BIG)),
            1 => format!("SELECT ({}::int8 * 2::int8)::text;", pick_str(g, I8_BIG)),
            2 => format!("SELECT ({} / 0::{})::text;", typed(g, lt), rt),
            _ => format!("SELECT ({} % 0)::text;", typed(g, lt)),
        };
    }
    let float_involved = matches!(lt, "float4" | "float8") || matches!(rt, "float4" | "float8");
    let op = if g.rng.chance(1, 2) {
        pick_str(g, CMP)
    } else if float_involved {
        // No float division fuel here (Inf/underflow handled in xnum:math);
        // + - * only.
        pick_str(g, &["+", "-", "*"])
    } else {
        pick_str(g, ARITH)
    };
    let (l, r) = (typed(g, lt), typed(g, rt));
    // Small operands for * / % arithmetic so ok-arm stays ok: reroll
    // extremes into the low half of each pool.
    let (l, r) = if op == "*" || op == "/" || op == "%" {
        (small(g, lt), small(g, rt))
    } else {
        (l, r)
    };
    if op == "/" || op == "%" {
        // Nonzero divisor from the small pool.
        return format!("SELECT ({} {} {})::text;", l, op, nonzero(g, rt));
    }
    format!("SELECT ({} {} {})::text;", l, op, r)
}

fn small(g: &mut Gen, ty: &str) -> String {
    // Products stay in range even for int2 (|v| <= 100 -> <= 10000).
    let v = pick_str(g, &["0", "1", "-2", "3", "7", "-11", "100"]);
    format!("{}::{}", v, ty)
}

fn nonzero(g: &mut Gen, ty: &str) -> String {
    let v = pick_str(g, &["1", "-2", "3", "7", "-11", "100"]);
    format!("{}::{}", v, ty)
}

/// int2/int4/int8 bitwise operators & | # ~ << >>.
fn gen_bit(g: &mut Gen) -> String {
    g.fire("xnum:bit");
    let ty = pick_str(g, &["int2", "int4", "int8"]);
    let l = small(g, ty);
    match g.rng.below(4) {
        0 => {
            let op = pick_str(g, &["&", "|", "#"]);
            let r = small(g, ty);
            format!("SELECT ({} {} {})::text;", l, op, r)
        }
        1 => format!("SELECT (~{})::text;", l),
        2 => {
            let n = g.rng.below(if ty == "int8" { 63 } else { 15 });
            format!("SELECT ({} << {})::text;", l, n)
        }
        _ => {
            let n = g.rng.below(if ty == "int8" { 63 } else { 15 });
            format!("SELECT ({} >> {})::text;", l, n)
        }
    }
}

/// money literals + arithmetic/comparison + int/float/numeric casts
/// (cash.c; C-locale output verified identical).
fn gen_money(g: &mut Gen) -> String {
    g.fire("xnum:money");
    let vals = ["'0'", "'1'", "'-2.5'", "'12.34'", "'92233720368547758.07'", "'-92233720368547758.08'", "'1234567.89'"];
    let l = format!("{}::money", pick_str(g, &vals));
    if err_arm(g) {
        return match g.rng.below(3) {
            0 => "SELECT ('92233720368547758.07'::money + '0.01'::money)::text;".to_string(),
            1 => format!("SELECT ({} / 0)::text;", l),
            _ => "SELECT ('notmoney'::money)::text;".to_string(),
        };
    }
    match g.rng.below(8) {
        0 => {
            let r = format!("{}::money", pick_str(g, &vals));
            format!("SELECT ({} {} {})::text;", l, pick_str(g, CMP), r)
        }
        1 => {
            let r = format!("{}::money", pick_str(g, &["'1'", "'-2.5'", "'12.34'"]));
            format!("SELECT ({} {} {})::text;", l, pick_str(g, &["+", "-"]), r)
        }
        2 => {
            // Small operand: products stay under the money ceiling.
            let sm = format!("{}::money", pick_str(g, &["'0'", "'1'", "'-2.5'", "'12.34'", "'1234567.89'"]));
            let k = pick_str(g, &["2", "3::int2", "2.5::float8", "0.5::float4", "1.25::numeric"]);
            format!("SELECT ({} * {})::text;", sm, k)
        }
        3 => {
            let k = pick_str(g, &["2", "3::int2", "2.5::float8", "4::int8", "1.25::numeric"]);
            format!("SELECT ({} / {})::text;", l, k)
        }
        4 => {
            // money / money -> float8.
            format!("SELECT ({} / '2.5'::money)::text;", l)
        }
        5 => {
            let src = pick_str(g, &["12", "12::int2", "12::int8", "12.34::numeric", "1.5::float8", "2.5::float4"]);
            format!("SELECT ({}::money)::text;", src)
        }
        6 => format!("SELECT ({}::numeric)::text;", l),
        _ => {
            format!("SELECT max(m)::text || '|' || min(m)::text FROM (VALUES ({}), ('7.77'::money)) v(m);", l)
        }
    }
}

/// oid comparison ops + oidvector comparisons/output over pg_proc rows
/// (fixed builtin procs: catalog-stable on both engines).
fn gen_oid(g: &mut Gen) -> String {
    g.fire("xnum:oid");
    let procs = ["int4pl", "int8pl", "textcat", "lower", "numeric_add"];
    let p = pick_str(g, &procs);
    match g.rng.below(6) {
        0 => {
            let a = g.rng.below(20000);
            let b = g.rng.below(20000);
            format!("SELECT ('{}'::oid {} '{}'::oid)::text;", a, pick_str(g, CMP), b)
        }
        1 => format!(
            "SELECT proargtypes::text FROM pg_proc WHERE proname = '{}' ORDER BY oid LIMIT 1;",
            p
        ),
        2 => format!(
            "SELECT (proargtypes {} '23 23'::oidvector)::text FROM pg_proc WHERE proname = '{}' ORDER BY oid LIMIT 1;",
            pick_str(g, CMP), p
        ),
        // Q2-F2 FIXED on main (fc_oidvectortypes, OID 1349 — PR #814):
        // the direct arm is armed alongside the comparison sweep.
        3 => {
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT oidvectortypes(proargtypes) FROM pg_proc WHERE proname = '{}' ORDER BY oid LIMIT 1;",
                    p
                )
            } else {
                format!(
                    "SELECT (proargtypes = proargtypes)::text, (proargtypes <= proargtypes)::text, (proargtypes <> '23'::oidvector)::text FROM pg_proc WHERE proname = '{}' ORDER BY oid LIMIT 1;",
                    p
                )
            }
        }
        4 => {
            let v = g.rng.below(100000);
            format!("SELECT ({}::oid::bigint)::text, ({}::bigint::oid)::text;", v, v)
        }
        _ => format!(
            "SELECT count(*) FROM pg_proc WHERE proargtypes[0] = 23 AND proname = '{}';",
            p
        ),
    }
}

/// pg_current_xact_id / pg_current_snapshot / pg_xact_status /
/// pg_visible_in_snapshot + pg_snapshot/xid8 I/O. Live xids differ
/// across engines, so live values only ever reach the compare surface
/// through engine-stable predicates; literal snapshot I/O compares raw.
fn gen_snap(g: &mut Gen) -> String {
    g.fire("xnum:snap");
    if err_arm(g) {
        return pick_str(g, &[
            "SELECT '20:10:15'::pg_snapshot;",
            "SELECT 'zz'::pg_snapshot;",
            "SELECT ''::xid8;",
            "SELECT pg_xact_status('1'::xid8);",
        ])
        .to_string();
    }
    match g.rng.below(8) {
        0 => "SELECT (pg_current_xact_id() >= '3'::xid8)::text;".to_string(),
        1 => "SELECT pg_xact_status(pg_current_xact_id());".to_string(),
        2 => "SELECT pg_visible_in_snapshot(pg_current_xact_id(), pg_current_snapshot())::text;".to_string(),
        3 => "SELECT (pg_snapshot_xmin(pg_current_snapshot()) <= pg_snapshot_xmax(pg_current_snapshot()))::text;".to_string(),
        4 => {
            let s = pick_str(g, &["10:20:10,15", "1:1:", "100:150:100,120,140", "10:10:"]);
            format!("SELECT ('{}'::pg_snapshot)::text;", s)
        }
        5 => {
            let s = pick_str(g, &["10:20:10,15", "100:150:100,120,140"]);
            format!(
                "SELECT pg_snapshot_xmin('{}')::text, pg_snapshot_xmax('{}')::text, pg_snapshot_xip('{}')::text;",
                s, s, s
            )
        }
        6 => {
            let (a, b) = (5 + g.rng.below(20), 5 + g.rng.below(20));
            format!(
                "SELECT ('{}'::xid8 {} '{}'::xid8)::text, max('{}'::xid8)::text, min('{}'::xid8)::text;",
                a, pick_str(g, CMP), b, a, b
            )
        }
        _ => {
            let s = pick_str(g, &["10:20:10,15", "7:7:"]);
            format!(
                "SELECT pg_visible_in_snapshot('6'::xid8, '{}')::text, pg_visible_in_snapshot('25'::xid8, '{}')::text;",
                s, s
            )
        }
    }
}

/// Float/numeric builtin breadth: cot/exp/pi/degrees/radians, degree
/// trig, hyperbolics, gcd/lcm, numeric_inc/int4inc, sign/scale.
fn gen_math(g: &mut Gen) -> String {
    g.fire("xnum:math");
    if err_arm(g) {
        return pick_str(g, &[
            "SELECT acosd(2)::text;",
            "SELECT asind(-1.5)::text;",
            "SELECT acosh(0.5)::text;",
            "SELECT atanh(2)::text;",
            "SELECT gcd(-9223372036854775808::int8, 0)::text;",
            "SELECT power(-8.0::numeric, 0.5::numeric)::text;",
            "SELECT lcm(9223372036854775807::int8, 2)::text;",
            "SELECT factorial(-1)::text;",
        ])
        .to_string();
    }
    let ang = pick_str(g, &["0", "30", "45", "60", "90", "180", "270", "360", "17.3", "-30"]);
    let x = pick_str(g, &["0", "0.5", "1.0", "-0.5", "2.2", "1.1"]);
    match g.rng.below(10) {
        0 => format!("SELECT cot({}::float8)::text, tan({}::float8)::text;", x, x),
        1 => format!("SELECT exp({}::float8)::text, exp({}::numeric)::text;", x, x),
        2 => "SELECT pi()::text, degrees(pi())::text, radians(180.0)::text;".to_string(),
        3 => format!("SELECT trunc({}::float8)::text, ceil({}::float8)::text, floor({}::float8)::text;", x, x, x),
        4 => format!("SELECT sind({})::text, cosd({})::text, tand({})::text;", ang, ang, ang),
        5 => format!("SELECT asind({}::float8)::text, acosd({}::float8)::text, atand({}::float8)::text;", x, x, x),
        6 => format!("SELECT sinh({}::float8)::text, cosh({}::float8)::text, tanh({}::float8)::text;", x, x, x),
        7 => format!("SELECT asinh({}::float8)::text, acosh(2.0::float8)::text, atanh({}::float8)::text;", x, x),
        8 => {
            let (a, b) = (g.rng.below(100000), g.rng.below(1000));
            match g.rng.below(3) {
                0 => format!("SELECT gcd({}, {})::text, lcm({}::int4, {}::int4)::text;", a, b, a % 1000, b),
                1 => format!("SELECT gcd({}::int8, {}::int8)::text, lcm({}::int8, {}::int8)::text;", a, b, a, b),
                _ => format!("SELECT gcd({}.5::numeric, {})::text, lcm({}::numeric, {})::text;", a, b, a % 100, b % 100),
            }
        }
        _ => {
            let n = pick_str(g, &["41.5", "-1.230", "0.000", "99999.99999"]);
            format!(
                "SELECT numeric_inc({})::text, int4inc({})::text, scale({}::numeric)::text, sign({}::numeric)::text, min_scale({}::numeric)::text, trim_scale({}::numeric)::text;",
                n, g.rng.below(1000), n, n, n, n
            )
        }
    }
}

/// width_bucket + round/trunc arity breadth.
fn gen_bucket(g: &mut Gen) -> String {
    g.fire("xnum:bucket");
    if err_arm(g) {
        return pick_str(g, &[
            "SELECT width_bucket(5.0, 1.0, 1.0, 10)::text;",
            "SELECT width_bucket(5.0, 1.0, 10.0, 0)::text;",
            "SELECT width_bucket(5.0::float8, 'NaN'::float8, 10.0, 3)::text;",
            "SELECT round(1.5, 100000)::text;",
        ])
        .to_string();
    }
    let v = pick_str(g, &["0.024", "5.35", "9.99", "-3.2", "10.06"]);
    match g.rng.below(5) {
        0 => format!("SELECT width_bucket({}, 0.024, 10.06, {})::text;", v, 1 + g.rng.below(10)),
        1 => format!(
            "SELECT width_bucket({}::float8, 0.024::float8, 10.06::float8, {})::text;",
            v, 1 + g.rng.below(10)
        ),
        2 => format!(
            "SELECT width_bucket({}::numeric, ARRAY[1, 3, 5, 7]::numeric[])::text, width_bucket('{}'::text, ARRAY['a', 'b', 'd'])::text;",
            v,
            pick_str(g, &["a", "c", "z"])
        ),
        3 => {
            let n = pick_str(g, &["42.4382", "-5.5", "0.5", "2.5", "999.994"]);
            format!(
                "SELECT round({})::text, round({}, {})::text, round({}::float8)::text;",
                n, n, g.rng.below(4), n
            )
        }
        _ => {
            let n = pick_str(g, &["42.4382", "-5.5", "0.5", "999.994"]);
            format!(
                "SELECT trunc({})::text, trunc({}, {})::text, trunc({}::float8)::text;",
                n, n, g.rng.below(4), n
            )
        }
    }
}

/// generate_series numeric/bigint (+ prosupport rowcount estimation via
/// EXPLAIN COSTS OFF is covered by the explain module; here just rows).
fn gen_series(g: &mut Gen) -> String {
    g.fire("xnum:series");
    if err_arm(g) {
        return pick_str(g, &[
            "SELECT count(*) FROM generate_series(1.0, 2.0, 0.0);",
            "SELECT count(*) FROM generate_series(1::bigint, 10::bigint, 0);",
            "SELECT count(*) FROM generate_series('NaN'::numeric, 5.0);",
        ])
        .to_string();
    }
    match g.rng.below(4) {
        0 => {
            let step = pick_str(g, &["0.5", "0.25", "1.0", "-0.5"]);
            let (a, b) = if step.starts_with('-') { ("3.0", "1.0") } else { ("1.0", "3.0") };
            format!(
                "SELECT string_agg(x::text, ',') FROM generate_series({}::numeric, {}::numeric, {}) x;",
                a, b, step
            )
        }
        1 => format!(
            "SELECT string_agg(x::text, ',') FROM generate_series({}::bigint, {}::bigint) x;",
            10000000000u64 + g.rng.below(5),
            10000000003u64 + g.rng.below(5)
        ),
        2 => format!(
            "SELECT string_agg(x::text, ',') FROM generate_series({}::bigint, {}::bigint, {}) x;",
            g.rng.below(10),
            20 + g.rng.below(10),
            2 + g.rng.below(5)
        ),
        _ => "SELECT count(*) FROM generate_series(1.0, 10.0, 0.1);".to_string(),
    }
}

/// pg_lsn: comparisons, +/- numeric, difference, hash, min/max, casts.
fn gen_lsn(g: &mut Gen) -> String {
    g.fire("xnum:lsn");
    let lsns = ["0/1", "0/0", "16/B374D848", "FFFFFFFF/FFFFFFFE", "1/AAAA0000"];
    let l = pick_str(g, &lsns);
    if err_arm(g) {
        return pick_str(g, &[
            "SELECT 'FFFFFFFF/FFFFFFFF'::pg_lsn + 1::numeric;",
            "SELECT '0/1'::pg_lsn - 2::numeric;",
            "SELECT 'notalsn'::pg_lsn;",
            "SELECT '0/1'::pg_lsn + 'NaN'::numeric;",
        ])
        .to_string();
    }
    match g.rng.below(7) {
        0 => {
            let r = pick_str(g, &lsns);
            format!("SELECT ('{}'::pg_lsn {} '{}'::pg_lsn)::text;", l, pick_str(g, CMP), r)
        }
        1 => format!("SELECT ('{}'::pg_lsn + {}::numeric)::text;", l, 1 + g.rng.below(1000)),
        2 => format!("SELECT ('16/B374D848'::pg_lsn - '{}'::pg_lsn)::text;", pick_str(g, &["0/1", "0/0", "1/AAAA0000"])),
        3 => format!(
            "SELECT max(x)::text || '|' || min(x)::text FROM (VALUES ('{}'::pg_lsn), ('0/2')) v(x);",
            l
        ),
        4 => format!("SELECT ({}::numeric::pg_lsn)::text;", g.rng.below(1000000000)),
        5 => format!(
            // Q7: pg_lsn hash support procs (deterministic int8/int4
            // outputs, identical algorithms on both engines).
            "SELECT pg_lsn_hash('{}'::pg_lsn), pg_lsn_hash_extended('{}'::pg_lsn, {});",
            l,
            l,
            g.rng.below(100)
        ),
        _ => format!("SELECT ('{}'::pg_lsn - {}::numeric)::text;", "16/B374D848", 1 + g.rng.below(100)),
    }
}

/// Non-decimal integer literals, string-cast parse paths, float special
/// literals, xid8/uint64 input.
fn gen_lit(g: &mut Gen) -> String {
    g.fire("xnum:lit");
    if err_arm(g) {
        return pick_str(g, &[
            "SELECT '0x'::numeric;",
            "SELECT '  '::int2;",
            "SELECT '99999999999999999999'::int8;",
            "SELECT '12abc'::int4;",
            "SELECT '0b2'::int4;",
        ])
        .to_string();
    }
    match g.rng.below(6) {
        0 => "SELECT ('0x1f'::numeric)::text, ('0o17'::numeric)::text, ('0b101'::numeric)::text;".to_string(),
        1 => format!("SELECT (0x{:X})::text, (0b1011)::text, (0o777)::text;", g.rng.below(65536)),
        2 => format!("SELECT ('  {} '::int2)::text, ('{}'::int8)::text;", g.rng.below(30000), g.rng.below(1u64 << 62)),
        3 => "SELECT ('NaN'::float4)::text, ('Infinity'::float4)::text, ('-Infinity'::float8)::text, ('nan'::numeric)::text;".to_string(),
        4 => format!("SELECT ('{}'::xid8)::text, ('{}'::xid)::text;", g.rng.below(1u64 << 40), g.rng.below(1u64 << 31)),
        _ => format!("SELECT (int8 '{}')::text, (int2 '{}')::text;", g.rng.below(1u64 << 60), g.rng.below(32000)),
    }
}

/// random()/random_normal()/random(lo,hi)/setseed: engine streams differ
/// even after setseed (banked Q2 observation), so only engine-stable
/// range predicates reach the compare surface.
fn gen_rand(g: &mut Gen) -> String {
    g.fire("xnum:rand");
    match g.rng.below(6) {
        0 => "SELECT random() >= 0.0 AND random() < 1.0;".to_string(),
        1 => {
            let (lo, hi) = (g.rng.below(50), 50 + g.rng.below(50));
            format!("SELECT random({}, {}) BETWEEN {} AND {};", lo, hi, lo, hi)
        }
        2 => {
            let (lo, hi) = (g.rng.below(1000), 100000 + g.rng.below(100000));
            format!(
                "SELECT random({}::bigint, {}::bigint) BETWEEN {} AND {};",
                lo, hi, lo, hi
            )
        }
        3 => format!(
            "SELECT random({}.{:02}, {}.99) BETWEEN 0 AND 1000;",
            g.rng.below(10),
            g.rng.below(100),
            500 + g.rng.below(400)
        ),
        4 => "SELECT random_normal() IS NOT NULL, random_normal(10.0, 2.0) IS NOT NULL;".to_string(),
        _ => format!("SELECT setseed({}.{:02});", if g.rng.chance(1, 2) { "0" } else { "-0" }, g.rng.below(100)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every shape + the err arm fires; textual invariants hold; the
    /// volatile random()/random_normal() family only ever appears inside
    /// engine-stable predicates (BETWEEN / range / IS NOT NULL).
    #[test]
    fn xnum_shapes_fire_and_hold_invariants() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0x2B02);
        let mut prods_all = Vec::new();
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_xnum_module(&mut g);
            assert_eq!(stmts.len(), 1);
            let sql = stmts[0].to_sql();
            assert!(sql.ends_with(';'), "{sql}");
            assert!(!sql.contains('\n'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            if sql.contains("random") {
                let stable = sql.contains("BETWEEN")
                    || sql.contains(">= 0.0 AND random() < 1.0")
                    || sql.contains("IS NOT NULL")
                    || sql.contains("setseed");
                assert!(stable, "raw volatile random on compare surface: {sql}");
            }
            prods_all.extend(prods);
        }
        for p in SHAPES.iter().chain(&["xnum:err"]) {
            assert!(prods_all.iter().any(|q| q == p), "{p} never fired");
        }
    }

    /// Same seed -> byte-identical statements.
    #[test]
    fn xnum_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(88);
            let mut out = Vec::new();
            for _ in 0..300 {
                let mut prods = Vec::new();
                let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                out.push(gen_xnum_module(&mut g)[0].to_sql());
            }
            out
        };
        assert_eq!(run(), run());
    }
}
