//! Float4/float8 math-function drain module (Track-B): the float.c surface
//! (backend/utils/adt/float — trig radian + degree variants, hyperbolic,
//! ln/log/log10/exp/pow/sqrt/cbrt, ceil/floor/round/trunc/sign/abs,
//! degrees/radians/pi, the arithmetic + comparison operator matrices, the
//! `@`/`|/`/`||/`/`^` operators, width_bucket, float<->int / float4<->float8
//! casts with out-of-range arms, float<->text round-trip, and the
//! sum/avg/var/stddev/regr aggregate family under forced-parallel plans).
//! Distinct from `numx` (NUMERIC is exact decimal); this module owns the
//! binary IEEE-754 float surface and its special values.
//!
//! Comparison law (per B1 ruling + crate::diff col_cmp_modes):
//!   * VALUE probes return a BARE float4/float8 column, so the differ
//!     compares them with the ulp comparator automatically (float surfaces
//!     get ulp comparators; both engines call the same platform libm, so
//!     transcendental results agree to <=4 ulp). A non-ulp value divergence
//!     is a real HIGH bug.
//!   * TEXT round-trip probes cast the value ::text, giving a text column
//!     that compares BYTE-EXACT — this is the shortest-repr (Ryu) surface;
//!     any mismatch is a HIGH bug.
//!   * ERROR arms are part of the surface: overflow (22003), domain
//!     (2201E log / 2201F power / etc.), div-by-zero (22012), width_bucket
//!     (2201G) and invalid-text (22P02) arms are emitted deliberately and
//!     matched on SQLSTATE (diff::classify), exact.
//!   * CAST-to-int and COMPARISON probes return int/bool columns, exact.
//!   * Sign-of-zero (-0.0) is masked by the ulp comparator, so it is tested
//!     only through the byte-exact ::text arm (floatmath:text:signzero).
//!
//! Accumulation-order pin: the aggregate family feeds EXACT-REPRESENTABLE
//! inputs (small integers / halves) so sum/avg are order-independent and
//! ride forced-parallel plans; the variance/stddev/regression family
//! (Youngs-Cramer, order-SENSITIVE) is pinned serial instead — parallel
//! chunk boundaries are nondeterministic and C's own combine legs drift
//! 1-7 ulp from serial on this fixture, beyond the 4-ulp budget (round-12
//! disposition; parallel-combine parity is pinned by adt_float's amd64
//! bit-parity unit tests). See findings-floatmath.md.
//!
//! Stateless: every probe is a self-contained one-statement group except
//! the aggregate family, which creates-and-drops a `fz_fma` fixture in-group
//! (numx::gen_sort / earm discipline).

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

const SHAPES: &[&str] = &[
    "floatmath:trig",
    "floatmath:trigd",
    "floatmath:hyp",
    "floatmath:explog",
    "floatmath:round",
    "floatmath:arith",
    "floatmath:cmp",
    "floatmath:conv",
    "floatmath:wb",
    "floatmath:cast",
    "floatmath:text",
    "floatmath:special",
    "floatmath:agg",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_floatmath_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath");
    match g.weights.pick(g.rng, SHAPES) {
        "floatmath:trig" => gen_trig(g),
        "floatmath:trigd" => gen_trigd(g),
        "floatmath:hyp" => gen_hyp(g),
        "floatmath:explog" => gen_explog(g),
        "floatmath:round" => gen_round(g),
        "floatmath:arith" => gen_arith(g),
        "floatmath:cmp" => gen_cmp(g),
        "floatmath:conv" => gen_conv(g),
        "floatmath:wb" => gen_wb(g),
        "floatmath:cast" => gen_cast(g),
        "floatmath:text" => gen_text(g),
        "floatmath:special" => gen_special(g),
        _ => gen_agg(g),
    }
}

// -------------------------------------------------------------- pools ----

/// Finite float8 operands spanning magnitude regimes; a mix of
/// exact-representable integers/halves and hard-to-round fractions.
const FIN8: &[&str] = &[
    "0.0",
    "1.0",
    "-1.0",
    "2.0",
    "0.5",
    "-0.5",
    "0.25",
    "-0.125",
    "3.5",
    "42.0",
    "0.1",
    "0.3",
    "3.141592653589793",
    "-2.718281828459045",
    "100.0",
    "1234.5",
    "1e10",
    "-1e-10",
    "9.999999999999999",
    "1.7976931348623157e308",
];

/// Radian angles (finite): cardinal fractions of pi + large/reduced.
const RAD: &[&str] = &[
    "0",
    "0.5",
    "1",
    "-1",
    "0.7853981633974483",
    "1.5707963267948966",
    "3.141592653589793",
    "6.283185307179586",
    "-0.7853981633974483",
    "100",
    "-100",
    "0.0001",
    "12345.678",
];

/// Degree angles: cardinal + out-of-[0,360] wrap + fractional.
const DEG: &[&str] = &[
    "0", "30", "45", "60", "90", "120", "135", "150", "180", "210", "270", "360", "-30", "-45",
    "-90", "405", "720", "30.5", "-360", "1000000",
];

/// Values inside the [-1,1] domain (asin/acos/atanh) incl. the endpoints.
const UNIT: &[&str] = &[
    "0",
    "0.5",
    "-0.5",
    "1",
    "-1",
    "0.9999",
    "-0.9999",
    "0.7071067811865476",
    "0.8660254037844387",
];

/// Special-value literal fragments (already valid float8 SQL, no cast).
const SPECIAL8: &[&str] = &[
    "'NaN'::float8",
    "'Infinity'::float8",
    "'-Infinity'::float8",
    "'-0'::float8",
];

/// Sub/near-limit magnitudes (subnormals, DBL_MIN/MAX).
const EXTREME8: &[&str] = &[
    "5e-324",
    "1e-320",
    "2.2250738585072014e-308",
    "1.7976931348623157e308",
    "-1.7976931348623157e308",
    "-5e-324",
];

fn ang_rad(g: &mut Gen) -> &'static str {
    if g.rng.chance(1, 6) {
        pick_str(g, SPECIAL8)
    } else {
        pick_str(g, RAD)
    }
}

// ---------------------------------------------------------------- trig ----

/// Radian trig: sin/cos/tan/cot/asin/acos/atan/atan2 — bare-float value
/// probes (ulp), plus the domain-error arm (asin/acos out of [-1,1] ->
/// 22003 input out of range).
fn gen_trig(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:trig");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:trig:one",
            "floatmath:trig:atan2",
            "floatmath:trig:err",
        ],
    );
    g.fire(shape);
    let sql = match shape {
        "floatmath:trig:one" => {
            let x = ang_rad(g);
            format!(
                "SELECT sin({x}::float8), cos({x}::float8), tan({x}::float8), \
                 cot({x}::float8), atan({x}::float8);"
            )
        }
        "floatmath:trig:atan2" => {
            let y = ang_rad(g);
            let x = ang_rad(g);
            format!("SELECT atan2({y}::float8, {x}::float8), atan2({x}::float8, {y}::float8);")
        }
        _ => {
            // asin/acos over the unit domain (ok) and deliberately out of it.
            let x = if g.rng.chance(1, 2) {
                pick_str(g, UNIT).to_string()
            } else {
                pick_str(g, &["1.5", "-2", "2", "100", "-1.0000001"]).to_string()
            };
            format!("SELECT asin({x}::float8), acos({x}::float8);")
        }
    };
    vec![raw(sql)]
}

/// Degree trig (PG12): sind/cosd/tand/cotd/asind/acosd/atand/atan2d — the
/// init_degree_constants exactness path (sind(30)=0.5 etc.). Bare-float
/// value probe; +-Inf argument -> 22003 (dsind/dcosd/etc. reject Inf).
fn gen_trigd(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:trigd");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:trigd:one",
            "floatmath:trigd:atan2",
            "floatmath:trigd:err",
        ],
    );
    g.fire(shape);
    let sql = match shape {
        "floatmath:trigd:one" => {
            let x = pick_str(g, DEG);
            format!(
                "SELECT sind({x}::float8), cosd({x}::float8), tand({x}::float8), cotd({x}::float8);"
            )
        }
        "floatmath:trigd:atan2" => {
            let x = pick_str(g, UNIT);
            let y = pick_str(g, DEG);
            format!(
                "SELECT asind({x}::float8), acosd({x}::float8), atand({y}::float8), \
                 atan2d({y}::float8, 1::float8);"
            )
        }
        _ => {
            // Inf argument to a degree function -> input out of range (22003);
            // asind/acosd out-of-unit-domain -> 22003.
            let x = pick_str(g, &["'Infinity'", "'-Infinity'"]);
            let u = pick_str(g, &["1.5", "-2", "3"]);
            format!("SELECT sind({x}::float8); SELECT asind({u}::float8);")
        }
    };
    vec![raw(sql)]
}

// ----------------------------------------------------------- hyperbolic ----

/// sinh/cosh/tanh/asinh/acosh/atanh — bare-float value probes; acosh(<1)
/// and atanh(|x|>1) -> 22003; atanh(+-1) yields +-Infinity (value arm).
fn gen_hyp(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:hyp");
    let shape = g
        .weights
        .pick(g.rng, &["floatmath:hyp:ok", "floatmath:hyp:err"]);
    g.fire(shape);
    let sql = match shape {
        "floatmath:hyp:ok" => {
            let x = pick_str(
                g,
                &["0", "0.5", "-0.5", "1", "-1", "2", "-2", "5", "-10", "700"],
            );
            let u = pick_str(g, UNIT);
            let ge1 = pick_str(g, &["1", "2", "10", "1000", "1.0000001", "1e100"]);
            format!(
                "SELECT sinh({x}::float8), cosh({x}::float8), tanh({x}::float8), \
                 asinh({x}::float8), acosh({ge1}::float8), atanh({u}::float8);"
            )
        }
        _ => {
            // acosh below 1 and atanh outside [-1,1] -> 22003.
            let lo = pick_str(g, &["0.5", "0", "-1", "0.9999"]);
            let out = pick_str(g, &["1.5", "-2", "100"]);
            format!("SELECT acosh({lo}::float8); SELECT atanh({out}::float8);")
        }
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------- exp/log ----

/// ln/log/log10/exp/power/sqrt/cbrt — bare-float value probes + the
/// overflow (22003) / domain (2201E log, 2201F power) error arms.
fn gen_explog(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:explog");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:explog:ln",
            "floatmath:explog:exp",
            "floatmath:explog:pow",
            "floatmath:explog:sqrt",
            "floatmath:explog:err",
        ],
    );
    g.fire(shape);
    let sql = match shape {
        "floatmath:explog:ln" => {
            let x = pick_str(
                g,
                &[
                    "1",
                    "2",
                    "2.718281828459045",
                    "10",
                    "100",
                    "0.5",
                    "0.001",
                    "1e100",
                    "1e-100",
                    "1.7976931348623157e308",
                ],
            );
            format!("SELECT ln({x}::float8), log({x}::float8), log10({x}::float8);")
        }
        "floatmath:explog:exp" => {
            let x = pick_str(
                g,
                &["0", "1", "-1", "2.5", "-2.5", "-700", "700", "709", "-745"],
            );
            format!("SELECT exp({x}::float8);")
        }
        "floatmath:explog:pow" => {
            let b = pick_str(
                g,
                &[
                    "0",
                    "1",
                    "-1",
                    "2",
                    "0.5",
                    "10",
                    "1.5",
                    "0.0001",
                    "'NaN'",
                    "'Infinity'",
                ],
            );
            let e = pick_str(
                g,
                &[
                    "0",
                    "1",
                    "-1",
                    "2",
                    "3",
                    "0.5",
                    "-0.5",
                    "12.5",
                    "-77",
                    "'Infinity'",
                ],
            );
            // Both the pow() function and the `^` operator.
            format!("SELECT power({b}::float8, {e}::float8), ({b}::float8 ^ {e}::float8);")
        }
        "floatmath:explog:sqrt" => {
            let x = pick_str(g, &["0", "1", "2", "4", "0.25", "1e300", "1e-300", "2.0"]);
            // sqrt() function, |/ operator, cbrt() and ||/ operator.
            let c = pick_str(g, &["0", "1", "-1", "8", "-27", "1e300", "-1e300"]);
            format!(
                "SELECT sqrt({x}::float8), (|/ {x}::float8), cbrt({c}::float8), (||/ {c}::float8);"
            )
        }
        _ => {
            // Domain + overflow error arms.
            match g.rng.below(4) {
                0 => "SELECT ln(0::float8); SELECT ln(-1::float8);".to_string(), // 2201E
                1 => "SELECT log(0::float8); SELECT log10(-2::float8);".to_string(), // 2201E
                2 => "SELECT sqrt(-1::float8); SELECT (|/ -4::float8);".to_string(), // 2201F
                // negative base to a non-integer power -> 2201F; overflow -> 22003.
                _ => "SELECT power(-2::float8, 0.5::float8); SELECT exp(1000::float8); \
                      SELECT power(10::float8, 400::float8);"
                    .to_string(),
            }
        }
    };
    vec![raw(sql)]
}

// -------------------------------------------------------- round family ----

/// ceil/ceiling/floor/round/trunc/sign/abs and the `@` (abs) operator.
/// round/trunc results are bare-float (ulp); sign/abs of special values.
fn gen_round(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:round");
    let shape = g
        .weights
        .pick(g.rng, &["floatmath:round:core", "floatmath:round:op"]);
    g.fire(shape);
    let x = if g.rng.chance(1, 5) {
        pick_str(g, SPECIAL8).to_string()
    } else {
        pick_str(
            g,
            &[
                "0.0",
                "0.5",
                "1.5",
                "2.5",
                "-0.5",
                "-1.5",
                "-2.5",
                "2.4",
                "2.6",
                "-2.4",
                "-2.6",
                "42.0",
                "0.4999999999999999",
                "1e16",
                "-0.0",
            ],
        )
        .to_string()
    };
    let sql = match shape {
        "floatmath:round:core" => format!(
            "SELECT ceil({x}::float8), ceiling({x}::float8), floor({x}::float8), \
             round({x}::float8), trunc({x}::float8), sign({x}::float8), abs({x}::float8);"
        ),
        // The `@` prefix operator is abs; also the float4 rounding surface.
        _ => format!(
            "SELECT @ ({x}::float8), ceil({x}::float4), floor({x}::float4), \
             round({x}::float4), trunc({x}::float4), sign({x}::float4);"
        ),
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------- arithmetic ----

/// float8/float4/mixed +,-,*,/ and unary minus — bare-float value probes;
/// div-by-zero (22012) and overflow (22003) error arms.
fn gen_arith(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:arith");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:arith:f8",
            "floatmath:arith:f4",
            "floatmath:arith:mixed",
            "floatmath:arith:err",
        ],
    );
    g.fire(shape);
    let a = pick_str(g, FIN8);
    let b = pick_str(g, FIN8);
    let sql = match shape {
        "floatmath:arith:f8" => format!(
            "SELECT ({a}::float8 + {b}::float8), ({a}::float8 - {b}::float8), \
             ({a}::float8 * {b}::float8), ({a}::float8 / {b}::float8), (- {a}::float8);"
        ),
        "floatmath:arith:f4" => format!(
            "SELECT ({a}::float4 + {b}::float4), ({a}::float4 - {b}::float4), \
             ({a}::float4 * {b}::float4), ({a}::float4 / {b}::float4);"
        ),
        "floatmath:arith:mixed" => format!(
            "SELECT ({a}::float4 + {b}::float8), ({a}::float8 - {b}::float4), \
             ({a}::float4 * {b}::float8), ({a}::float8 / {b}::float4);"
        ),
        _ => {
            // div-by-zero (22012) and overflow (22003).
            match g.rng.below(3) {
                0 => format!("SELECT ({a}::float8 / 0::float8);"),
                1 => "SELECT (1.5e308::float8 * 10::float8); \
                      SELECT (1.7976931348623157e308::float8 + 1.7976931348623157e308::float8);"
                    .to_string(),
                _ => "SELECT (3.4e38::float4 * 10::float4);".to_string(),
            }
        }
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------- comparison ----

/// The float comparison operator matrix (=,<>,<,<=,>,>=) across
/// float8/float4/mixed, incl. NaN ordering (PG: NaN sorts largest,
/// NaN = NaN is TRUE). Bool columns -> exact compare.
fn gen_cmp(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:cmp");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:cmp:f8",
            "floatmath:cmp:f4",
            "floatmath:cmp:mixed",
        ],
    );
    g.fire(shape);
    let pool: &[&str] = &[
        "0",
        "1",
        "-1",
        "0.5",
        "1e308",
        "-1e308",
        "'NaN'",
        "'Infinity'",
        "'-Infinity'",
        "'-0'",
    ];
    let a = pick_str(g, pool);
    let b = pick_str(g, pool);
    let (ta, tb) = match shape {
        "floatmath:cmp:f8" => ("float8", "float8"),
        "floatmath:cmp:f4" => ("float4", "float4"),
        _ => ("float4", "float8"),
    };
    let sql = format!(
        "SELECT ({a}::{ta} = {b}::{tb}), ({a}::{ta} <> {b}::{tb}), ({a}::{ta} < {b}::{tb}), \
         ({a}::{ta} <= {b}::{tb}), ({a}::{ta} > {b}::{tb}), ({a}::{ta} >= {b}::{tb});"
    );
    vec![raw(sql)]
}

// ------------------------------------------------------- degrees/radians ----

/// degrees/radians/pi — bare-float value probes; degrees/radians of a
/// huge magnitude can overflow (22003, float8_div/float8_mul).
fn gen_conv(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:conv");
    let shape = g
        .weights
        .pick(g.rng, &["floatmath:conv:dr", "floatmath:conv:pi"]);
    g.fire(shape);
    let sql = match shape {
        "floatmath:conv:dr" => {
            let x = pick_str(
                g,
                &[
                    "0",
                    "1",
                    "3.141592653589793",
                    "180",
                    "90",
                    "-45",
                    "6.283185307179646",
                    "'Infinity'",
                    "'NaN'",
                ],
            );
            format!("SELECT degrees({x}::float8), radians({x}::float8);")
        }
        _ => {
            // pi() plus a deliberate overflow through degrees() of DBL_MAX.
            if g.rng.chance(1, 4) {
                "SELECT degrees(1.7976931348623157e308::float8);".to_string() // 22003
            } else {
                "SELECT pi(), radians(degrees(pi()));".to_string()
            }
        }
    };
    vec![raw(sql)]
}

// -------------------------------------------------------- width_bucket ----

/// width_bucket(operand, low, high, count) over float8 — int result
/// (exact); the 2201G arms (count<=0, NaN operand/bound, infinite bound,
/// low=high) and the count+1 int overflow arm.
fn gen_wb(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:wb");
    let shape = g
        .weights
        .pick(g.rng, &["floatmath:wb:ok", "floatmath:wb:err"]);
    g.fire(shape);
    let sql = match shape {
        "floatmath:wb:ok" => {
            let op = pick_str(
                g,
                &[
                    "-5",
                    "0",
                    "0.5",
                    "5",
                    "5.5",
                    "9.999",
                    "10",
                    "15",
                    "'Infinity'",
                    "'-Infinity'",
                ],
            );
            let (lo, hi) = if g.rng.chance(1, 2) {
                ("0", "10")
            } else {
                ("10", "0")
            };
            let n = pick_str(g, &["1", "4", "5", "10", "100"]);
            format!("SELECT width_bucket({op}::float8, {lo}::float8, {hi}::float8, {n});")
        }
        _ => match g.rng.below(4) {
            0 => "SELECT width_bucket(5::float8, 0::float8, 10::float8, 0);".to_string(), // count<=0
            1 => "SELECT width_bucket('NaN'::float8, 0::float8, 10::float8, 5);".to_string(), // NaN
            2 => "SELECT width_bucket(5::float8, '-Infinity'::float8, 10::float8, 5);".to_string(), // inf bound
            _ => "SELECT width_bucket(5::float8, 3::float8, 3::float8, 5);".to_string(), // low=high
        },
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------- casts ----

/// float<->int (int2/int4/int8) with round-half-to-even, float4<->float8,
/// and the out-of-range (22003) arms. Int/float target columns compare
/// exact / ulp respectively; ::text used where the integer identity is the
/// point.
fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:cast");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:cast:toint",
            "floatmath:cast:tofloat",
            "floatmath:cast:err",
        ],
    );
    g.fire(shape);
    let sql = match shape {
        "floatmath:cast:toint" => {
            // rint round-half-to-even: 0.5->0, 1.5->2, 2.5->2, -0.5->0.
            let x = pick_str(
                g,
                &[
                    "0.5",
                    "1.5",
                    "2.5",
                    "3.5",
                    "-0.5",
                    "-1.5",
                    "-2.5",
                    "1.9",
                    "-1.9",
                    "32767.4",
                    "-32768.4",
                    "2147483647.0",
                    "100.5",
                ],
            );
            format!(
                "SELECT {x}::float8::int2, {x}::float8::int4, {x}::float8::int8, {x}::float4::int4;"
            )
        }
        "floatmath:cast:tofloat" => {
            let i = pick_str(
                g,
                &[
                    "0",
                    "1",
                    "-1",
                    "32767",
                    "-32768",
                    "2147483647",
                    "-2147483648",
                    "9223372036854775807",
                ],
            );
            let f = pick_str(
                g,
                &["0.1", "3.141592653589793", "1e38", "1e-38", "1.5", "-2.25"],
            );
            format!(
                "SELECT {i}::int4::float8, {i}::int4::float4, {i}::int8::float8, \
                 {f}::float8::float4, {f}::float4::float8;"
            )
        }
        _ => match g.rng.below(5) {
            0 => "SELECT 40000::float8::int2; SELECT (-40000)::float8::int2;".to_string(), // 22003
            1 => "SELECT 3e9::float8::int4; SELECT (-3e9)::float8::int4;".to_string(),     // 22003
            2 => "SELECT 1e19::float8::int8;".to_string(),                                 // 22003
            3 => "SELECT 'NaN'::float8::int4; SELECT 'Infinity'::float8::int8;".to_string(), // 22003
            // float8 -> float4 over/underflow.
            _ => "SELECT 1e300::float8::float4; SELECT 1e-300::float8::float4;".to_string(),
        },
    };
    vec![raw(sql)]
}

// ---------------------------------------------------------- text I/O ----

/// float<->text: shortest-repr (Ryu) round-trip is BYTE-EXACT; float8in/
/// float4in parsing of special/edge textual forms (::text, exact); the
/// invalid-text (22P02) / out-of-range (22003) arms; and the sign-of-zero
/// byte-exact arm (masked by the ulp comparator, only visible in text).
fn gen_text(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:text");
    let shape = g.weights.pick(
        g.rng,
        &[
            "floatmath:text:roundtrip",
            "floatmath:text:parse",
            "floatmath:text:signzero",
        ],
    );
    g.fire(shape);
    let sql = match shape {
        "floatmath:text:roundtrip" => {
            // Hard shortest-representation values for both widths.
            let v = pick_str(
                g,
                &[
                    "0.1",
                    "0.2",
                    "0.3",
                    "1.1",
                    "3.141592653589793",
                    "2.220446049250313e-16",
                    "1e20",
                    "1e-20",
                    "123456789.12345679",
                    "1e308",
                    "5e-324",
                    "0.0",
                    "-0.0",
                    "9007199254740993",
                    "0.6822871999174",
                    "'NaN'",
                    "'Infinity'",
                    "'-Infinity'",
                ],
            );
            format!("SELECT {v}::float8::text, {v}::float4::text;")
        }
        "floatmath:text:parse" => {
            let ok = pick_str(
                g,
                &[
                    "'inf'",
                    "'-inf'",
                    "'infinity'",
                    "'nan'",
                    "'  3.14  '",
                    "'.5'",
                    "'5.'",
                    "'1e10'",
                    "'-1E-10'",
                    "'1e-500'",
                    "'+0'",
                    "'-0'",
                ],
            );
            format!("SELECT {ok}::float8::text, {ok}::float4::text;")
        }
        _ => {
            // Sign-of-zero preservation: only the ::text arm can witness it.
            let v = pick_str(
                g,
                &[
                    "-0.0::float8",
                    "(0.0::float8 * -1)",
                    "(-1.0::float8 * 0.0)",
                    "sind(180::float8)",
                    "tand(0::float8)",
                    "(- 0.0::float8)",
                    "floor(-0.5::float8)",
                    "round(-0.4::float8)",
                ],
            );
            format!("SELECT ({v})::text;")
        }
    };
    let mut out = vec![raw(sql)];
    // Occasionally attach a deliberate parse/overflow error probe.
    if g.rng.chance(1, 4) {
        g.fire("floatmath:text:err");
        let bad = pick_str(g, &["'abc'", "'1.5x'", "''", "'1e'", "' '", "'0x1p4'"]);
        out.push(raw(format!("SELECT {bad}::float8;"))); // 22P02
        if g.rng.chance(1, 2) {
            out.push(raw("SELECT '1e400'::float8;".to_string())); // 22003 out of range
        }
    }
    out
}

// -------------------------------------------------------- special-value ----

/// Special-value propagation through the operator/function surface:
/// NaN/Inf/-Inf/subnormal/DBL_MAX. Value arms are bare-float (ulp); the
/// comparison arm returns bool (exact) to pin NaN ordering.
fn gen_special(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:special");
    let shape = g
        .weights
        .pick(g.rng, &["floatmath:special:prop", "floatmath:special:cmp"]);
    g.fire(shape);
    let sql = match shape {
        "floatmath:special:prop" => {
            let s = pick_str(g, SPECIAL8);
            let e = pick_str(g, EXTREME8);
            format!(
                "SELECT sqrt(abs({s})), sin({s}), exp({s}), ({s} + 1::float8), \
                 ({s} * 0::float8), atan({s}), ({e}::float8 * 2::float8), abs({e}::float8);"
            )
        }
        _ => {
            // NaN ordering + special comparisons (bool, exact).
            format!(
                "SELECT ('NaN'::float8 = 'NaN'::float8), ('NaN'::float8 > 'Infinity'::float8), \
                 ('Infinity'::float8 > 1e308::float8), ('-0'::float8 = 0::float8), \
                 (5e-324::float8 > 0::float8), (least('NaN'::float8, 1::float8)), \
                 (greatest('NaN'::float8, 1::float8));"
            )
        }
    };
    vec![raw(sql)]
}

// ------------------------------------------------------------- aggregates ----

/// float8/float4 aggregates (sum/avg/var_*/stddev_*/regr_*/corr) under
/// forced-parallel plans over an EXACT-REPRESENTABLE integer fixture, so
/// the result is accumulation-order independent (the pgrust and C planners
/// may pick different plans). The fixture is created and dropped in-group.
fn gen_agg(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("floatmath:agg");
    let shape = g
        .weights
        .pick(g.rng, &["floatmath:agg:build", "floatmath:agg:stat"]);
    g.fire(shape);
    let t = "fz_fma";
    let n: u64 = 200 + g.rng.below(300);
    // Exact-representable float8 values: integers and halves whose partial
    // sums are exact in IEEE-754 -> order-independent sum/avg.
    let vexpr = "((i % 64)::float8) / 2.0";
    let wexpr = "((i % 32) - 16)::float8";
    let mut out = vec![
        raw(format!(
            "CREATE TABLE {t} (i int4 PRIMARY KEY, v float8, w float8, r float4);"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT i, {vexpr}, {wexpr}, ((i % 16)::float4) \
             FROM generate_series(1, {n}) i;"
        )),
        // Forced-parallel GUCs, applied identically on both differential sides.
        raw("SET max_parallel_workers_per_gather = 4;".to_string()),
        raw("SET parallel_setup_cost = 0;".to_string()),
        raw("SET parallel_tuple_cost = 0;".to_string()),
        raw("SET min_parallel_table_scan_size = 0;".to_string()),
    ];
    match shape {
        "floatmath:agg:build" => {
            // Order-independent aggregates: sum/avg/count/min/max over exact
            // inputs are byte-identical regardless of plan.
            out.push(raw(format!(
                "SELECT sum(v), avg(v), count(v), min(v), max(v), sum(r), avg(r) FROM {t};"
            )));
            out.push(raw(format!(
                "SELECT sum(v + w), avg(v * 2::float8), max(abs(w)) FROM {t};"
            )));
        }
        _ => {
            // Variance/stddev/regression (Youngs-Cramer): the transition is
            // NOT order-independent, and a parallel plan's chunk boundaries
            // are nondeterministic (dynamic block assignment; the launched
            // worker count also varies with pool pressure under the mixed
            // workload) — so the combine-order drift is not bounded by the
            // 4-ulp budget. Round-12 (run 9348c12d...-59-13, seeds
            // 3446924009097215843 / 2383620390565615383 /
            // 1345841888102124772): C's OWN combine legs drift 1-7 ulp from
            // its serial leg on this very fixture (measured against pgdg
            // amd64 18), so the previous claim that the drift "stays within
            // the ulp budget" was simply wrong, and both-sides-forced-
            // parallel still diverged whenever the two engines landed on
            // different chunkings. The stat family is therefore pinned
            // SERIAL: deterministic C-ordered accumulation over the fixed
            // fixture is bit-exact on both engines (stronger than ulp), and
            // the combine/parallel transition parity is pinned instead by
            // adt_float's amd64 bit-parity unit tests.
            out.push(raw("SET max_parallel_workers_per_gather = 0;".to_string()));
            out.push(raw(format!(
                "SELECT var_pop(v), var_samp(v), stddev_pop(v), stddev_samp(v) FROM {t};"
            )));
            out.push(raw(format!(
                "SELECT corr(v, w), covar_pop(v, w), covar_samp(v, w), \
                 regr_slope(v, w), regr_intercept(v, w), regr_r2(v, w), \
                 regr_count(v, w), regr_avgx(v, w), regr_avgy(v, w) FROM {t};"
            )));
        }
    }
    out.push(raw("RESET max_parallel_workers_per_gather;".to_string()));
    out.push(raw("RESET parallel_setup_cost;".to_string()));
    out.push(raw("RESET parallel_tuple_cost;".to_string()));
    out.push(raw("RESET min_parallel_table_scan_size;".to_string()));
    out.push(raw(format!("DROP TABLE {t};")));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every top-level shape fires; every emitted statement is a
    /// well-formed one-liner (balanced parens, ';'-terminated, no newline).
    #[test]
    fn floatmath_shapes_fire_and_hold_invariants() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xF10A7);
        let mut prods_all = Vec::new();
        for _ in 0..6000 {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts = gen_floatmath_module(&mut g);
            assert!(!stmts.is_empty());
            for kind in &stmts {
                let sql = kind.to_sql();
                assert!(!sql.is_empty(), "empty sql");
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
            }
            prods_all.extend(prods);
        }
        for p in SHAPES {
            assert!(prods_all.iter().any(|q| q == p), "{p} never fired");
        }
        // The aggregate family always brackets its fixture with a matching
        // CREATE/DROP and its forced-parallel SETs with RESETs.
        assert!(prods_all.iter().any(|q| q == "floatmath:agg"));
    }

    /// Same seed -> byte-identical statement stream.
    #[test]
    fn floatmath_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(4242);
            let mut out = Vec::new();
            for _ in 0..400 {
                let mut prods = Vec::new();
                let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                for kind in gen_floatmath_module(&mut g) {
                    out.push(kind.to_sql());
                }
            }
            out
        };
        assert_eq!(run(), run());
    }

    /// Round-12: every order-SENSITIVE stat aggregate probe
    /// (var/stddev/corr/covar/regr over fz_fma) must run under the serial
    /// pin — a `SET max_parallel_workers_per_gather = 0;` after the
    /// forced-parallel block and before the first stat SELECT — because
    /// parallel combine chunking is nondeterministic and its legitimate
    /// drift exceeds the 4-ulp budget.
    #[test]
    fn stat_aggregates_are_serial_pinned() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xA66);
        let mut saw_stat = false;
        for _ in 0..4000 {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            let stmts: Vec<String> =
                gen_floatmath_module(&mut g).iter().map(|s| s.to_sql()).collect();
            let stat_idx = stmts.iter().position(|s| s.contains("var_pop"));
            if let Some(si) = stat_idx {
                saw_stat = true;
                let pin = stmts
                    .iter()
                    .position(|s| s == "SET max_parallel_workers_per_gather = 0;");
                let forced = stmts
                    .iter()
                    .position(|s| s == "SET max_parallel_workers_per_gather = 4;");
                assert!(
                    matches!((forced, pin), (Some(fp), Some(p)) if fp < p && p < si),
                    "stat probe not serial-pinned: {stmts:?}"
                );
                assert!(
                    stmts.iter().any(|s| s == "RESET max_parallel_workers_per_gather;"),
                    "missing RESET: {stmts:?}"
                );
            }
        }
        assert!(saw_stat, "floatmath:agg:stat never fired in 4000 groups");
    }
}
