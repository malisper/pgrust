//! TYPEIO: type-I/O + operator-residue drain for the network / mac / varbit
//! families plus the float4 to_char picture matrix, the moving-aggregate
//! inverse-transition surface, and the datetime out-of-range error arms.
//!
//! Aim: gap-report-010's utils/adt residue that the current adtmisc/agg/dtm
//! coverage does NOT already reach. gap-report-010 was generated with an
//! older fuzzgen (fuzzgen_sha=3fe07ec4b3e, pre-dating gen_inet2's operator
//! tail and agg's macaddr hash arms) so its per-file counts overstate what
//! is live on main; this module targets only the arms verified uncovered by
//! reading current main's emitters:
//!
//!   - network.c ordering/comparison + min/max (network_cmp / network_eq /
//!     network_ne / network_lt / network_le / network_gt / network_ge /
//!     network_sub / network_sup / network_smaller / network_larger). The
//!     `<<` / `&&` / `>>=` / `+` / `-` / `~` / `&` / `|` arms are already
//!     drained by adtmisc::gen_inet2, so this module adds only `>>` / `<<=`
//!     and the btree comparison + aggregate surface.
//!   - mac.c operators + ordering (macaddr_and `&`, macaddr_or `|`,
//!     macaddr_not `~`, macaddr_trunc trunc(), macaddr_cmp / lt / le / gt /
//!     ge / ne / eq). agg::aggx_hashx already drains the hash surface.
//!   - mac8.c operators, casts + ordering (macaddr8_and / or / not / trunc /
//!     set7bit, macaddrtomacaddr8, macaddr8tomacaddr incl. the ff:fe
//!     round-trip requirement as an error arm, macaddr8_cmp_internal + the
//!     comparison operators).
//!   - varbit.c shift/bitwise/comparison (bitshiftright `>>`, bitshiftleft
//!     `<<`, bitxor `#`, bitnot `~`, bitand `&`, bitor `|`, bitcmp / lt /
//!     le / gt / ne, plus get_bit + int casts).
//!   - formatting.c float4_to_char: the picture-code matrix (EEEE / RN / FM /
//!     PR / MI / PL / SG / TH / V / D / G / S) that adtmisc's three arms miss.
//!   - numeric.c / int agg residue: moving-window inverse transitions
//!     (int2/int4/int8_accum_inv + int4/int8_avg_accum_inv via a sliding
//!     ROWS frame) and in_range_numeric_numeric (RANGE BETWEEN numeric
//!     offsets over a numeric ORDER BY), plus numeric_fac (factorial()).
//!   - timestamp.c / date.c out-of-range error arms (tm2timestamp /
//!     date +/- overflow, make_* field validation) — deterministic error
//!     identity, a surface dtm's success arms do not reach.
//!
//! Discipline: every statement is a single-line, `;`-terminated,
//! balanced-paren `StmtKind::Raw` built from deterministic literals (no
//! wall-clock, no `now()`/`current_*`). Scalar results are cast `::text`
//! for byte-exact comparison; float-derived numbers are never emitted raw
//! (float4_to_char output is deterministic text; the moving-window
//! aggregates are integer/numeric-exact). Error probes are isolated as
//! standalone statements so error identity is compared per statement.

use crate::stmt::Gen;
use crate::stmt::StmtKind;

/// Weighted production names (all registered in weights::PROD_WEIGHTS).
const SHAPES: &[&str] = &[
    "tio:netord",
    "tio:mac",
    "tio:mac8",
    "tio:varbit",
    "tio:fmtf4",
    "tio:numwin",
    "tio:dtedge",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_typeio_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("typeio");
    match g.weights.pick(g.rng, SHAPES) {
        "tio:netord" => gen_netord(g),
        "tio:mac" => gen_mac(g),
        "tio:mac8" => gen_mac8(g),
        "tio:varbit" => gen_varbit(g),
        "tio:fmtf4" => gen_fmtf4(g),
        "tio:numwin" => gen_numwin(g),
        "tio:dtedge" => gen_dtedge(g),
        other => unreachable!("unknown typeio shape {other}"),
    }
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

/// inet/cidr btree comparison + ordering + min/max, and the subnet
/// containment operators adtmisc::gen_inet2 leaves out (`>>`, `<<=`).
/// network_cmp/eq/ne/lt/le/gt/ge/sub/sup/smaller/larger.
fn gen_netord(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:netord");
    let a: &[&str] = &[
        "10.0.0.1/8", "10.0.0.5/8", "10.1.2.3/32", "192.168.1.1/24",
        "192.168.1.0/24", "0.0.0.0/0", "255.255.255.255/32", "::1",
        "2001:db8::1/64", "fe80::1/64",
    ];
    let x = g.rng.pick(a);
    let y = g.rng.pick(a);
    let sql = match g.rng.below(5) {
        0 => format!(
            "SELECT v::text FROM (VALUES ('{}'::inet), ('{}'::inet), ('{}'::inet), ('{}'::inet)) t(v) ORDER BY v, v::text;",
            g.rng.pick(a), g.rng.pick(a), g.rng.pick(a), g.rng.pick(a)
        ),
        1 => format!(
            "SELECT min(v)::text, max(v)::text FROM (VALUES ('{}'::inet), ('{}'::inet), ('{}'::inet), ('{}'::inet)) t(v);",
            g.rng.pick(a), g.rng.pick(a), g.rng.pick(a), g.rng.pick(a)
        ),
        2 => format!(
            "SELECT ('{x}'::inet < '{y}'::inet), ('{x}'::inet <= '{y}'::inet), ('{x}'::inet > '{y}'::inet), ('{x}'::inet >= '{y}'::inet), ('{x}'::inet = '{y}'::inet), ('{x}'::inet <> '{y}'::inet);",
            x = x, y = y
        ),
        3 => format!(
            "SELECT ('{x}'::inet >> '{y}'::inet), ('{x}'::inet <<= '{y}'::inet), ('{x}'::inet >>= '{y}'::inet), ('{x}'::inet << '{y}'::inet);",
            x = x, y = y
        ),
        _ => format!(
            "SELECT least('{x}'::inet, '{y}'::inet)::text, greatest('{x}'::inet, '{y}'::inet)::text;",
            x = x, y = y
        ),
    };
    raw(sql)
}

/// macaddr bitwise + trunc + comparison/ordering.
/// macaddr_and/or/not/trunc/cmp/lt/le/gt/ge/ne/eq.
fn gen_mac(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:mac");
    let m: &[&str] = &[
        "08:00:2b:01:02:03", "01:02:03:04:05:06", "ff:ff:ff:ff:ff:ff",
        "00:00:00:00:00:00", "12:34:56:78:9a:bc", "de:ad:be:ef:00:11",
    ];
    let a = g.rng.pick(m);
    let b = g.rng.pick(m);
    let sql = match g.rng.below(4) {
        0 => format!(
            "SELECT ('{a}'::macaddr & '{b}'::macaddr)::text, ('{a}'::macaddr | '{b}'::macaddr)::text, (~'{a}'::macaddr)::text, trunc('{a}'::macaddr)::text;",
            a = a, b = b
        ),
        1 => format!(
            "SELECT ('{a}'::macaddr < '{b}'::macaddr), ('{a}'::macaddr <= '{b}'::macaddr), ('{a}'::macaddr > '{b}'::macaddr), ('{a}'::macaddr >= '{b}'::macaddr), ('{a}'::macaddr = '{b}'::macaddr), ('{a}'::macaddr <> '{b}'::macaddr);",
            a = a, b = b
        ),
        2 => format!(
            "SELECT v::text FROM (VALUES ('{}'::macaddr), ('{}'::macaddr), ('{}'::macaddr), ('{}'::macaddr)) t(v) ORDER BY v;",
            g.rng.pick(m), g.rng.pick(m), g.rng.pick(m), g.rng.pick(m)
        ),
        _ => format!(
            "SELECT min(v)::text, max(v)::text FROM (VALUES ('{}'::macaddr), ('{}'::macaddr), ('{}'::macaddr)) t(v);",
            g.rng.pick(m), g.rng.pick(m), g.rng.pick(m)
        ),
    };
    raw(sql)
}

/// macaddr8 bitwise + trunc + set7bit + casts + comparison/ordering.
/// macaddr8_and/or/not/trunc/set7bit, macaddrtomacaddr8, macaddr8tomacaddr,
/// macaddr8_cmp_internal + comparison operators.
fn gen_mac8(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:mac8");
    let m: &[&str] = &[
        "08:00:2b:01:02:03:04:05", "01:02:03:04:05:06:07:08",
        "ff:ff:ff:ff:ff:ff:ff:ff", "00:00:00:00:00:00:00:00",
        "12:34:56:78:9a:bc:de:f0",
    ];
    let a = g.rng.pick(m);
    let b = g.rng.pick(m);
    let sql = match g.rng.below(6) {
        0 => format!(
            "SELECT ('{a}'::macaddr8 & '{b}'::macaddr8)::text, ('{a}'::macaddr8 | '{b}'::macaddr8)::text, (~'{a}'::macaddr8)::text;",
            a = a, b = b
        ),
        1 => format!(
            "SELECT trunc('{a}'::macaddr8)::text, macaddr8_set7bit('{a}'::macaddr8)::text;",
            a = a
        ),
        2 => format!(
            "SELECT ('{a}'::macaddr8 < '{b}'::macaddr8), ('{a}'::macaddr8 <= '{b}'::macaddr8), ('{a}'::macaddr8 > '{b}'::macaddr8), ('{a}'::macaddr8 >= '{b}'::macaddr8), ('{a}'::macaddr8 = '{b}'::macaddr8), ('{a}'::macaddr8 <> '{b}'::macaddr8);",
            a = a, b = b
        ),
        3 => format!(
            "SELECT v::text FROM (VALUES ('{}'::macaddr8), ('{}'::macaddr8), ('{}'::macaddr8)) t(v) ORDER BY v;",
            g.rng.pick(m), g.rng.pick(m), g.rng.pick(m)
        ),
        // macaddr->macaddr8 widening (always valid) + the ff:fe round-trip.
        4 => "SELECT ('08:00:2b:01:02:03'::macaddr::macaddr8)::text, ('08:00:2b:ff:fe:04:05:06'::macaddr8::macaddr)::text;".to_string(),
        // macaddr8->macaddr where bytes 4-5 are not ff:fe: matched error.
        _ => "SELECT ('08:00:2b:01:02:03:04:05'::macaddr8::macaddr)::text;".to_string(),
    };
    raw(sql)
}

/// varbit shift/bitwise/comparison + get_bit + int casts.
/// bitshiftright/left, bitxor, bitnot, bitand, bitor, bitcmp/lt/le/gt/ne.
fn gen_varbit(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:varbit");
    let sql = match g.rng.below(6) {
        0 => {
            let s = g.rng.below(9);
            format!(
                "SELECT (B'11010011'::bit(8) >> {s})::text, (B'11010011'::bit(8) << {s})::text;",
                s = s
            )
        }
        1 => "SELECT (B'11010011'::bit(8) # B'10110001'::bit(8))::text, (~B'11010011'::bit(8))::text, (B'11010011'::bit(8) & B'10110001'::bit(8))::text, (B'11010011'::bit(8) | B'10110001'::bit(8))::text;".to_string(),
        2 => "SELECT (B'101' < B'110'), (B'101' <= B'101'), (B'110' > B'101'), (B'101' >= B'101'), (B'101' = B'101'), (B'101' <> B'110');".to_string(),
        3 => "SELECT v::text FROM (VALUES (B'101'::varbit), (B'1101'::varbit), (B'10'::varbit), (B'1010'::varbit)) t(v) ORDER BY v, v::text;".to_string(),
        4 => {
            let i = g.rng.below(8);
            format!("SELECT get_bit(B'10110010'::bit(8), {i}), (B'1010'::bit(4))::int4, (B'10101010'::bit(8))::int8;", i = i)
        }
        // matched length-mismatch error for bitxor.
        _ => "SELECT (B'1101' # B'101')::text;".to_string(),
    };
    raw(sql)
}

/// float4 to_char picture-code matrix (formatting.c float4_to_char) — the
/// EEEE / RN / FM / PR / MI / PL / SG / TH / V / D / G / S codes that
/// adtmisc's three float4 arms miss. Output is deterministic text.
fn gen_fmtf4(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:fmtf4");
    let v: &[&str] = &[
        "1234.5678", "-42.5", "0.00123", "42", "9.99e9", "0.5",
        "-1.5", "1000000", "3.25e-4", "1234",
    ];
    let n = g.rng.pick(v);
    let sql = match g.rng.below(5) {
        0 => format!(
            "SELECT to_char({n}::float4, '999,999.99'), to_char({n}::float4, 'FM990D000'), to_char({n}::float4, 'S999999.99');",
            n = n
        ),
        1 => format!(
            "SELECT to_char({n}::float4, '9.99EEEE'), to_char({n}::float4, 'FM9.999EEEE'), to_char({n}::float4, '0.9EEEE');",
            n = n
        ),
        2 => format!(
            "SELECT to_char({n}::float4, '999.99PR'), to_char({n}::float4, 'MI999.99'), to_char({n}::float4, 'PL999.99'), to_char({n}::float4, 'SG999.99');",
            n = n
        ),
        3 => format!(
            "SELECT to_char({n}::float4, '999TH'), to_char({n}::float4, '999V99'), to_char({n}::float4, 'FM999G999D99');",
            n = n
        ),
        // RN (roman) requires an integer in 1..3999.
        _ => "SELECT to_char(1234::float4, 'RN'), to_char(49::float4, 'FMRN'), to_char(3888::float4, 'RN');".to_string(),
    };
    raw(sql)
}

/// Moving-window inverse transitions (int2/int4/int8_accum_inv +
/// int4/int8_avg_accum_inv via a sliding ROWS frame) and
/// in_range_numeric_numeric (RANGE BETWEEN numeric offsets), plus
/// numeric_fac. Integer/numeric-exact outputs.
fn gen_numwin(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:numwin");
    let sql = match g.rng.below(4) {
        // Sliding ROWS frame forces inverse transitions as rows leave the
        // trailing edge: int4 sum/avg + int8 sum/avg.
        0 => "SELECT i, sum(v) OVER w, avg(v) OVER w, min(v) OVER w, max(v) OVER w FROM (VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)) t(i,v) WINDOW w AS (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ORDER BY i;".to_string(),
        1 => "SELECT i, sum(v) OVER w, avg(v) OVER w FROM (VALUES (1,1000000000000::int8),(2,2000000000000),(3,3000000000000),(4,4000000000000),(5,5000000000000)) t(i,v) WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY i;".to_string(),
        // in_range_numeric_numeric: numeric ORDER BY with numeric offsets.
        2 => "SELECT i, count(*) OVER (ORDER BY n RANGE BETWEEN 1.5 PRECEDING AND 2.5 FOLLOWING), sum(i) OVER (ORDER BY n RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) FROM (VALUES (1,1.0::numeric),(2,2.5),(3,4.0),(4,4.5),(5,7.0)) t(i,n) ORDER BY i, n;".to_string(),
        _ => "SELECT factorial(0), factorial(1), factorial(12), factorial(20);".to_string(),
    };
    raw(sql)
}

/// timestamp.c / date.c out-of-range + field-validation error identity —
/// tm2timestamp / date arithmetic overflow and make_* validation. Each is
/// a standalone statement so error identity is compared per statement.
fn gen_dtedge(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("tio:dtedge");
    let sql = match g.rng.below(8) {
        // timestamp + interval overflow past the high endpoint.
        0 => "SELECT ('294276-12-31 23:59:59'::timestamp + '1 day'::interval)::text;".to_string(),
        // timestamp - interval underflow past the low endpoint.
        1 => "SELECT ('4714-11-24 00:00:00 BC'::timestamp - '1 day'::interval)::text;".to_string(),
        // date + int overflow past the high endpoint (date max 5874897 AD).
        2 => "SELECT ('5874897-12-31'::date + 1)::text;".to_string(),
        // infinity stays infinity (non-error identity).
        3 => "SELECT ('infinity'::timestamp + '1 day'::interval)::text, extract(epoch FROM 'infinity'::timestamptz)::text;".to_string(),
        // make_timestamp invalid month: matched error.
        4 => "SELECT make_timestamp(2020, 13, 1, 0, 0, 0.0)::text;".to_string(),
        // make_date invalid day: matched error.
        5 => "SELECT make_date(2020, 2, 30)::text;".to_string(),
        // make_interval large-field overflow.
        6 => "SELECT make_interval(hours => 2147483647, mins => 2147483647)::text;".to_string(),
        // valid boundary round-trips (non-error identity anchors).
        _ => "SELECT ('294276-12-31 23:59:59'::timestamp)::text, ('4714-11-24 00:00:00 BC'::timestamp)::text, ('5874897-12-31'::date)::text;".to_string(),
    };
    raw(sql)
}
