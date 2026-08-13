//! Scalar-type drain module (Track-B): the niche fixed-width scalar types
//! and their system functions — money (cash.c), uuid (uuid.c), pg_lsn
//! (pg_lsn.c), tid (tid.c), xid8 (xid.c) and macaddr8 (mac8.c). These
//! types carry small, self-contained I/O + comparison + arithmetic
//! surfaces that the AST-driven modules never reach (they are not part of
//! the expr/agg column vocabulary), so the hollow lines survive until a
//! literal-driven drain like this one exercises them.
//!
//! Comparison law: every probe compares by result identity — ::text
//! byte-exact and totally ordered — plus error identity (SQLSTATE via
//! diff::classify). All of these surfaces are DETERMINISTIC given fixed
//! literal inputs, so any divergence is a real bug, never noise. The
//! non-deterministic generators on these types (gen_random_uuid,
//! txid_current, pg_current_wal_lsn/pg_current_xact_id) are deliberately
//! NEVER placed on the value-compare surface: only fixed literals feed the
//! probes here.
//!
//! money is locale-sensitive (cash_in/cash_out honour lc_monetary), so
//! every money group pins `SET LOCAL lc_monetary = 'C'` inside a
//! self-contained transaction — the pin auto-reverts at COMMIT, keeping
//! the module stateless (earm discipline). xid comparison is
//! wraparound-relative (age-based) and therefore non-absolute, so the xid
//! surface uses xid8 (absolute 64-bit) for ordering and only plain
//! xideq/xid8eq equality on narrow xid.
//!
//! Stateless: every probe is a self-contained one-statement group (money
//! probes are a BEGIN/SET LOCAL/…/COMMIT bracket) except the aggregate
//! families, which create-and-drop an `fz_scl_*` fixture in-group.

use crate::stmt::{Gen, StmtKind};

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

const SHAPES: &[&str] = &[
    "scl:money",
    "scl:uuid",
    "scl:lsn",
    "scl:tid",
    "scl:xid",
    "scl:mac8",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_scalartypes_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scalartypes");
    match g.weights.pick(g.rng, SHAPES) {
        "scl:money" => gen_money(g),
        "scl:uuid" => gen_uuid(g),
        "scl:lsn" => gen_lsn(g),
        "scl:tid" => gen_tid(g),
        "scl:xid" => gen_xid(g),
        _ => gen_mac8(g),
    }
}

/// Wrap a money probe set in a lc_monetary-pinned transaction so cash_out
/// formatting is deterministic and identical across engines regardless of
/// the session default locale. SET LOCAL reverts at COMMIT (self-contained).
fn money_group(probes: Vec<String>) -> Vec<StmtKind> {
    let mut v = vec![
        raw("BEGIN;".to_string()),
        raw("SET LOCAL lc_monetary = 'C';".to_string()),
    ];
    for p in probes {
        v.push(raw(p));
    }
    v.push(raw("COMMIT;".to_string()));
    v
}

// --------------------------------------------------------------- money ----

/// money operand pool spanning zero, sign, fractional cents, the int64
/// cent boundaries (±92233720368547758.07) and just past them (overflow
/// on input → 22003).
const CASH_OK: &[&str] = &[
    "0",
    "0.00",
    "-0",
    "1",
    "-1",
    "1.23",
    "-1.23",
    "0.005",
    "0.004",
    "1234.56",
    "-1234.56",
    "1000000",
    "0.01",
    "-0.01",
    "12.1",
    "12.19",
    "92233720368547758.07",
    "-92233720368547758.08",
];

/// cash_in strings with grouping/currency decoration (C locale: '$', ','
/// group, '.' decimal) and the parenthesised-negative form.
const CASH_STR: &[&str] = &[
    "$0.00",
    "$1,234.56",
    "-$1,234.56",
    "($1,234.56)",
    "$.5",
    "$1234",
    "1234.56",
    "  $12.30  ",
    "$-5",
    "$1,000,000.00",
    "($0.01)",
];

/// cash_in strings that must fail parsing (22P02) or overflow (22003).
const CASH_ERR: &[&str] = &[
    "",
    "   ",
    "abc",
    "$",
    "$$1",
    "1.2.3",
    "$1,23,4.56",
    "1e5",
    "99999999999999999999",
    "$92233720368547758.08",
];

fn gen_money(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scl:money");
    let shape = g.weights.pick(
        g.rng,
        &[
            "scl:money:io",
            "scl:money:arith",
            "scl:money:cmp",
            "scl:money:agg",
            "scl:money:cast",
            "scl:money:words",
        ],
    );
    g.fire(shape);
    match shape {
        "scl:money:io" => {
            let a = pick_str(g, CASH_OK);
            let s = pick_str(g, CASH_STR);
            let e = pick_str(g, CASH_ERR);
            money_group(vec![
                format!("SELECT {a}::money::text;"),
                format!("SELECT '{s}'::money::text;"),
                format!("SELECT '{e}'::money::text;"), // 22P02 / 22003 (matched)
            ])
        }
        "scl:money:arith" => {
            let a = pick_str(g, CASH_OK);
            let b = pick_str(g, CASH_OK);
            let i = pick_str(g, &["0", "1", "-1", "2", "7", "-13", "100", "-3"]);
            let f = pick_str(g, &["0.5", "-0.5", "2.0", "0.0", "3.25", "-4.75"]);
            // money±money, money*int/float, money/int/float→money,
            // money/money→float8 (cash_div_cash), plus the overflow and
            // divide-by-zero arms (22003 / 22012, matched).
            money_group(vec![
                format!("SELECT ({a}::money + {b}::money)::text, ({a}::money - {b}::money)::text;"),
                format!(
                    "SELECT ({a}::money * {i}::int8)::text, ({i}::int8 * {a}::money)::text, \
                     ({a}::money * {f}::float8)::text, ({f}::float8 * {a}::money)::text;"
                ),
                format!(
                    "SELECT ({a}::money / {b}::money)::text;" // cash_div_cash → float8
                ),
                format!(
                    "SELECT ({a}::money / NULLIF({i}::int8, 0))::text, \
                     ({a}::money / NULLIF({f}::float8, 0))::text;"
                ),
                "SELECT ('92233720368547758.07'::money * 2)::text;".to_string(), // 22003
                format!("SELECT ({a}::money / 0::int4)::text;"),                 // 22012
            ])
        }
        "scl:money:cmp" => {
            let a = pick_str(g, CASH_OK);
            let b = pick_str(g, CASH_OK);
            money_group(vec![format!(
                "SELECT {a}::money < {b}::money, {a}::money <= {b}::money, \
                 {a}::money = {b}::money, {a}::money >= {b}::money, \
                 {a}::money > {b}::money, {a}::money <> {b}::money, \
                 cash_cmp({a}::money, {b}::money);"
            )])
        }
        "scl:money:agg" => {
            let t = "fz_scl_m";
            let rows = 200 + g.rng.below(200);
            let stride = 1 + g.rng.below(9);
            money_group(vec![
                format!(
                    "CREATE TEMP TABLE {t} (i int4 PRIMARY KEY, v money);"
                ),
                format!(
                    "INSERT INTO {t} SELECT i, ((((i * {stride}) % 20000) - 10000)::numeric / 100)::money \
                     FROM generate_series(1, {rows}) i;"
                ),
                format!(
                    "SELECT (sum(v))::text, (avg(v))::text, (min(v))::text, (max(v))::text, count(v) \
                     FROM {t};"
                ),
                format!(
                    "SELECT i % 4 AS gk, (sum(v))::text, (max(v))::text \
                     FROM {t} GROUP BY 1 ORDER BY 1;"
                ),
                format!("SELECT v::text FROM {t} ORDER BY v, i LIMIT 20;"),
                format!("DROP TABLE {t};"),
            ])
        }
        "scl:money:cast" => {
            let a = pick_str(g, CASH_OK);
            let n = pick_str(
                g,
                &[
                    "0",
                    "1.23",
                    "-1.23",
                    "1234.5678",
                    "0.005",
                    "-0.004",
                    "92233720368547758.07",
                    "0.999",
                ],
            );
            // numeric/int→money and money→numeric round trips; int2/int4/int8
            // → money direct casts.
            money_group(vec![
                format!("SELECT {n}::numeric::money::text, {a}::money::numeric::text;"),
                format!(
                    "SELECT 100::int2::money::text, 100000::int4::money::text, \
                     100000000::int8::money::text;"
                ),
                "SELECT (1e19::numeric)::money::text;".to_string(), // 22003 overflow (matched)
            ])
        }
        _ => {
            let a = pick_str(
                g,
                &[
                    "0",
                    "0.01",
                    "1",
                    "1.23",
                    "-1.23",
                    "21.42",
                    "100",
                    "1234.56",
                    "-0.99",
                    "1000000.00",
                    "23.00",
                ],
            );
            money_group(vec![format!("SELECT cash_words({a}::money);")])
        }
    }
}

// ---------------------------------------------------------------- uuid ----

/// Well-formed uuid literals across the accepted input forms: canonical
/// hyphenated, upper/mixed case, brace-wrapped, and the hyphen-free form —
/// all canonicalise to the same lowercase hyphenated ::text.
const UUID_OK: &[&str] = &[
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
    "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
    "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
    "a0eebc999c0b4ef8bb6d6bb9bd380a11",
    "00000000-0000-0000-0000-000000000000",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "{A0EEBC99-9c0b-4Ef8-Bb6d-6bb9BD380a11}",
];

/// uuid_in inputs that must fail (22P02): wrong length, bad hex, misplaced
/// braces, empty.
const UUID_ERR: &[&str] = &[
    "",
    "not-a-uuid",
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",   // too short
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111", // too long
    "g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",  // non-hex
    "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", // unbalanced brace
    "a0eebc99+9c0b+4ef8+bb6d+6bb9bd380a11",  // bad separator
];

/// uuids with known version nibbles for uuid_extract_version /
/// uuid_extract_timestamp (PG18): v1 and v7 carry a timestamp, v4 does not
/// (extract_timestamp → NULL). All fixed literals → deterministic.
const UUID_VER: &[&str] = &[
    "a981bb3e-9c1e-11ee-8c90-0242ac120002", // v1 (has timestamp)
    "018f7f8e-1c7a-7c3e-8b2a-1e2d3c4d5e6f", // v7 (has timestamp)
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", // v4 (timestamp NULL)
    "00000000-0000-8000-8000-000000000000", // v8
    "00000000-0000-0000-0000-000000000000", // nil (v0)
];

fn gen_uuid(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scl:uuid");
    let shape = g.weights.pick(
        g.rng,
        &[
            "scl:uuid:io",
            "scl:uuid:ioerr",
            "scl:uuid:cmp",
            "scl:uuid:extract",
        ],
    );
    g.fire(shape);
    match shape {
        "scl:uuid:io" => {
            let a = pick_str(g, UUID_OK);
            vec![raw(format!("SELECT '{a}'::uuid::text;"))]
        }
        "scl:uuid:ioerr" => {
            let e = pick_str(g, UUID_ERR);
            vec![raw(format!("SELECT '{e}'::uuid::text;"))] // 22P02 (matched)
        }
        "scl:uuid:cmp" => {
            let a = pick_str(g, UUID_OK);
            let b = pick_str(g, UUID_OK);
            vec![raw(format!(
                "SELECT '{a}'::uuid < '{b}'::uuid, '{a}'::uuid <= '{b}'::uuid, \
                 '{a}'::uuid = '{b}'::uuid, '{a}'::uuid >= '{b}'::uuid, \
                 '{a}'::uuid > '{b}'::uuid, '{a}'::uuid <> '{b}'::uuid, \
                 uuid_cmp('{a}'::uuid, '{b}'::uuid), \
                 uuid_hash('{a}'::uuid) = uuid_hash('{a}'::uuid);"
            ))]
        }
        _ => {
            let a = pick_str(g, UUID_VER);
            // extract_version is total; extract_timestamp is NULL for
            // versions without an embedded time. Both fixed → deterministic.
            vec![raw(format!(
                "SELECT uuid_extract_version('{a}'::uuid), \
                 uuid_extract_timestamp('{a}'::uuid)::text;"
            ))]
        }
    }
}

// -------------------------------------------------------------- pg_lsn ----

/// pg_lsn literals in X/Y form spanning zero, small, the 32-bit segment
/// boundary and the max value.
const LSN_OK: &[&str] = &[
    "0/0",
    "0/1",
    "0/FFFFFFFF",
    "1/0",
    "16/B374D848",
    "AB/CDEF1234",
    "FFFFFFFF/FFFFFFFE",
    "FFFFFFFF/FFFFFFFF",
    "0/16B374D8",
];

/// pg_lsn_in strings that must fail (22P02).
const LSN_ERR: &[&str] = &["", "16", "16/", "/B374", "16/GGGG", "16/B374D848/1", "xyz"];

fn gen_lsn(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scl:lsn");
    let shape = g
        .weights
        .pick(g.rng, &["scl:lsn:io", "scl:lsn:arith", "scl:lsn:cmp"]);
    g.fire(shape);
    match shape {
        "scl:lsn:io" => {
            let a = pick_str(g, LSN_OK);
            let e = pick_str(g, LSN_ERR);
            vec![
                raw(format!("SELECT '{a}'::pg_lsn::text;")),
                raw(format!("SELECT '{e}'::pg_lsn::text;")), // 22P02 (matched)
            ]
        }
        "scl:lsn:arith" => {
            let a = pick_str(g, LSN_OK);
            let b = pick_str(g, LSN_OK);
            let n = pick_str(
                g,
                &["0", "1", "16", "4294967296", "-3", "1.5", "1000000000000"],
            );
            // lsn±numeric→lsn, lsn-lsn→numeric, plus the out-of-range arm
            // (past FFFFFFFF/FFFFFFFF → 22003, matched).
            vec![
                raw(format!(
                    "SELECT ('{a}'::pg_lsn + {n}::numeric)::text, \
                     ('{a}'::pg_lsn - {n}::numeric)::text;"
                )),
                raw(format!("SELECT ('{a}'::pg_lsn - '{b}'::pg_lsn)::text;")),
                raw("SELECT ('FFFFFFFF/FFFFFFFF'::pg_lsn + 1::numeric)::text;".to_string()), // 22003
            ]
        }
        _ => {
            let a = pick_str(g, LSN_OK);
            let b = pick_str(g, LSN_OK);
            vec![raw(format!(
                "SELECT '{a}'::pg_lsn < '{b}'::pg_lsn, '{a}'::pg_lsn <= '{b}'::pg_lsn, \
                 '{a}'::pg_lsn = '{b}'::pg_lsn, '{a}'::pg_lsn >= '{b}'::pg_lsn, \
                 '{a}'::pg_lsn > '{b}'::pg_lsn, '{a}'::pg_lsn <> '{b}'::pg_lsn, \
                 greatest('{a}'::pg_lsn, '{b}'::pg_lsn)::text, \
                 least('{a}'::pg_lsn, '{b}'::pg_lsn)::text;"
            ))]
        }
    }
}

// ----------------------------------------------------------------- tid ----

/// tid literals as (block, offset); zero, small, and the 32-bit block /
/// 16-bit offset maxima.
const TID_OK: &[&str] = &[
    "(0,0)",
    "(0,1)",
    "(1,1)",
    "(42,7)",
    "(4294967295,65535)",
    "(0,65535)",
    "(4294967295,1)",
];

/// tid_in strings that must fail (22P02).
const TID_ERR: &[&str] = &[
    "",
    "(0)",
    "(0,)",
    "0,0",
    "()",
    "(0,0,0)",
    "(-1,0)",
    "(0,65536)",
    "(a,b)",
];

fn gen_tid(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scl:tid");
    let shape = g.weights.pick(g.rng, &["scl:tid:io", "scl:tid:cmp"]);
    g.fire(shape);
    match shape {
        "scl:tid:io" => {
            let a = pick_str(g, TID_OK);
            let e = pick_str(g, TID_ERR);
            vec![
                raw(format!("SELECT '{a}'::tid::text;")),
                raw(format!("SELECT '{e}'::tid::text;")), // 22P02 (matched)
            ]
        }
        _ => {
            let a = pick_str(g, TID_OK);
            let b = pick_str(g, TID_OK);
            vec![raw(format!(
                "SELECT '{a}'::tid < '{b}'::tid, '{a}'::tid <= '{b}'::tid, \
                 '{a}'::tid = '{b}'::tid, '{a}'::tid >= '{b}'::tid, \
                 '{a}'::tid > '{b}'::tid, '{a}'::tid <> '{b}'::tid;"
            ))]
        }
    }
}

// ------------------------------------------------------------ xid / xid8 ----

/// xid8 literals (absolute 64-bit): zero/first-normal, small, and near the
/// unsigned-64 top. xid8 ordering is absolute (unlike wraparound-relative
/// narrow xid), so it is safe to totally-order.
const XID8_OK: &[&str] = &[
    "0",
    "1",
    "2",
    "3",
    "100",
    "12345",
    "4294967295",
    "4294967296",
    "9223372036854775807",
    "18446744073709551615",
];

fn gen_xid(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scl:xid");
    let shape = g.weights.pick(g.rng, &["scl:xid:cmp", "scl:xid:conv"]);
    g.fire(shape);
    match shape {
        "scl:xid:cmp" => {
            let a = pick_str(g, XID8_OK);
            let b = pick_str(g, XID8_OK);
            // xid8 full ordering (absolute) + xid8cmp; plain xideq on narrow
            // xid (equality is exact, not age-relative).
            vec![raw(format!(
                "SELECT '{a}'::xid8 < '{b}'::xid8, '{a}'::xid8 <= '{b}'::xid8, \
                 '{a}'::xid8 = '{b}'::xid8, '{a}'::xid8 >= '{b}'::xid8, \
                 '{a}'::xid8 > '{b}'::xid8, '{a}'::xid8 <> '{b}'::xid8, \
                 xid8cmp('{a}'::xid8, '{b}'::xid8), \
                 '{a}'::xid = '{a}'::xid, '{a}'::xid <> '{b}'::xid;"
            ))]
        }
        _ => {
            let a = pick_str(g, XID8_OK);
            // xid8 → xid narrowing cast (mod 2^32) and text round-trips.
            vec![raw(format!(
                "SELECT '{a}'::xid8::text, '{a}'::xid8::xid::text, \
                 '{a}'::xid8::xid = '{a}'::xid8::xid;"
            ))]
        }
    }
}

// ------------------------------------------------------------- macaddr8 ----

/// macaddr8 literals: the native 8-byte (EUI-64) forms across separator
/// styles, plus 6-byte (EUI-48) inputs that widen to EUI-64 via the
/// ff:fe insertion.
const MAC8_OK: &[&str] = &[
    "00:00:00:00:00:00:00:00",
    "08:00:2b:01:02:03:04:05",
    "08-00-2b-01-02-03-04-05",
    "0800.2b01.0203.0405",
    "FF:FF:FF:FF:FF:FF:FF:FF",
    "08:00:2b:01:02:03", // EUI-48 → widened
    "08-00-2b-01-02-03",
    "123456789abc", // bare 6-byte
];

/// macaddr8_in strings that must fail (22P02).
const MAC8_ERR: &[&str] = &[
    "",
    "08:00:2b",
    "zz:00:2b:01:02:03:04:05",
    "08:00:2b:01:02:03:04",
    "gg",
];

fn gen_mac8(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("scl:mac8");
    let shape = g
        .weights
        .pick(g.rng, &["scl:mac8:io", "scl:mac8:ops", "scl:mac8:cmp"]);
    g.fire(shape);
    match shape {
        "scl:mac8:io" => {
            let a = pick_str(g, MAC8_OK);
            let e = pick_str(g, MAC8_ERR);
            vec![
                raw(format!("SELECT '{a}'::macaddr8::text;")),
                raw(format!("SELECT '{e}'::macaddr8::text;")), // 22P02 (matched)
            ]
        }
        "scl:mac8:ops" => {
            let a = pick_str(g, MAC8_OK);
            // bitwise not/and/or, trunc (zero trailing 5 bytes),
            // macaddr8_set7bit (flip the U/L bit), and the macaddr <->
            // macaddr8 casts (macaddr8→macaddr only when the middle bytes
            // are ff:fe, else 22000 — matched).
            vec![raw(format!(
                "SELECT (~ '{a}'::macaddr8)::text, \
                 ('{a}'::macaddr8 & 'ff:ff:ff:00:00:00:00:00'::macaddr8)::text, \
                 ('{a}'::macaddr8 | '00:00:00:00:00:00:00:01'::macaddr8)::text, \
                 trunc('{a}'::macaddr8)::text, \
                 macaddr8_set7bit('{a}'::macaddr8)::text, \
                 '{a}'::macaddr8::macaddr::text;"
            ))]
        }
        _ => {
            let a = pick_str(g, MAC8_OK);
            let b = pick_str(g, MAC8_OK);
            vec![raw(format!(
                "SELECT '{a}'::macaddr8 < '{b}'::macaddr8, '{a}'::macaddr8 <= '{b}'::macaddr8, \
                 '{a}'::macaddr8 = '{b}'::macaddr8, '{a}'::macaddr8 >= '{b}'::macaddr8, \
                 '{a}'::macaddr8 > '{b}'::macaddr8, '{a}'::macaddr8 <> '{b}'::macaddr8, \
                 '00:00:2b:01:02:03'::macaddr::macaddr8::text;"
            ))]
        }
    }
}
