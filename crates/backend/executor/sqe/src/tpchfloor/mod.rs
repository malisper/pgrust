//! P4-2a TPC-H floor study (production-plan.md R7): handwritten,
//! hand-optimal kernels against pgrc2 banks — the perf bar the P4-2
//! stencil families must meet. Originally Q3/Q9/Q18; extended
//! (sqe-tpch-floor2, tpch-floor-2.md) to the served-set explosion's
//! shapes: Q1/Q12/Q14 (aggregate-heavy) and Q5/Q10 (joins). Bench-grain study code (feature
//! `tpchfloor`), NEVER engine code: nothing here is consulted by the
//! engine, the planner, or the stencils.
//!
//! Decimal currency (decided HERE, recorded in tpch-floor-study.md §2):
//! every TPC-H DECIMAL(15,2) column is ingested as a fixed-scale-2 i64/i32
//! WORD lane (cents) — the A6b `PackedNumeric{scale:2}` currency — because
//! `pgrc2_write::ingest::RawDatum` has no numeric-varlena ingest image
//! path (Null | Word | Bytes only). This is the study's type-gap finding,
//! not a shortcut: the packed-numeric lane is exactly the currency the
//! stencils will fold, so the floor prices the right arithmetic.
//!
//! Date currency: DATE as i32 days since 1970-01-01 (PG stores days since
//! 2000-01-01; the epoch offset is a render-side constant, irrelevant to
//! comparisons — the floor and the oracle share ONE render seam below).

pub mod floors;
pub mod gen;
pub mod ingest;
pub mod oracle;

use crate::bank::{Bank, ColMeta, OpenOpts};
use crate::typmeta::{oids, TypMeta, COLLATION_C};

// ---------------------------------------------------------------------------
// The ONE render seam (the floor's render::esc idiom): floor, oracle and
// the tiny-SF nested-loop reference all format answer cells through these.
// ---------------------------------------------------------------------------

/// Scale-2 cents -> "d.cc" (sign-aware).
pub fn fmt_money2(c: i64) -> String {
    let (s, a) = if c < 0 { ("-", -c) } else { ("", c) };
    format!("{s}{}.{:02}", a / 100, a % 100)
}

/// Scale-4 (cents * hundredths) -> "d.cccc".
pub fn fmt_money4(c: i64) -> String {
    let (s, a) = if c < 0 { ("-", -c) } else { ("", c) };
    format!("{s}{}.{:04}", a / 10_000, a % 10_000)
}

/// Scale-6 (cents * hundredths * hundredths — Q1's sum_charge) ->
/// "d.cccccc". i64 is enough at SF1 (~3.4e17 max per group); the SF100
/// study must widen this lane to i128 (recorded headroom, not taken).
pub fn fmt_money6(c: i64) -> String {
    fmt_scaled6(c as i128)
}

fn fmt_scaled6(v: i128) -> String {
    let (s, a) = if v < 0 { ("-", -v) } else { ("", v) };
    format!("{s}{}.{:06}", a / 1_000_000, a % 1_000_000)
}

/// Round-half-up signed division (den > 0) — the ONE division law for
/// every averaged/ratio answer cell (floors and oracles share it, so
/// identity never hinges on rounding).
fn div_half_up(num: i128, den: i128) -> i128 {
    if num >= 0 {
        (num + den / 2) / den
    } else {
        -((-num + den / 2) / den)
    }
}

/// avg over a scale-`scale` integer sum, rendered at scale 6 (Q1's
/// avg_qty / avg_price / avg_disc).
pub fn fmt_avg6(sum: i64, count: i64, scale: u32) -> String {
    fmt_scaled6(div_half_up(sum as i128 * 10i128.pow(6 - scale), count as i128))
}

/// 100 * num / den rendered at scale 6 (Q14's promo_revenue percentage;
/// num/den share one scale so it cancels).
pub fn fmt_pct6(num: i64, den: i64) -> String {
    fmt_scaled6(div_half_up(num as i128 * 100_000_000, den as i128))
}

/// Days since 1970-01-01 -> "YYYY-MM-DD" (civil-from-days).
pub fn fmt_date(days: i32) -> String {
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// "YYYY-MM-DD" literal -> days since 1970-01-01 (days-from-civil).
pub fn date_days(y: i64, m: i64, d: i64) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i32
}

/// Year of a days-since-epoch date (Q9's `extract(year from o_orderdate)`)
/// — arithmetic civil-from-days, no formatting on the hot path.
pub fn date_year(days: i32) -> i32 {
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }) as i32
}

// ---------------------------------------------------------------------------
// Read-side schemas (the catalog stand-in — type OIDs decided HERE, the
// rig's hits_schema idiom). Attnos are 1-based and match ingest order.
// ---------------------------------------------------------------------------

fn t() -> TypMeta {
    TypMeta::varlena(oids::TEXT, COLLATION_C)
}

pub fn customer_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "c_custkey", TypMeta::INT4),
        ColMeta::new(2, "c_name", t()),
        ColMeta::new(3, "c_address", t()),
        ColMeta::new(4, "c_nationkey", TypMeta::INT4),
        ColMeta::new(5, "c_phone", t()),
        ColMeta::new(6, "c_acctbal", TypMeta::INT8), // cents (scale 2)
        ColMeta::new(7, "c_mktsegment", t()),
        ColMeta::new(8, "c_comment", t()),
    ]
}

pub fn orders_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "o_orderkey", TypMeta::INT8),
        ColMeta::new(2, "o_custkey", TypMeta::INT4),
        ColMeta::new(3, "o_orderstatus", t()),
        ColMeta::new(4, "o_totalprice", TypMeta::INT8), // cents
        ColMeta::new(5, "o_orderdate", TypMeta::DATE),
        ColMeta::new(6, "o_orderpriority", t()),
        ColMeta::new(7, "o_clerk", t()),
        ColMeta::new(8, "o_shippriority", TypMeta::INT4),
        ColMeta::new(9, "o_comment", t()),
    ]
}

pub fn lineitem_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "l_orderkey", TypMeta::INT8),
        ColMeta::new(2, "l_partkey", TypMeta::INT4),
        ColMeta::new(3, "l_suppkey", TypMeta::INT4),
        ColMeta::new(4, "l_linenumber", TypMeta::INT4),
        ColMeta::new(5, "l_quantity", TypMeta::INT4), // cents (integral qty * 100)
        ColMeta::new(6, "l_extendedprice", TypMeta::INT8), // cents
        ColMeta::new(7, "l_discount", TypMeta::INT4), // hundredths (0..10)
        ColMeta::new(8, "l_tax", TypMeta::INT4),      // hundredths (0..8)
        ColMeta::new(9, "l_returnflag", t()),
        ColMeta::new(10, "l_linestatus", t()),
        ColMeta::new(11, "l_shipdate", TypMeta::DATE),
        ColMeta::new(12, "l_commitdate", TypMeta::DATE),
        ColMeta::new(13, "l_receiptdate", TypMeta::DATE),
        ColMeta::new(14, "l_shipinstruct", t()),
        ColMeta::new(15, "l_shipmode", t()),
        ColMeta::new(16, "l_comment", t()),
    ]
}

pub fn part_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "p_partkey", TypMeta::INT4),
        ColMeta::new(2, "p_name", t()),
        ColMeta::new(3, "p_mfgr", t()),
        ColMeta::new(4, "p_brand", t()),
        ColMeta::new(5, "p_type", t()),
        ColMeta::new(6, "p_size", TypMeta::INT4),
        ColMeta::new(7, "p_container", t()),
        ColMeta::new(8, "p_retailprice", TypMeta::INT4), // cents
        ColMeta::new(9, "p_comment", t()),
    ]
}

pub fn partsupp_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "ps_partkey", TypMeta::INT4),
        ColMeta::new(2, "ps_suppkey", TypMeta::INT4),
        ColMeta::new(3, "ps_availqty", TypMeta::INT4),
        ColMeta::new(4, "ps_supplycost", TypMeta::INT4), // cents
        ColMeta::new(5, "ps_comment", t()),
    ]
}

pub fn supplier_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "s_suppkey", TypMeta::INT4),
        ColMeta::new(2, "s_name", t()),
        ColMeta::new(3, "s_address", t()),
        ColMeta::new(4, "s_nationkey", TypMeta::INT4),
        ColMeta::new(5, "s_phone", t()),
        ColMeta::new(6, "s_acctbal", TypMeta::INT8), // cents
        ColMeta::new(7, "s_comment", t()),
    ]
}

pub fn nation_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "n_nationkey", TypMeta::INT4),
        ColMeta::new(2, "n_name", t()),
        ColMeta::new(3, "n_regionkey", TypMeta::INT4),
        ColMeta::new(4, "n_comment", t()),
    ]
}

pub fn region_schema() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "r_regionkey", TypMeta::INT4),
        ColMeta::new(2, "r_name", t()),
        ColMeta::new(3, "r_comment", t()),
    ]
}

pub const TABLES: [&str; 8] =
    ["customer", "orders", "lineitem", "part", "partsupp", "supplier", "nation", "region"];

pub fn schema_of(table: &str) -> Vec<ColMeta> {
    match table {
        "customer" => customer_schema(),
        "orders" => orders_schema(),
        "lineitem" => lineitem_schema(),
        "part" => part_schema(),
        "partsupp" => partsupp_schema(),
        "supplier" => supplier_schema(),
        "nation" => nation_schema(),
        "region" => region_schema(),
        other => panic!("unknown table {other}"),
    }
}

/// Open one table's bank under `<root>/<table>`.
pub fn open_table(root: &str, table: &str) -> Bank {
    Bank::open(
        &format!("{root}/{table}"),
        schema_of(table),
        &OpenOpts { bankstats: false, threads: 1 },
    )
}

// Query constants of record (TPC-H validation defaults).
pub const Q3_SEGMENT: &[u8] = b"BUILDING";
pub const Q5_REGION: &[u8] = b"ASIA";
pub const Q9_COLOR: &[u8] = b"green";
pub const Q12_MODE_A: &[u8] = b"MAIL";
pub const Q12_MODE_B: &[u8] = b"SHIP";
pub const Q14_PROMO: &[u8] = b"PROMO";
pub const Q18_QTY_C: i64 = 300 * 100; // sum(l_quantity) > 300, scale-2
pub fn q1_date() -> i32 {
    date_days(1998, 9, 2) // date '1998-12-01' - interval '90' day
}
pub fn q3_date() -> i32 {
    date_days(1995, 3, 15)
}
/// [lo, hi) o_orderdate window: '1994-01-01' + 1 year.
pub fn q5_dates() -> (i32, i32) {
    (date_days(1994, 1, 1), date_days(1995, 1, 1))
}
/// [lo, hi) o_orderdate window: '1993-10-01' + 3 months.
pub fn q10_dates() -> (i32, i32) {
    (date_days(1993, 10, 1), date_days(1994, 1, 1))
}
/// [lo, hi) l_receiptdate window: '1994-01-01' + 1 year.
pub fn q12_dates() -> (i32, i32) {
    (date_days(1994, 1, 1), date_days(1995, 1, 1))
}
/// [lo, hi) l_shipdate window: '1995-09-01' + 1 month.
pub fn q14_dates() -> (i32, i32) {
    (date_days(1995, 9, 1), date_days(1995, 10, 1))
}
