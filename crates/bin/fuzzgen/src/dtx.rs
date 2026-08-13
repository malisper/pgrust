//! Datetime-extra module (`dtx`): the arithmetic/timezone datetime surface
//! that the A2 cross-type matrix (`dtm`) does NOT reach. `dtm` covers +/-
//! arithmetic, comparisons, overlaps, date_trunc/extract, date_bin, age,
//! justify_*, make_*, AT TIME ZONE (operator syntax), to_char and casts.
//! This module complements it — deliberately non-overlapping — with:
//!
//!   gseries   generate_series over timestamp/timestamptz with an interval
//!             step, including the 4-arg zone-bucketing form (PG16+); the
//!             set-returning entry points generate_series_timestamp /
//!             generate_series_timestamptz / generate_series_timestamp_at_
//!             zone in timestamp.c that scalar SELECTs cannot reach.
//!             Wrapped `SELECT ... FROM generate_series(...) g ORDER BY 1`
//!             for a total order over a bounded (<~50-row) result.
//!   isfinite  isfinite(date/timestamp/timestamptz/interval) — the
//!             date_finite/timestamp_finite/interval_finite predicates,
//!             exercised over ±infinity and finite inputs.
//!   typmod    field- and precision-typmod adjustment: interval field
//!             specs (YEAR TO MONTH, DAY TO SECOND, HOUR TO MINUTE,
//!             SECOND(p), ...) driving AdjustIntervalForTypmod, plus
//!             timestamp(p)/timestamptz(p)/time(p)/timetz(p) precision
//!             casts driving AdjustTimestampForTypmod / AdjustTimeForTypmod
//!             (fractional-second rounding + range clamp).
//!   tzfunc    timezone(zone, source) FUNCTION form (as opposed to the
//!             AT TIME ZONE operator syntax `dtm` uses) — the distinct C
//!             entry points timestamp_zone/timestamp_izone/timestamptz_
//!             zone/timestamptz_izone/timetz_zone/timetz_izone, over both
//!             text-zone and interval-offset arguments.
//!   epoch     to_timestamp(double precision) — float8_timestamptz, the
//!             epoch->timestamptz path (NOT the to_timestamp(text,text)
//!             formatting path), over epoch edges + overflow error fuel.
//!
//! Determinism: every operand is a fixed literal (NEVER now()/current_*).
//! The session pin runner::DATETIME_GUC_PIN (TimeZone=UTC, DateStyle,
//! IntervalStyle, applied to BOTH differential sides) — established for
//! `dtm` — renders every timestamptz/interval result identically on both
//! engines, and the harness tzdata-parity rule makes the named-zone
//! arguments read the same compiled tzdata on both sides.

use crate::stmt::{Gen, StmtKind};

/// Named zones (tzdata-backed; half-hour offset + half-hour DST included on
/// purpose). Same pool `dtm` pins on — zone-database parity is a harness
/// precondition.
const ZONES: &[&str] = &[
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Australia/Lord_Howe",
];

/// Interval offset arguments for the interval-form timezone()/typmod paths.
const OFFSETS: &[&str] = &[
    "'+05:30'",
    "'-08:00'",
    "'+00:00'",
    "'+14:00'",
    "'-12:00'",
    "'+05:45'",
];

/// Curated (start, end, step) triples for generate_series over timestamp.
/// Each is chosen to emit a small (<~50-row) set: month/century steps that
/// exercise carry/clamp arithmetic, DST-relevant instants, a BC/AD span,
/// microsecond and day steps.
const GS_TS: &[(&str, &str, &str)] = &[
    ("'2000-01-01 00:00'", "'2000-01-02 00:00'", "'2 hours'"),
    ("'1999-12-31 23:00'", "'2000-01-01 01:00:00.5'", "'15 minutes'"),
    ("'2021-03-14 00:00'", "'2021-03-14 06:00'", "'30 minutes'"),
    ("'2024-01-31'", "'2024-06-30'", "'1 month'"),
    ("'2000-01-01'", "'2000-01-08'", "'1 day'"),
    ("'0002-01-01 BC'", "'0002-01-01'", "'1 year'"),
    ("'2000-01-01 00:00:00'", "'2000-01-01 00:00:00.00001'", "'0.000002 seconds'"),
    ("'294276-12-30'", "'294276-12-31'", "'6 hours'"),
];

/// Curated (start, end, step) triples for generate_series over
/// timestamptz. Offsets are explicit; TimeZone is pinned UTC.
const GS_TSTZ: &[(&str, &str, &str)] = &[
    ("'2021-11-06 00:00-04'", "'2021-11-08 00:00-05'", "'6 hours'"),
    ("'2000-01-01 00:00+00'", "'2000-01-02 00:00+00'", "'3 hours'"),
    ("'2019-12-31 18:30+05:30'", "'2020-01-01 18:30+05:30'", "'4 hours'"),
    ("'2000-01-01 00:00+00'", "'2000-04-01 00:00+00'", "'1 month'"),
];

/// generate_series steps that are deliberate error / edge fuel (zero step
/// is an error on both sides; kept rare by the caller).
const GS_STEP_ERR: &[&str] = &["'0'", "'0 seconds'"];

/// Finite + infinite literals per type for isfinite.
const ISF: &[(&str, &str)] = &[
    ("date", "'2000-01-01'"),
    ("date", "'infinity'"),
    ("date", "'-infinity'"),
    ("timestamp", "'2000-01-01 12:00:00'"),
    ("timestamp", "'infinity'"),
    ("timestamp", "'-infinity'"),
    ("timestamptz", "'2000-01-01 12:00:00+00'"),
    ("timestamptz", "'infinity'"),
    ("timestamptz", "'-infinity'"),
    ("interval", "'1 day'"),
    ("interval", "'infinity'"),
    ("interval", "'-infinity'"),
];

/// Interval literals rich in every field so field-typmod truncation is
/// observable (carry-heavy month/day/us, fractional seconds, mixed sign,
/// range extremes).
const IV_TYPMOD: &[&str] = &[
    "'1 year 2 mons 3 days 04:05:06.789'",
    "'-1 year -2 mons 3 days -04:05:06'",
    "'13 mons'",
    "'400 days 25:61:61.5'",
    "'1.6 years'",
    "'0.9999995 seconds'",
    "'178956970 years 7 mons'",
    "'2147483647 days 23:59:59.999999'",
    "'00:00:00.0000005'",
    "'99 hours 99 minutes 99.9999994 seconds'",
];

/// interval field-typmod specifications (AdjustIntervalForTypmod range +
/// precision bits). Precision-bearing specs are appended in the caller.
const IV_FIELDS: &[&str] = &[
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "YEAR TO MONTH",
    "DAY TO HOUR",
    "DAY TO MINUTE",
    "DAY TO SECOND",
    "HOUR TO MINUTE",
    "HOUR TO SECOND",
    "MINUTE TO SECOND",
];

/// Timestamp/time literals with sub-second tails so precision typmods
/// round observably (banker's-vs-half-up, carry into the next second, the
/// 0.9999995 boundary).
const TS_TYPMOD: &[&str] = &[
    "'2000-01-01 23:59:59.9999995'",
    "'2000-01-01 00:00:00.4999994'",
    "'1999-12-31 23:59:59.5'",
    "'2000-06-15 12:30:30.123456'",
];
const TIME_TYPMOD: &[&str] = &[
    "'23:59:59.9999995'",
    "'00:00:00.4999994'",
    "'12:30:30.123456'",
    "'24:00:00'",
];

/// timezone()-function source literals per branch.
const TZF_TS: &[&str] = &[
    "'2021-03-14 02:30:00'",
    "'2021-11-07 01:30:00'",
    "'2000-01-01 00:00:00'",
    "'2020-10-04 02:15:00'",
];
const TZF_TSTZ: &[&str] = &[
    "'2021-03-14 06:59:59.999999+00'",
    "'2000-01-01 00:00:00+00'",
    "'2019-12-31 18:30:00+05:30'",
    "'2000-06-01 12:00:00-08'",
];
const TZF_TIMETZ: &[&str] = &[
    "'12:00:00+05:30'",
    "'00:00:00+00'",
    "'23:59:59.999999-08'",
    "'12:00:00+05:45'",
];

/// Epoch (double precision) inputs for to_timestamp: edges + overflow fuel.
const EPOCHS: &[&str] = &[
    "0",
    "-1",
    "946684800",
    "1000000000.000001",
    "-210866803200",
    "9224318016000",
    "1e20",
    "-1e20",
    "'NaN'::float8",
    "'Infinity'::float8",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_dtx_module(g: &mut Gen) -> Vec<StmtKind> {
    let pick = g.weights.pick(
        g.rng,
        &[
            "dtx:gseries",
            "dtx:isfinite",
            "dtx:typmod",
            "dtx:tzfunc",
            "dtx:epoch",
        ],
    );
    // gseries is set-returning -> its own FROM/ORDER BY statement, so it
    // short-circuits the scalar-SELECT wrapper below.
    if pick == "dtx:gseries" {
        return vec![StmtKind::Raw(gen_gseries(g))];
    }
    let expr = match pick {
        "dtx:isfinite" => gen_isfinite(g),
        "dtx:typmod" => gen_typmod(g),
        "dtx:tzfunc" => gen_tzfunc(g),
        _ => gen_epoch(g),
    };
    vec![StmtKind::Raw(format!("SELECT {};", expr))]
}

fn gen_gseries(g: &mut Gen) -> String {
    g.fire("dtx:gseries");
    // 1/12 of the time inject a zero-step error case (identical two-sided
    // "step size cannot equal zero" rejection is the assertion).
    let err_step = g.rng.chance(1, 12);
    if g.rng.chance(1, 2) {
        // timestamp, 3-arg
        let (a, b, step) = *g.rng.pick(GS_TS);
        let step = if err_step { *g.rng.pick(GS_STEP_ERR) } else { step };
        format!(
            "SELECT g FROM generate_series(TIMESTAMP {}, TIMESTAMP {}, INTERVAL {}) AS g ORDER BY 1;",
            a, b, step
        )
    } else {
        let (a, b, step) = *g.rng.pick(GS_TSTZ);
        let step = if err_step { *g.rng.pick(GS_STEP_ERR) } else { step };
        if g.rng.chance(1, 2) {
            // timestamptz, 4-arg zone-bucketing form (PG16+)
            let z = g.rng.pick(ZONES);
            format!(
                "SELECT g FROM generate_series(TIMESTAMPTZ {}, TIMESTAMPTZ {}, INTERVAL {}, '{}') AS g ORDER BY 1;",
                a, b, step, z
            )
        } else {
            // timestamptz, 3-arg
            format!(
                "SELECT g FROM generate_series(TIMESTAMPTZ {}, TIMESTAMPTZ {}, INTERVAL {}) AS g ORDER BY 1;",
                a, b, step
            )
        }
    }
}

fn gen_isfinite(g: &mut Gen) -> String {
    g.fire("dtx:isfinite");
    let (ty, lit) = *g.rng.pick(ISF);
    format!("isfinite({} {})", ty.to_uppercase(), lit)
}

fn gen_typmod(g: &mut Gen) -> String {
    g.fire("dtx:typmod");
    match g.rng.below(4) {
        // interval field spec (SECOND-bearing specs also take a precision)
        0 => {
            let lit = g.rng.pick(IV_TYPMOD);
            let field = *g.rng.pick(IV_FIELDS);
            // append a fractional-seconds precision to SECOND-terminated specs
            let spec = if field.ends_with("SECOND") && g.rng.chance(1, 2) {
                format!("{}({})", field, g.rng.below(7))
            } else {
                field.to_string()
            };
            format!("(INTERVAL {} {})::text", lit, spec)
        }
        // interval precision-only typmod via cast: interval(p)
        1 => {
            let lit = g.rng.pick(IV_TYPMOD);
            let p = g.rng.below(7);
            format!("(INTERVAL {})::interval({})::text", lit, p)
        }
        // timestamp / timestamptz precision cast
        2 => {
            let lit = g.rng.pick(TS_TYPMOD);
            let p = g.rng.below(7);
            if g.rng.chance(1, 2) {
                format!("(TIMESTAMP {})::timestamp({})::text", lit, p)
            } else {
                // give the tstz form an explicit offset so it is a legal tstz
                format!("(TIMESTAMPTZ {}+00')::timestamptz({})::text", lit.trim_end_matches('\''), p)
            }
        }
        // time / timetz precision cast
        _ => {
            let lit = g.rng.pick(TIME_TYPMOD);
            let p = g.rng.below(7);
            if g.rng.chance(1, 2) {
                format!("(TIME {})::time({})::text", lit, p)
            } else {
                format!("(TIMETZ {}+00')::timetz({})::text", lit.trim_end_matches('\''), p)
            }
        }
    }
}

fn gen_tzfunc(g: &mut Gen) -> String {
    g.fire("dtx:tzfunc");
    // zone argument: named text zone (2/3) or interval offset (1/3)
    let zone_arg = |g: &mut Gen| -> String {
        if g.rng.chance(1, 3) {
            format!("INTERVAL {}", g.rng.pick(OFFSETS))
        } else {
            format!("'{}'", g.rng.pick(ZONES))
        }
    };
    match g.rng.below(3) {
        // timezone(zone, timestamp) -> timestamptz
        0 => format!(
            "timezone({}, TIMESTAMP {})",
            zone_arg(g),
            g.rng.pick(TZF_TS)
        ),
        // timezone(zone, timestamptz) -> timestamp
        1 => format!(
            "timezone({}, TIMESTAMPTZ {})",
            zone_arg(g),
            g.rng.pick(TZF_TSTZ)
        ),
        // timezone(zone, timetz) -> timetz
        _ => format!(
            "timezone({}, TIMETZ {})",
            zone_arg(g),
            g.rng.pick(TZF_TIMETZ)
        ),
    }
}

fn gen_epoch(g: &mut Gen) -> String {
    g.fire("dtx:epoch");
    // render ::text so the timestamptz output is byte-compared under the
    // pinned session TimeZone.
    format!("to_timestamp({})::text", g.rng.pick(EPOCHS))
}
