//! A2 datetime cross-type matrix: literal-only scalar SELECTs over
//! date/time/timestamp/timestamptz/interval/timetz — the adt target-#2
//! surface (timestamp.c/date.c/datetime.c/formatting.c and their ported
//! counterparts crates/backend/utils/adt/{adt_timestamp,adt_date,
//! adt_datetime,formatting}).
//!
//! Emitted as `StmtKind::Raw`: every operand is a literal drawn from the
//! seeded pools below (NEVER now()/current_date — the only nondeterministic
//! surface deliberately touched is single-arity age(), and only over
//! ±infinity inputs, where the hidden current-date operand cannot reach the
//! result). Determinism preconditions live in the runner's session pin
//! (runner::DATETIME_GUC_PIN: TimeZone=UTC, DateStyle='ISO, MDY',
//! IntervalStyle=postgres, applied to BOTH differential sides) and in the
//! harness tzdata-parity rule (PGRUST_TZDIR pointed at the reference
//! install's share/timezone — both engines read the same compiled tzdata).
//!
//! Productions (weights in weights.rs, all `dtm:` prefixed):
//!   arith     +/- across every legal type pair (date±int, date±interval,
//!             date+time, date+timetz, time/timetz±interval, ts/tstz±
//!             interval, ts-ts, tstz-tstz, date-date, time-time,
//!             interval±interval, -interval, interval*num, interval/num)
//!   cmp       cross-type comparisons (date vs ts vs tstz, time vs timetz
//!             via conversion, interval vs interval), BETWEEN
//!   overlaps  (a,b) OVERLAPS (c,d) over ts/date/time pairs, incl. the
//!             (start, interval) form
//!   trunc     date_trunc over every field name incl. week/quarter/
//!             millennium; sources ts/tstz/interval/date; 3-arg zone form
//!   extract   EXTRACT / date_part, every field incl. epoch/julian/
//!             isoyear/dow/doy/timezone_hour/timezone_minute (extract
//!             returns numeric, date_part float8 — both surfaces)
//!   bin       date_bin(stride, source, origin); month/zero strides as
//!             low-weight error fuel
//!   age       age(ts, ts) both orders; single-arity ONLY over ±infinity
//!   justify   justify_hours/days/interval over carry-heavy intervals
//!   make      make_date/make_time/make_timestamp/make_timestamptz/
//!             make_interval; low-weight error fuel (month 13, day 32,
//!             hour 24, overflow months)
//!   attz      AT TIME ZONE both directions (ts->tstz, tstz->ts, timetz)
//!             over the fixed zone pool + interval offsets
//!   tochar    to_char over ts/tstz/date/time/interval with DCH pictures
//!             (rich::gen_dch_picture, incl. TZ/OF/TZH/TZM tokens)
//!   cast      cross-type casts where legal + ::text round-trips

use crate::catalog::SqlType;
use crate::stmt::{Gen, StmtKind};

/// Fixed named-zone pool (tzdata-backed; 30-minute offsets and 30-minute
/// DST included on purpose). Zone-database parity between the two engines
/// is a harness precondition — see the module doc.
pub const TZ_NAMES: &[&str] = &[
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Australia/Lord_Howe",
];

/// AT TIME ZONE arguments: the named pool plus fixed-offset spellings and
/// the interval form (zone-abbreviation-free — abbreviation files are a
/// separate config surface).
const TZ_ARGS: &[&str] = &[
    "'UTC'",
    "'America/New_York'",
    "'Asia/Kolkata'",
    "'Australia/Lord_Howe'",
    "'UTC+5'",
    "'<+0530>-5:30'",
    "INTERVAL '+05:30'",
    "INTERVAL '-08:00'",
];

/// Date literals: epoch edges, leap days (incl. the 1900/2000 century
/// rule), julian-calendar floor, BC dates, the 5874897 AD extreme, and
/// ±infinity. All ISO-rendered under the pinned DateStyle.
const DATES: &[&str] = &[
    "'1970-01-01'",
    "'2000-01-01'",
    "'2000-02-29'",
    "'1900-02-28'",
    "'1600-02-29'",
    "'2024-02-29'",
    "'1969-12-31'",
    "'0001-01-01'",
    "'0001-12-31 BC'",
    "'4714-11-24 BC'",
    "'5874897-12-31'",
    "'275760-09-13'",
    "'9999-12-31'",
    "'1582-10-15'",
    "'infinity'",
    "'-infinity'",
    "'epoch'",
];

/// Timestamp literals: DST transition instants (meaningful when shifted
/// through the named-zone pool), microsecond edges, range extremes,
/// ±infinity.
const TIMESTAMPS: &[&str] = &[
    "'1970-01-01 00:00:00'",
    "'2000-01-01 00:00:00'",
    "'1999-12-31 23:59:59.999999'",
    "'2000-01-01 00:00:00.000001'",
    "'2021-03-14 02:30:00'",
    "'2021-11-07 01:30:00'",
    "'2020-10-04 02:15:00'",
    "'2020-04-05 01:45:00'",
    "'2038-01-19 03:14:08'",
    "'1901-12-13 20:45:52'",
    "'294276-12-31 23:59:59.999999'",
    "'4714-11-24 00:00:00 BC'",
    "'2004-02-29 13:37:00.5'",
    "'infinity'",
    "'-infinity'",
    "'epoch'",
];

/// Timestamptz literals carry explicit offsets (session TimeZone is
/// pinned UTC, so offset-free spellings read as UTC).
const TIMESTAMPTZS: &[&str] = &[
    "'1970-01-01 00:00:00+00'",
    "'2000-01-01 00:00:00+00'",
    "'2021-03-14 06:59:59.999999+00'",
    "'2021-11-07 05:30:00+00'",
    "'2020-10-03 15:30:00+00'",
    "'2019-12-31 18:30:00+05:30'",
    "'2000-06-01 12:00:00-08'",
    "'1969-12-31 23:59:59.999999+00'",
    "'294276-12-31 23:59:59.999999 UTC'",
    "'4714-11-24 00:00:00+00 BC'",
    "'infinity'",
    "'-infinity'",
    "'epoch'",
];

/// Time literals: 24:00:00 (allowed), fractional-second edges.
const TIMES: &[&str] = &[
    "'00:00:00'",
    "'24:00:00'",
    "'23:59:59.999999'",
    "'12:00:00.5'",
    "'01:23:45.678901'",
    "'15:00:00'",
    "'00:00:00.000001'",
];

/// Timetz literals: extreme legal offsets (±15:59:59), half-hour and
/// 45-minute zones, 24:00.
const TIMETZS: &[&str] = &[
    "'00:00:00+00'",
    "'24:00:00+00'",
    "'23:59:59.999999-08'",
    "'12:00:00+05:45'",
    "'10:30:00+05:30'",
    "'15:00:00+15:59:59'",
    "'12:00:00-15:59:59'",
    "'07:00:00.25+10:30'",
];

/// Interval literals: ISO-8601 forms (DecodeISO8601Interval — the
/// gap-report-005 #29 target), mixed-sign month/day/us, justify targets,
/// extreme months/days/us, verbose form, ±infinity.
const INTERVALS: &[&str] = &[
    "'0'",
    "'P1Y2M3DT4H5M6S'",
    "'P-1Y-2M3DT-4H'",
    "'PT0.000001S'",
    "'P0.5Y'",
    "'P1.5M'",
    "'1 mon -1 day'",
    "'-1 mon 30 days'",
    "'1 day 25 hours'",
    "'-1 day +26:03:04.005006'",
    "'35 days 900 minutes'",
    "'178956970 years 7 mons'",
    "'-178956970 years -8 mons'",
    "'2147483647 days'",
    "'-2147483648 days'",
    "'2562047788:00:54.775806'",
    "'9223372036854.775806 seconds'",
    "'-9223372036854.775807 seconds'",
    "'@ 1 year 2 mons ago'",
    "'1.5 weeks'",
    "'00:00:00.000001'",
    "'infinity'",
    "'-infinity'",
];

/// date_trunc / EXTRACT field names — the full set, incl. the ones only
/// valid for some source types (identical two-sided rejection is the
/// assertion for the rest).
const TRUNC_FIELDS: &[&str] = &[
    "millennium", "century", "decade", "year", "quarter", "month", "week",
    "day", "hour", "minute", "second", "milliseconds", "microseconds",
];

const EXTRACT_FIELDS: &[&str] = &[
    "epoch", "millennium", "century", "decade", "year", "quarter", "month",
    "week", "day", "hour", "minute", "second", "milliseconds",
    "microseconds", "dow", "isodow", "doy", "isoyear", "julian",
    "timezone", "timezone_hour", "timezone_minute",
];

/// date_bin strides; month/year and zero strides are deliberate error
/// fuel (kept rare by the caller).
const BIN_STRIDES: &[&str] = &["'15 minutes'", "'1 hour'", "'1 day'", "'7 days'", "'0.000001 seconds'"];
const BIN_STRIDES_ERR: &[&str] = &["'1 month'", "'0'", "'-1 hour'"];

/// The six matrix types (timestamptz/timetz have no SqlType — dtm is
/// literal-only, so it names them textually).
#[derive(Clone, Copy, PartialEq)]
enum DtmTy {
    Date,
    Time,
    Timetz,
    Timestamp,
    Timestamptz,
    Interval,
}

const ALL_DTM: &[DtmTy] = &[
    DtmTy::Date,
    DtmTy::Time,
    DtmTy::Timetz,
    DtmTy::Timestamp,
    DtmTy::Timestamptz,
    DtmTy::Interval,
];

impl DtmTy {
    fn name(self) -> &'static str {
        match self {
            DtmTy::Date => "date",
            DtmTy::Time => "time",
            DtmTy::Timetz => "timetz",
            DtmTy::Timestamp => "timestamp",
            DtmTy::Timestamptz => "timestamptz",
            DtmTy::Interval => "interval",
        }
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_dtm_module(g: &mut Gen) -> Vec<StmtKind> {
    let pick = g.weights.pick(
        g.rng,
        &[
            "dtm:arith",
            "dtm:cmp",
            "dtm:overlaps",
            "dtm:trunc",
            "dtm:extract",
            "dtm:bin",
            "dtm:age",
            "dtm:justify",
            "dtm:make",
            "dtm:attz",
            "dtm:tochar",
            "dtm:cast",
            "dtm:decode",
        ],
    );
    // LD3 decode drain: raw-literal field-permutation casts (dtmdec.rs).
    // Groups can be multi-statement (the style bracket), so it short-
    // circuits the single-SELECT wrapper below.
    if pick == "dtm:decode" {
        g.fire("dtm:decode");
        return crate::dtmdec::gen_decode_group(g);
    }
    let expr = match pick {
        "dtm:arith" => gen_arith(g),
        "dtm:cmp" => gen_cmp(g),
        "dtm:overlaps" => gen_overlaps(g),
        "dtm:trunc" => gen_trunc(g),
        "dtm:extract" => gen_extract(g),
        "dtm:bin" => gen_bin(g),
        "dtm:age" => gen_age(g),
        "dtm:justify" => gen_justify(g),
        "dtm:make" => gen_make(g),
        "dtm:attz" => gen_attz(g),
        "dtm:tochar" => gen_tochar(g),
        _ => gen_cast(g),
    };
    vec![StmtKind::Raw(format!("SELECT {};", expr))]
}

/// One typed literal, spelled `TYPE 'value'`. Occasionally (1/6) falls
/// back to the generic expr-module literal pool for date/time/timestamp/
/// interval so the two pools cross-pollinate.
fn lit(g: &mut Gen, ty: DtmTy) -> String {
    if g.rng.chance(1, 6) {
        let sql_ty = match ty {
            DtmTy::Date => Some(SqlType::Date),
            DtmTy::Time => Some(SqlType::Time),
            DtmTy::Timetz => Some(SqlType::Timetz),
            DtmTy::Timestamp => Some(SqlType::Timestamp),
            DtmTy::Interval => Some(SqlType::Interval),
            DtmTy::Timestamptz => None,
        };
        if let Some(t) = sql_ty {
            return g.gen_literal(t);
        }
    }
    let pool: &[&str] = match ty {
        DtmTy::Date => DATES,
        DtmTy::Time => TIMES,
        DtmTy::Timetz => TIMETZS,
        DtmTy::Timestamp => TIMESTAMPS,
        DtmTy::Timestamptz => TIMESTAMPTZS,
        DtmTy::Interval => INTERVALS,
    };
    format!("{} {}", ty.name().to_uppercase(), g.rng.pick(pool))
}

fn gen_arith(g: &mut Gen) -> String {
    g.fire("dtm:arith");
    use DtmTy::*;
    match g.rng.below(16) {
        // date ± integer
        0 => {
            let d = lit(g, Date);
            let n = g.rng.range_i64(-40000, 40000);
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", d, op, n)
        }
        // date - date -> integer
        1 => format!("({}) - ({})", lit(g, Date), lit(g, Date)),
        // date ± interval -> timestamp
        2 => {
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", lit(g, Date), op, lit(g, Interval))
        }
        // date + time -> timestamp
        3 => format!("({}) + ({})", lit(g, Date), lit(g, Time)),
        // date + timetz -> timestamptz
        4 => format!("({}) + ({})", lit(g, Date), lit(g, Timetz)),
        // time ± interval -> time (wraps)
        5 => {
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", lit(g, Time), op, lit(g, Interval))
        }
        // time - time -> interval
        6 => format!("({}) - ({})", lit(g, Time), lit(g, Time)),
        // timetz ± interval
        7 => {
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", lit(g, Timetz), op, lit(g, Interval))
        }
        // timestamp ± interval
        8 => {
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", lit(g, Timestamp), op, lit(g, Interval))
        }
        // timestamp - timestamp -> interval
        9 => format!("({}) - ({})", lit(g, Timestamp), lit(g, Timestamp)),
        // timestamptz ± interval (DST-aware when shifted through zones)
        10 => {
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", lit(g, Timestamptz), op, lit(g, Interval))
        }
        // timestamptz - timestamptz -> interval
        11 => format!("({}) - ({})", lit(g, Timestamptz), lit(g, Timestamptz)),
        // interval ± interval
        12 => {
            let op = if g.rng.chance(1, 2) { "+" } else { "-" };
            format!("({}) {} ({})", lit(g, Interval), op, lit(g, Interval))
        }
        // -interval
        13 => format!("- ({})", lit(g, Interval)),
        // interval * numeric factor (float path in C)
        14 => {
            let k = ["2", "0.5", "-3", "2.000001", "0", "8784.0"];
            format!("({}) * ({})", lit(g, Interval), g.rng.pick(&k))
        }
        // interval / numeric factor (÷0 = error fuel, rare)
        _ => {
            let k = if g.rng.chance(1, 8) { "0" } else { *g.rng.pick(&["2", "-0.5", "3", "1e6"]) };
            format!("({}) / ({})", lit(g, Interval), k)
        }
    }
}

fn gen_cmp(g: &mut Gen) -> String {
    g.fire("dtm:cmp");
    use DtmTy::*;
    let op = *g.rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
    // Legal comparison pairs incl. the cross-type date/timestamp/
    // timestamptz operator families.
    let (l, r) = match g.rng.below(8) {
        0 => (Date, Date),
        1 => (Date, Timestamp),
        2 => (Date, Timestamptz),
        3 => (Timestamp, Timestamptz),
        4 => (Timestamp, Timestamp),
        5 => (Time, Time),
        6 => (Timetz, Timetz),
        _ => (Interval, Interval),
    };
    if g.rng.chance(1, 5) {
        format!("({}) BETWEEN ({}) AND ({})", lit(g, l), lit(g, r), lit(g, r))
    } else if g.rng.chance(1, 2) {
        format!("({}) {} ({})", lit(g, l), op, lit(g, r))
    } else {
        format!("({}) {} ({})", lit(g, r), op, lit(g, l))
    }
}

fn gen_overlaps(g: &mut Gen) -> String {
    g.fire("dtm:overlaps");
    use DtmTy::*;
    let ty = *g.rng.pick(&[Timestamp, Timestamptz, Date, Time]);
    let bound = |g: &mut Gen| -> String {
        if g.rng.chance(1, 4) {
            lit(g, Interval) // (start, interval) form
        } else {
            lit(g, ty)
        }
    };
    let (a, b) = (lit(g, ty), bound(g));
    let (c, d) = (lit(g, ty), bound(g));
    format!("(({}), ({})) OVERLAPS (({}), ({}))", a, b, c, d)
}

fn gen_trunc(g: &mut Gen) -> String {
    g.fire("dtm:trunc");
    use DtmTy::*;
    let field = *g.rng.pick(TRUNC_FIELDS);
    match g.rng.below(5) {
        0 => format!("date_trunc('{}', {})", field, lit(g, Timestamp)),
        1 => format!("date_trunc('{}', {})", field, lit(g, Timestamptz)),
        2 => format!("date_trunc('{}', {})", field, lit(g, Interval)),
        // date promotes to timestamp
        3 => format!("date_trunc('{}', {})", field, lit(g, Date)),
        // 3-arg zone form (timestamptz only)
        _ => {
            let zone = *g.rng.pick(TZ_NAMES);
            format!("date_trunc('{}', {}, '{}')", field, lit(g, Timestamptz), zone)
        }
    }
}

fn gen_extract(g: &mut Gen) -> String {
    g.fire("dtm:extract");
    use DtmTy::*;
    let field = *g.rng.pick(EXTRACT_FIELDS);
    let ty = *ALL_DTM.get(g.rng.below_usize(ALL_DTM.len())).unwrap_or(&Timestamp);
    let arg = lit(g, ty);
    if g.rng.chance(1, 2) {
        // EXTRACT returns numeric (lowercase `from`: statement-level
        // convention — uppercase " FROM " introduces a relation source).
        format!("EXTRACT({} from {})", field, arg)
    } else {
        // date_part returns float8 — the other numeric surface.
        format!("date_part('{}', {})", field, arg)
    }
}

fn gen_bin(g: &mut Gen) -> String {
    g.fire("dtm:bin");
    use DtmTy::*;
    let stride = if g.rng.chance(1, 8) {
        *g.rng.pick(BIN_STRIDES_ERR)
    } else {
        *g.rng.pick(BIN_STRIDES)
    };
    let ty = if g.rng.chance(1, 2) { Timestamp } else { Timestamptz };
    let src = lit(g, ty);
    let origin = lit(g, ty);
    format!("date_bin({}, {}, {})", stride, src, origin)
}

fn gen_age(g: &mut Gen) -> String {
    g.fire("dtm:age");
    use DtmTy::*;
    let ty = if g.rng.chance(1, 2) { Timestamp } else { Timestamptz };
    if g.rng.chance(1, 4) {
        // Single-arity age(): hidden current-date operand — deterministic
        // ONLY over ±infinity (the result saturates regardless of today).
        let inf = if g.rng.chance(1, 2) { "'infinity'" } else { "'-infinity'" };
        format!("age({} {})", ty.name().to_uppercase(), inf)
    } else {
        format!("age({}, {})", lit(g, ty), lit(g, ty))
    }
}

fn gen_justify(g: &mut Gen) -> String {
    g.fire("dtm:justify");
    let f = *g.rng.pick(&["justify_hours", "justify_days", "justify_interval"]);
    format!("{}({})", f, lit(g, DtmTy::Interval))
}

fn gen_make(g: &mut Gen) -> String {
    g.fire("dtm:make");
    let fuel = g.rng.chance(1, 6); // low-weight error fuel
    let year = if fuel {
        *g.rng.pick(&["0", "5874898", "-2147483648"])
    } else {
        *g.rng.pick(&["2024", "1970", "-44", "1", "5874897", "9999"])
    };
    let month = if fuel { "13" } else { *g.rng.pick(&["1", "2", "6", "12"]) };
    let day = if fuel {
        *g.rng.pick(&["32", "0", "30"]) // Feb 30 via month=2 stays fuel too
    } else {
        *g.rng.pick(&["1", "28", "29", "31"])
    };
    match g.rng.below(5) {
        0 => format!("make_date({}, {}, {})", year, month, day),
        1 => {
            let (h, m, s) = if fuel {
                ("24", "60", "61.0")
            } else {
                (*g.rng.pick(&["0", "12", "23"]), "59", *g.rng.pick(&["0.0", "59.999999", "30.5"]))
            };
            format!("make_time({}, {}, {})", h, m, s)
        }
        2 => format!(
            "make_timestamp({}, {}, {}, {}, {}, {})",
            year,
            month,
            day,
            g.rng.below(24),
            g.rng.below(60),
            if fuel { "60.5" } else { "1.000001" }
        ),
        3 => {
            let zone = *g.rng.pick(TZ_NAMES);
            format!(
                "make_timestamptz({}, {}, {}, {}, {}, {}, '{}')",
                year,
                month,
                day,
                g.rng.below(24),
                g.rng.below(60),
                "0.5",
                zone
            )
        }
        _ => {
            let months = if fuel { "2147483647" } else { *g.rng.pick(&["0", "13", "-14"]) };
            let days = *g.rng.pick(&["0", "40", "-40"]);
            let secs = *g.rng.pick(&["0.0", "0.000001", "-86400.5", "9223372036.854"]);
            format!(
                "make_interval(years => 0, months => {}, days => {}, secs => {})",
                months, days, secs
            )
        }
    }
}

fn gen_attz(g: &mut Gen) -> String {
    g.fire("dtm:attz");
    use DtmTy::*;
    let zone = *g.rng.pick(TZ_ARGS);
    match g.rng.below(4) {
        // timestamp AT TIME ZONE zone -> timestamptz
        0 => format!("({}) AT TIME ZONE {}", lit(g, Timestamp), zone),
        // timestamptz AT TIME ZONE zone -> timestamp
        1 => format!("({}) AT TIME ZONE {}", lit(g, Timestamptz), zone),
        // timetz AT TIME ZONE zone -> timetz
        2 => format!("({}) AT TIME ZONE {}", lit(g, Timetz), zone),
        // round-trip: shift out and back (identity except DST holes)
        _ => format!(
            "(({}) AT TIME ZONE {}) AT TIME ZONE {}",
            lit(g, Timestamp),
            zone,
            zone
        ),
    }
}

fn gen_tochar(g: &mut Gen) -> String {
    g.fire("dtm:tochar");
    use DtmTy::*;
    let ty = *g.rng.pick(&[Timestamp, Timestamptz, Date, Time, Interval]);
    let pic = g.gen_dch_picture();
    format!("to_char({}, '{}')", lit(g, ty), pic)
}

fn gen_cast(g: &mut Gen) -> String {
    g.fire("dtm:cast");
    use DtmTy::*;
    // Legal cross-type casts among the six.
    const CASTS: &[(DtmTy, DtmTy)] = &[
        (Date, Timestamp),
        (Date, Timestamptz),
        (Timestamp, Date),
        (Timestamp, Time),
        (Timestamp, Timestamptz),
        (Timestamptz, Date),
        (Timestamptz, Time),
        (Timestamptz, Timetz),
        (Timestamptz, Timestamp),
        (Time, Interval),
        (Timetz, Time),
        (Interval, Time),
    ];
    if g.rng.chance(1, 3) {
        // ::text round-trip: type -> text -> type (identity modulo
        // canonicalization — the I/O function pair under one roof).
        let ty = *g.rng.pick(ALL_DTM);
        format!(
            "CAST(CAST(({}) AS text) AS {})",
            lit(g, ty),
            ty.name()
        )
    } else {
        let (from, to) = *g.rng.pick(CASTS);
        format!("CAST(({}) AS {})", lit(g, from), to.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Textual invariants for every dtm production: single line,
    /// ';'-terminated, balanced parens, no now()/current_date/current_time
    /// (the determinism law), and single-arity age only over ±infinity.
    #[test]
    fn dtm_statements_are_deterministic_shapes() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(0xA2);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..3000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts = gen_dtm_module(&mut g);
            // Single SELECT for the matrix shapes; dtm:decode groups may
            // carry up to 3 statements (the dtmdec style bracket).
            assert!((1..=3).contains(&stmts.len()), "bad group size: {}", stmts.len());
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
                if low.starts_with("select age(timestamp") && !low.contains(',') {
                    assert!(low.contains("infinity"), "1-ary age over finite input: {sql}");
                }
            }
            for p in &prods {
                seen.insert(p.clone());
            }
        }
        // Every production fires under defaults within the budget.
        for p in [
            "dtm:arith", "dtm:cmp", "dtm:overlaps", "dtm:trunc", "dtm:extract",
            "dtm:bin", "dtm:age", "dtm:justify", "dtm:make", "dtm:attz",
            "dtm:tochar", "dtm:cast", "dtm:decode",
        ] {
            assert!(seen.contains(p), "production {p} never fired");
        }
    }

    /// Same seed -> byte-identical statements (the reproducibility law).
    #[test]
    fn dtm_is_seed_deterministic() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let run = || {
            let mut rng = Rng::new(77);
            let mut out = Vec::new();
            for _ in 0..500 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                out.push(gen_dtm_module(&mut g)[0].to_sql());
            }
            out
        };
        assert_eq!(run(), run());
    }
}
