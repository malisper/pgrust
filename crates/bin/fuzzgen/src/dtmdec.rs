//! LD3 datetime DECODE drain: raw-literal casts into date/time/timetz/
//! timestamp/timestamptz/interval, built as field permutations rather
//! than drawn from fixed pools.  The line-gap targets are the
//! datetime.c decode family — DecodeTimeOnly (line-gap-report-001 #10),
//! DecodeDateTime (#13), DecodeISO8601Interval (#74), EncodeInterval
//! (#64) and the DecodeNumber/DecodeNumberField/DecodeSpecial/
//! DecodeTimezone/DecodeTimezoneAbbrev helpers under them — whose unhit
//! lines are format-branch and field-permutation arms: every field
//! order, month-name spelling, fractional width, timezone spelling,
//! era, julian day, compact/ISO/traditional form.
//!
//! Reached from `gen_dtm_module` as the `dtm:decode` production; the
//! sub-shape choice points are `dtmdec:*` weights.  Determinism: the
//! session pin (runner::DATETIME_GUC_PIN — TimeZone=UTC,
//! DateStyle='ISO, MDY', IntervalStyle=postgres) covers every rendered
//! output; the `dtmdec:style` bracket deliberately switches
//! DateStyle/IntervalStyle for ONE cast and restores the pin in the
//! same statement group (applied identically on both sides, so the
//! differential stays sound — the encode arms per output style are the
//! target).  The nondeterministic specials (now/today/tomorrow/
//! yesterday) are only ever observed through `IS NOT NULL`; the
//! deterministic specials (epoch/infinity/allballs) render as text.
//! Malformed literals are first-class error fuel: both sides must
//! reject with the same SQLSTATE.

use crate::stmt::{Gen, StmtKind};

// ---------------------------------------------------------------------
// Field material.
// ---------------------------------------------------------------------

const YEARS: &[&str] = &["1999", "2024", "0001", "1066", "1969", "2000", "9999", "5874897", "0099"];

/// Month spellings: full/abbrev/case-mangled (DecodeSpecial and the
/// month table lookups downcase; the '.' abbreviation form has its own
/// arm in DecodeDate).
const MONTHS: &[&str] = &[
    "Jan", "January", "FEB", "february", "Mar", "APRIL", "may", "Jun", "JULY",
    "aug", "Sept", "sep", "October", "nov", "DEC", "Feb.", "Jan.",
];

const DAYS: &[&str] = &["1", "08", "15", "28", "29", "30", "31"];

/// Day-of-week names — legal noise fields in a date literal ("Thu Jan 08
/// 1999"); DecodeDateTime carries a DTK_STRING/day arm for them.
const DOWS: &[&str] = &["Mon", "Thursday", "SAT", "sun", "Wed."];

/// Timezone spellings: numeric offsets in every width (DecodeTimezone
/// h/hm/hms arms, colon and compact), known abbreviations
/// (DecodeTimezoneAbbrev), full zone names, and z/Z/zulu.
const TZ_SPELLINGS: &[&str] = &[
    "+05", "-08", "+0530", "+05:30", "+05:30:30", "-08:30:15", "+15:59:59",
    "UTC", "GMT", "utc", "EST", "PST", "CET", "z", "Z", "zulu",
    "America/New_York", "Asia/Kolkata", "Australia/Lord_Howe",
    // Fixed abbrevs with DST semantics (DTZ arm), the "abbr DST" spelling,
    // and dynamic zone-linked abbreviations (DYNTZ arm) from the default
    // timezone_abbreviations list — shared by both engines via the
    // reference share dir.
    "MET DST", "MEST", "MSK", "AEST", "EET",
    // Abbreviation-with-numeric-offset spellings: the
    // DecodeTimezoneAbbrevPrefix arms (LD10) — a known abbrev or GMT/UTC
    // prefix directly followed by a signed offset in each width.
    "GMT+5", "GMT-0830", "UTC+05:30", "GMT+10:30:30", "EST+2", "Z+03",
];

/// Fractional-second tails, width 1..7 (7 digits exercises the rounding
/// arm) plus bare trailing dot.
const FRACS: &[&str] = &["", "", ".5", ".78", ".789", ".1234", ".56789", ".678901", ".8765432"];

const MERIDIEMS: &[&str] = &["AM", "PM", "am", "pm", "A.M.", "P.M."];

/// Interval unit spellings incl. every alias family.
const IVL_UNITS: &[&str] = &[
    "microsecond", "microseconds", "us", "millisecond", "milliseconds", "ms",
    "second", "seconds", "sec", "secs", "s", "minute", "minutes", "min", "mins", "m",
    "hour", "hours", "hrs", "hr", "h", "day", "days", "d", "week", "weeks", "w",
    "month", "months", "mon", "mons", "year", "years", "yr", "yrs", "y",
    "decade", "decades", "dec", "decs", "century", "centuries", "c", "cent",
    "millennium", "millenniums", "mil", "mils",
];

const IVL_VALUES: &[&str] = &["1", "-1", "2", "-3", "0", "1.5", "-2.25", "100", "0.000001", "+7"];

/// Malformed decode fuel: field-range, conflict, dangling-tz, empty and
/// junk shapes. Both engines must reject each with the same SQLSTATE.
const BAD_LITERALS: &[(&str, &str)] = &[
    ("2023-02-30", "date"),
    ("1999-13-01", "date"),
    ("2024-00-10", "date"),
    ("25:61:00", "time"),
    ("24:00:00.001", "time"),
    ("23:59:61", "time"),
    ("12:00:00+16", "timetz"),
    ("12:00:00-16:00", "timetz"),
    ("1999-01-08 04:05:06 America/Nowhere", "timestamptz"),
    ("1999-01-08 25:00:00", "timestamp"),
    ("J99999999", "date"),
    ("P", "interval"),
    ("PT", "interval"),
    ("P1Y2X", "interval"),
    ("1999-01-08 04:05:06 +", "timestamptz"),
    ("", "date"),
    ("   ", "time"),
    ("totally bogus", "timestamp"),
    ("1999-01-08 Jan", "date"),
    ("04:05:06 04:05:06", "time"),
];

// ---------------------------------------------------------------------
// Literal builders.
// ---------------------------------------------------------------------

fn date_lit(g: &mut Gen) -> String {
    let y = *g.rng.pick(YEARS);
    let d = *g.rng.pick(DAYS);
    let mon_name = *g.rng.pick(MONTHS);
    let m = 1 + g.rng.below(12);
    let mut s = match g.rng.below(9) {
        0 => format!("{y}-{m:02}-{d}"),
        // Compact 8-digit / 6-digit (DecodeNumberField).
        1 => format!("{y:0>4.4}{m:02}{}", if d.len() == 1 { format!("0{d}") } else { d.to_string() }),
        2 => format!("990{}0{}", 1 + g.rng.below(9), 1 + g.rng.below(9)),
        // Day-of-year dotted form (DecodeNumber decimal arm).
        3 => format!("{y}.{:03}", 1 + g.rng.below(365)),
        // MDY slash under the pinned 'ISO, MDY'.
        4 => format!("{m}/{d}/{y}"),
        // Month-name permutations, optional day-of-week noise field.
        5 => {
            let core = match g.rng.below(4) {
                0 => format!("{mon_name} {d} {y}"),
                1 => format!("{d} {mon_name} {y}"),
                2 => format!("{y} {mon_name} {d}"),
                _ => format!("{d}-{mon_name}-{y}"),
            };
            if g.rng.chance(1, 4) {
                format!("{} {core}", *g.rng.pick(DOWS))
            } else {
                core
            }
        }
        // Julian day, sometimes with a day-fraction (the J-with-'.'
        // ParseFraction arms in DecodeDateTime/DecodeTimeOnly).
        6 => {
            let frac = if g.rng.chance(1, 3) { *g.rng.pick(&[".5", ".25", ".999"]) } else { "" };
            format!("J{}{frac}", 1 + g.rng.below(3_000_000))
        }
        // Two-digit year first (ambiguous-year arm).
        7 => format!("{:02}-{m:02}-{d}", g.rng.below(100)),
        _ => format!("{y}-{mon_name}-{d}"),
    };
    if g.rng.chance(1, 5) {
        s.push_str(if g.rng.chance(1, 2) { " BC" } else { " ad" });
    }
    s
}

fn time_core(g: &mut Gen) -> String {
    let h = g.rng.below(24);
    let mi = g.rng.below(60);
    let sec = g.rng.below(60);
    let frac = *g.rng.pick(FRACS);
    match g.rng.below(8) {
        // Meridiem forms (12-hour arms; hour 0 spelled 12).
        0 | 1 => {
            let h12 = 1 + g.rng.below(12);
            format!("{h12}:{mi:02}{frac} {}", *g.rng.pick(MERIDIEMS))
        }
        // Compact hhmmss / hhmm (DecodeNumberField time arm).
        2 => format!("{h:02}{mi:02}{sec:02}{frac}"),
        3 => format!("{h:02}{mi:02}"),
        // ISO 8601 "T" prefix.
        4 => format!("T{h:02}:{mi:02}:{sec:02}{frac}"),
        5 => format!("T{h:02}{mi:02}{sec:02}"),
        6 => format!("{h}:{mi:02}"),
        _ => format!("{h:02}:{mi:02}:{sec:02}{frac}"),
    }
}

fn time_lit(g: &mut Gen, with_tz: bool) -> String {
    let mut s = if g.rng.chance(1, 10) {
        // 24:00 and leap-second edges.
        if g.rng.chance(1, 2) { "24:00:00".to_string() } else { "23:59:60".to_string() }
    } else {
        time_core(g)
    };
    // Date-prefixed time literal: DecodeTimeOnly's date-field arms (the
    // date resolves named-zone offsets for timetz).
    if g.rng.chance(1, 4) {
        s = format!("{} {s}", date_lit(g));
    }
    if with_tz || g.rng.chance(1, 6) {
        let tz = *g.rng.pick(TZ_SPELLINGS);
        if g.rng.chance(1, 2) && !tz.starts_with(['+', '-']) {
            s.push(' ');
        }
        s.push_str(tz);
    }
    s
}

fn ts_lit(g: &mut Gen, with_tz: bool) -> String {
    let d = date_lit(g);
    let t = time_core(g);
    let mut s = match g.rng.below(6) {
        // Time-first field order.
        0 => format!("{t} {d}"),
        // ISO "T" separator, spaced and compact.
        1 => format!("{d}T{}", time_core(g)),
        _ => format!("{d} {t}"),
    };
    if with_tz || g.rng.chance(1, 5) {
        let tz = *g.rng.pick(TZ_SPELLINGS);
        if !tz.starts_with(['+', '-']) {
            s.push(' ');
        }
        s.push_str(tz);
    }
    s
}

fn interval_lit(g: &mut Gen) -> String {
    match g.rng.below(6) {
        // Verbose unit list, optional @ prefix / ago suffix.
        0 | 1 => {
            let n = 1 + g.rng.below(4);
            let mut parts = Vec::new();
            for _ in 0..n {
                parts.push(format!("{} {}", *g.rng.pick(IVL_VALUES), *g.rng.pick(IVL_UNITS)));
            }
            let mut s = parts.join(" ");
            if g.rng.chance(1, 4) {
                s = format!("@ {s}");
            }
            if g.rng.chance(1, 4) {
                s.push_str(" ago");
            }
            s
        }
        // SQL-standard year-month / day-time forms.
        2 => match g.rng.below(4) {
            0 => format!("{}-{}", g.rng.below(20), g.rng.below(12)),
            1 => format!("-{}-{}", g.rng.below(20), g.rng.below(12)),
            2 => format!("{} {:02}:{:02}:{:02}", g.rng.below(40), g.rng.below(24), g.rng.below(60), g.rng.below(60)),
            _ => format!("{:02}:{:02}", g.rng.below(24), g.rng.below(60)),
        },
        // ISO 8601 designator form, decimals included.
        3 | 4 => {
            let mut s = "P".to_string();
            if g.rng.chance(1, 2) {
                s.push_str(&format!("{}Y", g.rng.below(30)));
            }
            if g.rng.chance(1, 2) {
                s.push_str(&format!("{}M", g.rng.below(12)));
            }
            if g.rng.chance(1, 3) {
                s.push_str(&format!("{}W", g.rng.below(5)));
            }
            if g.rng.chance(1, 2) {
                s.push_str(&format!("{}D", g.rng.below(31)));
            }
            if g.rng.chance(2, 3) {
                s.push('T');
                if g.rng.chance(1, 2) {
                    s.push_str(&format!("{}H", g.rng.below(30)));
                }
                if g.rng.chance(1, 2) {
                    s.push_str(&format!("{}M", g.rng.below(60)));
                }
                s.push_str(&format!("{}{}S", g.rng.below(60), *g.rng.pick(FRACS)));
            }
            if s == "P" {
                s.push_str("0D");
            }
            s
        }
        // ISO 8601 alternative datetime-shaped form: extended, basic
        // (compact 8-digit / 6-digit — ISO8601IntegerWidth arms), and
        // truncated variants (each early-return is a line).
        _ => match g.rng.below(6) {
            0 => format!(
                "P{:04}{:02}{:02}T{:02}{:02}{:02}",
                g.rng.below(20), g.rng.below(12), g.rng.below(28),
                g.rng.below(24), g.rng.below(60), g.rng.below(60)
            ),
            1 => format!("P{:04}-{:02}", g.rng.below(20), g.rng.below(12)),
            2 => format!("P{:04}-{:02}-{:02}", g.rng.below(20), g.rng.below(12), g.rng.below(28)),
            3 => format!(
                "P{:04}-{:02}-{:02}T{:02}",
                g.rng.below(20), g.rng.below(12), g.rng.below(28), g.rng.below(24)
            ),
            4 => format!(
                "P{:04}-{:02}-{:02}T{:02}:{:02}",
                g.rng.below(20), g.rng.below(12), g.rng.below(28),
                g.rng.below(24), g.rng.below(60)
            ),
            _ => format!(
                "P{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                g.rng.below(20), g.rng.below(12), g.rng.below(28),
                g.rng.below(24), g.rng.below(60), g.rng.below(60)
            ),
        },
    }
}

/// Interval qualifier (typmod) ranges: the whole INTERVAL_MASK dispatch
/// in DecodeInterval is unreachable without them.
const IVL_RANGES: &[&str] = &[
    "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
    "YEAR TO MONTH", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND",
    "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND", "SECOND(3)",
];

/// Range-shaped literals: bare numbers and partial day-time strings whose
/// meaning the qualifier disambiguates (the ambiguous-unit arms).
const IVL_RANGE_LITS: &[&str] = &[
    "3", "-3", "4 5", "1-2", "-1-2", "4:05", "4:05:06", "3 4:05",
    "3 4:05:06.789", "-3 +4:05:06", "12:34:56.789012", "+4 -5:06",
];

/// Typed interval literals: `INTERVAL 'lit' <range>` (the typmod rides
/// the type, not the literal — a plain cast can't reach these arms).
fn typed_interval_stmt(g: &mut Gen) -> String {
    let n = 3 + g.rng.below(3);
    let cols: Vec<String> = (0..n)
        .map(|_| {
            let lit = if g.rng.chance(1, 3) {
                interval_lit(g)
            } else {
                (*g.rng.pick(IVL_RANGE_LITS)).to_string()
            };
            format!("(INTERVAL '{}' {})::text", esc(&lit), *g.rng.pick(IVL_RANGES))
        })
        .collect();
    format!("SELECT {};", cols.join(", "))
}

// ---------------------------------------------------------------------
// Statement shapes.
// ---------------------------------------------------------------------

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Batch of well-formed-by-construction casts rendered as text. A batch
/// member that still errors aborts the whole statement identically on
/// both sides — acceptable error fuel, not a soundness leak.
fn cast_batch(g: &mut Gen, ty: &str, mut lit: impl FnMut(&mut Gen) -> String) -> String {
    let n = 3 + g.rng.below(3);
    let cols: Vec<String> = (0..n)
        .map(|_| format!("('{}'::{})::text", esc(&lit(g)), ty))
        .collect();
    format!("SELECT {};", cols.join(", "))
}

fn special_stmt(g: &mut Gen) -> String {
    let ty = *g.rng.pick(&["date", "timestamp", "timestamptz", "time", "timetz"]);
    if g.rng.chance(1, 2) {
        // Nondeterministic specials: observed only through IS NOT NULL.
        let sp = *g.rng.pick(&["now", "today", "tomorrow", "yesterday", "NOW", "Tomorrow"]);
        if (ty == "time" || ty == "timetz") && sp != "now" && sp != "NOW" {
            return format!("SELECT ('now'::{ty}) IS NOT NULL;");
        }
        format!("SELECT ('{sp}'::{ty}) IS NOT NULL;")
    } else {
        let sp = match ty {
            "time" | "timetz" => *g.rng.pick(&["allballs", "ALLBALLS", "epoch"]),
            _ => *g.rng.pick(&["epoch", "infinity", "-infinity", "EPOCH", "Infinity"]),
        };
        // ('epoch' into time/timetz is an identical two-sided error —
        // deliberate DecodeSpecial-reject fuel.)
        format!("SELECT ('{sp}'::{ty})::text;")
    }
}

/// DateStyle/IntervalStyle bracket: switch, cast, restore the pin —
/// one group, identical on both sides (the EncodeDateTime/EncodeInterval
/// per-style output arms are the target).
fn style_bracket(g: &mut Gen) -> Vec<String> {
    if g.rng.chance(1, 2) {
        let style = *g.rng.pick(&[
            "'SQL, DMY'", "'SQL, MDY'", "'Postgres, DMY'", "'Postgres, MDY'", "'German'", "'ISO, YMD'",
        ]);
        let ty = *g.rng.pick(&["date", "timestamp", "timestamptz"]);
        let lit = match ty {
            "date" => esc(&date_lit(g)),
            "timestamp" => esc(&ts_lit(g, false)),
            _ => esc(&ts_lit(g, true)),
        };
        vec![
            format!("SET DateStyle = {style};"),
            format!("SELECT ('{lit}'::{ty})::text;"),
            "SET DateStyle = 'ISO, MDY';".to_string(),
        ]
    } else {
        let style = *g.rng.pick(&["sql_standard", "iso_8601", "postgres_verbose"]);
        let lit = esc(&interval_lit(g));
        vec![
            format!("SET IntervalStyle = {style};"),
            format!("SELECT ('{lit}'::interval)::text;"),
            "SET IntervalStyle = postgres;".to_string(),
        ]
    }
}

fn err_stmt(g: &mut Gen) -> String {
    let (lit, ty) = *g.rng.pick(BAD_LITERALS);
    format!("SELECT ('{}'::{})::text;", esc(lit), ty)
}

// ---------------------------------------------------------------------
// LD10 residual-arm shapes: the *_part_common / *_trunc / interval
// arithmetic / OVERLAPS families of timestamp.c + date.c (adt-datetime
// residual, line-gap-report-002) — every field keyword against every
// type incl. the per-type reject arms and the +/-infinity propagation
// arms, all rendered ::text (or ::numeric::text for EXTRACT).
// ---------------------------------------------------------------------

/// Every EXTRACT field keyword (valid and per-type-invalid — the reject
/// ereport of each *_part_common is a target arm too).
const EXTRACT_FIELDS: &[&str] = &[
    "century", "day", "decade", "dow", "doy", "epoch", "hour", "isodow",
    "isoyear", "julian", "microseconds", "millennium", "milliseconds",
    "minute", "month", "quarter", "second", "timezone", "timezone_hour",
    "timezone_minute", "week", "year",
];

/// Deterministic datetime operands for extract/trunc/arith shapes:
/// ordinary values, boundary years, BC, and the infinity specials (the
/// NonFiniteTimestampTzPart arms).
const TS_OPERANDS: &[(&str, &str)] = &[
    ("1999-01-08 04:05:06.789", "timestamp"),
    ("2024-02-29 23:59:59.999999", "timestamp"),
    ("0001-01-01 00:00:00 BC", "timestamp"),
    ("infinity", "timestamp"),
    ("-infinity", "timestamp"),
    ("1999-01-08 04:05:06+05:30", "timestamptz"),
    ("2005-12-31 23:59:59.5 PST", "timestamptz"),
    ("infinity", "timestamptz"),
    ("-infinity", "timestamptz"),
    ("2024-06-15", "date"),
    ("0044-03-15 BC", "date"),
    ("infinity", "date"),
    ("-infinity", "date"),
    ("04:05:06.789", "time"),
    ("23:59:60", "time"),
    ("04:05:06.789-08", "timetz"),
    ("11:30:00+05:30:30", "timetz"),
    ("1 year 2 mons 3 days 04:05:06.789", "interval"),
    ("-1 year -2 mons +3 days -04:05:06", "interval"),
    ("infinity", "interval"),
    ("-infinity", "interval"),
];

/// EXTRACT / date_part over the full field x type matrix (numeric and
/// float8 outputs both rendered as text).
fn extract_stmt(g: &mut Gen) -> String {
    let n = 3 + g.rng.below(3);
    let cols: Vec<String> = (0..n)
        .map(|_| {
            let (lit, ty) = *g.rng.pick(TS_OPERANDS);
            let f = *g.rng.pick(EXTRACT_FIELDS);
            if g.rng.chance(1, 3) {
                format!("date_part('{f}', '{}'::{ty})::text", esc(lit))
            } else {
                format!("extract({f} FROM '{}'::{ty})::text", esc(lit))
            }
        })
        .collect();
    format!("SELECT {};", cols.join(", "))
}

/// date_trunc units incl. the reject arms and the timezone-argument form.
const TRUNC_UNITS: &[&str] = &[
    "microseconds", "milliseconds", "second", "minute", "hour", "day",
    "week", "month", "quarter", "year", "decade", "century", "millennium",
    "timezone", "bogus",
];

const TRUNC_ZONES: &[&str] = &[
    "UTC", "America/New_York", "Asia/Kathmandu", "Australia/Lord_Howe", "+05:30",
];

fn trunc_stmt(g: &mut Gen) -> String {
    let u = *g.rng.pick(TRUNC_UNITS);
    let (lit, ty) = loop {
        let (l, t) = *g.rng.pick(TS_OPERANDS);
        if t == "timestamp" || t == "timestamptz" || t == "interval" {
            break (l, t);
        }
    };
    if ty == "timestamptz" && g.rng.chance(1, 3) {
        let z = *g.rng.pick(TRUNC_ZONES);
        format!(
            "SELECT date_trunc('{u}', '{}'::timestamptz, '{z}')::text;",
            esc(lit)
        )
    } else {
        format!("SELECT date_trunc('{u}', '{}'::{ty})::text;", esc(lit))
    }
}

/// interval * float8 / interval / float8 / justify_* — the overflow,
/// infinity-propagation and zero-divide arms of interval_mul/interval_div
/// plus interval_justify_interval edges.
fn ivarith_stmt(g: &mut Gen) -> String {
    let iv = *g.rng.pick(&[
        "1 year 2 mons 3 days 04:05:06",
        "-1 mons 30 days",
        "1 mon -30 days +23:59:60",
        "178000000 years",
        "2147483647 days",
        "infinity",
        "-infinity",
        "0",
        "1 day 25:00:00",
    ]);
    let f = *g.rng.pick(&[
        "2", "-1", "0.5", "-0.25", "0", "1e10", "-1e10", "0.000001",
        "'nan'::float8", "'infinity'::float8", "'-infinity'::float8",
    ]);
    match g.rng.below(5) {
        0 => format!("SELECT ('{}'::interval * {})::text;", esc(iv), f),
        1 => format!("SELECT ({} * '{}'::interval)::text;", f, esc(iv)),
        2 => format!("SELECT ('{}'::interval / {})::text;", esc(iv), f),
        3 => format!(
            "SELECT justify_hours('{}'::interval)::text, justify_days('{}'::interval)::text;",
            esc(iv),
            esc(iv)
        ),
        _ => format!("SELECT justify_interval('{}'::interval)::text;", esc(iv)),
    }
}

/// (a, b) OVERLAPS (c, d) over timestamp/time pairs with interval second
/// operands, NULL members and reversed endpoints (each null-combination
/// early-return in overlaps_timestamp/overlaps_time is a line region).
fn overlaps_stmt(g: &mut Gen) -> String {
    let time_side = g.rng.chance(1, 3);
    let mut pick_operand = |g: &mut Gen| -> String {
        if g.rng.chance(1, 6) {
            return "NULL".to_string();
        }
        if time_side {
            format!("TIME '{:02}:{:02}:00'", g.rng.below(24), g.rng.below(60))
        } else {
            format!(
                "TIMESTAMP '2024-{:02}-{:02} {:02}:00:00'",
                1 + g.rng.below(12),
                1 + g.rng.below(28),
                g.rng.below(24)
            )
        }
    };
    let a = pick_operand(g);
    let b = if g.rng.chance(1, 3) {
        format!("INTERVAL '{} hours'", g.rng.below(48))
    } else {
        pick_operand(g)
    };
    let c = pick_operand(g);
    let d = if g.rng.chance(1, 3) {
        format!("INTERVAL '{} days'", g.rng.below(4))
    } else {
        pick_operand(g)
    };
    format!("SELECT ({a}, {b}) OVERLAPS ({c}, {d});")
}

/// `dtm:decode` production body (dispatched from gen_dtm_module).
pub fn gen_decode_group(g: &mut Gen) -> Vec<StmtKind> {
    let sub = g.weights.pick(
        g.rng,
        &[
            "dtmdec:date",
            "dtmdec:time",
            "dtmdec:timetz",
            "dtmdec:ts",
            "dtmdec:tstz",
            "dtmdec:interval",
            "dtmdec:special",
            "dtmdec:style",
            "dtmdec:err",
            "dtmdec:extract",
            "dtmdec:trunc",
            "dtmdec:ivarith",
            "dtmdec:overlaps",
        ],
    );
    g.fire(sub);
    // Occasional precision typmod on the target type: the
    // AdjustFractSeconds/tm rounding arms are typmod-gated.
    let prec = |g: &mut Gen, base: &str| -> String {
        if g.rng.chance(1, 5) {
            format!("{base}({})", g.rng.below(7))
        } else {
            base.to_string()
        }
    };
    let stmts: Vec<String> = match sub {
        "dtmdec:date" => vec![cast_batch(g, "date", date_lit)],
        "dtmdec:time" => {
            let ty = prec(g, "time");
            vec![cast_batch(g, &ty, |g| time_lit(g, false))]
        }
        "dtmdec:timetz" => {
            let ty = prec(g, "timetz");
            vec![cast_batch(g, &ty, |g| time_lit(g, true))]
        }
        "dtmdec:ts" => {
            let ty = prec(g, "timestamp");
            vec![cast_batch(g, &ty, |g| ts_lit(g, false))]
        }
        "dtmdec:tstz" => {
            let ty = prec(g, "timestamptz");
            vec![cast_batch(g, &ty, |g| ts_lit(g, true))]
        }
        "dtmdec:interval" => {
            if g.rng.chance(1, 3) {
                vec![typed_interval_stmt(g)]
            } else {
                vec![cast_batch(g, "interval", interval_lit)]
            }
        }
        "dtmdec:special" => vec![special_stmt(g)],
        "dtmdec:style" => style_bracket(g),
        "dtmdec:extract" => vec![extract_stmt(g)],
        "dtmdec:trunc" => vec![trunc_stmt(g)],
        "dtmdec:ivarith" => vec![ivarith_stmt(g)],
        "dtmdec:overlaps" => vec![overlaps_stmt(g)],
        _ => vec![err_stmt(g)],
    };
    stmts.into_iter().map(StmtKind::Raw).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Same seed -> byte-identical groups; every statement terminated and
    /// quote-balanced (no literal breakout from generated material).
    #[test]
    fn decode_groups_are_deterministic_and_quote_safe() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        for seed in [1u64, 7, 42] {
            let run = || {
                let mut rng = Rng::new(seed);
                let mut out = Vec::new();
                for _ in 0..300 {
                    let mut prods = Vec::new();
                    let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                    let stmts: Vec<String> =
                        gen_decode_group(&mut g).iter().map(|s| s.to_sql()).collect();
                    out.push(stmts);
                }
                out
            };
            let a = run();
            assert_eq!(a, run());
            for group in &a {
                for s in group {
                    assert!(s.ends_with(';'), "unterminated: {s}");
                    assert_eq!(s.matches('\'').count() % 2, 0, "odd quotes: {s}");
                }
            }
        }
    }

    /// A style bracket always restores the DATETIME_GUC_PIN value in the
    /// same group (differential-soundness invariant).
    #[test]
    fn style_brackets_always_restore_the_pin() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(11);
        let mut fired = std::collections::BTreeSet::new();
        for _ in 0..2000 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts: Vec<String> =
                gen_decode_group(&mut g).iter().map(|s| s.to_sql()).collect();
            let sets = stmts.iter().filter(|s| s.starts_with("SET ")).count();
            if sets > 0 {
                assert_eq!(sets, 2, "unbalanced style bracket: {stmts:?}");
                let last = stmts.last().unwrap();
                assert!(
                    last == "SET DateStyle = 'ISO, MDY';"
                        || last == "SET IntervalStyle = postgres;",
                    "bracket does not restore the pin: {stmts:?}"
                );
            }
            for p in &prods {
                fired.insert(p.clone());
            }
        }
        for p in [
            "dtmdec:date", "dtmdec:time", "dtmdec:timetz", "dtmdec:ts", "dtmdec:tstz",
            "dtmdec:interval", "dtmdec:special", "dtmdec:style", "dtmdec:err",
            "dtmdec:extract", "dtmdec:trunc", "dtmdec:ivarith", "dtmdec:overlaps",
        ] {
            assert!(fired.contains(p), "sub-production {p} never fired");
        }
    }
}
