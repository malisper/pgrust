use super::*;
use adt_datetime::{
    set_date_style, DATEORDER_DMY, DATEORDER_MDY, USE_GERMAN_DATES, USE_ISO_DATES,
    USE_POSTGRES_DATES, USE_SQL_DATES,
};

fn gmt_session() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // SAFETY: single-threaded test init, before any getenv (adt_datetime
        // tests' precedent).
        unsafe { std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo") };
        pgtz::init_seams();
        init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
    tz::pg_timezone_initialize();
}

fn zone_session(name: &[u8]) -> &'static tz::PgTz {
    gmt_session();
    let z = tz::pg_tzset(name).expect("zone loads from PGRUST_TZDIR");
    tz::set_session_timezone(Some(z));
    z
}

fn ts_in(s: &str) -> Timestamp {
    timestamp_in(s, -1, None).unwrap()
}

fn tstz_in(s: &str) -> TimestampTz {
    timestamptz_in(s, -1, None).unwrap()
}

fn ts_out(ts: Timestamp) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = timestamp_out(ts, &mut buf).unwrap();
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn tstz_out(ts: TimestampTz) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = timestamptz_out(ts, &mut buf).unwrap();
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

#[test]
fn epoch_timestamp_matches_c_constant() {
    gmt_session();
    // (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY
    assert_eq!(SetEpochTimestamp(), -946_684_800_000_000);
    let mut tm = pg_tm::default();
    GetEpochTime(&mut tm);
    assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (1970, 1, 1));
    assert_eq!((tm.tm_hour, tm.tm_min, tm.tm_sec), (0, 0, 0));
}

#[test]
fn timestamp2tm_tm2timestamp_round_trip() {
    for &dt in &[
        0i64,
        1,
        -1,
        34_488_306_789_000,
        -946_684_800_000_000,
        MIN_TIMESTAMP,
        END_TIMESTAMP - 1,
    ] {
        let mut tm = pg_tm::default();
        let mut fsec = 0;
        timestamp2tm(dt, None, &mut tm, &mut fsec, None, None).unwrap();
        let mut back = 0;
        tm2timestamp(&tm, fsec, None, &mut back).unwrap();
        assert_eq!(back, dt, "round trip failed for {dt}");
    }
}

#[test]
fn tm2timestamp_range_errors_match_c() {
    let tm = pg_tm { tm_year: 294277, tm_mon: 1, tm_mday: 1, ..Default::default() };
    let mut out = 5;
    assert!(tm2timestamp(&tm, 0, None, &mut out).is_err());
    assert_eq!(out, 0);

    let tm = pg_tm { tm_year: -4713, tm_mon: 11, tm_mday: 24, ..Default::default() };
    let mut out = 0;
    tm2timestamp(&tm, 0, None, &mut out).unwrap();
    assert_eq!(out, MIN_TIMESTAMP);

    let tm = pg_tm { tm_year: -4713, tm_mon: 11, tm_mday: 23, ..Default::default() };
    assert!(tm2timestamp(&tm, 0, None, &mut out).is_err());
}

#[test]
fn adjust_timestamp_for_typmod_rounds_like_c() {
    let mut t = 1_234_567i64;
    AdjustTimestampForTypmod(&mut t, 3, None).unwrap();
    assert_eq!(t, 1_235_000);

    let mut t = -1_234_567i64;
    AdjustTimestampForTypmod(&mut t, 3, None).unwrap();
    assert_eq!(t, -1_235_000);

    let mut t = 1_234_449i64;
    AdjustTimestampForTypmod(&mut t, 4, None).unwrap();
    assert_eq!(t, 1_234_400);

    let mut inf = DT_NOEND;
    AdjustTimestampForTypmod(&mut inf, 0, None).unwrap();
    assert_eq!(inf, DT_NOEND);

    let mut t = 42i64;
    let err = AdjustTimestampForTypmod(&mut t, 7, None).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(err.message.contains("timestamp(7) precision must be between 0 and 6"));

    let mut soft = SoftErrorContext::new(true);
    assert!(AdjustTimestampForTypmod(&mut t, 9, Some(&mut soft)).is_ok());
    assert!(soft.error_occurred());
}

// Expected strings below are live psql output from PostgreSQL 18.3
// (timezone=GMT), captured 2026-07-02.
#[test]
fn timestamp_io_differential_vs_pg18_iso() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    let cases = [
        ("2001-02-03 04:05:06.789", "2001-02-03 04:05:06.789"),
        ("1997-12-17 07:37:16.00", "1997-12-17 07:37:16"),
        ("epoch", "1970-01-01 00:00:00"),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
        ("4714-11-24 00:00:00 BC", "4714-11-24 00:00:00 BC"),
        ("294276-12-31 23:59:59.999999", "294276-12-31 23:59:59.999999"),
        ("1999-01-08 04:05:06", "1999-01-08 04:05:06"),
    ];
    for (input, expected) in cases {
        assert_eq!(ts_out(ts_in(input)), expected, "timestamp_in({input:?})");
    }

    let t = timestamp_in("2001-02-03 04:05:06.789", 1, None).unwrap();
    assert_eq!(ts_out(t), "2001-02-03 04:05:06.8");
    let t = timestamp_in("2004-02-29 13:44:21.500001", 4, None).unwrap();
    assert_eq!(ts_out(t), "2004-02-29 13:44:21.5");
}

#[test]
fn timestamptz_io_differential_vs_pg18_gmt_iso() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    let cases = [
        ("2001-02-03 04:05:06.789", "2001-02-03 04:05:06.789+00"),
        ("1997-12-17 07:37:16-08", "1997-12-17 15:37:16+00"),
        ("2001-01-01 00:00:00 GMT", "2001-01-01 00:00:00+00"),
        ("epoch", "1970-01-01 00:00:00+00"),
        ("infinity", "infinity"),
        ("1997-06-10 18:32:01 +05:30", "1997-06-10 13:02:01+00"),
        ("4714-11-24 00:00:00+00 BC", "4714-11-24 00:00:00+00 BC"),
        ("294276-12-31 23:59:59.999999+00", "294276-12-31 23:59:59.999999+00"),
    ];
    for (input, expected) in cases {
        assert_eq!(tstz_out(tstz_in(input)), expected, "timestamptz_in({input:?})");
    }
}

#[test]
fn timestamp_io_differential_vs_pg18_other_styles() {
    gmt_session();

    set_date_style(USE_POSTGRES_DATES);
    adt_datetime::set_date_order(DATEORDER_MDY);
    assert_eq!(ts_out(ts_in("2001-02-03 04:05:06.789")), "Sat Feb 03 04:05:06.789 2001");
    assert_eq!(
        tstz_out(tstz_in("2001-02-03 04:05:06.789")),
        "Sat Feb 03 04:05:06.789 2001 GMT"
    );
    assert_eq!(tstz_out(tstz_in("1997-12-17 07:37:16-08")), "Wed Dec 17 15:37:16 1997 GMT");

    set_date_style(USE_SQL_DATES);
    adt_datetime::set_date_order(DATEORDER_DMY);
    assert_eq!(ts_out(ts_in("2001-02-03 04:05:06.789")), "03/02/2001 04:05:06.789");
    assert_eq!(tstz_out(tstz_in("03/02/2001 04:05:06.789")), "03/02/2001 04:05:06.789 GMT");

    set_date_style(USE_GERMAN_DATES);
    assert_eq!(ts_out(ts_in("2001-02-03 04:05:06.789")), "03.02.2001 04:05:06.789");

    set_date_style(USE_ISO_DATES);
    adt_datetime::set_date_order(DATEORDER_MDY);
}

#[test]
fn timestamp_in_errors_match_c_surface() {
    gmt_session();
    // (unknown text fields like "junk" try pg_tzset in the live tz engine and
    // decode as DTERR_BAD_FORMAT; a time-only string exercises the same
    // BAD_FORMAT surface here)
    let err = timestamp_in("17:32:01", -1, None).unwrap_err();
    assert_eq!(err.message, "invalid input syntax for type timestamp: \"17:32:01\"");

    let err = timestamp_in("1997-02-30 10:00:00", -1, None).unwrap_err();
    assert_eq!(err.message, "date/time field value out of range: \"1997-02-30 10:00:00\"");

    let err = timestamp_in("295000-01-01 00:00:00", -1, None).unwrap_err();
    assert_eq!(err.message, "timestamp out of range: \"295000-01-01 00:00:00\"");
    assert_eq!(err.sqlstate, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);

    let err = timestamp_in("4714-11-23 23:59:59 BC", -1, None).unwrap_err();
    assert_eq!(err.message, "timestamp out of range: \"4714-11-23 23:59:59 BC\"");

    let mut soft = SoftErrorContext::new(true);
    let v = timestamp_in("17:32:01", -1, Some(&mut soft)).unwrap();
    assert_eq!(v, 0);
    assert!(soft.error_occurred());
}

#[test]
fn timestamp_out_range_error() {
    let mut buf = [0u8; MAXDATELEN + 1];
    // Finite but before the Julian-representable range: timestamp2tm fails.
    let err = timestamp_out(i64::MIN + 1, &mut buf).unwrap_err();
    assert_eq!(err.message, "timestamp out of range");
    assert_eq!(err.sqlstate, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
}

#[test]
fn get_current_timestamp_tracks_system_clock() {
    let sys = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
        - 946_684_800_000_000;
    let ts = GetCurrentTimestamp();
    assert!((ts - sys).abs() < 5_000_000, "ts={ts} sys={sys}");
}

#[test]
fn seam_installed_and_live() {
    gmt_session();
    let a = timestamp_seams::get_current_timestamp::call();
    let b = GetCurrentTimestamp();
    assert!(b >= a && b - a < 5_000_000);
}

#[test]
fn timestamp_differences_match_c() {
    assert_eq!(TimestampDifference(10_000_000, 5_000_000), (0, 0));
    assert_eq!(TimestampDifference(0, 3_500_001), (3, 500_001));

    assert_eq!(TimestampDifferenceMilliseconds(5, 5), 0);
    assert_eq!(TimestampDifferenceMilliseconds(0, 1), 1);
    assert_eq!(TimestampDifferenceMilliseconds(0, 1000), 1);
    assert_eq!(TimestampDifferenceMilliseconds(0, 1001), 2);
    assert_eq!(TimestampDifferenceMilliseconds(DT_NOBEGIN, DT_NOEND), i32::MAX as i64);
    assert_eq!(TimestampDifferenceMilliseconds(0, DT_NOEND), i32::MAX as i64);

    assert!(TimestampDifferenceExceeds(0, 2_000_000, 2000));
    assert!(!TimestampDifferenceExceeds(0, 1_999_999, 2000));
    assert!(TimestampDifferenceExceedsSeconds(0, 3_000_000, 3));
    assert!(!TimestampDifferenceExceedsSeconds(0, 2_999_999, 3));
}

#[test]
#[should_panic(expected = "session_timezone not initialized")]
fn timestamptz_out_without_session_zone_panics() {
    let mut buf = [0u8; MAXDATELEN + 1];
    let _ = timestamptz_out(0, &mut buf);
}

#[test]
fn get_sql_current_timestamp_rounds_by_typmod() {
    gmt_session();
    let full = GetSQLCurrentTimestamp(-1);
    let mut expect = full;
    AdjustTimestampForTypmod(&mut expect, 0, None).unwrap();
    assert_eq!(GetSQLCurrentTimestamp(0), expect);
    assert_eq!(GetSQLCurrentTimestamp(0) % 1_000_000, 0);
}

// Zone/extract/trunc/make goldens below are live psql output from PostgreSQL
// 18.3 (Homebrew, tzdata current), captured 2026-07-03.

#[test]
fn timestamp_zone_matches_pg18() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    // the "Default" timezone_abbreviations entries these cases need: EST is
    // a fixed offset, MSK a dynamic abbreviation over Europe/Moscow
    tz::InstallTimeZoneAbbrevs(tz::ConvertTimeZoneAbbrevs(&[
        tz::TzEntry { abbrev: b"est", zone: None, offset: -18000, is_dst: false },
        tz::TzEntry { abbrev: b"msk", zone: Some(b"Europe/Moscow"), offset: 0, is_dst: false },
    ]));
    // 2025-03-09 02:30 America/New_York is inside the spring-forward gap:
    // DetermineTimeZoneOffset prefers the before-interpretation (EST).
    // 2025-11-02 01:30 is ambiguous: fall-back prefers the after (EST).
    let cases: [(&[u8], &str, &str); 5] = [
        (b"America/New_York", "2025-03-09 02:30:00", "2025-03-09 07:30:00+00"),
        (b"America/New_York", "2025-11-02 01:30:00", "2025-11-02 06:30:00+00"),
        (b"Asia/Kolkata", "2025-06-01 12:00:00", "2025-06-01 06:30:00+00"),
        (b"EST", "2025-06-01 12:00:00", "2025-06-01 17:00:00+00"),
        (b"MSK", "2014-01-01 12:00:00", "2014-01-01 08:00:00+00"),
    ];
    for (zone, input, expect) in cases {
        let r = timestamp_zone(zone, ts_in(input)).unwrap();
        assert_eq!(tstz_out(r), expect, "timestamp_zone({:?}, {input})", String::from_utf8_lossy(zone));
    }

    assert_eq!(timestamp_zone(b"America/New_York", DT_NOEND).unwrap(), DT_NOEND);

    let err = timestamp_zone(b"Nowhere/Land", 0).unwrap_err();
    assert_eq!(err.message, "time zone \"Nowhere/Land\" not recognized");
    assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn timestamptz_zone_matches_pg18() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    let cases: [(&[u8], &str, &str); 4] = [
        (b"America/New_York", "2025-03-09 07:30:00+00", "2025-03-09 03:30:00"),
        (b"America/New_York", "2025-11-02 05:30:00+00", "2025-11-02 01:30:00"),
        (b"America/New_York", "2025-11-02 06:30:00+00", "2025-11-02 01:30:00"),
        (b"Asia/Kolkata", "2025-06-01 12:00:00+00", "2025-06-01 17:30:00"),
    ];
    for (zone, input, expect) in cases {
        let r = timestamptz_zone(zone, tstz_in(input)).unwrap();
        assert_eq!(tstz_out(tstz_in(input)), input, "sanity");
        assert_eq!(ts_out(r), expect, "timestamptz_zone({:?}, {input})", String::from_utf8_lossy(zone));
    }
}

fn part_num(v: &PartValue) -> String {
    match v {
        PartValue::Numeric(img) => {
            let mut out = Vec::new();
            numeric::numeric_out_into(img.num(), &mut out);
            String::from_utf8(out).unwrap()
        }
        _ => panic!("expected numeric part value"),
    }
}

fn part_f64(v: &PartValue) -> f64 {
    match v {
        PartValue::Float(f) => *f,
        _ => panic!("expected float part value"),
    }
}

#[test]
fn extract_timezone_fields_match_pg18() {
    zone_session(b"America/New_York");
    let ts = tstz_in("2025-06-01 12:00:00+00");
    assert_eq!(part_f64(&timestamptz_part_common(b"timezone", ts, false).unwrap()), -14400.0);
    assert_eq!(part_f64(&timestamptz_part_common(b"timezone_hour", ts, false).unwrap()), -4.0);
    assert_eq!(part_f64(&timestamptz_part_common(b"timezone_minute", ts, false).unwrap()), 0.0);
    let winter = tstz_in("2025-01-01 12:00:00+00");
    assert_eq!(part_num(&timestamptz_part_common(b"timezone", winter, true).unwrap()), "-18000");

    zone_session(b"Asia/Kolkata");
    let ts = tstz_in("2025-06-01 12:00:00+00");
    assert_eq!(part_f64(&timestamptz_part_common(b"timezone", ts, false).unwrap()), 19800.0);
    assert_eq!(part_num(&timestamptz_part_common(b"timezone_hour", ts, true).unwrap()), "5");
    assert_eq!(part_num(&timestamptz_part_common(b"timezone_minute", ts, true).unwrap()), "30");

    // timezone fields are timestamptz-only
    let err = timestamp_part_common(b"timezone", 0, true).unwrap_err();
    assert_eq!(
        err.message,
        "unit \"timezone\" not supported for type timestamp without time zone"
    );
    assert_eq!(err.sqlstate, ERRCODE_FEATURE_NOT_SUPPORTED);
}

#[test]
fn extract_fields_match_pg18() {
    gmt_session();
    set_date_style(USE_ISO_DATES);

    let ts = ts_in("2025-03-09 07:30:00");
    assert_eq!(part_num(&timestamp_part_common(b"epoch", ts, true).unwrap()), "1741505400.000000");
    assert_eq!(part_f64(&timestamp_part_common(b"epoch", ts, false).unwrap()), 1741505400.0);
    assert_eq!(
        part_num(&timestamptz_part_common(b"epoch", tstz_in("2025-03-09 07:30:00+00"), true).unwrap()),
        "1741505400.000000"
    );

    assert_eq!(
        part_num(&timestamp_part_common(b"julian", ts_in("2025-03-09 07:30:00.5"), true).unwrap()),
        "2460744.31250578703703703704"
    );

    let t = ts_in("2001-02-16 20:38:40.5");
    assert_eq!(part_num(&timestamp_part_common(b"second", t, true).unwrap()), "40.500000");
    assert_eq!(part_num(&timestamp_part_common(b"milliseconds", t, true).unwrap()), "40500.000");
    assert_eq!(part_f64(&timestamp_part_common(b"dow", ts, false).unwrap()), 0.0);
    assert_eq!(part_f64(&timestamp_part_common(b"week", ts_in("2005-01-01 12:00:00"), false).unwrap()), 53.0);
    assert_eq!(
        part_f64(&timestamp_part_common(b"isoyear", ts_in("0001-01-01 00:00:00 BC"), false).unwrap()),
        -2.0
    );

    assert_eq!(part_num(&timestamp_part_common(b"epoch", DT_NOEND, true).unwrap()), "Infinity");
    assert_eq!(part_num(&timestamp_part_common(b"epoch", DT_NOBEGIN, true).unwrap()), "-Infinity");
    assert_eq!(part_f64(&timestamp_part_common(b"epoch", DT_NOEND, false).unwrap()), f64::INFINITY);
    assert!(matches!(timestamp_part_common(b"dow", DT_NOEND, true), Ok(PartValue::Null)));

    let err = timestamp_part_common(b"gibberish", 0, true).unwrap_err();
    assert_eq!(
        err.message,
        "unit \"gibberish\" not recognized for type timestamp without time zone"
    );
    assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
    let err = timestamptz_part_common(b"gibberish", DT_NOEND, false).unwrap_err();
    assert_eq!(
        err.message,
        "unit \"gibberish\" not recognized for type timestamp with time zone"
    );
}

#[test]
fn date_trunc_matches_pg18() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    for (unit, input, expect) in [
        ("week", "2025-03-09 07:30:00", "2025-03-09 00:00:00"),
        ("quarter", "2025-08-15 07:30:00", "2025-07-01 00:00:00"),
        ("millennium", "2025-08-15 07:30:00", "2001-01-01 00:00:00"),
        ("decade", "2025-08-15 07:30:00", "2020-01-01 00:00:00"),
        ("hour", "2025-08-15 07:30:59.5", "2025-08-15 07:00:00"),
        ("milliseconds", "2025-08-15 07:30:59.5009", "2025-08-15 07:30:59.5"),
    ] {
        let got = timestamp_trunc(unit.as_bytes(), ts_in(input)).unwrap();
        let want = if unit == "week" {
            // 2025-03-09 is a Sunday; ISO week starts Monday 2025-03-03
            ts_in("2025-03-03 00:00:00")
        } else {
            ts_in(expect)
        };
        assert_eq!(ts_out(got), ts_out(want), "date_trunc({unit}, {input})");
    }
    assert_eq!(timestamp_trunc(b"week", DT_NOEND).unwrap(), DT_NOEND);
    let err = timestamp_trunc(b"timezone", DT_NOEND).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_FEATURE_NOT_SUPPORTED);

    zone_session(b"America/New_York");
    let got = timestamptz_trunc(b"week", tstz_in("2025-03-10 01:30:00-04")).unwrap();
    assert_eq!(tstz_out(got), "2025-03-10 00:00:00-04");
    // day-truncating back across the spring-forward boundary redoes the tz
    let got = timestamptz_trunc(b"day", tstz_in("2025-03-09 23:30:00-04")).unwrap();
    assert_eq!(tstz_out(got), "2025-03-09 00:00:00-05");

    tz::set_session_timezone(tz::pg_tzset(b"GMT"));
    let got =
        timestamptz_trunc_zone(b"day", tstz_in("2025-03-10 03:30:00+00"), b"America/New_York")
            .unwrap();
    assert_eq!(tstz_out(got), "2025-03-09 05:00:00+00");
}

#[test]
fn make_timestamp_family_matches_pg18() {
    zone_session(b"America/New_York");
    set_date_style(USE_ISO_DATES);

    assert_eq!(
        make_timestamp(2025, 3, 9, 2, 30, 0.0).unwrap(),
        ts_in("2025-03-09 02:30:00")
    );
    assert_eq!(
        tstz_out(make_timestamptz(2025, 3, 9, 2, 30, 0.0).unwrap()),
        "2025-03-09 03:30:00-04"
    );
    assert_eq!(
        tstz_out(make_timestamptz_at_timezone(2025, 3, 9, 2, 30, 0.0, b"America/New_York").unwrap()),
        "2025-03-09 03:30:00-04"
    );
    assert_eq!(
        tstz_out(make_timestamptz_at_timezone(2025, 11, 2, 1, 30, 0.0, b"America/New_York").unwrap()),
        "2025-11-02 01:30:00-05"
    );
    assert_eq!(
        tstz_out(make_timestamptz_at_timezone(2025, 6, 1, 12, 0, 0.5, b"+05:30").unwrap()),
        "2025-06-01 02:30:00.5-04"
    );

    let err = make_timestamptz_at_timezone(2025, 6, 1, 12, 0, 0.0, b"5").unwrap_err();
    assert_eq!(err.message, "invalid input syntax for type numeric time zone: \"5\"");
    assert_eq!(
        err.hint.as_deref(),
        Some("Numeric time zones must have \"-\" or \"+\" as first character.")
    );
    let err = make_timestamptz_at_timezone(2025, 6, 1, 12, 0, 0.0, b"+16:00").unwrap_err();
    assert_eq!(err.message, "numeric time zone \"+16:00\" out of range");
    let err = make_timestamp(2025, 13, 1, 0, 0, 0.0).unwrap_err();
    assert_eq!(err.message, "date field value out of range: 2025-13-01");
    let err = make_timestamp(2025, 6, 1, 25, 0, 0.0).unwrap_err();
    assert_eq!(err.message, "time field value out of range: 25:00:00");
}

#[test]
fn get_current_time_usec_memoizes_per_ts_and_zone() {
    gmt_session();
    let mut tm = pg_tm::default();
    let mut fsec = 0;
    let mut tzv = 1234;
    GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tzv)).unwrap();
    // test-thread xact start timestamp is 0 = 2000-01-01 00:00:00 UTC
    assert_eq!((tm.tm_year, tm.tm_mon, tm.tm_mday), (2000, 1, 1));
    assert_eq!((tm.tm_hour, tm.tm_min, tm.tm_sec, fsec, tzv), (0, 0, 0, 0, 0));

    let cached = CURRENT_TM_CACHE.with(core::cell::Cell::get).expect("memo populated");
    assert_eq!(cached.ts, 0);

    let mut tm2 = pg_tm::default();
    GetCurrentDateTime(&mut tm2).unwrap();
    assert_eq!(tm2, tm);
    let still = CURRENT_TM_CACHE.with(core::cell::Cell::get).unwrap();
    assert!(core::ptr::eq(still.zone, cached.zone), "second call reused the memo entry");

    // timezone change invalidates the memo
    let kolkata = zone_session(b"Asia/Kolkata");
    let mut tzv2 = 0;
    GetCurrentTimeUsec(&mut tm, &mut fsec, Some(&mut tzv2)).unwrap();
    assert_eq!(tzv2, -19800);
    assert_eq!((tm.tm_hour, tm.tm_min), (5, 30));
    assert!(core::ptr::eq(
        CURRENT_TM_CACHE.with(core::cell::Cell::get).unwrap().zone,
        kolkata
    ));

    // seam side stays in sync
    let snap = timestamp_seams::get_current_time_usec::call().unwrap();
    assert_eq!((snap.tm_hour, snap.tm_min, snap.tz), (5, 30, -19800));
}

#[test]
fn conversions_round_trip_under_session_zone() {
    zone_session(b"America/New_York");
    set_date_style(USE_ISO_DATES);
    let ts = ts_in("2025-03-09 02:30:00");
    // spring-forward gap resolves to the before-interpretation (EST)
    assert_eq!(tstz_out(timestamp2timestamptz(ts).unwrap()), "2025-03-09 03:30:00-04");
    let tstz = tstz_in("2025-06-01 12:00:00+00");
    assert_eq!(ts_out(timestamptz2timestamp(tstz).unwrap()), "2025-06-01 08:00:00");
    assert_eq!(timestamp2timestamptz(DT_NOBEGIN).unwrap(), DT_NOBEGIN);
    assert_eq!(timestamp_cmp_timestamptz_internal(ts, timestamp2timestamptz(ts).unwrap()), 0);

    let local = GetSQLLocalTimestamp(-1).unwrap();
    assert_eq!(local, timestamptz2timestamp(xact::GetCurrentTransactionStartTimestamp()).unwrap());
}

#[test]
fn timeofday_formats_like_c() {
    zone_session(b"Asia/Kolkata");
    let mut buf = [0u8; 128];
    let len = timeofday_into(&mut buf);
    let s = core::str::from_utf8(&buf[..len]).unwrap();
    assert!(s.ends_with(" IST"), "{s}");
    let dot = s.find('.').unwrap();
    // "Thu Jul 03 12:34:56.123456 2026 IST" shape: 6 usec digits after the dot
    assert!(s.as_bytes()[dot + 1..dot + 7].iter().all(u8::is_ascii_digit), "{s}");
    assert_eq!(&s[3..4], " ");
}

use adt_datetime::Interval;
use crate::interval::{
    interval_cmp_internal, interval_in, interval_justify_days, interval_justify_hours,
    interval_justify_interval, interval_mi, interval_out, interval_part_common, interval_pl,
    interval_trunc, interval_um, intervaltypmodin, intervaltypmodout, make_interval,
    timestamp_age, timestamp_izone, timestamp_mi, timestamp_mi_interval, timestamp_pl_interval,
    timestamptz_pl_interval,
};

fn iv_in(s: &str) -> Interval {
    interval_in(s, -1, None).unwrap()
}

fn iv_out(iv: &Interval) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = interval_out(iv, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

#[test]
fn interval_io_round_trips() {
    gmt_session();
    for (input, expect) in [
        ("1 year 2 mons 3 days 04:05:06.789", "1 year 2 mons 3 days 04:05:06.789"),
        ("-1 day +5 hours", "-1 days +05:00:00"),
        ("1-2", "1 year 2 mons"),
        ("@ 1 day ago", "-1 days"),
        ("P1Y2M3DT4H5M6.7S", "1 year 2 mons 3 days 04:05:06.7"),
        ("PT0S", "00:00:00"),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
        ("00:00", "00:00:00"),
    ] {
        assert_eq!(iv_out(&iv_in(input)), expect, "input {input}");
    }

    let err = interval_in("bogus", -1, None).unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_INVALID_DATETIME_FORMAT);
    let err = interval_in("100000000000000 years", -1, None).unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_INTERVAL_FIELD_OVERFLOW);
}

#[test]
fn interval_typmod_rounds_and_truncates_like_c() {
    gmt_session();
    use adt_datetime::{INTERVAL_MASK, MINUTE};
    use crate::interval::INTERVAL_TYPMOD;
    // INTERVAL MINUTE truncation
    let tmod = intervaltypmodin(&[INTERVAL_MASK(MINUTE)]).unwrap();
    let iv = interval_in("1 day 02:03:04.5", tmod, None).unwrap();
    assert_eq!(iv_out(&iv), "1 day 02:03:00");
    // precision rounding: interval(2)
    let tmod = intervaltypmodin(&[adt_datetime::INTERVAL_FULL_RANGE, 2]).unwrap();
    assert_eq!(tmod, INTERVAL_TYPMOD(2, adt_datetime::INTERVAL_FULL_RANGE));
    let iv = interval_in("00:00:00.789", tmod, None).unwrap();
    assert_eq!(iv_out(&iv), "00:00:00.79");
    // typmodout round trip
    let mut buf = [0u8; 64];
    let n = intervaltypmodout(tmod, &mut buf).unwrap();
    assert_eq!(core::str::from_utf8(&buf[..n]).unwrap(), "(2)");
    let tmod = intervaltypmodin(&[INTERVAL_MASK(MINUTE)]).unwrap();
    let n = intervaltypmodout(tmod, &mut buf).unwrap();
    assert_eq!(core::str::from_utf8(&buf[..n]).unwrap(), " minute");
    // negative precision
    let err = intervaltypmodin(&[adt_datetime::INTERVAL_FULL_RANGE, -3]).unwrap_err();
    assert!(err.message.contains("INTERVAL(-3) precision must not be negative"));
    // bogus mask
    let err = intervaltypmodin(&[1000]).unwrap_err();
    assert!(err.message.contains("invalid INTERVAL type modifier"));
}

#[test]
fn interval_typmod_invalid_range_is_clean_internal_error() {
    use crate::interval::{intervaltypmodleastfield, intervaltypmodout};
    let mut buf = [0u8; 64];
    let err = intervaltypmodout(1, &mut buf).unwrap_err();
    assert_eq!(err.message, "invalid INTERVAL typmod: 0x1");
    assert_eq!(err.sqlstate, types_error::ERRCODE_INTERNAL_ERROR);
    let err = intervaltypmodleastfield(0x10000).unwrap_err();
    assert_eq!(err.message, "invalid INTERVAL typmod: 0x10000");
    assert_eq!(err.sqlstate, types_error::ERRCODE_INTERNAL_ERROR);
}

#[test]
fn interval_agg_family_matches_c() {
    use crate::interval::{
        do_interval_accum, do_interval_discard, interval_agg_combine, interval_avg_final,
        interval_sum_final, IntervalAggState,
    };
    gmt_session();
    let mut st = IntervalAggState::default();
    assert_eq!(interval_avg_final(&st).unwrap(), None);
    assert_eq!(interval_sum_final(&st).unwrap(), None);
    for s in ["1 year", "2 years", "6 years 3 days"] {
        do_interval_accum(&mut st, &iv_in(s)).unwrap();
    }
    assert_eq!(iv_out(&interval_avg_final(&st).unwrap().unwrap()), "3 years 1 day");
    assert_eq!(iv_out(&interval_sum_final(&st).unwrap().unwrap()), "9 years 3 days");
    do_interval_discard(&mut st, &iv_in("6 years 3 days")).unwrap();
    assert_eq!(iv_out(&interval_avg_final(&st).unwrap().unwrap()), "1 year 6 mons");
    do_interval_accum(&mut st, &iv_in("infinity")).unwrap();
    assert_eq!(iv_out(&interval_avg_final(&st).unwrap().unwrap()), "infinity");
    assert_eq!(iv_out(&interval_sum_final(&st).unwrap().unwrap()), "infinity");
    do_interval_accum(&mut st, &iv_in("-infinity")).unwrap();
    assert!(interval_avg_final(&st).unwrap_err().message.contains("interval out of range"));
    assert!(interval_sum_final(&st).unwrap_err().message.contains("interval out of range"));

    let mut a = IntervalAggState::default();
    do_interval_accum(&mut a, &iv_in("1 day")).unwrap();
    let mut b = IntervalAggState::default();
    do_interval_accum(&mut b, &iv_in("3 days")).unwrap();
    interval_agg_combine(&mut a, &b).unwrap();
    assert_eq!(iv_out(&interval_sum_final(&a).unwrap().unwrap()), "4 days");
    assert_eq!(iv_out(&interval_avg_final(&a).unwrap().unwrap()), "2 days");
}

#[test]
fn interval_typmod_least_field_matches_c() {
    use adt_datetime::{DAY, HOUR, INTERVAL_MASK, MINUTE, MONTH, SECOND, YEAR};
    use crate::interval::{intervaltypmodleastfield, INTERVAL_FULL_PRECISION, INTERVAL_TYPMOD};
    let lf =
        |r: i32| intervaltypmodleastfield(INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, r)).unwrap();
    assert_eq!(intervaltypmodleastfield(-1).unwrap(), 0);
    assert_eq!(lf(INTERVAL_MASK(YEAR)), 5);
    assert_eq!(lf(INTERVAL_MASK(MONTH)), 4);
    assert_eq!(lf(INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)), 4);
    assert_eq!(lf(INTERVAL_MASK(DAY)), 3);
    assert_eq!(lf(INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)), 2);
    assert_eq!(lf(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)), 1);
    assert_eq!(lf(INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)), 0);
    assert_eq!(lf(INTERVAL_MASK(SECOND)), 0);
    assert_eq!(lf(adt_datetime::INTERVAL_FULL_RANGE), 0);
}

#[test]
fn make_interval_secs_overflow_is_22003() {
    let err = make_interval(0, 0, 0, 0, 0, 0, 1e308).unwrap_err();
    assert_eq!(err.message, "value out of range: overflow");
    let err = make_interval(0, 0, 0, 0, 0, 0, 1e18).unwrap_err();
    assert_eq!(err.message, "interval out of range");
}

#[test]
fn anytimestamp_typmod_out_shapes() {
    let mut buf = [0u8; 64];
    let n = crate::builtins::typmod_paren_suffix_out(2, b" with time zone", &mut buf);
    assert_eq!(core::str::from_utf8(&buf[..n]).unwrap(), "(2) with time zone");
    let n = crate::builtins::typmod_paren_suffix_out(-1, b" without time zone", &mut buf);
    assert_eq!(core::str::from_utf8(&buf[..n]).unwrap(), " without time zone");
    let n = crate::builtins::typmod_paren_suffix_out(0, b" without time zone", &mut buf);
    assert_eq!(core::str::from_utf8(&buf[..n]).unwrap(), "(0) without time zone");
}

#[test]
fn interval_arithmetic_matches_c() {
    gmt_session();
    let a = iv_in("1 mon 5 days 03:00:00");
    let b = iv_in("2 mons -1 day 01:30:00");
    assert_eq!(iv_out(&interval_pl(&a, &b).unwrap()), "3 mons 4 days 04:30:00");
    assert_eq!(iv_out(&interval_mi(&a, &b).unwrap()), "-1 mons +6 days 01:30:00");
    assert_eq!(iv_out(&interval_um(&a).unwrap()), "-1 mons -5 days -03:00:00");

    assert!(interval_pl(&Interval::NOBEGIN, &Interval::NOEND).is_err());
    assert_eq!(interval_mi(&a, &Interval::NOBEGIN).unwrap(), Interval::NOEND);

    assert_eq!(interval_cmp_internal(&iv_in("30 days"), &iv_in("1 mon")), 0);
    assert_eq!(interval_cmp_internal(&iv_in("24 hours"), &iv_in("1 day")), 0);
    assert_eq!(interval_cmp_internal(&iv_in("25 hours"), &iv_in("1 day")), 1);
    assert!(interval_cmp_internal(&Interval::NOBEGIN, &iv_in("0")) < 0);

    let j = interval_justify_hours(&iv_in("27 hours")).unwrap();
    assert_eq!(iv_out(&j), "1 day 03:00:00");
    let j = interval_justify_days(&iv_in("35 days")).unwrap();
    assert_eq!(iv_out(&j), "1 mon 5 days");
    let j = interval_justify_interval(&iv_in("29 days 25 hours")).unwrap();
    assert_eq!(iv_out(&j), "1 mon 1:00:00".replace("1:", "01:"));

    let mk = make_interval(1, 2, 1, 2, 3, 4, 5.5).unwrap();
    assert_eq!(iv_out(&mk), "1 year 2 mons 9 days 03:04:05.5");
    assert!(make_interval(0, 0, 0, 0, 0, 0, f64::INFINITY).is_err());
}

#[test]
fn timestamp_interval_arithmetic_and_dst() {
    zone_session(b"America/New_York");
    // DST spring forward 2025-03-09: +1 day keeps wall clock, +24 hours shifts
    let tstz = tstz_in("2025-03-08 12:00:00");
    let one_day = iv_in("1 day");
    let day_24h = iv_in("24 hours");
    assert_eq!(tstz_out(timestamptz_pl_interval(tstz, &one_day).unwrap()), "2025-03-09 12:00:00-04");
    assert_eq!(tstz_out(timestamptz_pl_interval(tstz, &day_24h).unwrap()), "2025-03-09 13:00:00-04");

    let ts = ts_in("2025-01-31 10:00:00");
    assert_eq!(ts_out(timestamp_pl_interval(ts, &iv_in("1 mon")).unwrap()), "2025-02-28 10:00:00");
    assert_eq!(
        ts_out(timestamp_mi_interval(ts, &iv_in("1 mon")).unwrap()),
        "2024-12-31 10:00:00"
    );

    let d = timestamp_mi(ts_in("2025-01-02 03:00:00"), ts_in("2025-01-01 00:00:00")).unwrap();
    assert_eq!(iv_out(&d), "1 day 03:00:00");

    let age = timestamp_age(ts_in("2025-03-15 10:30:00"), ts_in("2024-01-20 08:00:00")).unwrap();
    assert_eq!(iv_out(&age), "1 year 1 mon 26 days 02:30:00");

    let z = timestamp_izone(&iv_in("-5 hours"), ts_in("2025-06-01 12:00:00")).unwrap();
    assert_eq!(tstz_out(z), "2025-06-01 13:00:00-04");
    assert!(timestamp_izone(&iv_in("1 mon"), ts_in("2025-06-01 12:00:00")).is_err());
}

#[test]
fn interval_part_and_trunc_match_c() {
    gmt_session();
    let iv = iv_in("2 years 5 mons 10 days 12:30:45.678");
    let f = |unit: &str| match interval_part_common(unit.as_bytes(), &iv, false).unwrap() {
        PartValue::Float(v) => v,
        other => panic!("{other:?}"),
    };
    assert_eq!(f("year"), 2.0);
    assert_eq!(f("month"), 5.0);
    assert_eq!(f("day"), 10.0);
    assert_eq!(f("hour"), 12.0);
    assert_eq!(f("minute"), 30.0);
    assert_eq!(f("second"), 45.678);
    assert_eq!(f("quarter"), 2.0);
    assert_eq!(f("epoch"), 76984245.678);

    let neg = iv_in("-4 mons");
    let q = match interval_part_common(b"quarter", &neg, false).unwrap() {
        PartValue::Float(v) => v,
        other => panic!("{other:?}"),
    };
    assert_eq!(q, -2.0);

    let err = interval_part_common(b"timezone", &iv, false).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_FEATURE_NOT_SUPPORTED);
    let err = interval_part_common(b"bogus", &iv, false).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);

    assert_eq!(iv_out(&interval_trunc(b"hour", &iv).unwrap()), "2 years 5 mons 10 days 12:00:00");
    assert_eq!(iv_out(&interval_trunc(b"month", &iv).unwrap()), "2 years 5 mons");
    let err = interval_trunc(b"week", &iv).unwrap_err();
    assert!(err.detail.as_deref() == Some("Months usually have fractional weeks."), "{err:?}");
    assert_eq!(interval_trunc(b"day", &Interval::NOEND).unwrap(), Interval::NOEND);
}

// date_bin rows diffed vs live C 18.3 (psql, 2026-07-03).
#[test]
fn date_bin_rows() {
    use crate::interval::{interval_in, timestamp_bin};
    gmt_session();
    let iv = |s: &str| interval_in(s, -1, None).unwrap();
    let bin = |st: &str, ts: &str, org: &str| {
        ts_out(timestamp_bin(&iv(st), ts_in(ts), ts_in(org)).unwrap())
    };
    assert_eq!(
        bin("15 minutes", "2020-02-11 15:44:17", "2001-01-01"),
        "2020-02-11 15:30:00"
    );
    assert_eq!(
        bin("1 day", "2020-02-11 15:44:17", "2001-01-01 01:00:00"),
        "2020-02-11 01:00:00"
    );
    assert_eq!(bin("5 min", "1999-12-31 23:59:59", "2000-01-01"), "1999-12-31 23:55:00");
    assert_eq!(
        timestamp_bin(&iv("2 hours"), ts_in("infinity"), ts_in("2001-01-01")).unwrap(),
        DT_NOEND
    );
    assert_eq!(
        timestamp_bin(&iv("0 min"), ts_in("2020-01-01"), ts_in("2001-01-01"))
            .unwrap_err()
            .to_string(),
        "stride must be greater than zero"
    );
    assert_eq!(
        timestamp_bin(&iv("1 month"), ts_in("2020-01-01"), ts_in("2001-01-01"))
            .unwrap_err()
            .to_string(),
        "timestamps cannot be binned into intervals containing months or years"
    );
    assert_eq!(
        timestamp_bin(&iv("2 hours"), ts_in("2020-01-01"), ts_in("infinity"))
            .unwrap_err()
            .to_string(),
        "origin out of range"
    );
    assert_eq!(
        timestamp_bin(&Interval::NOEND, ts_in("2020-01-01"), ts_in("2001-01-01"))
            .unwrap_err()
            .to_string(),
        "timestamps cannot be binned into infinite intervals"
    );
}

#[test]
fn float8_timestamptz_vectors() {
    assert_eq!(float8_timestamptz(0.0).unwrap(), -946_684_800_000_000);
    assert_eq!(float8_timestamptz(946_684_800.0).unwrap(), 0);
    assert_eq!(
        float8_timestamptz(1_262_349_296.789_012_3).unwrap(),
        315_664_496_789_012
    );
    assert_eq!(float8_timestamptz(f64::INFINITY).unwrap(), DT_NOEND);
    assert_eq!(float8_timestamptz(f64::NEG_INFINITY).unwrap(), DT_NOBEGIN);

    let err = float8_timestamptz(f64::NAN).unwrap_err();
    assert_eq!(err.message(), "timestamp cannot be NaN");
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);

    let err = float8_timestamptz(1e18).unwrap_err();
    assert_eq!(err.message(), "timestamp out of range: \"1e+18\"");
    let err = float8_timestamptz(-1e18).unwrap_err();
    assert_eq!(err.message(), "timestamp out of range: \"-1e+18\"");
    // passes the pre-check but overflows IS_VALID_TIMESTAMP after rounding
    let err = float8_timestamptz(9224318015999999.0).unwrap_err();
    assert_eq!(err.message(), "timestamp out of range: \"9.22432e+15\"");
}
