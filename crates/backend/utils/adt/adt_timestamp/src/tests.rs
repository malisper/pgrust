use super::*;
use adt_datetime::{
    set_date_style, DATEORDER_DMY, DATEORDER_MDY, USE_GERMAN_DATES, USE_ISO_DATES,
    USE_POSTGRES_DATES, USE_SQL_DATES,
};

fn gmt_session() {
    tz::pg_timezone_initialize();
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
    // (unknown text fields like "junk" route through pg_tzset and stay behind
    // adt_datetime's unported-tz panic lock; BAD_FORMAT is reachable via a
    // time-only string)
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
    init_seams();
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
