#![allow(non_snake_case)]

use adt_datetime::*;

const TS_BUFLEN: usize = MAXDATELEN + MAXDATEFIELDS;

#[derive(Debug)]
struct Parsed<'w> {
    field: [&'w [u8]; MAXDATEFIELDS],
    ftype: [i32; MAXDATEFIELDS],
    nf: usize,
}

fn parse<'w>(input: &str, workbuf: &'w mut [u8]) -> Result<Parsed<'w>, i32> {
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;
    let rc = ParseDateTime(
        input.as_bytes(),
        workbuf,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS,
        &mut nf,
    );
    if rc != 0 {
        return Err(rc);
    }
    Ok(Parsed { field, ftype, nf })
}

fn decode_ts(input: &str, with_tz: bool) -> Result<(i32, pg_tm, fsec_t, i32), i32> {
    let mut workbuf = [0u8; TS_BUFLEN];
    let p = parse(input, &mut workbuf)?;
    let mut dtype = 0;
    let mut tm = pg_tm::default();
    let mut fsec = 0;
    let mut tz = 0;
    let mut extra = DateTimeErrorExtra::default();
    let rc = DecodeDateTime(
        &p.field[..p.nf],
        &p.ftype[..p.nf],
        p.nf,
        &mut dtype,
        &mut tm,
        &mut fsec,
        if with_tz { Some(&mut tz) } else { None },
        &mut extra,
    );
    if rc != 0 {
        return Err(rc);
    }
    Ok((dtype, tm, fsec, tz))
}

fn decode_time(input: &str, with_tz: bool) -> Result<(i32, pg_tm, fsec_t, i32), i32> {
    let mut workbuf = [0u8; MAXDATELEN + 1];
    let p = parse(input, &mut workbuf)?;
    let mut p = p;
    let mut dtype = 0;
    let mut tm = pg_tm::default();
    let mut fsec = 0;
    let mut tz = 0;
    let mut extra = DateTimeErrorExtra::default();
    let rc = DecodeTimeOnly(
        &p.field[..p.nf],
        &mut p.ftype[..p.nf],
        p.nf,
        &mut dtype,
        &mut tm,
        &mut fsec,
        if with_tz { Some(&mut tz) } else { None },
        &mut extra,
    );
    if rc != 0 {
        return Err(rc);
    }
    Ok((dtype, tm, fsec, tz))
}

fn ymd(tm: &pg_tm) -> (i32, i32, i32) {
    (tm.tm_year, tm.tm_mon, tm.tm_mday)
}

fn hms(tm: &pg_tm) -> (i32, i32, i32) {
    (tm.tm_hour, tm.tm_min, tm.tm_sec)
}

#[test]
fn token_tables_are_valid_and_complete() {
    assert!(CheckDateTokenTables());
    assert_eq!(DATETKTBL.len(), 72);
    assert_eq!(DELTATKTBL.len(), 61);
    assert_eq!(UNIX_EPOCH_JDATE, date2j(1970, 1, 1));
    assert_eq!(POSTGRES_EPOCH_JDATE, date2j(2000, 1, 1));
}

#[test]
fn datebsearch_matches_and_truncates() {
    let tp = datebsearch(b"epoch", &DATETKTBL).unwrap();
    assert_eq!((tp.typ as i32, tp.value), (RESERV, DTK_EPOCH));
    assert!(datebsearch(b"nosuchtok", &DATETKTBL).is_none());
    let tp = datebsearch(b"microsecond", &DELTATKTBL).unwrap();
    assert_eq!(tp.value, DTK_MICROSEC);
    let first = datebsearch(b"+infinity", &DATETKTBL).unwrap();
    assert_eq!(first.value, DTK_LATE);
    let last = datebsearch(b"yesterday", &DATETKTBL).unwrap();
    assert_eq!(last.value, DTK_YESTERDAY);
}

#[test]
fn decode_units_and_special_use_position_cache() {
    let mut val = 0;
    for _ in 0..2 {
        assert_eq!(DecodeUnits(0, b"day", &mut val), UNITS);
        assert_eq!(val, DTK_DAY);
        assert_eq!(DecodeUnits(0, b"month", &mut val), UNITS);
        assert_eq!(val, DTK_MONTH);
        assert_eq!(DecodeSpecial(1, b"jan", &mut val), MONTH);
        assert_eq!(val, 1);
        assert_eq!(DecodeSpecial(1, b"pm", &mut val), AMPM);
        assert_eq!(val, PM);
    }
    assert_eq!(DecodeUnits(0, b"bogus", &mut val), UNKNOWN_FIELD);
}

#[test]
fn parse_datetime_tokenizes_like_c() {
    let mut workbuf = [0u8; TS_BUFLEN];
    let p = parse("2023-06-12 10:11:12.5 +03 America/New_York J2451187", &mut workbuf).unwrap();
    assert_eq!(p.nf, 6);
    let got: Vec<(&[u8], i32)> =
        (0..p.nf).map(|i| (p.field[i], p.ftype[i])).collect();
    assert_eq!(
        got,
        vec![
            (&b"2023-06-12"[..], DTK_DATE),
            (&b"10:11:12.5"[..], DTK_TIME),
            (&b"+03"[..], DTK_TZ),
            (&b"america/new_york"[..], DTK_DATE),
            (&b"j"[..], DTK_STRING),
            (&b"2451187"[..], DTK_NUMBER),
        ]
    );

    let p = parse("20011225T040506.789-07", &mut workbuf).unwrap();
    let got: Vec<(&[u8], i32)> =
        (0..p.nf).map(|i| (p.field[i], p.ftype[i])).collect();
    assert_eq!(
        got,
        vec![
            (&b"20011225"[..], DTK_NUMBER),
            (&b"t"[..], DTK_STRING),
            (&b"040506.789"[..], DTK_NUMBER),
            (&b"-07"[..], DTK_TZ),
        ]
    );

    let p = parse("Jan 8, 1999 04:05 PM", &mut workbuf).unwrap();
    let got: Vec<(&[u8], i32)> =
        (0..p.nf).map(|i| (p.field[i], p.ftype[i])).collect();
    assert_eq!(
        got,
        vec![
            (&b"jan"[..], DTK_STRING),
            (&b"8"[..], DTK_NUMBER),
            (&b"1999"[..], DTK_NUMBER),
            (&b"04:05"[..], DTK_TIME),
            (&b"pm"[..], DTK_STRING),
        ]
    );
}

#[test]
fn parse_datetime_enforces_workbuf_and_field_limits() {
    let mut small = [0u8; 8];
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0;
    let rc = ParseDateTime(
        b"2023-06-12 10:11:12",
        &mut small,
        &mut field,
        &mut ftype,
        MAXDATEFIELDS,
        &mut nf,
    );
    assert_eq!(rc, DTERR_BAD_FORMAT);

    let mut workbuf = [0u8; TS_BUFLEN];
    let many = "1 ".repeat(MAXDATEFIELDS + 1);
    assert_eq!(parse(&many, &mut workbuf).unwrap_err(), DTERR_BAD_FORMAT);

    assert_eq!(parse("2023-06-12 \x01", &mut workbuf).unwrap_err(), DTERR_BAD_FORMAT);
}

#[test]
fn decode_iso_timestamp_with_zone() {
    let (dtype, tm, fsec, tz) = decode_ts("2024-01-02 03:04:05.678+07:30", true).unwrap();
    assert_eq!(dtype, DTK_DATE);
    assert_eq!(ymd(&tm), (2024, 1, 2));
    assert_eq!(hms(&tm), (3, 4, 5));
    assert_eq!(fsec, 678_000);
    assert_eq!(tz, -27_000);

    let (_, tm, _, tz) = decode_ts("1999-01-08 04:05:06 -08", true).unwrap();
    assert_eq!(ymd(&tm), (1999, 1, 8));
    assert_eq!(tz, 28_800);
}

#[test]
fn decode_plain_timestamp_without_zone() {
    let (dtype, tm, fsec, _) = decode_ts("1999-01-08 04:05:06", false).unwrap();
    assert_eq!(dtype, DTK_DATE);
    assert_eq!(ymd(&tm), (1999, 1, 8));
    assert_eq!(hms(&tm), (4, 5, 6));
    assert_eq!(fsec, 0);
    assert_eq!(tm.tm_isdst, -1);
}

#[test]
fn decode_text_month_and_ampm_and_bc() {
    let (_, tm, _, _) = decode_ts("January 8, 1999 04:05 PM", false).unwrap();
    assert_eq!(ymd(&tm), (1999, 1, 8));
    assert_eq!(hms(&tm), (16, 5, 0));

    let (_, tm, _, _) = decode_ts("08 Jan 1999 12:00 AM", false).unwrap();
    assert_eq!(ymd(&tm), (1999, 1, 8));
    assert_eq!(hms(&tm), (0, 0, 0));

    let (_, tm, _, _) = decode_ts("1999-01-08 04:05:06 BC", false).unwrap();
    assert_eq!(tm.tm_year, -1998);
}

#[test]
fn decode_date_order_variants() {
    set_date_order(DATEORDER_MDY);
    let (_, tm, _, _) = decode_ts("2/7/1997", false).unwrap();
    assert_eq!(ymd(&tm), (1997, 2, 7));

    set_date_order(DATEORDER_DMY);
    let (_, tm, _, _) = decode_ts("2/7/1997", false).unwrap();
    assert_eq!(ymd(&tm), (1997, 7, 2));

    set_date_order(DATEORDER_YMD);
    let (_, tm, _, _) = decode_ts("97/2/7", false).unwrap();
    assert_eq!(ymd(&tm), (1997, 2, 7));
    set_date_order(DATEORDER_MDY);
}

#[test]
fn decode_doy_concatenated_julian_and_epoch() {
    let (_, tm, _, _) = decode_ts("1999.038", false).unwrap();
    assert_eq!(ymd(&tm), (1999, 2, 7));

    let (_, tm, fsec, tz) = decode_ts("20011225T040506.789-07", true).unwrap();
    assert_eq!(ymd(&tm), (2001, 12, 25));
    assert_eq!(hms(&tm), (4, 5, 6));
    assert_eq!(fsec, 789_000);
    assert_eq!(tz, 25_200);

    let (_, tm, _, _) = decode_ts("19990108 040506", false).unwrap();
    assert_eq!(ymd(&tm), (1999, 1, 8));
    assert_eq!(hms(&tm), (4, 5, 6));

    let (_, tm, _, _) = decode_ts("990108", false).unwrap();
    assert_eq!(ymd(&tm), (1999, 1, 8));

    let (_, tm, _, _) = decode_ts("J2451187", false).unwrap();
    assert_eq!(ymd(&tm), (1999, 1, 8));

    let (dtype, ..) = decode_ts("epoch", false).unwrap();
    assert_eq!(dtype, DTK_EPOCH);
    let (dtype, ..) = decode_ts("infinity", false).unwrap();
    assert_eq!(dtype, DTK_LATE);
    let (dtype, ..) = decode_ts("-infinity", false).unwrap();
    assert_eq!(dtype, DTK_EARLY);
}

#[test]
fn decode_rejects_bad_inputs_with_c_error_codes() {
    assert_eq!(decode_ts("1999-13-08", false).unwrap_err(), DTERR_MD_FIELD_OVERFLOW);
    assert_eq!(decode_ts("1999-02-29", false).unwrap_err(), DTERR_FIELD_OVERFLOW);
    assert_eq!(decode_ts("1999-01-08 25:00:00", false).unwrap_err(), DTERR_FIELD_OVERFLOW);
    assert_eq!(decode_ts("1999-01-08 04:05:06 +08", false).unwrap_err(), DTERR_BAD_FORMAT);
    assert_eq!(decode_ts("0000-01-08", false).unwrap_err(), DTERR_FIELD_OVERFLOW);
    assert_eq!(decode_ts("1999-01-08 16:05 PM", false).unwrap_err(), DTERR_FIELD_OVERFLOW);
    assert_eq!(decode_ts("04:05:06 04:05:06", false).unwrap_err(), DTERR_BAD_FORMAT);
}

fn setup_tz_engine() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        pgtz::init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
}

#[test]
fn unknown_string_field_is_bad_timezone() {
    setup_tz_engine();
    // The all-alpha UNKNOWN_FIELD arm tries pg_tzset and gives BAD_FORMAT on
    // failure (C DecodeDateTime); no more unported-engine panic.
    assert_eq!(decode_ts("junk", false).unwrap_err(), DTERR_BAD_FORMAT);
    assert_eq!(decode_ts("junk", true).unwrap_err(), DTERR_BAD_FORMAT);
}

#[test]
fn session_zone_resolution_uses_the_engine() {
    setup_tz_engine();
    tz::pg_timezone_initialize();
    let (_, _, _, tzv) = decode_ts("1999-01-08 04:05:06", true).unwrap();
    assert_eq!(tzv, 0, "GMT session timezone");

    // Session zone with DST: EST in January (+18000 west), EDT in July.
    tz::set_session_timezone(tz::pg_tzset(b"America/New_York"));
    let (_, tm, _, tzv) = decode_ts("1999-01-08 04:05:06", true).unwrap();
    assert_eq!((tzv, tm.tm_isdst), (5 * 3600, 0));
    let (_, tm, _, tzv) = decode_ts("1999-07-08 04:05:06", true).unwrap();
    assert_eq!((tzv, tm.tm_isdst), (4 * 3600, 1));
    tz::pg_timezone_initialize();
}

#[test]
fn named_zone_field_resolves_through_pg_tzset() {
    setup_tz_engine();
    let (_, tm, _, tzv) = decode_ts("2024-07-01 04:05:06 America/New_York", true).unwrap();
    assert_eq!((tzv, tm.tm_isdst), (4 * 3600, 1));
}

#[test]
fn decode_timezone_field_conventions() {
    let mut tz = 0;
    assert_eq!(DecodeTimezone(b"+07:30:15", &mut tz), 0);
    assert_eq!(tz, -(7 * 3600 + 30 * 60 + 15));
    assert_eq!(DecodeTimezone(b"-0800", &mut tz), 0);
    assert_eq!(tz, 8 * 3600);
    assert_eq!(DecodeTimezone(b"+08", &mut tz), 0);
    assert_eq!(tz, -8 * 3600);
    assert_eq!(DecodeTimezone(b"+16", &mut tz), DTERR_TZDISP_OVERFLOW);
    assert_eq!(DecodeTimezone(b"+07:60", &mut tz), DTERR_TZDISP_OVERFLOW);
    assert_eq!(DecodeTimezone(b"07", &mut tz), DTERR_BAD_FORMAT);
    assert_eq!(DecodeTimezone(b"+07x", &mut tz), DTERR_BAD_FORMAT);
}

#[test]
fn decode_time_only() {
    let (dtype, tm, fsec, tz) = decode_time("04:05:06.789-08", true).unwrap();
    assert_eq!(dtype, DTK_TIME);
    assert_eq!(hms(&tm), (4, 5, 6));
    assert_eq!(fsec, 789_000);
    assert_eq!(tz, 28_800);

    let (_, tm, fsec, _) = decode_time("04:05:06.5", false).unwrap();
    assert_eq!(hms(&tm), (4, 5, 6));
    assert_eq!(fsec, 500_000);

    let (_, tm, _, _) = decode_time("04:05 PM", false).unwrap();
    assert_eq!(hms(&tm), (16, 5, 0));

    assert_eq!(decode_time("25:00:00", false).unwrap_err(), DTERR_FIELD_OVERFLOW);
}

#[test]
fn fractions_round_ties_to_even_like_rint() {
    let mut fsec = 0;
    assert_eq!(ParseFractionalSecond(b".5", &mut fsec), 0);
    assert_eq!(fsec, 500_000);
    assert_eq!(ParseFractionalSecond(b".0000005", &mut fsec), 0);
    assert_eq!(fsec, 0);
    assert_eq!(ParseFractionalSecond(b".0000015", &mut fsec), 0);
    assert_eq!(fsec, 2);
    assert_eq!(ParseFractionalSecond(b".12e4", &mut fsec), DTERR_BAD_FORMAT);
    let mut frac = 1.0;
    assert_eq!(ParseFraction(b".", &mut frac), 0);
    assert_eq!(frac, 0.0);
}

fn tm_at(y: i32, mo: i32, d: i32, h: i32, mi: i32, s: i32, isdst: i32) -> pg_tm {
    pg_tm {
        tm_year: y,
        tm_mon: mo,
        tm_mday: d,
        tm_hour: h,
        tm_min: mi,
        tm_sec: s,
        tm_isdst: isdst,
        ..Default::default()
    }
}

fn encode_dt(tm: &mut pg_tm, fsec: fsec_t, tz: i32, tzn: Option<&[u8]>, style: i32) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = EncodeDateTime(tm, fsec, true, tz, tzn, style, &mut buf);
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[test]
fn encode_datetime_all_styles() {
    set_date_order(DATEORDER_MDY);
    let mut tm = tm_at(2024, 1, 2, 3, 4, 5, 0);
    assert_eq!(encode_dt(&mut tm, 678_000, -27_000, None, USE_ISO_DATES),
        "2024-01-02 03:04:05.678+07:30");
    assert_eq!(encode_dt(&mut tm, 678_000, -27_000, None, USE_XSD_DATES),
        "2024-01-02T03:04:05.678+07:30");
    assert_eq!(encode_dt(&mut tm, 678_000, -27_000, None, USE_SQL_DATES),
        "01/02/2024 03:04:05.678+07:30");
    assert_eq!(encode_dt(&mut tm, 0, -27_000, Some(b"XYZ"), USE_SQL_DATES),
        "01/02/2024 03:04:05 XYZ");
    assert_eq!(encode_dt(&mut tm, 678_000, -27_000, None, USE_GERMAN_DATES),
        "02.01.2024 03:04:05.678+07:30");
    assert_eq!(encode_dt(&mut tm, 678_000, -27_000, None, USE_POSTGRES_DATES),
        "Tue Jan 02 03:04:05.678 2024 +07:30");
    assert_eq!(tm.tm_wday, 2);

    let mut no_tz = tm_at(2024, 1, 2, 3, 4, 5, -1);
    assert_eq!(encode_dt(&mut no_tz, 0, 0, None, USE_ISO_DATES), "2024-01-02 03:04:05");

    let mut bc = tm_at(-1998, 1, 8, 4, 5, 6, -1);
    assert_eq!(encode_dt(&mut bc, 0, 0, None, USE_ISO_DATES), "1999-01-08 04:05:06 BC");
}

#[test]
fn encode_date_and_time_only() {
    set_date_order(DATEORDER_MDY);
    let tm = tm_at(2024, 1, 2, 4, 5, 6, -1);
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = EncodeDateOnly(&tm, USE_ISO_DATES, &mut buf);
    assert_eq!(&buf[..n], b"2024-01-02");
    let n = EncodeDateOnly(&tm, USE_SQL_DATES, &mut buf);
    assert_eq!(&buf[..n], b"01/02/2024");
    let n = EncodeDateOnly(&tm, USE_GERMAN_DATES, &mut buf);
    assert_eq!(&buf[..n], b"02.01.2024");
    let n = EncodeDateOnly(&tm, USE_POSTGRES_DATES, &mut buf);
    assert_eq!(&buf[..n], b"01-02-2024");

    let n = EncodeTimeOnly(&tm, 500_000, true, 28_800, USE_ISO_DATES, &mut buf);
    assert_eq!(&buf[..n], b"04:05:06.5-08");
    let n = EncodeTimeOnly(&tm, 0, false, 0, USE_ISO_DATES, &mut buf);
    assert_eq!(&buf[..n], b"04:05:06");
}

#[test]
fn append_seconds_and_encode_timezone_edge_cases() {
    let mut buf = [0u8; 32];
    let n = AppendSeconds(&mut buf, 0, 5, 120_000, MAX_TIMESTAMP_PRECISION, true);
    assert_eq!(&buf[..n], b"05.12");
    let n = AppendSeconds(&mut buf, 0, 5, 123_456, MAX_TIMESTAMP_PRECISION, true);
    assert_eq!(&buf[..n], b"05.123456");
    let n = AppendSeconds(&mut buf, 0, 5, 0, MAX_TIMESTAMP_PRECISION, false);
    assert_eq!(&buf[..n], b"5");
    let n = AppendSeconds(&mut buf, 0, -5, -120_000, MAX_TIMESTAMP_PRECISION, true);
    assert_eq!(&buf[..n], b"05.12");

    let n = EncodeTimezone(&mut buf, 0, -(7 * 3600 + 30 * 60 + 15), USE_ISO_DATES);
    assert_eq!(&buf[..n], b"+07:30:15");
    let n = EncodeTimezone(&mut buf, 0, 8 * 3600, USE_ISO_DATES);
    assert_eq!(&buf[..n], b"-08");
    let n = EncodeTimezone(&mut buf, 0, -8 * 3600, USE_XSD_DATES);
    assert_eq!(&buf[..n], b"+08:00");
}

#[test]
fn calendar_roundtrip() {
    let (mut y, mut m, mut d) = (0, 0, 0);
    j2date(date2j(1999, 1, 8), &mut y, &mut m, &mut d);
    assert_eq!((y, m, d), (1999, 1, 8));
    j2date(date2j(-4713, 11, 24), &mut y, &mut m, &mut d);
    assert_eq!((y, m, d), (-4713, 11, 24));
    assert_eq!(j2day(date2j(2024, 1, 2)), 2);
    assert!(isleap(2000) && isleap(2024) && !isleap(1900));

    let (mut h, mut mi, mut s, mut f) = (0, 0, 0, 0);
    dt2time(4 * USECS_PER_HOUR + 5 * USECS_PER_MINUTE + 6 * USECS_PER_SEC + 7,
        &mut h, &mut mi, &mut s, &mut f);
    assert_eq!((h, mi, s, f), (4, 5, 6, 7));

    assert!(!time_overflows(24, 0, 0, 0));
    assert!(time_overflows(24, 0, 0, 1));
    assert!(time_overflows(23, 59, 60, 1));
    assert!(time_overflows(-1, 0, 0, 0));
}

#[test]
fn validate_date_bc_and_two_digit_years() {
    let mut tm = pg_tm { tm_year: 99, tm_mon: 1, tm_mday: 8, ..Default::default() };
    assert_eq!(ValidateDate(DTK_DATE_M, false, true, false, &mut tm), 0);
    assert_eq!(tm.tm_year, 1999);
    let mut tm = pg_tm { tm_year: 69, tm_mon: 1, tm_mday: 8, ..Default::default() };
    assert_eq!(ValidateDate(DTK_DATE_M, false, true, false, &mut tm), 0);
    assert_eq!(tm.tm_year, 2069);
    let mut tm = pg_tm { tm_year: 1999, tm_mon: 1, tm_mday: 8, ..Default::default() };
    assert_eq!(ValidateDate(DTK_DATE_M, false, false, true, &mut tm), 0);
    assert_eq!(tm.tm_year, -1998);
}

#[test]
fn parse_error_mapping_carries_c_sqlstates() {
    use types_error::SoftErrorContext;
    let err = DateTimeParseError(DTERR_BAD_FORMAT, None, "junk", "timestamp", None).unwrap_err();
    assert_eq!(err.message(), "invalid input syntax for type timestamp: \"junk\"");
    let err = DateTimeParseError(DTERR_MD_FIELD_OVERFLOW, None, "13/13/99", "date", None).unwrap_err();
    assert!(err.hint().unwrap().contains("DateStyle"));
    let extra = DateTimeErrorExtra { dtee_timezone: Some(b"Mars/Olympus"), dtee_abbrev: None };
    let err = DateTimeParseError(DTERR_BAD_TIMEZONE, Some(&extra), "x", "timestamptz", None).unwrap_err();
    assert_eq!(err.message(), "time zone \"Mars/Olympus\" not recognized");

    let mut soft = SoftErrorContext::new(true);
    assert!(DateTimeParseError(DTERR_FIELD_OVERFLOW, None, "x", "date", Some(&mut soft)).is_ok());
    assert!(soft.error_occurred());
}

fn decode_interval(input: &str, range: i32) -> Result<(i32, pg_itm_in), i32> {
    let mut workbuf = [0u8; TS_BUFLEN];
    let p = parse(input, &mut workbuf)?;
    let mut dtype = 0;
    let mut itm_in = pg_itm_in::default();
    let rc = DecodeInterval(&p.field[..p.nf], &p.ftype[..p.nf], p.nf, range, &mut dtype, &mut itm_in);
    if rc != 0 {
        return Err(rc);
    }
    Ok((dtype, itm_in))
}

fn decode_iso_interval(input: &str) -> Result<(i32, pg_itm_in), i32> {
    let mut dtype = 0;
    let mut itm_in = pg_itm_in::default();
    let rc = DecodeISO8601Interval(input.as_bytes(), &mut dtype, &mut itm_in);
    if rc != 0 {
        return Err(rc);
    }
    Ok((dtype, itm_in))
}

fn itm(usec: i64, mday: i32, mon: i32, year: i32) -> pg_itm_in {
    pg_itm_in { tm_usec: usec, tm_mday: mday, tm_mon: mon, tm_year: year }
}

#[test]
fn decode_interval_postgres_format() {
    let (dtype, v) = decode_interval("1 year 2 mons 3 days 04:05:06.789", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(dtype, DTK_DELTA);
    assert_eq!(v, itm(14_706_789_000, 3, 2, 1));

    let (_, v) = decode_interval("-1 day +5 hours", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(5 * USECS_PER_HOUR, -1, 0, 0));

    let (_, v) = decode_interval("1-2", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(0, 0, 14, 0));

    let (_, v) = decode_interval("-1-2", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(0, 0, -14, 0));

    let (_, v) = decode_interval("1 day ago", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(0, -1, 0, 0));

    let (_, v) = decode_interval("@ 1 minute", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(USECS_PER_MINUTE, 0, 0, 0));

    let (_, v) = decode_interval("1.5 mons", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(0, 15, 1, 0));

    let (_, v) = decode_interval("2.5 weeks", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(12 * USECS_PER_HOUR, 17, 0, 0));

    let (_, v) = decode_interval("-00:01:30", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(-90 * USECS_PER_SEC, 0, 0, 0));

    let (_, v) = decode_interval("1 +02:03", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(2 * USECS_PER_HOUR + 3 * USECS_PER_MINUTE, 1, 0, 0));
}

#[test]
fn decode_interval_range_and_specials() {
    let (_, v) = decode_interval("12:34", INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)).unwrap();
    assert_eq!(v, itm(12 * USECS_PER_MINUTE + 34 * USECS_PER_SEC, 0, 0, 0));

    let (_, v) = decode_interval("12:34", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(12 * USECS_PER_HOUR + 34 * USECS_PER_MINUTE, 0, 0, 0));

    let (_, v) = decode_interval("7", INTERVAL_MASK(DAY)).unwrap();
    assert_eq!(v, itm(0, 7, 0, 0));

    let (dtype, _) = decode_interval("infinity", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(dtype, DTK_LATE);
    let (dtype, _) = decode_interval("-infinity", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(dtype, DTK_EARLY);

    assert_eq!(decode_interval("infinity ago", INTERVAL_FULL_RANGE), Err(DTERR_BAD_FORMAT));
    assert_eq!(decode_interval("day", INTERVAL_FULL_RANGE), Err(DTERR_BAD_FORMAT));
    // trailing bare number picks up the range-default unit (seconds)
    let (_, v) = decode_interval("1 day 2", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(2 * USECS_PER_SEC, 1, 0, 0));
    assert_eq!(decode_interval("1 day day", INTERVAL_FULL_RANGE), Err(DTERR_BAD_FORMAT));
    assert_eq!(
        decode_interval("9999999999999999999 days", INTERVAL_FULL_RANGE),
        Err(DTERR_FIELD_OVERFLOW)
    );
    assert_eq!(
        decode_interval("2147483648 days", INTERVAL_FULL_RANGE),
        Err(DTERR_FIELD_OVERFLOW)
    );
}

#[test]
fn decode_interval_sql_standard_leading_sign() {
    set_interval_style(INTSTYLE_SQL_STANDARD);
    let (_, v) = decode_interval("-1 1:00:00", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(-USECS_PER_HOUR, -1, 0, 0));
    // an additional explicit sign disables force_negative
    let (_, v) = decode_interval("-1 +1:00:00", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(USECS_PER_HOUR, -1, 0, 0));
    set_interval_style(INTSTYLE_POSTGRES);
    let (_, v) = decode_interval("-1 1:00:00", INTERVAL_FULL_RANGE).unwrap();
    assert_eq!(v, itm(USECS_PER_HOUR, -1, 0, 0));
}

#[test]
fn decode_iso8601_interval() {
    let (dtype, v) = decode_iso_interval("P1Y2M3DT4H5M6.7S").unwrap();
    assert_eq!(dtype, DTK_DELTA);
    assert_eq!(v, itm(14_706_700_000, 3, 2, 1));

    let (_, v) = decode_iso_interval("P0001-02-03T04:05:06").unwrap();
    assert_eq!(v, itm(14_706_000_000, 3, 2, 1));

    let (_, v) = decode_iso_interval("P00010203T040506").unwrap();
    assert_eq!(v, itm(14_706_000_000, 3, 2, 1));

    let (_, v) = decode_iso_interval("PT0S").unwrap();
    assert_eq!(v, itm(0, 0, 0, 0));

    let (_, v) = decode_iso_interval("P1W").unwrap();
    assert_eq!(v, itm(0, 7, 0, 0));

    let (_, v) = decode_iso_interval("P-1M").unwrap();
    assert_eq!(v, itm(0, 0, -1, 0));

    let (_, v) = decode_iso_interval("PT1.5H").unwrap();
    assert_eq!(v, itm(90 * USECS_PER_MINUTE, 0, 0, 0));

    assert_eq!(decode_iso_interval("P"), Err(DTERR_BAD_FORMAT));
    assert_eq!(decode_iso_interval("1Y"), Err(DTERR_BAD_FORMAT));
    assert_eq!(decode_iso_interval("P1X"), Err(DTERR_BAD_FORMAT));
    assert_eq!(decode_iso_interval("P9999999999999999Y"), Err(DTERR_FIELD_OVERFLOW));
}
