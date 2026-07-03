use super::*;
use adt_datetime::{set_date_style, USE_GERMAN_DATES, USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES};
use types_error::{ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT};

fn gmt_session() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // SAFETY: single-threaded test init, before any getenv (adt_datetime
        // tests' precedent).
        unsafe { std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo") };
        pgtz::init_seams();
        adt_timestamp::init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
    tz::pg_timezone_initialize();
}

fn d_in(s: &str) -> DateADT {
    date_in(s, None).unwrap()
}

fn d_out(d: DateADT) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = date_out(d, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn t_in(s: &str, typmod: i32) -> TimeADT {
    time_in(s, typmod, None).unwrap()
}

fn t_out(t: TimeADT) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = time_out(t, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn tz_in(s: &str, typmod: i32) -> TimeTzADT {
    timetz_in(s, typmod, None).unwrap()
}

fn tz_out(t: &TimeTzADT) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = timetz_out(t, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

// Expected strings are live psql output from PostgreSQL 18.3
// (TimeZone=GMT, DateStyle=ISO, MDY).
#[test]
fn date_in_out_goldens() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    for (input, expect) in [
        ("2024-02-29", "2024-02-29"),
        ("J2451545", "2000-01-01"),
        ("epoch", "1970-01-01"),
        ("infinity", "infinity"),
        ("-infinity", "-infinity"),
        ("5874897-12-31", "5874897-12-31"),
        ("4714-11-24 BC", "4714-11-24 BC"),
        ("January 8, 1999", "1999-01-08"),
        ("1/8/1999", "1999-01-08"),
        ("19990108", "1999-01-08"),
        ("2000-1-1", "2000-01-01"),
    ] {
        assert_eq!(d_out(d_in(input)), expect, "for {input}");
    }
    assert_eq!(d_in("2000-01-01"), 0);
    assert_eq!(d_in("epoch"), UNIX_EPOCH_JDATE_DIFF);
}

const UNIX_EPOCH_JDATE_DIFF: DateADT = 2440588 - POSTGRES_EPOCH_JDATE;

#[test]
fn date_out_all_styles() {
    gmt_session();
    let d = d_in("1999-01-08");
    let bc = d_in("0044-03-15 BC");
    set_date_style(USE_SQL_DATES);
    assert_eq!(d_out(d), "01/08/1999");
    assert_eq!(d_out(bc), "03/15/0044 BC");
    set_date_style(USE_POSTGRES_DATES);
    assert_eq!(d_out(d), "01-08-1999");
    set_date_style(USE_GERMAN_DATES);
    assert_eq!(d_out(d), "08.01.1999");
    set_date_style(USE_ISO_DATES);
}

#[test]
fn date_in_errors_are_c_exact() {
    gmt_session();
    let e = date_in("not a date", None).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_DATETIME_FORMAT);
    assert_eq!(e.message(), "invalid input syntax for type date: \"not a date\"");
    let e = date_in("2024-02-30", None).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_DATETIME_FIELD_OVERFLOW);
    let e = date_in("5874898-01-01", None).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    assert_eq!(e.message(), "date out of range: \"5874898-01-01\"");

    let mut esc = SoftErrorContext::default();
    assert_eq!(date_in("bogus", Some(&mut esc)).unwrap(), 0);
    assert!(esc.error_occurred());
}

#[test]
fn date_arithmetic_and_bounds() {
    gmt_session();
    let d = d_in("2024-01-01");
    assert_eq!(date_mi(d_in("2024-02-29"), d_in("2024-01-01")).unwrap(), 59);
    assert_eq!(date_pli(d, 59).unwrap(), d_in("2024-02-29"));
    assert_eq!(date_mii(d_in("2024-02-29"), 59).unwrap(), d);
    assert_eq!(date_pli(DATEVAL_NOEND, 1).unwrap(), DATEVAL_NOEND);
    assert_eq!(
        date_mi(DATEVAL_NOEND, d).unwrap_err().sqlstate(),
        ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
    );
    let maxd = d_in("5874897-12-31");
    assert_eq!(
        date_pli(maxd, 1).unwrap_err().sqlstate(),
        ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
    );
    assert!(date_pli(d, i32::MAX).is_err());
    assert!(date_mii(d, i32::MAX).is_err());
    assert_eq!(make_date(2024, 2, 29).unwrap(), d_in("2024-02-29"));
    assert_eq!(make_date(-44, 3, 15).unwrap(), d_in("0044-03-15 BC"));
    assert_eq!(
        make_date(2024, 2, 30).unwrap_err().sqlstate(),
        ERRCODE_DATETIME_FIELD_OVERFLOW
    );
    assert!(make_date(i32::MIN, 1, 1).is_err());
}

#[test]
fn date_timestamp_conversions() {
    gmt_session();
    let d = d_in("2024-06-15");
    let ts = date2timestamp(d).unwrap();
    assert_eq!(adt_timestamp::timestamp_in("2024-06-15 00:00:00", -1, None).unwrap(), ts);
    assert_eq!(timestamp_date(ts).unwrap(), d);
    // GMT session: timestamptz epoch matches
    let tstz = date2timestamptz(d).unwrap();
    assert_eq!(tstz, ts);
    assert_eq!(timestamptz_date(tstz).unwrap(), d);

    assert_eq!(date2timestamp(DATEVAL_NOEND).unwrap(), DT_NOEND);
    assert_eq!(date2timestamp(DATEVAL_NOBEGIN).unwrap(), DT_NOBEGIN);
    let big = d_in("294277-01-01");
    assert_eq!(
        date2timestamp(big).unwrap_err().sqlstate(),
        ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
    );
    let mut ovf = 0;
    assert_eq!(date2timestamp_opt_overflow(big, Some(&mut ovf)).unwrap(), DT_NOEND);
    assert_eq!(ovf, 1);
    assert_eq!(date2timestamp_no_overflow(d), (d as f64) * USECS_PER_DAY as f64);
    assert_eq!(date2timestamp_no_overflow(DATEVAL_NOEND), f64::MAX);

    let t = t_in("12:34:56+00", -1);
    assert_eq!(
        datetime_timestamp(d, t).unwrap(),
        adt_timestamp::timestamp_in("2024-06-15 12:34:56", -1, None).unwrap()
    );
}

#[test]
fn date_cross_type_comparisons() {
    gmt_session();
    let d = d_in("2024-06-15");
    let ts = adt_timestamp::timestamp_in("2024-06-15 00:00:00", -1, None).unwrap();
    assert_eq!(date_cmp_timestamp_internal(d, ts), 0);
    assert_eq!(date_cmp_timestamp_internal(d, ts + 1), -1);
    assert_eq!(date_cmp_timestamp_internal(d, ts - 1), 1);
    // beyond-timestamp date sorts above every finite timestamp, below infinity
    let big = d_in("294277-01-01");
    assert_eq!(date_cmp_timestamp_internal(big, ts), 1);
    assert_eq!(date_cmp_timestamp_internal(big, DT_NOEND), -1);
    assert_eq!(date_cmp_timestamptz_internal(d, ts), 0);
}

#[test]
fn time_in_out_goldens() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    // explicit zones (parsed, discarded): the zoneless spellings block on the
    // tz lane's GetCurrentDateTime (see lock test below)
    for (input, typmod, expect) in [
        ("23:59:60+00", -1, "24:00:00"),
        ("11:59:59.9999995+00", -1, "12:00:00"),
        ("24:00:00+00", -1, "24:00:00"),
        ("00:00+00", -1, "00:00:00"),
        ("12:34:56.789456+00", 3, "12:34:56.789"),
        ("12:34:56.789456+00", -1, "12:34:56.789456"),
        ("3:14 PM GMT", -1, "15:14:00"),
    ] {
        assert_eq!(t_out(t_in(input, typmod)), expect, "for {input}");
    }
    assert_eq!(t_in("24:00:00+00", -1), USECS_PER_DAY);
    let e = time_in("25:00:00+00", -1, None).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_DATETIME_FIELD_OVERFLOW);
    let e = time_in("garbage", -1, None).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_DATETIME_FORMAT);
}

#[test]
fn zoneless_time_in_lives_on_tz_seams() {
    gmt_session();
    assert_eq!(t_out(t_in("12:34:56", -1)), "12:34:56");
}

#[test]
fn sql_current_time_lives_on_tz_seams() {
    gmt_session();
    let t = GetSQLLocalTime(-1);
    assert!((0..=USECS_PER_DAY).contains(&t));
    let ct = GetSQLCurrentTime(-1);
    assert!((0..=USECS_PER_DAY).contains(&ct.time));
    let d = GetSQLCurrentDate();
    // repeat hits the SQL_CURRENT_DATE_CACHE memo
    assert_eq!(GetSQLCurrentDate(), d);
    assert!(IS_VALID_DATE(d));
}

#[test]
fn time_helpers_match_c() {
    gmt_session();
    assert_eq!(make_time(8, 15, 55.333).unwrap(), t_in("08:15:55.333+00", -1));
    assert!(make_time(24, 0, 2.1).is_err());
    assert!(make_time(10, 60, 0.0).is_err());
    assert!(make_time(10, 10, f64::NAN).is_err());
    assert!(float_time_overflows(24, 0, 0.5));
    assert!(!float_time_overflows(24, 0, 0.0));
    assert!(float_time_overflows(23, 59, 60.6));
    assert!(!float_time_overflows(23, 59, 60.0));
    // rint ties-to-even keeps 60000000.5 at the boundary, as in C
    assert!(!float_time_overflows(23, 59, 60.0000005));

    assert_eq!(time_scale(t_in("12:34:56.789456+00", -1), 3), t_in("12:34:56.789+00", -1));
    let mut t = -1i64;
    AdjustTimeForTypmod(&mut t, 0);
    assert_eq!(t, 0);

    assert_eq!(time_cmp_internal(1, 2), -1);
    assert_eq!(time_cmp_internal(2, 1), 1);
    assert_eq!(time_cmp_internal(2, 2), 0);
}

#[test]
fn timetz_in_out_goldens() {
    gmt_session();
    set_date_style(USE_ISO_DATES);
    for (input, typmod, expect) in [
        ("12:00:00+05:30", -1, "12:00:00+05:30"),
        ("23:59:59.99-08", -1, "23:59:59.99-08"),
        ("00:00+15:59", -1, "00:00:00+15:59"),
        ("12:00:00.5-00:30:30", -1, "12:00:00.5-00:30:30"),
    ] {
        assert_eq!(tz_out(&tz_in(input, typmod)), expect, "for {input}");
    }
    let tt = tz_in("12:00:00+05:30", -1);
    assert_eq!(tt.time, t_in("12:00:00+00", -1));
    // zone stored negated relative to display sign (C convention)
    assert_eq!(tt.zone, -(5 * 3600 + 30 * 60));

    let e = timetz_in("12:00:00+16:00", -1, None).unwrap_err();
    assert_eq!(
        e.sqlstate(),
        types_error::ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE
    );
}

#[test]
fn timetz_cmp_gmt_equivalent_then_zone() {
    gmt_session();
    let a = tz_in("12:00:00+02", -1);
    let b = tz_in("11:00:00+01", -1);
    // same GMT instant: zone breaks the tie (larger zone value sorts higher)
    assert_eq!(timetz_cmp_internal(&a, &b), timetz_cmp_internal(&b, &a).wrapping_neg());
    assert_ne!(timetz_cmp_internal(&a, &b), 0);
    let c = tz_in("12:00:00+01", -1);
    assert_eq!(timetz_cmp_internal(&b, &c), -1);
    assert_eq!(timetz_cmp_internal(&c, &b), 1);
    assert_eq!(timetz_cmp_internal(&a, &a), 0);
    assert_eq!(timetz_time(&a), a.time);
    assert_eq!(timetz_scale(&tz_in("12:00:00.789456+03", -1), 3).time, t_in("12:00:00.789+00", -1));
}

#[test]
fn timestamp_time_conversions() {
    gmt_session();
    let ts = adt_timestamp::timestamp_in("2024-06-15 12:34:56.789", -1, None).unwrap();
    assert_eq!(timestamp_time(ts).unwrap().unwrap(), t_in("12:34:56.789+00", -1));
    assert_eq!(timestamp_time(DT_NOEND).unwrap(), None);
    assert_eq!(timestamptz_time(ts).unwrap().unwrap(), t_in("12:34:56.789+00", -1));
    let tt = timestamptz_timetz(ts).unwrap().unwrap();
    assert_eq!(tt.time, t_in("12:34:56.789+00", -1));
    assert_eq!(tt.zone, 0);
    assert_eq!(timestamptz_timetz(DT_NOBEGIN).unwrap(), None);

    let d = d_in("2024-06-15");
    let x = tz_in("12:00:00+05:30", -1);
    assert_eq!(
        datetimetz_timestamptz(d, &x).unwrap(),
        adt_timestamp::timestamptz_in("2024-06-15 12:00:00+05:30", -1, None).unwrap()
    );
    assert_eq!(datetimetz_timestamptz(DATEVAL_NOEND, &x).unwrap(), DT_NOEND);
    assert!(datetimetz_timestamptz(d_in("294277-01-01"), &x).is_err());
}

#[test]
fn hash_folds_match_c_shape() {
    // hashint8 fold: positive xors hi, negative xors !hi
    assert_eq!(int64_hash_fold(0x1234_5678_9abc_def0_u64 as i64 & i64::MAX), {
        let v = 0x1234_5678_9abc_def0_u64 as i64 & i64::MAX;
        (v as u32) ^ ((v >> 32) as u32)
    });
    let v = -42i64;
    assert_eq!(int64_hash_fold(v), (v as u32) ^ !((v >> 32) as u32));
    // equal int4/int8 values hash equal (cross-type hash join contract)
    assert_eq!(int64_hash_fold(12345), 12345u32 ^ 0);
}

#[test]
fn special_date_encoding_is_byte_exact() {
    let mut buf = [0u8; MAXDATELEN + 1];
    let n = EncodeSpecialDate(DATEVAL_NOBEGIN, &mut buf);
    assert_eq!(&buf[..n], b"-infinity");
    let n = EncodeSpecialDate(DATEVAL_NOEND, &mut buf);
    assert_eq!(&buf[..n], b"infinity");
    assert_eq!(d_out(DATEVAL_NOEND), "infinity");
}

#[test]
fn binary_wire_round_trip() {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;
    let ctx = MemoryContext::new("date-wire");
    let mcx = ctx.mcx();

    // date: Datum -> send -> bytea -> recv -> Datum, bit-identical.
    for d in [0i32, 1, -1, 12345, DATEVAL_NOBEGIN, DATEVAL_NOEND] {
        let b = date_send(mcx, d).unwrap();
        let mut si = StringInfo::with_capacity_in(mcx, b.data().len() + 1).unwrap();
        si.append_bytes(b.data()).unwrap();
        assert_eq!(date_recv(&mut si).unwrap(), d);
        assert_eq!(si.cursor, si.len());
    }

    for t in [0i64, 1, USECS_PER_DAY, 43200_000000] {
        let b = time_send(mcx, t).unwrap();
        let mut si = StringInfo::with_capacity_in(mcx, b.data().len() + 1).unwrap();
        si.append_bytes(b.data()).unwrap();
        assert_eq!(time_recv(&mut si, -1).unwrap(), t);
        assert_eq!(si.cursor, si.len());
    }

    for tz in [TimeTzADT { time: 0, zone: 0 }, TimeTzADT { time: 3600_000000, zone: -3600 }] {
        let b = timetz_send(mcx, &tz).unwrap();
        let mut si = StringInfo::with_capacity_in(mcx, b.data().len() + 1).unwrap();
        si.append_bytes(b.data()).unwrap();
        assert_eq!(timetz_recv(&mut si, -1).unwrap(), tz);
        assert_eq!(si.cursor, si.len());
    }
}
