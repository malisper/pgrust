#![allow(non_snake_case)]

use adt_datetime::*;

fn enc(itm: &pg_itm, style: i32) -> String {
    let mut buf = [0u8; MAXDATELEN + 1];
    let len = EncodeInterval(itm, style, &mut buf);
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

fn itm(year: i32, mon: i32, mday: i32, hour: i64, min: i32, sec: i32, usec: i32) -> pg_itm {
    pg_itm { tm_usec: usec, tm_sec: sec, tm_min: min, tm_hour: hour, tm_mday: mday, tm_mon: mon, tm_year: year }
}

const FULL: pg_itm =
    pg_itm { tm_usec: 789000, tm_sec: 6, tm_min: 5, tm_hour: 4, tm_mday: 3, tm_mon: 2, tm_year: 1 };

#[test]
fn postgres_style() {
    let p = INTSTYLE_POSTGRES;
    assert_eq!(enc(&FULL, p), "1 year 2 mons 3 days 04:05:06.789");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 0, 0), p), "00:00:00");
    assert_eq!(enc(&itm(0, 0, 0, -4, -5, -6, 0), p), "-04:05:06");
    assert_eq!(enc(&itm(1, 0, 0, -3, 0, 0, 0), p), "1 year -03:00:00");
    assert_eq!(enc(&itm(0, -1, 0, 2, 0, 0, 0), p), "-1 mons +02:00:00");
    assert_eq!(enc(&itm(0, -1, 2, 0, 0, 0, 0), p), "-1 mons +2 days");
    assert_eq!(enc(&itm(0, 0, 1, 0, 0, 0, 0), p), "1 day");
    assert_eq!(enc(&itm(-1, -2, -3, -4, -5, -6, -789000), p), "-1 years -2 mons -3 days -04:05:06.789");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 0, 100), p), "00:00:00.0001");
}

#[test]
fn sql_standard_style() {
    let s = INTSTYLE_SQL_STANDARD;
    assert_eq!(enc(&FULL, s), "+1-2 +3 +4:05:06.789");
    assert_eq!(enc(&itm(1, 2, 0, 0, 0, 0, 0), s), "1-2");
    assert_eq!(enc(&itm(-1, -2, 0, 0, 0, 0, 0), s), "-1-2");
    assert_eq!(enc(&itm(0, 0, 3, 4, 5, 6, 0), s), "3 4:05:06");
    assert_eq!(enc(&itm(0, 0, 0, 4, 5, 0, 0), s), "4:05:00");
    assert_eq!(enc(&itm(0, 0, 0, -4, -5, 0, 0), s), "-4:05:00");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 0, 0), s), "0");
    assert_eq!(enc(&itm(0, -1, 2, 0, 0, 0, 0), s), "-0-1 +2 +0:00:00");
}

#[test]
fn iso_8601_style() {
    let i = INTSTYLE_ISO_8601;
    assert_eq!(enc(&FULL, i), "P1Y2M3DT4H5M6.789S");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 0, 0), i), "PT0S");
    assert_eq!(enc(&itm(0, 0, 3, 0, 0, 0, 0), i), "P3D");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, -6, -789000), i), "PT-6.789S");
    assert_eq!(enc(&itm(0, -1, 0, 0, 0, 0, 0), i), "P-1M");
    assert_eq!(enc(&itm(0, 0, 0, 2, 0, 0, 0), i), "PT2H");
}

#[test]
fn postgres_verbose_style() {
    let v = INTSTYLE_POSTGRES_VERBOSE;
    assert_eq!(enc(&FULL, v), "@ 1 year 2 mons 3 days 4 hours 5 mins 6.789 secs");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 0, 0), v), "@ 0");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 1, 0), v), "@ 1 sec");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, 1, 500000), v), "@ 1.5 secs");
    assert_eq!(enc(&itm(-1, -2, 0, 0, 0, 0, 0), v), "@ 1 year 2 mons ago");
    assert_eq!(enc(&itm(0, -1, 2, 0, 0, 0, 0), v), "@ 1 mon -2 days ago");
    assert_eq!(enc(&itm(0, 0, 0, 0, 0, -5, 0), v), "@ 5 secs ago");
    assert_eq!(enc(&itm(0, 1, 0, 0, 0, -5, 0), v), "@ 1 mon -5 secs");
}
