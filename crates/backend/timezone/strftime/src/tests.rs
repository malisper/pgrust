use super::*;

fn sample_tm(zone: &'static str) -> PgTm<'static> {
    PgTm {
        tm_sec: 7,
        tm_min: 6,
        tm_hour: 15,
        tm_mday: 2,
        tm_mon: 0,
        tm_year: 124,
        tm_wday: 2,
        tm_yday: 1,
        tm_isdst: 0,
        tm_gmtoff: -8 * 60 * 60,
        tm_zone: Some(zone),
    }
}

fn format(fmt: &str, t: &PgTm<'_>) -> Vec<u8> {
    let mut buf = [0u8; 256];
    let len = pg_strftime(&mut buf, fmt.as_bytes(), t).expect("output fits");
    assert_eq!(buf[len], 0, "NUL terminated");
    buf[..len].to_vec()
}

#[test]
fn formats_common_postgres_timestamp_parts() {
    let t = sample_tm("PST");
    assert_eq!(
        format("%a %b %e %T %Y %Z %z", &t),
        b"Tue Jan  2 15:06:07 2024 PST -0800"
    );
    assert_eq!(format("%F %R %r", &t), b"2024-01-02 15:06 03:06:07 PM");
}

#[test]
fn composites_and_out_of_range_names() {
    let mut t = sample_tm("UTC");
    t.tm_wday = 99;
    t.tm_mon = -1;
    assert_eq!(format("%A %a %B %b", &t), b"? ? ? ?");
    assert_eq!(
        format("%c %x %X", &sample_tm("UTC")),
        b"Tue Jan  2 15:06:07 2024 01/02/24 15:06:07"
    );
    assert_eq!(format("%+", &sample_tm("PST")), b"Tue Jan  2 15:06:07 PST 2024");
    assert_eq!(format("%v %D", &sample_tm("PST")), b" 2-Jan-2024 01/02/24");
}

#[test]
fn iso_week_year_boundaries() {
    let mut t = sample_tm("UTC");
    t.tm_year = 119;
    t.tm_mon = 11;
    t.tm_mday = 30;
    t.tm_wday = 1;
    t.tm_yday = 363;
    assert_eq!(format("%G-W%V-%u %g", &t), b"2020-W01-1 20");

    // 2021-01-01 (Friday) belongs to ISO week 53 of 2020 (--base back-up arm).
    let mut t = sample_tm("UTC");
    t.tm_year = 121;
    t.tm_mon = 0;
    t.tm_mday = 1;
    t.tm_wday = 5;
    t.tm_yday = 0;
    assert_eq!(format("%G-W%V-%u", &t), b"2020-W53-5");
}

#[test]
fn z_offset_arms() {
    let mut t = sample_tm("UTC");
    t.tm_isdst = -1;
    assert_eq!(format("[%z]", &t), b"[]");

    let mut t = sample_tm("-00");
    t.tm_gmtoff = 0;
    assert_eq!(format("%z", &t), b"-0000");

    let mut t = sample_tm("UTC");
    t.tm_zone = None;
    assert_eq!(format("[%Z][%z]", &t), b"[][-0800]");
}

#[test]
fn literal_and_modifier_edge_cases() {
    let t = sample_tm("UTC");
    assert_eq!(format("%", &t), b"%");
    assert_eq!(format("%Q", &t), b"Q");
    assert_eq!(format("%E", &t), b"E");
    assert_eq!(format("%EY", &t), b"2024");
    assert_eq!(format("%%", &t), b"%");
    assert_eq!(format("100%% %j", &t), b"100% 002");
    assert_eq!(format("a%nb%tc", &t), b"a\nb\tc");
}

#[test]
fn hour_and_week_fields() {
    let mut t = sample_tm("UTC");
    t.tm_hour = 5;
    assert_eq!(format("%H|%I|%k|%l|%p", &t), b"05|05| 5| 5|AM");
    t.tm_hour = 0;
    assert_eq!(format("%H|%I|%k|%l|%p", &t), b"00|12| 0|12|AM");
    let t = sample_tm("UTC");
    assert_eq!(format("%U %W %w %u %j %C %y", &t), b"00 01 2 2 002 20 24");
}

#[test]
fn negative_and_large_years() {
    // tm_year -1905 is year -5; %Y renders via the "-0" + abs(trail) path and
    // %C%y must equal %Y.
    let mut t = sample_tm("UTC");
    t.tm_year = -1905;
    assert_eq!(format("%Y", &t), b"-005");
    assert_eq!(format("%C%y", &t), b"-005");
    t.tm_year = 8100;
    assert_eq!(format("%Y", &t), b"10000");
}

#[test]
fn overflow_semantics() {
    let t = sample_tm("UTC");
    let mut buf = [b'x'; 4];
    assert_eq!(pg_strftime(&mut buf, b"%Y", &t), None);
    // C leaves the truncated bytes with no NUL.
    assert_eq!(&buf, b"2024");

    // Exactly-fits-without-NUL is still overflow (p == s + maxsize).
    let mut buf5 = [0u8; 5];
    assert_eq!(pg_strftime(&mut buf5, b"%Y", &t), Some(4));
    let mut buf0 = [0u8; 0];
    assert_eq!(pg_strftime(&mut buf0, b"%Y", &t), None);

    let mut buf2 = [b'x'; 2];
    assert_eq!(pg_strftime(&mut buf2, b"", &t), Some(0));
    assert_eq!(buf2[0], 0);
}

// Differential golden against C strftime for the log_line_prefix shape
// PostgreSQL uses in elog.c ("%Y-%m-%d %H:%M:%S %Z").
#[test]
fn elog_timestamp_shape() {
    let t = PgTm {
        tm_sec: 59,
        tm_min: 30,
        tm_hour: 23,
        tm_mday: 31,
        tm_mon: 11,
        tm_year: 99,
        tm_wday: 5,
        tm_yday: 364,
        tm_isdst: 0,
        tm_gmtoff: 0,
        tm_zone: Some("GMT"),
    };
    assert_eq!(format("%Y-%m-%d %H:%M:%S %Z", &t), b"1999-12-31 23:30:59 GMT");
}
