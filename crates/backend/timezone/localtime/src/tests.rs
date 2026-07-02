use super::*;

// Differential fixture: macOS/Linux /usr/share/zoneinfo is the same IANA TZif
// format pg_TZDIR serves; goldens below were produced with `TZ=<zone> date -r
// <t>` (C tzcode).
const ZONEINFO: &str = "/usr/share/zoneinfo";

static SYNTH: std::sync::Mutex<Option<std::collections::HashMap<String, Vec<u8>>>> =
    std::sync::Mutex::new(None);

fn install_test_open_tzfile() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        pgtz_seams::pg_open_tzfile::set(|name, canonname, buf| {
            let Ok(name) = core::str::from_utf8(name) else {
                return Ok(None);
            };
            let synth = SYNTH.lock().unwrap();
            let bytes = match synth.as_ref().and_then(|m| m.get(name)).cloned() {
                Some(b) => b,
                None => {
                    drop(synth);
                    match std::fs::read(format!("{ZONEINFO}/{name}")) {
                        Ok(b) => b,
                        Err(_) => return Ok(None),
                    }
                }
            };
            let n = bytes.len().min(buf.len());
            buf[..n].copy_from_slice(&bytes[..n]);
            if let Some(c) = canonname {
                let nb = name.len().min(TZ_STRLEN_MAX);
                c[..nb].copy_from_slice(&name.as_bytes()[..nb]);
                c[nb] = 0;
            }
            Ok(Some(n))
        });
    });
}

fn register_synth(name: &str, bytes: Vec<u8>) {
    install_test_open_tzfile();
    SYNTH
        .lock()
        .unwrap()
        .get_or_insert_with(Default::default)
        .insert(name.to_string(), bytes);
}

fn load_zone(name: &str) -> PgTz {
    install_test_open_tzfile();
    let mut sp = Box::new(TzState::new());
    tzload(name.as_bytes(), None, &mut sp, true).expect("zone loads");
    PgTz::new(name.as_bytes(), *sp)
}

fn posix_tz(spec: &str) -> PgTz {
    let mut sp = Box::new(TzState::new());
    assert!(tzparse(spec.as_bytes(), &mut sp, false), "must parse: {spec}");
    PgTz::new(spec.as_bytes(), *sp)
}

fn ymdhms(tm: &PgTm<'_>) -> (i32, i32, i32, i32, i32, i32) {
    (
        tm.tm_year + TM_YEAR_BASE,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
    )
}

#[test]
fn gmt_lastditch_and_epoch() {
    let mut sp = TzState::new();
    assert!(tzparse(b"GMT", &mut sp, true));
    let gmt = PgTz::new(b"GMT", sp);
    assert_eq!(pg_get_timezone_offset(&gmt), Some(0));
    assert!(pg_tz_acceptable(&gmt));

    let tm = pg_localtime(0, &gmt).unwrap();
    assert_eq!(ymdhms(&tm), (1970, 1, 1, 0, 0, 0));
    assert_eq!(tm.tm_wday, 4);
    assert_eq!(tm.tm_zone, Some("GMT"));
}

#[test]
fn new_york_dst_boundaries_match_c_tzcode() {
    let ny = load_zone("America/New_York");

    let spring_before = pg_localtime(1_710_053_999, &ny).unwrap();
    assert_eq!(ymdhms(&spring_before), (2024, 3, 10, 1, 59, 59));
    assert_eq!(
        (spring_before.tm_isdst, spring_before.tm_gmtoff, spring_before.tm_zone),
        (0, -18_000, Some("EST"))
    );

    let spring_after = pg_localtime(1_710_054_000, &ny).unwrap();
    assert_eq!(ymdhms(&spring_after), (2024, 3, 10, 3, 0, 0));
    assert_eq!(
        (spring_after.tm_isdst, spring_after.tm_gmtoff, spring_after.tm_zone),
        (1, -14_400, Some("EDT"))
    );

    let fall_before = pg_localtime(1_730_613_599, &ny).unwrap();
    assert_eq!(ymdhms(&fall_before), (2024, 11, 3, 1, 59, 59));
    assert_eq!((fall_before.tm_isdst, fall_before.tm_gmtoff), (1, -14_400));

    let fall_after = pg_localtime(1_730_613_600, &ny).unwrap();
    assert_eq!(ymdhms(&fall_after), (2024, 11, 3, 1, 0, 0));
    assert_eq!((fall_after.tm_isdst, fall_after.tm_gmtoff), (0, -18_000));

    let epoch = pg_localtime(0, &ny).unwrap();
    assert_eq!(ymdhms(&epoch), (1969, 12, 31, 19, 0, 0));
    assert_eq!(epoch.tm_zone, Some("EST"));
}

#[test]
fn new_york_goahead_extrapolation() {
    // 2100 is past the transition table; served by the 400-year repeat
    // mapping. TZ=America/New_York date -r 4102462800 -> 2100-01-01 00:00 EST.
    let ny = load_zone("America/New_York");
    assert!(ny.state.goahead);
    let tm = pg_localtime(4_102_462_800, &ny).unwrap();
    assert_eq!(ymdhms(&tm), (2100, 1, 1, 0, 0, 0));
    assert_eq!((tm.tm_isdst, tm.tm_gmtoff, tm.tm_zone), (0, -18_000, Some("EST")));
}

#[test]
fn utc_zone_is_fixed() {
    let utc = load_zone("UTC");
    assert_eq!(pg_get_timezone_offset(&utc), Some(0));
    let tm = pg_localtime(1_710_054_000, &utc).unwrap();
    assert_eq!(ymdhms(&tm), (2024, 3, 10, 7, 0, 0));
    assert_eq!(tm.tm_zone, Some("UTC"));
    assert!(pg_tz_acceptable(&utc));
}

#[test]
fn lord_howe_half_hour_dst() {
    // Lord Howe's DST delta is 30 minutes; transition 2024-04-07 02:00 -> 01:30.
    let lh = load_zone("Australia/Lord_Howe");
    let before = pg_localtime(1_712_412_000, &lh).unwrap();
    assert_eq!((before.tm_hour, before.tm_min, before.tm_gmtoff), (1, 0, 39_600));
    let after = pg_localtime(1_712_415_600, &lh).unwrap();
    assert_eq!((after.tm_hour, after.tm_min, after.tm_gmtoff), (1, 30, 37_800));
}

#[test]
fn next_dst_boundary_new_york() {
    let ny = load_zone("America/New_York");
    // 2024-01-01 00:00 UTC; next boundary 2024-03-10 07:00 UTC.
    match pg_next_dst_boundary(1_704_067_200, &ny) {
        NextDstBoundary::Boundary(b) => {
            assert_eq!(b.boundary, 1_710_054_000);
            assert_eq!((b.before_gmtoff, b.before_isdst), (-18_000, 0));
            assert_eq!((b.after_gmtoff, b.after_isdst), (-14_400, 1));
        }
        other => panic!("expected boundary, got {other:?}"),
    }

    let utc = load_zone("UTC");
    assert!(matches!(
        pg_next_dst_boundary(1_704_067_200, &utc),
        NextDstBoundary::NoTransition {
            before_gmtoff: 0,
            before_isdst: 0
        }
    ));
}

#[test]
fn posix_zone_applies_us_default_rules() {
    let est = posix_tz("EST5EDT");
    // A bare STD/DST pair takes TZDEFRULESTRING (M3.2.0,M11.1.0): same 2024
    // boundaries as America/New_York.
    let winter = pg_localtime(1_704_085_200, &est).unwrap();
    assert_eq!((winter.tm_hour, winter.tm_isdst, winter.tm_gmtoff), (0, 0, -18_000));
    assert_eq!(winter.tm_zone, Some("EST"));
    let summer = pg_localtime(1_719_806_400, &est).unwrap();
    assert_eq!((summer.tm_hour, summer.tm_isdst, summer.tm_gmtoff), (0, 1, -14_400));
    assert_eq!(summer.tm_zone, Some("EDT"));
}

#[test]
fn posix_quoted_and_fixed_offsets() {
    let fixed = posix_tz("<+05>-5");
    assert_eq!(pg_get_timezone_offset(&fixed), Some(5 * 3600));
    let tm = pg_localtime(0, &fixed).unwrap();
    assert_eq!((tm.tm_hour, tm.tm_zone), (5, Some("+05")));

    // Empty STD abbrev allowed (unlike IANA); missing offset rejected.
    let mut sp = TzState::new();
    assert!(tzparse(b"<>5", &mut sp, false));
    assert!(tzparse(b"5", &mut sp, false));
    assert!(!tzparse(b"EST", &mut sp, false));
    // getsecs ranges: hours <= 167, minutes < 60, seconds <= 60.
    assert!(tzparse(b"FOO167", &mut sp, false));
    assert!(!tzparse(b"FOO168", &mut sp, false));
    assert!(tzparse(b"FOO5:59", &mut sp, false));
    assert!(!tzparse(b"FOO5:60", &mut sp, false));
    assert!(tzparse(b"FOO5:00:60", &mut sp, false));
    assert!(!tzparse(b"FOO5:00:61", &mut sp, false));
}

#[test]
fn abbrev_lookups() {
    let ny = load_zone("America/New_York");
    let (gmtoff, isdst) = pg_interpret_timezone_abbrev(b"EDT", 1_719_806_400, &ny).unwrap();
    assert_eq!((gmtoff, isdst), (-14_400, 1));
    assert!(pg_interpret_timezone_abbrev(b"XYZ", 0, &ny).is_none());

    let (isfixed, gmtoff, isdst) = pg_timezone_abbrev_is_known(b"EDT", &ny).unwrap();
    assert_eq!((isfixed, gmtoff, isdst), (true, -14_400, 1));

    let est = posix_tz("EST5EDT,M3.2.0,M11.1.0");
    let mut indx = 0;
    assert_eq!(pg_get_next_timezone_abbrev(&mut indx, &est), Some(b"EST".as_slice()));
    assert_eq!(pg_get_next_timezone_abbrev(&mut indx, &est), Some(b"EDT".as_slice()));
    assert_eq!(pg_get_next_timezone_abbrev(&mut indx, &est), None);
}

#[test]
fn pg_gmtime_matches_utc() {
    install_test_open_tzfile();
    let tm = pg_gmtime(951_868_800).unwrap();
    assert_eq!(ymdhms(&tm), (2000, 3, 1, 0, 0, 0));
    assert_eq!(tm.tm_zone, Some("GMT"));
}

#[test]
fn tzload_reports_canonical_name() {
    install_test_open_tzfile();
    let mut sp = Box::new(TzState::new());
    let mut canon = [0u8; TZ_STRLEN_MAX + 1];
    tzload(b"America/New_York", Some(&mut canon), &mut sp, true).unwrap();
    assert_eq!(cstr_bytes(&canon, 0), b"America/New_York");
    // Unknown name is NotFound.
    assert!(matches!(
        tzload(b"Not/A_Zone", None, &mut sp, true),
        Err(TzLoadError::Invalid) | Err(TzLoadError::NotFound)
    ));
}

// Synthetic version-1 TZif with one type and `charcnt` abbreviation bytes:
// header-validation probe (TZ_MAX_CHARS is strict).
fn synthetic_tzif_v1(charcnt: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"TZif");
    buf.push(0);
    buf.extend_from_slice(&[0u8; 15]);
    for c in [0i32, 0, 0, 0, 1, charcnt as i32] {
        buf.extend_from_slice(&c.to_be_bytes());
    }
    buf.extend_from_slice(&0i32.to_be_bytes());
    buf.push(0);
    buf.push(0);
    buf.extend(std::iter::repeat(b'A').take(charcnt));
    buf
}

#[test]
fn synthetic_header_bounds() {
    register_synth("synth-charcnt-100", synthetic_tzif_v1(100));
    register_synth("synth-charcnt-49", synthetic_tzif_v1(49));
    let mut sp = Box::new(TzState::new());
    assert!(matches!(
        tzload(b"synth-charcnt-100", None, &mut sp, false),
        Err(TzLoadError::Invalid)
    ));
    assert!(tzload(b"synth-charcnt-49", None, &mut sp, false).is_ok());
}
