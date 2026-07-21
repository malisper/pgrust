use super::*;

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
}

#[test]
fn gmt_needs_no_filesystem_and_caches() {
    setup();
    let gmt = pg_tzset(b"GMT").expect("GMT always parses");
    assert_eq!(gmt.name(), b"GMT");
    assert_eq!(localtime::pg_get_timezone_offset(gmt), Some(0));
    let again = pg_tzset(b"gmt").expect("case-insensitive cache hit");
    assert!(core::ptr::eq(gmt, again), "same leaked entry");
}

#[test]
fn timezone_initialize_sets_globals() {
    setup();
    assert!(session_timezone().is_none() || session_timezone().is_some());
    pg_timezone_initialize();
    let s = session_timezone().unwrap();
    let l = log_timezone().unwrap();
    assert_eq!(s.name(), b"GMT");
    assert!(core::ptr::eq(s, l));
}

#[test]
fn tzset_loads_real_zone_case_insensitively() {
    setup();
    let ny = pg_tzset(b"america/new_york").expect("scan_directory_ci resolves case");
    assert_eq!(ny.name(), b"America/New_York");

    let tm = localtime::pg_localtime(1_710_054_000, ny).unwrap();
    assert_eq!((tm.tm_hour, tm.tm_isdst, tm.tm_gmtoff), (3, 1, -14_400));
    assert_eq!(tm.tm_zone, Some("EDT"));

    let again = pg_tzset(b"AMERICA/NEW_YORK").unwrap();
    assert!(core::ptr::eq(ny, again));

    assert!(pg_tzset(b"Not/A/Zone").is_none());
    let too_long = [b'a'; TZ_STRLEN_MAX + 1];
    assert!(pg_tzset(&too_long).is_none());
}

#[test]
fn tzset_posix_spec_upcases_canonical() {
    setup();
    let est = pg_tzset(b"est5edt").expect("POSIX spec parses");
    assert_eq!(est.name(), b"EST5EDT");
    let summer = localtime::pg_localtime(1_719_806_400, est).unwrap();
    assert_eq!((summer.tm_isdst, summer.tm_gmtoff), (1, -14_400));
}

#[test]
fn tzset_offset_builds_iso_abbreviation() {
    setup();
    // Positive = west of Greenwich (POSIX), ISO sign in the abbreviation.
    let west = pg_tzset_offset(5 * 3600).unwrap();
    assert_eq!(west.name(), b"<-05>+05");
    assert_eq!(localtime::pg_get_timezone_offset(west), Some(-5 * 3600));

    let east = pg_tzset_offset(-(4 * 3600 + 30 * 60)).unwrap();
    assert_eq!(east.name(), b"<+04:30>-04:30");
    assert_eq!(localtime::pg_get_timezone_offset(east), Some(4 * 3600 + 30 * 60));

    let odd = pg_tzset_offset(-(3600 + 61)).unwrap();
    assert_eq!(odd.name(), b"<+01:01:01>-01:01:01");
}

// Regression: the pg_timezone_abbrevs clock.rs:32 panic (fleet job
// pgrust-fast-tests-18ae4c1cf2-1784615648-0f06). DynamicZoneAbbrev caches
// `&'static PgTz` in the PROCESS-shared zone-abbreviation table, so pg_tzset
// pointers must be process-permanent — one entry per zone for every thread,
// still valid after the resolving session/thread is gone. The old
// session-arena cache handed out a different, session-lifetime pointer per
// thread; the first resolver's death left the shared cache dangling and
// localsub read garbage `defaulttype`.
#[test]
fn tzset_pointers_are_process_permanent_across_threads() {
    setup();
    let from_thread = std::thread::spawn(|| {
        pg_tzset(b"America/Montevideo").expect("zone loads") as *const PgTz as usize
    })
    .join()
    .unwrap();
    let here = pg_tzset(b"America/Montevideo").expect("zone loads");
    assert_eq!(
        from_thread, here as *const PgTz as usize,
        "one permanent cache entry per zone, process-wide"
    );
    // The panic path: localsub -> ttis[defaulttype]. Prove the shared pointee
    // is alive and coherent after the resolving thread exited.
    let tm = localtime::pg_localtime(1_710_054_000, here).unwrap();
    assert_eq!(tm.tm_zone, Some("-03"));
}

#[test]
fn enumerate_walks_the_tree() {
    setup();
    let mut e = pg_tzenumerate_start().unwrap();
    let mut count = 0usize;
    let mut saw_ny = false;
    while let Some(tz) = pg_tzenumerate_next(&mut e).unwrap() {
        count += 1;
        if tz.name() == b"America/New_York" {
            saw_ny = true;
        }
    }
    pg_tzenumerate_end(e).unwrap();
    assert!(saw_ny, "America/New_York must be enumerated");
    assert!(count > 100, "expected a real tz tree, got {count}");
}
