use super::*;

// PGRUST_TZDIR is read once (pg_tzdir's OnceLock), so the whole process
// enumerates ONE tree: a scratch mirror of /usr/share/zoneinfo (one symlink
// per top-level entry) that tests may plant their own entries in without
// touching the system tree. Returns that directory.
fn setup() -> &'static str {
    static TZDIR: OnceLock<String> = OnceLock::new();
    TZDIR.get_or_init(|| {
        let dir = format!(
            "{}/pgtz-tests-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for entry in std::fs::read_dir("/usr/share/zoneinfo").unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            std::os::unix::fs::symlink(entry.path(), format!("{dir}/{}", name.to_string_lossy()))
                .unwrap();
        }
        std::env::set_var("PGRUST_TZDIR", &dir);
        init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        dir
    })
}

// The enumeration witnesses plant entries in the shared scratch tree; the
// whole-tree walk must not observe another test's plant.
static ENUM_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn gmt_needs_no_filesystem_and_caches() {
    setup();
    let gmt = pg_tzset(b"GMT").unwrap().expect("GMT always parses");
    assert_eq!(gmt.name(), b"GMT");
    assert_eq!(localtime::pg_get_timezone_offset(gmt), Some(0));
    let again = pg_tzset(b"gmt").unwrap().expect("case-insensitive cache hit");
    assert!(core::ptr::eq(gmt, again), "same leaked entry");
}

#[test]
fn timezone_initialize_sets_globals() {
    setup();
    assert!(session_timezone().is_none() || session_timezone().is_some());
    pg_timezone_initialize().unwrap();
    let s = session_timezone().unwrap();
    let l = log_timezone().unwrap();
    assert_eq!(s.name(), b"GMT");
    assert!(core::ptr::eq(s, l));
}

#[test]
fn tzset_loads_real_zone_case_insensitively() {
    setup();
    let ny = pg_tzset(b"america/new_york").unwrap().expect("scan_directory_ci resolves case");
    assert_eq!(ny.name(), b"America/New_York");

    let tm = localtime::pg_localtime(1_710_054_000, ny).unwrap();
    assert_eq!((tm.tm_hour, tm.tm_isdst, tm.tm_gmtoff), (3, 1, -14_400));
    assert_eq!(tm.tm_zone, Some("EDT"));

    let again = pg_tzset(b"AMERICA/NEW_YORK").unwrap().unwrap();
    assert!(core::ptr::eq(ny, again));

    assert!(pg_tzset(b"Not/A/Zone").unwrap().is_none());
    let too_long = [b'a'; TZ_STRLEN_MAX + 1];
    assert!(pg_tzset(&too_long).unwrap().is_none());
}

#[test]
fn tzset_posix_spec_upcases_canonical() {
    setup();
    let est = pg_tzset(b"est5edt").unwrap().expect("POSIX spec parses");
    assert_eq!(est.name(), b"EST5EDT");
    let summer = localtime::pg_localtime(1_719_806_400, est).unwrap();
    assert_eq!((summer.tm_isdst, summer.tm_gmtoff), (1, -14_400));
}

#[test]
fn tzset_offset_builds_iso_abbreviation() {
    setup();
    // Positive = west of Greenwich (POSIX), ISO sign in the abbreviation.
    let west = pg_tzset_offset(5 * 3600).unwrap().unwrap();
    assert_eq!(west.name(), b"<-05>+05");
    assert_eq!(localtime::pg_get_timezone_offset(west), Some(-5 * 3600));

    let east = pg_tzset_offset(-(4 * 3600 + 30 * 60)).unwrap().unwrap();
    assert_eq!(east.name(), b"<+04:30>-04:30");
    assert_eq!(localtime::pg_get_timezone_offset(east), Some(4 * 3600 + 30 * 60));

    let odd = pg_tzset_offset(-(3600 + 61)).unwrap().unwrap();
    assert_eq!(odd.name(), b"<+01:01:01>-01:01:01");
}

// Regression: the pg_timezone_abbrevs clock.rs:32 panic (CI cluster job
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
        pg_tzset(b"America/Montevideo").unwrap().expect("zone loads") as *const PgTz as usize
    })
    .join()
    .unwrap();
    let here = pg_tzset(b"America/Montevideo").unwrap().expect("zone loads");
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
    let _walk = ENUM_LOCK.lock().unwrap_or_else(|p| p.into_inner());
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

// audit-18.6 b195: check_timezone's numeric arm hands pg_tzset_offset the
// (long) cast of -hours*3600, which for "inf" is LONG_MIN. C's -LONG_MIN
// wraps into an unparsable "<+-...>" name and pg_tzset returns NULL; the
// port must fail the same way instead of panicking on the negation.
#[test]
fn tzset_offset_long_min_fails_without_panic() {
    setup();
    assert!(pg_tzset_offset(i64::MIN).unwrap().is_none());
}

// audit-18.6 b254 (pgtz.c:283): tzload's directory scan is C AllocateDir,
// whose reserveAllocatedDesc refusal is an ereport(ERROR) that unwinds
// through pg_tzset ("exceeded maxAllocatedDescs (%d) while trying to open
// directory \"%s\"", fd.c:2916). The port must surface that same error to
// the caller, not panic ("pgtz: ereport escaped pg_tzset"). Live pair: 18
// nested COPY FROM file levels then an uncached zone in a timestamptz
// literal -> C ERROR 53000, pgrust backend panic.

#[test]
fn tzset_surfaces_allocate_dir_refusal_as_error() {
    let dir = setup();
    // This thread's fd cache boots at the FD_MINFREE default: 16 allocated
    // descriptors, then reserveAllocatedDesc refuses (fd tests, fdcap).
    let mut held = Vec::new();
    for _ in 0..16 {
        held.push(fd::AllocateDir(dir).unwrap().expect("scratch tzdir opens"));
    }
    let refused = fd::AllocateDir(dir).unwrap_err();
    assert!(
        refused
            .message()
            .contains("exceeded maxAllocatedDescs (16) while trying to open directory"),
        "{}",
        refused.message()
    );

    // An uncached zone: pg_tzset -> tzload -> scan_directory_ci -> AllocateDir.
    let outcome = pg_tzset(b"Pacific/Auckland");
    for d in held {
        fd::FreeDir(Some(d)).unwrap();
    }
    let err = match outcome {
        Ok(_) => panic!("the descriptor-cap refusal must unwind as ERROR"),
        Err(err) => err,
    };
    assert_eq!(
        err.message(),
        format!("exceeded maxAllocatedDescs (16) while trying to open directory \"{dir}\"")
    );
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INSUFFICIENT_RESOURCES);
}

// audit-18.6 b254 (pgtz.c:450 -> common/file_utils.c:592 get_dirent_type):
// a stat failure during the enumeration walk is ereport(ERROR,
// errcode_for_file_access(), "could not stat file \"%s\": %m") -- strerror
// text only. Live pair, broken symlink in the tz tree + pg_timezone_names():
// C `...: No such file or directory`, pgrust `...: No such file or directory
// (os error 2)` (std::io::Error Display).
#[test]
fn enumerate_reports_stat_failure_with_strerror_text() {
    let dir = setup();
    let _walk = ENUM_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let link = format!("{dir}/zz-b254-broken");
    let _ = std::fs::remove_file(&link);
    std::os::unix::fs::symlink("/nonexistent/b254-target", &link).unwrap();

    let mut e = pg_tzenumerate_start().unwrap();
    let mut outcome = Ok(());
    loop {
        match pg_tzenumerate_next(&mut e) {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(err) => {
                outcome = Err(err);
                break;
            }
        }
    }
    pg_tzenumerate_end(e).unwrap();
    std::fs::remove_file(&link).unwrap();

    let err = outcome.expect_err("a broken symlink fails the walk with ERROR");
    assert_eq!(
        err.message(),
        format!("could not stat file \"{link}\": No such file or directory")
    );
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_UNDEFINED_FILE);
}
