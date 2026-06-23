use super::*;

#[test]
fn formats_elapsed_resource_usage() {
    // user:    4.560 - 1.300   => 3 s, 26 centis (260000 usec / 10000)
    // system:  5.780 - 2.400   => 3 s, 38 centis
    // elapsed: 15.450 - 10.200 => 5 s, 25 centis
    let start = PgRUsage::from_parts(
        Timeval::new(10, 200_000),
        Timeval::new(1, 300_000),
        Timeval::new(2, 400_000),
    );
    let end = PgRUsage::from_parts(
        Timeval::new(15, 450_000),
        Timeval::new(4, 560_000),
        Timeval::new(5, 780_000),
    );

    assert_eq!(
        pg_rusage_show_between(&start, &end),
        "CPU: user: 3.26 s, system: 3.38 s, elapsed: 5.25 s"
    );
}

#[test]
fn borrows_microseconds_when_needed() {
    // Each end.tv_usec < start.tv_usec, so the borrow-a-second fixup fires:
    // sec - 1 and usec + 1_000_000, yielding 0 s, 20 centis each.
    let start = PgRUsage::from_parts(
        Timeval::new(10, 900_000),
        Timeval::new(1, 900_000),
        Timeval::new(2, 900_000),
    );
    let end = PgRUsage::from_parts(
        Timeval::new(11, 100_000),
        Timeval::new(2, 100_000),
        Timeval::new(3, 100_000),
    );

    assert_eq!(
        pg_rusage_show_between(&start, &end),
        "CPU: user: 0.20 s, system: 0.20 s, elapsed: 0.20 s"
    );
}

#[test]
fn exact_borrow_boundary_no_fixup() {
    // Equal usec => no borrow; whole-second deltas, zero centis.
    let start = PgRUsage::from_parts(
        Timeval::new(0, 500_000),
        Timeval::new(0, 500_000),
        Timeval::new(0, 500_000),
    );
    let end = PgRUsage::from_parts(
        Timeval::new(7, 500_000),
        Timeval::new(3, 500_000),
        Timeval::new(9, 500_000),
    );

    assert_eq!(
        pg_rusage_show_between(&start, &end),
        "CPU: user: 3.00 s, system: 9.00 s, elapsed: 7.00 s"
    );
}

#[test]
fn centis_truncates_toward_zero() {
    // usec delta 99_999 / 10_000 == 9 (truncated); not rounded to 10.
    let start = PgRUsage::from_parts(
        Timeval::new(0, 0),
        Timeval::new(0, 0),
        Timeval::new(0, 0),
    );
    let end = PgRUsage::from_parts(
        Timeval::new(0, 99_999),
        Timeval::new(0, 99_999),
        Timeval::new(0, 99_999),
    );

    assert_eq!(
        pg_rusage_show_between(&start, &end),
        "CPU: user: 0.09 s, system: 0.09 s, elapsed: 0.09 s"
    );
}

#[test]
fn pg_rusage_init_populates_snapshot() {
    let mut ru = PgRUsage::default();
    pg_rusage_init(&mut ru);
    // The wall clock must be a plausible Unix-epoch read with usec in range.
    assert!(ru.tv.tv_sec > 0);
    assert!((0..1_000_000).contains(&ru.tv.tv_usec));
    assert!((0..1_000_000).contains(&ru.ru_utime.tv_usec));
    assert!((0..1_000_000).contains(&ru.ru_stime.tv_usec));
}

#[test]
fn live_capture_and_show_runs() {
    let start = pg_rusage_new();
    let s = pg_rusage_show(&start);
    assert!(s.starts_with("CPU: user: "));
    assert!(s.ends_with(" s"));
}
