//! Unit tests for the bgwriter constants and the arithmetic helpers (the parts
//! with no cross-subsystem seam dependency).

use super::*;

#[test]
fn bgwriter_delay_defaults_to_200() {
    // The C `int BgWriterDelay = 200;` boot default.
    assert_eq!(BgWriterDelay(), 200);
}

#[test]
fn bgwriter_delay_round_trips() {
    set_BgWriterDelay(123);
    assert_eq!(BgWriterDelay(), 123);
    // Restore the default so other tests in this thread see the boot value.
    set_BgWriterDelay(200);
}

#[test]
fn hibernate_factor_matches_c() {
    // `#define HIBERNATE_FACTOR 50` (bgwriter.c:65).
    assert_eq!(HIBERNATE_FACTOR, 50);
}

#[test]
fn log_snapshot_interval_matches_c() {
    // `#define LOG_SNAPSHOT_INTERVAL_MS 15000` (bgwriter.c:71).
    assert_eq!(LOG_SNAPSHOT_INTERVAL_MS, 15000);
}

#[test]
fn timestamp_tz_plus_milliseconds_scales_to_microseconds() {
    // TimestampTz counts microseconds; adding N ms adds N*1000 us.
    assert_eq!(TimestampTzPlusMilliseconds(0, 0), 0);
    assert_eq!(TimestampTzPlusMilliseconds(0, 15000), 15_000_000);
    assert_eq!(TimestampTzPlusMilliseconds(1_000, 5), 6_000);
}

#[test]
fn hibernate_sleep_is_factor_times_delay() {
    // The long hibernate sleep is BgWriterDelay * HIBERNATE_FACTOR ms.
    set_BgWriterDelay(200);
    let hibernate_ms = BgWriterDelay() as i64 * HIBERNATE_FACTOR as i64;
    assert_eq!(hibernate_ms, 10_000);
}
