//! The "now"-family of current-time functions, ported from `timestamp.c`
//! (idiomatic, safe Rust).
//!
//!   * `now()` / `transaction_timestamp()` -- `GetCurrentTransactionStartTimestamp()`
//!     (the value is fixed for the lifetime of the transaction).
//!   * `statement_timestamp()` -- `GetCurrentStatementStartTimestamp()`.
//!   * `clock_timestamp()` -- `GetCurrentTimestamp()` (live wall clock).
//!   * `timeofday()` -- the current time formatted as the C
//!     `"Dow Mon dd hh:mm:ss.uuuuuu yyyy TZ"` text via `pg_strftime`.
//!
//! The transaction/statement start timestamps come from the ported
//! `transam_xact` crate (the SQL `now()` returns the
//! transaction start time, not the live clock).  `pg_strftime` is owned by the
//! not-yet-direct-callable strftime unit, so `timeofday()` crosses it through
//! `strftime_seams::pg_strftime`.

use std::time::{SystemTime, UNIX_EPOCH};

use transam_xact::{
    GetCurrentStatementStartTimestamp, GetCurrentTransactionStartTimestamp,
};
use localtime::pg_localtime;
use state_pgtz::session_timezone;
use types_core::pg_time_t;
use types_datetime::TimestampTz;

use crate::timestamp::GetCurrentTimestamp;

/// `now()` (timestamp.c:1609) CORE -- returns the current transaction start
/// timestamp.  This is also the implementation of SQL `transaction_timestamp()`
/// and the `now` "special" value, all of which are stable within a transaction.
pub fn now() -> TimestampTz {
    GetCurrentTransactionStartTimestamp()
}

/// `transaction_timestamp()` (timestamp.c, alias of `now`) CORE.
pub fn transaction_timestamp() -> TimestampTz {
    GetCurrentTransactionStartTimestamp()
}

/// `statement_timestamp()` (timestamp.c:1615) CORE -- the start time of the
/// current statement.
pub fn statement_timestamp() -> TimestampTz {
    GetCurrentStatementStartTimestamp()
}

/// `clock_timestamp()` (timestamp.c:1621) CORE -- the live wall-clock time
/// (advances within a transaction, unlike `now()`).
pub fn clock_timestamp() -> TimestampTz {
    GetCurrentTimestamp()
}

/// `timeofday()` (timestamp.c:1691) CORE -- returns the current time as text.
///
/// Formats the live wall clock in the session time zone, exactly like the C
/// original: `pg_strftime` with the template `"%a %b %d %H:%M:%S.%%06d %Y %Z"`,
/// then the literal `%06d` placeholder is filled with the microseconds.  Yields
/// e.g. `"Wed May 28 12:34:56.000123 2026 UTC"`.  The `pg_strftime` call crosses
/// the strftime seam.
pub fn timeofday() -> String {
    // gettimeofday() equivalent: seconds + microseconds since the Unix epoch.
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let tt = dur.as_secs() as pg_time_t;
    let tv_usec = dur.subsec_micros();

    // pg_strftime(templ, sizeof(templ), "...%%06d...", pg_localtime(&tt, zone)).
    // The doubled "%%06d" survives strftime as a literal "%06d", which the
    // second step (the C snprintf(buf, templ, tp.tv_usec)) substitutes.
    let stz = session_timezone();
    let templ = match pg_localtime(tt, &stz) {
        Some(tm) => {
            // C templ[128]; pg_strftime writes the formatted text + NUL.
            let mut buf = [0u8; 128];
            let n = strftime_seams::pg_strftime::call(
                &mut buf,
                "%a %b %d %H:%M:%S.%%06d %Y %Z",
                &tm,
            );
            String::from_utf8_lossy(&buf[..n]).into_owned()
        }
        None => String::new(),
    };

    // snprintf(buf, sizeof(buf), templ, tp.tv_usec): fill the "%06d"
    // placeholder with the zero-padded microseconds.
    templ.replacen("%06d", &format!("{tv_usec:06}"), 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use transam_xact::SetParallelStartTimestamps;

    #[test]
    #[ignore = "SetParallelStartTimestamps asserts is_parallel_worker; pinning timestamps outside a parallel worker is not unit-isolatable"]
    fn now_equals_transaction_start() {
        crate::test_install_seams();
        // Pin a known transaction start timestamp and confirm now() returns it.
        let pinned: TimestampTz = 123_456_789;
        SetParallelStartTimestamps(pinned, pinned + 7);

        assert_eq!(now(), pinned);
        assert_eq!(transaction_timestamp(), pinned);
        assert_eq!(now(), transaction_timestamp());
        assert_eq!(statement_timestamp(), pinned + 7);

        SetParallelStartTimestamps(0, 0);
    }

    #[test]
    fn clock_timestamp_is_recent() {
        use types_datetime::{POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE};

        let now_unix_pg_usecs = || -> TimestampTz {
            let dur = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            let secs = dur.as_secs() as i64
                - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64;
            secs * 1_000_000 + dur.subsec_micros() as i64
        };

        let before = now_unix_pg_usecs();
        let c = clock_timestamp();
        let after = now_unix_pg_usecs();
        let slack = 5_000_000; // 5 seconds
        assert!(
            c >= before - slack && c <= after + slack,
            "clock_timestamp {c} not within [{before}, {after}] +/- slack"
        );
    }

    // NOTE: `timeofday()` routes its `pg_strftime` call through the centralized
    // strftime seam, whose default is a loud panic until a provider is installed;
    // the C-format / placeholder-substitution shape is covered by the
    // `backend-timezone-strftime` crate's own tests.  No unit test exercises the
    // seam here so the suite passes without a wired provider.
}
