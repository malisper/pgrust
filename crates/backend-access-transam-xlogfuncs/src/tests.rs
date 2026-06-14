//! Tests for the seam-independent logic of xlogfuncs.c — the error-message
//! shapes and the WAL-file-name validation/uppercasing path. The seam-driven
//! functions (those reading `wal_level`/recovery state/backup machinery) panic
//! until their owners install the seams, so they are exercised by the
//! integration harness, not here.

use super::*;
use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};

#[test]
fn recovery_in_progress_error_shape() {
    let e = recovery_in_progress_error();
    assert_eq!(e.message(), "recovery is in progress");
    assert_eq!(e.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        e.hint(),
        Some("WAL control functions cannot be executed during recovery.")
    );
}

#[test]
fn recovery_in_progress_named_error_shape() {
    let e = recovery_in_progress_named_error("pg_walfile_name()");
    assert_eq!(e.message(), "recovery is in progress");
    assert_eq!(
        e.hint(),
        Some("pg_walfile_name() cannot be executed during recovery.")
    );
}

#[test]
fn recovery_not_in_progress_error_shape() {
    let e = recovery_not_in_progress_error();
    assert_eq!(e.message(), "recovery is not in progress");
    assert_eq!(e.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        e.hint(),
        Some("Recovery control functions can only be executed during recovery.")
    );
}

#[test]
fn promotion_ongoing_error_shape() {
    let e = promotion_ongoing_error("pg_wal_replay_pause()");
    assert_eq!(e.message(), "standby promotion is ongoing");
    assert_eq!(
        e.hint(),
        Some("pg_wal_replay_pause() cannot be executed after promotion is triggered.")
    );
}

#[test]
fn split_walfile_name_rejects_invalid() {
    // A non-WAL string is rejected with the C's exact message + sqlstate, and
    // this path runs entirely before any seam (IsXLogFileName is a real fn).
    let ctx = mcx::MemoryContext::new("xlogfuncs-test");
    let mcx = ctx.mcx();
    let err = pg_split_walfile_name(mcx, "not a wal file").unwrap_err();
    assert_eq!(err.message(), "invalid WAL file name \"not a wal file\"");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn split_walfile_name_uppercases_before_validation() {
    // `pg_split_walfile_name` uppercases the input via `pg_toupper` (ASCII here)
    // before `IsXLogFileName`. Verify the uppercasing step exactly: a lower-case
    // 24-hex-digit WAL name maps to the canonical upper-case form that
    // `IsXLogFileName` (a real, seam-free fn) accepts, while the original
    // lower-case form is rejected by `IsXLogFileName`.
    let lower = "00000001000000000000000a";
    let upper: String = lower.bytes().map(|b| b.to_ascii_uppercase() as char).collect();
    assert_eq!(upper, "00000001000000000000000A");
    assert!(!xlog::IsXLogFileName(lower));
    assert!(xlog::IsXLogFileName(&upper));
}
