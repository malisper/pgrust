//! Tests for the genuinely in-crate walsender logic (no shmem / no seams).

use crate::core::WalSndState;
use crate::init::WalSndGetStateString;
use crate::stats::offset_to_interval;

#[test]
fn state_strings_match_c() {
    assert_eq!(WalSndGetStateString(WalSndState::WALSNDSTATE_STARTUP), "startup");
    assert_eq!(WalSndGetStateString(WalSndState::WALSNDSTATE_BACKUP), "backup");
    assert_eq!(WalSndGetStateString(WalSndState::WALSNDSTATE_CATCHUP), "catchup");
    assert_eq!(WalSndGetStateString(WalSndState::WALSNDSTATE_STREAMING), "streaming");
    assert_eq!(WalSndGetStateString(WalSndState::WALSNDSTATE_STOPPING), "stopping");
}

#[test]
fn offset_to_interval_is_microseconds_in_time_field() {
    let iv = offset_to_interval(1_234_567);
    assert_eq!(iv.month, 0);
    assert_eq!(iv.day, 0);
    assert_eq!(iv.time, 1_234_567);
}
