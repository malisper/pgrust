//! Constant / discriminant parity checks. Behavioral tests that touch the
//! shared state or barrier/CV protocol require the s-lock and
//! condition-variable seams installed and live in the workspace integration
//! suite, not here.

use super::pmsignal::{
    PMSignalReason, NUM_PMSIGNALS, PM_CHILD_ACTIVE, PM_CHILD_ASSIGNED, PM_CHILD_UNUSED,
    PM_CHILD_WALSENDER,
};
use super::signalfuncs::{
    SIGNAL_BACKEND_ERROR, SIGNAL_BACKEND_NOAUTOVAC, SIGNAL_BACKEND_NOPERMISSION,
    SIGNAL_BACKEND_NOSUPERUSER, SIGNAL_BACKEND_SUCCESS,
};

#[test]
fn pmsignal_reason_discriminants_match_header() {
    assert_eq!(PMSignalReason::PMSIGNAL_RECOVERY_STARTED as u32, 0);
    assert_eq!(PMSignalReason::PMSIGNAL_RECOVERY_CONSISTENT as u32, 1);
    assert_eq!(PMSignalReason::PMSIGNAL_BEGIN_HOT_STANDBY as u32, 2);
    assert_eq!(PMSignalReason::PMSIGNAL_ROTATE_LOGFILE as u32, 3);
    assert_eq!(PMSignalReason::PMSIGNAL_START_AUTOVAC_LAUNCHER as u32, 4);
    assert_eq!(PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER as u32, 5);
    assert_eq!(PMSignalReason::PMSIGNAL_BACKGROUND_WORKER_CHANGE as u32, 6);
    assert_eq!(PMSignalReason::PMSIGNAL_START_WALRECEIVER as u32, 7);
    assert_eq!(PMSignalReason::PMSIGNAL_ADVANCE_STATE_MACHINE as u32, 8);
    assert_eq!(PMSignalReason::PMSIGNAL_XLOG_IS_SHUTDOWN as u32, 9);
    assert_eq!(NUM_PMSIGNALS, 10);
}

#[test]
fn pm_child_states_match_header() {
    assert_eq!(PM_CHILD_UNUSED, 0);
    assert_eq!(PM_CHILD_ASSIGNED, 1);
    assert_eq!(PM_CHILD_ACTIVE, 2);
    assert_eq!(PM_CHILD_WALSENDER, 3);
}

#[test]
fn signal_backend_codes_match_header() {
    assert_eq!(SIGNAL_BACKEND_SUCCESS, 0);
    assert_eq!(SIGNAL_BACKEND_ERROR, 1);
    assert_eq!(SIGNAL_BACKEND_NOPERMISSION, 2);
    assert_eq!(SIGNAL_BACKEND_NOSUPERUSER, 3);
    assert_eq!(SIGNAL_BACKEND_NOAUTOVAC, 4);
}
