//! Tests for the `DestReceiver` dispatch router.

use super::*;

/// `init_seams()` installs all three dispatch seams.
#[test]
fn init_seams_installs_dispatch() {
    init_seams();
    assert!(backend_tcop_dest_seams::dest_rstartup::is_installed());
    assert!(backend_tcop_dest_seams::dest_receive_slot::is_installed());
    assert!(backend_tcop_dest_seams::dest_rshutdown::is_installed());
}

/// `CreateDestReceiver(DestNone)` mints a live handle whose `rShutdown` routes
/// to the no-op `donothingCleanup` (returns `Ok`), exercising registry lookup +
/// vtable dispatch end to end. `rShutdown` takes no slot/tupdesc, so this needs
/// no heavyweight executor state.
#[test]
fn donothing_shutdown_is_noop() {
    let h = CreateDestReceiver(CommandDest::None);
    assert!(h.0 >= 1, "handle must be a live, non-NULL id");
    assert_eq!(dest_rshutdown_impl(h), Ok(()));
}

/// `none_receiver()` is the `DestNone` shortcut; its shutdown is also a no-op.
#[test]
fn none_receiver_shutdown_is_noop() {
    let h = none_receiver();
    assert_eq!(dest_rshutdown_impl(h), Ok(()));
}

/// Each `CreateDestReceiver` mints a distinct registry id.
#[test]
fn distinct_handles() {
    let a = CreateDestReceiver(CommandDest::None);
    let b = CreateDestReceiver(CommandDest::None);
    assert_ne!(a, b);
}

/// An un-routed receiver kind dispatches to the honest mirror-and-panic vtable:
/// `rShutdown` panics naming the missing keystone, rather than silently
/// succeeding.
#[test]
#[should_panic(expected = "not wired into the tcop-dest router")]
fn unwired_kind_panics_on_dispatch() {
    let h = CreateDestReceiver(CommandDest::Remote);
    let _ = dest_rshutdown_impl(h);
}
