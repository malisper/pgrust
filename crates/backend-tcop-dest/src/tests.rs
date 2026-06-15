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

/// `register_dest_receiver` parks an owner's real vtable in the *one* router
/// registry and threads the owner's `state` token back to each callback on
/// dispatch — the receiver-value keystone hook. A fake owner registers a
/// `rShutdown` that records its token, confirming the `(DR_xxx *) self` stand-in
/// round-trips through the router.
#[test]
fn register_dest_receiver_threads_owner_state() {
    use core::cell::Cell;
    thread_local! {
        static SEEN_STATE: Cell<u64> = const { Cell::new(0) };
    }
    fn capture_startup(_state: u64, _op: CmdType, _td: &TupleDescData<'_>) -> PgResult<()> {
        Ok(())
    }
    fn capture_receive(_state: u64, _slot: &mut SlotData<'_>) -> PgResult<bool> {
        Ok(true)
    }
    fn capture_shutdown(state: u64) -> PgResult<()> {
        SEEN_STATE.with(|s| s.set(state));
        Ok(())
    }

    let vtable = ReceiverVtable {
        rStartup: capture_startup,
        receiveSlot: capture_receive,
        rShutdown: capture_shutdown,
    };
    let h = register_dest_receiver(CommandDest::CopyOut, vtable, 0xABCD);
    assert!(h.0 >= 1);
    assert_eq!(dest_rshutdown_impl(h), Ok(()));
    assert_eq!(SEEN_STATE.with(Cell::get), 0xABCD);
}

/// `CreateDestReceiver(DestCopyOut)` delegates to copyto's
/// `create_copy_dest_receiver` seam (mirroring the C switch's
/// `CreateCopyDestReceiver()` call). With the seam uninstalled it panics loudly
/// (the seam's own default), confirming the dispatch routes to the owner rather
/// than registering an in-crate placeholder.
#[test]
#[should_panic]
fn copyout_delegates_to_copyto_seam() {
    let _ = CreateDestReceiver(CommandDest::CopyOut);
}
