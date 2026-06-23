//! Tests for the `DestReceiver` dispatch router.

use super::*;

/// A throwaway per-query arena for exercising the mcx-threaded dispatch in
/// tests (the `rShutdown` path never allocates from it).
fn with_test_mcx<R>(f: impl for<'mcx> FnOnce(Mcx<'mcx>) -> R) -> R {
    let scratch = mcx::MemoryContext::new_bump("dest-router-test");
    f(scratch.mcx())
}

/// `init_seams()` installs all three dispatch seams.
#[test]
fn init_seams_installs_dispatch() {
    init_seams();
    assert!(dest_seams::dest_rstartup::is_installed());
    assert!(dest_seams::dest_receive_slot::is_installed());
    assert!(dest_seams::dest_rshutdown::is_installed());
}

/// `CreateDestReceiver(DestNone)` mints a live handle whose `rShutdown` routes
/// to the no-op `donothingCleanup` (returns `Ok`), exercising registry lookup +
/// vtable dispatch end to end. `rShutdown` takes no slot/tupdesc, so this needs
/// no heavyweight executor state.
#[test]
fn donothing_shutdown_is_noop() {
    let h = CreateDestReceiver(CommandDest::None);
    assert!(h.0 >= 1, "handle must be a live, non-NULL id");
    assert_eq!(with_test_mcx(|mcx| dest_rshutdown_impl(mcx, h)), Ok(()));
}

/// `none_receiver()` is the `DestNone` shortcut; its shutdown is also a no-op.
#[test]
fn none_receiver_shutdown_is_noop() {
    let h = none_receiver();
    assert_eq!(with_test_mcx(|mcx| dest_rshutdown_impl(mcx, h)), Ok(()));
}

/// Distinct stateful receivers get distinct registry ids (each
/// `register_dest_receiver` parks its own slot). `DestNone` is a separate case:
/// it is the shared static `None_Receiver` (see `none_receiver_is_shared`).
#[test]
fn distinct_handles() {
    let vtable = DONOTHING_DR.vtable;
    let a = register_dest_receiver(CommandDest::CopyOut, vtable, 0);
    let b = register_dest_receiver(CommandDest::CopyOut, vtable, 0);
    assert_ne!(a, b);
}

/// `CreateDestReceiver(DestNone)` / `none_receiver()` return the *same* cached
/// handle — the owned-model stand-in for C's shared static `&donothingDR` /
/// `None_Receiver`. Freeing it is a no-op (it is never reclaimed, like the C
/// static), so it stays valid for dispatch afterward.
#[test]
fn none_receiver_is_shared() {
    let a = CreateDestReceiver(CommandDest::None);
    let b = none_receiver();
    assert_eq!(a, b, "DestNone is the shared static receiver");
    // Freeing the shared None handle must not reclaim it.
    free_dest_receiver(a);
    assert_eq!(with_test_mcx(|mcx| dest_rshutdown_impl(mcx, a)), Ok(()));
    assert_eq!(none_receiver(), a, "None handle survives free");
}

/// A freed stateful receiver's slot is reused by the next registration (the
/// free-list pop), so the registry does not grow per create/destroy cycle. Free
/// is idempotent and safe on the NULL sentinel.
#[test]
fn free_list_reuses_slots() {
    let vtable = DONOTHING_DR.vtable;
    let h1 = register_dest_receiver(CommandDest::CopyOut, vtable, 11);
    free_dest_receiver(h1);
    // The just-freed slot is popped first by the next register.
    let h2 = register_dest_receiver(CommandDest::CopyOut, vtable, 22);
    assert_eq!(h1, h2, "freed slot is reused");
    assert_eq!(dest_receiver_state_token(h2), 22, "slot now holds new receiver");
    // Idempotent / NULL-safe.
    free_dest_receiver(h2);
    free_dest_receiver(h2);
    free_dest_receiver(DestReceiverHandle::NULL);
}

/// An un-routed receiver kind dispatches to the honest mirror-and-panic vtable:
/// `rShutdown` panics naming the missing keystone, rather than silently
/// succeeding. `DestSPI`'s owner (spi.c) is not yet routed into this router
/// (`DestRemote`/`DestRemoteExecute`/`DestDebug` now are — they delegate to
/// printtup's `printtup_create_dr` seam — so they no longer hit this path).
#[test]
#[should_panic(expected = "not wired into the tcop-dest router")]
fn unwired_kind_panics_on_dispatch() {
    let h = CreateDestReceiver(CommandDest::Spi);
    let _ = with_test_mcx(|mcx| dest_rshutdown_impl(mcx, h));
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
    fn capture_startup(
        _mcx: Mcx<'_>,
        _state: u64,
        _op: CmdType,
        _td: &TupleDescData<'_>,
    ) -> PgResult<()> {
        Ok(())
    }
    fn capture_receive(_mcx: Mcx<'_>, _state: u64, _slot: &mut SlotData<'_>) -> PgResult<bool> {
        Ok(true)
    }
    fn capture_shutdown(_mcx: Mcx<'_>, state: u64) -> PgResult<()> {
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
    assert_eq!(with_test_mcx(|mcx| dest_rshutdown_impl(mcx, h)), Ok(()));
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
