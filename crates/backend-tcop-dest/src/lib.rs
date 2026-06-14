#![allow(non_snake_case)]
//! Port of `tcop/dest.c` — the `DestReceiver` virtual-dispatch router.
//!
//! # Model
//!
//! In C a `DestReceiver *` is a struct whose first four fields are the
//! `receiveSlot` / `rStartup` / `rShutdown` / `rDestroy` function pointers plus
//! a `mydest` `CommandDest` tag (`tcop/dest.h`). Output sites such as
//! `execTuples.c`'s `begin/do/end_tup_output` reach the receiver *only* through
//! those pointers — they never know the concrete receiver type. `dest.c` owns
//! the one routine that builds a receiver of the right kind
//! (`CreateDestReceiver`) and the static `donothingDR` used for `DestNone`.
//!
//! The owned model names a receiver by a process-global id
//! ([`DestReceiverHandle`]) into this crate's registry; each registered receiver
//! carries a real vtable ([`ReceiverVtable`]) of `rStartup`/`receiveSlot`/
//! `rShutdown` function pointers, mirroring the C struct's first three slots.
//! The three dispatch seams declared in `backend-tcop-dest-seams`
//! (`dest_rstartup` / `dest_receive_slot` / `dest_rshutdown`, called by
//! `execTuples.c` tuple output) are installed here and route through that vtable.
//!
//! # What is real here vs. mirror-and-panic
//!
//! Only `DestNone`'s receiver lives entirely inside `dest.c` (the static
//! `donothingDR`: every callback is a no-op). That receiver is built and
//! dispatched fully here — `ExecutePlan` with a discarding destination and
//! `SHOW` with no client both run through it end to end.
//!
//! Every *other* `CommandDest` (`DestRemote*`/`DestSPI`/`DestTuplestore`/
//! `DestTransientRel`/`DestTupleQueue`/…) has its callbacks owned by another
//! translation unit (printtup.c / spi.c / tstoreReceiver.c / matview.c /
//! tqueue.c …), each of which today keeps its *own* per-crate receiver registry
//! keyed by its *own* handle type. Unifying those into one router — so each
//! owner registers its vtable here — is the receiver-value keystone (the F0 of
//! the tcop-dest decomposition) and is out of scope for this dispatch-router
//! unit. Until that lands, `CreateDestReceiver` for those kinds registers a
//! vtable whose callbacks `panic!` honestly (mirror-and-panic), so a stray
//! dispatch through an un-wired kind fails loudly instead of silently dropping
//! tuples.

extern crate alloc;

use core::cell::RefCell;

use types_dest::CommandDest;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_tuple::heaptuple::TupleDescData;

/// The first three function-pointer slots of a C `DestReceiver` struct
/// (`tcop/dest.h`): `rStartup`, `receiveSlot`, `rShutdown`. (`rDestroy` is not
/// reached through the tuple-output dispatch seams; receiver teardown is the
/// owner's concern via its own `*_destroy` path.)
///
/// Modeled as plain `fn` pointers — the receiver kinds this router can serve
/// in-crate (`DestNone`) carry no captured state, exactly like the static
/// `donothingDR`.
#[derive(Clone, Copy)]
struct ReceiverVtable {
    /// `void (*rStartup)(DestReceiver *self, int operation, TupleDesc typeinfo)`.
    rStartup: fn(operation: CmdType, tupdesc: &TupleDescData<'_>) -> PgResult<()>,
    /// `bool (*receiveSlot)(TupleTableSlot *slot, DestReceiver *self)`.
    receiveSlot: fn(slot: &SlotData<'_>) -> PgResult<bool>,
    /// `void (*rShutdown)(DestReceiver *self)`.
    rShutdown: fn() -> PgResult<()>,
}

/// One registered receiver: its `mydest` tag plus its vtable. Mirrors the
/// owned-state head of a C `DestReceiver`.
#[derive(Clone, Copy)]
struct Receiver {
    /// `CommandDest mydest` — the C `DestReceiver`'s tag field. Carried
    /// faithfully (it is part of the struct) though the dispatch seams route
    /// purely through the vtable; `EndCommand`-style code that switches on
    /// `mydest` is `dest.c`'s protocol leg, ported separately.
    #[allow(dead_code)]
    mydest: CommandDest,
    vtable: ReceiverVtable,
}

// ===========================================================================
// donothing receiver (dest.c) — the DestNone callbacks.
// ===========================================================================

/// `donothingStartup(DestReceiver *self, int operation, TupleDesc typeinfo)`
/// (dest.c) — does nothing.
fn donothing_startup(_operation: CmdType, _tupdesc: &TupleDescData<'_>) -> PgResult<()> {
    Ok(())
}

/// `donothingReceive(TupleTableSlot *slot, DestReceiver *self)` (dest.c) —
/// returns `true`.
fn donothing_receive(_slot: &SlotData<'_>) -> PgResult<bool> {
    Ok(true)
}

/// `donothingCleanup(DestReceiver *self)` (dest.c) — used for both the shutdown
/// and destroy methods; does nothing.
fn donothing_cleanup() -> PgResult<()> {
    Ok(())
}

/// `static const DestReceiver donothingDR = { donothingReceive,
/// donothingStartup, donothingCleanup, donothingCleanup, DestNone }` (dest.c).
const DONOTHING_DR: Receiver = Receiver {
    mydest: CommandDest::None,
    vtable: ReceiverVtable {
        rStartup: donothing_startup,
        receiveSlot: donothing_receive,
        rShutdown: donothing_cleanup,
    },
};

// ===========================================================================
// unwired-owner receiver — honest mirror-and-panic for the kinds whose
// callbacks live in a not-yet-routed owner crate.
// ===========================================================================

fn unwired(mydest: CommandDest) -> ! {
    panic!(
        "DestReceiver of kind {mydest:?} is not wired into the tcop-dest router: \
         its callbacks live in another translation unit (printtup/spi/\
         tstoreReceiver/matview/tqueue/createas/copy/functions/explain_dr) that \
         must register its vtable here via the receiver-value keystone (F0 of \
         the tcop-dest decomposition), which has not landed yet"
    )
}

fn unwired_startup_remote(_op: CmdType, _td: &TupleDescData<'_>) -> PgResult<()> {
    unwired(CommandDest::Remote)
}
fn unwired_receive_remote(_slot: &SlotData<'_>) -> PgResult<bool> {
    unwired(CommandDest::Remote)
}
fn unwired_shutdown_remote() -> PgResult<()> {
    unwired(CommandDest::Remote)
}

/// Build the honest mirror-and-panic vtable for an un-routed receiver kind.
/// Every slot panics on dispatch, naming the kind and the missing keystone.
fn unwired_vtable() -> ReceiverVtable {
    ReceiverVtable {
        rStartup: unwired_startup_remote,
        receiveSlot: unwired_receive_remote,
        rShutdown: unwired_shutdown_remote,
    }
}

// ===========================================================================
// registry — names a live Receiver by DestReceiverHandle (a 1-based id; 0 is
// the C NULL sentinel `DestReceiverHandle::NULL`).
// ===========================================================================

struct Registry {
    slots: alloc::vec::Vec<Option<Receiver>>,
}

impl Registry {
    const fn new() -> Self {
        Self {
            slots: alloc::vec::Vec::new(),
        }
    }

    fn insert(&mut self, r: Receiver) -> DestReceiverHandle {
        if let Some(i) = self.slots.iter().position(Option::is_none) {
            self.slots[i] = Some(r);
            DestReceiverHandle((i + 1) as u64)
        } else {
            self.slots.push(Some(r));
            DestReceiverHandle(self.slots.len() as u64)
        }
    }

    fn get(&self, h: DestReceiverHandle) -> Receiver {
        debug_assert!(h.0 >= 1, "DestReceiverHandle 0 is the NULL sentinel");
        self.slots[(h.0 - 1) as usize].expect("live DestReceiver id")
    }
}

thread_local! {
    static REGISTRY: RefCell<Registry> = const { RefCell::new(Registry::new()) };
}

fn register(r: Receiver) -> DestReceiverHandle {
    REGISTRY.with(|c| c.borrow_mut().insert(r))
}

fn lookup(h: DestReceiverHandle) -> Receiver {
    REGISTRY.with(|c| c.borrow().get(h))
}

// ===========================================================================
// CreateDestReceiver (dest.c) — return the appropriate receiver for `dest`.
// ===========================================================================

/// `DestReceiver *CreateDestReceiver(CommandDest dest)` (dest.c): return the
/// receiver function set for `dest`, parked in the registry and named by the
/// returned id.
///
/// Only `DestNone` is served with its real (no-op) callbacks here; every other
/// kind is registered with the honest mirror-and-panic vtable until the
/// receiver-value keystone routes its owner's callbacks (see the module docs).
/// C's `pg_unreachable()` tail is unreachable here too — `CommandDest` is a
/// closed enum, every arm is covered.
pub fn CreateDestReceiver(dest: CommandDest) -> DestReceiverHandle {
    let receiver = match dest {
        CommandDest::None => DONOTHING_DR,
        // DestRemote / DestRemoteExecute       -> printtup_create_DR        (printtup.c)
        // DestRemoteSimple                     -> printsimpleDR             (printsimple.c)
        // DestDebug                            -> debugtupDR                (printtup.c)
        // DestSPI                              -> spi_printtupDR            (spi.c)
        // DestTuplestore                       -> CreateTuplestoreDestReceiver  (tstoreReceiver.c)
        // DestIntoRel                          -> CreateIntoRelDestReceiver (createas.c)
        // DestCopyOut                          -> CreateCopyDestReceiver    (copyto.c)
        // DestSqlFunction                      -> CreateSQLFunctionDestReceiver (functions.c)
        // DestTransientRel                     -> CreateTransientRelDestReceiver (matview.c)
        // DestTupleQueue                       -> CreateTupleQueueDestReceiver  (tqueue.c)
        // DestExplainSerialize                 -> CreateExplainSerializeDestReceiver (explain_dr.c)
        _ => Receiver {
            mydest: dest,
            vtable: unwired_vtable(),
        },
    };
    register(receiver)
}

/// `DestReceiver *None_Receiver` (dest.c) — the globally-available receiver for
/// `DestNone`. Each call mints a fresh registry id for the static no-op
/// receiver (the underlying callbacks are stateless, exactly like C's shared
/// `&donothingDR`).
pub fn none_receiver() -> DestReceiverHandle {
    register(DONOTHING_DR)
}

// ===========================================================================
// dispatch seam implementations (tcop/dest.h vtable dispatch).
// ===========================================================================

/// `dest->rStartup(dest, operation, tupdesc)` — route to the receiver's
/// `rStartup` callback.
fn dest_rstartup_impl(
    dest: DestReceiverHandle,
    operation: CmdType,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    (lookup(dest).vtable.rStartup)(operation, tupdesc)
}

/// `dest->receiveSlot(slot, dest)` — route to the receiver's `receiveSlot`
/// callback.
fn dest_receive_slot_impl(slot: &SlotData<'_>, dest: DestReceiverHandle) -> PgResult<bool> {
    (lookup(dest).vtable.receiveSlot)(slot)
}

/// `dest->rShutdown(dest)` — route to the receiver's `rShutdown` callback.
fn dest_rshutdown_impl(dest: DestReceiverHandle) -> PgResult<()> {
    (lookup(dest).vtable.rShutdown)()
}

/// Install this crate's inward seams. Wired into `seams-init`.
pub fn init_seams() {
    backend_tcop_dest_seams::dest_rstartup::set(dest_rstartup_impl);
    backend_tcop_dest_seams::dest_receive_slot::set(dest_receive_slot_impl);
    backend_tcop_dest_seams::dest_rshutdown::set(dest_rshutdown_impl);
    backend_tcop_dest_seams::create_dest_receiver::set(CreateDestReceiver);
}

#[cfg(test)]
mod tests;
