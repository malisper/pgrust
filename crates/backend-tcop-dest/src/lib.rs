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
//! ([`DestReceiverHandle`]) into this crate's *single* router registry. Each
//! registered receiver carries a real vtable ([`ReceiverVtable`]) of
//! `rStartup`/`receiveSlot`/`rShutdown` callbacks, mirroring the C struct's
//! first three slots, plus an owner-supplied `state` token (a `u64`).
//!
//! The `state` token is what makes the C `(DR_xxx *) self` downcast work in the
//! owned model: a stateless C `DestReceiver` (the static `donothingDR`) carries
//! no per-receiver state, but every stateful receiver (`DR_copy`, `DR_printtup`,
//! …) reaches its owned state through `self`. Here the owner stores its own
//! per-receiver state under a private key and hands that key to
//! [`register_dest_receiver`] as the `state` token; the vtable callbacks receive
//! that token back on every dispatch and use it to find their state — exactly
//! the C `(DR_xxx *) self->field` indirection, just keyed instead of cast.
//!
//! The three dispatch seams declared in `backend-tcop-dest-seams`
//! (`dest_rstartup` / `dest_receive_slot` / `dest_rshutdown`, called by
//! `execTuples.c` tuple output) are installed here and route through that vtable,
//! threading the receiver's `state` token to each callback.
//!
//! # What is real here vs. mirror-and-panic
//!
//! `DestNone`'s receiver lives entirely inside `dest.c` (the static
//! `donothingDR`: every callback is a no-op, carrying no state). That receiver
//! is built and dispatched fully here — `ExecutePlan` with a discarding
//! destination and `SHOW` with no client both run through it end to end.
//!
//! `DestCopyOut`'s receiver is owned by `copyto.c`. Its constructor
//! (`CreateCopyDestReceiver`) registers a real vtable into *this* router via the
//! [`backend_commands_copyto_seams::create_copy_dest_receiver`] seam, so
//! `CreateDestReceiver(DestCopyOut)` delegates to copyto exactly as the C switch
//! does — one unified registry, no per-owner side registry.
//!
//! Every *other* `CommandDest` (`DestRemote*`/`DestSPI`/`DestTuplestore`/
//! `DestTransientRel`/`DestTupleQueue`/…) has its callbacks owned by another
//! translation unit (printtup.c / spi.c / tstoreReceiver.c / matview.c /
//! tqueue.c …) that has not yet been re-homed onto this router. Until each
//! owner's constructor registers its vtable here (the same way copyto now does),
//! `CreateDestReceiver` for those kinds registers a vtable whose callbacks
//! `panic!` honestly (mirror-and-panic), so a stray dispatch through an un-wired
//! kind fails loudly instead of silently dropping tuples.

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
/// Each callback takes a leading `state: u64` argument — the owner-supplied
/// token registered alongside the vtable (see [`register_dest_receiver`]). It is
/// the owned-model stand-in for the C `(DR_xxx *) self` downcast: stateless
/// receivers (the static `donothingDR`) ignore it; stateful receivers
/// (`DR_copy`, …) use it to recover their per-receiver state. Modeled as plain
/// `fn` pointers (the callbacks themselves are stateless code; all per-receiver
/// state hangs off the token), so the vtable stays `Copy`.
#[derive(Clone, Copy)]
pub struct ReceiverVtable {
    /// `void (*rStartup)(DestReceiver *self, int operation, TupleDesc typeinfo)`.
    pub rStartup: fn(state: u64, operation: CmdType, tupdesc: &TupleDescData<'_>) -> PgResult<()>,
    /// `bool (*receiveSlot)(TupleTableSlot *slot, DestReceiver *self)`.
    pub receiveSlot: fn(state: u64, slot: &mut SlotData<'_>) -> PgResult<bool>,
    /// `void (*rShutdown)(DestReceiver *self)`.
    pub rShutdown: fn(state: u64) -> PgResult<()>,
}

/// One registered receiver: its `mydest` tag, its vtable, and the owner-supplied
/// `state` token. Mirrors the owned-state head of a C `DestReceiver`.
#[derive(Clone, Copy)]
struct Receiver {
    /// `CommandDest mydest` — the C `DestReceiver`'s tag field. Carried
    /// faithfully (it is part of the struct) though the dispatch seams route
    /// purely through the vtable; `EndCommand`-style code that switches on
    /// `mydest` is `dest.c`'s protocol leg, ported separately.
    #[allow(dead_code)]
    mydest: CommandDest,
    vtable: ReceiverVtable,
    /// Owner-supplied per-receiver state token (the `(DR_xxx *) self` stand-in;
    /// `0` for stateless receivers like `donothingDR`).
    state: u64,
}

// ===========================================================================
// donothing receiver (dest.c) — the DestNone callbacks.
// ===========================================================================

/// `donothingStartup(DestReceiver *self, int operation, TupleDesc typeinfo)`
/// (dest.c) — does nothing. Carries no state (the leading token is unused).
fn donothing_startup(
    _state: u64,
    _operation: CmdType,
    _tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    Ok(())
}

/// `donothingReceive(TupleTableSlot *slot, DestReceiver *self)` (dest.c) —
/// returns `true`.
fn donothing_receive(_state: u64, _slot: &mut SlotData<'_>) -> PgResult<bool> {
    Ok(true)
}

/// `donothingCleanup(DestReceiver *self)` (dest.c) — used for both the shutdown
/// and destroy methods; does nothing.
fn donothing_cleanup(_state: u64) -> PgResult<()> {
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
    state: 0,
};

// ===========================================================================
// unwired-owner receiver — honest mirror-and-panic for the kinds whose
// callbacks live in a not-yet-routed owner crate.
// ===========================================================================

fn unwired(mydest: CommandDest) -> ! {
    panic!(
        "DestReceiver of kind {mydest:?} is not wired into the tcop-dest router: \
         its callbacks live in another translation unit (printtup/spi/\
         tstoreReceiver/matview/tqueue/createas/functions/explain_dr) whose \
         constructor must register its vtable here via `register_dest_receiver` \
         — the way copyto's `CreateCopyDestReceiver` already does — which has \
         not landed for this kind yet"
    )
}

fn unwired_startup_remote(_state: u64, _op: CmdType, _td: &TupleDescData<'_>) -> PgResult<()> {
    unwired(CommandDest::Remote)
}
fn unwired_receive_remote(_state: u64, _slot: &mut SlotData<'_>) -> PgResult<bool> {
    unwired(CommandDest::Remote)
}
fn unwired_shutdown_remote(_state: u64) -> PgResult<()> {
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

/// Register an owner's real receiver vtable into the *one* router registry,
/// returning the [`DestReceiverHandle`] that names it.
///
/// This is the per-owner registration hook (the receiver-value keystone): an
/// owning translation unit (copyto.c / printtup.c / tstoreReceiver.c / …) builds
/// its receiver's per-receiver state under its own private key, then calls this
/// with `mydest`, the callback `vtable`, and that key as the `state` token. The
/// dispatch seams thread `state` back to the callbacks, so the owner recovers
/// its state exactly as C does via `(DR_xxx *) self`. The returned handle is the
/// `DestReceiver *` the rest of the executor carries.
///
/// `CreateDestReceiver` serves `DestNone` in-crate and delegates every other
/// kind to its owner's constructor, which calls this — so all receivers, from
/// every owner, live in this single registry (no per-owner side registry).
pub fn register_dest_receiver(
    mydest: CommandDest,
    vtable: ReceiverVtable,
    state: u64,
) -> DestReceiverHandle {
    register(Receiver {
        mydest,
        vtable,
        state,
    })
}

// ===========================================================================
// CreateDestReceiver (dest.c) — return the appropriate receiver for `dest`.
// ===========================================================================

/// `DestReceiver *CreateDestReceiver(CommandDest dest)` (dest.c): return the
/// receiver function set for `dest`, parked in the registry and named by the
/// returned id.
///
/// `DestNone` is served with its real (no-op) callbacks here. `DestCopyOut`
/// delegates to copyto's `CreateCopyDestReceiver` through the
/// `create_copy_dest_receiver` seam, which registers its real vtable into this
/// same router (mirroring the C switch's `CreateCopyDestReceiver()` call).
/// Every other kind is registered with the honest mirror-and-panic vtable until
/// its owner's constructor is likewise routed (see the module docs). C's
/// `pg_unreachable()` tail is unreachable here too — `CommandDest` is a closed
/// enum, every arm is covered.
pub fn CreateDestReceiver(dest: CommandDest) -> DestReceiverHandle {
    match dest {
        CommandDest::None => register(DONOTHING_DR),

        // DestCopyOut -> CreateCopyDestReceiver (copyto.c): the owner registers
        // its real vtable into this router and returns the resulting handle.
        CommandDest::CopyOut => backend_commands_copyto_seams::create_copy_dest_receiver::call(),

        // DestRemote / DestRemoteExecute       -> printtup_create_DR        (printtup.c)
        // DestRemoteSimple                     -> printsimpleDR             (printsimple.c)
        // DestDebug                            -> debugtupDR                (printtup.c)
        // DestSPI                              -> spi_printtupDR            (spi.c)
        // DestTuplestore                       -> CreateTuplestoreDestReceiver  (tstoreReceiver.c)
        // DestIntoRel                          -> CreateIntoRelDestReceiver (createas.c)
        // DestSqlFunction                      -> CreateSQLFunctionDestReceiver (functions.c)
        // DestTransientRel                     -> CreateTransientRelDestReceiver (matview.c)
        // DestTupleQueue                       -> CreateTupleQueueDestReceiver  (tqueue.c)
        // DestExplainSerialize                 -> CreateExplainSerializeDestReceiver (explain_dr.c)
        _ => register(Receiver {
            mydest: dest,
            vtable: unwired_vtable(),
            state: 0,
        }),
    }
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
/// `rStartup` callback, threading its `state` token.
fn dest_rstartup_impl(
    dest: DestReceiverHandle,
    operation: CmdType,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    let r = lookup(dest);
    (r.vtable.rStartup)(r.state, operation, tupdesc)
}

/// `dest->receiveSlot(slot, dest)` — route to the receiver's `receiveSlot`
/// callback, threading its `state` token.
fn dest_receive_slot_impl(slot: &mut SlotData<'_>, dest: DestReceiverHandle) -> PgResult<bool> {
    let r = lookup(dest);
    (r.vtable.receiveSlot)(r.state, slot)
}

/// `dest->rShutdown(dest)` — route to the receiver's `rShutdown` callback,
/// threading its `state` token.
fn dest_rshutdown_impl(dest: DestReceiverHandle) -> PgResult<()> {
    let r = lookup(dest);
    (r.vtable.rShutdown)(r.state)
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
