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
//! # The mcx-vtable keystone
//!
//! Each vtable callback also takes a leading `mcx: Mcx<'mcx>` — the per-query
//! arena, threaded in per-dispatch by the caller (execMain recovers it from
//! `estate.es_query_cxt`; execTuples/pquery from their output context). This is
//! the same mcx-vtable threading the tableam routine uses
//! ([`types_tableam::tableam::TableAmRoutine`]): the callbacks are HRTB
//! (`for<'mcx> fn(Mcx<'mcx>, …)`) so the vtable stays `Copy`/lifetime-free and
//! lives in the `'static` registry, while a receiver that needs the arena gets
//! it on every call. Without it a receiver could not express an `'mcx`-bound
//! sink: `intorel_receive` (`createas`) must call
//! `table_tuple_insert(mcx, &rel, slot, …)`, which requires both an `Mcx<'mcx>`
//! and a `&mut SlotData<'mcx>`. With this contract, an `intorel`-style receiver
//! opens its target relation + `BulkInsertState` in `rStartup` (binding a raw
//! pointer to that `'mcx` state under its `state` token, the way `copyto` binds
//! its `cstate` for the run), then in `receiveSlot` recovers that state and
//! drives `table_tuple_insert` with the threaded `mcx` — the createas blocker is
//! now expressible. (createas itself is not ported here.)
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

use mcx::Mcx;
use types_dest::CommandDest;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_portal::{CommandTag, QueryCompletion};
use types_tuple::heaptuple::TupleDescData;

// Protocol message-type bytes (`protocol.h` / `PqMsg_*`). Defined locally,
// mirroring the per-crate convention in `printtup`/`explain_dr`/`auth` (there is
// no central protocol module in the workspace yet).

/// `PqMsg_CommandComplete` (`protocol.h`).
const PqMsg_CommandComplete: u8 = b'C';
/// `PqMsg_ReadyForQuery` (`protocol.h`).
const PqMsg_ReadyForQuery: u8 = b'Z';
/// `PqMsg_EmptyQueryResponse` (`protocol.h`).
const PqMsg_EmptyQueryResponse: u8 = b'I';

/// The first three function-pointer slots of a C `DestReceiver` struct
/// (`tcop/dest.h`): `rStartup`, `receiveSlot`, `rShutdown`. (`rDestroy` is not
/// reached through the tuple-output dispatch seams; receiver teardown is the
/// owner's concern via its own `*_destroy` path.)
///
/// Each callback takes a leading `mcx: Mcx<'mcx>` (the per-query arena the
/// receiver works in — the DestReceiver mcx-vtable keystone, mirroring
/// [`types_tableam::tableam::TableAmRoutine`]) and a `state: u64` token — the
/// owner-supplied key registered alongside the vtable (see
/// [`register_dest_receiver`]). The token is the owned-model stand-in for the C
/// `(DR_xxx *) self` downcast: stateless receivers (the static `donothingDR`)
/// ignore both; stateful receivers (`DR_copy`, `DR_intorel`, …) use the token to
/// recover their per-receiver state (a raw pointer bound around the run by their
/// owner's driver, the way `copyto` binds its `cstate`) and the `mcx` to express
/// `'mcx`-bound sinks (`intorel_receive`'s `table_tuple_insert(mcx, &rel, …)`).
///
/// The callbacks are HRTB (`for<'mcx> fn(Mcx<'mcx>, …)`) so the vtable stays
/// `Copy`/lifetime-free and can live in the `'static` router registry, exactly
/// like the tableam vtable: the `'mcx` flows in per-dispatch from the caller, it
/// is not baked into the function-pointer type.
#[derive(Clone, Copy)]
pub struct ReceiverVtable {
    /// `void (*rStartup)(DestReceiver *self, int operation, TupleDesc typeinfo)`.
    pub rStartup: for<'mcx> fn(
        mcx: Mcx<'mcx>,
        state: u64,
        operation: CmdType,
        tupdesc: &TupleDescData<'mcx>,
    ) -> PgResult<()>,
    /// `bool (*receiveSlot)(TupleTableSlot *slot, DestReceiver *self)`.
    pub receiveSlot:
        for<'mcx> fn(mcx: Mcx<'mcx>, state: u64, slot: &mut SlotData<'mcx>) -> PgResult<bool>,
    /// `void (*rShutdown)(DestReceiver *self)`.
    pub rShutdown: for<'mcx> fn(mcx: Mcx<'mcx>, state: u64) -> PgResult<()>,
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
    _mcx: Mcx<'_>,
    _state: u64,
    _operation: CmdType,
    _tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    Ok(())
}

/// `donothingReceive(TupleTableSlot *slot, DestReceiver *self)` (dest.c) —
/// returns `true`.
fn donothing_receive(_mcx: Mcx<'_>, _state: u64, _slot: &mut SlotData<'_>) -> PgResult<bool> {
    Ok(true)
}

/// `donothingCleanup(DestReceiver *self)` (dest.c) — used for both the shutdown
/// and destroy methods; does nothing.
fn donothing_cleanup(_mcx: Mcx<'_>, _state: u64) -> PgResult<()> {
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

fn unwired_startup_remote(
    _mcx: Mcx<'_>,
    _state: u64,
    _op: CmdType,
    _td: &TupleDescData<'_>,
) -> PgResult<()> {
    unwired(CommandDest::Remote)
}
fn unwired_receive_remote(_mcx: Mcx<'_>, _state: u64, _slot: &mut SlotData<'_>) -> PgResult<bool> {
    unwired(CommandDest::Remote)
}
fn unwired_shutdown_remote(_mcx: Mcx<'_>, _state: u64) -> PgResult<()> {
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

/// Return the owner-supplied `state` token a receiver was registered with (the
/// `(DR_xxx *) self` stand-in). An owner whose post-construction setup
/// (`SetRemoteDestReceiverParams`) is handed the router [`DestReceiverHandle`]
/// rather than its own token uses this to recover the token and reach its
/// per-receiver state — the owned-model equivalent of the C `(DR_xxx *) self`
/// downcast done from the bare `DestReceiver *`.
pub fn dest_receiver_state_token(dest: DestReceiverHandle) -> u64 {
    lookup(dest).state
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

        // DestRemote / DestRemoteExecute -> printtup_create_DR (printtup.c): the
        // owner registers its real vtable into this router (the same delegation
        // copyto uses), so a SELECT to a wire client emits RowDescription +
        // DataRow messages through `printtup`'s receiveSlot.
        CommandDest::Remote | CommandDest::RemoteExecute => {
            backend_access_common_printtup_seams::printtup_create_dr::call(dest)
        }

        // DestDebug -> &debugtupDR (dest.c:133): the static debugtup receiver,
        // whose callbacks (`debugtup` / `debugStartup`) live in printtup.c. The
        // standalone (`--single`) backend's `whereToSendOutput = DestDebug`
        // routes SELECT output here, printing each tuple to stdout. The owner
        // registers its real vtable into this router via the seam.
        CommandDest::Debug => {
            backend_access_common_printtup_seams::create_debug_dest_receiver::call()
        }

        // DestSPI -> spi_printtupDR (spi.c): the SPI owner registers its real
        // collecting vtable (spi_dest_startup / spi_printtup) into this router
        // via the seam, the same delegation copyto/printtup use.
        CommandDest::Spi => backend_executor_spi_seams::create_spi_dest_receiver::call(),

        // DestRemoteSimple                     -> printsimpleDR             (printsimple.c)
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
/// `rStartup` callback, threading the per-query `mcx` and its `state` token.
fn dest_rstartup_impl<'mcx>(
    mcx: Mcx<'mcx>,
    dest: DestReceiverHandle,
    operation: CmdType,
    tupdesc: &TupleDescData<'mcx>,
) -> PgResult<()> {
    let r = lookup(dest);
    (r.vtable.rStartup)(mcx, r.state, operation, tupdesc)
}

/// `dest->receiveSlot(slot, dest)` — route to the receiver's `receiveSlot`
/// callback, threading the per-query `mcx` and its `state` token.
fn dest_receive_slot_impl<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    dest: DestReceiverHandle,
) -> PgResult<bool> {
    let r = lookup(dest);
    (r.vtable.receiveSlot)(mcx, r.state, slot)
}

/// `dest->rShutdown(dest)` — route to the receiver's `rShutdown` callback,
/// threading the per-query `mcx` and its `state` token.
fn dest_rshutdown_impl<'mcx>(mcx: Mcx<'mcx>, dest: DestReceiverHandle) -> PgResult<()> {
    let r = lookup(dest);
    (r.vtable.rShutdown)(mcx, r.state)
}

/// `dest->mydest` (tcop/dest.h) — return the receiver's `CommandDest` tag.
fn dest_get_mydest_impl(dest: DestReceiverHandle) -> CommandDest {
    lookup(dest).mydest
}

// ===========================================================================
// Command-completion / protocol helpers (dest.c) — BeginCommand, EndCommand,
// EndReplicationCommand, NullCommand, ReadyForQuery.
// ===========================================================================

/// `void BeginCommand(CommandTag commandTag, CommandDest dest)` (dest.c):
/// initialize the destination at the start of a command. "Nothing to do at
/// present" — but the call site is preserved faithfully.
pub fn BeginCommand(_command_tag: CommandTag, _dest: CommandDest) {
    // Nothing to do at present
}

/// `void EndCommand(const QueryCompletion *qc, CommandDest dest, bool
/// force_undecorated_output)` (dest.c): clean up the destination at the end of
/// a command. For the remote destinations this builds the command-completion
/// tag and sends the protocol `CommandComplete` ('C') message; for all other
/// destinations it does nothing.
///
/// C uses a stack `char completionTag[COMPLETION_TAG_BUFSIZE]` and
/// `BuildQueryCompletionString(...) -> len`, then
/// `pq_putmessage(PqMsg_CommandComplete, completionTag, len + 1)` — the `+ 1`
/// sends the trailing NUL. Here `build_query_completion_string` returns the tag
/// (without terminator) allocated in `mcx`; we append a single NUL to match the
/// `len + 1` byte count exactly.
pub fn EndCommand<'mcx>(
    mcx: Mcx<'mcx>,
    qc: &QueryCompletion,
    dest: CommandDest,
    force_undecorated_output: bool,
) -> PgResult<()> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple => {
            let tag = backend_tcop_cmdtag::build_query_completion_string(
                mcx,
                qc,
                force_undecorated_output,
            )?;
            // len = strlen(completionTag); send len + 1 bytes (incl. NUL).
            let mut body = alloc::vec::Vec::with_capacity(tag.as_bytes().len() + 1);
            body.extend_from_slice(tag.as_bytes());
            body.push(0);
            let _eof =
                backend_libpq_pqcomm_seams::pq_putmessage::call(PqMsg_CommandComplete, &body)?;
        }

        CommandDest::None
        | CommandDest::Debug
        | CommandDest::Spi
        | CommandDest::Tuplestore
        | CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {}
    }
    Ok(())
}

/// `void EndReplicationCommand(const char *commandTag)` (dest.c): a stripped
/// down `EndCommand` for replication commands — send the `CommandComplete`
/// ('C') message for the given tag. `strlen(commandTag) + 1` sends the trailing
/// NUL.
pub fn end_replication_command_impl(command_tag: alloc::string::String) -> PgResult<()> {
    let bytes = command_tag.as_bytes();
    let mut body = alloc::vec::Vec::with_capacity(bytes.len() + 1);
    body.extend_from_slice(bytes);
    body.push(0);
    let _eof = backend_libpq_pqcomm_seams::pq_putmessage::call(PqMsg_CommandComplete, &body)?;
    Ok(())
}

/// `void NullCommand(CommandDest dest)` (dest.c): tell the destination an empty
/// query string was recognized. For the remote destinations this sends the
/// protocol `EmptyQueryResponse` ('I') message (with no body); for all other
/// destinations it does nothing. This ensures a recognizable end to the
/// response to an Execute message in the extended query protocol.
pub fn NullCommand(dest: CommandDest) -> PgResult<()> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple => {
            // Tell the FE that we saw an empty query string
            backend_libpq_pqformat::pq_putemptymessage(PqMsg_EmptyQueryResponse)?;
        }

        CommandDest::None
        | CommandDest::Debug
        | CommandDest::Spi
        | CommandDest::Tuplestore
        | CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {}
    }
    Ok(())
}

/// `void ReadyForQuery(CommandDest dest)` (dest.c): tell the destination we are
/// ready for a new query. For the remote destinations this sends the protocol
/// `ReadyForQuery` ('Z') message — which in protocol 3.0+ carries the
/// transaction-block status code — and flushes the output (the flush happens in
/// any case for the remote dests). For all other destinations it does nothing.
pub fn ReadyForQuery<'mcx>(mcx: Mcx<'mcx>, dest: CommandDest) -> PgResult<()> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple => {
            let mut buf = backend_libpq_pqformat::pq_beginmessage(mcx, PqMsg_ReadyForQuery)?;
            let code = backend_access_transam_xact_seams::transaction_block_status_code::call();
            backend_libpq_pqformat::pq_sendbyte(&mut buf, code as u8)?;
            backend_libpq_pqformat::pq_endmessage(buf)?;
            // Flush output at end of cycle in any case.
            let _eof = backend_libpq_pqcomm_seams::pq_flush::call()?;
        }

        CommandDest::None
        | CommandDest::Debug
        | CommandDest::Spi
        | CommandDest::Tuplestore
        | CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {}
    }
    Ok(())
}

/// Install this crate's inward seams. Wired into `seams-init`.
pub fn init_seams() {
    backend_tcop_dest_seams::dest_rstartup::set(dest_rstartup_impl);
    backend_tcop_dest_seams::dest_receive_slot::set(dest_receive_slot_impl);
    backend_tcop_dest_seams::dest_rshutdown::set(dest_rshutdown_impl);
    backend_tcop_dest_seams::create_dest_receiver::set(CreateDestReceiver);
    backend_tcop_dest_seams::dest_get_mydest::set(dest_get_mydest_impl);
    // Command-completion / protocol helpers (dest.c).
    backend_tcop_dest_seams::begin_command::set(BeginCommand);
    backend_tcop_dest_seams::end_command::set(EndCommand);
    backend_tcop_dest_seams::null_command::set(NullCommand);
    backend_tcop_dest_seams::ready_for_query::set(ReadyForQuery);
    backend_tcop_dest_seams::end_replication_command::set(end_replication_command_impl);
}

#[cfg(test)]
mod tests;
