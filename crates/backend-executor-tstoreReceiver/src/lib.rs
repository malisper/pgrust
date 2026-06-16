#![allow(non_snake_case)]
//! Port of `src/backend/executor/tstoreReceiver.c` — the `DestReceiver` that
//! stores result tuples in a `Tuplestorestate`.
//!
//! Optionally the receiver can force detoasting (but not decompression) of
//! out-of-line toasted values — this supports cursors `WITH HOLD`, which must
//! retain data even if the underlying table is dropped. Optionally it can also
//! apply a tuple-conversion map before storing.
//!
//! # Model: the unified tcop-dest router
//!
//! In C a `TStoreState` is a `palloc0`'d struct whose first field is the
//! `DestReceiver pub`; the four callbacks recover it by casting `(TStoreState *)
//! self`. The repo's owned model names every receiver by a router-keyed
//! [`DestReceiverHandle`] into the single `backend-tcop-dest` registry; each
//! receiver carries a real [`ReceiverVtable`] of
//! `rStartup`/`receiveSlot`/`rShutdown` callbacks plus an owner-supplied `state`
//! token. This crate is the `DestTuplestore` owner: its constructor
//! ([`create_dest_receiver_tuplestore`](self)) registers the tstore vtable into
//! that router exactly as `copyto.c`'s `CreateCopyDestReceiver` does, and the
//! `state` token is an index into this crate's private [`RECEIVERS`] table — the
//! owned-model stand-in for C's `(TStoreState *) self` downcast.
//!
//! # Where the tuplestore lives
//!
//! In C `TStoreState.tstore`/`.cxt` are set by
//! `SetTuplestoreDestReceiverParams` to `portal->holdStore`/`portal->holdContext`
//! (portalcmds' only caller). In the repo's owned model the store is owned by
//! the [`Portal`] itself (`PortalData::holdStore`), so this receiver does not
//! hold its own `Tuplestorestate`; instead its per-receiver state keeps a
//! [`Portal`] handle (an `Rc` clone) and reaches `portal.holdStore` on each
//! `receiveSlot`, matching C's `myState->tstore` aliasing of `portal->holdStore`.
//!
//! # Receive variants
//!
//! `tstoreStartupReceiver` decides — over the result `TupleDesc` — whether any
//! column needs detoast work and selects the matching variant:
//!
//!   * [`ReceiveMode::Notoast`] — `tstoreReceiveSlot_notoast`: store the slot's
//!     tuple directly (deform via `slot_getallattrs`, then `tuplestore_putvalues`).
//!   * [`ReceiveMode::Detoast`] — `tstoreReceiveSlot_detoast`: deform, fetch back
//!     any external toasted varlena values (`detoast_external_attr`), then store
//!     the flattened row from values.
//!   * `tstoreReceiveSlot_tupmap` (the conversion-map variant) is ported in C but
//!     unreachable through this repo's seam contract: portalcmds'
//!     `SetTuplestoreDestReceiverParams` call passes no `target_tupdesc`, so the
//!     receiver never builds a `tupmap` and the variant is never selected. The
//!     `set_tuplestore_dest_receiver_params` seam carries no `target_tupdesc`
//!     parameter, so `target_tupdesc` is always `None` here.
//!
//! There is no `*mut`/`c_void` in the dispatch surface; the workspace lives as
//! owned `Vec`s, and the `MemoryContextSwitchTo(myState->cxt)` around the put is
//! implicit (the store carries its own context; `tuplestore_putvalues` forms the
//! tuple into the store's context).

extern crate alloc;

use core::cell::RefCell;

use backend_tcop_dest::ReceiverVtable;
use mcx::Mcx;
use types_dest::CommandDest;
use types_error::{PgError, PgResult};
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_portal::Portal;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::TupleDescData;

/// Which receive variant `tstoreStartupReceiver` selected, mirroring the C
/// `myState->pub.receiveSlot = tstoreReceiveSlot_X` assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiveMode {
    /// `tstoreReceiveSlot_notoast` — store the slot's tuple directly.
    Notoast,
    /// `tstoreReceiveSlot_detoast` — flatten external toasted values, then store.
    Detoast,
}

/// `TStoreState` (`tstoreReceiver.c`) — the tuplestore `DestReceiver` state.
///
/// The C `DestReceiver pub` head is the router registration (its `mydest` is
/// `DestTuplestore` and its vtable holds the three callbacks); the private
/// fields below are the rest of the struct. `tstore`/`cxt` are not held here —
/// they live on the [`Portal`] (`holdStore`/`holdContext`), reached through the
/// `portal` handle (C's `myState->tstore == portal->holdStore` aliasing).
struct TStoreState {
    /// The router handle naming this receiver (the C `DestReceiver *`). Set when
    /// the receiver is registered; used by `dest_destroy` and to bind params.
    dr_handle: DestReceiverHandle,

    /// The portal owning the destination tuplestore (`portal->holdStore` /
    /// `portal->holdContext`). C aliases the store into `myState->tstore`; the
    /// owned model keeps the `Rc`-cloned handle and reaches `holdStore` per row.
    /// `None` until `SetTuplestoreDestReceiverParams`.
    portal: Option<Portal>,

    /// `bool detoast` — were we told to detoast?
    detoast: bool,

    /// The active receive variant (C records this in `pub.receiveSlot`). Set at
    /// startup.
    mode: ReceiveMode,
}

impl TStoreState {
    /// `CreateTuplestoreDestReceiver` initial private state (before
    /// `Set…Params`); `receiveSlot` starts at `notoast` and might change at
    /// startup.
    fn new() -> Self {
        TStoreState {
            dr_handle: DestReceiverHandle::NULL,
            portal: None,
            detoast: false,
            mode: ReceiveMode::Notoast,
        }
    }
}

// ===========================================================================
// Per-receiver state table — the owned stand-in for C's `(TStoreState *) self`.
// The router `state` token is a 1-based index into this table (0 is never a
// live token).
// ===========================================================================

thread_local! {
    static RECEIVERS: RefCell<alloc::vec::Vec<Option<TStoreState>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

/// Allocate a fresh receiver state slot, returning its 1-based token.
fn receiver_register(state: TStoreState) -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(i) = reg.iter().position(Option::is_none) {
            reg[i] = Some(state);
            (i + 1) as u64
        } else {
            reg.push(Some(state));
            reg.len() as u64
        }
    })
}

/// Run `f` with a mutable borrow of the receiver state named by `token`.
fn with_receiver<R>(token: u64, f: impl FnOnce(&mut TStoreState) -> R) -> R {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let slot = reg
            .get_mut((token - 1) as usize)
            .and_then(Option::as_mut)
            .expect("live tuplestore DestReceiver state");
        f(slot)
    })
}

/// Find the receiver state token for a router handle (linear scan; the table is
/// tiny — one live held cursor at a time).
fn token_for_handle(dr: DestReceiverHandle) -> Option<u64> {
    RECEIVERS.with(|r| {
        let reg = r.borrow();
        reg.iter().position(|s| match s {
            Some(st) => st.dr_handle == dr,
            None => false,
        })
        .map(|i| (i + 1) as u64)
    })
}

/// Release the receiver state slot named by `token` (the C `pfree(self)`).
fn receiver_unregister(token: u64) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(slot) = reg.get_mut((token - 1) as usize) {
            *slot = None;
        }
    });
}

// ===========================================================================
// CreateTuplestoreDestReceiver (tstoreReceiver.c) — register the receiver.
// ===========================================================================

/// `CreateTuplestoreDestReceiver(void)` (tstoreReceiver.c) — create the
/// `DestReceiver` object for `DestTuplestore` and register it into the
/// tcop-dest router, returning its [`DestReceiverHandle`].
///
/// Mirrors C's constructor: allocate the per-receiver state (the [`RECEIVERS`]
/// slot) with `receiveSlot = tstoreReceiveSlot_notoast` (might change at
/// startup), `mydest = DestTuplestore`, then register the vtable. The
/// `RECEIVERS` index is the router's `state` token — the owned-model stand-in
/// for C's `(TStoreState *) self`. The router handle is recorded back into the
/// state so `dest_destroy`/`Set…Params` can find the slot.
pub fn CreateTuplestoreDestReceiver() -> DestReceiverHandle {
    let token = receiver_register(TStoreState::new());
    let dr = backend_tcop_dest::register_dest_receiver(
        CommandDest::Tuplestore,
        ReceiverVtable {
            rStartup: tstore_startup_receiver,
            receiveSlot: tstore_receive_slot,
            rShutdown: tstore_shutdown_receiver,
        },
        token,
    );
    with_receiver(token, |st| st.dr_handle = dr);
    dr
}

/// `SetTuplestoreDestReceiverParams(self, tStore, tContext, detoast,
/// target_tupdesc, map_failure_msg)` (tstoreReceiver.c) — set parameters for a
/// tuplestore `DestReceiver`.
///
/// Specialized to portalcmds' only call: `tStore`/`tContext` are
/// `portal->holdStore`/`portal->holdContext` (held here through the `portal`
/// handle), `target_tupdesc`/`map_failure_msg` are NULL, and `detoast` is the
/// "detoast all data passed through" flag. C's `Assert(!(detoast &&
/// target_tupdesc))` is trivially satisfied (no `target_tupdesc`), and the
/// `Assert(myState->pub.mydest == DestTuplestore)` is the router registration.
pub fn SetTuplestoreDestReceiverParams(
    receiver: DestReceiverHandle,
    portal: &Portal,
    detoast: bool,
) -> PgResult<()> {
    let token = token_for_handle(receiver)
        .ok_or_else(|| PgError::error("SetTuplestoreDestReceiverParams: unknown receiver handle"))?;
    with_receiver(token, |st| {
        // myState->tstore = tStore;  myState->cxt = tContext;  (both reached
        // through the portal handle).
        st.portal = Some(portal.clone());
        st.detoast = detoast;
        // myState->target_tupdesc = NULL;  myState->map_failure_msg = NULL;
        // (no target_tupdesc in this seam contract).
    });
    Ok(())
}

// ===========================================================================
// DestReceiver vtable callbacks (routed through tcop-dest).
// ===========================================================================

/// `tstoreStartupReceiver(self, operation, typeinfo)` (tstoreReceiver.c) —
/// prepare to receive tuples from the executor. Decides whether any column
/// needs detoast work and selects the matching receive variant.
///
/// The conversion-map (`tupmap`) branch of the C function is omitted here: this
/// receiver never has a `target_tupdesc` (the seam carries none), so the
/// `convert_tuples_by_position` path is never taken and `myState->tupmap` is
/// always NULL.
fn tstore_startup_receiver(
    _mcx: Mcx<'_>,
    state: u64,
    _operation: CmdType,
    typeinfo: &TupleDescData<'_>,
) -> PgResult<()> {
    let natts = typeinfo.natts;

    with_receiver(state, |st| {
        // Check if any columns require detoast work.
        let mut needtoast = false;
        if st.detoast {
            for i in 0..natts {
                let attr = typeinfo.compact_attr(i as usize);
                if attr.attisdropped {
                    continue;
                }
                if attr.attlen == -1 {
                    needtoast = true;
                    break;
                }
            }
        }

        // Set up appropriate callback. (myState->tupmap is always NULL — no
        // target_tupdesc — so the `else if (myState->tupmap)` arm is dead.)
        if needtoast {
            st.mode = ReceiveMode::Detoast;
        } else {
            st.mode = ReceiveMode::Notoast;
        }
    });

    Ok(())
}

/// `dest->receiveSlot(slot, self)` dispatch — route to the variant
/// `tstoreStartupReceiver` selected (C dispatches through the overwritten
/// `pub.receiveSlot` fn pointer).
fn tstore_receive_slot<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let mode = with_receiver(state, |st| st.mode);
    match mode {
        ReceiveMode::Notoast => tstore_receive_slot_notoast(mcx, state, slot),
        ReceiveMode::Detoast => tstore_receive_slot_detoast(mcx, state, slot),
    }
}

/// `tstoreReceiveSlot_notoast(slot, self)` (tstoreReceiver.c) — receive a tuple
/// and store it in the tuplestore. The easy case: no detoast nor map.
///
/// C calls `tuplestore_puttupleslot(myState->tstore, slot)`, which copies the
/// slot's minimal tuple into the store. The repo's `puttupleslot` seam is keyed
/// to a pool `SlotId` + `EState`, which the receiver dispatch does not carry;
/// the equivalent owned path deforms the standalone slot (`slot_getallattrs`)
/// and forms the row from values (`tuplestore_putvalues`) — the same stored
/// tuple.
fn tstore_receive_slot_notoast<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Deform the slot into its (value, isnull) columns and store them directly
    // (C `tuplestore_puttupleslot`; the owned model forms the row from the
    // deformed values — the deformed Datums move into the values array, the
    // word-copy of C's `outvalues[i] = val`).
    let cols = backend_executor_execTuples_seams::slot_getallattrs::call(mcx, slot)?;

    let mut values: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::new();
    let mut isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::new();
    for (val, null) in cols {
        values.push(val);
        isnull.push(null);
    }

    put_row(state, slot, &values, &isnull)?;
    Ok(true)
}

/// `tstoreReceiveSlot_detoast(slot, self)` (tstoreReceiver.c) — receive a tuple
/// and store it, fetching back any out-of-line toasted varlena values first.
fn tstore_receive_slot_detoast<'mcx>(
    mcx: Mcx<'mcx>,
    state: u64,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // Make sure the tuple is fully deconstructed (slot_getallattrs(slot)).
    let cols = backend_executor_execTuples_seams::slot_getallattrs::call(mcx, slot)?;

    // typeinfo->natts (read off the slot's descriptor); read the per-attribute
    // (attisdropped, attlen) up front so the slot-descriptor borrow does not
    // outlive the detoast/put borrows below.
    let attrs: alloc::vec::Vec<(bool, i16)> = {
        let td = slot
            .base()
            .tts_tupleDescriptor
            .as_ref()
            .ok_or_else(|| PgError::error("detoast slot has no tuple descriptor"))?;
        let natts = td.natts as usize;
        (0..natts)
            .map(|i| {
                let a = td.compact_attr(i);
                (a.attisdropped, a.attlen)
            })
            .collect()
    };

    // Fetch back any out-of-line datums. We build the new datums array in
    // myState->outvalues[] (re-using the slot's isnull values). The owned model
    // grows fresh Vecs (C MemoryContextAlloc'd outvalues/tofree); the temporary
    // detoasted values are owned by their `Datum::ByRef` and freed when the
    // local Vec drops (C pfree of tofree[]).
    let mut outvalues: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::new();
    let mut isnull: alloc::vec::Vec<bool> = alloc::vec::Vec::new();

    for (i, (val, null)) in cols.into_iter().enumerate() {
        let (attisdropped, attlen) = attrs[i];

        // if (!attisdropped && attlen == -1 && !isnull) and the value is an
        // external toast pointer, detoast it; otherwise the value is the
        // word-copy of C's `outvalues[i] = val`.
        let mut out = val;
        if !attisdropped && attlen == -1 && !null {
            let is_external = varatt_is_external(out.as_ref_bytes());
            if is_external {
                // val = PointerGetDatum(detoast_external_attr(...))
                let fetched = backend_access_common_detoast_seams::detoast_external_attr::call(
                    mcx,
                    out.as_ref_bytes(),
                )?;
                out = Datum::ByRef(fetched);
            }
        }

        outvalues.push(out);
        isnull.push(null);
    }

    // Push the modified tuple into the tuplestore. (C switches to myState->cxt
    // around the put; tuplestore_putvalues forms the tuple into the store's own
    // context, so the switch is implicit.)
    put_row(state, slot, &outvalues, &isnull)?;

    Ok(true)
}

/// Common tail of the receive variants: form the row from `(values, isnull)`
/// under the slot's descriptor and append it to the portal's `holdStore`
/// (C `tuplestore_putvalues(myState->tstore, typeinfo, outvalues, isnull)`,
/// with `myState->tstore == portal->holdStore`).
fn put_row<'mcx>(
    state: u64,
    slot: &SlotData<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    // The tuple descriptor is the slot's (C `slot->tts_tupleDescriptor`). The
    // store lives on the portal (a distinct object), so its borrow does not
    // alias the slot's descriptor borrow.
    let tdesc = slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .ok_or_else(|| PgError::error("tuplestore receiver slot has no tuple descriptor"))?;

    // Recover the portal handle (an Rc clone) without holding the receiver-state
    // borrow across the store borrow.
    let portal = with_receiver(state, |st| {
        st.portal
            .as_ref()
            .cloned()
            .ok_or_else(|| PgError::error("tuplestore receiver has no portal/store configured"))
    })?;

    let mut data = portal.borrow_mut();
    let store = data
        .holdStore
        .as_mut()
        .ok_or_else(|| PgError::error("tuplestore receiver portal has no holdStore"))?;
    backend_utils_sort_storage_seams::tuplestore_putvalues::call(store, tdesc, values, isnull)
}

/// `tstoreShutdownReceiver(self)` (tstoreReceiver.c) — clean up at end of an
/// executor run. In the owned model the workspace was per-row locals (dropped
/// already) and there is no `tupmap`/`mapslot`, so nothing remains to release.
fn tstore_shutdown_receiver(_mcx: Mcx<'_>, _state: u64) -> PgResult<()> {
    // myState->outvalues / tofree / tupmap / mapslot are all None in this model
    // (the per-row workspace is dropped after each row; no conversion map).
    Ok(())
}

/// `tstoreDestroyReceiver(self)` (tstoreReceiver.c) — destroy the receiver when
/// done with it (C `pfree(self)`). The repo's portalcmds driver reaches this
/// through the `dest_destroy` seam with the router handle.
fn tstore_destroy_receiver(receiver: DestReceiverHandle) -> PgResult<()> {
    if let Some(token) = token_for_handle(receiver) {
        receiver_unregister(token);
    }
    Ok(())
}

// ===========================================================================
// varatt.h helper — pure bit-twiddling, reproduced inline (the same `VARATT_*`
// macro detoast.c / heaptuple.c each reproduce locally; not a TU boundary).
// ===========================================================================

/// `VARATT_IS_EXTERNAL(PTR)` for a 1-byte-header varlena datum: `va_header ==
/// 0x01` (postgres.h / varatt.h).
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    !b.is_empty() && b[0] == 0x01
}

// ===========================================================================
// inward seam installation.
// ===========================================================================

/// Install this crate's inward seams. Wired into `seams-init`.
pub fn init_seams() {
    backend_executor_tstorereceiver_seams::create_dest_receiver_tuplestore::set(|| {
        Ok(CreateTuplestoreDestReceiver())
    });
    backend_executor_tstorereceiver_seams::set_tuplestore_dest_receiver_params::set(
        SetTuplestoreDestReceiverParams,
    );
    backend_executor_tstorereceiver_seams::dest_destroy::set(tstore_destroy_receiver);
}

#[cfg(test)]
mod tests;
