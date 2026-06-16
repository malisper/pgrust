//! Seam declarations for the `DestReceiver` virtual dispatch (`tcop/dest.h`):
//! the `rStartup` / `receiveSlot` / `rShutdown` callbacks a `DestReceiver *`
//! carries. In C these are function pointers on the receiver struct; the owned
//! model keeps the receiver behind a [`DestReceiverHandle`] and dispatches
//! through these seams.
//!
//! The owning dest/printtup infrastructure installs these from its
//! `init_seams()` when it lands; until then a call panics loudly. Callers such
//! as `execTuples.c`'s `begin/do/end_tup_output` reach the receiver only
//! through this surface.

#![allow(non_snake_case)]

use types_dest::CommandDest;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `DestReceiver *CreateDestReceiver(CommandDest dest)` (tcop/dest.c) —
    /// return (a router-keyed handle to) the receiver function set for `dest`.
    /// `PerformPortalFetch` calls this with `DestNone` to discard `MOVE`
    /// output. Infallible in C; the seam returns a plain handle.
    pub fn create_dest_receiver(dest: CommandDest) -> DestReceiverHandle
);

seam_core::seam!(
    /// `dest->rStartup(dest, operation, tupdesc)` (tcop/dest.h): tell the
    /// receiver a result set of `tupdesc` rows is about to be sent under the
    /// given command type (`begin_tup_output_tupdesc` passes `CMD_SELECT`).
    /// `Err` carries whatever the receiver's startup raises.
    pub fn dest_rstartup(
        dest: DestReceiverHandle,
        operation: CmdType,
        tupdesc: &TupleDescData<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `dest->receiveSlot(slot, dest)` (tcop/dest.h): send one tuple (held in
    /// `slot`) to the receiver, returning the receiver's bool result (C casts
    /// it to `(void)`). `Err` carries whatever the receiver raises.
    pub fn dest_receive_slot(
        slot: &mut SlotData<'_>,
        dest: DestReceiverHandle,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `dest->rShutdown(dest)` (tcop/dest.h): tell the receiver the result set
    /// is finished. `Err` carries whatever the receiver's shutdown raises.
    pub fn dest_rshutdown(dest: DestReceiverHandle) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `dest->mydest` (tcop/dest.h): the [`CommandDest`] discriminant the
    /// receiver was created for. `PortalRunMulti` tests it against
    /// `DestRemoteExecute` (to swap in `None_Receiver`); `DoPortalRunFetch`
    /// tests it against `DestNone`. A plain field read; infallible.
    pub fn dest_get_mydest(dest: DestReceiverHandle) -> CommandDest
);
