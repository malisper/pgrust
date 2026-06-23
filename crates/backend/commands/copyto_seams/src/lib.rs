//! Seam declarations for the `backend-commands-copyto` unit
//! (`commands/copyto.c`), for callers that would otherwise cycle.
//!
//! The COPY-(query)-TO `DestReceiver` (`DestCopyOut`) is created by copyto's
//! `CreateCopyDestReceiver`, but the executor (a not-yet-ported unit that does
//! not depend on copyto) drives its `receiveSlot` callback. That callback —
//! `copy_dest_receive` → `CopyOneRowTo` — therefore crosses back into copyto
//! through this seam: the executor calls it with the receiver handle copyto
//! built (which carries the live `CopyToStateData`) and the received slot.
//!
//! copyto installs this from its own `init_seams()`.

seam_core::seam!(
    /// `CreateCopyDestReceiver()` (copyto.c:1435): build the COPY-TO
    /// `DestReceiver` (`DestCopyOut`) and register it into the tcop-dest router,
    /// returning its [`DestReceiverHandle`]. `tcop/dest.c`'s `CreateDestReceiver`
    /// switch calls this for `DestCopyOut` (the owner cannot live in dest.c, so
    /// dest delegates here; copyto installs the seam from its own `init_seams()`).
    pub fn create_copy_dest_receiver() -> nodes::parsestmt::DestReceiverHandle
);

seam_core::seam!(
    /// `copy_dest_receive(slot, self)` (copyto.c:1398): emit one received tuple
    /// to the COPY destination (`CopyOneRowTo(cstate, slot)`) and bump the
    /// receiver's processed count. `receiver` is the handle
    /// `CreateCopyDestReceiver` returned (associated with the live cstate).
    /// `Err` carries any output-function / write `ereport(ERROR)`.
    pub fn copy_dest_receive(
        receiver: u64,
        slot: &mut nodes::tuptable::SlotData<'_>,
    ) -> types_error::PgResult<bool>
);
