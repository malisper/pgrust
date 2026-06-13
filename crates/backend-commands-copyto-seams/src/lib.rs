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
    /// `copy_dest_receive(slot, self)` (copyto.c:1398): emit one received tuple
    /// to the COPY destination (`CopyOneRowTo(cstate, slot)`) and bump the
    /// receiver's processed count. `receiver` is the handle
    /// `CreateCopyDestReceiver` returned (associated with the live cstate).
    /// `Err` carries any output-function / write `ereport(ERROR)`.
    pub fn copy_dest_receive(
        receiver: u64,
        slot: &mut types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<bool>
);
