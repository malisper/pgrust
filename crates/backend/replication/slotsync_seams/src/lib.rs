// upstream 58c1188a3eaa (18.4): Fix slotsync worker blocking promotion when stuck in wait
// slotsync.h's cross-process surface (PROCSIG_SLOTSYNC_MESSAGE): procsignal
// and tcop/postgres call in; the slotsync crate installs (it depends on both,
// so the calls ride seams). The pending flag itself is
// init_small::globals::SlotSyncShutdownPending.

seam_core::seam!(
    // HandleSlotSyncMessageInterrupt (slotsync.c): the SIGUSR1-handler arm
    // for PROCSIG_SLOTSYNC_MESSAGE; only sets the pending flags.
    pub fn handle_slot_sync_message_interrupt()
);

seam_core::seam!(
    // ProcessSlotSyncMessage (slotsync.c), called from ProcessInterrupts when
    // SlotSyncShutdownPending is set: the worker exits, the
    // pg_sync_replication_slots() backend errors out.
    pub fn process_slot_sync_message() -> types_error::PgResult<()>
);
