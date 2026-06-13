//! Seam declarations for the `backend-storage-ipc-sinval` unit
//! (`storage/ipc/sinval.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::LocalTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `HandleCatchupInterrupt()` (sinval.c) — the PROCSIG_CATCHUP_INTERRUPT
    /// arm of `procsignal_sigusr1_handler`. Signal-handler-safe flag
    /// flipping; infallible.
    pub fn handle_catchup_interrupt()
);

seam_core::seam!(
    /// `SharedInvalBackendInit(sendOnly)` (sinvaladt.c) — can
    /// `ereport(FATAL)` when no free procState slot exists.
    pub fn shared_inval_backend_init(send_only: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `GetNextLocalTransactionId()` (sinvaladt.c).
    pub fn get_next_local_transaction_id() -> LocalTransactionId
);

seam_core::seam!(
    /// `SharedInvalidMessageCounter` (sinval.c): the running count of shared
    /// invalidation messages this backend has processed. Pure global read.
    pub fn shared_invalid_message_counter() -> u64
);
