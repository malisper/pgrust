//! Seam declarations for the `backend-storage-ipc-sinval` unit
//! (`storage/ipc/sinval.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::LocalTransactionId;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

seam_core::seam!(
    /// `SendSharedInvalidMessages(msgs, n)` (sinval.c): enqueue `msgs` on the
    /// shared invalidation message queue (so other backends will process
    /// them). The owned model passes the decoded `&[SharedInvalidationMessage]`
    /// slice (not raw bytes). Can `ereport(ERROR)` (queue overflow handling),
    /// carried on `Err`.
    pub fn send_shared_invalid_messages(msgs: &[SharedInvalidationMessage]) -> PgResult<()>
);

seam_core::seam!(
    /// `ReceiveSharedInvalidMessages(invalFunction, resetFunction)` (sinval.c):
    /// drain the shared invalidation queue, calling `inval_function` per
    /// decoded message and `reset_function` when the backend has fallen too far
    /// behind (a full cache reset). Each is invoked with one decoded
    /// `SharedInvalidationMessage`. Can `ereport(ERROR)` via the callbacks,
    /// carried on `Err`.
    pub fn receive_shared_invalid_messages(
        inval_function: &mut dyn FnMut(&SharedInvalidationMessage),
        reset_function: &mut dyn FnMut(),
    ) -> PgResult<()>
);

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

// shared_inval_backend_init already declared above (used by postinit).

seam_core::seam!(
    /// `SharedInvalShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn shared_inval_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `SharedInvalShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn shared_inval_shmem_init() -> types_error::PgResult<()>
);
