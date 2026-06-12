//! Seam declarations for the `backend-access-transam-twophase` unit
//! (`access/transam/twophase.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! C threads an opaque `GlobalTransaction` (gxact) through
//! `MarkAsPreparing` → `StartPrepare` → `EndPrepare`; the owner keeps that
//! being-prepared gxact as backend-local state, so these seams carry no
//! handle (one transaction is prepared at a time per backend).

use types_core::{Oid, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    /// `MarkAsPreparing(xid, gid, prepared_at, owner, databaseid)` — reserve
    /// the GID; fails if invalid or already in use.
    pub fn mark_as_preparing(
        xid: TransactionId,
        gid: &str,
        prepared_at: TimestampTz,
        owner: Oid,
        databaseid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `StartPrepare(gxact)` — begin collecting the 2PC state file data.
    pub fn start_prepare() -> PgResult<()>
);

seam_core::seam!(
    /// `EndPrepare(gxact)` — write the prepare record; the durable prepare.
    pub fn end_prepare() -> PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_Twophase()` — detach the prepared transaction from this
    /// backend.
    pub fn post_prepare_twophase()
);

seam_core::seam!(
    /// `AtAbort_Twophase()` — clean up a partially-prepared gxact on abort.
    pub fn at_abort_twophase()
);

seam_core::seam!(
    /// `PrepareRedoAdd(...)` under `TwoPhaseStateLock` (the xact_redo caller
    /// acquires/releases the lock in C; the installed impl carries it until
    /// the lwlock surface is ported).
    pub fn prepare_redo_add(
        data: &[u8],
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        origin_id: RepOriginId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PrepareRedoRemove(xid, giveWarning)` under `TwoPhaseStateLock` (see
    /// `prepare_redo_add`).
    pub fn prepare_redo_remove(xid: TransactionId, give_warning: bool) -> PgResult<()>
);
