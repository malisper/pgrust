//! Seam declarations for the `backend-access-transam-subtrans` unit
//! (`access/transam/subtrans.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `SubTransGetParent(xid)` (subtrans.c): the immediate parent xid
    /// recorded in pg_subtrans for a subtransaction, or
    /// `InvalidTransactionId` if none is recorded (e.g. the post-startup
    /// window where pg_subtrans was zeroed). The SLRU page read can
    /// `ereport(ERROR)` on I/O failure, carried on `Err`.
    pub fn sub_trans_get_parent(xid: TransactionId) -> PgResult<TransactionId>
);

seam_core::seam!(
    /// `SubTransSetParent(xid, parent)` — record the parent of a
    /// subtransaction in pg_subtrans; SLRU page access can `ereport(ERROR)`.
    pub fn sub_trans_set_parent(xid: TransactionId, parent: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `SubTransGetTopmostTransaction(xid)` (subtrans.c): walk the
    /// pg_subtrans parent chain to the top-level xid (bounded by
    /// `TransactionXmin`). SLRU page reads can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn sub_trans_get_topmost_transaction(xid: TransactionId) -> PgResult<TransactionId>
);

seam_core::seam!(
    /// `SUBTRANSShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn sub_trans_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `SUBTRANSShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn sub_trans_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExtendSUBTRANS(newestXact)` (subtrans.c): zero the pg_subtrans page
    /// that would hold `newestXact` if it is the first XID of a new page,
    /// while the caller holds `XidGenLock`. The SLRU page write can
    /// `ereport(ERROR)`, carried on `Err`. Owner unported; scaffolded slot.
    pub fn extend_subtrans(newest_xact: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `StartupSUBTRANS(oldestActiveXID)` (subtrans.c) — zero the currently
    /// active pg_subtrans page range at startup. Called from `StartupXLOG`
    /// (xlog.c:5870 / 6155). The owner wraps its private `SubTransState`; the
    /// SLRU page zeroing can `ereport(ERROR)`, carried on `Err`.
    pub fn startup_subtrans(oldest_active_xid: TransactionId) -> PgResult<()>
);
