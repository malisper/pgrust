//! Seam declarations for the `backend-utils-time-snapmgr` unit
//! (`utils/time/snapmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Per `docs/query-lifecycle-raii.md` the ActiveSnapshot stack ports as an
//! owned `SnapshotStack` facet, never as an ambient push/pop pair. The seam
//! below is therefore scope-shaped: the owner brackets the callback with the
//! snapshot push/pop on its own owned stack, so no ambient-stack signature
//! is ever installed.

use types_error::PgResult;

seam_core::seam!(
    /// Run `f` with a transaction snapshot active — the C
    /// `PushActiveSnapshot(GetTransactionSnapshot()); ...; PopActiveSnapshot()`
    /// bracket of `RemoveTempRelationsCallback`, owned by snapmgr as one
    /// scope. Snapshot acquisition allocates and can `ereport(ERROR)`, and
    /// `f`'s error propagates; both carried on `Err`.
    pub fn with_transaction_snapshot(
        f: &mut dyn FnMut() -> PgResult<()>,
    ) -> PgResult<()>
);
