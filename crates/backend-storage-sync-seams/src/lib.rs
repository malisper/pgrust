//! Seam declarations for the `backend-storage-sync` unit
//! (`storage/sync/sync.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `RegisterSyncRequest(ftag, type, retryOnError)` (`storage/sync.c`) —
    /// queue a durability request for the checkpointer (or handle it locally
    /// in a standalone backend / the checkpointer itself). Returns false if
    /// the request queue is full and `retryOnError` is false. `Err` carries
    /// the `ereport(ERROR)`s reachable through the local-handling path
    /// (e.g. pendingOps hash growth OOM).
    pub fn register_sync_request(
        ftag: types_storage::sync::FileTag,
        request_type: types_storage::sync::SyncRequestType,
        retry_on_error: bool,
    ) -> types_error::PgResult<bool>
);
