//! Seam declarations for the `backend-utils-activity-shmem` unit
//! (`utils/activity/pgstat_shmem.c`): the DSA/dshash-backed shared
//! stats-entry table operations.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_pgstat::activity_pgstat::PgStat_Kind;

seam_core::seam!(
    /// `pgstat_drop_entry(kind, dboid, objid)` (`pgstat_shmem.c`) — release
    /// the backend-local reference and mark/delete the shared hash entry.
    /// Returns `true` if the entry was actually freed, `false` if another
    /// backend still references it (the caller then schedules an entry-refs
    /// GC). `Err` carries the `ereport(ERROR)`s reachable through
    /// dshash/`LWLockAcquire` (`too many LWLocks taken`) and, for database
    /// entries, `pgstat_drop_database_and_contents`.
    pub fn pgstat_drop_entry(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<bool>
);

seam_core::seam!(
    /// `pgstat_request_entry_refs_gc()` (`pgstat_shmem.c`) — atomically bump
    /// `pgStatLocal.shmem->gc_request_count` so every backend garbage-collects
    /// its local entry references soon. Infallible.
    pub fn pgstat_request_entry_refs_gc()
);

seam_core::seam!(
    /// `pgstat_get_entry_ref(kind, dboid, objid, /*create=*/ false, NULL)
    /// != NULL` (`pgstat_shmem.c`) — true iff a shared stats entry already
    /// exists for the object. The full C function returns an entry-ref
    /// handle; this seam carries only the existence verdict its consumer
    /// (`pgstat_create_transactional`) needs. `Err` carries the
    /// `ereport(ERROR)`s reachable through the lookup (entry-ref hash
    /// creation palloc/dsa out-of-memory, dshash `LWLockAcquire`).
    pub fn pgstat_get_entry_ref_exists(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<bool>
);
