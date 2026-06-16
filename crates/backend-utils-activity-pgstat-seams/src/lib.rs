//! Seam declarations for the `backend-utils-activity-pgstat` unit
//! (`utils/activity/pgstat.c`): the `pgStatLocal` shared-memory control block
//! and per-backend snapshot, plus the cross-kind helpers the per-kind stats
//! files call.
//!
//! The `with_shmem_*`/`with_snapshot_*` slots run a caller-supplied callback
//! against the live shmem-resident / snapshot-resident per-kind structs,
//! mirroring C's `&pgStatLocal.shmem-><kind>` / `&pgStatLocal.snapshot.<kind>`
//! pointers. (A callback rather than a returned `&'static mut`: aliasable
//! mutable statics are unsound in Rust.) The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

use types_pgstat::activity_pgstat::{
    PgStatShared_Archiver, PgStatShared_Checkpointer, PgStat_ArchiverStats,
    PgStat_CheckpointerStats,
};
use types_pgstat::backend_utils_activity_pgstat_bgwriter::{
    PgStatShared_BgWriter, PgStat_BgWriterStats,
};

seam_core::seam!(
    /// `pgstat_prepare_report_checksum_failure(dboid)` (pgstat_database.c):
    /// ensure a pending per-database stats entry exists so a subsequent
    /// checksum-failure increment can be recorded. Keyed by the database OID.
    pub fn pgstat_prepare_report_checksum_failure(
        dboid: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_report_checksum_failures_in_db(dboid, failurecount)`
    /// (pgstat_database.c): record `failurecount` data-checksum failures for the
    /// given database in the pending per-database stats.
    pub fn pgstat_report_checksum_failures_in_db(
        dboid: types_core::primitive::Oid,
        failurecount: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_init_relation(rel)` (pgstat_relation.c): set the relcache
    /// entry's `pgstat_enabled` / `pgstat_info` according to whether the
    /// relation has storage (or is a partitioned table) and whether
    /// `pgstat_track_counts` is on. Keyed by the relation OID; the owner reads
    /// the relkind and mutates its per-relation pending-stats bookkeeping.
    pub fn pgstat_init_relation(relid: types_core::primitive::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_drop_database(databaseid)` (pgstat_database.c): tell the
    /// cumulative stats system to forget the database's stats immediately
    /// (`pgstat_drop_entry(PGSTAT_KIND_DATABASE, databaseid, InvalidOid)`),
    /// dropping the dropped database's entry as part of DROP DATABASE.
    pub fn pgstat_drop_database(databaseid: types_core::primitive::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_create_relation(rel)` (pgstat_relation.c): create the pending
    /// per-relation stats entry transactionally (`pgstat_create_transactional(
    /// PGSTAT_KIND_RELATION, rel->rd_rel->relisshared ? InvalidOid :
    /// MyDatabaseId, RelationGetRelid(rel))`) plus `pgstat_init_relation(rel)`,
    /// so the entry is dropped if the transaction aborts. Keyed by the
    /// relation OID; `relisshared` selects the database OID the way the C reads
    /// `rel->rd_rel->relisshared`.
    pub fn pgstat_create_relation(
        relid: types_core::primitive::Oid,
        relisshared: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_drop_relation(rel)` (pgstat_relation.c): drop the per-relation
    /// stats entry transactionally (`pgstat_drop_transactional(
    /// PGSTAT_KIND_RELATION, rel->rd_rel->relisshared ? InvalidOid :
    /// MyDatabaseId, RelationGetRelid(rel))`) and unlink the relcache entry's
    /// pending-stats pointer, so the entry is removed when the transaction
    /// commits. Keyed by the relation OID; `relisshared` selects the database
    /// OID as the C reads `rel->rd_rel->relisshared`.
    pub fn pgstat_drop_relation(
        relid: types_core::primitive::Oid,
        relisshared: bool,
    ) -> types_error::PgResult<()>
);

// --- per-relation count seams (pgstat.h macros / pgstat_relation.c) ---
//
// The C count macros gate on `pgstat_should_count_relation(rel)`, which checks
// `rel->pgstat_info != NULL` (a live pending entry) else `rel->pgstat_enabled`
// (lazily creating one via `pgstat_assoc_relation`). Our `RelationData` carries
// only `pgstat_enabled` (the pending-entry link is keyed by relation OID inside
// pgstat, not back-pointered from the relcache), so each count seam carries
// `(relid, pgstat_enabled)` — the narrowest faithful capability for the owner
// to replicate the C gate without an ambient relcache lookup. The owner reads
// `pgstat_enabled` off the caller's `RelationData`; callers pass it explicitly.

seam_core::seam!(
    /// `pgstat_count_index_tuples(rel, n)` (pgstat.h macro): add `n` to the
    /// relation's pending `tuples_returned` counter (only when
    /// `pgstat_should_count_relation`). `pgstat_enabled` mirrors
    /// `rel->pgstat_enabled`; the macro never errors.
    pub fn pgstat_count_index_tuples(
        index_oid: types_core::primitive::Oid,
        pgstat_enabled: bool,
        n: i64,
    )
);

seam_core::seam!(
    /// `pgstat_count_heap_fetch(rel)` (pgstat.h macro): increment the
    /// relation's pending `tuples_fetched` counter (only when
    /// `pgstat_should_count_relation`).
    pub fn pgstat_count_heap_fetch(index_oid: types_core::primitive::Oid, pgstat_enabled: bool)
);

seam_core::seam!(
    /// `pgstat_count_index_scan(rel)` (pgstat.h macro): increment the
    /// relation's pending `numscans` counter (only when
    /// `pgstat_should_count_relation`). The macro never errors.
    pub fn pgstat_count_index_scan(index_oid: types_core::primitive::Oid, pgstat_enabled: bool)
);

seam_core::seam!(
    /// `pgstat_count_heap_scan(rel)` (pgstat.h macro): increment the relation's
    /// pending `numscans` counter (only when `pgstat_should_count_relation`).
    /// The macro never errors.
    pub fn pgstat_count_heap_scan(relid: types_core::primitive::Oid, pgstat_enabled: bool)
);

seam_core::seam!(
    /// `pgstat_count_heap_getnext(rel)` (pgstat.h macro): increment the
    /// relation's pending `tuples_returned` counter (only when
    /// `pgstat_should_count_relation`). The macro never errors.
    pub fn pgstat_count_heap_getnext(relid: types_core::primitive::Oid, pgstat_enabled: bool)
);

seam_core::seam!(
    /// `pgstat_count_heap_insert(rel, n)` (pgstat_relation.c): add `n` to the
    /// relation's pending `tuples_inserted` counter (only when
    /// `pgstat_should_count_relation`). The function never errors.
    pub fn pgstat_count_heap_insert(relid: types_core::primitive::Oid, pgstat_enabled: bool, n: i64)
);

seam_core::seam!(
    /// `pgstat_count_heap_delete(rel)` (pgstat_relation.c): bump the relation's
    /// pending `tuples_deleted` counter (only when
    /// `pgstat_should_count_relation`). The function never errors.
    pub fn pgstat_count_heap_delete(relid: types_core::primitive::Oid, pgstat_enabled: bool)
);

seam_core::seam!(
    /// `pgstat_count_heap_update(rel, hot, newpage)` (pgstat_relation.c): bump
    /// the relation's pending `tuples_updated` counter (and the
    /// `tuples_hot_updated` / `tuples_newpage_updated` sub-counters per the
    /// `hot` / `newpage` flags), only when `pgstat_should_count_relation`. The
    /// function never errors.
    pub fn pgstat_count_heap_update(
        relid: types_core::primitive::Oid,
        pgstat_enabled: bool,
        hot: bool,
        newpage: bool,
    )
);

seam_core::seam!(
    /// `pgstat_update_heap_dead_tuples(rel, delta)` (pgstat_relation.c): add
    /// `delta` to the relation's pending dead-tuple counter (on-access pruning
    /// reports tuples it reclaimed this way). No-ops when
    /// `!pgstat_should_count_relation`; never errors.
    pub fn pgstat_update_heap_dead_tuples(
        relid: types_core::primitive::Oid,
        pgstat_enabled: bool,
        delta: i32,
    )
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.shmem->archiver`.
    pub fn with_shmem_archiver(f: &mut dyn FnMut(&mut PgStatShared_Archiver))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.snapshot.archiver`.
    pub fn with_snapshot_archiver(f: &mut dyn FnMut(&mut PgStat_ArchiverStats))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.shmem->bgwriter`.
    pub fn with_shmem_bgwriter(f: &mut dyn FnMut(&mut PgStatShared_BgWriter))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.snapshot.bgwriter`.
    pub fn with_snapshot_bgwriter(f: &mut dyn FnMut(&mut PgStat_BgWriterStats))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.shmem->checkpointer`.
    pub fn with_shmem_checkpointer(f: &mut dyn FnMut(&mut PgStatShared_Checkpointer))
);

seam_core::seam!(
    /// Run `f` on `&pgStatLocal.snapshot.checkpointer`.
    pub fn with_snapshot_checkpointer(f: &mut dyn FnMut(&mut PgStat_CheckpointerStats))
);

seam_core::seam!(
    /// `pgStatLocal.shmem->is_shutdown` (read for the `Assert` in the report
    /// paths).
    pub fn shmem_is_shutdown() -> bool
);

seam_core::seam!(
    /// `pgstat_assert_is_up()` (`utils/pgstat_internal.h` / `pgstat.c`) — a
    /// no-op macro outside `USE_ASSERT_CHECKING`; infallible.
    pub fn assert_is_up()
);

seam_core::seam!(
    /// `pgstat_snapshot_fixed(PgStat_Kind kind)` (`pgstat.c`). `Err` carries
    /// the `ereport(ERROR)`s reachable through `pgstat_build_snapshot`
    /// (palloc / dsa out-of-memory) and the per-kind `snapshot_cb`s'
    /// `LWLockAcquire` (`too many LWLocks taken`).
    pub fn snapshot_fixed(
        kind: types_pgstat::activity_pgstat::PgStat_Kind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_clear_snapshot()` (`pgstat.c`) — discard any materialized stats
    /// snapshot: reset the fixed/custom validity flags, drop the snapshot hash
    /// and its memory context, and forward the reset to `backend_status.c`.
    /// Frees only; infallible.
    pub fn pgstat_clear_snapshot()
);

seam_core::seam!(
    /// `pgstat_reset(kind, dboid, objid)` (`pgstat.c`) — reset one
    /// variable-numbered stats entry to zero (and, for kinds not accessed
    /// across databases, touch the database entry's reset timestamp). `Err`
    /// carries the `ereport(ERROR)`s reachable through
    /// `pgstat_get_entry_ref_locked` (palloc/dsa out-of-memory,
    /// `LWLockAcquire`'s `too many LWLocks taken`).
    pub fn pgstat_reset(
        kind: types_pgstat::activity_pgstat::PgStat_Kind,
        dboid: types_core::Oid,
        objid: u64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `(pgstat_get_kind_info(kind))->name` (`pgstat.c`) — the human-readable
    /// name of a stats kind (builtin table `pgstat_kind_builtin_infos[]` or
    /// the custom-kind registry; both hold `'static`-equivalent struct
    /// pointers). Callers only consult it for kinds that already resolved to
    /// a live entry, so the lookup cannot miss; infallible.
    pub fn pgstat_get_kind_name(kind: types_pgstat::activity_pgstat::PgStat_Kind) -> &'static str
);

seam_core::seam!(
    /// `pgstat_report_stat(force)` (pgstat.c) — flush pending stats; returns
    /// the soonest time another flush could be useful (0 if idle).
    pub fn pgstat_report_stat(force: bool) -> types_error::PgResult<i64>
);

// --- backend-utils-init-postinit consumers (pgstat.c) ---

seam_core::seam!(
    /// `pgstat_initialize()` (pgstat.c): initialize this backend's cumulative
    /// statistics state and register the pgstat shutdown callback. `Err`
    /// carries its `ereport` surface.
    pub fn pgstat_initialize() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_before_server_shutdown(code, arg)` (pgstat.c): the
    /// before_shmem_exit callback that flushes pending statistics. `Err`
    /// carries its `ereport` surface.
    ///
    /// `arg` is the canonical unified `Datum`, the machine word the C `Datum
    /// arg` carries, pinned to `'static` — matching the `before_shmem_exit`
    /// registration contract (bare-word datum.c value stored in the exit list).
    pub fn pgstat_before_server_shutdown(
        code: i32,
        arg: types_tuple::Datum<'static>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `StatsShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn stats_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `StatsShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn stats_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_set_wait_event_storage(&GetPGProcByNumber(procno)->wait_event_info)`
    /// (pgstat.c / wait_event.c): point this backend's wait-event reporting at
    /// the named PGPROC's `wait_event_info` word, so other backends can read
    /// what it is waiting on. Called from `InitProcess` / `InitAuxiliaryProcess`.
    pub fn pgstat_set_wait_event_storage_for_proc(procno: types_core::ProcNumber)
);

seam_core::seam!(
    /// `pgstat_reset_wait_event_storage()` (wait_event.c): reset wait-event
    /// reporting back to the process-local word during proc teardown.
    pub fn pgstat_reset_wait_event_storage()
);

seam_core::seam!(
    /// `pgstat_restore_stats()` (pgstat.c:506) — at WAL startup on a clean
    /// (non-crash) boot, read the on-disk `pg_stat/` statistics file and load it
    /// into shared memory. Called from `StartupXLOG` (xlog.c). Fallible: the file
    /// read / shmem-load path `ereport(ERROR)`s on corruption.
    pub fn pgstat_restore_stats() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_discard_stats()` (pgstat.c:518) — at WAL startup on a crash boot,
    /// throw away any stale on-disk statistics. Called from `StartupXLOG`
    /// (xlog.c). Fallible: the unlink path `ereport(ERROR)`s.
    pub fn pgstat_discard_stats() -> types_error::PgResult<()>
);
