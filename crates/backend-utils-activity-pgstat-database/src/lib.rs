//! Port of `src/backend/utils/activity/pgstat_database.c` (PostgreSQL 18.3).
//!
//! Implementation of database statistics (`PGSTAT_KIND_DATABASE`, a
//! variable-numbered stats kind that uses backend-local pending data). Kept
//! separate from `pgstat.c` to enforce the line between the statistics
//! access/storage implementation and the details of individual kinds.
//!
//! The kind's callbacks (`flush_pending_cb`, `reset_timestamp_cb`) are
//! registered into the pgstat core's per-kind table via [`KindInfoBuilder`] from
//! [`init_seams`]; the outward seams with live callers — recovery-conflict
//! (tcop), deadlock (lmgr), tempfile / buffer-read-time / buffer-write-time /
//! at-eoxact (the io/wal/xact units), checksum-failure / drop-database (pgstat
//! core's transactional drop path), report-autovac / fetch-stat-dbentry
//! (autovacuum) and create-database — are installed there too.
//!
//! The backend-local C globals of this file (`pgStatBlockReadTime`,
//! `pgStatBlockWriteTime`, `pgStatActiveTime`, `pgStatTransactionIdleTime`,
//! `pgStatSessionEndCause`, `pgStatXactCommit`, `pgStatXactRollback`,
//! `pgLastSessionReportTime`) are `thread_local!`s here (one backend == one
//! thread), reached through the accessors below.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::Cell;

use backend_utils_activity_pgstat::entry_ref::PgStat_EntryRef;
use backend_utils_activity_pgstat::kind_info::KindInfoBuilder;
use backend_utils_activity_pgstat::pgstat_core;
use backend_utils_activity_pgstat::registry;
use backend_utils_activity_pgstat::shmem;
use backend_utils_activity_xact as xact;
use backend_utils_adt_timestamp_seams::{get_current_timestamp, timestamp_difference};
use backend_utils_init_small_seams::{my_backend_type, my_database_id, my_start_timestamp};
use types_core::init::BackendType;
use types_core::primitive::{InvalidOid, Oid};
use types_core::TimestampTz;
use types_error::PgResult;
use types_pgstat::activity_pgstat::{
    PgStat_Counter, SessionEndType, PgStat_StatDBEntry, PGSTAT_KIND_DATABASE,
};
use types_pgstat::pgstat_internal::{PgStatShared_Common, PgStatShared_Database};
use types_storage::ProcSignalReason;

/// The `objid` value for database stats keys (`InvalidOid` as the u64 object id).
const INVALID_OBJID: u64 = InvalidOid as u64;

// ---------------------------------------------------------------------------
// File-static / global backend-local state (pgstat_database.c:28-37).
// ---------------------------------------------------------------------------

thread_local! {
    /// `PgStat_Counter pgStatBlockReadTime = 0;`
    static PG_STAT_BLOCK_READ_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    /// `PgStat_Counter pgStatBlockWriteTime = 0;`
    static PG_STAT_BLOCK_WRITE_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    /// `PgStat_Counter pgStatActiveTime = 0;`
    static PG_STAT_ACTIVE_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    /// `PgStat_Counter pgStatTransactionIdleTime = 0;`
    static PG_STAT_TRANSACTION_IDLE_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    /// `SessionEndType pgStatSessionEndCause = DISCONNECT_NORMAL;`
    static PG_STAT_SESSION_END_CAUSE: Cell<SessionEndType> =
        const { Cell::new(SessionEndType::DISCONNECT_NORMAL) };

    /// `static int pgStatXactCommit = 0;`
    static PG_STAT_XACT_COMMIT: Cell<i32> = const { Cell::new(0) };
    /// `static int pgStatXactRollback = 0;`
    static PG_STAT_XACT_ROLLBACK: Cell<i32> = const { Cell::new(0) };
    /// `static PgStat_Counter pgLastSessionReportTime = 0;`
    static PG_LAST_SESSION_REPORT_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
}

/// `pgStatSessionEndCause` accessor (read).
pub fn pgstat_session_end_cause() -> SessionEndType {
    PG_STAT_SESSION_END_CAUSE.with(|c| c.get())
}

/// `pgStatSessionEndCause = cause;` (set).
pub fn set_pgstat_session_end_cause(cause: SessionEndType) {
    PG_STAT_SESSION_END_CAUSE.with(|c| c.set(cause));
}

/// `pgStatActiveTime` accessor (set), used by the statement executor.
pub fn set_pgstat_active_time(usecs: PgStat_Counter) {
    PG_STAT_ACTIVE_TIME.with(|c| c.set(usecs));
}

/// `pgStatTransactionIdleTime` accessor (set).
pub fn set_pgstat_transaction_idle_time(usecs: PgStat_Counter) {
    PG_STAT_TRANSACTION_IDLE_TIME.with(|c| c.set(usecs));
}

/// `pgstat_count_conn_active_time(n)` (pgstat.h): `pgStatActiveTime += n`.
/// Called by `backend_status.c` (`pgstat_report_activity`) when a backend
/// leaves the running/fastpath state.
pub fn pgstat_count_conn_active_time(usecs: PgStat_Counter) {
    PG_STAT_ACTIVE_TIME.with(|c| c.set(c.get() + usecs));
}

/// `pgstat_count_conn_txn_idle_time(n)` (pgstat.h):
/// `pgStatTransactionIdleTime += n`. Called by `backend_status.c`
/// (`pgstat_report_activity`) when a backend leaves the idle-in-transaction
/// state.
pub fn pgstat_count_conn_txn_idle_time(usecs: PgStat_Counter) {
    PG_STAT_TRANSACTION_IDLE_TIME.with(|c| c.set(c.get() + usecs));
}

// ---------------------------------------------------------------------------
// Drop.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_drop_database(Oid databaseid)` — remove the entry for
/// the database being dropped (ensures the entry is dropped if the transaction
/// commits).
pub fn pgstat_drop_database(databaseid: Oid) -> PgResult<()> {
    xact::pgstat_drop_transactional(PGSTAT_KIND_DATABASE, databaseid, INVALID_OBJID)
}

// ---------------------------------------------------------------------------
// Autovac / immediate-report paths (write shared stats directly).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_report_autovac(Oid dboid)` — report startup of an
/// autovacuum process. Called before `InitPostgres` is done, so the db OID is
/// passed in rather than read from `MyDatabaseId`.
pub fn pgstat_report_autovac(dboid: Oid) -> PgResult<()> {
    // Assert(IsUnderPostmaster) — can't get here in single user mode.

    // entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_DATABASE, dboid, InvalidOid, false);
    let entry_ref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_DATABASE,
        dboid,
        INVALID_OBJID,
        false,
    )?
    .expect("pgstat_report_autovac: get_entry_ref_locked(create=true) returned None");

    // SAFETY: just-resolved, content-locked live reference.
    let er = unsafe { entry_ref.get() };
    // dbentry = (PgStatShared_Database *) entry_ref->shared_stats;
    let dbentry = unsafe { &mut *(er.shared_stats as *mut PgStatShared_Database) };
    dbentry.stats.last_autovac_time = get_current_timestamp::call();

    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

/// Port of `void pgstat_report_recovery_conflict(int reason)` — report a Hot
/// Standby recovery conflict.
pub fn pgstat_report_recovery_conflict(reason: ProcSignalReason) -> PgResult<()> {
    // Assert(IsUnderPostmaster);
    if !pgstat_track_counts() {
        return Ok(());
    }

    let dbentry = pgstat_prep_database_pending(my_database_id::call())?;
    match reason {
        // Since we drop the information about the database as soon as it
        // replicates, there is no point in counting these conflicts.
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE => {}
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            with_db_pending(dbentry, |d| d.conflict_tablespace += 1);
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK => {
            with_db_pending(dbentry, |d| d.conflict_lock += 1);
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            with_db_pending(dbentry, |d| d.conflict_snapshot += 1);
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            with_db_pending(dbentry, |d| d.conflict_bufferpin += 1);
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            with_db_pending(dbentry, |d| d.conflict_logicalslot += 1);
        }
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            with_db_pending(dbentry, |d| d.conflict_startup_deadlock += 1);
        }
        // The non-recovery-conflict ProcSignalReason values never reach here.
        _ => {}
    }
    Ok(())
}

/// Port of `void pgstat_report_deadlock(void)` — report a detected deadlock.
pub fn pgstat_report_deadlock() -> PgResult<()> {
    if !pgstat_track_counts() {
        return Ok(());
    }
    let dbent = pgstat_prep_database_pending(my_database_id::call())?;
    with_db_pending(dbent, |d| d.deadlocks += 1);
    Ok(())
}

/// Port of `void pgstat_prepare_report_checksum_failure(Oid dboid)` — allow this
/// backend to later report checksum failures for `dboid`, even if in a critical
/// section at the time of the report (ensure an entry ref exists, so no DSM
/// mapping / allocation is needed later).
pub fn pgstat_prepare_report_checksum_failure(dboid: Oid) -> PgResult<()> {
    // Assert(!CritSectionCount);
    shmem::pgstat_get_entry_ref(PGSTAT_KIND_DATABASE, dboid, INVALID_OBJID, true, None)?;
    Ok(())
}

/// Port of `void pgstat_report_checksum_failures_in_db(Oid dboid, int
/// failurecount)` — report one or more checksum failures by writing the shared
/// stats directly (no allocation, so it is safe in a critical section, provided
/// `pgstat_prepare_report_checksum_failure` ran first).
pub fn pgstat_report_checksum_failures_in_db(dboid: Oid, failurecount: i32) -> PgResult<()> {
    if !pgstat_track_counts() {
        return Ok(());
    }

    // create=false: we must not require allocations here.
    let entry_ref = shmem::pgstat_get_entry_ref(PGSTAT_KIND_DATABASE, dboid, INVALID_OBJID, false, None)?;
    let entry_ref = match entry_ref {
        Some(er) => er,
        None => {
            // elog(WARNING, ...) — should always have been created; don't crash.
            return Ok(());
        }
    };

    // SAFETY: just-resolved live reference.
    let er = unsafe { entry_ref.get() };
    // (void) pgstat_lock_entry(entry_ref, false);
    shmem::pgstat_lock_entry(er, false)?;

    // sharedent = (PgStatShared_Database *) entry_ref->shared_stats;
    let sharedent = unsafe { &mut *(er.shared_stats as *mut PgStatShared_Database) };
    sharedent.stats.checksum_failures += failurecount as PgStat_Counter;
    sharedent.stats.last_checksum_failure = get_current_timestamp::call();

    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Pending-stats report paths.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_report_tempfile(size_t filesize)` — report creation of a
/// temporary file.
pub fn pgstat_report_tempfile(filesize: u64) -> PgResult<()> {
    if !pgstat_track_counts() {
        return Ok(());
    }
    let dbent = pgstat_prep_database_pending(my_database_id::call())?;
    with_db_pending(dbent, |d| {
        d.temp_bytes += filesize as PgStat_Counter;
        d.temp_files += 1;
    });
    Ok(())
}

/// Port of `void pgstat_report_connect(Oid dboid)` — notify stats system of a
/// new connection.
pub fn pgstat_report_connect(_dboid: Oid) -> PgResult<()> {
    if !pgstat_should_report_connstat() {
        return Ok(());
    }

    // pgLastSessionReportTime = MyStartTimestamp;
    PG_LAST_SESSION_REPORT_TIME.with(|c| c.set(my_start_timestamp::call()));

    let dbentry = pgstat_prep_database_pending(my_database_id::call())?;
    with_db_pending(dbentry, |d| d.sessions += 1);
    Ok(())
}

/// Port of `void pgstat_report_disconnect(Oid dboid)` — notify the stats system
/// of a disconnect.
pub fn pgstat_report_disconnect(_dboid: Oid) -> PgResult<()> {
    if !pgstat_should_report_connstat() {
        return Ok(());
    }

    let dbentry = pgstat_prep_database_pending(my_database_id::call())?;
    match pgstat_session_end_cause() {
        // we don't collect these
        SessionEndType::DISCONNECT_NOT_YET | SessionEndType::DISCONNECT_NORMAL => {}
        SessionEndType::DISCONNECT_CLIENT_EOF => {
            with_db_pending(dbentry, |d| d.sessions_abandoned += 1);
        }
        SessionEndType::DISCONNECT_FATAL => {
            with_db_pending(dbentry, |d| d.sessions_fatal += 1);
        }
        SessionEndType::DISCONNECT_KILLED => {
            with_db_pending(dbentry, |d| d.sessions_killed += 1);
        }
    }
    Ok(())
}

/// Port of `void pgstat_update_parallel_workers_stats(PgStat_Counter
/// workers_to_launch, PgStat_Counter workers_launched)`.
pub fn pgstat_update_parallel_workers_stats(
    workers_to_launch: PgStat_Counter,
    workers_launched: PgStat_Counter,
) -> PgResult<()> {
    if !OidIsValid(my_database_id::call()) {
        return Ok(());
    }
    let dbentry = pgstat_prep_database_pending(my_database_id::call())?;
    with_db_pending(dbentry, |d| {
        d.parallel_workers_to_launch += workers_to_launch;
        d.parallel_workers_launched += workers_launched;
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Fetch.
// ---------------------------------------------------------------------------

/// Port of `PgStat_StatDBEntry *pgstat_fetch_stat_dbentry(Oid dboid)` — the
/// collected statistics for one database, or `None`.
pub fn pgstat_fetch_stat_dbentry(dboid: Oid) -> PgResult<Option<PgStat_StatDBEntry>> {
    let bytes = pgstat_core::pgstat_fetch_entry(PGSTAT_KIND_DATABASE, dboid, INVALID_OBJID)?;
    Ok(bytes.map(|b| decode_db_entry(&b)))
}

/// Decode the `shared_data_len` bytes `pgstat_fetch_entry` copies out into the
/// typed `PgStat_StatDBEntry` (C's `(PgStat_StatDBEntry *) ...`).
fn decode_db_entry(bytes: &[u8]) -> PgStat_StatDBEntry {
    assert_eq!(
        bytes.len(),
        core::mem::size_of::<PgStat_StatDBEntry>(),
        "pgstat_fetch_stat_dbentry: unexpected stats blob size"
    );
    // SAFETY: the blob is exactly a `PgStat_StatDBEntry` (a Copy, pointer-free
    // POD), copied byte-for-byte by pgstat_fetch_entry.
    unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const PgStat_StatDBEntry) }
}

// ---------------------------------------------------------------------------
// End-of-xact + report_stat subroutine.
// ---------------------------------------------------------------------------

/// Port of `void AtEOXact_PgStat_Database(bool isCommit, bool parallel)` — count
/// one transaction commit/abort (skipped for parallel workers).
pub fn AtEOXact_PgStat_Database(isCommit: bool, parallel: bool) {
    // Don't count parallel worker transaction stats.
    if !parallel {
        if isCommit {
            PG_STAT_XACT_COMMIT.with(|c| c.set(c.get() + 1));
        } else {
            PG_STAT_XACT_ROLLBACK.with(|c| c.set(c.get() + 1));
        }
    }
}

/// Port of `void pgstat_update_dbstats(TimestampTz ts)` — subroutine for
/// `pgstat_report_stat()`: handle xact commit/rollback and I/O timings.
pub fn pgstat_update_dbstats(ts: TimestampTz) -> PgResult<()> {
    // If not connected to a database yet, don't attribute time to "shared state".
    if !OidIsValid(my_database_id::call()) {
        return Ok(());
    }

    let dbentry = pgstat_prep_database_pending(my_database_id::call())?;

    let xact_commit = PG_STAT_XACT_COMMIT.with(|c| c.get()) as PgStat_Counter;
    let xact_rollback = PG_STAT_XACT_ROLLBACK.with(|c| c.get()) as PgStat_Counter;
    let blk_read_time = PG_STAT_BLOCK_READ_TIME.with(|c| c.get());
    let blk_write_time = PG_STAT_BLOCK_WRITE_TIME.with(|c| c.get());

    with_db_pending(dbentry, |d| {
        d.xact_commit += xact_commit;
        d.xact_rollback += xact_rollback;
        d.blk_read_time += blk_read_time;
        d.blk_write_time += blk_write_time;
    });

    if pgstat_should_report_connstat() {
        // pgLastSessionReportTime is initialized to MyStartTimestamp by
        // pgstat_report_connect().
        let last = PG_LAST_SESSION_REPORT_TIME.with(|c| c.get());
        let (secs, usecs) = timestamp_difference::call(last, ts);
        PG_LAST_SESSION_REPORT_TIME.with(|c| c.set(ts));
        let active_time = PG_STAT_ACTIVE_TIME.with(|c| c.get());
        let idle_time = PG_STAT_TRANSACTION_IDLE_TIME.with(|c| c.get());
        with_db_pending(dbentry, |d| {
            d.session_time += secs * 1_000_000 + usecs as PgStat_Counter;
            d.active_time += active_time;
            d.idle_in_transaction_time += idle_time;
        });
    }

    PG_STAT_XACT_COMMIT.with(|c| c.set(0));
    PG_STAT_XACT_ROLLBACK.with(|c| c.set(0));
    PG_STAT_BLOCK_READ_TIME.with(|c| c.set(0));
    PG_STAT_BLOCK_WRITE_TIME.with(|c| c.set(0));
    PG_STAT_ACTIVE_TIME.with(|c| c.set(0));
    PG_STAT_TRANSACTION_IDLE_TIME.with(|c| c.set(0));
    Ok(())
}

/// Port of `static bool pgstat_should_report_connstat(void)` — we report session
/// statistics only for normal backend processes.
fn pgstat_should_report_connstat() -> bool {
    my_backend_type::call() == BackendType::Backend
}

// ---------------------------------------------------------------------------
// Pending block prep + reach.
// ---------------------------------------------------------------------------

/// Port of `PgStat_StatDBEntry *pgstat_prep_database_pending(Oid dboid)` — find
/// or create a local `PgStat_StatDBEntry` pending entry for `dboid`.
///
/// Returns the entry-ref pointer (mirroring C's `entry_ref->pending`); the
/// pending block is reached/mutated via [`with_db_pending`], because in this
/// model the pending block is the owner-private `Box<dyn Any>` on the entry-ref.
pub fn pgstat_prep_database_pending(dboid: Oid) -> PgResult<shmem::EntryRefPtr> {
    // Assert(!OidIsValid(dboid) || OidIsValid(MyDatabaseId));
    pgstat_core::pgstat_prep_pending_entry(PGSTAT_KIND_DATABASE, dboid, INVALID_OBJID, new_pending_db)
}

/// The backend-local pending block for a database entry: C allocates a zeroed
/// `PgStat_StatDBEntry` (`pending_size`).
fn new_pending_db() -> Box<dyn core::any::Any> {
    Box::new(PgStat_StatDBEntry::default())
}

/// Mutate the just-prepped entry-ref's pending block as a `PgStat_StatDBEntry`.
fn with_db_pending<R>(entry_ref: shmem::EntryRefPtr, f: impl FnOnce(&mut PgStat_StatDBEntry) -> R) -> R {
    // SAFETY: a just-prepped live reference whose pending was just ensured present.
    let er = unsafe { entry_ref.get() };
    let pending = er
        .pending
        .as_mut()
        .expect("database entry_ref has no pending after prep")
        .downcast_mut::<PgStat_StatDBEntry>()
        .expect("database pending is not a PgStat_StatDBEntry");
    f(pending)
}

/// Port of `void pgstat_reset_database_timestamp(Oid dboid, TimestampTz ts)` —
/// reset the database's reset timestamp without resetting the stats contents.
pub fn pgstat_reset_database_timestamp(_dboid: Oid, ts: TimestampTz) -> PgResult<()> {
    let dbref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_DATABASE,
        my_database_id::call(),
        INVALID_OBJID,
        false,
    )?
    .expect("pgstat_reset_database_timestamp: get_entry_ref_locked returned None");

    // SAFETY: just-resolved, content-locked live reference.
    let er = unsafe { dbref.get() };
    let dbentry = unsafe { &mut *(er.shared_stats as *mut PgStatShared_Database) };
    dbentry.stats.stat_reset_timestamp = ts;

    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Callbacks.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_database_flush_cb(PgStat_EntryRef *entry_ref, bool
/// nowait)` — flush out pending stats for the entry.
///
/// Returns `Ok(false)` (C `false`) if `nowait` and the lock could not be
/// acquired (no flush); `Ok(true)` (C `true`) otherwise.
pub fn pgstat_database_flush_cb(entry_ref: &mut PgStat_EntryRef, nowait: bool) -> PgResult<bool> {
    // pendingent = (PgStat_StatDBEntry *) entry_ref->pending;
    let pendingent: PgStat_StatDBEntry = *entry_ref
        .pending
        .as_ref()
        .expect("database flush: entry_ref has no pending")
        .downcast_ref::<PgStat_StatDBEntry>()
        .expect("database pending is not a PgStat_StatDBEntry");

    if !shmem::pgstat_lock_entry(entry_ref, nowait)? {
        return Ok(false);
    }

    // sharedent = (PgStatShared_Database *) entry_ref->shared_stats;
    // SAFETY: shared_stats points at a live PgStatShared_Database; lock held.
    let sharedent = unsafe { &mut *(entry_ref.shared_stats as *mut PgStatShared_Database) };
    let s = &mut sharedent.stats;

    // PGSTAT_ACCUM_DBCOUNT(item): s.item += pendingent.item
    s.xact_commit += pendingent.xact_commit;
    s.xact_rollback += pendingent.xact_rollback;
    s.blocks_fetched += pendingent.blocks_fetched;
    s.blocks_hit += pendingent.blocks_hit;

    s.tuples_returned += pendingent.tuples_returned;
    s.tuples_fetched += pendingent.tuples_fetched;
    s.tuples_inserted += pendingent.tuples_inserted;
    s.tuples_updated += pendingent.tuples_updated;
    s.tuples_deleted += pendingent.tuples_deleted;

    // last_autovac_time is reported immediately.
    debug_assert_eq!(pendingent.last_autovac_time, 0);

    s.conflict_tablespace += pendingent.conflict_tablespace;
    s.conflict_lock += pendingent.conflict_lock;
    s.conflict_snapshot += pendingent.conflict_snapshot;
    s.conflict_logicalslot += pendingent.conflict_logicalslot;
    s.conflict_bufferpin += pendingent.conflict_bufferpin;
    s.conflict_startup_deadlock += pendingent.conflict_startup_deadlock;

    s.temp_bytes += pendingent.temp_bytes;
    s.temp_files += pendingent.temp_files;
    s.deadlocks += pendingent.deadlocks;

    // checksum failures are reported immediately.
    debug_assert_eq!(pendingent.checksum_failures, 0);
    debug_assert_eq!(pendingent.last_checksum_failure, 0);

    s.blk_read_time += pendingent.blk_read_time;
    s.blk_write_time += pendingent.blk_write_time;

    s.sessions += pendingent.sessions;
    s.session_time += pendingent.session_time;
    s.active_time += pendingent.active_time;
    s.idle_in_transaction_time += pendingent.idle_in_transaction_time;
    s.sessions_abandoned += pendingent.sessions_abandoned;
    s.sessions_fatal += pendingent.sessions_fatal;
    s.sessions_killed += pendingent.sessions_killed;
    s.parallel_workers_to_launch += pendingent.parallel_workers_to_launch;
    s.parallel_workers_launched += pendingent.parallel_workers_launched;

    shmem::pgstat_unlock_entry(entry_ref)?;

    // memset(pendingent, 0, sizeof(*pendingent));
    if let Some(p) = entry_ref.pending.as_mut() {
        if let Some(d) = p.downcast_mut::<PgStat_StatDBEntry>() {
            *d = PgStat_StatDBEntry::default();
        }
    }

    Ok(true)
}

/// Port of `void pgstat_database_reset_timestamp_cb(PgStatShared_Common *header,
/// TimestampTz ts)`.
fn pgstat_database_reset_timestamp_cb(header: &mut PgStatShared_Common, ts: TimestampTz) {
    // ((PgStatShared_Database *) header)->stats.stat_reset_timestamp = ts;
    // SAFETY: the kind table only hands this cb the PgStatShared_Common embedded
    // as the first field of a PgStatShared_Database.
    let shdb = unsafe { &mut *((header as *mut PgStatShared_Common) as *mut PgStatShared_Database) };
    shdb.stats.stat_reset_timestamp = ts;
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// `OidIsValid(oid)` (`c.h`).
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `pgstat_track_counts` GUC (`globals`).
fn pgstat_track_counts() -> bool {
    backend_utils_misc_guc_tables::vars::pgstat_track_counts.read()
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_DATABASE`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_DATABASE]`).
fn database_kind_info() -> types_pgstat::pgstat_internal::PgStat_KindInfo {
    types_pgstat::pgstat_internal::PgStat_KindInfo {
        fixed_amount: false,
        // so pg_stat_database entries can be seen in all databases
        accessed_across_databases: true,
        write_to_file: true,
        shared_size: core::mem::size_of::<PgStatShared_Database>() as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: core::mem::size_of::<PgStat_StatDBEntry>() as u32,
        pending_size: core::mem::size_of::<PgStat_StatDBEntry>() as u32,
        name: "database",
    }
}

/// Register `PGSTAT_KIND_DATABASE` and install the database outward seams.
///
/// Must run before `backend_utils_activity_pgstat::init_seams()` seals the
/// per-kind table.
pub fn init_seams() {
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_DATABASE, database_kind_info())
            .flush_pending_cb(pgstat_database_flush_cb)
            .reset_timestamp_cb(pgstat_database_reset_timestamp_cb)
            // On-disk (de)serialization: the bytes are the C image of the
            // `PgStat_StatDBEntry` body following the `PgStatShared_Common` header.
            .read_var_cb(|header, bytes| {
                // SAFETY: header points at a live PgStatShared_Database body.
                let shdb = unsafe { &mut *(header as *mut PgStatShared_Database) };
                shdb.stats = backend_utils_activity_pgstat::kind_info::pgstat_deserialize_pod::<
                    PgStat_StatDBEntry,
                >(bytes);
                Ok(())
            })
            .write_var_cb(|header| {
                // SAFETY: header points at a live PgStatShared_Database body.
                let shdb = unsafe { &*(header as *const PgStatShared_Database) };
                backend_utils_activity_pgstat::kind_info::pgstat_serialize_pod(&shdb.stats)
            }),
    );

    // pgstat_database.c outward seams with live callers.
    backend_tcop_postgres_seams::pgstat_report_recovery_conflict::set(|reason| {
        // The tcop caller discards errors; the only failure surface here is the
        // pending-entry allocation, which on this path is benign — surface a
        // panic on a genuine OOM rather than silently dropping the conflict.
        pgstat_report_recovery_conflict(reason).expect("pgstat_report_recovery_conflict failed");
    });
    backend_utils_activity_stat_seams::report_deadlock::set(|| {
        pgstat_report_deadlock().expect("pgstat_report_deadlock failed");
    });
    backend_utils_activity_stat_seams::pgstat_report_tempfile::set(|filesize| {
        pgstat_report_tempfile(filesize).expect("pgstat_report_tempfile failed");
    });
    backend_utils_activity_stat_seams::pgstat_count_buffer_read_time::set(|usecs| {
        PG_STAT_BLOCK_READ_TIME.with(|c| c.set(c.get() + usecs as PgStat_Counter));
    });
    backend_utils_activity_stat_seams::pgstat_count_buffer_write_time::set(|usecs| {
        PG_STAT_BLOCK_WRITE_TIME.with(|c| c.set(c.get() + usecs as PgStat_Counter));
    });
    // backend_status.c calls these when a backend leaves the active /
    // idle-in-transaction state (pgstat.h macros: += into the module statics).
    backend_utils_activity_pgstat_database_seams::pgstat_count_conn_active_time::set(|usecs| {
        pgstat_count_conn_active_time(usecs as PgStat_Counter);
    });
    backend_utils_activity_pgstat_database_seams::pgstat_count_conn_txn_idle_time::set(|usecs| {
        pgstat_count_conn_txn_idle_time(usecs as PgStat_Counter);
    });
    backend_utils_activity_stat_seams::at_eoxact_pgstat_database::set(AtEOXact_PgStat_Database);
    backend_utils_activity_stat_seams::pgstat_set_session_end_cause_fatal::set(|| {
        // elog.c FATAL path: only mark fatal if no other cause is known.
        if pgstat_session_end_cause() == SessionEndType::DISCONNECT_NORMAL {
            set_pgstat_session_end_cause(SessionEndType::DISCONNECT_FATAL);
        }
    });
    // `pgStatSessionEndCause = DISCONNECT_KILLED;` (tcop/postgres.c:3036, `die()`)
    // — record the session as terminated by an administrator. The
    // `pgStatSessionEndCause` session-stats global is owned here; C sets it
    // unconditionally on this path.
    backend_tcop_postgres_seams::set_session_end_cause_killed::set(|| {
        set_pgstat_session_end_cause(SessionEndType::DISCONNECT_KILLED);
    });

    backend_utils_activity_pgstat_seams::pgstat_drop_database::set(pgstat_drop_database);
    backend_utils_activity_pgstat_seams::pgstat_prepare_report_checksum_failure::set(
        pgstat_prepare_report_checksum_failure,
    );
    backend_utils_activity_pgstat_seams::pgstat_report_checksum_failures_in_db::set(
        pgstat_report_checksum_failures_in_db,
    );

    backend_postmaster_autovacuum_ext_seams::pgstat_report_autovac::set(|dbid| {
        pgstat_report_autovac(dbid).expect("pgstat_report_autovac failed");
    });
    backend_postmaster_autovacuum_ext_seams::pgstat_fetch_stat_dbentry::set(|datid| {
        pgstat_fetch_stat_dbentry(datid)
            .expect("pgstat_fetch_stat_dbentry failed")
            .map(|e| types_autovacuum::DbStatEntry {
                last_autovac_time: e.last_autovac_time,
            })
    });

    // --- lazy-vacuum driver's I/O-timing accumulator reads (vacuumlazy.c
    //     logging). `pgStatBlockReadTime` / `pgStatBlockWriteTime` are this
    //     file's backend-local globals; the read seams home in vacuumlazy-seams. ---
    backend_access_heap_vacuumlazy_seams::pgstat_block_read_time::set(|| {
        Ok(PG_STAT_BLOCK_READ_TIME.with(|c| c.get()))
    });
    backend_access_heap_vacuumlazy_seams::pgstat_block_write_time::set(|| {
        Ok(PG_STAT_BLOCK_WRITE_TIME.with(|c| c.get()))
    });
}
