// pgstat_database.c — the per-database pending entry plus the backend-wide
// accumulators pgstat_report_stat folds into it.

use core::cell::Cell;

use init_small::globals::MyDatabaseId;
use types_core::{InvalidOid, Oid, TimestampTz};

use crate::pending::{self, PendingData, PgStatState, PgStat_HashKey, PGSTAT_KIND_DATABASE};
use crate::xact;
use crate::PgStat_Counter;

// repr(C), all-i64 fields: statsfile serialization copies these as bytes.
#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_StatDBEntry {
    pub xact_commit: PgStat_Counter,
    pub xact_rollback: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub last_autovac_time: TimestampTz,
    pub conflict_tablespace: PgStat_Counter,
    pub conflict_lock: PgStat_Counter,
    pub conflict_snapshot: PgStat_Counter,
    pub conflict_logicalslot: PgStat_Counter,
    pub conflict_bufferpin: PgStat_Counter,
    pub conflict_startup_deadlock: PgStat_Counter,
    pub temp_files: PgStat_Counter,
    pub temp_bytes: PgStat_Counter,
    pub deadlocks: PgStat_Counter,
    pub checksum_failures: PgStat_Counter,
    pub last_checksum_failure: TimestampTz,
    pub blk_read_time: PgStat_Counter,
    pub blk_write_time: PgStat_Counter,
    pub sessions: PgStat_Counter,
    pub session_time: PgStat_Counter,
    pub active_time: PgStat_Counter,
    pub idle_in_transaction_time: PgStat_Counter,
    pub sessions_abandoned: PgStat_Counter,
    pub sessions_fatal: PgStat_Counter,
    pub sessions_killed: PgStat_Counter,
    pub parallel_workers_to_launch: PgStat_Counter,
    pub parallel_workers_launched: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SessionEndType {
    DisconnectNotYet,
    DisconnectNormal,
    DisconnectClientEof,
    DisconnectFatal,
    DisconnectKilled,
}

thread_local! {
    static BLOCK_READ_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    static BLOCK_WRITE_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    static ACTIVE_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    static TRANSACTION_IDLE_TIME: Cell<PgStat_Counter> = const { Cell::new(0) };
    static SESSION_END_CAUSE: Cell<SessionEndType> =
        const { Cell::new(SessionEndType::DisconnectNormal) };
    static XACT_COMMIT: Cell<i32> = const { Cell::new(0) };
    static XACT_ROLLBACK: Cell<i32> = const { Cell::new(0) };
    static LAST_SESSION_REPORT_TIME: Cell<TimestampTz> = const { Cell::new(0) };
}

pub fn pgstat_count_buffer_read_time(n: PgStat_Counter) {
    BLOCK_READ_TIME.with(|c| c.set(c.get() + n));
}

pub fn pgstat_count_buffer_write_time(n: PgStat_Counter) {
    BLOCK_WRITE_TIME.with(|c| c.set(c.get() + n));
}

pub fn pgstat_count_conn_active_time(n: PgStat_Counter) {
    ACTIVE_TIME.with(|c| c.set(c.get() + n));
}

pub fn pgstat_count_conn_txn_idle_time(n: PgStat_Counter) {
    TRANSACTION_IDLE_TIME.with(|c| c.set(c.get() + n));
}

pub fn pgstat_session_end_cause() -> SessionEndType {
    SESSION_END_CAUSE.with(|c| c.get())
}

pub fn pgstat_set_session_end_cause(cause: SessionEndType) {
    SESSION_END_CAUSE.with(|c| c.set(cause));
}

// elog.c's FATAL path: only a so-far-normal session becomes DISCONNECT_FATAL.
pub fn pgstat_set_session_end_cause_fatal() {
    SESSION_END_CAUSE.with(|c| {
        if c.get() == SessionEndType::DisconnectNormal {
            c.set(SessionEndType::DisconnectFatal);
        }
    });
}

fn pgstat_should_report_connstat() -> bool {
    miscinit::GetMyBackendType() == types_core::BackendType::Backend
}

pub fn pgstat_report_connect(dboid: Oid) {
    debug_assert_eq!(dboid, MyDatabaseId());
    if !pgstat_should_report_connstat() {
        return;
    }
    LAST_SESSION_REPORT_TIME.with(|c| c.set(init_small::globals::MyStartTimestamp()));
    pending::with_state(|st| {
        pgstat_prep_database_pending_in(st, MyDatabaseId()).sessions += 1;
    });
}

pub fn pgstat_report_disconnect(dboid: Oid) {
    debug_assert_eq!(dboid, MyDatabaseId());
    if !pgstat_should_report_connstat() {
        return;
    }
    pending::with_state(|st| {
        let dbentry = pgstat_prep_database_pending_in(st, MyDatabaseId());
        match SESSION_END_CAUSE.with(|c| c.get()) {
            SessionEndType::DisconnectNotYet | SessionEndType::DisconnectNormal => {}
            SessionEndType::DisconnectClientEof => dbentry.sessions_abandoned += 1,
            SessionEndType::DisconnectFatal => dbentry.sessions_fatal += 1,
            SessionEndType::DisconnectKilled => dbentry.sessions_killed += 1,
        }
    });
}

pub fn pgstat_drop_database(databaseid: Oid) {
    xact::pgstat_drop_transactional(PGSTAT_KIND_DATABASE, databaseid, 0);
}

/// pgstat_report_recovery_conflict (pgstat_database.c:81).
pub fn pgstat_report_recovery_conflict(reason: ::types_storage::storage::ProcSignalReason) {
    use ::types_storage::storage::ProcSignalReason::*;
    if !crate::pgstat_track_counts() {
        return;
    }
    pending::with_state(|st| {
        let dbent = pgstat_prep_database_pending_in(st, MyDatabaseId());
        match reason {
            // The database's stats drop as soon as the drop replicates: no
            // point counting PROCSIG_RECOVERY_CONFLICT_DATABASE.
            PROCSIG_RECOVERY_CONFLICT_DATABASE => {}
            PROCSIG_RECOVERY_CONFLICT_TABLESPACE => dbent.conflict_tablespace += 1,
            PROCSIG_RECOVERY_CONFLICT_LOCK => dbent.conflict_lock += 1,
            PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => dbent.conflict_snapshot += 1,
            PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => dbent.conflict_bufferpin += 1,
            PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => dbent.conflict_logicalslot += 1,
            PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => dbent.conflict_startup_deadlock += 1,
            _ => {}
        }
    });
}

pub fn pgstat_report_deadlock() {
    if !crate::pgstat_track_counts() {
        return;
    }
    pending::with_state(|st| {
        pgstat_prep_database_pending_in(st, MyDatabaseId()).deadlocks += 1;
    });
}

/// pgstat_prepare_report_checksum_failure (pgstat_database.c): pre-create the
/// database's shared entry so the later report — which may run in a critical
/// section — is a plain lookup-and-bump.
pub fn pgstat_prepare_report_checksum_failure(dboid: Oid) {
    let key = PgStat_HashKey {
        kind: PGSTAT_KIND_DATABASE,
        dboid,
        objid: 0,
    };
    crate::shmem::update_database_entry(key, |_| {});
}

/// pgstat_report_checksum_failures_in_db (pgstat_database.c): checksum
/// failures update the shared entry directly (C bypasses pending so the
/// report works in critical sections; they're also never common enough for
/// pending to matter).
pub fn pgstat_report_checksum_failures_in_db(dboid: Oid, failurecount: i64) {
    if !crate::pgstat_track_counts() {
        return;
    }
    let key = PgStat_HashKey {
        kind: PGSTAT_KIND_DATABASE,
        dboid,
        objid: 0,
    };
    crate::shmem::update_database_entry(key, |dbentry| {
        dbentry.checksum_failures += failurecount;
        dbentry.last_checksum_failure = timestamp_seams::get_current_timestamp::call();
    });
}

pub fn pgstat_report_tempfile(filesize: u64) {
    if !crate::pgstat_track_counts() {
        return;
    }
    pending::with_state(|st| {
        let dbent = pgstat_prep_database_pending_in(st, MyDatabaseId());
        dbent.temp_bytes += filesize as PgStat_Counter;
        dbent.temp_files += 1;
    });
}

pub(crate) fn AtEOXact_PgStat_Database(isCommit: bool, parallel: bool) {
    if !parallel {
        if isCommit {
            XACT_COMMIT.with(|c| c.set(c.get() + 1));
        } else {
            XACT_ROLLBACK.with(|c| c.set(c.get() + 1));
        }
    }
}

pub fn pgstat_update_parallel_workers_stats(
    workers_to_launch: PgStat_Counter,
    workers_launched: PgStat_Counter,
) {
    if MyDatabaseId() == InvalidOid {
        return;
    }
    pending::with_state(|st| {
        let dbentry = pgstat_prep_database_pending_in(st, MyDatabaseId());
        dbentry.parallel_workers_to_launch += workers_to_launch;
        dbentry.parallel_workers_launched += workers_launched;
    });
}

pub(crate) fn pgstat_update_dbstats(ts: TimestampTz) {
    if MyDatabaseId() == InvalidOid {
        return;
    }
    pending::with_state(|st| {
        let dbentry = pgstat_prep_database_pending_in(st, MyDatabaseId());
        dbentry.xact_commit += XACT_COMMIT.with(|c| c.replace(0)) as PgStat_Counter;
        dbentry.xact_rollback += XACT_ROLLBACK.with(|c| c.replace(0)) as PgStat_Counter;
        dbentry.blk_read_time += BLOCK_READ_TIME.with(|c| c.replace(0));
        dbentry.blk_write_time += BLOCK_WRITE_TIME.with(|c| c.replace(0));
        if pgstat_should_report_connstat() {
            let last = LAST_SESSION_REPORT_TIME.with(|c| c.get());
            if last > 0 && ts > last {
                dbentry.session_time += ts - last;
            }
            if last > 0 {
                LAST_SESSION_REPORT_TIME.with(|c| c.set(ts));
            }
            dbentry.active_time += ACTIVE_TIME.with(|c| c.get());
            dbentry.idle_in_transaction_time += TRANSACTION_IDLE_TIME.with(|c| c.get());
        }
        ACTIVE_TIME.with(|c| c.set(0));
        TRANSACTION_IDLE_TIME.with(|c| c.set(0));
    });
}

pub fn pgstat_report_autovac(dboid: Oid) {
    let key = PgStat_HashKey {
        kind: PGSTAT_KIND_DATABASE,
        dboid,
        objid: 0,
    };
    crate::shmem::update_database_entry(key, |dbentry| {
        dbentry.last_autovac_time = timestamp_seams::get_current_timestamp::call();
    });
}

pub fn pgstat_fetch_stat_dbentry(dboid: Oid) -> Option<PgStat_StatDBEntry> {
    let key = PgStat_HashKey {
        kind: PGSTAT_KIND_DATABASE,
        dboid,
        objid: 0,
    };
    match crate::shmem::fetch_entry(key) {
        Some(crate::shmem::SharedEntry::Database(d)) => Some(d),
        Some(_) => unreachable!("database key holds non-database shared entry"),
        None => None,
    }
}

pub(crate) fn pgstat_prep_database_pending_in<'a>(
    st: &'a mut PgStatState,
    dboid: Oid,
) -> &'a mut PgStat_StatDBEntry {
    debug_assert!(dboid == InvalidOid || MyDatabaseId() != InvalidOid);
    let key = PgStat_HashKey {
        kind: PGSTAT_KIND_DATABASE,
        dboid,
        objid: 0,
    };
    match st.prep_pending_entry(key) {
        PendingData::Database(db) => db,
        _ => unreachable!("database key holds non-database pending data"),
    }
}
