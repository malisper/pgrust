// pgstat_database.c — the per-database pending entry plus the backend-wide
// accumulators pgstat_report_stat folds into it. Shared-entry paths
// (drop_database apply, report_autovac, checksum failures, reset timestamps)
// and connstat session times (need MyBackendType) are phase 2.

use core::cell::Cell;

use init_small::globals::MyDatabaseId;
use types_core::{InvalidOid, Oid, TimestampTz};

use crate::pending::{self, PendingData, PgStatState, PgStat_HashKey, PGSTAT_KIND_DATABASE};
use crate::xact;
use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
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

pub fn pgstat_report_deadlock() {
    if !crate::pgstat_track_counts() {
        return;
    }
    pending::with_state(|st| {
        pgstat_prep_database_pending_in(st, MyDatabaseId()).deadlocks += 1;
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

pub(crate) fn pgstat_update_dbstats(_ts: TimestampTz) {
    if MyDatabaseId() == InvalidOid {
        return;
    }
    pending::with_state(|st| {
        let dbentry = pgstat_prep_database_pending_in(st, MyDatabaseId());
        dbentry.xact_commit += XACT_COMMIT.with(|c| c.replace(0)) as PgStat_Counter;
        dbentry.xact_rollback += XACT_ROLLBACK.with(|c| c.replace(0)) as PgStat_Counter;
        dbentry.blk_read_time += BLOCK_READ_TIME.with(|c| c.replace(0));
        dbentry.blk_write_time += BLOCK_WRITE_TIME.with(|c| c.replace(0));
        // C's pgstat_should_report_connstat session/active/idle fold: phase 2.
    });
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
