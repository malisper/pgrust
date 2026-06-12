//! pgstat statistics types shared across the per-kind pgstat ports
//! (`pgstat.h`, `utils/pgstat_internal.h`), trimmed to the archiver and
//! checkpointer kinds consumed so far.
//!
//! The `lock` field of each `PgStatShared_*` struct is the real shmem-resident
//! [`LWLock`] from C's `PgStatShared_Common` header; the ported
//! `LWLockInitialize`/`LWLockAcquire`/`LWLockRelease` operate on it directly.

use types_core::primitive::TimestampTz;
use types_storage::LWLock;

/// `PgStat_Counter` (`pgstat.h`): `typedef int64 PgStat_Counter;`.
pub type PgStat_Counter = i64;

/// `MAX_XFN_CHARS` (`postmaster/pgarch.h`): max length of an XLOG filename.
pub const MAX_XFN_CHARS: usize = 40;

/// Size of the WAL-filename byte buffers — `char[MAX_XFN_CHARS + 1]` in C.
pub const WAL_NAME_LEN: usize = MAX_XFN_CHARS + 1;

/// `PgStat_ArchiverStats` (`pgstat.h`). Field order matches C exactly.
///
/// The WAL-name fields are fixed `char[MAX_XFN_CHARS + 1]` buffers in C;
/// modeled as fixed `[u8; WAL_NAME_LEN]` byte arrays, preserving the exact
/// size and the NUL-terminated-string semantics (clearing via `wal[0] = 0`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_ArchiverStats {
    /// archival successes
    pub archived_count: PgStat_Counter,
    /// last WAL file archived (`char last_archived_wal[MAX_XFN_CHARS + 1]`)
    pub last_archived_wal: [u8; WAL_NAME_LEN],
    /// last archival success time
    pub last_archived_timestamp: TimestampTz,
    /// failed archival attempts
    pub failed_count: PgStat_Counter,
    /// WAL file involved in last failure (`char last_failed_wal[MAX_XFN_CHARS + 1]`)
    pub last_failed_wal: [u8; WAL_NAME_LEN],
    /// last archival failure time
    pub last_failed_timestamp: TimestampTz,
    pub stat_reset_timestamp: TimestampTz,
}

impl Default for PgStat_ArchiverStats {
    fn default() -> Self {
        PgStat_ArchiverStats {
            archived_count: 0,
            last_archived_wal: [0; WAL_NAME_LEN],
            last_archived_timestamp: 0,
            failed_count: 0,
            last_failed_wal: [0; WAL_NAME_LEN],
            last_failed_timestamp: 0,
            stat_reset_timestamp: 0,
        }
    }
}

/// `PgStatShared_Archiver` (`utils/pgstat_internal.h`). Field order matches C.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgStatShared_Archiver {
    /// lock protects `reset_offset` as well as `stats.stat_reset_timestamp`
    pub lock: LWLock,
    pub changecount: u32,
    pub stats: PgStat_ArchiverStats,
    pub reset_offset: PgStat_ArchiverStats,
}

/// `PgStat_CheckpointerStats` (`pgstat.h`). Field order matches C exactly.
///
/// This struct should contain only actual event counters (plus the reset
/// timestamp), because `pg_memory_is_all_zeros()` is used to detect whether
/// there are any stats updates to apply.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_CheckpointerStats {
    pub num_timed: PgStat_Counter,
    pub num_requested: PgStat_Counter,
    pub num_performed: PgStat_Counter,
    pub restartpoints_timed: PgStat_Counter,
    pub restartpoints_requested: PgStat_Counter,
    pub restartpoints_performed: PgStat_Counter,
    /// times in milliseconds
    pub write_time: PgStat_Counter,
    pub sync_time: PgStat_Counter,
    pub buffers_written: PgStat_Counter,
    pub slru_written: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_CheckpointerStats {
    /// Equivalent of C's `pg_memory_is_all_zeros(&PendingCheckpointerStats,
    /// sizeof(struct PgStat_CheckpointerStats))` (`utils/memutils.h`): true iff
    /// every byte of the struct is zero. The struct is a plain record of
    /// integer fields, so an all-zero byte image is exactly all fields == 0.
    pub fn is_all_zeros(&self) -> bool {
        *self == PgStat_CheckpointerStats::default()
    }
}

/// `PgStatShared_Checkpointer` (`utils/pgstat_internal.h`). Field order
/// matches C.
#[derive(Clone, Copy, Debug, Default)]
pub struct PgStatShared_Checkpointer {
    /// lock protects `reset_offset` as well as `stats.stat_reset_timestamp`
    pub lock: LWLock,
    pub changecount: u32,
    pub stats: PgStat_CheckpointerStats,
    pub reset_offset: PgStat_CheckpointerStats,
}
