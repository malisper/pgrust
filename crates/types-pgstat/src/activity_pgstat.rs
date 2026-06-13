//! pgstat statistics types shared across the per-kind pgstat ports
//! (`pgstat.h`, `utils/pgstat_internal.h`), trimmed to the archiver and
//! checkpointer kinds consumed so far.
//!
//! The `lock` field of each `PgStatShared_*` struct is the real shmem-resident
//! [`LWLock`] from C's `PgStatShared_Common` header; the ported
//! `LWLockInitialize`/`LWLockAcquire`/`LWLockRelease` operate on it directly.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::sync::atomic::AtomicU32;

use types_core::primitive::TimestampTz;
use types_core::xact::XlXactStatsItem;
use types_storage::LWLock;

/// `PgStat_Kind` (`utils/pgstat_kind.h`): `typedef uint32 PgStat_Kind;` — the
/// id of a cumulative-statistics kind (builtin or custom). A newtype rather
/// than a bare `u32` so kind ids cannot be confused with other counters; the
/// full builtin id table lives below, values per `pgstat_kind.h`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PgStat_Kind(pub u32);

/// `PGSTAT_KIND_MIN` — minimum ID allowed.
pub const PGSTAT_KIND_MIN: PgStat_Kind = PgStat_Kind(1);
/// `PGSTAT_KIND_MAX` — maximum ID allowed.
pub const PGSTAT_KIND_MAX: PgStat_Kind = PgStat_Kind(32);
/// `PGSTAT_KIND_INVALID` — must be 0 for `pgstat_register_kind` initialization.
pub const PGSTAT_KIND_INVALID: PgStat_Kind = PgStat_Kind(0);

// Stats with variable number of entries.
/// `PGSTAT_KIND_DATABASE` — database-wide statistics.
pub const PGSTAT_KIND_DATABASE: PgStat_Kind = PgStat_Kind(1);
/// `PGSTAT_KIND_RELATION` — per-table statistics.
pub const PGSTAT_KIND_RELATION: PgStat_Kind = PgStat_Kind(2);
/// `PGSTAT_KIND_FUNCTION` — per-function statistics.
pub const PGSTAT_KIND_FUNCTION: PgStat_Kind = PgStat_Kind(3);
/// `PGSTAT_KIND_REPLSLOT` — per-slot statistics.
pub const PGSTAT_KIND_REPLSLOT: PgStat_Kind = PgStat_Kind(4);
/// `PGSTAT_KIND_SUBSCRIPTION` — per-subscription statistics.
pub const PGSTAT_KIND_SUBSCRIPTION: PgStat_Kind = PgStat_Kind(5);
/// `PGSTAT_KIND_BACKEND` — per-backend statistics.
pub const PGSTAT_KIND_BACKEND: PgStat_Kind = PgStat_Kind(6);

// Stats with a fixed number of entries.
pub const PGSTAT_KIND_ARCHIVER: PgStat_Kind = PgStat_Kind(7);
pub const PGSTAT_KIND_BGWRITER: PgStat_Kind = PgStat_Kind(8);
pub const PGSTAT_KIND_CHECKPOINTER: PgStat_Kind = PgStat_Kind(9);
pub const PGSTAT_KIND_IO: PgStat_Kind = PgStat_Kind(10);
pub const PGSTAT_KIND_SLRU: PgStat_Kind = PgStat_Kind(11);
pub const PGSTAT_KIND_WAL: PgStat_Kind = PgStat_Kind(12);

pub const PGSTAT_KIND_BUILTIN_MIN: PgStat_Kind = PGSTAT_KIND_DATABASE;
pub const PGSTAT_KIND_BUILTIN_MAX: PgStat_Kind = PGSTAT_KIND_WAL;
/// `PGSTAT_KIND_BUILTIN_SIZE`.
pub const PGSTAT_KIND_BUILTIN_SIZE: usize = PGSTAT_KIND_BUILTIN_MAX.0 as usize + 1;

/// `PGSTAT_KIND_CUSTOM_MIN` — custom stats kinds allotted to extensions.
pub const PGSTAT_KIND_CUSTOM_MIN: PgStat_Kind = PgStat_Kind(24);
pub const PGSTAT_KIND_CUSTOM_MAX: PgStat_Kind = PGSTAT_KIND_MAX;
pub const PGSTAT_KIND_CUSTOM_SIZE: usize =
    (PGSTAT_KIND_CUSTOM_MAX.0 - PGSTAT_KIND_CUSTOM_MIN.0 + 1) as usize;

impl PgStat_Kind {
    /// `pgstat_is_kind_builtin(kind)` (`pgstat_kind.h` inline).
    pub const fn is_builtin(self) -> bool {
        self.0 >= PGSTAT_KIND_BUILTIN_MIN.0 && self.0 <= PGSTAT_KIND_BUILTIN_MAX.0
    }

    /// `pgstat_is_kind_custom(kind)` (`pgstat_kind.h` inline).
    pub const fn is_custom(self) -> bool {
        self.0 >= PGSTAT_KIND_CUSTOM_MIN.0 && self.0 <= PGSTAT_KIND_CUSTOM_MAX.0
    }
}

/// `PgStat_Counter` (`pgstat.h`): `typedef int64 PgStat_Counter;`.
pub type PgStat_Counter = i64;

/// `PgStat_PendingDroppedStatsItem` (`pgstat_xact.c`): one stats-entry
/// create/drop scheduled by a (sub)transaction. The C `dlist_node` link is
/// the containing [`PgStat_SubXactStatus::pending_drops`] deque.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_PendingDroppedStatsItem {
    pub item: XlXactStatsItem,
    pub is_create: bool,
}

/// `PgStat_TableXactStatus` (`pgstat.h`) — per-table transactional status
/// for one (sub)transaction nesting level, trimmed to its scalar fields.
///
/// The C `upper`/`next` links and the `parent` back-pointer into
/// `PgStat_TableStatus` are intrusive-list/per-table mechanics owned by
/// `pgstat_relation.c`; the same-level `next` chain is the containing
/// [`PgStat_SubXactStatus::first`] vec, and the `upper`/`parent` references
/// are populated (in the owner's shape) when that unit lands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_TableXactStatus {
    /// tuples inserted in (sub)xact
    pub tuples_inserted: PgStat_Counter,
    /// tuples updated in (sub)xact
    pub tuples_updated: PgStat_Counter,
    /// tuples deleted in (sub)xact
    pub tuples_deleted: PgStat_Counter,
    /// relation truncated/dropped in this (sub)xact
    pub truncdropped: bool,
    /// tuples i/u/d prior to truncate/drop
    pub inserted_pre_truncdrop: PgStat_Counter,
    pub updated_pre_truncdrop: PgStat_Counter,
    pub deleted_pre_truncdrop: PgStat_Counter,
    /// subtransaction nest level
    pub nest_level: i32,
}

/// `PgStat_SubXactStatus` (`utils/pgstat_internal.h`) — one node of the
/// per-backend `pgStatXactStack`, carrying everything transactional the
/// cumulative stats system tracks for one (sub)transaction nesting level.
/// The C `prev` link is the containing stack's order.
///
/// Shared so exactly one stack exists: `pgstat_xact.c` owns the stack and
/// the `pending_drops` schedule; `pgstat_relation.c` links its per-relation
/// nodes into the same level node via [`first`](Self::first), as in C.
#[derive(Debug, Default)]
pub struct PgStat_SubXactStatus {
    /// subtransaction nest level
    pub nest_level: i32,
    /// `pending_drops` dclist: stats objects created/dropped in this
    /// (sub)transaction (owned by `pgstat_xact.c`).
    pub pending_drops: VecDeque<PgStat_PendingDroppedStatsItem>,
    /// `first`: head of the per-relation `PgStat_TableXactStatus` chain for
    /// this level — the C same-level `next` links are this vec's order
    /// (owned by `pgstat_relation.c`).
    pub first: Vec<PgStat_TableXactStatus>,
}

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
/// `changecount` is the shmem-resident counter the changecount protocol
/// (`pgstat_internal.h`) runs on; it is a real atomic because concurrent
/// readers race the writer by design.
#[derive(Debug, Default)]
pub struct PgStatShared_Archiver {
    /// lock protects `reset_offset` as well as `stats.stat_reset_timestamp`
    pub lock: LWLock,
    pub changecount: AtomicU32,
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
/// matches C. See [`PgStatShared_Archiver`] on `changecount`.
#[derive(Debug, Default)]
pub struct PgStatShared_Checkpointer {
    /// lock protects `reset_offset` as well as `stats.stat_reset_timestamp`
    pub lock: LWLock,
    pub changecount: AtomicU32,
    pub stats: PgStat_CheckpointerStats,
    pub reset_offset: PgStat_CheckpointerStats,
}
