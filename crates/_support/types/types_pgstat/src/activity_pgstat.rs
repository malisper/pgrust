//! pgstat statistics types shared across the per-kind pgstat ports
//! (`pgstat.h`, `utils/pgstat_internal.h`), trimmed to the archiver and
//! checkpointer kinds consumed so far.
//!
//! The `lock` field of each `PgStatShared_*` struct is the real shmem-resident
//! [`LWLock`] from C's `PgStatShared_Common` header; the ported
//! `LWLockInitialize`/`LWLockAcquire`/`LWLockRelease` operate on it directly.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::sync::atomic::AtomicU32;

use crate::pgstat_internal::PgStat_HashKey;

use types_core::init::BACKEND_NUM_TYPES;
use types_core::instrument::instr_time;
use types_core::primitive::TimestampTz;
use types_core::xact::XlXactStatsItem;
use replication::conflict::CONFLICT_NUM_TYPES;
use types_storage::LWLock;

// Re-export the existing canonical `IOContext` (defined in `types-storage`,
// where the buffer-strategy ring first needed it) so the pgstat I/O types name
// the same enum rather than introducing a duplicate.
pub use types_storage::buf::IOContext;

/// Values for `track_functions` GUC variable (`pgstat.h`) — order is
/// significant!
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrackFunctionsLevel {
    TRACK_FUNC_OFF = 0,
    TRACK_FUNC_PL = 1,
    TRACK_FUNC_ALL = 2,
}

/// `PgStat_FetchConsistency` (`pgstat.h`).
///
/// `PartialOrd`/`Ord` follow the C enum's ascending ordinal values
/// (`NONE < CACHE < SNAPSHOT`); `pgstat_fetch_entry` compares them with `>`
/// exactly as `pgstat.c` does (`pgstat_fetch_consistency > PGSTAT_FETCH_CONSISTENCY_NONE`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum PgStat_FetchConsistency {
    PGSTAT_FETCH_CONSISTENCY_NONE = 0,
    PGSTAT_FETCH_CONSISTENCY_CACHE = 1,
    PGSTAT_FETCH_CONSISTENCY_SNAPSHOT = 2,
}

/// `SessionEndType` (`pgstat.h`) — tracks the cause of session termination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionEndType {
    /// still active
    DISCONNECT_NOT_YET = 0,
    DISCONNECT_NORMAL = 1,
    DISCONNECT_CLIENT_EOF = 2,
    DISCONNECT_FATAL = 3,
    DISCONNECT_KILLED = 4,
}

/// `IOObject` (`pgstat.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IOObject {
    IOOBJECT_RELATION = 0,
    IOOBJECT_TEMP_RELATION = 1,
    IOOBJECT_WAL = 2,
}

/// `IOOBJECT_NUM_TYPES` (`pgstat.h`): `IOOBJECT_WAL + 1`.
pub const IOOBJECT_NUM_TYPES: usize = IOObject::IOOBJECT_WAL as usize + 1;

/// `IOCONTEXT_NUM_TYPES` (`pgstat.h`): `IOCONTEXT_VACUUM + 1`.
pub const IOCONTEXT_NUM_TYPES: usize = IOContext::IOCONTEXT_VACUUM as usize + 1;

/// `IOOp` (`pgstat.h`) — enumeration of IO operations. The byte-tracked group
/// (`IOOP_EXTEND` first, `IOOP_WRITE` last) must stay in this order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IOOp {
    // IOs not tracked in bytes
    IOOP_EVICT = 0,
    IOOP_FSYNC = 1,
    IOOP_HIT = 2,
    IOOP_REUSE = 3,
    IOOP_WRITEBACK = 4,
    // IOs tracked in bytes
    IOOP_EXTEND = 5,
    IOOP_READ = 6,
    IOOP_WRITE = 7,
}

/// `IOOP_NUM_TYPES` (`pgstat.h`): `IOOP_WRITE + 1`.
pub const IOOP_NUM_TYPES: usize = IOOp::IOOP_WRITE as usize + 1;

/// `pgstat_is_ioop_tracked_in_bytes(io_op)` (`pgstat.h`).
pub const fn pgstat_is_ioop_tracked_in_bytes(io_op: IOOp) -> bool {
    (io_op as u32) < IOOP_NUM_TYPES as u32 && (io_op as u32) >= IOOp::IOOP_EXTEND as u32
}

/// `PgStat_Kind` (`utils/pgstat_kind.h`): `typedef uint32 PgStat_Kind;` — the
/// id of a cumulative-statistics kind (builtin or custom). A newtype rather
/// than a bare `u32` so kind ids cannot be confused with other counters; the
/// full builtin id table lives below, values per `pgstat_kind.h`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(C)]
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
/// for one (sub)transaction nesting level.
///
/// The C struct cross-links three ways:
///
/// * `struct PgStat_TableXactStatus *upper` — the same table's node at the
///   next-*outer* nesting level. The per-table chain is rooted at
///   [`PgStat_TableStatus::trans`] (the innermost level) and walks outward
///   through `upper`. Each node owns its outer node, so the chain is modeled
///   as an owned [`Option<Box<PgStat_TableXactStatus>>`](Box); the head box
///   is owned by the table's pending block, exactly mirroring C's
///   `add_tabstat_xact_level` / `AtEOSubXact_PgStat_Relations` pointer walks.
/// * `PgStat_TableStatus *parent` — back-pointer to the owning
///   `PgStat_TableStatus`. The table-status block is the per-kind `pending`
///   value living in `pgstat.c`'s owner-private entry-ref hash, so this
///   back-reference is modeled as the entry's [`PgStat_HashKey`] (the same
///   key-lookup reconciliation used for every other shared-entry pointer in
///   this model). The owner reaches the parent's pending block by that key.
/// * `struct PgStat_TableXactStatus *next` — the same-level link into the
///   containing [`PgStat_SubXactStatus`]'s table list. That intrusive list is
///   modeled by [`PgStat_SubXactStatus::first`] carrying the per-level set of
///   table keys (one node per table per level), so this node does not carry
///   the `next` pointer itself: the level membership is owned by the level
///   node, the per-table chain is owned by `trans`/`upper`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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
    /// `upper` — the same table's `PgStat_TableXactStatus` at the next-outer
    /// nesting level (`NULL`/`None` at the outermost). Owned: the per-table
    /// chain is rooted at [`PgStat_TableStatus::trans`].
    pub upper: Option<Box<PgStat_TableXactStatus>>,
    /// `parent` — the owning `PgStat_TableStatus`, identified by its shared
    /// entry's [`PgStat_HashKey`] (C: `PgStat_TableStatus *parent`).
    pub parent: PgStat_HashKey,
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
    /// `first`: the per-relation `PgStat_TableXactStatus` chain for this level
    /// (C: `PgStat_TableXactStatus *first`, an intrusive `next`-linked list).
    ///
    /// The nodes themselves are owned by their tables' per-table `trans`/`upper`
    /// chains (see [`PgStat_TableXactStatus`]); a node lives in both the C
    /// per-level list and the per-table chain at once via raw pointers, which a
    /// single Rust owner cannot reproduce. The owning side is the per-table
    /// chain, so this level list carries only the *identities* of the tables
    /// with a node at this level — their [`PgStat_HashKey`]s. There is exactly
    /// one node per table per level, so the key plus this level's
    /// [`nest_level`](Self::nest_level) uniquely names the node; the owner
    /// reaches it through the pending-mutation API keyed on the parent table.
    /// Owned by `pgstat_relation.c`.
    pub first: Vec<PgStat_HashKey>,
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
#[repr(C)]
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

// ============================================================================
// Structures kept in backend local memory while accumulating counts
// ============================================================================

/// `PgStat_FunctionCounts` (`pgstat.h`) — the actual per-function counts kept
/// by a backend. Time counters are in `instr_time` format here; converted to
/// microseconds (`PgStat_Counter`) when flushing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_FunctionCounts {
    pub numcalls: PgStat_Counter,
    pub total_time: instr_time,
    pub self_time: instr_time,
}

/// `PgStat_FunctionCallUsage` (`pgstat.h`) — working state needed to accumulate
/// per-function-call timing statistics.
///
/// C's `fs` field is a back-pointer into the function's hashtable entry
/// (`PgStat_FunctionCounts *`, NULL when not tracking). That pointer is into
/// the owner-internal `PgStat_EntryRef->pending` working space; the owner
/// resolves it from the executor-held token when `pgstat_function.c` lands, so
/// it is modeled here as a flag plus the proid the call is tracking.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_FunctionCallUsage {
    /// `fs != NULL`: are we tracking the current function call?
    pub tracking: bool,
    /// the function OID `fs` points into: `pgstat_end_function_usage`
    /// re-resolves the pending `PgStat_FunctionCounts` from `(MyDatabaseId,
    /// proid)` because the owner-private pending block can't be carried as a raw
    /// pointer here. Meaningful only while `tracking`.
    pub proid: types_core::primitive::Oid,
    /// total time previously charged to function, as of function start
    pub save_f_total_time: instr_time,
    /// backend-wide total time as of function start
    pub save_total: instr_time,
    /// system clock as of function start
    pub start: instr_time,
}

/// `PgStat_BackendSubEntry` (`pgstat.h`) — non-flushed subscription stats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_BackendSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
}

impl Default for PgStat_BackendSubEntry {
    fn default() -> Self {
        PgStat_BackendSubEntry {
            apply_error_count: 0,
            sync_error_count: 0,
            conflict_count: [0; CONFLICT_NUM_TYPES],
        }
    }
}

/// `PgStat_TableCounts` (`pgstat.h`) — the actual per-table counts kept by a
/// backend. Contains only event counters, because the flush path uses
/// `pg_memory_is_all_zeros()` to detect whether there are updates to apply.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_TableCounts {
    pub numscans: PgStat_Counter,

    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,

    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,
    pub truncdropped: bool,

    pub delta_live_tuples: PgStat_Counter,
    pub delta_dead_tuples: PgStat_Counter,
    pub changed_tuples: PgStat_Counter,

    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
}

impl PgStat_TableCounts {
    /// `pg_memory_is_all_zeros(&counts, sizeof(PgStat_TableCounts))` — true iff
    /// every counter is zero (used by the flush path to short-circuit).
    pub fn is_all_zeros(&self) -> bool {
        *self == PgStat_TableCounts::default()
    }
}

/// `PgStat_TableStatus` (`pgstat.h`) — per-table status within a backend. This
/// is the per-kind `pending` value for [`PGSTAT_KIND_RELATION`]: it lives in
/// the owner-private entry-ref hash of `pgstat.c`, keyed by its
/// [`PgStat_HashKey`].
///
/// C's `relation` back-pointer (`Relation`) is intentionally DROPPED: the
/// per-table entry is keyed by `(shared, id)`, and the relcache `pgstat_info`
/// link is rebuilt from that key when the owner lands.
///
/// C's `trans` (`PgStat_TableXactStatus *`) is the head of this table's
/// per-(sub)transaction chain — the *innermost* open level's node, walking
/// outward through each node's [`upper`](PgStat_TableXactStatus::upper). The
/// chain is owned through `trans`, so this field is the owning head box
/// ([`Option<Box<PgStat_TableXactStatus>>`](Box)); `None` when no open subxact
/// touches this table. The box's identity is stable for the node's lifetime,
/// which is what the per-level [`PgStat_SubXactStatus::first`] key list and the
/// `parent` back-key resolve against.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgStat_TableStatus {
    /// `id` — table's OID
    pub id: types_core::primitive::Oid,
    /// `shared` — is it a shared catalog?
    pub shared: bool,
    /// `trans` — owning head of the lowest-open-subxact `PgStat_TableXactStatus`
    /// chain (C: `struct PgStat_TableXactStatus *trans`).
    pub trans: Option<Box<PgStat_TableXactStatus>>,
    /// `counts` — event counts to be sent
    pub counts: PgStat_TableCounts,
}

// ============================================================================
// Data structures on disk and in shared memory
// ============================================================================

/// `PGSTAT_FILE_FORMAT_ID` (`pgstat.h`) — bump whenever any on-disk struct
/// changes.
pub const PGSTAT_FILE_FORMAT_ID: u32 = 0x01A5BCB7;

/// `PgStat_BktypeIO` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_BktypeIO {
    pub bytes: [[[u64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub times: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}

impl Default for PgStat_BktypeIO {
    fn default() -> Self {
        PgStat_BktypeIO {
            bytes: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            counts: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            times: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
        }
    }
}

/// `PgStat_PendingIO` (`pgstat.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_PendingIO {
    pub bytes: [[[u64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub counts: [[[PgStat_Counter; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
    pub pending_times:
        [[[instr_time; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
}

impl Default for PgStat_PendingIO {
    fn default() -> Self {
        PgStat_PendingIO {
            bytes: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            counts: [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES],
            pending_times: [[[instr_time::default(); IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES];
                IOOBJECT_NUM_TYPES],
        }
    }
}

/// `PgStat_IO` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_IO {
    pub stat_reset_timestamp: TimestampTz,
    pub stats: [PgStat_BktypeIO; BACKEND_NUM_TYPES],
}

impl Default for PgStat_IO {
    fn default() -> Self {
        PgStat_IO {
            stat_reset_timestamp: 0,
            stats: [PgStat_BktypeIO::default(); BACKEND_NUM_TYPES],
        }
    }
}

/// `PgStat_StatDBEntry` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
    /// times in microseconds
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

/// `PgStat_StatFuncEntry` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_StatFuncEntry {
    pub numcalls: PgStat_Counter,
    /// times in microseconds
    pub total_time: PgStat_Counter,
    pub self_time: PgStat_Counter,
}

/// `PgStat_StatReplSlotEntry` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_StatReplSlotEntry {
    pub spill_txns: PgStat_Counter,
    pub spill_count: PgStat_Counter,
    pub spill_bytes: PgStat_Counter,
    pub stream_txns: PgStat_Counter,
    pub stream_count: PgStat_Counter,
    pub stream_bytes: PgStat_Counter,
    pub total_txns: PgStat_Counter,
    pub total_bytes: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

/// `PgStat_SLRUStats` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_SLRUStats {
    pub blocks_zeroed: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub blocks_read: PgStat_Counter,
    pub blocks_written: PgStat_Counter,
    pub blocks_exists: PgStat_Counter,
    pub flush: PgStat_Counter,
    pub truncate: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

/// `PgStat_StatSubEntry` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_StatSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
    pub stat_reset_timestamp: TimestampTz,
}

impl Default for PgStat_StatSubEntry {
    fn default() -> Self {
        PgStat_StatSubEntry {
            apply_error_count: 0,
            sync_error_count: 0,
            conflict_count: [0; CONFLICT_NUM_TYPES],
            stat_reset_timestamp: 0,
        }
    }
}

/// `PgStat_StatTabEntry` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_StatTabEntry {
    pub numscans: PgStat_Counter,
    pub lastscan: TimestampTz,

    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,

    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,

    pub live_tuples: PgStat_Counter,
    pub dead_tuples: PgStat_Counter,
    pub mod_since_analyze: PgStat_Counter,
    pub ins_since_vacuum: PgStat_Counter,

    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,

    /// user initiated vacuum
    pub last_vacuum_time: TimestampTz,
    pub vacuum_count: PgStat_Counter,
    /// autovacuum initiated
    pub last_autovacuum_time: TimestampTz,
    pub autovacuum_count: PgStat_Counter,
    /// user initiated
    pub last_analyze_time: TimestampTz,
    pub analyze_count: PgStat_Counter,
    /// autovacuum initiated
    pub last_autoanalyze_time: TimestampTz,
    pub autoanalyze_count: PgStat_Counter,

    /// times in milliseconds
    pub total_vacuum_time: PgStat_Counter,
    pub total_autovacuum_time: PgStat_Counter,
    pub total_analyze_time: PgStat_Counter,
    pub total_autoanalyze_time: PgStat_Counter,
}

/// `PgStat_WalCounters` (`pgstat.h`) — WAL activity data gathered from
/// `WalUsage`, separated so it can be shared across stats structs.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_WalCounters {
    pub wal_records: PgStat_Counter,
    pub wal_fpi: PgStat_Counter,
    pub wal_bytes: u64,
    pub wal_buffers_full: PgStat_Counter,
}

/// `PgStat_WalStats` (`pgstat.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_WalStats {
    pub wal_counters: PgStat_WalCounters,
    pub stat_reset_timestamp: TimestampTz,
}

/// `PgStat_Backend` (`pgstat.h`) — backend statistics.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgStat_Backend {
    pub stat_reset_timestamp: TimestampTz,
    pub io_stats: PgStat_BktypeIO,
    pub wal_counters: PgStat_WalCounters,
}

impl Default for PgStat_Backend {
    fn default() -> Self {
        PgStat_Backend {
            stat_reset_timestamp: 0,
            io_stats: PgStat_BktypeIO::default(),
            wal_counters: PgStat_WalCounters::default(),
        }
    }
}

/// `PgStat_BackendPending` (`pgstat.h`) — non-flushed backend stats (stores the
/// same amount of I/O data as `PGSTAT_KIND_IO`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgStat_BackendPending {
    pub pending_io: PgStat_PendingIO,
}
