//! Internal cumulative-statistics types (`utils/pgstat_internal.h`): the shared
//! dshash-registry model, the per-kind descriptor metadata
//! ([`PgStat_KindInfo`]), the central shmem control block, and the cached
//! snapshot.
//!
//! These are the types ports of `pgstat.c` / `pgstat_shmem.c` need to name
//! across the dependency cycle; the owner-internal callback table, the
//! `PgStat_EntryRef` backend-local reference, and the live `pgStatLocal` are
//! all owner-private and are NOT modeled here (they never cross a seam).

use core::sync::atomic::{AtomicU32, AtomicU64};

use ::types_core::primitive::{Oid, TimestampTz};
use ::types_storage::storage::{dsa_pointer, dshash_table_handle};
use ::types_storage::LWLock;

use crate::activity_pgstat::{
    PgStat_ArchiverStats, PgStat_CheckpointerStats, PgStat_FetchConsistency, PgStat_IO,
    PgStat_Kind, PgStat_SLRUStats, PgStat_StatDBEntry, PgStat_StatFuncEntry,
    PgStat_StatReplSlotEntry, PgStat_StatSubEntry, PgStat_StatTabEntry, PgStat_WalStats,
    PGSTAT_KIND_BUILTIN_SIZE, PGSTAT_KIND_CUSTOM_SIZE,
};
use crate::backend_utils_activity_pgstat_bgwriter::{PgStatShared_BgWriter, PgStat_BgWriterStats};

pub use crate::activity_pgstat::{PgStatShared_Archiver, PgStatShared_Checkpointer};

/// `SLRU_NUM_ELEMENTS` (`utils/pgstat_internal.h`): `lengthof(slru_names)` over
/// the fixed list of SLRU names. There is no central SLRU registry, so this
/// fixed list (with a trailing "other" bucket) is used instead.
pub const SLRU_NUM_ELEMENTS: usize = 8;

/// `slru_names[]` (`utils/pgstat_internal.h`) — the fixed list of SLRU names
/// kept for stats. "other" must stay last.
pub const SLRU_NAMES: [&str; SLRU_NUM_ELEMENTS] = [
    "commit_timestamp",
    "multixact_member",
    "multixact_offset",
    "notify",
    "serializable",
    "subtransaction",
    "transaction",
    "other",
];

/// `PgStat_HashKey` (`utils/pgstat_internal.h`) — key of the shared statistics
/// hash table.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(C)]
pub struct PgStat_HashKey {
    /// statistics entry kind
    pub kind: PgStat_Kind,
    /// database ID. `InvalidOid` for shared objects.
    pub dboid: Oid,
    /// object ID (table, function, etc.), or identifier.
    pub objid: u64,
}

impl Default for PgStat_Kind {
    fn default() -> Self {
        crate::activity_pgstat::PGSTAT_KIND_INVALID
    }
}

/// `PgStatShared_Common` (`utils/pgstat_internal.h`) — common header struct for
/// the `PgStatShared_*` variable-amount entries; embedded as the first member
/// of each.
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_Common {
    /// just a validity cross-check
    pub magic: u32,
    /// lock protecting stats contents (the data following the header)
    pub lock: LWLock,
}

/// `PgStatShared_HashEntry` (`utils/pgstat_internal.h`) — a shared statistics
/// hash entry. Doesn't itself contain any stats, but points to them via
/// [`body`](Self::body) (a `dsa_pointer` into the DSA area), so the stats
/// entries themselves can be of variable size.
#[derive(Debug)]
pub struct PgStatShared_HashEntry {
    /// hash key
    pub key: PgStat_HashKey,
    /// if set, backends must release their references so the entry's memory can
    /// be freed; no new references may be made once dropped.
    pub dropped: bool,
    /// refcount managing the lifetime of the entry itself (separate from the
    /// dshash entry). Atomic because backends increment it under a shared lock.
    pub refcount: AtomicU32,
    /// number of times the entry has been reused (`pgstat_reinit_entry`).
    /// Atomic for the same reason as `refcount`.
    pub generation: AtomicU32,
    /// pointer to shared stats; the stats entry always starts with
    /// [`PgStatShared_Common`], embedded in a larger kind-specific struct.
    pub body: dsa_pointer,
}

// ============================================================================
// Variable-amount shared stats entries — each starts with PgStatShared_Common.
// ============================================================================

/// `PgStatShared_Database` (`utils/pgstat_internal.h`).
///
/// `#[repr(C)]` so the leading `header` is guaranteed first (see
/// [`PgStatShared_Backend`]): the pgstat core hands out the shared body as a
/// `*mut PgStatShared_Common` (offset 0) and locks `header.lock`, while the
/// per-kind code recovers `.stats` via `offset_of!`. Without `repr(C)` Rust may
/// reorder the fields, so the two views disagree, corrupting the embedded
/// LWLock state (→ `PANIC: queueing for lock while waiting on another one`).
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_Database {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatDBEntry,
}

/// `PgStatShared_Relation` (`utils/pgstat_internal.h`). `#[repr(C)]` required —
/// see [`PgStatShared_Database`].
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_Relation {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatTabEntry,
}

/// `PgStatShared_Function` (`utils/pgstat_internal.h`). `#[repr(C)]` required —
/// see [`PgStatShared_Database`].
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_Function {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatFuncEntry,
}

/// `PgStatShared_Subscription` (`utils/pgstat_internal.h`).
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_Subscription {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatSubEntry,
}

/// `PgStatShared_ReplSlot` (`utils/pgstat_internal.h`).
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_ReplSlot {
    pub header: PgStatShared_Common,
    pub stats: PgStat_StatReplSlotEntry,
}

/// `PgStatShared_Backend` (`utils/pgstat_internal.h`).
///
/// `#[repr(C)]` so the leading `header` is guaranteed first: the pgstat core
/// hands out the shared stats body as a `*mut PgStatShared_Common`, and the
/// per-kind code recovers the full struct with `(shared_stats as *mut
/// PgStatShared_Backend)` (C's `(PgStatShared_Backend *) entry_ref->shared_stats`).
#[derive(Debug, Default)]
#[repr(C)]
pub struct PgStatShared_Backend {
    pub header: PgStatShared_Common,
    pub stats: crate::activity_pgstat::PgStat_Backend,
}

// ============================================================================
// Fixed-amount shared stats entries.
// ============================================================================

/// `PgStatShared_IO` (`utils/pgstat_internal.h`). `locks[i]` protects
/// `stats.stats[i]`; `locks[0]` also protects `stats.stat_reset_timestamp`.
#[derive(Debug, Default)]
pub struct PgStatShared_IO {
    pub locks: [LWLock; ::types_core::init::BACKEND_NUM_TYPES],
    pub stats: PgStat_IO,
}

/// `PgStatShared_SLRU` (`utils/pgstat_internal.h`). `lock` protects `stats`.
#[derive(Debug, Default)]
pub struct PgStatShared_SLRU {
    pub lock: LWLock,
    pub stats: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
}

/// `PgStatShared_Wal` (`utils/pgstat_internal.h`). `lock` protects `stats`.
#[derive(Debug, Default)]
pub struct PgStatShared_Wal {
    pub lock: LWLock,
    pub stats: PgStat_WalStats,
}

/// `PgStat_KindInfo` (`utils/pgstat_internal.h`) — metadata for a specific kind
/// of statistics. This is the descriptor portion of C's static
/// `pgstat_kind_builtin_infos[]` array (a faithful built-in table, not an
/// invented registry).
///
/// The C struct's function-pointer members (`flush_pending_cb`,
/// `init_shmem_cb`, `snapshot_cb`, …) point into the owner's stats functions;
/// they are OWNER-INTERNAL state and live in the owner crate's own callback
/// table, NOT here (so this crate avoids a `PgResult` / owner dependency).
/// This struct carries only the scalar metadata that crosses the seam.
#[derive(Clone, Copy, Debug)]
pub struct PgStat_KindInfo {
    /// Do a fixed number of stats objects exist for this kind (e.g. bgwriter)
    /// or not (e.g. tables)? (C bitfield `fixed_amount:1`.)
    pub fixed_amount: bool,
    /// Can stats of this kind be accessed from another database? Determines
    /// whether a stats object gets included in snapshots. (C `:1`.)
    pub accessed_across_databases: bool,
    /// Should stats be written to the on-disk stats file? (C `:1`.)
    pub write_to_file: bool,
    /// Size of an entry in the shared stats hash table (the `body`). For
    /// fixed-numbered stats, the size of an entry in
    /// `PgStat_ShmemControl::custom_data`.
    pub shared_size: u32,
    /// Offset of the statistics struct in the cached snapshot
    /// `PgStat_Snapshot`, for fixed-numbered statistics.
    pub snapshot_ctl_off: u32,
    /// Offset of the statistics struct in `PgStat_ShmemControl`, for
    /// fixed-numbered statistics.
    pub shared_ctl_off: u32,
    /// Offset of statistics inside the shared stats entry (for
    /// [de]serialization).
    pub shared_data_off: u32,
    /// Length of statistics inside the shared stats entry (for
    /// [de]serialization; separate from `shared_size` because serialization
    /// excludes in-memory state like lwlocks).
    pub shared_data_len: u32,
    /// Size of the pending data for this kind (`PgStat_EntryRef->pending`). 0
    /// means an entry of this kind should never have a pending entry.
    pub pending_size: u32,
    /// name of the kind of stats
    pub name: &'static str,
}

/// `PgStat_ShmemControl` (`utils/pgstat_internal.h`) — the central shared-memory
/// entry for the cumulative stats system. Fixed-amount stats, the dynamic
/// shared hash table for non-fixed-amount stats, and the remaining bits are all
/// reached from here.
///
/// C's `raw_dsa_area` is a `void *` into the DSA segment; modeled as the
/// `dsa_pointer` base the owner resolves through its `dsa_area` handle.
///
/// `#[repr(C)]` because this block is carved from the main shared-memory segment
/// (`ShmemInitStruct`) so it has a single instance the whole cluster shares —
/// the fixed-numbered stats (archiver/bgwriter/checkpointer/io/slru/wal) and
/// their reset timestamps must be visible across backends, not per-process.
#[repr(C)]
#[derive(Debug, Default)]
pub struct PgStat_ShmemControl {
    /// base of the raw DSA area (`void *raw_dsa_area`)
    pub raw_dsa_area: dsa_pointer,
    /// shared dbstat hash (`dshash_table_handle`)
    pub hash_handle: dshash_table_handle,
    /// has the stats system already been shut down? (debugging check)
    pub is_shutdown: bool,
    /// incremented when stats for dropped objects could not be freed; triggers
    /// `pgstat_gc_entry_refs()` in backends.
    pub gc_request_count: AtomicU64,
    // Stats data for fixed-numbered objects.
    pub archiver: PgStatShared_Archiver,
    pub bgwriter: PgStatShared_BgWriter,
    pub checkpointer: PgStatShared_Checkpointer,
    pub io: PgStatShared_IO,
    pub slru: PgStatShared_SLRU,
    pub wal: PgStatShared_Wal,
    /// Custom fixed-numbered stats, indexed by `kind - PGSTAT_KIND_CUSTOM_MIN`.
    /// Each slot is a `dsa_pointer` to the owner-allocated region (C `void *`).
    pub custom_data: [dsa_pointer; PGSTAT_KIND_CUSTOM_SIZE],
}

/// `PgStat_Snapshot` (`utils/pgstat_internal.h`) — a cached statistics snapshot.
///
/// C's `stats` (`struct pgstat_snapshot_hash *`) is the simplehash of
/// variable-numbered snapshot entries and `context` is the `MemoryContext` they
/// are bulk-freed from; both are owner-internal lifecycle state, so the snapshot
/// hash and its arena live in the owner. `custom_data` slots are
/// `dsa_pointer`-style handles to TopMemoryContext allocations the owner makes.
#[derive(Debug)]
pub struct PgStat_Snapshot {
    pub mode: PgStat_FetchConsistency,
    /// time at which snapshot was taken
    pub snapshot_timestamp: TimestampTz,
    pub fixed_valid: [bool; PGSTAT_KIND_BUILTIN_SIZE],
    pub archiver: PgStat_ArchiverStats,
    pub bgwriter: PgStat_BgWriterStats,
    pub checkpointer: PgStat_CheckpointerStats,
    pub io: PgStat_IO,
    pub slru: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS],
    pub wal: PgStat_WalStats,
    /// validity flags for custom fixed-numbered statistics.
    pub custom_valid: [bool; PGSTAT_KIND_CUSTOM_SIZE],
    /// data for custom fixed-numbered statistics (owner-allocated regions).
    pub custom_data: [dsa_pointer; PGSTAT_KIND_CUSTOM_SIZE],
}

impl Default for PgStat_Snapshot {
    fn default() -> Self {
        PgStat_Snapshot {
            mode: PgStat_FetchConsistency::PGSTAT_FETCH_CONSISTENCY_NONE,
            snapshot_timestamp: 0,
            fixed_valid: [false; PGSTAT_KIND_BUILTIN_SIZE],
            archiver: PgStat_ArchiverStats::default(),
            bgwriter: PgStat_BgWriterStats::default(),
            checkpointer: PgStat_CheckpointerStats::default(),
            io: PgStat_IO::default(),
            slru: [PgStat_SLRUStats::default(); SLRU_NUM_ELEMENTS],
            wal: PgStat_WalStats::default(),
            custom_valid: [false; PGSTAT_KIND_CUSTOM_SIZE],
            custom_data: [0; PGSTAT_KIND_CUSTOM_SIZE],
        }
    }
}
