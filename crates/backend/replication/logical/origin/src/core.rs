//! File-owned foundation of `replication/logical/origin.c`: the GUC/external
//! globals, the in-memory `ReplicationState` array types, the on-disk format
//! type, and the constants (most declared in `replication/origin.h`, owned by
//! this subsystem).
//!
//! The shared `ReplicationState` array is real shared memory in C
//! (a `repr(C)` `ReplicationStateCtl` block carved by `ShmemInitStruct`). Here
//! a backend is a thread and shared memory is explicitly shared, synchronized
//! state (AGENTS.md "Backend-global state"): each entry embeds a *real* ported
//! [`LWLock`]/[`ConditionVariable`], and the scalar fields C mutates under
//! those locks (`roident`/`acquired_by` under `ReplicationOriginLock`,
//! `remote_lsn`/`local_lsn` under the per-entry `lock`) are real atomics. The
//! LWLock/ReplicationOriginLock still serialize the logical protocol exactly as
//! in C; the atomics are only the memory-safe carrier for the shared words.

use core::sync::atomic::{AtomicI32, AtomicU16, AtomicU64};

use condvar::ConditionVariable;
use types_core::{Oid, RepOriginId, XLogRecPtr};
use types_storage::LWLock;

/// `#define PG_UINT16_MAX UINT16_MAX` (c.h) — `RepOriginId` is `uint16`.
pub const PG_UINT16_MAX: RepOriginId = u16::MAX;

// ---------------------------------------------------------------------------
// header constants (replication/origin.h)
// ---------------------------------------------------------------------------

/// `#define XLOG_REPLORIGIN_SET  0x00` (origin.h).
pub const XLOG_REPLORIGIN_SET: u8 = 0x00;
/// `#define XLOG_REPLORIGIN_DROP 0x10` (origin.h).
pub const XLOG_REPLORIGIN_DROP: u8 = 0x10;

/// `#define InvalidRepOriginId 0` (origin.h).
pub const InvalidRepOriginId: RepOriginId = 0;
/// `#define DoNotReplicateId PG_UINT16_MAX` (origin.h).
pub const DoNotReplicateId: RepOriginId = PG_UINT16_MAX;

/// `#define MAX_RONAME_LEN 512` (origin.h).
pub const MAX_RONAME_LEN: usize = 512;

/// `#define LOGICALREP_ORIGIN_NONE "none"` (catalog/pg_subscription.h).
pub const LOGICALREP_ORIGIN_NONE: &str = "none";
/// `#define LOGICALREP_ORIGIN_ANY "any"` (catalog/pg_subscription.h).
pub const LOGICALREP_ORIGIN_ANY: &str = "any";

// ---------------------------------------------------------------------------
// origin.c file-private constants
// ---------------------------------------------------------------------------

/// `#define PG_REPLORIGIN_CHECKPOINT_FILENAME PG_LOGICAL_DIR "/replorigin_checkpoint"`.
/// `PG_LOGICAL_DIR` is `"pg_logical"`.
pub const PG_REPLORIGIN_CHECKPOINT_FILENAME: &str = "pg_logical/replorigin_checkpoint";
/// `#define PG_REPLORIGIN_CHECKPOINT_TMPFILE PG_REPLORIGIN_CHECKPOINT_FILENAME ".tmp"`.
pub const PG_REPLORIGIN_CHECKPOINT_TMPFILE: &str = "pg_logical/replorigin_checkpoint.tmp";

/// `#define REPLICATION_STATE_MAGIC ((uint32) 0x1257DADE)` — magic for on-disk files.
pub const REPLICATION_STATE_MAGIC: u32 = 0x1257_DADE;

/// `RM_REPLORIGIN_ID` — the resource-manager id for replication-origin WAL
/// records (entry 19 of `access/rmgrlist.h`, "ReplicationOrigin").
pub const RM_REPLORIGIN_ID: u8 = 19;

/// `InvalidXLogRecPtr` — `#define InvalidXLogRecPtr 0` (`access/xlogdefs.h`).
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

// ---------------------------------------------------------------------------
// GUC variable
// ---------------------------------------------------------------------------

/// `int max_active_replication_origins = 10;` — GUC default.
pub const DEFAULT_MAX_ACTIVE_REPLICATION_ORIGINS: i32 = 10;

// ---------------------------------------------------------------------------
// in-memory / on-disk state types
// ---------------------------------------------------------------------------

/// Replay progress of a single remote node.
///
/// `typedef struct ReplicationState` (origin.c lines 109-142). The C shmem
/// struct's mutable scalar fields become real atomics (the shared-state
/// synchronization carrier); the `LWLock lock` and `ConditionVariable
/// origin_cv` are *real* ported primitives embedded directly.
#[derive(Debug)]
pub struct ReplicationState {
    /// Local identifier for the remote node. Mutated under
    /// `ReplicationOriginLock`.
    pub roident: AtomicU16,

    /// Location of the latest commit from the remote side. Mutated under
    /// [`Self::lock`].
    pub remote_lsn: AtomicU64,

    /// Remember the local lsn of the commit record so we can `XLogFlush()` to
    /// it during a checkpoint. Mutated under [`Self::lock`].
    pub local_lsn: AtomicU64,

    /// PID of backend that's acquired slot, or 0 if none. Mutated under
    /// `ReplicationOriginLock`.
    pub acquired_by: AtomicI32,

    /// Condition variable that's signaled when `acquired_by` changes.
    pub origin_cv: ConditionVariable,

    /// Lock protecting `remote_lsn` and `local_lsn`.
    pub lock: LWLock,
}

impl ReplicationState {
    /// A zeroed (`MemSet(..., 0, ...)`) entry, matching C's fresh shmem block:
    /// `roident == InvalidRepOriginId`, both LSNs `InvalidXLogRecPtr`,
    /// `acquired_by == 0`. The embedded `lock`/`origin_cv` are (re)initialized
    /// afterwards by [`crate::ReplicationOriginShmemInit`]
    /// (`LWLockInitialize`/`ConditionVariableInit`).
    pub fn zeroed() -> Self {
        ReplicationState {
            roident: AtomicU16::new(InvalidRepOriginId),
            remote_lsn: AtomicU64::new(InvalidXLogRecPtr),
            local_lsn: AtomicU64::new(InvalidXLogRecPtr),
            acquired_by: AtomicI32::new(0),
            origin_cv: ConditionVariable::new(),
            lock: LWLock::default(),
        }
    }
}

/// On-disk version of `ReplicationState`.
///
/// `typedef struct ReplicationStateOnDisk` (origin.c lines 147-151):
/// `{ RepOriginId roident; XLogRecPtr remote_lsn; }`. The byte image (incl. the
/// 6 bytes of C padding between the `uint16` and the 8-byte-aligned `uint64`)
/// is the checkpoint-I/O seam's concern (a genuine external), so this carries
/// only the two logical fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationStateOnDisk {
    pub roident: RepOriginId,
    pub remote_lsn: XLogRecPtr,
}

// ---------------------------------------------------------------------------
// WAL record structs (replication/origin.h — owned by this subsystem)
// ---------------------------------------------------------------------------

/// `typedef struct xl_replorigin_set` (origin.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct xl_replorigin_set {
    pub remote_lsn: XLogRecPtr,
    pub node_id: RepOriginId,
    pub force: bool,
}

/// `typedef struct xl_replorigin_drop` (origin.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct xl_replorigin_drop {
    pub node_id: RepOriginId,
}

// ---------------------------------------------------------------------------
// SRF row for pg_show_replication_origin_status
// ---------------------------------------------------------------------------

/// One row emitted by `pg_show_replication_origin_status`
/// (`REPLICATION_ORIGIN_PROGRESS_COLS == 4`). The C `values[]`/`nulls[]`
/// arrays are surfaced as typed fields (NULL = `None`) in column order:
/// `(local_id, external_id, remote_lsn, local_lsn)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationOriginStatusRow {
    /// `values[0] = ObjectIdGetDatum(state->roident)` — never NULL.
    pub local_id: Oid,
    /// `values[1] = CStringGetTextDatum(roname)` — NULL when the origin was
    /// concurrently dropped (`replorigin_by_oid(..., missing_ok=true)` failed).
    pub external_id: Option<alloc::string::String>,
    /// `values[2] = LSNGetDatum(state->remote_lsn)`.
    pub remote_lsn: XLogRecPtr,
    /// `values[3] = LSNGetDatum(state->local_lsn)`.
    pub local_lsn: XLogRecPtr,
}
