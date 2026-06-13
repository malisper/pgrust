//! Portal vocabulary (`utils/portal.h`, `tcop/cmdtag.h`, `nodes/parsenodes.h`
//! cursor-option bits) consumed by the `backend-utils-mmgr-portalmem` port,
//! trimmed to the items that port uses. The `PortalData` record itself lives in
//! the owning crate (it holds real owned `mcx::MemoryContext` arenas, above this
//! layer); this crate carries the field-level enums, the command-tag/completion
//! vocabulary, the external-object identity handles, and the resource-release
//! phase constants.

#![no_std]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

extern crate alloc;

/// `typedef enum PortalStrategy` (`utils/portal.h`). Values are the C
/// enumerator order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum PortalStrategy {
    OneSelect = 0,
    OneReturning = 1,
    OneModWith = 2,
    UtilSelect = 3,
    MultiQuery = 4,
}
pub use PortalStrategy::MultiQuery as PORTAL_MULTI_QUERY;
pub use PortalStrategy::OneSelect as PORTAL_ONE_SELECT;

/// `typedef enum PortalStatus` (`utils/portal.h`). Values are the C enumerator
/// order; the lifecycle never backs up except ACTIVE→READY.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum PortalStatus {
    New = 0,
    Defined = 1,
    Ready = 2,
    Active = 3,
    Done = 4,
    Failed = 5,
}
pub use PortalStatus::Active as PORTAL_ACTIVE;
pub use PortalStatus::Defined as PORTAL_DEFINED;
pub use PortalStatus::Done as PORTAL_DONE;
pub use PortalStatus::Failed as PORTAL_FAILED;
pub use PortalStatus::New as PORTAL_NEW;
pub use PortalStatus::Ready as PORTAL_READY;

/// `typedef enum CommandTag` (`tcop/cmdtag.h`, generated from
/// `cmdtaglist.h`). Trimmed to the one enumerator portalmem references; the
/// real enum is recovered when a consumer needs more tags (newtype keeps the
/// namespace, not a bare integer alias).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandTag(pub u32);

/// `CMDTAG_UNKNOWN` — the first `cmdtaglist.h` entry (value 0).
pub const CMDTAG_UNKNOWN: CommandTag = CommandTag(0);

/// `struct QueryCompletion` (`tcop/cmdtag.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryCompletion {
    pub commandTag: CommandTag,
    pub nprocessed: u64,
}

// Cursor-option bits (`nodes/parsenodes.h`), values verified against the C
// header.
/// `CURSOR_OPT_BINARY` = 0x0001.
pub const CURSOR_OPT_BINARY: i32 = 0x0001;
/// `CURSOR_OPT_SCROLL` = 0x0002.
pub const CURSOR_OPT_SCROLL: i32 = 0x0002;
/// `CURSOR_OPT_NO_SCROLL` = 0x0004.
pub const CURSOR_OPT_NO_SCROLL: i32 = 0x0004;
/// `CURSOR_OPT_HOLD` = 0x0020.
pub const CURSOR_OPT_HOLD: i32 = 0x0020;

/// `#define MAX_PORTALNAME_LEN NAMEDATALEN` (`utils/portal.h`); `NAMEDATALEN`
/// is 64 (`pg_config_manual.h`).
pub const MAX_PORTALNAME_LEN: usize = 64;

/// `typedef enum ResourceReleasePhase` (`utils/resowner.h`) — the phase passed
/// to `ResourceOwnerRelease`. Values are the C enumerator order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ResourceReleasePhase {
    BeforeLocks = 0,
    Locks = 1,
    AfterLocks = 2,
}
pub use ResourceReleasePhase::AfterLocks as RESOURCE_RELEASE_AFTER_LOCKS;
pub use ResourceReleasePhase::BeforeLocks as RESOURCE_RELEASE_BEFORE_LOCKS;
pub use ResourceReleasePhase::Locks as RESOURCE_RELEASE_LOCKS;

/// Identity token for an object owned by a subsystem portalmem does not own —
/// the executor `QueryDesc`, the planned-statement `List`, the `ParamListInfo`,
/// the `QueryEnvironment`, the held `Tuplestorestate`, the result `TupleDesc`.
/// portalmem never inspects these; it only threads them back to their owner
/// through that owner's seam. `NONE` models the C `NULL` pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ExternHandle(pub u64);

impl ExternHandle {
    pub const NONE: ExternHandle = ExternHandle(0);
    pub fn is_none(self) -> bool {
        self == ExternHandle::NONE
    }
    pub fn is_some(self) -> bool {
        self != ExternHandle::NONE
    }
}

/// `Snapshot` identity token (`utils/snapshot.h` `Snapshot` = `SnapshotData *`).
/// portalmem only stores it, clears it, and threads it through the snapshot
/// manager's seams (register/unregister against a resource owner, pop the active
/// stack); the snapshot manager owns the data. `NULL` models the C NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct SnapshotHandle(pub u64);

impl SnapshotHandle {
    pub const NULL: SnapshotHandle = SnapshotHandle(0);
    pub fn is_null(self) -> bool {
        self == SnapshotHandle::NULL
    }
}

/// `ResourceOwner` identity token (`utils/resowner.h`). Resource owners dissolve
/// into RAII owner values (docs/query-lifecycle-raii.md); until the owner lands
/// portalmem threads this token through the resowner seam. `NULL` models C NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ResourceOwnerHandle(pub u64);

impl ResourceOwnerHandle {
    pub const NULL: ResourceOwnerHandle = ResourceOwnerHandle(0);
    pub fn is_null(self) -> bool {
        self == ResourceOwnerHandle::NULL
    }
}

/// `CachedPlan *` (`utils/plancache.h`) — pointer to plancache-private storage;
/// portalmem only stores it, tests it against NULL, and threads it back to the
/// plan cache through a seam. Identity token; `NULL` models the C NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct CachedPlanHandle(pub u64);

impl CachedPlanHandle {
    pub const NULL: CachedPlanHandle = CachedPlanHandle(0);
    pub fn is_null(self) -> bool {
        self == CachedPlanHandle::NULL
    }
}

/// `void (*cleanup)(Portal)` — portalcmds.c's `PortalCleanup` hook handle.
/// `NONE` models the C NULL function pointer; portalmem only checks
/// presence and routes the call through the portalcmds seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PortalCleanupHook(pub u64);

impl PortalCleanupHook {
    pub const NONE: PortalCleanupHook = PortalCleanupHook(0);
    pub fn is_none(self) -> bool {
        self == Self::NONE
    }
    pub fn is_some(self) -> bool {
        self != Self::NONE
    }
}

/// `fcinfo` (`PG_FUNCTION_ARGS`) identity token for the `pg_cursor` SRF.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct FcinfoHandle(pub u64);

/// One reportable `pg_cursor()` row — the visible, defined portals collected by
/// the `hash_seq_search` walk (portalmem's in-crate part) and handed to the
/// SRF/`Datum` body (the fmgr-value layer, seamed). `TimestampTz`
/// (`datatype/timestamp.h`) is an `int64`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgCursorRow {
    pub name: alloc::string::String,
    pub statement: alloc::string::String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
    pub creation_time: i64,
}
