//! Portal / cursor vocabulary (`utils/portal.h`, `tcop/dest.h`,
//! `executor/execdesc.h`, `tcop/cmdtag.h`, `nodes/parsenodes.h` cursor-option
//! bits), trimmed to the fields the portal subsystem (`portalcmds.c` +
//! `portalmem.c`) consumes.
//!
//! `Portal` is the open-handle alias for a `PortalData` owned by
//! `utils/mmgr/portalmem.c` (its hash table). It is a shared, interior-mutable
//! handle (C `struct PortalData *` is a raw pointer the whole subsystem aliases
//! and mutates), mirroring the `types-rel` `Relation` alias precedent.
//!
//! Sub-objects that belong to not-yet-ported owners (`QueryDesc`,
//! `Tuplestorestate`, `Snapshot`, `ResourceOwner`, portal memory contexts) are
//! carried as the real owners' values where they exist on main, and as trimmed
//! real handle types here where they do not yet — never opaque integer
//! stand-ins.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;
use alloc::string::String;
use core::cell::RefCell;

use mcx::MemoryContext;
use types_core::primitive::TimestampTz;
use types_core::xact::SubTransactionId;
use types_nodes::funcapi::Tuplestorestate;
use types_snapshot::SnapshotData;
use types_tuple::heaptuple::TupleDescData;

/// `CommandTag` (`tcop/cmdtaglist.h`) — the 0-based position of the enumerator
/// in `cmdtaglist.h`. Kept as the integer the C enum compiles to so command
/// completion data round-trips exactly.
pub type CommandTag = i32;

/// `CMDTAG_UNKNOWN` (cmdtaglist.h list position 0).
pub const CMDTAG_UNKNOWN: CommandTag = 0;
/// `CMDTAG_DELETE` (cmdtaglist.h list position 103).
pub const CMDTAG_DELETE: CommandTag = 103;
/// `CMDTAG_FETCH` (cmdtaglist.h list position 154).
pub const CMDTAG_FETCH: CommandTag = 154;
/// `CMDTAG_INSERT` (cmdtaglist.h list position 158).
pub const CMDTAG_INSERT: CommandTag = 158;
/// `CMDTAG_MERGE` (cmdtaglist.h list position 163).
pub const CMDTAG_MERGE: CommandTag = 163;
/// `CMDTAG_MOVE` (cmdtaglist.h list position 164).
pub const CMDTAG_MOVE: CommandTag = 164;
/// `CMDTAG_SELECT` (cmdtaglist.h list position 179).
pub const CMDTAG_SELECT: CommandTag = 179;
/// `CMDTAG_UPDATE` (cmdtaglist.h list position 191).
pub const CMDTAG_UPDATE: CommandTag = 191;

/// `COMPLETION_TAG_BUFSIZE` (`tcop/cmdtag.h`) — required size of the
/// caller-supplied buffer for the command-completion string.
pub const COMPLETION_TAG_BUFSIZE: usize = 64;

/// `QueryCompletion` (`tcop/cmdtag.h`) — command completion status data.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryCompletion {
    /// `CommandTag commandTag`.
    pub commandTag: CommandTag,
    /// `uint64 nprocessed`.
    pub nprocessed: u64,
}

/// `FetchDirection` (`nodes/parsenodes.h`) — values verified against
/// PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum FetchDirection {
    /// for these, howMany is how many rows to fetch; FETCH_ALL means ALL
    FETCH_FORWARD = 0,
    FETCH_BACKWARD = 1,
    /// for these, howMany indicates a position; only one row is fetched
    FETCH_ABSOLUTE = 2,
    FETCH_RELATIVE = 3,
}

/// `PortalStrategy` (`utils/portal.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum PortalStrategy {
    #[default]
    PORTAL_ONE_SELECT = 0,
    PORTAL_ONE_RETURNING,
    PORTAL_ONE_MOD_WITH,
    PORTAL_UTIL_SELECT,
    PORTAL_MULTI_QUERY,
}

/// `PortalStatus` (`utils/portal.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum PortalStatus {
    #[default]
    PORTAL_NEW = 0,
    PORTAL_DEFINED,
    PORTAL_READY,
    PORTAL_ACTIVE,
    PORTAL_DONE,
    PORTAL_FAILED,
}

pub use FetchDirection::{FETCH_ABSOLUTE, FETCH_BACKWARD, FETCH_FORWARD, FETCH_RELATIVE};
pub use PortalStatus::{
    PORTAL_ACTIVE, PORTAL_DEFINED, PORTAL_DONE, PORTAL_FAILED, PORTAL_NEW, PORTAL_READY,
};
pub use PortalStrategy::{
    PORTAL_MULTI_QUERY, PORTAL_ONE_MOD_WITH, PORTAL_ONE_RETURNING, PORTAL_ONE_SELECT,
    PORTAL_UTIL_SELECT,
};

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

/// `ResourceOwner` (`utils/resowner.h`) — the one canonical
/// [`types_resowner::ResourceOwner`] handle. The portal subsystem reads
/// `portal->resowner` and threads it back into `CurrentResourceOwner`, never
/// dereferencing it. `ResourceOwner::NULL` is the C NULL.
pub type ResourceOwner = types_resowner::ResourceOwner;

/// Identity token for an object owned by a subsystem the portal subsystem does
/// not own — the executor `QueryDesc`, the `ParamListInfo`, the
/// `QueryEnvironment`, the held `Tuplestorestate`, the result `TupleDesc`. The
/// holder never inspects these; it only threads them back to their owner
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
/// Stored/cleared and threaded through the snapshot manager's seams; the
/// snapshot manager owns the data. `NULL` models the C NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct SnapshotHandle(pub u64);

impl SnapshotHandle {
    pub const NULL: SnapshotHandle = SnapshotHandle(0);
    pub fn is_null(self) -> bool {
        self == SnapshotHandle::NULL
    }
}

/// `ResourceOwner` identity token (`utils/resowner.h`) — the one canonical
/// [`types_resowner::ResourceOwner`] handle, re-exported under the
/// `ResourceOwnerHandle` name used by the plan-cache-threading callers.
pub type ResourceOwnerHandle = types_resowner::ResourceOwner;

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
    pub name: String,
    pub statement: String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
    pub creation_time: i64,
}

/// `struct PortalData` (`utils/portal.h`), trimmed to the fields the portal
/// subsystem (portalmem owns the record; portalcmds reads/writes a subset)
/// reads or writes. Owned by `utils/mmgr/portalmem.c`'s hash table.
pub struct PortalData {
    /// `const char *name` — portal's name.
    pub name: String,
    /// `const char *prepStmtName` — source prepared statement (NULL if none).
    pub prepStmtName: Option<String>,
    /// `MemoryContext portalContext` — subsidiary memory for portal
    /// (`None` = C NULL until `CreatePortal` assigns it).
    pub portalContext: Option<MemoryContext>,
    /// `ResourceOwner resowner` — resources owned by portal.
    pub resowner: ResourceOwner,
    /// `void (*cleanup)(Portal portal)` — cleanup hook.
    pub cleanup: PortalCleanupHook,

    /// `SubTransactionId createSubid` — the creating subxact.
    pub createSubid: SubTransactionId,
    /// `SubTransactionId activeSubid` — the last subxact with activity.
    pub activeSubid: SubTransactionId,
    /// `int createLevel` — creating subxact's nesting level.
    pub createLevel: i32,

    /// `const char *sourceText` — text of query (as of 8.4, never NULL).
    pub sourceText: Option<String>,
    /// `CommandTag commandTag` — command tag for original query.
    pub commandTag: CommandTag,
    /// `QueryCompletion qc` — command completion data for executed query.
    pub qc: QueryCompletion,
    /// `List *stmts` — list of PlannedStmts. Owned by the cached plan; the
    /// real planned-statement nodes (the `canSetTag` walk reads `PlannedStmt`).
    pub stmts: Option<alloc::vec::Vec<types_nodes::nodeindexscan::PlannedStmt<'static>>>,
    /// `CachedPlan *cplan` — CachedPlan, if stmts are from one (`NULL` = none).
    pub cplan: CachedPlanHandle,

    /// `ParamListInfo portalParams` — params to pass to query.
    pub portalParams: types_nodes::portalcmds::ParamListInfo,
    /// `QueryEnvironment *queryEnv` — environment for query.
    pub queryEnv: Option<Rc<()>>,

    /// `PortalStrategy strategy`.
    pub strategy: PortalStrategy,
    /// `int cursorOptions` — DECLARE CURSOR option bits.
    pub cursorOptions: i32,

    /// `PortalStatus status`.
    pub status: PortalStatus,
    /// `bool portalPinned` — a pinned portal can't be dropped.
    pub portalPinned: bool,
    /// `bool autoHeld` — was automatically converted from pinned to held.
    pub autoHeld: bool,

    /// `QueryDesc *queryDesc` — info needed for executor invocation
    /// (`None` = C NULL). The canonical owned executor invocation handle
    /// (`types_nodes::querydesc::QueryDesc`): lifetime-free, storable by value
    /// (its `'mcx` lives inside the `McxOwned<QueryWorkState>` bundle), so it
    /// does not infect `PortalData`. portalcmds reads `snapshot`/`dest` off it
    /// and hands it to the execMain driver seams — QueryDesc de-handle F1b.
    pub queryDesc: Option<types_nodes::querydesc::QueryDesc>,

    /// `TupleDesc tupDesc` — descriptor for result tuples (`None` = C NULL).
    pub tupDesc: Option<TupleDescData<'static>>,
    /// `int16 *formats` — format codes for result tuples.
    pub formats: alloc::vec::Vec<i16>,

    /// `struct SnapshotData *portalSnapshot` — active snapshot, if any
    /// (`None` = C NULL).
    pub portalSnapshot: Option<Rc<SnapshotData>>,

    /// `Tuplestorestate *holdStore` — store for holdable cursors
    /// (`None` = C NULL). The store outlives the transaction, so `'static`.
    pub holdStore: Option<Tuplestorestate<'static>>,
    /// `MemoryContext holdContext` — memory containing holdStore
    /// (`None` = C NULL).
    pub holdContext: Option<MemoryContext>,
    /// `Snapshot holdSnapshot` — registered snapshot for held tuples
    /// (`None` = C NULL).
    pub holdSnapshot: Option<Rc<SnapshotData>>,

    /// `bool atStart`.
    pub atStart: bool,
    /// `bool atEnd`.
    pub atEnd: bool,
    /// `uint64 portalPos`.
    pub portalPos: u64,

    /// `TimestampTz creation_time`.
    pub creation_time: TimestampTz,
    /// `bool visible` — include this portal in pg_cursors?
    pub visible: bool,
}

impl Drop for PortalData {
    /// Enforce that the arena-allocated payloads (`stmts`, `tupDesc`,
    /// `holdStore`) are released **before** the `MemoryContext`s they were
    /// interned into (`portalContext`, `holdContext`).
    ///
    /// Those payloads carry their own arena lifetime as a `'static` *marker*
    /// (the data is real `Global`-heap memory owned by the inner
    /// `PgBox`/`PgVec`, freed by their own `Drop`, which deallocates through an
    /// `Mcx` reference into the owning `MemoryContext`). The struct's default
    /// field-drop order would drop `portalContext` first (it is declared before
    /// `stmts`/`tupDesc`), invalidating the `Mcx` reference those payloads
    /// deallocate through. Dropping the payloads first makes both this implicit
    /// drop and `PortalDrop`'s teardown sound, mirroring C where
    /// `MemoryContextDelete(portalContext)` frees the plans and nothing touches
    /// them afterward.
    fn drop(&mut self) {
        // Release tuplestore before the hold context.
        self.holdStore = None;
        // Release interned plans / result descriptor / params before the
        // portal context they live in.
        self.stmts = None;
        self.tupDesc = None;
        self.portalParams = None;
        // Contexts (portalContext, holdContext) drop last, via the default
        // field drop, after the payloads above are gone.
    }
}

/// `typedef struct PortalData *Portal` (`utils/portal.h`). Shared,
/// interior-mutable open handle: the whole portal subsystem aliases the same
/// `PortalData` through a raw pointer, so the Rust alias is
/// `Rc<RefCell<PortalData>>` (cf. `types-rel`'s `Relation`).
#[derive(Clone)]
pub struct Portal(Rc<RefCell<PortalData>>);

impl Portal {
    pub fn new(data: PortalData) -> Self {
        Portal(Rc::new(RefCell::new(data)))
    }

    /// Borrow the underlying `PortalData` immutably.
    pub fn borrow(&self) -> core::cell::Ref<'_, PortalData> {
        self.0.borrow()
    }

    /// Borrow the underlying `PortalData` mutably (C field assignment).
    pub fn borrow_mut(&self) -> core::cell::RefMut<'_, PortalData> {
        self.0.borrow_mut()
    }

    /// `PortalIsValid(p)` (`utils/portal.h`) — pointer non-NULL.
    pub fn is_valid(&self) -> bool {
        true
    }

    /// Pointer-identity comparison of two `Portal` handles (C `p1 == p2`).
    pub fn ptr_eq(&self, other: &Portal) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}
