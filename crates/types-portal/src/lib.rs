//! Portal / cursor vocabulary (`utils/portal.h`, `tcop/dest.h`,
//! `executor/execdesc.h`), trimmed to the fields the portalcmds port consumes.
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
/// `CMDTAG_FETCH` (cmdtaglist.h list position 154).
pub const CMDTAG_FETCH: CommandTag = 154;
/// `CMDTAG_MOVE` (cmdtaglist.h list position 164).
pub const CMDTAG_MOVE: CommandTag = 164;
/// `CMDTAG_SELECT` (cmdtaglist.h list position 179).
pub const CMDTAG_SELECT: CommandTag = 179;

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

/// `ResourceOwner` (`utils/resowner.h`) — opaque handle to a resource owner
/// owned by `utils/resowner/resowner.c` (not yet ported). portalcmds only
/// reads `portal->resowner` and threads it back into `CurrentResourceOwner`,
/// never dereferencing it; modeled as a shared handle to the owner's
/// not-yet-defined payload. `None` is the C NULL.
#[derive(Clone, Default)]
pub struct ResourceOwner(pub Option<Rc<()>>);

impl ResourceOwner {
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
}

/// `QueryDesc` (`executor/execdesc.h`), trimmed to the fields portalcmds reads
/// or writes. The executor (`executor/execMain.c`) owns it; portalcmds reads
/// `snapshot`, swaps `dest`, and hands the whole thing to the executor seams.
pub struct QueryDesc {
    /// `Snapshot snapshot` — snapshot to use for query (`None` = C NULL).
    pub snapshot: Option<Rc<SnapshotData>>,
    /// `DestReceiver *dest` — destination for tuple output (`None` = C NULL).
    pub dest: Option<DestReceiver>,
}

/// `DestReceiver` (`tcop/dest.h`) — opaque output sink, owned by the receiver
/// implementation (e.g. tstoreReceiver.c). portalcmds only stores it into the
/// queryDesc and calls `rDestroy`; modeled as a handle to the receiver's
/// not-yet-defined state plus its `mydest` tag.
#[derive(Clone)]
pub struct DestReceiver {
    pub mydest: types_dest::CommandDest,
    state: Rc<()>,
}

impl DestReceiver {
    pub fn new(mydest: types_dest::CommandDest) -> Self {
        DestReceiver {
            mydest,
            state: Rc::new(()),
        }
    }
    fn _keep(&self) -> &Rc<()> {
        &self.state
    }
}

/// `struct PortalData` (`utils/portal.h`), trimmed to the fields the portalcmds
/// unit reads or writes. Owned by `utils/mmgr/portalmem.c`'s hash table; this
/// is the consumed slice of that struct.
pub struct PortalData {
    /// `const char *name` — portal's name.
    pub name: String,
    /// `MemoryContext portalContext` — subsidiary memory for portal.
    pub portalContext: MemoryContext,
    /// `ResourceOwner resowner` — resources owned by portal.
    pub resowner: ResourceOwner,

    /// `SubTransactionId createSubid` — the creating subxact.
    pub createSubid: SubTransactionId,

    /// `int cursorOptions` — DECLARE CURSOR option bits.
    pub cursorOptions: i32,
    /// `PortalStrategy strategy`.
    pub strategy: PortalStrategy,
    /// `PortalStatus status`.
    pub status: PortalStatus,

    /// `QueryDesc *queryDesc` — info needed for executor invocation
    /// (`None` = C NULL).
    pub queryDesc: Option<QueryDesc>,

    /// `TupleDesc tupDesc` — descriptor for result tuples (`None` = C NULL).
    pub tupDesc: Option<TupleDescData<'static>>,

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
}
