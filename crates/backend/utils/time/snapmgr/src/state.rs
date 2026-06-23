//! Process-local snapshot-manager state and the owned, shared `SnapshotData`
//! representation.
//!
//! In C every long-lived snapshot is a `palloc`'d `SnapshotData` reached
//! through raw `*mut SnapshotData` pointers; the same address may
//! simultaneously sit on the active-snapshot stack, in the
//! `RegisteredSnapshots` heap, and in the `exportedSnapshots` list, with
//! `active_count`/`regd_count` tracking how many of those references exist. C
//! frees the block (`pfree`) exactly when both counts reach zero.
//!
//! Each long-lived snapshot is modelled as a shared handle [`SnapHandle`]
//! (`Rc<RefCell<SnapshotData>>`): the active stack, the registered set and the
//! exported list all hold clones of the same `Rc`, so the shared mutable
//! refcounts behave exactly as in C, and the snapshot's storage is reclaimed
//! by `Rc`'s drop when the last reference goes away — which the manager
//! arranges to coincide with both counts hitting zero, mirroring
//! `FreeSnapshot`. Pointer-identity comparisons (`snapshot == CurrentSnapshot`)
//! become `Rc::ptr_eq`.
//!
//! All of this state is per-backend (one backend = one thread), so it lives in
//! a `thread_local!` cell — the C globals are plain process-local variables.

use std::cell::RefCell;
use std::rc::Rc;

use ::types_core::FirstNormalTransactionId;
use ::types_core::TransactionId;
use ::types_logical::TupleCidHash;
use snapshot::{SnapshotData, SnapshotType};

/// A shared, mutable, owned snapshot — the replacement for C's `Snapshot`
/// (`*mut SnapshotData`). Cloning bumps the `Rc` refcount, mirroring how C
/// stores the same pointer on the active stack / in the registered heap.
pub type SnapHandle = Rc<RefCell<SnapshotData>>;

/// Construct a zeroed `SnapshotData` of the given type, matching C's
/// `{SNAPSHOT_MVCC}` aggregate initializer (type set, everything else
/// zero/empty/false).
pub fn new_snapshot_data(snapshot_type: SnapshotType) -> SnapshotData {
    SnapshotData {
        snapshot_type,
        vistest: ::snapshot::snapshot::GlobalVisStateHandle::new(0),
        xmin: 0,
        xmax: 0,
        xip: Vec::new(),
        xcnt: 0,
        subxip: Vec::new(),
        subxcnt: 0,
        suboverflowed: false,
        takenDuringRecovery: false,
        copied: false,
        curcid: 0,
        speculativeToken: 0,
        active_count: 0,
        regd_count: 0,
        snapXactCompletionCount: 0,
        reg_id: 0,
    }
}

/// Allocate a fresh shared snapshot handle wrapping `data`.
pub fn new_handle(data: SnapshotData) -> SnapHandle {
    Rc::new(RefCell::new(data))
}

/// One element of the active snapshot stack (`ActiveSnapshotElt`,
/// snapmgr.c:172). The C list is singly-linked with the top at the head; a
/// `Vec` used as a stack with the top at the end preserves the
/// non-increasing-`as_level` invariant the code relies on (top has the highest
/// level).
pub struct ActiveSnapshotElt {
    pub as_snap: SnapHandle,
    pub as_level: i32,
}

/// Info about an exported snapshot (`ExportedSnapshot`, snapmgr.c:205).
pub struct ExportedSnapshot {
    pub snapfile: String,
    pub snapshot: SnapHandle,
}

/// All process-local snapshot-manager state. Mirrors the file-scope statics of
/// snapmgr.c one-for-one.
pub struct SnapMgrState {
    /// `static SnapshotData CurrentSnapshotData = {SNAPSHOT_MVCC}` — the
    /// reusable backing struct. Always present; `current` is `Some` when valid.
    pub current_data: SnapHandle,
    pub secondary_data: SnapHandle,
    pub catalog_data: SnapHandle,

    /// `static Snapshot CurrentSnapshot/SecondarySnapshot/CatalogSnapshot` —
    /// `Some` (pointing at the matching `*_data`, or a copy) when valid, `None`
    /// for the C NULL pointer.
    pub current: Option<SnapHandle>,
    pub secondary: Option<SnapHandle>,
    pub catalog: Option<SnapHandle>,

    /// `static Snapshot HistoricSnapshot` (snapmgr.c:151).
    pub historic: Option<SnapHandle>,

    /// `(relfilelocator, ctid) => (cmin, cmax)` lookup hash during timetravel
    /// (`static HTAB *tuplecid_data`). Built by reorderbuffer's
    /// `ReorderBufferBuildTupleCidHash` and handed to the snapshot manager by
    /// `SetupHistoricSnapshot`; the C `HTAB *` is modelled as the real owned
    /// map value (`None` == the C `NULL` when no historic snapshot is set up).
    /// The manager only stores and returns it; reorderbuffer's
    /// `ResolveCminCmaxDuringDecoding` is the sole reader.
    pub tuplecid_data: Option<TupleCidHash>,

    /// Active snapshot stack (`static ActiveSnapshotElt *ActiveSnapshot`). Top
    /// of stack is the last element.
    pub active: Vec<ActiveSnapshotElt>,

    /// `static pairingheap RegisteredSnapshots` ordered by xmin (snapmgr.c:189).
    /// The heap is only ever queried for its minimum-`xmin` member and mutated
    /// by add/remove, so an unordered `Vec` scanned with the wraparound-aware
    /// comparator reproduces it exactly.
    pub registered: Vec<SnapHandle>,

    /// `bool FirstSnapshotSet` (snapmgr.c:192).
    pub first_snapshot_set: bool,

    /// `static Snapshot FirstXactSnapshot` (snapmgr.c:199).
    pub first_xact_snapshot: Option<SnapHandle>,

    /// `static List *exportedSnapshots = NIL` (snapmgr.c:212).
    pub exported_snapshots: Vec<ExportedSnapshot>,

    /// `TransactionId TransactionXmin = FirstNormalTransactionId`
    /// (snapmgr.c:158). Equals `MyProc->xmin`.
    pub transaction_xmin: TransactionId,

    /// `TransactionId RecentXmin = FirstNormalTransactionId` (snapmgr.c:159).
    pub recent_xmin: TransactionId,

    /// Monotonic counter handing out `SnapshotData::reg_id` values. Stands in
    /// for the stable palloc'd-pointer identity C uses to key the
    /// `RegisteredSnapshots` heap, which the value-marshalling seams can't
    /// preserve. Never reset; `reg_id == 0` always means "unregistered".
    pub next_reg_id: u64,
}

impl SnapMgrState {
    fn new() -> Self {
        SnapMgrState {
            current_data: new_handle(new_snapshot_data(SnapshotType::SNAPSHOT_MVCC)),
            secondary_data: new_handle(new_snapshot_data(SnapshotType::SNAPSHOT_MVCC)),
            catalog_data: new_handle(new_snapshot_data(SnapshotType::SNAPSHOT_MVCC)),
            current: None,
            secondary: None,
            catalog: None,
            historic: None,
            tuplecid_data: None,
            active: Vec::new(),
            registered: Vec::new(),
            first_snapshot_set: false,
            first_xact_snapshot: None,
            exported_snapshots: Vec::new(),
            transaction_xmin: FirstNormalTransactionId,
            recent_xmin: FirstNormalTransactionId,
            next_reg_id: 0,
        }
    }
}

thread_local! {
    static STATE: RefCell<SnapMgrState> = RefCell::new(SnapMgrState::new());
}

/// Run `f` with mutable access to the process-local snapshot-manager state.
pub fn with_state<T>(f: impl FnOnce(&mut SnapMgrState) -> T) -> T {
    STATE.with(|cell| f(&mut cell.borrow_mut()))
}
