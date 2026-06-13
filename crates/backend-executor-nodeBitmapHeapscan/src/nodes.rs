//! Node-state and plan-node vocabulary owned by `nodeBitmapHeapscan.c`
//! (execnodes.h / plannodes.h fields it consumes).
//!
//! These types reference the table-scan descriptor (`TableScanDesc`, in
//! `types-tableam`) and the `TIDBitmap` (in `types-tidbitmap`), which sit above
//! the shared executor-node knot (`types-nodes`): `types-tableam` already
//! depends on `types-nodes`, so the bitmap-heap-scan state — the first and only
//! consumer of `ss_currentScanDesc` — lives in this crate rather than forcing a
//! cyclic edge into `types-nodes`.

use mcx::{Mcx, PgBox};
use types_condvar::ConditionVariable;
use types_nodes::execnodes::ScanStateData;
use types_nodes::execexpr::ExprState;
use types_rel::Relation;
use types_storage::Spinlock;
use types_tableam::relscan::TableScanDesc;
use types_tidbitmap::{dsa_pointer, TIDBitmap};

pub use types_nodes::nodeindexscan::Plan;

/// `SharedBitmapState` (execnodes.h) — the state of the parallel bitmap scan.
/// `#[repr(i32)]` to match the C enum's storage in the DSM-shared
/// `ParallelBitmapHeapState`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SharedBitmapState {
    /// `BM_INITIAL` — leader has not yet built the TID bitmap.
    #[default]
    BmInitial = 0,
    /// `BM_INPROGRESS` — leader is building (or has built) the TID bitmap.
    BmInprogress = 1,
    /// `BM_FINISHED` — the leader is done building the TID bitmap.
    BmFinished = 2,
}

/// `BM_INITIAL` (execnodes.h).
pub const BM_INITIAL: SharedBitmapState = SharedBitmapState::BmInitial;
/// `BM_INPROGRESS` (execnodes.h).
pub const BM_INPROGRESS: SharedBitmapState = SharedBitmapState::BmInprogress;
/// `BM_FINISHED` (execnodes.h).
pub const BM_FINISHED: SharedBitmapState = SharedBitmapState::BmFinished;

/// `BitmapHeapScanInstrumentation` (execnodes.h). Plain POD copied into shared
/// memory during parallel scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BitmapHeapScanInstrumentation {
    /// `uint64 exact_pages` — total number of exact pages retrieved.
    pub exact_pages: u64,
    /// `uint64 lossy_pages` — total number of lossy pages retrieved.
    pub lossy_pages: u64,
}

/// `ParallelBitmapHeapState` (execnodes.h) — shared state for a parallel bitmap
/// heap scan, allocated in the DSM segment.
#[derive(Debug, Default)]
pub struct ParallelBitmapHeapState {
    /// `dsa_pointer tbmiterator` — iterator for scanning the TID bitmap.
    pub tbmiterator: dsa_pointer,
    /// `slock_t mutex` — mutual exclusion for state machine and iterator.
    pub mutex: Spinlock,
    /// `SharedBitmapState state` — current state of the TID bitmap.
    pub state: SharedBitmapState,
    /// `ConditionVariable cv` — used for waiting/wakeup on state changes.
    pub cv: ConditionVariable,
}

impl ParallelBitmapHeapState {
    /// A zeroed shared state (the C `SpinLockInit` + `ConditionVariableInit`
    /// leave the lock free and the CV empty, `state = BM_INITIAL`).
    pub fn new() -> Self {
        ParallelBitmapHeapState {
            tbmiterator: 0,
            mutex: Spinlock::new(),
            state: BM_INITIAL,
            cv: ConditionVariable::new(),
        }
    }
}

/// `SharedBitmapHeapInstrumentation` (execnodes.h) — shared instrumentation for
/// a parallel bitmap heap scan; `sinstrument` is a flexible-array-member.
#[derive(Clone, Debug, Default)]
pub struct SharedBitmapHeapInstrumentation {
    /// `int num_workers`
    pub num_workers: i32,
    /// `BitmapHeapScanInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER]`
    pub sinstrument: alloc::vec::Vec<BitmapHeapScanInstrumentation>,
}

impl SharedBitmapHeapInstrumentation {
    /// `offsetof(SharedBitmapHeapInstrumentation, sinstrument)` — the size of
    /// the fixed header before the flexible array (the `int num_workers` plus
    /// padding to the array's alignment). The C uses this when sizing the DSM
    /// chunk; the owned model computes it from the layout (8-byte aligned, as
    /// the `BitmapHeapScanInstrumentation` array is `uint64`-aligned).
    pub fn offset_of_sinstrument() -> usize {
        // num_workers (int) padded up to the 8-byte alignment of the
        // uint64-bearing array element.
        8
    }
}

/// `Scan` plan node (plannodes.h) — the abstract scan base: embeds `Plan` then
/// adds `scanrelid`.
#[derive(Debug, Default)]
pub struct Scan<'mcx> {
    /// `Plan plan` head.
    pub plan: Plan<'mcx>,
    /// `Index scanrelid` — relid (range-table index) of the scanned relation.
    pub scanrelid: types_core::primitive::Index,
}

/// `BitmapHeapScan` plan node (plannodes.h) — embeds `Scan` and adds
/// `bitmapqualorig`.
#[derive(Debug)]
pub struct BitmapHeapScan<'mcx> {
    /// `Scan scan` head.
    pub scan: Scan<'mcx>,
    /// `List *bitmapqualorig` — original index quals (expression nodes), for
    /// rechecking on lossy pages.
    pub bitmapqualorig: mcx::PgVec<'mcx, types_nodes::primnodes::Expr>,
}

/// `BitmapHeapScanState` (execnodes.h) — the bitmap-heap-scan executor node
/// state. (No `derive(Debug)`: `TableScanDescData` carries the AM's opaque
/// `dyn Any` tail, which is not `Debug`.)
pub struct BitmapHeapScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`. Carries the embedded
    /// `PlanState ps` and the `ss_ScanTupleSlot`.
    pub ss: ScanStateData<'mcx>,
    /// `Relation ss.ss_currentRelation` — the open scan relation. Held here
    /// rather than in the shared `ScanStateData` (which can't reference
    /// `types-rel` without cycling `types-nodes`); `None` is the C `NULL`.
    pub ss_currentRelation: Option<Relation<'mcx>>,
    /// `TableScanDesc ss.ss_currentScanDesc` — the underlying table scan
    /// descriptor. Held here for the same layering reason; `None` is the C
    /// `NULL`.
    pub ss_currentScanDesc: Option<TableScanDesc<'mcx>>,
    /// `ExprState *bitmapqualorig`
    pub bitmapqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `TIDBitmap *tbm`
    pub tbm: Option<PgBox<'mcx, TIDBitmap>>,
    /// `BitmapHeapScanInstrumentation stats`
    pub stats: BitmapHeapScanInstrumentation,
    /// `bool initialized`
    pub initialized: bool,
    /// `ParallelBitmapHeapState *pstate`
    pub pstate: Option<alloc::boxed::Box<ParallelBitmapHeapState>>,
    /// `SharedBitmapHeapInstrumentation *sinstrument`
    pub sinstrument: Option<alloc::boxed::Box<SharedBitmapHeapInstrumentation>>,
    /// `bool recheck`
    pub recheck: bool,
}

impl<'mcx> BitmapHeapScanState<'mcx> {
    /// `makeNode(BitmapHeapScanState)` — a zeroed node with its `ScanStateData`
    /// head default-initialized in `mcx` (matching the C `palloc0`).
    pub fn new(_mcx: Mcx<'mcx>) -> Self {
        BitmapHeapScanState {
            ss: ScanStateData::default(),
            ss_currentRelation: None,
            ss_currentScanDesc: None,
            bitmapqualorig: None,
            tbm: None,
            stats: BitmapHeapScanInstrumentation::default(),
            initialized: false,
            pstate: None,
            sinstrument: None,
            recheck: true,
        }
    }
}

/// RAII spinlock guard: `SpinLockAcquire` on construction, `SpinLockRelease`
/// on `Drop`. Acquire is the uncontended TAS fast path, falling back to the
/// `s_lock` backoff loop on contention (storage/lmgr/s_lock.c).
pub struct SpinLockGuard<'a> {
    lock: &'a Spinlock,
}

impl<'a> SpinLockGuard<'a> {
    /// `SpinLockAcquire(lock)`.
    pub fn acquire(lock: &'a Spinlock) -> Self {
        // SpinLockAcquire: TAS_SPIN; on failure, s_lock() the backoff loop.
        if lock.tas_spin() != 0 {
            backend_storage_lmgr_s_lock::s_lock(lock, Some(file!()), line!() as i32, None);
        }
        SpinLockGuard { lock }
    }
}

impl Drop for SpinLockGuard<'_> {
    /// `SpinLockRelease(lock)`.
    fn drop(&mut self) {
        self.lock.unlock();
    }
}
