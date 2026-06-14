//! Node-state and plan-node vocabulary owned by `nodeBitmapHeapscan.c`
//! (execnodes.h / plannodes.h fields it consumes).
//!
//! These types reference the table-scan descriptor (`TableScanDesc`, in
//! `types-tableam`) and the `TIDBitmap` (in `types-tidbitmap`), which sit above
//! the shared executor-node knot (`types-nodes`): `types-tableam` already
//! depends on `types-nodes`, so the bitmap-heap-scan state — the first and only
//! consumer of `ss_currentScanDesc` — lives in this crate rather than forcing a
//! cyclic edge into `types-nodes`.

use core::sync::atomic::Ordering;

use backend_access_transam_parallel::shared_dsm_object::{SharedDsmObject, SharedRef};
use mcx::{Mcx, PgBox};
use types_condvar::ConditionVariable;
use types_nodes::execnodes::ScanStateData;
use types_nodes::execexpr::ExprState;
use types_rel::Relation;
use types_storage::storage::{pg_atomic_uint32, pg_atomic_uint64};
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
/// memory during parallel scans. `#[repr(C)]` so a private snapshot of one
/// in-DSM `SharedBitmapHeapScanInstr` slot has the same two-`uint64` layout.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BitmapHeapScanInstrumentation {
    /// `uint64 exact_pages` — total number of exact pages retrieved.
    pub exact_pages: u64,
    /// `uint64 lossy_pages` — total number of lossy pages retrieved.
    pub lossy_pages: u64,
}

/// `ParallelBitmapHeapState` (execnodes.h) — shared state for a parallel bitmap
/// heap scan, allocated in the DSM segment.
///
/// `#[repr(C)]` with the C field order (`tbmiterator`, `mutex`, `state`, `cv`)
/// because the leader placement-initializes this struct DIRECTLY in the DSM
/// chunk and every worker reinterprets the SAME in-segment bytes through the
/// keystone [`SharedRef`]; the layout must match the C struct so the
/// cross-process aliasing is over the real fields.
///
/// `tbmiterator` and `state` are the two fields the C mutates concurrently
/// (under `mutex`), so — to be a sound [`SharedDsmObject`] (mutated through a
/// shared `&self`) — they are interior-mutable atomic words: `tbmiterator` a
/// `pg_atomic_uint64` over the `dsa_pointer`, `state` a `pg_atomic_uint32` over
/// the `SharedBitmapState` `i32` repr. The C accesses them as plain fields while
/// holding `mutex`; using relaxed atomic load/store under that same spinlock is
/// behaviour-preserving (the spinlock supplies the ordering, exactly like the
/// `phs_startblock`/`phs_nallocated` model in `ParallelTableScanDescData`).
#[repr(C)]
#[derive(Debug, Default)]
pub struct ParallelBitmapHeapState {
    /// `dsa_pointer tbmiterator` — iterator for scanning the TID bitmap. Held
    /// in an atomic word so it round-trips through the shared `&self` under
    /// `mutex` (the C plain field, serialized by the spinlock).
    pub tbmiterator: pg_atomic_uint64,
    /// `slock_t mutex` — mutual exclusion for state machine and iterator.
    pub mutex: Spinlock,
    /// `SharedBitmapState state` — current state of the TID bitmap, as the
    /// `i32` repr in an atomic word (see the struct doc).
    pub state: pg_atomic_uint32,
    /// `ConditionVariable cv` — used for waiting/wakeup on state changes.
    pub cv: ConditionVariable,
}

// SAFETY: `ParallelBitmapHeapState` is `#[repr(C)]` matching the C struct
// field-for-field; every field the C mutates concurrently after the launch
// barrier is interior-mutable — `tbmiterator`/`state` are atomic words and
// `mutex`/`cv` are the in-segment spinlock / condition variable; the leader's
// placement initializer ([`ParallelBitmapHeapState::init_in_place`]) writes
// every field. A shared `&Self` is therefore sound to alias across processes.
unsafe impl SharedDsmObject for ParallelBitmapHeapState {}

impl ParallelBitmapHeapState {
    /// A zeroed shared state (the C `SpinLockInit` + `ConditionVariableInit`
    /// leave the lock free and the CV empty, `state = BM_INITIAL`,
    /// `tbmiterator = 0`).
    pub fn new() -> Self {
        ParallelBitmapHeapState {
            tbmiterator: pg_atomic_uint64::new(0),
            mutex: Spinlock::new(),
            state: pg_atomic_uint32::new(BM_INITIAL as i32 as u32),
            cv: ConditionVariable::new(),
        }
    }

    /// `dsa_pointer tbmiterator` (read), the relaxed load issued while holding
    /// `mutex` (the C plain read).
    pub fn tbmiterator(&self) -> dsa_pointer {
        self.tbmiterator.read()
    }

    /// `pstate->tbmiterator = dp` (the C plain store under `mutex`).
    pub fn set_tbmiterator(&self, dp: dsa_pointer) {
        self.tbmiterator.write(dp);
    }

    /// `SharedBitmapState state` (read), the relaxed load issued while holding
    /// `mutex` (the C plain read).
    pub fn state(&self) -> SharedBitmapState {
        match self.state.read() as i32 {
            x if x == BM_INPROGRESS as i32 => BM_INPROGRESS,
            x if x == BM_FINISHED as i32 => BM_FINISHED,
            _ => BM_INITIAL,
        }
    }

    /// `pstate->state = s` (the C plain store under `mutex`).
    pub fn set_state(&self, s: SharedBitmapState) {
        self.state.value.store(s as i32 as u32, Ordering::Relaxed);
    }
}

/// One element of the DSM-resident `SharedBitmapHeapInstrumentation` flexible
/// array — the same shape as [`BitmapHeapScanInstrumentation`], but with each
/// counter in an atomic word so a worker can write its own slot through the
/// shared `&self` at executor shutdown. `#[repr(C)]` so its layout matches the
/// C `BitmapHeapScanInstrumentation` array element (two `uint64`s).
#[repr(C)]
#[derive(Debug, Default)]
pub struct SharedBitmapHeapScanInstr {
    /// `uint64 exact_pages`
    pub exact_pages: pg_atomic_uint64,
    /// `uint64 lossy_pages`
    pub lossy_pages: pg_atomic_uint64,
}

// SAFETY: `#[repr(C)]` matching the C `BitmapHeapScanInstrumentation` array
// element (two `uint64`s); both counters are interior-mutable atomic words,
// each slot written only by its owning worker. A shared `&Self` is sound to
// alias across processes.
unsafe impl SharedDsmObject for SharedBitmapHeapScanInstr {}

impl SharedBitmapHeapScanInstr {
    /// `si->exact_pages += node->stats.exact_pages; si->lossy_pages += ...` —
    /// each worker accumulates into its own slot (relaxed, the slot is written
    /// only by its owning worker).
    pub fn accumulate(&self, exact: u64, lossy: u64) {
        self.exact_pages
            .value
            .fetch_add(exact, Ordering::Relaxed);
        self.lossy_pages
            .value
            .fetch_add(lossy, Ordering::Relaxed);
    }

    /// Snapshot the slot as a plain [`BitmapHeapScanInstrumentation`] (the
    /// leader's `memcpy` of the shared array into private memory).
    pub fn snapshot(&self) -> BitmapHeapScanInstrumentation {
        BitmapHeapScanInstrumentation {
            exact_pages: self.exact_pages.read(),
            lossy_pages: self.lossy_pages.read(),
        }
    }
}

/// `SharedBitmapHeapInstrumentation` (execnodes.h) — shared instrumentation for
/// a parallel bitmap heap scan; the header before a flexible array of
/// [`SharedBitmapHeapScanInstr`] (the C `BitmapHeapScanInstrumentation
/// sinstrument[FLEXIBLE_ARRAY_MEMBER]`).
///
/// `#[repr(C)]` matching the C `{ int num_workers; <array> }` header so the
/// keystone places it directly in the DSM chunk and workers reinterpret the
/// same bytes. `num_workers` is launch-once (leader-write pre-launch / worker-
/// read); the array elements are interior-mutable (each worker writes its own
/// slot through the shared `&self`).
#[repr(C)]
#[derive(Debug, Default)]
pub struct SharedBitmapHeapInstrumentation {
    /// `int num_workers`
    pub num_workers: i32,
}

// SAFETY: `#[repr(C)]` header matching the C struct; `num_workers` is a
// launch-once leader-write / worker-read scalar; the flexible tail of
// `SharedBitmapHeapScanInstr` elements is interior-mutable (atomic counters),
// each slot written only by its owning worker. A shared `&Self` (plus the
// keystone flexible-array slice over the tail) is sound to alias across
// processes.
unsafe impl SharedDsmObject for SharedBitmapHeapInstrumentation {}

impl SharedBitmapHeapInstrumentation {
    /// `offsetof(SharedBitmapHeapInstrumentation, sinstrument)` — the size of
    /// the fixed header before the flexible array (the `int num_workers` plus
    /// padding to the array's alignment). The C uses this when sizing the DSM
    /// chunk; the owned model computes it from the layout (8-byte aligned, as
    /// the `SharedBitmapHeapScanInstr` array element is `uint64`-aligned).
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

/// What `node->sinstrument` (a single C `SharedBitmapHeapInstrumentation *`)
/// points at, modeled as the two states the C pointer takes:
///
/// * `Shared` — the DSM-resident object the leader placed / the worker attached
///   (the keystone header [`SharedRef`] + the flexible-array [`SharedSlice`]
///   tail). Workers write their own slot through it at shutdown.
/// * `Private` — the leader's private deep copy made by
///   `ExecBitmapHeapRetrieveInstrumentation` (`palloc` + `memcpy`), read by
///   EXPLAIN.
pub enum NodeSinstrument<'mcx> {
    /// In-DSM placement: header + flexible-array tail.
    Shared {
        /// `SharedBitmapHeapInstrumentation *` header (`num_workers`).
        header: SharedRef<'mcx, SharedBitmapHeapInstrumentation>,
        /// The flexible-array `sinstrument[]` tail.
        slots: backend_access_transam_parallel::shared_dsm_object::SharedSlice<
            'mcx,
            SharedBitmapHeapScanInstr,
        >,
    },
    /// Private deep copy (post-`RetrieveInstrumentation`).
    Private {
        /// `num_workers`.
        num_workers: i32,
        /// The copied `sinstrument[]` array.
        sinstrument: alloc::vec::Vec<BitmapHeapScanInstrumentation>,
    },
}

impl<'mcx> NodeSinstrument<'mcx> {
    /// `sinstrument->num_workers`.
    pub fn num_workers(&self) -> i32 {
        match self {
            NodeSinstrument::Shared { header, .. } => header.get().num_workers,
            NodeSinstrument::Private { num_workers, .. } => *num_workers,
        }
    }
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
    /// `ParallelBitmapHeapState *pstate` — the in-DSM shared state placed by
    /// the leader / attached by each worker through the keystone, so `mutex`,
    /// `cv`, `state`, and `tbmiterator` are the real cross-process primitives.
    pub pstate: Option<SharedRef<'mcx, ParallelBitmapHeapState>>,
    /// `SharedBitmapHeapInstrumentation *sinstrument`
    pub sinstrument: Option<NodeSinstrument<'mcx>>,
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
