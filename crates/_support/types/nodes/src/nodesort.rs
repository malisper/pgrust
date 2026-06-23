//! Sort plan-node / executor-state vocabulary (`nodes/plannodes.h`,
//! `executor/execnodes.h`, `utils/tuplesort.h`), trimmed to what `nodeSort.c`
//! consumes.
//!
//! The `Tuplesortstate` carrier mirrors the `Tuplestorestate` one in
//! [`crate::funcapi`]: `tuplesort.c` keeps its state private, everyone else
//! holds it as an opaque pointer. The owned model type-erases the payload;
//! the tuplesort owner (when it lands) names the concrete engine type.

use core::any::Any;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use execparallel::SerializeCursor;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::execstate_tags::T_SortState;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

// ===========================================================================
// tuplesort.h option flags consumed by nodeSort.c.
// ===========================================================================

/// `TUPLESORT_NONE` (utils/tuplesort.h).
pub const TUPLESORT_NONE: i32 = 0;
/// `TUPLESORT_RANDOMACCESS` (utils/tuplesort.h) — non-sequential access to the
/// sort result is required (`1 << 0`).
pub const TUPLESORT_RANDOMACCESS: i32 = 1 << 0;
/// `TUPLESORT_ALLOWBOUNDED` (utils/tuplesort.h) — the tuplesort is able to
/// support bounded sorts (`1 << 1`).
pub const TUPLESORT_ALLOWBOUNDED: i32 = 1 << 1;

// ===========================================================================
// TuplesortInstrumentation (utils/tuplesort.h).
// ===========================================================================

/// `TuplesortMethod` (utils/tuplesort.h) — the sort algorithm used. A zero
/// value (`SORT_TYPE_STILL_IN_PROGRESS`) means a worker never did anything; the
/// other values are single-bit so they can be OR'ed across workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum TuplesortMethod {
    SORT_TYPE_STILL_IN_PROGRESS = 0,
    SORT_TYPE_TOP_N_HEAPSORT = 1 << 0,
    SORT_TYPE_QUICKSORT = 1 << 1,
    SORT_TYPE_EXTERNAL_SORT = 1 << 2,
    SORT_TYPE_EXTERNAL_MERGE = 1 << 3,
}

/// `TuplesortSpaceType` (utils/tuplesort.h) — what the recorded space usage
/// represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum TuplesortSpaceType {
    SORT_SPACE_TYPE_DISK = 0,
    SORT_SPACE_TYPE_MEMORY = 1,
}

/// `TuplesortInstrumentation` (utils/tuplesort.h):
///
/// ```c
/// typedef struct TuplesortInstrumentation {
///     TuplesortMethod sortMethod;
///     TuplesortSpaceType spaceType;
///     int64 spaceUsed;
/// } TuplesortInstrumentation;
/// ```
///
/// `#[repr(C)]` because it is the element type of the `SharedSortInfo`
/// flexible-array member that lives DIRECTLY in the parallel-query DSM segment
/// (`ExecSortInitializeDSM` `shm_toc_allocate`s the chunk and each worker writes
/// its own `sinstrument[ParallelWorkerNumber]` slot into it). Placed/attached
/// through the typed shared-DSM-object flex primitive
/// (`shared_dsm_object::place_flex` / `attach_flex`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TuplesortInstrumentation {
    /// `TuplesortMethod sortMethod` — sort algorithm used.
    pub sortMethod: TuplesortMethod,
    /// `TuplesortSpaceType spaceType` — type of space `spaceUsed` represents.
    pub spaceType: TuplesortSpaceType,
    /// `int64 spaceUsed` — space consumption, in kB.
    pub spaceUsed: i64,
}

impl Default for TuplesortInstrumentation {
    /// `memset(..., 0, ...)` — a zeroed slot (the C parallel-sort
    /// infrastructure relies on a zero method meaning "still in progress").
    fn default() -> Self {
        TuplesortInstrumentation {
            sortMethod: TuplesortMethod::SORT_TYPE_STILL_IN_PROGRESS,
            spaceType: TuplesortSpaceType::SORT_SPACE_TYPE_DISK,
            spaceUsed: 0,
        }
    }
}

// SAFETY (audited per the `SharedDsmObject` contract):
//   1. `TuplesortInstrumentation` is `#[repr(C)]` and matches `tuplesort.h`
//      field-for-field (TuplesortMethod (int), TuplesortSpaceType (int), int64,
//      in C order; the two `#[repr(i32)]` enums are 4 bytes each).
//   2. There is NO concurrent mutation of any single element across processes:
//      each parallel worker writes ONLY its own
//      `sinstrument[ParallelWorkerNumber]` slot (in `ExecSort`'s copyback), and
//      the leader reads the whole array only in
//      `ExecSortRetrieveInstrumentation`, which the C runs after the workers have
//      detached. The element bytes are therefore never aliased-and-mutated
//      concurrently, so plain POD scalars satisfy clause 2 by partition.
//   3. The leader's placement initializer (`ExecSortInitializeDSM`) zero-fills
//      every element before any worker attaches (`place_flex` writes
//      `TuplesortInstrumentation::default()` into each slot — a zero method
//      meaning "still in progress").
//   4. A shared `&TuplesortInstrumentation` aliasing another process's mapping of
//      the SAME element is never created concurrently with a write (clause 2).
unsafe impl types_parallel::SharedDsmObject for TuplesortInstrumentation {}

// ===========================================================================
// Tuplesortstate carrier (utils/tuplesort.c, private).
// ===========================================================================

/// `Tuplesortstate *` (utils/tuplesort.h) — opaque to every consumer. The owned
/// model type-erases the real engine state; only the tuplesort owner downcasts.
pub struct Tuplesortstate<'mcx> {
    /// The real owned state, type-erased and context-allocated (C:
    /// `tuplesort_begin_common` pallocs in the caller's current context);
    /// `None` for a default-constructed carrier (the C `NULL`).
    state: Option<PgBox<'mcx, dyn Any>>,
}

impl<'mcx> Tuplesortstate<'mcx> {
    /// `tuplesort_begin_*`-shaped construction: allocate the concrete engine
    /// state in `mcx` and type-erase it. Only the tuplesort owner (or a test
    /// mock) calls this. Fallible: allocating.
    pub fn begin<T: Any>(mcx: Mcx<'mcx>, state: T) -> PgResult<Self> {
        let boxed = alloc_in(mcx, state)?;
        let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
        // SAFETY: `ptr` came from `into_raw_with_allocator` with `alloc`; the
        // cast only attaches the `dyn Any` vtable (no `CoerceUnsized` on stable).
        let erased: PgBox<'mcx, dyn Any> =
            unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) };
        Ok(Tuplesortstate {
            state: Some(erased),
        })
    }

    /// The type-erased engine state (the tuplesort owner downcasts; loud panic
    /// on mismatch is its job). `None` is the C `NULL`.
    pub fn payload(&self) -> Option<&dyn Any> {
        self.state.as_deref()
    }

    /// Mutable [`Self::payload`].
    pub fn payload_mut(&mut self) -> Option<&mut (dyn Any + 'static)> {
        self.state.as_deref_mut()
    }
}

impl core::fmt::Debug for Tuplesortstate<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.state {
            Some(_) => f.write_str("Tuplesortstate(<owned state>)"),
            None => f.write_str("Tuplesortstate(<empty>)"),
        }
    }
}

// ===========================================================================
// ValidateIndexState (catalog/index.h) — CREATE INDEX CONCURRENTLY validation.
// ===========================================================================

/// `ValidateIndexState` (`catalog/index.h`):
///
/// ```c
/// typedef struct ValidateIndexState {
///     Tuplesortstate *tuplesort;  /* for sorting the index TIDs */
///     /* statistics (for debug messages only): */
///     double  htups, itups, tups_inserted;
/// } ValidateIndexState;
/// ```
///
/// The merge state for `validate_index` (catalog/index.c) and the heap AM's
/// `index_validate_scan` (heapam_handler.c). The `Tuplesortstate *` is carried
/// by value (the owned-model type-erased carrier); the validation phase feeds
/// it the sorted index TIDs via `index_bulk_delete`'s callback and then
/// merge-joins the heap scan against it.
#[derive(Debug)]
pub struct ValidateIndexState<'mcx> {
    /// `Tuplesortstate *tuplesort` — for sorting the index TIDs.
    pub tuplesort: Tuplesortstate<'mcx>,
    /// `double htups` — heap tuples scanned (debug only).
    pub htups: f64,
    /// `double itups` — index tuples fed into the sort (debug only).
    pub itups: f64,
    /// `double tups_inserted` — entries inserted into the index (debug only).
    pub tups_inserted: f64,
}

// ===========================================================================
// Sort plan node (nodes/plannodes.h).
// ===========================================================================

/// `Sort` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct Sort {
///     Plan      plan;
///     int       numCols;
///     AttrNumber *sortColIdx;
///     Oid       *sortOperators;
///     Oid       *collations;
///     bool      *nullsFirst;
/// } Sort;
/// ```
///
/// The four parallel arrays are `numCols` long; the owned model carries them as
/// vectors (their length is `numCols`).
#[derive(Debug)]
pub struct Sort<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `int numCols` — number of sort-key columns.
    pub numCols: i32,
    /// `AttrNumber *sortColIdx` — their indexes in the target list.
    pub sortColIdx: PgVec<'mcx, AttrNumber>,
    /// `Oid *sortOperators` — OIDs of operators to sort them by.
    pub sortOperators: PgVec<'mcx, Oid>,
    /// `Oid *collations` — OIDs of collations.
    pub collations: PgVec<'mcx, Oid>,
    /// `bool *nullsFirst` — NULLS FIRST/LAST directions.
    pub nullsFirst: PgVec<'mcx, bool>,
}

impl Sort<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Sort<'b>> {
        Ok(Sort {
            plan: self.plan.clone_in(mcx)?,
            numCols: self.numCols,
            sortColIdx: copy_vec(mcx, &self.sortColIdx)?,
            sortOperators: copy_vec(mcx, &self.sortOperators)?,
            collations: copy_vec(mcx, &self.collations)?,
            nullsFirst: copy_vec(mcx, &self.nullsFirst)?,
        })
    }
}

fn copy_vec<'b, T: Copy>(mcx: Mcx<'b>, src: &PgVec<'_, T>) -> PgResult<PgVec<'b, T>> {
    let mut out = vec_with_capacity_in(mcx, src.len())?;
    for &v in src.iter() {
        out.push(v);
    }
    Ok(out)
}

// ===========================================================================
// SortState executor node + SharedSortInfo (executor/execnodes.h).
// ===========================================================================

/// `offsetof(SharedSortInfo, num_workers)`-bearing header of `SharedSortInfo`
/// (execnodes.h): `{ int num_workers; TuplesortInstrumentation sinstrument[]; }`.
/// This is the `H` of the `place_flex`/`attach_flex` flexible-array placement;
/// the `sinstrument[]` tail is the `E = TuplesortInstrumentation` slice.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SharedSortInfoHeader {
    /// `int num_workers`.
    pub num_workers: i32,
}

// SAFETY: `#[repr(C)]` POD header written once by the leader
// (`ExecSortInitializeDSM`) before any worker attaches, read-only thereafter
// (workers only read `num_workers`); no concurrent mutation. Matches the C
// `SharedSortInfo` header field-for-field.
unsafe impl types_parallel::SharedDsmObject for SharedSortInfoHeader {}

/// `SharedSortInfo` (execnodes.h) — shared-memory container for per-worker sort
/// information:
///
/// ```c
/// typedef struct SharedSortInfo {
///     int num_workers;
///     TuplesortInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER];
/// } SharedSortInfo;
/// ```
///
/// In C this is a single `SharedSortInfo *` pointer that is FIRST the
/// DSM-resident shared area (set in `ExecSortInitializeDSM` / inherited by
/// workers via `shm_toc_lookup`) and is LATER REPLACED, in
/// `ExecSortRetrieveInstrumentation`, by a backend-local `palloc`'d copy. Each
/// worker writes its own `sinstrument[ParallelWorkerNumber]` slot into the DSM
/// array directly. The two states have different ownership (cross-process DSM
/// view vs. owned backend-local array), so they are modelled as the two arms.
#[derive(Debug)]
pub enum SharedSortInfo<'mcx> {
    /// The DSM-resident shared area: a cursor to the `shm_toc`-allocated chunk
    /// (`{ SharedSortInfoHeader; TuplesortInstrumentation[num_workers] }`) plus
    /// the worker count needed to recover the flex length. Mirrors the leader's
    /// `node->shared_info = shm_toc_allocate(...)` and the worker's
    /// `shm_toc_lookup` result.
    Dsm {
        /// Real in-segment chunk address (the `shm_toc_allocate`/`shm_toc_lookup`
        /// return value).
        chunk: SerializeCursor,
        /// The DSM segment the chunk lives in, so the retrieve path can
        /// `attach_flex` the array and the worker copyback can `with_mut` its
        /// slot before detach.
        seg: execparallel::DsmSegmentHandle,
        /// `shared_info->num_workers`.
        num_workers: i32,
    },
    /// The backend-local copy `ExecSortRetrieveInstrumentation` makes before the
    /// DSM segment is detached (`node->shared_info = palloc(size); memcpy(...)`).
    Local {
        /// `shared_info->num_workers`.
        num_workers: i32,
        /// `TuplesortInstrumentation sinstrument[]` copied out of DSM.
        sinstrument: PgVec<'mcx, TuplesortInstrumentation>,
    },
}

impl<'mcx> SharedSortInfo<'mcx> {
    /// `shared_info->num_workers` — the number of per-worker slots, regardless of
    /// arm.
    pub fn num_workers(&self) -> i32 {
        match self {
            SharedSortInfo::Dsm { num_workers, .. } => *num_workers,
            SharedSortInfo::Local { num_workers, .. } => *num_workers,
        }
    }
}

/// `SortState` (execnodes.h) — owned-tree form of the sort executor node:
///
/// ```c
/// typedef struct SortState {
///     ScanState   ss;
///     bool        randomAccess;
///     bool        bounded;
///     int64       bound;
///     bool        sort_Done;
///     bool        bounded_Done;
///     int64       bound_Done;
///     void       *tuplesortstate;
///     bool        am_worker;
///     bool        datumSort;
///     SharedSortInfo *shared_info;
/// } SortState;
/// ```
#[derive(Debug, Default)]
pub struct SortStateData<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `bool randomAccess` — need random access to sort output?
    pub randomAccess: bool,
    /// `bool bounded` — is the result set bounded?
    pub bounded: bool,
    /// `int64 bound` — if bounded, how many tuples are needed.
    pub bound: i64,
    /// `bool sort_Done` — sort completed yet?
    pub sort_Done: bool,
    /// `bool bounded_Done` — value of bounded we did the sort with.
    pub bounded_Done: bool,
    /// `int64 bound_Done` — value of bound we did the sort with.
    pub bound_Done: i64,
    /// `void *tuplesortstate` — private state of tuplesort.c. `None` is the C
    /// `NULL`.
    pub tuplesortstate: Option<PgBox<'mcx, Tuplesortstate<'mcx>>>,
    /// `bool am_worker` — are we a worker?
    pub am_worker: bool,
    /// `bool datumSort` — Datum sort instead of tuple sort?
    pub datumSort: bool,
    /// `SharedSortInfo *shared_info` — one entry per worker. `None` is the C
    /// `NULL`. Either the DSM-resident shared area (leader after
    /// `ExecSortInitializeDSM` / worker after `ExecSortInitializeWorker`) or the
    /// backend-local copy (leader after `ExecSortRetrieveInstrumentation`).
    pub shared_info: Option<SharedSortInfo<'mcx>>,
}

impl<'mcx> SortStateData<'mcx> {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}

/// `nodeTag(SortState)`.
pub const fn sort_state_tag() -> NodeTag {
    T_SortState
}
