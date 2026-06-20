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
/// The flexible array member is modelled as an owned vector.
#[derive(Clone, Debug)]
pub struct SharedSortInfo<'mcx> {
    /// `int num_workers`.
    pub num_workers: i32,
    /// `TuplesortInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER]`.
    pub sinstrument: PgVec<'mcx, TuplesortInstrumentation>,
}

impl<'mcx> SharedSortInfo<'mcx> {
    /// A freshly allocated container with the flexible array empty (the C
    /// `shm_toc_allocate` + `memset(0)` before any worker fills a slot).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        SharedSortInfo {
            num_workers: 0,
            sinstrument: PgVec::new_in(mcx),
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
    /// `NULL`.
    pub shared_info: Option<PgBox<'mcx, SharedSortInfo<'mcx>>>,
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
