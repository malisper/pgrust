//! `#[repr(C)]` ABI for `nodeSort.c` (the sort executor node).
//!
//! The sort node is ported in-crate (`backend-executor-nodeSort`), so its state
//! node is a complete, address-stable `#[repr(C)]` struct laid out exactly like
//! the C `SortState` (execnodes.h). The `Sort` plan node it navigates and the
//! `TuplesortInstrumentation` value that `tuplesort_get_stats` fills in are
//! shared with the incremental-sort ABI ([`crate::nodeincrementalsort_abi`]); the
//! shared-memory per-worker container `SharedSortInfo` is spelled out here.
//!
//! The embedded `ScanState`/`PlanState` head reuses the shared
//! [`crate::ScanStateData`] / [`crate::PlanStateData`] layouts from `execnodes`.
//! `Tuplesortstate` is an opaque tuplesort-private type, modelled as `c_void`.

use core::ffi::{c_int, c_void};

use crate::nodeincrementalsort_abi::TuplesortInstrumentation;
use crate::{NodeTag, ScanStateData};

/// NodeTag for `SortState` (the executor state node). Matches `T_SortState`.
pub const T_SortState: NodeTag = crate::execnodes::T_SortState;

/// `Tuplesortstate` — opaque tuplesort-private state (`tuplesort.c`). Only its
/// address is ever held; the node never inspects its contents.
pub type Tuplesortstate = c_void;

/// `SharedSortInfo` (execnodes.h) — shared-memory container for per-worker sort
/// information.
///
/// ```c
/// typedef struct SharedSortInfo {
///     int num_workers;
///     TuplesortInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER];
/// } SharedSortInfo;
/// ```
///
/// The flexible array member `sinstrument` is modelled as a zero-length array;
/// the node indexes into it via the trailing-storage helpers in the node crate.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedSortInfo {
    /// `int num_workers`.
    pub num_workers: c_int,
    /// `TuplesortInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER]`.
    pub sinstrument: [TuplesortInstrumentation; 0],
}

/// `SortState` (execnodes.h) — faithful `#[repr(C)]` ABI for the sort executor
/// node.
///
/// ```c
/// typedef struct SortState {
///     ScanState   ss;             /* its first field is NodeTag */
///     bool        randomAccess;   /* need random access to sort output? */
///     bool        bounded;        /* is the result set bounded? */
///     int64       bound;          /* if bounded, how many tuples are needed */
///     bool        sort_Done;      /* sort completed yet? */
///     bool        bounded_Done;   /* value of bounded we did the sort with */
///     int64       bound_Done;     /* value of bound we did the sort with */
///     void       *tuplesortstate; /* private state of tuplesort.c */
///     bool        am_worker;      /* are we a worker? */
///     bool        datumSort;      /* Datum sort instead of tuple sort? */
///     SharedSortInfo *shared_info; /* one entry per worker */
/// } SortState;
/// ```
///
/// The leading [`ScanStateData`] head's first field is a `NodeTag`, so a
/// `*mut SortStateData` is also a valid `Node *` / `PlanState *` and the opaque
/// public `SortState *` handle.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SortStateData {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
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
    /// `void *tuplesortstate` — private state of tuplesort.c.
    pub tuplesortstate: *mut Tuplesortstate,
    /// `bool am_worker` — are we a worker?
    pub am_worker: bool,
    /// `bool datumSort` — Datum sort instead of tuple sort?
    pub datumSort: bool,
    /// `SharedSortInfo *shared_info` — one entry per worker.
    pub shared_info: *mut SharedSortInfo,
}

// Layout asserts: the embedded heads keep their C offsets so a
// `*mut SortStateData` can be navigated as the C `SortState *`.
const _: () = {
    assert!(core::mem::offset_of!(SortStateData, ss) == 0);
    assert!(core::mem::offset_of!(ScanStateData, ps) == 0);
    assert!(core::mem::offset_of!(crate::PlanStateData, type_) == 0);
    // `sinstrument` flexible array begins right after `num_workers` (with
    // padding to the 8-byte alignment of `TuplesortInstrumentation`, which
    // contains an `int64 spaceUsed`).
    assert!(core::mem::offset_of!(SharedSortInfo, sinstrument) == 8);
};
