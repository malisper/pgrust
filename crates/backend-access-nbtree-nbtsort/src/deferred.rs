//! Deferred entry points of `nbtsort.c` — the heap-scan-driven build and the
//! parallel build coordination.
//!
//! These are NOT silently stubbed: they panic loudly (sanctioned
//! mirror-and-panic, never a `todo!()`). They are blocked on prerequisites the
//! repo does not yet provide, and grounding them now would be a false-green or
//! require a cross-subsystem port out of this crate's scope:
//!
//! 1. **Closure-based build scan.** `btbuild` / `_bt_spools_heapscan` /
//!    `_bt_build_callback` / `_bt_spool` drive `table_index_build_scan`, which
//!    invokes a per-heap-tuple Rust **callback closure** carrying borrows of
//!    the live `BTBuildState` spools. The seam mechanism is a plain `fn`
//!    pointer — it cannot carry a closure with captured lifetimes — so the
//!    scan/callback boundary cannot be expressed as a seam yet. The grounded
//!    leaf-load path ([`crate::_bt_leafbuild`] → [`crate::_bt_load`]) is driven
//!    instead by the pull-based `tuplesort_getindextuple` seam and IS fully
//!    ported.
//!
//! 2. **Parallel build.** `_bt_begin_parallel` / `_bt_end_parallel` /
//!    `_bt_parallel_estimate_shared` / `_bt_parallel_heapscan` /
//!    `_bt_leader_participate_as_worker` / `_bt_parallel_scan_and_sort` /
//!    `_bt_parallel_build_main` coordinate leader and worker processes through
//!    dynamic shared memory: a `ParallelContext` + DSM segment + `shm_toc`
//!    holding the shared `BTShared` (with an embedded spinlock + condition
//!    variable), the shared tuplesort `Sharedsort`, the parallel table scan
//!    descriptor, and per-worker WAL/buffer usage arrays. That machinery lives
//!    across `access/transam/parallel.c`, `storage/ipc`, `utils/sort`, tableam,
//!    snapshots and instrumentation — sibling subsystems whose ports are not
//!    present — and it also rides on the same per-tuple build-scan callback as
//!    (1).

/// `btbuild()` — build a new btree index (the AM's `ambuild`).
///
/// DEFERRED: drives the closure-based heap build scan; see module docs. Use
/// [`crate::_bt_leafbuild`] for the grounded sort + leaf-load half once a spool
/// has been filled by the (external) build scan.
pub fn btbuild() -> ! {
    panic!(
        "nbtsort::btbuild is deferred: the heap build scan invokes a per-tuple \
         callback closure that the function seam cannot carry yet; the grounded \
         leaf-load path is _bt_leafbuild"
    );
}

/// `_bt_spools_heapscan()` — create spools, scan the heap filling them.
///
/// DEFERRED: closure-based build scan; see module docs.
pub fn _bt_spools_heapscan() -> ! {
    panic!("nbtsort::_bt_spools_heapscan is deferred (closure-based build scan)");
}

/// `_bt_build_callback()` — per-heap-tuple build-scan callback.
///
/// DEFERRED: the callback boundary cannot be a function seam; see docs.
pub fn _bt_build_callback() -> ! {
    panic!("nbtsort::_bt_build_callback is deferred (scan-callback boundary)");
}

/// `_bt_spool()` — spool an index tuple via `tuplesort_putindextuplevalues`.
///
/// DEFERRED: the push side is fed only by the deferred heap build scan, and
/// `tuplesort_putindextuplevalues` is part of the unported tuplesort owner. The
/// grounded entry the build drives is the pull-based [`crate::_bt_load`].
pub fn _bt_spool() -> ! {
    panic!(
        "nbtsort::_bt_spool is deferred: the push side is fed by the deferred heap \
         build scan; the grounded path is _bt_load over tuplesort_getindextuple"
    );
}

/// `_bt_begin_parallel()` — create the parallel context and launch workers.
///
/// DEFERRED: DSM / parallel-context / shared-tuplesort machinery; see docs.
pub fn _bt_begin_parallel() -> ! {
    panic!("nbtsort::_bt_begin_parallel is deferred (parallel build / DSM)");
}

/// `_bt_end_parallel()` — shut down workers, destroy the parallel context.
///
/// DEFERRED: parallel build machinery; see docs.
pub fn _bt_end_parallel() -> ! {
    panic!("nbtsort::_bt_end_parallel is deferred (parallel build / DSM)");
}

/// `_bt_parallel_estimate_shared()` — size of the shared build state.
///
/// DEFERRED: parallel build machinery; see docs.
pub fn _bt_parallel_estimate_shared() -> ! {
    panic!("nbtsort::_bt_parallel_estimate_shared is deferred (parallel build / DSM)");
}

/// `_bt_parallel_heapscan()` — within the leader, wait for end of heap scan.
///
/// DEFERRED: parallel build machinery; see docs.
pub fn _bt_parallel_heapscan() -> ! {
    panic!("nbtsort::_bt_parallel_heapscan is deferred (parallel build / DSM)");
}

/// `_bt_leader_participate_as_worker()` — leader participates as a worker.
///
/// DEFERRED: parallel build machinery; see docs.
pub fn _bt_leader_participate_as_worker() -> ! {
    panic!("nbtsort::_bt_leader_participate_as_worker is deferred (parallel build / DSM)");
}

/// `_bt_parallel_scan_and_sort()` — a worker's portion of a parallel sort.
///
/// DEFERRED: parallel build machinery + scan callback; see docs.
pub fn _bt_parallel_scan_and_sort() -> ! {
    panic!("nbtsort::_bt_parallel_scan_and_sort is deferred (parallel build / DSM)");
}

/// `_bt_parallel_build_main()` — a worker's portion of a parallel build.
///
/// DEFERRED: parallel build machinery + scan callback; see docs.
pub fn _bt_parallel_build_main() -> ! {
    panic!("nbtsort::_bt_parallel_build_main is deferred (parallel build / DSM)");
}
