//! `src/port/pg_numa.c` — basic NUMA portability routines.
//!
//! Upstream only provides a real implementation on Linux via libnuma
//! (`#ifdef USE_LIBNUMA`). On every other platform — including macOS, which
//! is what pgrust builds and tests on — the C file compiles the `#else`
//! "empty wrappers" branch: `pg_numa_init()` returns `-1` ("NUMA is not
//! available"), `pg_numa_query_pages()` returns `0`, and
//! `pg_numa_get_max_node()` returns `0`.
//!
//! pgrust is a non-`USE_LIBNUMA` build, so this crate faithfully ports the
//! `#else` branch. `pg_numa_touch_mem_if_required` is a `pg_numa.h` static
//! inline and is ported in its consumers, not seamed here.

/// `pg_numa_init()` — empty-wrapper branch: NUMA is not available.
///
/// Faithful port of the `#else` (`!USE_LIBNUMA`) body, which returns `-1` to
/// state that NUMA is not available.
pub fn pg_numa_init() -> i32 {
    -1
}

/// `pg_numa_query_pages(pid, count, pages, status)` — empty-wrapper branch.
///
/// Faithful port of the `#else` (`!USE_LIBNUMA`) body, which is a no-op
/// returning `0`. The page/status slices are left untouched (the C wrapper
/// likewise never writes them).
pub fn pg_numa_query_pages(
    _pid: i32,
    _pages: &mut [*mut u8],
    _status: &mut [i32],
) -> i32 {
    0
}

/// `pg_numa_get_max_node()` — empty-wrapper branch: returns `0`.
///
/// Faithful port of the `#else` (`!USE_LIBNUMA`) body.
pub fn pg_numa_get_max_node() -> i32 {
    0
}

/// Install this crate's seams.
pub fn init_seams() {
    pg_numa_seams::pg_numa_init::set(pg_numa_init);
    pg_numa_seams::pg_numa_query_pages::set(pg_numa_query_pages);
    pg_numa_seams::pg_numa_get_max_node::set(pg_numa_get_max_node);
}
