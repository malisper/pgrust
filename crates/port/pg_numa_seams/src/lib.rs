//! Seam declarations for `src/port/pg_numa.c` (NUMA portability routines;
//! part of the `port-batch*` catalog units).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. On non-`USE_LIBNUMA` builds the C
//! fallbacks return `-1`/`0`; the owner installs whichever variant the build
//! provides. (`pg_numa_touch_mem_if_required` is a `pg_numa.h` static inline
//! and is ported in its consumers, not seamed.)

seam_core::seam!(
    /// `pg_numa_init()` — initialize libnuma; `-1` when NUMA is unavailable.
    pub fn pg_numa_init() -> i32
);

seam_core::seam!(
    /// `pg_numa_query_pages(pid, count, pages, status)` — query the NUMA node
    /// of each page via `move_pages(2)`; the C `count`/array pointers are
    /// folded into the equal-length slices. Returns `-1` on failure with
    /// `errno` set (the caller reports `%m`).
    pub fn pg_numa_query_pages(pid: i32, pages: &mut [*mut u8], status: &mut [i32]) -> i32
);

seam_core::seam!(
    /// `pg_numa_get_max_node()` — highest possible NUMA node number.
    pub fn pg_numa_get_max_node() -> i32
);
