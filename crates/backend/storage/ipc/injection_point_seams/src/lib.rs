//! Seam declarations for the injection-point owner
//! (`src/backend/utils/misc/injection_point.c`). ipci.c sizes
//! (`InjectionPointShmemSize`) and initializes (`InjectionPointShmemInit`) its
//! shared state. The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.
//!
//! In addition to the shmem size/init pair, this crate publishes the
//! `INJECTION_POINT(...)` call-site entrypoints (`injection_point_run`,
//! `injection_point_cached`, `injection_point_load`,
//! `is_injection_point_attached`) so that ported backend code (checkpoint,
//! slot invalidation, standby snapshots, pre-auth, ...) can place injection
//! points without taking a direct dependency on the `injection_point` crate
//! (which would form a cycle). The owner installs them from `init_seams()`.

seam_core::seam!(
    /// `InjectionPointShmemSize()` — shared-memory bytes for the injection
    /// point table; summed by ipci.c `CalculateShmemSize`. `Err` carries the
    /// `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn injection_point_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `InjectionPointShmemInit()` — allocate-or-attach the injection point
    /// shared table. `Err` carries the out-of-shmem `ereport(ERROR)`.
    /// Scaffolded slot.
    pub fn injection_point_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `INJECTION_POINT(name, arg)` → `InjectionPointRun(name, arg)` — execute
    /// the injection point if one is attached. `arg` is the optional opaque
    /// string passed to the callback (the `void *arg`, modeled as an
    /// `Option<String>` for the points pgrust uses). When no point is attached
    /// this is a cheap registry check.
    pub fn injection_point_run(name: &str, arg: Option<&str>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `INJECTION_POINT_CACHED(name, arg)` → `InjectionPointCached(name, arg)`
    /// — run the injection point straight from the backend-local cache (used in
    /// code paths where no allocation may happen, after `INJECTION_POINT_LOAD`).
    pub fn injection_point_cached(name: &str, arg: Option<&str>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `INJECTION_POINT_LOAD(name)` → `InjectionPointLoad(name)` — pre-load the
    /// injection point into the backend-local cache.
    pub fn injection_point_load(name: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `IS_INJECTION_POINT_ATTACHED(name)` → `IsInjectionPointAttached(name)`
    /// — test (and cache) whether a point is currently attached.
    pub fn is_injection_point_attached(name: &str) -> types_error::PgResult<bool>
);
