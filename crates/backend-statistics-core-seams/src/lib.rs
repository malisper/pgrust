//! Seam declarations OWNED by the (not-yet-ported) combined unit
//! `backend-statistics-core` (covers `attribute_stats.c`, `dependencies.c`,
//! `extended_stats.c`, `mcv.c`, `mvdistinct.c`, `relation_stats.c`,
//! `stat_utils.c`).
//!
//! `backend-statistics-dependencies` (the functional-dependency slice of that
//! unit, ported first) CONSUMES these: the build-side validation kernel
//! (`dependency_degree`) reaches the per-column `VacAttrStats` / multi-sort
//! support / `build_sorted_items` machinery that lives in `extended_stats.c`
//! and the vacuum subsystem, none of which is ported. The owner installs these
//! from its own `init_seams()` when it lands; until then a call panics loudly
//! (mirror-pg-and-panic).
//!
//! This crate installs nothing — its owner is `todo` in CATALOG.tsv, so the
//! `every_declared_seam_is_installed_by_its_owner` guard exempts it.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `dependency_degree(StatsBuildData *data, int k, AttrNumber *dependency)`
    /// (dependencies.c:220-329) — the work-horse that validates one candidate
    /// functional dependency on the sampled data.
    ///
    /// SEAMED, not in-crate: its body needs `multi_sort_init` /
    /// `multi_sort_add_dimension` / `build_sorted_items` /
    /// `multi_sort_compare_dim(s)` (extended_stats.c) plus per-column
    /// `lookup_type_cache(...)->lt_opr` over the `VacAttrStats` matrix inside the
    /// opaque `StatsBuildData`. It can `elog(ERROR, "cache lookup failed for
    /// ordering operator ...")`, so the failure surface is carried on `Err`.
    ///
    /// `dependency` is the array of `k` zero-based column indexes into the
    /// statistics object (NOT yet translated to attnums; the owner translates
    /// via `data->attnums[dependency[i]]`).
    pub fn dependency_degree(
        data: types_statistics::StatsBuildDataHandle,
        k: i32,
        dependency: &[types_core::AttrNumber],
    ) -> types_error::PgResult<f64>
);
