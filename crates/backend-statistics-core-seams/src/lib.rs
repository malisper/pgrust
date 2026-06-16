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

seam_core::seam!(
    /// `ndistinct_for_combination(double totalrows, StatsBuildData *data, int k,
    /// int *combination)` (mvdistinct.c:424-517) — the Duj1 n-distinct estimator
    /// for one column combination.
    ///
    /// SEAMED, not in-crate: its body builds the per-row `values[]`/`isnull[]`
    /// sort buffer from `data->values[combination[i]][j]` /
    /// `data->nulls[combination[i]][j]`, sets up `multi_sort_init` /
    /// `multi_sort_add_dimension` using each column's
    /// `lookup_type_cache(colstat->attrtypid, TYPECACHE_LT_OPR)->lt_opr` (with
    /// `colstat->attrcollid`), `qsort_interruptible`s it with `multi_sort_compare`,
    /// and counts distinct combinations. All of `multi_sort_*` + the per-column
    /// `VacAttrStats` matrix live inside the opaque `StatsBuildData`, owned by the
    /// not-yet-ported `extended_stats.c` + multi-sort support. It can
    /// `elog(ERROR, "cache lookup failed for ordering operator ...")`, so the
    /// failure surface is carried on `Err`.
    ///
    /// `combination` is the array of `k` zero-based column indexes into the
    /// statistics object (NOT yet translated to attnums).
    pub fn ndistinct_for_combination(
        totalrows: f64,
        data: types_statistics::StatsBuildDataHandle,
        k: i32,
        combination: &[i32],
    ) -> types_error::PgResult<f64>
);

seam_core::seam!(
    /// The `pg_statistic_ext_data` syscache read of `statext_ndistinct_load`
    /// (mvdistinct.c:147-172): `SearchSysCache2(STATEXTDATASTXOID, mvoid, inh)` +
    /// `SysCacheGetAttr(..., Anum_pg_statistic_ext_data_stxdndistinct, &isnull)` +
    /// `ReleaseSysCache`.
    ///
    /// SEAMED, not in-crate: the syscache lives in the not-yet-ported relcache /
    /// syscache subsystem. The owner returns the detoasted `stxdndistinct` bytea
    /// body (`DatumGetByteaPP`) as `Ok(Some(bytes))`, the is-null-attribute case
    /// as `Ok(None)` (so the kind-not-built error text / behaviour stays in-crate),
    /// and the missing-tuple `elog(ERROR, "cache lookup failed for statistics
    /// object %u")` case as `Err`.
    pub fn statext_ndistinct_load_bytea(
        mvoid: types_core::Oid,
        inh: bool,
    ) -> types_error::PgResult<Option<std::vec::Vec<u8>>>
);
