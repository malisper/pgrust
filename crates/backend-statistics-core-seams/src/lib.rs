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
    /// `lookup_type_cache(...)->lt_opr` over the `VacAttrStats` matrix carried by
    /// the (now real) `StatsBuildData`. It can `elog(ERROR, "cache lookup failed
    /// for ordering operator ...")`, so the failure surface is carried on `Err`.
    ///
    /// `dependency` is the array of `k` zero-based column indexes into the
    /// statistics object (NOT yet translated to attnums; the owner translates
    /// via `data->attnums[dependency[i]]`).
    pub fn dependency_degree<'mcx>(
        data: &types_statistics::StatsBuildData<'mcx>,
        k: i32,
        dependency: &[types_core::AttrNumber],
    ) -> types_error::PgResult<f64>
);

/* ===========================================================================
 * MCV-list seams (consumed by `backend-statistics-mcv`, the
 * most-common-value slice of the combined `backend-statistics-core` unit).
 *
 * The MCV byte-layout serialize/deserialize and the selectivity arithmetic
 * are ported IN `backend-statistics-mcv`; the pieces that touch the unported
 * build framework (`StatsBuildData` / `VacAttrStats` / the multi-sort support),
 * the per-dimension `Datum`<->bytes value codec, the type-cache ordering-operator
 * lookup, the `pg_statistic_ext_data` syscache, the planner-arena clause
 * introspection, the per-clause fmgr operator dispatch and the SRF / type-I/O
 * fmgr surface cross these seams. The owner installs them when it lands; until
 * then a call panics loudly (mirror-pg-and-panic).
 * ========================================================================= */

seam_core::seam!(
    /// `statext_mcv_build(StatsBuildData *data, double totalrows, int stattarget)`
    /// (mcv.c:179) — build an MCV list from the sampled rows.
    ///
    /// SEAMED, not in-crate: the body needs `build_mss` / `build_sorted_items` /
    /// `build_distinct_groups` / `build_column_frequencies` over the opaque
    /// `StatsBuildData` (the `VacAttrStats` matrix + `Datum`/`bool` value
    /// matrices) plus per-column `lookup_type_cache(...)->lt_opr` and the
    /// multi-sort comparator machinery — all in the not-yet-ported extended-stats
    /// build framework. Returns `None` when nothing was built (C `NULL`).
    pub fn statext_mcv_build<'mcx>(
        data: &types_statistics::StatsBuildData<'mcx>,
        totalrows: f64,
        stattarget: i32,
    ) -> types_error::PgResult<Option<types_statistics::MCVList>>
);

seam_core::seam!(
    /// `statext_mcv_load(Oid mvoid, bool inh)` (mcv.c:557) — read the serialized
    /// MCV bytea for a `pg_statistic_ext_data` tuple from the syscache.
    ///
    /// SEAMED: the `SearchSysCache2(STATEXTDATASTXOID, ...)` /
    /// `SysCacheGetAttr(Anum_pg_statistic_ext_data_stxdmcv)` lookup is the
    /// unported pg_statistic_ext_data syscache layer; it can `elog(ERROR)` for a
    /// missing object or an un-built MCV kind, carried on `Err`. The returned
    /// bytea (varlena framing included) is deserialized in-crate.
    pub fn mcv_load_bytea<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mvoid: types_core::Oid,
        inh: bool,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `lookup_type_cache(typid, TYPECACHE_LT_OPR)->lt_opr` — the ordering
    /// operator for a type (mcv.c:361/664). Returns `InvalidOid` (0) when the
    /// type has no '<' operator; can `elog(ERROR)` on a cache failure.
    pub fn mcv_lookup_lt_opr(
        attrtypid: types_core::Oid,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `compare_scalars_simple(a, b, ssup)` (extended_stats.c) for a single
    /// dimension's `(lt_opr, collation)`. Three-way `< 0 / 0 / > 0`, used to sort
    /// and binary-search the deduplicated per-dimension value arrays during MCV
    /// serialization.
    ///
    /// SEAMED: needs `PrepareSortSupportFromOrderingOp` + the fmgr comparison
    /// dispatch over the by-value/by-ref `Datum`, all owner-side.
    pub fn mcv_compare_scalars_simple(
        a: types_datum::Datum,
        b: types_datum::Datum,
        lt_opr: types_core::Oid,
        collation: types_core::Oid,
    ) -> i32
);

seam_core::seam!(
    /// Serialize one MCV value into its on-wire payload bytes for the given type
    /// `(typlen, typbyval)`, mirroring the per-category bodies of
    /// `statext_mcv_serialize` (mcv.c:868-919):
    ///   * by-value  -> `store_att_byval` then the `typlen` significant bytes;
    ///   * fixed by-ref (`typlen > 0`) -> the `typlen` bytes at the pointer;
    ///   * varlena (`typlen == -1`) -> the detoasted `VARSIZE_ANY_EXHDR` body
    ///     (NO length prefix — the caller prepends the uint32 length);
    ///   * cstring (`typlen == -2`) -> the NUL-terminated bytes incl. terminator
    ///     (NO length prefix — the caller prepends the uint32 length).
    ///
    /// SEAMED: `store_att_byval` / `PG_DETOAST_DATUM` / `DatumGetCString` are the
    /// project-wide-deferred `Datum`-value codec.
    pub fn mcv_value_to_serialized_bytes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: types_datum::Datum,
        typlen: i16,
        typbyval: bool,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// Reconstruct an MCV value `Datum` from its on-wire payload bytes for the
    /// given type `(typlen, typbyval)`, mirroring the per-category bodies of
    /// `statext_mcv_deserialize` (mcv.c:1186-1259):
    ///   * by-value  -> `fetch_att(&v, true, typlen)` over the `typlen` bytes;
    ///   * fixed by-ref (`typlen > 0`) -> a `PointerGetDatum` over a fresh copy;
    ///   * varlena (`typlen == -1`) -> a full-header varlena built from the body;
    ///   * cstring (`typlen == -2`) -> a `PointerGetDatum` over a copy.
    ///
    /// The returned `Datum` owns its backing storage in `mcx` (the deserialized
    /// MCV list's single chunk in C).
    ///
    /// SEAMED: `fetch_att` / `SET_VARSIZE` / `PointerGetDatum` are the
    /// project-wide-deferred `Datum`-value codec.
    pub fn mcv_serialized_bytes_to_value<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        bytes: &[u8],
        typlen: i16,
        typbyval: bool,
    ) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `mcv_get_match_bitmap(root, clauses, keys, exprs, mcvlist, is_or)`
    /// (mcv.c:1598) — evaluate the clause list against the MCV list and return a
    /// per-item match bitmap (`Vec<bool>` of length `mcvlist->nitems`).
    ///
    /// SEAMED: the body walks planner `Node` clauses (`OpExpr` / `NullTest` /
    /// `ScalarArrayOpExpr` / AND/OR/NOT / boolean `Var` / bare bool expr) over
    /// the planner arena — `is_opclause` / `examine_opclause_args` /
    /// `mcv_match_expression` / `bms_member_index` / `deconstruct_array` — and
    /// dispatches the per-clause fmgr operator (`FunctionCall2Coll`) and
    /// `DatumGetBool`. None of those node/fmgr surfaces is ported; `clauses` /
    /// `keys` / `exprs` are opaque planner-arena ids the owner resolves.
    pub fn mcv_get_match_bitmap(
        root_id: u64,
        clauses_id: u64,
        keys_id: u64,
        exprs_id: u64,
        mcvlist: &types_statistics::MCVList,
        is_or: bool,
    ) -> types_error::PgResult<std::vec::Vec<bool>>
);

seam_core::seam!(
    /// `RangeTblEntry *rte = root->simple_rte_array[rel->relid]; rte->inh`
    /// (mcv.c:2057) — the `inh` flag the MCV load is keyed on. SEAMED: reads the
    /// planner `PlannerInfo`/`RelOptInfo` arena.
    pub fn mcv_rte_inh_for_rel(root_id: u64, rel_id: u64) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `pg_stats_ext_mcvlist_items(fcinfo)` (mcv.c:1337) — the SRF returning the
    /// per-item details. SEAMED: pure SRF / fmgr / tupdesc / array-builder /
    /// type-output dispatch (`get_call_result_type` / `accumArrayResult` /
    /// `getTypeOutputInfo` / `heap_form_tuple` / `SRF_RETURN_*`), all over the
    /// project-wide-deferred `Datum` fmgr surface.
    pub fn pg_stats_ext_mcvlist_items(fcinfo_id: u64) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `pg_mcv_list_out(fcinfo)` (mcv.c:1497) — `return byteaout(fcinfo)`.
    /// SEAMED: the `byteaout` fmgr dispatch over the opaque `FunctionCallInfo`.
    pub fn pg_mcv_list_out(fcinfo_id: u64) -> types_error::PgResult<types_datum::Datum>
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
    pub fn ndistinct_for_combination<'mcx>(
        totalrows: f64,
        data: &types_statistics::StatsBuildData<'mcx>,
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
