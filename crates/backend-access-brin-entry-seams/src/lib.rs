//! Seam declarations for the `backend-access-brin-entry` unit (`brin.c` and the
//! built-in BRIN opclasses `brin_bloom.c` / `brin_minmax_multi.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// The opclass `bv_serialize` callback (`brin_serialize_callback_type`,
    /// `brin_tuple.h`): `void (*)(BrinDesc *bdesc, Datum src, Datum *dst)`,
    /// dispatched for indexed column `keyno` (0-based). It serializes the
    /// in-memory expanded value `mem_value` (`bv_mem_value`) into the column's
    /// `values` slice (`bv_values`, length `oi_nstored`), allocating any
    /// by-reference output in `mcx`. Owned by the opclass (e.g. `brin_bloom`,
    /// `brin_minmax_multi`); `Err` carries its `ereport(ERROR)` surface and OOM.
    pub fn brin_serialize<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        keyno: usize,
        mem_value: &types_tuple::backend_access_common_heaptuple::Datum<'_>,
        values: &mut [types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_getprocinfo(idxRel, keyno+1, BRIN_PROCNUM_OPCINFO)` +
    /// `FunctionCall1(opcInfoFn, atttypid)` (brin.c `brin_build_desc`): invoke
    /// indexed column `keyno` (0-based) opclass' `OpcInfo` support procedure,
    /// returning the `BrinOpcInfo` describing that column's on-disk layout. The
    /// opclass (`brin_minmax` / `brin_inclusion` / `brin_bloom` /
    /// `brin_minmax_multi`) owns the procedure; until those land a call panics.
    /// `Err` carries its `ereport(ERROR)` surface and OOM.
    pub fn brin_opcinfo<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
        keyno: usize,
        atttypid: types_core::primitive::Oid,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_brin::BrinOpcInfo<'mcx>>>
);

seam_core::seam!(
    /// Whether indexed column `attno` (0-based) opclass' `Consistent` support
    /// procedure accepts the multi-key form (`consistentFn->fn_nargs >= 4`,
    /// brin.c `bringetbitmap`). Determines whether [`brin_consistent_multi`] or
    /// per-key [`brin_consistent_single`] dispatch is used. Owned by the
    /// opclass; panics until it lands.
    pub fn brin_consistent_is_multi(
        index: &types_rel::Relation<'_>,
        attno: usize,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `FunctionCall4Coll(consistentFn, collation, bdesc, bval, keys, nkeys)`
    /// (brin.c `bringetbitmap`): the multi-key opclass `Consistent` call — does
    /// the page range described by `bval` possibly match all of `keys`? Owned by
    /// the opclass; panics until it lands. `Err` carries its `ereport(ERROR)`.
    pub fn brin_consistent_multi<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
        attno: usize,
        collation: types_core::primitive::Oid,
        bdesc: &types_brin::BrinDesc<'mcx>,
        bval: &types_brin::BrinValues<'mcx>,
        keys: &[types_scan::scankey::ScanKeyData<'mcx>],
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `FunctionCall3Coll(consistentFn, collation, bdesc, bval, key)` (brin.c
    /// `bringetbitmap`): the single-key opclass `Consistent` call — does the
    /// page range described by `bval` possibly match `key`? Owned by the
    /// opclass; panics until it lands. `Err` carries its `ereport(ERROR)`.
    pub fn brin_consistent_single<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
        attno: usize,
        collation: types_core::primitive::Oid,
        bdesc: &types_brin::BrinDesc<'mcx>,
        bval: &types_brin::BrinValues<'mcx>,
        key: &types_scan::scankey::ScanKeyData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `FunctionCall4Coll(addValue, collation, bdesc, bval, value, isnull)`
    /// (brin.c `add_values_to_range`): the opclass `BRIN_PROCNUM_ADDVALUE`
    /// support procedure — incorporate a new heap value into the range summary
    /// `bval` for indexed column `attno` (0-based). The procedure may modify
    /// `bval` in place (its by-reference output rides `mcx`); returns whether
    /// the summary tuple changed (the C `DatumGetBool(result)`). Owned by the
    /// opclass (`brin_minmax` / `brin_inclusion` / `brin_bloom` /
    /// `brin_minmax_multi`); panics until it lands. `Err` carries its
    /// `ereport(ERROR)`.
    pub fn brin_addvalue<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
        attno: usize,
        collation: types_core::primitive::Oid,
        bdesc: &types_brin::BrinDesc<'mcx>,
        bval: &mut types_brin::BrinValues<'mcx>,
        value: &types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        isnull: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `FunctionCall3Coll(unionFn, collation, bdesc, col_a, col_b)` (brin.c
    /// `union_tuples`): the opclass `BRIN_PROCNUM_UNION` support procedure —
    /// merge the summary of column `attno` (0-based) so that `col_a` becomes
    /// consistent with both `col_a` and `col_b`. The procedure modifies `col_a`
    /// in place (its by-reference output rides `mcx`). Owned by the opclass;
    /// panics until it lands. `Err` carries its `ereport(ERROR)`.
    pub fn brin_union<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
        attno: usize,
        collation: types_core::primitive::Oid,
        bdesc: &types_brin::BrinDesc<'mcx>,
        col_a: &mut types_brin::BrinValues<'mcx>,
        col_b: &types_brin::BrinValues<'mcx>,
    ) -> types_error::PgResult<()>
);
