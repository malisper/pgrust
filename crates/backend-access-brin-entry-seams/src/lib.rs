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
        mem_value: &types_tuple::backend_access_common_heaptuple::TupleValue<'_>,
        values: &mut [types_tuple::backend_access_common_heaptuple::TupleValue<'mcx>],
    ) -> types_error::PgResult<()>
);
