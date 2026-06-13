//! Seam declarations for the `backend-utils-fmgr-funcapi` unit
//! (`utils/fmgr/funcapi.c`): the materialized set-returning-function
//! plumbing, over the owned `FunctionCallInfoBaseData` / `ReturnSetInfo`
//! shapes in `types_nodes`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `InitMaterializedSRF(fcinfo, flags)` (funcapi.c) — set up the calling
    /// function's materialize-mode tuplestore and descriptor in the
    /// `ReturnSetInfo` at `fcinfo->resultinfo` (`setResult`/`setDesc`). Can
    /// `ereport(ERROR)` (allocation, tupledesc lookup), carried on `Err`.
    pub fn InitMaterializedSRF<'mcx>(
        fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
        flags: u32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The C call `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
    /// values, nulls)` against an `InitMaterializedSRF`-prepared
    /// `ReturnSetInfo`: resolving `setResult`/`setDesc` is funcapi-owned; the
    /// append delegates to tuplestore. `values`/`nulls` mirror the C stack
    /// arrays (borrowed, no allocation at the call site). Can
    /// `ereport(ERROR)` (palloc inside tuple forming/append), carried on
    /// `Err`.
    pub fn materialized_srf_putvalues<'mcx>(
        rsinfo: &mut types_nodes::funcapi::ReturnSetInfo<'mcx>,
        values: &[types_datum::Datum],
        nulls: &[bool],
    ) -> types_error::PgResult<()>
);
