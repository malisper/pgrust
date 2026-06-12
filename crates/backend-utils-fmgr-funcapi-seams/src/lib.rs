//! Seam declarations for the `backend-utils-fmgr-funcapi` unit
//! (`utils/fmgr/funcapi.c`): the materialized set-returning-function
//! plumbing. The `FunctionCallInfo` / `ReturnSetInfo` shapes are owned by the
//! fmgr layer; until it lands they cross these seams as the opaque handles in
//! `types_core::fmgr`.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `InitMaterializedSRF(fcinfo, flags)` (funcapi.c) — set up the calling
    /// function's materialize-mode tuplestore and descriptor in
    /// `fcinfo->resultinfo`. In C it returns void; here it returns the handle
    /// by which subsequent rows reach that `ReturnSetInfo`. Can
    /// `ereport(ERROR)` (allocation, tupledesc lookup), carried on `Err`.
    pub fn InitMaterializedSRF(
        fcinfo: types_core::fmgr::FunctionCallInfoHandle,
        flags: u32,
    ) -> types_error::PgResult<types_core::fmgr::MaterializedSrfHandle>
);

seam_core::seam!(
    /// The C call `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
    /// values, nulls)` against an `InitMaterializedSRF`-prepared
    /// `ReturnSetInfo`: resolving `setResult`/`setDesc` from the handle is
    /// funcapi-owned; the append delegates to tuplestore. Can
    /// `ereport(ERROR)` (palloc inside tuple forming/append), carried on
    /// `Err`.
    pub fn materialized_srf_putvalues(
        srf: types_core::fmgr::MaterializedSrfHandle,
        values: Vec<types_datum::Datum>,
        nulls: Vec<bool>,
    ) -> types_error::PgResult<()>
);
