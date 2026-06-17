//! Seam declarations for the tablesample-method registry and its callbacks
//! (`src/backend/access/tablesample/tablesample.c`, `access/tsmapi.h`).
//!
//! These were previously mis-homed in `backend-executor-nodeSamplescan-seams`;
//! the true owner is `backend-access-tablesample-core`, which installs them
//! from its `init_seams()`. Moved here so the seam crate's stem matches its
//! owner. Both the executor (`ExecInitSampleScan`) and the parser
//! (`transformRangeTableSample`) reach the registry across a dependency cycle,
//! so the seam indirection is retained (this is a relocation, not a removal).

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use mcx::{Mcx, PgBox};
use types_core::primitive::uint32;
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_samplescan::{SampleScanState, TsmRoutine};

seam_core::seam!(
    /// `GetTsmRoutine(handlerOid)` (access/tablesample/tablesample.c) — resolve
    /// the tablesample method's `TsmRoutine` from its handler-function OID,
    /// charging the result to `mcx`. The C reads only the handler OID; both the
    /// executor (`ExecInitSampleScan`) and the parser
    /// (`transformRangeTableSample`, which needs the routine's `parameterTypes`
    /// / `repeatable_across_queries` before any node state exists) use it.
    pub fn get_tsm_routine_oid<'mcx>(
        mcx: Mcx<'mcx>,
        handler_oid: Oid,
    ) -> PgResult<PgBox<'mcx, TsmRoutine>>
);

seam_core::seam!(
    /// `tsm->InitSampleScan != NULL`.
    pub fn tsm_has_init_sample_scan<'mcx>(scanstate: &SampleScanState<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `tsm->InitSampleScan(scanstate, eflags)`.
    pub fn tsm_init_sample_scan<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        eflags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tsm->BeginSampleScan(scanstate, params, nparams, seed)`.
    pub fn tsm_begin_sample_scan<'mcx>(
        scanstate: &mut SampleScanState<'mcx>,
        params: &[Datum],
        seed: uint32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tsm->NextSampleBlock != NULL` (the `allow_sync` test).
    pub fn tsm_has_next_sample_block<'mcx>(scanstate: &SampleScanState<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `tsm->EndSampleScan != NULL`.
    pub fn tsm_has_end_sample_scan<'mcx>(node: &SampleScanState<'mcx>) -> PgResult<bool>
);

seam_core::seam!(
    /// `tsm->EndSampleScan(node)`.
    pub fn tsm_end_sample_scan<'mcx>(node: &mut SampleScanState<'mcx>) -> PgResult<()>
);
