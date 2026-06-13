//! Seam declarations for the `backend-access-common-tupdesc` unit
//! (`access/common/tupdesc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_error::PgResult;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `CreateTupleDescCopy(tupdesc)` (tupdesc.c) — a flat copy of the
    /// descriptor (dropping constraints/defaults) into `mcx`. Allocates.
    pub fn create_tuple_desc_copy<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &TupleDescData<'mcx>,
    ) -> PgResult<TupleDescData<'mcx>>
);
