//! Seam declarations for the `backend-access-common-tupdesc` unit
//! (`access/common/tupdesc.c`): the row-type structural hash/equality and
//! tuple-descriptor copy algorithms the typcache's record cache needs.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `hashRowType(tupdesc)` (tupdesc.c): the structural row-type hash used
    /// as the record-cache key. Pure computation over the descriptor; the
    /// owner's body cannot allocate, so this is infallible.
    pub fn hash_row_type(tupdesc: &TupleDescData<'_>) -> u32
);

seam_core::seam!(
    /// `equalRowTypes(tupdesc1, tupdesc2)` (tupdesc.c): structural equality of
    /// two row types (the record-cache match function). Pure computation.
    pub fn equal_row_types(a: &TupleDescData<'_>, b: &TupleDescData<'_>) -> bool
);

seam_core::seam!(
    /// `CreateTupleDescCopy(tupdesc)` (tupdesc.c): copy WITHOUT constraints or
    /// defaults, resetting the per-attribute constraint/default/identity/
    /// generated flags and re-deriving the compact attrs; the result is a
    /// non-refcounted descriptor allocated in `mcx`. `Err` carries OOM.
    pub fn create_tupledesc_copy<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &TupleDescData<'_>,
    ) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>>
);
