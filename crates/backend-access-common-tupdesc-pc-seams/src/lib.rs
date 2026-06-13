//! plancache's slice of tuple-descriptor helpers (`access/common/tupdesc.c`).
//! The owning unit installs these; until then a call panics loudly.

use types_error::PgResult;
use types_plancache::TupleDescHandle;

seam_core::seam!(
    /// `CreateTupleDescCopy(src)` in the current context.
    pub fn create_tuple_desc_copy(src: TupleDescHandle) -> PgResult<TupleDescHandle>
);

seam_core::seam!(
    /// `FreeTupleDesc(desc)`.
    pub fn free_tuple_desc(desc: TupleDescHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `equalRowTypes(a, b)`.
    pub fn equal_row_types(a: TupleDescHandle, b: TupleDescHandle) -> PgResult<bool>
);
