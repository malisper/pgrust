use mcx::{Mcx, PgVec};
use types_error::PgResult;

// Owner: the future backend-access-common-detoast unit (detoast.c). Input is
// the full varlena image bytes (external TOAST pointer or 4B-compressed);
// output is the detoasted plain 4B-header image, charged to `mcx`. Uninstalled
// call = loud panic — never a silent slow path (AGENTS.md rule 5).

seam_core::seam!(
    pub fn detoast_attr<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    // C detoast_attr_slice: sliceoffset >= 0; slicelength < 0 = to the end.
    pub fn detoast_attr_slice<'mcx>(
        mcx: Mcx<'mcx>,
        image: &[u8],
        sliceoffset: i32,
        slicelength: i32,
    ) -> PgResult<PgVec<'mcx, u8>>
);

// Owner: heaptoast (heaptoast.c toast_flatten_tuple_to_datum). Hosted here so
// heaptuple's heap_copy_tuple_as_datum can reach it without a crate cycle.
seam_core::seam!(
    pub fn toast_flatten_tuple_to_datum<'mcx>(
        mcx: Mcx<'mcx>,
        tup: &types_tuple::HeapTupleData<'_>,
        tuple_desc: &types_tuple::TupleDescData<'_>,
    ) -> PgResult<datum::Datum>
);
