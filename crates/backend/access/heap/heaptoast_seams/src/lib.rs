use heaptuple::HeapTuple;
use types_error::PgResult;
use types_rel::RelationData;
use types_tuple::HeapTupleData;

// Cycle: heaptoast calls heap_insert back into heapam; None = C "return newtup".
seam_core::seam!(
    pub fn heap_toast_insert_or_update<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &RelationData<'_>,
        newtup: &HeapTupleData<'_>,
        oldtup: Option<&HeapTupleData<'_>>,
        options: i32,
    ) -> PgResult<Option<HeapTuple<'mcx>>>
);

// Cycle: brin_tuple's TOAST_INDEX_HACK compresses oversized stored values
// while heaptoast's own insert path calls back through indexam.
seam_core::seam!(
    pub fn toast_compress_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: &[u8],
        cmethod: i8,
    ) -> PgResult<Option<mcx::PgVec<'mcx, u8>>>
);

seam_core::seam!(
    pub fn toast_flatten_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tup: &HeapTupleData<'_>,
        tuple_desc: &types_tuple::TupleDescData<'_>,
    ) -> PgResult<HeapTuple<'mcx>>
);

seam_core::seam!(
    pub fn heap_toast_delete(
        mcx: mcx::Mcx<'_>,
        rel: &RelationData<'_>,
        oldtup: &HeapTupleData<'_>,
        is_speculative: bool,
    ) -> PgResult<()>
);
