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

seam_core::seam!(
    pub fn heap_toast_delete(
        mcx: mcx::Mcx<'_>,
        rel: &RelationData<'_>,
        oldtup: &HeapTupleData<'_>,
        is_speculative: bool,
    ) -> PgResult<()>
);
