seam_core::seam!(
    pub fn at_eoxact_combocid()
);

seam_core::seam!(
    // HeapTupleHeaderGetCmin (combocid.c): Assert-only, no ereport.
    pub fn heap_tuple_header_get_cmin<'a>(
        tup: &'a types_tuple::HeapTupleHeaderData,
    ) -> types_core::CommandId
);

seam_core::seam!(
    pub fn heap_tuple_header_get_cmax<'a>(
        tup: &'a types_tuple::HeapTupleHeaderData,
    ) -> types_core::CommandId
);

seam_core::seam!(
    // HeapTupleHeaderAdjustCmax (combocid.c): returns (cmax-to-store, iscombo).
    pub fn heap_tuple_header_adjust_cmax<'a>(
        tup: &'a types_tuple::HeapTupleHeaderData,
        cid: types_core::CommandId,
    ) -> types_error::PgResult<(types_core::CommandId, bool)>
);
