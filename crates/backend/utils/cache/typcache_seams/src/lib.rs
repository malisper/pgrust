use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::TupleDescData;

seam_core::seam!(
    pub fn at_eoxact_type_cache()
);

seam_core::seam!(
    pub fn at_eosubxact_type_cache()
);

seam_core::seam!(
    // lookup_rowtype_tupdesc_copy(type_id, typmod) (typcache.c) — a
    // freestanding copy the caller owns and may re-stamp.
    pub fn lookup_rowtype_tupdesc_copy<'mcx>(
        mcx: Mcx<'mcx>,
        type_id: Oid,
        typmod: i32,
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    // assign_record_type_typmod(tupDesc) (typcache.c) — registers the rowtype
    // and stamps tdtypmod.
    pub fn assign_record_type_typmod(tupdesc: &mut TupleDescData<'_>) -> PgResult<()>
);
