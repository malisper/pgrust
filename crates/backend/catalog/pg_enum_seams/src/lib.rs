use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn at_eoxact_enum()
);

seam_core::seam!(
    pub fn enum_uncommitted(enum_id: Oid) -> bool
);

seam_core::seam!(
    pub fn scan_enum_members<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        enum_type_id: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, (Oid, f32)>>
);

// pg_enum row + header xmin facts — enum.c endpoint/range consumer.
pub struct EnumSortedRow {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumlabel: types_tuple::NameData,
    pub xmin: types_core::TransactionId,
    pub xmin_committed: bool,
}

seam_core::seam!(
    pub fn scan_enum_typid_sorted<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        enumtypoid: Oid,
        backward: bool,
        limit_one: bool,
    ) -> PgResult<mcx::PgVec<'mcx, EnumSortedRow>>
);
