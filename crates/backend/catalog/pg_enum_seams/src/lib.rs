use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn at_eoxact_enum()
);

seam_core::seam!(
    // EnumUncommitted (pg_enum.c) — enum.c check_safe_enum_use consumer.
    pub fn enum_uncommitted(enum_id: Oid) -> bool
);

seam_core::seam!(
    // pg_enum member (oid, enumsortorder) pairs — typcache
    // load_enum_cache_data consumer.
    pub fn scan_enum_members<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        enum_type_id: Oid,
    ) -> PgResult<mcx::PgVec<'mcx, (Oid, f32)>>
);

// One pg_enum row in enumsortorder order plus the tuple-header xmin facts
// check_safe_enum_use reads — enum.c enum_endpoint/enum_range consumer.
pub struct EnumSortedRow {
    pub oid: Oid,
    pub enumtypid: Oid,
    pub enumlabel: types_tuple::NameData,
    pub xmin: types_core::TransactionId,
    pub xmin_committed: bool,
}

seam_core::seam!(
    // Ordered EnumTypIdSortOrderIndexId scan; backward flips the direction,
    // limit_one stops after the first row (enum_first/enum_last).
    pub fn scan_enum_typid_sorted<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        enumtypoid: Oid,
        backward: bool,
        limit_one: bool,
    ) -> PgResult<mcx::PgVec<'mcx, EnumSortedRow>>
);
