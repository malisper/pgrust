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
