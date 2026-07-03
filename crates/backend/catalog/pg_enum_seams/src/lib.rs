use types_core::Oid;

seam_core::seam!(
    pub fn at_eoxact_enum()
);

seam_core::seam!(
    // EnumUncommitted (pg_enum.c) — enum.c check_safe_enum_use consumer.
    pub fn enum_uncommitted(enum_id: Oid) -> bool
);
