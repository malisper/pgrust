use types_core::Oid;
use types_error::PgResult;

// Owner: the future backend-utils-adt-pg-locale unit (pg_locale.c). Callers
// (varlena) resolve C_COLLATION_OID/POSIX_COLLATION_OID locally without
// crossing a seam; only genuinely locale-dependent collations arrive here.
// Uninstalled call = loud panic.

// Fused non-C-collation comparison (C: pg_newlocale_from_collation +
// collate_is_c/pg_strncoll/deterministic-tiebreak in varstr_cmp) — ONE
// crossing per comparison, mirroring C's single locale resolve.
seam_core::seam!(
    pub fn varstr_cmp_locale(collid: Oid, arg1: &[u8], arg2: &[u8]) -> PgResult<i32>
);

// C: pg_newlocale_from_collation(collid)->deterministic.
seam_core::seam!(
    pub fn collation_is_deterministic(collid: Oid) -> PgResult<bool>
);
