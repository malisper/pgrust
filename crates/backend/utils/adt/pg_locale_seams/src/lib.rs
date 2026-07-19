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

seam_core::seam!(
    // pg_perm_setlocale(category, locale) (pg_locale.c); category is the POSIX
    // LC_* value; Ok(None) is C's NULL return (setlocale failure).
    pub fn pg_perm_setlocale<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        category: i32,
        locale: &str,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    // database_ctype_is_c = v (pg_locale.c global; postinit writes it once).
    pub fn set_database_ctype_is_c(v: bool)
);

seam_core::seam!(
    // init_database_collation (pg_locale.c).
    pub fn init_database_collation() -> PgResult<()>
);

seam_core::seam!(
    // Nondeterministic-collation hashing (hashfunc.c hashtext*/varchar.c
    // hashbpchar*): hash_any over the pg_strnxfrm sort key including its NUL
    // terminator (C hashes bsize+1). Ok(None) = deterministic collation, the
    // caller hashes the raw bytes. seed None = 32-bit hash zero-extended.
    pub fn varstr_nondeterministic_hash(
        collid: Oid,
        data: &[u8],
        seed: Option<u64>,
    ) -> PgResult<Option<u64>>
);

seam_core::seam!(
    // get_collation_actual_version(collprovider, locale) (pg_locale.c);
    // Ok(None) is C's NULL (no version for this provider).
    pub fn get_collation_actual_version<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        collprovider: u8,
        locale: &str,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    // The Unicode version of the loaded ICU library — pgrust's analog of C's
    // compile-time U_UNICODE_VERSION (icu_unicode_version, varlena.c). None =
    // libicu not loadable, matching a C build without --with-icu.
    pub fn icu_unicode_version() -> Option<&'static str>
);
