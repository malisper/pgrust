//! Seam declarations for `backend-utils-adt-pg-locale-icu`
//! (`utils/adt/pg_locale_icu.c`).
//!
//! `pg_locale.c` calls `create_pg_locale_icu` when resolving an ICU-provider
//! collation; a direct dependency would cycle once `pg_locale.c` lands, so the
//! call crosses here. The owning crate installs the implementation from its
//! `init_seams()`.

seam_core::seam!(
    /// `create_pg_locale_icu(collid, context)` (`pg_locale_icu.c:142`).
    ///
    /// In an ICU build this looks up the collation/database locale and
    /// allocates a `pg_locale_t` (backed by a `UCollator`) in `context`. In
    /// the ICU-disabled migration profile the only compiled branch is the
    /// `#else` (lines 211-218): it `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED)`s
    /// with "ICU is not supported in this build", so the result is always
    /// `Err`. The allocating signature is preserved for the ICU-enabled
    /// owner: `mcx` is the C `context`, the output carries `'mcx`.
    pub fn create_pg_locale_icu<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        collid: types_core::primitive::Oid,
    ) -> types_error::PgResult<locale::PgLocale<'mcx>>
);
