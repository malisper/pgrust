//! Seam declarations for the `backend-utils-misc-more` unit
//! (`utils/misc/ps_status.c`, `pg_controldata.c`, `rls.c`, `superuser.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `init_ps_display(fixed_part)` (`ps_status.c`) — set this process's ps
    /// title; `None` mirrors the C `NULL` (derive the fixed part from
    /// `MyBackendType`). Infallible in C (assert-only).
    pub fn init_ps_display(fixed_part: Option<&str>)
);

seam_core::seam!(
    /// `check_enable_rls(relid, checkAsUser, noError)` (`rls.c`): whether
    /// row-level security applies to `relid` for `check_as_user`
    /// (`InvalidOid` means the current user). With `no_error = true` a
    /// permission problem returns `RLS_ENABLED` instead of raising; with
    /// `no_error = false` it can `ereport(ERROR)`, carried on `Err`. Performs
    /// catalog/syscache lookups.
    pub fn check_enable_rls(
        relid: types_core::Oid,
        check_as_user: types_core::Oid,
        no_error: bool,
    ) -> types_error::PgResult<types_acl::CheckEnableRlsResult>
);
