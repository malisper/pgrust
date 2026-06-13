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
    /// `check_enable_rls(relid, checkAsUser, noError)` (`rls.c`) — decide
    /// whether row-level security applies to `relid` for the given (or
    /// current) user. Can `ereport(ERROR, ERRCODE_INSUFFICIENT_PRIVILEGE)`
    /// when `noError` is false and `row_security` is off (and via the syscache
    /// lookups it performs), carried on `Err`.
    pub fn check_enable_rls(
        relid: types_core::Oid,
        check_as_user: types_core::Oid,
        no_error: bool,
    ) -> types_error::PgResult<types_acl::CheckEnableRlsResult>
);
