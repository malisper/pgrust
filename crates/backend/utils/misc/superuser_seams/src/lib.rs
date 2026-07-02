seam_core::seam!(
    // superuser() (utils/misc/superuser.c): pg_authid rolsuper of GetUserId().
    pub fn superuser() -> types_error::PgResult<bool>
);

seam_core::seam!(
    // superuser_arg(roleid) (utils/misc/superuser.c).
    pub fn superuser_arg(roleid: types_core::Oid) -> types_error::PgResult<bool>
);
