//! Seam declaration for `superuser_arg` (`utils/misc/superuser.c`), reached by
//! `miscinit.c` `has_rolreplication`. Calls panic until the owner lands.

seam_core::seam!(
    /// `superuser_arg(roleid)` (`utils/misc/superuser.c`) — does the specified
    /// role have superuser privilege? Reads `pg_authid.rolsuper` via the
    /// syscache (with the `!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID`
    /// escape); `Err` includes syscache lookup failure.
    pub fn superuser_arg(roleid: types_core::Oid) -> types_error::PgResult<bool>
);
