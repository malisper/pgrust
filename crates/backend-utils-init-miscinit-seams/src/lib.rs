//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`): user-id / security-context state and role-name
//! lookup.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `GetUserIdAndSecContext(&userid, &sec_context)` (miscinit.c): the
    /// current user ID and security-context bitmask. Reads backend-local
    /// state; infallible.
    pub fn get_user_id_and_sec_context() -> (Oid, i32)
);

seam_core::seam!(
    /// `SetUserIdAndSecContext(userid, sec_context)` (miscinit.c): install a
    /// new current user ID and security-context bitmask. Writes
    /// backend-local state; infallible.
    pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32)
);

seam_core::seam!(
    /// `GetUserNameFromId(roleid, noerr)` (miscinit.c): the role name for
    /// `roleid`, copied into `mcx` (C: `pstrdup` in the current context).
    /// With `noerr = false` a missing role raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `noerr = true` it is
    /// `Ok(None)`. `Err` includes OOM from the copy and syscache lookup
    /// errors.
    pub fn get_user_name_from_id<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        noerr: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);
