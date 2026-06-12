//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;

seam_core::seam!(
    /// `GetUserIdAndSecContext(&userid, &sec_context)`.
    pub fn get_user_id_and_sec_context() -> (Oid, i32)
);

seam_core::seam!(
    /// `SetUserIdAndSecContext(userid, sec_context)`.
    pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32)
);

seam_core::seam!(
    /// `GetUserId()`.
    pub fn get_user_id() -> Oid
);
