//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `GetUserId()` (miscinit.c): the current user ID (the backend-global
    /// `CurrentUserId`). Infallible (the C `Assert(OidIsValid(...))` is the
    /// owner's debug assertion).
    pub fn get_user_id() -> types_core::primitive::Oid
);
