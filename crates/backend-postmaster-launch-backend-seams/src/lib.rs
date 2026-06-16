//! Seam declarations for the `backend-postmaster-launch-backend` unit
//! (`postmaster/launch_backend.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `postmaster_child_launch(child_type, child_slot, startup_data,
    /// startup_data_len, client_sock)` (`launch_backend.c`) — fork (and on
    /// EXEC_BACKEND, exec) a postmaster child of the given type. Returns the
    /// child pid in the postmaster, `-1` on fork failure (errno set); in the
    /// child it runs the child main entry and never returns. The C
    /// `client_sock` argument is `NULL` for non-backend children and is not
    /// carried; extend the declaration when a backend-launching consumer
    /// lands.
    pub fn postmaster_child_launch(
        child_type: types_core::init::BackendType,
        child_slot: i32,
        startup_data: &[u8]
    ) -> i32
);

seam_core::seam!(
    /// `PostmasterChildName(child_type)` (`launch_backend.c`) — the human-readable
    /// name of a postmaster child kind, used in postmaster/pmchild log messages.
    /// Pure lookup into the static child-process-kinds table; cannot fail.
    pub fn postmaster_child_name(child_type: types_core::init::BackendType) -> &'static str
);
