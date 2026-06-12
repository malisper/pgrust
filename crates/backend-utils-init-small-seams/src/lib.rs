//! Seam declarations for the `backend-utils-init-small` unit
//! (`src/backend/utils/init/globals.c`, `usercontext.c`) — accessors for the
//! per-process globals that `globals.c` owns. The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// Read `IsPostmasterEnvironment` (`globals.c`).
    pub fn is_postmaster_environment() -> bool
);

seam_core::seam!(
    /// Read `IsUnderPostmaster` (`globals.c`).
    pub fn is_under_postmaster() -> bool
);

seam_core::seam!(
    /// `MyPMChildSlot = child_slot` (`globals.c`): record the `PMChildFlags`
    /// array index reserved for this child process.
    pub fn set_my_pm_child_slot(child_slot: i32)
);

seam_core::seam!(
    /// `MyClientSocket = palloc(...); memcpy(...)` (`globals.c` global): store
    /// this child's inherited client socket.
    pub fn set_my_client_socket(client_sock: types_net::ClientSocket)
);
