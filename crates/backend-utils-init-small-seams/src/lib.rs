//! Seam declarations for the `backend-utils-init-small` unit
//! (`utils/init/globals.c`, `utils/init/usercontext.c`): backend-global
//! variable reads.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `work_mem` (globals.c): the `work_mem` GUC — per-operation memory
    /// budget in kilobytes.
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// `MyProcPort` (globals.c): run `f` with mutable access to this
    /// backend's connection `Port`, or `None` when there is no client
    /// connection (`MyProcPort == NULL`). Callback shape per the seam rules:
    /// a seam must not hand out `&'static mut`.
    pub fn with_my_proc_port(f: &mut dyn FnMut(Option<&mut types_net::Port>))
);

seam_core::seam!(
    /// `ClientConnectionLost = value` (globals.c / miscadmin.h).
    pub fn set_client_connection_lost(value: bool)
);

seam_core::seam!(
    /// `InterruptPending = value` (globals.c / miscadmin.h).
    pub fn set_interrupt_pending(value: bool)
);

seam_core::seam!(
    /// `MaxConnections` (globals.c): the `max_connections` GUC.
    pub fn max_connections() -> i32
);
