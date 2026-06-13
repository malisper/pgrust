//! Seam declarations for the `backend-postmaster-postmaster` unit
//! (`src/backend/postmaster/postmaster.c`). The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `ClosePostmasterPorts(am_syslogger)` (`postmaster.c`): in a child
    /// process, close the postmaster's listen sockets and other
    /// postmaster-only file descriptors.
    pub fn close_postmaster_ports(am_syslogger: bool)
);
