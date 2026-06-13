//! Seam declarations for the `backend-postmaster-postmaster` unit
//! (`src/backend/postmaster/postmaster.c`). The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.


seam_core::seam!(
    /// `ClosePostmasterPorts(am_syslogger)` (`postmaster.c`): in a child
    /// process, close the postmaster's listen sockets and other
    /// postmaster-only file descriptors.
    pub fn close_postmaster_ports(am_syslogger: bool)
);

seam_core::seam!(
    /// `PreAuthDelay` (`postmaster.c` GUC) — seconds to sleep before
    /// authentication, a debugging aid. Pure read of backend-local GUC state.
    pub fn pre_auth_delay() -> i32
);

seam_core::seam!(
    /// `AuthenticationTimeout` (`postmaster.c` GUC) — seconds to wait for the
    /// startup packet / authentication exchange. Pure read.
    pub fn authentication_timeout() -> i32
);

seam_core::seam!(
    /// `log_hostname` (`postmaster.c` GUC) — whether to log/resolve the
    /// client's host name (vs. numeric address). Pure read.
    pub fn log_hostname() -> bool
);

// --- backend-utils-init-postinit consumers (postmaster.c) ---

seam_core::seam!(
    /// `ClientAuthInProgress` (postmaster.c global): read the flag.
    pub fn client_auth_in_progress() -> bool
);

seam_core::seam!(
    /// `ClientAuthInProgress = value` (postmaster.c global): set the flag that
    /// limits log-message visibility during authentication.
    pub fn set_client_auth_in_progress(value: bool)
);
