//! Seam declarations for the `backend-postmaster-syslogger` unit
//! (`postmaster/syslogger.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The call is unreachable under the boot defaults
//! (not the syslogger process).

seam_core::seam!(
    /// `write_syslogger_file(buffer, count, destination)`
    /// (`postmaster/syslogger.c`) — in the syslogger process, write directly
    /// to the current log file. `dest` is a `LOG_DESTINATION_*` code.
    pub fn write_syslogger_file(data: &[u8], dest: i32)
);
