//! Seam declarations for the `backend-postmaster-syslogger` unit
//! (`postmaster/syslogger.c`), installed by that crate's `init_seams()`.

seam_core::seam!(
    /// `write_syslogger_file(buffer, count, destination)`
    /// (`postmaster/syslogger.c`) — in the syslogger process, write directly
    /// to the current log file (the C `buffer`/`count` pair is the slice).
    /// `dest` is a `LOG_DESTINATION_*` code. Infallible in C (failures go to
    /// `write_stderr`, never `ereport`).
    pub fn write_syslogger_file(data: &[u8], dest: i32)
);
