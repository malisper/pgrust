//! Seam declarations for the `backend-postmaster-syslogger` unit
//! (`postmaster/syslogger.c`) plus the syslogger pipe-chunk writer.
//!
//! `write_pipe_chunks` is textually defined in `elog.c`, but its
//! `PipeProtoChunk` layout and chunk protocol belong to `syslogger.h` and pair
//! with syslogger.c's chunk reassembly, so this project treats the syslogger
//! unit as its owner (a sanctioned adaptation recorded in the
//! `backend-utils-error` crate docs).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Both calls are unreachable under the boot
//! defaults (`redirection_done = false`, not the syslogger process).

seam_core::seam!(
    /// `write_pipe_chunks(data, len, dest)` (`elog.c`, protocol owned by
    /// `postmaster/syslogger.h`) — send `data` to the syslogger over the
    /// stderr pipe using the chunked `PipeProtoChunk` protocol. `dest` is a
    /// `LOG_DESTINATION_*` code. Write failures are deliberately ignored in C.
    pub fn write_pipe_chunks(data: &[u8], dest: i32)
);

seam_core::seam!(
    /// `write_syslogger_file(buffer, count, destination)`
    /// (`postmaster/syslogger.c`) — in the syslogger process, write directly
    /// to the current log file. `dest` is a `LOG_DESTINATION_*` code.
    pub fn write_syslogger_file(data: &[u8], dest: i32)
);
