//! Seam declarations for the `backend-utils-error-small` unit
//! (`utils/error/csvlog.c`, `utils/error/jsonlog.c`, `utils/error/assert.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Both calls are unreachable under the boot
//! default `log_destination = stderr`.

use ::types_error::PgError;

seam_core::seam!(
    /// `write_csvlog(edata)` (`utils/error/csvlog.c`) — format the error as a
    /// CSV log line and emit it (pipe chunks or syslogger file).
    pub fn write_csvlog(edata: &PgError)
);

seam_core::seam!(
    /// `write_jsonlog(edata)` (`utils/error/jsonlog.c`) — format the error as
    /// a JSON log line and emit it (pipe chunks or syslogger file).
    pub fn write_jsonlog(edata: &PgError)
);
