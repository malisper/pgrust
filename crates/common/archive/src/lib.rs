//! Port of PostgreSQL's `src/common/archive.c` — common WAL archive routines.
//!
//! Owns and installs the `archive_seams::build_restore_command` inward seam.

use mcx::{Mcx, PgString};
use types_error::PgResult;

/// `BuildRestoreCommand(restoreCommand, xlogpath, xlogfname,
/// lastRestartPointFname)` (common/archive.c).
///
/// Builds a restore command to retrieve a file from WAL archives, replacing
/// the supported aliases with values supplied by the caller as defined by the
/// GUC parameter `restore_command`: `xlogpath` for `%p`, `xlogfname` for `%f`
/// and `lastRestartPointFname` for `%r`.
///
/// The result is charged to `mcx` (the `palloc`'d-result analog). If any of the
/// required arguments is `None` and the corresponding alias appears in the
/// command, an error is thrown (`ERRCODE_INVALID_PARAMETER_VALUE`).
pub fn build_restore_command<'mcx>(
    mcx: Mcx<'mcx>,
    restore_command: &str,
    xlogpath: Option<&str>,
    xlogfname: Option<&str>,
    last_restart_point_fname: Option<&str>,
) -> PgResult<PgString<'mcx>> {
    // char *nativePath = NULL;
    // if (xlogpath) { nativePath = pstrdup(xlogpath); make_native_path(nativePath); }
    //
    // make_native_path() is a no-op on non-Windows targets (it only converts
    // '/' to '\\' under WIN32), so nativePath is xlogpath verbatim.
    let native_path: Option<&str> = xlogpath;

    // result = replace_percent_placeholders(restoreCommand, "restore_command",
    //     "frp", xlogfname, lastRestartPointFname, nativePath);
    //
    // The C "frp" letter list maps positionally to (xlogfname,
    // lastRestartPointFname, nativePath).
    percentrepl::replace_percent_placeholders(
        mcx,
        restore_command,
        "restore_command",
        &[
            ('f', xlogfname),
            ('r', last_restart_point_fname),
            ('p', native_path),
        ],
    )
}

/// Install the inward seams owned by `common/archive.c`.
pub fn init_seams() {
    archive_seams::build_restore_command::set(build_restore_command);
}
