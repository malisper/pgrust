//! Seam declarations for the `conffiles` unit (`utils/misc/conffiles.c`): the
//! configuration-file path helpers `AbsoluteConfigLocation` and
//! `GetConfFilesInDir`, called by `guc-file.l` (and `hba.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use std::path::PathBuf;

use ::types_error::{ErrorLevel, PgResult};

/// Result of `GetConfFilesInDir`: the `*.conf` files found in a directory
/// (sorted, absolute paths). `err_msg` mirrors the C `*err_msg` out-parameter
/// set on the `return NULL` paths; when it is `Some`, `filenames` is empty and
/// the caller records the message rather than processing files.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConfFilesInDir {
    pub filenames: Vec<PathBuf>,
    pub err_msg: Option<String>,
}

seam_core::seam!(
    /// `AbsoluteConfigLocation(location, calling_file)` (conffiles.c): make
    /// `location` absolute, relative to `calling_file`'s directory (or
    /// `DataDir` at the top level). Allocates the returned path.
    pub fn absolute_config_location(
        location: String,
        calling_file: Option<PathBuf>,
    ) -> PathBuf
);

seam_core::seam!(
    /// `GetConfFilesInDir(includedir, calling_file, elevel, &num, &err_msg)`
    /// (conffiles.c): list the `*.conf` files in `includedir`, alphabetically.
    /// Allocates; the directory-access errors `ereport(elevel)` (carried on
    /// `Err` at/above `ERROR`) and set `err_msg` for the recording path.
    pub fn get_conf_files_in_dir(
        includedir: String,
        calling_file: Option<PathBuf>,
        elevel: ErrorLevel,
    ) -> PgResult<ConfFilesInDir>
);
