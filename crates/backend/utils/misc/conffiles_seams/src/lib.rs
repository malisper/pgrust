use std::path::PathBuf;

use types_error::{ErrorLevel, PgResult};

// GetConfFilesInDir's out-params: sorted absolute *.conf paths, or the
// recorded err_msg (filenames empty).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConfFilesInDir {
    pub filenames: Vec<PathBuf>,
    pub err_msg: Option<String>,
}

seam_core::seam!(
    // AbsoluteConfigLocation(location, calling_file) (conffiles.c).
    pub fn absolute_config_location(
        location: String,
        calling_file: Option<PathBuf>,
    ) -> PathBuf
);

seam_core::seam!(
    // GetConfFilesInDir(includedir, calling_file, elevel, &num, &err_msg)
    // (conffiles.c).
    pub fn get_conf_files_in_dir(
        includedir: String,
        calling_file: Option<PathBuf>,
        elevel: ErrorLevel,
    ) -> PgResult<ConfFilesInDir>
);
