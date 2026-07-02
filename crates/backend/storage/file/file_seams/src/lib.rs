use types_error::{ErrorLevel, PgResult};

seam_core::seam!(
    pub fn open_transient_file(file_name: &str, file_flags: i32) -> PgResult<i32>
);

seam_core::seam!(
    pub fn close_transient_file(fd: i32) -> i32
);

seam_core::seam!(
    pub fn pg_fsync(fd: i32) -> i32
);

seam_core::seam!(
    pub fn fsync_fname(fname: &str, isdir: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel
);

seam_core::seam!(
    pub fn with_allocated_dir(
        dirname: &str,
        callback: &mut dyn FnMut(&str) -> PgResult<bool>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // BasicOpenFile(fileName, fileFlags) (fd.c): fd, or -1 with errno set.
    pub fn basic_open_file(file_name: &str, file_flags: i32) -> i32
);
