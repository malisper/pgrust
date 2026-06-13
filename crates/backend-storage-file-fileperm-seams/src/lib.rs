//! Seam declaration for `SetDataDirectoryCreatePerm` (`common/file_perm.c`),
//! which `checkDataDir` calls to derive the file/directory create modes and
//! umask from the data directory's mode. Calls panic until the owner lands.

seam_core::seam!(
    /// `SetDataDirectoryCreatePerm(dataDirMode)` (`common/file_perm.c`) — set
    /// `pg_mode_mask` / `pg_dir_create_mode` / `pg_file_create_mode` from the
    /// data directory's stat mode (and apply the umask / `data_directory_mode`
    /// GUC). Infallible in C, but the GUC write can fail, so it returns
    /// `PgResult`.
    pub fn set_data_directory_create_perm(data_dir_mode: u32) -> types_error::PgResult<()>
);
