//! Seam declaration for `SetDataDirectoryCreatePerm` (`common/file_perm.c`),
//! which `checkDataDir` calls to derive the file/directory create modes and
//! umask from the data directory's mode. Calls panic until the owner lands.

seam_core::seam!(
    /// `SetDataDirectoryCreatePerm(dataDirMode)` (`common/file_perm.c`) — set
    /// the `pg_mode_mask` / `pg_dir_create_mode` / `pg_file_create_mode`
    /// globals from the data directory's stat mode. This is purely the global
    /// assignment (file_perm.c:33-50); the `umask()` syscall and the
    /// `data_directory_mode` GUC write are the *caller's* (miscinit's) own
    /// statements, not part of this routine. Infallible. Returns the resulting
    /// `(pg_mode_mask, pg_dir_create_mode)` so the caller can run its own
    /// `umask(pg_mode_mask)` and `data_directory_mode = pg_dir_create_mode`.
    pub fn set_data_directory_create_perm(data_dir_mode: u32) -> (u32, u32)
);
