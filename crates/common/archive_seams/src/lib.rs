//! Inward seams for `src/common/archive.c`. The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `BuildRestoreCommand(restoreCommand, xlogpath, xlogfname,
    /// lastRestartPointFname)` (common/archive.c) — build a `restore_command`
    /// shell line, substituting `%p`/`%f`/`%r` with the supplied paths. Any
    /// argument may be `None`; if its alias appears in the command an error is
    /// thrown (`ERRCODE_INVALID_PARAMETER_VALUE`). The result is charged to
    /// `mcx` (the `palloc`'d-result analog).
    pub fn build_restore_command<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        restore_command: &str,
        xlogpath: Option<&str>,
        xlogfname: Option<&str>,
        last_restart_point_fname: Option<&str>,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);
