//! Seam declarations for the server-file path-policy owner — `common/path.c`
//! (`canonicalize_path` / `is_absolute_path` / `path_is_prefix_of_path` /
//! `path_is_relative_and_below_cwd`) together with the `DataDir` /
//! `Log_directory` globals and the `miscadmin.h` current-user identity, which
//! are genuinely unported.
//!
//! `genfile.c::convert_and_check_filename` is the one consumer so far. Its
//! body is a single security-sensitive gate over these unported primitives, so
//! it crosses as one seam (the data-directory file-access policy) rather than
//! piecemeal across four unported owners. The owning unit(s) install it from
//! `init_seams()` when they land; until then a call panics loudly.

use ::mcx::{Mcx, PgString};
use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `convert_and_check_filename(arg)` (genfile.c): canonicalize the
    /// caller-supplied filename and enforce the server-file-read policy —
    /// roles with privileges of `pg_read_server_files` may name any path;
    /// otherwise an absolute path must be within `DataDir` or `Log_directory`
    /// and a relative path must be at or below the data directory. The
    /// canonicalized C string is returned in `mcx`. `Err` carries the
    /// `absolute path not allowed` / `path must be in or below the data
    /// directory` `ereport(ERROR)`s (ERRCODE_INSUFFICIENT_PRIVILEGE) and the
    /// `GetUserId`/role-membership lookup errors.
    pub fn convert_and_check_filename<'mcx>(
        mcx: Mcx<'mcx>,
        filename: &str,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// The data-directory-relative path the `pg_ls_*` directory functions feed
    /// to `pg_ls_dir_files`: `Log_directory` (`pg_ls_logdir`), `XLOGDIR`
    /// (`pg_ls_waldir`), `XLOGDIR "/archive_status"`, `XLOGDIR "/summaries"`,
    /// `PG_LOGICAL_SNAPSHOTS_DIR`, `PG_LOGICAL_MAPPINGS_DIR`. These resolve
    /// against `DataDir`/`Log_directory` globals (genuinely unported), so the
    /// owner returns the concrete path in `mcx`.
    pub fn wal_or_log_subdir<'mcx>(mcx: Mcx<'mcx>, which: WellKnownDir) -> PgString<'mcx>
);

seam_core::seam!(
    /// `TempTablespacePath(path, tblspc)` (tablespace.c): the `pgsql_tmp`
    /// directory for the given tablespace OID. Resolves against `DataDir`
    /// (genuinely unported), returned in `mcx`.
    pub fn temp_tablespace_path<'mcx>(
        mcx: Mcx<'mcx>,
        tblspc: Oid,
    ) -> PgString<'mcx>
);

/// Selector for [`wal_or_log_subdir`] — the well-known server directories the
/// `pg_ls_*` functions enumerate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WellKnownDir {
    /// `Log_directory` — `pg_ls_logdir`.
    LogDir,
    /// `XLOGDIR` — `pg_ls_waldir`.
    WalDir,
    /// `XLOGDIR "/archive_status"` — `pg_ls_archive_statusdir`.
    ArchiveStatusDir,
    /// `XLOGDIR "/summaries"` — `pg_ls_summariesdir`.
    SummariesDir,
    /// `PG_LOGICAL_SNAPSHOTS_DIR` — `pg_ls_logicalsnapdir`.
    LogicalSnapDir,
    /// `PG_LOGICAL_MAPPINGS_DIR` — `pg_ls_logicalmapdir`.
    LogicalMapDir,
}
