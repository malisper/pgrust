//! Seam declarations for `backend-utils-adt-misc` (`utils/adt/misc.c`): the
//! SRF data-production boundaries where a misc.c set-returning function reaches
//! unported substrate (fd.c directory/file walking, the generated
//! `system_fk_info.h` catalog table). The misc.c control flow / validation
//! lives in the owning crate; these seams produce the raw rows.
//!
//! Each defaults to a loud panic until the underlying substrate lands.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use adt_misc::CatalogForeignKeyRow;

seam_core::seam!(
    /// `pg_tablespace_databases(tablespaceOid)` (misc.c:223): the OIDs of the
    /// databases whose subdirectory under the tablespace is non-empty — the
    /// `AllocateDir`/`ReadDir`/`directory_is_empty` walk (misc.c:248-290).
    /// `Ok(None)` reproduces the empty tuplestore returned for the
    /// `GLOBALTABLESPACE_OID` / "not a tablespace OID" WARNING cases. Reaches
    /// fd.c/tablespace.c; panics until that substrate lands.
    pub fn tablespace_databases(tablespace_oid: Oid) -> PgResult<Option<Vec<Oid>>>
);

seam_core::seam!(
    /// `pg_tablespace_location(tablespaceOid)` (misc.c:300): read
    /// `pg_tblspc/<oid>` via `lstat`/`readlink` (misc.c:336-361), returning the
    /// resolved path text bytes (empty for the cluster default/global
    /// tablespaces). `Err` carries the `errcode_for_file_access` / "target is too
    /// long" `ereport`s. Reaches fd.c; panics until that substrate lands.
    pub fn tablespace_location<'mcx>(
        mcx: Mcx<'mcx>,
        tablespace_oid: Oid,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `pg_current_logfile()` file scan (misc.c:1022-1079): scan
    /// `LOG_METAINFO_DATAFILE` (`current_logfiles`) for the entry matching
    /// `logfmt` (or the first entry if `logfmt` is `None`), returning its file
    /// path bytes. `Ok(None)` reproduces every `PG_RETURN_NULL` (file absent or
    /// no matching format). The format-name validation is done by the misc.c
    /// caller; this seam owns only the `AllocateFile`/`fgets` walk. Reaches
    /// fd.c/syslogger; panics until that substrate lands.
    pub fn current_logfile<'mcx>(
        mcx: Mcx<'mcx>,
        logfmt: Option<&[u8]>,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `pg_get_catalog_foreign_keys()` (misc.c:495): the catalog foreign-key
    /// relationships from the generated `sys_fk_relationships[]` table
    /// (`catalog/system_fk_info.h`), with `fk_columns`/`pk_columns` already
    /// passed through `array_in` (misc.c:539/544). The generated table and the
    /// `array_in` fmgr dispatch are unported; panics until they land.
    pub fn catalog_foreign_keys() -> PgResult<Vec<CatalogForeignKeyRow>>
);
