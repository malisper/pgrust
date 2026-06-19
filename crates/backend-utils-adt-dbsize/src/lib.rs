//! Port of PostgreSQL `src/backend/utils/adt/dbsize.c` — database object size
//! functions, and related inquiries.
//!
//! Every function in `dbsize.c` is implemented here with its full logic:
//! identical branch order, message text, and SQLSTATE.
//!
//!   * The pure size-formatting cores (`pg_size_pretty`, `pg_size_pretty_numeric`,
//!     `pg_size_bytes` and the `numeric_*` helpers) ride on the real ported
//!     `backend-utils-adt-numeric` arithmetic (`DirectFunctionCall*` round-trips
//!     become direct calls over the packed on-disk `Numeric` image, an
//!     `mcx`-charged `PgVec<u8>`).
//!   * The relation-path build (`relpathbackend`) and `forkname_to_number` call
//!     the ported `common/relpath.c` directly.
//!   * The on-disk size walk (`db_dir_size`, `calculate_*_size`) and every
//!     catalog / syscache / relation-open / ACL / relmapper / namespace
//!     touchpoint cross an outbound `seam_core::seam!`: the runtime filesystem
//!     and the still-unported catalog owners install them; each panics loudly
//!     until then (no silent fallback). This file's own control flow is ported
//!     1:1; only the cross-subsystem call is delegated.
//!
//! The SQL-callable entry points map to the project's fmgr/Datum boundary
//! convention: a `Datum`-returning C function that `PG_RETURN_NULL`s becomes a
//! Rust function returning `PgResult<Option<_>>`; the `PG_GETARG_*`/detoast of
//! its arguments is the fmgr layer's responsibility.

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_utils_error::{ereport, PgError, PgResult};
use mcx::{Mcx, PgVec};
use types_catalog::catalog::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID};
use types_core::primitive::{
    ForkNumber, ProcNumber, INVALID_PROC_NUMBER, MAX_FORKNUM as MAX_FORKNUM_ENUM,
};
use types_error::error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_storage::file::{PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY};
use types_storage::storage::RelFileLocator;

use types_core::primitive::Oid;

/// The fmgr/`Datum` boundary: the SQL-callable `fc_*` adapters for the
/// `dbsize.c` functions, registered into the fmgr-core builtin table by
/// [`fmgr_builtins::register_dbsize_builtins`] (called from [`init_seams`]).
pub mod fmgr_builtins;

/// `RelFileNumber` (`relpath.h`).
pub type RelFileNumber = types_core::primitive::Oid;

// ===========================================================================
// Constants and small helpers mirroring the C macros / static tables.
// ===========================================================================

/// `InvalidOid`.
const InvalidOid: Oid = 0;

/// `InvalidRelFileNumber` (`relpath.h:26`).
const InvalidRelFileNumber: RelFileNumber = 0;

/// `MAX_FORKNUM` (`storage/relfilelocator.h`, `== INIT_FORKNUM == 3`), as the
/// `i32` fork index the `for (forkNum = 0; forkNum <= MAX_FORKNUM; ...)` loops
/// run over.
const MAX_FORKNUM: i32 = MAX_FORKNUM_ENUM as i32;

/// `RELKIND_*` (`pg_class.h:167-176`), as raw `char`s.
const RELKIND_RELATION: i8 = b'r' as i8;
const RELKIND_INDEX: i8 = b'i' as i8;
const RELKIND_SEQUENCE: i8 = b'S' as i8;
const RELKIND_TOASTVALUE: i8 = b't' as i8;
const RELKIND_MATVIEW: i8 = b'm' as i8;

/// `RELPERSISTENCE_*` (`pg_class.h:179-181`), as raw `char`s.
const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
const RELPERSISTENCE_UNLOGGED: i8 = b'u' as i8;
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `DatabaseRelationId` (`pg_database.h`, relation Oid 1262).
const DatabaseRelationId: Oid = 1262;
/// `TableSpaceRelationId` (`pg_tablespace.h`, relation Oid 1213).
const TableSpaceRelationId: Oid = 1213;
/// `ACL_CREATE` (`parsenodes.h`, `1<<9`).
const ACL_CREATE: u64 = 1 << 9;
/// `ACL_CONNECT` (`parsenodes.h`, `1<<11`).
const ACL_CONNECT: u64 = 1 << 11;
/// `ROLE_PG_READ_ALL_STATS` (`pg_authid.dat`, Oid 3375).
const ROLE_PG_READ_ALL_STATS: Oid = 3375;

/// `#define RELKIND_HAS_STORAGE(relkind)` (`pg_class.h:200`).
fn RELKIND_HAS_STORAGE(relkind: i8) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// `#define RelFileNumberIsValid(relnumber)` (`relpath.h:27`).
fn RelFileNumberIsValid(relnumber: RelFileNumber) -> bool {
    relnumber != InvalidRelFileNumber
}

/// `#define OidIsValid(objectId)` (`c.h`).
fn OidIsValid(object_id: Oid) -> bool {
    object_id != InvalidOid
}

/// `errcode_for_file_access()` + `errmsg("could not stat file \"%s\": %m", path)`,
/// raised from the `stat` error path of the size routines.
fn could_not_stat_file(path: &str, errno: i32) -> PgError {
    ereport(ERROR)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(format!("could not stat file \"{path}\": %m"))
        .into_error()
}

/// `errcode_for_file_access()` + `errmsg("could not open directory \"%s\": %m",
/// dirname)`, the error `ReadDir(NULL, dirname)` raises when `AllocateDir`
/// returned NULL.  Only reachable from `calculate_database_size`'s un-NULL-checked
/// `pg_tblspc` scan.
fn could_not_open_directory(dirname: &str, errno: i32) -> PgError {
    ereport(ERROR)
        .with_saved_errno(errno)
        .errcode_for_file_access()
        .errmsg(format!("could not open directory \"{dirname}\": %m"))
        .into_error()
}

// ===========================================================================
// Outbound seam carriers + declarations.
// ===========================================================================

/// The subset of `struct stat` that `dbsize.c` consults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStat {
    /// `fst.st_size`.
    pub size: i64,
    /// `S_ISDIR(fst.st_mode)`.
    pub is_dir: bool,
}

/// One directory entry returned by the directory-walk seam (`direntry->d_name`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    /// `direntry->d_name`.
    pub name: String,
}

/// Result of `AllocateDir(path)` plus the eager `ReadDir`/`FreeDir` walk,
/// modelling both `if (!dirdesc)` (the NULL-`DIR*` branch) and the entries it
/// would yield.  Failure carries the saved `errno` so the one caller that does
/// NOT NULL-check `AllocateDir` (`calculate_database_size`) can replicate the
/// "could not open directory" error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenDir {
    /// `AllocateDir` succeeded; the entries (including `"."`/`".."`, skipped 1:1).
    Opened(Vec<DirEntry>),
    /// `AllocateDir` returned NULL; `errno` is the value saved at the failed
    /// `opendir`, used only by `calculate_database_size`.
    Failed { errno: i32 },
}

/// Result of a `stat` call, modelling C's
/// `if (stat(...) < 0) { if (errno == ENOENT) ...; else ereport(...); }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatResult {
    /// `stat` succeeded.
    Ok(FileStat),
    /// `stat` failed with `errno == ENOENT` (file vanished); caller `continue`s
    /// / `break`s, as in C.
    NotFound,
    /// `stat` failed otherwise; `errno` carried so the caller raises
    /// `errcode_for_file_access()` + "could not stat file \"%s\": %m".
    Error { errno: i32 },
}

/// `ObjectType` argument to `aclcheck_error` for the two object kinds the size
/// routines check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclObjectType {
    /// `OBJECT_DATABASE`.
    Database,
    /// `OBJECT_TABLESPACE`.
    Tablespace,
}

/// `Form_pg_class` columns that `pg_relation_filenode` / `pg_relation_filepath`
/// read out of the syscache tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgClassForm {
    /// `relform->relkind`.
    pub relkind: i8,
    /// `relform->relfilenode`.
    pub relfilenode: RelFileNumber,
    /// `relform->relisshared`.
    pub relisshared: bool,
    /// `relform->reltablespace`.
    pub reltablespace: Oid,
    /// `relform->relnamespace`.
    pub relnamespace: Oid,
    /// `relform->relpersistence`.
    pub relpersistence: i8,
}

/// A relation opened by the table/index size routines: the on-disk address plus
/// the toast/index metadata they consult.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRelation {
    /// `rel->rd_locator.spcOid`.
    pub spc_oid: Oid,
    /// `rel->rd_locator.dbOid`.
    pub db_oid: Oid,
    /// `rel->rd_locator.relNumber`.
    pub rel_number: RelFileNumber,
    /// `rel->rd_backend` (a `ProcNumber`).
    pub backend: ProcNumber,
    /// `rel->rd_rel->reltoastrelid` (`InvalidOid` when none).
    pub reltoastrelid: Oid,
    /// `rel->rd_rel->relhasindex`.
    pub relhasindex: bool,
}

/* ---- storage/fd.c + <sys/stat.h> ---- */

seam_core::seam!(
    /// `AllocateDir(path)` + iterate `ReadDir(dirdesc, path)` + `FreeDir()`.
    pub fn read_dir(path: &str) -> OpenDir
);
seam_core::seam!(
    /// `stat(path, &fst)`.
    pub fn stat(path: &str) -> StatResult
);
seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` — `Err` if a pending interrupt aborts the walk.
    pub fn check_for_interrupts() -> PgResult<()>
);

/* ---- catalog: syscache / relation / namespace / commands / acl / relmapper ---- */

seam_core::seam!(
    /// `SearchSysCacheExists1(DATABASEOID, ...)`.
    pub fn database_exists(db_oid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `SearchSysCacheExists1(TABLESPACEOID, ...)`.
    pub fn tablespace_exists(tblspc_oid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `get_database_oid(name, false)`.
    pub fn get_database_oid(name: &str) -> PgResult<Oid>
);
seam_core::seam!(
    /// `get_tablespace_oid(name, false)`.
    pub fn get_tablespace_oid(name: &str) -> PgResult<Oid>
);
seam_core::seam!(
    /// `GetUserId()`.
    pub fn get_user_id() -> PgResult<Oid>
);
seam_core::seam!(
    /// `has_privs_of_role(member, role)`.
    pub fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `object_aclcheck(classId, objId, roleId, mode)`: `Ok(true)` for
    /// `ACLCHECK_OK`, `Ok(false)` for a non-OK `AclResult`.
    pub fn object_aclcheck(class_id: Oid, obj_id: Oid, role_id: Oid, mode: u64) -> PgResult<bool>
);
seam_core::seam!(
    /// `aclcheck_error(aclresult, objtype, objectname)` — always returns the
    /// permission-denied [`PgError`] (C's `aclcheck_error` never returns); the
    /// object name is computed inside via `get_database_name`/`get_tablespace_name`.
    pub fn aclcheck_error(objtype: AclObjectType, obj_id: Oid) -> PgError
);
seam_core::seam!(
    /// `MyDatabaseTableSpace`.
    pub fn my_database_tablespace() -> Oid
);
seam_core::seam!(
    /// `MyDatabaseId`.
    pub fn my_database_id() -> Oid
);
seam_core::seam!(
    /// `try_relation_open(relOid, AccessShareLock)` — `None` when the relation
    /// could not be opened. The provider holds the lock until `relation_close`.
    pub fn try_relation_open(rel_oid: Oid) -> PgResult<Option<OpenRelation>>
);
seam_core::seam!(
    /// `relation_open(relOid, AccessShareLock)` — must succeed.
    pub fn relation_open(rel_oid: Oid) -> PgResult<OpenRelation>
);
seam_core::seam!(
    /// `relation_close(rel, AccessShareLock)`.
    pub fn relation_close(rel_oid: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `RelationGetIndexList(rel)`.
    pub fn relation_get_index_list(rel_oid: Oid) -> PgResult<Vec<Oid>>
);
seam_core::seam!(
    /// `SearchSysCache1(RELOID, ...)` then read the `Form_pg_class` fields;
    /// `None` when the tuple is not valid. The provider releases the tuple.
    pub fn pg_class_form(rel_oid: Oid) -> PgResult<Option<PgClassForm>>
);
seam_core::seam!(
    /// `RelationMapOidToFilenumber(relid, relisshared)`.
    pub fn relation_map_oid_to_filenumber(relid: Oid, relisshared: bool) -> PgResult<RelFileNumber>
);
seam_core::seam!(
    /// `RelidByRelfilenumber(reltablespace, relfilenumber)`.
    pub fn relid_by_relfilenumber(reltablespace: Oid, relfilenumber: RelFileNumber) -> PgResult<Oid>
);
seam_core::seam!(
    /// `isTempOrTempToastNamespace(relnamespace)`.
    pub fn is_temp_or_temp_toast_namespace(relnamespace: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `ProcNumberForTempRelations()`.
    pub fn proc_number_for_temp_relations() -> ProcNumber
);
seam_core::seam!(
    /// `GetTempNamespaceProcNumber(relnamespace)`.
    pub fn get_temp_namespace_proc_number(relnamespace: Oid) -> PgResult<ProcNumber>
);

// ===========================================================================
// pg_size_pretty units table (dbsize.c:37-70).
// ===========================================================================

/// `struct size_pretty_unit` (dbsize.c:38).  All units are powers of 2.
struct SizePrettyUnit {
    name: Option<&'static str>,
    limit: u32,
    round: bool,
    unitbits: u8,
}

/// `size_pretty_units[]` (dbsize.c:49).
const SIZE_PRETTY_UNITS: &[SizePrettyUnit] = &[
    SizePrettyUnit { name: Some("bytes"), limit: 10 * 1024, round: false, unitbits: 0 },
    SizePrettyUnit { name: Some("kB"), limit: 20 * 1024 - 1, round: true, unitbits: 10 },
    SizePrettyUnit { name: Some("MB"), limit: 20 * 1024 - 1, round: true, unitbits: 20 },
    SizePrettyUnit { name: Some("GB"), limit: 20 * 1024 - 1, round: true, unitbits: 30 },
    SizePrettyUnit { name: Some("TB"), limit: 20 * 1024 - 1, round: true, unitbits: 40 },
    SizePrettyUnit { name: Some("PB"), limit: 20 * 1024 - 1, round: true, unitbits: 50 },
    SizePrettyUnit { name: None, limit: 0, round: false, unitbits: 0 },
];

/// `struct size_bytes_unit_alias` (dbsize.c:60).
struct SizeBytesUnitAlias {
    alias: Option<&'static str>,
    unit_index: usize,
}

/// `size_bytes_aliases[]` (dbsize.c:67).
const SIZE_BYTES_ALIASES: &[SizeBytesUnitAlias] = &[
    SizeBytesUnitAlias { alias: Some("B"), unit_index: 0 },
    SizeBytesUnitAlias { alias: None, unit_index: 0 },
];

/// `#define half_rounded(x)` (dbsize.c:35) — divide by two, rounding away from
/// zero.
fn half_rounded(x: i64) -> i64 {
    (x + (if x < 0 { -1 } else { 1 })) / 2
}

// ===========================================================================
// On-disk size walk (dbsize.c:72-361).
// ===========================================================================

/// `db_dir_size()` (dbsize.c:73) — physical size of directory contents, or 0 if
/// dir doesn't exist.
pub fn db_dir_size(path: &str) -> PgResult<i64> {
    let mut dirsize: i64 = 0;

    // dirdesc = AllocateDir(path); if (!dirdesc) return 0;
    let OpenDir::Opened(dirdesc) = read_dir::call(path) else {
        return Ok(0);
    };

    for direntry in dirdesc {
        check_for_interrupts::call()?;

        if direntry.name == "." || direntry.name == ".." {
            continue;
        }

        // snprintf(filename, ..., "%s/%s", path, d_name);
        let filename = format!("{path}/{}", direntry.name);

        match stat::call(&filename) {
            StatResult::Ok(FileStat { size, .. }) => dirsize += size,
            StatResult::NotFound => continue,
            StatResult::Error { errno } => return Err(could_not_stat_file(&filename, errno)),
        }
    }

    Ok(dirsize)
}

/// `calculate_database_size()` (dbsize.c:117) — size of database in all
/// tablespaces.
fn calculate_database_size(db_oid: Oid) -> PgResult<i64> {
    // aclresult = object_aclcheck(DatabaseRelationId, dbOid, GetUserId(), ACL_CONNECT);
    // if (aclresult != ACLCHECK_OK &&
    //     !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS))
    //     aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(dbOid));
    let aclok =
        object_aclcheck::call(DatabaseRelationId, db_oid, get_user_id::call()?, ACL_CONNECT)?;
    if !aclok && !has_privs_of_role::call(get_user_id::call()?, ROLE_PG_READ_ALL_STATS)? {
        return Err(aclcheck_error::call(AclObjectType::Database, db_oid));
    }

    // Shared storage in pg_global is not counted.

    // Include pg_default storage: snprintf(pathname, ..., "base/%u", dbOid);
    let pathname = format!("base/{db_oid}");
    let mut totalsize = db_dir_size(&pathname)?;

    // Scan the non-default tablespaces. C does NOT NULL-check
    // AllocateDir(PG_TBLSPC_DIR); if it were NULL the first ReadDir(NULL, dirpath)
    // raises errcode_for_file_access() + "could not open directory \"%s\": %m".
    let dirpath = PG_TBLSPC_DIR.to_string();
    let dirdesc = match read_dir::call(&dirpath) {
        OpenDir::Opened(entries) => entries,
        OpenDir::Failed { errno } => return Err(could_not_open_directory(&dirpath, errno)),
    };

    for direntry in dirdesc {
        check_for_interrupts::call()?;

        if direntry.name == "." || direntry.name == ".." {
            continue;
        }

        // "%s/%s/%s/%u", PG_TBLSPC_DIR, d_name, TABLESPACE_VERSION_DIRECTORY, dbOid
        let pathname = format!(
            "{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}/{db_oid}",
            direntry.name,
        );
        totalsize += db_dir_size(&pathname)?;
    }

    Ok(totalsize)
}

/// `pg_database_size_oid()` (dbsize.c:167).
pub fn pg_database_size_oid(db_oid: Oid) -> PgResult<Option<i64>> {
    // Avoid non-user-facing error message later if the database doesn't exist.
    if !database_exists::call(db_oid)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("database with OID {db_oid} does not exist"))
            .into_error());
    }

    let size = calculate_database_size(db_oid)?;
    if size == 0 {
        return Ok(None);
    }
    Ok(Some(size))
}

/// `pg_database_size_name()` (dbsize.c:190).
pub fn pg_database_size_name(db_name: &str) -> PgResult<Option<i64>> {
    let db_oid = get_database_oid::call(db_name)?;
    let size = calculate_database_size(db_oid)?;
    if size == 0 {
        return Ok(None);
    }
    Ok(Some(size))
}

/// `calculate_tablespace_size()` (dbsize.c:210) — total size of tablespace.
/// Returns `None` (`-1` in C) if the directory cannot be found.
fn calculate_tablespace_size(tblspc_oid: Oid) -> PgResult<Option<i64>> {
    let mut totalsize: i64 = 0;

    // if (tblspcOid != MyDatabaseTableSpace &&
    //     !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)) {
    //     aclresult = object_aclcheck(TableSpaceRelationId, tblspcOid, GetUserId(), ACL_CREATE);
    //     if (aclresult != ACLCHECK_OK) aclcheck_error(...);
    // }
    if tblspc_oid != my_database_tablespace::call()
        && !has_privs_of_role::call(get_user_id::call()?, ROLE_PG_READ_ALL_STATS)?
    {
        let aclok = object_aclcheck::call(
            TableSpaceRelationId,
            tblspc_oid,
            get_user_id::call()?,
            ACL_CREATE,
        )?;
        if !aclok {
            return Err(aclcheck_error::call(AclObjectType::Tablespace, tblspc_oid));
        }
    }

    let tblspc_path = if tblspc_oid == DEFAULTTABLESPACE_OID {
        "base".to_string()
    } else if tblspc_oid == GLOBALTABLESPACE_OID {
        "global".to_string()
    } else {
        format!("{PG_TBLSPC_DIR}/{tblspc_oid}/{TABLESPACE_VERSION_DIRECTORY}")
    };

    // dirdesc = AllocateDir(tblspcPath); if (!dirdesc) return -1;
    let OpenDir::Opened(dirdesc) = read_dir::call(&tblspc_path) else {
        return Ok(None);
    };

    for direntry in dirdesc {
        check_for_interrupts::call()?;

        if direntry.name == "." || direntry.name == ".." {
            continue;
        }

        let pathname = format!("{tblspc_path}/{}", direntry.name);

        let fst = match stat::call(&pathname) {
            StatResult::Ok(fst) => fst,
            StatResult::NotFound => continue,
            StatResult::Error { errno } => return Err(could_not_stat_file(&pathname, errno)),
        };

        if fst.is_dir {
            totalsize += db_dir_size(&pathname)?;
        }

        totalsize += fst.size;
    }

    Ok(Some(totalsize))
}

/// `pg_tablespace_size_oid()` (dbsize.c:280).
pub fn pg_tablespace_size_oid(tblspc_oid: Oid) -> PgResult<Option<i64>> {
    if !tablespace_exists::call(tblspc_oid)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("tablespace with OID {tblspc_oid} does not exist"))
            .into_error());
    }

    let size = calculate_tablespace_size(tblspc_oid)?;
    // if (size < 0) PG_RETURN_NULL();
    Ok(size)
}

/// `pg_tablespace_size_name()` (dbsize.c:303).
pub fn pg_tablespace_size_name(tblspc_name: &str) -> PgResult<Option<i64>> {
    let tblspc_oid = get_tablespace_oid::call(tblspc_name)?;
    let size = calculate_tablespace_size(tblspc_oid)?;
    Ok(size)
}

/// `calculate_relation_size()` (dbsize.c:325) — size of (one fork of) a relation.
fn calculate_relation_size(
    spc_oid: Oid,
    db_oid: Oid,
    rel_number: RelFileNumber,
    backend: ProcNumber,
    forknum: i32,
) -> PgResult<i64> {
    let mut totalsize: i64 = 0;

    // relationpath = relpathbackend(*rfn, backend, forknum);
    let fork = ForkNumber::from_i32(forknum)
        .ok_or_else(|| PgError::error("calculate_relation_size: fork number out of range"))?;
    let relationpath = backend_common_relpath::relpathbackend(
        RelFileLocator { spcOid: spc_oid, dbOid: db_oid, relNumber: rel_number },
        backend,
        fork,
    );

    let mut segcount: u32 = 0;
    loop {
        check_for_interrupts::call()?;

        let pathname = if segcount == 0 {
            // snprintf(pathname, MAXPGPATH, "%s", relationpath.str)
            relationpath.clone()
        } else {
            // snprintf(pathname, MAXPGPATH, "%s.%u", relationpath.str, segcount)
            format!("{relationpath}.{segcount}")
        };

        match stat::call(&pathname) {
            StatResult::Ok(FileStat { size, .. }) => totalsize += size,
            StatResult::NotFound => break, // if (errno == ENOENT) break;
            StatResult::Error { errno } => return Err(could_not_stat_file(&pathname, errno)),
        }

        segcount += 1;
    }

    Ok(totalsize)
}

/// `pg_relation_size()` (dbsize.c:363).
pub fn pg_relation_size(rel_oid: Oid, fork_name: &str) -> PgResult<Option<i64>> {
    // rel = try_relation_open(relOid, AccessShareLock);
    let Some(rel) = try_relation_open::call(rel_oid)? else {
        // Return NULL for already-dropped tables rather than throw.
        return Ok(None);
    };

    let forknum = backend_common_relpath::forkname_to_number(fork_name)? as i32;
    let result = calculate_relation_size(
        rel.spc_oid,
        rel.db_oid,
        rel.rel_number,
        rel.backend,
        forknum,
    );

    relation_close::call(rel_oid)?;

    let size = result?;
    Ok(Some(size))
}

/// `calculate_toast_table_size()` (dbsize.c:395) — total on-disk size of a TOAST
/// relation, including its indexes.
fn calculate_toast_table_size(toastrelid: Oid) -> PgResult<i64> {
    let mut size: i64 = 0;

    let toast_rel = relation_open::call(toastrelid)?;

    // toast heap size, including FSM and VM size.
    for fork_num in 0..=MAX_FORKNUM {
        size += calculate_relation_size(
            toast_rel.spc_oid,
            toast_rel.db_oid,
            toast_rel.rel_number,
            toast_rel.backend,
            fork_num,
        )?;
    }

    // toast index size, including FSM and VM size.
    let indexlist = relation_get_index_list::call(toastrelid)?;

    for idx_oid in &indexlist {
        let toast_idx_rel = relation_open::call(*idx_oid)?;
        for fork_num in 0..=MAX_FORKNUM {
            size += calculate_relation_size(
                toast_idx_rel.spc_oid,
                toast_idx_rel.db_oid,
                toast_idx_rel.rel_number,
                toast_idx_rel.backend,
                fork_num,
            )?;
        }
        relation_close::call(*idx_oid)?;
    }
    relation_close::call(toastrelid)?;

    Ok(size)
}

/// `calculate_table_size()` (dbsize.c:441).
fn calculate_table_size(rel: &OpenRelation) -> PgResult<i64> {
    let mut size: i64 = 0;

    // heap size, including FSM and VM.
    for fork_num in 0..=MAX_FORKNUM {
        size += calculate_relation_size(
            rel.spc_oid,
            rel.db_oid,
            rel.rel_number,
            rel.backend,
            fork_num,
        )?;
    }

    // Size of toast relation.
    if OidIsValid(rel.reltoastrelid) {
        size += calculate_toast_table_size(rel.reltoastrelid)?;
    }

    Ok(size)
}

/// `calculate_indexes_size()` (dbsize.c:468).
fn calculate_indexes_size(rel_oid: Oid, rel: &OpenRelation) -> PgResult<i64> {
    let mut size: i64 = 0;

    if rel.relhasindex {
        let index_oids = relation_get_index_list::call(rel_oid)?;

        for idx_oid in &index_oids {
            let idx_rel = relation_open::call(*idx_oid)?;
            for fork_num in 0..=MAX_FORKNUM {
                size += calculate_relation_size(
                    idx_rel.spc_oid,
                    idx_rel.db_oid,
                    idx_rel.rel_number,
                    idx_rel.backend,
                    fork_num,
                )?;
            }
            relation_close::call(*idx_oid)?;
        }
    }

    Ok(size)
}

/// `pg_table_size()` (dbsize.c:503).
pub fn pg_table_size(rel_oid: Oid) -> PgResult<Option<i64>> {
    let Some(rel) = try_relation_open::call(rel_oid)? else {
        return Ok(None);
    };

    let result = calculate_table_size(&rel);
    relation_close::call(rel_oid)?;

    let size = result?;
    Ok(Some(size))
}

/// `pg_indexes_size()` (dbsize.c:522).
pub fn pg_indexes_size(rel_oid: Oid) -> PgResult<Option<i64>> {
    let Some(rel) = try_relation_open::call(rel_oid)? else {
        return Ok(None);
    };

    let result = calculate_indexes_size(rel_oid, &rel);
    relation_close::call(rel_oid)?;

    let size = result?;
    Ok(Some(size))
}

/// `calculate_total_relation_size()` (dbsize.c:545).
fn calculate_total_relation_size(rel_oid: Oid, rel: &OpenRelation) -> PgResult<i64> {
    let mut size = calculate_table_size(rel)?;
    size += calculate_indexes_size(rel_oid, rel)?;
    Ok(size)
}

/// `pg_total_relation_size()` (dbsize.c:564).
pub fn pg_total_relation_size(rel_oid: Oid) -> PgResult<Option<i64>> {
    let Some(rel) = try_relation_open::call(rel_oid)? else {
        return Ok(None);
    };

    let result = calculate_total_relation_size(rel_oid, &rel);
    relation_close::call(rel_oid)?;

    let size = result?;
    Ok(Some(size))
}

// ===========================================================================
// Size formatting (dbsize.c:583-880).
// ===========================================================================

/// `pg_size_pretty()` (dbsize.c:586) — format an `int64` byte count with units.
/// C builds the result with `cstring_to_text(buf)`; this returns the owned text
/// string (the fmgr layer wraps it as `text`).
pub fn pg_size_pretty(mut size: i64) -> String {
    let mut buf = String::new();

    let mut i = 0;
    loop {
        let unit = &SIZE_PRETTY_UNITS[i];
        if unit.name.is_none() {
            break; // loop is over `unit->name != NULL`
        }
        let next = &SIZE_PRETTY_UNITS[i + 1];

        let abs_size: u64 = if size < 0 {
            0u64.wrapping_sub(size as u64)
        } else {
            size as u64
        };

        // Use this unit if there are no more units or below the limit.
        if next.name.is_none() || abs_size < unit.limit as u64 {
            if unit.round {
                size = half_rounded(size);
            }
            // snprintf(buf, ..., INT64_FORMAT " %s", size, unit->name);
            buf = format!("{size} {}", unit.name.unwrap());
            break;
        }

        // Determine the number of bits to use to build the divisor.
        let bits: u8 = (next.unitbits as i32 - unit.unitbits as i32 - (next.round as i32)
            + (unit.round as i32)) as u8;
        size /= 1i64 << bits;

        i += 1;
    }

    buf
}

/// `numeric_to_cstring()` (dbsize.c:628) — `DatumGetCString(numeric_out(n))`.
fn numeric_to_cstring(mcx: Mcx<'_>, n: &[u8]) -> PgResult<String> {
    backend_utils_adt_numeric::io::numeric_out(mcx, n)
}

/// `numeric_is_less()` (dbsize.c:636) — `numeric_lt(a, b)`.
fn numeric_is_less(a: &[u8], b: &[u8]) -> bool {
    backend_utils_adt_numeric::ops_sql::numeric_lt(a, b)
}

/// `numeric_absolute()` (dbsize.c:645) — `numeric_abs(n)`.
fn numeric_absolute<'mcx>(mcx: Mcx<'mcx>, n: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_numeric::ops_sql::numeric_abs(mcx, n)
}

/// `numeric_half_rounded()` (dbsize.c:655).
fn numeric_half_rounded<'mcx>(mcx: Mcx<'mcx>, n: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let zero = int64_to_numeric(mcx, 0)?;
    let one = int64_to_numeric(mcx, 1)?;
    let two = int64_to_numeric(mcx, 2)?;

    let d = if backend_utils_adt_numeric::ops_sql::numeric_ge(n, &zero) {
        backend_utils_adt_numeric::ops_sql::numeric_add(mcx, n, &one)?
    } else {
        backend_utils_adt_numeric::ops_sql::numeric_sub(mcx, n, &one)?
    };

    backend_utils_adt_numeric::ops_sql::numeric_div_trunc(mcx, &d, &two)
}

/// `numeric_truncated_divide()` (dbsize.c:677).
fn numeric_truncated_divide<'mcx>(
    mcx: Mcx<'mcx>,
    n: &[u8],
    divisor: i64,
) -> PgResult<PgVec<'mcx, u8>> {
    let divisor_numeric = int64_to_numeric(mcx, divisor)?;
    backend_utils_adt_numeric::ops_sql::numeric_div_trunc(mcx, n, &divisor_numeric)
}

/// `int64_to_numeric(value)` (numeric.c) — the packed on-disk image.
fn int64_to_numeric<'mcx>(mcx: Mcx<'mcx>, value: i64) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_numeric::convert::int64_to_numeric(mcx, value)
}

/// `numeric_int8(num)` (numeric.c) — `numeric_int8_opt_error(num, NULL)`: convert
/// to `int8`, raising on NaN/infinity or out-of-range.  Ported here because the
/// numeric crate exposes the primitives (`set_var_from_num`,
/// `numericvar_to_int64`) but not this SQL-level wrapper.
fn numeric_int8(mcx: Mcx<'_>, num: &[u8]) -> PgResult<i64> {
    if types_numeric::numeric_is_special(num) {
        if types_numeric::numeric_is_nan(num) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("cannot convert NaN to {}", "bigint"))
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("cannot convert infinity to {}", "bigint"))
                .into_error());
        }
    }

    // Convert to variable format, then convert to int8.
    let x = backend_utils_adt_numeric::convert::set_var_from_num(mcx, num)?;

    match backend_utils_adt_numeric::kernel_transcendental::numericvar_to_int64(&x)? {
        Some(result) => Ok(result),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("bigint out of range")
            .into_error()),
    }
}

/// `pg_size_pretty_numeric()` (dbsize.c:689) — format a `Numeric` byte count.
/// `size` is the (detoasted) packed on-disk numeric image; returns the owned
/// text (the fmgr layer wraps it as `text`).
pub fn pg_size_pretty_numeric(mcx: Mcx<'_>, size: &[u8]) -> PgResult<String> {
    // The running value; starts as a copy of the argument's bytes.
    let mut size: Vec<u8> = size.to_vec();
    let mut result: Option<String> = None;

    let mut i = 0;
    loop {
        let unit = &SIZE_PRETTY_UNITS[i];
        if unit.name.is_none() {
            break;
        }
        let next = &SIZE_PRETTY_UNITS[i + 1];

        // use this unit if there are no more units or we're below the limit.
        let below_limit = if next.name.is_none() {
            true
        } else {
            let abs = numeric_absolute(mcx, &size)?;
            let lim = int64_to_numeric(mcx, unit.limit as i64)?;
            numeric_is_less(&abs, &lim)
        };

        if below_limit {
            if unit.round {
                size = numeric_half_rounded(mcx, &size)?.to_vec();
            }
            // result = psprintf("%s %s", numeric_to_cstring(size), unit->name);
            result = Some(format!(
                "{} {}",
                numeric_to_cstring(mcx, &size)?,
                unit.name.unwrap()
            ));
            break;
        }

        // Determine the number of bits to use to build the divisor.
        let shiftby: u32 = (next.unitbits as i32 - unit.unitbits as i32 - (next.round as i32)
            + (unit.round as i32)) as u32;
        size = numeric_truncated_divide(mcx, &size, 1i64 << shiftby)?.to_vec();

        i += 1;
    }

    // PG_RETURN_TEXT_P(cstring_to_text(result)); `result` is NULL only if the
    // table were empty, which it never is.
    result.ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal("pg_size_pretty_numeric: no unit matched")
            .into_error()
    })
}

/// `pg_size_bytes()` (dbsize.c:730) — convert a human-readable size to bytes.
/// `arg` is the (already detoasted) text argument's bytes.
pub fn pg_size_bytes(mcx: Mcx<'_>, arg: &[u8]) -> PgResult<i64> {
    let str_bytes = arg;

    // Skip leading whitespace.
    let mut strptr = 0usize;
    while strptr < str_bytes.len() && is_space(str_bytes[strptr]) {
        strptr += 1;
    }

    // Check that we have a valid number and determine where it ends.
    let mut endptr = strptr;
    let mut have_digits = false;

    // Part (1): sign.
    if endptr < str_bytes.len() && (str_bytes[endptr] == b'-' || str_bytes[endptr] == b'+') {
        endptr += 1;
    }

    // Part (2): main digit string.
    if endptr < str_bytes.len() && is_digit(str_bytes[endptr]) {
        have_digits = true;
        loop {
            endptr += 1;
            if !(endptr < str_bytes.len() && is_digit(str_bytes[endptr])) {
                break;
            }
        }
    }

    // Part (3): optional decimal point and fractional digits.
    if endptr < str_bytes.len() && str_bytes[endptr] == b'.' {
        endptr += 1;
        if endptr < str_bytes.len() && is_digit(str_bytes[endptr]) {
            have_digits = true;
            loop {
                endptr += 1;
                if !(endptr < str_bytes.len() && is_digit(str_bytes[endptr])) {
                    break;
                }
            }
        }
    }

    // Complain if we don't have a valid number at this point.
    if !have_digits {
        return Err(invalid_size_lossy(arg));
    }

    // Part (4): optional exponent.
    if endptr < str_bytes.len() && (str_bytes[endptr] == b'e' || str_bytes[endptr] == b'E') {
        // strtol(endptr + 1, &cp, 10).
        let cp = scan_strtol_end(str_bytes, endptr + 1);
        if cp > endptr + 1 {
            endptr = cp;
        }
    }

    // Parse the number; the next byte may be the first char of the unit.
    //   num = numeric_in(strptr, InvalidOid, -1);
    let number_str =
        std::str::from_utf8(&str_bytes[strptr..endptr]).map_err(|_| invalid_size_lossy(arg))?;
    let mut num: Vec<u8> = backend_utils_adt_numeric::io::numeric_in(mcx, number_str, -1)?.to_vec();

    // Skip whitespace between number and unit.
    let mut strptr = endptr;
    while strptr < str_bytes.len() && is_space(str_bytes[strptr]) {
        strptr += 1;
    }

    // Handle possible unit.
    if strptr < str_bytes.len() && str_bytes[strptr] != 0 {
        // Trim any trailing whitespace: endptr = str + VARSIZE_ANY_EXHDR - 1;
        // while (isspace(*endptr)) endptr--; endptr++; *endptr = '\0';
        let mut tail = str_bytes.len();
        while tail > 0 && is_space(str_bytes[tail - 1]) {
            tail -= 1;
        }
        let unit_str = &str_bytes[strptr..tail];

        // for (unit = size_pretty_units; unit->name != NULL; unit++)
        //     if (pg_strcasecmp(strptr, unit->name) == 0) break;
        let mut unit_index: Option<usize> = None;
        for (idx, unit) in SIZE_PRETTY_UNITS.iter().enumerate() {
            let Some(name) = unit.name else { break };
            if pg_strcasecmp(unit_str, name.as_bytes()) == 0 {
                unit_index = Some(idx);
                break;
            }
        }

        // If not found, look in table of aliases.
        if unit_index.is_none() {
            for a in SIZE_BYTES_ALIASES {
                let Some(alias) = a.alias else { break };
                if pg_strcasecmp(unit_str, alias.as_bytes()) == 0 {
                    unit_index = Some(a.unit_index);
                    break;
                }
            }
        }

        // Verify we found a valid unit in the loop above.
        let Some(unit_index) = unit_index else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid size: \"{}\"", lossy(arg)))
                .errdetail(format!("Invalid size unit: \"{}\".", lossy(unit_str)))
                .errhint(
                    "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\".",
                )
                .into_error());
        };

        let multiplier: i64 = 1i64 << SIZE_PRETTY_UNITS[unit_index].unitbits;

        if multiplier > 1 {
            let mul_num = int64_to_numeric(mcx, multiplier)?;
            num = backend_utils_adt_numeric::ops_sql::numeric_mul(mcx, &mul_num, &num)?.to_vec();
        }
    }

    // result = DatumGetInt64(numeric_int8(num));
    numeric_int8(mcx, &num)
}

/// `ereport(ERROR, errcode(22023), errmsg("invalid size: \"%s\"", str))`.
fn invalid_size(str_: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("invalid size: \"{str_}\""))
        .into_error()
}

fn invalid_size_lossy(arg: &[u8]) -> PgError {
    invalid_size(&lossy(arg))
}

/// `text_to_cstring(arg)` for an error message: a lossy UTF-8 view of the bytes.
fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `isspace((unsigned char) c)` for the bytes `pg_size_bytes` skips.
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// `isdigit((unsigned char) c)`.
fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

/// `pg_strcasecmp(s1, s2)` (`src/port/pgstrcasecmp.c`) over NUL-free byte slices.
fn pg_strcasecmp(s1: &[u8], s2: &[u8]) -> i32 {
    let mut i = 0usize;
    loop {
        let ch1 = if i < s1.len() { s1[i] } else { 0 };
        let ch2 = if i < s2.len() { s2[i] } else { 0 };
        if ch1 != ch2 {
            let l1 = ascii_tolower(ch1);
            let l2 = ascii_tolower(ch2);
            if l1 != l2 {
                return l1 as i32 - l2 as i32;
            }
        }
        if ch1 == 0 {
            break;
        }
        i += 1;
    }
    0
}

/// Lower-case an ASCII upper-case letter, leaving everything else untouched.
fn ascii_tolower(c: u8) -> u8 {
    if c.is_ascii_uppercase() {
        c + (b'a' - b'A')
    } else {
        c
    }
}

/// Mirror C's `strtol(s + start, &cp, 10)` end-pointer computation.
fn scan_strtol_end(bytes: &[u8], start: usize) -> usize {
    let mut p = start;
    while p < bytes.len() && is_space(bytes[p]) {
        p += 1;
    }
    if p < bytes.len() && (bytes[p] == b'+' || bytes[p] == b'-') {
        p += 1;
    }
    let mut any = false;
    while p < bytes.len() && is_digit(bytes[p]) {
        any = true;
        p += 1;
    }
    if any {
        p
    } else {
        start
    }
}

// ===========================================================================
// filenode / filepath (dbsize.c:882-1046).
// ===========================================================================

/// `pg_relation_filenode()` (dbsize.c:896).
pub fn pg_relation_filenode(relid: Oid) -> PgResult<Option<Oid>> {
    // tuple = SearchSysCache1(RELOID, ...); if (!HeapTupleIsValid) return NULL.
    let Some(relform) = pg_class_form::call(relid)? else {
        return Ok(None);
    };

    let result: RelFileNumber = if RELKIND_HAS_STORAGE(relform.relkind) {
        if relform.relfilenode != 0 {
            relform.relfilenode
        } else {
            // Consult the relation mapper.
            relation_map_oid_to_filenumber::call(relid, relform.relisshared)?
        }
    } else {
        // no storage, return NULL.
        InvalidRelFileNumber
    };

    if !RelFileNumberIsValid(result) {
        return Ok(None);
    }

    Ok(Some(result))
}

/// `pg_filenode_relation()` (dbsize.c:947).
pub fn pg_filenode_relation(
    reltablespace: Oid,
    relfilenumber: RelFileNumber,
) -> PgResult<Option<Oid>> {
    // test needed so RelidByRelfilenumber doesn't misbehave.
    if !RelFileNumberIsValid(relfilenumber) {
        return Ok(None);
    }

    let heaprel = relid_by_relfilenumber::call(reltablespace, relfilenumber)?;

    if !OidIsValid(heaprel) {
        Ok(None)
    } else {
        Ok(Some(heaprel))
    }
}

/// `pg_relation_filepath()` (dbsize.c:971).
pub fn pg_relation_filepath(relid: Oid) -> PgResult<Option<String>> {
    // tuple = SearchSysCache1(RELOID, ...); if (!HeapTupleIsValid) return NULL.
    let Some(relform) = pg_class_form::call(relid)? else {
        return Ok(None);
    };

    let spc_oid: Oid;
    let db_oid: Oid;
    let rel_number: RelFileNumber;

    if RELKIND_HAS_STORAGE(relform.relkind) {
        // This logic should match RelationInitPhysicalAddr.
        if relform.reltablespace != 0 {
            spc_oid = relform.reltablespace;
        } else {
            spc_oid = my_database_tablespace::call();
        }
        if spc_oid == GLOBALTABLESPACE_OID {
            db_oid = InvalidOid;
        } else {
            db_oid = my_database_id::call();
        }
        if relform.relfilenode != 0 {
            rel_number = relform.relfilenode;
        } else {
            // Consult the relation mapper.
            rel_number = relation_map_oid_to_filenumber::call(relid, relform.relisshared)?;
        }
    } else {
        // no storage, return NULL.
        rel_number = InvalidRelFileNumber;
        db_oid = InvalidOid;
        spc_oid = InvalidOid;
    }

    if !RelFileNumberIsValid(rel_number) {
        return Ok(None);
    }

    // Determine owning backend.
    let backend: ProcNumber = if relform.relpersistence == RELPERSISTENCE_UNLOGGED
        || relform.relpersistence == RELPERSISTENCE_PERMANENT
    {
        INVALID_PROC_NUMBER
    } else if relform.relpersistence == RELPERSISTENCE_TEMP {
        if is_temp_or_temp_toast_namespace::call(relform.relnamespace)? {
            proc_number_for_temp_relations::call()
        } else {
            // Do it the hard way.
            let backend = get_temp_namespace_proc_number::call(relform.relnamespace)?;
            debug_assert!(backend != INVALID_PROC_NUMBER);
            backend
        }
    } else {
        // elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal(format!(
                "invalid relpersistence: {}",
                (relform.relpersistence as u8) as char
            ))
            .into_error());
    };

    // path = relpathbackend(rlocator, backend, MAIN_FORKNUM);
    let path = backend_common_relpath::relpathbackend(
        RelFileLocator { spcOid: spc_oid, dbOid: db_oid, relNumber: rel_number },
        backend,
        ForkNumber::MAIN_FORKNUM,
    );

    Ok(Some(path))
}

// ===========================================================================
// Outbound-seam provider bodies (relation-open family + stat).
// ===========================================================================

/// `try_relation_open(relOid, AccessShareLock)` provider.
///
/// Open the relation through the real `relation_open` family over a scratch
/// `MemoryContext` the seam contract does not carry, project the on-disk address
/// + toast/index metadata `dbsize.c` reads into the flat [`OpenRelation`]
/// carrier, then let the [`Relation`] handle drop.
///
/// The handle's `Drop` releases only the relcache reference with `NoLock`,
/// leaving the `AccessShareLock` (transaction-scoped, taken with `.keep()` by
/// the open) held until [`relation_close_provider`] releases it — exactly C's
/// split between `try_relation_open` (takes lock + pin) and
/// `relation_close(rel, AccessShareLock)` (drops both). The relcache pin is not
/// needed across the gap because the size routines consult only the copied-out
/// fields, and the lock keeps the relation from being dropped concurrently.
fn try_relation_open_provider(rel_oid: Oid) -> PgResult<Option<OpenRelation>> {
    let scratch = mcx::MemoryContext::new("dbsize try_relation_open");
    let mcx = scratch.mcx();
    let opened = backend_access_common_relation::try_relation_open(
        mcx,
        rel_oid,
        types_storage::lock::AccessShareLock,
    )?;
    Ok(opened.map(|rel| project_open_relation(&rel)))
}

/// `relation_open(relOid, AccessShareLock)` provider — must succeed (errors if
/// the relation cannot be opened). See [`try_relation_open_provider`] for the
/// lock/pin split.
fn relation_open_provider(rel_oid: Oid) -> PgResult<OpenRelation> {
    let scratch = mcx::MemoryContext::new("dbsize relation_open");
    let mcx = scratch.mcx();
    let rel = backend_access_common_relation::relation_open(
        mcx,
        rel_oid,
        types_storage::lock::AccessShareLock,
    )?;
    Ok(project_open_relation(&rel))
}

/// Project a live [`Relation`] into the flat [`OpenRelation`] carrier the size
/// routines consult: `rd_locator.{spcOid,dbOid,relNumber}`, `rd_backend`, and
/// `rd_rel.{reltoastrelid,relhasindex}`.
fn project_open_relation(rel: &types_rel::Relation<'_>) -> OpenRelation {
    OpenRelation {
        spc_oid: rel.rd_locator.spcOid,
        db_oid: rel.rd_locator.dbOid,
        rel_number: rel.rd_locator.relNumber,
        backend: rel.rd_backend,
        reltoastrelid: rel.rd_rel.reltoastrelid,
        relhasindex: rel.rd_rel.relhasindex,
    }
}

/// `relation_close(rel, AccessShareLock)` provider: release the
/// `AccessShareLock` the matching open took. The relcache reference was already
/// released when the open's [`Relation`] handle dropped (with `NoLock`), so this
/// only re-derives the relation lock tag from the OID and unlocks it — mirroring
/// C's `UnlockRelationId(&relid, AccessShareLock)` leg of `relation_close`.
fn relation_close_provider(rel_oid: Oid) -> PgResult<()> {
    backend_storage_lmgr_lmgr_seams::unlock_relation_oid::call(
        rel_oid,
        types_storage::lock::AccessShareLock,
    )
}

/// `stat(path, &fst)` provider: `stat()` over the relation segment path
/// (relative to the data-directory cwd the backend runs in, exactly as C's bare
/// `stat()`), discriminated by `errno` so the caller replicates C's
/// `errno == ENOENT` (vanished file → stop the segment walk) vs. other-`errno`
/// (`could not stat file` error) branches. Follows symlinks, like `stat(2)`.
fn stat_provider(path: &str) -> StatResult {
    match std::fs::metadata(path) {
        Ok(md) => StatResult::Ok(FileStat {
            size: md.len() as i64,
            is_dir: md.is_dir(),
        }),
        Err(e) => match e.raw_os_error() {
            Some(errno) if errno == libc::ENOENT => StatResult::NotFound,
            Some(errno) => StatResult::Error { errno },
            // No OS errno (should not happen for fs::metadata); treat as the
            // generic file-access failure so the caller raises the file error.
            None => StatResult::Error { errno: libc::EIO },
        },
    }
}

// ===========================================================================
// Seam install + fmgr builtin registration.
// ===========================================================================

/// Register the `dbsize.c` fmgr builtins (so `fmgr_isbuiltin` resolves them on
/// the fast path). This crate owns no inward seams — its outbound seams are
/// installed by their owners — so registration is the only init work here.
/// Invoked by `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_dbsize_builtins();

    // Install the value-typed, acyclic outbound seams to their real owners.
    my_database_id::set(backend_utils_init_small::globals::MyDatabaseId);
    my_database_tablespace::set(backend_utils_init_small::globals::MyDatabaseTableSpace);
    get_user_id::set(|| Ok(backend_utils_init_miscinit::GetUserId()));
    has_privs_of_role::set(backend_utils_adt_acl::role_membership::has_privs_of_role);
    is_temp_or_temp_toast_namespace::set(
        backend_catalog_namespace::isTempOrTempToastNamespace,
    );
    proc_number_for_temp_relations::set(
        backend_catalog_storage::proc_number_for_temp_relations,
    );
    check_for_interrupts::set(backend_tcop_postgres::interrupt::check_for_interrupts);

    // Relation-open family over AccessShareLock (the table/index size routines).
    try_relation_open::set(try_relation_open_provider);
    relation_open::set(relation_open_provider);
    relation_close::set(relation_close_provider);

    // RelationGetIndexList(rel) — the index OIDs for the indexes/total-size
    // walks (takes the relation OID directly).
    relation_get_index_list::set(backend_utils_cache_relcache::derived::RelationGetIndexList);

    // SearchSysCache1(RELOID, ...) -> the Form_pg_class columns pg_relation_filenode
    // / pg_relation_filepath read, mapped into the dbsize PgClassForm carrier.
    pg_class_form::set(|relid| {
        Ok(backend_utils_cache_syscache::pg_class_form_dbsize(relid)?.map(
            |(relkind, relfilenode, relisshared, reltablespace, relnamespace, relpersistence)| {
                PgClassForm {
                    relkind,
                    relfilenode,
                    relisshared,
                    reltablespace,
                    relnamespace,
                    relpersistence,
                }
            },
        ))
    });

    // RelationMapOidToFilenumber(relid, relisshared) — the relation mapper
    // (infallible in C; wrapped Ok for the seam's PgResult contract).
    relation_map_oid_to_filenumber::set(|relid, relisshared| {
        Ok(backend_utils_cache_relmapper::RelationMapOidToFilenumber(relid, relisshared))
    });

    // RelidByRelfilenumber(reltablespace, relfilenumber) — the relfilenumber map
    // reverse lookup. C reads MyDatabaseTableSpace inside; the owner takes it as
    // an explicit arg, so supply it from the backend global here.
    relid_by_relfilenumber::set(|reltablespace, relfilenumber| {
        backend_utils_cache_relfilenumbermap::RelidByRelfilenumber(
            reltablespace,
            relfilenumber,
            backend_utils_init_small::globals::MyDatabaseTableSpace(),
        )
    });

    // stat(path) over the runtime filesystem (relative to the backend's
    // data-directory cwd, like C's bare stat()).
    stat::set(stat_provider);

    // NOTE: `read_dir` stays uninstalled. It is the AllocateDir/ReadDir/FreeDir
    // directory-walk substrate used only by pg_database_size / pg_tablespace_size
    // (not by pg_relation_size or any other relation-size routine); modelling the
    // eager AllocateDir-or-NULL + skip-"."/".." walk faithfully needs the fd.c
    // directory-descriptor lane, which is out of scope here.
}

#[cfg(test)]
mod tests;
