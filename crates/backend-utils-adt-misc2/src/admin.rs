//! Family `admin` — the SRF / system-administration glue files:
//! `genfile.c` + `hbafuncs.c` + `lockfuncs.c` + `partitionfuncs.c` +
//! `pg_upgrade_support.c`.
//!
//! These are grouped because they share the same shape: set-returning or
//! privileged SQL functions that are thin glue over substrate owned by other
//! units, which they reach through seams in those real owners. This unit's own
//! logic (the path policy, the read loop's clamps/variants, the advisory
//! locktag construction, the partition level/parent walk, the per-column value
//! mapping) is written out; only genuinely-unported owners are seam-and-panic:
//!
//! * the `common/path.c` data-directory file-access policy + `DataDir` /
//!   `Log_directory` globals + `GetUserId` (→ `backend-common-path-seams`),
//! * the `storage/file/fd.c` stdio file / directory walk (→ fd seams),
//! * the value-per-call SRF (`FuncCallContext`) machinery and record forming
//!   (→ funcapi seams; the materialize-mode tuplestore path is modeled),
//! * the `lock.c` lock-status snapshot + `lock_acquire`/`lock_release`/
//!   `lock_release_session` (→ lmgr-lock seams),
//! * `catalog/partition.c` + `pg_inherits.c` inheritance walks (→ partition
//!   seams), `lsyscache.c` / `syscache.c` relkind/exists lookups,
//! * the genuinely-unported `libpq/hba.c` parser (→ hba seams),
//! * the genuinely-unported binary-upgrade catalog state owners (→
//!   binary-upgrade seams).
//!
//! Functions that allocate result text / records take `Mcx` and surface
//! `ereport`s as `PgResult`. SRFs take the call's
//! [`FunctionCallInfoBaseData`]. Independent of the keystone.

use alloc::format;
use mcx::Mcx;
// The canonical unified value (Datum-unification keystone); these SQL-callable
// builtins return / assemble the unified `Datum<'mcx>`. The varlena owner still
// hands back the bare scalar word, bridged at `text_datum`/`bytes_to_varlena`.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_CANT_CHANGE_RUNTIME_PARAM, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_storage::lock as lk;

use backend_utils_adt_varlena_seams as varlena;

use backend_catalog_binary_upgrade_seams as binup;
use backend_common_path_seams as path;
use backend_libpq_hba_seams as hba;
use backend_storage_file_fd_seams as fd;
use backend_storage_lmgr_lock_seams as lock;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_replication_slot_seams as slot;

// Well-known catalog OIDs the C names via pg_type_d.h / catalog/*_d.h #defines.
// These are genfile.c's / lockfuncs.c's own knowledge of their output columns.
const INT8OID: u32 = 20;
const BOOLOID: u32 = 16;
const TIMESTAMPTZOID: u32 = 1184;
/// `DEFAULTTABLESPACE_OID` (`catalog/pg_tablespace_d.h`) — pg_default.
const DEFAULTTABLESPACE_OID: u32 = 1663;

/// `RELKIND_HAS_PARTITIONS(relkind)` (`catalog/pg_class.h`) — true for a
/// partitioned table or partitioned index.
fn relkind_has_partitions(relkind: u8) -> bool {
    relkind == types_tuple::access::RELKIND_PARTITIONED_TABLE
        || relkind == types_tuple::access::RELKIND_PARTITIONED_INDEX
}

// =====================================================================
// genfile.c
// =====================================================================

/// `read_binary_file(filename, seek_offset, bytes_to_read, missing_ok)`
/// (genfile.c): read a section of an already-validated server file as raw
/// bytes. The `AllocateFile`/`fseeko`/`fread` stdio machinery is fd-owned (the
/// `requested length too large` clamp and the whole-file growth loop with its
/// `file length too large` limit live behind the seam, faithful to C). Returns
/// `None` for the C `missing_ok && errno == ENOENT` case.
fn read_binary_file<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    missing_ok: bool,
) -> PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    fd::read_server_file::call(mcx, filename, seek_offset, bytes_to_read, missing_ok)
}

/// `read_text_file(...)` (genfile.c): as [`read_binary_file`], then verify the
/// bytes are valid in the database encoding (`pg_verifymbstr`).
fn read_text_file<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    missing_ok: bool,
) -> PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    match read_binary_file(mcx, filename, seek_offset, bytes_to_read, missing_ok)? {
        Some(buf) => {
            // pg_verifymbstr(VARDATA(buf), VARSIZE(buf) - VARHDRSZ, false)
            backend_utils_mb_mbutils_seams::pg_verifymbstr::call(&buf[..], false)?;
            Ok(Some(buf))
        }
        None => Ok(None),
    }
}

/// `pg_read_file_common(filename, seek_offset, bytes_to_read, read_to_eof,
/// missing_ok)` (genfile.c).
fn pg_read_file_common<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    read_to_eof: bool,
    missing_ok: bool,
) -> PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    if read_to_eof {
        debug_assert_eq!(bytes_to_read, -1);
    } else if bytes_to_read < 0 {
        return Err(PgError::new(ERROR, "requested length cannot be negative")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let filename = path::convert_and_check_filename::call(mcx, filename)?;
    read_text_file(mcx, filename.as_str(), seek_offset, bytes_to_read, missing_ok)
}

/// `pg_read_binary_file_common(...)` (genfile.c): parameters as
/// [`pg_read_file_common`], without the encoding verification.
fn pg_read_binary_file_common<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    read_to_eof: bool,
    missing_ok: bool,
) -> PgResult<Option<mcx::PgVec<'mcx, u8>>> {
    if read_to_eof {
        debug_assert_eq!(bytes_to_read, -1);
    } else if bytes_to_read < 0 {
        return Err(PgError::new(ERROR, "requested length cannot be negative")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let filename = path::convert_and_check_filename::call(mcx, filename)?;
    read_binary_file(mcx, filename.as_str(), seek_offset, bytes_to_read, missing_ok)
}

/// Wrap the read result as a `text`/`bytea` `Datum`, or NULL when the file was
/// missing (`PG_RETURN_NULL`). The varlena payload is the raw bytes with a
/// `VARHDRSZ` header — `bytea` and `text` share the representation.
fn bytes_to_varlena_datum<'mcx>(
    mcx: Mcx<'mcx>,
    bytes: Option<mcx::PgVec<'mcx, u8>>,
) -> PgResult<Datum<'mcx>> {
    match bytes {
        Some(b) => Ok(varlena::bytes_to_varlena_v::call(mcx, &b[..])?),
        None => Ok(Datum::null()),
    }
}

/// `CStringGetTextDatum(s)` — a `text` `Datum` from a Rust string, via the
/// varlena owner.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    // `text` is pass-by-reference; the `_v` seam variant returns a
    // `Datum::ByRef` varlena directly.
    Ok(varlena::cstring_to_text_v::call(mcx, s)?)
}

/// `pg_read_file_off_len(filename, offset, length)` (genfile.c).
pub fn pg_read_file_off_len<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    offset: i64,
    length: i64,
) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_file_common(mcx, filename, offset, length, false, false)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_file_off_len_missing(filename, offset, length, missing_ok)`.
pub fn pg_read_file_off_len_missing<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    offset: i64,
    length: i64,
    missing_ok: bool,
) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_file_common(mcx, filename, offset, length, false, missing_ok)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_file_all(filename)`.
pub fn pg_read_file_all<'mcx>(mcx: Mcx<'mcx>, filename: &str) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_file_common(mcx, filename, 0, -1, true, false)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_file_all_missing(filename, missing_ok)`.
pub fn pg_read_file_all_missing<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    missing_ok: bool,
) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_file_common(mcx, filename, 0, -1, true, missing_ok)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_binary_file_off_len(filename, offset, length)`.
pub fn pg_read_binary_file_off_len<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    offset: i64,
    length: i64,
) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_binary_file_common(mcx, filename, offset, length, false, false)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_binary_file_off_len_missing(filename, offset, length, missing_ok)`.
pub fn pg_read_binary_file_off_len_missing<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    offset: i64,
    length: i64,
    missing_ok: bool,
) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_binary_file_common(mcx, filename, offset, length, false, missing_ok)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_binary_file_all(filename)`.
pub fn pg_read_binary_file_all<'mcx>(mcx: Mcx<'mcx>, filename: &str) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_binary_file_common(mcx, filename, 0, -1, true, false)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_read_binary_file_all_missing(filename, missing_ok)`.
pub fn pg_read_binary_file_all_missing<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    missing_ok: bool,
) -> PgResult<Datum<'mcx>> {
    let ret = pg_read_binary_file_common(mcx, filename, 0, -1, true, missing_ok)?;
    bytes_to_varlena_datum(mcx, ret)
}

/// `pg_stat_file(filename [, missing_ok])` (genfile.c): the 6-column record.
/// `two_args` is the C `PG_NARGS() == 2`. Returns NULL when the file is missing
/// with `missing_ok`.
pub fn pg_stat_file<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &str,
    missing_ok: bool,
    _two_args: bool,
) -> PgResult<Datum<'mcx>> {
    let filename = path::convert_and_check_filename::call(mcx, filename)?;

    let st = match fd::stat_file::call(filename.as_str(), missing_ok)? {
        Some(st) => st,
        // missing_ok && errno == ENOENT -> PG_RETURN_NULL()
        None => return Ok(Datum::null()),
    };

    // This record type had better match the output parameters declared for me
    // in pg_proc.h: (size int8, access/modification/change/creation timestamptz,
    // isdir bool). On Unix, "creation" (col 5) is NULL and "change" (col 4) is
    // st_ctime.
    let coltypes = [
        INT8OID,        // size
        TIMESTAMPTZOID, // access
        TIMESTAMPTZOID, // modification
        TIMESTAMPTZOID, // change
        TIMESTAMPTZOID, // creation
        BOOLOID,        // isdir
    ];
    let values = [
        Datum::from_i64(st.size),
        Datum::from_i64(st.access),
        Datum::from_i64(st.modification),
        Datum::from_i64(st.change),
        Datum::null(),
        Datum::from_bool(st.isdir),
    ];
    // #if !defined(WIN32): isnull[4] = true (creation), change valued.
    let nulls = [false, false, false, false, true, false];

    funcapi::record_from_values::call(mcx, &coltypes, &values, &nulls)
}

/// `pg_stat_file_1arg(filename)` (genfile.c) — the one-argument variant. C is a
/// pure fmgr wrapper (`return pg_stat_file(fcinfo)`) that exists only to satisfy
/// the `opr_sanity` check requiring built-ins sharing an implementing C function
/// to take the same number of arguments. With a single argument `PG_NARGS() != 2`
/// so `missing_ok` is `false`.
pub fn pg_stat_file_1arg<'mcx>(mcx: Mcx<'mcx>, filename: &str) -> PgResult<Datum<'mcx>> {
    pg_stat_file(mcx, filename, false, false)
}

/// `pg_ls_dir(dirname [, missing_ok, include_dot_dirs])` (genfile.c) — a
/// materialized SRF of file names.
pub fn pg_ls_dir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    dirname: &str,
    missing_ok: bool,
    include_dot_dirs: bool,
) -> PgResult<Datum<'mcx>> {
    let location = path::convert_and_check_filename::call(mcx, dirname)?;

    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    let entries = match fd::list_dir::call(mcx, location.as_str(), missing_ok)? {
        // Return empty tuplestore if appropriate (missing_ok && ENOENT)
        None => return Ok(Datum::null()),
        Some(e) => e,
    };

    for de in entries.iter() {
        if !include_dot_dirs && (de.name.as_str() == "." || de.name.as_str() == "..") {
            continue;
        }
        let values = [text_datum(mcx, de.name.as_str())?];
        let nulls = [false];
        funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    Ok(Datum::null())
}

/// `pg_ls_dir_1arg(dirname)` (genfile.c) — the one-argument variant. C is a pure
/// fmgr wrapper (`return pg_ls_dir(fcinfo)`) existing only for the `opr_sanity`
/// same-arity check. With a single argument `PG_NARGS() != 3` so both
/// `missing_ok` and `include_dot_dirs` are `false`.
pub fn pg_ls_dir_1arg<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    dirname: &str,
) -> PgResult<Datum<'mcx>> {
    pg_ls_dir(mcx, fcinfo, dirname, false, false)
}

/// `pg_ls_dir_files(fcinfo, dir, missing_ok)` (genfile.c): the generic
/// directory listing — (name text, size int8, modification timestamptz),
/// skipping hidden files and anything but regular files.
fn pg_ls_dir_files<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    dir: &str,
    missing_ok: bool,
) -> PgResult<Datum<'mcx>> {
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    let entries = match fd::list_dir::call(mcx, dir, missing_ok)? {
        None => return Ok(Datum::null()),
        Some(e) => e,
    };

    for de in entries.iter() {
        // Skip hidden files (d_name[0] == '.')
        if de.name.as_str().starts_with('.') {
            continue;
        }
        // Ignore anything but regular files (and concurrently-deleted files,
        // which list_dir already drops via the per-entry stat).
        if !de.isreg {
            continue;
        }
        let values = [
            text_datum(mcx, de.name.as_str())?,
            Datum::from_i64(de.size),
            Datum::from_i64(de.modification),
        ];
        let nulls = [false, false, false];
        funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    Ok(Datum::null())
}

/// `pg_ls_logdir()` — files in the log directory.
pub fn pg_ls_logdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let dir = path::wal_or_log_subdir::call(mcx, path::WellKnownDir::LogDir);
    pg_ls_dir_files(mcx, fcinfo, dir.as_str(), false)
}

/// `pg_ls_waldir()` — files in the WAL directory.
pub fn pg_ls_waldir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let dir = path::wal_or_log_subdir::call(mcx, path::WellKnownDir::WalDir);
    pg_ls_dir_files(mcx, fcinfo, dir.as_str(), false)
}

/// `pg_ls_tmpdir(fcinfo, tblspc)` (genfile.c): the `pgsql_tmp` directory of the
/// given tablespace; errors if the tablespace OID does not exist.
fn pg_ls_tmpdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    tblspc: u32,
) -> PgResult<Datum<'mcx>> {
    if !syscache::tablespace_exists::call(tblspc)? {
        return Err(PgError::new(
            ERROR,
            format!("tablespace with OID {tblspc} does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    let path = path::temp_tablespace_path::call(mcx, tblspc);
    pg_ls_dir_files(mcx, fcinfo, path.as_str(), true)
}

/// `pg_ls_tmpdir_noargs()` — pg_default tablespace's pgsql_tmp.
pub fn pg_ls_tmpdir_noargs<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    pg_ls_tmpdir(mcx, fcinfo, DEFAULTTABLESPACE_OID)
}

/// `pg_ls_tmpdir_1arg(tablespace)` — the given tablespace's pgsql_tmp.
pub fn pg_ls_tmpdir_1arg<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    tablespace: u32,
) -> PgResult<Datum<'mcx>> {
    pg_ls_tmpdir(mcx, fcinfo, tablespace)
}

/// `pg_ls_archive_statusdir()` — files in `pg_wal/archive_status`.
pub fn pg_ls_archive_statusdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let dir = path::wal_or_log_subdir::call(mcx, path::WellKnownDir::ArchiveStatusDir);
    pg_ls_dir_files(mcx, fcinfo, dir.as_str(), true)
}

/// `pg_ls_summariesdir()` — files in `pg_wal/summaries`.
pub fn pg_ls_summariesdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let dir = path::wal_or_log_subdir::call(mcx, path::WellKnownDir::SummariesDir);
    pg_ls_dir_files(mcx, fcinfo, dir.as_str(), true)
}

/// `pg_ls_logicalsnapdir()` — files in `pg_logical/snapshots`.
pub fn pg_ls_logicalsnapdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let dir = path::wal_or_log_subdir::call(mcx, path::WellKnownDir::LogicalSnapDir);
    pg_ls_dir_files(mcx, fcinfo, dir.as_str(), false)
}

/// `pg_ls_logicalmapdir()` — files in `pg_logical/mappings`.
pub fn pg_ls_logicalmapdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let dir = path::wal_or_log_subdir::call(mcx, path::WellKnownDir::LogicalMapDir);
    pg_ls_dir_files(mcx, fcinfo, dir.as_str(), false)
}

/// `pg_ls_replslotdir(slot_name)` — files in `pg_replslot/<slot_name>`; errors
/// if the slot does not exist.
pub fn pg_ls_replslotdir<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    slotname: &str,
) -> PgResult<Datum<'mcx>> {
    if !slot::SearchNamedReplicationSlot::call(slotname, true)? {
        return Err(PgError::new(
            ERROR,
            format!("replication slot \"{slotname}\" does not exist"),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    // snprintf(path, "%s/%s", PG_REPLSLOT_DIR, slotname)
    let mut p = alloc::string::String::from("pg_replslot/");
    p.push_str(slotname);
    pg_ls_dir_files(mcx, fcinfo, &p, false)
}

// =====================================================================
// hbafuncs.c
// =====================================================================

/// `pg_hba_file_rules(fcinfo)` — materialized SRF over the parsed pg_hba.conf
/// lines. The `Materialize` mode init is this unit's own glue; the view fill
/// (the genuinely-unported hba.c parser) crosses the hba seam.
pub fn pg_hba_file_rules<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");
    hba::fill_hba_view::call(rsinfo)?;
    Ok(Datum::null())
}

/// `pg_ident_file_mappings(fcinfo)` — materialized SRF over the parsed
/// pg_ident.conf maps.
pub fn pg_ident_file_mappings<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");
    hba::fill_ident_view::call(rsinfo)?;
    Ok(Datum::null())
}

// =====================================================================
// lockfuncs.c
// =====================================================================

/// `LockTagTypeNames[]` (lockfuncs.c) — must match `enum LockTagType`.
const LOCK_TAG_TYPE_NAMES: [&str; (lk::LOCKTAG_LAST_TYPE as usize) + 1] = [
    "relation",
    "extend",
    "frozenid",
    "page",
    "tuple",
    "transactionid",
    "virtualxid",
    "spectoken",
    "object",
    "userlock",
    "advisory",
    "applytransaction",
];

/// Number of columns in pg_locks output.
const NUM_LOCK_STATUS_COLUMNS: usize = 16;

/// `pg_lock_status(fcinfo)` (lockfuncs.c) — one row per held or awaited lock
/// mode. C uses a value-per-call SRF (`FuncCallContext`) which is funcapi-owned
/// and not yet modeled; the snapshot + per-column mapping below is this unit's
/// own logic, but the value-SRF emission protocol crosses the (genuinely
/// unported) funcapi seam.
///
/// The predicate (SIREAD) lock rows additionally require
/// `GetPredicateLockStatusData` (predicate.c, genuinely unported); they are
/// produced by the same value-SRF owner once it lands.
pub fn pg_lock_status<'mcx>(
    _mcx: Mcx<'mcx>,
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // `pg_lock_status` is a set-returning function whose by-OID `fcinfo` frame's
    // `resultinfo` is tag-only (cannot carry the live `ReturnSetInfo`). It is
    // driven over the executor frame (materialize mode) from
    // `backend-executor-execSRF`'s `pg_lock_status` wrapper, which calls
    // [`pg_lock_status_rows`] below and fills the executor's tuplestore — exactly
    // the dual-home arrangement `pg_prepared_xact` / `generate_series` use. This
    // by-OID entry point is therefore never the SRF driver; route any direct
    // by-OID call (there is none for a retset builtin) to the funcapi boundary.
    funcapi::value_srf_unported::call();
    unreachable!("pg_lock_status is driven over the executor frame via pg_lock_status_rows")
}

/// One projected `pg_locks` row: the 16 column Datums + their null flags, in the
/// `(locktype, database, relation, page, tuple, virtualxid, transactionid,
/// classid, objid, objsubid, virtualtransaction, pid, mode, granted, fastpath,
/// waitstart)` order lockfuncs.c lays out.
pub type LockStatusRow<'mcx> =
    ([Datum<'mcx>; NUM_LOCK_STATUS_COLUMNS], [bool; NUM_LOCK_STATUS_COLUMNS]);

/// `pg_lock_status` (lockfuncs.c) row producer — the whole-function projection,
/// hoisted out of the value-per-call SRF protocol so the executor-frame wrapper
/// can drive it in materialize mode.
///
/// Faithfully reproduces the C per-call series as a flat row list: for each
/// `LockInstanceData` from `GetLockStatusData()`, emit one row per *held* lock
/// mode (the C `holdMask` walk that destructively clears each reported bit), then
/// — if the PROCLOCK is waiting — one final row for `waitLockMode`; PROCLOCKs
/// that are neither holding nor waiting contribute no rows. The SIREAD predicate
/// locks from `GetPredicateLockStatusData()` are appended after the regular
/// locks, exactly as C does.
pub fn pg_lock_status_rows<'mcx>(mcx: Mcx<'mcx>) -> PgResult<alloc::vec::Vec<LockStatusRow<'mcx>>> {
    let mut rows: alloc::vec::Vec<LockStatusRow<'mcx>> = alloc::vec::Vec::new();

    // C: mystatus->lockData = GetLockStatusData(); the snapshot of every PROCLOCK.
    let lock_data = lock::get_lock_status_data::call(mcx)?;

    for instance in lock_data.iter() {
        // The C code mutates `instance->holdMask` in place across calls; here we
        // copy the mask and walk it locally to enumerate every held mode.
        let mut hold_mask = instance.holdMask;

        // One row per held lock mode (C: the `holdMask & LOCKBIT_ON(mode)` walk,
        // breaking after each bit, clearing it, until none remain).
        loop {
            let mut granted = false;
            let mut mode: lk::LOCKMODE = 0;
            if hold_mask != 0 {
                let mut m: lk::LOCKMODE = 0;
                while (m as usize) < lk::MAX_LOCKMODES {
                    if hold_mask & lk::LOCKBIT_ON(m) != 0 {
                        granted = true;
                        hold_mask &= lk::LOCKBIT_OFF(m);
                        mode = m;
                        break;
                    }
                    m += 1;
                }
            }

            if granted {
                rows.push(fill_lock_row(mcx, instance, true, mode)?);
                continue;
            }

            // C: if !granted, report the waited-for mode (if any) once, else stop.
            if instance.waitLockMode != lk::NoLock {
                rows.push(fill_lock_row(mcx, instance, false, instance.waitLockMode)?);
            }
            break;
        }
    }

    // C: SIREAD predicate locks, appended after the regular locks. The decode +
    // projection is predicate.c-internal (its target-tag macros + xact fields);
    // the seam yields each already-projected SIREAD row's scalar fields.
    let pred_rows = predicate::predicate_lock_status_rows::call(mcx)?;
    for pr in pred_rows.iter() {
        rows.push(fill_predicate_lock_row(mcx, pr)?);
    }

    Ok(rows)
}

/// Project one SIREAD predicate-lock row (lockfuncs.c's predicate leg). All
/// predicate locks are `SIReadLock`: always held, never waiting, no fast path,
/// no waitstart. The target fields (db/relation/page/tuple) and holder
/// (vxid/pid) come pre-decoded from the predicate seam.
fn fill_predicate_lock_row<'mcx>(
    mcx: Mcx<'mcx>,
    pr: &lk::PredLockStatusRow,
) -> PgResult<LockStatusRow<'mcx>> {
    let mut values: [Datum<'mcx>; NUM_LOCK_STATUS_COLUMNS] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NUM_LOCK_STATUS_COLUMNS];

    // locktype = PredicateLockTagTypeNames[lockType]
    values[0] = text_datum(mcx, pr.locktypename.as_str())?;
    // database / relation
    values[1] = Datum::from_u32(pr.database);
    values[2] = Datum::from_u32(pr.relation);
    // page (TUPLE or PAGE) / tuple (TUPLE only)
    if pr.has_page {
        values[3] = Datum::from_u32(pr.page);
    } else {
        nulls[3] = true;
    }
    if pr.has_tuple {
        values[4] = Datum::from_u16(pr.tuple);
    } else {
        nulls[4] = true;
    }
    // virtualxid/transactionid/classid/objid/objsubid: not applicable.
    nulls[5] = true;
    nulls[6] = true;
    nulls[7] = true;
    nulls[8] = true;
    nulls[9] = true;
    // virtualtransaction = VXIDGetDatum(xact->vxid.procNumber, localXid)
    values[10] = vxid_datum(mcx, pr.proc_number, pr.local_xid)?;
    if pr.pid != 0 {
        values[11] = Datum::from_i32(pr.pid);
    } else {
        nulls[11] = true;
    }
    // mode = "SIReadLock"; granted = true; fastpath = false; waitstart NULL.
    values[12] = text_datum(mcx, "SIReadLock")?;
    values[13] = Datum::from_bool(true);
    values[14] = Datum::from_bool(false);
    nulls[15] = true;

    Ok((values, nulls))
}

/// Map one `LockInstanceData` to a `pg_locks` row (`values`/`nulls`). This is
/// lockfuncs.c's own column logic; shared by the value-SRF owner once it lands.
/// Kept here (not behind a seam) because it is this unit's logic; it consumes
/// only the `LockInstanceData` ABI (lock.h) and the `GetLockmodeName` seam.
#[allow(dead_code)]
fn fill_lock_row<'mcx>(
    mcx: Mcx<'mcx>,
    instance: &lk::LockInstanceData,
    granted: bool,
    mode: lk::LOCKMODE,
) -> PgResult<([Datum<'mcx>; NUM_LOCK_STATUS_COLUMNS], [bool; NUM_LOCK_STATUS_COLUMNS])> {
    let mut values: [Datum<'mcx>; NUM_LOCK_STATUS_COLUMNS] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NUM_LOCK_STATUS_COLUMNS];

    let tag = &instance.locktag;
    // locktypename = (type <= LAST) ? LockTagTypeNames[type] : "unknown N"
    let unknown;
    let locktypename: &str = if tag.locktag_type <= lk::LOCKTAG_LAST_TYPE {
        LOCK_TAG_TYPE_NAMES[tag.locktag_type as usize]
    } else {
        // snprintf(tnbuf, "unknown %d", (int) locktag_type)
        unknown = format!("unknown {}", tag.locktag_type);
        unknown.as_str()
    };
    values[0] = text_datum(mcx, locktypename)?;

    match tag.locktag_type {
        lk::LOCKTAG_RELATION | lk::LOCKTAG_RELATION_EXTEND => {
            values[1] = Datum::from_u32(tag.locktag_field1);
            values[2] = Datum::from_u32(tag.locktag_field2);
            for i in 3..=9 {
                nulls[i] = true;
            }
        }
        lk::LOCKTAG_DATABASE_FROZEN_IDS => {
            values[1] = Datum::from_u32(tag.locktag_field1);
            for i in 2..=9 {
                nulls[i] = true;
            }
        }
        lk::LOCKTAG_PAGE => {
            values[1] = Datum::from_u32(tag.locktag_field1);
            values[2] = Datum::from_u32(tag.locktag_field2);
            values[3] = Datum::from_u32(tag.locktag_field3);
            for i in 4..=9 {
                nulls[i] = true;
            }
        }
        lk::LOCKTAG_TUPLE => {
            values[1] = Datum::from_u32(tag.locktag_field1);
            values[2] = Datum::from_u32(tag.locktag_field2);
            values[3] = Datum::from_u32(tag.locktag_field3);
            values[4] = Datum::from_u16(tag.locktag_field4);
            for i in 5..=9 {
                nulls[i] = true;
            }
        }
        lk::LOCKTAG_TRANSACTION => {
            values[6] = Datum::from_transaction_id(tag.locktag_field1);
            nulls[1] = true;
            for i in 2..=5 {
                nulls[i] = true;
            }
            nulls[7] = true;
            nulls[8] = true;
            nulls[9] = true;
        }
        lk::LOCKTAG_VIRTUALTRANSACTION => {
            values[5] = vxid_datum(mcx, tag.locktag_field1 as i32, tag.locktag_field2)?;
            nulls[1] = true;
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[6] = true;
            nulls[7] = true;
            nulls[8] = true;
            nulls[9] = true;
        }
        lk::LOCKTAG_SPECULATIVE_TOKEN => {
            values[6] = Datum::from_transaction_id(tag.locktag_field1);
            values[8] = Datum::from_u32(tag.locktag_field2);
            nulls[1] = true;
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[7] = true;
            nulls[9] = true;
        }
        lk::LOCKTAG_APPLY_TRANSACTION => {
            values[1] = Datum::from_u32(tag.locktag_field1);
            values[8] = Datum::from_u32(tag.locktag_field2);
            values[6] = Datum::from_u32(tag.locktag_field3);
            values[9] = Datum::from_i16(tag.locktag_field4 as i16);
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[7] = true;
        }
        // LOCKTAG_OBJECT / LOCKTAG_USERLOCK / LOCKTAG_ADVISORY / default
        _ => {
            values[1] = Datum::from_u32(tag.locktag_field1);
            values[7] = Datum::from_u32(tag.locktag_field2);
            values[8] = Datum::from_u32(tag.locktag_field3);
            values[9] = Datum::from_i16(tag.locktag_field4 as i16);
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[6] = true;
        }
    }

    values[10] = vxid_datum(
        mcx,
        instance.vxid.procNumber,
        instance.vxid.localTransactionId,
    )?;
    if instance.pid != 0 {
        values[11] = Datum::from_i32(instance.pid);
    } else {
        nulls[11] = true;
    }
    let modename = lock::get_lockmode_name::call(tag.locktag_lockmethodid as u16, mode);
    values[12] = text_datum(mcx, modename.as_str())?;
    values[13] = Datum::from_bool(granted);
    values[14] = Datum::from_bool(instance.fastpath);
    if !granted && instance.waitStart != 0 {
        values[15] = Datum::from_i64(instance.waitStart);
    } else {
        nulls[15] = true;
    }

    Ok((values, nulls))
}

/// `VXIDGetDatum(procNumber, lxid)` (lockfuncs.c) — the "<procNumber>/<lxid>"
/// text representation of a VXID.
#[allow(dead_code)]
fn vxid_datum<'mcx>(mcx: Mcx<'mcx>, proc_number: i32, lxid: u32) -> PgResult<Datum<'mcx>> {
    // snprintf(vxidstr, "%d/%u", procNumber, lxid)
    text_datum(mcx, &format!("{proc_number}/{lxid}"))
}

/// `pg_blocking_pids(blocked_pid)` (lockfuncs.c) — int4[] of leader PIDs
/// blocking the given PID. The traversal is over `GetBlockerStatusData` plus
/// the lock-method conflict tables (lock.c-internal, genuinely unported), so
/// the PID list crosses the lock seam; the int4[] construction is this unit's
/// glue.
pub fn pg_blocking_pids<'mcx>(mcx: Mcx<'mcx>, blocked_pid: i32) -> PgResult<Datum<'mcx>> {
    let pids = lock::blocking_pids::call(mcx, blocked_pid)?;
    construct_int4_array(mcx, &pids[..])
}

/// `pg_safe_snapshot_blocking_pids(blocked_pid)` (lockfuncs.c) — int4[] of PIDs
/// blocking `blocked_pid` from getting a safe snapshot. The
/// `GetSafeSnapshotBlockingPids` collection is predicate.c-internal (genuinely
/// unported); the int4[] construction is this unit's glue.
pub fn pg_safe_snapshot_blocking_pids<'mcx>(
    mcx: Mcx<'mcx>,
    blocked_pid: i32,
) -> PgResult<Datum<'mcx>> {
    let pids = lock::safe_snapshot_blocking_pids::call(mcx, blocked_pid)?;
    construct_int4_array(mcx, &pids[..])
}

/// `construct_array_builtin(datums, n, INT4OID)` for a list of PIDs.
fn construct_int4_array<'mcx>(mcx: Mcx<'mcx>, pids: &[i32]) -> PgResult<Datum<'mcx>> {
    // The arrayfuncs owner hands back the bare scalar word (a pointer to the
    // detoasted `int4[]`); carry it in the canonical by-value arm.
    Ok(Datum::ByVal(
        backend_utils_adt_arrayfuncs_seams::construct_int4_array::call(mcx, pids)?.as_usize(),
    ))
}

// --- advisory locks ---
//
// We make use of the locktag fields as in C:
//   field1: MyDatabaseId
//   field2: first int4 key, or high half of an int8 key
//   field3: second int4 key, or low half of an int8 key
//   field4: 1 if using an int8 key, 2 if using two int4 keys

/// `SET_LOCKTAG_INT64(tag, key64)`.
fn set_locktag_int64(key64: i64) -> lk::LOCKTAG {
    let db = backend_utils_init_small::globals::MyDatabaseId();
    lk::LOCKTAG::advisory(
        db,
        (key64 >> 32) as u32,
        key64 as u32,
        1,
    )
}

/// `SET_LOCKTAG_INT32(tag, key1, key2)`.
fn set_locktag_int32(key1: i32, key2: i32) -> lk::LOCKTAG {
    let db = backend_utils_init_small::globals::MyDatabaseId();
    lk::LOCKTAG::advisory(db, key1 as u32, key2 as u32, 2)
}

/// `pg_advisory_lock(int8)` — exclusive, session scope.
pub fn pg_advisory_lock_int8(key: i64) -> PgResult<()> {
    let tag = set_locktag_int64(key);
    let guard = lock::lock_acquire(&tag, lk::ExclusiveLock, true, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_advisory_xact_lock(int8)` — exclusive, xact scope.
pub fn pg_advisory_xact_lock_int8(key: i64) -> PgResult<()> {
    let tag = set_locktag_int64(key);
    let guard = lock::lock_acquire(&tag, lk::ExclusiveLock, false, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_advisory_lock_shared(int8)` — share, session scope.
pub fn pg_advisory_lock_shared_int8(key: i64) -> PgResult<()> {
    let tag = set_locktag_int64(key);
    let guard = lock::lock_acquire(&tag, lk::ShareLock, true, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_advisory_xact_lock_shared(int8)` — share, xact scope.
pub fn pg_advisory_xact_lock_shared_int8(key: i64) -> PgResult<()> {
    let tag = set_locktag_int64(key);
    let guard = lock::lock_acquire(&tag, lk::ShareLock, false, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_try_advisory_lock(int8)` — exclusive, session scope, no wait.
pub fn pg_try_advisory_lock_int8(key: i64) -> PgResult<bool> {
    let tag = set_locktag_int64(key);
    try_advisory_lock(&tag, lk::ExclusiveLock, true)
}

/// `pg_try_advisory_xact_lock(int8)` — exclusive, xact scope, no wait.
pub fn pg_try_advisory_xact_lock_int8(key: i64) -> PgResult<bool> {
    let tag = set_locktag_int64(key);
    try_advisory_lock(&tag, lk::ExclusiveLock, false)
}

/// `pg_try_advisory_lock_shared(int8)` — share, session scope, no wait.
pub fn pg_try_advisory_lock_shared_int8(key: i64) -> PgResult<bool> {
    let tag = set_locktag_int64(key);
    try_advisory_lock(&tag, lk::ShareLock, true)
}

/// `pg_try_advisory_xact_lock_shared(int8)` — share, xact scope, no wait.
pub fn pg_try_advisory_xact_lock_shared_int8(key: i64) -> PgResult<bool> {
    let tag = set_locktag_int64(key);
    try_advisory_lock(&tag, lk::ShareLock, false)
}

/// `pg_advisory_unlock(int8)` — release exclusive session lock.
pub fn pg_advisory_unlock_int8(key: i64) -> PgResult<bool> {
    let tag = set_locktag_int64(key);
    Ok(lock::lock_release::call(&tag, lk::ExclusiveLock, true))
}

/// `pg_advisory_unlock_shared(int8)` — release share session lock.
pub fn pg_advisory_unlock_shared_int8(key: i64) -> PgResult<bool> {
    let tag = set_locktag_int64(key);
    Ok(lock::lock_release::call(&tag, lk::ShareLock, true))
}

/// `pg_advisory_lock(int4, int4)` — exclusive, session scope.
pub fn pg_advisory_lock_int4(key1: i32, key2: i32) -> PgResult<()> {
    let tag = set_locktag_int32(key1, key2);
    let guard = lock::lock_acquire(&tag, lk::ExclusiveLock, true, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_advisory_xact_lock(int4, int4)` — exclusive, xact scope.
pub fn pg_advisory_xact_lock_int4(key1: i32, key2: i32) -> PgResult<()> {
    let tag = set_locktag_int32(key1, key2);
    let guard = lock::lock_acquire(&tag, lk::ExclusiveLock, false, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_advisory_lock_shared(int4, int4)` — share, session scope.
pub fn pg_advisory_lock_shared_int4(key1: i32, key2: i32) -> PgResult<()> {
    let tag = set_locktag_int32(key1, key2);
    let guard = lock::lock_acquire(&tag, lk::ShareLock, true, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_advisory_xact_lock_shared(int4, int4)` — share, xact scope.
pub fn pg_advisory_xact_lock_shared_int4(key1: i32, key2: i32) -> PgResult<()> {
    let tag = set_locktag_int32(key1, key2);
    let guard = lock::lock_acquire(&tag, lk::ShareLock, false, false)?;
    guard.keep_held();
    Ok(())
}

/// `pg_try_advisory_lock(int4, int4)` — exclusive, session scope, no wait.
pub fn pg_try_advisory_lock_int4(key1: i32, key2: i32) -> PgResult<bool> {
    let tag = set_locktag_int32(key1, key2);
    try_advisory_lock(&tag, lk::ExclusiveLock, true)
}

/// `pg_try_advisory_xact_lock(int4, int4)` — exclusive, xact scope, no wait.
pub fn pg_try_advisory_xact_lock_int4(key1: i32, key2: i32) -> PgResult<bool> {
    let tag = set_locktag_int32(key1, key2);
    try_advisory_lock(&tag, lk::ExclusiveLock, false)
}

/// `pg_try_advisory_lock_shared(int4, int4)` — share, session scope, no wait.
pub fn pg_try_advisory_lock_shared_int4(key1: i32, key2: i32) -> PgResult<bool> {
    let tag = set_locktag_int32(key1, key2);
    try_advisory_lock(&tag, lk::ShareLock, true)
}

/// `pg_try_advisory_xact_lock_shared(int4, int4)` — share, xact scope, no wait.
pub fn pg_try_advisory_xact_lock_shared_int4(key1: i32, key2: i32) -> PgResult<bool> {
    let tag = set_locktag_int32(key1, key2);
    try_advisory_lock(&tag, lk::ShareLock, false)
}

/// `pg_advisory_unlock(int4, int4)` — release exclusive session lock.
pub fn pg_advisory_unlock_int4(key1: i32, key2: i32) -> PgResult<bool> {
    let tag = set_locktag_int32(key1, key2);
    Ok(lock::lock_release::call(&tag, lk::ExclusiveLock, true))
}

/// `pg_advisory_unlock_shared(int4, int4)` — release share session lock.
pub fn pg_advisory_unlock_shared_int4(key1: i32, key2: i32) -> PgResult<bool> {
    let tag = set_locktag_int32(key1, key2);
    Ok(lock::lock_release::call(&tag, lk::ShareLock, true))
}

/// `pg_advisory_unlock_all()` — release all advisory (USER_LOCKMETHOD) session
/// locks.
pub fn pg_advisory_unlock_all() -> PgResult<()> {
    lock::lock_release_session::call(lk::USER_LOCKMETHOD);
    Ok(())
}

/// The `pg_try_advisory_*` body: `res = LockAcquire(&tag, mode, sessionLock,
/// /*dontWait=*/true); return res != LOCKACQUIRE_NOT_AVAIL`.
fn try_advisory_lock(tag: &lk::LOCKTAG, mode: lk::LOCKMODE, session_lock: bool) -> PgResult<bool> {
    let guard = lock::lock_acquire(tag, mode, session_lock, true)?;
    let acquired = guard.result() != lk::LOCKACQUIRE_NOT_AVAIL;
    // C returns with the lock held (when acquired); keep it.
    guard.keep_held();
    Ok(acquired)
}

// =====================================================================
// partitionfuncs.c
// =====================================================================

/// `check_rel_can_be_partition(relid)` (partitionfuncs.c): whether `relid` is a
/// relation that can appear in a partition tree (it exists and is either a
/// partition or a partitioned table/index).
fn check_rel_can_be_partition(relid: u32) -> PgResult<bool> {
    // Check if relation exists
    if !syscache::reloid_exists::call(relid)? {
        return Ok(false);
    }

    let relkind = lsyscache::get_rel_relkind::call(relid)?;
    let relispartition = lsyscache::get_rel_relispartition::call(relid)?;

    // Only allow relation types that can appear in partition trees.
    if !relispartition && !relkind_has_partitions(relkind) {
        return Ok(false);
    }

    Ok(true)
}

/// `pg_partition_tree(rootrelid)` (partitionfuncs.c) — a value-per-call SRF with
/// one row (relid, parentid, isleaf, level) per partition-tree member. The data
/// logic (the inheritance walk, the level/parent computation) is this unit's
/// own; the value-SRF emission protocol is funcapi-owned and unported.
pub fn pg_partition_tree<'mcx>(
    _mcx: Mcx<'mcx>,
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    _rootrelid: u32,
) -> PgResult<Datum<'mcx>> {
    funcapi::value_srf_unported::call();
    unreachable!("value_srf_unported panics until the funcapi value-SRF owner lands")
}

/// `pg_partition_tree` per-row builder (the C inner block): for partition-tree
/// member `relid` rooted at `rootrelid`, with its `ancestors` list (immediate
/// parent first), produce (relid, parentid|NULL, isleaf, level). This is this
/// unit's own logic, shared by the value-SRF owner once it lands.
#[allow(dead_code)]
fn pg_partition_tree_row(
    relid: u32,
    rootrelid: u32,
    relkind: u8,
    ancestors: &[u32],
) -> ([Datum<'static>; 4], [bool; 4]) {
    let mut values: [Datum<'static>; 4] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; 4];

    // relid
    values[0] = Datum::from_u32(relid);

    // parentid = linitial_oid(ancestors) if any
    let parentid = ancestors.first().copied().unwrap_or(0);
    if parentid != 0 {
        values[1] = Datum::from_u32(parentid);
    } else {
        nulls[1] = true;
    }

    // isleaf = !RELKIND_HAS_PARTITIONS(relkind)
    values[2] = Datum::from_bool(!relkind_has_partitions(relkind));

    // level: 0 for the root; else count ancestors up to and including rootrelid
    let mut level = 0i32;
    if relid != rootrelid {
        for &a in ancestors {
            level += 1;
            if a == rootrelid {
                break;
            }
        }
    }
    values[3] = Datum::from_i32(level);

    (values, nulls)
}

/// `pg_partition_root(relid)` (partitionfuncs.c): the top-most parent of the
/// partition tree `relid` belongs to, or NULL if it is not (or cannot be) a
/// partition-tree member.
pub fn pg_partition_root<'mcx>(mcx: Mcx<'mcx>, relid: u32) -> PgResult<Datum<'mcx>> {
    if !check_rel_can_be_partition(relid)? {
        return Ok(Datum::null());
    }

    // fetch the list of ancestors (immediate parent first, root last)
    let ancestors = backend_catalog_partition_seams::get_partition_ancestors::call(mcx, relid)?;

    // If the input relation is already the top-most parent, return itself.
    if ancestors.is_empty() {
        return Ok(Datum::from_u32(relid));
    }

    // rootrelid = llast_oid(ancestors)
    let rootrelid = *ancestors.last().expect("non-empty ancestors");
    debug_assert!(rootrelid != 0);
    Ok(Datum::from_u32(rootrelid))
}

/// `pg_partition_ancestors(relid)` (partitionfuncs.c) — a value-per-call SRF of
/// every ancestor of `relid` including `relid` itself. The ancestor list is
/// this unit's own logic (`relid` consed onto `get_partition_ancestors`); the
/// value-SRF emission protocol is funcapi-owned and unported.
pub fn pg_partition_ancestors<'mcx>(
    _mcx: Mcx<'mcx>,
    _fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    _relid: u32,
) -> PgResult<Datum<'mcx>> {
    funcapi::value_srf_unported::call();
    unreachable!("value_srf_unported panics until the funcapi value-SRF owner lands")
}

// =====================================================================
// pg_upgrade_support.c
// =====================================================================

/// `CHECK_IS_BINARY_UPGRADE` — every `binary_upgrade_*` function's first gate.
fn check_is_binary_upgrade() -> PgResult<()> {
    if !binup::is_binary_upgrade::call() {
        return Err(PgError::new(
            ERROR,
            "function can only be called when server is in binary upgrade mode",
        )
        .with_sqlstate(ERRCODE_CANT_CHANGE_RUNTIME_PARAM));
    }
    Ok(())
}

/// `binary_upgrade_set_next_pg_tablespace_oid(tbspoid)`.
pub fn binary_upgrade_set_next_pg_tablespace_oid(tbspoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::PgTablespace, tbspoid);
    Ok(())
}

/// `binary_upgrade_set_next_pg_type_oid(typoid)`.
pub fn binary_upgrade_set_next_pg_type_oid(typoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::PgType, typoid);
    Ok(())
}

/// `binary_upgrade_set_next_array_pg_type_oid(typoid)`.
pub fn binary_upgrade_set_next_array_pg_type_oid(typoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::ArrayPgType, typoid);
    Ok(())
}

/// `binary_upgrade_set_next_multirange_pg_type_oid(typoid)`.
pub fn binary_upgrade_set_next_multirange_pg_type_oid(typoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::MultirangePgType, typoid);
    Ok(())
}

/// `binary_upgrade_set_next_multirange_array_pg_type_oid(typoid)`.
pub fn binary_upgrade_set_next_multirange_array_pg_type_oid(typoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::MultirangeArrayPgType, typoid);
    Ok(())
}

/// `binary_upgrade_set_next_heap_pg_class_oid(reloid)`.
pub fn binary_upgrade_set_next_heap_pg_class_oid(reloid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::HeapPgClass, reloid);
    Ok(())
}

/// `binary_upgrade_set_next_heap_relfilenode(relfilenumber)`.
pub fn binary_upgrade_set_next_heap_relfilenode(relfilenumber: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::HeapRelfilenode, relfilenumber);
    Ok(())
}

/// `binary_upgrade_set_next_index_pg_class_oid(reloid)`.
pub fn binary_upgrade_set_next_index_pg_class_oid(reloid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::IndexPgClass, reloid);
    Ok(())
}

/// `binary_upgrade_set_next_index_relfilenode(relfilenumber)`.
pub fn binary_upgrade_set_next_index_relfilenode(relfilenumber: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::IndexRelfilenode, relfilenumber);
    Ok(())
}

/// `binary_upgrade_set_next_toast_pg_class_oid(reloid)`.
pub fn binary_upgrade_set_next_toast_pg_class_oid(reloid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::ToastPgClass, reloid);
    Ok(())
}

/// `binary_upgrade_set_next_toast_relfilenode(relfilenumber)`.
pub fn binary_upgrade_set_next_toast_relfilenode(relfilenumber: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::ToastRelfilenode, relfilenumber);
    Ok(())
}

/// `binary_upgrade_set_next_pg_enum_oid(enumoid)`.
pub fn binary_upgrade_set_next_pg_enum_oid(enumoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::PgEnum, enumoid);
    Ok(())
}

/// `binary_upgrade_set_next_pg_authid_oid(authoid)`.
pub fn binary_upgrade_set_next_pg_authid_oid(authoid: u32) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_next_oid::call(binup::NextOidSlot::PgAuthid, authoid);
    Ok(())
}

/// `binary_upgrade_set_record_init_privs(record_init_privs)`.
pub fn binary_upgrade_set_record_init_privs(record_init_privs: bool) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_record_init_privs::call(record_init_privs);
    Ok(())
}

/// `binary_upgrade_set_missing_value(table_id, attname, value)`.
pub fn binary_upgrade_set_missing_value(
    table_id: u32,
    attname: &str,
    value: &str,
) -> PgResult<()> {
    check_is_binary_upgrade()?;
    binup::set_missing_value::call(table_id, attname, value)
}

/// `binary_upgrade_create_empty_extension(name, schema, relocatable, version,
/// config, condition, requires)`.
///
/// The four leading arguments must be non-NULL (the C
/// `elog(ERROR, "null argument ... not allowed")`); `config`/`condition` are
/// optional `text[]` varlena bytes; `requires` is an optional `text[]` of
/// required-extension names.
pub fn binary_upgrade_create_empty_extension(
    ext_name: Option<&str>,
    schema_name: Option<&str>,
    relocatable: Option<bool>,
    ext_version: Option<&str>,
    ext_config: Option<&[u8]>,
    ext_condition: Option<&[u8]>,
    required_extension_names: &[&str],
) -> PgResult<()> {
    check_is_binary_upgrade()?;

    // We must check these things before dereferencing the arguments.
    let (ext_name, schema_name, relocatable, ext_version) =
        match (ext_name, schema_name, relocatable, ext_version) {
            (Some(n), Some(s), Some(r), Some(v)) => (n, s, r, v),
            _ => {
                return Err(PgError::new(
                    ERROR,
                    "null argument to binary_upgrade_create_empty_extension is not allowed",
                ))
            }
        };

    binup::create_empty_extension::call(
        ext_name,
        schema_name,
        relocatable,
        ext_version,
        ext_config,
        ext_condition,
        required_extension_names,
    )
}

/// `binary_upgrade_logical_slot_has_caught_up(slot_name)`.
pub fn binary_upgrade_logical_slot_has_caught_up(slot_name: &str) -> PgResult<bool> {
    check_is_binary_upgrade()?;
    binup::logical_slot_has_caught_up::call(slot_name)
}

/// `binary_upgrade_add_sub_rel_state(subname, relid, relstate, sublsn)`. The
/// first three arguments must be non-NULL; `sublsn` is `None` for a NULL LSN.
pub fn binary_upgrade_add_sub_rel_state(
    subname: Option<&str>,
    relid: Option<u32>,
    relstate: Option<i8>,
    sublsn: Option<u64>,
) -> PgResult<()> {
    check_is_binary_upgrade()?;

    let (subname, relid, relstate) = match (subname, relid, relstate) {
        (Some(s), Some(r), Some(st)) => (s, r, st),
        _ => {
            return Err(PgError::new(
                ERROR,
                "null argument to binary_upgrade_add_sub_rel_state is not allowed",
            ))
        }
    };

    binup::add_sub_rel_state::call(subname, relid, relstate, sublsn)
}

/// `binary_upgrade_replorigin_advance(subname, remote_commit)`. `subname` must
/// be non-NULL; `remote_commit` is `None` for a NULL LSN.
pub fn binary_upgrade_replorigin_advance(
    subname: Option<&str>,
    remote_commit: Option<u64>,
) -> PgResult<()> {
    check_is_binary_upgrade()?;

    let subname = match subname {
        Some(s) => s,
        None => {
            return Err(PgError::new(
                ERROR,
                "null argument to binary_upgrade_replorigin_advance is not allowed",
            ))
        }
    };

    binup::replorigin_advance::call(subname, remote_commit)
}
