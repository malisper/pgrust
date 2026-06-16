#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/tablespace.c` — CREATE / DROP / RENAME / ALTER TABLESPACE,
//! the per-dbspace directory bootstrap (`TablespaceCreateDbspace`), the
//! `default_tablespace` / `temp_tablespaces` GUC hooks, the name/oid lookups,
//! and the `tblspc_redo` WAL handler.
//!
//! Faithful 1:1 port against the owned node tree. tablespace.c's own decision
//! logic — branch order, permission checks before mutations, every
//! `ereport`/`elog` (SQLSTATE + exact message + hint/detail), constants, the
//! in-place computation, the redo-vs-normal severity selection, and the
//! `SplitIdentifierString` identifier-list parser — lives in-crate. Genuine
//! externals cross seams to their owners: the catalog `pg_tablespace`
//! primitives + reloptions build (`backend-catalog-pg-tablespace-seams`), the
//! raw filesystem syscalls (`backend-storage-file-tblspc-fs-seams`), and the
//! ambient globals / GUC readers / path helpers / `TablespaceCreateLock`
//! (`backend-commands-tablespace-globals-seams`). The `AllocateDir`/`ReadDir`/
//! `FreeDir`, WAL insert, transaction, proc-signal-barrier, recovery-conflict,
//! checkpoint-request, comment/seclabel/dependency/acl/objectaccess callees are
//! real ported functions (or their owners' seams) called directly.
//!
//! No `extern "C"`, no raw pointers; soft errors flow through
//! `backend-utils-error`. Data-derived `Vec` growth is guarded with
//! `try_reserve`.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use backend_catalog_catalog::IsReservedName;
use backend_utils_error::ereport;
use mcx::{Mcx, MemoryContext};
use types_acl::acl::ACL_CREATE;
use types_acl::acl::{ACLCHECK_NOT_OWNER, ACLCHECK_NO_PRIV, ACLCHECK_OK};
use types_catalog::catalog::{GLOBALTABLESPACE_OID, TABLE_SPACE_RELATION_ID};
use types_tuple::access::RELPERSISTENCE_TEMP;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::Oid;
use types_error::{
    ErrorLevel, ErrorLocation, PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_NAME, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_IN_USE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_RESERVED_NAME, ERRCODE_UNDEFINED_FILE, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, LOG, NOTICE, PANIC, WARNING,
};
use types_guc::guc::{GucSource, PGC_S_INTERACTIVE, PGC_S_TEST};
use types_nodes::ddlnodes::{
    AlterTableSpaceOptionsStmt, CreateTableSpaceStmt, DefElem, DropTableSpaceStmt,
};
use types_nodes::nodes::Node;
use types_nodes::parsenodes::OBJECT_TABLESPACE;

use backend_catalog_pg_tablespace_seams as cat;
use backend_commands_tablespace_globals_seams as glob;
use backend_storage_file_tblspc_fs_seams as fs;
use backend_storage_file_tblspc_fs_seams::{StatKind, StatResult, EEXIST, ENOENT};

/// `NAMEDATALEN` (`catalog/pg_attribute.h`) — used by `truncate_identifier`.
const NAMEDATALEN: usize = 64;

// The path-layout constants are the workspace's single source of truth
// (`types-storage`), so the version directory stays in lockstep with
// `GetDatabasePath` and the relcache init file.
use types_core::primitive::MAXPGPATH;
use types_storage::file::{
    FORKNAMECHARS, OIDCHARS, PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY,
};

/// `RM_TBLSPC_ID` (`access/rmgrlist.h`).
const RM_TBLSPC_ID: types_core::primitive::RmgrId = 5;
/// `XLOG_TBLSPC_CREATE` (`commands/tablespace.h`).
const XLOG_TBLSPC_CREATE: u8 = 0x00;
/// `XLOG_TBLSPC_DROP` (`commands/tablespace.h`).
const XLOG_TBLSPC_DROP: u8 = 0x10;

/// `AccessShareLock`/`RowExclusiveLock`/`NoLock` (`storage/lockdefs.h`).
const AccessShareLock: i32 = 1;
const RowExclusiveLock: i32 = 3;
const NoLock: i32 = 0;

/// `OidIsValid(oid)` (`c.h`).
fn OidIsValid(oid: Oid) -> bool {
    oid != types_core::primitive::InvalidOid
}

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/tablespace.c", 0, funcname)
}

/* =========================================================================
 * TablespaceCreateDbspace   (C lines 111-198)
 * ========================================================================= */

/// `TablespaceCreateDbspace` — create the per-database subdirectory for a
/// tablespace on first object creation (also used during WAL replay).
pub fn TablespaceCreateDbspace(spcOid: Oid, _dbOid: Oid, isRedo: bool) -> PgResult<()> {
    /*
     * The global tablespace doesn't have per-database subdirectories, so
     * nothing to do for it.
     */
    if spcOid == GLOBALTABLESPACE_OID {
        return Ok(());
    }

    // Assert(OidIsValid(spcOid)); Assert(OidIsValid(dbOid));

    let dir = backend_common_relpath::GetDatabasePath(_dbOid, spcOid);

    match fs::stat::call(&dir)? {
        StatResult::Failed(errno) => {
            /* Directory does not exist? */
            if errno == ENOENT {
                /*
                 * Acquire TablespaceCreateLock to ensure that no DROP TABLESPACE
                 * or TablespaceCreateDbspace is running concurrently.
                 */
                glob::lwlock_acquire_tablespace_create::call()?;

                /*
                 * Recheck to see if someone created the directory while we were
                 * waiting for lock.
                 */
                let created = matches!(fs::stat::call(&dir)?, StatResult::Found(StatKind::Dir));
                if created {
                    /* Directory was created */
                } else {
                    /* Directory creation failed? */
                    if let Err(mkerr) = fs::make_pg_directory::call(&dir)? {
                        /* Failure other than not exists or not in WAL replay? */
                        if mkerr != ENOENT || !isRedo {
                            return Err(file_access_error(ERROR, mkerr)
                                .errmsg(format!("could not create directory \"{dir}\": %m"))
                                .into_error());
                        }

                        /*
                         * During WAL replay, it's conceivable that several levels
                         * of directories are missing if tablespaces are dropped
                         * further ahead of the WAL stream than we're currently
                         * replaying.  An easy way forward is to create them as
                         * plain directories and hope they are removed by further
                         * WAL replay if necessary.  If this also fails, there is
                         * trouble we cannot get out of, so just report that and
                         * bail out.
                         */
                        if let Err(mkperr) = fs::pg_mkdir_p::call(&dir)? {
                            return Err(file_access_error(ERROR, mkperr)
                                .errmsg(format!("could not create directory \"{dir}\": %m"))
                                .into_error());
                        }
                    }
                }

                glob::lwlock_release_tablespace_create::call()?;
            } else {
                return Err(file_access_error(ERROR, errno)
                    .errmsg(format!("could not stat directory \"{dir}\": %m"))
                    .into_error());
            }
        }
        StatResult::Found(kind) => {
            /* Is it not a directory? */
            if kind != StatKind::Dir {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("\"{dir}\" exists but is not a directory"))
                    .into_error());
            }
        }
    }

    // pfree(dir);
    Ok(())
}

/* =========================================================================
 * CreateTableSpace   (C lines 207-387)
 * ========================================================================= */

/// `CreateTableSpace` — CREATE TABLESPACE.  Returns the new tablespace OID.
pub fn CreateTableSpace<'mcx>(mcx: Mcx<'mcx>, stmt: &CreateTableSpaceStmt<'mcx>) -> PgResult<Oid> {
    let tablespacename: &str = stmt.tablespacename.as_deref().unwrap_or("");

    /* Must be superuser */
    if !backend_utils_misc_superuser_seams::superuser::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to create tablespace \"{tablespacename}\""
            ))
            .errhint("Must be superuser to create a tablespace.")
            .into_error());
    }

    /* However, the eventual owner of the tablespace need not be */
    let ownerId = match stmt.owner.as_deref() {
        Some(owner) => {
            // get_rolespec_oid(stmt->owner, false): the parser only ever fills
            // `owner` with a RoleSpec; any other node kind is a malformed tree.
            match owner {
                Node::RoleSpec(rs) => {
                    // The acl seam takes the analyze-side `parsenodes::RoleSpec`;
                    // the grammar DDL family carries `ddlnodes::RoleSpec` (same
                    // RoleSpecType + fields). Rebuild it field-for-field in `mcx`.
                    let role = types_nodes::parsenodes::RoleSpec {
                        roletype: rs.roletype,
                        rolename: match &rs.rolename {
                            Some(s) => Some(s.clone_in(mcx)?),
                            None => None,
                        },
                    };
                    backend_utils_adt_acl_seams::get_rolespec_oid::call(&role, false)?
                }
                _ => {
                    return Err(ereport(ERROR)
                        .errmsg_internal("CreateTableSpace: owner is not a RoleSpec")
                        .into_error())
                }
            }
        }
        None => backend_utils_init_miscinit::GetUserId(),
    };

    /* Unix-ify the offered path, and strip any trailing slashes */
    let location = glob::canonicalize_path::call(stmt.location.as_deref().unwrap_or(""))?;

    /* disallow quotes, else CREATE DATABASE would be at risk */
    if location.contains('\'') {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg("tablespace location cannot contain single quotes")
            .into_error());
    }

    let in_place = glob::allow_in_place_tablespaces::call()? && location.is_empty();

    /*
     * Allowing relative paths seems risky.  This also helps us ensure that
     * location is not empty or whitespace, unless specifying a developer-only
     * in-place tablespace.
     */
    if !in_place && !glob::is_absolute_path::call(&location)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("tablespace location must be an absolute path")
            .into_error());
    }

    /*
     * Check that location isn't too long. Remember that we're going to append
     * 'PG_XXX/<dboid>/<relid>_<fork>.<nnn>'.
     */
    if location.len()
        + 1
        + TABLESPACE_VERSION_DIRECTORY.len()
        + 1
        + OIDCHARS
        + 1
        + OIDCHARS
        + 1
        + FORKNAMECHARS
        + 1
        + OIDCHARS
        > MAXPGPATH
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!("tablespace location \"{location}\" is too long"))
            .into_error());
    }

    /* Warn if the tablespace is in the data directory. */
    let data_dir = backend_utils_init_small::globals::DataDir().unwrap_or_default();
    if glob::path_is_prefix_of_path::call(&data_dir, &location)? {
        ereport(WARNING)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("tablespace location should not be inside the data directory")
            .finish(here("CreateTableSpace"))?;
    }

    /*
     * Disallow creation of tablespaces named "pg_xxx"; we reserve this
     * namespace for system purposes.
     */
    if !glob::allowSystemTableMods::call()? && IsReservedName(tablespacename) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("unacceptable tablespace name \"{tablespacename}\""))
            .errdetail("The prefix \"pg_\" is reserved for system tablespaces.")
            .into_error());
    }

    /*
     * Check that there is no other tablespace by this name.  (The unique
     * index would catch this anyway, but might as well give a friendlier
     * message.)
     */
    if OidIsValid(get_tablespace_oid(mcx, tablespacename, true)?) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("tablespace \"{tablespacename}\" already exists"))
            .into_error());
    }

    /*
     * Insert tuple into pg_tablespace.  The purpose of doing this first is to
     * lock the proposed tablename against other would-be creators.
     */
    let rel = cat::tablespace_table_open::call(mcx, RowExclusiveLock)?;

    let tablespaceoid = if glob::IsBinaryUpgrade::call()? {
        /* Use binary-upgrade override for tablespace oid */
        let next = glob::take_binary_upgrade_next_oid::call()?;
        if !OidIsValid(next) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("pg_tablespace OID value not set when in binary upgrade mode")
                .into_error());
        }
        next
    } else {
        backend_catalog_catalog::GetNewOidWithIndex(
            &rel,
            types_catalog::catalog::TABLESPACE_OID_INDEX_ID,
            ANUM_PG_TABLESPACE_OID,
        )?
    };

    /* Generate new proposed spcoptions (text array) and form + insert the tuple */
    let options = materialize_def_elems(mcx, &stmt.options)?;
    let new_options = cat::build_create_options::call(mcx, &options)?;
    cat::insert_tablespace_tuple::call(&rel, tablespaceoid, tablespacename, ownerId, new_options)?;

    /* Record dependency on owner */
    backend_catalog_pg_shdepend_seams::recordDependencyOnOwner::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        ownerId,
    )?;

    /* Post creation hook for new tablespace */
    backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        0,
    )?;

    create_tablespace_directories(&location, tablespaceoid)?;

    /* Record the filesystem change in XLOG */
    xlog_tblspc_create(tablespaceoid, &location)?;

    /*
     * Force synchronous commit, to minimize the window between creating the
     * symlink on-disk and marking the transaction committed.
     */
    backend_access_transam_xact::ForceSyncCommit();

    // pfree(location);

    /* We keep the lock on pg_tablespace until commit */
    cat::tablespace_table_close::call(rel, NoLock)?;

    Ok(tablespaceoid)
}

/* =========================================================================
 * DropTableSpace   (C lines 394-562)
 * ========================================================================= */

/// `DropTableSpace` — DROP TABLESPACE.
pub fn DropTableSpace<'mcx>(mcx: Mcx<'mcx>, stmt: &DropTableSpaceStmt<'mcx>) -> PgResult<()> {
    let tablespacename: &str = stmt.tablespacename.as_deref().unwrap_or("");

    /*
     * Find the target tuple
     */
    let rel = cat::tablespace_table_open::call(mcx, RowExclusiveLock)?;

    let tuple = cat::scan_tablespace_by_name::call(&rel, tablespacename)?;

    let tuple = match tuple {
        Some(t) => t,
        None => {
            if !stmt.missing_ok {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("tablespace \"{tablespacename}\" does not exist"))
                    .into_error());
            } else {
                ereport(NOTICE)
                    .errmsg(format!(
                        "tablespace \"{tablespacename}\" does not exist, skipping"
                    ))
                    .finish(here("DropTableSpace"))?;
                // table_endscan handled inside scan helper; close rel.
                cat::tablespace_table_close::call(rel, NoLock)?;
            }
            return Ok(());
        }
    };

    let tablespaceoid = tuple.oid;

    /* Must be tablespace owner */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        backend_utils_init_miscinit::GetUserId(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_TABLESPACE,
            Some(tablespacename.to_string()),
        )?;
    }

    /* Disallow drop of the standard tablespaces, even by superuser */
    if backend_catalog_catalog::IsPinnedObject(TABLE_SPACE_RELATION_ID, tablespaceoid) {
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NO_PRIV,
            OBJECT_TABLESPACE,
            Some(tablespacename.to_string()),
        )?;
    }

    /* Check for pg_shdepend entries depending on this tablespace */
    let (has_deps, detail, detail_log) =
        backend_catalog_pg_shdepend_seams::checkSharedDependencies::call(
            mcx,
            TABLE_SPACE_RELATION_ID,
            tablespaceoid,
        )?;
    if has_deps {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .errmsg(format!(
                "tablespace \"{tablespacename}\" cannot be dropped because some objects depend on it"
            ))
            .errdetail_internal(detail.as_deref().unwrap_or("").to_string())
            .errdetail_log(detail_log.as_deref().unwrap_or("").to_string())
            .into_error());
    }

    /* DROP hook for the tablespace being removed */
    backend_catalog_objectaccess_seams::InvokeObjectDropHookArg::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        0,
        0,
    )?;

    /*
     * Remove the pg_tablespace tuple (this will roll back if we fail below)
     */
    cat::delete_tablespace_tuple::call(&rel, tuple.handle)?;

    // table_endscan(scandesc);

    /*
     * Remove any comments or security labels on this tablespace.
     */
    backend_commands_comment::DeleteSharedComments(tablespaceoid, TABLE_SPACE_RELATION_ID)?;
    backend_commands_seclabel::DeleteSharedSecurityLabel(
        mcx,
        tablespaceoid,
        TABLE_SPACE_RELATION_ID,
    )?;

    /*
     * Remove dependency on owner.
     */
    backend_catalog_pg_shdepend_seams::deleteSharedDependencyRecordsFor::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        0,
    )?;

    /*
     * Acquire TablespaceCreateLock to ensure that no TablespaceCreateDbspace
     * is running concurrently.
     */
    glob::lwlock_acquire_tablespace_create::call()?;

    /*
     * Try to remove the physical infrastructure.
     */
    if !destroy_tablespace_directories(tablespaceoid, false)? {
        /*
         * Not all files deleted?  Force a checkpoint which will clean out any
         * lingering files, and try again.
         */
        backend_postmaster_checkpointer_seams::request_checkpoint::call(
            CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT,
        );

        /*
         * Also use a global barrier to ask all backends to close all files, and
         * wait until they're finished.
         */
        glob::lwlock_release_tablespace_create::call()?;
        smgr_release_barrier()?;
        glob::lwlock_acquire_tablespace_create::call()?;

        /* And now try again. */
        if !destroy_tablespace_directories(tablespaceoid, false)? {
            /* Still not empty, the files must be important then */
            // NB: lock released on transaction abort.
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("tablespace \"{tablespacename}\" is not empty"))
                .into_error());
        }
    }

    /* Record the filesystem change in XLOG */
    xlog_tblspc_drop(tablespaceoid)?;

    /*
     * Force synchronous commit, to minimize the window between removing the
     * files on-disk and marking the transaction committed.
     */
    backend_access_transam_xact::ForceSyncCommit();

    /*
     * Allow TablespaceCreateDbspace again.
     */
    glob::lwlock_release_tablespace_create::call()?;

    /* We keep the lock on pg_tablespace until commit */
    cat::tablespace_table_close::call(rel, NoLock)?;

    Ok(())
}

/* =========================================================================
 * create_tablespace_directories   (C lines 571-669, static)
 * ========================================================================= */

/// `create_tablespace_directories` — link `$PGDATA/pg_tblspc/<oid>` to the
/// target directory (or, for an in-place tablespace, create it directly).
fn create_tablespace_directories(location: &str, tablespaceoid: Oid) -> PgResult<()> {
    let linkloc = format!("{PG_TBLSPC_DIR}/{tablespaceoid}");

    /*
     * If we're asked to make an 'in place' tablespace, create the directory
     * directly where the symlink would normally go.
     */
    let in_place = location.is_empty();

    if in_place {
        match fs::make_pg_directory::call(&linkloc)? {
            Ok(()) => {}
            Err(errno) if errno == EEXIST => {}
            Err(errno) => {
                return Err(file_access_error(ERROR, errno)
                    .errmsg(format!("could not create directory \"{linkloc}\": %m"))
                    .into_error());
            }
        }
    }

    let location_with_version_dir = format!(
        "{}/{}",
        if in_place { &linkloc } else { location },
        TABLESPACE_VERSION_DIRECTORY
    );

    /*
     * Attempt to coerce target directory to safe permissions.  Not needed for
     * in-place mode, because we created the directory with desired permissions.
     */
    if !in_place {
        if let Err(errno) = fs::chmod_dir::call(location)? {
            if errno == ENOENT {
                let mut b = ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FILE)
                    .errmsg(format!("directory \"{location}\" does not exist"));
                if glob::InRecovery::call()? {
                    b = b.errhint(
                        "Create this directory for the tablespace before restarting the server.",
                    );
                }
                return Err(b.into_error());
            } else {
                return Err(file_access_error(ERROR, errno)
                    .errmsg(format!(
                        "could not set permissions on directory \"{location}\": %m"
                    ))
                    .into_error());
            }
        }
    }

    /*
     * The creation of the version directory prevents more than one tablespace
     * in a single location.
     */
    match fs::stat::call(&location_with_version_dir)? {
        StatResult::Failed(errno) => {
            if errno != ENOENT {
                return Err(file_access_error(ERROR, errno)
                    .errmsg(format!(
                        "could not stat directory \"{location_with_version_dir}\": %m"
                    ))
                    .into_error());
            } else if let Err(mkerr) = fs::make_pg_directory::call(&location_with_version_dir)? {
                return Err(file_access_error(ERROR, mkerr)
                    .errmsg(format!(
                        "could not create directory \"{location_with_version_dir}\": %m"
                    ))
                    .into_error());
            }
        }
        StatResult::Found(kind) => {
            if kind != StatKind::Dir {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!(
                        "\"{location_with_version_dir}\" exists but is not a directory"
                    ))
                    .into_error());
            } else if !glob::InRecovery::call()? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_IN_USE)
                    .errmsg(format!(
                        "directory \"{location_with_version_dir}\" already in use as a tablespace"
                    ))
                    .into_error());
            }
        }
    }

    /*
     * In recovery, remove old symlink, in case it points to the wrong place.
     */
    if !in_place && glob::InRecovery::call()? {
        remove_tablespace_symlink(&linkloc)?;
    }

    /*
     * Create the symlink under PGDATA
     */
    if !in_place {
        if let Err(errno) = fs::symlink::call(location, &linkloc)? {
            return Err(file_access_error(ERROR, errno)
                .errmsg(format!("could not create symbolic link \"{linkloc}\": %m"))
                .into_error());
        }
    }

    Ok(())
}

/* =========================================================================
 * destroy_tablespace_directories   (C lines 685-844, static)
 * ========================================================================= */

/// `destroy_tablespace_directories` — remove the filesystem infrastructure for a
/// tablespace.  Returns `true` if successful, `false` if some subdirectory is not
/// empty.  In `redo` mode, problems are logged (LOG) instead of erroring.
fn destroy_tablespace_directories(tablespaceoid: Oid, redo: bool) -> PgResult<bool> {
    let linkloc_with_version_dir =
        format!("{PG_TBLSPC_DIR}/{tablespaceoid}/{TABLESPACE_VERSION_DIRECTORY}");

    // ereport severity for filesystem failures: redo ? LOG : ERROR.
    let fail_level: ErrorLevel = if redo { LOG } else { ERROR };

    /*
     * Check if the tablespace still contains any files.  Try to rmdir each
     * per-database directory we find in it.
     */
    let dirdesc = match backend_storage_file_fd::allocated_desc::AllocateDir(&linkloc_with_version_dir)? {
        Some(handle) => Some(handle),
        None => {
            // AllocateDir returned NULL: the open failed, errno carries why.
            // Probe via stat to recover the errno C would have seen.
            let errno = open_dir_errno(&linkloc_with_version_dir)?;
            if errno == ENOENT {
                if !redo {
                    file_access_error(WARNING, errno)
                        .errmsg(format!(
                            "could not open directory \"{linkloc_with_version_dir}\": %m"
                        ))
                        .finish(here("destroy_tablespace_directories"))?;
                }
                /* The symlink might still exist, so go try to remove it */
                return remove_symlink_phase(&linkloc_with_version_dir, redo, fail_level);
            } else if redo {
                /* in redo, just log other types of error */
                file_access_error(LOG, errno)
                    .errmsg(format!(
                        "could not open directory \"{linkloc_with_version_dir}\": %m"
                    ))
                    .finish(here("destroy_tablespace_directories"))?;
                return Ok(false);
            }
            /* else let ReadDir report the error */
            None
        }
    };

    if let Some(dirdesc) = dirdesc {
        loop {
            let name =
                match backend_storage_file_fd::allocated_desc::ReadDir(Some(dirdesc), &linkloc_with_version_dir)? {
                    Some(de) => de.d_name,
                    None => break,
                };
            if name == "." || name == ".." {
                continue;
            }

            let subfile = format!("{linkloc_with_version_dir}/{name}");

            /* This check is just to deliver a friendlier error message */
            if !redo && !directory_is_empty(&subfile)? {
                backend_storage_file_fd::allocated_desc::FreeDir(Some(dirdesc))?;
                return Ok(false);
            }

            /* remove empty directory */
            if let Err(errno) = fs::rmdir::call(&subfile)? {
                file_access_error(fail_level, errno)
                    .errmsg(format!("could not remove directory \"{subfile}\": %m"))
                    .finish(here("destroy_tablespace_directories"))?;
            }
        }

        backend_storage_file_fd::allocated_desc::FreeDir(Some(dirdesc))?;
    }

    /* remove version directory */
    if let Err(errno) = fs::rmdir::call(&linkloc_with_version_dir)? {
        file_access_error(fail_level, errno)
            .errmsg(format!(
                "could not remove directory \"{linkloc_with_version_dir}\": %m"
            ))
            .finish(here("destroy_tablespace_directories"))?;
        return Ok(false);
    }

    remove_symlink_phase(&linkloc_with_version_dir, redo, fail_level)
}

/// The `remove_symlink:` label block of `destroy_tablespace_directories`.
fn remove_symlink_phase(
    linkloc_with_version_dir: &str,
    redo: bool,
    fail_level: ErrorLevel,
) -> PgResult<bool> {
    /*
     * Try to remove the symlink.  We must deal with the possibility that it's
     * a directory instead of a symlink (WAL replay; see TablespaceCreateDbspace).
     */
    let linkloc = glob::get_parent_directory::call(linkloc_with_version_dir)?;

    match fs::lstat::call(&linkloc)? {
        StatResult::Failed(saved_errno) => {
            let level = if redo {
                LOG
            } else if saved_errno == ENOENT {
                WARNING
            } else {
                ERROR
            };
            file_access_error(level, saved_errno)
                .errmsg(format!("could not stat file \"{linkloc}\": %m"))
                .finish(here("destroy_tablespace_directories"))?;
        }
        StatResult::Found(StatKind::Dir) => {
            if let Err(saved_errno) = fs::rmdir::call(&linkloc)? {
                let level = if redo {
                    LOG
                } else if saved_errno == ENOENT {
                    WARNING
                } else {
                    ERROR
                };
                file_access_error(level, saved_errno)
                    .errmsg(format!("could not remove directory \"{linkloc}\": %m"))
                    .finish(here("destroy_tablespace_directories"))?;
            }
        }
        StatResult::Found(StatKind::Symlink) => {
            if let Err(saved_errno) = fs::unlink::call(&linkloc)? {
                let level = if redo {
                    LOG
                } else if saved_errno == ENOENT {
                    WARNING
                } else {
                    ERROR
                };
                file_access_error(level, saved_errno)
                    .errmsg(format!("could not remove symbolic link \"{linkloc}\": %m"))
                    .finish(here("destroy_tablespace_directories"))?;
            }
        }
        StatResult::Found(StatKind::Other) => {
            /* Refuse to remove anything that's not a directory or symlink */
            ereport(fail_level)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("\"{linkloc}\" is not a directory or symbolic link"))
                .finish(here("destroy_tablespace_directories"))?;
        }
    }

    Ok(true)
}

/* =========================================================================
 * directory_is_empty   (C lines 852-871)
 * ========================================================================= */

/// `directory_is_empty` — true iff `path` contains no entries beyond `.`/`..`.
pub fn directory_is_empty(path: &str) -> PgResult<bool> {
    let dirdesc = backend_storage_file_fd::allocated_desc::AllocateDir(path)?;

    loop {
        // ReadDir with a None dir mirrors C's NULL DIR* (failed AllocateDir):
        // it reports the open failure at ERROR.
        match backend_storage_file_fd::allocated_desc::ReadDir(dirdesc, path)? {
            Some(de) => {
                if de.d_name == "." || de.d_name == ".." {
                    continue;
                }
                backend_storage_file_fd::allocated_desc::FreeDir(dirdesc)?;
                return Ok(false);
            }
            None => break,
        }
    }

    backend_storage_file_fd::allocated_desc::FreeDir(dirdesc)?;
    Ok(true)
}

/* =========================================================================
 * remove_tablespace_symlink   (C lines 882-924)
 * ========================================================================= */

/// `remove_tablespace_symlink` — remove a symlink (or junction directory) in
/// `pg_tblspc`.  Failure to remove is always an ERROR, but a missing file is OK.
pub fn remove_tablespace_symlink(linkloc: &str) -> PgResult<()> {
    match fs::lstat::call(linkloc)? {
        StatResult::Failed(errno) => {
            if errno == ENOENT {
                return Ok(());
            }
            return Err(file_access_error(ERROR, errno)
                .errmsg(format!("could not stat file \"{linkloc}\": %m"))
                .into_error());
        }
        StatResult::Found(StatKind::Dir) => {
            /*
             * This will fail if the directory isn't empty, but not if it's a
             * junction point.
             */
            if let Err(errno) = fs::rmdir::call(linkloc)? {
                if errno != ENOENT {
                    return Err(file_access_error(ERROR, errno)
                        .errmsg(format!("could not remove directory \"{linkloc}\": %m"))
                        .into_error());
                }
            }
        }
        StatResult::Found(StatKind::Symlink) => {
            if let Err(errno) = fs::unlink::call(linkloc)? {
                if errno != ENOENT {
                    return Err(file_access_error(ERROR, errno)
                        .errmsg(format!("could not remove symbolic link \"{linkloc}\": %m"))
                        .into_error());
                }
            }
        }
        StatResult::Found(StatKind::Other) => {
            /* Refuse to remove anything that's not a directory or symlink */
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("\"{linkloc}\" is not a directory or symbolic link"))
                .into_error());
        }
    }

    Ok(())
}

/* =========================================================================
 * RenameTableSpace   (C lines 929-1009)
 * ========================================================================= */

/// `RenameTableSpace` — rename a tablespace.
pub fn RenameTableSpace<'mcx>(
    mcx: Mcx<'mcx>,
    oldname: &str,
    newname: &str,
) -> PgResult<ObjectAddress> {
    /* Search pg_tablespace */
    let rel = cat::tablespace_table_open::call(mcx, RowExclusiveLock)?;

    let tup = cat::scan_tablespace_by_name::call(&rel, oldname)?;
    let tup = match tup {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("tablespace \"{oldname}\" does not exist"))
                .into_error());
        }
    };

    // heap_copytuple(tup); newform->oid:
    let tspId = tup.oid;

    // table_endscan(scan);

    /* Must be owner */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TABLE_SPACE_RELATION_ID,
        tspId,
        backend_utils_init_miscinit::GetUserId(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NO_PRIV,
            OBJECT_TABLESPACE,
            Some(oldname.to_string()),
        )?;
    }

    /* Validate new name */
    if !glob::allowSystemTableMods::call()? && IsReservedName(newname) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("unacceptable tablespace name \"{newname}\""))
            .errdetail("The prefix \"pg_\" is reserved for system tablespaces.")
            .into_error());
    }

    /* Make sure the new name doesn't exist */
    if cat::scan_tablespace_by_name::call(&rel, newname)?.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("tablespace \"{newname}\" already exists"))
            .into_error());
    }

    /* OK, update the entry */
    // namestrcpy(&newform->spcname, newname); CatalogTupleUpdate(...).
    cat::update_tablespace_name::call(&rel, tup.handle, newname)?;

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TABLE_SPACE_RELATION_ID,
        tspId,
        0,
    )?;

    let address = ObjectAddressSet(TABLE_SPACE_RELATION_ID, tspId);

    cat::tablespace_table_close::call(rel, NoLock)?;

    Ok(address)
}

/* =========================================================================
 * AlterTableSpaceOptions   (C lines 1014-1083)
 * ========================================================================= */

/// `AlterTableSpaceOptions` — ALTER TABLESPACE … SET/RESET options.  Returns the
/// tablespace OID.
pub fn AlterTableSpaceOptions<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTableSpaceOptionsStmt<'mcx>,
) -> PgResult<Oid> {
    let tablespacename: &str = stmt.tablespacename.as_deref().unwrap_or("");

    /* Search pg_tablespace */
    let rel = cat::tablespace_table_open::call(mcx, RowExclusiveLock)?;

    let tup = cat::scan_tablespace_by_name::call(&rel, tablespacename)?;
    let tup = match tup {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("tablespace \"{tablespacename}\" does not exist"))
                .into_error());
        }
    };

    let tablespaceoid = tup.oid;

    /* Must be owner of the existing object */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        backend_utils_init_miscinit::GetUserId(),
    )? {
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_TABLESPACE,
            Some(tablespacename.to_string()),
        )?;
    }

    /*
     * Generate new proposed spcoptions (text array), validate, build new tuple,
     * and update the system catalog.
     */
    let options = materialize_def_elems(mcx, &stmt.options)?;
    cat::update_tablespace_options::call(&rel, tup.handle, &options, stmt.isReset)?;

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TABLE_SPACE_RELATION_ID,
        tablespaceoid,
        0,
    )?;

    /* Conclude heap scan. */
    cat::tablespace_table_close::call(rel, NoLock)?;

    Ok(tablespaceoid)
}

/* =========================================================================
 * check_default_tablespace   (C lines 1090-1124)  — GUC check_hook
 * ========================================================================= */

/// `check_default_tablespace` — check_hook validating `default_tablespace`.
pub fn check_default_tablespace<'mcx>(
    mcx: Mcx<'mcx>,
    newval: &str,
    source: GucSource,
) -> PgResult<bool> {
    /*
     * If we aren't inside a transaction, or connected to a database, we
     * cannot do the catalog accesses necessary to verify the name.  Must
     * accept the value on faith.
     */
    if backend_access_transam_xact::IsTransactionState() && glob::MyDatabaseId::call()? != InvalidOid
    {
        if !newval.is_empty() && !OidIsValid(get_tablespace_oid(mcx, newval, true)?) {
            /*
             * When source == PGC_S_TEST, don't throw a hard error for a
             * nonexistent tablespace, only a NOTICE.
             */
            if source == PGC_S_TEST {
                ereport(NOTICE)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("tablespace \"{newval}\" does not exist"))
                    .finish(here("check_default_tablespace"))?;
            } else {
                backend_utils_misc_guc::GUC_check_errdetail(format!(
                    "Tablespace \"{newval}\" does not exist."
                ));
                return Ok(false);
            }
        }
    }

    Ok(true)
}

/* =========================================================================
 * GetDefaultTablespace   (C lines 1142-1182)
 * ========================================================================= */

/// `GetDefaultTablespace` — OID of the current default tablespace for the given
/// relpersistence.  May return `InvalidOid` ("use the database's default").
pub fn GetDefaultTablespace<'mcx>(
    mcx: Mcx<'mcx>,
    relpersistence: i8,
    partitioned: bool,
) -> PgResult<Oid> {
    /* The temp-table case is handled elsewhere */
    if relpersistence as u8 == RELPERSISTENCE_TEMP {
        PrepareTempTablespaces(mcx)?;
        return Ok(backend_storage_file_fd::temp_files::GetNextTempTableSpace());
    }

    /* Fast path for default_tablespace == "" */
    let default_tablespace = glob::default_tablespace::call()?;
    if default_tablespace.is_empty() {
        return Ok(InvalidOid);
    }

    /*
     * It is tempting to cache this lookup for more speed, but then we would
     * fail to detect the case where the tablespace was dropped since the GUC
     * variable was set.  We don't complain if the value fails to refer to an
     * existing tablespace; we just silently return InvalidOid.
     */
    let mut result = get_tablespace_oid(mcx, &default_tablespace, true)?;

    /*
     * Allow explicit specification of database's default tablespace in
     * default_tablespace without triggering permissions checks.  Don't allow
     * specifying that when creating a partitioned table, however.
     */
    if result == glob::MyDatabaseTableSpace::call()? {
        if partitioned {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot specify default tablespace for partitioned relations")
                .into_error());
        }
        result = InvalidOid;
    }
    Ok(result)
}

/* =========================================================================
 * GUC variable 'temp_tablespaces' handling   (C lines 1196-1416)
 * ========================================================================= */

/// `temp_tablespaces_extra` — the validated OID list `check_temp_tablespaces`
/// builds for `assign_temp_tablespaces`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TempTablespacesExtra {
    tbl_spcs: Vec<Oid>,
}

impl TempTablespacesExtra {
    pub fn new(tbl_spcs: Vec<Oid>) -> Self {
        Self { tbl_spcs }
    }

    pub fn numSpcs(&self) -> usize {
        self.tbl_spcs.len()
    }

    pub fn tblSpcs(&self) -> &[Oid] {
        &self.tbl_spcs
    }
}

/// `check_temp_tablespaces` — check_hook validating `temp_tablespaces`.
pub fn check_temp_tablespaces<'mcx>(
    mcx: Mcx<'mcx>,
    newval: &str,
    extra: &mut Option<TempTablespacesExtra>,
    source: GucSource,
) -> PgResult<bool> {
    let namelist = match SplitIdentifierString(newval, ',') {
        Ok(namelist) => namelist,
        Err(()) => {
            /* syntax error in name list */
            backend_utils_misc_guc::GUC_check_errdetail("List syntax is invalid.");
            return Ok(false);
        }
    };

    /*
     * If we aren't inside a transaction, or connected to a database, we
     * cannot do the catalog accesses necessary to verify the name.  Must
     * accept the value on faith.
     */
    if backend_access_transam_xact::IsTransactionState() && glob::MyDatabaseId::call()? != InvalidOid
    {
        let table_spaces = resolve_temp_tablespaces(mcx, &namelist, source, true)?;
        *extra = Some(TempTablespacesExtra::new(table_spaces));
    }

    Ok(true)
}

/// `assign_temp_tablespaces` — assign_hook for `temp_tablespaces`.
pub fn assign_temp_tablespaces(_newval: &str, extra: Option<&TempTablespacesExtra>) {
    /*
     * If check_temp_tablespaces was executed inside a transaction, then pass
     * the list it made to fd.c.  Otherwise, clear fd.c's list.
     */
    if let Some(extra) = extra {
        backend_storage_file_fd::temp_files::SetTempTablespaces(extra.tblSpcs())
    } else {
        backend_storage_file_fd::temp_files::SetTempTablespaces(&[])
    }
}

/// `PrepareTempTablespaces` — parse `temp_tablespaces` and hand the OID list to
/// fd.c, once per transaction.
pub fn PrepareTempTablespaces<'mcx>(mcx: Mcx<'mcx>) -> PgResult<()> {
    /* No work if already done in current transaction */
    if backend_storage_file_fd::temp_files::TempTablespacesAreSet() {
        return Ok(());
    }

    /*
     * Can't do catalog access unless within a transaction.
     */
    if !backend_access_transam_xact::IsTransactionState() {
        return Ok(());
    }

    let namelist = match SplitIdentifierString(&glob::temp_tablespaces::call()?, ',') {
        Ok(namelist) => namelist,
        Err(()) => {
            /* syntax error in name list */
            backend_storage_file_fd::temp_files::SetTempTablespaces(&[]);
            return Ok(());
        }
    };

    // The Prepare path skips bad list elements SILENTLY (C `continue;`).
    let table_spaces = resolve_prepare_tablespaces(mcx, &namelist)?;
    backend_storage_file_fd::temp_files::SetTempTablespaces(&table_spaces);
    Ok(())
}

/* =========================================================================
 * get_tablespace_oid / get_tablespace_name   (C lines 1425-1504)
 * ========================================================================= */

/// `get_tablespace_oid` — look up a tablespace OID by name.  Errors when not
/// found unless `missing_ok`.
pub fn get_tablespace_oid<'mcx>(
    mcx: Mcx<'mcx>,
    tablespacename: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    /*
     * Search pg_tablespace.  We use a heapscan here even though there is an
     * index on name, on the theory that pg_tablespace will usually have just a
     * few entries.
     */
    let rel = cat::tablespace_table_open::call(mcx, AccessShareLock)?;

    let result = match cat::scan_tablespace_by_name::call(&rel, tablespacename)? {
        Some(t) => t.oid,
        None => InvalidOid,
    };

    cat::tablespace_table_close::call(rel, AccessShareLock)?;

    if !OidIsValid(result) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("tablespace \"{tablespacename}\" does not exist"))
            .into_error());
    }

    Ok(result)
}

/// `get_tablespace_name` — look up a tablespace name by OID.  Returns `None`
/// (the C `NULL`) when no such tablespace exists.
pub fn get_tablespace_name<'mcx>(
    mcx: Mcx<'mcx>,
    spc_oid: Oid,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    /*
     * Search pg_tablespace.  We use a heapscan here even though there is an
     * index on oid.
     */
    let rel = cat::tablespace_table_open::call(mcx, AccessShareLock)?;

    let result = cat::scan_tablespace_name_by_oid::call(mcx, &rel, spc_oid)?;

    cat::tablespace_table_close::call(rel, AccessShareLock)?;

    Ok(result)
}

/* =========================================================================
 * tblspc_redo   (C lines 1510-1569)  — TABLESPACE rmgr redo
 * ========================================================================= */

/// `tblspc_redo` — WAL redo handler for TABLESPACE records.
pub fn tblspc_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !types_wal::wal::XLR_INFO_MASK;
    let record_data = record_get_data(record);

    // Assert(!XLogRecHasAnyBlockRefs(record));

    if info == XLOG_TBLSPC_CREATE {
        // xl_tblspc_create_rec: Oid ts_id; char ts_path[FLEXIBLE_ARRAY_MEMBER];
        let ts_id = decode_oid(&record_data[0..4]);
        let location = decode_cstr(&record_data[4..]);

        create_tablespace_directories(&location, ts_id)
    } else if info == XLOG_TBLSPC_DROP {
        let ts_id = decode_oid(&record_data[0..4]);

        /* Close all smgr fds in all backends. */
        smgr_release_barrier()?;

        /*
         * It is possible for standby users to be using this tablespace as a
         * location for their temporary files, so if we fail to remove all files
         * then do conflict processing and try again, if currently enabled.
         */
        if !destroy_tablespace_directories(ts_id, true)? {
            let ctx = MemoryContext::new("tblspc_redo conflict resolution");
            backend_storage_ipc_standby::ResolveRecoveryConflictWithTablespace(ctx.mcx(), ts_id)?;

            /*
             * Retry before complaining.  If we fail again, this is just a LOG
             * condition.
             */
            if !destroy_tablespace_directories(ts_id, true)? {
                ereport(LOG)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "directories for tablespace {ts_id} could not be removed"
                    ))
                    .errhint("You can remove the directories manually if necessary.")
                    .finish(here("tblspc_redo"))?;
            }
        }
        Ok(())
    } else {
        // elog(PANIC, "tblspc_redo: unknown op code %u", info);
        Err(ereport(PANIC)
            .errmsg_internal(format!("tblspc_redo: unknown op code {info}"))
            .into_error())
    }
}

/* -------------------------------------------------------------------------
 * WAL emit (CreateTableSpace / DropTableSpace XLOG legs)
 * ------------------------------------------------------------------------- */

/// The CreateTableSpace XLOG leg (C lines 360-371): write
/// `xl_tblspc_create_rec` (`ts_id` Oid followed by the NUL-terminated
/// `ts_path`) under `RM_TBLSPC_ID` / `XLOG_TBLSPC_CREATE`.
fn xlog_tblspc_create(tablespaceoid: Oid, location: &str) -> PgResult<()> {
    backend_access_transam_xloginsert::XLogBeginInsert()?;
    // offsetof(xl_tblspc_create_rec, ts_path) == sizeof(Oid) == 4
    backend_access_transam_xloginsert::XLogRegisterData(&tablespaceoid.to_ne_bytes())?;
    // XLogRegisterData(location, strlen(location) + 1) — include the NUL.
    let mut path_bytes = location.as_bytes().to_vec();
    path_bytes.push(0);
    backend_access_transam_xloginsert::XLogRegisterData(&path_bytes)?;
    backend_access_transam_xloginsert::XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_CREATE)?;
    Ok(())
}

/// The DropTableSpace XLOG leg (C lines 530-539): write `xl_tblspc_drop_rec`
/// (just `ts_id`) under `RM_TBLSPC_ID` / `XLOG_TBLSPC_DROP`.
fn xlog_tblspc_drop(tablespaceoid: Oid) -> PgResult<()> {
    backend_access_transam_xloginsert::XLogBeginInsert()?;
    backend_access_transam_xloginsert::XLogRegisterData(&tablespaceoid.to_ne_bytes())?;
    backend_access_transam_xloginsert::XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_DROP)?;
    Ok(())
}

/// `WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE))`
/// — ask all backends to close their smgr fds and wait.
fn smgr_release_barrier() -> PgResult<()> {
    let gen = backend_storage_ipc_procsignal::EmitProcSignalBarrier(
        types_storage::storage::ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
    );
    backend_storage_ipc_procsignal::WaitForProcSignalBarrier(gen)
}

/* -------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------- */

use types_core::primitive::InvalidOid;

/// `Anum_pg_tablespace_oid` (`catalog/pg_tablespace.h`) — the `oid` column.
const ANUM_PG_TABLESPACE_OID: types_core::AttrNumber = 1;

/// `CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT`
/// (`access/xlog.h`).
const CHECKPOINT_IMMEDIATE: i32 = 0x0004;
const CHECKPOINT_FORCE: i32 = 0x0008;
const CHECKPOINT_WAIT: i32 = 0x0020;

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// Build an `ereport(level)` with `errcode_for_file_access()` derived from
/// `errno` and the `errno` saved so a trailing `%m` in the message expands to
/// the system error string.
fn file_access_error(level: ErrorLevel, errno: i32) -> backend_utils_error::ErrorBuilder {
    ereport(level)
        .with_saved_errno(errno)
        .errcode_for_file_access()
}

/// The errno of a failed `AllocateDir` (C reads `errno` after the NULL return).
/// `AllocateDir` opens the dir via `opendir`; we recover the errno by a `stat`
/// probe of the same path, which yields the same `ENOENT`/permission errno the
/// C code would branch on.
fn open_dir_errno(path: &str) -> PgResult<i32> {
    match fs::stat::call(path)? {
        StatResult::Failed(errno) => Ok(errno),
        // The directory exists but AllocateDir still failed (descriptor
        // exhaustion); fall through to the C "let ReadDir report" path errno 0.
        StatResult::Found(_) => Ok(0),
    }
}

/// Decode an `Oid` (4-byte native-endian) from a WAL payload slice.
fn decode_oid(bytes: &[u8]) -> Oid {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[0..4]);
    Oid::from_ne_bytes(buf)
}

/// Decode a NUL-terminated C string from a WAL payload slice.
fn decode_cstr(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// `XLogRecGetData(record)` — the record's main data.
fn record_get_data<'a>(record: &'a types_wal::rmgr::XLogReaderState<'_>) -> &'a [u8] {
    record.record.as_ref().map(|r| r.data()).unwrap_or(&[])
}

/// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
fn record_get_info(record: &types_wal::rmgr::XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

/// Extract the owned `DefElem` list of a statement's `options` (the parser
/// fills `options` with `Node::DefElem`).
fn materialize_def_elems<'mcx>(
    _mcx: Mcx<'mcx>,
    options: &[types_nodes::nodes::NodePtr<'mcx>],
) -> PgResult<Vec<DefElem<'mcx>>> {
    let mut out: Vec<DefElem<'mcx>> = Vec::new();
    out.try_reserve(options.len())
        .map_err(|_| out_of_memory("materialize_def_elems"))?;
    for node in options {
        match &**node {
            Node::DefElem(de) => out.push(de.clone_in(_mcx)?),
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("tablespace options list element is not a DefElem")
                    .into_error())
            }
        }
    }
    Ok(out)
}

/// `palloc` failure surfaced as a recoverable error.
fn out_of_memory(funcname: &'static str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg_internal(format!("out of memory in {funcname}"))
        .into_error()
}

/// `SplitIdentifierString(rawstring, ',', &namelist)` (`varlena.c`) — split a
/// comma-separated identifier list, lower-casing unquoted identifiers and
/// honoring `"…"` quoting. Returns `Err(())` on a syntax error (C `false`).
pub fn SplitIdentifierString(rawstring: &str, separator: char) -> Result<Vec<String>, ()> {
    let mut parser = IdentifierListParser::new(rawstring, separator);
    parser.parse()
}

/// The `check_temp_tablespaces` foreach loop (C lines 1232-1284).
fn resolve_temp_tablespaces<'mcx>(
    mcx: Mcx<'mcx>,
    namelist: &[String],
    source: GucSource,
    interactive_errors: bool,
) -> PgResult<Vec<Oid>> {
    let mut tbl_spcs: Vec<Oid> = Vec::new();
    tbl_spcs
        .try_reserve(namelist.len())
        .map_err(|_| out_of_memory("resolve_temp_tablespaces"))?;
    for curname in namelist {
        /* Allow an empty string (signifying database default) */
        if curname.is_empty() {
            tbl_spcs.push(InvalidOid);
            continue;
        }

        /*
         * In an interactive SET command, we ereport for bad info.  When
         * source == PGC_S_TEST, don't throw a hard error for a nonexistent
         * tablespace, only a NOTICE.
         */
        let curoid = get_tablespace_oid(mcx, curname, source <= PGC_S_TEST)?;
        if curoid == InvalidOid {
            if source == PGC_S_TEST {
                ereport(NOTICE)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("tablespace \"{curname}\" does not exist"))
                    .finish(here("check_temp_tablespaces"))?;
            }
            continue;
        }

        /*
         * Allow explicit specification of database's default tablespace in
         * temp_tablespaces without triggering permissions checks.
         */
        if curoid == glob::MyDatabaseTableSpace::call()? {
            tbl_spcs.push(InvalidOid);
            continue;
        }

        /* Check permissions, similarly complaining only if interactive */
        let aclresult = backend_catalog_aclchk_seams::object_aclcheck::call(
            TABLE_SPACE_RELATION_ID,
            curoid,
            backend_utils_init_miscinit::GetUserId(),
            ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            if interactive_errors && source >= PGC_S_INTERACTIVE {
                backend_catalog_aclchk_seams::aclcheck_error::call(
                    aclresult,
                    OBJECT_TABLESPACE,
                    Some(curname.clone()),
                )?;
            }
            continue;
        }

        tbl_spcs.push(curoid);
    }
    Ok(tbl_spcs)
}

/// The `PrepareTempTablespaces` foreach loop (C lines 1370-1410): skips invalid
/// entries SILENTLY (no NOTICE) and never raises an ACL error.
fn resolve_prepare_tablespaces<'mcx>(mcx: Mcx<'mcx>, namelist: &[String]) -> PgResult<Vec<Oid>> {
    let mut tbl_spcs: Vec<Oid> = Vec::new();
    tbl_spcs
        .try_reserve(namelist.len())
        .map_err(|_| out_of_memory("resolve_prepare_tablespaces"))?;
    for curname in namelist {
        /* Allow an empty string (signifying database default) */
        if curname.is_empty() {
            tbl_spcs.push(InvalidOid);
            continue;
        }

        /* Else verify that name is a valid tablespace name */
        let curoid = get_tablespace_oid(mcx, curname, true)?;
        if curoid == InvalidOid {
            /* Skip any bad list elements — NO notice */
            continue;
        }

        /*
         * Allow explicit specification of database's default tablespace in
         * temp_tablespaces without triggering permissions checks.
         */
        if curoid == glob::MyDatabaseTableSpace::call()? {
            tbl_spcs.push(InvalidOid);
            continue;
        }

        /* Check permissions similarly */
        if backend_catalog_aclchk_seams::object_aclcheck::call(
            TABLE_SPACE_RELATION_ID,
            curoid,
            backend_utils_init_miscinit::GetUserId(),
            ACL_CREATE,
        )? != ACLCHECK_OK
        {
            continue;
        }

        tbl_spcs.push(curoid);
    }
    Ok(tbl_spcs)
}

/* -------------------------------------------------------------------------
 * SplitIdentifierString identifier-list parser (varlena.c)
 * ------------------------------------------------------------------------- */

struct IdentifierListParser<'a> {
    input: &'a str,
    separator: char,
    index: usize,
}

impl<'a> IdentifierListParser<'a> {
    fn new(input: &'a str, separator: char) -> Self {
        Self {
            input,
            separator,
            index: 0,
        }
    }

    fn parse(&mut self) -> Result<Vec<String>, ()> {
        let mut names = Vec::new();
        self.skip_ws();
        if self.is_done() {
            return Ok(names);
        }

        loop {
            let mut name = if self.peek() == Some('"') {
                self.parse_quoted()?
            } else {
                self.parse_unquoted()?
            };
            self.skip_ws();

            let done = match self.peek() {
                Some(ch) if ch == self.separator => {
                    self.bump();
                    self.skip_ws();
                    false
                }
                None => true,
                _ => return Err(()),
            };

            truncate_identifier(&mut name);
            names.try_reserve(1).map_err(|_| ())?;
            names.push(name);

            if done {
                return Ok(names);
            }
            if self.is_done() {
                return Err(());
            }
        }
    }

    fn parse_quoted(&mut self) -> Result<String, ()> {
        self.expect('"')?;
        let mut name = String::new();
        loop {
            match self.bump() {
                Some('"') if self.peek() == Some('"') => {
                    self.bump();
                    name.push('"');
                }
                Some('"') => return Ok(name),
                Some(ch) => name.push(ch),
                None => return Err(()),
            }
        }
    }

    fn parse_unquoted(&mut self) -> Result<String, ()> {
        let start = self.index;
        while let Some(ch) = self.peek() {
            if ch == self.separator || scanner_isspace(ch) {
                break;
            }
            self.bump();
        }
        if self.index == start {
            return Err(());
        }
        Ok(self.input[start..self.index].to_ascii_lowercase())
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(scanner_isspace) {
            self.bump();
        }
    }

    fn expect(&mut self, expected: char) -> Result<(), ()> {
        match self.bump() {
            Some(actual) if actual == expected => Ok(()),
            _ => Err(()),
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.index..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.index += ch.len_utf8();
        Some(ch)
    }

    fn is_done(&self) -> bool {
        self.index >= self.input.len()
    }
}

/// `scanner_isspace(ch)` (parser/scansup.c): space, tab, newline, carriage
/// return, vertical tab, or form feed.
fn scanner_isspace(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c')
}

/// `truncate_identifier(ident, len, false)` (`scansup.c`): clamp to
/// `NAMEDATALEN - 1` bytes (on a char boundary).
fn truncate_identifier(name: &mut String) {
    let max_len = NAMEDATALEN - 1;
    if name.len() <= max_len {
        return;
    }
    let mut end = max_len;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name.truncate(end);
}

/* -------------------------------------------------------------------------
 * Seam installation
 * ------------------------------------------------------------------------- */

/// Install the outward seams this unit owns (`commands/tablespace.h` callees
/// consumed by rmgr / acl / pg_shdepend). The fine-grained catalog/FS/globals
/// primitives this crate *consumes* are installed by their own owners.
pub fn init_seams() {
    backend_commands_tablespace_seams::tblspc_redo::set(tblspc_redo);
    backend_commands_tablespace_seams::get_tablespace_name::set(get_tablespace_name);
    backend_commands_tablespace_seams::get_tablespace_oid::set(|name, missing_ok| {
        // The outward seam carries no mcx; open a transient catalog context
        // (mirrors C's use of CurrentMemoryContext for the short-lived scan).
        let ctx = MemoryContext::new("get_tablespace_oid");
        get_tablespace_oid(ctx.mcx(), name, missing_ok)
    });
    backend_commands_tablespace_seams::prepare_temp_tablespaces::set(|| {
        let ctx = MemoryContext::new("PrepareTempTablespaces");
        PrepareTempTablespaces(ctx.mcx())
    });
    backend_commands_tablespace_seams::tablespace_create_dbspace::set(TablespaceCreateDbspace);

    // --- ProcessUtility dispatch arms (utility.c tablespace globals) ---------
    backend_tcop_utility_out_seams::create_table_space::set(create_table_space_arm);
    backend_tcop_utility_out_seams::drop_table_space::set(drop_table_space_arm);
    backend_tcop_utility_out_seams::alter_table_space_options::set(alter_table_space_options_arm);
}

/// `case T_CreateTableSpaceStmt: CreateTableSpace(stmt)` (utility.c). Extract the
/// variant from the dispatch's `&Node` and forward (the created Oid is unused by
/// the dispatch, exactly as in C).
fn create_table_space_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Node::CreateTableSpaceStmt(s) = stmt else {
        panic!("create_table_space: parse tree is not a CreateTableSpaceStmt");
    };
    CreateTableSpace(mcx, s)?;
    Ok(())
}

/// `case T_DropTableSpaceStmt: DropTableSpace(stmt)` (utility.c).
fn drop_table_space_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Node::DropTableSpaceStmt(s) = stmt else {
        panic!("drop_table_space: parse tree is not a DropTableSpaceStmt");
    };
    DropTableSpace(mcx, s)
}

/// `case T_AlterTableSpaceOptionsStmt: AlterTableSpaceOptions(stmt)` (utility.c).
fn alter_table_space_options_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Node::AlterTableSpaceOptionsStmt(s) = stmt else {
        panic!("alter_table_space_options: parse tree is not an AlterTableSpaceOptionsStmt");
    };
    AlterTableSpaceOptions(mcx, s)?;
    Ok(())
}

#[cfg(test)]
mod tests;
