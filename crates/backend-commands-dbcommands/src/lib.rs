#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is the large shared error type; boxing it would diverge from every
// sibling crate's Result shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/dbcommands.c` — CREATE / ALTER / DROP / RENAME DATABASE,
//! the pg_database name/oid lookups, the `database_is_invalid_*` predicates,
//! and the `dbase_redo` WAL handler.
//!
//! This unit owns and installs the four inward seams declared in
//! [`backend_commands_dbcommands_seams`] that landed consumers
//! (parse-target/procarray/comment/postinit/the rmgr table) already depend on:
//! [`get_database_name`], [`get_database_oid`], [`database_is_invalid_form`],
//! and [`dbase_redo`].
//!
//! ## Coverage
//!
//! Fully ported here (family F0 + the WAL handler), branch-for-branch with the
//! C:
//!  * [`get_db_info`] — the `table_open` + by-name scan / `LockSharedObject` /
//!    re-fetch-by-oid / name-recheck retry loop, returning the decoded
//!    [`FormPgDatabase`] (the C reads every column off `GETSTRUCT`/
//!    `SysCacheGetAttr*`, which the landed pg_database read seams decode once);
//!  * [`get_database_oid`] / [`get_database_name`] — the name↔oid lookups;
//!  * [`database_is_invalid_form`] / [`database_is_invalid_oid`] — the
//!    `datconnlimit == DATCONNLIMIT_INVALID_DB` predicate + its syscache
//!    convenience wrapper;
//!  * [`have_createdb_privilege`] — `superuser() || rolcreatedb`;
//!  * [`errdetail_busy_db`] — the busy-database errdetail (English plural
//!    forms, the project-wide gettext deferral);
//!  * [`recovery_create_dbdir`] — the recovery directory-creation helper;
//!  * [`dbase_redo`] — the DATABASE rmgr replay routine: the `info`-byte
//!    dispatch + every replay decision in-crate, the filesystem / buffer /
//!    barrier / slot / xlog-relcache primitives through their owners' seams.
//!
//! The createdb / dropdb / RenameDatabase / movedb / AlterDatabase* family
//! (the heavy half of dbcommands.c) is ported in [`mod@heavy`]:
//!  * [`createdb`] (both WAL_LOG and FILE_COPY strategies, with
//!    `CreateDirAndVersionFile` / `check_encoding_locale_matches` /
//!    `createdb_failure_callback`);
//!  * [`dropdb`] / [`DropDatabase`];
//!  * [`RenameDatabase`]; [`AlterDatabaseOwner`];
//!  * `movedb` (ALTER DATABASE SET TABLESPACE) + `movedb_failure_callback`;
//!  * [`AlterDatabase`] / [`AlterDatabaseRefreshColl`] / [`AlterDatabaseSet`];
//!  * [`pg_database_collation_actual_version`];
//!  * `remove_dbtablespaces` / `check_db_file_conflict`.
//!
//! The cross-subsystem surface (smgr/relmapper/checkpoint barriers, the
//! createdb file-copy engine, replication-slot/subscription counts, pgstat,
//! ACL/role rewrite, GUC settings, the tablespace catalog scan) is reached
//! through each owner's real fns or seams; the pg_database catalog writes go
//! through the landed `backend-catalog-pg-database` mutate seams.

extern crate alloc;

mod fmgr_builtins;
mod heavy;
pub use heavy::{
    createdb, check_encoding_locale_matches, dropdb, AlterDatabase, AlterDatabaseOwner,
    AlterDatabaseRefreshColl, AlterDatabaseSet, DropDatabase, RenameDatabase,
    pg_database_collation_actual_version,
};

use alloc::format;
use alloc::string::{String, ToString};

use backend_commands_dbcommands_seams as inward;
use backend_catalog_pg_database_seams as dbcat;

use backend_utils_error::ereport;
use mcx::{Mcx, PgString};
use types_catalog::pg_database::{
    DatabaseRelationId, FormPgDatabase, DATCONNLIMIT_INVALID_DB,
};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_UNDEFINED_DATABASE, ERROR, PANIC, WARNING,
};
use types_storage::lock::{AccessExclusiveLock, LOCKMODE, NoLock};
use types_storage::storage::ProcSignalBarrierType;
use types_wal::rmgr::XLogReaderState;

// ---------------------------------------------------------------------------
// Resource-manager opcode bits (commands/dbcommands_xlog.h) for dbase_redo.
// ---------------------------------------------------------------------------
const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
const XLOG_DBASE_DROP: u8 = 0x20;

/// `InplaceUpdateTupleLock` / the heavyweight `AccessExclusiveLock` taken on a
/// shared object — `dbase_redo`'s hot-standby drop path locks the database with
/// `AccessExclusiveLock` (xact.h `InplaceUpdateTupleLock` is the same value).
const HotStandbyDropLock: LOCKMODE = AccessExclusiveLock;

/// `ErrorLocation` helper — dbcommands.c is `src/backend/commands/dbcommands.c`.
pub(crate) fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/dbcommands.c", lineno, funcname)
}

// ===========================================================================
// get_db_info — C 2821-2976
// ===========================================================================

/// Collect data about a database (`get_db_info`).  Returns the decoded
/// [`FormPgDatabase`], or `None` when no such database exists.  When `lockmode`
/// is not `NoLock`, the shared-object lock on the resolved database OID is left
/// held for the rest of the transaction (the C default).
///
/// The C reads each requested column off the syscache tuple; this returns the
/// whole decoded row (the landed read seams already do the `heap_copytuple` /
/// `SysCacheGetAttr*` decode), so the caller projects the fields it wants.
pub fn get_db_info<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    lockmode: LOCKMODE,
) -> PgResult<Option<FormPgDatabase<'mcx>>> {
    // The landed read seams open/scan/close pg_database themselves; this loop
    // mirrors C's "scan by name → lock by oid → re-fetch by oid → recheck
    // name" retry, which covers the rare rename-before-lock race.
    loop {
        // There's no syscache for database-indexed-by-name, so scan by name.
        let db_oid = match dbcat::get_database_tuple_by_name::call(mcx, name)? {
            Some(form) => form.oid,
            None => {
                // Definitely no database of that name.
                return Ok(None);
            }
        };

        // Now that we have a database OID, lock the DB.
        if lockmode != NoLock {
            backend_storage_lmgr_lmgr_seams::lock_shared_object::call(
                DatabaseRelationId,
                db_oid,
                0,
                lockmode,
            )?
            .keep();
        }

        // Re-fetch by OID.  If still there and still the same name, we win;
        // else drop the lock and loop back to try again.
        if let Some(form) = dbcat::search_database_syscache::call(mcx, db_oid)? {
            if name == form.datname.as_str() {
                return Ok(Some(form));
            }
            // Can only get here if it was just renamed.
        }

        if lockmode != NoLock {
            backend_storage_lmgr_lmgr_seams::unlock_shared_object::call(
                DatabaseRelationId,
                db_oid,
                0,
                lockmode,
            )?;
        }
    }
}

// ===========================================================================
// have_createdb_privilege — C 2978-2995
// ===========================================================================

/// Check if current user has createdb privileges.
pub fn have_createdb_privilege(mcx: Mcx<'_>) -> PgResult<bool> {
    // Superusers can always do everything.
    if backend_utils_misc_superuser_seams::superuser::call()? {
        return Ok(true);
    }

    let roleid = backend_utils_init_miscinit_seams::get_user_id::call();
    Ok(backend_catalog_pg_authid_seams::user_rolcreatedb::call(mcx, roleid)?.unwrap_or(false))
}

// ===========================================================================
// errdetail_busy_db — C 3137-3158
// ===========================================================================

/// Build a suitable errdetail message for a busy database.  gettext plural is
/// the project deferral, so the English forms are emitted directly.
pub fn errdetail_busy_db(notherbackends: i32, npreparedxacts: i32) -> String {
    if notherbackends > 0 && npreparedxacts > 0 {
        // No singular/plural here (gettext can't carry two plurals at once).
        format!(
            "There are {notherbackends} other session(s) and {npreparedxacts} prepared transaction(s) using the database."
        )
    } else if notherbackends > 0 {
        if notherbackends == 1 {
            "There is 1 other session using the database.".to_string()
        } else {
            format!("There are {notherbackends} other sessions using the database.")
        }
    } else if npreparedxacts == 1 {
        "There is 1 prepared transaction using the database.".to_string()
    } else {
        format!("There are {npreparedxacts} prepared transactions using the database.")
    }
}

// ===========================================================================
// get_database_oid — C 3167-3205
// ===========================================================================

/// Given a database name, look up the OID.  If `missing_ok` is false, error
/// when not found; if true, return `InvalidOid`.
///
/// Inward seam (consumed by comment.c's COMMENT ON DATABASE path).
pub fn get_database_oid(dbname: &str, missing_ok: bool) -> PgResult<Oid> {
    // There's no syscache for pg_database indexed by name, so look the hard
    // way (the read seam does the table_open + systable scan + close). The
    // inward seam takes no `Mcx`, so the lookup runs in a transient context;
    // only the `Oid` (Copy) escapes it.
    let ctx = mcx::MemoryContext::new("get_database_oid");
    let oid = match dbcat::get_database_tuple_by_name::call(ctx.mcx(), dbname)? {
        Some(form) => form.oid,
        None => InvalidOid,
    };

    if !OidIsValid(oid) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .finish(errloc(3202, "get_database_oid"))
            .unwrap_err());
    }

    Ok(oid)
}

// ===========================================================================
// get_database_name — C 3214-3229
// ===========================================================================

/// Given a database OID, look up the name.  Returns a copy in `mcx`, or `None`
/// if no such database.
///
/// Inward seam (consumed by parse-target / procarray).
pub fn get_database_name<'mcx>(mcx: Mcx<'mcx>, dbid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    Ok(dbcat::search_database_syscache::call(mcx, dbid)?.map(|form| form.datname))
}

// ===========================================================================
// database_is_invalid_form — C 3238-3242
// ===========================================================================

/// While dropping a database the pg_database row is marked invalid (the
/// `datconnlimit == DATCONNLIMIT_INVALID_DB` sentinel).
///
/// Inward seam (consumed by postinit.c).  Only `datconnlimit` crosses.
pub fn database_is_invalid_form(datconnlimit: i32) -> bool {
    datconnlimit == DATCONNLIMIT_INVALID_DB
}

// ===========================================================================
// database_is_invalid_oid — C 3248-3264
// ===========================================================================

/// Convenience wrapper around [`database_is_invalid_form`].
pub fn database_is_invalid_oid<'mcx>(mcx: Mcx<'mcx>, dboid: Oid) -> PgResult<bool> {
    let dbform = match dbcat::search_database_syscache::call(mcx, dboid)? {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for database {dboid}"))
                .finish(errloc(3256, "database_is_invalid_oid"))
                .unwrap_err());
        }
    };
    Ok(database_is_invalid_form(dbform.datconnlimit))
}

// ===========================================================================
// recovery_create_dbdir — C 3281-3303
// ===========================================================================

const PG_TBLSPC_DIR_SLASH: &str = "pg_tblspc/";

/// During recovery, recover a missing tablespace directory so recovery can
/// continue.  If `only_tblspc`, the requested directory must be in `pg_tblspc/`.
fn recovery_create_dbdir(path: &str, only_tblspc: bool) -> PgResult<()> {
    // Assert(RecoveryInProgress());

    // if (stat(path) == 0 && S_ISDIR) return;
    if let Some(st) = backend_storage_file_fd_seams::stat_file::call(path, true)? {
        if st.isdir {
            return Ok(());
        }
    }

    if only_tblspc && !path.contains(PG_TBLSPC_DIR_SLASH) {
        return Err(ereport(PANIC)
            .errmsg(format!("requested to created invalid directory: {path}"))
            .finish(errloc(3291, "recovery_create_dbdir"))
            .unwrap_err());
    }

    let reached = backend_access_transam_xlogrecovery_seams::reached_consistency::call();
    if reached && !backend_commands_tablespace_globals_seams::allow_in_place_tablespaces::call()? {
        return Err(ereport(PANIC)
            .errmsg(format!("missing directory \"{path}\""))
            .finish(errloc(3295, "recovery_create_dbdir"))
            .unwrap_err());
    }

    ereport(if reached { WARNING } else { DEBUG1 })
        .errmsg(format!("creating missing directory: {path}"))
        .finish(errloc(3298, "recovery_create_dbdir"))?;

    if let Err(errno) = backend_storage_file_fd_seams::pg_mkdir_p::call(path) {
        return Err(ereport(PANIC)
            .errmsg(format!(
                "could not create missing directory \"{path}\": {errno}"
            ))
            .finish(errloc(3302, "recovery_create_dbdir"))
            .unwrap_err());
    }

    Ok(())
}

// ===========================================================================
// dbase_redo — C 3309-3472
// ===========================================================================

/// DATABASE resource manager's replay routine.  The `info`-byte dispatch and
/// every replay decision stay in-crate; the filesystem / buffer / barrier /
/// slot / xlog-relcache primitives cross to their owners.
///
/// Inward seam (the rmgr table's `rm_redo` slot).
pub fn dbase_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("dbase_redo: reader has no decoded record");

    // XLogRecGetInfo(record) & ~XLR_INFO_MASK.
    const XLR_INFO_MASK: u8 = 0x0F;
    let info = decoded.info() & !XLR_INFO_MASK;

    // Backup blocks are not used in dbase records.
    debug_assert!(!decoded.has_block_ref(0));

    let data = decoded.data();

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let xlrec = types_wal::rmgrdesc::xl_dbase_create_file_copy_rec::from_bytes(data)
            .expect("dbase_redo: short xl_dbase_create_file_copy_rec");

        let src_path =
            backend_common_relpath::GetDatabasePath(xlrec.src_db_id(), xlrec.src_tablespace_id());
        let dst_path =
            backend_common_relpath::GetDatabasePath(xlrec.db_id(), xlrec.tablespace_id());

        // Our theory for replaying a CREATE is to forcibly drop the target
        // subdirectory if present, then re-copy the source data.
        if let Some(st) = backend_storage_file_fd_seams::stat_file::call(&dst_path, true)? {
            if st.isdir && !backend_storage_file_fd_seams::rmtree::call(&dst_path, true) {
                // If this failed, copydir() below is going to error.
                ereport(WARNING)
                    .errmsg(format!(
                        "some useless files may be left behind in old database directory \"{dst_path}\""
                    ))
                    .finish(errloc(3340, "dbase_redo"))?;
            }
        }

        // If the parent of the target path doesn't exist, create it now.
        let parent_path = get_parent_directory(&dst_path);
        if backend_storage_file_fd_seams::stat_file::call(&parent_path, true)?.is_none() {
            // C: errno != ENOENT => FATAL; stat_file maps ENOENT to None and
            // raises other errors as Err, so reaching None here is the ENOENT
            // case — create the parent directory if needed and valid.
            recovery_create_dbdir(&parent_path, true)?;
        }

        // There's a case where the copy source directory is missing for the
        // same reason; create the empty source dir so copydir doesn't fail.
        if backend_storage_file_fd_seams::stat_file::call(&src_path, true)?.is_none() {
            recovery_create_dbdir(&src_path, false)?;
        }

        // Force dirty buffers out to disk, to ensure source is up-to-date.
        backend_storage_buffer_bufmgr_seams::flush_database_buffers::call(xlrec.src_db_id())?;

        // Close all smgr fds in all backends.
        let gen = backend_storage_ipc_procsignal::EmitProcSignalBarrier(
            ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
        );
        backend_storage_ipc_procsignal::WaitForProcSignalBarrier(gen)?;

        // Copy this subdirectory to the new location (no subdirectories).
        backend_storage_file_copydir::copydir(&src_path, &dst_path, false)?;
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let xlrec = types_wal::rmgrdesc::xl_dbase_create_wal_log_rec::from_bytes(data)
            .expect("dbase_redo: short xl_dbase_create_wal_log_rec");

        let dbpath = backend_common_relpath::GetDatabasePath(xlrec.db_id(), xlrec.tablespace_id());

        // Create the parent directory if needed and valid.
        let parent_path = get_parent_directory(&dbpath);
        recovery_create_dbdir(&parent_path, true)?;

        // Create the database directory with the version file. The
        // `MakePGDirectory` + `OpenTransientFile`/`write`/`pg_fsync` +
        // `fsync_fname` filesystem sequence is fd-owned (`isRedo = true`, so
        // an already-existing dir/version file is tolerated); the !isRedo WAL
        // emission of `CreateDirAndVersionFile` is in the deferred createdb
        // family and never reached on the redo path.
        backend_storage_file_fd_seams::create_db_dir_and_version_file::call(
            &dbpath,
            xlrec.db_id(),
            xlrec.tablespace_id(),
            true,
        )?;
    } else if info == XLOG_DBASE_DROP {
        let xlrec = types_wal::rmgrdesc::xl_dbase_drop_rec::from_bytes(data)
            .expect("dbase_redo: short xl_dbase_drop_rec");
        let db_id = xlrec.db_id();

        let in_hot_standby = backend_storage_ipc_standby_seams::in_hot_standby::call();
        if in_hot_standby {
            // Lock database while we resolve conflicts to ensure
            // InitPostgres() cannot fully re-execute concurrently (also locks
            // out walsenders connecting to db-specific slots for decoding, so
            // it's safe to drop slots).
            backend_storage_lmgr_lmgr_seams::lock_shared_object_for_session::call(
                DatabaseRelationId,
                db_id,
                0,
                HotStandbyDropLock,
            )?;
            backend_storage_ipc_standby_seams::resolve_recovery_conflict_with_database::call(db_id)?;
        }

        // Drop any database-specific replication slots.
        backend_replication_slot::ReplicationSlotsDropDBSlots(
            db_id,
            backend_utils_init_miscinit_seams::my_proc_pid::call(),
        )?;

        // Drop pages for this database in the shared buffer cache.
        backend_storage_buffer_bufmgr_seams::drop_database_buffers::call(db_id)?;

        // Clean out any fsync requests pending in md.c.
        backend_storage_smgr_md::ForgetDatabaseSyncRequests(db_id)?;

        // Clean out the xlog relcache too.
        backend_access_transam_xlogutils::XLogDropDatabase(db_id)?;

        // Close all smgr fds in all backends.
        let gen = backend_storage_ipc_procsignal::EmitProcSignalBarrier(
            ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
        );
        backend_storage_ipc_procsignal::WaitForProcSignalBarrier(gen)?;

        for tblspc in xlrec.tablespace_ids() {
            let dst_path = backend_common_relpath::GetDatabasePath(db_id, tblspc);

            // And remove the physical files.
            if !backend_storage_file_fd_seams::rmtree::call(&dst_path, true) {
                ereport(WARNING)
                    .errmsg(format!(
                        "some useless files may be left behind in old database directory \"{dst_path}\""
                    ))
                    .finish(errloc(3450, "dbase_redo"))?;
            }
        }

        if in_hot_standby {
            // Release locks prior to commit. (C notes a small race window.)
            backend_storage_lmgr_lmgr_seams::unlock_shared_object_for_session::call(
                DatabaseRelationId,
                db_id,
                0,
                HotStandbyDropLock,
            )?;
        }
    } else {
        return Err(ereport(PANIC)
            .errmsg(format!("dbase_redo: unknown op code {info}"))
            .finish(errloc(3471, "dbase_redo"))
            .unwrap_err());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers for the dbcommands.c primitives whose owners are not yet ported.
// ---------------------------------------------------------------------------

/// `get_parent_directory(path)` (port/path.c): strip the last path component,
/// yielding the parent directory.  A path with no slash becomes `"."` (C leaves
/// an empty string, which `stat` treats as the current directory — we keep the
/// behaviour-preserving `"."`).
pub(crate) fn get_parent_directory(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => path[..idx].to_string(),
        None => ".".to_string(),
    }
}

// ===========================================================================
// Seam wiring.
// ===========================================================================

/// `RenameDatabase` inward-seam shape (the landed consumer passes no `Mcx`): run
/// the rename in a transient context. Only the `ObjectAddress` (Copy) escapes.
fn rename_database_seam(
    oldname: &str,
    newname: &str,
) -> PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    let ctx = mcx::MemoryContext::new("RenameDatabase");
    heavy::RenameDatabase(ctx.mcx(), oldname, newname)
}

/// `AlterDatabaseOwner` inward-seam shape (no `Mcx`): run in a transient context.
fn alter_database_owner_seam(
    dbname: &str,
    new_owner_id: Oid,
) -> PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    let ctx = mcx::MemoryContext::new("AlterDatabaseOwner");
    heavy::AlterDatabaseOwner(ctx.mcx(), dbname, new_owner_id)
}

/// Install the inward seams this unit owns.
pub fn init_seams() {
    // Register this crate's SQL-callable builtins into the fmgr-core table.
    fmgr_builtins::register_dbcommands_builtins();

    inward::dbase_redo::set(dbase_redo);
    inward::get_database_name::set(get_database_name);
    inward::database_is_invalid_form::set(database_is_invalid_form);
    inward::get_database_oid::set(get_database_oid);
    inward::RenameDatabase::set(rename_database_seam);
    inward::AlterDatabaseOwner::set(alter_database_owner_seam);

    // --- ProcessUtility dispatch arms (utility.c database globals) -----------
    backend_tcop_utility_out_seams::createdb::set(createdb_arm);
    backend_tcop_utility_out_seams::alter_database::set(alter_database_arm);
    backend_tcop_utility_out_seams::alter_database_refresh_coll::set(alter_database_refresh_coll_arm);
    backend_tcop_utility_out_seams::alter_database_set::set(alter_database_set_arm);
    backend_tcop_utility_out_seams::drop_database::set(drop_database_arm);
}

use types_nodes::nodes::Node as UtilNode;
use types_nodes::parsestmt::ParseState as UtilParseState;

/// `case T_CreatedbStmt: createdb(pstate, stmt)` (utility.c). The dispatch carries
/// the parse tree as `&Node` and supplies the working `mcx`; `createdb` opens its
/// own transient context internally and returns the new Oid (unused here).
fn createdb_arm<'mcx>(
    _mcx: Mcx<'mcx>,
    pstate: &mut UtilParseState<'mcx>,
    stmt: &UtilNode<'mcx>,
) -> PgResult<()> {
    let UtilNode::CreatedbStmt(s) = stmt else {
        panic!("createdb: parse tree is not a CreatedbStmt");
    };
    createdb(pstate, s)?;
    Ok(())
}

/// `case T_AlterDatabaseStmt: AlterDatabase(pstate, stmt, isTopLevel)` (utility.c).
fn alter_database_arm<'mcx>(
    _mcx: Mcx<'mcx>,
    pstate: &mut UtilParseState<'mcx>,
    stmt: &UtilNode<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let UtilNode::AlterDatabaseStmt(s) = stmt else {
        panic!("alter_database: parse tree is not an AlterDatabaseStmt");
    };
    AlterDatabase(pstate, s, is_top_level)?;
    Ok(())
}

/// `case T_AlterDatabaseRefreshCollStmt: AlterDatabaseRefreshColl(stmt)` (utility.c).
fn alter_database_refresh_coll_arm<'mcx>(_mcx: Mcx<'mcx>, stmt: &UtilNode<'mcx>) -> PgResult<()> {
    let UtilNode::AlterDatabaseRefreshCollStmt(s) = stmt else {
        panic!("alter_database_refresh_coll: parse tree is not an AlterDatabaseRefreshCollStmt");
    };
    AlterDatabaseRefreshColl(s)?;
    Ok(())
}

/// `case T_AlterDatabaseSetStmt: AlterDatabaseSet(stmt)` (utility.c).
fn alter_database_set_arm<'mcx>(_mcx: Mcx<'mcx>, stmt: &UtilNode<'mcx>) -> PgResult<()> {
    let UtilNode::AlterDatabaseSetStmt(s) = stmt else {
        panic!("alter_database_set: parse tree is not an AlterDatabaseSetStmt");
    };
    AlterDatabaseSet(s)?;
    Ok(())
}

/// `case T_DropdbStmt: DropDatabase(pstate, stmt)` (utility.c).
fn drop_database_arm<'mcx>(
    _mcx: Mcx<'mcx>,
    pstate: &mut UtilParseState<'mcx>,
    stmt: &UtilNode<'mcx>,
) -> PgResult<()> {
    let UtilNode::DropdbStmt(s) = stmt else {
        panic!("drop_database: parse tree is not a DropdbStmt");
    };
    DropDatabase(pstate, s)
}
