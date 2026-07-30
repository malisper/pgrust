//! tablespace.c: tablespace DDL, directory/symlink management, tblspc rmgr.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::Cell;

use datum::Datum;
use elog::ereport;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, OidIsValid, NAMEDATALEN};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_NAME, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_RESERVED_NAME, ERRCODE_UNDEFINED_FILE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, LOG, NOTICE, WARNING,
};
use types_nodes::parsenodes::{
    AlterTableSpaceOptionsStmt, CreateTableSpaceStmt, DropTableSpaceStmt, ObjectType,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::file::{PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY};
use types_storage::lock::{AccessShareLock, RowExclusiveLock};
use types_storage::storage::ProcSignalBarrierType;
use types_tuple::NameData;

pub const TableSpaceRelationId: Oid = 1213;
pub const TablespaceOidIndexId: Oid = 2697;
pub const DEFAULTTABLESPACE_OID: Oid = 1663;
pub const GLOBALTABLESPACE_OID: Oid = 1664;

const Natts_pg_tablespace: usize = 5;
const Anum_pg_tablespace_oid: usize = 1;
const Anum_pg_tablespace_spcname: usize = 2;
const Anum_pg_tablespace_spcowner: usize = 3;
const Anum_pg_tablespace_spcacl: usize = 4;
const Anum_pg_tablespace_spcoptions: usize = 5;

const SharedDescriptionRelationId: Oid = 2396;
const SharedDescriptionObjIndexId: Oid = 2397;
const SharedSecLabelRelationId: Oid = 3592;
const SharedSecLabelObjectIndexId: Oid = 3593;

pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
pub const XLOG_TBLSPC_DROP: u8 = 0x10;
const XLR_INFO_MASK: u8 = 0x0F;

// lwlocklist.h position of TablespaceCreateLock.
const TABLESPACE_CREATE_LOCK: usize = 19;

// C: OIDCHARS 10, FORKNAMECHARS 4, MAXPGPATH 1024.
const MAXPGPATH: usize = 1024;
const OIDCHARS: usize = 10;
const FORKNAMECHARS: usize = 4;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// binary_upgrade_next_pg_tablespace_oid (pg_upgrade_support.c): set-once,
// consume-once override for CreateTableSpace's OID assignment.
thread_local! {
    static NEXT_PG_TABLESPACE_OID: Cell<Oid> = const { Cell::new(InvalidOid) };
}

pub fn SetNextPgTablespaceOid(oid: Oid) {
    NEXT_PG_TABLESPACE_OID.set(oid);
}

fn take_next_pg_tablespace_oid() -> Option<Oid> {
    let oid = NEXT_PG_TABLESPACE_OID.get();
    if OidIsValid(oid) {
        NEXT_PG_TABLESPACE_OID.set(InvalidOid);
        Some(oid)
    } else {
        None
    }
}

fn in_place_allowed() -> bool {
    guc_tables::vars::allow_in_place_tablespaces.installed()
        && guc_tables::vars::allow_in_place_tablespaces.read()
}

pub fn TablespaceCreateDbspace(spc_oid: Oid, db_oid: Oid, is_redo: bool) -> PgResult<()> {
    if spc_oid == GLOBALTABLESPACE_OID {
        return Ok(());
    }
    let ctx = MemoryContext::new("TablespaceCreateDbspace");
    let dir = relpath::GetDatabasePath(ctx.mcx(), db_oid, spc_oid)?;
    let dir = dir.as_str();
    match std::fs::metadata(dir) {
        Ok(md) => {
            if !md.is_dir() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("\"{dir}\" exists but is not a directory"))
                    .into_error()
                    .into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let lock = lwlock::main_lock(TABLESPACE_CREATE_LOCK);
            lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
            let result = (|| -> PgResult<()> {
                if std::fs::metadata(dir).map(|m| m.is_dir()).unwrap_or(false) {
                    return Ok(());
                }
                if fd::MakePGDirectory(dir) < 0 {
                    let errnum = std::io::Error::last_os_error();
                    if errnum.kind() != std::io::ErrorKind::NotFound || !is_redo {
                        return Err(ereport(ERROR)
                            .with_saved_errno(errnum.raw_os_error().unwrap_or(0))
                            .errcode_for_file_access()
                            .errmsg(format!("could not create directory \"{dir}\": %m"))
                            .into_error()
                            .into());
                    }
                    fd::pg_mkdir_p(dir)?;
                }
                Ok(())
            })();
            lwlock::LWLockRelease(lock)?;
            result?;
        }
        Err(e) => {
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat directory \"{dir}\": %m"))
                .into_error()
                .into());
        }
    }
    Ok(())
}

pub fn CreateTableSpace<'mcx>(mcx: Mcx<'mcx>, stmt: &CreateTableSpaceStmt<'mcx>) -> PgResult<Oid> {
    let tablespacename = stmt.tablespacename.expect("CREATE TABLESPACE name");

    if !superuser::superuser()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied to create tablespace \"{tablespacename}\""))
            .errhint("Must be superuser to create a tablespace.".to_string())
            .into_error()
            .into());
    }

    let owner_id = match stmt.owner {
        Some(owner) => aclchk::get_rolespec_oid(owner, false)?,
        None => miscinit::GetUserId(),
    };

    let location = pg_path::canonicalize_path(stmt.location.unwrap_or(""));

    if location.contains('\'') {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg("tablespace location cannot contain single quotes".to_string())
            .into_error()
            .into());
    }

    let in_place = in_place_allowed() && location.is_empty();

    if !in_place && !pg_path::is_absolute_path(&location) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("tablespace location must be an absolute path".to_string())
            .into_error()
            .into());
    }

    if location.len() + 1 + TABLESPACE_VERSION_DIRECTORY.len() + 1 + OIDCHARS + 1 + OIDCHARS + 1
        + FORKNAMECHARS + 1 + OIDCHARS
        > MAXPGPATH
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!("tablespace location \"{location}\" is too long"))
            .into_error()
            .into());
    }

    if let Some(datadir) = init_small::globals::DataDir() {
        if pg_path::path_is_prefix_of_path(datadir, &location) {
            ereport(WARNING)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("tablespace location should not be inside the data directory".to_string())
                .finish(loc("CreateTableSpace"))?;
        }
    }

    if !init_small::globals::allowSystemTableMods() && catalog::IsReservedName(tablespacename) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("unacceptable tablespace name \"{tablespacename}\""))
            .errdetail("The prefix \"pg_\" is reserved for system tablespaces.".to_string())
            .into_error()
            .into());
    }

    if get_tablespace_oid(mcx, tablespacename, true)? != InvalidOid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("tablespace \"{tablespacename}\" already exists"))
            .into_error()
            .into());
    }

    let rel = table::table_open(mcx, TableSpaceRelationId, RowExclusiveLock)?;

    let tablespaceoid = if init_small::globals::IsBinaryUpgrade() {
        take_next_pg_tablespace_oid().ok_or_else(|| {
            Box::new(
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("pg_tablespace OID value not set when in binary upgrade mode")
                    .into_error()
                    .with_error_location(loc("CreateTableSpace")),
            )
        })?
    } else {
        catalog::GetNewOidWithIndex(
            mcx,
            &rel,
            TablespaceOidIndexId,
            Anum_pg_tablespace_oid as AttrNumber,
        )?
    };

    let mut spcname = NameData::default();
    spcname.namestrcpy(tablespacename);

    let new_options = reloptions::transformRelOptions(mcx, None, &stmt.options, None, &[], false, false)?;
    reloptions::tablespace_reloptions(mcx, new_options.as_deref(), true)?;

    let mut values = [Datum::null(); Natts_pg_tablespace];
    let mut nulls = [false; Natts_pg_tablespace];
    values[Anum_pg_tablespace_oid - 1] = Datum::from_oid(tablespaceoid);
    values[Anum_pg_tablespace_spcname - 1] = Datum::from_usize(spcname.data.as_ptr() as usize);
    values[Anum_pg_tablespace_spcowner - 1] = Datum::from_oid(owner_id);
    nulls[Anum_pg_tablespace_spcacl - 1] = true;
    match &new_options {
        Some(opts) => {
            values[Anum_pg_tablespace_spcoptions - 1] = Datum::from_usize(opts.as_ptr() as usize)
        }
        None => nulls[Anum_pg_tablespace_spcoptions - 1] = true,
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    pg_shdepend::recordDependencyOnOwner(mcx, TableSpaceRelationId, tablespaceoid, owner_id)?;

    create_tablespace_directories(&location, tablespaceoid)?;

    let mut xlrec = std::vec::Vec::with_capacity(4 + location.len() + 1);
    xlrec.extend_from_slice(&tablespaceoid.to_ne_bytes());
    xlrec.extend_from_slice(location.as_bytes());
    xlrec.push(0);
    xloginsert::insert_record(
        types_core::primitive::RmgrIds::RM_TBLSPC_ID as u8,
        XLOG_TBLSPC_CREATE,
        0,
        &[&xlrec],
        &[],
    )?;

    xact::ForceSyncCommit();

    rel.close(types_storage::lock::NoLock)?;

    Ok(tablespaceoid)
}

fn spcname_key<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
) -> PgResult<(mcx::PgBox<'mcx, NameData>, ScanKeyData)> {
    let mut nd = NameData::default();
    nd.namestrcpy(name);
    let boxed = mcx::PgBox::new_in(nd, mcx);
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_tablespace_spcname as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_NAMEEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_usize(boxed.as_ref() as *const NameData as usize);
    Ok((boxed, key))
}

fn oid_key(attno: usize, arg: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(arg);
    key
}

// DeleteSharedComments (comment.c) / DeleteSharedSecurityLabel (seclabel.c):
// delete rows keyed (objoid, classoid).
fn delete_shared_object_rows(
    mcx: Mcx<'_>,
    relid: Oid,
    index_id: Oid,
    oid: Oid,
    classoid: Oid,
) -> PgResult<()> {
    let keys = [oid_key(1, oid), oid_key(2, classoid)];
    let rel = table::table_open(mcx, relid, RowExclusiveLock)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, index_id, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        catalog_indexing::CatalogTupleDelete(&rel, &tup.t_self)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

pub fn DropTableSpace<'mcx>(mcx: Mcx<'mcx>, stmt: &DropTableSpaceStmt<'mcx>) -> PgResult<()> {
    let tablespacename = stmt.tablespacename.expect("DROP TABLESPACE name");

    let rel = table::table_open(mcx, TableSpaceRelationId, RowExclusiveLock)?;

    let (_nd, key) = spcname_key(mcx, tablespacename)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        genam::systable_endscan(mcx, scan)?;
        rel.close(types_storage::lock::NoLock)?;
        if !stmt.missing_ok {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("tablespace \"{tablespacename}\" does not exist"))
                .into_error()
                .into());
        }
        ereport(NOTICE)
            .errmsg(format!("tablespace \"{tablespacename}\" does not exist, skipping"))
            .finish(loc("DropTableSpace"))?;
        return Ok(());
    };

    let mut isnull = false;
    // SAFETY: oid is a fixed NOT NULL pg_tablespace column.
    let tablespaceoid = unsafe {
        types_tuple::heap_getattr(tuple, Anum_pg_tablespace_oid as i32, rel.descr(), &mut isnull)
    }
    .as_oid();
    let t_self = tuple.t_self;

    if !aclchk::object_ownercheck(TableSpaceRelationId, tablespaceoid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, ObjectType::OBJECT_TABLESPACE, tablespacename)?;
    }

    if catalog::IsPinnedObject(TableSpaceRelationId, tablespaceoid) {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NO_PRIV, ObjectType::OBJECT_TABLESPACE, tablespacename)?;
    }

    if let Some((detail, detail_log)) =
        pg_shdepend::checkSharedDependencies(mcx, TableSpaceRelationId, tablespaceoid)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .errmsg(format!(
                "tablespace \"{tablespacename}\" cannot be dropped because some objects depend on it"
            ))
            .errdetail_internal(detail.as_str().to_string())
            .errdetail_log(detail_log.as_str().to_string())
            .into_error()
            .into());
    }

    catalog_indexing::CatalogTupleDelete(&rel, &t_self)?;
    genam::systable_endscan(mcx, scan)?;

    delete_shared_object_rows(
        mcx,
        SharedDescriptionRelationId,
        SharedDescriptionObjIndexId,
        tablespaceoid,
        TableSpaceRelationId,
    )?;
    delete_shared_object_rows(
        mcx,
        SharedSecLabelRelationId,
        SharedSecLabelObjectIndexId,
        tablespaceoid,
        TableSpaceRelationId,
    )?;

    pg_shdepend::deleteSharedDependencyRecordsFor(mcx, TableSpaceRelationId, tablespaceoid, 0)?;

    let lock = lwlock::main_lock(TABLESPACE_CREATE_LOCK);
    lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;

    let result = (|| -> PgResult<()> {
        if !destroy_tablespace_directories(tablespaceoid, false)? {
            checkpointer::RequestCheckpoint(
                transam_xlog::CHECKPOINT_IMMEDIATE
                    | transam_xlog::CHECKPOINT_FORCE
                    | transam_xlog::CHECKPOINT_WAIT,
            )?;

            lwlock::LWLockRelease(lock)?;
            let gen = procsignal::EmitProcSignalBarrier(
                ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
            );
            procsignal::WaitForProcSignalBarrier(gen)?;
            lwlock::LWLockAcquire(lock, lwlock::LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;

            if !destroy_tablespace_directories(tablespaceoid, false)? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!("tablespace \"{tablespacename}\" is not empty"))
                    .into_error()
                    .into());
            }
        }

        xloginsert::insert_record(
            types_core::primitive::RmgrIds::RM_TBLSPC_ID as u8,
            XLOG_TBLSPC_DROP,
            0,
            &[&tablespaceoid.to_ne_bytes()],
            &[],
        )?;

        xact::ForceSyncCommit();
        Ok(())
    })();
    lwlock::LWLockRelease(lock)?;
    result?;

    rel.close(types_storage::lock::NoLock)
}

fn create_tablespace_directories(location: &str, tablespaceoid: Oid) -> PgResult<()> {
    let linkloc = format!("{PG_TBLSPC_DIR}/{tablespaceoid}");
    let in_place = location.is_empty();

    if in_place {
        if fd::MakePGDirectory(&linkloc) < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not create directory \"{linkloc}\": %m"))
                    .into_error()
                    .into());
            }
        }
    }

    let location_with_version_dir = format!(
        "{}/{TABLESPACE_VERSION_DIRECTORY}",
        if in_place { linkloc.as_str() } else { location }
    );

    let in_recovery = xlogutils_seams::in_recovery::call();

    if !in_place {
        // wasm32: WASI files carry no unix mode bits (no chmod); the ENOENT
        // report below is what this stanza exists for, so stat stands in.
        #[cfg(not(target_family = "wasm"))]
        let perm_result = {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(fd::vfd::pg_dir_create_mode());
            std::fs::set_permissions(location, perms)
        };
        #[cfg(target_family = "wasm")]
        let perm_result = std::fs::metadata(location).map(|_| ());
        if let Err(e) = perm_result {
            if e.kind() == std::io::ErrorKind::NotFound {
                let mut b = ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FILE)
                    .errmsg(format!("directory \"{location}\" does not exist"));
                if in_recovery {
                    b = b.errhint(
                        "Create this directory for the tablespace before restarting the server."
                            .to_string(),
                    );
                }
                return Err(b.into_error().into());
            }
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not set permissions on directory \"{location}\": %m"))
                .into_error()
                .into());
        }
    }

    match std::fs::symlink_metadata(&location_with_version_dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if fd::MakePGDirectory(&location_with_version_dir) < 0 {
                let e2 = std::io::Error::last_os_error();
                return Err(ereport(ERROR)
                    .with_saved_errno(e2.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not create directory \"{location_with_version_dir}\": %m"
                    ))
                    .into_error()
                    .into());
            }
        }
        Err(e) => {
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat directory \"{location_with_version_dir}\": %m"))
                .into_error()
                .into());
        }
        Ok(md) if !md.is_dir() => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "\"{location_with_version_dir}\" exists but is not a directory"
                ))
                .into_error()
                .into());
        }
        Ok(_) if !in_recovery => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "directory \"{location_with_version_dir}\" already in use as a tablespace"
                ))
                .into_error()
                .into());
        }
        Ok(_) => {}
    }

    if !in_place && in_recovery {
        remove_tablespace_symlink(&linkloc)?;
    }

    if !in_place {
        // wasm32: std exposes no symlink creation on wasi (unix::fs is
        // absent; wasi::fs's is unstable), and preopen-relative symlink
        // targets are runtime-dependent — refuse with the C error shape
        // (52 = WASI ENOSYS). Non-in-place tablespaces are effectively
        // unsupported on wasm.
        #[cfg(target_family = "wasm")]
        let link_result: std::io::Result<()> = Err(std::io::Error::from_raw_os_error(52));
        #[cfg(not(target_family = "wasm"))]
        let link_result = std::os::unix::fs::symlink(location, &linkloc);
        if let Err(e) = link_result {
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not create symbolic link \"{linkloc}\": %m"))
                .into_error()
                .into());
        }
    }

    Ok(())
}

fn file_err(
    redo: bool,
    errnum: &std::io::Error,
    msg: String,
    funcname: &'static str,
) -> PgResult<()> {
    let level = if redo {
        LOG
    } else if errnum.kind() == std::io::ErrorKind::NotFound {
        WARNING
    } else {
        ERROR
    };
    ereport(level)
        .with_saved_errno(errnum.raw_os_error().unwrap_or(0))
        .errcode_for_file_access()
        .errmsg(msg)
        .finish(loc(funcname))
}

fn destroy_tablespace_directories(tablespaceoid: Oid, redo: bool) -> PgResult<bool> {
    let linkloc_with_version_dir =
        format!("{PG_TBLSPC_DIR}/{tablespaceoid}/{TABLESPACE_VERSION_DIRECTORY}");
    let linkloc = format!("{PG_TBLSPC_DIR}/{tablespaceoid}");

    let entries = match std::fs::read_dir(&linkloc_with_version_dir) {
        Ok(entries) => Some(entries),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if !redo {
                ereport(WARNING)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not open directory \"{linkloc_with_version_dir}\": %m"
                    ))
                    .finish(loc("destroy_tablespace_directories"))?;
            }
            None
        }
        Err(e) if redo => {
            ereport(LOG)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not open directory \"{linkloc_with_version_dir}\": %m"
                ))
                .finish(loc("destroy_tablespace_directories"))?;
            return Ok(false);
        }
        Err(e) => {
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not read directory \"{linkloc_with_version_dir}\": %m"
                ))
                .into_error()
                .into());
        }
    };

    if let Some(entries) = entries {
        for entry in entries {
            let entry = entry.map_err(|e| -> Box<PgError> {
                ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not read directory \"{linkloc_with_version_dir}\": %m"
                    ))
                    .into_error()
                    .into()
            })?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "." || name == ".." {
                continue;
            }
            let subfile = format!("{linkloc_with_version_dir}/{name}");

            if !redo && !fd::directory_is_empty(&subfile)? {
                return Ok(false);
            }

            if let Err(e) = std::fs::remove_dir(&subfile) {
                let level = if redo { LOG } else { ERROR };
                ereport(level)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove directory \"{subfile}\": %m"))
                    .finish(loc("destroy_tablespace_directories"))?;
            }
        }

        if let Err(e) = std::fs::remove_dir(&linkloc_with_version_dir) {
            let level = if redo { LOG } else { ERROR };
            ereport(level)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not remove directory \"{linkloc_with_version_dir}\": %m"
                ))
                .finish(loc("destroy_tablespace_directories"))?;
            return Ok(false);
        }
    }

    match std::fs::symlink_metadata(&linkloc) {
        Err(e) => {
            file_err(redo, &e, format!("could not stat file \"{linkloc}\": %m"),
                "destroy_tablespace_directories")?;
        }
        Ok(md) if md.is_dir() => {
            if let Err(e) = std::fs::remove_dir(&linkloc) {
                file_err(redo, &e, format!("could not remove directory \"{linkloc}\": %m"),
                    "destroy_tablespace_directories")?;
            }
        }
        Ok(md) if md.file_type().is_symlink() => {
            if let Err(e) = std::fs::remove_file(&linkloc) {
                file_err(redo, &e, format!("could not remove symbolic link \"{linkloc}\": %m"),
                    "destroy_tablespace_directories")?;
            }
        }
        Ok(_) => {
            let level = if redo { LOG } else { ERROR };
            ereport(level)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("\"{linkloc}\" is not a directory or symbolic link"))
                .finish(loc("destroy_tablespace_directories"))?;
        }
    }

    Ok(true)
}

pub fn remove_tablespace_symlink(linkloc: &str) -> PgResult<()> {
    let md = match std::fs::symlink_metadata(linkloc) {
        Ok(md) => md,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{linkloc}\": %m"))
                .into_error()
                .into());
        }
    };

    if md.is_dir() {
        if let Err(e) = std::fs::remove_dir(linkloc) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove directory \"{linkloc}\": %m"))
                    .into_error()
                    .into());
            }
        }
    } else if md.file_type().is_symlink() {
        if let Err(e) = std::fs::remove_file(linkloc) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove symbolic link \"{linkloc}\": %m"))
                    .into_error()
                    .into());
            }
        }
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("\"{linkloc}\" is not a directory or symbolic link"))
            .into_error()
            .into());
    }
    Ok(())
}

pub fn RenameTableSpace(mcx: Mcx<'_>, oldname: &str, newname: &str) -> PgResult<Oid> {
    let rel = table::table_open(mcx, TableSpaceRelationId, RowExclusiveLock)?;

    let (_nd, key) = spcname_key(mcx, oldname)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("tablespace \"{oldname}\" does not exist"))
            .into_error()
            .into());
    };
    let mut isnull = false;
    // SAFETY: oid is a fixed NOT NULL pg_tablespace column.
    let tsp_id = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_tablespace_oid as i32, rel.descr(), &mut isnull)
    }
    .as_oid();
    let otid = tup.t_self;
    let mut newname_nd = NameData::default();
    newname_nd.namestrcpy(newname);
    let mut values = [Datum::null(); Natts_pg_tablespace];
    let mut nullsv = [false; Natts_pg_tablespace];
    let mut replace = [false; Natts_pg_tablespace];
    values[Anum_pg_tablespace_spcname - 1] =
        Datum::from_usize(newname_nd.data.as_ptr() as usize);
    replace[Anum_pg_tablespace_spcname - 1] = true;
    let mut newtuple = heaptuple::heap_modify_tuple(mcx, tup, rel.descr(), &values, &nullsv, &replace)?;
    genam::systable_endscan(mcx, scan)?;

    if !aclchk::object_ownercheck(TableSpaceRelationId, tsp_id, miscinit::GetUserId())? {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NO_PRIV, ObjectType::OBJECT_TABLESPACE, oldname)?;
    }

    if !init_small::globals::allowSystemTableMods() && catalog::IsReservedName(newname) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("unacceptable tablespace name \"{newname}\""))
            .errdetail("The prefix \"pg_\" is reserved for system tablespaces.".to_string())
            .into_error()
            .into());
    }

    let (_nd2, key2) = spcname_key(mcx, newname)?;
    let mut scan2 = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key2])?;
    if genam::systable_getnext(mcx, &mut scan2)?.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("tablespace \"{newname}\" already exists"))
            .into_error()
            .into());
    }
    genam::systable_endscan(mcx, scan2)?;

    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;

    rel.close(types_storage::lock::NoLock)?;
    Ok(tsp_id)
}

pub fn AlterTableSpaceOptions<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTableSpaceOptionsStmt<'mcx>,
) -> PgResult<Oid> {
    let tablespacename = stmt.tablespacename.expect("ALTER TABLESPACE name");

    let rel = table::table_open(mcx, TableSpaceRelationId, RowExclusiveLock)?;

    let (_nd, key) = spcname_key(mcx, tablespacename)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("tablespace \"{tablespacename}\" does not exist"))
            .into_error()
            .into());
    };

    let mut isnull = false;
    // SAFETY: oid is a fixed NOT NULL pg_tablespace column.
    let tablespaceoid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_tablespace_oid as i32, rel.descr(), &mut isnull)
    }
    .as_oid();

    if !aclchk::object_ownercheck(TableSpaceRelationId, tablespaceoid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_TABLESPACE,
            tablespacename,
        )?;
    }

    let mut opt_isnull = false;
    // SAFETY: spcoptions attno is within the pg_tablespace descriptor.
    let datum = unsafe {
        types_tuple::heap_getattr(
            tup,
            Anum_pg_tablespace_spcoptions as i32,
            rel.descr(),
            &mut opt_isnull,
        )
    };
    let old_options: Option<PgVec<'mcx, u8>> = if opt_isnull {
        None
    } else {
        Some(reloptions::text_array_image(mcx, datum)?)
    };
    let new_options = reloptions::transformRelOptions(
        mcx,
        old_options.as_deref(),
        &stmt.options,
        None,
        &[],
        false,
        stmt.isReset,
    )?;
    reloptions::tablespace_reloptions(mcx, new_options.as_deref(), true)?;

    let mut values = [Datum::null(); Natts_pg_tablespace];
    let mut nullsv = [false; Natts_pg_tablespace];
    let mut replace = [false; Natts_pg_tablespace];
    match &new_options {
        Some(opts) => {
            values[Anum_pg_tablespace_spcoptions - 1] = Datum::from_usize(opts.as_ptr() as usize)
        }
        None => nullsv[Anum_pg_tablespace_spcoptions - 1] = true,
    }
    replace[Anum_pg_tablespace_spcoptions - 1] = true;
    let otid = tup.t_self;
    let mut newtuple = heaptuple::heap_modify_tuple(mcx, tup, rel.descr(), &values, &nullsv, &replace)?;
    genam::systable_endscan(mcx, scan)?;

    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;

    rel.close(types_storage::lock::NoLock)?;
    Ok(tablespaceoid)
}

// AlterObjectOwner_internal (alter.c) specialized to pg_tablespace: the
// generic get_object_address/catalog-property route is unported. Tuple-level
// InplaceUpdateTupleLock is not taken (no inplace updaters touch
// pg_tablespace rows).
pub fn AlterTableSpaceOwner(mcx: Mcx<'_>, name: &str, new_owner_id: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, TableSpaceRelationId, RowExclusiveLock)?;

    let (_nd, key) = spcname_key(mcx, name)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("tablespace \"{name}\" does not exist"))
            .into_error()
            .into());
    };

    let desc = rel.descr();
    let mut isnull = false;
    // SAFETY: oid/spcowner are fixed NOT NULL pg_tablespace columns.
    let tablespaceoid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_tablespace_oid as i32, desc, &mut isnull)
    }
    .as_oid();
    // SAFETY: as above.
    let old_owner_id = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_tablespace_spcowner as i32, desc, &mut isnull)
    }
    .as_oid();

    if old_owner_id != new_owner_id {
        if !superuser::superuser()? {
            if !adt_acl::has_privs_of_role(miscinit::GetUserId(), old_owner_id)? {
                aclchk::aclcheck_error(
                    aclchk::ACLCHECK_NOT_OWNER,
                    ObjectType::OBJECT_TABLESPACE,
                    name,
                )?;
            }
            if !adt_acl::member_can_set_role(miscinit::GetUserId(), new_owner_id)? {
                let rolename = miscinit::GetUserNameFromId(mcx, new_owner_id, false)?
                    .expect("noerr=false yields a name");
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!("must be able to SET ROLE \"{}\"", rolename.as_str()))
                    .into_error()
                    .into());
            }
        }

        let mut values = [Datum::null(); Natts_pg_tablespace];
        let mut nullsv = [false; Natts_pg_tablespace];
        let mut replace = [false; Natts_pg_tablespace];
        values[Anum_pg_tablespace_spcowner - 1] = Datum::from_oid(new_owner_id);
        replace[Anum_pg_tablespace_spcowner - 1] = true;

        let mut acl_isnull = false;
        // SAFETY: spcacl attno is within the pg_tablespace descriptor.
        let acl_datum = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_tablespace_spcacl as i32, desc, &mut acl_isnull)
        };
        let new_acl_image;
        if !acl_isnull {
            let p = acl_datum.as_usize() as *const u8;
            // SAFETY: not-null aclitem[] column datum on a live catalog tuple.
            let img =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            let payload = varlena::open_image(mcx, img)?;
            let old_acl = adt_acl::varlena::decode_acl_payload(mcx, payload.as_bytes())?;
            let new_acl = adt_acl::aclnewowner(mcx, &old_acl, old_owner_id, new_owner_id)?;
            new_acl_image = adt_acl::varlena::acl_image(mcx, &new_acl)?;
            values[Anum_pg_tablespace_spcacl - 1] =
                Datum::from_usize(new_acl_image.as_ptr() as usize);
            replace[Anum_pg_tablespace_spcacl - 1] = true;
        }

        let otid = tup.t_self;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nullsv, &replace)?;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

        pg_shdepend::changeDependencyOnOwner(mcx, TableSpaceRelationId, tablespaceoid, new_owner_id)?;
    } else {
        genam::systable_endscan(mcx, scan)?;
    }

    rel.close(RowExclusiveLock)
}

pub fn GetDefaultTablespace(mcx: Mcx<'_>, relpersistence: u8, partitioned: bool) -> PgResult<Oid> {
    if relpersistence == types_core::catalog::RELPERSISTENCE_TEMP {
        PrepareTempTablespaces(mcx)?;
        return Ok(fd::GetNextTempTableSpace());
    }

    let default_tablespace = guc_tables::vars::default_tablespace.read();
    let Some(name) = default_tablespace.filter(|s| !s.is_empty()) else {
        return Ok(InvalidOid);
    };

    let mut result = get_tablespace_oid(mcx, &name, true)?;
    if result == init_small::globals::MyDatabaseTableSpace() {
        if partitioned {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot specify default tablespace for partitioned relations".to_string())
                .into_error()
                .into());
        }
        result = InvalidOid;
    }
    Ok(result)
}

pub fn check_default_tablespace(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    source: types_guc::GucSource,
) -> PgResult<bool> {
    if xact::IsTransactionState() && init_small::globals::MyDatabaseId() != InvalidOid {
        let val = newval.as_deref().unwrap_or("");
        if !val.is_empty() {
            let ctx = MemoryContext::new("check_default_tablespace");
            if get_tablespace_oid(ctx.mcx(), val, true)? == InvalidOid {
                if source == types_guc::GucSource::PGC_S_TEST {
                    ereport(NOTICE)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("tablespace \"{val}\" does not exist"))
                        .finish(loc("check_default_tablespace"))?;
                } else {
                    guc_seams::guc_check_errdetail::call(format!(
                        "Tablespace \"{val}\" does not exist."
                    ));
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn temp_tablespace_oids(mcx: Mcx<'_>, namelist: &[String], check_perms: bool, source: types_guc::GucSource) -> PgResult<std::vec::Vec<Oid>> {
    let mut spcs = std::vec::Vec::with_capacity(namelist.len());
    for curname in namelist {
        if curname.is_empty() {
            spcs.push(InvalidOid);
            continue;
        }
        let missing_ok = !check_perms || source <= types_guc::GucSource::PGC_S_TEST;
        let curoid = get_tablespace_oid(mcx, curname, missing_ok)?;
        if curoid == InvalidOid {
            if check_perms && source == types_guc::GucSource::PGC_S_TEST {
                ereport(NOTICE)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("tablespace \"{curname}\" does not exist"))
                    .finish(loc("check_temp_tablespaces"))?;
            }
            continue;
        }
        if curoid == init_small::globals::MyDatabaseTableSpace() {
            spcs.push(InvalidOid);
            continue;
        }
        let aclresult = aclchk::object_aclcheck(
            TableSpaceRelationId,
            curoid,
            miscinit::GetUserId(),
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            if check_perms && source >= types_guc::GucSource::PGC_S_INTERACTIVE {
                aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_TABLESPACE, curname)?;
            }
            continue;
        }
        spcs.push(curoid);
    }
    Ok(spcs)
}

pub fn check_temp_tablespaces(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    source: types_guc::GucSource,
) -> PgResult<bool> {
    let raw = newval.clone().unwrap_or_default();
    let ctx = MemoryContext::new("check_temp_tablespaces");
    let mcx = ctx.mcx();
    let Some(namelist) =
        varlena::split_identifier_string(mcx, &raw, b',', mbutils::GetDatabaseEncoding())?
    else {
        guc_seams::guc_check_errdetail::call("List syntax is invalid.".to_string());
        return Ok(false);
    };
    if xact::IsTransactionState() && init_small::globals::MyDatabaseId() != InvalidOid {
        let spcs = temp_tablespace_oids(mcx, &namelist, true, source)?;
        *extra = Some(Box::new(spcs));
    }
    Ok(true)
}

pub fn assign_temp_tablespaces(_newval: Option<&str>, extra: Option<&guc_tables::GucHookExtra>) {
    match extra.and_then(|e| e.downcast_ref::<std::vec::Vec<Oid>>()) {
        Some(spcs) => fd::SetTempTablespaces(spcs),
        None => fd::SetTempTablespaces(&[]),
    }
}

pub fn PrepareTempTablespaces(mcx: Mcx<'_>) -> PgResult<()> {
    if fd::TempTablespacesAreSet() {
        return Ok(());
    }
    if !xact::IsTransactionState() {
        return Ok(());
    }
    let raw = guc_tables::vars::temp_tablespaces.read().unwrap_or_default();
    let Some(namelist) =
        varlena::split_identifier_string(mcx, &raw, b',', mbutils::GetDatabaseEncoding())?
    else {
        fd::SetTempTablespaces(&[]);
        return Ok(());
    };
    let spcs = temp_tablespace_oids(mcx, &namelist, false, types_guc::GucSource::PGC_S_DEFAULT)?;
    fd::SetTempTablespaces(&spcs);
    Ok(())
}

// get_tablespace_oid (tablespace.c): C seq-scans pg_tablespace with a
// spcname key.
pub fn get_tablespace_oid(mcx: Mcx<'_>, tablespacename: &str, missing_ok: bool) -> PgResult<Oid> {
    let rel = table::table_open(mcx, TableSpaceRelationId, AccessShareLock)?;
    let n = NAMEDATALEN as usize;
    let mut name_buf: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    let take = tablespacename.len().min(n - 1);
    mcx::vec_append_bytes(&mut name_buf, &tablespacename.as_bytes()[..take])?;
    mcx::vec_append_bytes(&mut name_buf, &[0u8; 64][..n - take])?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_tablespace_spcname as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_NAMEEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_usize(name_buf.as_ptr() as usize);
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let result = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: oid is a fixed NOT NULL pg_tablespace column.
            unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_tablespace_oid as i32,
                    rel.descr(),
                    &mut isnull,
                )
            }
            .as_oid()
        }
        None => InvalidOid,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    if result == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("tablespace \"{tablespacename}\" does not exist"),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(result)
}

pub fn get_tablespace_name(mcx: Mcx<'_>, spc_oid: Oid) -> PgResult<Option<NameData>> {
    let rel = table::table_open(mcx, TableSpaceRelationId, AccessShareLock)?;
    let key = oid_key(Anum_pg_tablespace_oid, spc_oid);
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let result = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: spcname is a fixed NOT NULL pg_tablespace column.
            let d = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_tablespace_spcname as i32,
                    rel.descr(),
                    &mut isnull,
                )
            };
            // SAFETY: name column datum points at NAMEDATALEN in-tuple bytes.
            Some(unsafe { *(d.as_usize() as *const NameData) })
        }
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(result)
}

pub fn tblspc_redo(record: &mut xlogreader_seams::XLogReaderState) -> PgResult<()> {
    let decoded = record.record.as_ref().expect("tblspc_redo with no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;
    // SAFETY: the decoded record's main data lives for the redo call.
    let data = unsafe { decoded.main_data_bytes() };

    if info == XLOG_TBLSPC_CREATE {
        let ts_id = u32::from_ne_bytes(data[0..4].try_into().expect("short tblspc create rec"));
        let path = &data[4..];
        let path = &path[..path.iter().position(|&b| b == 0).unwrap_or(path.len())];
        let location = std::str::from_utf8(path).expect("tablespace path is not UTF-8");
        create_tablespace_directories(location, ts_id)?;
    } else if info == XLOG_TBLSPC_DROP {
        let ts_id = u32::from_ne_bytes(data[0..4].try_into().expect("short tblspc drop rec"));

        let gen = procsignal::EmitProcSignalBarrier(
            ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
        );
        procsignal::WaitForProcSignalBarrier(gen)?;

        if !destroy_tablespace_directories(ts_id, true)? {
            standby::ResolveRecoveryConflictWithTablespace(ts_id)?;
            if !destroy_tablespace_directories(ts_id, true)? {
                ereport(LOG)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!("directories for tablespace {ts_id} could not be removed"))
                    .errhint("You can remove the directories manually if necessary.".to_string())
                    .finish(loc("tblspc_redo"))?;
            }
        }
    } else {
        panic!("tblspc_redo: unknown op code {info}");
    }
    Ok(())
}

fn prepare_temp_tablespaces_seam() -> PgResult<()> {
    let ctx = MemoryContext::new("PrepareTempTablespaces");
    PrepareTempTablespaces(ctx.mcx())
}

thread_local! {
    static DEFAULT_TABLESPACE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
    static ALLOW_IN_PLACE_TABLESPACES: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

fn default_tablespace_guc() -> Option<String> {
    DEFAULT_TABLESPACE.with(|c| c.borrow().clone())
}

fn set_default_tablespace_guc(value: Option<String>) {
    DEFAULT_TABLESPACE.with(|c| *c.borrow_mut() = value);
}

fn allow_in_place_tablespaces_guc() -> bool {
    ALLOW_IN_PLACE_TABLESPACES.with(|c| c.get())
}

fn set_allow_in_place_tablespaces_guc(value: bool) {
    ALLOW_IN_PLACE_TABLESPACES.with(|c| c.set(value));
}

pub fn init_seams() {
    guc_tables::vars::default_tablespace.install(guc_tables::GucVarAccessors {
        get: default_tablespace_guc,
        set: set_default_tablespace_guc,
    });
    guc_tables::vars::allow_in_place_tablespaces.install(guc_tables::GucVarAccessors {
        get: allow_in_place_tablespaces_guc,
        set: set_allow_in_place_tablespaces_guc,
    });
    tablespace_seams::tablespace_create_dbspace::set(TablespaceCreateDbspace);
    tablespace_seams::get_tablespace_oid::set(get_tablespace_oid);
    tablespace_seams::get_tablespace_name::set(get_tablespace_name);
    tablespace_seams::tblspc_redo::set(tblspc_redo);
    tablespace_seams::prepare_temp_tablespaces::set(prepare_temp_tablespaces_seam);
    guc_tables::hooks::check_default_tablespace.install(check_default_tablespace);
    guc_tables::hooks::check_temp_tablespaces.install(check_temp_tablespaces);
    guc_tables::hooks::assign_temp_tablespaces.install(assign_temp_tablespaces);
}

#[cfg(test)]
mod pg_upgrade_oid_tests {
    use super::*;

    #[test]
    fn next_pg_tablespace_oid_set_take_once() {
        assert_eq!(take_next_pg_tablespace_oid(), None);
        SetNextPgTablespaceOid(200);
        assert_eq!(take_next_pg_tablespace_oid(), Some(200));
        assert_eq!(take_next_pg_tablespace_oid(), None);
    }
}
