use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use pg_database::{
    Anum_pg_database_datacl, Anum_pg_database_datallowconn, Anum_pg_database_datcollate,
    Anum_pg_database_datcollversion, Anum_pg_database_datconnlimit, Anum_pg_database_datdba,
    Anum_pg_database_datistemplate, Anum_pg_database_datlocale, Anum_pg_database_datlocprovider,
    Anum_pg_database_datname, Anum_pg_database_dattablespace, Anum_pg_database_oid,
    DatabaseNameIndexId, Natts_pg_database, DATCONNLIMIT_UNLIMITED,
};
use pg_database_seams::COLLPROVIDER_LIBC;
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::{InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_DATABASE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_DATABASE,
    ERROR, FATAL, NOTICE, WARNING,
};
use types_nodes::parsenodes::{
    AlterDatabaseRefreshCollStmt, AlterDatabaseSetStmt, AlterDatabaseStmt, DefElem, ObjectType,
};
use types_rel::Relation;
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, InplaceUpdateTupleLock, RowExclusiveLock,
};
use types_storage::storage::ProcSignalBarrierType;
use types_tuple::{HeapTupleData, NameData};

use crate::{
    errdetail_busy_db, get_db_info, have_createdb_privilege, loc, name_key,
    GLOBALTABLESPACE_OID, TableSpaceRelationId, XLOG_DBASE_CREATE_FILE_COPY, XLOG_DBASE_DROP,
};

fn text_datum_string(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let p = d.as_usize() as *const u8;
    // SAFETY: d comes off a not-null text column: a live varlena image
    // readable through its varsize_any extent.
    let image = unsafe {
        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
    };
    let payload = varlena::open_image(mcx, image)?;
    Ok(String::from_utf8_lossy(payload.as_bytes()).into_owned())
}

fn getattr(
    tup: &HeapTupleData<'_>,
    attno: i32,
    rel: &Relation<'_>,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: pg_database row read under this relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, rel.descr(), &mut isnull) };
    (d, isnull)
}

fn not_owner(dbname: &str) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg(format!("must be owner of database {dbname}"))
        .into_error()
        .into()
}

pub fn RenameDatabase(mcx: Mcx<'_>, oldname: &str, newname: &str) -> PgResult<Oid> {
    let rel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;

    let Some(db) = get_db_info(mcx, oldname, AccessExclusiveLock)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{oldname}\" does not exist"))
            .into_error()
            .into());
    };
    let db_id = db.oid;

    if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db_id, miscinit::GetUserId())? {
        return Err(not_owner(oldname));
    }

    if !have_createdb_privilege()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to rename database".to_string())
            .into_error()
            .into());
    }

    if crate::get_database_oid(mcx, newname, true)? != InvalidOid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_DATABASE)
            .errmsg(format!("database \"{newname}\" already exists"))
            .into_error()
            .into());
    }

    if db_id == init_small::globals::MyDatabaseId() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("current database cannot be renamed".to_string())
            .into_error()
            .into());
    }

    if let Some((notherbackends, npreparedxacts)) = procarray::CountOtherDBBackends(db_id)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!("database \"{oldname}\" is being accessed by other users"))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .into_error()
            .into());
    }

    let (_nd, key) = name_key(mcx, oldname)?;
    let mut scan =
        genam::systable_beginscan(mcx, &rel, DatabaseNameIndexId, true, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        panic!("cache lookup failed for database {db_id}");
    };
    let otid = tup.t_self;
    lmgr::LockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

    let mut datname = NameData::default();
    datname.namestrcpy(newname);
    let natts = Natts_pg_database;
    let mut values = vec![Datum::null(); natts];
    let mut isnull = vec![false; natts];
    let mut replace = vec![false; natts];
    values[Anum_pg_database_datname as usize - 1] =
        Datum::from_usize(datname.data.as_ptr() as usize);
    replace[Anum_pg_database_datname as usize - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, rel.descr(), &values, &isnull, &replace)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
    lmgr::UnlockTuple(&rel, &otid, InplaceUpdateTupleLock)?;
    genam::systable_endscan(mcx, scan)?;

    rel.close(types_storage::lock::NoLock)?;
    Ok(db_id)
}

pub fn movedb(mcx: Mcx<'_>, dbname: &str, tblspcname: &str) -> PgResult<()> {
    let pgdbrel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;

    let Some(db) = get_db_info(mcx, dbname, AccessExclusiveLock)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .into_error()
            .into());
    };
    let db_id = db.oid;
    let src_tblspcoid = db.dattablespace;

    lmgr::LockSharedObjectForSession(DATABASE_RELATION_ID, db_id, 0, AccessExclusiveLock)?;

    if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db_id, miscinit::GetUserId())? {
        return Err(not_owner(dbname));
    }

    if db_id == init_small::globals::MyDatabaseId() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg("cannot change the tablespace of the currently open database".to_string())
            .into_error()
            .into());
    }

    let dst_tblspcoid = commands_tablespace::get_tablespace_oid(mcx, tblspcname, false)?;

    let aclresult = aclchk::object_aclcheck(
        TableSpaceRelationId,
        dst_tblspcoid,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_TABLESPACE, tblspcname)?;
    }

    if dst_tblspcoid == GLOBALTABLESPACE_OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("pg_global cannot be used as default tablespace".to_string())
            .into_error()
            .into());
    }

    if src_tblspcoid == dst_tblspcoid {
        pgdbrel.close(types_storage::lock::NoLock)?;
        lmgr::UnlockSharedObjectForSession(
            DATABASE_RELATION_ID,
            db_id,
            0,
            AccessExclusiveLock,
        )?;
        return Ok(());
    }

    if let Some((notherbackends, npreparedxacts)) = procarray::CountOtherDBBackends(db_id)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!("database \"{dbname}\" is being accessed by other users"))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .into_error()
            .into());
    }

    let src_dbpath = relpath::GetDatabasePath(mcx, db_id, src_tblspcoid)?;
    let dst_dbpath = relpath::GetDatabasePath(mcx, db_id, dst_tblspcoid)?;

    checkpointer::RequestCheckpoint(
        transam_xlog::CHECKPOINT_IMMEDIATE
            | transam_xlog::CHECKPOINT_FORCE
            | transam_xlog::CHECKPOINT_WAIT
            | transam_xlog::CHECKPOINT_FLUSH_ALL,
    )?;

    let gen =
        procsignal::EmitProcSignalBarrier(ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE);
    procsignal::WaitForProcSignalBarrier(gen)?;

    bufmgr::DropDatabaseBuffers(db_id)?;

    if let Ok(entries) = std::fs::read_dir(dst_dbpath.as_str()) {
        for entry in entries {
            let entry = entry.map_err(|e| {
                Box::new(types_error::PgError::error(format!(
                    "could not read directory \"{}\": {e}",
                    dst_dbpath.as_str()
                )))
            })?;
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "some relations of database \"{dbname}\" are already in tablespace \"{tblspcname}\""
                ))
                .errhint(
                    "You must move them back to the database's default tablespace before using this command."
                        .to_string(),
                )
                .into_error()
                .into());
        }
        if let Err(e) = std::fs::remove_dir(dst_dbpath.as_str()) {
            return Err(ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errmsg(format!(
                    "could not remove directory \"{}\": %m",
                    dst_dbpath.as_str()
                ))
                .into_error()
                .into());
        }
    }

    let moved = movedb_body(
        mcx,
        &pgdbrel,
        dbname,
        db_id,
        src_dbpath.as_str(),
        dst_dbpath.as_str(),
        src_tblspcoid,
        dst_tblspcoid,
    );
    if let Err(e) = moved {
        let _ = fd::rmtree(dst_dbpath.as_str(), true);
        return Err(e);
    }

    pgdbrel.close(types_storage::lock::NoLock)?;

    snapmgr::PopActiveSnapshot()?;
    xact::CommitTransactionCommand()?;

    xact::StartTransactionCommand()?;

    if !fd::rmtree(src_dbpath.as_str(), true)? {
        ereport(WARNING)
            .errmsg(format!(
                "some useless files may be left behind in old database directory \"{}\"",
                src_dbpath.as_str()
            ))
            .finish(loc("movedb"))?;
    }

    let mut xlrec = [0u8; 12];
    xlrec[0..4].copy_from_slice(&db_id.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&1i32.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&src_tblspcoid.to_ne_bytes());
    xloginsert::insert_record(
        types_core::primitive::RmgrIds::RM_DBASE_ID as u8,
        XLOG_DBASE_DROP | xloginsert::XLR_SPECIAL_REL_UPDATE,
        0,
        &[&xlrec],
        &[],
    )?;

    lmgr::UnlockSharedObjectForSession(DATABASE_RELATION_ID, db_id, 0, AccessExclusiveLock)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn movedb_body(
    mcx: Mcx<'_>,
    pgdbrel: &Relation<'_>,
    dbname: &str,
    db_id: Oid,
    src_dbpath: &str,
    dst_dbpath: &str,
    _src_tblspcoid: Oid,
    dst_tblspcoid: Oid,
) -> PgResult<()> {
    fd::copydir(src_dbpath, dst_dbpath, false)?;

    let mut xlrec = [0u8; 16];
    xlrec[0..4].copy_from_slice(&db_id.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&dst_tblspcoid.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&db_id.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&_src_tblspcoid.to_ne_bytes());
    xloginsert::insert_record(
        types_core::primitive::RmgrIds::RM_DBASE_ID as u8,
        XLOG_DBASE_CREATE_FILE_COPY | xloginsert::XLR_SPECIAL_REL_UPDATE,
        0,
        &[&xlrec],
        &[],
    )?;

    let (_nd, key) = name_key(mcx, dbname)?;
    let mut scan =
        genam::systable_beginscan(mcx, pgdbrel, DatabaseNameIndexId, true, None, &[key])?;
    let Some(oldtuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .into_error()
            .into());
    };
    let otid = oldtuple.t_self;
    lmgr::LockTuple(pgdbrel, &otid, InplaceUpdateTupleLock)?;

    let natts = Natts_pg_database;
    let mut values = vec![Datum::null(); natts];
    let mut isnull = vec![false; natts];
    let mut replace = vec![false; natts];
    values[Anum_pg_database_dattablespace as usize - 1] = Datum::from_oid(dst_tblspcoid);
    replace[Anum_pg_database_dattablespace as usize - 1] = true;
    let mut newtuple =
        heaptuple::heap_modify_tuple(mcx, oldtuple, pgdbrel.descr(), &values, &isnull, &replace)?;
    catalog_indexing::CatalogTupleUpdate(mcx, pgdbrel, &otid, &mut newtuple)?;
    lmgr::UnlockTuple(pgdbrel, &otid, InplaceUpdateTupleLock)?;

    genam::systable_endscan(mcx, scan)?;

    checkpointer::RequestCheckpoint(
        transam_xlog::CHECKPOINT_IMMEDIATE
            | transam_xlog::CHECKPOINT_FORCE
            | transam_xlog::CHECKPOINT_WAIT,
    )?;

    xact::ForceSyncCommit();
    Ok(())
}

pub fn AlterDatabase<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterDatabaseStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<Oid> {
    let dbname = stmt.dbname.unwrap_or("");

    let mut distemplate: Option<&DefElem> = None;
    let mut dallowconnections: Option<&DefElem> = None;
    let mut dconnlimit: Option<&DefElem> = None;
    let mut dtablespace: Option<&DefElem> = None;
    let mut noptions = 0usize;

    for opt in stmt.options.iter() {
        let defel = opt.as_def_elem().expect("ALTER DATABASE options are DefElems");
        noptions += 1;
        let slot: &mut Option<&DefElem> = match defel.defname.unwrap_or("") {
            "is_template" => &mut distemplate,
            "allow_connections" => &mut dallowconnections,
            "connection_limit" => &mut dconnlimit,
            "tablespace" => &mut dtablespace,
            other => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("option \"{other}\" not recognized"))
                    .errposition(defel.location + 1)
                    .into_error()
                    .into());
            }
        };
        if slot.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("conflicting or redundant options".to_string())
                .errposition(defel.location + 1)
                .into_error()
                .into());
        }
        *slot = Some(defel);
    }

    if let Some(dts) = dtablespace {
        if noptions != 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "option \"{}\" cannot be specified with other options",
                    dts.defname.unwrap_or("")
                ))
                .errposition(dts.location + 1)
                .into_error()
                .into());
        }
        xact::PreventInTransactionBlock(is_top_level, "ALTER DATABASE SET TABLESPACE")?;
        movedb(mcx, dbname, explain::defGetString(mcx, dts)?)?;
        return Ok(InvalidOid);
    }

    let mut dbistemplate = false;
    if let Some(el) = distemplate {
        if el.arg.is_some() {
            dbistemplate = explain::defGetBoolean(el)?;
        }
    }
    let mut dballowconnections = true;
    if let Some(el) = dallowconnections {
        if el.arg.is_some() {
            dballowconnections = explain::defGetBoolean(el)?;
        }
    }
    let mut dbconnlimit = DATCONNLIMIT_UNLIMITED;
    if let Some(el) = dconnlimit {
        if el.arg.is_some() {
            dbconnlimit = match el.arg.and_then(|n| n.as_integer()) {
                Some(i) => i.ival,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "{} requires an integer value",
                            el.defname.unwrap_or("")
                        ))
                        .into_error()
                        .into())
                }
            };
            if dbconnlimit < DATCONNLIMIT_UNLIMITED {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid connection limit: {dbconnlimit}"))
                    .into_error()
                    .into());
            }
        }
    }

    let rel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;
    let (_nd, key) = name_key(mcx, dbname)?;
    let mut scan =
        genam::systable_beginscan(mcx, &rel, DatabaseNameIndexId, true, None, &[key])?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .into_error()
            .into());
    };
    let otid = tuple.t_self;
    lmgr::LockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

    let dboid = getattr(tuple, Anum_pg_database_oid, &rel).0.as_oid();
    let datconnlimit = getattr(tuple, Anum_pg_database_datconnlimit, &rel).0.as_i32();

    if crate::database_is_invalid_form(datconnlimit) {
        return Err(ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot alter invalid database \"{dbname}\""))
            .errhint("Use DROP DATABASE to drop invalid databases.".to_string())
            .into_error()
            .into());
    }

    if !aclchk::object_ownercheck(DATABASE_RELATION_ID, dboid, miscinit::GetUserId())? {
        return Err(not_owner(dbname));
    }

    if !dballowconnections && dboid == init_small::globals::MyDatabaseId() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot disallow connections for current database".to_string())
            .into_error()
            .into());
    }

    let natts = Natts_pg_database;
    let mut values = vec![Datum::null(); natts];
    let mut isnull = vec![false; natts];
    let mut replace = vec![false; natts];
    if distemplate.is_some() {
        values[Anum_pg_database_datistemplate as usize - 1] = Datum::from_bool(dbistemplate);
        replace[Anum_pg_database_datistemplate as usize - 1] = true;
    }
    if dallowconnections.is_some() {
        values[Anum_pg_database_datallowconn as usize - 1] = Datum::from_bool(dballowconnections);
        replace[Anum_pg_database_datallowconn as usize - 1] = true;
    }
    if dconnlimit.is_some() {
        values[Anum_pg_database_datconnlimit as usize - 1] = Datum::from_i32(dbconnlimit);
        replace[Anum_pg_database_datconnlimit as usize - 1] = true;
    }

    let mut newtuple =
        heaptuple::heap_modify_tuple(mcx, tuple, rel.descr(), &values, &isnull, &replace)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;
    lmgr::UnlockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

    genam::systable_endscan(mcx, scan)?;

    rel.close(types_storage::lock::NoLock)?;

    Ok(dboid)
}

pub fn AlterDatabaseRefreshColl<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterDatabaseRefreshCollStmt<'mcx>,
) -> PgResult<Oid> {
    let dbname = stmt.dbname.unwrap_or("");

    let rel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;
    let (_nd, key) = name_key(mcx, dbname)?;
    let mut scan =
        genam::systable_beginscan(mcx, &rel, DatabaseNameIndexId, true, None, &[key])?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .into_error()
            .into());
    };

    let db_id = getattr(tuple, Anum_pg_database_oid, &rel).0.as_oid();
    let datlocprovider = getattr(tuple, Anum_pg_database_datlocprovider, &rel).0.as_u8();

    if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db_id, miscinit::GetUserId())? {
        return Err(not_owner(dbname));
    }
    let otid = tuple.t_self;
    lmgr::LockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

    let (collversion_d, collversion_null) = getattr(tuple, Anum_pg_database_datcollversion, &rel);
    let oldversion = if collversion_null {
        None
    } else {
        Some(text_datum_string(mcx, collversion_d)?)
    };

    let locale_attno = if datlocprovider == COLLPROVIDER_LIBC {
        Anum_pg_database_datcollate
    } else {
        Anum_pg_database_datlocale
    };
    let (locale_d, locale_null) = getattr(tuple, locale_attno, &rel);
    if locale_null {
        return Err(ereport(ERROR)
            .errmsg("unexpected null in pg_database".to_string())
            .into_error()
            .into());
    }
    let locale = text_datum_string(mcx, locale_d)?;

    let newversion = pg_locale::get_collation_actual_version(datlocprovider, &locale)?;

    match (&oldversion, &newversion) {
        (None, Some(_)) | (Some(_), None) => {
            return Err(ereport(ERROR)
                .errmsg("invalid collation version change".to_string())
                .into_error()
                .into());
        }
        (Some(old), Some(new)) if old != new => {
            ereport(NOTICE)
                .errmsg(format!("changing version from {old} to {new}"))
                .finish(loc("AlterDatabaseRefreshColl"))?;

            let version_text = varlena::cstring_to_text(mcx, new.as_bytes())?;
            let natts = Natts_pg_database;
            let mut values = vec![Datum::null(); natts];
            let mut isnull = vec![false; natts];
            let mut replace = vec![false; natts];
            values[Anum_pg_database_datcollversion as usize - 1] =
                Datum::from_usize(version_text.as_bytes().as_ptr() as usize);
            replace[Anum_pg_database_datcollversion as usize - 1] = true;
            let mut newtuple = heaptuple::heap_modify_tuple(
                mcx,
                tuple,
                rel.descr(),
                &values,
                &isnull,
                &replace,
            )?;
            catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;
        }
        _ => {
            ereport(NOTICE)
                .errmsg("version has not changed".to_string())
                .finish(loc("AlterDatabaseRefreshColl"))?;
        }
    }
    lmgr::UnlockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

    genam::systable_endscan(mcx, scan)?;
    rel.close(types_storage::lock::NoLock)?;

    Ok(db_id)
}

pub fn AlterDatabaseSet<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterDatabaseSetStmt<'mcx>) -> PgResult<Oid> {
    let dbname = stmt.dbname.unwrap_or("");
    let datid = crate::get_database_oid(mcx, dbname, false)?;

    pg_shdepend::shdepLockAndCheckObject(mcx, DATABASE_RELATION_ID, datid)?;

    if !aclchk::object_ownercheck(DATABASE_RELATION_ID, datid, miscinit::GetUserId())? {
        return Err(not_owner(dbname));
    }

    let setstmt = stmt
        .setstmt
        .as_ref()
        .and_then(|n| n.as_variable_set_stmt())
        .expect("AlterDatabaseSetStmt.setstmt");
    pg_db_role_setting::AlterSetting(mcx, datid, InvalidOid, setstmt)?;

    lmgr::UnlockSharedObject(DATABASE_RELATION_ID, datid, 0, AccessShareLock)?;

    Ok(datid)
}

pub fn AlterDatabaseOwner(mcx: Mcx<'_>, dbname: &str, new_owner_id: Oid) -> PgResult<Oid> {
    let rel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;
    let (_nd, key) = name_key(mcx, dbname)?;
    let mut scan =
        genam::systable_beginscan(mcx, &rel, DatabaseNameIndexId, true, None, &[key])?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .into_error()
            .into());
    };

    let db_id = getattr(tuple, Anum_pg_database_oid, &rel).0.as_oid();
    let datdba = getattr(tuple, Anum_pg_database_datdba, &rel).0.as_oid();

    if datdba != new_owner_id {
        if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db_id, miscinit::GetUserId())? {
            return Err(not_owner(dbname));
        }

        if !adt_acl::member_can_set_role(miscinit::GetUserId(), new_owner_id)? {
            let rolename = miscinit::GetUserNameFromId(mcx, new_owner_id, false)?
                .expect("noerr=false yields a name");
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "must be able to SET ROLE \"{}\"",
                    rolename.as_str()
                ))
                .into_error()
                .into());
        }

        if !have_createdb_privilege()? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to change owner of database".to_string())
                .into_error()
                .into());
        }

        let otid = tuple.t_self;
        lmgr::LockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

        let natts = Natts_pg_database;
        let mut values = vec![Datum::null(); natts];
        let mut isnull = vec![false; natts];
        let mut replace = vec![false; natts];
        replace[Anum_pg_database_datdba as usize - 1] = true;
        values[Anum_pg_database_datdba as usize - 1] = Datum::from_oid(new_owner_id);

        let (acl_d, acl_null) = getattr(tuple, Anum_pg_database_datacl, &rel);
        let new_acl_img;
        if !acl_null {
            let p = acl_d.as_usize() as *const u8;
            // SAFETY: non-null datacl datum: a live varlena image inside the
            // scanned tuple, readable through its varsize_any extent.
            let image = unsafe {
                core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
            };
            let payload = varlena::open_image(mcx, image)?;
            let items = adt_acl::varlena::decode_acl_payload(mcx, payload.as_bytes())?;
            let new_items = adt_acl::aclnewowner(mcx, &items, datdba, new_owner_id)?;
            new_acl_img = adt_acl::varlena::acl_image(mcx, &new_items)?;
            replace[Anum_pg_database_datacl as usize - 1] = true;
            values[Anum_pg_database_datacl as usize - 1] =
                Datum::from_usize(new_acl_img.as_ptr() as usize);
        }

        let mut newtuple =
            heaptuple::heap_modify_tuple(mcx, tuple, rel.descr(), &values, &isnull, &replace)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;
        lmgr::UnlockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

        pg_shdepend::changeDependencyOnOwner(mcx, DATABASE_RELATION_ID, db_id, new_owner_id)?;
    }

    genam::systable_endscan(mcx, scan)?;
    rel.close(types_storage::lock::NoLock)?;

    Ok(db_id)
}
