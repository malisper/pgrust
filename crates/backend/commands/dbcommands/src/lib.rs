#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use cache_syscache::cacheinfo::DATABASEOID;
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use pg_database_seams::PgDatabaseForm;
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::{InvalidOid, Oid};
use types_error::{ErrorLocation, PgResult, ERROR, PANIC, WARNING};
use types_storage::lock::{AccessExclusiveLock, LOCKMODE};
use types_storage::storage::ProcSignalBarrierType;
use xlogreader_seams::XLogReaderState;

pub mod builtins;
mod alterdb;
mod createdb;
mod dropdb;
mod walcopy;

pub use alterdb::{
    movedb, AlterDatabase, AlterDatabaseOwner, AlterDatabaseRefreshColl, AlterDatabaseSet,
    RenameDatabase,
};
pub use createdb::{check_encoding_locale_matches, createdb};
pub use dropdb::dropdb;
pub(crate) use dropdb::name_key;

#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

const ANUM_PG_DATABASE_DATNAME: i32 = 2;
const NAMEDATALEN: usize = 64;

pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
pub const XLOG_DBASE_DROP: u8 = 0x20;
const XLR_INFO_MASK: u8 = 0x0F;

pub const GLOBALTABLESPACE_OID: Oid = 1664;
pub const TableSpaceRelationId: Oid = 1213;

pub fn get_database_name(dbid: Oid) -> PgResult<Option<String>> {
    let Some(tuple) = SearchSysCache1(DATABASEOID, SysCacheKey::Value(Datum::from_oid(dbid)))?
    else {
        return Ok(None);
    };
    let d = SysCacheGetAttrNotNull(DATABASEOID, &tuple, ANUM_PG_DATABASE_DATNAME)?;
    // SAFETY: datname is a NameData column; the datum points at its
    // NUL-terminated 64-byte buffer inside the pinned tuple image.
    let name = unsafe {
        let p = d.as_usize() as *const u8;
        let mut len = 0usize;
        while len < NAMEDATALEN && *p.add(len) != 0 {
            len += 1;
        }
        core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len)).to_owned()
    };
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

/// get_db_info (dbcommands.c): scan by name, lock the found OID, then re-fetch
/// by OID and re-check the name (covers a concurrent rename).
pub fn get_db_info<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    lockmode: LOCKMODE,
) -> PgResult<Option<PgDatabaseForm<'mcx>>> {
    loop {
        let Some(scanned) = pg_database::get_database_tuple_by_name(mcx, name)? else {
            return Ok(None);
        };
        let dboid = scanned.oid;

        if lockmode != types_storage::lock::NoLock {
            lmgr::LockSharedObject(DATABASE_RELATION_ID, dboid, 0, lockmode)?;
        }

        if let Some(form) = pg_database::search_database_syscache(mcx, dboid)? {
            if form.datname.as_str() == name {
                return Ok(Some(form));
            }
        }
        if lockmode != types_storage::lock::NoLock {
            lmgr::UnlockSharedObject(DATABASE_RELATION_ID, dboid, 0, lockmode)?;
        }
    }
}

pub fn get_database_oid(mcx: Mcx<'_>, dbname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = match pg_database::get_database_tuple_by_name(mcx, dbname)? {
        Some(form) => form.oid,
        None => InvalidOid,
    };
    if oid == InvalidOid && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{dbname}\" does not exist"))
            .into_error()
            .into());
    }
    Ok(oid)
}

pub fn database_is_invalid_form(datconnlimit: i32) -> bool {
    datconnlimit == pg_database::DATCONNLIMIT_INVALID_DB
}

pub fn database_is_invalid_oid(mcx: Mcx<'_>, dboid: Oid) -> PgResult<bool> {
    match pg_database::search_database_syscache(mcx, dboid)? {
        Some(form) => Ok(database_is_invalid_form(form.datconnlimit)),
        None => Err(ereport(ERROR)
            .errmsg(format!("cache lookup failed for database {dboid}"))
            .into_error()
            .into()),
    }
}

const ANUM_PG_AUTHID_ROLCREATEDB: i32 = 6;

pub fn have_createdb_privilege() -> PgResult<bool> {
    if superuser::superuser()? {
        return Ok(true);
    }
    let roleid = miscinit::GetUserId();
    let Some(tuple) = SearchSysCache1(
        cache_syscache::cacheinfo::AUTHOID,
        SysCacheKey::Value(Datum::from_oid(roleid)),
    )?
    else {
        return Ok(false);
    };
    let result = SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::AUTHOID,
        &tuple,
        ANUM_PG_AUTHID_ROLCREATEDB,
    )?
    .as_bool();
    ReleaseSysCache(tuple);
    Ok(result)
}

pub fn errdetail_busy_db(notherbackends: i32, npreparedxacts: i32) -> String {
    if notherbackends > 0 && npreparedxacts > 0 {
        format!(
            "There are {notherbackends} other session(s) and {npreparedxacts} prepared transaction(s) using the database."
        )
    } else if notherbackends > 0 {
        if notherbackends == 1 {
            "There is 1 other session using the database.".into()
        } else {
            format!("There are {notherbackends} other sessions using the database.")
        }
    } else if npreparedxacts == 1 {
        "There is 1 prepared transaction using the database.".into()
    } else {
        format!("There are {npreparedxacts} prepared transactions using the database.")
    }
}

/// remove_dbtablespaces (dbcommands.c): rmtree every per-tablespace dir of the
/// database and log one XLOG_DBASE_DROP record naming them all.
pub fn remove_dbtablespaces(mcx: Mcx<'_>, db_id: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, TableSpaceRelationId, types_storage::lock::AccessShareLock)?;
    let mut tablespace_ids: Vec<Oid> = Vec::new();
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: pg_tablespace row under this relation's descriptor; attno 1
        // is the oid column.
        let dsttablespace =
            unsafe { types_tuple::heap_getattr(tup, 1, rel.descr(), &mut isnull) }.as_oid();
        if dsttablespace == GLOBALTABLESPACE_OID {
            continue;
        }
        let dstpath = relpath::GetDatabasePath(mcx, db_id, dsttablespace)?;
        match std::fs::symlink_metadata(dstpath.as_str()) {
            Ok(md) if md.is_dir() => {}
            _ => continue,
        }
        if !fd::rmtree(dstpath.as_str(), true)? {
            ereport(WARNING)
                .errmsg(format!(
                    "some useless files may be left behind in old database directory \"{}\"",
                    dstpath.as_str()
                ))
                .finish(loc("remove_dbtablespaces"))?;
        }
        tablespace_ids.push(dsttablespace);
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_storage::lock::AccessShareLock)?;

    if tablespace_ids.is_empty() {
        return Ok(());
    }

    let mut xlrec = Vec::with_capacity(8 + 4 * tablespace_ids.len());
    xlrec.extend_from_slice(&db_id.to_ne_bytes());
    xlrec.extend_from_slice(&(tablespace_ids.len() as i32).to_ne_bytes());
    for ts in &tablespace_ids {
        xlrec.extend_from_slice(&ts.to_ne_bytes());
    }
    xloginsert::insert_record(
        types_core::primitive::RmgrIds::RM_DBASE_ID as u8,
        XLOG_DBASE_DROP | xloginsert::XLR_SPECIAL_REL_UPDATE,
        0,
        &[&xlrec],
        &[],
    )?;
    Ok(())
}

/// check_db_file_conflict (dbcommands.c): any tablespace already holding a
/// directory for this OID makes the OID unusable.
pub fn check_db_file_conflict(mcx: Mcx<'_>, db_id: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, TableSpaceRelationId, types_storage::lock::AccessShareLock)?;
    let mut result = false;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: pg_tablespace row; attno 1 is the oid column.
        let dsttablespace =
            unsafe { types_tuple::heap_getattr(tup, 1, rel.descr(), &mut isnull) }.as_oid();
        if dsttablespace == GLOBALTABLESPACE_OID {
            continue;
        }
        let dstpath = relpath::GetDatabasePath(mcx, db_id, dsttablespace)?;
        if std::fs::symlink_metadata(dstpath.as_str()).is_ok() {
            result = true;
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_storage::lock::AccessShareLock)?;
    Ok(result)
}

pub(crate) fn get_parent_directory(path: &str) -> &str {
    match path.rfind('/') {
        Some(0) => "/",
        Some(idx) => &path[..idx],
        None => ".",
    }
}

const PG_TBLSPC_DIR_SLASH: &str = "pg_tblspc/";

fn recovery_create_dbdir(path: &str, only_tblspc: bool) -> PgResult<()> {
    if std::fs::metadata(path).is_ok() {
        return Ok(());
    }
    if only_tblspc && !path.contains(PG_TBLSPC_DIR_SLASH) {
        return Err(ereport(PANIC)
            .errmsg(format!("requested to created invalid directory: {path}"))
            .into_error()
            .into());
    }
    let reached_consistency = xlogrecovery_seams::reached_consistency::call();
    let in_place_ok = guc_tables::vars::allow_in_place_tablespaces.installed()
        && guc_tables::vars::allow_in_place_tablespaces.read();
    // After consistency a missing tablespace dir means tablespace loss, never
    // the expected drop-then-recreate case; masking it corrupts silently.
    if reached_consistency && !in_place_ok {
        return Err(ereport(PANIC)
            .errmsg(format!("missing directory \"{path}\""))
            .into_error()
            .into());
    }
    if reached_consistency {
        ereport(WARNING)
            .errmsg(format!("creating missing directory: {path}"))
            .finish(loc("recovery_create_dbdir"))?;
    }
    fd::pg_mkdir_p(path)
}

/// dbase_redo (dbcommands.c). WAL_LOG-strategy create records are loud: the
/// WAL_LOG copy engine is unported (createdb runs FILE_COPY only).
pub fn dbase_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let decoded = record.record.as_ref().expect("dbase_redo with no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;
    // SAFETY: the decoded record's main data lives for the redo call.
    let data = unsafe { decoded.main_data_bytes() };

    let ctx = mcx::MemoryContext::new("dbase_redo");
    let mcx = ctx.mcx();

    let u32_at = |off: usize| -> u32 {
        u32::from_ne_bytes(data[off..off + 4].try_into().expect("short dbase record"))
    };

    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let db_id = u32_at(0);
        let tablespace_id = u32_at(4);
        let src_db_id = u32_at(8);
        let src_tablespace_id = u32_at(12);

        let src_path = relpath::GetDatabasePath(mcx, src_db_id, src_tablespace_id)?;
        let dst_path = relpath::GetDatabasePath(mcx, db_id, tablespace_id)?;

        if let Ok(md) = std::fs::metadata(dst_path.as_str()) {
            if md.is_dir() && !fd::rmtree(dst_path.as_str(), true)? {
                ereport(WARNING)
                    .errmsg(format!(
                        "some useless files may be left behind in old database directory \"{}\"",
                        dst_path.as_str()
                    ))
                    .finish(loc("dbase_redo"))?;
            }
        }

        let parent = get_parent_directory(dst_path.as_str());
        if let Err(e) = std::fs::metadata(parent) {
            if e.kind() != std::io::ErrorKind::NotFound {
                // C names dst_path here, not the parent it stat'ed.
                return Err(ereport(types_error::FATAL)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errmsg(format!("could not stat directory \"{}\": %m", dst_path.as_str()))
                    .into_error()
                    .into());
            }
            recovery_create_dbdir(parent, true)?;
        }
        if matches!(std::fs::metadata(src_path.as_str()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound)
        {
            recovery_create_dbdir(src_path.as_str(), false)?;
        }

        bufmgr::FlushDatabaseBuffers(src_db_id)?;

        let gen = procsignal::EmitProcSignalBarrier(
            ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
        );
        procsignal::WaitForProcSignalBarrier(gen)?;

        fd::copydir(src_path.as_str(), dst_path.as_str(), false)?;
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let db_id = u32_at(0);
        let tablespace_id = u32_at(4);

        let dbpath = relpath::GetDatabasePath(mcx, db_id, tablespace_id)?;
        recovery_create_dbdir(get_parent_directory(dbpath.as_str()), true)?;
        walcopy::CreateDirAndVersionFile(dbpath.as_str(), db_id, tablespace_id, true)?;
    } else if info == XLOG_DBASE_DROP {
        let db_id = u32_at(0);
        let ntablespaces = i32::from_ne_bytes(data[4..8].try_into().expect("short drop record"));

        if xlogutils::InHotStandby() {
            // Lock out InitPostgres re-connects (and walsenders on
            // db-specific slots) while conflicts resolve.
            lmgr::LockSharedObjectForSession(DATABASE_RELATION_ID, db_id, 0, AccessExclusiveLock)?;
            standby::ResolveRecoveryConflictWithDatabase(db_id)?;
        }

        // Drop any database-specific replication slots (dbcommands.c:3432).
        if slot_seams::replication_slots_drop_db_slots::is_installed() {
            slot_seams::replication_slots_drop_db_slots::call(db_id)?;
        }
        bufmgr::DropDatabaseBuffers(db_id)?;
        smgr::ForgetDatabaseSyncRequests(db_id)?;
        xlogutils::XLogDropDatabase(db_id)?;

        let gen = procsignal::EmitProcSignalBarrier(
            ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
        );
        procsignal::WaitForProcSignalBarrier(gen)?;

        for i in 0..ntablespaces.max(0) as usize {
            let tsid = u32_at(8 + 4 * i);
            let dst_path = relpath::GetDatabasePath(mcx, db_id, tsid)?;
            if !fd::rmtree(dst_path.as_str(), true)? {
                ereport(WARNING)
                    .errmsg(format!(
                        "some useless files may be left behind in old database directory \"{}\"",
                        dst_path.as_str()
                    ))
                    .finish(loc("dbase_redo"))?;
            }
        }

        if xlogutils::InHotStandby() {
            // Release prior to commit; the reconnect race window is small, as C.
            lmgr::UnlockSharedObjectForSession(DATABASE_RELATION_ID, db_id, 0, AccessExclusiveLock)?;
        }
    } else {
        panic!("dbase_redo: unknown op code {info}");
    }
    Ok(())
}

pub fn init_seams() {
    dbcommands_seams::get_database_name::set(get_database_name);
    dbcommands_seams::get_database_oid::set(get_database_oid);
    dbcommands_seams::dbase_redo::set(dbase_redo);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_seams() {
        init_seams();
        assert!(dbcommands_seams::get_database_name::is_installed());
        assert!(dbcommands_seams::dbase_redo::is_installed());
        // Unbooted catcache: loud stop, never a fabricated name.
        assert!(std::panic::catch_unwind(|| dbcommands_seams::get_database_name::call(1)).is_err());
    }
}
