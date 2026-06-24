//! `backend/commands/dbcommands.c` — the heavy half: the CREATE / DROP / RENAME
//! / ALTER DATABASE command entry points and their copy / cleanup helpers.
//!
//! Branch-for-branch with the C. The pg_database catalog writes go through the
//! landed `backend-catalog-pg-database` mutate seams; the cross-subsystem
//! primitives (tablespace catalog scan, the WAL_LOG copy engine, ACL /
//! dependency / seclabel / comment cleanup, procarray backend counts,
//! replication slots, locale validation, GUC settings, checkpoint / xlog,
//! filesystem) go through their owners' real fns or seams.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use ::utils_error::ereport;
use ::mcx::Mcx;

use ::types_acl::acl::{AclResult, ACL_CREATE, ACLCHECK_NOT_OWNER};
use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_catalog::pg_collation::{COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, COLLPROVIDER_LIBC};
use ::types_catalog::pg_database::{
    DatabaseRelationId, NewDbRecord, DATCONNLIMIT_UNLIMITED,
};
use ::types_core::catalog::FirstNormalObjectId;
use ::types_core::{InvalidOid, Oid, OidIsValid};
use ::types_error::{
    PgResult, ERRCODE_DUPLICATE_DATABASE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_DATABASE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, FATAL, NOTICE, WARNING,
};
use ::nodes::ddlnodes::{
    AlterDatabaseRefreshCollStmt, AlterDatabaseSetStmt, AlterDatabaseStmt, CreatedbStmt, DefElem,
    DropdbStmt,
};
use ::nodes::nodes::Node;
use ::nodes::parsenodes::{OBJECT_DATABASE, OBJECT_TABLESPACE};
use ::nodes::parsestmt::ParseState;
use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, LockRelId, RowExclusiveLock, ShareLock, NoLock,
};
use ::types_storage::storage::{ProcSignalBarrierType, RelFileLocator};
use ::wal::xlog_consts::{
    CHECKPOINT_FLUSH_ALL, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT,
};
use ::wal::{RM_DBASE_ID, XLR_SPECIAL_REL_UPDATE};
use ::types_wchar::encoding::{pg_valid_be_encoding, PG_SQL_ASCII};

use crate::{
    database_is_invalid_oid, errdetail_busy_db, errloc, get_database_name, get_database_oid,
    get_db_info, have_createdb_privilege,
};

// Owner crates / seams.
use ::table::{table_close, table_open};
use pg_database_seams as dbcat;
use pg_tablespace_seams as tscat;
use catalog_storage_seams as storage;
use collationcmds_seams as coll;
use slot_seams as slot;
use pgstat_seams as pgstat;

// ---------------------------------------------------------------------------
// Constants (catalog/headers).
// ---------------------------------------------------------------------------

/// `GLOBALTABLESPACE_OID` (pg_tablespace.h).
const GLOBALTABLESPACE_OID: Oid = 1664;
/// `TableSpaceRelationId` (pg_tablespace.h).
const TableSpaceRelationId: Oid = 1213;
const DatabaseOidIndexId: Oid = ::types_catalog::pg_database::DatabaseOidIndexId;
const Anum_pg_database_oid: i32 = ::types_catalog::pg_database::Anum_pg_database_oid;

/// `XLOG_DBASE_*` opcodes (dbcommands_xlog.h).
const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
const XLOG_DBASE_DROP: u8 = 0x20;

/// `CreateDBStrategy` (dbcommands.c).
#[derive(Clone, Copy, PartialEq, Eq)]
enum CreateDBStrategy {
    WalLog,
    FileCopy,
}

// ---------------------------------------------------------------------------
// DefElem value extractors over the `'mcx` parse-node layer (define.c).
//
// The statement option lists are `PgVec<NodePtr>` of `T_DefElem` nodes. We read
// the option name and value (`defGetString`/`defGetInt32`/`defGetBoolean`/
// `defGetObjectId`) directly off the contained value node, matching the
// behaviour of define.c's extractors.
// ---------------------------------------------------------------------------

/// `defel->defname`, or `""` (the parser always fills it).
fn def_name<'a>(defel: &'a DefElem<'_>) -> &'a str {
    defel.defname.as_deref().unwrap_or("")
}

/// `defel->arg != NULL`.
fn defel_has_arg(defel: &DefElem) -> bool {
    defel.arg.is_some()
}

/// `defGetString(defel)` — `strVal(defel->arg)`; raises if the value isn't a
/// String or there's no arg (define.c).
fn def_get_string(defel: &DefElem) -> PgResult<String> {
    match defel.arg.as_deref() {
        Some(n) if n.is_string() => Ok(n.expect_string().sval.as_str().to_string()),
        Some(n) if n.is_integer() || n.is_float() || n.is_boolean() => {
            // define.c: TypeName/numeric arms aren't used by the database
            // options, so the only valid string-yielding arm is T_String.
            Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("{} requires a parameter", def_name(defel)))
                .finish(errloc(0, "defGetString"))
                .unwrap_err())
        }
        _ => Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{} requires a parameter", def_name(defel)))
            .finish(errloc(0, "defGetString"))
            .unwrap_err()),
    }
}

/// `defGetInt32(defel)` (define.c).
fn def_get_int32(defel: &DefElem) -> PgResult<i32> {
    match defel.arg.as_deref().and_then(|n| n.as_integer()) {
        Some(i) => Ok(i.ival),
        _ => Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{} requires an integer value", def_name(defel)))
            .finish(errloc(0, "defGetInt32"))
            .unwrap_err()),
    }
}

/// `defGetBoolean(defel)` (define.c) — accepts a Boolean, an Integer (0/1), or
/// the strings "true"/"false"/"on"/"off".
fn def_get_boolean(defel: &DefElem) -> PgResult<bool> {
    match defel.arg.as_deref() {
        Some(n) if n.is_boolean() => Ok(n.expect_boolean().boolval),
        Some(n) if n.is_integer() => match n.expect_integer().ival {
            0 => Ok(false),
            1 => Ok(true),
            _ => boolean_err(defel),
        },
        Some(n) if n.is_string() => {
            let s = n.expect_string();
            let v = s.sval.as_str();
            if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on") {
                Ok(true)
            } else if v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off") {
                Ok(false)
            } else {
                boolean_err(defel)
            }
        }
        _ => boolean_err(defel),
    }
}

fn boolean_err(defel: &DefElem) -> PgResult<bool> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("{} requires a Boolean value", def_name(defel)))
        .finish(errloc(0, "defGetBoolean"))
        .unwrap_err())
}

/// `defGetObjectId(defel)` (define.c).
fn def_get_object_id(defel: &DefElem) -> PgResult<Oid> {
    match defel.arg.as_deref().and_then(|n| n.as_integer()) {
        Some(i) => Ok(i.ival as u32),
        _ => Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{} requires a numeric value", def_name(defel)))
            .finish(errloc(0, "defGetObjectId"))
            .unwrap_err()),
    }
}

/// `IsA(defel->arg, Integer)`.
fn arg_is_integer(defel: &DefElem) -> bool {
    defel.arg.as_deref().is_some_and(|n| n.is_integer())
}

/// `errorConflictingDefElem(defel, pstate)` (define.c): a duplicate option.
fn error_conflicting_def_elem(_defel: &DefElem, _pstate: &ParseState<'_>) -> PgResult<()> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("conflicting or redundant options"))
        .finish(errloc(0, "errorConflictingDefElem"))
        .map(|_| ())
        .unwrap_err())
}

/// Iterate a statement's `PgVec<NodePtr>` option list as `&DefElem` references.
fn def_elems<'a, 'mcx>(
    options: &'a [::mcx::PgBox<'mcx, Node<'mcx>>],
) -> Vec<&'a DefElem<'mcx>> {
    options
        .iter()
        .filter_map(|n| n.as_defelem())
        .collect()
}

/// Allocate a `PgString` in `mcx` from a `&str` (the `cstring_to_text`/
/// `pstrdup` analog for the formed catalog columns).
fn pgstr<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<::mcx::PgString<'mcx>> {
    ::mcx::PgString::from_str_in(s, mcx)
}

/// As [`pgstr`], lifting an `Option<&str>` to `Option<PgString>`.
fn pgstr_opt<'mcx>(mcx: Mcx<'mcx>, s: Option<&str>) -> PgResult<Option<::mcx::PgString<'mcx>>> {
    match s {
        Some(s) => Ok(Some(pgstr(mcx, s)?)),
        None => Ok(None),
    }
}

/// `collprovider_name(c)` (pg_locale.c): the user-facing provider name.
fn collprovider_name(c: i8) -> &'static str {
    if c == COLLPROVIDER_BUILTIN {
        "builtin"
    } else if c == COLLPROVIDER_ICU {
        "icu"
    } else if c == COLLPROVIDER_LIBC {
        "libc"
    } else {
        "???"
    }
}

// ===========================================================================
// ScanSourceDatabasePgClass — C 250-470 (dbcommands.c)
// ===========================================================================

/// `RELKIND_HAS_STORAGE(relkind)` (pg_class.h) — relkinds that have physical
/// storage (table / index / sequence / TOAST value / materialized view).
fn relkind_has_storage(relkind: u8) -> bool {
    use ::types_tuple::access::{
        RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
    };
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// `ScanSourceDatabasePgClassTuple(tuple, tbid, dbid, srcpath)` (dbcommands.c):
/// decide whether a `pg_class` tuple represents something that needs copying
/// and, if so, build a [`CreateDBRelInfo`]. Visibility was already checked by
/// the caller. `userdata` is the tuple's user-data area (the GETSTRUCT view of
/// `FormData_pg_class`).
///
/// The fixed columns this reads are all in the non-null fixed-width prefix of
/// pg_class, so the C `GETSTRUCT` struct overlay is exact: `oid` @0,
/// `relfilenode` @88, `reltablespace` @92, `relkind` @119, `relpersistence`
/// @118 (every preceding column is a 4-byte Oid/int32/float4 or the 64-byte
/// NameData `relname`, none nullable).
fn scan_source_database_pg_class_tuple(
    userdata: &[u8],
    tbid: Oid,
    dbid: Oid,
    srcpath: &str,
) -> PgResult<Option<storage::CreateDBRelInfo>> {
    use ::types_tuple::access::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP};
    // GETSTRUCT field readers over the fixed FormData_pg_class prefix.
    let oid_at = |o: usize| -> PgResult<Oid> {
        userdata
            .get(o..o + 4)
            .map(|b| Oid::from_ne_bytes([b[0], b[1], b[2], b[3]]))
            .ok_or_else(|| {
                ::types_error::PgError::error("pg_class tuple shorter than FormData_pg_class prefix")
            })
    };
    let char_at = |o: usize| -> PgResult<u8> {
        userdata.get(o).copied().ok_or_else(|| {
            ::types_error::PgError::error("pg_class tuple shorter than FormData_pg_class prefix")
        })
    };

    let class_oid = oid_at(0)?;
    let relfilenode = oid_at(88)?;
    let reltablespace = oid_at(92)?;
    let relpersistence = char_at(118)?;
    let relkind = char_at(119)?;

    // Return None if this object does not need to be copied: shared objects
    // (GLOBALTABLESPACE_OID), objects without storage, and temporary relations.
    if reltablespace == GLOBALTABLESPACE_OID
        || !relkind_has_storage(relkind)
        || relpersistence == RELPERSISTENCE_TEMP
    {
        return Ok(None);
    }

    // If relfilenumber is valid then directly use it. Otherwise consult the
    // relmap (mapped catalogs have relfilenode == 0).
    let relfilenumber = if OidIsValid(relfilenode) {
        relfilenode
    } else {
        relmapper::RelationMapOidToFilenumberForDatabase(srcpath, class_oid)?
    };

    // We must have a valid relfilenumber.
    if !OidIsValid(relfilenumber) {
        return Err(::types_error::PgError::error(format!(
            "relation with OID {class_oid} does not have a valid relfilenumber"
        )));
    }

    // Prepare a rel info element.
    let spc_oid = if OidIsValid(reltablespace) {
        reltablespace
    } else {
        tbid
    };
    Ok(Some(storage::CreateDBRelInfo {
        rlocator: RelFileLocator {
            spcOid: spc_oid,
            dbOid: dbid,
            relNumber: relfilenumber,
        },
        reloid: class_oid,
        // Temporary relations were rejected above.
        permanent: relpersistence == RELPERSISTENCE_PERMANENT,
    }))
}

/// `ScanSourceDatabasePgClass(tbid, dbid, srcpath)` (dbcommands.c): the
/// cross-database raw buffered scan of the source database's `pg_class`
/// relation. We can't rely on the relcache (it only knows the connected
/// database) or the heap-scan infrastructure (it might do HOT pruning, unsafe
/// in a database we're not connected to), so this reads `pg_class` block by
/// block through the buffer manager and walks each page's line pointers,
/// gating on `HeapTupleSatisfiesVisibility`.
pub(crate) fn scan_source_database_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    tbid: Oid,
    dbid: Oid,
    srcpath: &str,
) -> PgResult<::mcx::PgVec<'mcx, storage::CreateDBRelInfo>> {
    use bufmgr_seams as bufmgr;
    use ::page::{
        ItemIdGetLength, ItemIdIsDead, ItemIdIsRedirected, ItemIdIsUsed, PageGetItem, PageGetItemId,
        PageGetMaxOffsetNumber, PageIsEmpty, PageIsNew, PageRef,
    };
    use ::types_storage::buf::BUFFER_LOCK_SHARE;
    use ::types_tuple::heaptuple::FormedTuple;
    use ::types_tuple::heaptuple::FIRST_OFFSET_NUMBER;

    let pg_class_oid = ::types_core::catalog::RELATION_RELATION_ID;

    // Get pg_class relfilenumber.
    let relfilenumber =
        relmapper::RelationMapOidToFilenumberForDatabase(srcpath, pg_class_oid)?;

    // Don't read data into shared_buffers without holding a relation lock.
    let relid = LockRelId {
        relId: pg_class_oid,
        dbId: dbid,
    };
    lmgr::LockRelationId(&relid, AccessShareLock)?;

    // Prepare a RelFileLocator for the pg_class relation.
    let rlocator = RelFileLocator {
        spcOid: tbid,
        dbOid: dbid,
        relNumber: relfilenumber,
    };

    // smgr = smgropen(rlocator, INVALID_PROC_NUMBER); nblocks = smgrnblocks(smgr,
    // MAIN_FORKNUM); smgrclose(smgr). pg_class is a permanent catalog.
    let nblocks = smgr_seams::smgrnblocks::call(
        rlocator,
        ::types_core::primitive::INVALID_PROC_NUMBER,
        ::types_core::primitive::ForkNumber::MAIN_FORKNUM,
    )?;

    // We need a snapshot that will see all committed transactions as committed;
    // GetLatestSnapshot() works fine.
    let snapshot = snapmgr::RegisterSnapshot(Some(
        &snapmgr::GetLatestSnapshot()?,
    ));

    let mut rlocatorlist: ::mcx::PgVec<'mcx, storage::CreateDBRelInfo> = ::mcx::PgVec::new_in(mcx);

    // Process the relation block by block.
    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        // buf = ReadBufferWithoutRelcache(rlocator, MAIN_FORKNUM, blkno,
        //   RBM_NORMAL, bstrategy, permanent=true).
        let buf = bufmgr::read_buffer_without_relcache::call(
            rlocator,
            ::types_core::primitive::ForkNumber::MAIN_FORKNUM,
            blkno,
            ::types_storage::storage::ReadBufferMode::Normal,
            // CreateAndCopyRelationData reads the source with a BAS_BULKREAD ring.
            ::types_storage::buf::IOContext::IOCONTEXT_BULKREAD,
            true,
        )?;

        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;

        // Walk the page's line pointers under the share lock, collecting the
        // visible pg_class tuples that need copying.
        let mut page_relinfos: Vec<storage::CreateDBRelInfo> = Vec::new();
        bufmgr::with_buffer_page::call(buf, &mut |page_bytes| {
            let page = PageRef::new(page_bytes)?;
            if PageIsNew(&page) || PageIsEmpty(&page) {
                return Ok(());
            }

            let maxoff = PageGetMaxOffsetNumber(&page);
            let mut offnum = FIRST_OFFSET_NUMBER;
            while offnum <= maxoff {
                let itemid = PageGetItemId(&page, offnum)?;

                // Nothing to do if the slot is empty or already dead.
                if !ItemIdIsUsed(&itemid) || ItemIdIsDead(&itemid) || ItemIdIsRedirected(&itemid) {
                    offnum += 1;
                    continue;
                }

                // Materialize the on-page tuple (header + user-data area).
                let item = PageGetItem(&page, &itemid)?;
                let len = ItemIdGetLength(&itemid) as usize;
                let formed = FormedTuple::read_on_page_full(
                    mcx,
                    &item[..len],
                    blkno,
                    offnum,
                    pg_class_oid,
                )?;

                // Skip tuples that are not visible to this snapshot.
                let visible = match &snapshot {
                    Some(snap) => {
                        let mut tuple = formed.tuple.clone_in(mcx)?;
                        heapam_visibility::HeapTupleSatisfiesVisibility(
                            &mut tuple,
                            &mut snap.borrow_mut(),
                            buf,
                        )?
                    }
                    None => false,
                };

                if visible {
                    if let Some(relinfo) = scan_source_database_pg_class_tuple(
                        formed.data.as_slice(),
                        tbid,
                        dbid,
                        srcpath,
                    )? {
                        page_relinfos.push(relinfo);
                    }
                }

                offnum += 1;
            }
            Ok(())
        })?;

        for relinfo in page_relinfos {
            rlocatorlist.push(relinfo);
        }

        // UnlockReleaseBuffer(buf).
        bufmgr::unlock_release_buffer::call(buf);
    }

    snapmgr::UnregisterSnapshot(snapshot.as_ref())?;

    // Release relation lock.
    lmgr::UnlockRelationId(&relid, AccessShareLock)?;

    Ok(rlocatorlist)
}

// ===========================================================================
// CreateDatabaseUsingWalLog — C 147-226
// ===========================================================================

/// Create a new database using the WAL_LOG strategy. Each copied block is
/// separately written to the write-ahead log.
fn CreateDatabaseUsingWalLog(
    src_dboid: Oid,
    dst_dboid: Oid,
    src_tsid: Oid,
    dst_tsid: Oid,
) -> PgResult<()> {
    // Get source and destination database paths.
    let srcpath = relpath::GetDatabasePath(src_dboid, src_tsid);
    let dstpath = relpath::GetDatabasePath(dst_dboid, dst_tsid);

    // Create database directory and write PG_VERSION file.
    CreateDirAndVersionFile(&dstpath, dst_dboid, dst_tsid, false)?;

    // Copy relmap file from source database to the destination database.
    relmapper::RelationMapCopy(dst_dboid, dst_tsid, &srcpath, &dstpath)?;

    // Get list of relfilelocators to copy from the source database. The whole
    // cross-database raw buffered pg_class scan + visibility lives behind the
    // storage owner seam (its buffer/smgr/snapshot engine is not the command
    // layer's to own).
    let ctx = ::mcx::MemoryContext::new("CreateDatabaseUsingWalLog");
    let rlocatorlist = storage::scan_source_database_pg_class::call(
        ctx.mcx(),
        src_tsid,
        src_dboid,
        &srcpath,
    )?;

    // Database IDs will be the same for all relations.
    let mut srcrelid = LockRelId {
        relId: InvalidOid,
        dbId: src_dboid,
    };
    let mut dstrelid = LockRelId {
        relId: InvalidOid,
        dbId: dst_dboid,
    };

    for relinfo in rlocatorlist.iter() {
        let srcrlocator = relinfo.rlocator;

        // If the relation is from the source db's default tablespace then we
        // create it in the destination db's default tablespace; otherwise we
        // create it in the same tablespace as in the source database.
        let dst_spc = if srcrlocator.spcOid == src_tsid {
            dst_tsid
        } else {
            srcrlocator.spcOid
        };
        let dstrlocator = RelFileLocator {
            spcOid: dst_spc,
            dbOid: dst_dboid,
            relNumber: srcrlocator.relNumber,
        };

        // Acquire locks on source and target relations before copying.
        srcrelid.relId = relinfo.reloid;
        dstrelid.relId = relinfo.reloid;
        lmgr::LockRelationId(&srcrelid, AccessShareLock)?;
        lmgr::LockRelationId(&dstrelid, AccessShareLock)?;

        // Copy relation storage from source to the destination.
        storage::create_and_copy_relation_data::call(
            srcrlocator,
            dstrlocator,
            relinfo.permanent,
        )?;

        // Release the relation locks.
        lmgr::UnlockRelationId(&srcrelid, AccessShareLock)?;
        lmgr::UnlockRelationId(&dstrelid, AccessShareLock)?;
    }

    Ok(())
}

// ===========================================================================
// CreateDirAndVersionFile — C 456-540
// ===========================================================================

/// Create the database directory and write out the PG_VERSION file. If
/// `isRedo` is true, an already-existing directory / version file is tolerated.
///
/// The fd/storage half (`MakePGDirectory` + `OpenTransientFile` / `write` /
/// `pg_fsync` / `fsync_fname`, tolerating EEXIST under `isRedo`) is the fd
/// owner's adapter; the `!isRedo` WAL emission of
/// `XLOG_DBASE_CREATE_WAL_LOG` is done here.
fn CreateDirAndVersionFile(dbpath: &str, dbid: Oid, tsid: Oid, isRedo: bool) -> PgResult<()> {
    // The fd seam does the directory + PG_VERSION create/write/fsync sequence,
    // tolerating an existing dir/version file when isRedo.
    fd_seams::create_db_dir_and_version_file::call(
        dbpath, dbid, tsid, isRedo,
    )?;

    // If we are not in WAL replay then write the WAL.
    if !isRedo {
        // xl_dbase_create_wal_log_rec = { db_id: Oid, tablespace_id: Oid }.
        let mut xlrec: Vec<u8> = Vec::with_capacity(8);
        xlrec.extend_from_slice(&dbid.to_ne_bytes());
        xlrec.extend_from_slice(&tsid.to_ne_bytes());

        // START_CRIT_SECTION();
        xloginsert::XLogBeginInsert()?;
        xloginsert::XLogRegisterData(&xlrec)?;
        xloginsert::XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE_WAL_LOG)?;
        // END_CRIT_SECTION();
    }

    Ok(())
}

// ===========================================================================
// CreateDatabaseUsingFileCopy — C 550-678
// ===========================================================================

/// Create a new database using the FILE_COPY strategy. Copy each tablespace at
/// the filesystem level, logging a single WAL record per tablespace; this
/// requires a checkpoint before and after the copy.
fn CreateDatabaseUsingFileCopy(
    src_dboid: Oid,
    dst_dboid: Oid,
    src_tsid: Oid,
    dst_tsid: Oid,
) -> PgResult<()> {
    // Force a checkpoint before starting the copy (skipped in binary upgrade).
    if !tablespace_globals_seams::IsBinaryUpgrade::call()? {
        checkpointer::RequestCheckpoint(
            CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL,
        )?;
    }

    // Iterate through all tablespaces of the template database, and copy each.
    let ctx = ::mcx::MemoryContext::new("CreateDatabaseUsingFileCopy");
    let rel = tscat::tablespace_table_open::call(ctx.mcx(), AccessShareLock)?;
    let oids = tscat::scan_all_tablespace_oids::call(&rel)?;
    for srctablespace in oids {
        // No need to copy global tablespace.
        if srctablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        let srcpath = relpath::GetDatabasePath(src_dboid, srctablespace);

        match fd_seams::stat_file::call(&srcpath, true)? {
            Some(st) if st.isdir && !tablespace::directory_is_empty(&srcpath)? => {}
            _ => {
                // Assume we can ignore it.
                continue;
            }
        }

        let dsttablespace = if srctablespace == src_tsid {
            dst_tsid
        } else {
            srctablespace
        };

        let dstpath = relpath::GetDatabasePath(dst_dboid, dsttablespace);

        // Copy this subdirectory to the new location (no subdirectories).
        copydir::copydir(&srcpath, &dstpath, false)?;

        // Record the filesystem change in XLOG. xl_dbase_create_file_copy_rec =
        // { db_id, tablespace_id, src_db_id, src_tablespace_id }.
        let mut xlrec: Vec<u8> = Vec::with_capacity(16);
        xlrec.extend_from_slice(&dst_dboid.to_ne_bytes());
        xlrec.extend_from_slice(&dsttablespace.to_ne_bytes());
        xlrec.extend_from_slice(&src_dboid.to_ne_bytes());
        xlrec.extend_from_slice(&srctablespace.to_ne_bytes());
        xloginsert::XLogBeginInsert()?;
        xloginsert::XLogRegisterData(&xlrec)?;
        xloginsert::XLogInsert(
            RM_DBASE_ID,
            XLOG_DBASE_CREATE_FILE_COPY | XLR_SPECIAL_REL_UPDATE,
        )?;
    }
    tscat::tablespace_table_close::call(rel, AccessShareLock)?;

    // Force a checkpoint before committing (skipped in binary upgrade).
    if !tablespace_globals_seams::IsBinaryUpgrade::call()? {
        checkpointer::RequestCheckpoint(
            CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT,
        )?;
    }

    Ok(())
}

// ===========================================================================
// createdb — C 683-1571
// ===========================================================================

/// `CREATE DATABASE`.
pub fn createdb<'mcx>(pstate: &ParseState<'mcx>, stmt: &CreatedbStmt<'mcx>) -> PgResult<Oid> {
    let ctx = ::mcx::MemoryContext::new("createdb");
    let mcx = ctx.mcx();

    let dbname = stmt.dbname.as_deref().unwrap_or("");

    let mut dboid: Oid = InvalidOid;

    // Extract options from the statement node tree.
    let options = def_elems(&stmt.options);
    let mut tablespacename_el: Option<&DefElem> = None;
    let mut owner_el: Option<&DefElem> = None;
    let mut template_el: Option<&DefElem> = None;
    let mut encoding_el: Option<&DefElem> = None;
    let mut locale_el: Option<&DefElem> = None;
    let mut builtinlocale_el: Option<&DefElem> = None;
    let mut collate_el: Option<&DefElem> = None;
    let mut ctype_el: Option<&DefElem> = None;
    let mut iculocale_el: Option<&DefElem> = None;
    let mut icurules_el: Option<&DefElem> = None;
    let mut locprovider_el: Option<&DefElem> = None;
    let mut istemplate_el: Option<&DefElem> = None;
    let mut allowconnections_el: Option<&DefElem> = None;
    let mut connlimit_el: Option<&DefElem> = None;
    let mut collversion_el: Option<&DefElem> = None;
    let mut strategy_el: Option<&DefElem> = None;

    macro_rules! once {
        ($slot:ident, $defel:expr) => {{
            if $slot.is_some() {
                error_conflicting_def_elem($defel, pstate)?;
            }
            $slot = Some($defel);
        }};
    }

    for defel in options {
        match def_name(defel) {
            "tablespace" => once!(tablespacename_el, defel),
            "owner" => once!(owner_el, defel),
            "template" => once!(template_el, defel),
            "encoding" => once!(encoding_el, defel),
            "locale" => once!(locale_el, defel),
            "builtin_locale" => once!(builtinlocale_el, defel),
            "lc_collate" => once!(collate_el, defel),
            "lc_ctype" => once!(ctype_el, defel),
            "icu_locale" => once!(iculocale_el, defel),
            "icu_rules" => once!(icurules_el, defel),
            "locale_provider" => once!(locprovider_el, defel),
            "is_template" => once!(istemplate_el, defel),
            "allow_connections" => once!(allowconnections_el, defel),
            "connection_limit" => once!(connlimit_el, defel),
            "collation_version" => once!(collversion_el, defel),
            "location" => {
                ereport(WARNING)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("LOCATION is not supported anymore".to_string())
                    .errhint("Consider using tablespaces instead.".to_string())
                    .finish(errloc(841, "createdb"))?;
            }
            "oid" => {
                dboid = def_get_object_id(defel)?;

                // We don't normally permit new databases to be created with
                // system-assigned OIDs, except with allow_system_table_mods or
                // during a binary upgrade.
                if dboid < FirstNormalObjectId
                    && !tablespace_globals_seams::allowSystemTableMods::call()?
                    && !tablespace_globals_seams::IsBinaryUpgrade::call()?
                {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "OIDs less than {} are reserved for system objects",
                            FirstNormalObjectId
                        ))
                        .finish(errloc(869, "createdb"))
                        .unwrap_err());
                }
            }
            "strategy" => once!(strategy_el, defel),
            other => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("option \"{other}\" not recognized"))
                    .finish(errloc(880, "createdb"))
                    .unwrap_err());
            }
        }
    }

    let mut dbowner: Option<String> = None;
    let mut dbtemplate: Option<String> = None;
    let mut encoding: i32 = -1;
    let mut dbcollate: Option<String> = None;
    let mut dbctype: Option<String> = None;
    let mut dblocale: Option<String> = None;
    let mut dbicurules: Option<String> = None;
    let mut dblocprovider: i8 = b'\0' as i8;
    let mut dbistemplate = false;
    let mut dballowconnections = true;
    let mut dbconnlimit: i32 = DATCONNLIMIT_UNLIMITED;
    let mut dbcollversion: Option<String> = None;
    let mut dbstrategy = CreateDBStrategy::WalLog;

    if let Some(el) = owner_el {
        if defel_has_arg(el) {
            dbowner = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = template_el {
        if defel_has_arg(el) {
            dbtemplate = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = encoding_el {
        if defel_has_arg(el) {
            if arg_is_integer(el) {
                encoding = def_get_int32(el)?;
                let encoding_name =
                    extra_encnames_fgram::pg_encoding_to_char(encoding);
                if encoding_name.is_empty()
                    || extra_encnames_fgram::pg_valid_server_encoding(encoding_name) < 0
                {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("{encoding} is not a valid encoding code"))
                        .finish(errloc(901, "createdb"))
                        .unwrap_err());
                }
            } else {
                let encoding_name = def_get_string(el)?;
                encoding = extra_encnames_fgram::pg_valid_server_encoding(&encoding_name);
                if encoding < 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("{encoding_name} is not a valid encoding name"))
                        .finish(errloc(911, "createdb"))
                        .unwrap_err());
                }
            }
        }
    }
    if let Some(el) = locale_el {
        if defel_has_arg(el) {
            let s = def_get_string(el)?;
            dbcollate = Some(s.clone());
            dbctype = Some(s.clone());
            dblocale = Some(s);
        }
    }
    if let Some(el) = builtinlocale_el {
        if defel_has_arg(el) {
            dblocale = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = collate_el {
        if defel_has_arg(el) {
            dbcollate = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = ctype_el {
        if defel_has_arg(el) {
            dbctype = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = iculocale_el {
        if defel_has_arg(el) {
            dblocale = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = icurules_el {
        if defel_has_arg(el) {
            dbicurules = Some(def_get_string(el)?);
        }
    }
    if let Some(el) = locprovider_el {
        if defel_has_arg(el) {
            let locproviderstr = def_get_string(el)?;
            if locproviderstr.eq_ignore_ascii_case("builtin") {
                dblocprovider = COLLPROVIDER_BUILTIN;
            } else if locproviderstr.eq_ignore_ascii_case("icu") {
                dblocprovider = COLLPROVIDER_ICU;
            } else if locproviderstr.eq_ignore_ascii_case("libc") {
                dblocprovider = COLLPROVIDER_LIBC;
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!("unrecognized locale provider: {locproviderstr}"))
                    .finish(errloc(945, "createdb"))
                    .unwrap_err());
            }
        }
    }
    if let Some(el) = istemplate_el {
        if defel_has_arg(el) {
            dbistemplate = def_get_boolean(el)?;
        }
    }
    if let Some(el) = allowconnections_el {
        if defel_has_arg(el) {
            dballowconnections = def_get_boolean(el)?;
        }
    }
    if let Some(el) = connlimit_el {
        if defel_has_arg(el) {
            dbconnlimit = def_get_int32(el)?;
            if dbconnlimit < DATCONNLIMIT_UNLIMITED {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid connection limit: {dbconnlimit}"))
                    .finish(errloc(957, "createdb"))
                    .unwrap_err());
            }
        }
    }
    if let Some(el) = collversion_el {
        dbcollversion = Some(def_get_string(el)?);
    }

    // Obtain OID of proposed owner.
    let datdba = match &dbowner {
        Some(owner) => adt_acl::role_membership::get_role_oid(owner, false)?,
        None => miscinit::GetUserId(),
    };

    // To create a database, must have createdb privilege and be able to become
    // the target role.
    if !have_createdb_privilege(mcx)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to create database".to_string())
            .finish(errloc(978, "createdb"))
            .unwrap_err());
    }

    adt_acl::role_membership::check_can_set_role(
        miscinit::GetUserId(),
        datdba,
    )?;

    // Lookup database (template) to be cloned, and obtain share lock on it.
    let dbtemplate_name = dbtemplate.clone().unwrap_or_else(|| "template1".to_string());

    let src = get_db_info(mcx, &dbtemplate_name, ShareLock)?;
    let src = match src {
        Some(s) => s,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!(
                    "template database \"{dbtemplate_name}\" does not exist"
                ))
                .finish(errloc(1001, "createdb"))
                .unwrap_err());
        }
    };
    let src_dboid = src.oid;
    let src_encoding = src.encoding;
    let src_istemplate = src.datistemplate;
    let src_hasloginevt = src.dathasloginevt;
    let src_frozenxid = src.datfrozenxid;
    let src_minmxid = src.datminmxid;
    let src_deftablespace = src.dattablespace;
    let src_collate = src.datcollate.as_str().to_string();
    let src_ctype = src.datctype.as_str().to_string();
    let src_locprovider = src.datlocprovider;
    let src_locale: Option<String> = src.datlocale.as_ref().map(|s| s.as_str().to_string());
    let src_icurules: Option<String> = src.daticurules.as_ref().map(|s| s.as_str().to_string());
    let src_collversion: Option<String> =
        src.datcollversion.as_ref().map(|s| s.as_str().to_string());

    // If the source database was in the process of being dropped, we can't use
    // it as a template.
    if database_is_invalid_oid(mcx, src_dboid)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot use invalid database \"{dbtemplate_name}\" as template"
            ))
            .errhint("Use DROP DATABASE to drop invalid databases.".to_string())
            .finish(errloc(1013, "createdb"))
            .unwrap_err());
    }

    // To copy a DB that's not marked datistemplate, must be superuser/owner.
    if !src_istemplate
        && !aclchk::object_ownercheck(
            mcx,
            DatabaseRelationId,
            src_dboid,
            miscinit::GetUserId(),
        )?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied to copy database \"{dbtemplate_name}\""))
            .finish(errloc(1023, "createdb"))
            .unwrap_err());
    }

    // Validate the database creation strategy.
    if let Some(el) = strategy_el {
        if defel_has_arg(el) {
            let strategy = def_get_string(el)?;
            if strategy.eq_ignore_ascii_case("wal_log") {
                dbstrategy = CreateDBStrategy::WalLog;
            } else if strategy.eq_ignore_ascii_case("file_copy") {
                dbstrategy = CreateDBStrategy::FileCopy;
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid create database strategy \"{strategy}\""))
                    .errhint("Valid strategies are \"wal_log\" and \"file_copy\".".to_string())
                    .finish(errloc(1043, "createdb"))
                    .unwrap_err());
            }
        }
    }

    // If encoding or locales are defaulted, use source's setting.
    if encoding < 0 {
        encoding = src_encoding;
    }
    if dbcollate.is_none() {
        dbcollate = Some(src_collate.clone());
    }
    if dbctype.is_none() {
        dbctype = Some(src_ctype.clone());
    }
    if dblocprovider == b'\0' as i8 {
        dblocprovider = src_locprovider;
    }
    if dblocale.is_none() && dblocprovider == src_locprovider {
        dblocale = src_locale.clone();
    }
    if dbicurules.is_none() {
        dbicurules = src_icurules.clone();
    }

    let mut dbcollate = dbcollate.unwrap_or_default();
    let mut dbctype = dbctype.unwrap_or_default();

    // Some encodings are client only.
    if !pg_valid_be_encoding(encoding) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("invalid server encoding {encoding}"))
            .finish(errloc(1064, "createdb"))
            .unwrap_err());
    }

    // Check that the chosen locales are valid, and get canonical spellings.
    let (ok, canon) =
        pg_locale::setup::check_locale(libc::LC_COLLATE, &dbcollate)?;
    if !ok {
        return Err(invalid_locale_err("LC_COLLATE", &dbcollate, dblocprovider, 1067));
    }
    dbcollate = canon.unwrap_or(dbcollate);

    let (ok, canon) =
        pg_locale::setup::check_locale(libc::LC_CTYPE, &dbctype)?;
    if !ok {
        return Err(invalid_locale_err("LC_CTYPE", &dbctype, dblocprovider, 1085));
    }
    dbctype = canon.unwrap_or(dbctype);

    check_encoding_locale_matches(mcx, encoding, &dbcollate, &dbctype)?;

    // Validate provider-specific parameters.
    if dblocprovider != COLLPROVIDER_BUILTIN && builtinlocale_el.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(
                "BUILTIN_LOCALE cannot be specified unless locale provider is builtin"
                    .to_string(),
            )
            .finish(errloc(1113, "createdb"))
            .unwrap_err());
    }

    if dblocprovider != COLLPROVIDER_ICU {
        if iculocale_el.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("ICU locale cannot be specified unless locale provider is ICU".to_string())
                .finish(errloc(1121, "createdb"))
                .unwrap_err());
        }
        if dbicurules.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("ICU rules cannot be specified unless locale provider is ICU".to_string())
                .finish(errloc(1126, "createdb"))
                .unwrap_err());
        }
    }

    // Validate and canonicalize locale for the provider.
    if dblocprovider == COLLPROVIDER_BUILTIN {
        let loc = match &dblocale {
            Some(l) => l.clone(),
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("LOCALE or BUILTIN_LOCALE must be specified".to_string())
                    .finish(errloc(1139, "createdb"))
                    .unwrap_err());
            }
        };
        dblocale = Some(
            coll::builtin_validate_locale::call(mcx, encoding, &loc)?
                .as_str()
                .to_string(),
        );
    } else if dblocprovider == COLLPROVIDER_ICU {
        if !coll::is_encoding_supported_by_icu::call(encoding)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "encoding \"{}\" is not supported with ICU provider",
                    extra_encnames_fgram::pg_encoding_to_char(encoding)
                ))
                .finish(errloc(1148, "createdb"))
                .unwrap_err());
        }

        let loc = match &dblocale {
            Some(l) => l.clone(),
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("LOCALE or ICU_LOCALE must be specified".to_string())
                    .finish(errloc(1157, "createdb"))
                    .unwrap_err());
            }
        };

        // During binary upgrade, or when the locale came from the template
        // database, preserve locale string; otherwise canonicalize.
        let mut loc = loc;
        if !tablespace_globals_seams::IsBinaryUpgrade::call()?
            && Some(&loc) != src_locale.as_ref()
        {
            let level = coll::icu_validation_level::call()?;
            if let Some(langtag) = coll::icu_language_tag::call(mcx, &loc, level)? {
                let langtag = langtag.as_str().to_string();
                if loc != langtag {
                    ereport(NOTICE)
                        .errmsg(format!(
                            "using standard form \"{langtag}\" for ICU locale \"{loc}\""
                        ))
                        .finish(errloc(1172, "createdb"))?;
                    loc = langtag;
                }
            }
        }
        coll::icu_validate_locale::call(&loc)?;
        dblocale = Some(loc);
    }

    // For libc, locale comes from datcollate and datctype.
    if dblocprovider == COLLPROVIDER_LIBC {
        dblocale = None;
    }

    // Check that the new encoding and locale settings match the source database
    // (unless the template is template0).
    if dbtemplate_name != "template0" {
        if encoding != src_encoding {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new encoding ({}) is incompatible with the encoding of the template database ({})",
                    extra_encnames_fgram::pg_encoding_to_char(encoding),
                    extra_encnames_fgram::pg_encoding_to_char(src_encoding)
                ))
                .errhint(
                    "Use the same encoding as in the template database, or use template0 as template."
                        .to_string(),
                )
                .finish(errloc(1200, "createdb"))
                .unwrap_err());
        }
        if dbcollate != src_collate {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new collation ({dbcollate}) is incompatible with the collation of the template database ({src_collate})"
                ))
                .errhint(
                    "Use the same collation as in the template database, or use template0 as template."
                        .to_string(),
                )
                .finish(errloc(1208, "createdb"))
                .unwrap_err());
        }
        if dbctype != src_ctype {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new LC_CTYPE ({dbctype}) is incompatible with the LC_CTYPE of the template database ({src_ctype})"
                ))
                .errhint(
                    "Use the same LC_CTYPE as in the template database, or use template0 as template."
                        .to_string(),
                )
                .finish(errloc(1215, "createdb"))
                .unwrap_err());
        }
        if dblocprovider != src_locprovider {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new locale provider ({}) does not match locale provider of the template database ({})",
                    collprovider_name(dblocprovider),
                    collprovider_name(src_locprovider)
                ))
                .errhint(
                    "Use the same locale provider as in the template database, or use template0 as template."
                        .to_string(),
                )
                .finish(errloc(1222, "createdb"))
                .unwrap_err());
        }
        if dblocprovider == COLLPROVIDER_ICU {
            // Assert(dblocale); Assert(src_locale);
            let dl = dblocale.clone().unwrap_or_default();
            let sl = src_locale.clone().unwrap_or_default();
            if dl != sl {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "new ICU locale ({dl}) is incompatible with the ICU locale of the template database ({sl})"
                    ))
                    .errhint(
                        "Use the same ICU locale as in the template database, or use template0 as template."
                            .to_string(),
                    )
                    .finish(errloc(1236, "createdb"))
                    .unwrap_err());
            }

            let val1 = dbicurules.clone().unwrap_or_default();
            let val2 = src_icurules.clone().unwrap_or_default();
            if val1 != val2 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "new ICU collation rules ({val1}) are incompatible with the ICU collation rules of the template database ({val2})"
                    ))
                    .errhint(
                        "Use the same ICU collation rules as in the template database, or use template0 as template."
                            .to_string(),
                    )
                    .finish(errloc(1249, "createdb"))
                    .unwrap_err());
            }
        }
    }

    // If we got a collation version for the template database, check that it
    // matches the actual OS collation version (unless explicitly specified).
    if src_collversion.is_some() && collversion_el.is_none() {
        let locale = if dblocprovider == COLLPROVIDER_LIBC {
            dbcollate.clone()
        } else {
            dblocale.clone().unwrap_or_default()
        };

        let actual_versionstr = coll_actual_version(mcx, dblocprovider, &locale)?;
        let actual_versionstr = match actual_versionstr {
            Some(v) => v,
            None => {
                return Err(ereport(ERROR)
                    .errmsg(format!(
                        "template database \"{dbtemplate_name}\" has a collation version, but no actual collation version could be determined"
                    ))
                    .finish(errloc(1282, "createdb"))
                    .unwrap_err());
            }
        };

        let scv = src_collversion.clone().unwrap_or_default();
        if actual_versionstr != scv {
            let quoted = ruleutils_seams::quote_identifier::call(
                mcx,
                &dbtemplate_name,
            )?;
            return Err(ereport(ERROR)
                .errmsg(format!(
                    "template database \"{dbtemplate_name}\" has a collation version mismatch"
                ))
                .errdetail(format!(
                    "The template database was created using collation version {scv}, but the operating system provides version {actual_versionstr}."
                ))
                .errhint(format!(
                    "Rebuild all objects in the template database that use the default collation and run ALTER DATABASE {} REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.",
                    quoted.as_str()
                ))
                .finish(errloc(1286, "createdb"))
                .unwrap_err());
        }
    }

    if dbcollversion.is_none() {
        dbcollversion = src_collversion.clone();
    }

    // Last resort: derive from the actual OS collation version (template0).
    if dbcollversion.is_none() {
        let locale = if dblocprovider == COLLPROVIDER_LIBC {
            dbcollate.clone()
        } else {
            dblocale.clone().unwrap_or_default()
        };
        dbcollversion = coll_actual_version(mcx, dblocprovider, &locale)?;
    }

    // Resolve default tablespace for new database.
    let dst_deftablespace;
    if let Some(el) = tablespacename_el {
        if defel_has_arg(el) {
            let tablespacename = def_get_string(el)?;
            let resolved =
                tablespace::get_tablespace_oid(mcx, &tablespacename, false)?;
            // Check permissions.
            let aclresult = aclchk::object_aclcheck(
                mcx,
                TableSpaceRelationId,
                resolved,
                miscinit::GetUserId(),
                ACL_CREATE,
            )?;
            if aclresult != AclResult::AclcheckOk {
                aclchk::aclcheck_error(
                    aclresult,
                    OBJECT_TABLESPACE,
                    Some(tablespacename.clone()),
                )?;
            }

            // pg_global must never be the default tablespace.
            if resolved == GLOBALTABLESPACE_OID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("pg_global cannot be used as default tablespace".to_string())
                    .finish(errloc(1337, "createdb"))
                    .unwrap_err());
            }

            // If changing the template's default tablespace, the template must
            // not have any files there.
            if resolved != src_deftablespace {
                let srcpath = relpath::GetDatabasePath(src_dboid, resolved);
                if let Some(st) = fd_seams::stat_file::call(&srcpath, true)? {
                    if st.isdir && !tablespace::directory_is_empty(&srcpath)? {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                            .errmsg(format!("cannot assign new default tablespace \"{tablespacename}\""))
                            .errdetail(format!(
                                "There is a conflict because database \"{dbtemplate_name}\" already has some tables in this tablespace."
                            ))
                            .finish(errloc(1361, "createdb"))
                            .unwrap_err());
                    }
                }
            }
            dst_deftablespace = resolved;
        } else {
            dst_deftablespace = src_deftablespace;
        }
    } else {
        // Use template database's default tablespace.
        dst_deftablespace = src_deftablespace;
    }

    // Check for db name conflict (friendlier than a unique-index violation).
    if OidIsValid(get_database_oid(dbname, true)?) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_DATABASE)
            .errmsg(format!("database \"{dbname}\" already exists"))
            .finish(errloc(1394, "createdb"))
            .unwrap_err());
    }

    // The source DB can't have any active backends, except this one.
    let (busy, notherbackends, npreparedxacts) =
        procarray::visibility_lookup::CountOtherDBBackends(src_dboid)?;
    if busy {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "source database \"{dbtemplate_name}\" is being accessed by other users"
            ))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .finish(errloc(1407, "createdb"))
            .unwrap_err());
    }

    // Select an OID for the new database.
    let pg_database_rel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;

    if OidIsValid(dboid) {
        let existing_dbname = get_database_name(mcx, dboid)?;
        if let Some(name) = existing_dbname {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "database OID {dboid} is already in use by database \"{}\"",
                    name.as_str()
                ))
                .finish(errloc(1431, "createdb"))
                .unwrap_err());
        }
        if check_db_file_conflict(mcx, dboid)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "data directory with the specified OID {dboid} already exists"
                ))
                .finish(errloc(1437, "createdb"))
                .unwrap_err());
        }
    } else {
        // Select an OID for the new database if not explicitly configured.
        loop {
            dboid = catalog_catalog::GetNewOidWithIndex(
                &pg_database_rel,
                DatabaseOidIndexId,
                Anum_pg_database_oid as i16,
            )?;
            if !check_db_file_conflict(mcx, dboid)? {
                break;
            }
        }
    }

    // Assert((dblocprovider != LIBC && dblocale) || (LIBC && !dblocale)).

    // Form and insert the new pg_database row. The per-column tuple build +
    // CatalogTupleInsert is the pg_database owner's mutate seam.
    let record = NewDbRecord {
        oid: dboid,
        datname: pgstr(mcx, dbname)?,
        datdba: datdba,
        encoding,
        datlocprovider: dblocprovider,
        datistemplate: dbistemplate,
        datallowconn: dballowconnections,
        dathasloginevt: src_hasloginevt,
        datconnlimit: dbconnlimit,
        datfrozenxid: src_frozenxid,
        datminmxid: src_minmxid,
        dattablespace: dst_deftablespace,
        datcollate: pgstr(mcx, &dbcollate)?,
        datctype: pgstr(mcx, &dbctype)?,
        datlocale: pgstr_opt(mcx, dblocale.as_deref())?,
        daticurules: pgstr_opt(mcx, dbicurules.as_deref())?,
        datcollversion: pgstr_opt(mcx, dbcollversion.as_deref())?,
    };
    dbcat::insert_pg_database::call(mcx, &pg_database_rel, &record)?;

    // Register owner dependency.
    pg_shdepend::recordDependencyOnOwner(DatabaseRelationId, dboid, datdba)?;

    // Create pg_shdepend entries for objects within database.
    pg_shdepend::copyTemplateDependencies(src_dboid, dboid)?;

    // Post creation hook for new database.
    objectaccess::invoke_object_post_create_hook(
        DatabaseRelationId,
        dboid,
        0,
        false,
    )?;

    // If WAL_LOG, lock the to-be-created database (createdb_failure_callback
    // expects this lock to be held already).
    if dbstrategy == CreateDBStrategy::WalLog {
        lmgr_seams::lock_shared_object::call(
            DatabaseRelationId,
            dboid,
            0,
            AccessShareLock,
        )?
        .keep();
    }

    // ENSURE block: clean up on failure (createdb_failure_callback).
    let body = (|| -> PgResult<()> {
        match dbstrategy {
            CreateDBStrategy::WalLog => {
                CreateDatabaseUsingWalLog(src_dboid, dboid, src_deftablespace, dst_deftablespace)?;
            }
            CreateDBStrategy::FileCopy => {
                CreateDatabaseUsingFileCopy(
                    src_dboid,
                    dboid,
                    src_deftablespace,
                    dst_deftablespace,
                )?;
            }
        }

        // Close pg_database, but keep lock till commit.
        table_close(pg_database_rel.alias(), NoLock)?;

        // Force synchronous commit.
        transam_xact::ForceSyncCommit();
        Ok(())
    })();

    if let Err(e) = body {
        createdb_failure_callback(mcx, src_dboid, dboid, dbstrategy)?;
        return Err(e);
    }

    Ok(dboid)
}

/// The four LC_COLLATE/LC_CTYPE `check_locale` ereports (createdb), keyed by the
/// provider so the right hint is emitted.
fn invalid_locale_err(
    which: &str,
    locname: &str,
    provider: i8,
    line: i32,
) -> ::types_error::PgError {
    let mut b = ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("invalid {which} locale name: \"{locname}\""));
    if provider == COLLPROVIDER_BUILTIN {
        b = b.errhint(
            "If the locale name is specific to the builtin provider, use BUILTIN_LOCALE."
                .to_string(),
        );
    } else if provider == COLLPROVIDER_ICU {
        b = b.errhint(
            "If the locale name is specific to the ICU provider, use ICU_LOCALE.".to_string(),
        );
    }
    b.finish(errloc(line, "createdb")).unwrap_err()
}

/// `get_collation_actual_version(provider, locale)` (pg_locale.c) — returns an
/// owned `Option<String>` so it can escape the transient context.
fn coll_actual_version(mcx: Mcx<'_>, provider: i8, locale: &str) -> PgResult<Option<String>> {
    Ok(
        pg_locale_seams::get_collation_actual_version::call(
            mcx, provider, locale,
        )?
        .map(|s| s.as_str().to_string()),
    )
}

// ===========================================================================
// check_encoding_locale_matches — C 1595-1630
// ===========================================================================

/// Check whether the chosen encoding matches the chosen locale settings.
pub fn check_encoding_locale_matches(
    mcx: Mcx<'_>,
    encoding: i32,
    collate: &str,
    ctype: &str,
) -> PgResult<()> {
    let _ = mcx;
    let ctype_encoding =
        pg_locale_env_seams::pg_get_encoding_from_locale::call(ctype)?;
    let collate_encoding =
        pg_locale_env_seams::pg_get_encoding_from_locale::call(collate)?;

    let superuser = superuser_seams::superuser::call()?;

    if !(ctype_encoding == encoding
        || ctype_encoding == PG_SQL_ASCII
        || ctype_encoding == -1
        || (encoding == PG_SQL_ASCII && superuser))
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "encoding \"{}\" does not match locale \"{ctype}\"",
                extra_encnames_fgram::pg_encoding_to_char(encoding)
            ))
            .errdetail(format!(
                "The chosen LC_CTYPE setting requires encoding \"{}\".",
                extra_encnames_fgram::pg_encoding_to_char(ctype_encoding)
            ))
            .finish(errloc(1608, "check_encoding_locale_matches"))
            .unwrap_err());
    }

    if !(collate_encoding == encoding
        || collate_encoding == PG_SQL_ASCII
        || collate_encoding == -1
        || (encoding == PG_SQL_ASCII && superuser))
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "encoding \"{}\" does not match locale \"{collate}\"",
                extra_encnames_fgram::pg_encoding_to_char(encoding)
            ))
            .errdetail(format!(
                "The chosen LC_COLLATE setting requires encoding \"{}\".",
                extra_encnames_fgram::pg_encoding_to_char(collate_encoding)
            ))
            .finish(errloc(1623, "check_encoding_locale_matches"))
            .unwrap_err());
    }

    Ok(())
}

// ===========================================================================
// createdb_failure_callback — C 1633-1666
// ===========================================================================

/// Error cleanup callback for createdb. Re-signed from `(int, Datum)` to the
/// unpacked `createdb_failure_params`.
fn createdb_failure_callback(
    mcx: Mcx<'_>,
    src_dboid: Oid,
    dest_dboid: Oid,
    strategy: CreateDBStrategy,
) -> PgResult<()> {
    // If WAL_LOG, drop the target db's shared buffers + pending fsyncs and
    // release the target db lock.
    if strategy == CreateDBStrategy::WalLog {
        bufmgr_seams::drop_database_buffers::call(dest_dboid)?;
        md::ForgetDatabaseSyncRequests(dest_dboid)?;

        lmgr_seams::unlock_shared_object::call(
            DatabaseRelationId,
            dest_dboid,
            0,
            AccessShareLock,
        )?;
    }

    // Release lock on source database before recursive remove.
    lmgr_seams::unlock_shared_object::call(
        DatabaseRelationId,
        src_dboid,
        0,
        ShareLock,
    )?;

    // Throw away any successfully copied subdirectories.
    remove_dbtablespaces(mcx, dest_dboid)
}

// ===========================================================================
// dropdb — C 1672-1895
// ===========================================================================

/// `DROP DATABASE`.
pub fn dropdb(mcx: Mcx<'_>, dbname: &str, missing_ok: bool, force: bool) -> PgResult<()> {
    // Look up the target database's OID + get AccessExclusiveLock.
    let pgdbrel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;

    let info = get_db_info(mcx, dbname, AccessExclusiveLock)?;
    let dbform = match info {
        Some(f) => f,
        None => {
            if !missing_ok {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{dbname}\" does not exist"))
                    .finish(errloc(1703, "dropdb"))
                    .unwrap_err());
            }
            // Close pg_database, release the lock, since we changed nothing.
            table_close(pgdbrel, RowExclusiveLock)?;
            ereport(NOTICE)
                .errmsg(format!("database \"{dbname}\" does not exist, skipping"))
                .finish(errloc(1711, "dropdb"))?;
            return Ok(());
        }
    };
    let db_id = dbform.oid;
    let db_istemplate = dbform.datistemplate;

    // Permission checks.
    if !aclchk::object_ownercheck(
        mcx,
        DatabaseRelationId,
        db_id,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_DATABASE,
            Some(dbname.to_string()),
        )?;
    }

    // DROP hook.
    objectaccess::invoke_object_drop_hook(DatabaseRelationId, db_id, 0, 0)?;

    // Disallow dropping a template database.
    if db_istemplate {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot drop a template database".to_string())
            .finish(errloc(1734, "dropdb"))
            .unwrap_err());
    }

    // Can't drop my own database.
    if db_id == tablespace_globals_seams::MyDatabaseId::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg("cannot drop the currently open database".to_string())
            .finish(errloc(1740, "dropdb"))
            .unwrap_err());
    }

    // Active logical slots referring to the database?
    let (_timed, _nslots, nslots_active) = slot::replication_slots_count_db_slots::call(db_id)?;
    if nslots_active != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "database \"{dbname}\" is used by an active logical replication slot"
            ))
            .errdetail(if nslots_active == 1 {
                "There is 1 active slot.".to_string()
            } else {
                format!("There are {nslots_active} active slots.")
            })
            .finish(errloc(1752, "dropdb"))
            .unwrap_err());
    }

    // Subscriptions defined in the target database?
    let nsubscriptions =
        pg_subscription_seams::count_db_subscriptions::call(db_id)?;
    if nsubscriptions > 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "database \"{dbname}\" is being used by logical replication subscription"
            ))
            .errdetail(if nsubscriptions == 1 {
                "There is 1 subscription.".to_string()
            } else {
                format!("There are {nsubscriptions} subscriptions.")
            })
            .finish(errloc(1768, "dropdb"))
            .unwrap_err());
    }

    // Terminate existing connections if forced.
    if force {
        procarray::visibility_lookup::TerminateOtherDBBackends(mcx, db_id)?;
    }

    // Check for other backends in the target database.
    let (busy, notherbackends, npreparedxacts) =
        procarray::visibility_lookup::CountOtherDBBackends(db_id)?;
    if busy {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!("database \"{dbname}\" is being accessed by other users"))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .finish(errloc(1791, "dropdb"))
            .unwrap_err());
    }

    // Delete comments / security labels.
    comment::DeleteSharedComments(db_id, DatabaseRelationId)?;
    seclabel::DeleteSharedSecurityLabel(mcx, db_id, DatabaseRelationId)?;

    // Remove settings.
    pg_db_role_setting::DropSetting(mcx, db_id, InvalidOid)?;

    // Remove shared dependency references.
    pg_shdepend::dropDatabaseDependencies(db_id)?;

    // Tell the cumulative stats system to forget it.
    pgstat::pgstat_drop_database::call(db_id)?;

    // Mark the database invalid via an in-place update, then delete the row
    // transactionally. The in-place invalidate + XLogFlush is the pg_database
    // owner's seam; the delete is a second seam.
    let tid = match dbcat::set_pg_database_invalid_inplace::call(mcx, &pgdbrel, dbname)? {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for database {db_id}"))
                .finish(errloc(1836, "dropdb"))
                .unwrap_err());
        }
    };
    dbcat::delete_pg_database::call(mcx, &pgdbrel, tid)?;

    // Drop db-specific replication slots.
    slot::replication_slots_drop_db_slots::call(
        db_id,
        miscinit_seams::my_proc_pid::call(),
    )?;

    // Drop pages for this database in the shared buffer cache.
    bufmgr_seams::drop_database_buffers::call(db_id)?;

    // Tell checkpointer to forget pending fsync/unlink requests.
    md::ForgetDatabaseSyncRequests(db_id)?;

    // Force a checkpoint to ensure the message is received.
    checkpointer::RequestCheckpoint(
        CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT,
    )?;

    // Close all smgr fds in all backends.
    let gen = procsignal::EmitProcSignalBarrier(
        ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
    );
    procsignal::WaitForProcSignalBarrier(gen)?;

    // Remove all tablespace subdirs belonging to the database.
    remove_dbtablespaces(mcx, db_id)?;

    // Close pg_database, but keep lock till commit.
    table_close(pgdbrel, NoLock)?;

    // Force synchronous commit.
    transam_xact::ForceSyncCommit();

    Ok(())
}

// ===========================================================================
// RenameDatabase — C 1901-1996
// ===========================================================================

/// Rename a database.
pub fn RenameDatabase(mcx: Mcx<'_>, oldname: &str, newname: &str) -> PgResult<ObjectAddress> {
    let rel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;

    let info = get_db_info(mcx, oldname, AccessExclusiveLock)?;
    let dbform = match info {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{oldname}\" does not exist"))
                .finish(errloc(1922, "RenameDatabase"))
                .unwrap_err());
        }
    };
    let db_id = dbform.oid;

    // Must be owner.
    if !aclchk::object_ownercheck(
        mcx,
        DatabaseRelationId,
        db_id,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_DATABASE,
            Some(oldname.to_string()),
        )?;
    }

    // Must have createdb rights.
    if !have_createdb_privilege(mcx)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to rename database".to_string())
            .finish(errloc(1933, "RenameDatabase"))
            .unwrap_err());
    }

    // Make sure the new name doesn't exist.
    if OidIsValid(get_database_oid(newname, true)?) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_DATABASE)
            .errmsg(format!("database \"{newname}\" already exists"))
            .finish(errloc(1951, "RenameDatabase"))
            .unwrap_err());
    }

    // Can't rename the current database.
    if db_id == tablespace_globals_seams::MyDatabaseId::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("current database cannot be renamed".to_string())
            .finish(errloc(1962, "RenameDatabase"))
            .unwrap_err());
    }

    // No active sessions.
    let (busy, notherbackends, npreparedxacts) =
        procarray::visibility_lookup::CountOtherDBBackends(db_id)?;
    if busy {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!("database \"{oldname}\" is being accessed by other users"))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .finish(errloc(1975, "RenameDatabase"))
            .unwrap_err());
    }

    // Rename: take the inplace-update tuple lock, set datname, CatalogTupleUpdate
    // + UnlockTuple. The owner's scan-locked + update seams cover the
    // SearchSysCacheLockedCopy1 / CatalogTupleUpdate / UnlockTuple sequence.
    let my_db = tablespace_globals_seams::MyDatabaseId::call()?;
    let locked = dbcat::scan_pg_database_locked_for_update::call(
        mcx, &rel, my_db, true, db_id, "",
    )?;
    let (otid, mut newform) = match locked {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for database {db_id}"))
                .finish(errloc(1980, "RenameDatabase"))
                .unwrap_err());
        }
    };
    newform.datname = pgstr(mcx, newname)?;
    dbcat::update_pg_database::call(mcx, &rel, otid, &newform)?;

    objectaccess::invoke_object_post_alter_hook(
        DatabaseRelationId,
        db_id,
        0,
        InvalidOid,
        false,
    )?;

    // Close pg_database, but keep lock till commit.
    table_close(rel, NoLock)?;

    Ok(ObjectAddress {
        classId: DatabaseRelationId,
        objectId: db_id,
        objectSubId: 0,
    })
}

// ===========================================================================
// movedb — C 2002-2321 ; movedb_failure_callback — C 2324-2336
// ===========================================================================

/// `ALTER DATABASE SET TABLESPACE`.
fn movedb(mcx: Mcx<'_>, dbname: &str, tblspcname: &str) -> PgResult<()> {
    let pgdbrel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;

    let info = get_db_info(mcx, dbname, AccessExclusiveLock)?;
    let dbform = match info {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{dbname}\" does not exist"))
                .finish(errloc(2034, "movedb"))
                .unwrap_err());
        }
    };
    let db_id = dbform.oid;
    let src_tblspcoid = dbform.dattablespace;

    // Session lock (persists across the commit/restart below).
    let db_relid = LockRelId {
        relId: db_id,
        dbId: InvalidOid,
    };
    let _ = &db_relid;
    lmgr_seams::lock_shared_object_for_session::call(
        DatabaseRelationId,
        db_id,
        0,
        AccessExclusiveLock,
    )?;

    // Permission checks.
    if !aclchk::object_ownercheck(
        mcx,
        DatabaseRelationId,
        db_id,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_DATABASE,
            Some(dbname.to_string()),
        )?;
    }

    // Can't move my own database.
    if db_id == tablespace_globals_seams::MyDatabaseId::call()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg("cannot change the tablespace of the currently open database".to_string())
            .finish(errloc(2057, "movedb"))
            .unwrap_err());
    }

    // Get tablespace OID.
    let dst_tblspcoid =
        tablespace::get_tablespace_oid(mcx, tblspcname, false)?;

    // Permission checks.
    let aclresult = aclchk::object_aclcheck(
        mcx,
        TableSpaceRelationId,
        dst_tblspcoid,
        miscinit::GetUserId(),
        ACL_CREATE,
    )?;
    if aclresult != AclResult::AclcheckOk {
        aclchk::aclcheck_error(
            aclresult,
            OBJECT_TABLESPACE,
            Some(tblspcname.to_string()),
        )?;
    }

    // pg_global must never be the default tablespace.
    if dst_tblspcoid == GLOBALTABLESPACE_OID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("pg_global cannot be used as default tablespace".to_string())
            .finish(errloc(2079, "movedb"))
            .unwrap_err());
    }

    // No-op if same tablespace.
    if src_tblspcoid == dst_tblspcoid {
        table_close(pgdbrel, NoLock)?;
        lmgr_seams::unlock_shared_object_for_session::call(
            DatabaseRelationId,
            db_id,
            0,
            AccessExclusiveLock,
        )?;
        return Ok(());
    }

    // Other backends?
    let (busy, notherbackends, npreparedxacts) =
        procarray::visibility_lookup::CountOtherDBBackends(db_id)?;
    if busy {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!("database \"{dbname}\" is being accessed by other users"))
            .errdetail(errdetail_busy_db(notherbackends, npreparedxacts))
            .finish(errloc(2104, "movedb"))
            .unwrap_err());
    }

    // Old and new database paths.
    let src_dbpath = relpath::GetDatabasePath(db_id, src_tblspcoid);
    let dst_dbpath = relpath::GetDatabasePath(db_id, dst_tblspcoid);

    // Force a checkpoint before proceeding.
    checkpointer::RequestCheckpoint(
        CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL,
    )?;

    // Close all smgr fds in all backends.
    let gen = procsignal::EmitProcSignalBarrier(
        ProcSignalBarrierType::PROCSIGNAL_BARRIER_SMGRRELEASE,
    );
    procsignal::WaitForProcSignalBarrier(gen)?;

    // Drop all buffers holding data of the target database.
    bufmgr_seams::drop_database_buffers::call(db_id)?;

    // Check for existing files in the target directory.
    let dstdir = fd::allocated_desc::AllocateDir(&dst_dbpath)?;
    if dstdir.is_some() {
        while let Some(ent) = fd::allocated_desc::ReadDir(dstdir, &dst_dbpath)? {
            if ent.d_name == "." || ent.d_name == ".." {
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
                .finish(errloc(2162, "movedb"))
                .unwrap_err());
        }
        fd::allocated_desc::FreeDir(dstdir)?;

        // The directory exists but is empty; remove it before copydir.
        if let Err(errno) = fd_seams::rmdir::call(&dst_dbpath) {
            return Err(ereport(ERROR)
                .errmsg(format!("could not remove directory \"{dst_dbpath}\": {errno}"))
                .finish(errloc(2176, "movedb"))
                .unwrap_err());
        }
    }

    // ENSURE block: clean up the debris if the copy fails.
    let body = (|| -> PgResult<()> {
        // Copy files from the old tablespace to the new one.
        copydir::copydir(&src_dbpath, &dst_dbpath, false)?;

        // Record the filesystem change in XLOG (CREATE_FILE_COPY).
        // xl_dbase_create_file_copy_rec = { db_id, tablespace_id, src_db_id,
        // src_tablespace_id }.
        let mut xlrec: Vec<u8> = Vec::with_capacity(16);
        xlrec.extend_from_slice(&db_id.to_ne_bytes());
        xlrec.extend_from_slice(&dst_tblspcoid.to_ne_bytes());
        xlrec.extend_from_slice(&db_id.to_ne_bytes());
        xlrec.extend_from_slice(&src_tblspcoid.to_ne_bytes());
        xloginsert::XLogBeginInsert()?;
        xloginsert::XLogRegisterData(&xlrec)?;
        xloginsert::XLogInsert(
            RM_DBASE_ID,
            XLOG_DBASE_CREATE_FILE_COPY | XLR_SPECIAL_REL_UPDATE,
        )?;

        // Update the database's pg_database tuple (dattablespace).
        let my_db = tablespace_globals_seams::MyDatabaseId::call()?;
        let locked = dbcat::scan_pg_database_locked_for_update::call(
            mcx, &pgdbrel, my_db, false, InvalidOid, dbname,
        )?;
        let (otid, mut form) = match locked {
            Some(t) => t,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{dbname}\" does not exist"))
                    .finish(errloc(2232, "movedb"))
                    .unwrap_err());
            }
        };
        form.dattablespace = dst_tblspcoid;
        dbcat::update_pg_database::call(mcx, &pgdbrel, otid, &form)?;

        objectaccess::invoke_object_post_alter_hook(
            DatabaseRelationId,
            db_id,
            0,
            InvalidOid,
            false,
        )?;

        // Force another checkpoint.
        checkpointer::RequestCheckpoint(
            CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT,
        )?;

        // Force synchronous commit.
        transam_xact::ForceSyncCommit();

        // Close pg_database, but keep lock till commit.
        table_close(pgdbrel, NoLock)?;
        Ok(())
    })();

    if let Err(e) = body {
        movedb_failure_callback(db_id, dst_tblspcoid)?;
        return Err(e);
    }

    // Commit the transaction so the pg_database update is committed.
    snapmgr::PopActiveSnapshot()?;
    transam_xact::CommitTransactionCommand()?;

    // Start a new transaction for the remaining work.
    transam_xact::StartTransactionCommand()?;

    // Remove files from the old tablespace.
    if !fd_seams::rmtree::call(&src_dbpath, true) {
        ereport(WARNING)
            .errmsg(format!(
                "some useless files may be left behind in old database directory \"{src_dbpath}\""
            ))
            .finish(errloc(2295, "movedb"))?;
    }

    // Record the filesystem change in XLOG (DROP).
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&db_id.to_ne_bytes());
    buf.extend_from_slice(&1i32.to_ne_bytes()); // ntablespaces = 1
    buf.extend_from_slice(&src_tblspcoid.to_ne_bytes());
    xloginsert::XLogBeginInsert()?;
    xloginsert::XLogRegisterData(&buf)?;
    xloginsert::XLogInsert(
        RM_DBASE_ID,
        XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE,
    )?;

    // Now safe to release the database lock.
    lmgr_seams::unlock_shared_object_for_session::call(
        DatabaseRelationId,
        db_id,
        0,
        AccessExclusiveLock,
    )?;

    Ok(())
}

/// Error cleanup callback for movedb (re-signed from `(int, Datum)`).
fn movedb_failure_callback(dest_dboid: Oid, dest_tsoid: Oid) -> PgResult<()> {
    let dstpath = relpath::GetDatabasePath(dest_dboid, dest_tsoid);
    let _ = fd_seams::rmtree::call(&dstpath, true);
    Ok(())
}

// ===========================================================================
// DropDatabase — C 2342-2362
// ===========================================================================

/// Process options and call `dropdb`.
pub fn DropDatabase<'mcx>(pstate: &ParseState<'mcx>, stmt: &DropdbStmt<'mcx>) -> PgResult<()> {
    let mut force = false;
    for opt in def_elems(&stmt.options) {
        if def_name(opt) == "force" {
            force = true;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "unrecognized DROP DATABASE option \"{}\"",
                    def_name(opt)
                ))
                .finish(errloc(2356, "DropDatabase"))
                .unwrap_err());
        }
    }
    let _ = pstate;

    let ctx = ::mcx::MemoryContext::new("DropDatabase");
    dropdb(
        ctx.mcx(),
        stmt.dbname.as_deref().unwrap_or(""),
        stmt.missing_ok,
        force,
    )
}

// ===========================================================================
// AlterDatabase — C 2367-2534
// ===========================================================================

/// `ALTER DATABASE name ...`.
pub fn AlterDatabase<'mcx>(
    pstate: &ParseState<'mcx>,
    stmt: &AlterDatabaseStmt<'mcx>,
    isTopLevel: bool,
) -> PgResult<Oid> {
    let ctx = ::mcx::MemoryContext::new("AlterDatabase");
    let mcx = ctx.mcx();
    let dbname = stmt.dbname.as_deref().unwrap_or("");

    let options = def_elems(&stmt.options);
    let mut distemplate: Option<&DefElem> = None;
    let mut dallowconnections: Option<&DefElem> = None;
    let mut dconnlimit: Option<&DefElem> = None;
    let mut dtablespace: Option<&DefElem> = None;

    macro_rules! once {
        ($slot:ident, $defel:expr) => {{
            if $slot.is_some() {
                error_conflicting_def_elem($defel, pstate)?;
            }
            $slot = Some($defel);
        }};
    }

    for defel in options {
        match def_name(defel) {
            "is_template" => once!(distemplate, defel),
            "allow_connections" => once!(dallowconnections, defel),
            "connection_limit" => once!(dconnlimit, defel),
            "tablespace" => once!(dtablespace, defel),
            other => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("option \"{other}\" not recognized"))
                    .finish(errloc(2421, "AlterDatabase"))
                    .unwrap_err());
            }
        }
    }

    if let Some(el) = dtablespace {
        // SET TABLESPACE syntax forbids other options.
        if stmt.options.len() != 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "option \"{}\" cannot be specified with other options",
                    def_name(el)
                ))
                .finish(errloc(2435, "AlterDatabase"))
                .unwrap_err());
        }
        // Not allowed within a transaction block.
        transam_xact::PreventInTransactionBlock(
            isTopLevel,
            "ALTER DATABASE SET TABLESPACE",
        )?;
        let ts = def_get_string(el)?;
        movedb(mcx, dbname, &ts)?;
        return Ok(InvalidOid);
    }

    let mut dbistemplate = false;
    let mut dballowconnections = true;
    let mut dbconnlimit = DATCONNLIMIT_UNLIMITED;

    if let Some(el) = distemplate {
        if defel_has_arg(el) {
            dbistemplate = def_get_boolean(el)?;
        }
    }
    if let Some(el) = dallowconnections {
        if defel_has_arg(el) {
            dballowconnections = def_get_boolean(el)?;
        }
    }
    if let Some(el) = dconnlimit {
        if defel_has_arg(el) {
            dbconnlimit = def_get_int32(el)?;
            if dbconnlimit < DATCONNLIMIT_UNLIMITED {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid connection limit: {dbconnlimit}"))
                    .finish(errloc(2454, "AlterDatabase"))
                    .unwrap_err());
            }
        }
    }

    // Get the old tuple (+ inplace-update tuple lock).
    let rel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;
    let my_db = tablespace_globals_seams::MyDatabaseId::call()?;
    let locked = dbcat::scan_pg_database_locked_for_update::call(
        mcx, &rel, my_db, false, InvalidOid, dbname,
    )?;
    let (otid, mut form) = match locked {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{dbname}\" does not exist"))
                .finish(errloc(2473, "AlterDatabase"))
                .unwrap_err());
        }
    };
    let dboid = form.oid;

    if crate::database_is_invalid_form(form.datconnlimit) {
        return Err(ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot alter invalid database \"{dbname}\""))
            .errhint("Use DROP DATABASE to drop invalid databases.".to_string())
            .finish(errloc(2483, "AlterDatabase"))
            .unwrap_err());
    }

    if !aclchk::object_ownercheck(
        mcx,
        DatabaseRelationId,
        dboid,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_DATABASE,
            Some(dbname.to_string()),
        )?;
    }

    // Refuse to disallow connections to the current database.
    if !dballowconnections && dboid == my_db {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot disallow connections for current database".to_string())
            .finish(errloc(2500, "AlterDatabase"))
            .unwrap_err());
    }

    // Build an updated tuple (only replacing the specified columns).
    if distemplate.is_some() {
        form.datistemplate = dbistemplate;
    }
    if dallowconnections.is_some() {
        form.datallowconn = dballowconnections;
    }
    if dconnlimit.is_some() {
        form.datconnlimit = dbconnlimit;
    }

    dbcat::update_pg_database::call(mcx, &rel, otid, &form)?;

    objectaccess::invoke_object_post_alter_hook(
        DatabaseRelationId,
        dboid,
        0,
        InvalidOid,
        false,
    )?;

    // Close pg_database, but keep lock till commit.
    table_close(rel, NoLock)?;

    Ok(dboid)
}

// ===========================================================================
// AlterDatabaseRefreshColl — C 2540-2631
// ===========================================================================

/// `ALTER DATABASE name REFRESH COLLATION VERSION`.
pub fn AlterDatabaseRefreshColl<'mcx>(
    stmt: &AlterDatabaseRefreshCollStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let ctx = ::mcx::MemoryContext::new("AlterDatabaseRefreshColl");
    let mcx = ctx.mcx();
    let dbname = stmt.dbname.as_deref().unwrap_or("");

    let rel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;
    let my_db = tablespace_globals_seams::MyDatabaseId::call()?;
    let locked = dbcat::scan_pg_database_locked_for_update::call(
        mcx, &rel, my_db, false, InvalidOid, dbname,
    )?;
    let (otid, mut form) = match locked {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{dbname}\" does not exist"))
                .finish(errloc(2566, "AlterDatabaseRefreshColl"))
                .unwrap_err());
        }
    };
    let db_id = form.oid;

    if !aclchk::object_ownercheck(
        mcx,
        DatabaseRelationId,
        db_id,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_DATABASE,
            Some(dbname.to_string()),
        )?;
    }

    let oldversion: Option<String> = form.datcollversion.as_ref().map(|s| s.as_str().to_string());

    // The locale to query depends on the provider.
    let datum = if form.datlocprovider == COLLPROVIDER_LIBC {
        form.datcollate.as_str().to_string()
    } else {
        match form.datlocale.as_ref() {
            Some(l) => l.as_str().to_string(),
            None => {
                return Err(ereport(ERROR)
                    .errmsg("unexpected null in pg_database".to_string())
                    .finish(errloc(2589, "AlterDatabaseRefreshColl"))
                    .unwrap_err());
            }
        }
    };

    let newversion = coll_actual_version(mcx, form.datlocprovider, &datum)?;

    // Cannot change from NULL to non-NULL or vice versa.
    match (&oldversion, &newversion) {
        (None, Some(_)) | (Some(_), None) => {
            return Err(ereport(ERROR)
                .errmsg("invalid collation version change".to_string())
                .finish(errloc(2597, "AlterDatabaseRefreshColl"))
                .unwrap_err());
        }
        (Some(ov), Some(nv)) if nv != ov => {
            ereport(NOTICE)
                .errmsg(format!("changing version from {ov} to {nv}"))
                .finish(errloc(2606, "AlterDatabaseRefreshColl"))?;

            form.datcollversion = Some(pgstr(mcx, nv)?);
            dbcat::update_pg_database::call(mcx, &rel, otid, &form)?;
        }
        _ => {
            ereport(NOTICE)
                .errmsg("version has not changed".to_string())
                .finish(errloc(2618, "AlterDatabaseRefreshColl"))?;
        }
    }

    objectaccess::invoke_object_post_alter_hook(
        DatabaseRelationId,
        db_id,
        0,
        InvalidOid,
        false,
    )?;

    table_close(rel, NoLock)?;

    Ok(ObjectAddress {
        classId: DatabaseRelationId,
        objectId: db_id,
        objectSubId: 0,
    })
}

// ===========================================================================
// AlterDatabaseSet — C 2637-2657
// ===========================================================================

/// `ALTER DATABASE name SET ...`.
pub fn AlterDatabaseSet<'mcx>(stmt: &AlterDatabaseSetStmt<'mcx>) -> PgResult<Oid> {
    let ctx = ::mcx::MemoryContext::new("AlterDatabaseSet");
    let mcx = ctx.mcx();
    let dbname = stmt.dbname.as_deref().unwrap_or("");

    let datid = get_database_oid(dbname, false)?;

    // Obtain a lock on the database and make sure it didn't go away.
    pg_shdepend::shdepLockAndCheckObject(DatabaseRelationId, datid)?;

    if !aclchk::object_ownercheck(
        mcx,
        DatabaseRelationId,
        datid,
        miscinit::GetUserId(),
    )? {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_DATABASE,
            Some(dbname.to_string()),
        )?;
    }

    // AlterSetting(datid, InvalidOid, stmt->setstmt). The setstmt is a
    // VariableSetStmt; the pg_db_role_setting owner consumes it through its own
    // seam (the parser's `'mcx` parse-node model and the owner's owned-String
    // VariableSetStmt model meet only at that seam).
    let setstmt = match stmt.setstmt.as_deref() {
        Some(node) if node.is_variablesetstmt() => node,
        _ => {
            return Err(ereport(ERROR)
                .errmsg("ALTER DATABASE SET requires a SET statement".to_string())
                .finish(errloc(2652, "AlterDatabaseSet"))
                .unwrap_err());
        }
    };
    pg_db_role_setting_seams::alter_database_setting::call(
        mcx,
        datid,
        InvalidOid,
        setstmt,
    )?;

    lmgr_seams::unlock_shared_object::call(
        DatabaseRelationId,
        datid,
        0,
        AccessShareLock,
    )?;

    Ok(datid)
}

// ===========================================================================
// AlterDatabaseOwner — C 2663-2773
// ===========================================================================

/// `ALTER DATABASE name OWNER TO newowner`.
pub fn AlterDatabaseOwner(
    mcx: Mcx<'_>,
    dbname: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let rel = table_open(mcx, DatabaseRelationId, RowExclusiveLock)?;
    let my_db = tablespace_globals_seams::MyDatabaseId::call()?;
    let locked = dbcat::scan_pg_database_locked_for_update::call(
        mcx, &rel, my_db, false, InvalidOid, dbname,
    )?;
    let (otid, mut form) = match locked {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{dbname}\" does not exist"))
                .finish(errloc(2690, "AlterDatabaseOwner"))
                .unwrap_err());
        }
    };
    let db_id = form.oid;

    // If the new owner is the same as the existing owner, succeed.
    if form.datdba != new_owner_id {
        // Must be owner of the existing object.
        if !aclchk::object_ownercheck(
            mcx,
            DatabaseRelationId,
            db_id,
            miscinit::GetUserId(),
        )? {
            aclchk::aclcheck_error(
                ACLCHECK_NOT_OWNER,
                OBJECT_DATABASE,
                Some(dbname.to_string()),
            )?;
        }

        // Must be able to become new owner.
        adt_acl::role_membership::check_can_set_role(
            miscinit::GetUserId(),
            new_owner_id,
        )?;

        // Must have createdb rights.
        if !have_createdb_privilege(mcx)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to change owner of database".to_string())
                .finish(errloc(2730, "AlterDatabaseOwner"))
                .unwrap_err());
        }

        let old_dba = form.datdba;
        form.datdba = new_owner_id;

        // Determine the modified ACL for the new owner (only when non-null).
        let new_datacl = match form.datacl.as_ref() {
            Some(acl_bytes) => Some(dbcat::aclnewowner_datacl::call(
                mcx,
                acl_bytes.as_slice(),
                old_dba,
                new_owner_id,
            )?),
            None => None,
        };
        if let Some(new_datacl) = new_datacl {
            form.datacl = Some(new_datacl);
        }

        dbcat::update_pg_database::call(mcx, &rel, otid, &form)?;

        // Update owner dependency reference.
        pg_shdepend::changeDependencyOnOwner(
            DatabaseRelationId,
            db_id,
            new_owner_id,
        )?;
    }

    objectaccess::invoke_object_post_alter_hook(
        DatabaseRelationId,
        db_id,
        0,
        InvalidOid,
        false,
    )?;

    table_close(rel, NoLock)?;

    Ok(ObjectAddress {
        classId: DatabaseRelationId,
        objectId: db_id,
        objectSubId: 0,
    })
}

// ===========================================================================
// pg_database_collation_actual_version — C 2776-2807
// ===========================================================================

/// `pg_database_collation_actual_version(dbid)` (SQL-callable). Re-signed to a
/// typed `(Oid) -> Option<String>` (the fmgr Datum wrapper is deferred).
pub fn pg_database_collation_actual_version(mcx: Mcx<'_>, dbid: Oid) -> PgResult<Option<String>> {
    let dbform = match dbcat::search_database_syscache::call(mcx, dbid)? {
        Some(f) => f,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("database with OID {dbid} does not exist"))
                .finish(errloc(2789, "pg_database_collation_actual_version"))
                .unwrap_err());
        }
    };

    let locale = if dbform.datlocprovider == COLLPROVIDER_LIBC {
        dbform.datcollate.as_str().to_string()
    } else {
        // SysCacheGetAttrNotNull(datlocale).
        match dbform.datlocale.as_ref() {
            Some(l) => l.as_str().to_string(),
            None => {
                return Err(ereport(ERROR)
                    .errmsg("unexpected null in pg_database".to_string())
                    .finish(errloc(2796, "pg_database_collation_actual_version"))
                    .unwrap_err());
            }
        }
    };

    coll_actual_version(mcx, dbform.datlocprovider, &locale)
}

// ===========================================================================
// remove_dbtablespaces — C 3003-3079
// ===========================================================================

/// Remove tablespace directories: iterate through all tablespaces removing
/// `<tablespace>/db_id`.
fn remove_dbtablespaces(mcx: Mcx<'_>, db_id: Oid) -> PgResult<()> {
    let rel = tscat::tablespace_table_open::call(mcx, AccessShareLock)?;
    let oids = tscat::scan_all_tablespace_oids::call(&rel)?;

    let mut ltblspc: Vec<Oid> = Vec::new();
    for dsttablespace in oids {
        // Don't mess with the global tablespace.
        if dsttablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        let dstpath = relpath::GetDatabasePath(db_id, dsttablespace);

        match fd_seams::lstat_file::call(&dstpath, true)? {
            Some(st) if st.isdir => {}
            _ => continue,
        }

        if !fd_seams::rmtree::call(&dstpath, true) {
            ereport(WARNING)
                .errmsg(format!(
                    "some useless files may be left behind in old database directory \"{dstpath}\""
                ))
                .finish(errloc(3038, "remove_dbtablespaces"))?;
        }

        ltblspc.push(dsttablespace);
    }

    let ntblspc = ltblspc.len();
    if ntblspc == 0 {
        tscat::tablespace_table_close::call(rel, AccessShareLock)?;
        return Ok(());
    }

    // Record the filesystem change in XLOG: MinSizeOfDbaseDropRec (db_id +
    // ntablespaces) followed by the tablespace oid array.
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&db_id.to_ne_bytes());
    buf.extend_from_slice(&(ntblspc as i32).to_ne_bytes());
    xloginsert::XLogBeginInsert()?;
    xloginsert::XLogRegisterData(&buf)?;
    let mut oidbuf: Vec<u8> = Vec::with_capacity(ntblspc * 4);
    for ts in &ltblspc {
        oidbuf.extend_from_slice(&ts.to_ne_bytes());
    }
    xloginsert::XLogRegisterData(&oidbuf)?;
    xloginsert::XLogInsert(
        RM_DBASE_ID,
        XLOG_DBASE_DROP | XLR_SPECIAL_REL_UPDATE,
    )?;

    tscat::tablespace_table_close::call(rel, AccessShareLock)?;
    Ok(())
}

// ===========================================================================
// check_db_file_conflict — C 3093-3131
// ===========================================================================

/// Check for existing files that conflict with a proposed new DB OID; return
/// true if there are any.
fn check_db_file_conflict(mcx: Mcx<'_>, db_id: Oid) -> PgResult<bool> {
    let rel = tscat::tablespace_table_open::call(mcx, AccessShareLock)?;
    let oids = tscat::scan_all_tablespace_oids::call(&rel)?;

    let mut result = false;
    for dsttablespace in oids {
        // Don't mess with the global tablespace.
        if dsttablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        let dstpath = relpath::GetDatabasePath(db_id, dsttablespace);
        if fd_seams::lstat_file::call(&dstpath, true)?.is_some() {
            // Found a conflicting file (or directory, whatever).
            result = true;
            break;
        }
    }

    tscat::tablespace_table_close::call(rel, AccessShareLock)?;
    Ok(result)
}
