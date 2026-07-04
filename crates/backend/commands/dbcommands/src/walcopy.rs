use elog::ereport;
use mcx::Mcx;
use types_core::primitive::RmgrIds;
use types_core::{
    BlockNumber, ForkNumber, InvalidOid, Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    RELPERSISTENCE_TEMP,
};
use types_error::{PgResult, ERROR};
use types_rel::{LockRelId, RELKIND_HAS_STORAGE};
use types_storage::buf::BufferAccessStrategyType;
use types_storage::bufpage::PageRef;
use types_storage::lock::AccessShareLock;
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::XLOG_DBASE_CREATE_WAL_LOG;

const RelationRelationId: Oid = 1259;
const PG_MAJORVERSION: &str = "18";

struct CreateDBRelInfo {
    rlocator: RelFileLocator,
    reloid: Oid,
    permanent: bool,
}

pub(crate) fn CreateDatabaseUsingWalLog(
    mcx: Mcx<'_>,
    src_dboid: Oid,
    dst_dboid: Oid,
    src_tsid: Oid,
    dst_tsid: Oid,
) -> PgResult<()> {
    let srcpath = relpath::GetDatabasePath(mcx, src_dboid, src_tsid)?;
    let dstpath = relpath::GetDatabasePath(mcx, dst_dboid, dst_tsid)?;

    CreateDirAndVersionFile(dstpath.as_str(), dst_dboid, dst_tsid, false)?;

    relmapper::RelationMapCopy(dst_dboid, dst_tsid, srcpath.as_str(), dstpath.as_str())?;

    let rlocatorlist = ScanSourceDatabasePgClass(src_tsid, src_dboid, srcpath.as_str())?;
    debug_assert!(!rlocatorlist.is_empty());

    for relinfo in &rlocatorlist {
        let srcrlocator = relinfo.rlocator;
        let dstrlocator = RelFileLocator {
            spcOid: if srcrlocator.spcOid == src_tsid { dst_tsid } else { srcrlocator.spcOid },
            dbOid: dst_dboid,
            relNumber: srcrlocator.relNumber,
        };

        let srcrelid = LockRelId { relId: relinfo.reloid, dbId: src_dboid };
        let dstrelid = LockRelId { relId: relinfo.reloid, dbId: dst_dboid };
        lmgr::LockRelationId(&srcrelid, AccessShareLock)?;
        lmgr::LockRelationId(&dstrelid, AccessShareLock)?;

        bufmgr::CreateAndCopyRelationData(srcrlocator, dstrlocator, relinfo.permanent)?;

        lmgr::UnlockRelationId(&srcrelid, AccessShareLock)?;
        lmgr::UnlockRelationId(&dstrelid, AccessShareLock)?;
    }
    Ok(())
}

fn ScanSourceDatabasePgClass(
    tbid: Oid,
    dbid: Oid,
    srcpath: &str,
) -> PgResult<Vec<CreateDBRelInfo>> {
    let relfilenumber =
        relmapper::RelationMapOidToFilenumberForDatabase(srcpath, RelationRelationId)?;

    let relid = LockRelId { relId: RelationRelationId, dbId: dbid };
    lmgr::LockRelationId(&relid, AccessShareLock)?;

    let rlocator = RelFileLocator { spcOid: tbid, dbOid: dbid, relNumber: relfilenumber };
    let key = RelFileLocatorBackend { locator: rlocator, backend: INVALID_PROC_NUMBER };
    smgr::smgropen(rlocator, INVALID_PROC_NUMBER)?;
    let nblocks = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM)?;
    smgr::smgrclose(key)?;

    let bstrategy = bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread);

    let latest = snapmgr::GetLatestSnapshot()?;
    let snapshot = snapmgr::RegisterSnapshot(Some(&latest))?.expect("registered snapshot");

    let mut rlocatorlist = Vec::new();
    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        let buf = bufmgr::ReadBufferWithoutRelcache(
            rlocator,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            bstrategy.clone(),
            true,
        )?;

        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
        // SAFETY: pinned, share-locked buffer page for the scan body.
        let page = unsafe { PageRef::from_raw(bufmgr::BufferGetPagePtr(buf)) };
        if page.is_new() || page.max_offset_number() == 0 {
            bufmgr::UnlockReleaseBuffer(buf)?;
            continue;
        }

        scan_page(page, buf, blkno, tbid, dbid, srcpath, &snapshot, &mut rlocatorlist)?;

        bufmgr::UnlockReleaseBuffer(buf)?;
    }
    snapmgr::UnregisterSnapshot(Some(&snapshot));

    lmgr::UnlockRelationId(&relid, AccessShareLock)?;

    Ok(rlocatorlist)
}

#[allow(clippy::too_many_arguments)]
fn scan_page(
    page: PageRef<'_>,
    buf: types_core::Buffer,
    blkno: BlockNumber,
    tbid: Oid,
    dbid: Oid,
    srcpath: &str,
    snapshot: &snapmgr::Snapshot,
    rlocatorlist: &mut Vec<CreateDBRelInfo>,
) -> PgResult<()> {
    let maxoff = page.max_offset_number();
    for offnum in 1..=maxoff {
        let itemid = page.item_id(offnum);
        if !itemid.is_used() || itemid.is_dead() || itemid.is_redirected() {
            continue;
        }
        debug_assert!(itemid.is_normal());

        let mut tid = ItemPointerData::default();
        types_tuple::ItemPointerSet(&mut tid, blkno, offnum);
        // SAFETY: normal item id on a share-locked page; the item image is a
        // live heap tuple readable for its recorded length.
        let (item_ptr, item_len) = unsafe { page.item_raw_unchecked(itemid) };
        let mut tuple = unsafe {
            HeapTupleData::from_raw_parts(item_ptr, item_len, tid, RelationRelationId)
        };

        if heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
            &mut tuple,
            snapshot.as_ref(),
            buf,
        )? {
            if let Some(relinfo) = scan_tuple(&tuple, tbid, dbid, srcpath)? {
                rlocatorlist.push(relinfo);
            }
        }
    }
    Ok(())
}

// ScanSourceDatabasePgClassTuple: fixed-offset GETSTRUCT reads of the
// pg_class prefix (oid, relfilenode, reltablespace, relpersistence, relkind);
// prefix columns precede any nullable/varlena attribute.
fn scan_tuple(
    tuple: &HeapTupleData<'_>,
    tbid: Oid,
    dbid: Oid,
    srcpath: &str,
) -> PgResult<Option<CreateDBRelInfo>> {
    let base = tuple.getstruct();
    // SAFETY: pg_class rows always carry the full fixed prefix (120 bytes);
    // offsets mirror FormData_pg_class field order in pg_class.h.
    let (oid, relfilenode, reltablespace, relpersistence, relkind) = unsafe {
        let u32_at = |off: usize| -> u32 {
            u32::from_ne_bytes(core::slice::from_raw_parts(base.add(off), 4).try_into().unwrap())
        };
        (u32_at(0), u32_at(88), u32_at(92), *base.add(118), *base.add(119))
    };

    if reltablespace == crate::GLOBALTABLESPACE_OID
        || !RELKIND_HAS_STORAGE(relkind)
        || relpersistence == RELPERSISTENCE_TEMP
    {
        return Ok(None);
    }

    let relfilenumber = if relfilenode != InvalidOid {
        relfilenode
    } else {
        relmapper::RelationMapOidToFilenumberForDatabase(srcpath, oid)?
    };

    if relfilenumber == InvalidOid {
        return Err(ereport(ERROR)
            .errmsg(format!("relation with OID {oid} does not have a valid relfilenumber"))
            .into_error()
            .into());
    }

    Ok(Some(CreateDBRelInfo {
        rlocator: RelFileLocator {
            spcOid: if reltablespace != InvalidOid { reltablespace } else { tbid },
            dbOid: dbid,
            relNumber: relfilenumber,
        },
        reloid: oid,
        permanent: relpersistence == RELPERSISTENCE_PERMANENT,
    }))
}

pub(crate) fn CreateDirAndVersionFile(
    dbpath: &str,
    dbid: Oid,
    tsid: Oid,
    is_redo: bool,
) -> PgResult<()> {
    let buf = format!("{PG_MAJORVERSION}\n");

    if fd::MakePGDirectory(dbpath) < 0 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno != libc::EEXIST || !is_redo {
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .with_saved_errno(errno)
                .errmsg(format!("could not create directory \"{dbpath}\": %m"))
                .into_error()
                .into());
        }
    }

    let versionfile = format!("{dbpath}/PG_VERSION");

    let mut fdnum =
        fd::OpenTransientFile(&versionfile, libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL)?;
    if fdnum < 0 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno == libc::EEXIST && is_redo {
            fdnum = fd::OpenTransientFile(&versionfile, libc::O_WRONLY | libc::O_TRUNC)?;
        }
    }
    if fdnum < 0 {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{versionfile}\": %m"))
            .into_error()
            .into());
    }

    // SAFETY: fdnum is an open fd owned by the transient-file table.
    let written = unsafe { libc::write(fdnum, buf.as_ptr().cast(), buf.len()) };
    if written != buf.len() as isize {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::ENOSPC);
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .with_saved_errno(if errno == 0 { libc::ENOSPC } else { errno })
            .errmsg(format!("could not write to file \"{versionfile}\": %m"))
            .into_error()
            .into());
    }

    if fd::pg_fsync(fdnum) != 0 {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .with_saved_errno(errno)
            .errmsg(format!("could not fsync file \"{versionfile}\": %m"))
            .into_error()
            .into());
    }
    fd::fsync_fname(dbpath, true)?;

    fd::CloseTransientFile(fdnum);

    if !is_redo {
        init_small::globals::StartCriticalSection();
        let mut xlrec = [0u8; 8];
        xlrec[0..4].copy_from_slice(&dbid.to_ne_bytes());
        xlrec[4..8].copy_from_slice(&tsid.to_ne_bytes());
        let res = xloginsert::insert_record(
            RmgrIds::RM_DBASE_ID as u8,
            XLOG_DBASE_CREATE_WAL_LOG,
            0,
            &[&xlrec],
            &[],
        );
        init_small::globals::EndCriticalSection();
        res?;
    }
    Ok(())
}
