// inv_api.c: server-side large-object byte API over pg_largeobject pages.
#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use types_core::{int64, uint64, AttrNumber, Oid};
use types_error::{
    PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_rel::{NoLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, ScanKeyData};
use types_scan::sdir::ScanDirection;
use types_storage::large_object::{IFS_RDLOCK, IFS_WRLOCK, LOBLKSIZE, MAX_LARGE_OBJECT_SIZE};
use types_tuple::varatt;
use types_tuple::{HeapTupleData, ItemPointerData};

use pg_largeobject::{
    Anum_pg_largeobject_data, Anum_pg_largeobject_loid, Anum_pg_largeobject_pageno,
    LargeObjectLOidPNIndexId, LargeObjectRelationId, Snapshot,
};

pub type LargeObjectDesc = types_storage::large_object::LargeObjectDesc<Option<Snapshot>>;

pub const INV_WRITE: i32 = 0x0002_0000;
pub const INV_READ: i32 = 0x0004_0000;

pub const SEEK_SET: i32 = 0;
pub const SEEK_CUR: i32 = 1;
pub const SEEK_END: i32 = 2;

const LOBLKSIZE_USZ: usize = LOBLKSIZE as usize;
const LOBLKSIZE_U64: uint64 = LOBLKSIZE as uint64;
const VARHDRSZ: usize = varatt::VARHDRSZ;

// C's workbuf union: 4-byte varlena header + LOBLKSIZE payload, int-aligned so
// the header word write and heap_form_tuple's source reads are in-bounds.
#[repr(C, align(8))]
struct WorkBuf {
    hdr: [u8; VARHDRSZ],
    data: [u8; LOBLKSIZE_USZ],
}

impl WorkBuf {
    fn new() -> Self {
        WorkBuf {
            hdr: [0; VARHDRSZ],
            data: [0; LOBLKSIZE_USZ],
        }
    }

    // SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ); returns the data-column datum.
    fn datum(&mut self, len: usize) -> Datum {
        debug_assert!(len <= LOBLKSIZE_USZ);
        let word = varatt::set_varsize_4b_word((len + VARHDRSZ) as u32);
        self.hdr.copy_from_slice(&word.to_ne_bytes());
        Datum::from_usize(self as *const WorkBuf as usize)
    }
}

// Held copy of a scanned pg_largeobject row (the C keeps the scan tuple
// pointer alive instead; the copy frees the scan borrow).
struct LoPageBuf {
    pageno: i32,
    tid: ItemPointerData,
    len: usize,
    data: [u8; LOBLKSIZE_USZ],
}

fn scankey(attno: AttrNumber, strategy: u16, func: types_core::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = strategy;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn oid_eq_key(attno: AttrNumber, value: Oid) -> ScanKeyData {
    scankey(attno, BTEqualStrategyNumber, types_core::fmgr::F_OIDEQ, Datum::from_oid(value))
}

fn int4_ge_key(attno: AttrNumber, value: i32) -> ScanKeyData {
    scankey(
        attno,
        BTGreaterEqualStrategyNumber,
        types_core::fmgr::F_INT4GE,
        Datum::from_i32(value),
    )
}

fn open_lo_relation<'mcx>(mcx: Mcx<'mcx>) -> PgResult<(Relation<'mcx>, Relation<'mcx>)> {
    // C caches these in statics until xact end; the relcache makes per-op
    // opens equivalent, and the NoLock closes retain the lock to xact end.
    let lo_heap_r = table::table_open(mcx, LargeObjectRelationId, RowExclusiveLock)?;
    let lo_index_r = indexam::index_open(mcx, LargeObjectLOidPNIndexId, RowExclusiveLock)?;
    Ok((lo_heap_r, lo_index_r))
}

pub fn close_lo_relation(_isCommit: bool) -> PgResult<()> {
    Ok(())
}

std::thread_local! {
    // bool lo_compat_privileges (inv_api.c:56): PGC_SUSET, boot false.
    static LO_COMPAT_PRIVILEGES: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

pub fn init_seams() {
    guc_tables::vars::lo_compat_privileges.install(guc_tables::GucVarAccessors {
        get: || LO_COMPAT_PRIVILEGES.with(core::cell::Cell::get),
        set: |v| LO_COMPAT_PRIVILEGES.with(|c| c.set(v)),
    });
}

#[cold]
fn corrupt_page(loid: Oid, pageno: i32, size: usize) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_DATA_CORRUPTED)
        .errmsg(format!(
            "pg_largeobject entry for OID {loid}, page {pageno} has invalid data field size {size}"
        ))
        .into_error()
        .into()
}

// HeapTupleHasNulls paranoia + GETSTRUCT + getdatafield: pageno, tid, and the
// detoasted data payload copied into `out.data`.
fn read_lo_page<'mcx>(
    mcx: Mcx<'mcx>,
    lo_heap_r: &Relation<'mcx>,
    tuple: &HeapTupleData<'_>,
    loid: Oid,
    out: &mut LoPageBuf,
) -> PgResult<()> {
    if tuple.has_nulls() {
        return Err(ereport(ERROR)
            .errmsg_internal("null field found in pg_largeobject")
            .into_error()
            .into());
    }
    let desc = lo_heap_r.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_largeobject columns under the relation's descriptor.
    let (pageno, data) = unsafe {
        (
            types_tuple::heap_getattr(tuple, Anum_pg_largeobject_pageno as i32, desc, &mut isnull)
                .as_i32(),
            types_tuple::heap_getattr(tuple, Anum_pg_largeobject_data as i32, desc, &mut isnull),
        )
    };
    out.pageno = pageno;
    out.tid = tuple.t_self;

    let p = data.as_usize() as *const u8;
    // SAFETY: non-null data column is a live varlena inside the held tuple.
    unsafe {
        if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p)) {
            let image = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = detoast::detoast_attr(mcx, image)?;
            let payload = &flat[VARHDRSZ..];
            if payload.len() > LOBLKSIZE_USZ {
                return Err(corrupt_page(loid, pageno, payload.len()));
            }
            out.len = payload.len();
            out.data[..payload.len()].copy_from_slice(payload);
        } else if varatt::varatt_is_1b(p) {
            let len = varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT;
            if len > LOBLKSIZE_USZ {
                return Err(corrupt_page(loid, pageno, len));
            }
            out.len = len;
            out.data[..len]
                .copy_from_slice(core::slice::from_raw_parts(p.add(varatt::VARHDRSZ_SHORT), len));
        } else {
            let len = varatt::varsize_4b(p) - VARHDRSZ;
            if len > LOBLKSIZE_USZ {
                return Err(corrupt_page(loid, pageno, len));
            }
            out.len = len;
            out.data[..len]
                .copy_from_slice(core::slice::from_raw_parts(p.add(VARHDRSZ), len));
        }
    }
    Ok(())
}


pub fn inv_create<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid) -> PgResult<Oid> {
    let lobjId_new = pg_largeobject::LargeObjectCreate(mcx, lobjId)?;

    // LO dependencies are recorded under LargeObjectRelationId (heap classid)
    // for backwards-compatibility reasons.
    pg_depend::recordDependencyOnOwner(mcx, LargeObjectRelationId, lobjId_new, miscinit::GetUserId())?;

    // InvokeObjectPostCreateHook: no object_access_hook can be installed.

    xact::CommandCounterIncrement()?;

    Ok(lobjId_new)
}

pub fn inv_open<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid, flags: i32) -> PgResult<LargeObjectDesc> {
    let mut descflags: i32 = 0;

    if flags & INV_WRITE != 0 {
        descflags |= IFS_WRLOCK | IFS_RDLOCK;
    }
    if flags & INV_READ != 0 {
        descflags |= IFS_RDLOCK;
    }

    if descflags == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid flags for opening a large object: {flags}"))
            .into_error()
            .into());
    }

    // If write is requested, use an instantaneous snapshot (None => the
    // up-to-date catalog snapshot in the scans below).
    let snapshot: Option<Snapshot> = if descflags & IFS_WRLOCK != 0 {
        None
    } else {
        Some(snapmgr::GetActiveSnapshot())
    };

    if !pg_largeobject::LargeObjectExistsWithSnapshot(mcx, lobjId, snapshot.clone())? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("large object {lobjId} does not exist"))
            .into_error()
            .into());
    }

    if (descflags & IFS_RDLOCK) != 0
        && !guc_tables::vars::lo_compat_privileges.read()
        && aclchk::pg_largeobject_aclcheck_snapshot(
            mcx,
            lobjId,
            miscinit::GetUserId(),
            adt_acl::ACL_SELECT,
            snapshot.clone(),
        )? != 0
    {
        return Err(permission_denied(lobjId));
    }
    if (descflags & IFS_WRLOCK) != 0
        && !guc_tables::vars::lo_compat_privileges.read()
        && aclchk::pg_largeobject_aclcheck_snapshot(
            mcx,
            lobjId,
            miscinit::GetUserId(),
            adt_acl::ACL_UPDATE,
            snapshot.clone(),
        )? != 0
    {
        return Err(permission_denied(lobjId));
    }

    Ok(LargeObjectDesc {
        id: lobjId,
        snapshot,
        subid: types_core::InvalidSubTransactionId,
        offset: 0,
        flags: descflags,
    })
}

#[cold]
fn permission_denied(lobjId: Oid) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg(format!("permission denied for large object {lobjId}"))
        .into_error()
        .into()
}

pub fn inv_close(obj_desc: LargeObjectDesc) -> PgResult<()> {
    drop(obj_desc);
    Ok(())
}

pub fn inv_drop<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid) -> PgResult<i32> {
    let object = pg_depend::ObjectAddress::set(LargeObjectRelationId, lobjId);
    catalog_dependency::performDeletion(mcx, &object, catalog_dependency::DropBehavior::DROP_CASCADE, 0)?;

    xact::CommandCounterIncrement()?;

    // For historical reasons, we always return 1 on success.
    Ok(1)
}

fn inv_getsize<'mcx>(mcx: Mcx<'mcx>, obj_desc: &LargeObjectDesc) -> PgResult<uint64> {
    let mut lastbyte: uint64 = 0;

    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    let skey = [oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id)];

    let mut sd = genam::systable_beginscan_ordered(
        mcx,
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.clone(),
        &skey,
    )?;

    // The index covers (loid, pageno); one backward step lands on the last page.
    let mut page = LoPageBuf {
        pageno: 0,
        tid: ItemPointerData::invalid(),
        len: 0,
        data: [0; LOBLKSIZE_USZ],
    };
    if let Some(tuple) =
        genam::systable_getnext_ordered(mcx, &mut sd, ScanDirection::BackwardScanDirection)?
    {
        read_lo_page(mcx, &lo_heap_r, tuple, obj_desc.id, &mut page)?;
        lastbyte = page.pageno as uint64 * LOBLKSIZE_U64 + page.len as uint64;
    }

    genam::systable_endscan_ordered(mcx, sd)?;

    indexam::index_close(lo_index_r, NoLock)?;
    lo_heap_r.close(NoLock)?;

    Ok(lastbyte)
}

pub fn inv_seek<'mcx>(
    mcx: Mcx<'mcx>,
    obj_desc: &mut LargeObjectDesc,
    offset: int64,
    whence: i32,
) -> PgResult<int64> {
    let newoffset: int64 = match whence {
        SEEK_SET => offset,
        SEEK_CUR => (obj_desc.offset as int64).wrapping_add(offset),
        SEEK_END => (inv_getsize(mcx, obj_desc)? as int64).wrapping_add(offset),
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid whence setting: {whence}"))
                .into_error()
                .into());
        }
    };

    if newoffset < 0 || newoffset > MAX_LARGE_OBJECT_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg_internal(format!("invalid large object seek target: {newoffset}"))
            .into_error()
            .into());
    }

    obj_desc.offset = newoffset as uint64;
    Ok(newoffset)
}

pub fn inv_tell(obj_desc: &LargeObjectDesc) -> PgResult<int64> {
    Ok(obj_desc.offset as int64)
}

pub fn inv_read<'mcx>(
    mcx: Mcx<'mcx>,
    obj_desc: &mut LargeObjectDesc,
    buf: &mut [u8],
) -> PgResult<i32> {
    let nbytes: i32 = buf.len() as i32;
    let mut nread: i32 = 0;
    let pageno: i32 = (obj_desc.offset / LOBLKSIZE_U64) as i32;

    if (obj_desc.flags & IFS_RDLOCK) == 0 {
        return Err(permission_denied(obj_desc.id));
    }

    if nbytes <= 0 {
        return Ok(0);
    }

    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    let skey = [
        oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id),
        int4_ge_key(Anum_pg_largeobject_pageno, pageno),
    ];

    let mut sd = genam::systable_beginscan_ordered(
        mcx,
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.clone(),
        &skey,
    )?;

    let mut page = LoPageBuf {
        pageno: 0,
        tid: ItemPointerData::invalid(),
        len: 0,
        data: [0; LOBLKSIZE_USZ],
    };
    while let Some(tuple) =
        genam::systable_getnext_ordered(mcx, &mut sd, ScanDirection::ForwardScanDirection)?
    {
        read_lo_page(mcx, &lo_heap_r, tuple, obj_desc.id, &mut page)?;

        let pageoff: uint64 = page.pageno as uint64 * LOBLKSIZE_U64;
        if pageoff > obj_desc.offset {
            let mut n = (pageoff - obj_desc.offset) as int64;
            n = n.min((nbytes - nread) as int64);
            buf[nread as usize..nread as usize + n as usize].fill(0);
            nread += n as i32;
            obj_desc.offset += n as uint64;
        }

        if nread < nbytes {
            debug_assert!(obj_desc.offset >= pageoff);
            let off = (obj_desc.offset - pageoff) as usize;
            debug_assert!(off < LOBLKSIZE_USZ);

            if page.len > off {
                let mut n = (page.len - off) as int64;
                n = n.min((nbytes - nread) as int64);
                buf[nread as usize..nread as usize + n as usize]
                    .copy_from_slice(&page.data[off..off + n as usize]);
                nread += n as i32;
                obj_desc.offset += n as uint64;
            }
        }

        if nread >= nbytes {
            break;
        }
    }

    genam::systable_endscan_ordered(mcx, sd)?;

    indexam::index_close(lo_index_r, NoLock)?;
    lo_heap_r.close(NoLock)?;

    Ok(nread)
}

fn form_lo_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    lo_heap_r: &Relation<'mcx>,
    loid: Oid,
    pageno: i32,
    data: Datum,
) -> PgResult<heaptuple::HeapTuple<'mcx>> {
    let values = [Datum::from_oid(loid), Datum::from_i32(pageno), data];
    let nulls = [false; pg_largeobject::Natts_pg_largeobject];
    heaptuple::heap_form_tuple(mcx, lo_heap_r.descr(), &values, &nulls)
}

pub fn inv_write<'mcx>(
    mcx: Mcx<'mcx>,
    obj_desc: &mut LargeObjectDesc,
    buf: &[u8],
) -> PgResult<i32> {
    let nbytes: i32 = buf.len() as i32;
    let mut nwritten: i32 = 0;
    let mut pageno: i32 = (obj_desc.offset / LOBLKSIZE_U64) as i32;
    let mut workbuf = WorkBuf::new();
    let mut neednextpage = true;

    // Enforce writability because the snapshot is probably wrong otherwise.
    if (obj_desc.flags & IFS_WRLOCK) == 0 {
        return Err(permission_denied(obj_desc.id));
    }

    if nbytes <= 0 {
        return Ok(0);
    }

    // This addition can't overflow: nbytes is only int32.
    if nbytes as int64 + obj_desc.offset as int64 > MAX_LARGE_OBJECT_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid large object write request size: {nbytes}"))
            .into_error()
            .into());
    }

    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &lo_heap_r)?;

    let skey = [
        oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id),
        int4_ge_key(Anum_pg_largeobject_pageno, pageno),
    ];

    let mut sd = genam::systable_beginscan_ordered(
        mcx,
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.clone(),
        &skey,
    )?;

    let mut oldpage = LoPageBuf {
        pageno: 0,
        tid: ItemPointerData::invalid(),
        len: 0,
        data: [0; LOBLKSIZE_USZ],
    };
    let mut have_old = false;

    while nwritten < nbytes {
        if neednextpage {
            have_old = match genam::systable_getnext_ordered(
                mcx,
                &mut sd,
                ScanDirection::ForwardScanDirection,
            )? {
                Some(tuple) => {
                    read_lo_page(mcx, &lo_heap_r, tuple, obj_desc.id, &mut oldpage)?;
                    debug_assert!(oldpage.pageno >= pageno);
                    true
                }
                None => false,
            };
            neednextpage = false;
        }

        if have_old && oldpage.pageno == pageno {
            let mut len = oldpage.len;
            workbuf.data[..len].copy_from_slice(&oldpage.data[..len]);

            let mut off = (obj_desc.offset % LOBLKSIZE_U64) as usize;
            if off > len {
                workbuf.data[len..off].fill(0);
            }

            let n = (LOBLKSIZE_USZ - off).min((nbytes - nwritten) as usize);
            workbuf.data[off..off + n]
                .copy_from_slice(&buf[nwritten as usize..nwritten as usize + n]);
            nwritten += n as i32;
            obj_desc.offset += n as uint64;
            off += n;
            len = len.max(off);

            let d = workbuf.datum(len);
            let mut newtup = form_lo_tuple(mcx, &lo_heap_r, obj_desc.id, pageno, d)?;
            catalog_indexing::CatalogTupleUpdateWithInfo(
                mcx,
                &lo_heap_r,
                &oldpage.tid,
                &mut newtup,
                &mut indstate,
            )?;

            have_old = false;
            neednextpage = true;
        } else {
            let off = (obj_desc.offset % LOBLKSIZE_U64) as usize;
            if off > 0 {
                workbuf.data[..off].fill(0);
            }

            let n = (LOBLKSIZE_USZ - off).min((nbytes - nwritten) as usize);
            workbuf.data[off..off + n]
                .copy_from_slice(&buf[nwritten as usize..nwritten as usize + n]);
            nwritten += n as i32;
            obj_desc.offset += n as uint64;
            let len = off + n;

            let d = workbuf.datum(len);
            let mut newtup = form_lo_tuple(mcx, &lo_heap_r, obj_desc.id, pageno, d)?;
            catalog_indexing::CatalogTupleInsertWithInfo(mcx, &lo_heap_r, &mut newtup, &mut indstate)?;
        }
        pageno += 1;
    }

    genam::systable_endscan_ordered(mcx, sd)?;

    catalog_indexing::CatalogCloseIndexes(indstate)?;

    indexam::index_close(lo_index_r, NoLock)?;
    lo_heap_r.close(NoLock)?;

    xact::CommandCounterIncrement()?;

    Ok(nwritten)
}

pub fn inv_truncate<'mcx>(
    mcx: Mcx<'mcx>,
    obj_desc: &mut LargeObjectDesc,
    len: int64,
) -> PgResult<()> {
    let pageno: i32 = (len / LOBLKSIZE as int64) as i32;
    let mut workbuf = WorkBuf::new();

    // Enforce writability because the snapshot is probably wrong otherwise.
    if (obj_desc.flags & IFS_WRLOCK) == 0 {
        return Err(permission_denied(obj_desc.id));
    }

    if len < 0 || len > MAX_LARGE_OBJECT_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg_internal(format!("invalid large object truncation target: {len}"))
            .into_error()
            .into());
    }

    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &lo_heap_r)?;

    let skey = [
        oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id),
        int4_ge_key(Anum_pg_largeobject_pageno, pageno),
    ];

    let mut sd = genam::systable_beginscan_ordered(
        mcx,
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.clone(),
        &skey,
    )?;

    let mut oldpage = LoPageBuf {
        pageno: 0,
        tid: ItemPointerData::invalid(),
        len: 0,
        data: [0; LOBLKSIZE_USZ],
    };
    let have_old = match genam::systable_getnext_ordered(
        mcx,
        &mut sd,
        ScanDirection::ForwardScanDirection,
    )? {
        Some(tuple) => {
            read_lo_page(mcx, &lo_heap_r, tuple, obj_desc.id, &mut oldpage)?;
            debug_assert!(oldpage.pageno >= pageno);
            true
        }
        None => false,
    };

    if have_old && oldpage.pageno == pageno {
        let pagelen = oldpage.len;
        workbuf.data[..pagelen].copy_from_slice(&oldpage.data[..pagelen]);

        let off = (len % LOBLKSIZE as int64) as usize;
        if off > pagelen {
            workbuf.data[pagelen..off].fill(0);
        }

        let d = workbuf.datum(off);
        let mut newtup = form_lo_tuple(mcx, &lo_heap_r, obj_desc.id, pageno, d)?;
        catalog_indexing::CatalogTupleUpdateWithInfo(
            mcx,
            &lo_heap_r,
            &oldpage.tid,
            &mut newtup,
            &mut indstate,
        )?;
    } else {
        if have_old {
            debug_assert!(oldpage.pageno > pageno);
            catalog_indexing::CatalogTupleDelete(&lo_heap_r, &oldpage.tid)?;
        }

        let off = (len % LOBLKSIZE as int64) as usize;
        if off > 0 {
            workbuf.data[..off].fill(0);
        }

        let d = workbuf.datum(off);
        let mut newtup = form_lo_tuple(mcx, &lo_heap_r, obj_desc.id, pageno, d)?;
        catalog_indexing::CatalogTupleInsertWithInfo(mcx, &lo_heap_r, &mut newtup, &mut indstate)?;
    }

    if have_old {
        while let Some(tuple) =
            genam::systable_getnext_ordered(mcx, &mut sd, ScanDirection::ForwardScanDirection)?
        {
            let tid = tuple.t_self;
            catalog_indexing::CatalogTupleDelete(&lo_heap_r, &tid)?;
        }
    }

    genam::systable_endscan_ordered(mcx, sd)?;

    catalog_indexing::CatalogCloseIndexes(indstate)?;

    indexam::index_close(lo_index_r, NoLock)?;
    lo_heap_r.close(NoLock)?;

    xact::CommandCounterIncrement()?;

    Ok(())
}
