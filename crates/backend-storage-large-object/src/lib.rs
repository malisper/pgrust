#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`, the
// project-wide error contract; we accept the large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]
// The C declares locals up top and assigns later; keep that decl-then-assign
// shape so the port reads 1:1 against inv_api.c.
#![allow(clippy::needless_late_init)]
// inv_seek / inv_truncate use the C bounds idiom `x < 0 || x > MAX_*`; keep it
// 1:1 rather than rewriting as a RangeInclusive::contains negation.
#![allow(clippy::manual_range_contains)]

//! Port of `src/backend/storage/large_object/inv_api.c` — the server-side
//! inversion-fs large-object byte API (the user-level large-object application
//! interface routines).
//!
//! Every exported C function is present with its original name, flag/snapshot
//! handling, permission-check branch order, page-chunking byte arithmetic
//! (`LOBLKSIZE` read/modify/write, hole-filling), lock levels, scan order, and
//! error code / message / SQLSTATE preserved:
//!
//!   * [`close_lo_relation`] (inv_api.c:96-122) — main-xact-end cleanup of the
//!     single `pg_largeobject` heap/index relation references;
//!   * [`inv_create`] (inv_api.c:172-202) — `LargeObjectCreate` (called
//!     directly into the merged `backend-catalog-pg-largeobject`) + owner
//!     dependency (`recordDependencyOnOwner`) + post-create hook +
//!     `CommandCounterIncrement`;
//!   * [`inv_open`] (inv_api.c:214-292) — flag→descflags mapping, snapshot
//!     selection (`GetActiveSnapshot` for read, instantaneous `None` for write),
//!     existence check (`LargeObjectExistsWithSnapshot`) and the SELECT/UPDATE
//!     permission checks (`pg_largeobject_aclcheck_snapshot`), then descriptor
//!     construction;
//!   * [`inv_close`] (inv_api.c:298-303) — consumes (drops) the owned
//!     descriptor (the C `pfree`);
//!   * [`inv_drop`] (inv_api.c:310-331) — `performDeletion(DROP_CASCADE)` +
//!     `CommandCounterIncrement`;
//!   * [`inv_seek`] / [`inv_tell`] (inv_api.c:387-447) — offset arithmetic with
//!     `MAX_LARGE_OBJECT_SIZE` bounds (`inv_getsize` for `SEEK_END`);
//!   * [`inv_read`] (inv_api.c:449-540) — forward ordered scan, hole-zeroing,
//!     partial-page copy;
//!   * [`inv_write`] (inv_api.c:542-737) — forward ordered scan, read/modify/
//!     write of existing pages and insertion of brand-new pages;
//!   * [`inv_truncate`] (inv_api.c:739-915) — truncate the page at the cut point
//!     (or fill a hole) and delete every page after it.
//!
//! `inv_getsize` and `getdatafield` are file-static helpers in the C;
//! `inv_getsize` is ported in-crate. `getdatafield` (the `HeapTupleHasNulls`
//! paranoia + detoast of the `data` bytea + `VARSIZE` length-sanity raising
//! `ERRCODE_DATA_CORRUPTED`) is the catalog/indexing.c-owned value layer and is
//! performed inside the [`backend_catalog_indexing_seams::deform_lo_page`] seam
//! as each scanned page is surfaced.
//!
//! ## Idiomatic owned style vs the raw-pointer C
//!
//!   * the descriptor is the owned [`LargeObjectDesc`]; [`inv_open`] returns it
//!     boxed (the C `MemoryContextAlloc`) and [`inv_close`] consumes it (the C
//!     `pfree` becomes `Drop`);
//!   * the read/write byte buffers are owned slices (`&mut [u8]` / `&[u8]`)
//!     rather than `char *` + `int nbytes`;
//!   * the `LOBLKSIZE` scratch page (`workbuf`) is an owned `Vec<u8>`.
//!
//! ## The single-relation reference machinery (`open_lo_relation`)
//!
//! The C caches the `pg_largeobject` heap + index `Relation` references in file
//! statics (`lo_heap_r` / `lo_index_r`) and assigns their ownership to
//! `TopTransactionResourceOwner` so the relcache pins and locks survive until
//! main-xact end, where `close_lo_relation` releases them. That static cache +
//! the resowner-transfer kludge is backend state that cannot be expressed as a
//! `'mcx`-bound `Relation` static here, and the resowner machinery is itself
//! unported. The owned model instead opens both relations in the operation's
//! memory context (`table_open` / `index_open` with `RowExclusiveLock`, the
//! repo's direct-call idiom — exactly as the merged `pg_largeobject` /
//! `pg_namespace` / `pg_depend` ports do) and releases them with `NoLock` at the
//! end of each operation, retaining the lock until main-xact end as C does. The
//! relcache makes the per-operation open cheap; functionally it is equivalent to
//! C's static cache. [`close_lo_relation`] therefore has no static to release
//! and is a no-op stub matching the C entry-point signature (the C lock release
//! already happened via the per-operation `NoLock` closes).
//!
//! Foundation crates called directly (deps, not re-stubbed): the merged
//! `backend-catalog-pg-largeobject` (`LargeObjectCreate` /
//! `LargeObjectExistsWithSnapshot`), `backend-catalog-objectaccess`
//! (`object_access_hook_present` / `run_object_post_create_hook`),
//! `backend-access-table-table` (`table_open`), `backend-access-common-scankey`
//! (`ScanKeyInit`).
//!
//! Genuine externals cross per-owner seams (panic until the owner lands): the
//! genam ordered scans (`backend-access-index-genam-seams`), `index_open`
//! (`backend-access-index-indexam-seams`), `recordDependencyOnOwner`
//! (`backend-catalog-pg-shdepend-seams`), `performDeletion`
//! (`backend-catalog-dependency-seams`), `GetUserId`
//! (`backend-utils-init-miscinit-seams`), `GetActiveSnapshot`
//! (`backend-utils-time-snapmgr-seams`), `CommandCounterIncrement`
//! (`backend-access-transam-xact-seams`), `pg_largeobject_aclcheck_snapshot`
//! (`backend-catalog-aclchk-seams`), and the fmgr/`Datum`/varlena value layer of
//! page deform/form/modify (`backend-catalog-indexing-seams`).
//!
//! This crate installs its outward-consumed `inv_*` / `close_lo_relation` seams
//! (`backend-storage-large-object-seams`, called by `be-fsstubs` / the LO SQL
//! functions / `xact.c`) from [`init_seams`].

use std::rc::Rc;

use mcx::MemoryContext;
use types_acl::acl::{AclResult::AclcheckOk as ACLCHECK_OK, ACL_SELECT, ACL_UPDATE};
use types_catalog::catalog::{
    ANUM_PG_LARGEOBJECT_LOID, ANUM_PG_LARGEOBJECT_PAGENO, LARGE_OBJECT_LOID_PN_INDEX_ID,
    LARGE_OBJECT_RELATION_ID,
};
use types_core::fmgr::{F_INT4GE, F_OIDEQ};
use types_core::primitive::{AttrNumber, Oid};
use types_core::xact::InvalidSubTransactionId;
use types_core::{int64, uint64};
use types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::parsenodes::DropBehavior;
use types_scan::scankey::{BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, ScanKeyData};
use types_scan::sdir::ScanDirection;
use types_storage::large_object::{
    LargeObjectDesc, IFS_RDLOCK, IFS_WRLOCK, LOBLKSIZE, MAX_LARGE_OBJECT_SIZE,
};
use types_storage::lock::{NoLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_index_indexam_seams::index_open;
use backend_access_table_table::table_open;
use backend_catalog_indexing_seams as indexing;
use backend_utils_error::ereport;

/* ===========================================================================
 * Aliases / constants spelled as the C macros so the port reads 1:1 against
 * inv_api.c and the headers it includes.
 * ========================================================================= */

/// `LargeObjectRelationId` (`catalog/pg_largeobject.h`) — for historical
/// reasons LO dependencies are recorded under the *heap* relation's class id.
const LargeObjectRelationId: Oid = LARGE_OBJECT_RELATION_ID;
/// `LargeObjectLOidPNIndexId` (`catalog/pg_largeobject.h`) — the
/// `pg_largeobject_loid_pn_index`.
const LargeObjectLOidPNIndexId: Oid = LARGE_OBJECT_LOID_PN_INDEX_ID;

/// `Anum_pg_largeobject_loid`.
const Anum_pg_largeobject_loid: AttrNumber = ANUM_PG_LARGEOBJECT_LOID;
/// `Anum_pg_largeobject_pageno`.
const Anum_pg_largeobject_pageno: AttrNumber = ANUM_PG_LARGEOBJECT_PAGENO;

/// `INV_WRITE` (`libpq/libpq-fs.h`).
const INV_WRITE: i32 = 0x0002_0000;
/// `INV_READ` (`libpq/libpq-fs.h`).
const INV_READ: i32 = 0x0004_0000;

/// `SEEK_SET` (`<stdio.h>`).
const SEEK_SET: i32 = 0;
/// `SEEK_CUR` (`<stdio.h>`).
const SEEK_CUR: i32 = 1;
/// `SEEK_END` (`<stdio.h>`).
const SEEK_END: i32 = 2;

/// `DROP_CASCADE`.
const DROP_CASCADE: DropBehavior = DropBehavior::Cascade;

/// `LOBLKSIZE` as a `uint64` (the page-arithmetic operand).
const LOBLKSIZE_U64: uint64 = LOBLKSIZE as uint64;

/// `lo_compat_privileges` (inv_api.c:56) — GUC backwards-compatibility flag to
/// suppress LO permission checks. The GUC machinery is unported; it defaults to
/// `false` (the boot value, `postgresql.conf` default), matching a server that
/// has not set it. When the GUC owner lands this becomes a real `bool` GUC.
fn lo_compat_privileges() -> bool {
    false
}

/// Allocate the `LOBLKSIZE`-byte scratch "page" the C declares as the
/// `workbuf.data[LOBLKSIZE + VARHDRSZ]` union, zero-initialised. The size is the
/// fixed `LOBLKSIZE`, never data-derived.
fn alloc_workbuf() -> Vec<u8> {
    vec![0u8; LOBLKSIZE as usize]
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_eq_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTGreaterEqualStrategyNumber, F_INT4GE,
/// Int32GetDatum(value))`.
fn int4_ge_key<'mcx>(attno: AttrNumber, value: i32) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTGreaterEqualStrategyNumber,
        F_INT4GE,
        Datum::from_i32(value),
    )?;
    Ok(key)
}

/* ===========================================================================
 * open_lo_relation (inv_api.c:72-91) / close_lo_relation (inv_api.c:96-122)
 * ========================================================================= */

/// `open_lo_relation()` — open `pg_largeobject` and its index for the current
/// operation.
///
/// The C static cache + `TopTransactionResourceOwner` ownership kludge is
/// replaced by a per-operation open in `mcx` (see the crate docs); the lock is
/// `RowExclusiveLock` "since we might either read or write".
fn open_lo_relation<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<(types_rel::Relation<'mcx>, types_rel::Relation<'mcx>)> {
    // lo_heap_r = table_open(LargeObjectRelationId, RowExclusiveLock);
    let lo_heap_r = table_open(mcx, LargeObjectRelationId, RowExclusiveLock)?;
    // lo_index_r = index_open(LargeObjectLOidPNIndexId, RowExclusiveLock);
    let lo_index_r = index_open::call(mcx, LargeObjectLOidPNIndexId, RowExclusiveLock)?;
    Ok((lo_heap_r, lo_index_r))
}

/// `close_lo_relation(isCommit)` — clean up at main transaction end.
///
/// In the owned model the heap/index references are opened and released
/// per-operation (the per-operation `NoLock` close retains the lock until
/// main-xact end, as C does), so there is no surviving static reference to close
/// here. The entry point is kept for the `xact.c` caller's contract.
pub fn close_lo_relation(_isCommit: bool) -> PgResult<()> {
    Ok(())
}

/* ===========================================================================
 * inv_create (inv_api.c:172-202)
 * ========================================================================= */

/// `inv_create` — create a new large object.
///
/// `lobjId` is the OID to use for the new large object, or `InvalidOid` to pick
/// one. Returns the OID of the new object. If `lobjId` is not `InvalidOid`, an
/// error occurs if the OID is already in use.
pub fn inv_create(lobjId: Oid) -> PgResult<Oid> {
    let lobjId_new: Oid;

    /*
     * Create a new largeobject with empty data pages
     */
    lobjId_new = backend_catalog_pg_largeobject::LargeObjectCreate(lobjId)?;

    /*
     * dependency on the owner of largeobject
     *
     * Note that LO dependencies are recorded using classId
     * LargeObjectRelationId for backwards-compatibility reasons.  Using
     * LargeObjectMetadataRelationId instead would simplify matters for the
     * backend, but it'd complicate pg_dump and possibly break other clients.
     */
    backend_catalog_pg_shdepend_seams::recordDependencyOnOwner::call(
        LargeObjectRelationId,
        lobjId_new,
        backend_utils_init_miscinit_seams::get_user_id::call(),
    )?;

    /* Post creation hook for new large object */
    InvokeObjectPostCreateHook(LargeObjectRelationId, lobjId_new, 0)?;

    /*
     * Advance command counter to make new tuple visible to later operations.
     */
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    Ok(lobjId_new)
}

/// `InvokeObjectPostCreateHook(classId, objectId, subId)` (`objectaccess.h`):
/// `if (object_access_hook) RunObjectPostCreateHook(classId, objectId, subId,
/// false)`.
fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: i32) -> PgResult<()> {
    if backend_catalog_objectaccess::object_access_hook_present() {
        backend_catalog_objectaccess::run_object_post_create_hook(classId, objectId, subId, false)?;
    }
    Ok(())
}

/* ===========================================================================
 * inv_open (inv_api.c:214-292)
 * ========================================================================= */

/// `inv_open` — access an existing large object.
///
/// Returns a large object descriptor, appropriately filled in. The descriptor
/// is returned boxed (the C allocates it in a caller-supplied memory context);
/// if it has a snapshot associated with it, the caller must ensure that snapshot
/// lives long enough.
pub fn inv_open(lobjId: Oid, flags: i32) -> PgResult<Box<LargeObjectDesc>> {
    let snapshot: Option<Rc<types_snapshot::SnapshotData>>;
    let mut descflags: i32 = 0;

    /*
     * Historically, no difference is made between (INV_WRITE) and (INV_WRITE
     * | INV_READ), the caller being allowed to read the large object
     * descriptor in either case.
     */
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
            .into_error());
    }

    /* Get snapshot.  If write is requested, use an instantaneous snapshot. */
    if descflags & IFS_WRLOCK != 0 {
        snapshot = None;
    } else {
        snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;
    }

    /* Can't use LargeObjectExists here because we need to specify snapshot */
    if !backend_catalog_pg_largeobject::LargeObjectExistsWithSnapshot(lobjId, snapshot.clone())? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("large object {lobjId} does not exist"))
            .into_error());
    }

    /* Apply permission checks, again specifying snapshot */
    if (descflags & IFS_RDLOCK) != 0
        && !lo_compat_privileges()
        && backend_catalog_aclchk_seams::pg_largeobject_aclcheck_snapshot::call(
            lobjId,
            backend_utils_init_miscinit_seams::get_user_id::call(),
            ACL_SELECT,
            snapshot.clone(),
        )? != ACLCHECK_OK
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for large object {lobjId}"))
            .into_error());
    }
    if (descflags & IFS_WRLOCK) != 0
        && !lo_compat_privileges()
        && backend_catalog_aclchk_seams::pg_largeobject_aclcheck_snapshot::call(
            lobjId,
            backend_utils_init_miscinit_seams::get_user_id::call(),
            ACL_UPDATE,
            snapshot.clone(),
        )? != ACLCHECK_OK
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for large object {lobjId}"))
            .into_error());
    }

    /* OK to create a descriptor */
    let retval = LargeObjectDesc {
        id: lobjId,
        offset: 0,
        flags: descflags,
        /* caller sets if needed, not used by the functions in this file */
        subid: InvalidSubTransactionId,
        /*
         * The snapshot (if any) is just the currently active snapshot.  The
         * caller will replace it with a longer-lived copy if needed.
         */
        snapshot,
    };

    Ok(Box::new(retval))
}

/* ===========================================================================
 * inv_close (inv_api.c:298-303)
 * ========================================================================= */

/// Closes a large object descriptor previously made by [`inv_open`], releasing
/// the long-term memory used by it (the C `pfree`; here `Drop` of the owned
/// descriptor).
pub fn inv_close(obj_desc: Box<LargeObjectDesc>) -> PgResult<()> {
    // Assert(PointerIsValid(obj_desc));  -- the owned value is always valid.
    // pfree(obj_desc);
    drop(obj_desc);
    Ok(())
}

/* ===========================================================================
 * inv_drop (inv_api.c:310-331)
 * ========================================================================= */

/// Destroys an existing large object (not to be confused with a descriptor!).
///
/// Note we expect the caller to have done any required permissions check. For
/// historical reasons, we always return 1 on success.
pub fn inv_drop(lobjId: Oid) -> PgResult<i32> {
    /*
     * Delete any comments and dependencies on the large object
     */
    // object.classId = LargeObjectRelationId;
    // object.objectId = lobjId;
    // object.objectSubId = 0;
    // performDeletion(&object, DROP_CASCADE, 0);
    backend_catalog_dependency_seams::perform_deletion::call(
        LargeObjectRelationId,
        lobjId,
        0,
        DROP_CASCADE,
        0,
    )?;

    /*
     * Advance command counter so that tuple removal will be seen by later
     * large-object operations in this transaction.
     */
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    /* For historical reasons, we always return 1 on success. */
    Ok(1)
}

/* ===========================================================================
 * inv_getsize (inv_api.c:339-385) — file-static
 * ========================================================================= */

/// Determine size of a large object.
///
/// NOTE: LOs can contain gaps, just like Unix files. We actually return the
/// offset of the last byte + 1.
fn inv_getsize(obj_desc: &LargeObjectDesc) -> PgResult<uint64> {
    let mut lastbyte: uint64 = 0;

    let ctx = MemoryContext::new("inv_getsize");
    let mcx = ctx.mcx();

    // open_lo_relation();
    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    // ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(obj_desc->id));
    let skey = [oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id)?];

    // sd = systable_beginscan_ordered(lo_heap_r, lo_index_r, obj_desc->snapshot,
    //                                 1, skey);
    let mut sd = genam::systable_beginscan_ordered::call(
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.as_deref(),
        &skey,
    )?;

    /*
     * Because the pg_largeobject index is on both loid and pageno, but we
     * constrain only loid, a backwards scan should visit all pages of the large
     * object in reverse pageno order.  So, it's sufficient to examine the first
     * valid tuple (== last valid page).
     */
    // tuple = systable_getnext_ordered(sd, BackwardScanDirection);
    if let Some(tuple) = genam::systable_getnext_ordered::call(
        mcx,
        sd.desc_mut(),
        ScanDirection::BackwardScanDirection,
    )? {
        // if (HeapTupleHasNulls(tuple)) elog(ERROR, "null field found ...");
        // data = (Form_pg_largeobject) GETSTRUCT(tuple);
        // getdatafield(data, &datafield, &len, &pfreeit);
        let data = indexing::deform_lo_page::call(&lo_heap_r, &tuple)?;
        let len = data.data.len() as int64;
        // lastbyte = (uint64) data->pageno * LOBLKSIZE + len;
        lastbyte = data.pageno as uint64 * LOBLKSIZE_U64 + len as uint64;
        // if (pfreeit) pfree(datafield);  -- seam owns the detoasted buffer.
    }

    // systable_endscan_ordered(sd);
    sd.end()?;

    lo_index_r.close(NoLock)?;
    lo_heap_r.close(NoLock)?;

    Ok(lastbyte)
}

/* ===========================================================================
 * inv_seek (inv_api.c:387-434)
 * ========================================================================= */

/// `inv_seek` — reposition the descriptor's seek offset.
pub fn inv_seek(obj_desc: &mut LargeObjectDesc, offset: int64, whence: i32) -> PgResult<int64> {
    let newoffset: int64;

    /*
     * We allow seek/tell if you have either read or write permission, so no
     * need for a permission check here.
     *
     * Note: overflow in the additions is possible, but since we will reject
     * negative results, we don't need any extra test for that.
     */
    match whence {
        SEEK_SET => {
            newoffset = offset;
        }
        SEEK_CUR => {
            newoffset = obj_desc.offset as int64 + offset;
        }
        SEEK_END => {
            newoffset = inv_getsize(obj_desc)? as int64 + offset;
        }
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid whence setting: {whence}"))
                .into_error());
        }
    }

    /*
     * use errmsg_internal here because we don't want to expose INT64_FORMAT in
     * translatable strings; doing better is not worth the trouble
     */
    if newoffset < 0 || newoffset > MAX_LARGE_OBJECT_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg_internal(format!("invalid large object seek target: {newoffset}"))
            .into_error());
    }

    obj_desc.offset = newoffset as uint64;
    Ok(newoffset)
}

/* ===========================================================================
 * inv_tell (inv_api.c:436-447)
 * ========================================================================= */

/// `inv_tell` — return the descriptor's current seek offset.
pub fn inv_tell(obj_desc: &LargeObjectDesc) -> PgResult<int64> {
    /*
     * We allow seek/tell if you have either read or write permission, so no
     * need for a permission check here.
     */
    Ok(obj_desc.offset as int64)
}

/* ===========================================================================
 * inv_read (inv_api.c:449-540)
 * ========================================================================= */

/// `inv_read` — read up to `buf.len()` bytes from the large object into `buf`.
///
/// Returns the number of bytes read. (The C signature `(char *buf, int nbytes)`
/// is the owned slice `buf: &mut [u8]`; `nbytes == buf.len()`.)
pub fn inv_read(obj_desc: &mut LargeObjectDesc, buf: &mut [u8]) -> PgResult<i32> {
    let nbytes: i32 = buf.len() as i32;
    let mut nread: i32 = 0;
    let mut n: int64;
    let mut off: int64;
    let mut len: i32;
    let pageno: i32 = (obj_desc.offset / LOBLKSIZE_U64) as i32;
    let mut pageoff: uint64;

    if (obj_desc.flags & IFS_RDLOCK) == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for large object {}", obj_desc.id))
            .into_error());
    }

    if nbytes <= 0 {
        return Ok(0);
    }

    let ctx = MemoryContext::new("inv_read");
    let mcx = ctx.mcx();

    // open_lo_relation();
    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    // ScanKeyInit loid = id (F_OIDEQ); pageno >= pageno (F_INT4GE).
    let skey = [
        oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id)?,
        int4_ge_key(Anum_pg_largeobject_pageno, pageno)?,
    ];

    // sd = systable_beginscan_ordered(lo_heap_r, lo_index_r, obj_desc->snapshot,
    //                                 2, skey);
    let mut sd = genam::systable_beginscan_ordered::call(
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.as_deref(),
        &skey,
    )?;

    // while ((tuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL)
    while let Some(tuple) = genam::systable_getnext_ordered::call(
        mcx,
        sd.desc_mut(),
        ScanDirection::ForwardScanDirection,
    )? {
        // (HeapTupleHasNulls paranoia + GETSTRUCT happen in deform_lo_page.)
        let data = indexing::deform_lo_page::call(&lo_heap_r, &tuple)?;

        /*
         * We expect the indexscan will deliver pages in order.  However, there
         * may be missing pages if the LO contains unwritten "holes". We want
         * missing sections to read out as zeroes.
         */
        // pageoff = ((uint64) data->pageno) * LOBLKSIZE;
        pageoff = data.pageno as uint64 * LOBLKSIZE_U64;
        if pageoff > obj_desc.offset {
            n = (pageoff - obj_desc.offset) as int64;
            n = if n <= (nbytes - nread) as int64 {
                n
            } else {
                (nbytes - nread) as int64
            };
            // MemSet(buf + nread, 0, n);
            for b in buf.iter_mut().skip(nread as usize).take(n as usize) {
                *b = 0;
            }
            nread += n as i32;
            obj_desc.offset += n as uint64;
        }

        if nread < nbytes {
            // Assert(obj_desc->offset >= pageoff);
            debug_assert!(obj_desc.offset >= pageoff);
            off = (obj_desc.offset - pageoff) as int64;
            // Assert(off >= 0 && off < LOBLKSIZE);
            debug_assert!(off >= 0 && off < LOBLKSIZE as int64);

            // getdatafield(data, &datafield, &len, &pfreeit);  -- in deform.
            len = data.data.len() as i32;
            if len > off as i32 {
                n = (len - off as i32) as int64;
                n = if n <= (nbytes - nread) as int64 {
                    n
                } else {
                    (nbytes - nread) as int64
                };
                // memcpy(buf + nread, VARDATA(datafield) + off, n);
                buf[nread as usize..(nread as usize + n as usize)]
                    .copy_from_slice(&data.data[off as usize..(off as usize + n as usize)]);
                nread += n as i32;
                obj_desc.offset += n as uint64;
            }
            // if (pfreeit) pfree(datafield);  -- seam owns detoasted buffer.
        }

        if nread >= nbytes {
            break;
        }
    }

    // systable_endscan_ordered(sd);
    sd.end()?;

    lo_index_r.close(NoLock)?;
    lo_heap_r.close(NoLock)?;

    Ok(nread)
}

/* ===========================================================================
 * inv_write (inv_api.c:542-737)
 * ========================================================================= */

/// `inv_write` — write `buf.len()` bytes from `buf` into the large object at the
/// descriptor's current offset.
///
/// Returns the number of bytes written. (The C signature
/// `(const char *buf, int nbytes)` is the owned slice `buf: &[u8]`;
/// `nbytes == buf.len()`.)
pub fn inv_write(obj_desc: &mut LargeObjectDesc, buf: &[u8]) -> PgResult<i32> {
    let nbytes: i32 = buf.len() as i32;
    let mut nwritten: i32 = 0;
    let mut n: i32;
    let mut off: i32;
    let mut len: i32;
    let mut pageno: i32 = (obj_desc.offset / LOBLKSIZE_U64) as i32;
    // workbuf: the LOBLKSIZE-sized scratch page assembled here as a Vec<u8>;
    // the varlena framing (`SET_VARSIZE`) happens in the indexing seam.
    let mut workb: Vec<u8> = alloc_workbuf();
    let mut neednextpage: bool;
    let indstate: types_cluster::CatalogIndexStateToken;

    /* enforce writability because snapshot is probably wrong otherwise */
    if (obj_desc.flags & IFS_WRLOCK) == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for large object {}", obj_desc.id))
            .into_error());
    }

    if nbytes <= 0 {
        return Ok(0);
    }

    /* this addition can't overflow because nbytes is only int32 */
    if (nbytes as int64 + obj_desc.offset as int64) > MAX_LARGE_OBJECT_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid large object write request size: {nbytes}"))
            .into_error());
    }

    let ctx = MemoryContext::new("inv_write");
    let mcx = ctx.mcx();

    // open_lo_relation();
    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    // indstate = CatalogOpenIndexes(lo_heap_r);
    indstate = indexing::catalog_open_indexes::call(mcx, &lo_heap_r)?;

    // ScanKeyInit loid = id; pageno >= pageno.
    let skey = [
        oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id)?,
        int4_ge_key(Anum_pg_largeobject_pageno, pageno)?,
    ];

    // sd = systable_beginscan_ordered(lo_heap_r, lo_index_r, obj_desc->snapshot,
    //                                 2, skey);
    let mut sd = genam::systable_beginscan_ordered::call(
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.as_deref(),
        &skey,
    )?;

    // oldtuple = NULL; olddata = NULL; neednextpage = true;
    let mut olddata: Option<indexing::LoPageRow> = None;
    neednextpage = true;

    while nwritten < nbytes {
        /*
         * If possible, get next pre-existing page of the LO.  We expect the
         * indexscan will deliver these in order --- but there may be holes.
         */
        if neednextpage {
            // if ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL)
            if let Some(oldtuple) = genam::systable_getnext_ordered::call(
                mcx,
                sd.desc_mut(),
                ScanDirection::ForwardScanDirection,
            )? {
                // (HeapTupleHasNulls paranoia + GETSTRUCT happen in deform.)
                // olddata = (Form_pg_largeobject) GETSTRUCT(oldtuple);
                let row = indexing::deform_lo_page::call(&lo_heap_r, &oldtuple)?;
                // Assert(olddata->pageno >= pageno);
                debug_assert!(row.pageno >= pageno);
                olddata = Some(row);
            } else {
                olddata = None;
            }
            neednextpage = false;
        }

        /*
         * If we have a pre-existing page, see if it is the page we want to
         * write, or a later one.
         */
        if olddata.as_ref().is_some_and(|o| o.pageno == pageno) {
            let old = olddata.take().expect("olddata is Some");

            /*
             * Update an existing page with fresh data.
             *
             * First, load old data into workbuf
             */
            // getdatafield(olddata, &datafield, &len, &pfreeit);
            // memcpy(workb, VARDATA(datafield), len);
            len = old.data.len() as i32;
            workb[..len as usize].copy_from_slice(&old.data);

            /*
             * Fill any hole
             */
            off = (obj_desc.offset % LOBLKSIZE_U64) as i32;
            if off > len {
                // MemSet(workb + len, 0, off - len);
                for b in workb.iter_mut().take(off as usize).skip(len as usize) {
                    *b = 0;
                }
            }

            /*
             * Insert appropriate portion of new data
             */
            n = LOBLKSIZE - off;
            n = if n <= (nbytes - nwritten) {
                n
            } else {
                nbytes - nwritten
            };
            // memcpy(workb + off, buf + nwritten, n);
            workb[off as usize..(off as usize + n as usize)]
                .copy_from_slice(&buf[nwritten as usize..(nwritten as usize + n as usize)]);
            nwritten += n;
            obj_desc.offset += n as uint64;
            off += n;
            /* compute valid length of new page */
            len = if len >= off { len } else { off };
            // SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ);  -- in seam.

            /*
             * Form and insert updated tuple
             */
            // values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);
            // replace[Anum_pg_largeobject_data - 1] = true;
            // newtup = heap_modify_tuple(oldtuple, ..., values, nulls, replace);
            // CatalogTupleUpdateWithInfo(lo_heap_r, &newtup->t_self, newtup, indstate);
            indexing::catalog_tuple_update_with_info_pg_largeobject::call(
                &lo_heap_r,
                old.tid,
                &workb[..len as usize],
                &indstate,
            )?;
            // heap_freetuple(newtup);  -- seam owns the tuple memory.

            /*
             * We're done with this old page.
             */
            olddata = None;
            neednextpage = true;
        } else {
            /*
             * Write a brand new page.
             *
             * First, fill any hole
             */
            off = (obj_desc.offset % LOBLKSIZE_U64) as i32;
            if off > 0 {
                // MemSet(workb, 0, off);
                for b in workb.iter_mut().take(off as usize) {
                    *b = 0;
                }
            }

            /*
             * Insert appropriate portion of new data
             */
            n = LOBLKSIZE - off;
            n = if n <= (nbytes - nwritten) {
                n
            } else {
                nbytes - nwritten
            };
            // memcpy(workb + off, buf + nwritten, n);
            workb[off as usize..(off as usize + n as usize)]
                .copy_from_slice(&buf[nwritten as usize..(nwritten as usize + n as usize)]);
            nwritten += n;
            obj_desc.offset += n as uint64;
            /* compute valid length of new page */
            len = off + n;
            // SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ);  -- in seam.

            /*
             * Form and insert updated tuple
             */
            // values[loid] = id; values[pageno] = pageno; values[data] = &workbuf;
            // newtup = heap_form_tuple(lo_heap_r->rd_att, values, nulls);
            // CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate);
            indexing::catalog_tuple_insert_with_info_pg_largeobject::call(
                &lo_heap_r,
                obj_desc.id,
                pageno,
                &workb[..len as usize],
                &indstate,
            )?;
            // heap_freetuple(newtup);  -- seam owns the tuple memory.
        }
        pageno += 1;
    }

    // systable_endscan_ordered(sd);
    sd.end()?;

    // CatalogCloseIndexes(indstate);
    indexing::catalog_close_indexes::call(indstate)?;

    lo_index_r.close(NoLock)?;
    lo_heap_r.close(NoLock)?;

    /*
     * Advance command counter so that my tuple updates will be seen by later
     * large-object operations in this transaction.
     */
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    Ok(nwritten)
}

/* ===========================================================================
 * inv_truncate (inv_api.c:739-915)
 * ========================================================================= */

/// `inv_truncate` — truncate the large object to `len` bytes.
pub fn inv_truncate(obj_desc: &mut LargeObjectDesc, len: int64) -> PgResult<()> {
    let pageno: i32 = (len / LOBLKSIZE as int64) as i32;
    let off: i32;
    // workbuf scratch page as a Vec<u8>; varlena framing in the seam.
    let mut workb: Vec<u8> = alloc_workbuf();
    let indstate: types_cluster::CatalogIndexStateToken;

    /* enforce writability because snapshot is probably wrong otherwise */
    if (obj_desc.flags & IFS_WRLOCK) == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied for large object {}", obj_desc.id))
            .into_error());
    }

    /*
     * use errmsg_internal here because we don't want to expose INT64_FORMAT in
     * translatable strings; doing better is not worth the trouble
     */
    if len < 0 || len > MAX_LARGE_OBJECT_SIZE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg_internal(format!("invalid large object truncation target: {len}"))
            .into_error());
    }

    let ctx = MemoryContext::new("inv_truncate");
    let mcx = ctx.mcx();

    // open_lo_relation();
    let (lo_heap_r, lo_index_r) = open_lo_relation(mcx)?;

    // indstate = CatalogOpenIndexes(lo_heap_r);
    indstate = indexing::catalog_open_indexes::call(mcx, &lo_heap_r)?;

    /*
     * Set up to find all pages with desired loid and pageno >= target
     */
    let skey = [
        oid_eq_key(Anum_pg_largeobject_loid, obj_desc.id)?,
        int4_ge_key(Anum_pg_largeobject_pageno, pageno)?,
    ];

    // sd = systable_beginscan_ordered(lo_heap_r, lo_index_r, obj_desc->snapshot,
    //                                 2, skey);
    let mut sd = genam::systable_beginscan_ordered::call(
        &lo_heap_r,
        &lo_index_r,
        obj_desc.snapshot.as_deref(),
        &skey,
    )?;

    /*
     * If possible, get the page the truncation point is in. The truncation
     * point may be beyond the end of the LO or in a hole.
     */
    // olddata = NULL;
    // if ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL)
    let olddata: Option<indexing::LoPageRow> = match genam::systable_getnext_ordered::call(
        mcx,
        sd.desc_mut(),
        ScanDirection::ForwardScanDirection,
    )? {
        Some(oldtuple) => {
            let row = indexing::deform_lo_page::call(&lo_heap_r, &oldtuple)?;
            // Assert(olddata->pageno >= pageno);
            debug_assert!(row.pageno >= pageno);
            Some(row)
        }
        None => None,
    };

    /*
     * If we found the page of the truncation point we need to truncate the data
     * in it.  Otherwise if we're in a hole, we need to create a page to mark the
     * end of data.
     */
    if olddata.as_ref().is_some_and(|o| o.pageno == pageno) {
        let old = olddata.as_ref().expect("olddata is Some");

        /* First, load old data into workbuf */
        let pagelen: i32;

        // getdatafield(olddata, &datafield, &pagelen, &pfreeit);
        // memcpy(workb, VARDATA(datafield), pagelen);
        pagelen = old.data.len() as i32;
        workb[..pagelen as usize].copy_from_slice(&old.data);

        /*
         * Fill any hole
         */
        off = (len % LOBLKSIZE as int64) as i32;
        if off > pagelen {
            // MemSet(workb + pagelen, 0, off - pagelen);
            for b in workb.iter_mut().take(off as usize).skip(pagelen as usize) {
                *b = 0;
            }
        }

        /* compute length of new page */
        // SET_VARSIZE(&workbuf.hdr, off + VARHDRSZ);  -- in seam.

        /*
         * Form and insert updated tuple
         */
        // newtup = heap_modify_tuple(oldtuple, ..., values, nulls, replace);
        // CatalogTupleUpdateWithInfo(lo_heap_r, &newtup->t_self, newtup, indstate);
        indexing::catalog_tuple_update_with_info_pg_largeobject::call(
            &lo_heap_r,
            old.tid,
            &workb[..off as usize],
            &indstate,
        )?;
    } else {
        /*
         * If the first page we found was after the truncation point, we're in a
         * hole that we'll fill, but we need to delete the later page because the
         * loop below won't visit it again.
         */
        if let Some(ref old) = olddata {
            // Assert(olddata->pageno > pageno);
            debug_assert!(old.pageno > pageno);
            // CatalogTupleDelete(lo_heap_r, &oldtuple->t_self);
            indexing::catalog_tuple_delete::call(&lo_heap_r, old.tid)?;
        }

        /*
         * Write a brand new page.
         *
         * Fill the hole up to the truncation point
         */
        off = (len % LOBLKSIZE as int64) as i32;
        if off > 0 {
            // MemSet(workb, 0, off);
            for b in workb.iter_mut().take(off as usize) {
                *b = 0;
            }
        }

        /* compute length of new page */
        // SET_VARSIZE(&workbuf.hdr, off + VARHDRSZ);  -- in seam.

        /*
         * Form and insert new tuple
         */
        // newtup = heap_form_tuple(lo_heap_r->rd_att, values, nulls);
        // CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate);
        indexing::catalog_tuple_insert_with_info_pg_largeobject::call(
            &lo_heap_r,
            obj_desc.id,
            pageno,
            &workb[..off as usize],
            &indstate,
        )?;
    }

    /*
     * Delete any pages after the truncation point.  If the initial search didn't
     * find a page, then of course there's nothing more to do.
     */
    if olddata.is_some() {
        // while ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL)
        while let Some(oldtuple) = genam::systable_getnext_ordered::call(
            mcx,
            sd.desc_mut(),
            ScanDirection::ForwardScanDirection,
        )? {
            let row = indexing::deform_lo_page::call(&lo_heap_r, &oldtuple)?;
            // CatalogTupleDelete(lo_heap_r, &oldtuple->t_self);
            indexing::catalog_tuple_delete::call(&lo_heap_r, row.tid)?;
        }
    }

    // systable_endscan_ordered(sd);
    sd.end()?;

    // CatalogCloseIndexes(indstate);
    indexing::catalog_close_indexes::call(indstate)?;

    lo_index_r.close(NoLock)?;
    lo_heap_r.close(NoLock)?;

    /*
     * Advance command counter so that tuple updates will be seen by later
     * large-object operations in this transaction.
     */
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    Ok(())
}

/// Install this unit's outward-consumed `inv_*` / `close_lo_relation` seams
/// (called by `be-fsstubs` / the large-object SQL functions / `xact.c`). Wired
/// into `seams-init`'s `init_all`.
pub fn init_seams() {
    use backend_storage_large_object_seams as seams;
    seams::close_lo_relation::set(close_lo_relation);
    seams::inv_create::set(inv_create);
    seams::inv_open::set(inv_open);
    seams::inv_close::set(inv_close);
    seams::inv_drop::set(inv_drop);
    seams::inv_seek::set(inv_seek);
    seams::inv_tell::set(inv_tell);
    seams::inv_read::set(inv_read);
    seams::inv_write::set(inv_write);
    seams::inv_truncate::set(inv_truncate);
}

#[cfg(test)]
mod tests {
    //! Seam-free unit tests for the pure-arithmetic / branch-order paths of
    //! inv_api.c: the flag→descflags mapping, the seek/tell offset arithmetic
    //! and `MAX_LARGE_OBJECT_SIZE` bounds, the zero-length read/write no-ops,
    //! and the permission / parameter / size-limit error codes. (The catalog
    //! scan/insert/update paths cross uninstalled owner seams and would panic;
    //! those are covered against the real owners once indexing.c lands.)

    use super::*;

    fn desc(flags: i32, offset: uint64) -> LargeObjectDesc {
        LargeObjectDesc {
            id: 1234,
            snapshot: None,
            subid: InvalidSubTransactionId,
            offset,
            flags,
        }
    }

    #[test]
    fn inv_open_zero_flags_is_invalid_parameter() {
        let err = inv_open(1, 0).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn inv_tell_returns_offset() {
        let d = desc(IFS_RDLOCK, 42);
        assert_eq!(inv_tell(&d).unwrap(), 42);
    }

    #[test]
    fn inv_seek_set_cur_and_bounds() {
        // SEEK_SET sets the absolute offset.
        let mut d = desc(IFS_RDLOCK, 100);
        assert_eq!(inv_seek(&mut d, 10, SEEK_SET).unwrap(), 10);
        assert_eq!(d.offset, 10);

        // SEEK_CUR adds to the current offset.
        assert_eq!(inv_seek(&mut d, 5, SEEK_CUR).unwrap(), 15);
        assert_eq!(d.offset, 15);

        // Negative result is rejected (and the offset is unchanged).
        let err = inv_seek(&mut d, -100, SEEK_SET).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(d.offset, 15);

        // Beyond MAX_LARGE_OBJECT_SIZE is rejected.
        let err = inv_seek(&mut d, MAX_LARGE_OBJECT_SIZE + 1, SEEK_SET).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

        // Exactly MAX_LARGE_OBJECT_SIZE is allowed.
        assert_eq!(
            inv_seek(&mut d, MAX_LARGE_OBJECT_SIZE, SEEK_SET).unwrap(),
            MAX_LARGE_OBJECT_SIZE
        );
    }

    #[test]
    fn inv_seek_invalid_whence() {
        let mut d = desc(IFS_RDLOCK, 0);
        let err = inv_seek(&mut d, 0, 99).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn inv_read_requires_rdlock() {
        let mut d = desc(IFS_WRLOCK, 0); // write-only, no read lock
        let mut buf = [0u8; 8];
        let err = inv_read(&mut d, &mut buf).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    }

    #[test]
    fn inv_read_zero_length_is_noop() {
        // nbytes <= 0 returns 0 before opening any relation (no seam touched).
        let mut d = desc(IFS_RDLOCK, 0);
        let mut buf: [u8; 0] = [];
        assert_eq!(inv_read(&mut d, &mut buf).unwrap(), 0);
    }

    #[test]
    fn inv_write_requires_wrlock() {
        let mut d = desc(IFS_RDLOCK, 0); // read-only
        let err = inv_write(&mut d, b"data").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    }

    #[test]
    fn inv_write_zero_length_is_noop() {
        let mut d = desc(IFS_WRLOCK, 0);
        assert_eq!(inv_write(&mut d, b"").unwrap(), 0);
    }

    #[test]
    fn inv_write_request_too_large() {
        let mut d = desc(IFS_WRLOCK, MAX_LARGE_OBJECT_SIZE as uint64);
        let err = inv_write(&mut d, b"x").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn inv_truncate_requires_wrlock() {
        let mut d = desc(IFS_RDLOCK, 0);
        let err = inv_truncate(&mut d, 0).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    }

    #[test]
    fn inv_truncate_negative_and_over_max() {
        let mut d = desc(IFS_WRLOCK, 0);
        assert_eq!(
            inv_truncate(&mut d, -1).unwrap_err().sqlstate(),
            ERRCODE_INVALID_PARAMETER_VALUE
        );
        assert_eq!(
            inv_truncate(&mut d, MAX_LARGE_OBJECT_SIZE + 1)
                .unwrap_err()
                .sqlstate(),
            ERRCODE_INVALID_PARAMETER_VALUE
        );
    }

    #[test]
    fn close_lo_relation_is_noop() {
        assert!(close_lo_relation(true).is_ok());
        assert!(close_lo_relation(false).is_ok());
    }

    #[test]
    fn constants_match_c_headers() {
        // LOBLKSIZE = BLCKSZ / 4 = 8192 / 4 = 2048.
        assert_eq!(LOBLKSIZE, 2048);
        // MAX_LARGE_OBJECT_SIZE = INT_MAX * LOBLKSIZE.
        assert_eq!(MAX_LARGE_OBJECT_SIZE, i32::MAX as int64 * 2048);
        assert_eq!(INV_WRITE, 0x0002_0000);
        assert_eq!(INV_READ, 0x0004_0000);
        assert_eq!(IFS_RDLOCK, 1);
        assert_eq!(IFS_WRLOCK, 2);
    }
}
