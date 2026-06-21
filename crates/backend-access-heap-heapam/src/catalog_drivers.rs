//! Three cross-family heap-AM driver seams that batch a small amount of
//! catalog-scan / tuple-form vocabulary the heap owner already has the
//! substrate for, but whose *callers* live in not-yet-ported units
//! (bootstrap.c, cluster.c) or in a different access method (genam.c):
//!
//!   * [`index_compute_xid_horizon_for_tuples`] — genam.c's AM-generic
//!     `table_index_delete_tuples()` shim (`access/index/genam.c:295`). The
//!     heap owner already installs `heap_index_delete_tuples`
//!     (`table_index_delete_tuples` for the heap AM), so this is the thin
//!     line-pointer -> `TM_IndexDeleteOp` build + dispatch around it.
//!   * [`read_pg_type`] — bootstrap.c `populate_typ_list()`
//!     (`bootstrap.c:726`): `table_open(TypeRelationId, NoLock)` +
//!     `table_beginscan_catalog` + `heap_getnext` loop + `GETSTRUCT` deform
//!     of each `pg_type` row into `(oid, FormData_pg_type)`.
//!   * [`scan_indisclustered`] — cluster.c `get_tables_to_cluster()`
//!     (`cluster.c:1643`): `table_open(IndexRelationId, AccessShareLock)` +
//!     `ScanKeyInit(indisclustered = true)` + `table_beginscan_catalog` +
//!     `heap_getnext` loop, returning each `(indrelid, indexrelid)`. The
//!     per-row ACL filter stays in the cluster.c caller.
//!   * [`insert_one_tuple`] — bootstrap.c `InsertOneTuple()`
//!     (`bootstrap.c:629`): `CreateTupleDesc(numattr, attrtypes)` +
//!     `heap_form_tuple` + `simple_heap_insert(boot_reldesc, tuple)`.

extern crate alloc;

use mcx::{Mcx, PgVec};
use types_core::primitive::{BlockNumber, Oid, OffsetNumber, TransactionId};
use types_error::PgResult;
use types_rel::Relation;
use types_storage::lock::{AccessShareLock, NoLock};
use types_storage::storage::Buffer;
use types_tuple::backend_access_common_heaptuple::{Datum as TupleDatum, DeformedColumn};
use types_tuple::heaptuple::{
    BlockIdData, FormData_pg_attribute, ItemPointerData, NameData,
};
use types_tuple::pg_type::FormData_pg_type;
use types_nbtree::{TmIndexDelete, TmIndexDeleteOp, TmIndexStatus};

use backend_access_common_heaptuple::{heap_deform_tuple, heap_form_tuple};
use backend_access_common_tupdesc::CreateTupleDesc;
use backend_access_heap_heapam_seams as heapam_seam;
use backend_access_table_table_seams as table_seam;
use backend_access_table_tableam::{table_beginscan_catalog, table_endscan};
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_page::{ItemIdIsDead, PageGetItem, PageGetItemId, PageRef};

use crate::scan::heap_getnext;

use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_scan::sdir::ScanDirection;

// Catalog OIDs / Anum constants (re-exported from the catalog type crate).
use types_catalog::pg_type::{
    Anum_pg_type_oid, Anum_pg_type_typalign, Anum_pg_type_typanalyze, Anum_pg_type_typarray,
    Anum_pg_type_typbasetype, Anum_pg_type_typbyval, Anum_pg_type_typcategory,
    Anum_pg_type_typcollation, Anum_pg_type_typdelim, Anum_pg_type_typelem,
    Anum_pg_type_typinput, Anum_pg_type_typisdefined, Anum_pg_type_typispreferred,
    Anum_pg_type_typlen, Anum_pg_type_typmodin, Anum_pg_type_typmodout, Anum_pg_type_typname,
    Anum_pg_type_typnamespace, Anum_pg_type_typndims, Anum_pg_type_typnotnull,
    Anum_pg_type_typoutput, Anum_pg_type_typowner, Anum_pg_type_typreceive,
    Anum_pg_type_typrelid, Anum_pg_type_typsend, Anum_pg_type_typstorage,
    Anum_pg_type_typsubscript, Anum_pg_type_typtype, Anum_pg_type_typtypmod, TypeRelationId,
};

// ===========================================================================
// index_compute_xid_horizon_for_tuples   (access/index/genam.c:295)
// ===========================================================================

/// `index_compute_xid_horizon_for_tuples(irel, hrel, ibuf, itemnos, nitems)`.
///
/// AM-generic `table_index_delete_tuples()` shim. Walks the index
/// line-pointers `itemnos` on the (share-locked, pinned) index buffer `ibuf`,
/// reads each index tuple's table TID (`itup->t_tid`, the leading
/// `ItemPointerData` of the page item), builds a known-deletable
/// `TM_IndexDeleteOp`, and dispatches to the heap AM's
/// `heap_index_delete_tuples` to compute the operation's
/// `snapshotConflictHorizon`.
pub fn index_compute_xid_horizon_for_tuples<'mcx>(
    irel: &Relation<'mcx>,
    hrel: &Relation<'mcx>,
    ibuf: Buffer,
    itemnos: &[OffsetNumber],
) -> PgResult<TransactionId> {
    debug_assert!(!itemnos.is_empty()); // Assert(nitems > 0)

    let nitems = itemnos.len();

    // A private arena for the delstate arrays + page snapshot (C palloc's the
    // deltids/status arrays in the caller's context; we use a scratch context
    // and clone the horizon scalar back out).
    let scratch = mcx::MemoryContext::new("index_compute_xid_horizon_for_tuples");
    let mcx = scratch.mcx();

    // ipage = BufferGetPage(ibuf);
    let page_bytes = bufmgr_seam::buffer_get_page::call(mcx, ibuf)?;
    let ipage = PageRef::new(&page_bytes)?;

    let iblknum: BlockNumber = bufmgr_seam::buffer_get_block_number::call(ibuf);

    let mut deltids: PgVec<TmIndexDelete> = PgVec::new_in(mcx);
    let mut status: PgVec<TmIndexStatus> = PgVec::new_in(mcx);

    // identify what the index tuples about to be deleted point to
    for &offnum in itemnos.iter() {
        // iitemid = PageGetItemId(ipage, offnum);
        let iitemid = PageGetItemId(&ipage, offnum)?;
        // Assert(ItemIdIsDead(iitemid));
        debug_assert!(ItemIdIsDead(&iitemid));
        // itup = (IndexTuple) PageGetItem(ipage, iitemid);
        let itup = PageGetItem(&ipage, &iitemid)?;
        // ItemPointerCopy(&itup->t_tid, &delstate.deltids[i].tid) — t_tid is the
        // leading 6 bytes (ItemPointerData) of the IndexTupleData header.
        let tid = read_item_pointer(itup);

        let id = deltids.len() as i16;
        deltids.push(TmIndexDelete { tid, id });
        status.push(TmIndexStatus {
            idxoffnum: offnum,
            knowndeletable: true, // LP_DEAD-marked
            promising: false,     // unused
            freespace: 0,         // unused
        });
    }

    let mut delstate = TmIndexDeleteOp {
        iblknum,
        bottomup: false,
        bottomupfreespace: 0,
        deltids,
        status,
    };

    // determine the actual xid horizon
    let snapshot_conflict_horizon =
        heapam_seam::heap_index_delete_tuples::call(mcx, hrel, &mut delstate)?;

    // assert tableam agrees that all items are deletable
    debug_assert_eq!(delstate.deltids.len(), nitems);

    // `irel` is consulted by C only for the descriptor identity carried into
    // delstate.irel; the heap AM reads it back for corruption reporting. Here
    // the heap AM seam carries `hrel` (the heap relation) and reaches `irel`
    // through the op's `iblknum`; the borrow keeps `irel` live for the call.
    let _ = irel;

    Ok(snapshot_conflict_horizon)
}

/// Read the leading `ItemPointerData` (6 bytes) of a page item — an index
/// tuple's `t_tid` (`IndexTupleData`'s first field). Mirrors C's `itup->t_tid`.
fn read_item_pointer(bytes: &[u8]) -> ItemPointerData {
    debug_assert!(bytes.len() >= 6);
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_ne_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_ne_bytes([bytes[4], bytes[5]]),
    }
}

// ===========================================================================
// read_pg_type / populate_typ_list   (bootstrap.c:726)
// ===========================================================================

/// `populate_typ_list()`'s catalog read: `table_open(TypeRelationId, NoLock)` +
/// `table_beginscan_catalog(rel, 0, NULL)` + `heap_getnext` loop +
/// `table_endscan` + `table_close`. Each row is `GETSTRUCT(tup)` cast to
/// `Form_pg_type`, copied into the result as `(oid, FormData_pg_type)`.
pub fn read_pg_type<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, (Oid, FormData_pg_type)>> {
    // rel = table_open(TypeRelationId, NoLock);
    let rel = table_seam::table_open::call(mcx, TypeRelationId, NoLock)?;

    // scan = table_beginscan_catalog(rel, 0, NULL);
    let mut scan = table_beginscan_catalog(mcx, &rel, 0, PgVec::new_in(mcx))?;

    let mut out: PgVec<(Oid, FormData_pg_type)> = PgVec::new_in(mcx);

    // while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
    while let Some(tup) = heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        // Form_pg_type typForm = (Form_pg_type) GETSTRUCT(tup);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let form = pg_type_form_from_columns(&cols)?;
        // newtyp->am_oid = typForm->oid; memcpy(&newtyp->am_typ, typForm, ...);
        out.push((form.oid, form));
    }

    // table_endscan(scan);
    table_endscan(scan)?;
    // table_close(rel, NoLock); — Relation::drop releases the relcache ref
    // (NoLock matches C's close lockmode).
    rel.close(NoLock)?;

    Ok(out)
}

/// `(Form_pg_type) GETSTRUCT(tup)` — project the fixed-width `pg_type` columns
/// (every field through `typcollation`) out of a deformed catalog tuple. All
/// of these columns are NOT NULL in `pg_type`, so each is read by value;
/// `typname` is the by-reference `NameData` image.
fn pg_type_form_from_columns(cols: &[DeformedColumn<'_>]) -> PgResult<FormData_pg_type> {
    Ok(FormData_pg_type {
        oid: col_oid(cols, Anum_pg_type_oid),
        typname: NameData {
            data: col_namedata(cols, Anum_pg_type_typname)?,
        },
        typnamespace: col_oid(cols, Anum_pg_type_typnamespace),
        typowner: col_oid(cols, Anum_pg_type_typowner),
        typlen: col_i16(cols, Anum_pg_type_typlen),
        typbyval: col_bool(cols, Anum_pg_type_typbyval),
        typtype: col_char(cols, Anum_pg_type_typtype),
        typcategory: col_char(cols, Anum_pg_type_typcategory),
        typispreferred: col_bool(cols, Anum_pg_type_typispreferred),
        typisdefined: col_bool(cols, Anum_pg_type_typisdefined),
        typdelim: col_char(cols, Anum_pg_type_typdelim),
        typrelid: col_oid(cols, Anum_pg_type_typrelid),
        typsubscript: col_oid(cols, Anum_pg_type_typsubscript),
        typelem: col_oid(cols, Anum_pg_type_typelem),
        typarray: col_oid(cols, Anum_pg_type_typarray),
        typinput: col_oid(cols, Anum_pg_type_typinput),
        typoutput: col_oid(cols, Anum_pg_type_typoutput),
        typreceive: col_oid(cols, Anum_pg_type_typreceive),
        typsend: col_oid(cols, Anum_pg_type_typsend),
        typmodin: col_oid(cols, Anum_pg_type_typmodin),
        typmodout: col_oid(cols, Anum_pg_type_typmodout),
        typanalyze: col_oid(cols, Anum_pg_type_typanalyze),
        typalign: col_char(cols, Anum_pg_type_typalign),
        typstorage: col_char(cols, Anum_pg_type_typstorage),
        typnotnull: col_bool(cols, Anum_pg_type_typnotnull),
        typbasetype: col_oid(cols, Anum_pg_type_typbasetype),
        typtypmod: col_i32(cols, Anum_pg_type_typtypmod),
        typndims: col_i32(cols, Anum_pg_type_typndims),
        typcollation: col_oid(cols, Anum_pg_type_typcollation),
    })
}

// ===========================================================================
// scan_indisclustered / get_tables_to_cluster   (cluster.c:1643)
// ===========================================================================

/// `table_open(IndexRelationId, AccessShareLock)` +
/// `ScanKeyInit(Anum_pg_index_indisclustered = true)` +
/// `table_beginscan_catalog(rel, 1, &entry)` + `heap_getnext` loop +
/// `table_endscan` + `relation_close`, returning each `(indrelid, indexrelid)`
/// of every `pg_index` row with `indisclustered`. The per-row aclcheck stays
/// in cluster.c's `get_tables_to_cluster`.
pub fn scan_indisclustered<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, (Oid, Oid)>> {
    use types_catalog::pg_index::{
        Anum_pg_index_indexrelid, Anum_pg_index_indisclustered, Anum_pg_index_indrelid,
        IndexRelationId,
    };
    use types_core::fmgr::{FmgrInfo, F_BOOLEQ};

    // indRelation = table_open(IndexRelationId, AccessShareLock);
    let rel = table_seam::table_open::call(mcx, IndexRelationId, AccessShareLock)?;

    // ScanKeyInit(&entry, Anum_pg_index_indisclustered, BTEqualStrategyNumber,
    //             F_BOOLEQ, BoolGetDatum(true));
    let mut entry = ScanKeyData::empty();
    entry.sk_flags = 0;
    entry.sk_attno = Anum_pg_index_indisclustered;
    entry.sk_strategy = BTEqualStrategyNumber;
    entry.sk_subtype = types_core::InvalidOid;
    entry.sk_collation = types_core::InvalidOid;
    entry.sk_func = FmgrInfo {
        fn_oid: F_BOOLEQ,
        ..Default::default()
    };
    entry.sk_argument = TupleDatum::from_bool(true);
    let mut key: PgVec<ScanKeyData> = PgVec::new_in(mcx);
    key.push(entry);

    // scan = table_beginscan_catalog(indRelation, 1, &entry);
    let mut scan = table_beginscan_catalog(mcx, &rel, 1, key)?;

    let mut out: PgVec<(Oid, Oid)> = PgVec::new_in(mcx);

    // while ((indexTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    while let Some(tup) = heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        // index = (Form_pg_index) GETSTRUCT(indexTuple);
        // (indrelid / indexrelid are the leading fixed-width oid columns.)
        let cols = heap_deform_tuple(mcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let indrelid = col_oid(&cols, Anum_pg_index_indrelid);
        let indexrelid = col_oid(&cols, Anum_pg_index_indexrelid);
        out.push((indrelid, indexrelid));
    }

    // table_endscan(scan);
    table_endscan(scan)?;
    // relation_close(indRelation, AccessShareLock);
    rel.close(AccessShareLock)?;

    Ok(out)
}

// ===========================================================================
// scan_typed_table_dependencies   (tablecmds.c:7094 find_typed_table_dependencies)
// ===========================================================================

/// The catalog-scan half of `find_typed_table_dependencies(typeOid, ...)`
/// (tablecmds.c:7094): `table_open(RelationRelationId, AccessShareLock)` +
/// `ScanKeyInit(Anum_pg_class_reloftype = typeOid)` +
/// `table_beginscan_catalog` + `heap_getnext(ForwardScanDirection)` loop +
/// `table_endscan` + `table_close`, returning the `oid` of every `pg_class`
/// row whose `reloftype` equals `typeOid` (the typed tables declared
/// `OF that_type`). The RESTRICT/CASCADE policy and the error stay in
/// tablecmds.c's `find_typed_table_dependencies` caller.
pub fn scan_typed_table_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
) -> PgResult<PgVec<'mcx, Oid>> {
    use types_catalog::pg_class::{Anum_pg_class_oid, Anum_pg_class_reloftype, RelationRelationId};
    use types_core::fmgr::{FmgrInfo, F_OIDEQ};

    // classRel = table_open(RelationRelationId, AccessShareLock);
    let rel = table_seam::table_open::call(mcx, RelationRelationId, AccessShareLock)?;

    // ScanKeyInit(&key[0], Anum_pg_class_reloftype, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(typeOid));
    let mut entry = ScanKeyData::empty();
    entry.sk_flags = 0;
    entry.sk_attno = Anum_pg_class_reloftype;
    entry.sk_strategy = BTEqualStrategyNumber;
    entry.sk_subtype = types_core::InvalidOid;
    entry.sk_collation = types_core::InvalidOid;
    entry.sk_func = FmgrInfo {
        fn_oid: F_OIDEQ,
        ..Default::default()
    };
    entry.sk_argument = TupleDatum::from_oid(type_oid);
    let mut key: PgVec<ScanKeyData> = PgVec::new_in(mcx);
    key.push(entry);

    // scan = table_beginscan_catalog(classRel, 1, key);
    let mut scan = table_beginscan_catalog(mcx, &rel, 1, key)?;

    let mut out: PgVec<Oid> = PgVec::new_in(mcx);

    // while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    while let Some(tup) = heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        // classform = (Form_pg_class) GETSTRUCT(tuple); result =
        // lappend_oid(result, classform->oid);
        let cols = heap_deform_tuple(mcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        out.push(col_oid(&cols, Anum_pg_class_oid));
    }

    // table_endscan(scan);
    table_endscan(scan)?;
    // table_close(classRel, AccessShareLock);
    rel.close(AccessShareLock)?;

    Ok(out)
}

// ===========================================================================
// insert_one_tuple / InsertOneTuple   (bootstrap.c:629)
// ===========================================================================

/// `InsertOneTuple()`: `tupDesc = CreateTupleDesc(numattr, attrtypes); tuple =
/// heap_form_tuple(tupDesc, values, Nulls); simple_heap_insert(boot_reldesc,
/// tuple); heap_freetuple(tuple)`. The tuple is formed from the supplied
/// `attrtypes`/`values`/`nulls`, then inserted into `rel` (C's `boot_reldesc`).
pub fn insert_one_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attrtypes: &[FormData_pg_attribute],
    values: &[TupleDatum<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    // tupDesc = CreateTupleDesc(numattr, attrtypes);
    let tup_desc = CreateTupleDesc(mcx, attrtypes)?;

    // tuple = heap_form_tuple(tupDesc, values, Nulls);
    let mut tuple = heap_form_tuple(mcx, &tup_desc, values, nulls)
        .map_err(|e| types_error::PgError::error(alloc::format!("heap_form_tuple: {e:?}")))?;

    // pfree(tupDesc) — the owned descriptor drops at end of scope.
    // simple_heap_insert(boot_reldesc, tuple);
    crate::insert::simple_heap_insert(mcx, rel, &mut tuple)?;

    // heap_freetuple(tuple) — the owned tuple drops at end of scope.
    Ok(())
}

// ---------------------------------------------------------------------------
// GETSTRUCT column readers over a deformed catalog tuple (by-Anum, 1-based).
// ---------------------------------------------------------------------------

fn col_oid(cols: &[DeformedColumn<'_>], anum: i16) -> Oid {
    cols[(anum - 1) as usize].0.as_oid()
}
fn col_bool(cols: &[DeformedColumn<'_>], anum: i16) -> bool {
    cols[(anum - 1) as usize].0.as_bool()
}
fn col_char(cols: &[DeformedColumn<'_>], anum: i16) -> i8 {
    cols[(anum - 1) as usize].0.as_char()
}
fn col_i16(cols: &[DeformedColumn<'_>], anum: i16) -> i16 {
    cols[(anum - 1) as usize].0.as_i16()
}
fn col_i32(cols: &[DeformedColumn<'_>], anum: i16) -> i32 {
    cols[(anum - 1) as usize].0.as_i32()
}

/// Read a `NameData` (the by-reference `name` image) out of a deformed column.
/// C's `GETSTRUCT` exposes the embedded fixed 64-byte `NameData`; the deformed
/// by-reference image is the NUL-padded name bytes.
fn col_namedata(cols: &[DeformedColumn<'_>], anum: i16) -> PgResult<[u8; 64]> {
    let datum = &cols[(anum - 1) as usize].0;
    let bytes = datum.as_ref_bytes();
    let mut data = [0u8; 64];
    let n = bytes.len().min(64);
    data[..n].copy_from_slice(&bytes[..n]);
    Ok(data)
}
