#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

#[cfg(test)]
mod tests;

use std::rc::Rc;

use exectuples::FetchedHeapTuple;
use indexam::{IndexScanDescData, IndexScanOpaque};
use mcx::{Mcx, MemoryContext, PgVec};
use tableam::TableScanDesc;
use types_core::primitive::AttrNumber;
use types_core::xact::TransactionIdIsValid;
use types_core::{Oid, TransactionId};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TRANSACTION_STATE,
    ERRCODE_TRANSACTION_ROLLBACK, WARNING,
};
use types_rel::{AccessShareLock, NoLock, Relation, RelationData};
use types_scan::scankey::ScanKeyData;
use types_scan::sdir::{ForwardScanDirection, ScanDirection};
use types_slot::SlotData;
use types_snapshot::SnapshotData;
use types_tuple::itemptr::ItemPointerEquals;
use types_tuple::HeapTupleData;

pub fn init_seams() {
    genam_seams::systable_scan_catalog::set(systable_scan_catalog);
    genam_seams::build_index_value_description::set(build_index_value_description);
}

fn build_index_value_description(
    index_relation: &RelationData<'_>,
    values: &[datum::Datum],
    isnull: &[bool],
) -> PgResult<Option<String>> {
    let cx = MemoryContext::new("BuildIndexValueDescription");
    BuildIndexValueDescription(cx.mcx(), index_relation, values, isnull)
}

// The per-scan context stands in for C's CurrentMemoryContext palloc of the
// sysscan machinery; direct consumers use the Mcx-taking API instead.
fn systable_scan_catalog(
    relation: &RelationData<'_>,
    index_oid: Oid,
    index_ok: bool,
    keys: &[ScanKeyData],
    consume: &mut dyn FnMut(&HeapTupleData<'_>) -> PgResult<bool>,
) -> PgResult<bool> {
    let scan_cx = MemoryContext::new("systable_scan_catalog");
    scan_catalog(scan_cx.mcx(), relation.rd_id, index_oid, index_ok, keys, consume)
}

fn scan_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relid: Oid,
    index_oid: Oid,
    index_ok: bool,
    keys: &[ScanKeyData],
    consume: &mut dyn FnMut(&HeapTupleData<'_>) -> PgResult<bool>,
) -> PgResult<bool> {
    // The seam trimmed the caller's open handle to &RelationData; NoLock
    // re-acquires it (the analog of C aliasing the passed pointer).
    let heap_rel = relation_seams::relation_open::call(mcx, heap_relid, NoLock)?;
    let mut scan = systable_beginscan(mcx, &heap_rel, index_oid, index_ok, None, keys)?;
    let ordered = matches!(scan.arm, SysScanArm::Index { .. });
    loop {
        let Some(ntp) = systable_getnext(mcx, &mut scan)? else {
            break;
        };
        if !consume(ntp)? {
            break;
        }
    }
    systable_endscan(mcx, scan)?;
    heap_rel.close(NoLock)?;
    Ok(ordered)
}

pub fn RelationGetIndexScan<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
    opaque: IndexScanOpaque<'mcx>,
) -> PgResult<IndexScanDescData<'mcx>> {
    indexam::relation_get_index_scan(
        mcx,
        indexRelation,
        nkeys,
        norderbys,
        opaque,
        xact::TransactionStartedDuringRecovery(),
    )
}

// C IndexScanEnd dissolves: dropping the IndexScanDescData value is the pfree.

// genam.h SysScanDescData. C's irel/iscan/scan nullable-pointer trio is the
// closed heap-or-index arm (rule 4); descriptors held by value, no erasure.
pub enum SysScanArm<'mcx> {
    Index {
        irel: Relation<'mcx>,
        iscan: IndexScanDescData<'mcx>,
    },
    Heap {
        scan: TableScanDesc<'mcx>,
    },
}

pub struct SysScanDescData<'mcx> {
    pub heap_rel: Relation<'mcx>,
    pub slot: SlotData<'mcx>,
    // Some only when begin registered the catalog snapshot (caller passed None);
    // registered snapshots are snapmgr-owned ('static) per snapmgr_seams.
    pub snapshot: Option<Rc<SnapshotData<'static>>>,
    pub arm: SysScanArm<'mcx>,
}

pub type SysScanDesc<'mcx> = SysScanDescData<'mcx>;

pub fn systable_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexId: Oid,
    indexOK: bool,
    snapshot: Option<Rc<SnapshotData<'mcx>>>,
    key: &[ScanKeyData],
) -> PgResult<SysScanDescData<'mcx>> {
    let irel = if indexOK
        && !miscinit::IgnoreSystemIndexes()
        && !reindex_is_processing_index(indexId)
    {
        Some(indexam::index_open(mcx, indexId, AccessShareLock)?)
    } else {
        None
    };

    let slot = tableam::table_slot_create(mcx, heapRelation)?;
    let (snap, registered) = setup_snapshot(mcx, heapRelation.rd_id, snapshot)?;

    let nkeys = key.len() as i32;
    let arm = match irel {
        Some(irel) => {
            let idxkey = convert_scan_keys(mcx, &irel, key)?;
            let mut iscan =
                indexam::index_beginscan(mcx, heapRelation, &irel, snap, nkeys, 0)?;
            indexam::index_rescan(&mut iscan, Some(&idxkey), None)?;
            SysScanArm::Index { irel, iscan }
        }
        None => {
            // No syncscan on a forced catalog heapscan: wanted rows cluster at
            // the front, an unpredictable start point only hurts.
            let scan = tableam::table_beginscan_strat(
                mcx,
                heapRelation,
                Some(snap),
                nkeys,
                skey_slice_in(mcx, key)?,
                true,
                false,
            )?;
            SysScanArm::Heap { scan }
        }
    };

    if TransactionIdIsValid(xact::CheckXidAlive()) {
        xact::SetBsysscan(true);
    }

    Ok(SysScanDescData {
        heap_rel: heapRelation.alias(),
        slot,
        snapshot: registered,
        arm,
    })
}

/// Returned tuple borrows the scan's slot: valid until the next
/// `systable_getnext`/`systable_endscan`, exactly C's contract.
pub fn systable_getnext<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    sysscan: &'a mut SysScanDescData<'mcx>,
) -> PgResult<Option<&'a HeapTupleData<'mcx>>> {
    let SysScanDescData { arm, slot, .. } = sysscan;
    let found = match arm {
        SysScanArm::Index { iscan, .. } => {
            let found =
                indexam::index_getnext_slot(mcx, iscan, ForwardScanDirection, slot)?;
            if found && iscan.xs_recheck {
                return Err(lossy_sysscan());
            }
            found
        }
        SysScanArm::Heap { scan } => {
            tableam::table_scan_getnextslot(mcx, scan, ForwardScanDirection, slot)?
        }
    };

    handle_concurrent_abort()?;

    if !found {
        return Ok(None);
    }
    fetch_slot_tuple(mcx, slot).map(Some)
}

pub fn systable_recheck_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    sysscan: &mut SysScanDescData<'mcx>,
    tup: &HeapTupleData<'_>,
) -> PgResult<bool> {
    debug_assert!(slot_holds(&sysscan.slot, tup));

    let freshsnap = Some(snapmgr_seams::register_snapshot::call(
        snapmgr_seams::get_catalog_snapshot::call(sysscan.heap_rel.rd_id)?,
    )?);

    let result = tableam::table_tuple_satisfies_snapshot(
        &sysscan.heap_rel,
        &mut sysscan.slot,
        &freshsnap,
    )?;

    snapmgr_seams::unregister_snapshot::call(freshsnap.expect("registered above"));

    handle_concurrent_abort()?;

    Ok(result)
}

pub fn systable_endscan<'mcx>(mcx: Mcx<'mcx>, sysscan: SysScanDescData<'mcx>) -> PgResult<()> {
    let SysScanDescData {
        heap_rel: _,
        mut slot,
        snapshot,
        arm,
    } = sysscan;

    // ExecDropSingleTupleTableSlot: the clear releases any pin, the drop frees.
    exectuples::exec_clear_tuple(&mut slot, mcx);
    drop(slot);

    match arm {
        SysScanArm::Index { irel, iscan } => {
            indexam::index_endscan(iscan)?;
            indexam::index_close(irel, AccessShareLock)?;
        }
        SysScanArm::Heap { scan } => tableam::table_endscan(scan)?,
    }

    if let Some(snap) = snapshot {
        snapmgr_seams::unregister_snapshot::call(snap);
    }

    if TransactionIdIsValid(xact::CheckXidAlive()) {
        xact::SetBsysscan(false);
    }

    Ok(())
}

/// Unlike `systable_beginscan` the index is opened and locked by the caller.
pub fn systable_beginscan_ordered<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexRelation: &Relation<'mcx>,
    snapshot: Option<Rc<SnapshotData<'mcx>>>,
    key: &[ScanKeyData],
) -> PgResult<SysScanDescData<'mcx>> {
    if reindex_is_processing_index(indexRelation.rd_id) {
        return Err(reindex_in_progress(indexRelation));
    }
    if miscinit::IgnoreSystemIndexes() {
        elog::elog(
            WARNING,
            format!(
                "using index \"{}\" despite IgnoreSystemIndexes",
                indexRelation.name()
            ),
        )?;
    }

    let slot = tableam::table_slot_create(mcx, heapRelation)?;
    let (snap, registered) = setup_snapshot(mcx, heapRelation.rd_id, snapshot)?;

    let idxkey = convert_scan_keys(mcx, indexRelation, key)?;
    let mut iscan =
        indexam::index_beginscan(mcx, heapRelation, indexRelation, snap, key.len() as i32, 0)?;
    indexam::index_rescan(&mut iscan, Some(&idxkey), None)?;

    if TransactionIdIsValid(xact::CheckXidAlive()) {
        xact::SetBsysscan(true);
    }

    Ok(SysScanDescData {
        heap_rel: heapRelation.alias(),
        slot,
        snapshot: registered,
        arm: SysScanArm::Index {
            irel: indexRelation.alias(),
            iscan,
        },
    })
}

pub fn systable_getnext_ordered<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    sysscan: &'a mut SysScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<Option<&'a HeapTupleData<'mcx>>> {
    let SysScanDescData { arm, slot, .. } = sysscan;
    let SysScanArm::Index { iscan, .. } = arm else {
        panic!("systable_getnext_ordered on a heap scan (C Assert(sysscan->irel))");
    };

    let found = indexam::index_getnext_slot(mcx, iscan, direction, slot)?;
    if found && iscan.xs_recheck {
        return Err(lossy_sysscan());
    }

    handle_concurrent_abort()?;

    if !found {
        return Ok(None);
    }
    fetch_slot_tuple(mcx, slot).map(Some)
}

/// The caller closes the index relation itself (it opened it).
pub fn systable_endscan_ordered<'mcx>(
    mcx: Mcx<'mcx>,
    sysscan: SysScanDescData<'mcx>,
) -> PgResult<()> {
    let SysScanDescData {
        heap_rel: _,
        mut slot,
        snapshot,
        arm,
    } = sysscan;

    exectuples::exec_clear_tuple(&mut slot, mcx);
    drop(slot);

    let SysScanArm::Index { irel, iscan } = arm else {
        panic!("systable_endscan_ordered on a heap scan (C Assert(sysscan->irel))");
    };
    indexam::index_endscan(iscan)?;
    drop(irel);

    if let Some(snap) = snapshot {
        snapmgr_seams::unregister_snapshot::call(snap);
    }

    if TransactionIdIsValid(xact::CheckXidAlive()) {
        xact::SetBsysscan(false);
    }

    Ok(())
}

// The buffer stays pinned by the scan slot; the erased-lifetime view must not
// outlive the open scan.
fn slot_buffer_tuple<'any>(slot: &SlotData<'_>) -> (HeapTupleData<'any>, types_core::Buffer) {
    let SlotData::BufferHeap(s) = slot else {
        panic!("systable_inplace_update: non-buffer slot from catalog scan");
    };
    let src = s.base.tuple.as_ref().expect("stored scan tuple");
    // SAFETY: aliases the tuple pinned by the slot's buffer (see above).
    let view = unsafe {
        HeapTupleData::from_raw_parts(src.header_ptr(), src.t_len, src.t_self, src.t_tableOid)
    };
    (view, s.buffer)
}

/// C's begin/finish/cancel out-param protocol: `state` is the live scan.
pub fn systable_inplace_update_begin<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    indexId: Oid,
    indexOK: bool,
    key: &[ScanKeyData],
) -> PgResult<Option<(heaptuple::HeapTuple<'mcx>, SysScanDescData<'mcx>)>> {
    if xact::IsInParallelMode() {
        return Err(parallel_inplace_update());
    }

    // The C snapshot arg is Assert(NULL); begin advances the catalog snapshot.
    let mut retries = 0;
    loop {
        postgres_seams::check_for_interrupts::call()?;
        retries += 1;
        if retries > 10000 {
            return Err(too_many_overwrite_tries());
        }

        let mut scan = systable_beginscan(mcx, relation, indexId, indexOK, None, key)?;
        if systable_getnext(mcx, &mut scan)?.is_none() {
            systable_endscan(mcx, scan)?;
            return Ok(None);
        }

        let (oldtup, buffer) = slot_buffer_tuple(&scan.slot);
        let mut scan_opt = Some(scan);
        let locked = heapam::heap_inplace_lock(relation, &oldtup, buffer, &mut || {
            systable_endscan(mcx, scan_opt.take().expect("scan released twice"))
        })?;
        if locked {
            let copy = heaptuple::heap_copytuple(mcx, &oldtup)?;
            return Ok(Some((copy, scan_opt.take().expect("scan released on success"))));
        }
    }
}

pub fn systable_inplace_update_finish(
    mcx: Mcx<'_>,
    state: SysScanDescData<'_>,
    tuple: &HeapTupleData<'_>,
) -> PgResult<()> {
    let (oldtup, buffer) = slot_buffer_tuple(&state.slot);
    heapam::heap_inplace_update_and_unlock(mcx, &state.heap_rel, &oldtup, tuple, buffer)?;
    systable_endscan(mcx, state)
}

pub fn systable_inplace_update_cancel(
    mcx: Mcx<'_>,
    state: SysScanDescData<'_>,
) -> PgResult<()> {
    let (oldtup, buffer) = slot_buffer_tuple(&state.slot);
    heapam::heap_inplace_unlock(&state.heap_rel, &oldtup, buffer)?;
    systable_endscan(mcx, state)
}

pub fn BuildIndexValueDescription<'mcx>(
    mcx: Mcx<'mcx>,
    indexRelation: &RelationData<'_>,
    values: &[datum::Datum],
    isnull: &[bool],
) -> PgResult<Option<String>> {
    let indnkeyatts = indexRelation.indnkeyatts() as usize;
    let idxrec = indexRelation
        .rd_index
        .as_ref()
        .expect("BuildIndexValueDescription on non-index relation");
    let indrelid = idxrec.indrelid;

    if rls_seams::check_enable_rls::call(indrelid, types_core::InvalidOid, true)?
        == rls_seams::CheckEnableRls::RlsEnabled
    {
        return Ok(None);
    }

    let user = miscinit::GetUserId();
    let select = types_nodes::parsenodes::ACL_SELECT;
    if aclchk_seams::pg_class_aclcheck_ext::call(indrelid, user, select)?.0 != 0 {
        for keyno in 0..indnkeyatts {
            let attnum = idxrec.indkey[keyno];
            if attnum == 0
                || aclchk_seams::pg_attribute_aclcheck::call(indrelid, attnum, user, select)? != 0
            {
                return Ok(None);
            }
        }
    }

    let mut buf = String::from("(");
    match genam_seams::pg_get_indexdef_columns_keys_only::call(mcx, indexRelation.rd_id)? {
        Some(cols) => buf.push_str(&cols),
        None => return Ok(None),
    }
    buf.push_str(")=(");
    for i in 0..indnkeyatts {
        if i > 0 {
            buf.push_str(", ");
        }
        if isnull[i] {
            buf.push_str("null");
        } else {
            let (foutoid, _) = lsyscache::typ::getTypeOutputInfo(indexRelation.rd_opcintype[i])?;
            let mut finfo = fmgr_core::fmgr_info(foutoid)?;
            let out = fmgr_core::function_call1_coll_in(&mut finfo, 0, mcx, values[i])?;
            // SAFETY: output fns return a NUL-terminated cstring datum.
            let s = unsafe {
                core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char)
            }
            .to_bytes();
            buf.push_str(core::str::from_utf8(s).expect("type output is UTF-8"));
        }
    }
    buf.push(')');
    Ok(Some(buf))
}

// unported: needs tableam table_index_delete_tuples / heapam
// heap_index_delete_tuples (callers hash/gist LP_DEAD reuse). A clean 0A000
// instead of a panic: both callers reach this before any page or WAL
// mutation, so the error unwind is safe and only fails the triggering DML
// statement.
pub fn index_compute_xid_horizon_for_tuples(
    _irel: &Relation<'_>,
    _hrel: &Relation<'_>,
    _ibuf: types_core::primitive::Buffer,
    _itemnos: &[types_core::primitive::OffsetNumber],
) -> PgResult<TransactionId> {
    Err(Box::new(
        PgError::error(
            "reuse of dead index entries is not supported (index_compute_xid_horizon_for_tuples unported)",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}

// idxkey[i].sk_attno = j+1 where key[i].sk_attno == indkey[j].
fn convert_scan_keys<'mcx>(
    mcx: Mcx<'mcx>,
    irel: &Relation<'mcx>,
    key: &[ScanKeyData],
) -> PgResult<PgVec<'mcx, ScanKeyData>> {
    let indkey = &irel
        .rd_index
        .as_ref()
        .expect("systable scan index has no rd_index (C would deref NULL)")
        .indkey;

    let mut idxkey = skey_vec(mcx, key.len())?;
    for k in key {
        let Some(j) = indkey.iter().position(|&col| col == k.sk_attno) else {
            return Err(column_not_in_index());
        };
        let mut ik = k.clone();
        ik.sk_attno = (j + 1) as AttrNumber;
        idxkey.push(ik);
    }
    Ok(idxkey)
}

// ScanKeyData is droppy (sk_func.fn_extra), so the !needs_drop arena helpers
// refuse it; scan descriptors are rule-3 by-value owners, Drop is correct.
fn skey_vec<'mcx>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, ScanKeyData>> {
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(cap)
        .map_err(|_| Box::new(mcx.oom(cap * core::mem::size_of::<ScanKeyData>())))?;
    Ok(v)
}

fn skey_slice_in<'mcx>(
    mcx: Mcx<'mcx>,
    key: &[ScanKeyData],
) -> PgResult<PgVec<'mcx, ScanKeyData>> {
    let mut v = skey_vec(mcx, key.len())?;
    for k in key {
        v.push(k.clone());
    }
    Ok(v)
}

// Caller-provided snapshot: caller owns it (nothing to unregister). None:
// RegisterSnapshot(GetCatalogSnapshot(relid)), unregistered at endscan.
fn setup_snapshot<'mcx>(
    _mcx: Mcx<'mcx>,
    relid: Oid,
    snapshot: Option<Rc<SnapshotData<'mcx>>>,
) -> PgResult<(Rc<SnapshotData<'mcx>>, Option<Rc<SnapshotData<'static>>>)> {
    match snapshot {
        Some(s) => Ok((s, None)),
        None => {
            let s = snapmgr_seams::register_snapshot::call(
                snapmgr_seams::get_catalog_snapshot::call(relid)?,
            )?;
            Ok((s.clone(), Some(s)))
        }
    }
}

// ExecFetchSlotHeapTuple(slot, false, &shouldFree) + Assert(!shouldFree).
#[inline]
fn fetch_slot_tuple<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    slot: &'a mut SlotData<'mcx>,
) -> PgResult<&'a HeapTupleData<'mcx>> {
    match exectuples::exec_fetch_slot_heap_tuple(slot, false, mcx, mcx)? {
        FetchedHeapTuple::Slot(htup) => Ok(htup),
        FetchedHeapTuple::Copied(_) => non_heap_sysscan_slot(),
    }
}

fn slot_holds(slot: &SlotData<'_>, tup: &HeapTupleData<'_>) -> bool {
    let stored = match slot {
        SlotData::Heap(h) => h.tuple.as_ref(),
        SlotData::BufferHeap(b) => b.base.tuple.as_ref(),
        _ => None,
    };
    stored.is_some_and(|t| ItemPointerEquals(&t.t_self, &tup.t_self))
}

// Error out if CheckXidAlive aborted concurrently (logical decoding of an
// in-progress transaction); see the xact.c comments on CheckXidAlive/bsysscan.
#[inline]
fn handle_concurrent_abort() -> PgResult<()> {
    let xid = xact::CheckXidAlive();
    if TransactionIdIsValid(xid) {
        return concurrent_abort_check(xid);
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn concurrent_abort_check(xid: TransactionId) -> PgResult<()> {
    if !procarray_seams::transaction_id_is_in_progress::call(xid)?
        && !transam_seams::transaction_id_did_commit::call(xid)?
    {
        return Err(Box::new(
            PgError::error("transaction aborted during system catalog scan")
                .with_sqlstate(ERRCODE_TRANSACTION_ROLLBACK),
        ));
    }
    Ok(())
}

#[inline]
fn reindex_is_processing_index(indexId: Oid) -> bool {
    types_rel::reindex::ReindexIsProcessingIndex(indexId)
}

#[track_caller]
#[cold]
#[inline(never)]
fn reindex_in_progress(r: &RelationData<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "cannot access index \"{}\" while it is being reindexed",
            r.name()
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn column_not_in_index() -> Box<PgError> {
    Box::new(PgError::error("column is not in index"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn lossy_sysscan() -> Box<PgError> {
    Box::new(PgError::error(
        "system catalog scans with lossy index conditions are not implemented",
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn parallel_inplace_update() -> Box<PgError> {
    Box::new(
        PgError::error("cannot update tuples during a parallel operation")
            .with_sqlstate(ERRCODE_INVALID_TRANSACTION_STATE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_many_overwrite_tries() -> Box<PgError> {
    Box::new(PgError::error(
        "giving up after too many tries to overwrite row",
    ))
}

#[cold]
#[inline(never)]
fn non_heap_sysscan_slot() -> ! {
    panic!("systable slot is not heap/buffer-backed (C Assert(!shouldFree))")
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: {what}")
}
