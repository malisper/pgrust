//! `contrib/pg_visibility` — visibility map (VM) inspectors: per-block and
//! whole-relation VM bits, the PD_ALL_VISIBLE page bit, the VM-vs-heap
//! corruption checkers, and VM-fork truncation.
//!
//! C's read_stream prefetch in the collectors is not reproduced: blocks are
//! read sequentially with the same BAS_BULKREAD strategy (same buffers, same
//! lock protocol, same outputs).

#![allow(non_snake_case)]

use std::any::Any;

use datum::Datum;
use mcx::Mcx;
use types_core::{
    xact::{TransactionIdIsValid, TransactionIdPrecedes},
    BlockNumber, ForkNumber, InvalidBlockNumber, MaxBlockNumber, TransactionId,
};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_WRONG_OBJECT_TYPE};
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_rel::pg_class::RELKIND_HAS_TABLE_AM;
use types_snapshot::HTSV_Result;
use types_storage::buf::{Buffer, BufferAccessStrategyType};
use types_storage::bufpage::{LP_DEAD, LP_NORMAL, LP_REDIRECT, LP_UNUSED, PD_ALL_VISIBLE};
use types_tuple::htup::{HeapTupleData, HeapTupleHeaderData};
use types_tuple::itemptr::ItemPointerData;
use types_tuple::tupdesc::TupleDescData;
use visibilitymap::{
    visibilitymap_get_status, visibilitymap_prepare_truncate, vm_all_frozen, vm_all_visible,
    VmBuffer, VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
};

const LIBRARY: &str = "pg_visibility";
const BLCKSZ: usize = types_core::BLCKSZ;
const RM_SMGR_ID: u8 = 2;
const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;

#[track_caller]
#[cold]
fn invalid_block_err() -> Box<PgError> {
    Box::new(PgError::error("invalid block number").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn check_relation_relkind(rel: &types_rel::RelationData<'_>) -> PgResult<()> {
    if !RELKIND_HAS_TABLE_AM(rel.rd_rel.relkind) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(Box::new(
            PgError::error(format!(
                "relation \"{}\" is of wrong relation kind",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(detail),
        ));
    }
    Ok(())
}


fn composite_tupdesc<'m>(mcx: Mcx<'m>, flinfo: &FmgrInfo) -> PgResult<TupleDescData<'m>> {
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    Ok(resolved.result_tuple_desc.expect("composite result has tupdesc"))
}

fn composite_result(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Datum> {
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

fn tuple_image(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Vec<u8>> {
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    Ok(tup.image().to_vec())
}

struct RowSet {
    rows: Vec<Vec<u8>>,
}

fn srf_stream(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    first_call_rows: Option<Vec<Vec<u8>>>,
) -> PgResult<Datum> {
    if let Some(rows) = first_call_rows {
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(RowSet { rows }) as Box<dyn Any>);
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rs = fctx
        .user_fctx
        .as_ref()
        .expect("pg_visibility SRF: rows set at first call")
        .downcast_ref::<RowSet>()
        .expect("pg_visibility SRF: user_fctx is RowSet");
    match rs.rows.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

fn tid_image(blkno: BlockNumber, offnum: u16) -> Vec<u8> {
    let ip = ItemPointerData::new(blkno, offnum);
    let mut out = vec![0u8; 6];
    out[0..2].copy_from_slice(&ip.ip_blkid.bi_hi.to_ne_bytes());
    out[2..4].copy_from_slice(&ip.ip_blkid.bi_lo.to_ne_bytes());
    out[4..6].copy_from_slice(&ip.ip_posid.to_ne_bytes());
    out
}


fn r_u16(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(b[off..off + 2].try_into().unwrap())
}

fn r_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

const SIZE_OF_PAGE_HEADER: usize = types_storage::bufpage::SizeOfPageHeaderData;

fn pd_flags(b: &[u8]) -> u16 {
    r_u16(b, 10)
}

fn page_max_offset_number(b: &[u8]) -> usize {
    let lower = r_u16(b, 12) as usize;
    if lower <= SIZE_OF_PAGE_HEADER {
        0
    } else {
        (lower - SIZE_OF_PAGE_HEADER) / 4
    }
}

#[derive(Clone, Copy)]
struct ItemIdView {
    off: u16,
    flags: u8,
    len: u16,
}

fn page_item_id(b: &[u8], offnum: usize) -> ItemIdView {
    let pos = SIZE_OF_PAGE_HEADER + (offnum - 1) * 4;
    let raw = if pos + 4 <= b.len() { r_u32(b, pos) } else { 0 };
    ItemIdView {
        off: (raw & 0x7FFF) as u16,
        flags: ((raw >> 15) & 0x3) as u8,
        len: (raw >> 17) as u16,
    }
}


struct VBits {
    bits: Vec<u8>,
}

fn collect_visibility_data(
    mcx: Mcx<'_>,
    relid: types_core::Oid,
    include_pd: bool,
) -> PgResult<VBits> {
    let bstrategy = bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread);

    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    check_relation_relkind(&rel)?;

    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, ForkNumber::MAIN_FORKNUM)?;
    let mut info = VBits { bits: vec![0u8; nblocks as usize] };
    let mut vmbuffer = VmBuffer::new();

    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        let mapbits = visibilitymap_get_status(&rel, blkno, &mut vmbuffer)?;
        if mapbits & VISIBILITYMAP_ALL_VISIBLE != 0 {
            info.bits[blkno as usize] |= 1 << 0;
        }
        if mapbits & VISIBILITYMAP_ALL_FROZEN != 0 {
            info.bits[blkno as usize] |= 1 << 1;
        }

        if include_pd {
            let buf = bufmgr::ReadBufferExtended(
                &rel,
                ForkNumber::MAIN_FORKNUM,
                blkno,
                types_storage::storage::ReadBufferMode::Normal,
                bstrategy.clone(),
            )?;
            bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
            let pageptr = bufmgr::BufferGetPagePtr(buf);
            // SAFETY: a locked, pinned buffer page is BLCKSZ readable.
            let b: &[u8] = unsafe { core::slice::from_raw_parts(pageptr.as_ptr(), BLCKSZ) };
            if pd_flags(b) & PD_ALL_VISIBLE != 0 {
                info.bits[blkno as usize] |= 1 << 2;
            }
            bufmgr::UnlockReleaseBuffer(buf)?;
        }
    }

    vmbuffer.release();
    rel.close(types_rel::AccessShareLock)?;
    Ok(info)
}

/// GetStrictOldestNonRemovableTransactionId: an xid horizon that can only be
/// newer than any horizon computed before (no proc xmins, no
/// KnownAssignedXids, no walsender xmin), so the checkers never report false
/// corruption.
fn strict_oldest_nonremovable_xid(rel: &types_rel::RelationData<'_>) -> PgResult<TransactionId> {
    if transam_xlog::RecoveryInProgress() {
        // C reads nextXid under XidGenLock; the atomic load reads the same
        // monotonic value.
        let fxid = varsup::TransamVariables()
            .nextXid
            .load(std::sync::atomic::Ordering::Relaxed);
        Ok(types_core::FullTransactionId::from_u64(fxid).xid())
    } else if rel.rd_rel.relisshared {
        // GetRunningTransactionData runs the closure with ProcArrayLock and
        // XidGenLock held; the closure must release both (C's caller does).
        procarray::GetRunningTransactionData(|rt| {
            let x = rt.oldest_running_xid;
            release_running_locks()?;
            Ok(x)
        })
    } else if !relation_is_local(rel) {
        procarray::GetRunningTransactionData(|rt| {
            let x = rt.oldest_database_running_xid;
            release_running_locks()?;
            Ok(x)
        })
    } else {
        procarray::GetOldestNonRemovableTransactionId(rel)
    }
}

fn release_running_locks() -> PgResult<()> {
    lwlock::LWLockRelease(lwlock::main_lock(procarray::PROC_ARRAY_LOCK))?;
    lwlock::LWLockRelease(lwlock::main_lock(procarray::XID_GEN_LOCK))?;
    Ok(())
}

// RELATION_IS_LOCAL (rel.h).
fn relation_is_local(rel: &types_rel::RelationData<'_>) -> bool {
    rel.rd_islocaltemp || rel.rd_createSubid.get() != types_core::InvalidSubTransactionId
}

fn tuple_all_visible(
    tup: &mut HeapTupleData<'_>,
    oldest_xmin: TransactionId,
    buffer: Buffer,
) -> PgResult<bool> {
    let state = heapam_visibility::HeapTupleSatisfiesVacuum(tup, oldest_xmin, buffer)?;
    if state != HTSV_Result::HEAPTUPLE_LIVE {
        return Ok(false); // all-visible implies live
    }
    // Hint bits may be lost after a crash; check the xmin itself.
    // SAFETY: the header lives in the locked, pinned buffer.
    let xmin = unsafe { (*tup.header_ptr().cast::<HeapTupleHeaderData>()).xmin() };
    Ok(TransactionIdPrecedes(xmin, oldest_xmin))
}

fn collect_corrupt_items(
    mcx: Mcx<'_>,
    relid: types_core::Oid,
    all_visible: bool,
    all_frozen: bool,
) -> PgResult<Vec<Vec<u8>>> {
    let bstrategy = bufmgr::GetAccessStrategy(BufferAccessStrategyType::BasBulkread);

    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    check_relation_relkind(&rel)?;

    let mut oldest_xmin = types_core::InvalidTransactionId;
    if all_visible {
        oldest_xmin = strict_oldest_nonremovable_xid(&rel)?;
    }

    let mut items: Vec<Vec<u8>> = Vec::with_capacity(64);
    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, ForkNumber::MAIN_FORKNUM)?;
    let mut filter_vmbuf = VmBuffer::new();
    let mut vmbuffer = VmBuffer::new();

    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;

        let want_frozen = all_frozen && vm_all_frozen(&rel, blkno, &mut filter_vmbuf)?;
        let want_visible = all_visible && vm_all_visible(&rel, blkno, &mut filter_vmbuf)?;
        if !want_frozen && !want_visible {
            continue;
        }

        let buf = bufmgr::ReadBufferExtended(
            &rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            bstrategy.clone(),
        )?;
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;

        let check_frozen = want_frozen && vm_all_frozen(&rel, blkno, &mut vmbuffer)?;
        let check_visible = want_visible && vm_all_visible(&rel, blkno, &mut vmbuffer)?;
        if !check_visible && !check_frozen {
            bufmgr::UnlockReleaseBuffer(buf)?;
            continue;
        }

        let pageptr = bufmgr::BufferGetPagePtr(buf);
        // SAFETY: a locked, pinned buffer page is BLCKSZ readable.
        let b: &[u8] = unsafe { core::slice::from_raw_parts(pageptr.as_ptr(), BLCKSZ) };
        let maxoff = page_max_offset_number(b);

        for offnum in 1..=maxoff {
            let itemid = page_item_id(b, offnum);

            if itemid.flags == LP_UNUSED as u8 && itemid.len == 0 {
                continue;
            }
            if itemid.flags == LP_REDIRECT as u8 {
                continue;
            }
            if itemid.flags == LP_DEAD as u8 {
                items.push(tid_image(blkno, offnum as u16));
                continue;
            }
            debug_assert_eq!(itemid.flags, LP_NORMAL as u8);
            let lp_off = itemid.off as usize;
            let lp_len = itemid.len as u32;
            if lp_off + lp_len as usize > BLCKSZ || lp_len < 23 {
                continue; // corrupt line pointer; C would overread
            }

            // SAFETY: the tuple image lives in the locked, pinned buffer.
            let mut tuple = unsafe {
                HeapTupleData::from_raw_parts(
                    pageptr.as_ptr().add(lp_off),
                    lp_len,
                    ItemPointerData::new(blkno, offnum as u16),
                    relid,
                )
            };

            if check_visible && !tuple_all_visible(&mut tuple, oldest_xmin, buf)? {
                // The horizon may have advanced since we computed it; retake
                // it so a merely-stale horizon can't report a false positive.
                let recomputed = strict_oldest_nonremovable_xid(&rel)?;
                if !TransactionIdPrecedes(oldest_xmin, recomputed) {
                    items.push(tid_image(blkno, offnum as u16));
                } else {
                    oldest_xmin = recomputed;
                    if !tuple_all_visible(&mut tuple, oldest_xmin, buf)? {
                        items.push(tid_image(blkno, offnum as u16));
                    }
                }
            }

            if check_frozen {
                // SAFETY: header in the locked, pinned buffer.
                let hdr = unsafe { &*tuple.header_ptr().cast::<HeapTupleHeaderData>() };
                if heapam::heap_tuple_needs_eventual_freeze(hdr) {
                    items.push(tid_image(blkno, offnum as u16));
                }
            }
        }

        bufmgr::UnlockReleaseBuffer(buf)?;
    }

    filter_vmbuf.release();
    vmbuffer.release();
    rel.close(types_rel::AccessShareLock)?;
    debug_assert!(TransactionIdIsValid(oldest_xmin) || !all_visible);
    Ok(items)
}


fn fc_pg_visibility_map(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_visibility_map: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    let blkno = fcinfo.arg(1).as_i64();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    check_relation_relkind(&rel)?;
    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(invalid_block_err());
    }
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let mut vmbuffer = VmBuffer::new();
    let mapbits = visibilitymap_get_status(&rel, blkno as BlockNumber, &mut vmbuffer)?;
    vmbuffer.release();
    let values = [
        Datum::from_bool(mapbits & VISIBILITYMAP_ALL_VISIBLE != 0),
        Datum::from_bool(mapbits & VISIBILITYMAP_ALL_FROZEN != 0),
    ];

    rel.close(types_rel::AccessShareLock)?;
    composite_result(mcx, &tupdesc, &values, &[false; 2])
}

fn fc_pg_visibility(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_visibility: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    let blkno = fcinfo.arg(1).as_i64();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    check_relation_relkind(&rel)?;
    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(invalid_block_err());
    }
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let mut vmbuffer = VmBuffer::new();
    let mapbits = visibilitymap_get_status(&rel, blkno as BlockNumber, &mut vmbuffer)?;
    vmbuffer.release();

    // Silently return false for pages past EOF, as the VM does.
    let mut pd_all_visible = false;
    if (blkno as BlockNumber)
        < bufmgr::RelationGetNumberOfBlocksInFork(&rel, ForkNumber::MAIN_FORKNUM)?
    {
        let buf = bufmgr::ReadBufferExtended(
            &rel,
            ForkNumber::MAIN_FORKNUM,
            blkno as BlockNumber,
            types_storage::storage::ReadBufferMode::Normal,
            None,
        )?;
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
        let pageptr = bufmgr::BufferGetPagePtr(buf);
        // SAFETY: a locked, pinned buffer page is BLCKSZ readable.
        let b: &[u8] = unsafe { core::slice::from_raw_parts(pageptr.as_ptr(), BLCKSZ) };
        pd_all_visible = pd_flags(b) & PD_ALL_VISIBLE != 0;
        bufmgr::UnlockReleaseBuffer(buf)?;
    }

    let values = [
        Datum::from_bool(mapbits & VISIBILITYMAP_ALL_VISIBLE != 0),
        Datum::from_bool(mapbits & VISIBILITYMAP_ALL_FROZEN != 0),
        Datum::from_bool(pd_all_visible),
    ];

    rel.close(types_rel::AccessShareLock)?;
    composite_result(mcx, &tupdesc, &values, &[false; 3])
}

fn vbits_rows(
    mcx: Mcx<'_>,
    flinfo: &FmgrInfo,
    relid: types_core::Oid,
    include_pd: bool,
) -> PgResult<Vec<Vec<u8>>> {
    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let info = collect_visibility_data(mcx, relid, include_pd)?;
    let mut rows = Vec::with_capacity(info.bits.len());
    for (blkno, bits) in info.bits.iter().enumerate() {
        let mut values = vec![
            Datum::from_i64(blkno as i64),
            Datum::from_bool(bits & (1 << 0) != 0),
            Datum::from_bool(bits & (1 << 1) != 0),
        ];
        if include_pd {
            values.push(Datum::from_bool(bits & (1 << 2) != 0));
        }
        let n = values.len();
        rows.push(tuple_image(mcx, &tupdesc, &values, &vec![false; n])?);
    }
    Ok(rows)
}

fn fc_pg_visibility_map_rel(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_visibility_map_rel: resolved FmgrInfo required");
    let first = if !flinfo.has_fn_extra() {
        let relid = fcinfo.arg(0).as_oid();
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        Some(vbits_rows(mcx, flinfo, relid, false)?)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, first)
}

fn fc_pg_visibility_rel(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_visibility_rel: resolved FmgrInfo required");
    let first = if !flinfo.has_fn_extra() {
        let relid = fcinfo.arg(0).as_oid();
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        Some(vbits_rows(mcx, flinfo, relid, true)?)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, first)
}

fn fc_pg_visibility_map_summary(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_visibility_map_summary: resolved FmgrInfo required");
    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;
    check_relation_relkind(&rel)?;

    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, ForkNumber::MAIN_FORKNUM)?;
    let mut all_visible: i64 = 0;
    let mut all_frozen: i64 = 0;
    let mut vmbuffer = VmBuffer::new();
    for blkno in 0..nblocks {
        postgres_seams::check_for_interrupts::call()?;
        let mapbits = visibilitymap_get_status(&rel, blkno, &mut vmbuffer)?;
        if mapbits & VISIBILITYMAP_ALL_VISIBLE != 0 {
            all_visible += 1;
        }
        if mapbits & VISIBILITYMAP_ALL_FROZEN != 0 {
            all_frozen += 1;
        }
    }
    vmbuffer.release();
    rel.close(types_rel::AccessShareLock)?;

    let tupdesc = composite_tupdesc(mcx, flinfo)?;
    let values = [Datum::from_i64(all_visible), Datum::from_i64(all_frozen)];
    composite_result(mcx, &tupdesc, &values, &[false; 2])
}

fn fc_pg_check_frozen(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_check_frozen: resolved FmgrInfo required");
    let first = if !flinfo.has_fn_extra() {
        let relid = fcinfo.arg(0).as_oid();
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        Some(collect_corrupt_items(mcx, relid, false, true)?)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, first)
}

fn fc_pg_check_visible(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_check_visible: resolved FmgrInfo required");
    let first = if !flinfo.has_fn_extra() {
        let relid = fcinfo.arg(0).as_oid();
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        Some(collect_corrupt_items(mcx, relid, true, false)?)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, first)
}

// RelationNeedsWAL (rel.h).
fn relation_needs_wal(rel: &types_rel::RelationData<'_>) -> bool {
    rel.rd_rel.relpersistence == types_core::catalog::RELPERSISTENCE_PERMANENT
        && (transam_xlog::XLogIsNeeded()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

fn fc_pg_truncate_visibility_map(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use std::sync::atomic::Ordering;
    use types_storage::storage::{DELAY_CHKPT_COMPLETE, DELAY_CHKPT_START};

    let relid = fcinfo.arg(0).as_oid();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let rel = relation::relation_open(mcx, relid, types_rel::AccessExclusiveLock)?;
    check_relation_relkind(&rel)?;

    let rlocator = bufmgr_seams::relation_smgr_locator::call(&rel);
    smgr::smgropen(rlocator.locator, rlocator.backend)?;

    smgr::smgr_set_cached_nblocks(rlocator, ForkNumber::VISIBILITYMAP_FORKNUM, InvalidBlockNumber)?;

    let block = visibilitymap_prepare_truncate(&rel, 0)?;
    let old_block = if block != InvalidBlockNumber {
        smgr::smgrnblocks(rlocator, ForkNumber::VISIBILITYMAP_FORKNUM)?
    } else {
        0
    };

    // WAL-logging, buffer dropping and file truncation must be atomic and on
    // one side of a checkpoint (RelationTruncate).
    let my_procno = lmgr_proc::MyProc().expect("pg_truncate_visibility_map: backend PGPROC");
    let my_proc = lmgr_proc::GetPGProcByNumber(my_procno);
    debug_assert_eq!(
        my_proc.delayChkptFlags.load(Ordering::Relaxed) & (DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE),
        0
    );
    my_proc
        .delayChkptFlags
        .fetch_or(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE, Ordering::SeqCst);
    init_small::globals::StartCriticalSection();

    let crit = (|| -> PgResult<()> {
        if relation_needs_wal(&rel) {
            // xl_smgr_truncate image: blkno + RelFileLocator + flags.
            let loc = rel.rd_locator.get();
            let mut xlrec = [0u8; 20];
            xlrec[0..4].copy_from_slice(&0u32.to_ne_bytes());
            xlrec[4..8].copy_from_slice(&loc.spcOid.to_ne_bytes());
            xlrec[8..12].copy_from_slice(&loc.dbOid.to_ne_bytes());
            xlrec[12..16].copy_from_slice(&loc.relNumber.to_ne_bytes());
            xlrec[16..20].copy_from_slice(&(storage_xlog::SMGR_TRUNCATE_VM as i32).to_ne_bytes());
            let lsn = xloginsert_seams::xlog_insert_record::call(
                RM_SMGR_ID,
                storage_xlog::XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE,
                0,
                &[&xlrec],
                &[],
            )?;
            transam_xlog::XLogFlush(lsn)?;
        }

        if block != InvalidBlockNumber {
            smgr::smgrtruncate(
                rlocator,
                &[ForkNumber::VISIBILITYMAP_FORKNUM],
                &[old_block],
                &[block],
            )?;
        }
        Ok(())
    })();

    init_small::globals::EndCriticalSection();
    my_proc
        .delayChkptFlags
        .fetch_and(!(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE), Ordering::SeqCst);
    crit?;

    // Release the lock right away, not at commit (only a non-transactional
    // smgr invalidation was sent; see the C comment).
    rel.close(types_rel::AccessExclusiveLock)?;

    Ok(Datum::from_i32(0)) // PG_RETURN_VOID
}


fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_visibility_map" => fc_pg_visibility_map,
        "pg_visibility" => fc_pg_visibility,
        "pg_visibility_map_rel" => fc_pg_visibility_map_rel,
        "pg_visibility_rel" => fc_pg_visibility_rel,
        "pg_visibility_map_summary" => fc_pg_visibility_map_summary,
        "pg_check_frozen" => fc_pg_check_frozen,
        "pg_check_visible" => fc_pg_check_visible,
        "pg_truncate_visibility_map" => fc_pg_truncate_visibility_map,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tid_image_layout() {
        // ItemPointerData wire image: bi_hi, bi_lo, ip_posid (native-endian).
        let img = tid_image(0x0001_0002, 7);
        assert_eq!(img.len(), 6);
        assert_eq!(u16::from_ne_bytes(img[0..2].try_into().unwrap()), 1);
        assert_eq!(u16::from_ne_bytes(img[2..4].try_into().unwrap()), 2);
        assert_eq!(u16::from_ne_bytes(img[4..6].try_into().unwrap()), 7);
    }

    #[test]
    fn item_id_clamps_beyond_buffer() {
        let b = [0u8; 64];
        let id = page_item_id(&b, 4096);
        assert_eq!(id.flags, LP_UNUSED as u8);
        assert_eq!(id.len, 0);
    }

    #[test]
    fn vm_bit_positions() {
        assert_eq!(VISIBILITYMAP_ALL_VISIBLE, 0x01);
        assert_eq!(VISIBILITYMAP_ALL_FROZEN, 0x02);
    }
}
