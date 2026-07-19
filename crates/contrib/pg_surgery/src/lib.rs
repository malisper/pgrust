//! Port of `contrib/pg_surgery/heap_surgery.c` — heap_force_kill /
//! heap_force_freeze: owner-only functions that surgically mark heap tuples
//! LP_DEAD or rewrite them frozen by direct page edits under a cleanup lock,
//! WAL-logged as full-page images (C uses `log_newpage_buffer`, not the
//! heapam freeze record — replay is the shared FPI path on both engines).

use datum::Datum;
use types_core::{
    BlockNumber, ForkNumber, FrozenTransactionId, InvalidTransactionId, OffsetNumber,
    RELATION_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::parsenodes::ObjectType;
use types_rel::pg_class::{
    RELKIND_HAS_TABLE_AM, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_VIEW,
};
use types_rel::{RelationData, RowExclusiveLock};
use types_storage::bufpage::{MaxHeapTuplesPerPage, PageMut, PageRef};
use types_tuple::{
    HeapTupleHeaderData, InvalidOffsetNumber, ItemPointerCompare, ItemPointerData,
    ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumberNoCheck, ItemPointerSet,
    HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_MOVED_OFF, HEAP_XACT_MASK,
    HEAP_XMAX_INVALID, HEAP_XMIN_FROZEN,
};
use visibilitymap::{VmBuffer, VISIBILITYMAP_VALID_BITS};

const LIBRARY: &str = "pg_surgery";

#[derive(Clone, Copy, PartialEq, Eq)]
enum ForceOption {
    Kill,
    Freeze,
}

fn fc_heap_force_kill(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    heap_force_common(fcinfo, ForceOption::Kill)
}

fn fc_heap_force_freeze(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    heap_force_common(fcinfo, ForceOption::Freeze)
}

// RelationNeedsWAL (rel.h), as brin_pageops renders it.
fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

// get_relkind_objtype (objectaddress.c), the reachable kinds.
fn get_relkind_objtype(relkind: u8) -> ObjectType {
    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => ObjectType::OBJECT_TABLE,
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => ObjectType::OBJECT_INDEX,
        RELKIND_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}

fn read_tid_array<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    ta: &[u8],
) -> PgResult<mcx::PgVec<'mcx, ItemPointerData>> {
    if arrayfuncs::arr_hasnull(ta) && arrayfuncs::array_contains_nulls(ta) {
        return Err(Box::new(
            PgError::error("array must not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }
    let ndim = arrayfuncs::arr_ndim(ta);
    if ndim > 1 {
        return Err(Box::new(
            PgError::error("argument must be empty or one-dimensional array")
                .with_sqlstate(ERRCODE_DATA_EXCEPTION),
        ));
    }
    let (nd, dims, _lbs) = arrayfuncs::read_dims_lbounds(ta);
    let ntids = arrayutils::array_get_n_items(nd, &dims[..nd.max(0) as usize])? as usize;

    let mut tids = mcx::vec_with_capacity_in(mcx, ntids)?;
    let mut off = arrayfuncs::arr_data_offset(ta);
    for _ in 0..ntids {
        let word = |o: usize| u16::from_ne_bytes([ta[o], ta[o + 1]]);
        tids.push(ItemPointerData {
            ip_blkid: types_tuple::BlockIdData {
                bi_hi: word(off),
                bi_lo: word(off + 2),
            },
            ip_posid: word(off + 4),
        });
        off += 6;
    }
    Ok(tids)
}

fn find_tids_one_page(tids: &[ItemPointerData], next_start_ptr: &mut usize) -> BlockNumber {
    let mut prev_blkno = types_core::InvalidBlockNumber;
    let mut i = *next_start_ptr;
    while i < tids.len() {
        let blkno = ItemPointerGetBlockNumberNoCheck(&tids[i]);
        if i == *next_start_ptr {
            prev_blkno = blkno;
        }
        if prev_blkno != blkno {
            break;
        }
        i += 1;
    }
    *next_start_ptr = i;
    prev_blkno
}

fn notice(msg: String) -> PgError {
    PgError::notice(msg)
}

/// # Safety
/// Caller holds the buffer pinned and cleanup-locked; `offset` is an
/// LP_NORMAL item of the page.
unsafe fn header_mut_at<'a>(
    page: PageRef<'a>,
    offset: OffsetNumber,
) -> &'a mut HeapTupleHeaderData {
    let lp = page.item_id(offset);
    let (ptr, _len) = page.item_raw(lp);
    // SAFETY: caller contract.
    unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) }
}

fn heap_force_common(fcinfo: &mut Fcinfo, opt: ForceOption) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(Box::new(
            PgError::error("recovery is in progress")
                .with_hint("Heap surgery functions cannot be executed during recovery.")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }

    let relid = fcinfo.arg_oid(0);
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict arg 1 is a non-null tid[] varlena.
    let raw = unsafe { fcinfo.arg_varlena_raw(1) };
    let ta = detoast_seams::detoast_attr::call(mcx, raw)?;
    let mut tids = read_tid_array(mcx, &ta)?;
    let ntids = tids.len();

    let rel = relation::relation_open(mcx, relid, RowExclusiveLock)?;

    if !RELKIND_HAS_TABLE_AM(rel.rd_rel.relkind) {
        return Err(Box::new(
            PgError::error(format!("cannot operate on relation \"{}\"", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(pg_class::errdetail_relkind_not_supported(
                    rel.rd_rel.relkind,
                )?),
        ));
    }

    if rel.rd_rel.relam != tableam_vocab::HEAP_TABLE_AM_OID {
        return Err(Box::new(
            PgError::error("only heap AM is supported")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            get_relkind_objtype(rel.rd_rel.relkind),
            rel.name(),
        )?;
    }

    if ntids > 1 {
        tids.sort_unstable_by(|a, b| ItemPointerCompare(a, b).cmp(&0));
    }

    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, ForkNumber::MAIN_FORKNUM)?;
    let mut curr_start_ptr = 0usize;
    let mut next_start_ptr = 0usize;

    while next_start_ptr != ntids {
        postgres_seams::check_for_interrupts::call()?;

        let blkno = find_tids_one_page(&tids, &mut next_start_ptr);

        if blkno >= nblocks {
            curr_start_ptr = next_start_ptr;
            elog_seams::ereport::call(
                notice(format!(
                    "skipping block {blkno} for relation \"{}\" because the block number is out of range",
                    rel.name()
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            )?;
            continue;
        }

        let buf = bufmgr::ReadBuffer(&rel, blkno)?;
        bufmgr::LockBufferForCleanup(buf)?;
        let page_ptr = bufmgr::BufferGetPagePtr(buf);
        // SAFETY: pinned + cleanup-locked for the rest of this iteration.
        let page = unsafe { PageRef::from_raw(page_ptr) };
        let maxoffset = page.max_offset_number();

        let mut include_this_tid = [false; MaxHeapTuplesPerPage + 1];
        for tid in &tids[curr_start_ptr..next_start_ptr] {
            let offno = ItemPointerGetOffsetNumberNoCheck(tid);

            if offno == InvalidOffsetNumber || offno > maxoffset {
                elog_seams::ereport::call(notice(format!(
                    "skipping tid ({blkno}, {offno}) for relation \"{}\" because the item number is out of range",
                    rel.name()
                )))?;
                continue;
            }

            let itemid = page.item_id(offno);

            if itemid.is_redirected() {
                elog_seams::ereport::call(notice(format!(
                    "skipping tid ({blkno}, {offno}) for relation \"{}\" because it redirects to item {}",
                    rel.name(),
                    itemid.lp_off()
                )))?;
                continue;
            }
            if itemid.is_dead() {
                elog_seams::ereport::call(notice(format!(
                    "skipping tid ({blkno}, {offno}) for relation \"{}\" because it is marked dead",
                    rel.name()
                )))?;
                continue;
            }
            if !itemid.is_used() {
                elog_seams::ereport::call(notice(format!(
                    "skipping tid ({blkno}, {offno}) for relation \"{}\" because it is marked unused",
                    rel.name()
                )))?;
                continue;
            }

            debug_assert!((offno as usize) < MaxHeapTuplesPerPage);
            include_this_tid[offno as usize] = true;
        }

        let mut vmbuf = VmBuffer::new();
        if opt == ForceOption::Kill && page.is_all_visible() {
            visibilitymap::visibilitymap_pin(&rel, blkno, &mut vmbuf)?;
        }

        let mut did_modify_page = false;
        let mut did_modify_vm = false;

        init_small::globals::StartCriticalSection();

        for curoff in 1..=maxoffset {
            if !include_this_tid[curoff as usize] {
                continue;
            }
            let mut itemid = page.item_id(curoff);
            debug_assert!(itemid.is_normal());
            did_modify_page = true;

            match opt {
                ForceOption::Kill => {
                    itemid.set_dead();
                    // SAFETY: cleanup lock = exclusive ownership of the image.
                    let mut pm = unsafe { PageMut::from_raw(page_ptr) };
                    pm.set_item_id(curoff, itemid);

                    if page.is_all_visible() {
                        pm.clear_all_visible();
                        visibilitymap::visibilitymap_clear(
                            &rel,
                            blkno,
                            &vmbuf,
                            VISIBILITYMAP_VALID_BITS,
                        )?;
                        did_modify_vm = true;
                    }
                }
                ForceOption::Freeze => {
                    // Reset all visibility-related fields, mimicking
                    // heap_execute_freeze_tuple but also resetting xmin/ctid
                    // (C's belt-and-suspenders against garbled data).
                    // SAFETY: LP_NORMAL item of this cleanup-locked page.
                    let htup = unsafe { header_mut_at(page, curoff) };
                    ItemPointerSet(&mut htup.t_ctid, blkno, curoff);
                    htup.set_xmin(FrozenTransactionId);
                    htup.set_xmax(InvalidTransactionId);
                    if htup.t_infomask & HEAP_MOVED != 0 {
                        if htup.t_infomask & HEAP_MOVED_OFF != 0 {
                            htup.set_xvac(InvalidTransactionId);
                        } else {
                            htup.set_xvac(FrozenTransactionId);
                        }
                    }
                    htup.t_infomask &= !HEAP_XACT_MASK;
                    htup.t_infomask |= HEAP_XMIN_FROZEN | HEAP_XMAX_INVALID;
                    htup.t_infomask2 &= !HEAP_HOT_UPDATED;
                    htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
                }
            }
        }

        if did_modify_page {
            bufmgr::MarkBufferDirty(buf)?;
            if relation_needs_wal(&rel) {
                xloginsert::log_newpage_buffer(buf, true)?;
            }
        }

        if did_modify_vm && relation_needs_wal(&rel) {
            xloginsert::log_newpage_buffer(vmbuf.buffer(), false)?;
        }

        init_small::globals::EndCriticalSection();

        bufmgr::UnlockReleaseBuffer(buf)?;
        vmbuf.release();

        curr_start_ptr = next_start_ptr;
    }

    rel.close(RowExclusiveLock)?;

    Ok(Datum::from_usize(0))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "heap_force_kill" => fc_heap_force_kill,
        "heap_force_freeze" => fc_heap_force_freeze,
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
